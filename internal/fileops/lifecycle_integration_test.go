//go:build integration

package fileops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

type copyQueue struct{}

func (copyQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, "INSERT INTO fileops_receipt_jobs(user_id) VALUES($1) RETURNING id", in.UserID).Scan(&id)
	return id, err
}

type copyStorage struct {
	*fixture.Storage
	mu            sync.Mutex
	calls         int
	pool          *pgxpool.Pool
	deleteChannel int64
	deleteError   error
}

func (s *copyStorage) CopyPart(ctx context.Context, owner, channel, message, destination int64) (telegramstore.StoredPart, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	var reserved bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM remote_write_receipts r JOIN remote_write_receipts op ON op.id=r.operation_id
 WHERE r.row_kind='message_attempt' AND r.user_id=$1 AND r.channel_id=$2
 AND r.state='reserved' AND r.message_id IS NULL AND r.result_file_id IS NOT NULL
 AND op.row_kind='copy_operation' AND op.lease_token=r.lease_token
 AND op.state='reserved' AND op.terminal_at IS NULL AND op.lease_expires_at>clock_timestamp())`, owner, destination).Scan(&reserved)
	if err != nil {
		return telegramstore.StoredPart{}, err
	}
	if !reserved {
		return telegramstore.StoredPart{}, errors.New("actual CopyPart lacked committed exact destination reservation")
	}
	return s.Storage.CopyPart(ctx, owner, channel, message, destination)
}

func (s *copyStorage) DeleteMessages(ctx context.Context, owner, channel int64, ids []int64) error {
	if channel == s.deleteChannel && s.deleteError != nil {
		return s.deleteError
	}
	return s.Storage.DeleteMessages(ctx, owner, channel, ids)
}

func (s *copyStorage) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

type copyFixture struct {
	pool                          *pgxpool.Pool
	databaseURL                   string
	catalog                       *catalog.Service
	ledger                        *lifecycle.Service
	storage                       *copyStorage
	service                       *Service
	ancestor, parent, source, old uuid.UUID
	body                          []byte
}

func newCopyFixture(t *testing.T) *copyFixture {
	t.Helper()
	db := testpostgres.New(t)
	ctx := context.Background()
	for _, statement := range []string{
		"INSERT INTO users(user_id) VALUES(1001),(1002)",
		"INSERT INTO channels(channel_id,user_id,name,selected) VALUES(9001,1001,'fixture',true),(9002,1001,'second fixture',false)",
		"CREATE TABLE fileops_receipt_jobs(id bigint GENERATED ALWAYS AS IDENTITY,user_id bigint)",
	} {
		if _, err := db.Pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	storage := &copyStorage{pool: db.Pool, Storage: fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}, {ChannelID: 9002, Actors: []int64{1001, 1002}}})}
	ledger := lifecycle.NewService(db.Pool, storage)
	if err := ledger.BindReceiptEnqueuer(copyQueue{}); err != nil {
		t.Fatal(err)
	}
	hierarchy := catalog.NewService(db.Pool, nil)
	if err := hierarchy.BindLifecycle(ledger); err != nil {
		t.Fatal(err)
	}
	ancestor := insertFolder(t, db, nil, "ancestor")
	parent := insertFolder(t, db, &ancestor, "destination")
	source := insertStoredFile(t, db, "source.bin", 10)
	old := insertStoredFile(t, db, "old.bin", 20)
	body := []byte("data")
	for _, m := range []fixture.Message{
		{UserID: 1001, ChannelID: 9001, MessageID: 10, Bytes: body, Preserve: true},
		{UserID: 1001, ChannelID: 9001, MessageID: 20, Bytes: []byte("old!"), Preserve: true},
		{UserID: 1002, ChannelID: 9001, MessageID: 30, Bytes: []byte("foreign shared bytes"), Preserve: true},
	} {
		if err := storage.Seed(m); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Pool.Exec(ctx, "INSERT INTO file_shares(id,owner_id,file_id,token_prefix,token_hash,permission) VALUES($1,1001,$2,'fixture',$3,'read')", uuid.New(), old, sha256Bytes("fileops independent share")); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(db.Pool, hierarchy, channels.NewService(db.Pool, nil, channels.Config{PartLimit: 10000}), storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.BindLifecycle(ledger); err != nil {
		t.Fatal(err)
	}
	return &copyFixture{pool: db.Pool, databaseURL: db.URL, catalog: hierarchy, ledger: ledger, storage: storage, service: service, ancestor: ancestor, parent: parent, source: source, old: old, body: body}
}

func (f *copyFixture) input() CopyInput {
	name := "copied.bin"
	return CopyInput{UserID: 1001, FileID: f.source, ParentID: &f.parent, Name: &name}
}

func (f *copyFixture) preserve(t *testing.T) {
	t.Helper()
	if err := f.storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[int64][]byte{10: f.body, 20: []byte("old!"), 30: []byte("foreign shared bytes")} {
		got, ok := f.storage.Bytes(9001, id)
		if !ok || sha256.Sum256(got) != sha256.Sum256(want) {
			t.Fatalf("independent preserved SHA-256 for %d", id)
		}
	}
	var revoked bool
	if err := f.pool.QueryRow(context.Background(), "SELECT revoked_at IS NOT NULL FROM file_shares WHERE file_id=$1", f.old).Scan(&revoked); err != nil || revoked {
		t.Fatalf("old share changed: %v %v", revoked, err)
	}
}

func sha256Bytes(value string) []byte { digest := sha256.Sum256([]byte(value)); return digest[:] }

func (f *copyFixture) operation(t *testing.T) (uuid.UUID, uuid.UUID, bool, bool) {
	t.Helper()
	var id, result uuid.UUID
	var terminal, published bool
	if err := f.pool.QueryRow(context.Background(), "SELECT id,result_file_id,terminal_at IS NOT NULL,published_at IS NOT NULL FROM remote_write_receipts WHERE row_kind='copy_operation' ORDER BY created_at LIMIT 1").Scan(&id, &result, &terminal, &published); err != nil {
		t.Fatal(err)
	}
	return id, result, terminal, published
}

func (f *copyFixture) noLoser(t *testing.T) {
	t.Helper()
	_, result, terminal, published := f.operation(t)
	if !terminal || published {
		t.Fatalf("operation outcome terminal=%v published=%v", terminal, published)
	}
	var files, parts, events int
	if err := f.pool.QueryRow(context.Background(), "SELECT (SELECT count(*) FROM files WHERE id=$1),(SELECT count(*) FROM file_parts WHERE file_id=$1),(SELECT count(*) FROM user_events WHERE resource_id=$1::text AND event_type='file.created')", result).Scan(&files, &parts, &events); err != nil {
		t.Fatal(err)
	}
	if files+parts+events != 0 {
		t.Fatalf("loser published files=%d parts=%d events=%d", files, parts, events)
	}
	f.preserve(t)
}

func TestB2CopyLifecycle(t *testing.T) {
	t.Run("missing_policy_fails_closed", func(t *testing.T) {
		f := newCopyFixture(t)
		fresh, err := NewService(f.pool, f.catalog, channels.NewService(f.pool, nil, channels.Config{}), f.storage)
		if err != nil {
			t.Fatal(err)
		}
		if err = fresh.BindLifecycle(nil); !errors.Is(err, ErrLifecycleUnavailable) {
			t.Fatal("nil policy accepted")
		}
		if _, err = fresh.Copy(context.Background(), f.input()); !errors.Is(err, ErrLifecycleUnavailable) {
			t.Fatal("unconfigured copy admitted")
		}
		if err = fresh.QueuePurge(context.Background(), 1001, f.source); !errors.Is(err, ErrLifecycleUnavailable) {
			t.Fatal("unconfigured purge admitted")
		}
		if err = fresh.BindLifecycle(f.ledger); err != nil {
			t.Fatal(err)
		}
		if err = fresh.BindLifecycle(f.ledger); !errors.Is(err, ErrLifecycleUnavailable) {
			t.Fatal("policy rebound")
		}
		if f.storage.count() != 0 {
			t.Fatal("missing policy reached remote copy")
		}
		f.preserve(t)
	})
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty_%v", empty), func(t *testing.T) {
			f := newCopyFixture(t)
			in := f.input()
			if empty {
				in.FileID = f.ancestor
				in.ParentID = nil
				name := "empty-copy"
				in.Name = &name
			}
			result, err := f.service.Copy(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			_, id, terminal, published := f.operation(t)
			if id != uuid.UUID(result.ID.Bytes) || terminal || !published {
				t.Fatal("copy outcome not published atomically")
			}
			if !empty {
				parts, err := f.catalog.Parts(context.Background(), 1001, id)
				if err != nil || len(parts) != 1 {
					t.Fatalf("parts: %v %v", parts, err)
				}
				got, ok := f.storage.Bytes(parts[0].ChannelID, parts[0].MessageID)
				if !ok || sha256.Sum256(got) != sha256.Sum256(f.body) {
					t.Fatal("copy byte identity")
				}
			}
			f.preserve(t)
		})
	}
}

func TestB2CopyDestinationFence(t *testing.T) {
	for _, name := range []string{"trash_restore_resume", "ancestor_trash_restore_resume", "purge_destination", "replacement_destination", "publication_first", "deletion_rollback", "late_receipt_exact_cleanup"} {
		t.Run(name, func(t *testing.T) {
			f := newCopyFixture(t)
			ctx := context.Background()
			if name == "publication_first" {
				result, err := f.service.Copy(ctx, f.input())
				if err != nil {
					t.Fatal(err)
				}
				parts, err := f.catalog.Parts(ctx, 1001, uuid.UUID(result.ID.Bytes))
				if err != nil || len(parts) != 1 {
					t.Fatalf("winner parts %v", err)
				}
				if _, err = f.catalog.Trash(ctx, 1001, f.parent); err != nil {
					t.Fatal(err)
				}
				_, _, terminal, published := f.operation(t)
				if terminal || !published {
					t.Fatal("winner terminalized")
				}
				if _, ok := f.storage.Bytes(parts[0].ChannelID, parts[0].MessageID); !ok {
					t.Fatal("winner bytes removed")
				}
				f.preserve(t)
				return
			}
			barrier := fixture.NewBarrier(t, nil)
			faults := fixture.SendFaults{BeforeCopy: barrier}
			if name == "late_receipt_exact_cleanup" {
				faults = fixture.SendFaults{AfterCopy: barrier}
			}
			f.storage.SetSendFaults(faults)
			done := make(chan error, 1)
			go func() { _, err := f.service.Copy(ctx, f.input()); done <- err }()
			if err := barrier.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			operation, _, _, _ := f.operation(t)
			switch name {
			case "deletion_rollback":
				tx, err := f.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				if err = lifecycle.LockOwnerTx(ctx, tx, 1001); err != nil {
					t.Fatal(err)
				}
				ids, err := sqlcgen.New(tx).ListLifecycleSubtreeIDs(ctx, sqlcgen.ListLifecycleSubtreeIDsParams{UserID: 1001, RootIds: pgUUIDs([]uuid.UUID{f.parent})})
				if err != nil {
					t.Fatal(err)
				}
				var subtree []uuid.UUID
				for _, id := range ids {
					subtree = append(subtree, uuid.UUID(id.Bytes))
				}
				if err = f.ledger.TerminalizeDestinationsTx(ctx, tx, 1001, subtree); err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(ctx, "UPDATE files SET status='trashed',deleted_at=now() WHERE id=$1", f.parent); err != nil {
					t.Fatal(err)
				}
				if err = tx.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
			case "replacement_destination":
				// Replace the exact destination UUID through actual copy conflict publication.
				f.storage.SetSendFaults(fixture.SendFaults{})
				name := "destination"
				if _, err := f.service.Copy(ctx, CopyInput{UserID: 1001, FileID: f.source, ParentID: &f.ancestor, Name: &name, ConflictPolicy: sqlcgen.NameConflictPolicyReplace}); err != nil {
					t.Fatal(err)
				}
			default:
				target := f.parent
				if name == "ancestor_trash_restore_resume" {
					target = f.ancestor
				}
				if _, err := f.catalog.Trash(ctx, 1001, target); err != nil {
					t.Fatal(err)
				}
				if name == "purge_destination" {
					if err := f.service.QueuePurge(ctx, 1001, target); err != nil {
						t.Fatal(err)
					}
					if err := f.service.PurgeMany(ctx, 1001, []uuid.UUID{target}); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := f.catalog.Restore(ctx, 1001, target); err != nil {
						t.Fatal(err)
					}
				}
			}
			barrier.Release()
			err := <-done
			if name == "deletion_rollback" {
				if err != nil {
					t.Fatal(err)
				}
				f.preserve(t)
				return
			}
			if !errors.Is(err, lifecycle.ErrCopyDestinationUnavailable) {
				t.Fatalf("copy loss: %v", err)
			}
			var terminal bool
			var lease time.Time
			var token uuid.UUID
			if err = f.pool.QueryRow(ctx, "SELECT terminal_at IS NOT NULL,lease_expires_at,lease_token FROM remote_write_receipts WHERE id=$1", operation).Scan(&terminal, &lease, &token); err != nil || !terminal || lease.After(time.Now()) {
				t.Fatalf("terminal lease fence: %v %v", terminal, err)
			}
			if _, err = sqlcgen.New(f.pool).RenewCopyOperation(ctx, sqlcgen.RenewCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(operation), LeaseToken: dbtypes.UUID(token), LeaseExpiresAt: dbtypes.Time(time.Now().Add(time.Minute))}); !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("terminal renewed with original token: %v", err)
			}
			f.noLoser(t)
			var remaining int
			if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM remote_write_receipts WHERE operation_id=$1 AND row_kind='message_attempt' AND state<>'deleted'", operation).Scan(&remaining); err != nil || remaining != 0 {
				t.Fatalf("late destination receipt not exactly cleaned: %d %v", remaining, err)
			}
			for _, call := range f.storage.Deletes() {
				for _, id := range call.MessageIDs {
					if id <= 30 {
						t.Fatalf("cleanup touched original identity %d", id)
					}
				}
			}
		})
	}
}

func TestB2CopyCompensation(t *testing.T) {
	for _, name := range []string{"receipt_and_error", "cleanup_failure", "commit_lost_ack", "commit_rollback", "unknown_receipt", "shared_cleanup_budget"} {
		t.Run(name, func(t *testing.T) {
			f := newCopyFixture(t)
			ctx := context.Background()
			primary := errors.New("copy transport failed")
			var budgetTrace *copyBudgetTracer
			if name == "shared_cleanup_budget" {
				budgetTrace = attachCopyBudget(t, f)
			}
			switch name {
			case "commit_lost_ack":
				f.service.commit = func(ctx context.Context, tx pgx.Tx) error {
					if err := tx.Commit(ctx); err != nil {
						return err
					}
					return fixture.ErrResponseLost
				}
			case "commit_rollback":
				f.service.commit = func(ctx context.Context, tx pgx.Tx) error { _ = tx.Rollback(ctx); return pgx.ErrTxCommitRollback }
			case "unknown_receipt":
				f.storage.SetSendFaults(fixture.SendFaults{CopyWithoutReceipt: true, CopyError: primary})
			default:
				f.storage.SetSendFaults(fixture.SendFaults{CopyError: primary})
			}
			if name == "cleanup_failure" {
				f.storage.SetDeleteFaults(fixture.DeleteFaults{Error: errors.New("delete failed")})
			}
			result, err := f.service.Copy(ctx, f.input())
			if name == "commit_lost_ack" {
				if err != nil || result == nil {
					t.Fatalf("actual committed winner readback: %v", err)
				}
				if len(f.storage.Deletes()) != 0 {
					t.Fatal("compensated committed winner")
				}
				f.preserve(t)
				return
			}
			if err == nil {
				t.Fatal("copy failure acknowledged")
			}
			if name == "receipt_and_error" || name == "cleanup_failure" {
				if !errors.Is(err, primary) {
					t.Fatalf("lost primary error %v", err)
				}
			}
			f.noLoser(t)
			var known, deleted, reported int
			if err = f.pool.QueryRow(ctx, "SELECT count(*) FILTER(WHERE message_id IS NOT NULL),count(*) FILTER(WHERE state='deleted'),count(*) FILTER(WHERE job_id>0) FROM remote_write_receipts WHERE row_kind='message_attempt'").Scan(&known, &deleted, &reported); err != nil {
				t.Fatal(err)
			}
			if name == "unknown_receipt" {
				if known != 0 || len(f.storage.Deletes()) != 0 {
					t.Fatal("unknown receipt guessed")
				}
			} else if known != 1 || reported != 1 {
				t.Fatalf("exact durable debt known=%d reported=%d", known, reported)
			}
			if budgetTrace != nil {
				budgetTrace.assert(t)
			}
			if name == "receipt_and_error" || name == "commit_rollback" {
				if deleted != 1 {
					t.Fatal("known loser not cleaned")
				}
			}
		})
	}
}

func TestB2CopyRequestAdmission(t *testing.T) {
	for _, name := range []string{"success_reply_lost", "terminal_reply_lost", "conflict_terminal_replay", "concurrent_restart", "fingerprint_mismatch", "source_removed", "current_result_move", "current_result_trash", "current_result_purge", "scope_mismatch", "retained_expired"} {
		t.Run(name, func(t *testing.T) {
			f := newCopyFixture(t)
			ctx := context.Background()
			in := f.input()
			actor := int64(1001)
			in.Admission = &lifecycle.RequestAdmissionInput{Scope: lifecycle.RequestScope{ActorUserID: &actor}, RequestKey: uuid.New(), RequestFingerprint: sha256.Sum256([]byte("original immutable copy request"))}
			if name == "success_reply_lost" {
				f.service.commit = func(ctx context.Context, tx pgx.Tx) error {
					if err := tx.Commit(ctx); err != nil {
						return err
					}
					return fixture.ErrResponseLost
				}
			}
			var first *sqlcgen.File
			if name == "conflict_terminal_replay" {
				name := "old.bin"
				in.Name = &name
				in.ParentID = nil
				if _, err := f.service.Copy(ctx, in); !errors.Is(err, catalog.ErrConflict) {
					t.Fatalf("initial conflict outcome: %v", err)
				}
			} else if name == "terminal_reply_lost" {
				barrier := fixture.NewBarrier(t, nil)
				f.storage.SetSendFaults(fixture.SendFaults{BeforeCopy: barrier})
				done := make(chan error, 1)
				go func() { _, err := f.service.Copy(ctx, in); done <- err }()
				if err := barrier.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.Trash(ctx, 1001, f.parent); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.Restore(ctx, 1001, f.parent); err != nil {
					t.Fatal(err)
				}
				barrier.Release()
				if err := <-done; !errors.Is(err, lifecycle.ErrCopyDestinationUnavailable) {
					t.Fatal(err)
				}
			} else if name == "concurrent_restart" || name == "retained_expired" {
				barrier := fixture.NewBarrier(t, nil)
				f.storage.SetSendFaults(fixture.SendFaults{BeforeCopy: barrier})
				done := make(chan error, 1)
				go func() { _, err := f.service.Copy(ctx, in); done <- err }()
				if err := barrier.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				if name == "retained_expired" {
					if _, err := f.pool.Exec(ctx, "UPDATE remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE row_kind='copy_operation'"); err != nil {
						t.Fatal(err)
					}
				}
				var operationBefore, operationAfter string
				if err := f.pool.QueryRow(ctx, "SELECT to_jsonb(r)::text FROM remote_write_receipts r WHERE row_kind='copy_operation'").Scan(&operationBefore); err != nil {
					t.Fatal(err)
				}
				if _, err := f.service.Copy(ctx, in); !errors.Is(err, catalog.ErrConflict) {
					t.Fatalf("duplicate in progress: %v", err)
				}
				if err := f.pool.QueryRow(ctx, "SELECT to_jsonb(r)::text FROM remote_write_receipts r WHERE row_kind='copy_operation'").Scan(&operationAfter); err != nil || operationBefore != operationAfter {
					t.Fatal("duplicate renewed or took over original operation")
				}
				barrier.Release()
				err := <-done
				if name == "concurrent_restart" && err != nil {
					t.Fatal(err)
				}
			} else {
				var err error
				first, err = f.service.Copy(ctx, in)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := f.storage.count()
			op, result, terminal, published := f.operation(t)
			var beforeEvents int
			if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE event_type='file.created'").Scan(&beforeEvents); err != nil {
				t.Fatal(err)
			}
			identitiesBefore := fileopsCopyIdentities(t, f.pool)
			switch name {
			case "fingerprint_mismatch":
				in.Admission.RequestFingerprint = sha256.Sum256([]byte("different request"))
			case "scope_mismatch":
				in.UserID = 1002
			case "source_removed":
				if _, err := f.catalog.Trash(ctx, 1001, f.source); err != nil {
					t.Fatal(err)
				}
			case "current_result_move":
				if _, err := f.catalog.MoveWithPolicy(ctx, 1001, result, nil, nil, string(sqlcgen.NameConflictPolicyFail)); err != nil {
					t.Fatal(err)
				}
			case "current_result_trash", "current_result_purge":
				if _, err := f.catalog.Trash(ctx, 1001, result); err != nil {
					t.Fatal(err)
				}
				if name == "current_result_purge" {
					if err := f.service.QueuePurge(ctx, 1001, result); err != nil {
						t.Fatal(err)
					}
					if err := f.service.PurgeMany(ctx, 1001, []uuid.UUID{result}); err != nil {
						t.Fatal(err)
					}
				}
			}
			fresh, err := NewService(f.pool, f.catalog, channels.NewService(f.pool, nil, channels.Config{}), f.storage)
			if err != nil {
				t.Fatal(err)
			}
			if err = fresh.BindLifecycle(f.ledger); err != nil {
				t.Fatal(err)
			}
			replayed, err := fresh.Copy(ctx, in)
			switch name {
			case "fingerprint_mismatch", "scope_mismatch":
				if !errors.Is(err, lifecycle.ErrRequestMismatch) {
					t.Fatalf("mismatch %v", err)
				}
			case "conflict_terminal_replay":
				if !errors.Is(err, catalog.ErrConflict) {
					t.Fatalf("retained conflict changed: %v", err)
				}
			case "terminal_reply_lost", "retained_expired", "current_result_trash", "current_result_purge":
				if err == nil {
					t.Fatal("unavailable admission replayed success")
				}
			default:
				if err != nil || replayed == nil || uuid.UUID(replayed.ID.Bytes) != result {
					t.Fatalf("original result replay: %v", err)
				}
				if first != nil && first.ID != replayed.ID {
					t.Fatal("result replaced")
				}
			}
			var afterEvents int
			if e := f.pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE event_type='file.created'").Scan(&afterEvents); e != nil {
				t.Fatal(e)
			}
			afterOp, afterResult, afterTerminal, afterPublished := f.operation(t)
			if f.storage.count() != before || beforeEvents != afterEvents || op != afterOp || result != afterResult || terminal != afterTerminal || published != afterPublished {
				t.Fatal("replay repeated or replaced operation")
			}
			if after := fileopsCopyIdentities(t, f.pool); after != identitiesBefore {
				t.Fatal("same-key retry allocated or replaced receipt identities")
			}
			f.preserve(t)
		})
	}
}

func (f *copyFixture) upload(t *testing.T) (*uploads.Service, *transfer.Pipeline, uuid.UUID, *sqlcgen.File, *sqlcgen.UploadPart) {
	return f.uploadNamed(t, "uploaded.bin")
}

func TestB2CompletedUploadPurge(t *testing.T) {
	t.Run("zero_byte_completed", func(t *testing.T) {
		f := newCopyFixture(t)
		ctx := context.Background()
		service := uploads.NewService(f.pool)
		if err := service.BindLifecycle(f.ledger); err != nil {
			t.Fatal(err)
		}
		session, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: "empty.bin", ExpectedSize: 0})
		if err != nil {
			t.Fatal(err)
		}
		file, err := service.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: uuid.UUID(session.ID.Bytes), Size: 0})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = service.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: uuid.UUID(session.ID.Bytes), Size: 0}); err != nil {
			t.Fatal(err)
		}
		if _, err = f.catalog.Trash(ctx, 1001, f.parent); err != nil {
			t.Fatal(err)
		}
		if err = f.service.QueuePurge(ctx, 1001, f.parent); err != nil {
			t.Fatal(err)
		}
		if err = f.service.PurgeMany(ctx, 1001, []uuid.UUID{f.parent}); err != nil {
			t.Fatal(err)
		}
		var parent *uuid.UUID
		var state string
		if err = f.pool.QueryRow(ctx, "SELECT parent_id,state::text FROM upload_sessions WHERE id=$1", session.ID).Scan(&parent, &state); err != nil || parent != nil || state != "completed" {
			t.Fatalf("zero-byte completed parent retention: %v %s %v", parent, state, err)
		}
		var events int
		if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE event_type='file.purged' AND resource_id=$1", uuid.UUID(file.ID.Bytes).String()).Scan(&events); err != nil || events != 1 {
			t.Fatalf("zero-byte purge event=%d %v", events, err)
		}
		if len(f.storage.Deletes()) != 0 {
			t.Fatal("zero-byte purge guessed remote deletion")
		}
		f.preserve(t)
	})
	for _, reference := range []string{"owned", "open", "aborted", "expired", "completed", "uploading_session", "other_channel"} {
		t.Run(reference, func(t *testing.T) {
			f := newCopyFixture(t)
			ctx := context.Background()
			service, pipeline, id, file, part := f.upload(t)
			fileID := uuid.UUID(file.ID.Bytes)
			if _, err := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: id, PartNo: 1, PlainSize: int64(len(f.body)), Body: bytes.NewReader(f.body)}); err != nil {
				t.Fatal(err)
			}
			var aliases int
			if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1", id).Scan(&aliases); err != nil || aliases != 0 {
				t.Fatalf("actual completed origin alias not retired: %d %v", aliases, err)
			}
			reader, err := f.storage.OpenRange(ctx, telegramstore.RangeRequest{UserID: 1001, ChannelID: part.ChannelID, MessageID: part.MessageID.Int64, Length: -1})
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil || sha256.Sum256(got) != sha256.Sum256(f.body) {
				t.Fatal("independent completed download")
			}
			channel := part.ChannelID
			if reference == "other_channel" {
				channel = 9002
			}
			if reference == "other_channel" {
				if err = f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: channel, MessageID: part.MessageID.Int64, Bytes: []byte("unrelated physical identity"), Preserve: true}); err != nil {
					t.Fatal(err)
				}
			}
			if reference == "open" || reference == "aborted" || reference == "expired" || reference == "completed" || reference == "other_channel" || reference == "uploading_session" {
				alias := uuid.New()
				state := reference
				if state == "other_channel" || state == "uploading_session" {
					state = "open"
				}
				if _, err = f.pool.Exec(ctx, "INSERT INTO upload_sessions(id,user_id,name,expected_size,mod_time,encryption,conflict_policy,part_size,state,expires_at,file_id,completed_at) VALUES($1,1001,'independent',4,now(),false,'fail',4,$2::upload_state,now()+interval '1 day',CASE WHEN $2::upload_state='completed' THEN $3::uuid END,CASE WHEN $2::upload_state='completed' THEN now() END)", alias, state, f.source); err != nil {
					t.Fatal(err)
				}
				if _, err = f.pool.Exec(ctx, "INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,state,plain_size,stored_size,checksum,block_hashes) VALUES($1,1,$2,$3,'stored',4,4,$4,$5)", alias, channel, part.MessageID.Int64, part.Checksum.String, part.BlockHashes); err != nil {
					t.Fatal(err)
				}
				if reference == "uploading_session" {
					if _, err = f.pool.Exec(ctx, "UPDATE upload_parts SET state='uploading',lease_token=$2,lease_expires_at=clock_timestamp()+interval '1 minute' WHERE upload_id=$1", alias, uuid.New()); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err = f.catalog.Trash(ctx, 1001, fileID); err != nil {
				t.Fatal(err)
			}
			if err = f.service.QueuePurge(ctx, 1001, fileID); err != nil {
				t.Fatal(err)
			}
			referencesBefore := fileopsIndependentReferences(t, f.pool)
			err = f.service.PurgeMany(ctx, 1001, []uuid.UUID{fileID})
			eligible := reference == "owned" || reference == "other_channel"
			if eligible && err != nil {
				t.Fatal(err)
			}
			if !eligible && err == nil {
				t.Fatal("independent reference not reported")
			}
			_, exists := f.storage.Bytes(part.ChannelID, part.MessageID.Int64)
			if exists == eligible {
				t.Fatalf("physical result exists=%v eligible=%v", exists, eligible)
			}
			var events int
			if e := f.pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE resource_id=$1 AND event_type='file.purged'", fileID.String()).Scan(&events); e != nil {
				t.Fatal(e)
			}
			want := 0
			if eligible {
				want = 1
			}
			if events != want {
				t.Fatalf("purged events=%d want=%d", events, want)
			}
			if _, e := service.GetPart(ctx, 1001, id, 1); e == nil {
				t.Fatal("completed proof returned after trash/purge")
			}
			if _, e := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: id, PartNo: 1, PlainSize: int64(len(f.body)), Body: bytes.NewReader(f.body)}); e == nil {
				t.Fatal("post-purge replay revived result")
			}
			f.preserve(t)
			if after := fileopsIndependentReferences(t, f.pool); after != referencesBefore {
				t.Fatal("independent session/file reference evidence changed")
			}
		})
	}
}

func TestB2OrdinaryTerminalUploadPurge(t *testing.T) {
	for _, state := range []string{"aborted", "expired"} {
		for _, entry := range []string{"direct", "ancestor", "clean"} {
			t.Run(state+"/"+entry, func(t *testing.T) {
				f := newCopyFixture(t)
				ctx := context.Background()
				service := uploads.NewService(f.pool)
				if err := service.BindLifecycle(f.ledger); err != nil {
					t.Fatal(err)
				}
				session, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: "empty-terminal", ExpectedSize: 0})
				if err != nil {
					t.Fatal(err)
				}
				if state == "aborted" {
					if _, err = service.Abort(ctx, 1001, uuid.UUID(session.ID.Bytes)); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err = f.pool.Exec(ctx, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", session.ID); err != nil {
						t.Fatal(err)
					}
					if _, err = sqlcgen.New(f.pool).ExpireUploadSessions(ctx, dbtypes.Time(time.Now())); err != nil {
						t.Fatal(err)
					}
				}
				var before string
				if err = f.pool.QueryRow(ctx, "SELECT (to_jsonb(s)-'parent_id'-'updated_at')::text FROM upload_sessions s WHERE id=$1 AND state=$2::upload_state AND terminal_failure_code IS NULL", session.ID, state).Scan(&before); err != nil {
					t.Fatalf("ordinary terminal state absent: %v", err)
				}
				target := f.parent
				if entry != "direct" {
					target = f.ancestor
				}
				if _, err = f.catalog.Trash(ctx, 1001, target); err != nil {
					t.Fatal(err)
				}
				if entry == "clean" {
					if _, err = f.service.CleanTrash(ctx, 1001); err != nil {
						t.Fatal(err)
					}
				} else if err = f.service.QueuePurge(ctx, 1001, target); err != nil {
					t.Fatal(err)
				}
				if err = f.service.PurgeMany(ctx, 1001, []uuid.UUID{target}); err != nil {
					t.Fatal(err)
				}
				var after string
				var parent *uuid.UUID
				var remaining, parts int
				if err = f.pool.QueryRow(ctx, "SELECT (to_jsonb(s)-'parent_id'-'updated_at')::text,parent_id,(SELECT count(*) FROM files WHERE id=ANY($2::uuid[])),(SELECT count(*) FROM upload_parts WHERE upload_id=s.id) FROM upload_sessions s WHERE id=$1", session.ID, []uuid.UUID{target, f.parent}).Scan(&after, &parent, &remaining, &parts); err != nil {
					t.Fatal(err)
				}
				if after != before || parent != nil || remaining != 0 || parts != 0 {
					t.Fatalf("terminal evidence or physical purge changed: equal=%v parent=%v files=%d parts=%d", after == before, parent, remaining, parts)
				}
				f.preserve(t)
			})
		}
	}
}

func TestB2PurgeSessionParents(t *testing.T) {
	for _, entry := range []string{"queue_direct", "queue_ancestor", "clean", "many_direct", "many_ancestor"} {
		t.Run(entry, func(t *testing.T) {
			f := newCopyFixture(t)
			ctx := context.Background()
			service := uploads.NewService(f.pool)
			if err := service.BindLifecycle(f.ledger); err != nil {
				t.Fatal(err)
			}
			session, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: "zero", ExpectedSize: 0})
			if err != nil {
				t.Fatal(err)
			}
			leased, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: "leased", ExpectedSize: 4})
			if err != nil {
				t.Fatal(err)
			}
			claim, err := service.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: uuid.UUID(leased.ID.Bytes), PartNo: 1, ChannelID: 9001, PlainSize: 4})
			if err != nil {
				t.Fatal(err)
			}
			if claim.LeaseToken == uuid.Nil {
				t.Fatal("actual active lease absent")
			}
			batch := uuid.New()
			tx, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			pins, err := service.PinImportDestinationsTx(ctx, tx, uploads.PinImportDestinationsInput{UserID: 1001, BatchID: batch, Destination: f.parent.String()})
			if err != nil {
				t.Fatal(err)
			}
			var jobID int64
			if err = tx.QueryRow(ctx, "INSERT INTO fileops_receipt_jobs(user_id) VALUES(1001) RETURNING id").Scan(&jobID); err != nil {
				t.Fatal(err)
			}
			if err = service.InsertImportBatchAdmissionTx(ctx, tx, uploads.InsertImportBatchAdmissionInput{UserID: 1001, BatchID: batch, OriginJobID: jobID, RequestFingerprint: sha256.Sum256([]byte("Q-only")), Pins: pins}); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			dispatched, attempt := uuid.New(), uuid.New()
			tx, err = f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			pins, err = service.PinImportDestinationsTx(ctx, tx, uploads.PinImportDestinationsInput{UserID: 1001, BatchID: dispatched, Destination: f.parent.String(), SourceDestinations: []string{""}})
			if err != nil {
				t.Fatal(err)
			}
			var dispatchedJob, childJob int64
			if err = tx.QueryRow(ctx, "INSERT INTO fileops_receipt_jobs(user_id) VALUES(1001) RETURNING id").Scan(&dispatchedJob); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256([]byte("A without session"))
			if err = service.InsertImportBatchAdmissionTx(ctx, tx, uploads.InsertImportBatchAdmissionInput{UserID: 1001, BatchID: dispatched, OriginJobID: dispatchedJob, RequestFingerprint: digest, Pins: pins}); err != nil {
				t.Fatal(err)
			}
			destinations, err := service.ReserveImportBatchTx(ctx, tx, uploads.ReserveImportBatchInput{UserID: 1001, BatchID: dispatched, OriginJobID: dispatchedJob, RequestFingerprint: digest, Destination: f.parent.String(), SourceDestinations: []string{""}, Sources: []uploads.ImportSourceDestination{{AttemptID: attempt, SourceIndex: 0, DestinationOrdinal: 0}}})
			if err != nil {
				t.Fatal(err)
			}
			if err = tx.QueryRow(ctx, "INSERT INTO fileops_receipt_jobs(user_id) VALUES(1001) RETURNING id").Scan(&childJob); err != nil {
				t.Fatal(err)
			}
			if err = service.RecordImportChildrenTx(ctx, tx, uploads.RecordImportChildrenInput{UserID: 1001, BatchID: dispatched, OriginJobID: dispatchedJob, Destinations: destinations, Children: []uploads.ImportChild{{AttemptID: attempt, SourceIndex: 0, JobID: childJob}}}); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			// Seed a pre-trashed hierarchy without simulating terminalization, so each entry point owns the fence.
			if _, err = f.pool.Exec(ctx, "UPDATE files SET status='trashed',deleted_at=now() WHERE id IN($1,$2)", f.ancestor, f.parent); err != nil {
				t.Fatal(err)
			}
			target := f.ancestor
			if strings.HasSuffix(entry, "direct") {
				target = f.parent
			}
			switch {
			case strings.HasPrefix(entry, "queue"):
				err = f.service.QueuePurge(ctx, 1001, target)
			case entry == "clean":
				_, err = f.service.CleanTrash(ctx, 1001)
			case strings.HasPrefix(entry, "many"):
				err = f.service.PurgeMany(ctx, 1001, []uuid.UUID{target})
			}
			if err != nil {
				t.Fatal(err)
			}
			var reason, batchReason string
			if err = f.pool.QueryRow(ctx, "SELECT terminal_failure_code FROM upload_sessions WHERE id=$1", session.ID).Scan(&reason); err != nil || reason != "upload_parent_unavailable" {
				t.Fatalf("session terminal %s %v", reason, err)
			}
			if err = f.pool.QueryRow(ctx, "SELECT terminal_failure_code FROM import_batches WHERE batch_id=$1", batch).Scan(&batchReason); err != nil || batchReason != "upload_parent_unavailable" {
				t.Fatalf("Q terminal %s %v", batchReason, err)
			}
			if err = f.pool.QueryRow(ctx, "SELECT terminal_failure_code FROM upload_sessions WHERE id=$1", leased.ID).Scan(&reason); err != nil || reason != "upload_parent_unavailable" {
				t.Fatalf("leased session not fenced: %s %v", reason, err)
			}
			var immutableParent uuid.UUID
			if err = f.pool.QueryRow(ctx, "SELECT terminal_failure_code,parent_id FROM import_job_attempts WHERE attempt_id=$1", attempt).Scan(&reason, &immutableParent); err != nil || reason != "upload_parent_unavailable" || immutableParent != f.parent {
				t.Fatalf("A-only source not permanently fenced: %s %v", reason, err)
			}
			if !strings.HasPrefix(entry, "many") {
				if err = f.service.PurgeMany(ctx, 1001, []uuid.UUID{f.ancestor}); err != nil {
					t.Fatal(err)
				}
			}
			var parent *uuid.UUID
			if err = f.pool.QueryRow(ctx, "SELECT parent_id FROM upload_sessions WHERE id=$1", session.ID).Scan(&parent); err != nil || parent != nil {
				t.Fatalf("terminal parent not detached %v %v", parent, err)
			}
			if _, err = service.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: uuid.UUID(session.ID.Bytes), Size: 0}); !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("zero-byte loser published %v", err)
			}
			recreated, err := f.catalog.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "ancestor"})
			if err != nil {
				t.Fatal(err)
			}
			recreatedID := uuid.UUID(recreated.ID.Bytes)
			if _, err = f.catalog.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, ParentID: &recreatedID, Name: "destination"}); err != nil {
				t.Fatal(err)
			}
			tx, err = f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err = service.ReserveImportBatchTx(ctx, tx, uploads.ReserveImportBatchInput{UserID: 1001, BatchID: batch, OriginJobID: jobID, Destination: f.parent.String(), RequestFingerprint: sha256.Sum256([]byte("Q-only"))}); !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("Q-only terminal batch redispatched: %v", err)
			}
			f.preserve(t)
		})
	}
	t.Run("unknown_reason_retains_parent", func(t *testing.T) {
		f := newCopyFixture(t)
		ctx := context.Background()
		service := uploads.NewService(f.pool)
		if err := service.BindLifecycle(f.ledger); err != nil {
			t.Fatal(err)
		}
		session, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: "unknown", ExpectedSize: 0})
		if err != nil {
			t.Fatal(err)
		}
		// Supplemental unknown-reason fixture; genuine old detach/restore proof belongs to03-12.
		if _, err = f.pool.Exec(ctx, "UPDATE upload_sessions SET terminal_failure_code='upload_destination_unknown' WHERE id=$1", session.ID); err != nil {
			t.Fatal(err)
		}
		var before string
		if err = f.pool.QueryRow(ctx, "SELECT to_jsonb(s)::text FROM upload_sessions s WHERE id=$1", session.ID).Scan(&before); err != nil {
			t.Fatal(err)
		}
		if _, err = f.catalog.Trash(ctx, 1001, f.parent); err != nil {
			t.Fatal(err)
		}
		if err = f.service.QueuePurge(ctx, 1001, f.parent); err != nil {
			t.Fatal(err)
		}
		if err = f.service.PurgeMany(ctx, 1001, []uuid.UUID{f.parent}); !errors.Is(err, ErrPurgeIncomplete) {
			t.Fatalf("unknown parent detached: %v", err)
		}
		var after string
		if err = f.pool.QueryRow(ctx, "SELECT to_jsonb(s)::text FROM upload_sessions s WHERE id=$1", session.ID).Scan(&after); err != nil || after != before {
			t.Fatal("unknown session evidence changed")
		}
		f.preserve(t)
	})
}

func TestB2ConservativePurge(t *testing.T) {
	for _, kind := range []string{"unknown", "protected", "failed", "exhausted", "ack_lost"} {
		t.Run(kind, func(t *testing.T) {
			f := newCopyFixture(t)
			ctx := context.Background()
			_, _, _, good, _ := f.upload(t)
			goodID := uuid.UUID(good.ID.Bytes)
			bad := f.source
			if kind != "unknown" {
				_, _, _, file, _ := f.uploadOn(t, "retained.bin", 9002)
				bad = uuid.UUID(file.ID.Bytes)
			}
			if _, err := f.catalog.Trash(ctx, 1001, bad); err != nil {
				t.Fatal(err)
			}
			if _, err := f.catalog.Trash(ctx, 1001, goodID); err != nil {
				t.Fatal(err)
			}
			if kind == "protected" {
				independent := uuid.New()
				if _, err := f.pool.Exec(ctx, "INSERT INTO upload_sessions(id,user_id,name,expected_size,mod_time,part_size,expires_at) VALUES($1,1002,'independent',4,now(),4,now()+interval '1 hour')", independent); err != nil {
					t.Fatal(err)
				}
				if _, err := f.pool.Exec(ctx, "INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size) SELECT $1,1,channel_id,message_id,plain_size FROM file_parts WHERE file_id=$2", independent, bad); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "failed" {
				// Fail only the retained physical group; the other group must still progress.
				f.storage.deleteChannel = 9002
				f.storage.deleteError = errors.New("delete failed")
				if err := f.service.PurgeMany(ctx, 1001, []uuid.UUID{bad}); err == nil {
					t.Fatal("failed deletion acknowledged")
				}
			}
			if kind == "ack_lost" {
				// The remote ACK succeeds; an actual SQL constraint rejects its durable ACK.
				if _, err := f.pool.Exec(ctx, "ALTER TABLE remote_write_receipts ADD CONSTRAINT reject_fileops_ack CHECK (user_id<>1001 OR state<>'deleted')"); err != nil {
					t.Fatal(err)
				}
				if err := f.service.PurgeMany(ctx, 1001, []uuid.UUID{bad}); err == nil {
					t.Fatal("lost durable ACK acknowledged")
				}
				if _, err := f.pool.Exec(ctx, "ALTER TABLE remote_write_receipts DROP CONSTRAINT reject_fileops_ack"); err != nil {
					t.Fatal(err)
				}
				if _, err := f.pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE result_file_id=$1 AND row_kind='message_attempt'", bad); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "exhausted" {
				for range 4 {
					f.storage.SetDeleteFaults(fixture.DeleteFaults{Error: errors.New("delete failed")})
					if err := f.service.PurgeMany(ctx, 1001, []uuid.UUID{bad}); err == nil {
						t.Fatal("failed attempt acknowledged")
					}
					if _, err := f.pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE result_file_id=$1 AND row_kind='message_attempt'", bad); err != nil {
						t.Fatal(err)
					}
				}
				f.storage.SetDeleteFaults(fixture.DeleteFaults{})
			}
			// A fresh service consumes retained identity/budget, never a process-local retry count.
			restarted, err := NewService(f.pool, f.catalog, channels.NewService(f.pool, nil, channels.Config{}), f.storage)
			if err != nil {
				t.Fatal(err)
			}
			if err = restarted.BindLifecycle(f.ledger); err != nil {
				t.Fatal(err)
			}
			err = restarted.PurgeMany(ctx, 1001, []uuid.UUID{bad, goodID})
			retained := kind == "unknown" || kind == "protected" || kind == "exhausted" || kind == "failed"
			if retained && err == nil {
				t.Fatal("retained evidence not reported")
			}
			if !retained && err != nil {
				t.Fatal(err)
			}
			if _, e := f.catalog.Get(ctx, 1001, goodID); !errors.Is(e, catalog.ErrNotFound) {
				t.Fatalf("eligible sibling starved: %v; aggregate %v", e, err)
			}
			if retained {
				if _, e := f.catalog.Get(ctx, 1001, bad); e != nil {
					t.Fatalf("retained catalog erased: %v", e)
				}
			}
			if kind == "exhausted" {
				var attempts int
				if e := f.pool.QueryRow(ctx, "SELECT attempts FROM remote_write_receipts WHERE result_file_id=$1 AND row_kind='message_attempt'", bad).Scan(&attempts); e != nil || attempts != 4 {
					t.Fatalf("lifetime attempts=%d %v", attempts, e)
				}
			}
			if kind == "failed" {
				f.storage.deleteError = nil
				if e := restarted.PurgeMany(ctx, 1001, []uuid.UUID{bad}); e != nil {
					t.Fatalf("retained failed subset could not recover: %v", e)
				}
			}
			if !retained {
				if e := restarted.PurgeMany(ctx, 1001, []uuid.UUID{bad, goodID}); e != nil {
					t.Fatalf("idempotent purge %v", e)
				}
			}
			f.preserve(t)
		})
	}
}

func (f *copyFixture) uploadNamed(t *testing.T, name string) (*uploads.Service, *transfer.Pipeline, uuid.UUID, *sqlcgen.File, *sqlcgen.UploadPart) {
	return f.uploadOn(t, name, 0)
}

func (f *copyFixture) uploadOn(t *testing.T, name string, channel int64) (*uploads.Service, *transfer.Pipeline, uuid.UUID, *sqlcgen.File, *sqlcgen.UploadPart) {
	t.Helper()
	ctx := context.Background()
	service := uploads.NewService(f.pool)
	if err := service.BindLifecycle(f.ledger); err != nil {
		t.Fatal(err)
	}
	session, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: name, ExpectedSize: int64(len(f.body))})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.UUID(session.ID.Bytes)
	pipeline := transfer.NewPipeline(service, channels.NewService(f.pool, nil, channels.Config{}), f.storage, nil, transfer.Config{Receipts: f.ledger})
	put, err := pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: id, PartNo: 1, RequestedChannelID: channel, PlainSize: int64(len(f.body)), Body: bytes.NewReader(f.body)})
	if err != nil {
		t.Fatal(err)
	}
	file, err := service.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: id, Size: int64(len(f.body))})
	if err != nil {
		t.Fatal(err)
	}
	return service, pipeline, id, file, put.Part
}

func fileopsIndependentReferences(t testing.TB, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	err := pool.QueryRow(context.Background(), `SELECT jsonb_build_object(
 'sessions',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) FROM upload_sessions s WHERE s.name='independent'),
 'upload_parts',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.upload_id,p.part_no) FROM upload_parts p JOIN upload_sessions s ON s.id=p.upload_id WHERE s.name='independent'),
 'files',(SELECT jsonb_agg(to_jsonb(f) ORDER BY f.id) FROM files f WHERE f.name='independent'),
 'file_parts',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.file_id,p.part_no) FROM file_parts p JOIN files f ON f.id=p.file_id WHERE f.name='independent'))::text`).Scan(&snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

type copyBudgetTracer struct {
	mu        sync.Mutex
	deadlines map[string]time.Time
	invalid   bool
}

func (tr *copyBudgetTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	for _, name := range []string{"ObserveMessageReceipt", "StageReceiptCleanup", "ClaimReceiptDeletion", "AcknowledgeReceiptDeletion"} {
		if strings.Contains(data.SQL, "-- name: "+name) {
			deadline, ok := ctx.Deadline()
			tr.mu.Lock()
			if !ok {
				tr.invalid = true
			}
			if tr.deadlines == nil {
				tr.deadlines = make(map[string]time.Time)
			}
			if previous, exists := tr.deadlines[name]; exists && !previous.Equal(deadline) {
				tr.invalid = true
			}
			tr.deadlines[name] = deadline
			tr.mu.Unlock()
		}
	}
	return ctx
}
func (*copyBudgetTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (tr *copyBudgetTracer) assert(t testing.TB) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.invalid || len(tr.deadlines) != 4 {
		t.Fatalf("cleanup deadline phases=%d invalid=%v", len(tr.deadlines), tr.invalid)
	}
	var first time.Time
	for _, deadline := range tr.deadlines {
		if first.IsZero() {
			first = deadline
		}
		if !deadline.Equal(first) {
			t.Fatal("Observe/stage/claim/ACK reset cleanup budget")
		}
	}
}

func attachCopyBudget(t *testing.T, f *copyFixture) *copyBudgetTracer {
	t.Helper()
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(f.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	trace := &copyBudgetTracer{}
	config.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	policy := lifecycle.NewService(pool, f.storage)
	if err = policy.BindReceiptEnqueuer(copyQueue{}); err != nil {
		t.Fatal(err)
	}
	hierarchy := catalog.NewService(pool, nil)
	if err = hierarchy.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(pool, hierarchy, channels.NewService(pool, nil, channels.Config{}), f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	f.service = service
	f.catalog = hierarchy
	f.ledger = policy
	return trace
}

func fileopsCopyIdentities(t testing.TB, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(context.Background(), `SELECT jsonb_agg(jsonb_build_array(id,operation_id,result_file_id,channel_id,message_id,lease_token) ORDER BY id)::text FROM remote_write_receipts`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
