//go:build integration

package uploads

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
)

type b2DomainQueue struct{}

func (b2DomainQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, "INSERT INTO b2_domain_jobs(user_id,receipt_ids) VALUES ($1,$2) RETURNING id", in.UserID, in.ReceiptIDs).Scan(&id)
	return id, err
}

type b2Domain struct {
	pool    *pgxpool.Pool
	service *Service
	ledger  *lifecycle.Service
	storage *fixture.Storage
}

func newB2Domain(t *testing.T) *b2Domain {
	t.Helper()
	db := testpostgres.New(t)
	ctx := context.Background()
	for _, sql := range []string{"INSERT INTO users(user_id) VALUES (1001),(1002)", "INSERT INTO channels(channel_id,user_id,name,selected) VALUES (9001,1001,'fixture',true)", "CREATE TABLE b2_domain_jobs(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,user_id bigint NOT NULL,receipt_ids uuid[] NOT NULL)"} {
		if _, err := db.Pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	storage := fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}})
	for _, m := range []fixture.Message{{UserID: 1001, ChannelID: 9001, MessageID: 80, Bytes: []byte("retained original source"), Preserve: true}, {UserID: 1002, ChannelID: 9001, MessageID: 81, Bytes: []byte("retained old destination/share"), Preserve: true}} {
		if err := storage.Seed(m); err != nil {
			t.Fatal(err)
		}
	}
	oldID, shareID := uuid.New(), uuid.New()
	if _, err := db.Pool.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,status,size,mod_time) VALUES ($1,1001,'old.bin','file','active',30,now())", oldID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size) VALUES ($1,1,9001,81,30,30)", oldID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, "INSERT INTO file_shares(id,file_id,owner_id,token_prefix,token_hash) VALUES ($1,$2,1001,'b2',$3)", shareID, oldID, []byte(shareID.String())); err != nil {
		t.Fatal(err)
	}
	ledger := lifecycle.NewService(db.Pool, storage)
	if err := ledger.BindReceiptEnqueuer(b2DomainQueue{}); err != nil {
		t.Fatal(err)
	}
	service := NewService(db.Pool)
	if err := service.BindLifecycle(ledger); err != nil {
		t.Fatal(err)
	}
	return &b2Domain{db.Pool, service, ledger, storage}
}
func (f *b2Domain) create(t *testing.T, in CreateInput) uuid.UUID {
	t.Helper()
	session, err := f.service.Create(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return uuid.UUID(session.ID.Bytes)
}
func (f *b2Domain) store(t *testing.T, id uuid.UUID, body []byte) (*sqlcgen.UploadPart, error, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	claim, err := f.service.ClaimPart(ctx, ClaimPartInput{UserID: 1001, UploadID: id, PartNo: 1, ChannelID: 9001, PlainSize: int64(len(body))})
	if err != nil {
		t.Fatal(err)
	}
	rid := uuid.New()
	if _, err = f.ledger.Reserve(ctx, lifecycle.ReserveInput{ID: rid, UserID: 1001, ChannelID: 9001, OriginKind: "upload", UploadID: &id, PartNo: 1, LeaseToken: claim.LeaseToken, LeaseExpiresAt: claim.Part.LeaseExpiresAt.Time}); err != nil {
		t.Fatal(err)
	}
	remote, err := f.storage.Upload(ctx, telegramstore.UploadRequest{UserID: 1001, ChannelID: 9001, Reader: bytes.NewReader(body), Size: int64(len(body))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.ledger.Observe(ctx, lifecycle.ObserveInput{ReceiptID: rid, UserID: 1001, ChannelID: 9001, MessageID: remote.MessageID, LeaseToken: claim.LeaseToken, UploadID: &id, PartNo: 1}); err != nil {
		t.Fatal(err)
	}
	hasher := treehash.NewBlockHasher()
	_, _ = hasher.Write(body)
	blocks := hasher.Sum()
	part, err := f.service.StorePart(ctx, StorePartInput{UploadID: id, PartNo: 1, LeaseToken: claim.LeaseToken, ReceiptID: rid, MessageID: remote.MessageID, StoredSize: remote.Size, Checksum: treehash.SumToHex(treehash.ComputeTreeHash(blocks)), BlockHashes: blocks})
	return part, err, rid
}
func b2DomainSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var value string
	err := pool.QueryRow(context.Background(), `SELECT jsonb_build_array(
	(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM upload_sessions s),
	(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY upload_id,part_no),'[]') FROM upload_parts p),
	(SELECT COALESCE(jsonb_agg(to_jsonb(f) ORDER BY id),'[]') FROM files f),
	(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY file_id,part_no),'[]') FROM file_parts p),
	(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM file_shares s),
	(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM remote_write_receipts r),
	(SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY id),'[]') FROM user_events e),
	(SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY id),'[]') FROM request_admissions a))::text`).Scan(&value)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func b2DomainBytes(t *testing.T, f *b2Domain, part *sqlcgen.UploadPart, body []byte) {
	t.Helper()
	reader, err := f.storage.OpenRange(context.Background(), telegramstore.RangeRequest{UserID: 1001, ChannelID: part.ChannelID, MessageID: part.MessageID.Int64, Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil || sha256.Sum256(got) != sha256.Sum256(body) {
		t.Fatalf("independent bytes: %v", err)
	}
	if err := f.storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
}
func b2LostCommit(t *testing.T) func(context.Context, pgx.Tx) error {
	t.Helper()
	after := fixture.NewBarrier(t, fixture.ErrResponseLost)
	after.Release()
	return (fixture.Hooks{BindCommitted: after}).CommitBind
}
func b2NormalCommit(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) }

func TestB2WinnerNotCompensated(t *testing.T) {
	for _, boundary := range []string{"store_lost_ack", "complete_lost_ack", "store_rollback", "complete_rollback", "reread_failure"} {
		t.Run(boundary, func(t *testing.T) {
			f := newB2Domain(t)
			ctx := context.Background()
			body := []byte("durable exact bytes")
			id := f.create(t, CreateInput{UserID: 1001, Name: "winner.bin", ExpectedSize: int64(len(body)), PartSize: treehash.BlockSize})
			if boundary == "store_lost_ack" || boundary == "reread_failure" {
				f.service.commit = b2LostCommit(t)
			}
			if boundary == "store_rollback" {
				before := fixture.NewBarrier(t, fixture.ErrProcessLost)
				before.Release()
				f.service.commit = (fixture.Hooks{BindBeforeCommit: before}).CommitBind
			}
			part, err, rid := f.store(t, id, body)
			if boundary == "store_rollback" {
				if !errors.Is(err, fixture.ErrProcessLost) {
					t.Fatalf("rollback: %v", err)
				}
				var count int
				if e := f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1 AND state='stored'", id).Scan(&count); e != nil || count != 0 {
					t.Fatalf("rollback stored winner: %d %v", count, e)
				}
				return
			}
			if boundary == "store_lost_ack" || boundary == "reread_failure" {
				if !errors.Is(err, fixture.ErrResponseLost) {
					t.Fatalf("commit loss: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			f.service.commit = b2NormalCommit
			part, err = NewService(f.pool).GetPart(ctx, 1001, id, 1)
			if err != nil {
				t.Fatal(err)
			}
			b2DomainBytes(t, f, part, body)
			if boundary == "complete_lost_ack" {
				f.service.commit = b2LostCommit(t)
			}
			if boundary == "complete_rollback" {
				before := fixture.NewBarrier(t, fixture.ErrProcessLost)
				before.Release()
				f.service.commit = (fixture.Hooks{BindBeforeCommit: before}).CommitBind
			}
			file, err := f.service.Complete(ctx, CompleteInput{UserID: 1001, UploadID: id, Size: int64(len(body))})
			if boundary == "complete_rollback" {
				if !errors.Is(err, fixture.ErrProcessLost) {
					t.Fatalf("completion rollback: %v", err)
				}
				var count int
				if e := f.pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE name='winner.bin'").Scan(&count); e != nil || count != 0 {
					t.Fatalf("rollback published: %d %v", count, e)
				}
				b2DomainBytes(t, f, part, body)
				return
			}
			if boundary == "complete_lost_ack" {
				if !errors.Is(err, fixture.ErrResponseLost) {
					t.Fatalf("completion ACK: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			f.service.commit = b2NormalCommit
			file, err = f.service.Complete(ctx, CompleteInput{UserID: 1001, UploadID: id, Size: int64(len(body))})
			if err != nil {
				t.Fatal(err)
			}
			var published, completed int
			if err = f.pool.QueryRow(ctx, "SELECT count(*) FILTER (WHERE event_type='file.created'),count(*) FILTER (WHERE event_type='upload.completed') FROM user_events WHERE resource_id IN ($1,$2)", uuid.UUID(file.ID.Bytes).String(), id.String()).Scan(&published, &completed); err != nil || published != 1 || completed != 1 {
				t.Fatalf("publication event delta=%d/%d %v", published, completed, err)
			}
			before := b2DomainSnapshot(t, f.pool)
			again, err := f.service.Complete(ctx, CompleteInput{UserID: 1001, UploadID: id, Size: int64(len(body))})
			if err != nil || again.ID != file.ID || again.Generation != file.Generation {
				t.Fatalf("idempotent current result: %+v %v", again, err)
			}
			if boundary == "reread_failure" {
				// The failed read uses an independently canceled context, never a fabricated SQL success.
				dead, cancel := context.WithCancel(ctx)
				cancel()
				if _, e := f.service.GetPart(dead, 1001, id, 1); e == nil {
					t.Fatal("unavailable read accepted")
				}
			}
			tx, e := f.pool.Begin(ctx)
			if e != nil {
				t.Fatal(e)
			}
			e = f.ledger.StageCleanupTx(ctx, tx, lifecycle.StageCleanupInput{UserID: 1001, ReceiptIDs: []uuid.UUID{rid}, PrimaryCode: "late_failure"})
			if e == nil {
				e = tx.Commit(ctx)
			}
			_ = tx.Rollback(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if e = f.ledger.Recover(ctx, 1001, []uuid.UUID{rid}); e != nil {
				t.Fatal(e)
			}
			if len(f.storage.Deletes()) != 0 || b2DomainSnapshot(t, f.pool) != before {
				t.Fatal("late compensation changed winner/events")
			}
			b2DomainBytes(t, f, part, body)
		})
	}
}

func TestB2UploadAdmissionDomain(t *testing.T) {
	for _, scenario := range []string{"create_lost_ack", "parent_delete_restore", "truncated_terminal_reply", "terminal_retention", "missing_session", "expired_session", "fingerprint_mismatch", "completed_success", "completed_current_result", "unknown_destination"} {
		t.Run(scenario, func(t *testing.T) {
			f := newB2Domain(t)
			ctx := context.Background()
			owner := int64(1001)
			parent := uuid.New()
			if _, err := f.pool.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,status,mod_time) VALUES ($1,1001,'parent','folder','active',now())", parent); err != nil {
				t.Fatal(err)
			}
			admission := lifecycle.RequestAdmissionInput{Scope: lifecycle.RequestScope{ActorUserID: &owner}, RequestKey: uuid.New(), RequestFingerprint: sha256.Sum256([]byte("original request geometry"))}
			in := CreateInput{UserID: 1001, ParentID: &parent, Name: "admitted.bin", ExpectedSize: 0, PartSize: treehash.BlockSize, Admission: &admission}
			if scenario == "create_lost_ack" {
				f.service.commit = b2LostCommit(t)
			}
			session, err := f.service.Create(ctx, in)
			if err != nil && scenario != "create_lost_ack" {
				t.Fatal(err)
			}
			f.service.commit = b2NormalCommit
			binding, err := f.ledger.GetRequestAdmission(ctx, admission.Scope, "upload", admission.RequestKey)
			if err != nil {
				t.Fatal(err)
			}
			id := uuid.UUID(binding.UploadID.Bytes)
			if session != nil && session.ID != binding.UploadID {
				t.Fatal("create returned replacement")
			}
			original, err := f.service.Get(ctx, owner, id)
			if err != nil {
				t.Fatal(err)
			}
			want := error(nil)
			switch scenario {
			case "parent_delete_restore", "truncated_terminal_reply", "terminal_retention":
				hierarchy := catalog.NewService(f.pool, nil)
				if err = hierarchy.BindLifecycle(f.ledger); err != nil {
					t.Fatal(err)
				}
				if _, err = hierarchy.Trash(ctx, owner, parent); err != nil {
					t.Fatal(err)
				}
				if _, err = hierarchy.Restore(ctx, owner, parent); err != nil {
					t.Fatal(err)
				}
				want = ErrParentUnavailable
				if scenario == "terminal_retention" {
					before := b2DomainSnapshot(t, f.pool)
					if _, err = f.pool.Exec(ctx, "DELETE FROM upload_sessions WHERE id=$1", id); err == nil {
						t.Fatal("terminal session tombstone removed")
					}
					cutoff, cutoffErr := sqlcgen.New(f.pool).GetCleanupSweepCutoff(ctx)
					if cutoffErr != nil {
						t.Fatal(cutoffErr)
					}
					if _, err = sqlcgen.New(f.pool).ExpireUploadSessions(ctx, cutoff); err != nil {
						t.Fatal(err)
					}
					if b2DomainSnapshot(t, f.pool) != before {
						t.Fatal("cleanup erased terminal admission evidence")
					}
				}
			case "missing_session":
				if _, err = f.pool.Exec(ctx, "DELETE FROM upload_sessions WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				want = ErrNotFound
			case "expired_session":
				if _, err = f.pool.Exec(ctx, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				want = ErrExpired
			case "fingerprint_mismatch":
				changed := admission
				changed.RequestFingerprint = sha256.Sum256([]byte("different request"))
				in.Admission = &changed
				want = lifecycle.ErrRequestMismatch
			case "completed_success", "completed_current_result":
				file, e := f.service.Complete(ctx, CompleteInput{UserID: owner, UploadID: id, Size: 0})
				if e != nil {
					t.Fatal(e)
				}
				if scenario == "completed_current_result" {
					hierarchy := catalog.NewService(f.pool, nil)
					if e = hierarchy.BindLifecycle(f.ledger); e != nil {
						t.Fatal(e)
					}
					if _, e = hierarchy.Trash(ctx, owner, uuid.UUID(file.ID.Bytes)); e != nil {
						t.Fatal(e)
					}
					want = ErrNotFound
				}
			case "unknown_destination":
				if _, err = f.pool.Exec(ctx, "UPDATE upload_sessions SET terminal_failure_code='upload_destination_unknown' WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				want = ErrParentUnavailable
				before := b2DomainSnapshot(t, f.pool)
				raw, e := f.service.GetAnyOwner(ctx, id)
				if e != nil || raw.TerminalFailureCode.String != "upload_destination_unknown" {
					t.Fatalf("raw authorization hint: %+v %v", raw, e)
				}
				token := uuid.New()
				checks := []func() error{
					func() error { _, e := f.service.Get(ctx, owner, id); return e },
					func() error { _, e := f.service.Create(ctx, CreateInput{ID: id, UserID: owner}); return e },
					func() error {
						_, e := f.service.ClaimPart(ctx, ClaimPartInput{UserID: owner, UploadID: id, PartNo: 1, ChannelID: 9001, PlainSize: 1})
						return e
					},
					func() error {
						_, e := f.service.StorePart(ctx, StorePartInput{UploadID: id, PartNo: 1, LeaseToken: token, MessageID: 99, StoredSize: 1})
						return e
					},
					func() error {
						return f.service.RenewPart(ctx, RenewPartInput{UploadID: id, PartNo: 1, LeaseToken: token})
					},
					func() error {
						_, e := f.service.FailPart(ctx, FailPartInput{UploadID: id, PartNo: 1, LeaseToken: token, ErrorCode: "late_failure"})
						return e
					},
					func() error { _, e := f.service.Abort(ctx, owner, id); return e },
					func() error { _, e := f.service.ListParts(ctx, ListPartsInput{UserID: owner, UploadID: id}); return e },
				}
				for i, check := range checks {
					if e := check(); !errors.Is(e, ErrParentUnavailable) {
						t.Fatalf("unknown operational guard %d: %v", i, e)
					}
				}
				cutoff, cutoffErr := sqlcgen.New(f.pool).GetCleanupSweepCutoff(ctx)
				if cutoffErr != nil {
					t.Fatal(cutoffErr)
				}
				if _, e = sqlcgen.New(f.pool).ExpireUploadSessions(ctx, cutoff); e != nil {
					t.Fatal(e)
				}
				if b2DomainSnapshot(t, f.pool) != before {
					t.Fatal("unknown evidence/state/events changed")
				}
			}
			before := b2DomainSnapshot(t, f.pool)
			// A new default must not renegotiate an already admitted session.
			in.PartSize = 2 * treehash.BlockSize
			in.Encryption = true
			version := int32(99)
			in.EncryptionKeyVersion = &version
			restarted := NewService(f.pool, time.Hour)
			if err = restarted.BindLifecycle(f.ledger); err != nil {
				t.Fatal(err)
			}
			again, err := restarted.Create(ctx, in)
			if !errors.Is(err, want) {
				t.Fatalf("same key outcome: %+v %v, want %v", again, err, want)
			}
			if err == nil && (again.ID != original.ID || again.PartSize != original.PartSize || again.Encryption != original.Encryption || again.EncryptionKeyVersion != original.EncryptionKeyVersion || again.ExpiresAt != original.ExpiresAt) {
				t.Fatal("frozen geometry/key/session replaced")
			}
			if before != b2DomainSnapshot(t, f.pool) {
				t.Fatal("same key mutated session/file/events")
			}
			var admissions int
			if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM request_admissions").Scan(&admissions); err != nil || admissions != 1 {
				t.Fatalf("admission count=%d %v", admissions, err)
			}
			if err = f.storage.CheckPreserved(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestB2ImportAttemptAssociation(t *testing.T) {
	for _, name := range []string{"q_rollback", "q_lost_ack", "q_to_a_loss", "a_folder_rollback", "a_child_rollback", "a_lost_ack", "a_to_s_loss", "s_rollback", "s_lost_ack", "s_to_b_loss", "b_rollback", "b_lost_ack", "selected_once", "compatible_resume", "stale_candidate", "incompatible_candidate", "materialized_missing", "result_replay", "wrong_owner_job", "changed_prefix", "root_empty_dispatch", "removed_job_replay", "legacy_unknown_candidate"} {
		t.Run(name, func(t *testing.T) {
			f := newB2Domain(t)
			ctx := context.Background()
			batch, attemptID := uuid.New(), uuid.New()
			digest := sha256.Sum256([]byte("original args"))
			client, err := river.NewClient(riverpgxv5.New(f.pool), &river.Config{Schema: "teldrive", SkipUnknownJobCheck: true})
			if err != nil {
				t.Fatal(err)
			}
			hierarchy := catalog.NewService(f.pool, nil)
			if err = hierarchy.BindLifecycle(f.ledger); err != nil {
				t.Fatal(err)
			}
			qin := PinImportDestinationsInput{UserID: 1001, BatchID: batch, Destination: "/imports/new", SourceDestinations: []string{"source/file.bin"}}
			var anchor uuid.UUID
			if name == "q_to_a_loss" {
				folder, e := hierarchy.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "imports"})
				if e != nil {
					t.Fatal(e)
				}
				anchor = uuid.UUID(folder.ID.Bytes)
			}
			if name == "root_empty_dispatch" {
				qin.Destination = ""
				qin.SourceDestinations = nil
			}
			tx, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			pins, err := f.service.PinImportDestinationsTx(ctx, tx, qin)
			if err != nil {
				t.Fatal(err)
			}
			origin, err := client.InsertTx(ctx, tx, b2AssociationArgs{BatchID: batch}, nil)
			if err != nil || origin == nil || origin.Job == nil {
				t.Fatalf("actual Q job %v", err)
			}
			originID := origin.Job.ID
			if err = f.service.InsertImportBatchAdmissionTx(ctx, tx, InsertImportBatchAdmissionInput{UserID: 1001, BatchID: batch, OriginJobID: originID, RequestFingerprint: digest, Pins: pins}); err != nil {
				t.Fatal(err)
			}
			if name == "q_rollback" {
				_ = tx.Rollback(ctx)
				if _, err = f.service.GetImportBatch(ctx, 1001, batch, originID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("Q leak %v", err)
				}
				return
			}
			if name == "q_lost_ack" {
				err = b2LostCommit(t)(ctx, tx)
				if !errors.Is(err, fixture.ErrResponseLost) {
					t.Fatal(err)
				}
			} else if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if name == "q_to_a_loss" {
				if _, err = hierarchy.Trash(ctx, 1001, anchor); err != nil {
					t.Fatal(err)
				}
				if _, err = hierarchy.Restore(ctx, 1001, anchor); err != nil {
					t.Fatal(err)
				}
			}
			tx, err = f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			ain := ReserveImportBatchInput{UserID: 1001, BatchID: batch, OriginJobID: originID, RequestFingerprint: digest, Destination: qin.Destination, SourceDestinations: qin.SourceDestinations, Sources: []ImportSourceDestination{{AttemptID: attemptID, SourceIndex: 0, DestinationOrdinal: 0, Directory: "source"}}}
			if name == "root_empty_dispatch" {
				ain.Sources = nil
			}
			if name == "changed_prefix" {
				ain.SourceDestinations = []string{"other/file.bin"}
			}
			resolved, err := f.service.ReserveImportBatchTx(ctx, tx, ain)
			if name == "changed_prefix" {
				if !errors.Is(err, lifecycle.ErrRequestMismatch) {
					t.Fatalf("changed Q prefix %v", err)
				}
				return
			}
			if name == "q_to_a_loss" {
				if !errors.Is(err, ErrParentUnavailable) {
					t.Fatalf("Q restored loser %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if name == "a_folder_rollback" {
				_ = tx.Rollback(ctx)
				var n int
				if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE name='imports'").Scan(&n); err != nil || n != 0 {
					t.Fatalf("A folder leak %d %v", n, err)
				}
				return
			}
			var children []ImportChild
			var childID int64
			if len(ain.Sources) > 0 {
				inserted, e := client.InsertManyTx(ctx, tx, []river.InsertManyParams{{Args: b2AssociationArgs{BatchID: batch, AttemptID: attemptID}}})
				if e != nil || len(inserted) != 1 || inserted[0].Job == nil {
					t.Fatalf("actual A child %v", e)
				}
				childID = inserted[0].Job.ID
				children = []ImportChild{{AttemptID: attemptID, SourceIndex: 0, JobID: childID}}
			}
			if err = f.service.RecordImportChildrenTx(ctx, tx, RecordImportChildrenInput{UserID: 1001, BatchID: batch, OriginJobID: originID, Destinations: resolved, Children: children}); err != nil {
				t.Fatal(err)
			}
			if name == "a_child_rollback" {
				_ = tx.Rollback(ctx)
				if _, err = f.service.GetImportAttempt(ctx, 1001, batch, 0, childID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("A association leak %v", err)
				}
				var n int
				if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM river_job WHERE id=$1", childID).Scan(&n); err != nil || n != 0 {
					t.Fatalf("A River child leaked %d %v", n, err)
				}
				return
			}
			if name == "a_lost_ack" {
				err = b2LostCommit(t)(ctx, tx)
				if !errors.Is(err, fixture.ErrResponseLost) {
					t.Fatal(err)
				}
			} else if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if name == "root_empty_dispatch" {
				b, e := f.service.GetImportBatch(ctx, 1001, batch, originID)
				if e != nil || !b.Dispatched || !b.DestinationIsRoot.Bool || b.SourceCount.Int32 != 0 {
					t.Fatalf("root count0 %+v %v", b, e)
				}
				return
			}
			a, err := f.service.GetImportAttempt(ctx, 1001, batch, 0, childID)
			if err != nil {
				t.Fatal(err)
			}
			parent := uuid.UUID(a.ParentID.Bytes)
			if name == "wrong_owner_job" {
				if _, e := f.service.GetImportAttempt(ctx, 1002, batch, 0, childID); !errors.Is(e, ErrNotFound) {
					t.Fatal(e)
				}
				if _, e := f.service.GetImportAttempt(ctx, 1001, batch, 0, childID+1000); !errors.Is(e, ErrNotFound) {
					t.Fatal(e)
				}
				return
			}
			if name == "removed_job_replay" {
				if _, err = f.pool.Exec(ctx, "DELETE FROM river_job WHERE id=ANY($1)", []int64{originID, childID}); err != nil {
					t.Fatal(err)
				}
				replay, e := f.pool.Begin(ctx)
				if e != nil {
					t.Fatal(e)
				}
				defer replay.Rollback(ctx)
				again, e := f.service.ReserveImportBatchTx(ctx, replay, ain)
				if e != nil || !again.Dispatched || len(again.Sources) != 1 || again.Sources[0].AttemptID != attemptID {
					t.Fatalf("removed job recreated A %+v %v", again, e)
				}
				original, e := f.service.GetImportAttempt(ctx, 1001, batch, 0, childID)
				if e != nil || original.AttemptID != a.AttemptID {
					t.Fatalf("removed child lost identity %v", e)
				}
				return
			}
			lose := func() {
				t.Helper()
				if _, e := hierarchy.Trash(ctx, 1001, parent); e != nil {
					t.Fatal(e)
				}
				if _, e := hierarchy.Restore(ctx, 1001, parent); e != nil {
					t.Fatal(e)
				}
			}
			if name == "result_replay" {
				id := f.create(t, CreateInput{UserID: 1001, ParentID: &parent, Name: "matching.bin", ExpectedSize: 0})
				file, e := f.service.Complete(ctx, CompleteInput{UserID: 1001, UploadID: id, Size: 0})
				if e != nil {
					t.Fatal(e)
				}
				resultID := uuid.UUID(file.ID.Bytes)
				for range 2 {
					resultTx, e := f.pool.Begin(ctx)
					if e != nil {
						t.Fatal(e)
					}
					if _, e = hierarchy.CheckActiveImportResultTx(ctx, resultTx, catalog.CheckActiveImportResultInput{UserID: 1001, FileID: resultID, ParentID: &parent}); e != nil {
						t.Fatal(e)
					}
					selected, e := f.service.RecordImportResultTx(ctx, resultTx, RecordImportResultInput{UserID: 1001, BatchID: batch, SourceIndex: 0, JobID: childID, ResultFileID: resultID})
					if e != nil || selected.ResultFileID != file.ID || selected.UploadID.Valid {
						t.Fatalf("result S %v", e)
					}
					if e = resultTx.Commit(ctx); e != nil {
						t.Fatal(e)
					}
				}
				return
			}
			if name == "a_to_s_loss" {
				lose()
			}
			sin := ReserveImportAttemptInput{UserID: 1001, BatchID: batch, SourceIndex: 0, JobID: childID, UploadID: uuid.New(), Create: CreateInput{UserID: 1001, ParentID: &parent, Name: "file.bin", ExpectedSize: 0}}
			if name == "stale_candidate" {
				sin.Candidate = true
			}
			if name == "incompatible_candidate" {
				other := sin.Create
				other.Name = "another-source.bin"
				sin.UploadID = f.create(t, other)
				sin.Candidate = true
			}
			if name == "compatible_resume" || name == "legacy_unknown_candidate" {
				candidate := f.create(t, sin.Create)
				sin.UploadID = candidate
				sin.Candidate = true
				if name == "legacy_unknown_candidate" {
					if _, err = f.pool.Exec(ctx, "UPDATE upload_sessions SET terminal_failure_code='upload_destination_unknown' WHERE id=$1", candidate); err != nil {
						t.Fatal(err)
					}
				}
			}
			tx, err = f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			selection, err := f.service.ReserveImportAttemptTx(ctx, tx, sin)
			if name == "stale_candidate" || name == "incompatible_candidate" {
				want := ErrNotFound
				if name == "incompatible_candidate" {
					want = ErrInvalidState
				}
				if !errors.Is(err, want) {
					t.Fatalf("explicit candidate fallback: %v", err)
				}
				_ = tx.Rollback(ctx)
				retained, e := f.service.GetImportAttempt(ctx, 1001, batch, 0, childID)
				if e != nil || retained.UploadID.Valid || retained.SessionMaterialized {
					t.Fatalf("refused candidate changed association %v", e)
				}
				return
			}
			if name == "a_to_s_loss" || name == "legacy_unknown_candidate" {
				if !errors.Is(err, ErrParentUnavailable) {
					t.Fatalf("terminal S replacement %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if name == "s_rollback" {
				_ = tx.Rollback(ctx)
				original, e := f.service.GetImportAttempt(ctx, 1001, batch, 0, childID)
				if e != nil || original.UploadID.Valid {
					t.Fatalf("S rollback leaked %v", e)
				}
				return
			}
			if name == "s_lost_ack" {
				err = b2LostCommit(t)(ctx, tx)
				if !errors.Is(err, fixture.ErrResponseLost) {
					t.Fatal(err)
				}
			} else if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if name == "selected_once" {
				againTx, e := f.pool.Begin(ctx)
				if e != nil {
					t.Fatal(e)
				}
				defer againTx.Rollback(ctx)
				sin.UploadID = uuid.New()
				again, e := f.service.ReserveImportAttemptTx(ctx, againTx, sin)
				if e != nil || again.UploadID != selection.UploadID {
					t.Fatalf("S overwrote selection %v", e)
				}
				if e = againTx.Commit(ctx); e != nil {
					t.Fatal(e)
				}
			}
			if name == "s_to_b_loss" {
				lose()
			}
			create := sin.Create
			create.ID = uuid.UUID(selection.UploadID.Bytes)
			create.ImportAttemptID = &attemptID
			if name == "b_rollback" {
				before := fixture.NewBarrier(t, fixture.ErrProcessLost)
				before.Release()
				f.service.commit = (fixture.Hooks{ImportBBeforeCommit: before}).CommitImportB
			}
			if name == "b_lost_ack" {
				after := fixture.NewBarrier(t, fixture.ErrResponseLost)
				after.Release()
				f.service.commit = (fixture.Hooks{ImportBCommitted: after}).CommitImportB
			}
			session, err := f.service.Create(ctx, create)
			if name == "b_rollback" {
				if !errors.Is(err, fixture.ErrProcessLost) {
					t.Fatal(err)
				}
				original, e := f.service.GetImportAttempt(ctx, 1001, batch, 0, childID)
				if e != nil || original.SessionMaterialized {
					t.Fatalf("B rollback leaked %v", e)
				}
				return
			}
			if name == "b_lost_ack" {
				if !errors.Is(err, fixture.ErrResponseLost) {
					t.Fatal(err)
				}
				f.service.commit = b2NormalCommit
				session, err = f.service.Get(ctx, 1001, create.ID)
			}
			if name == "s_to_b_loss" {
				if !errors.Is(err, ErrParentUnavailable) {
					t.Fatalf("B revived loser %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if name == "materialized_missing" {
				if _, err = f.pool.Exec(ctx, "DELETE FROM upload_sessions WHERE id=$1", uuid.UUID(session.ID.Bytes)); err != nil {
					t.Fatal(err)
				}
				if _, err = f.service.Create(ctx, create); !errors.Is(err, ErrNotFound) {
					t.Fatalf("materialized missing recreated %v", err)
				}
				return
			}
			before := b2DomainSnapshot(t, f.pool)
			again, err := f.service.Create(ctx, create)
			if err != nil || again.ID != session.ID || again.PartSize != session.PartSize || b2DomainSnapshot(t, f.pool) != before {
				t.Fatalf("B replay changed %v", err)
			}
		})
	}
}

type b2AssociationArgs struct {
	BatchID   uuid.UUID `json:"batch_id"`
	AttemptID uuid.UUID `json:"attempt_id"`
}

func (b2AssociationArgs) Kind() string { return "b2_import_association_fixture" }
