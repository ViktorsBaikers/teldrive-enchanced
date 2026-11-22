//go:build integration

package uploads_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/tgdrive/teldrive/v2/db/migrations"
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

type publicationQueue struct{}

func (publicationQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, "INSERT INTO publication_jobs(user_id) VALUES($1) RETURNING id", in.UserID).Scan(&id)
	return id, err
}

type publicationFixture struct {
	pool       *pgxpool.Pool
	catalog    *catalog.Service
	uploads    *uploads.Service
	ledger     *lifecycle.Service
	storage    *fixture.Storage
	pipeline   *transfer.Pipeline
	parent, id uuid.UUID
	body       []byte
	part       *sqlcgen.UploadPart
}

func newPublication(t *testing.T, empty bool) *publicationFixture {
	t.Helper()
	db := testpostgres.New(t)
	ctx := context.Background()
	for _, sql := range []string{"INSERT INTO users(user_id) VALUES(1001),(1002)", "INSERT INTO channels(channel_id,user_id,name,selected) VALUES(9001,1001,'fixture',true)", "CREATE TABLE publication_jobs(id bigint GENERATED ALWAYS AS IDENTITY,user_id bigint)"} {
		if _, err := db.Pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	storage := fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}})
	if err := storage.Seed(fixture.Message{UserID: 1002, ChannelID: 9001, MessageID: 70, Bytes: []byte("independent source and old share"), Preserve: true}); err != nil {
		t.Fatal(err)
	}
	policy := lifecycle.NewService(db.Pool, storage)
	if err := policy.BindReceiptEnqueuer(publicationQueue{}); err != nil {
		t.Fatal(err)
	}
	hierarchy := catalog.NewService(db.Pool, nil)
	if err := hierarchy.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	parent, err := hierarchy.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "destination"})
	if err != nil {
		t.Fatal(err)
	}
	parentID := uuid.UUID(parent.ID.Bytes)
	service := uploads.NewService(db.Pool)
	if err = service.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	body := []byte("actual pipeline publication bytes")
	if empty {
		body = nil
	}
	session, err := service.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &parentID, Name: "result.bin", ExpectedSize: int64(len(body))})
	if err != nil {
		t.Fatal(err)
	}
	f := &publicationFixture{pool: db.Pool, catalog: hierarchy, uploads: service, ledger: policy, storage: storage, parent: parentID, id: uuid.UUID(session.ID.Bytes), body: body}
	f.pipeline = transfer.NewPipeline(service, channels.NewService(db.Pool, nil, channels.Config{}), storage, nil, transfer.Config{Receipts: policy})
	if !empty {
		result, e := f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: int64(len(body)), Body: bytes.NewReader(body)})
		if e != nil {
			t.Fatal(e)
		}
		f.part = result.Part
	}
	return f
}
func (f *publicationFixture) bytes(t *testing.T) {
	t.Helper()
	if f.part != nil {
		r, err := f.storage.OpenRange(context.Background(), telegramstore.RangeRequest{UserID: 1001, ChannelID: f.part.ChannelID, MessageID: f.part.MessageID.Int64, Length: -1})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil || sha256.Sum256(got) != sha256.Sum256(f.body) {
			t.Fatalf("independent bytes: %v", err)
		}
	}
	if err := f.storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
}
func (f *publicationFixture) complete(ctx context.Context) (*sqlcgen.File, error) {
	return f.uploads.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: f.id, Size: int64(len(f.body))})
}
func (f *publicationFixture) snapshot(t *testing.T) string {
	t.Helper()
	var s string
	err := f.pool.QueryRow(context.Background(), `SELECT jsonb_build_array((SELECT to_jsonb(s) FROM upload_sessions s WHERE id=$1),(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY part_no),'[]') FROM upload_parts p WHERE upload_id=$1),(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM remote_write_receipts r),(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY file_id,part_no),'[]') FROM file_parts p),(SELECT count(*) FROM user_events))::text`, f.id).Scan(&s)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestB2PublicationDeletionRace(t *testing.T) {
	for _, order := range []string{"publication_first", "deletion_first"} {
		for _, empty := range []bool{false, true} {
			name := "bytes"
			if empty {
				name = "zero"
			}
			t.Run(order+"/"+name, func(t *testing.T) {
				f := newPublication(t, empty)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				blocker, err := f.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback(ctx)
				var held uuid.UUID
				if order == "publication_first" {
					err = blocker.QueryRow(ctx, "SELECT id FROM upload_sessions WHERE id=$1 FOR UPDATE", f.id).Scan(&held)
				} else {
					err = blocker.QueryRow(ctx, "SELECT id FROM files WHERE id=$1 FOR UPDATE", f.parent).Scan(&held)
				}
				if err != nil {
					t.Fatal(err)
				}
				var blockerPID uint32
				if err = blocker.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
					t.Fatal(err)
				}
				published := make(chan error, 1)
				deleted := make(chan error, 1)
				complete := func() { _, e := f.complete(ctx); published <- e }
				trash := func() { _, e := f.catalog.Trash(ctx, 1001, f.parent); deleted <- e }
				if order == "publication_first" {
					go complete()
				} else {
					go trash()
				}
				observe := func(blocking uint32) uint32 {
					t.Helper()
					var waiter uint32
					for waiter == 0 {
						if e := ctx.Err(); e != nil {
							t.Fatal(e)
						}
						if e := f.pool.QueryRow(ctx, "SELECT COALESCE((SELECT pid FROM pg_stat_activity WHERE $1::int=ANY(pg_blocking_pids(pid)) LIMIT 1),0)", blocking).Scan(&waiter); e != nil {
							t.Fatal(e)
						}
					}
					if e := fixture.WaitForBlocking(ctx, f.pool, waiter, blocking); e != nil {
						t.Fatal(e)
					}
					return waiter
				}
				// First real mutator holds owner; second real mutator waits for that exact connection.
				firstPID := observe(blockerPID)
				if order == "publication_first" {
					go trash()
				} else {
					go complete()
				}
				observe(firstPID)
				if err = blocker.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				publicationErr, deletionErr := <-published, <-deleted
				if deletionErr != nil {
					t.Fatal(deletionErr)
				}
				if order == "publication_first" {
					if publicationErr != nil {
						t.Fatal(publicationErr)
					}
					var status string
					var fileID uuid.UUID
					if err = f.pool.QueryRow(ctx, "SELECT f.id,f.status FROM upload_sessions s JOIN files f ON f.id=s.file_id WHERE s.id=$1", f.id).Scan(&fileID, &status); err != nil || status != "trashed" {
						t.Fatalf("fresh deletion missed publication %s %v", status, err)
					}
					var n int
					if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM user_events WHERE event_type='file.created' AND resource_id=$1", fileID.String()).Scan(&n); err != nil || n != 1 {
						t.Fatalf("publication event count %d %v", n, err)
					}
				} else {
					if !errors.Is(publicationErr, uploads.ErrParentUnavailable) {
						t.Fatalf("deletion loser: %v", publicationErr)
					}
					if _, err = f.catalog.Restore(ctx, 1001, f.parent); err != nil {
						t.Fatal(err)
					}
					before := f.snapshot(t)
					if _, err = f.complete(ctx); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatalf("restored loser: %v", err)
					}
					if before != f.snapshot(t) {
						t.Fatal("loser changed evidence")
					}
				}
				f.bytes(t)
			})
		}
	}
}
func TestB2TerminalParentFailure(t *testing.T) {
	for _, mode := range []string{"direct", "ancestor", "restart", "zero_byte"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublication(t, mode == "zero_byte")
			ctx := context.Background()
			target := f.parent
			if mode == "ancestor" {
				root, err := f.catalog.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "ancestor"})
				if err != nil {
					t.Fatal(err)
				}
				target = uuid.UUID(root.ID.Bytes)
				if _, err = f.catalog.Move(ctx, 1001, f.parent, &target, nil); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.catalog.Trash(ctx, 1001, target); err != nil {
				t.Fatal(err)
			}
			if _, err := f.catalog.Restore(ctx, 1001, target); err != nil {
				t.Fatal(err)
			}
			if mode == "restart" {
				f.uploads = uploads.NewService(f.pool)
				if err := f.uploads.BindLifecycle(f.ledger); err != nil {
					t.Fatal(err)
				}
			}
			before := f.snapshot(t)
			for range 2 {
				if _, err := f.complete(ctx); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatalf("terminal loss: %v", err)
				}
			}
			if before != f.snapshot(t) {
				t.Fatal("terminal failure altered evidence")
			}
			var n int
			if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE name='result.bin'").Scan(&n); err != nil || n != 0 {
				t.Fatalf("false result %d %v", n, err)
			}
			f.bytes(t)
		})
	}
}
func TestB2CompletedAliasRetirement(t *testing.T) {
	for _, mode := range []string{"owned", "independent_alias", "zero_byte", "rollback_mismatch"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublication(t, mode == "zero_byte")
			ctx := context.Background()
			if mode == "rollback_mismatch" {
				if _, err := f.pool.Exec(ctx, "UPDATE remote_write_receipts SET origin_part_created_at=now()-interval '1 day',origin_part_updated_at=now()-interval '1 day' WHERE upload_id=$1", f.id); err != nil {
					t.Fatal(err)
				}
			}
			before := f.snapshot(t)
			file, err := f.complete(ctx)
			if mode == "rollback_mismatch" {
				if err == nil {
					t.Fatal("mismatched original timestamps accepted")
				}
				if before != f.snapshot(t) {
					t.Fatal("failed retirement leaked publication")
				}
				f.bytes(t)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var n int
			if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE upload_id=$1", f.id).Scan(&n); err != nil || n != 0 {
				t.Fatalf("owned aliases remain %d %v", n, err)
			}
			if mode == "independent_alias" {
				other, err := f.uploads.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "independent", ExpectedSize: int64(len(f.body))})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.pool.Exec(ctx, "INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size,stored_size,state) VALUES($1,1,9001,$2,$3,$3,'stored')", uuid.UUID(other.ID.Bytes), f.part.MessageID.Int64, len(f.body)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "independent_alias" {
				if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_parts WHERE message_id=$1", f.part.MessageID.Int64).Scan(&n); err != nil || n != 1 {
					t.Fatalf("independent alias retired %d %v", n, err)
				}
			}
			before = f.snapshot(t)
			again, err := f.complete(ctx)
			if err != nil || again.ID != file.ID {
				t.Fatalf("actual-result replay %v", err)
			}
			if f.part != nil {
				proof, e := f.uploads.GetPart(ctx, 1001, f.id, 1)
				if e != nil || proof.CreatedAt != f.part.CreatedAt || proof.UpdatedAt != f.part.UpdatedAt {
					t.Fatalf("original proof timestamps %v", e)
				}
				if _, e = f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: int64(len(f.body)), Body: bytes.NewReader(f.body)}); e != nil {
					t.Fatal(e)
				}
			}
			if before != f.snapshot(t) {
				t.Fatal("completed replay wrote state/alias/event")
			}
			f.bytes(t)
		})
	}
}

type publicationReader struct {
	io.Reader
	reads int
}

func (r *publicationReader) Read(p []byte) (int, error) { r.reads++; return r.Reader.Read(p) }

func TestB2LegacyDestinationGuard(t *testing.T) {
	for _, name := range []string{"zero_byte_before_conflict", "nonempty_evidence_retained", "existing_create_identity", "get_claim_store_renew_put", "compatible_resume_refusal", "legacy_nonroot_active", "completed_legacy_replay", "independent_scope_control"} {
		t.Run(name, func(t *testing.T) {
			nonempty := name == "nonempty_evidence_retained"
			f := newPublication(t, !nonempty)
			ctx := context.Background()
			if name == "legacy_nonroot_active" || name == "completed_legacy_replay" {
				file, err := f.complete(ctx)
				if err != nil {
					t.Fatal(err)
				}
				before := f.snapshot(t)
				again, err := f.complete(ctx)
				if err != nil || again.ID != file.ID || before != f.snapshot(t) {
					t.Fatalf("completed/nonroot control %v", err)
				}
				return
			}
			// This supplemental candidate seed is not the required pre-migration dump/restore proof.
			size := int64(0)
			if nonempty {
				size = int64(len(f.body))
			}
			root, err := f.uploads.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "legacy.bin", ExpectedSize: size, ConflictPolicy: sqlcgen.NameConflictPolicyReplace})
			if err != nil {
				t.Fatal(err)
			}
			f.id = uuid.UUID(root.ID.Bytes)
			f.part = nil
			if nonempty {
				stored, e := f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: size, Body: bytes.NewReader(f.body)})
				if e != nil {
					t.Fatal(e)
				}
				f.part = stored.Part
			} else {
				f.body = nil
			}
			if name == "zero_byte_before_conflict" {
				old, e := f.uploads.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "legacy.bin", ExpectedSize: 0})
				if e != nil {
					t.Fatal(e)
				}
				file, e := f.uploads.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: uuid.UUID(old.ID.Bytes), Size: 0})
				if e != nil {
					t.Fatal(e)
				}
				share := uuid.New()
				if _, e = f.pool.Exec(ctx, "INSERT INTO file_shares(id,file_id,owner_id,token_prefix,token_hash) VALUES($1,$2,1001,'b2',$3)", share, uuid.UUID(file.ID.Bytes), []byte(share.String())); e != nil {
					t.Fatal(e)
				}
			}
			if _, err = f.pool.Exec(ctx, "UPDATE upload_sessions SET terminal_failure_code='upload_destination_unknown' WHERE id=$1", f.id); err != nil {
				t.Fatal(err)
			}
			snapshot := func() string {
				t.Helper()
				var value string
				err := f.pool.QueryRow(ctx, `SELECT jsonb_build_array(
				(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM upload_sessions s),
				(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY upload_id,part_no),'[]') FROM upload_parts p),
				(SELECT COALESCE(jsonb_agg(to_jsonb(f) ORDER BY id),'[]') FROM files f),
				(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM file_shares s),
				(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM remote_write_receipts r),
				(SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY id),'[]') FROM user_events e))::text`).Scan(&value)
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
			before := snapshot()
			for range 2 {
				restarted := uploads.NewService(f.pool)
				if err = restarted.BindLifecycle(f.ledger); err != nil {
					t.Fatal(err)
				}
				token := uuid.New()
				checks := []func() error{
					func() error {
						_, e := restarted.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: f.id, Size: size})
						return e
					},
					func() error { _, e := restarted.Create(ctx, uploads.CreateInput{ID: f.id, UserID: 1001}); return e },
					func() error { _, e := restarted.Get(ctx, 1001, f.id); return e },
					func() error {
						_, e := restarted.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: f.id, PartNo: 1, ChannelID: 9001, PlainSize: 1})
						return e
					},
					func() error {
						_, e := restarted.StorePart(ctx, uploads.StorePartInput{UploadID: f.id, PartNo: 1, LeaseToken: token, MessageID: 99, StoredSize: 1})
						return e
					},
					func() error {
						return restarted.RenewPart(ctx, uploads.RenewPartInput{UploadID: f.id, PartNo: 1, LeaseToken: token})
					},
					func() error {
						_, e := restarted.FailPart(ctx, uploads.FailPartInput{UploadID: f.id, PartNo: 1, LeaseToken: token, ErrorCode: "late_failure"})
						return e
					},
					func() error { _, e := restarted.Abort(ctx, 1001, f.id); return e },
				}
				for _, check := range checks {
					if e := check(); !errors.Is(e, uploads.ErrParentUnavailable) {
						t.Fatalf("unknown operation %v", e)
					}
				}
				reader := &publicationReader{Reader: bytes.NewReader([]byte("must not read"))}
				if _, e := f.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: 1001, UploadID: f.id, PartNo: 1, PlainSize: 1, Body: reader}); !errors.Is(e, uploads.ErrParentUnavailable) || reader.reads != 0 {
					t.Fatalf("unknown PUT read/source success %d %v", reader.reads, e)
				}
				cutoff, cutoffErr := sqlcgen.New(f.pool).GetCleanupSweepCutoff(ctx)
				if cutoffErr != nil {
					t.Fatal(cutoffErr)
				}
				if _, e := sqlcgen.New(f.pool).ExpireUploadSessions(ctx, cutoff); e != nil {
					t.Fatal(e)
				}
				if before != snapshot() {
					t.Fatal("unknown reason/session/parts/leases/bytes/events changed")
				}
			}
			if name == "compatible_resume_refusal" {
				rows, e := sqlcgen.New(f.pool).FindResumableUploadSessions(ctx, sqlcgen.FindResumableUploadSessionsParams{UserID: 1001, Name: "legacy.bin", ExpectedSize: 0})
				if e != nil || len(rows) != 1 || uuid.UUID(rows[0].ID.Bytes) != f.id {
					t.Fatalf("unknown candidate filtered away %+v %v", rows, e)
				}
			}
			if name == "independent_scope_control" {
				fresh, e := f.uploads.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "new-explicit-root", ExpectedSize: 0})
				if e != nil {
					t.Fatal(e)
				}
				if _, e = f.uploads.Complete(ctx, uploads.CompleteInput{UserID: 1001, UploadID: uuid.UUID(fresh.ID.Bytes), Size: 0}); e != nil {
					t.Fatal(e)
				}
			}
			f.bytes(t)
		})
	}
}

type selectionJob struct{ BatchID uuid.UUID }

func (selectionJob) Kind() string { return "b2_selection_snapshot" }

func TestB2ImportSelectionSnapshot(t *testing.T) {
	for _, candidate := range []bool{false, true} {
		name := "fresh"
		if candidate {
			name = "candidate_old_key_geometry"
		}
		t.Run(name, func(t *testing.T) {
			f := newPublication(t, true)
			ctx := context.Background()
			batch, attemptID := uuid.New(), uuid.New()
			client, err := river.NewClient(riverpgxv5.New(f.pool), &river.Config{Schema: "teldrive", SkipUnknownJobCheck: true})
			if err != nil {
				t.Fatal(err)
			}
			tx, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			pins, err := f.uploads.PinImportDestinationsTx(ctx, tx, uploads.PinImportDestinationsInput{UserID: 1001, BatchID: batch, Destination: f.parent.String(), SourceDestinations: []string{""}})
			if err != nil {
				t.Fatal(err)
			}
			origin, err := client.InsertTx(ctx, tx, selectionJob{batch}, nil)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256([]byte("snapshot request"))
			if err = f.uploads.InsertImportBatchAdmissionTx(ctx, tx, uploads.InsertImportBatchAdmissionInput{UserID: 1001, BatchID: batch, OriginJobID: origin.Job.ID, RequestFingerprint: digest, Pins: pins}); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			tx, err = f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			resolved, err := f.uploads.ReserveImportBatchTx(ctx, tx, uploads.ReserveImportBatchInput{UserID: 1001, BatchID: batch, OriginJobID: origin.Job.ID, RequestFingerprint: digest, Destination: f.parent.String(), SourceDestinations: []string{""}, Sources: []uploads.ImportSourceDestination{{AttemptID: attemptID, SourceIndex: 0, DestinationOrdinal: 0}}})
			if err != nil {
				t.Fatal(err)
			}
			children, err := client.InsertManyTx(ctx, tx, []river.InsertManyParams{{Args: selectionJob{batch}}})
			if err != nil {
				t.Fatal(err)
			}
			jobID := children[0].Job.ID
			if err = f.uploads.RecordImportChildrenTx(ctx, tx, uploads.RecordImportChildrenInput{UserID: 1001, BatchID: batch, OriginJobID: origin.Job.ID, Destinations: resolved, Children: []uploads.ImportChild{{AttemptID: attemptID, SourceIndex: 0, JobID: jobID}}}); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			for _, invalid := range []any{nil, []byte(`{}`), []byte(`{"version":1}`)} {
				if _, e := f.pool.Exec(ctx, "UPDATE import_job_attempts SET upload_id=$2,selected_create=$3::jsonb WHERE attempt_id=$1", attemptID, uuid.New(), invalid); e == nil {
					t.Fatal("invalid selected shape accepted")
				}
			}
			mime, algorithm, value := "application/snapshot", "blake3", fmt.Sprintf("%x", sha256.Sum256(nil))
			key := int32(2)
			create := uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: "snapshot.bin", ExpectedSize: 0, MIMEType: &mime, ExpectedHashAlgorithm: &algorithm, ExpectedHashValue: &value, Encryption: true, EncryptionKeyVersion: &key, ConflictPolicy: sqlcgen.NameConflictPolicyReplace, PartSize: 16 * 1024 * 1024}
			id := uuid.New()
			var original *sqlcgen.UploadSession
			if candidate {
				original, err = f.uploads.Create(ctx, create)
				if err != nil {
					t.Fatal(err)
				}
				id = uuid.UUID(original.ID.Bytes)
				create.PartSize = 512 * 1024 * 1024
				newKey := int32(3)
				create.EncryptionKeyVersion = &newKey
			}
			if candidate {
				create.ExpectedHashAlgorithm = nil
				create.ExpectedHashValue = nil
			}
			tx, err = f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			before := time.Now().UTC().Truncate(time.Microsecond)
			selected, err := f.uploads.ReserveImportAttemptTx(ctx, tx, uploads.ReserveImportAttemptInput{UserID: 1001, BatchID: batch, JobID: jobID, UploadID: id, Candidate: candidate, Create: create})
			if err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			frozen, err := uploads.SelectedImportCreateInput(selected)
			if err != nil {
				t.Fatal(err)
			}
			if frozen.ModTime.IsZero() || frozen.ModTime.Nanosecond()%1000 != 0 || (!candidate && frozen.ModTime.Before(before)) {
				t.Fatal("S did not fix microsecond modtime")
			}
			if frozen.ID != id || frozen.UserID != 1001 || frozen.ParentID == nil || *frozen.ParentID != f.parent || frozen.ImportAttemptID == nil || *frozen.ImportAttemptID != attemptID || frozen.Admission != nil {
				t.Fatal("identity not rebuilt from row")
			}
			var descriptor map[string]any
			if err = json.Unmarshal(selected.SelectedCreate, &descriptor); err != nil || len(descriptor) != 11 {
				t.Fatalf("descriptor whitelist: %v", err)
			}
			for _, bad := range [][]byte{nil, []byte(`{}`), []byte(`{"version":2}`), []byte(`{"version":1,"id":"foreign"}`)} {
				broken := *selected
				broken.SelectedCreate = bad
				if _, e := uploads.SelectedImportCreateInput(&broken); e == nil {
					t.Fatal("malformed descriptor accepted")
				}
			}
			for _, mutation := range []string{"unknown_version", "unknown_key", "missing_key", "invalid_geometry", "null_required"} {
				fields := make(map[string]json.RawMessage)
				if e := json.Unmarshal(selected.SelectedCreate, &fields); e != nil {
					t.Fatal(e)
				}
				switch mutation {
				case "unknown_version":
					fields["version"] = json.RawMessage(`2`)
				case "unknown_key":
					fields["id"] = json.RawMessage(`"foreign"`)
				case "missing_key":
					delete(fields, "mimeType")
				case "invalid_geometry":
					fields["partSize"] = json.RawMessage(`1`)
				case "null_required":
					fields["encryption"] = json.RawMessage(`null`)
				}
				broken := *selected
				broken.SelectedCreate, err = json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				if _, e := uploads.SelectedImportCreateInput(&broken); e == nil {
					t.Fatalf("%s accepted", mutation)
				}
			}
			tx, err = f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			changedPreference := create
			changedPreference.Name = "ignored replay preference"
			changedPreference.PartSize = 1024 * 1024 * 1024
			replay, e := f.uploads.ReserveImportAttemptTx(ctx, tx, uploads.ReserveImportAttemptInput{UserID: 1001, BatchID: batch, JobID: jobID, UploadID: uuid.New(), Create: changedPreference})
			if e != nil || replay.UploadID != selected.UploadID || !bytes.Equal(replay.SelectedCreate, selected.SelectedCreate) {
				t.Fatalf("S replay changed descriptor: %v", e)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			broken := *selected
			broken.AttemptID = dbtypes.UUID(uuid.Nil)
			if _, e := uploads.SelectedImportCreateInput(&broken); e == nil {
				t.Fatal("invalid row identity accepted")
			}
			restartedGuard := uploads.NewService(f.pool)
			var countBefore int
			if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_sessions").Scan(&countBefore); err != nil {
				t.Fatal(err)
			}
			for _, identity := range []string{"owner", "parent", "upload"} {
				bad := frozen
				switch identity {
				case "owner":
					bad.UserID = 1002
				case "parent":
					wrong := uuid.New()
					bad.ParentID = &wrong
				case "upload":
					bad.ID = uuid.New()
				}
				if _, e := restartedGuard.Create(ctx, bad); e == nil {
					t.Fatalf("foreign %s accepted", identity)
				}
			}
			var countAfter int
			if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM upload_sessions").Scan(&countAfter); err != nil || countAfter != countBefore {
				t.Fatal("identity refusal allocated a session")
			}
			for _, query := range []string{`UPDATE import_job_attempts SET selected_create=jsonb_set(selected_create,'{name}','"mutated"') WHERE attempt_id=$1`, `UPDATE import_job_attempts SET selected_create=NULL WHERE attempt_id=$1`} {
				if _, e := f.pool.Exec(ctx, query, attemptID); e == nil {
					t.Fatal("immutable snapshot update accepted")
				}
			}
			if _, e := sqlcgen.New(f.pool).SelectImportUpload(ctx, sqlcgen.SelectImportUploadParams{UserID: 1001, AttemptID: selected.AttemptID, UploadID: dbtypes.UUID(uuid.New()), SelectedCreate: selected.SelectedCreate}); !errors.Is(e, pgx.ErrNoRows) {
				t.Fatalf("selection CAS: %v", e)
			}
			restarted := uploads.NewService(f.pool, time.Hour)
			changed := create
			changed.ID = id
			changed.ImportAttemptID = &attemptID
			changed.Name = "changed"
			changed.MIMEType = nil
			changed.ExpectedHashAlgorithm = nil
			changed.ExpectedHashValue = nil
			changed.Encryption = false
			changed.EncryptionKeyVersion = nil
			changed.PartSize = 512 * 1024 * 1024
			changed.ModTime = time.Now().Add(time.Hour)
			session, err := restarted.Create(ctx, changed)
			if err != nil {
				t.Fatal(err)
			}
			if session.ID != selected.UploadID || session.Name != frozen.Name || session.ExpectedSize != frozen.ExpectedSize || session.MimeType != dbtypes.OptionalText(frozen.MIMEType) || !session.ModTime.Valid || !session.ModTime.Time.Equal(frozen.ModTime) || session.ExpectedHashAlgorithm != dbtypes.OptionalText(frozen.ExpectedHashAlgorithm) || session.ExpectedHashValue != dbtypes.OptionalText(frozen.ExpectedHashValue) || session.Encryption != frozen.Encryption || session.EncryptionKeyVersion != dbtypes.OptionalInt4(frozen.EncryptionKeyVersion) || session.PartSize != frozen.PartSize || session.ConflictPolicy != frozen.ConflictPolicy {
				t.Fatal("B changed frozen selection")
			}
			if candidate && (session.ID != original.ID || session.PartSize != original.PartSize || session.EncryptionKeyVersion != original.EncryptionKeyVersion) {
				t.Fatal("candidate policy changed")
			}
			sqlDB := stdlib.OpenDBFromPool(f.pool)
			defer sqlDB.Close()
			if e := migrations.Down(ctx, sqlDB, "teldrive"); e == nil {
				t.Fatal("Down erased selected evidence")
			}
			retained, e := restarted.GetImportAttempt(ctx, 1001, batch, 0, jobID)
			if e != nil || !bytes.Equal(retained.SelectedCreate, selected.SelectedCreate) {
				t.Fatalf("Down changed snapshot: %v", e)
			}
			if _, e = f.catalog.Trash(ctx, 1001, f.parent); e != nil {
				t.Fatal(e)
			}
			if _, e = f.pool.Exec(ctx, `UPDATE import_job_attempts SET selected_create=jsonb_set(selected_create,'{name}','"terminal mutation"') WHERE attempt_id=$1`, attemptID); e == nil {
				t.Fatal("terminal snapshot mutated")
			}
		})
	}
}
