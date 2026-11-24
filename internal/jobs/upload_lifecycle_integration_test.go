//go:build integration

package jobs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/divyam234/riverpro/riverencrypt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertest"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

type importerFixture struct {
	db             *testpostgres.Database
	runtime        *Runtime
	policy         *lifecycle.Service
	catalog        *catalog.Service
	uploads        *uploads.Service
	pipeline       *transfer.Pipeline
	storage        *fixture.Storage
	cipher         *secureblob.Cipher
	root, location string
	payload        []byte
	parent         uuid.UUID
}

func newImporterFixture(t *testing.T) *importerFixture {
	t.Helper()
	f := &importerFixture{db: testpostgres.New(t), root: t.TempDir(), payload: []byte("independent importer source bytes")}
	ctx := context.Background()
	if _, err := f.db.Pool.Exec(ctx, "INSERT INTO users(user_id) VALUES(1001),(1002)"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(ctx, "INSERT INTO channels(channel_id,user_id,name,selected) VALUES(9001,1001,'synthetic',true)"); err != nil {
		t.Fatal(err)
	}
	f.location = filepath.Join(f.root, "source.txt")
	if err := os.WriteFile(f.location, f.payload, 0600); err != nil {
		t.Fatal(err)
	}
	f.storage = fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}})
	f.policy = lifecycle.NewService(f.db.Pool, f.storage)
	f.catalog = catalog.NewService(f.db.Pool, nil)
	f.uploads = uploads.NewService(f.db.Pool)
	if err := f.catalog.BindLifecycle(f.policy); err != nil {
		t.Fatal(err)
	}
	if err := f.uploads.BindLifecycle(f.policy); err != nil {
		t.Fatal(err)
	}
	cs := channels.NewService(f.db.Pool, channels.TelegramCreator{Storage: f.storage}, channels.Config{PartLimit: 1000})
	f.pipeline = transfer.NewPipeline(f.uploads, cs, f.storage, transfer.StaticKeyProvider{1: "synthetic-import-key", 2: "synthetic-older-import-key"}, transfer.Config{Receipts: f.policy})
	var err error
	f.cipher, err = secureblob.NewWithKey(bytes.Repeat([]byte{7}, 32), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.runtime, err = NewRuntimeWithServices(f.db.Pool, f.storage, "teldrive", nil, f.cipher, time.Hour, UploaderServices{Catalog: f.catalog, Uploads: f.uploads, Pipeline: f.pipeline, ActiveKeyVersion: 1, LocalImportRoots: []string{f.root}}, f.policy)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.policy.BindReceiptEnqueuer(f.runtime); err != nil {
		t.Fatal(err)
	}
	folder, err := f.catalog.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "destination"})
	if err != nil {
		t.Fatal(err)
	}
	f.parent = uuid.UUID(folder.ID.Bytes)
	return f
}
func (f *importerFixture) admit(t *testing.T, destination string) *river.Job[UploadBatchArgs] {
	t.Helper()
	args := UploadBatchArgs{BatchID: uuid.NewString(), UserID: 1001, Destination: destination, Sources: []UploadSource{{Type: "local", Path: f.location, DestinationPath: "source.txt"}}}
	return f.admitArgs(t, args)
}
func (f *importerFixture) admitArgs(t *testing.T, args UploadBatchArgs) *river.Job[UploadBatchArgs] {
	t.Helper()
	row, err := f.runtime.InsertUploadBatch(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := f.runtime.client.JobGet(context.Background(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(actual.EncodedArgs, []byte(`"river_encrypted_args"`)) || bytes.Contains(actual.EncodedArgs, []byte(f.location)) {
		t.Fatal("new batch args were not encrypted")
	}
	encoded, err := riverencrypt.DecryptArgs(f.cipher, actual.EncodedArgs)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(encoded, &args); err != nil {
		t.Fatal(err)
	}
	return &river.Job[UploadBatchArgs]{JobRow: actual, Args: args}
}
func (f *importerFixture) batch(t *testing.T, job *river.Job[UploadBatchArgs]) error {
	t.Helper()
	ctx := rivertest.WorkContext(context.Background(), f.runtime.client.Client)
	return NewUploadBatchWorker(f.db.Pool, nil, f.catalog, f.uploads, []string{f.root}).Work(ctx, job)
}
func (f *importerFixture) child(t *testing.T, job *river.Job[UploadBatchArgs]) *river.Job[UploadSourceArgs] {
	t.Helper()
	var id int64
	if err := f.db.Pool.QueryRow(context.Background(), "SELECT job_id FROM import_job_attempts WHERE user_id=1001 AND batch_id=$1 AND source_index=0", job.Args.BatchID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	row, err := f.runtime.client.JobGet(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(row.EncodedArgs, []byte(`"river_encrypted_args"`)) || bytes.Contains(row.EncodedArgs, []byte(f.location)) {
		t.Fatal("new source args were not encrypted")
	}
	var metadata struct {
		UserID int64 `json:"user_id"`
	}
	if err = json.Unmarshal(row.Metadata, &metadata); err != nil || metadata.UserID != 1001 {
		t.Fatalf("child lacked insert-time owner metadata: %v", err)
	}
	encoded, err := riverencrypt.DecryptArgs(f.cipher, row.EncodedArgs)
	if err != nil {
		t.Fatal(err)
	}
	var args UploadSourceArgs
	if err = json.Unmarshal(encoded, &args); err != nil {
		t.Fatal(err)
	}
	return &river.Job[UploadSourceArgs]{JobRow: row, Args: args}
}
func (f *importerFixture) source(job *river.Job[UploadSourceArgs]) error {
	return NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, nil, 1, false, []string{f.root}).Work(context.Background(), job)
}
func (f *importerFixture) preserved(t *testing.T) {
	t.Helper()
	got, err := os.ReadFile(f.location)
	if err != nil || sha256.Sum256(got) != sha256.Sum256(f.payload) {
		t.Fatalf("source changed: %v", err)
	}
	if err = f.storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
}
func (f *importerFixture) snapshot(t *testing.T) string {
	t.Helper()
	var result string
	if err := f.db.Pool.QueryRow(context.Background(), `SELECT jsonb_build_array(
 (SELECT jsonb_agg(to_jsonb(b) ORDER BY batch_id) FROM import_batches b),
 (SELECT jsonb_agg(to_jsonb(a) ORDER BY attempt_id) FROM import_job_attempts a),
 (SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM upload_sessions s),
 (SELECT jsonb_agg(to_jsonb(p) ORDER BY upload_id,part_no) FROM upload_parts p),
 (SELECT jsonb_agg(to_jsonb(p) ORDER BY batch_id,destination_ordinal) FROM import_destination_pins p),
 (SELECT jsonb_agg(to_jsonb(f) ORDER BY id) FROM files f),
 (SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM file_shares s),
 (SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM user_events e),
 (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM remote_write_receipts r))::text`).Scan(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestB2ImporterTerminalParent(t *testing.T) {
	for _, name := range []string{"before_source", "ancestor", "legacy_unknown_no_replacement", "selected_unknown_no_replacement", "missing_origin", "independent_unknown_scope", "zero_byte", "multipart"} {
		t.Run(name, func(t *testing.T) {
			f := newImporterFixture(t)
			ctx := context.Background()
			if name == "zero_byte" || name == "multipart" {
				f.payload = nil
				if name == "multipart" {
					f.payload = bytes.Repeat([]byte("p"), 64*1024*1024+17)
				}
				if err := os.WriteFile(f.location, f.payload, 0600); err != nil {
					t.Fatal(err)
				}
			}
			batch := f.admit(t, f.parent.String())
			if name == "multipart" {
				args := batch.Args
				args.BatchID = uuid.NewString()
				args.ChunkSize = 64 * 1024 * 1024
				batch = f.admitArgs(t, args)
			}
			if err := f.batch(t, batch); err != nil {
				t.Fatal(err)
			}
			child := f.child(t, batch)
			if name == "missing_origin" {
				args := child.Args
				args.SourceIndex++
				inserted, err := f.runtime.client.Insert(ctx, args, nil)
				if err != nil {
					t.Fatal(err)
				}
				unbound := &river.Job[UploadSourceArgs]{JobRow: inserted.Job, Args: args}
				if err = os.Rename(f.location, f.location+".saved"); err != nil {
					t.Fatal(err)
				}
				before := f.snapshot(t)
				if err = f.source(unbound); !errors.Is(err, uploads.ErrNotFound) {
					t.Fatalf("unbound source accessed source or folder: %v", err)
				}
				if before != f.snapshot(t) {
					t.Fatal("unbound source created association")
				}
				if err = os.Rename(f.location+".saved", f.location); err != nil {
					t.Fatal(err)
				}
				f.preserved(t)
				return
			}
			if name == "independent_unknown_scope" {
				session, err := f.uploads.Create(ctx, uploads.CreateInput{UserID: 1002, Name: "unrelated.txt", ExpectedSize: 0})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.db.Pool.Exec(ctx, "UPDATE upload_sessions SET terminal_failure_code='upload_destination_unknown' WHERE id=$1", session.ID); err != nil {
					t.Fatal(err)
				}
				if err = f.source(child); err != nil {
					t.Fatalf("unrelated unknown poisoned source: %v", err)
				}
				f.preserved(t)
				return
			}
			if name == "selected_unknown_no_replacement" {
				worker := NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, nil, 1, false, []string{f.root})
				worker.create = func(ctx context.Context, input uploads.CreateInput) (*sqlcgen.UploadSession, error) {
					session, err := f.uploads.Create(ctx, input)
					if err != nil {
						return nil, err
					}
					if _, err = f.db.Pool.Exec(ctx, "UPDATE upload_sessions SET terminal_failure_code='upload_destination_unknown' WHERE id=$1", session.ID); err != nil {
						return nil, err
					}
					return nil, uploads.ErrParentUnavailable
				}
				if err := worker.Work(ctx, child); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatalf("selected unknown=%v", err)
				}
				if err := os.Rename(f.location, f.location+".saved"); err != nil {
					t.Fatal(err)
				}
				before := f.snapshot(t)
				if err := f.source(child); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatalf("selected unknown retried source: %v", err)
				}
				if before != f.snapshot(t) {
					t.Fatal("selected unknown changed attempt")
				}
				if err := os.Rename(f.location+".saved", f.location); err != nil {
					t.Fatal(err)
				}
				f.preserved(t)
				return
			}
			if name == "legacy_unknown_no_replacement" {
				// Supplemental unknown fixture; the genuine old detach/restore oracle belongs to03-12.
				mime := "text/plain"
				session, err := f.uploads.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: "source.txt", ExpectedSize: int64(len(f.payload)), MIMEType: &mime, ModTime: child.Args.Source.ModTime, ConflictPolicy: "replace"})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.db.Pool.Exec(ctx, "UPDATE upload_sessions SET terminal_failure_code='upload_destination_unknown' WHERE id=$1", session.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				target := f.parent
				if name == "ancestor" {
					folder, err := f.catalog.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "ancestor"})
					if err != nil {
						t.Fatal(err)
					}
					target = uuid.UUID(folder.ID.Bytes)
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
			}
			before := f.snapshot(t)
			for range 2 {
				if err := f.source(child); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatalf("terminal Work=%v", err)
				}
			}
			if f.snapshot(t) != before {
				t.Fatal("terminal replay changed durable identities or parts")
			}
			if err := f.batch(t, batch); name != "legacy_unknown_no_replacement" && !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("terminal batch=%v", err)
			}
			f.preserved(t)
		})
	}
}

func TestB2ImporterAttemptDurability(t *testing.T) {
	t.Run("selected_policy_restart", func(t *testing.T) {
		for _, mode := range []string{"frozen", "stricter_policy", "missing_key", "changed_size", "changed_mime", "changed_modtime", "materialized_missing", "removed_river"} {
			t.Run(mode, func(t *testing.T) {
				f := newImporterFixture(t)
				ctx := context.Background()
				batch := f.admit(t, f.parent.String())
				args := batch.Args
				args.BatchID = uuid.NewString()
				args.Encryption = mode != "stricter_policy"
				args.ChunkSize = 64 * 1024 * 1024
				batch = f.admitArgs(t, args)
				if err := f.batch(t, batch); err != nil {
					t.Fatal(err)
				}
				child := f.child(t, batch)
				original := *child
				worker := NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, nil, 1, false, []string{f.root})
				lost := errors.New("process lost after real S before B")
				worker.commit = func(ctx context.Context, tx pgx.Tx) error {
					if err := tx.Commit(ctx); err != nil {
						return err
					}
					panic(lost)
				}
				var recovered any
				func() { defer func() { recovered = recover() }(); _ = worker.Work(ctx, child) }()
				if recovered != lost {
					t.Fatalf("S process loss=%v", recovered)
				}
				attempt, err := f.uploads.GetImportAttempt(ctx, 1001, uuid.MustParse(batch.Args.BatchID), 0, child.ID)
				if err != nil {
					t.Fatal(err)
				}
				frozen, err := uploads.SelectedImportCreateInput(attempt)
				if err != nil {
					t.Fatal(err)
				}
				if attempt.SessionMaterialized || frozen.ID == uuid.Nil || frozen.PartSize != 64*1024*1024 || frozen.Encryption != args.Encryption {
					t.Fatal("S did not freeze effective create")
				}
				if frozen.ID != uuid.UUID(attempt.UploadID.Bytes) || frozen.UserID != 1001 || frozen.ParentID == nil || *frozen.ParentID != f.parent || frozen.ImportAttemptID == nil || *frozen.ImportAttemptID != uuid.UUID(attempt.AttemptID.Bytes) {
					t.Fatal("descriptor did not retain immutable association IDs")
				}
				if frozen.Encryption && (frozen.EncryptionKeyVersion == nil || *frozen.EncryptionKeyVersion != 1) {
					t.Fatal("S lost original key")
				}
				var sessions int
				if err = f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM upload_sessions").Scan(&sessions); err != nil || sessions != 0 {
					t.Fatalf("B ran before process loss: %d %v", sessions, err)
				}
				retry := original
				retry.Args.ChunkSize = 128 * 1024 * 1024
				retry.Args.PartConcurrency = 2
				pipeline := f.pipeline
				if mode == "missing_key" {
					cs := channels.NewService(f.db.Pool, channels.TelegramCreator{Storage: f.storage}, channels.Config{PartLimit: 1000})
					pipeline = transfer.NewPipeline(f.uploads, cs, f.storage, transfer.StaticKeyProvider{2: "synthetic-older-import-key"}, transfer.Config{Receipts: f.policy})
					if err = os.Rename(f.location, f.location+".saved"); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := os.Rename(f.location+".saved", f.location); err != nil {
							t.Error(err)
						}
					}()
				}
				fresh := NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, pipeline, nil, 2, mode == "stricter_policy", []string{f.root})
				if mode == "changed_size" {
					if err = os.WriteFile(f.location, append(append([]byte(nil), f.payload...), 33), 0600); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "changed_mime" {
					retry.Args.Source.MIMEType = "image/png"
				}
				if mode == "changed_modtime" {
					stamp := frozen.ModTime.Add(time.Hour)
					if err = os.Chtimes(f.location, stamp, stamp); err != nil {
						t.Fatal(err)
					}
				}
				before := f.snapshot(t)
				err = fresh.Work(ctx, &retry)
				if mode == "missing_key" {
					if !errors.Is(err, transfer.ErrEncryptionKey) {
						t.Fatalf("missing saved key reached source or changed cause: %v", err)
					}
					if f.snapshot(t) != before {
						t.Fatal("missing saved key changed selection, session or events")
					}
					return
				}
				if mode == "stricter_policy" || mode == "changed_size" || mode == "changed_mime" || mode == "changed_modtime" {
					if err == nil {
						t.Fatal("changed policy/source materialized selected upload")
					}
					if f.snapshot(t) != before {
						t.Fatal("refusal changed durable selection or events")
					}
					if mode != "changed_size" {
						f.preserved(t)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				session, err := f.uploads.Get(ctx, 1001, frozen.ID)
				if err != nil {
					t.Fatal(err)
				}
				if session.PartSize != frozen.PartSize || session.Name != frozen.Name || session.ExpectedSize != frozen.ExpectedSize || session.Encryption != frozen.Encryption || !session.FileID.Valid {
					t.Fatal("restart changed frozen policy")
				}
				if frozen.Encryption && session.EncryptionKeyVersion.Int32 != *frozen.EncryptionKeyVersion {
					t.Fatal("restart rekeyed selected upload")
				}
				f.download(t, uuid.UUID(session.FileID.Bytes), f.payload)
				if mode == "materialized_missing" {
					if _, err = f.db.Pool.Exec(ctx, "DELETE FROM upload_sessions WHERE id=$1", frozen.ID); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "removed_river" {
					if _, err = f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE id=ANY($1)", []int64{batch.ID, child.ID}); err != nil {
						t.Fatal(err)
					}
					if err = f.batch(t, batch); err != nil {
						t.Fatal(err)
					}
				}
				before = f.snapshot(t)
				err = fresh.Work(ctx, &retry)
				if mode == "materialized_missing" {
					if err == nil {
						t.Fatal("missing materialized upload recreated")
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if f.snapshot(t) != before {
					t.Fatal("replay changed IDs, bytes or events")
				}
				f.preserved(t)
			})
		}
	})
	for _, name := range []string{"source_failure_restart", "completed_replay", "materialized_missing", "removed_river", "s_committed_lost_ack", "s_known_rollback", "s_readback_unavailable", "s_process_loss", "s_parent_loss_before_b", "b_committed_lost_ack", "b_process_loss", "b_parent_loss", "b_known_rollback", "zero_byte", "multipart", "compatible_adoption", "matching_result", "concurrent_selection"} {
		t.Run(name, func(t *testing.T) {
			f := newImporterFixture(t)
			if name == "zero_byte" || name == "multipart" {
				f.payload = nil
				if name == "multipart" {
					f.payload = bytes.Repeat([]byte("m"), 64*1024*1024+17)
				}
				if err := os.WriteFile(f.location, f.payload, 0600); err != nil {
					t.Fatal(err)
				}
			}
			batch := f.admit(t, f.parent.String())
			if name == "multipart" || name == "compatible_adoption" {
				args := batch.Args
				args.BatchID = uuid.NewString()
				args.ChunkSize = 64 * 1024 * 1024
				args.Encryption = name == "compatible_adoption"
				batch = f.admitArgs(t, args)
			}
			if err := f.batch(t, batch); err != nil {
				t.Fatal(err)
			}
			child := f.child(t, batch)
			var adopted uuid.UUID
			if name == "compatible_adoption" {
				key := int32(2)
				mime := "text/plain"
				session, err := f.uploads.Create(context.Background(), uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: "source.txt", ExpectedSize: int64(len(f.payload)), MIMEType: &mime, ModTime: child.Args.Source.ModTime, ConflictPolicy: "replace", Encryption: true, EncryptionKeyVersion: &key, PartSize: 128 * 1024 * 1024})
				if err != nil {
					t.Fatal(err)
				}
				adopted = uuid.UUID(session.ID.Bytes)
			}
			if name == "concurrent_selection" {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				entered, release := make(chan struct{}), make(chan struct{})
				results := make(chan error, 2)
				var leader uint32
				worker := NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, nil, 1, false, []string{f.root})
				worker.commit = func(ctx context.Context, tx pgx.Tx) error {
					if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&leader); err != nil {
						return err
					}
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					return tx.Commit(ctx)
				}
				first, second := *child, *child
				go func() { results <- worker.Work(ctx, &first) }()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				go func() { results <- f.source(&second) }()
				receiptEventually(t, func() bool {
					var count int
					return f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))", leader).Scan(&count) == nil && count > 0
				})
				close(release)
				for range 2 {
					if err := <-results; err != nil {
						t.Fatal(err)
					}
				}
				var count int
				if err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM upload_sessions WHERE name='source.txt'").Scan(&count); err != nil || count != 1 {
					t.Fatalf("concurrent selection sessions=%d error=%v", count, err)
				}
			}
			if name == "b_committed_lost_ack" || name == "b_process_loss" || name == "b_parent_loss" || name == "b_known_rollback" {
				worker := NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, nil, 1, false, []string{f.root})
				ack := errors.New("synthetic B acknowledgement lost")
				if name == "b_known_rollback" {
					if _, err := f.db.Pool.Exec(context.Background(), "ALTER TABLE upload_sessions ADD CONSTRAINT importer_b_rollback CHECK(name<>'source.txt')"); err != nil {
						t.Fatal(err)
					}
				}
				worker.create = func(ctx context.Context, input uploads.CreateInput) (*sqlcgen.UploadSession, error) {
					_, err := f.uploads.Create(ctx, input)
					if err != nil {
						return nil, err
					}
					if name == "b_process_loss" {
						panic(ack)
					}
					if name == "b_parent_loss" {
						if _, err = f.catalog.Trash(ctx, 1001, f.parent); err != nil {
							return nil, err
						}
						if _, err = f.catalog.Restore(ctx, 1001, f.parent); err != nil {
							return nil, err
						}
					}
					return nil, ack
				}
				var err error
				var lost any
				func() { defer func() { lost = recover() }(); err = worker.Work(context.Background(), child) }()
				if name == "b_process_loss" {
					if lost != ack {
						t.Fatalf("B process loss=%v", lost)
					}
				} else if name != "b_known_rollback" && !errors.Is(err, ack) {
					t.Fatalf("B lost reply=%v", err)
				} else if name == "b_known_rollback" && err == nil {
					t.Fatal("B rollback succeeded")
				}
				if name == "b_known_rollback" {
					if _, err = f.db.Pool.Exec(context.Background(), "ALTER TABLE upload_sessions DROP CONSTRAINT importer_b_rollback"); err != nil {
						t.Fatal(err)
					}
				}
				var selected uuid.UUID
				var materialized bool
				if err = f.db.Pool.QueryRow(context.Background(), "SELECT upload_id,session_materialized FROM import_job_attempts WHERE job_id=$1", child.ID).Scan(&selected, &materialized); err != nil {
					t.Fatal(err)
				}
				if materialized == (name == "b_known_rollback") {
					t.Fatal("B outcome and materialization marker disagree")
				}
				if name == "b_parent_loss" {
					before := f.snapshot(t)
					if err = f.source(child); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatalf("B terminal replay=%v", err)
					}
					if before != f.snapshot(t) {
						t.Fatal("B terminal retry changed identity")
					}
					f.preserved(t)
					return
				}
				if err = f.source(child); err != nil {
					t.Fatal(err)
				}
				var actual uuid.UUID
				if err = f.db.Pool.QueryRow(context.Background(), "SELECT upload_id FROM import_job_attempts WHERE job_id=$1", child.ID).Scan(&actual); err != nil || actual != selected {
					t.Fatalf("B retry replaced selection: %v", err)
				}
			}
			if name == "s_committed_lost_ack" || name == "s_known_rollback" || name == "s_readback_unavailable" || name == "s_process_loss" || name == "s_parent_loss_before_b" {
				pool, service := f.db.Pool, f.uploads
				if name == "s_readback_unavailable" {
					var err error
					pool, err = pgxpool.New(context.Background(), f.db.URL)
					if err != nil {
						t.Fatal(err)
					}
					service = uploads.NewService(pool)
					if err = service.BindLifecycle(f.policy); err != nil {
						t.Fatal(err)
					}
				}
				worker := NewUploadSourceWorker(pool, f.catalog, service, f.pipeline, nil, 1, false, []string{f.root})
				ack := errors.New("synthetic selection acknowledgement lost")
				worker.commit = func(ctx context.Context, tx pgx.Tx) error {
					if name == "s_known_rollback" {
						if err := tx.Rollback(ctx); err != nil {
							return err
						}
						return pgx.ErrTxCommitRollback
					}
					if err := tx.Commit(ctx); err != nil {
						return err
					}
					if name == "s_process_loss" {
						panic(ack)
					}
					if name == "s_readback_unavailable" {
						pool.Close()
					}
					if name == "s_parent_loss_before_b" {
						if _, err := f.catalog.Trash(ctx, 1001, f.parent); err != nil {
							return err
						}
						if _, err := f.catalog.Restore(ctx, 1001, f.parent); err != nil {
							return err
						}
					}
					return ack
				}
				var err error
				var lost any
				func() { defer func() { lost = recover() }(); err = worker.Work(context.Background(), child) }()
				switch name {
				case "s_known_rollback":
					if !errors.Is(err, pgx.ErrTxCommitRollback) {
						t.Fatalf("known S rollback=%v", err)
					}
				case "s_readback_unavailable":
					if !errors.Is(err, ack) {
						t.Fatalf("unknown S outcome lost cause=%v", err)
					}
				case "s_process_loss":
					if lost != ack {
						t.Fatalf("S process loss=%v", lost)
					}
				case "s_parent_loss_before_b":
					if !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatalf("S loser materialized=%v", err)
					}
					before := f.snapshot(t)
					if err = f.source(child); !errors.Is(err, uploads.ErrParentUnavailable) {
						t.Fatalf("S loser revived=%v", err)
					}
					if before != f.snapshot(t) {
						t.Fatal("S loser changed")
					}
					f.preserved(t)
					return
				default:
					if err != nil {
						t.Fatal(err)
					}
				}
				var selected *uuid.UUID
				if err = f.db.Pool.QueryRow(context.Background(), "SELECT upload_id FROM import_job_attempts WHERE job_id=$1", child.ID).Scan(&selected); err != nil {
					t.Fatal(err)
				}
				if name == "s_known_rollback" && selected != nil {
					t.Fatal("S rollback retained selection")
				}
				if name != "s_known_rollback" && selected == nil {
					t.Fatal("committed S lost selected UUID")
				}
				if err = f.source(child); err != nil {
					t.Fatal(err)
				}
				if selected != nil {
					var actual uuid.UUID
					if err = f.db.Pool.QueryRow(context.Background(), "SELECT upload_id FROM import_job_attempts WHERE job_id=$1", child.ID).Scan(&actual); err != nil || actual != *selected {
						t.Fatalf("S selected another UUID: %v", err)
					}
				}
			}
			if name == "source_failure_restart" {
				if err := os.Rename(f.location, f.location+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := f.source(child); err == nil {
					t.Fatal("missing source succeeded")
				}
				if err := os.Rename(f.location+".saved", f.location); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.source(child); err != nil {
				t.Fatal(err)
			}
			var published uuid.UUID
			if err := f.db.Pool.QueryRow(context.Background(), "SELECT id FROM files WHERE parent_id=$1 AND name='source.txt' AND status='active'", f.parent).Scan(&published); err != nil {
				t.Fatal(err)
			}
			f.download(t, published, f.payload)
			if name == "compatible_adoption" {
				var selected uuid.UUID
				var size int64
				var key int32
				if err := f.db.Pool.QueryRow(context.Background(), "SELECT a.upload_id,s.part_size,s.encryption_key_version FROM import_job_attempts a JOIN upload_sessions s ON s.id=a.upload_id WHERE a.job_id=$1", child.ID).Scan(&selected, &size, &key); err != nil || selected != adopted || size != 128*1024*1024 || key != 2 {
					t.Fatalf("negotiated adoption changed: %s/%d/%d %v", selected, size, key, err)
				}
			}
			if name == "matching_result" {
				next := f.admit(t, f.parent.String())
				if err := f.batch(t, next); err != nil {
					t.Fatal(err)
				}
				matched := f.child(t, next)
				if err := f.source(matched); err != nil {
					t.Fatal(err)
				}
				var result uuid.UUID
				var count int
				if err := f.db.Pool.QueryRow(context.Background(), "SELECT result_file_id,(SELECT count(*) FROM upload_sessions) FROM import_job_attempts WHERE job_id=$1", matched.ID).Scan(&result, &count); err != nil || count != 1 || result == uuid.Nil {
					t.Fatalf("matching result was not durably selected: %v", err)
				}
				if err := os.Rename(f.location, f.location+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := f.source(matched); err != nil {
					t.Fatalf("matching replay accessed source: %v", err)
				}
				if err := os.Rename(f.location+".saved", f.location); err != nil {
					t.Fatal(err)
				}
			}
			before := f.snapshot(t)
			if name == "materialized_missing" {
				var id uuid.UUID
				if err := f.db.Pool.QueryRow(context.Background(), "SELECT upload_id FROM import_job_attempts WHERE job_id=$1", child.ID).Scan(&id); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.Pool.Exec(context.Background(), "DELETE FROM upload_sessions WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				before = f.snapshot(t)
				if err := f.source(child); err == nil {
					t.Fatal("missing materialized session recreated")
				}
			}
			if name == "removed_river" {
				if _, err := f.db.Pool.Exec(context.Background(), "DELETE FROM river_job WHERE id=ANY($1)", []int64{batch.ID, child.ID}); err != nil {
					t.Fatal(err)
				}
				if err := f.batch(t, batch); err != nil {
					t.Fatal(err)
				}
			}
			if name != "materialized_missing" {
				if err := f.source(child); err != nil {
					t.Fatal(err)
				}
			}
			if f.snapshot(t) != before {
				t.Fatal("retry changed selected upload/result identity")
			}
			f.preserved(t)
		})
	}
}

func TestB2ImporterLifecyclePolicy(t *testing.T) {
	for _, name := range []string{"leaf", "ancestor", "current_root_disabled", "explicit_root_control", "required_encryption_replacement"} {
		t.Run(name, func(t *testing.T) {
			f := newImporterFixture(t)
			destination := f.parent.String()
			if name == "explicit_root_control" {
				destination = "/"
			}
			batch := f.admit(t, destination)
			if err := f.batch(t, batch); err != nil {
				t.Fatal(err)
			}
			child := f.child(t, batch)
			if name == "required_encryption_replacement" {
				if err := f.source(child); err != nil {
					t.Fatal(err)
				}
				old := f.storage.Snapshot()
				next := f.admit(t, f.parent.String())
				if err := f.batch(t, next); err != nil {
					t.Fatal(err)
				}
				encrypted := f.child(t, next)
				worker := NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, nil, 1, true, []string{f.root})
				if err := worker.Work(context.Background(), encrypted); err != nil {
					t.Fatal(err)
				}
				var id uuid.UUID
				var required bool
				var key int32
				if err := f.db.Pool.QueryRow(context.Background(), "SELECT id,encryption,encryption_key_version FROM files WHERE parent_id=$1 AND name='source.txt' AND status='active'", f.parent).Scan(&id, &required, &key); err != nil || !required || key != 1 {
					t.Fatalf("matching plaintext bypassed effective encryption: %v", err)
				}
				f.download(t, id, f.payload)
				for _, saved := range old {
					got, ok := f.storage.Bytes(saved.ChannelID, saved.MessageID)
					if !ok || sha256.Sum256(got) != saved.SHA256 {
						t.Fatal("encrypted replacement destroyed old bytes")
					}
				}
				if err := worker.Work(context.Background(), encrypted); err != nil {
					t.Fatal(err)
				}
				f.preserved(t)
				return
			}
			if name == "explicit_root_control" {
				if err := f.source(child); err != nil {
					t.Fatal(err)
				}
				f.preserved(t)
				return
			}
			outside := t.TempDir()
			foreign := filepath.Join(outside, "source.txt")
			if err := os.WriteFile(foreign, []byte("foreign bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t)
			roots := []string{f.root}
			if name == "leaf" {
				if err := os.Rename(f.location, f.location+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(foreign, f.location); err != nil {
					t.Fatal(err)
				}
			}
			if name == "ancestor" {
				if err := os.Rename(f.root, f.root+".saved"); err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(f.root + ".saved")
				if err := os.Symlink(outside, f.root); err != nil {
					t.Fatal(err)
				}
			}
			if name == "current_root_disabled" {
				roots = nil
			}
			worker := NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, nil, 1, false, roots)
			if err := worker.Work(context.Background(), child); err == nil {
				t.Fatal("escaped/disabled source succeeded")
			}
			if f.snapshot(t) != before {
				t.Fatal("rejected source selected or published upload")
			}
			got, err := os.ReadFile(foreign)
			if err != nil || string(got) != "foreign bytes" {
				t.Fatal("foreign bytes changed")
			}
		})
	}
	for _, boundary := range []string{"http_before_s", "http_terminal_midstream", "http_encrypted_terminal_midstream"} {
		t.Run(boundary, func(t *testing.T) {
			f := newImporterFixture(t)
			ctx := context.Background()
			original := f.admit(t, f.parent.String())
			if boundary == "http_encrypted_terminal_midstream" {
				args := original.Args
				args.BatchID = uuid.NewString()
				args.Encryption = true
				original = f.admitArgs(t, args)
			}
			if err := f.batch(t, original); err != nil {
				t.Fatal(err)
			}
			if err := f.source(f.child(t, original)); err != nil {
				t.Fatal(err)
			}
			oldBytes := f.storage.Snapshot()
			var oldID uuid.UUID
			if err := f.db.Pool.QueryRow(ctx, "SELECT id FROM files WHERE parent_id=$1 AND name='source.txt' AND status='active'", f.parent).Scan(&oldID); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.Pool.Exec(ctx, "INSERT INTO file_shares(file_id,owner_id,token_prefix,token_hash) VALUES($1,1001,'synthetic',$2)", oldID, []byte(uuid.NewString())); err != nil {
				t.Fatal(err)
			}
			f.download(t, oldID, f.payload)
			oldSnapshot := func() string {
				var value string
				if err := f.db.Pool.QueryRow(ctx, `SELECT jsonb_build_array(
    (SELECT to_jsonb(f) FROM files f WHERE id=$1),
    (SELECT jsonb_agg(to_jsonb(p) ORDER BY part_no) FROM file_parts p WHERE file_id=$1),
    (SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM file_shares s WHERE file_id=$1),
    (SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM user_events e WHERE resource_id=$1::text))::text`, oldID).Scan(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			afterDeletion := ""
			payload := bytes.Repeat([]byte("new representation"), 128)
			gets, closed := 0, 0
			cleanup := errors.New("synthetic cleanup unavailable")
			closeFailure := errors.New("synthetic response close failed")
			terminalize := func() {
				if _, err := f.catalog.Trash(ctx, 1001, f.parent); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.Restore(ctx, 1001, f.parent); err != nil {
					t.Fatal(err)
				}
				afterDeletion = oldSnapshot()
			}
			client := &http.Client{Transport: importerTransport(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodHead {
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/plain"}}, ContentLength: int64(len(payload)), Body: io.NopCloser(bytes.NewReader(nil))}, nil
				}
				gets++
				if request.Method != http.MethodGet || request.Header.Get("Range") != "" || request.Header.Get("Accept-Encoding") != "identity" {
					t.Fatal("source did not use one unconditional identity representation")
				}
				body := &importLifecycleBody{reader: bytes.NewReader(payload), closed: &closed, closeError: closeFailure}
				if boundary == "http_before_s" {
					terminalize()
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/plain"}}, ContentLength: int64(len(payload)), Body: body}, nil
			})}
			batch := f.admitArgs(t, UploadBatchArgs{BatchID: uuid.NewString(), UserID: 1001, Destination: f.parent.String(), Encryption: boundary == "http_encrypted_terminal_midstream", Sources: []UploadSource{{Type: "http", URL: "https://synthetic.invalid/source", DestinationPath: "source.txt"}}})
			if err := NewUploadBatchWorker(f.db.Pool, client, f.catalog, f.uploads, []string{f.root}).Work(rivertest.WorkContext(ctx, f.runtime.client.Client), batch); err != nil {
				t.Fatal(err)
			}
			child := f.child(t, batch)
			worker := NewUploadSourceWorker(f.db.Pool, f.catalog, f.uploads, f.pipeline, client, 1, false, []string{f.root})
			var err error
			if boundary == "http_before_s" {
				err = worker.Work(ctx, child)
			} else {
				barrier := fixture.NewBarrier(t, nil)
				f.storage.SetSendFaults(fixture.SendFaults{AfterUpload: barrier})
				f.storage.SetDeleteFaults(fixture.DeleteFaults{Error: cleanup})
				done := make(chan error, 1)
				go func() { done <- worker.Work(ctx, child) }()
				if waitErr := barrier.Wait(ctx); waitErr != nil {
					t.Fatal(waitErr)
				}
				terminalize()
				barrier.Release()
				err = <-done
			}
			if !errors.Is(err, uploads.ErrParentUnavailable) || !errors.Is(err, closeFailure) {
				t.Fatalf("primary/close causes lost: %v", err)
			}
			if boundary != "http_before_s" && !errors.Is(err, cleanup) {
				t.Fatalf("cleanup cause lost: %v", err)
			}
			if gets != 1 || closed != 1 {
				t.Fatalf("source GET/close=%d/%d", gets, closed)
			}
			before := f.snapshot(t)
			if err = worker.Work(ctx, child); !errors.Is(err, uploads.ErrParentUnavailable) {
				t.Fatalf("terminal HTTP retry=%v", err)
			}
			if gets != 1 || before != f.snapshot(t) {
				t.Fatal("terminal retry reopened representation or changed identity")
			}
			for _, saved := range oldBytes {
				actual, ok := f.storage.Bytes(saved.ChannelID, saved.MessageID)
				if !ok || sha256.Sum256(actual) != saved.SHA256 {
					t.Fatal("old destination/independent bytes changed")
				}
			}
			var active uuid.UUID
			if err = f.db.Pool.QueryRow(ctx, "SELECT id FROM files WHERE parent_id=$1 AND name='source.txt' AND status='active'", f.parent).Scan(&active); err != nil || active != oldID {
				t.Fatalf("terminal import replaced old file: %v", err)
			}
			if afterDeletion == "" || oldSnapshot() != afterDeletion {
				t.Fatal("failed import changed old file/parts/shares/events/cipher metadata after hierarchy commits")
			}
			f.preserved(t)
		})
	}
}

type importLifecycleBody struct {
	reader     io.Reader
	beforeRead func()
	once       sync.Once
	closed     *int
	closeError error
}

func (b *importLifecycleBody) Read(p []byte) (int, error) {
	b.once.Do(func() {
		if b.beforeRead != nil {
			b.beforeRead()
		}
	})
	return b.reader.Read(p)
}
func (b *importLifecycleBody) Close() error { (*b.closed)++; return b.closeError }

func (f *importerFixture) download(t *testing.T, id uuid.UUID, want []byte) {
	t.Helper()
	downloader := transfer.NewDownloader(f.catalog, f.storage, transfer.StaticKeyProvider{1: "synthetic-import-key", 2: "synthetic-older-import-key"})
	result, err := downloader.Open(context.Background(), transfer.DownloadRequest{UserID: 1001, FileID: id, Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	size, err := io.Copy(hash, result.Reader)
	closeErr := result.Reader.Close()
	var actual [32]byte
	hash.Sum(actual[:0])
	if err != nil || closeErr != nil || size != int64(len(want)) || actual != sha256.Sum256(want) {
		t.Fatalf("independent full download changed: size=%d error=%v close=%v", size, err, closeErr)
	}
}
