//go:build integration

package jobs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/rivertest"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func TestUploadBatchWorkerCreatesDestinationPath(t *testing.T) {
	f := newImporterFixture(t)
	job := f.admit(t, "/videos/new")
	if _, err := f.catalog.ResolveFolderPath(context.Background(), 1001, nil, "/videos/new"); err == nil {
		t.Fatal("Q created destination")
	}
	if err := f.batch(t, job); err != nil {
		t.Fatal(err)
	}
	resolved, err := f.catalog.ResolveFolderPath(context.Background(), 1001, nil, "/videos/new")
	if err != nil || resolved == nil {
		t.Fatalf("actual atomic dispatch did not create destination: %v", err)
	}
	child := f.child(t, job)
	if child.Args.ParentID != resolved.String() {
		t.Fatal("actual child has another parent")
	}
}

func TestB2ImporterBatchReplay(t *testing.T) {
	for _, name := range []string{"dispatched_changed_source", "removed_children", "terminal_child", "concurrent_dispatch", "empty_dispatch", "child_insert_rollback", "association_rollback", "committed_lost_ack", "rollback_before_commit", "readback_unavailable", "process_loss_after_a", "missing_admission", "changed_request", "wrong_owner"} {
		t.Run(name, func(t *testing.T) {
			f := newImporterFixture(t)
			ctx := context.Background()
			job := f.admit(t, "/new/folder")
			if name == "missing_admission" || name == "changed_request" || name == "wrong_owner" {
				if name == "missing_admission" {
					job.Args.BatchID = uuid.NewString()
					raw, err := f.runtime.client.Insert(ctx, job.Args, nil)
					if err != nil {
						t.Fatal(err)
					}
					job.JobRow = raw.Job
				}
				if name == "changed_request" {
					job.Args.Destination = "/another/path"
				}
				if name == "wrong_owner" {
					job.Args.UserID = 1002
				}
				if err := os.Rename(f.location, f.location+".saved"); err != nil {
					t.Fatal(err)
				}
				before := f.snapshot(t)
				err := f.batch(t, job)
				if name == "changed_request" {
					if !errors.Is(err, lifecycle.ErrRequestMismatch) {
						t.Fatalf("changed Q request=%v", err)
					}
				} else if !errors.Is(err, uploads.ErrNotFound) {
					t.Fatalf("unassociated job reached source or path: %v", err)
				}
				if before != f.snapshot(t) {
					t.Fatal("unassociated job mutated admission")
				}
				if err = os.Rename(f.location+".saved", f.location); err != nil {
					t.Fatal(err)
				}
				f.preserved(t)
				return
			}
			if name == "empty_dispatch" {
				args := job.Args
				args.BatchID = uuid.NewString()
				args.Exclude = []string{"**"}
				job = f.admitArgs(t, args)
			}
			if name == "committed_lost_ack" || name == "rollback_before_commit" || name == "readback_unavailable" || name == "process_loss_after_a" {
				pool := f.db.Pool
				service := f.uploads
				if name == "readback_unavailable" {
					var err error
					pool, err = pgxpool.New(ctx, f.db.URL)
					if err != nil {
						t.Fatal(err)
					}
					service = uploads.NewService(pool)
					if err = service.BindLifecycle(f.policy); err != nil {
						t.Fatal(err)
					}
				}
				worker := NewUploadBatchWorker(pool, nil, f.catalog, service, []string{f.root})
				ack := errors.New("synthetic dispatch acknowledgement lost")
				worker.commit = func(ctx context.Context, tx pgx.Tx) error {
					if name == "rollback_before_commit" {
						if err := tx.Rollback(ctx); err != nil {
							return err
						}
						return pgx.ErrTxCommitRollback
					}
					if err := tx.Commit(ctx); err != nil {
						return err
					}
					if name == "readback_unavailable" {
						pool.Close()
					}
					if name == "process_loss_after_a" {
						panic(ack)
					}
					return ack
				}
				var err error
				var lost any
				func() {
					defer func() { lost = recover() }()
					err = worker.Work(rivertest.WorkContext(ctx, f.runtime.client.Client), job)
				}()
				switch name {
				case "rollback_before_commit":
					if !errors.Is(err, pgx.ErrTxCommitRollback) {
						t.Fatalf("known A rollback=%v", err)
					}
					var n int
					if err = f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM import_job_attempts").Scan(&n); err != nil || n != 0 {
						t.Fatalf("rolled back attempts=%d error=%v", n, err)
					}
				case "readback_unavailable":
					if !errors.Is(err, ack) {
						t.Fatalf("unknown A outcome lost cause: %v", err)
					}
				case "process_loss_after_a":
					if lost != ack {
						t.Fatalf("A process loss=%v", lost)
					}
				default:
					if err != nil {
						t.Fatalf("committed A was not reconciled: %v", err)
					}
				}
			}
			if name == "child_insert_rollback" || name == "association_rollback" {
				table, check := "river_job", "kind<>'"+UploadSourceKind+"'"
				if name == "association_rollback" {
					table, check = "import_job_attempts", "source_index<0"
				}
				if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE "+table+" ADD CONSTRAINT importer_reject CHECK("+check+")"); err != nil {
					t.Fatal(err)
				}
				if err := f.batch(t, job); err == nil {
					t.Fatal("A failure succeeded")
				}
				var folders, children, attempts int
				if err := f.db.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM files WHERE name IN ('new','folder')),(SELECT count(*) FROM river_job WHERE kind=$1),(SELECT count(*) FROM import_job_attempts)`, UploadSourceKind).Scan(&folders, &children, &attempts); err != nil {
					t.Fatal(err)
				}
				if folders+children+attempts != 0 {
					t.Fatal("A leaked folder, child or association")
				}
				if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE "+table+" DROP CONSTRAINT importer_reject"); err != nil {
					t.Fatal(err)
				}
			}
			if name == "concurrent_dispatch" {
				var wg sync.WaitGroup
				results := make(chan error, 2)
				for range 2 {
					wg.Add(1)
					go func() { defer wg.Done(); results <- f.batch(t, job) }()
				}
				wg.Wait()
				close(results)
				for err := range results {
					if err != nil {
						t.Fatal(err)
					}
				}
			} else if err := f.batch(t, job); err != nil {
				t.Fatal(err)
			}
			if name == "empty_dispatch" {
				var count int
				if err := f.db.Pool.QueryRow(ctx, "SELECT source_count FROM import_batches WHERE batch_id=$1", job.Args.BatchID).Scan(&count); err != nil || count != 0 {
					t.Fatalf("empty dispatch count=%d error=%v", count, err)
				}
				if err := f.batch(t, job); err != nil {
					t.Fatal(err)
				}
				return
			}
			child := f.child(t, job)
			if name == "terminal_child" {
				parent := uuid.MustParse(child.Args.ParentID)
				if _, err := f.catalog.Trash(ctx, 1001, parent); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.Restore(ctx, 1001, parent); err != nil {
					t.Fatal(err)
				}
				if err := f.source(child); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatalf("loser child=%v", err)
				}
				if _, err := f.runtime.client.JobCancel(ctx, child.ID); err != nil {
					t.Fatal(err)
				}
			}
			before := f.snapshot(t)
			if name == "removed_children" || name == "terminal_child" {
				if _, err := f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE id=ANY($1)", []int64{job.ID, child.ID}); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(f.root, "added.txt"), []byte("must not expand"), 0600); err != nil {
				t.Fatal(err)
			}
			err := f.batch(t, job)
			if name == "terminal_child" {
				if !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatalf("terminal batch=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if before != f.snapshot(t) {
				t.Fatal("batch replay changed durable dispatch")
			}
			var attempts int
			if err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM import_job_attempts WHERE batch_id=$1", job.Args.BatchID).Scan(&attempts); err != nil || attempts != 1 {
				t.Fatalf("source attempts=%d error=%v", attempts, err)
			}
			f.preserved(t)
		})
	}
}

type importerTransport func(*http.Request) (*http.Response, error)

func (f importerTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestB2ImportAdmissionFence(t *testing.T) {
	for _, name := range []string{"uuid_trash_restore_before_a", "ancestor_trash_restore_before_a", "purge_before_a", "absolute_nearest_anchor", "relative_source_anchor", "preflight_failure_restart", "blocked_preflight_resume", "root_and_rollback"} {
		t.Run(name, func(t *testing.T) {
			f := newImporterFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var calls atomic.Int32
			entered := make(chan struct{})
			release := make(chan struct{})
			client := &http.Client{Transport: importerTransport(func(r *http.Request) (*http.Response, error) {
				n := calls.Add(1)
				if name == "blocked_preflight_resume" && n == 1 {
					close(entered)
					select {
					case <-release:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
				}
				if name == "preflight_failure_restart" && n == 1 {
					return nil, errors.New("synthetic preflight failure")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/plain"}}, ContentLength: int64(len(f.payload)), Body: io.NopCloser(strings.NewReader(""))}, nil
			})}
			destination := f.parent.String()
			spec := "source.txt"
			target := f.parent
			switch name {
			case "absolute_nearest_anchor":
				destination = "/destination/missing/child"
			case "relative_source_anchor":
				child, err := f.catalog.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, ParentID: &f.parent, Name: "pinned"})
				if err != nil {
					t.Fatal(err)
				}
				target = uuid.UUID(child.ID.Bytes)
				spec = "pinned/source.txt"
			case "ancestor_trash_restore_before_a":
				ancestor, err := f.catalog.CreateFolder(ctx, catalog.CreateFolderInput{UserID: 1001, Name: "ancestor"})
				if err != nil {
					t.Fatal(err)
				}
				target = uuid.UUID(ancestor.ID.Bytes)
				if _, err = f.catalog.Move(ctx, 1001, f.parent, &target, nil); err != nil {
					t.Fatal(err)
				}
			case "root_and_rollback":
				destination = "/"
			}
			args := UploadBatchArgs{BatchID: uuid.NewString(), UserID: 1001, Destination: destination, Sources: []UploadSource{{Type: "http", URL: "https://synthetic.invalid/source", DestinationPath: spec}}}
			job := f.admitArgs(t, args)
			work := func() error {
				return NewUploadBatchWorker(f.db.Pool, client, f.catalog, f.uploads, []string{f.root}).Work(rivertest.WorkContext(ctx, f.runtime.client.Client), job)
			}
			if calls.Load() != 0 {
				t.Fatal("Q accessed source")
			}
			var blocked chan error
			if name == "blocked_preflight_resume" {
				blocked = make(chan error, 1)
				go func() { blocked <- work() }()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if name == "preflight_failure_restart" {
				if err := work(); err == nil {
					t.Fatal("preflight failure succeeded")
				}
			}
			if name == "preflight_failure_restart" {
				if _, err := f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE id=$1", job.ID); err != nil {
					t.Fatal(err)
				}
				retained, err := f.runtime.InsertUploadBatch(ctx, job.Args)
				if err != nil || retained.ID != job.ID || retained.State != "discarded" {
					t.Fatalf("removed undispatched Q=%+v error=%v", retained, err)
				}
			}
			if name == "root_and_rollback" {
				for _, boundary := range []string{"q_commit_lost_ack", "q_known_rollback"} {
					t.Run(boundary, func(t *testing.T) {
						control := newImporterFixture(t)
						request := UploadBatchArgs{BatchID: uuid.NewString(), UserID: 1001, Destination: control.parent.String(), Sources: []UploadSource{{Type: "local", Path: control.location, DestinationPath: "source.txt"}}}
						ack := errors.New("synthetic Q acknowledgement lost")
						control.runtime.commit = func(ctx context.Context, tx pgx.Tx) error {
							if boundary == "q_known_rollback" {
								if err := tx.Rollback(ctx); err != nil {
									return err
								}
								return pgx.ErrTxCommitRollback
							}
							if err := tx.Commit(ctx); err != nil {
								return err
							}
							return ack
						}
						job, err := control.runtime.InsertUploadBatch(ctx, request)
						if boundary == "q_known_rollback" {
							if !errors.Is(err, pgx.ErrTxCommitRollback) {
								t.Fatalf("Q rollback=%v", err)
							}
							var count int
							if err = control.db.Pool.QueryRow(ctx, "SELECT count(*) FROM import_batches").Scan(&count); err != nil || count != 0 {
								t.Fatalf("Q rollback retained admission=%d error=%v", count, err)
							}
						} else if err != nil || job.ID <= 0 {
							t.Fatalf("Q committed outcome=%+v error=%v", job, err)
						}
						control.runtime.commit = func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) }
						replayed, err := control.runtime.InsertUploadBatch(ctx, request)
						if err != nil {
							t.Fatal(err)
						}
						if boundary != "q_known_rollback" && replayed.ID != job.ID {
							t.Fatal("Q ACK loss inserted replacement job")
						}
						var folders, attempts int
						if err = control.db.Pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM files),(SELECT count(*) FROM import_job_attempts)").Scan(&folders, &attempts); err != nil || folders != 1 || attempts != 0 {
							t.Fatalf("Q created paths/children: %d/%d %v", folders, attempts, err)
						}
					})
				}
				if err := work(); err != nil {
					t.Fatal(err)
				}
				rollbackArgs := args
				rollbackArgs.BatchID = uuid.NewString()
				rollbackArgs.Destination = f.parent.String()
				rollbackJob := f.admitArgs(t, rollbackArgs)
				if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE files ADD CONSTRAINT importer_trash_rollback CHECK(status<>'trashed')"); err != nil {
					t.Fatal(err)
				}
				if _, err := f.catalog.Trash(ctx, 1001, f.parent); err == nil {
					t.Fatal("forced hierarchy rollback succeeded")
				}
				if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE files DROP CONSTRAINT importer_trash_rollback"); err != nil {
					t.Fatal(err)
				}
				if err := NewUploadBatchWorker(f.db.Pool, client, f.catalog, f.uploads, []string{f.root}).Work(rivertest.WorkContext(ctx, f.runtime.client.Client), rollbackJob); err != nil {
					t.Fatalf("known deletion rollback poisoned Q: %v", err)
				}
				f.preserved(t)
				return
			}
			if _, err := f.catalog.Trash(ctx, 1001, target); err != nil {
				t.Fatal(err)
			}
			if name == "purge_before_a" {
				ops, err := fileops.NewService(f.db.Pool, f.catalog, channels.NewService(f.db.Pool, nil, channels.Config{}), f.storage)
				if err != nil {
					t.Fatal(err)
				}
				if err = ops.BindLifecycle(f.policy); err != nil {
					t.Fatal(err)
				}
				if err = ops.QueuePurge(ctx, 1001, target); err != nil {
					t.Fatal(err)
				}
				if err = ops.PurgeMany(ctx, 1001, []uuid.UUID{target}); err != nil {
					t.Fatal(err)
				}
			} else if _, err := f.catalog.Restore(ctx, 1001, target); err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t)
			beforeCalls := calls.Load()
			if blocked != nil {
				close(release)
				if err := <-blocked; !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatalf("blocked continuation=%v", err)
				}
			}
			for range 2 {
				if err := work(); !errors.Is(err, uploads.ErrParentUnavailable) {
					t.Fatalf("terminal replay=%v", err)
				}
			}
			if calls.Load() != beforeCalls {
				t.Fatal("terminal Q reopened source")
			}
			if before != f.snapshot(t) {
				t.Fatal("terminal Q changed admission/attempt/session")
			}
			var children int
			if err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM import_job_attempts WHERE batch_id=$1", args.BatchID).Scan(&children); err != nil || children != 0 {
				t.Fatalf("terminal Q dispatched children=%d error=%v", children, err)
			}
			f.preserved(t)
		})
	}
}
