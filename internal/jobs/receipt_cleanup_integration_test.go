//go:build integration

package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/divyam234/riverpro"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertest"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/fileops"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

type receiptRuntimeFixture struct {
	db      *testpostgres.Database
	storage *fixture.Storage
	policy  *lifecycle.Service
	runtime *Runtime
	catalog *catalog.Service
	uploads *uploads.Service
	source  uuid.UUID
	parent  uuid.UUID
	started bool
}

func newReceiptRuntimeFixture(t *testing.T) *receiptRuntimeFixture {
	t.Helper()
	db := testpostgres.New(t)
	ctx := context.Background()
	for _, sql := range []string{
		"INSERT INTO users(user_id) VALUES(1001),(1002)",
		"INSERT INTO channels(channel_id,user_id,name,selected) VALUES(9001,1001,'synthetic',true)",
	} {
		if _, err := db.Pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	storage := fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}})
	for _, m := range []fixture.Message{
		{UserID: 1001, ChannelID: 9001, MessageID: 10, Bytes: []byte("source bytes"), Preserve: true},
		{UserID: 1002, ChannelID: 9001, MessageID: 20, Bytes: []byte("foreign bytes"), Preserve: true},
		{UserID: 1001, ChannelID: 9001, MessageID: 30, Bytes: []byte("winner bytes"), Preserve: true},
	} {
		if err := storage.Seed(m); err != nil {
			t.Fatal(err)
		}
	}
	f := &receiptRuntimeFixture{db: db, storage: storage, source: uuid.New(), parent: uuid.New()}
	if _, err := db.Pool.Exec(ctx, `INSERT INTO files(id,user_id,name,kind,status,size,mod_time) VALUES
($1,1001,'source.bin','file','active',12,clock_timestamp()),
($2,1001,'destination','folder','active',0,clock_timestamp())`, f.source, f.parent); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size) VALUES($1,1,9001,10,12,12)", f.source); err != nil {
		t.Fatal(err)
	}
	f.compose(t)
	t.Cleanup(func() {
		if f.started {
			stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := f.runtime.Stop(stop); err != nil {
				t.Error(err)
			}
		}
	})
	return f
}

func (f *receiptRuntimeFixture) compose(t *testing.T) {
	t.Helper()
	f.policy = lifecycle.NewService(f.db.Pool, f.storage)
	var err error
	f.runtime, err = NewRuntime(f.db.Pool, f.storage, f.policy)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.policy.BindReceiptEnqueuer(f.runtime); err != nil {
		t.Fatal(err)
	}
	f.catalog = catalog.NewService(f.db.Pool, nil)
	f.uploads = uploads.NewService(f.db.Pool)
	if err = f.catalog.BindLifecycle(f.policy); err != nil {
		t.Fatal(err)
	}
	if err = f.uploads.BindLifecycle(f.policy); err != nil {
		t.Fatal(err)
	}
}
func (f *receiptRuntimeFixture) start(t *testing.T) {
	t.Helper()
	if f.started {
		t.Fatal("fixture attempted repeated Start")
	}
	if err := f.runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.started = true
}
func (f *receiptRuntimeFixture) restart(t *testing.T) {
	t.Helper()
	if f.started {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.runtime.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		f.started = false
	}
	f.compose(t)
	f.start(t)
}
func receiptEventually(t *testing.T, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for !check() {
		if err := ctx.Err(); err != nil {
			t.Fatal("bounded persisted scheduler observation failed")
		}
		runtime.Gosched()
	}
}
func (f *receiptRuntimeFixture) due(t *testing.T) {
	t.Helper()
	tag, err := f.db.Pool.Exec(context.Background(), "UPDATE river_periodic_job SET next_run_at=clock_timestamp()-interval '1 second' WHERE id=$1", receiptCleanupPeriodicID)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatal("required receipt schedule is missing")
	}
}
func (f *receiptRuntimeFixture) preserved(t *testing.T) {
	t.Helper()
	for id, want := range map[int64]string{10: "source bytes", 20: "foreign bytes", 30: "winner bytes"} {
		got, ok := f.storage.Bytes(9001, id)
		if !ok || sha256.Sum256(got) != sha256.Sum256([]byte(want)) {
			t.Fatalf("independent SHA-256 changed for %d", id)
		}
	}
	if err := f.storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
}

// The tracer faults only after the actual Observe COMMIT has succeeded. Panic
// models process loss and deliberately bypasses Copy's ordinary compensation.
type receiptObserveCrash struct {
	mu        sync.Mutex
	observed  map[uint32]bool
	armed     atomic.Bool
	hooks     fixture.Hooks
	queryName string
}
type receiptTraceSQL struct{}

func (c *receiptObserveCrash) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, receiptTraceSQL{}, data.SQL)
}
func (c *receiptObserveCrash) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if data.Err != nil || !c.armed.Load() {
		return
	}
	sql, _ := ctx.Value(receiptTraceSQL{}).(string)
	pid := conn.PgConn().PID()
	c.mu.Lock()
	queryName := c.queryName
	if queryName == "" {
		queryName = "ObserveMessageReceipt"
	}
	if strings.Contains(sql, "-- name: "+queryName) {
		c.observed[pid] = true
	}
	crash := strings.EqualFold(strings.TrimSpace(sql), "commit") && c.observed[pid]
	if crash {
		delete(c.observed, pid)
		c.armed.Store(false)
	}
	c.mu.Unlock()
	if crash {
		gate := c.hooks.ObserveCommittedBeforeStage
		if c.queryName == "ReserveCopyOperation" {
			gate = c.hooks.CopyOperationReserved
		}
		if err := gate.Pause(ctx); !errors.Is(err, fixture.ErrProcessLost) {
			panic(fmt.Errorf("unexpected crash boundary: %w", err))
		}
		panic(fixture.ErrProcessLost)
	}
}
func (f *receiptRuntimeFixture) copyCrash(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	gate := fixture.NewBarrier(t, fixture.ErrProcessLost)
	gate.Release()
	tracer := &receiptObserveCrash{observed: make(map[uint32]bool), hooks: fixture.Hooks{ObserveCommittedBeforeStage: gate}}
	tracer.armed.Store(true)
	cfg, err := pgxpool.ParseConfig(f.db.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	policy := lifecycle.NewService(pool, f.storage)
	if err = policy.BindReceiptEnqueuer(f.runtime); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewService(pool, nil)
	if err = cat.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	ops, err := fileops.NewService(pool, cat, channels.NewService(pool, nil, channels.Config{PartLimit: 10000}), f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = ops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	var lost any
	func() {
		defer func() { lost = recover() }()
		name := uuid.NewString()
		_, err = ops.Copy(ctx, fileops.CopyInput{UserID: 1001, FileID: f.source, ParentID: &f.parent, Name: &name})
	}()
	if lost != fixture.ErrProcessLost {
		t.Fatalf("actual Copy did not lose process after Observe: panic=%v error=%v", lost, err)
	}
	var observed, jobs, refs int
	if err = f.db.Pool.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM remote_write_receipts r WHERE row_kind='message_attempt' AND state='observed' AND message_id IS NOT NULL AND job_id IS NULL AND NOT EXISTS(SELECT 1 FROM file_parts p WHERE p.channel_id=r.channel_id AND p.message_id=r.message_id)),
 (SELECT count(*) FROM river_job WHERE kind=$1),
 (SELECT count(*) FROM file_parts p JOIN remote_write_receipts r ON r.channel_id=p.channel_id AND r.message_id=p.message_id WHERE r.message_id=(SELECT max(message_id) FROM remote_write_receipts WHERE state='observed'))`, ReceiptCleanupKind).Scan(&observed, &jobs, &refs); err != nil {
		t.Fatal(err)
	}
	if observed < 1 || jobs != 0 || refs != 0 {
		t.Fatalf("not ledger-only observed debt: observed=%d jobs=%d references=%d", observed, jobs, refs)
	}
	// Expiry is a fixture-owned clock boundary, not a new cleanup authorization.
	if _, err = f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE operation_id=(SELECT operation_id FROM remote_write_receipts WHERE row_kind='message_attempt' AND state='observed' ORDER BY message_id DESC LIMIT 1) AND published_at IS NULL"); err != nil {
		t.Fatal(err)
	}
}
func (f *receiptRuntimeFixture) assertRecovered(t *testing.T) {
	t.Helper()
	f.due(t)
	receiptEventually(t, func() bool {
		var pending int
		err := f.db.Pool.QueryRow(context.Background(), "SELECT count(*) FROM remote_write_receipts r WHERE row_kind='message_attempt' AND message_id IS NOT NULL AND state NOT IN ('bound','deleted') AND NOT EXISTS(SELECT 1 FROM file_parts p WHERE p.channel_id=r.channel_id AND p.message_id=r.message_id)").Scan(&pending)
		return err == nil && pending == 0
	})
	var message, first, current, owner int64
	if err := f.db.Pool.QueryRow(context.Background(), "SELECT message_id,job_id,wakeup_job_id,user_id FROM remote_write_receipts WHERE row_kind='message_attempt' AND state='deleted' ORDER BY message_id DESC LIMIT 1").Scan(&message, &first, &current, &owner); err != nil {
		t.Fatal(err)
	}
	if first <= 0 || current <= 0 || owner != 1001 {
		t.Fatal("missing actual positive first/current owner job identity")
	}
	if _, ok := f.storage.Bytes(9001, message); ok {
		t.Fatal("exact observed destination bytes remain")
	}
	item, err := f.runtime.GetForUser(context.Background(), first, 1001)
	if err != nil || item.ID != first {
		t.Fatalf("first owner report: %v", err)
	}
	if _, err = f.runtime.GetForUser(context.Background(), first, 1002); !errors.Is(err, river.ErrNotFound) {
		t.Fatalf("foreign report leak: %v", err)
	}
	items, _, err := f.runtime.List(context.Background(), ListInput{UserID: 1001, Kind: ReceiptCleanupKind, Limit: 200})
	if err != nil || len(items) == 0 {
		t.Fatalf("owner List missing recovery: %v", err)
	}
	stats, err := f.runtime.StatisticsForUser(context.Background(), 1001)
	if err != nil || stats.Completed+stats.Discarded+stats.Pending+stats.Retryable == 0 {
		t.Fatalf("owner statistics: %+v %v", stats, err)
	}
	receiptEventually(t, func() bool {
		var active int
		return f.db.Pool.QueryRow(context.Background(), "SELECT count(*) FROM river_job WHERE kind=$1 AND state NOT IN ('completed','discarded','cancelled')", ReceiptCleanupKind).Scan(&active) == nil && active == 0
	})
	if _, err = f.db.Pool.Exec(context.Background(), "DELETE FROM river_job WHERE kind=$1", ReceiptCleanupKind); err != nil {
		t.Fatal(err)
	}
	item, err = f.runtime.GetForUser(context.Background(), first, 1001)
	if err != nil || item.ID != first || item.State != "completed" {
		t.Fatalf("retained first report: %+v %v", item, err)
	}
	f.preserved(t)
}
func (f *receiptRuntimeFixture) noReceipt(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	op, result, token := uuid.New(), uuid.New(), uuid.New()
	tx, err := f.db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = f.policy.ReserveCopyTx(ctx, tx, lifecycle.ReserveCopyInput{UserID: 1001, OperationID: op, RootResultFileID: result, LeaseToken: token, DestinationParentID: &f.parent, LeaseExpiresAt: time.Now().Add(time.Minute).UTC()})
	if err == nil {
		err = tx.Commit(ctx)
	}
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.policy.Reserve(ctx, lifecycle.ReserveInput{ID: uuid.New(), UserID: 1001, ChannelID: 9001, OriginKind: "copy", OperationID: &op, ResultFileID: &result, PartNo: 1, LeaseToken: token, LeaseExpiresAt: time.Now().Add(time.Minute).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE operation_id=$1", op); err != nil {
		t.Fatal(err)
	}
	before := len(f.storage.Deletes())
	f.due(t)
	receiptEventually(t, func() bool {
		var n int
		err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM remote_write_receipts WHERE operation_id=$1 AND state='unresolved' AND job_id>0 AND wakeup_job_id>0", op).Scan(&n)
		return err == nil && n == 2
	})
	if len(f.storage.Deletes()) != before {
		t.Fatal("report-only no-message debt invoked destructive storage")
	}
	var id int64
	if err = f.db.Pool.QueryRow(ctx, "SELECT job_id FROM remote_write_receipts WHERE id=$1", op).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if item, e := f.runtime.GetForUser(ctx, id, 1001); e != nil || item.State != "discarded" {
		t.Fatalf("no-receipt owner report: %+v %v", item, e)
	}
	t.Run("expired_upload_reservation", func(t *testing.T) {
		session, e := f.uploads.Create(ctx, uploads.CreateInput{UserID: 1001, ParentID: &f.parent, Name: uuid.NewString(), ExpectedSize: 12, ModTime: time.Now().UTC()})
		if e != nil {
			t.Fatal(e)
		}
		uploadID := uuid.UUID(session.ID.Bytes)
		claim, e := f.uploads.ClaimPart(ctx, uploads.ClaimPartInput{UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 12})
		if e != nil {
			t.Fatal(e)
		}
		id := uuid.New()
		if _, e = f.policy.Reserve(ctx, lifecycle.ReserveInput{ID: id, UserID: 1001, ChannelID: 9001, OriginKind: "upload", UploadID: &uploadID, PartNo: 1, LeaseToken: claim.LeaseToken, LeaseExpiresAt: time.Now().Add(time.Minute).UTC()}); e != nil {
			t.Fatal(e)
		}
		if _, e = f.db.Pool.Exec(ctx, "UPDATE upload_parts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE upload_id=$1", uploadID); e != nil {
			t.Fatal(e)
		}
		if _, e = f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); e != nil {
			t.Fatal(e)
		}
		before := len(f.storage.Deletes())
		f.due(t)
		receiptEventually(t, func() bool {
			var first, current int64
			return f.db.Pool.QueryRow(ctx, "SELECT job_id,wakeup_job_id FROM remote_write_receipts WHERE id=$1 AND state='unresolved'", id).Scan(&first, &current) == nil && first > 0 && current > 0
		})
		if len(f.storage.Deletes()) != before {
			t.Fatal("expired unknown upload reservation sent destructive RPC")
		}
	})
}

func TestB2CleanupRecovery(t *testing.T) {
	for _, name := range []string{"actual_owner_ids", "caller_rollback_after_insert", "insertion_failure", "id_assignment_failure", "replacement_wakeup", "report_only"} {
		t.Run(name, func(t *testing.T) {
			f := newReceiptRuntimeFixture(t)
			ctx := context.Background()
			if name == "report_only" {
				f.start(t)
				f.noReceipt(t)
				return
			}
			f.copyCrash(t)
			var id uuid.UUID
			if err := f.db.Pool.QueryRow(ctx, "SELECT id FROM remote_write_receipts WHERE row_kind='message_attempt' LIMIT 1").Scan(&id); err != nil {
				t.Fatal(err)
			}
			if name == "insertion_failure" || name == "id_assignment_failure" {
				check := "ALTER TABLE river_job ADD CONSTRAINT b2_reject_receipt CHECK(kind<>'" + ReceiptCleanupKind + "')"
				if name == "id_assignment_failure" {
					check = "ALTER TABLE remote_write_receipts ADD CONSTRAINT b2_reject_id CHECK(job_id IS NULL)"
				}
				if _, err := f.db.Pool.Exec(ctx, check); err != nil {
					t.Fatal(err)
				}
				if err := f.policy.DiscoverRecoveryCandidate(ctx, id, time.Now().UTC()); err == nil {
					t.Fatal("forced transactional failure succeeded")
				}
				var jobs, assigned int
				if err := f.db.Pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM river_job WHERE kind=$1),(SELECT count(*) FROM remote_write_receipts WHERE job_id IS NOT NULL)", ReceiptCleanupKind).Scan(&jobs, &assigned); err != nil {
					t.Fatal(err)
				}
				if jobs+assigned != 0 {
					t.Fatal("partial enqueue/identity assignment committed")
				}
				return
			}
			if name == "caller_rollback_after_insert" {
				tx, err := f.db.Pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				jobID, err := f.runtime.EnqueueReceiptCleanupTx(ctx, tx, lifecycle.ReceiptCleanupInput{UserID: 1001, ReceiptIDs: []uuid.UUID{id}})
				if err != nil || jobID <= 0 {
					t.Fatalf("actual InsertTx: %d %v", jobID, err)
				}
				var owner int64
				if err = tx.QueryRow(ctx, "SELECT (metadata->>'user_id')::bigint FROM river_job WHERE id=$1", jobID).Scan(&owner); err != nil || owner != 1001 {
					t.Fatalf("insert-time metadata: %v", err)
				}
				if err = tx.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err = f.runtime.client.JobGet(ctx, jobID); !errors.Is(err, river.ErrNotFound) {
					t.Fatalf("caller rollback leaked River row: %v", err)
				}
				return
			}
			if err := f.policy.DiscoverRecoveryCandidate(ctx, id, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			var first, current int64
			if err := f.db.Pool.QueryRow(ctx, "SELECT job_id,wakeup_job_id FROM remote_write_receipts WHERE id=$1", id).Scan(&first, &current); err != nil {
				t.Fatal(err)
			}
			if first <= 0 || first != current {
				t.Fatal("initial IDs were not actual identical River IDs")
			}
			if name == "replacement_wakeup" {
				if _, err := f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE id=$1", current); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
					t.Fatal(err)
				}
				if err := f.policy.DiscoverRecoveryCandidate(ctx, id, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				var retained, replacement int64
				if err := f.db.Pool.QueryRow(ctx, "SELECT job_id,wakeup_job_id FROM remote_write_receipts WHERE id=$1", id).Scan(&retained, &replacement); err != nil {
					t.Fatal(err)
				}
				if retained != first || replacement <= first {
					t.Fatal("replacement lost first actual ID")
				}
			}
			f.start(t)
			f.assertRecovered(t)
		})
	}
}

func TestB2CleanupRetryBudget(t *testing.T) {
	t.Run("delete_ack_loss", testReceiptACKLoss)
	f := newReceiptRuntimeFixture(t)
	ctx := context.Background()
	f.copyCrash(t)
	failure := errors.New("synthetic remote failure")
	f.storage.SetDeleteFaults(fixture.DeleteFaults{Error: failure})
	var id uuid.UUID
	if err := f.db.Pool.QueryRow(ctx, "SELECT id FROM remote_write_receipts WHERE row_kind='message_attempt' LIMIT 1").Scan(&id); err != nil {
		t.Fatal(err)
	}
	if err := f.policy.DiscoverRecoveryCandidate(ctx, id, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Immediate reservation plus three recovery reservations, each on real rows.
	if err := f.policy.Recover(ctx, 1001, []uuid.UUID{id}); !errors.Is(err, failure) {
		t.Fatalf("immediate attempt: %v", err)
	}
	for attempt := 2; attempt <= 4; attempt++ {
		if _, err := f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		for range 2 {
			tx, err := f.db.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.runtime.EnqueueReceiptCleanupTx(ctx, tx, lifecycle.ReceiptCleanupInput{UserID: 1001, ReceiptIDs: []uuid.UUID{id}})
			if err == nil {
				err = tx.Commit(ctx)
			}
			_ = tx.Rollback(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
		receiptEventually(t, func() bool {
			var count int
			err := f.db.Pool.QueryRow(ctx, "SELECT attempts FROM remote_write_receipts WHERE id=$1", id).Scan(&count)
			return err == nil && count >= attempt && len(f.storage.Deletes()) >= count
		})
		stop, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := f.runtime.Stop(stop); err != nil {
			t.Fatal(err)
		}
		cancel()
		f.started = false
		if _, err := f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE kind=$1", ReceiptCleanupKind); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	f.due(t)
	receiptEventually(t, func() bool {
		var state string
		err := f.db.Pool.QueryRow(ctx, "SELECT state FROM remote_write_receipts WHERE id=$1", id).Scan(&state)
		return err == nil && state == "exhausted"
	})
	if err := f.policy.Recover(ctx, 1001, []uuid.UUID{id}); !errors.Is(err, lifecycle.ErrAttemptsExhausted) {
		t.Fatalf("fifth reservation not refused: %v", err)
	}
	if len(f.storage.Deletes()) != 4 {
		t.Fatalf("destructive calls=%d, want lifetime four", len(f.storage.Deletes()))
	}
	f.preserved(t)
}

func TestB2LedgerOnlyDiscovery(t *testing.T) {
	for _, name := range []string{
		"observed_copy_restart", "same_runtime_new_debt_without_notification", "reserved_no_receipt", "abandoned_operation", "live_lease_winner", "full_retained_page", "enqueue_rollback", "terminal_report_retention",
		"reset_periodic_jobs_observed_debt", "reset_interrupted_before_catalog_rebuild", "reset_interrupted_after_catalog_rebuild",
		"startup_paused_definition", "startup_wrong_definition", "startup_existing_definition_preserves_due", "startup_reconciliation_failure", "mandatory_definition_mutation_refused",
	} {
		t.Run(name, func(t *testing.T) {
			f := newReceiptRuntimeFixture(t)
			ctx := context.Background()
			var counts *receiptSweepQueries
			switch name {
			case "reserved_no_receipt":
				f.start(t)
				f.noReceipt(t)
				return
			case "abandoned_operation":
				f.start(t)
				testEmptyCopyOperationDebt(t, f)
				return
			case "same_runtime_new_debt_without_notification":
				f.start(t)
				f.due(t)
				receiptEventually(t, func() bool {
					var completed int
					return f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM river_job WHERE kind=$1 AND state='completed'", ReceiptDiscoverySweepKind).Scan(&completed) == nil && completed == 1
				})
			case "mandatory_definition_mutation_refused":
				testReceiptPeriodicMutationMatrix(t, f)
				return
			case "startup_reconciliation_failure":
				if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE river_periodic_job ADD CONSTRAINT b2_no_required CHECK(id<>'receipt_cleanup_sweep')"); err != nil {
					t.Fatal(err)
				}
				if err := f.runtime.Start(ctx); err == nil || f.runtime.started {
					t.Fatal("Start promised recovery after failed upsert")
				}
				return
			case "startup_wrong_definition":
				for _, field := range []string{"kind", "args", "queue", "priority", "attempts", "tags", "cron", "timezone"} {
					t.Run(field, func(t *testing.T) {
						g := newReceiptRuntimeFixture(t)
						spec := requiredReceiptPeriodicSpec()
						switch field {
						case "kind":
							spec.Kind = UploadCleanupSweepKind
						case "args":
							spec.Args = []byte(`{"wrong":true}`)
						case "queue":
							spec.Queue = UploadQueue
						case "priority":
							spec.Priority = 4
						case "attempts":
							spec.MaxAttempts = 1
						case "tags":
							spec.Tags = []string{"wrong"}
						case "cron":
							spec.Schedule.CronExpression = "@every 24h"
						case "timezone":
							spec.Schedule.CronTimezone = "Europe/London"
						}
						if _, err := g.runtime.client.PeriodicJobInsert(ctx, spec); err != nil {
							t.Fatal(err)
						}
						g.start(t)
						g.copyCrash(t)
						g.assertRecovered(t)
						t.Run("no_receipt", func(t *testing.T) { g.noReceipt(t) })
					})
				}
				return
			case "startup_paused_definition", "startup_existing_definition_preserves_due":
				spec := requiredReceiptPeriodicSpec()
				spec.Paused = name == "startup_paused_definition"
				spec.Schedule.NextRunAt = time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
				row, err := f.runtime.client.PeriodicJobInsert(ctx, spec)
				if err != nil {
					t.Fatal(err)
				}
				f.start(t)
				actual, err := f.runtime.client.PeriodicJobGet(ctx, receiptCleanupPeriodicID)
				if err != nil {
					t.Fatal(err)
				}
				if err = checkRequiredReceiptPeriodic(actual); err != nil {
					t.Fatal(err)
				}
				if !actual.NextRunAt.Equal(row.NextRunAt) {
					t.Fatal("canonical startup postponed existing due time")
				}
			case "reset_periodic_jobs_observed_debt", "reset_interrupted_before_catalog_rebuild", "reset_interrupted_after_catalog_rebuild":
				f.start(t)
				custom := periodicInputForTest("custom-reset-control", EventCleanupKind)
				if _, err := f.runtime.CreatePeriodicJob(ctx, custom); err != nil {
					t.Fatal(err)
				}
				if name == "reset_periodic_jobs_observed_debt" {
					if _, err := f.runtime.ResetPeriodicJobs(ctx); err != nil {
						t.Fatal(err)
					}
					if _, err := f.runtime.client.PeriodicJobGet(ctx, custom.ID); !errors.Is(err, river.ErrNotFound) {
						t.Fatalf("full reset kept custom row: %v", err)
					}
				} else {
					interruptReceiptReset(t, f, name == "reset_interrupted_after_catalog_rebuild")
				}
			case "full_retained_page":
				seedRetainedReceiptPage(t, f)
				counts = traceReceiptSweepQueries(t, f)
				f.start(t)
			case "live_lease_winner":
				t.Run("live_lease_and_late_receipt", func(t *testing.T) {
					g := newReceiptRuntimeFixture(t)
					g.start(t)
					testReceiptLiveLeaseAndLateReceipt(t, g)
				})
				f.start(t)
				cat := f.catalog
				ops, err := fileops.NewService(f.db.Pool, cat, channels.NewService(f.db.Pool, nil, channels.Config{PartLimit: 10000}), f.storage)
				if err != nil {
					t.Fatal(err)
				}
				if err = ops.BindLifecycle(f.policy); err != nil {
					t.Fatal(err)
				}
				name := "published-winner"
				winner, err := ops.Copy(ctx, fileops.CopyInput{UserID: 1001, FileID: f.source, ParentID: &f.parent, Name: &name})
				if err != nil || winner == nil {
					t.Fatalf("actual published winner: %v", err)
				}
				f.due(t)
				var message int64
				if err = f.db.Pool.QueryRow(ctx, "SELECT message_id FROM file_parts WHERE file_id=$1", winner.ID).Scan(&message); err != nil {
					t.Fatal(err)
				}
				got, ok := f.storage.Bytes(9001, message)
				if !ok || sha256.Sum256(got) != sha256.Sum256([]byte("source bytes")) {
					t.Fatal("published winner bytes changed")
				}
			case "enqueue_rollback":
				f.copyCrash(t)
				if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE river_job ADD CONSTRAINT b2_enqueue_fail CHECK(kind<>'"+ReceiptCleanupKind+"')"); err != nil {
					t.Fatal(err)
				}
				f.start(t)
				f.due(t)
				receiptEventually(t, func() bool {
					var failed int
					err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM river_job WHERE kind=$1 AND cardinality(errors)>0", ReceiptDiscoverySweepKind).Scan(&failed)
					return err == nil && failed > 0
				})
				var jobs, ids int
				if err := f.db.Pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM river_job WHERE kind=$1),(SELECT count(*) FROM remote_write_receipts WHERE job_id IS NOT NULL)", ReceiptCleanupKind).Scan(&jobs, &ids); err != nil {
					t.Fatal(err)
				}
				if jobs+ids != 0 {
					t.Fatal("failed sweep committed partial report/job")
				}
				if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE river_job DROP CONSTRAINT b2_enqueue_fail"); err != nil {
					t.Fatal(err)
				}
				f.assertRecovered(t)
				return
			}
			if !f.started {
				f.copyCrash(t)
				f.compose(t)
				f.start(t)
			} else {
				f.copyCrash(t)
			}
			f.assertRecovered(t)
			if name == "live_lease_winner" {
				var message int64
				if err := f.db.Pool.QueryRow(ctx, "SELECT p.message_id FROM file_parts p JOIN files f ON f.id=p.file_id WHERE f.name='published-winner'").Scan(&message); err != nil {
					t.Fatal(err)
				}
				got, ok := f.storage.Bytes(9001, message)
				if !ok || sha256.Sum256(got) != sha256.Sum256([]byte("source bytes")) {
					t.Fatal("recovery changed independently committed winner SHA-256")
				}
			}
			if name == "full_retained_page" {
				var pages, selected int
				receiptEventually(t, func() bool {
					return f.db.Pool.QueryRow(ctx, "SELECT (metadata->'output'->>'pages')::integer,(metadata->'output'->>'selected')::integer FROM river_job WHERE kind=$1 AND state='completed' ORDER BY id DESC LIMIT 1", ReceiptDiscoverySweepKind).Scan(&pages, &selected) == nil
				})
				if pages != 2 || selected != 1002 {
					t.Fatalf("unbounded retained-page discovery: pages=%d selected=%d", pages, selected)
				}
				var retained int
				if err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM remote_write_receipts WHERE channel_id=9001 AND message_id BETWEEN 1000 AND 1999 AND state='observed' AND attempts=0 AND job_id IS NULL").Scan(&retained); err != nil || retained != 1000 {
					t.Fatalf("retained full page changed: %d %v", retained, err)
				}
				if counts == nil {
					t.Fatal("missing real SQL query counters")
				}
				if counts.pages.Load() != 2 || counts.claims.Load() != 2 || len(f.storage.Deletes()) != 1 {
					t.Fatalf("retained scan query/RPC bounds: pages=%d claims=%d RPC=%d", counts.pages.Load(), counts.claims.Load(), len(f.storage.Deletes()))
				}
			}
			t.Run("no_receipt", func(t *testing.T) { f.noReceipt(t) })
		})
	}
}

func periodicInputForTest(id, kind string) PeriodicJobInput {
	return PeriodicJobInput{ID: id, Kind: kind, Args: rawArgs(EventCleanupArgs{Retention: "48h"}), Queue: CleanupQueue, Priority: 2, MaxAttempts: 3, Tags: []string{}, Schedule: PeriodicSchedule{CronExpression: "@every 12h", CronTimezone: "UTC"}}
}
func testReceiptPeriodicMutationMatrix(t *testing.T, f *receiptRuntimeFixture) {
	t.Helper()
	ctx := context.Background()
	if _, err := f.runtime.client.PeriodicJobUpsert(ctx, requiredReceiptPeriodicSpec()); err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"canonical", "wrong_kind", "alias"} {
		t.Run(variant, func(t *testing.T) {
			id := receiptCleanupPeriodicID
			spec := requiredReceiptPeriodicSpec()
			if variant == "wrong_kind" {
				spec.Kind = EventCleanupKind
			}
			if variant == "alias" {
				id = "reserved-kind-alias"
				spec.ID = id
			}
			if _, err := f.runtime.client.PeriodicJobUpsert(ctx, spec); err != nil {
				t.Fatal(err)
			}
			for _, operation := range []string{"create", "update", "replace", "delete", "pause", "resume"} {
				t.Run(operation, func(t *testing.T) {
					before := receiptPeriodicSnapshot(t, f.db.Pool)
					input := periodicInputForTest(id, EventCleanupKind)
					var err error
					switch operation {
					case "create":
						input.Kind = ReceiptDiscoverySweepKind
						_, err = f.runtime.CreatePeriodicJob(ctx, input)
					case "update":
						_, err = f.runtime.UpdatePeriodicJob(ctx, id, input)
					case "replace":
						input.Paused = true
						_, err = f.runtime.UpdatePeriodicJob(ctx, id, input)
					case "delete":
						err = f.runtime.DeletePeriodicJob(ctx, id)
					case "pause":
						_, err = f.runtime.PausePeriodicJob(ctx, id)
					case "resume":
						_, err = f.runtime.ResumePeriodicJob(ctx, id)
					}
					if !errors.Is(err, ErrSystemPeriodicJob) {
						t.Fatalf("%s accepted system mutation: %v", operation, err)
					}
					if after := receiptPeriodicSnapshot(t, f.db.Pool); before != after {
						t.Fatal("refused mutation changed persisted definition/due")
					}
				})
			}
		})
	}
	other := periodicInputForTest("ordinary-control", EventCleanupKind)
	if _, err := f.runtime.CreatePeriodicJob(ctx, other); err != nil {
		t.Fatal(err)
	}
	before := receiptPeriodicSnapshot(t, f.db.Pool)
	replacement := other
	replacement.Kind = ReceiptDiscoverySweepKind
	if _, err := f.runtime.UpdatePeriodicJob(ctx, other.ID, replacement); !errors.Is(err, ErrSystemPeriodicJob) {
		t.Fatalf("proposed reserved kind under ordinary ID: %v", err)
	}
	if receiptPeriodicSnapshot(t, f.db.Pool) != before {
		t.Fatal("reserved-kind replacement partially mutated ordinary row")
	}
	if _, err := f.runtime.PausePeriodicJob(ctx, other.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runtime.ResumePeriodicJob(ctx, other.ID); err != nil {
		t.Fatal(err)
	}
	other.Priority = 3
	if _, err := f.runtime.UpdatePeriodicJob(ctx, other.ID, other); err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.DeletePeriodicJob(ctx, other.ID); err != nil {
		t.Fatal(err)
	}
	alias := periodicInputForTest("new-reserved-kind-alias", ReceiptDiscoverySweepKind)
	if _, err := f.runtime.CreatePeriodicJob(ctx, alias); !errors.Is(err, ErrSystemPeriodicJob) {
		t.Fatalf("reserved-kind create: %v", err)
	}
}
func receiptPeriodicSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var value string
	if err := pool.QueryRow(context.Background(), "SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY id),'[]')::text FROM river_periodic_job p").Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

// Real trigger/advisory barriers stop the actual reset transaction at either
// boundary. No replacement reset loop or production test hook is involved.
func interruptReceiptReset(t *testing.T, f *receiptRuntimeFixture, after bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := int64(9131301)
	barrierSQL := `CREATE FUNCTION b2_reset_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.id = 'custom-reset-control' THEN PERFORM pg_advisory_xact_lock(9131301); END IF; RETURN NULL; END $$; CREATE TRIGGER b2_reset_barrier AFTER DELETE ON river_periodic_job FOR EACH ROW EXECUTE FUNCTION b2_reset_barrier()`
	if after {
		barrierSQL = `CREATE FUNCTION b2_reset_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = 'receipt_cleanup_sweep' THEN PERFORM pg_advisory_xact_lock(9131301); END IF; RETURN NULL; END $$; CREATE TRIGGER b2_reset_barrier AFTER UPDATE ON river_periodic_job FOR EACH ROW EXECUTE FUNCTION b2_reset_barrier()`
	}
	if _, err := f.db.Pool.Exec(ctx, barrierSQL); err != nil {
		t.Fatal(err)
	}
	blocker, err := f.db.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Release()
	if _, err = blocker.Exec(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		t.Fatal(err)
	}
	before := receiptPeriodicSnapshot(t, f.db.Pool)
	resetCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := f.runtime.ResetPeriodicJobs(resetCtx); done <- err }()
	var waiter uint32
	receiptEventually(t, func() bool {
		return f.db.Pool.QueryRow(ctx, "SELECT pid FROM pg_stat_activity WHERE $1::integer=ANY(pg_blocking_pids(pid))", int32(blocker.Conn().PgConn().PID())).Scan(&waiter) == nil
	})
	if err = fixture.WaitForBlocking(ctx, f.db.Pool, waiter, blocker.Conn().PgConn().PID()); err != nil {
		t.Fatal(err)
	}
	stop()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("interrupted reset succeeded")
		}
	case <-ctx.Done():
		t.Fatal("reset cancellation did not release transaction")
	}
	if got := receiptPeriodicSnapshot(t, f.db.Pool); got != before {
		t.Fatal("interrupted reset did not atomically roll back catalog")
	}
	if _, err = blocker.Exec(ctx, "SELECT pg_advisory_unlock($1)", key); err != nil {
		t.Fatal(err)
	}
	if _, err = f.db.Pool.Exec(ctx, "DROP TRIGGER b2_reset_barrier ON river_periodic_job; DROP FUNCTION b2_reset_barrier()"); err != nil {
		t.Fatal(err)
	}
}

var _ pgx.QueryTracer = (*receiptObserveCrash)(nil)

func seedRetainedReceiptPage(t *testing.T, f *receiptRuntimeFixture) {
	t.Helper()
	ctx := context.Background()
	operation, result, token, foreign := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	// The fixed default belongs only to this guarded fixture and the next actual Copy.
	if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE remote_write_receipts ALTER COLUMN created_at SET DEFAULT '2026-01-01'::timestamptz"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(ctx, `INSERT INTO remote_write_receipts(id,row_kind,user_id,origin_kind,operation_id,result_file_id,destination_is_root,lease_token,lease_expires_at)
 VALUES($1,'copy_operation',1001,'copy',$1,$2,true,$3,clock_timestamp()+interval '1 hour')`, operation, result, token); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(ctx, `INSERT INTO remote_write_receipts(id,row_kind,user_id,channel_id,message_id,origin_kind,part_no,operation_id,result_file_id,lease_token,lease_expires_at,state)
 SELECT ('00000000-0000-0000-0000-'||lpad(to_hex(i+1),12,'0'))::uuid,'message_attempt',1001,9001,1000+i,'copy',i+1,$1,$2,$3,clock_timestamp()-interval '1 hour','observed' FROM generate_series(0,999) i`, operation, result, token); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,status,size,mod_time) VALUES($1,1002,'independent foreign references','file','active',1000,clock_timestamp())", foreign); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size) SELECT $1,i+1,9001,1000+i,1,1 FROM generate_series(0,999)i", foreign); err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		if err := f.storage.Seed(fixture.Message{UserID: 1002, ChannelID: 9001, MessageID: int64(1000 + i), Bytes: []byte{byte(i)}, Preserve: true}); err != nil {
			t.Fatal(err)
		}
	}
}

func testReceiptACKLoss(t *testing.T) {
	f := newReceiptRuntimeFixture(t)
	ctx := context.Background()
	f.copyCrash(t)
	if _, err := f.db.Pool.Exec(ctx, "ALTER TABLE remote_write_receipts ADD CONSTRAINT b2_ack_loss CHECK(state<>'deleted')"); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	f.due(t)
	for attempt := 1; attempt <= 4; attempt++ {
		receiptEventually(t, func() bool {
			var count int
			return f.db.Pool.QueryRow(ctx, "SELECT attempts FROM remote_write_receipts WHERE row_kind='message_attempt' LIMIT 1").Scan(&count) == nil && count >= attempt && len(f.storage.Deletes()) >= count
		})
		stop, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := f.runtime.Stop(stop); err != nil {
			t.Fatal(err)
		}
		cancel()
		f.started = false
		var first, message int64
		if err := f.db.Pool.QueryRow(ctx, "SELECT job_id,message_id FROM remote_write_receipts WHERE row_kind='message_attempt' LIMIT 1").Scan(&first, &message); err != nil {
			t.Fatal(err)
		}
		if first <= 0 {
			t.Fatal("ACK-loss debt has no actual first owner report")
		}
		if _, ok := f.storage.Bytes(9001, message); ok {
			t.Fatal("positive remote deletion did not remove exact destination bytes")
		}
		if _, err := f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE kind=$1", ReceiptCleanupKind); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE row_kind='message_attempt'"); err != nil {
			t.Fatal(err)
		}
		f.compose(t)
		f.start(t)
		f.due(t)
	}
	receiptEventually(t, func() bool {
		var state string
		return f.db.Pool.QueryRow(ctx, "SELECT state FROM remote_write_receipts WHERE row_kind='message_attempt' LIMIT 1").Scan(&state) == nil && state == "exhausted"
	})
	if len(f.storage.Deletes()) != 4 {
		t.Fatalf("ACK-loss/restart/retention forwarded %d destructive invocations", len(f.storage.Deletes()))
	}
	var first int64
	if err := f.db.Pool.QueryRow(ctx, "SELECT job_id FROM remote_write_receipts WHERE row_kind='message_attempt' LIMIT 1").Scan(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runtime.RetryForUser(ctx, first, 1001); !errors.Is(err, ErrReceiptRetryUnavailable) {
		t.Fatalf("ACK-loss exhaustion reopened: %v", err)
	}
	f.preserved(t)
}

func testReceiptLiveLeaseAndLateReceipt(t *testing.T, f *receiptRuntimeFixture) {
	t.Helper()
	ctx := context.Background()
	operation, result, token, id := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	tx, err := f.db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = f.policy.ReserveCopyTx(ctx, tx, lifecycle.ReserveCopyInput{UserID: 1001, OperationID: operation, RootResultFileID: result, LeaseToken: token, DestinationParentID: &f.parent, LeaseExpiresAt: time.Now().Add(time.Hour).UTC()})
	if err == nil {
		err = tx.Commit(ctx)
	}
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.policy.Reserve(ctx, lifecycle.ReserveInput{ID: id, UserID: 1001, ChannelID: 9001, OriginKind: "copy", OperationID: &operation, ResultFileID: &result, PartNo: 1, LeaseToken: token, LeaseExpiresAt: time.Now().Add(time.Hour).UTC()}); err != nil {
		t.Fatal(err)
	}
	part, err := f.storage.CopyPart(ctx, 1001, 9001, 10, 9001)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.policy.Observe(ctx, lifecycle.ObserveInput{ReceiptID: id, UserID: 1001, ChannelID: 9001, MessageID: part.MessageID, LeaseToken: token, OperationID: &operation, ResultFileID: &result, PartNo: 1}); err != nil {
		t.Fatal(err)
	}
	// A stale candidate hint cannot override the actual live operation lease.
	if _, err = f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	before := len(f.storage.Deletes())
	f.due(t)
	receiptEventually(t, func() bool {
		var completed int
		return f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM river_job WHERE kind=$1 AND state='completed'", ReceiptDiscoverySweepKind).Scan(&completed) == nil && completed > 0
	})
	var state string
	var attempts int
	if err = f.db.Pool.QueryRow(ctx, "SELECT state,attempts FROM remote_write_receipts WHERE id=$1", id).Scan(&state, &attempts); err != nil || state != "observed" || attempts != 0 || len(f.storage.Deletes()) != before {
		t.Fatalf("live operation was not retained: %s/%d %v", state, attempts, err)
	}
	lateID := uuid.New()
	if _, err = f.policy.Reserve(ctx, lifecycle.ReserveInput{ID: lateID, UserID: 1001, ChannelID: 9001, OriginKind: "copy", OperationID: &operation, ResultFileID: &result, PartNo: 2, LeaseToken: token, LeaseExpiresAt: time.Now().Add(time.Hour).UTC()}); err != nil {
		t.Fatal(err)
	}
	late, err := f.storage.CopyPart(ctx, 1001, 9001, 10, 9001)
	if err != nil {
		t.Fatal(err)
	}
	// Actual parent loss permanently fences the operation, including late receipts.
	if _, err = f.catalog.Trash(ctx, 1001, f.parent); err != nil {
		t.Fatal(err)
	}
	if _, err = f.catalog.Restore(ctx, 1001, f.parent); err != nil {
		t.Fatal(err)
	}
	var first int64
	if err = f.db.Pool.QueryRow(ctx, "SELECT job_id FROM remote_write_receipts WHERE id=$1", lateID).Scan(&first); err != nil || first <= 0 {
		t.Fatalf("late unknown origin missing first real report: %v", err)
	}
	if _, err = f.policy.Observe(ctx, lifecycle.ObserveInput{ReceiptID: lateID, UserID: 1001, ChannelID: 9001, MessageID: late.MessageID, LeaseToken: token, OperationID: &operation, ResultFileID: &result, PartNo: 2}); err != nil {
		t.Fatal(err)
	}
	f.due(t)
	receiptEventually(t, func() bool {
		var done bool
		return f.db.Pool.QueryRow(ctx, "SELECT state='deleted' FROM remote_write_receipts WHERE id=$1", id).Scan(&done) == nil && done
	})
	if _, ok := f.storage.Bytes(9001, part.MessageID); ok {
		t.Fatal("terminal exact destination was not cleaned")
	}
	receiptEventually(t, func() bool {
		var done bool
		return f.db.Pool.QueryRow(ctx, "SELECT state='deleted' AND job_id=$2 AND wakeup_job_id>0 AND attempts=1 FROM remote_write_receipts WHERE id=$1", lateID, first).Scan(&done) == nil && done
	})
	if _, ok := f.storage.Bytes(9001, late.MessageID); ok {
		t.Fatal("late exact receipt survived terminal cleanup")
	}
	f.preserved(t)
}

func testEmptyCopyOperationDebt(t *testing.T, f *receiptRuntimeFixture) {
	t.Helper()
	ctx := context.Background()
	empty := uuid.New()
	if _, err := f.db.Pool.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,status,size,mod_time) VALUES($1,1001,'empty-copy-source','folder','active',0,clock_timestamp())", empty); err != nil {
		t.Fatal(err)
	}
	gate := fixture.NewBarrier(t, fixture.ErrProcessLost)
	gate.Release()
	tracer := &receiptObserveCrash{observed: make(map[uint32]bool), hooks: fixture.Hooks{CopyOperationReserved: gate}, queryName: "ReserveCopyOperation"}
	tracer.armed.Store(true)
	cfg, err := pgxpool.ParseConfig(f.db.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	policy := lifecycle.NewService(pool, f.storage)
	if err = policy.BindReceiptEnqueuer(f.runtime); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewService(pool, nil)
	if err = cat.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	ops, err := fileops.NewService(pool, cat, channels.NewService(pool, nil, channels.Config{PartLimit: 10000}), f.storage)
	if err != nil {
		t.Fatal(err)
	}
	if err = ops.BindLifecycle(policy); err != nil {
		t.Fatal(err)
	}
	var lost any
	func() {
		defer func() { lost = recover() }()
		name := "empty-copy-loser"
		_, err = ops.Copy(ctx, fileops.CopyInput{UserID: 1001, FileID: empty, ParentID: &f.parent, Name: &name})
	}()
	if lost != fixture.ErrProcessLost {
		t.Fatalf("empty actual Copy did not lose process after reservation COMMIT: %v %v", lost, err)
	}
	var messages, jobs, results int
	if err = f.db.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM remote_write_receipts WHERE row_kind='message_attempt'),
 (SELECT count(*) FROM river_job WHERE kind=$1),
 (SELECT count(*) FROM files f JOIN remote_write_receipts r ON r.result_file_id=f.id)`, ReceiptCleanupKind).Scan(&messages, &jobs, &results); err != nil || messages+jobs+results != 0 {
		t.Fatalf("empty-copy debt has side effects: %d/%d/%d %v", messages, jobs, results, err)
	}
	if _, err = f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE row_kind='copy_operation'"); err != nil {
		t.Fatal(err)
	}
	before := len(f.storage.Deletes())
	f.due(t)
	var first, current int64
	receiptEventually(t, func() bool {
		return f.db.Pool.QueryRow(ctx, "SELECT job_id,wakeup_job_id FROM remote_write_receipts WHERE row_kind='copy_operation' AND state='unresolved'").Scan(&first, &current) == nil && first > 0 && current > 0
	})
	if len(f.storage.Deletes()) != before {
		t.Fatal("empty operation report made destructive RPC")
	}
	if _, err = f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE kind=$1", ReceiptCleanupKind); err != nil {
		t.Fatal(err)
	}
	if report, e := f.runtime.GetForUser(ctx, first, 1001); e != nil || report.State != "discarded" {
		t.Fatalf("retained empty-copy first owner report: %+v %v", report, e)
	}
	if _, err = f.runtime.GetForUser(ctx, first, 1002); !errors.Is(err, river.ErrNotFound) {
		t.Fatalf("empty-copy foreign detail: %v", err)
	}
	f.preserved(t)
}

type receiptSweepQueries struct {
	pages  atomic.Int64
	claims atomic.Int64
}

func (c *receiptSweepQueries) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	switch {
	case strings.Contains(data.SQL, "-- name: ListLifecycleRecoveryCandidates"):
		c.pages.Add(1)
	case strings.Contains(data.SQL, "-- name: ClaimReceiptDeletion"):
		c.claims.Add(1)
	}
	return ctx
}

func (*receiptSweepQueries) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func traceReceiptSweepQueries(t *testing.T, f *receiptRuntimeFixture) *receiptSweepQueries {
	t.Helper()
	counts := &receiptSweepQueries{}
	config := f.db.Pool.Config()
	config.ConnConfig.Tracer = counts
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if f.started {
			if err := f.runtime.Stop(ctx); err != nil {
				t.Error(err)
			}
			f.started = false
		}
		pool.Close()
	})
	f.db.Pool = pool
	f.compose(t)
	return counts
}

func seedReceiptPeriodicDefinitions(t *testing.T, f *receiptRuntimeFixture, count int, paused bool) {
	t.Helper()
	if _, err := f.db.Pool.Exec(context.Background(), `
INSERT INTO river_periodic_job
    (id,kind,args,queue,priority,max_attempts,tags,cron_expression,cron_timezone,next_run_at,paused_at)
SELECT 'a-capacity-'||lpad(i::text,5,'0'),$2,'{"retention":"48h"}'::jsonb,$3,2,3,'{}',
       '@every 12h','UTC',clock_timestamp()+interval '12 hours',
       CASE WHEN $4::boolean THEN clock_timestamp() ELSE NULL END
FROM generate_series(1,$1::integer) i`, count, EventCleanupKind, CleanupQueue, paused); err != nil {
		t.Fatal(err)
	}
}

func TestB2PeriodicPriorityAdmission(t *testing.T) {
	f := newReceiptRuntimeFixture(t)
	ctx := context.Background()
	input := periodicInputForTest("priority-control", EventCleanupKind)
	input.Priority = 5
	before := receiptPeriodicSnapshot(t, f.db.Pool)
	if _, err := f.runtime.CreatePeriodicJob(ctx, input); !errors.Is(err, ErrInvalidPeriodicJobPriority) {
		t.Fatalf("priority 5 Create: %v", err)
	}
	if receiptPeriodicSnapshot(t, f.db.Pool) != before {
		t.Fatal("rejected priority Create changed definitions")
	}
	for _, priority := range []int{-9, 0, 1, 4} {
		t.Run(fmt.Sprint(priority), func(t *testing.T) {
			input.ID = fmt.Sprintf("priority-%d", priority)
			input.Priority = priority
			want := priority
			if priority <= 0 {
				want = river.PriorityDefault
			}
			created, err := f.runtime.CreatePeriodicJob(ctx, input)
			if err != nil || created.Priority != want {
				t.Fatalf("Create priority %d: %+v %v", priority, created, err)
			}
			input.Priority = 5
			input.Paused = true
			before := receiptPeriodicSnapshot(t, f.db.Pool)
			if _, err = f.runtime.UpdatePeriodicJob(ctx, input.ID, input); !errors.Is(err, ErrInvalidPeriodicJobPriority) {
				t.Fatalf("priority 5 Update: %v", err)
			}
			if receiptPeriodicSnapshot(t, f.db.Pool) != before {
				t.Fatal("rejected priority Update changed definition or pause state")
			}
			input.Priority = priority
			updated, err := f.runtime.UpdatePeriodicJob(ctx, input.ID, input)
			if err != nil || updated.Priority != want || !updated.Paused {
				t.Fatalf("Update priority %d: %+v %v", priority, updated, err)
			}
		})
	}
}

func TestB2PeriodicStartupPoison(t *testing.T) {
	for _, paused := range []bool{false, true} {
		for _, priority := range []int{0, 5} {
			t.Run(fmt.Sprintf("paused_%t_priority_%d", paused, priority), func(t *testing.T) {
				f := newReceiptRuntimeFixture(t)
				ctx := context.Background()
				input := periodicInputForTest("a-poison", EventCleanupKind)
				input.Paused = paused
				if _, err := f.runtime.CreatePeriodicJob(ctx, input); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.Pool.Exec(ctx, "UPDATE river_periodic_job SET priority=$1,next_run_at=clock_timestamp()-interval '1 hour' WHERE id=$2", priority, input.ID); err != nil {
					t.Fatal(err)
				}
				f.copyCrash(t)
				before := receiptPeriodicSnapshot(t, f.db.Pool)
				if err := f.runtime.Start(ctx); !errors.Is(err, ErrInvalidPeriodicJobPriority) {
					t.Fatalf("retained poison startup: %v", err)
				}
				if f.runtime.started || f.runtime.cancel != nil {
					t.Fatal("poisoned definitions started workers")
				}
				if receiptPeriodicSnapshot(t, f.db.Pool) != before {
					t.Fatal("failed startup repaired or deleted retained definitions")
				}
				var jobs int
				if err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM river_job").Scan(&jobs); err != nil || jobs != 0 {
					t.Fatalf("failed startup dispatched work: %d %v", jobs, err)
				}
				candidate := periodicInputForTest("refused-with-retained-poison", EventCleanupKind)
				if _, err := f.runtime.CreatePeriodicJob(ctx, candidate); !errors.Is(err, ErrInvalidPeriodicJobPriority) {
					t.Fatalf("Create committed alongside retained poison: %v", err)
				}
				if receiptPeriodicSnapshot(t, f.db.Pool) != before {
					t.Fatal("failed Create changed retained poison")
				}
				input.Priority = 4
				repaired, err := f.runtime.UpdatePeriodicJob(ctx, input.ID, input)
				if err != nil || repaired.Priority != 4 || repaired.Paused != paused {
					t.Fatalf("explicit retained-definition repair: %+v %v", repaired, err)
				}
				f.start(t)
				f.assertRecovered(t)
				var retained int
				if err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM river_periodic_job WHERE id=$1 AND priority=4 AND (paused_at IS NOT NULL)=$2", input.ID, paused).Scan(&retained); err != nil || retained != 1 {
					t.Fatalf("startup discarded repaired definition: %d %v", retained, err)
				}
			})
		}
	}
}

func TestB2PeriodicStartupCapacity(t *testing.T) {
	for _, name := range []string{"active_prefix_overflow", "paused_definitions", "missing_defaults"} {
		t.Run(name, func(t *testing.T) {
			f := newReceiptRuntimeFixture(t)
			ctx := context.Background()
			seedReceiptPeriodicDefinitions(t, f, 10000, name == "paused_definitions")
			if name == "active_prefix_overflow" {
				if _, err := f.runtime.client.PeriodicJobInsert(ctx, requiredReceiptPeriodicSpec()); err != nil {
					t.Fatal(err)
				}
				rows, err := f.runtime.client.PeriodicJobList(ctx, &riverpro.PeriodicJobListOpts{Max: 10000})
				if err != nil || len(rows) != 10000 {
					t.Fatalf("driver prefix: %d %v", len(rows), err)
				}
				for _, row := range rows {
					if row.ID == receiptCleanupPeriodicID {
						t.Fatal("fixture did not put required definition beyond driver prefix")
					}
				}
			}
			before := receiptPeriodicSnapshot(t, f.db.Pool)
			if err := f.runtime.Start(ctx); !errors.Is(err, ErrPeriodicJobCapacity) {
				t.Fatalf("over-capacity startup: %v", err)
			}
			if f.runtime.started || f.runtime.cancel != nil || receiptPeriodicSnapshot(t, f.db.Pool) != before {
				t.Fatal("capacity refusal started workers or changed retained definitions")
			}
			var jobs int
			if err := f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM river_job").Scan(&jobs); err != nil || jobs != 0 {
				t.Fatalf("capacity refusal dispatched work: %d %v", jobs, err)
			}
		})
	}
}

func TestB2PeriodicSupportedLimit(t *testing.T) {
	f := newReceiptRuntimeFixture(t)
	ctx := context.Background()
	if _, err := f.runtime.ResetPeriodicJobs(ctx); err != nil {
		t.Fatal(err)
	}
	seedReceiptPeriodicDefinitions(t, f, 9999-len(f.runtime.PeriodicJobCatalog()), false)
	input := periodicInputForTest("a-last-supported", EventCleanupKind)
	if _, err := f.runtime.CreatePeriodicJob(ctx, input); err != nil {
		t.Fatal(err)
	}
	input.ID = "a-over-capacity"
	before := receiptPeriodicSnapshot(t, f.db.Pool)
	if _, err := f.runtime.CreatePeriodicJob(ctx, input); !errors.Is(err, ErrPeriodicJobCapacity) {
		t.Fatalf("10001st definition: %v", err)
	}
	if receiptPeriodicSnapshot(t, f.db.Pool) != before {
		t.Fatal("capacity refusal changed persisted definitions")
	}
	f.start(t)
	rows, err := f.runtime.client.PeriodicJobList(ctx, &riverpro.PeriodicJobListOpts{Max: 10000})
	if err != nil || len(rows) != 10000 {
		t.Fatalf("supported definitions: %d %v", len(rows), err)
	}
	required := false
	for _, row := range rows {
		if row.ID == receiptCleanupPeriodicID {
			required = true
			if err := checkRequiredReceiptPeriodic(row); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !required {
		t.Fatal("mandatory definition hidden at supported limit")
	}
	f.copyCrash(t)
	f.assertRecovered(t)
}

func TestB2PeriodicConcurrentCapacity(t *testing.T) {
	f := newReceiptRuntimeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := f.runtime.ResetPeriodicJobs(ctx); err != nil {
		t.Fatal(err)
	}
	seedReceiptPeriodicDefinitions(t, f, 9999-len(f.runtime.PeriodicJobCatalog()), true)
	other, err := NewRuntime(f.db.Pool, f.storage, f.policy)
	if err != nil {
		t.Fatal(err)
	}
	barrier, err := f.db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Rollback(context.Background())
	if _, err = barrier.Exec(ctx, "LOCK TABLE teldrive.river_periodic_job IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	outcomes := make(chan error, 2)
	for i, runtime := range []*Runtime{f.runtime, other} {
		go func() {
			_, err := runtime.CreatePeriodicJob(ctx, periodicInputForTest(fmt.Sprintf("concurrent-%d", i), EventCleanupKind))
			outcomes <- err
		}()
	}
	receiptEventually(t, func() bool {
		var waiting int
		return f.db.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE relation='teldrive.river_periodic_job'::regclass AND mode='ShareRowExclusiveLock' AND NOT granted").Scan(&waiting) == nil && waiting == 2
	})
	if err = barrier.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	accepted, refused := 0, 0
	for range 2 {
		select {
		case err := <-outcomes:
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrPeriodicJobCapacity):
				refused++
			default:
				t.Fatalf("concurrent Create: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if accepted != 1 || refused != 1 {
		t.Fatalf("capacity edge accepted=%d refused=%d", accepted, refused)
	}
	var total, custom int
	if err := f.db.Pool.QueryRow(ctx, "SELECT count(*),count(*) FILTER(WHERE id LIKE 'concurrent-%') FROM river_periodic_job").Scan(&total, &custom); err != nil || total != 10000 || custom != 1 {
		t.Fatalf("concurrent committed definitions: total=%d custom=%d %v", total, custom, err)
	}
	f.start(t)
	f.copyCrash(t)
	f.assertRecovered(t)
}

func TestB2PeriodicStartupPreservesCustomDefaults(t *testing.T) {
	f := newReceiptRuntimeFixture(t)
	ctx := context.Background()
	if _, err := f.runtime.ResetPeriodicJobs(ctx); err != nil {
		t.Fatal(err)
	}
	templates := f.runtime.PeriodicJobCatalog()
	missing := templates[len(templates)-1].ID
	for _, template := range templates {
		if template.ID == receiptCleanupPeriodicID || template.ID == missing {
			continue
		}
		input := periodicInputForTest(template.ID, template.Kind)
		input.Args = template.DefaultArgs
		input.Priority = 4
		input.Paused = true
		input.Schedule.CronExpression = "@every 24h"
		if _, err := f.runtime.UpdatePeriodicJob(ctx, template.ID, input); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.runtime.DeletePeriodicJob(ctx, missing); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		var value string
		if err := f.db.Pool.QueryRow(ctx, "SELECT jsonb_agg(to_jsonb(p) ORDER BY id)::text FROM river_periodic_job p WHERE id<>$1 AND id<>$2", receiptCleanupPeriodicID, missing).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := snapshot()
	f.start(t)
	if snapshot() != before {
		t.Fatal("startup overwrote customized default definitions")
	}
	if _, err := f.runtime.client.PeriodicJobGet(ctx, missing); err != nil {
		t.Fatalf("duplicate default insert poisoned transaction before missing default: %v", err)
	}
	f.copyCrash(t)
	f.assertRecovered(t)
}

// This is metadata-only staging/queue proof, not content-integrity or legacy-restore proof.
func seedReceiptBatchSubtree(t *testing.T, f *receiptRuntimeFixture) []uuid.UUID {
	t.Helper()
	ctx := context.Background()
	// The fixture stands in for an already-populated library; the ordinary 512
	// append bound is proven by TestB3BulkCatalogLane.
	seedTx, err := f.db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer seedTx.Rollback(ctx)
	if _, err := seedTx.Exec(ctx, "SET LOCAL teldrive.event_bulk = 'on'"); err != nil {
		t.Fatal(err)
	}
	if _, err := seedTx.Exec(ctx, `INSERT INTO files(id,user_id,parent_id,name,kind,status,size,mod_time)
 SELECT ('10000000-0000-0000-0000-'||lpad(to_hex(n),12,'0'))::uuid,1001,$1,'batch-'||n,'file','active',1,clock_timestamp()
 FROM generate_series(1,1003) n`, f.parent); err != nil {
		t.Fatal(err)
	}
	if err := seedTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Pool.Exec(ctx, `INSERT INTO remote_write_receipts(id,row_kind,user_id,origin_kind,operation_id,result_file_id,destination_is_root,lease_token,lease_expires_at,state,published_at)
 VALUES('30000000-0000-0000-0000-000000000001','copy_operation',1001,'copy',
 '30000000-0000-0000-0000-000000000001',$1,true,
 '40000000-0000-0000-0000-000000000001',clock_timestamp()+interval '1 hour','bound',clock_timestamp())`, f.parent); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`INSERT INTO file_parts(file_id,part_no,channel_id,message_id,plain_size,stored_size)
 SELECT ('10000000-0000-0000-0000-'||lpad(to_hex(n),12,'0'))::uuid,1,9001,10000+n,1,1
 FROM generate_series(1,1003) n`,
		`INSERT INTO remote_write_receipts(id,row_kind,user_id,channel_id,message_id,origin_kind,operation_id,result_file_id,part_no,lease_token,lease_expires_at,state)
 SELECT ('20000000-0000-0000-0000-'||lpad(to_hex(n),12,'0'))::uuid,'message_attempt',1001,9001,10000+n,'copy',
 '30000000-0000-0000-0000-000000000001',('10000000-0000-0000-0000-'||lpad(to_hex(n),12,'0'))::uuid,1,
 '40000000-0000-0000-0000-000000000001',clock_timestamp()+interval '1 hour','bound'
 FROM generate_series(1,1002) n`,
		`INSERT INTO upload_sessions(id,user_id,name,expected_size,mod_time,part_size,state,expires_at,completed_at)
 VALUES('50000000-0000-0000-0000-000000000001',1002,'foreign-reference',1,clock_timestamp(),1,'completed',clock_timestamp()+interval '1 hour',clock_timestamp())`,
		`INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size,stored_size,state)
 VALUES('50000000-0000-0000-0000-000000000001',1,9001,11002,1,1,'stored')`,
	} {
		if _, err := f.db.Pool.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	for n := 1; n <= 1003; n++ {
		if err := f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: int64(10000 + n), Bytes: []byte{byte(n)}, Preserve: n > 1001}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.catalog.Trash(ctx, 1001, f.parent); err != nil {
		t.Fatal(err)
	}
	ids := make([]uuid.UUID, 1001)
	for n := range ids {
		ids[n] = uuid.MustParse(fmt.Sprintf("20000000-0000-0000-0000-%012x", n+1))
	}
	return ids
}

func receiptBatchSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(context.Background(), `SELECT jsonb_build_object(
 'files',(SELECT jsonb_agg(to_jsonb(f) ORDER BY id) FROM files f),
 'parts',(SELECT jsonb_agg(to_jsonb(p) ORDER BY file_id,part_no) FROM file_parts p),
 'sessions',(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM upload_sessions s),
 'upload_parts',(SELECT jsonb_agg(to_jsonb(p) ORDER BY upload_id,part_no) FROM upload_parts p),
 'receipts',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM remote_write_receipts r),
 'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM river_job j))::text`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestB2ReceiptCleanupBatches(t *testing.T) {
	for _, consumer := range []string{"subtree", "clean_trash"} {
		t.Run(consumer, func(t *testing.T) {
			for _, outcome := range []string{"success", "later_insert_failure", "later_assignment_failure"} {
				t.Run(outcome, func(t *testing.T) {
					f := newReceiptRuntimeFixture(t)
					ctx := context.Background()
					ids := seedReceiptBatchSubtree(t, f)
					ops, err := fileops.NewService(f.db.Pool, f.catalog, channels.NewService(f.db.Pool, nil, channels.Config{PartLimit: 10000}), f.storage)
					if err != nil {
						t.Fatal(err)
					}
					if err = ops.BindLifecycle(f.policy); err != nil {
						t.Fatal(err)
					}
					stage := func() error {
						if consumer == "clean_trash" {
							_, err := ops.CleanTrash(ctx, 1001)
							return err
						}
						return ops.QueuePurge(ctx, 1001, f.parent)
					}
					// Keep the real producer's admission guard; lifecycle must partition before calling it.
					tx, err := f.db.Pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					_, err = f.runtime.EnqueueReceiptCleanupTx(ctx, tx, lifecycle.ReceiptCleanupInput{UserID: 1001, ReceiptIDs: ids})
					rollbackErr := tx.Rollback(ctx)
					if !errors.Is(err, lifecycle.ErrInvalidInput) || rollbackErr != nil {
						t.Fatalf("unbounded actual producer input: %v; rollback: %v", err, rollbackErr)
					}
					if outcome == "later_insert_failure" {
						if _, err = f.db.Pool.Exec(ctx, `CREATE FUNCTION b2_reject_later_receipt_insert() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF NEW.kind='receipt_cleanup' AND EXISTS(SELECT 1 FROM river_job WHERE kind='receipt_cleanup') THEN
 RAISE EXCEPTION USING MESSAGE='b2 later actual receipt insert',ERRCODE='P0001',
 DETAIL='first_jobs='||(SELECT count(*) FROM river_job WHERE kind='receipt_cleanup')||
 ' first_assignments='||(SELECT count(*) FROM remote_write_receipts WHERE job_id IS NOT NULL AND wakeup_job_id=job_id);
 END IF;
 RETURN NEW;
 END $$;
 CREATE TRIGGER b2_reject_later_receipt_insert BEFORE INSERT ON river_job
 FOR EACH ROW EXECUTE FUNCTION b2_reject_later_receipt_insert()`); err != nil {
							t.Fatal(err)
						}
					}
					if outcome == "later_assignment_failure" {
						if _, err = f.db.Pool.Exec(ctx, `CREATE FUNCTION b2_skip_later_receipt_assignment() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF NEW.wakeup_job_id IS DISTINCT FROM OLD.wakeup_job_id
 AND (SELECT count(*) FROM river_job WHERE kind='receipt_cleanup')>1 THEN RETURN NULL; END IF;
 RETURN NEW;
 END $$;
 CREATE TRIGGER b2_skip_later_receipt_assignment BEFORE UPDATE ON remote_write_receipts
 FOR EACH ROW EXECUTE FUNCTION b2_skip_later_receipt_assignment()`); err != nil {
							t.Fatal(err)
						}
					}
					before := receiptBatchSnapshot(t, f.db.Pool)
					err = stage()
					if outcome != "success" {
						if outcome == "later_insert_failure" {
							var cause *pgconn.PgError
							if !errors.As(err, &cause) || cause.Code != "P0001" ||
								cause.Message != "b2 later actual receipt insert" || cause.Detail != "first_jobs=1 first_assignments=1000" {
								t.Fatalf("lost later-batch insertion cause or first-batch assignment: %v", err)
							}
						} else if !errors.Is(err, lifecycle.ErrReceiptConflict) {
							t.Fatalf("later assignment count mismatch was accepted: %v", err)
						}
						if after := receiptBatchSnapshot(t, f.db.Pool); after != before {
							t.Fatal("later-batch failure committed partial hierarchy, queue, receipt or owner identities")
						}
						if len(f.storage.Deletes()) != 0 {
							t.Fatal("failed staging made a destructive call")
						}
						f.preserved(t)
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					var pending, assigned, protected, unknown int
					if err = f.db.Pool.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM files WHERE parent_id=$1 AND status='deletion_pending'),
 (SELECT count(*) FROM remote_write_receipts r JOIN river_job j ON j.id=r.job_id
  WHERE r.id=ANY($2::uuid[]) AND r.user_id=1001 AND r.state='cleanup_pending' AND r.attempts=0
  AND r.primary_code='file_purge' AND r.wakeup_job_id=r.job_id AND j.id>0 AND j.kind=$3
  AND (j.metadata->>'user_id')::bigint=1001 AND (j.args->>'user_id')::bigint=1001
  AND j.args->'receipt_ids' ? r.id::text),
 (SELECT count(*) FROM remote_write_receipts WHERE message_id=11002 AND state='bound' AND job_id IS NULL AND wakeup_job_id IS NULL AND attempts=0),
 (SELECT count(*) FROM file_parts p WHERE p.message_id=11003 AND NOT EXISTS
  (SELECT 1 FROM remote_write_receipts r WHERE r.channel_id=p.channel_id AND r.message_id=p.message_id))`,
						f.parent, ids, ReceiptCleanupKind).Scan(&pending, &assigned, &protected, &unknown); err != nil ||
						pending != 1003 || assigned != 1001 || protected != 1 || unknown != 1 {
						t.Fatalf("complete bounded staging: pending=%d assigned=%d protected=%d unknown=%d: %v", pending, assigned, protected, unknown, err)
					}
					firstIDs := make([]int64, 0, 2)
					rows, err := f.db.Pool.Query(ctx, "SELECT id,args FROM river_job WHERE kind=$1 ORDER BY id", ReceiptCleanupKind)
					if err != nil {
						t.Fatal(err)
					}
					var queued []uuid.UUID
					for rows.Next() {
						var id int64
						var raw []byte
						var args ReceiptCleanupArgs
						if err = rows.Scan(&id, &raw); err != nil {
							rows.Close()
							t.Fatal(err)
						}
						if err = json.Unmarshal(raw, &args); err != nil || args.UserID != 1001 || id <= 0 {
							rows.Close()
							t.Fatalf("actual queued owner/args: %+v %v", args, err)
						}
						batchSize := 1000
						if len(firstIDs) == 1 {
							batchSize = 1
						}
						if len(firstIDs) >= 2 || len(args.ReceiptIDs) != batchSize {
							rows.Close()
							t.Fatalf("queue batch sizes: batch=%d count=%d", len(firstIDs), len(args.ReceiptIDs))
						}
						firstIDs = append(firstIDs, id)
						queued = append(queued, args.ReceiptIDs...)
					}
					rows.Close()
					if err = rows.Err(); err != nil || len(firstIDs) != 2 || !slices.Equal(queued, ids) {
						t.Fatalf("queue truncated, duplicated or reordered identities: jobs=%v count=%d %v", firstIDs, len(queued), err)
					}
					for _, id := range firstIDs {
						if report, e := f.runtime.GetForUser(ctx, id, 1001); e != nil || report.ID != id {
							t.Fatalf("actual owner report %d: %+v %v", id, report, e)
						}
						if _, e := f.runtime.GetForUser(ctx, id, 1002); !errors.Is(e, river.ErrNotFound) {
							t.Fatalf("foreign receipt report %d: %v", id, e)
						}
					}
					if len(f.storage.Deletes()) != 0 {
						t.Fatal("staging unexpectedly ran immediate cleanup")
					}
					// Requeue due rows without changing claims, lifetime attempts or first owner reports.
					if _, err = f.db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=ANY($1::uuid[])", ids); err != nil {
						t.Fatal(err)
					}
					if err = ops.QueuePurge(ctx, 1001, f.parent); err != nil {
						t.Fatal(err)
					}
					var retained, jobs int
					var current int64
					if err = f.db.Pool.QueryRow(ctx, `SELECT
 count(*) FILTER(WHERE r.job_id=CASE WHEN r.message_id<=11000 THEN $2::bigint ELSE $3::bigint END
 AND r.wakeup_job_id>r.job_id AND r.attempts=0 AND r.primary_code='file_purge'
 AND j.kind=$4 AND (j.metadata->>'user_id')::bigint=1001 AND j.args->'receipt_ids' ? r.id::text),
 (SELECT count(*) FROM river_job WHERE kind=$4),
 max(r.wakeup_job_id) FILTER(WHERE r.message_id=11001)
 FROM remote_write_receipts r JOIN river_job j ON j.id=r.wakeup_job_id WHERE r.id=ANY($1::uuid[])`,
						ids, firstIDs[0], firstIDs[1], ReceiptCleanupKind).Scan(&retained, &jobs, &current); err != nil || retained != 1001 || jobs != 4 || current <= 0 {
						t.Fatalf("replacement lost immutable first/current actual assignments: %d/%d/%d %v", retained, jobs, current, err)
					}
					// Run only the actual one-receipt tail job; never stretch the five-second cleanup budget.
					job, err := f.runtime.client.JobGet(ctx, current)
					if err != nil {
						t.Fatal(err)
					}
					var args ReceiptCleanupArgs
					if err = json.Unmarshal(job.EncodedArgs, &args); err != nil || args.UserID != 1001 ||
						!slices.Equal(args.ReceiptIDs, ids[1000:]) {
						t.Fatalf("bounded actual continuation args: %+v %v", args, err)
					}
					worker := NewReceiptCleanupWorker(f.db.Pool, f.policy)
					work := &river.Job[ReceiptCleanupArgs]{JobRow: job, Args: args}
					if worker.Timeout(work) != 5*time.Second {
						t.Fatal("cleanup worker budget changed")
					}
					tx, err = f.db.Pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback(ctx)
					tester := rivertest.NewWorker(t, riverpgxv5.New(f.db.Pool), &river.Config{SkipUnknownJobCheck: true}, worker)
					result, err := tester.WorkJob(ctx, t, tx, job)
					if err != nil || result == nil || result.Job == nil || result.Job.ID != current ||
						result.EventKind != river.EventKindJobCompleted {
						t.Fatalf("bounded actual worker continuation: %+v %v", result, err)
					}
					if err = tx.Commit(ctx); err != nil {
						t.Fatal(err)
					}
					var completed, stillPending int
					if err = f.db.Pool.QueryRow(ctx, `SELECT
 count(*) FILTER(WHERE id=$1 AND state='deleted' AND attempts=1 AND job_id=$2 AND wakeup_job_id=$3),
 count(*) FILTER(WHERE id<>$1 AND state='cleanup_pending' AND attempts=0)
 FROM remote_write_receipts WHERE id=ANY($4::uuid[])`, ids[1000], firstIDs[1], current, ids).Scan(&completed, &stillPending); err != nil || completed != 1 || stillPending != 1000 {
						t.Fatalf("bounded continuation lost ACK, first/current IDs or remaining work: %d/%d %v", completed, stillPending, err)
					}
					calls := f.storage.Deletes()
					if len(calls) != 1 || calls[0].UserID != 1001 || calls[0].ChannelID != 9001 ||
						!slices.Equal(calls[0].MessageIDs, []int64{11001}) || !slices.Equal(calls[0].DeletedIDs, []int64{11001}) ||
						len(calls[0].PreservedIDs) != 0 || calls[0].Err != nil {
						t.Fatalf("continuation deleted foreign, protected, unknown or unrelated messages: %+v", calls)
					}
					if _, ok := f.storage.Bytes(9001, 11001); ok {
						t.Fatal("positive exact ACK retained tail message")
					}
					for n := 1; n <= 1000; n++ {
						if _, ok := f.storage.Bytes(9001, int64(10000+n)); !ok {
							t.Fatalf("bounded continuation lost queued sibling %d", n)
						}
					}
					if _, err = f.db.Pool.Exec(ctx, "DELETE FROM river_job WHERE id=ANY($1::bigint[])", firstIDs); err != nil {
						t.Fatal(err)
					}
					for n, id := range firstIDs {
						want := "pending"
						if n == 1 {
							want = "completed"
						}
						if report, e := f.runtime.GetForUser(ctx, id, 1001); e != nil || report.ID != id || report.State != want {
							t.Fatalf("retained first owner report %d: %+v %v", id, report, e)
						}
						if _, e := f.runtime.GetForUser(ctx, id, 1002); !errors.Is(e, river.ErrNotFound) {
							t.Fatalf("retained foreign receipt report %d: %v", id, e)
						}
					}
					f.preserved(t)
				})
			}
		})
	}
}
