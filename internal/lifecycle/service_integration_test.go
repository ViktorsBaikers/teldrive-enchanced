//go:build integration

package lifecycle_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	fixture "github.com/tgdrive/teldrive/v2/internal/testutil/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
)

// This producer uses real caller SQL, not a sequence guess. Runtime's actual
// River producer and deepest destructive invocation count have separate gates.
type receiptQueue struct{ fail error }

func (q *receiptQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	if in.UserID <= 0 || len(in.ReceiptIDs) == 0 || len(in.ReceiptIDs) > 1000 {
		return 0, lifecycle.ErrInvalidInput
	}
	if q.fail != nil {
		return 0, q.fail
	}
	var id int64
	err := tx.QueryRow(ctx, "INSERT INTO b2_receipt_jobs(user_id, receipt_ids) VALUES ($1,$2) RETURNING id", in.UserID, in.ReceiptIDs).Scan(&id)
	return id, err
}

type outsideSQLDeleter struct {
	pool    *pgxpool.Pool
	storage *fixture.Storage
}

func (d outsideSQLDeleter) DeleteMessages(ctx context.Context, userID, channelID int64, ids []int64) error {
	var open bool
	if err := d.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='idle in transaction')").Scan(&open); err != nil {
		return err
	}
	if open {
		return errors.New("destructive RPC under an open SQL transaction")
	}
	return d.storage.DeleteMessages(ctx, userID, channelID, ids)
}

type ledgerFixture struct {
	pool    *pgxpool.Pool
	svc     *lifecycle.Service
	queue   *receiptQueue
	storage *fixture.Storage
}

func newLedger(t *testing.T) *ledgerFixture {
	t.Helper()
	db := testpostgres.New(t)
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, "CREATE TABLE b2_receipt_jobs(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,user_id bigint NOT NULL,receipt_ids uuid[] NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, "INSERT INTO users(user_id) VALUES (1001),(1002)"); err != nil {
		t.Fatal(err)
	}
	storage := fixture.NewStorage(t, []fixture.ChannelScope{{ChannelID: 9001, Actors: []int64{1001, 1002}}, {ChannelID: 9002, Actors: []int64{1001, 1002}}})
	queue := &receiptQueue{}
	svc := lifecycle.NewService(db.Pool, outsideSQLDeleter{db.Pool, storage})
	if err := svc.BindReceiptEnqueuer(queue); err != nil {
		t.Fatal(err)
	}
	return &ledgerFixture{db.Pool, svc, queue, storage}
}
func transaction(t *testing.T, pool *pgxpool.Pool, fn func(pgx.Tx) error) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := fn(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
func reserveCopy(t *testing.T, f *ledgerFixture) (uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	op, result, token := uuid.New(), uuid.New(), uuid.New()
	transaction(t, f.pool, func(tx pgx.Tx) error {
		_, created, err := f.svc.ReserveCopyTx(context.Background(), tx, lifecycle.ReserveCopyInput{UserID: 1001, OperationID: op, RootResultFileID: result, LeaseToken: token, DestinationIsRoot: true, LeaseExpiresAt: time.Now().UTC().Add(time.Hour)})
		if err == nil && !created {
			return errors.New("fresh operation was not created")
		}
		return err
	})
	return op, result, token
}
func receipt(t *testing.T, f *ledgerFixture, channel, message int64) uuid.UUID {
	t.Helper()
	op, result, token := reserveCopy(t, f)
	id := uuid.New()
	_, err := f.svc.Reserve(context.Background(), lifecycle.ReserveInput{ID: id, UserID: 1001, ChannelID: channel, OriginKind: "copy", OperationID: &op, ResultFileID: &result, PartNo: 1, LeaseToken: token, LeaseExpiresAt: time.Now().UTC().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if message > 0 {
		_, err = f.svc.Observe(context.Background(), lifecycle.ObserveInput{ReceiptID: id, UserID: 1001, ChannelID: channel, MessageID: message, LeaseToken: token, OperationID: &op, ResultFileID: &result})
		if err != nil {
			t.Fatal(err)
		}
	}
	return id
}
func abandon(t *testing.T, f *ledgerFixture, id uuid.UUID) {
	t.Helper()
	// This is the real monotone operation CAS; no receipt state is fabricated.
	if _, err := sqlcgen.New(f.pool).TerminalizeCopyOperation(context.Background(), sqlcgen.TerminalizeCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(copyOrigin(t, f, id)), FailureCode: "remote_write_abandoned"}); err != nil {
		t.Fatal(err)
	}
}
func copyOrigin(t *testing.T, f *ledgerFixture, id uuid.UUID) uuid.UUID {
	t.Helper()
	var op uuid.UUID
	if err := f.pool.QueryRow(context.Background(), "SELECT operation_id FROM remote_write_receipts WHERE id=$1", id).Scan(&op); err != nil {
		t.Fatal(err)
	}
	return op
}
func stage(t *testing.T, f *ledgerFixture, ids ...uuid.UUID) {
	t.Helper()
	transaction(t, f.pool, func(tx pgx.Tx) error {
		return f.svc.StageCleanupTx(context.Background(), tx, lifecycle.StageCleanupInput{UserID: 1001, ReceiptIDs: ids, PrimaryCode: "remote_write_failed"})
	})
}
func TestB2ReceiptRoundTrip(t *testing.T) {
	f := newLedger(t)
	ctx := context.Background()
	payload := []byte("independent owned receipt bytes")
	want := sha256.Sum256(payload)
	for _, m := range []fixture.Message{{UserID: 1001, ChannelID: 9001, MessageID: 41, Bytes: payload}, {UserID: 1002, ChannelID: 9001, MessageID: 42, Bytes: []byte("foreign"), Preserve: true}, {UserID: 1001, ChannelID: 9002, MessageID: 41, Bytes: []byte("same number other channel"), Preserve: true}} {
		if err := f.storage.Seed(m); err != nil {
			t.Fatal(err)
		}
	}
	id := receipt(t, f, 9001, 41)
	got, _ := f.storage.Bytes(9001, 41)
	if sha256.Sum256(got) != want {
		t.Fatal("fixture evidence changed")
	}
	abandon(t, f, id)
	stage(t, f, id, id)
	var first, current int64
	if err := f.pool.QueryRow(ctx, "SELECT job_id,wakeup_job_id FROM remote_write_receipts WHERE id=$1", id).Scan(&first, &current); err != nil || first <= 0 || first != current {
		t.Fatalf("actual job IDs: %d %d %v", first, current, err)
	}
	if err := f.svc.Recover(ctx, 1001, []uuid.UUID{id, id}); err != nil {
		t.Fatal(err)
	}
	row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
	if err != nil || row.State != "deleted" || row.Attempts != 1 {
		t.Fatalf("receipt = %#v, %v", row, err)
	}
	if _, ok := f.storage.Bytes(9001, 41); ok {
		t.Fatal("owned bytes remain after exact ACK")
	}
	if err := f.storage.CheckPreserved(); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.Recover(ctx, 1001, []uuid.UUID{id}); err != nil {
		t.Fatal(err)
	}
	if len(f.storage.Deletes()) != 1 {
		t.Fatal("duplicate ACK resent deletion")
	}
	if _, err = f.pool.Exec(ctx, "DELETE FROM b2_receipt_jobs WHERE id=$1", first); err != nil {
		t.Fatal(err)
	}
	report, err := f.svc.GetReceiptJobReport(ctx, 1001, first)
	if err != nil || report.JobID != first || report.State != "completed" {
		t.Fatalf("retained owner report: %#v %v", report, err)
	}
	if report, err = f.svc.GetReceiptJobReport(ctx, 1002, first); !errors.Is(err, pgx.ErrNoRows) || report != nil {
		t.Fatalf("foreign report disclosed: %#v %v", report, err)
	}
	t.Run("enqueue_rollback", func(t *testing.T) {
		id := receipt(t, f, 9001, 43)
		abandon(t, f, id)
		cause := errors.New("fixture enqueue failure")
		f.queue.fail = cause
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		err = f.svc.StageCleanupTx(ctx, tx, lifecycle.StageCleanupInput{UserID: 1001, ReceiptIDs: []uuid.UUID{id}, PrimaryCode: "remote_write_failed"})
		if !errors.Is(err, cause) {
			t.Fatalf("lost persistence cause: %v", err)
		}
		_ = tx.Rollback(ctx)
		f.queue.fail = nil
		row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil || row.State != "observed" || row.JobID.Valid {
			t.Fatalf("partial enqueue: %#v %v", row, err)
		}
	})
	t.Run("no_receipt_report", func(t *testing.T) {
		id := receipt(t, f, 9001, 0)
		abandon(t, f, id)
		if err := f.svc.DiscoverRecoveryCandidate(ctx, id, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil || row.State != "unresolved" || !row.JobID.Valid || row.MessageID.Valid {
			t.Fatalf("report debt %#v %v", row, err)
		}
		if len(f.storage.Deletes()) != 1 {
			t.Fatal("no-receipt report deleted")
		}
	})
	t.Run("live_origin_and_fixed_cursor", func(t *testing.T) {
		f := newLedger(t)
		op, result, token := reserveCopy(t, f)
		// A stale receipt lease is only a scan hint. The actual operation is live.
		if _, err := f.pool.Exec(ctx, "INSERT INTO remote_write_receipts(id,row_kind,user_id,channel_id,origin_kind,operation_id,result_file_id,part_no,lease_token,lease_expires_at,created_at) SELECT gen_random_uuid(),'message_attempt',1001,9001,'copy',$1,$2,n,$3,clock_timestamp()-interval '1 hour',statement_timestamp()-interval '2 hours' FROM generate_series(1,1000) n", op, result, token); err != nil {
			t.Fatal(err)
		}
		later := receipt(t, f, 9002, 101)
		abandon(t, f, later)
		var cutoff time.Time
		if err := f.pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&cutoff); err != nil {
			t.Fatal(err)
		}
		page, err := f.svc.ListLifecycleRecoveryCandidates(ctx, lifecycle.RecoveryCandidatesInput{PassCutoff: cutoff, PageSize: 1000})
		if err != nil || len(page) != 1000 {
			t.Fatalf("first retained page: %d %v", len(page), err)
		}
		for _, r := range page {
			if err = f.svc.DiscoverRecoveryCandidate(ctx, uuid.UUID(r.ID.Bytes), cutoff); err != nil {
				t.Fatal(err)
			}
		}
		last := page[len(page)-1]
		afterTime, afterID := last.CreatedAt.Time, uuid.UUID(last.ID.Bytes)
		next, err := f.svc.ListLifecycleRecoveryCandidates(ctx, lifecycle.RecoveryCandidatesInput{PassCutoff: cutoff, AfterCreatedAt: &afterTime, AfterID: &afterID, PageSize: 1000})
		found := false
		for _, r := range next {
			if uuid.UUID(r.ID.Bytes) == later {
				found = true
			}
		}
		if err != nil || len(next) != 2 || !found {
			t.Fatalf("cursor failed to advance past retained page: %#v %v", next, err)
		}
		if err = f.svc.DiscoverRecoveryCandidate(ctx, later, cutoff); err != nil {
			t.Fatal(err)
		}
		var retainedJobs int
		if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM remote_write_receipts WHERE operation_id=$1 AND job_id IS NOT NULL", op).Scan(&retainedJobs); err != nil || retainedJobs != 0 {
			t.Fatalf("live operation staged: %d %v", retainedJobs, err)
		}
	})
	t.Run("job_assignment_rollback", func(t *testing.T) {
		f := newLedger(t)
		id := receipt(t, f, 9001, 103)
		abandon(t, f, id)
		if _, err := f.pool.Exec(ctx, "ALTER TABLE remote_write_receipts ADD CONSTRAINT b2_reject_job_assignment CHECK (wakeup_job_id IS NULL)"); err != nil {
			t.Fatal(err)
		}
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err = f.svc.StageCleanupTx(ctx, tx, lifecycle.StageCleanupInput{UserID: 1001, ReceiptIDs: []uuid.UUID{id}, PrimaryCode: "remote_write_failed"}); err == nil {
			t.Fatal("ID assignment failure was hidden")
		}
		_ = tx.Rollback(ctx)
		var jobs int
		if err = f.pool.QueryRow(ctx, "SELECT count(*) FROM b2_receipt_jobs").Scan(&jobs); err != nil || jobs != 0 {
			t.Fatalf("enqueue escaped rollback: %d %v", jobs, err)
		}
		row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil || row.State != "observed" || row.JobID.Valid {
			t.Fatalf("state escaped rollback: %#v %v", row, err)
		}
	})
	t.Run("empty_operation_report", func(t *testing.T) {
		f := newLedger(t)
		op, _, _ := reserveCopy(t, f)
		if _, err := sqlcgen.New(f.pool).TerminalizeCopyOperation(ctx, sqlcgen.TerminalizeCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(op), FailureCode: "remote_write_abandoned"}); err != nil {
			t.Fatal(err)
		}
		if err := f.svc.DiscoverRecoveryCandidate(ctx, op, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(op))
		if err != nil || row.MessageID.Valid || row.Attempts != 0 || !row.JobID.Valid {
			t.Fatalf("empty-copy report: %#v %v", row, err)
		}
		if err = f.svc.Recover(ctx, 1001, []uuid.UUID{op}); !errors.Is(err, lifecycle.ErrUnresolved) || len(f.storage.Deletes()) != 0 {
			t.Fatalf("empty-copy cleanup: %v", err)
		}
	})
	t.Run("published_winner", func(t *testing.T) {
		f := newLedger(t)
		id := receipt(t, f, 9001, 104)
		op := copyOrigin(t, f, id)
		row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil {
			t.Fatal(err)
		}
		result := uuid.UUID(row.ResultFileID.Bytes)
		if err = f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: 104, Bytes: []byte("published winner"), Preserve: true}); err != nil {
			t.Fatal(err)
		}
		transaction(t, f.pool, func(tx pgx.Tx) error {
			if err := f.svc.BindTx(ctx, tx, lifecycle.BindInput{UserID: 1001, ReceiptIDs: []uuid.UUID{id}, ResultFileID: &result}); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,size,mod_time) VALUES ($1,1001,'winner','file',16,now())", result); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id) VALUES ($1,1,9001,104)", result); err != nil {
				return err
			}
			n, err := sqlcgen.New(tx).PublishCopyOperation(ctx, sqlcgen.PublishCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(op), LeaseToken: row.LeaseToken})
			if err == nil && n != 1 {
				return errors.New("winner did not publish")
			}
			return err
		})
		n, err := sqlcgen.New(f.pool).TerminalizeCopyOperation(ctx, sqlcgen.TerminalizeCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(op), FailureCode: "copy_destination_unavailable"})
		if err != nil || n != 0 {
			t.Fatalf("published operation terminalized: %d %v", n, err)
		}
		stage(t, f, id)
		if err = f.svc.Recover(ctx, 1001, []uuid.UUID{id}); err != nil || len(f.storage.Deletes()) != 0 {
			t.Fatalf("winner compensated: %v", err)
		}
		if err = f.storage.CheckPreserved(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("upload_origin_exact_target", func(t *testing.T) {
		f := newLedger(t)
		upload, token, id := uuid.New(), uuid.New(), uuid.New()
		expiry := time.Now().UTC().Add(time.Hour)
		transaction(t, f.pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "INSERT INTO upload_sessions(id,user_id,name,expected_size,mod_time,part_size,expires_at) VALUES ($1,1001,'origin',3,now(),3,$2)", upload, expiry); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO upload_parts(upload_id,part_no,channel_id,plain_size,state,lease_token,lease_expires_at) VALUES ($1,1,9001,3,'uploading',$2,$3)", upload, token, expiry)
			return err
		})
		if _, err := f.svc.Reserve(ctx, lifecycle.ReserveInput{ID: id, UserID: 1001, ChannelID: 9001, OriginKind: "upload", UploadID: &upload, PartNo: 1, LeaseToken: token, LeaseExpiresAt: expiry}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.svc.Observe(ctx, lifecycle.ObserveInput{ReceiptID: id, UserID: 1001, ChannelID: 9001, MessageID: 105, LeaseToken: token, UploadID: &upload, PartNo: 1}); err != nil {
			t.Fatal(err)
		}
		transaction(t, f.pool, func(tx pgx.Tx) error {
			if err := f.svc.BindTx(ctx, tx, lifecycle.BindInput{UserID: 1001, ReceiptIDs: []uuid.UUID{id}}); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "UPDATE upload_parts SET state='stored',message_id=105,stored_size=3,lease_token=NULL,lease_expires_at=NULL WHERE upload_id=$1", upload)
			return err
		})
		if err := f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: 105, Bytes: []byte("abc")}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(ctx, "UPDATE upload_sessions SET state='aborted',terminal_failure_code='upload_parent_unavailable' WHERE id=$1", upload); err != nil {
			t.Fatal(err)
		}
		stage(t, f, id)
		if err := f.svc.Recover(ctx, 1001, []uuid.UUID{id}); err != nil {
			t.Fatal(err)
		}
		row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil || row.State != "deleted" {
			t.Fatalf("exact terminal upload target: %#v %v", row, err)
		}
	})
	t.Run("enqueuer_initialization", func(t *testing.T) {
		unbound := lifecycle.NewService(f.pool, f.storage)
		var nilProducer *receiptQueue
		if err := unbound.BindReceiptEnqueuer(nilProducer); !errors.Is(err, lifecycle.ErrEnqueuerUnavailable) {
			t.Fatalf("typed nil producer: %v", err)
		}
		if err := unbound.BindReceiptEnqueuer(f.queue); err != nil {
			t.Fatal(err)
		}
		if err := unbound.BindReceiptEnqueuer(f.queue); !errors.Is(err, lifecycle.ErrEnqueuerUnavailable) {
			t.Fatalf("producer rebound: %v", err)
		}
	})
	t.Run("copy_destination_terminalization_rollback", func(t *testing.T) {
		f := newLedger(t)
		parent, op, result, token, id := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
		if _, err := f.pool.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,mod_time) VALUES ($1,1001,'destination','folder',now())", parent); err != nil {
			t.Fatal(err)
		}
		transaction(t, f.pool, func(tx pgx.Tx) error {
			_, _, err := f.svc.ReserveCopyTx(ctx, tx, lifecycle.ReserveCopyInput{UserID: 1001, OperationID: op, RootResultFileID: result, LeaseToken: token, DestinationParentID: &parent, LeaseExpiresAt: time.Now().UTC().Add(time.Hour)})
			return err
		})
		if _, err := f.svc.Reserve(ctx, lifecycle.ReserveInput{ID: id, UserID: 1001, ChannelID: 9001, OriginKind: "copy", OperationID: &op, ResultFileID: &result, PartNo: 1, LeaseToken: token, LeaseExpiresAt: time.Now().UTC().Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.svc.Observe(ctx, lifecycle.ObserveInput{ReceiptID: id, UserID: 1001, ChannelID: 9001, MessageID: 106, LeaseToken: token, OperationID: &op, ResultFileID: &result}); err != nil {
			t.Fatal(err)
		}
		cause := errors.New("fixture terminal enqueue failure")
		f.queue.fail = cause
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, _, err = f.svc.TerminalizeCopyDestinationsTx(ctx, tx, 1001, []uuid.UUID{parent}); !errors.Is(err, cause) {
			t.Fatalf("terminal persistence cause lost: %v", err)
		}
		_ = tx.Rollback(ctx)
		f.queue.fail = nil
		original, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(op))
		if err != nil || original.TerminalAt.Valid || original.JobID.Valid {
			t.Fatalf("terminalization escaped rollback: %#v %v", original, err)
		}
		transaction(t, f.pool, func(tx pgx.Tx) error {
			operations, receipts, err := f.svc.TerminalizeCopyDestinationsTx(ctx, tx, 1001, []uuid.UUID{parent})
			if err == nil && (operations != 1 || receipts != 1) {
				return fmt.Errorf("terminal counts = %d/%d", operations, receipts)
			}
			return err
		})
		n, err := sqlcgen.New(f.pool).PublishCopyOperation(ctx, sqlcgen.PublishCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(op), LeaseToken: dbtypes.UUID(token)})
		if err != nil || n != 0 {
			t.Fatalf("terminal copy revived: %d %v", n, err)
		}
	})
}
func TestB2ReferenceFence(t *testing.T) {
	for _, state := range []string{"open", "aborted", "expired", "completed"} {
		t.Run("independent_"+state, func(t *testing.T) {
			f := newLedger(t)
			id := receipt(t, f, 9001, 51)
			abandon(t, f, id)
			other := uuid.New()
			_, err := f.pool.Exec(context.Background(), "INSERT INTO upload_sessions(id,user_id,name,expected_size,mod_time,part_size,state,expires_at,completed_at) VALUES ($1,1002,'independent',1,now(),1,$2::upload_state,now()+interval '1 hour',CASE WHEN $2::upload_state='completed' THEN now() END)", other, state)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.pool.Exec(context.Background(), "INSERT INTO upload_parts(upload_id,part_no,channel_id,message_id,plain_size) VALUES ($1,1,9001,51,1)", other); err != nil {
				t.Fatal(err)
			}
			stage(t, f, id)
			if err = f.svc.Recover(context.Background(), 1001, []uuid.UUID{id}); err != nil {
				t.Fatal(err)
			}
			if len(f.storage.Deletes()) != 0 {
				t.Fatal("independent all-state reference deleted")
			}
		})
	}
	for _, status := range []string{"active", "trashed", "deletion_pending"} {
		t.Run("independent_file_"+status, func(t *testing.T) {
			f := newLedger(t)
			ctx := context.Background()
			id := receipt(t, f, 9001, 52)
			abandon(t, f, id)
			fileID := uuid.New()
			if _, err := f.pool.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,size,status,deleted_at,mod_time) VALUES ($1,1002,'independent','file',1,$2::file_status,CASE WHEN $2::file_status='active' THEN NULL ELSE now() END,now())", fileID, status); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id) VALUES ($1,1,9001,52)", fileID); err != nil {
				t.Fatal(err)
			}
			stage(t, f, id)
			claims, err := f.svc.ClaimDelete(ctx, 1001, []uuid.UUID{id})
			if err != nil || len(claims) != 0 {
				t.Fatalf("independent file veto lost: %#v %v", claims, err)
			}
		})
	}
	for _, bindFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("bind_first_%t", bindFirst), func(t *testing.T) {
			f := newLedger(t)
			id := receipt(t, f, 9001, 61)
			abandon(t, f, id)
			if !bindFirst {
				stage(t, f, id)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			blocker, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(ctx)
			if err = lifecycle.LockOwnerTx(ctx, blocker, 1001); err != nil {
				t.Fatal(err)
			}
			waiter, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer waiter.Rollback(ctx)
			out := make(chan error, 1)
			go func() { out <- lifecycle.LockOwnerTx(ctx, waiter, 1001) }()
			if err = fixture.WaitForBlocking(ctx, f.pool, waiter.Conn().PgConn().PID(), blocker.Conn().PgConn().PID()); err != nil {
				t.Fatal(err)
			}
			if bindFirst {
				if err = lifecycle.RequireBindableMessagesTx(ctx, blocker, []lifecycle.PhysicalMessage{{ChannelID: 9001, MessageID: 61}}); err != nil {
					t.Fatal(err)
				}
				if _, err = blocker.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,size,mod_time) VALUES ($1,1001,'protected','file',1,now())", uuid.MustParse("00000000-0000-4000-8000-000000000001")); err != nil {
					t.Fatal(err)
				}
				if _, err = blocker.Exec(ctx, "INSERT INTO file_parts(file_id,part_no,channel_id,message_id) VALUES ('00000000-0000-4000-8000-000000000001',1,9001,61)"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err = lifecycle.LockChannelWritesTx(ctx, blocker, []int64{9001}); err != nil {
					t.Fatal(err)
				}
				if err = lifecycle.LockMessagesTx(ctx, blocker, []lifecycle.PhysicalMessage{{ChannelID: 9001, MessageID: 61}}); err != nil {
					t.Fatal(err)
				}
				if _, err = sqlcgen.New(blocker).ClaimReceiptDeletion(ctx, sqlcgen.ClaimReceiptDeletionParams{UserID: 1001, ReceiptID: dbtypes.UUID(id)}); err != nil {
					t.Fatal(err)
				}
			}
			if err = blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err = <-out; err != nil {
				t.Fatal(err)
			}
			if !bindFirst {
				err = lifecycle.RequireBindableMessagesTx(ctx, waiter, []lifecycle.PhysicalMessage{{ChannelID: 9001, MessageID: 61}})
				if !errors.Is(err, lifecycle.ErrDeletionFenced) {
					t.Fatalf("late bind: %v", err)
				}
			}
			_ = waiter.Rollback(ctx)
			if bindFirst {
				stage(t, f, id)
				claims, err := f.svc.ClaimDelete(ctx, 1001, []uuid.UUID{id})
				if err != nil || len(claims) != 0 {
					t.Fatalf("reference lost: %v %v", claims, err)
				}
			}
		})
	}
	for _, reservationFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("channel_reservation_first_%t", reservationFirst), func(t *testing.T) {
			f := newLedger(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			op, result, token := reserveCopy(t, f)
			blocker, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(ctx)
			waiter, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer waiter.Rollback(ctx)
			firstOwner, secondOwner := int64(1002), int64(1001)
			if reservationFirst {
				firstOwner, secondOwner = 1001, 1002
			}
			if err = lifecycle.LockOwnerTx(ctx, blocker, firstOwner); err != nil {
				t.Fatal(err)
			}
			if err = lifecycle.LockOwnerTx(ctx, waiter, secondOwner); err != nil {
				t.Fatal(err)
			}
			originTx := waiter
			if reservationFirst {
				originTx = blocker
			}
			if _, err = sqlcgen.New(originTx).LockCopyOperation(ctx, sqlcgen.LockCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(op)}); err != nil {
				t.Fatal(err)
			}
			if err = lifecycle.LockChannelWritesTx(ctx, blocker, []int64{9001}); err != nil {
				t.Fatal(err)
			}
			out := make(chan error, 1)
			go func() { out <- lifecycle.LockChannelWritesTx(ctx, waiter, []int64{9001}) }()
			if err = fixture.WaitForBlocking(ctx, f.pool, waiter.Conn().PgConn().PID(), blocker.Conn().PgConn().PID()); err != nil {
				t.Fatal(err)
			}
			if reservationFirst {
				_, err = sqlcgen.New(blocker).ReserveMessageReceipt(ctx, sqlcgen.ReserveMessageReceiptParams{ID: dbtypes.UUID(uuid.New()), UserID: 1001, ChannelID: 9001, OriginKind: "copy", PartNo: 1, LeaseToken: dbtypes.UUID(token), OperationID: dbtypes.UUID(op), ResultFileID: dbtypes.UUID(result), LeaseExpiresAt: dbtypes.Time(time.Now().UTC().Add(time.Hour))})
			} else {
				_, err = sqlcgen.New(blocker).InsertChannelDeletionIntent(ctx, sqlcgen.InsertChannelDeletionIntentParams{ChannelID: 9001, IntentID: dbtypes.UUID(uuid.New()), RequestedBy: 1002})
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err = <-out; err != nil {
				t.Fatal(err)
			}
			if reservationFirst {
				protected, err := sqlcgen.New(waiter).HasChannelDeletionProtection(ctx, 9001)
				if err != nil || !protected {
					t.Fatalf("cross-owner no-message reservation unprotected: %v", err)
				}
			} else if err = lifecycle.RequireWritableChannelsTx(ctx, waiter, []int64{9001}); !errors.Is(err, lifecycle.ErrChannelWriteUnavailable) {
				t.Fatalf("post-intent reservation: %v", err)
			}
		})
	}
	t.Run("receipt_identity_controls", func(t *testing.T) {
		f := newLedger(t)
		ctx := context.Background()
		id := receipt(t, f, 9001, 111)
		op := copyOrigin(t, f, id)
		row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil {
			t.Fatal(err)
		}
		result, token := uuid.UUID(row.ResultFileID.Bytes), uuid.UUID(row.LeaseToken.Bytes)
		for _, in := range []lifecycle.ObserveInput{
			{ReceiptID: id, UserID: 1002, ChannelID: 9001, MessageID: 111, LeaseToken: token, OperationID: &op, ResultFileID: &result},
			{ReceiptID: id, UserID: 1001, ChannelID: 9002, MessageID: 111, LeaseToken: token, OperationID: &op, ResultFileID: &result},
			{ReceiptID: id, UserID: 1001, ChannelID: 9001, MessageID: 0, LeaseToken: token, OperationID: &op, ResultFileID: &result},
			{ReceiptID: id, UserID: 1001, ChannelID: 9001, MessageID: 111, LeaseToken: uuid.New(), OperationID: &op, ResultFileID: &result},
		} {
			if _, err = f.svc.Observe(ctx, in); err == nil {
				t.Fatal("invalid physical receipt accepted")
			}
		}
		if _, err = f.svc.ClaimDelete(ctx, 1001, []uuid.UUID{uuid.Nil}); !errors.Is(err, lifecycle.ErrInvalidInput) {
			t.Fatalf("nil claim: %v", err)
		}
		otherOp, otherResult, otherToken := reserveCopy(t, f)
		otherID := uuid.New()
		if _, err = f.svc.Reserve(ctx, lifecycle.ReserveInput{ID: otherID, UserID: 1001, ChannelID: 9001, OriginKind: "copy", OperationID: &otherOp, ResultFileID: &otherResult, PartNo: 1, LeaseToken: otherToken, LeaseExpiresAt: time.Now().UTC().Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		if _, err = f.svc.Observe(ctx, lifecycle.ObserveInput{ReceiptID: otherID, UserID: 1001, ChannelID: 9001, MessageID: 111, LeaseToken: otherToken, OperationID: &otherOp, ResultFileID: &otherResult}); err == nil {
			t.Fatal("duplicate physical ownership accepted")
		}
		_ = receipt(t, f, 9002, 111)
		if len(f.storage.Deletes()) != 0 {
			t.Fatal("identity rejection called delete")
		}
	})
}
func TestB2ReceiptBudget(t *testing.T) {
	f := newLedger(t)
	ctx := context.Background()
	id := receipt(t, f, 9001, 71)
	abandon(t, f, id)
	stage(t, f, id)
	f.storage.SetDeleteFaults(fixture.DeleteFaults{Error: errors.New("fixture delete failure")})
	for attempt := 1; attempt <= 4; attempt++ {
		if err := f.svc.Recover(ctx, 1001, []uuid.UUID{id, id}); err == nil {
			t.Fatal("failure was hidden")
		}
		row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil || row.Attempts != int32(attempt) {
			t.Fatalf("attempt %d: %#v %v", attempt, row, err)
		}
		if attempt < 4 {
			if _, err = f.pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp() WHERE id=$1", id); err != nil {
				t.Fatal(err)
			}
		}
	}
	fresh := lifecycle.NewService(f.pool, f.storage)
	if err := fresh.BindReceiptEnqueuer(f.queue); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Recover(ctx, 1001, []uuid.UUID{id}); !errors.Is(err, lifecycle.ErrAttemptsExhausted) {
		t.Fatalf("terminal report: %v", err)
	}
	row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
	if err != nil || row.State != "exhausted" || len(f.storage.Deletes()) != 4 {
		t.Fatalf("cap failed %#v %v", row, err)
	}
	if _, err = f.pool.Exec(ctx, "UPDATE remote_write_receipts SET attempts=5 WHERE id=$1", id); err == nil {
		t.Fatal("out-of-range attempt accepted")
	}
	if err = fresh.Recover(ctx, 1001, nil); err != nil {
		t.Fatal(err)
	}
	if len(f.storage.Deletes()) != 4 {
		t.Fatal("empty recovery called RPC")
	}
	t.Run("replacement_keeps_first_job", func(t *testing.T) {
		id := receipt(t, f, 9001, 72)
		abandon(t, f, id)
		stage(t, f, id)
		before, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.pool.Exec(ctx, "DELETE FROM b2_receipt_jobs WHERE id=$1", before.WakeupJobID.Int64); err != nil {
			t.Fatal(err)
		}
		if _, err = f.pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp() WHERE id=$1", id); err != nil {
			t.Fatal(err)
		}
		if err = fresh.DiscoverRecoveryCandidate(ctx, id, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		after, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil || before.JobID != after.JobID || before.WakeupJobID == after.WakeupJobID {
			t.Fatalf("replacement IDs %#v %#v %v", before, after, err)
		}
	})
	t.Run("expiry_equality_and_late_receipt", func(t *testing.T) {
		f := newLedger(t)
		id := receipt(t, f, 9001, 0)
		op := copyOrigin(t, f, id)
		var cutoff time.Time
		if err := f.pool.QueryRow(ctx, "UPDATE remote_write_receipts SET lease_expires_at=clock_timestamp() WHERE id=$1 RETURNING lease_expires_at", op).Scan(&cutoff); err != nil {
			t.Fatal(err)
		}
		if err := f.svc.DiscoverRecoveryCandidate(ctx, id, cutoff); err != nil {
			t.Fatal(err)
		}
		row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil || row.State != "unresolved" {
			t.Fatalf("expiry did not fence: %#v %v", row, err)
		}
		token, result := uuid.UUID(row.LeaseToken.Bytes), uuid.UUID(row.ResultFileID.Bytes)
		if _, err = sqlcgen.New(f.pool).RenewCopyOperation(ctx, sqlcgen.RenewCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(op), LeaseToken: row.LeaseToken, LeaseExpiresAt: dbtypes.Time(time.Now().UTC().Add(time.Hour))}); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("expired operation renewed: %v", err)
		}
		if _, err = f.svc.Observe(ctx, lifecycle.ObserveInput{ReceiptID: id, UserID: 1001, ChannelID: 9001, MessageID: 121, LeaseToken: token, OperationID: &op, ResultFileID: &result}); err != nil {
			t.Fatal(err)
		}
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err = f.svc.BindTx(ctx, tx, lifecycle.BindInput{UserID: 1001, ReceiptIDs: []uuid.UUID{id}, ResultFileID: &result}); !errors.Is(err, lifecycle.ErrDeletionFenced) {
			t.Fatalf("late receipt revived publisher: %v", err)
		}
		_ = tx.Rollback(ctx)
		late, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil || late.JobID != row.JobID || late.State != "cleanup_pending" {
			t.Fatalf("late receipt lost stable report: %#v %v", late, err)
		}
	})
	t.Run("unavailable_database", func(t *testing.T) {
		f := newLedger(t)
		id := receipt(t, f, 9001, 122)
		abandon(t, f, id)
		stage(t, f, id)
		f.pool.Close()
		if err := f.svc.Recover(ctx, 1001, []uuid.UUID{id}); err == nil {
			t.Fatal("unavailable claim was treated as committed")
		}
		if len(f.storage.Deletes()) != 0 {
			t.Fatal("unknown SQL outcome authorized deletion")
		}
	})
	t.Run("ack_loss_consumes_lifetime_budget", func(t *testing.T) {
		f := newLedger(t)
		id := receipt(t, f, 9001, 123)
		abandon(t, f, id)
		stage(t, f, id)
		if err := f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: 9001, MessageID: 123, Bytes: []byte("owned ack-loss bytes")}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(ctx, "ALTER TABLE remote_write_receipts ADD CONSTRAINT b2_reject_ack CHECK (state <> 'deleted')"); err != nil {
			t.Fatal(err)
		}
		for attempt := 1; attempt <= 4; attempt++ {
			if err := f.svc.Recover(ctx, 1001, []uuid.UUID{id}); err == nil {
				t.Fatal("ACK persistence failure hidden")
			}
			if _, ok := f.storage.Bytes(9001, 123); ok {
				t.Fatal("positive remote deletion not observed")
			}
			row, err := sqlcgen.New(f.pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
			if err != nil || row.Attempts != int32(attempt) || row.State != "deleting" {
				t.Fatalf("ACK loss reset claim: %#v %v", row, err)
			}
			if _, err = f.pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp() WHERE id=$1", id); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.svc.Recover(ctx, 1001, []uuid.UUID{id}); !errors.Is(err, lifecycle.ErrAttemptsExhausted) || len(f.storage.Deletes()) != 4 {
			t.Fatalf("fifth ACK-loss invocation: %v", err)
		}
	})
	t.Run("stable_physical_batch", func(t *testing.T) {
		f := newLedger(t)
		ids := []uuid.UUID{receipt(t, f, 9002, 2), receipt(t, f, 9001, 3), receipt(t, f, 9001, 2)}
		for _, id := range ids {
			abandon(t, f, id)
		}
		for _, pair := range []lifecycle.PhysicalMessage{{ChannelID: 9002, MessageID: 2}, {ChannelID: 9001, MessageID: 3}, {ChannelID: 9001, MessageID: 2}} {
			if err := f.storage.Seed(fixture.Message{UserID: 1001, ChannelID: pair.ChannelID, MessageID: pair.MessageID, Bytes: []byte("owned")}); err != nil {
				t.Fatal(err)
			}
		}
		stage(t, f, ids...)
		if err := f.svc.Recover(ctx, 1001, append(ids, ids[0], ids[1])); err != nil {
			t.Fatal(err)
		}
		calls := f.storage.Deletes()
		if len(calls) != 2 || calls[0].ChannelID != 9001 || calls[1].ChannelID != 9002 || !slices.Equal(calls[0].MessageIDs, []int64{2, 3}) || !slices.Equal(calls[1].MessageIDs, []int64{2}) {
			t.Fatalf("unstable exact physical groups: %#v", calls)
		}
	})
}
func TestB2RequestAdmission(t *testing.T) {
	for _, name := range []string{"upload_lost_create_ack", "upload_terminal_retention", "copy_terminal_replay", "copy_success_replay", "fingerprint_mismatch", "scope_authorization", "concurrent_restart"} {
		t.Run(name, func(t *testing.T) {
			f := newLedger(t)
			ctx := context.Background()
			actor := int64(1001)
			in := lifecycle.RequestAdmissionInput{Scope: lifecycle.RequestScope{ActorUserID: &actor}, RequestKey: uuid.New(), RequestFingerprint: sha256.Sum256([]byte("canonical immutable intent"))}
			kind := "upload"
			if name == "copy_terminal_replay" || name == "copy_success_replay" {
				kind = "copy"
			}
			selected, result, token := uuid.New(), uuid.New(), uuid.New()
			var upload, operation *uuid.UUID
			if kind == "upload" {
				upload = &selected
			} else {
				operation = &selected
			}
			bind := func(svc *lifecycle.Service, tx pgx.Tx, target uuid.UUID) (*sqlcgen.RequestAdmission, bool, error) {
				u, o := upload, operation
				if u != nil {
					u = &target
				} else {
					o = &target
				}
				return svc.BindRequestAdmissionTx(ctx, tx, 1001, kind, in, u, o)
			}
			if name == "concurrent_restart" {
				start := make(chan struct{})
				outcomes := make(chan *sqlcgen.RequestAdmission, 2)
				errs := make(chan error, 2)
				var wg sync.WaitGroup
				for range 2 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						tx, err := f.pool.Begin(ctx)
						if err != nil {
							errs <- err
							return
						}
						defer tx.Rollback(ctx)
						row, _, err := bind(lifecycle.NewService(f.pool, f.storage), tx, uuid.New())
						if err == nil {
							err = tx.Commit(ctx)
						}
						if err != nil {
							errs <- err
							return
						}
						outcomes <- row
					}()
				}
				close(start)
				wg.Wait()
				close(errs)
				close(outcomes)
				for err := range errs {
					t.Fatal(err)
				}
				var first *sqlcgen.RequestAdmission
				count := 0
				for row := range outcomes {
					if first == nil {
						first = row
					} else if row.ID != first.ID || row.UploadID != first.UploadID {
						t.Fatal("concurrent first bind allocated twice")
					}
					count++
				}
				if count != 2 {
					t.Fatal("missing concurrent result")
				}
				fresh, err := lifecycle.NewService(f.pool, f.storage).GetRequestAdmission(ctx, in.Scope, kind, in.RequestKey)
				if err != nil || fresh.ID != first.ID {
					t.Fatalf("restart lost identity: %#v %v", fresh, err)
				}
				return
			}
			tx, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			first, created, err := bind(f.svc, tx, selected)
			if err != nil || !created {
				t.Fatalf("initial bind: %#v %v", first, err)
			}
			if kind == "upload" {
				_, err = tx.Exec(ctx, "INSERT INTO upload_sessions(id,user_id,name,expected_size,mod_time,part_size,expires_at) VALUES ($1,1001,'admitted',0,now(),1,now()+interval '1 hour')", selected)
			} else {
				_, _, err = f.svc.ReserveCopyTx(ctx, tx, lifecycle.ReserveCopyInput{UserID: 1001, OperationID: selected, RootResultFileID: result, LeaseToken: token, DestinationIsRoot: true, LeaseExpiresAt: time.Now().UTC().Add(time.Hour)})
			}
			if err != nil {
				t.Fatal(err)
			}
			if name == "upload_lost_create_ack" {
				lost := fixture.NewBarrier(t, fixture.ErrProcessLost)
				go func() {
					if err := lost.Wait(ctx); err == nil {
						lost.Release()
					}
				}()
				if err = (fixture.Hooks{ImportQCommittedBeforePreflight: lost}).CommitImportQ(ctx, tx); !errors.Is(err, fixture.ErrProcessLost) {
					t.Fatalf("expected real committed-returned-error: %v", err)
				}
			} else if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "upload_terminal_retention":
				if _, err = f.pool.Exec(ctx, "UPDATE upload_sessions SET state='aborted',terminal_failure_code='upload_parent_unavailable' WHERE id=$1", selected); err != nil {
					t.Fatal(err)
				}
				if _, err = f.pool.Exec(ctx, "DELETE FROM upload_sessions WHERE id=$1", selected); err == nil {
					t.Fatal("terminal evidence was erased by retention")
				}
			case "copy_terminal_replay":
				if _, err = sqlcgen.New(f.pool).TerminalizeCopyOperation(ctx, sqlcgen.TerminalizeCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(selected), FailureCode: "copy_destination_unavailable"}); err != nil {
					t.Fatal(err)
				}
			case "copy_success_replay":
				transaction(t, f.pool, func(tx pgx.Tx) error {
					if _, err := tx.Exec(ctx, "INSERT INTO files(id,user_id,name,kind,mod_time) VALUES ($1,1001,'empty-copy','folder',now())", result); err != nil {
						return err
					}
					n, err := sqlcgen.New(tx).PublishCopyOperation(ctx, sqlcgen.PublishCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(selected), LeaseToken: dbtypes.UUID(token)})
					if err == nil && n != 1 {
						return errors.New("empty copy did not publish")
					}
					return err
				})
				if _, err = f.pool.Exec(ctx, "DELETE FROM files WHERE id=$1", result); err != nil {
					t.Fatal(err)
				}
			}
			before := admissionSnapshot(t, f.pool)
			fresh := lifecycle.NewService(f.pool, f.storage)
			if name == "fingerprint_mismatch" {
				in.RequestFingerprint[0] ^= 1
				tx, err := f.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				if _, _, err = bind(fresh, tx, uuid.New()); !errors.Is(err, lifecycle.ErrRequestMismatch) {
					t.Fatalf("mismatch: %v", err)
				}
				_ = tx.Rollback(ctx)
			} else if name == "scope_authorization" {
				for _, scope := range []lifecycle.RequestScope{{ActorUserID: new(int64(1002))}, {PublicShareID: new(uuid.New())}} {
					row, err := fresh.GetRequestAdmission(ctx, scope, kind, in.RequestKey)
					if !errors.Is(err, pgx.ErrNoRows) || row != nil {
						t.Fatalf("foreign binding: %#v %v", row, err)
					}
				}
				if row, err := fresh.GetRequestAdmission(ctx, in.Scope, "copy", in.RequestKey); !errors.Is(err, pgx.ErrNoRows) || row != nil {
					t.Fatalf("wrong-kind binding: %#v %v", row, err)
				}
				tx, err := f.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				if _, _, err = fresh.BindRequestAdmissionTx(ctx, tx, 1002, kind, in, upload, operation); !errors.Is(err, lifecycle.ErrRequestMismatch) {
					t.Fatalf("owner mismatch: %v", err)
				}
				_ = tx.Rollback(ctx)
			} else {
				transaction(t, f.pool, func(tx pgx.Tx) error {
					row, created, err := bind(fresh, tx, uuid.New())
					if err == nil && (created || row.ID != first.ID || row.UploadID != first.UploadID || row.CopyOperationID != first.CopyOperationID) {
						return errors.New("replay allocated another target")
					}
					return err
				})
				// Missing target is not absence of admission.
				if name == "upload_lost_create_ack" {
					if _, err = f.pool.Exec(ctx, "DELETE FROM upload_sessions WHERE id=$1", selected); err != nil {
						t.Fatal(err)
					}
					transaction(t, f.pool, func(tx pgx.Tx) error {
						row, created, err := bind(fresh, tx, uuid.New())
						if err == nil && (created || row.UploadID != first.UploadID) {
							return errors.New("missing target freed admission")
						}
						return err
					})
					closed, err := f.pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					_ = closed.Rollback(ctx)
					if _, _, err = bind(fresh, closed, uuid.New()); err == nil {
						t.Fatal("unavailable transaction allocated a replacement")
					}
				}
			}
			if after := admissionSnapshot(t, f.pool); !bytes.Equal(before, after) {
				t.Fatalf("admission or events changed: %s / %s", before, after)
			}
		})
	}
}
func admissionSnapshot(t *testing.T, pool *pgxpool.Pool) []byte {
	t.Helper()
	var b []byte
	if err := pool.QueryRow(context.Background(), "SELECT jsonb_build_object('admissions',(SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM request_admissions a),'events',(SELECT count(*) FROM user_events))::text").Scan(&b); err != nil {
		t.Fatal(err)
	}
	return b
}
