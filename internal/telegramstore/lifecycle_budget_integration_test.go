//go:build integration

package telegramstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
)

type b2BudgetArgs struct {
	UserID     int64       `json:"user_id"`
	ReceiptIDs []uuid.UUID `json:"receipt_ids"`
}

func (b2BudgetArgs) Kind() string { return "b2_invocation_budget_fixture" }

type b2BudgetQueue struct{ client *river.Client[pgx.Tx] }

func (q b2BudgetQueue) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (int64, error) {
	result, err := q.client.InsertTx(ctx, tx, b2BudgetArgs{in.UserID, in.ReceiptIDs}, &river.InsertOpts{MaxAttempts: 3, Metadata: []byte(fmt.Sprintf(`{"user_id":%d}`, in.UserID))})
	if err != nil {
		return 0, err
	}
	if result == nil || result.Job == nil || result.Job.ID <= 0 {
		return 0, errors.New("missing actual River job ID")
	}
	return result.Job.ID, nil
}

func TestB2DestructiveInvocationLifetimeBudget(t *testing.T) {
	db := testpostgres.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err := db.Pool.Exec(ctx, "INSERT INTO users(user_id) VALUES (1001)"); err != nil {
		t.Fatal(err)
	}
	client, err := river.NewClient(riverpgxv5.New(db.Pool), &river.Config{SkipUnknownJobCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	queue := b2BudgetQueue{client}
	calls := 0
	storage := b2Storage(t, func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
		switch r := in.(type) {
		case *tg.ChannelsGetChannelsRequest:
			return b2Reply(out, &tg.MessagesChats{Chats: []tg.ChatClass{b2Channel(202)}})
		case *tg.ChannelsDeleteMessagesRequest:
			if len(r.ID) != 1 || r.ID[0] != 8 || r.Channel.(*tg.InputChannel).ChannelID != 202 {
				t.Fatalf("wrong physical deletion: %+v", r)
			}
			calls++
			switch calls {
			case 1:
				return errors.New("connection reset by peer")
			case 2:
				return tgerr.New(420, "FLOOD_WAIT_1")
			case 3:
				return b2Reply(out, &tg.MessagesAffectedMessages{})
			default:
				return tgerr.New(-503, "Timeout")
			}
		default:
			return fmt.Errorf("unexpected RPC %T", in)
		}
	}, nil)
	fresh := func() *lifecycle.Service {
		s := lifecycle.NewService(db.Pool, storage)
		if err := s.BindReceiptEnqueuer(queue); err != nil {
			t.Fatal(err)
		}
		return s
	}
	svc := fresh()
	op, result, token, id := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, created, err := svc.ReserveCopyTx(ctx, tx, lifecycle.ReserveCopyInput{UserID: 1001, OperationID: op, RootResultFileID: result, LeaseToken: token, DestinationIsRoot: true, LeaseExpiresAt: time.Now().UTC().Add(time.Hour)}); err != nil || !created {
		t.Fatalf("operation reservation: %v %v", created, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Reserve(ctx, lifecycle.ReserveInput{ID: id, UserID: 1001, ChannelID: 202, OriginKind: "copy", OperationID: &op, ResultFileID: &result, PartNo: 1, LeaseToken: token, LeaseExpiresAt: time.Now().UTC().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Observe(ctx, lifecycle.ObserveInput{ReceiptID: id, UserID: 1001, ChannelID: 202, MessageID: 8, LeaseToken: token, OperationID: &op, ResultFileID: &result}); err != nil {
		t.Fatal(err)
	}
	if n, err := sqlcgen.New(db.Pool).TerminalizeCopyOperation(ctx, sqlcgen.TerminalizeCopyOperationParams{UserID: 1001, OperationID: dbtypes.UUID(op), FailureCode: "remote_write_abandoned"}); err != nil || n != 1 {
		t.Fatalf("terminal origin: %d %v", n, err)
	}
	tx, err = db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := svc.StageCleanupTx(ctx, tx, lifecycle.StageCleanupInput{UserID: 1001, ReceiptIDs: []uuid.UUID{id}, PrimaryCode: "remote_write_failed"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var firstJob int64
	if err := db.Pool.QueryRow(ctx, "SELECT job_id FROM remote_write_receipts WHERE id=$1", id).Scan(&firstJob); err != nil || firstJob <= 0 {
		t.Fatalf("actual job: %d %v", firstJob, err)
	}
	for attempt := 1; attempt <= 4; attempt++ {
		svc = fresh()
		if attempt == 3 {
			// Remote ACK succeeds, but a real SQL constraint rejects its persistence.
			if _, err := db.Pool.Exec(ctx, "ALTER TABLE remote_write_receipts ADD CONSTRAINT b2_budget_ack_loss CHECK (state <> 'deleted')"); err != nil {
				t.Fatal(err)
			}
		}
		if err := svc.Recover(ctx, 1001, []uuid.UUID{id, id}); err == nil {
			t.Fatal("ambiguous/failing deletion reported success")
		}
		row, err := sqlcgen.New(db.Pool).GetRemoteWriteReceipt(ctx, dbtypes.UUID(id))
		if err != nil || row.Attempts != int32(attempt) || calls != attempt {
			t.Fatalf("reservation %d: calls=%d row=%+v err=%v", attempt, calls, row, err)
		}
		// A lost ACK leaves a live claim; a normal failed delivery may use the next lifetime attempt.
		if attempt >= 3 {
			err := fresh().Recover(ctx, 1001, []uuid.UUID{id, id})
			if attempt == 3 && err != nil {
				t.Fatal(err)
			}
			if attempt == 4 && !errors.Is(err, lifecycle.ErrAttemptsExhausted) {
				t.Fatalf("exhausted duplicate: %v", err)
			}
			if calls != attempt {
				t.Fatal("duplicate bypassed durable guard")
			}
		}
		if attempt == 3 {
			if _, err := db.Pool.Exec(ctx, "ALTER TABLE remote_write_receipts DROP CONSTRAINT b2_budget_ack_loss"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Pool.Exec(ctx, "DELETE FROM river_job WHERE id=$1", row.WakeupJobID.Int64); err != nil {
			t.Fatal(err)
		}
		if attempt < 4 {
			// Advance only scheduling time; never modify attempts or deletion state.
			if _, err := db.Pool.Exec(ctx, "UPDATE remote_write_receipts SET next_attempt_at=clock_timestamp() WHERE id=$1", id); err != nil {
				t.Fatal(err)
			}
			if err := fresh().DiscoverRecoveryCandidate(ctx, id, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
		}
	}
	svc = fresh()
	if err := svc.Recover(ctx, 1001, []uuid.UUID{id, id}); !errors.Is(err, lifecycle.ErrAttemptsExhausted) {
		t.Fatalf("fifth attempt: %v", err)
	}
	if claims, err := svc.ClaimDelete(ctx, 1001, []uuid.UUID{id}); err != nil || len(claims) != 0 {
		t.Fatalf("exhausted claim reopened: %+v %v", claims, err)
	}
	if err := svc.Recover(ctx, 1001, nil); err != nil {
		t.Fatal(err)
	}
	report, err := svc.GetReceiptJobReport(ctx, 1001, firstJob)
	if err != nil || report.JobID != firstJob || calls != 4 {
		t.Fatalf("retained budget: calls=%d report=%+v err=%v", calls, report, err)
	}
}
