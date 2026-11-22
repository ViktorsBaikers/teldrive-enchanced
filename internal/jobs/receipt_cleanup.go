package jobs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
)

const (
	ReceiptCleanupKind        = "receipt_cleanup"
	ReceiptDiscoverySweepKind = "receipt_cleanup_sweep"
)

var errReceiptCleanupPending = errors.New("receipt cleanup remains pending")

type ReceiptCleanupArgs struct {
	UserID     int64       `json:"user_id"`
	ReceiptIDs []uuid.UUID `json:"receipt_ids"`
}

func (ReceiptCleanupArgs) Kind() string { return ReceiptCleanupKind }
func (ReceiptCleanupArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: CleanupQueue, Priority: 2, MaxAttempts: 3}
}
func validReceiptCleanupInput(owner int64, ids []uuid.UUID) bool {
	if owner <= 0 || len(ids) == 0 || len(ids) > 1000 {
		return false
	}
	seen := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		if id == uuid.Nil {
			return false
		}
		if _, ok := seen[id]; ok {
			return false
		}
		seen[id] = struct{}{}
	}
	return true
}

type ReceiptCleanupWorker struct {
	river.WorkerDefaults[ReceiptCleanupArgs]
	pool   *pgxpool.Pool
	policy *lifecycle.Service
}

func NewReceiptCleanupWorker(pool *pgxpool.Pool, policy *lifecycle.Service) *ReceiptCleanupWorker {
	return &ReceiptCleanupWorker{pool: pool, policy: policy}
}
func (*ReceiptCleanupWorker) Timeout(*river.Job[ReceiptCleanupArgs]) time.Duration {
	return 5 * time.Second
}

type receiptCleanupOutput struct {
	Receipts    int    `json:"receipts"`
	Completed   int    `json:"completed"`
	Pending     int    `json:"pending"`
	Unresolved  int    `json:"unresolved"`
	Exhausted   int    `json:"exhausted"`
	PrimaryCode string `json:"primaryCode,omitempty"`
	CleanupCode string `json:"cleanupCode,omitempty"`
}

func (w *ReceiptCleanupWorker) Work(ctx context.Context, job *river.Job[ReceiptCleanupArgs]) (workErr error) {
	if w == nil || w.pool == nil || w.policy == nil {
		return ErrRuntimeNotConfigured
	}
	if job == nil || !validReceiptCleanupInput(job.Args.UserID, job.Args.ReceiptIDs) {
		return river.JobCancel(lifecycle.ErrInvalidInput)
	}
	budget, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	pgIDs := makeReceiptIDs(job.Args.ReceiptIDs)
	q := sqlcgen.New(w.pool)
	rows, err := q.ListRemoteWriteReceipts(budget, sqlcgen.ListRemoteWriteReceiptsParams{UserID: job.Args.UserID, ReceiptIds: pgIDs})
	if err != nil {
		return fmt.Errorf("read exact owner receipts: %w", err)
	}
	if len(rows) != len(job.Args.ReceiptIDs) {
		return river.JobCancel(lifecycle.ErrReceiptConflict)
	}
	workErr = w.policy.RecoverWithinBudget(budget, job.Args.UserID, job.Args.ReceiptIDs)
	rows, err = q.ListRemoteWriteReceipts(budget, sqlcgen.ListRemoteWriteReceiptsParams{UserID: job.Args.UserID, ReceiptIds: pgIDs})
	if err != nil {
		return errors.Join(workErr, fmt.Errorf("read receipt recovery outcome: %w", err))
	}
	output := receiptCleanupOutput{Receipts: len(rows)}
	for _, row := range rows {
		if output.PrimaryCode == "" {
			if row.PrimaryCode.Valid {
				output.PrimaryCode = row.PrimaryCode.String
			} else if row.TerminalFailureCode.Valid {
				output.PrimaryCode = row.TerminalFailureCode.String
			}
		}
		if output.CleanupCode == "" && row.CleanupCode.Valid {
			output.CleanupCode = row.CleanupCode.String
		}
		switch row.State {
		case "deleted":
			output.Completed++
		case "unresolved":
			output.Unresolved++
		case "exhausted":
			output.Exhausted++
		default:
			output.Pending++
		}
	}
	if job.JobRow != nil {
		workErr = errors.Join(workErr, river.RecordOutput(budget, output))
	}
	if output.Unresolved > 0 || output.Exhausted > 0 {
		return river.JobCancel(errors.Join(workErr, ErrReceiptRetryUnavailable))
	}
	if output.Pending > 0 {
		return errors.Join(workErr, errReceiptCleanupPending)
	}
	return workErr
}

type ReceiptDiscoverySweepArgs struct{}

func (ReceiptDiscoverySweepArgs) Kind() string { return ReceiptDiscoverySweepKind }
func (ReceiptDiscoverySweepArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: CleanupQueue, Priority: 2, MaxAttempts: 3}
}

type ReceiptDiscoverySweepWorker struct {
	river.WorkerDefaults[ReceiptDiscoverySweepArgs]
	pool   *pgxpool.Pool
	policy *lifecycle.Service
}

func NewReceiptDiscoverySweepWorker(pool *pgxpool.Pool, policy *lifecycle.Service) *ReceiptDiscoverySweepWorker {
	return &ReceiptDiscoverySweepWorker{pool: pool, policy: policy}
}
func (*ReceiptDiscoverySweepWorker) Timeout(*river.Job[ReceiptDiscoverySweepArgs]) time.Duration {
	return 2 * time.Hour
}
func (w *ReceiptDiscoverySweepWorker) Work(ctx context.Context, job *river.Job[ReceiptDiscoverySweepArgs]) (workErr error) {
	if w == nil || w.pool == nil || w.policy == nil {
		return ErrRuntimeNotConfigured
	}
	output := struct {
		Cutoff   time.Time `json:"cutoff"`
		Pages    int       `json:"pages"`
		Selected int       `json:"selected"`
		Failed   int       `json:"failed"`
	}{}
	defer func() {
		workErr = errors.Join(workErr, recordCleanupOutput(ctx, job != nil && job.JobRow != nil, output))
	}()
	cutoff, err := sqlcgen.New(w.pool).LifecycleDatabaseTime(ctx)
	if err != nil {
		return fmt.Errorf("capture receipt sweep cutoff: %w", err)
	}
	output.Cutoff = cutoff.Time.UTC()
	input := lifecycle.RecoveryCandidatesInput{PassCutoff: output.Cutoff, PageSize: 1000}
	var failure error
	for {
		if err = ctx.Err(); err != nil {
			return errors.Join(failure, err)
		}
		rows, e := w.policy.ListLifecycleRecoveryCandidates(ctx, input)
		if e != nil {
			return errors.Join(failure, fmt.Errorf("list receipt recovery candidates: %w", e))
		}
		output.Pages++
		output.Selected += len(rows)
		for _, row := range rows {
			if e = w.policy.DiscoverRecoveryCandidate(ctx, uuid.UUID(row.ID.Bytes), output.Cutoff); e != nil {
				output.Failed++
				if failure == nil {
					failure = e
				}
			}
			if e = ctx.Err(); e != nil {
				return errors.Join(failure, e)
			}
		}
		if len(rows) < 1000 {
			return failure
		}
		last := rows[len(rows)-1]
		afterTime, afterID := last.CreatedAt.Time.UTC(), uuid.UUID(last.ID.Bytes)
		if input.AfterCreatedAt != nil && (afterTime.Before(*input.AfterCreatedAt) || (afterTime.Equal(*input.AfterCreatedAt) && bytes.Compare(afterID[:], input.AfterID[:]) <= 0)) {
			return errors.Join(failure, errors.New("receipt discovery cursor did not advance"))
		}
		input.AfterCreatedAt, input.AfterID = &afterTime, &afterID
	}
}

var _ lifecycle.ReceiptEnqueuer = (*Runtime)(nil)

func makeReceiptIDs(ids []uuid.UUID) []pgtype.UUID {
	result := make([]pgtype.UUID, len(ids))
	for i, id := range ids {
		result[i] = dbtypes.UUID(id)
	}
	return result
}
