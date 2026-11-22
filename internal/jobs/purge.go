package jobs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
)

const (
	PurgeSweepKind = "teldrive_purge_pending_files"
	PurgeQueue     = CleanupQueue
)

var ErrPurgeNotConfigured = errors.New("pending-file purge worker is not configured")

// PurgeSweepOutput bounds diagnostics independently of the number of failed owners.
type PurgeSweepOutput struct {
	Cutoff        time.Time `json:"cutoff"`
	Pages         int       `json:"pages"`
	Roots         int       `json:"roots"`
	OwnerBatches  int       `json:"owner_batches"`
	FailedBatches int       `json:"failed_batches"`
	Incomplete    bool      `json:"incomplete"`
}

type purgeSweepFailure struct {
	count int
	first error
	last  error
}

func (f *purgeSweepFailure) add(err error) {
	if err != nil {
		f.count++
		if f.first == nil {
			f.first = err
		}
		f.last = err
	}
}

func (f *purgeSweepFailure) err() error {
	if f.first == nil {
		return nil
	}
	if f.count == 1 {
		return fmt.Errorf("purge sweep incomplete: failed_batches=1: %w", f.first)
	}
	return fmt.Errorf("purge sweep incomplete: failed_batches=%d: %w", f.count, errors.Join(f.first, f.last))
}

func validPurgeCursor(created pgtype.Timestamptz, id pgtype.UUID, cutoff pgtype.Timestamptz, afterCreated pgtype.Timestamptz, afterID pgtype.UUID) bool {
	if !created.Valid || !id.Valid || id.Bytes == [16]byte{} || created.Time.After(cutoff.Time) {
		return false
	}
	return !afterCreated.Valid || created.Time.After(afterCreated.Time) ||
		(created.Time.Equal(afterCreated.Time) && bytes.Compare(id.Bytes[:], afterID.Bytes[:]) > 0)
}

type PurgeService interface {
	Purge(context.Context, int64, uuid.UUID) error
	PurgeMany(context.Context, int64, []uuid.UUID) error
}

type PurgeSweepArgs struct{}

func (PurgeSweepArgs) Kind() string { return PurgeSweepKind }

func (PurgeSweepArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: PurgeQueue, MaxAttempts: 3, Priority: 1}
}

type PendingFilePurgeWorker struct {
	river.WorkerDefaults[PurgeSweepArgs]
	pool    *pgxpool.Pool
	queries *sqlcgen.Queries
	service PurgeService
}

func NewPendingFilePurgeWorker(pool *pgxpool.Pool, service PurgeService) *PendingFilePurgeWorker {
	return &PendingFilePurgeWorker{pool: pool, queries: sqlcgen.New(pool), service: service}
}

func (w *PendingFilePurgeWorker) Timeout(*river.Job[PurgeSweepArgs]) time.Duration {
	return 2 * time.Hour
}

func (w *PendingFilePurgeWorker) Work(ctx context.Context, job *river.Job[PurgeSweepArgs]) (workErr error) {
	output := PurgeSweepOutput{}
	defer func() {
		output.Incomplete = workErr != nil
		workErr = errors.Join(workErr, recordCleanupOutput(ctx, job != nil && job.JobRow != nil, output))
	}()
	if w == nil || w.pool == nil || w.service == nil {
		return ErrPurgeNotConfigured
	}
	cutoff, err := w.queries.GetCleanupSweepCutoff(ctx)
	if err != nil {
		return fmt.Errorf("read pending purge cutoff: %w", err)
	}
	if !cutoff.Valid {
		return errors.New("invalid pending purge cutoff")
	}
	output.Cutoff = cutoff.Time
	params := sqlcgen.ListDeletionPendingRootsParams{PassCutoff: cutoff}
	var failures purgeSweepFailure
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(failures.err(), err)
		}
		rows, err := w.queries.ListDeletionPendingRoots(ctx, params)
		output.Pages++
		if err != nil {
			return errors.Join(failures.err(), fmt.Errorf("list deletion-pending roots: %w", err))
		}
		if len(rows) == 0 {
			return failures.err()
		}
		byUser := make(map[int64][]uuid.UUID)
		created, id := params.AfterCreatedAt, params.AfterID
		for _, item := range rows {
			if item.UserID <= 0 || !validPurgeCursor(item.CreatedAt, item.FileID, cutoff, created, id) {
				return errors.Join(failures.err(), errors.New("invalid deletion-pending root cursor"))
			}
			created, id = item.CreatedAt, item.FileID
			byUser[item.UserID] = append(byUser[item.UserID], uuid.UUID(item.FileID.Bytes))
		}
		output.Roots += len(rows)
		for _, userID := range slices.Sorted(maps.Keys(byUser)) {
			if err := ctx.Err(); err != nil {
				return errors.Join(failures.err(), err)
			}
			output.OwnerBatches++
			if err := w.service.PurgeMany(ctx, userID, byUser[userID]); err != nil {
				output.FailedBatches++
				failures.add(err)
			}
		}
		params.AfterCreatedAt, params.AfterID = created, id
		if len(rows) < 1000 {
			return failures.err()
		}
	}
}
