package jobs

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
)

const TrashCleanupSweepKind = "teldrive_cleanup_trash"

var ErrTrashCleanupNotConfigured = errors.New("trash cleanup worker is not configured")

type TrashCleanupSweepArgs struct {
	Retention string `json:"retention,omitempty"`
}

func (TrashCleanupSweepArgs) Kind() string { return TrashCleanupSweepKind }

func (TrashCleanupSweepArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: CleanupQueue, MaxAttempts: 3, Priority: 1}
}

type TrashCleanupWorker struct {
	river.WorkerDefaults[TrashCleanupSweepArgs]
	pool    *pgxpool.Pool
	queries *sqlcgen.Queries
	service PurgeService
}

func NewTrashCleanupWorker(pool *pgxpool.Pool, service PurgeService) *TrashCleanupWorker {
	return &TrashCleanupWorker{pool: pool, queries: sqlcgen.New(pool), service: service}
}

func (w *TrashCleanupWorker) Timeout(*river.Job[TrashCleanupSweepArgs]) time.Duration {
	return 2 * time.Hour
}

func (w *TrashCleanupWorker) Work(ctx context.Context, job *river.Job[TrashCleanupSweepArgs]) (workErr error) {
	output := PurgeSweepOutput{}
	defer func() {
		output.Incomplete = workErr != nil
		workErr = errors.Join(workErr, recordCleanupOutput(ctx, job != nil && job.JobRow != nil, output))
	}()
	if w == nil || w.pool == nil || w.service == nil {
		return ErrTrashCleanupNotConfigured
	}
	retentionText := ""
	if job != nil {
		retentionText = job.Args.Retention
	}
	if retentionText == "" {
		retentionText = "720h"
	}
	retention, err := time.ParseDuration(retentionText)
	if err != nil || retention <= 0 {
		return fmt.Errorf("invalid trash retention %q", retentionText)
	}
	cutoff, err := w.queries.GetCleanupSweepCutoff(ctx)
	if err != nil {
		return fmt.Errorf("read trash cleanup cutoff: %w", err)
	}
	if !cutoff.Valid {
		return errors.New("invalid trash cleanup cutoff")
	}
	output.Cutoff = cutoff.Time
	params := sqlcgen.ListTrashedRootsBeforeParams{PassCutoff: cutoff, DeletedBefore: dbtypes.Time(cutoff.Time.Add(-retention))}
	var failures purgeSweepFailure
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(failures.err(), err)
		}
		rows, err := w.queries.ListTrashedRootsBefore(ctx, params)
		output.Pages++
		if err != nil {
			return errors.Join(failures.err(), fmt.Errorf("list expired trash roots: %w", err))
		}
		if len(rows) == 0 {
			return failures.err()
		}
		byUser := make(map[int64][]uuid.UUID)
		created, id := params.AfterCreatedAt, params.AfterID
		for _, item := range rows {
			if item.UserID <= 0 || !validPurgeCursor(item.CreatedAt, item.FileID, cutoff, created, id) {
				return errors.Join(failures.err(), errors.New("invalid expired trash root cursor"))
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
