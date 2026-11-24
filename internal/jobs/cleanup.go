package jobs

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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
	UploadCleanupSweepKind = "teldrive_cleanup_uploads"
	CleanupQueue           = "maintenance"
)

var ErrUploadCleanupNotConfigured = errors.New("upload cleanup worker is not configured")
var ErrUploadCleanupIncomplete = errors.New("upload cleanup retained incomplete work")
var errUploadCleanupQuery = errors.New("upload cleanup query failed")

// Deprecated compatibility names; new code should use the explicit upload-cleanup names.
const CleanupSweepKind = UploadCleanupSweepKind

var ErrCleanupNotConfigured = ErrUploadCleanupNotConfigured

type UploadCleanupSweepArgs struct{}

type CleanupSweepArgs = UploadCleanupSweepArgs

func (UploadCleanupSweepArgs) Kind() string { return UploadCleanupSweepKind }

func (UploadCleanupSweepArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       CleanupQueue,
		MaxAttempts: 3,
		Priority:    2,
	}
}

type UploadCleanupWorker struct {
	river.WorkerDefaults[UploadCleanupSweepArgs]
	pool    *pgxpool.Pool
	queries *sqlcgen.Queries
	policy  *lifecycle.Service
}

type cleanupChannel struct {
	userID    int64
	channelID int64
}

type cleanupPartRecord struct {
	UserID    int64     `json:"user_id"`
	UploadID  uuid.UUID `json:"upload_id"`
	PartNo    int32     `json:"part_no"`
	MessageID int64     `json:"message_id"`
	ChannelID int64     `json:"channel_id"`
}

func NewUploadCleanupWorker(pool *pgxpool.Pool, policy *lifecycle.Service) *UploadCleanupWorker {
	return &UploadCleanupWorker{pool: pool, queries: sqlcgen.New(pool), policy: policy}
}

type UploadCleanupOutput struct {
	lifecycle.CleanupMessageResult
	Cutoff          time.Time `json:"cutoff"`
	CompletedAt     time.Time `json:"completedAt"`
	Pages           int       `json:"pages"`
	ExpiryBatches   int       `json:"expiryBatches"`
	Sessions        int       `json:"sessions"`
	UnknownSessions int       `json:"unknownSessions"`
	RemovedParts    int64     `json:"removedParts"`
	FailedGroups    int       `json:"failedGroups"`
	Incomplete      bool      `json:"incomplete"`
}

func addCleanupCounts(total *lifecycle.CleanupMessageResult, part lifecycle.CleanupMessageResult) {
	total.Deleted += part.Deleted
	total.Acknowledged += part.Acknowledged
	total.Unknown += part.Unknown
	total.Foreign += part.Foreign
	total.Protected += part.Protected
	total.Pending += part.Pending
	total.Exhausted += part.Exhausted
	total.Failed += part.Failed
}

func recordCleanupOutput(ctx context.Context, present bool, output any) error {
	if !present {
		return nil
	}
	report, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := river.RecordOutput(report, output); err != nil {
		return fmt.Errorf("record cleanup output: %w", err)
	}
	return nil
}

func (w *UploadCleanupWorker) Timeout(*river.Job[UploadCleanupSweepArgs]) time.Duration {
	return 2 * time.Hour
}

func (w *UploadCleanupWorker) Work(ctx context.Context, job *river.Job[UploadCleanupSweepArgs]) (workErr error) {
	output := UploadCleanupOutput{}
	defer func() {
		output.CompletedAt = time.Now().UTC()
		output.Incomplete = workErr != nil
		workErr = errors.Join(workErr, recordCleanupOutput(ctx, job != nil && job.JobRow != nil, output))
	}()
	if w.pool == nil || w.policy == nil {
		return ErrUploadCleanupNotConfigured
	}
	cutoff, err := w.queries.GetCleanupSweepCutoff(ctx)
	if err != nil {
		return fmt.Errorf("read upload cleanup cutoff: %w", err)
	}
	if !cutoff.Valid {
		return errors.New("invalid upload cleanup cutoff")
	}
	output.Cutoff = cutoff.Time
	// Expiry takes session locks only; it never enters owner/receipt locking.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		expired, err := w.expireBatch(ctx, cutoff)
		output.ExpiryBatches++
		if err != nil {
			return fmt.Errorf("expire upload sessions: %w", err)
		}
		if len(expired) < 1000 {
			break
		}
	}
	params := sqlcgen.ListUploadSessionsPendingCleanupParams{PassCutoff: cutoff}
	var failures []error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(errors.Join(failures...), err)
		}
		sessions, err := w.queries.ListUploadSessionsPendingCleanup(ctx, params)
		output.Pages++
		if err != nil {
			return errors.Join(errors.Join(failures...), fmt.Errorf("list upload cleanup sessions: %w", err))
		}
		if len(sessions) == 0 {
			break
		}
		last := sessions[len(sessions)-1]
		if !last.CreatedAt.Valid || !last.ID.Valid || (params.AfterCreatedAt.Valid &&
			(last.CreatedAt.Time.Before(params.AfterCreatedAt.Time) ||
				(last.CreatedAt.Time.Equal(params.AfterCreatedAt.Time) && bytes.Compare(last.ID.Bytes[:], params.AfterID.Bytes[:]) <= 0))) {
			return errors.Join(errors.Join(failures...), errors.New("upload cleanup cursor did not advance"))
		}
		// Advance from the selected row even when every target is retained.
		params.AfterCreatedAt, params.AfterID = last.CreatedAt, last.ID
		output.Sessions += len(sessions)
		if err := w.cleanupUploads(ctx, sessions, &output); err != nil {
			failures = append(failures, err)
			if errors.Is(err, errUploadCleanupQuery) || ctx.Err() != nil {
				return errors.Join(failures...)
			}
		}
		if len(sessions) < 1000 {
			break
		}
	}
	if output.UnknownSessions+output.Unknown+output.Foreign+output.Protected+output.Pending+output.Exhausted+output.Failed > 0 {
		failures = append(failures, ErrUploadCleanupIncomplete)
	}
	return errors.Join(failures...)
}

// expireBatch expires one bounded page of sessions in its own transaction. The
// sweep emits one upload.expired event per row, so it uses the same
// transaction-local bulk lane as catalog subtree work: the row cap is lifted,
// the gate wait and absolute budget are not. SET LOCAL ends with the
// transaction, so the pool never inherits it.
func (w *UploadCleanupWorker) expireBatch(ctx context.Context, cutoff pgtype.Timestamptz) ([]*sqlcgen.UploadSession, error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL teldrive.event_bulk = 'on'"); err != nil {
		return nil, err
	}
	expired, err := w.queries.WithTx(tx).ExpireUploadSessions(ctx, cutoff)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return expired, nil
}

func (w *UploadCleanupWorker) cleanupUploads(ctx context.Context, sessions []*sqlcgen.UploadSession, output *UploadCleanupOutput) error {
	userByUpload := make(map[uuid.UUID]int64, len(sessions))
	uploadIDs := make([]pgtype.UUID, 0, len(sessions))
	for _, session := range sessions {
		if session.TerminalFailureCode.Valid && session.TerminalFailureCode.String == "upload_destination_unknown" {
			output.UnknownSessions++
			continue
		}
		id, ok := dbtypes.GoogleUUID(session.ID)
		if !ok {
			return errors.New("cleanup session has invalid upload id")
		}
		userByUpload[id] = session.UserID
		uploadIDs = append(uploadIDs, session.ID)
	}
	if len(uploadIDs) == 0 {
		return nil
	}
	parts, err := w.queries.ListUploadPartsForCleanupMany(ctx, uploadIDs)
	if err != nil {
		return errors.Join(errUploadCleanupQuery, fmt.Errorf("list upload cleanup parts: %w", err))
	}
	byChannel := make(map[cleanupChannel][]*sqlcgen.UploadPart)
	for _, part := range parts {
		if !part.MessageID.Valid || part.MessageID.Int64 <= 0 {
			output.Failed++
			continue
		}
		id, ok := dbtypes.GoogleUUID(part.UploadID)
		if !ok {
			return errors.New("cleanup part has invalid upload id")
		}
		owner, ok := userByUpload[id]
		if !ok {
			return errors.New("cleanup part has no selected owner")
		}
		key := cleanupChannel{userID: owner, channelID: part.ChannelID}
		byChannel[key] = append(byChannel[key], part)
	}
	channels := make([]cleanupChannel, 0, len(byChannel))
	for channel := range byChannel {
		channels = append(channels, channel)
	}
	slices.SortFunc(channels, func(a, b cleanupChannel) int {
		return cmp.Or(cmp.Compare(a.userID, b.userID), cmp.Compare(a.channelID, b.channelID))
	})
	var failures []error
	for _, channel := range channels {
		channelParts := byChannel[channel]
		for start := 0; start < len(channelParts); start += 1000 {
			if err := ctx.Err(); err != nil {
				return errors.Join(errors.Join(failures...), err)
			}
			group := channelParts[start:min(start+1000, len(channelParts))]
			messages := make([]int64, len(group))
			for i, p := range group {
				messages[i] = p.MessageID.Int64
			}
			budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
			result, cleanupErr := w.policy.CleanupMessagesWithinBudget(budget, channel.userID, channel.channelID, messages)
			addCleanupCounts(&output.CleanupMessageResult, result)
			acknowledged := make(map[int64]bool, len(result.DeletedMessageIDs))
			for _, id := range result.DeletedMessageIDs {
				acknowledged[id] = true
			}
			records := make([]cleanupPartRecord, 0, len(acknowledged))
			originIDs := make([]uuid.UUID, 0, len(acknowledged))
			for _, p := range group {
				if !acknowledged[p.MessageID.Int64] {
					continue
				}
				id := uuid.UUID(p.UploadID.Bytes)
				originIDs = append(originIDs, id)
				records = append(records, cleanupPartRecord{UserID: channel.userID, UploadID: id, PartNo: p.PartNo, ChannelID: p.ChannelID, MessageID: p.MessageID.Int64})
			}
			if len(records) > 0 {
				encoded, e := json.Marshal(records)
				if e == nil {
					tx, beginErr := w.pool.Begin(budget)
					e = beginErr
					if e == nil {
						e = lifecycle.LockOwnerTx(budget, tx, channel.userID)
						if e == nil {
							e = lifecycle.LockHierarchyTx(budget, tx, channel.userID, nil, originIDs)
						}
						if e == nil {
							var deleted int64
							deleted, e = sqlcgen.New(tx).DeleteUploadPartsForCleanup(budget, encoded)
							if e == nil && deleted != int64(len(records)) {
								e = errors.New("upload cleanup target changed before exact acknowledgement")
							}
							if e == nil {
								e = tx.Commit(budget)
								if e == nil {
									output.RemovedParts += deleted
								}
							}
						}
						_ = tx.Rollback(budget)
					}
				}
				cleanupErr = errors.Join(cleanupErr, e)
			}
			cancel()
			if cleanupErr != nil {
				output.FailedGroups++
				failures = append(failures, fmt.Errorf("clean exact upload targets: %w", cleanupErr))
			}
		}
	}
	return errors.Join(failures...)
}
