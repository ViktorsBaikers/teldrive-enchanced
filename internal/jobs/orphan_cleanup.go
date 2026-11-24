package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

const OrphanCleanupKind = "teldrive_cleanup_orphaned_telegram_parts"

// maxOrphanOutputBytes bounds the recorded job output well below River's
// 32MB limit, so a badly damaged channel degrades to counters instead of
// failing the sweep.
const maxOrphanOutputBytes = 16 << 20

type OrphanCleanupArgs struct{}

func (OrphanCleanupArgs) Kind() string { return OrphanCleanupKind }
func (OrphanCleanupArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: CleanupQueue, MaxAttempts: 3, Priority: 3}
}

// OrphanCleanupOutput is recorded as the River job output when the sweep
// completes, so operators can inspect per-run counters without log access.
type OrphanCleanupOutput struct {
	Channels       int                            `json:"channels"`
	Ownership      lifecycle.CleanupMessageResult `json:"ownership"`
	Pages          int                            `json:"pages"`
	Recent         int                            `json:"recent"`
	Invalid        int                            `json:"invalid"`
	FailedChannels int                            `json:"failedChannels"`
	Incomplete     bool                           `json:"incomplete"`
	Scanned        int                            `json:"scanned"`
	Deleted        int                            `json:"deleted"`
	Cutoff         time.Time                      `json:"cutoff"`
	CompletedAt    time.Time                      `json:"completedAt"`
	// BrokenFiles lists active files with DB-referenced messages missing from
	// Telegram, so owners know what to re-upload. The list is uncapped;
	// limitBrokenFilesSize degrades to counters when it would exceed the
	// job-output size budget.
	BrokenFiles     []BrokenFile `json:"brokenFiles"`
	BrokenTotal     int          `json:"brokenTotal"`
	BrokenTruncated bool         `json:"brokenTruncated"`
}

// BrokenFile is one active file with at least one referenced part message
// absent from its Telegram channel.
type BrokenFile struct {
	FileID            string  `json:"fileId"`
	Name              string  `json:"name"`
	Size              int64   `json:"size"`
	ChannelID         int64   `json:"channelId"`
	MissingMessageIDs []int64 `json:"missingMessageIds"`
}

// findBrokenFiles returns files with referenced messages absent from the
// Telegram listing, grouped by file and sorted by name.
func findBrokenFiles(seen map[int64]struct{}, rows []*sqlcgen.ListChannelReferencedPartsRow, channelID int64) []BrokenFile {
	type pending struct {
		name string
		size int64
		ids  []int64
	}
	byFile := make(map[string]*pending)
	order := make([]string, 0)
	for _, row := range rows {
		if _, ok := seen[row.MessageID]; ok {
			continue
		}
		fileID, ok := dbtypes.GoogleUUID(row.FileID)
		if !ok {
			continue
		}
		key := fileID.String()
		entry, ok := byFile[key]
		if !ok {
			entry = &pending{name: row.FileName, size: row.FileSize.Int64}
			byFile[key] = entry
			order = append(order, key)
		}
		entry.ids = append(entry.ids, row.MessageID)
	}
	files := make([]BrokenFile, 0, len(order))
	for _, key := range order {
		entry := byFile[key]
		slices.Sort(entry.ids)
		files = append(files, BrokenFile{FileID: key, Name: entry.name, Size: entry.size, ChannelID: channelID, MissingMessageIDs: entry.ids})
	}
	slices.SortFunc(files, func(a, b BrokenFile) int {
		if result := strings.Compare(a.Name, b.Name); result != 0 {
			return result
		}
		return strings.Compare(a.FileID, b.FileID)
	})
	return files
}

// limitBrokenFilesSize drops the broken-file list, keeping counters, when
// the marshaled output would exceed maxBytes. The sweep still completes
// with full totals instead of failing on oversized job output.
func limitBrokenFilesSize(output OrphanCleanupOutput, maxBytes int) OrphanCleanupOutput {
	if len(output.BrokenFiles) == 0 {
		return output
	}
	raw, err := json.Marshal(output)
	if err != nil || len(raw) <= maxBytes {
		return output
	}
	output.BrokenFiles = []BrokenFile{}
	output.BrokenTruncated = true
	return output
}

type OrphanedTelegramPartsCleanupWorker struct {
	river.WorkerDefaults[OrphanCleanupArgs]
	queries    *sqlcgen.Queries
	lister     telegramstore.DocumentMessageLister
	policy     *lifecycle.Service
	minimumAge time.Duration
}

func NewOrphanedTelegramPartsCleanupWorker(pool *pgxpool.Pool, policy *lifecycle.Service, lister telegramstore.DocumentMessageLister, minimumAge time.Duration) *OrphanedTelegramPartsCleanupWorker {
	return &OrphanedTelegramPartsCleanupWorker{queries: sqlcgen.New(pool), policy: policy, lister: lister, minimumAge: minimumAge}
}

func (w *OrphanedTelegramPartsCleanupWorker) Timeout(*river.Job[OrphanCleanupArgs]) time.Duration {
	return 4 * time.Hour
}

func (w *OrphanedTelegramPartsCleanupWorker) Work(ctx context.Context, job *river.Job[OrphanCleanupArgs]) (workErr error) {
	output := OrphanCleanupOutput{Cutoff: time.Now().UTC().Add(-w.minimumAge), BrokenFiles: make([]BrokenFile, 0)}
	defer func() {
		output.CompletedAt = time.Now().UTC()
		output.Incomplete = workErr != nil
		// Global maintenance output has no owner scope. Keep diagnoses internal.
		global := output
		global.BrokenFiles = []BrokenFile{}
		global.BrokenTruncated = global.BrokenTotal > 0
		global = limitBrokenFilesSize(global, maxOrphanOutputBytes)
		workErr = errors.Join(workErr, recordCleanupOutput(ctx, job != nil && job.JobRow != nil, global))
	}()
	if w.policy == nil || w.lister == nil {
		return ErrUploadCleanupNotConfigured
	}
	channels, err := w.queries.ListChannelsForOrphanCleanup(ctx)
	if err != nil {
		return fmt.Errorf("list channels for orphan cleanup: %w", err)
	}
	output.Channels = len(channels)
	var failures []error
	for _, channel := range channels {
		if err := ctx.Err(); err != nil {
			return errors.Join(errors.Join(failures...), err)
		}
		seen := make(map[int64]struct{})
		beforeID := int64(0)
		complete := false
		channelFailed := false
		for {
			if err := ctx.Err(); err != nil {
				return errors.Join(errors.Join(failures...), err)
			}
			page, e := w.lister.ListDocumentMessages(ctx, telegramstore.ListDocumentMessagesRequest{
				UserID: channel.UserID, ChannelID: channel.ChannelID, BeforeID: beforeID, Limit: 100,
			})
			output.Pages++
			if e != nil {
				failures = append(failures, fmt.Errorf("list cleanup documents: %w", e))
				channelFailed = true
				break
			}
			if len(page.Messages) > 100 {
				failures = append(failures, errors.New("cleanup document page exceeded limit"))
				channelFailed = true
				break
			}
			candidates := make([]int64, 0, len(page.Messages))
			for _, message := range page.Messages {
				if message.ID <= 0 {
					output.Invalid++
					continue
				}
				if _, duplicate := seen[message.ID]; duplicate {
					continue
				}
				seen[message.ID] = struct{}{}
				output.Scanned++
				if message.CreatedAt.Before(output.Cutoff) {
					candidates = append(candidates, message.ID)
				} else {
					output.Recent++
				}
			}
			// A listing is a discovery hint, never proof of ownership.
			if len(candidates) > 0 {
				budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				result, e := w.policy.CleanupMessagesWithinBudget(budget, channel.UserID, channel.ChannelID, candidates)
				cancel()
				addCleanupCounts(&output.Ownership, result)
				output.Deleted += result.Deleted
				if e != nil {
					failures = append(failures, fmt.Errorf("clean exact orphan receipts: %w", e))
					channelFailed = true
				}
			}
			if page.Exhausted {
				complete = true
				break
			}
			if page.BeforeID <= 0 || (beforeID > 0 && page.BeforeID >= beforeID) {
				failures = append(failures, errors.New("cleanup document pagination did not advance"))
				channelFailed = true
				break
			}
			beforeID = page.BeforeID
		}
		if channelFailed {
			output.FailedChannels++
		}
		if !complete {
			continue
		}
		referenced, e := w.queries.ListChannelReferencedParts(ctx, sqlcgen.ListChannelReferencedPartsParams{TargetChannelID: channel.ChannelID, TargetUserID: channel.UserID})
		if e != nil {
			output.FailedChannels++
			return errors.Join(errors.Join(failures...), fmt.Errorf("list cleanup referenced parts: %w", e))
		}
		broken := findBrokenFiles(seen, referenced, channel.ChannelID)
		output.BrokenTotal += len(broken)
		// Preserve exact owner-scoped diagnostics until the output size limit.
		if !output.BrokenTruncated {
			output.BrokenFiles = append(output.BrokenFiles, broken...)
			output = limitBrokenFilesSize(output, maxOrphanOutputBytes)
		}
		slog.DebugContext(ctx, "orphan cleanup channel scanned", "scanned", len(seen), "broken", len(broken))
	}
	slog.InfoContext(ctx, "orphan cleanup completed", "channels", output.Channels, "scanned", output.Scanned, "deleted", output.Deleted, "broken", output.BrokenTotal, "failed_channels", output.FailedChannels)
	return errors.Join(failures...)
}
