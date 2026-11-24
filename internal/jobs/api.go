package jobs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
)

var ErrInvalidCursor = errors.New("invalid job cursor")
var ErrReceiptRetryUnavailable = errors.New("receipt cleanup retry is unavailable")

type JobError struct {
	Attempt int
	At      time.Time
	Error   string
	Trace   string
}

type Job struct {
	ID          int64
	State       string
	Attempt     int
	MaxAttempts int
	AttemptedAt *time.Time
	CreatedAt   time.Time
	FinalizedAt *time.Time
	ScheduledAt time.Time
	Priority    int
	Kind        string
	Queue       string
	Args        map[string]json.RawMessage
	Metadata    map[string]json.RawMessage
	Output      json.RawMessage
	Errors      []JobError
	AttemptedBy []string
	Tags        []string
	LastError   string
}

func (j Job) UserID() (int64, bool) {
	raw, ok := j.Args["user_id"]
	if !ok {
		raw, ok = j.Metadata["user_id"]
		if !ok {
			return 0, false
		}
	}
	var userID int64
	if err := json.Unmarshal(raw, &userID); err != nil || userID <= 0 {
		return 0, false
	}
	return userID, true
}

type ListInput struct {
	UserID int64
	Cursor string
	Limit  int32
	State  string
	Kind   string
	Queue  string
}

type Statistics struct {
	Available int64
	Cancelled int64
	Completed int64
	Discarded int64
	Pending   int64
	Retryable int64
	Running   int64
	Scheduled int64
}

func (r *Runtime) List(ctx context.Context, input ListInput) ([]Job, string, error) {
	if r == nil || r.client == nil {
		return nil, "", ErrRuntimeNotConfigured
	}
	if input.Limit <= 0 || input.Limit > 200 {
		input.Limit = 100
	}
	beforeID, err := decodeCursor(input.Cursor)
	if err != nil {
		return nil, "", err
	}

	params := river.NewJobListParams().
		OrderBy(river.JobListOrderByID, river.SortOrderDesc).
		First(int(input.Limit) + 1)
	if input.UserID > 0 {
		params = params.Where("COALESCE(args->>'user_id', metadata->>'user_id') = @user_id", river.NamedArgs{"user_id": strconv.FormatInt(input.UserID, 10)})
	}
	// Only the immutable first owner ID represents receipt work in job views.
	params = params.Where("kind <> @receipt_kind", river.NamedArgs{"receipt_kind": ReceiptCleanupKind})
	if beforeID > 0 {
		params = params.Where("id < @before_id", river.NamedArgs{"before_id": beforeID})
	}
	if input.State != "" {
		params = params.States(rivertype.JobState(input.State))
	}
	if input.Kind != "" {
		params = params.Kinds(input.Kind)
	}
	if input.Queue != "" {
		params = params.Queues(input.Queue)
	}

	result, err := r.client.JobList(ctx, params)
	if err != nil {
		return nil, "", fmt.Errorf("list jobs: %w", err)
	}
	items := make([]Job, 0, len(result.Jobs))
	for _, row := range result.Jobs {
		items = append(items, jobFromRiver(row))
	}
	if input.UserID > 0 && (input.Kind == "" || input.Kind == ReceiptCleanupKind) && (input.Queue == "" || input.Queue == CleanupQueue) {
		before := pgtype.Int8{}
		if beforeID > 0 {
			before = dbtypes.Int8(beforeID)
		}
		state := pgtype.Text{}
		if input.State != "" {
			state = dbtypes.Text(input.State)
		}
		reports, err := r.policy.ListReceiptJobReports(ctx, sqlcgen.ListReceiptJobReportsParams{
			UserID: input.UserID, BeforeJobID: before, State: state, PageSize: input.Limit + 1,
		})
		if err != nil {
			return nil, "", fmt.Errorf("list durable receipt reports: %w", err)
		}
		for _, report := range reports {
			items = append(items, jobFromReceiptReport(sqlcgen.GetReceiptJobReportRow(*report)))
		}
		slices.SortFunc(items, func(a, b Job) int {
			if a.ID > b.ID {
				return -1
			}
			if a.ID < b.ID {
				return 1
			}
			return 0
		})
	}
	var next string
	if len(items) > int(input.Limit) {
		items = items[:input.Limit]
		next = encodeCursor(items[len(items)-1].ID)
	}
	return items, next, nil
}

func (r *Runtime) GetForUser(ctx context.Context, id, userID int64) (Job, error) {
	if userID <= 0 {
		return Job{}, river.ErrNotFound
	}
	if r == nil || r.policy == nil {
		return Job{}, ErrRuntimeNotConfigured
	}
	report, err := r.policy.GetReceiptJobReport(ctx, userID, id)
	if err == nil {
		return jobFromReceiptReport(*report), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("get durable receipt report: %w", err)
	}
	item, err := r.Get(ctx, id)
	if err != nil {
		return Job{}, err
	}
	ownerID, ok := item.UserID()
	if !ok || ownerID != userID {
		return Job{}, river.ErrNotFound
	}
	return item, nil
}

func (r *Runtime) CancelForUser(ctx context.Context, id, userID int64) (Job, error) {
	if _, err := r.GetForUser(ctx, id, userID); err != nil {
		return Job{}, err
	}
	return r.Cancel(ctx, id)
}

func (r *Runtime) RetryForUser(ctx context.Context, id, userID int64) (Job, error) {
	if _, err := r.GetForUser(ctx, id, userID); err != nil {
		return Job{}, err
	}
	return r.Retry(ctx, id)
}

func (r *Runtime) DeleteForUser(ctx context.Context, id, userID int64) error {
	if _, err := r.GetForUser(ctx, id, userID); err != nil {
		return err
	}
	return r.Delete(ctx, id)
}

func (r *Runtime) StatisticsForUser(ctx context.Context, userID int64) (Statistics, error) {
	if userID <= 0 {
		return Statistics{}, ErrRuntimeNotConfigured
	}
	return r.jobStatistics(ctx, &userID)
}

func (r *Runtime) Statistics(ctx context.Context) (Statistics, error) {
	return r.jobStatistics(ctx, nil)
}

func (r *Runtime) jobStatistics(ctx context.Context, userID *int64) (Statistics, error) {
	if r == nil || r.pool == nil {
		return Statistics{}, ErrRuntimeNotConfigured
	}
	jobTable := pgx.Identifier{r.schema, "river_job"}.Sanitize()
	receiptTable := pgx.Identifier{r.schema, "remote_write_receipts"}.Sanitize()
	rows, err := r.pool.Query(ctx, `WITH states AS (
 SELECT state::text AS state FROM `+jobTable+`
 WHERE kind<>$2 AND ($1::bigint IS NULL OR COALESCE(args->>'user_id',metadata->>'user_id')=$1::text)
 UNION ALL
 SELECT CASE WHEN bool_and(state='deleted') THEN 'completed'
   WHEN bool_or(state IN ('unresolved','exhausted')) THEN 'discarded'
   WHEN bool_or(attempts>0) THEN 'retryable' ELSE 'pending' END
 FROM `+receiptTable+` WHERE job_id IS NOT NULL AND ($1::bigint IS NULL OR user_id=$1::bigint)
 GROUP BY user_id,job_id
) SELECT state,count(*) FROM states GROUP BY state`, dbtypes.OptionalInt8(userID), ReceiptCleanupKind)
	if err != nil {
		return Statistics{}, fmt.Errorf("job statistics: %w", err)
	}
	defer rows.Close()
	var stats Statistics
	for rows.Next() {
		var state string
		var count int64
		if err = rows.Scan(&state, &count); err != nil {
			return Statistics{}, err
		}
		switch state {
		case "available":
			stats.Available = count
		case "cancelled":
			stats.Cancelled = count
		case "completed":
			stats.Completed = count
		case "discarded":
			stats.Discarded = count
		case "pending":
			stats.Pending = count
		case "retryable":
			stats.Retryable = count
		case "running":
			stats.Running = count
		case "scheduled":
			stats.Scheduled = count
		}
	}
	return stats, rows.Err()
}

func (r *Runtime) Cancel(ctx context.Context, id int64) (Job, error) {
	if r == nil || r.client == nil {
		return Job{}, ErrRuntimeNotConfigured
	}
	row, err := r.client.JobCancel(ctx, id)
	if err != nil {
		return Job{}, err
	}
	return jobFromRiver(row), nil
}

func (r *Runtime) Retry(ctx context.Context, id int64) (Job, error) {
	if r == nil || r.client == nil || r.pool == nil {
		return Job{}, ErrRuntimeNotConfigured
	}
	var row *rivertype.JobRow
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		var err error
		var ownerID int64
		receiptTable := pgx.Identifier{r.schema, "remote_write_receipts"}.Sanitize()
		err = tx.QueryRow(ctx, "SELECT user_id FROM "+receiptTable+" WHERE job_id=$1 OR wakeup_job_id=$1 ORDER BY id LIMIT 1", id).Scan(&ownerID)
		if err == nil {
			if err = lifecycleReceiptRetryTx(ctx, tx, r.schema, ownerID, id); err != nil {
				return err
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		row, err = r.client.JobRetryTx(ctx, tx, id)
		if err != nil || row.State == rivertype.JobStateRunning {
			return err
		}

		jobTable := pgx.Identifier{r.schema, "river_job"}.Sanitize()
		return tx.QueryRow(ctx,
			"UPDATE "+jobTable+" SET metadata = metadata - 'cancel_attempted_at' WHERE id = $1 RETURNING metadata",
			id,
		).Scan(&row.Metadata)
	})
	if err != nil {
		return Job{}, err
	}
	return jobFromRiver(row), nil
}

func (r *Runtime) Delete(ctx context.Context, id int64) error {
	if r == nil || r.client == nil {
		return ErrRuntimeNotConfigured
	}
	_, err := r.client.JobDelete(ctx, id)
	return err
}

func jobFromRiver(row *rivertype.JobRow) Job {
	item := Job{
		ID: row.ID, State: string(row.State), Attempt: row.Attempt, MaxAttempts: row.MaxAttempts,
		AttemptedAt: row.AttemptedAt, CreatedAt: row.CreatedAt, FinalizedAt: row.FinalizedAt,
		ScheduledAt: row.ScheduledAt, Priority: row.Priority, Kind: row.Kind, Queue: row.Queue,
		Args: map[string]json.RawMessage{}, Metadata: map[string]json.RawMessage{},
		AttemptedBy: append([]string(nil), row.AttemptedBy...), Tags: append([]string(nil), row.Tags...),
	}
	_ = json.Unmarshal(row.EncodedArgs, &item.Args)
	_ = json.Unmarshal(row.Metadata, &item.Metadata)
	item.Output = append(json.RawMessage(nil), row.Output()...)
	item.Errors = make([]JobError, 0, len(row.Errors))
	for _, attemptError := range row.Errors {
		item.Errors = append(item.Errors, JobError{
			Attempt: attemptError.Attempt,
			At:      attemptError.At,
			Error:   attemptError.Error,
			Trace:   attemptError.Trace,
		})
	}
	if len(item.Errors) > 0 {
		item.LastError = item.Errors[len(item.Errors)-1].Error
	}
	return item
}

func encodeCursor(id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(id, 10)))
}
func decodeCursor(cursor string) (int64, error) {
	if strings.TrimSpace(cursor) == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, ErrInvalidCursor
	}
	id, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || id <= 0 {
		return 0, ErrInvalidCursor
	}
	return id, nil
}

func lifecycleReceiptRetryTx(ctx context.Context, tx pgx.Tx, schema string, ownerID, id int64) error {
	// Serialize retry admission with every lifetime claim before River changes.
	if err := lifecycle.LockOwnerTx(ctx, tx, ownerID); err != nil {
		return err
	}
	var unavailable bool
	table := pgx.Identifier{schema, "remote_write_receipts"}.Sanitize()
	if err := tx.QueryRow(ctx, "SELECT COALESCE(bool_or(state IN ('unresolved','exhausted') OR attempts>=4),false) FROM "+table+" WHERE user_id=$1 AND (job_id=$2 OR wakeup_job_id=$2)", ownerID, id).Scan(&unavailable); err != nil {
		return err
	}
	if unavailable {
		return ErrReceiptRetryUnavailable
	}
	return nil
}

func jobFromReceiptReport(report sqlcgen.GetReceiptJobReportRow) Job {
	output := struct {
		Receipts    int64  `json:"receipts"`
		Attempts    int32  `json:"attempts"`
		PrimaryCode string `json:"primaryCode,omitempty"`
		CleanupCode string `json:"cleanupCode,omitempty"`
	}{Receipts: report.ReceiptCount, Attempts: report.Attempts, PrimaryCode: report.PrimaryCode, CleanupCode: report.CleanupCode}
	encoded, _ := json.Marshal(output)
	item := Job{ID: report.JobID, State: report.State, Kind: ReceiptCleanupKind, Queue: CleanupQueue,
		Attempt: int(report.Attempts), MaxAttempts: 4, Priority: 2, CreatedAt: report.CreatedAt.Time,
		ScheduledAt: report.UpdatedAt.Time, Args: rawArgs(struct {
			UserID int64 `json:"user_id"`
		}{report.UserID}),
		Metadata: map[string]json.RawMessage{}, Output: encoded, Errors: []JobError{}, Tags: []string{}, AttemptedBy: []string{}}
	if report.State == "completed" || report.State == "discarded" {
		at := report.UpdatedAt.Time
		item.FinalizedAt = &at
	}
	if report.RetryUnavailable {
		code := output.CleanupCode
		if code == "" {
			code = "remote_outcome_unknown"
		}
		item.LastError = code
		item.Errors = append(item.Errors, JobError{Attempt: item.Attempt, At: report.UpdatedAt.Time, Error: code})
	}
	return item
}
