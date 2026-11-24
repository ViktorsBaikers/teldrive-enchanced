package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/divyam234/riverpro"
	prodriver "github.com/divyam234/riverpro/driver"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
)

var ErrInvalidJobState = errors.New("invalid job state")
var ErrSystemPeriodicJob = errors.New("periodic job is system owned")
var ErrInvalidPeriodicJobPriority = errors.New("periodic job priority must be between 1 and 4")
var ErrPeriodicJobCapacity = errors.New("periodic job capacity is exhausted")

const receiptCleanupPeriodicID = "receipt_cleanup_sweep"

// The pinned scheduler reads only the first 10000 definitions, ordered by ID.
const maxPeriodicDefinitions = 10000

func (r *Runtime) lockPeriodicDefinitions(ctx context.Context, tx pgx.Tx) error {
	table := pgx.Identifier{r.schema, "river_periodic_job"}.Sanitize()
	if _, err := tx.Exec(ctx, "LOCK TABLE "+table+" IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		return fmt.Errorf("lock periodic definitions: %w", err)
	}
	return nil
}

func (r *Runtime) checkPeriodicDefinitions(ctx context.Context, tx pgx.Tx) error {
	table := pgx.Identifier{r.schema, "river_periodic_job"}.Sanitize()
	var total, invalid int64
	if err := tx.QueryRow(ctx, "SELECT count(*),count(*) FILTER(WHERE priority NOT BETWEEN 1 AND 4) FROM "+table).Scan(&total, &invalid); err != nil {
		return fmt.Errorf("check periodic definitions: %w", err)
	}
	if invalid > 0 {
		return ErrInvalidPeriodicJobPriority
	}
	if total > maxPeriodicDefinitions {
		return ErrPeriodicJobCapacity
	}
	return nil
}

func requiredReceiptPeriodicSpec() *riverpro.PeriodicJobSpec {
	return &riverpro.PeriodicJobSpec{
		ID: receiptCleanupPeriodicID, Kind: ReceiptDiscoverySweepKind, Args: []byte("{}"),
		Queue: CleanupQueue, Priority: 2, MaxAttempts: 3, Tags: []string{},
		Schedule: &riverpro.PeriodicJobSchedule{CronExpression: uploadCleanupDefaultCron, CronTimezone: maintenanceTimezone},
		Paused:   false,
	}
}

func checkRequiredReceiptPeriodic(row *prodriver.PeriodicJob) error {
	spec := requiredReceiptPeriodicSpec()
	if row == nil || row.ID != spec.ID || row.Kind != spec.Kind || !jsonBytesEqual(row.Args, spec.Args) ||
		row.Queue != spec.Queue || row.Priority != spec.Priority || row.MaxAttempts != spec.MaxAttempts ||
		len(row.Tags) != 0 || row.CronExpression == nil || *row.CronExpression != spec.Schedule.CronExpression ||
		row.CronTimezone != spec.Schedule.CronTimezone || row.PausedAt != nil {
		return errors.New("required receipt discovery definition did not reconcile")
	}
	return nil
}

func periodicInsertOptions(input PeriodicJobInput) (*riverpro.PeriodicJobSpec, error) {
	if input.Priority > 4 {
		return nil, ErrInvalidPeriodicJobPriority
	}
	args, err := newRawPeriodicJobArgs(input.Kind, input.Args)
	if err != nil {
		return nil, err
	}
	return &riverpro.PeriodicJobSpec{
		ID: input.ID, JobArgs: args, Queue: defaultQueue(input.Queue), Priority: defaultPriority(input.Priority),
		MaxAttempts: defaultMaxAttempts(input.MaxAttempts), Tags: append([]string(nil), input.Tags...),
		Schedule: &riverpro.PeriodicJobSchedule{CronExpression: input.Schedule.CronExpression, CronTimezone: defaultTimezone(input.Schedule.CronTimezone)},
		Paused:   input.Paused,
	}, nil
}

func (r *Runtime) guardPeriodicMutation(ctx context.Context, id, proposedKind string) error {
	if id == receiptCleanupPeriodicID || strings.TrimSpace(proposedKind) == ReceiptDiscoverySweepKind {
		return ErrSystemPeriodicJob
	}
	row, err := r.client.PeriodicJobGet(ctx, id)
	if errors.Is(err, river.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if row.Kind == ReceiptDiscoverySweepKind {
		return ErrSystemPeriodicJob
	}
	return nil
}

type CreateInput struct {
	Kind        string
	Args        map[string]json.RawMessage
	Queue       string
	Priority    int
	MaxAttempts int
	Tags        []string
}

type Queue struct {
	Name      string
	Paused    bool
	Available int64
	Running   int64
	Retryable int64
	Scheduled int64
}

type PeriodicSchedule struct {
	CronExpression string
	CronTimezone   string
}

type PeriodicJob struct {
	ID          string
	Kind        string
	Args        map[string]json.RawMessage
	Queue       string
	Priority    int
	MaxAttempts int
	Tags        []string
	Schedule    PeriodicSchedule
	NextRunAt   time.Time
	Paused      bool
	PausedAt    *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type PeriodicJobInput struct {
	ID          string
	Kind        string
	Args        map[string]json.RawMessage
	Queue       string
	Priority    int
	MaxAttempts int
	Tags        []string
	Schedule    PeriodicSchedule
	Paused      bool
}

type PeriodicTemplate struct {
	ID                    string
	Label                 string
	Description           string
	Kind                  string
	DefaultArgs           map[string]json.RawMessage
	DefaultQueue          string
	DefaultPriority       int
	DefaultMaxAttempts    int
	DefaultTags           []string
	DefaultCronExpression string
	DefaultCronTimezone   string
}

func (r *Runtime) Get(ctx context.Context, id int64) (Job, error) {
	if r == nil || r.client == nil || r.pool == nil || r.policy == nil {
		return Job{}, ErrRuntimeNotConfigured
	}
	var ownerID int64
	table := pgx.Identifier{r.schema, "remote_write_receipts"}.Sanitize()
	err := r.pool.QueryRow(ctx, "SELECT user_id FROM "+table+" WHERE job_id=$1 LIMIT 1", id).Scan(&ownerID)
	if err == nil {
		report, err := r.policy.GetReceiptJobReport(ctx, ownerID, id)
		if err != nil {
			return Job{}, err
		}
		return jobFromReceiptReport(*report), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, err
	}
	row, err := r.client.JobGet(ctx, id)
	if err != nil {
		return Job{}, err
	}
	if row.Kind == ReceiptCleanupKind {
		return Job{}, river.ErrNotFound
	}
	return jobFromRiver(row), nil
}

func (r *Runtime) ListQueues(ctx context.Context) ([]Queue, error) {
	if r == nil || r.client == nil {
		return nil, ErrRuntimeNotConfigured
	}
	executor := r.client.Driver().GetExecutor()
	rows, err := executor.QueueList(ctx, &riverdriver.QueueListParams{Max: 1000, Schema: r.schema})
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.Name)
	}
	counts, err := executor.JobCountByQueueAndState(ctx, &riverdriver.JobCountByQueueAndStateParams{
		QueueNames: names,
		Schema:     r.schema,
	})
	if err != nil {
		return nil, fmt.Errorf("count queue jobs: %w", err)
	}
	countByQueue := make(map[string]*riverdriver.JobCountByQueueAndStateResult, len(counts))
	for _, count := range counts {
		countByQueue[count.Queue] = count
	}
	result := make([]Queue, 0, len(rows))
	for _, row := range rows {
		queue := Queue{Name: row.Name, Paused: row.PausedAt != nil}
		if count := countByQueue[row.Name]; count != nil {
			queue.Available = int64(count.CountAvailable)
			queue.Running = int64(count.CountRunning)
		}
		result = append(result, queue)
	}
	slices.SortFunc(result, func(a, b Queue) int { return strings.Compare(a.Name, b.Name) })
	return result, nil
}

func (r *Runtime) ListQueuesForUser(ctx context.Context, userID int64) ([]Queue, error) {
	if r == nil || r.pool == nil || userID <= 0 {
		return nil, ErrRuntimeNotConfigured
	}
	jobTable := pgx.Identifier{r.schema, "river_job"}.Sanitize()
	rows, err := r.pool.Query(ctx, fmt.Sprintf(`
SELECT queue,
       count(*) FILTER (WHERE state::text = 'available'),
       count(*) FILTER (WHERE state::text = 'running'),
       count(*) FILTER (WHERE state::text = 'retryable'),
       count(*) FILTER (WHERE state::text = 'scheduled')
FROM %s
WHERE COALESCE(args->>'user_id', metadata->>'user_id') = $1
GROUP BY queue
ORDER BY queue`, jobTable), fmt.Sprintf("%d", userID))
	if err != nil {
		return nil, fmt.Errorf("list user queues: %w", err)
	}
	defer rows.Close()
	result := make([]Queue, 0)
	for rows.Next() {
		var queue Queue
		if err := rows.Scan(&queue.Name, &queue.Available, &queue.Running, &queue.Retryable, &queue.Scheduled); err != nil {
			return nil, fmt.Errorf("scan user queue: %w", err)
		}
		result = append(result, queue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user queues: %w", err)
	}
	global, err := r.ListQueues(ctx)
	if err == nil {
		paused := make(map[string]bool, len(global))
		for _, queue := range global {
			paused[queue.Name] = queue.Paused
		}
		for index := range result {
			result[index].Paused = paused[result[index].Name]
		}
	}
	return result, nil
}

func (r *Runtime) PauseQueue(ctx context.Context, name string) error {
	if r == nil || r.client == nil {
		return ErrRuntimeNotConfigured
	}
	return r.client.QueuePause(ctx, name, nil)
}

func (r *Runtime) ResumeQueue(ctx context.Context, name string) error {
	if r == nil || r.client == nil {
		return ErrRuntimeNotConfigured
	}
	return r.client.QueueResume(ctx, name, nil)
}

func (r *Runtime) Purge(ctx context.Context, state string) (int64, error) {
	if r == nil || r.pool == nil {
		return 0, ErrRuntimeNotConfigured
	}
	switch state {
	case "cancelled", "completed", "discarded":
	default:
		return 0, ErrInvalidJobState
	}
	jobTable := pgx.Identifier{r.schema, "river_job"}.Sanitize()
	command, err := r.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE state::text = $1", jobTable), state)
	if err != nil {
		return 0, fmt.Errorf("purge %s jobs: %w", state, err)
	}
	return command.RowsAffected(), nil
}

func (r *Runtime) PurgeForUser(ctx context.Context, userID int64, state string) (int64, error) {
	if r == nil || r.pool == nil || userID <= 0 {
		return 0, ErrRuntimeNotConfigured
	}
	switch state {
	case "cancelled", "completed", "discarded":
	default:
		return 0, ErrInvalidJobState
	}
	jobTable := pgx.Identifier{r.schema, "river_job"}.Sanitize()
	command, err := r.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE state::text = $1 AND COALESCE(args->>'user_id', metadata->>'user_id') = $2", jobTable), state, fmt.Sprintf("%d", userID))
	if err != nil {
		return 0, fmt.Errorf("purge %s jobs for user: %w", state, err)
	}
	return command.RowsAffected(), nil
}

func (r *Runtime) ListPeriodicJobs(ctx context.Context) ([]PeriodicJob, error) {
	if r == nil || r.client == nil {
		return nil, ErrRuntimeNotConfigured
	}
	rows, err := r.client.PeriodicJobList(ctx, &riverpro.PeriodicJobListOpts{Max: 1000})
	if err != nil {
		return nil, fmt.Errorf("list periodic jobs: %w", err)
	}
	result := make([]PeriodicJob, 0, len(rows))
	for _, row := range rows {
		job, err := periodicJobFromRiver(row)
		if err != nil {
			return nil, err
		}
		result = append(result, job)
	}
	return result, nil
}

func (r *Runtime) ResetPeriodicJobs(ctx context.Context) (items []PeriodicJob, resetErr error) {
	if r == nil || r.client == nil || r.pool == nil {
		return nil, ErrRuntimeNotConfigured
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin periodic reset: %w", err)
	}
	defer rollbackJobTransaction(tx, &resetErr)
	if err = r.lockPeriodicDefinitions(ctx, tx); err != nil {
		return nil, err
	}
	for {
		rows, err := r.client.PeriodicJobListTx(ctx, tx, &riverpro.PeriodicJobListOpts{Max: 1000})
		if err != nil {
			return nil, fmt.Errorf("list periodic jobs for reset: %w", err)
		}
		deleted := 0
		for _, row := range rows {
			if row.ID == receiptCleanupPeriodicID {
				continue
			}
			if _, err = r.client.PeriodicJobDeleteTx(ctx, tx, row.ID); err != nil {
				return nil, fmt.Errorf("delete periodic job during reset: %w", err)
			}
			deleted++
		}
		if deleted == 0 {
			break
		}
	}
	templates := r.PeriodicJobCatalog()
	result := make([]PeriodicJob, 0, len(templates))
	for _, template := range templates {
		if template.ID == receiptCleanupPeriodicID {
			continue
		}
		opts, err := periodicInsertOptions(PeriodicJobInput{
			ID: template.ID, Kind: template.Kind, Args: template.DefaultArgs, Queue: template.DefaultQueue,
			Priority: template.DefaultPriority, MaxAttempts: template.DefaultMaxAttempts, Tags: template.DefaultTags,
			Schedule: PeriodicSchedule{CronExpression: template.DefaultCronExpression, CronTimezone: template.DefaultCronTimezone},
		})
		if err != nil {
			return nil, err
		}
		row, err := r.client.PeriodicJobInsertTx(ctx, tx, opts)
		if err != nil {
			return nil, fmt.Errorf("restore default periodic job: %w", err)
		}
		job, err := periodicJobFromRiver(row)
		if err != nil {
			return nil, err
		}
		result = append(result, job)
	}
	row, err := r.client.PeriodicJobUpsertTx(ctx, tx, requiredReceiptPeriodicSpec())
	if err != nil {
		return nil, fmt.Errorf("reconcile required receipt discovery during reset: %w", err)
	}
	if err = checkRequiredReceiptPeriodic(row); err != nil {
		return nil, err
	}
	required, err := periodicJobFromRiver(row)
	if err != nil {
		return nil, err
	}
	result = append(result, required)
	if err = r.checkPeriodicDefinitions(ctx, tx); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit periodic reset: %w", err)
	}
	return result, nil
}

func (r *Runtime) PeriodicJobCatalog() []PeriodicTemplate {
	required := requiredReceiptPeriodicSpec()
	templates := []PeriodicTemplate{
		{
			ID: required.ID, Label: "Receipt cleanup discovery",
			Description: "System-owned recovery of durable remote-write evidence; full reset preserves this enabled schedule.",
			Kind:        required.Kind, DefaultArgs: rawArgs(ReceiptDiscoverySweepArgs{}),
			DefaultQueue: required.Queue, DefaultPriority: required.Priority, DefaultMaxAttempts: required.MaxAttempts, DefaultTags: required.Tags,
			DefaultCronExpression: required.Schedule.CronExpression, DefaultCronTimezone: required.Schedule.CronTimezone,
		},
		{
			ID: uploadCleanupPeriodicID, Label: "Upload cleanup", Description: "Remove abandoned upload sessions and temporary parts.",
			Kind: UploadCleanupSweepKind, DefaultArgs: rawArgs(UploadCleanupSweepArgs{}),
			DefaultQueue: CleanupQueue, DefaultPriority: 2, DefaultMaxAttempts: 3,
			DefaultCronExpression: uploadCleanupDefaultCron, DefaultCronTimezone: maintenanceTimezone,
		},
		{
			ID: eventCleanupPeriodicID, Label: "User event cleanup", Description: "Delete replayable user events older than the configured retention period.",
			Kind: EventCleanupKind, DefaultArgs: rawArgs(EventCleanupArgs{Retention: eventCleanupDefaultRetention}),
			DefaultQueue: CleanupQueue, DefaultPriority: 2, DefaultMaxAttempts: 3,
			DefaultCronExpression: eventCleanupDefaultCron, DefaultCronTimezone: maintenanceTimezone,
		},
		{
			ID: folderSizeRollupPeriodicID, Label: "Folder size rollup", Description: "Recompute cumulative folder sizes for the whole drive.",
			Kind: FolderSizeRollupKind, DefaultArgs: rawArgs(FolderSizeRollupArgs{}),
			DefaultQueue: CleanupQueue, DefaultPriority: 3, DefaultMaxAttempts: 1,
			DefaultCronExpression: folderSizeRollupDefaultCron, DefaultCronTimezone: maintenanceTimezone,
		},
	}
	if r.purgeEnabled {
		templates = append(templates,
			PeriodicTemplate{
				ID: trashCleanupPeriodicID, Label: "Trash cleanup", Description: "Permanently remove trashed files after their retention period.",
				Kind: TrashCleanupSweepKind, DefaultArgs: rawArgs(TrashCleanupSweepArgs{Retention: "720h"}),
				DefaultQueue: CleanupQueue, DefaultPriority: 1, DefaultMaxAttempts: 3,
				DefaultCronExpression: trashCleanupDefaultCron, DefaultCronTimezone: maintenanceTimezone,
			},
			PeriodicTemplate{
				ID: purgePeriodicID, Label: "Pending deletion cleanup", Description: "Finish permanent deletion for files already marked deletion-pending.",
				Kind: PurgeSweepKind, DefaultArgs: rawArgs(PurgeSweepArgs{}),
				DefaultQueue: PurgeQueue, DefaultPriority: 1, DefaultMaxAttempts: 3,
				DefaultCronExpression: pendingDeletionCleanupDefaultCron, DefaultCronTimezone: maintenanceTimezone,
			},
		)
	}
	if r.orphanCleanupEnabled {
		templates = append(templates, PeriodicTemplate{
			ID: orphanCleanupPeriodicID, Label: "Orphaned Telegram-part cleanup",
			Description: "Delete old Telegram documents that are not referenced by file or upload parts.",
			Kind:        OrphanCleanupKind, DefaultArgs: rawArgs(OrphanCleanupArgs{}),
			DefaultQueue: CleanupQueue, DefaultPriority: 3, DefaultMaxAttempts: 3,
			DefaultCronExpression: orphanCleanupDefaultCron, DefaultCronTimezone: maintenanceTimezone,
		})
	}
	return templates
}

func (r *Runtime) CreatePeriodicJob(ctx context.Context, input PeriodicJobInput) (created PeriodicJob, createErr error) {
	if r == nil || r.client == nil || r.pool == nil {
		return PeriodicJob{}, ErrRuntimeNotConfigured
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.guardPeriodicMutation(ctx, input.ID, input.Kind); err != nil {
		return PeriodicJob{}, err
	}
	opts, err := periodicInsertOptions(input)
	if err != nil {
		return PeriodicJob{}, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return PeriodicJob{}, fmt.Errorf("begin periodic creation: %w", err)
	}
	defer rollbackJobTransaction(tx, &createErr)
	if err = r.lockPeriodicDefinitions(ctx, tx); err != nil {
		return PeriodicJob{}, err
	}
	row, err := r.client.PeriodicJobInsertTx(ctx, tx, opts)
	if err != nil {
		return PeriodicJob{}, fmt.Errorf("create periodic job %q: %w", input.ID, err)
	}
	created, err = periodicJobFromRiver(row)
	if err != nil {
		return PeriodicJob{}, err
	}
	if err = r.checkPeriodicDefinitions(ctx, tx); err != nil {
		return PeriodicJob{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return PeriodicJob{}, fmt.Errorf("commit periodic creation: %w", err)
	}
	return created, nil
}

func (r *Runtime) UpdatePeriodicJob(ctx context.Context, id string, input PeriodicJobInput) (PeriodicJob, error) {
	if r == nil || r.client == nil {
		return PeriodicJob{}, ErrRuntimeNotConfigured
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.guardPeriodicMutation(ctx, id, input.Kind); err != nil {
		return PeriodicJob{}, err
	}
	current, err := r.client.PeriodicJobGet(ctx, id)
	if err != nil {
		return PeriodicJob{}, err
	}
	args, encodedArgs, err := rawPeriodicJobArgsAndJSON(input.Kind, input.Args)
	if err != nil {
		return PeriodicJob{}, err
	}
	queue := defaultQueue(input.Queue)
	priority := defaultPriority(input.Priority)
	if priority > 4 {
		return PeriodicJob{}, ErrInvalidPeriodicJobPriority
	}
	maxAttempts := defaultMaxAttempts(input.MaxAttempts)
	tags := append([]string(nil), input.Tags...)
	timezone := defaultTimezone(input.Schedule.CronTimezone)
	opts := &riverpro.PeriodicJobUpdateOpts{}
	if current.Kind != input.Kind || !jsonBytesEqual(current.Args, encodedArgs) {
		opts.JobArgs = args
	}
	if current.Queue != queue {
		opts.Queue = &queue
	}
	if current.Priority != priority {
		opts.Priority = &priority
	}
	if current.MaxAttempts != maxAttempts {
		opts.MaxAttempts = &maxAttempts
	}
	if !reflect.DeepEqual(current.Tags, tags) {
		opts.Tags = &tags
	}
	currentCron := ""
	if current.CronExpression != nil {
		currentCron = *current.CronExpression
	}
	if currentCron != input.Schedule.CronExpression || current.CronTimezone != timezone {
		opts.Schedule = &riverpro.PeriodicJobSchedule{CronExpression: input.Schedule.CronExpression, CronTimezone: timezone}
	}
	row := current
	if opts.JobArgs != nil || opts.Queue != nil || opts.Priority != nil || opts.MaxAttempts != nil || opts.Tags != nil || opts.Schedule != nil {
		row, err = r.client.PeriodicJobUpdate(ctx, id, opts)
		if err != nil {
			return PeriodicJob{}, fmt.Errorf("update periodic job %q: %w", id, err)
		}
	}
	if input.Paused && row.PausedAt == nil {
		row, err = r.client.PeriodicJobPause(ctx, id)
	} else if !input.Paused && row.PausedAt != nil {
		row, err = r.client.PeriodicJobResume(ctx, id)
	}
	if err != nil {
		return PeriodicJob{}, err
	}
	return periodicJobFromRiver(row)
}

func (r *Runtime) DeletePeriodicJob(ctx context.Context, id string) error {
	if r == nil || r.client == nil {
		return ErrRuntimeNotConfigured
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.guardPeriodicMutation(ctx, id, ""); err != nil {
		return err
	}
	_, err := r.client.PeriodicJobDelete(ctx, id)
	return err
}

func (r *Runtime) PausePeriodicJob(ctx context.Context, id string) (PeriodicJob, error) {
	if r == nil || r.client == nil {
		return PeriodicJob{}, ErrRuntimeNotConfigured
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.guardPeriodicMutation(ctx, id, ""); err != nil {
		return PeriodicJob{}, err
	}
	row, err := r.client.PeriodicJobPause(ctx, id)
	if err != nil {
		return PeriodicJob{}, err
	}
	return periodicJobFromRiver(row)
}

func (r *Runtime) ResumePeriodicJob(ctx context.Context, id string) (PeriodicJob, error) {
	if r == nil || r.client == nil {
		return PeriodicJob{}, ErrRuntimeNotConfigured
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.guardPeriodicMutation(ctx, id, ""); err != nil {
		return PeriodicJob{}, err
	}
	row, err := r.client.PeriodicJobResume(ctx, id)
	if err != nil {
		return PeriodicJob{}, err
	}
	return periodicJobFromRiver(row)
}

type rawPeriodicJobArgs struct {
	kind string
	raw  json.RawMessage
}

func (a rawPeriodicJobArgs) Kind() string                 { return a.kind }
func (a rawPeriodicJobArgs) MarshalJSON() ([]byte, error) { return append([]byte(nil), a.raw...), nil }

func newRawPeriodicJobArgs(kind string, args map[string]json.RawMessage) (rawPeriodicJobArgs, error) {
	jobArgs, _, err := rawPeriodicJobArgsAndJSON(kind, args)
	return jobArgs, err
}

func rawPeriodicJobArgsAndJSON(kind string, args map[string]json.RawMessage) (rawPeriodicJobArgs, []byte, error) {
	kind = strings.TrimSpace(kind)
	if kind == UploadBatchKind || kind == UploadSourceKind || kind == ReceiptCleanupKind {
		return rawPeriodicJobArgs{}, nil, errors.New("job kind requires its typed transactional producer")
	}
	if strings.TrimSpace(kind) == "" {
		return rawPeriodicJobArgs{}, nil, errors.New("periodic job kind is required")
	}
	if args == nil {
		args = map[string]json.RawMessage{}
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return rawPeriodicJobArgs{}, nil, fmt.Errorf("encode periodic job args: %w", err)
	}
	return rawPeriodicJobArgs{kind: kind, raw: encoded}, encoded, nil
}

func periodicJobFromRiver(row *prodriver.PeriodicJob) (PeriodicJob, error) {
	if row == nil {
		return PeriodicJob{}, errors.New("periodic job row is nil")
	}
	args := map[string]json.RawMessage{}
	if len(row.Args) > 0 {
		if err := json.Unmarshal(row.Args, &args); err != nil {
			return PeriodicJob{}, fmt.Errorf("decode periodic job %q args: %w", row.ID, err)
		}
	}
	cron := ""
	if row.CronExpression != nil {
		cron = *row.CronExpression
	}
	return PeriodicJob{
		ID: row.ID, Kind: row.Kind, Args: args, Queue: row.Queue, Priority: row.Priority,
		MaxAttempts: row.MaxAttempts, Tags: append([]string(nil), row.Tags...),
		Schedule:  PeriodicSchedule{CronExpression: cron, CronTimezone: row.CronTimezone},
		NextRunAt: row.NextRunAt.UTC(), PausedAt: utcTimePtr(row.PausedAt),
		Paused: row.PausedAt != nil, CreatedAt: row.CreatedAt.UTC(), UpdatedAt: row.UpdatedAt.UTC(),
	}, nil
}

type rawJobArgs struct {
	kind string
	raw  json.RawMessage
}

func (a rawJobArgs) Kind() string                 { return a.kind }
func (a rawJobArgs) MarshalJSON() ([]byte, error) { return append([]byte(nil), a.raw...), nil }

func (r *Runtime) Create(ctx context.Context, input CreateInput) (Job, error) {
	if r == nil || r.client == nil {
		return Job{}, ErrRuntimeNotConfigured
	}
	kind := strings.TrimSpace(input.Kind)
	if kind != UploadCleanupSweepKind && kind != EventCleanupKind && kind != TrashCleanupSweepKind && kind != PurgeSweepKind && kind != OrphanCleanupKind {
		return Job{}, fmt.Errorf("unsupported job kind %q", kind)
	}
	encoded, err := json.Marshal(input.Args)
	if err != nil {
		return Job{}, fmt.Errorf("encode job args: %w", err)
	}
	result, err := r.client.Insert(ctx, rawJobArgs{kind: kind, raw: encoded}, &river.InsertOpts{
		Queue: defaultQueue(input.Queue), Priority: defaultPriority(input.Priority),
		MaxAttempts: defaultMaxAttempts(input.MaxAttempts), Tags: append([]string(nil), input.Tags...),
	})
	if err != nil {
		return Job{}, fmt.Errorf("create job: %w", err)
	}
	return jobFromRiver(result.Job), nil
}

func rawArgs(value any) map[string]json.RawMessage {
	encoded, _ := json.Marshal(value)
	result := map[string]json.RawMessage{}
	_ = json.Unmarshal(encoded, &result)
	return result
}

func defaultQueue(value string) string {
	if strings.TrimSpace(value) == "" {
		return river.QueueDefault
	}
	return value
}

func defaultPriority(value int) int {
	if value <= 0 {
		return river.PriorityDefault
	}
	return value
}

func defaultMaxAttempts(value int) int {
	if value <= 0 {
		return river.MaxAttemptsDefault
	}
	return value
}

func defaultTimezone(value string) string {
	if strings.TrimSpace(value) == "" {
		return "UTC"
	}
	return value
}

func utcTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.UTC()
	return &result
}

func jsonBytesEqual(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return string(left) == string(right)
	}
	return reflect.DeepEqual(leftValue, rightValue)
}
