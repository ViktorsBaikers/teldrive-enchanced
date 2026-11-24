package jobs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/divyam234/riverpro"
	"github.com/divyam234/riverpro/driver/riverpropgxv5"
	"github.com/divyam234/riverpro/riverencrypt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/bots"
	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

const (
	uploadCleanupPeriodicID           = "teldrive-upload-cleanup"
	eventCleanupPeriodicID            = "teldrive-user-event-cleanup"
	folderSizeRollupPeriodicID        = "teldrive-folder-size-rollup"
	trashCleanupPeriodicID            = "teldrive-trash-cleanup"
	purgePeriodicID                   = "teldrive-pending-file-purge"
	orphanCleanupPeriodicID           = "teldrive-orphaned-telegram-part-cleanup"
	uploadCleanupDefaultCron          = "@every 12h"
	eventCleanupDefaultCron           = "0 0 * * *"
	folderSizeRollupDefaultCron       = "@every 1h"
	eventCleanupDefaultRetention      = "48h"
	trashCleanupDefaultCron           = "@every 12h"
	pendingDeletionCleanupDefaultCron = "@every 12h"
	orphanCleanupDefaultCron          = "@every 336h"
	maintenanceTimezone               = "UTC"
	maintenanceWorkers                = 2
	cleanupPeriodicID                 = uploadCleanupPeriodicID // deprecated compatibility name
)

var ErrRuntimeNotConfigured = errors.New("job runtime is not configured")

type Runtime struct {
	client               *riverpro.Client[pgx.Tx]
	pool                 *pgxpool.Pool
	policy               *lifecycle.Service
	commit               func(context.Context, pgx.Tx) error
	uploads              *uploads.Service
	schema               string
	purgeEnabled         bool
	orphanCleanupEnabled bool
	botProvisionEnabled  bool
	uploadEnabled        bool
	mu                   sync.Mutex
	started              bool
	cancel               context.CancelFunc
}

func NewRuntime(pool *pgxpool.Pool, storage telegramstore.Storage, policy *lifecycle.Service, purgeServices ...PurgeService) (*Runtime, error) {
	return NewRuntimeWithSchema(pool, storage, "teldrive", policy, purgeServices...)
}

func NewRuntimeWithSchema(pool *pgxpool.Pool, storage telegramstore.Storage, schema string, policy *lifecycle.Service, purgeServices ...PurgeService) (*Runtime, error) {
	return newRuntimeWithSchema(pool, storage, schema, nil, nil, 7*24*time.Hour, nil, policy, purgeServices)
}

func NewRuntimeWithSchemaAndBotProvision(pool *pgxpool.Pool, storage telegramstore.Storage, schema string, botService *bots.Service, encryptor riverencrypt.Encryptor, uploadSessionTTL time.Duration, policy *lifecycle.Service, purgeServices ...PurgeService) (*Runtime, error) {
	return newRuntimeWithSchema(pool, storage, schema, botService, encryptor, uploadSessionTTL, nil, policy, purgeServices)
}

type UploaderServices struct {
	Catalog            *catalog.Service
	Uploads            *uploads.Service
	Pipeline           *transfer.Pipeline
	HTTPClient         *http.Client
	ActiveKeyVersion   int32
	EncryptionRequired bool
	LocalImportRoots   []string
}

func NewRuntimeWithServices(pool *pgxpool.Pool, storage telegramstore.Storage, schema string, botService *bots.Service, encryptor riverencrypt.Encryptor, uploadSessionTTL time.Duration, uploader UploaderServices, policy *lifecycle.Service, purgeServices ...PurgeService) (*Runtime, error) {
	return newRuntimeWithSchema(pool, storage, schema, botService, encryptor, uploadSessionTTL, &uploader, policy, purgeServices)
}

func newRuntimeWithSchema(pool *pgxpool.Pool, storage telegramstore.Storage, schema string, botService *bots.Service, encryptor riverencrypt.Encryptor, uploadSessionTTL time.Duration, uploader *UploaderServices, policy *lifecycle.Service, purgeServices []PurgeService) (*Runtime, error) {
	if pool == nil || storage == nil || policy == nil {
		return nil, ErrRuntimeNotConfigured
	}
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, NewUploadCleanupWorker(pool, policy)); err != nil {
		return nil, fmt.Errorf("register upload cleanup worker: %w", err)
	}
	if err := river.AddWorkerSafely(workers, NewReceiptCleanupWorker(pool, policy)); err != nil {
		return nil, fmt.Errorf("register receipt cleanup worker: %w", err)
	}
	if err := river.AddWorkerSafely(workers, NewReceiptDiscoverySweepWorker(pool, policy)); err != nil {
		return nil, fmt.Errorf("register receipt discovery worker: %w", err)
	}
	if err := river.AddWorkerSafely(workers, NewEventCleanupWorker(pool)); err != nil {
		return nil, fmt.Errorf("register event cleanup worker: %w", err)
	}
	if err := river.AddWorkerSafely(workers, NewFolderSizeRollupWorker(pool)); err != nil {
		return nil, fmt.Errorf("register folder size rollup worker: %w", err)
	}
	var purgeService PurgeService
	if len(purgeServices) > 0 {
		purgeService = purgeServices[0]
	}
	if purgeService != nil {
		if err := river.AddWorkerSafely(workers, NewPendingFilePurgeWorker(pool, purgeService)); err != nil {
			return nil, fmt.Errorf("register purge worker: %w", err)
		}
		if err := river.AddWorkerSafely(workers, NewTrashCleanupWorker(pool, purgeService)); err != nil {
			return nil, fmt.Errorf("register trash cleanup worker: %w", err)
		}
	}
	lister, orphanCleanupEnabled := storage.(telegramstore.DocumentMessageLister)
	if orphanCleanupEnabled {
		minimumAge := 7 * 24 * time.Hour
		if uploadSessionTTL > 0 {
			minimumAge = uploadSessionTTL
		}
		if err := river.AddWorkerSafely(workers, NewOrphanedTelegramPartsCleanupWorker(pool, policy, lister, minimumAge)); err != nil {
			return nil, fmt.Errorf("register orphan cleanup worker: %w", err)
		}
	}
	inviter, hasInviter := storage.(telegramstore.BotInviter)
	botProvisionEnabled := hasInviter && botService != nil && encryptor != nil
	if botProvisionEnabled {
		if err := river.AddWorkerSafely(workers, NewBotProvisionWorker(pool, botService, inviter)); err != nil {
			return nil, fmt.Errorf("register bot provisioning worker: %w", err)
		}
	}
	uploadEnabled := uploader != nil && uploader.Catalog != nil && uploader.Uploads != nil && uploader.Pipeline != nil
	if uploadEnabled && encryptor == nil {
		return nil, errors.New("upload job argument encryption is not configured")
	}
	if uploadEnabled {
		if uploader.HTTPClient == nil {
			uploader.HTTPClient = NewUploadHTTPClient()
		}
		if err := river.AddWorkerSafely(workers, NewUploadBatchWorker(pool, uploader.HTTPClient, uploader.Catalog, uploader.Uploads, uploader.LocalImportRoots)); err != nil {
			return nil, fmt.Errorf("register upload batch worker: %w", err)
		}
		if err := river.AddWorkerSafely(workers, NewUploadSourceWorker(pool, uploader.Catalog, uploader.Uploads, uploader.Pipeline, uploader.HTTPClient, uploader.ActiveKeyVersion, uploader.EncryptionRequired, uploader.LocalImportRoots)); err != nil {
			return nil, fmt.Errorf("register upload source worker: %w", err)
		}
	}
	riverConfig := river.Config{
		Schema:  schema,
		Workers: workers,
		Queues: map[string]river.QueueConfig{
			CleanupQueue: {MaxWorkers: maintenanceWorkers},
			UploadQueue:  {MaxWorkers: 2},
		},
		SoftStopTimeout: 30 * time.Second,
	}
	if encryptor != nil {
		kinds := make([]string, 0, 3)
		if botProvisionEnabled {
			kinds = append(kinds, BotProvisionKind)
		}
		if uploadEnabled {
			kinds = append(kinds, UploadBatchKind, UploadSourceKind)
		}
		if len(kinds) > 0 {
			riverConfig.Hooks = append(riverConfig.Hooks, riverencrypt.NewEncryptHookConfig(&riverencrypt.EncryptHookConfig{
				Encryptor: encryptor, JobKindsInclude: kinds,
			}))
		}
	}
	client, err := riverpro.NewClient(riverpropgxv5.New(pool), &riverpro.Config{
		Config: riverConfig,
		DurablePeriodicJobs: riverpro.DurablePeriodicJobsConfig{
			Enabled: true,
			// The pgx listener adds a second schema prefix to RiverPro's topic.
			PollOnly:     true,
			PollInterval: time.Second,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create RiverPro client: %w", err)
	}
	runtime := &Runtime{
		client: client, pool: pool, schema: schema, policy: policy,
		commit:       func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) },
		purgeEnabled: purgeService != nil, orphanCleanupEnabled: orphanCleanupEnabled, botProvisionEnabled: botProvisionEnabled, uploadEnabled: uploadEnabled,
	}
	if uploadEnabled {
		runtime.uploads = uploader.Uploads
	}
	return runtime, nil
}

func (r *Runtime) Start(ctx context.Context) (startErr error) {
	if r == nil || r.client == nil || r.pool == nil {
		return ErrRuntimeNotConfigured
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin periodic startup: %w", err)
	}
	defer rollbackJobTransaction(tx, &startErr)
	if err = r.lockPeriodicDefinitions(ctx, tx); err != nil {
		return err
	}
	if err = r.checkPeriodicDefinitions(ctx, tx); err != nil {
		return err
	}
	required, err := r.client.PeriodicJobUpsertTx(ctx, tx, requiredReceiptPeriodicSpec())
	if err != nil {
		return fmt.Errorf("reconcile required receipt discovery: %w", err)
	}
	if err = checkRequiredReceiptPeriodic(required); err != nil {
		return err
	}
	uploadCleanupArgs, err := json.Marshal(UploadCleanupSweepArgs{})
	if err != nil {
		return fmt.Errorf("marshal upload cleanup periodic args: %w", err)
	}
	if _, err := r.client.PeriodicJobInsertTx(ctx, tx, &riverpro.PeriodicJobInsertOpts{
		ID:          uploadCleanupPeriodicID,
		Kind:        UploadCleanupSweepKind,
		Args:        uploadCleanupArgs,
		Queue:       CleanupQueue,
		Priority:    2,
		MaxAttempts: 3,
		Schedule: &riverpro.PeriodicJobSchedule{
			CronExpression: uploadCleanupDefaultCron,
			CronTimezone:   maintenanceTimezone,
		},
	}); err != nil && !errors.Is(err, riverpro.ErrPeriodicJobAlreadyExists) {
		return fmt.Errorf("upsert upload cleanup periodic job: %w", err)
	}
	eventCleanupArgs, err := json.Marshal(EventCleanupArgs{Retention: eventCleanupDefaultRetention})
	if err != nil {
		return fmt.Errorf("marshal event cleanup periodic args: %w", err)
	}
	if _, err := r.client.PeriodicJobInsertTx(ctx, tx, &riverpro.PeriodicJobInsertOpts{
		ID:          eventCleanupPeriodicID,
		Kind:        EventCleanupKind,
		Args:        eventCleanupArgs,
		Queue:       CleanupQueue,
		Priority:    2,
		MaxAttempts: 3,
		Schedule: &riverpro.PeriodicJobSchedule{
			CronExpression: eventCleanupDefaultCron,
			CronTimezone:   maintenanceTimezone,
		},
	}); err != nil && !errors.Is(err, riverpro.ErrPeriodicJobAlreadyExists) {
		return fmt.Errorf("upsert event cleanup periodic job: %w", err)
	}
	rollupArgs, err := json.Marshal(FolderSizeRollupArgs{})
	if err != nil {
		return fmt.Errorf("marshal folder size rollup periodic args: %w", err)
	}
	if _, err := r.client.PeriodicJobInsertTx(ctx, tx, &riverpro.PeriodicJobInsertOpts{
		ID:          folderSizeRollupPeriodicID,
		Kind:        FolderSizeRollupKind,
		Args:        rollupArgs,
		Queue:       CleanupQueue,
		Priority:    3,
		MaxAttempts: 1,
		Schedule: &riverpro.PeriodicJobSchedule{
			CronExpression: folderSizeRollupDefaultCron,
			CronTimezone:   maintenanceTimezone,
		},
	}); err != nil && !errors.Is(err, riverpro.ErrPeriodicJobAlreadyExists) {
		return fmt.Errorf("upsert folder size rollup periodic job: %w", err)
	}
	if r.purgeEnabled {
		trashCleanupArgs, err := json.Marshal(TrashCleanupSweepArgs{Retention: "720h"})
		if err != nil {
			return fmt.Errorf("marshal trash cleanup periodic args: %w", err)
		}
		if _, err := r.client.PeriodicJobInsertTx(ctx, tx, &riverpro.PeriodicJobInsertOpts{
			ID:          trashCleanupPeriodicID,
			Kind:        TrashCleanupSweepKind,
			Args:        trashCleanupArgs,
			Queue:       CleanupQueue,
			Priority:    1,
			MaxAttempts: 3,
			Schedule: &riverpro.PeriodicJobSchedule{
				CronExpression: trashCleanupDefaultCron,
				CronTimezone:   maintenanceTimezone,
			},
		}); err != nil && !errors.Is(err, riverpro.ErrPeriodicJobAlreadyExists) {
			return fmt.Errorf("upsert trash cleanup periodic job: %w", err)
		}

		purgeArgs, err := json.Marshal(PurgeSweepArgs{})
		if err != nil {
			return fmt.Errorf("marshal purge periodic args: %w", err)
		}
		if _, err := r.client.PeriodicJobInsertTx(ctx, tx, &riverpro.PeriodicJobInsertOpts{
			ID:          purgePeriodicID,
			Kind:        PurgeSweepKind,
			Args:        purgeArgs,
			Queue:       PurgeQueue,
			Priority:    1,
			MaxAttempts: 3,
			Schedule: &riverpro.PeriodicJobSchedule{
				CronExpression: pendingDeletionCleanupDefaultCron,
				CronTimezone:   maintenanceTimezone,
			},
		}); err != nil && !errors.Is(err, riverpro.ErrPeriodicJobAlreadyExists) {
			return fmt.Errorf("upsert purge periodic job: %w", err)
		}
	}
	if r.orphanCleanupEnabled {
		orphanCleanupArgs, err := json.Marshal(OrphanCleanupArgs{})
		if err != nil {
			return fmt.Errorf("marshal orphan cleanup periodic args: %w", err)
		}
		if _, err := r.client.PeriodicJobInsertTx(ctx, tx, &riverpro.PeriodicJobInsertOpts{
			ID:          orphanCleanupPeriodicID,
			Kind:        OrphanCleanupKind,
			Args:        orphanCleanupArgs,
			Queue:       CleanupQueue,
			Priority:    3,
			MaxAttempts: 3,
			Schedule: &riverpro.PeriodicJobSchedule{
				CronExpression: orphanCleanupDefaultCron,
				CronTimezone:   maintenanceTimezone,
			},
		}); err != nil && !errors.Is(err, riverpro.ErrPeriodicJobAlreadyExists) {
			return fmt.Errorf("upsert orphan cleanup periodic job: %w", err)
		}
	}
	if err = r.checkPeriodicDefinitions(ctx, tx); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit periodic startup: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	if err := r.client.Start(runCtx); err != nil {
		cancel()
		return fmt.Errorf("start RiverPro client: %w", err)
	}
	r.cancel = cancel
	r.started = true
	return nil
}

func (r *Runtime) Stop(ctx context.Context) error {
	if r == nil || r.client == nil {
		return ErrRuntimeNotConfigured
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return nil
	}
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	if err := r.client.Stop(ctx); err != nil {
		return fmt.Errorf("stop RiverPro client: %w", err)
	}
	r.started = false
	return nil
}

func (r *Runtime) InsertUploadCleanup(ctx context.Context) error {
	if r == nil || r.client == nil {
		return ErrRuntimeNotConfigured
	}
	if _, err := r.client.Insert(ctx, UploadCleanupSweepArgs{}, nil); err != nil {
		return fmt.Errorf("insert upload cleanup sweep: %w", err)
	}
	return nil
}

// InsertCleanup is kept for callers using the previous generic name.
func (r *Runtime) InsertCleanup(ctx context.Context) error {
	return r.InsertUploadCleanup(ctx)
}

func (r *Runtime) InsertPurge(ctx context.Context) error {
	if r == nil || r.client == nil || !r.purgeEnabled {
		return ErrRuntimeNotConfigured
	}
	if _, err := r.client.Insert(ctx, PurgeSweepArgs{}, nil); err != nil {
		return fmt.Errorf("insert purge sweep: %w", err)
	}
	return nil
}

// InsertBotProvision queues one independent task per bot in a single transaction.
func (r *Runtime) InsertBotProvision(ctx context.Context, userID int64, botIDs []int64) ([]string, error) {
	return r.insertBotProvision(ctx, userID, botIDs, false)
}

// InsertBotReprovision repairs channel access even when the bot is already enabled.
func (r *Runtime) InsertBotReprovision(ctx context.Context, userID, botID int64) (string, error) {
	ids, err := r.insertBotProvision(ctx, userID, []int64{botID}, true)
	if err != nil || len(ids) == 0 {
		return "", err
	}
	return ids[0], nil
}

func (r *Runtime) InsertBotsReprovision(ctx context.Context, userID int64, botIDs []int64) ([]string, error) {
	return r.insertBotProvision(ctx, userID, botIDs, true)
}

func (r *Runtime) insertBotProvision(ctx context.Context, userID int64, botIDs []int64, force bool) ([]string, error) {
	if r == nil || r.client == nil || !r.botProvisionEnabled || userID <= 0 {
		return nil, ErrRuntimeNotConfigured
	}
	botIDs = normalizedBotIDs(botIDs)
	if len(botIDs) == 0 {
		return []string{}, nil
	}
	params := make([]river.InsertManyParams, 0, len(botIDs))
	for _, botID := range botIDs {
		params = append(params, river.InsertManyParams{
			Args: BotProvisionArgs{UserID: userID, BotIDs: []int64{botID}, Force: force},
			// Arguments are encrypted; retain server-controlled ownership for task views.
			InsertOpts: &river.InsertOpts{Metadata: []byte(fmt.Sprintf(`{"user_id":%d}`, userID))},
		})
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin bot provisioning insert: %w", err)
	}
	defer tx.Rollback(ctx)
	results, err := r.client.InsertManyTx(ctx, tx, params)
	if err != nil {
		return nil, fmt.Errorf("insert bot provisioning jobs: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit bot provisioning jobs: %w", err)
	}
	ids := make([]string, 0, len(results))
	for _, result := range results {
		ids = append(ids, fmt.Sprintf("%d", result.Job.ID))
	}
	return ids, nil
}

func (r *Runtime) InsertUploadBatch(ctx context.Context, args UploadBatchArgs) (job Job, workErr error) {
	if r == nil || r.client == nil || r.uploads == nil || !r.uploadEnabled || args.UserID <= 0 || len(args.Sources) == 0 {
		return Job{}, ErrRuntimeNotConfigured
	}
	var err error
	args, err = canonicalUploadBatchArgs(args)
	if err != nil {
		return Job{}, err
	}
	batchID := uuid.MustParse(args.BatchID)
	fingerprint, err := uploadBatchFingerprint(args)
	if err != nil {
		return Job{}, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("begin import admission: %w", err)
	}
	defer rollbackJobTransaction(tx, &workErr)
	if err = lifecycle.LockOwnerTx(ctx, tx, args.UserID); err != nil {
		return Job{}, err
	}
	existing, err := r.readBatchAdmission(ctx, tx, args, fingerprint)
	if err == nil {
		if err = tx.Rollback(ctx); err != nil {
			return Job{}, err
		}
		return r.batchAdmissionJob(ctx, existing, args)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, err
	}
	var base *uuid.UUID
	if args.ParentID != "" {
		id := uuid.MustParse(args.ParentID)
		base = &id
	}
	specs := uploadBatchSourceDestinations(args)
	pins, err := r.uploads.PinImportDestinationsTx(ctx, tx, uploads.PinImportDestinationsInput{
		UserID: args.UserID, BatchID: batchID, BaseParentID: base,
		Destination: args.Destination, SourceDestinations: specs,
	})
	if err != nil {
		return Job{}, err
	}
	result, err := r.client.InsertTx(ctx, tx, args, &river.InsertOpts{Metadata: ownerJobMetadata(args.UserID)})
	if err != nil {
		return Job{}, fmt.Errorf("insert admitted upload batch: %w", err)
	}
	if result == nil || result.Job == nil || result.Job.ID <= 0 {
		return Job{}, errors.New("upload batch insert returned no positive job ID")
	}
	if err = r.uploads.InsertImportBatchAdmissionTx(ctx, tx, uploads.InsertImportBatchAdmissionInput{
		UserID: args.UserID, BatchID: batchID, OriginJobID: result.Job.ID, RequestFingerprint: fingerprint, Pins: pins,
	}); err != nil {
		return Job{}, err
	}
	if err = r.commit(ctx, tx); err == nil {
		return jobFromRiver(result.Job), nil
	}
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return Job{}, fmt.Errorf("commit import admission: %w", err)
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	admission, readErr := r.readBatchAdmission(readCtx, r.pool, args, fingerprint)
	if readErr != nil {
		return Job{}, errors.Join(fmt.Errorf("commit import admission: %w", err), readErr)
	}
	return r.batchAdmissionJob(readCtx, admission, args)
}

// EnqueueReceiptCleanupTx leaves the supplied transaction and ledger IDs to its caller.
func (r *Runtime) EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in lifecycle.ReceiptCleanupInput) (jobID int64, err error) {
	if r == nil || r.client == nil {
		return 0, ErrRuntimeNotConfigured
	}
	if tx == nil || !validReceiptCleanupInput(in.UserID, in.ReceiptIDs) {
		return 0, lifecycle.ErrInvalidInput
	}
	result, err := r.client.InsertTx(ctx, tx, ReceiptCleanupArgs{UserID: in.UserID, ReceiptIDs: in.ReceiptIDs},
		&river.InsertOpts{Metadata: ownerJobMetadata(in.UserID)})
	if err != nil {
		return 0, fmt.Errorf("insert exact receipt cleanup: %w", err)
	}
	if result == nil || result.Job == nil || result.Job.ID <= 0 {
		return 0, errors.New("receipt cleanup insert returned no positive job ID")
	}
	return result.Job.ID, nil
}

func ownerJobMetadata(userID int64) []byte { return []byte(fmt.Sprintf(`{"user_id":%d}`, userID)) }

func rollbackJobTransaction(tx pgx.Tx, primary *error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		*primary = errors.Join(*primary, fmt.Errorf("roll back job transaction: %w", err))
	}
}

// These shared canonical args are also the actual batch worker's Q/A identity.
func canonicalUploadBatchArgs(args UploadBatchArgs) (UploadBatchArgs, error) {
	if strings.TrimSpace(args.BatchID) == "" {
		args.BatchID = uuid.NewString()
	}
	batch, err := uuid.Parse(args.BatchID)
	if err != nil || batch == uuid.Nil {
		return args, uploads.ErrInvalidInput
	}
	args.BatchID = batch.String()
	args.ParentID = strings.TrimSpace(args.ParentID)
	if args.ParentID != "" {
		id, e := uuid.Parse(args.ParentID)
		if e != nil || id == uuid.Nil {
			return args, uploads.ErrInvalidInput
		}
		args.ParentID = id.String()
	}
	args.Destination = strings.TrimSpace(args.Destination)
	if args.Destination != "" {
		if id, e := uuid.Parse(args.Destination); e == nil {
			if id == uuid.Nil || args.ParentID != "" {
				return args, uploads.ErrInvalidInput
			}
			args.Destination = id.String()
		} else {
			if args.ParentID == "" && !strings.HasPrefix(args.Destination, "/") {
				return args, uploads.ErrInvalidInput
			}
			if args.ParentID != "" && strings.HasPrefix(args.Destination, "/") {
				return args, uploads.ErrInvalidInput
			}
			for _, segment := range strings.Split(args.Destination, "/") {
				if segment == ".." {
					return args, uploads.ErrInvalidInput
				}
			}
			args.Destination = path.Clean(args.Destination)
			if args.Destination == "." {
				args.Destination = ""
			}
		}
	}
	// Do not mutate caller-owned slices while freezing relative destination specs.
	args.Sources = append([]UploadSource(nil), args.Sources...)
	for i := range args.Sources {
		value := args.Sources[i].DestinationPath
		value, err = validateDestinationPath(value)
		if err != nil {
			return args, err
		}
		args.Sources[i].DestinationPath = value
	}
	return args, nil
}

func uploadBatchSourceDestinations(args UploadBatchArgs) []string {
	specs := make([]string, len(args.Sources))
	for i := range args.Sources {
		specs[i] = args.Sources[i].DestinationPath
	}
	return specs
}

func uploadBatchFingerprint(args UploadBatchArgs) ([32]byte, error) {
	encoded, err := json.Marshal(args)
	if err != nil {
		return [32]byte{}, fmt.Errorf("encode import admission identity: %w", err)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("teldrive/import-admission/v1\x00"))
	_, _ = hash.Write(encoded)
	var fingerprint [32]byte
	hash.Sum(fingerprint[:0])
	return fingerprint, nil
}

func (r *Runtime) readBatchAdmission(ctx context.Context, db sqlcgen.DBTX, args UploadBatchArgs, fingerprint [32]byte) (*sqlcgen.ImportBatch, error) {
	q := sqlcgen.New(db)
	// Q has no origin job ID yet, so read by its actual owner/batch primary key.
	var originJobID int64
	err := db.QueryRow(ctx, "SELECT origin_job_id FROM "+pgx.Identifier{r.schema, "import_batches"}.Sanitize()+" WHERE user_id=$1 AND batch_id=$2", args.UserID, uuid.MustParse(args.BatchID)).Scan(&originJobID)
	if err != nil {
		return nil, err
	}
	batch, err := q.GetImportBatch(ctx, sqlcgen.GetImportBatchParams{UserID: args.UserID, BatchID: dbtypes.UUID(uuid.MustParse(args.BatchID)), OriginJobID: originJobID})
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(batch.RequestFingerprint, fingerprint[:]) {
		return nil, lifecycle.ErrRequestMismatch
	}
	pins, err := q.ListImportDestinationPins(ctx, sqlcgen.ListImportDestinationPinsParams{UserID: args.UserID, BatchID: batch.BatchID})
	if err != nil {
		return nil, err
	}
	if len(pins) != len(args.Sources)+1 {
		return nil, lifecycle.ErrRequestMismatch
	}
	for i, pin := range pins {
		if pin.DestinationOrdinal != int32(i)-1 || pin.AnchorIsRoot == pin.AnchorID.Valid ||
			len(pin.SpecFingerprint) != sha256.Size || len(pin.RemainderFingerprint) != sha256.Size || pin.RemainderSegments < 0 {
			return nil, lifecycle.ErrRequestMismatch
		}
	}
	return batch, nil
}

func (r *Runtime) batchAdmissionJob(ctx context.Context, batch *sqlcgen.ImportBatch, args UploadBatchArgs) (Job, error) {
	if batch.TerminalFailureCode.Valid {
		return Job{}, uploads.ErrParentUnavailable
	}
	row, err := r.client.JobGet(ctx, batch.OriginJobID)
	if err == nil {
		return jobFromRiver(row), nil
	}
	if !errors.Is(err, river.ErrNotFound) {
		return Job{}, err
	}
	// Removed River rows never release Q identity or authorize another dispatch.
	state, failure := "discarded", "import_job_unavailable"
	if batch.Dispatched {
		state, failure = "completed", ""
	}
	return Job{ID: batch.OriginJobID, State: state, Kind: UploadBatchKind, Queue: UploadQueue,
		Args: rawArgs(args), Metadata: rawArgs(struct {
			UserID int64 `json:"user_id"`
		}{args.UserID}),
		CreatedAt: batch.CreatedAt.Time, MaxAttempts: 3, LastError: failure}, nil
}
