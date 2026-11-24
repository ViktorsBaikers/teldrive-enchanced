package jobs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

const (
	skipExcluded     = "excluded"
	skipBelowMinSize = "below_min_size"
	skipAboveMaxSize = "above_max_size"
)

var errInvalidUploadSource = errors.New("invalid upload source")

const (
	UploadBatchKind    = "teldrive_upload_batch"
	UploadSourceKind   = "teldrive_upload_source"
	UploadQueue        = "uploads"
	uploadChunkBlock   = int64(16 * 1024 * 1024)
	minUploadChunk     = int64(64 * 1024 * 1024)
	maxUploadChunk     = int64(2000 * 1024 * 1024)
	defaultUploadChunk = int64(512 * 1024 * 1024)
)

type UploadSource struct {
	Type            string            `json:"type"`
	Path            string            `json:"path,omitempty"`
	URL             string            `json:"url,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
	DestinationPath string            `json:"destination_path,omitempty"`
	Exclude         []string          `json:"exclude,omitempty"`
}

type UploadBatchArgs struct {
	BatchID         string            `json:"batch_id"`
	UserID          int64             `json:"user_id"`
	Destination     string            `json:"destination,omitempty"`
	ParentID        string            `json:"parent_id,omitempty"`
	Sources         []UploadSource    `json:"sources"`
	Headers         map[string]string `json:"headers,omitempty"`
	Exclude         []string          `json:"exclude,omitempty"`
	MinSize         string            `json:"min_size,omitempty"`
	MaxSize         string            `json:"max_size,omitempty"`
	PartConcurrency int               `json:"part_concurrency,omitempty"`
	ChunkSize       int64             `json:"chunk_size,omitempty"`
	Encryption      bool              `json:"encryption,omitempty"`
}

func (UploadBatchArgs) Kind() string { return UploadBatchKind }
func (UploadBatchArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: UploadQueue, MaxAttempts: 3}
}

type UploadFileSource struct {
	Type            string            `json:"type"`
	Location        string            `json:"location"`
	Headers         map[string]string `json:"headers,omitempty"`
	DestinationPath string            `json:"destination_path"`
	Size            int64             `json:"size"`
	ModTime         time.Time         `json:"mod_time"`
	HasModTime      bool              `json:"has_mod_time"`
	MIMEType        string            `json:"mime_type,omitempty"`
	Validator       string            `json:"validator,omitempty"`
}

type UploadSourceArgs struct {
	BatchID         string           `json:"batch_id" river:"unique"`
	SourceIndex     int              `json:"source_index" river:"unique"`
	UserID          int64            `json:"user_id"`
	ParentID        string           `json:"parent_id,omitempty"`
	Source          UploadFileSource `json:"source"`
	PartConcurrency int              `json:"part_concurrency"`
	ChunkSize       int64            `json:"chunk_size"`
	Encryption      bool             `json:"encryption,omitempty"`
}

func (UploadSourceArgs) Kind() string { return UploadSourceKind }
func (UploadSourceArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: UploadQueue, MaxAttempts: 5, UniqueOpts: river.UniqueOpts{ByArgs: true}}
}

type UploadBatchWorker struct {
	river.WorkerDefaults[UploadBatchArgs]
	pool             *pgxpool.Pool
	uploads          *uploads.Service
	httpClient       *http.Client
	catalog          *catalog.Service
	localImportRoots []string
	commit           func(context.Context, pgx.Tx) error
}

func NewUploadBatchWorker(pool *pgxpool.Pool, httpClient *http.Client, catalogService *catalog.Service, uploadService *uploads.Service, localImportRoots ...[]string) *UploadBatchWorker {
	if httpClient == nil {
		httpClient = NewUploadHTTPClient()
	}
	var roots []string
	if len(localImportRoots) > 0 {
		roots = append([]string(nil), localImportRoots[0]...)
	}
	return &UploadBatchWorker{pool: pool, uploads: uploadService, httpClient: httpClient, catalog: catalogService, localImportRoots: roots, commit: func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) }}
}

func (w *UploadBatchWorker) Timeout(*river.Job[UploadBatchArgs]) time.Duration { return time.Hour }

func (w *UploadBatchWorker) Work(ctx context.Context, job *river.Job[UploadBatchArgs]) (workErr error) {
	if w == nil || w.pool == nil || w.uploads == nil || w.catalog == nil || job == nil || job.JobRow == nil || job.ID <= 0 {
		return ErrRuntimeNotConfigured
	}
	defer func() {
		if errors.Is(workErr, uploads.ErrParentUnavailable) || errors.Is(workErr, catalog.ErrParentUnavailable) {
			workErr = river.JobCancel(workErr)
		}
	}()
	if job.Args.UserID <= 0 || job.Args.BatchID == "" {
		return errInvalidUploadSource
	}
	args, err := canonicalUploadBatchArgs(job.Args)
	if err != nil {
		return err
	}
	batchID := uuid.MustParse(args.BatchID)
	fingerprint, err := uploadBatchFingerprint(args)
	if err != nil {
		return err
	}
	batch, err := w.uploads.GetImportBatch(ctx, args.UserID, batchID, job.ID)
	if err != nil {
		return err
	}
	if !bytes.Equal(batch.RequestFingerprint, fingerprint[:]) {
		return lifecycle.ErrRequestMismatch
	}
	if batch.TerminalFailureCode.Valid {
		return river.JobCancel(uploads.ErrParentUnavailable)
	}
	// Validate persisted Q pins even on a dispatched replay, without resolving paths.
	pins, err := sqlcgen.New(w.pool).ListImportDestinationPins(ctx, sqlcgen.ListImportDestinationPinsParams{UserID: args.UserID, BatchID: dbtypes.UUID(batchID)})
	if err != nil {
		return err
	}
	if len(pins) != len(args.Sources)+1 {
		return lifecycle.ErrRequestMismatch
	}
	for i, pin := range pins {
		if pin.DestinationOrdinal != int32(i)-1 || pin.AnchorIsRoot == pin.AnchorID.Valid || len(pin.SpecFingerprint) != 32 || len(pin.RemainderFingerprint) != 32 || pin.RemainderSegments < 0 {
			return lifecycle.ErrRequestMismatch
		}
	}
	if batch.Dispatched {
		return nil
	}
	client, err := river.ClientFromContextSafely[pgx.Tx](ctx)
	if err != nil {
		return fmt.Errorf("get River client: %w", err)
	}
	filter, err := newUploadFilter(args.Exclude, args.MinSize, args.MaxSize)
	if err != nil {
		return err
	}
	concurrency := args.PartConcurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	if concurrency > 16 {
		return errInvalidUploadSource
	}
	chunkSize, err := normalizeUploadChunkSize(args.ChunkSize)
	if err != nil {
		return err
	}
	var sources []UploadFileSource
	var destinations []uploads.ImportSourceDestination
	for ordinal, source := range args.Sources {
		files, expandErr := w.expand(ctx, source, args.Headers, filter)
		if expandErr != nil {
			return expandErr
		}
		for _, file := range files {
			directory, name := path.Split(file.DestinationPath)
			if name == "" || name == "." || len(sources) >= math.MaxInt32 {
				return errInvalidUploadSource
			}
			destinations = append(destinations, uploads.ImportSourceDestination{AttemptID: uuid.New(), SourceIndex: int32(len(sources)), DestinationOrdinal: int32(ordinal), Directory: strings.TrimSuffix(directory, "/")})
			sources = append(sources, file)
		}
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackJobTransaction(tx, &workErr)
	if err = lifecycle.LockOwnerTx(ctx, tx, args.UserID); err != nil {
		return err
	}
	base, err := parseOptionalUUID(args.ParentID)
	if err != nil {
		return err
	}
	reservation, err := w.uploads.ReserveImportBatchTx(ctx, tx, uploads.ReserveImportBatchInput{
		UserID: args.UserID, BatchID: batchID, OriginJobID: job.ID, RequestFingerprint: fingerprint,
		BaseParentID: base, Destination: args.Destination, SourceDestinations: uploadBatchSourceDestinations(args), Sources: destinations,
	})
	if err != nil {
		return err
	}
	if reservation.Dispatched {
		return nil
	}
	if len(reservation.Sources) != len(sources) {
		return lifecycle.ErrRequestMismatch
	}
	params := make([]river.InsertManyParams, len(sources))
	for i, destination := range reservation.Sources {
		parent := ""
		if destination.ParentID != nil {
			parent = destination.ParentID.String()
		}
		params[i] = river.InsertManyParams{Args: UploadSourceArgs{BatchID: args.BatchID, SourceIndex: int(destination.SourceIndex), UserID: args.UserID, ParentID: parent, Source: sources[i], PartConcurrency: concurrency, ChunkSize: chunkSize, Encryption: args.Encryption},
			InsertOpts: &river.InsertOpts{Metadata: ownerJobMetadata(args.UserID)}}
	}
	children := make([]uploads.ImportChild, len(params))
	if len(params) > 0 {
		results, insertErr := client.InsertManyTx(ctx, tx, params)
		if insertErr != nil {
			return fmt.Errorf("insert upload source jobs: %w", insertErr)
		}
		if len(results) != len(params) {
			return errors.New("upload source insert returned wrong child count")
		}
		for i, result := range results {
			if result == nil || result.Job == nil || result.Job.ID <= 0 {
				return errors.New("upload source insert returned no positive job ID")
			}
			children[i] = uploads.ImportChild{AttemptID: reservation.Sources[i].AttemptID, SourceIndex: reservation.Sources[i].SourceIndex, JobID: result.Job.ID}
		}
	}
	if err = w.uploads.RecordImportChildrenTx(ctx, tx, uploads.RecordImportChildrenInput{UserID: args.UserID, BatchID: batchID, OriginJobID: job.ID, Destinations: reservation, Children: children}); err != nil {
		return err
	}
	if err = w.commit(ctx, tx); err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return fmt.Errorf("commit import dispatch: %w", err)
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	committed, readErr := w.uploads.GetImportBatch(readCtx, args.UserID, batchID, job.ID)
	if readErr != nil {
		return errors.Join(fmt.Errorf("commit import dispatch: %w", err), readErr)
	}
	if committed.TerminalFailureCode.Valid {
		return errors.Join(err, river.JobCancel(uploads.ErrParentUnavailable))
	}
	if committed.Dispatched && bytes.Equal(committed.RequestFingerprint, fingerprint[:]) {
		return nil
	}
	return fmt.Errorf("commit import dispatch: %w", err)
}

func (w *UploadBatchWorker) expand(ctx context.Context, source UploadSource, defaults map[string]string, batchFilter uploadFilter) ([]UploadFileSource, error) {
	switch source.Type {
	case "local":
		if err := validateLocalImportSource(source.Path, w.localImportRoots); err != nil {
			return nil, err
		}
		return expandLocalSource(source, batchFilter)
	case "http":
		file, err := inspectHTTPSource(ctx, w.httpClient, source, defaults)
		if err != nil {
			return nil, err
		}
		if batchFilter.skipReason(file.DestinationPath, file.Size) != "" {
			return nil, nil
		}
		return []UploadFileSource{file}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported source type %q", errInvalidUploadSource, source.Type)
	}
}

type UploadSourceWorker struct {
	river.WorkerDefaults[UploadSourceArgs]
	pool               *pgxpool.Pool
	queries            *sqlcgen.Queries
	catalog            *catalog.Service
	uploads            *uploads.Service
	pipeline           *transfer.Pipeline
	httpClient         *http.Client
	activeKeyVersion   int32
	encryptionRequired bool
	localImportRoots   []string
	commit             func(context.Context, pgx.Tx) error
	create             func(context.Context, uploads.CreateInput) (*sqlcgen.UploadSession, error)
}

func NewUploadSourceWorker(pool *pgxpool.Pool, catalogService *catalog.Service, uploadService *uploads.Service, pipeline *transfer.Pipeline, httpClient *http.Client, activeKeyVersion int32, encryptionRequired bool, localImportRoots []string) *UploadSourceWorker {
	if httpClient == nil {
		httpClient = NewUploadHTTPClient()
	}
	return &UploadSourceWorker{pool: pool, queries: sqlcgen.New(pool), catalog: catalogService, uploads: uploadService, pipeline: pipeline, httpClient: httpClient, activeKeyVersion: activeKeyVersion, encryptionRequired: encryptionRequired, localImportRoots: append([]string(nil), localImportRoots...), commit: func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) }}
}

func (w *UploadSourceWorker) Timeout(*river.Job[UploadSourceArgs]) time.Duration {
	return 24 * time.Hour
}

func (w *UploadSourceWorker) Work(ctx context.Context, job *river.Job[UploadSourceArgs]) (workErr error) {
	if w == nil || w.pool == nil || w.catalog == nil || w.uploads == nil || w.pipeline == nil || job == nil || job.JobRow == nil || job.ID <= 0 || job.Args.UserID <= 0 {
		return ErrRuntimeNotConfigured
	}
	defer func() {
		if errors.Is(workErr, uploads.ErrParentUnavailable) || errors.Is(workErr, catalog.ErrParentUnavailable) {
			workErr = river.JobCancel(workErr)
		}
	}()
	batchID, err := uuid.Parse(job.Args.BatchID)
	if err != nil || batchID == uuid.Nil || job.Args.SourceIndex < 0 || job.Args.SourceIndex > math.MaxInt32 {
		return errInvalidUploadSource
	}
	attempt, err := w.uploads.GetImportAttempt(ctx, job.Args.UserID, batchID, int32(job.Args.SourceIndex), job.ID)
	if err != nil {
		return err
	}
	if attempt.TerminalFailureCode.Valid {
		return river.JobCancel(uploads.ErrParentUnavailable)
	}
	parentValue, hasParent := dbtypes.GoogleUUID(attempt.ParentID)
	var parentID *uuid.UUID
	if hasParent {
		parentID = &parentValue
	}
	requestParent, err := parseOptionalUUID(job.Args.ParentID)
	if err != nil {
		return err
	}
	if dbtypes.OptionalUUID(requestParent) != attempt.ParentID {
		return lifecycle.ErrRequestMismatch
	}
	if attempt.ResultFileID.Valid {
		id := uuid.UUID(attempt.ResultFileID.Bytes)
		result, getErr := w.catalog.Get(ctx, job.Args.UserID, id)
		if getErr != nil {
			return getErr
		}
		if (job.Args.Encryption || w.encryptionRequired) && !result.Encryption {
			return uploads.ErrInvalidState
		}
		var key *int32
		if result.EncryptionKeyVersion.Valid {
			value := result.EncryptionKeyVersion.Int32
			key = &value
		}
		_, err = w.catalog.CheckActiveImportResult(ctx, catalog.CheckActiveImportResultInput{UserID: job.Args.UserID, FileID: id, ParentID: parentID, Encryption: result.Encryption, EncryptionKeyVersion: key})
		return err
	}
	var selected *sqlcgen.UploadSession
	var frozen uploads.CreateInput
	if attempt.UploadID.Valid {
		frozen, err = uploads.SelectedImportCreateInput(attempt)
		if err != nil {
			return err
		}
		if (job.Args.Encryption || w.encryptionRequired) && !frozen.Encryption {
			return uploads.ErrInvalidState
		}
		selected, err = w.uploads.Get(ctx, job.Args.UserID, uuid.UUID(attempt.UploadID.Bytes))
		if err != nil {
			if errors.Is(err, uploads.ErrParentUnavailable) {
				return river.JobCancel(err)
			}
			if attempt.SessionMaterialized || !errors.Is(err, uploads.ErrNotFound) {
				return err
			}
		}
		if selected != nil && selected.State == sqlcgen.UploadStateCompleted {
			if !selected.FileID.Valid {
				return uploads.ErrNotFound
			}
			var key *int32
			if selected.EncryptionKeyVersion.Valid {
				value := selected.EncryptionKeyVersion.Int32
				key = &value
			}
			_, err = w.catalog.CheckActiveImportResult(ctx, catalog.CheckActiveImportResultInput{UserID: job.Args.UserID, FileID: uuid.UUID(selected.FileID.Bytes), ParentID: parentID, Encryption: selected.Encryption, EncryptionKeyVersion: key})
			return err
		}
		if selected != nil && (job.Args.Encryption || w.encryptionRequired) && !selected.Encryption {
			return uploads.ErrInvalidState
		}
		if frozen.Encryption {
			if frozen.EncryptionKeyVersion == nil {
				return transfer.ErrEncryptionKey
			}
			if err = w.pipeline.ValidateUploadKey(ctx, job.Args.UserID, *frozen.EncryptionKeyVersion); err != nil {
				return err
			}
		}
	}
	if job.Args.PartConcurrency <= 0 {
		job.Args.PartConcurrency = 4
	}
	job.Args.Encryption = job.Args.Encryption || w.encryptionRequired
	if job.Args.PartConcurrency > 16 {
		return fmt.Errorf("%w: part concurrency exceeds 16", errInvalidUploadSource)
	}
	var chunkSize int64
	if !attempt.UploadID.Valid {
		chunkSize, err = normalizeUploadChunkSize(job.Args.ChunkSize)
		if err != nil {
			return err
		}
	}
	client, _ := river.ClientFromContextSafely[pgx.Tx](ctx)
	_, name := path.Split(job.Args.Source.DestinationPath)
	if name == "" || name == "." {
		return errInvalidUploadSource
	}
	if !attempt.UploadID.Valid {
		probe := job.Args.Source
		if parsed, _, e := mime.ParseMediaType(probe.MIMEType); e == nil {
			probe.MIMEType = parsed
		}
		if _, _, err = w.findResumableUpload(ctx, job.Args.UserID, parentID, name, probe, job.Args.Encryption); err != nil {
			return err
		}
	}
	hadModTime := job.Args.Source.HasModTime
	sourceReader, err := w.openSource(ctx, &job.Args.Source)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := sourceReader.Close(); closeErr != nil {
			workErr = errors.Join(workErr, fmt.Errorf("close upload source: %w", closeErr))
		}
	}()
	if parsed, _, parseErr := mime.ParseMediaType(job.Args.Source.MIMEType); parseErr == nil {
		job.Args.Source.MIMEType = parsed
	}
	buffered := bufio.NewReader(sourceReader)
	if job.Args.Source.MIMEType == "" || job.Args.Source.MIMEType == "application/octet-stream" {
		prefix, peekErr := buffered.Peek(512)
		if peekErr != nil && !errors.Is(peekErr, io.EOF) {
			return fmt.Errorf("read upload source prefix failed: %w", peekErr)
		}
		job.Args.Source.MIMEType = http.DetectContentType(prefix)
		if parsed, _, parseErr := mime.ParseMediaType(job.Args.Source.MIMEType); parseErr == nil {
			job.Args.Source.MIMEType = parsed
		}
		if job.Args.Source.MIMEType == "application/octet-stream" {
			if inferred := mime.TypeByExtension(path.Ext(job.Args.Source.DestinationPath)); inferred != "" {
				if parsed, _, parseErr := mime.ParseMediaType(inferred); parseErr == nil {
					job.Args.Source.MIMEType = parsed
				}
			}
		}
	}
	var session *sqlcgen.UploadSession
	if attempt.UploadID.Valid {
		session = selected
	} else {
		candidate, _, findErr := w.findResumableUpload(ctx, job.Args.UserID, parentID, name, job.Args.Source, job.Args.Encryption)
		if findErr != nil {
			return findErr
		}
		existing, resolveErr := w.queries.ResolveActiveChild(ctx, sqlcgen.ResolveActiveChildParams{UserID: job.Args.UserID, ParentID: attempt.ParentID, Name: name})
		if resolveErr != nil && !errors.Is(resolveErr, pgx.ErrNoRows) {
			return resolveErr
		}
		var matching *sqlcgen.File
		if resolveErr == nil {
			if existing.Kind != sqlcgen.FileKindFile {
				return uploads.ErrNameConflict
			}
			if job.Args.Source.Type == "local" && (!job.Args.Encryption || existing.Encryption) && existing.Size.Valid && existing.Size.Int64 == job.Args.Source.Size && existing.HashAlgorithm.String == "blake3" && existing.HashValue.Valid && w.localSourceMatches(ctx, job.Args.Source, existing.HashValue.String) {
				matching = existing
			}
		}
		input := uploads.CreateInput{UserID: job.Args.UserID, ParentID: parentID, Name: name, ExpectedSize: job.Args.Source.Size, MIMEType: optionalString(job.Args.Source.MIMEType), ModTime: job.Args.Source.ModTime, ConflictPolicy: sqlcgen.NameConflictPolicyReplace, Encryption: job.Args.Encryption, PartSize: chunkSize}
		if candidate != nil {
			input.ID = uuid.UUID(candidate.ID.Bytes)
			input.PartSize = candidate.PartSize
			if candidate.EncryptionKeyVersion.Valid {
				value := candidate.EncryptionKeyVersion.Int32
				input.EncryptionKeyVersion = &value
			}
		} else if matching == nil {
			input.ID = uuid.New()
			if input.Encryption {
				if w.activeKeyVersion <= 0 {
					return transfer.ErrEncryptionKey
				}
				input.EncryptionKeyVersion = &w.activeKeyVersion
			}
		}
		attempt, err = w.selectImportAttempt(ctx, job, batchID, input, candidate != nil, matching)
		if err != nil {
			return err
		}
		if attempt.UploadID.Valid {
			frozen, err = uploads.SelectedImportCreateInput(attempt)
			if err != nil {
				return err
			}
		}
		if attempt.ResultFileID.Valid {
			output := UploadSourceOutput{Path: job.Args.Source.DestinationPath, SourceType: job.Args.Source.Type, Stage: "skipped", Reason: "destination_matches", Progress: 100, TotalBytes: job.Args.Source.Size, UpdatedAt: time.Now().UTC(), FileID: uuid.UUID(attempt.ResultFileID.Bytes).String()}
			if client != nil {
				return river.RecordOutput(ctx, output)
			}
			return nil
		}
	}
	if !attempt.UploadID.Valid {
		return uploads.ErrInvalidState
	}
	if name != frozen.Name || job.Args.Source.Size != frozen.ExpectedSize || (frozen.MIMEType == nil && job.Args.Source.MIMEType != "") || (frozen.MIMEType != nil && job.Args.Source.MIMEType != *frozen.MIMEType) || ((hadModTime || job.Args.Source.HasModTime) && !job.Args.Source.ModTime.UTC().Truncate(time.Microsecond).Equal(frozen.ModTime)) {
		return fmt.Errorf("%w: source metadata differs from selected upload", errInvalidUploadSource)
	}
	if session == nil {
		input := frozen
		if w.create != nil {
			session, err = w.create(ctx, input)
		} else {
			session, err = w.uploads.Create(ctx, input)
		}
	}
	if err != nil {
		return err
	}
	if session.State != sqlcgen.UploadStateOpen {
		return uploads.ErrInvalidState
	}
	uploadID, ok := dbtypes.GoogleUUID(session.ID)
	if !ok {
		return errInvalidUploadSource
	}
	partCount := int(session.ExpectedSize / session.PartSize)
	if session.ExpectedSize%session.PartSize != 0 {
		partCount++
	}
	var jobID int64
	if job.JobRow != nil {
		jobID = job.ID
	}
	tracker := newUploadProgressTracker(client, jobID, uploadID, job.Args.Source, session.PartSize, 1, partCount, nil)
	if err := tracker.update(ctx, "uploading", false); err != nil {
		return err
	}
	var consumed int64
	for part := range partCount {
		if err := ctx.Err(); err != nil {
			return err
		}
		size := min(session.PartSize, session.ExpectedSize-consumed)
		// Limit each part so the pipeline EOF probe cannot consume the next part.
		body := &io.LimitedReader{R: buffered, N: size}
		_, err = w.pipeline.UploadPart(ctx, transfer.UploadPartRequest{UserID: job.Args.UserID, UploadID: uploadID, PartNo: int32(part + 1), PlainSize: size, Body: io.TeeReader(body, tracker)})
		if err != nil {
			_ = tracker.finish(ctx, "failed", nil)
			return err
		}
		if body.N != 0 {
			return io.ErrUnexpectedEOF
		}
		consumed += size
		tracker.partCompleted(ctx, size)
	}
	if _, err := buffered.ReadByte(); !errors.Is(err, io.EOF) {
		_ = tracker.finish(ctx, "failed", nil)
		return errors.New("upload source did not end at declared size")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if job.Args.Source.Type == "local" {
		current, openErr := w.openLocalSource(job.Args.Source.Location)
		if openErr != nil {
			_ = tracker.finish(ctx, "failed", nil)
			return fmt.Errorf("%w: local source changed during upload", errInvalidUploadSource)
		}
		info, statErr := current.Stat()
		_ = current.Close()
		opened, openedErr := sourceReader.(*os.File).Stat()
		if statErr != nil || openedErr != nil || !os.SameFile(opened, info) || info.Size() != job.Args.Source.Size || !info.ModTime().Equal(job.Args.Source.ModTime) {
			_ = tracker.finish(ctx, "failed", nil)
			return fmt.Errorf("%w: local source changed during upload", errInvalidUploadSource)
		}
	}
	if err := tracker.update(ctx, "completing", false); err != nil {
		return err
	}
	file, err := w.uploads.Complete(ctx, uploads.CompleteInput{UserID: job.Args.UserID, UploadID: uploadID, Size: consumed})
	if err != nil {
		_ = tracker.finish(ctx, "failed", nil)
		return err
	}
	fileID, _ := dbtypes.GoogleUUID(file.ID)
	return tracker.finish(ctx, "completed", &fileID)
}

// Selection commits before materialization; uncertain acknowledgements never select another UUID.
func (w *UploadSourceWorker) selectImportAttempt(ctx context.Context, job *river.Job[UploadSourceArgs], batchID uuid.UUID, input uploads.CreateInput, candidate bool, matching *sqlcgen.File) (attempt *sqlcgen.ImportJobAttempt, workErr error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackJobTransaction(tx, &workErr)
	if err = lifecycle.LockOwnerTx(ctx, tx, job.Args.UserID); err != nil {
		return nil, err
	}
	if matching != nil {
		var key *int32
		if matching.EncryptionKeyVersion.Valid {
			value := matching.EncryptionKeyVersion.Int32
			key = &value
		}
		if _, err = w.catalog.CheckActiveImportResultTx(ctx, tx, catalog.CheckActiveImportResultInput{UserID: job.Args.UserID, FileID: uuid.UUID(matching.ID.Bytes), ParentID: input.ParentID, Encryption: matching.Encryption, EncryptionKeyVersion: key}); err != nil {
			return nil, err
		}
		attempt, err = w.uploads.RecordImportResultTx(ctx, tx, uploads.RecordImportResultInput{UserID: job.Args.UserID, BatchID: batchID, SourceIndex: int32(job.Args.SourceIndex), JobID: job.ID, ResultFileID: uuid.UUID(matching.ID.Bytes), Encryption: matching.Encryption, EncryptionKeyVersion: key})
	} else {
		attempt, err = w.uploads.ReserveImportAttemptTx(ctx, tx, uploads.ReserveImportAttemptInput{UserID: job.Args.UserID, BatchID: batchID, SourceIndex: int32(job.Args.SourceIndex), JobID: job.ID, UploadID: input.ID, Candidate: candidate, Create: input})
	}
	if err != nil {
		return nil, err
	}
	if err = w.commit(ctx, tx); err == nil {
		return attempt, nil
	}
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return nil, fmt.Errorf("commit import selection: %w", err)
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	retained, readErr := w.uploads.GetImportAttempt(readCtx, job.Args.UserID, batchID, int32(job.Args.SourceIndex), job.ID)
	if readErr != nil {
		return nil, errors.Join(fmt.Errorf("commit import selection: %w", err), readErr)
	}
	if retained.TerminalFailureCode.Valid {
		return nil, errors.Join(err, river.JobCancel(uploads.ErrParentUnavailable))
	}
	if retained.UploadID == attempt.UploadID && retained.ResultFileID == attempt.ResultFileID {
		return retained, nil
	}
	return nil, fmt.Errorf("commit import selection: %w", err)
}

type UploadSourceOutput struct {
	UploadID            string    `json:"uploadId,omitempty"`
	FileID              string    `json:"fileId,omitempty"`
	Path                string    `json:"path"`
	SourceType          string    `json:"sourceType"`
	Stage               string    `json:"stage"`
	Reason              string    `json:"reason,omitempty"`
	Progress            float64   `json:"progress"`
	UploadedBytes       int64     `json:"uploadedBytes"`
	StoredBytes         int64     `json:"storedBytes"`
	TotalBytes          int64     `json:"totalBytes"`
	SpeedBytesPerSecond int64     `json:"speedBytesPerSecond"`
	CompletedParts      int32     `json:"completedParts"`
	TotalParts          int       `json:"totalParts"`
	ChunkSize           int64     `json:"chunkSize"`
	PartConcurrency     int       `json:"partConcurrency"`
	StartedAt           time.Time `json:"startedAt"`
	UpdatedAt           time.Time `json:"updatedAt"`
}

type uploadProgressTracker struct {
	client      *river.Client[pgx.Tx]
	jobID       int64
	output      UploadSourceOutput
	started     time.Time
	transferred atomic.Int64
	attempted   atomic.Int64
	stored      atomic.Int64
	completed   atomic.Int32
	mu          sync.Mutex
}

func newUploadProgressTracker(client *river.Client[pgx.Tx], jobID int64, uploadID uuid.UUID, source UploadFileSource, chunkSize int64, concurrency, totalParts int, stored map[int32]int64) *uploadProgressTracker {
	now := time.Now().UTC()
	tracker := &uploadProgressTracker{client: client, jobID: jobID, started: now, output: UploadSourceOutput{UploadID: uploadID.String(), Path: source.DestinationPath, SourceType: source.Type, TotalBytes: source.Size, TotalParts: totalParts, ChunkSize: chunkSize, PartConcurrency: concurrency, StartedAt: now}}
	for _, size := range stored {
		tracker.stored.Add(size)
		tracker.completed.Add(1)
	}
	return tracker
}

func (t *uploadProgressTracker) Write(p []byte) (int, error) {
	t.transferred.Add(int64(len(p)))
	t.attempted.Add(int64(len(p)))
	return len(p), nil
}

func (t *uploadProgressTracker) partCompleted(ctx context.Context, size int64) {
	t.transferred.Add(-size)
	t.stored.Add(size)
	t.completed.Add(1)
	_ = t.update(ctx, "uploading", false)
}

func (t *uploadProgressTracker) update(ctx context.Context, stage string, terminal bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now().UTC()
	stored, transferred := t.stored.Load(), t.transferred.Load()
	uploaded := min(t.output.TotalBytes, stored+transferred)
	t.output.Stage, t.output.StoredBytes, t.output.UploadedBytes = stage, stored, uploaded
	t.output.CompletedParts, t.output.UpdatedAt = t.completed.Load(), now
	if t.output.TotalBytes == 0 || stage == "completed" || stage == "skipped" {
		t.output.Progress = 100
	} else {
		t.output.Progress = float64(uploaded) * 100 / float64(t.output.TotalBytes)
	}
	if elapsed := now.Sub(t.started).Seconds(); elapsed > 0 {
		t.output.SpeedBytesPerSecond = int64(float64(t.attempted.Load()) / elapsed)
	}
	if terminal {
		if t.client == nil {
			return nil
		}
		return river.RecordOutput(ctx, t.output)
	}
	if t.client == nil || t.jobID <= 0 {
		return nil
	}
	_, err := t.client.JobUpdate(ctx, t.jobID, &river.JobUpdateParams{Output: t.output})
	return err
}

func normalizeUploadChunkSize(value int64) (int64, error) {
	if value <= 0 {
		return defaultUploadChunk, nil
	}
	if value < minUploadChunk || value > maxUploadChunk {
		return 0, fmt.Errorf("%w: chunk size must be between 64 MiB and 2000 MiB", errInvalidUploadSource)
	}
	aligned := ((value + uploadChunkBlock/2) / uploadChunkBlock) * uploadChunkBlock
	return min(aligned, maxUploadChunk), nil
}

func (w *UploadSourceWorker) findResumableUpload(ctx context.Context, userID int64, parentID *uuid.UUID, name string, source UploadFileSource, encryption bool) (*sqlcgen.UploadSession, map[int32]int64, error) {
	modTime := pgtype.Timestamptz{}
	if source.HasModTime {
		modTime = dbtypes.Time(source.ModTime)
	}
	sessions, err := w.queries.FindResumableUploadSessions(ctx, sqlcgen.FindResumableUploadSessionsParams{
		UserID: userID, ParentID: dbtypes.OptionalUUID(parentID), Name: name,
		ExpectedSize: source.Size, Encryption: encryption, MimeType: dbtypes.OptionalText(optionalString(source.MIMEType)),
		HasModTime: source.HasModTime, ModTime: modTime,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("find resumable uploads: %w", err)
	}
	if len(sessions) == 0 {
		return nil, map[int32]int64{}, nil
	}
	uploadIDs := make([]pgtype.UUID, 0, len(sessions))
	for i, session := range sessions {
		id, ok := dbtypes.GoogleUUID(session.ID)
		if !ok {
			return nil, nil, errInvalidUploadSource
		}
		// The exact candidate veto precedes part compatibility and any new selection.
		current, getErr := w.uploads.Get(ctx, userID, id)
		if getErr != nil {
			return nil, nil, getErr
		}
		sessions[i] = current
		uploadIDs = append(uploadIDs, current.ID)
	}
	parts, err := w.queries.ListUploadPartsByUploadIDs(ctx, uploadIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("list resumable upload parts: %w", err)
	}
	partsByUpload := make(map[uuid.UUID][]*sqlcgen.UploadPart, len(sessions))
	for _, part := range parts {
		uploadID, ok := dbtypes.GoogleUUID(part.UploadID)
		if ok {
			partsByUpload[uploadID] = append(partsByUpload[uploadID], part)
		}
	}
	for _, session := range sessions {
		uploadID, ok := dbtypes.GoogleUUID(session.ID)
		if !ok {
			continue
		}
		stored, compatible := resumableStoredParts(partsByUpload[uploadID], session.ExpectedSize, session.PartSize)
		if compatible {
			return session, stored, nil
		}
	}
	return nil, map[int32]int64{}, nil
}

func resumableStoredParts(parts []*sqlcgen.UploadPart, totalSize, partSize int64) (map[int32]int64, bool) {
	if totalSize < 0 || partSize <= 0 || partSize%uploadChunkBlock != 0 || totalSize/partSize > math.MaxInt32 || (totalSize/partSize == math.MaxInt32 && totalSize%partSize != 0) {
		return nil, false
	}
	stored := make(map[int32]int64)
	for _, part := range parts {
		if part.State != sqlcgen.UploadPartStateStored {
			continue
		}
		if part.PartNo <= 0 || int64(part.PartNo-1) > totalSize/partSize {
			return nil, false
		}
		offset := int64(part.PartNo-1) * partSize
		digestCount := part.PlainSize / treehash.BlockSize
		if part.PlainSize%treehash.BlockSize != 0 {
			digestCount++
		}
		if part.PlainSize <= 0 || digestCount > math.MaxInt32 || int64(len(part.BlockHashes)) != digestCount*treehash.DigestSize || !part.Checksum.Valid || part.Checksum.String != treehash.SumToHex(treehash.ComputeTreeHash(part.BlockHashes)) || part.ChannelID == 0 || !part.MessageID.Valid || part.MessageID.Int64 == 0 || !part.StoredSize.Valid || part.StoredSize.Int64 <= 0 || offset < 0 || offset >= totalSize || part.PlainSize != min(partSize, totalSize-offset) {
			return nil, false
		}
		stored[part.PartNo] = part.PlainSize
	}
	return stored, true
}

func (t *uploadProgressTracker) finish(ctx context.Context, stage string, fileID *uuid.UUID) error {
	if fileID != nil {
		t.output.FileID = fileID.String()
	}
	if stage == "completed" {
		t.stored.Store(t.output.TotalBytes)
		t.transferred.Store(0)
		t.completed.Store(int32(t.output.TotalParts))
	}
	return t.update(ctx, stage, true)
}

func (w *UploadSourceWorker) openSource(ctx context.Context, source *UploadFileSource) (io.ReadCloser, error) {
	if source.Type == "local" {
		file, err := w.openLocalSource(source.Location)
		if err != nil {
			return nil, errors.New("open local upload source failed")
		}
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() {
			_ = file.Close()
			return nil, errInvalidUploadSource
		}
		source.Size, source.ModTime, source.HasModTime = info.Size(), info.ModTime(), true
		return file, nil
	}
	if source.Type != "http" {
		return nil, errInvalidUploadSource
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source.Location, nil)
	if err != nil {
		return nil, errInvalidUploadSource
	}
	if request.URL.Scheme != "http" && request.URL.Scheme != "https" {
		return nil, errInvalidUploadSource
	}
	applyUploadHeaders(request.Header, source.Headers)
	for _, name := range []string{"Range", "If-Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
		request.Header.Del(name)
	}
	request.Header.Set("Accept-Encoding", "identity")
	response, err := w.httpClient.Do(request)
	if err != nil {
		return nil, errors.New("open HTTP upload source failed")
	}
	encoding := strings.TrimSpace(response.Header.Get("Content-Encoding"))
	if response.StatusCode != http.StatusOK || response.ContentLength < 0 || response.Header.Get("Content-Range") != "" || response.Uncompressed || (encoding != "" && !strings.EqualFold(encoding, "identity")) {
		_ = response.Body.Close()
		return nil, errors.New("HTTP upload source requires a full identity response with content length")
	}
	source.Size = response.ContentLength
	source.MIMEType = strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0])
	source.ModTime, err = http.ParseTime(response.Header.Get("Last-Modified"))
	source.HasModTime = err == nil
	return response.Body, nil
}

func (w *UploadSourceWorker) localSourceMatches(ctx context.Context, source UploadFileSource, expected string) bool {
	file, err := w.openLocalSource(source.Location)
	if err != nil {
		return false
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return false
	}
	hasher := treehash.NewBlockHasher()
	buffer := make([]byte, 64*1024)
	var size int64
	for {
		if ctx.Err() != nil {
			return false
		}
		n, readErr := file.Read(buffer)
		_, _ = hasher.Write(buffer[:n])
		size += int64(n)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return false
		}
	}
	current, err := w.openLocalSource(source.Location)
	if err != nil {
		return false
	}
	defer current.Close()
	after, err := current.Stat()
	return err == nil && os.SameFile(before, after) && before.Size() == after.Size() && before.ModTime().Equal(after.ModTime()) && size == source.Size && expected == treehash.SumToHex(treehash.ComputeTreeHash(hasher.Sum()))
}

func (w *UploadSourceWorker) openLocalSource(location string) (*os.File, error) {
	location = filepath.Clean(strings.TrimSpace(location))
	if !filepath.IsAbs(location) || len(w.localImportRoots) == 0 {
		return nil, errInvalidUploadSource
	}
	resolvedLocation, err := filepath.EvalSymlinks(location)
	if err != nil || resolvedLocation != location {
		return nil, errInvalidUploadSource
	}
	for _, configuredRoot := range w.localImportRoots {
		root := filepath.Clean(strings.TrimSpace(configuredRoot))
		if !filepath.IsAbs(root) {
			continue
		}
		resolvedRoot, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		relative, err := filepath.Rel(resolvedRoot, resolvedLocation)
		if err != nil || !filepath.IsLocal(relative) {
			continue
		}
		// Resolution preserves configured aliases; the rooted open enforces containment even if paths change.
		file, err := os.OpenInRoot(resolvedRoot, relative)
		if err == nil {
			return file, nil
		}
	}
	return nil, errInvalidUploadSource
}

type uploadFilter struct {
	exclude []*regexp.Regexp
	minSize *int64
	maxSize *int64
}

func newUploadFilter(patterns []string, minSize, maxSize string) (uploadFilter, error) {
	filter := uploadFilter{}
	for _, pattern := range patterns {
		compiled, err := compileUploadGlob(pattern)
		if err != nil {
			return uploadFilter{}, err
		}
		filter.exclude = append(filter.exclude, compiled)
	}
	for _, item := range []struct {
		value  string
		target **int64
	}{{minSize, &filter.minSize}, {maxSize, &filter.maxSize}} {
		value, target := item.value, item.target
		if strings.TrimSpace(value) == "" {
			continue
		}
		size, err := parseUploadSize(value)
		if err != nil {
			return uploadFilter{}, err
		}
		*target = &size
	}
	if filter.minSize != nil && filter.maxSize != nil && *filter.minSize > *filter.maxSize {
		return uploadFilter{}, fmt.Errorf("%w: minimum size exceeds maximum size", errInvalidUploadSource)
	}
	return filter, nil
}

func (f uploadFilter) skipReason(name string, size int64) string {
	normalized := strings.TrimPrefix(path.Clean(strings.ReplaceAll(name, "\\", "/")), "./")
	for _, pattern := range f.exclude {
		if pattern.MatchString(normalized) {
			return skipExcluded
		}
	}
	if f.minSize != nil && size < *f.minSize {
		return skipBelowMinSize
	}
	if f.maxSize != nil && size > *f.maxSize {
		return skipAboveMaxSize
	}
	return ""
}

func parseUploadSize(value string) (int64, error) {
	normalized := strings.TrimSpace(value)
	match := regexp.MustCompile(`(?i)^([0-9]+(?:\.[0-9]+)?)\s*([kmgtpe]?i?b?)?$`).FindStringSubmatch(normalized)
	if match == nil {
		return 0, fmt.Errorf("%w: invalid size %q", errInvalidUploadSource, value)
	}
	number, err := strconv.ParseFloat(match[1], 64)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("%w: invalid size %q", errInvalidUploadSource, value)
	}
	unit := strings.ToUpper(match[2])
	unit = strings.TrimSuffix(unit, "B")
	base := float64(1000)
	if strings.HasSuffix(unit, "I") {
		base = 1024
		unit = strings.TrimSuffix(unit, "I")
	}
	power := strings.Index(" KMGTPE", unit)
	if power < 0 {
		return 0, fmt.Errorf("%w: invalid size %q", errInvalidUploadSource, value)
	}
	result := number * math.Pow(base, float64(power))
	if result > math.MaxInt64 || math.Trunc(result) != result {
		return 0, fmt.Errorf("%w: invalid size %q", errInvalidUploadSource, value)
	}
	return int64(result), nil
}

func compileUploadGlob(pattern string) (*regexp.Regexp, error) {
	pattern = strings.TrimPrefix(path.Clean(strings.ReplaceAll(strings.TrimSpace(pattern), "\\", "/")), "./")
	if pattern == "" || pattern == "." {
		return nil, fmt.Errorf("%w: empty exclusion", errInvalidUploadSource)
	}
	var expression strings.Builder
	expression.WriteString("^(?:.*/)?")
	for index := 0; index < len(pattern); index++ {
		switch pattern[index] {
		case '*':
			if index+1 < len(pattern) && pattern[index+1] == '*' {
				if index+2 < len(pattern) && pattern[index+2] == '/' {
					expression.WriteString("(?:.*/)?")
					index += 2
				} else {
					expression.WriteString(".*")
					index++
				}
			} else {
				expression.WriteString("[^/]*")
			}
		case '?':
			expression.WriteString("[^/]")
		default:
			expression.WriteString(regexp.QuoteMeta(string(pattern[index])))
		}
	}
	expression.WriteString("$")
	compiled, err := regexp.Compile(expression.String())
	if err != nil {
		return nil, fmt.Errorf("%w: exclusion %q: %v", errInvalidUploadSource, pattern, err)
	}
	return compiled, nil
}

// redactPathError keeps the syscall cause while hiding the local path.
func redactPathError(err error) error {
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		return err
	}
	redacted := *pathErr
	redacted.Path = "<redacted>"
	return &redacted
}

func validateLocalImportSource(location string, roots []string) error {
	location = filepath.Clean(strings.TrimSpace(location))
	if !filepath.IsAbs(location) {
		return fmt.Errorf("%w: local path must be absolute", errInvalidUploadSource)
	}
	if len(roots) == 0 {
		return fmt.Errorf("%w: local imports are disabled", errInvalidUploadSource)
	}
	resolvedLocation, err := filepath.EvalSymlinks(location)
	if err != nil {
		return fmt.Errorf("inspect local upload source: %w", redactPathError(err))
	}
	for _, configuredRoot := range roots {
		root := filepath.Clean(strings.TrimSpace(configuredRoot))
		if !filepath.IsAbs(root) {
			continue
		}
		resolvedRoot, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		relative, err := filepath.Rel(resolvedRoot, resolvedLocation)
		if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return nil
		}
	}
	return fmt.Errorf("%w: local path is outside configured import roots", errInvalidUploadSource)
}

func expandLocalSource(source UploadSource, batchFilter uploadFilter) ([]UploadFileSource, error) {
	location := filepath.Clean(strings.TrimSpace(source.Path))
	if !filepath.IsAbs(location) {
		return nil, fmt.Errorf("%w: local path must be absolute", errInvalidUploadSource)
	}
	info, err := os.Lstat(location)
	if err != nil {
		return nil, fmt.Errorf("inspect local upload source: %w", redactPathError(err))
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: symlink sources are not supported", errInvalidUploadSource)
	}
	location, err = filepath.EvalSymlinks(location)
	if err != nil {
		return nil, fmt.Errorf("resolve local upload source: %w", redactPathError(err))
	}
	filter, err := newUploadFilter(source.Exclude, "", "")
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: local source is not a regular file", errInvalidUploadSource)
		}
		name := source.DestinationPath
		if name == "" {
			name = info.Name()
		}
		destination, err := validateDestinationPath(name)
		if err != nil {
			return nil, err
		}
		if uploadSkipReason(batchFilter, filter, info.Name(), info.Size()) != "" {
			return nil, nil
		}
		return []UploadFileSource{{Type: "local", Location: location, DestinationPath: destination, Size: info.Size(), ModTime: info.ModTime().UTC(), HasModTime: true, MIMEType: mime.TypeByExtension(filepath.Ext(info.Name()))}}, nil
	}
	baseDestination, err := validateDestinationPath(source.DestinationPath)
	if err != nil {
		return nil, err
	}
	files := make([]UploadFileSource, 0)
	err = filepath.WalkDir(location, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return redactPathError(walkErr)
		}
		relative, err := filepath.Rel(location, filePath)
		if err != nil || relative == "." {
			return err
		}
		relative = filepath.ToSlash(relative)
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			if filter.skipReason(relative, 0) == skipExcluded || batchFilter.skipReason(relative, 0) == skipExcluded {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return redactPathError(err)
		}
		if !info.Mode().IsRegular() || uploadSkipReason(batchFilter, filter, relative, info.Size()) != "" {
			return nil
		}
		files = append(files, UploadFileSource{Type: "local", Location: filePath, DestinationPath: cleanDestinationPath(path.Join(baseDestination, relative)), Size: info.Size(), ModTime: info.ModTime().UTC(), HasModTime: true, MIMEType: mime.TypeByExtension(filepath.Ext(info.Name()))})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk local upload source: %w", redactPathError(err))
	}
	return files, nil
}

func inspectHTTPSource(ctx context.Context, client *http.Client, source UploadSource, defaults map[string]string) (UploadFileSource, error) {
	if client == nil {
		client = http.DefaultClient
	}
	parsed, err := url.Parse(strings.TrimSpace(source.URL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return UploadFileSource{}, fmt.Errorf("%w: invalid HTTP URL", errInvalidUploadSource)
	}
	headers := mergeUploadHeaders(defaults, source.Headers)
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, parsed.String(), nil)
	if err != nil {
		return UploadFileSource{}, err
	}
	applyUploadHeaders(request.Header, headers)
	response, err := client.Do(request)
	if err != nil {
		return UploadFileSource{}, errors.New("inspect HTTP upload source request failed")
	}
	_ = response.Body.Close()
	if response.StatusCode == http.StatusMethodNotAllowed || response.StatusCode == http.StatusNotImplemented {
		request, err = http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
		if err != nil {
			return UploadFileSource{}, err
		}
		applyUploadHeaders(request.Header, headers)
		request.Header.Set("Range", "bytes=0-0")
		response, err = client.Do(request)
		if err != nil {
			return UploadFileSource{}, errors.New("inspect HTTP upload source request failed")
		}
		_ = response.Body.Close()
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return UploadFileSource{}, fmt.Errorf("inspect HTTP upload source: %s", response.Status)
	}
	size := response.ContentLength
	if contentRange := response.Header.Get("Content-Range"); contentRange != "" {
		if slash := strings.LastIndex(contentRange, "/"); slash >= 0 {
			size, _ = strconv.ParseInt(contentRange[slash+1:], 10, 64)
		}
	}
	if size < 0 {
		return UploadFileSource{}, fmt.Errorf("%w: HTTP source has no content length", errInvalidUploadSource)
	}
	name := source.DestinationPath
	if name == "" {
		if _, params, parseErr := mime.ParseMediaType(response.Header.Get("Content-Disposition")); parseErr == nil {
			name = params["filename"]
		}
	}
	if name == "" {
		name = path.Base(parsed.Path)
	}
	if name == "" || name == "." || name == "/" {
		return UploadFileSource{}, fmt.Errorf("%w: HTTP destination name is required", errInvalidUploadSource)
	}
	modTime, timeErr := http.ParseTime(response.Header.Get("Last-Modified"))
	validator := strings.TrimSpace(response.Header.Get("ETag"))
	if validator == "" && timeErr == nil {
		validator = modTime.Format(http.TimeFormat)
	}
	destination, err := validateDestinationPath(name)
	if err != nil {
		return UploadFileSource{}, err
	}
	return UploadFileSource{Type: "http", Location: parsed.String(), Headers: headers, DestinationPath: destination, Size: size, ModTime: modTime.UTC(), HasModTime: timeErr == nil, MIMEType: strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]), Validator: validator}, nil
}

func mergeUploadHeaders(defaults, overrides map[string]string) map[string]string {
	result := make(map[string]string, len(defaults)+len(overrides))
	for key, value := range defaults {
		result[http.CanonicalHeaderKey(key)] = value
	}
	for key, value := range overrides {
		result[http.CanonicalHeaderKey(key)] = value
	}
	return result
}

func applyUploadHeaders(target http.Header, values map[string]string) {
	for key, value := range values {
		switch http.CanonicalHeaderKey(key) {
		case "Host", "Content-Length", "Range", "Connection", "Transfer-Encoding", "Proxy-Connection":
			continue
		}
		target.Set(key, value)
	}
}

func cleanDestinationPath(value string) string {
	cleaned := path.Clean(strings.TrimSpace(strings.ReplaceAll(value, "\\", "/")))
	if cleaned == "." || cleaned == "/" {
		return ""
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return ""
	}
	return strings.TrimPrefix(cleaned, "/")
}

func validateDestinationPath(value string) (string, error) {
	normalized := strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	if normalized == "" {
		return "", nil
	}
	if strings.HasPrefix(normalized, "/") {
		return "", fmt.Errorf("%w: destination path must be relative", errInvalidUploadSource)
	}
	cleaned := cleanDestinationPath(normalized)
	if cleaned == "" {
		return "", fmt.Errorf("%w: invalid destination path", errInvalidUploadSource)
	}
	return cleaned, nil
}

func parseOptionalUUID(value string) (*uuid.UUID, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid parent id", errInvalidUploadSource)
	}
	return &parsed, nil
}

func optionalString(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}

func uploadSkipReason(batch, source uploadFilter, name string, size int64) string {
	if reason := batch.skipReason(name, size); reason != "" {
		return reason
	}
	return source.skipReason(name, size)
}

func NewUploadHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		for _, address := range addresses {
			if !safeUploadAddress(address) {
				continue
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(address.String(), port))
		}
		return nil, fmt.Errorf("%w: HTTP target has no public address", errInvalidUploadSource)
	}
	client := &http.Client{Transport: transport}
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if request.URL.Scheme != "http" && request.URL.Scheme != "https" {
			return fmt.Errorf("%w: unsupported redirect scheme", errInvalidUploadSource)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if len(via) > 0 && !strings.EqualFold(request.URL.Host, via[len(via)-1].URL.Host) {
			request.Header.Del("Authorization")
			request.Header.Del("Cookie")
			request.Header.Del("Proxy-Authorization")
		}
		return nil
	}
	return client
}

func safeUploadAddress(address netip.Addr) bool {
	return address.IsValid() && !address.IsUnspecified() && !address.IsLoopback() && !address.IsPrivate() && !address.IsLinkLocalUnicast() && !address.IsLinkLocalMulticast() && !address.IsMulticast()
}
