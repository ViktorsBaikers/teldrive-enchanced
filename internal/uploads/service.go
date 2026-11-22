package uploads

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zeebo/blake3"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
)

const (
	defaultPartSize   int64 = 512 * 1024 * 1024
	defaultSessionTTL       = 7 * 24 * time.Hour
	defaultLeaseTTL         = time.Minute
)

var (
	ErrInvalidInput              = errors.New("invalid upload input")
	ErrNotFound                  = errors.New("upload not found")
	ErrExpired                   = errors.New("upload expired")
	ErrInvalidState              = errors.New("invalid upload state")
	ErrPartConflict              = errors.New("upload part conflicts with stored metadata")
	ErrPartBusy                  = errors.New("upload part is currently leased")
	ErrLeaseLost                 = errors.New("upload part lease was lost")
	ErrIncomplete                = errors.New("upload parts are incomplete")
	ErrNameConflict              = errors.New("destination name already exists")
	ErrInvalidParent             = errors.New("invalid parent folder")
	ErrInvalidChannel            = errors.New("invalid storage channel")
	ErrHashMismatch              = errors.New("upload hash does not match expected hash")
	ErrUnsupportedConflictPolicy = errors.New("upload conflict policy is not implemented")
	ErrParentUnavailable         = errors.New("upload destination folder is unavailable")
	ErrLifecycleUnavailable      = errors.New("upload lifecycle is not configured")
	ErrGuardedRouteRequired      = errors.New("guarded uploads require the guarded route")
	ErrRecoveryChanged           = errors.New("upload recovery incarnation changed")
	ErrGuardConflict             = errors.New("guarded upload destination changed")
)

type CatalogCacheInvalidator interface {
	InvalidateFiles(context.Context, int64, ...uuid.UUID)
}

type Service struct {
	pool               *pgxpool.Pool
	queries            *sqlcgen.Queries
	now                func() time.Time
	sessionTTL         time.Duration
	leaseTTL           time.Duration
	catalogInvalidator CatalogCacheInvalidator
	lifecycle          *lifecycle.Service
	commit             func(context.Context, pgx.Tx) error
}

func NewService(pool *pgxpool.Pool, sessionTTLs ...time.Duration) *Service {
	sessionTTL := defaultSessionTTL
	if len(sessionTTLs) > 0 && sessionTTLs[0] > 0 {
		sessionTTL = sessionTTLs[0]
	}
	return &Service{
		pool:       pool,
		queries:    sqlcgen.New(pool),
		now:        time.Now,
		sessionTTL: sessionTTL,
		leaseTTL:   defaultLeaseTTL,
		commit:     func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) },
	}
}

func (s *Service) SetCacheInvalidator(catalogInvalidator CatalogCacheInvalidator) {
	s.catalogInvalidator = catalogInvalidator
}

// BindLifecycle is initialization-only; all writers share the composed ledger.
func (s *Service) BindLifecycle(service *lifecycle.Service) error {
	if service == nil || s.lifecycle != nil {
		return ErrLifecycleUnavailable
	}
	s.lifecycle = service
	return nil
}

// GetRequestAdmission returns a scoped binding hint, not authorization.
func (s *Service) GetRequestAdmission(ctx context.Context, scope lifecycle.RequestScope, key uuid.UUID) (*sqlcgen.RequestAdmission, error) {
	if s.lifecycle == nil {
		return nil, ErrLifecycleUnavailable
	}
	return s.lifecycle.GetRequestAdmission(ctx, scope, "upload", key)
}

// RecoveryIncarnation returns the current guarded-client recovery incarnation.
func (s *Service) RecoveryIncarnation(ctx context.Context) (uuid.UUID, error) {
	current, err := s.queries.GetRecoveryIncarnation(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("get recovery incarnation: %w", err)
	}
	incarnation, ok := dbtypes.GoogleUUID(current)
	if !ok || incarnation == uuid.Nil {
		return uuid.Nil, ErrInvalidState
	}
	return incarnation, nil
}

// GuardInput is the immutable destination intent admitted for a guarded upload.
// A nil ExpectedFileID means the selected slot must stay empty through commit.
type GuardInput struct {
	RecoveryIncarnation uuid.UUID
	MountRoot           *uuid.UUID
	ExpectedFileID      *uuid.UUID
	ExpectedGeneration  *int64
}

type CreateInput struct {
	ID                    uuid.UUID
	UserID                int64
	ParentID              *uuid.UUID
	Name                  string
	ExpectedSize          int64
	ExpectedHashAlgorithm *string
	ExpectedHashValue     *string
	MIMEType              *string
	ModTime               time.Time
	Encryption            bool
	EncryptionKeyVersion  *int32
	ConflictPolicy        sqlcgen.NameConflictPolicy
	PartSize              int64
	Admission             *lifecycle.RequestAdmissionInput
	ImportAttemptID       *uuid.UUID
	Guard                 *GuardInput
}

// selectedImportCreate contains only nonsecret creation metadata, never identity or source authority.
type selectedImportCreate struct {
	Version               int                        `json:"version"`
	Name                  string                     `json:"name"`
	ExpectedSize          int64                      `json:"expectedSize"`
	ExpectedHashAlgorithm *string                    `json:"expectedHashAlgorithm"`
	ExpectedHashValue     *string                    `json:"expectedHashValue"`
	MIMEType              *string                    `json:"mimeType"`
	ModTime               time.Time                  `json:"modTime"`
	Encryption            bool                       `json:"encryption"`
	EncryptionKeyVersion  *int32                     `json:"encryptionKeyVersion"`
	ConflictPolicy        sqlcgen.NameConflictPolicy `json:"conflictPolicy"`
	PartSize              int64                      `json:"partSize"`
}

func encodeSelectedImportCreate(in CreateInput) ([]byte, error) {
	return json.Marshal(selectedImportCreate{Version: 1, Name: in.Name, ExpectedSize: in.ExpectedSize, ExpectedHashAlgorithm: in.ExpectedHashAlgorithm, ExpectedHashValue: in.ExpectedHashValue, MIMEType: in.MIMEType, ModTime: in.ModTime, Encryption: in.Encryption, EncryptionKeyVersion: in.EncryptionKeyVersion, ConflictPolicy: in.ConflictPolicy, PartSize: in.PartSize})
}

// SelectedImportCreateInput reconstructs a frozen request from its durable server identity.
func SelectedImportCreateInput(attempt *sqlcgen.ImportJobAttempt) (CreateInput, error) {
	if attempt == nil || attempt.UserID <= 0 || !attempt.AttemptID.Valid || uuid.UUID(attempt.AttemptID.Bytes) == uuid.Nil || !attempt.BatchID.Valid || uuid.UUID(attempt.BatchID.Bytes) == uuid.Nil || attempt.JobID <= 0 || attempt.SourceIndex < 0 || !attempt.UploadID.Valid || uuid.UUID(attempt.UploadID.Bytes) == uuid.Nil || attempt.ResultFileID.Valid || (attempt.ParentID.Valid && uuid.UUID(attempt.ParentID.Bytes) == uuid.Nil) {
		return CreateInput{}, ErrInvalidState
	}
	if attempt.TerminalFailureCode.Valid {
		return CreateInput{}, ErrParentUnavailable
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(attempt.SelectedCreate, &fields); err != nil || len(fields) != 11 {
		return CreateInput{}, ErrInvalidState
	}
	for _, key := range []string{"version", "name", "expectedSize", "expectedHashAlgorithm", "expectedHashValue", "mimeType", "modTime", "encryption", "encryptionKeyVersion", "conflictPolicy", "partSize"} {
		value, ok := fields[key]
		if !ok {
			return CreateInput{}, ErrInvalidState
		}
		switch key {
		case "expectedHashAlgorithm", "expectedHashValue", "mimeType", "encryptionKeyVersion":
		default:
			if string(value) == "null" {
				return CreateInput{}, ErrInvalidState
			}
		}
	}
	var saved selectedImportCreate
	if err := json.Unmarshal(attempt.SelectedCreate, &saved); err != nil || saved.Version != 1 || saved.Name == "" || saved.ModTime.IsZero() || saved.ModTime.Location() != time.UTC || saved.ModTime.Nanosecond()%1000 != 0 || saved.ConflictPolicy == "" || saved.PartSize <= 0 || (saved.EncryptionKeyVersion != nil && *saved.EncryptionKeyVersion <= 0) {
		return CreateInput{}, ErrInvalidState
	}
	id := uuid.UUID(attempt.AttemptID.Bytes)
	in := CreateInput{ID: uuid.UUID(attempt.UploadID.Bytes), UserID: attempt.UserID, ImportAttemptID: &id, Name: saved.Name, ExpectedSize: saved.ExpectedSize, ExpectedHashAlgorithm: saved.ExpectedHashAlgorithm, ExpectedHashValue: saved.ExpectedHashValue, MIMEType: saved.MIMEType, ModTime: saved.ModTime, Encryption: saved.Encryption, EncryptionKeyVersion: saved.EncryptionKeyVersion, ConflictPolicy: saved.ConflictPolicy, PartSize: saved.PartSize}
	if attempt.ParentID.Valid {
		parent := uuid.UUID(attempt.ParentID.Bytes)
		in.ParentID = &parent
	}
	normalized := in
	if err := normalizeCreateInput(&normalized); err != nil || normalized.PartSize != in.PartSize || !optionalTextEqual(dbtypes.OptionalText(normalized.ExpectedHashAlgorithm), in.ExpectedHashAlgorithm) || !optionalTextEqual(dbtypes.OptionalText(normalized.ExpectedHashValue), in.ExpectedHashValue) {
		return CreateInput{}, ErrInvalidState
	}
	return in, nil
}

func (s *Service) Create(ctx context.Context, in CreateInput) (*sqlcgen.UploadSession, error) {
	if in.UserID <= 0 {
		return nil, ErrInvalidInput
	}
	if in.Admission != nil && in.ImportAttemptID != nil {
		return nil, ErrInvalidInput
	}
	if in.Guard != nil {
		if in.Admission == nil || in.ImportAttemptID != nil || in.Guard.RecoveryIncarnation == uuid.Nil {
			return nil, ErrInvalidInput
		}
		if (in.Guard.ExpectedFileID == nil) != (in.Guard.ExpectedGeneration == nil) {
			return nil, ErrInvalidInput
		}
		if (in.Guard.ExpectedFileID == nil && in.ConflictPolicy != sqlcgen.NameConflictPolicyFail) ||
			(in.Guard.ExpectedFileID != nil && in.ConflictPolicy != sqlcgen.NameConflictPolicyReplace) {
			return nil, ErrInvalidInput
		}
		if in.Guard.ExpectedGeneration != nil && *in.Guard.ExpectedGeneration <= 0 {
			return nil, ErrInvalidInput
		}
	}
	fresh := in.ID == uuid.Nil && in.Admission == nil && in.ImportAttemptID == nil
	if fresh {
		if err := normalizeCreateInput(&in); err != nil {
			return nil, err
		}
	}
	opts := pgx.TxOptions{}
	if in.Guard != nil {
		opts.IsoLevel = pgx.ReadCommitted
	}
	tx, err := s.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("begin upload creation: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, err
	}
	q := s.queries.WithTx(tx)
	id := in.ID
	var admissionID uuid.UUID
	if in.Admission != nil {
		if s.lifecycle == nil {
			return nil, ErrLifecycleUnavailable
		}
		hint, err := s.lifecycle.GetRequestAdmission(ctx, in.Admission.Scope, "upload", in.Admission.RequestKey)
		if err == nil {
			id = uuid.UUID(hint.UploadID.Bytes)
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		if id == uuid.Nil {
			id = uuid.New()
		}
		binding, created, err := s.lifecycle.BindRequestAdmissionTx(ctx, tx, in.UserID, "upload", *in.Admission, &id, nil)
		if err != nil {
			return nil, err
		}
		id = uuid.UUID(binding.UploadID.Bytes)
		admissionID = uuid.UUID(binding.ID.Bytes)
		if !created {
			retained, err := s.getRetainedSession(ctx, q, in.UserID, id)
			if err != nil {
				return nil, err
			}
			if in.Guard != nil {
				if err := s.verifyGuardedBinding(ctx, q, retained, in); err != nil {
					return nil, err
				}
			} else if retained.Guarded {
				return nil, ErrGuardedRouteRequired
			}
			return retained, nil
		}
	}
	if in.ImportAttemptID != nil {
		attempt, err := lockCreateImport(ctx, q, in.UserID, *in.ImportAttemptID)
		if err != nil {
			return nil, err
		}
		if !attempt.UploadID.Valid || (in.ID != uuid.Nil && attempt.UploadID != dbtypes.UUID(id)) || attempt.ParentID != dbtypes.OptionalUUID(in.ParentID) {
			return nil, ErrInvalidState
		}
		id = uuid.UUID(attempt.UploadID.Bytes)
		selected, err := SelectedImportCreateInput(attempt)
		if err != nil {
			return nil, err
		}
		in = selected
		if attempt.SessionMaterialized {
			return s.getRetainedSession(ctx, q, in.UserID, id)
		}
	}
	if id == uuid.Nil {
		id = uuid.New()
	}
	if err := lockUploadLifecycle(ctx, tx, in.UserID, id); err != nil {
		return nil, err
	}
	existing, err := q.LockUploadSessionForUser(ctx, sqlcgen.LockUploadSessionForUserParams{UserID: in.UserID, UploadID: dbtypes.UUID(id)})
	if err == nil {
		retained, err := s.retainedSession(ctx, q, existing)
		if err != nil {
			return nil, err
		}
		if in.Guard != nil {
			if err := s.verifyGuardedBinding(ctx, q, retained, in); err != nil {
				return nil, err
			}
		} else if retained.Guarded {
			return nil, ErrGuardedRouteRequired
		}
		if in.ImportAttemptID != nil {
			count, err := q.MarkUploadImportMaterialized(ctx, sqlcgen.MarkUploadImportMaterializedParams{UserID: in.UserID, AttemptID: dbtypes.UUID(*in.ImportAttemptID), UploadID: dbtypes.UUID(id)})
			if err != nil || count != 1 {
				return nil, errors.Join(ErrInvalidState, err)
			}
			if err := s.commit(ctx, tx); err != nil {
				return nil, fmt.Errorf("commit resumed import materialization: %w", err)
			}
		}
		return retained, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("get selected upload: %w", err)
	}
	if !fresh && in.ImportAttemptID == nil {
		if err := normalizeCreateInput(&in); err != nil {
			return nil, err
		}
	}
	if in.Guard != nil {
		if err := s.validateGuardedCreate(ctx, q, in); err != nil {
			return nil, err
		}
	}
	if in.ParentID != nil {
		active, err := q.IsLifecycleDestinationActive(ctx, sqlcgen.IsLifecycleDestinationActiveParams{ParentID: dbtypes.UUID(*in.ParentID), UserID: in.UserID})
		if err != nil {
			return nil, fmt.Errorf("get upload ancestry: %w", err)
		}
		if !active {
			return nil, ErrParentUnavailable
		}
	}
	modTime := in.ModTime
	if modTime.IsZero() {
		modTime = s.now().UTC()
	}
	session, err := q.CreateUploadSession(ctx, sqlcgen.CreateUploadSessionParams{
		ID: dbtypes.UUID(id), UserID: in.UserID, ParentID: dbtypes.OptionalUUID(in.ParentID), Name: in.Name,
		ExpectedSize: in.ExpectedSize, ExpectedHashAlgorithm: dbtypes.OptionalText(in.ExpectedHashAlgorithm), ExpectedHashValue: dbtypes.OptionalText(in.ExpectedHashValue),
		MimeType: dbtypes.OptionalText(in.MIMEType), ModTime: dbtypes.Time(modTime.UTC()), Encryption: in.Encryption, EncryptionKeyVersion: dbtypes.OptionalInt4(in.EncryptionKeyVersion),
		ConflictPolicy: in.ConflictPolicy, PartSize: in.PartSize, Guarded: in.Guard != nil, ExpiresAt: dbtypes.Time(s.now().UTC().Add(s.sessionTTL)),
	})
	if err != nil {
		return nil, fmt.Errorf("create upload session: %w", err)
	}
	if in.Guard != nil {
		if _, err := q.InsertGuardedUploadBinding(ctx, sqlcgen.InsertGuardedUploadBindingParams{
			UploadID:            dbtypes.UUID(id),
			UserID:              in.UserID,
			RequestAdmissionID:  dbtypes.UUID(admissionID),
			RequestKey:          dbtypes.UUID(in.Admission.RequestKey),
			RecoveryIncarnation: dbtypes.UUID(in.Guard.RecoveryIncarnation),
			MountRoot:           dbtypes.OptionalUUID(in.Guard.MountRoot),
			ParentID:            dbtypes.OptionalUUID(in.ParentID),
			Name:                in.Name,
			ExpectedFileID:      dbtypes.OptionalUUID(in.Guard.ExpectedFileID),
			ExpectedGeneration:  dbtypes.OptionalInt8(in.Guard.ExpectedGeneration),
		}); err != nil {
			return nil, fmt.Errorf("bind guarded upload: %w", err)
		}
	}
	if in.ImportAttemptID != nil {
		count, err := q.MarkUploadImportMaterialized(ctx, sqlcgen.MarkUploadImportMaterializedParams{UserID: in.UserID, AttemptID: dbtypes.UUID(*in.ImportAttemptID), UploadID: dbtypes.UUID(id)})
		if err != nil || count != 1 {
			return nil, errors.Join(ErrInvalidState, err)
		}
	}
	if err := s.commit(ctx, tx); err != nil {
		// COMMIT errors never authorize allocating a second target.
		if in.Admission != nil {
			readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			binding, readErr := s.lifecycle.GetRequestAdmission(readCtx, in.Admission.Scope, "upload", in.Admission.RequestKey)
			if readErr == nil && binding.UserID == in.UserID && binding.UploadID == dbtypes.UUID(id) {
				retained, targetErr := s.Get(readCtx, in.UserID, id)
				if targetErr == nil {
					return retained, nil
				}
				return nil, errors.Join(fmt.Errorf("commit upload creation: %w", err), targetErr)
			}
			return nil, errors.Join(fmt.Errorf("commit upload creation: %w", err), readErr)
		}
		return nil, fmt.Errorf("commit upload creation: %w", err)
	}
	return session, nil
}

func (s *Service) Get(ctx context.Context, userID int64, uploadID uuid.UUID) (*sqlcgen.UploadSession, error) {
	if userID <= 0 {
		return nil, ErrInvalidInput
	}
	session, err := s.queries.GetUploadSessionForUser(ctx, sqlcgen.GetUploadSessionForUserParams{
		UploadID: dbtypes.UUID(uploadID),
		UserID:   userID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get upload: %w", err)
	}
	if err := s.validateDestination(ctx, s.queries, session); err != nil {
		return nil, err
	}
	if session.State == sqlcgen.UploadStateOpen {
		if err := validateSessionGeometry(session); err != nil {
			return nil, err
		}
		if !session.ExpiresAt.Time.After(s.now()) {
			return nil, ErrExpired
		}
	}
	return session, nil
}

func (s *Service) GetAnyOwner(ctx context.Context, uploadID uuid.UUID) (*sqlcgen.UploadSession, error) {
	if uploadID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	session, err := s.queries.GetUploadSessionAnyOwner(ctx, dbtypes.UUID(uploadID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get upload by id: %w", err)
	}
	return session, nil
}

// GetPart returns one upload part after verifying ownership of its session.
func (s *Service) GetPart(ctx context.Context, userID int64, uploadID uuid.UUID, partNo int32) (*sqlcgen.UploadPart, error) {
	if userID <= 0 || uploadID == uuid.Nil || partNo <= 0 {
		return nil, ErrInvalidInput
	}
	session, err := s.Get(ctx, userID, uploadID)
	if err != nil {
		return nil, err
	}
	if session.State == sqlcgen.UploadStateCompleted {
		return completedPart(ctx, s.queries, userID, uploadID, partNo)
	}
	part, err := s.queries.GetUploadPart(ctx, sqlcgen.GetUploadPartParams{UploadID: dbtypes.UUID(uploadID), PartNo: partNo})
	if errors.Is(err, pgx.ErrNoRows) {
		// Completion can retire the alias after the first state read.
		current, readErr := s.Get(ctx, userID, uploadID)
		if readErr != nil {
			return nil, readErr
		}
		if current.State == sqlcgen.UploadStateCompleted {
			return completedPart(ctx, s.queries, userID, uploadID, partNo)
		}
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get upload part: %w", err)
	}
	return part, nil
}

type ClaimPartInput struct {
	UserID              int64
	UploadID            uuid.UUID
	PartNo              int32
	ChannelID           int64
	PlainSize           int64
	Checksum            *string
	Guarded             bool
	RecoveryIncarnation uuid.UUID
}

type ClaimPartResult struct {
	Part       *sqlcgen.UploadPart
	LeaseToken uuid.UUID
	Existing   bool
}

func (s *Service) ClaimPart(ctx context.Context, in ClaimPartInput) (*ClaimPartResult, error) {
	if in.UserID <= 0 || in.UploadID == uuid.Nil || in.PartNo <= 0 || in.PlainSize <= 0 {
		return nil, ErrInvalidInput
	}
	if in.Checksum != nil {
		checksum, err := normalizeDigest(*in.Checksum)
		if err != nil {
			return nil, err
		}
		in.Checksum = &checksum
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin part claim: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := lockUploadLifecycle(ctx, tx, in.UserID, in.UploadID); err != nil {
		return nil, err
	}
	q := s.queries.WithTx(tx)
	session, err := q.LockUploadSessionForUser(ctx, sqlcgen.LockUploadSessionForUserParams{
		UploadID: dbtypes.UUID(in.UploadID), UserID: in.UserID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock upload for claim: %w", err)
	}
	if err := s.requireGuardedSession(ctx, q, session, in.Guarded, in.RecoveryIncarnation); err != nil {
		return nil, err
	}
	if session.State == sqlcgen.UploadStateCompleted {
		if err := s.validateDestination(ctx, q, session); err != nil {
			return nil, err
		}
		part, err := completedPart(ctx, q, in.UserID, in.UploadID, in.PartNo)
		if err != nil {
			return nil, err
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit completed part claim: %w", err)
		}
		if part.PlainSize != in.PlainSize || (in.Checksum != nil && !optionalTextEqual(part.Checksum, in.Checksum)) || !validPartProof(part) {
			return nil, ErrPartConflict
		}
		return &ClaimPartResult{Part: part, Existing: true}, nil
	}
	if err := s.validateOpenSession(session); err != nil {
		return nil, err
	}
	if err := s.validateDestination(ctx, q, session); err != nil {
		return nil, err
	}
	if err := validatePartShape(session, in.PartNo, in.PlainSize); err != nil {
		return nil, err
	}
	if in.ChannelID == 0 {
		return nil, ErrInvalidInput
	}
	if _, err := q.GetChannelForUser(ctx, sqlcgen.GetChannelForUserParams{
		UserID:    in.UserID,
		ChannelID: in.ChannelID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrInvalidChannel
		}
		return nil, fmt.Errorf("get upload channel: %w", err)
	}

	existing, err := q.GetUploadPart(ctx, sqlcgen.GetUploadPartParams{
		UploadID: dbtypes.UUID(in.UploadID),
		PartNo:   in.PartNo,
	})
	if err == nil && existing.State == sqlcgen.UploadPartStateStored {
		if existing.PlainSize == in.PlainSize && (in.Checksum == nil || optionalTextEqual(existing.Checksum, in.Checksum)) {
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit existing part claim: %w", err)
			}
			if !validPartProof(existing) {
				return nil, ErrPartConflict
			}
			return &ClaimPartResult{Part: existing, Existing: true}, nil
		}
		return nil, ErrPartConflict
	}
	if errors.Is(err, pgx.ErrNoRows) {
		existing = nil
	} else if err != nil {
		return nil, fmt.Errorf("get upload part: %w", err)
	}

	// A retained physical alias is evidence, never a reusable empty claim.
	if existing != nil && existing.MessageID.Valid {
		return nil, ErrPartConflict
	}
	channelIDs := []int64{in.ChannelID}
	if existing != nil {
		channelIDs = append(channelIDs, existing.ChannelID)
	}
	if err := lifecycle.RequireWritableChannelsTx(ctx, tx, channelIDs); err != nil {
		return nil, err
	}
	leaseToken := uuid.New()
	part, err := q.ClaimUploadPart(ctx, sqlcgen.ClaimUploadPartParams{
		UploadID:       dbtypes.UUID(in.UploadID),
		PartNo:         in.PartNo,
		ChannelID:      in.ChannelID,
		PlainSize:      in.PlainSize,
		Checksum:       dbtypes.OptionalText(in.Checksum),
		LeaseToken:     dbtypes.UUID(leaseToken),
		LeaseExpiresAt: dbtypes.Time(s.now().UTC().Add(s.leaseTTL)),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPartBusy
	}
	if err != nil {
		return nil, fmt.Errorf("claim upload part: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit part claim: %w", err)
	}
	return &ClaimPartResult{Part: part, LeaseToken: leaseToken}, nil
}

type StorePartInput struct {
	UploadID            uuid.UUID
	ReceiptID           uuid.UUID
	PartNo              int32
	LeaseToken          uuid.UUID
	MessageID           int64
	StoredSize          int64
	Checksum            string
	Salt                *string
	BlockHashes         []byte
	Guarded             bool
	RecoveryIncarnation uuid.UUID
}

type RenewPartInput struct {
	UploadID            uuid.UUID
	PartNo              int32
	LeaseToken          uuid.UUID
	Guarded             bool
	RecoveryIncarnation uuid.UUID
}

func (s *Service) RenewPart(ctx context.Context, in RenewPartInput) error {
	if in.UploadID == uuid.Nil || in.PartNo <= 0 || in.LeaseToken == uuid.Nil {
		return ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin part renewal: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	session, err := s.lockLeaseSession(ctx, q, in.UploadID)
	if err != nil {
		return err
	}
	if err := s.requireGuardedSession(ctx, q, session, in.Guarded, in.RecoveryIncarnation); err != nil {
		return err
	}
	part, err := q.RenewUploadPartLease(ctx, sqlcgen.RenewUploadPartLeaseParams{
		LeaseExpiresAt: dbtypes.Time(s.now().UTC().Add(s.leaseTTL)),
		UploadID:       dbtypes.UUID(in.UploadID),
		PartNo:         in.PartNo,
		LeaseToken:     dbtypes.UUID(in.LeaseToken),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("renew upload part lease: %w", err)
	}
	if err := validatePartShape(session, part.PartNo, part.PlainSize); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit part renewal: %w", err)
	}
	return nil
}

func (s *Service) StorePart(ctx context.Context, in StorePartInput) (*sqlcgen.UploadPart, error) {
	if in.UploadID == uuid.Nil || in.PartNo <= 0 || in.LeaseToken == uuid.Nil || in.MessageID <= 0 || in.StoredSize <= 0 || (len(in.BlockHashes) > 0 && len(in.BlockHashes)%treehash.DigestSize != 0) {
		return nil, ErrInvalidInput
	}
	if (strings.TrimSpace(in.Checksum) == "") != (len(in.BlockHashes) == 0) {
		return nil, ErrInvalidInput
	}
	var checksum *string
	if strings.TrimSpace(in.Checksum) != "" {
		normalized, err := normalizeDigest(in.Checksum)
		if err != nil {
			return nil, err
		}
		checksum = &normalized
	}
	hint, err := s.GetAnyOwner(ctx, in.UploadID)
	if err != nil {
		return nil, err
	}
	if hint.State != sqlcgen.UploadStateCompleted && hint.TerminalFailureCode.Valid {
		return nil, ErrParentUnavailable
	}
	if checksum != nil && treehash.SumToHex(treehash.ComputeTreeHash(in.BlockHashes)) != *checksum {
		return nil, ErrPartConflict
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin part storage: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	if err := lockUploadLifecycle(ctx, tx, hint.UserID, in.UploadID); err != nil {
		return nil, err
	}
	session, err := s.lockLeaseSession(ctx, q, in.UploadID)
	if err != nil {
		return nil, err
	}
	if err := s.requireGuardedSession(ctx, q, session, in.Guarded, in.RecoveryIncarnation); err != nil {
		return nil, err
	}
	if s.lifecycle == nil || in.ReceiptID == uuid.Nil {
		return nil, ErrLifecycleUnavailable
	}
	claimed, err := q.GetUploadPart(ctx, sqlcgen.GetUploadPartParams{UploadID: dbtypes.UUID(in.UploadID), PartNo: in.PartNo})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrLeaseLost
	}
	if err != nil {
		return nil, fmt.Errorf("get claimed part: %w", err)
	}
	if claimed.State != sqlcgen.UploadPartStateUploading || claimed.LeaseToken != dbtypes.UUID(in.LeaseToken) || !claimed.LeaseExpiresAt.Valid || !claimed.LeaseExpiresAt.Time.After(s.now()) {
		return nil, ErrLeaseLost
	}
	if err := validatePartShape(session, in.PartNo, claimed.PlainSize); err != nil {
		return nil, err
	}
	if claimed.Checksum.Valid && !optionalTextEqual(claimed.Checksum, checksum) {
		return nil, ErrPartConflict
	}
	if checksum != nil && int64(len(in.BlockHashes)/treehash.DigestSize) != expectedPartCount(claimed.PlainSize, treehash.BlockSize) {
		return nil, ErrPartConflict
	}
	if (!session.Encryption && (in.StoredSize != claimed.PlainSize || in.Salt != nil)) || (session.Encryption && (in.StoredSize <= claimed.PlainSize || in.Salt == nil || strings.TrimSpace(*in.Salt) == "")) {
		return nil, ErrInvalidInput
	}
	if err := s.validateDestination(ctx, q, session); err != nil {
		return nil, err
	}
	if err := lifecycle.RequireWritableChannelsTx(ctx, tx, []int64{claimed.ChannelID}); err != nil {
		return nil, err
	}
	if err := lifecycle.RequireBindableMessagesTx(ctx, tx, []lifecycle.PhysicalMessage{{ChannelID: claimed.ChannelID, MessageID: in.MessageID}}); err != nil {
		return nil, err
	}
	receipt, err := q.GetRemoteWriteReceipt(ctx, dbtypes.UUID(in.ReceiptID))
	if err != nil {
		return nil, fmt.Errorf("get storage receipt: %w", err)
	}
	if receipt.UserID != session.UserID || receipt.OriginKind != "upload" || receipt.UploadID != session.ID || receipt.PartNo != dbtypes.Int4(in.PartNo) || receipt.LeaseToken != dbtypes.UUID(in.LeaseToken) || receipt.ChannelID != dbtypes.Int8(claimed.ChannelID) || receipt.MessageID != dbtypes.Int8(in.MessageID) || receipt.State != "observed" {
		return nil, lifecycle.ErrReceiptConflict
	}
	if err := s.lifecycle.BindTx(ctx, tx, lifecycle.BindInput{UserID: session.UserID, ReceiptIDs: []uuid.UUID{in.ReceiptID}}); err != nil {
		return nil, err
	}
	part, err := q.MarkUploadPartStored(ctx, sqlcgen.MarkUploadPartStoredParams{
		ReceiptID:   dbtypes.UUID(in.ReceiptID),
		MessageID:   dbtypes.Int8(in.MessageID),
		StoredSize:  dbtypes.Int8(in.StoredSize),
		Checksum:    dbtypes.OptionalText(checksum),
		Salt:        dbtypes.OptionalText(in.Salt),
		BlockHashes: in.BlockHashes,
		UploadID:    dbtypes.UUID(in.UploadID),
		PartNo:      in.PartNo,
		LeaseToken:  dbtypes.UUID(in.LeaseToken),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrLeaseLost
	}
	if err != nil {
		return nil, fmt.Errorf("store upload part: %w", err)
	}
	if err := s.commit(ctx, tx); err != nil {
		return nil, fmt.Errorf("commit part storage: %w", err)
	}
	return part, nil
}

type FailPartInput struct {
	UploadID            uuid.UUID
	PartNo              int32
	LeaseToken          uuid.UUID
	ErrorCode           string
	ReceiptID           uuid.UUID
	Guarded             bool
	RecoveryIncarnation uuid.UUID
}

// FailPart releases a part lease after a transport or validation failure. The
// lease token prevents a stale uploader from overwriting a newer attempt.
func (s *Service) FailPart(ctx context.Context, in FailPartInput) (*sqlcgen.UploadPart, error) {
	if in.UploadID == uuid.Nil || in.PartNo <= 0 || in.LeaseToken == uuid.Nil || strings.TrimSpace(in.ErrorCode) == "" {
		return nil, ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin part failure: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	if in.ReceiptID != uuid.Nil {
		hint, err := s.GetAnyOwner(ctx, in.UploadID)
		if err != nil {
			return nil, err
		}
		if s.lifecycle == nil {
			return nil, ErrLifecycleUnavailable
		}
		if err := lockUploadLifecycle(ctx, tx, hint.UserID, in.UploadID); err != nil {
			return nil, err
		}
	}
	session, err := q.LockUploadSession(ctx, dbtypes.UUID(in.UploadID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrLeaseLost
	}
	if err != nil {
		return nil, fmt.Errorf("lock failed upload: %w", err)
	}
	if err := s.requireGuardedSession(ctx, q, session, in.Guarded, in.RecoveryIncarnation); err != nil {
		return nil, err
	}
	if session.TerminalFailureCode.Valid && session.TerminalFailureCode.String == "upload_destination_unknown" {
		return nil, ErrParentUnavailable
	}
	if in.ReceiptID == uuid.Nil {
		if err := s.validateOpenSession(session); err != nil {
			return nil, err
		}
	}
	part, err := q.MarkUploadPartFailed(ctx, sqlcgen.MarkUploadPartFailedParams{
		ErrorCode:  dbtypes.Text(strings.TrimSpace(in.ErrorCode)),
		UploadID:   dbtypes.UUID(in.UploadID),
		PartNo:     in.PartNo,
		LeaseToken: dbtypes.UUID(in.LeaseToken),
	})
	lost := errors.Is(err, pgx.ErrNoRows)
	if lost && in.ReceiptID == uuid.Nil {
		return nil, ErrLeaseLost
	}
	if err != nil && !lost {
		return nil, fmt.Errorf("fail upload part: %w", err)
	}
	if !lost {
		if err := validatePartShape(session, part.PartNo, part.PlainSize); err != nil {
			return nil, err
		}
	}
	if in.ReceiptID != uuid.Nil {
		if err := s.lifecycle.StageCleanupTx(ctx, tx, lifecycle.StageCleanupInput{UserID: session.UserID, ReceiptIDs: []uuid.UUID{in.ReceiptID}, PrimaryCode: strings.TrimSpace(in.ErrorCode)}); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit part failure: %w", err)
	}
	if lost {
		return nil, ErrLeaseLost
	}
	return part, nil
}

type CompleteInput struct {
	UserID              int64
	UploadID            uuid.UUID
	Size                int64
	HashAlgorithm       *string
	HashValue           *string
	Guarded             bool
	Admission           *lifecycle.RequestAdmissionInput
	RecoveryIncarnation uuid.UUID
}

func (s *Service) Complete(ctx context.Context, in CompleteInput) (*sqlcgen.File, error) {
	if in.UserID <= 0 || in.UploadID == uuid.Nil || in.Size < 0 || (in.HashAlgorithm == nil) != (in.HashValue == nil) {
		return nil, ErrInvalidInput
	}
	if in.Guarded && (in.Admission == nil || in.RecoveryIncarnation == uuid.Nil || in.HashAlgorithm == nil) {
		return nil, ErrInvalidInput
	}
	if in.HashAlgorithm != nil {
		algorithm, value, err := normalizeExpectedHash(*in.HashAlgorithm, *in.HashValue)
		if err != nil {
			return nil, err
		}
		in.HashAlgorithm, in.HashValue = &algorithm, &value
	}
	userID, uploadID := in.UserID, in.UploadID
	proofSession, err := s.queries.GetUploadSessionForUser(ctx, sqlcgen.GetUploadSessionForUserParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var proofParts []*sqlcgen.UploadPart
	var hashAlgorithm, hashValue *string
	if proofSession.State != sqlcgen.UploadStateCompleted {
		if proofSession.TerminalFailureCode.Valid {
			return nil, ErrParentUnavailable
		}
		if err = s.validateOpenSession(proofSession); err != nil {
			return nil, err
		}
		proofParts, err = s.queries.ListUploadPartsByUploadIDs(ctx, []pgtype.UUID{proofSession.ID})
		if err != nil {
			return nil, err
		}
		hashAlgorithm, hashValue, err = validateStoredParts(proofSession, in, proofParts)
		if err != nil {
			return nil, err
		}
	}
	opts := pgx.TxOptions{}
	if in.Guarded {
		opts.IsoLevel = pgx.ReadCommitted
	}
	tx, err := s.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("begin upload completion: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	if err := lifecycle.LockOwnerTx(ctx, tx, userID); err != nil {
		return nil, err
	}
	var completeAdmissionID uuid.UUID
	if in.Guarded {
		if s.lifecycle == nil {
			return nil, ErrLifecycleUnavailable
		}
		binding, _, err := s.lifecycle.BindRequestAdmissionTx(ctx, tx, userID, "upload", *in.Admission, &uploadID, nil)
		if err != nil {
			return nil, err
		}
		if binding.UploadID != dbtypes.UUID(uploadID) {
			return nil, ErrInvalidState
		}
		completeAdmissionID = uuid.UUID(binding.ID.Bytes)
	}
	hint, err := q.GetUploadSessionForUser(ctx, sqlcgen.GetUploadSessionForUserParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if hint.State != sqlcgen.UploadStateCompleted && hint.TerminalFailureCode.Valid {
		return nil, ErrParentUnavailable
	}
	var replacing []uuid.UUID
	if hint.State != sqlcgen.UploadStateCompleted && hint.Guarded {
		conflict, err := q.GetGuardedUploadBinding(ctx, sqlcgen.GetGuardedUploadBindingParams{UploadID: dbtypes.UUID(uploadID), UserID: userID})
		if err != nil {
			return nil, fmt.Errorf("get guarded binding: %w", err)
		}
		if conflict.ExpectedFileID.Valid {
			ids, e := q.ListLifecycleSubtreeIDs(ctx, sqlcgen.ListLifecycleSubtreeIDsParams{UserID: userID, RootIds: []pgtype.UUID{conflict.ExpectedFileID}})
			if e != nil {
				return nil, e
			}
			for _, id := range ids {
				replacing = append(replacing, uuid.UUID(id.Bytes))
			}
		}
	} else if hint.State != sqlcgen.UploadStateCompleted && hint.ConflictPolicy == sqlcgen.NameConflictPolicyReplace {
		conflict, e := q.ResolveActiveChild(ctx, sqlcgen.ResolveActiveChildParams{UserID: userID, ParentID: hint.ParentID, Name: hint.Name})
		if e == nil {
			ids, e := q.ListLifecycleSubtreeIDs(ctx, sqlcgen.ListLifecycleSubtreeIDsParams{UserID: userID, RootIds: []pgtype.UUID{conflict.ID}})
			if e != nil {
				return nil, e
			}
			for _, id := range ids {
				replacing = append(replacing, uuid.UUID(id.Bytes))
			}
		} else if !errors.Is(e, pgx.ErrNoRows) {
			return nil, e
		}
	}
	if err := lifecycle.LockHierarchyTx(ctx, tx, userID, replacing, []uuid.UUID{uploadID}); err != nil {
		return nil, err
	}

	session, err := q.LockUploadSessionForUser(ctx, sqlcgen.LockUploadSessionForUserParams{
		UploadID: dbtypes.UUID(uploadID),
		UserID:   userID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock upload: %w", err)
	}
	if err := s.requireGuardedSession(ctx, q, session, in.Guarded, in.RecoveryIncarnation); err != nil {
		return nil, err
	}
	if session.State == sqlcgen.UploadStateCompleted {
		if in.Guarded {
			return s.verifyGuardedCompletion(ctx, q, session, in)
		}
		fileID, ok := dbtypes.GoogleUUID(session.FileID)
		if !ok {
			return nil, ErrNotFound
		}
		file, err := q.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{FileID: dbtypes.UUID(fileID), UserID: userID})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("get completed file: %w", err)
		}
		if file.Status != sqlcgen.FileStatusActive {
			return nil, ErrNotFound
		}
		if !file.Size.Valid || file.Size.Int64 != in.Size || (in.HashValue != nil && (!optionalTextEqual(file.HashAlgorithm, in.HashAlgorithm) || !optionalTextEqual(file.HashValue, in.HashValue))) {
			return nil, ErrInvalidState
		}
		return file, nil
	}
	if s.lifecycle == nil {
		return nil, ErrLifecycleUnavailable
	}
	if err := s.validateDestination(ctx, q, session); err != nil {
		return nil, err
	}
	parts, err := q.ListUploadPartsByUploadIDs(ctx, []pgtype.UUID{session.ID})
	if err != nil {
		return nil, err
	}
	channels := make([]int64, 0, len(parts))
	messages := make([]lifecycle.PhysicalMessage, 0, len(parts))
	for _, p := range parts {
		channels = append(channels, p.ChannelID)
		if p.MessageID.Valid {
			messages = append(messages, lifecycle.PhysicalMessage{ChannelID: p.ChannelID, MessageID: p.MessageID.Int64})
		}
	}
	if err := lifecycle.RequireWritableChannelsTx(ctx, tx, channels); err != nil {
		return nil, err
	}
	if err := lifecycle.RequireBindableMessagesTx(ctx, tx, messages); err != nil {
		return nil, err
	}
	if err := s.validateOpenSession(session); err != nil {
		return nil, err
	}
	if session.ExpectedSize != proofSession.ExpectedSize || session.PartSize != proofSession.PartSize || session.Encryption != proofSession.Encryption || session.ExpectedHashAlgorithm != proofSession.ExpectedHashAlgorithm || session.ExpectedHashValue != proofSession.ExpectedHashValue || !slices.EqualFunc(parts, proofParts, samePublicationPart) {
		return nil, ErrIncomplete
	}
	var guardedBinding *sqlcgen.GuardedUploadBinding
	var guardedParentSegments []string
	if in.Guarded {
		guardedBinding, err = s.lockGuardedDestination(ctx, q, session)
		if err != nil {
			return nil, err
		}
		guardedParentSegments, err = s.lockGuardedAncestry(ctx, q, session, guardedBinding)
		if err != nil {
			return nil, err
		}
		if err := s.compareGuardedSlot(ctx, q, session, guardedBinding, true); err != nil {
			return nil, s.holdGuardedConflict(ctx, tx, userID, uploadID, err)
		}
	}
	if session.ExpectedSize < 0 {
		session, err = q.FinalizeUploadExpectedSize(ctx, sqlcgen.FinalizeUploadExpectedSizeParams{
			ExpectedSize: in.Size,
			UploadID:     dbtypes.UUID(uploadID),
			UserID:       userID,
		})
		if err != nil {
			return nil, fmt.Errorf("finalize upload size: %w", err)
		}
	}
	var replacedID *uuid.UUID
	if in.Guarded {
		replacedID, err = s.prepareGuardedConflictPolicy(ctx, tx, session, guardedBinding)
	} else {
		replacedID, err = s.prepareConflictPolicy(ctx, tx, session)
	}
	if err != nil {
		if in.Guarded {
			return nil, s.holdGuardedConflict(ctx, tx, userID, uploadID, err)
		}
		return nil, err
	}
	if _, err := q.MarkUploadCompleting(ctx, sqlcgen.MarkUploadCompletingParams{
		UploadID: dbtypes.UUID(uploadID),
		UserID:   userID,
	}); err != nil {
		return nil, fmt.Errorf("mark upload completing: %w", err)
	}

	fileID := uuid.New()
	file, err := q.InsertFileFromUpload(ctx, sqlcgen.InsertFileFromUploadParams{
		FileID:        dbtypes.UUID(fileID),
		HashAlgorithm: dbtypes.OptionalText(hashAlgorithm),
		HashValue:     dbtypes.OptionalText(hashValue),
		UploadID:      dbtypes.UUID(uploadID),
		UserID:        userID,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			if in.Guarded {
				return nil, s.holdGuardedConflict(ctx, tx, userID, uploadID, ErrGuardConflict)
			}
			return nil, ErrNameConflict
		}
		return nil, fmt.Errorf("create file from upload: %w", err)
	}
	inserted, err := q.InsertFilePartsFromUpload(ctx, sqlcgen.InsertFilePartsFromUploadParams{
		FileID:   dbtypes.UUID(fileID),
		UploadID: dbtypes.UUID(uploadID),
	})
	if err != nil {
		return nil, fmt.Errorf("copy upload parts: %w", err)
	}
	expectedParts := expectedPartCount(session.ExpectedSize, session.PartSize)
	if inserted != expectedParts {
		return nil, fmt.Errorf("copied %d upload parts, expected %d: %w", inserted, expectedParts, ErrIncomplete)
	}
	if in.Guarded {
		if hashAlgorithm == nil || hashValue == nil {
			return nil, s.holdGuardedConflict(ctx, tx, userID, uploadID, ErrIncomplete)
		}
		segments := guardedParentSegments
		if segments == nil {
			segments = []string{}
		}
		if _, err := q.InsertGuardedUploadResult(ctx, sqlcgen.InsertGuardedUploadResultParams{
			UploadID:           dbtypes.UUID(uploadID),
			UserID:             userID,
			RequestAdmissionID: dbtypes.UUID(completeAdmissionID),
			Size:               in.Size,
			HashAlgorithm:      *hashAlgorithm,
			HashValue:          *hashValue,
			FileID:             dbtypes.UUID(fileID),
			Generation:         file.Generation,
			ParentID:           guardedBinding.ParentID,
			Name:               guardedBinding.Name,
			MountRoot:          guardedBinding.MountRoot,
			ParentSegments:     segments,
			ExpectedFileID:     guardedBinding.ExpectedFileID,
			ExpectedGeneration: guardedBinding.ExpectedGeneration,
		}); err != nil {
			return nil, s.holdGuardedConflict(ctx, tx, userID, uploadID, errors.Join(ErrGuardConflict, err))
		}
	}
	if _, err := q.CompleteUploadSession(ctx, sqlcgen.CompleteUploadSessionParams{
		FileID:   dbtypes.UUID(fileID),
		UploadID: dbtypes.UUID(uploadID),
		UserID:   userID,
	}); err != nil {
		if in.Guarded {
			return nil, s.holdGuardedConflict(ctx, tx, userID, uploadID, errors.Join(ErrGuardConflict, err))
		}
		return nil, fmt.Errorf("complete upload session: %w", err)
	}
	owned, err := q.ListUploadPublicationReceipts(ctx, sqlcgen.ListUploadPublicationReceiptsParams{UserID: userID, UploadID: session.ID})
	if err != nil {
		return nil, err
	}
	captured, err := q.CaptureCompletedUploadPartOrigins(ctx, sqlcgen.CaptureCompletedUploadPartOriginsParams{UserID: userID, UploadID: session.ID, FileID: dbtypes.UUID(fileID)})
	if err != nil {
		return nil, fmt.Errorf("capture completed upload origins: %w", err)
	}
	retired, err := q.RetireCompletedUploadPartAliases(ctx, sqlcgen.RetireCompletedUploadPartAliasesParams{UserID: userID, UploadID: session.ID, FileID: dbtypes.UUID(fileID)})
	if err != nil {
		return nil, fmt.Errorf("retire completed upload aliases: %w", err)
	}
	if captured != int64(len(owned)) || retired != captured {
		return nil, fmt.Errorf("completed origin transfer count: %w", ErrIncomplete)
	}
	if err := s.commit(ctx, tx); err != nil {
		return nil, fmt.Errorf("commit upload completion: %w", err)
	}
	if replacedID != nil {
		if s.catalogInvalidator != nil {
			s.catalogInvalidator.InvalidateFiles(ctx, userID, *replacedID)
		}
	}
	return file, nil
}

type GuardedPartOrigin struct {
	ParentSegments []string
}

// VerifyGuardedPart proves the admitted destination before a part body is read.
// The proof is read-only; the caller keeps no transaction across Telegram I/O.
func (s *Service) VerifyGuardedPart(ctx context.Context, userID int64, uploadID, incarnation uuid.UUID) (*GuardedPartOrigin, error) {
	if userID <= 0 || uploadID == uuid.Nil || incarnation == uuid.Nil {
		return nil, ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin guarded part verification: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	session, err := q.GetUploadSessionForUser(ctx, sqlcgen.GetUploadSessionForUserParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := s.requireGuardedSession(ctx, q, session, true, incarnation); err != nil {
		return nil, err
	}
	if err := s.validateOpenSession(session); err != nil {
		return nil, err
	}
	binding, err := q.GetGuardedUploadBinding(ctx, sqlcgen.GetGuardedUploadBindingParams{UploadID: dbtypes.UUID(uploadID), UserID: userID})
	if err != nil {
		return nil, fmt.Errorf("get guarded binding: %w", err)
	}
	segments, _, err := s.guardedAncestry(ctx, q, userID, session.ParentID, binding.MountRoot)
	if err != nil {
		_ = s.recordGuardedConflict(ctx, userID, uploadID)
		return nil, err
	}
	if err := s.compareGuardedSlot(ctx, q, session, binding, false); err != nil {
		_ = s.recordGuardedConflict(ctx, userID, uploadID)
		return nil, err
	}
	return &GuardedPartOrigin{ParentSegments: segments}, nil
}

type GuardedReceipt struct {
	AdmissionKey  uuid.UUID
	UploadID      uuid.UUID
	Size          int64
	HashAlgorithm string
	HashValue     string
	File          *sqlcgen.File
	Generation    int64
	// ParentSegments is nil when the current location is unproved or outside
	// the pinned root; a proved root destination is an empty, non-nil slice.
	ParentSegments []string
}

type GuardedUpload struct {
	Session *sqlcgen.UploadSession
	Receipt *GuardedReceipt
}

// GetGuarded returns the session and, after completion, the exact publication
// receipt from one read-only Repeatable Read snapshot.
func (s *Service) GetGuarded(ctx context.Context, userID int64, uploadID, incarnation uuid.UUID) (*GuardedUpload, error) {
	if userID <= 0 || uploadID == uuid.Nil || incarnation == uuid.Nil {
		return nil, ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin guarded status: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	session, err := q.GetUploadSessionForUser(ctx, sqlcgen.GetUploadSessionForUserParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := s.requireGuardedSession(ctx, q, session, true, incarnation); err != nil {
		return nil, err
	}
	binding, err := q.GetGuardedUploadBinding(ctx, sqlcgen.GetGuardedUploadBindingParams{UploadID: dbtypes.UUID(uploadID), UserID: userID})
	if err != nil {
		return nil, fmt.Errorf("get guarded binding: %w", err)
	}
	if session.State != sqlcgen.UploadStateCompleted {
		if err := s.validateOpenSession(session); err != nil {
			return nil, err
		}
		if _, _, err := s.guardedAncestry(ctx, q, userID, session.ParentID, binding.MountRoot); err != nil {
			return nil, err
		}
		if err := s.compareGuardedSlot(ctx, q, session, binding, false); err != nil {
			return nil, err
		}
		return &GuardedUpload{Session: session}, nil
	}
	result, err := q.GetGuardedUploadResult(ctx, sqlcgen.GetGuardedUploadResultParams{UploadID: dbtypes.UUID(uploadID), UserID: userID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get guarded receipt: %w", err)
	}
	fileID, ok := dbtypes.GoogleUUID(session.FileID)
	if !ok || result.FileID != session.FileID {
		return nil, ErrNotFound
	}
	file, err := q.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{FileID: dbtypes.UUID(fileID), UserID: userID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if file.Status != sqlcgen.FileStatusActive || !file.Size.Valid || file.Size.Int64 != result.Size ||
		result.HashAlgorithm != string(treehash.TypeBlake3) || result.HashValue == "" {
		return nil, ErrNotFound
	}
	segments, _, err := s.guardedAncestry(ctx, q, userID, file.ParentID, binding.MountRoot)
	if err != nil {
		segments = nil
	}
	return &GuardedUpload{Session: session, Receipt: &GuardedReceipt{
		AdmissionKey:   uuid.UUID(result.AdmissionKey.Bytes),
		UploadID:       uploadID,
		Size:           result.Size,
		HashAlgorithm:  result.HashAlgorithm,
		HashValue:      result.HashValue,
		File:           file,
		Generation:     file.Generation,
		ParentSegments: segments,
	}}, nil
}

func (s *Service) validateGuardedCreate(ctx context.Context, q *sqlcgen.Queries, in CreateInput) error {
	current, err := q.GetRecoveryIncarnation(ctx)
	if err != nil {
		return fmt.Errorf("get recovery incarnation: %w", err)
	}
	incarnation, ok := dbtypes.GoogleUUID(current)
	if !ok || incarnation != in.Guard.RecoveryIncarnation {
		return ErrRecoveryChanged
	}
	if _, _, err := s.guardedAncestry(ctx, q, in.UserID, dbtypes.OptionalUUID(in.ParentID), dbtypes.OptionalUUID(in.Guard.MountRoot)); err != nil {
		return err
	}
	return s.compareGuardedCreateSlot(ctx, q, in)
}

func (s *Service) compareGuardedCreateSlot(ctx context.Context, q *sqlcgen.Queries, in CreateInput) error {
	if in.Guard.ExpectedFileID == nil {
		_, err := q.ResolveActiveChild(ctx, sqlcgen.ResolveActiveChildParams{UserID: in.UserID, ParentID: dbtypes.OptionalUUID(in.ParentID), Name: in.Name})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		return ErrGuardConflict
	}
	file, err := q.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{FileID: dbtypes.UUID(*in.Guard.ExpectedFileID), UserID: in.UserID})
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrGuardConflict
	}
	if err != nil {
		return err
	}
	if file.Status != sqlcgen.FileStatusActive || file.Kind != sqlcgen.FileKindFile ||
		file.ParentID != dbtypes.OptionalUUID(in.ParentID) || file.Name != in.Name ||
		file.Generation != *in.Guard.ExpectedGeneration {
		return ErrGuardConflict
	}
	return nil
}

func (s *Service) verifyGuardedBinding(ctx context.Context, q *sqlcgen.Queries, session *sqlcgen.UploadSession, in CreateInput) error {
	if !session.Guarded {
		return ErrGuardedRouteRequired
	}
	binding, err := q.GetGuardedUploadBinding(ctx, sqlcgen.GetGuardedUploadBindingParams{UploadID: session.ID, UserID: in.UserID})
	if err != nil {
		return fmt.Errorf("get guarded binding: %w", err)
	}
	if binding.ParentID != session.ParentID || binding.Name != session.Name ||
		session.ParentID != dbtypes.OptionalUUID(in.ParentID) || session.Name != in.Name ||
		binding.MountRoot != dbtypes.OptionalUUID(in.Guard.MountRoot) ||
		binding.ExpectedFileID != dbtypes.OptionalUUID(in.Guard.ExpectedFileID) ||
		binding.ExpectedGeneration != dbtypes.OptionalInt8(in.Guard.ExpectedGeneration) ||
		binding.RequestKey != dbtypes.UUID(in.Admission.RequestKey) {
		return ErrGuardConflict
	}
	current, err := q.GetRecoveryIncarnation(ctx)
	if err != nil {
		return fmt.Errorf("get recovery incarnation: %w", err)
	}
	if incarnation, ok := dbtypes.GoogleUUID(current); !ok || incarnation != uuid.UUID(binding.RecoveryIncarnation.Bytes) {
		return ErrRecoveryChanged
	}
	if session.GuardConflictCode.Valid {
		return ErrGuardConflict
	}
	return nil
}

func (s *Service) requireGuardedSession(ctx context.Context, q *sqlcgen.Queries, session *sqlcgen.UploadSession, guarded bool, incarnation uuid.UUID) error {
	if !session.Guarded {
		if guarded {
			return ErrGuardedRouteRequired
		}
		return nil
	}
	if !guarded || incarnation == uuid.Nil {
		return ErrGuardedRouteRequired
	}
	if session.GuardConflictCode.Valid {
		return ErrGuardConflict
	}
	binding, err := q.GetGuardedUploadBinding(ctx, sqlcgen.GetGuardedUploadBindingParams{UploadID: session.ID, UserID: session.UserID})
	if err != nil {
		return fmt.Errorf("get guarded binding: %w", err)
	}
	current, err := q.GetRecoveryIncarnation(ctx)
	if err != nil {
		return fmt.Errorf("get recovery incarnation: %w", err)
	}
	currentIncarnation, ok := dbtypes.GoogleUUID(current)
	if !ok || currentIncarnation != incarnation || currentIncarnation != uuid.UUID(binding.RecoveryIncarnation.Bytes) {
		return ErrRecoveryChanged
	}
	return nil
}

func (s *Service) lockGuardedDestination(ctx context.Context, q *sqlcgen.Queries, session *sqlcgen.UploadSession) (*sqlcgen.GuardedUploadBinding, error) {
	binding, err := q.GetGuardedUploadBinding(ctx, sqlcgen.GetGuardedUploadBindingParams{UploadID: session.ID, UserID: session.UserID})
	if err != nil {
		return nil, fmt.Errorf("get guarded binding: %w", err)
	}
	if binding.ParentID != session.ParentID || binding.Name != session.Name {
		return nil, ErrGuardConflict
	}
	return binding, nil
}

func (s *Service) lockGuardedAncestry(ctx context.Context, q *sqlcgen.Queries, session *sqlcgen.UploadSession, binding *sqlcgen.GuardedUploadBinding) ([]string, error) {
	segments, chain, err := s.guardedAncestry(ctx, q, session.UserID, session.ParentID, binding.MountRoot)
	if err != nil {
		return nil, err
	}
	if len(chain) > 0 {
		if err := q.LockCatalogRowsForLifecycle(ctx, sqlcgen.LockCatalogRowsForLifecycleParams{UserID: session.UserID, FileIds: chain}); err != nil {
			return nil, err
		}
	}
	return segments, nil
}

func (s *Service) guardedAncestry(ctx context.Context, q *sqlcgen.Queries, userID int64, parentID, mountRoot pgtype.UUID) ([]string, []pgtype.UUID, error) {
	if !parentID.Valid {
		if mountRoot.Valid {
			return nil, nil, ErrGuardConflict
		}
		return []string{}, nil, nil
	}
	chain, err := q.ListActiveAncestorChain(ctx, sqlcgen.ListActiveAncestorChainParams{FileID: parentID, UserID: userID})
	if err != nil {
		return nil, nil, fmt.Errorf("list guarded ancestry: %w", err)
	}
	if len(chain) == 0 {
		return nil, nil, ErrGuardConflict
	}
	ids := make([]pgtype.UUID, len(chain))
	for i, row := range chain {
		if row.Depth != int32(i) {
			return nil, nil, ErrGuardConflict
		}
		ids[i] = row.ID
	}
	limit, root := len(chain), -1
	if mountRoot.Valid {
		rootID := uuid.UUID(mountRoot.Bytes)
		for i, row := range chain {
			if uuid.UUID(row.ID.Bytes) == rootID {
				root = i
				break
			}
		}
		if root < 0 {
			return nil, nil, ErrGuardConflict
		}
		limit = root
	} else if chain[len(chain)-1].ParentID.Valid {
		return nil, nil, ErrGuardConflict
	}
	segments := make([]string, 0, limit)
	for i := limit - 1; i >= 0; i-- {
		segments = append(segments, chain[i].Name)
	}
	return segments, ids, nil
}

func (s *Service) compareGuardedSlot(ctx context.Context, q *sqlcgen.Queries, session *sqlcgen.UploadSession, binding *sqlcgen.GuardedUploadBinding, lock bool) error {
	if !binding.ExpectedFileID.Valid {
		var (
			file *sqlcgen.File
			err  error
		)
		if lock {
			file, err = q.LockUploadDestinationConflict(ctx, sqlcgen.LockUploadDestinationConflictParams{UserID: session.UserID, ParentID: binding.ParentID, Name: binding.Name})
		} else {
			file, err = q.ResolveActiveChild(ctx, sqlcgen.ResolveActiveChildParams{UserID: session.UserID, ParentID: binding.ParentID, Name: binding.Name})
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if file != nil {
			return ErrGuardConflict
		}
		return nil
	}
	var (
		file *sqlcgen.File
		err  error
	)
	if lock {
		file, err = q.LockGuardedExpectedFile(ctx, sqlcgen.LockGuardedExpectedFileParams{FileID: binding.ExpectedFileID, UserID: session.UserID})
	} else {
		file, err = q.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{FileID: binding.ExpectedFileID, UserID: session.UserID})
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrGuardConflict
	}
	if err != nil {
		return err
	}
	if file.Status != sqlcgen.FileStatusActive || file.Kind != sqlcgen.FileKindFile ||
		file.ParentID != binding.ParentID || file.Name != binding.Name ||
		file.Generation != binding.ExpectedGeneration.Int64 {
		return ErrGuardConflict
	}
	return nil
}

func (s *Service) prepareGuardedConflictPolicy(ctx context.Context, tx pgx.Tx, session *sqlcgen.UploadSession, binding *sqlcgen.GuardedUploadBinding) (*uuid.UUID, error) {
	queries := sqlcgen.New(tx)
	if err := queries.AcquireAdvisoryTransactionLock(ctx, uploadDestinationLockID(session)); err != nil {
		return nil, fmt.Errorf("lock upload destination: %w", err)
	}
	if err := s.compareGuardedSlot(ctx, queries, session, binding, true); err != nil {
		return nil, err
	}
	if !binding.ExpectedFileID.Valid {
		return nil, nil
	}
	expectedID := uuid.UUID(binding.ExpectedFileID.Bytes)
	if s.lifecycle == nil {
		return nil, ErrLifecycleUnavailable
	}
	if err := s.lifecycle.TerminalizeDestinationsTx(ctx, tx, session.UserID, []uuid.UUID{expectedID}); err != nil {
		return nil, err
	}
	count, err := queries.MarkActiveFileDeletionPendingForReplace(ctx, sqlcgen.MarkActiveFileDeletionPendingForReplaceParams{FileID: binding.ExpectedFileID, UserID: session.UserID})
	if err != nil {
		return nil, fmt.Errorf("mark guarded file for cleanup: %w", err)
	}
	if count != 1 {
		return nil, ErrGuardConflict
	}
	if err := queries.RevokeActiveSharesForFile(ctx, sqlcgen.RevokeActiveSharesForFileParams{FileID: binding.ExpectedFileID, UserID: session.UserID}); err != nil {
		return nil, fmt.Errorf("revoke guarded file shares: %w", err)
	}
	return &expectedID, nil
}

func (s *Service) holdGuardedConflict(ctx context.Context, tx pgx.Tx, userID int64, uploadID uuid.UUID, cause error) error {
	_ = tx.Rollback(context.WithoutCancel(ctx))
	if err := s.recordGuardedConflict(ctx, userID, uploadID); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (s *Service) recordGuardedConflict(ctx context.Context, userID int64, uploadID uuid.UUID) error {
	holdCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(holdCtx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin guarded conflict hold: %w", err)
	}
	defer tx.Rollback(holdCtx)
	q := s.queries.WithTx(tx)
	if _, err := q.SetUploadGuardConflict(holdCtx, sqlcgen.SetUploadGuardConflictParams{
		ConflictCode: dbtypes.Text("destination_changed"), UploadID: dbtypes.UUID(uploadID), UserID: userID,
	}); err != nil {
		return fmt.Errorf("record guarded conflict: %w", err)
	}
	if err := tx.Commit(holdCtx); err != nil {
		return fmt.Errorf("commit guarded conflict hold: %w", err)
	}
	return nil
}

func (s *Service) verifyGuardedCompletion(ctx context.Context, q *sqlcgen.Queries, session *sqlcgen.UploadSession, in CompleteInput) (*sqlcgen.File, error) {
	result, err := q.GetGuardedUploadResult(ctx, sqlcgen.GetGuardedUploadResultParams{UploadID: session.ID, UserID: session.UserID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get guarded receipt: %w", err)
	}
	fileID, ok := dbtypes.GoogleUUID(session.FileID)
	if !ok || result.FileID != session.FileID {
		return nil, ErrNotFound
	}
	file, err := q.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{FileID: dbtypes.UUID(fileID), UserID: session.UserID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get guarded file: %w", err)
	}
	if file.Status != sqlcgen.FileStatusActive || !file.Size.Valid || file.Size.Int64 != result.Size ||
		result.Size != in.Size || in.HashAlgorithm == nil || in.HashValue == nil ||
		result.HashAlgorithm != *in.HashAlgorithm || result.HashValue != *in.HashValue ||
		!file.HashAlgorithm.Valid || file.HashAlgorithm.String != result.HashAlgorithm ||
		!file.HashValue.Valid || file.HashValue.String != result.HashValue {
		return nil, ErrInvalidState
	}
	return file, nil
}

func (s *Service) prepareConflictPolicy(ctx context.Context, tx pgx.Tx, session *sqlcgen.UploadSession) (*uuid.UUID, error) {
	if session == nil {
		return nil, ErrInvalidInput
	}
	queries := sqlcgen.New(tx)
	if session.ConflictPolicy == sqlcgen.NameConflictPolicyReplace {
		conflict, err := queries.ResolveActiveChild(ctx, sqlcgen.ResolveActiveChildParams{UserID: session.UserID, ParentID: session.ParentID, Name: session.Name})
		if err == nil {
			if conflict.Kind != sqlcgen.FileKindFile {
				return nil, ErrNameConflict
			}
			if s.lifecycle == nil {
				return nil, ErrLifecycleUnavailable
			}
			ids, e := queries.ListLifecycleSubtreeIDs(ctx, sqlcgen.ListLifecycleSubtreeIDsParams{UserID: session.UserID, RootIds: []pgtype.UUID{conflict.ID}})
			if e != nil {
				return nil, e
			}
			subtree := make([]uuid.UUID, len(ids))
			for i, id := range ids {
				subtree[i] = uuid.UUID(id.Bytes)
			}
			if e = s.lifecycle.TerminalizeDestinationsTx(ctx, tx, session.UserID, subtree); e != nil {
				return nil, e
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
	}
	if err := queries.AcquireAdvisoryTransactionLock(ctx, uploadDestinationLockID(session)); err != nil {
		return nil, fmt.Errorf("lock upload destination: %w", err)
	}

	existing, err := queries.LockUploadDestinationConflict(ctx, sqlcgen.LockUploadDestinationConflictParams{
		UserID: session.UserID, ParentID: session.ParentID, Name: session.Name,
	})
	hasConflict := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("check upload destination conflict: %w", err)
	}

	switch session.ConflictPolicy {
	case sqlcgen.NameConflictPolicyFail:
		if hasConflict {
			return nil, ErrNameConflict
		}
		return nil, nil
	case sqlcgen.NameConflictPolicyReplace:
		if !hasConflict {
			return nil, nil
		}
		if existing.Kind != sqlcgen.FileKindFile {
			return nil, ErrNameConflict
		}
		existingID, ok := dbtypes.GoogleUUID(existing.ID)
		if !ok {
			return nil, ErrNameConflict
		}
		count, err := queries.MarkActiveFileDeletionPendingForReplace(ctx, sqlcgen.MarkActiveFileDeletionPendingForReplaceParams{
			FileID: dbtypes.UUID(existingID), UserID: session.UserID,
		})
		if err != nil {
			return nil, fmt.Errorf("mark replaced file for cleanup: %w", err)
		}
		if count != 1 {
			return nil, ErrNameConflict
		}
		if err := queries.RevokeActiveSharesForFile(ctx, sqlcgen.RevokeActiveSharesForFileParams{
			FileID: dbtypes.UUID(existingID), UserID: session.UserID,
		}); err != nil {
			return nil, fmt.Errorf("revoke replaced file shares: %w", err)
		}
		return &existingID, nil
	case sqlcgen.NameConflictPolicyRename:
		if !hasConflict {
			return nil, nil
		}
		name, err := nextAvailableUploadName(ctx, queries, session)
		if err != nil {
			return nil, err
		}
		count, err := queries.RenameUploadSession(ctx, sqlcgen.RenameUploadSessionParams{
			Name: name, UploadID: session.ID, UserID: session.UserID,
		})
		if err != nil {
			return nil, fmt.Errorf("rename upload destination: %w", err)
		}
		if count != 1 {
			return nil, ErrInvalidState
		}
		session.Name = name
		return nil, nil
	default:
		return nil, ErrInvalidInput
	}
}

func nextAvailableUploadName(ctx context.Context, queries *sqlcgen.Queries, session *sqlcgen.UploadSession) (string, error) {
	names, err := queries.ListActiveNames(ctx, sqlcgen.ListActiveNamesParams{
		UserID: session.UserID, ParentID: session.ParentID,
	})
	if err != nil {
		return "", fmt.Errorf("list upload destination names: %w", err)
	}
	used := make(map[string]struct{}, len(names))
	for _, normalized := range names {
		used[normalized] = struct{}{}
	}

	base, extension := splitUploadName(session.Name)
	for sequence := 1; sequence <= 10000; sequence++ {
		candidate := base + fmt.Sprintf(" (%d)", sequence) + extension
		if _, exists := used[candidate]; !exists {
			return candidate, nil
		}
	}
	return "", ErrNameConflict
}

func splitUploadName(name string) (string, string) {
	index := strings.LastIndex(name, ".")
	if index <= 0 || index == len(name)-1 {
		return name, ""
	}
	return name[:index], name[index:]
}

func uploadDestinationLockID(session *sqlcgen.UploadSession) int64 {
	input := make([]byte, 0, 8+16+len("teldrive/upload-destination/"))
	input = append(input, []byte("teldrive/upload-destination/")...)
	var user [8]byte
	binary.BigEndian.PutUint64(user[:], uint64(session.UserID))
	input = append(input, user[:]...)
	if session.ParentID.Valid {
		input = append(input, session.ParentID.Bytes[:]...)
	} else {
		input = append(input, make([]byte, 16)...)
	}
	digest := sha256.Sum256(input)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func (s *Service) Abort(ctx context.Context, userID int64, uploadID uuid.UUID) (*sqlcgen.UploadSession, error) {
	if userID <= 0 || uploadID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin upload abort: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	session, err := q.LockUploadSessionForUser(ctx, sqlcgen.LockUploadSessionForUserParams{UploadID: dbtypes.UUID(uploadID), UserID: userID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock upload for abort: %w", err)
	}
	if session.TerminalFailureCode.Valid && session.TerminalFailureCode.String == "upload_destination_unknown" {
		return nil, ErrParentUnavailable
	}
	switch session.State {
	case sqlcgen.UploadStateAborted, sqlcgen.UploadStateExpired:
		return session, nil
	}
	if err := s.validateOpenSession(session); err != nil {
		return nil, err
	}
	aborted, err := q.AbortUploadSession(ctx, sqlcgen.AbortUploadSessionParams{
		UploadID: dbtypes.UUID(uploadID), UserID: userID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidState
	}
	if err != nil {
		return nil, fmt.Errorf("abort upload: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit upload abort: %w", err)
	}
	return aborted, nil
}

func negotiatePartSize(preference, size int64) (int64, error) {
	if preference < 0 || size < -1 {
		return 0, ErrInvalidInput
	}
	if preference == 0 {
		preference = defaultPartSize
	}
	if remainder := preference % treehash.BlockSize; remainder != 0 {
		padding := treehash.BlockSize - remainder
		if preference > math.MaxInt64-padding {
			return 0, ErrInvalidInput
		}
		preference += padding
	}
	if size >= 0 && expectedPartCount(size, preference) > math.MaxInt32 {
		return 0, ErrInvalidInput
	}
	return preference, nil
}

func validateSessionGeometry(session *sqlcgen.UploadSession) error {
	if session.PartSize <= 0 || session.PartSize%treehash.BlockSize != 0 || session.ExpectedSize < -1 || (session.ExpectedSize >= 0 && expectedPartCount(session.ExpectedSize, session.PartSize) > math.MaxInt32) {
		return ErrInvalidInput
	}
	return nil
}

func (s *Service) validateOpenSession(session *sqlcgen.UploadSession) error {
	if session.TerminalFailureCode.Valid {
		return ErrParentUnavailable
	}
	if session.State != sqlcgen.UploadStateOpen {
		return ErrInvalidState
	}
	if !session.ExpiresAt.Time.After(s.now()) {
		return ErrExpired
	}
	return validateSessionGeometry(session)
}

func (s *Service) lockLeaseSession(ctx context.Context, q *sqlcgen.Queries, uploadID uuid.UUID) (*sqlcgen.UploadSession, error) {
	session, err := q.LockUploadSession(ctx, dbtypes.UUID(uploadID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrLeaseLost
	}
	if err != nil {
		return nil, fmt.Errorf("lock leased upload: %w", err)
	}
	if err := s.validateOpenSession(session); err != nil {
		return nil, err
	}
	return session, nil
}

func validatePartShape(session *sqlcgen.UploadSession, partNo int32, plainSize int64) error {
	if err := validateSessionGeometry(session); err != nil {
		return err
	}
	if partNo <= 0 || plainSize <= 0 || plainSize > session.PartSize || int64(partNo-1) > math.MaxInt64/session.PartSize {
		return ErrInvalidInput
	}
	offset := int64(partNo-1) * session.PartSize
	if plainSize > math.MaxInt64-offset {
		return ErrInvalidInput
	}
	if session.ExpectedSize < 0 {
		return nil
	}
	if offset >= session.ExpectedSize || plainSize != min(session.PartSize, session.ExpectedSize-offset) {
		return ErrInvalidInput
	}
	return nil
}

func validPartProof(part *sqlcgen.UploadPart) bool {
	if !part.Checksum.Valid || part.PlainSize <= 0 || int64(len(part.BlockHashes)/treehash.DigestSize) != expectedPartCount(part.PlainSize, treehash.BlockSize) || len(part.BlockHashes)%treehash.DigestSize != 0 {
		return false
	}
	return treehash.SumToHex(treehash.ComputeTreeHash(part.BlockHashes)) == part.Checksum.String
}

// validateStoredParts hashes only the immutable metadata snapshot, before SQL publication begins.
func validateStoredParts(session *sqlcgen.UploadSession, in CompleteInput, parts []*sqlcgen.UploadPart) (*string, *string, error) {
	expected := expectedPartCount(in.Size, session.PartSize)
	if expected > math.MaxInt32 {
		return nil, nil, ErrInvalidInput
	}
	if session.ExpectedSize >= 0 && session.ExpectedSize != in.Size {
		return nil, nil, ErrIncomplete
	}
	if int64(len(parts)) != expected {
		return nil, nil, ErrIncomplete
	}
	root := blake3.New()
	sawHashed, sawUnhashed := false, false
	for i, part := range parts {
		number := int64(i) + 1
		if int64(part.PartNo) != number || part.State != sqlcgen.UploadPartStateStored || part.PlainSize != min(session.PartSize, in.Size-(number-1)*session.PartSize) || !part.MessageID.Valid || part.MessageID.Int64 <= 0 || !part.StoredSize.Valid || part.StoredSize.Int64 <= 0 {
			return nil, nil, ErrIncomplete
		}
		if (!session.Encryption && (part.StoredSize.Int64 != part.PlainSize || part.Salt.Valid)) || (session.Encryption && (part.StoredSize.Int64 <= part.PlainSize || !part.Salt.Valid || strings.TrimSpace(part.Salt.String) == "")) {
			return nil, nil, ErrIncomplete
		}
		if !part.Checksum.Valid && len(part.BlockHashes) == 0 {
			sawUnhashed = true
		} else {
			if !validPartProof(part) {
				return nil, nil, ErrIncomplete
			}
			sawHashed = true
			_, _ = root.Write(part.BlockHashes)
		}
	}
	if (sawHashed && sawUnhashed) || (sawUnhashed && (session.ExpectedHashAlgorithm.Valid || in.HashAlgorithm != nil)) {
		return nil, nil, ErrIncomplete
	}
	if sawUnhashed {
		return nil, nil, nil
	}
	algorithm, value := string(treehash.TypeBlake3), hex.EncodeToString(root.Sum(nil))
	if session.ExpectedHashAlgorithm.Valid && (session.ExpectedHashAlgorithm.String != algorithm || !session.ExpectedHashValue.Valid || session.ExpectedHashValue.String != value) {
		return nil, nil, ErrHashMismatch
	}
	if in.HashAlgorithm != nil && (*in.HashAlgorithm != algorithm || *in.HashValue != value) {
		return nil, nil, ErrHashMismatch
	}
	return &algorithm, &value, nil
}

func samePublicationPart(a, b *sqlcgen.UploadPart) bool {
	return a.UploadID == b.UploadID && a.PartNo == b.PartNo && a.ChannelID == b.ChannelID && a.MessageID == b.MessageID && a.PlainSize == b.PlainSize && a.StoredSize == b.StoredSize && a.Checksum == b.Checksum && a.Salt == b.Salt && a.State == b.State && a.CreatedAt == b.CreatedAt && a.UpdatedAt == b.UpdatedAt && slices.Equal(a.BlockHashes, b.BlockHashes)
}

func expectedPartCount(size, partSize int64) int64 {
	count := size / partSize
	if size%partSize != 0 {
		count++
	}
	return count
}

func normalizeExpectedHash(algorithm, value string) (string, string, error) {
	algorithm = strings.ToLower(strings.TrimSpace(algorithm))
	if algorithm != string(treehash.TypeBlake3) {
		return "", "", ErrInvalidInput
	}
	normalized, err := normalizeDigest(value)
	if err != nil {
		return "", "", err
	}
	return algorithm, normalized, nil
}

func normalizeDigest(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	digest, err := hex.DecodeString(value)
	if err != nil || len(digest) != treehash.DigestSize {
		return "", ErrInvalidInput
	}
	return value, nil
}

func optionalTextEqual(stored pgtype.Text, expected *string) bool {
	if expected == nil {
		return !stored.Valid
	}
	return stored.Valid && stored.String == *expected
}

func lockUploadLifecycle(ctx context.Context, tx pgx.Tx, userID int64, uploadID uuid.UUID) error {
	if err := lifecycle.LockOwnerTx(ctx, tx, userID); err != nil {
		return err
	}
	q := sqlcgen.New(tx)
	if err := q.LockUploadOriginBatch(ctx, sqlcgen.LockUploadOriginBatchParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)}); err != nil {
		return err
	}
	if err := q.LockUploadOriginPins(ctx, sqlcgen.LockUploadOriginPinsParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)}); err != nil {
		return err
	}
	return q.LockUploadOriginAttempt(ctx, sqlcgen.LockUploadOriginAttemptParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)})
}
func lockCreateImport(ctx context.Context, q *sqlcgen.Queries, userID int64, id uuid.UUID) (*sqlcgen.ImportJobAttempt, error) {
	batch, err := q.LockUploadCreateImportBatch(ctx, sqlcgen.LockUploadCreateImportBatchParams{UserID: userID, AttemptID: dbtypes.UUID(id)})
	if err != nil {
		return nil, errors.Join(ErrParentUnavailable, err)
	}
	if err := q.LockUploadCreateImportPins(ctx, sqlcgen.LockUploadCreateImportPinsParams{UserID: userID, AttemptID: dbtypes.UUID(id)}); err != nil {
		return nil, err
	}
	attempt, err := q.LockUploadCreateImportAttempt(ctx, sqlcgen.LockUploadCreateImportAttemptParams{UserID: userID, AttemptID: dbtypes.UUID(id)})
	if err != nil {
		return nil, errors.Join(ErrParentUnavailable, err)
	}
	if attempt.TerminalFailureCode.Valid {
		return nil, ErrParentUnavailable
	}
	if !batch.Dispatched {
		return nil, ErrInvalidState
	}
	if batch.TerminalFailureCode.Valid && !attempt.ResultFileID.Valid {
		if !attempt.UploadID.Valid {
			return nil, ErrParentUnavailable
		}
		session, e := q.GetUploadSessionForUser(ctx, sqlcgen.GetUploadSessionForUserParams{UserID: userID, UploadID: attempt.UploadID})
		if e != nil || session.State != sqlcgen.UploadStateCompleted {
			return nil, errors.Join(ErrParentUnavailable, e)
		}
	}
	return attempt, nil
}
func (s *Service) getRetainedSession(ctx context.Context, q *sqlcgen.Queries, userID int64, id uuid.UUID) (*sqlcgen.UploadSession, error) {
	if err := q.LockUploadOriginBatch(ctx, sqlcgen.LockUploadOriginBatchParams{UserID: userID, UploadID: dbtypes.UUID(id)}); err != nil {
		return nil, err
	}
	if err := q.LockUploadOriginPins(ctx, sqlcgen.LockUploadOriginPinsParams{UserID: userID, UploadID: dbtypes.UUID(id)}); err != nil {
		return nil, err
	}
	if err := q.LockUploadOriginAttempt(ctx, sqlcgen.LockUploadOriginAttemptParams{UserID: userID, UploadID: dbtypes.UUID(id)}); err != nil {
		return nil, err
	}
	session, err := q.LockUploadSessionForUser(ctx, sqlcgen.LockUploadSessionForUserParams{UserID: userID, UploadID: dbtypes.UUID(id)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.retainedSession(ctx, q, session)
}
func (s *Service) retainedSession(ctx context.Context, q *sqlcgen.Queries, session *sqlcgen.UploadSession) (*sqlcgen.UploadSession, error) {
	if err := s.validateDestination(ctx, q, session); err != nil {
		return nil, err
	}
	if session.State != sqlcgen.UploadStateCompleted {
		if err := s.validateOpenSession(session); err != nil {
			return nil, err
		}
	}
	return session, nil
}
func (s *Service) validateDestination(ctx context.Context, q *sqlcgen.Queries, session *sqlcgen.UploadSession) error {
	if session.State == sqlcgen.UploadStateCompleted {
		if !session.FileID.Valid {
			return ErrNotFound
		}
		file, err := q.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{UserID: session.UserID, FileID: session.FileID})
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if file.Status != sqlcgen.FileStatusActive {
			return ErrNotFound
		}
		if file.ParentID.Valid {
			active, err := q.IsLifecycleDestinationActive(ctx, sqlcgen.IsLifecycleDestinationActiveParams{ParentID: file.ParentID, UserID: session.UserID})
			if err != nil {
				return err
			}
			if !active {
				return ErrNotFound
			}
		}
		return nil
	}
	if session.TerminalFailureCode.Valid {
		return ErrParentUnavailable
	}
	if session.ParentID.Valid {
		active, err := q.IsLifecycleDestinationActive(ctx, sqlcgen.IsLifecycleDestinationActiveParams{ParentID: session.ParentID, UserID: session.UserID})
		if err != nil {
			return err
		}
		if !active {
			return ErrParentUnavailable
		}
	}
	return nil
}
func completedPart(ctx context.Context, q *sqlcgen.Queries, userID int64, uploadID uuid.UUID, partNo int32) (*sqlcgen.UploadPart, error) {
	proof, err := q.GetCompletedUploadPartProof(ctx, sqlcgen.GetCompletedUploadPartProofParams{UserID: userID, UploadID: dbtypes.UUID(uploadID), PartNo: partNo})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get completed part proof: %w", err)
	}
	return projectCompletedPart(sqlcgen.ListCompletedUploadPartProofsRow(*proof)), nil
}
func projectCompletedPart(proof sqlcgen.ListCompletedUploadPartProofsRow) *sqlcgen.UploadPart {
	return &sqlcgen.UploadPart{UploadID: proof.UploadID, PartNo: proof.PartNo, ChannelID: proof.ChannelID, MessageID: dbtypes.Int8(proof.MessageID), PlainSize: proof.PlainSize.Int64, StoredSize: proof.StoredSize, Checksum: proof.Checksum, Salt: proof.Salt, BlockHashes: proof.BlockHashes, State: sqlcgen.UploadPartStateStored, CreatedAt: proof.CreatedAt, UpdatedAt: proof.UpdatedAt}
}

func normalizeCreateInput(in *CreateInput) error {
	if in.ExpectedSize < -1 || (in.ExpectedHashAlgorithm == nil) != (in.ExpectedHashValue == nil) || in.Encryption != (in.EncryptionKeyVersion != nil) {
		return ErrInvalidInput
	}
	if in.ExpectedHashAlgorithm != nil {
		algorithm, value, err := normalizeExpectedHash(*in.ExpectedHashAlgorithm, *in.ExpectedHashValue)
		if err != nil {
			return err
		}
		in.ExpectedHashAlgorithm, in.ExpectedHashValue = &algorithm, &value
	}
	if in.ConflictPolicy == "" {
		in.ConflictPolicy = sqlcgen.NameConflictPolicyFail
	}
	switch in.ConflictPolicy {
	case sqlcgen.NameConflictPolicyFail, sqlcgen.NameConflictPolicyReplace, sqlcgen.NameConflictPolicyRename:
		// Applied atomically during publication.
	default:
		return ErrInvalidInput
	}
	partSize, err := negotiatePartSize(in.PartSize, in.ExpectedSize)
	if err != nil {
		return err
	}
	in.PartSize = partSize
	return nil
}

type ImportDestinationPin struct {
	AnchorID             *uuid.UUID
	AnchorIsRoot         bool
	DestinationOrdinal   int32
	SpecFingerprint      [32]byte
	RemainderFingerprint [32]byte
	RemainderSegments    int32
}
type PinImportDestinationsInput struct {
	UserID             int64
	BatchID            uuid.UUID
	BaseParentID       *uuid.UUID
	Destination        string
	SourceDestinations []string
}
type InsertImportBatchAdmissionInput struct {
	UserID             int64
	BatchID            uuid.UUID
	OriginJobID        int64
	RequestFingerprint [32]byte
	Pins               []ImportDestinationPin
}
type ImportSourceDestination struct {
	AttemptID          uuid.UUID
	SourceIndex        int32
	DestinationOrdinal int32
	Directory          string
}
type ReserveImportBatchInput struct {
	UserID             int64
	BatchID            uuid.UUID
	OriginJobID        int64
	RequestFingerprint [32]byte
	BaseParentID       *uuid.UUID
	Destination        string
	SourceDestinations []string
	Sources            []ImportSourceDestination
}
type ImportResolvedDestination struct {
	AttemptID   uuid.UUID
	SourceIndex int32
	ParentID    *uuid.UUID
	JobID       int64
}
type ImportBatchReservation struct {
	ParentID   *uuid.UUID
	Dispatched bool
	Sources    []ImportResolvedDestination
}
type ImportChild struct {
	AttemptID   uuid.UUID
	SourceIndex int32
	JobID       int64
}
type RecordImportChildrenInput struct {
	UserID       int64
	BatchID      uuid.UUID
	OriginJobID  int64
	Destinations *ImportBatchReservation
	Children     []ImportChild
}
type ReserveImportAttemptInput struct {
	UserID      int64
	BatchID     uuid.UUID
	SourceIndex int32
	JobID       int64
	UploadID    uuid.UUID
	Candidate   bool
	Create      CreateInput
}
type RecordImportResultInput struct {
	UserID               int64
	BatchID              uuid.UUID
	SourceIndex          int32
	JobID                int64
	ResultFileID         uuid.UUID
	Encryption           bool
	EncryptionKeyVersion *int32
}

func (s *Service) GetImportBatch(ctx context.Context, userID int64, batchID uuid.UUID, originJobID int64) (*sqlcgen.ImportBatch, error) {
	if userID <= 0 || batchID == uuid.Nil || originJobID <= 0 {
		return nil, ErrInvalidInput
	}
	b, err := s.queries.GetImportBatch(ctx, sqlcgen.GetImportBatchParams{UserID: userID, BatchID: dbtypes.UUID(batchID), OriginJobID: originJobID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}
func (s *Service) GetImportAttempt(ctx context.Context, userID int64, batchID uuid.UUID, sourceIndex int32, jobID int64) (*sqlcgen.ImportJobAttempt, error) {
	if userID <= 0 || batchID == uuid.Nil || sourceIndex < 0 || jobID <= 0 {
		return nil, ErrInvalidInput
	}
	a, err := s.queries.GetImportAttempt(ctx, sqlcgen.GetImportAttemptParams{UserID: userID, BatchID: dbtypes.UUID(batchID), SourceIndex: sourceIndex, JobID: jobID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return a, err
}

func importSegments(value string, absolute bool) ([]string, error) {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	if strings.HasPrefix(value, "/") && !absolute {
		return nil, ErrInvalidInput
	}
	if value == "" || (absolute && value == "/") {
		return nil, nil
	}
	value = path.Clean(value)
	if value == "." || value == ".." || strings.HasPrefix(value, "../") {
		return nil, ErrInvalidInput
	}
	value = strings.Trim(value, "/")
	return strings.Split(value, "/"), nil
}
func importSpec(base *uuid.UUID, destination string, relative string) [32]byte {
	root := "root"
	if base != nil {
		root = base.String()
	}
	return sha256.Sum256([]byte(root + "\x00" + destination + "\x00" + relative))
}
func importRemainder(parts []string) [32]byte { return sha256.Sum256([]byte(strings.Join(parts, "/"))) }
func importParent(ctx context.Context, q *sqlcgen.Queries, userID int64, parent *uuid.UUID) error {
	if parent == nil {
		return nil
	}
	ok, err := q.IsLifecycleDestinationActive(ctx, sqlcgen.IsLifecycleDestinationActiveParams{UserID: userID, ParentID: dbtypes.UUID(*parent)})
	if err != nil {
		return err
	}
	if !ok {
		return ErrParentUnavailable
	}
	return nil
}
func pinImportPath(ctx context.Context, q *sqlcgen.Queries, userID int64, parent *uuid.UUID, parts []string, ordinal int32, spec [32]byte) (ImportDestinationPin, error) {
	if err := importParent(ctx, q, userID, parent); err != nil {
		return ImportDestinationPin{}, err
	}
	for i, name := range parts {
		child, err := q.ResolveActiveChild(ctx, sqlcgen.ResolveActiveChildParams{UserID: userID, ParentID: dbtypes.OptionalUUID(parent), Name: name})
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && ordinal >= 0 && i == len(parts)-1 && child.Kind == sqlcgen.FileKindFile) {
			return ImportDestinationPin{AnchorID: parent, AnchorIsRoot: parent == nil, DestinationOrdinal: ordinal, SpecFingerprint: spec, RemainderFingerprint: importRemainder(parts[i:]), RemainderSegments: int32(len(parts) - i)}, nil
		}
		if err != nil {
			return ImportDestinationPin{}, err
		}
		if child.Kind != sqlcgen.FileKindFolder {
			return ImportDestinationPin{}, ErrNameConflict
		}
		id := uuid.UUID(child.ID.Bytes)
		parent = &id
	}
	return ImportDestinationPin{AnchorID: parent, AnchorIsRoot: parent == nil, DestinationOrdinal: ordinal, SpecFingerprint: spec, RemainderFingerprint: importRemainder(nil)}, nil
}
func importBatchPath(base *uuid.UUID, destination string) (*uuid.UUID, []string, error) {
	destination = strings.TrimSpace(destination)
	if destination == "" {
		return base, nil, nil
	}
	if id, err := uuid.Parse(destination); err == nil {
		if base != nil {
			return nil, nil, ErrInvalidInput
		}
		return &id, nil, nil
	}
	if base == nil && !strings.HasPrefix(destination, "/") {
		return nil, nil, ErrInvalidInput
	}
	if base != nil && strings.HasPrefix(destination, "/") {
		return nil, nil, ErrInvalidInput
	}
	parts, err := importSegments(destination, base == nil)
	return base, parts, err
}

// PinImportDestinationsTx is Q: metadata only, with no folder creation or source access.
func (s *Service) PinImportDestinationsTx(ctx context.Context, tx pgx.Tx, in PinImportDestinationsInput) ([]ImportDestinationPin, error) {
	if in.UserID <= 0 || in.BatchID == uuid.Nil || tx == nil {
		return nil, ErrInvalidInput
	}
	if err := lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, err
	}
	base, parts, err := importBatchPath(in.BaseParentID, in.Destination)
	if err != nil {
		return nil, err
	}
	q := s.queries.WithTx(tx)
	batch, err := pinImportPath(ctx, q, in.UserID, base, parts, -1, importSpec(in.BaseParentID, in.Destination, ""))
	if err != nil {
		return nil, err
	}
	pins := make([]ImportDestinationPin, 1, len(in.SourceDestinations)+1)
	pins[0] = batch
	remainder := parts[len(parts)-int(batch.RemainderSegments):]
	for i, spec := range in.SourceDestinations {
		relative, err := importSegments(spec, false)
		if err != nil {
			return nil, err
		}
		route := append(slices.Clone(remainder), relative...)
		pin, err := pinImportPath(ctx, q, in.UserID, batch.AnchorID, route, int32(i), importSpec(in.BaseParentID, in.Destination, spec))
		if err != nil {
			return nil, err
		}
		pins = append(pins, pin)
	}
	return pins, nil
}
func sameImportPins(saved []*sqlcgen.ImportDestinationPin, pins []ImportDestinationPin) bool {
	if len(saved) != len(pins) {
		return false
	}
	for i, p := range pins {
		r := saved[i]
		if r.DestinationOrdinal != p.DestinationOrdinal || r.AnchorID != dbtypes.OptionalUUID(p.AnchorID) || r.AnchorIsRoot != p.AnchorIsRoot || !slices.Equal(r.SpecFingerprint, p.SpecFingerprint[:]) || !slices.Equal(r.RemainderFingerprint, p.RemainderFingerprint[:]) || r.RemainderSegments != p.RemainderSegments {
			return false
		}
	}
	return true
}
func (s *Service) InsertImportBatchAdmissionTx(ctx context.Context, tx pgx.Tx, in InsertImportBatchAdmissionInput) error {
	if in.UserID <= 0 || in.BatchID == uuid.Nil || in.OriginJobID <= 0 || len(in.Pins) == 0 {
		return ErrInvalidInput
	}
	if err := lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return err
	}
	for i, p := range in.Pins {
		if p.DestinationOrdinal != int32(i)-1 || p.AnchorIsRoot != (p.AnchorID == nil) || p.RemainderSegments < 0 {
			return ErrInvalidInput
		}
		if err := importParent(ctx, s.queries.WithTx(tx), in.UserID, p.AnchorID); err != nil {
			return err
		}
	}
	q := s.queries.WithTx(tx)
	_, err := q.InsertImportBatchAdmission(ctx, sqlcgen.InsertImportBatchAdmissionParams{UserID: in.UserID, BatchID: dbtypes.UUID(in.BatchID), OriginJobID: in.OriginJobID, RequestFingerprint: in.RequestFingerprint[:]})
	if errors.Is(err, pgx.ErrNoRows) {
		b, e := q.GetImportBatch(ctx, sqlcgen.GetImportBatchParams{UserID: in.UserID, BatchID: dbtypes.UUID(in.BatchID), OriginJobID: in.OriginJobID})
		if e != nil {
			return errors.Join(lifecycle.ErrRequestMismatch, e)
		}
		if !slices.Equal(b.RequestFingerprint, in.RequestFingerprint[:]) {
			return lifecycle.ErrRequestMismatch
		}
		saved, e := q.ListImportDestinationPins(ctx, sqlcgen.ListImportDestinationPinsParams{UserID: in.UserID, BatchID: dbtypes.UUID(in.BatchID)})
		if e != nil {
			return e
		}
		if !sameImportPins(saved, in.Pins) {
			return lifecycle.ErrRequestMismatch
		}
		if b.TerminalFailureCode.Valid {
			return ErrParentUnavailable
		}
		return nil
	}
	if err != nil {
		return err
	}
	for _, p := range in.Pins {
		if err = q.InsertImportDestinationPin(ctx, sqlcgen.InsertImportDestinationPinParams{UserID: in.UserID, BatchID: dbtypes.UUID(in.BatchID), DestinationOrdinal: p.DestinationOrdinal, AnchorID: dbtypes.OptionalUUID(p.AnchorID), AnchorIsRoot: p.AnchorIsRoot, SpecFingerprint: p.SpecFingerprint[:], RemainderFingerprint: p.RemainderFingerprint[:], RemainderSegments: p.RemainderSegments}); err != nil {
			return err
		}
	}
	return nil
}
func lockImportBatch(ctx context.Context, q *sqlcgen.Queries, userID int64, batch uuid.UUID, job int64) (*sqlcgen.ImportBatch, error) {
	b, err := q.LockImportBatch(ctx, sqlcgen.LockImportBatchParams{UserID: userID, BatchID: dbtypes.UUID(batch), OriginJobID: job})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if b.TerminalFailureCode.Valid {
		return nil, ErrParentUnavailable
	}
	if err = q.LockImportBatchPins(ctx, sqlcgen.LockImportBatchPinsParams{UserID: userID, BatchID: dbtypes.UUID(batch)}); err != nil {
		return nil, err
	}
	return b, nil
}
func verifyImportPin(ctx context.Context, q *sqlcgen.Queries, userID int64, p *sqlcgen.ImportDestinationPin, spec [32]byte, route []string) (*uuid.UUID, []string, error) {
	if !slices.Equal(p.SpecFingerprint, spec[:]) || p.RemainderSegments < 0 || int(p.RemainderSegments) > len(route) {
		return nil, nil, lifecycle.ErrRequestMismatch
	}
	remainder := route[len(route)-int(p.RemainderSegments):]
	digest := importRemainder(remainder)
	if !slices.Equal(p.RemainderFingerprint, digest[:]) {
		return nil, nil, lifecycle.ErrRequestMismatch
	}
	var parent *uuid.UUID
	if p.AnchorID.Valid {
		id := uuid.UUID(p.AnchorID.Bytes)
		parent = &id
	}
	if p.AnchorIsRoot != (parent == nil) {
		return nil, nil, ErrParentUnavailable
	}
	if err := importParent(ctx, q, userID, parent); err != nil {
		return nil, nil, err
	}
	return parent, remainder, nil
}
func ensureImportPath(ctx context.Context, q *sqlcgen.Queries, userID int64, parent *uuid.UUID, parts []string) (*uuid.UUID, error) {
	for _, name := range parts {
		child, err := q.ResolveActiveChild(ctx, sqlcgen.ResolveActiveChildParams{UserID: userID, ParentID: dbtypes.OptionalUUID(parent), Name: name})
		if errors.Is(err, pgx.ErrNoRows) {
			child, err = q.CreateFolder(ctx, sqlcgen.CreateFolderParams{ID: dbtypes.UUID(uuid.New()), UserID: userID, ParentID: dbtypes.OptionalUUID(parent), Name: name, ModTime: dbtypes.Time(time.Now().UTC())})
		}
		if err != nil {
			return nil, err
		}
		if child.Kind != sqlcgen.FileKindFolder {
			return nil, ErrNameConflict
		}
		id := uuid.UUID(child.ID.Bytes)
		parent = &id
	}
	return parent, nil
}

// ReserveImportBatchTx is A's folder half; RecordImportChildrenTx closes A after real InsertManyTx.
func (s *Service) ReserveImportBatchTx(ctx context.Context, tx pgx.Tx, in ReserveImportBatchInput) (*ImportBatchReservation, error) {
	if in.UserID <= 0 || in.BatchID == uuid.Nil || in.OriginJobID <= 0 {
		return nil, ErrInvalidInput
	}
	if err := lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, err
	}
	q := s.queries.WithTx(tx)
	b, err := lockImportBatch(ctx, q, in.UserID, in.BatchID, in.OriginJobID)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(b.RequestFingerprint, in.RequestFingerprint[:]) {
		return nil, lifecycle.ErrRequestMismatch
	}
	pins, err := q.ListImportDestinationPins(ctx, sqlcgen.ListImportDestinationPinsParams{UserID: in.UserID, BatchID: dbtypes.UUID(in.BatchID)})
	if err != nil {
		return nil, err
	}
	if len(pins) != len(in.SourceDestinations)+1 {
		return nil, lifecycle.ErrRequestMismatch
	}
	for i, pin := range pins {
		spec := ""
		if i > 0 {
			spec = in.SourceDestinations[i-1]
		}
		digest := importSpec(in.BaseParentID, in.Destination, spec)
		if pin.DestinationOrdinal != int32(i)-1 || !slices.Equal(pin.SpecFingerprint, digest[:]) {
			return nil, lifecycle.ErrRequestMismatch
		}
	}
	result := &ImportBatchReservation{Dispatched: b.Dispatched}
	if b.ParentID.Valid {
		id := uuid.UUID(b.ParentID.Bytes)
		result.ParentID = &id
	}
	if b.Dispatched {
		rows, e := q.ListImportBatchAttempts(ctx, sqlcgen.ListImportBatchAttemptsParams{UserID: in.UserID, BatchID: dbtypes.UUID(in.BatchID)})
		if e != nil {
			return nil, e
		}
		for _, a := range rows {
			var parent *uuid.UUID
			if a.ParentID.Valid {
				id := uuid.UUID(a.ParentID.Bytes)
				parent = &id
			}
			result.Sources = append(result.Sources, ImportResolvedDestination{AttemptID: uuid.UUID(a.AttemptID.Bytes), SourceIndex: a.SourceIndex, ParentID: parent, JobID: a.JobID})
		}
		return result, nil
	}
	_, route, err := importBatchPath(in.BaseParentID, in.Destination)
	if err != nil {
		return nil, err
	}
	parent, remainder, err := verifyImportPin(ctx, q, in.UserID, pins[0], importSpec(in.BaseParentID, in.Destination, ""), route)
	if err != nil {
		return nil, err
	}
	// Validate every saved anchor and original spec before creating the first folder.
	for i, spec := range in.SourceDestinations {
		segments, e := importSegments(spec, false)
		if e != nil {
			return nil, e
		}
		if pins[i+1].DestinationOrdinal != int32(i) {
			return nil, lifecycle.ErrRequestMismatch
		}
		if _, _, e = verifyImportPin(ctx, q, in.UserID, pins[i+1], importSpec(in.BaseParentID, in.Destination, spec), append(slices.Clone(remainder), segments...)); e != nil {
			return nil, e
		}
	}
	result.ParentID, err = ensureImportPath(ctx, q, in.UserID, parent, remainder)
	if err != nil {
		return nil, err
	}
	seen := make(map[int32]bool, len(in.Sources))
	attempts := make(map[uuid.UUID]bool, len(in.Sources))
	for _, source := range in.Sources {
		if source.AttemptID == uuid.Nil || source.SourceIndex < 0 || seen[source.SourceIndex] || attempts[source.AttemptID] || source.DestinationOrdinal < 0 || int(source.DestinationOrdinal) >= len(in.SourceDestinations) {
			return nil, ErrInvalidInput
		}
		seen[source.SourceIndex] = true
		attempts[source.AttemptID] = true
		spec := in.SourceDestinations[source.DestinationOrdinal]
		declared, e := importSegments(spec, false)
		if e != nil {
			return nil, e
		}
		directory, e := importSegments(source.Directory, false)
		if e != nil {
			return nil, e
		}
		if len(declared) > 0 && !slices.Equal(directory, declared[:len(declared)-1]) && (len(directory) < len(declared) || !slices.Equal(directory[:len(declared)], declared)) {
			return nil, lifecycle.ErrRequestMismatch
		}
		pin := pins[source.DestinationOrdinal+1]
		full := append(slices.Clone(remainder), declared...)
		anchor, _, e := verifyImportPin(ctx, q, in.UserID, pin, importSpec(in.BaseParentID, in.Destination, spec), full)
		if e != nil {
			return nil, e
		}
		actual := append(slices.Clone(remainder), directory...)
		consumed := len(full) - int(pin.RemainderSegments)
		if consumed > len(actual) || !slices.Equal(actual[:consumed], full[:consumed]) {
			return nil, ErrParentUnavailable
		}
		resolved, e := ensureImportPath(ctx, q, in.UserID, anchor, actual[consumed:])
		if e != nil {
			return nil, e
		}
		result.Sources = append(result.Sources, ImportResolvedDestination{AttemptID: source.AttemptID, SourceIndex: source.SourceIndex, ParentID: resolved})
	}
	return result, nil
}
func (s *Service) RecordImportChildrenTx(ctx context.Context, tx pgx.Tx, in RecordImportChildrenInput) error {
	if in.Destinations == nil || len(in.Children) != len(in.Destinations.Sources) {
		return ErrInvalidInput
	}
	if err := lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return err
	}
	q := s.queries.WithTx(tx)
	b, err := lockImportBatch(ctx, q, in.UserID, in.BatchID, in.OriginJobID)
	if err != nil {
		return err
	}
	if b.Dispatched {
		return ErrInvalidState
	}
	if err = importParent(ctx, q, in.UserID, in.Destinations.ParentID); err != nil {
		return err
	}
	seen := make(map[int32]bool, len(in.Children))
	jobs := make(map[int64]bool, len(in.Children))
	for i, child := range in.Children {
		d := in.Destinations.Sources[i]
		if child.JobID <= 0 || child.AttemptID != d.AttemptID || child.SourceIndex != d.SourceIndex || seen[child.SourceIndex] || jobs[child.JobID] {
			return ErrInvalidInput
		}
		seen[child.SourceIndex] = true
		jobs[child.JobID] = true
		if err = importParent(ctx, q, in.UserID, d.ParentID); err != nil {
			return err
		}
		if _, err = q.InsertImportChild(ctx, sqlcgen.InsertImportChildParams{UserID: in.UserID, BatchID: dbtypes.UUID(in.BatchID), AttemptID: dbtypes.UUID(child.AttemptID), SourceIndex: child.SourceIndex, JobID: child.JobID, ParentID: dbtypes.OptionalUUID(d.ParentID)}); err != nil {
			return err
		}
	}
	n, err := q.DispatchImportBatch(ctx, sqlcgen.DispatchImportBatchParams{UserID: in.UserID, BatchID: dbtypes.UUID(in.BatchID), OriginJobID: in.OriginJobID, ParentID: dbtypes.OptionalUUID(in.Destinations.ParentID), DestinationIsRoot: pgtype.Bool{Bool: in.Destinations.ParentID == nil, Valid: true}, SourceCount: dbtypes.Int4(int32(len(in.Children)))})
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrInvalidState
	}
	return nil
}
func lockSourceSelection(ctx context.Context, q *sqlcgen.Queries, userID int64, batch uuid.UUID, index int32, job int64) (*sqlcgen.ImportJobAttempt, error) {
	hint, err := q.GetImportAttempt(ctx, sqlcgen.GetImportAttemptParams{UserID: userID, BatchID: dbtypes.UUID(batch), SourceIndex: index, JobID: job})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	a, err := lockCreateImport(ctx, q, userID, uuid.UUID(hint.AttemptID.Bytes))
	if err != nil {
		return nil, err
	}
	if a.BatchID != dbtypes.UUID(batch) || a.SourceIndex != index || a.JobID != job {
		return nil, ErrInvalidState
	}
	var parent *uuid.UUID
	if a.ParentID.Valid {
		id := uuid.UUID(a.ParentID.Bytes)
		parent = &id
	}
	if err = importParent(ctx, q, userID, parent); err != nil {
		return nil, err
	}
	return a, nil
}
func (s *Service) ReserveImportAttemptTx(ctx context.Context, tx pgx.Tx, in ReserveImportAttemptInput) (*sqlcgen.ImportJobAttempt, error) {
	if in.UserID <= 0 || in.BatchID == uuid.Nil || in.JobID <= 0 || in.SourceIndex < 0 || in.UploadID == uuid.Nil || in.Create.UserID != in.UserID {
		return nil, ErrInvalidInput
	}
	if err := lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, err
	}
	q := s.queries.WithTx(tx)
	assigned, e := q.GetImportAttemptForUpload(ctx, sqlcgen.GetImportAttemptForUploadParams{UserID: in.UserID, UploadID: dbtypes.UUID(in.UploadID)})
	if e == nil && (assigned.BatchID != dbtypes.UUID(in.BatchID) || assigned.SourceIndex != in.SourceIndex || assigned.JobID != in.JobID) {
		return nil, ErrInvalidState
	}
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return nil, e
	}
	a, err := lockSourceSelection(ctx, q, in.UserID, in.BatchID, in.SourceIndex, in.JobID)
	if err != nil {
		return nil, err
	}
	if a.ParentID != dbtypes.OptionalUUID(in.Create.ParentID) {
		return nil, ErrParentUnavailable
	}
	if a.ResultFileID.Valid {
		return nil, ErrInvalidState
	}
	if a.UploadID.Valid {
		if _, err := SelectedImportCreateInput(a); err != nil {
			return nil, err
		}
		session, e := q.GetUploadSessionForUser(ctx, sqlcgen.GetUploadSessionForUserParams{UserID: in.UserID, UploadID: a.UploadID})
		if e == nil {
			if _, e = s.retainedSession(ctx, q, session); e != nil {
				return nil, e
			}
		} else if !errors.Is(e, pgx.ErrNoRows) || a.SessionMaterialized {
			return nil, errors.Join(ErrNotFound, e)
		}
		return a, nil
	}
	candidate, err := q.LockUploadSessionForUser(ctx, sqlcgen.LockUploadSessionForUserParams{UserID: in.UserID, UploadID: dbtypes.UUID(in.UploadID)})
	if err == nil {
		if candidate.TerminalFailureCode.Valid {
			return nil, ErrParentUnavailable
		}
		if !in.Candidate {
			return nil, ErrInvalidState
		}
		if _, err = s.retainedSession(ctx, q, candidate); err != nil {
			return nil, err
		}
		policy := in.Create.ConflictPolicy
		if policy == "" {
			policy = sqlcgen.NameConflictPolicyFail
		}
		if candidate.ConflictPolicy != policy || (in.Create.ExpectedHashAlgorithm != nil && (!optionalTextEqual(candidate.ExpectedHashAlgorithm, in.Create.ExpectedHashAlgorithm) || !optionalTextEqual(candidate.ExpectedHashValue, in.Create.ExpectedHashValue))) {
			return nil, ErrInvalidState
		}
		if candidate.State != sqlcgen.UploadStateOpen || candidate.ParentID != a.ParentID || candidate.Name != in.Create.Name || candidate.ExpectedSize != in.Create.ExpectedSize || candidate.Encryption != in.Create.Encryption || candidate.MimeType != dbtypes.OptionalText(in.Create.MIMEType) || (!in.Create.ModTime.IsZero() && math.Abs(candidate.ModTime.Time.Sub(in.Create.ModTime).Seconds()) > 1) {
			return nil, ErrInvalidState
		}
		in.Create = CreateInput{Name: candidate.Name, ExpectedSize: candidate.ExpectedSize, ModTime: candidate.ModTime.Time.UTC(), Encryption: candidate.Encryption, ConflictPolicy: candidate.ConflictPolicy, PartSize: candidate.PartSize}
		if candidate.ExpectedHashAlgorithm.Valid {
			v := candidate.ExpectedHashAlgorithm.String
			in.Create.ExpectedHashAlgorithm = &v
		}
		if candidate.ExpectedHashValue.Valid {
			v := candidate.ExpectedHashValue.String
			in.Create.ExpectedHashValue = &v
		}
		if candidate.MimeType.Valid {
			v := candidate.MimeType.String
			in.Create.MIMEType = &v
		}
		if candidate.EncryptionKeyVersion.Valid {
			v := candidate.EncryptionKeyVersion.Int32
			in.Create.EncryptionKeyVersion = &v
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	} else if in.Candidate {
		return nil, ErrNotFound
	} else if err = normalizeCreateInput(&in.Create); err != nil {
		return nil, err
	}
	if in.Create.ModTime.IsZero() {
		in.Create.ModTime = s.now()
	}
	in.Create.ModTime = in.Create.ModTime.UTC().Truncate(time.Microsecond)
	descriptor, err := encodeSelectedImportCreate(in.Create)
	if err != nil {
		return nil, err
	}
	probe := *a
	probe.UploadID = dbtypes.UUID(in.UploadID)
	probe.SelectedCreate = descriptor
	if _, err = SelectedImportCreateInput(&probe); err != nil {
		return nil, err
	}
	selected, err := q.SelectImportUpload(ctx, sqlcgen.SelectImportUploadParams{UserID: in.UserID, AttemptID: a.AttemptID, UploadID: dbtypes.UUID(in.UploadID), SelectedCreate: descriptor})
	if err != nil {
		return nil, err
	}
	if in.Candidate {
		n, e := q.MarkUploadImportMaterialized(ctx, sqlcgen.MarkUploadImportMaterializedParams{UserID: in.UserID, AttemptID: a.AttemptID, UploadID: dbtypes.UUID(in.UploadID)})
		if e != nil || n != 1 {
			return nil, errors.Join(ErrInvalidState, e)
		}
		selected.SessionMaterialized = true
	}
	return selected, nil
}
func (s *Service) RecordImportResultTx(ctx context.Context, tx pgx.Tx, in RecordImportResultInput) (*sqlcgen.ImportJobAttempt, error) {
	if in.UserID <= 0 || in.ResultFileID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	if err := lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, err
	}
	q := s.queries.WithTx(tx)
	a, err := lockSourceSelection(ctx, q, in.UserID, in.BatchID, in.SourceIndex, in.JobID)
	if err != nil {
		return nil, err
	}
	if a.UploadID.Valid {
		return nil, ErrInvalidState
	}
	if a.ResultFileID.Valid && a.ResultFileID != dbtypes.UUID(in.ResultFileID) {
		return nil, ErrInvalidState
	}
	file, err := q.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{UserID: in.UserID, FileID: dbtypes.UUID(in.ResultFileID)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if file.Status != sqlcgen.FileStatusActive || file.Kind != sqlcgen.FileKindFile || file.ParentID != a.ParentID {
		return nil, ErrParentUnavailable
	}
	if file.Encryption != in.Encryption || file.EncryptionKeyVersion != dbtypes.OptionalInt4(in.EncryptionKeyVersion) {
		return nil, ErrInvalidState
	}
	if a.ResultFileID.Valid {
		return a, nil
	}
	return q.SelectImportResult(ctx, sqlcgen.SelectImportResultParams{UserID: in.UserID, AttemptID: a.AttemptID, ResultFileID: dbtypes.UUID(in.ResultFileID)})
}
