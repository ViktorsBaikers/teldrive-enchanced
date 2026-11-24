// Package lifecycle owns durable remote-write provenance and deletion fences.
// Remote calls never run while a lifecycle SQL transaction is open.
package lifecycle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
)

const cleanupTimeout = 5 * time.Second

var (
	ErrInvalidInput               = errors.New("invalid lifecycle input")
	ErrReceiptConflict            = errors.New("remote receipt conflicts with reservation")
	ErrDeletionFenced             = errors.New("physical message deletion is fenced")
	ErrChannelWriteUnavailable    = errors.New("physical channel is unavailable for writes")
	ErrCopyDestinationUnavailable = errors.New("copy destination folder is unavailable")
	ErrRequestMismatch            = errors.New("request conflicts with original admission")
	ErrEnqueuerUnavailable        = errors.New("receipt enqueuer is unavailable or already bound")
	ErrUnresolved                 = errors.New("remote write outcome is unknown")
	ErrAttemptsExhausted          = errors.New("receipt cleanup attempts are exhausted")
)

// MessageDeleter must acknowledge only confirmed deletion/absence of exact IDs.
// Its destructive request middleware must not retry under one durable claim.
type MessageDeleter interface {
	DeleteMessages(context.Context, int64, int64, []int64) error
}
type ReceiptCleanupInput struct {
	UserID     int64
	ReceiptIDs []uuid.UUID
}
type ReceiptEnqueuer interface {
	EnqueueReceiptCleanupTx(ctx context.Context, tx pgx.Tx, in ReceiptCleanupInput) (jobID int64, err error)
}
type Service struct {
	pool            *pgxpool.Pool
	queries         *sqlcgen.Queries
	storage         MessageDeleter
	receiptEnqueuer ReceiptEnqueuer
}

func NewService(pool *pgxpool.Pool, storage MessageDeleter) *Service {
	return &Service{pool: pool, queries: sqlcgen.New(pool), storage: storage}
}

// BindReceiptEnqueuer is initialization-only, before workers or HTTP start.
func (s *Service) BindReceiptEnqueuer(enqueuer ReceiptEnqueuer) error {
	if s.receiptEnqueuer != nil || enqueuer == nil {
		return ErrEnqueuerUnavailable
	}
	value := reflect.ValueOf(enqueuer)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return ErrEnqueuerUnavailable
		}
	}
	s.receiptEnqueuer = enqueuer
	return nil
}

// RequireWritableChannelsTx rechecks intent absence in a fresh statement after locks.
func RequireWritableChannelsTx(ctx context.Context, tx pgx.Tx, channelIDs []int64) error {
	if err := LockChannelWritesTx(ctx, tx, channelIDs); err != nil {
		return err
	}
	q := sqlcgen.New(tx)
	for _, id := range channelIDs {
		_, err := q.GetChannelDeletionIntent(ctx, id)
		if err == nil {
			return ErrChannelWriteUnavailable
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read physical channel intent: %w", err)
		}
	}
	return nil
}

type RequestScope struct {
	ActorUserID   *int64
	PublicShareID *uuid.UUID
}
type RequestAdmissionInput struct {
	Scope              RequestScope
	RequestKey         uuid.UUID
	RequestFingerprint [32]byte
}

func validScope(scope RequestScope) bool {
	return (scope.ActorUserID != nil && *scope.ActorUserID > 0 && scope.PublicShareID == nil) ||
		(scope.ActorUserID == nil && scope.PublicShareID != nil && *scope.PublicShareID != uuid.Nil)
}
func getAdmission(ctx context.Context, q *sqlcgen.Queries, scope RequestScope, kind string, key uuid.UUID) (*sqlcgen.RequestAdmission, error) {
	if !validScope(scope) || (kind != "upload" && kind != "copy") || key == uuid.Nil {
		return nil, ErrInvalidInput
	}
	var row *sqlcgen.RequestAdmission
	var err error
	if scope.ActorUserID != nil {
		row, err = q.GetRequestAdmissionForActor(ctx, sqlcgen.GetRequestAdmissionForActorParams{ActorUserID: *scope.ActorUserID, RequestKind: kind, RequestKey: dbtypes.UUID(key)})
	} else {
		row, err = q.GetRequestAdmissionForShare(ctx, sqlcgen.GetRequestAdmissionForShareParams{PublicShareID: dbtypes.UUID(*scope.PublicShareID), RequestKind: kind, RequestKey: dbtypes.UUID(key)})
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

// GetRequestAdmission is an internal authorization hint, never access authority.
func (s *Service) GetRequestAdmission(ctx context.Context, scope RequestScope, kind string, key uuid.UUID) (*sqlcgen.RequestAdmission, error) {
	return getAdmission(ctx, s.queries, scope, kind, key)
}

// BindRequestAdmissionTx never allocates a replacement for a retained/missing target.
// The adapter must establish current actor/share authorization before disclosure.
func (s *Service) BindRequestAdmissionTx(ctx context.Context, tx pgx.Tx, ownerID int64, kind string, admission RequestAdmissionInput, uploadID *uuid.UUID, operationID *uuid.UUID) (binding *sqlcgen.RequestAdmission, created bool, err error) {
	if !validScope(admission.Scope) || admission.RequestKey == uuid.Nil ||
		!((kind == "upload" && validID(uploadID) && operationID == nil) || (kind == "copy" && validID(operationID) && uploadID == nil)) {
		return nil, false, ErrInvalidInput
	}
	if err = LockOwnerTx(ctx, tx, ownerID); err != nil {
		return nil, false, err
	}
	// Scope uniqueness spans owners. A conflicting owner is rejected without
	// taking its owner key or touching its original target.
	scopeID := uuid.Nil
	if admission.Scope.PublicShareID != nil {
		scopeID = *admission.Scope.PublicShareID
	}
	var key [49]byte
	copy(key[:16], admission.RequestKey[:])
	copy(key[16:32], scopeID[:])
	if admission.Scope.ActorUserID != nil {
		copy(key[32:40], []byte("actor"))
		binary.BigEndian.PutUint64(key[40:48], uint64(*admission.Scope.ActorUserID))
	}
	if kind == "copy" {
		key[48] = 1
	}
	digest := sha256.Sum256(key[:])
	q := sqlcgen.New(tx)
	if err = q.AcquireAdvisoryTransactionLock(ctx, lockKey(4, int64(binary.BigEndian.Uint64(digest[:8])), int64(binary.BigEndian.Uint64(digest[8:16])))); err != nil {
		return nil, false, fmt.Errorf("lock request admission: %w", err)
	}
	binding, err = getAdmission(ctx, q, admission.Scope, kind, admission.RequestKey)
	if errors.Is(err, pgx.ErrNoRows) {
		binding, err = q.InsertRequestAdmission(ctx, sqlcgen.InsertRequestAdmissionParams{ID: dbtypes.UUID(uuid.New()), UserID: ownerID, RequestKind: kind, RequestKey: dbtypes.UUID(admission.RequestKey), ActorUserID: dbtypes.OptionalInt8(admission.Scope.ActorUserID), PublicShareID: dbtypes.OptionalUUID(admission.Scope.PublicShareID), RequestFingerprint: admission.RequestFingerprint[:], UploadID: dbtypes.OptionalUUID(uploadID), CopyOperationID: dbtypes.OptionalUUID(operationID)})
		created = err == nil
		if errors.Is(err, pgx.ErrNoRows) {
			binding, err = getAdmission(ctx, q, admission.Scope, kind, admission.RequestKey)
		}
	}
	if err != nil {
		return nil, false, fmt.Errorf("bind request admission: %w", err)
	}
	if binding.UserID != ownerID || !bytes.Equal(binding.RequestFingerprint, admission.RequestFingerprint[:]) {
		return nil, false, ErrRequestMismatch
	}
	return binding, created, nil
}

type ReserveCopyInput struct {
	UserID              int64
	Admission           *RequestAdmissionInput
	OperationID         uuid.UUID
	RootResultFileID    uuid.UUID
	LeaseToken          uuid.UUID
	DestinationParentID *uuid.UUID
	DestinationIsRoot   bool
	LeaseExpiresAt      time.Time
}

func (s *Service) ReserveCopyTx(ctx context.Context, tx pgx.Tx, in ReserveCopyInput) (operation *sqlcgen.RemoteWriteReceipt, created bool, err error) {
	if in.UserID <= 0 || in.OperationID == uuid.Nil || in.RootResultFileID == uuid.Nil || in.LeaseToken == uuid.Nil || !validTime(in.LeaseExpiresAt) || in.DestinationIsRoot != (in.DestinationParentID == nil) || (in.DestinationParentID != nil && !validID(in.DestinationParentID)) {
		return nil, false, ErrInvalidInput
	}
	if err = LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, false, err
	}
	q := sqlcgen.New(tx)
	if in.Admission != nil {
		binding, fresh, e := s.BindRequestAdmissionTx(ctx, tx, in.UserID, "copy", *in.Admission, nil, &in.OperationID)
		if e != nil {
			return nil, false, e
		}
		if !fresh {
			row, e := q.GetRemoteWriteReceipt(ctx, binding.CopyOperationID)
			if e != nil {
				return nil, false, fmt.Errorf("read original copy operation: %w", e)
			}
			return row, false, nil
		}
	}
	if in.DestinationParentID != nil {
		active, e := q.IsLifecycleDestinationActive(ctx, sqlcgen.IsLifecycleDestinationActiveParams{ParentID: dbtypes.UUID(*in.DestinationParentID), UserID: in.UserID})
		if e != nil {
			return nil, false, e
		}
		if !active {
			return nil, false, ErrCopyDestinationUnavailable
		}
	}
	operation, err = q.ReserveCopyOperation(ctx, sqlcgen.ReserveCopyOperationParams{OperationID: dbtypes.UUID(in.OperationID), UserID: in.UserID, ResultFileID: dbtypes.UUID(in.RootResultFileID), DestinationParentID: dbtypes.OptionalUUID(in.DestinationParentID), DestinationIsRoot: in.DestinationIsRoot, LeaseToken: dbtypes.UUID(in.LeaseToken), LeaseExpiresAt: dbtypes.Time(in.LeaseExpiresAt.UTC())})
	if err == nil {
		return operation, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("reserve copy operation: %w", err)
	}
	operation, err = q.GetRemoteWriteReceipt(ctx, dbtypes.UUID(in.OperationID))
	if err != nil {
		return nil, false, err
	}
	if operation.UserID != in.UserID || operation.RowKind != "copy_operation" || operation.ResultFileID != dbtypes.UUID(in.RootResultFileID) || operation.DestinationParentID != dbtypes.OptionalUUID(in.DestinationParentID) || !operation.DestinationIsRoot.Valid || operation.DestinationIsRoot.Bool != in.DestinationIsRoot {
		return nil, false, ErrReceiptConflict
	}
	return operation, false, nil
}

type ReserveInput struct {
	ID             uuid.UUID
	UserID         int64
	ChannelID      int64
	OriginKind     string
	UploadID       *uuid.UUID
	PartNo         int32
	LeaseToken     uuid.UUID
	OperationID    *uuid.UUID
	ResultFileID   *uuid.UUID
	LeaseExpiresAt time.Time
}

func (s *Service) Reserve(ctx context.Context, in ReserveInput) (*sqlcgen.RemoteWriteReceipt, error) {
	if in.ID == uuid.Nil || in.UserID <= 0 || in.ChannelID == 0 || in.PartNo <= 0 || in.LeaseToken == uuid.Nil || !validTime(in.LeaseExpiresAt) ||
		!((in.OriginKind == "upload" && validID(in.UploadID) && in.OperationID == nil && in.ResultFileID == nil) || (in.OriginKind == "copy" && in.UploadID == nil && validID(in.OperationID) && validID(in.ResultFileID))) {
		return nil, ErrInvalidInput
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin receipt reservation: %w", err)
	}
	defer tx.Rollback(ctx)
	if err = LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, err
	}
	q := sqlcgen.New(tx)
	if in.UploadID != nil {
		if err = lockUploadOrigin(ctx, q, in.UserID, *in.UploadID); err != nil {
			return nil, err
		}
	} else {
		if _, err = q.LockCopyOperation(ctx, sqlcgen.LockCopyOperationParams{UserID: in.UserID, OperationID: dbtypes.UUID(*in.OperationID)}); err != nil {
			return nil, fmt.Errorf("lock copy origin: %w", err)
		}
	}
	if err = RequireWritableChannelsTx(ctx, tx, []int64{in.ChannelID}); err != nil {
		return nil, err
	}
	row, err := q.ReserveMessageReceipt(ctx, sqlcgen.ReserveMessageReceiptParams{ID: dbtypes.UUID(in.ID), UserID: in.UserID, ChannelID: in.ChannelID, OriginKind: in.OriginKind, UploadID: dbtypes.OptionalUUID(in.UploadID), PartNo: in.PartNo, LeaseToken: dbtypes.UUID(in.LeaseToken), OperationID: dbtypes.OptionalUUID(in.OperationID), ResultFileID: dbtypes.OptionalUUID(in.ResultFileID), LeaseExpiresAt: dbtypes.Time(in.LeaseExpiresAt.UTC())})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrReceiptConflict
	}
	if err != nil {
		return nil, fmt.Errorf("reserve message receipt: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit receipt reservation: %w", err)
	}
	return row, nil
}

type ObserveInput struct {
	ReceiptID    uuid.UUID
	UserID       int64
	ChannelID    int64
	MessageID    int64
	LeaseToken   uuid.UUID
	UploadID     *uuid.UUID
	PartNo       int32
	OperationID  *uuid.UUID
	ResultFileID *uuid.UUID
}

func (s *Service) Observe(ctx context.Context, in ObserveInput) (*sqlcgen.RemoteWriteReceipt, error) {
	if in.ReceiptID == uuid.Nil || in.UserID <= 0 || in.ChannelID == 0 || in.MessageID <= 0 || in.LeaseToken == uuid.Nil {
		return nil, ErrInvalidInput
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin receipt observation: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := s.lockRowsTx(ctx, tx, in.UserID, []uuid.UUID{in.ReceiptID}, []PhysicalMessage{{ChannelID: in.ChannelID, MessageID: in.MessageID}})
	if err != nil {
		return nil, err
	}
	r := rows[0]
	if r.RowKind != "message_attempt" || !r.ChannelID.Valid || r.ChannelID.Int64 != in.ChannelID || r.LeaseToken != dbtypes.UUID(in.LeaseToken) || r.UploadID != dbtypes.OptionalUUID(in.UploadID) || r.OperationID != dbtypes.OptionalUUID(in.OperationID) || r.ResultFileID != dbtypes.OptionalUUID(in.ResultFileID) || (in.UploadID != nil && (!r.PartNo.Valid || r.PartNo.Int32 != in.PartNo)) {
		return nil, ErrReceiptConflict
	}
	if r.MessageID.Valid && r.MessageID.Int64 == in.MessageID && (r.State == "bound" || r.State == "deleting" || r.State == "deleted" || r.State == "exhausted") {
		return r, nil
	}
	q := sqlcgen.New(tx)
	row, err := q.ObserveMessageReceipt(ctx, sqlcgen.ObserveMessageReceiptParams{ReceiptID: dbtypes.UUID(in.ReceiptID), UserID: in.UserID, ChannelID: in.ChannelID, MessageID: in.MessageID, LeaseToken: dbtypes.UUID(in.LeaseToken)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrReceiptConflict
	}
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == "23505" {
			return nil, errors.Join(ErrReceiptConflict, err)
		}
		return nil, fmt.Errorf("observe destination receipt: %w", err)
	}
	if row.State == "cleanup_pending" || (row.State == "exhausted" && !row.JobID.Valid) {
		now, e := q.LifecycleDatabaseTime(ctx)
		if e != nil {
			return nil, e
		}
		if !r.MessageID.Valid || !row.JobID.Valid || r.State != row.State || !row.NextAttemptAt.Valid || !row.NextAttemptAt.Time.After(now.Time) {
			if err = s.enqueueTx(ctx, tx, in.UserID, []uuid.UUID{in.ReceiptID}); err != nil {
				return nil, err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit receipt observation: %w", err)
	}
	return row, nil
}

type BindInput struct {
	UserID       int64
	ReceiptIDs   []uuid.UUID
	ResultFileID *uuid.UUID
}

func (s *Service) BindTx(ctx context.Context, tx pgx.Tx, in BindInput) error {
	rows, err := s.lockRowsTx(ctx, tx, in.UserID, in.ReceiptIDs, nil)
	if err != nil {
		return err
	}
	q := sqlcgen.New(tx)
	for _, r := range rows {
		if !r.ChannelID.Valid || !r.MessageID.Valid {
			return ErrReceiptConflict
		}
		if err = RequireWritableChannelsTx(ctx, tx, []int64{r.ChannelID.Int64}); err != nil {
			return err
		}
		if _, err = q.BindMessageReceipt(ctx, sqlcgen.BindMessageReceiptParams{ReceiptID: r.ID, UserID: in.UserID, ResultFileID: dbtypes.OptionalUUID(in.ResultFileID)}); errors.Is(err, pgx.ErrNoRows) {
			return ErrDeletionFenced
		} else if err != nil {
			return fmt.Errorf("bind message receipt: %w", err)
		}
	}
	return nil
}

type StageCleanupInput struct {
	UserID      int64
	ReceiptIDs  []uuid.UUID
	PrimaryCode string
}

func (s *Service) StageCleanupTx(ctx context.Context, tx pgx.Tx, in StageCleanupInput) error {
	if !safeCode(in.PrimaryCode) {
		return ErrInvalidInput
	}
	rows, err := s.lockRowsTx(ctx, tx, in.UserID, in.ReceiptIDs, nil)
	if err != nil {
		return err
	}
	return s.stageRowsTx(ctx, tx, in, rows)
}
func (s *Service) stageRowsTx(ctx context.Context, tx pgx.Tx, in StageCleanupInput, rows []*sqlcgen.RemoteWriteReceipt) error {
	q := sqlcgen.New(tx)
	enqueue := make([]uuid.UUID, 0, len(rows))
	now, err := q.LifecycleDatabaseTime(ctx)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if r.State == "deleted" || (r.State == "unresolved" && r.JobID.Valid) || r.PublishedAt.Valid {
			continue
		}
		if r.State == "exhausted" {
			if !r.JobID.Valid {
				enqueue = append(enqueue, uuid.UUID(r.ID.Bytes))
			}
			continue
		}
		if r.RowKind == "copy_operation" {
			if r.TerminalAt.Valid && !r.JobID.Valid {
				enqueue = append(enqueue, uuid.UUID(r.ID.Bytes))
			}
			continue
		}
		staged, e := q.StageReceiptCleanup(ctx, sqlcgen.StageReceiptCleanupParams{ReceiptID: r.ID, UserID: in.UserID, PrimaryCode: dbtypes.Text(in.PrimaryCode)})
		if errors.Is(e, pgx.ErrNoRows) {
			continue
		}
		if e != nil {
			return fmt.Errorf("stage receipt cleanup: %w", e)
		}
		if !staged.JobID.Valid || (staged.State == "cleanup_pending" && (!staged.NextAttemptAt.Valid || !staged.NextAttemptAt.Time.After(now.Time))) {
			enqueue = append(enqueue, uuid.UUID(r.ID.Bytes))
		}
	}
	if len(enqueue) == 0 {
		return nil
	}
	return s.enqueueTx(ctx, tx, in.UserID, enqueue)
}
func (s *Service) enqueueTx(ctx context.Context, tx pgx.Tx, userID int64, ids []uuid.UUID) error {
	if s.receiptEnqueuer == nil {
		return ErrEnqueuerUnavailable
	}
	q := sqlcgen.New(tx)
	for start := 0; start < len(ids); start += 1000 {
		batch := ids[start:min(start+1000, len(ids))]
		jobID, err := s.receiptEnqueuer.EnqueueReceiptCleanupTx(ctx, tx, ReceiptCleanupInput{UserID: userID, ReceiptIDs: batch})
		if err != nil {
			return fmt.Errorf("enqueue receipt cleanup: %w", err)
		}
		if jobID <= 0 {
			return fmt.Errorf("enqueue receipt cleanup returned nonpositive job ID: %w", ErrEnqueuerUnavailable)
		}
		count, err := q.AssignReceiptCleanupJob(ctx, sqlcgen.AssignReceiptCleanupJobParams{JobID: jobID, UserID: userID, ReceiptIds: pgIDs(batch)})
		if err != nil {
			return fmt.Errorf("assign actual receipt job ID: %w", err)
		}
		if count != int64(len(batch)) {
			return fmt.Errorf("assign actual receipt job ID: %w", ErrReceiptConflict)
		}
	}
	return nil
}

// ClaimDelete commits a conservative invocation reservation before returning.
func (s *Service) ClaimDelete(ctx context.Context, userID int64, ids []uuid.UUID) ([]*sqlcgen.RemoteWriteReceipt, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin receipt deletion claim: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := s.lockRowsTx(ctx, tx, userID, ids, nil)
	if err != nil {
		return nil, err
	}
	q := sqlcgen.New(tx)
	claimed := make([]*sqlcgen.RemoteWriteReceipt, 0, len(rows))
	for _, r := range rows {
		if _, err = q.ExhaustReceiptBudget(ctx, sqlcgen.ExhaustReceiptBudgetParams{ReceiptID: r.ID, UserID: userID}); err != nil {
			return nil, err
		}
		row, e := q.ClaimReceiptDeletion(ctx, sqlcgen.ClaimReceiptDeletionParams{ReceiptID: r.ID, UserID: userID})
		if errors.Is(e, pgx.ErrNoRows) {
			continue
		}
		if e != nil {
			return nil, fmt.Errorf("claim exact receipt deletion: %w", e)
		}
		claimed = append(claimed, row)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit deletion claim: %w", err)
	}
	return claimed, nil
}

// AcknowledgeDelete accepts only the exact committed claim after positive ACK.
func (s *Service) AcknowledgeDelete(ctx context.Context, userID int64, claims []*sqlcgen.RemoteWriteReceipt) error {
	if len(claims) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, len(claims))
	for i, r := range claims {
		if r == nil || r.UserID != userID || !r.MessageID.Valid || !r.ChannelID.Valid || r.State != "deleting" || r.Attempts < 1 || r.Attempts > 4 {
			return ErrInvalidInput
		}
		ids[i] = uuid.UUID(r.ID.Bytes)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin receipt ACK: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err = s.lockRowsTx(ctx, tx, userID, ids, nil); err != nil {
		return err
	}
	q := sqlcgen.New(tx)
	for _, r := range claims {
		count, e := q.AcknowledgeReceiptDeletion(ctx, sqlcgen.AcknowledgeReceiptDeletionParams{ReceiptID: r.ID, UserID: userID, ChannelID: r.ChannelID.Int64, MessageID: r.MessageID.Int64, Attempts: r.Attempts})
		if e != nil {
			return fmt.Errorf("acknowledge exact receipt: %w", e)
		}
		if count != 1 {
			return ErrReceiptConflict
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit receipt ACK: %w", err)
	}
	return nil
}
func (s *Service) recordFailure(ctx context.Context, userID int64, claims []*sqlcgen.RemoteWriteReceipt) error {
	ids := make([]uuid.UUID, len(claims))
	for i, r := range claims {
		ids[i] = uuid.UUID(r.ID.Bytes)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = s.lockRowsTx(ctx, tx, userID, ids, nil); err != nil {
		return err
	}
	q := sqlcgen.New(tx)
	for _, r := range claims {
		n, e := q.RecordReceiptDeletionFailure(ctx, sqlcgen.RecordReceiptDeletionFailureParams{ReceiptID: r.ID, UserID: userID, Attempts: r.Attempts, FailureCode: "remote_delete_failed"})
		if e != nil {
			return fmt.Errorf("persist receipt deletion failure: %w", e)
		}
		if n != 1 {
			return ErrReceiptConflict
		}
	}
	return tx.Commit(ctx)
}

// Recover has one detached five-second budget across all claims, RPCs and ACKs.
func (s *Service) Recover(ctx context.Context, userID int64, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	if s.storage == nil {
		return ErrInvalidInput
	}
	budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	return s.RecoverWithinBudget(budget, userID, ids)
}

// RecoverWithinBudget keeps the caller's cleanup deadline across persistence and deletion.
func (s *Service) RecoverWithinBudget(ctx context.Context, userID int64, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	if s.storage == nil {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	claims, err := s.ClaimDelete(ctx, userID, ids)
	if err != nil {
		return err
	}
	var failures []error
	for start := 0; start < len(claims); {
		end := start + 1
		for end < len(claims) && claims[end].ChannelID == claims[start].ChannelID {
			end++
		}
		group := claims[start:end]
		messages := make([]int64, len(group))
		for i, r := range group {
			messages[i] = r.MessageID.Int64
		}
		if e := s.storage.DeleteMessages(ctx, userID, group[0].ChannelID.Int64, messages); e != nil {
			failures = append(failures, errors.Join(e, s.recordFailure(ctx, userID, group)))
		} else if e = s.AcknowledgeDelete(ctx, userID, group); e != nil {
			failures = append(failures, e)
		}
		start = end
	}
	rows, e := s.queries.ListRemoteWriteReceipts(ctx, sqlcgen.ListRemoteWriteReceiptsParams{UserID: userID, ReceiptIds: pgIDs(ids)})
	if e != nil {
		failures = append(failures, e)
	} else {
		for _, r := range rows {
			if r.State == "unresolved" {
				failures = append(failures, ErrUnresolved)
			}
			if r.State == "exhausted" {
				failures = append(failures, ErrAttemptsExhausted)
			}
		}
	}
	return errors.Join(failures...)
}

// CleanupMessageResult contains safe counters; exact acknowledged IDs stay internal.
type CleanupMessageResult struct {
	Deleted           int     `json:"deleted"`
	Acknowledged      int     `json:"acknowledged"`
	Unknown           int     `json:"unknown"`
	Foreign           int     `json:"foreign"`
	Protected         int     `json:"protected"`
	Pending           int     `json:"pending"`
	Exhausted         int     `json:"exhausted"`
	Failed            int     `json:"failed"`
	DeletedMessageIDs []int64 `json:"-"`
}

// CleanupMessagesWithinBudget resolves provenance without inferring it from a scan.
// The caller supplies its existing operation budget; this method never renews it.
func (s *Service) CleanupMessagesWithinBudget(ctx context.Context, userID, channelID int64, messageIDs []int64) (CleanupMessageResult, error) {
	var result CleanupMessageResult
	if userID <= 0 || channelID == 0 || len(messageIDs) > 1000 {
		return result, ErrInvalidInput
	}
	if len(messageIDs) == 0 {
		return result, nil
	}
	ids := slices.Clone(messageIDs)
	for _, id := range ids {
		if id <= 0 {
			return result, ErrInvalidInput
		}
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	params := sqlcgen.ListCleanupMessageDispositionsParams{ChannelID: channelID, MessageIds: ids}
	rows, err := s.queries.ListCleanupMessageDispositions(ctx, params)
	if err != nil {
		result.Failed = len(ids)
		return result, fmt.Errorf("read cleanup provenance: %w", err)
	}
	var initiallyDeleted map[int64]bool
	var recoverIDs, stageIDs []uuid.UUID
	for _, r := range rows {
		if r.UserID != userID || r.HasReferences || !r.OriginEligible || r.State == "exhausted" {
			continue
		}
		if r.State == "deleted" {
			if initiallyDeleted == nil {
				initiallyDeleted = make(map[int64]bool)
			}
			initiallyDeleted[r.MessageID] = true
			continue
		}
		if r.State == "unresolved" {
			continue
		}
		id := uuid.UUID(r.ID.Bytes)
		recoverIDs = append(recoverIDs, id)
		if r.State == "observed" || r.State == "bound" {
			stageIDs = append(stageIDs, id)
		}
	}
	var cleanupErr error
	if len(stageIDs) > 0 {
		tx, e := s.pool.Begin(ctx)
		if e == nil {
			e = s.StageCleanupTx(ctx, tx, StageCleanupInput{UserID: userID, ReceiptIDs: stageIDs, PrimaryCode: "cleanup_scan"})
			if e == nil {
				e = tx.Commit(ctx)
			}
			_ = tx.Rollback(ctx)
		}
		if e != nil {
			cleanupErr = fmt.Errorf("stage exact scan receipts: %w", e)
		}
	}
	if cleanupErr == nil && len(recoverIDs) > 0 {
		cleanupErr = s.RecoverWithinBudget(ctx, userID, recoverIDs)
	}
	if len(recoverIDs) > 0 {
		rows, err = s.queries.ListCleanupMessageDispositions(ctx, params)
		if err != nil {
			result.Failed = len(ids)
			return result, errors.Join(cleanupErr, fmt.Errorf("read cleanup outcome: %w", err))
		}
	}
	result.Unknown = len(ids) - len(rows)
	for _, r := range rows {
		switch {
		case r.UserID != userID:
			result.Foreign++
		case r.HasReferences || !r.OriginEligible:
			result.Protected++
		case r.State == "deleted":
			result.Acknowledged++
			result.DeletedMessageIDs = append(result.DeletedMessageIDs, r.MessageID)
			if !initiallyDeleted[r.MessageID] {
				result.Deleted++
			}
		case r.State == "exhausted":
			result.Exhausted++
		case cleanupErr != nil:
			result.Failed++
		default:
			result.Pending++
		}
	}
	return result, cleanupErr
}

type RecoveryCandidatesInput struct {
	PassCutoff     time.Time
	AfterCreatedAt *time.Time
	AfterID        *uuid.UUID
	PageSize       int32
}

func (s *Service) ListLifecycleRecoveryCandidates(ctx context.Context, in RecoveryCandidatesInput) ([]*sqlcgen.RemoteWriteReceipt, error) {
	if !validTime(in.PassCutoff) || (in.AfterCreatedAt == nil) != (in.AfterID == nil) || in.PageSize != 1000 {
		return nil, ErrInvalidInput
	}
	return s.queries.ListLifecycleRecoveryCandidates(ctx, sqlcgen.ListLifecycleRecoveryCandidatesParams{PassCutoff: dbtypes.Time(in.PassCutoff.UTC()), AfterCreatedAt: dbtypes.OptionalTime(in.AfterCreatedAt), AfterID: dbtypes.OptionalUUID(in.AfterID), PageSize: in.PageSize})
}
func (s *Service) DiscoverRecoveryCandidate(ctx context.Context, candidateID uuid.UUID, passCutoff time.Time) error {
	if candidateID == uuid.Nil || !validTime(passCutoff) {
		return ErrInvalidInput
	}
	budget, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	hint, err := s.queries.GetRemoteWriteReceipt(budget, dbtypes.UUID(candidateID))
	if err != nil {
		return fmt.Errorf("read recovery hint: %w", err)
	}
	if hint.CreatedAt.Time.After(passCutoff) {
		return nil
	}
	tx, err := s.pool.Begin(budget)
	if err != nil {
		return err
	}
	defer tx.Rollback(budget)
	rows, err := s.lockRowsTx(budget, tx, hint.UserID, []uuid.UUID{candidateID}, nil)
	if err != nil {
		return err
	}
	r := rows[0]
	q := sqlcgen.New(tx)
	if r.State == "deleted" || r.State == "bound" || r.PublishedAt.Valid || ((r.State == "unresolved" || r.State == "exhausted") && r.JobID.Valid) {
		return nil
	}
	now, err := q.LifecycleDatabaseTime(budget)
	if err != nil {
		return err
	}
	if r.NextAttemptAt.Valid && r.NextAttemptAt.Time.After(now.Time) {
		return nil
	}
	if r.OriginKind == "copy" {
		op, err := q.GetRemoteWriteReceipt(budget, r.OperationID)
		if err != nil {
			return err
		}
		if op.PublishedAt.Valid {
			// Only a previously staged exact deletion_pending file target can
			// outlive publication; the shared SQL predicate rechecks that target.
			if r.RowKind == "copy_operation" || (r.State != "cleanup_pending" && r.State != "deleting") {
				return nil
			}
		} else if !op.TerminalAt.Valid {
			if op.LeaseExpiresAt.Time.After(now.Time) {
				return nil
			}
			if _, err = q.TerminalizeCopyOperation(budget, sqlcgen.TerminalizeCopyOperationParams{UserID: r.UserID, OperationID: op.ID, FailureCode: "remote_write_abandoned"}); err != nil {
				return err
			}
		}
		if r.RowKind == "copy_operation" {
			if err = s.enqueueTx(budget, tx, r.UserID, []uuid.UUID{candidateID}); err != nil {
				return err
			}
			return tx.Commit(budget)
		}
	} else {
		if _, err = q.FenceExpiredUploadReceiptOrigin(budget, sqlcgen.FenceExpiredUploadReceiptOriginParams{UserID: r.UserID, ReceiptID: r.ID}); err != nil {
			return err
		}
	}
	if _, err = q.ExhaustReceiptBudget(budget, sqlcgen.ExhaustReceiptBudgetParams{UserID: r.UserID, ReceiptID: r.ID}); err != nil {
		return err
	}
	current, err := q.GetRemoteWriteReceipt(budget, r.ID)
	if err != nil {
		return err
	}
	if current.State == "exhausted" {
		if !current.JobID.Valid {
			if err = s.enqueueTx(budget, tx, r.UserID, []uuid.UUID{candidateID}); err != nil {
				return err
			}
		}
		return tx.Commit(budget)
	}
	staged, err := q.StageReceiptCleanup(budget, sqlcgen.StageReceiptCleanupParams{UserID: r.UserID, ReceiptID: r.ID, PrimaryCode: dbtypes.Text("remote_write_abandoned")})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !staged.JobID.Valid || staged.State == "cleanup_pending" || staged.State == "deleting" {
		if err = s.enqueueTx(budget, tx, r.UserID, []uuid.UUID{candidateID}); err != nil {
			return err
		}
	}
	if err = tx.Commit(budget); err != nil {
		return fmt.Errorf("commit recovery disposition: %w", err)
	}
	return nil
}

// TerminalizeCopyDestinationsTx joins the hierarchy mutation's caller transaction.
func (s *Service) TerminalizeCopyDestinationsTx(ctx context.Context, tx pgx.Tx, userID int64, subtreeIDs []uuid.UUID) (operations int64, receipts int64, err error) {
	if err = LockOwnerTx(ctx, tx, userID); err != nil {
		return 0, 0, err
	}
	if len(subtreeIDs) == 0 {
		return 0, 0, nil
	}
	q := sqlcgen.New(tx)
	ops, err := q.ListCopyDestinationOperations(ctx, sqlcgen.ListCopyDestinationOperationsParams{UserID: userID, SubtreeIds: pgIDs(subtreeIDs)})
	if err != nil {
		return 0, 0, err
	}
	if len(ops) == 0 {
		return 0, 0, nil
	}
	opIDs := make([]uuid.UUID, len(ops))
	for i, r := range ops {
		opIDs[i] = uuid.UUID(r.ID.Bytes)
	}
	rows, err := q.ListCopyOperationReceipts(ctx, sqlcgen.ListCopyOperationReceiptsParams{UserID: userID, OperationIds: pgIDs(opIDs)})
	if err != nil {
		return 0, 0, err
	}
	ids := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		ids[i] = uuid.UUID(r.ID.Bytes)
	}
	if _, err = s.lockRowsTx(ctx, tx, userID, ids, nil); err != nil {
		return 0, 0, err
	}
	for _, op := range ops {
		n, e := q.TerminalizeCopyOperation(ctx, sqlcgen.TerminalizeCopyOperationParams{UserID: userID, OperationID: op.ID, FailureCode: "copy_destination_unavailable"})
		if e != nil {
			return 0, 0, e
		}
		operations += n
	}
	rows, err = q.ListRemoteWriteReceipts(ctx, sqlcgen.ListRemoteWriteReceiptsParams{UserID: userID, ReceiptIds: pgIDs(ids)})
	if err != nil {
		return 0, 0, err
	}
	if err = s.stageRowsTx(ctx, tx, StageCleanupInput{UserID: userID, ReceiptIDs: ids, PrimaryCode: "copy_destination_unavailable"}, rows); err != nil {
		return 0, 0, err
	}
	rows, err = q.ListRemoteWriteReceipts(ctx, sqlcgen.ListRemoteWriteReceiptsParams{UserID: userID, ReceiptIds: pgIDs(ids)})
	if err != nil {
		return 0, 0, err
	}
	for _, r := range rows {
		if r.RowKind == "message_attempt" && r.MessageID.Valid && (r.State == "cleanup_pending" || r.State == "deleting" || r.State == "exhausted") {
			receipts++
		}
	}
	return operations, receipts, nil
}

func (s *Service) ListReceiptJobReports(ctx context.Context, in sqlcgen.ListReceiptJobReportsParams) ([]*sqlcgen.ListReceiptJobReportsRow, error) {
	if in.UserID <= 0 || in.PageSize < 1 || in.PageSize > 1000 {
		return nil, ErrInvalidInput
	}
	return s.queries.ListReceiptJobReports(ctx, in)
}
func (s *Service) GetReceiptJobReport(ctx context.Context, userID, jobID int64) (*sqlcgen.GetReceiptJobReportRow, error) {
	if userID <= 0 || jobID <= 0 {
		return nil, ErrInvalidInput
	}
	report, err := s.queries.GetReceiptJobReport(ctx, sqlcgen.GetReceiptJobReportParams{UserID: userID, JobID: jobID})
	if err != nil {
		return nil, err
	}
	return report, nil
}

// lockRowsTx starts from unlocked hints, then acquires C2 origins before physical
// keys and receipt rows. A fresh row read never turns the hint into authority.
func (s *Service) lockRowsTx(ctx context.Context, tx pgx.Tx, userID int64, ids []uuid.UUID, extra []PhysicalMessage) ([]*sqlcgen.RemoteWriteReceipt, error) {
	ids, err := exactIDs(ids)
	if err != nil {
		return nil, err
	}
	if err = LockOwnerTx(ctx, tx, userID); err != nil {
		return nil, err
	}
	q := sqlcgen.New(tx)
	pg := pgIDs(ids)
	rows, err := q.ListRemoteWriteReceipts(ctx, sqlcgen.ListRemoteWriteReceiptsParams{UserID: userID, ReceiptIds: pg})
	if err != nil {
		return nil, err
	}
	if len(rows) != len(ids) {
		return nil, ErrReceiptConflict
	}
	if err = q.LockReceiptImportBatches(ctx, sqlcgen.LockReceiptImportBatchesParams{UserID: userID, ReceiptIds: pg}); err != nil {
		return nil, err
	}
	if err = q.LockReceiptImportPins(ctx, sqlcgen.LockReceiptImportPinsParams{UserID: userID, ReceiptIds: pg}); err != nil {
		return nil, err
	}
	if err = q.LockReceiptImportAttempts(ctx, sqlcgen.LockReceiptImportAttemptsParams{UserID: userID, ReceiptIds: pg}); err != nil {
		return nil, err
	}
	if err = q.LockReceiptCopyOperations(ctx, sqlcgen.LockReceiptCopyOperationsParams{UserID: userID, ReceiptIds: pg}); err != nil {
		return nil, err
	}
	if err = q.LockReceiptUploadSessions(ctx, sqlcgen.LockReceiptUploadSessionsParams{UserID: userID, ReceiptIds: pg}); err != nil {
		return nil, err
	}
	channels := make([]int64, 0, len(rows)+len(extra))
	messages := make([]PhysicalMessage, 0, len(rows)+len(extra))
	messages = append(messages, extra...)
	for _, p := range extra {
		channels = append(channels, p.ChannelID)
	}
	for _, r := range rows {
		if r.ChannelID.Valid {
			channels = append(channels, r.ChannelID.Int64)
			if r.MessageID.Valid {
				messages = append(messages, PhysicalMessage{r.ChannelID.Int64, r.MessageID.Int64})
			}
		}
	}
	if err = LockChannelWritesTx(ctx, tx, channels); err != nil {
		return nil, err
	}
	if err = LockMessagesTx(ctx, tx, messages); err != nil {
		return nil, err
	}
	return q.LockRemoteWriteReceipts(ctx, sqlcgen.LockRemoteWriteReceiptsParams{UserID: userID, ReceiptIds: pg})
}
func lockUploadOrigin(ctx context.Context, q *sqlcgen.Queries, userID int64, uploadID uuid.UUID) error {
	if err := q.LockUploadOriginBatch(ctx, sqlcgen.LockUploadOriginBatchParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)}); err != nil {
		return err
	}
	if err := q.LockUploadOriginPins(ctx, sqlcgen.LockUploadOriginPinsParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)}); err != nil {
		return err
	}
	if err := q.LockUploadOriginAttempt(ctx, sqlcgen.LockUploadOriginAttemptParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)}); err != nil {
		return err
	}
	_, err := q.LockUploadSessionForUser(ctx, sqlcgen.LockUploadSessionForUserParams{UserID: userID, UploadID: dbtypes.UUID(uploadID)})
	return err
}
func exactIDs(ids []uuid.UUID) ([]uuid.UUID, error) {
	if len(ids) == 0 {
		return nil, ErrInvalidInput
	}
	out := slices.Clone(ids)
	for _, id := range out {
		if id == uuid.Nil {
			return nil, ErrInvalidInput
		}
	}
	slices.SortFunc(out, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	return slices.Compact(out), nil
}
func pgIDs(ids []uuid.UUID) []pgtype.UUID {
	result := make([]pgtype.UUID, len(ids))
	for i, id := range ids {
		result[i] = dbtypes.UUID(id)
	}
	return result
}
func validID(id *uuid.UUID) bool { return id != nil && *id != uuid.Nil }
func validTime(t time.Time) bool { return !t.IsZero() && t.Year() > 0 && t.Year() < 10000 }
func safeCode(code string) bool {
	if len(code) < 1 || len(code) > 64 || code[0] < 'a' || code[0] > 'z' {
		return false
	}
	for _, c := range code {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}
