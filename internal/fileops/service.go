package fileops

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

var (
	ErrInvalidInput         = errors.New("invalid file operation input")
	ErrNotFound             = errors.New("file not found")
	ErrNotTrashed           = errors.New("file must be trashed before permanent deletion")
	ErrLifecycleUnavailable = errors.New("file operation lifecycle is not configured")
	ErrPurgeIncomplete      = errors.New("physical purge is incomplete")
)

type Service struct {
	pool      *pgxpool.Pool
	queries   *sqlcgen.Queries
	catalog   *catalog.Service
	channels  *channels.Service
	storage   telegramstore.Storage
	lifecycle *lifecycle.Service
	commit    func(context.Context, pgx.Tx) error
}

type CopyInput struct {
	UserID         int64
	FileID         uuid.UUID
	ParentID       *uuid.UUID
	Name           *string
	ConflictPolicy sqlcgen.NameConflictPolicy
	Admission      *lifecycle.RequestAdmissionInput
}

type treeNode struct {
	File  sqlcgen.File
	Depth int32
}

type copiedPart struct {
	Part      sqlcgen.FilePart
	Stored    telegramstore.StoredPart
	ReceiptID uuid.UUID
}

type copiedFileRecord struct {
	ID                   uuid.UUID  `json:"id"`
	UserID               int64      `json:"user_id"`
	ParentID             *uuid.UUID `json:"parent_id"`
	Name                 string     `json:"name"`
	Kind                 string     `json:"kind"`
	MIMEType             *string    `json:"mime_type"`
	Size                 *int64     `json:"size"`
	HashAlgorithm        *string    `json:"hash_algorithm"`
	HashValue            *string    `json:"hash_value"`
	Encryption           bool       `json:"encryption"`
	EncryptionKeyVersion *int32     `json:"encryption_key_version"`
	ModTime              time.Time  `json:"mod_time"`
}

type copiedFilePartRecord struct {
	FileID      uuid.UUID `json:"file_id"`
	PartNo      int32     `json:"part_no"`
	ChannelID   int64     `json:"channel_id"`
	MessageID   int64     `json:"message_id"`
	PlainSize   *int64    `json:"plain_size"`
	StoredSize  *int64    `json:"stored_size"`
	Checksum    *string   `json:"checksum"`
	Salt        *string   `json:"salt"`
	BlockHashes []byte    `json:"block_hashes"`
	ReceiptID   uuid.UUID `json:"receipt_id"`
}

func NewService(pool *pgxpool.Pool, catalogService *catalog.Service, channelService *channels.Service, storage telegramstore.Storage) (*Service, error) {
	if pool == nil || catalogService == nil || channelService == nil || storage == nil {
		return nil, ErrInvalidInput
	}
	return &Service{pool: pool, queries: sqlcgen.New(pool), catalog: catalogService, channels: channelService, storage: storage, commit: func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) }}, nil
}

// BindLifecycle is initialization-only; every publisher and deleter shares one policy.
func (s *Service) BindLifecycle(policy *lifecycle.Service) error {
	if policy == nil || s.lifecycle != nil {
		return ErrLifecycleUnavailable
	}
	s.lifecycle = policy
	return nil
}

// GetRequestAdmission returns a scoped binding hint, not authorization.
func (s *Service) GetRequestAdmission(ctx context.Context, scope lifecycle.RequestScope, key uuid.UUID) (*sqlcgen.RequestAdmission, error) {
	if s.lifecycle == nil {
		return nil, ErrLifecycleUnavailable
	}
	return s.lifecycle.GetRequestAdmission(ctx, scope, "copy", key)
}

// GetCopyOperation returns an owner-scoped copy operation hint, not authorization.
func (s *Service) GetCopyOperation(ctx context.Context, userID int64, operationID uuid.UUID) (*sqlcgen.RemoteWriteReceipt, error) {
	if userID <= 0 || operationID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	if s.lifecycle == nil {
		return nil, ErrLifecycleUnavailable
	}
	operation, err := s.queries.GetRemoteWriteReceipt(ctx, dbtypes.UUID(operationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get copy operation: %w", err)
	}
	if operation.UserID != userID || operation.RowKind != "copy_operation" {
		return nil, ErrNotFound
	}
	if operation.PublishedAt.Valid {
		if _, err := s.activeCopyResult(ctx, userID, operation.ResultFileID); err != nil {
			return nil, err
		}
	}
	return operation, nil
}

const copyLeaseTTL = time.Minute

func (s *Service) Copy(ctx context.Context, in CopyInput) (*sqlcgen.File, error) {
	if in.UserID <= 0 || in.FileID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	if s.lifecycle == nil {
		return nil, ErrLifecycleUnavailable
	}
	if in.Admission != nil {
		binding, err := s.lifecycle.GetRequestAdmission(ctx, in.Admission.Scope, "copy", in.Admission.RequestKey)
		if err == nil {
			if binding.UserID != in.UserID || !bytes.Equal(binding.RequestFingerprint, in.Admission.RequestFingerprint[:]) {
				return nil, lifecycle.ErrRequestMismatch
			}
			return s.copyOutcome(ctx, in.UserID, binding.CopyOperationID)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
	}
	if in.ConflictPolicy == "" {
		in.ConflictPolicy = sqlcgen.NameConflictPolicyFail
	}
	switch in.ConflictPolicy {
	case sqlcgen.NameConflictPolicyFail, sqlcgen.NameConflictPolicyRename, sqlcgen.NameConflictPolicyReplace:
	default:
		return nil, ErrInvalidInput
	}
	operationID, rootNewID, token := uuid.New(), uuid.New(), uuid.New()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err = lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, err
	}
	q := s.queries.WithTx(tx)
	operation, created, err := s.lifecycle.ReserveCopyTx(ctx, tx, lifecycle.ReserveCopyInput{
		UserID: in.UserID, Admission: in.Admission, OperationID: operationID, RootResultFileID: rootNewID,
		LeaseToken: token, DestinationParentID: in.ParentID, DestinationIsRoot: in.ParentID == nil,
		LeaseExpiresAt: time.Now().UTC().Add(copyLeaseTTL),
	})
	if err != nil {
		return nil, err
	}
	if !created {
		if err = tx.Rollback(ctx); err != nil {
			return nil, err
		}
		return s.copyOutcome(ctx, in.UserID, operation.ID)
	}
	source, err := q.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{UserID: in.UserID, FileID: dbtypes.UUID(in.FileID)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if source.Status != sqlcgen.FileStatusActive {
		return nil, ErrNotFound
	}
	// A lost reservation ACK never authorizes a send, even if reservation committed.
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit copy reservation: %w", err)
	}
	var budget context.Context
	var cancel context.CancelFunc
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	cleanupContext := func() context.Context {
		if budget == nil {
			budget, cancel = context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		}
		return budget
	}
	fail := func(primary error) (*sqlcgen.File, error) {
		if op, readErr := s.queries.GetRemoteWriteReceipt(cleanupContext(), dbtypes.UUID(operationID)); readErr == nil && op.TerminalFailureCode.String == "copy_destination_unavailable" {
			primary = errors.Join(lifecycle.ErrCopyDestinationUnavailable, primary)
		}
		code := "copy_failed"
		switch {
		case errors.Is(primary, lifecycle.ErrCopyDestinationUnavailable):
			code = "copy_destination_unavailable"
		case errors.Is(primary, catalog.ErrConflict):
			code = "copy_conflict"
		case errors.Is(primary, ErrNotFound), errors.Is(primary, catalog.ErrNotFound):
			code = "copy_source_unavailable"
		}
		return nil, errors.Join(primary, s.abandonCopy(cleanupContext(), in.UserID, operationID, code))
	}
	nodes, err := s.loadTree(ctx, in.UserID, in.FileID)
	if err != nil {
		return fail(err)
	}
	idMap := make(map[uuid.UUID]uuid.UUID, len(nodes))
	for _, node := range nodes {
		oldID, ok := dbtypes.GoogleUUID(node.File.ID)
		if !ok || node.File.Status != sqlcgen.FileStatusActive {
			return fail(ErrNotFound)
		}
		if oldID == in.FileID {
			idMap[oldID] = rootNewID
		} else {
			idMap[oldID] = uuid.New()
		}
	}
	rootName := source.Name
	if in.Name != nil {
		rootName = *in.Name
	}
	sourceIDs := make([]uuid.UUID, 0, len(nodes))
	for _, node := range nodes {
		if node.File.Kind == sqlcgen.FileKindFile {
			sourceIDs = append(sourceIDs, uuid.UUID(node.File.ID.Bytes))
		}
	}
	parts, err := s.queries.ListFilePartsByFileIDs(ctx, pgUUIDs(sourceIDs))
	if err != nil {
		return fail(fmt.Errorf("list copied file parts: %w", err))
	}
	destinationChannels, err := s.channels.ResolveMany(ctx, in.UserID, len(parts))
	if err != nil {
		return fail(err)
	}
	copied := make(map[uuid.UUID][]copiedPart, len(sourceIDs))
	for index, part := range parts {
		if err = s.renewCopy(ctx, in.UserID, operationID, token); err != nil {
			return fail(err)
		}
		oldID, ok := dbtypes.GoogleUUID(part.FileID)
		if !ok {
			return fail(ErrNotFound)
		}
		resultID := idMap[oldID]
		receiptID := uuid.New()
		_, err = s.lifecycle.Reserve(ctx, lifecycle.ReserveInput{
			ID: receiptID, UserID: in.UserID, ChannelID: destinationChannels[index], OriginKind: "copy",
			PartNo: part.PartNo, LeaseToken: token, OperationID: &operationID, ResultFileID: &resultID,
			LeaseExpiresAt: time.Now().UTC().Add(copyLeaseTTL),
		})
		if err != nil {
			return fail(err)
		}
		stored, remoteErr := s.storage.CopyPart(ctx, in.UserID, part.ChannelID, part.MessageID, destinationChannels[index])
		// Only a validated destination identity may enter the ledger, including receipt-plus-error.
		trusted := stored.ChannelID == destinationChannels[index] && stored.MessageID > 0 &&
			(stored.ChannelID != part.ChannelID || stored.MessageID != part.MessageID)
		if trusted {
			observed, observeErr := s.lifecycle.Observe(cleanupContext(), lifecycle.ObserveInput{
				ReceiptID: receiptID, UserID: in.UserID, ChannelID: stored.ChannelID, MessageID: stored.MessageID,
				LeaseToken: token, PartNo: part.PartNo, OperationID: &operationID, ResultFileID: &resultID,
			})
			if observeErr != nil {
				return fail(errors.Join(remoteErr, observeErr))
			}
			if observed.State != "observed" {
				return fail(errors.Join(lifecycle.ErrCopyDestinationUnavailable, remoteErr))
			}
		}
		if remoteErr != nil {
			primary := fmt.Errorf("copy Telegram part %d: %w", part.PartNo, remoteErr)
			if fenceErr := s.renewCopy(cleanupContext(), in.UserID, operationID, token); errors.Is(fenceErr, lifecycle.ErrCopyDestinationUnavailable) {
				primary = errors.Join(fenceErr, primary)
			}
			return fail(primary)
		}
		if !trusted {
			return fail(lifecycle.ErrReceiptConflict)
		}
		if !part.StoredSize.Valid || stored.Size != part.StoredSize.Int64 {
			return fail(telegramstore.ErrSizeMismatch)
		}
		copied[oldID] = append(copied[oldID], copiedPart{Part: *part, Stored: stored, ReceiptID: receiptID})
		// A successful observed step has no cleanup debt. A failing step retains its
		// same budget through abandonment, enqueue and every grouped delete.
		cancel()
		cancel = nil
		budget = nil
	}
	result, replaced, publishErr, uncertain := s.publishCopy(ctx, in, operationID, token, rootNewID, rootName, nodes, idMap, copied)
	if publishErr != nil {
		if uncertain {
			winner, outcomeErr := s.copyOutcome(cleanupContext(), in.UserID, dbtypes.UUID(operationID))
			if outcomeErr == nil {
				s.catalog.InvalidateFiles(cleanupContext(), in.UserID, replaced...)
				return winner, nil
			}
			// Unavailable or merely unfinished readback is not proof of rollback.
			return nil, errors.Join(publishErr, outcomeErr)
		}
		return fail(publishErr)
	}
	s.catalog.InvalidateFiles(ctx, in.UserID, replaced...)
	return result, nil
}

func (s *Service) copyOutcome(ctx context.Context, userID int64, operationID pgtype.UUID) (*sqlcgen.File, error) {
	op, err := s.queries.GetRemoteWriteReceipt(ctx, operationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read original copy outcome: %w", err)
	}
	if op.UserID != userID || op.RowKind != "copy_operation" {
		return nil, lifecycle.ErrRequestMismatch
	}
	if op.TerminalAt.Valid {
		switch op.TerminalFailureCode.String {
		case "copy_destination_unavailable":
			return nil, lifecycle.ErrCopyDestinationUnavailable
		case "copy_conflict":
			return nil, catalog.ErrConflict
		case "copy_source_unavailable":
			return nil, ErrNotFound
		default:
			return nil, fmt.Errorf("original copy operation failed: %s", op.TerminalFailureCode.String)
		}
	}
	if !op.PublishedAt.Valid {
		return nil, catalog.ErrConflict
	}
	return s.activeCopyResult(ctx, userID, op.ResultFileID)
}

func (s *Service) activeCopyResult(ctx context.Context, userID int64, resultID pgtype.UUID) (*sqlcgen.File, error) {
	file, err := s.queries.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{UserID: userID, FileID: resultID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if file.Status != sqlcgen.FileStatusActive {
		return nil, ErrNotFound
	}
	if file.ParentID.Valid {
		active, err := s.queries.IsLifecycleDestinationActive(ctx, sqlcgen.IsLifecycleDestinationActiveParams{UserID: userID, ParentID: file.ParentID})
		if err != nil {
			return nil, err
		}
		if !active {
			return nil, ErrNotFound
		}
	}
	return file, nil
}

func (s *Service) renewCopy(ctx context.Context, owner int64, operationID, token uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lifecycle.LockOwnerTx(ctx, tx, owner); err != nil {
		return err
	}
	q := s.queries.WithTx(tx)
	op, err := q.LockCopyOperation(ctx, sqlcgen.LockCopyOperationParams{UserID: owner, OperationID: dbtypes.UUID(operationID)})
	if err != nil {
		return err
	}
	if op.TerminalAt.Valid || op.PublishedAt.Valid || op.LeaseToken != dbtypes.UUID(token) || !op.LeaseExpiresAt.Time.After(time.Now()) {
		return lifecycle.ErrCopyDestinationUnavailable
	}
	expires := time.Now().UTC().Add(copyLeaseTTL)
	if !expires.After(op.LeaseExpiresAt.Time) {
		return tx.Commit(ctx)
	}
	if _, err = q.RenewCopyOperation(ctx, sqlcgen.RenewCopyOperationParams{UserID: owner, OperationID: op.ID, LeaseToken: dbtypes.UUID(token), LeaseExpiresAt: dbtypes.Time(expires)}); errors.Is(err, pgx.ErrNoRows) {
		return lifecycle.ErrCopyDestinationUnavailable
	} else if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) abandonCopy(ctx context.Context, owner int64, operationID uuid.UUID, code string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lifecycle.LockOwnerTx(ctx, tx, owner); err != nil {
		return err
	}
	q := s.queries.WithTx(tx)
	rows, err := q.ListCopyOperationReceipts(ctx, sqlcgen.ListCopyOperationReceiptsParams{UserID: owner, OperationIds: pgUUIDs([]uuid.UUID{operationID})})
	if err != nil {
		return err
	}
	op, err := q.LockCopyOperation(ctx, sqlcgen.LockCopyOperationParams{UserID: owner, OperationID: dbtypes.UUID(operationID)})
	if err != nil {
		return err
	}
	if op.PublishedAt.Valid {
		return nil
	}
	if _, err = q.TerminalizeCopyOperation(ctx, sqlcgen.TerminalizeCopyOperationParams{UserID: owner, OperationID: op.ID, FailureCode: code}); err != nil {
		return err
	}
	ids := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		ids[i] = uuid.UUID(r.ID.Bytes)
	}
	if err = s.lifecycle.StageCleanupTx(ctx, tx, lifecycle.StageCleanupInput{UserID: owner, ReceiptIDs: ids, PrimaryCode: code}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return s.lifecycle.RecoverWithinBudget(ctx, owner, ids)
}

func (s *Service) publishCopy(ctx context.Context, in CopyInput, operationID, token, rootID uuid.UUID, rootName string, nodes []treeNode, idMap map[uuid.UUID]uuid.UUID, copied map[uuid.UUID][]copiedPart) (*sqlcgen.File, []uuid.UUID, error, bool) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err, false
	}
	defer tx.Rollback(ctx)
	// Copying a folder inserts one event-emitting row per copied entry in this
	// transaction. The lane keeps the gate wait and absolute budget but lifts only
	// the row cap. SET LOCAL ends with the transaction, so the pool never inherits it.
	if _, err = tx.Exec(ctx, "SET LOCAL teldrive.event_bulk = 'on'"); err != nil {
		return nil, nil, fmt.Errorf("select bulk event lane: %w", err), false
	}
	if err = lifecycle.LockOwnerTx(ctx, tx, in.UserID); err != nil {
		return nil, nil, err, false
	}
	q := s.queries.WithTx(tx)
	entries, err := q.ListActiveDestinationEntries(ctx, sqlcgen.ListActiveDestinationEntriesParams{UserID: in.UserID, ParentID: dbtypes.OptionalUUID(in.ParentID)})
	if err != nil {
		return nil, nil, err, false
	}
	var conflictID *uuid.UUID
	for _, entry := range entries {
		if entry.Name == rootName {
			id := uuid.UUID(entry.ID.Bytes)
			conflictID = &id
			break
		}
	}
	var replaced []uuid.UUID
	if conflictID != nil && in.ConflictPolicy == sqlcgen.NameConflictPolicyReplace {
		ids, err := q.ListLifecycleSubtreeIDs(ctx, sqlcgen.ListLifecycleSubtreeIDsParams{UserID: in.UserID, RootIds: pgUUIDs([]uuid.UUID{*conflictID})})
		if err != nil {
			return nil, nil, err, false
		}
		for _, id := range ids {
			replaced = append(replaced, uuid.UUID(id.Bytes))
		}
	}
	// Include this result and destination so all origins precede any physical/catalog locks.
	affected := append(slices.Clone(replaced), rootID)
	if in.ParentID != nil {
		affected = append(affected, *in.ParentID)
	}
	if err = lifecycle.LockHierarchyTx(ctx, tx, in.UserID, affected, nil); err != nil {
		return nil, nil, err, false
	}
	op, err := q.LockCopyOperation(ctx, sqlcgen.LockCopyOperationParams{UserID: in.UserID, OperationID: dbtypes.UUID(operationID)})
	if err != nil {
		return nil, nil, err, false
	}
	if op.TerminalAt.Valid || op.PublishedAt.Valid || op.LeaseToken != dbtypes.UUID(token) || !op.LeaseExpiresAt.Time.After(time.Now()) {
		return nil, nil, lifecycle.ErrCopyDestinationUnavailable, false
	}
	if in.ParentID != nil {
		active, err := q.IsLifecycleDestinationActive(ctx, sqlcgen.IsLifecycleDestinationActiveParams{UserID: in.UserID, ParentID: dbtypes.UUID(*in.ParentID)})
		if err != nil {
			return nil, nil, err, false
		}
		if !active {
			return nil, nil, lifecycle.ErrCopyDestinationUnavailable, false
		}
	}
	originalIDs := make([]uuid.UUID, len(nodes))
	for i, n := range nodes {
		originalIDs[i] = uuid.UUID(n.File.ID.Bytes)
	}
	current, err := q.ListActiveFilesForLifecycle(ctx, sqlcgen.ListActiveFilesForLifecycleParams{UserID: in.UserID, FileIds: pgUUIDs(originalIDs)})
	if err != nil {
		return nil, nil, err, false
	}
	if len(current) != len(nodes) {
		return nil, nil, ErrNotFound, false
	}
	for _, node := range nodes {
		index, found := slices.BinarySearchFunc(current, node.File.ID, func(file *sqlcgen.File, id pgtype.UUID) int { return bytes.Compare(file.ID.Bytes[:], id.Bytes[:]) })
		if !found || current[index].Generation != node.File.Generation || current[index].ParentID != node.File.ParentID {
			return nil, nil, catalog.ErrConflict, false
		}
	}
	if len(replaced) > 0 {
		if err = s.lifecycle.TerminalizeDestinationsTx(ctx, tx, in.UserID, replaced); err != nil {
			return nil, nil, err, false
		}
	}
	receiptIDs := make([]uuid.UUID, 0)
	for _, parts := range copied {
		for _, part := range parts {
			receiptIDs = append(receiptIDs, part.ReceiptID)
		}
	}
	// Each copy receipt already carries its immutable per-file result identity.
	if len(receiptIDs) > 0 {
		if err = s.lifecycle.BindTx(ctx, tx, lifecycle.BindInput{UserID: in.UserID, ReceiptIDs: receiptIDs}); err != nil {
			return nil, nil, err, false
		}
	}
	catalogIDs := append(slices.Clone(originalIDs), affected...)
	if err = q.LockCatalogRowsForLifecycle(ctx, sqlcgen.LockCatalogRowsForLifecycleParams{UserID: in.UserID, FileIds: pgUUIDs(catalogIDs)}); err != nil {
		return nil, nil, err, false
	}
	if err = q.AcquireAdvisoryTransactionLock(ctx, copyDestinationLockID(in.UserID, in.ParentID)); err != nil {
		return nil, nil, err, false
	}
	if conflictID != nil {
		switch in.ConflictPolicy {
		case sqlcgen.NameConflictPolicyFail:
			return nil, nil, catalog.ErrConflict, false
		case sqlcgen.NameConflictPolicyRename:
			rootName, err = nextAvailableCopyName(ctx, q, in.UserID, in.ParentID, rootName)
			if err != nil {
				return nil, nil, err, false
			}
		case sqlcgen.NameConflictPolicyReplace:
			if err = q.MarkFileSubtreeDeletionPending(ctx, sqlcgen.MarkFileSubtreeDeletionPendingParams{UserID: in.UserID, FileID: dbtypes.UUID(*conflictID)}); err != nil {
				return nil, nil, err, false
			}
			if err = q.RevokeSharesForFileSubtree(ctx, sqlcgen.RevokeSharesForFileSubtreeParams{UserID: in.UserID, FileID: dbtypes.UUID(*conflictID)}); err != nil {
				return nil, nil, err, false
			}
		}
	}
	files := make([]copiedFileRecord, 0, len(nodes))
	parts := make([]copiedFilePartRecord, 0)
	for _, node := range nodes {
		oldID := uuid.UUID(node.File.ID.Bytes)
		newID := idMap[oldID]
		var parent *uuid.UUID
		name := node.File.Name
		if oldID == in.FileID {
			parent = in.ParentID
			name = rootName
		} else if node.File.ParentID.Valid {
			mapped := idMap[uuid.UUID(node.File.ParentID.Bytes)]
			parent = &mapped
		}
		files = append(files, copiedFileRecord{ID: newID, UserID: in.UserID, ParentID: parent, Name: name, Kind: string(node.File.Kind), MIMEType: optionalText(node.File.MimeType), Size: optionalInt64(node.File.Size), HashAlgorithm: optionalText(node.File.HashAlgorithm), HashValue: optionalText(node.File.HashValue), Encryption: node.File.Encryption, EncryptionKeyVersion: optionalInt32(node.File.EncryptionKeyVersion), ModTime: node.File.ModTime.Time})
		for _, part := range copied[oldID] {
			parts = append(parts, copiedFilePartRecord{FileID: newID, PartNo: part.Part.PartNo, ChannelID: part.Stored.ChannelID, MessageID: part.Stored.MessageID, PlainSize: optionalInt64(part.Part.PlainSize), StoredSize: optionalInt64(part.Part.StoredSize), Checksum: optionalText(part.Part.Checksum), Salt: optionalText(part.Part.Salt), BlockHashes: part.Part.BlockHashes, ReceiptID: part.ReceiptID})
		}
	}
	encoded, err := json.Marshal(files)
	if err != nil {
		return nil, nil, err, false
	}
	inserted, err := q.InsertCopiedFiles(ctx, encoded)
	if err != nil {
		return nil, nil, err, false
	}
	if len(inserted) != len(files) {
		return nil, nil, ErrNotFound, false
	}
	if len(parts) > 0 {
		encoded, err = json.Marshal(parts)
		if err != nil {
			return nil, nil, err, false
		}
		count, err := q.InsertCopiedFileParts(ctx, encoded)
		if err != nil {
			return nil, nil, err, false
		}
		if count != int64(len(parts)) {
			return nil, nil, lifecycle.ErrDeletionFenced, false
		}
	}
	count, err := q.PublishCopyOperation(ctx, sqlcgen.PublishCopyOperationParams{UserID: in.UserID, OperationID: op.ID, LeaseToken: dbtypes.UUID(token)})
	if err != nil {
		return nil, nil, err, false
	}
	if count != 1 {
		return nil, nil, lifecycle.ErrCopyDestinationUnavailable, false
	}
	if err = s.commit(ctx, tx); err != nil {
		return nil, replaced, fmt.Errorf("commit file copy: %w", err), !errors.Is(err, pgx.ErrTxCommitRollback)
	}
	for _, file := range inserted {
		if file.ID == dbtypes.UUID(rootID) {
			return file, replaced, nil, false
		}
	}
	return nil, nil, ErrNotFound, true
}

func optionalText(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func optionalInt64(value pgtype.Int8) *int64 {
	if !value.Valid {
		return nil
	}
	return &value.Int64
}

func optionalInt32(value pgtype.Int4) *int32 {
	if !value.Valid {
		return nil
	}
	return &value.Int32
}

func nextAvailableCopyName(ctx context.Context, queries *sqlcgen.Queries, userID int64, parentID *uuid.UUID, original string) (string, error) {
	names, err := queries.ListActiveNames(ctx, sqlcgen.ListActiveNamesParams{
		UserID: userID, ParentID: dbtypes.OptionalUUID(parentID),
	})
	if err != nil {
		return "", fmt.Errorf("list copy destination names: %w", err)
	}
	used := make(map[string]struct{}, len(names))
	for _, name := range names {
		used[name] = struct{}{}
	}
	base, extension := splitCopyName(original)
	for sequence := 1; sequence <= 10000; sequence++ {
		candidate := base + fmt.Sprintf(" (%d)", sequence) + extension
		if _, exists := used[candidate]; !exists {
			return candidate, nil
		}
	}
	return "", catalog.ErrConflict
}

func splitCopyName(name string) (string, string) {
	index := strings.LastIndex(name, ".")
	if index <= 0 || index == len(name)-1 {
		return name, ""
	}
	return name[:index], name[index:]
}

func copyDestinationLockID(userID int64, parentID *uuid.UUID) int64 {
	input := []byte("teldrive/catalog-destination/")
	var user [8]byte
	binary.BigEndian.PutUint64(user[:], uint64(userID))
	input = append(input, user[:]...)
	if parentID != nil {
		input = append(input, parentID[:]...)
	} else {
		input = append(input, make([]byte, 16)...)
	}
	digest := sha256.Sum256(input)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}
func (s *Service) CleanTrash(ctx context.Context, userID int64) (int64, error) {
	if userID <= 0 {
		return 0, ErrInvalidInput
	}
	if s.lifecycle == nil {
		return 0, ErrLifecycleUnavailable
	}
	ids, _, count, err := s.markPurge(ctx, userID, nil, true, false)
	if err == nil {
		s.catalog.InvalidateFiles(ctx, userID, ids...)
	}
	return count, err
}

func (s *Service) QueuePurge(ctx context.Context, userID int64, fileID uuid.UUID) error {
	if userID <= 0 || fileID == uuid.Nil {
		return ErrInvalidInput
	}
	if s.lifecycle == nil {
		return ErrLifecycleUnavailable
	}
	ids, _, _, err := s.markPurge(ctx, userID, []uuid.UUID{fileID}, false, true)
	if err == nil {
		s.catalog.InvalidateFiles(ctx, userID, ids...)
	}
	return err
}

func (s *Service) Purge(ctx context.Context, userID int64, fileID uuid.UUID) error {
	return s.PurgeMany(ctx, userID, []uuid.UUID{fileID})
}

func (s *Service) PurgeMany(ctx context.Context, userID int64, rootIDs []uuid.UUID) error {
	if userID <= 0 || len(rootIDs) == 0 {
		return ErrInvalidInput
	}
	for _, id := range rootIDs {
		if id == uuid.Nil {
			return ErrInvalidInput
		}
	}
	if s.lifecycle == nil {
		return ErrLifecycleUnavailable
	}
	ids, receipts, _, markErr := s.markPurge(ctx, userID, rootIDs, false, false)
	if len(ids) == 0 {
		return markErr
	}
	s.catalog.InvalidateFiles(ctx, userID, ids...)
	budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	recoveryErr := s.lifecycle.RecoverWithinBudget(budget, userID, receipts)
	// Eligible siblings finalize even when another target is unknown, protected or failed.
	finalErr := s.finalizePurge(budget, userID, ids)
	return errors.Join(markErr, recoveryErr, finalErr)
}

func (s *Service) markPurge(ctx context.Context, userID int64, roots []uuid.UUID, all, strict bool) ([]uuid.UUID, []uuid.UUID, int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	defer tx.Rollback(ctx)
	// A subtree update emits one event per affected row. This transaction-local
	// lane keeps the gate wait and absolute budget but lifts only the row cap.
	// SET LOCAL ends with the transaction, so the pool never inherits it.
	if _, err = tx.Exec(ctx, "SET LOCAL teldrive.event_bulk = 'on'"); err != nil {
		return nil, nil, 0, fmt.Errorf("select bulk event lane: %w", err)
	}
	if err = lifecycle.LockOwnerTx(ctx, tx, userID); err != nil {
		return nil, nil, 0, err
	}
	q := s.queries.WithTx(tx)
	if all {
		rows, err := q.ListTrashedFileIDs(ctx, userID)
		if err != nil {
			return nil, nil, 0, err
		}
		roots = make([]uuid.UUID, len(rows))
		for i, id := range rows {
			roots[i] = uuid.UUID(id.Bytes)
		}
	}
	eligible := make([]uuid.UUID, 0, len(roots))
	var retained []error
	candidates, err := q.ListFilePurgeRoots(ctx, sqlcgen.ListFilePurgeRootsParams{UserID: userID, FileIds: pgUUIDs(roots)})
	if err != nil {
		return nil, nil, 0, err
	}
	if strict && len(candidates) == 0 {
		return nil, nil, 0, ErrNotFound
	}
	for _, file := range candidates {
		if file.Status != sqlcgen.FileStatusTrashed && file.Status != sqlcgen.FileStatusDeletionPending {
			if strict {
				return nil, nil, 0, ErrNotTrashed
			}
			retained = append(retained, ErrNotTrashed)
			continue
		}
		eligible = append(eligible, uuid.UUID(file.ID.Bytes))
	}
	if len(eligible) == 0 {
		return nil, nil, 0, errors.Join(retained...)
	}
	rows, err := q.ListLifecycleSubtreeIDs(ctx, sqlcgen.ListLifecycleSubtreeIDsParams{UserID: userID, RootIds: pgUUIDs(eligible)})
	if err != nil {
		return nil, nil, 0, err
	}
	ids := make([]uuid.UUID, len(rows))
	for i, id := range rows {
		ids[i] = uuid.UUID(id.Bytes)
	}
	if err = lifecycle.LockHierarchyTx(ctx, tx, userID, ids, nil); err != nil {
		return nil, nil, 0, err
	}
	if err = s.lifecycle.TerminalizeDestinationsTx(ctx, tx, userID, ids); err != nil {
		return nil, nil, 0, err
	}
	if err = q.LockCatalogRowsForLifecycle(ctx, sqlcgen.LockCatalogRowsForLifecycleParams{UserID: userID, FileIds: rows}); err != nil {
		return nil, nil, 0, err
	}
	var count int64
	if all {
		count, err = q.MarkAllTrashedDeletionPending(ctx, userID)
		if err == nil {
			err = q.MarkFileIDsDeletionPending(ctx, sqlcgen.MarkFileIDsDeletionPendingParams{UserID: userID, FileIds: rows})
		}
	} else {
		err = q.MarkFileIDsDeletionPending(ctx, sqlcgen.MarkFileIDsDeletionPendingParams{UserID: userID, FileIds: rows})
		count = int64(len(ids))
	}
	if err != nil {
		return nil, nil, 0, err
	}
	receiptRows, err := q.ListPurgeReceiptIDs(ctx, sqlcgen.ListPurgeReceiptIDsParams{UserID: userID, FileIds: rows})
	if err != nil {
		return nil, nil, 0, err
	}
	receipts := make([]uuid.UUID, len(receiptRows))
	for i, id := range receiptRows {
		receipts[i] = uuid.UUID(id.Bytes)
	}
	if len(receipts) > 0 {
		if err = s.lifecycle.StageCleanupTx(ctx, tx, lifecycle.StageCleanupInput{UserID: userID, ReceiptIDs: receipts, PrimaryCode: "file_purge"}); err != nil {
			return nil, nil, 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, nil, 0, fmt.Errorf("commit purge mark: %w", err)
	}
	return ids, receipts, count, errors.Join(retained...)
}

func (s *Service) finalizePurge(ctx context.Context, userID int64, roots []uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Deleting a subtree emits one event per removed row. This transaction-local
	// lane keeps the gate wait and absolute budget but lifts only the row cap.
	// SET LOCAL ends with the transaction, so the pool never inherits it.
	if _, err = tx.Exec(ctx, "SET LOCAL teldrive.event_bulk = 'on'"); err != nil {
		return fmt.Errorf("select bulk event lane: %w", err)
	}
	if err = lifecycle.LockOwnerTx(ctx, tx, userID); err != nil {
		return err
	}
	q := s.queries.WithTx(tx)
	rows, err := q.LoadFileSubtrees(ctx, sqlcgen.LoadFileSubtreesParams{UserID: userID, RootIds: pgUUIDs(roots)})
	if err != nil {
		return err
	}
	byID := make(map[uuid.UUID]int32, len(rows))
	for _, r := range rows {
		id := uuid.UUID(r.ID.Bytes)
		if depth, ok := byID[id]; !ok || r.Depth > depth {
			byID[id] = r.Depth
		}
	}
	ids := make([]uuid.UUID, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	if err = lifecycle.LockHierarchyTx(ctx, tx, userID, ids, nil); err != nil {
		return err
	}
	if err = s.lifecycle.TerminalizeDestinationsTx(ctx, tx, userID, ids); err != nil {
		return err
	}
	if err = q.LockCatalogRowsForLifecycle(ctx, sqlcgen.LockCatalogRowsForLifecycleParams{UserID: userID, FileIds: pgUUIDs(ids)}); err != nil {
		return err
	}
	if err = q.DeleteAcknowledgedFileParts(ctx, sqlcgen.DeleteAcknowledgedFilePartsParams{UserID: userID, FileIds: pgUUIDs(ids)}); err != nil {
		return err
	}
	if err = q.ClearUploadSessionParentsByFileIDs(ctx, sqlcgen.ClearUploadSessionParentsByFileIDsParams{UserID: userID, FileIds: pgUUIDs(ids)}); err != nil {
		return err
	}
	byDepth := make(map[int32][]uuid.UUID)
	depths := make([]int32, 0)
	for id, depth := range byID {
		if _, ok := byDepth[depth]; !ok {
			depths = append(depths, depth)
		}
		byDepth[depth] = append(byDepth[depth], id)
	}
	slices.SortFunc(depths, func(a, b int32) int { return cmp.Compare(b, a) })
	var deleted int64
	for _, depth := range depths {
		count, err := q.DeleteFileCatalogRowsByIDs(ctx, sqlcgen.DeleteFileCatalogRowsByIDsParams{UserID: userID, FileIds: pgUUIDs(byDepth[depth])})
		if err != nil {
			return err
		}
		deleted += count
	}
	var incomplete error
	if deleted != int64(len(ids)) {
		report, err := q.GetPurgeRetentionSummary(ctx, sqlcgen.GetPurgeRetentionSummaryParams{UserID: userID, FileIds: pgUUIDs(ids)})
		if err != nil {
			return err
		}
		incomplete = fmt.Errorf("%w: retained_catalog_rows=%d legacy_ownership_unknown=%d protected=%d pending=%d exhausted=%d", ErrPurgeIncomplete, int64(len(ids))-deleted, report.LegacyUnknown, report.Protected, report.Pending, report.Exhausted)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit purge finalization: %w", err)
	}
	s.catalog.InvalidateFiles(ctx, userID, ids...)
	return incomplete
}

func (s *Service) loadTree(ctx context.Context, userID int64, rootID uuid.UUID) ([]treeNode, error) {
	rows, err := s.queries.LoadFileSubtree(ctx, sqlcgen.LoadFileSubtreeParams{
		RootID: dbtypes.UUID(rootID), UserID: userID,
	})
	if err != nil {
		return nil, fmt.Errorf("load file subtree: %w", err)
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	out := make([]treeNode, 0, len(rows))
	for _, row := range rows {
		out = append(out, treeNode{File: sqlcgen.File{
			ID: row.ID, UserID: row.UserID, ParentID: row.ParentID, Name: row.Name,
			Kind: row.Kind, MimeType: row.MimeType,
			Size: row.Size, HashAlgorithm: row.HashAlgorithm, HashValue: row.HashValue,
			Encryption: row.Encryption, EncryptionKeyVersion: row.EncryptionKeyVersion,
			Status: row.Status, ModTime: row.ModTime, Generation: row.Generation,
			CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, DeletedAt: row.DeletedAt,
		}, Depth: row.Depth})
	}
	return out, nil
}

func pgUUIDs(ids []uuid.UUID) []pgtype.UUID {
	result := make([]pgtype.UUID, len(ids))
	for index, id := range ids {
		result[index] = dbtypes.UUID(id)
	}
	return result
}
