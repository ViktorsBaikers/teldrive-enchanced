package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
)

// PhysicalMessage is a physical identity, never an assertion of ownership.
type PhysicalMessage struct {
	ChannelID int64
	MessageID int64
}

func lockKey(domain byte, first, second int64) int64 {
	var data [18]byte
	data[0], data[1] = 0xb2, domain
	binary.BigEndian.PutUint64(data[2:10], uint64(first))
	binary.BigEndian.PutUint64(data[10:18], uint64(second))
	digest := sha256.Sum256(data[:])
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

// OwnerLockKey is shared by every hierarchy publisher and deleter.
func OwnerLockKey(userID int64) int64 { return lockKey(1, userID, 0) }

// LockOwnerTx precedes admission/import/operation/session, channel and message locks.
func LockOwnerTx(ctx context.Context, tx pgx.Tx, userID int64) error {
	if userID <= 0 || tx == nil {
		return ErrInvalidInput
	}
	if err := sqlcgen.New(tx).AcquireAdvisoryTransactionLock(ctx, OwnerLockKey(userID)); err != nil {
		return fmt.Errorf("lock lifecycle owner: %w", err)
	}
	return nil
}

// LockChannelWritesTx locks signed physical channels, not owner registration rows.
// Caller owns the transaction and must acquire all owner/origin locks first.
func LockChannelWritesTx(ctx context.Context, tx pgx.Tx, channelIDs []int64) error {
	if tx == nil {
		return ErrInvalidInput
	}
	ids := slices.Clone(channelIDs)
	for _, id := range ids {
		if id == 0 {
			return ErrInvalidInput
		}
	}
	slices.Sort(ids)
	q := sqlcgen.New(tx)
	for _, id := range slices.Compact(ids) {
		if err := q.AcquireAdvisoryTransactionLock(ctx, lockKey(2, id, 0)); err != nil {
			return fmt.Errorf("lock lifecycle channel: %w", err)
		}
	}
	return nil
}

// LockMessagesTx takes stable channel/message locks before any receipt/catalog row.
func LockMessagesTx(ctx context.Context, tx pgx.Tx, messages []PhysicalMessage) error {
	if tx == nil {
		return ErrInvalidInput
	}
	pairs := slices.Clone(messages)
	for _, p := range pairs {
		if p.ChannelID == 0 || p.MessageID <= 0 {
			return ErrInvalidInput
		}
	}
	slices.SortFunc(pairs, func(a, b PhysicalMessage) int {
		if a.ChannelID < b.ChannelID {
			return -1
		}
		if a.ChannelID > b.ChannelID {
			return 1
		}
		if a.MessageID < b.MessageID {
			return -1
		}
		if a.MessageID > b.MessageID {
			return 1
		}
		return 0
	})
	q := sqlcgen.New(tx)
	for _, p := range slices.Compact(pairs) {
		if err := q.AcquireAdvisoryTransactionLock(ctx, lockKey(3, p.ChannelID, p.MessageID)); err != nil {
			return fmt.Errorf("lock lifecycle message: %w", err)
		}
	}
	return nil
}

// RequireBindableMessagesTx also protects legacy binds that have no receipt.
// It grants no ownership; new writes additionally require BindTx's matching row.
func RequireBindableMessagesTx(ctx context.Context, tx pgx.Tx, messages []PhysicalMessage) error {
	channels := make([]int64, 0, len(messages))
	for _, p := range messages {
		channels = append(channels, p.ChannelID)
	}
	if err := RequireWritableChannelsTx(ctx, tx, channels); err != nil {
		return err
	}
	if err := LockMessagesTx(ctx, tx, messages); err != nil {
		return err
	}
	q := sqlcgen.New(tx)
	for _, p := range messages {
		fenced, err := q.HasPhysicalDeletionFence(ctx, sqlcgen.HasPhysicalDeletionFenceParams{ChannelID: p.ChannelID, MessageID: p.MessageID})
		if err != nil {
			return fmt.Errorf("read lifecycle message fence: %w", err)
		}
		if fenced {
			return ErrDeletionFenced
		}
	}
	return nil
}

// LockHierarchyTx locks the complete origin set before any physical or catalog row.
// Caller holds the owner fence before discovering replacement/subtree identities.
func LockHierarchyTx(ctx context.Context, tx pgx.Tx, userID int64, subtreeIDs, uploadIDs []uuid.UUID) error {
	if err := LockOwnerTx(ctx, tx, userID); err != nil {
		return err
	}
	q := sqlcgen.New(tx)
	subtree := hierarchyIDs(subtreeIDs)
	uploads, err := q.ListHierarchyUploadIDs(ctx, sqlcgen.ListHierarchyUploadIDsParams{UserID: userID, SubtreeIds: subtree, UploadIds: hierarchyIDs(uploadIDs)})
	if err != nil {
		return err
	}
	if err = q.LockHierarchyAdmissions(ctx, sqlcgen.LockHierarchyAdmissionsParams{UserID: userID, UploadIds: uploads, SubtreeIds: subtree}); err != nil {
		return err
	}
	batches, err := q.ListHierarchyImportBatches(ctx, sqlcgen.ListHierarchyImportBatchesParams{UserID: userID, SubtreeIds: subtree, UploadIds: uploads})
	if err != nil {
		return err
	}
	anchored, err := q.ListImportBatchesAnchoredInSubtree(ctx, sqlcgen.ListImportBatchesAnchoredInSubtreeParams{UserID: userID, SubtreeIds: subtree})
	if err != nil {
		return err
	}
	for _, batch := range anchored {
		batches = append(batches, batch.BatchID)
	}
	if err = q.LockImportBatchesForLifecycle(ctx, sqlcgen.LockImportBatchesForLifecycleParams{UserID: userID, BatchIds: batches}); err != nil {
		return err
	}
	if err = q.LockImportDestinationPinsForLifecycle(ctx, sqlcgen.LockImportDestinationPinsForLifecycleParams{UserID: userID, BatchIds: batches}); err != nil {
		return err
	}
	if err = q.LockImportAttemptsForLifecycle(ctx, sqlcgen.LockImportAttemptsForLifecycleParams{UserID: userID, BatchIds: batches}); err != nil {
		return err
	}
	if err = q.LockHierarchyCopyOperations(ctx, sqlcgen.LockHierarchyCopyOperationsParams{UserID: userID, SubtreeIds: subtree}); err != nil {
		return err
	}
	if err = q.LockHierarchyUploadSessions(ctx, sqlcgen.LockHierarchyUploadSessionsParams{UserID: userID, UploadIds: uploads}); err != nil {
		return err
	}
	rows, err := q.ListHierarchyReceiptHints(ctx, sqlcgen.ListHierarchyReceiptHintsParams{UserID: userID, SubtreeIds: subtree, UploadIds: uploads})
	if err != nil {
		return err
	}
	channels := make([]int64, 0, len(rows))
	messages := make([]PhysicalMessage, 0, len(rows))
	receiptIDs := make([]pgtype.UUID, 0, len(rows))
	for _, r := range rows {
		receiptIDs = append(receiptIDs, r.ID)
		if r.ChannelID.Valid {
			channels = append(channels, r.ChannelID.Int64)
			if r.MessageID.Valid {
				messages = append(messages, PhysicalMessage{r.ChannelID.Int64, r.MessageID.Int64})
			}
		}
	}
	parts, err := q.ListUploadPartsByUploadIDs(ctx, uploads)
	if err != nil {
		return err
	}
	for _, p := range parts {
		channels = append(channels, p.ChannelID)
		if p.MessageID.Valid {
			messages = append(messages, PhysicalMessage{p.ChannelID, p.MessageID.Int64})
		}
	}
	if err = LockChannelWritesTx(ctx, tx, channels); err != nil {
		return err
	}
	if err = LockMessagesTx(ctx, tx, messages); err != nil {
		return err
	}
	if len(receiptIDs) > 0 {
		if _, err = q.LockRemoteWriteReceipts(ctx, sqlcgen.LockRemoteWriteReceiptsParams{UserID: userID, ReceiptIds: receiptIDs}); err != nil {
			return err
		}
	}
	return nil
}

// TerminalizeDestinationsTx joins deletion, terminal evidence and durable cleanup.
// LockHierarchyTx must precede catalog locks; repeated calls only reacquire held locks.
func (s *Service) TerminalizeDestinationsTx(ctx context.Context, tx pgx.Tx, userID int64, subtreeIDs []uuid.UUID) error {
	if err := LockHierarchyTx(ctx, tx, userID, subtreeIDs, nil); err != nil {
		return err
	}
	q := sqlcgen.New(tx)
	ids := hierarchyIDs(subtreeIDs)
	if _, err := q.TerminalizeUndispatchedImportBatches(ctx, sqlcgen.TerminalizeUndispatchedImportBatchesParams{UserID: userID, SubtreeIds: ids}); err != nil {
		return err
	}
	if _, err := q.TerminalizeDispatchedImportBatches(ctx, sqlcgen.TerminalizeDispatchedImportBatchesParams{UserID: userID, SubtreeIds: ids}); err != nil {
		return err
	}
	if _, err := q.TerminalizeImportDestinations(ctx, sqlcgen.TerminalizeImportDestinationsParams{UserID: userID, SubtreeIds: ids}); err != nil {
		return err
	}
	if _, err := q.TerminalizeUploadDestinations(ctx, sqlcgen.TerminalizeUploadDestinationsParams{UserID: userID, SubtreeIds: ids}); err != nil {
		return err
	}
	if _, _, err := s.TerminalizeCopyDestinationsTx(ctx, tx, userID, subtreeIDs); err != nil {
		return err
	}
	uploads, err := q.ListHierarchyUploadIDs(ctx, sqlcgen.ListHierarchyUploadIDsParams{UserID: userID, SubtreeIds: ids, UploadIds: []pgtype.UUID{}})
	if err != nil {
		return err
	}
	rows, err := q.ListHierarchyReceiptHints(ctx, sqlcgen.ListHierarchyReceiptHintsParams{UserID: userID, SubtreeIds: []pgtype.UUID{}, UploadIds: uploads})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	receipts := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		receipts[i] = uuid.UUID(r.ID.Bytes)
	}
	return s.StageCleanupTx(ctx, tx, StageCleanupInput{UserID: userID, ReceiptIDs: receipts, PrimaryCode: "upload_parent_unavailable"})
}

func hierarchyIDs(ids []uuid.UUID) []pgtype.UUID {
	out := make([]pgtype.UUID, len(ids))
	for i, id := range ids {
		out[i] = dbtypes.UUID(id)
	}
	return out
}
