package channels

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
)

var (
	ErrSelectedChannel = errors.New("selected channel cannot be deleted")
	ErrChannelInUse    = errors.New("channel still contains referenced objects")
)

type ListInput struct {
	UserID         int64
	AfterCreatedAt *time.Time
	AfterChannelID *int64
	Limit          int32
}

func (s *Service) List(ctx context.Context, in ListInput) ([]*sqlcgen.Channel, error) {
	if in.UserID <= 0 {
		return nil, ErrInvalidOwner
	}
	if in.Limit <= 0 {
		in.Limit = 100
	}
	if in.Limit > 200 {
		in.Limit = 200
	}
	var afterID pgtype.Int8
	if in.AfterChannelID != nil {
		afterID = dbtypes.Int8(*in.AfterChannelID)
	}
	rows, err := s.queries.ListChannels(ctx, sqlcgen.ListChannelsParams{
		UserID: in.UserID, AfterCreatedAt: dbtypes.OptionalTime(in.AfterCreatedAt),
		AfterChannelID: afterID, PageSize: in.Limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	return rows, nil
}

func (s *Service) Create(ctx context.Context, userID int64, name string, selected bool) (*sqlcgen.Channel, error) {
	if userID <= 0 || s.creator == nil {
		return nil, ErrInvalidOwner
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = fmt.Sprintf("%s_%s", strings.TrimSpace(s.config.NamePrefix), s.now().UTC().Format("20060102_150405"))
	}
	remote, err := s.creator.Create(ctx, userID, name)
	if err != nil {
		return nil, s.retainAllocation(userID, remote.ID, nil, fmt.Errorf("create Telegram channel: %w", err))
	}
	if remote.ID == 0 {
		return nil, errors.New("Telegram returned an empty channel id")
	}
	if strings.TrimSpace(remote.Name) == "" {
		remote.Name = name
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, s.retainAllocation(userID, remote.ID, nil, err)
	}
	defer tx.Rollback(ctx)
	if err := lifecycle.LockOwnerTx(ctx, tx, userID); err != nil {
		return nil, s.retainAllocation(userID, remote.ID, tx, err)
	}
	if err := lifecycle.RequireWritableChannelsTx(ctx, tx, []int64{remote.ID}); err != nil {
		return nil, s.retainAllocation(userID, remote.ID, tx, err)
	}
	q := s.queries.WithTx(tx)
	if selected {
		if err := q.ClearSelectedChannel(ctx, userID); err != nil {
			return nil, s.retainAllocation(userID, remote.ID, tx, fmt.Errorf("clear selected channel: %w", err))
		}
	}
	row, err := q.CreateChannel(ctx, sqlcgen.CreateChannelParams{ChannelID: remote.ID, UserID: userID, Name: remote.Name})
	if err != nil {
		return nil, s.retainAllocation(userID, remote.ID, tx, fmt.Errorf("create channel record: %w", err))
	}
	if selected {
		row, err = q.SelectChannel(ctx, sqlcgen.SelectChannelParams{UserID: userID, ChannelID: remote.ID})
		if err != nil {
			return nil, s.retainAllocation(userID, remote.ID, tx, fmt.Errorf("select created channel: %w", err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, s.retainAllocation(userID, remote.ID, nil, fmt.Errorf("commit created channel: %w", err))
	}
	return row, nil
}

func (s *Service) Select(ctx context.Context, userID, channelID int64) (*sqlcgen.Channel, error) {
	if userID <= 0 || channelID == 0 {
		return nil, ErrInvalidChannel
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin channel selection: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := lifecycle.LockOwnerTx(ctx, tx, userID); err != nil {
		return nil, err
	}
	if err := lifecycle.RequireWritableChannelsTx(ctx, tx, []int64{channelID}); err != nil {
		return nil, err
	}
	q := s.queries.WithTx(tx)
	channel, err := q.GetChannelForUser(ctx, sqlcgen.GetChannelForUserParams{UserID: userID, ChannelID: channelID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidChannel
	}
	if err != nil {
		return nil, fmt.Errorf("get channel for selection: %w", err)
	}
	if channel.Health == sqlcgen.ChannelHealthUnavailable {
		return nil, ErrChannelUnhealthy
	}
	if err := q.ClearSelectedChannel(ctx, userID); err != nil {
		return nil, fmt.Errorf("clear selected channel: %w", err)
	}
	channel, err = q.SelectChannel(ctx, sqlcgen.SelectChannelParams{UserID: userID, ChannelID: channelID})
	if err != nil {
		return nil, fmt.Errorf("select channel: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit channel selection: %w", err)
	}
	return channel, nil
}

func (s *Service) Delete(ctx context.Context, userID, channelID int64) error {
	if userID <= 0 || channelID == 0 || s.creator == nil {
		return ErrInvalidChannel
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin channel deletion: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := lifecycle.LockOwnerTx(ctx, tx, userID); err != nil {
		return err
	}
	if err := lifecycle.RequireWritableChannelsTx(ctx, tx, []int64{channelID}); err != nil {
		return err
	}
	q := s.queries.WithTx(tx)
	channel, err := q.GetChannelForUser(ctx, sqlcgen.GetChannelForUserParams{UserID: userID, ChannelID: channelID})
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidChannel
	}
	if err != nil {
		return fmt.Errorf("get channel for deletion: %w", err)
	}
	if channel.Selected {
		return ErrSelectedChannel
	}
	protected, err := q.HasChannelDeletionProtection(ctx, channelID)
	if err != nil {
		return fmt.Errorf("check channel deletion protection: %w", err)
	}
	if protected {
		return ErrChannelInUse
	}
	intentID := dbtypes.UUID(uuid.New())
	if _, err := q.InsertChannelDeletionIntent(ctx, sqlcgen.InsertChannelDeletionIntentParams{
		ChannelID: channelID, IntentID: intentID, RequestedBy: userID,
	}); err != nil {
		return fmt.Errorf("close channel for deletion: %w", err)
	}
	// A failed COMMIT never authorizes a remote attempt, even if it committed.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit channel closing intent: %w", err)
	}
	// Remote work starts only after the intent transaction releases its locks.
	remoteErr := s.creator.Delete(ctx, userID, channelID)
	finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if remoteErr != nil {
		return s.recordDeletionFailure(finalCtx, channelID, intentID, "remote_delete_failed", fmt.Errorf("delete Telegram channel: %w", remoteErr))
	}
	if err := s.finalizeChannelDeletion(finalCtx, userID, channelID, intentID); err != nil {
		return s.recordDeletionFailure(finalCtx, channelID, intentID, "finalization_failed", err)
	}
	return nil
}

func (s *Service) finalizeChannelDeletion(finalCtx context.Context, userID, channelID int64, intentID pgtype.UUID) error {
	finalTx, err := s.pool.Begin(finalCtx)
	if err != nil {
		return fmt.Errorf("begin channel deletion finalization: %w", err)
	}
	defer finalTx.Rollback(finalCtx)
	if err := lifecycle.LockOwnerTx(finalCtx, finalTx, userID); err != nil {
		return err
	}
	if err := lifecycle.LockChannelWritesTx(finalCtx, finalTx, []int64{channelID}); err != nil {
		return err
	}
	fq := s.queries.WithTx(finalTx)
	count, err := fq.FinalizeChannelDeletionIntent(finalCtx, sqlcgen.FinalizeChannelDeletionIntentParams{ChannelID: channelID, IntentID: intentID})
	if err != nil {
		return fmt.Errorf("finalize channel deletion intent: %w", err)
	}
	if count != 1 {
		return errors.New("channel deletion intent changed")
	}
	removed, err := fq.DeleteChannelRegistrationsForIntent(finalCtx, sqlcgen.DeleteChannelRegistrationsForIntentParams{ChannelID: channelID, IntentID: intentID})
	if err != nil {
		return fmt.Errorf("remove deleted channel registrations: %w", err)
	}
	if removed < 1 {
		return errors.New("deleted channel registration missing")
	}
	if err := finalTx.Commit(finalCtx); err != nil {
		return fmt.Errorf("commit channel deletion finalization: %w", err)
	}
	return nil
}

func (s *Service) recordDeletionFailure(ctx context.Context, channelID int64, intentID pgtype.UUID, code string, primary error) error {
	count, err := s.queries.RecordChannelDeletionFailure(ctx, sqlcgen.RecordChannelDeletionFailureParams{
		ChannelID: channelID, IntentID: intentID, FailureCode: code,
	})
	if err != nil {
		return errors.Join(primary, fmt.Errorf("record channel deletion failure: %w", err))
	}
	if count != 1 {
		return errors.Join(primary, errors.New("channel deletion failure token changed"))
	}
	return primary
}
