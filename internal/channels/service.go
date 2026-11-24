package channels

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/lifecycle"
)

var (
	ErrInvalidOwner     = errors.New("invalid channel owner")
	ErrInvalidChannel   = errors.New("channel does not belong to user")
	ErrNoSelected       = errors.New("no selected channel")
	ErrChannelFull      = errors.New("channel capacity reached")
	ErrAutoCreateOff    = errors.New("automatic channel creation is disabled")
	ErrChannelUnhealthy = errors.New("channel is unavailable")
)

// RemoteChannel is the minimum Telegram metadata required for durable storage.
type RemoteChannel struct {
	ID   int64  `json:"channel_id"`
	Name string `json:"name"`
}

// Creator performs Telegram-side channel lifecycle operations. Implementations
// are expected to add the configured upload bots before returning from Create.
// Create may return a known channel alongside a provisioning error. Callers must
// retain that identity and must never automatically delete the remote channel.
type Creator interface {
	Create(ctx context.Context, userID int64, name string) (RemoteChannel, error)
	Delete(ctx context.Context, userID, channelID int64) error
}

type Config struct {
	PartLimit  int64
	AutoCreate bool
	NamePrefix string
}

type Service struct {
	pool    *pgxpool.Pool
	queries *sqlcgen.Queries
	creator Creator
	config  Config
	now     func() time.Time
}

func NewService(pool *pgxpool.Pool, creator Creator, cfg Config) *Service {
	if cfg.NamePrefix == "" {
		cfg.NamePrefix = "storage"
	}
	return &Service{
		pool:    pool,
		queries: sqlcgen.New(pool),
		creator: creator,
		config:  cfg,
		now:     time.Now,
	}
}

// Resolve returns an owned channel suitable for one more uploaded part.
// Explicit channel requests never roll over silently.
func (s *Service) Resolve(ctx context.Context, userID, requestedChannelID int64) (int64, error) {
	if userID <= 0 {
		return 0, ErrInvalidOwner
	}
	if requestedChannelID != 0 {
		channel, err := s.queries.GetChannelForUser(ctx, sqlcgen.GetChannelForUserParams{
			UserID:    userID,
			ChannelID: requestedChannelID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrInvalidChannel
		}
		if err != nil {
			return 0, fmt.Errorf("get requested channel: %w", err)
		}
		if channel.Health == sqlcgen.ChannelHealthUnavailable {
			return 0, ErrChannelUnhealthy
		}
		full, err := s.limitReached(ctx, requestedChannelID)
		if err != nil {
			return 0, err
		}
		if full {
			return 0, ErrChannelFull
		}
		if err := s.requireWritableHint(ctx, nil, userID, requestedChannelID); err != nil {
			return 0, err
		}
		return requestedChannelID, nil
	}

	selected, err := s.queries.GetSelectedChannel(ctx, userID)
	if err == nil {
		if selected.Health != sqlcgen.ChannelHealthUnavailable {
			full, countErr := s.limitReached(ctx, selected.ChannelID)
			if countErr != nil {
				return 0, countErr
			}
			if !full {
				if err := s.requireWritableHint(ctx, nil, userID, selected.ChannelID); err != nil {
					return 0, err
				}
				return selected.ChannelID, nil
			}
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("get selected channel: %w", err)
	}

	if !s.config.AutoCreate {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNoSelected
		}
		return 0, ErrAutoCreateOff
	}
	return s.rollover(ctx, userID)
}

// ResolveMany plans channel capacity under the rollover lock, in part order.
// Returned IDs are hints; each remote write still requires a committed receipt reservation.
func (s *Service) ResolveMany(ctx context.Context, userID int64, count int) (channelIDs []int64, err error) {
	if userID <= 0 {
		return nil, ErrInvalidOwner
	}
	if count <= 0 {
		return []int64{}, nil
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire channel allocation connection: %w", err)
	}
	defer conn.Release()
	lockID := advisoryLockID(userID)
	queries := sqlcgen.New(conn)
	if err := queries.AcquireAdvisoryLock(ctx, lockID); err != nil {
		return nil, fmt.Errorf("acquire channel allocation lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, unlockErr := queries.ReleaseAdvisoryLock(unlockCtx, lockID); unlockErr != nil && err == nil {
			err = fmt.Errorf("release channel allocation lock: %w", unlockErr)
		}
	}()

	selected, selectedErr := queries.GetSelectedChannel(ctx, userID)
	if selectedErr != nil && !errors.Is(selectedErr, pgx.ErrNoRows) {
		return nil, fmt.Errorf("get selected channel for allocation: %w", selectedErr)
	}
	for len(channelIDs) < count {
		var channelID int64
		used := int64(0)
		if selectedErr == nil && selected.Health != sqlcgen.ChannelHealthUnavailable {
			channelID = selected.ChannelID
			if s.config.PartLimit > 0 {
				used, err = queries.CountChannelStoredMessages(ctx, channelID)
				if err != nil {
					return nil, fmt.Errorf("count channel parts: %w", err)
				}
			}
		}
		capacity := count - len(channelIDs)
		if channelID != 0 && s.config.PartLimit > 0 {
			capacity = int(max(s.config.PartLimit-used, 0))
			capacity = min(capacity, count-len(channelIDs))
		}
		if channelID != 0 && capacity > 0 {
			if err := s.requireWritableHint(ctx, conn, userID, channelID); err != nil {
				return nil, err
			}
			for range capacity {
				channelIDs = append(channelIDs, channelID)
			}
			if len(channelIDs) < count {
				selected.Health = sqlcgen.ChannelHealthUnavailable
			}
			continue
		}
		if !s.config.AutoCreate {
			if errors.Is(selectedErr, pgx.ErrNoRows) {
				return nil, ErrNoSelected
			}
			return nil, ErrAutoCreateOff
		}
		selected, err = s.createSelectedChannel(ctx, conn, queries, userID)
		if err != nil {
			return nil, err
		}
		selectedErr = nil
	}
	return channelIDs, nil
}

func (s *Service) createSelectedChannel(ctx context.Context, conn *pgxpool.Conn, queries *sqlcgen.Queries, userID int64) (*sqlcgen.Channel, error) {
	if s.creator == nil {
		return nil, errors.New("Telegram channel creator is not configured")
	}
	name := fmt.Sprintf("%s_%s", strings.TrimSpace(s.config.NamePrefix), s.now().UTC().Format("20060102_150405"))
	remote, err := s.creator.Create(ctx, userID, name)
	if err != nil {
		return nil, s.retainAllocation(conn, userID, remote.ID, nil, fmt.Errorf("create Telegram channel: %w", err))
	}
	if remote.ID == 0 {
		return nil, errors.New("Telegram returned an empty channel id")
	}
	if strings.TrimSpace(remote.Name) == "" {
		remote.Name = name
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, s.retainAllocation(conn, userID, remote.ID, nil, fmt.Errorf("begin channel allocation: %w", err))
	}
	defer tx.Rollback(ctx)
	if err := lifecycle.LockOwnerTx(ctx, tx, userID); err != nil {
		return nil, s.retainAllocation(conn, userID, remote.ID, tx, err)
	}
	if err := lifecycle.RequireWritableChannelsTx(ctx, tx, []int64{remote.ID}); err != nil {
		return nil, s.retainAllocation(conn, userID, remote.ID, tx, err)
	}
	q := queries.WithTx(tx)
	if err := q.ClearSelectedChannel(ctx, userID); err != nil {
		return nil, s.retainAllocation(conn, userID, remote.ID, tx, fmt.Errorf("clear selected channel: %w", err))
	}
	if _, err := q.CreateChannel(ctx, sqlcgen.CreateChannelParams{ChannelID: remote.ID, UserID: userID, Name: remote.Name}); err != nil {
		return nil, s.retainAllocation(conn, userID, remote.ID, tx, fmt.Errorf("create channel record: %w", err))
	}
	selected, err := q.SelectChannel(ctx, sqlcgen.SelectChannelParams{UserID: userID, ChannelID: remote.ID})
	if err != nil {
		return nil, s.retainAllocation(conn, userID, remote.ID, tx, fmt.Errorf("select channel record: %w", err))
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, s.retainAllocation(conn, userID, remote.ID, nil, fmt.Errorf("commit channel allocation: %w", err))
	}
	return selected, nil
}

func (s *Service) limitReached(ctx context.Context, channelID int64) (bool, error) {
	return s.limitReachedWith(ctx, s.queries, channelID)
}

func (s *Service) limitReachedWith(ctx context.Context, queries *sqlcgen.Queries, channelID int64) (bool, error) {
	if s.config.PartLimit <= 0 {
		return false, nil
	}
	count, err := queries.CountChannelStoredMessages(ctx, channelID)
	if err != nil {
		return false, fmt.Errorf("count channel parts: %w", err)
	}
	return count >= s.config.PartLimit, nil
}

func (s *Service) rollover(ctx context.Context, userID int64) (channelID int64, err error) {
	if s.creator == nil {
		return 0, errors.New("Telegram channel creator is not configured")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire rollover connection: %w", err)
	}
	defer conn.Release()

	lockID := advisoryLockID(userID)
	lockedQueries := sqlcgen.New(conn)
	if err := lockedQueries.AcquireAdvisoryLock(ctx, lockID); err != nil {
		return 0, fmt.Errorf("acquire channel rollover lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, unlockErr := lockedQueries.ReleaseAdvisoryLock(unlockCtx, lockID); unlockErr != nil && err == nil {
			err = fmt.Errorf("release channel rollover lock: %w", unlockErr)
		}
	}()

	// Another process may have completed rollover while this caller waited.
	selected, selectedErr := lockedQueries.GetSelectedChannel(ctx, userID)
	if selectedErr == nil && selected.Health != sqlcgen.ChannelHealthUnavailable {
		full, countErr := s.limitReachedWith(ctx, lockedQueries, selected.ChannelID)
		if countErr != nil {
			return 0, countErr
		}
		if !full {
			if err := s.requireWritableHint(ctx, conn, userID, selected.ChannelID); err != nil {
				return 0, err
			}
			return selected.ChannelID, nil
		}
	} else if selectedErr != nil && !errors.Is(selectedErr, pgx.ErrNoRows) {
		return 0, fmt.Errorf("recheck selected channel: %w", selectedErr)
	}
	selected, err = s.createSelectedChannel(ctx, conn, lockedQueries, userID)
	if err != nil {
		return 0, err
	}
	return selected.ChannelID, nil
}

// AllocationFailure reports retention, never remote cleanup authority.
type AllocationFailure struct {
	UserID      int64
	ChannelID   int64
	Disposition string
	State       string
	Cause       error
	Secondary   error
}

func (e *AllocationFailure) Error() string {
	return fmt.Sprintf("channel allocation failed: owner=%d channel=%d disposition=%s state=%s", e.UserID, e.ChannelID, e.Disposition, e.State)
}

func (e *AllocationFailure) Unwrap() []error {
	if e.Secondary == nil {
		return []error{e.Cause}
	}
	return []error{e.Cause, e.Secondary}
}

func (s *Service) retainAllocation(conn *pgxpool.Conn, userID, channelID int64, tx pgx.Tx, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report := &AllocationFailure{UserID: userID, ChannelID: channelID,
		Disposition: "cleanup_not_attempted_retained", State: "unknown_retained", Cause: cause}
	defer func() {
		secondaryCode := "none"
		if report.Secondary != nil {
			secondaryCode = "allocation_reconciliation_failed"
		}
		slog.WarnContext(ctx, "channel allocation retained", "user_id", userID, "channel_id", channelID,
			"disposition", report.Disposition, "state", report.State,
			"primary_code", "channel_allocation_failed", "secondary_code", secondaryCode)
	}()
	rolledBack := errors.Is(cause, pgx.ErrTxCommitRollback)
	if tx != nil {
		if err := tx.Rollback(ctx); err != nil {
			report.Secondary = errors.Join(report.Secondary, fmt.Errorf("rollback allocation: %w", err))
		} else {
			rolledBack = true
		}
	}
	// Try a separate connection briefly so pool-level failures stay visible,
	// but never block the advisory-lock path on an exhausted pool.
	var q *sqlcgen.Queries
	if conn == nil {
		acquired, err := s.pool.Acquire(ctx)
		if err != nil {
			report.Secondary = errors.Join(report.Secondary, fmt.Errorf("read retained allocation: %w", err))
			return report
		}
		defer acquired.Release()
		q = sqlcgen.New(acquired)
	} else {
		acquireCtx, cancel := context.WithTimeout(ctx, time.Second)
		acquired, err := s.pool.Acquire(acquireCtx)
		cancel()
		if err != nil {
			report.Secondary = errors.Join(report.Secondary, fmt.Errorf("read retained allocation: %w", err))
			q = sqlcgen.New(conn)
		} else {
			defer acquired.Release()
			q = sqlcgen.New(acquired)
		}
	}
	var rowErr error
	if channelID != 0 {
		_, rowErr = q.GetChannelForUser(ctx, sqlcgen.GetChannelForUserParams{UserID: userID, ChannelID: channelID})
	}
	_, selectedErr := q.GetSelectedChannel(ctx, userID)
	if selectedErr != nil && !errors.Is(selectedErr, pgx.ErrNoRows) {
		report.Secondary = errors.Join(report.Secondary, fmt.Errorf("read retained selection: %w", selectedErr))
	}
	selectionKnown := selectedErr == nil || errors.Is(selectedErr, pgx.ErrNoRows)
	if channelID != 0 && rowErr == nil && selectionKnown {
		report.State = "committed_row_retained"
	} else if errors.Is(rowErr, pgx.ErrNoRows) && rolledBack && selectionKnown {
		report.State = "known_rollback_unpersisted_retained"
	} else if rowErr != nil && !errors.Is(rowErr, pgx.ErrNoRows) {
		report.Secondary = errors.Join(report.Secondary, fmt.Errorf("read retained channel: %w", rowErr))
	}
	return report
}

func (s *Service) requireWritableHint(ctx context.Context, conn *pgxpool.Conn, userID, channelID int64) error {
	var (
		tx  pgx.Tx
		err error
	)
	if conn != nil {
		tx, err = conn.Begin(ctx)
	} else {
		tx, err = s.pool.Begin(ctx)
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lifecycle.LockOwnerTx(ctx, tx, userID); err != nil {
		return err
	}
	if err = lifecycle.RequireWritableChannelsTx(ctx, tx, []int64{channelID}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// FilterDiscoverable is read-only; discovery never grants later write authority.
func (s *Service) FilterDiscoverable(ctx context.Context, userID int64, remote []RemoteChannel) ([]RemoteChannel, error) {
	if s == nil || s.pool == nil || userID <= 0 {
		return nil, ErrInvalidOwner
	}
	result := make([]RemoteChannel, 0, len(remote))
	for _, channel := range remote {
		if channel.ID == 0 {
			return nil, ErrInvalidChannel
		}
		_, err := s.queries.GetChannelDeletionIntent(ctx, channel.ID)
		if errors.Is(err, pgx.ErrNoRows) {
			result = append(result, channel)
		} else if err != nil {
			return nil, fmt.Errorf("filter channel intent: %w", err)
		}
	}
	return result, nil
}

func advisoryLockID(userID int64) int64 {
	var input [8]byte
	binary.BigEndian.PutUint64(input[:], uint64(userID))
	digest := sha256.Sum256(append([]byte("teldrive/channel-rollover/"), input[:]...))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}
