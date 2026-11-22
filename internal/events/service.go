package events

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
)

var (
	ErrInvalidCursor = errors.New("invalid event stream cursor")
	ErrInvalidTicket = errors.New("invalid or expired event stream ticket")
	ErrUnknownOwner  = errors.New("event owner has no incarnation")
)

type Config struct {
	BatchSize             int32
	MaxConnectionsPerUser int
	Heartbeat             time.Duration
	WriteTimeout          time.Duration
	TicketTTL             time.Duration
	CleanupInterval       time.Duration
	ConnectTimeout        time.Duration
	PingInterval          time.Duration
	ReconnectMin          time.Duration
	ReconnectMax          time.Duration
}

type Event struct {
	ID           int64
	UserID       int64
	Type         string
	ResourceType string
	ResourceID   string
	Generation   *int64
	Payload      []byte
	OccurredAt   time.Time
}

type Ticket struct {
	Value     string
	ExpiresAt time.Time
}

// TicketIdentity is the credential proof for an ordered stream. ExpiresAt is the
// persisted ticket expiry, which bounds the stream's authority.
type TicketIdentity struct {
	UserID    int64
	ExpiresAt time.Time
	// Digest is the SHA-256 digest of the presented ticket. It identifies the
	// exact credential without retaining the secret.
	Digest string
	// OwnerIncarnation is read in the same snapshot as the ticket row.
	OwnerIncarnation uuid.UUID
}

// OrderedState is the committed strict cursor state of one owner incarnation.
type OrderedState struct {
	Epoch            uuid.UUID
	OwnerIncarnation uuid.UUID
	High             int64
	Floor            int64
}

// OrderedSnapshot is one bounded read-only Repeatable Read view of the strict
// stream state and its replay rows.
type OrderedSnapshot struct {
	tx      pgx.Tx
	queries *sqlcgen.Queries
}

// OrderedEvent is a committed event in strict position order. Location is the
// nullable JSON projection captured from the emitting trigger.
type OrderedEvent struct {
	ID           int64
	Position     int64
	Type         string
	ResourceType string
	ResourceID   string
	Generation   *int64
	Payload      []byte
	Location     []byte
	OccurredAt   time.Time
}

type Service struct {
	pool     *pgxpool.Pool
	queries  *sqlcgen.Queries
	hub      *Hub
	listener *Listener
	logger   *slog.Logger
	config   Config

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	running bool
	closed  bool
}

func NewService(pool *pgxpool.Pool, logger *slog.Logger, cfg Config) (*Service, error) {
	if pool == nil {
		return nil, errors.New("event service requires a database pool")
	}
	if logger == nil {
		logger = slog.Default()
	}
	cfg = withDefaults(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	hub := NewHub(cfg.MaxConnectionsPerUser)
	done := make(chan struct{})
	close(done)
	return &Service{
		pool:    pool,
		queries: sqlcgen.New(pool),
		hub:     hub,
		listener: newListener(pool, hub, logger, listenerConfig{
			ConnectTimeout: cfg.ConnectTimeout,
			PingInterval:   cfg.PingInterval,
			ReconnectMin:   cfg.ReconnectMin,
			ReconnectMax:   cfg.ReconnectMax,
		}),
		logger: logger,
		config: cfg,
		done:   done,
	}, nil
}

func withDefaults(cfg Config) Config {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxConnectionsPerUser == 0 {
		cfg.MaxConnectionsPerUser = 5
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = 20 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.TicketTTL == 0 {
		cfg.TicketTTL = 2 * time.Minute
	}
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = time.Hour
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 5 * time.Second
	}
	if cfg.ReconnectMin == 0 {
		cfg.ReconnectMin = 100 * time.Millisecond
	}
	if cfg.ReconnectMax == 0 {
		cfg.ReconnectMax = 30 * time.Second
	}
	return cfg
}

func validateConfig(cfg Config) error {
	switch {
	case cfg.BatchSize < 1 || cfg.BatchSize > 1000:
		return errors.New("event batch size must be between 1 and 1000")
	case cfg.MaxConnectionsPerUser < 1 || cfg.MaxConnectionsPerUser > 1000:
		return errors.New("event connections per user must be between 1 and 1000")
	case cfg.Heartbeat <= 0:
		return errors.New("event heartbeat must be positive")
	case cfg.WriteTimeout <= 0:
		return errors.New("event write timeout must be positive")
	case cfg.TicketTTL <= 0:
		return errors.New("event ticket TTL must be positive")
	case cfg.CleanupInterval <= 0:
		return errors.New("event cleanup interval must be positive")
	case cfg.ConnectTimeout <= 0:
		return errors.New("event listener connect timeout must be positive")
	case cfg.PingInterval <= 0:
		return errors.New("event listener ping interval must be positive")
	case cfg.ReconnectMin <= 0 || cfg.ReconnectMax < cfg.ReconnectMin:
		return errors.New("event listener reconnect bounds are invalid")
	default:
		return nil
	}
}

func (s *Service) Start(ctx context.Context) error {
	if s == nil || s.listener == nil {
		return errors.New("event service is not configured")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrServiceClosed
	}
	if s.running {
		return nil
	}

	serviceCtx, cancel := context.WithCancel(ctx)
	if err := s.listener.Start(serviceCtx); err != nil {
		cancel()
		return err
	}
	s.cancel = cancel
	s.running = true
	s.done = make(chan struct{})
	go s.runTicketCleanup(serviceCtx, s.done)
	return nil
}

func (s *Service) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if !s.running {
		s.mu.Unlock()
		s.hub.Close()
		return nil
	}
	cancel, done := s.cancel, s.done
	s.mu.Unlock()

	cancel()
	s.hub.Close()
	listenerErr := s.listener.Close(ctx)
	select {
	case <-done:
		return listenerErr
	case <-ctx.Done():
		return errors.Join(listenerErr, ctx.Err())
	}
}

func (s *Service) Done() <-chan struct{} {
	if s == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

func (s *Service) Subscribe(userID int64) (<-chan struct{}, func(), error) {
	if s == nil || s.hub == nil {
		return nil, nil, ErrServiceClosed
	}
	return s.hub.Subscribe(userID)
}

func (s *Service) BatchSize() int32 {
	if s == nil {
		return 100
	}
	return s.config.BatchSize
}

func (s *Service) Heartbeat() time.Duration {
	if s == nil {
		return 20 * time.Second
	}
	return s.config.Heartbeat
}

func (s *Service) WriteTimeout() time.Duration {
	if s == nil {
		return 10 * time.Second
	}
	return s.config.WriteTimeout
}

func (s *Service) ListAfter(ctx context.Context, userID, afterID int64, eventTypes []string) ([]Event, error) {
	if s == nil || s.queries == nil || userID <= 0 || afterID < 0 {
		return nil, errors.New("invalid event list request")
	}
	rows, err := s.queries.ListUserEventsAfter(ctx, sqlcgen.ListUserEventsAfterParams{
		UserID: userID, AfterID: afterID, EventTypes: eventTypes, EventLimit: s.config.BatchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("list user events: %w", err)
	}
	events := make([]Event, 0, len(rows))
	for _, row := range rows {
		event, err := eventFromRow(row.ID, row.UserID, row.EventType, row.ResourceType, row.ResourceID, row.Generation, row.Payload, row.OccurredAt)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// CurrentCursor returns the newest event ID known for the user.
func (s *Service) CurrentCursor(ctx context.Context, userID int64) (int64, error) {
	if s == nil || s.queries == nil || userID <= 0 {
		return 0, errors.New("invalid event cursor request")
	}
	state, err := s.queries.GetUserEventCursorState(ctx, sqlcgen.GetUserEventCursorStateParams{
		CursorUserID: userID,
		AfterID:      0,
	})
	if err != nil {
		return 0, fmt.Errorf("get current event cursor: %w", err)
	}
	return state.LastEventID, nil
}

func (s *Service) CursorExpired(ctx context.Context, userID, afterID int64) (bool, error) {
	if afterID <= 0 {
		return false, nil
	}
	state, err := s.queries.GetUserEventCursorState(ctx, sqlcgen.GetUserEventCursorStateParams{
		CursorUserID: userID,
		AfterID:      afterID,
	})
	if err != nil {
		return false, fmt.Errorf("get event cursor state: %w", err)
	}
	if afterID > state.LastEventID {
		return false, ErrInvalidCursor
	}
	return !state.CursorExists && state.LastEventID > afterID, nil
}

func (s *Service) IssueTicket(ctx context.Context, userID int64) (Ticket, error) {
	if s == nil || s.queries == nil || userID <= 0 {
		return Ticket{}, errors.New("invalid event ticket request")
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return Ticket{}, ErrServiceClosed
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return Ticket{}, fmt.Errorf("generate event stream ticket: %w", err)
	}
	value := base64.RawURLEncoding.EncodeToString(raw)
	expiresAt := time.Now().UTC().Add(s.config.TicketTTL)
	hash := sha256.Sum256([]byte(value))
	if err := s.queries.CreateEventStreamTicket(ctx, sqlcgen.CreateEventStreamTicketParams{
		TokenHash: hash[:],
		UserID:    userID,
		ExpiresAt: pgtype.Timestamptz{Time: expiresAt, Valid: true},
	}); err != nil {
		return Ticket{}, fmt.Errorf("store event stream ticket: %w", err)
	}
	return Ticket{Value: value, ExpiresAt: expiresAt}, nil
}

func (s *Service) AuthenticateTicket(ctx context.Context, value string) (int64, error) {
	if s == nil || s.queries == nil || strings.TrimSpace(value) == "" {
		return 0, ErrInvalidTicket
	}
	hash := sha256.Sum256([]byte(value))
	userID, err := s.queries.GetEventStreamTicketUser(ctx, hash[:])
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrInvalidTicket
	}
	if err != nil {
		return 0, fmt.Errorf("authenticate event stream ticket: %w", err)
	}
	if userID <= 0 {
		return 0, ErrInvalidTicket
	}
	return userID, nil
}

// AuthenticateOrderedTicket resolves an ordered-stream ticket together with its
// persisted expiry. The expiry is returned for valid tickets so the caller can
// bound stream authority; expired or unknown tickets stay invalid.
func (s *Service) AuthenticateOrderedTicket(ctx context.Context, value string) (TicketIdentity, error) {
	if s == nil || s.queries == nil || strings.TrimSpace(value) == "" {
		return TicketIdentity{}, ErrInvalidTicket
	}
	hash := sha256.Sum256([]byte(value))
	row, err := s.queries.GetOrderedEventTicket(ctx, hash[:])
	if errors.Is(err, pgx.ErrNoRows) {
		return TicketIdentity{}, ErrInvalidTicket
	}
	if err != nil {
		return TicketIdentity{}, fmt.Errorf("authenticate ordered event stream ticket: %w", err)
	}
	if row == nil || row.UserID <= 0 || !row.Active || !row.ExpiresAt.Valid {
		return TicketIdentity{}, ErrInvalidTicket
	}
	return TicketIdentity{UserID: row.UserID, ExpiresAt: row.ExpiresAt.Time.UTC()}, nil
}

// AuthenticateOrderedTicketSnapshot validates the ticket and reads its owner
// incarnation in one bounded read-only Repeatable Read snapshot.
func (s *Service) AuthenticateOrderedTicketSnapshot(ctx context.Context, value string) (TicketIdentity, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(value) == "" {
		return TicketIdentity{}, ErrInvalidTicket
	}
	hash := sha256.Sum256([]byte(value))
	txCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(txCtx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return TicketIdentity{}, fmt.Errorf("begin ordered ticket snapshot: %w", err)
	}
	defer rollbackSnapshot(tx)
	queries := sqlcgen.New(tx)
	row, err := queries.GetOrderedEventTicket(txCtx, hash[:])
	if errors.Is(err, pgx.ErrNoRows) {
		return TicketIdentity{}, ErrInvalidTicket
	}
	if err != nil {
		return TicketIdentity{}, fmt.Errorf("authenticate ordered event stream ticket: %w", err)
	}
	if row == nil || row.UserID <= 0 || !row.Active || !row.ExpiresAt.Valid {
		return TicketIdentity{}, ErrInvalidTicket
	}
	incarnation, err := queries.GetEventOwnerIncarnation(txCtx, row.UserID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !incarnation.Valid) {
		return TicketIdentity{}, ErrUnknownOwner
	}
	if err != nil {
		return TicketIdentity{}, fmt.Errorf("get ordered ticket incarnation: %w", err)
	}
	return TicketIdentity{
		UserID: row.UserID, ExpiresAt: row.ExpiresAt.Time.UTC(),
		Digest: hex.EncodeToString(hash[:]), OwnerIncarnation: incarnation.Bytes,
	}, nil
}

// CheckOrderedTicket reports whether the exact admitted ticket is still active.
func (s *Service) CheckOrderedTicket(ctx context.Context, digest string) error {
	if s == nil || s.queries == nil || digest == "" {
		return ErrInvalidTicket
	}
	hash, err := hex.DecodeString(digest)
	if err != nil || len(hash) != sha256.Size {
		return ErrInvalidTicket
	}
	row, err := s.queries.GetOrderedEventTicket(ctx, hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidTicket
	}
	if err != nil {
		return fmt.Errorf("check ordered ticket: %w", err)
	}
	if row == nil || !row.Active || !row.ExpiresAt.Valid {
		return ErrInvalidTicket
	}
	return nil
}

// OpenOrderedSnapshot begins one bounded caller-timed Repeatable Read read-only
// transaction. Close it before writing HTTP.
func (s *Service) OpenOrderedSnapshot(ctx context.Context) (*OrderedSnapshot, error) {
	if s == nil || s.pool == nil {
		return nil, ErrServiceClosed
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin ordered event snapshot: %w", err)
	}
	return &OrderedSnapshot{tx: tx, queries: sqlcgen.New(tx)}, nil
}

// Close releases the snapshot without holding a pool lease.
func (snapshot *OrderedSnapshot) Close() {
	if snapshot == nil {
		return
	}
	rollbackSnapshot(snapshot.tx)
	snapshot.tx = nil
}

// State returns committed epoch, owner incarnation, high and floor.
func (snapshot *OrderedSnapshot) State(ctx context.Context, userID int64) (OrderedState, error) {
	if snapshot == nil || snapshot.queries == nil || userID <= 0 {
		return OrderedState{}, errors.New("invalid ordered event state request")
	}
	row, err := snapshot.queries.GetOrderedEventState(ctx, userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrderedState{}, ErrUnknownOwner
	}
	if err != nil {
		return OrderedState{}, fmt.Errorf("get ordered event state: %w", err)
	}
	return orderedStateFromRow(row)
}

// List returns committed events in strict position order in (after, state.High].
func (snapshot *OrderedSnapshot) List(ctx context.Context, state OrderedState, after int64, eventTypes []string, limit int32) ([]OrderedEvent, error) {
	if snapshot == nil || snapshot.queries == nil || after < 0 || limit <= 0 || state.OwnerIncarnation == uuid.Nil || state.High < 0 || state.Floor < 0 {
		return nil, errors.New("invalid ordered event list request")
	}
	rows, err := snapshot.queries.ListOrderedUserEvents(ctx, sqlcgen.ListOrderedUserEventsParams{
		OwnerIncarnation: pgtype.UUID{Bytes: state.OwnerIncarnation, Valid: true},
		AfterPosition:    pgtype.Int8{Int64: after, Valid: true},
		High:             pgtype.Int8{Int64: state.High, Valid: true},
		EventTypes:       eventTypes,
		EventLimit:       limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list ordered user events: %w", err)
	}
	return orderedEventsFromRows(rows)
}

func orderedStateFromRow(row *sqlcgen.GetOrderedEventStateRow) (OrderedState, error) {
	if row == nil || !row.StreamEpoch.Valid || !row.OwnerIncarnation.Valid {
		return OrderedState{}, ErrUnknownOwner
	}
	return OrderedState{
		Epoch:            uuid.UUID(row.StreamEpoch.Bytes),
		OwnerIncarnation: uuid.UUID(row.OwnerIncarnation.Bytes),
		High:             row.High,
		Floor:            row.Floor,
	}, nil
}

func orderedEventsFromRows(rows []*sqlcgen.ListOrderedUserEventsRow) ([]OrderedEvent, error) {
	events := make([]OrderedEvent, 0, len(rows))
	for _, row := range rows {
		if row == nil || row.ID <= 0 || !row.StrictPosition.Valid || row.StrictPosition.Int64 <= 0 || !row.OccurredAt.Valid {
			return nil, errors.New("invalid stored ordered event")
		}
		event := OrderedEvent{
			ID:           row.ID,
			Position:     row.StrictPosition.Int64,
			Type:         row.EventType,
			ResourceType: row.ResourceType,
			Payload:      append([]byte(nil), row.Payload...),
			Location:     append([]byte(nil), row.Location...),
			OccurredAt:   row.OccurredAt.Time.UTC(),
		}
		if row.ResourceID.Valid {
			event.ResourceID = row.ResourceID.String
		}
		if row.Generation.Valid {
			generation := row.Generation.Int64
			event.Generation = &generation
		}
		events = append(events, event)
	}
	return events, nil
}

func rollbackSnapshot(tx pgx.Tx) {
	if tx == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// OwnerIncarnation returns the immutable incarnation for a live numeric user.
func (s *Service) OwnerIncarnation(ctx context.Context, userID int64) (uuid.UUID, error) {
	if s == nil || s.queries == nil || userID <= 0 {
		return uuid.UUID{}, errors.New("invalid event owner request")
	}
	incarnation, err := s.queries.GetEventOwnerIncarnation(ctx, userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.UUID{}, ErrUnknownOwner
	}
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("get event owner incarnation: %w", err)
	}
	if !incarnation.Valid {
		return uuid.UUID{}, ErrUnknownOwner
	}
	return uuid.UUID(incarnation.Bytes), nil
}

// OrderedState returns the committed epoch, owner incarnation, high and floor
// for a live user.
func (s *Service) OrderedState(ctx context.Context, userID int64) (OrderedState, error) {
	if s == nil || s.queries == nil || userID <= 0 {
		return OrderedState{}, errors.New("invalid ordered event state request")
	}
	row, err := s.queries.GetOrderedEventState(ctx, userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrderedState{}, ErrUnknownOwner
	}
	if err != nil {
		return OrderedState{}, fmt.Errorf("get ordered event state: %w", err)
	}
	return orderedStateFromRow(row)
}

// ListOrdered returns committed events in strict position order in
// (after, state.High], bounded by the configured batch size.
func (s *Service) ListOrdered(ctx context.Context, state OrderedState, after int64, eventTypes []string) ([]OrderedEvent, error) {
	if s == nil || s.queries == nil || after < 0 || state.OwnerIncarnation == uuid.Nil || state.High < 0 || state.Floor < 0 {
		return nil, errors.New("invalid ordered event list request")
	}
	rows, err := s.queries.ListOrderedUserEvents(ctx, sqlcgen.ListOrderedUserEventsParams{
		OwnerIncarnation: pgtype.UUID{Bytes: state.OwnerIncarnation, Valid: true},
		AfterPosition:    pgtype.Int8{Int64: after, Valid: true},
		High:             pgtype.Int8{Int64: state.High, Valid: true},
		EventTypes:       eventTypes,
		EventLimit:       s.config.BatchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("list ordered user events: %w", err)
	}
	return orderedEventsFromRows(rows)
}

func (s *Service) runTicketCleanup(ctx context.Context, done chan struct{}) {
	defer func() {
		s.mu.Lock()
		s.running = false
		s.cancel = nil
		close(done)
		s.mu.Unlock()
	}()

	ticker := time.NewTicker(s.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanupCtx, cancel := context.WithTimeout(ctx, s.config.ConnectTimeout)
			if _, err := s.queries.DeleteExpiredEventStreamTickets(cleanupCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.ErrorContext(ctx, "delete expired event stream tickets", "error", err)
			}
			cancel()
		}
	}
}

func eventFromRow(
	id int64,
	userID int64,
	eventType string,
	resourceType string,
	resourceID pgtype.Text,
	generation pgtype.Int8,
	payload []byte,
	occurredAt pgtype.Timestamptz,
) (Event, error) {
	if id <= 0 || userID <= 0 || !occurredAt.Valid {
		return Event{}, errors.New("invalid stored user event")
	}
	event := Event{
		ID:           id,
		UserID:       userID,
		Type:         eventType,
		ResourceType: resourceType,
		Payload:      append([]byte(nil), payload...),
		OccurredAt:   occurredAt.Time.UTC(),
	}
	if resourceID.Valid {
		event.ResourceID = resourceID.String
	}
	if generation.Valid {
		value := generation.Int64
		event.Generation = &value
	}
	return event, nil
}
