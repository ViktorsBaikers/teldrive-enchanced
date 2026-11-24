//go:build integration

package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/events"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
)

type orderedFixture struct {
	t       *testing.T
	db      *testpostgres.Database
	pool    *pgxpool.Pool
	service *events.Service
	ctx     context.Context
	nextID  *int64
}

func newOrderedFixture(t *testing.T) *orderedFixture {
	t.Helper()
	db := testpostgres.New(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service, err := events.NewService(db.Pool, logger, events.Config{BatchSize: 1000})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	nextID := int64(2000)
	return &orderedFixture{
		t: t, db: db, pool: db.Pool, service: service,
		ctx: context.Background(), nextID: &nextID,
	}
}

// forTest returns a view that reports helper failures on the current subtest.
func (f *orderedFixture) forTest(t *testing.T) *orderedFixture {
	view := *f
	view.t = t
	return &view
}

func (f *orderedFixture) user() int64 {
	f.t.Helper()
	*f.nextID++
	userID := *f.nextID
	if _, err := f.pool.Exec(f.ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
		f.t.Fatalf("seed user %d: %v", userID, err)
	}
	return userID
}

func (f *orderedFixture) transaction() pgx.Tx {
	f.t.Helper()
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		f.t.Fatalf("begin transaction: %v", err)
	}
	return tx
}

func (f *orderedFixture) insertEvent(userID int64, eventType string) int64 {
	f.t.Helper()
	var id int64
	err := f.pool.QueryRow(f.ctx, `
		INSERT INTO user_events (user_id, event_type, resource_type, payload)
		VALUES ($1, $2, 'test', '{}'::jsonb)
		RETURNING id`, userID, eventType).Scan(&id)
	if err != nil {
		f.t.Fatalf("insert event %q: %v", eventType, err)
	}
	return id
}

func (f *orderedFixture) insertEventTx(tx pgx.Tx, userID int64, eventType string) int64 {
	f.t.Helper()
	var id int64
	err := tx.QueryRow(f.ctx, `
		INSERT INTO user_events (user_id, event_type, resource_type, payload)
		VALUES ($1, $2, 'test', '{}'::jsonb)
		RETURNING id`, userID, eventType).Scan(&id)
	if err != nil {
		f.t.Fatalf("insert event %q in transaction: %v", eventType, err)
	}
	return id
}

func (f *orderedFixture) insertEventsTx(tx pgx.Tx, userID int64, count int) {
	f.t.Helper()
	for index := 0; index < count; index++ {
		f.insertEventTx(tx, userID, fmt.Sprintf("test.batch.%d", index))
	}
}

func (f *orderedFixture) insertFolder(userID int64, name string) string {
	f.t.Helper()
	var id string
	err := f.pool.QueryRow(f.ctx, `
		INSERT INTO files (user_id, name, kind, mod_time)
		VALUES ($1, $2, 'folder', now())
		RETURNING id::text`, userID, name).Scan(&id)
	if err != nil {
		f.t.Fatalf("insert folder %q: %v", name, err)
	}
	return id
}

func (f *orderedFixture) state(userID int64) events.OrderedState {
	f.t.Helper()
	state, err := f.service.OrderedState(f.ctx, userID)
	if err != nil {
		f.t.Fatalf("OrderedState(%d) error = %v", userID, err)
	}
	return state
}

func (f *orderedFixture) ordered(userID, after int64, eventTypes []string) []events.OrderedEvent {
	f.t.Helper()
	list, err := f.service.ListOrdered(f.ctx, f.state(userID), after, eventTypes)
	if err != nil {
		f.t.Fatalf("ListOrdered(%d) error = %v", userID, err)
	}
	return list
}

func (f *orderedFixture) position(eventID int64) int64 {
	f.t.Helper()
	var position pgtype.Int8
	var incarnation pgtype.UUID
	if err := f.pool.QueryRow(f.ctx,
		"SELECT strict_position, owner_incarnation FROM user_events WHERE id = $1",
		eventID).Scan(&position, &incarnation); err != nil {
		f.t.Fatalf("read event %d position: %v", eventID, err)
	}
	if !position.Valid || position.Int64 <= 0 || !incarnation.Valid {
		f.t.Fatalf("event %d has no strict position (%v) or incarnation (%v)", eventID, position.Valid, incarnation.Valid)
	}
	return position.Int64
}

func (f *orderedFixture) positions(userID int64) []int64 {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, `
		SELECT strict_position FROM user_events
		WHERE user_id = $1 ORDER BY strict_position`, userID)
	if err != nil {
		f.t.Fatalf("list event positions: %v", err)
	}
	defer rows.Close()
	var result []int64
	for rows.Next() {
		var position int64
		if err := rows.Scan(&position); err != nil {
			f.t.Fatalf("scan event position: %v", err)
		}
		result = append(result, position)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatalf("iterate event positions: %v", err)
	}
	return result
}

func (f *orderedFixture) allocatorPosition() int64 {
	f.t.Helper()
	var position int64
	if err := f.pool.QueryRow(f.ctx,
		"SELECT last_position FROM event_stream_allocator WHERE id = 1").Scan(&position); err != nil {
		f.t.Fatalf("read allocator position: %v", err)
	}
	return position
}

func (f *orderedFixture) legacyLastEventID(userID int64) int64 {
	f.t.Helper()
	var lastID int64
	if err := f.pool.QueryRow(f.ctx,
		"SELECT last_event_id FROM user_event_stream_state WHERE user_id = $1",
		userID).Scan(&lastID); err != nil {
		f.t.Fatalf("read legacy stream state: %v", err)
	}
	return lastID
}

func (f *orderedFixture) eventCount(userID int64) int64 {
	f.t.Helper()
	var count int64
	if err := f.pool.QueryRow(f.ctx,
		"SELECT count(*) FROM user_events WHERE user_id = $1", userID).Scan(&count); err != nil {
		f.t.Fatalf("count user events: %v", err)
	}
	return count
}

func (f *orderedFixture) incarnation(userID int64) uuid.UUID {
	f.t.Helper()
	value, err := f.service.OwnerIncarnation(f.ctx, userID)
	if err != nil {
		f.t.Fatalf("OwnerIncarnation(%d) error = %v", userID, err)
	}
	return value
}

func (f *orderedFixture) listen() *pgx.Conn {
	f.t.Helper()
	conn, err := pgx.Connect(f.ctx, f.db.URL)
	if err != nil {
		f.t.Fatalf("listen connection: %v", err)
	}
	if _, err := conn.Exec(f.ctx, "LISTEN teldrive_events"); err != nil {
		t := f.t
		_ = conn.Close(f.ctx)
		t.Fatalf("LISTEN teldrive_events: %v", err)
	}
	f.t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func expectNoNotificationFor(t *testing.T, conn *pgx.Conn, userID int64, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for {
		waitCtx, cancel := context.WithDeadline(context.Background(), deadline)
		notification, err := conn.WaitForNotification(waitCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return
			}
			t.Fatalf("wait for event notification: %v", err)
		}
		var payload struct {
			UserID int64 `json:"user_id"`
		}
		if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
			continue
		}
		if payload.UserID == userID {
			t.Fatalf("transaction emitted a notification for user %d", userID)
		}
	}
}

func pgErrorCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

const preparedHolderName = "teldrive_test_ordered_gate_holder"

// preparedHolder creates one committed event, then prepares a transaction that
// deletes it. The deferred floor trigger has already run, so the prepared
// transaction holds the global stream gate after its abortable work completed.
// PostgreSQL refuses PREPARE TRANSACTION after NOTIFY, so this witnesses the
// post-trigger durable tail through the floor path rather than an append.
func (f *orderedFixture) preparedHolder(userID int64) int64 {
	f.t.Helper()
	victim := f.insertEvent(userID, "test.prepared")
	pc, err := f.pool.Acquire(f.ctx)
	if err != nil {
		f.t.Fatalf("acquire preparer connection: %v", err)
	}
	run := func(statement string) {
		if _, err := pc.Conn().PgConn().Exec(f.ctx, statement).ReadAll(); err != nil {
			pc.Release()
			f.t.Fatalf("prepared holder %q: %v", statement, err)
		}
	}
	run("BEGIN")
	if _, err := pc.Conn().Exec(f.ctx, "DELETE FROM user_events WHERE id = $1", victim); err != nil {
		pc.Release()
		f.t.Fatalf("prepare victim delete: %v", err)
	}
	run("PREPARE TRANSACTION '" + preparedHolderName + "'")
	pc.Release()
	f.t.Cleanup(func() {
		rollbackPrepared(f)
	})
	return victim
}

func rollbackPrepared(f *orderedFixture) {
	pc, err := f.pool.Acquire(context.Background())
	if err != nil {
		return
	}
	defer pc.Release()
	_, _ = pc.Conn().PgConn().Exec(context.Background(), "ROLLBACK PREPARED '"+preparedHolderName+"'").ReadAll()
}

func commitPrepared(t *testing.T, f *orderedFixture) {
	t.Helper()
	pc, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatalf("acquire committed connection: %v", err)
	}
	defer pc.Release()
	if _, err := pc.Conn().PgConn().Exec(f.ctx, "COMMIT PREPARED '"+preparedHolderName+"'").ReadAll(); err != nil {
		t.Fatalf("COMMIT PREPARED: %v", err)
	}
}

func (f *orderedFixture) commitEventUnderGate(userID int64) (time.Duration, error) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_events (user_id, event_type, resource_type, payload)
		VALUES ($1, 'test.waiter', 'test', '{}'::jsonb)`, userID); err != nil {
		_ = tx.Rollback(ctx)
		return 0, err
	}
	started := time.Now()
	err = tx.Commit(ctx)
	return time.Since(started), err
}

func TestB3CommittedEventOrder(t *testing.T) {
	f := newOrderedFixture(t)

	t.Run("low_id_late_commit", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		lowTx := f.transaction()
		lowID := f.insertEventTx(lowTx, userID, "test.low")
		highTx := f.transaction()
		highID := f.insertEventTx(highTx, userID, "test.high")
		if err := highTx.Commit(f.ctx); err != nil {
			t.Fatalf("commit high-id transaction: %v", err)
		}
		if err := lowTx.Commit(f.ctx); err != nil {
			t.Fatalf("commit low-id transaction: %v", err)
		}
		if highID <= lowID {
			t.Fatalf("legacy ids did not follow insert order: low=%d high=%d", lowID, highID)
		}
		lowPosition := f.position(lowID)
		highPosition := f.position(highID)
		if highPosition >= lowPosition {
			t.Fatalf("high legacy id committed first but position %d is not before %d", highPosition, lowPosition)
		}
		list := f.ordered(userID, 0, nil)
		if len(list) != 2 || list[0].Type != "test.high" || list[1].Type != "test.low" {
			t.Fatalf("ordered replay = %#v, want commit order high then low", list)
		}
		state := f.state(userID)
		if state.High != lowPosition || state.Floor != 0 {
			t.Fatalf("state = %#v, want high %d floor 0", state, lowPosition)
		}
	})

	t.Run("high_id_late_commit", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		earlyTx := f.transaction()
		earlyID := f.insertEventTx(earlyTx, userID, "test.early")
		lateTx := f.transaction()
		lateID := f.insertEventTx(lateTx, userID, "test.late")
		if err := earlyTx.Commit(f.ctx); err != nil {
			t.Fatalf("commit early transaction: %v", err)
		}
		if err := lateTx.Commit(f.ctx); err != nil {
			t.Fatalf("commit late transaction: %v", err)
		}
		if f.position(lateID) <= f.position(earlyID) {
			t.Fatalf("commit order was not preserved for ascending ids")
		}
		list := f.ordered(userID, 0, nil)
		if len(list) != 2 || list[0].Type != "test.early" || list[1].Type != "test.late" {
			t.Fatalf("ordered replay = %#v, want early then late", list)
		}
	})

	t.Run("rollback", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		listener := f.listen()
		allocatorBefore := f.allocatorPosition()
		tx := f.transaction()
		f.insertEventTx(tx, userID, "test.rolled_back")
		if err := tx.Rollback(f.ctx); err != nil {
			t.Fatalf("rollback event transaction: %v", err)
		}
		expectNoNotificationFor(t, listener, userID, 250*time.Millisecond)
		if got := f.eventCount(userID); got != 0 {
			t.Fatalf("rolled-back transaction left %d events", got)
		}
		state := f.state(userID)
		if state.High != 0 || state.Floor != 0 {
			t.Fatalf("rolled-back strict state = %#v, want zero", state)
		}
		if got := f.allocatorPosition(); got != allocatorBefore {
			t.Fatalf("rolled-back allocator = %d, want %d", got, allocatorBefore)
		}
		var legacy int64
		if err := f.pool.QueryRow(f.ctx,
			"SELECT count(*) FROM user_event_stream_state WHERE user_id = $1", userID).Scan(&legacy); err != nil {
			t.Fatalf("read legacy state: %v", err)
		}
		if legacy != 0 {
			t.Fatal("rolled-back transaction advanced legacy stream state")
		}
	})

	t.Run("multi_owner_multi_event", func(t *testing.T) {
		f := f.forTest(t)
		firstUser := f.user()
		secondUser := f.user()
		allocatorBefore := f.allocatorPosition()
		tx := f.transaction()
		firstEvent := f.insertEventTx(tx, firstUser, "test.first.one")
		secondEvent := f.insertEventTx(tx, secondUser, "test.second.one")
		firstEventSecond := f.insertEventTx(tx, firstUser, "test.first.two")
		// Domain work and event insertion must not take the global gate before the
		// deferred trigger runs at commit.
		if got := f.allocatorPosition(); got != allocatorBefore {
			t.Fatalf("allocator advanced before commit: %d, want %d", got, allocatorBefore)
		}
		var gateLocks int64
		if err := tx.QueryRow(f.ctx, `
			SELECT count(*) FROM pg_locks
			WHERE pid = pg_backend_pid()
			  AND relation = 'event_stream_allocator'::regclass`).Scan(&gateLocks); err != nil {
			t.Fatalf("inspect pre-commit locks: %v", err)
		}
		if gateLocks != 0 {
			t.Fatalf("transaction took %d allocator locks before deferred entry", gateLocks)
		}
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatalf("commit multi-owner transaction: %v", err)
		}
		firstPosition := f.position(firstEvent)
		secondPosition := f.position(secondEvent)
		firstSecondPosition := f.position(firstEventSecond)
		if !(firstPosition < secondPosition && secondPosition < firstSecondPosition) {
			t.Fatalf("positions %d,%d,%d are not in insert order", firstPosition, secondPosition, firstSecondPosition)
		}
		firstState := f.state(firstUser)
		secondState := f.state(secondUser)
		if firstState.High != firstSecondPosition || firstState.Floor != 0 {
			t.Fatalf("first owner state = %#v", firstState)
		}
		if secondState.High != secondPosition || secondState.Floor != 0 {
			t.Fatalf("second owner state = %#v", secondState)
		}
		if got := f.ordered(firstUser, 0, nil); len(got) != 2 {
			t.Fatalf("first owner ordered events = %d, want 2", len(got))
		}
		if got := f.ordered(secondUser, 0, nil); len(got) != 1 {
			t.Fatalf("second owner ordered events = %d, want 1", len(got))
		}
		if got := f.legacyLastEventID(firstUser); got != firstEventSecond {
			t.Fatalf("first legacy cursor = %d, want %d", got, firstEventSecond)
		}
		if got := f.legacyLastEventID(secondUser); got != secondEvent {
			t.Fatalf("second legacy cursor = %d, want %d", got, secondEvent)
		}
	})

	t.Run("direct_insert", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		eventID := f.insertEvent(userID, "test.direct")
		position := f.position(eventID)
		list := f.ordered(userID, 0, nil)
		if len(list) != 1 || list[0].ID != eventID || list[0].Position != position {
			t.Fatalf("ordered direct event = %#v", list)
		}
		if got := f.incarnation(userID); got == uuid.Nil {
			t.Fatal("direct insert did not capture an owner incarnation")
		}
		if got := f.legacyLastEventID(userID); got != eventID {
			t.Fatalf("legacy cursor = %d, want %d", got, eventID)
		}
	})

	t.Run("cascade_owner_recreate", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		before := f.incarnation(userID)
		f.insertFolder(userID, "keep")
		if _, err := f.pool.Exec(f.ctx, `
			INSERT INTO channels (channel_id, user_id, name)
			VALUES (900000000001, $1, 'channel')`, userID); err != nil {
			t.Fatalf("seed channel: %v", err)
		}
		var fileID string
		if err := f.pool.QueryRow(f.ctx,
			"SELECT id::text FROM files WHERE user_id = $1 LIMIT 1", userID).Scan(&fileID); err != nil {
			t.Fatalf("read file id: %v", err)
		}
		if _, err := f.pool.Exec(f.ctx, `
			INSERT INTO file_shares (file_id, owner_id, token_prefix, token_hash)
			VALUES ($1, $2, 'prefix', decode(md5(random()::text), 'hex'))`, fileID, userID); err != nil {
			t.Fatalf("seed share: %v", err)
		}
		if _, err := f.pool.Exec(f.ctx, "DELETE FROM users WHERE user_id = $1", userID); err != nil {
			t.Fatalf("delete owner with children: %v", err)
		}
		if got := f.eventCount(userID); got != 0 {
			t.Fatalf("cascade left %d events for the deleted owner", got)
		}
		var states int64
		if err := f.pool.QueryRow(f.ctx,
			"SELECT count(*) FROM event_owner_stream_state WHERE owner_incarnation = $1",
			pgtype.UUID{Bytes: before, Valid: true}).Scan(&states); err != nil {
			t.Fatalf("count retired state: %v", err)
		}
		if states != 0 {
			t.Fatalf("retired incarnation still has %d state rows", states)
		}
		if _, err := f.pool.Exec(f.ctx, "INSERT INTO users (user_id) VALUES ($1)", userID); err != nil {
			t.Fatalf("recreate owner: %v", err)
		}
		after := f.incarnation(userID)
		if after == before {
			t.Fatal("recreated owner reused the retired incarnation")
		}
		state := f.state(userID)
		if state.High != 0 || state.Floor != 0 {
			t.Fatalf("recreated owner state = %#v, want fresh zero state", state)
		}
		eventID := f.insertEvent(userID, "test.recreated")
		if f.position(eventID) <= 0 {
			t.Fatal("recreated owner did not receive a new global position")
		}
		if got := f.ordered(userID, 0, nil); len(got) != 1 || got[0].ID != eventID {
			t.Fatalf("recreated owner ordered events = %#v", got)
		}
	})

	t.Run("producer_retention_lock_order", func(t *testing.T) {
		f := f.forTest(t)
		holderUser := f.user()
		producerUser := f.user()
		retentionUser := f.user()
		retentionEvent := f.insertEvent(retentionUser, "test.expired")
		f.preparedHolder(holderUser)

		tx := f.transaction()
		started := time.Now()
		if _, err := tx.Exec(f.ctx, `
			INSERT INTO files (user_id, name, kind, mod_time)
			VALUES ($1, 'produced', 'folder', now())`, producerUser); err != nil {
			t.Fatalf("producer work under held gate: %v", err)
		}
		if _, err := tx.Exec(f.ctx, "DELETE FROM user_events WHERE id = $1", retentionEvent); err != nil {
			t.Fatalf("retention delete in transaction: %v", err)
		}
		if elapsed := time.Since(started); elapsed >= time.Second {
			t.Fatalf("domain work waited %s under a held gate", elapsed)
		}
		if err := tx.Rollback(f.ctx); err != nil {
			t.Fatalf("rollback pre-commit domain work: %v", err)
		}
		commitPrepared(t, f)
		if got := f.eventCount(producerUser); got != 0 {
			t.Fatalf("rolled-back producer left %d events", got)
		}
		if got := f.eventCount(retentionUser); got != 1 {
			t.Fatalf("rolled-back retention left %d events", got)
		}
	})

	t.Run("cascade_file_share_channel_no_fk_failure", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		folderID := f.insertFolder(userID, "folder")
		f.insertFolder(userID, "second")
		if _, err := f.pool.Exec(f.ctx, `
			INSERT INTO channels (channel_id, user_id, name)
			VALUES (900000000002, $1, 'cascade-channel')`, userID); err != nil {
			t.Fatalf("seed channel: %v", err)
		}
		if _, err := f.pool.Exec(f.ctx, `
			INSERT INTO file_shares (file_id, owner_id, token_prefix, token_hash)
			VALUES ($1, $2, 'prefix', decode(md5(random()::text), 'hex'))`, folderID, userID); err != nil {
			t.Fatalf("seed share: %v", err)
		}
		incarnation := f.incarnation(userID)
		tx := f.transaction()
		if _, err := tx.Exec(f.ctx, "DELETE FROM users WHERE user_id = $1", userID); err != nil {
			t.Fatalf("delete owner inside transaction: %v", err)
		}
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatalf("commit owner cascade: %v", err)
		}
		var eventsLeft, statesLeft int64
		if err := f.pool.QueryRow(f.ctx, `
			SELECT
				(SELECT count(*) FROM user_events WHERE user_id = $1),
				(SELECT count(*) FROM event_owner_stream_state WHERE owner_incarnation = $2)`,
			userID, pgtype.UUID{Bytes: incarnation, Valid: true}).Scan(&eventsLeft, &statesLeft); err != nil {
			t.Fatalf("inspect cascade result: %v", err)
		}
		if eventsLeft != 0 || statesLeft != 0 {
			t.Fatalf("cascade left events=%d states=%d", eventsLeft, statesLeft)
		}
	})

	t.Run("cascade_rollback_and_ordinary_child_events", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		folderID := f.insertFolder(userID, "folder")
		if _, err := f.pool.Exec(f.ctx, `
			INSERT INTO channels (channel_id, user_id, name)
			VALUES (900000000003, $1, 'rollback-channel')`, userID); err != nil {
			t.Fatalf("seed channel: %v", err)
		}
		if _, err := f.pool.Exec(f.ctx, `
			INSERT INTO file_shares (file_id, owner_id, token_prefix, token_hash)
			VALUES ($1, $2, 'prefix', decode(md5(random()::text), 'hex'))`, folderID, userID); err != nil {
			t.Fatalf("seed share: %v", err)
		}
		incarnation := f.incarnation(userID)
		eventsBefore := f.eventCount(userID)
		tx := f.transaction()
		if _, err := tx.Exec(f.ctx, "DELETE FROM users WHERE user_id = $1", userID); err != nil {
			t.Fatalf("delete owner inside transaction: %v", err)
		}
		if err := tx.Rollback(f.ctx); err != nil {
			t.Fatalf("rollback owner cascade: %v", err)
		}
		var users, files, shares, channels, states int64
		if err := f.pool.QueryRow(f.ctx, `
			SELECT
				(SELECT count(*) FROM users WHERE user_id = $1),
				(SELECT count(*) FROM files WHERE user_id = $1),
				(SELECT count(*) FROM file_shares WHERE owner_id = $1),
				(SELECT count(*) FROM channels WHERE user_id = $1),
				(SELECT count(*) FROM event_owner_stream_state WHERE owner_incarnation = $2)`,
			userID, pgtype.UUID{Bytes: incarnation, Valid: true}).Scan(&users, &files, &shares, &channels, &states); err != nil {
			t.Fatalf("inspect rolled-back cascade: %v", err)
		}
		if users != 1 || files != 1 || shares != 1 || channels != 1 || states != 1 {
			t.Fatalf("rollback did not restore cascade: users=%d files=%d shares=%d channels=%d states=%d",
				users, files, shares, channels, states)
		}
		if got := f.eventCount(userID); got != eventsBefore {
			t.Fatalf("rollback changed event count: %d, want %d", got, eventsBefore)
		}

		if _, err := f.pool.Exec(f.ctx, "DELETE FROM channels WHERE channel_id = 900000000003"); err != nil {
			t.Fatalf("ordinary channel delete: %v", err)
		}
		if _, err := f.pool.Exec(f.ctx, "DELETE FROM file_shares WHERE owner_id = $1", userID); err != nil {
			t.Fatalf("ordinary share delete: %v", err)
		}
		if _, err := f.pool.Exec(f.ctx, "DELETE FROM files WHERE id = $1", folderID); err != nil {
			t.Fatalf("ordinary file delete: %v", err)
		}
		var purged, deletedShare, deletedChannel int64
		if err := f.pool.QueryRow(f.ctx, `
			SELECT
				(SELECT count(*) FROM user_events WHERE user_id = $1 AND event_type = 'file.purged'),
				(SELECT count(*) FROM user_events WHERE user_id = $1 AND event_type = 'share.deleted'),
				(SELECT count(*) FROM user_events WHERE user_id = $1 AND event_type = 'channel.deleted')`,
			userID).Scan(&purged, &deletedShare, &deletedChannel); err != nil {
			t.Fatalf("inspect ordinary delete events: %v", err)
		}
		if purged != 1 || deletedShare != 1 || deletedChannel != 1 {
			t.Fatalf("ordinary deletes emitting purged=%d share=%d channel=%d", purged, deletedShare, deletedChannel)
		}
	})

	t.Run("ordinary_appender_waiter_bounded", func(t *testing.T) {
		f := f.forTest(t)
		holderUser := f.user()
		waiterUser := f.user()
		holderEvent := f.preparedHolder(holderUser)
		holderPosition := f.position(holderEvent)
		allocatorBefore := f.allocatorPosition()
		elapsed, err := f.commitEventUnderGate(waiterUser)
		if err == nil {
			t.Fatal("waiter committed while the global gate was held")
		}
		if code := pgErrorCode(err); code != "55P03" {
			t.Fatalf("waiter error code = %q (%v), want 55P03", code, err)
		}
		if elapsed < 500*time.Millisecond || elapsed > 5*time.Second {
			t.Fatalf("waiter waited %s, want a bounded wait near one second", elapsed)
		}
		if got := f.eventCount(waiterUser); got != 0 {
			t.Fatalf("waiter rollback left %d events", got)
		}
		if got := f.allocatorPosition(); got != allocatorBefore {
			t.Fatalf("waiter changed allocator: %d, want %d", got, allocatorBefore)
		}
		commitPrepared(t, f)
		if got := f.eventCount(holderUser); got != 0 {
			t.Fatalf("holder commit left %d events", got)
		}
		if state := f.state(holderUser); state.Floor != holderPosition {
			t.Fatalf("holder floor = %d, want %d", state.Floor, holderPosition)
		}
	})

	t.Run("ordinary_appender_holder_bounded", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		tx := f.transaction()
		f.insertEventsTx(tx, userID, 100)
		started := time.Now()
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatalf("commit holder batch: %v", err)
		}
		if elapsed := time.Since(started); elapsed >= 5*time.Second {
			t.Fatalf("holder abortable work took %s", elapsed)
		}
		state := f.state(userID)
		if state.High == 0 || state.Floor != 0 {
			t.Fatalf("holder state = %#v", state)
		}
		if got := f.ordered(userID, 0, nil); len(got) != 100 {
			t.Fatalf("holder ordered events = %d, want 100", len(got))
		}
	})

	t.Run("ordinary_multi_owner_append_cap_rolls_back", func(t *testing.T) {
		f := f.forTest(t)
		firstUser := f.user()
		secondUser := f.user()
		allocatorBefore := f.allocatorPosition()
		tx := f.transaction()
		f.insertEventsTx(tx, firstUser, 300)
		f.insertEventsTx(tx, secondUser, 213)
		err := tx.Commit(f.ctx)
		if err == nil {
			t.Fatal("over-cap append transaction committed")
		}
		if code := pgErrorCode(err); code != "54000" {
			t.Fatalf("over-cap error code = %q (%v), want 54000", code, err)
		}
		if got := f.eventCount(firstUser); got != 0 {
			t.Fatalf("over-cap rollback left %d first-owner events", got)
		}
		if got := f.eventCount(secondUser); got != 0 {
			t.Fatalf("over-cap rollback left %d second-owner events", got)
		}
		if got := f.allocatorPosition(); got != allocatorBefore {
			t.Fatalf("over-cap rollback changed allocator: %d, want %d", got, allocatorBefore)
		}
	})

	t.Run("post_trigger_durable_commit_stall_bounds_waiter", func(t *testing.T) {
		f := f.forTest(t)
		holderUser := f.user()
		waiterUser := f.user()
		probeUser := f.user()
		holderEvent := f.preparedHolder(holderUser)
		holderPosition := f.position(holderEvent)
		time.Sleep(5200 * time.Millisecond)
		elapsed, err := f.commitEventUnderGate(waiterUser)
		if err == nil {
			t.Fatal("waiter committed while a prepared transaction held the gate past five seconds")
		}
		if code := pgErrorCode(err); code != "55P03" {
			t.Fatalf("post-trigger waiter error code = %q (%v)", code, err)
		}
		if elapsed > 5*time.Second {
			t.Fatalf("post-trigger waiter waited %s", elapsed)
		}
		commitPrepared(t, f)
		if state := f.state(holderUser); state.Floor != holderPosition {
			t.Fatalf("holder floor = %d, want %d", state.Floor, holderPosition)
		}
		probeID := f.insertEvent(probeUser, "test.after_stall")
		if f.position(probeID) <= holderPosition {
			t.Fatalf("post-release event position %d did not follow %d", f.position(probeID), holderPosition)
		}
		if got := f.eventCount(waiterUser); got != 0 {
			t.Fatalf("timed-out waiter left %d events", got)
		}
	})
}

func TestB3RetentionReplayBoundary(t *testing.T) {
	f := newOrderedFixture(t)

	t.Run("interior_removed_cursor_survives", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		ids := make([]int64, 5)
		for index := range ids {
			ids[index] = f.insertEvent(userID, fmt.Sprintf("test.replay.%d", index))
		}
		positions := f.positions(userID)
		if _, err := f.pool.Exec(f.ctx, "DELETE FROM user_events WHERE id = $1", ids[2]); err != nil {
			t.Fatalf("delete interior event: %v", err)
		}
		state := f.state(userID)
		if state.Floor != positions[2] || state.High != positions[4] {
			t.Fatalf("state = %#v, want floor %d high %d", state, positions[2], positions[4])
		}
		list := f.ordered(userID, positions[2], nil)
		if len(list) != 2 || list[0].Position != positions[3] || list[1].Position != positions[4] {
			t.Fatalf("cursor at the floor did not survive: %#v", list)
		}
	})

	t.Run("delete_rollback_floor", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		firstID := f.insertEvent(userID, "test.floor.one")
		f.insertEvent(userID, "test.floor.two")
		positions := f.positions(userID)
		tx := f.transaction()
		if _, err := tx.Exec(f.ctx, "DELETE FROM user_events WHERE id = $1", firstID); err != nil {
			t.Fatalf("delete event inside transaction: %v", err)
		}
		if err := tx.Rollback(f.ctx); err != nil {
			t.Fatalf("rollback delete: %v", err)
		}
		if state := f.state(userID); state.Floor != 0 {
			t.Fatalf("rolled-back delete advanced floor to %d", state.Floor)
		}
		if _, err := f.pool.Exec(f.ctx, "DELETE FROM user_events WHERE id = $1", firstID); err != nil {
			t.Fatalf("delete event: %v", err)
		}
		if state := f.state(userID); state.Floor != positions[0] || state.High != positions[1] {
			t.Fatalf("committed delete state = %#v", state)
		}
	})

	t.Run("old_row_does_not_advance", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		eventID := f.insertEvent(userID, "test.legacy_row")
		position := f.position(eventID)
		if _, err := f.pool.Exec(f.ctx, `
			UPDATE user_events SET owner_incarnation = NULL, strict_position = NULL
			WHERE id = $1`, eventID); err != nil {
			t.Fatalf("demote event to legacy row: %v", err)
		}
		if _, err := f.pool.Exec(f.ctx, "DELETE FROM user_events WHERE id = $1", eventID); err != nil {
			t.Fatalf("delete legacy row: %v", err)
		}
		state := f.state(userID)
		if state.Floor != 0 || state.High != position {
			t.Fatalf("legacy delete changed strict state: %#v", state)
		}
	})

	t.Run("live_overtaken_cursor", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		firstID := f.insertEvent(userID, "test.overtake.one")
		secondID := f.insertEvent(userID, "test.overtake.two")
		positions := f.positions(userID)
		if _, err := f.pool.Exec(f.ctx, "DELETE FROM user_events WHERE id = $1", secondID); err != nil {
			t.Fatalf("delete live suffix: %v", err)
		}
		state := f.state(userID)
		if state.Floor != positions[1] || state.High != positions[1] {
			t.Fatalf("overtaken state = %#v", state)
		}
		if state.Floor <= positions[0] {
			t.Fatalf("floor %d did not overtake cursor %d", state.Floor, positions[0])
		}
		if list := f.ordered(userID, positions[0], nil); len(list) != 0 {
			t.Fatalf("overtaken cursor still replayed %#v", list)
		}
		var surviving int64
		if err := f.pool.QueryRow(f.ctx,
			"SELECT count(*) FROM user_events WHERE id = $1", firstID).Scan(&surviving); err != nil {
			t.Fatalf("count surviving event: %v", err)
		}
		if surviving != 1 {
			t.Fatal("cursor event did not survive the suffix removal")
		}
	})

	t.Run("backlog_bounded_batches_exact_count", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		const total = 1100
		for inserted := 0; inserted < total; inserted += 400 {
			count := 400
			if remaining := total - inserted; remaining < count {
				count = remaining
			}
			tx := f.transaction()
			for index := range count {
				position := inserted + index
				if _, err := tx.Exec(f.ctx, `
					INSERT INTO user_events (user_id, event_type, resource_type, payload, occurred_at)
					VALUES ($1, 'test.backlog', 'test', '{}'::jsonb, now() - interval '2 hours' + $2 * interval '1 second')`,
					userID, position); err != nil {
					t.Fatalf("insert backlog event %d: %v", position, err)
				}
			}
			if err := tx.Commit(f.ctx); err != nil {
				t.Fatalf("commit backlog batch: %v", err)
			}
		}
		high := f.state(userID).High
		queries := sqlcgen.New(f.pool)
		cutoff := pgtype.Timestamptz{Time: time.Now().UTC().Add(-time.Hour), Valid: true}
		var batches []int64
		var deleted int64
		for {
			count, err := queries.DeleteUserEventsBefore(f.ctx, sqlcgen.DeleteUserEventsBeforeParams{
				Cutoff: cutoff, EventLimit: 512,
			})
			if err != nil {
				t.Fatalf("retention batch: %v", err)
			}
			if count > 512 {
				t.Fatalf("retention batch deleted %d rows", count)
			}
			batches = append(batches, count)
			deleted += count
			if count < 512 {
				break
			}
		}
		if deleted != total {
			t.Fatalf("retention deleted %d rows in %v batches, want %d", deleted, batches, total)
		}
		if len(batches) != 3 || batches[0] != 512 || batches[1] != 512 || batches[2] != 76 {
			t.Fatalf("retention batches = %v, want [512 512 76]", batches)
		}
		state := f.state(userID)
		if state.Floor != high || state.High != high {
			t.Fatalf("retained state = %#v, want floor=high=%d", state, high)
		}
	})

	t.Run("per_incarnation_max_removed", func(t *testing.T) {
		f := f.forTest(t)
		firstUser := f.user()
		secondUser := f.user()
		firstIDs := make([]int64, 3)
		secondIDs := make([]int64, 3)
		for index := range firstIDs {
			firstIDs[index] = f.insertEvent(firstUser, fmt.Sprintf("test.max.first.%d", index))
			secondIDs[index] = f.insertEvent(secondUser, fmt.Sprintf("test.max.second.%d", index))
		}
		firstPositions := f.positions(firstUser)
		secondPositions := f.positions(secondUser)
		tx := f.transaction()
		if _, err := tx.Exec(f.ctx, "DELETE FROM user_events WHERE id = ANY($1)",
			[]int64{firstIDs[0], firstIDs[2], secondIDs[1]}); err != nil {
			t.Fatalf("delete mixed events: %v", err)
		}
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatalf("commit mixed delete: %v", err)
		}
		firstState := f.state(firstUser)
		secondState := f.state(secondUser)
		if firstState.Floor != firstPositions[2] || firstState.High != firstPositions[2] {
			t.Fatalf("first owner state = %#v, want floor=%d", firstState, firstPositions[2])
		}
		if secondState.Floor != secondPositions[1] || secondState.High != secondPositions[2] {
			t.Fatalf("second owner state = %#v, want floor=%d high=%d",
				secondState, secondPositions[1], secondPositions[2])
		}
	})

	t.Run("gate_timeout_rolls_back_then_unrelated_progress", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		otherUser := f.user()
		tx := f.transaction()
		if _, err := tx.Exec(f.ctx, "SELECT pg_sleep(5.2)"); err != nil {
			t.Fatalf("expire transaction budget: %v", err)
		}
		_, err := tx.Exec(f.ctx, `
			INSERT INTO user_events (user_id, event_type, resource_type, payload)
			VALUES ($1, 'test.late', 'test', '{}'::jsonb)`, userID)
		if err == nil {
			t.Fatal("event work started after the absolute budget")
		}
		if code := pgErrorCode(err); code != "57014" {
			t.Fatalf("exhausted-budget error code = %q (%v)", code, err)
		}
		if rollbackErr := tx.Rollback(f.ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			t.Fatalf("rollback exhausted transaction: %v", rollbackErr)
		}
		if got := f.eventCount(userID); got != 0 {
			t.Fatalf("exhausted transaction left %d events", got)
		}
		otherID := f.insertEvent(otherUser, "test.unrelated")
		if f.position(otherID) <= 0 {
			t.Fatal("unrelated progress did not resume after the timed-out transaction")
		}
	})

	t.Run("cascade_gate_work_bounded", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		tx := f.transaction()
		for index := range 200 {
			name := fmt.Sprintf("cascade-%03d", index)
			if _, err := tx.Exec(f.ctx, `
				INSERT INTO files (user_id, name, kind, mod_time)
				VALUES ($1, $2, 'folder', now())`, userID, name); err != nil {
				t.Fatalf("seed cascade file %d: %v", index, err)
			}
		}
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatalf("commit cascade files: %v", err)
		}
		started := time.Now()
		if _, err := f.pool.Exec(f.ctx, "DELETE FROM users WHERE user_id = $1", userID); err != nil {
			t.Fatalf("delete cascade owner: %v", err)
		}
		if elapsed := time.Since(started); elapsed >= 5*time.Second {
			t.Fatalf("owner cascade took %s", elapsed)
		}
		if got := f.eventCount(userID); got != 0 {
			t.Fatalf("cascade left %d events", got)
		}
	})

	t.Run("direct_delete_floor_and_count", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		ids := make([]int64, 3)
		for index := range ids {
			ids[index] = f.insertEvent(userID, fmt.Sprintf("test.direct.delete.%d", index))
		}
		positions := f.positions(userID)
		legacyBefore := f.legacyLastEventID(userID)
		tag, err := f.pool.Exec(f.ctx, "DELETE FROM user_events WHERE id = ANY($1)", []int64{ids[0], ids[2]})
		if err != nil {
			t.Fatalf("direct delete: %v", err)
		}
		if tag.RowsAffected() != 2 {
			t.Fatalf("direct delete removed %d rows", tag.RowsAffected())
		}
		state := f.state(userID)
		if state.Floor != positions[2] || state.High != positions[2] {
			t.Fatalf("direct delete state = %#v, want floor=high=%d", state, positions[2])
		}
		if got := f.legacyLastEventID(userID); got != legacyBefore {
			t.Fatalf("direct delete changed legacy cursor: %d, want %d", got, legacyBefore)
		}
	})

	t.Run("direct_delete_waiter_bounded", func(t *testing.T) {
		f := f.forTest(t)
		holderUser := f.user()
		deleteUser := f.user()
		doomedID := f.insertEvent(deleteUser, "test.delete.waiter")
		f.preparedHolder(holderUser)
		ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
		defer cancel()
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin delete waiter: %v", err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM user_events WHERE id = $1", doomedID); err != nil {
			t.Fatalf("delete under held gate: %v", err)
		}
		started := time.Now()
		err = tx.Commit(ctx)
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("delete waiter committed while the gate was held")
		}
		if code := pgErrorCode(err); code != "55P03" {
			t.Fatalf("delete waiter error code = %q (%v)", code, err)
		}
		if elapsed < 500*time.Millisecond || elapsed > 5*time.Second {
			t.Fatalf("delete waiter waited %s", elapsed)
		}
		if got := f.eventCount(deleteUser); got != 1 {
			t.Fatalf("timed-out delete removed events: %d", got)
		}
		if state := f.state(deleteUser); state.Floor != 0 {
			t.Fatalf("timed-out delete advanced floor to %d", state.Floor)
		}
		commitPrepared(t, f)
	})

	t.Run("direct_delete_holder_bounded", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		tx := f.transaction()
		f.insertEventsTx(tx, userID, 300)
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatalf("commit delete holder seed: %v", err)
		}
		high := f.state(userID).High
		started := time.Now()
		tag, err := f.pool.Exec(f.ctx, "DELETE FROM user_events WHERE user_id = $1", userID)
		if err != nil {
			t.Fatalf("delete holder batch: %v", err)
		}
		if elapsed := time.Since(started); elapsed >= 5*time.Second {
			t.Fatalf("delete holder took %s", elapsed)
		}
		if tag.RowsAffected() != 300 {
			t.Fatalf("delete holder removed %d rows", tag.RowsAffected())
		}
		state := f.state(userID)
		if state.Floor != high || state.High != high {
			t.Fatalf("delete holder state = %#v, want floor=high=%d", state, high)
		}
	})

	t.Run("direct_multi_owner_delete_cap_rolls_back", func(t *testing.T) {
		f := f.forTest(t)
		firstUser := f.user()
		secondUser := f.user()
		firstTx := f.transaction()
		f.insertEventsTx(firstTx, firstUser, 260)
		if err := firstTx.Commit(f.ctx); err != nil {
			t.Fatalf("commit first owner seed: %v", err)
		}
		secondTx := f.transaction()
		f.insertEventsTx(secondTx, secondUser, 260)
		if err := secondTx.Commit(f.ctx); err != nil {
			t.Fatalf("commit second owner seed: %v", err)
		}
		firstHigh := f.state(firstUser).High
		secondHigh := f.state(secondUser).High
		tx := f.transaction()
		if _, err := tx.Exec(f.ctx, "DELETE FROM user_events WHERE user_id = ANY($1)",
			[]int64{firstUser, secondUser}); err != nil {
			t.Fatalf("over-cap delete: %v", err)
		}
		err := tx.Commit(f.ctx)
		if err == nil {
			t.Fatal("over-cap delete committed")
		}
		if code := pgErrorCode(err); code != "54000" {
			t.Fatalf("over-cap delete error code = %q (%v)", code, err)
		}
		if got := f.eventCount(firstUser); got != 260 {
			t.Fatalf("over-cap delete rollback left %d first-owner events", got)
		}
		if got := f.eventCount(secondUser); got != 260 {
			t.Fatalf("over-cap delete rollback left %d second-owner events", got)
		}
		if got := f.state(firstUser); got.Floor != 0 || got.High != firstHigh {
			t.Fatalf("first owner state after rollback = %#v", got)
		}
		if got := f.state(secondUser); got.Floor != 0 || got.High != secondHigh {
			t.Fatalf("second owner state after rollback = %#v", got)
		}
	})
}

func TestB3BulkCatalogLane(t *testing.T) {
	f := newOrderedFixture(t)

	t.Run("ordinary_513_still_bounded", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		tx := f.transaction()
		f.insertEventsTx(tx, userID, 513)
		err := tx.Commit(f.ctx)
		if err == nil {
			t.Fatal("ordinary 513-row append committed")
		}
		if code := pgErrorCode(err); code != "54000" {
			t.Fatalf("ordinary over-cap error code = %q (%v)", code, err)
		}
		if got := f.eventCount(userID); got != 0 {
			t.Fatalf("ordinary over-cap rollback left %d events", got)
		}
	})

	t.Run("bulk_lane_lifts_row_cap", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		tx := f.transaction()
		if _, err := tx.Exec(f.ctx, "SET LOCAL teldrive.event_bulk = 'on'"); err != nil {
			t.Fatalf("select bulk lane: %v", err)
		}
		f.insertEventsTx(tx, userID, 513)
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatalf("bulk 513-row append: %v", err)
		}
		if got := f.eventCount(userID); got != 513 {
			t.Fatalf("bulk append events = %d, want 513", got)
		}
		state := f.state(userID)
		if state.High == 0 || state.Floor != 0 {
			t.Fatalf("bulk state = %#v", state)
		}
		if positions := f.positions(userID); len(positions) != 513 || positions[0] >= positions[512] {
			t.Fatalf("bulk positions not strictly ordered: count=%d", len(positions))
		}
	})

	t.Run("setting_does_not_leak_to_pool", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		otherUser := f.user()
		pc, err := f.pool.Acquire(f.ctx)
		if err != nil {
			t.Fatalf("acquire pinned connection: %v", err)
		}
		defer pc.Release()
		tx, err := pc.Begin(f.ctx)
		if err != nil {
			t.Fatalf("begin pinned transaction: %v", err)
		}
		if _, err := tx.Exec(f.ctx, "SET LOCAL teldrive.event_bulk = 'on'"); err != nil {
			t.Fatalf("select bulk lane: %v", err)
		}
		if _, err := tx.Exec(f.ctx, `
			INSERT INTO user_events (user_id, event_type, resource_type, payload)
			VALUES ($1, 'test.bulk.pinned', 'test', '{}'::jsonb)`, userID); err != nil {
			t.Fatalf("bulk append on pinned connection: %v", err)
		}
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatalf("commit pinned bulk transaction: %v", err)
		}
		var value *string
		if err := pc.Conn().QueryRow(f.ctx,
			"SELECT current_setting('teldrive.event_bulk', true)").Scan(&value); err != nil {
			t.Fatalf("read bulk setting after commit: %v", err)
		}
		if value != nil && *value == "on" {
			t.Fatal("transaction-local bulk setting leaked to the pooled connection")
		}
		// The next ordinary transaction on the same pool must still be capped.
		fallback := f.transaction()
		f.insertEventsTx(fallback, otherUser, 513)
		if err := fallback.Commit(f.ctx); err == nil || pgErrorCode(err) != "54000" {
			t.Fatalf("post-bulk ordinary append error = %v, want 54000", err)
		}
	})

	t.Run("bulk_lane_stalled_transaction_aborts", func(t *testing.T) {
		f := f.forTest(t)
		userID := f.user()
		tx := f.transaction()
		if _, err := tx.Exec(f.ctx, "SET LOCAL teldrive.event_bulk = 'on'"); err != nil {
			t.Fatalf("select bulk lane: %v", err)
		}
		if _, err := tx.Exec(f.ctx, "SELECT pg_sleep(5.2)"); err != nil {
			t.Fatalf("stall bulk transaction: %v", err)
		}
		_, err := tx.Exec(f.ctx, `
			INSERT INTO user_events (user_id, event_type, resource_type, payload)
			VALUES ($1, 'test.bulk.late', 'test', '{}'::jsonb)`, userID)
		if err == nil {
			t.Fatal("stalled bulk transaction accepted event work")
		}
		if code := pgErrorCode(err); code != "57014" {
			t.Fatalf("stalled bulk error code = %q (%v)", code, err)
		}
		if rollbackErr := tx.Rollback(f.ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			t.Fatalf("rollback stalled bulk transaction: %v", rollbackErr)
		}
		if got := f.eventCount(userID); got != 0 {
			t.Fatalf("stalled bulk transaction left %d events", got)
		}
	})

	t.Run("bulk_lane_waiter_bounded", func(t *testing.T) {
		f := f.forTest(t)
		holderUser := f.user()
		waiterUser := f.user()
		f.preparedHolder(holderUser)
		ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
		defer cancel()
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin bulk waiter: %v", err)
		}
		if _, err := tx.Exec(ctx, "SET LOCAL teldrive.event_bulk = 'on'"); err != nil {
			t.Fatalf("select bulk lane for waiter: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO user_events (user_id, event_type, resource_type, payload)
			VALUES ($1, 'test.bulk.waiter', 'test', '{}'::jsonb)`, waiterUser); err != nil {
			t.Fatalf("bulk waiter append: %v", err)
		}
		started := time.Now()
		err = tx.Commit(ctx)
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("bulk waiter committed while the gate was held")
		}
		if code := pgErrorCode(err); code != "55P03" {
			t.Fatalf("bulk waiter error code = %q (%v)", code, err)
		}
		if elapsed < 500*time.Millisecond || elapsed > 5*time.Second {
			t.Fatalf("bulk waiter waited %s", elapsed)
		}
		if got := f.eventCount(waiterUser); got != 0 {
			t.Fatalf("bulk waiter rollback left %d events", got)
		}
		commitPrepared(t, f)
	})
}
