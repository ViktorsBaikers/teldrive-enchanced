-- name: ListUserEventsAfter :many
SELECT id, user_id, event_type, resource_type, resource_id, generation, payload, occurred_at
FROM /* TEMPLATE: schema */user_events
WHERE user_id = sqlc.arg(user_id)
  AND id > sqlc.arg(after_id)
  AND (
    COALESCE(cardinality(sqlc.arg(event_types)::text[]), 0) = 0
    OR event_type = ANY(sqlc.arg(event_types)::text[])
  )
ORDER BY id
LIMIT sqlc.arg(event_limit);

-- name: GetUserEventCursorState :one
SELECT
    EXISTS (
        SELECT 1
        FROM /* TEMPLATE: schema */user_events AS cursor_event
        WHERE cursor_event.user_id = sqlc.arg(cursor_user_id)
          AND cursor_event.id = sqlc.arg(after_id)
    ) AS cursor_exists,
    COALESCE(MIN(event_rows.id), 0)::bigint AS oldest_id,
    COALESCE(MAX(event_rows.id), 0)::bigint AS newest_id,
    COALESCE((
        SELECT stream_state.last_event_id
        FROM /* TEMPLATE: schema */user_event_stream_state AS stream_state
        WHERE stream_state.user_id = sqlc.arg(cursor_user_id)
    ), 0)::bigint AS last_event_id
FROM /* TEMPLATE: schema */user_events AS event_rows
WHERE event_rows.user_id = sqlc.arg(cursor_user_id);

-- name: InsertUserEvent :one
INSERT INTO /* TEMPLATE: schema */user_events (
    user_id, event_type, resource_type, resource_id, generation, payload
) VALUES (
    sqlc.arg(user_id),
    sqlc.arg(event_type),
    sqlc.arg(resource_type),
    sqlc.narg(resource_id),
    sqlc.narg(generation),
    sqlc.arg(payload)
)
RETURNING id, user_id, event_type, resource_type, resource_id, generation, payload, occurred_at;

-- name: DeleteUserEventsBefore :execrows
DELETE FROM /* TEMPLATE: schema */user_events
WHERE id IN (
    SELECT expired.id
    FROM /* TEMPLATE: schema */user_events AS expired
    WHERE expired.occurred_at < sqlc.arg(cutoff)
    ORDER BY expired.occurred_at, expired.id
    LIMIT sqlc.arg(event_limit)
);

-- name: GetEventOwnerIncarnation :one
SELECT incarnation
FROM /* TEMPLATE: schema */event_owner_incarnations
WHERE user_id = sqlc.arg(user_id);

-- name: GetOrderedEventState :one
SELECT
    allocator.stream_epoch,
    owner.incarnation AS owner_incarnation,
    COALESCE(state.high, 0)::bigint AS high,
    COALESCE(state.floor, 0)::bigint AS floor
FROM /* TEMPLATE: schema */event_stream_allocator AS allocator
JOIN /* TEMPLATE: schema */event_owner_incarnations AS owner
    ON owner.user_id = sqlc.arg(user_id)
LEFT JOIN /* TEMPLATE: schema */event_owner_stream_state AS state
    ON state.owner_incarnation = owner.incarnation
WHERE allocator.id = 1;

-- name: ListOrderedUserEvents :many
SELECT
    id, strict_position, event_type, resource_type, resource_id,
    generation, payload, location, occurred_at
FROM /* TEMPLATE: schema */user_events
WHERE owner_incarnation = sqlc.arg(owner_incarnation)
  AND strict_position > sqlc.arg(after_position)
  AND strict_position <= sqlc.arg(high)
  AND (
    COALESCE(cardinality(sqlc.arg(event_types)::text[]), 0) = 0
    OR event_type = ANY(sqlc.arg(event_types)::text[])
  )
ORDER BY strict_position
LIMIT sqlc.arg(event_limit);

-- name: GetOrderedEventTicket :one
SELECT user_id, expires_at, expires_at > now() AS active
FROM /* TEMPLATE: schema */event_stream_tickets
WHERE token_hash = sqlc.arg(token_hash);

-- name: CreateEventStreamTicket :exec
INSERT INTO /* TEMPLATE: schema */event_stream_tickets (token_hash, user_id, expires_at)
VALUES (sqlc.arg(token_hash), sqlc.arg(user_id), sqlc.arg(expires_at));

-- name: GetEventStreamTicketUser :one
SELECT user_id
FROM /* TEMPLATE: schema */event_stream_tickets
WHERE token_hash = sqlc.arg(token_hash)
  AND expires_at > now();

-- name: DeleteExpiredEventStreamTickets :execrows
DELETE FROM /* TEMPLATE: schema */event_stream_tickets
WHERE expires_at <= now();
