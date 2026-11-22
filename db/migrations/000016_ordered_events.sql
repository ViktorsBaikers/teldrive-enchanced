-- +goose Up

-- 000016 is forward-only. Existing user_events keep their identity, payload and
-- recorded history. Legacy rows stay unpositioned and without a location; commit
-- order and historical locations are never reconstructed.
ALTER TABLE /* TEMPLATE: schema */user_events
    ADD COLUMN owner_incarnation UUID,
    ADD COLUMN strict_position BIGINT,
    ADD COLUMN location JSONB;

ALTER TABLE /* TEMPLATE: schema */user_events
    ADD CONSTRAINT user_events_strict_position_positive
        CHECK (strict_position IS NULL OR strict_position > 0),
    ADD CONSTRAINT user_events_location_object
        CHECK (location IS NULL OR jsonb_typeof(location) = 'object');

CREATE UNIQUE INDEX user_events_owner_position_idx
    ON /* TEMPLATE: schema */user_events (owner_incarnation, strict_position)
    WHERE strict_position IS NOT NULL;

-- One immutable incarnation per live numeric user. The users cascade removes it,
-- so a recreated numeric ID always receives a different incarnation and state.
CREATE TABLE /* TEMPLATE: schema */event_owner_incarnations (
    user_id BIGINT PRIMARY KEY REFERENCES /* TEMPLATE: schema */users(user_id) ON DELETE CASCADE,
    incarnation UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- The singleton allocator row is the global stream commit gate. Position order is
-- gate acquisition order, and the gate is retained until transaction commit.
CREATE TABLE /* TEMPLATE: schema */event_stream_allocator (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    stream_epoch UUID NOT NULL DEFAULT gen_random_uuid(),
    last_position BIGINT NOT NULL DEFAULT 0 CHECK (last_position >= 0)
);

INSERT INTO /* TEMPLATE: schema */event_stream_allocator (id) VALUES (1);

-- Strict state is keyed by immutable incarnation and deliberately has no foreign
-- key to users: owner deletion retires it once, after domain work, under the gate.
CREATE TABLE /* TEMPLATE: schema */event_owner_stream_state (
    owner_incarnation UUID PRIMARY KEY,
    high BIGINT NOT NULL DEFAULT 0 CHECK (high >= 0),
    floor BIGINT NOT NULL DEFAULT 0 CHECK (floor >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT event_owner_stream_state_floor_within_high CHECK (floor <= high)
);

INSERT INTO /* TEMPLATE: schema */event_owner_incarnations (user_id)
SELECT user_id FROM /* TEMPLATE: schema */users
ON CONFLICT (user_id) DO NOTHING;

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */event_owner_incarnation(p_schema text, p_user_id bigint)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    v_incarnation uuid;
BEGIN
    EXECUTE format(
        'SELECT incarnation FROM %I.event_owner_incarnations WHERE user_id = $1',
        p_schema
    ) INTO v_incarnation USING p_user_id;
    RETURN v_incarnation;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */ensure_event_owner_incarnation(p_schema text, p_user_id bigint)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    v_incarnation uuid;
BEGIN
    v_incarnation := /* TEMPLATE: schema */event_owner_incarnation(p_schema, p_user_id);
    IF v_incarnation IS NULL THEN
        EXECUTE format(
            'INSERT INTO %I.event_owner_incarnations (user_id) VALUES ($1)
             ON CONFLICT (user_id) DO NOTHING',
            p_schema
        ) USING p_user_id;
        v_incarnation := /* TEMPLATE: schema */event_owner_incarnation(p_schema, p_user_id);
    END IF;
    IF v_incarnation IS NULL THEN
        RAISE EXCEPTION 'event owner incarnation unavailable for user %', p_user_id;
    END IF;
    RETURN v_incarnation;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */event_bump_counter(p_name text)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_value bigint;
BEGIN
    v_value := COALESCE(NULLIF(current_setting(p_name, true), ''), '0')::bigint + 1;
    PERFORM set_config(p_name, v_value::text, true);
    RETURN v_value;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */event_setting_ms(p_name text)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_value text := NULLIF(current_setting(p_name, true), '');
BEGIN
    IF v_value IS NULL OR v_value = '0' THEN
        RETURN 0;
    END IF;
    RETURN (extract(epoch FROM v_value::interval) * 1000)::bigint;
END;
$$;
-- +goose StatementEnd

-- The absolute budget starts at transaction begin. transaction_timeout is armed
-- for the remaining budget and never relaxed; PostgreSQL disables it before the
-- durable commit tail, which is outside this abortable-work bound.
-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */arm_event_budget()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_remaining bigint;
    v_current bigint;
BEGIN
    v_remaining := floor(extract(epoch FROM (
        transaction_timestamp() + interval '5 seconds' - clock_timestamp()
    )) * 1000)::bigint;
    IF v_remaining <= 0 THEN
        RAISE EXCEPTION 'event transaction budget exhausted' USING ERRCODE = '57014';
    END IF;
    v_current := /* TEMPLATE: schema */event_setting_ms('transaction_timeout');
    IF v_current = 0 OR v_current > v_remaining THEN
        PERFORM set_config('transaction_timeout', v_remaining::text || 'ms', true);
    END IF;
END;
$$;
-- +goose StatementEnd

-- Gate acquisition is bounded by the lesser of one second, the remaining budget
-- and an already stricter session lock_timeout.
-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */hold_event_gate(p_schema text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_remaining bigint;
    v_lock bigint;
    v_current bigint;
    v_row integer;
BEGIN
    PERFORM /* TEMPLATE: schema */arm_event_budget();
    v_remaining := floor(extract(epoch FROM (
        transaction_timestamp() + interval '5 seconds' - clock_timestamp()
    )) * 1000)::bigint;
    IF v_remaining <= 0 THEN
        RAISE EXCEPTION 'event transaction budget exhausted' USING ERRCODE = '57014';
    END IF;
    v_lock := LEAST(v_remaining, 1000);
    v_current := /* TEMPLATE: schema */event_setting_ms('lock_timeout');
    IF v_current > 0 AND v_current < v_lock THEN
        v_lock := v_current;
    END IF;
    IF v_lock <= 0 THEN
        RAISE EXCEPTION 'event transaction budget exhausted' USING ERRCODE = '57014';
    END IF;
    PERFORM set_config('lock_timeout', v_lock::text || 'ms', true);
    EXECUTE format(
        'SELECT 1 FROM %I.event_stream_allocator WHERE id = 1 FOR UPDATE',
        p_schema
    ) INTO v_row;
    IF v_row IS NULL THEN
        RAISE EXCEPTION 'event stream allocator is unavailable';
    END IF;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */allocate_event_position(p_schema text)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_position bigint;
BEGIN
    PERFORM /* TEMPLATE: schema */hold_event_gate(p_schema);
    EXECUTE format(
        'UPDATE %I.event_stream_allocator SET last_position = last_position + 1
         WHERE id = 1 RETURNING last_position',
        p_schema
    ) INTO v_position;
    RETURN v_position;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */event_retiring_incarnation(p_user_id bigint)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    v_marker text := NULLIF(current_setting('teldrive.events_retiring_users', true), '');
BEGIN
    IF v_marker IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN substring(
        v_marker FROM ',' || p_user_id::text || ':([0-9a-fA-F-]{36}),'
    )::uuid;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */event_owner_is_retiring(p_schema text, p_user_id bigint)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v_marked uuid;
    v_current uuid;
BEGIN
    v_marked := /* TEMPLATE: schema */event_retiring_incarnation(p_user_id);
    IF v_marked IS NULL THEN
        RETURN false;
    END IF;
    -- The incarnation row may already be cascaded away. A recreated numeric ID
    -- has a new incarnation and therefore is not suppressed.
    v_current := /* TEMPLATE: schema */event_owner_incarnation(p_schema, p_user_id);
    RETURN v_current IS NULL OR v_current = v_marked;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */event_incarnation_is_retired(p_incarnation uuid)
RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN position(
        ',' || p_incarnation::text || ','
        IN COALESCE(current_setting('teldrive.events_retired', true), ',')
    ) > 0;
END;
$$;
-- +goose StatementEnd

-- Capture the incarnation for every writer, including old SQL that omits it, and
-- arm the abortable budget at entry to event work.
-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */prepare_user_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_incarnation uuid;
BEGIN
    PERFORM /* TEMPLATE: schema */arm_event_budget();
    v_incarnation := /* TEMPLATE: schema */ensure_event_owner_incarnation(TG_TABLE_SCHEMA, NEW.user_id);
    IF NEW.owner_incarnation IS NULL THEN
        NEW.owner_incarnation := v_incarnation;
    ELSIF NEW.owner_incarnation <> v_incarnation THEN
        RAISE EXCEPTION 'event owner incarnation mismatch for user %', NEW.user_id;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */budget_user_event_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM /* TEMPLATE: schema */arm_event_budget();
    RETURN OLD;
END;
$$;
-- +goose StatementEnd

-- Append work happens only at deferred entry, after domain work. The cap is
-- checked before the gate; positions come from the singleton allocator row and
-- the gate stays held to commit. Legacy last_event_id is updated with GREATEST.
-- Catalog-wide subtree operations select the bulk lane with a transaction-local
-- teldrive.event_bulk setting: they keep the gate wait and the 5-second budget
-- but are not row-count bounded, because their event cost is a single statement.
-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */append_user_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_position bigint;
    v_survived integer;
BEGIN
    IF NEW.owner_incarnation IS NULL THEN
        RETURN NULL;
    END IF;
    EXECUTE format(
        'SELECT 1 FROM %I.user_events WHERE id = $1',
        TG_TABLE_SCHEMA
    ) INTO v_survived USING NEW.id;
    IF v_survived IS NULL THEN
        RETURN NULL;
    END IF;
    IF COALESCE(current_setting('teldrive.event_bulk', true), 'off') <> 'on' THEN
        IF /* TEMPLATE: schema */event_bump_counter('teldrive.events_assign_count') > 512 THEN
            RAISE EXCEPTION 'event append exceeds transaction bound' USING ERRCODE = '54000';
        END IF;
    END IF;
    v_position := /* TEMPLATE: schema */allocate_event_position(TG_TABLE_SCHEMA);
    EXECUTE format(
        'UPDATE %I.user_events SET strict_position = $1 WHERE id = $2',
        TG_TABLE_SCHEMA
    ) USING v_position, NEW.id;
    EXECUTE format(
        'INSERT INTO %I.event_owner_stream_state (owner_incarnation, high, floor)
         VALUES ($1, $2, 0)
         ON CONFLICT (owner_incarnation) DO UPDATE
         SET high = GREATEST(event_owner_stream_state.high, EXCLUDED.high),
             updated_at = clock_timestamp()',
        TG_TABLE_SCHEMA
    ) USING NEW.owner_incarnation, v_position;
    EXECUTE format(
        'INSERT INTO %I.user_event_stream_state (user_id, last_event_id, updated_at)
         VALUES ($1, $2, now())
         ON CONFLICT (user_id) DO UPDATE
         SET last_event_id = GREATEST(user_event_stream_state.last_event_id, EXCLUDED.last_event_id),
             updated_at = EXCLUDED.updated_at',
        TG_TABLE_SCHEMA
    ) USING NEW.user_id, NEW.id;
    PERFORM pg_notify('teldrive_events', json_build_object('user_id', NEW.user_id)::text);
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- A row without a strict position has no strict effect. Removing a strict row
-- advances the owning incarnation's floor to the greatest position actually
-- removed in this transaction, including interior removal. Rows removed by a
-- retiring-owner cascade are covered once by that retirement instead.
-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */retire_user_event_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.owner_incarnation IS NULL OR OLD.strict_position IS NULL THEN
        RETURN NULL;
    END IF;
    IF /* TEMPLATE: schema */event_incarnation_is_retired(OLD.owner_incarnation) THEN
        RETURN NULL;
    END IF;
    IF /* TEMPLATE: schema */event_bump_counter('teldrive.events_delete_count') > 512 THEN
        RAISE EXCEPTION 'event delete exceeds transaction bound' USING ERRCODE = '54000';
    END IF;
    PERFORM /* TEMPLATE: schema */hold_event_gate(TG_TABLE_SCHEMA);
    EXECUTE format(
        'INSERT INTO %I.event_owner_stream_state (owner_incarnation, high, floor)
         VALUES ($1, $2, $2)
         ON CONFLICT (owner_incarnation) DO UPDATE
         SET floor = GREATEST(event_owner_stream_state.floor, EXCLUDED.floor),
             updated_at = clock_timestamp()',
        TG_TABLE_SCHEMA
    ) USING OLD.owner_incarnation, OLD.strict_position;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- Mark the exact retiring incarnation transaction-locally before the users
-- cascade starts, so child delete emitters can suppress only that incarnation.
-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */mark_retiring_event_owner()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_incarnation uuid;
BEGIN
    v_incarnation := /* TEMPLATE: schema */event_owner_incarnation(TG_TABLE_SCHEMA, OLD.user_id);
    IF v_incarnation IS NULL THEN
        RETURN OLD;
    END IF;
    PERFORM set_config(
        'teldrive.events_retiring_users',
        COALESCE(NULLIF(current_setting('teldrive.events_retiring_users', true), ''), '')
            || ',' || OLD.user_id::text || ':' || v_incarnation::text || ',',
        true
    );
    PERFORM set_config(
        'teldrive.events_retired',
        COALESCE(NULLIF(current_setting('teldrive.events_retired', true), ''), '')
            || ',' || v_incarnation::text || ',',
        true
    );
    RETURN OLD;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */retire_event_owner_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_incarnation uuid;
BEGIN
    v_incarnation := /* TEMPLATE: schema */event_retiring_incarnation(OLD.user_id);
    IF v_incarnation IS NULL THEN
        RETURN NULL;
    END IF;
    IF /* TEMPLATE: schema */event_bump_counter('teldrive.events_delete_count') > 512 THEN
        RAISE EXCEPTION 'event delete exceeds transaction bound' USING ERRCODE = '54000';
    END IF;
    PERFORM /* TEMPLATE: schema */hold_event_gate(TG_TABLE_SCHEMA);
    EXECUTE format(
        'DELETE FROM %I.event_owner_stream_state WHERE owner_incarnation = $1',
        TG_TABLE_SCHEMA
    ) USING v_incarnation;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */create_event_owner_incarnation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM /* TEMPLATE: schema */ensure_event_owner_incarnation(TG_TABLE_SCHEMA, NEW.user_id);
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */preserve_event_owner_incarnation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.user_id IS DISTINCT FROM OLD.user_id
       OR NEW.incarnation IS DISTINCT FROM OLD.incarnation THEN
        RAISE EXCEPTION 'event owner incarnation is immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */preserve_event_stream_allocator()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'event stream allocator cannot be removed';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id OR NEW.last_position < OLD.last_position THEN
        RAISE EXCEPTION 'event stream allocator is monotone';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER event_owner_incarnations_immutable
BEFORE UPDATE ON /* TEMPLATE: schema */event_owner_incarnations
FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_event_owner_incarnation();

CREATE TRIGGER event_stream_allocator_guard
BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */event_stream_allocator
FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_event_stream_allocator();

CREATE TRIGGER users_event_incarnation
AFTER INSERT ON /* TEMPLATE: schema */users
FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */create_event_owner_incarnation();

CREATE TRIGGER users_retire_event_owner
BEFORE DELETE ON /* TEMPLATE: schema */users
FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */mark_retiring_event_owner();

CREATE CONSTRAINT TRIGGER users_retire_event_owner_state
AFTER DELETE ON /* TEMPLATE: schema */users
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */retire_event_owner_state();

CREATE TRIGGER user_events_prepare
BEFORE INSERT ON /* TEMPLATE: schema */user_events
FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */prepare_user_event();

CREATE TRIGGER user_events_delete_budget
BEFORE DELETE ON /* TEMPLATE: schema */user_events
FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */budget_user_event_delete();

DROP TRIGGER user_events_notify_after_insert ON /* TEMPLATE: schema */user_events;
DROP FUNCTION /* TEMPLATE: schema */notify_user_event();

CREATE CONSTRAINT TRIGGER user_events_notify_after_insert
AFTER INSERT ON /* TEMPLATE: schema */user_events
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */append_user_event();

CREATE CONSTRAINT TRIGGER user_events_ordered_retire
AFTER DELETE ON /* TEMPLATE: schema */user_events
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */retire_user_event_row();

-- File, share and channel delete emitters keep their ordinary behavior but
-- suppress only events for the incarnation that is retiring in this transaction.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION /* TEMPLATE: schema */emit_file_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    row_value RECORD;
    event_name TEXT;
    location_value JSONB;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF /* TEMPLATE: schema */event_owner_is_retiring(TG_TABLE_SCHEMA, OLD.user_id) THEN
            RETURN NULL;
        END IF;
        row_value := OLD;
        event_name := 'file.purged';
        location_value := jsonb_build_object(
            'old', jsonb_build_object('parentId', OLD.parent_id, 'name', OLD.name, 'kind', OLD.kind)
        );
    ELSIF TG_OP = 'INSERT' THEN
        row_value := NEW;
        event_name := 'file.created';
        location_value := jsonb_build_object(
            'new', jsonb_build_object('parentId', NEW.parent_id, 'name', NEW.name, 'kind', NEW.kind)
        );
    ELSE
        row_value := NEW;
        IF ROW(
            OLD.parent_id, OLD.name, OLD.kind, OLD.mime_type, OLD.size,
            OLD.hash_algorithm, OLD.hash_value, OLD.encryption,
            OLD.encryption_key_version, OLD.status, OLD.mod_time, OLD.generation,
            OLD.deleted_at
        ) IS NOT DISTINCT FROM ROW(
            NEW.parent_id, NEW.name, NEW.kind, NEW.mime_type, NEW.size,
            NEW.hash_algorithm, NEW.hash_value, NEW.encryption,
            NEW.encryption_key_version, NEW.status, NEW.mod_time, NEW.generation,
            NEW.deleted_at
        ) THEN
            RETURN NEW;
        END IF;

        event_name := CASE
            WHEN OLD.status IS DISTINCT FROM NEW.status AND NEW.status = 'trashed' THEN 'file.trashed'
            WHEN OLD.status IS DISTINCT FROM NEW.status AND NEW.status = 'deletion_pending' THEN 'file.deletion_pending'
            WHEN OLD.status IS DISTINCT FROM NEW.status AND NEW.status = 'active' THEN 'file.restored'
            ELSE 'file.updated'
        END;
        location_value := jsonb_build_object(
            'old', jsonb_build_object('parentId', OLD.parent_id, 'name', OLD.name, 'kind', OLD.kind),
            'new', jsonb_build_object('parentId', NEW.parent_id, 'name', NEW.name, 'kind', NEW.kind)
        );
    END IF;

    EXECUTE format(
        'INSERT INTO %I.user_events (
             user_id, event_type, resource_type, resource_id, generation, payload, location
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)',
        TG_TABLE_SCHEMA
    ) USING
        row_value.user_id,
        event_name,
        'file',
        row_value.id::text,
        row_value.generation,
        jsonb_strip_nulls(jsonb_build_object(
            'parentId', row_value.parent_id,
            'name', row_value.name,
            'kind', row_value.kind,
            'status', row_value.status
        )),
        location_value;

    RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION /* TEMPLATE: schema */emit_share_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    row_value RECORD;
    event_name TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF /* TEMPLATE: schema */event_owner_is_retiring(TG_TABLE_SCHEMA, OLD.owner_id) THEN
            RETURN NULL;
        END IF;
        row_value := OLD;
        event_name := 'share.deleted';
    ELSIF TG_OP = 'INSERT' THEN
        row_value := NEW;
        event_name := 'share.created';
    ELSE
        row_value := NEW;
        IF ROW(OLD.password_hash, OLD.expires_at, OLD.max_downloads, OLD.revoked_at)
            IS NOT DISTINCT FROM
           ROW(NEW.password_hash, NEW.expires_at, NEW.max_downloads, NEW.revoked_at) THEN
            RETURN NEW;
        END IF;
        event_name := CASE
            WHEN OLD.revoked_at IS NULL AND NEW.revoked_at IS NOT NULL THEN 'share.revoked'
            ELSE 'share.updated'
        END;
    END IF;

    EXECUTE format(
        'INSERT INTO %I.user_events (
             user_id, event_type, resource_type, resource_id, payload
         ) VALUES ($1, $2, $3, $4, $5)',
        TG_TABLE_SCHEMA
    ) USING
        row_value.owner_id,
        event_name,
        'share',
        row_value.id::text,
        jsonb_strip_nulls(jsonb_build_object(
            'fileId', row_value.file_id,
            'expiresAt', row_value.expires_at,
            'revokedAt', row_value.revoked_at
        ));

    RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION /* TEMPLATE: schema */emit_channel_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    row_value RECORD;
    event_name TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF /* TEMPLATE: schema */event_owner_is_retiring(TG_TABLE_SCHEMA, OLD.user_id) THEN
            RETURN NULL;
        END IF;
        row_value := OLD;
        event_name := 'channel.deleted';
    ELSIF TG_OP = 'INSERT' THEN
        row_value := NEW;
        event_name := 'channel.created';
    ELSE
        row_value := NEW;
        IF ROW(OLD.name, OLD.selected, OLD.health)
            IS NOT DISTINCT FROM
           ROW(NEW.name, NEW.selected, NEW.health) THEN
            RETURN NEW;
        END IF;
        event_name := 'channel.updated';
    END IF;

    EXECUTE format(
        'INSERT INTO %I.user_events (
             user_id, event_type, resource_type, resource_id, payload
         ) VALUES ($1, $2, $3, $4, $5)',
        TG_TABLE_SCHEMA
    ) USING
        row_value.user_id,
        event_name,
        'channel',
        row_value.channel_id::text,
        jsonb_build_object(
            'name', row_value.name,
            'selected', row_value.selected,
            'health', row_value.health
        );

    RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- Another forward migration must recover this schema. Dropping it would discard
-- committed strict positions, floors, the stream epoch and owner incarnations.
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION '000016_ordered_events is forward-only; recover forward instead of dropping ordered event state';
END $$;
-- +goose StatementEnd
