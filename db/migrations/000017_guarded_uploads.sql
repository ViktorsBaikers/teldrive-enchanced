-- +goose Up

-- Guarded publication is forward-only. A guarded session can be created only by
-- an admitted request, and completion can succeed only with the commit-time
-- result evidence produced by that same transaction.
ALTER TABLE /* TEMPLATE: schema */upload_sessions
    ADD COLUMN guarded BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN guard_conflict_code TEXT
        CHECK (guard_conflict_code ~ '^[a-z][a-z0-9_]{0,63}$');

-- The current recovery incarnation is authority for guarded mutations. A
-- restore rotates this row before serving; bindings keep the admitted value as
-- provenance and never become mutation authority after rotation.
CREATE TABLE /* TEMPLATE: schema */recovery_incarnations (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    incarnation UUID NOT NULL DEFAULT gen_random_uuid() CHECK (incarnation <> '00000000-0000-0000-0000-000000000000'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO /* TEMPLATE: schema */recovery_incarnations (singleton)
VALUES (TRUE);

-- One immutable destination guard per guarded upload. The expected slot is the
-- authoritative write-open baseline: NULL expected_file_id means the session was
-- admitted against an empty destination; otherwise the named file and generation
-- must still be the active slot at publication.
CREATE TABLE /* TEMPLATE: schema */guarded_upload_bindings (
    upload_id UUID PRIMARY KEY REFERENCES /* TEMPLATE: schema */upload_sessions(id) ON DELETE RESTRICT,
    user_id BIGINT NOT NULL REFERENCES /* TEMPLATE: schema */users(user_id) ON DELETE RESTRICT,
    request_admission_id UUID NOT NULL REFERENCES /* TEMPLATE: schema */request_admissions(id) ON DELETE RESTRICT,
    request_key UUID NOT NULL CHECK (request_key <> '00000000-0000-0000-0000-000000000000'),
    recovery_incarnation UUID NOT NULL CHECK (recovery_incarnation <> '00000000-0000-0000-0000-000000000000'),
    mount_root UUID CHECK (mount_root <> '00000000-0000-0000-0000-000000000000'),
    parent_id UUID CHECK (parent_id <> '00000000-0000-0000-0000-000000000000'),
    name TEXT NOT NULL CHECK (length(btrim(name)) > 0),
    expected_file_id UUID CHECK (expected_file_id <> '00000000-0000-0000-0000-000000000000'),
    expected_generation BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((expected_file_id IS NULL) = (expected_generation IS NULL)),
    CHECK (expected_generation IS NULL OR expected_generation > 0)
);
CREATE UNIQUE INDEX guarded_upload_bindings_admission_idx
    ON /* TEMPLATE: schema */guarded_upload_bindings(request_admission_id);

-- The result is the immutable publication receipt. It records what was compared,
-- what was published and the commit-time location proven under the owner fence.
CREATE TABLE /* TEMPLATE: schema */guarded_upload_results (
    upload_id UUID PRIMARY KEY REFERENCES /* TEMPLATE: schema */upload_sessions(id) ON DELETE RESTRICT,
    user_id BIGINT NOT NULL,
    request_admission_id UUID NOT NULL REFERENCES /* TEMPLATE: schema */request_admissions(id) ON DELETE RESTRICT,
    size BIGINT NOT NULL CHECK (size >= 0),
    hash_algorithm TEXT NOT NULL CHECK (hash_algorithm = 'blake3'),
    hash_value TEXT NOT NULL CHECK (hash_value ~ '^[0-9a-f]{64}$'),
    file_id UUID NOT NULL CHECK (file_id <> '00000000-0000-0000-0000-000000000000'),
    generation BIGINT NOT NULL CHECK (generation > 0),
    parent_id UUID CHECK (parent_id <> '00000000-0000-0000-0000-000000000000'),
    name TEXT NOT NULL CHECK (length(btrim(name)) > 0),
    mount_root UUID CHECK (mount_root <> '00000000-0000-0000-0000-000000000000'),
    parent_segments TEXT[] NOT NULL,
    expected_file_id UUID CHECK (expected_file_id <> '00000000-0000-0000-0000-000000000000'),
    expected_generation BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (file_id),
    CHECK ((expected_file_id IS NULL) = (expected_generation IS NULL)),
    CHECK (expected_generation IS NULL OR expected_generation > 0)
);

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */validate_guarded_upload_binding() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_guarded boolean;
    v_user_id bigint;
    v_parent_id uuid;
    v_name text;
    v_state text;
    v_admission_upload uuid;
BEGIN
    EXECUTE format(
        'SELECT guarded,user_id,parent_id,name,state FROM %I.upload_sessions WHERE id=$1',
        TG_TABLE_SCHEMA
    ) INTO v_guarded, v_user_id, v_parent_id, v_name, v_state USING NEW.upload_id;
    IF v_guarded IS NULL OR NOT v_guarded OR v_user_id <> NEW.user_id
        OR v_parent_id IS DISTINCT FROM NEW.parent_id OR v_name <> NEW.name
        OR v_state <> 'open' THEN
        RAISE EXCEPTION 'guarded binding does not match its admitted session';
    END IF;
    EXECUTE format(
        'SELECT upload_id FROM %I.request_admissions WHERE id=$1',
        TG_TABLE_SCHEMA
    ) INTO v_admission_upload USING NEW.request_admission_id;
    IF v_admission_upload IS DISTINCT FROM NEW.upload_id THEN
        RAISE EXCEPTION 'guarded binding requires the selected upload admission';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guarded_upload_bindings_insert
    BEFORE INSERT ON /* TEMPLATE: schema */guarded_upload_bindings
    FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */validate_guarded_upload_binding();

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */preserve_guarded_evidence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'guarded_upload_bindings' THEN
        IF TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'immutable guarded upload binding';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        RAISE EXCEPTION 'immutable guarded upload result';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guarded_upload_bindings_evidence
    BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */guarded_upload_bindings
    FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_guarded_evidence();
CREATE TRIGGER guarded_upload_results_evidence
    BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */guarded_upload_results
    FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_guarded_evidence();

-- A result may be inserted only after the file row exists at the exact compared
-- slot. This is the commit-time comparison latch; inserting history later or
-- publishing an uncompared or differently located row fails closed.
-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */validate_guarded_upload_result() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_guarded boolean;
    v_user_id bigint;
    v_parent_id uuid;
    v_name text;
    v_state text;
    v_session_file uuid;
    v_binding record;
    v_status text;
    v_file_parent uuid;
    v_file_name text;
    v_generation bigint;
    v_expected_status text;
    v_expected_generation bigint;
    v_conflict bigint;
    v_admission_upload uuid;
BEGIN
    EXECUTE format(
        'SELECT guarded,user_id,parent_id,name,state,file_id FROM %I.upload_sessions WHERE id=$1',
        TG_TABLE_SCHEMA
    ) INTO v_guarded, v_user_id, v_parent_id, v_name, v_state, v_session_file
      USING NEW.upload_id;
    IF v_guarded IS NULL OR NOT v_guarded OR v_state <> 'completing'
        OR v_user_id <> NEW.user_id
        OR v_parent_id IS DISTINCT FROM NEW.parent_id OR v_name <> NEW.name THEN
        RAISE EXCEPTION 'guarded result requires the completing guarded session';
    END IF;
    EXECUTE format(
        'SELECT request_admission_id,recovery_incarnation,mount_root,parent_id,name,expected_file_id,expected_generation
         FROM %I.guarded_upload_bindings WHERE upload_id=$1 AND user_id=$2',
        TG_TABLE_SCHEMA
    ) INTO v_binding USING NEW.upload_id, NEW.user_id;
    IF v_binding.request_admission_id IS NULL THEN
        RAISE EXCEPTION 'guarded result requires an admitted binding';
    END IF;
    EXECUTE format(
        'SELECT upload_id FROM %I.request_admissions WHERE id=$1',
        TG_TABLE_SCHEMA
    ) INTO v_admission_upload USING NEW.request_admission_id;
    IF v_admission_upload IS DISTINCT FROM NEW.upload_id THEN
        RAISE EXCEPTION 'guarded result requires the selected complete admission';
    END IF;
    IF v_binding.parent_id IS DISTINCT FROM NEW.parent_id
        OR v_binding.name <> NEW.name
        OR v_binding.mount_root IS DISTINCT FROM NEW.mount_root
        OR v_binding.expected_file_id IS DISTINCT FROM NEW.expected_file_id
        OR v_binding.expected_generation IS DISTINCT FROM NEW.expected_generation THEN
        RAISE EXCEPTION 'guarded result changed the admitted slot';
    END IF;
    EXECUTE format(
        'SELECT status,parent_id,name,generation FROM %I.files WHERE id=$1 AND user_id=$2',
        TG_TABLE_SCHEMA
    ) INTO v_status, v_file_parent, v_file_name, v_generation USING NEW.file_id, NEW.user_id;
    IF v_status IS DISTINCT FROM 'active' OR v_file_parent IS DISTINCT FROM NEW.parent_id
        OR v_file_name IS DISTINCT FROM NEW.name OR v_generation IS DISTINCT FROM NEW.generation THEN
        RAISE EXCEPTION 'guarded result does not identify the published slot';
    END IF;
    IF NEW.expected_file_id IS NULL THEN
        EXECUTE format(
            'SELECT count(*) FROM %I.files WHERE user_id=$1 AND parent_id IS NOT DISTINCT FROM $2
             AND name=$3 AND status=''active'' AND id<>$4',
            TG_TABLE_SCHEMA
        ) INTO v_conflict USING NEW.user_id, NEW.parent_id, NEW.name, NEW.file_id;
        IF v_conflict <> 0 THEN
            RAISE EXCEPTION 'guarded result published into an occupied slot';
        END IF;
    ELSE
        EXECUTE format(
            'SELECT status,generation FROM %I.files WHERE id=$1 AND user_id=$2',
            TG_TABLE_SCHEMA
        ) INTO v_expected_status, v_expected_generation USING NEW.expected_file_id, NEW.user_id;
        IF v_expected_status IS NULL OR v_expected_status = 'active'
            OR v_expected_generation <> NEW.expected_generation + 1 THEN
            RAISE EXCEPTION 'guarded result did not replace the compared generation';
        END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guarded_upload_results_insert
    BEFORE INSERT ON /* TEMPLATE: schema */guarded_upload_results
    FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */validate_guarded_upload_result();

-- A guarded session keeps its admission, cannot lose the guard and cannot reach
-- completion without the result written by the same transaction.
-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */enforce_guarded_upload_completion() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_result bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.guarded THEN
            RAISE EXCEPTION 'guarded upload evidence cannot be deleted';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.guarded AND NOT OLD.guarded THEN
        RAISE EXCEPTION 'guarded admission cannot be added after creation';
    END IF;
    IF OLD.guarded THEN
        IF NOT NEW.guarded THEN
            RAISE EXCEPTION 'guarded admission cannot be removed';
        END IF;
        IF NEW.parent_id IS DISTINCT FROM OLD.parent_id OR NEW.name IS DISTINCT FROM OLD.name THEN
            RAISE EXCEPTION 'guarded destination is immutable';
        END IF;
        IF OLD.guard_conflict_code IS NOT NULL
            AND NEW.guard_conflict_code IS DISTINCT FROM OLD.guard_conflict_code THEN
            RAISE EXCEPTION 'guarded conflict is sticky';
        END IF;
        IF OLD.state = 'completed' AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'completed guarded evidence is immutable';
        END IF;
        IF NEW.state = 'completed' AND OLD.state <> 'completed' THEN
            IF NEW.guard_conflict_code IS NOT NULL THEN
                RAISE EXCEPTION 'held guarded upload cannot complete';
            END IF;
            EXECUTE format(
                'SELECT count(*) FROM %I.guarded_upload_results r
                 JOIN %I.guarded_upload_bindings b ON b.upload_id=r.upload_id
                 WHERE r.upload_id=$1 AND r.user_id=$2 AND r.file_id=$3
                   AND b.parent_id IS NOT DISTINCT FROM r.parent_id
                   AND b.name=r.name AND b.mount_root IS NOT DISTINCT FROM r.mount_root',
                TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
            ) INTO v_result USING NEW.id, NEW.user_id, NEW.file_id;
            IF v_result <> 1 THEN
                RAISE EXCEPTION 'guarded completion requires its comparison result';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guarded_upload_completion
    BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */upload_sessions
    FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */enforce_guarded_upload_completion();

-- +goose Down
-- Dropping guarded evidence would discard the admission, compared slot and
-- publication receipt. Recover forward with a new migration instead.
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION '000017_guarded_uploads is forward-only; recover forward instead of dropping guarded upload evidence';
END $$;
-- +goose StatementEnd
