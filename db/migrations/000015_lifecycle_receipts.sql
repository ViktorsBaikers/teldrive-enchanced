-- +goose Up

ALTER TABLE /* TEMPLATE: schema */upload_sessions
    ADD COLUMN terminal_failure_code TEXT CHECK (terminal_failure_code ~ '^[a-z][a-z0-9_]{0,63}$');

-- NULL in the old schema proves neither explicit root nor a former destination.
-- Leave state, timestamps, parts and the installed event triggers unchanged.
UPDATE /* TEMPLATE: schema */upload_sessions
SET terminal_failure_code = 'upload_destination_unknown'
WHERE parent_id IS NULL AND state <> 'completed';

CREATE TABLE /* TEMPLATE: schema */remote_write_receipts (
    id UUID PRIMARY KEY CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    row_kind TEXT NOT NULL CHECK (row_kind IN ('message_attempt', 'copy_operation')),
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    channel_id BIGINT CHECK (channel_id <> 0),
    message_id BIGINT CHECK (message_id > 0),
    origin_kind TEXT NOT NULL CHECK (origin_kind IN ('upload', 'copy')),
    upload_id UUID CHECK (upload_id <> '00000000-0000-0000-0000-000000000000'),
    part_no INTEGER CHECK (part_no > 0),
    lease_token UUID NOT NULL CHECK (lease_token <> '00000000-0000-0000-0000-000000000000'),
    operation_id UUID CHECK (operation_id <> '00000000-0000-0000-0000-000000000000'),
    result_file_id UUID CHECK (result_file_id <> '00000000-0000-0000-0000-000000000000'),
    destination_parent_id UUID CHECK (destination_parent_id <> '00000000-0000-0000-0000-000000000000'),
    destination_is_root BOOLEAN,
    terminal_failure_code TEXT CHECK (terminal_failure_code ~ '^[a-z][a-z0-9_]{0,63}$'),
    terminal_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    origin_part_created_at TIMESTAMPTZ,
    origin_part_updated_at TIMESTAMPTZ,
    state TEXT NOT NULL DEFAULT 'reserved',
    lease_expires_at TIMESTAMPTZ NOT NULL CHECK (isfinite(lease_expires_at)),
    next_attempt_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 4),
    primary_code TEXT CHECK (primary_code ~ '^[a-z][a-z0-9_]{0,63}$'),
    cleanup_code TEXT CHECK (cleanup_code ~ '^[a-z][a-z0-9_]{0,63}$'),
    job_id BIGINT CHECK (job_id > 0),
    wakeup_job_id BIGINT CHECK (wakeup_job_id > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((terminal_at IS NULL) = (terminal_failure_code IS NULL)),
    CHECK (terminal_at IS NULL OR published_at IS NULL),
    CHECK ((origin_part_created_at IS NULL) = (origin_part_updated_at IS NULL)),
    CHECK (
        (row_kind = 'copy_operation' AND origin_kind = 'copy' AND operation_id IS NOT NULL AND operation_id = id
         AND result_file_id IS NOT NULL AND upload_id IS NULL AND part_no IS NULL
         AND channel_id IS NULL AND message_id IS NULL AND attempts = 0
         AND destination_is_root IS NOT NULL
         AND destination_is_root = (destination_parent_id IS NULL)
         AND state IN ('reserved','bound','unresolved')
         AND origin_part_created_at IS NULL
         AND ((published_at IS NOT NULL) = (state = 'bound')))
        OR
        (row_kind = 'message_attempt' AND channel_id IS NOT NULL AND part_no IS NOT NULL
         AND destination_parent_id IS NULL AND destination_is_root IS NULL
         AND terminal_at IS NULL AND published_at IS NULL
         AND state IN ('reserved','observed','bound','cleanup_pending','deleting','deleted','exhausted','unresolved')
         AND (state IN ('reserved','unresolved') OR message_id IS NOT NULL)
         AND ((origin_kind = 'upload' AND upload_id IS NOT NULL AND operation_id IS NULL)
              OR (origin_kind = 'copy' AND upload_id IS NULL AND operation_id IS NOT NULL AND result_file_id IS NOT NULL)))
    )
);
CREATE UNIQUE INDEX remote_write_receipts_physical_idx
    ON /* TEMPLATE: schema */remote_write_receipts(channel_id,message_id)
    WHERE row_kind = 'message_attempt' AND message_id IS NOT NULL;
CREATE UNIQUE INDEX remote_write_receipts_upload_attempt_idx
    ON /* TEMPLATE: schema */remote_write_receipts(upload_id,part_no,lease_token)
    WHERE origin_kind = 'upload';
CREATE UNIQUE INDEX remote_write_receipts_copy_attempt_idx
    ON /* TEMPLATE: schema */remote_write_receipts(operation_id,result_file_id,part_no)
    WHERE row_kind = 'message_attempt' AND origin_kind = 'copy';
CREATE INDEX remote_write_receipts_discovery_idx ON /* TEMPLATE: schema */remote_write_receipts(created_at,id);
CREATE INDEX remote_write_receipts_destination_idx ON /* TEMPLATE: schema */remote_write_receipts(user_id,destination_parent_id,id)
    WHERE row_kind = 'copy_operation' AND published_at IS NULL;
CREATE INDEX remote_write_receipts_operation_idx ON /* TEMPLATE: schema */remote_write_receipts(operation_id,id);
CREATE INDEX remote_write_receipts_owner_job_idx ON /* TEMPLATE: schema */remote_write_receipts(user_id,job_id DESC) WHERE job_id IS NOT NULL;

CREATE TABLE /* TEMPLATE: schema */request_admissions (
    id UUID PRIMARY KEY CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    request_kind TEXT NOT NULL CHECK (request_kind IN ('upload','copy')),
    request_key UUID NOT NULL CHECK (request_key <> '00000000-0000-0000-0000-000000000000'),
    actor_user_id BIGINT CHECK (actor_user_id > 0),
    public_share_id UUID CHECK (public_share_id <> '00000000-0000-0000-0000-000000000000'),
    request_fingerprint BYTEA NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    upload_id UUID CHECK (upload_id <> '00000000-0000-0000-0000-000000000000'),
    copy_operation_id UUID CHECK (copy_operation_id <> '00000000-0000-0000-0000-000000000000'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((actor_user_id IS NOT NULL)::int + (public_share_id IS NOT NULL)::int = 1),
    CHECK ((request_kind = 'upload' AND upload_id IS NOT NULL AND copy_operation_id IS NULL)
        OR (request_kind = 'copy' AND upload_id IS NULL AND copy_operation_id IS NOT NULL))
);
CREATE UNIQUE INDEX request_admissions_actor_idx ON /* TEMPLATE: schema */request_admissions(actor_user_id,request_kind,request_key) WHERE actor_user_id IS NOT NULL;
CREATE UNIQUE INDEX request_admissions_share_idx ON /* TEMPLATE: schema */request_admissions(public_share_id,request_kind,request_key) WHERE public_share_id IS NOT NULL;

CREATE TABLE /* TEMPLATE: schema */import_batches (
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    batch_id UUID NOT NULL CHECK (batch_id <> '00000000-0000-0000-0000-000000000000'),
    origin_job_id BIGINT NOT NULL UNIQUE CHECK (origin_job_id > 0),
    request_fingerprint BYTEA NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    parent_id UUID CHECK (parent_id <> '00000000-0000-0000-0000-000000000000'),
    destination_is_root BOOLEAN,
    source_count INTEGER CHECK (source_count >= 0),
    dispatched BOOLEAN NOT NULL DEFAULT FALSE,
    terminal_failure_code TEXT CHECK (terminal_failure_code ~ '^[a-z][a-z0-9_]{0,63}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (user_id,batch_id),
    CHECK ((NOT dispatched AND parent_id IS NULL AND destination_is_root IS NULL AND source_count IS NULL)
        OR (dispatched AND source_count IS NOT NULL AND destination_is_root IS NOT NULL AND destination_is_root = (parent_id IS NULL)))
);
CREATE TABLE /* TEMPLATE: schema */import_destination_pins (
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    batch_id UUID NOT NULL CHECK (batch_id <> '00000000-0000-0000-0000-000000000000'),
    destination_ordinal INTEGER NOT NULL CHECK (destination_ordinal >= -1),
    anchor_id UUID CHECK (anchor_id <> '00000000-0000-0000-0000-000000000000'),
    anchor_is_root BOOLEAN NOT NULL,
    spec_fingerprint BYTEA NOT NULL CHECK (octet_length(spec_fingerprint) = 32),
    remainder_fingerprint BYTEA NOT NULL CHECK (octet_length(remainder_fingerprint) = 32),
    remainder_segments INTEGER NOT NULL CHECK (remainder_segments >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (user_id,batch_id,destination_ordinal),
    FOREIGN KEY (user_id,batch_id) REFERENCES /* TEMPLATE: schema */import_batches(user_id,batch_id),
    CHECK (anchor_is_root = (anchor_id IS NULL))
);
CREATE INDEX import_destination_pins_anchor_idx ON /* TEMPLATE: schema */import_destination_pins(user_id,anchor_id) WHERE anchor_id IS NOT NULL;
CREATE TABLE /* TEMPLATE: schema */import_job_attempts (
    attempt_id UUID PRIMARY KEY CHECK (attempt_id <> '00000000-0000-0000-0000-000000000000'),
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    batch_id UUID NOT NULL,
    source_index INTEGER NOT NULL CHECK (source_index >= 0),
    job_id BIGINT NOT NULL UNIQUE CHECK (job_id > 0),
    parent_id UUID CHECK (parent_id <> '00000000-0000-0000-0000-000000000000'),
    upload_id UUID UNIQUE CHECK (upload_id <> '00000000-0000-0000-0000-000000000000'),
    selected_create JSONB,
    session_materialized BOOLEAN NOT NULL DEFAULT FALSE,
    result_file_id UUID CHECK (result_file_id <> '00000000-0000-0000-0000-000000000000'),
    terminal_failure_code TEXT CHECK (terminal_failure_code ~ '^[a-z][a-z0-9_]{0,63}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (user_id,batch_id,source_index),
    FOREIGN KEY (user_id,batch_id) REFERENCES /* TEMPLATE: schema */import_batches(user_id,batch_id),
    CHECK (upload_id IS NULL OR result_file_id IS NULL),
    CHECK ((selected_create IS NULL) = (upload_id IS NULL)),
    CHECK (selected_create IS NULL OR (
        jsonb_typeof(selected_create) = 'object'
        AND selected_create->'version' = '1'::jsonb
        AND selected_create ?& ARRAY['version','name','expectedSize','expectedHashAlgorithm','expectedHashValue','mimeType','modTime','encryption','encryptionKeyVersion','conflictPolicy','partSize']
        AND selected_create - ARRAY['version','name','expectedSize','expectedHashAlgorithm','expectedHashValue','mimeType','modTime','encryption','encryptionKeyVersion','conflictPolicy','partSize'] = '{}'::jsonb
    )),
    CHECK (NOT session_materialized OR upload_id IS NOT NULL)
);
CREATE INDEX import_job_attempts_parent_idx ON /* TEMPLATE: schema */import_job_attempts(user_id,parent_id,attempt_id);

CREATE TABLE /* TEMPLATE: schema */channel_deletion_intents (
    channel_id BIGINT PRIMARY KEY CHECK (channel_id <> 0),
    intent_id UUID NOT NULL UNIQUE CHECK (intent_id <> '00000000-0000-0000-0000-000000000000'),
    requested_by BIGINT NOT NULL CHECK (requested_by > 0),
    state TEXT NOT NULL DEFAULT 'closing' CHECK (state IN ('closing','deleted')),
    failure_code TEXT CHECK (failure_code ~ '^[a-z][a-z0-9_]{0,63}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- One physical-reference and origin policy serves staging, discovery and claims.
CREATE VIEW /* TEMPLATE: schema */lifecycle_receipt_status AS
SELECT r.id,
    COALESCE(
      (r.result_file_id IS NOT NULL AND EXISTS (
          SELECT 1 FROM /* TEMPLATE: schema */files f
          JOIN /* TEMPLATE: schema */file_parts fp ON fp.file_id = f.id
          WHERE f.id = r.result_file_id AND f.user_id = r.user_id AND f.status = 'deletion_pending'
            AND fp.channel_id = r.channel_id AND fp.message_id = r.message_id AND fp.part_no = r.part_no))
      OR (r.origin_kind = 'copy' AND op.published_at IS NULL
          AND (op.terminal_at IS NOT NULL OR op.lease_expires_at <= clock_timestamp()))
      OR (r.origin_kind = 'upload'
          AND us.terminal_failure_code IS DISTINCT FROM 'upload_destination_unknown'
          AND (us.id IS NULL OR us.state IN ('aborted','expired')
            OR (us.state <> 'completed' AND (
                us.terminal_failure_code IS NOT NULL OR us.expires_at <= clock_timestamp()
                OR up.state = 'failed'
                OR (up.lease_token IS DISTINCT FROM r.lease_token
                    AND r.lease_expires_at <= clock_timestamp())
                OR (up.lease_token = r.lease_token AND up.lease_expires_at <= clock_timestamp())
                OR (up.upload_id IS NULL AND r.lease_expires_at <= clock_timestamp()))))),
      FALSE) AS origin_eligible,
    (EXISTS (
        SELECT 1 FROM /* TEMPLATE: schema */file_parts fp
        JOIN /* TEMPLATE: schema */files f ON f.id = fp.file_id
        WHERE fp.channel_id = r.channel_id AND fp.message_id = r.message_id
          AND NOT (f.id IS NOT DISTINCT FROM r.result_file_id AND fp.part_no = r.part_no
                   AND f.user_id = r.user_id AND f.status = 'deletion_pending'))
     OR EXISTS (
        SELECT 1 FROM /* TEMPLATE: schema */upload_parts p
        JOIN /* TEMPLATE: schema */upload_sessions s ON s.id = p.upload_id
        WHERE p.channel_id = r.channel_id AND p.message_id = r.message_id
          AND NOT (r.origin_kind = 'upload' AND p.upload_id = r.upload_id
                   AND p.part_no = r.part_no AND s.user_id = r.user_id
                   AND s.state IN ('aborted','expired')
                   AND s.terminal_failure_code IS DISTINCT FROM 'upload_destination_unknown'))
    ) AS has_references
FROM /* TEMPLATE: schema */remote_write_receipts r
LEFT JOIN /* TEMPLATE: schema */remote_write_receipts op
    ON op.id = r.operation_id AND op.row_kind = 'copy_operation' AND op.user_id = r.user_id
LEFT JOIN /* TEMPLATE: schema */upload_sessions us
    ON us.id = r.upload_id AND us.user_id = r.user_id
LEFT JOIN /* TEMPLATE: schema */upload_parts up
    ON up.upload_id = r.upload_id AND up.part_no = r.part_no;

-- These rows are evidence, not children of mutable catalog or River rows.
-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */preserve_lifecycle_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'lifecycle evidence cannot be deleted';
    END IF;
    IF TG_TABLE_NAME IN ('request_admissions','import_destination_pins') THEN
        IF NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'immutable lifecycle identity'; END IF;
    ELSIF TG_TABLE_NAME = 'import_batches' THEN
        IF ROW(NEW.user_id,NEW.batch_id,NEW.origin_job_id,NEW.request_fingerprint,NEW.created_at)
            IS DISTINCT FROM ROW(OLD.user_id,OLD.batch_id,OLD.origin_job_id,OLD.request_fingerprint,OLD.created_at)
            OR (OLD.dispatched AND ROW(NEW.dispatched,NEW.parent_id,NEW.destination_is_root,NEW.source_count)
                IS DISTINCT FROM ROW(OLD.dispatched,OLD.parent_id,OLD.destination_is_root,OLD.source_count))
            OR (OLD.terminal_failure_code IS NOT NULL AND NEW.terminal_failure_code IS DISTINCT FROM OLD.terminal_failure_code)
            OR (OLD.terminal_failure_code IS NOT NULL AND ROW(NEW.dispatched,NEW.parent_id,NEW.destination_is_root,NEW.source_count)
                IS DISTINCT FROM ROW(OLD.dispatched,OLD.parent_id,OLD.destination_is_root,OLD.source_count))
        THEN RAISE EXCEPTION 'immutable import admission'; END IF;
    ELSIF TG_TABLE_NAME = 'import_job_attempts' THEN
        IF ROW(NEW.attempt_id,NEW.user_id,NEW.batch_id,NEW.source_index,NEW.job_id,NEW.parent_id,NEW.created_at)
            IS DISTINCT FROM ROW(OLD.attempt_id,OLD.user_id,OLD.batch_id,OLD.source_index,OLD.job_id,OLD.parent_id,OLD.created_at)
            OR (OLD.upload_id IS NOT NULL AND NEW.upload_id IS DISTINCT FROM OLD.upload_id)
            OR (OLD.selected_create IS NOT NULL AND NEW.selected_create IS DISTINCT FROM OLD.selected_create)
            OR (OLD.result_file_id IS NOT NULL AND NEW.result_file_id IS DISTINCT FROM OLD.result_file_id)
            OR (OLD.session_materialized AND NOT NEW.session_materialized)
            OR (OLD.terminal_failure_code IS NOT NULL AND NEW.terminal_failure_code IS DISTINCT FROM OLD.terminal_failure_code)
            OR (OLD.terminal_failure_code IS NOT NULL AND ROW(NEW.upload_id,NEW.selected_create,NEW.result_file_id,NEW.session_materialized)
                IS DISTINCT FROM ROW(OLD.upload_id,OLD.selected_create,OLD.result_file_id,OLD.session_materialized))
        THEN RAISE EXCEPTION 'immutable import attempt'; END IF;
    ELSIF TG_TABLE_NAME = 'channel_deletion_intents' THEN
        IF ROW(NEW.channel_id,NEW.intent_id,NEW.requested_by,NEW.created_at)
            IS DISTINCT FROM ROW(OLD.channel_id,OLD.intent_id,OLD.requested_by,OLD.created_at)
            OR (OLD.state = 'deleted' AND NEW IS DISTINCT FROM OLD)
        THEN RAISE EXCEPTION 'immutable physical channel intent'; END IF;
    ELSE
        IF ROW(NEW.id,NEW.row_kind,NEW.user_id,NEW.channel_id,NEW.origin_kind,NEW.upload_id,NEW.part_no,NEW.lease_token,NEW.operation_id,NEW.destination_parent_id,NEW.destination_is_root,NEW.created_at)
            IS DISTINCT FROM ROW(OLD.id,OLD.row_kind,OLD.user_id,OLD.channel_id,OLD.origin_kind,OLD.upload_id,OLD.part_no,OLD.lease_token,OLD.operation_id,OLD.destination_parent_id,OLD.destination_is_root,OLD.created_at)
            OR (OLD.message_id IS NOT NULL AND NEW.message_id IS DISTINCT FROM OLD.message_id)
            OR (OLD.result_file_id IS NOT NULL AND NEW.result_file_id IS DISTINCT FROM OLD.result_file_id)
            OR (OLD.job_id IS NOT NULL AND NEW.job_id IS DISTINCT FROM OLD.job_id)
            OR NEW.attempts < OLD.attempts OR NEW.attempts > OLD.attempts + 1
            OR (NEW.attempts > OLD.attempts AND (NEW.state <> 'deleting' OR OLD.row_kind <> 'message_attempt'))
            OR (OLD.terminal_at IS NOT NULL AND ROW(NEW.terminal_at,NEW.terminal_failure_code) IS DISTINCT FROM ROW(OLD.terminal_at,OLD.terminal_failure_code))
            OR (OLD.terminal_at IS NOT NULL AND NEW.lease_expires_at > OLD.lease_expires_at)
            OR (OLD.published_at IS NOT NULL AND NEW IS DISTINCT FROM OLD)
            OR (OLD.state IN ('deleted','exhausted') AND NEW.state IS DISTINCT FROM OLD.state)
            OR (OLD.state = 'bound' AND NEW.state NOT IN ('bound','cleanup_pending'))
            OR (OLD.state IN ('cleanup_pending','deleting','unresolved') AND NEW.state IN ('reserved','observed','bound'))
            OR (OLD.origin_part_created_at IS NOT NULL AND ROW(NEW.origin_part_created_at,NEW.origin_part_updated_at) IS DISTINCT FROM ROW(OLD.origin_part_created_at,OLD.origin_part_updated_at))
        THEN RAISE EXCEPTION 'nonmonotone remote receipt'; END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER remote_write_receipts_evidence BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */remote_write_receipts FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_lifecycle_evidence();
CREATE TRIGGER request_admissions_evidence BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */request_admissions FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_lifecycle_evidence();
CREATE TRIGGER import_batches_evidence BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */import_batches FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_lifecycle_evidence();
CREATE TRIGGER import_destination_pins_evidence BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */import_destination_pins FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_lifecycle_evidence();
CREATE TRIGGER import_job_attempts_evidence BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */import_job_attempts FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_lifecycle_evidence();
CREATE TRIGGER channel_deletion_intents_evidence BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */channel_deletion_intents FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_lifecycle_evidence();

-- +goose StatementBegin
CREATE FUNCTION /* TEMPLATE: schema */preserve_upload_terminal_reason() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.terminal_failure_code IS NOT NULL AND
       (TG_OP = 'DELETE' OR NEW.terminal_failure_code IS DISTINCT FROM OLD.terminal_failure_code) THEN
        RAISE EXCEPTION 'upload terminal evidence cannot be removed';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER upload_terminal_evidence BEFORE UPDATE OR DELETE ON /* TEMPLATE: schema */upload_sessions FOR EACH ROW EXECUTE FUNCTION /* TEMPLATE: schema */preserve_upload_terminal_reason();

-- +goose Down
-- Lock first so a concurrent insertion cannot slip between the guard and DROP.
LOCK TABLE /* TEMPLATE: schema */remote_write_receipts, /* TEMPLATE: schema */request_admissions,
    /* TEMPLATE: schema */import_batches, /* TEMPLATE: schema */import_destination_pins,
    /* TEMPLATE: schema */import_job_attempts, /* TEMPLATE: schema */channel_deletion_intents,
    /* TEMPLATE: schema */upload_sessions IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts)
       OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */request_admissions)
       OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */import_batches)
       OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */import_destination_pins)
       OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */import_job_attempts)
       OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */channel_deletion_intents)
       OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */upload_sessions WHERE terminal_failure_code IS NOT NULL)
    THEN RAISE EXCEPTION 'lifecycle evidence requires forward recovery'; END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER upload_terminal_evidence ON /* TEMPLATE: schema */upload_sessions;
DROP FUNCTION /* TEMPLATE: schema */preserve_upload_terminal_reason();
DROP VIEW /* TEMPLATE: schema */lifecycle_receipt_status;
DROP TABLE /* TEMPLATE: schema */channel_deletion_intents, /* TEMPLATE: schema */import_job_attempts,
    /* TEMPLATE: schema */import_destination_pins, /* TEMPLATE: schema */import_batches,
    /* TEMPLATE: schema */request_admissions, /* TEMPLATE: schema */remote_write_receipts;
DROP FUNCTION /* TEMPLATE: schema */preserve_lifecycle_evidence();
ALTER TABLE /* TEMPLATE: schema */upload_sessions DROP COLUMN terminal_failure_code;
