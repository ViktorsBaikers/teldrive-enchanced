-- name: CreateUploadSession :one
INSERT INTO /* TEMPLATE: schema */upload_sessions (
    id,
    user_id,
    parent_id,
    name,
    expected_size,
    expected_hash_algorithm,
    expected_hash_value,
    mime_type,
    mod_time,
    encryption,
    encryption_key_version,
    conflict_policy,
    part_size,
    state,
    guarded,
    expires_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(user_id),
    sqlc.narg(parent_id),
    sqlc.arg(name),
    sqlc.arg(expected_size),
    sqlc.narg(expected_hash_algorithm),
    sqlc.narg(expected_hash_value),
    sqlc.narg(mime_type),
    sqlc.arg(mod_time),
    sqlc.arg(encryption),
    sqlc.narg(encryption_key_version),
    sqlc.arg(conflict_policy),
    sqlc.arg(part_size),
    'open',
    sqlc.arg(guarded),
    sqlc.arg(expires_at)
)
RETURNING *;

-- name: GetUploadSessionForUser :one
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id);


-- name: GetUploadSessionAnyOwner :one
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE id = sqlc.arg(upload_id);

-- name: ListUploadSessions :many
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE user_id = sqlc.arg(user_id)
  AND (sqlc.narg(state)::/* TEMPLATE: schema */upload_state IS NULL OR state = sqlc.narg(state)::/* TEMPLATE: schema */upload_state)
  AND (
    sqlc.narg(after_created_at)::timestamptz IS NULL
    OR (created_at, id) < (
      sqlc.narg(after_created_at)::timestamptz,
      sqlc.narg(after_id)::uuid
    )
  )
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_size);

-- name: FindResumableUploadSessions :many
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE user_id = sqlc.arg(user_id)
  AND parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid
  AND name = sqlc.arg(name)
  AND expected_size = sqlc.arg(expected_size)
  AND encryption = sqlc.arg(encryption)
  AND conflict_policy = 'replace'
  AND ((expires_at > now() AND state = 'open')
       OR terminal_failure_code = 'upload_destination_unknown')
  AND mime_type IS NOT DISTINCT FROM sqlc.narg(mime_type)::text
  AND (
    NOT sqlc.arg(has_mod_time)::boolean
    OR abs(extract(epoch FROM (mod_time - sqlc.arg(mod_time)::timestamptz))) <= 1
  )
ORDER BY created_at DESC, id DESC;

-- name: GetUploadPart :one
SELECT *
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no);

-- name: ClaimUploadPart :one
INSERT INTO /* TEMPLATE: schema */upload_parts AS target (
    upload_id,
    part_no,
    channel_id,
    plain_size,
    checksum,
    state,
    lease_token,
    lease_expires_at
) SELECT
    sqlc.arg(upload_id),
    sqlc.arg(part_no),
    sqlc.arg(channel_id),
    sqlc.arg(plain_size),
    sqlc.narg(checksum),
    'uploading',
    sqlc.arg(lease_token),
    sqlc.arg(lease_expires_at)
WHERE EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */upload_sessions s
    WHERE s.id = sqlc.arg(upload_id) AND s.state = 'open'
      AND s.terminal_failure_code IS NULL AND s.expires_at > clock_timestamp()
)
AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */channel_deletion_intents g
    WHERE g.channel_id = sqlc.arg(channel_id)
)
ON CONFLICT (upload_id, part_no) DO UPDATE
SET channel_id = EXCLUDED.channel_id,
    plain_size = EXCLUDED.plain_size,
    checksum = EXCLUDED.checksum,
    state = 'uploading',
    lease_token = EXCLUDED.lease_token,
    lease_expires_at = EXCLUDED.lease_expires_at,
    last_error_code = NULL,
    updated_at = now()
WHERE target.state <> 'stored'
  AND target.message_id IS NULL
  AND (
    target.lease_expires_at IS NULL
    OR target.lease_expires_at <= clock_timestamp()
  )
  AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */upload_sessions s
    WHERE s.id = target.upload_id AND s.state = 'open'
      AND s.terminal_failure_code IS NULL AND s.expires_at > clock_timestamp()
  )
  AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */channel_deletion_intents g
    WHERE g.channel_id = EXCLUDED.channel_id
  )
RETURNING *;

-- name: MarkUploadPartStored :one
UPDATE /* TEMPLATE: schema */upload_parts
SET message_id = sqlc.arg(message_id),
    stored_size = sqlc.arg(stored_size),
    checksum = sqlc.narg(checksum),
    salt = sqlc.narg(salt),
    block_hashes = sqlc.narg(block_hashes),
    state = 'stored',
    lease_token = NULL,
    lease_expires_at = NULL,
    last_error_code = NULL,
    updated_at = now()
WHERE upload_parts.upload_id = sqlc.arg(upload_id)
  AND upload_parts.part_no = sqlc.arg(part_no)
  AND upload_parts.state = 'uploading'
  AND upload_parts.lease_token = sqlc.arg(lease_token)
  AND upload_parts.lease_expires_at > clock_timestamp()
  AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts r
    WHERE r.id = sqlc.arg(receipt_id)::uuid AND r.row_kind = 'message_attempt'
      AND r.origin_kind = 'upload' AND r.upload_id = sqlc.arg(upload_id)
      AND r.part_no = sqlc.arg(part_no) AND r.lease_token = sqlc.arg(lease_token)
      AND r.channel_id = /* TEMPLATE: schema */upload_parts.channel_id
      AND r.message_id = sqlc.arg(message_id) AND r.state = 'bound'
      AND r.user_id = (SELECT s.user_id FROM /* TEMPLATE: schema */upload_sessions s WHERE s.id = sqlc.arg(upload_id))
  )
  AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */channel_deletion_intents g
    WHERE g.channel_id = /* TEMPLATE: schema */upload_parts.channel_id
  )
  AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */upload_sessions session
    WHERE session.id = sqlc.arg(upload_id)
      AND session.state = 'open'
      AND session.terminal_failure_code IS NULL
      AND session.expires_at > clock_timestamp()
  )
RETURNING *;

-- name: RenewUploadPartLease :one
UPDATE /* TEMPLATE: schema */upload_parts
SET lease_expires_at = sqlc.arg(lease_expires_at),
    updated_at = now()
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no)
  AND state = 'uploading'
  AND lease_token = sqlc.arg(lease_token)
  AND lease_expires_at > clock_timestamp()
  AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */upload_sessions session
    WHERE session.id = sqlc.arg(upload_id)
      AND session.state = 'open'
      AND session.terminal_failure_code IS NULL
      AND session.expires_at > clock_timestamp()
  )
RETURNING part_no, plain_size;

-- name: MarkUploadPartFailed :one
UPDATE /* TEMPLATE: schema */upload_parts
SET state = 'failed',
    lease_token = NULL,
    lease_expires_at = NULL,
    last_error_code = sqlc.arg(error_code),
    updated_at = now()
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no)
  AND lease_token = sqlc.arg(lease_token)
  AND state = 'uploading'
  AND lease_expires_at > clock_timestamp()
  AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */upload_sessions session
    WHERE session.id = sqlc.arg(upload_id)
      AND session.state = 'open'
      AND session.terminal_failure_code IS NULL
      AND session.expires_at > clock_timestamp()
  )
RETURNING *;

-- name: ListUploadParts :many
SELECT *
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id)
  AND (
    sqlc.narg(after_part_no)::integer IS NULL
    OR part_no > sqlc.narg(after_part_no)::integer
  )
ORDER BY part_no
LIMIT sqlc.arg(page_size);

-- name: ListUploadPartsByUploadIDs :many
SELECT *
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = ANY(sqlc.arg(upload_ids)::uuid[])
ORDER BY upload_id, part_no;

-- name: LockUploadSessionForUser :one
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
FOR UPDATE;

-- name: LockUploadSession :one
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE id = sqlc.arg(upload_id)
FOR UPDATE;

-- name: MarkUploadCompleting :one
UPDATE /* TEMPLATE: schema */upload_sessions
SET state = 'completing',
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
  AND state = 'open'
  AND terminal_failure_code IS NULL
RETURNING *;

-- name: FinalizeUploadExpectedSize :one
UPDATE /* TEMPLATE: schema */upload_sessions
SET expected_size = sqlc.arg(expected_size),
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
  AND expected_size = -1
  AND state = 'open'
  AND terminal_failure_code IS NULL
RETURNING *;

-- name: GetStoredUploadPartSummary :one
SELECT
    count(*)::integer AS part_count,
    COALESCE(sum(plain_size), 0)::bigint AS plain_size,
    COALESCE(sum(stored_size), 0)::bigint AS stored_size,
    COALESCE(min(part_no), 0)::integer AS min_part_no,
    COALESCE(max(part_no), 0)::integer AS max_part_no
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id)
  AND state = 'stored';

-- name: InsertFileFromUpload :one
INSERT INTO /* TEMPLATE: schema */files (
    id,
    user_id,
    parent_id,
    name,
    kind,
    mime_type,
    size,
    hash_algorithm,
    hash_value,
    encryption,
    encryption_key_version,
    status,
    mod_time
)
SELECT
    sqlc.arg(file_id),
    user_id,
    parent_id,
    name,
    'file',
    mime_type,
    expected_size,
    sqlc.narg(hash_algorithm),
    sqlc.narg(hash_value),
    encryption,
    encryption_key_version,
    'active',
    mod_time
FROM /* TEMPLATE: schema */upload_sessions us
WHERE us.id = sqlc.arg(upload_id)
  AND us.user_id = sqlc.arg(user_id)
  AND us.state = 'completing'
  AND us.terminal_failure_code IS NULL
RETURNING *;

-- name: InsertFilePartsFromUpload :execrows
INSERT INTO /* TEMPLATE: schema */file_parts (
    file_id,
    part_no,
    channel_id,
    message_id,
    plain_size,
    stored_size,
    checksum,
    salt,
    block_hashes
)
SELECT
    sqlc.arg(file_id),
    part_no,
    channel_id,
    message_id,
    plain_size,
    stored_size,
    checksum,
    salt,
    block_hashes
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_parts.upload_id = sqlc.arg(upload_id)
  AND upload_parts.state = 'stored'
  AND EXISTS (SELECT 1 FROM /* TEMPLATE: schema */upload_sessions s
      WHERE s.id=sqlc.arg(upload_id) AND s.state='completing' AND s.terminal_failure_code IS NULL)
  AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */channel_deletion_intents g
      WHERE g.channel_id=/* TEMPLATE: schema */upload_parts.channel_id)
  AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts r
      WHERE r.channel_id=/* TEMPLATE: schema */upload_parts.channel_id
        AND r.message_id=/* TEMPLATE: schema */upload_parts.message_id
        AND r.state IN ('cleanup_pending','deleting','deleted','exhausted','unresolved'))
ORDER BY part_no;

-- name: CompleteUploadSession :one
UPDATE /* TEMPLATE: schema */upload_sessions
SET state = 'completed',
    file_id = sqlc.arg(file_id),
    completed_at = now(),
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND /* TEMPLATE: schema */upload_sessions.user_id = sqlc.arg(user_id)
  AND state = 'completing'
  AND terminal_failure_code IS NULL
  AND (NOT guarded OR EXISTS (
      SELECT 1 FROM /* TEMPLATE: schema */guarded_upload_results r
      WHERE r.upload_id = sqlc.arg(upload_id)
        AND r.file_id = sqlc.arg(file_id)))
RETURNING *;

-- name: AbortUploadSession :one
UPDATE /* TEMPLATE: schema */upload_sessions
SET state = 'aborted',
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
  AND state IN ('open', 'completing')
  AND terminal_failure_code IS DISTINCT FROM 'upload_destination_unknown'
RETURNING *;

-- name: GetCleanupSweepCutoff :one
SELECT clock_timestamp()::timestamptz AS pass_cutoff;

-- name: ExpireUploadSessions :many
UPDATE /* TEMPLATE: schema */upload_sessions
SET state = 'expired',
    updated_at = now()
WHERE id IN (
    SELECT id
    FROM /* TEMPLATE: schema */upload_sessions
    WHERE state IN ('open', 'completing')
      AND expires_at <= sqlc.arg(pass_cutoff)::timestamptz
      AND created_at <= sqlc.arg(pass_cutoff)::timestamptz
      AND terminal_failure_code IS DISTINCT FROM 'upload_destination_unknown'
    ORDER BY created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1000
)
RETURNING *;

-- name: ListUploadPartsForCleanupMany :many
SELECT up.*
FROM /* TEMPLATE: schema */upload_parts AS up
JOIN /* TEMPLATE: schema */upload_sessions AS us ON us.id = up.upload_id
WHERE us.id = ANY(sqlc.arg(upload_ids)::uuid[])
  AND us.state IN ('aborted', 'expired')
  AND us.terminal_failure_code IS DISTINCT FROM 'upload_destination_unknown'
  AND up.message_id IS NOT NULL
ORDER BY up.upload_id, up.channel_id, up.part_no;

-- name: ListUploadSessionsPendingCleanup :many
SELECT us.*
FROM /* TEMPLATE: schema */upload_sessions us
WHERE us.created_at <= sqlc.arg(pass_cutoff)::timestamptz
  AND (
    (us.state IN ('aborted', 'expired')
     AND EXISTS (SELECT 1 FROM /* TEMPLATE: schema */upload_parts up
                 WHERE up.upload_id = us.id AND up.message_id IS NOT NULL))
    OR us.terminal_failure_code = 'upload_destination_unknown'
  )
  AND ((sqlc.narg(after_created_at)::timestamptz IS NULL
        AND sqlc.narg(after_id)::uuid IS NULL)
       OR (sqlc.narg(after_created_at)::timestamptz IS NOT NULL
           AND sqlc.narg(after_id)::uuid IS NOT NULL
           AND (us.created_at, us.id) > (
             sqlc.narg(after_created_at)::timestamptz, sqlc.narg(after_id)::uuid)))
ORDER BY us.created_at, us.id
LIMIT 1000;

-- Scans return existing provenance only; age and missing references create none.
-- name: ListCleanupMessageDispositions :many
SELECT r.id, r.user_id, COALESCE(r.message_id, 0)::bigint AS message_id,
       r.state, r.attempts,
       COALESCE(status.origin_eligible, false)::boolean AS origin_eligible,
       COALESCE(status.has_references, true)::boolean AS has_references
FROM /* TEMPLATE: schema */remote_write_receipts r
JOIN /* TEMPLATE: schema */lifecycle_receipt_status status ON status.id = r.id
WHERE r.row_kind = 'message_attempt'
  AND r.channel_id = sqlc.arg(channel_id)::bigint
  AND r.message_id = ANY(sqlc.arg(message_ids)::bigint[])
ORDER BY r.message_id, r.id;

-- name: DeleteUploadPartsForCleanup :execrows
DELETE FROM /* TEMPLATE: schema */upload_parts AS part
USING /* TEMPLATE: schema */upload_sessions AS session,
      /* TEMPLATE: schema */remote_write_receipts AS receipt,
      jsonb_to_recordset(sqlc.arg(parts)::jsonb) AS cleanup_part(
        user_id bigint, upload_id uuid, part_no integer, channel_id bigint, message_id bigint
      )
WHERE part.upload_id = cleanup_part.upload_id
  AND part.part_no = cleanup_part.part_no
  AND part.message_id = cleanup_part.message_id
  AND part.channel_id = cleanup_part.channel_id
  AND session.id = part.upload_id
  AND session.user_id = cleanup_part.user_id
  AND session.state IN ('aborted', 'expired')
  AND session.terminal_failure_code IS DISTINCT FROM 'upload_destination_unknown'
  AND receipt.row_kind = 'message_attempt' AND receipt.origin_kind = 'upload'
  AND receipt.user_id = session.user_id AND receipt.upload_id = part.upload_id
  AND receipt.part_no = part.part_no AND receipt.channel_id = part.channel_id
  AND receipt.message_id = part.message_id AND receipt.state = 'deleted'
  AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */file_parts fp
    WHERE fp.channel_id = part.channel_id AND fp.message_id = part.message_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */upload_parts other
    WHERE other.channel_id = part.channel_id AND other.message_id = part.message_id
      AND (other.upload_id, other.part_no) <> (part.upload_id, part.part_no)
  );

-- name: LockUploadDestinationConflict :one
SELECT *
FROM /* TEMPLATE: schema */files
WHERE user_id = sqlc.arg(user_id)
  AND parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid
  AND name = sqlc.arg(name)
  AND status = 'active'
FOR UPDATE;

-- name: MarkActiveFileDeletionPendingForReplace :execrows
UPDATE /* TEMPLATE: schema */files
SET status = 'deletion_pending',
    deleted_at = COALESCE(deleted_at, now()),
    updated_at = now(),
    generation = generation + 1
WHERE id = sqlc.arg(file_id)
  AND user_id = sqlc.arg(user_id)
  AND status = 'active';

-- name: RevokeActiveSharesForFile :exec
UPDATE /* TEMPLATE: schema */file_shares
SET revoked_at = COALESCE(revoked_at, now())
WHERE file_id = sqlc.arg(file_id)
  AND owner_id = sqlc.arg(user_id)
  AND revoked_at IS NULL;

-- name: RenameUploadSession :execrows
UPDATE /* TEMPLATE: schema */upload_sessions
SET name = sqlc.arg(name),
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id);

-- name: ListUploadDailyStatistics :many
WITH days AS (
  SELECT generate_series(
    (CURRENT_DATE - (sqlc.arg(days)::integer - 1))::date,
    CURRENT_DATE,
    interval '1 day'
  )::date AS day
), totals AS (
  SELECT completed_at::date AS day,
         COALESCE(sum(expected_size), 0)::bigint AS uploaded_bytes,
         count(*)::bigint AS completed_files
  FROM /* TEMPLATE: schema */upload_sessions
  WHERE user_id = sqlc.arg(user_id)
    AND state = 'completed'
    AND completed_at >= CURRENT_DATE - (sqlc.arg(days)::integer - 1)
  GROUP BY completed_at::date
)
SELECT d.day, COALESCE(t.uploaded_bytes, 0)::bigint AS uploaded_bytes,
       COALESCE(t.completed_files, 0)::bigint AS completed_files
FROM days d
LEFT JOIN totals t USING (day)
ORDER BY d.day;

-- name: GetCompletedUploadPartProof :one
SELECT s.id AS upload_id, p.part_no, p.channel_id, p.message_id,
       p.plain_size, p.stored_size, p.checksum, p.salt, p.block_hashes,
       COALESCE(r.origin_part_created_at, a.created_at)::timestamptz AS created_at,
       COALESCE(r.origin_part_updated_at, a.updated_at)::timestamptz AS updated_at
FROM /* TEMPLATE: schema */upload_sessions s
JOIN /* TEMPLATE: schema */files f ON f.id = s.file_id AND f.user_id = s.user_id AND f.status = 'active'
JOIN /* TEMPLATE: schema */file_parts p ON p.file_id = f.id
LEFT JOIN /* TEMPLATE: schema */remote_write_receipts r
  ON r.row_kind = 'message_attempt' AND r.origin_kind = 'upload'
 AND r.user_id = s.user_id AND r.upload_id = s.id AND r.part_no = p.part_no
 AND r.channel_id = p.channel_id AND r.message_id = p.message_id
 AND r.result_file_id = f.id AND r.state = 'bound'
 AND r.origin_part_created_at IS NOT NULL AND r.origin_part_updated_at IS NOT NULL
LEFT JOIN /* TEMPLATE: schema */upload_parts a
  ON a.upload_id = s.id AND a.part_no = p.part_no AND a.state = 'stored'
 AND a.channel_id = p.channel_id AND a.message_id = p.message_id
 AND a.plain_size IS NOT DISTINCT FROM p.plain_size
 AND a.stored_size IS NOT DISTINCT FROM p.stored_size
 AND a.checksum IS NOT DISTINCT FROM p.checksum AND a.salt IS NOT DISTINCT FROM p.salt
 AND a.block_hashes IS NOT DISTINCT FROM p.block_hashes
WHERE s.id = sqlc.arg(upload_id)::uuid AND s.user_id = sqlc.arg(user_id)::bigint
  AND s.state = 'completed' AND p.part_no = sqlc.arg(part_no)::integer
  AND p.plain_size IS NOT NULL AND (r.id IS NOT NULL OR a.upload_id IS NOT NULL);

-- name: ListCompletedUploadPartProofs :many
SELECT s.id AS upload_id, p.part_no, p.channel_id, p.message_id,
       p.plain_size, p.stored_size, p.checksum, p.salt, p.block_hashes,
       COALESCE(r.origin_part_created_at, a.created_at)::timestamptz AS created_at,
       COALESCE(r.origin_part_updated_at, a.updated_at)::timestamptz AS updated_at
FROM /* TEMPLATE: schema */upload_sessions s
JOIN /* TEMPLATE: schema */files f ON f.id = s.file_id AND f.user_id = s.user_id AND f.status = 'active'
JOIN /* TEMPLATE: schema */file_parts p ON p.file_id = f.id
LEFT JOIN /* TEMPLATE: schema */remote_write_receipts r
  ON r.row_kind = 'message_attempt' AND r.origin_kind = 'upload'
 AND r.user_id = s.user_id AND r.upload_id = s.id AND r.part_no = p.part_no
 AND r.channel_id = p.channel_id AND r.message_id = p.message_id
 AND r.result_file_id = f.id AND r.state = 'bound'
 AND r.origin_part_created_at IS NOT NULL AND r.origin_part_updated_at IS NOT NULL
LEFT JOIN /* TEMPLATE: schema */upload_parts a
  ON a.upload_id = s.id AND a.part_no = p.part_no AND a.state = 'stored'
 AND a.channel_id = p.channel_id AND a.message_id = p.message_id
 AND a.plain_size IS NOT DISTINCT FROM p.plain_size
 AND a.stored_size IS NOT DISTINCT FROM p.stored_size
 AND a.checksum IS NOT DISTINCT FROM p.checksum AND a.salt IS NOT DISTINCT FROM p.salt
 AND a.block_hashes IS NOT DISTINCT FROM p.block_hashes
WHERE s.id = sqlc.arg(upload_id)::uuid AND s.user_id = sqlc.arg(user_id)::bigint
  AND s.state = 'completed' AND p.plain_size IS NOT NULL
  AND (r.id IS NOT NULL OR a.upload_id IS NOT NULL)
  AND (sqlc.narg(after_part_no)::integer IS NULL OR p.part_no > sqlc.narg(after_part_no)::integer)
ORDER BY p.part_no LIMIT sqlc.arg(page_size);

-- name: LockUploadCreateImportBatch :one
SELECT b.* FROM /* TEMPLATE: schema */import_batches b
JOIN /* TEMPLATE: schema */import_job_attempts a ON a.user_id=b.user_id AND a.batch_id=b.batch_id
WHERE a.user_id=sqlc.arg(user_id)::bigint AND a.attempt_id=sqlc.arg(attempt_id)::uuid
FOR UPDATE OF b;

-- name: LockUploadCreateImportPins :exec
SELECT p.batch_id FROM /* TEMPLATE: schema */import_destination_pins p
JOIN /* TEMPLATE: schema */import_job_attempts a ON a.user_id=p.user_id AND a.batch_id=p.batch_id
WHERE a.user_id=sqlc.arg(user_id)::bigint AND a.attempt_id=sqlc.arg(attempt_id)::uuid
ORDER BY p.destination_ordinal FOR UPDATE OF p;

-- name: LockUploadCreateImportAttempt :one
SELECT * FROM /* TEMPLATE: schema */import_job_attempts
WHERE user_id=sqlc.arg(user_id)::bigint AND attempt_id=sqlc.arg(attempt_id)::uuid
FOR UPDATE;

-- name: MarkUploadImportMaterialized :execrows
UPDATE /* TEMPLATE: schema */import_job_attempts a
SET session_materialized=TRUE
WHERE a.user_id=sqlc.arg(user_id)::bigint AND a.attempt_id=sqlc.arg(attempt_id)::uuid
  AND a.upload_id=sqlc.arg(upload_id)::uuid AND NOT a.session_materialized
  AND a.terminal_failure_code IS NULL
  AND EXISTS (SELECT 1 FROM /* TEMPLATE: schema */import_batches b
      WHERE b.user_id=a.user_id AND b.batch_id=a.batch_id AND b.terminal_failure_code IS NULL)
  AND EXISTS (SELECT 1 FROM /* TEMPLATE: schema */upload_sessions s
      WHERE s.id=a.upload_id AND s.user_id=a.user_id AND s.terminal_failure_code IS NULL);

-- name: CaptureCompletedUploadPartOrigins :execrows
UPDATE /* TEMPLATE: schema */remote_write_receipts r
SET result_file_id=s.file_id, origin_part_created_at=p.created_at,
    origin_part_updated_at=p.updated_at, updated_at=now()
FROM /* TEMPLATE: schema */upload_sessions s,
     /* TEMPLATE: schema */upload_parts p,
     /* TEMPLATE: schema */file_parts f
WHERE s.id=sqlc.arg(upload_id)::uuid AND s.user_id=sqlc.arg(user_id)::bigint
  AND s.state='completed' AND s.file_id=sqlc.arg(file_id)::uuid
  AND p.upload_id=s.id AND p.state='stored'
  AND f.file_id=s.file_id AND f.part_no=p.part_no
  AND f.channel_id=p.channel_id AND f.message_id=p.message_id
  AND f.plain_size IS NOT DISTINCT FROM p.plain_size
  AND f.stored_size IS NOT DISTINCT FROM p.stored_size
  AND f.checksum IS NOT DISTINCT FROM p.checksum AND f.salt IS NOT DISTINCT FROM p.salt
  AND f.block_hashes IS NOT DISTINCT FROM p.block_hashes
  AND r.row_kind='message_attempt' AND r.origin_kind='upload'
  AND r.user_id=s.user_id AND r.upload_id=s.id AND r.part_no=p.part_no
  AND r.channel_id=p.channel_id AND r.message_id=p.message_id AND r.state='bound'
  AND (r.result_file_id IS NULL OR r.result_file_id=s.file_id);

-- name: RetireCompletedUploadPartAliases :execrows
DELETE FROM /* TEMPLATE: schema */upload_parts p
USING /* TEMPLATE: schema */upload_sessions s,
      /* TEMPLATE: schema */file_parts f,
      /* TEMPLATE: schema */remote_write_receipts r
WHERE s.id=sqlc.arg(upload_id)::uuid AND s.user_id=sqlc.arg(user_id)::bigint
  AND s.state='completed' AND s.file_id=sqlc.arg(file_id)::uuid
  AND p.upload_id=s.id AND p.state='stored' AND f.file_id=s.file_id AND f.part_no=p.part_no
  AND f.channel_id=p.channel_id AND f.message_id=p.message_id
  AND f.plain_size IS NOT DISTINCT FROM p.plain_size
  AND f.stored_size IS NOT DISTINCT FROM p.stored_size
  AND f.checksum IS NOT DISTINCT FROM p.checksum AND f.salt IS NOT DISTINCT FROM p.salt
  AND f.block_hashes IS NOT DISTINCT FROM p.block_hashes
  AND r.row_kind='message_attempt' AND r.origin_kind='upload'
  AND r.user_id=s.user_id AND r.upload_id=s.id AND r.part_no=p.part_no
  AND r.channel_id=p.channel_id AND r.message_id=p.message_id
  AND r.state='bound' AND r.result_file_id=s.file_id
  AND r.origin_part_created_at=p.created_at AND r.origin_part_updated_at=p.updated_at;

-- name: ListUploadPublicationReceipts :many
SELECT r.* FROM /* TEMPLATE: schema */remote_write_receipts r
JOIN /* TEMPLATE: schema */upload_parts p ON p.upload_id=r.upload_id AND p.part_no=r.part_no
JOIN /* TEMPLATE: schema */upload_sessions s ON s.id=p.upload_id AND s.user_id=r.user_id
WHERE s.user_id=sqlc.arg(user_id)::bigint AND s.id=sqlc.arg(upload_id)::uuid
  AND p.state='stored' AND r.row_kind='message_attempt' AND r.origin_kind='upload'
  AND r.channel_id=p.channel_id AND r.message_id=p.message_id AND r.state='bound'
ORDER BY r.channel_id,r.message_id,r.id;

-- name: GetImportBatch :one
SELECT * FROM /* TEMPLATE: schema */import_batches
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=sqlc.arg(batch_id)::uuid
  AND origin_job_id=sqlc.arg(origin_job_id)::bigint;

-- name: LockImportBatch :one
SELECT * FROM /* TEMPLATE: schema */import_batches
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=sqlc.arg(batch_id)::uuid
  AND origin_job_id=sqlc.arg(origin_job_id)::bigint FOR UPDATE;

-- name: GetImportAttempt :one
SELECT * FROM /* TEMPLATE: schema */import_job_attempts
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=sqlc.arg(batch_id)::uuid
  AND source_index=sqlc.arg(source_index)::integer AND job_id=sqlc.arg(job_id)::bigint;

-- name: InsertImportBatchAdmission :one
INSERT INTO /* TEMPLATE: schema */import_batches(user_id,batch_id,origin_job_id,request_fingerprint)
VALUES(sqlc.arg(user_id),sqlc.arg(batch_id),sqlc.arg(origin_job_id),sqlc.arg(request_fingerprint))
ON CONFLICT DO NOTHING RETURNING *;

-- name: InsertImportDestinationPin :exec
INSERT INTO /* TEMPLATE: schema */import_destination_pins
 (user_id,batch_id,destination_ordinal,anchor_id,anchor_is_root,spec_fingerprint,remainder_fingerprint,remainder_segments)
VALUES(sqlc.arg(user_id),sqlc.arg(batch_id),sqlc.arg(destination_ordinal),sqlc.narg(anchor_id),
 sqlc.arg(anchor_is_root),sqlc.arg(spec_fingerprint),sqlc.arg(remainder_fingerprint),sqlc.arg(remainder_segments));

-- name: ListImportDestinationPins :many
SELECT * FROM /* TEMPLATE: schema */import_destination_pins
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=sqlc.arg(batch_id)::uuid
ORDER BY destination_ordinal;

-- name: LockImportBatchPins :exec
SELECT destination_ordinal FROM /* TEMPLATE: schema */import_destination_pins
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=sqlc.arg(batch_id)::uuid
ORDER BY destination_ordinal FOR UPDATE;

-- name: DispatchImportBatch :execrows
UPDATE /* TEMPLATE: schema */import_batches
SET dispatched=true,parent_id=sqlc.narg(parent_id),destination_is_root=sqlc.arg(destination_is_root),
    source_count=sqlc.arg(source_count)
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=sqlc.arg(batch_id)::uuid
  AND origin_job_id=sqlc.arg(origin_job_id)::bigint AND NOT dispatched AND terminal_failure_code IS NULL;

-- name: InsertImportChild :one
INSERT INTO /* TEMPLATE: schema */import_job_attempts
 (attempt_id,user_id,batch_id,source_index,job_id,parent_id)
SELECT sqlc.arg(attempt_id),b.user_id,b.batch_id,sqlc.arg(source_index),sqlc.arg(job_id),sqlc.narg(parent_id)
FROM /* TEMPLATE: schema */import_batches b
WHERE b.user_id=sqlc.arg(user_id)::bigint AND b.batch_id=sqlc.arg(batch_id)::uuid
  AND NOT b.dispatched AND b.terminal_failure_code IS NULL RETURNING *;

-- name: SelectImportUpload :one
UPDATE /* TEMPLATE: schema */import_job_attempts
SET upload_id=sqlc.arg(upload_id),selected_create=sqlc.arg(selected_create)::jsonb
WHERE attempt_id=sqlc.arg(attempt_id)::uuid AND user_id=sqlc.arg(user_id)::bigint
  AND terminal_failure_code IS NULL AND upload_id IS NULL AND result_file_id IS NULL RETURNING *;

-- name: SelectImportResult :one
UPDATE /* TEMPLATE: schema */import_job_attempts
SET result_file_id=sqlc.arg(result_file_id)
WHERE attempt_id=sqlc.arg(attempt_id)::uuid AND user_id=sqlc.arg(user_id)::bigint
  AND terminal_failure_code IS NULL AND upload_id IS NULL AND result_file_id IS NULL RETURNING *;

-- name: ListImportBatchAttempts :many
SELECT * FROM /* TEMPLATE: schema */import_job_attempts
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=sqlc.arg(batch_id)::uuid
ORDER BY source_index;

-- name: GetImportAttemptForUpload :one
SELECT * FROM /* TEMPLATE: schema */import_job_attempts
WHERE user_id=sqlc.arg(user_id)::bigint AND upload_id=sqlc.arg(upload_id)::uuid;

-- name: GetRecoveryIncarnation :one
SELECT incarnation
FROM /* TEMPLATE: schema */recovery_incarnations
WHERE singleton;

-- name: InsertGuardedUploadBinding :one
INSERT INTO /* TEMPLATE: schema */guarded_upload_bindings (
    upload_id,
    user_id,
    request_admission_id,
    request_key,
    recovery_incarnation,
    mount_root,
    parent_id,
    name,
    expected_file_id,
    expected_generation
) VALUES (
    sqlc.arg(upload_id),
    sqlc.arg(user_id),
    sqlc.arg(request_admission_id),
    sqlc.arg(request_key),
    sqlc.arg(recovery_incarnation),
    sqlc.narg(mount_root),
    sqlc.narg(parent_id),
    sqlc.arg(name),
    sqlc.narg(expected_file_id),
    sqlc.narg(expected_generation)
)
RETURNING *;

-- name: GetGuardedUploadBinding :one
SELECT *
FROM /* TEMPLATE: schema */guarded_upload_bindings
WHERE upload_id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id);

-- name: SetUploadGuardConflict :execrows
UPDATE /* TEMPLATE: schema */upload_sessions
SET guard_conflict_code = sqlc.arg(conflict_code),
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
  AND guarded
  AND guard_conflict_code IS NULL;

-- name: InsertGuardedUploadResult :one
INSERT INTO /* TEMPLATE: schema */guarded_upload_results (
    upload_id,
    user_id,
    request_admission_id,
    size,
    hash_algorithm,
    hash_value,
    file_id,
    generation,
    parent_id,
    name,
    mount_root,
    parent_segments,
    expected_file_id,
    expected_generation
) VALUES (
    sqlc.arg(upload_id),
    sqlc.arg(user_id),
    sqlc.arg(request_admission_id),
    sqlc.arg(size),
    sqlc.arg(hash_algorithm),
    sqlc.arg(hash_value),
    sqlc.arg(file_id),
    sqlc.arg(generation),
    sqlc.narg(parent_id),
    sqlc.arg(name),
    sqlc.narg(mount_root),
    sqlc.arg(parent_segments),
    sqlc.narg(expected_file_id),
    sqlc.narg(expected_generation)
)
RETURNING *;

-- The receipt's admission key is the guarded create admission. A client that
-- survives a crash holds only that key (it journals it before the first dirty
-- byte); the completing request's own admission is never durable client state.
-- The result row itself keeps the commit-time admission as publication evidence.
-- name: GetGuardedUploadResult :one
SELECT r.*, b.request_key AS admission_key
FROM /* TEMPLATE: schema */guarded_upload_results r
JOIN /* TEMPLATE: schema */guarded_upload_bindings b
  ON b.upload_id = r.upload_id AND b.user_id = r.user_id
WHERE r.upload_id = sqlc.arg(upload_id)
  AND r.user_id = sqlc.arg(user_id);

-- name: LockGuardedExpectedFile :one
SELECT *
FROM /* TEMPLATE: schema */files
WHERE id = sqlc.arg(file_id)
  AND user_id = sqlc.arg(user_id)
FOR UPDATE;

-- The chain starts at the destination parent and walks toward the owner root.
-- Callers compare it with the pinned mount root before any publication effect.
-- name: ListActiveAncestorChain :many
WITH RECURSIVE chain AS (
  SELECT file.id, file.parent_id, file.name, 0::integer AS depth
  FROM /* TEMPLATE: schema */files AS file
  WHERE file.id = sqlc.arg(file_id)
    AND file.user_id = sqlc.arg(user_id)
    AND file.status = 'active'
  UNION ALL
  SELECT parent.id, parent.parent_id, parent.name, child.depth + 1
  FROM /* TEMPLATE: schema */files AS parent
  JOIN chain AS child ON parent.id = child.parent_id
  WHERE parent.user_id = sqlc.arg(user_id)
    AND parent.status = 'active'
)
SELECT id, parent_id, name, depth
FROM chain
ORDER BY depth;
