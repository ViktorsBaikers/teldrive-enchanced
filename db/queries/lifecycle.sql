-- name: LifecycleDatabaseTime :one
SELECT clock_timestamp()::timestamptz AS now;

-- name: GetRemoteWriteReceipt :one
SELECT * FROM /* TEMPLATE: schema */remote_write_receipts WHERE id = sqlc.arg(receipt_id)::uuid;

-- name: ListRemoteWriteReceipts :many
SELECT * FROM /* TEMPLATE: schema */remote_write_receipts
WHERE user_id = sqlc.arg(user_id)::bigint AND id = ANY(sqlc.arg(receipt_ids)::uuid[])
ORDER BY channel_id NULLS FIRST, message_id NULLS FIRST, id;

-- name: LockRemoteWriteReceipts :many
SELECT * FROM /* TEMPLATE: schema */remote_write_receipts
WHERE user_id = sqlc.arg(user_id)::bigint AND id = ANY(sqlc.arg(receipt_ids)::uuid[])
ORDER BY channel_id NULLS FIRST, message_id NULLS FIRST, id FOR UPDATE;

-- name: LockReceiptImportBatches :exec
SELECT b.batch_id FROM /* TEMPLATE: schema */import_batches b
WHERE b.user_id = sqlc.arg(user_id)::bigint AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */import_job_attempts a
    JOIN /* TEMPLATE: schema */remote_write_receipts r ON r.upload_id = a.upload_id
    WHERE a.user_id = b.user_id AND a.batch_id = b.batch_id AND r.id = ANY(sqlc.arg(receipt_ids)::uuid[]))
ORDER BY b.batch_id FOR UPDATE;

-- name: LockReceiptImportPins :exec
SELECT p.destination_ordinal FROM /* TEMPLATE: schema */import_destination_pins p
WHERE p.user_id = sqlc.arg(user_id)::bigint AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */import_job_attempts a
    JOIN /* TEMPLATE: schema */remote_write_receipts r ON r.upload_id = a.upload_id
    WHERE a.user_id = p.user_id AND a.batch_id = p.batch_id AND r.id = ANY(sqlc.arg(receipt_ids)::uuid[]))
ORDER BY p.batch_id,p.destination_ordinal FOR UPDATE;

-- name: LockReceiptImportAttempts :exec
SELECT a.attempt_id FROM /* TEMPLATE: schema */import_job_attempts a
WHERE a.user_id = sqlc.arg(user_id)::bigint AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts r
    WHERE r.upload_id = a.upload_id AND r.id = ANY(sqlc.arg(receipt_ids)::uuid[]))
ORDER BY a.attempt_id FOR UPDATE;

-- name: LockReceiptCopyOperations :exec
SELECT op.id FROM /* TEMPLATE: schema */remote_write_receipts op
WHERE op.user_id = sqlc.arg(user_id)::bigint AND op.row_kind = 'copy_operation'
  AND (op.id = ANY(sqlc.arg(receipt_ids)::uuid[]) OR EXISTS (
      SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts r
      WHERE r.operation_id = op.id AND r.id = ANY(sqlc.arg(receipt_ids)::uuid[])))
ORDER BY op.id FOR UPDATE;

-- name: LockReceiptUploadSessions :exec
SELECT s.id FROM /* TEMPLATE: schema */upload_sessions s
WHERE s.user_id = sqlc.arg(user_id)::bigint AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts r
    WHERE r.upload_id = s.id AND r.id = ANY(sqlc.arg(receipt_ids)::uuid[]))
ORDER BY s.id FOR UPDATE;

-- name: GetRequestAdmissionForActor :one
SELECT * FROM /* TEMPLATE: schema */request_admissions
WHERE actor_user_id = sqlc.arg(actor_user_id)::bigint
  AND request_kind = sqlc.arg(request_kind)::text AND request_key = sqlc.arg(request_key)::uuid;

-- name: GetRequestAdmissionForShare :one
SELECT * FROM /* TEMPLATE: schema */request_admissions
WHERE public_share_id = sqlc.arg(public_share_id)::uuid
  AND request_kind = sqlc.arg(request_kind)::text AND request_key = sqlc.arg(request_key)::uuid;

-- name: InsertRequestAdmission :one
INSERT INTO /* TEMPLATE: schema */request_admissions (
    id,user_id,request_kind,request_key,actor_user_id,public_share_id,request_fingerprint,upload_id,copy_operation_id
) VALUES (
    sqlc.arg(id)::uuid,sqlc.arg(user_id)::bigint,sqlc.arg(request_kind)::text,sqlc.arg(request_key)::uuid,
    sqlc.narg(actor_user_id)::bigint,sqlc.narg(public_share_id)::uuid,sqlc.arg(request_fingerprint)::bytea,
    sqlc.narg(upload_id)::uuid,sqlc.narg(copy_operation_id)::uuid
) ON CONFLICT DO NOTHING RETURNING *;

-- name: IsLifecycleDestinationActive :one
WITH RECURSIVE ancestry AS (
    SELECT f.id,f.parent_id,f.user_id,f.kind,f.status,ARRAY[f.id] AS visited,FALSE AS cycle
    FROM /* TEMPLATE: schema */files f WHERE f.id = sqlc.arg(parent_id)::uuid
    UNION ALL
    SELECT f.id,f.parent_id,f.user_id,f.kind,f.status,a.visited || f.id,f.id = ANY(a.visited)
    FROM /* TEMPLATE: schema */files f JOIN ancestry a ON f.id = a.parent_id WHERE NOT a.cycle
)
SELECT COALESCE(EXISTS (SELECT 1 FROM ancestry WHERE parent_id IS NULL)
   AND NOT EXISTS (SELECT 1 FROM ancestry WHERE user_id <> sqlc.arg(user_id)::bigint OR kind <> 'folder' OR status <> 'active' OR cycle),FALSE)::boolean AS active;

-- name: ReserveCopyOperation :one
INSERT INTO /* TEMPLATE: schema */remote_write_receipts (
    id,row_kind,user_id,origin_kind,operation_id,result_file_id,destination_parent_id,destination_is_root,lease_token,lease_expires_at
) SELECT
    sqlc.arg(operation_id)::uuid,'copy_operation',sqlc.arg(user_id)::bigint,'copy',sqlc.arg(operation_id)::uuid,
    sqlc.arg(result_file_id)::uuid,sqlc.narg(destination_parent_id)::uuid,sqlc.arg(destination_is_root)::boolean,
    sqlc.arg(lease_token)::uuid,sqlc.arg(lease_expires_at)::timestamptz
WHERE sqlc.arg(lease_expires_at)::timestamptz > clock_timestamp()
ON CONFLICT DO NOTHING RETURNING *;

-- name: RenewCopyOperation :one
UPDATE /* TEMPLATE: schema */remote_write_receipts
SET lease_expires_at = sqlc.arg(lease_expires_at)::timestamptz,updated_at = clock_timestamp()
WHERE id = sqlc.arg(operation_id)::uuid AND user_id = sqlc.arg(user_id)::bigint AND row_kind = 'copy_operation'
  AND lease_token = sqlc.arg(lease_token)::uuid AND lease_expires_at > clock_timestamp()
  AND sqlc.arg(lease_expires_at)::timestamptz > lease_expires_at
  AND state = 'reserved' AND terminal_at IS NULL AND published_at IS NULL
RETURNING *;

-- name: PublishCopyOperation :execrows
UPDATE /* TEMPLATE: schema */remote_write_receipts
SET state = 'bound',published_at = clock_timestamp(),updated_at = clock_timestamp()
WHERE id = sqlc.arg(operation_id)::uuid AND user_id = sqlc.arg(user_id)::bigint AND row_kind = 'copy_operation'
  AND lease_token = sqlc.arg(lease_token)::uuid AND lease_expires_at > clock_timestamp()
  AND state = 'reserved' AND terminal_at IS NULL AND published_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts r
      WHERE r.operation_id = sqlc.arg(operation_id)::uuid AND r.row_kind = 'message_attempt' AND r.state <> 'bound');

-- name: TerminalizeCopyOperation :execrows
UPDATE /* TEMPLATE: schema */remote_write_receipts
SET terminal_at = clock_timestamp(),terminal_failure_code = sqlc.arg(failure_code)::text,
    primary_code = COALESCE(primary_code,sqlc.arg(failure_code)::text),
    cleanup_code = COALESCE(cleanup_code,'remote_outcome_unknown'),
    lease_expires_at = LEAST(lease_expires_at,clock_timestamp()),state = 'unresolved',updated_at = clock_timestamp()
WHERE id = sqlc.arg(operation_id)::uuid AND user_id = sqlc.arg(user_id)::bigint
  AND row_kind = 'copy_operation' AND published_at IS NULL AND terminal_at IS NULL;

-- name: ListCopyDestinationOperations :many
SELECT * FROM /* TEMPLATE: schema */remote_write_receipts
WHERE user_id = sqlc.arg(user_id)::bigint AND row_kind = 'copy_operation'
  AND destination_parent_id = ANY(sqlc.arg(subtree_ids)::uuid[])
  AND published_at IS NULL AND terminal_at IS NULL
ORDER BY id;

-- name: ListCopyOperationReceipts :many
SELECT * FROM /* TEMPLATE: schema */remote_write_receipts
WHERE user_id = sqlc.arg(user_id)::bigint AND operation_id = ANY(sqlc.arg(operation_ids)::uuid[])
ORDER BY id;

-- name: ReserveMessageReceipt :one
INSERT INTO /* TEMPLATE: schema */remote_write_receipts (
    id,row_kind,user_id,channel_id,origin_kind,upload_id,part_no,lease_token,operation_id,result_file_id,lease_expires_at
)
SELECT sqlc.arg(id)::uuid,'message_attempt',sqlc.arg(user_id)::bigint,sqlc.arg(channel_id)::bigint,
    sqlc.arg(origin_kind)::text,sqlc.narg(upload_id)::uuid,sqlc.arg(part_no)::integer,sqlc.arg(lease_token)::uuid,
    sqlc.narg(operation_id)::uuid,sqlc.narg(result_file_id)::uuid,sqlc.arg(lease_expires_at)::timestamptz
WHERE sqlc.arg(lease_expires_at)::timestamptz > clock_timestamp()
  AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */channel_deletion_intents WHERE channel_id = sqlc.arg(channel_id)::bigint)
  AND ((sqlc.arg(origin_kind)::text = 'copy' AND EXISTS (
      SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts op
      WHERE op.id = sqlc.narg(operation_id)::uuid AND op.row_kind = 'copy_operation' AND op.user_id = sqlc.arg(user_id)::bigint
        AND op.lease_token = sqlc.arg(lease_token)::uuid AND op.lease_expires_at > clock_timestamp()
        AND op.state = 'reserved' AND op.terminal_at IS NULL AND op.published_at IS NULL))
    OR (sqlc.arg(origin_kind)::text = 'upload' AND EXISTS (
      SELECT 1 FROM /* TEMPLATE: schema */upload_sessions s
      JOIN /* TEMPLATE: schema */upload_parts p ON p.upload_id = s.id
      WHERE s.id = sqlc.narg(upload_id)::uuid AND s.user_id = sqlc.arg(user_id)::bigint
        AND s.state = 'open' AND s.terminal_failure_code IS NULL AND s.expires_at > clock_timestamp()
        AND p.part_no = sqlc.arg(part_no)::integer AND p.channel_id = sqlc.arg(channel_id)::bigint
        AND p.lease_token = sqlc.arg(lease_token)::uuid AND p.lease_expires_at > clock_timestamp() AND p.state = 'uploading')))
ON CONFLICT DO NOTHING RETURNING *;

-- name: ObserveMessageReceipt :one
UPDATE /* TEMPLATE: schema */remote_write_receipts r
SET message_id = sqlc.arg(message_id)::bigint,
    state = CASE WHEN r.attempts >= 4 THEN 'exhausted'
        WHEN r.state IN ('unresolved','cleanup_pending','deleting') OR status.origin_eligible
             OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */channel_deletion_intents g WHERE g.channel_id = r.channel_id)
        THEN 'cleanup_pending' ELSE 'observed' END,
    updated_at = clock_timestamp()
FROM /* TEMPLATE: schema */lifecycle_receipt_status status
WHERE r.id = sqlc.arg(receipt_id)::uuid AND r.user_id = sqlc.arg(user_id)::bigint AND status.id = r.id
  AND r.row_kind = 'message_attempt' AND r.channel_id = sqlc.arg(channel_id)::bigint
  AND r.lease_token = sqlc.arg(lease_token)::uuid AND sqlc.arg(message_id)::bigint > 0
  AND (r.message_id IS NULL OR r.message_id = sqlc.arg(message_id)::bigint)
  AND r.state IN ('reserved','observed','unresolved','cleanup_pending')
RETURNING r.*;

-- name: BindMessageReceipt :one
UPDATE /* TEMPLATE: schema */remote_write_receipts r
SET state = 'bound',result_file_id = COALESCE(r.result_file_id,sqlc.narg(result_file_id)::uuid),updated_at = clock_timestamp()
FROM /* TEMPLATE: schema */lifecycle_receipt_status status
WHERE r.id = sqlc.arg(receipt_id)::uuid AND r.user_id = sqlc.arg(user_id)::bigint AND status.id = r.id
  AND r.row_kind = 'message_attempt' AND r.message_id IS NOT NULL AND r.state IN ('observed','bound')
  AND NOT status.origin_eligible
  AND (sqlc.narg(result_file_id)::uuid IS NULL OR r.result_file_id IS NULL OR r.result_file_id = sqlc.narg(result_file_id)::uuid)
  AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */channel_deletion_intents g WHERE g.channel_id = r.channel_id)
RETURNING r.*;

-- name: HasPhysicalDeletionFence :one
SELECT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts
    WHERE channel_id = sqlc.arg(channel_id)::bigint AND message_id = sqlc.arg(message_id)::bigint
      AND state IN ('cleanup_pending','deleting','deleted','exhausted','unresolved')) AS fenced;

-- name: GetReceiptEligibility :one
SELECT * FROM /* TEMPLATE: schema */lifecycle_receipt_status WHERE id = sqlc.arg(receipt_id)::uuid;

-- name: StageReceiptCleanup :one
UPDATE /* TEMPLATE: schema */remote_write_receipts r
SET state = CASE
        WHEN r.state = 'deleting' AND r.next_attempt_at > clock_timestamp() THEN 'deleting'
        WHEN r.message_id IS NULL THEN 'unresolved'
        WHEN r.attempts >= 4 THEN 'exhausted' ELSE 'cleanup_pending' END,
    primary_code = COALESCE(r.primary_code,sqlc.narg(primary_code)::text),
    cleanup_code = CASE WHEN r.message_id IS NULL THEN COALESCE(r.cleanup_code,'remote_outcome_unknown') ELSE r.cleanup_code END,
    next_attempt_at = COALESCE(r.next_attempt_at,clock_timestamp()),updated_at = clock_timestamp()
FROM /* TEMPLATE: schema */lifecycle_receipt_status status
WHERE r.id = sqlc.arg(receipt_id)::uuid AND r.user_id = sqlc.arg(user_id)::bigint AND status.id = r.id
  AND r.row_kind = 'message_attempt' AND r.state NOT IN ('deleted','exhausted')
  AND status.origin_eligible AND NOT status.has_references
RETURNING r.*;

-- name: AssignReceiptCleanupJob :execrows
UPDATE /* TEMPLATE: schema */remote_write_receipts
SET job_id = COALESCE(job_id,sqlc.arg(job_id)::bigint),wakeup_job_id = sqlc.arg(job_id)::bigint,
    next_attempt_at = clock_timestamp() + interval '5 seconds',updated_at = clock_timestamp()
WHERE user_id = sqlc.arg(user_id)::bigint AND id = ANY(sqlc.arg(receipt_ids)::uuid[])
  AND sqlc.arg(job_id)::bigint > 0 AND published_at IS NULL
  AND state IN ('cleanup_pending','deleting','unresolved','exhausted');

-- River owns job retry backoff. next_attempt_at fences an in-flight claim and
-- duplicate discovery, not delivery of an already admitted cleanup wakeup.
-- name: ClaimReceiptDeletion :one
UPDATE /* TEMPLATE: schema */remote_write_receipts r
SET state = 'deleting',attempts = r.attempts + 1,
    next_attempt_at = clock_timestamp() + interval '5 seconds',updated_at = clock_timestamp()
FROM /* TEMPLATE: schema */lifecycle_receipt_status status
WHERE r.id = sqlc.arg(receipt_id)::uuid AND r.user_id = sqlc.arg(user_id)::bigint AND status.id = r.id
  AND r.row_kind = 'message_attempt' AND r.message_id > 0 AND r.attempts BETWEEN 0 AND 3
  AND r.state IN ('cleanup_pending','deleting')
  AND (r.state <> 'deleting' OR r.next_attempt_at <= clock_timestamp())
  AND status.origin_eligible AND NOT status.has_references
RETURNING r.*;

-- name: AcknowledgeReceiptDeletion :execrows
UPDATE /* TEMPLATE: schema */remote_write_receipts
SET state = 'deleted',cleanup_code = NULL,next_attempt_at = NULL,updated_at = clock_timestamp()
WHERE id = sqlc.arg(receipt_id)::uuid AND user_id = sqlc.arg(user_id)::bigint
  AND channel_id = sqlc.arg(channel_id)::bigint AND message_id = sqlc.arg(message_id)::bigint
  AND state = 'deleting' AND attempts = sqlc.arg(attempts)::integer;

-- name: RecordReceiptDeletionFailure :execrows
UPDATE /* TEMPLATE: schema */remote_write_receipts
SET state = CASE WHEN attempts >= 4 THEN 'exhausted' ELSE 'cleanup_pending' END,
    cleanup_code = sqlc.arg(failure_code)::text,next_attempt_at = clock_timestamp() + interval '5 seconds',updated_at = clock_timestamp()
WHERE id = sqlc.arg(receipt_id)::uuid AND user_id = sqlc.arg(user_id)::bigint
  AND state = 'deleting' AND attempts = sqlc.arg(attempts)::integer;

-- name: ExhaustReceiptBudget :execrows
UPDATE /* TEMPLATE: schema */remote_write_receipts
SET state = 'exhausted',cleanup_code = COALESCE(cleanup_code,'cleanup_attempts_exhausted'),updated_at = clock_timestamp()
WHERE id = sqlc.arg(receipt_id)::uuid AND user_id = sqlc.arg(user_id)::bigint
  AND row_kind = 'message_attempt' AND attempts = 4 AND state IN ('cleanup_pending','deleting')
  AND next_attempt_at <= clock_timestamp();

-- name: FenceExpiredUploadReceiptOrigin :execrows
UPDATE /* TEMPLATE: schema */upload_parts p
SET state = 'failed',lease_expires_at = LEAST(p.lease_expires_at,clock_timestamp()),last_error_code = 'remote_write_abandoned'
FROM /* TEMPLATE: schema */remote_write_receipts r, /* TEMPLATE: schema */upload_sessions s
WHERE r.id = sqlc.arg(receipt_id)::uuid AND r.user_id = sqlc.arg(user_id)::bigint
  AND s.id = r.upload_id AND s.state <> 'completed' AND s.terminal_failure_code IS DISTINCT FROM 'upload_destination_unknown'
  AND p.upload_id = r.upload_id AND p.part_no = r.part_no AND p.lease_token = r.lease_token
  AND p.state IN ('reserved','uploading') AND p.lease_expires_at <= clock_timestamp();

-- name: ListLifecycleRecoveryCandidates :many
SELECT * FROM /* TEMPLATE: schema */remote_write_receipts r
WHERE r.created_at <= sqlc.arg(pass_cutoff)::timestamptz
  AND ((sqlc.narg(after_created_at)::timestamptz IS NULL AND sqlc.narg(after_id)::uuid IS NULL)
    OR (r.created_at,r.id) > (sqlc.narg(after_created_at)::timestamptz,sqlc.narg(after_id)::uuid))
  AND ((r.state IN ('reserved','observed') AND r.lease_expires_at <= sqlc.arg(pass_cutoff)::timestamptz)
    OR (r.state IN ('reserved','observed') AND EXISTS (
        SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts op WHERE op.id = r.operation_id AND op.terminal_at IS NOT NULL))
    OR (r.state IN ('reserved','observed') AND EXISTS (
        SELECT 1 FROM /* TEMPLATE: schema */upload_sessions s WHERE s.id = r.upload_id
          AND s.user_id = r.user_id AND (s.state IN ('aborted','expired') OR s.terminal_failure_code IS NOT NULL)
          AND s.terminal_failure_code IS DISTINCT FROM 'upload_destination_unknown'))
    OR (r.state IN ('cleanup_pending','deleting') AND (r.next_attempt_at IS NULL OR r.next_attempt_at <= sqlc.arg(pass_cutoff)::timestamptz))
    OR (r.state IN ('unresolved','exhausted') AND r.job_id IS NULL))
ORDER BY r.created_at,r.id LIMIT LEAST(sqlc.arg(page_size)::integer,1000);

-- name: ListReceiptJobReports :many
SELECT COALESCE(r.job_id,0)::bigint AS job_id,r.user_id,
    CASE WHEN bool_and(r.state = 'deleted') THEN 'completed'
         WHEN bool_or(r.state IN ('unresolved','exhausted')) THEN 'discarded'
         WHEN bool_or(r.attempts > 0) THEN 'retryable' ELSE 'pending' END::text AS state,
    min(r.created_at)::timestamptz AS created_at,max(r.updated_at)::timestamptz AS updated_at,
    COALESCE(max(r.attempts),0)::integer AS attempts,count(*)::bigint AS receipt_count,
    COALESCE(min(COALESCE(r.primary_code,r.terminal_failure_code)),'')::text AS primary_code,COALESCE(min(r.cleanup_code),'')::text AS cleanup_code,
    COALESCE(bool_or(r.state IN ('unresolved','exhausted')),FALSE)::boolean AS retry_unavailable
FROM /* TEMPLATE: schema */remote_write_receipts r
WHERE r.user_id = sqlc.arg(user_id)::bigint AND r.job_id IS NOT NULL
  AND (sqlc.narg(before_job_id)::bigint IS NULL OR r.job_id < sqlc.narg(before_job_id)::bigint)
GROUP BY r.job_id,r.user_id
HAVING sqlc.narg(state)::text IS NULL OR
    (CASE WHEN bool_and(r.state = 'deleted') THEN 'completed'
          WHEN bool_or(r.state IN ('unresolved','exhausted')) THEN 'discarded'
          WHEN bool_or(r.attempts > 0) THEN 'retryable' ELSE 'pending' END) = sqlc.narg(state)::text
ORDER BY r.job_id DESC LIMIT sqlc.arg(page_size)::integer;

-- name: GetReceiptJobReport :one
SELECT COALESCE(r.job_id,0)::bigint AS job_id,r.user_id,
    CASE WHEN bool_and(r.state = 'deleted') THEN 'completed'
         WHEN bool_or(r.state IN ('unresolved','exhausted')) THEN 'discarded'
         WHEN bool_or(r.attempts > 0) THEN 'retryable' ELSE 'pending' END::text AS state,
    min(r.created_at)::timestamptz AS created_at,max(r.updated_at)::timestamptz AS updated_at,
    COALESCE(max(r.attempts),0)::integer AS attempts,count(*)::bigint AS receipt_count,
    COALESCE(min(COALESCE(r.primary_code,r.terminal_failure_code)),'')::text AS primary_code,COALESCE(min(r.cleanup_code),'')::text AS cleanup_code,
    COALESCE(bool_or(r.state IN ('unresolved','exhausted')),FALSE)::boolean AS retry_unavailable
FROM /* TEMPLATE: schema */remote_write_receipts r
WHERE r.user_id = sqlc.arg(user_id)::bigint AND r.job_id = sqlc.arg(job_id)::bigint
GROUP BY r.job_id,r.user_id;

-- name: GetChannelDeletionIntent :one
SELECT * FROM /* TEMPLATE: schema */channel_deletion_intents WHERE channel_id = sqlc.arg(channel_id)::bigint;

-- name: InsertChannelDeletionIntent :one
INSERT INTO /* TEMPLATE: schema */channel_deletion_intents(channel_id,intent_id,requested_by)
VALUES (sqlc.arg(channel_id)::bigint,sqlc.arg(intent_id)::uuid,sqlc.arg(requested_by)::bigint)
RETURNING *;

-- name: FinalizeChannelDeletionIntent :execrows
UPDATE /* TEMPLATE: schema */channel_deletion_intents
SET state = 'deleted',updated_at = clock_timestamp()
WHERE channel_id = sqlc.arg(channel_id)::bigint AND intent_id = sqlc.arg(intent_id)::uuid AND state = 'closing';

-- name: RecordChannelDeletionFailure :execrows
UPDATE /* TEMPLATE: schema */channel_deletion_intents
SET failure_code = sqlc.arg(failure_code)::text,updated_at = clock_timestamp()
WHERE channel_id = sqlc.arg(channel_id)::bigint AND intent_id = sqlc.arg(intent_id)::uuid AND state = 'closing';

-- name: LockCopyOperation :one
SELECT * FROM /* TEMPLATE: schema */remote_write_receipts
WHERE id = sqlc.arg(operation_id)::uuid AND user_id = sqlc.arg(user_id)::bigint AND row_kind = 'copy_operation'
FOR UPDATE;

-- name: LockUploadOriginBatch :exec
SELECT b.batch_id FROM /* TEMPLATE: schema */import_batches b
WHERE b.user_id = sqlc.arg(user_id)::bigint AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */import_job_attempts a
    WHERE a.user_id = b.user_id AND a.batch_id = b.batch_id AND a.upload_id = sqlc.arg(upload_id)::uuid)
ORDER BY b.batch_id FOR UPDATE;

-- name: LockUploadOriginPins :exec
SELECT p.destination_ordinal FROM /* TEMPLATE: schema */import_destination_pins p
WHERE p.user_id = sqlc.arg(user_id)::bigint AND EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */import_job_attempts a
    WHERE a.user_id = p.user_id AND a.batch_id = p.batch_id AND a.upload_id = sqlc.arg(upload_id)::uuid)
ORDER BY p.batch_id,p.destination_ordinal FOR UPDATE;

-- name: LockUploadOriginAttempt :exec
SELECT a.attempt_id FROM /* TEMPLATE: schema */import_job_attempts a
WHERE a.user_id = sqlc.arg(user_id)::bigint AND a.upload_id = sqlc.arg(upload_id)::uuid
ORDER BY a.attempt_id FOR UPDATE;
