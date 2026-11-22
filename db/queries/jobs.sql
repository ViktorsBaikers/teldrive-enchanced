-- name: ListDeletionPendingRoots :many
SELECT f.user_id, f.id AS file_id, f.created_at
FROM /* TEMPLATE: schema */files f
WHERE f.status = 'deletion_pending'
  AND f.created_at <= sqlc.arg(pass_cutoff)::timestamptz
  AND (
    (sqlc.narg(after_created_at)::timestamptz IS NULL AND sqlc.narg(after_id)::uuid IS NULL)
    OR (f.created_at, f.id) > (sqlc.narg(after_created_at)::timestamptz, sqlc.narg(after_id)::uuid)
  )
  AND (
    f.parent_id IS NULL
    OR NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */files parent
      WHERE parent.id = f.parent_id
        AND parent.user_id = f.user_id
        AND parent.status = 'deletion_pending'
    )
  )
ORDER BY f.created_at, f.id
LIMIT 1000;

-- name: ListTrashedRootsBefore :many
SELECT f.user_id, f.id AS file_id, f.created_at
FROM /* TEMPLATE: schema */files f
WHERE f.status = 'trashed'
  AND f.deleted_at IS NOT NULL
  AND f.deleted_at <= sqlc.arg(deleted_before)
  AND f.created_at <= sqlc.arg(pass_cutoff)::timestamptz
  AND (
    (sqlc.narg(after_created_at)::timestamptz IS NULL AND sqlc.narg(after_id)::uuid IS NULL)
    OR (f.created_at, f.id) > (sqlc.narg(after_created_at)::timestamptz, sqlc.narg(after_id)::uuid)
  )
  AND (
    f.parent_id IS NULL
    OR NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */files parent
      WHERE parent.id = f.parent_id
        AND parent.user_id = f.user_id
        AND parent.status = 'trashed'
    )
  )
ORDER BY f.created_at, f.id
LIMIT 1000;
