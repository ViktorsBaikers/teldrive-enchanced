-- name: GetFileForUser :one
SELECT *
FROM /* TEMPLATE: schema */files
WHERE id = sqlc.arg(file_id)
  AND user_id = sqlc.arg(user_id);

-- name: GetPublicFileByID :one
SELECT *
FROM /* TEMPLATE: schema */files
WHERE id = sqlc.arg(file_id)
  AND kind = 'file'
  AND status = 'active';

-- name: CountBookmarkableFiles :one
SELECT count(DISTINCT file.id)::integer
FROM /* TEMPLATE: schema */files AS file
WHERE file.user_id = sqlc.arg(user_id)
  AND file.id = ANY(sqlc.arg(file_ids)::uuid[])
  AND file.status = 'active';

-- name: AddFileBookmarks :exec
INSERT INTO /* TEMPLATE: schema */file_bookmarks (user_id, file_id)
SELECT sqlc.arg(user_id), input.file_id
FROM unnest(sqlc.arg(file_ids)::uuid[]) AS input(file_id)
ON CONFLICT (user_id, file_id) DO NOTHING;

-- name: RemoveFileBookmarks :exec
DELETE FROM /* TEMPLATE: schema */file_bookmarks
WHERE user_id = sqlc.arg(user_id)
  AND file_id = ANY(sqlc.arg(file_ids)::uuid[]);

-- name: GetActiveFolderForUser :one
SELECT *
FROM /* TEMPLATE: schema */files
WHERE id = sqlc.arg(folder_id)
  AND user_id = sqlc.arg(user_id)
  AND kind = 'folder'
  AND status = 'active';

-- name: ListFiles :many
SELECT *
FROM /* TEMPLATE: schema */files
WHERE files.user_id = sqlc.arg(user_id)
  AND (
    files.parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid
    OR (
      sqlc.arg(status)::/* TEMPLATE: schema */file_status = 'trashed'
      AND sqlc.narg(parent_id)::uuid IS NULL
      AND files.parent_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM /* TEMPLATE: schema */files parent
        WHERE parent.id = files.parent_id
          AND parent.user_id = files.user_id
          AND parent.status = 'trashed'
      )
    )
  )
  AND files.status = sqlc.arg(status)::/* TEMPLATE: schema */file_status
  AND (sqlc.narg(kind)::/* TEMPLATE: schema */file_kind IS NULL OR files.kind = sqlc.narg(kind)::/* TEMPLATE: schema */file_kind)
  AND (
    sqlc.narg(search)::text IS NULL
    OR (sqlc.narg(search)::text !~ '[[:space:]]' AND files.name % sqlc.narg(search)::text)
    OR files.name ILIKE '%' || sqlc.narg(search)::text || '%'
    OR (
      -- Seed candidates through files_name_search_idx before checking every term.
      files.name ILIKE (
        SELECT '%' || term.value || '%'
        FROM regexp_split_to_table(trim(sqlc.narg(search)::text), '[[:space:]]+') AS term(value)
        ORDER BY length(term.value) DESC
        LIMIT 1
      )
      AND NOT EXISTS (
        SELECT 1
        FROM regexp_split_to_table(trim(sqlc.narg(search)::text), '[[:space:]]+') AS term(value)
        WHERE files.name NOT ILIKE '%' || term.value || '%'
      )
    )
  )
  AND (
    sqlc.narg(after_name)::text IS NULL
    OR (name COLLATE /* TEMPLATE: schema */natural_name, id) > (sqlc.narg(after_name)::text COLLATE /* TEMPLATE: schema */natural_name, sqlc.narg(after_id)::uuid)
  )
ORDER BY name COLLATE /* TEMPLATE: schema */natural_name, id
LIMIT sqlc.arg(page_size);

-- name: CreateFolder :one
INSERT INTO /* TEMPLATE: schema */files (
    id,
    user_id,
    parent_id,
    name,
    kind,
    mime_type,
    size,
    encryption,
    status,
    mod_time
) VALUES (
    sqlc.arg(id),
    sqlc.arg(user_id),
    sqlc.narg(parent_id),
    sqlc.arg(name),
    'folder',
    'inode/directory',
    0,
    false,
    'active',
    sqlc.arg(mod_time)
)
RETURNING *;

-- name: UpdateFileMetadata :one
UPDATE /* TEMPLATE: schema */files
SET name = COALESCE(sqlc.narg(name), name),
    mod_time = COALESCE(sqlc.narg(mod_time), mod_time),
    generation = generation + 1,
    updated_at = now()
WHERE id = sqlc.arg(file_id)
  AND user_id = sqlc.arg(user_id)
  AND status = 'active'
  AND (
    sqlc.narg(expected_generation)::bigint IS NULL
    OR generation = sqlc.narg(expected_generation)::bigint
  )
RETURNING *;

-- name: MoveFile :one
UPDATE /* TEMPLATE: schema */files
SET parent_id = sqlc.narg(parent_id),
    generation = generation + 1,
    updated_at = now()
WHERE id = sqlc.arg(file_id)
  AND user_id = sqlc.arg(user_id)
  AND status = 'active'
  AND (
    sqlc.narg(expected_generation)::bigint IS NULL
    OR generation = sqlc.narg(expected_generation)::bigint
  )
RETURNING *;

-- name: TrashFile :one
UPDATE /* TEMPLATE: schema */files
SET status = 'trashed',
    deleted_at = now(),
    generation = generation + 1,
    updated_at = now()
WHERE id = sqlc.arg(file_id)
  AND user_id = sqlc.arg(user_id)
  AND status = 'active'
RETURNING *;

-- name: RestoreFileSubtree :many
WITH RECURSIVE target AS (
  SELECT root.id
  FROM /* TEMPLATE: schema */files root
  WHERE root.id = sqlc.arg(file_id)
    AND root.user_id = sqlc.arg(user_id)
    AND root.status = 'trashed'
    AND (
      root.parent_id IS NULL
      OR EXISTS (
        SELECT 1
        FROM /* TEMPLATE: schema */files parent
        WHERE parent.id = root.parent_id
          AND parent.user_id = root.user_id
          AND parent.status = 'active'
      )
    )
  UNION ALL
  SELECT child.id
  FROM /* TEMPLATE: schema */files child
  JOIN target parent ON child.parent_id = parent.id
  WHERE child.user_id = sqlc.arg(user_id)
    AND child.status = 'trashed'
)
UPDATE /* TEMPLATE: schema */files AS target_file
SET status = 'active',
    deleted_at = NULL,
    generation = target_file.generation + 1,
    updated_at = now()
WHERE target_file.user_id = sqlc.arg(user_id)
  AND target_file.id IN (SELECT target.id FROM target)
RETURNING target_file.*;

-- name: MarkFileDeletionPending :one
UPDATE /* TEMPLATE: schema */files
SET status = 'deletion_pending',
    deleted_at = COALESCE(deleted_at, now()),
    generation = generation + 1,
    updated_at = now()
WHERE id = sqlc.arg(file_id)
  AND user_id = sqlc.arg(user_id)
  AND status = 'trashed'
RETURNING *;

-- name: DeleteFileCatalogRowsByIDs :execrows
DELETE FROM /* TEMPLATE: schema */files AS target
WHERE target.id = ANY(sqlc.arg(file_ids)::uuid[])
  AND target.user_id = sqlc.arg(user_id)
  AND target.status = 'deletion_pending'
  AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */file_parts p WHERE p.file_id=target.id)
  AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */files child WHERE child.parent_id=target.id)
  AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */upload_sessions s WHERE s.parent_id=target.id);

-- Recursive move-cycle validation will be implemented as a hand-reviewed query in the file service.

-- name: ListFileParts :many
SELECT *
FROM /* TEMPLATE: schema */file_parts
WHERE file_id = sqlc.arg(file_id)
ORDER BY part_no;

-- name: ListFilePartsByFileIDs :many
SELECT *
FROM /* TEMPLATE: schema */file_parts
WHERE file_id = ANY(sqlc.arg(file_ids)::uuid[])
ORDER BY file_id, part_no;

-- name: SumFilePartSizes :one
SELECT
    COALESCE(sum(plain_size), 0)::bigint AS plain_size,
    COALESCE(sum(stored_size), 0)::bigint AS stored_size,
    count(*)::integer AS part_count
FROM /* TEMPLATE: schema */file_parts
WHERE file_id = sqlc.arg(file_id);

-- name: ResolveActiveChildFolder :one
SELECT id
FROM /* TEMPLATE: schema */files
WHERE user_id = sqlc.arg(user_id)
  AND parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid
  AND name = sqlc.arg(name)
  AND kind = 'folder'
  AND status = 'active';

-- name: ResolveActiveChild :one
SELECT *
FROM /* TEMPLATE: schema */files
WHERE user_id = sqlc.arg(user_id)
  AND parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid
  AND name = sqlc.arg(name)
  AND status = 'active';

-- name: ListFilesAdvanced :many
WITH RECURSIVE scope_files AS (
  SELECT root.id
  FROM /* TEMPLATE: schema */files root
  WHERE sqlc.arg(scope)::text IN ('drive', 'recursive')
    AND sqlc.narg(scope_folder)::uuid IS NULL
    AND root.user_id = sqlc.arg(user_id) AND root.parent_id IS NULL
  UNION ALL
  SELECT child.id
  FROM /* TEMPLATE: schema */files child
  JOIN /* TEMPLATE: schema */files selected ON selected.id = sqlc.narg(scope_folder)::uuid
  WHERE sqlc.arg(scope)::text = 'recursive'
    AND selected.user_id = sqlc.arg(user_id)
    AND selected.kind = 'folder' AND selected.status = 'active'
    AND child.parent_id = selected.id AND child.user_id = sqlc.arg(user_id)
  UNION ALL
  SELECT child.id
  FROM /* TEMPLATE: schema */files child
  JOIN scope_files parent ON child.parent_id = parent.id
  WHERE sqlc.arg(scope)::text IN ('drive', 'recursive')
    AND child.user_id = sqlc.arg(user_id)
)
SELECT f.*
FROM /* TEMPLATE: schema */files f
WHERE f.user_id = sqlc.arg(user_id)
  AND (
    (sqlc.arg(scope)::text = 'folder' AND f.parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid)
    OR (sqlc.arg(scope)::text <> 'folder' AND f.id IN (SELECT id FROM scope_files))
    OR (
      sqlc.arg(status)::/* TEMPLATE: schema */file_status = 'trashed'
      AND sqlc.narg(parent_id)::uuid IS NULL
      AND f.parent_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM /* TEMPLATE: schema */files parent
        WHERE parent.id = f.parent_id
          AND parent.user_id = f.user_id
          AND parent.status = 'trashed'
      )
    )
  )
  AND f.status = sqlc.arg(status)::/* TEMPLATE: schema */file_status
  AND (
    NOT sqlc.arg(bookmarked)::boolean
    OR EXISTS (
      SELECT 1 FROM /* TEMPLATE: schema */file_bookmarks bookmark
      WHERE bookmark.user_id = f.user_id AND bookmark.file_id = f.id
    )
  )
  AND (sqlc.narg(kind)::/* TEMPLATE: schema */file_kind IS NULL OR f.kind = sqlc.narg(kind)::/* TEMPLATE: schema */file_kind)
  AND (
    sqlc.narg(search)::text IS NULL
    OR (sqlc.arg(search_type)::text = 'regex' AND f.name ~* sqlc.narg(search)::text)
    OR (
      sqlc.arg(search_type)::text = 'text'
      AND (
        (sqlc.narg(search)::text !~ '[[:space:]]' AND f.name % sqlc.narg(search)::text)
        OR f.name ILIKE '%' || sqlc.narg(search)::text || '%'
        OR (
          -- Seed candidates through files_name_search_idx before checking every term.
          f.name ILIKE (
            SELECT '%' || term.value || '%'
            FROM regexp_split_to_table(trim(sqlc.narg(search)::text), '[[:space:]]+') AS term(value)
            ORDER BY length(term.value) DESC
            LIMIT 1
          )
          AND NOT EXISTS (
            SELECT 1
            FROM regexp_split_to_table(trim(sqlc.narg(search)::text), '[[:space:]]+') AS term(value)
            WHERE f.name NOT ILIKE '%' || term.value || '%'
          )
        )
      )
    )
  )
  AND (
    cardinality(sqlc.arg(categories)::text[]) = 0
    OR (CASE
      WHEN f.kind = 'folder' THEN 'other'
      WHEN lower(COALESCE(f.mime_type, '')) LIKE 'image/%' THEN 'image'
      WHEN lower(COALESCE(f.mime_type, '')) LIKE 'audio/%' THEN 'audio'
      WHEN lower(COALESCE(f.mime_type, '')) LIKE 'video/%' THEN 'video'
      WHEN lower(COALESCE(f.mime_type, '')) LIKE 'text/%'
        OR lower(COALESCE(f.mime_type, '')) IN (
          'application/pdf', 'application/json', 'application/xml',
          'application/msword', 'application/rtf',
          'application/vnd.ms-excel', 'application/vnd.ms-powerpoint',
          'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          'application/vnd.openxmlformats-officedocument.presentationml.presentation'
        ) THEN 'document'
      WHEN lower(COALESCE(f.mime_type, '')) IN (
          'application/zip', 'application/x-rar-compressed', 'application/x-7z-compressed',
          'application/x-tar', 'application/gzip', 'application/x-bzip2', 'application/x-xz'
        ) OR lower(f.name) ~ '\.(zip|rar|7z|tar|gz|tgz|bz2|xz)$' THEN 'archive'
      ELSE 'other'
    END) = ANY(sqlc.arg(categories)::text[])
  )
  AND (sqlc.narg(updated_after)::timestamptz IS NULL OR f.updated_at >= sqlc.narg(updated_after)::timestamptz)
  AND (sqlc.narg(updated_before)::timestamptz IS NULL OR f.updated_at < sqlc.narg(updated_before)::timestamptz)
  AND (
    sqlc.narg(after_id)::uuid IS NULL
    OR (
      sqlc.arg(sort_by)::text = 'name'
      AND sqlc.narg(after_name)::text IS NOT NULL
      AND (
        (sqlc.arg(sort_order)::text = 'asc' AND (f.name COLLATE /* TEMPLATE: schema */natural_name, f.id) > (sqlc.narg(after_name)::text COLLATE /* TEMPLATE: schema */natural_name, sqlc.narg(after_id)::uuid))
        OR (sqlc.arg(sort_order)::text = 'desc' AND (f.name COLLATE /* TEMPLATE: schema */natural_name, f.id) < (sqlc.narg(after_name)::text COLLATE /* TEMPLATE: schema */natural_name, sqlc.narg(after_id)::uuid))
      )
    )
    OR (
      sqlc.arg(sort_by)::text = 'updatedAt'
      AND sqlc.narg(after_updated_at)::timestamptz IS NOT NULL
      AND (
        (sqlc.arg(sort_order)::text = 'asc' AND (f.updated_at, f.id) > (sqlc.narg(after_updated_at)::timestamptz, sqlc.narg(after_id)::uuid))
        OR (sqlc.arg(sort_order)::text = 'desc' AND (f.updated_at, f.id) < (sqlc.narg(after_updated_at)::timestamptz, sqlc.narg(after_id)::uuid))
      )
    )
    OR (
      sqlc.arg(sort_by)::text = 'size'
      AND sqlc.narg(after_size)::bigint IS NOT NULL
      AND (
        (sqlc.arg(sort_order)::text = 'asc' AND (COALESCE(f.size, -1), f.id) > (sqlc.narg(after_size)::bigint, sqlc.narg(after_id)::uuid))
        OR (sqlc.arg(sort_order)::text = 'desc' AND (COALESCE(f.size, -1), f.id) < (sqlc.narg(after_size)::bigint, sqlc.narg(after_id)::uuid))
      )
    )
    OR (
      sqlc.arg(sort_by)::text = 'id'
      AND (
        (sqlc.arg(sort_order)::text = 'asc' AND f.id > sqlc.narg(after_id)::uuid)
        OR (sqlc.arg(sort_order)::text = 'desc' AND f.id < sqlc.narg(after_id)::uuid)
      )
    )
  )
ORDER BY
  CASE WHEN sqlc.arg(sort_by)::text = 'name' AND sqlc.arg(sort_order)::text = 'asc' THEN f.name END COLLATE /* TEMPLATE: schema */natural_name ASC,
  CASE WHEN sqlc.arg(sort_by)::text = 'name' AND sqlc.arg(sort_order)::text = 'desc' THEN f.name END COLLATE /* TEMPLATE: schema */natural_name DESC,
  CASE WHEN sqlc.arg(sort_by)::text = 'updatedAt' AND sqlc.arg(sort_order)::text = 'asc' THEN f.updated_at END ASC,
  CASE WHEN sqlc.arg(sort_by)::text = 'updatedAt' AND sqlc.arg(sort_order)::text = 'desc' THEN f.updated_at END DESC,
  CASE WHEN sqlc.arg(sort_by)::text = 'size' AND sqlc.arg(sort_order)::text = 'asc' THEN COALESCE(f.size, -1) END ASC,
  CASE WHEN sqlc.arg(sort_by)::text = 'size' AND sqlc.arg(sort_order)::text = 'desc' THEN COALESCE(f.size, -1) END DESC,
  CASE WHEN sqlc.arg(sort_by)::text = 'id' AND sqlc.arg(sort_order)::text = 'asc' THEN f.id END ASC,
  CASE WHEN sqlc.arg(sort_by)::text = 'id' AND sqlc.arg(sort_order)::text = 'desc' THEN f.id END DESC,
  CASE WHEN sqlc.arg(sort_order)::text = 'asc' THEN f.id END ASC,
  CASE WHEN sqlc.arg(sort_order)::text = 'desc' THEN f.id END DESC
LIMIT sqlc.arg(page_size);

-- name: ListFileParentPaths :many
WITH RECURSIVE ancestors AS (
  SELECT f.id AS listed_id, f.parent_id AS ancestor_id, 0 AS depth
  FROM /* TEMPLATE: schema */files f
  WHERE f.user_id = sqlc.arg(user_id) AND f.id = ANY(sqlc.arg(file_ids)::uuid[])
  UNION ALL
  SELECT a.listed_id, parent.parent_id, a.depth + 1
  FROM ancestors a
  JOIN /* TEMPLATE: schema */files parent ON parent.id = a.ancestor_id
  WHERE parent.user_id = sqlc.arg(user_id)
)
SELECT a.listed_id AS file_id,
       COALESCE('/' || string_agg(node.name, '/' ORDER BY a.depth DESC) FILTER (WHERE node.id IS NOT NULL), '/')::text AS parent_path
FROM ancestors a
LEFT JOIN /* TEMPLATE: schema */files node ON node.id = a.ancestor_id AND node.user_id = sqlc.arg(user_id)
GROUP BY a.listed_id;

-- name: ListFileCategoryStatistics :many
SELECT category, count(*)::bigint AS total_files, COALESCE(sum(size), 0)::bigint AS total_size
FROM (
  SELECT CASE
    WHEN lower(COALESCE(f.mime_type, '')) LIKE 'image/%' THEN 'image'
    WHEN lower(COALESCE(f.mime_type, '')) LIKE 'audio/%' THEN 'audio'
    WHEN lower(COALESCE(f.mime_type, '')) LIKE 'video/%' THEN 'video'
    WHEN lower(COALESCE(f.mime_type, '')) LIKE 'text/%'
      OR lower(COALESCE(f.mime_type, '')) IN (
        'application/pdf', 'application/json', 'application/xml',
        'application/msword', 'application/rtf',
        'application/vnd.ms-excel', 'application/vnd.ms-powerpoint',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation'
      ) THEN 'document'
    WHEN lower(COALESCE(f.mime_type, '')) IN (
        'application/zip', 'application/x-rar-compressed', 'application/x-7z-compressed',
        'application/x-tar', 'application/gzip', 'application/x-bzip2', 'application/x-xz'
      ) OR lower(f.name) ~ '\.(zip|rar|7z|tar|gz|tgz|bz2|xz)$' THEN 'archive'
    ELSE 'other'
  END AS category, f.size
  FROM /* TEMPLATE: schema */files f
  WHERE f.user_id = sqlc.arg(user_id)
    AND f.kind = 'file'
    AND f.status = 'active'
) categorized
GROUP BY category
ORDER BY category;

-- name: GetDriveStatistics :one
SELECT
  count(*) FILTER (WHERE kind = 'file' AND status = 'active')::bigint AS total_files,
  count(*) FILTER (WHERE kind = 'folder' AND status = 'active')::bigint AS total_folders,
  COALESCE(sum(size) FILTER (WHERE kind = 'file' AND status = 'active'), 0)::bigint AS total_bytes,
  count(*) FILTER (WHERE status = 'trashed')::bigint AS trashed_files,
  (SELECT count(*)::bigint FROM /* TEMPLATE: schema */file_shares WHERE owner_id = sqlc.arg(user_id) AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > now())) AS active_shares,
  (SELECT count(*)::bigint FROM /* TEMPLATE: schema */upload_sessions WHERE user_id = sqlc.arg(user_id) AND state IN ('open', 'completing')) AS open_uploads
FROM /* TEMPLATE: schema */files
WHERE user_id = sqlc.arg(user_id);

-- name: LockActiveFiles :many
SELECT *
FROM /* TEMPLATE: schema */files
WHERE user_id = sqlc.arg(user_id)
  AND id = ANY(sqlc.arg(file_ids)::uuid[])
  AND status = 'active'
FOR UPDATE;

-- name: LockActiveFolder :one
SELECT *
FROM /* TEMPLATE: schema */files
WHERE id = sqlc.arg(folder_id)
  AND user_id = sqlc.arg(user_id)
  AND kind = 'folder'
  AND status = 'active'
FOR UPDATE;

-- name: LockActiveDestinationEntries :many
SELECT id, name
FROM /* TEMPLATE: schema */files
WHERE user_id = sqlc.arg(user_id)
  AND parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid
  AND status = 'active'
FOR UPDATE;

-- name: ListFileAncestorIDs :many
WITH RECURSIVE ancestors AS (
  SELECT file.id, file.parent_id
  FROM /* TEMPLATE: schema */files AS file
  WHERE file.id = sqlc.arg(file_id)
    AND file.user_id = sqlc.arg(user_id)
  UNION ALL
  SELECT parent.id, parent.parent_id
  FROM /* TEMPLATE: schema */files AS parent
  JOIN ancestors AS child ON parent.id = child.parent_id
  WHERE parent.user_id = sqlc.arg(user_id)
)
SELECT id FROM ancestors;

-- name: ListFileSubtreeIDs :many
WITH RECURSIVE subtree AS (
  SELECT root.id FROM /* TEMPLATE: schema */files root WHERE root.id = sqlc.arg(file_id) AND root.user_id = sqlc.arg(user_id)
  UNION ALL
  SELECT f.id FROM /* TEMPLATE: schema */files f JOIN subtree s ON f.parent_id = s.id WHERE f.user_id = sqlc.arg(user_id)
)
SELECT id FROM subtree;

-- name: TrashFileSubtrees :many
WITH RECURSIVE target AS (
  SELECT root.id
  FROM /* TEMPLATE: schema */files root
  WHERE root.user_id = sqlc.arg(user_id)
    AND root.id = ANY(sqlc.arg(file_ids)::uuid[])
    AND root.status = 'active'
  UNION
  SELECT child.id
  FROM /* TEMPLATE: schema */files child
  JOIN target parent ON child.parent_id = parent.id
  WHERE child.user_id = sqlc.arg(user_id)
    AND child.status = 'active'
)
UPDATE /* TEMPLATE: schema */files AS target_file
SET status = 'trashed',
    deleted_at = now(),
    generation = target_file.generation + 1,
    updated_at = now()
WHERE target_file.user_id = sqlc.arg(user_id)
  AND target_file.id IN (SELECT target.id FROM target)
RETURNING target_file.*;

-- name: RevokeSharesForFileSubtrees :exec
WITH RECURSIVE target AS (
  SELECT root.id FROM /* TEMPLATE: schema */files root WHERE root.user_id = sqlc.arg(user_id) AND root.id = ANY(sqlc.arg(file_ids)::uuid[])
  UNION
  SELECT child.id FROM /* TEMPLATE: schema */files child JOIN target parent ON child.parent_id = parent.id WHERE child.user_id = sqlc.arg(user_id)
)
UPDATE /* TEMPLATE: schema */file_shares AS share
SET revoked_at = COALESCE(share.revoked_at, now())
WHERE share.owner_id = sqlc.arg(user_id)
  AND share.revoked_at IS NULL
  AND share.file_id IN (SELECT target.id FROM target);

-- name: MarkFileSubtreeDeletionPending :exec
WITH RECURSIVE target AS (
  SELECT root.id FROM /* TEMPLATE: schema */files root WHERE root.id = sqlc.arg(file_id) AND root.user_id = sqlc.arg(user_id) AND root.status = 'active'
  UNION ALL
  SELECT child.id FROM /* TEMPLATE: schema */files child JOIN target parent ON child.parent_id = parent.id WHERE child.user_id = sqlc.arg(user_id) AND child.status = 'active'
)
UPDATE /* TEMPLATE: schema */files AS target_file
SET status = 'deletion_pending',
    deleted_at = COALESCE(target_file.deleted_at, now()),
    generation = target_file.generation + 1,
    updated_at = now()
WHERE target_file.user_id = sqlc.arg(user_id) AND target_file.id IN (SELECT target.id FROM target);

-- name: MarkFileSubtreesDeletionPending :exec
WITH RECURSIVE target AS (
  SELECT root.id
  FROM /* TEMPLATE: schema */files AS root
  WHERE root.id = ANY(sqlc.arg(file_ids)::uuid[])
    AND root.user_id = sqlc.arg(user_id)
    AND root.status = 'active'
  UNION
  SELECT child.id
  FROM /* TEMPLATE: schema */files AS child
  JOIN target AS parent ON child.parent_id = parent.id
  WHERE child.user_id = sqlc.arg(user_id)
    AND child.status = 'active'
)
UPDATE /* TEMPLATE: schema */files AS target_file
SET status = 'deletion_pending',
    deleted_at = COALESCE(target_file.deleted_at, now()),
    generation = target_file.generation + 1,
    updated_at = now()
WHERE target_file.user_id = sqlc.arg(user_id)
  AND target_file.id IN (SELECT target.id FROM target);

-- name: RevokeSharesForFileSubtree :exec
WITH RECURSIVE target AS (
  SELECT root.id FROM /* TEMPLATE: schema */files root WHERE root.id = sqlc.arg(file_id) AND root.user_id = sqlc.arg(user_id)
  UNION ALL
  SELECT child.id FROM /* TEMPLATE: schema */files child JOIN target parent ON child.parent_id = parent.id WHERE child.user_id = sqlc.arg(user_id)
)
UPDATE /* TEMPLATE: schema */file_shares AS share
SET revoked_at = COALESCE(share.revoked_at, now())
WHERE share.owner_id = sqlc.arg(user_id)
  AND share.revoked_at IS NULL
  AND share.file_id IN (SELECT target.id FROM target);

-- name: ListActiveNames :many
SELECT name
FROM /* TEMPLATE: schema */files
WHERE user_id = sqlc.arg(user_id)
  AND parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid
  AND status = 'active'
  AND (sqlc.narg(exclude_id)::uuid IS NULL OR id <> sqlc.narg(exclude_id)::uuid);

-- name: MoveFilesWithNames :many
WITH arrays AS (
  SELECT sqlc.arg(file_ids)::uuid[] AS file_ids,
         sqlc.arg(names)::text[] AS names
), input AS (
  SELECT arrays.file_ids[index] AS file_id,
         arrays.names[index] AS name
  FROM arrays
  CROSS JOIN LATERAL generate_subscripts(arrays.file_ids, 1) AS index
)
UPDATE /* TEMPLATE: schema */files AS file
SET parent_id = sqlc.narg(parent_id),
    name = input.name,
    generation = file.generation + 1,
    updated_at = now()
FROM input
WHERE file.id = input.file_id
  AND file.user_id = sqlc.arg(user_id)
  AND file.status = 'active'
  AND (sqlc.narg(expected_generation)::bigint IS NULL OR file.generation = sqlc.narg(expected_generation)::bigint)
RETURNING file.*;

-- name: LoadFileSubtree :many
WITH RECURSIVE tree AS (
    SELECT f.*, 0::integer AS depth
    FROM /* TEMPLATE: schema */files f
    WHERE f.id = sqlc.arg(root_id) AND f.user_id = sqlc.arg(user_id)
    UNION ALL
    SELECT child.*, tree.depth + 1
    FROM /* TEMPLATE: schema */files child
    JOIN tree ON child.parent_id = tree.id
    WHERE child.user_id = sqlc.arg(user_id)
)
SELECT id, user_id, parent_id, name, kind, mime_type, size,
       hash_algorithm, hash_value, encryption, encryption_key_version, status,
       mod_time, generation, created_at, updated_at, deleted_at, depth
FROM tree
ORDER BY depth, id;

-- name: LoadFileSubtrees :many
WITH RECURSIVE tree AS (
    SELECT f.*, 0::integer AS depth
    FROM /* TEMPLATE: schema */files f
    WHERE f.id = ANY(sqlc.arg(root_ids)::uuid[])
      AND f.user_id = sqlc.arg(user_id)
    UNION ALL
    SELECT child.*, tree.depth + 1
    FROM /* TEMPLATE: schema */files child
    JOIN tree ON child.parent_id = tree.id
    WHERE child.user_id = sqlc.arg(user_id)
)
SELECT id, user_id, parent_id, name, kind, mime_type, size,
       hash_algorithm, hash_value, encryption, encryption_key_version, status,
       mod_time, generation, created_at, updated_at, deleted_at, depth
FROM tree
ORDER BY depth, id;

-- name: InsertCopiedFiles :many
INSERT INTO /* TEMPLATE: schema */files AS file (
    id, user_id, parent_id, name, kind, mime_type, size,
    hash_algorithm, hash_value, encryption, encryption_key_version,
    status, mod_time, generation
)
SELECT input.id, input.user_id, input.parent_id, input.name,
       input.kind::/* TEMPLATE: schema */file_kind, input.mime_type, input.size,
       input.hash_algorithm, input.hash_value, input.encryption,
       input.encryption_key_version, 'active', input.mod_time, 1
FROM jsonb_to_recordset(sqlc.arg(files)::jsonb) AS input(
    id uuid, user_id bigint, parent_id uuid, name text,
    kind text, mime_type text, size bigint, hash_algorithm text, hash_value text,
    encryption boolean, encryption_key_version integer, mod_time timestamptz
)
RETURNING file.*;

-- name: InsertCopiedFileParts :execrows
INSERT INTO /* TEMPLATE: schema */file_parts (
    file_id, part_no, channel_id, message_id, plain_size, stored_size,
    checksum, salt, block_hashes
)
SELECT input.file_id, input.part_no, input.channel_id, input.message_id,
       input.plain_size, input.stored_size, input.checksum, input.salt,
       decode(input.block_hashes, 'base64')
FROM jsonb_to_recordset(sqlc.arg(parts)::jsonb) AS input(
    file_id uuid, part_no integer, channel_id bigint, message_id bigint,
    plain_size bigint, stored_size bigint, checksum text, salt text,
    block_hashes text, receipt_id uuid
)
JOIN /* TEMPLATE: schema */remote_write_receipts receipt
  ON receipt.id=input.receipt_id AND receipt.row_kind='message_attempt'
 AND receipt.origin_kind='copy' AND receipt.result_file_id=input.file_id
 AND receipt.part_no=input.part_no AND receipt.channel_id=input.channel_id
 AND receipt.message_id=input.message_id AND receipt.state='bound'
JOIN /* TEMPLATE: schema */files result ON result.id=input.file_id AND result.user_id=receipt.user_id
JOIN /* TEMPLATE: schema */remote_write_receipts operation
  ON operation.id=receipt.operation_id AND operation.row_kind='copy_operation'
 AND operation.user_id=receipt.user_id AND operation.lease_token=receipt.lease_token
 AND operation.state='reserved' AND operation.terminal_at IS NULL AND operation.published_at IS NULL
 AND operation.lease_expires_at>clock_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */channel_deletion_intents gate WHERE gate.channel_id=input.channel_id)
 AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts fence
 WHERE fence.channel_id=input.channel_id AND fence.message_id=input.message_id
 AND fence.state IN ('cleanup_pending','deleting','deleted','exhausted','unresolved'));

-- name: MarkFileIDsDeletionPending :exec
UPDATE /* TEMPLATE: schema */files
SET status = 'deletion_pending',
    deleted_at = COALESCE(deleted_at, now()),
    updated_at = now()
WHERE user_id = sqlc.arg(user_id)
  AND id = ANY(sqlc.arg(file_ids)::uuid[])
  AND (status <> 'deletion_pending' OR deleted_at IS NULL);



-- name: MarkAllTrashedDeletionPending :execrows
UPDATE /* TEMPLATE: schema */files
SET status = 'deletion_pending',
    deleted_at = COALESCE(deleted_at, now()),
    updated_at = now()
WHERE user_id = sqlc.arg(user_id)
  AND status = 'trashed';

-- name: ListTrashedFileIDs :many
SELECT id FROM /* TEMPLATE: schema */files
WHERE user_id=sqlc.arg(user_id)::bigint AND status='trashed' ORDER BY id;

-- name: ListFilePurgeRoots :many
SELECT * FROM /* TEMPLATE: schema */files
WHERE user_id=sqlc.arg(user_id)::bigint AND id=ANY(sqlc.arg(file_ids)::uuid[])
ORDER BY id;

-- name: ListPurgeReceiptIDs :many
SELECT DISTINCT r.id FROM /* TEMPLATE: schema */remote_write_receipts r
JOIN /* TEMPLATE: schema */file_parts p
 ON p.file_id=r.result_file_id AND p.part_no=r.part_no
 AND p.channel_id=r.channel_id AND p.message_id=r.message_id
JOIN /* TEMPLATE: schema */files f ON f.id=p.file_id AND f.user_id=r.user_id
WHERE r.user_id=sqlc.arg(user_id)::bigint AND f.status='deletion_pending'
 AND f.id=ANY(sqlc.arg(file_ids)::uuid[]) AND r.row_kind='message_attempt'
ORDER BY r.id;

-- name: DeleteAcknowledgedFileParts :exec
DELETE FROM /* TEMPLATE: schema */file_parts p
USING /* TEMPLATE: schema */files f, /* TEMPLATE: schema */remote_write_receipts r
WHERE p.file_id=f.id AND f.user_id=sqlc.arg(user_id)::bigint
 AND f.id=ANY(sqlc.arg(file_ids)::uuid[]) AND f.status='deletion_pending'
 AND r.row_kind='message_attempt' AND r.user_id=f.user_id AND r.result_file_id=f.id
 AND r.part_no=p.part_no AND r.channel_id=p.channel_id AND r.message_id=p.message_id AND r.state='deleted'
 AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */lifecycle_receipt_status status
 WHERE status.id=r.id AND status.has_references);

-- name: GetPurgeRetentionSummary :one
SELECT count(*) FILTER (WHERE r.id IS NULL)::bigint AS legacy_unknown,
 count(*) FILTER (WHERE status.has_references)::bigint AS protected,
 count(*) FILTER (WHERE r.state NOT IN ('deleted','exhausted'))::bigint AS pending,
 count(*) FILTER (WHERE r.state='exhausted')::bigint AS exhausted
FROM /* TEMPLATE: schema */file_parts p
JOIN /* TEMPLATE: schema */files f ON f.id=p.file_id
LEFT JOIN /* TEMPLATE: schema */remote_write_receipts r
 ON r.row_kind='message_attempt' AND r.user_id=f.user_id AND r.result_file_id=f.id
 AND r.part_no=p.part_no AND r.channel_id=p.channel_id AND r.message_id=p.message_id
LEFT JOIN /* TEMPLATE: schema */lifecycle_receipt_status status ON status.id=r.id
WHERE f.user_id=sqlc.arg(user_id)::bigint AND f.id=ANY(sqlc.arg(file_ids)::uuid[]);

-- name: ClearUploadSessionParentsByFileIDs :exec
UPDATE /* TEMPLATE: schema */upload_sessions
SET parent_id = NULL,
    updated_at = now()
WHERE user_id = sqlc.arg(user_id)
  AND parent_id = ANY(sqlc.arg(file_ids)::uuid[])
  AND (state IN ('completed','aborted','expired') OR terminal_failure_code='upload_parent_unavailable')
  AND terminal_failure_code IS DISTINCT FROM 'upload_destination_unknown';

-- name: UpdateFilePartSizes :execrows
UPDATE /* TEMPLATE: schema */file_parts
SET plain_size = sqlc.arg(plain_size),
    stored_size = sqlc.arg(stored_size)
WHERE file_id = sqlc.arg(file_id)
  AND part_no = sqlc.arg(part_no)
  AND (plain_size IS NULL OR stored_size IS NULL);

-- name: UpdateFilePartSizesMany :execrows
UPDATE /* TEMPLATE: schema */file_parts AS part
SET plain_size = input.plain_size,
    stored_size = input.stored_size
FROM jsonb_to_recordset(sqlc.arg(parts)::jsonb) AS input(
    part_no integer, plain_size bigint, stored_size bigint
)
WHERE part.file_id = sqlc.arg(file_id)
  AND part.part_no = input.part_no
  AND (part.plain_size IS NULL OR part.stored_size IS NULL);



-- name: RollupFolderSizes :execrows
WITH RECURSIVE folder_hierarchy AS (
  SELECT id, id AS root_id
  FROM /* TEMPLATE: schema */files
  WHERE kind = 'folder'
  UNION ALL
  SELECT f.id, fh.root_id
  FROM /* TEMPLATE: schema */files f
  JOIN folder_hierarchy fh ON f.parent_id = fh.id
),
folder_totals AS (
  SELECT fh.root_id AS id, COALESCE(SUM(f.size), 0)::bigint AS total
  FROM folder_hierarchy fh
  JOIN /* TEMPLATE: schema */files f ON f.id = fh.id
  WHERE f.kind = 'file'
    AND f.status = 'active'
  GROUP BY fh.root_id
)
UPDATE /* TEMPLATE: schema */files AS folder
SET size = COALESCE(desired.total, 0)
FROM (
  SELECT f.id AS id, folder_totals.total AS total
  FROM /* TEMPLATE: schema */files AS f
  LEFT JOIN folder_totals ON folder_totals.id = f.id
  WHERE f.kind = 'folder'
) AS desired
WHERE folder.id = desired.id
  AND (folder.size IS NULL OR folder.size <> desired.total);

-- name: ListLifecycleSubtreeIDs :many
WITH RECURSIVE tree AS (
 SELECT id FROM /* TEMPLATE: schema */files
 WHERE user_id=sqlc.arg(user_id)::bigint AND id=ANY(sqlc.arg(root_ids)::uuid[])
 UNION
 SELECT f.id FROM /* TEMPLATE: schema */files f JOIN tree t ON f.parent_id=t.id
 WHERE f.user_id=sqlc.arg(user_id)::bigint
)
SELECT id FROM tree ORDER BY id;

-- name: ListImportBatchesAnchoredInSubtree :many
SELECT b.batch_id,b.origin_job_id FROM /* TEMPLATE: schema */import_batches b
WHERE b.user_id=sqlc.arg(user_id)::bigint AND NOT b.dispatched
 AND EXISTS (SELECT 1 FROM /* TEMPLATE: schema */import_destination_pins p
 WHERE p.user_id=b.user_id AND p.batch_id=b.batch_id AND NOT p.anchor_is_root
 AND p.anchor_id=ANY(sqlc.arg(subtree_ids)::uuid[]))
ORDER BY b.batch_id;

-- name: ListHierarchyImportBatches :many
SELECT b.batch_id FROM /* TEMPLATE: schema */import_batches b
WHERE b.user_id=sqlc.arg(user_id)::bigint AND (
 b.parent_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */import_job_attempts a
 WHERE a.user_id=b.user_id AND a.batch_id=b.batch_id
 AND (a.parent_id=ANY(sqlc.arg(subtree_ids)::uuid[]) OR a.upload_id=ANY(sqlc.arg(upload_ids)::uuid[]))))
ORDER BY b.batch_id;

-- name: LockHierarchyAdmissions :exec
SELECT id FROM /* TEMPLATE: schema */request_admissions
WHERE user_id=sqlc.arg(user_id)::bigint AND (upload_id=ANY(sqlc.arg(upload_ids)::uuid[])
 OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts op
 WHERE op.id=/* TEMPLATE: schema */request_admissions.copy_operation_id AND op.user_id=sqlc.arg(user_id)::bigint
 AND op.row_kind='copy_operation' AND (op.destination_parent_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 OR op.result_file_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts r
 WHERE r.operation_id=op.id AND r.user_id=op.user_id AND r.result_file_id=ANY(sqlc.arg(subtree_ids)::uuid[])))))
ORDER BY user_id,actor_user_id,public_share_id,request_kind,request_key FOR UPDATE;

-- name: LockImportBatchesForLifecycle :exec
SELECT batch_id FROM /* TEMPLATE: schema */import_batches
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=ANY(sqlc.arg(batch_ids)::uuid[])
ORDER BY user_id,batch_id FOR UPDATE;

-- name: LockImportDestinationPinsForLifecycle :exec
SELECT batch_id FROM /* TEMPLATE: schema */import_destination_pins
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=ANY(sqlc.arg(batch_ids)::uuid[])
ORDER BY user_id,batch_id,destination_ordinal FOR UPDATE;

-- name: LockImportAttemptsForLifecycle :exec
SELECT attempt_id FROM /* TEMPLATE: schema */import_job_attempts
WHERE user_id=sqlc.arg(user_id)::bigint AND batch_id=ANY(sqlc.arg(batch_ids)::uuid[])
ORDER BY attempt_id FOR UPDATE;

-- name: ListHierarchyUploadIDs :many
SELECT s.id FROM /* TEMPLATE: schema */upload_sessions s
WHERE s.user_id=sqlc.arg(user_id)::bigint AND
 (s.parent_id=ANY(sqlc.arg(subtree_ids)::uuid[]) OR s.id=ANY(sqlc.arg(upload_ids)::uuid[])
 OR s.file_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts r
 WHERE r.user_id=s.user_id AND r.upload_id=s.id AND r.result_file_id=ANY(sqlc.arg(subtree_ids)::uuid[])))
ORDER BY s.id;

-- name: ListHierarchyReceiptHints :many
SELECT r.* FROM /* TEMPLATE: schema */remote_write_receipts r
WHERE r.user_id=sqlc.arg(user_id)::bigint AND (
 r.upload_id=ANY(sqlc.arg(upload_ids)::uuid[]) OR r.result_file_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 OR (r.row_kind='copy_operation' AND r.destination_parent_id=ANY(sqlc.arg(subtree_ids)::uuid[]))
 OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts op
 WHERE op.id=r.operation_id AND op.user_id=r.user_id AND op.row_kind='copy_operation'
 AND (op.destination_parent_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 OR op.result_file_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts sibling
 WHERE sibling.operation_id=op.id AND sibling.user_id=op.user_id AND sibling.result_file_id=ANY(sqlc.arg(subtree_ids)::uuid[])))))
ORDER BY r.channel_id,r.message_id,r.id;

-- name: LockHierarchyCopyOperations :exec
SELECT op.id FROM /* TEMPLATE: schema */remote_write_receipts op
WHERE op.user_id=sqlc.arg(user_id)::bigint AND op.row_kind='copy_operation'
 AND (op.destination_parent_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 OR op.result_file_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */remote_write_receipts r
 WHERE r.operation_id=op.id AND r.row_kind='message_attempt'
 AND r.user_id=op.user_id AND r.result_file_id=ANY(sqlc.arg(subtree_ids)::uuid[])))
ORDER BY op.id FOR UPDATE;

-- name: LockHierarchyUploadSessions :exec
SELECT id FROM /* TEMPLATE: schema */upload_sessions
WHERE user_id=sqlc.arg(user_id)::bigint AND id=ANY(sqlc.arg(upload_ids)::uuid[])
ORDER BY id FOR UPDATE;

-- name: TerminalizeUndispatchedImportBatches :many
UPDATE /* TEMPLATE: schema */import_batches b
SET terminal_failure_code='upload_parent_unavailable'
WHERE b.user_id=sqlc.arg(user_id)::bigint AND NOT b.dispatched AND b.terminal_failure_code IS NULL
 AND EXISTS (SELECT 1 FROM /* TEMPLATE: schema */import_destination_pins p
 WHERE p.user_id=b.user_id AND p.batch_id=b.batch_id AND NOT p.anchor_is_root
 AND p.anchor_id=ANY(sqlc.arg(subtree_ids)::uuid[]))
RETURNING b.batch_id,b.origin_job_id;

-- name: TerminalizeImportDestinations :execrows
UPDATE /* TEMPLATE: schema */import_job_attempts a
SET terminal_failure_code='upload_parent_unavailable'
WHERE a.user_id=sqlc.arg(user_id)::bigint AND a.parent_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 AND a.terminal_failure_code IS NULL AND a.result_file_id IS NULL
 AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */upload_sessions s
 WHERE s.id=a.upload_id AND s.user_id=a.user_id AND s.state='completed');

-- name: TerminalizeDispatchedImportBatches :execrows
UPDATE /* TEMPLATE: schema */import_batches b
SET terminal_failure_code='upload_parent_unavailable'
WHERE b.user_id=sqlc.arg(user_id)::bigint AND b.dispatched
 AND b.parent_id=ANY(sqlc.arg(subtree_ids)::uuid[]) AND b.terminal_failure_code IS NULL
 AND (b.source_count=0 OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */import_job_attempts a
 WHERE a.user_id=b.user_id AND a.batch_id=b.batch_id AND a.result_file_id IS NULL
 AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */upload_sessions s
 WHERE s.id=a.upload_id AND s.user_id=a.user_id AND s.state='completed')));

-- name: TerminalizeUploadDestinations :execrows
UPDATE /* TEMPLATE: schema */upload_sessions
SET state='aborted',terminal_failure_code='upload_parent_unavailable',updated_at=now()
WHERE user_id=sqlc.arg(user_id)::bigint AND parent_id=ANY(sqlc.arg(subtree_ids)::uuid[])
 AND state IN ('open','completing') AND terminal_failure_code IS NULL;

-- name: ListActiveFilesForLifecycle :many
SELECT * FROM /* TEMPLATE: schema */files
WHERE user_id=sqlc.arg(user_id)::bigint AND id=ANY(sqlc.arg(file_ids)::uuid[]) AND status='active'
ORDER BY id;

-- name: ListActiveDestinationEntries :many
SELECT id,name FROM /* TEMPLATE: schema */files
WHERE user_id=sqlc.arg(user_id)::bigint
 AND parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid AND status='active'
ORDER BY id;

-- name: LockCatalogRowsForLifecycle :exec
SELECT id FROM /* TEMPLATE: schema */files
WHERE user_id=sqlc.arg(user_id)::bigint AND id=ANY(sqlc.arg(file_ids)::uuid[])
ORDER BY id FOR UPDATE;
