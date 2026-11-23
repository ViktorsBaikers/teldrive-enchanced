#!/usr/bin/env bash
# Measure schema compatibility between the previous production TelDrive image
# and a fresh restore of the current schema, capture the rollback anchor, and
# inventory pending writes. Modes:
#   compat          previous image runs `check` against the disposable restore
#   anchor          read-only rollback anchor from local image + B0 provenance
#   pending-writes  open upload and VFS queue inventory (read-only)
# Never touches production: no production database, no `serve`, no workers.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PREVIOUS_IMAGE="sha256:aeb7071a049882720e4cb6db10c3b48309d082dc0be03cd5fbc8cbd429947f44"
POSTGRES_IMAGE="postgres:18-alpine@sha256:b6972e1115a4ff8eba5d7f7d19184926d1e16d7d1136ae1e1a7ebf286da0f015"
BOOTSTRAP_USER="b7bootstrap"
DOC_ROOT="${B7_DOC_ROOT:-$(cd -- "$SCRIPT_DIR/../../.." && pwd)}"
B0_DOC="$DOC_ROOT/docs/research/teldrive-rclone/b0-provenance.md"
STATE_DOC="$DOC_ROOT/.planning/STATE.md"

MODE="compat"
DUMP=""
OUT=""
IMAGE="$PREVIOUS_IMAGE"
KEEP=false
SELF_TEST=false

usage() {
  cat >&2 <<'EOF'
usage: schema-compat.sh --out DIR [options]
  --mode compat|anchor|pending-writes   default: compat
  --restored-dump PATH                  restored cluster dump (0600, parent 0700)
  --image REF                           previous image ID or repo@digest
  --keep                                keep the disposable containers
  --self-test                           run refusal checks without Docker
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) [[ $# -ge 2 ]] || usage; MODE="$2"; shift 2 ;;
    --restored-dump) [[ $# -ge 2 ]] || usage; DUMP="$2"; shift 2 ;;
    --out) [[ $# -ge 2 ]] || usage; OUT="$2"; shift 2 ;;
    --image) [[ $# -ge 2 ]] || usage; IMAGE="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
    --self-test) SELF_TEST=true; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

# Accept only a content-addressed reference: bare sha256:<64hex> or
# name@sha256:<64hex>. A floating tag is refused before anything is created.
require_digest_image() {
  local ref="$1"
  if [[ "$ref" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    return 0
  fi
  if [[ "$ref" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]]; then
    return 0
  fi
  printf 'b7: image-not-digest: %s\n' "$ref" >&2
  return 2
}

# A production variable in the caller environment could silently redirect this
# run. Refuse before creating anything.
refuse_inherited_teldrive() {
  local inherited
  inherited="$(env | cut -d= -f1 | grep -E '^TELDRIVE_' || true)"
  if [[ -n "$inherited" ]]; then
    printf 'b7: refusing to run with TelDrive variables in the caller environment:\n%s\n' \
      "$inherited" >&2
    return 1
  fi
  return 0
}

# --- self-test: refusal paths and private modes, no Docker ------------------
self_test() {
  local root dir good rc=0
  root="$(mktemp -d)"
  chmod 700 "$root"
  trap "rm -rf -- '$root'" EXIT

  dir="$root/private"
  mkdir -m 700 "$dir"
  good="$dir/good.sql"
  printf 'SELECT 1;\n' > "$good"
  chmod 600 "$good"

  b7_require_dump "$good" || { printf 'self-test: FAIL: valid dump refused\n' >&2; return 1; }
  if err="$(b7_require_dump "$dir/missing.sql" 2>&1)"; then
    printf 'self-test: FAIL: missing dump accepted\n' >&2
    return 1
  fi
  [[ "$err" == *dump-missing* ]] ||
    { printf 'self-test: FAIL: missing dump token: %s\n' "$err" >&2; return 1; }

  require_digest_image "sha256:aeb7071a049882720e4cb6db10c3b48309d082dc0be03cd5fbc8cbd429947f44" ||
    { printf 'self-test: FAIL: image ID refused\n' >&2; return 1; }
  if require_digest_image "postgres:18-alpine" 2>/dev/null; then
    printf 'self-test: FAIL: floating tag accepted\n' >&2
    return 1
  fi

  if ( export TELDRIVE_PROBE=1; refuse_inherited_teldrive ) 2>/dev/null; then
    printf 'self-test: FAIL: inherited TELDRIVE_ variable accepted\n' >&2
    return 1
  fi
  refuse_inherited_teldrive ||
    { printf 'self-test: FAIL: clean environment refused\n' >&2; return 1; }

  printf '{"outcome":"PASS"}\n' | b7_write_receipt "$root/receipt.json" ||
    { printf 'self-test: FAIL: receipt write\n' >&2; return 1; }
  [[ "$(stat -c '%a' "$root/receipt.json")" == "600" ]] ||
    { printf 'self-test: FAIL: receipt mode is not 0600\n' >&2; return 1; }

  [[ "$rc" -eq 0 ]] || { printf 'self-test: FAIL\n' >&2; return 1; }
  printf 'self-test: PASS\n'
}

if [[ "$SELF_TEST" == true ]]; then
  self_test
  exit $?
fi

case "$MODE" in
  compat|anchor|pending-writes) ;;
  *) usage ;;
esac
[[ -n "$OUT" ]] || usage
if [[ "$MODE" != "anchor" ]]; then
  [[ -n "$DUMP" ]] || usage
fi

RUNTIME=""
RECEIPT=""
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PHASE="validate"
FAIL_REASON=""
RECEIPT_WRITTEN=false
LOCKED=false
DUMP_SHA=""
DUMP_BYTES=0
IMAGE_ID=""
IMAGE_VERSION=""
NETWORK=""
PG_NAME=""
APP_DB=""
SCHEMA=""
APP_ROLE=""
APP_ROLE_SUPER=false
BEFORE_APP=0
AFTER_APP=0
BEFORE_RIVER='[]'
AFTER_RIVER='[]'
CHECK_EXIT=""
CHECK_MARKER=false
LOG_EXCERPT='[]'
APPLIED_MIGRATIONS='[]'
IRREVERSIBLE='[]'
CLASSIFICATION=""
UPLOAD_OPEN=0
UPLOAD_COMPLETING=0
UPLOAD_PARTS_STORED=0
VFS_STATUS="NOT-RUN"
VFS_REASON="not requested"
VFS_QUEUE_ENTRIES=0
ANCHOR_JSON='{}'

fail() {
  FAIL_REASON="$1"
  printf 'b7: %s\n' "$1" >&2
  exit 1
}

write_compat_receipt() {
  local outcome="$1" finished
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n \
    --arg kind "b7-schema-compat-receipt-v1" \
    --arg plan "08-03" \
    --arg mode "$MODE" \
    --arg outcome "$outcome" \
    --arg reason "$FAIL_REASON" \
    --arg phase "$PHASE" \
    --arg image "$IMAGE" \
    --arg image_id "$IMAGE_ID" \
    --arg image_version "$IMAGE_VERSION" \
    --arg dump_sha256 "$DUMP_SHA" \
    --argjson dump_bytes "$DUMP_BYTES" \
    --arg database "$APP_DB" \
    --arg schema "$SCHEMA" \
    --arg app_role "$APP_ROLE" \
    --argjson app_role_rolsuper "$APP_ROLE_SUPER" \
    --argjson before_app_version "$BEFORE_APP" \
    --argjson after_app_version "$AFTER_APP" \
    --argjson before_river_versions "$BEFORE_RIVER" \
    --argjson after_river_versions "$AFTER_RIVER" \
    --arg check_exit_code "$CHECK_EXIT" \
    --argjson check_marker "$CHECK_MARKER" \
    --argjson log_excerpt "$LOG_EXCERPT" \
    --argjson applied_migrations "$APPLIED_MIGRATIONS" \
    --argjson irreversible_steps "$IRREVERSIBLE" \
    --arg classification "$CLASSIFICATION" \
    --argjson upload_sessions_open "$UPLOAD_OPEN" \
    --argjson upload_sessions_completing "$UPLOAD_COMPLETING" \
    --argjson upload_parts_stored "$UPLOAD_PARTS_STORED" \
    --arg vfs_queue_status "$VFS_STATUS" \
    --arg vfs_queue_reason "$VFS_REASON" \
    --argjson vfs_queue_entries "$VFS_QUEUE_ENTRIES" \
    --arg started "$STARTED" \
    --arg finished "$finished" \
    '{
      kind: $kind,
      plan: $plan,
      mode: $mode,
      outcome: $outcome,
      failure_reason: $reason,
      last_phase: $phase,
      previous_image: $image,
      previous_image_id: $image_id,
      previous_image_version: $image_version,
      restored_dump_sha256: $dump_sha256,
      restored_dump_bytes: $dump_bytes,
      database: $database,
      schema: $schema,
      app_role: $app_role,
      app_role_rolsuper: $app_role_rolsuper,
      before_app_version: $before_app_version,
      after_app_version: $after_app_version,
      before_river_versions: $before_river_versions,
      after_river_versions: $after_river_versions,
      check_exit_code: $check_exit_code,
      check_success_marker: $check_marker,
      log_excerpt: $log_excerpt,
      applied_migrations: $applied_migrations,
      irreversible_steps: $irreversible_steps,
      classification: $classification,
      pending_writes: {
        upload_sessions_open: $upload_sessions_open,
        upload_sessions_completing: $upload_sessions_completing,
        upload_parts_stored: $upload_parts_stored,
        vfs_queue: {status: $vfs_queue_status, reason: $vfs_queue_reason, entries: $vfs_queue_entries}
      },
      started_at: $started,
      finished_at: $finished
    }' | b7_write_receipt "$RECEIPT"
  RECEIPT_WRITTEN=true
}

write_anchor_receipt() {
  local outcome="$1" finished
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n \
    --arg kind "b7-schema-compat-anchor-v1" \
    --arg plan "08-03" \
    --arg outcome "$outcome" \
    --arg reason "$FAIL_REASON" \
    --arg started "$STARTED" \
    --arg finished "$finished" \
    --argjson anchor "$ANCHOR_JSON" \
    '{kind: $kind, plan: $plan, mode: "anchor", outcome: $outcome,
      failure_reason: $reason, rollback_anchor: $anchor,
      started_at: $started, finished_at: $finished}' |
    b7_write_receipt "$RECEIPT"
  RECEIPT_WRITTEN=true
}

on_exit() {
  local status=$?
  if [[ "$LOCKED" == true && -n "$OUT" ]]; then
    rmdir -- "$OUT/.lock" 2>/dev/null || true
  fi
  if [[ "$RECEIPT_WRITTEN" != true && -n "$RECEIPT" ]]; then
    [[ -n "$FAIL_REASON" ]] || FAIL_REASON="unexpected exit $status"
    case "$MODE" in
      anchor) write_anchor_receipt FAIL || true ;;
      *) write_compat_receipt FAIL || true ;;
    esac
  fi
  if [[ -n "$RUNTIME" ]]; then
    b7_cleanup "$RUNTIME" "$KEEP" "" || true
  fi
}
trap on_exit EXIT
trap 'exit 130' INT TERM

install -d -m 0700 "$OUT"
RECEIPT="$OUT/receipt.json"
if ! mkdir "$OUT/.lock" 2>/dev/null; then
  printf 'b7: run-exists: another run holds %s/.lock\n' "$OUT" >&2
  exit 3
fi
LOCKED=true

# --- anchor mode ------------------------------------------------------------
if [[ "$MODE" == "anchor" ]]; then
  PHASE="anchor"
  require_digest_image "$IMAGE" || fail "image is not content-addressed"
  RUNTIME="$(b6_runtime)"
  "$RUNTIME" image inspect "$IMAGE" >/dev/null 2>&1 ||
    fail "previous image is not present locally; refusing to pull"
  IMAGE_ID="$("$RUNTIME" image inspect --format '{{.Id}}' "$IMAGE")"
  IMAGE_CREATED="$("$RUNTIME" image inspect --format '{{.Created}}' "$IMAGE")"
  IMAGE_VERSION="$("$RUNTIME" image inspect \
    --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$IMAGE")"
  IMAGE_REVISION="$("$RUNTIME" image inspect \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$IMAGE")"
  REPO_DIGESTS="$("$RUNTIME" image inspect --format '{{json .RepoDigests}}' "$IMAGE" |
    jq -S .)"
  CONFIG_DIGEST="$("$RUNTIME" image inspect --format '{{json .Config}}' "$IMAGE" |
    jq -S . | sha256sum | cut -d' ' -f1)"
  [[ "$IMAGE_ID" == "$PREVIOUS_IMAGE" ]] ||
    fail "observed image ID does not match the previous production identity"
  [[ "$IMAGE_VERSION" == "2.0.12" ]] || fail "observed image version is not 2.0.12"

  [[ -r "$B0_DOC" ]] || fail "B0 provenance document is missing"
  B0_JSON="$(awk '/^```json$/{f=1;next} /^```$/{if(f){exit}} f' "$B0_DOC")"
  RCLONE_SHA="$(jq -r '.rclone_runtime.binary_sha256 // ""' <<<"$B0_JSON")"
  RCLONE_VERSION="$(jq -r '.rclone_runtime.version // ""' <<<"$B0_JSON")"
  [[ "$RCLONE_SHA" =~ ^[0-9a-f]{64}$ ]] || fail "B0 rclone binary SHA-256 is absent"
  [[ -n "$RCLONE_VERSION" ]] || fail "B0 rclone version is absent"

  B0_WRITE_BACK="$(sed -n 's/.*write-back \([^;]*\);.*/\1/p' "$B0_DOC" | head -n1)"
  STATE_WRITE_BACK=""
  STATE_COMMENT=""
  if [[ -r "$STATE_DOC" ]]; then
    STATE_WRITE_BACK="$(sed -n 's/.*--vfs-write-back \([^ `]*\)`.*/\1/p' "$STATE_DOC" | head -n1)"
    STATE_COMMENT="$(sed -n 's/.*unit comment says `(\([^)]*\))`.*/(\1)/p' "$STATE_DOC" | head -n1)"
  fi

  if [[ -n "${B7_RCLONE_BINARY:-}" ]]; then
    [[ -f "$B7_RCLONE_BINARY" ]] || fail "B7_RCLONE_BINARY is not a file"
    OBSERVED_RCLONE_SHA="$(sha256sum "$B7_RCLONE_BINARY" | cut -d' ' -f1)"
    if [[ "$OBSERVED_RCLONE_SHA" == "$RCLONE_SHA" ]]; then
      RCLONE_REOBS="PASS"
    else
      RCLONE_REOBS="MISMATCH"
    fi
  else
    OBSERVED_RCLONE_SHA=""
    RCLONE_REOBS="NOT-RUN"
  fi

  ANCHOR_JSON="$(jq -n \
    --arg image_id "$IMAGE_ID" \
    --arg image_version "$IMAGE_VERSION" \
    --arg image_created "$IMAGE_CREATED" \
    --arg image_revision "$IMAGE_REVISION" \
    --arg config_digest "$CONFIG_DIGEST" \
    --argjson repo_digests "$REPO_DIGESTS" \
    --arg rclone_sha "$RCLONE_SHA" \
    --arg rclone_version "$RCLONE_VERSION" \
    --arg observed_rclone_sha "$OBSERVED_RCLONE_SHA" \
    --arg reobs "$RCLONE_REOBS" \
    --arg b0_write_back "$B0_WRITE_BACK" \
    --arg state_write_back "$STATE_WRITE_BACK" \
    --arg state_comment "$STATE_COMMENT" \
    '{
      image_id: $image_id,
      image_version: $image_version,
      image_created: $image_created,
      image_revision: $image_revision,
      image_config_digest: $config_digest,
      image_repo_digests: $repo_digests,
      image_source: "observed-local",
      rclone_sha256: $rclone_sha,
      rclone_version: $rclone_version,
      rclone_source: "recorded-b0-not-reobserved",
      rclone_observed_sha256: (if $observed_rclone_sha == "" then null else $observed_rclone_sha end),
      rclone_binary_reobservation: $reobs,
      effective_configuration: {
        source: "recorded-b0-not-reobserved",
        b0_vfs_write_back: (if $b0_write_back == "" then null else $b0_write_back end),
        state_recorded_live_vfs_write_back: (if $state_write_back == "" then null else $state_write_back end),
        state_recorded_unit_comment: (if $state_comment == "" then null else $state_comment end),
        note: "B0 argv allowlist recorded write-back 24h; STATE.md records the live unit line as 2h with a stale unit comment and PROJECT.md intent 24h. Not re-observed."
      }
    }')"
  write_anchor_receipt PASS
  printf 'b7: anchor PASS; receipt %s\n' "$RECEIPT"
  exit 0
fi

# --- disposable restored cluster (compat and pending-writes) ---------------
require_digest_image "$IMAGE" || fail "image is not content-addressed"
refuse_inherited_teldrive || fail "inherited TELDRIVE_ environment"
b7_require_dump "$DUMP" || exit 2

DUMP_SHA="$(sha256sum "$DUMP" | cut -d' ' -f1)"
DUMP_BYTES="$(stat -c '%s' "$DUMP")"

RUNTIME="$(b6_runtime)"
IMAGE_ID="$("$RUNTIME" image inspect --format '{{.Id}}' "$IMAGE" 2>/dev/null || true)"
[[ "$IMAGE_ID" == "$PREVIOUS_IMAGE" ]] ||
  fail "previous image is not present locally or is a different identity"
IMAGE_VERSION="$("$RUNTIME" image inspect \
  --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$IMAGE")"

PHASE="cluster"
NETWORK="$(b7_name net)"
PG_NAME="$(b7_name postgres)"
PG_PASSWORD="$(b6_hex32)"
b7_write_env_file "$OUT/postgres.env" \
  "POSTGRES_USER=$BOOTSTRAP_USER" \
  "POSTGRES_PASSWORD=$PG_PASSWORD" \
  "POSTGRES_DB=postgres"

"$RUNTIME" network create "$NETWORK" >/dev/null
b7_track_network "$NETWORK"
b6_start_postgres "$RUNTIME" "$PG_NAME" "$POSTGRES_IMAGE" "$NETWORK" "$OUT/postgres.env"
b7_track_container "$PG_NAME"
b6_wait_postgres "$RUNTIME" "$PG_NAME" "$BOOTSTRAP_USER" postgres 120 ||
  fail "PostgreSQL did not become ready within 120 s"

# The bootstrap superuser is not named postgres, so the dump's own
# `CREATE ROLE postgres` can replay without a conflict.
EMPTY_COUNT="$("$RUNTIME" exec "$PG_NAME" psql -X -Atq -U "$BOOTSTRAP_USER" -d postgres -c "
SELECT (SELECT count(*) FROM pg_database
        WHERE datname NOT IN ('template0', 'template1', 'postgres'))
     + (SELECT count(*) FROM pg_roles
        WHERE rolname NOT LIKE 'pg\\_%' AND rolname <> '$BOOTSTRAP_USER');")"
[[ "$EMPTY_COUNT" == "0" ]] ||
  fail "target cluster is not empty; refusing to replay"

PHASE="replay"
if ! "$RUNTIME" exec -i "$PG_NAME" psql -X -v ON_ERROR_STOP=1 \
  -U "$BOOTSTRAP_USER" -d postgres -f /dev/stdin < "$DUMP" > "$OUT/replay.log" 2>&1; then
  chmod 600 "$OUT/replay.log" 2>/dev/null || true
  fail "replay failed: $(tail -n 3 "$OUT/replay.log" 2>/dev/null | tr '\n' ' ')"
fi
chmod 600 "$OUT/replay.log" 2>/dev/null || true

psql_bootstrap() {
  local db="$1" sql="$2"
  "$RUNTIME" exec "$PG_NAME" psql -X -Atq -v ON_ERROR_STOP=1 -U "$BOOTSTRAP_USER" -d "$db" -c "$sql"
}

PHASE="objects"
APP_DB=""
SCHEMA=""
while IFS= read -r db; do
  [[ -n "$db" ]] || continue
  candidate="$(psql_bootstrap "$db" "SELECT n.nspname FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'files' AND c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY n.nspname LIMIT 1")"
  if [[ -n "$candidate" ]]; then
    APP_DB="$db"
    SCHEMA="$candidate"
    break
  fi
done < <("$RUNTIME" exec "$PG_NAME" psql -X -Atq -U "$BOOTSTRAP_USER" -d postgres -c \
  "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1','postgres') ORDER BY 1")
[[ -n "$SCHEMA" ]] || fail "no application schema with a files table found"
[[ "$SCHEMA" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "unexpected schema name"

ROLE_ROW="$(psql_bootstrap "$APP_DB" "SELECT r.rolname, r.rolsuper
  FROM pg_roles r JOIN pg_namespace n ON n.nspowner = r.oid
  WHERE n.nspname = '$SCHEMA' LIMIT 1")"
IFS='|' read -r APP_ROLE APP_ROLE_SUPER_RAW <<<"$ROLE_ROW"
APP_ROLE="${APP_ROLE:-unknown}"
[[ "$APP_ROLE" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "unexpected app role name"
[[ "$APP_ROLE_SUPER_RAW" == "t" ]] && APP_ROLE_SUPER=true || APP_ROLE_SUPER=false

BEFORE_APP="$(psql_bootstrap "$APP_DB" \
  "SELECT COALESCE(max(version_id), 0) FROM \"$SCHEMA\".migrations")"
BEFORE_RIVER="$(psql_bootstrap "$APP_DB" \
  "SELECT COALESCE(json_agg(json_build_object('line', line, 'version', v) ORDER BY line), '[]'::json)
   FROM (SELECT line, max(version) AS v FROM \"$SCHEMA\".river_migration GROUP BY line) s")"

# --- pending-writes mode ----------------------------------------------------
if [[ "$MODE" == "pending-writes" ]]; then
  PHASE="pending-writes"
  COUNTS="$(psql_bootstrap "$APP_DB" "SELECT
    (SELECT count(*) FROM \"$SCHEMA\".upload_sessions WHERE state = 'open'),
    (SELECT count(*) FROM \"$SCHEMA\".upload_sessions WHERE state = 'completing'),
    (SELECT count(*) FROM \"$SCHEMA\".upload_parts WHERE state = 'stored')" | tr '|' '\n')"
  mapfile -t C < <(printf '%s\n' "$COUNTS")
  UPLOAD_OPEN="${C[0]:-0}"
  UPLOAD_COMPLETING="${C[1]:-0}"
  UPLOAD_PARTS_STORED="${C[2]:-0}"

  if [[ -n "${B7_RCLONE_RC_URL:-}" ]]; then
    if command -v rclone >/dev/null 2>&1; then
      if QUEUE="$(rclone rc --url "$B7_RCLONE_RC_URL" vfs/queue --json 2>/dev/null)"; then
        VFS_STATUS="recorded"
        VFS_REASON="read-only rclone rc vfs/queue"
        VFS_QUEUE_ENTRIES="$(jq -r '(.queue // []) | length' <<<"$QUEUE" 2>/dev/null || echo 0)"
        # Stats are fetched for presence only; contents may carry paths.
        if ! rclone rc --url "$B7_RCLONE_RC_URL" vfs/stats --json >/dev/null 2>&1; then
          VFS_REASON="$VFS_REASON; vfs/stats unavailable"
        fi
      else
        VFS_REASON="rclone rc endpoint unreachable"
      fi
    else
      VFS_REASON="rclone binary not on PATH"
    fi
  else
    VFS_REASON="B7_RCLONE_RC_URL not set; live queue not observed"
  fi

  CLASSIFICATION="INVENTORY"
  write_compat_receipt PASS
  printf 'b7: pending-writes PASS; receipt %s\n' "$RECEIPT"
  exit 0
fi

# --- compat mode: previous image runs `check` only -------------------------
PHASE="compat"
APP_PASS="$(b6_hex32)"
psql_bootstrap "$APP_DB" "ALTER ROLE \"$APP_ROLE\" WITH LOGIN PASSWORD '$APP_PASS'" >/dev/null

LOCAL_ROOT="$OUT/local-telegram"
install -d -m 0700 "$LOCAL_ROOT"
ENV_FILE="$OUT/check.env"
b7_synthetic_keys "$ENV_FILE"
cat >> "$ENV_FILE" <<ENV
TELDRIVE_DATABASE_URL=postgres://$APP_ROLE:$APP_PASS@postgres:5432/$APP_DB?sslmode=disable
TELDRIVE_DATABASE_SCHEMA=$SCHEMA
TELDRIVE_TELEGRAM_BACKEND=filesystem
TELDRIVE_TELEGRAM_LOCAL_ROOT=/data/local-telegram
TELDRIVE_JOBS_RUN_WORKERS=false
ENV
chmod 600 "$ENV_FILE"

CHECK_NAME="$(b7_name check)"
CHECK_LOG="$OUT/check.log"
set +e
( umask 077; "$RUNTIME" run --name "$CHECK_NAME" --network "$NETWORK" \
  --user "$(id -u):$(id -g)" --env-file "$ENV_FILE" \
  --mount "type=bind,source=$LOCAL_ROOT,target=/data/local-telegram" \
  --entrypoint /usr/bin/teldrive "$IMAGE" check > "$CHECK_LOG" 2>&1 )
CHECK_EXIT=$?
set -e
b7_track_container "$CHECK_NAME"

AFTER_APP="$(psql_bootstrap "$APP_DB" \
  "SELECT COALESCE(max(version_id), 0) FROM \"$SCHEMA\".migrations")"
AFTER_RIVER="$(psql_bootstrap "$APP_DB" \
  "SELECT COALESCE(json_agg(json_build_object('line', line, 'version', v) ORDER BY line), '[]'::json)
   FROM (SELECT line, max(version) AS v FROM \"$SCHEMA\".river_migration GROUP BY line) s")"

grep -q 'application.check.succeeded' "$CHECK_LOG" && CHECK_MARKER=true || CHECK_MARKER=false

# Sanitized tail: strip URLs, home paths, and long tokens.
LOG_EXCERPT="$(tail -n 40 "$CHECK_LOG" 2>/dev/null |
  sed -E -e 's#postgres://[^ "]*#postgres://<redacted>#g' \
         -e 's#/home/[^ "]*#<private-path>#g' \
         -e 's#[A-Za-z0-9+/=_-]{40,}#<redacted>#g' |
  jq -R -s 'split("\n") | map(select(length > 0))')"

APPLIED_MIGRATIONS="$(psql_bootstrap "$APP_DB" \
  "SELECT COALESCE(json_agg(json_build_object('version_id', version_id) ORDER BY version_id), '[]'::json)
   FROM \"$SCHEMA\".migrations WHERE version_id > $BEFORE_APP")"

IRREVERSIBLE="$(jq -n \
  --argjson applied "$APPLIED_MIGRATIONS" \
  --argjson before "$BEFORE_RIVER" \
  --argjson after "$AFTER_RIVER" \
  '([ $applied[] | {kind: "application-migration", version_id: .version_id} ]
   + [ $after[] as $a
       | ($before[] | select(.line == $a.line) | .version) as $b
       | select(($b // 0) < $a.version)
       | {kind: "river-migration", line: $a.line, from: ($b // 0), to: $a.version} ])')"

if [[ "$CHECK_EXIT" == "0" && "$CHECK_MARKER" == true && "$AFTER_APP" == "$BEFORE_APP" &&
  "$AFTER_RIVER" == "$BEFORE_RIVER" ]]; then
  CLASSIFICATION="PASS"
elif [[ "$CHECK_EXIT" == "0" && "$CHECK_MARKER" == true ]]; then
  CLASSIFICATION="PASS_WITH_MIGRATIONS"
else
  CLASSIFICATION="FAIL"
fi

PHASE="receipt"
if [[ "$CLASSIFICATION" == "FAIL" ]]; then
  write_compat_receipt FAIL
  printf 'b7: schema-compat FAIL (exit %s); receipt %s\n' "$CHECK_EXIT" "$RECEIPT" >&2
  exit 1
fi
write_compat_receipt "$CLASSIFICATION"
printf 'b7: schema-compat %s; receipt %s\n' "$CLASSIFICATION" "$RECEIPT"
