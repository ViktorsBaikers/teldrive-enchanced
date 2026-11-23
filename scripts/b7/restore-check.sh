#!/usr/bin/env bash
# Replay one operator-captured production pg_dumpall into a fresh disposable
# PostgreSQL cluster, prove cluster objects and catalog/upload invariants, and
# write a private receipt. Never touches production.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

POSTGRES_IMAGE="postgres:18-alpine@sha256:b6972e1115a4ff8eba5d7f7d19184926d1e16d7d1136ae1e1a7ebf286da0f015"
DUMP=""
OUT=""
RESTORED_DUMP_OUT=""
ENV_FILE=""
PROD_CONTAINER=""
KEEP=false
SELF_TEST=false

usage() {
  cat >&2 <<'EOF'
usage: restore-check.sh --dump PATH --out DIR [options]
  --dump PATH                       operator pg_dumpall file (0600, parent 0700)
  --out DIR                         private receipt directory (0700)
  --postgres-image REF              digest-pinned PostgreSQL image
  --restored-dump-out PATH          second-generation dump of the restored cluster
  --env-file PATH                   optional 0600 key/config replica
  --production-postgres-container   read-only SELECT version() preflight only
  --keep                            keep the disposable containers
  --self-test                       run refusal, lock and permission checks without Docker
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dump) [[ $# -ge 2 ]] || usage; DUMP="$2"; shift 2 ;;
    --out) [[ $# -ge 2 ]] || usage; OUT="$2"; shift 2 ;;
    --postgres-image) [[ $# -ge 2 ]] || usage; POSTGRES_IMAGE="$2"; shift 2 ;;
    --restored-dump-out) [[ $# -ge 2 ]] || usage; RESTORED_DUMP_OUT="$2"; shift 2 ;;
    --env-file) [[ $# -ge 2 ]] || usage; ENV_FILE="$2"; shift 2 ;;
    --production-postgres-container) [[ $# -ge 2 ]] || usage; PROD_CONTAINER="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
    --self-test) SELF_TEST=true; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

# --- self-test: refusal paths and private modes, no Docker ------------------
expect_refusal() {
  local token="$1" path="$2" rc=0 err
  err="$(b7_require_dump "$path" 2>&1)" || rc=$?
  if [[ "$rc" -eq 0 ]]; then
    printf 'self-test: FAIL: %s was accepted\n' "$token" >&2
    return 1
  fi
  if [[ "$err" != *"$token"* ]]; then
    printf 'self-test: FAIL: expected %s, got: %s\n' "$token" "$err" >&2
    return 1
  fi
}

self_test() {
  local root dir good bad open fixture rc=0
  root="$(mktemp -d)"
  chmod 700 "$root"
  trap "rm -rf -- '$root'" EXIT

  dir="$root/private"
  mkdir "$dir"
  chmod 700 "$dir"
  good="$dir/good.sql"
  printf 'SELECT 1;\n' > "$good"
  chmod 600 "$good"

  b7_require_dump "$good" || { printf 'self-test: FAIL: valid dump refused\n' >&2; return 1; }

  expect_refusal dump-missing "$dir/missing.sql" || rc=1

  ln -s "$good" "$dir/link.sql"
  expect_refusal dump-symlink "$dir/link.sql" || rc=1

  bad="$dir/bad.sql"
  printf 'SELECT 1;\n' > "$bad"
  chmod 644 "$bad"
  expect_refusal dump-permissions "$bad" || rc=1

  open="$root/open"
  mkdir "$open"
  chmod 755 "$open"
  fixture="$open/dump.sql"
  printf 'SELECT 1;\n' > "$fixture"
  chmod 600 "$fixture"
  expect_refusal dump-parent-permissions "$fixture" || rc=1

  : > "$dir/empty.sql"
  chmod 600 "$dir/empty.sql"
  expect_refusal dump-empty "$dir/empty.sql" || rc=1

  printf '{"outcome":"PASS"}\n' | b7_write_receipt "$root/receipt.json" ||
    { printf 'self-test: FAIL: receipt write\n' >&2; return 1; }
  if [[ "$(stat -c '%a' "$root/receipt.json")" != "600" ]]; then
    printf 'self-test: FAIL: receipt mode is not 0600\n' >&2
    return 1
  fi

  b7_write_env_file "$root/env" "PROBE=1"
  if [[ "$(stat -c '%a' "$root/env")" != "600" ]]; then
    printf 'self-test: FAIL: env mode is not 0600\n' >&2
    return 1
  fi

  case "$(b7_name postgres)" in
    b7-*) ;;
    *) printf 'self-test: FAIL: resource name lacks b7- prefix\n' >&2; return 1 ;;
  esac

  [[ "$rc" -eq 0 ]] || { printf 'self-test: FAIL\n' >&2; return 1; }
  printf 'self-test: PASS\n'
}

if [[ "$SELF_TEST" == true ]]; then
  self_test
  exit $?
fi

[[ -n "$DUMP" && -n "$OUT" ]] || usage

# A production variable in the caller environment could silently redirect this
# disposable run. Refuse before creating anything.
if inherited="$(env | cut -d= -f1 | grep -E '^TELDRIVE_' || true)" && [[ -n "$inherited" ]]; then
  printf 'b7: refusing to run with TelDrive variables in the caller environment:\n%s\n' \
    "$inherited" >&2
  exit 1
fi

b7_require_dump "$DUMP" || exit 2

RUNTIME=""
RECEIPT=""
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PHASE="validate"
FAIL_REASON=""
RECEIPT_WRITTEN=false
DUMP_SHA=""
DUMP_BYTES=0
DUMP_MTIME=""
TARGET_VERSION=""
DUMP_HEADER=""
RESTORED_SHA=""
RESTORED_BYTES=0
PROD_STATUS="NOT-RUN"
PROD_VERSION=""
PROD_REASON="not requested"
PG_NAME=""
POSTGRES_IMAGE_ID=""
APP_DB=""
SCHEMA=""
APP_ROLE=""
APP_ROLE_SUPER=false
APP_ROLE_LOGIN=false
ROLES_COUNT=0
DATABASES_JSON='[]'
DATABASES_COUNT=0
SCHEMA_USAGE=false
SCHEMA_CREATE=false
PGCRYPTO=false
PGTRGM=false
MIGRATIONS_MAX=0
RIVER_LINES='[]'
INV_ACTIVE_FILES=0
INV_FILE_PARTS=0
INV_ORPHAN_FILE_PARTS=0
INV_UPLOAD_ACTIVE=0
INV_UPLOAD_PARTS_STORED=0
INV_ORPHAN_UPLOAD_PARTS=0
CONFIG_REPLICA="NOT-RUN"
CONFIG_SOURCE="none"
DUMP_DELETED=false
DUMP_ABSENT=false
DELETED_AT=""

fail() {
  FAIL_REASON="$1"
  printf 'b7: %s\n' "$1" >&2
  exit 1
}

write_receipt() {
  local outcome="$1" finished
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n \
    --arg kind "b7-restore-receipt-v1" \
    --arg plan "08-01" \
    --arg outcome "$outcome" \
    --arg reason "$FAIL_REASON" \
    --arg phase "$PHASE" \
    --arg dump_sha256 "$DUMP_SHA" \
    --argjson dump_bytes "$DUMP_BYTES" \
    --arg dump_mtime "$DUMP_MTIME" \
    --arg postgres_image "$POSTGRES_IMAGE" \
    --arg postgres_image_id "$POSTGRES_IMAGE_ID" \
    --arg target_version "$TARGET_VERSION" \
    --arg dump_header_version "$DUMP_HEADER" \
    --arg prod_status "$PROD_STATUS" \
    --arg prod_version "$PROD_VERSION" \
    --arg prod_reason "$PROD_REASON" \
    --arg app_db "$APP_DB" \
    --arg schema "$SCHEMA" \
    --arg app_role "$APP_ROLE" \
    --argjson app_role_super "$APP_ROLE_SUPER" \
    --argjson app_role_login "$APP_ROLE_LOGIN" \
    --argjson roles_count "$ROLES_COUNT" \
    --argjson databases "$DATABASES_JSON" \
    --argjson databases_count "$DATABASES_COUNT" \
    --argjson schema_usage "$SCHEMA_USAGE" \
    --argjson schema_create "$SCHEMA_CREATE" \
    --argjson pgcrypto "$PGCRYPTO" \
    --argjson pg_trgm "$PGTRGM" \
    --argjson migrations_max_version "$MIGRATIONS_MAX" \
    --argjson river_lines "$RIVER_LINES" \
    --argjson active_files "$INV_ACTIVE_FILES" \
    --argjson file_parts "$INV_FILE_PARTS" \
    --argjson orphan_file_parts "$INV_ORPHAN_FILE_PARTS" \
    --argjson active_upload_sessions "$INV_UPLOAD_ACTIVE" \
    --argjson stored_upload_parts "$INV_UPLOAD_PARTS_STORED" \
    --argjson orphan_upload_parts "$INV_ORPHAN_UPLOAD_PARTS" \
    --arg config_replica "$CONFIG_REPLICA" \
    --arg config_source "$CONFIG_SOURCE" \
    --arg restored_sha256 "$RESTORED_SHA" \
    --argjson restored_bytes "$RESTORED_BYTES" \
    --argjson dump_deleted "$DUMP_DELETED" \
    --argjson dump_absent "$DUMP_ABSENT" \
    --arg deleted_at "$DELETED_AT" \
    --arg started "$STARTED" \
    --arg finished "$finished" \
    '{
      kind: $kind,
      plan: $plan,
      outcome: $outcome,
      failure_reason: $reason,
      last_phase: $phase,
      dump_sha256: $dump_sha256,
      dump_bytes: $dump_bytes,
      dump_mtime: $dump_mtime,
      postgres_image: $postgres_image,
      postgres_image_id: $postgres_image_id,
      target_database: "postgres",
      target_server_version: $target_version,
      dump_header_version: $dump_header_version,
      production_preflight: {status: $prod_status, server_version: $prod_version, reason: $prod_reason},
      database: $app_db,
      schema: $schema,
      app_role: $app_role,
      app_role_rolsuper: $app_role_super,
      app_role_rolcanlogin: $app_role_login,
      roles_count: $roles_count,
      databases: $databases,
      databases_count: $databases_count,
      schema_privileges: {usage: $schema_usage, create: $schema_create},
      extensions: {pgcrypto: $pgcrypto, pg_trgm: $pg_trgm},
      migrations_max_version: $migrations_max_version,
      river_lines: $river_lines,
      invariant_counts: {
        active_files: $active_files,
        file_parts: $file_parts,
        orphan_file_parts: $orphan_file_parts,
        active_upload_sessions: $active_upload_sessions,
        stored_upload_parts: $stored_upload_parts,
        orphan_upload_parts: $orphan_upload_parts
      },
      config_replica: $config_replica,
      config_replica_source: $config_source,
      restored_dump_sha256: $restored_sha256,
      restored_dump_bytes: $restored_bytes,
      dump_deleted: $dump_deleted,
      dump_absent: $dump_absent,
      deleted_at: $deleted_at,
      started_at: $started,
      finished_at: $finished
    }' | b7_write_receipt "$RECEIPT"
  RECEIPT_WRITTEN=true
}

on_exit() {
  local status=$?
  if [[ "$RECEIPT_WRITTEN" != true && -n "$RECEIPT" ]]; then
    [[ -n "$FAIL_REASON" ]] || FAIL_REASON="unexpected exit $status"
    write_receipt FAIL || true
  fi
  if [[ -n "$RUNTIME" ]]; then
    b7_cleanup "$RUNTIME" "$KEEP" "" || true
  fi
}
trap on_exit EXIT
trap 'exit 130' INT TERM

install -d -m 0700 "$OUT"
RECEIPT="$OUT/receipt.json"

DUMP_SHA="$(sha256sum "$DUMP" | cut -d' ' -f1)"
DUMP_BYTES="$(stat -c '%s' "$DUMP")"
DUMP_MTIME="$(stat -c '%y' "$DUMP")"
DUMP_HEADER="$(grep -m1 -E '^-- Dumped from database version' "$DUMP" 2>/dev/null |
  sed -E 's/^-- Dumped from database version //' || true)"

RUNTIME="$(b6_runtime)"
POSTGRES_IMAGE_ID="$("$RUNTIME" image inspect --format '{{.Id}}' "$POSTGRES_IMAGE" 2>/dev/null || true)"

# --- optional read-only production preflight ------------------------------
# Only SELECT version() and current_database(): no write, no restart, no schema
# read beyond the server identity. The role comes from the container's own
# POSTGRES_USER so the probe never guesses credentials.
if [[ -n "$PROD_CONTAINER" ]]; then
  PROD_ROLE="$("$RUNTIME" inspect --format '{{range .Config.Env}}{{println .}}{{end}}' \
    "$PROD_CONTAINER" 2>/dev/null | sed -n 's/^POSTGRES_USER=//p' | head -n1 || true)"
  [[ -n "$PROD_ROLE" ]] || PROD_ROLE="postgres"
  if raw="$("$RUNTIME" exec "$PROD_CONTAINER" psql -X -Atqc \
    'SELECT version(), current_database()' -U "$PROD_ROLE" 2>/dev/null)" && [[ -n "$raw" ]]; then
    PROD_STATUS="recorded"
    PROD_VERSION="${raw%%|*}"
  else
    PROD_REASON="read-only probe failed or container unreachable"
  fi
fi

# --- disposable cluster ----------------------------------------------------
PHASE="cluster"
NETWORK="$(b7_name net)"
PG_NAME="$(b7_name postgres)"
PG_PASSWORD="$(b6_hex32)"
b7_write_env_file "$OUT/postgres.env" \
  "POSTGRES_USER=postgres" \
  "POSTGRES_PASSWORD=$PG_PASSWORD" \
  "POSTGRES_DB=postgres"

"$RUNTIME" network create "$NETWORK" >/dev/null
b7_track_network "$NETWORK"
b6_start_postgres "$RUNTIME" "$PG_NAME" "$POSTGRES_IMAGE" "$NETWORK" "$OUT/postgres.env"
b7_track_container "$PG_NAME"

b6_wait_postgres "$RUNTIME" "$PG_NAME" postgres postgres 120 ||
  fail "PostgreSQL did not become ready within 120 s"
b7_assert_empty_cluster "$RUNTIME" "$PG_NAME" ||
  fail "target cluster is not empty; refusing to replay"
TARGET_VERSION="$("$RUNTIME" exec "$PG_NAME" psql -X -Atqc 'SHOW server_version' -U postgres -d postgres)"

# --- replay ----------------------------------------------------------------
# pg_dumpall emits CREATE DATABASE; never wrap the replay in a transaction and
# never pass --clean. ON_ERROR_STOP makes any failure terminal.
PHASE="replay"
if ! "$RUNTIME" exec -i "$PG_NAME" psql -X -v ON_ERROR_STOP=1 \
  -U postgres -d postgres -f /dev/stdin < "$DUMP" > "$OUT/replay.log" 2>&1; then
  chmod 600 "$OUT/replay.log" 2>/dev/null || true
  fail "replay failed: $(tail -n 3 "$OUT/replay.log" 2>/dev/null | tr '\n' ' ')"
fi
chmod 600 "$OUT/replay.log" 2>/dev/null || true

psql_db() {
  local db="$1" sql="$2"
  "$RUNTIME" exec "$PG_NAME" psql -X -Atq -v ON_ERROR_STOP=1 -U postgres -d "$db" -c "$sql"
}

# --- cluster objects -------------------------------------------------------
PHASE="objects"
DATABASES_JSON="$("$RUNTIME" exec "$PG_NAME" psql -X -Atq -U postgres -d postgres -c \
  "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1','postgres') ORDER BY 1" |
  jq -R -s 'split("\n") | map(select(length > 0))')"
DATABASES_COUNT="$(jq -r 'length' <<<"$DATABASES_JSON")"

# The application database and schema are discovered from the restored catalog,
# not assumed: the dump carries whatever schema the deployment used.
APP_DB=""
SCHEMA=""
while IFS= read -r db; do
  [[ -n "$db" ]] || continue
  candidate="$(psql_db "$db" "SELECT n.nspname FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'files' AND c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY n.nspname LIMIT 1")"
  if [[ -n "$candidate" ]]; then
    APP_DB="$db"
    SCHEMA="$candidate"
    break
  fi
done < <(jq -r '.[]' <<<"$DATABASES_JSON")

[[ -n "$SCHEMA" ]] || fail "no application schema with a files table found in the restored cluster"
[[ "$SCHEMA" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "unexpected schema name in restored cluster"

EXTENSIONS="$(psql_db "$APP_DB" \
  "SELECT extname FROM pg_extension WHERE extname IN ('pgcrypto','pg_trgm') ORDER BY 1")"
grep -qx 'pgcrypto' <<<"$EXTENSIONS" && PGCRYPTO=true || PGCRYPTO=false
grep -qx 'pg_trgm' <<<"$EXTENSIONS" && PGTRGM=true || PGTRGM=false

ROLES_COUNT="$(psql_db "$APP_DB" \
  "SELECT count(*) FROM pg_roles WHERE rolname NOT LIKE 'pg\\_%' AND rolname <> 'postgres'")"
ROLE_ROW="$(psql_db "$APP_DB" "SELECT r.rolname || '|' || r.rolsuper || '|' || r.rolcanlogin
  FROM pg_roles r JOIN pg_namespace n ON n.nspowner = r.oid
  WHERE n.nspname = '$SCHEMA' LIMIT 1")"
APP_ROLE="${ROLE_ROW%%|*}"
ROLE_REST="${ROLE_ROW#*|}"
APP_ROLE_SUPER="${ROLE_REST%%|*}"
APP_ROLE_LOGIN="${ROLE_REST#*|}"
[[ "$APP_ROLE_SUPER" == "t" ]] && APP_ROLE_SUPER=true || APP_ROLE_SUPER=false
[[ "$APP_ROLE_LOGIN" == "t" ]] && APP_ROLE_LOGIN=true || APP_ROLE_LOGIN=false
APP_ROLE="${APP_ROLE:-unknown}"

PRIV_ROW="$(psql_db "$APP_DB" "SELECT has_schema_privilege('$APP_ROLE', '$SCHEMA', 'USAGE'),
  has_schema_privilege('$APP_ROLE', '$SCHEMA', 'CREATE')")"
[[ "${PRIV_ROW%%|*}" == "t" ]] && SCHEMA_USAGE=true || SCHEMA_USAGE=false
[[ "${PRIV_ROW##*|}" == "t" ]] && SCHEMA_CREATE=true || SCHEMA_CREATE=false

MIGRATIONS_MAX="$(psql_db "$APP_DB" \
  "SELECT COALESCE(max(version_id), 0) FROM \"$SCHEMA\".migrations")"
RIVER_LINES="$(psql_db "$APP_DB" \
  "SELECT COALESCE(json_agg(json_build_object('line', line, 'version', v) ORDER BY line), '[]'::json)
   FROM (SELECT line, max(version) AS v FROM \"$SCHEMA\".river_migration GROUP BY line) s")"

# --- config and key replica (shape only, values never printed) -------------
PHASE="config"
if [[ -n "$ENV_FILE" ]]; then
  [[ "$(stat -c '%a' "$ENV_FILE")" == "600" ]] || fail "config replica file mode is not 0600"
  CONFIG_SOURCE="provided"
else
  CONFIG_SOURCE="generated"
  ENV_FILE="$OUT/env"
  b7_write_env_file "$ENV_FILE" \
    "TELDRIVE_SECURITY_DATA_KEY=$(b6_data_key)" \
    "TELDRIVE_SECURITY_SIGNING_KEY=$(b6_hex32 | cut -c1-48)" \
    "TELDRIVE_ENCRYPTION_KEYS=1:$(b6_hex32)" \
    "TELDRIVE_ENCRYPTION_ACTIVE_KEY_VERSION=1"
fi

env_value() { sed -n "s/^$1=//p" "$ENV_FILE" | head -n1; }
DATA_KEY="$(env_value TELDRIVE_SECURITY_DATA_KEY)"
SIGNING_KEY="$(env_value TELDRIVE_SECURITY_SIGNING_KEY)"
ENCRYPTION_KEYS="$(env_value TELDRIVE_ENCRYPTION_KEYS)"
ACTIVE_VERSION="$(env_value TELDRIVE_ENCRYPTION_ACTIVE_KEY_VERSION)"
[[ -n "$DATA_KEY" ]] || fail "config replica lacks TELDRIVE_SECURITY_DATA_KEY"
[[ -n "$SIGNING_KEY" ]] || fail "config replica lacks TELDRIVE_SECURITY_SIGNING_KEY"
[[ -n "$ENCRYPTION_KEYS" ]] || fail "config replica lacks TELDRIVE_ENCRYPTION_KEYS"
DATA_KEY_BYTES="$(printf '%s' "$DATA_KEY" | base64 -d 2>/dev/null | wc -c | tr -d ' ')" ||
  fail "config replica data key is not base64"
[[ "$DATA_KEY_BYTES" == "32" ]] || fail "config replica data key does not decode to 32 bytes"
[[ "${#SIGNING_KEY}" -ge 32 ]] || fail "config replica signing key is shorter than 32 characters"
[[ "$ENCRYPTION_KEYS" =~ ^1:[^,]+$ ]] || fail "config replica encryption key set has an unexpected shape"
[[ "$ACTIVE_VERSION" == "1" ]] || fail "config replica active key version is not 1"
unset DATA_KEY SIGNING_KEY ENCRYPTION_KEYS
CONFIG_REPLICA="valid"

# --- catalog and upload invariants ----------------------------------------
# Fidelity is restored-side only: no production count query is used, so no
# pre/post equality is claimed (D-19 keeps RECV-01 partial).
PHASE="invariants"
INV_ROW="$(psql_db "$APP_DB" "SELECT
  (SELECT count(*) FROM \"$SCHEMA\".files WHERE status = 'active'),
  (SELECT count(*) FROM \"$SCHEMA\".file_parts),
  (SELECT count(*) FROM \"$SCHEMA\".file_parts p
     WHERE NOT EXISTS (SELECT 1 FROM \"$SCHEMA\".files f WHERE f.id = p.file_id)),
  (SELECT count(*) FROM \"$SCHEMA\".upload_sessions WHERE state IN ('open','completing')),
  (SELECT count(*) FROM \"$SCHEMA\".upload_parts WHERE state = 'stored'),
  (SELECT count(*) FROM \"$SCHEMA\".upload_parts p
     WHERE NOT EXISTS (SELECT 1 FROM \"$SCHEMA\".upload_sessions s WHERE s.id = p.upload_id))" |
  tr '|' '\n')"
mapfile -t INV < <(printf '%s\n' "$INV_ROW")
INV_ACTIVE_FILES="${INV[0]:-0}"
INV_FILE_PARTS="${INV[1]:-0}"
INV_ORPHAN_FILE_PARTS="${INV[2]:-0}"
INV_UPLOAD_ACTIVE="${INV[3]:-0}"
INV_UPLOAD_PARTS_STORED="${INV[4]:-0}"
INV_ORPHAN_UPLOAD_PARTS="${INV[5]:-0}"
[[ "$INV_ORPHAN_FILE_PARTS" == "0" ]] || fail "orphan file_parts: $INV_ORPHAN_FILE_PARTS"
[[ "$INV_ORPHAN_UPLOAD_PARTS" == "0" ]] || fail "orphan upload_parts: $INV_ORPHAN_UPLOAD_PARTS"

# --- optional second-generation private dump -------------------------------
if [[ -n "$RESTORED_DUMP_OUT" ]]; then
  PHASE="restored_dump"
  install -d -m 0700 "$(dirname -- "$RESTORED_DUMP_OUT")"
  if ! ( umask 077; "$RUNTIME" exec "$PG_NAME" pg_dumpall -U postgres \
    --no-role-passwords > "$RESTORED_DUMP_OUT" ); then
    fail "second-generation dump failed"
  fi
  chmod 600 "$RESTORED_DUMP_OUT"
  RESTORED_SHA="$(sha256sum "$RESTORED_DUMP_OUT" | cut -d' ' -f1)"
  RESTORED_BYTES="$(stat -c '%s' "$RESTORED_DUMP_OUT")"
  [[ -n "$RESTORED_SHA" ]] || fail "second-generation dump has no digest"
fi

# --- receipt ---------------------------------------------------------------
PHASE="receipt"
write_receipt PASS

printf 'b7: restore PASS; receipt %s\n' "$RECEIPT"
