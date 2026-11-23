#!/usr/bin/env bash
# Prove that an encrypted object created through the real API survives a full
# database-plus-payload restore under synthetic keys, and that the restored
# ciphertext is bound to the restored key set.
#
# Isolation contract: every resource is named b7-<run>-*, lives on a private
# network, and is removed by b7_cleanup. The production database, channels,
# mount and keys are never read or used. Keys live only in 0600 env files.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

POSTGRES_IMAGE="postgres:18-alpine@sha256:b6972e1115a4ff8eba5d7f7d19184926d1e16d7d1136ae1e1a7ebf286da0f015"
# The second-generation dump produced by plan 08-01. A different dump must pass
# its own digest explicitly; the replay never trusts an unverified file.
EXPECTED_DUMP_SHA256="${B7_RESTORED_DUMP_SHA256:-6ab178d9666f5d23bb6c0edf30efc981e1bdeaebb8df80aac5cfc9e68ea8d70c}"
BOOTSTRAP_USER="b7bootstrap"
CANARY_SIZE=33554432
CANARY_PART_SIZE=16777216
CANARY_PARTS=2
MAGIC_HEX="54454c44524956450000"

MODE=""
RESTORED_DUMP=""
IMAGE=""
OUT=""
FROM_RECEIPT=""
KEEP=false
SELF_TEST=false

usage() {
  cat >&2 <<'EOF'
usage: encrypted-restore-check.sh --mode create|restore|negative --out DIR [options]
  --mode create                 create the encrypted canary through the API
  --mode restore                dump, restore into fresh instances, prove key binding
  --mode negative               standalone wrong-key diagnostic against a restore
  --restored-dump PATH          plan 08-01 second-generation dump (create only)
  --image REF                   TelDrive image digest or locally inspectable tag
  --from-receipt PATH           create-phase receipt (restore and negative)
  --out DIR                     private receipt directory (0700)
  --postgres-image REF          digest-pinned PostgreSQL image
  --keep                        keep the disposable containers
  --self-test                   argument, digest and env-shape checks without Docker
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) [[ $# -ge 2 ]] || usage; MODE="$2"; shift 2 ;;
    --restored-dump) [[ $# -ge 2 ]] || usage; RESTORED_DUMP="$2"; shift 2 ;;
    --image) [[ $# -ge 2 ]] || usage; IMAGE="$2"; shift 2 ;;
    --from-receipt) [[ $# -ge 2 ]] || usage; FROM_RECEIPT="$2"; shift 2 ;;
    --out) [[ $# -ge 2 ]] || usage; OUT="$2"; shift 2 ;;
    --postgres-image) [[ $# -ge 2 ]] || usage; POSTGRES_IMAGE="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
    --self-test) SELF_TEST=true; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

valid_digest() { [[ "${1:-}" =~ ^sha256:[0-9a-f]{64}$ ]]; }

# Accept a digest directly, or a tag only when the local runtime resolves it to
# a digest. A malformed reference is never accepted.
resolve_image_ref() {
  local runtime="$1" ref="$2" id
  if valid_digest "$ref"; then
    printf '%s\n' "$ref"
    return 0
  fi
  [[ -n "$ref" ]] || return 1
  id="$("$runtime" image inspect --format '{{.Id}}' "$ref" 2>/dev/null)" || return 1
  valid_digest "$id" || return 1
  printf '%s\n' "$id"
}

payload_digest() {
  local root="$1"
  ( cd -- "$root" && find . -type f -printf '%P\n' | LC_ALL=C sort |
    while IFS= read -r rel; do sha256sum -- "$rel"; done | sha256sum | cut -d' ' -f1 )
}

# --- self-test: no containers ---------------------------------------------
self_test() {
  local root rc=0
  root="$(mktemp -d "${TMPDIR:-/tmp}/b7-encrypted-selftest.XXXXXX")"
  chmod 700 "$root"
  trap "rm -rf -- '$root'" EXIT

  b7_synthetic_keys "$root/env" || { printf 'self-test: FAIL synthetic keys\n' >&2; return 1; }
  [[ "$(stat -c '%a' "$root/env")" == "600" ]] ||
    { printf 'self-test: FAIL env mode is not 0600\n' >&2; return 1; }
  grep -qx 'TELDRIVE_ENCRYPTION_ACTIVE_KEY_VERSION=1' "$root/env" ||
    { printf 'self-test: FAIL active key version missing\n' >&2; return 1; }
  grep -Eq '^TELDRIVE_ENCRYPTION_KEYS=1:[0-9a-f]{64}$' "$root/env" ||
    { printf 'self-test: FAIL encryption key shape\n' >&2; return 1; }

  local digest
  digest="sha256:$(printf 'a%.0s' {1..64})"
  [[ "$(resolve_image_ref /bin/false "$digest")" == "$digest" ]] ||
    { printf 'self-test: FAIL digest image refused\n' >&2; return 1; }
  if resolve_image_ref /bin/false "sha256:not-a-digest" >/dev/null 2>&1; then
    printf 'self-test: FAIL malformed image accepted\n' >&2
    return 1
  fi
  if resolve_image_ref /bin/false "" >/dev/null 2>&1; then
    printf 'self-test: FAIL empty image accepted\n' >&2
    return 1
  fi

  printf '{"outcome":"PASS"}\n' | b7_write_receipt "$root/receipt.json" ||
    { printf 'self-test: FAIL receipt write\n' >&2; return 1; }
  [[ "$(stat -c '%a' "$root/receipt.json")" == "600" ]] ||
    { printf 'self-test: FAIL receipt mode is not 0600\n' >&2; return 1; }

  case "$(b7_name postgres)" in
    b7-*) ;;
    *) printf 'self-test: FAIL resource name lacks b7- prefix\n' >&2; return 1 ;;
  esac

  [[ "$rc" -eq 0 ]] || { printf 'self-test: FAIL\n' >&2; return 1; }
  printf 'self-test: PASS\n'
}

if [[ "$SELF_TEST" == true ]]; then
  self_test
  exit $?
fi

[[ "$MODE" == create || "$MODE" == restore || "$MODE" == negative ]] || usage
[[ -n "$OUT" ]] || usage
if [[ "$MODE" == create ]]; then
  [[ -n "$RESTORED_DUMP" && -n "$IMAGE" ]] || usage
else
  [[ -n "$FROM_RECEIPT" ]] || usage
fi

if inherited="$(env | cut -d= -f1 | grep -E '^TELDRIVE_' || true)" && [[ -n "$inherited" ]]; then
  printf 'b7: refusing to run with TelDrive variables in the caller environment:\n%s\n' \
    "$inherited" >&2
  exit 1
fi

RUNTIME="$(b6_runtime)"
RECEIPT=""
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PHASE="prepare"
FAIL_REASON=""
RECEIPT_WRITTEN=false
LOCKED=false
PG_NAME=""
NETWORK=""
TD_NAME=""
LOCAL_ROOT=""
APP_DB=""
SCHEMA=""
APP_ROLE=""
APP_ROLE_SUPER=false
APP_PW=""
IMAGE_DIGEST=""
IMAGE_REF=""
POSTGRES_IMAGE_ID=""

fail() {
  FAIL_REASON="$1"
  printf 'b7: %s\n' "$1" >&2
  exit 1
}

prepare_out() {
  local out="$1"
  local receipt="$out/receipt.json"
  if [[ -f "$receipt" ]] && [[ "$(jq -r '.outcome // empty' "$receipt" 2>/dev/null)" == "PASS" ]]; then
    printf 'b7: run-exists: %s already holds a PASS receipt\n' "$out" >&2
    return 3
  fi
  if ! mkdir "$out/.lock" 2>/dev/null; then
    printf 'b7: run-exists: another run holds %s/.lock\n' "$out" >&2
    return 3
  fi
  return 0
}

on_exit() {
  local status=$?
  if [[ "$LOCKED" == true && -n "$OUT" ]]; then
    rmdir -- "$OUT/.lock" 2>/dev/null || true
  fi
  if [[ "$RECEIPT_WRITTEN" != true && -n "$RECEIPT" ]]; then
    [[ -n "$FAIL_REASON" ]] || FAIL_REASON="unexpected exit $status"
    write_failure_receipt || true
  fi
  if [[ -n "$RUNTIME" ]]; then
    b7_cleanup "$RUNTIME" "$KEEP" "" || true
  fi
}
trap on_exit EXIT
trap 'exit 130' INT TERM

write_failure_receipt() {
  local finished
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n \
    --arg kind "b7-encrypted-canary-v1" \
    --arg plan "08-02" \
    --arg mode "$MODE" \
    --arg outcome "FAIL" \
    --arg reason "$FAIL_REASON" \
    --arg phase "$PHASE" \
    --arg image_ref "$IMAGE_REF" \
    --arg image_digest "$IMAGE_DIGEST" \
    --arg started "$STARTED" \
    --arg finished "$finished" \
    '{kind:$kind, plan:$plan, mode:$mode, outcome:$outcome, failure_reason:$reason,
      last_phase:$phase, image_ref:$image_ref, image_digest:$image_digest,
      started_at:$started, finished_at:$finished}' | b7_write_receipt "$RECEIPT"
  RECEIPT_WRITTEN=true
}

pgq() {
  local db="$1" sql="$2"
  "$RUNTIME" exec "$PG_NAME" psql -X -Atq -v ON_ERROR_STOP=1 -U "$BOOTSTRAP_USER" -d "$db" -c "$sql"
}

start_postgres_cluster() {
  local envfile="$1"
  NETWORK="$(b7_name net)"
  PG_NAME="$(b7_name postgres)"
  b7_write_env_file "$envfile" \
    "POSTGRES_USER=$BOOTSTRAP_USER" \
    "POSTGRES_PASSWORD=$(b6_hex32)" \
    "POSTGRES_DB=$BOOTSTRAP_USER"
  "$RUNTIME" network create "$NETWORK" >/dev/null
  b7_track_network "$NETWORK"
  b6_start_postgres "$RUNTIME" "$PG_NAME" "$POSTGRES_IMAGE" "$NETWORK" "$envfile"
  b7_track_container "$PG_NAME"
  b6_wait_postgres "$RUNTIME" "$PG_NAME" "$BOOTSTRAP_USER" "$BOOTSTRAP_USER" 120 ||
    fail "PostgreSQL did not become ready within 120 s"
}

# The dump carries CREATE ROLE postgres, so this cluster boots a differently
# named superuser; the stock b7_assert_empty_cluster would count it.
assert_empty_cluster() {
  local count
  count="$(pgq postgres "
SELECT (SELECT count(*) FROM pg_database
        WHERE datname NOT IN ('template0', 'template1', 'postgres', '$BOOTSTRAP_USER'))
     + (SELECT count(*) FROM pg_roles
        WHERE rolname NOT LIKE 'pg\\_%' AND rolname <> '$BOOTSTRAP_USER');")"
  [[ "$count" == "0" ]] || fail "target cluster is not empty: $count unexpected object(s)"
}

discover_app_db_schema() {
  local db candidate
  APP_DB=""
  SCHEMA=""
  while IFS= read -r db; do
    [[ -n "$db" ]] || continue
    candidate="$(pgq "$db" "SELECT n.nspname FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = 'files' AND c.relkind = 'r'
        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      ORDER BY n.nspname LIMIT 1" 2>/dev/null || true)"
    if [[ -n "$candidate" ]]; then
      APP_DB="$db"
      SCHEMA="$candidate"
      break
    fi
  done < <(pgq postgres "SELECT datname FROM pg_database
    WHERE datname NOT IN ('template0','template1','postgres','$BOOTSTRAP_USER') ORDER BY 1")
  [[ -n "$SCHEMA" ]] || fail "no application schema with a files table found"
  [[ "$SCHEMA" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "unexpected schema name"
}

resolve_app_role() {
  local row
  row="$(pgq "$APP_DB" "SELECT r.rolname, r.rolsuper
    FROM pg_roles r JOIN pg_namespace n ON n.nspowner = r.oid
    WHERE n.nspname = '$SCHEMA' LIMIT 1")"
  IFS='|' read -r APP_ROLE APP_ROLE_SUPER <<<"$row"
  [[ "$APP_ROLE" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "application role is not a valid identifier"
  [[ "$APP_ROLE_SUPER" == "t" ]] && APP_ROLE_SUPER=true || APP_ROLE_SUPER=false
}

write_app_env() {
  local envfile="$1" db_url="$2" local_root="$3"
  b6_write_env_file "$envfile" \
    "TELDRIVE_DATABASE_URL=$db_url" \
    "TELDRIVE_DATABASE_SCHEMA=$SCHEMA" \
    "TELDRIVE_TELEGRAM_BACKEND=filesystem" \
    "TELDRIVE_TELEGRAM_LOCAL_ROOT=$local_root" \
    "TELDRIVE_JOBS_RUN_WORKERS=false" \
    "TELDRIVE_HTTP_ADDRESS=:8080"
  local pair
  for pair in \
    "TELDRIVE_SECURITY_DATA_KEY=$(sed -n 's/^TELDRIVE_SECURITY_DATA_KEY=//p' "$ENV_SOURCE" | head -n1)" \
    "TELDRIVE_SECURITY_SIGNING_KEY=$(sed -n 's/^TELDRIVE_SECURITY_SIGNING_KEY=//p' "$ENV_SOURCE" | head -n1)" \
    "TELDRIVE_ENCRYPTION_KEYS=$(sed -n 's/^TELDRIVE_ENCRYPTION_KEYS=//p' "$ENV_SOURCE" | head -n1)" \
    "TELDRIVE_ENCRYPTION_ACTIVE_KEY_VERSION=$(sed -n 's/^TELDRIVE_ENCRYPTION_ACTIVE_KEY_VERSION=//p' "$ENV_SOURCE" | head -n1)"; do
    printf '%s\n' "$pair" >> "$envfile"
  done
  chmod 600 "$envfile"
}

write_wrong_key_env() {
  local envfile="$1" db_url="$2" local_root="$3"
  b6_write_env_file "$envfile" \
    "TELDRIVE_DATABASE_URL=$db_url" \
    "TELDRIVE_DATABASE_SCHEMA=$SCHEMA" \
    "TELDRIVE_TELEGRAM_BACKEND=filesystem" \
    "TELDRIVE_TELEGRAM_LOCAL_ROOT=$local_root" \
    "TELDRIVE_JOBS_RUN_WORKERS=false" \
    "TELDRIVE_HTTP_ADDRESS=:8080" \
    "TELDRIVE_SECURITY_DATA_KEY=$(sed -n 's/^TELDRIVE_SECURITY_DATA_KEY=//p' "$ENV_SOURCE" | head -n1)" \
    "TELDRIVE_SECURITY_SIGNING_KEY=$(sed -n 's/^TELDRIVE_SECURITY_SIGNING_KEY=//p' "$ENV_SOURCE" | head -n1)" \
    "TELDRIVE_ENCRYPTION_KEYS=2:$(b6_hex32)" \
    "TELDRIVE_ENCRYPTION_ACTIVE_KEY_VERSION=2"
  chmod 600 "$envfile"
}

stop_app() {
  [[ -n "$TD_NAME" ]] || return 0
  "$RUNTIME" rm -f "$TD_NAME" >/dev/null 2>&1 || true
}

start_app() {
  local envfile="$1"
  TD_NAME="$(b7_name teldrive)"
  b7_start_canary_teldrive "$RUNTIME" "$TD_NAME" "$IMAGE_DIGEST" "$NETWORK" "$envfile" "$LOCAL_ROOT"
  PORT="$B7_CANARY_PORT"
}

parts_json() {
  pgq "$APP_DB" "SELECT part_no, coalesce(salt,''), coalesce(plain_size,0), coalesce(stored_size,0)
    FROM \"$SCHEMA\".file_parts WHERE file_id = '$FILE_ID' ORDER BY part_no" |
    jq -R -s 'split("\n") | map(select(length > 0)) | map(split("|") |
      {part_no:(.[0]|tonumber), salt_present:((.[1]//"")|length>0),
       plain_size:(.[2]|tonumber), stored_size:(.[3]|tonumber)})'
}

ciphertext_document_count() {
  local root="$1" count=0 f magic
  for f in "$root"/documents/*.bin; do
    [[ -f "$f" ]] || continue
    count=$((count + 1))
    magic="$(od -An -tx1 -N10 -- "$f" | tr -d ' \n')"
    [[ "$magic" == "$MAGIC_HEX" ]] || return 1
  done
  (( count > 0 )) || return 1
  printf '%s\n' "$count"
}

# --- create ---------------------------------------------------------------
create_run() {
  b7_require_dump "$RESTORED_DUMP" || exit 2
  local dump_sha
  dump_sha="$(sha256sum "$RESTORED_DUMP" | cut -d' ' -f1)"
  [[ "$dump_sha" == "$EXPECTED_DUMP_SHA256" ]] ||
    fail "restored dump digest mismatch"
  IMAGE_REF="$IMAGE"
  IMAGE_DIGEST="$(resolve_image_ref "$RUNTIME" "$IMAGE")" || fail "image is not a digest or inspectable tag"
  POSTGRES_IMAGE_ID="$("$RUNTIME" image inspect --format '{{.Id}}' "$POSTGRES_IMAGE" 2>/dev/null || true)"

  b6_ensure_private_dir "$OUT"
  RECEIPT="$OUT/receipt.json"
  prepare_out "$OUT" || exit 3
  LOCKED=true
  mkdir -m 700 "$OUT/snapshot"

  PHASE="cluster"
  start_postgres_cluster "$OUT/postgres.env"
  assert_empty_cluster

  PHASE="replay"
  if ! "$RUNTIME" exec -i "$PG_NAME" psql -X -v ON_ERROR_STOP=1 \
    -U "$BOOTSTRAP_USER" -d "$BOOTSTRAP_USER" -f /dev/stdin < "$RESTORED_DUMP" \
    > "$OUT/replay.log" 2>&1; then
    chmod 600 "$OUT/replay.log" 2>/dev/null || true
    fail "replay failed: $(tail -n 3 "$OUT/replay.log" 2>/dev/null | tr '\n' ' ')"
  fi
  chmod 600 "$OUT/replay.log" 2>/dev/null || true

  PHASE="objects"
  discover_app_db_schema
  resolve_app_role
  APP_PW="$(b6_hex32)"
  pgq postgres "ALTER ROLE \"$APP_ROLE\" WITH LOGIN PASSWORD '$APP_PW'" ||
    fail "could not set the application role password"

  PHASE="app"
  LOCAL_ROOT="$OUT/local-telegram"
  mkdir -m 700 "$LOCAL_ROOT"
  ENV_SOURCE="$OUT/keys.env"
  b7_synthetic_keys "$ENV_SOURCE"
  write_app_env "$OUT/env" "postgres://$APP_ROLE:$APP_PW@postgres:5432/$APP_DB?sslmode=disable" "/data/local-telegram"
  start_app "$OUT/env" || fail "TelDrive did not become ready"
  local port="$PORT"

  PHASE="credential"
  local api_secret api_conf
  api_secret="$(b6_api_secret)"
  printf '%s\n' "$api_secret" > "$OUT/api-key"
  chmod 600 "$OUT/api-key"
  api_conf="$OUT/curl.conf"
  ( umask 077; printf 'header = "X-Api-Key: %s"\n' "$api_secret" > "$api_conf" )
  chmod 600 "$api_conf"
  b7_bootstrap_canary_user "$RUNTIME" "$PG_NAME" "$BOOTSTRAP_USER" "$APP_DB" "$SCHEMA" \
    "$api_secret" "b7-encrypted-canary" || fail "API key bootstrap failed"
  unset api_secret

  PHASE="upload"
  local base source chunk0 chunk1 code body
  base="http://127.0.0.1:$port"
  source="$OUT/source.bin"
  head -c "$CANARY_SIZE" /dev/zero |
    openssl enc -aes-256-ctr -K 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
      -iv 000102030405060708090a0b0c0d0e0f -nosalt -out "$source"
  chmod 600 "$source"
  SOURCE_SHA="$(sha256sum "$source" | cut -d' ' -f1)"
  split -b "$CANARY_PART_SIZE" -d -a 1 "$source" "$OUT/chunk."
  chunk0="$OUT/chunk.0"
  chunk1="$OUT/chunk.1"
  [[ "$(stat -c '%s' "$chunk0")" == "$CANARY_PART_SIZE" ]] || fail "first chunk has the wrong size"
  [[ "$(stat -c '%s' "$chunk1")" == "$CANARY_PART_SIZE" ]] || fail "second chunk has the wrong size"

  body="$OUT/create.json"
  jq -n --arg name "b7-canary.bin" --argjson size "$CANARY_SIZE" \
    --arg modTime "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson partSize "$CANARY_PART_SIZE" \
    '{name:$name, size:$size, modTime:$modTime, encryption:true, preferredPartSize:$partSize}' > "$body"
  code="$(curl -sS -K "$api_conf" --max-time 60 -X POST \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
    --data-binary @"$body" -o "$OUT/create-response.json" -w '%{http_code}' \
    "$base/v1/uploads")" || fail "create upload request failed"
  [[ "$code" == 201 ]] || fail "create upload returned $code"
  UPLOAD_ID="$(jq -er '.id' "$OUT/create-response.json")" || fail "create response lacks an upload id"
  [[ "$(jq -er '.encryption' "$OUT/create-response.json")" == "true" ]] || fail "session is not encrypted"
  [[ "$(jq -er '.partSize' "$OUT/create-response.json")" == "$CANARY_PART_SIZE" ]] ||
    fail "session part size is not 16 MiB"

  code="$(curl -sS -K "$api_conf" --max-time 120 -X PUT \
    -H 'Content-Type: application/octet-stream' --data-binary @"$chunk0" \
    -o "$OUT/part1.json" -w '%{http_code}' "$base/v1/uploads/$UPLOAD_ID/parts/1")" ||
    fail "part 1 request failed"
  [[ "$code" == 200 || "$code" == 201 ]] || fail "part 1 returned $code"
  code="$(curl -sS -K "$api_conf" --max-time 120 -X PUT \
    -H 'Content-Type: application/octet-stream' --data-binary @"$chunk1" \
    -o "$OUT/part2.json" -w '%{http_code}' "$base/v1/uploads/$UPLOAD_ID/parts/2")" ||
    fail "part 2 request failed"
  [[ "$code" == 200 || "$code" == 201 ]] || fail "part 2 returned $code"

  jq -n --argjson size "$CANARY_SIZE" '{size:$size}' > "$OUT/complete.json"
  code="$(curl -sS -K "$api_conf" --max-time 120 -X POST \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
    --data-binary @"$OUT/complete.json" -o "$OUT/complete-response.json" -w '%{http_code}' \
    "$base/v1/uploads/$UPLOAD_ID/complete")" || fail "complete request failed"
  [[ "$code" == 200 || "$code" == 201 ]] || fail "complete returned $code"
  FILE_ID="$(jq -er '.id' "$OUT/complete-response.json")" || fail "complete response lacks a file id"

  PHASE="download"
  code="$(curl -sS -K "$api_conf" --max-time 120 -o "$OUT/download.bin" -w '%{http_code}' \
    "$base/v1/files/$FILE_ID/content")" || fail "download request failed"
  [[ "$code" == 200 ]] || fail "download returned $code"
  DOWNLOAD_SHA="$(sha256sum "$OUT/download.bin" | cut -d' ' -f1)"
  [[ "$DOWNLOAD_SHA" == "$SOURCE_SHA" ]] || fail "downloaded SHA-256 differs from the source"

  PHASE="database"
  local file_row
  file_row="$(pgq "$APP_DB" "SELECT encryption, coalesce(encryption_key_version,0), coalesce(size,0)
    FROM \"$SCHEMA\".files WHERE id = '$FILE_ID'")"
  IFS='|' read -r FILE_ENCRYPTION KEY_VERSION FILE_SIZE <<<"$file_row"
  [[ "$FILE_ENCRYPTION" == "t" ]] || fail "files.encryption is not true"
  [[ "$KEY_VERSION" == "1" ]] || fail "files.encryption_key_version is not 1"
  [[ "$FILE_SIZE" == "$CANARY_SIZE" ]] || fail "files.size does not match the canary"

  PARTS_JSON="$(parts_json)"
  [[ "$(jq -r 'length' <<<"$PARTS_JSON")" == "$CANARY_PARTS" ]] || fail "unexpected part count"
  jq -e 'all(.[]; .salt_present)' <<<"$PARTS_JSON" >/dev/null || fail "a part salt is empty"
  jq -e 'all(.[]; .stored_size > .plain_size)' <<<"$PARTS_JSON" >/dev/null ||
    fail "a stored part is not larger than its plaintext"

  PHASE="payload"
  DOCUMENT_COUNT="$(ciphertext_document_count "$LOCAL_ROOT")" || fail "payload lacks the ciphertext magic header"

  PHASE="snapshot"
  stop_app
  TD_NAME=""
  ( umask 077; "$RUNTIME" exec "$PG_NAME" pg_dump --no-owner --no-privileges \
    -U "$BOOTSTRAP_USER" -d "$APP_DB" > "$OUT/snapshot/db.sql" ) || fail "database snapshot failed"
  chmod 600 "$OUT/snapshot/db.sql"
  cp -a "$LOCAL_ROOT" "$OUT/snapshot/payload"
  chmod 700 "$OUT/snapshot/payload"
  SNAPSHOT_SHA="$(sha256sum "$OUT/snapshot/db.sql" | cut -d' ' -f1)"
  PAYLOAD_SHA="$(payload_digest "$OUT/snapshot/payload")"

  PHASE="receipt"
  local finished
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n \
    --arg kind "b7-encrypted-canary-v1" \
    --arg plan "08-02" \
    --arg mode "create" \
    --arg outcome "PASS" \
    --arg reason "" \
    --arg image_ref "$IMAGE_REF" \
    --arg image_digest "$IMAGE_DIGEST" \
    --arg postgres_image "$POSTGRES_IMAGE" \
    --arg postgres_image_id "$POSTGRES_IMAGE_ID" \
    --arg database "$APP_DB" \
    --arg schema "$SCHEMA" \
    --arg app_role "$APP_ROLE" \
    --argjson app_role_rolsuper "$APP_ROLE_SUPER" \
    --argjson non_superuser_login "$([[ "$APP_ROLE_SUPER" == true ]] && echo false || echo true)" \
    --arg file_id "$FILE_ID" \
    --arg file_name "b7-canary.bin" \
    --argjson plain_size "$CANARY_SIZE" \
    --argjson part_size "$CANARY_PART_SIZE" \
    --argjson part_count "$CANARY_PARTS" \
    --argjson parts "$PARTS_JSON" \
    --argjson file_encryption true \
    --argjson encryption_key_version "$KEY_VERSION" \
    --arg source_sha256 "$SOURCE_SHA" \
    --arg download_sha256 "$DOWNLOAD_SHA" \
    --argjson sha256_match true \
    --argjson document_count "$DOCUMENT_COUNT" \
    --argjson ciphertext_header true \
    --arg env_file "$OUT/env" \
    --arg local_root "$LOCAL_ROOT" \
    --arg snapshot_db_path "$OUT/snapshot/db.sql" \
    --arg snapshot_payload_path "$OUT/snapshot/payload" \
    --arg snapshot_sha256 "$SNAPSHOT_SHA" \
    --arg payload_root_sha256 "$PAYLOAD_SHA" \
    --arg started "$STARTED" \
    --arg finished "$finished" \
    '{kind:$kind, plan:$plan, mode:$mode, outcome:$outcome, failure_reason:$reason,
      image_ref:$image_ref, image_digest:$image_digest,
      postgres_image:$postgres_image, postgres_image_id:$postgres_image_id,
      database:$database, schema:$schema, app_role:$app_role,
      app_role_rolsuper:$app_role_rolsuper, non_superuser_login:$non_superuser_login,
      file_id:$file_id, file_name:$file_name, plain_size:$plain_size,
      part_size:$part_size, part_count:$part_count, parts:$parts,
      file_encryption:$file_encryption, encryption_key_version:$encryption_key_version,
      source_sha256:$source_sha256, download_sha256:$download_sha256,
      sha256_match:$sha256_match, document_count:$document_count,
      ciphertext_header:$ciphertext_header,
      env_file:$env_file, local_root:$local_root,
      snapshot_db_path:$snapshot_db_path, snapshot_payload_path:$snapshot_payload_path,
      snapshot_sha256:$snapshot_sha256, payload_root_sha256:$payload_root_sha256,
      started_at:$started, finished_at:$finished}' | b7_write_receipt "$RECEIPT"
  RECEIPT_WRITTEN=true
  printf 'b7: encrypted canary PASS; receipt %s\n' "$RECEIPT"
}

# --- shared restore preparation -------------------------------------------
restore_prepare() {
  [[ -f "$FROM_RECEIPT" ]] || fail "create receipt not found"
  [[ "$(jq -r '.mode' "$FROM_RECEIPT")" == "create" ]] || fail "receipt is not a create-phase receipt"
  [[ "$(jq -r '.outcome' "$FROM_RECEIPT")" == "PASS" ]] || fail "create phase did not pass"

  CREATE_OUT="$(cd -- "$(dirname -- "$FROM_RECEIPT")" && pwd)"
  CREATE_RECEIPT_SHA="$(sha256sum "$FROM_RECEIPT" | cut -d' ' -f1)"
  IMAGE_REF="$(jq -er '.image_ref' "$FROM_RECEIPT")"
  IMAGE_DIGEST="$(jq -er '.image_digest' "$FROM_RECEIPT")"
  valid_digest "$IMAGE_DIGEST" || fail "receipt image digest is not a sha256"
  SCHEMA="$(jq -er '.schema' "$FROM_RECEIPT")"
  APP_ROLE="$(jq -er '.app_role' "$FROM_RECEIPT")"
  APP_ROLE_SUPER="$(jq -er '.app_role_rolsuper' "$FROM_RECEIPT")"
  APP_DB="$(jq -er '.database' "$FROM_RECEIPT")"
  FILE_ID="$(jq -er '.file_id' "$FROM_RECEIPT")"
  CREATE_SHA="$(jq -er '.download_sha256' "$FROM_RECEIPT")"
  PLAIN_SIZE="$(jq -er '.plain_size' "$FROM_RECEIPT")"
  SNAP_DB="$(jq -er '.snapshot_db_path' "$FROM_RECEIPT")"
  SNAP_PAYLOAD="$(jq -er '.snapshot_payload_path' "$FROM_RECEIPT")"
  CREATE_SNAPSHOT_SHA="$(jq -er '.snapshot_sha256' "$FROM_RECEIPT")"
  CREATE_PAYLOAD_SHA="$(jq -er '.payload_root_sha256' "$FROM_RECEIPT")"
  ENV_SOURCE="$(jq -er '.env_file' "$FROM_RECEIPT")"

  [[ "$SCHEMA" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "receipt schema is invalid"
  [[ "$APP_ROLE" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "receipt app role is invalid"
  [[ "$APP_DB" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "receipt database is invalid"
  [[ "$(stat -c '%a' "$ENV_SOURCE")" == "600" ]] || fail "create env file is not 0600"
  [[ "$(stat -c '%a' "$SNAP_DB")" == "600" ]] || fail "snapshot database is not 0600"
  [[ -d "$SNAP_PAYLOAD" ]] || fail "snapshot payload directory is missing"
  [[ -f "$CREATE_OUT/api-key" ]] || fail "create API key file is missing"
  [[ "$(stat -c '%a' "$CREATE_OUT/api-key")" == "600" ]] || fail "create API key file is not 0600"

  b6_ensure_private_dir "$OUT"
  RECEIPT="$OUT/receipt.json"
  prepare_out "$OUT" || exit 3
  LOCKED=true

  PHASE="cluster"
  start_postgres_cluster "$OUT/postgres.env"
  assert_empty_cluster

  APP_PW="$(b6_hex32)"
  local super_clause=""
  if [[ "$APP_ROLE_SUPER" == true ]]; then super_clause="SUPERUSER"; fi
  pgq postgres "CREATE ROLE \"$APP_ROLE\" LOGIN $super_clause PASSWORD '$APP_PW'" ||
    fail "could not create the application role"
  pgq postgres "CREATE DATABASE \"$APP_DB\" OWNER \"$APP_ROLE\"" ||
    fail "could not create the application database"

  PHASE="replay"
  if ! "$RUNTIME" exec -i -e "PGOPTIONS=-c role=$APP_ROLE" "$PG_NAME" psql -X \
    -v ON_ERROR_STOP=1 -U "$BOOTSTRAP_USER" -d "$APP_DB" -f /dev/stdin < "$SNAP_DB" \
    > "$OUT/replay.log" 2>&1; then
    chmod 600 "$OUT/replay.log" 2>/dev/null || true
    fail "database replay failed: $(tail -n 3 "$OUT/replay.log" 2>/dev/null | tr '\n' ' ')"
  fi
  chmod 600 "$OUT/replay.log" 2>/dev/null || true

  PHASE="payload"
  LOCAL_ROOT="$OUT/local-telegram"
  mkdir -m 700 "$LOCAL_ROOT"
  cp -a "$SNAP_PAYLOAD/." "$LOCAL_ROOT/"
  RESTORED_PAYLOAD_SHA="$(payload_digest "$LOCAL_ROOT")"
  [[ "$RESTORED_PAYLOAD_SHA" == "$CREATE_PAYLOAD_SHA" ]] || fail "restored payload root differs from the snapshot"
  RESTORED_SNAPSHOT_SHA="$(sha256sum "$SNAP_DB" | cut -d' ' -f1)"
  [[ "$RESTORED_SNAPSHOT_SHA" == "$CREATE_SNAPSHOT_SHA" ]] || fail "snapshot database digest changed"

  PHASE="app"
  write_app_env "$OUT/env" "postgres://$APP_ROLE:$APP_PW@postgres:5432/$APP_DB?sslmode=disable" "/data/local-telegram"
  API_CONF="$OUT/curl.conf"
  ( umask 077; printf 'header = "X-Api-Key: %s"\n' "$(cat "$CREATE_OUT/api-key")" > "$API_CONF" )
  chmod 600 "$API_CONF"
  start_app "$OUT/env" || fail "restored TelDrive did not become ready"
}

download_status() {
  local dest="$1"
  curl -sS -K "$API_CONF" --max-time 120 -o "$dest" -w '%{http_code}' \
    "http://127.0.0.1:$PORT/v1/files/$FILE_ID/content"
}

assert_restored_invariants() {
  local file_row
  file_row="$(pgq "$APP_DB" "SELECT encryption, coalesce(encryption_key_version,0), coalesce(size,0)
    FROM \"$SCHEMA\".files WHERE id = '$FILE_ID'")"
  IFS='|' read -r FILE_ENCRYPTION KEY_VERSION FILE_SIZE <<<"$file_row"
  [[ "$FILE_ENCRYPTION" == "t" ]] || fail "restored files.encryption is not true"
  [[ "$KEY_VERSION" == "1" ]] || fail "restored encryption key version is not 1"
  [[ "$FILE_SIZE" == "$PLAIN_SIZE" ]] || fail "restored file size does not match"

  PARTS_JSON="$(parts_json)"
  [[ "$(jq -r 'length' <<<"$PARTS_JSON")" == "$CANARY_PARTS" ]] || fail "restored part count differs"
  jq -e 'all(.[]; .salt_present)' <<<"$PARTS_JSON" >/dev/null || fail "a restored part salt is empty"
  jq -e 'all(.[]; .stored_size > .plain_size)' <<<"$PARTS_JSON" >/dev/null ||
    fail "a restored part is not larger than its plaintext"
  STORED_GT_PLAIN=true

  DOCUMENT_COUNT="$(ciphertext_document_count "$LOCAL_ROOT")" || fail "restored payload lacks the ciphertext magic header"
  CIPHERTEXT_HEADER=true
}

restore_run() {
  restore_prepare

  PHASE="download"
  local code
  code="$(download_status "$OUT/restored.bin")" || fail "restored download request failed"
  [[ "$code" == 200 ]] || fail "restored download returned $code"
  RESTORE_SHA="$(sha256sum "$OUT/restored.bin" | cut -d' ' -f1)"
  RESTORE_MATCH=false
  if [[ "$RESTORE_SHA" == "$CREATE_SHA" ]]; then RESTORE_MATCH=true; fi
  [[ "$RESTORE_MATCH" == true ]] || fail "restored SHA-256 differs from the create-phase SHA-256"

  PHASE="invariants"
  assert_restored_invariants

  PHASE="negative"
  stop_app
  TD_NAME=""
  write_wrong_key_env "$OUT/env-wrong" "postgres://$APP_ROLE:$APP_PW@postgres:5432/$APP_DB?sslmode=disable" "/data/local-telegram"
  start_app "$OUT/env-wrong" || fail "wrong-key TelDrive did not become ready"
  NEG_STATUS="$(download_status "$OUT/negative.bin")" || NEG_STATUS=0
  NEG_CODE="$(jq -r '.error.code // empty' "$OUT/negative.bin" 2>/dev/null || true)"
  [[ -n "$NEG_CODE" ]] || NEG_CODE="no-error-body"
  [[ "$NEG_STATUS" =~ ^[0-9]+$ ]] || NEG_STATUS=0
  (( NEG_STATUS >= 400 )) || fail "wrong-key download returned $NEG_STATUS"

  PHASE="recovery"
  stop_app
  TD_NAME=""
  start_app "$OUT/env" || fail "recovered TelDrive did not become ready"
  code="$(download_status "$OUT/recovered.bin")" || fail "recovered download request failed"
  RECOVERED_STATUS="$code"
  RECOVERED_SHA="$(sha256sum "$OUT/recovered.bin" | cut -d' ' -f1)"
  RECOVERED_MATCH=false
  if [[ "$code" == 200 && "$RECOVERED_SHA" == "$CREATE_SHA" ]]; then RECOVERED_MATCH=true; fi
  [[ "$RECOVERED_MATCH" == true ]] || fail "correct-key rerun did not restore the download"

  PHASE="receipt"
  local finished
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n \
    --arg kind "b7-encrypted-canary-v1" \
    --arg plan "08-02" \
    --arg mode "restore" \
    --arg outcome "PASS" \
    --arg reason "" \
    --arg image_ref "$IMAGE_REF" \
    --arg image_digest "$IMAGE_DIGEST" \
    --arg create_receipt_sha256 "$CREATE_RECEIPT_SHA" \
    --arg database "$APP_DB" \
    --arg schema "$SCHEMA" \
    --arg app_role "$APP_ROLE" \
    --argjson app_role_rolsuper "$APP_ROLE_SUPER" \
    --arg file_id "$FILE_ID" \
    --argjson plain_size "$PLAIN_SIZE" \
    --arg create_sha256 "$CREATE_SHA" \
    --arg restored_sha256 "$RESTORE_SHA" \
    --argjson restore_sha256_match "$RESTORE_MATCH" \
    --argjson file_encryption true \
    --argjson encryption_key_version "$KEY_VERSION" \
    --argjson parts "$PARTS_JSON" \
    --argjson stored_size_gt_plain "$STORED_GT_PLAIN" \
    --argjson ciphertext_header "$CIPHERTEXT_HEADER" \
    --argjson document_count "$DOCUMENT_COUNT" \
    --arg snapshot_sha256 "$RESTORED_SNAPSHOT_SHA" \
    --arg payload_root_sha256 "$RESTORED_PAYLOAD_SHA" \
    --argjson negative_control_status "$NEG_STATUS" \
    --arg negative_control_code "$NEG_CODE" \
    --argjson recovered_status "$RECOVERED_STATUS" \
    --argjson recovered_sha256_match "$RECOVERED_MATCH" \
    --arg started "$STARTED" \
    --arg finished "$finished" \
    '{kind:$kind, plan:$plan, mode:$mode, outcome:$outcome, failure_reason:$reason,
      image_ref:$image_ref, image_digest:$image_digest,
      create_receipt_sha256:$create_receipt_sha256,
      database:$database, schema:$schema, app_role:$app_role,
      app_role_rolsuper:$app_role_rolsuper,
      file_id:$file_id, plain_size:$plain_size,
      create_sha256:$create_sha256, restored_sha256:$restored_sha256,
      restore_sha256_match:$restore_sha256_match,
      file_encryption:$file_encryption, encryption_key_version:$encryption_key_version,
      parts:$parts, stored_size_gt_plain:$stored_size_gt_plain,
      ciphertext_header:$ciphertext_header, document_count:$document_count,
      snapshot_sha256:$snapshot_sha256, payload_root_sha256:$payload_root_sha256,
      negative_control_status:$negative_control_status,
      negative_control_code:$negative_control_code,
      recovered_status:$recovered_status, recovered_sha256_match:$recovered_sha256_match,
      started_at:$started, finished_at:$finished}' | b7_write_receipt "$RECEIPT"
  RECEIPT_WRITTEN=true
  printf 'b7: encrypted restore PASS; receipt %s\n' "$RECEIPT"
}

negative_run() {
  restore_prepare
  stop_app
  TD_NAME=""
  write_wrong_key_env "$OUT/env-wrong" "postgres://$APP_ROLE:$APP_PW@postgres:5432/$APP_DB?sslmode=disable" "/data/local-telegram"
  start_app "$OUT/env-wrong" || fail "wrong-key TelDrive did not become ready"
  NEG_STATUS="$(download_status "$OUT/negative.bin")" || NEG_STATUS=0
  NEG_CODE="$(jq -r '.error.code // empty' "$OUT/negative.bin" 2>/dev/null || true)"
  [[ -n "$NEG_CODE" ]] || NEG_CODE="no-error-body"
  [[ "$NEG_STATUS" =~ ^[0-9]+$ ]] || NEG_STATUS=0
  (( NEG_STATUS >= 400 )) || fail "wrong-key download returned $NEG_STATUS"

  PHASE="receipt"
  local finished
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n \
    --arg kind "b7-encrypted-canary-v1" \
    --arg plan "08-02" \
    --arg mode "negative" \
    --arg outcome "PASS" \
    --arg reason "" \
    --arg image_digest "$IMAGE_DIGEST" \
    --arg create_receipt_sha256 "$CREATE_RECEIPT_SHA" \
    --argjson negative_control_status "$NEG_STATUS" \
    --arg negative_control_code "$NEG_CODE" \
    --arg started "$STARTED" \
    --arg finished "$finished" \
    '{kind:$kind, plan:$plan, mode:$mode, outcome:$outcome, failure_reason:$reason,
      image_digest:$image_digest, create_receipt_sha256:$create_receipt_sha256,
      negative_control_status:$negative_control_status,
      negative_control_code:$negative_control_code,
      started_at:$started, finished_at:$finished}' | b7_write_receipt "$RECEIPT"
  RECEIPT_WRITTEN=true
  printf 'b7: negative control PASS; receipt %s\n' "$RECEIPT"
}

case "$MODE" in
  create) create_run ;;
  restore) restore_run ;;
  negative) negative_run ;;
esac
