#!/usr/bin/env bash
# Replay one operator-captured production pg_dumpall into a fresh disposable
# PostgreSQL cluster and write a private receipt. Never touches production.
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
  --self-test                       run refusal and permission checks without Docker
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

# --- self-test: refusal paths and private file modes, no Docker -------------
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
install -d -m 0700 "$OUT"
RECEIPT="$OUT/receipt.json"
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
    --arg restored_sha256 "$RESTORED_SHA" \
    --argjson restored_bytes "$RESTORED_BYTES" \
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
      restored_dump_sha256: $restored_sha256,
      restored_dump_bytes: $restored_bytes,
      started_at: $started,
      finished_at: $finished
    }' | b7_write_receipt "$RECEIPT"
  RECEIPT_WRITTEN=true
}

on_exit() {
  local status=$?
  if [[ "$RECEIPT_WRITTEN" != true && -n "$OUT" ]]; then
    [[ -n "$FAIL_REASON" ]] || FAIL_REASON="unexpected exit $status"
    write_receipt FAIL || true
  fi
  if [[ -n "$RUNTIME" ]]; then
    b7_cleanup "$RUNTIME" "$KEEP" "" || true
  fi
}
trap on_exit EXIT
trap 'exit 130' INT TERM

DUMP_SHA="$(sha256sum "$DUMP" | cut -d' ' -f1)"
DUMP_BYTES="$(stat -c '%s' "$DUMP")"
DUMP_MTIME="$(stat -c '%y' "$DUMP")"
DUMP_HEADER="$(grep -m1 -E '^-- Dumped from database version' "$DUMP" 2>/dev/null |
  sed -E 's/^-- Dumped from database version //' || true)"

RUNTIME="$(b6_runtime)"
POSTGRES_IMAGE_ID="$("$RUNTIME" image inspect --format '{{.Id}}' "$POSTGRES_IMAGE" 2>/dev/null || true)"

# --- optional read-only production preflight ------------------------------
if [[ -n "$PROD_CONTAINER" ]]; then
  if raw="$("$RUNTIME" exec "$PROD_CONTAINER" psql -X -Atqc \
    'SELECT version(), current_database()' 2>/dev/null)" && [[ -n "$raw" ]]; then
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
fi

PHASE="receipt"
write_receipt PASS
printf 'b7: restore PASS; receipt %s\n' "$RECEIPT"
