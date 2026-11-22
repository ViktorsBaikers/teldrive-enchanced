#!/usr/bin/env bash
# Prove the Docker TelDrive + PostgreSQL path on Linux with disposable,
# isolated resources. Writes a private receipt; never touches production.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

IMAGE=""
SNAPSHOT=""
POSTGRES_IMAGE="postgres:18-alpine@sha256:b6972e1115a4ff8eba5d7f7d19184926d1e16d7d1136ae1e1a7ebf286da0f015"
RECEIPT=""
OUT=""
KEEP=false

usage() {
  cat >&2 <<'EOF'
usage: linux-docker-check.sh --image TAG --snapshot DIR [options]
Starts disposable PostgreSQL and TelDrive, proves readiness and one
authenticated /api/v1/me call, then writes a private receipt.
  --image TAG            sealed TelDrive image to exercise (required)
  --snapshot DIR         sealed snapshot the image was built from (required)
  --postgres-image REF   digest-pinned PostgreSQL image
  --out DIR              directory for the receipt when --receipt is absent
  --receipt PATH         explicit private receipt path
  --keep                 keep containers and the private work root
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image) [[ $# -ge 2 ]] || usage; IMAGE="$2"; shift 2 ;;
    --snapshot) [[ $# -ge 2 ]] || usage; SNAPSHOT="$2"; shift 2 ;;
    --postgres-image) [[ $# -ge 2 ]] || usage; POSTGRES_IMAGE="$2"; shift 2 ;;
    --out) [[ $# -ge 2 ]] || usage; OUT="$2"; shift 2 ;;
    --receipt) [[ $# -ge 2 ]] || usage; RECEIPT="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

[[ -n "$IMAGE" && -n "$SNAPSHOT" ]] || usage

# A production variable in the caller environment could silently redirect this
# disposable run. Refuse before creating anything.
if inherited="$(env | cut -d= -f1 | grep -E '^TELDRIVE_' || true)" && [[ -n "$inherited" ]]; then
  printf 'b6: refusing to run with TelDrive variables present in the caller environment:\n%s\n' \
    "$inherited" >&2
  exit 1
fi

RUNTIME="$(b6_runtime)"
[[ -d "$SNAPSHOT" ]] || { printf 'b6: snapshot directory not found: %s\n' "$SNAPSHOT" >&2; exit 1; }
SNAP_ABS="$(cd -- "$SNAPSHOT" && pwd)"
SNAP_PARENT="$(dirname -- "$SNAP_ABS")"
[[ -f "$SNAP_ABS/snapshot.sha256" && -f "$SNAP_ABS/manifest.txt" ]] ||
  { printf 'b6: snapshot manifest missing; reseal with seal-snapshot.sh\n' >&2; exit 1; }
SNAP_DIGEST="$(cat "$SNAP_ABS/snapshot.sha256")"
[[ "$SNAP_DIGEST" =~ ^[0-9a-f]{64}$ ]] ||
  { printf 'b6: snapshot digest is not a sha256\n' >&2; exit 1; }
SNAP_SHA="$(sha256sum "$SNAP_ABS/snapshot.sha256" | cut -d' ' -f1)"
# Exactly one explicitly named snapshot directory; never a lexical "latest".
snapshot_dirs="$(find "$SNAP_PARENT" -mindepth 1 -maxdepth 1 -type d -name 'snapshot-*' | wc -l | tr -d ' ')"
[[ "$snapshot_dirs" == 1 ]] ||
  { printf 'b6: expected exactly one snapshot-* directory in %s\n' "$SNAP_PARENT" >&2; exit 1; }
[[ "$(find "$SNAP_PARENT" -mindepth 1 -maxdepth 1 -type d -name 'snapshot-*')" == "$SNAP_ABS" ]] ||
  { printf 'b6: snapshot argument is not the single sealed snapshot\n' >&2; exit 1; }

IMAGE_ID="$("$RUNTIME" image inspect --format '{{.Id}}' "$IMAGE" 2>/dev/null)" ||
  { printf 'b6: image not found: %s\n' "$IMAGE" >&2; exit 1; }
[[ "$IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]] ||
  { printf 'b6: image ID is not a sha256 digest\n' >&2; exit 1; }
IMAGE_SNAP_LABEL="$("$RUNTIME" image inspect \
  --format '{{index .Config.Labels "io.enchanced.source.snapshot"}}' "$IMAGE")"
[[ "$IMAGE_SNAP_LABEL" == "$SNAP_DIGEST" ]] ||
  { printf 'b6: image snapshot label does not match the supplied snapshot\n' >&2; exit 1; }
POSTGRES_IMAGE_ID="$("$RUNTIME" image inspect --format '{{.Id}}' "$POSTGRES_IMAGE" 2>/dev/null || true)"

PRIV="$(b6_private_root)"
install -d -m 0700 "$PRIV"
if [[ -n "$OUT" ]]; then
  install -d -m 0700 "$OUT"
fi
if [[ -z "$RECEIPT" ]]; then
  RECEIPT="${OUT:-$PRIV}/linux-docker-receipt.json"
fi
install -d -m 0700 "$(dirname -- "$RECEIPT")"

umask 077
WORK="$PRIV/b6-$(b6_run_id)-work"
mkdir -m 700 "$WORK"
mkdir -m 700 "$WORK/telegram-root"

DB_NAME="teldrive_b6"
DB_USER="b6"
SCHEMA="b6_probe"
PG_NAME="$(b6_name postgres)"
TD_NAME="$(b6_name teldrive)"
NETWORK="$(b6_name net)"
PG_PORT=0
HTTP_PORT=0
LIVE_CODE=0
READY_CODE=0
API_READY_CODE=0
ME_CODE=0
USER_ID=0
REASON=""
RECEIPT_WRITTEN=false
START_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
DEADLINE=$((SECONDS + 180))

fail() {
  REASON="$1"
  printf 'b6: %s\n' "$1" >&2
  exit 1
}

write_receipt() {
  local outcome="$1" reason="$2"
  local finished duration go_version runtime_version host_os tmp
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  duration=$((SECONDS))
  go_version="$(go version 2>/dev/null || true)"
  runtime_version="$("$RUNTIME" version --format '{{.Server.Version}}' 2>/dev/null ||
    "$RUNTIME" --version 2>/dev/null || true)"
  host_os="$(uname -srm)"
  port_num() { [[ "${1:-}" =~ ^[0-9]+$ ]] && printf '%s' "$1" || printf '0'; }
  local commands
  commands="$(jq -n --args '$ARGS.positional' -- \
    "$RUNTIME network create $NETWORK" \
    "$RUNTIME run -d --name $PG_NAME --network $NETWORK --network-alias postgres --env-file <private>/postgres.env -p 127.0.0.1::5432 $POSTGRES_IMAGE" \
    "$RUNTIME run -d --name $TD_NAME --network $NETWORK --user $(id -u):$(id -g) --env-file <private>/teldrive.env --mount type=bind,source=<private>/telegram-root,target=/data/local-telegram -p 127.0.0.1::8080 $IMAGE" \
    "curl -fsS http://127.0.0.1:$HTTP_PORT/health/live" \
    "curl -fsS http://127.0.0.1:$HTTP_PORT/health/ready" \
    "curl -fsS http://127.0.0.1:$HTTP_PORT/api/health/ready" \
    "psql: INSERT users/ensure_event_owner_incarnation/api_keys in schema $SCHEMA" \
    "curl -fsS -H 'X-Api-Key: <redacted>' http://127.0.0.1:$HTTP_PORT/api/v1/me")"
  tmp="$RECEIPT.tmp.$$"
  jq -n \
    --arg plan "07-05" \
    --arg outcome "$outcome" \
    --arg reason "$reason" \
    --arg snapshot_path "$SNAP_ABS" \
    --arg snapshot_sha256 "$SNAP_SHA" \
    --arg snapshot_digest "$SNAP_DIGEST" \
    --arg server_image_tag "$IMAGE" \
    --arg server_image_digest "$IMAGE_ID" \
    --arg postgres_image "$POSTGRES_IMAGE" \
    --arg postgres_image_digest "$POSTGRES_IMAGE_ID" \
    --arg network "$NETWORK" \
    --arg database "$DB_NAME" \
    --arg schema "$SCHEMA" \
    --arg postgres_container "$PG_NAME" \
    --arg teldrive_container "$TD_NAME" \
    --argjson postgres_port "$(port_num "$PG_PORT")" \
    --argjson http_port "$(port_num "$HTTP_PORT")" \
    --argjson live "$(port_num "$LIVE_CODE")" \
    --argjson ready "$(port_num "$READY_CODE")" \
    --argjson api_ready "$(port_num "$API_READY_CODE")" \
    --argjson me_status "$(port_num "$ME_CODE")" \
    --argjson user_id "$(port_num "$USER_ID")" \
    --arg started "$START_ISO" \
    --arg finished "$finished" \
    --argjson duration_seconds "$duration" \
    --arg go_version "$go_version" \
    --arg runtime_version "$runtime_version" \
    --arg host_os "$host_os" \
    --argjson commands "$commands" \
    '{
      plan: $plan,
      outcome: $outcome,
      reason: $reason,
      snapshot_path: $snapshot_path,
      snapshot_sha256: $snapshot_sha256,
      snapshot_digest: $snapshot_digest,
      server_image_tag: $server_image_tag,
      server_image_digest: $server_image_digest,
      postgres_image: $postgres_image,
      postgres_image_digest: $postgres_image_digest,
      network: $network,
      database: $database,
      schema: $schema,
      containers: {
        postgres: {name: $postgres_container, port: $postgres_port},
        teldrive: {name: $teldrive_container, port: $http_port}
      },
      health: {
        live: $live,
        ready: $ready,
        api_ready: $api_ready
      },
      api: {endpoint: "/api/v1/me", status: $me_status, user_id: $user_id},
      impact_limits: {seconds: 180, bytes: 8589934592},
      environment: {
        allowlist: [
          "TELDRIVE_DATABASE_URL",
          "TELDRIVE_DATABASE_SCHEMA",
          "TELDRIVE_SECURITY_SIGNING_KEY",
          "TELDRIVE_SECURITY_DATA_KEY",
          "TELDRIVE_TELEGRAM_BACKEND",
          "TELDRIVE_TELEGRAM_LOCAL_ROOT",
          "TELDRIVE_HTTP_ADDRESS"
        ]
      },
      commands: $commands,
      versions: {go: $go_version, container_runtime: $runtime_version, host: $host_os},
      started_at: $started,
      finished_at: $finished,
      duration_seconds: $duration_seconds
    }' > "$tmp"
  chmod 600 "$tmp"
  mv -f "$tmp" "$RECEIPT"
  RECEIPT_WRITTEN=true
}

on_exit() {
  local status=$?
  if [[ "$RECEIPT_WRITTEN" != true && -n "$RECEIPT" && -n "$IMAGE_ID" ]]; then
    write_receipt FAIL "${REASON:-unexpected exit $status}" || true
  fi
  if [[ "$status" -ne 0 && -d "$WORK" ]]; then
    local name
    for name in "${B6_CREATED_CONTAINERS[@]}"; do
      [[ -n "$name" ]] && "$RUNTIME" logs "$name" > "$WORK/$name.log" 2>&1 || true
    done
    chmod 600 "$WORK"/*.log 2>/dev/null || true
    printf 'b6: failure; private work root kept at %s (not published)\n' "$WORK" >&2
  fi
  local keep="$KEEP"
  [[ "$status" -ne 0 ]] && keep=true
  b6_cleanup "$RUNTIME" "$keep" "$WORK"
}
trap on_exit EXIT
trap 'exit 130' INT TERM

remaining() {
  local left=$((DEADLINE - SECONDS))
  printf '%s' "$left"
}

# --- disposable network and database ---------------------------------------
"$RUNTIME" network create "$NETWORK" >/dev/null
b6_track_network "$NETWORK"

PG_PASSWORD="$(b6_hex32)"
b6_write_env_file "$WORK/postgres.env" \
  "POSTGRES_USER=$DB_USER" \
  "POSTGRES_PASSWORD=$PG_PASSWORD" \
  "POSTGRES_DB=$DB_NAME"

b6_start_postgres "$RUNTIME" "$PG_NAME" "$POSTGRES_IMAGE" "$NETWORK" "$WORK/postgres.env"
PG_PORT="$(b6_published_port "$RUNTIME" "$PG_NAME" 5432)"
[[ "$PG_PORT" =~ ^[0-9]+$ ]] || fail "could not determine the disposable PostgreSQL port"

left="$(remaining)"
(( left > 0 )) || fail "readiness timeout (180 s limit)"
b6_wait_postgres "$RUNTIME" "$PG_NAME" "$DB_USER" "$DB_NAME" "$left" ||
  fail "readiness timeout (PostgreSQL did not become ready)"

# --- disposable TelDrive ----------------------------------------------------
SIGNING_KEY="$(b6_hex32)"
DATA_KEY="$(b6_data_key)"
DB_URL="postgres://$DB_USER:$PG_PASSWORD@postgres:5432/$DB_NAME?sslmode=disable"
b6_write_env_file "$WORK/teldrive.env" \
  "TELDRIVE_DATABASE_URL=$DB_URL" \
  "TELDRIVE_DATABASE_SCHEMA=$SCHEMA" \
  "TELDRIVE_SECURITY_SIGNING_KEY=$SIGNING_KEY" \
  "TELDRIVE_SECURITY_DATA_KEY=$DATA_KEY" \
  "TELDRIVE_TELEGRAM_BACKEND=filesystem" \
  "TELDRIVE_TELEGRAM_LOCAL_ROOT=/data/local-telegram" \
  "TELDRIVE_HTTP_ADDRESS=0.0.0.0:8080"

b6_start_teldrive "$RUNTIME" "$TD_NAME" "$IMAGE" "$NETWORK" \
  "$WORK/teldrive.env" "$WORK/telegram-root"
HTTP_PORT="$(b6_published_port "$RUNTIME" "$TD_NAME" 8080)"
[[ "$HTTP_PORT" =~ ^[0-9]+$ ]] || fail "could not determine the disposable TelDrive port"

BASE_URL="http://127.0.0.1:$HTTP_PORT"
left="$(remaining)"
(( left > 0 )) || fail "readiness timeout (180 s limit)"
b6_wait_health "$BASE_URL/health/live" "$left" ||
  fail "readiness timeout (/health/live)"

left="$(remaining)"
(( left > 0 )) || fail "readiness timeout (180 s limit)"
b6_wait_health "$BASE_URL/health/ready" "$left" ||
  fail "readiness timeout (/health/ready)"

LIVE_CODE="$(curl -sS --max-time 5 -o "$WORK/live.json" -w '%{http_code}' "$BASE_URL/health/live")" ||
  fail "readiness timeout (/health/live)"
READY_CODE="$(curl -sS --max-time 5 -o "$WORK/ready.json" -w '%{http_code}' "$BASE_URL/health/ready")" ||
  fail "readiness timeout (/health/ready)"
API_READY_CODE="$(curl -sS --max-time 5 -o "$WORK/api-ready.json" -w '%{http_code}' "$BASE_URL/api/health/ready")" ||
  fail "readiness timeout (/api/health/ready)"
[[ "$LIVE_CODE" == 200 && "$READY_CODE" == 200 && "$API_READY_CODE" == 200 ]] ||
  fail "readiness endpoints returned live=$LIVE_CODE ready=$READY_CODE api=$API_READY_CODE"

b6_assert_env_allowlist "$RUNTIME" "$TD_NAME" \
  TELDRIVE_DATABASE_URL TELDRIVE_DATABASE_SCHEMA \
  TELDRIVE_SECURITY_SIGNING_KEY TELDRIVE_SECURITY_DATA_KEY \
  TELDRIVE_TELEGRAM_BACKEND TELDRIVE_TELEGRAM_LOCAL_ROOT \
  TELDRIVE_HTTP_ADDRESS || fail "resolved container environment contains a non-allowlisted variable"

# --- test-only credential and authenticated call ---------------------------
# The filesystem backend has no login path, so the API key is bootstrapped
# directly in the disposable database.
API_SECRET="$(b6_api_secret)"
printf '%s\n' "$API_SECRET" > "$WORK/api-key"
chmod 600 "$WORK/api-key"
b6_bootstrap_api_key "$RUNTIME" "$PG_NAME" "$DB_USER" "$DB_NAME" "$SCHEMA" "$API_SECRET" \
  "b6-linux-docker" || fail "API key bootstrap failed"

ME_CODE="$(curl -sS --max-time 10 -o "$WORK/me.json" -w '%{http_code}' \
  -H "X-Api-Key: $API_SECRET" "$BASE_URL/api/v1/me")" ||
  fail "authenticated /api/v1/me request failed"
[[ "$ME_CODE" == 200 ]] || fail "/api/v1/me returned status $ME_CODE"
USER_ID="$(jq -r '.userId // 0' "$WORK/me.json")"
[[ "$USER_ID" =~ ^[0-9]+$ ]] && (( USER_ID > 0 )) ||
  fail "/api/v1/me did not return a non-zero userId"

left="$(remaining)"
(( left > 0 )) || fail "180 s limit reached before completion"

write_receipt PASS ""
printf '%s\n' "$SNAP_PARENT" > "$PRIV/linux-docker-current"
chmod 600 "$PRIV/linux-docker-current"

printf 'LINUX-DOCKER PASS snapshot_sha256=%s server_image_digest=%s receipt=%s\n' \
  "$SNAP_SHA" "$IMAGE_ID" "$RECEIPT"
