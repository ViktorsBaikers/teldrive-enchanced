#!/usr/bin/env bash
# Shared helpers for the B7 restore checks. Source-only: defines functions and
# the cleanup trackers; it starts nothing on its own.
#
# Isolation contract: every resource is named b7-<run>-*, lives on a private
# network, and is removed by b7_cleanup. Private artifacts stay outside Git.
set -euo pipefail

B7_LIB_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../b6/lib.sh
source "$B7_LIB_DIR/../b6/lib.sh"

B7_CREATED_CONTAINERS=()
B7_CREATED_NETWORKS=()

b7_private_root() {
  printf '%s\n' "${B7_PRIVATE_DIR:-$HOME/.local/state/teldrive-b7}"
}

b7_run_id() {
  if [[ -z "${B7_RUN_ID:-}" ]]; then
    B7_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  fi
  printf '%s\n' "$B7_RUN_ID"
}

b7_name() {
  printf 'b7-%s-%s\n' "$(b7_run_id)" "$1"
}

# Refuse anything but a private regular file: no symlink, non-empty, mode 0600,
# parent directory mode 0700, readable. Prints a stable reason token on stderr.
b7_require_dump() {
  local path="$1" mode parent_mode parent
  if [[ ! -e "$path" ]]; then
    printf 'b7: dump-missing\n' >&2
    return 2
  fi
  if [[ -L "$path" ]]; then
    printf 'b7: dump-symlink\n' >&2
    return 2
  fi
  if [[ ! -f "$path" ]]; then
    printf 'b7: dump-not-regular\n' >&2
    return 2
  fi
  mode="$(stat -c '%a' "$path")"
  if [[ "$mode" != "600" ]]; then
    printf 'b7: dump-permissions: mode %s\n' "$mode" >&2
    return 2
  fi
  parent="$(dirname -- "$path")"
  parent_mode="$(stat -c '%a' "$parent")"
  if [[ "$parent_mode" != "700" ]]; then
    printf 'b7: dump-parent-permissions: mode %s\n' "$parent_mode" >&2
    return 2
  fi
  if [[ ! -s "$path" ]]; then
    printf 'b7: dump-empty\n' >&2
    return 2
  fi
  if [[ ! -r "$path" ]]; then
    printf 'b7: dump-unreadable\n' >&2
    return 2
  fi
  return 0
}

# Read one JSON document from stdin and write it atomically as a 0600 receipt.
b7_write_receipt() {
  local path="$1" tmp
  tmp="$path.tmp.$$"
  ( umask 077; cat > "$tmp" )
  if ! jq -e . "$tmp" >/dev/null 2>&1; then
    rm -f -- "$tmp"
    printf 'b7: receipt is not valid JSON\n' >&2
    return 1
  fi
  chmod 600 "$tmp"
  mv -f -- "$tmp" "$path"
}

b7_write_env_file() {
  b6_write_env_file "$@"
}

# A disposable cluster is empty when it holds no database besides the three
# system ones and no role besides the generated superuser and pg_* roles.
b7_assert_empty_cluster() {
  local runtime="$1" name="$2" count
  count="$("$runtime" exec "$name" psql -X -Atq -U postgres -d postgres -c "
SELECT (SELECT count(*) FROM pg_database
        WHERE datname NOT IN ('template0', 'template1', 'postgres'))
     + (SELECT count(*) FROM pg_roles
        WHERE rolname NOT LIKE 'pg\\_%' AND rolname <> 'postgres');")"
  if [[ "$count" != "0" ]]; then
    printf 'b7: target cluster is not empty: %s unexpected object(s)\n' "$count" >&2
    return 1
  fi
  return 0
}

b7_track_container() {
  B7_CREATED_CONTAINERS+=("$1")
  b6_track_container "$1"
}

b7_track_network() {
  B7_CREATED_NETWORKS+=("$1")
  b6_track_network "$1"
}

b7_cleanup() {
  b6_cleanup "$@"
}

# Synthetic key set for the encrypted canary. Values live only in this 0600
# file; the caller appends database and Telegram settings before use.
b7_synthetic_keys() {
  local path="$1"
  b6_write_env_file "$path" \
    "TELDRIVE_SECURITY_DATA_KEY=$(b6_data_key)" \
    "TELDRIVE_SECURITY_SIGNING_KEY=$(b6_hex32 | cut -c1-48)" \
    "TELDRIVE_ENCRYPTION_KEYS=1:$(b6_hex32)" \
    "TELDRIVE_ENCRYPTION_ACTIVE_KEY_VERSION=1"
  [[ "$(stat -c '%a' "$path")" == "600" ]] ||
    { printf 'b7: synthetic key file mode is not 0600\n' >&2; return 1; }
}

# Start the candidate against the filesystem Telegram root with workers
# disabled, wait for readiness, and print the published host port.
b7_start_canary_teldrive() {
  local runtime="$1" name="$2" image="$3" network="$4" envfile="$5" local_root="$6"
  b6_start_teldrive "$runtime" "$name" "$image" "$network" "$envfile" "$local_root"
  b7_track_container "$name"
  local port
  port="$(b6_published_port "$runtime" "$name" 8080)"
  [[ "$port" =~ ^[0-9]+$ ]] || { printf 'b7: published port is not numeric\n' >&2; return 1; }
  b6_wait_health "http://127.0.0.1:$port/health/ready" 120 || return 1
  printf '%s\n' "$port"
}

# Insert the test user, its owner incarnation, and one API key row.
b7_bootstrap_canary_user() {
  local runtime="$1" name="$2" user="$3" db="$4" schema="$5" secret="$6" key_name="$7"
  b6_bootstrap_api_key "$runtime" "$name" "$user" "$db" "$schema" "$secret" "$key_name"
}
