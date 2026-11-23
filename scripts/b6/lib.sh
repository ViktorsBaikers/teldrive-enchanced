#!/usr/bin/env bash
# Shared helpers for disposable b6 container checks. Source-only: defines
# functions and the cleanup trackers; it starts nothing on its own.
#
# Isolation contract: every resource this library creates is named b6-<run>-*,
# lives on a private network, and is removed by b6_cleanup. No production
# TelDrive variable is read from the caller environment; the check script
# refuses to run if one is present.
set -euo pipefail

B6_CREATED_CONTAINERS=()
B6_CREATED_NETWORKS=()

b6_run_id() {
  if [[ -z "${B6_RUN_ID:-}" ]]; then
    B6_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  fi
  printf '%s\n' "$B6_RUN_ID"
}

b6_name() {
  printf 'b6-%s-%s\n' "$(b6_run_id)" "$1"
}

b6_private_root() {
  printf '%s\n' "${B6_PRIVATE_DIR:-$HOME/.local/state/teldrive-b6}"
}

# Create DIR as 0700, or accept an existing directory only when it is already
# private. A caller-owned directory is never chmodded (IN-04).
b6_ensure_private_dir() {
  local dir="$1" mode
  if [[ -L "$dir" ]]; then
    printf 'b6: refusing symlinked directory: %s\n' "$dir" >&2
    return 1
  fi
  if [[ -e "$dir" ]]; then
    [[ -d "$dir" ]] || { printf 'b6: not a directory: %s\n' "$dir" >&2; return 1; }
    mode="$(stat -c %a -- "$dir")"
    [[ "$mode" == 700 ]] ||
      { printf 'b6: out-dir-permissions: %s has mode %s, want 700\n' "$dir" "$mode" >&2; return 1; }
    return 0
  fi
  mkdir -p -m 700 -- "$dir"
}

b6_runtime() {
  local candidate
  for candidate in ${B6_CONTAINER_RUNTIME:-} docker podman; do
    [[ -n "$candidate" ]] || continue
    if command -v "$candidate" >/dev/null 2>&1; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  printf 'b6: docker or podman is required\n' >&2
  return 1
}

b6_hex32() {
  od -An -N32 -tx1 /dev/urandom | tr -d ' \n'
}

# 32 random bytes in standard base64, accepted by secureblob.Cipher.
b6_data_key() {
  head -c 32 /dev/urandom | base64 -w0
}

# Opaque API key shaped like authn.newOpaqueToken ("tdk_" + raw URL base64).
b6_api_secret() {
  printf 'tdk_%s\n' "$(head -c 32 /dev/urandom | base64 -w0 | tr '+/' '-_' | tr -d '=')"
}

# Write KEY=VALUE pairs to a 0600 env file so secrets never appear in argv.
b6_write_env_file() {
  local path="$1"
  shift
  local pair
  ( umask 077; : > "$path" )
  for pair in "$@"; do
    [[ "$pair" == *=* ]] || { printf 'b6: env entry lacks =: %s\n' "${pair%%=*}" >&2; return 1; }
    printf '%s\n' "$pair" >> "$path"
  done
  chmod 600 "$path"
}

b6_track_container() { B6_CREATED_CONTAINERS+=("$1"); }
b6_track_network() { B6_CREATED_NETWORKS+=("$1"); }

b6_start_postgres() {
  local runtime="$1" name="$2" image="$3" network="$4" envfile="$5"
  "$runtime" run -d --name "$name" --network "$network" --network-alias postgres \
    --env-file "$envfile" -p 127.0.0.1::5432 "$image" >/dev/null
  b6_track_container "$name"
}

b6_wait_postgres() {
  local runtime="$1" name="$2" user="$3" db="$4" timeout="$5"
  local start=$SECONDS
  while (( SECONDS - start < timeout )); do
    # TCP must accept before the final server starts; the socket query then
    # confirms the configured user and database, not the init-time server.
    if "$runtime" exec "$name" pg_isready -h 127.0.0.1 -p 5432 -U "$user" -d "$db" >/dev/null 2>&1 &&
      [[ "$("$runtime" exec "$name" psql -U "$user" -d "$db" -Atqc 'SELECT 1' 2>/dev/null)" == 1 ]]; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

b6_start_teldrive() {
  local runtime="$1" name="$2" image="$3" network="$4" envfile="$5" local_root="$6"
  # Run as the caller so the bind-mounted filesystem Telegram root stays writable
  # for the distroless nonroot image without loosening directory permissions.
  "$runtime" run -d --name "$name" --network "$network" \
    --user "$(id -u):$(id -g)" --env-file "$envfile" \
    --mount "type=bind,source=$local_root,target=/data/local-telegram" \
    -p 127.0.0.1::8080 "$image" >/dev/null
  b6_track_container "$name"
}

b6_published_port() {
  local runtime="$1" name="$2" container_port="$3"
  "$runtime" port "$name" "$container_port/tcp" | awk -F: 'NR==1 {print $NF}'
}

b6_wait_health() {
  local url="$1" timeout="$2"
  local start=$SECONDS
  while (( SECONDS - start < timeout )); do
    if curl -fsS --max-time 3 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

# Create the owner user, its ordered-event incarnation, and one API key row.
# The secret is hashed with public.digest to match authn.hashToken.
b6_bootstrap_api_key() {
  local runtime="$1" name="$2" user="$3" db="$4" schema="$5" secret="$6" key_name="$7"
  [[ "$schema" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || { printf 'b6: invalid schema name\n' >&2; return 1; }
  "$runtime" exec -i "$name" psql -v ON_ERROR_STOP=1 -q -U "$user" -d "$db" <<SQL
INSERT INTO "$schema".users (user_id, display_name, premium, role)
VALUES (1001, 'b6', false, 'owner')
ON CONFLICT (user_id) DO NOTHING;
SELECT "$schema".ensure_event_owner_incarnation('$schema', 1001);
INSERT INTO "$schema".api_keys (user_id, name, key_prefix, secret_hash)
SELECT 1001, '$key_name', substring('$secret', 1, 16),
       public.digest('$secret', 'sha256')
WHERE NOT EXISTS (
  SELECT 1 FROM "$schema".api_keys
  WHERE secret_hash = public.digest('$secret', 'sha256')
);
SQL
}

# Fail closed if the resolved container carries a TELDRIVE_ variable that the
# caller did not explicitly allow.
b6_assert_env_allowlist() {
  local runtime="$1" name="$2"
  shift 2
  local allowed=" $* " key
  while IFS= read -r key; do
    [[ "$key" == TELDRIVE_* ]] || continue
    [[ "$allowed" == *" $key "* ]] ||
      { printf 'b6: unexpected TELDRIVE_ variable in %s: %s\n' "$name" "$key" >&2; return 1; }
  done < <("$runtime" inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$name" | cut -d= -f1)
}

b6_cleanup() {
  local runtime="$1" keep="$2" work="$3"
  local name
  for name in "${B6_CREATED_CONTAINERS[@]}"; do
    [[ -n "$name" ]] && "$runtime" rm -f "$name" >/dev/null 2>&1 || true
  done
  for name in "${B6_CREATED_NETWORKS[@]}"; do
    [[ -n "$name" ]] && "$runtime" network rm "$name" >/dev/null 2>&1 || true
  done
  if [[ "$keep" != true && -n "$work" ]]; then
    chmod -R u+rwX "$work" 2>/dev/null || true
    rm -rf -- "$work"
  fi
}
