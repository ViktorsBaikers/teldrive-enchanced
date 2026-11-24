#!/usr/bin/env bash
set -euo pipefail

image="${TELDRIVE_POSTGRES_IMAGE:-ghcr.io/tgdrive/postgres:18}"
name="teldrive-test-${USER:-user}-$$"
user="teldrive"
password="teldrive"
database="teldrive_test"
network=""
approval_dir=""
ci_approval="${B2_CI_APPROVAL:-0}"

runtime="${TELDRIVE_CONTAINER_RUNTIME:-}"
if [[ -z "$runtime" ]]; then
  if command -v podman >/dev/null 2>&1; then
    runtime=podman
  elif command -v docker >/dev/null 2>&1; then
    runtime=docker
  else
    echo "podman or docker is required" >&2
    exit 1
  fi
elif ! command -v "$runtime" >/dev/null 2>&1; then
  echo "requested container runtime '$runtime' is not available" >&2
  exit 1
fi

cleanup() {
  "$runtime" rm -f "$name" >/dev/null 2>&1 || true
  if [[ -n "$network" ]]; then
    "$runtime" network rm "$network" >/dev/null 2>&1 || true
  fi
  if [[ -n "$approval_dir" ]]; then
    rm -rf -- "$approval_dir"
  fi
}
trap cleanup EXIT INT TERM

run_args=(run -d --name "$name")
postgres_args=()
if [[ "$ci_approval" == "1" ]]; then
  # CI runs a disposable, isolated cluster under an explicit workflow authority.
  # The pinned image and generated approval bind the exact container identity.
  image="postgres:18-alpine@sha256:b6972e1115a4ff8eba5d7f7d19184926d1e16d7d1136ae1e1a7ebf286da0f015"
  user="b2_audit"
  database="postgres"
  run_id="$(printf '%s' "$(date +%s%N)-$$" | sha256sum | cut -c1-32)"
  network="b2-ci-$run_id"
  "$runtime" network create "$network" >/dev/null
  run_args+=(--network "$network")
  postgres_args+=(-c "cluster_name=b2-ci-$run_id" -c "max_prepared_transactions=64")
fi

"$runtime" "${run_args[@]}" \
  -e POSTGRES_USER="$user" \
  -e POSTGRES_PASSWORD="$password" \
  -e POSTGRES_DB="$database" \
  -p 127.0.0.1::5432 \
  "$image" "${postgres_args[@]}" >/dev/null

if [[ "$ci_approval" == "1" ]]; then
  host="$("$runtime" inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$name")"
  port=5432
else
  host=127.0.0.1
  port="$("$runtime" port "$name" 5432/tcp | awk -F: '{print $NF}')"
fi
if [[ -z "$host" || -z "$port" ]]; then
  echo "failed to determine PostgreSQL endpoint" >&2
  "$runtime" logs "$name" >&2 || true
  exit 1
fi

# The official PostgreSQL entrypoint starts a temporary Unix-socket-only server
# during initialization and then restarts PostgreSQL normally. Probing the local
# socket can therefore report ready too early. Requiring a TCP SQL query avoids
# that restart race and verifies the configured user and database as well.
ready=false
for _ in $(seq 1 120); do
  if "$runtime" exec -e PGPASSWORD="$password" "$name" \
    psql -h 127.0.0.1 -U "$user" -d "$database" -Atqc 'SELECT 1' \
    2>/dev/null | grep -qx '1'; then
    ready=true
    break
  fi
  sleep 0.25
done

if [[ "$ready" != true ]]; then
  echo "PostgreSQL did not become ready" >&2
  "$runtime" logs "$name" >&2 || true
  exit 1
fi

if [[ "$ci_approval" == "1" ]]; then
  container_id="$("$runtime" inspect -f '{{.Id}}' "$name" | sed 's/^sha256://')"
  network_id="$("$runtime" network inspect -f '{{.Id}}' "$network")"
  system_id="$("$runtime" exec -e PGPASSWORD="$password" "$name" \
    psql -h 127.0.0.1 -U "$user" -d postgres -Atqc 'select system_identifier from pg_control_system()')"
  approval_dir="$(mktemp -d "${TMPDIR:-/tmp}/b2-ci-approval-XXXXXX")"
  chmod 700 "$approval_dir"
  expires_at="$(date -u -d '+2 hours' +%Y-%m-%dT%H:%M:%SZ)"
  namespace="teldrive_test_b2_${run_id:0:8}_"
  cat >"$approval_dir/approval.json" <<EOF
{"version":1,"run_id":"$run_id","expires_at":"$expires_at","host":"$host","port":$port,"role":"$user","cluster_name":"b2-ci-$run_id","system_identifier":"$system_id","namespace":"$namespace","tools":{"executor":"docker-exec","container_id":"$container_id","image_id":"sha256:b6972e1115a4ff8eba5d7f7d19184926d1e16d7d1136ae1e1a7ebf286da0f015","network_id":"$network_id","run_tag":"$run_id","psql":"/usr/local/bin/psql","pg_dump":"/usr/local/bin/pg_dump","pg_restore":"/usr/local/bin/pg_restore"}}
EOF
  chmod 600 "$approval_dir/approval.json"
  export B2_ISOLATION_APPROVAL="$approval_dir/approval.json"
fi

export TEST_DATABASE_URL="postgres://${user}:${password}@$host:${port}/${database}?sslmode=disable"

if [[ "$#" -eq 0 ]]; then
  set -- go test -p 1 -tags=integration ./...
fi

"$@"
