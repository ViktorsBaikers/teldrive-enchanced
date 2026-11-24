#!/usr/bin/env bash
# Run the B7 project gates and write a private receipt.
#
# `just` is not installed on the acceptance host, so the `check` profile runs
# every `just check` recipe body itself. `--snapshot` binds the race/rclone/
# targeted profiles to a sealed candidate: the repository HEAD and the
# `git status --porcelain` listing must equal the sealed files.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
ROOT_DIR="$(cd -- "$PROJECT_DIR/.." && pwd)"
# shellcheck source=../b6/lib.sh
source "$PROJECT_DIR/scripts/b6/lib.sh"

PROFILES=(check race rclone targeted)
TD_REPO="$PROJECT_DIR"
RC_REPO="$ROOT_DIR/rclone-enchanced"

PROFILE=""
OUT=""
SNAPSHOT=""
KEEP=false
SELF_TEST=false

usage() {
  cat >&2 <<'EOF'
usage: gates.sh --profile check|race|rclone|targeted [--out DIR] [--snapshot DIR] [--keep]
  --profile check     full project check (generate, lint, unit, integration, UI, docs)
  --profile race      integration suite with -race
  --profile rclone    rclone quicktest and racequicktest
  --profile targeted  rclone backend, chunkedreader, and vfs suites
  --out DIR           private receipt directory (default: $B7_PRIVATE_DIR/gates-<profile>-<UTC>)
  --snapshot DIR      sealed candidate; HEAD and status must match before running
  --keep              keep the temporary status snapshot directory
  --self-test         prove the profile parser, drift detector, manual command
                      list, missing-just path, and failing-gate path
EOF
  exit 2
}

parse_profile() {
  case "$1" in
    check|race|rclone|targeted) printf '%s\n' "$1"; return 0 ;;
    *) return 1 ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) [[ $# -ge 2 ]] || usage; PROFILE="$(parse_profile "$2")" || usage; shift 2 ;;
    --out) [[ $# -ge 2 ]] || usage; OUT="$2"; shift 2 ;;
    --snapshot) [[ $# -ge 2 ]] || usage; SNAPSHOT="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
    --self-test) SELF_TEST=true; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

fail() { printf 'gates: %s\n' "$*" >&2; exit 1; }

private_root() {
  printf '%s\n' "${B7_PRIVATE_DIR:-$HOME/.local/state/teldrive-b7}"
}

repo_status() {
  local repo="$1"
  ( cd -- "$repo" && git status --porcelain=v1 --untracked-files=all )
}

repo_status_hash() {
  { git -C "$1" rev-parse HEAD; repo_status "$1"; } | sha256sum | cut -d' ' -f1
}

# Prints the paths whose status differs between two porcelain files.
status_drift_paths() {
  diff -u "$1" "$2" | sed -n 's/^[+-]\([^+-].*\)$/\1/p' | sed -E 's/^.. //' | sort -u
}

# Every command body of `just check`, in recipe order. generate-api is two
# commands plus its two invariant assertions.
check_manual_commands() {
  CHECK_COMMANDS=(
    "bun run --cwd typespec generate:openapi"
    "go run github.com/ogen-go/ogen/cmd/ogen@v1.24.0 --config ogen.yml --target internal/api/gen --package gen --clean openapi/teldrive.openapi.yaml"
    "test ! -e internal/api/gen/oas_unimplemented_gen.go"
    "test \"\$(grep -c '^\\s*[A-Z][A-Za-z0-9]*(ctx context.Context' internal/api/gen/oas_server_gen.go)\" -eq \"\$(grep -c 'operationId:' openapi/teldrive.openapi.yaml)\""
    "go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0 generate"
    "go run ./internal/tools/patchsqlc"
    "bun run --cwd ui generate:api"
    "go run ./internal/tools/docsconfig"
    "go run ./internal/tools/nixconfig"
    "go mod tidy"
    "bun run --cwd typespec lint"
    "bun run --cwd ui lint"
    "go vet ./..."
    "go test ./..."
    "./scripts/test-postgres.sh go test -tags=integration ./..."
    "bun run --cwd ui build"
    "bun run --cwd ui typecheck"
    "bun run --cwd ui test"
    "bun run --cwd docs build"
  )
  CHECK_TIMEOUTS=(900 900 60 60 900 300 300 300 300 300 600 600 600 1800 2400 1200 600 1800 1200)
}

write_receipt() {
  local path="$1" tmp
  tmp="$path.tmp.$$"
  ( umask 077; cat > "$tmp" )
  jq -e . "$tmp" >/dev/null 2>&1 || { rm -f -- "$tmp"; fail "receipt is not valid JSON"; }
  chmod 600 "$tmp"
  mv -f -- "$tmp" "$path"
}

json_commands() {
  local file="$1"
  jq -s '.' "$file"
}

# Extract failing test and package names from a captured log.
json_failures() {
  local log="$1" out
  out="$(grep -E '^(--- FAIL: [^ ]+|FAIL[[:space:]]+[^ ]+)' "$log" 2>/dev/null |
    sed -E 's/^--- FAIL: ([^ ]+).*/test \1/; s/^FAIL[[:space:]]+([^[:space:]]+).*/package \1/' || true)"
  if [[ -z "$out" ]]; then
    printf '[]\n'
    return 0
  fi
  printf '%s\n' "$out" | jq -R -s 'split("\n") | map(select(length > 0))'
}

# Run one command body from DIR, capture its log, and append a JSON row to
# $ROWS. Returns the command's exit status.
run_cmd() {
  local dir="$1" label="$2" timeout_s="$3" cmd="$4"
  local log="$LOGS/$label.log" start dur rc=0
  start=$SECONDS
  ( cd -- "$dir" && timeout --signal=TERM "$timeout_s" bash -c "$cmd" ) >"$log" 2>&1 || rc=$?
  dur=$((SECONDS - start))
  chmod 600 "$log" 2>/dev/null || true
  jq -cn --arg label "$label" --arg command "$cmd" --argjson timeout "$timeout_s" \
    --argjson duration "$dur" --argjson exit "$rc" --arg log "$(basename -- "$log")" \
    --argjson failures "$(json_failures "$log")" \
    '{label:$label, command:$command, timeout_seconds:$timeout,
      duration_seconds:$duration, exit_code:$exit, log:$log, failures:$failures}' >>"$ROWS"
  return "$rc"
}

# A sealed candidate must equal the live checkout before a regression gate runs.
guard_snapshot() {
  local repo
  [[ -n "$SNAPSHOT" ]] || fail "snapshot-required: --snapshot is required for the $PROFILE profile"
  [[ -f "$SNAPSHOT/manifest.txt" && -f "$SNAPSHOT/snapshot.sha256" ]] ||
    fail "snapshot-invalid: $SNAPSHOT is not a sealed snapshot"
  for repo in teldrive-enchanced rclone-enchanced; do
    [[ -d "$SNAPSHOT/$repo" ]] || fail "snapshot-invalid: missing $repo"
    local live_head sealed_head
    live_head="$(git -C "$([ "$repo" = teldrive-enchanced ] && echo "$TD_REPO" || echo "$RC_REPO")" rev-parse HEAD)"
    sealed_head="$(cat "$SNAPSHOT/$repo/HEAD")"
    [[ "$live_head" == "$sealed_head" ]] || fail "snapshot-head-mismatch: $repo live=$live_head sealed=$sealed_head"
    local live_status="$TMP/$repo.live.status"
    repo_status "$([ "$repo" = teldrive-enchanced ] && echo "$TD_REPO" || echo "$RC_REPO")" >"$live_status"
    if ! diff -q "$SNAPSHOT/$repo/status.porcelain" "$live_status" >/dev/null; then
      fail "snapshot-status-mismatch: $repo $(status_drift_paths "$SNAPSHOT/$repo/status.porcelain" "$live_status" | tr '\n' ' ')"
    fi
  done
}

self_test() {
  local failures=0 tmp rc
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/b7-gates-selftest.XXXXXX")"
  chmod 700 "$tmp"

  local p
  for p in check race rclone targeted; do
    if [[ "$(parse_profile "$p")" == "$p" ]]; then
      printf 'self-test: PASS profile %s accepted\n' "$p"
    else
      printf 'self-test: FAIL profile %s rejected\n' "$p" >&2
      failures=$((failures + 1))
    fi
  done
  if parse_profile bogus >/dev/null 2>&1; then
    printf 'self-test: FAIL profile bogus accepted\n' >&2
    failures=$((failures + 1))
  else
    printf 'self-test: PASS profile bogus rejected\n'
  fi

  check_manual_commands
  if [[ "${#CHECK_COMMANDS[@]}" -eq 19 ]]; then
    printf 'self-test: PASS manual path lists 19 commands\n'
  else
    printf 'self-test: FAIL manual path lists %s commands\n' "${#CHECK_COMMANDS[@]}" >&2
    failures=$((failures + 1))
  fi
  if printf '%s\n' "${CHECK_COMMANDS[@]}" | grep -qF 'test ! -e internal/api/gen/oas_unimplemented_gen.go' &&
     printf '%s\n' "${CHECK_COMMANDS[@]}" | grep -qF 'operationId:'; then
    printf 'self-test: PASS both generate-api invariant assertions present\n'
  else
    printf 'self-test: FAIL generate-api invariant assertions missing\n' >&2
    failures=$((failures + 1))
  fi

  printf 'a\nb\n' >"$tmp/status-a"
  printf 'a\nb\n' >"$tmp/status-b"
  printf 'a\nc\n' >"$tmp/status-c"
  if [[ -z "$(status_drift_paths "$tmp/status-a" "$tmp/status-b")" ]]; then
    printf 'self-test: PASS identical status accepted\n'
  else
    printf 'self-test: FAIL identical status reported drift\n' >&2
    failures=$((failures + 1))
  fi
  if [[ -n "$(status_drift_paths "$tmp/status-a" "$tmp/status-c")" ]]; then
    printf 'self-test: PASS changed status rejected as drift\n'
  else
    printf 'self-test: FAIL changed status accepted\n' >&2
    failures=$((failures + 1))
  fi

  LOGS="$tmp/logs"; ROWS="$tmp/rows.jsonl"; mkdir -p "$LOGS"; : >"$ROWS"
  rc=0
  run_cmd "$tmp" selftest-fail 30 "exit 7" || rc=$?
  if [[ "$rc" -eq 7 ]] && jq -e 'select(.exit_code == 7)' "$ROWS" >/dev/null; then
    printf 'self-test: PASS failing command recorded as FAIL\n'
  else
    printf 'self-test: FAIL failing command was not recorded (rc=%s)\n' "$rc" >&2
    failures=$((failures + 1))
  fi

  printf '{"outcome":"PASS"}\n' | write_receipt "$tmp/receipt.json"
  if [[ "$(stat -c '%a' "$tmp/receipt.json")" == "600" ]]; then
    printf 'self-test: PASS receipt is 0600\n'
  else
    printf 'self-test: FAIL receipt mode is not 0600\n' >&2
    failures=$((failures + 1))
  fi

  rm -rf -- "$tmp"
  if ((failures == 0)); then
    printf 'self-test: PASS\n'
    return 0
  fi
  printf 'self-test: FAIL (%d)\n' "$failures" >&2
  return 1
}

if [[ "$SELF_TEST" == true ]]; then
  self_test
  exit $?
fi

[[ -n "$PROFILE" ]] || usage
[[ -n "$OUT" ]] || OUT="$(private_root)/gates-$PROFILE-$(date -u +%Y%m%dT%H%M%SZ)"
umask 077
b6_ensure_private_dir "$OUT"
OUT="$(cd -- "$OUT" && pwd)"
LOGS="$OUT/logs"
b6_ensure_private_dir "$LOGS"
ROWS="$OUT/commands.jsonl"
: >"$ROWS"
RECEIPT="$OUT/receipt.json"
TMP="$(mktemp -d "$OUT/.tmp.XXXXXX")"

cleanup() {
  if [[ "$KEEP" == true ]]; then
    printf 'gates: keeping temporary status directory %s\n' "$TMP" >&2
  else
    rm -rf -- "$TMP"
  fi
}
trap cleanup EXIT INT TERM

JUST_AVAILABLE=false
if command -v just >/dev/null 2>&1; then
  JUST_AVAILABLE=true
fi

STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
START_EPOCH=$SECONDS

# Status before the run, per repository.
repo_status "$TD_REPO" >"$TMP/td.before.status"
repo_status "$RC_REPO" >"$TMP/rc.before.status"
TD_BEFORE="$(repo_status_hash "$TD_REPO")"
RC_BEFORE="$(repo_status_hash "$RC_REPO")"

case "$PROFILE" in
  check)
    if [[ "$JUST_AVAILABLE" == true ]]; then
      run_cmd "$TD_REPO" check-just 3600 "just check" || true
    else
      check_manual_commands
      for i in "${!CHECK_COMMANDS[@]}"; do
        label="$(printf 'check-%02d' "$((i + 1))")"
        run_cmd "$TD_REPO" "$label" "${CHECK_TIMEOUTS[$i]}" "${CHECK_COMMANDS[$i]}" || true
      done
    fi
    ;;
  race)
    guard_snapshot
    if [[ "$JUST_AVAILABLE" == true ]]; then
      run_cmd "$TD_REPO" race-just 2400 "just test-race" || true
    else
      run_cmd "$TD_REPO" race 2400 "./scripts/test-postgres.sh go test -race -tags=integration ./..." || true
    fi
    ;;
  rclone)
    guard_snapshot
    run_cmd "$RC_REPO" quicktest 1200 "make quicktest" || true
    run_cmd "$RC_REPO" racequicktest 1200 "make racequicktest" || true
    ;;
  targeted)
    guard_snapshot
    run_cmd "$RC_REPO" targeted 1800 "go test ./backend/teldrive/... ./fs/chunkedreader/... ./vfs/... -count=1" || true
    ;;
esac

repo_status "$TD_REPO" >"$TMP/td.after.status"
repo_status "$RC_REPO" >"$TMP/rc.after.status"
TD_AFTER="$(repo_status_hash "$TD_REPO")"
RC_AFTER="$(repo_status_hash "$RC_REPO")"

GENERATED_DRIFT=false
DRIFT_PATHS=""
if ! diff -q "$TMP/td.before.status" "$TMP/td.after.status" >/dev/null; then
  GENERATED_DRIFT=true
  DRIFT_PATHS+="teldrive-enchanted:$( { status_drift_paths "$TMP/td.before.status" "$TMP/td.after.status" || true; } | tr '\n' ' ');"
fi
if ! diff -q "$TMP/rc.before.status" "$TMP/rc.after.status" >/dev/null; then
  GENERATED_DRIFT=true
  DRIFT_PATHS+="rclone-enchanced:$( { status_drift_paths "$TMP/rc.before.status" "$TMP/rc.after.status" || true; } | tr '\n' ' ');"
fi

COMMAND_COUNT="$(jq -s 'length' "$ROWS")"
FAILED_COUNT="$(jq -s '[.[] | select(.exit_code != 0)] | length' "$ROWS")"
FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
DURATION=$((SECONDS - START_EPOCH))

SNAPSHOT_JSON="null"
if [[ -n "$SNAPSHOT" ]]; then
  SNAPSHOT_JSON="$(jq -n --arg path "$SNAPSHOT" --arg digest "$(cat "$SNAPSHOT/snapshot.sha256")" \
    '{path:$path, snapshot_sha256:$digest}')"
fi

OUTCOME=PASS
if [[ "$FAILED_COUNT" -ne 0 || "$GENERATED_DRIFT" == true ]]; then
  OUTCOME=FAIL
fi

jq -n \
  --arg kind "b7-gates-v1" \
  --arg profile "$PROFILE" \
  --argjson just_available "$JUST_AVAILABLE" \
  --argjson generated_drift "$GENERATED_DRIFT" \
  --arg drift_paths "$DRIFT_PATHS" \
  --arg td_before "$TD_BEFORE" --arg td_after "$TD_AFTER" \
  --arg rc_before "$RC_BEFORE" --arg rc_after "$RC_AFTER" \
  --argjson snapshot "$SNAPSHOT_JSON" \
  --argjson commands "$(json_commands "$ROWS")" \
  --argjson command_count "$COMMAND_COUNT" \
  --argjson failed_count "$FAILED_COUNT" \
  --arg started "$STARTED_AT" --arg finished "$FINISHED_AT" \
  --argjson duration "$DURATION" --arg outcome "$OUTCOME" \
  '{kind:$kind, profile:$profile, just_available:$just_available,
    generated_drift:$generated_drift, drift_paths:$drift_paths,
    status_hashes:{before:{teldrive:$td_before, rclone:$rc_before},
                   after:{teldrive:$td_after, rclone:$rc_after}},
    snapshot:$snapshot, command_count:$command_count, failed_count:$failed_count,
    commands:$commands, started_at:$started, finished_at:$finished,
    duration_seconds:$duration, outcome:$outcome}' | write_receipt "$RECEIPT"

printf 'GATES %s profile=%s commands=%s failed=%s generated_drift=%s receipt=%s\n' \
  "$OUTCOME" "$PROFILE" "$COMMAND_COUNT" "$FAILED_COUNT" "$GENERATED_DRIFT" "$RECEIPT"

if [[ "$GENERATED_DRIFT" == true ]]; then
  printf 'gates: generated-drift: %s\n' "$DRIFT_PATHS" >&2
fi
if [[ "$FAILED_COUNT" -ne 0 ]]; then
  jq -r '.[] | select(.exit_code != 0) |
    "gates: FAIL \(.label) exit=\(.exit_code) \(.command)" +
    (if (.failures | length) > 0 then " [\(.failures | join(", "))]" else "" end)' \
    < <(jq -s '.' "$ROWS") >&2
fi
[[ "$OUTCOME" == PASS ]]
