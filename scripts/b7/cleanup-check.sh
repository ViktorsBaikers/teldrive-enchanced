#!/usr/bin/env bash
# Manifest-bound cleanup for B7. The manifest is the sole deletion authority:
# only exact, validated paths and names listed in it are removed. Release
# inputs and the media root are never deletion targets.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

PRIVATE_ROOT="${B7_PRIVATE_DIR:-$HOME/.local/state/teldrive-b7}"
B6_ROOT="${B6_PRIVATE_DIR:-$HOME/.local/state/teldrive-b6}"
MEDIA_ROOT="${B7_MEDIA_ROOT:-$HOME/rclone/dropbox}"
VFS_CACHE="${B7_VFS_CACHE:-$HOME/.teldrive/cache}"

MANIFEST=""
OUT=""
MODE=""
RELEASE_INPUTS=()

usage() {
  cat >&2 <<'EOF'
usage: cleanup-check.sh --manifest PATH (--dry-run|--execute) [--out DIR]
       cleanup-check.sh --collect [--manifest PATH] [--out DIR] [--release-input PATH ...]
       cleanup-check.sh --self-test
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest) [[ $# -ge 2 ]] || usage; MANIFEST="$2"; shift 2 ;;
    --out) [[ $# -ge 2 ]] || usage; OUT="$2"; shift 2 ;;
    --collect) MODE=collect; shift ;;
    --dry-run) MODE=dry-run; shift ;;
    --execute) MODE=execute; shift ;;
    --self-test) MODE=self-test; shift ;;
    --release-input) [[ $# -ge 2 ]] || usage; RELEASE_INPUTS+=("$2"); shift 2 ;;
    -h|--help) usage ;;
    *) echo "cleanup: unknown argument: $1" >&2; usage ;;
  esac
done

fail() { printf 'cleanup: %s\n' "$*" >&2; exit 1; }

private_root_abs() { (cd -- "$PRIVATE_ROOT" 2>/dev/null && pwd) || printf '%s\n' "$PRIVATE_ROOT"; }

# Refuse wildcard, outside-root, media, and release-input paths.
validate_path() {
  local p="$1" root="$2"
  local rp media
  [[ -n "$p" ]] || fail "empty-path"
  [[ "$p" != *'*'* && "$p" != *'?'* && "$p" != *'['* ]] || fail "wildcard-owned-path"
  [[ "$p" == /* ]] || fail "path-not-absolute"
  rp="$(realpath -m -- "$p")"
  root="$(realpath -m -- "$root")"
  [[ "$rp" == "$root" || "$rp" == "$root"/* ]] || fail "path-outside-private-root"
  media="$(realpath -m -- "$MEDIA_ROOT")"
  [[ "$rp" != "$media" && "$rp" != "$media"/* ]] || fail "media-path-refused"
  local rel
  for rel in "${RELEASE_INPUTS[@]}"; do
    rel="$(realpath -m -- "$rel")"
    if [[ "$rp" == "$rel" || "$rp" == "$rel"/* || "$rel" == "$rp"/* ]]; then
      fail "release-input-protected"
    fi
  done
}

validate_name() {
  local n="$1"
  [[ "$n" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || fail "invalid-name"
  [[ "$n" != *'*'* ]] || fail "wildcard-owned-path"
}

inventory() {
  local p="$1" depth="$2"
  if [[ ! -e "$p" ]]; then printf 'absent\n'; return; fi
  find "$p" -maxdepth "$depth" -printf '%P\t%s\t%T@\n' 2>/dev/null | LC_ALL=C sort | sha256sum | cut -d' ' -f1
}

collect_release_inputs() {
  local out=()
  local cand="$B6_ROOT/b7-candidate-2"
  if [[ -d "$cand" ]]; then
    while IFS= read -r p; do out+=("$p"); done < <(find "$cand" -maxdepth 1 -type d -name 'snapshot-*' -printf '%p/manifest.txt\n%p/snapshot.sha256\n')
    while IFS= read -r p; do out+=("$p"); done < <(find "$cand" -maxdepth 3 \( -path '*/work-*/receipt.json' -o -path '*/work-*/out1/teldrive' -o -path '*/work-*/out1/rclone' \) -print)
  fi
  while IFS= read -r p; do out+=("$p"); done < <(find "$PRIVATE_ROOT" -maxdepth 2 \( -path '*/vuln-*/summary.txt' -o -path '*/schema-compat-*/receipt.json' \) -print 2>/dev/null)
  printf '%s\n' "${out[@]}"
}

write_manifest() {
  local out="$1" run_id="$2" ns="$3"
  local containers_json networks_json mounts_json dirs_json audit_json release_json
  containers_json="$(docker ps -a --filter 'name=^b7-' --format '{{.Names}}' 2>/dev/null | jq -R . | jq -s .)"
  networks_json="$(docker network ls --filter 'name=^b7-' --format '{{.Name}}' 2>/dev/null | jq -R . | jq -s .)"
  mounts_json='[]'
  dirs_json="$(find "$PRIVATE_ROOT" -mindepth 1 -maxdepth 1 -type d -name 'b7-*' -print 2>/dev/null | jq -R . | jq -s .)"
  audit_json="$(find "$PRIVATE_ROOT" -mindepth 1 -maxdepth 1 -type d -name '__teldrive_audit_*' -print 2>/dev/null | jq -R . | jq -s .)"
  release_json="$(printf '%s\n' "${RELEASE_INPUTS[@]}" | jq -R . | jq -s .)"
  umask 077
  jq -n --arg run "$run_id" --arg ns "$ns" \
    --argjson containers "$containers_json" --argjson networks "$networks_json" \
    --argjson mounts "$mounts_json" --argjson dirs "$dirs_json" \
    --argjson audit "$audit_json" --argjson release "$release_json" \
    --arg media "$MEDIA_ROOT" \
    '{run_id:$run, namespace:$ns, containers:$containers, networks:$networks,
      mountpoints:$mounts, work_dirs:$dirs, audit_dirs:$audit, file_ids:[],
      release_inputs:$release, readonly_media_root:$media}' >"$out"
  chmod 0600 "$out"
}

load_manifest() {
  [[ -f "$MANIFEST" ]] || fail "manifest-missing"
  [[ ! -L "$MANIFEST" ]] || fail "manifest-symlink"
  local mode; mode="$(stat -c '%a' "$MANIFEST")"
  [[ "$mode" == "600" ]] || fail "manifest-permissions"
  jq -e . "$MANIFEST" >/dev/null || fail "manifest-invalid-json"
  mapfile -t RELEASE_INPUTS < <(jq -r '.release_inputs[]' "$MANIFEST")
  [[ ${#RELEASE_INPUTS[@]} -gt 0 ]] || fail "release-inputs-required"
}

validate_manifest() {
  local root; root="$(private_root_abs)"
  local p
  for p in $(jq -r '.containers[], .networks[]' "$MANIFEST"); do validate_name "$p"; done
  for p in $(jq -r '.mountpoints[], .work_dirs[], .audit_dirs[]' "$MANIFEST"); do validate_path "$p" "$root"; done
  local rel
  for rel in "${RELEASE_INPUTS[@]}"; do
    [[ "$rel" == /* ]] || fail "release-input-not-absolute"
  done
}

receipt() {
  local outcome="$1" out="$2"
  local remaining; remaining="$(jq -r '.mountpoints[], .work_dirs[], .audit_dirs[]' "$MANIFEST" | while read -r p; do [[ -e "$p" ]] && printf '%s\n' "$p"; done | jq -R . | jq -s .)"
  local present=true p
  for p in "${RELEASE_INPUTS[@]}"; do [[ -e "$p" ]] || present=false; done
  umask 077
  jq -n --arg outcome "$outcome" --argjson remaining "$remaining" --argjson present "$present" \
    --argjson cache_unchanged "$CACHE_UNCHANGED" --argjson media_unchanged "$MEDIA_UNCHANGED" \
    --argjson deleted_containers "$DELETED_CONTAINERS" --argjson deleted_networks "$DELETED_NETWORKS" \
    --argjson deleted_paths "$DELETED_PATHS" \
    '{outcome:$outcome, deleted:{containers:$deleted_containers, networks:$deleted_networks, paths:$deleted_paths},
      remaining:$remaining, release_inputs_present:$present,
      cache_inventory_unchanged:$cache_unchanged, media_unchanged:$media_unchanged}' >"$out/receipt.json"
  chmod 0600 "$out/receipt.json"
}

run_cleanup() {
  local execute="$1" out="$2"
  load_manifest
  validate_manifest
  mkdir -p "$out"; chmod 700 "$out"
  local before_cache before_media
  before_cache="$(inventory "$VFS_CACHE" 2)"
  before_media="$(inventory "$MEDIA_ROOT" 1)"
  DELETED_CONTAINERS=0; DELETED_NETWORKS=0; DELETED_PATHS=0
  if [[ "$execute" == true ]]; then
    local n
    while IFS= read -r n; do
      [[ -n "$n" ]] || continue
      docker rm -f "$n" >/dev/null 2>&1 || fail "container-remove-failed"
      DELETED_CONTAINERS=$((DELETED_CONTAINERS + 1))
    done < <(jq -r '.containers[]' "$MANIFEST")
    while IFS= read -r n; do
      [[ -n "$n" ]] || continue
      docker network rm "$n" >/dev/null 2>&1 || fail "network-remove-failed"
      DELETED_NETWORKS=$((DELETED_NETWORKS + 1))
    done < <(jq -r '.networks[]' "$MANIFEST")
    local p
    while IFS= read -r p; do
      [[ -n "$p" ]] || continue
      validate_path "$p" "$(private_root_abs)"
      rm -rf -- "$p" || fail "path-remove-failed"
      DELETED_PATHS=$((DELETED_PATHS + 1))
    done < <(jq -r '.mountpoints[], .work_dirs[], .audit_dirs[]' "$MANIFEST")
    [[ -z "$(docker ps -a --filter 'name=^b7-' --format '{{.Names}}')" ]] || fail "b7-container-remains"
    [[ -z "$(docker network ls --filter 'name=^b7-' --format '{{.Name}}')" ]] || fail "b7-network-remains"
  else
    DELETED_CONTAINERS=0; DELETED_NETWORKS=0; DELETED_PATHS=0
  fi
  local after_cache after_media
  after_cache="$(inventory "$VFS_CACHE" 2)"
  after_media="$(inventory "$MEDIA_ROOT" 1)"
  CACHE_UNCHANGED=false; [[ "$before_cache" == "$after_cache" ]] && CACHE_UNCHANGED=true
  MEDIA_UNCHANGED=false; [[ "$before_media" == "$after_media" ]] && MEDIA_UNCHANGED=true
  [[ "$CACHE_UNCHANGED" == true ]] || fail "cache-changed"
  [[ "$MEDIA_UNCHANGED" == true ]] || fail "media-changed"
  local outcome=PASS
  if [[ "$execute" != true ]]; then outcome=DRY-RUN; fi
  receipt "$outcome" "$out"
  printf 'cleanup: %s containers=%s networks=%s paths=%s release_inputs_present=%s cache_unchanged=%s media_unchanged=%s\n' \
    "$outcome" "$DELETED_CONTAINERS" "$DELETED_NETWORKS" "$DELETED_PATHS" "$(jq -r .release_inputs_present "$out/receipt.json")" "$CACHE_UNCHANGED" "$MEDIA_UNCHANGED"
}

self_test() {
  local tmp; tmp="$(mktemp -d "${TMPDIR:-/tmp}/b7-cleanup-self.XXXXXX")"
  trap 'rm -rf -- "$tmp"' RETURN
  local root="$tmp/root"; mkdir -p "$root"; chmod 700 "$root"
  local rel="$tmp/release/receipt.json"; mkdir -p "$tmp/release"; : >"$rel"; chmod 600 "$rel"
  local ok=0
  expect_fail() {
    local label="$1"; shift
    if "$@" >/dev/null 2>&1; then echo "self-test: FAIL expected $label" >&2; ok=1; else echo "self-test: PASS $label"; fi
  }
  mkmanifest() {
    local path="$1" workdir="$2" release="$3"
    jq -n --arg p "$workdir" --arg rel "$release" \
      '{run_id:"x",namespace:"n",containers:[],networks:[],mountpoints:[],work_dirs:[$p],audit_dirs:[],file_ids:[],release_inputs:[$rel],readonly_media_root:"media"}' >"$path"
    chmod 600 "$path"
  }
  local m
  m="$tmp/m-wildcard.json"; mkmanifest "$m" "$root/*" "$rel"
  expect_fail wildcard env B7_PRIVATE_DIR="$root" B6_PRIVATE_DIR="$tmp/b6" B7_MEDIA_ROOT="$tmp/media" "$SCRIPT_DIR/cleanup-check.sh" --manifest "$m" --dry-run --out "$tmp/o1"
  m="$tmp/m-outside.json"; mkmanifest "$m" "/tmp/not-here" "$rel"
  expect_fail outside env B7_PRIVATE_DIR="$root" B6_PRIVATE_DIR="$tmp/b6" B7_MEDIA_ROOT="$tmp/media" "$SCRIPT_DIR/cleanup-check.sh" --manifest "$m" --dry-run --out "$tmp/o2"
  m="$tmp/m-media.json"; mkmanifest "$m" "$tmp/media" "$rel"
  expect_fail media env B7_PRIVATE_DIR="$root" B6_PRIVATE_DIR="$tmp/b6" B7_MEDIA_ROOT="$tmp/media" "$SCRIPT_DIR/cleanup-check.sh" --manifest "$m" --dry-run --out "$tmp/o3"
  m="$tmp/m-release.json"; mkmanifest "$m" "$tmp/release" "$rel"
  expect_fail release_descendant env B7_PRIVATE_DIR="$root" B6_PRIVATE_DIR="$tmp/b6" B7_MEDIA_ROOT="$tmp/media" "$SCRIPT_DIR/cleanup-check.sh" --manifest "$m" --dry-run --out "$tmp/o4"
  m="$tmp/m-empty.json"; jq -n --arg p "$root/owned" '{run_id:"x",namespace:"n",containers:[],networks:[],mountpoints:[],work_dirs:[$p],audit_dirs:[],file_ids:[],release_inputs:[],readonly_media_root:"media"}' >"$m"; chmod 600 "$m"
  expect_fail empty_release env B7_PRIVATE_DIR="$root" B6_PRIVATE_DIR="$tmp/b6" B7_MEDIA_ROOT="$tmp/media" "$SCRIPT_DIR/cleanup-check.sh" --manifest "$m" --dry-run --out "$tmp/o5"
  # dry-run must not delete
  local owned="$root/owned"; mkdir -p "$owned"; chmod 700 "$owned"; : >"$owned/keep"
  m="$tmp/m-owned.json"; mkmanifest "$m" "$owned" "$rel"
  B7_PRIVATE_DIR="$root" B6_PRIVATE_DIR="$tmp/b6" B7_MEDIA_ROOT="$tmp/media" B7_VFS_CACHE="$tmp/cache" \
    "$SCRIPT_DIR/cleanup-check.sh" --manifest "$m" --dry-run --out "$tmp/o6" >/dev/null
  if [[ -e "$owned/keep" ]]; then echo "self-test: PASS dry-run-nondestructive"; else echo "self-test: FAIL dry-run deleted" >&2; ok=1; fi
  [[ "$ok" -eq 0 ]] || { echo "self-test: FAIL" >&2; return 1; }
  echo "self-test: PASS"
}

case "$MODE" in
  self-test) self_test ;;
  collect)
    [[ ${#RELEASE_INPUTS[@]} -gt 0 ]] || mapfile -t RELEASE_INPUTS < <(collect_release_inputs)
    [[ ${#RELEASE_INPUTS[@]} -gt 0 ]] || fail "release-inputs-required"
    OUT="${OUT:-$PRIVATE_ROOT/cleanup-manifest-$(date -u +%Y%m%dT%H%M%SZ)}"
    mkdir -p "$OUT"; chmod 700 "$OUT"
    MANIFEST="$OUT/manifest.json"
    write_manifest "$MANIFEST" "$(date -u +%Y%m%dT%H%M%SZ)-$$" "__teldrive_audit_b7_$(date -u +%Y%m%d%H%M%S)_"
    printf 'cleanup: manifest %s\n' "$MANIFEST"
    ;;
  dry-run|execute)
    [[ -n "$MANIFEST" ]] || usage
    OUT="${OUT:-$PRIVATE_ROOT/cleanup-$(date -u +%Y%m%dT%H%M%SZ)}"
    if [[ "$MODE" == execute ]]; then run_cleanup true "$OUT"; else run_cleanup false "$OUT"; fi
    ;;
  *) usage ;;
esac
