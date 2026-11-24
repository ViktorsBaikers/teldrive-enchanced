#!/usr/bin/env bash
# Seal both module repositories into a private, verifiable source snapshot.
# Read-only with respect to Git: no ref, index, or working-tree mutation.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
SELF="$SCRIPT_DIR/$(basename -- "${BASH_SOURCE[0]}")"

OUT="${B6_PRIVATE_DIR:-$HOME/.local/state/teldrive-b6}"
TELDRIVE="$PROJECT_DIR/teldrive-enchanced"
RCLONE="$PROJECT_DIR/rclone-enchanced"
DOCKERFILE=""
DIRTY_FLAG=""
SELF_TEST=false

declare -A TOOLS=(
  [go]="1.27.4"
  [bun]="1.4.2"
  [ogen]="v1.24.0"
  [sqlc]="v1.30.0"
)
declare -A BASE_IMAGES=(
  [golang]="sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280"
  [bun]="sha256:d888c0ae6c86d7866ff10c5aafdd9077b36aee6455b33dd270fb93c0dd5cef6f"
  [distroless]="sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab"
)

usage() {
  cat >&2 <<'EOF'
usage: seal-snapshot.sh [options]
  --out DIR                        private output root (default $B6_PRIVATE_DIR or ~/.local/state/teldrive-b6)
  --teldrive PATH                  teldrive-enchanced repository (default: sibling of this script)
  --rclone PATH                    rclone-enchanced repository (default: sibling of teldrive-enchanced)
  --tool NAME=VERSION              repeatable; go, bun, ogen, sqlc (defaults: 1.27.4, 1.4.2, v1.24.0, v1.30.0)
  --base-image NAME=DIGEST         repeatable; golang, bun, distroless
  --base-image-from-dockerfile P   parse FROM references for base-image names (default: <teldrive>/Dockerfile)
  --dirty true|false               fail unless the measured dirty state matches
  --self-test                      prove the exclusion and base-image gates on temp fixtures
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) [[ $# -ge 2 ]] || usage; OUT="$2"; shift 2 ;;
    --teldrive) [[ $# -ge 2 ]] || usage; TELDRIVE="$2"; shift 2 ;;
    --rclone) [[ $# -ge 2 ]] || usage; RCLONE="$2"; shift 2 ;;
    --tool)
      [[ $# -ge 2 && "$2" == *=* ]] || usage
      TOOLS["${2%%=*}"]="${2#*=}"; shift 2 ;;
    --base-image)
      [[ $# -ge 2 && "$2" == *=* ]] || usage
      BASE_IMAGES["${2%%=*}"]="${2#*=}"; shift 2 ;;
    --base-image-from-dockerfile) [[ $# -ge 2 ]] || usage; DOCKERFILE="$2"; shift 2 ;;
    --dirty)
      [[ $# -ge 2 && ( "$2" == true || "$2" == false ) ]] || usage
      DIRTY_FLAG="$2"; shift 2 ;;
    --self-test) SELF_TEST=true; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

TELDRIVE="$(cd -- "$TELDRIVE" && pwd)"
RCLONE="$(cd -- "$RCLONE" && pwd)"
DOCKERFILE="${DOCKERFILE:-$TELDRIVE/Dockerfile}"

fail() { printf 'seal-snapshot: %s\n' "$*" >&2; exit 1; }
log() { printf 'seal-snapshot: %s\n' "$*" >&2; }

self_test() {
  local tmp failures=0
  tmp="$(mktemp -d)"
  trap "rm -rf -- '$tmp'" EXIT INT TERM

  local golang_digest="${BASE_IMAGES[golang]}"
  local bun_digest="${BASE_IMAGES[bun]}"
  local distroless_digest="${BASE_IMAGES[distroless]}"

  mkdir -p "$tmp/teldrive/internal/cache" "$tmp/teldrive/scripts/baseline" "$tmp/rclone"
  printf 'fixture\n' > "$tmp/teldrive/README.md"
  printf 'fixture\n' > "$tmp/rclone/README.md"
  printf 'package cache\n' > "$tmp/teldrive/internal/cache/probe.go"
  printf '#!/bin/sh\n' > "$tmp/teldrive/scripts/baseline/probe.sh"
  cat > "$tmp/teldrive/Dockerfile" <<EOF
FROM golang:1.27.4-alpine@$golang_digest AS build
FROM oven/bun:1.4.2-alpine@$bun_digest AS ui
FROM gcr.io/distroless/static-debian12:nonroot@$distroless_digest
EOF
  printf 'FROM golang:1.27.4-alpine@sha256:%064d\n' 0 > "$tmp/Dockerfile.mismatch"
  printf 'FROM golang:1.27.4-alpine\n' > "$tmp/Dockerfile.missing"
  printf 'FROM example.invalid/unknown@%s\n' "$golang_digest" > "$tmp/Dockerfile.unresolved"

  for repo in teldrive rclone; do
    git -C "$tmp/$repo" init -q
    git -C "$tmp/$repo" add README.md
    git -C "$tmp/$repo" -c user.name=fixture -c user.email=fixture@example.invalid \
      commit -q -m init
  done
  printf 'scripts/baseline/\n' >> "$tmp/teldrive/.git/info/exclude"

  run_seal() {
    local out="$1"; shift
    "$SELF" --out "$out" --teldrive "$tmp/teldrive" --rclone "$tmp/rclone" "$@"
  }

  if ! run_seal "$tmp/out-ok" >"$tmp/ok.log" 2>&1; then
    printf 'self-test: FAIL fixture seal exited non-zero\n' >&2
    failures=$((failures + 1))
  else
    local snap
    snap="$(find "$tmp/out-ok" -maxdepth 1 -type d -name 'snapshot-*' -print -quit)"
    if ! cut -f1 "$snap/teldrive-enchanced/untracked.txt" | grep -qxF 'internal/cache/probe.go'; then
      printf 'self-test: FAIL untracked source under a cache path was hidden\n' >&2
      failures=$((failures + 1))
    elif ! tar -tzf "$snap/teldrive-enchanced/untracked.tar.gz" | grep -qxF 'internal/cache/probe.go'; then
      printf 'self-test: FAIL cache-path source is missing from the archive\n' >&2
      failures=$((failures + 1))
    elif ! grep -qxF 'private-harness-omitted:scripts/baseline files=1' "$snap/manifest.txt"; then
      printf 'self-test: FAIL private harness omission is not recorded\n' >&2
      failures=$((failures + 1))
    else
      printf 'self-test: PASS untracked cache-path source archived\n'
    fi
  fi

  if run_seal "$tmp/out-override" --base-image "golang=$golang_digest" >"$tmp/override.log" 2>&1; then
    printf 'self-test: PASS matching base-image override accepted\n'
  else
    printf 'self-test: FAIL matching base-image override rejected\n' >&2
    failures=$((failures + 1))
  fi

  if [[ -f "$PROJECT_DIR/teldrive-enchanced/Dockerfile" ]] &&
     run_seal "$tmp/out-real" --base-image-from-dockerfile "$PROJECT_DIR/teldrive-enchanced/Dockerfile" \
       >"$tmp/real.log" 2>&1; then
    printf 'self-test: PASS real Dockerfile digests match\n'
  else
    printf 'self-test: FAIL real Dockerfile digests do not match\n' >&2
    failures=$((failures + 1))
  fi

  if run_seal "$tmp/out-mismatch" --base-image-from-dockerfile "$tmp/Dockerfile.mismatch" \
      >"$tmp/mismatch.log" 2>&1; then
    printf 'self-test: FAIL digest mismatch accepted\n' >&2
    failures=$((failures + 1))
  elif ! grep -q 'base-image-digest-mismatch' "$tmp/mismatch.log"; then
    printf 'self-test: FAIL digest mismatch rejected without its label\n' >&2
    failures=$((failures + 1))
  else
    printf 'self-test: PASS digest mismatch rejected\n'
  fi

  if run_seal "$tmp/out-missing" --base-image-from-dockerfile "$tmp/Dockerfile.missing" \
      >"$tmp/missing.log" 2>&1; then
    printf 'self-test: FAIL digest-less reference accepted\n' >&2
    failures=$((failures + 1))
  elif ! grep -q 'base-image-digest-missing' "$tmp/missing.log"; then
    printf 'self-test: FAIL digest-less reference rejected without its label\n' >&2
    failures=$((failures + 1))
  else
    printf 'self-test: PASS digest-less reference rejected\n'
  fi

  if run_seal "$tmp/out-unresolved" --base-image-from-dockerfile "$tmp/Dockerfile.unresolved" \
      >"$tmp/unresolved.log" 2>&1; then
    printf 'self-test: FAIL unresolved base image accepted\n' >&2
    failures=$((failures + 1))
  elif ! grep -q 'base-image-unresolved' "$tmp/unresolved.log"; then
    printf 'self-test: FAIL unresolved base image rejected without its label\n' >&2
    failures=$((failures + 1))
  else
    printf 'self-test: PASS unresolved base image rejected\n'
  fi

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

# Git must never refresh the index or take optional locks during a read-only seal.
export GIT_OPTIONAL_LOCKS=0
export LC_ALL=C

VERIFY_TMP=""
cleanup() {
  if [[ -n "$VERIFY_TMP" && -d "$VERIFY_TMP" ]]; then
    rm -rf -- "$VERIFY_TMP"
  fi
}
trap cleanup EXIT INT TERM

# Bind every Dockerfile FROM reference to a configured digest; fail closed.
if [[ -f "$DOCKERFILE" ]]; then
  while IFS= read -r ref; do
    [[ -n "$ref" ]] || continue
    case "$ref" in
      *@sha256:*) digest="${ref##*@}" ;;
      *) fail "base-image-digest-missing: $ref" ;;
    esac
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] ||
      fail "base-image-digest-mismatch: $ref (malformed digest)"
    case "$ref" in
      *oven/bun*) name=bun ;;
      golang*) name=golang ;;
      *distroless*) name=distroless ;;
      alpine*) name=alpine ;;
      *postgres*) name=postgres ;;
      *) name="" ;;
    esac
    if [[ -z "$name" || -z "${BASE_IMAGES[$name]:-}" ]]; then
      fail "base-image-unresolved: $ref"
    fi
    [[ "${BASE_IMAGES[$name]}" == "$digest" ]] ||
      fail "base-image-digest-mismatch: $ref configured=${BASE_IMAGES[$name]} actual=$digest"
  done < <(awk 'toupper($1)=="FROM" {print $2}' "$DOCKERFILE")
fi

install -d -m 0700 "$OUT"
SNAP="$OUT/snapshot-$(date -u +%Y%m%dT%H%M%SZ)"
[[ ! -e "$SNAP" ]] || fail "snapshot already exists: $(basename "$SNAP")"
mkdir -m 0700 "$SNAP"

REPOS=(teldrive-enchanced rclone-enchanced)
declare -A REPO_PATH=([teldrive-enchanced]="$TELDRIVE" [rclone-enchanced]="$RCLONE")

now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
manifest_lines=()
manifest_lines+=("sealed-at:$now_iso")

# Runtime/evidence and secret paths never enter the untracked archive.
is_excluded() {
  local p="$1" base="${1##*/}"
  case "$p" in
    node_modules|node_modules/*|*/node_modules/*) return 0 ;;
    ui/dist|ui/dist/*|*/ui/dist/*) return 0 ;;
    ui/src/routeTree.gen.ts|*/ui/src/routeTree.gen.ts) return 0 ;;
  esac
  case "/$p/" in
    */vfs/vfs/*|*/vfs/vfsMeta/*) return 0 ;;
    */.cache/*|*/.config/*|*/session/*|*/sessions/*) return 0 ;;
  esac
  case "$base" in
    .env|.env.*|rclone.conf|teldrive.conf) return 0 ;;
    config.toml|config.yaml|config.yml|*.session|*.key) return 0 ;;
  esac
  return 1
}

dirty_measured=false

for repo in "${REPOS[@]}"; do
  src="${REPO_PATH[$repo]}"
  [[ -d "$src/.git" ]] || fail "not a Git repository: $repo"
  out="$SNAP/$repo"
  mkdir -m 0700 "$out"

  head_sha="$(git -C "$src" rev-parse HEAD)"
  tree_sha="$(git -C "$src" rev-parse 'HEAD^{tree}')"

  git -C "$src" bundle create "$out/repo.bundle" HEAD >/dev/null
  git -C "$src" diff HEAD --binary --full-index > "$out/tracked.patch"
  git -C "$src" status --porcelain > "$out/status.porcelain"
  printf '%s\n' "$head_sha" > "$out/HEAD"
  printf '%s\n' "$tree_sha" > "$out/TREE"
  sha256sum "$out/tracked.patch" | cut -d' ' -f1 > "$out/diff.sha256"

  filtered="$(mktemp "$SNAP/.untracked-$repo.XXXXXX")"
  git -C "$src" ls-files -o --exclude-standard -z |
    while IFS= read -r -d '' p; do
      is_excluded "$p" || printf '%s\0' "$p"
    done > "$filtered"

  # tar paths are repository-relative; run from the repository for both listing and archive.
  ( cd "$src" && tar --null -T "$filtered" -czf "$out/untracked.tar.gz" \
      --mtime=@0 --owner=0 --group=0 --numeric-owner )
  rm -f -- "$filtered"

  # The untracked listing is the hash list verified against a fresh extraction.
  # shortcut: tab/newline in an untracked filename is unsupported; repository paths are plain.
  while IFS= read -r -d '' p; do
    h="$(sha256sum "$src/$p" | cut -d' ' -f1)"
    printf '%s\t%s\n' "$p" "$h"
  done < <(git -C "$src" ls-files -o --exclude-standard -z |
             while IFS= read -r -d '' p; do
               is_excluded "$p" || printf '%s\0' "$p"
             done) | LC_ALL=C sort > "$out/untracked.txt"
  untracked_count="$(wc -l < "$out/untracked.txt" | tr -d ' ')"

  # Fail closed if any archived member still matches a secret/config basename.
  while IFS= read -r member; do
    base="${member##*/}"
    case "$base" in
      .env|.env.*|rclone.conf|teldrive.conf|config.toml|config.yaml|config.yml|*.session|*.key)
        fail "secret-pattern path in untracked archive: $repo/$member" ;;
    esac
  done < <(tar -tzf "$out/untracked.tar.gz")

  if [[ -s "$out/tracked.patch" || "$untracked_count" -gt 0 ]]; then
    dirty_measured=true
  fi

  sha256sum "$out/repo.bundle" "$out/tracked.patch" "$out/untracked.tar.gz" > "$out/artifact.sha256"

  {
    printf 'repo:%s\n' "$repo"
    printf 'head:%s\n' "$head_sha"
    printf 'tree:%s\n' "$tree_sha"
    printf 'dirty:%s\n' "$([[ -s "$out/tracked.patch" || "$untracked_count" -gt 0 ]] && echo true || echo false)"
    printf 'diff-sha256:%s\n' "$(cat "$out/diff.sha256")"
    printf 'untracked-count:%s\n' "$untracked_count"
    printf 'secret-scan:clean\n'
    while IFS= read -r line; do printf 'artifact:%s\n' "$line"; done < "$out/artifact.sha256"
  } | LC_ALL=C sort > "$out/manifest.txt"
  manifest_lines+=("repo:$repo head=$head_sha tree=$tree_sha dirty=$([[ -s "$out/tracked.patch" || "$untracked_count" -gt 0 ]] && echo true || echo false) patch=$(cat "$out/diff.sha256") untracked=$untracked_count")
done

# The private measurement harness lives outside Git; record the omission, not a broad exclusion.
if [[ -d "$TELDRIVE/scripts/baseline" ]]; then
  harness_files="$(find "$TELDRIVE/scripts/baseline" -type f | wc -l | tr -d ' ')"
  manifest_lines+=("private-harness-omitted:scripts/baseline files=$harness_files")
fi

if [[ -n "$DIRTY_FLAG" && "$DIRTY_FLAG" != "$dirty_measured" ]]; then
  fail "measured dirty=$dirty_measured does not match --dirty $DIRTY_FLAG"
fi
dirty="$dirty_measured"

for name in "${!TOOLS[@]}"; do
  manifest_lines+=("tool:$name=${TOOLS[$name]}")
done
for name in "${!BASE_IMAGES[@]}"; do
  manifest_lines+=("base-image:$name=${BASE_IMAGES[$name]}")
done

top_manifest="$SNAP/manifest.txt"
while IFS= read -r line; do
  rel="${line#*  }"
  manifest_lines+=("sha256:${line%%  *} $rel")
done < <(cd "$SNAP" && find . -type f ! -name manifest.txt ! -name snapshot.sha256 -print0 |
           LC_ALL=C sort -z | xargs -0 sha256sum)
printf '%s\n' "${manifest_lines[@]}" | LC_ALL=C sort > "$top_manifest"
sha256sum "$top_manifest" | cut -d' ' -f1 > "$SNAP/snapshot.sha256"

# Verify the seal with fresh extraction outside the snapshot directory.
VERIFY_TMP="$(mktemp -d "$OUT/.verify.XXXXXX")"
for repo in "${REPOS[@]}"; do
  out="$SNAP/$repo"
  clone="$VERIFY_TMP/$repo"
  git clone "$out/repo.bundle" "$clone" >/dev/null 2>&1 ||
    fail "snapshot verification failed: cannot clone $repo bundle"
  [[ "$(git -C "$clone" rev-parse HEAD)" == "$(cat "$out/HEAD")" ]] ||
    fail "snapshot verification failed: $repo HEAD mismatch"
  [[ "$(git -C "$clone" rev-parse 'HEAD^{tree}')" == "$(cat "$out/TREE")" ]] ||
    fail "snapshot verification failed: $repo tree mismatch"
  if [[ -s "$out/tracked.patch" ]]; then
    git -C "$clone" apply --binary "$out/tracked.patch" ||
      fail "snapshot verification failed: $repo patch does not apply"
  fi
  [[ "$(git -C "$clone" diff HEAD --binary --full-index | sha256sum | cut -d' ' -f1)" == "$(cat "$out/diff.sha256")" ]] ||
    fail "snapshot verification failed: $repo diff mismatch"
  tar -xzf "$out/untracked.tar.gz" -C "$clone"
  # Recompute the extracted untracked set and compare it with the sealed list.
  got="$(mktemp "$VERIFY_TMP/.got-$repo.XXXXXX")"
  while IFS= read -r -d '' p; do
    h="$(sha256sum "$clone/$p" | cut -d' ' -f1)"
    printf '%s\t%s\n' "$p" "$h"
  done < <(git -C "$clone" ls-files -o --exclude-standard -z) | LC_ALL=C sort > "$got"
  if ! diff -u "$out/untracked.txt" "$got" >/dev/null; then
    rm -f -- "$got"
    fail "snapshot verification failed: $repo untracked set mismatch"
  fi
  rm -f -- "$got"
done

find "$SNAP" -type f -exec chmod 600 {} +
find "$SNAP" -type d -exec chmod 700 {} +

snapshot_digest="$(cat "$SNAP/snapshot.sha256")"
log "verified snapshot $(basename "$SNAP")"
printf 'SNAPSHOT ok snapshot=%s sha256=%s teldrive_head=%s rclone_head=%s dirty=%s\n' \
  "$(basename "$SNAP")" "$snapshot_digest" \
  "$(cat "$SNAP/teldrive-enchanced/HEAD")" "$(cat "$SNAP/rclone-enchanced/HEAD")" "$dirty"
