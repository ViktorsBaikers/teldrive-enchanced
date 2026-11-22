#!/usr/bin/env bash
# Seal both module repositories into a private, verifiable source snapshot.
# Read-only with respect to Git: no ref, index, or working-tree mutation.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"

OUT="${B6_PRIVATE_DIR:-$HOME/.local/state/teldrive-b6}"
TELDRIVE="$PROJECT_DIR/teldrive-enchanced"
RCLONE="$PROJECT_DIR/rclone-enchanced"
DOCKERFILE=""
DIRTY_FLAG=""

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
    -h|--help) usage ;;
    *) usage ;;
  esac
done

TELDRIVE="$(cd -- "$TELDRIVE" && pwd)"
RCLONE="$(cd -- "$RCLONE" && pwd)"
DOCKERFILE="${DOCKERFILE:-$TELDRIVE/Dockerfile}"

fail() { printf 'seal-snapshot: %s\n' "$*" >&2; exit 1; }
log() { printf 'seal-snapshot: %s\n' "$*" >&2; }

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

install -d -m 0700 "$OUT"
SNAP="$OUT/snapshot-$(date -u +%Y%m%dT%H%M%SZ)"
[[ ! -e "$SNAP" ]] || fail "snapshot already exists: $(basename "$SNAP")"
mkdir -m 0700 "$SNAP"

# Resolve base-image names referenced by the Dockerfile; only passed/known digests are recorded.
if [[ -f "$DOCKERFILE" ]]; then
  while IFS= read -r ref; do
    [[ -n "$ref" ]] || continue
    name=""
    case "$ref" in
      *oven/bun*) name=bun ;;
      golang*) name=golang ;;
      *distroless*) name=distroless ;;
      alpine*) name=alpine ;;
      *postgres*) name=postgres ;;
      *) name="$(printf '%s' "${ref%%@*}" | tr '/:' '--')" ;;
    esac
    [[ -n "${BASE_IMAGES[$name]:-}" ]] || BASE_IMAGES[$name]="unresolved:$ref"
  done < <(awk 'toupper($1)=="FROM" {print $2}' "$DOCKERFILE")
fi

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
    */baseline/*|*/vfs/vfs/*|*/vfs/vfsMeta/*) return 0 ;;
    */.cache/*|*/.config/*|*/session/*|*/sessions/*|*/cache/*) return 0 ;;
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
