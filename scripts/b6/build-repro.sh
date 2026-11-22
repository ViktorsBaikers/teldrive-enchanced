#!/usr/bin/env bash
# Build each artifact twice from a sealed snapshot with isolated caches, then
# compare byte digests. Gated builds never download modules or tools.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

SNAPSHOT=""
NATIVE="both"
GO_REQUIRED="go1.27.4"
VERSION_STRING="v0.1.0-enchanced"
RCLONE_UPSTREAM_BASE="v1.76.0"
BUN_IMAGE="oven/bun:1.4.2-alpine@sha256:d888c0ae6c86d7866ff10c5aafdd9077b36aee6455b33dd270fb93c0dd5cef6f"

usage() {
  cat >&2 <<'EOF'
usage: build-repro.sh --snapshot DIR [--native teldrive|rclone|both]
Builds each selected artifact twice from a sealed snapshot and compares digests.
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot) [[ $# -ge 2 ]] || usage; SNAPSHOT="$2"; shift 2 ;;
    --native) [[ $# -ge 2 ]] || usage; NATIVE="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

[[ -n "$SNAPSHOT" ]] || usage
case "$NATIVE" in teldrive|rclone|both) ;; *) usage ;; esac
[[ -d "$SNAPSHOT" ]] || { echo "build-repro: snapshot not found" >&2; exit 1; }
SNAPSHOT="$(cd -- "$SNAPSHOT" && pwd)"
PRIVATE_ROOT="$(dirname -- "$SNAPSHOT")"
SNAP_NAME="$(basename -- "$SNAPSHOT")"

fail() { printf 'build-repro: %s\n' "$*" >&2; exit 1; }
log() { printf 'build-repro: %s\n' "$*" >&2; }

export LC_ALL=C
export GOTOOLCHAIN=local

# --- Snapshot and toolchain preconditions -----------------------------------
[[ -f "$SNAPSHOT/snapshot.sha256" && -f "$SNAPSHOT/manifest.txt" ]] ||
  fail "snapshot manifest missing; reseal with seal-snapshot.sh"
SNAP_DIGEST="$(cat "$SNAPSHOT/snapshot.sha256")"
[[ "$(sha256sum "$SNAPSHOT/manifest.txt" | cut -d' ' -f1)" == "$SNAP_DIGEST" ]] ||
  fail "snapshot digest mismatch; snapshot was modified after sealing"

go_version="$(go version)"
printf '%s\n' "$go_version" | grep -Eq "(^| )$GO_REQUIRED( |$)" ||
  fail "Go toolchain must be exactly $GO_REQUIRED (found: $go_version)"
GO_VERSION="$GO_REQUIRED"

if [[ "$NATIVE" != rclone ]]; then
  docker image inspect "$BUN_IMAGE" >/dev/null 2>&1 ||
    fail "pinned Bun image is absent locally; pre-pull it before the gated build"
fi

STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
WORK="$PRIVATE_ROOT/work-$SNAP_NAME"
[[ "$(basename -- "$WORK")" == work-* && "$WORK" != "$SNAPSHOT" ]] ||
  fail "refusing unsafe work directory"
umask 077
remove_tree() { [[ -e "$1" ]] || return 0; chmod -R u+w "$1" 2>/dev/null || true; rm -rf -- "$1"; }
remove_tree "$WORK"
mkdir -m 700 "$WORK"
mkdir -m 700 "$WORK/extract" "$WORK/out1" "$WORK/out2"

cleanup() { :; }
trap cleanup EXIT INT TERM

# Extract the sealed source once; each build copies this tree and removes .git.
extract_repo() {
  local repo="$1" src="$WORK/extract/$1"
  rm -rf -- "$src"
  git clone "$SNAPSHOT/$repo/repo.bundle" "$src" >/dev/null 2>&1 ||
    fail "cannot extract $repo bundle"
  [[ "$(git -C "$src" rev-parse HEAD)" == "$(cat "$SNAPSHOT/$repo/HEAD")" ]] ||
    fail "extracted $repo HEAD does not match the seal"
  if [[ -s "$SNAPSHOT/$repo/tracked.patch" ]]; then
    git -C "$src" apply --binary "$SNAPSHOT/$repo/tracked.patch" ||
      fail "cannot apply sealed $repo patch"
  fi
  tar -xzf "$SNAPSHOT/$repo/untracked.tar.gz" -C "$src"
}

for repo in teldrive-enchanced rclone-enchanced; do
  [[ -s "$SNAPSHOT/$repo/repo.bundle" ]] || fail "empty bundle for $repo"
  if ! git bundle verify "$SNAPSHOT/$repo/repo.bundle" >/dev/null 2>&1; then
    fail "bundle verify failed for $repo"
  fi
done

log "extracting snapshot"
extract_repo teldrive-enchanced
extract_repo rclone-enchanced

dirty_for() {
  awk -v key="repo:$1 " 'index($0, key) == 1 { for (i = 1; i <= NF; i++) if ($i ~ /^dirty=/) print substr($i, 7) }' \
    "$SNAPSHOT/manifest.txt" | head -n 1
}
head_for() { cat "$SNAPSHOT/$1/HEAD"; }

# --- Warm caches outside the gated builds -----------------------------------
# Module and Bun caches may be populated here (network allowed). The gated
# builds below run with GOPROXY=off / --network none and fail if content is missing.
MODSEED="$PRIVATE_ROOT/modseed"
BUNCACHE="$PRIVATE_ROOT/bun-cache"
mkdir -m 700 -p "$MODSEED" "$BUNCACHE"

if [[ "$NATIVE" != rclone ]]; then
  pre="$WORK/ui-prewarm"
  mkdir -p "$pre/ui"
  cp "$WORK/extract/teldrive-enchanced/ui/package.json" "$pre/ui/package.json"
  cp "$WORK/extract/teldrive-enchanced/ui/bun.lock" "$pre/ui/bun.lock"
  chmod -R u+rwX "$pre"
  mkdir -m 700 -p "$WORK/home-prewarm"
  log "prewarming Bun cache"
  docker run --rm --pull=never --user "$(id -u):$(id -g)" \
    -e HOME=/tmp-home -e BUN_INSTALL_CACHE_DIR=/cache \
    -v "$pre:/src" -v "$WORK/home-prewarm:/tmp-home" -v "$BUNCACHE:/cache" \
    -w /src "$BUN_IMAGE" sh -eu -c 'bun install --frozen-lockfile --cwd ui' >/dev/null
fi

log "prewarming Go module seed"
for repo in teldrive-enchanced rclone-enchanced; do
  ( cd "$WORK/extract/$repo" &&
    GOMODCACHE="$MODSEED" GOFLAGS=-mod=readonly go mod download )
done

seed_run_caches() {
  local run="$1" repo
  remove_tree "$WORK/cache$run"
  remove_tree "$WORK/mod$run"
  mkdir -m 700 "$WORK/cache$run" "$WORK/mod$run"
  # shortcut: hardlinks share module files with the seed; builds only read them.
  cp -al "$MODSEED/." "$WORK/mod$run/"
  cp -a "$WORK/extract" "$WORK/build$run"
  for repo in teldrive-enchanced rclone-enchanced; do
    rm -rf -- "$WORK/build$run/$repo/.git"
  done
}

# Build the UI from sealed source only. Reused by later native and --ui modes.
build_ui_tree() {
  local root="$1" run="$2" home="$WORK/home$run"
  local ui="$root/ui"
  rm -rf -- "$ui/node_modules" "$ui/dist" "$ui/src/routeTree.gen.ts"
  mkdir -m 700 -p "$home"
  docker run --rm --pull=never --network none --user "$(id -u):$(id -g)" \
    -e HOME=/tmp-home -e BUN_INSTALL_CACHE_DIR=/cache \
    -v "$root:/src" -v "$home:/tmp-home" -v "$BUNCACHE:/cache" \
    -w /src "$BUN_IMAGE" sh -eu -c \
    "bun install --frozen-lockfile --cwd ui && VERSION=$VERSION_STRING bun run --cwd ui build" >/dev/null
  [[ -d "$ui/dist" ]] || fail "UI build produced no ui/dist in run $run"
}

ui_manifest() {
  local root="$1" out="$2"
  ( cd "$root/ui/dist" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum ) > "$out"
}

build_go() {
  local repo="$1" run="$2" root="$WORK/build$run/$repo" out="$3" pkg="$4" ldflags="$5"
  (
    cd "$root"
    GOCACHE="$WORK/cache$run" GOMODCACHE="$WORK/mod$run" \
      GOFLAGS=-mod=readonly GOPROXY=off GOSUMDB=off CGO_ENABLED=0 GOTOOLCHAIN=local \
      go mod download
    GOCACHE="$WORK/cache$run" GOMODCACHE="$WORK/mod$run" \
      GOFLAGS=-mod=readonly GOPROXY=off GOSUMDB=off CGO_ENABLED=0 GOTOOLCHAIN=local \
      go build -trimpath -buildvcs=false -ldflags "$ldflags" -o "$out" "$pkg"
  ) || fail "$repo build failed (module cache must already contain every dependency; no download is permitted)"
}

json_commands() { jq -n --args '$ARGS.positional' -- "$@"; }

declare -A ARTIFACT_JSON=()
OVERALL="PASS"

build_teldrive() {
  local repo=teldrive-enchanced
  local head short cdate ldflags run root ui1 ui2 ui_eq bin_eq a1 a2
  head="$(head_for "$repo")"
  short="$(git -C "$WORK/extract/$repo" rev-parse --short=12 HEAD)"
  cdate="$(git -C "$WORK/extract/$repo" log -1 --format=%cI)"
  ldflags="-s -w -X main.version=$VERSION_STRING -X main.commit=$short -X main.date=$cdate"

  for run in 1 2; do
    seed_run_caches "$run"
    root="$WORK/build$run/$repo"
    build_ui_tree "$root" "$run"
    ui_manifest "$root" "$WORK/ui-manifest$run.txt"
    out="$WORK/out$run/teldrive"
    build_go "$repo" "$run" "$out" ./cmd/teldrive "$ldflags"
  done

  a1="$(sha256sum "$WORK/out1/teldrive" | cut -d' ' -f1)"
  a2="$(sha256sum "$WORK/out2/teldrive" | cut -d' ' -f1)"
  [[ "$("$WORK/out1/teldrive" version | head -n 1)" == "teldrive $VERSION_STRING" ]] ||
    fail "teldrive binary does not report $VERSION_STRING"
  bin_eq=false; cmp -s "$WORK/out1/teldrive" "$WORK/out2/teldrive" && bin_eq=true
  ui1="$(sha256sum "$WORK/ui-manifest1.txt" | cut -d' ' -f1)"
  ui2="$(sha256sum "$WORK/ui-manifest2.txt" | cut -d' ' -f1)"
  ui_eq=false; cmp -s "$WORK/ui-manifest1.txt" "$WORK/ui-manifest2.txt" && ui_eq=true
  [[ "$bin_eq" == true ]] || { printf 'build-repro: teldrive artifact mismatch\n' >&2; OVERALL=FAIL; }
  [[ "$ui_eq" == true ]] || { printf 'build-repro: teldrive UI manifest mismatch\n' >&2; OVERALL=FAIL; }

  ARTIFACT_JSON[$repo]="$(jq -n \
    --arg repo "$repo" --arg head "$head" --arg dirty "$(dirty_for "$repo")" \
    --arg go "$GO_VERSION" --arg a1 "$a1" --arg a2 "$a2" --argjson bin_eq "$bin_eq" \
    --arg ui1 "$ui1" --arg ui2 "$ui2" --argjson ui_eq "$ui_eq" \
    --argjson commands "$(json_commands \
      "docker run --network none --pull=never $BUN_IMAGE bun install --frozen-lockfile --cwd ui" \
      "VERSION=$VERSION_STRING bun run --cwd ui build" \
      "GOTOOLCHAIN=local GOFLAGS=-mod=readonly GOPROXY=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags '$ldflags' -o out/teldrive ./cmd/teldrive")" \
    --arg outcome "$([[ "$bin_eq" == true && "$ui_eq" == true ]] && echo PASS || echo FAIL)" \
    '{repo:$repo, head:$head, dirty:($dirty=="true"), go_version:$go,
      artifact_sha256_run1:$a1, artifact_sha256_run2:$a2, binary_equal:$bin_eq,
      ui_manifest_sha256_run1:$ui1, ui_manifest_sha256_run2:$ui2, ui_manifest_equal:$ui_eq,
      commands:$commands, outcome:$outcome}')"
}

build_rclone() {
  local repo=rclone-enchanced
  local head short cdate ldflags run out a1 a2 bin_eq
  head="$(head_for "$repo")"
  short="$(git -C "$WORK/extract/$repo" rev-parse --short=12 HEAD)"
  cdate="$(git -C "$WORK/extract/$repo" log -1 --format=%cI)"
  ldflags="-s -w -X github.com/rclone/rclone/fs.Version=$VERSION_STRING"

  for run in 1 2; do
    seed_run_caches "$run"
    out="$WORK/out$run/rclone"
    build_go "$repo" "$run" "$out" . "$ldflags"
  done

  a1="$(sha256sum "$WORK/out1/rclone" | cut -d' ' -f1)"
  a2="$(sha256sum "$WORK/out2/rclone" | cut -d' ' -f1)"
  [[ "$("$WORK/out1/rclone" version | head -n 1)" == "rclone $VERSION_STRING" ]] ||
    fail "rclone binary does not report $VERSION_STRING"
  bin_eq=false; cmp -s "$WORK/out1/rclone" "$WORK/out2/rclone" && bin_eq=true
  [[ "$bin_eq" == true ]] || { printf 'build-repro: rclone artifact mismatch\n' >&2; OVERALL=FAIL; }

  ARTIFACT_JSON[$repo]="$(jq -n \
    --arg repo "$repo" --arg head "$head" --arg dirty "$(dirty_for "$repo")" \
    --arg go "$GO_VERSION" --arg a1 "$a1" --arg a2 "$a2" --argjson bin_eq "$bin_eq" \
    --arg upstream "$RCLONE_UPSTREAM_BASE" \
    --argjson commands "$(json_commands \
      "GOTOOLCHAIN=local GOFLAGS=-mod=readonly GOPROXY=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags '-X github.com/rclone/rclone/fs.Version=$VERSION_STRING' -o out/rclone .")" \
    --arg outcome "$([[ "$bin_eq" == true ]] && echo PASS || echo FAIL)" \
    '{repo:$repo, head:$head, dirty:($dirty=="true"), go_version:$go,
      artifact_sha256_run1:$a1, artifact_sha256_run2:$a2, binary_equal:$bin_eq,
      upstream_base:$upstream, commands:$commands, outcome:$outcome}')"
}

[[ "$NATIVE" == rclone ]] || build_teldrive
[[ "$NATIVE" == teldrive ]] || build_rclone

artifacts="{}"
for artifact in "${!ARTIFACT_JSON[@]}"; do
  artifacts="$(jq -c --arg k "$artifact" --argjson v "${ARTIFACT_JSON[$artifact]}" '. + {($k): $v}' <<<"$artifacts")"
done

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RECEIPT="$WORK/receipt.json"
jq -n --arg snap "$SNAP_DIGEST" --arg go "$GO_VERSION" --argjson artifacts "$artifacts" \
  --arg started "$STARTED_AT" --arg finished "$FINISHED_AT" --arg outcome "$OVERALL" \
  '{snapshot_sha256:$snap, go_version:$go, started_at:$started, finished_at:$finished,
    outcome:$outcome, artifacts:$artifacts}' > "$RECEIPT"
chmod 600 "$RECEIPT"

printf 'BUILD %s snapshot_sha256=%s receipt=%s/work-%s/receipt.json\n' \
  "$OVERALL" "$SNAP_DIGEST" "$(basename -- "$PRIVATE_ROOT")" "$SNAP_NAME"

[[ "$OVERALL" == PASS ]] || exit 1
