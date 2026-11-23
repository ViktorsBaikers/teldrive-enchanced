#!/usr/bin/env bash
# Build each artifact twice from a sealed snapshot with isolated caches, then
# compare byte digests. Gated builds never download modules or tools.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

SNAPSHOT=""
NATIVE="both"
WANT_UI=false
WANT_IMAGE=false
IMAGE_TAG="teldrive-enchanced:build"
GO_REQUIRED="go1.27.4"
VERSION_STRING="v0.1.0-enchanced"
RCLONE_UPSTREAM_BASE="v1.76.0"
BUN_IMAGE="oven/bun:1.4.2-alpine@sha256:d888c0ae6c86d7866ff10c5aafdd9077b36aee6455b33dd270fb93c0dd5cef6f"
DISTROLESS_NAME="gcr.io/distroless/static-debian12:nonroot"

usage() {
  cat >&2 <<'EOF'
usage: build-repro.sh --snapshot DIR [--native teldrive|rclone|both] [--ui] [--image] [--image-tag TAG]
Builds each selected artifact twice from a sealed snapshot and compares digests.
  --ui         build the UI twice and compare sorted dist manifests
  --image      build the Docker image twice and compare binary plus base layers
  --image-tag  base image tag; runs use TAG-1 and TAG-2 (default teldrive-enchanced:build)
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot) [[ $# -ge 2 ]] || usage; SNAPSHOT="$2"; shift 2 ;;
    --native) [[ $# -ge 2 ]] || usage; NATIVE="$2"; shift 2 ;;
    --ui) WANT_UI=true; shift ;;
    --image) WANT_IMAGE=true; shift ;;
    --image-tag) [[ $# -ge 2 ]] || usage; IMAGE_TAG="$2"; shift 2 ;;
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
# The manifest top-level digest only binds manifest.txt itself. Re-hash every
# listed payload before extraction so a swapped or truncated artifact cannot
# be built while the receipt still claims the sealed source.
sealed_entries() { awk '/^sha256:/ { print substr($0, 8) }' "$SNAPSHOT/manifest.txt"; }
grep -q '^sha256:' "$SNAPSHOT/manifest.txt" || fail "snapshot manifest lists no sealed files"
if ! mismatch="$(cd -- "$SNAPSHOT" &&
  sealed_entries | sed 's/^\([0-9a-f]\{64\}\) /\1  /' | sha256sum -c --quiet - 2>&1)"; then
  fail "snapshot file digest mismatch: $mismatch"
fi

go_version="$(go version)"
printf '%s\n' "$go_version" | grep -Eq "(^| )$GO_REQUIRED( |$)" ||
  fail "Go toolchain must be exactly $GO_REQUIRED (found: $go_version)"
GO_VERSION="$GO_REQUIRED"

if [[ "$NATIVE" != rclone || "$WANT_UI" == true || "$WANT_IMAGE" == true ]]; then
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

# --- Sealed metadata (never host checkout or clock) -------------------------
meta_for() {
  local repo="$1" root="$WORK/extract/$1"
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$(git -C "$root" rev-parse --short=12 HEAD)" \
    "$(git -C "$root" log -1 --format=%cI)" \
    "$(git -C "$root" log -1 --format=%ct)" \
    "$(head_for "$repo")" \
    "$(dirty_for "$repo")"
}
IFS=$'\t' read -r TD_SHORT TD_DATE TD_EPOCH TD_HEAD TD_DIRTY < <(meta_for teldrive-enchanced)
IFS=$'\t' read -r RC_SHORT RC_DATE RC_EPOCH RC_HEAD RC_DIRTY < <(meta_for rclone-enchanced)

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
  head="$TD_HEAD"
  short="$TD_SHORT"
  cdate="$TD_DATE"
  ldflags="-s -w -X main.version=$VERSION_STRING -X main.commit=$short -X main.date=$cdate -X main.snapshot=$SNAP_DIGEST -X main.dirty=$TD_DIRTY"

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
  "$WORK/out1/teldrive" version | grep -qx -- "- snapshot: $SNAP_DIGEST" ||
    fail "teldrive binary does not report the sealed snapshot digest"
  "$WORK/out1/teldrive" version | grep -qx -- "- dirty: $TD_DIRTY" ||
    fail "teldrive binary does not report the sealed dirty state"
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
  head="$RC_HEAD"
  short="$RC_SHORT"
  cdate="$RC_DATE"
  ldflags="-s -w -X github.com/rclone/rclone/fs.Version=$VERSION_STRING -X github.com/rclone/rclone/fs.BuildCommit=$short -X github.com/rclone/rclone/fs.BuildDate=$cdate -X github.com/rclone/rclone/fs.BuildSnapshot=$SNAP_DIGEST -X github.com/rclone/rclone/fs.BuildDirty=$RC_DIRTY"

  for run in 1 2; do
    seed_run_caches "$run"
    out="$WORK/out$run/rclone"
    build_go "$repo" "$run" "$out" . "$ldflags"
  done

  a1="$(sha256sum "$WORK/out1/rclone" | cut -d' ' -f1)"
  a2="$(sha256sum "$WORK/out2/rclone" | cut -d' ' -f1)"
  [[ "$("$WORK/out1/rclone" version | head -n 1)" == "rclone $VERSION_STRING" ]] ||
    fail "rclone binary does not report $VERSION_STRING"
  "$WORK/out1/rclone" version | grep -qx -- "- build/snapshot: $SNAP_DIGEST" ||
    fail "rclone binary does not report the sealed snapshot digest"
  "$WORK/out1/rclone" version | grep -qx -- "- build/commit: $short" ||
    fail "rclone binary does not report the sealed commit"
  bin_eq=false; cmp -s "$WORK/out1/rclone" "$WORK/out2/rclone" && bin_eq=true
  [[ "$bin_eq" == true ]] || { printf 'build-repro: rclone artifact mismatch\n' >&2; OVERALL=FAIL; }

  ARTIFACT_JSON[$repo]="$(jq -n \
    --arg repo "$repo" --arg head "$head" --arg dirty "$(dirty_for "$repo")" \
    --arg go "$GO_VERSION" --arg a1 "$a1" --arg a2 "$a2" --argjson bin_eq "$bin_eq" \
    --arg upstream "$RCLONE_UPSTREAM_BASE" \
    --argjson commands "$(json_commands \
      "GOTOOLCHAIN=local GOFLAGS=-mod=readonly GOPROXY=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags '$ldflags' -o out/rclone .")" \
    --arg outcome "$([[ "$bin_eq" == true ]] && echo PASS || echo FAIL)" \
    '{repo:$repo, head:$head, dirty:($dirty=="true"), go_version:$go,
      artifact_sha256_run1:$a1, artifact_sha256_run2:$a2, binary_equal:$bin_eq,
      upstream_base:$upstream, commands:$commands, outcome:$outcome}')"
}

[[ "$NATIVE" == rclone ]] || build_teldrive
[[ "$NATIVE" == teldrive ]] || build_rclone

UI_JSON="null"
if [[ "$WANT_UI" == true ]]; then
  for run in 1 2; do
    root="$WORK/ui-tree$run"
    remove_tree "$root"
    cp -a "$WORK/extract/teldrive-enchanced" "$root"
    remove_tree "$root/.git"
    build_ui_tree "$root" "$run"
    ui_manifest "$root" "$WORK/ui-only-manifest$run.txt"
  done
  ui_a="$(sha256sum "$WORK/ui-only-manifest1.txt" | cut -d' ' -f1)"
  ui_b="$(sha256sum "$WORK/ui-only-manifest2.txt" | cut -d' ' -f1)"
  ui_eq=false; cmp -s "$WORK/ui-only-manifest1.txt" "$WORK/ui-only-manifest2.txt" && ui_eq=true
  [[ "$ui_eq" == true ]] || { printf 'build-repro: UI manifest mismatch\n' >&2; OVERALL=FAIL; }
  UI_JSON="$(jq -n --arg a "$ui_a" --arg b "$ui_b" --argjson eq "$ui_eq" \
    --arg outcome "$([[ "$ui_eq" == true ]] && echo PASS || echo FAIL)" \
    '{ui_manifest_sha256_run1:$a, ui_manifest_sha256_run2:$b, ui_manifest_equal:$eq, outcome:$outcome}')"
fi

IMAGE_JSON="null"
if [[ "$WANT_IMAGE" == true ]]; then
  image_root="$WORK/extract/teldrive-enchanced"
  distroless_digest="$(awk 'toupper($1)=="FROM" {print $2}' "$image_root/Dockerfile" |
    sed -n 's/.*@\(sha256:[0-9a-f]\{64\}\)$/\1/p' | tail -n 1)"
  [[ "$distroless_digest" == sha256:* ]] ||
    fail "cannot read the pinned distroless digest from the Dockerfile"
  for run in 1 2; do
    root="$WORK/image-src$run"
    remove_tree "$root"
    cp -a "$image_root" "$root"
    remove_tree "$root/.git"
    tag="${IMAGE_TAG}-${run}"
    log "building image $tag"
    docker buildx build --no-cache --load --provenance=false \
      --build-arg "VERSION=$VERSION_STRING" \
      --build-arg "COMMIT=$TD_SHORT" \
      --build-arg "BUILD_DATE=$TD_DATE" \
      --build-arg "SNAPSHOT=$SNAP_DIGEST" \
      --build-arg "DIRTY=$TD_DIRTY" \
      --build-arg "SOURCE_DATE_EPOCH=$TD_EPOCH" \
      --build-arg "DISTROLESS_DIGEST=$distroless_digest" \
      -t "$tag" "$root" >/dev/null ||
      fail "image build failed for $tag"
    cid="$(docker create "$tag")"
    docker cp "$cid:/usr/bin/teldrive" "$WORK/img-bin$run" >/dev/null
    docker rm "$cid" >/dev/null
    docker image inspect "$tag" --format '{{json .RootFS.Layers}}' > "$WORK/img-layers$run.json"
    docker image inspect "$tag" --format '{{.Id}}' > "$WORK/img-id$run.txt"
  done
  img_a="$(sha256sum "$WORK/img-bin1" | cut -d' ' -f1)"
  img_b="$(sha256sum "$WORK/img-bin2" | cut -d' ' -f1)"
  img_bin_eq=false; cmp -s "$WORK/img-bin1" "$WORK/img-bin2" && img_bin_eq=true
  base_eq="$(jq -n --slurpfile a "$WORK/img-layers1.json" --slurpfile b "$WORK/img-layers2.json" \
    '($a[0][:-1] == $b[0][:-1])')"
  app_a="$(jq -r '.[-1] // ""' "$WORK/img-layers1.json")"
  app_b="$(jq -r '.[-1] // ""' "$WORK/img-layers2.json")"
  layers_eq=false
  [[ "$(cat "$WORK/img-layers1.json")" == "$(cat "$WORK/img-layers2.json")" ]] && layers_eq=true
  if [[ "$img_bin_eq" != true ]]; then
    image_outcome=FAIL
    OVERALL=FAIL
    printf 'build-repro: image binary mismatch\n' >&2
  elif [[ "$layers_eq" == true ]]; then
    image_outcome=PASS
  elif [[ "$base_eq" == true ]]; then
    image_outcome=PASS_WITH_LAYER_DIFFERENCE
    printf 'build-repro: final layer differs (%s vs %s); binary and base layers match\n' "$app_a" "$app_b" >&2
  else
    image_outcome=FAIL
    OVERALL=FAIL
    printf 'build-repro: base image layers differ between runs\n' >&2
  fi
  IMAGE_JSON="$(jq -n \
    --arg tag1 "${IMAGE_TAG}-1" --arg tag2 "${IMAGE_TAG}-2" \
    --arg id1 "$(cat "$WORK/img-id1.txt")" --arg id2 "$(cat "$WORK/img-id2.txt")" \
    --arg bin1 "$img_a" --arg bin2 "$img_b" --argjson bin_eq "$img_bin_eq" \
    --argjson layers_eq "$layers_eq" --argjson base_eq "$base_eq" \
    --arg app1 "$app_a" --arg app2 "$app_b" --arg outcome "$image_outcome" \
    --arg distroless "$distroless_digest" \
    --arg distroless_name "$DISTROLESS_NAME" \
    '{tag_run1:$tag1, tag_run2:$tag2, image_id_run1:$id1, image_id_run2:$id2,
      binary_sha256_run1:$bin1, binary_sha256_run2:$bin2, image_binary_equal:$bin_eq,
      image_layers_equal:$layers_eq, image_base_layers_equal:$base_eq,
      application_layer_sha256_run1:$app1, application_layer_sha256_run2:$app2,
      base_image_name:$distroless_name, base_image_digest:$distroless, image_outcome:$outcome}')"
fi

artifacts="{}"
for artifact in "${!ARTIFACT_JSON[@]}"; do
  artifacts="$(jq -c --arg k "$artifact" --argjson v "${ARTIFACT_JSON[$artifact]}" '. + {($k): $v}' <<<"$artifacts")"
done

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RECEIPT="$WORK/receipt.json"
metadata="$(jq -n \
  --arg version "$VERSION_STRING" --arg snap "$SNAP_DIGEST" \
  --arg upstream "$RCLONE_UPSTREAM_BASE" \
  --arg td_short "$TD_SHORT" --arg td_head "$TD_HEAD" --arg td_date "$TD_DATE" \
  --arg td_epoch "$TD_EPOCH" --argjson td_dirty "$TD_DIRTY" \
  --arg rc_short "$RC_SHORT" --arg rc_head "$RC_HEAD" --arg rc_date "$RC_DATE" \
  --arg rc_epoch "$RC_EPOCH" --argjson rc_dirty "$RC_DIRTY" \
  '{version:$version, snapshot_sha256:$snap, upstream_base:$upstream,
    teldrive:{commit:$td_short, commit_full:$td_head, build_date:$td_date,
      build_epoch:($td_epoch|tonumber), dirty:$td_dirty},
    rclone:{commit:$rc_short, commit_full:$rc_head, build_date:$rc_date,
      build_epoch:($rc_epoch|tonumber), dirty:$rc_dirty}}')"
jq -n --arg snap "$SNAP_DIGEST" --arg go "$GO_VERSION" --arg version "$VERSION_STRING" \
  --argjson artifacts "$artifacts" --argjson metadata "$metadata" \
  --argjson ui "$UI_JSON" --argjson image "$IMAGE_JSON" \
  --arg started "$STARTED_AT" --arg finished "$FINISHED_AT" --arg outcome "$OVERALL" \
  '{snapshot_sha256:$snap, version:$version, go_version:$go, started_at:$started,
    finished_at:$finished, outcome:$outcome, metadata:$metadata, ui:$ui, image:$image,
    artifacts:$artifacts}' > "$RECEIPT"
chmod 600 "$RECEIPT"

printf 'BUILD %s snapshot_sha256=%s receipt=%s/work-%s/receipt.json\n' \
  "$OVERALL" "$SNAP_DIGEST" "$(basename -- "$PRIVATE_ROOT")" "$SNAP_NAME"

[[ "$OVERALL" == PASS ]] || exit 1
