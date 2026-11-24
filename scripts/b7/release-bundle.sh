#!/usr/bin/env bash
# Assemble the private B7 release bundle. Local only: never push, tag, or publish.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
ROOT_DIR="$(cd -- "$PROJECT_DIR/.." && pwd)"
DOCS="$ROOT_DIR/docs/research/teldrive-rclone"
CANDIDATE_IMAGE="teldrive-enchanced:b7-candidate-1"
EXPECTED_ANCHOR="sha256:aeb7071a049882720e4cb6db10c3b48309d082dc0be03cd5fbc8cbd429947f44"

SNAPSHOT=""; BUILD_RECEIPT=""; IMAGE=""; VULN=""; SBOM=""; SCHEMA=""; CLEANUP=""; OUT=""; SELF_TEST=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot) SNAPSHOT="$2"; shift 2 ;;
    --build-receipt) BUILD_RECEIPT="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --vuln-receipt) VULN="$2"; shift 2 ;;
    --sbom) SBOM="$2"; shift 2 ;;
    --schema-compat-receipt) SCHEMA="$2"; shift 2 ;;
    --cleanup-receipt) CLEANUP="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --self-test) SELF_TEST=true; shift ;;
    -h|--help) echo "usage: release-bundle.sh --snapshot DIR --build-receipt F --image TAG --vuln-receipt F --sbom F --schema-compat-receipt F --cleanup-receipt F --out DIR | --self-test" >&2; exit 2 ;;
    *) echo "release-bundle: unknown argument: $1" >&2; exit 2 ;;
  esac
done

fail() { printf 'release-bundle: %s\n' "$*" >&2; exit 1; }

require_file() { [[ -f "$1" ]] || fail "missing-release-input: $1"; }

sha() { sha256sum "$1" | cut -d' ' -f1; }

checksums() {
  local out="$1"
  ( cd "$out" && find . -maxdepth 1 -type f ! -name checksums.sha256 -printf '%f\n' | LC_ALL=C sort | while read -r f; do printf '%s  %s\n' "$(sha256sum "$f" | cut -d' ' -f1)" "$f"; done > checksums.sha256 )
  ( cd "$out" && sha256sum -c checksums.sha256 >/dev/null ) || fail "checksum-verify-failed"
}

assemble() {
  [[ -n "$SNAPSHOT" && -n "$BUILD_RECEIPT" && -n "$IMAGE" && -n "$VULN" && -n "$SBOM" && -n "$SCHEMA" && -n "$CLEANUP" && -n "$OUT" ]] || fail "missing-argument"
  [[ "$IMAGE" == "$CANDIDATE_IMAGE" ]] || fail "image-not-candidate"
  [[ -d "$SNAPSHOT" ]] || fail "missing-release-input: snapshot"
  require_file "$BUILD_RECEIPT"
  require_file "$VULN"
  require_file "$SBOM"
  require_file "$SCHEMA"
  require_file "$CLEANUP"
  [[ ! -e "$OUT" ]] || fail "output-exists"

  local work; work="$(dirname "$SNAPSHOT")/work-$(basename "$SNAPSHOT")"
  require_file "$work/out1/teldrive"
  require_file "$work/out1/rclone"

  umask 077
  mkdir -p "$OUT"; chmod 700 "$OUT"
  cp -- "$work/out1/teldrive" "$OUT/teldrive"; chmod 600 "$OUT/teldrive"
  cp -- "$work/out1/rclone" "$OUT/rclone"; chmod 600 "$OUT/rclone"
  cp -- "$BUILD_RECEIPT" "$OUT/build-receipt.json"; chmod 600 "$OUT/build-receipt.json"
  cp -- "$VULN" "$OUT/vulnerability-summary.txt"; chmod 600 "$OUT/vulnerability-summary.txt"
  cp -- "$SBOM" "$OUT/sbom.cdx.json"; chmod 600 "$OUT/sbom.cdx.json"
  cp -- "$SCHEMA" "$OUT/schema-compat-receipt.json"; chmod 600 "$OUT/schema-compat-receipt.json"
  cp -- "$CLEANUP" "$OUT/cleanup-receipt.json"; chmod 600 "$OUT/cleanup-receipt.json"
  local d
  for d in b7-provenance.md b7-acceptance.md b7-rollback.md b7-report.md; do
    [[ -f "$DOCS/$d" ]] && { cp -- "$DOCS/$d" "$OUT/$d"; chmod 600 "$OUT/$d"; }
  done

  docker save "$IMAGE" -o "$OUT/image.tar" || fail "image-save-failed"
  chmod 600 "$OUT/image.tar"
  local image_id image_digest
  image_id="$(docker image inspect -f '{{.Id}}' "$IMAGE")"
  image_digest="$(docker image inspect -f '{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null || true)"
  [[ -n "$image_digest" ]] || image_digest="local:$image_id"

  local td_sha rc_sha snap_seal schema_out
  td_sha="$(sha "$OUT/teldrive")"; rc_sha="$(sha "$OUT/rclone")"
  snap_seal="$(sha "$SNAPSHOT/manifest.txt")"
  schema_out="$(jq -r '.outcome // "UNKNOWN"' "$SCHEMA")"

  jq -n \
    --arg created "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg snapshot_sha "$(jq -r '.snapshot_sha256 // ""' "$BUILD_RECEIPT")" \
    --arg snapshot_seal "$snap_seal" \
    --arg td_sha "$td_sha" --arg rc_sha "$rc_sha" \
    --arg image "$IMAGE" --arg image_id "$image_id" --arg image_digest "$image_digest" \
    --arg anchor_image "$EXPECTED_ANCHOR" \
    --arg anchor_rclone "fc428540" \
    --arg schema_out "$schema_out" \
    '{kind:"b7-release-bundle-v1", created_at:$created,
      candidate:{snapshot_sha256:$snapshot_sha, snapshot_seal_sha256:$snapshot_seal,
        teldrive_sha256:$td_sha, rclone_sha256:$rc_sha,
        image:$image, image_id:$image_id, image_digest:$image_digest},
      rollback_anchor:{image:$anchor_image, rclone_binary_sha256_prefix:$anchor_rclone,
        note:"observed production identity; B6 candidate is not the anchor"},
      schema_compat:{outcome:$schema_out},
      not_run:["real-key production restore","original-quality Plex playback","Windows mount/Plex/Docker/PostgreSQL","nfsmount environment","production rollout"],
      pending_writes:"see b7-rollback.md",
      publication:"none - local private bundle only"}' >"$OUT/release-manifest.json"
  chmod 600 "$OUT/release-manifest.json"
  checksums "$OUT"
  printf 'release-bundle: PASS out=%s teldrive=%s rclone=%s image=%s\n' "$OUT" "${td_sha:0:16}" "${rc_sha:0:16}" "${image_id:0:16}"
}

self_test() {
  local tmp; tmp="$(mktemp -d "${TMPDIR:-/tmp}/b7-bundle-self.XXXXXX")"
  trap 'rm -rf -- "$tmp"' RETURN
  local ok=0
  mkdir -p "$tmp/snap"; : >"$tmp/snap/manifest.txt"; : >"$tmp/snap/snapshot.sha256"
  local work="$tmp/work-snap"; mkdir -p "$work/out1"; : >"$work/out1/teldrive"; : >"$work/out1/rclone"
  : >"$tmp/build.json"; : >"$tmp/vuln.txt"; : >"$tmp/sbom.json"; : >"$tmp/schema.json"; : >"$tmp/cleanup.json"
  # missing input
  if "$SCRIPT_DIR/release-bundle.sh" --snapshot "$tmp/snap" --build-receipt "$tmp/build.json" --image "$CANDIDATE_IMAGE" --vuln-receipt "$tmp/missing" --sbom "$tmp/sbom.json" --schema-compat-receipt "$tmp/schema.json" --cleanup-receipt "$tmp/cleanup.json" --out "$tmp/o1" >/dev/null 2>&1; then
    echo "self-test: FAIL missing-input accepted" >&2; ok=1
  else echo "self-test: PASS missing-input"; fi
  # wrong image
  if "$SCRIPT_DIR/release-bundle.sh" --snapshot "$tmp/snap" --build-receipt "$tmp/build.json" --image other:tag --vuln-receipt "$tmp/vuln.txt" --sbom "$tmp/sbom.json" --schema-compat-receipt "$tmp/schema.json" --cleanup-receipt "$tmp/cleanup.json" --out "$tmp/o2" >/dev/null 2>&1; then
    echo "self-test: FAIL wrong-image accepted" >&2; ok=1
  else echo "self-test: PASS wrong-image"; fi
  # checksum mismatch detection
  mkdir -p "$tmp/cs"; : >"$tmp/cs/a"; printf '%s  a\n' "0000000000000000000000000000000000000000000000000000000000000000" >"$tmp/cs/checksums.sha256"
  if ( cd "$tmp/cs" && sha256sum -c checksums.sha256 >/dev/null 2>&1 ); then
    echo "self-test: FAIL checksum mismatch accepted" >&2; ok=1
  else echo "self-test: PASS checksum-mismatch"; fi
  [[ "$ok" -eq 0 ]] || { echo "self-test: FAIL" >&2; return 1; }
  echo "self-test: PASS"
}

if [[ "$SELF_TEST" == true ]]; then self_test; else assemble; fi
