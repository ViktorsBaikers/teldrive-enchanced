#!/usr/bin/env bash
# Assert build metadata on binaries and the tested image against a receipt.
# Prints allowlisted keys only; never dumps full label maps.
set -euo pipefail

TD_BIN=""
RC_BIN=""
IMAGE=""
RECEIPT=""

usage() {
  cat >&2 <<'EOF'
usage: verify-metadata.sh [--teldrive-binary PATH] [--rclone-binary PATH] [--image TAG] [--receipt PATH]
Asserts non-empty, non-placeholder provenance and receipt agreement.
Receipt defaults to the newest private work receipt when omitted.
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --teldrive-binary) [[ $# -ge 2 ]] || usage; TD_BIN="$2"; shift 2 ;;
    --rclone-binary) [[ $# -ge 2 ]] || usage; RC_BIN="$2"; shift 2 ;;
    --image) [[ $# -ge 2 ]] || usage; IMAGE="$2"; shift 2 ;;
    --receipt) [[ $# -ge 2 ]] || usage; RECEIPT="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

[[ -n "$TD_BIN$RC_BIN$IMAGE" ]] || usage

status=0
fail() { printf 'FAIL %s expected=%s actual=%s\n' "$1" "$2" "$3" >&2; status=1; }

require_value() {
  local field="$1" value="$2"
  case "$value" in
    ""|unknown|dev|"<no value>") fail "$field" "non-placeholder value" "${value:-<empty>}" ;;
  esac
}

check_eq() {
  local field="$1" expected="$2" actual="$3"
  [[ "$expected" == "$actual" ]] || fail "$field" "$expected" "$actual"
}

if [[ -n "$TD_BIN" ]]; then
  out="$("$TD_BIN" version)"
  td_ver="$(sed -n '1s/^teldrive //p' <<<"$out")"
  td_commit="$(sed -n 's/^- commit: //p' <<<"$out")"
  td_built="$(sed -n 's/^- built: //p' <<<"$out")"
  td_snap="$(sed -n 's/^- snapshot: //p' <<<"$out")"
  td_dirty="$(sed -n 's/^- dirty: //p' <<<"$out")"
  require_value teldrive.version "$td_ver"
  require_value teldrive.commit "$td_commit"
  require_value teldrive.built "$td_built"
  require_value teldrive.snapshot "$td_snap"
  require_value teldrive.dirty "$td_dirty"
  [[ "$td_ver" == *-enchanced* ]] || fail teldrive.version "contains -enchanced" "$td_ver"
  [[ "$td_ver" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-enchanced$ ]] ||
    fail teldrive.version "fork semver, not an upstream release" "$td_ver"
  [[ "$td_snap" =~ ^[0-9a-f]{64}$ ]] || fail teldrive.snapshot "64 hex characters" "$td_snap"
  [[ "$td_dirty" == true || "$td_dirty" == false ]] || fail teldrive.dirty "true|false" "$td_dirty"
fi

if [[ -n "$RC_BIN" ]]; then
  out="$("$RC_BIN" version)"
  ver="$(sed -n '1s/^rclone //p' <<<"$out")"
  commit="$(sed -n 's/^- build\/commit: //p' <<<"$out")"
  built="$(sed -n 's/^- build\/date: //p' <<<"$out")"
  snap="$(sed -n 's/^- build\/snapshot: //p' <<<"$out")"
  dirty="$(sed -n 's/^- build\/dirty: //p' <<<"$out")"
  require_value rclone.version "$ver"
  require_value rclone.commit "$commit"
  require_value rclone.build_date "$built"
  require_value rclone.snapshot "$snap"
  require_value rclone.dirty "$dirty"
  [[ "$ver" == *-enchanced* ]] || fail rclone.version "contains -enchanced" "$ver"
  [[ "$ver" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-enchanced$ ]] ||
    fail rclone.version "fork semver, not an upstream release" "$ver"
  [[ "$snap" =~ ^[0-9a-f]{64}$ ]] || fail rclone.snapshot "64 hex characters" "$snap"
  [[ "$dirty" == true || "$dirty" == false ]] || fail rclone.dirty "true|false" "$dirty"
fi

meta_version=""; meta_snap=""; meta_commit=""; meta_date=""; meta_dirty=""
if [[ -z "$RECEIPT" ]]; then
  RECEIPT="$(ls -t "${B6_PRIVATE_DIR:-$HOME/.local/state/teldrive-b6}"/work-*/receipt.json 2>/dev/null | head -n 1 || true)"
fi
if [[ -z "$RECEIPT" || ! -f "$RECEIPT" ]]; then
  fail receipt "readable receipt JSON" "${RECEIPT:-<none>}"
else
  meta_version="$(jq -r '.metadata.version // empty' "$RECEIPT")"
  meta_snap="$(jq -r '.metadata.snapshot_sha256 // empty' "$RECEIPT")"
  meta_commit="$(jq -r '.metadata.teldrive.commit // empty' "$RECEIPT")"
  meta_date="$(jq -r '.metadata.teldrive.build_date // empty' "$RECEIPT")"
  meta_dirty="$(jq -r '.metadata.teldrive.dirty // empty' "$RECEIPT")"
  require_value receipt.version "$meta_version"
  require_value receipt.snapshot_sha256 "$meta_snap"
  require_value receipt.teldrive.commit "$meta_commit"
  require_value receipt.teldrive.build_date "$meta_date"
  if [[ -n "$TD_BIN" ]]; then
    check_eq teldrive.version "$meta_version" "$td_ver"
    check_eq teldrive.snapshot "$meta_snap" "$td_snap"
    [[ "$meta_commit" == "$td_commit"* ]] ||
      fail teldrive.commit "$meta_commit (or its short form)" "$td_commit"
    check_eq teldrive.built "$meta_date" "$td_built"
    check_eq teldrive.dirty "$meta_dirty" "$td_dirty"
  fi
fi

if [[ -n "$IMAGE" ]]; then
  label() {
    docker image inspect "$IMAGE" --format "{{index .Config.Labels \"$1\"}}" 2>/dev/null || true
  }
  revision="$(label org.opencontainers.image.revision)"
  image_version="$(label org.opencontainers.image.version)"
  base_name="$(label org.opencontainers.image.base.name)"
  base_digest="$(label org.opencontainers.image.base.digest)"
  image_snap="$(label io.enchanced.source.snapshot)"
  image_dirty="$(label io.enchanced.source.dirty)"
  require_value image.revision "$revision"
  require_value image.version "$image_version"
  require_value image.base.name "$base_name"
  require_value image.base.digest "$base_digest"
  require_value image.source.snapshot "$image_snap"
  require_value image.source.dirty "$image_dirty"
  [[ "$image_version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-enchanced$ ]] ||
    fail image.version "fork semver, not an upstream release" "$image_version"
  [[ "$base_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail image.base.digest "sha256 digest" "$base_digest"
  [[ "$image_snap" =~ ^[0-9a-f]{64}$ ]] || fail image.source.snapshot "64 hex characters" "$image_snap"
  [[ "$image_dirty" == true || "$image_dirty" == false ]] || fail image.source.dirty "true|false" "$image_dirty"
  if [[ -f "$RECEIPT" ]]; then
    receipt_image_digest="$(jq -r '.image.base_image_digest // empty' "$RECEIPT")"
    receipt_base_name="$(jq -r '.image.base_image_name // empty' "$RECEIPT")"
    require_value receipt.image.base_image_digest "$receipt_image_digest"
    require_value receipt.image.base_image_name "$receipt_base_name"
    check_eq image.revision "$meta_commit" "$revision"
    check_eq image.version "$meta_version" "$image_version"
    check_eq image.source.snapshot "$meta_snap" "$image_snap"
    check_eq image.source.dirty "$meta_dirty" "$image_dirty"
    check_eq image.base.digest "$receipt_image_digest" "$base_digest"
    check_eq image.base.name "$receipt_base_name" "$base_name"
  fi
fi

if [[ "$status" -ne 0 ]]; then
  printf 'METADATA FAIL\n' >&2
  exit 1
fi
printf 'METADATA PASS\n'
