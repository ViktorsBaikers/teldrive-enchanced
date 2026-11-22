#!/usr/bin/env bash
# Pinned vulnerability gate: source-mode reachability for both modules, optional
# binary symbol presence, SBOM and image scan. Fails closed on a reachable
# High/Critical finding, unknown severity, or an unpinned tool.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
RCLONE_DIR="$(cd -- "$PROJECT_DIR/../rclone-enchanced" 2>/dev/null && pwd || true)"

GOVULNCHECK_VERSION="v1.10.0"
SYFT_VERSION="v1.57.2"
GRYPE_VERSION="v0.122.1"
SYFT_REPO="anchore/syft"
GRYPE_REPO="anchore/grype"

SOURCE="both"
OUT=""
IMAGE=""
WANT_SBOM=false
declare -a BINARIES=()

usage() {
  cat >&2 <<'EOF'
usage: vuln-gate.sh [--source teldrive|rclone|both] [--binary PATH]... [--image IMAGE] [--sbom] --out DIR
  --source   module roots to scan in govulncheck source mode (default both)
  --binary   repeatable; scan a built Linux artifact in binary mode
             (symbol presence, never reachability)
  --image    local runtime image to scan with grype
  --sbom     write a CycloneDX SBOM for --image (requires --image)
  --out      private output directory for JSON, SBOM, and summary.txt
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) [[ $# -ge 2 ]] || usage; SOURCE="$2"; shift 2 ;;
    --binary) [[ $# -ge 2 ]] || usage; BINARIES+=("$2"); shift 2 ;;
    --image) [[ $# -ge 2 ]] || usage; IMAGE="$2"; shift 2 ;;
    --sbom) WANT_SBOM=true; shift ;;
    --out) [[ $# -ge 2 ]] || usage; OUT="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

[[ -n "$OUT" ]] || usage
case "$SOURCE" in teldrive|rclone|both) ;; *) usage ;; esac
if [[ "$WANT_SBOM" == true && -z "$IMAGE" ]]; then
  echo "vuln-gate: --sbom requires --image" >&2
  exit 2
fi

umask 077
mkdir -p "$OUT"
OUT="$(cd -- "$OUT" && pwd)"
SUMMARY="$OUT/summary.txt"
: > "$SUMMARY"

status=PASS
note() { printf '%s\n' "$*" >> "$SUMMARY"; }
fail_close() {
  note ""
  note "GATE: FAIL — $*"
  printf 'vuln-gate: %s\n' "$*" >&2
  exit 1
}
log() { printf 'vuln-gate: %s\n' "$*" >&2; }

STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
note "# B6 vulnerability gate"
note ""
note "- started_at: $STARTED_AT"
note "- pinned: govulncheck@$GOVULNCHECK_VERSION, syft $SYFT_VERSION, grype $GRYPE_VERSION"
note "- scan output stays in this private directory; this file is the sanitized report"
note ""

# --- Pinned tool -------------------------------------------------------------
GOV_VERSION_OUT="$(GOTOOLCHAIN=local go run "golang.org/x/vuln/cmd/govulncheck@v1.10.0" -version 2>&1)" ||
  fail_close "cannot run pinned govulncheck@$GOVULNCHECK_VERSION"
if ! printf '%s\n' "$GOV_VERSION_OUT" | grep -q "$GOVULNCHECK_VERSION"; then
  fail_close "govulncheck reported an unexpected version: $(printf '%s' "$GOV_VERSION_OUT" | tr '\n' ' ')"
fi
GOV_DB_DATE="$(printf '%s\n' "$GOV_VERSION_OUT" | sed -n 's/^DB updated:[[:space:]]*//p' | head -n 1)"
[[ -n "$GOV_DB_DATE" ]] || fail_close "govulncheck did not report the vulnerability DB date"
note "## Tool identity"
note ""
note '| Tool | Version | Identity |'
note '|------|---------|----------|'
note "| govulncheck | $GOVULNCHECK_VERSION | pinned via go run; DB updated $GOV_DB_DATE |"
note "| syft | $SYFT_VERSION | resolved below when image scanning runs |"
note "| grype | $GRYPE_VERSION | resolved below when image scanning runs |"
note ""

# Reachable source findings carry a call path (a trace frame with a function).
# Module-level findings have no function and are recorded without blocking.
reachable_tsv() {
  jq -r '
    select(.finding) | .finding
    | select(any(.trace[]?; ((.function // "") != "")))
    | [ .osv, (.fixed_version // "none"),
        ([ .trace[] | select((.function // "") != "") | ((.module // "") + ":" + .function) ] | join(" -> ")) ]
    | @tsv' "$1"
}

symbol_tsv() {
  jq -r '
    select(.finding) | .finding
    | [ .osv, (.trace[0].module // "unknown") ]
    | @tsv' "$1"
}

declare -A SEVERITY=()
severity_row() { SEVERITY["$1"]="$2"; }
severity_for() { printf '%s' "${SEVERITY[$1]:-unknown}"; }

scan_source() {
  local label="$1"
  local dir="$2"
  local json="$OUT/govulncheck-$label.json"
  local err="$OUT/govulncheck-$label.err"
  [[ -d "$dir" ]] || fail_close "source root for $label not found"
  log "source scan $label"
  ( cd "$dir" && GOTOOLCHAIN=local go run "golang.org/x/vuln/cmd/govulncheck@v1.10.0" -json ./... ) \
    >"$json" 2>"$err" ||
    fail_close "govulncheck source scan failed for $label (see private $err)"
  [[ -s "$json" ]] || fail_close "govulncheck produced no JSON for $label"
}

# --- Optional image scan: identity, SBOM, severity evidence ------------------
IMAGE_ID=""
GRYPE_DB_DATE=""
SYFT_DIGEST=""
GRYPE_DIGEST=""

resolve_digest() {
  local ref="$1"
  docker buildx imagetools inspect "$ref" --format '{{json .Manifest}}' 2>/dev/null |
    jq -r '.digest // empty'
}

run_image_scan() {
  command -v docker >/dev/null 2>&1 || fail_close "docker is required for --image/--sbom"
  docker image inspect "$IMAGE" >/dev/null 2>&1 ||
    fail_close "image $IMAGE is not present locally"
  IMAGE_ID="$(docker image inspect "$IMAGE" --format '{{.Id}}')"

  SYFT_DIGEST="$(resolve_digest "$SYFT_REPO:$SYFT_VERSION")" ||
    fail_close "cannot resolve syft:$SYFT_VERSION to a digest"
  GRYPE_DIGEST="$(resolve_digest "$GRYPE_REPO:$GRYPE_VERSION")" ||
    fail_close "cannot resolve grype:$GRYPE_VERSION to a digest"
  [[ "$SYFT_DIGEST" == sha256:* && "$GRYPE_DIGEST" == sha256:* ]] ||
    fail_close "scanner tag did not resolve to a sha256 digest"

  log "image scan $IMAGE"
  mkdir -p "$OUT/grype-db"
  docker run --rm -e GRYPE_DB_CACHE_DIR=/db -v "$OUT/grype-db:/db" \
    "$GRYPE_REPO@$GRYPE_DIGEST" db update >"$OUT/grype-db-update.log" 2>&1 ||
    fail_close "grype DB update failed (see private grype-db-update.log)"

  GRYPE_DB_DATE="$(docker run --rm -e GRYPE_DB_CACHE_DIR=/db -v "$OUT/grype-db:/db" \
    "$GRYPE_REPO@$GRYPE_DIGEST" db status 2>/dev/null |
    sed -n 's/^[[:space:]]*Built:[[:space:]]*//p' | head -n 1)"
  [[ -n "$GRYPE_DB_DATE" ]] || fail_close "grype DB build date could not be recorded"

  docker run --rm -e GRYPE_DB_CACHE_DIR=/db -v "$OUT/grype-db:/db" \
    -v /var/run/docker.sock:/var/run/docker.sock \
    "$GRYPE_REPO@$GRYPE_DIGEST" -q -o json "$IMAGE" >"$OUT/grype-image.json" 2>"$OUT/grype-image.err" ||
    fail_close "grype image scan failed (see private grype-image.err)"

  # Severity is taken from grype's DB, keyed by primary and alias IDs.
  while IFS=$'\t' read -r id sev; do
    if [[ -n "$id" ]]; then severity_for_id "$id" "$sev"; fi
  done < <(jq -r '
    .matches[]? | .vulnerability.severity as $s
    | ([.vulnerability.id] + [.relatedVulnerabilities[]?.id])[]
    | select(. != null) | [., $s] | @tsv' "$OUT/grype-image.json" | sort -u)
}

severity_for_id() { severity_row "$1" "${2,,}"; }

if [[ -n "$IMAGE" ]]; then
  run_image_scan
fi

if [[ "$WANT_SBOM" == true ]]; then
  log "SBOM $IMAGE"
  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
    "$SYFT_REPO@$SYFT_DIGEST" -q -o cyclonedx-json "docker:$IMAGE" \
    >"$OUT/sbom.cdx.json" 2>"$OUT/syft.err" ||
    fail_close "syft SBOM generation failed (see private syft.err)"
  [[ -s "$OUT/sbom.cdx.json" ]] || fail_close "SBOM is empty"
fi

# --- Source-mode reachability (authority) ------------------------------------
[[ "$SOURCE" == rclone ]] || scan_source teldrive "$PROJECT_DIR"
[[ "$SOURCE" == teldrive ]] || scan_source rclone "$RCLONE_DIR"

note "## Source-mode reachability (authority)"
note ""
note '| Module | Reachable findings |'
note '|--------|--------------------|'
for label in teldrive rclone; do
  [[ -f "$OUT/govulncheck-$label.json" ]] || continue
  count="$(reachable_tsv "$OUT/govulncheck-$label.json" | wc -l)"
  note "| $label | $count |"
done
note ""

REACHABLE_ROWS="$OUT/reachable.tsv"
: > "$REACHABLE_ROWS"
for label in teldrive rclone; do
  [[ -f "$OUT/govulncheck-$label.json" ]] || continue
  reachable_tsv "$OUT/govulncheck-$label.json" |
    while IFS=$'\t' read -r id fixed path; do
      [[ -n "$id" ]] || continue
      printf '%s\t%s\t%s\t%s\n' "$id" "$label" "$fixed" "$path" >>"$REACHABLE_ROWS"
    done
done

note "## Reachable findings and decisions"
note ""
if [[ ! -s "$REACHABLE_ROWS" ]]; then
  note "None. No source-level call path reaches a known vulnerable symbol in the scanned modules."
else
  note '| ID | Module | Fixed | Severity evidence | Call path | Decision |'
  note '|----|--------|-------|-------------------|-----------|----------|'
  while IFS=$'\t' read -r id module fixed path; do
    sev="$(severity_for "$id")"
    decision="block"
    case "$sev" in
      critical|high) status=FAIL ;;
      medium|low) decision="record" ;;
      *) sev="unknown"; decision="block (unknown severity)" ;;
    esac
    note "| $id | $module | $fixed | $sev | $path | $decision |"
  done <"$REACHABLE_ROWS"
  note ""
  note "Reachable High/Critical or unknown severity blocks the accepted build (D-17)."
fi
note ""

# --- Binary symbol presence (supplementary only) -----------------------------
if ((${#BINARIES[@]} > 0)); then
  note "## Binary-mode symbol presence (supplementary, NOT reachability)"
  note ""
  note '| Artifact | Finding | Module | Note |'
  note '|----------|---------|--------|------|'
  for bin in "${BINARIES[@]}"; do
    [[ -f "$bin" ]] || fail_close "binary artifact not found: $bin"
    name="$(basename -- "$bin")"
    json="$OUT/govulncheck-binary-$name.json"
    log "binary scan $name"
    GOTOOLCHAIN=local go run "golang.org/x/vuln/cmd/govulncheck@v1.10.0" \
      -mode=binary -json "$bin" >"$json" 2>"$OUT/govulncheck-binary-$name.err" ||
      fail_close "govulncheck binary scan failed for $name"
    found=false
    while IFS=$'\t' read -r id module; do
      [[ -n "$id" ]] || continue
      found=true
      note "| $name | $id | $module | vulnerable symbol present; call path not proven |"
    done < <(symbol_tsv "$json" | sort -u)
    if [[ "$found" == false ]]; then
      note "| $name | none | - | no vulnerable symbols matched |"
    fi
  done
  note ""
  note "Binary mode reports linked symbols only. Reachability authority is source mode."
  note ""
fi

# --- Image evidence ----------------------------------------------------------
if [[ -n "$IMAGE" ]]; then
  MATCH_COUNT="$(jq -r '.matches | length' "$OUT/grype-image.json")"
  note "## Image scan and SBOM"
  note ""
  note "- image tag: $IMAGE"
  note "- image id: $IMAGE_ID"
  note "- runtime target: final distroless stage of the built image"
  note "- grype findings: $MATCH_COUNT"
  note "- grype DB build date: $GRYPE_DB_DATE"
  note "- syft: $SYFT_REPO:$SYFT_VERSION@$SYFT_DIGEST"
  note "- grype: $GRYPE_REPO:$GRYPE_VERSION@$GRYPE_DIGEST"
  note "- SBOM: $([[ -s "$OUT/sbom.cdx.json" ]] && echo "sbom.cdx.json present" || echo "not requested")"
  note ""
  note '| Severity | Count |'
  note '|----------|-------|'
  if [[ "$MATCH_COUNT" -eq 0 ]]; then
    note "| none | 0 |"
  else
    jq -r '[.matches[]?.vulnerability.severity] | group_by(.)[] | "| \(.[0]) | \(length) |"' \
      "$OUT/grype-image.json" >>"$SUMMARY"
  fi
  note ""
  note "Image findings are environment/base evidence. Source-mode reachability decides accepted-build risk."
  note ""
fi

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
note "## Result"
note ""
if [[ "$status" == PASS ]]; then
  note "- outcome: PASS"
  note "- finished_at: $FINISHED_AT"
  printf 'vuln-gate: PASS summary=%s\n' "$SUMMARY" >&2
  exit 0
fi
note "- outcome: FAIL (reachable High/Critical or unknown severity)"
note "- finished_at: $FINISHED_AT"
printf 'vuln-gate: FAIL summary=%s\n' "$SUMMARY" >&2
exit 1
