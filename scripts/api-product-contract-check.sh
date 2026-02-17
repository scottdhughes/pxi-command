#!/usr/bin/env bash
set -euo pipefail

API_URL="${1:-${API_URL:-https://api.pxicommand.com}}"

TMP_HEADERS="$(mktemp)"
TMP_BODY="$(mktemp)"
trap 'rm -f "$TMP_HEADERS" "$TMP_BODY"' EXIT

check_api_json_contract() {
  local path="$1"
  local jq_expr="$2"
  local label="$3"
  local code

  code=$(curl -sS -D "$TMP_HEADERS" -o "$TMP_BODY" -w "%{http_code}" --max-time 20 "$API_URL$path")
  if [[ "$code" != "200" ]]; then
    echo "Expected 200 for API contract $path but got $code"
    exit 1
  fi

  if ! grep -qi "^content-type: application/json" "$TMP_HEADERS"; then
    echo "Expected application/json content type for API contract $path"
    cat "$TMP_HEADERS"
    exit 1
  fi

  if ! jq -e "$jq_expr" "$TMP_BODY" > /dev/null; then
    echo "Contract assertion failed for $label ($path)"
    cat "$TMP_BODY"
    exit 1
  fi
}

check_api_json_contract "/api/plan" \
  'type=="object" and (.as_of|type=="string") and (.setup_summary|type=="string") and (.action_now.risk_allocation_target|type=="number") and (.edge_quality.score|type=="number") and (.edge_quality.calibration.quality|type=="string") and (.edge_quality.calibration.sample_size_7d|type=="number") and (.risk_band.d7.sample_size|type=="number") and (.invalidation_rules|type=="array")' \
  "plan"

check_api_json_contract "/api/brief?scope=market" \
  'type=="object" and (.as_of|type=="string") and (.summary|type=="string") and (.regime_delta|type=="string") and (.risk_posture|type=="string") and (.freshness_status.stale_count|type=="number")' \
  "brief"

check_api_json_contract "/api/opportunities?horizon=7d&limit=5" \
  'type=="object" and (.as_of|type=="string") and (.horizon|type=="string") and (.items|type=="array") and (all(.items[]?; (.id|type=="string") and (.theme_id|type=="string") and (.direction|type=="string") and (.conviction_score|type=="number") and (.calibration.quality|type=="string") and (.calibration.basis=="conviction_decile")))' \
  "opportunities"

check_api_json_contract "/api/alerts/feed?limit=10" \
  'type=="object" and (.as_of|type=="string") and (.alerts|type=="array") and ((.degraded_reason==null) or (.degraded_reason|type=="string")) and (all(.alerts[]?; (.id|type=="string") and (.event_type|type=="string") and (.severity|type=="string") and (.title|type=="string") and (.body|type=="string") and (.entity_type|type=="string") and (.created_at|type=="string")))' \
  "alerts-feed"

echo "Product API contract checks passed against $API_URL"
