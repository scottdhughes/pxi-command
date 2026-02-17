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

check_policy_state_consistency() {
  local plan_json
  local stance
  local conflict_state
  local base_signal
  local regime_context

  plan_json=$(curl -sS --max-time 20 "$API_URL/api/plan")

  stance=$(echo "$plan_json" | jq -r '.policy_state.stance // ""')
  if [[ -z "$stance" ]]; then
    # Backward-compatible path while policy_state rollout reaches all environments.
    return 0
  fi

  conflict_state=$(echo "$plan_json" | jq -r '.policy_state.conflict_state // ""')
  base_signal=$(echo "$plan_json" | jq -r '.policy_state.base_signal // ""')
  regime_context=$(echo "$plan_json" | jq -r '.policy_state.regime_context // ""')

  case "$stance" in
    RISK_ON|RISK_OFF|MIXED) ;;
    *)
      echo "Invalid policy_state.stance value: $stance"
      echo "$plan_json"
      exit 1
      ;;
  esac

  if [[ "$conflict_state" == "CONFLICT" && "$stance" != "MIXED" ]]; then
    echo "Unresolved contradiction: policy_state.conflict_state=CONFLICT but stance is not MIXED"
    echo "$plan_json"
    exit 1
  fi

  if [[ "$regime_context" == "RISK_ON" && ( "$base_signal" == "RISK_OFF" || "$base_signal" == "DEFENSIVE" ) && "$stance" != "MIXED" ]]; then
    echo "Unresolved contradiction: regime_context=RISK_ON with defensive base_signal but stance is not MIXED"
    echo "$plan_json"
    exit 1
  fi

  if [[ "$regime_context" == "RISK_OFF" && ( "$base_signal" == "FULL_RISK" || "$base_signal" == "REDUCED_RISK" ) && "$stance" != "MIXED" ]]; then
    echo "Unresolved contradiction: regime_context=RISK_OFF with risk-on base_signal but stance is not MIXED"
    echo "$plan_json"
    exit 1
  fi
}

check_api_json_contract "/api/plan" \
  'type=="object" and (.as_of|type=="string") and (.setup_summary|type=="string") and (.action_now.risk_allocation_target|type=="number") and (.edge_quality.score|type=="number") and (.edge_quality.calibration.quality|type=="string") and (.edge_quality.calibration.sample_size_7d|type=="number") and (.risk_band.d7.sample_size|type=="number") and (.invalidation_rules|type=="array") and ((.policy_state==null) or ((.policy_state.stance|type=="string") and (.policy_state.risk_posture|type=="string") and (.policy_state.conflict_state|type=="string") and (.policy_state.base_signal|type=="string") and (.policy_state.regime_context|type=="string") and (.policy_state.rationale|type=="string")))' \
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

check_policy_state_consistency

echo "Product API contract checks passed against $API_URL"
