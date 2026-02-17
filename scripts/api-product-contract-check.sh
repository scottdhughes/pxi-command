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

check_plan_brief_coherence() {
  local plan_json
  local brief_json
  local plan_stance
  local brief_stance
  local plan_risk_posture
  local brief_risk_posture

  plan_json=$(curl -sS --max-time 20 "$API_URL/api/plan")
  brief_json=$(curl -sS --max-time 20 "$API_URL/api/brief?scope=market")

  plan_stance=$(echo "$plan_json" | jq -r '.policy_state.stance // ""')
  brief_stance=$(echo "$brief_json" | jq -r '.policy_state.stance // ""')
  plan_risk_posture=$(echo "$plan_json" | jq -r '.policy_state.risk_posture // ""')
  brief_risk_posture=$(echo "$brief_json" | jq -r '.policy_state.risk_posture // ""')

  if [[ -n "$plan_stance" && -n "$brief_stance" && "$plan_stance" != "$brief_stance" ]]; then
    echo "Coherence violation: plan.policy_state.stance ($plan_stance) != brief.policy_state.stance ($brief_stance)"
    echo "plan: $plan_json"
    echo "brief: $brief_json"
    exit 1
  fi

  if [[ -n "$plan_risk_posture" && -n "$brief_risk_posture" && "$plan_risk_posture" != "$brief_risk_posture" ]]; then
    echo "Coherence violation: plan.policy_state.risk_posture ($plan_risk_posture) != brief.policy_state.risk_posture ($brief_risk_posture)"
    echo "plan: $plan_json"
    echo "brief: $brief_json"
    exit 1
  fi
}

check_consistency_gate() {
  local consistency_json
  local state
  local score

  consistency_json=$(curl -sS --max-time 20 "$API_URL/api/market/consistency")
  state=$(echo "$consistency_json" | jq -r '.state // ""')
  score=$(echo "$consistency_json" | jq -r '.score // ""')

  if [[ -z "$state" || -z "$score" ]]; then
    echo "Missing consistency state/score from /api/market/consistency"
    echo "$consistency_json"
    exit 1
  fi

  if [[ "$state" == "FAIL" ]]; then
    echo "Consistency gate failed: state=FAIL"
    echo "$consistency_json"
    exit 1
  fi

  if ! awk "BEGIN { exit !($score >= 90) }"; then
    echo "Consistency gate failed: score $score is below minimum 90"
    echo "$consistency_json"
    exit 1
  fi
}

check_api_json_contract "/api/plan" \
  'type=="object" and (.as_of|type=="string") and (.setup_summary|type=="string") and (.action_now.risk_allocation_target|type=="number") and (.edge_quality.score|type=="number") and (.edge_quality.calibration.quality|type=="string") and (.edge_quality.calibration.sample_size_7d|type=="number") and (.risk_band.d7.sample_size|type=="number") and (.invalidation_rules|type=="array") and ((.policy_state.stance|type=="string") and (.policy_state.risk_posture|type=="string") and (.policy_state.conflict_state|type=="string") and (.policy_state.base_signal|type=="string") and (.policy_state.regime_context|type=="string") and (.policy_state.rationale|type=="string") and (.policy_state.rationale_codes|type=="array")) and ((.uncertainty.headline==null) or (.uncertainty.headline|type=="string")) and (.uncertainty.flags.stale_inputs|type=="boolean") and (.uncertainty.flags.limited_calibration|type=="boolean") and (.uncertainty.flags.limited_scenario_sample|type=="boolean") and (.consistency.score|type=="number") and (.consistency.state|type=="string") and (.consistency.violations|type=="array") and (.trader_playbook.recommended_size_pct.min|type=="number") and (.trader_playbook.recommended_size_pct.target|type=="number") and (.trader_playbook.recommended_size_pct.max|type=="number") and (.trader_playbook.scenarios|type=="array") and ((.trader_playbook.benchmark_follow_through_7d.hit_rate==null) or (.trader_playbook.benchmark_follow_through_7d.hit_rate|type=="number")) and (.trader_playbook.benchmark_follow_through_7d.sample_size|type=="number") and ((.trader_playbook.benchmark_follow_through_7d.unavailable_reason==null) or (.trader_playbook.benchmark_follow_through_7d.unavailable_reason|type=="string"))' \
  "plan"

check_api_json_contract "/api/brief?scope=market" \
  'type=="object" and (.as_of|type=="string") and (.summary|type=="string") and (.regime_delta|type=="string") and (.risk_posture|type=="string") and (.freshness_status.stale_count|type=="number") and (.policy_state.stance|type=="string") and (.policy_state.risk_posture|type=="string") and (.source_plan_as_of|type=="string") and (.contract_version|type=="string") and (.consistency.score|type=="number") and (.consistency.state|type=="string") and ((.degraded_reason==null) or (.degraded_reason|type=="string"))' \
  "brief"

check_api_json_contract "/api/opportunities?horizon=7d&limit=5" \
  'type=="object" and (.as_of|type=="string") and (.horizon|type=="string") and (.items|type=="array") and (all(.items[]?; (.id|type=="string") and (.theme_id|type=="string") and (.direction|type=="string") and (.conviction_score|type=="number") and (.calibration.quality|type=="string") and (.calibration.basis=="conviction_decile") and ((.calibration.unavailable_reason==null) or (.calibration.unavailable_reason|type=="string")) and (.expectancy.sample_size|type=="number") and ((.expectancy.expected_move_pct==null) or (.expectancy.expected_move_pct|type=="number")) and ((.expectancy.max_adverse_move_pct==null) or (.expectancy.max_adverse_move_pct|type=="number")) and ((.expectancy.unavailable_reason==null) or (.expectancy.unavailable_reason|type=="string"))))' \
  "opportunities"

check_api_json_contract "/api/alerts/feed?limit=10" \
  'type=="object" and (.as_of|type=="string") and (.alerts|type=="array") and ((.degraded_reason==null) or (.degraded_reason|type=="string")) and (all(.alerts[]?; (.id|type=="string") and (.event_type|type=="string") and (.severity|type=="string") and (.title|type=="string") and (.body|type=="string") and (.entity_type|type=="string") and (.created_at|type=="string")))' \
  "alerts-feed"

check_api_json_contract "/api/pxi" \
  'type=="object" and (.date|type=="string") and (.score|type=="number") and (.dataFreshness.hasStaleData|type=="boolean") and (.dataFreshness.staleCount|type=="number") and (.dataFreshness.topOffenders|type=="array") and ((.dataFreshness.lastRefreshAtUtc==null) or (.dataFreshness.lastRefreshAtUtc|type=="string")) and (.dataFreshness.nextExpectedRefreshAtUtc|type=="string") and (.dataFreshness.nextExpectedRefreshInMinutes|type=="number")' \
  "pxi"

check_api_json_contract "/api/market/consistency" \
  'type=="object" and (.as_of|type=="string") and (.score|type=="number") and (.state|type=="string") and (.violations|type=="array")' \
  "market-consistency"

check_policy_state_consistency
check_plan_brief_coherence
check_consistency_gate

echo "Product API contract checks passed against $API_URL"
