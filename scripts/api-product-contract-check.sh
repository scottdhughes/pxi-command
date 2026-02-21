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

check_plan_playbook_target_coherence() {
  local plan_json
  local plan_target
  local playbook_target
  local plan_target_pct
  local abs_diff

  plan_json=$(curl -sS --max-time 20 "$API_URL/api/plan")
  plan_target=$(echo "$plan_json" | jq -r '.action_now.risk_allocation_target // ""')
  playbook_target=$(echo "$plan_json" | jq -r '.trader_playbook.recommended_size_pct.target // ""')

  if [[ -z "$plan_target" || -z "$playbook_target" ]]; then
    echo "Missing plan/playbook target values for coherence check"
    echo "$plan_json"
    exit 1
  fi

  plan_target_pct=$(awk "BEGIN { printf \"%.6f\", ($plan_target * 100.0) }")
  abs_diff=$(awk "BEGIN { d=$plan_target_pct-$playbook_target; if (d < 0) d=-d; printf \"%.6f\", d }")

  if ! awk "BEGIN { exit !($abs_diff <= 0.5) }"; then
    echo "Plan/playbook target mismatch: action_now=${plan_target_pct}% vs playbook=${playbook_target}% (diff ${abs_diff})"
    echo "$plan_json"
    exit 1
  fi
}

check_refresh_recency() {
  local pxi_json
  local last_refresh
  local age_hours

  pxi_json=$(curl -sS --max-time 20 "$API_URL/api/pxi")
  last_refresh=$(echo "$pxi_json" | jq -r '.dataFreshness.lastRefreshAtUtc // ""')
  if [[ -z "$last_refresh" ]]; then
    echo "Missing dataFreshness.lastRefreshAtUtc in /api/pxi"
    echo "$pxi_json"
    exit 1
  fi

  age_hours=$(python3 - "$last_refresh" <<'PY'
import datetime
import sys

raw = sys.argv[1].strip()
if not raw:
    print("nan")
    raise SystemExit(0)

value = raw.replace(" ", "T")
if len(value) >= 19 and value[10] == "T":
    suffix = value[19:]
    if suffix == "":
        value = value + "Z"

if value.endswith("Z"):
    value = value[:-1] + "+00:00"

try:
    dt = datetime.datetime.fromisoformat(value)
except Exception:
    print("nan")
    raise SystemExit(0)

if dt.tzinfo is None:
    dt = dt.replace(tzinfo=datetime.timezone.utc)
else:
    dt = dt.astimezone(datetime.timezone.utc)

now = datetime.datetime.now(datetime.timezone.utc)
age = (now - dt).total_seconds() / 3600.0
print(f"{age:.6f}")
PY
)

  if [[ "$age_hours" == "nan" ]]; then
    echo "Unable to parse /api/pxi dataFreshness.lastRefreshAtUtc: $last_refresh"
    exit 1
  fi

  if ! awk "BEGIN { exit !($age_hours <= 36.0) }"; then
    echo "Refresh recency failure: last refresh age ${age_hours}h exceeds 36h threshold"
    echo "$pxi_json"
    exit 1
  fi
}

check_freshness_alert_parity() {
  local plan_json
  local brief_json
  local pxi_json
  local alerts_json
  local plan_stale
  local brief_stale
  local pxi_stale
  local alert_body
  local alert_stale

  plan_json=$(curl -sS --max-time 20 "$API_URL/api/plan")
  brief_json=$(curl -sS --max-time 20 "$API_URL/api/brief?scope=market")
  pxi_json=$(curl -sS --max-time 20 "$API_URL/api/pxi")
  alerts_json=$(curl -sS --max-time 20 "$API_URL/api/alerts/feed?limit=50")

  plan_stale=$(echo "$plan_json" | jq -r '.edge_quality.stale_count // -1')
  brief_stale=$(echo "$brief_json" | jq -r '.freshness_status.stale_count // -1')
  pxi_stale=$(echo "$pxi_json" | jq -r '.dataFreshness.staleCount // -1')
  alert_body=$(echo "$alerts_json" | jq -r '([.alerts[]? | select(.event_type=="freshness_warning")] | first | .body) // ""')

  if [[ "$plan_stale" == "-1" || "$brief_stale" == "-1" || "$pxi_stale" == "-1" ]]; then
    echo "Missing stale counts in plan/brief/pxi parity check"
    echo "plan: $plan_json"
    echo "brief: $brief_json"
    echo "pxi: $pxi_json"
    exit 1
  fi

  if [[ "$plan_stale" != "$brief_stale" || "$plan_stale" != "$pxi_stale" ]]; then
    echo "Stale count mismatch across plan/brief/pxi: plan=$plan_stale brief=$brief_stale pxi=$pxi_stale"
    exit 1
  fi

  if [[ -n "$alert_body" ]]; then
    if [[ "$alert_body" =~ ([0-9]+) ]]; then
      alert_stale="${BASH_REMATCH[1]}"
      if [[ "$alert_stale" != "$plan_stale" ]]; then
        echo "Freshness alert parity mismatch: alert=$alert_stale plan=$plan_stale"
        echo "$alerts_json"
        exit 1
      fi
    else
      echo "Unable to parse stale count from freshness alert body: $alert_body"
      echo "$alerts_json"
      exit 1
    fi
  fi
}

check_degraded_consistency_score() {
  local plan_json
  local degraded
  local score

  plan_json=$(curl -sS --max-time 20 "$API_URL/api/plan")
  degraded=$(echo "$plan_json" | jq -r '(.uncertainty.flags.stale_inputs or .uncertainty.flags.limited_calibration or .uncertainty.flags.limited_scenario_sample)')
  score=$(echo "$plan_json" | jq -r '.consistency.score // 0')

  if [[ "$degraded" == "true" ]] && awk "BEGIN { exit !($score >= 100) }"; then
    echo "Degraded reliability should not score 100 in consistency"
    echo "$plan_json"
    exit 1
  fi
}

check_ml_low_sample_semantics() {
  local ml_json
  local evaluated
  local minimum
  local has_reason

  ml_json=$(curl -sS --max-time 20 "$API_URL/api/ml/accuracy")
  evaluated=$(echo "$ml_json" | jq -r '.coverage.evaluated_count // .evaluated_count // 0')
  minimum=$(echo "$ml_json" | jq -r '.minimum_reliable_sample // 30')
  has_reason=$(echo "$ml_json" | jq -r '(.unavailable_reasons // [] | index("insufficient_sample")) != null')

  if awk "BEGIN { exit !($evaluated < $minimum) }" && [[ "$has_reason" != "true" ]]; then
    echo "ML low-sample semantics failure: evaluated_count=$evaluated minimum_reliable_sample=$minimum but insufficient_sample reason is missing"
    echo "$ml_json"
    exit 1
  fi
}

check_api_json_contract "/api/plan" \
  'type=="object" and (.as_of|type=="string") and (.setup_summary|type=="string") and (.action_now.risk_allocation_target|type=="number") and (.action_now.raw_signal_allocation_target|type=="number") and (.action_now.risk_allocation_basis|type=="string") and (.edge_quality.score|type=="number") and (.edge_quality.calibration.quality|type=="string") and (.edge_quality.calibration.sample_size_7d|type=="number") and (.risk_band.d7.sample_size|type=="number") and (.invalidation_rules|type=="array") and ((.policy_state.stance|type=="string") and (.policy_state.risk_posture|type=="string") and (.policy_state.conflict_state|type=="string") and (.policy_state.base_signal|type=="string") and (.policy_state.regime_context|type=="string") and (.policy_state.rationale|type=="string") and (.policy_state.rationale_codes|type=="array")) and ((.uncertainty.headline==null) or (.uncertainty.headline|type=="string")) and (.uncertainty.flags.stale_inputs|type=="boolean") and (.uncertainty.flags.limited_calibration|type=="boolean") and (.uncertainty.flags.limited_scenario_sample|type=="boolean") and (.consistency.score|type=="number") and (.consistency.state|type=="string") and (.consistency.violations|type=="array") and (.consistency.components.base_score|type=="number") and (.consistency.components.structural_penalty|type=="number") and (.consistency.components.reliability_penalty|type=="number") and (.trader_playbook.recommended_size_pct.min|type=="number") and (.trader_playbook.recommended_size_pct.target|type=="number") and (.trader_playbook.recommended_size_pct.max|type=="number") and (.trader_playbook.scenarios|type=="array") and ((.trader_playbook.benchmark_follow_through_7d.hit_rate==null) or (.trader_playbook.benchmark_follow_through_7d.hit_rate|type=="number")) and (.trader_playbook.benchmark_follow_through_7d.sample_size|type=="number") and ((.trader_playbook.benchmark_follow_through_7d.unavailable_reason==null) or (.trader_playbook.benchmark_follow_through_7d.unavailable_reason|type=="string"))' \
  "plan"

check_api_json_contract "/api/brief?scope=market" \
  'type=="object" and (.as_of|type=="string") and (.summary|type=="string") and (.regime_delta|type=="string") and (.risk_posture|type=="string") and (.freshness_status.stale_count|type=="number") and (.policy_state.stance|type=="string") and (.policy_state.risk_posture|type=="string") and (.source_plan_as_of|type=="string") and (.contract_version|type=="string") and (.consistency.score|type=="number") and (.consistency.state|type=="string") and ((.degraded_reason==null) or (.degraded_reason|type=="string"))' \
  "brief"

check_api_json_contract "/api/opportunities?horizon=7d&limit=5" \
  'type=="object" and (.as_of|type=="string") and (.horizon|type=="string") and (.items|type=="array") and (all(.items[]?; (.id|type=="string") and (.theme_id|type=="string") and (.direction|type=="string") and (.conviction_score|type=="number") and (.calibration.quality|type=="string") and (.calibration.basis=="conviction_decile") and ((.calibration.window==null) or (.calibration.window|type=="string")) and ((.calibration.unavailable_reason==null) or (.calibration.unavailable_reason|type=="string")) and (.expectancy.sample_size|type=="number") and (.expectancy.basis|type=="string") and (.expectancy.quality|type=="string") and ((.expectancy.expected_move_pct==null) or (.expectancy.expected_move_pct|type=="number")) and ((.expectancy.max_adverse_move_pct==null) or (.expectancy.max_adverse_move_pct|type=="number")) and ((.expectancy.unavailable_reason==null) or (.expectancy.unavailable_reason|type=="string"))))' \
  "opportunities"

check_api_json_contract "/api/alerts/feed?limit=10" \
  'type=="object" and (.as_of|type=="string") and (.alerts|type=="array") and ((.degraded_reason==null) or (.degraded_reason|type=="string")) and (all(.alerts[]?; (.id|type=="string") and (.event_type|type=="string") and (.severity|type=="string") and (.title|type=="string") and (.body|type=="string") and (.entity_type|type=="string") and (.created_at|type=="string")))' \
  "alerts-feed"

check_api_json_contract "/api/pxi" \
  'type=="object" and (.date|type=="string") and (.score|type=="number") and (.dataFreshness.hasStaleData|type=="boolean") and (.dataFreshness.staleCount|type=="number") and (.dataFreshness.topOffenders|type=="array") and ((.dataFreshness.lastRefreshAtUtc==null) or (.dataFreshness.lastRefreshAtUtc|type=="string")) and (.dataFreshness.lastRefreshSource|type=="string") and (.dataFreshness.nextExpectedRefreshAtUtc|type=="string") and (.dataFreshness.nextExpectedRefreshInMinutes|type=="number")' \
  "pxi"

check_api_json_contract "/api/market/consistency" \
  'type=="object" and (.as_of|type=="string") and (.score|type=="number") and (.state|type=="string") and (.violations|type=="array") and (.components.base_score|type=="number") and (.components.structural_penalty|type=="number") and (.components.reliability_penalty|type=="number")' \
  "market-consistency"

check_api_json_contract "/api/ml/accuracy" \
  'type=="object" and (.as_of|type=="string") and (.coverage.total_predictions|type=="number") and (.coverage.evaluated_count|type=="number") and (.coverage.pending_count|type=="number") and (.coverage_quality|type=="string") and (.minimum_reliable_sample|type=="number") and (.unavailable_reasons|type=="array")' \
  "ml-accuracy"

check_api_json_contract "/api/accuracy" \
  'type=="object" and (.as_of|type=="string") and (.coverage.total_predictions|type=="number") and (.coverage.evaluated_count|type=="number") and (.coverage.pending_count|type=="number") and (.coverage_quality|type=="string") and (.minimum_reliable_sample|type=="number") and (.unavailable_reasons|type=="array")' \
  "accuracy"

check_policy_state_consistency
check_plan_brief_coherence
check_consistency_gate
check_plan_playbook_target_coherence
check_refresh_recency
check_freshness_alert_parity
check_degraded_consistency_score
check_ml_low_sample_semantics

echo "Product API contract checks passed against $API_URL"
