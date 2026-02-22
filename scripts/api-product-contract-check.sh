#!/usr/bin/env bash
set -euo pipefail

API_URL="${1:-${API_URL:-https://api.pxicommand.com}}"
EDGE_DIAGNOSTICS_REQUIRED="${EDGE_DIAGNOSTICS_REQUIRED:-0}"
OPPORTUNITY_TTL_GRACE_SECONDS="${OPPORTUNITY_TTL_GRACE_SECONDS:-5400}"
UTILITY_FUNNEL_REQUIRED="${UTILITY_FUNNEL_REQUIRED:-0}"
DECISION_GRADE_REQUIRED="${DECISION_GRADE_REQUIRED:-0}"
DECISION_IMPACT_REQUIRED="${DECISION_IMPACT_REQUIRED:-0}"
GO_LIVE_READINESS_REQUIRED="${GO_LIVE_READINESS_REQUIRED:-0}"
UTILITY_FUNNEL_LIVE=0
DECISION_GRADE_LIVE=0
DECISION_IMPACT_LIVE=0
GO_LIVE_READINESS_LIVE=0

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

edge_diagnostics_available() {
  local code
  code=$(curl -sS -o "$TMP_BODY" -w "%{http_code}" --max-time 20 "$API_URL/api/diagnostics/edge?horizon=all")
  if [[ "$code" == "200" ]]; then
    return 0
  fi

  if [[ "$code" == "404" ]]; then
    if [[ "$EDGE_DIAGNOSTICS_REQUIRED" == "1" ]]; then
      echo "Edge diagnostics endpoint is required but returned 404"
      exit 1
    fi
    echo "Skipping edge diagnostics checks: /api/diagnostics/edge is not live yet."
    return 1
  fi

  echo "Unexpected status for /api/diagnostics/edge: $code"
  cat "$TMP_BODY" || true
  exit 1
}

utility_funnel_available() {
  local code
  code=$(curl -sS -o "$TMP_BODY" -w "%{http_code}" --max-time 20 "$API_URL/api/ops/utility-funnel?window=7")
  if [[ "$code" == "200" ]]; then
    return 0
  fi

  if [[ "$code" == "404" ]]; then
    if [[ "$UTILITY_FUNNEL_REQUIRED" == "1" ]]; then
      echo "Utility funnel endpoint is required but returned 404"
      exit 1
    fi
    echo "Skipping utility funnel checks: /api/ops/utility-funnel is not live yet."
    return 1
  fi

  echo "Unexpected status for /api/ops/utility-funnel: $code"
  cat "$TMP_BODY" || true
  exit 1
}

decision_grade_available() {
  local code
  code=$(curl -sS -o "$TMP_BODY" -w "%{http_code}" --max-time 20 "$API_URL/api/ops/decision-grade?window=30")
  if [[ "$code" == "200" ]]; then
    return 0
  fi

  if [[ "$code" == "404" ]]; then
    if [[ "$DECISION_GRADE_REQUIRED" == "1" ]]; then
      echo "Decision-grade endpoint is required but returned 404"
      exit 1
    fi
    echo "Skipping decision-grade checks: /api/ops/decision-grade is not live yet."
    return 1
  fi

  echo "Unexpected status for /api/ops/decision-grade: $code"
  cat "$TMP_BODY" || true
  exit 1
}

decision_impact_available() {
  local code
  code=$(curl -sS -o "$TMP_BODY" -w "%{http_code}" --max-time 20 "$API_URL/api/decision-impact?horizon=7d&scope=market&window=30")
  if [[ "$code" == "200" ]]; then
    return 0
  fi

  if [[ "$code" == "404" ]]; then
    if [[ "$DECISION_IMPACT_REQUIRED" == "1" ]]; then
      echo "Decision-impact endpoint is required but returned 404"
      exit 1
    fi
    echo "Skipping decision-impact checks: /api/decision-impact is not live yet."
    return 1
  fi

  echo "Unexpected status for /api/decision-impact: $code"
  cat "$TMP_BODY" || true
  exit 1
}

go_live_readiness_available() {
  local code
  code=$(curl -sS -o "$TMP_BODY" -w "%{http_code}" --max-time 20 "$API_URL/api/ops/go-live-readiness?window=30")
  if [[ "$code" == "200" ]]; then
    return 0
  fi

  if [[ "$code" == "404" ]]; then
    if [[ "$GO_LIVE_READINESS_REQUIRED" == "1" ]]; then
      echo "Go-live readiness endpoint is required but returned 404"
      exit 1
    fi
    echo "Skipping go-live readiness checks: /api/ops/go-live-readiness is not live yet."
    return 1
  fi

  echo "Unexpected status for /api/ops/go-live-readiness: $code"
  cat "$TMP_BODY" || true
  exit 1
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

check_plan_actionability_contract() {
  local plan_json
  local actionability_state
  local eligible_count
  local edge_label
  local has_override
  local cross_horizon_state

  plan_json=$(curl -sS --max-time 20 "$API_URL/api/plan")
  actionability_state=$(echo "$plan_json" | jq -r '.actionability_state // ""')
  edge_label=$(echo "$plan_json" | jq -r '.edge_quality.label // ""')
  eligible_count=$(echo "$plan_json" | jq -r '.opportunity_ref.eligible_count // -1')
  has_override=$(echo "$plan_json" | jq -r '((.actionability_reason_codes // []) | index("high_edge_override_no_eligible")) != null')
  cross_horizon_state=$(echo "$plan_json" | jq -r '.cross_horizon.state // ""')

  case "$actionability_state" in
    ACTIONABLE|WATCH|NO_ACTION) ;;
    *)
      echo "Invalid /api/plan actionability_state: $actionability_state"
      echo "$plan_json"
      exit 1
      ;;
  esac

  if [[ "$eligible_count" != "-1" && "$eligible_count" == "0" && "$actionability_state" != "NO_ACTION" ]]; then
    echo "Plan actionability contradiction: eligible_count=0 but actionability_state is $actionability_state"
    echo "$plan_json"
    exit 1
  fi

  if [[ "$edge_label" == "HIGH" && "$eligible_count" == "0" && "$has_override" != "true" ]]; then
    echo "HIGH edge with zero eligible opportunities requires explicit high_edge_override_no_eligible reason code"
    echo "$plan_json"
    exit 1
  fi

  if [[ "$cross_horizon_state" == "CONFLICT" && "$actionability_state" == "ACTIONABLE" ]]; then
    echo "Cross-horizon conflict cannot publish ACTIONABLE state"
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

check_alert_aggregate_parity() {
  local plan_json
  local alerts_json
  local warning_plan
  local critical_plan
  local warning_actual
  local critical_actual
  local since_utc

  since_utc=$(python3 - <<'PY'
import datetime
now = datetime.datetime.now(datetime.timezone.utc)
cutoff = now - datetime.timedelta(hours=24)
print(cutoff.isoformat().replace("+00:00", "Z"))
PY
)

  plan_json=$(curl -sS --max-time 20 "$API_URL/api/plan")
  alerts_json=$(curl -sS --max-time 20 "$API_URL/api/alerts/feed?limit=200&since=$since_utc")

  warning_plan=$(echo "$plan_json" | jq -r '.alerts_ref.warning_count_24h // 0')
  critical_plan=$(echo "$plan_json" | jq -r '.alerts_ref.critical_count_24h // 0')
  warning_actual=$(echo "$alerts_json" | jq -r '[.alerts[]? | select(.severity=="warning")] | length')
  critical_actual=$(echo "$alerts_json" | jq -r '[.alerts[]? | select(.severity=="critical")] | length')

  if [[ "$warning_plan" != "$warning_actual" || "$critical_plan" != "$critical_actual" ]]; then
    echo "24h alert aggregate mismatch: plan(warning=$warning_plan critical=$critical_plan) vs feed(warning=$warning_actual critical=$critical_actual)"
    echo "plan: $plan_json"
    echo "alerts: $alerts_json"
    exit 1
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

check_opportunity_coherence_contract() {
  local opp_json
  local contradictions
  local bad_contract
  local degraded_reason
  local suppressed_count
  local item_count
  local cta_enabled
  local cta_reason_count
  local actionability_state
  local quality_filter_rate
  local coherence_fail_rate
  local ttl_state
  local data_age_seconds
  local overdue_seconds
  local next_expected_refresh_at
  local has_overdue_reason
  local has_unknown_reason

  opp_json=$(curl -sS --max-time 20 "$API_URL/api/opportunities?horizon=7d&limit=50")

  contradictions=$(echo "$opp_json" | jq -r '
    any(.items[]?;
      (.direction == "bullish" and (
        ((.calibration.probability_correct_direction != null) and (.calibration.probability_correct_direction < 0.5)) or
        ((.expectancy.expected_move_pct != null) and (.expectancy.expected_move_pct <= 0))
      )) or
      (.direction == "bearish" and (
        ((.calibration.probability_correct_direction != null) and (.calibration.probability_correct_direction < 0.5)) or
        ((.expectancy.expected_move_pct != null) and (.expectancy.expected_move_pct >= 0))
      )) or
      (.direction == "neutral")
    )
  ')

  if [[ "$contradictions" == "true" ]]; then
    echo "Opportunity coherence contradiction found in published feed"
    echo "$opp_json"
    exit 1
  fi

  bad_contract=$(echo "$opp_json" | jq -r '
    any(.items[]?;
      (.eligibility.passed != true) or
      (.eligibility.failed_checks | length != 0) or
      (.decision_contract.coherent != true)
    )
  ')
  if [[ "$bad_contract" == "true" ]]; then
    echo "Published opportunity includes failed eligibility/decision contract"
    echo "$opp_json"
    exit 1
  fi

  degraded_reason=$(echo "$opp_json" | jq -r '.degraded_reason // ""')
  suppressed_count=$(echo "$opp_json" | jq -r '.suppressed_count // 0')
  item_count=$(echo "$opp_json" | jq -r '.items | length')
  cta_enabled=$(echo "$opp_json" | jq -r '.cta_enabled // false')
  cta_reason_count=$(echo "$opp_json" | jq -r '(.cta_disabled_reasons // []) | length')
  actionability_state=$(echo "$opp_json" | jq -r '.actionability_state // ""')
  quality_filter_rate=$(echo "$opp_json" | jq -r '.quality_filter_rate // -1')
  coherence_fail_rate=$(echo "$opp_json" | jq -r '.coherence_fail_rate // -1')
  ttl_state=$(echo "$opp_json" | jq -r '.ttl_state // ""')
  data_age_seconds=$(echo "$opp_json" | jq -r '.data_age_seconds // "null"')
  overdue_seconds=$(echo "$opp_json" | jq -r '.overdue_seconds // "null"')
  next_expected_refresh_at=$(echo "$opp_json" | jq -r '.next_expected_refresh_at // ""')
  has_overdue_reason=$(echo "$opp_json" | jq -r '((.cta_disabled_reasons // []) | index("refresh_ttl_overdue")) != null')
  has_unknown_reason=$(echo "$opp_json" | jq -r '((.cta_disabled_reasons // []) | index("refresh_ttl_unknown")) != null')

  if [[ -n "$degraded_reason" ]]; then
    case "$degraded_reason" in
      quality_filtered|coherence_gate_failed|suppressed_data_quality|refresh_ttl_overdue|refresh_ttl_unknown|feature_disabled|migration_guard_failed|snapshot_unavailable|snapshot_rebuilt)
        ;;
      *)
        echo "Invalid opportunities degraded_reason: $degraded_reason"
        echo "$opp_json"
        exit 1
        ;;
    esac
  fi

  if [[ "$degraded_reason" == "suppressed_data_quality" && "$item_count" != "0" ]]; then
    echo "suppressed_data_quality must return an empty items array"
    echo "$opp_json"
    exit 1
  fi

  if [[ "$degraded_reason" == "coherence_gate_failed" || "$degraded_reason" == "quality_filtered" || "$degraded_reason" == "suppressed_data_quality" ]]; then
    if ! awk "BEGIN { exit !($suppressed_count > 0) }"; then
      echo "degraded_reason=$degraded_reason requires suppressed_count > 0"
      echo "$opp_json"
      exit 1
    fi
  fi

  case "$actionability_state" in
    ACTIONABLE|WATCH|NO_ACTION) ;;
    *)
      echo "Invalid actionability_state in opportunities response: $actionability_state"
      echo "$opp_json"
      exit 1
      ;;
  esac

  if [[ "$item_count" == "0" && "$actionability_state" != "NO_ACTION" ]]; then
    echo "Expected actionability_state=NO_ACTION when no eligible opportunities are published"
    echo "$opp_json"
    exit 1
  fi

  if [[ "$cta_enabled" == "true" && "$cta_reason_count" != "0" ]]; then
    echo "cta_enabled=true but cta_disabled_reasons is non-empty"
    echo "$opp_json"
    exit 1
  fi

  if [[ "$cta_enabled" != "true" && "$cta_reason_count" == "0" ]]; then
    echo "cta_enabled=false requires at least one cta_disabled_reason"
    echo "$opp_json"
    exit 1
  fi

  if ! awk "BEGIN { exit !($quality_filter_rate >= 0 && $quality_filter_rate <= 1) }"; then
    echo "quality_filter_rate out of bounds: $quality_filter_rate"
    echo "$opp_json"
    exit 1
  fi

  if ! awk "BEGIN { exit !($coherence_fail_rate >= 0 && $coherence_fail_rate <= 1) }"; then
    echo "coherence_fail_rate out of bounds: $coherence_fail_rate"
    echo "$opp_json"
    exit 1
  fi

  case "$ttl_state" in
    fresh|stale|overdue|unknown) ;;
    *)
      echo "Invalid opportunities ttl_state: $ttl_state"
      echo "$opp_json"
      exit 1
      ;;
  esac

  if [[ "$ttl_state" != "unknown" ]]; then
    if [[ -z "$next_expected_refresh_at" ]]; then
      echo "ttl_state=$ttl_state requires next_expected_refresh_at"
      echo "$opp_json"
      exit 1
    fi
    if [[ "$data_age_seconds" == "null" ]] || ! awk "BEGIN { exit !($data_age_seconds >= 0) }"; then
      echo "ttl_state=$ttl_state requires non-negative data_age_seconds"
      echo "$opp_json"
      exit 1
    fi
  fi

  if [[ "$ttl_state" == "fresh" ]]; then
    if [[ "$overdue_seconds" == "null" ]] || ! awk "BEGIN { exit !($overdue_seconds == 0) }"; then
      echo "ttl_state=fresh requires overdue_seconds=0"
      echo "$opp_json"
      exit 1
    fi
  elif [[ "$ttl_state" == "stale" ]]; then
    if [[ "$overdue_seconds" == "null" ]] || ! awk "BEGIN { exit !($overdue_seconds > 0 && $overdue_seconds <= $OPPORTUNITY_TTL_GRACE_SECONDS) }"; then
      echo "ttl_state=stale requires overdue_seconds in (0, ${OPPORTUNITY_TTL_GRACE_SECONDS}]"
      echo "$opp_json"
      exit 1
    fi
  elif [[ "$ttl_state" == "overdue" ]]; then
    if [[ "$overdue_seconds" == "null" ]] || ! awk "BEGIN { exit !($overdue_seconds > $OPPORTUNITY_TTL_GRACE_SECONDS) }"; then
      echo "ttl_state=overdue requires overdue_seconds>${OPPORTUNITY_TTL_GRACE_SECONDS}"
      echo "$opp_json"
      exit 1
    fi
    if [[ "$cta_enabled" == "true" ]]; then
      echo "ttl_state=overdue must disable CTA"
      echo "$opp_json"
      exit 1
    fi
    if [[ "$has_overdue_reason" != "true" ]]; then
      echo "ttl_state=overdue requires cta_disabled_reasons to include refresh_ttl_overdue"
      echo "$opp_json"
      exit 1
    fi
  else
    if [[ "$cta_enabled" == "true" ]]; then
      echo "ttl_state=unknown must disable CTA"
      echo "$opp_json"
      exit 1
    fi
    if [[ "$has_unknown_reason" != "true" ]]; then
      echo "ttl_state=unknown requires cta_disabled_reasons to include refresh_ttl_unknown"
      echo "$opp_json"
      exit 1
    fi
  fi
}

check_edge_diagnostics_contract() {
  local edge_json
  local promotion_pass
  local malformed_windows
  local sentinel_invariant_broken
  local lower_bound_invariant_broken

  edge_json=$(curl -sS --max-time 20 "$API_URL/api/diagnostics/edge?horizon=all")
  promotion_pass=$(echo "$edge_json" | jq -r '.promotion_gate.pass // false')

  if [[ "$promotion_pass" != "true" ]]; then
    echo "Edge diagnostics promotion gate failed"
    echo "$edge_json"
    exit 1
  fi

  malformed_windows=$(echo "$edge_json" | jq -r '
    any(.windows[]?;
      ((.horizon != "7d") and (.horizon != "30d")) or
      (.sample_size | type != "number") or
      ((.model_direction_accuracy != null) and ((.model_direction_accuracy < 0) or (.model_direction_accuracy > 1))) or
      ((.baseline_direction_accuracy != null) and ((.baseline_direction_accuracy < 0) or (.baseline_direction_accuracy > 1))) or
      ((.uplift_ci95_low != null and .uplift_ci95_high != null) and (.uplift_ci95_low > .uplift_ci95_high)) or
      (.leakage_sentinel.pass | type != "boolean") or
      (.leakage_sentinel.violation_count | type != "number") or
      (.leakage_sentinel.reasons | type != "array")
    )
  ')
  if [[ "$malformed_windows" == "true" ]]; then
    echo "Malformed edge diagnostics window payload"
    echo "$edge_json"
    exit 1
  fi

  sentinel_invariant_broken=$(echo "$edge_json" | jq -r '
    any(.windows[]?;
      ((.leakage_sentinel.pass == true) and ((.leakage_sentinel.violation_count != 0) or ((.leakage_sentinel.reasons | length) != 0))) or
      ((.leakage_sentinel.pass == false) and ((.leakage_sentinel.violation_count == 0) or ((.leakage_sentinel.reasons | length) == 0)))
    )
  ')
  if [[ "$sentinel_invariant_broken" == "true" ]]; then
    echo "Leakage sentinel invariants failed in edge diagnostics payload"
    echo "$edge_json"
    exit 1
  fi

  lower_bound_invariant_broken=$(echo "$edge_json" | jq -r '
    any(.windows[]?;
      (.lower_bound_positive == true and (.uplift_ci95_low == null or .uplift_ci95_low <= 0)) or
      (.lower_bound_positive == false and (.uplift_ci95_low != null and .uplift_ci95_low > 0))
    )
  ')
  if [[ "$lower_bound_invariant_broken" == "true" ]]; then
    echo "lower_bound_positive invariant failed in edge diagnostics payload"
    echo "$edge_json"
    exit 1
  fi
}

check_utility_funnel_semantics() {
  local utility_json
  local sessions
  local decision_total
  local no_action_views
  local unlock_views
  local unlock_coverage
  local cta_clicks
  local actionable_sessions
  local cta_action_rate

  utility_json=$(curl -sS --max-time 20 "$API_URL/api/ops/utility-funnel?window=7")

  sessions=$(echo "$utility_json" | jq -r '.funnel.unique_sessions // -1')
  decision_total=$(echo "$utility_json" | jq -r '.funnel.decision_events_total // -1')
  no_action_views=$(echo "$utility_json" | jq -r '.funnel.decision_no_action_views // -1')
  unlock_views=$(echo "$utility_json" | jq -r '.funnel.no_action_unlock_views // -1')
  unlock_coverage=$(echo "$utility_json" | jq -r '.funnel.no_action_unlock_coverage_pct // -1')
  cta_clicks=$(echo "$utility_json" | jq -r '.funnel.cta_action_clicks // -1')
  actionable_sessions=$(echo "$utility_json" | jq -r '.funnel.actionable_sessions // -1')
  cta_action_rate=$(echo "$utility_json" | jq -r '.funnel.cta_action_rate_pct // -1')

  if ! awk "BEGIN { exit !($sessions >= 0 && $decision_total >= 0 && $no_action_views >= 0 && $unlock_views >= 0 && $cta_clicks >= 0 && $actionable_sessions >= 0) }"; then
    echo "Utility funnel counts must be non-negative"
    echo "$utility_json"
    exit 1
  fi

  if ! awk "BEGIN { exit !($unlock_coverage >= 0 && $unlock_coverage <= 100) }"; then
    echo "Utility funnel coverage must be within [0,100]"
    echo "$utility_json"
    exit 1
  fi

  if ! awk "BEGIN { exit !($cta_action_rate >= 0 && $cta_action_rate <= 100) }"; then
    echo "Utility funnel CTA action rate must be within [0,100]"
    echo "$utility_json"
    exit 1
  fi
}

check_cta_action_event_contract() {
  local code
  local accepted_type
  local ignored_reason
  local payload

  payload='{"session_id":"ux_contract_cta","event_type":"cta_action_click","route":"/opportunities","actionability_state":"ACTIONABLE","metadata":{"source":"contract_check","as_of":"2026-02-22T00:00:00.000Z","horizon":"7d","scope":"market_theme"}}'
  code=$(curl -sS -o "$TMP_BODY" -w "%{http_code}" --max-time 20 -X POST "$API_URL/api/metrics/utility-event" \
    -H "Content-Type: application/json" \
    -d "$payload")

  if [[ "$code" != "200" ]]; then
    echo "Expected 200 for cta_action_click utility event but got $code"
    cat "$TMP_BODY"
    exit 1
  fi

  accepted_type=$(jq -r '.accepted.event_type // ""' "$TMP_BODY")
  ignored_reason=$(jq -r '.ignored_reason // ""' "$TMP_BODY")
  if [[ "$accepted_type" != "cta_action_click" && "$ignored_reason" != "cta_intent_tracking_disabled" ]]; then
    echo "cta_action_click was neither accepted nor explicitly ignored due to feature flag"
    cat "$TMP_BODY"
    exit 1
  fi
}

check_decision_grade_semantics() {
  local grade_json
  local score
  local grade
  local go_live_ready
  local window_days
  local bad_status
  local over_suppression
  local conflict_rate
  local unlock_coverage
  local blockers_len
  local enforce_ready
  local enforce_breaches_len

  grade_json=$(curl -sS --max-time 20 "$API_URL/api/ops/decision-grade?window=30")
  score=$(echo "$grade_json" | jq -r '.score // -1')
  grade=$(echo "$grade_json" | jq -r '.grade // ""')
  go_live_ready=$(echo "$grade_json" | jq -r '.go_live_ready // false')
  window_days=$(echo "$grade_json" | jq -r '.window_days // -1')
  over_suppression=$(echo "$grade_json" | jq -r '.components.opportunity_hygiene.over_suppression_rate_pct // -1')
  conflict_rate=$(echo "$grade_json" | jq -r '.components.opportunity_hygiene.cross_horizon_conflict_rate_pct // -1')
  unlock_coverage=$(echo "$grade_json" | jq -r '.components.utility.no_action_unlock_coverage_pct // -1')
  blockers_len=$(echo "$grade_json" | jq -r '(.go_live_blockers // [] | length)')
  enforce_ready=$(echo "$grade_json" | jq -r '.readiness.decision_impact_enforce_ready // false')
  enforce_breaches_len=$(echo "$grade_json" | jq -r '(.readiness.decision_impact_breaches // [] | length)')

  if ! awk "BEGIN { exit !($score >= 0 && $score <= 100) }"; then
    echo "Decision-grade score must be in [0,100]"
    echo "$grade_json"
    exit 1
  fi

  if [[ "$window_days" != "30" ]]; then
    echo "Decision-grade window_days should echo requested value 30"
    echo "$grade_json"
    exit 1
  fi

  case "$grade" in
    GREEN|YELLOW|RED) ;;
    *)
      echo "Invalid decision-grade grade: $grade"
      echo "$grade_json"
      exit 1
      ;;
  esac

  if awk "BEGIN { exit !($score >= 85) }" && [[ "$grade" != "GREEN" ]]; then
    echo "Decision-grade threshold mismatch: score>=85 must map to GREEN"
    echo "$grade_json"
    exit 1
  fi

  if awk "BEGIN { exit !($score >= 70 && $score < 85) }" && [[ "$grade" != "YELLOW" ]]; then
    echo "Decision-grade threshold mismatch: score in [70,85) must map to YELLOW"
    echo "$grade_json"
    exit 1
  fi

  if awk "BEGIN { exit !($score < 70) }" && [[ "$grade" != "RED" ]]; then
    echo "Decision-grade threshold mismatch: score<70 must map to RED"
    echo "$grade_json"
    exit 1
  fi

  if [[ "$go_live_ready" == "true" && "$grade" != "GREEN" ]]; then
    echo "go_live_ready=true requires GREEN grade"
    echo "$grade_json"
    exit 1
  fi

  if [[ "$go_live_ready" == "true" && "$blockers_len" != "0" ]]; then
    echo "go_live_ready=true cannot have go-live blockers"
    echo "$grade_json"
    exit 1
  fi

  if [[ "$go_live_ready" == "false" && "$blockers_len" == "0" ]]; then
    echo "go_live_ready=false requires at least one go-live blocker"
    echo "$grade_json"
    exit 1
  fi

  if [[ "$enforce_ready" == "true" && "$enforce_breaches_len" != "0" ]]; then
    echo "decision impact readiness cannot be enforce-ready with active breaches"
    echo "$grade_json"
    exit 1
  fi

  bad_status=$(echo "$grade_json" | jq -r '
    any([
      .components.freshness.status,
      .components.consistency.status,
      .components.calibration.status,
      .components.edge.status,
      .components.opportunity_hygiene.status,
      .components.utility.status
    ][]; . != "pass" and . != "watch" and . != "fail" and . != "insufficient")
  ')
  if [[ "$bad_status" == "true" ]]; then
    echo "Decision-grade includes invalid component status"
    echo "$grade_json"
    exit 1
  fi

  if ! awk "BEGIN { exit !($over_suppression >= 0 && $over_suppression <= 100) }"; then
    echo "Decision-grade over_suppression_rate_pct must be in [0,100]"
    echo "$grade_json"
    exit 1
  fi

  if ! awk "BEGIN { exit !($conflict_rate >= 0 && $conflict_rate <= 100) }"; then
    echo "Decision-grade cross_horizon_conflict_rate_pct must be in [0,100]"
    echo "$grade_json"
    exit 1
  fi

  if ! awk "BEGIN { exit !($unlock_coverage >= 0 && $unlock_coverage <= 100) }"; then
    echo "Decision-grade utility coverage must be in [0,100]"
    echo "$grade_json"
    exit 1
  fi
}

check_go_live_readiness_semantics() {
  local readiness_json
  local score_window_days
  local go_live_ready
  local blockers_len
  local breach_len
  local enforce_ready
  local grade_value

  readiness_json=$(curl -sS --max-time 20 "$API_URL/api/ops/go-live-readiness?window=30")
  score_window_days=$(echo "$readiness_json" | jq -r '.score_window_days // -1')
  go_live_ready=$(echo "$readiness_json" | jq -r '.go_live_ready // false')
  blockers_len=$(echo "$readiness_json" | jq -r '(.blockers // [] | length)')
  breach_len=$(echo "$readiness_json" | jq -r '(.readiness.decision_impact_breaches // [] | length)')
  enforce_ready=$(echo "$readiness_json" | jq -r '.readiness.decision_impact_enforce_ready // false')
  grade_value=$(echo "$readiness_json" | jq -r '.grade.grade // ""')

  if [[ "$score_window_days" != "30" ]]; then
    echo "Go-live readiness score window should be fixed at 30 days"
    echo "$readiness_json"
    exit 1
  fi

  if [[ "$go_live_ready" == "true" && "$blockers_len" != "0" ]]; then
    echo "go_live_ready=true cannot have blockers"
    echo "$readiness_json"
    exit 1
  fi

  if [[ "$go_live_ready" == "true" && "$grade_value" != "GREEN" ]]; then
    echo "go_live_readiness requires GREEN grade"
    echo "$readiness_json"
    exit 1
  fi

  if [[ "$enforce_ready" == "true" && "$breach_len" != "0" ]]; then
    echo "decision impact enforce ready cannot have active breaches"
    echo "$readiness_json"
    exit 1
  fi
}

check_decision_impact_semantics() {
  local market_json
  local theme_json
  local ops_json
  local market_valid
  local theme_valid
  local ops_valid

  market_json=$(curl -sS --max-time 20 "$API_URL/api/decision-impact?horizon=7d&scope=market&window=30")
  theme_json=$(curl -sS --max-time 20 "$API_URL/api/decision-impact?horizon=7d&scope=theme&window=30&limit=10")
  ops_json=$(curl -sS --max-time 20 "$API_URL/api/ops/decision-impact?window=30")

  market_valid=$(echo "$market_json" | jq -r '
    (.scope == "market") and
    (.horizon == "7d") and
    (.window_days == 30) and
    (.outcome_basis == "spy_forward_proxy") and
    (.themes | type == "array" and length == 0) and
    (.market.sample_size | type == "number" and . >= 0) and
    (.market.hit_rate | type == "number" and . >= 0 and . <= 1) and
    (.market.win_rate | type == "number" and . >= 0 and . <= 1) and
    (.coverage.coverage_ratio | type == "number" and . >= 0 and . <= 1) and
    ((.market.quality_band == "ROBUST") or (.market.quality_band == "LIMITED") or (.market.quality_band == "INSUFFICIENT"))
  ')
  if [[ "$market_valid" != "true" ]]; then
    echo "Decision-impact market semantics failure"
    echo "$market_json"
    exit 1
  fi

  theme_valid=$(echo "$theme_json" | jq -r '
    (.scope == "theme") and
    (.horizon == "7d") and
    (.window_days == 30) and
    ((.outcome_basis == "theme_proxy_blend") or (.outcome_basis == "spy_forward_proxy")) and
    (.themes | type == "array" and length <= 10) and
    (all(.themes[]?;
      (.theme_id | type == "string") and
      (.theme_name | type == "string") and
      (.sample_size | type == "number" and . >= 0) and
      (.hit_rate | type == "number" and . >= 0 and . <= 1) and
      (.win_rate | type == "number" and . >= 0 and . <= 1) and
      ((.quality_band == "ROBUST") or (.quality_band == "LIMITED") or (.quality_band == "INSUFFICIENT"))
    ))
  ')
  if [[ "$theme_valid" != "true" ]]; then
    echo "Decision-impact theme semantics failure"
    echo "$theme_json"
    exit 1
  fi

  ops_valid=$(echo "$ops_json" | jq -r '
    (.window_days == 30) and
    (.market_7d.hit_rate | type == "number" and . >= 0 and . <= 1) and
    (.market_30d.hit_rate | type == "number" and . >= 0 and . <= 1) and
    (.utility_attribution.cta_action_rate_pct | type == "number" and . >= 0 and . <= 100) and
    (.observe_mode.enabled == true) and
    (.observe_mode.breach_count | type == "number" and . >= 0) and
    (.observe_mode.thresholds.market_7d_hit_rate_min | type == "number") and
    (.observe_mode.thresholds.market_30d_hit_rate_min | type == "number") and
    (.observe_mode.thresholds.market_7d_avg_signed_return_min | type == "number") and
    (.observe_mode.thresholds.market_30d_avg_signed_return_min | type == "number") and
    (.observe_mode.thresholds.cta_action_rate_pct_min | type == "number")
  ')
  if [[ "$ops_valid" != "true" ]]; then
    echo "Ops decision-impact semantics failure"
    echo "$ops_json"
    exit 1
  fi
}

check_api_json_contract "/api/plan" \
  'type=="object" and (.as_of|type=="string") and (.setup_summary|type=="string") and (.actionability_state|type=="string") and (.actionability_reason_codes|type=="array") and (all(.actionability_reason_codes[]?; type=="string")) and (.action_now.risk_allocation_target|type=="number") and (.action_now.raw_signal_allocation_target|type=="number") and (.action_now.risk_allocation_basis|type=="string") and (.edge_quality.score|type=="number") and (.edge_quality.calibration.quality|type=="string") and (.edge_quality.calibration.sample_size_7d|type=="number") and (.risk_band.d7.sample_size|type=="number") and (.invalidation_rules|type=="array") and ((.policy_state.stance|type=="string") and (.policy_state.risk_posture|type=="string") and (.policy_state.conflict_state|type=="string") and (.policy_state.base_signal|type=="string") and (.policy_state.regime_context|type=="string") and (.policy_state.rationale|type=="string") and (.policy_state.rationale_codes|type=="array")) and ((.uncertainty.headline==null) or (.uncertainty.headline|type=="string")) and (.uncertainty.flags.stale_inputs|type=="boolean") and (.uncertainty.flags.limited_calibration|type=="boolean") and (.uncertainty.flags.limited_scenario_sample|type=="boolean") and (.consistency.score|type=="number") and (.consistency.state|type=="string") and (.consistency.violations|type=="array") and (.consistency.components.base_score|type=="number") and (.consistency.components.structural_penalty|type=="number") and (.consistency.components.reliability_penalty|type=="number") and (.trader_playbook.recommended_size_pct.min|type=="number") and (.trader_playbook.recommended_size_pct.target|type=="number") and (.trader_playbook.recommended_size_pct.max|type=="number") and (.trader_playbook.scenarios|type=="array") and ((.trader_playbook.benchmark_follow_through_7d.hit_rate==null) or (.trader_playbook.benchmark_follow_through_7d.hit_rate|type=="number")) and (.trader_playbook.benchmark_follow_through_7d.sample_size|type=="number") and ((.trader_playbook.benchmark_follow_through_7d.unavailable_reason==null) or (.trader_playbook.benchmark_follow_through_7d.unavailable_reason|type=="string")) and ((has("brief_ref")|not) or ((.brief_ref.as_of|type=="string") and (.brief_ref.regime_delta|type=="string") and (.brief_ref.risk_posture|type=="string"))) and ((has("opportunity_ref")|not) or ((.opportunity_ref.as_of|type=="string") and (.opportunity_ref.horizon|type=="string") and (.opportunity_ref.eligible_count|type=="number") and (.opportunity_ref.suppressed_count|type=="number") and ((.opportunity_ref.degraded_reason==null) or (.opportunity_ref.degraded_reason|type=="string")))) and ((has("alerts_ref")|not) or ((.alerts_ref.as_of|type=="string") and (.alerts_ref.warning_count_24h|type=="number") and (.alerts_ref.critical_count_24h|type=="number"))) and ((has("cross_horizon")|not) or ((.cross_horizon.as_of|type=="string") and (.cross_horizon.state|type=="string") and (.cross_horizon.eligible_7d|type=="number") and (.cross_horizon.eligible_30d|type=="number") and ((.cross_horizon.top_direction_7d==null) or (.cross_horizon.top_direction_7d|type=="string")) and ((.cross_horizon.top_direction_30d==null) or (.cross_horizon.top_direction_30d|type=="string")) and (.cross_horizon.rationale_codes|type=="array") and (all(.cross_horizon.rationale_codes[]?; type=="string")) and ((.cross_horizon.invalidation_note==null) or (.cross_horizon.invalidation_note|type=="string")))) and ((has("decision_stack")|not) or ((.decision_stack.what_changed|type=="string") and (.decision_stack.what_to_do|type=="string") and (.decision_stack.why_now|type=="string") and (.decision_stack.confidence|type=="string") and (.decision_stack.cta_state|type=="string")))' \
  "plan"

check_api_json_contract "/api/brief?scope=market" \
  'type=="object" and (.as_of|type=="string") and (.summary|type=="string") and (.regime_delta|type=="string") and (.risk_posture|type=="string") and (.freshness_status.stale_count|type=="number") and (.policy_state.stance|type=="string") and (.policy_state.risk_posture|type=="string") and (.source_plan_as_of|type=="string") and (.contract_version|type=="string") and (.consistency.score|type=="number") and (.consistency.state|type=="string") and ((.degraded_reason==null) or (.degraded_reason|type=="string"))' \
  "brief"

check_api_json_contract "/api/opportunities?horizon=7d&limit=5" \
  'type=="object" and (.as_of|type=="string") and (.horizon|type=="string") and (.suppressed_count|type=="number") and (.suppression_by_reason.coherence_failed|type=="number") and (.suppression_by_reason.quality_filtered|type=="number") and (.suppression_by_reason.data_quality_suppressed|type=="number") and (.quality_filter_rate|type=="number") and (.coherence_fail_rate|type=="number") and (.actionability_state|type=="string") and (.actionability_reason_codes|type=="array") and (all(.actionability_reason_codes[]?; type=="string")) and (.cta_enabled|type=="boolean") and (.cta_disabled_reasons|type=="array") and (all(.cta_disabled_reasons[]?; type=="string")) and ((.degraded_reason==null) or (.degraded_reason|type=="string")) and ((.data_age_seconds==null) or (.data_age_seconds|type=="number")) and (.ttl_state|type=="string") and ((.next_expected_refresh_at==null) or (.next_expected_refresh_at|type=="string")) and ((.overdue_seconds==null) or (.overdue_seconds|type=="number")) and (.items|type=="array") and (all(.items[]?; (.id|type=="string") and (.theme_id|type=="string") and (.direction|type=="string") and (.conviction_score|type=="number") and (.calibration.quality|type=="string") and (.calibration.basis=="conviction_decile") and ((.calibration.window==null) or (.calibration.window|type=="string")) and ((.calibration.unavailable_reason==null) or (.calibration.unavailable_reason|type=="string")) and (.expectancy.sample_size|type=="number") and (.expectancy.basis|type=="string") and (.expectancy.quality|type=="string") and ((.expectancy.expected_move_pct==null) or (.expectancy.expected_move_pct|type=="number")) and ((.expectancy.max_adverse_move_pct==null) or (.expectancy.max_adverse_move_pct|type=="number")) and ((.expectancy.unavailable_reason==null) or (.expectancy.unavailable_reason|type=="string")) and (.eligibility.passed|type=="boolean") and (.eligibility.failed_checks|type=="array") and (.decision_contract.coherent|type=="boolean") and (.decision_contract.confidence_band|type=="string") and (.decision_contract.rationale_codes|type=="array")))' \
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

check_api_json_contract "/api/diagnostics/calibration?metric=conviction&horizon=7d" \
  'type=="object" and (.as_of|type=="string") and (.metric=="conviction") and (.horizon=="7d") and (.basis|type=="string") and (.total_samples|type=="number") and (.bins|type=="array") and (.diagnostics.quality_band|type=="string") and (.diagnostics.minimum_reliable_sample|type=="number") and (.diagnostics.insufficient_reasons|type=="array") and ((.diagnostics.brier_score==null) or (.diagnostics.brier_score|type=="number")) and ((.diagnostics.ece==null) or (.diagnostics.ece|type=="number")) and ((.diagnostics.log_loss==null) or (.diagnostics.log_loss|type=="number"))' \
  "diagnostics-calibration"

if edge_diagnostics_available; then
  check_api_json_contract "/api/diagnostics/edge?horizon=all" \
    'type=="object" and (.as_of|type=="string") and (.basis|type=="string") and (.windows|type=="array") and ((.windows|length) >= 1) and (.promotion_gate.pass|type=="boolean") and (.promotion_gate.reasons|type=="array") and (all(.promotion_gate.reasons[]?; type=="string")) and (all(.windows[]?; (.horizon|type=="string") and ((.as_of==null) or (.as_of|type=="string")) and (.sample_size|type=="number") and ((.model_direction_accuracy==null) or (.model_direction_accuracy|type=="number")) and ((.baseline_direction_accuracy==null) or (.baseline_direction_accuracy|type=="number")) and ((.uplift_vs_baseline==null) or (.uplift_vs_baseline|type=="number")) and ((.uplift_ci95_low==null) or (.uplift_ci95_low|type=="number")) and ((.uplift_ci95_high==null) or (.uplift_ci95_high|type=="number")) and (.lower_bound_positive|type=="boolean") and (.minimum_reliable_sample|type=="number") and (.quality_band|type=="string") and (.baseline_strategy=="lagged_actual_direction") and (.leakage_sentinel.pass|type=="boolean") and (.leakage_sentinel.violation_count|type=="number") and (.leakage_sentinel.reasons|type=="array") and (.calibration_diagnostics.quality_band|type=="string")))' \
    "diagnostics-edge"
fi

if utility_funnel_available; then
  check_api_json_contract "/api/ops/utility-funnel?window=7" \
    'type=="object" and (.as_of|type=="string") and (.funnel.window_days|type=="number") and (.funnel.days_observed|type=="number") and (.funnel.total_events|type=="number") and (.funnel.unique_sessions|type=="number") and (.funnel.plan_views|type=="number") and (.funnel.opportunities_views|type=="number") and (.funnel.decision_actionable_views|type=="number") and (.funnel.decision_watch_views|type=="number") and (.funnel.decision_no_action_views|type=="number") and (.funnel.no_action_unlock_views|type=="number") and (.funnel.cta_action_clicks|type=="number") and (.funnel.actionable_view_sessions|type=="number") and (.funnel.actionable_sessions|type=="number") and (.funnel.cta_action_rate_pct|type=="number") and (.funnel.decision_events_total|type=="number") and (.funnel.decision_events_per_session|type=="number") and (.funnel.no_action_unlock_coverage_pct|type=="number") and ((.funnel.last_event_at==null) or (.funnel.last_event_at|type=="string"))' \
    "utility-funnel"
  UTILITY_FUNNEL_LIVE=1
fi

check_api_json_contract "/api/ops/freshness-slo" \
  'type=="object" and (.as_of|type=="string") and (.windows["7d"].days_observed|type=="number") and (.windows["7d"].days_with_critical_stale|type=="number") and (.windows["7d"].slo_attainment_pct|type=="number") and (.windows["7d"].recent_incidents|type=="array") and (.windows["7d"].incident_impact.state|type=="string") and (.windows["7d"].incident_impact.stale_days|type=="number") and (.windows["7d"].incident_impact.warning_events|type=="number") and (.windows["7d"].incident_impact.critical_events|type=="number") and (.windows["7d"].incident_impact.estimated_suppressed_days|type=="number") and ((.windows["7d"].incident_impact.latest_warning_event==null) or (.windows["7d"].incident_impact.latest_warning_event.created_at|type=="string")) and ((.windows["7d"].incident_impact.latest_warning_event==null) or (.windows["7d"].incident_impact.latest_warning_event.severity|type=="string")) and (.windows["30d"].days_observed|type=="number") and (.windows["30d"].days_with_critical_stale|type=="number") and (.windows["30d"].slo_attainment_pct|type=="number") and (.windows["30d"].recent_incidents|type=="array") and (.windows["30d"].incident_impact.state|type=="string") and (.windows["30d"].incident_impact.stale_days|type=="number") and (.windows["30d"].incident_impact.warning_events|type=="number") and (.windows["30d"].incident_impact.critical_events|type=="number") and (.windows["30d"].incident_impact.estimated_suppressed_days|type=="number") and ((.windows["30d"].incident_impact.latest_warning_event==null) or (.windows["30d"].incident_impact.latest_warning_event.created_at|type=="string")) and ((.windows["30d"].incident_impact.latest_warning_event==null) or (.windows["30d"].incident_impact.latest_warning_event.severity|type=="string"))' \
  "freshness-slo"

if decision_grade_available; then
  check_api_json_contract "/api/ops/decision-grade?window=30" \
    'type=="object" and (.as_of|type=="string") and (.window_days|type=="number") and (.score|type=="number") and (.grade|type=="string") and (.go_live_ready|type=="boolean") and (.go_live_blockers|type=="array") and (all(.go_live_blockers[]?; type=="string")) and (.readiness.decision_impact_window_days|type=="number") and (.readiness.decision_impact_enforce_ready|type=="boolean") and (.readiness.decision_impact_breaches|type=="array") and (all(.readiness.decision_impact_breaches[]?; type=="string")) and (.readiness.decision_impact_market_7d_sample_size|type=="number") and (.readiness.decision_impact_market_30d_sample_size|type=="number") and (.readiness.decision_impact_actionable_sessions|type=="number") and (.readiness.minimum_samples_required|type=="number") and (.readiness.minimum_actionable_sessions_required|type=="number") and (.components.freshness.score|type=="number") and (.components.freshness.status|type=="string") and (.components.freshness.slo_attainment_pct|type=="number") and (.components.freshness.days_with_critical_stale|type=="number") and (.components.freshness.days_observed|type=="number") and (.components.consistency.score|type=="number") and (.components.consistency.status|type=="string") and (.components.consistency.pass_count|type=="number") and (.components.consistency.warn_count|type=="number") and (.components.consistency.fail_count|type=="number") and (.components.consistency.total|type=="number") and (.components.calibration.score|type=="number") and (.components.calibration.status|type=="string") and (.components.calibration.conviction_7d|type=="string") and (.components.calibration.conviction_30d|type=="string") and (.components.calibration.edge_quality|type=="string") and (.components.edge.score|type=="number") and (.components.edge.status|type=="string") and (.components.edge.promotion_gate_pass|type=="boolean") and (.components.edge.lower_bound_positive_horizons|type=="number") and (.components.edge.horizons_observed|type=="number") and (.components.edge.reasons|type=="array") and (all(.components.edge.reasons[]?; type=="string")) and (.components.opportunity_hygiene.score|type=="number") and (.components.opportunity_hygiene.status|type=="string") and (.components.opportunity_hygiene.publish_rate_pct|type=="number") and (.components.opportunity_hygiene.over_suppression_rate_pct|type=="number") and (.components.opportunity_hygiene.cross_horizon_conflict_rate_pct|type=="number") and (.components.opportunity_hygiene.conflict_persistence_days|type=="number") and (.components.opportunity_hygiene.rows_observed|type=="number") and (.components.utility.score|type=="number") and (.components.utility.status|type=="string") and (.components.utility.decision_events_total|type=="number") and (.components.utility.no_action_unlock_coverage_pct|type=="number") and (.components.utility.unique_sessions|type=="number")' \
    "decision-grade"
  DECISION_GRADE_LIVE=1
fi

if go_live_readiness_available; then
  check_api_json_contract "/api/ops/go-live-readiness?window=30" \
    'type=="object" and (.as_of|type=="string") and (.window_days==30) and (.score_window_days==30) and (.go_live_ready|type=="boolean") and (.blockers|type=="array") and (all(.blockers[]?; type=="string")) and (.grade.score|type=="number") and (.grade.grade|type=="string") and (.readiness.decision_impact_window_days|type=="number") and (.readiness.decision_impact_enforce_ready|type=="boolean") and (.readiness.decision_impact_breaches|type=="array") and (.readiness.decision_impact_market_7d_sample_size|type=="number") and (.readiness.decision_impact_market_30d_sample_size|type=="number") and (.readiness.decision_impact_actionable_sessions|type=="number") and (.readiness.minimum_samples_required|type=="number") and (.readiness.minimum_actionable_sessions_required|type=="number") and (.components.freshness.status|type=="string") and (.components.edge.status|type=="string") and (.components.utility.status|type=="string")' \
    "go-live-readiness"
  GO_LIVE_READINESS_LIVE=1
fi

if decision_impact_available; then
  check_api_json_contract "/api/decision-impact?horizon=7d&scope=market&window=30" \
    'type=="object" and (.as_of|type=="string") and (.horizon=="7d") and (.scope=="market") and (.window_days==30) and (.outcome_basis=="spy_forward_proxy") and (.market.sample_size|type=="number") and (.market.hit_rate|type=="number") and (.market.avg_forward_return_pct|type=="number") and (.market.avg_signed_return_pct|type=="number") and (.market.win_rate|type=="number") and (.market.downside_p10_pct|type=="number") and (.market.max_loss_pct|type=="number") and (.market.quality_band|type=="string") and (.themes|type=="array") and (.coverage.matured_items|type=="number") and (.coverage.eligible_items|type=="number") and (.coverage.coverage_ratio|type=="number") and (.coverage.insufficient_reasons|type=="array")' \
    "decision-impact-market"

  check_api_json_contract "/api/decision-impact?horizon=7d&scope=theme&window=30&limit=10" \
    'type=="object" and (.as_of|type=="string") and (.horizon=="7d") and (.scope=="theme") and (.window_days==30) and ((.outcome_basis=="spy_forward_proxy") or (.outcome_basis=="theme_proxy_blend")) and (.market.sample_size|type=="number") and (.themes|type=="array") and (all(.themes[]?; (.theme_id|type=="string") and (.theme_name|type=="string") and (.sample_size|type=="number") and (.hit_rate|type=="number") and (.avg_signed_return_pct|type=="number") and (.avg_forward_return_pct|type=="number") and (.win_rate|type=="number") and (.quality_band|type=="string") and (.last_as_of|type=="string"))) and (.coverage.matured_items|type=="number") and (.coverage.eligible_items|type=="number") and (.coverage.coverage_ratio|type=="number") and (.coverage.insufficient_reasons|type=="array") and ((.coverage.theme_proxy_eligible_items == null) or (.coverage.theme_proxy_eligible_items|type=="number")) and ((.coverage.spy_fallback_items == null) or (.coverage.spy_fallback_items|type=="number"))' \
    "decision-impact-theme"

  check_api_json_contract "/api/ops/decision-impact?window=30" \
    'type=="object" and (.as_of|type=="string") and (.window_days==30) and (.market_7d.sample_size|type=="number") and (.market_7d.hit_rate|type=="number") and (.market_30d.sample_size|type=="number") and (.market_30d.hit_rate|type=="number") and (.theme_summary.themes_with_samples|type=="number") and (.theme_summary.themes_robust|type=="number") and (.theme_summary.top_positive|type=="array") and (.theme_summary.top_negative|type=="array") and (.utility_attribution.actionable_views|type=="number") and (.utility_attribution.actionable_sessions|type=="number") and (.utility_attribution.cta_action_clicks|type=="number") and (.utility_attribution.cta_action_rate_pct|type=="number") and (.utility_attribution.no_action_unlock_views|type=="number") and (.utility_attribution.decision_events_total|type=="number") and (.observe_mode.enabled==true) and ((.observe_mode.mode=="observe") or (.observe_mode.mode=="enforce")) and (.observe_mode.thresholds.market_7d_hit_rate_min|type=="number") and (.observe_mode.thresholds.market_30d_hit_rate_min|type=="number") and (.observe_mode.thresholds.market_7d_avg_signed_return_min|type=="number") and (.observe_mode.thresholds.market_30d_avg_signed_return_min|type=="number") and (.observe_mode.thresholds.cta_action_rate_pct_min|type=="number") and (.observe_mode.minimum_samples_required|type=="number") and (.observe_mode.minimum_actionable_sessions_required|type=="number") and (.observe_mode.enforce_ready|type=="boolean") and (.observe_mode.enforce_breaches|type=="array") and (.observe_mode.enforce_breach_count|type=="number") and (.observe_mode.breaches|type=="array") and (.observe_mode.breach_count|type=="number")' \
    "ops-decision-impact"
  DECISION_IMPACT_LIVE=1
fi

check_policy_state_consistency
check_plan_brief_coherence
check_consistency_gate
check_plan_playbook_target_coherence
check_plan_actionability_contract
check_refresh_recency
check_freshness_alert_parity
check_alert_aggregate_parity
check_degraded_consistency_score
check_ml_low_sample_semantics
check_opportunity_coherence_contract
if edge_diagnostics_available; then
  check_edge_diagnostics_contract
fi
if [[ "$UTILITY_FUNNEL_LIVE" == "1" ]]; then
  check_utility_funnel_semantics
  check_cta_action_event_contract
fi
if [[ "$DECISION_GRADE_LIVE" == "1" ]]; then
  check_decision_grade_semantics
fi
if [[ "$GO_LIVE_READINESS_LIVE" == "1" ]]; then
  check_go_live_readiness_semantics
fi
if [[ "$DECISION_IMPACT_LIVE" == "1" ]]; then
  check_decision_impact_semantics
fi

echo "Product API contract checks passed against $API_URL"
