# Scoring & Classification Algorithms

This document details the algorithms used to analyze investment themes and generate signal classifications.

## Overview

The analysis pipeline processes Reddit discussions through four stages:

1. **Metrics Computation** - Calculate raw measurements
2. **Z-Score Normalization** - Standardize across themes
3. **Composite Scoring** - Weight and combine components
4. **Classification** - Assign signal type, confidence, timing

---

## 1. Metrics Computation

### Mention Velocity

Measures how discussion volume is changing over time.

**Windows**
- Lookback (L): Recent window (default: 7 days)
- Baseline (B): Historical comparison (default: 30 days)

**Calculations**
```
current_rate  = mentions_L / L
baseline_rate = mentions_B / B
growth_ratio  = (current_rate + ε) / (baseline_rate + ε)
```

Where `ε` (epsilon) is a small constant to prevent division by zero.

**Slope**
Linear regression slope over daily mention counts in the lookback window:
- Positive slope: Accelerating discussion
- Negative slope: Decelerating discussion
- Near-zero: Stable volume

**Raw Velocity Formula**
```
velocity_raw = log(growth_ratio) + slope
```

The logarithm dampens extreme ratios while preserving relative ordering.

### Sentiment Analysis

Uses VADER (Valence Aware Dictionary and sEntiment Reasoner) for sentiment scoring.

**VADER Properties**
- Returns compound score in [-1, 1]
- Handles emojis, slang, capitalization
- Pre-trained on social media text

**Calculations**
```
sentiment_L     = avg(VADER(text)) for lookback docs
sentiment_B     = avg(VADER(text)) for baseline docs
sentiment_shift = sentiment_L - sentiment_B
```

**Interpretation**
- Positive shift: Sentiment improving
- Negative shift: Sentiment deteriorating
- Near-zero: Stable sentiment

### Confirmation Score

Measures how widely distributed discussion is across sources.

**Dispersion**
```
unique_subreddits = count(distinct subreddits with theme mentions)
```

Higher dispersion suggests broader market awareness.

**Concentration**
```
concentration = sum(top 3 post mention counts) / total_mentions
```

High concentration (>0.6) indicates reliance on few sources.

**Confirmation Formula**
```
confirmation = f(unique_subreddits, concentration)
```

Where `f` rewards dispersion and penalizes concentration.

### Price Metrics (Optional)

When a price provider is enabled:

```
momentum_score   = normalized_returns(proxy_etf, lookback)
divergence_score = |price_trend - sentiment_trend|
price_raw        = momentum_score + divergence_score
```

When unavailable, `price_raw = 0` and its weight is redistributed.

---

## 2. Z-Score Normalization

Raw values are normalized across all themes using z-scores:

```
z = (x - μ) / σ
```

Where:
- `x` = raw value for a theme
- `μ` = mean across all themes
- `σ` = standard deviation

**Properties**
- Mean of z-scores = 0
- Standard deviation = 1
- Enables fair comparison across different scales

**Edge Cases**
- Single theme: z-score = 0 (no comparison possible)
- Zero variance: z-score = 0 (all values identical)

---

## 3. Composite Scoring

The final score combines normalized components with weights:

```
score = w₁·velocity_z + w₂·sentiment_z + w₃·confirmation_z + w₄·price_z
```

**Default Weights** (defined in `src/analysis/constants.ts`)

| Component | Weight | Rationale |
|-----------|--------|-----------|
| Velocity | 0.40 | Primary driver - accelerating interest |
| Sentiment | 0.20 | Secondary - directional sentiment |
| Confirmation | 0.30 | Important - multi-source validation |
| Price | 0.10 | Optional - market confirmation |

**Weight Normalization**
Weights sum to 1.0. When price data is unavailable, the price weight is redistributed proportionally to other components.

**Sorting**
Themes are sorted by composite score in descending order.

---

## 4. Classification

Each theme receives four classification dimensions.

### Signal Type

Based on pattern matching across metrics:

| Type | Criteria |
|------|----------|
| **Rotation** | High velocity + positive sentiment + dispersion |
| **Momentum** | Price confirmation + positive velocity |
| **Mean Reversion** | Negative sentiment shift + declining velocity |
| **Contrarian** | High volume + mixed/negative sentiment |

### Confidence

Based on data quality thresholds:

```
confidence_flags = 0

if mentions_L >= 8:          confidence_flags++
if unique_subreddits >= 3:   confidence_flags++
if concentration <= 0.5:     confidence_flags++
if price_available:          confidence_flags++
```

| Flags | Confidence |
|-------|------------|
| 3-4 | High |
| 2 | Medium |
| 0-1 | Low |

### Timing

Based on velocity characteristics:

| Timing | Criteria |
|--------|----------|
| **Now** | slope > 0.2 AND growth_ratio > 1.4 |
| **Now (volatile)** | "Now" criteria + concentration > 0.6 |
| **Building** | 1.2 < growth_ratio ≤ 1.4 OR moderate slope |
| **Early** | Low volume but accelerating slope |
| **Ongoing** | Sustained volume without acceleration |

### Star Rating

Composite quality score (1-5 stars):

```
base_stars = 3  // Average

if rank <= top_20%:        stars++
if confidence == "High":   stars++
if timing == "Now":        stars++
if concentration > 0.6:    stars--
if mentions_L < 5:         stars--

stars = clamp(stars, 1, 5)
```

---

## 5. Takeaways Generation

Human-readable summaries are generated from classified themes.

### Data Shows

Top 3 themes with growth metrics:
```
"Theme X showing 2.50x growth across 7 subreddits."
```

### Actionable Signals

Grouped by timing:
- **Immediate**: "Now" and "Now (volatile)" themes
- **Building**: Themes still developing
- **Watch**: "Early" and "Ongoing" themes

### Risk Factors

Identified concerns:
- High concentration (>0.6)
- Low sample size (<5 mentions)
- Default message if no risks found

---

## Prediction Evaluation Horizon

Predictions are evaluated on a **+7 trading-day horizon** (not +7 calendar days).

Implementation policy:
- Start from `signal_date`.
- Advance one calendar day at a time.
- Count only NYSE trading sessions (skip weekends + NYSE holidays).
- Set `target_date` when 7 trading sessions have elapsed.
- At evaluation time, fetch historical ETF close on/after `target_date` (bounded forward window), not runtime spot.
- Persist the resolved market date as `exit_price_date` for auditability.
- If no valid historical close is available, keep `hit = null` and record `evaluation_note`.

Why this matters:
- Reduces horizon drift around long weekends and holiday weeks.
- Prevents delayed-run spot pricing from biasing realized returns.
- Keeps measured holding periods consistent with intended market opportunity.
- Avoids denominator bias by excluding unresolved rows (`hit = null`) from hit-rate aggregates.

---

## Accuracy Evaluation Uncertainty (`/api/accuracy`)

Point estimates can overstate certainty when sample sizes are small.
The API therefore reports a 95% Wilson confidence interval for each hit-rate estimate.
Only rows with resolved outcomes (`hit` non-null) are included in hit-rate denominators.

For a group with `hits = k` and `total = n`:

```
p̂ = k / n
z = 1.959963984540054  // 95% two-sided normal quantile

center = (p̂ + z² / (2n)) / (1 + z² / n)
margin = z * sqrt((p̂(1-p̂) + z²/(4n)) / n) / (1 + z² / n)

CI_low  = max(0, center - margin)
CI_high = min(1, center + margin)
```

The response exposes these as percent fields:
- `hit_rate_ci_low`
- `hit_rate_ci_high`

It also includes:
- `minimum_recommended_sample_size` (currently `30`)
- `sample_size_warning` per bucket (`true` when `count < 30`)
- coverage counters: `evaluated_count`, `resolved_count`, `unresolved_count`, and `unresolved_rate`

Coverage interpretation:
- High unresolved rates imply missing market-data exits and should reduce confidence in point estimates.
- Compare `resolved_count` to `evaluated_count` before promoting threshold/weight changes.

Interpretation policy:
- Prefer decisions where both point estimate and lower CI bound are acceptable.
- Treat subgroups with warnings as exploratory, not deployment-grade evidence.

## Validation Protocol Primitives (Offline / Research Path)

To reduce overfitting and data-snooping risk before any production threshold/weight changes,
the repo includes offline validation primitives in `src/evaluation_validation.ts`:

- `computeWalkForwardSlices(...)`
  - deterministic rolling/expanding temporal splits,
  - explicit no-leakage train/test boundaries.
- `computeRankICSeries(...)`
  - per-signal-date Spearman rank correlation series,
  - `rank_ic` sign convention where positive indicates better alignment.
- `computeHitRateIntervals(...)`
  - Wilson CI summary for hit-rate stability checks.
- `computeMultipleTestingAdjustedPvalues(...)`
  - Holm step-down family-wise error control baseline,
  - designed as a stable interface for future bootstrap Reality-Check/SPA implementations.

Primary-source anchors for this protocol direction:
- White (2000), *A Reality Check for Data Snooping* — https://doi.org/10.1111/1468-0262.00152
- Romano & Wolf (2005), stepwise multiple testing formalization — https://doi.org/10.1111/j.1468-0262.2005.00615.x
- Bailey et al. (2016), *The Probability of Backtest Overfitting* — https://doi.org/10.21314/jcf.2016.322
- Arian et al. (2024), ML-era backtest overfitting OOS protocol sensitivity — https://doi.org/10.1016/j.knosys.2024.112477
- Bailey & López de Prado (2014), *Deflated Sharpe Ratio* — https://doi.org/10.3905/jpm.2014.40.5.094

### Evaluation Report Script (`report:evaluation`)

A deterministic offline report path is available in `scripts/evaluation_report.ts`.

It consumes historical outcomes (`predictions[]` rows with `signal_date`, `rank`, `return_pct`, `hit`) and emits:
- `out/evaluation/report.json` (machine-readable artifact)
- `out/evaluation/report.md` (operator summary)

Default command (uses `data/evaluation_sample_predictions.json`):
```bash
npm run report:evaluation
```

Typical run with explicit input and smaller windows for sparse samples:
```bash
npm run report:evaluation -- \
  --input data/evaluation_sample_predictions.json \
  --out out/evaluation \
  --min-train 4 \
  --test-size 2 \
  --step-size 2
```

Current report contents:
- full-sample hit rate with Wilson CI,
- full-sample rank-IC summary (mean/median/IQR),
- walk-forward slice stats,
- Holm-adjusted p-values across slice and subgroup hypotheses,
- governance gate status (`pass|fail`) with explicit threshold-breach reasons,
- explicit primary-source references for anti-snooping context.

Governance thresholds are configurable at run time:
- `--min-resolved <n>` minimum required resolved observations (default: `30`)
- `--max-unresolved-rate <pct>` maximum allowed unresolved rate percentage (default: `20`)
- `--min-slices <n>` minimum required walk-forward slices (default: `3`)

When governance status is `fail`, the script exits non-zero after writing artifacts so the report can be used as a CI/deploy gate.

## Configuration Constants

All thresholds are defined in `src/analysis/constants.ts`:

```typescript
export const WEIGHTS = {
  velocity: 0.4,
  sentiment: 0.2,
  confirmation: 0.3,
  price: 0.1,
}

export const THRESHOLDS = {
  confidence: {
    minMentions: 8,
    minSubreddits: 3,
    maxConcentration: 0.5,
  },
  timing: {
    slopeNow: 0.2,
    concentrationVolatile: 0.6,
    growthBuilding: 1.4,
    growthOngoing: 1.0,
  },
}

export const RISK_THRESHOLDS = {
  highConcentration: 0.6,
  lowMentions: 5,
}
```

---

## Mathematical Notes

### Why Logarithm for Velocity?

The log transform:
1. Compresses extreme ratios (100x vs 2x become more comparable)
2. Converts multiplicative relationships to additive
3. Treats 2x growth same magnitude as 0.5x decline

### Why Z-Scores?

Z-score normalization:
1. Makes components dimensionless
2. Enables weighted addition of different metrics
3. Produces stable rankings regardless of absolute values
4. Handles outliers proportionally

### Why These Weights?

Default weights reflect:
- Velocity (0.4): Primary signal - discussion acceleration predates price moves
- Confirmation (0.3): Validates signal authenticity across sources
- Sentiment (0.2): Directional indicator but noisy
- Price (0.1): Confirmatory but optional; social signals should lead
