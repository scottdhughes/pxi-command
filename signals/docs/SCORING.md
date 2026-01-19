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
