# PXI production audit — 2026-08-20

## Verdict

PXI is a coherent percentile-based market-regime composite, not a validated
trading model. The deployed product had material source-contract drift: labels,
configured inputs, live scoring inputs, and category-detail output did not agree.
The live score should be treated as descriptive research, not decision-grade
evidence, until the corrected credit history is refreshed and the signal model
has adequate out-of-sample calibration.

## Confirmed defects and disposition

| Severity | Finding | Disposition |
| --- | --- | --- |
| High correctness | `MANEMP` was presented as ISM Manufacturing PMI. It is BLS manufacturing payrolls in thousands. | Renamed and described accurately; retained the legacy internal ID to preserve history. Monthly freshness now reflects observation-date lag. |
| High correctness | `BAMLC0A4CBBBEY` (BBB effective yield) was scored as investment-grade OAS. | Fetch changed to `BAMLC0A0CM` (US Corporate OAS). Production history must be refreshed before the credit score is trusted. |
| High correctness | A fake ISM Services series fell back from unrelated FRED series and ultimately copied manufacturing payrolls onto today's date. | Removed from the canonical model and ingestion pipeline. |
| High correctness | The canonical 28-input definition and deployed 26-input model disagreed. | Canonical definitions now match the deployed 26-input model and category weights. |
| Medium correctness | Category detail used a stale hand-maintained ID map and returned empty or incomplete indicator lists. | It now derives IDs and names from the canonical definition. |
| Medium trust | AAII sentiment and SPX gamma labels concealed calculated proxies. | Renamed to Risk Sentiment Proxy and Volatility-Surface Proxy with explicit descriptions. |
| Medium security | Public `POST /api/embed` could invoke paid AI repeatedly and mutate Vectorize. | Admin authentication added before any work. |
| Medium integrity | Anonymous utility telemetry affected go-live and enforcement gates. | Telemetry remains available for product analytics but is excluded from readiness and enforcement decisions. |
| Medium abuse | Subscription start could repeatedly email an arbitrary recipient. | Added a durable 10-minute per-recipient cooldown with a generic response. |

## Data-quality profile

- Grain: one observation per `indicator_id` and observation date; latest values
  are forward-filled for the daily composite.
- Timeliness: daily market inputs were current on 2026-08-20. Monthly observation
  dates can legitimately trail release dates by more than 45 calendar days.
- Validity: the live `ism_manufacturing=12611` value is valid for manufacturing
  payrolls but impossible for a diffusion PMI; this exposed the source mismatch.
- Consistency: the live `ig_oas_spread` near 5.5 was a yield, while an IG OAS is
  near 0.8 in the same units. This exposed a second source mismatch.
- Historical contamination: retired/proxy series remain in D1. They are not in
  the canonical scoring model, but should be retained only as quarantined legacy
  data until a versioned-model migration is implemented.

## Model interpretation

The Worker scores each input by its empirical percentile over up to five years,
inverts risk-off inputs, applies fixed indicator weights, and renormalizes when
inputs are missing. That produces an interpretable regime score, but it also
means:

- a source substitution changes the meaning of historical percentiles;
- a missing input silently changes effective weights;
- revisions and look-ahead effects are not controlled by vintage data;
- proxy features are not equivalent to the branded economic/positioning data;
- predictive labels and opportunities require independent out-of-sample proof.

## Quant-readiness gate

Do not add execution or portfolio-sizing tools yet. First require a versioned
feature dictionary, point-in-time data, immutable model versions, walk-forward
validation, transaction-cost assumptions, and a minimum sample/calibration gate.
Until then PXI should be presented as a research dashboard and regime heuristic,
not a forecast or investment recommendation.

