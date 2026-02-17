---
name: quant-lit-review
description: Find, assess, and operationalize high-signal quant research for PXI using primary sources and implementability scoring.
metadata:
  {
    "openclaw":
      {
        "emoji": "Q",
      },
  }
---

# Quant Literature Review

Use this skill when asked to scan new research and turn findings into actionable PXI upgrades.

## Source Policy

- Prefer primary sources: arXiv, SSRN, NBER, peer-reviewed journals, official technical notes.
- Avoid blog-only evidence unless it links to primary methodology/data.
- Keep the default recency window to the last 30 days unless the user asks otherwise.

## Selection Rubric

For each candidate paper, score 1-5 on:

1. Relevance to PXI (macro regime, signal construction, risk allocation, robustness).
2. Empirical quality (out-of-sample validation, realistic assumptions, sample size).
3. Implementability (clear feature definition, available data, low operational friction).
4. Robustness (regime stability, turnover/cost awareness, leakage controls).

## Output Contract

Return:

1. Top 3-5 papers with citation + URL.
2. Why each matters for PXI (2-3 bullets each).
3. Transfer risk: what does not transfer to PXI.
4. One concrete hypothesis per top paper, with:
   - target module/file,
   - test plan,
   - expected impact,
   - failure criteria.

## Engineering Guardrails

- No look-ahead bias.
- No data leakage between feature engineering and evaluation.
- Separate research idea from production rollout; require tests/backtests first.
