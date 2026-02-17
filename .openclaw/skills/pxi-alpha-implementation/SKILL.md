---
name: pxi-alpha-implementation
description: Convert quant hypotheses into safe PXI code changes with validation, tests, and rollback-aware delivery.
metadata:
  {
    "openclaw":
      {
        "emoji": "A",
      },
  }
---

# PXI Alpha Implementation

Use this skill when moving from idea to code in `/Users/scott/pxi`.

## Default Workflow

1. Read current implementation and identify insertion points.
2. State hypothesis, assumptions, and expected behavior change.
3. Implement the smallest high-signal patch.
4. Add or update tests for:
   - correctness,
   - edge cases,
   - regression protection.
5. Run test suite and summarize outcomes.
6. Report risk, rollout advice, and rollback path.

## Quality Bar

- Prefer simple models/features that are interpretable and testable.
- Avoid over-parameterized logic without clear out-of-sample benefit.
- Keep compute and data dependency costs explicit.

## Required Deliverables

- File-level change list.
- Before vs after behavior summary.
- Validation evidence (test output, metrics, or benchmark snippets).
- Open risks and next experiment queue.
