# 0014. Publish gold through a write-audit-publish gate with exact reconciliation

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

"Write, then check" exposes unchecked numbers and leaves bad data in place. Checking only the lake ignores the ClickHouse copy that dashboards show. Row counts alone miss a duplicated row paired with a dropped one.

## Decision

Implemented in `src/cdp/publish_gate.py`:
- **Write:** each gold run writes its whole business-date window to an Iceberg branch `audit_<run_id>`, using `overwrite(window)`, not `overwritePartitions()`.
- **Audit:** control totals (rows, signed paise, distinct keys) per (source_system, business_date) must exactly match the source's totals and silver.
- **Publish or keep main:**
  - *On a pass,* `fast_forward` moves `main`. Iceberg refuses if `main` has moved in the meantime, which catches a second writer.
  - *On a failure,* `main` is untouched and the branch is kept.
- **Logging:** every verdict goes to `ops.reconciliation_log`.
- **ClickHouse:** a load becomes visible only after it reconciles to the same snapshot (ADR 0008).
- **Scope:** money tables block on failure; other tables flag and still publish.

## Consequences

- **Consumers never see unreconciled money figures.** Stale-but-correct data is visible through the watermark.
- **Publication waits for the audit,** and a late source control file blocks its tables.
- **Depends on independent source totals.** Without them, the gate can only check the lake against itself.
- **No test file,** by plan. The scenarios were verified by hand against real Iceberg and are listed in `docs/test-plan.md`.

## Alternatives considered

- **Checks after publishing, with rollback:** consumers see bad data in the meantime.
- **Tolerance-based comparison:** unnecessary with integer paise, and it hides real errors.
