# 0023. Layer data-quality checks by where they run and what they block; build money checks in-house and adopt Soda Core for rules

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

"Is it correct?" has to be answered by checks that sit in the pipeline, with a clear action when they fail. The design already has blocking file validation (ADR 0013) and the publish gate (ADR 0014). It only said "an off-the-shelf tool" for rule-based checks, without naming one.

## Decision

Four layers of checks:

| Layer | Where | Checks | On failure |
|---|---|---|---|
| 1. Contract | Ingestion boundary | Schema Registry compatibility for streams; whole-file validation: header, types, keys, business date, trailer totals | Reject or quarantine the whole unit; nothing lands |
| 2. Rules | Bronze → silver | Nulls, ranges, allowed values, referential integrity, sign matching the transaction type, uniqueness | Failing **rows** go to `ops.quarantine` with the rule and batch id. Money tables fail the batch above a threshold (0.1% of rows) |
| 3. Reconciliation | Per batch and per business date | Exact rows, signed paise and distinct keys against independent totals | Money gold **blocks** (the gate); other tables flag |
| 4. Monitoring | After publish | Volume drift, distribution shifts, freshness | Alert only |

**Tooling:**

- **Layers 1 and 3 stay in-house PySpark** (`validate`, `control_totals`). They're small, exact, already tested, and have no dependencies. Money semantics shouldn't depend on a third-party library's interpretation.
- **Layers 2 and 4 use Soda Core.** Checks are written in YAML (SodaCL) and kept as code next to each table's contract. They run inside the Spark jobs on the same DataFrame. Before adopting it, confirm it supports Spark 4.1; if not, fall back to Great Expectations.
- **Results** go to Prometheus (alerts, design Appendix C) and to the catalog (ADR 0024), so each table shows its quality history.
- **Ownership:** domain teams own the rules for their tables; the platform owns the framework and the quarantine replay tool.

## Consequences

- **Every check has an owner and a defined action.** Nothing just logs a warning.
- **Quarantined rows can be replayed** once their rule or source is fixed, so data is never silently dropped.
- **Two mechanisms to maintain:** in-house for money, a tool for everything else.

## Alternatives considered

- **Great Expectations:** comprehensive, but heavier to set up. It's the fallback.
- **Deequ / PyDeequ:** Spark-native, but the Python wrapper tends to lag behind new Spark versions.
- **dbt tests:** a good fit only if gold is modelled in dbt, which it isn't.
- **All in-house:** full control, but a large rule and anomaly library to maintain.
