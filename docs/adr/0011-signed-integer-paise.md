# 0011. Represent money as signed integer paise, with Spark ANSI mode on

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

The brief states that money is in integer paise. Reconciliation needs exact equality across Spark, ClickHouse and the source systems.

## Decision

Money is stored as `BIGINT` paise from end to end. It is **signed**: refunds, reversals and write-offs are negative. There are no floats or `DECIMAL` rupees before the presentation layer. Spark runs with ANSI mode on, which is the Spark 4 default and was verified locally, so an overflowing sum raises an error instead of wrapping.

## Consequences

- **Reconciliation can demand exact matches,** with no tolerance band (ADR 0014).
- **Validation rules check that the sign matches the transaction type,** not `amount >= 0`.

## Alternatives considered

- **`DECIMAL` rupees:** workable, but invites rounding and scale mismatches between engines.
- **Floats:** make exact reconciliation impossible.
