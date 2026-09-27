# 0009. Use Spark SQL, not Trino, for ad hoc and audit queries

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

The first draft of the design included Trino for ad hoc and audit queries. On review we questioned whether it was needed. These queries are low-concurrency and human-driven, and the platform has no cross-database federation, which is Trino's main strength.

## Decision

Drop Trino. Ad hoc and audit queries run on Spark SQL: notebooks, plus a shared Spark Connect endpoint on its own autoscaled cluster with per-user query timeouts. Auditors get a read-only Polaris role.

## Consequences

- **One less engine** to deploy, secure, upgrade and run on call.
- **Audit queries use the engine with the fullest Iceberg support,** reading the system of record directly, not a copy.
- **Interactive latency is seconds to minutes,** and concurrency is limited.
- **How to reverse it:** if ad hoc demand outgrows this, add Trino against the same catalog. Nothing else changes.

## Alternatives considered

- **Trino / Starburst:** fast interactive SQL, but an extra engine for a workload this small.
- **ClickHouse querying Iceberg in place:** see ADR 0008.
