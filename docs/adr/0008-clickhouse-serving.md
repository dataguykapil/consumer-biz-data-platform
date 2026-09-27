# 0008. Serve dashboards and derived app reads from ClickHouse, reconciled per snapshot

- **Status:** Accepted; app-read part superseded by [0019](0019-online-serving-store-for-app-reads.md)
- **Date:** 2026-09-26

## Context

Dashboards repeat the same analytical queries, and apps need fast customer-keyed reads, for example transaction history. Querying Iceberg directly for these would rescan Parquet on every refresh.

## Decision

ClickHouse holds a copy of published gold. Rows carry `_snapshot_seq` in the `ReplacingMergeTree` sort key. A load becomes visible only after its control totals match the Iceberg snapshot it came from, recorded in `cdp_meta.published_versions` (ADR 0014). App tables are sorted by `customer_id` first.

## Consequences

- **Fast repeated analytics and point lookups** at 50M customers.
- **A second copy of the numbers,** which is why every load is reconciled.
- **Superseded versions accumulate** until a purge job removes them.
- **Not yet tested for real:** the ClickHouse SQL has only run against a fake client in tests, never against a real ClickHouse.

## Alternatives considered

- **Querying Iceberg directly for dashboards:** Parquet is rescanned on every refresh.
- **Druid or Pinot:** better at very high-concurrency, user-facing analytics, but more complex to run.
- **Snowflake or BigQuery:** low operations, but cost at scale and another vendor.
- **ClickHouse querying Iceberg in place:** would remove the copy, but its time travel against tags and snapshots is unverified, and audit can't depend on a half-supported feature.
