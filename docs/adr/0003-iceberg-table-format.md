# 0003. Use Apache Iceberg as the table format

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

Several engines read and write the same tables: Spark, ClickHouse, and possibly Trino later. The publish gate needs an atomic "write, check, then make visible" step. CDC needs row-level updates with safe concurrent writers.

## Decision

All lake tables are Iceberg tables. CDC-fed silver tables use merge-on-read, and gold tables use the default copy-on-write.

## Consequences

- **Branches and `fast_forward`** make write-audit-publish a metadata operation (ADR 0014).
- **Optimistic concurrency** with serializable isolation makes the file-restatement race safe (ADR 0013). The race tests confirmed this.
- **Maintenance is ours:** compaction, delete-file rewrites, snapshot expiry (7 days) and orphan cleanup are recurring jobs with their own SLO.
- **History:** because snapshots expire, Iceberg time travel can't serve as 5-year audit history, so history is modelled in the data instead (ADR 0013).

## Alternatives considered

- **Delta Lake:** mature, and best inside Databricks. It has narrower multi-engine support, and no native branch-based publish.
- **Apache Hudi:** strong at upserts, but has a heavier operational surface and a smaller multi-engine ecosystem.
- **Plain Parquet with Hive tables:** no atomic publish and no safe concurrent writes. Not acceptable for money.
