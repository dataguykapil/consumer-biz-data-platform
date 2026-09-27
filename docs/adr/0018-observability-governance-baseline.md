# 0018. Adopt a baseline for observability and governance

- **Status:** Accepted; PII handling refined by [0025](0025-pii-storage-and-anonymisation.md), catalog and lineage tooling by [0024](0024-metadata-catalog-and-lineage.md)
- **Date:** 2026-09-27

## Context

Trust depends on being able to answer "is it healthy?", "who gets paged?" and "who can see this?", not only on the correctness of the pipelines. The platform handles regulated lending, insurance and payments data in India.

## Decision

The details are in `docs/design.md`, Appendix C.

**Observability.**
- **Collection and routing:** Prometheus metrics from Spark, Kafka and ClickHouse, plus table-health metrics from Iceberg's metadata tables. Grafana shows them, and Alertmanager routes by severity: P1 pages now, P2 pages during business hours, others become tickets.
- **P1 alerts:** replication-slot WAL above 50% of its cap, a failed reconciliation on a money table, a money table past its freshness SLO, and the catalog being down.

**Governance.**
- **Ownership:** each domain owns its tables and names a steward.
- **Classification:** every column carries a sensitivity tag, and CI rejects untagged columns.
- **PII:** tokenised in silver, and detokenised only through the vault service, with a stated purpose and a log entry.
- **Fine-grained access:** row and column rules are enforced through governed views and ClickHouse row policies.
- **Access requests:** time-limited, owner-approved grants, reviewed quarterly.
- **Access audit:** logs shipped to append-only storage.
- **Contracts:** kept as code.
- **Data localisation:** storage, compute and DR all stay in Indian regions.

## Consequences

- **Alert thresholds are starting values,** to be tuned against real baselines.
- **The exact scope of RBI data-localisation rules** must be confirmed with compliance.
- **Fine-grained access sits outside the catalog,** so it needs governed views kept in step with contracts.

## Alternatives considered

- **Monitoring only on failures (no SLOs):** it misses slow degradation, such as compaction falling behind.
- **Catalog-only access control:** too coarse for masking PII per column.
