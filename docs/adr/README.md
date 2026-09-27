# Architecture Decision Records

Each ADR records one decision: its context, what was decided, its consequences, and the alternatives rejected. Accepted ADRs are not rewritten: a change of mind is a new ADR that supersedes the old one. Use [`0000-template.md`](0000-template.md) for new records. The process is in [`CONTRIBUTING.md`](../../CONTRIBUTING.md#architecture-decisions).

| # | Decision | Status | Date |
|---|---|---|---|
| 0001 | [Record architecture decisions](0001-record-architecture-decisions.md) | Accepted | 2026-09-26 |
| 0002 | [Build on an open lakehouse stack rather than a managed platform](0002-open-lakehouse-stack.md) | Accepted | 2026-09-26 |
| 0003 | [Use Apache Iceberg as the table format](0003-iceberg-table-format.md) | Accepted | 2026-09-26 |
| 0004 | [Use Apache Polaris as the Iceberg REST catalog](0004-polaris-rest-catalog.md) | Accepted | 2026-09-26 |
| 0005 | [Land everything in an append-only bronze layer under one ingestion envelope](0005-layers-and-ingestion-envelope.md) | Accepted | 2026-09-26 |
| 0006 | [Capture Postgres changes with Debezium into Kafka](0006-debezium-kafka-cdc.md) | Accepted | 2026-09-26 |
| 0007 | [Use Spark as the single processing engine; keep Flink outside the lake](0007-spark-single-processing-engine.md) | Accepted | 2026-09-26 |
| 0008 | [Serve dashboards and derived app reads from ClickHouse, reconciled per snapshot](0008-clickhouse-serving.md) | Accepted | 2026-09-26 |
| 0009 | [Use Spark SQL, not Trino, for ad hoc and audit queries](0009-spark-sql-not-trino.md) | Accepted | 2026-09-27 |
| 0010 | [Serve money-authoritative reads from the owning OLTP service, never the lake](0010-money-reads-from-oltp.md) | Accepted | 2026-09-26 |
| 0011 | [Represent money as signed integer paise, with Spark ANSI mode on](0011-signed-integer-paise.md) | Accepted | 2026-09-26 |
| 0012 | [Merge CDC into silver ordered by LSN, with tombstones](0012-lsn-ordered-cdc-merge.md) | Accepted | 2026-09-26 |
| 0013 | [Decide and write partner-file loads in a single MERGE, with bitemporal history](0013-file-restatement-single-merge.md) | Accepted | 2026-09-27 |
| 0014 | [Publish gold through a write-audit-publish gate with exact reconciliation](0014-write-audit-publish-gate.md) | Accepted | 2026-09-26 |
| 0015 | [Implement in PySpark and test against a real local Iceberg catalog](0015-pyspark-real-iceberg-tests.md) | Accepted | 2026-09-26 |
| 0016 | [Pin the toolchain to the newest mutually compatible stable versions](0016-toolchain-versions.md) | Accepted | 2026-09-27 |
| 0017 | [Use Airflow for batch orchestration](0017-airflow-orchestration.md) | Accepted | 2026-09-26 |
| 0018 | [Adopt a baseline for observability and governance](0018-observability-governance-baseline.md) | Accepted | 2026-09-27 |
