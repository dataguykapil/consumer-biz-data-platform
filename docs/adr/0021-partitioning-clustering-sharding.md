# 0021. Partition by time and bucket by key; shard only the serving stores

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

A good layout makes queries prune to a few files and keeps concurrent writers out of each other's way. A bad one causes small-file explosions, hot partitions and full scans. In the lake, "sharding" means how files are laid out; in the serving stores it means real distribution across nodes. Iceberg's hidden partitioning and partition evolution mean a layout can change later without rewriting history.

## Principles

- **Time and key:** partition on how data is written and pruned (usually time), and **bucket** high-cardinality keys. Never partition by the key itself: 50M customers would mean 50M partitions.
- **Size:** aim for files of 256–512 MB. A partition too small to reach that is a sign of over-partitioning.
- **Monitoring:** watch small-file and delete-file counts per partition (design Appendix C), and evolve the spec when the metrics say so.

## Decision

| Data | Partition / distribution | Sort / cluster | Why |
|---|---|---|---|
| Kafka CDC topics | Key = primary key; 3–6 partitions per table, 24 on busy ones | — | Per-key order and parallelism (the merge doesn't rely on order; ADR 0012) |
| Kafka event topics | Key = `customer_id`; 24 partitions | — | Spreads load; keeps a customer's events together |
| Landing | Object prefix `source/yyyy/mm/dd/` | — | Lifecycle rules and replay by day |
| Bronze | `days(_ingested_at)`; `hours(_ingested_at)` for high-volume CDC | `_source_entity` | Append-only, pruned by load time; the replay unit is a time range |
| Silver entities (CDC current state) | `bucket(64, customer_id)`, `bucket(32, loan_id)`; **no time partition** | Primary key | Updates land anywhere in history; buckets limit each `MERGE` to the files it touches |
| Silver file facts | `days(business_date)` + `identity(partner)` | `customer_id` | One partition per delivery: restatement prunes to it, and commit conflicts stay within it (ADR 0013) |
| Silver event facts | `days(business_date)` | `customer_id` | Late events land in their business day |
| Gold marts | `days(business_date)`, or `months(…)` for small marts | Main filter columns | The publish gate replaces a date window (ADR 0014) |
| Gold feature tables | `days(as_of_date)` + `bucket(16, customer_id)` | `customer_id` | Point-in-time training reads by date |
| ClickHouse (BI) | `PARTITION BY toYYYYMM(business_date)`; one shard with 2 replicas to start, then `Distributed` over `cityHash64(customer_id)` once data per node passes a few TB | `ORDER BY (source_system, business_date, …, _snapshot_seq)` | Monthly partitions make TTL cheap; shard only when a single node can't hold the data |
| Cassandra (app reads) | Partition key `(customer_id, month)`; replication factor 3 | `event_ts DESC` | Bounded partition size per customer; newest first |

## Consequences

- **Pruning:** queries by date or customer prune to a handful of files, and restatements and streaming merges conflict only within their own partition or bucket.
- **Bucket counts are fixed per spec.** Changing them is a partition evolution, cheap in metadata, but old data keeps its old layout until it's rewritten.
- **Small partners** produce small `(date, partner)` partitions. Compaction merges their files, and if there are thousands of partners, `identity(partner)` becomes `bucket(n, partner)`.

## Alternatives considered

- **Partitioning silver entities by update time:** every `MERGE` would touch many partitions, and a current-state table has no useful time filter.
- **Identity-partitioning on `customer_id`:** millions of tiny partitions.
- **Sharding ClickHouse from day one:** cross-shard queries and extra operations before any data volume needs it.
