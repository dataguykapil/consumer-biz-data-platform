# 0006. Capture Postgres changes with Debezium into Kafka

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

Each business runs PostgreSQL. The lake needs every change, including deletes, with the source ordering metadata that the silver merge depends on (ADR 0012).

## Decision

Use Debezium with `pgoutput`, one Kafka topic per table keyed by primary key, and Schema Registry with `BACKWARD` compatibility. Use `REPLICA IDENTITY FULL` on tables where TOAST columns matter.

## Consequences

- **Ordering metadata:** every change carries `source.lsn`, which is the order key.
- **At-least-once delivery:** replays and overlap between the initial snapshot and streaming are expected, and handled by the merge.
- **Highest-impact risk:** if the lake stops reading, the replication slot keeps WAL on the production primary until its disk fills. The mandatory guard rails are `max_slot_wal_keep_size`, a P1 alert at 50% of it, and a drop-and-re-snapshot runbook.

## Alternatives considered

- **AWS DMS:** managed, but coarser control over ordering metadata, and AWS only.
- **Fivetran or Airbyte:** fast to set up, but less control over ordering and delivery guarantees for core money tables.
- **Nightly dumps:** hours stale, and lose intermediate changes and deletes.
