# 0005. Land everything in an append-only bronze layer under one ingestion envelope

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

The sources behave very differently: Postgres CDC, Kafka events, partner files, third-party APIs, and hand-edited spreadsheets. If each source had its own landing conventions, questions like "where did this row come from?" and "how fresh is it?" would each need a different answer.

## Decision

- **Landing** keeps raw files and API responses exactly as received, addressed by sha256.
- **Bronze** is append-only, and every row carries `_source_system`, `_source_entity`, `_source_position` (the LSN, `topic:partition:offset`, or `sha256:row`), `_source_ts`, `_ingested_at`, `_ingest_batch_id` and `_schema_version`.
- **Silver** holds current-state entities (from CDC) and bitemporal facts (from files and events).
- **Gold** is published only through the gate.
- **Ordering guarantee:** each source's contract declares it as `lsn`, `source_ts` or `none`, and the silver merge rule follows that declaration.

## Consequences

- **Bronze is the replay point:** every silver and gold table can be rebuilt from it (Kafka keeps only 7 days).
- **Lineage is row-level:** `_source_position` traces a gold number back to a WAL position or a byte range in a file.
- **Storage cost:** bronze is the largest layer, so it's tiered to infrequent-access storage after 90 days.

## Alternatives considered

- **Per-source bronze conventions:** less upfront work, but freshness and lineage can no longer be answered uniformly.
- **Skipping bronze and merging straight into silver:** no replay point once Kafka retention has passed.
