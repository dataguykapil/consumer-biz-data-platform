# 0022. Tier storage by age without breaking Iceberg, and delete through Iceberg at the end of retention

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

Five years of retention (≈200 TB) is mostly cold: after about 90 days, data is read by audits, restatements and occasional analyses. Cheaper storage classes save money, but two traps are specific to Iceberg:

1. **Unreadable data files.** Moving Iceberg data files to classes that need a restore (S3 Glacier Flexible Retrieval or Deep Archive) makes them unreadable. Any query or maintenance job that touches them fails.
2. **Lifecycle expiration corrupts tables.** Deleting objects with storage lifecycle *expiration* removes files the table metadata still references. Deletion has to go through Iceberg.

## Decision

| Data | 0–90 days | 90 days–5 years | End of retention |
|---|---|---|---|
| Kafka | 7 days, then gone | — | Bronze is the durable copy |
| Landing raw files (tokenised copies; ADR 0025) | S3 Standard | Glacier Instant Retrieval (read in milliseconds) up to 1 year, then **Deep Archive**, which needs a restore run through a runbook | Deleted by a lifecycle rule (these aren't Iceberg files) |
| Iceberg data (bronze, silver, gold) | S3 Standard | **Standard-IA or Glacier Instant Retrieval only**, as a lifecycle *transition*, never *expiration* | Monthly job: `DELETE` whole partitions older than 5 years (a metadata-only delete), then `expire_snapshots` and `remove_orphan_files` physically remove the files |
| Month-end tags `close-YYYY-MM` | Kept | Kept, with `max-ref-age` = retention | Tag expires, and its files become eligible for cleanup |
| ClickHouse (BI) | Hot | TTL at **25 months** (enough for year-on-year dashboards) | Older queries go to Spark SQL on Iceberg, the source of truth |
| Cassandra (app reads) | Hot | Row TTL at **13 months** | Older history is served on request from the lake |
| Audit data (`ops.reconciliation_log`, access logs) | S3 with Object Lock (WORM), compliance mode | Same | 5 years, then released |

Further rules:

- **No compaction of cold data.** Compaction skips partitions older than 90 days. Rewriting them costs compute and triggers early-deletion charges (30 days minimum for IA, 90 days for Glacier IR).
- **Legal hold.** Records under legal hold are copied to a `legal_hold` table before the retention delete. The delete's predicate excludes nothing else.
- **DR and localisation.** DR copies (cross-region replication) and every storage tier stay in Indian regions (ADR 0018).

## Consequences

- **Cost falls:** most of the 200 TB sits in IA or Glacier IR, at roughly half the cost of Standard or less (list prices vary).
- **Everything stays queryable:** every Iceberg file remains readable without a restore, so time-travel within retention, audits and restatements all work.
- **Month-end tags pin files.** They keep alive files that compaction replaced, so the extra storage for 60 tags is budgeted.
- **Raw files older than 1 year** need a restore (hours) before they can be re-parsed.

## Alternatives considered

- **Everything in Standard:** simplest, but roughly twice the storage bill for data that's rarely read.
- **Iceberg files in Deep Archive:** cheapest, but breaks tables.
- **Separate archive tables in cold storage:** workable, but it needs a rewrite pipeline and a restore step before anyone can query old periods.
