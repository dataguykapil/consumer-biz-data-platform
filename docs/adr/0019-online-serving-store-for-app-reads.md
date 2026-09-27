# 0019. Serve app derived reads from an online serving store; keep ClickHouse for BI only

- **Status:** Accepted. Supersedes the app-read part of [0008](0008-clickhouse-serving.md)
- **Date:** 2026-09-27

## Context

The design had apps read *derived* data from ClickHouse: transaction history, customer summaries, offer eligibility. A review questioned the arrow from ClickHouse (an OLAP engine) to Applications. The concern holds up:

- **Different workloads.** App traffic means thousands of small queries a second, a strict p99 and 24x7 availability. ClickHouse is built for fewer, heavier analytical queries. Its concurrency is bounded per server, and a heavy BI query competes with app lookups for CPU and memory.
- **Shared blast radius.** Analysts and customer-facing screens would share one cluster, so a bad dashboard query degrades the app.
- **Access pattern.** Almost every app read is "everything for customer X, newest first". That's a key-value, wide-row access pattern, not an analytical one.

Money-authoritative reads (balances, amounts due) already come from the owning OLTP service (ADR 0010). This ADR is about derived reads only.

## Options considered

| Option | For | Against |
|---|---|---|
| Keep shared ClickHouse | One technology, simplest | BI and app workloads interfere; OLAP concurrency limits; app p99 at the mercy of analysts |
| Separate ClickHouse cluster for apps | Isolation with one technology | Still an analytics engine under point-lookup traffic; needs a cache in front for spikes |
| **Online key-value store (chosen)** | Built for per-key reads at high concurrency with predictable p99; scales horizontally; isolated from BI | One more datastore to run; no atomic "flip" of a whole snapshot |
| Postgres read replicas per read model | Familiar | Scaling 50M customers × history to high QPS means manual sharding |

## Decision

- **The store:** app derived reads are served from **Apache Cassandra**. It's open source (Apache-2.0), wide-row, and multi-replica, so it fits "all rows for a customer, newest first".
- **Keys and tables:**
  - *History* is partitioned by `(customer_id, month)`, with rows clustered by `event_ts DESC`.
  - *Summaries* are one row per `customer_id`.
  - *Retention:* rows have a 13-month TTL (ADR 0022).
- **Cache:** a **Valkey** cache (the open-source Redis fork) may sit in front for the hottest summaries, only if measurements show it's needed.
- **Loading:** a reverse-ETL job loads **only published, reconciled gold snapshots** (ADR 0014), never branches or silver.
  - *Old snapshots can't win:* each write uses `USING TIMESTAMP <snapshot commit time>`, so a replay of an older snapshot can never overwrite a newer one. This is the same guard as the LSN rule in ADR 0012.
  - *Removed rows:* rows that a restatement removed are deleted with the same timestamp.
- **Reads:** apps read through the internal read API, which returns the gold watermark with every response.
- **Checking the copy:** the loader compares rows it has confirmed written, per (source, business_date), with the Iceberg snapshot's totals. A nightly job reads back a random sample of customers and compares them with Iceberg. Any drift raises an alert.
- **ClickHouse becomes BI-only:** dashboards and repeated analyst queries.

## Consequences

- **App latency no longer depends on analyst load,** and each store is used for the access pattern it's built for.
- **One more datastore to operate** (Cassandra, plus Valkey if added).
- **Weaker visibility guarantee than ClickHouse.** ClickHouse gets an atomic, version-based "flip". Here rows become visible as they're written, so during a load a customer can briefly see part of a day's update. That's acceptable for derived data, but it's weaker, and the design says so. Correctness is still established upstream by the gate, and drift is detected by the checks.
- **Cheaper checking:** the read API can compare row counts per customer against gold samples, rather than checking the whole store.

## Open questions

- **Capacity:** peak app QPS and the p99 target are assumptions (a few thousand QPS, under 20 ms). A load test decides node count and whether Valkey is needed.
- **Alternative engine:** ScyllaDB offers more throughput per node with the same data model. Check its current licence before choosing it.
