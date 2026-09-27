# 0010. Serve money-authoritative reads from the owning OLTP service, never the lake

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

Apps show customers balances and amounts due. The lake is minutes behind and built for scans, not single-row reads with strict latency and exactness.

## Decision

Money-authoritative reads (current balance, amount due) come from the service that owns the money. The lake, ClickHouse included, serves only derived reads, and the read API returns a watermark with each response.

## Consequences

- **Apps have two read paths,** and the rule for choosing between them is explicit.
- **The lake is never a system of record for money a customer sees.**

## Alternatives considered

- **Apps reading balances from ClickHouse:** its row-level update guarantees don't suit this, and data would be minutes stale.
