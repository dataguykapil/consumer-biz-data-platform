# 0012. Merge CDC into silver ordered by LSN, with tombstones

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

Connector restarts, snapshot overlap and replays from bronze deliver older changes after newer ones. A plain `MERGE` on the key trusts arrival order, lets deleted rows come back, and fails when a batch holds several changes to one key.

## Decision

Implemented in `src/cdp/cdc_merge.py`:
- **Order key:** changes are ordered by source LSN. At an equal LSN, a change beats a snapshot read.
- **In-batch reduction:** each batch is reduced to one change per key.
- **Guarded merge:** `WHEN MATCHED AND s._lsn > t._lsn`. It must be strict; `>=` lets an event re-sent at an already-applied LSN overwrite what's there.
- **Tombstones:** deletes become tombstones that keep their LSN and clear their values. A delete for a key never seen is stored too.
- **Watermark:** the batch's highest LSN is written as a snapshot property of the same commit.
- **Sources without a usable ordering field** get a weaker, declared guarantee instead of an invented order.

## Consequences

- **Order-independent:** any arrival order and any redelivery converge to the source state. 20 tests cover this, and each mechanism was broken on purpose to show a test fails.
- **Tombstones need a purge job** after the replay horizon. This isn't built yet.
- **No cross-table consistency:** gold reads at the minimum watermark of its inputs.
- **Completeness isn't detectable here,** because LSN gaps are normal. It's proven by reconciliation (ADR 0014).

## Alternatives considered

- **Arrival-order merge with hard deletes:** silently wrong under replay.
- **Tracking processed batch ids:** breaks when a query restarts and loses its checkpoint.
