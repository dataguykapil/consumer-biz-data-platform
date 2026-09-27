# 0007. Use Spark as the single processing engine; keep Flink outside the lake

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

Freshness needs vary by consumer, not by source. A lambda architecture, with separate streaming and batch pipelines both producing silver, tends to give two answers to the same question.

## Decision

Spark (Structured Streaming micro-batches of 1–5 minutes, plus batch) builds bronze, silver and gold. Flink is used only outside the lake, for fraud and live signals read directly from Kafka. It never writes to silver.

## Consequences

- **One codebase and one set of semantics for silver.** Spark also has the most complete Iceberg integration (`MERGE`, procedures, branches).
- **Lake freshness bottoms out at minutes.** Anything that needs sub-second freshness is served from the source or from Flink.

## Alternatives considered

- **Flink everywhere (Kappa):** true low latency, but a second codebase for batch files, and lighter Iceberg maintenance tooling.
- **Lambda (separate stream and batch pipelines):** two pipelines claiming to produce silver will eventually disagree.
