# 0017. Use Airflow for batch orchestration

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

File loads, API pulls, gold builds and table maintenance need scheduling and retries. Streaming jobs run continuously on Kubernetes.

## Decision

Airflow orchestrates batch work. Freshness is tracked through table watermarks (snapshot properties), not scheduler state.

## Consequences

- **Widely known, with a large ecosystem.**
- **Freshness is tracked per task, not per data asset,** which the table watermarks make up for.

## Alternatives considered

- **Dagster:** its asset-based model fits freshness tracking well; it would be a reasonable choice on a greenfield team.
- **Prefect:** a pleasant Python API, but a smaller data-platform ecosystem.
