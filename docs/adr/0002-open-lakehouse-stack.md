# 0002. Build on an open lakehouse stack rather than a managed platform

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

The platform serves lending, insurance and recharge, at about 10k events/s at peak, 500M file rows/day, 50M customers and 5 years of history. Three stack shapes were compared: open and self-operated; a managed platform such as Databricks; and one cloud's native services.

## Decision

Use an open stack: Iceberg tables on object storage, a Polaris REST catalog, Debezium and Kafka for ingestion, Spark for processing, and ClickHouse for serving.

## Consequences

- **Portability:** there's no single vendor roadmap, and any engine that speaks Iceberg and the REST catalog can be added later.
- **Operations:** we run the catalog, Kafka, Spark on Kubernetes and ClickHouse ourselves. That needs a small platform team, which is the main cost of this choice.
- **The core guarantees don't depend on the vendor:** LSN-ordered merges, restatement in a single `MERGE`, and the publish gate would carry over to a managed platform.

## Alternatives considered

- **Databricks (Delta + Unity Catalog):** lowest operational load, with governance and lineage largely built in. Rejected because of vendor dependence and compute cost at this scale. It would be the right choice for a much smaller team.
- **Single-cloud native (for example S3 Tables, Glue, EMR):** less integration work, but tied to one provider's pace and pricing.
