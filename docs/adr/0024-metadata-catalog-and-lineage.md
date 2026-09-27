# 0024. Use OpenMetadata as the business catalog and lineage store, fed by OpenLineage and the contracts

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

Two different catalogs are needed:

- **A technical catalog** that engines use: tables, schemas, snapshots and access. That's Polaris (ADR 0004).
- **A business catalog** for people: discovery, owners, a glossary, classification, quality and lineage.

The first design drew "OpenLineage → Marquez". Marquez stores lineage but offers no discovery or glossary, so a second tool would be needed anyway.

## Decision

- **OpenMetadata** is the business catalog.
  - It ingests table metadata from Polaris.
  - It ingests lineage from **OpenLineage** events, emitted by the Spark jobs (the `openlineage-spark` listener) and by Airflow (its OpenLineage provider).
  - It ingests quality results from ADR 0023.
- **Lineage has three levels:**
  - *Table level:* job → table, from OpenLineage.
  - *Column level:* where the Spark integration supports it.
  - *Row level:* the `_source_position` columns (ADR 0005), which trace a gold number back to a WAL position or a byte range in a file.
- **Contracts are the source of truth.** Owners, descriptions, classification tags and glossary links live in the contract files in git. CI pushes them into OpenMetadata, and they're not edited in the UI, which prevents drift between code and catalog.
- **Marquez is dropped** from the design.

## Consequences

- **One place to answer the governance questions:** "What is this table, who owns it, is it healthy, where did it come from?"
- **Another service to run:** OpenMetadata plus its database and search index.
- **Integration versions to confirm:** check that `openlineage-spark` supports Spark 4.1, and confirm OpenMetadata's OpenLineage ingestion, before rollout.

## Alternatives considered

- **DataHub:** equally capable, with strong lineage. It needs Kafka, Elasticsearch and MySQL. We already run Kafka, so this was close; OpenMetadata was chosen for its built-in quality and glossary features and fewer moving parts.
- **Marquez only:** lineage without discovery or a glossary.
- **Amundsen:** less active.
- **Unity Catalog:** best inside Databricks.
