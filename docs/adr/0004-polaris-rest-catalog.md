# 0004. Use Apache Polaris as the Iceberg REST catalog

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

With several engines, the catalog is the shared source of truth for table metadata and access. Our choice of catalog is harder to change later than our choice of table format.

## Decision

Use Apache Polaris, which implements the Iceberg REST catalog spec, for all engines. It runs highly available on a backed-up Postgres.

## Consequences

- **Any REST-capable engine can attach** without catalog-specific plugins.
- **Access control and storage credentials:** it has RBAC per namespace and table, and gives engines short-lived storage credentials.
- **Commits stop if Polaris is down.** Reads of already-resolved snapshots continue, and the design accepts this.
- **Fine-grained rules live elsewhere.** Row- and column-level rules aren't enforced by the catalog; they live in governed views and ClickHouse policies (ADR 0018).

## Alternatives considered

- **AWS Glue:** managed, but ties us to one cloud.
- **Unity Catalog:** strongest inside Databricks.
- **Hive Metastore:** legacy, with no REST spec and no credential vending.
- **Nessie or Lakekeeper:** viable open REST catalogs. Nessie's multi-table commits weren't needed.
