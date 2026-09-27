# 0015. Implement in PySpark and test against a real local Iceberg catalog

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

The guarantees depend on Iceberg's actual `MERGE`, snapshot, branch and commit-conflict behaviour. Mocks would test our assumptions about that behaviour, not the behaviour itself.

## Decision

The code is written in PySpark. Tests use pytest with a local SparkSession and a filesystem-backed Iceberg catalog, with no mocks for Iceberg. Each mechanism is also broken on purpose to prove at least one test fails.

## Consequences

- **The tests exercise real engine behaviour.** They found real bugs: order-dependent tombstones, the race in a pre-`MERGE` check, a `MERGE` that inserts nothing still committing a snapshot, and `overwritePartitions()` letting a missing date through.
- **The suite takes about 40 seconds and needs a JDK.**

## Alternatives considered

- **Scala Spark:** stronger typing, but a separate language from notebooks and tests.
- **Mocked Spark or Iceberg:** fast, but can't show the guarantees hold.
