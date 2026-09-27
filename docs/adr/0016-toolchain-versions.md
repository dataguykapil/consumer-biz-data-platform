# 0016. Pin the toolchain to the newest mutually compatible stable versions

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

The goal is the latest stable versions, but Spark, Iceberg and the JDK constrain each other.

## Decision

| Component | Version | Why not newer |
|---|---|---|
| PySpark | 4.1.3 | PySpark 4.2.0 exists, but Iceberg has no Spark 4.2 runtime yet |
| Iceberg | 1.11.0 (`iceberg-spark-runtime-4.1_2.13`) | Latest release |
| JDK | 21 LTS (Temurin) | JDK 25 is the newest LTS, but Spark 4.1 supports Java 17/21 |
| Python | 3.14 (CI also tests 3.12) | Latest; PySpark 4.1.3 lists 3.10–3.14 |
| pytest, ruff, pre-commit, bandit, pip-audit | Pinned in `pyproject.toml` | Latest at the time of writing |

Dependabot proposes updates weekly. It is configured to skip PySpark minor versions and JDK major versions, because those must move in lockstep with Iceberg and Spark and are bumped by hand.

## Consequences

- **Verified locally:** the suite passes on Python 3.14 with JDK 21. Spark's Python workers are pinned to the driver's interpreter, avoiding a minor-version mismatch.
- **Upgrading Spark means upgrading the Iceberg runtime too.**

## Alternatives considered

- **JDK 25:** the newest LTS, but outside Spark 4.1's supported set.
- **PySpark 4.2:** has no matching Iceberg runtime.
