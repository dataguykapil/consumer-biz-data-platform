"""Publish a gold table only after it reconciles to the paise; then prove the ClickHouse copy.

Design: docs/design.md §7.3.

Guarantee: consumers read ``main`` and only ever see a snapshot whose control
totals per (source_system, business_date) exactly match every independent
side: the source's own totals, and silver. ClickHouse serves a snapshot only
after its copy has reconciled to that same snapshot.

Write-audit-publish on an Iceberg branch:
  1. Branch off ``main`` and replace the run's whole business-date window on the branch.
  2. Compute control totals on the branch (rows, signed paise, distinct keys)
     and compare each side exactly. A date missing on either side is a mismatch.
  3. On a pass, ``fast_forward`` main to the branch in one atomic step. Iceberg
     refuses if main moved since the branch was cut, so a second writer is
     caught rather than overwritten.
  4. On a failure, main is untouched and the branch is kept for inspection.
     Every verdict goes to the reconciliation log.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GROUPING = ("source_system", "business_date")
_SNAPSHOT_PROPERTY_CONF = "spark.sql.iceberg.snapshot-property."
_MAX_REPORTED = 20
_RUN_ID = re.compile(r"^[A-Za-z0-9_]{1,64}$")


@dataclass(frozen=True)
class GoldTable:
    name: str  # catalog.namespace.table, partitioned by business_date
    key_columns: tuple[str, ...]
    amount_column: str = "amount_paise"


@dataclass
class GateResult:
    run_id: str
    branch: str
    snapshot_id: int
    published: bool
    mismatches: list[str] = field(default_factory=list)


def control_totals(rows: DataFrame, gold: GoldTable) -> DataFrame:
    """(source_system, business_date) -> rows, paise, keys. Every side must use this shape.

    Distinct keys catch a duplicated row paired with a dropped one, which leaves
    the row count unchanged. With ANSI mode on, an overflowing paise sum fails
    the run instead of wrapping.
    """
    return rows.groupBy(*GROUPING).agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(gold.amount_column).alias("paise"),
        F.count_distinct(*gold.key_columns).alias("keys"),
    )


def compare(actual: DataFrame, expected: DataFrame, side: str) -> list[str]:
    """Exact, null-safe comparison. A group present on only one side is a mismatch."""
    a, e = actual.alias("a"), expected.alias("e")
    joined = a.join(e, [*GROUPING], "full_outer")
    same = F.lit(True)
    for m in ("rows", "paise", "keys"):
        same = same & F.col(f"a.{m}").eqNullSafe(F.col(f"e.{m}"))
    return [
        f"{side}: {r.source_system} {r.business_date} gold={r.a_rows, r.a_paise, r.a_keys} "
        f"{side}={r.e_rows, r.e_paise, r.e_keys}"
        for r in joined.where(~same)
        .select(*GROUPING, *(F.col(f"{s}.{m}").alias(f"{s}_{m}") for s in "ae" for m in ("rows", "paise", "keys")))
        .limit(_MAX_REPORTED)
        .collect()
    ]


def publish_with_gate(
    spark: SparkSession,
    gold: GoldTable,
    rows: DataFrame,
    business_dates: list[date],
    expected: dict[str, DataFrame],
    run_id: str,
    log_table: str,
    watermarks: dict[str, str] | None = None,
) -> GateResult:
    """Write ``rows`` for ``business_dates`` on an audit branch and publish only if they reconcile.

    ``expected`` maps a side name ("source", "silver") to totals in the
    ``control_totals`` shape. Silver must be aggregated as known at the input
    watermarks recorded in ``watermarks``. Gold tables have exactly one writer.
    """
    if not _RUN_ID.match(run_id):
        raise ValueError(f"run_id must match {_RUN_ID.pattern}: {run_id!r}")
    branch = f"audit_{run_id}"
    catalog, ident = gold.name.split(".", 1)
    in_window = F.col("business_date").isin(business_dates)

    spark.sql(f"ALTER TABLE {gold.name} CREATE BRANCH `{branch}`")
    properties = {"cdp.run_id": run_id, **{f"cdp.watermark.{k}": v for k, v in (watermarks or {}).items()}}
    for key, value in properties.items():
        spark.conf.set(_SNAPSHOT_PROPERTY_CONF + key, value)
    try:
        # overwrite(window), not overwritePartitions(): a date with no rows this run must become
        # empty on the branch, not silently keep the previous run's rows.
        rows.where(in_window).writeTo(f"{gold.name}.branch_{branch}").overwrite(in_window)
    finally:
        for key in properties:
            spark.conf.unset(_SNAPSHOT_PROPERTY_CONF + key)
    snapshot_id = spark.sql(f"SELECT snapshot_id FROM {gold.name}.refs WHERE name = '{branch}'").first()[0]  # nosec B608: identifiers come from trusted config/validated ids, never row data

    audited = spark.read.option("versionAsOf", snapshot_id).table(gold.name).where(in_window)
    actual = control_totals(audited, gold).cache()
    try:
        mismatches = [m for side, totals in expected.items() for m in compare(actual, totals.where(in_window), side)]
    finally:
        actual.unpersist()

    published = not mismatches
    if published:
        # Raises if main moved since the branch was cut: a second writer, never retried blindly.
        spark.sql(f"CALL {catalog}.system.fast_forward('{ident}', 'main', '{branch}')")
        spark.sql(f"ALTER TABLE {gold.name} DROP BRANCH `{branch}`")
    result = GateResult(run_id, branch, snapshot_id, published, mismatches)
    _log(spark, log_table, gold, result, business_dates, properties)
    return result


def _log(
    spark: SparkSession,
    log_table: str,
    gold: GoldTable,
    result: GateResult,
    business_dates: list[date],
    properties: dict[str, str],
) -> None:
    spark.createDataFrame(
        [
            (
                result.run_id,
                gold.name,
                result.branch,
                result.snapshot_id,
                result.published,
                business_dates,
                result.mismatches,
                properties,
                datetime.now(timezone.utc),
            )
        ],
        "run_id STRING, table_name STRING, branch STRING, snapshot_id BIGINT, published BOOLEAN, "
        "business_dates ARRAY<DATE>, mismatches ARRAY<STRING>, properties MAP<STRING, STRING>, checked_at TIMESTAMP",
    ).writeTo(log_table).append()


# ClickHouse side. Rows carry the Iceberg snapshot's commit time as a version.
# The version is part of the sort key, so each load's rows stay separate: a load
# that fails reconciliation can't replace published rows. Re-loading the same
# version collapses onto itself. Serving views show only (business_date, version)
# pairs recorded as published, which also hides rows a restatement removed.
# Superseded versions are purged by a TTL/ALTER DELETE job, not shown here.
#
#   CREATE TABLE gold.<t> (..., _snapshot_seq UInt64)
#   ENGINE = ReplacingMergeTree ORDER BY (business_date, source_system, <keys>, _snapshot_seq);
#   CREATE VIEW serving.<t> AS SELECT * FROM gold.<t> FINAL
#   WHERE (business_date, _snapshot_seq) IN (
#       SELECT business_date, max(snapshot_seq) FROM cdp_meta.published_versions
#       WHERE table_name = '<t>' GROUP BY business_date);


def sync_to_clickhouse(
    spark: SparkSession,
    gold: GoldTable,
    snapshot_id: int,
    business_dates: list[date],
    ch,
    ch_table: str,
    batch_rows: int = 100_000,
) -> list[str]:
    """Load a published snapshot's dates into ClickHouse, reconcile, then make them visible.

    ``ch`` is a ``clickhouse_connect`` client. Re-running is safe: the same
    version replaces itself. At production volume the insert would be ClickHouse
    reading the snapshot's Parquet via ``s3()``; the reconciliation is unchanged.
    """
    committed_at = spark.sql(
        f"SELECT committed_at FROM {gold.name}.snapshots WHERE snapshot_id = {snapshot_id}"  # nosec B608: identifiers come from trusted config/validated ids, never row data
    ).first()[0]
    seq = int(committed_at.timestamp() * 1000)
    rows = (
        spark.read.option("versionAsOf", snapshot_id)
        .table(gold.name)
        .where(F.col("business_date").isin(business_dates))
    )
    columns = [*rows.columns, "_snapshot_seq"]
    batch = []
    for row in rows.toLocalIterator():
        batch.append([*row, seq])
        if len(batch) == batch_rows:
            ch.insert(ch_table, batch, column_names=columns)
            batch = []
    if batch:
        ch.insert(ch_table, batch, column_names=columns)

    keys = ", ".join(gold.key_columns)
    served = ch.query(
        f"SELECT source_system, business_date, count(), sum({gold.amount_column}), uniqExact({keys}) "  # nosec B608: identifiers come from trusted config/validated ids, never row data
        f"FROM {ch_table} FINAL WHERE _snapshot_seq = {{seq:UInt64}} "
        f"AND business_date IN {{dates:Array(Date)}} GROUP BY source_system, business_date",
        parameters={"seq": seq, "dates": business_dates},
    ).result_rows
    ch_totals = spark.createDataFrame(
        served, "source_system STRING, business_date DATE, rows BIGINT, paise BIGINT, keys BIGINT"
    )
    mismatches = compare(control_totals(rows, gold), ch_totals, "clickhouse")
    if not mismatches:
        ch.insert(
            "cdp_meta.published_versions",
            [[ch_table, d, seq] for d in business_dates],
            column_names=["table_name", "business_date", "snapshot_seq"],
        )
    return mismatches
