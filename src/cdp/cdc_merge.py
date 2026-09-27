"""Apply Debezium CDC events from bronze to a silver current-state Iceberg table.

Design: docs/design.md §7.1.

Guarantee: for every primary key, silver ends in the same state whatever order
the changes arrive in and however often they are redelivered. That state is the
source row as of the highest LSN applied.

How the guarantee is kept:
  * Changes are ordered by the source LSN (Debezium ``source.lsn``), never by
    arrival. Postgres row locks mean two committed changes to the same row carry
    increasing LSNs, and a snapshot read (``op = 'r'``) carries the slot's
    consistent-point LSN, below any change streamed after it.
  * Each batch is reduced to one change per key before merging, so MERGE never
    sees two source rows for one target row.
  * The MERGE only overwrites a row when the incoming LSN is strictly greater,
    so a replayed or stale change changes nothing.
  * Deletes become tombstones that keep their LSN, so a stale update that
    arrives after the delete loses the comparison instead of re-inserting.
  * The batch's highest LSN is written as a property of the same Iceberg commit,
    so the table's watermark and its data cannot disagree.

One weaker spot: a TOAST placeholder means "unchanged since the previous
change", and it resolves to whatever value silver currently holds. If that
previous change is still in flight, the resolved value is stale. So for tables
where TOAST columns matter, the real fix is ``REPLICA IDENTITY FULL`` at the
source (design §4.2); ``toastable_columns`` is only the fallback.

Bronze stores Debezium events already flattened: the key and value columns
(taken from ``after``, or from ``before`` for deletes), plus ``_op`` and ``_lsn``.
Readers query live rows through a view with ``NOT _is_deleted``. Tombstones are
purged by a maintenance job once they are older than the replay horizon
(design §7.1, point 4), which is not shown here.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TOAST_PLACEHOLDER = "__debezium_unavailable_value"
WATERMARK_PROPERTY = "cdp.watermark.source_lsn"
_SNAPSHOT_PROPERTY_CONF = "spark.sql.iceberg.snapshot-property."
_ROW_OPS = ("c", "u", "d", "r")


@dataclass(frozen=True)
class CdcTable:
    """A silver current-state table fed by one source table's CDC stream.

    The silver table has the key and value columns plus ``_lsn BIGINT`` and
    ``_is_deleted BOOLEAN``. ``toastable_columns`` are the string columns that
    Debezium may send as a placeholder when the value didn't change (Postgres
    TOAST without ``REPLICA IDENTITY FULL``).
    """

    name: str
    key_columns: tuple[str, ...]
    value_columns: tuple[str, ...]
    toastable_columns: tuple[str, ...] = ()


def latest_change_per_key(events: DataFrame, table: CdcTable) -> DataFrame:
    """Reduce a batch to the single highest-LSN change per primary key.

    If a snapshot read and a streamed change share an LSN, the change wins. That
    keeps the result deterministic; ``row_number`` over tied keys would not be.
    Unknown ops (for example Debezium's truncate, ``'t'``) and missing LSNs raise
    when the batch executes, so the MERGE fails and nothing is committed.
    """
    checked_op = F.when(F.col("_op").isin(*_ROW_OPS), F.col("_op")).otherwise(
        F.raise_error(F.concat(F.lit("unsupported CDC op: "), F.coalesce(F.col("_op"), F.lit("null"))))
    )
    checked_lsn = F.when(F.col("_lsn").isNotNull(), F.col("_lsn")).otherwise(
        F.raise_error(F.lit("CDC event without _lsn"))
    )
    events = events.withColumn("_op", checked_op).withColumn("_lsn", checked_lsn)

    op_rank = F.when(F.col("_op") == "r", 0).otherwise(1)
    newest_first = Window.partitionBy(*table.key_columns).orderBy(F.col("_lsn").desc(), op_rank.desc())
    is_delete = F.col("_op") == "d"
    return (
        events.withColumn("_rank", F.row_number().over(newest_first))
        .where(F.col("_rank") == 1)
        .select(
            *table.key_columns,
            # A tombstone carries no values. "Last known values" would depend on
            # whether older changes arrived before the delete. History is in bronze.
            *(F.when(is_delete, F.lit(None)).otherwise(F.col(c)).alias(c) for c in table.value_columns),
            "_lsn",
            is_delete.alias("_is_deleted"),
        )
    )


def merge_sql(table: CdcTable, source_view: str) -> str:
    """The guarded MERGE. Only a strictly newer LSN may change a row."""

    def incoming(col: str, current: str) -> str:
        # A TOAST placeholder means "unchanged": keep the current value (NULL on insert).
        if col in table.toastable_columns:
            return f"CASE WHEN s.{col} = '{TOAST_PLACEHOLDER}' THEN {current} ELSE s.{col} END"
        return f"s.{col}"

    on = " AND ".join(f"t.{k} = s.{k}" for k in table.key_columns)
    updates = [f"t.{c} = {incoming(c, f't.{c}')}" for c in table.value_columns]
    updates += ["t._lsn = s._lsn", "t._is_deleted = s._is_deleted"]
    insert_cols = [*table.key_columns, *table.value_columns, "_lsn", "_is_deleted"]
    insert_vals = [
        *(f"s.{k}" for k in table.key_columns),
        *(incoming(c, "NULL") for c in table.value_columns),
        "s._lsn",
        "s._is_deleted",
    ]
    return f"""
        MERGE INTO {table.name} t
        USING {source_view} s
        ON {on}
        WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET {", ".join(updates)}
        WHEN NOT MATCHED THEN INSERT ({", ".join(insert_cols)}) VALUES ({", ".join(insert_vals)})
    """  # nosec B608: identifiers come from trusted config/validated ids, never row data


def apply_cdc_batch(events: DataFrame, table: CdcTable) -> int | None:
    """Merge one batch of CDC events into ``table``. Returns the batch's highest LSN.

    Safe to call again with the same batch: every change is then no newer than
    the row it matches, so nothing changes. That is why this does not track
    foreachBatch's ``batch_id``.

    It is not safe for two writers to merge into the same table at once; the
    design gives each silver table exactly one streaming writer.
    """
    spark = events.sparkSession
    changes = latest_change_per_key(events, table).cache()
    try:
        max_lsn = changes.agg(F.max("_lsn")).first()[0]
        if max_lsn is None:
            return None  # empty batch: no commit, watermark unchanged

        view = f"cdc_batch_{abs(hash(table.name))}"
        changes.createOrReplaceTempView(view)
        watermark_conf = _SNAPSHOT_PROPERTY_CONF + WATERMARK_PROPERTY
        # Scoped to this session: in a streaming query, foreachBatch runs on the
        # query's own cloned session, so the property reaches only this MERGE.
        spark.conf.set(watermark_conf, str(max_lsn))
        try:
            spark.sql(merge_sql(table, view))
        finally:
            spark.conf.unset(watermark_conf)
            spark.catalog.dropTempView(view)
        return max_lsn
    finally:
        changes.unpersist()


def start_cdc_stream(spark, bronze_table: str, source_entity: str, table: CdcTable, checkpoint: str):
    """Run the bronze → silver merge continuously. Bronze is append-only, so an
    Iceberg streaming read of new snapshots sees every event exactly as landed."""
    return (
        spark.readStream.format("iceberg")
        .load(bronze_table)
        .where(F.col("_source_entity") == source_entity)
        .writeStream.foreachBatch(lambda batch, _batch_id: apply_cdc_batch(batch, table))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="1 minute")
        .start()
    )
