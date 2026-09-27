"""Tests for the CDC merge (design §7.1).

Each test notes the broken design it would catch. The central property: any
arrival order and any redelivery converge to the state implied by LSN order.
"""

import itertools

import pytest

from cdp.cdc_merge import TOAST_PLACEHOLDER, WATERMARK_PROPERTY, CdcTable, apply_cdc_batch

EVENT_SCHEMA = "loan_id BIGINT, status STRING, principal_paise BIGINT, notes STRING, _op STRING, _lsn BIGINT"


@pytest.fixture
def loans(spark, namespace):
    name = f"{namespace}.loan"
    spark.sql(f"""
        CREATE TABLE {name} (
            loan_id BIGINT, status STRING, principal_paise BIGINT, notes STRING,
            _lsn BIGINT, _is_deleted BOOLEAN)
        USING iceberg
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')
    """)
    return CdcTable(
        name=name,
        key_columns=("loan_id",),
        value_columns=("status", "principal_paise", "notes"),
        toastable_columns=("notes",),
    )


def apply(spark, table, *events):
    """Apply one batch. Each event is (loan_id, status, principal_paise, notes, op, lsn)."""
    return apply_cdc_batch(spark.createDataFrame(list(events), EVENT_SCHEMA), table)


def rows(spark, table):
    collected = spark.table(table.name).collect()
    by_key = {r.loan_id: (r.status, r.principal_paise, r.notes, r._lsn, r._is_deleted) for r in collected}
    assert len(by_key) == len(collected), "silver must hold exactly one row per key"
    return by_key


APPROVED = (1, "APPROVED", 5_000_00, "kyc ok", "c", 101)
REJECTED = (1, "REJECTED", 5_000_00, "kyc ok", "u", 102)
DISBURSED = (1, "DISBURSED", 5_000_00, "kyc ok", "u", 103)
DELETED = (1, None, None, None, "d", 104)


def test_replayed_older_change_does_not_overwrite_newer(spark, loans):
    # Would fail if the MERGE trusted arrival order: 102 arrives last and "wins".
    apply(spark, loans, APPROVED)
    apply(spark, loans, DISBURSED)
    apply(spark, loans, REJECTED)
    assert rows(spark, loans) == {1: ("DISBURSED", 5_000_00, "kyc ok", 103, False)}


@pytest.mark.parametrize("order", list(itertools.permutations([APPROVED, DISBURSED, DELETED])))
def test_every_arrival_order_converges_to_lsn_order(spark, namespace, loans, order):
    # Would fail on any order-dependence, including a delete that loses its LSN
    # and lets a later-arriving older update re-insert the row.
    for event in order:
        apply(spark, loans, event)
    assert rows(spark, loans) == {1: (None, None, None, 104, True)}


def test_delete_then_stale_update_does_not_resurrect(spark, loans):
    # Would fail with hard deletes: the stale update finds no row and inserts it.
    apply(spark, loans, APPROVED)
    apply(spark, loans, DELETED)
    apply(spark, loans, REJECTED)
    assert rows(spark, loans)[1][4] is True
    live = spark.table(loans.name).where("NOT _is_deleted").count()
    assert live == 0


def test_delete_carrying_values_gives_same_tombstone_in_any_order(spark, namespace, loans):
    # With REPLICA IDENTITY FULL, Debezium deletes carry the old row. Would fail if
    # tombstones kept values: their content would then depend on arrival order.
    full_delete = (1, "DISBURSED", 5_000_00, "kyc ok", "d", 104)
    apply(spark, loans, full_delete)
    apply(spark, loans, APPROVED)
    delete_first = rows(spark, loans)
    spark.sql(f"DELETE FROM {loans.name}")
    apply(spark, loans, APPROVED)
    apply(spark, loans, full_delete)
    assert rows(spark, loans) == delete_first == {1: (None, None, None, 104, True)}


def test_delete_for_unseen_key_blocks_later_stale_insert(spark, loans):
    # The delete is the first thing silver sees, e.g. a replay starting mid-stream.
    # Would fail if deletes of unknown keys were dropped instead of stored as tombstones.
    apply(spark, loans, DELETED)
    apply(spark, loans, APPROVED)
    assert rows(spark, loans) == {1: (None, None, None, 104, True)}


def test_key_reused_after_delete_is_live_again(spark, loans):
    # Would fail if tombstones were permanent: a genuinely newer insert must win.
    apply(spark, loans, APPROVED, DELETED)
    apply(spark, loans, (1, "APPROVED", 7_000_00, "re-applied", "c", 110))
    assert rows(spark, loans) == {1: ("APPROVED", 7_000_00, "re-applied", 110, False)}


def test_redelivered_batch_is_a_no_op(spark, loans):
    # Would fail if idempotency relied on remembering batch ids that a restarted
    # query no longer has.
    batch = [APPROVED, (2, "APPROVED", 1_000_00, "n", "c", 105)]
    apply(spark, loans, *batch)
    before = rows(spark, loans)
    apply(spark, loans, *batch)
    assert rows(spark, loans) == before


def test_many_changes_and_duplicates_for_one_key_in_one_batch(spark, loans):
    # Would fail without in-batch reduction: MERGE rejects several source rows
    # matching one target row, and picking one arbitrarily is non-deterministic.
    apply(spark, loans, DISBURSED, APPROVED, REJECTED, DISBURSED)
    assert rows(spark, loans) == {1: ("DISBURSED", 5_000_00, "kyc ok", 103, False)}


def test_many_changes_for_an_existing_key_in_one_batch(spark, loans):
    # Same as above against an existing row, where MERGE's one-match-per-row rule applies.
    apply(spark, loans, APPROVED)
    apply(spark, loans, REJECTED, DELETED, DISBURSED)
    assert rows(spark, loans) == {1: (None, None, None, 104, True)}


SNAPSHOT_READ_AT_103 = (1, "APPROVED", 5_000_00, "kyc ok", "r", 103)


def test_snapshot_read_loses_to_change_at_same_lsn_in_one_batch(spark, loans):
    # Would fail with an arbitrary tie-break inside the batch.
    apply(spark, loans, SNAPSHOT_READ_AT_103, DISBURSED)
    assert rows(spark, loans)[1][0] == "DISBURSED"


def test_equal_lsn_never_overwrites(spark, loans):
    # Would fail with `>=` in the guard: a later-arriving event at an LSN already
    # applied must not replace what is there.
    apply(spark, loans, DISBURSED)
    apply(spark, loans, SNAPSHOT_READ_AT_103)
    assert rows(spark, loans)[1][0] == "DISBURSED"


def test_toast_placeholder_keeps_previous_value(spark, loans):
    # Would fail if the placeholder string were written over the real value.
    apply(spark, loans, APPROVED)
    apply(spark, loans, (1, "DISBURSED", 5_000_00, TOAST_PLACEHOLDER, "u", 103))
    assert rows(spark, loans)[1][2] == "kyc ok"


def test_watermark_is_committed_with_the_data(spark, loans):
    # Would fail if the watermark were written separately and could disagree with the data.
    assert apply(spark, loans, APPROVED, DISBURSED) == 103
    latest = spark.sql(
        f"SELECT summary FROM {loans.name}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()
    assert latest.summary[WATERMARK_PROPERTY] == "103"
    assert spark.conf.get("spark.sql.iceberg.snapshot-property." + WATERMARK_PROPERTY, None) is None


def test_empty_batch_commits_nothing(spark, loans):
    apply(spark, loans, APPROVED)
    snapshots = spark.sql(f"SELECT * FROM {loans.name}.snapshots").count()
    assert apply(spark, loans) is None
    assert spark.sql(f"SELECT * FROM {loans.name}.snapshots").count() == snapshots


def test_unsupported_op_fails_and_commits_nothing(spark, loans):
    # Would fail if a truncate ('t') were silently treated as an update.
    apply(spark, loans, APPROVED)
    before = rows(spark, loans)
    with pytest.raises(Exception, match="unsupported CDC op: t"):
        apply(spark, loans, (1, None, None, None, "t", 200))
    assert rows(spark, loans) == before
