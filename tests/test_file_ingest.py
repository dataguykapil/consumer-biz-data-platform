"""Tests for partner-file ingestion (design §7.2).

The manifest is faked in memory so tests can make it lose state, fail, or grant
the same claim twice. The data side is a real Iceberg table, because that is
where the guarantee has to hold.
"""

import json
import threading
from datetime import date, datetime, timedelta

import pytest

from cdp.file_ingest import SHA_PROPERTY, Delivery, FileContract, Outcome, as_of, ingest_file, sha256_of

DAY = date(2026, 9, 1)
T0 = datetime(2026, 9, 2, 6, 0)
COLUMNS = {"txn_id": "STRING", "business_date": "DATE", "txn_type": "STRING", "amount_paise": "BIGINT"}


class FakeManifest:
    def __init__(self, grant_every_claim=False, fail_on_mark=None):
        self.states, self.details = {}, {}
        self.grant_every_claim = grant_every_claim  # simulates expired leases / a lost manifest
        self.fail_on_mark = fail_on_mark
        self._lock = threading.Lock()

    def claim(self, sha, delivery, worker):
        with self._lock:
            if self.grant_every_claim or self.states.get(sha) in (None, "CLAIMED", "FAILED"):
                self.states[sha] = "CLAIMED"
                return True
            return False

    def mark(self, sha, state, detail=None):
        if state == self.fail_on_mark:
            self.fail_on_mark = None
            raise RuntimeError("worker died before recording the outcome")
        self.states[sha], self.details[sha] = state, detail


@pytest.fixture
def contract(spark, namespace):
    table = f"{namespace}.settlement"
    spark.sql(f"""
        CREATE TABLE {table} (
            txn_id STRING, business_date DATE, txn_type STRING, amount_paise BIGINT,
            partner STRING, file_sha256 STRING, file_generated_at TIMESTAMP,
            recorded_from TIMESTAMP, recorded_to TIMESTAMP)
        USING iceberg PARTITIONED BY (business_date)
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')
    """)
    return FileContract(table=table, columns=COLUMNS, key_columns=("txn_id",))


@pytest.fixture
def deliver(tmp_path):
    """Write a partner file plus its trailer. Rows are (txn_id, txn_type, amount_paise)."""
    counter = iter(range(1000))

    def write(rows, generated_at, header="txn_id,business_date,txn_type,amount_paise", day=DAY, **trailer):
        n = next(counter)
        path, trailer_path = tmp_path / f"acme_{n}.csv", tmp_path / f"acme_{n}.trailer.json"
        body = [header] + [f"{t},{day},{kind},{amount}" for t, kind, amount in rows]
        path.write_text("\n".join(body) + "\n")
        declared = {
            "row_count": len(rows),
            "total_paise": sum(int(a) for _, _, a in rows if str(a).lstrip("-").isdigit()),
            "distinct_keys": len({t for t, _, _ in rows}),
            "generated_at": generated_at.isoformat(),
        }
        trailer_path.write_text(json.dumps({**declared, **trailer}))
        return Delivery("acme", DAY, str(path), str(trailer_path))

    return write


def current(spark, contract):
    spark.catalog.refreshTable(contract.table)  # ingest commits from its own session
    rows = spark.table(contract.table).where("recorded_to IS NULL").collect()
    return sorted((r.txn_id, r.amount_paise) for r in rows)


V1 = [("t1", "PAYMENT", 10_000), ("t2", "PAYMENT", 25_000), ("t3", "REFUND", -5_000)]
V2 = [("t1", "PAYMENT", 10_000), ("t2", "PAYMENT", 20_000)]  # t2 corrected, t3 withdrawn


def test_same_file_twice_publishes_once(spark, contract, deliver):
    # Would fail if dedup lived only in the manifest: here the manifest has lost its state.
    file = deliver(V1, generated_at=T0)
    assert ingest_file(spark, file, contract, FakeManifest(), "w1", recorded_at=T0) == Outcome.LOADED
    assert (
        ingest_file(spark, file, contract, FakeManifest(), "w1", recorded_at=T0 + timedelta(hours=1))
        == Outcome.ALREADY_LOADED
    )
    assert current(spark, contract) == [("t1", 10_000), ("t2", 25_000), ("t3", -5_000)]
    assert spark.table(contract.table).count() == 3


def test_crash_after_commit_then_retry_does_not_duplicate(spark, contract, deliver):
    # Would fail if "already loaded" were recorded outside the data commit: the
    # worker dies after the MERGE but before the manifest says LOADED.
    file = deliver(V1, generated_at=T0)
    manifest = FakeManifest(fail_on_mark="LOADED")
    with pytest.raises(RuntimeError):
        ingest_file(spark, file, contract, manifest, "w1", recorded_at=T0)
    assert manifest.states[sha256_of(file.path)] == "CLAIMED"

    assert (
        ingest_file(spark, file, contract, manifest, "w2", recorded_at=T0 + timedelta(hours=1))
        == Outcome.ALREADY_LOADED
    )
    spark.catalog.refreshTable(contract.table)
    assert spark.table(contract.table).count() == 3
    commits_with_sha = spark.sql(f"SELECT summary FROM {contract.table}.snapshots").collect()
    assert sum(1 for c in commits_with_sha if c.summary.get(SHA_PROPERTY) == sha256_of(file.path)) >= 1


def race(spark, contract, deliveries):
    """Run one ingest per delivery at the same time, with every claim granted."""
    manifest, outcomes, errors = FakeManifest(grant_every_claim=True), [], []

    def run(d, worker):
        try:
            outcomes.append(ingest_file(spark, d, contract, manifest, worker, recorded_at=T0 + timedelta(hours=worker)))
        except Exception as e:  # surfaced below; a thread must not swallow it
            errors.append(e)

    threads = [threading.Thread(target=run, args=(d, i)) for i, d in enumerate(deliveries, start=1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    return outcomes


def test_two_workers_racing_on_the_same_file_publish_once(spark, contract, deliver):
    # Would fail if the "already loaded?" check ran before, not inside, the MERGE:
    # both workers pass the check and both insert.
    file = deliver(V1, generated_at=T0)
    for _ in range(3):
        race(spark, contract, [file, file])
        assert current(spark, contract) == [("t1", 10_000), ("t2", 25_000), ("t3", -5_000)]
        assert spark.table(contract.table).count() == 3


def test_two_versions_racing_leave_only_the_newer_current(spark, contract, deliver):
    # Would fail if either order of commits could leave both versions current:
    # the day's money would be counted twice.
    v1, v2 = deliver(V1, generated_at=T0), deliver(V2, generated_at=T0 + timedelta(hours=4))
    race(spark, contract, [v1, v2])
    assert current(spark, contract) == [("t1", 10_000), ("t2", 20_000)]


@pytest.mark.parametrize(
    "bad, reason",
    [
        (dict(rows=[("t1", "PAYMENT", "12.50")]), "amount_paise are not BIGINT"),
        (dict(rows=V1, total_paise=999), "!= trailer"),
        (dict(rows=V1 + [("t1", "PAYMENT", 1)]), "duplicate keys"),
        (dict(rows=V1, day=date(2026, 8, 31)), "not dated 2026-09-01"),
        (dict(rows=V1, header="txn_id,business_date,amount_paise,txn_type"), "header"),
    ],
)
def test_invalid_file_is_quarantined_and_publishes_nothing(spark, contract, deliver, bad, reason):
    # Would fail if validation ran row by row and loaded the good rows.
    file = deliver(generated_at=T0, **bad)
    manifest = FakeManifest()
    assert ingest_file(spark, file, contract, manifest, "w1", recorded_at=T0) == Outcome.QUARANTINED
    spark.catalog.refreshTable(contract.table)
    assert spark.table(contract.table).count() == 0
    assert reason in manifest.details[sha256_of(file.path)]


def test_restatement_replaces_current_and_keeps_what_was_known(spark, contract, deliver):
    # Would fail with fingerprint-only dedup (v2 loads next to v1, t1 counted twice),
    # or if history lived only in Iceberg snapshots, which are expired.
    v1, v2 = deliver(V1, generated_at=T0), deliver(V2, generated_at=T0 + timedelta(hours=4))
    ingest_file(spark, v1, contract, FakeManifest(), "w1", recorded_at=T0)
    assert ingest_file(spark, v2, contract, FakeManifest(), "w1", recorded_at=T0 + timedelta(hours=5)) == Outcome.LOADED

    assert current(spark, contract) == [("t1", 10_000), ("t2", 20_000)]
    between = as_of(spark, contract.table, T0 + timedelta(hours=2)).collect()
    assert sorted((r.txn_id, r.amount_paise) for r in between) == [("t1", 10_000), ("t2", 25_000), ("t3", -5_000)]
    assert as_of(spark, contract.table, T0 - timedelta(hours=1)).count() == 0


@pytest.mark.parametrize("order", [("v1", "v2", "v1"), ("v2", "v1")])
def test_stale_version_never_becomes_current(spark, contract, deliver, order):
    # Would fail if "newest to arrive wins": a resent or late v1 would overwrite v2.
    files = {"v1": deliver(V1, generated_at=T0), "v2": deliver(V2, generated_at=T0 + timedelta(hours=4))}
    outcomes = [
        ingest_file(spark, files[name], contract, FakeManifest(), "w1", recorded_at=T0 + timedelta(hours=i))
        for i, name in enumerate(order, start=1)
    ]
    assert outcomes[-1] == Outcome.SUPERSEDED
    assert current(spark, contract) == [("t1", 10_000), ("t2", 20_000)]
