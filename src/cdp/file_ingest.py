"""Load partner files into a bitemporal silver fact table.

Design: docs/design.md §7.2.

Guarantee, for a delivery (partner, business_date):
  * Retries, duplicate deliveries, a crash at any point, or two workers racing
    never produce duplicate published records.
  * An invalid file is quarantined whole and publishes nothing.
  * A corrected file replaces the previous version in one commit.
  * ``as_of(t)`` returns exactly what the table held at time ``t``.

Two identities are used. ``sha256(content)`` identifies the bytes.
``(partner, business_date)`` identifies the delivery, and a newer
``generated_at`` in the trailer makes a file a restatement of it. The whole
decision (skip, restate, or ignore as stale) runs inside the single MERGE that
writes the data. The check and the write therefore read the same snapshot, and
Iceberg's serializable isolation rejects the MERGE if another commit reached
that partition first. The manifest (Postgres) only schedules work and shows
state to operators; correctness never depends on it.

"The same snapshot" relies on Iceberg's catalog cache (on by default): both
references to the table inside one statement resolve to one cached table
object. Each ingest runs in its own Spark session, so it starts from fresh
metadata.
"""

from __future__ import annotations

import enum
import functools
import hashlib
import operator
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SHA_PROPERTY = "cdp.file.sha256"
_SNAPSHOT_PROPERTY_CONF = "spark.sql.iceberg.snapshot-property."
_PARTNER_ID = re.compile(r"^[a-z0-9_-]{1,64}$")
_COMMIT_ATTEMPTS = 3


class Outcome(enum.Enum):
    LOADED = "LOADED"                  # this call committed the file
    ALREADY_LOADED = "ALREADY_LOADED"  # these bytes were already committed
    SUPERSEDED = "SUPERSEDED"          # a newer version of the delivery is already loaded
    QUARANTINED = "QUARANTINED"
    NOT_CLAIMED = "NOT_CLAIMED"        # another worker holds the claim


@dataclass(frozen=True)
class FileContract:
    """Columns in file order. The silver table has these columns plus
    ``partner, file_sha256, file_generated_at, recorded_from, recorded_to``."""

    table: str
    columns: dict[str, str]
    key_columns: tuple[str, ...]
    amount_column: str = "amount_paise"
    date_column: str = "business_date"


@dataclass(frozen=True)
class Delivery:
    partner: str
    business_date: date
    path: str
    trailer_path: str


@dataclass(frozen=True)
class Trailer:
    row_count: int
    total_paise: int
    distinct_keys: int
    generated_at: datetime  # the partner's version of this delivery

    @classmethod
    def read(cls, path: str) -> Trailer:
        with open(path) as f:
            raw = json.load(f)
        return cls(int(raw["row_count"]), int(raw["total_paise"]), int(raw["distinct_keys"]),
                   datetime.fromisoformat(raw["generated_at"]))


class Manifest(Protocol):
    """``ops.file_manifest`` in Postgres; ``claim`` runs ``CLAIM_SQL``."""

    def claim(self, sha256: str, delivery: Delivery, worker: str) -> bool: ...
    def mark(self, sha256: str, state: str, detail: str | None = None) -> None: ...


# A lease, not a lock: a worker paused past lease_until can still reach the MERGE,
# which is why the MERGE carries its own guard.
CLAIM_SQL = """
    INSERT INTO ops.file_manifest (file_sha256, partner, business_date, state, claimed_by, lease_until)
    VALUES (%(sha)s, %(partner)s, %(business_date)s, 'CLAIMED', %(worker)s, now() + interval '15 minutes')
    ON CONFLICT (file_sha256) DO UPDATE
       SET state = 'CLAIMED', claimed_by = EXCLUDED.claimed_by, lease_until = EXCLUDED.lease_until
     WHERE ops.file_manifest.state IN ('DISCOVERED', 'FAILED')
        OR (ops.file_manifest.state = 'CLAIMED' AND ops.file_manifest.lease_until < now())
    RETURNING file_sha256
"""


def sha256_of(path: str, chunk: int = 8 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            digest.update(block)
    return digest.hexdigest()


def validate(raw: DataFrame, contract: FileContract, delivery: Delivery, trailer: Trailer) -> tuple[DataFrame, list[str]]:
    """Type the all-string file against its contract, in one aggregation pass.

    Returns (typed rows, failures). Any failure quarantines the whole file.
    Amounts are signed (refunds and reversals are negative), so there is no
    ``>= 0`` rule here.
    """
    if raw.columns != list(contract.columns):
        return raw, [f"header {raw.columns} != contract {list(contract.columns)}"]

    typed_cols = [F.col(c).try_cast(t).alias(c) for c, t in contract.columns.items()]
    both = raw.select(*(F.col(c).alias(f"_raw_{c}") for c in contract.columns), *typed_cols)
    any_null_key = functools.reduce(operator.or_, (F.col(k).isNull() for k in contract.key_columns))
    stats = both.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(contract.amount_column).alias("paise"),
        F.count_distinct(*contract.key_columns).alias("keys"),
        F.sum(any_null_key.cast("int")).alias("null_keys"),
        F.sum((F.col(contract.date_column) != F.lit(delivery.business_date)).cast("int")).alias("wrong_date"),
        *(F.sum((F.col(f"_raw_{c}").isNotNull() & F.col(c).isNull()).cast("int")).alias(f"bad_{c}")
          for c in contract.columns),
    ).first()

    failures = [f"{stats[f'bad_{c}']} values in {c} are not {t}"
                for c, t in contract.columns.items() if stats[f"bad_{c}"]]
    if stats["null_keys"]:
        failures.append(f"{stats['null_keys']} rows with a null key")
    if stats["keys"] != stats["rows"] - (stats["null_keys"] or 0):
        failures.append(f"duplicate keys: {stats['rows']} rows, {stats['keys']} distinct")
    if stats["wrong_date"]:
        failures.append(f"{stats['wrong_date']} rows not dated {delivery.business_date}")
    actual = (stats["rows"], stats["paise"] or 0, stats["keys"])
    declared = (trailer.row_count, trailer.total_paise, trailer.distinct_keys)
    if actual != declared:
        failures.append(f"(rows, paise, distinct keys) {actual} != trailer {declared}")
    return both.select(*contract.columns), failures


def restatement_merge_sql(contract: FileContract, delivery: Delivery, staged: str, params: str) -> str:
    """One MERGE that loads, restates or ignores, decided against the snapshot it writes.

    * Insert the file's rows only if no version of this delivery with the same
      or a newer ``generated_at`` exists. A replay or a stale resend inserts nothing.
    * Close the current rows of older versions only. A replay or a stale resend
      closes nothing.
    * The ON clause's partner/date literals prune the target scan to one
      partition, which also scopes Iceberg's conflict check to it.
    """
    if not _PARTNER_ID.match(delivery.partner):
        raise ValueError(f"invalid partner id {delivery.partner!r}")
    cols = list(contract.columns)
    delivery_filter = f"x.partner = p.partner AND x.{contract.date_column} = p.business_date"
    key_match = " AND ".join(f"t.{k} = s.{k}" for k in contract.key_columns)
    return f"""
        MERGE INTO {contract.table} t
        USING (
            SELECT 'insert' AS _action, {", ".join(f"f.{c}" for c in cols)}, p.partner, p.sha256, p.generated_at, p.recorded_at
            FROM {staged} f CROSS JOIN {params} p
            WHERE NOT EXISTS (SELECT 1 FROM {contract.table} x
                              WHERE {delivery_filter} AND x.file_generated_at >= p.generated_at)
            UNION ALL
            SELECT 'close', {", ".join(f"x.{c}" for c in cols)}, p.partner, p.sha256, p.generated_at, p.recorded_at
            FROM {contract.table} x JOIN {params} p ON {delivery_filter}
            WHERE x.recorded_to IS NULL AND x.file_generated_at < p.generated_at
        ) s
        ON t.partner = '{delivery.partner}' AND t.{contract.date_column} = DATE'{delivery.business_date.isoformat()}'
           AND s._action = 'close' AND t.recorded_to IS NULL AND {key_match}
        WHEN MATCHED THEN UPDATE SET t.recorded_to = s.recorded_at
        WHEN NOT MATCHED AND s._action = 'insert' THEN INSERT
            ({", ".join(cols)}, partner, file_sha256, file_generated_at, recorded_from, recorded_to)
            VALUES ({", ".join(f"s.{c}" for c in cols)}, s.partner, s.sha256, s.generated_at, s.recorded_at, NULL)
    """


def _commit(spark: SparkSession, contract: FileContract, delivery: Delivery, typed: DataFrame,
            sha: str, trailer: Trailer, recorded_at: datetime) -> None:
    staged, params = f"staged_{sha[:16]}", f"delivery_{sha[:16]}"  # session-scoped, see ingest_file
    typed.createOrReplaceTempView(staged)
    spark.createDataFrame(
        [(delivery.partner, delivery.business_date, sha, trailer.generated_at, recorded_at)],
        "partner STRING, business_date DATE, sha256 STRING, generated_at TIMESTAMP, recorded_at TIMESTAMP",
    ).createOrReplaceTempView(params)
    spark.conf.set(_SNAPSHOT_PROPERTY_CONF + SHA_PROPERTY, sha)
    try:
        for attempt in range(1, _COMMIT_ATTEMPTS + 1):
            try:
                spark.sql(restatement_merge_sql(contract, delivery, staged, params))
                return
            except Exception as e:  # Iceberg's ValidationException / CommitFailedException, via py4j
                if attempt == _COMMIT_ATTEMPTS or not _is_commit_conflict(e):
                    raise
                # Another writer committed to this partition first; re-run against the new snapshot.
    finally:
        spark.conf.unset(_SNAPSHOT_PROPERTY_CONF + SHA_PROPERTY)
        spark.catalog.dropTempView(staged)
        spark.catalog.dropTempView(params)


def _is_commit_conflict(e: Exception) -> bool:
    text = str(e)
    return "ValidationException" in text or "CommitFailedException" in text


def _outcome(spark: SparkSession, contract: FileContract, delivery: Delivery, trailer: Trailer,
             sha: str, recorded_at: datetime) -> Outcome:
    """Classify from the table itself, so a retry after a crash reports correctly.

    A snapshot carrying our sha is not proof of a load: a MERGE that inserts
    nothing still commits. Rows stamped with this call's ``recorded_at`` are.
    """
    spark.catalog.refreshTable(contract.table)  # see commits from other sessions too
    delivery_rows = spark.table(contract.table).where(
        (F.col("partner") == delivery.partner) & (F.col(contract.date_column) == F.lit(delivery.business_date)))
    if delivery_rows.where(F.col("recorded_to").isNull()
                           & (F.col("file_generated_at") > F.lit(trailer.generated_at))).limit(1).count():
        return Outcome.SUPERSEDED
    loaded_now = delivery_rows.where((F.col("file_sha256") == sha)
                                     & (F.col("recorded_from") == F.lit(recorded_at))).limit(1).count()
    return Outcome.LOADED if loaded_now else Outcome.ALREADY_LOADED


def ingest_file(spark: SparkSession, delivery: Delivery, contract: FileContract, manifest: Manifest,
                worker: str, recorded_at: datetime | None = None) -> Outcome:
    # Own session: the snapshot property and temp views are session state, and a
    # worker may run several ingests at once.
    spark = spark.newSession()
    recorded_at = recorded_at or datetime.now(timezone.utc)
    sha = sha256_of(delivery.path)
    if not manifest.claim(sha, delivery, worker):
        return Outcome.NOT_CLAIMED

    trailer = Trailer.read(delivery.trailer_path)
    raw = spark.read.csv(delivery.path, header=True)  # all strings, named by the file's own header
    typed, failures = validate(raw, contract, delivery, trailer)
    if failures:
        manifest.mark(sha, "QUARANTINED", "; ".join(failures))  # the file itself is moved by the caller
        return Outcome.QUARANTINED

    _commit(spark, contract, delivery, typed, sha, trailer, recorded_at)
    outcome = _outcome(spark, contract, delivery, trailer, sha, recorded_at)
    manifest.mark(sha, outcome.value)  # a crash before this line is fine: the retry lands in ALREADY_LOADED
    return outcome



def as_of(spark: SparkSession, table: str, t: datetime) -> DataFrame:
    """What the table held at ``t``: works for any ``t`` in retention, unlike snapshot time travel."""
    return spark.table(table).where(
        (F.col("recorded_from") <= F.lit(t)) & (F.col("recorded_to").isNull() | (F.col("recorded_to") > F.lit(t)))
    )
