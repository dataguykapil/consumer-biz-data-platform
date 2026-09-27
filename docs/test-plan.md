# Test Plan: the three hard parts

Each test targets one guarantee from the design (§7) and names the **wrong design it would catch**. A test that would still pass against a broken design doesn't count. So each mechanism was also broken on purpose to confirm that at least one test fails. That's in the "Proof the tests bite" tables.

**How to run:** `make test` (local JDK 17 + PySpark 4.1.3 + Iceberg 1.11.0), or `make docker-test`. The tests use a real Iceberg table on local disk, not mocks: every guarantee depends on Iceberg's actual `MERGE`, snapshot and commit behaviour. The suite has 33 tests and takes about 30 seconds.

---

## 1. CDC merge into silver (`tests/test_cdc_merge.py`, 20 tests, written)

**Guarantee:** for every key, silver ends in the same state whatever order the changes arrive in and however often they are redelivered. That state is the source row at the highest LSN applied.

| Test | Asserts | Fails if the design… |
|---|---|---|
| `every_arrival_order_converges_to_lsn_order` (all 6 orders of create, update and delete) | Every order gives the state that LSN order gives | depends on arrival order in any way. This test found a real bug: tombstones kept "last known values", which varied with arrival order. |
| `replayed_older_change_does_not_overwrite_newer` | APPROVED(101), DISBURSED(103), then REJECTED(102) leaves DISBURSED | trusts arrival order |
| `delete_then_stale_update_does_not_resurrect` | A stale update after a delete leaves no live row | hard-deletes rows, losing the delete's LSN |
| `delete_for_unseen_key_blocks_later_stale_insert` | A delete seen first still blocks an older insert | drops deletes for keys it hasn't seen yet |
| `delete_carrying_values_gives_same_tombstone_in_any_order` | With `REPLICA IDENTITY FULL` deletes, the tombstone is the same in both orders | keeps values on tombstones |
| `key_reused_after_delete_is_live_again` | A genuinely newer insert after a delete makes the key live | treats tombstones as permanent |
| `redelivered_batch_is_a_no_op` | Applying the same batch twice leaves the same state | relies on remembering micro-batch ids |
| `many_changes_and_duplicates_for_one_key_in_one_batch`, `…_for_an_existing_key…` | One row per key, the highest LSN wins | skips reducing the batch first, so the `MERGE` fails or duplicates rows |
| `snapshot_read_loses_to_change_at_same_lsn_in_one_batch` | A change beats a snapshot read at the same LSN | breaks ties arbitrarily |
| `equal_lsn_never_overwrites` | A later event at an already-applied LSN changes nothing | uses `>=` in the guard |
| `toast_placeholder_keeps_previous_value` | The placeholder never replaces the real value | writes Debezium's placeholder string into the table |
| `watermark_is_committed_with_the_data` | The watermark property is on the same snapshot as the data, and the session setting is cleared | writes the watermark separately |
| `empty_batch_commits_nothing` | No snapshot for an empty batch | advances the watermark with no data |
| `unsupported_op_fails_and_commits_nothing` | A truncate (`'t'`) raises, and the table is unchanged | treats unknown ops as updates |

The shared `rows()` helper, used by most tests, also checks that silver holds **exactly one row per key**. Its first version hid duplicates, which the deliberate breakages exposed.

**Proof the tests bite** (re-run against the final 20 tests):

| Mechanism removed | Tests failing |
|---|---|
| LSN guard (`WHEN MATCHED` with no condition) | 9 |
| In-batch reduction | 4 |
| `>` changed to `>=` | 1 |
| Change-beats-read tie-break | 1 |
| Tombstones clear their values | 1 |
| TOAST placeholder handling | 1 |

## 2. Partner-file ingestion (`tests/test_file_ingest.py`, 12 tests, written)

**Guarantee:** for a delivery (partner, business date), retries, duplicate deliveries, crashes and racing workers never produce duplicate records. Invalid files publish nothing, corrections replace the old version in one commit, a stale version never becomes current, and history can be queried "as known at" any past time.

The manifest is faked so tests can make it lose state, fail mid-run, or grant the same claim twice. The data side is a real Iceberg table.

| Test | Asserts | Fails if the design… |
|---|---|---|
| `same_file_twice_publishes_once` (the manifest has lost its state) | One copy; the second call reports `ALREADY_LOADED` | keeps duplicate detection only in the manifest |
| `crash_after_commit_then_retry_does_not_duplicate` | The worker dies after the `MERGE` but before the manifest update; the retry adds nothing | records "loaded" outside the data commit |
| `two_workers_racing_on_the_same_file_publish_once` (3 rounds, threads) | One copy, whatever the interleaving | checks "already loaded?" *before* the `MERGE` rather than inside it |
| `two_versions_racing_leave_only_the_newer_current` | Only the newer version is current | can leave two versions current, double-counting the day's money |
| `invalid_file_is_quarantined_and_publishes_nothing` (5 cases) | Non-integer paise, trailer mismatch, duplicate keys, wrong date or wrong header: nothing written, and the reason recorded | validates row by row and loads the good rows |
| `restatement_replaces_current_and_keeps_what_was_known` | Current rows are v2 (the withdrawn row is gone), `as_of` between versions returns v1, and before any load returns nothing | deduplicates on a fingerprint alone (v1 and v2 both load), or keeps history only in snapshots |
| `stale_version_never_becomes_current` (v1→v2→v1, and v2→v1) | v2 stays current, and the late v1 reports `SUPERSEDED` | lets "newest to arrive" win |

**Proof the tests bite:**

| Mechanism removed | Tests failing |
|---|---|
| The `MERGE`'s insert guard | 5 |
| Closing only older versions | 5 |
| `>=` changed to `>` in the version check | 3 |
| Retry on commit conflict | 2 |
| Quarantine on validation failure | 5 |
| Guard moved *outside* the `MERGE` (check, then write, with a 0.5 s gap) | 2, both race tests |

The race tests produced **real Iceberg commit conflicts**, 2 in a checked run, which the retry resolved. That confirms serializable isolation rejects the second of two competing `MERGE`s on one partition. It was only an assumption in the first draft of the design.

## 3. Publish gate (`src/cdp/publish_gate.py`, no test file, as planned)

**Guarantee:** consumers only see a gold snapshot whose control totals (rows, signed paise, distinct keys per source and date) exactly match every independent side. ClickHouse shows a version only after it has reconciled to that same snapshot.

Each test below was run once, by hand, against a local Iceberg table using a throwaway script. They would become `tests/test_publish_gate.py` next.

| Test | Asserts | Fails if the design… | Checked by hand |
|---|---|---|---|
| Clean run | main moves to the branch snapshot, the branch is dropped, and the log says published | — | ✅ |
| Paise off by 1 | Refused, main unchanged, branch kept, mismatch reported against source and silver | checks after publishing, or allows a tolerance | ✅ |
| Date missing from the run | Refused | uses `overwritePartitions()`. This was found this way: the old rows for the missing date survived and matched. | ✅ |
| Duplicated row plus dropped row (same count and total) | Refused, caught **only** by distinct keys | compares row count and paise alone | ✅ |
| Main moved after the branch was cut (a second writer) | `fast_forward` refuses | overwrites main, or retries blindly | ✅ (branch cut by hand, not via `publish_with_gate`) |
| Paise sum overflow | The run fails | wraps silently. Covered by ANSI mode, which was checked separately. | ✅ indirectly |
| Watermarks and run id | On the branch commit and in the log row | records them elsewhere | ✅ |
| ClickHouse: load, then load again | Reconciles, and the re-run is idempotent | — | ⚠️ fake client only |
| ClickHouse: a failed load doesn't hide published rows | Published version still served | leaves `_snapshot_seq` out of the sort key (the bug found in review) | ❌ needs real ClickHouse |
| ClickHouse: a restated-away row disappears from serving | Only published versions are visible | serves the table without the published-versions filter | ❌ needs real ClickHouse |

## What these tests can't tell us

These would come next, in order of risk:

1. **Real ClickHouse (Docker Compose).** The `FINAL`, `uniqExact` and `{seq:UInt64}` parameter SQL has never run against real ClickHouse. Nor has the multi-version view.
2. **End-to-end CDC with real Debezium and Postgres.** This is the only way to check what actually arrives, as opposed to what our code does with it:
   - that `source.lsn` behaves as assumed across the snapshot-to-streaming handoff
   - TOAST placeholders with and without `REPLICA IDENTITY FULL`
   - truncate events
   - a connector restart mid-transaction
3. **Load test at 10k events/s.** Can merge-on-read CDC plus hourly compaction keep up? Do compaction and streaming `MERGE` fight over commits (design §8.4)? That needs measuring, not reasoning.
4. **Property-based ordering tests.** `hypothesis` would generate random histories of changes per key and random batchings, and assert that the result equals LSN-order replay. That's a much larger search than the six permutations we test now.
5. **Replication-slot guard rails.** An alert fires when slot lag passes its threshold, before `max_slot_wal_keep_size` is reached. This is the failure that can hurt production Postgres.
6. **Catalog cache assumption.** A test that pins the rule "one statement, one snapshot" on the production catalog (Polaris over REST, not the Hadoop catalog used here), since §7.2's race safety depends on it.
