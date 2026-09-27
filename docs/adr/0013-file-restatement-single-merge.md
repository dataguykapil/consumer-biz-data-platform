# 0013. Decide and write partner-file loads in a single MERGE, with bitemporal history

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

Partner files get retried, duplicated, raced by two workers, and corrected. Two early approaches failed:
- **Fingerprint-only deduplication:** a corrected file has a new sha256, so it loads alongside the original and double-counts the day.
- **Check whether it's loaded, then write:** this races, whether the check reads a manifest or the table.

Five years of "as known at" history can't come from Iceberg snapshots, which expire.

This decision merges two proposals: the author's (file state machine, quarantine, claim flow, and the crash and race tests) and a review that found the two failures above.

## Decision

Implemented in `src/cdp/file_ingest.py`:
- **Identity:** sha256 identifies the bytes, and (partner, business date) identifies the delivery. The trailer's `generated_at` orders versions.
- **Validation:** the whole file is validated in one pass, and any failure quarantines the whole file.
- **Bitemporal rows:** each row carries `business_date`, `recorded_from` and `recorded_to`.
- **One `MERGE`:** it inserts only if no same-or-newer version exists, and closes only older current versions. The check is inside the write, so Iceberg's serializable isolation rejects a competing commit, and the retry decides again.
- **The Postgres manifest (claims, leases, state) is for operations only.**

## Consequences

- **Crash- and race-safe:** the race tests saw real commit conflicts resolved by the retry. Moving the check outside the `MERGE` made them fail.
- **Relies on Iceberg's catalog cache** for "one snapshot per statement". This needs re-verifying on Polaris.
- **Known gap:** a late, never-loaded older version can still load after an empty newest version. The fix would be a marker row per version.

## Alternatives considered

- **Fingerprint-only deduplication,** or a manifest checked before writing: both fail as described above.
- **Iceberg time travel for history:** snapshots expire after 7 days at this volume.
