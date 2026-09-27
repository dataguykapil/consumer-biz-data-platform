# 0020. Compress with zstd in Parquet and Kafka, and choose column codecs in the serving stores

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

At about 0.8B rows a day and 5 years of retention, storage is roughly 200 TB, so compression is a first-order cost lever. It also affects scan speed, because compressed bytes are read from object storage. How well data compresses depends as much on **layout** (sort order, types, file size) as on the codec.

## Options considered

| Codec | Ratio | Speed | Notes |
|---|---|---|---|
| Snappy | Lower | Fastest | Spark's historical default; files are noticeably larger |
| gzip | High | Slow writes | Iceberg's default before 1.4 |
| **zstd** | High (similar to gzip at level 3) | Fast | Level can be tuned: higher for write-once, read-rarely data |
| LZ4 | Lower | Very fast | ClickHouse's default; less common for Parquet |

## Decision

- **Iceberg / Parquet: zstd.** This is already the default for tables created on Iceberg 1.4 or later. It was confirmed in the 1.11 jar (`PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0 = "zstd"`), and is set explicitly anyway with `write.parquet.compression-codec=zstd`.
  - **Hot tables** (silver, gold) use the default level, for fast writes. Delete files are zstd too.
  - **Bronze** (write-once, rarely read) uses a higher level (`write.parquet.compression-level=9`) once a sample confirms the ratio gain justifies the CPU.
- **Layout matters as much as the codec:**
  - A declared write sort order per table (`ALTER TABLE … WRITE ORDERED BY`) groups similar values: silver entities by key, file facts by `(partner, customer_id)`, gold by the main filter columns. That improves both compression and min/max pruning.
  - Types stay narrow and native: money as `BIGINT` paise, times as `TIMESTAMP`, enums as dictionary-encoded strings. Never strings for numbers or dates.
  - The default file sizes are kept: 512 MB target files, 128 MB row groups, confirmed in the jar. Compaction exists partly to keep files at this size.
- **Kafka:** producers (Debezium, app producers) set `compression.type=zstd`, and brokers keep the producer's compression.
- **Landing zone:** files are stored **as received**. The sha256 identity is defined over the original bytes, so they aren't recompressed.
- **ClickHouse (BI):**
  - LZ4 is the default for hot columns.
  - `ZSTD(3)` for large string columns.
  - `Delta` or `DoubleDelta` plus ZSTD for timestamps and increasing ids.
  - `T64` for small-range integers.
  - `LowCardinality(String)` for status, operator, circle and channel.
- **Cassandra (online store):** its default table compressor (LZ4) is kept for low read latency.

## Consequences

- **Estimates to confirm:** storage and scan cost fall substantially compared with Snappy or uncompressed data. The design's estimate of 60–80 bytes/row compressed is an assumption, to be measured on a 1-day sample before sizing is final.
- **CPU cost:** higher zstd levels cost CPU on writes and compaction, which is why the higher level is used only where data is written once.

## Alternatives considered

- **Snappy everywhere:** faster, but bigger files mean more storage and more bytes scanned, over 5 years.
- **Maximum zstd everywhere:** saves storage, but slows streaming `MERGE` and compaction on hot tables.
