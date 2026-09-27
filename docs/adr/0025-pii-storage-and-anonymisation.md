# 0025. Tokenise PII at the bronze boundary, encrypt originals per subject, and publish de-identified data for analytics

- **Status:** Accepted. Refines the PII points in [0018](0018-observability-governance-baseline.md)
- **Date:** 2026-09-27

## Context

The platform holds phone numbers, emails, PAN, Aadhaar references, names, addresses, bank accounts and device ids for 50M customers.

The design had a flaw. It tokenised PII **into silver**, yet bronze, the landing zone and Kafka held the same fields in clear text. So its claim that "deleting the vault entry erases a customer" was false: raw PII would survive in immutable bronze and in 5-year raw files.

India's Digital Personal Data Protection Act gives people a right to erasure, and lending and insurance have record-keeping obligations. UIDAI rules restrict how Aadhaar numbers may be stored. The exact obligations need compliance and legal sign-off.

## Options considered

| Option | For | Against |
|---|---|---|
| Tokenise in silver (previous design) | Simple | Raw PII in bronze and landing for 5 years; erasure impossible without rewriting immutable layers |
| Encrypt whole tables | Easy to add | Anyone with the key sees everything; can't erase one person |
| **Tokenise at the bronze boundary, originals encrypted per subject (chosen)** | Joins still work; erasure is a key deletion; raw PII confined to short-lived places | A tokenisation service in the ingestion path; the token key becomes critical |
| Format-preserving encryption | Keeps field formats | Reversible by anyone holding the key; more complex |

## Decision

1. **Where PII is tokenised.** PII is tokenised **before the bronze write**: in the Kafka → bronze stream and in the file loader.
   - *Direct identifiers* are replaced by a deterministic token, `HMAC-SHA256(k_v, normalised value)`, stored with the key version `v`. That keeps joins across tables and businesses working without the real value.
   - *Where clear PII still exists:* only in Kafka (7 days; ACLs, TLS, encryption at rest) and briefly in the landing zone (point 3).
2. **Originals.** The real values are stored only in the **vault**. Each customer's values are encrypted with their own data key (envelope encryption), and that key is wrapped by a master key held in KMS or an HSM.
   - *Erasure* deletes the customer's data key, which **crypto-shreds** every copy of their values, including in backups.
   - *Detokenisation* goes through the vault service only, with a stated purpose, and is logged.
3. **Raw files.** After a successful load, the loader writes a **tokenised copy** of the raw file, which becomes the archived original (ADR 0022). The clear-text original is deleted after **30 days**. The original's sha256 is kept in the manifest as proof of what was received.
4. **Treatment by class:**

   | Class | Examples | In silver and gold | Analytics / DS views | External or shared data |
   |---|---|---|---|---|
   | Direct identifiers | Phone, email, PAN, account number, device id | Token only | Token only | Dropped, or re-tokenised with an export-specific key so exports can't be joined to each other |
   | Aadhaar | Aadhaar number | **Never stored.** Only a reference or token, subject to UIDAI rules (confirm with compliance) | — | — |
   | Quasi-identifiers | Date of birth, PIN code, gender, income | Restricted column tag | Generalised: age band, district instead of PIN | Aggregates only, with cells of fewer than 10 people suppressed (k ≥ 10) |
   | Sensitive financial | Credit score, loan amounts, claims | Confidential tag, column grants | Allowed for approved purposes | Aggregates only |
   | Free text | Notes, address lines | Scanned for PII at ingestion; tokenised or dropped | Not exposed | Not exposed |

5. **Display values.** Apps get pre-computed masked values (for example `******4321`) from the online store (ADR 0019), never the raw values.
6. **Keys.** The HMAC key lives in KMS or an HSM, and only the tokenisation service can use it. Rotating it means re-tokenising, so rotation is a planned backfill using the stored key version, not a routine event.
7. **Logs.** Spark, ClickHouse, Cassandra and application logs must not contain PII. Log scrubbing is enforced, and query-log access is restricted.

## Consequences

- **Erasure becomes real:** deleting a customer's data key makes their values unrecoverable everywhere, without rewriting immutable layers.
- **Joins keep working:** analytics and data science work on tokens, so most users never need PII.
- **The HMAC key becomes a crown jewel.** With it, phone numbers could be brute-forced from tokens, since the space of possible numbers is small. It's therefore HSM-held and usable only by the service.
- **Tokens remain after erasure.** They are pseudonymous and no longer linked to a person once the data key is gone. Whether that meets erasure duties must be confirmed legally.
- **The ingestion path depends on the tokenisation service.** It must be highly available, and if it's down, ingestion pauses rather than writing clear text.

## Open questions

- **Legal:** retention obligations versus erasure for lending and insurance, the Aadhaar storage rules, and whether leftover tokens are acceptable.
- **Latency:** the tokenisation service at 10k events/s needs a load test. The expected approach is local HMAC with the key fetched from KMS, not a network call per value.
