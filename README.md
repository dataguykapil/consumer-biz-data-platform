# consumer-biz-data-platform
A data lakehouse platform for multiple consumer business

## Deliverables

- [`docs/design.md`](docs/design.md): the design document, including the honesty section (§9)
- [`docs/architecture.png`](docs/architecture.png): the architecture diagram, also embedded in the design as Mermaid
- [`src/cdp/`](src/cdp): code for the three hard parts
  - `cdc_merge.py`: CDC into silver
  - `file_ingest.py`: partner files
  - `publish_gate.py`: the publish gate
- [`docs/test-plan.md`](docs/test-plan.md) and [`tests/`](tests): the test plan and the tests

## Running the tests

Stack: Python 3.12, JDK 17, PySpark 4.1.3, Iceberg 1.11.0 (`iceberg-spark-runtime-4.1_2.13`).
Tests use a local Spark session with a filesystem-backed Iceberg catalog; no external services.

**Locally** (needs Python 3.12 and a JDK 17; `make` picks up a JDK unpacked at `.tools/jdk17`, else `JAVA_HOME`):

```sh
make test                              # creates .venv, downloads the Iceberg jar, runs pytest
make test ARGS="-k cdc -x"             # pass-through pytest args
```

**In Docker** (no local Java or Python needed):

```sh
make docker-test                       # or: docker build -t cdp-test . && docker run --rm cdp-test
```
