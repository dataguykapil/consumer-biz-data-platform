ICEBERG_VERSION := 1.11.0
ICEBERG_JAR     := .tools/jars/iceberg-spark-runtime-4.1_2.13-$(ICEBERG_VERSION).jar
ICEBERG_URL     := https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/$(ICEBERG_VERSION)/iceberg-spark-runtime-4.1_2.13-$(ICEBERG_VERSION).jar

# Prefer a project-local Python 3.14 (see README); fall back to one on PATH.
LOCAL_PYTHON := $(firstword $(wildcard $(CURDIR)/.tools/python/cpython-3.14*/bin/python3.14))
PYTHON ?= $(or $(LOCAL_PYTHON),python3.14)
VENV   := .venv
BIN    := $(VENV)/bin

# Prefer the project-local JDK 21 (see README): macOS tarball layout first, then Linux.
# Falls back to whatever JAVA_HOME already says.
LOCAL_JDK := $(firstword $(patsubst %/bin/java,%,$(wildcard $(CURDIR)/.tools/jdk21/Contents/Home/bin/java $(CURDIR)/.tools/jdk21/bin/java)))
ifneq ($(LOCAL_JDK),)
export JAVA_HOME := $(LOCAL_JDK)
endif
export ICEBERG_SPARK_JAR := $(CURDIR)/$(ICEBERG_JAR)

NODE_BIN := $(CURDIR)/.tools/node/bin
MMDC     := .tools/mermaid/node_modules/.bin/mmdc

.PHONY: setup hooks lint format security test check diagram docker-build docker-test clean

setup: $(BIN)/pytest $(ICEBERG_JAR)

$(BIN)/pytest: pyproject.toml
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --quiet --upgrade pip
	$(BIN)/pip install --quiet -e '.[dev]'
	touch $@

$(ICEBERG_JAR):
	mkdir -p $(dir $@)
	curl -sSfL -o $@ $(ICEBERG_URL)

# Install the git hooks: fast checks on commit, the test suite on push.
hooks: setup
	$(BIN)/pre-commit install --hook-type pre-commit --hook-type pre-push

lint: setup
	$(BIN)/ruff check src tests
	$(BIN)/ruff format --check src tests

format: setup
	$(BIN)/ruff format src tests
	$(BIN)/ruff check --fix src tests

security: setup
	$(BIN)/bandit -c pyproject.toml -r src -q
	$(BIN)/pip-audit --skip-editable

test: setup
	$(BIN)/pytest $(ARGS)

# Everything CI runs, except the Docker build and CodeQL.
check: lint security test

# Render the two Mermaid diagrams in docs/design.md (§3.1 containers, §3.2 data flow) to PNGs.
diagram:
	mkdir -p .tools/diagrams
	awk '/^```mermaid/{n++; f=1; next} /^```/{f=0} f{print > (".tools/diagrams/design-" n ".mmd")}' docs/design.md
	PATH=$(NODE_BIN):$$PATH $(MMDC) -i .tools/diagrams/design-1.mmd -o docs/architecture-containers.png -s 2
	PATH=$(NODE_BIN):$$PATH $(MMDC) -i .tools/diagrams/design-2.mmd -o docs/architecture-dataflow.png -s 2

docker-build:
	docker build -t cdp-test .

docker-test: docker-build
	docker run --rm cdp-test

clean:
	rm -rf spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
