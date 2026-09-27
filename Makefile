ICEBERG_VERSION := 1.11.0
ICEBERG_JAR     := .tools/jars/iceberg-spark-runtime-4.1_2.13-$(ICEBERG_VERSION).jar
ICEBERG_URL     := https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/$(ICEBERG_VERSION)/iceberg-spark-runtime-4.1_2.13-$(ICEBERG_VERSION).jar

PYTHON ?= python3.12
VENV   := .venv

# Prefer the project-local JDK (see README); fall back to whatever JAVA_HOME says.
LOCAL_JDK := $(CURDIR)/.tools/jdk17/Contents/Home
ifneq ($(wildcard $(LOCAL_JDK)/bin/java),)
export JAVA_HOME := $(LOCAL_JDK)
endif
export ICEBERG_SPARK_JAR := $(CURDIR)/$(ICEBERG_JAR)

NODE_BIN := $(CURDIR)/.tools/node/bin
MMDC     := .tools/mermaid/node_modules/.bin/mmdc

.PHONY: setup test diagram docker-build docker-test clean

setup: $(VENV)/bin/pytest $(ICEBERG_JAR)

$(VENV)/bin/pytest: pyproject.toml
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --quiet --upgrade pip
	$(VENV)/bin/pip install --quiet -e '.[dev]'

$(ICEBERG_JAR):
	mkdir -p $(dir $@)
	curl -sSfL -o $@ $(ICEBERG_URL)

test: setup
	$(VENV)/bin/pytest $(ARGS)

# Render the Mermaid architecture diagram embedded in docs/design.md to a PNG.
diagram:
	awk '/^```mermaid/{f=1;next} /^```/{f=0} f' docs/design.md > .tools/architecture.mmd
	PATH=$(NODE_BIN):$$PATH $(MMDC) -i .tools/architecture.mmd -o docs/architecture.png -s 2

docker-build:
	docker build -t cdp-test .

docker-test: docker-build
	docker run --rm cdp-test

clean:
	rm -rf spark-warehouse metastore_db derby.log .pytest_cache
