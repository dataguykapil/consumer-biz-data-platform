# Reproducible test runner: Python 3.12 + JDK 17 + PySpark 4.1 + Iceberg 1.11.
# Build: docker build -t cdp-test .      Run: docker run --rm cdp-test
FROM python:3.12-slim-bookworm

ARG ICEBERG_VERSION=1.11.0

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/* \
 # JVM path differs between amd64 and arm64 images; pin a stable alias.
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    ICEBERG_SPARK_JAR=/opt/jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar \
    PYTHONDONTWRITEBYTECODE=1

RUN mkdir -p /opt/jars && curl -sSfL -o "${ICEBERG_SPARK_JAR}" \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar"

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir -e '.[dev]'
COPY tests ./tests

CMD ["pytest"]
