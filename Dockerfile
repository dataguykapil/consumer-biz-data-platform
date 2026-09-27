# Reproducible test runner: Python 3.14 + JDK 21 (Temurin) + PySpark 4.1 + Iceberg 1.11.
# Build: docker build -t cdp-test .      Run: docker run --rm cdp-test
#
# JDK 21 is the newest LTS that Spark 4.1 supports (Spark 4.1 runs on Java 17/21).
FROM eclipse-temurin:21-jre AS jre

FROM python:3.14-slim-trixie

ARG ICEBERG_VERSION=1.11.0

COPY --from=jre /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    ICEBERG_SPARK_JAR=/opt/jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

ADD --chmod=644 \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar \
    /opt/jars/

RUN useradd --create-home --uid 10001 app && mkdir /app && chown app /app
WORKDIR /app
COPY --chown=app pyproject.toml README.md ./
COPY --chown=app src ./src
RUN pip install --no-cache-dir -e '.[dev]'
COPY --chown=app tests ./tests

USER 10001
CMD ["pytest", "-q"]
