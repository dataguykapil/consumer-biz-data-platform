"""Shared fixtures: one local SparkSession with an Iceberg catalog backed by a temp dir.

The catalog is a Hadoop (filesystem) catalog named ``lake``. Production uses a REST
catalog (Polaris), but the MERGE, branch and snapshot semantics under test are the
same Iceberg table-format behaviour either way.
"""

import os
import sys
import uuid

import pytest
from pyspark.sql import SparkSession

ICEBERG_COORDINATE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

# Python workers must run the same interpreter as the driver; otherwise Spark picks
# whatever `python3` is on PATH and fails on a minor-version mismatch.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("cdp-tests")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    jar = os.environ.get("ICEBERG_SPARK_JAR")
    if jar and os.path.exists(jar):
        builder = builder.config("spark.jars", jar)
    else:
        builder = builder.config("spark.jars.packages", ICEBERG_COORDINATE)

    session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def namespace(spark):
    """A fresh namespace per test so tables never leak state between tests."""
    ns = f"lake.t_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE NAMESPACE {ns}")
    return ns
