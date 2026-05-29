import os
import sys

os.environ.pop("SPARK_HOME", None)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pytest
from pyspark.sql import SparkSession

from spark_app.dataframe_example import build_dataframe, engineers_over_30


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("DataFrameExampleTest")
        .master("local[*]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_dataframe_has_all_rows(spark):
    df = build_dataframe(spark)
    assert df.count() == 5
    assert df.columns == ["name", "age", "department"]


def test_engineers_over_30_returns_carol_and_eve(spark):
    df = build_dataframe(spark)
    result = engineers_over_30(df)
    names = sorted(row.name for row in result.collect())
    assert names == ["Carol", "Eve"]


def test_engineers_over_30_filters_out_alice(spark):
    df = build_dataframe(spark)
    result = engineers_over_30(df)
    names = {row.name for row in result.collect()}
    assert "Alice" not in names
    assert "Bob" not in names
