import os
import sys
from datetime import datetime

os.environ.pop("SPARK_HOME", None)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from spark_app.song_gaps import dt_minus_start, successive_start_gaps


SCHEMA = StructType(
    [
        StructField("name", StringType()),
        StructField("song", StringType()),
        StructField("start_time", TimestampType()),
        StructField("end_time", TimestampType()),
        StructField("dt", StringType()),
    ]
)


def ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("SongGapsTest")
        .master("local[*]")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# dt_minus_start  (Q1 / Q2)
# ---------------------------------------------------------------------------


def test_dt_minus_start_fractional_days(spark):
    # dt = midnight 2025-01-03; start_time 1.5 days earlier -> 1.5 day diff.
    rows = [
        ("Alice", "s1", ts("2025-01-01 12:00:00"), ts("2025-01-01 12:03:00"), "20250103"),
        ("Bob", "s2", ts("2025-01-02 00:00:00"), ts("2025-01-02 00:02:00"), "20250103"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    diffs = sorted(r.dt_minus_start_days for r in dt_minus_start(df).collect())
    assert diffs == pytest.approx([1.0, 1.5])


def test_dt_minus_start_drops_null_start_time(spark):
    rows = [
        ("Alice", "s1", None, ts("2025-01-01 12:03:00"), "20250103"),
        ("Bob", "s2", ts("2025-01-02 00:00:00"), ts("2025-01-02 00:02:00"), "20250103"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    result = dt_minus_start(df)
    assert result.count() == 1
    assert result.first().dt_minus_start_days == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# successive_start_gaps  (Q3 / Q4)
# ---------------------------------------------------------------------------


def test_successive_gaps_only_short_plays(spark):
    # Alice has three short plays (<10s) at 0s, 60s, 100s -> gaps 60 and 40.
    rows = [
        ("Alice", "s1", ts("2025-01-01 00:00:00"), ts("2025-01-01 00:00:05"), "20250101"),
        ("Alice", "s2", ts("2025-01-01 00:01:00"), ts("2025-01-01 00:01:05"), "20250101"),
        ("Alice", "s3", ts("2025-01-01 00:01:40"), ts("2025-01-01 00:01:45"), "20250101"),
        # A long play (>10s) must be excluded, so it never creates a gap.
        ("Alice", "s4", ts("2025-01-01 00:02:00"), ts("2025-01-01 00:03:00"), "20250101"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    gaps = sorted(r.gap_seconds for r in successive_start_gaps(df).collect())
    assert gaps == [40, 60]


def test_successive_gaps_are_per_name(spark):
    # Two names; gaps must not cross the name boundary.
    rows = [
        ("Alice", "s1", ts("2025-01-01 00:00:00"), ts("2025-01-01 00:00:05"), "20250101"),
        ("Alice", "s2", ts("2025-01-01 00:00:30"), ts("2025-01-01 00:00:35"), "20250101"),
        ("Bob", "s1", ts("2025-01-01 00:00:00"), ts("2025-01-01 00:00:05"), "20250101"),
        ("Bob", "s2", ts("2025-01-01 00:00:10"), ts("2025-01-01 00:00:15"), "20250101"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    by_name = {(r.name, r.gap_seconds) for r in successive_start_gaps(df).collect()}
    assert by_name == {("Alice", 30), ("Bob", 10)}


def test_successive_gaps_single_row_name_has_no_gap(spark):
    # One short play -> no predecessor -> no gap row emitted.
    rows = [
        ("Solo", "s1", ts("2025-01-01 00:00:00"), ts("2025-01-01 00:00:05"), "20250101"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    assert successive_start_gaps(df).count() == 0
