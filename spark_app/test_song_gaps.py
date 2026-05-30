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

from spark_app.song_gaps import (
    daily_stats,
    dt_minus_start,
    duration_hour_deciles,
    merge_song_ranges,
    relations_on_day,
    successive_start_gaps,
)


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


# ---------------------------------------------------------------------------
# merge_song_ranges
# ---------------------------------------------------------------------------


def _merged(df):
    """Collect merged ranges as sorted (name, song, start, end) string tuples."""
    return sorted(
        (r.name, r.song, str(r.start_time), str(r.end_time))
        for r in merge_song_ranges(df).collect()
    )


def test_merge_overlapping_ranges(spark):
    # Second range starts before the first ends -> overlap -> single range.
    rows = [
        ("Alice", "s1", ts("2025-01-01 10:00:00"), ts("2025-01-01 10:30:00"), "20250101"),
        ("Alice", "s1", ts("2025-01-01 10:15:00"), ts("2025-01-01 10:45:00"), "20250101"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    assert _merged(df) == [
        ("Alice", "s1", "2025-01-01 10:00:00", "2025-01-01 10:45:00"),
    ]


def test_merge_within_30_minutes(spark):
    # 20-minute gap (< 30 min) -> merged.
    rows = [
        ("Alice", "s1", ts("2025-01-01 10:00:00"), ts("2025-01-01 10:30:00"), "20250101"),
        ("Alice", "s1", ts("2025-01-01 10:50:00"), ts("2025-01-01 11:10:00"), "20250101"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    assert _merged(df) == [
        ("Alice", "s1", "2025-01-01 10:00:00", "2025-01-01 11:10:00"),
    ]


def test_no_merge_at_30_minute_boundary(spark):
    # Exactly 30-minute gap -> NOT merged (must be < 30 min apart).
    rows = [
        ("Alice", "s1", ts("2025-01-01 10:00:00"), ts("2025-01-01 10:30:00"), "20250101"),
        ("Alice", "s1", ts("2025-01-01 11:00:00"), ts("2025-01-01 11:20:00"), "20250101"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    assert _merged(df) == [
        ("Alice", "s1", "2025-01-01 10:00:00", "2025-01-01 10:30:00"),
        ("Alice", "s1", "2025-01-01 11:00:00", "2025-01-01 11:20:00"),
    ]


def test_merge_does_not_cross_song_or_name(spark):
    # Same time window but different song / name must stay separate.
    rows = [
        ("Alice", "s1", ts("2025-01-01 10:00:00"), ts("2025-01-01 10:30:00"), "20250101"),
        ("Alice", "s2", ts("2025-01-01 10:10:00"), ts("2025-01-01 10:40:00"), "20250101"),
        ("Bob", "s1", ts("2025-01-01 10:05:00"), ts("2025-01-01 10:35:00"), "20250101"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    assert _merged(df) == [
        ("Alice", "s1", "2025-01-01 10:00:00", "2025-01-01 10:30:00"),
        ("Alice", "s2", "2025-01-01 10:10:00", "2025-01-01 10:40:00"),
        ("Bob", "s1", "2025-01-01 10:05:00", "2025-01-01 10:35:00"),
    ]


def test_merge_chains_then_breaks(spark):
    # Three ranges chain (each < 30 min from the running end), a 4th is far off.
    rows = [
        ("Alice", "s1", ts("2025-01-01 10:00:00"), ts("2025-01-01 10:10:00"), "20250101"),
        ("Alice", "s1", ts("2025-01-01 10:30:00"), ts("2025-01-01 10:40:00"), "20250101"),
        ("Alice", "s1", ts("2025-01-01 11:00:00"), ts("2025-01-01 11:10:00"), "20250101"),
        ("Alice", "s1", ts("2025-01-01 13:00:00"), ts("2025-01-01 13:10:00"), "20250101"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    assert _merged(df) == [
        ("Alice", "s1", "2025-01-01 10:00:00", "2025-01-01 11:10:00"),
        ("Alice", "s1", "2025-01-01 13:00:00", "2025-01-01 13:10:00"),
    ]


# ---------------------------------------------------------------------------
# relations_on_day  (daily-stats step 1)
# ---------------------------------------------------------------------------


def test_relations_on_day_keeps_overlapping_only(spark):
    rows = [
        # Fully inside the day -> kept.
        ("Alice", "s1", ts("2026-04-01 10:00:00"), ts("2026-04-01 10:30:00"), "20260401"),
        # Starts day before, ends inside -> overlaps -> kept.
        ("Bob", "s2", ts("2026-03-31 23:50:00"), ts("2026-04-01 00:10:00"), "20260401"),
        # Entirely the day before -> dropped.
        ("Carol", "s3", ts("2026-03-31 10:00:00"), ts("2026-03-31 10:30:00"), "20260331"),
        # Entirely the next day -> dropped.
        ("Dave", "s4", ts("2026-04-02 00:00:00"), ts("2026-04-02 00:30:00"), "20260402"),
        # Null times -> dropped.
        ("Eve", "s5", None, ts("2026-04-01 12:00:00"), "20260401"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    kept = {r.name for r in relations_on_day(df, "2026-04-01").collect()}
    assert kept == {"Alice", "Bob"}


# ---------------------------------------------------------------------------
# duration_hour_deciles  (daily-stats step 6)
# ---------------------------------------------------------------------------


def test_duration_hour_deciles(spark):
    # Durations of 1h, 2h, ..., 10h -> p10..p90 are monotonic and bounded.
    rows = [
        ("Alice", f"s{h}", ts("2026-04-01 00:00:00"), ts(f"2026-04-01 {h:02d}:00:00"), "20260401")
        for h in range(1, 11)
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    deciles = duration_hour_deciles(df)
    assert len(deciles) == 9
    assert deciles == sorted(deciles)
    assert deciles[0] >= 1.0 and deciles[-1] <= 10.0


# ---------------------------------------------------------------------------
# daily_stats  (full pipeline)
# ---------------------------------------------------------------------------


def test_daily_stats_full_pipeline(spark):
    rows = [
        # Alice/s1: two ranges 20 min apart -> merge into one.
        ("Alice", "s1", ts("2026-04-01 10:00:00"), ts("2026-04-01 10:30:00"), "20260401"),
        ("Alice", "s1", ts("2026-04-01 10:50:00"), ts("2026-04-01 11:10:00"), "20260401"),
        # Bob/s2: one range, untouched.
        ("Bob", "s2", ts("2026-04-01 12:00:00"), ts("2026-04-01 13:00:00"), "20260401"),
        # Off-day row -> excluded by step 1.
        ("Carol", "s3", ts("2026-03-30 10:00:00"), ts("2026-03-30 10:30:00"), "20260330"),
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    stats = daily_stats(df, "2026-04-01")

    assert stats["raw_count"] == 3       # 3 on-day rows
    assert stats["merged_count"] == 2    # Alice's two merged into one + Bob
    assert stats["n_names"] == 2
    assert stats["n_songs"] == 2
    assert stats["n_relations"] == 2
    assert len(stats["duration_hour_deciles"]) == 9
