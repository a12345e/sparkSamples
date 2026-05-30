
import glob
import os
import sys

os.environ.pop("SPARK_HOME", None)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def _pick_jdk21():
    candidates = sorted(
        glob.glob(r"C:\Program Files\Microsoft\jdk-21*")
        + glob.glob(r"C:\Program Files\Eclipse Adoptium\jdk-21*")
        + glob.glob(r"C:\Program Files\Java\jdk-21*")
        + glob.glob(r"C:\java\21*"),
        reverse=True,
    )
    for path in candidates:
        if os.path.isfile(os.path.join(path, "bin", "java.exe")):
            return path
    return None


_jdk21 = _pick_jdk21()
if _jdk21:
    os.environ["JAVA_HOME"] = _jdk21
    os.environ["PATH"] = os.path.join(_jdk21, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Assumptions
#   * dt is the partition column formatted yyyymmdd; we treat it as midnight of
#     that day when comparing against the start_time timestamp.
#   * "difference in days" is a fractional day count (seconds / 86400), not a
#     truncated datediff, since dt is always after the time fields.
#   * Q3's "play duration < 10s" is interpreted as end_time - start_time < 10,
#     because end_time comes after start_time. Flip the subtraction if you
#     literally meant start_time - end_time.
# ---------------------------------------------------------------------------


def load_table(spark, table, dt_from, dt_to):
    """Read the requested partition range only (partition pruning on dt)."""
    return spark.table(table).where(
        (F.col("dt") >= dt_from) & (F.col("dt") <= dt_to)
    )


def dt_minus_start(df):
    """Fractional-day difference between dt (midnight) and start_time."""
    dt_ts = F.to_timestamp(F.col("dt").cast("string"), "yyyyMMdd")
    return (
        df.where(F.col("start_time").isNotNull())
        .withColumn(
            "dt_minus_start_days",
            (dt_ts.cast("long") - F.col("start_time").cast("long")) / 86400.0,
        )
    )


def successive_start_gaps(df):
    """Per-name gaps (seconds) between successive non-null start_times,
    restricted to rows whose play duration is under 10 seconds."""
    short = df.where(
        F.col("start_time").isNotNull()
        & F.col("end_time").isNotNull()
        & ((F.col("end_time").cast("long") - F.col("start_time").cast("long")) < 10)
    ).select("name", "start_time")

    w = Window.partitionBy("name").orderBy("start_time")
    return (
        short.withColumn("prev_start", F.lag("start_time").over(w))
        .where(F.col("prev_start").isNotNull())
        .withColumn(
            "gap_seconds",
            F.col("start_time").cast("long") - F.col("prev_start").cast("long"),
        )
    )


def merge_song_ranges(df, gap_seconds=1800):
    """Merge overlapping or near-adjacent time ranges per (name, song).

    Two ranges for the same (name, song) are merged when they overlap or are
    less than ``gap_seconds`` apart (default 30 minutes). The result has one row
    per merged range with the earliest start_time and latest end_time.
    """
    ranges = df.where(
        F.col("start_time").isNotNull() & F.col("end_time").isNotNull()
    ).select("name", "song", "start_time", "end_time")

    order = Window.partitionBy("name", "song").orderBy("start_time", "end_time")
    # Max end_time across all strictly-earlier rows in the group.
    prior = order.rowsBetween(Window.unboundedPreceding, -1)

    flagged = (
        ranges.withColumn("prev_max_end", F.max("end_time").over(prior))
        # A new merged range starts on the first row, or when this start_time is
        # >= gap_seconds after every earlier end_time (i.e. a real gap exists).
        .withColumn(
            "new_range",
            (
                F.col("prev_max_end").isNull()
                | (
                    F.col("start_time").cast("long")
                    - F.col("prev_max_end").cast("long")
                    >= gap_seconds
                )
            ).cast("int"),
        )
    )

    running = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    grouped = flagged.withColumn(
        "range_id", F.sum("new_range").over(running)
    )

    return (
        grouped.groupBy("name", "song", "range_id")
        .agg(
            F.min("start_time").alias("start_time"),
            F.max("end_time").alias("end_time"),
        )
        .drop("range_id")
    )


def relations_on_day(df, day):
    """All relations whose [start_time, end_time] overlaps a single calendar day.

    ``day`` is a ``"yyyy-MM-dd"`` string (e.g. ``"2026-04-01"``). A relation is
    kept when it overlaps the ``[day 00:00:00, next-day 00:00:00)`` window, i.e.
    ``start_time < next_day`` and ``end_time >= day``. Rows with a null start or
    end time are dropped.
    """
    day_start = F.to_timestamp(F.lit(day), "yyyy-MM-dd")
    day_end = day_start + F.expr("INTERVAL 1 DAY")
    return df.where(
        F.col("start_time").isNotNull()
        & F.col("end_time").isNotNull()
        & (F.col("start_time") < day_end)
        & (F.col("end_time") >= day_start)
    )


def duration_hour_deciles(df):
    """Deciles (10%..90%) of the play duration, in hours, across all rows.

    Returns a list of 9 values [p10, p20, ..., p90] where duration is
    ``(end_time - start_time) / 3600``.
    """
    hours = df.select(
        ((F.col("end_time").cast("long") - F.col("start_time").cast("long")) / 3600.0).alias(
            "duration_hours"
        )
    )
    probs = [i / 10.0 for i in range(1, 10)]
    arr = ", ".join(str(p) for p in probs)
    return hours.select(
        F.expr(f"percentile_approx(duration_hours, array({arr}), 10000)").alias("deciles")
    ).first()["deciles"]


def daily_stats(df, day, gap_seconds=1800):
    """Run the full daily statistics pipeline for a single day.

    Returns a dict with the raw/merged row counts, distinct name/song/relation
    counts (on the merged data), and the duration deciles in hours.
    """
    # 1) Relations on the given day.
    day_df = relations_on_day(df, day).cache()

    # 2) Row count before merging.
    raw_count = day_df.count()

    # 3) Merge by 30 minutes (per name, song).
    merged = merge_song_ranges(day_df, gap_seconds=gap_seconds).cache()

    # 4) Row count after merging.
    merged_count = merged.count()

    # 5) Distinct names, songs, and (name, song) relations on the merged data.
    n_names = merged.select("name").distinct().count()
    n_songs = merged.select("song").distinct().count()
    n_relations = merged.select("name", "song").distinct().count()

    # 6) Deciles of the merged-range durations, in hours.
    deciles = duration_hour_deciles(merged)

    day_df.unpersist()
    merged.unpersist()

    return {
        "day": day,
        "raw_count": raw_count,
        "merged_count": merged_count,
        "n_names": n_names,
        "n_songs": n_songs,
        "n_relations": n_relations,
        "duration_hour_deciles": deciles,
    }


def main():
    # CLI args: <table> <dt_from> <dt_to> [day]
    #   e.g.  my_table 20250101 20250331
    #   e.g.  my_table 20260401 20260401 2026-04-01   (daily-stats mode)
    table = sys.argv[1] if len(sys.argv) > 1 else "your_table"
    dt_from = sys.argv[2] if len(sys.argv) > 2 else "20250101"
    dt_to = sys.argv[3] if len(sys.argv) > 3 else "20250331"
    day = sys.argv[4] if len(sys.argv) > 4 else None

    spark = SparkSession.builder.appName("SongGaps").getOrCreate()

    df = load_table(spark, table, dt_from, dt_to)

    if day is not None:
        stats = daily_stats(df, day)
        print(f"--- Daily stats for {stats['day']} ---")
        print(f"1+2) rows on the day              : {stats['raw_count']}")
        print(f"3+4) rows after 30-min merge      : {stats['merged_count']}")
        print(f"5)   distinct names               : {stats['n_names']}")
        print(f"5)   distinct songs               : {stats['n_songs']}")
        print(f"5)   distinct (name, song)        : {stats['n_relations']}")
        labels = [f"p{p}0" for p in range(1, 10)]
        print("6)   duration deciles (hours):")
        for label, value in zip(labels, stats["duration_hour_deciles"]):
            print(f"       {label}: {value}")
        spark.stop()
        return

    # Q1 + Q2: max and 99th-percentile of (dt - start_time), in one pass.
    diff_df = dt_minus_start(df)
    q12 = diff_df.select(
        F.max("dt_minus_start_days").alias("max_diff_days"),
        F.expr("percentile_approx(dt_minus_start_days, 0.99, 10000)").alias("p99_diff_days"),
    ).first()

    # Q3 + Q4: max and 99th-percentile of successive start_time gaps, one pass.
    gaps = successive_start_gaps(df)
    q34 = gaps.select(
        F.max("gap_seconds").alias("max_gap_seconds"),
        F.expr("percentile_approx(gap_seconds, 0.99, 10000)").alias("p99_gap_seconds"),
    ).first()

    print(f"Q1  max(dt - start_time)        : {q12['max_diff_days']} days")
    print(f"Q2  p99(dt - start_time)        : {q12['p99_diff_days']} days")
    print(f"Q3  max successive start gap    : {q34['max_gap_seconds']} seconds")
    print(f"Q4  p99 successive start gap    : {q34['p99_gap_seconds']} seconds")

    spark.stop()


if __name__ == "__main__":
    main()