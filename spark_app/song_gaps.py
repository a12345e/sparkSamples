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


def main():
    # CLI args: <table> <dt_from> <dt_to>   e.g.  my_table 20250101 20250331
    table = sys.argv[1] if len(sys.argv) > 1 else "your_table"
    dt_from = sys.argv[2] if len(sys.argv) > 2 else "20250101"
    dt_to = sys.argv[3] if len(sys.argv) > 3 else "20250331"

    spark = SparkSession.builder.appName("SongGaps").getOrCreate()

    df = load_table(spark, table, dt_from, dt_to)

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