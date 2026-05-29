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

from pyspark.sql import SparkSession


def build_dataframe(spark):
    data = [
        ("Alice", 30, "Engineering"),
        ("Bob", 25, "Sales"),
        ("Carol", 35, "Engineering"),
        ("Dave", 28, "Marketing"),
        ("Eve", 42, "Engineering"),
    ]
    columns = ["name", "age", "department"]
    return spark.createDataFrame(data, columns)


def engineers_over_30(df):
    return df.filter((df.department == "Engineering") & (df.age > 30))


def main():
    spark = SparkSession.builder.appName("DataFrameExample").master("local[*]").getOrCreate()

    import pyspark

    jvm = spark.sparkContext._jvm
    java_version = jvm.java.lang.System.getProperty("java.version")
    java_vendor = jvm.java.lang.System.getProperty("java.vendor")

    print(f"Python  : {sys.version.split()[0]}")
    print(f"PySpark : {pyspark.__version__}")
    print(f"Spark   : {spark.version}")
    print(f"Java    : {java_version} ({java_vendor})")
    print()

    df = build_dataframe(spark)

    print("Full DataFrame:")
    df.show()

    print("Engineers older than 30:")
    engineers_over_30(df).show()

    spark.stop()


if __name__ == "__main__":
    main()
