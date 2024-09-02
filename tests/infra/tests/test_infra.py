import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Row, DoubleType, TimestampType
from tests.infra.dataframes_helpers import  row_validate_schema, dataframe_create

from tests.infra.default_spark_builder import DefaultSparkFactory



def test_things():
    s1 = DefaultSparkFactory()
    s2 = DefaultSparkFactory()

    assert s1 is s2
    assert id(s1) == id(s2)
    assert s1.spark == s2.spark


def test_dataframe_creat():
    schema = StructType([
        StructField("height", DoubleType(), True),
        StructField("birthday", TimestampType(), True),
        StructField("id", IntegerType(), True),
        StructField("count", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    rows = [Row( name="Alice", age=25), Row(name="Bob", age=30), Row(name="Cathy",age=28)]
    row_validate_schema(rows,schema)
    df = dataframe_create(rows, schema, complete_with_nulls=False)
    df.show(truncate=False)
    df.printSchema()
