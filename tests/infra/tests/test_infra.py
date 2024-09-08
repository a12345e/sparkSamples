import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Row, DoubleType, TimestampType
from tests.infra.dataframes_helpers import row_validate_schema, dataframe_create, RowsBuilder

from tests.infra.default_spark_builder import DefaultSparkFactory

spark = DefaultSparkFactory().spark

def test_spark_create_singleton():
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
        StructField("birthday", TimestampType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    rows = [Row( name="Alice", age=25), Row(name="Bob", age=30), Row(name="Cathy",age=28)]
    row_validate_schema(rows,schema)
    df = dataframe_create(rows, schema, complete_with_nulls=False)
    df.show(truncate=False)
    df.printSchema()

def test_rows_builder():
    schema = StructType([
        StructField("height", DoubleType(), True),
        StructField("id", IntegerType(), True),
        StructField("count", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("birthday", TimestampType(), True),
        StructField("age", IntegerType(), True)
    ])
    rb = RowsBuilder(schema, session=spark, anchor= Row( name="Alice", age=25), counter=1)
    rb.add(Row(height=10.0, id=1)).add(Row(id=2, height=13.9))
    df = rb.df
    df.printSchema()
    df.show()
