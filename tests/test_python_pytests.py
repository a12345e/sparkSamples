from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Row, FloatType, LongType, DoubleType, TimestampType
from tests.generate_dataframes_helpers import  row_validate_schema, dataframe_create

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


