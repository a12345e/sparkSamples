from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row

from spark.find_match.find_match_ranges import initial_filter_non_nulls_a_col_time_col
from tests.infra.dataframes_helpers import complete_row_to_schema, compare_dataframes
from tests.infra.default_spark_builder import DefaultSparkFactory

def test_initial_filter_non_nulls_a_col_time_col():
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("time", IntegerType(), True),
        StructField("other", StringType(), True),
    ])
    spark = DefaultSparkFactory().spark
    rows = [Row(a="a", time=1),
            Row(a="a", time=None),
            Row(a=None, time=2),
            Row(a=None, time=None)]
    completed_rows = complete_row_to_schema(rows, schema, complete_with_nulls=True)
    df = spark.createDataFrame(completed_rows, schema)
    df = initial_filter_non_nulls_a_col_time_col(df,a_col='a', time_col='time')
    rows_expected = [Row(a="a", time=1)]
    rows_expected = complete_row_to_schema(rows_expected, schema, complete_with_nulls=True)
    df_expected =  spark.createDataFrame(rows_expected, schema)
    compare_dataframes(df_expected,df)
