from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row, ArrayType
import pyspark.sql.functions as F
from spark.find_match.find_match_ranges import FindMatchRange
from tests.infra.dataframes_helpers import complete_row_to_schema, compare_dataframes, dataframe_create
from tests.infra.default_spark_builder import DefaultSparkFactory

spark = DefaultSparkFactory().spark

find_match_ranges = FindMatchRange()
def test_initial_filter_non_nulls_a_col_time_col():
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("time", IntegerType(), True),
        StructField("other", StringType(), True),
    ])
    rows = [Row(a="a", time=1),
            Row(a="a", time=None),
            Row(a=None, time=2),
            Row(a=None, time=None)]
    df =dataframe_create(rows, schema, complete_with_nulls=True)
    df = find_match_ranges._initial_filter_non_nulls_a_col_time_col(df,a_col='a', time_col='time')
    rows_expected = [Row(a="a", time=1)]
    rows_expected = complete_row_to_schema(rows_expected, schema, complete_with_nulls=True)
    df_expected =  spark.createDataFrame(rows_expected, schema)
    compare_dataframes(df_expected,df)


def test_enumerate_status_colum():
    schema = StructType([
        StructField("status", StringType(), True),
        StructField("other", StringType(), True),
    ])
    rows = [Row(status="invalid"),
            Row(status="start"),
            Row(status="update"),
            Row(status="end")]
    df = dataframe_create(rows, schema, complete_with_nulls=True)
    df = find_match_ranges.enumerate_status_column(df, status_col='status',status_values={
         find_match_ranges.Status.INVALID: "invalid",
         find_match_ranges.Status.START: "start",
         find_match_ranges.Status.UPDATE : "update",
         find_match_ranges.Status.END: "end"
    })
    schema = StructType([
        StructField("status", IntegerType(), True),
        StructField("other", StringType(), True),
    ])
    expected = dataframe_create([Row(status=i) for i in range(0,4)],
                                     schema, complete_with_nulls=True)
    compare_dataframes(expected, df)

def test_keep_only_start_end_status():
    schema = StructType([
        StructField("status", IntegerType(), True),
    ])
    rows = [Row(status=find_match_ranges.Status.INVALID.value),
            Row(status=find_match_ranges.Status.START.value),
            Row(status=find_match_ranges.Status.UPDATE.value),
            Row(status=find_match_ranges.Status.END.value)]
    df = dataframe_create(rows, schema, complete_with_nulls=True)
    df = find_match_ranges.keep_only_start_end_status(df, 'status')
    expected = dataframe_create(
        [Row(status=find_match_ranges.Status.START.value),
         Row(status=find_match_ranges.Status.END.value)],
         schema)
    compare_dataframes(expected, df)

def test_avoid_b_col_null_on_start_status():
    schema = StructType([
        StructField("status", IntegerType(), True),
        StructField("b_col", StringType(), True),
        StructField("other", StringType(), True),
    ])
    rows = [Row(status=find_match_ranges.Status.UPDATE.value, b_col='x'),
            Row(status=find_match_ranges.Status.UPDATE.value, b_col=None),
            Row(status=find_match_ranges.Status.START.value, b_col='x'),
            Row(status=find_match_ranges.Status.START.value, b_col=None)]
    df = dataframe_create(rows, schema)
    df = find_match_ranges.avoid_b_col_null_on_start_status(df, b_col='b_col',status_col='status')
    rows = [Row(status=find_match_ranges.Status.UPDATE.value, b_col='x'),
            Row(status=find_match_ranges.Status.UPDATE.value, b_col=None),
            Row(status=find_match_ranges.Status.START.value, b_col='x')]
    expected =  dataframe_create(rows, schema)
    compare_dataframes(expected, df)



def test_invalidate_multiple_match_solutions():

    schema = StructType([StructField("a", IntegerType(), True),
                         StructField("b", IntegerType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("t", IntegerType(), True),
                         StructField("o1", IntegerType(), True),
                         StructField("o2", IntegerType(), True)
                         ])

    rows = [Row(a=1,b=1, status=FindMatchRange.Status.START.value,t=1,o1=None, o2=1),
            Row(a=1, b=1, status=FindMatchRange.Status.END.value, t=2, o1=1, o2=None),

            Row(a=2, b=2, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1),
            Row(a=2, b=2, status=FindMatchRange.Status.START.value, t=2, o1=2, o2=1),
            Row(a=2, b=2, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=2),

            Row(a=3, b=3, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1),
            Row(a=3, b=4, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1),

            Row(a=5, b=5, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1),
            Row(a=6, b=5, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1)

            ]
    df = dataframe_create(rows, schema)
    df.show(truncate=False)
    df.printSchema()
    df = find_match_ranges.invalidate_multiple_match_solutions(
        df=df,
        key_cols=['a','status','t'],
        match_cols=['o1','o2','b'], validity_column='valid')
    df.printSchema()

    schema = StructType([StructField("id", IntegerType(), True),
                         StructField("col", StringType(), True)])
    #expected = dataframe_create(rows, schema)
    #compare_dataframes(expected, df)