from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row, ArrayType, BooleanType
import pyspark.sql.functions as F
from spark.find_match.find_match_ranges import FindMatchRange
from tests.infra.dataframes_helpers import complete_row_to_schema, compare_dataframes, dataframe_create, RowsBuilder
from tests.infra.default_spark_builder import DefaultSparkFactory

spark = DefaultSparkFactory().spark

find_match_ranges = FindMatchRange(hero_col='a', matched_col='b',
                                   time_col='t', status_col='status',
                                   other_matches=['o1','o2'],
                                   status_namings={
                                   'invalid': FindMatchRange.Status.INVALID.value,
                                   'start': FindMatchRange.Status.START.value,
                                   'update': FindMatchRange.Status.UPDATE.value,
                                   'end': FindMatchRange.Status.END.value
                                   }, start_valid_column='v')
def test_initial_filter_non_nulls_a_col_time_col():
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("t", IntegerType(), True),
        StructField("other", StringType(), True),
    ])
    rows = [
            Row(a="a", t=None),
            Row(a=None, t=2),
            Row(a=None, t=None)]
    df_a = RowsBuilder(schema,spark).add_rows(rows).df
    df_a = find_match_ranges._initial_filter_non_nulls_hero_and_time_col(df_a)
    df_e = RowsBuilder(schema,spark).add_row(Row(a="a", t=1)).df
    compare_dataframes(df_e,df_a)


def test_enumerate_status_colum():
    schema = StructType([
        StructField("status", StringType(), True),
        StructField("o1", StringType(), True),
    ])
    rows = [Row(status="invalid"),
            Row(status="start"),
            Row(status="update"),
            Row(status="end")]
    df_a = RowsBuilder(schema, spark).add_rows(rows).df
    df_a = find_match_ranges._enumerate_status_column(df_a)
    schema = StructType([
        StructField("status", IntegerType(), True),
        StructField("o1", StringType(), True),
    ])
    df_e = RowsBuilder(schema, spark).add_rows([Row(status=i) for i in range(0,4)]).df
    df_a.show()
    compare_dataframes(df_a, df_e)

def test_keep_only_start_end_status():
    schema = StructType([
        StructField("status", IntegerType(), True),
    ])
    rows = [Row(status=find_match_ranges.Status.INVALID.value),
            Row(status=find_match_ranges.Status.START.value),
            Row(status=find_match_ranges.Status.UPDATE.value),
            Row(status=find_match_ranges.Status.END.value)]
    df_a = find_match_ranges._keep_only_start_end_status(RowsBuilder(schema, spark).add_rows(rows).df)
    df_e = RowsBuilder(schema, spark).add_rows([Row(status=find_match_ranges.Status.START.value),
         Row(status=find_match_ranges.Status.END.value)]).df
    compare_dataframes(df_a, df_e)

def test_avoid_b_col_null_on_start_status():
    schema = StructType([
        StructField("status", IntegerType(), True),
        StructField("b", StringType(), True),
        StructField("o2", StringType(), True),
    ])
    rows = [Row(status=find_match_ranges.Status.UPDATE.value, b='x'),
            Row(status=find_match_ranges.Status.UPDATE.value, b=None),
            Row(status=find_match_ranges.Status.START.value, b='x'),
            Row(status=find_match_ranges.Status.START.value, b=None)]
    df_a = find_match_ranges._avoid_match_col_null_on_start_status(RowsBuilder(schema, spark).add_rows(rows).df)
    rows = [Row(status=find_match_ranges.Status.UPDATE.value, b='x'),
            Row(status=find_match_ranges.Status.UPDATE.value, b=None),
            Row(status=find_match_ranges.Status.START.value, b='x')]
    df_e = RowsBuilder(schema, spark).add_rows(rows).df
    compare_dataframes(df_e, df_a)



def test_invalidate_multiple_match_solutions():
    schema = StructType([StructField("a", IntegerType(), True),
                         StructField("b", IntegerType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("t", IntegerType(), True),
                         StructField("o1", IntegerType(), True),
                         StructField("o2", IntegerType(), True),
                         StructField("v", BooleanType(), True),
                         ])
    rows = [
            #these two rows will create one valid start point reducing the other matches
            Row(a=1,b=1, status=FindMatchRange.Status.START.value,t=1,o1=None, o2=1,v=True),
            Row(a=1, b=1, status=FindMatchRange.Status.START.value, t=1, o1=1, o2=None, v=True),

            # these two rows will create one valid end point reducing the other matches
            Row(a=1, b=1, status=FindMatchRange.Status.END.value, t=2, o1=1, o2=None, v=True),
            Row(a=1, b=1, status=FindMatchRange.Status.END.value, t=2, o1=None, o2=1, v=True),

            Row(a=2, b=2, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1,  v=True),
            Row(a=2, b=2, status=FindMatchRange.Status.START.value, t=2, o1=2, o2=1, v=True),
            Row(a=2, b=2, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=2, v=True),

            Row(a=3, b=3, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1,  v=True),
            Row(a=3, b=4, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1,  v=True),

            Row(a=5, b=5, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1,  v=True),
            Row(a=6, b=5, status=FindMatchRange.Status.START.value, t=2, o1=1, o2=1, v=True)
            ]
    df_a = RowsBuilder(schema, spark).add_rows(rows).df
    df_a = df_a.withColumn(find_match_ranges._start_validity_column, F.lit(True))
    df_a.show(truncate=False)
    df_a.printSchema()
    df_a = find_match_ranges._prepare_valid_start_events(
        df=df_a,
        key_cols=['a','status','t'],
        match_cols=['o1','o2','b'])
    df_a.show()
    df_a = find_match_ranges._prepare_valid_start_events(
         df=df_a,
         key_cols=['b','status','t'],
         match_cols=['o1','o2','a'])
    df_a.show()
    # schema = StructType([StructField("id", IntegerType(), True),
    #                      StructField("col", StringType(), True)])
    #expected = dataframe_create(rows, schema)
    #compare_dataframes(expected, df)