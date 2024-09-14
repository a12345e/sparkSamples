import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row, BooleanType
import pyspark.sql.functions as F
from spark.find_match.find_match_ranges import TransactionAnalysis
from tests.infra.dataframes_helpers import compare_dataframes, RowsBuilder, print_dataframe_schema_and_rows
from tests.infra.default_spark_builder import spark


find_match_ranges = TransactionAnalysis(anchor_col='a', matched_col='b',
                                        time_col='t', status_col='status',
                                        other_matches=['o1','o2'],
                                        status_namings={
                                   'start': TransactionAnalysis.Status.START.value,
                                   'update': TransactionAnalysis.Status.UPDATE.value,
                                   'end': TransactionAnalysis.Status.END.value
                                   }, start_valid_column='v')

@pytest.mark.parametrize("input_row, expected_row",[
        pytest.param(Row(status="other"),Row(status=None), id='check enumeration of invalid status'),
        pytest.param(Row(status=None),Row(status=None), id='check enumeration of none status'),
        pytest.param(Row(status="start"), Row(status=find_match_ranges.Status.START.value), id='check enumeration of start status'),
        pytest.param(Row(status="update"), Row(status=find_match_ranges.Status.UPDATE.value), id='check enumeration of update status'),
        pytest.param(Row(status="end"), Row(status=find_match_ranges.Status.END.value), id='check enumeration of end status')])

def test_enumerate_status_colum(spark, input_row, expected_row):
    input_schema = StructType([
        StructField("status", StringType(), True),
        StructField("o1", StringType(), True),
    ])
    expected_schema = StructType([
        StructField("status", IntegerType(), True),
        StructField("o1", StringType(), True),
    ])
    df_a = RowsBuilder(input_schema, session=spark).add(input_row).df
    df_a = find_match_ranges._enumerate_status_column(df_a)
    df_e = RowsBuilder(expected_schema, spark).add(expected_row).df
    compare_dataframes(df_a, df_e)


@pytest.mark.parametrize("input_row, expected_rows",[
        pytest.param(Row(a="a",t=1),[Row(a="a",t=1)], id='check row is not removed if both time and anchor are not null'),
        pytest.param(Row(a=None,t=1),[], id='check row is filtered out if anchor is null'),
        pytest.param(Row(a='a',t=None),[], id='check row is filtered out if time is null')])
def test_initial_filter_non_nulls_anchor_col_time_col(spark, input_row, expected_rows):
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("t", IntegerType(), True),
    ])
    df_a = RowsBuilder(schema,spark).add(input_row).df
    df_a = find_match_ranges._initial_filter_non_nulls_anchor_and_time_col(df_a)
    df_e = RowsBuilder(schema,spark).add_rows(expected_rows).df
    compare_dataframes(df_e,df_a)


@pytest.mark.parametrize("anchor, input_rows, expected_rows",[
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row()],[Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)], id='one full row with start validation=true is valid'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row(),Row(o2=2)],
             [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False),
              Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=2, v=False)], id='invalid because two rows for same key (a=1,t=1) with o1=1 and o1=2'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row(),Row(b=1)],
             [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False),
              Row(a=0, b=1, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False)], id='invalid because two rows for same key (a=1,t=1) with b=1 and b=0'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row(),Row(a=1)],
             [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True),
              Row(a=1, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)], id='Valid because values of a in this case are in keys'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=None, o2=None, v=True)
             , [Row(), Row(o1=1),Row(o2=1),Row(o2=1),Row(o2=None)],
               [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
                ],
                 id='Valid and o1,o2 are reduced to the non null values'),
])

def test_reduce_and_validate_transaction_start_points_with_anchor_col_in_keys(spark, anchor, input_rows, expected_rows):
    schema = StructType([StructField("a", IntegerType(), True),
                         StructField("b", IntegerType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("t", IntegerType(), True),
                         StructField("o1", IntegerType(), True),
                         StructField("o2", IntegerType(), True),
                         StructField("v", BooleanType(), True),
                         ])
    df_a = RowsBuilder(schema, spark, anchor).add_rows(input_rows).df
    df_a = find_match_ranges.reduce_and_validate_transaction_start_points(
        df=df_a,
        key_cols=[find_match_ranges._anchor_col, find_match_ranges._status_col, find_match_ranges._time_col],
        match_cols=[find_match_ranges._matched_col] + find_match_ranges._other_matches)
    df_e = RowsBuilder(schema,spark).add_rows(expected_rows).df
    compare_dataframes(df_e,df_a)

@pytest.mark.parametrize("anchor, input_rows, expected_rows",[
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row()],[Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)], id='one full row with start validation=true is valid'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row(),Row(o2=2)],
             [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False),
              Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=2, v=False)], id='invalid because two rows for same key (a=1,t=1) with o1=1 and o1=2'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row(),Row(b=1)],
             [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False),
              Row(a=0, b=1, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False)], id='invalid because two rows for same key (a=1,t=1) with b=1 and b=0'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row(),Row(a=1)],
             [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False),
              Row(a=1, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False)], id='Invalid because two values for a with (b=0,t=1)'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=None, o2=None, v=True)
             , [Row(), Row(o1=1),Row(o2=1)],
               [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
                ],
                 id='Valid because values of a there is only one none null value for o1,o2'),
         pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=None, o2=None, v=True)
        , [Row(), Row(status=3), Row(o1=1), Row(o2=1), Row(status=3, o1=1), Row(status=3, o2=1)],
                 [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True),
                  Row(a=0, b=0, status=TransactionAnalysis.Status.END.value, t=1, o1=1, o2=None, v=True),
                  Row(a=0, b=0, status=TransactionAnalysis.Status.END.value, t=1, o1=None, o2=1, v=True),
                  Row(a=0, b=0, status=TransactionAnalysis.Status.END.value, t=1, o1=None, o2=None, v=True)
                  ],
                 id='Valid because values of a there is only one none null value for o1,o2'),

])
def test_prepare_valid_transaction_start_points(spark, anchor, input_rows, expected_rows):
    schema = StructType([StructField("a", IntegerType(), True),
                         StructField("b", IntegerType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("t", IntegerType(), True),
                         StructField("o1", IntegerType(), True),
                         StructField("o2", IntegerType(), True),
                         StructField("v", BooleanType(), True),
                         ])
    df_a = RowsBuilder(schema, spark, anchor).add_rows(input_rows).df
    df_a = find_match_ranges._prepare_valid_transaction_start_points(df=df_a)
    df_e = RowsBuilder(schema,spark).add_rows(expected_rows).df
    compare_dataframes(df_e,df_a)


""""
        NEXT_END_FOR_SAME_MATCH = 'next_match_end_same' # the normal start end events for the same transaction
        NEXT_END_NULL = 'next_match_end_null'
        NEXT_START_DIFFERENT_MATCH = 'next_match_start_different'
        NEXT_END_DIFFERENT_MATCH = 'next_match_end_different'
        NEXT_START_FOR_SAME_MATCH = 'next_match_start'
"""
@pytest.mark.parametrize("input_rows, expected_rows",[
#     pytest.param(
# [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
#                 Row(a=0, b=0, status=3, t=2, o1=1, o2=1, v=True)],
#         [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_end_same',end_time=2),
#          Row(a=0, b=0,status=3,t=2, o1=1, o2=1, v=True, start_time=None, end_reason=None,end_time=None)], id='simple  start end')
#     , pytest.param(
# [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
#         Row(a=0, b=0, status=3, t=1, o1=1, o2=1, v=True)],
#
#         [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_end_same',end_time=1),
#          Row(a=0, b=0,status=3,t=1, o1=1, o2=1, v=True, start_time=None, end_reason=None,end_time=None)], id='simple  start end same time')
#
#     , pytest.param(
# [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
#         Row(a=0, b=0, status=3, t=0, o1=1, o2=1, v=True)],
#
#         [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason=None,end_time=None),
#          Row(a=0, b=0,status=3,t=0, o1=1, o2=1, v=True, start_time=None, end_reason=None,end_time=None)], id='end time before start time')

#      pytest.param(
# [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
#         Row(a=0, b=None, status=3, t=2, o1=1, o2=1, v=True)],
#
#         [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_end_null',end_time=2),
#          Row(a=0, b=None,status=3,t=2, o1=1, o2=1, v=True, start_time=None, end_reason=None,end_time=None)], id='next match null'),
#
#      pytest.param(
# [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
#         Row(a=0, b=1, status=1, t=2, o1=1, o2=1, v=True)],
#
#         [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_start_different',end_time=2),
#          Row(a=0, b=1,status=1,t=2, o1=1, o2=1, v=True, start_time=2, end_reason=None,end_time=None)], id='next_match_start_different now with anchor changing'),

#      pytest.param(
# [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
#         Row(a=1, b=0, status=1, t=2, o1=1, o2=1, v=True)],
#
#         [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_start_different',end_time=2),
#          Row(a=1, b=0,status=1,t=2, o1=1, o2=1, v=True, start_time=2, end_reason=None,end_time=None)], id='next_match_start_different now with anchor changing')

#      pytest.param(
# [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
#         Row(a=0, b=1, status=3, t=2, o1=1, o2=1, v=True)],
#
#         [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_end_different',end_time=2),
#          Row(a=0, b=1,status=3,t=2, o1=1, o2=1, v=True, start_time=None, end_reason=None,end_time=None)], id='next_match_start_different now with anchor changing'),
#
#      pytest.param(
# [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
#         Row(a=1, b=0, status=3, t=2, o1=1, o2=1, v=True)],
#
#         [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_end_different',end_time=2),
#          Row(a=1, b=0,status=3,t=2, o1=1, o2=1, v=True, start_time=None, end_reason=None,end_time=None)], id='next_match_start_different now with anchor changing')

#      pytest.param(
# [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
#         Row(a=0, b=0, status=1, t=2, o1=1, o2=1, v=True)],
#
#         [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_start',end_time=2),
#          Row(a=0, b=0,status=1,t=2, o1=1, o2=1, v=True, start_time=2, end_reason=None,end_time=None)], id='next_match_start now with anchor changing')

     pytest.param(
[

        Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True), # the start row with time =1
        #  -- all the following have time <= 1 so not have any impact
        Row(a=0, b=0, status=3, t=0, o1=1, o2=11, v=True),
        Row(a=0, b=0, status=3, t=1, o1=1, o2=12, v=True),
        Row(a=0, b=1, status=3, t=1, o1=1, o2=13, v=True),
        Row(a=1, b=0, status=3, t=1, o1=1, o2=14, v=True),
        Row(a=0, b=None, status=3, t=1, o1=1, o2=15, v=True),
        Row(a=None, b=0, status=3, t=1, o1=1, o2=16, v=True),

        #these two are in less preference than the next two because of nulls less preferred then values
        Row(a=0, b=None, status=3, t=2, o1=1, o2=17, v=True),
        Row(a=None, b=0, status=3, t=3, o1=1, o2=18, v=True),

        #these two provide two same type of ending yet later the earlier will be chosen
        Row(a=0, b=1, status=3, t=2, o1=1, o2=1, v=True),
        Row(a=1, b=0, status=3, t=3, o1=1, o2=1, v=True),
],
        [
            Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_end_different', end_time=2) ,
            Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_end_null', end_time=3) ,
            Row(a=0, b=None, status=3, t=1, o1=1, o2=15, v=True, start_time=None, end_reason=None, end_time=None) ,
            Row(a=0, b=None, status=3, t=2, o1=1, o2=17, v=True, start_time=None, end_reason=None, end_time=None) ,
            Row(a=0, b=0, status=3, t=0, o1=1, o2=11, v=True, start_time=None, end_reason=None, end_time=None) ,
            Row(a=None, b=0, status=3, t=1, o1=1, o2=16, v=True, start_time=None, end_reason=None, end_time=None) ,
            Row(a=0, b=0, status=3, t=1, o1=1, o2=12, v=True, start_time=None, end_reason=None, end_time=None) ,
            Row(a=1, b=0, status=3, t=1, o1=1, o2=14, v=True, start_time=None, end_reason=None, end_time=None) ,
            Row(a=None, b=0, status=3, t=3, o1=1, o2=18, v=True, start_time=None, end_reason=None, end_time=None) ,
            Row(a=1, b=0, status=3, t=3, o1=1, o2=1, v=True, start_time=None, end_reason=None, end_time=None) ,
            Row(a=0, b=1, status=3, t=1, o1=1, o2=13, v=True, start_time=None, end_reason=None, end_time=None) ,
            Row(a=0, b=1, status=3, t=2, o1=1, o2=1, v=True, start_time=None, end_reason=None, end_time=None) ,
        ],
         id='condition when we get two endings of type next_match_end_different and one of them has lower time'
            '. Also we see that ending for same key of (anchor, time) does not have any influence because we'
            'decided that we take descending order of ')


])
def test_mark_end_time_with_ending_reason(spark, input_rows, expected_rows):
    output_schema = StructType([StructField("a", IntegerType(), True),
                         StructField("b", IntegerType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("t", IntegerType(), True),
                         StructField("o1", IntegerType(), True),
                         StructField("o2", IntegerType(), True),
                         StructField("v", BooleanType(), True),
                         StructField("start_time", IntegerType(), True),
                         StructField("end_reason", StringType(), True),
                         StructField("end_time", IntegerType(), True),
                         ])
    input_schema = StructType([StructField("a", IntegerType(), True),
                         StructField("b", IntegerType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("t", IntegerType(), True),
                         StructField("o1", IntegerType(), True),
                         StructField("o2", IntegerType(), True),
                         StructField("v", BooleanType(), True),
                         ])
    df_a = RowsBuilder(input_schema, spark).add_rows(input_rows).df
    df_a = find_match_ranges._mark_end_time_with_ending_reason(df=df_a, match_columns=["a","b"])
    df_e = RowsBuilder(output_schema,spark).add_rows(expected_rows).df
    compare_dataframes(df_e,df_a)





def test_get_close_transactions():
    schema = StructType([StructField('a', IntegerType(), True), StructField('b', IntegerType(), True),
                         StructField('status', IntegerType(), True), StructField('t', IntegerType(), True),
                         StructField('o1', IntegerType(), True), StructField('o2', IntegerType(), True),
                         StructField('v', BooleanType(), True), StructField('start_time', IntegerType(), True),
                         StructField('end_reason', StringType(), True), StructField('end_time', IntegerType(), True)])
    rows = [
        Row(a=0, b=0, status='start', t=1, o1=1, o2=1, v=True, start_time=1, end_reason='next_match_end_same',
            end_time=2),
        Row(a=0, b=0, status='start', t=3, o1=1, o2=1, v=True, start_time=3, end_reason='next_match_end_null',
            end_time=4),
        Row(a=0, b=0, status='start', t=3, o1=1, o2=1, v=True, start_time=3, end_reason='next_match_start', end_time=5),
        Row(a=0, b=0, status='start', t=5, o1=1, o2=1, v=True, start_time=5, end_reason='next_match_start_different',
            end_time=6),
        Row(a=0, b=0, status='start', t=5, o1=1, o2=1, v=True, start_time=5, end_reason='next_match_start', end_time=7),
        Row(a=0, b=0, status='start', t=7, o1=1, o2=1, v=True, start_time=7, end_reason='next_match_end_different',
            end_time=8),
        Row(a=0, b=0, status='start', t=7, o1=1, o2=1, v=True, start_time=7, end_reason='next_match_start', end_time=9),
        Row(a=0, b=0, status='start', t=9, o1=1, o2=1, v=True, start_time=9, end_reason='next_match_start',
            end_time=10),
        Row(a=0, b=1, status='start', t=6, o1=1, o2=1, v=True, start_time=6, end_reason='next_match_start_different',
            end_time=7),
        Row(a=0, b=1, status='start', t=6, o1=1, o2=1, v=True, start_time=6, end_reason='next_match_end_same',
            end_time=8),
        Row(a=1, b=1, status='start', t=20, o1=1, o2=1, v=True, start_time=20, end_reason='next_match_end_different',
            end_time=21),
        Row(a=0, b=None, status='end', t=4, o1=1, o2=1, v=True, start_time=None, end_reason=None, end_time=None),
        Row(a=0, b=0, status='end', t=2, o1=1, o2=1, v=True, start_time=None, end_reason=None, end_time=None),
        Row(a=0, b=0, status='start', t=10, o1=1, o2=1, v=True, start_time=10, end_reason=None, end_time=None),
        Row(a=0, b=1, status='end', t=8, o1=1, o2=1, v=True, start_time=None, end_reason=None, end_time=None),
        Row(a=2, b=1, status='end', t=21, o1=1, o2=1, v=True, start_time=None, end_reason=None, end_time=None),
    ]
    df_a = RowsBuilder(schema, spark).add_rows(rows).df
    df_a = find_match_ranges._get_close_transactions(df_a)
    schema = StructType([StructField('a', IntegerType(), True), StructField('b', IntegerType(), True),
                         StructField('status', IntegerType(), True), StructField('t', IntegerType(), True),
                         StructField('o1', IntegerType(), True), StructField('o2', IntegerType(), True),
                         StructField('v', BooleanType(), True), StructField('start_time', IntegerType(), True),
                         StructField('final_end_time', IntegerType(), True),
                         StructField('final_end_reason', StringType(), True)])
    rows = [
        Row(a=0, b=0, status='start', t=1, o1=1, o2=1, v=True, start_time=1, final_end_time=2,
            final_end_reason='next_match_end_same'),
        Row(a=0, b=0, status='start', t=3, o1=1, o2=1, v=True, start_time=3, final_end_time=4,
            final_end_reason='next_match_end_null'),
        Row(a=0, b=0, status='start', t=5, o1=1, o2=1, v=True, start_time=5, final_end_time=6,
            final_end_reason='next_match_start_different'),
        Row(a=0, b=0, status='start', t=7, o1=1, o2=1, v=True, start_time=7, final_end_time=8,
            final_end_reason='next_match_end_different'),
        Row(a=0, b=0, status='start', t=9, o1=1, o2=1, v=True, start_time=9, final_end_time=10,
            final_end_reason='next_match_start'),
        Row(a=0, b=1, status='start', t=6, o1=1, o2=1, v=True, start_time=6, final_end_time=7,
            final_end_reason='next_match_start_different'),
        Row(a=1, b=1, status='start', t=20, o1=1, o2=1, v=True, start_time=20, final_end_time=21,
            final_end_reason='next_match_end_different'),
    ]
    df_e = RowsBuilder(schema, spark).add_rows(rows).df
    compare_dataframes(df_a, df_e)


def test_get_connect_succeeding_transactions():
    schema = StructType([StructField('a', IntegerType(), True), StructField('b', IntegerType(), True),
                         StructField('status', IntegerType(), True), StructField('t', IntegerType(), True),
                         StructField('o1', IntegerType(), True), StructField('o2', IntegerType(), True),
                         StructField('v', BooleanType(), True), StructField('start_time', IntegerType(), True),
                         StructField('final_end_time', IntegerType(), True),
                         StructField('final_end_reason', StringType(), True)])
    rows = [
    Row(a=1, b=1, status=1, t=1, o1=1, o2=1, v=True, start_time=1, final_end_time=21,
        final_end_reason='next_match_end_different'),
    Row(a=1, b=1, status=1, t=1, o1=1, o2=1, v=True, start_time=21, final_end_time=25,
        final_end_reason='next_match_end_different')
        ]
    df  = RowsBuilder(schema, spark).add_rows(rows).df
    df_a = find_match_ranges._connect_succeeding_transactions(df)
    print_dataframe_schema_and_rows(df_a)