import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row, BooleanType
import pyspark.sql.functions as F
from spark.find_match.find_match_ranges import TransactionAnalysis
from tests.infra.dataframes_helpers import compare_dataframes, RowsBuilder, print_dataframe_schema_and_rows
from tests.infra.default_spark_builder import spark


find_match_ranges = (TransactionAnalysis(a_col='a', b_col='b',
                                        time_col='t', status_col='status',
                                        other_matches=['o1','o2'],
                                        status_namings={
                                   'start': TransactionAnalysis.Status.START.value,
                                   'update': TransactionAnalysis.Status.UPDATE.value,
                                   'end': TransactionAnalysis.Status.END.value
                                   }, start_valid_column='v').filter_avoid_null_a_col().
                     filter_avoid_null_b_col_when_status_is_start().
                     filter_only_start_end_status())


@pytest.mark.parametrize("input_row, expected_row",[
        pytest.param(Row(status="other"),Row(), id='check enumeration of invalid status'),
        pytest.param(Row(status=None),Row(), id='check enumeration of none status'),
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
        pytest.param(Row(a="a",t=1),[Row(a="a",t=1)], id='column with valid time'),
        pytest.param(Row(a='a',t=None),[], id='check row is filtered out if time is null')])
def test_filter_avoid_null_time_col(spark, input_row, expected_rows):
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("t", IntegerType(), True),
    ])
    df_a = RowsBuilder(schema,spark).add(input_row).df
    df_a = find_match_ranges._filter_avoid_null_time_col(df_a)
    df_e = RowsBuilder(schema,spark).add_rows(expected_rows).df
    compare_dataframes(df_e,df_a)

@pytest.mark.parametrize("input_row, expected_rows",[
        pytest.param(Row(a="a",b="b"),[Row(a="a",b="b")], id='column non null a and b columns'),
        pytest.param(Row(a="a",b=None),[Row(a="a",b=None)], id='column with null b column and non null a column'),
        pytest.param(Row(a=None,b='b'),[Row(a=None,b="b")], id='column with non null b column and null a column'),
        pytest.param(Row(a=None,b=None),[], id='row is filtered out if both ba nd a columns are null')])
def test_filter_avoid_a_col_and_b_col_null_together(spark, input_row, expected_rows):
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True),
    ])
    df_a = RowsBuilder(schema,spark).add(input_row).df
    df_a = find_match_ranges._filter_avoid_a_col_and_b_col_null_together(df_a)
    df_e = RowsBuilder(schema,spark).add_rows(expected_rows).df
    compare_dataframes(df_e,df_a)

@pytest.mark.parametrize("input_row, expected_rows",[
        pytest.param(Row(a='a',b='b',status=TransactionAnalysis.Status.END.value),[Row(a='a',b='b',status=TransactionAnalysis.Status.END.value)], id='a not null is ok'),
        pytest.param(Row(a=None,b='b',status=TransactionAnalysis.Status.END.value),[], id='a is null is and we chose filter_avoid_null_a_col'),
        pytest.param(Row(a='a',b=None,status=TransactionAnalysis.Status.START.value),[], id='b is null and status=Start row removed'),
        pytest.param(Row(a='a',b='b',status=TransactionAnalysis.Status.START.value),[Row(a='a',b='b',status=TransactionAnalysis.Status.START.value)], id='b is not none and status=Start row not removed'),
        pytest.param(Row(a='a',b=None,status=TransactionAnalysis.Status.END.value),[Row(a='a',b=None,status=TransactionAnalysis.Status.END.value)], id='b is none and status!=start so row not removed'),
        pytest.param(Row(a='a',b='b',status=TransactionAnalysis.Status.START.value),[Row(a='a',b='b',status=TransactionAnalysis.Status.START.value)],   id='start status not removed'),
        pytest.param(Row(a='a',b='b',status=TransactionAnalysis.Status.END.value),[Row(a='a',b='b',status=TransactionAnalysis.Status.END.value)],   id='end status not removed'),
        pytest.param(Row(a='a',b='b',status=TransactionAnalysis.Status.UPDATE.value),[],   id='update status is removed')
        ])

def test_filter_optional(spark, input_row, expected_rows):
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True),
        StructField("status", IntegerType(), True),
    ])
    df_a = RowsBuilder(schema,spark).add(input_row).df
    df_a = find_match_ranges._filter_optional(df_a)
    df_e = RowsBuilder(schema,spark).add_rows(expected_rows).df
    compare_dataframes(df_e,df_a)


@pytest.mark.parametrize("default_row, input_rows, expected_rows",[
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row()],[Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)], id='one full row with start validation=true is valid'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
            ,[Row(),Row(o2=2)],
             [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False),
              Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=2, v=False)], id='invalid because two rows for same key (a=1,t=1) with two values for o1 witch is part of the other matches columns'),
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

def test_reduce_and_validate_transaction_start_points_with_a_col_in_keys(spark, default_row, input_rows, expected_rows):
    schema = StructType([StructField("a", IntegerType(), True),
                         StructField("b", IntegerType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("t", IntegerType(), True),
                         StructField("o1", IntegerType(), True),
                         StructField("o2", IntegerType(), True),
                         StructField("v", BooleanType(), True),
                         ])
    df_a = RowsBuilder(schema, spark, default_row).add_rows(input_rows).df
    df_a = find_match_ranges._reduce_and_validate_transaction_start_points(
        df=df_a,
        key_cols=[find_match_ranges._a_col, find_match_ranges._status_col, find_match_ranges._time_col],
        match_cols=[find_match_ranges._b_col] + find_match_ranges._other_matches)
    df_e = RowsBuilder(schema,spark).add_rows(expected_rows).df
    compare_dataframes(df_e,df_a)

@pytest.mark.parametrize("default_row, input_rows, expected_rows",[
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
              Row(a=1, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=False)], id='Invalid because two values for a_col'),
        pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=None, o2=None, v=True)
             , [Row(), Row(o1=1),Row(o2=1)],
               [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True)
                ],
                 id='Valid because values of a there is only one none null value for o1,o2'),
         pytest.param(Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=None, o2=None, v=True)
        , [Row(), Row(status=3), Row(o1=1), Row(o2=1), Row(status=3, o1=1), Row(status=3, o2=1)],
                 [Row(a=0, b=0, status=TransactionAnalysis.Status.START.value, t=1, o1=1, o2=1, v=True),
                  Row(a=0, b=0, status=TransactionAnalysis.Status.END.value, t=1, o1=1, o2=None, v=False),
                  Row(a=0, b=0, status=TransactionAnalysis.Status.END.value, t=1, o1=None, o2=1, v=False),
                  Row(a=0, b=0, status=TransactionAnalysis.Status.END.value, t=1, o1=None, o2=None, v=False)
                  ],
                 id='Valid because values of a there is only one none null value for o1,o2'),

])
def test__prepare_valid_transaction_start_points(spark, default_row, input_rows, expected_rows):
    schema = StructType([StructField("a", IntegerType(), True),
                         StructField("b", IntegerType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("t", IntegerType(), True),
                         StructField("o1", IntegerType(), True),
                         StructField("o2", IntegerType(), True),
                         StructField("v", BooleanType(), True),
                         ])
    df_a = RowsBuilder(schema, spark, default_row).add_rows(input_rows).df
    df_a = find_match_ranges._prepare_valid_transaction_start_points(df=df_a)
    df_e = RowsBuilder(schema,spark).add_rows(expected_rows).df
    compare_dataframes(df_e,df_a)



@pytest.mark.parametrize("input_rows, expected_rows",[
    pytest.param(
[Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
                Row(a=0, b=0, status=3, t=2, o1=1, o2=1, v=True)],
        [
        Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_END_FOR_SAME_MATCH.value, end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_END_FOR_SAME_MATCH.name, end_time=2) ,
        Row(a=0, b=0, status=3, t=2, o1=1, o2=1, v=True, start_time=None, end_reason_ordinal=None, end_reason_name=None, end_time=None) ,
        ], id='simple  start end'),
    pytest.param(
[Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
        Row(a=0, b=0, status=3, t=1, o1=1, o2=1, v=False)],
        [
        Row(a=0, b=0, status=3, t=1, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,end_reason_name=None, end_time=None) ,
        Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=None,end_reason_name=None, end_time=None) ,
        ], id='simple  start end same time so no transaction')

    , pytest.param(
[Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
        Row(a=0, b=0, status=3, t=0, o1=1, o2=1, v=False)],

        [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=None,end_reason_name=None),
         Row(a=0, b=0,status=3,t=0, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,end_reason_name=None,end_time=None)], id='end time before start time so no transaction'),

    pytest.param(
        [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
         Row(a=0, b=1, status=3, t=2, o1=1, o2=1, v=False)],

        [Row(a=0, b=1, status=3, t=2, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None, end_reason_name=None, end_time=None),
            Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1,
             end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_END_MATCH_BREAK.value,
             end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_END_MATCH_BREAK.name, end_time=2)],id='end with match break'),

         pytest.param(
[Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
        Row(a=0, b=None, status=3, t=2, o1=1, o2=1, v=False)],

        [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_END_NULL.value,end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_END_NULL.name,end_time=2),
         Row(a=0, b=None,status=3,t=2, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,end_reason_name=None,end_time=None)], id='next match null'),


         pytest.param(
[Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
        Row(a=0, b=1, status=1, t=2, o1=1, o2=1, v=True)],

        [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1,end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_START_MATCH_BREAK.value,end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_START_MATCH_BREAK.name,end_time=2),
         Row(a=0, b=1,status=1,t=2, o1=1, o2=1, v=True, start_time=2, end_reason_ordinal=None,end_reason_name=None,end_time=None)], id='next start transaction breaking incumbent match'),



     pytest.param(
[Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
        Row(a=1, b=0, status=1, t=2, o1=1, o2=1, v=True)],

        [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1,end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_START_MATCH_BREAK.value,end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_START_MATCH_BREAK.name,end_time=2),
         Row(a=1, b=0,status=1,t=2, o1=1, o2=1, v=True, start_time=2, end_reason_ordinal=None,end_reason_name=None,end_time=None)], id='next start match break'),

    pytest.param(
[Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
        Row(a=0, b=None, status=1, t=2, o1=1, o2=1, v=True)],

        [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1,end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_START_MATCH_NULL.value,end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_START_MATCH_NULL.name,end_time=2),
         Row(a=0, b=None,status=1,t=2, o1=1, o2=1, v=True, start_time=2, end_reason_ordinal=None,end_reason_name=None,end_time=None)], id='next start match null'),


    pytest.param(
        [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
         Row(a=0, b=0, status=1, t=2, o1=1, o2=1, v=True)],

        [Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1,
             end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_START_SAME_MATCH.value,
             end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_START_SAME_MATCH.name, end_time=2),
         Row(a=0, b=0, status=1, t=2, o1=1, o2=1, v=True, start_time=2,end_reason_ordinal=None,end_reason_name=None, end_time=None)],
        id='next start match with same match'),


     pytest.param(
[Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
        Row(a=0, b=1, status=2, t=2, o1=1, o2=1, v=False)],

        [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_UPDATE_MATCH_BREAK.value,end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_UPDATE_MATCH_BREAK.name,end_time=2),
         Row(a=0, b=1,status=2,t=2, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,end_reason_name=None,end_time=None)], id='next match break by update '),

     pytest.param(
[Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
        Row(a=0, b=None, status=2, t=2, o1=1, o2=1, v=False)],

        [Row(a=0, b=0,status=1,t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_UPDATE_MATCH_NULL.value,end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_UPDATE_MATCH_NULL.name,end_time=2),
         Row(a=0, b=None,status=2,t=2, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,end_reason_name=None,end_time=None)], id='next match null by update '),


     pytest.param(
      [ Row(a=0, b=0, status=3, t=1, o1=1, o2=1, v=True),
          Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
          Row(a=0, b=0, status=3, t=3, o1=1, o2=1, v=False),
          Row(a=0, b=None, status=2, t=2, o1=1, o2=1, v=False),
          ],

         [
             Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=8,
                 end_reason_name='NEXT_UPDATE_MATCH_NULL', end_time=2),
             Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=1,
                 end_reason_name='NEXT_END_FOR_SAME_MATCH', end_time=3),
             Row(a=0, b=None, status=2, t=2, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,
                 end_reason_name=None, end_time=None),
             Row(a=0, b=0, status=3, t=1, o1=1, o2=1, v=True, start_time=None, end_reason_ordinal=None,
                 end_reason_name=None, end_time=None),
             Row(a=0, b=0, status=3, t=3, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,
                 end_reason_name=None, end_time=None),
         ], id=' two endings possible with different time'),
pytest.param(
      [ Row(a=0, b=0, status=3, t=1, o1=1, o2=1, v=True),
          Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True),
          Row(a=0, b=0, status=3, t=2, o1=1, o2=1, v=False),
          Row(a=0, b=None, status=2, t=2, o1=1, o2=1, v=False),
          ],

    [
        Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=1,
            end_reason_name='NEXT_END_FOR_SAME_MATCH', end_time=2),
        Row(a=0, b=None, status=2, t=2, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,
            end_reason_name=None, end_time=None),
        Row(a=0, b=0, status=3, t=1, o1=1, o2=1, v=True, start_time=None, end_reason_ordinal=None, end_reason_name=None,
            end_time=None),
        Row(a=0, b=0, status=3, t=2, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,
            end_reason_name=None, end_time=None),
    ], id=' one ending with higher priority is chosen when there are two endings with same time '),

])
def test_mark_end_time_with_ending_reason(spark, input_rows, expected_rows):
    output_schema = StructType([StructField('a', IntegerType(), True),
                                         StructField('b', IntegerType(), True),
                                         StructField('status', IntegerType(), True),
                                         StructField('t', IntegerType(), True),
                                         StructField('o1', IntegerType(), True),
                                         StructField('o2', IntegerType(), True),
                                         StructField('v', BooleanType(), True),
                                         StructField('start_time', IntegerType(), True),
                                         StructField('end_reason_ordinal', IntegerType(), True),
                                         StructField('end_reason_name', StringType(), True),
                                         StructField('end_time', IntegerType(), True)])
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





@pytest.mark.parametrize("input_rows, expected_rows",[
pytest.param(
    [
        Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_END_NULL.value,
            end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_END_NULL.name, end_time=2),
        Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_END_MATCH_BREAK.value,
            end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_END_MATCH_BREAK.name, end_time=2),
        Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, end_reason_ordinal=TransactionAnalysis.EndingTransactionReason.NEXT_END_FOR_SAME_MATCH.value,
            end_reason_name=TransactionAnalysis.EndingTransactionReason.NEXT_END_FOR_SAME_MATCH.name, end_time=3),
        Row(a=0, b=None, status=2, t=2, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,
            end_reason_name=None, end_time=None),
        Row(a=0, b=0, status=3, t=1, o1=1, o2=1, v=True, start_time=None, end_reason_ordinal=None, end_reason_name=None,
            end_time=None),
        Row(a=0, b=0, status=3, t=2, o1=1, o2=1, v=False, start_time=None, end_reason_ordinal=None,
            end_reason_name=None, end_time=None),
    ],
    [
        Row(a=0, b=0, status=1, t=1, o1=1, o2=1, v=True, start_time=1, final_end_time=2,
            final_end_reason=TransactionAnalysis.EndingTransactionReason.NEXT_END_MATCH_BREAK.name),
    ]
    , id=' we teke the first match break then preference between end reasons in the same end time'),

])
def test_choose_final_transactions_end(spark, input_rows, expected_rows):
    input_schema = StructType([StructField('a', IntegerType(), True),
                StructField('b', IntegerType(), True),
                StructField('status', IntegerType(), True),
                StructField('t', IntegerType(), True),
                StructField('o1', IntegerType(), True),
                StructField('o2', IntegerType(), True),
                StructField('v', BooleanType(), True),
                StructField('start_time', IntegerType(), True),
                StructField('end_reason_ordinal', IntegerType(), True),
                StructField('end_reason_name', StringType(), True),
                StructField('end_time', IntegerType(), True)])
    df_a = RowsBuilder(input_schema, spark).add_rows(input_rows).df
    df_a = find_match_ranges._choose_final_transactions_end(df_a)
    output_schema = StructType([StructField('a', IntegerType(), True), StructField('b', IntegerType(), True),
                         StructField('status', IntegerType(), True), StructField('t', IntegerType(), True),
                         StructField('o1', IntegerType(), True), StructField('o2', IntegerType(), True),
                         StructField('v', BooleanType(), True), StructField('start_time', IntegerType(), True),
                         StructField('final_end_time', IntegerType(), True),
                         StructField('final_end_reason', StringType(), True)])

    df_e = RowsBuilder(output_schema, spark).add_rows(expected_rows).df
    compare_dataframes(df_a, df_e)


def test_connect_succeeding_transactions(spark):
    schema = StructType([StructField('a', IntegerType(), True), StructField('b', IntegerType(), True),
                         StructField('status', IntegerType(), True), StructField('t', IntegerType(), True),
                         StructField('o1', IntegerType(), True), StructField('o2', IntegerType(), True),
                         StructField('v', BooleanType(), True), StructField('start_time', IntegerType(), True),
                         StructField('final_end_time', IntegerType(), True),
                         StructField('final_end_reason', StringType(), True)])
    rows = [
    Row(a=1, b=1, status=1, t=1, o1=1, o2=1, v=True, start_time=1, final_end_time=21,
        final_end_reason=TransactionAnalysis.EndingTransactionReason.NEXT_END_MATCH_BREAK.name),
    Row(a=1, b=1, status=1, t=21, o1=1, o2=1, v=True, start_time=21, final_end_time=25,
        final_end_reason=TransactionAnalysis.EndingTransactionReason.NEXT_END_NULL.name),
    Row(a=1, b=1, status=1, t=25, o1=1, o2=1, v=True, start_time=25, final_end_time=30,
            final_end_reason=TransactionAnalysis.EndingTransactionReason.NEXT_START_MATCH_NULL.name),

    Row(a=2, b=2, status=1, t=21, o1=1, o2=1, v=True, start_time=21, final_end_time=25,
        final_end_reason=TransactionAnalysis.EndingTransactionReason.NEXT_END_NULL.name),
    Row(a=2, b=2, status=1, t=25, o1=1, o2=1, v=True, start_time=25, final_end_time=30,
            final_end_reason=TransactionAnalysis.EndingTransactionReason.NEXT_START_MATCH_NULL.name),

    Row(a=1, b=1, status=1, t=21, o1=1, o2=1, v=True, start_time=51, final_end_time=60,
        final_end_reason=TransactionAnalysis.EndingTransactionReason.NEXT_END_NULL.name),
    Row(a=1, b=1, status=1, t=25, o1=1, o2=1, v=True, start_time=60, final_end_time=78,
            final_end_reason=TransactionAnalysis.EndingTransactionReason.NEXT_START_MATCH_NULL.name)
    ]
    df  = RowsBuilder(schema, spark).add_rows(rows).df
    df_a = find_match_ranges._connect_succeeding_transactions(df)
    output_schema = StructType([StructField('a', IntegerType(), True), StructField('b', IntegerType(), True),
                         StructField('status', IntegerType(), True), StructField('t', IntegerType(), True),
                         StructField('o1', IntegerType(), True), StructField('o2', IntegerType(), True),
                         StructField('v', BooleanType(), True), StructField('start_time', IntegerType(), True),
                         StructField('final_end_time', IntegerType(), True),
                         StructField('final_end_reason', StringType(), True)
                         ])
    output_rows =[
Row(a=1, b=1, status=1, t=1, o1=1, o2=1, v=True, start_time=1, final_end_time=30, final_end_reason='NEXT_END_MATCH_BREAK') ,
Row(a=1, b=1, status=1, t=21, o1=1, o2=1, v=True, start_time=51, final_end_time=78, final_end_reason='NEXT_END_NULL') ,
Row(a=2, b=2, status=1, t=21, o1=1, o2=1, v=True, start_time=21, final_end_time=30, final_end_reason='NEXT_END_NULL') ,
        ]
    df_e = RowsBuilder(output_schema, spark).add_rows(output_rows).df
    compare_dataframes(df_a, df_e)
