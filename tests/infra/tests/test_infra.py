import datetime

import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Row, DoubleType, TimestampType
from tests.infra.dataframes_helpers import RowsBuilder, compare_dataframes
from tests.infra.default_spark_builder import spark


@pytest.mark.parametrize("anchor, row, expected_row",[
        pytest.param(Row(birthday=datetime.datetime(2000,1,1), id=1,name='x'),Row(height=12.0, id=2),
            Row(birthday=datetime.datetime(2000,1,1), id=2,name='x', height=12.0), id='test set_anchor and add row')
])
def test_rows_builder_add_row_and_set_anchor(spark, anchor, row, expected_row):
    schema = StructType([
        StructField("height", DoubleType(), True),
        StructField("id", IntegerType(), True),
        StructField("count", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("birthday", TimestampType(), True),
        StructField("age", IntegerType(), True)
    ])
    df_a = RowsBuilder(schema, session=spark).set_anchor(anchor).add(row).df
    df_e = RowsBuilder(schema, session=spark).add(expected_row).df
    compare_dataframes(df_a,df_e)

@pytest.mark.parametrize("anchor, rows, expected_rows",[
        pytest.param(
            Row(birthday=datetime.datetime(2000,1,1), id=1,name='x'),
                   [Row(height=12.0, id=2),Row(height=13.0, id=3)],
            [Row(birthday=datetime.datetime(2000,1,1), id=2,name='x', height=12.0),
             Row(birthday=datetime.datetime(2000,1,1), id=3,name='x', height=13.0)], id='test init with anchor and add rows')
])
def test_rows_builder_add_rows_and_anchor_in_construct(spark, anchor, rows, expected_rows):
    schema = StructType([
        StructField("height", DoubleType(), True),
        StructField("id", IntegerType(), True),
        StructField("count", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("birthday", TimestampType(), True),
        StructField("age", IntegerType(), True)
    ])
    df_a = RowsBuilder(schema, session=spark, anchor=anchor).add_rows(rows).df
    df_e = RowsBuilder(schema, session=spark).add_rows(expected_rows).df
    compare_dataframes(df_a,df_e)
