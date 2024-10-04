from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Row, FloatType, LongType, DoubleType, TimestampType
from datetime import datetime, timedelta
import time
import random
seed = int(time.time() * 1000)
# Set the random seed
random.seed(seed)


def create_rows_builder(spark, schema:StructType, anchor: DataFrame=None, counter: int = None):
    return RowsBuilder(schema, spark, anchor, counter)

class RowsBuilder:
    def __init__(self, schema: StructType,
                 session: SparkSession,
                 anchor: DataFrame=None):
        self._schema = schema
        self._session = session
        self._rows: List = []
        self._anchor = anchor

    def _validate(self, row: Row):
        assert isinstance(row, Row)
        row_validate_schema(row, self._schema)

    def set_anchor(self, anchor: Row):
        row_validate_schema(anchor, self._schema)
        self._anchor = anchor
        return self

    def add_rows(self, rows: List[Row]):
        for row in rows:
            self.add(row)
        return self
    def add(self, row: Row):
        if self._anchor:
            row_as_dict = row.asDict().copy() if row != Row() else {}
            base_as_dict = self._anchor.asDict()
            for key in set(base_as_dict.keys())-set(row_as_dict.keys()):
                row_as_dict[key] = base_as_dict[key]
            row = Row(**row_as_dict)
        new_row = complete_row_to_schema(row,self._schema)
        self._rows.append(new_row)
        return self
    @property
    def rows(self):
        return self._rows
    @property
    def anchor(self):
        return self._anchor

    @property
    def df(self):
        return self._session.createDataFrame(self._rows, self._schema)


def row_validate_schema(row: Row, schema: StructType):
        if row != Row():
            row_as_dict = row.asDict()
            for row_field in row_as_dict.keys():
                if row_field not in schema.fieldNames():
                    msg = f'{row_field} in row but not in schema {schema.fieldNames()}'
                    raise ValueError(msg)
                field: StructField = schema[row_field]
                if row_as_dict[row_field]:
                    if isinstance(field.dataType, StringType):
                        assert isinstance(row_as_dict[row_field], str)
                    elif isinstance(field.dataType, IntegerType) or isinstance(field.dataType, LongType):
                        assert isinstance(row_as_dict[row_field],int)
                    elif isinstance(field.dataType, FloatType) or isinstance(field.dataType, DoubleType):
                        assert isinstance(row_as_dict[row_field],float)
                    elif isinstance(field.dataType, TimestampType):
                        assert isinstance(row_as_dict[row_field], datetime)

def rows_validate_schema(rows: List[Row], schema: StructType):
    for row in rows:
        row_validate_schema(row, schema)

def complete_row_to_schema(row: Row,
                           schema: StructType
                           ) -> Row:
    row_validate_schema(row, schema)
    if row == Row():
        completed_row = {}
    else:
        completed_row = row.asDict()
    missing_fields = [field for field in schema.fields if field.name not in completed_row.keys()]
    for field in missing_fields:
        completed_row[field.name] = None
    return Row(*[completed_row[field] for field in schema.fieldNames()])

def complete_rows_to_schema(rows: List[Row],
                           schema: StructType
                           ) -> list[Row]:
    for row in rows:
        complete_row_to_schema(row, schema)

def compare_dataframes(expected_df: DataFrame, actual_df: DataFrame):
    report = []
    compare_schemas(expected_df,actual_df)
    distinct_expected_subtract_actual= expected_df.distinct().subtract(actual_df.distinct())
    distinct_actual_subtract_expected = actual_df.distinct().subtract(expected_df.distinct())
    if distinct_actual_subtract_expected.count():
        report.append('In actual distinct but not in expected distinct:')
        report.append(distinct_actual_subtract_expected.collect())
    if distinct_expected_subtract_actual.count():
        report.append('in expected distinct but not in actual distinct:')
        report.append(distinct_expected_subtract_actual.collect())
    if report:
        raise Exception(report)
    expected_subtract_actual= expected_df.subtract(actual_df)
    actual_subtract_expected = actual_df.subtract(expected_df)
    if actual_subtract_expected.count():
        report.append('in actual but not in expected:')
        report.append(actual_subtract_expected.collect())
    if expected_subtract_actual.count():
        report.append('in actual  but not in expected:')
        report.append(expected_subtract_actual.collect())
    if report:
        raise Exception(report)
def compare_schemas(expected_df: DataFrame, actual_df: DataFrame):
    field_names_df1 = set(expected_df.schema.fieldNames())
    field_names_df2 = set(actual_df.schema.fieldNames())
# Compare field names
    if field_names_df1 == field_names_df2:
        pass
    else:
        raise Exception (f"Fields only in expected df: {field_names_df1 - field_names_df2}"+
                   f"\nFields only in actual df: {field_names_df2 - field_names_df1}")
# Extract field data types
    data_types_df_expected = {field.name: field.dataType for field in expected_df.schema.fields}
    data_types_df_actual = {field.name: field.dataType for field in actual_df.schema.fields}
# Compare data types
    if data_types_df_expected == data_types_df_actual:
        pass
    else:
        different_data_types = ''
    # Identify fields with different data types
        common_fields = field_names_df1.intersection(field_names_df2)
        for field in common_fields:
            if data_types_df_expected[field] != data_types_df_actual[field]:
                different_data_types += (f"\nField '{field}' has different data types: {data_types_df_expected[field]} (expected) vs {data_types_df_actual[field]} (actual)")
        raise  Exception(different_data_types)

def print_dataframe_schema_and_rows(df: DataFrame):
        print()
        print(f'schema = {df.schema}')
        if df.count() == 0:
            print('rows =  []')
        else:
            print('rows =  [')
            for row in df.collect():
                print(row,',')
            print('        ]')

