from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Row, FloatType, LongType, DoubleType, TimestampType
from datetime import datetime, timedelta
from tests.infra.default_spark_builder import DefaultSparkFactory

spark = DefaultSparkFactory().spark
class RowsBuilder:
    def __init__(self, schema: StructType,
                 session: SparkSession,
                 anchor: DataFrame=None,
                 value_generator: int=None):
        self._schema = schema
        self._session = session
        self._rows: List = []
        self._counter = value_generator
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
            self.add_row(row)
        return self
    def add_row(self, row: Row):
        self._validate(row)
        if self._anchor:
            row_as_dict = row.asDict().copy()
            base_as_dict = self._anchor.asDict()
            for key in set(base_as_dict.keys())-set(row_as_dict.keys()):
                row_as_dict[key] = base_as_dict[key]
        new_row = complete_row_to_schema(row,self._schema, self._counter)
        if self._counter:
            self._counter = self._counter + 1
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

def dataframe_create(rows: List[Row],
              schema: StructType,
              complete_with_nulls=True,
              specific_spark: SparkSession=None) -> DataFrame:
    session = specific_spark
    if not session:
        session = spark
        session = spark
    return session.createDataFrame(
        complete_rows_to_schema(rows, schema, complete_with_nulls),schema)

def row_validate_schema(row: Row, schema: StructType):
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
                           schema: StructType,
                           counter: int =None
                           ) -> Row:
    row_validate_schema(row, schema)
    completed_row = row.asDict()
    missing_fields = [field for field in schema.fields if field.name not in row.__fields__]
    for field in missing_fields:
        if not counter:
            completed_row[field.name] = None
        elif isinstance(field.dataType, StringType):
            completed_row[field.name] = f'{field.name}_{counter}'
        elif isinstance(field.dataType, LongType) or isinstance(field.dataType, IntegerType):
            completed_row[field.name] = counter
        elif isinstance(field.dataType, DoubleType) or isinstance(field.dataType, FloatType):
            completed_row[field.name] = float(counter)
        elif isinstance(field.dataType, TimestampType):
            completed_row[field.name] = datetime(2000, 1, 1) + timedelta(hours=counter)
        else:
            completed_row[field.name] = None
    return Row(*[completed_row[field] for field in schema.fieldNames()])

def reorder_row(row:Row, schema: StructType):
    d = row.asDict()
    return Row(*[d[field] for field in schema.fieldNames()])

def complete_rows_to_schema(rows: List[Row],
                           schema: StructType,
                           counter=None,
                           ) -> list[Row]:
    for row in rows:
        if counter:
            counter = counter + 1
        complete_row_to_schema(row, schema, counter)

def compare_dataframes(expected_df: DataFrame, actual_df: DataFrame):
    compare_schemas(expected_df,actual_df)
    distinct_expected_subtract_actual= expected_df.distinct().subtract(actual_df.distinct())
    distinct_actual_subtract_expected = actual_df.distinct().subtract(expected_df.distinct())
    if distinct_actual_subtract_expected.count():
        print('in actual distinct but not in expected distict:')
        distinct_actual_subtract_expected.show(truncate=False)
    if distinct_expected_subtract_actual.count():
        print('in expected distinct but not in actual distinct:')
        distinct_expected_subtract_actual.show(truncate=False)
    if distinct_actual_subtract_expected.count() or distinct_expected_subtract_actual.count():
        return
    expected_subtract_actual= expected_df.subtract(actual_df)
    actual_subtract_expected = actual_df.subtract(expected_df)
    if actual_subtract_expected.count():
        print('in actual but not in expected:')
        actual_subtract_expected.show(truncate=False)
    if expected_subtract_actual.count():
        print('in actual  but not in expected:')
        expected_subtract_actual.show(truncate=False)

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