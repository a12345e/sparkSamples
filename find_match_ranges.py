from typing import Dict, List
import pyspark.sql.functions as F
from pyspark.sql import Window, WindowSpec
import uuid


from pyspark.sql.dataframe import DataFrame
from enum import Enum
class Status(Enum):
      INVALID = 0
      START = 1
      UPDATE = 2
      END = 3


class EndingTransactionReason(Enum):
    MATCH_EXPECTED_END = 1
    MATCH_END_FOR_UNEXPECTED_MATCH = 2
    MATCH_END_FOR_UNEXPECTED_NULL = 3
    MATCH_UNEXPECTED_START_DIFFERENT_MATCH = 4
    MATCH_UNEXPECTED_START_SAME_MATCH = 5


def initial_filter_non_nulls_a_col_time_col(df: DataFrame,
                    a_col: str,
                    time_col: str):
    return df.where( (F.col(a_col).isNotNull()) & (F.col(time_col).isNotNull()))

def enumerate_status_column(df:DataFrame,
                             status_col: str,
                             status_values: Dict[str, Status]):
    tmp_col =  f"{status_col}_{uuid.uuid4().hex}"
    for status_value, status_enum in status_values.items():
        df = df.withColumn(tmp_col,
                           F.when(F.col(status_col) == F.lit(status_value),
                                  status_enum.value).otherwise(F.lit(Status.INVALID.value)))
    return df.withColumn(status_col,F.col(tmp_col)).drop(tmp_col)

def keep_only_start_end_status(df: DataFrame, status_col: str):
    return df.where(F.col(status_col).isin([Status.START.value, Status.END]))

def avoid_b_col_null_on_start_status(df: DataFrame, b_col: str, status_col: str):
    df = df.where((F.col(status_col) != Status.START) |
                  (F.col(b_col).isNotNull()))

    return df.where(F.col(status_col).isin([Status.START.value, Status.END]))


def keep_when_only_one_non_null_value_and_be_the_non_null_one_if_exist(
        df: DataFrame,
        collect_set_col: str):
    tmp_col = f"{collect_set_col}_{uuid.uuid4().hex}"
    df = df.withColumn(tmp_col, F.when((F.col(collect_set_col).isNotNull()) & (F.col(collect_set_col) != ""), F.col(collect_set_col)))
    df = df.filter(
        (F.size(F.col(tmp_col)) <= 2) &  # Ensure set size is not larger than 2
        (
                (F.size(F.col(tmp_col)) < 2) |  # If size is less than 2, include
                (
                        (F.size(F.col(tmp_col)) == 2) &  # If size is 2, check for None or empty string
                        (F.array_contains(F.col(tmp_col), None) | F.array_contains(F.col(tmp_col), ""))
                )
        )
    )
    df = df.withColumn(collect_set_col,
                       F.when(F.size(F.col(tmp_col)) == 1,
                              F.explode(tmp_col))
                       .otherwise(F.explode(F.array_except(F.lit(None))))
                       )
    return df
def  reduce_into_valid_nutshell_start_transaction_events(
        df: DataFrame,
        a_col: str,
        b_col: str,
        status_col: str,
        time_col: str,
        other_b_cols: List[str]):

    collect_set_cols =[b_col]+other_b_cols
    agg_expressions = [F.collect_set(F.col(column)).alias(column) for column in collect_set_cols]
    df_aggregated = df.groupBy(a_col, time_col, status_col).agg(*agg_expressions)
    for col in collect_set_cols:
        df_aggregated = keep_when_only_one_non_null_value_and_be_the_non_null_one_if_exist(df_aggregated, col)
    return df

def create_transaction_window(a_col: str, b_col: str, time_col: str, status_col:str):
    return Window.partitionBy(a_col).orderBy(time_col, b_col, status_col)

def create_condition_end_after_start(
        window_spec: WindowSpec, time_col: str, status_col:str):
    end_after_start_cond = ((F.col(status_col) == Status.END.value) &
                                   (F.lag(status_col, 1).over(window_spec) == Status.START))
    time_is_after = F.col(time_col) > F.lag(time_col, 1).over(window_spec)
    return end_after_start_cond & time_is_after

def mark_end_time_with_ending_reason(df: DataFrame,
                                   partition_by_col: str,
                                   time_col: str,
                                   match_col: str,
                                   status_col:str):
    start_time_col = 'start_time'
    end_time_map_reason4time_col = 'end_time'

    window_spec: WindowSpec = (Window.partitionBy(partition_by_col).
                               orderBy(time_col, match_col, status_col))
    start_after_start_cond = ((F.col(status_col) == Status.START.value) &
                              (F.lead(status_col, 1).over(window_spec) == Status.START))
    end_after_start_cond = ((F.col(status_col) == Status.START.value) &
                              (F.lead(status_col, 1).over(window_spec) == Status.END))
    next_match_is_null = F.lead(match_col, 1).over(window_spec).isNull()
    next_match_is_not_null = ~next_match_is_null
    same_match_col = F.col(match_col) == F.lead(match_col, 1)
    match_broken = ~same_match_col
    df.withColumn(start_time_col, F.when((start_after_start_cond) | (end_after_start_cond),
                                         F.col(time_col)))
    map_expected = F.create_map(F.lit(EndingTransactionReason.MATCH_EXPECTED_END), F.col(time_col))
    df = df.withColumn(end_time_map_reason4time_col, F.when(
        end_after_start_cond & same_match_col, map_expected))
    map_end_for_unexpected_match = F.create_map(F.lit(EndingTransactionReason.MATCH_END_FOR_UNEXPECTED_MATCH), F.col(time_col))
    df = df.withColumn(end_time_map_reason4time_col, F.when(
        end_after_start_cond & match_broken & next_match_is_not_null, map_end_for_unexpected_match))
    map_end_for_unexpected_match_null = F.create_map(F.lit(EndingTransactionReason.MATCH_END_FOR_UNEXPECTED_NULL), F.col(time_col))
    df = df.withColumn(end_time_map_reason4time_col, F.when(
        end_after_start_cond & match_broken & next_match_is_not_null, map_end_for_unexpected_match_null))
    map_start_for_unexpected_match = F.create_map(F.lit(EndingTransactionReason.MATCH_UNEXPECTED_START_DIFFERENT_MATCH), F.col(time_col))
    df = df.withColumn(end_time_map_reason4time_col, F.when(
        start_after_start_cond & match_broken & next_match_is_not_null, map_start_for_unexpected_match))
    map_start_for_unexpected_same_match = F.create_map(F.lit(EndingTransactionReason.MATCH_UNEXPECTED_START_SAME_MATCH), F.col(time_col))
    df = df.withColumn(end_time_map_reason4time_col, F.when(
        start_after_start_cond & same_match_col & next_match_is_not_null, map_start_for_unexpected_same_match))


def prepare_matches(df: DataFrame,
                    a_col: str,
                    b_col: str,
                    status_col: str,
                    time_col: str,
                    status_values: Dict[str, Status],
                    other_b_cols: List[str]) -> DataFrame:
    df = initial_filter_non_nulls_a_col_time_col(df, a_col, time_col)
    df = enumerate_status_column(df, status_col, status_values)
    df = keep_only_start_end_status(df, status_col)
    df = avoid_b_col_null_on_start_status(df, b_col, status_col)
    df = reduce_into_valid_nutshell_start_transaction_events(df,a_col,b_col,status_col,time_col,other_b_cols)

    #remove succeeding start status row for the same a_col,b_col
    df = mark_end_time_with_ending_reason(df, a_col, time_col, b_col, status_col)
    #
    # a_to_b_window: WindowSpec = create_transaction_window(a_col,b_col, time_col)
    # start_end_a_b_cond = create_condition_end_after_start(a_to_b_window,time_col, status_col)
    #
    # b_to_a_window: WindowSpec = create_transaction_window(b_col,a_col, time_col)
    # start_end_b_a_cond = create_condition_end_after_start(b_to_a_window,time_col, status_col)
    #
    # df = df.withColumn('start_time_a_b', F.when(start_end_a_b_cond, F.lag(time_col,1).over(a_to_b_window)))
    # df = df.withColumn('start_time_b_a', F.when(start_end_b_a_cond, F.lag(time_col,1).over(b_to_a_window)))



    return df
