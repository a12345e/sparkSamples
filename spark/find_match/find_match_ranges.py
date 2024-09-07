from itertools import chain
from typing import Dict, List
import pyspark.sql.functions as F
from pyspark.sql import Window, WindowSpec
import uuid


from pyspark.sql.dataframe import DataFrame
from enum import Enum

class FindMatchRange:
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

    def __init__(self,
                 hero_col: str,
                 matched_col: str,
                 time_col: str,
                 status_col: str,
                 status_namings: 'Dict[Status, int]',
                 other_matches: 'List[str]',
                 start_valid_column: str):
        self._hero_col = hero_col
        self._matched_col = matched_col
        self._time_col = time_col
        self._status_col = status_col
        self._status_namings = status_namings
        self._other_matches = other_matches
        self._start_validity_column = start_valid_column
        given_cols = [self._start_validity_column, self._hero_col, self._matched_col, self._time_col, self._status_col] + other_matches
        assert len(set(given_cols)) == 5+len(other_matches), 'Some columns defied twice!!'
        self._status_map = F.create_map([F.lit(k) for pair in status_namings.items() for k in pair])


    @property
    def validity_column(self):
        return self._start_validity_column
    def _initialize_dataframe(self,df: DataFrame) -> DataFrame:
        mandatory_columns = set([self._hero_col,
                                                self._matched_col,self._status_col,
                                                self._time_col]+self._other_matches)
        assert len(set(df.columns).intersection(mandatory_columns))  == len(mandatory_columns)
        df = self._initial_filter_non_nulls_hero_and_time_col(df)
        df.show()
        df = df.withColumn(self._start_validity_column, F.lit(True))
        df.show()
        df = self._enumerate_status_column(df)
        df.show()
        print('stage-1')
        df = self._avoid_match_col_null_on_start_status(df)
        print('stage-2')
        df.show()
        df = self._keep_only_start_end_status(df)
        print('stage-3')
        df.show()
        return df

    @staticmethod
    def generate_name_plus_uuid(name: str):
        return f"{name}_{uuid.uuid4().hex}"

    def _initial_filter_non_nulls_hero_and_time_col(self, df: DataFrame):
        return df.where( (F.col(self._hero_col).isNotNull()) & (F.col(self._time_col).isNotNull()))

    def _enumerate_status_column(self, df:DataFrame) -> DataFrame:
        return df.withColumn(self._status_col,
                           F.when(F.col(self._status_col).isin(list(self._status_namings.keys())),
                                  self._status_map.getItem(F.col(self._status_col))).otherwise(FindMatchRange.Status.INVALID.value))

    def _keep_only_start_end_status(self, df: DataFrame) -> DataFrame:
        return df.where(F.col(self._status_col).isin([self.Status.START.value, self.Status.END.value]))

    def _avoid_match_col_null_on_start_status(self, df: DataFrame) -> DataFrame:
        return df.where((F.col(self._status_col) != self.Status.START.value) |
                  (F.col(self._matched_col).isNotNull()))


    def  _prepare_valid_start_events(self,
                                     df: DataFrame,
                                     key_cols: List[str],
                                     match_cols: List[str]):
            """
                In case of start events, collect values for the matched cols for each keys list
                It is now allowed to have
            :rtype: object
            """
            assert not set(key_cols).intersection(match_cols), 'It is not allowed to have intersection between mach and key cols'
            f_agg_column = lambda x: f'{x}_values'
            window_spec = Window.partitionBy(*key_cols)
            agg_expressions = [F.collect_set(column).over(window_spec).alias(f_agg_column(column)) for column in match_cols]
            df_agg = df.select(df.columns+agg_expressions)
            for col in match_cols:
                condition = ((F.size(f_agg_column(col))>1) &
                             (F.col(self._status_col) == FindMatchRange.Status.START.value))
                df_agg = df_agg.withColumn(self._start_validity_column,
                    F.when(condition, F.lit(False)).otherwise(F.col(self._start_validity_column)))
                condition = (F.col(self._start_validity_column) == F.lit(True)) & (F.size(F.col(f_agg_column(col))) == 1) & (F.col(self._status_col) == FindMatchRange.Status.START.value)
                df_agg = df_agg.withColumn(col,F.when(condition,F.col(f_agg_column(col)).getItem(0)).otherwise(F.col(col)))
            df_agg=df_agg.drop(*[f_agg_column(column) for column in match_cols])
            return df_agg.distinct()


    def _create_transaction_window_partitioned_by_hero(self) -> WindowSpec:
        return Window.partitionBy(self._hero_col).orderBy(self._time_col, self._matched_col, self._status_col)
    def _create_transaction_window_partitioned_by_matched(self) -> WindowSpec:
        return Window.partitionBy(self._matched_col).orderBy(self._time_col, self._hero_col, self._status_col)

    def _create_condition_end_after_start(
            self,
            window_spec: WindowSpec):
            end_after_start_cond = ((F.col(self._status_col) == self.Status.END.value) &
                                   (F.lag(self._status_col, 1).over(window_spec) == self.Status.START))
            time_is_after = F.col(self._time_col) > F.lag(self._time_col, 1).over(window_spec)
            return end_after_start_cond & time_is_after

    def mark_end_time_with_ending_reason(
                                self,
                                df: DataFrame,
                                partition_by_col: str):
        start_time_col = 'start_time'
        end_time_map_reason4time_col = 'end_time'

        window_spec: WindowSpec = (Window.partitionBy(partition_by_col).
                               orderBy(self._time_col, self._matched_col, self._status_col))
        start_after_start_cond = ((F.col(self._status_col) == self.Status.START.value) &
                              (F.lead(self._status_col, 1).over(window_spec) == self.Status.START))
        end_after_start_cond = ((F.col(self._status_col) == self.Status.START.value) &
                              (F.lead(self._status_col, 1).over(window_spec) == self.Status.END))
        next_match_is_null = F.lead(self._matched_col, 1).over(window_spec).isNull()
        next_match_is_not_null = ~next_match_is_null
        same_match_col = F.col(self._matched_col) == F.lead(self._matched_col, 1)
        match_broken = ~same_match_col

        df = df.withColumn(start_time_col, F.when((start_after_start_cond) | (end_after_start_cond),
                                         F.col(self._time_col)))
        map_expected = F.create_map(F.lit(self.EndingTransactionReason.MATCH_EXPECTED_END), F.col(self._time_col))
        df = df.withColumn(end_time_map_reason4time_col, F.when(
            end_after_start_cond & same_match_col, map_expected))
        map_end_for_unexpected_match = F.create_map(F.lit(self.EndingTransactionReason.MATCH_END_FOR_UNEXPECTED_MATCH), F.col(self._time_col))
        df = df.withColumn(end_time_map_reason4time_col, F.when(
            end_after_start_cond & match_broken & next_match_is_not_null, map_end_for_unexpected_match))
        map_end_for_unexpected_match_null = F.create_map(F.lit(self.EndingTransactionReason.MATCH_END_FOR_UNEXPECTED_NULL), F.col(self._time_col))
        df = df.withColumn(end_time_map_reason4time_col, F.when(
            end_after_start_cond & match_broken & next_match_is_not_null, map_end_for_unexpected_match_null))
        map_start_for_unexpected_match = F.create_map(F.lit(self.EndingTransactionReason.MATCH_UNEXPECTED_START_DIFFERENT_MATCH), F.col(self._time_col))
        df = df.withColumn(end_time_map_reason4time_col, F.when(
            start_after_start_cond & match_broken & next_match_is_not_null, map_start_for_unexpected_match))
        map_start_for_unexpected_same_match = F.create_map(F.lit(self.EndingTransactionReason.MATCH_UNEXPECTED_START_SAME_MATCH), F.col(self._time_col))
        df = df.withColumn(end_time_map_reason4time_col, F.when(
            start_after_start_cond & same_match_col & next_match_is_not_null, map_start_for_unexpected_same_match))
        return df

    def prepare_matches(
                    self,
                    df: DataFrame) -> DataFrame:
        df = self._initialize_dataframe(df)
        df = self._prepare_valid_start_events(df, key_cols=[self._hero_col,
                                                            self._status_col,
                                                            self._time_col],
                                              match_cols=self._other_matches+[self._matched_col])
        df = self._prepare_valid_start_events(df, key_cols=[self._matched_col,
                                                            self._status_col,
                                                            self._time_col],
                                              match_cols=self._other_matches + [self._hero_col])
        df = self.mark_end_time_with_ending_reason(df)
        return df
