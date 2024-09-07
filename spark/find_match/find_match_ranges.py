from itertools import chain
from typing import Dict, List
import pyspark.sql.functions as F
from pyspark.sql import Window, WindowSpec, Column
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
        NEXT_END_FOR_SAME_MATCH = 1 # the normal start end events for the same match
        NEXT_END_NULL = 2
        NEXT_START_NULL = 3
        NEXT_START_DIFFERENT_MATCH = 4
        NEXT_END_DIFFERENT_MATCH = 5
        NEXT_START_FOR_SAME_MATCH = 6

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
    def start_condition(self) -> Column:
        return F.col(self._status_col) == FindMatchRange.Status.START.value
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


    def  _reduce_matched_cols_into_one_value_or_invalidate(self,
                                                           df: DataFrame,
                                                           base_condition: Column,
                                                           validity_column: str,
                                                           key_cols: List[str],
                                                           match_cols: List[str]):
            """
            This function is used for delivering one row per each key
            When all the rows for a key produce one value at most for each matched col (all together),
            then it is marked as valid (not marked invalid actually) and it is reduced
            into just one row with those values.
            When there is more than one value then all rows for the key  are just marked as invalid
            This invalidity is just  for specific purpose, we use it for the purpose of being a start point
            At the end we just call distinct
            :param df: DataFrame
            :param base_condition: filter the rows of relevance
            :param validity_column:  The column where to mark rows of relevance as invalid
            :param key_cols: The cols making the key - valid rows will be reduced into one row for each key
            :param match_cols: The columns where we check validity and we reduce into one value
            :return:
            :rtype: DataFrame
            """
            assert  len(key_cols)+len(match_cols)+len([validity_column]) == len(set(key_cols+match_cols+[validity_column])), 'There is intersection of exclusive columns sets'
            f_agg_column = lambda x: f'{x}_values'
            window_spec = Window.partitionBy(*key_cols)
            agg_expressions = [F.collect_set(column).over(window_spec).alias(f_agg_column(column)) for column in match_cols]
            df_agg = df.select(df.columns+agg_expressions)
            for col in match_cols:
                condition = base_condition & (F.size(f_agg_column(col))>1)
                df_agg = df_agg.withColumn(validity_column,
                    F.when(condition, F.lit(False)).otherwise(F.col(validity_column)))
                condition = base_condition & (F.size(F.col(f_agg_column(col))) == 1)
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
            time_is_after = F.col(self._time_col) >= F.lag(self._time_col, 1).over(window_spec)
            return end_after_start_cond & time_is_after

    def mark_end_time_with_ending_reason(
                                self,
                                df: DataFrame,
                                anchor_col: str,
                                matches_col: str):
        """
        Notice that input is that the match for each start is not null for both elements
        Notice that also there is one row only for start  of a match at specific time
        :param anchor_col:
        :param df:
        :param matched_col:
        :return:
        """
        start_time_col = 'start_time'
        end_time_map_reason4time_col: str = 'end_time'

        window_spec: WindowSpec = Window.partitionBy(anchor_col).orderBy(self._time_col, matches_col, self._status_col)
        curren_status_is_start = (F.col(self._status_col) == self.Status.START.value)
        next_status_is_start = F.lead(self._status_col, 1).over(window_spec) == self.Status.START
        next_status_is_end = F.lead(self._status_col, 1).over(window_spec) == self.Status.END
        start_after_start_cond = curren_status_is_start & next_status_is_start
        end_after_start_cond = curren_status_is_start & next_status_is_end
        next_match_is_null = F.lead(self._matched_col, 1).over(window_spec).isNull()
        same_match_col = F.col(self._matched_col) == F.lead(self._matched_col, 1)
        match_broken = ~same_match_col & F.lead(self._matched_col, 1).isNotNull()

        df = df.withColumn(start_time_col, F.when(curren_status_is_start, F.col(self._time_col)))

        #The most classic case of (start,end) events for the same matched couple
        df = df.withColumn(end_time_map_reason4time_col, F.when(
            end_after_start_cond & same_match_col,
            F.map_concat(
                end_time_map_reason4time_col,
                F.create_map(F.lit(self.EndingTransactionReason.NEXT_END_FOR_SAME_MATCH), F.col(self._time_col)))))
        df = df.withColumn(end_time_map_reason4time_col, F.when(
            end_after_start_cond & match_broken,
            F.create_map(F.lit(self.EndingTransactionReason.NEXT_END_NULL), F.col(self._time_col))))

        df = df.withColumn(end_time_map_reason4time_col, F.when(
            start_after_start_cond & next_match_is_null,
            F.create_map(F.lit(self.EndingTransactionReason.NEXT_START_NULL), F.col(self._time_col))))

        df = df.withColumn(end_time_map_reason4time_col, F.when(
            start_after_start_cond & match_broken,
            F.create_map(F.lit(self.EndingTransactionReason.NEXT_START_DIFFERENT_MATCH), F.col(self._time_col))))

        df = df.withColumn(end_time_map_reason4time_col, F.when(
            end_after_start_cond & match_broken,
            F.create_map(F.lit(self.EndingTransactionReason.NEXT_END_DIFFERENT_MATCH), F.col(self._time_col))))

        df = df.withColumn(end_time_map_reason4time_col, F.when(
            start_after_start_cond & same_match_col,
            F.create_map(F.lit(self.EndingTransactionReason.NEXT_START_FOR_SAME_MATCH), F.col(self._time_col))))

        return df

    def prepare_matches(
                    self,
                    df: DataFrame) -> DataFrame:
        df = self._initialize_dataframe(df)
        df = self._reduce_matched_cols_into_one_value_or_invalidate(df,
                                                                    base_condition=self.start_condition,
                                                                    validity_column=self._start_validity_column,
                                                                    key_cols=[self._hero_col,
                                                                            self._status_col,
                                                                            self._time_col],
                                                                    match_cols=self._other_matches+[self._matched_col])
        df = self._reduce_matched_cols_into_one_value_or_invalidate(df,
                                                                    base_condition=self.start_condition,
                                                                    validity_column=self._start_validity_column,
                                                                    key_cols=[self._matched_col,
                                                                            self._status_col,
                                                                            self._time_col],
                                                                    match_cols=self._other_matches + [self._hero_col])
        df = self.mark_end_time_with_ending_reason(df, anchor_col=self._hero_col, matches_col=self._matched_col)
        df = self.mark_end_time_with_ending_reason(df, matches_col=self._hero_col, anchor_col=self._hero_col)
        return df.distinct()
