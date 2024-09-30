from itertools import chain
from typing import Dict, List
import pyspark.sql.functions as F
from pyspark.sql import Window, WindowSpec, Column
import uuid


from pyspark.sql.dataframe import DataFrame
from enum import Enum

class TransactionAnalysis:
    class Status(Enum):
        """
        We use number because we are sorting by status and this is the ascending order we need
        so that end comes after start
        """
        START = 1
        UPDATE = 2
        END = 3


    class EndingTransactionReason(Enum):
        NEXT_END_FOR_SAME_MATCH=1 # the normal start end events for the same transaction
        NEXT_END_MATCH_BREAK=2
        NEXT_END_NULL = 3
        NEXT_START_MATCH_BREAK = 4
        NEXT_START_SAME_MATCH = 5
        NEXT_START_MATCH_NULL = 6
        NEXT_UPDATE_MATCH_BREAK = 7
        NEXT_UPDATE_MATCH_NULL = 8



    START_TIME_COL = 'start_time'
    END_TIME_COL = 'end_time'
    END_REASON_ORDINAL = 'end_reason_ordinal'
    END_REASON_NAME = 'end_reason_name'
    END_TIME_MAP_COL = 'end_time_map'
    END_EVENTS_ARRAY_COL = 'end_events_array_col'

    def __init__(self,
                 a_col: str,
                 b_col: str,
                 time_col: str,
                 status_col: str,
                 status_namings: 'Dict[Status, int]',
                 other_matches: 'List[str]',
                 start_valid_column: str,
                 ending_conditions=None):

        self._a_col = a_col
        self._b_col = b_col
        self._time_col = time_col
        self._status_col = status_col
        self._status_namings = status_namings
        self._other_matches = other_matches
        self._start_validity_column = start_valid_column
        given_cols = [self._start_validity_column, self._a_col, self._b_col, self._time_col, self._status_col] + other_matches
        assert len(set(given_cols)) == 5+len(other_matches), 'Some columns defied twice!!'
        self._optional_initial_filters = []

        if ending_conditions is None:
            self._ending_conditions = [reason for reason in self.EndingTransactionReason]
        else:
            self._ending_conditions = ending_conditions

    @property
    def _start_condition(self) -> Column:
        return ((F.col(self._status_col) == TransactionAnalysis.Status.START.value) &
                (F.col(self._start_validity_column) == F.lit(True)))

    @property
    def _validity_column(self):
        return self._start_validity_column
    def _initialize_dataframe(self,df: DataFrame) -> DataFrame:
        """
        This does cleanup of rows we not like and modification of the status column values to our wording
        :param df:
        :return:
        """
        mandatory_columns = set([self._a_col,
                                 self._b_col, self._status_col,
                                 self._time_col]+self._other_matches)
        assert len(set(df.columns).intersection(mandatory_columns))  == len(mandatory_columns)
        df = self._enumerate_status_column(df)
        df = self._filter_avoid_null_time_col(df)
        df = self._filter_avoid_a_col_and_b_col_null_together(df)
        df = self._filter_optional(df)

        return df

    def _filter_optional(self, df):
        for transform in self._optional_initial_filters:
            df = transform(df)
        return df

    def _filter_avoid_a_col_and_b_col_null_together(self, df: DataFrame):
        return df.where((F.col(self._a_col).isNotNull()) | (F.col(self._b_col).isNotNull()))

    def filter_avoid_null_a_col(self):
        """
        Avoid using those rows even for ending a transaction
        :return:
        """
        self._optional_initial_filters.append(lambda df: df.where(F.col(self._a_col).isNotNull()))
        return self

    def filter_avoid_null_b_col_when_status_is_start(self):
        """
        Avoid using those rows even for ending a transaction
        :return:
        """

        self._optional_initial_filters.append(lambda df: df.where((F.col(self._b_col).isNotNull()) |
                                                                  (F.col(self._status_col) != TransactionAnalysis.Status.START.value)))
        return self

    def filter_only_start_end_status(self):
        """
        Not use update status events even for ending a transaction
        :return:
        """
        self._optional_initial_filters.append(lambda df: df.where(F.col(self._status_col).isin([self.Status.START.value, self.Status.END.value])))
        return self

    def _filter_avoid_null_time_col(self, df: DataFrame):
        return df.where((F.col(self._a_col).isNotNull()) & (F.col(self._time_col).isNotNull()))

    def _enumerate_status_column(self, df:DataFrame) -> DataFrame:
        status_map = F.create_map([F.lit(k) for pair in self._status_namings.items() for k in pair])
        df = df.withColumn(self._status_col,
                             F.when(F.col(self._status_col).isin(list(self._status_namings.keys())),
                                  status_map.getItem(F.col(self._status_col))).otherwise(None))
        return df



    def _prepare_valid_transaction_start_points(self, df):
        """
        This function is calling twice the reduce_and_validate_transaction_start_points
        See how it flips roles between the a_col and b_col
        At the end we have one row per each valid starting point marked as valid with at most one value for
        the match columns, and all the other start point are just left with and invalid mark as it.
        :param df:
        :return:df
        """
        df = df.withColumn(self._start_validity_column, F.when((F.col(self._status_col) == TransactionAnalysis.Status.START.value) &
                                                               (F.col(self._a_col).isNotNull()) & (F.col(self._b_col).isNotNull()),
                                                               F.lit(True)).otherwise(F.lit(False)))
        df = self._reduce_and_validate_transaction_start_points(df,
                                                                key_cols=[self._a_col,
                                                                            self._time_col],
                                                                match_cols=self._other_matches+[self._b_col])
        df = self._reduce_and_validate_transaction_start_points(df,
                                                                key_cols=[self._b_col,
                                                                            self._time_col],
                                                                match_cols=self._other_matches + [self._a_col])
        return df

    def  _reduce_and_validate_transaction_start_points(self,
                                                       df: DataFrame,
                                                       key_cols: List[str],
                                                       match_cols: List[str]) -> DataFrame:
        """
            We have a list of key columns that form a potential start point.
            Practically in our case it is either (time_col,status(=start), b_col) or (time_col,status(=start), a_col)
            We then look into the matched_cols. These will practically be
                (anchor_col, other_matches) or (match_col, other_match_es) accordingly in our case
            We have to make sure that per each key tuple there can be only one matched value for the other columns
            Per each row tuple we end up with either one reduced row and a validity column set to valid,
            or we keep the original rows and marks them as invalid start points. While invalid, as
            start points these columns are still valid to end another match started earlier


            1) Row(s=start,t=1,anchor_col=1,match_col=1,other_match_col1=1) - will be marked valid

            2) Row(s=start,t=1,anchor_col=1,match_col=1,other_match_col1=1)
               Row(s=start,t=1,anchor_col=1,match_col=1,other_match_col1=2)
              Will be marked NOT valid because more than one match of other_match_col1 per (t=1,match_col=1)

            3) Row(s=start,t=1,anchor_col=1,match_col=1,other_match_col1=1)
                Row(s=start,t=1,anchor_col=1,match_col=1,other_match_col1=None)
                Will be marked valid because and reduced to one row
                as Row(t=1,anchor_col=1,match_col=1,other_match_col1=1)

            4) Row(t=1,anchor_col=2,match_col=1,other_match_col1=1)
                Row(t=1,anchor_col=1,match_col=1,other_match_col1=None)
                Will be marked invalid because more than one match for (t=1,match_col=1) column  with same match_col

            5) Row(t=1,anchor_col=1,match_col=1,other_match_col1=1)
               Row(t=1,anchor_col=1,match_col=2,other_match_col1=None)
               Will be marked invalid because more than one match for anchor column  with same match_col


            return:
            :param df: DataFrame
            :param key_cols: The cols making the key - valid rows will be reduced into one row for each key
            :param match_cols: columns we have to collect values for and make sure there is not more than one
            :return:
            :rtype: DataFrame
        """
        assert  len(key_cols)+len(match_cols)+len([self._start_validity_column]) == len(set(key_cols+match_cols+[self._start_validity_column])), 'There is intersection of exclusive columns sets'
        assert len(set(df.columns).intersection(set(key_cols+match_cols))) == len(key_cols+match_cols), 'the key  and match columns are in the schema'
        f_agg_column = lambda x: f'{x}_values'
        window_spec = Window.partitionBy(*key_cols)
        agg_expressions = [F.collect_set(column).over(window_spec).alias(f_agg_column(column)) for column in match_cols]
        df_agg = df.select(df.columns+agg_expressions)
        for col in match_cols:
            condition = self._start_condition & (F.size(f_agg_column(col))>1)
            df_agg = df_agg.withColumn(self._start_validity_column,
                F.when(condition, F.lit(False)).otherwise(F.col(self._start_validity_column)))
            condition = self._start_condition & (F.size(F.col(f_agg_column(col))) == 1)
            df_agg = df_agg.withColumn(col,F.when(condition,F.col(f_agg_column(col)).getItem(0)).otherwise(F.col(col)))
        df_agg=df_agg.drop(*[f_agg_column(column) for column in match_cols])
        return df_agg.distinct()


    def _create_transaction_window_partitioned_by_hero(self) -> WindowSpec:
        return Window.partitionBy(self._a_col).orderBy(self._time_col, self._b_col, self._status_col)
    def _create_transaction_window_partitioned_by_matched(self) -> WindowSpec:
        return Window.partitionBy(self._b_col).orderBy(self._time_col, self._a_col, self._status_col)

    def _mark_end_time_with_ending_reason(
                                self,
                                df: DataFrame,
                                match_columns: List[str]) -> DataFrame:

        assert self.END_TIME_MAP_COL not in df.columns, f'{self.END_TIME_MAP_COL} is temporary columns we not expect to see'
        assert self.END_EVENTS_ARRAY_COL not in df.columns, f'{self.END_EVENTS_ARRAY_COL} is temporary columns we not expect to see'
        assert len(match_columns) == 2, f' We see {len(match_columns)} instead of two column'
        for col in match_columns:
            assert col in df.columns
        df = df.withColumn(self.END_EVENTS_ARRAY_COL, F.array())
        curren_status_is_start = (F.col(self._status_col) == self.Status.START.value)
        df = df.withColumn(self.START_TIME_COL, F.when(curren_status_is_start, F.col(self._time_col)))
        df = self._mark_end_time_with_ending_reason_one_direction(df, a_col=match_columns[0], b_col=match_columns[1])
        df = self._mark_end_time_with_ending_reason_one_direction(df, a_col=match_columns[1], b_col=match_columns[0])
        df = df.select("*", F.explode_outer(F.col(self.END_EVENTS_ARRAY_COL)).alias(self.END_TIME_MAP_COL))
        df = df.withColumn(self.END_REASON_ORDINAL, F.col(self.END_TIME_MAP_COL + '.' + self.END_REASON_ORDINAL))
        df = df.withColumn(self.END_REASON_NAME, F.col(self.END_TIME_MAP_COL + '.' + self.END_REASON_NAME))
        df = df.withColumn(self.END_TIME_COL, F.col(self.END_TIME_MAP_COL+'.'+self.END_TIME_COL))
        df = df.drop(self.END_TIME_MAP_COL, self.END_EVENTS_ARRAY_COL).distinct()
        return df

    def _build_transaction_end_when(self, cond: Column,
                                    output_col: str,
                                    reason: EndingTransactionReason,
                                    window_spec: WindowSpec):
        """
            This is for inserting a couple( time,end reason)  element into output_cl
        :param cond: The condition when
        :param output_col:  where to ad the Map element
        :param reason: The reason for ending transaction
        :param window_spec: The Window spec we use
        :return:
        """
        return F.when(cond, F.concat(F.col(output_col),
                                     F.array(F.struct(F.lit(reason.name).alias(self.END_REASON_NAME),
                                                      F.lit(reason.value).alias(self.END_REASON_ORDINAL),
                                                      F.lead(self._time_col, 1).over(window_spec).alias(
                                                          self.END_TIME_COL)))
                      )).otherwise(F.col(output_col))

    def _mark_end_time_with_ending_reason_one_direction(
                                self,
                                df: DataFrame,
                                a_col: str,
                                b_col: str):
        """
        We assume only valid start i.e. = per each (time,a_col,, status=start, F.col(self._start_validity_column)=True ) there is one row
        Notice the window order. If we have (anchor=1,status=start,t=1) and (anchor=1,starts=end,t=1)
        The usage of _matched_col in the order by, is just for determinism of the result, and preference
        of ending not involving null matched  col

        :param df:
        :param a_col:
        :param b_col:
        :return: DataFrame
        """
        print(f'anchor={a_col}')
        print(f'match={b_col}')
        order_by = [F.col(a_col).asc_nulls_last(),
                    F.col(self._time_col).asc(),
                    F.col(self._status_col).desc(),
                    F.col(b_col).asc_nulls_last()] + \
                   [F.col(col).asc_nulls_last() for col in self._other_matches]
        window_spec: WindowSpec = Window.partitionBy(a_col).orderBy(*order_by)

        same_a_col = F.col(a_col) == F.lead(a_col, 1).over(window_spec)
        same_b_col = F.col(b_col) == F.lead(b_col, 1).over(window_spec)
        same_match = same_b_col & same_a_col
        next_status_is_start = F.lead(self._status_col, 1).over(window_spec) == self.Status.START.value
        next_status_is_update = F.lead(self._status_col, 1).over(window_spec) == self.Status.UPDATE.value
        next_status_is_end = F.lead(self._status_col, 1).over(window_spec) == self.Status.END.value
        next_b_col_is_null = F.lead(b_col, 1).over(window_spec).isNull()
        match_broken = ~same_b_col & ~next_b_col_is_null

        curren_status_is_start = (F.col(self._status_col) == self.Status.START.value)
        curren_status_is_valid_start = curren_status_is_start & (F.col(self._start_validity_column) == F.lit(True))
        start_after_start_valid = curren_status_is_valid_start & next_status_is_start
        end_after_start_valid = curren_status_is_valid_start & next_status_is_end
        update_after_start_valid = curren_status_is_valid_start & next_status_is_update


        ending_reason_to_condition_map = {
            self.EndingTransactionReason.NEXT_END_FOR_SAME_MATCH : same_match & end_after_start_valid,
            self.EndingTransactionReason.NEXT_END_MATCH_BREAK: same_a_col & match_broken & end_after_start_valid,
            self.EndingTransactionReason.NEXT_END_NULL: same_a_col & next_b_col_is_null & end_after_start_valid,
            self.EndingTransactionReason.NEXT_START_MATCH_BREAK: same_a_col & match_broken & start_after_start_valid,
            self.EndingTransactionReason.NEXT_START_SAME_MATCH: same_match & start_after_start_valid,
            self.EndingTransactionReason.NEXT_START_MATCH_NULL: same_a_col & start_after_start_valid & next_b_col_is_null,
            self.EndingTransactionReason.NEXT_UPDATE_MATCH_BREAK: same_a_col & update_after_start_valid & match_broken,
            self.EndingTransactionReason.NEXT_UPDATE_MATCH_NULL: same_a_col & update_after_start_valid & next_b_col_is_null,
        }

        for reason in self._ending_conditions:
            if reason not in ending_reason_to_condition_map.keys():
                raise Exception(f"reason {reason.value} not in our support")
            cond=ending_reason_to_condition_map[reason]
            when = self._build_transaction_end_when(
                cond=cond,
                output_col= self.END_EVENTS_ARRAY_COL,
                reason=reason,
                window_spec=window_spec)
            df = df.withColumn(self.END_EVENTS_ARRAY_COL, when)

        return df.distinct()


    def _choose_final_transactions_end(self, df):
        df = df.where(F.col(self.END_TIME_COL).isNotNull())
        window_spec:WindowSpec = Window.partitionBy(self._a_col, self._b_col, self.START_TIME_COL).orderBy(*[F.col(self.END_TIME_COL), F.col(self.END_REASON_ORDINAL).asc()])
        df = df.withColumn('final_end_time',F.first(self.END_TIME_COL).over(window_spec))
        df = df.withColumn('final_end_reason', F.first(self.END_REASON_NAME).over(window_spec))
        df = df.drop(self.END_TIME_COL, self.END_REASON_ORDINAL, self.END_REASON_NAME)
        df = df.where(F.col('final_end_time') == F.col('end_time')).orderBy('start_time')
        df = df.distinct()
        return df


    def _connect_succeeding_transactions(self, df):
        df = df.where(F.col('final_end_time').isNotNull())
        window_spec:WindowSpec = Window.partitionBy(self._a_col, self._b_col).orderBy(self.START_TIME_COL)
        has_upper_neighbor = F.lead('start_time').over(window_spec) == F.col('final_end_time')
        has_lower_neighbor = F.lag('final_end_time').over(window_spec) == F.col('start_time')
        df = df.withColumn('neighbors_status',
                           F.when(has_upper_neighbor & has_lower_neighbor, 3).
                           otherwise(F.when(has_upper_neighbor,1).otherwise(F.when(has_lower_neighbor,2).otherwise(0))))
        df = df.where(F.col('neighbors_status').isin(*[2,1,0]))
        df = df.withColumn('final_end_time',F.when(F.col('neighbors_status') == 1,F.lead('final_end_time').over(window_spec)).otherwise(F.col('final_end_time')))
        df = df.where(F.col('neighbors_status').isin(*[1,0]))
        df = df.orderBy(self._a_col,self._b_col,'start_time')
        df = df.drop('neighbors_status')
        return df.distinct()
    def prepare_transactions(
                    self,
                    df: DataFrame) -> DataFrame:
        df_normalized = self._initialize_dataframe(df)
        df = self._prepare_valid_transaction_start_points(df_normalized)
        df = self._mark_end_time_with_ending_reason(df, match_columns=[self._a_col, self._b_col])
        df = self._choose_final_transactions_end(df)
        df = self._connect_succeeding_transactions(df)
        return  df_normalized,df.distinct()
