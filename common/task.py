import uuid
from typing import List, Dict, Any, Optional
from .dataframe import DataFrame
from .result import Result

class Task:
    def __init__(self, operation: str, task_id: Optional[str] = None):
        self.task_id = task_id if task_id else str(uuid.uuid4())
        self.operation = operation

    def execute(self) -> Result:
        raise NotImplementedError("Execute method not implemented.")

class JoinTask(Task):
    def __init__(self, df1: DataFrame, df2: DataFrame, join_keys: List[str]):
        super().__init__("JOIN")
        self.df1 = df1
        self.df2 = df2
        self.join_keys = join_keys

    def execute(self) -> 'Result':
        try:
            lookup = {}
            for row in self.df2.rows:
                key = tuple(row.get(k, "") for k in self.join_keys)
                lookup[key] = row

            joined_rows = []
            for row in self.df1.rows:
                key = tuple(row.get(k, "") for k in self.join_keys)
                if key in lookup:
                    joined_row = {**row, **lookup[key]}
                    joined_rows.append(joined_row)

            result_df = DataFrame(joined_rows)
            return Result(self.task_id, result_df, True)
        except Exception as e:
            return Result(self.task_id, None, False, str(e))

class JoinTask(Task):
    def __init__(self, df1: DataFrame, df2: DataFrame, join_keys: List[str], task_id: Optional[str] = None):
        super().__init__("JOIN", task_id)
        self.df1 = df1
        self.df2 = df2
        self.join_keys = join_keys

    def execute(self) -> 'Result':
        try:
            lookup = {}
            for row in self.df2.rows:
                key = tuple(row.get(k, "") for k in self.join_keys)
                lookup[key] = row

            joined_rows = []
            for row in self.df1.rows:
                key = tuple(row.get(k, "") for k in self.join_keys)
                if key in lookup:
                    joined_row = {**row, **lookup[key]}
                    joined_rows.append(joined_row)

            result_df = DataFrame(joined_rows)
            return Result(self.task_id, result_df, True)
        except Exception as e:
            return Result(self.task_id, None, False, str(e))

class DistinctTask(Task):
    def __init__(self, df: DataFrame, columns: List[str], task_id: Optional[str] = None):
        super().__init__("DISTINCT", task_id)
        self.df = df
        self.columns = columns

    def execute(self) -> 'Result':
        try:
            seen = set()
            distinct_rows = []
            for row in self.df.rows:
                key = tuple(row.get(col, "") for col in self.columns)
                if key not in seen:
                    seen.add(key)
                    distinct_rows.append(row)
            result_df = DataFrame(distinct_rows)
            return Result(self.task_id, result_df, True)
        except Exception as e:
            return Result(self.task_id, None, False, str(e))

class GroupCountTask(Task):
    def __init__(self, df: DataFrame, group_by_columns: List[str], task_id: Optional[str] = None):
        super().__init__("GROUP_COUNT", task_id)
        self.df = df
        self.group_by_columns = group_by_columns

    def execute(self) -> 'Result':
        try:
            count_dict = {}
            for row in self.df.rows:
                key = tuple(row.get(col, "") for col in self.group_by_columns)
                count_dict[key] = count_dict.get(key, 0) + 1

            grouped_rows = []
            for key, count in count_dict.items():
                grouped_row = {col: val for col, val in zip(self.group_by_columns, key)}
                grouped_row["count"] = count
                grouped_rows.append(grouped_row)

            result_df = DataFrame(grouped_rows)
            return Result(self.task_id, result_df, True)
        except Exception as e:
            return Result(self.task_id, None, False, str(e))

class GroupSumTask(Task):
    def __init__(self, df: DataFrame, group_by_columns: List[str], sum_column: str, task_id: Optional[str] = None):
        super().__init__("GROUP_SUM", task_id)
        self.df = df
        self.group_by_columns = group_by_columns
        self.sum_column = sum_column

    def execute(self) -> 'Result':
        try:
            sum_dict = {}
            for row in self.df.rows:
                key = tuple(row.get(col, "") for col in self.group_by_columns)
                value = float(row.get(self.sum_column, 0))
                sum_dict[key] = sum_dict.get(key, 0) + value

            grouped_rows = []
            for key, total in sum_dict.items():
                grouped_row = {col: val for col, val in zip(self.group_by_columns, key)}
                grouped_row[f"sum_{self.sum_column}"] = total
                grouped_rows.append(grouped_row)

            result_df = DataFrame(grouped_rows)
            return Result(self.task_id, result_df, True)
        except Exception as e:
            return Result(self.task_id, None, False, str(e))

class OrderByTask(Task):
    def __init__(self, df: DataFrame, order_by_columns: List[str], ascending: bool = True, task_id: Optional[str] = None):
        super().__init__("ORDER_BY", task_id)
        self.df = df
        self.order_by_columns = order_by_columns
        self.ascending = ascending

    def execute(self) -> 'Result':
        try:
            sorted_rows = sorted(
                self.df.rows,
                key=lambda row: tuple(row.get(col, "") for col in self.order_by_columns),
                reverse=not self.ascending
            )
            result_df = DataFrame(sorted_rows)
            return Result(self.task_id, result_df, True)
        except Exception as e:
            return Result(self.task_id, None, False, str(e))
