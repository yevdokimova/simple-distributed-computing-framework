import json
from .task import Task, JoinTask, DistinctTask, GroupCountTask, GroupSumTask, OrderByTask
from .result import Result
from .dataframe import DataFrame

class SerializationUtil:
    @staticmethod
    def serialize_task(task: Task) -> str:
        if isinstance(task, JoinTask):
            task_dict = {
                "task_id": task.task_id,
                "operation": task.operation,
                "df1": {"rows": task.df1.rows},
                "df2": {"rows": task.df2.rows},
                "join_keys": task.join_keys
            }
        elif isinstance(task, DistinctTask):
            task_dict = {
                "task_id": task.task_id,
                "operation": task.operation,
                "df": {"rows": task.df.rows},
                "columns": task.columns
            }
        elif isinstance(task, GroupCountTask):
            task_dict = {
                "task_id": task.task_id,
                "operation": task.operation,
                "df": {"rows": task.df.rows},
                "group_by_columns": task.group_by_columns
            }
        elif isinstance(task, GroupSumTask):
            task_dict = {
                "task_id": task.task_id,
                "operation": task.operation,
                "df": {"rows": task.df.rows},
                "group_by_columns": task.group_by_columns,
                "sum_column": task.sum_column
            }
        elif isinstance(task, OrderByTask):
            task_dict = {
                "task_id": task.task_id,
                "operation": task.operation,
                "df": {"rows": task.df.rows},
                "order_by_columns": task.order_by_columns,
                "ascending": task.ascending
            }
        else:
            raise ValueError(f"Unknown Task type: {type(task)}")
        return json.dumps(task_dict)

    @staticmethod
    def deserialize_task(data: str) -> Task:
        obj = json.loads(data)
        operation = obj["operation"]
        task_id = obj["task_id"]
        if operation == "JOIN":
            df1 = DataFrame(rows=obj["df1"]["rows"])
            df2 = DataFrame(rows=obj["df2"]["rows"])
            join_keys = obj["join_keys"]
            return JoinTask(df1, df2, join_keys, task_id=task_id)
        elif operation == "DISTINCT":
            df = DataFrame(rows=obj["df"]["rows"])
            columns = obj["columns"]
            return DistinctTask(df, columns, task_id=task_id)
        elif operation == "GROUP_COUNT":
            df = DataFrame(rows=obj["df"]["rows"])
            group_by_columns = obj["group_by_columns"]
            return GroupCountTask(df, group_by_columns, task_id=task_id)
        elif operation == "GROUP_SUM":
            df = DataFrame(rows=obj["df"]["rows"])
            group_by_columns = obj["group_by_columns"]
            sum_column = obj["sum_column"]
            return GroupSumTask(df, group_by_columns, sum_column, task_id=task_id)
        elif operation == "ORDER_BY":
            df = DataFrame(rows=obj["df"]["rows"])
            order_by_columns = obj["order_by_columns"]
            ascending = obj["ascending"]
            return OrderByTask(df, order_by_columns, ascending, task_id=task_id)
        else:
            raise ValueError(f"Unknown operation type: {operation}")

    @staticmethod
    def serialize_result(result: Result) -> str:
        return result.to_json()

    @staticmethod
    def deserialize_result(data: str) -> Result:
        return Result.from_json(data)
