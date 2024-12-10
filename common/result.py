import json
from typing import Optional
from .dataframe import DataFrame

class Result:
    def __init__(self, task_id: str, data_frame: Optional[DataFrame], success: bool, error_message: Optional[str] = None):
        self.task_id = task_id
        self.data_frame = data_frame
        self.success = success
        self.error_message = error_message

    def to_json(self) -> str:
        return json.dumps({
            "task_id": self.task_id,
            "data_frame": self.data_frame.to_json() if self.data_frame else None,
            "success": self.success,
            "error_message": self.error_message
        })

    @staticmethod
    def from_json(data: str):
        obj = json.loads(data)
        data_frame = DataFrame.from_json(obj["data_frame"]) if obj.get("data_frame") else None
        return Result(
            task_id=obj["task_id"],
            data_frame=data_frame,
            success=obj["success"],
            error_message=obj.get("error_message")
        )
