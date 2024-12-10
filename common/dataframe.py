
import json
from typing import List, Dict

class DataFrame:
    def __init__(self, rows: List[Dict[str, str]]):
        self.rows = rows

    def to_json(self) -> str:
        return json.dumps({"rows": self.rows})

    @staticmethod
    def from_json(data: str):
        obj = json.loads(data)
        return DataFrame(obj["rows"])
