from typing import Dict, Any


class BaseConnector:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def connect(self):
        raise NotImplementedError

    def execute_query(self, query: str) -> Dict:
        raise NotImplementedError
