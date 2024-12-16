from typing import Dict, Any


class BaseConnector:
    def __init__(self, connection_params: Dict[str, Any]):
        self.connection_params = connection_params

    def connect(self):
        raise NotImplementedError

    def execute_query(self, query: str) -> Dict:
        raise NotImplementedError
