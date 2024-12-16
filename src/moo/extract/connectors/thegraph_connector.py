# src/moo/extract/connectors/thegraph_connector.py
from typing import Dict
from moo.extract.connectors.base_connector import BaseConnector


class TheGraphConnector(BaseConnector):
    def __init__(self, subgraph_id: str, api_key: str):
        super().__init__({"subgraph_id": subgraph_id, "api_key": api_key})
        self.base_url = "https://gateway.thegraph.com/api"

    def execute_query(self, query: str) -> Dict:
        # Implementation here
        pass
