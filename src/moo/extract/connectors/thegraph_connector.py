# src/moo/extract/connectors/thegraph_connector.py
from typing import Dict
from moo.extract.connectors.base_connector import BaseConnector


class TheGraphConnector(BaseConnector):
    def __init__(self):
        super().__init__(
            {
                "subgraph_id": self.config["extract"]["subgraph_id"],
                "api_key": self.config["extract"]["subgraph_api_key"],
            }
        )
        self.base_url = self.config["extract"]["subgraph_base_url"]

    def execute_query(self, query: str) -> Dict:
        pass
