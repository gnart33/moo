# src/moo/extract/extractors/cowswap_extractor.py
from typing import Dict, List, Optional
from moo.extract.connectors.thegraph_connector import TheGraphConnector


class CowSwapExtractor:
    def __init__(self, connector: TheGraphConnector):
        self.connector = connector

    def extract_swaps(self, start_timestamp: Optional[int] = None) -> List[Dict]:
        query = self._build_swap_query(start_timestamp)
        return self.connector.execute_query(query)
