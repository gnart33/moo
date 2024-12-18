from typing import Dict, List
from pathlib import Path
import json

from moo.utils.config import load_config
from moo.utils.query import SWAP_QUERY
from moo.extract.connectors.thegraph_connector import TheGraphDataCollector

config = load_config("pipeline_config.yaml")


class CowSwapExtractor(TheGraphDataCollector):
    def __init__(
        self,
        subgraph_id: str = config["extract"]["cowswap_subgraph_id"],
    ):
        super().__init__(subgraph_id=subgraph_id)
        self.raw_data_dir = Path(config["extract"]["raw_data_dir"])
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

    def swaps_initial(self, save_to_file: bool = False) -> List[Dict]:
        skip = 0
        all_swaps = []
        while True:
            query = SWAP_QUERY.format(skip=skip)
            result = self.query(query)
            if len(result["data"]["swaps"]) == 0:
                break
            all_swaps.extend(result["data"]["swaps"])
            skip += 1000
        if save_to_file:
            with open(self.raw_data_dir / "swaps.json", "w") as f:
                json.dump(all_swaps, f)
        return all_swaps

    def swaps_latest(self) -> List[Dict]:
        pass
