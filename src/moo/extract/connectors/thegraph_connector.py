import logging
import os
import requests

from moo.utils.config import load_config

config = load_config("pipeline_config.yaml")
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TheGraphDataCollector:
    def __init__(
        self,
        subgraph_id: str,
        base_url: str = config["extract"]["subgraph_base_url"],
        api_key: str = os.getenv("THEGRAPH_API_KEY"),
    ):
        self.base_url = base_url
        self.subgraph_id = subgraph_id
        self.api_key = api_key
        self.query_url = (
            f"{self.base_url}/{self.api_key}/subgraphs/id/{self.subgraph_id}"
        )

    def query(self, query: str):
        request = requests.post(self.query_url, "", json={"query": query}, timeout=15)
        if request.status_code != 200:
            raise Exception(
                f"Query failed. Url: {self.query_url}. Return code is {request.status_code}\n{query}"
            )
        result = request.json()
        return result
