# src/moo/pipeline/orchestrator.py
import os
from dotenv import load_dotenv

load_dotenv()

from moo.extract.extractors.cowswap_extractor import CowSwapExtractor
from moo.transform.transformers.swap_transformer import SwapTransformer

# from moo.transform.validators.data_validator import SwapDataValidator
from moo.load.loaders.postgres_loader import PostgresLoader
from moo.utils.config import load_config

config = load_config("pipeline_config.yaml")


class SwapPipelineOrchestrator:
    def __init__(self):
        self.extractor = CowSwapExtractor()
        self.transformer = SwapTransformer()
        connection_string = "postgresql://{user}:{password}@localhost:5432/{db}".format(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            db=os.getenv("POSTGRES_DB"),
        )
        self.loader = PostgresLoader(connection_string)

    def run_pipeline(self):
        # Extract
        raw_data = self.extractor.swaps_initial(save_to_file=True)

        # Transform
        transformed_data = self.transformer.transform(raw_data)

        # Load
        self.loader.load_swaps(transformed_data)


if __name__ == "__main__":
    orchestrator = SwapPipelineOrchestrator()
    orchestrator.run_pipeline()
