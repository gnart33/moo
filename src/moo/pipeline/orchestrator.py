# src/moo/pipeline/orchestrator.py
from moo.data_collector import CowSwapDataCollector
from moo.data_processor import DataProcessor
from moo.database.connection import DatabaseManager
from moo.storage.gcp_manager import GCPStorageManager
from moo.models import Order, DailyMetrics
import logging
from datetime import datetime
import pandas as pd
from moo.extract.extractors.cowswap_extractor import CowSwapExtractor
from moo.transform.transformers.swap_transformer import SwapTransformer
from moo.transform.validators.data_validator import SwapDataValidator
from moo.load.loaders.postgres_loader import PostgresLoader


# src/moo/pipeline/orchestrator.py
class SwapPipelineOrchestrator:
    def __init__(self):
        self.extractor = CowSwapExtractor(TheGraphConnector())
        self.transformer = SwapTransformer()
        self.validator = SwapDataValidator()
        self.loader = PostgresLoader()

    def run_pipeline(self):
        # Extract
        raw_data = self.extractor.extract_swaps()

        # Transform
        transformed_data = self.transformer.transform(raw_data)

        # Validate
        if not self.validator.validate_schema(transformed_data):
            raise ValidationError("Data validation failed")

        # Load
        self.loader.load_swaps(transformed_data)
