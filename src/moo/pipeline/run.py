# src/moo/pipeline/run.py
from typing import Optional
from datetime import datetime
import logging
import argparse

from moo.extract.extractors.cowswap_extractor import CowSwapExtractor
from moo.transform.transformers.swap_transformer import SwapTransformer
from moo.load.loaders.postgres_loader import PostgresLoader
from moo.utils.config import load_config

logger = logging.getLogger(__name__)


def run_pipeline(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    config_path: str = "config/pipeline_config.yaml",
):
    """
    Main pipeline execution function
    """
    try:
        # Load configuration
        config = load_config(config_path)

        # Initialize components
        extractor = CowSwapExtractor(config["extract"])
        transformer = SwapTransformer(config["transform"])
        loader = PostgresLoader(config["load"]["connection_string"])

        # Extract
        logger.info(f"Starting extraction from {start_date} to {end_date}")
        raw_data = extractor.extract_swaps(start_date, end_date)

        # Transform
        logger.info("Starting transformation")
        transformed_data = transformer.transform(raw_data)

        # Load
        logger.info("Starting load")
        loader.load_swaps(transformed_data)

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Run CowSwap data pipeline")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--config", type=str, default="config/pipeline_config.yaml")

    args = parser.parse_args()

    start_date = (
        datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    )
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None

    run_pipeline(start_date, end_date, args.config)


if __name__ == "__main__":
    main()
