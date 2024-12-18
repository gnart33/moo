# src/moo/transform/transformers/swap_transformer.py
from typing import List, Dict
import polars as pl
import logging
from moo.transform.validators.data_validator import SwapDataValidator
from datetime import datetime


class SwapTransformer:
    def __init__(self):
        self.validator = SwapDataValidator()
        self.logger = logging.getLogger(__name__)

    def transform(self, raw_data: List[Dict]) -> pl.DataFrame:
        # Convert to LazyFrame
        df = pl.LazyFrame(raw_data)

        # Apply transformations
        df = self._apply_transformations(df)

        # Validate data
        is_valid, error_msg = self.validator.validate(df)
        if not is_valid:
            self.logger.error(f"Data validation failed: {error_msg}")
            raise ValueError(f"Data validation failed: {error_msg}")

        return df.collect()

    def _apply_transformations(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df.with_columns(
            [
                # Convert Unix timestamp to datetime
                pl.col("blockTimestamp")
                .cast(pl.Int64)
                .map_elements(
                    lambda x: datetime.fromtimestamp(x), return_dtype=pl.Datetime
                )
                .cast(pl.Datetime),
                # Normalize addresses to lowercase
                pl.col("tokenInSymbol").str.to_lowercase(),
                pl.col("tokenOutSymbol").str.to_lowercase(),
                # Round amounts to 18 decimals
                pl.col("tokenAmountIn").cast(float).round(18),
                pl.col("tokenAmountOut").cast(float).round(18),
            ]
        )
