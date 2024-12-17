# src/moo/transform/validators/data_validator.py
from typing import List, Dict, Optional
import polars as pl
from datetime import datetime
import logging


class SwapDataValidator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def validate(self, df: pl.LazyFrame) -> tuple[bool, Optional[str]]:
        """
        Run all validations and return result with error message if any
        """
        validations = [
            self._validate_schema,
            self._validate_data_types,
            self._validate_business_rules,
            # self._validate_nulls,
            # self._validate_duplicates,
        ]

        for validation in validations:
            is_valid, error_msg = validation(df)
            if not is_valid:
                return False, error_msg

        return True, None

    def _validate_schema(self, df: pl.LazyFrame) -> tuple[bool, Optional[str]]:
        """Validate that all required columns are present"""
        required_columns = {
            "transactionHash",
            "blockTimestamp",
            "tokenInSymbol",
            "tokenOutSymbol",
            "tokenAmountIn",
            "tokenAmountOut",
        }

        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            return False, f"Missing required columns: {missing_columns}"
        return True, None

    def _validate_data_types(self, df: pl.LazyFrame) -> tuple[bool, Optional[str]]:
        """Validate data types of columns"""
        try:
            df = df.with_columns(
                [
                    # pl.col("transaction_hash").cast(str),
                    pl.col("blockTimestamp").cast(pl.Datetime),
                    pl.col("tokenInSymbol").cast(str),
                    pl.col("tokenOutSymbol").cast(str),
                    pl.col("tokenAmountIn").cast(float),
                    pl.col("tokenAmountOut").cast(float),
                ]
            )
            return True, None
        except Exception as e:
            return False, f"Data type validation failed: {str(e)}"

    def _validate_business_rules(self, df: pl.LazyFrame) -> tuple[bool, Optional[str]]:
        """Validate business rules"""
        try:
            # Check if amounts are positive
            invalid_amounts = df.filter(
                (pl.col("tokenAmountIn") <= 0) | (pl.col("tokenAmountOut") <= 0)
            ).collect()

            if len(invalid_amounts) > 0:
                return False, "Found transactions with zero or negative amounts"

            # Check if timestamps are in valid range
            min_date = datetime(2020, 1, 1)  # CowSwap launch date
            invalid_dates = df.filter(pl.col("blockTimestamp") < min_date).collect()

            if len(invalid_dates) > 0:
                return False, f"Found transactions before {min_date}"

            return True, None

        except Exception as e:
            return False, f"Business rule validation failed: {str(e)}"

    def _validate_nulls(self, df: pl.LazyFrame) -> tuple[bool, Optional[str]]:
        """Check for null values in required fields"""
        null_counts = df.select([pl.all().null_count()]).collect()

        # Sum all null counts across columns
        total_nulls = null_counts.sum().sum()

        if total_nulls > 0:
            return False, f"Found null values: {null_counts}"
        return True, None

    def _validate_duplicates(self, df: pl.LazyFrame) -> tuple[bool, Optional[str]]:
        """Check for duplicate transaction hashes"""
        duplicates = (
            df.select(pl.col("transactionHash"))
            .group_by("transactionHash")
            .count()
            .filter(pl.col("count") > 1)
            .collect()
        )

        if len(duplicates) > 0:
            return False, f"Found duplicate transactions: {duplicates}"
        return True, None
