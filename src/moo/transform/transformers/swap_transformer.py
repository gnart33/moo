# src/moo/transform/transformers/swap_transformer.py
from typing import List, Dict
import pandas as pd


class SwapTransformer:
    def transform(self, raw_data: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(raw_data)
        return self._apply_transformations(df)

    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._normalize_amounts(df)
        df = self._add_timestamps(df)
        return self._calculate_metrics(df)
