# src/moo/load/loaders/postgres_loader.py
from sqlalchemy import create_engine
import pandas as pd


class PostgresLoader:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def load_swaps(self, df: pd.DataFrame):
        with self.engine.begin() as conn:
            df.to_sql("swaps", conn, if_exists="append", index=False)
