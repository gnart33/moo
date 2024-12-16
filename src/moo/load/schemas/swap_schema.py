# src/moo/load/schemas/swap_schema.py
from moo.load.schemas.base import BaseModel  # Import our custom base class
from sqlalchemy import Column, String, DateTime, Float


class SwapSchema(BaseModel):  # Inherit from BaseModel instead of Base
    __tablename__ = "swaps"

    # id is already included in BaseModel
    transaction_hash = Column(String(66), unique=True, nullable=False)
    block_timestamp = Column(DateTime, nullable=False)
    token_in = Column(String(42), nullable=False)
    token_out = Column(String(42), nullable=False)
    amount_in = Column(Float, nullable=False)
    amount_out = Column(Float, nullable=False)
