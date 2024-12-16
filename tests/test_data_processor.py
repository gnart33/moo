import pytest
import pandas as pd
from datetime import datetime
import json
from pathlib import Path
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_processor import DataProcessor

@pytest.fixture
def sample_order():
    return {
        "orderUid": "0x1234",
        "owner": "0xabcd",
        "sellToken": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "buyToken": "0x6b175474e89094c44da98b954eedeac495271d0f",
        "sellAmount": "1000000000000000000",  # 1 ETH
        "buyAmount": "1800000000000000000000",  # 1800 DAI
        "validTo": str(int(datetime.now().timestamp())),
        "appData": "0x0",
        "feeAmount": "1000000000000000",
        "kind": "sell",
        "partiallyFillable": False,
        "status": "fulfilled"
    }

def test_validate_order(sample_order):
    processor = DataProcessor()
    assert processor.validate_order(sample_order) == True

    # Test with missing field
    invalid_order = sample_order.copy()
    del invalid_order['orderUid']
    assert processor.validate_order(invalid_order) == False

def test_normalize_token_symbol():
    processor = DataProcessor()
    
    # Test known token
    assert processor.normalize_token_symbol(
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    ) == "WETH"
    
    # Test unknown token
    unknown_address = "0x1234567890abcdef1234567890abcdef12345678"
    assert processor.normalize_token_symbol(unknown_address) == unknown_address[:10]

def test_compute_metrics():
    processor = DataProcessor()
    
    # Create sample DataFrame
    data = {
        'owner': ['0x1', '0x2', '0x1'],
        'sellToken': ['WETH', 'DAI', 'WETH'],
        'buyToken': ['DAI', 'WETH', 'USDC'],
        'sellAmount': [1.0, 2.0, 1.5],
        'buyAmount': [1800.0, 1.0, 2700.0],
        'feeAmount': [0.001, 0.002, 0.001],
        'status': ['fulfilled', 'fulfilled', 'cancelled']
    }
    df = pd.DataFrame(data)
    
    metrics = processor.compute_metrics(df)
    
    assert metrics['total_orders'] == 3
    assert metrics['unique_traders'] == 2
    assert metrics['total_volume_eth'] == 3.5  # 1.0 + 1.5 + 1.0 (from buyAmount)
    assert metrics['success_rate'] == pytest.approx(66.67, rel=1e-2)