import pytest
from files.data_generator import generate_sales_data
import pandas as pd
import os

def test_data_generation():
    """Test if data generation works"""
    filename = generate_sales_data(num_records=10)
    filepath = f'data/raw/{filename}'
    
    assert os.path.exists(filepath)
    
    df = pd.read_csv(filepath)
    assert len(df) == 10
    assert all(col in df.columns for col in 
              ['transaction_id', 'customer_name', 'product', 'price'])
    
    # Cleanup
    os.remove(filepath)