import pytest
import pandas as pd
from datetime import datetime

@pytest.fixture
def sample_sales_data():
    return pd.DataFrame({
        'order_id': ['ORD_001', 'ORD_002'],
        'product_id': ['PROD_001', 'PROD_002'],
        'customer_id': ['CUST_001', 'CUST_002'],
        'quantity': [2, 1],
        'unit_price': [100.0, 50.0],
        'order_date': [datetime.now(), datetime.now()]
    })