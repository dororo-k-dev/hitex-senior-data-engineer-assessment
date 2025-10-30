import pandas as pd
import pytest

from dags.hitex_sales_pipeline import HitexDataProcessor

def test_data_quality_validation():
    """Test data quality validation rules"""
    processor = HitexDataProcessor()
    
    # Test data with quality issues
    test_data = pd.DataFrame({
        'order_id': ['ORD001', None, 'ORD003'],
        'product_id': ['PROD1', 'PROD2', None],
        'quantity': [1, -1, 3],
        'unit_price': [10.0, 0, 30.0]
    })
    
    results = processor.run_data_quality_checks(test_data, 'test_suite')
    assert results['failed_checks'] > 0  # Should catch issues

def test_quality_gates():
    """Test quality gate enforcement"""
    processor = HitexDataProcessor()
    
    good_data = pd.DataFrame({
        'order_id': ['ORD001', 'ORD002'],
        'product_id': ['PROD1', 'PROD2'],
        'quantity': [1, 2],
        'unit_price': [10.0, 20.0]
    })
    
    # Should pass quality gates
    transformed = processor.transform_with_quality_gates(good_data)
    assert transformed is not None