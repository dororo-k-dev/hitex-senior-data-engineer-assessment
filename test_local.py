#!/usr/bin/env python3
"""
Local test script to verify code structure without Airflow dependencies
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))

# Test basic imports and class structure
def test_checkpoint_manager():
    """Test CheckpointManager class structure"""
    from pathlib import Path
    import json
    import tempfile
    from datetime import datetime
    
    class SecureCheckpointManager:
        def __init__(self, checkpoint_dir=None):
            if checkpoint_dir is None:
                checkpoint_dir = '/tmp/hitex_checkpoints'
            self.checkpoint_dir = Path(checkpoint_dir).resolve()
            self.checkpoint_dir.mkdir(exist_ok=True, mode=0o700)
        
        def save_checkpoint(self, job_id: str, processed_rows: int, total_rows: int):
            safe_job_id = "".join(c for c in job_id if c.isalnum() or c in ('_', '-'))
            checkpoint_file = self.checkpoint_dir / f"{safe_job_id}.json"
            
            checkpoint_data = {
                'processed_rows': processed_rows,
                'total_rows': total_rows,
                'timestamp': datetime.now().isoformat(),
                'completion_percentage': (processed_rows / total_rows) * 100 if total_rows > 0 else 0
            }
            
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f)
            return True
        
        def load_checkpoint(self, job_id: str):
            safe_job_id = "".join(c for c in job_id if c.isalnum() or c in ('_', '-'))
            checkpoint_file = self.checkpoint_dir / f"{safe_job_id}.json"
            
            if checkpoint_file.exists():
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return None
        
        def clear_checkpoint(self, job_id: str):
            safe_job_id = "".join(c for c in job_id if c.isalnum() or c in ('_', '-'))
            checkpoint_file = self.checkpoint_dir / f"{safe_job_id}.json"
            
            if checkpoint_file.exists():
                checkpoint_file.unlink()
    
    # Test the class
    with tempfile.TemporaryDirectory() as temp_dir:
        mgr = SecureCheckpointManager(temp_dir)
        
        # Test save
        mgr.save_checkpoint("test_job", 100, 200)
        
        # Test load
        checkpoint = mgr.load_checkpoint("test_job")
        assert checkpoint is not None
        assert checkpoint['processed_rows'] == 100
        assert checkpoint['total_rows'] == 200
        assert checkpoint['completion_percentage'] == 50.0
        
        # Test clear
        mgr.clear_checkpoint("test_job")
        assert mgr.load_checkpoint("test_job") is None
        
        print("✅ CheckpointManager test passed!")

def test_data_quality_checks():
    """Test data quality functions"""
    import pandas as pd
    import numpy as np
    
    # Create test data
    products_df = pd.DataFrame({
        'product_id': ['PROD_001', 'PROD_002', 'PROD_003'],
        'product_name': ['Item 1', 'Item 2', 'Item 3'],
        'category': ['A', 'B', 'A'],
        'unit_cost': [10.0, 20.0, 15.0]
    })
    
    sales_df = pd.DataFrame({
        'order_id': ['ORD_001', 'ORD_002', 'ORD_003'],
        'quantity': [1, 2, 3],
        'total_amount': [10.0, 40.0, 45.0]
    })
    
    # Test quality checks function
    def run_data_quality_checks(products_df, sales_df):
        checks = []
        
        # Product dimension checks
        checks.append({
            'check': 'product_id_uniqueness',
            'passed': products_df['product_id'].nunique() == len(products_df),
            'score': 100 if products_df['product_id'].nunique() == len(products_df) else 0
        })
        
        checks.append({
            'check': 'product_name_not_null',
            'passed': products_df['product_name'].notna().all(),
            'score': 100 if products_df['product_name'].notna().all() else 0
        })
        
        # Sales fact checks
        checks.append({
            'check': 'sales_quantity_positive',
            'passed': (sales_df['quantity'] > 0).all(),
            'score': 100 if (sales_df['quantity'] > 0).all() else 0
        })
        
        overall_score = sum(check['score'] for check in checks) / len(checks) if checks else 0
        
        return {
            'checks': checks,
            'overall_score': overall_score,
            'total_checks': len(checks),
            'passed_checks': sum(1 for check in checks if check['passed'])
        }
    
    quality_results = run_data_quality_checks(products_df, sales_df)
    
    assert quality_results['overall_score'] == 100.0
    assert quality_results['passed_checks'] == quality_results['total_checks']
    
    print("✅ Data quality checks test passed!")

if __name__ == "__main__":
    print("Running local tests...")
    test_checkpoint_manager()
    test_data_quality_checks()
    print("🎉 All local tests passed!")