import pytest
import pandas as pd
from dags.hitex_sales_pipeline import HitexDataProcessor

class TestHitexPipeline:
    
    def test_pipeline_integration(self):
        """Test end-to-end pipeline integration"""
        processor = HitexDataProcessor()
        
        # Test data extraction
        sample_data = processor._create_sample_data()
        assert len(sample_data) > 0
        
        # Test transformation
        transformed = processor._apply_transformations(sample_data)
        assert 'processed_at' in transformed.columns
        
        # Test quality checks
        quality_results = processor.run_data_quality_checks(transformed, 'test')
        assert quality_results['quality_score'] >= 0

    def test_error_handling(self):
        """Test pipeline error handling"""
        processor = HitexDataProcessor()
        
        # Test with empty data
        empty_df = pd.DataFrame()
        with pytest.raises(Exception):
            processor._apply_transformations(empty_df)