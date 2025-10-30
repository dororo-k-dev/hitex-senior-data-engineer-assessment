import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json
import tempfile
import os

from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from airflow.utils.dates import days_ago

from dags.hitex_csv_to_dw import (
    create_sample_data,
    extract_with_fault_tolerance,
    transform_to_dimensional_model,
    load_to_bigquery,
    validate_data_warehouse,
    CheckpointManager,
    run_data_quality_checks
)

class TestHitexCsvToDw:
    """Test suite for HITEx CSV to Data Warehouse pipeline"""
    
    @pytest.fixture
    def dag_bag(self):
        return DagBag(dag_folder='dags/', include_examples=False)
    
    @pytest.fixture
    def sample_context(self):
        return {
            'ti': Mock(),
            'ds': '2024-01-01',
            'execution_date': datetime(2024, 1, 1),
            'params': {}
        }
    
    def test_dag_loaded_successfully(self, dag_bag):
        """Test that the DAG loads without errors"""
        dag = dag_bag.get_dag('hitex_csv_to_dw')
        assert dag is not None
        assert len(dag.tasks) == 7
        assert dag_bag.import_errors == {}
    
    def test_dag_structure(self, dag_bag):
        """Test DAG structure and dependencies"""
        dag = dag_bag.get_dag('hitex_csv_to_dw')
        
        expected_tasks = [
            'start', 'create_sample_data', 'extract_with_fault_tolerance',
            'transform_to_dimensional_model', 'load_to_bigquery',
            'validate_data_warehouse', 'end'
        ]
        
        actual_tasks = [task.task_id for task in dag.tasks]
        assert set(expected_tasks) == set(actual_tasks)
        
        # Test dependencies
        extract_task = dag.get_task('extract_with_fault_tolerance')
        assert 'create_sample_data' in [t.task_id for t in extract_task.upstream_list]
    
    def test_create_sample_data(self, sample_context):
        """Test sample data creation"""
        try:
            result = create_sample_data(**sample_context)
        except Exception as e:
            pytest.fail(f"Sample data creation failed: {str(e)}")
        
        assert 'products_file' in result
        assert 'sales_file' in result
        assert os.path.exists(result['products_file'])
        assert os.path.exists(result['sales_file'])
        
        # Verify data quality
        products_df = pd.read_csv(result['products_file'])
        sales_df = pd.read_csv(result['sales_file'])
        
        if result and 'products_file' in result:
            assert len(products_df) == 100
            assert len(sales_df) == 10000
            assert products_df['product_id'].nunique() == 100
            assert sales_df['quantity'].min() >= 1
        else:
            pytest.fail("Invalid result from create_sample_data")
    
    def test_checkpoint_manager(self):
        """Test checkpoint functionality for fault tolerance"""
        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint_mgr = CheckpointManager(temp_dir)
            
            # Test save and load
            job_id = "test_job_123"
            checkpoint_mgr.save_checkpoint(job_id, 5000, 10000)
            
            loaded_checkpoint = checkpoint_mgr.load_checkpoint(job_id)
            assert loaded_checkpoint['processed_rows'] == 5000
            assert loaded_checkpoint['total_rows'] == 10000
            assert loaded_checkpoint['completion_percentage'] == 50.0
            
            # Test clear
            checkpoint_mgr.clear_checkpoint(job_id)
            assert checkpoint_mgr.load_checkpoint(job_id) is None
    
    @patch('dags.hitex_csv_to_dw.pd.read_csv')
    def test_extract_with_fault_tolerance_success(self, mock_read_csv, sample_context):
        """Test successful extraction with fault tolerance"""
        # Mock data
        mock_chunk1 = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        mock_chunk2 = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        mock_read_csv.return_value = [mock_chunk1, mock_chunk2]
        
        # Mock TI xcom
        sample_context['ti'].xcom_pull.return_value = {
            'products_file': '/tmp/products.csv',
            'sales_file': '/tmp/sales.csv'
        }
        
        result = extract_with_fault_tolerance(**sample_context)
        assert result is True
        sample_context['ti'].xcom_push.assert_called()
    
    def test_data_quality_checks(self):
        """Test comprehensive data quality checks"""
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
        
        quality_results = run_data_quality_checks(products_df, sales_df)
        
        assert quality_results['overall_score'] == 100.0
        assert quality_results['passed_checks'] == quality_results['total_checks']
        assert len(quality_results['checks']) == 5
    
    def test_data_quality_checks_with_failures(self):
        """Test data quality checks with data issues"""
        # Create problematic data
        products_df = pd.DataFrame({
            'product_id': ['PROD_001', 'PROD_001', 'PROD_003'],  # Duplicate IDs
            'product_name': ['Item 1', None, 'Item 3'],  # Null name
            'category': ['A', 'B', 'A'],
            'unit_cost': [10.0, 20.0, 15.0]
        })
        
        sales_df = pd.DataFrame({
            'order_id': ['ORD_001', 'ORD_002', 'ORD_003'],
            'quantity': [1, -2, 3],  # Negative quantity
            'total_amount': [10.0, 40.0, -45.0]  # Negative amount
        })
        
        quality_results = run_data_quality_checks(products_df, sales_df)
        
        assert quality_results['overall_score'] < 100.0
        assert quality_results['passed_checks'] < quality_results['total_checks']
    
    @patch('dags.hitex_csv_to_dw.BigQueryHook')
    def test_load_to_bigquery(self, mock_bq_hook, sample_context):
        """Test BigQuery loading functionality"""
        # Mock transformed data
        sample_context['ti'].xcom_pull.return_value = {
            'dim_product': pd.DataFrame({'product_id': ['P1'], 'name': ['Product 1']}).to_json(orient='split'),
            'fct_sales': pd.DataFrame({'order_id': ['O1'], 'amount': [100]}).to_json(orient='split'),
            'quality_metrics': {'overall_score': 95.0}
        }
        
        # Mock BigQuery hook
        mock_hook_instance = Mock()
        mock_bq_hook.return_value = mock_hook_instance
        
        with patch('dags.hitex_csv_to_dw.Variable.get', return_value='test-project'):
            result = load_to_bigquery(**sample_context)
        
        assert result is True
        assert mock_hook_instance.insert_all.call_count == 2  # dim + fact tables
        sample_context['ti'].xcom_push.assert_called()
    
    def test_validate_data_warehouse_success(self, sample_context):
        """Test successful data warehouse validation"""
        sample_context['ti'].xcom_pull.return_value = {
            'dim_product_rows': 100,
            'fct_sales_rows': 10000,
            'quality_score': 95.0,
            'load_timestamp': datetime.now().isoformat()
        }
        
        result = validate_data_warehouse(**sample_context)
        assert result is True
    
    def test_validate_data_warehouse_failure(self, sample_context):
        """Test data warehouse validation failure scenarios"""
        # Test with no data loaded
        sample_context['ti'].xcom_pull.return_value = {
            'dim_product_rows': 0,
            'fct_sales_rows': 10000,
            'quality_score': 95.0
        }
        
        with pytest.raises(Exception):
            validate_data_warehouse(**sample_context)
        
        # Test with low quality score
        sample_context['ti'].xcom_pull.return_value = {
            'dim_product_rows': 100,
            'fct_sales_rows': 10000,
            'quality_score': 70.0
        }
        
        with pytest.raises(Exception):
            validate_data_warehouse(**sample_context)
    
    @patch('dags.hitex_csv_to_dw.pd.read_csv')
    def test_fault_tolerance_with_simulated_failure(self, mock_read_csv, sample_context):
        """Test fault tolerance with simulated connection failure"""
        # Setup for failure simulation
        sample_context['params'] = {'SIMULATE_FAILURE': True}
        sample_context['ti'].xcom_pull.return_value = {
            'products_file': '/tmp/products_fail_at_50.csv',
            'sales_file': '/tmp/sales.csv'
        }
        
        # Mock chunks that will trigger failure
        chunks = [pd.DataFrame({'col1': [i], 'col2': [f'val_{i}']}) for i in range(10)]
        mock_read_csv.return_value = chunks
        
        with pytest.raises(Exception) as exc_info:
            extract_with_fault_tolerance(**sample_context)
        
        assert "Extraction failed" in str(exc_info.value)
    
    def test_transform_to_dimensional_model(self, sample_context):
        """Test transformation to dimensional model"""
        # Mock extracted data
        products_data = pd.DataFrame({
            'product_id': ['PROD_001', 'PROD_002'],
            'product_name': ['Item 1', 'Item 2'],
            'category': ['A', 'B'],
            'brand': ['Brand1', 'Brand2'],
            'unit_cost': [10.0, 20.0]
        })
        
        sales_data = pd.DataFrame({
            'order_id': ['ORD_001', 'ORD_002'],
            'product_id': ['PROD_001', 'PROD_002'],
            'customer_id': ['CUST_001', 'CUST_002'],
            'quantity': [1, 2],
            'unit_price': [10.0, 20.0],
            'total_amount': [10.0, 40.0],
            'channel': ['Amazon_FBA', 'Shopify_DTC'],
            'region': ['North', 'South'],
            'order_date': ['2024-01-01', '2024-01-02']
        })
        
        sample_context['ti'].xcom_pull.return_value = {
            'products_file': products_data.to_json(orient='split'),
            'sales_file': sales_data.to_json(orient='split')
        }
        
        result = transform_to_dimensional_model(**sample_context)
        assert result is True
        
        # Verify xcom push was called with transformed data
        sample_context['ti'].xcom_push.assert_called()
        call_args = sample_context['ti'].xcom_push.call_args
        assert call_args[1]['key'] == 'transformed_data'
        
        transformed_data = call_args[1]['value']
        assert 'dim_product' in transformed_data
        assert 'fct_sales' in transformed_data
        assert 'quality_metrics' in transformed_data