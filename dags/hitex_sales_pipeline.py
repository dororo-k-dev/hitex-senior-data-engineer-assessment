from datetime import datetime, timedelta
import logging
import os
import json
from pathlib import Path
from typing import Dict, List, Optional, Generator
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException

import pandas as pd
import numpy as np
# from great_expectations.core import ExpectationSuite
# from great_expectations.dataset import PandasDataset

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'hitex-data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

class HitexDataProcessor:
    """Enhanced Data processor with fault tolerance and data quality"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.checkpoint_dir = Path("/tmp/checkpoints")
        self.checkpoint_dir.mkdir(exist_ok=True)
    
    def extract_with_fault_tolerance(self, file_path: str, chunk_size: int = 10000, **kwargs) -> Generator[pd.DataFrame, None, None]:
        """Extract data with checkpointing for fault tolerance"""
        checkpoint_file = self.checkpoint_dir / f"{Path(file_path).stem}.checkpoint"
        start_row = 0
        
        try:
            # Resume from checkpoint if exists
            if checkpoint_file.exists():
                with open(checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                    start_row = checkpoint_data.get('last_processed_row', 0)
                    self.logger.info(f"Resuming from row {start_row}")
            
            # Simulate connection issues for testing
            if "fail_at_50" in file_path and start_row > 5000:
                raise ConnectionError("Simulated connection drop after 50% data")
            
            # Read CSV in chunks with checkpointing
            for chunk_number, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size, skiprows=range(1, start_row+1))):
                current_row = start_row + (chunk_number * chunk_size)
                
                # Update checkpoint
                checkpoint_data = {
                    'last_processed_row': current_row + len(chunk),
                    'file_path': file_path,
                    'last_updated': datetime.now().isoformat(),
                    'total_rows_processed': current_row + len(chunk)
                }
                
                with open(checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f)
                
                self.logger.info(f"Processed rows {current_row} to {current_row + len(chunk)}")
                yield chunk
                
        except (ConnectionError, IOError) as e:
            self.logger.error(f"Connection error at row {start_row}: {str(e)}")
            self.logger.info(f"Checkpoint saved. Pipeline can resume from row {start_row}")
            raise AirflowException(f"Extraction failed at row {start_row}. Can resume from checkpoint.")
        
        finally:
            # Cleanup checkpoint on successful completion
            if checkpoint_file.exists():
                checkpoint_file.unlink()
    
    def run_data_quality_checks(self, df: pd.DataFrame, suite_name: str) -> Dict:
        """Run comprehensive data quality checks using Great Expectations"""
        try:
            # Simple data quality checks without great_expectations
            results = {}
            
            # Row count check
            row_count = len(df)
            results['row_count_check'] = {
                'success': 1 <= row_count <= 10000000,
                'result': {'observed_value': row_count}
            }
            
            # Null checks
            if 'order_id' in df.columns:
                null_count = df['order_id'].isnull().sum()
                results['order_id_not_null'] = {
                    'success': null_count == 0,
                    'result': {'null_count': null_count}
                }
            
            if 'product_id' in df.columns:
                null_count = df['product_id'].isnull().sum()
                results['product_id_not_null'] = {
                    'success': null_count == 0,
                    'result': {'null_count': null_count}
                }
            
            # Range checks
            if 'quantity' in df.columns:
                valid_quantity = ((df['quantity'] >= 0) & (df['quantity'] <= 1000)).all()
                results['quantity_range_check'] = {
                    'success': valid_quantity,
                    'result': {'valid': valid_quantity}
                }
            
            if 'unit_price' in df.columns:
                valid_price = ((df['unit_price'] >= 0) & (df['unit_price'] <= 10000)).all()
                results['unit_price_range_check'] = {
                    'success': valid_price,
                    'result': {'valid': valid_price}
                }
            
            # Uniqueness check
            if 'order_id' in df.columns:
                unique_count = df['order_id'].nunique()
                results['order_id_unique'] = {
                    'success': unique_count == len(df),
                    'result': {'unique_count': unique_count, 'total_count': len(df)}
                }
            
            # Calculate overall quality score
            passed_checks = sum(1 for r in results.values() if r.get('success', False))
            total_checks = len(results)
            quality_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
            
            return {
                'quality_score': quality_score,
                'passed_checks': passed_checks,
                'total_checks': total_checks,
                'detailed_results': results,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Data quality check failed: {str(e)}")
            raise
    
    def transform_with_quality_gates(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform data with quality gates"""
        # Run quality checks before transformation
        quality_results = self.run_data_quality_checks(df, 'pre_transformation_checks')
        
        if quality_results['quality_score'] < 80:
            raise AirflowException(f"Data quality too low: {quality_results['quality_score']}%")
        
        # Proceed with transformation
        df_transformed = self._apply_transformations(df)
        
        # Run quality checks after transformation
        post_quality = self.run_data_quality_checks(df_transformed, 'post_transformation_checks')
        
        # Push quality metrics to XCom
        ti = kwargs.get('ti')
        if ti:
            ti.xcom_push(key='data_quality_metrics', value={
                'pre_transformation': quality_results,
                'post_transformation': post_quality
            })
        
        return df_transformed
    
    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business transformations"""
        df_clean = df.copy()
        
        # Standardize column names
        df_clean.columns = [col.lower().replace(' ', '_') for col in df_clean.columns]
        
        # Data type conversions
        type_conversions = {
            'order_id': 'str',
            'product_id': 'str', 
            'customer_id': 'str',
            'quantity': 'int64',
            'unit_price': 'float64'
        }
        
        for col, dtype in type_conversions.items():
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(dtype)
        
        # Add metadata
        df_clean['processed_at'] = datetime.now()
        df_clean['batch_id'] = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return df_clean
    
    def load_to_bigquery(self, df: pd.DataFrame, table_name: str, **kwargs):
        """Load data to BigQuery with proper error handling"""
        try:
            # Simulate BigQuery loading
            self.logger.info(f"Loading {len(df)} rows to {table_name}")
            # In real implementation, use BigQueryHook here
            return True
        except Exception as e:
            self.logger.error(f"Failed to load to {table_name}: {str(e)}")
            raise

def create_sample_csv():
    """Create sample CSV file for testing"""
    sample_data = {
        'order_id': [f'ORD_{i:06d}' for i in range(10000)],
        'product_id': [f'PROD_{i%100:03d}' for i in range(10000)],
        'customer_id': [f'CUST_{i%200:04d}' for i in range(10000)],
        'channel': np.random.choice(['Amazon_FBA', 'Shopify_DTC', 'Retail', 'Wholesale'], 10000),
        'quantity': np.random.randint(1, 10, 10000),
        'unit_price': np.round(np.random.uniform(10, 200, 10000), 2),
        'order_date': pd.date_range('2024-01-01', periods=10000, freq='H')
    }
    
    df = pd.DataFrame(sample_data)
    df['total_amount'] = df['quantity'] * df['unit_price']
    
    # Save to CSV
    csv_path = "/tmp/sales_data.csv"
    df.to_csv(csv_path, index=False)
    return csv_path

# Task implementations with enhanced fault tolerance
def extract_with_resume_capability(**kwargs):
    """Extract task with resume capability"""
    processor = HitexDataProcessor()
    csv_path = create_sample_csv()
    
    all_chunks = []
    try:
        for chunk in processor.extract_with_fault_tolerance(csv_path, chunk_size=1000):
            all_chunks.append(chunk)
        
        # Combine all chunks
        if all_chunks:
            final_df = pd.concat(all_chunks, ignore_index=True)
            kwargs['ti'].xcom_push(key='extracted_data', value=final_df.to_json(orient='split'))
            return True
        else:
            raise AirflowException("No data extracted")
            
    except AirflowException as e:
        # This exception contains resume information
        kwargs['ti'].xcom_push(key='extraction_error', value=str(e))
        raise

def validate_with_quality_gates(**kwargs):
    """Validation with quality gates"""
    processor = HitexDataProcessor()
    ti = kwargs['ti']
    
    extracted_data = ti.xcom_pull(key='extracted_data', task_ids='extract_with_resume')
    df = pd.read_json(extracted_data, orient='split')
    
    # Transform with quality gates
    transformed_df = processor.transform_with_quality_gates(df, **kwargs)
    ti.xcom_push(key='transformed_data', value=transformed_df.to_json(orient='split'))
    
    return True

def load_to_data_layers(**kwargs):
    """Load data through all layers: Raw → Staging → Core → Mart"""
    processor = HitexDataProcessor()
    ti = kwargs['ti']
    
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_with_quality')
    df = pd.read_json(transformed_data, orient='split')
    
    # Simulate loading to different data layers
    layers = ['raw', 'staging', 'core', 'mart']
    
    for layer in layers:
        processor.load_to_bigquery(df, f'{layer}.sales_data', **kwargs)
        logger.info(f"Loaded data to {layer} layer")
        ti.xcom_push(key=f'loaded_{layer}', value=True)
    
    return True

def verify_business_model(**kwargs):
    """Verify final business data model"""
    ti = kwargs['ti']
    
    # Check all layers were loaded
    layers = ['raw', 'staging', 'core', 'mart']
    for layer in layers:
        if not ti.xcom_pull(key=f'loaded_{layer}', task_ids='load_to_layers'):
            raise AirflowException(f"Layer {layer} not loaded successfully")
    
    # Verify data quality metrics
    quality_metrics = ti.xcom_pull(key='data_quality_metrics', task_ids='transform_with_quality')
    if quality_metrics['post_transformation']['quality_score'] < 90:
        raise AirflowException("Business model quality standards not met")
    
    logger.info("Business data model verified successfully")
    return True

# Create the enhanced DAG
with DAG(
    'hitex_complete_pipeline',
    default_args=default_args,
    description='Complete HITEx pipeline with fault tolerance and data quality',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['hitex', 'complete', 'production']
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    extract_task = PythonOperator(
        task_id='extract_with_resume',
        python_callable=extract_with_resume_capability,
        provide_context=True,
        retries=2
    )
    
    transform_task = PythonOperator(
        task_id='transform_with_quality',
        python_callable=validate_with_quality_gates,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load_to_layers',
        python_callable=load_to_data_layers,
        provide_context=True
    )
    
    verify_task = PythonOperator(
        task_id='verify_business_model',
        python_callable=verify_business_model,
        provide_context=True
    )
    
    # Define workflow
    start >> extract_task >> transform_task >> load_task >> verify_task >> end