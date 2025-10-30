from datetime import datetime, timedelta
import logging
import pandas as pd
import json
import tempfile
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import Variable
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'hitex-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

class SecureCheckpointManager:
    """Secure checkpoint manager with proper path validation"""
    
    def __init__(self):
        checkpoint_dir = Variable.get('checkpoint_dir', '/tmp/hitex_checkpoints')
        self.checkpoint_dir = Path(checkpoint_dir).resolve()
        self.checkpoint_dir.mkdir(exist_ok=True, mode=0o700)
    
    def save_checkpoint(self, job_id: str, processed_rows: int, total_rows: int):
        # Sanitize job_id to prevent path traversal
        safe_job_id = "".join(c for c in job_id if c.isalnum() or c in ('_', '-'))
        checkpoint_file = self.checkpoint_dir / f"{safe_job_id}.json"
        
        checkpoint_data = {
            'processed_rows': processed_rows,
            'total_rows': total_rows,
            'timestamp': datetime.now().isoformat(),
            'completion_percentage': (processed_rows / total_rows) * 100 if total_rows > 0 else 0
        }
        
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f)
            logger.info(f"Checkpoint saved: {processed_rows}/{total_rows} rows")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {str(e)}")
            raise
    
    def load_checkpoint(self, job_id: str):
        safe_job_id = "".join(c for c in job_id if c.isalnum() or c in ('_', '-'))
        checkpoint_file = self.checkpoint_dir / f"{safe_job_id}.json"
        
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {str(e)}")
        return None
    
    def clear_checkpoint(self, job_id: str):
        safe_job_id = "".join(c for c in job_id if c.isalnum() or c in ('_', '-'))
        checkpoint_file = self.checkpoint_dir / f"{safe_job_id}.json"
        
        if checkpoint_file.exists():
            try:
                checkpoint_file.unlink()
            except Exception as e:
                logger.error(f"Failed to clear checkpoint: {str(e)}")

# Alias for backward compatibility with tests
CheckpointManager = SecureCheckpointManager

def create_sample_data(**kwargs):
    """Create sample CSV data in secure temporary location"""
    import numpy as np
    
    try:
        # Create secure temporary directory
        temp_dir = tempfile.mkdtemp(prefix='hitex_secure_')
        os.chmod(temp_dir, 0o700)
        
        # Product dimension data
        products_data = {
            'product_id': [f'PROD_{i:05d}' for i in range(1, 101)],
            'product_name': [f'Sports Item {i}' for i in range(1, 101)],
            'category': np.random.choice(['Apparel', 'Equipment', 'Accessories'], 100),
            'brand': np.random.choice(['HITEx Pro', 'HITEx Elite', 'HITEx Basic'], 100),
            'unit_cost': np.round(np.random.uniform(10, 100, 100), 2),
            'created_date': '2024-01-01',
            'is_active': True
        }
        
        # Sales fact data
        sales_data = {
            'order_id': [f'ORD_{i:08d}' for i in range(1, 10001)],
            'product_id': [f'PROD_{np.random.randint(1, 101):05d}' for _ in range(10000)],
            'customer_id': [f'CUST_{i%500:05d}' for i in range(10000)],
            'channel': np.random.choice(['Amazon_FBA', 'Shopify_DTC', 'Retail', 'Wholesale'], 10000),
            'quantity': np.random.randint(1, 10, 10000),
            'unit_price': np.round(np.random.uniform(15, 150, 10000), 2),
            'order_date': pd.date_range('2024-01-01', periods=10000, freq='H'),
            'region': np.random.choice(['North', 'South', 'East', 'West'], 10000)
        }
        
        # Create DataFrames and save to secure location
        products_df = pd.DataFrame(products_data)
        sales_df = pd.DataFrame(sales_data)
        sales_df['total_amount'] = sales_df['quantity'] * sales_df['unit_price']
        
        products_file = os.path.join(temp_dir, 'products.csv')
        sales_file = os.path.join(temp_dir, 'sales.csv')
        
        products_df.to_csv(products_file, index=False)
        sales_df.to_csv(sales_file, index=False)
        
        logger.info(f"Created sample data: {len(products_df)} products, {len(sales_df)} sales")
        return {'products_file': products_file, 'sales_file': sales_file}
        
    except Exception as e:
        logger.error(f"Failed to create sample data: {str(e)}")
        raise AirflowException(f"Sample data creation failed: {str(e)}")

def extract_with_fault_tolerance(**kwargs):
    """Extract data with secure checkpoint-based fault tolerance"""
    try:
        checkpoint_mgr = SecureCheckpointManager()
        ti = kwargs['ti']
        
        # Get file paths from previous task
        file_paths = ti.xcom_pull(task_ids='create_sample_data')
        if not file_paths:
            raise AirflowException("No file paths received from previous task")
        
        extracted_data = {}
        
        for file_type, file_path in file_paths.items():
            # Validate file path
            if not os.path.exists(file_path):
                raise AirflowException(f"File not found: {file_path}")
            
            job_id = f"extract_{file_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Check for existing checkpoint
            checkpoint = checkpoint_mgr.load_checkpoint(job_id)
            start_row = checkpoint['processed_rows'] if checkpoint else 0
            
            try:
                # Read CSV with chunking for fault tolerance
                chunk_size = 1000
                all_chunks = []
                total_rows = sum(1 for _ in open(file_path)) - 1  # Exclude header
                
                for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size, skiprows=range(1, start_row+1))):
                    current_row = start_row + (chunk_num * chunk_size)
                    
                    all_chunks.append(chunk)
                    checkpoint_mgr.save_checkpoint(job_id, current_row + len(chunk), total_rows)
                
                # Combine chunks
                if all_chunks:
                    final_df = pd.concat(all_chunks, ignore_index=True)
                    extracted_data[file_type] = final_df.to_json(orient='split')
                    checkpoint_mgr.clear_checkpoint(job_id)
                else:
                    raise AirflowException(f"No data extracted from {file_path}")
                    
            except Exception as e:
                logger.error(f"Extraction failed for {file_type}: {str(e)}")
                raise AirflowException(f"Extraction failed for {file_type}. Can resume from checkpoint.")
        
        ti.xcom_push(key='extracted_data', value=extracted_data)
        return True
        
    except Exception as e:
        logger.error(f"Extract task failed: {str(e)}")
        raise

def transform_to_dimensional_model(**kwargs):
    """Transform data to dimensional model with enhanced validation"""
    try:
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(key='extracted_data', task_ids='extract_with_fault_tolerance')
        
        if not extracted_data:
            raise AirflowException("No extracted data received")
        
        # Process products (SCD-2 dimension)
        products_df = pd.read_json(extracted_data['products_file'], orient='split')
        products_df['effective_date'] = datetime.now()
        products_df['end_date'] = pd.to_datetime('2099-12-31')
        products_df['is_current'] = True
        products_df['record_hash'] = products_df.apply(
            lambda x: hash(str(x[['product_name', 'category', 'brand', 'unit_cost']].values)), axis=1
        )
        
        # Process sales (fact table)
        sales_df = pd.read_json(extracted_data['sales_file'], orient='split')
        sales_df['load_date'] = datetime.now()
        sales_df['batch_id'] = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Data quality checks
        quality_results = run_data_quality_checks(products_df, sales_df)
        
        if quality_results['overall_score'] < 85:
            raise AirflowException(f"Data quality below threshold: {quality_results['overall_score']}%")
        
        transformed_data = {
            'dim_product': products_df.to_json(orient='split'),
            'fct_sales': sales_df.to_json(orient='split'),
            'quality_metrics': quality_results
        }
        
        ti.xcom_push(key='transformed_data', value=transformed_data)
        return True
        
    except Exception as e:
        logger.error(f"Transform task failed: {str(e)}")
        raise

def run_data_quality_checks(products_df, sales_df):
    """Run comprehensive data quality checks with error handling"""
    checks = []
    
    try:
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
        
        checks.append({
            'check': 'sales_amount_positive',
            'passed': (sales_df['total_amount'] > 0).all(),
            'score': 100 if (sales_df['total_amount'] > 0).all() else 0
        })
        
        checks.append({
            'check': 'order_id_not_null',
            'passed': sales_df['order_id'].notna().all(),
            'score': 100 if sales_df['order_id'].notna().all() else 0
        })
        
        overall_score = sum(check['score'] for check in checks) / len(checks) if checks else 0
        
        return {
            'checks': checks,
            'overall_score': overall_score,
            'total_checks': len(checks),
            'passed_checks': sum(1 for check in checks if check['passed'])
        }
        
    except Exception as e:
        logger.error(f"Data quality checks failed: {str(e)}")
        return {
            'checks': [],
            'overall_score': 0,
            'total_checks': 0,
            'passed_checks': 0,
            'error': str(e)
        }

def load_to_bigquery(**kwargs):
    """Load data to BigQuery with secure configuration"""
    try:
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_to_dimensional_model')
        
        if not transformed_data:
            raise AirflowException("No transformed data received")
        
        # Initialize BigQuery hook with proper connection
        bq_hook = BigQueryHook(
            gcp_conn_id='google_cloud_default',
            use_legacy_sql=False
        )
        
        # Get project ID from Airflow Variables (secure)
        project_id = Variable.get('gcp_project_id')
        dataset_id = Variable.get('bigquery_dataset_curated', 'curated')
        
        # Load dimension table (SCD-2)
        dim_product_df = pd.read_json(transformed_data['dim_product'], orient='split')
        
        # Load fact table
        fct_sales_df = pd.read_json(transformed_data['fct_sales'], orient='split')
        
        # Use proper BigQuery loading method
        bq_hook.insert_all(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id='dim_product',
            rows=dim_product_df.to_dict('records')
        )
        
        bq_hook.insert_all(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id='fct_sales',
            rows=fct_sales_df.to_dict('records')
        )
        
        logger.info(f"Loaded {len(dim_product_df)} products and {len(fct_sales_df)} sales records")
        
        # Push quality metrics for monitoring
        ti.xcom_push(key='load_summary', value={
            'dim_product_rows': len(dim_product_df),
            'fct_sales_rows': len(fct_sales_df),
            'quality_score': transformed_data['quality_metrics']['overall_score'],
            'load_timestamp': datetime.now().isoformat()
        })
        
        return True
        
    except Exception as e:
        logger.error(f"Load task failed: {str(e)}")
        raise

def validate_data_warehouse(**kwargs):
    """Final validation of data warehouse with comprehensive checks"""
    try:
        ti = kwargs['ti']
        load_summary = ti.xcom_pull(key='load_summary', task_ids='load_to_bigquery')
        
        if not load_summary:
            raise AirflowException("No load summary received")
        
        # Validate row counts
        if load_summary.get('dim_product_rows', 0) == 0:
            raise AirflowException("No product data loaded to warehouse")
            
        if load_summary.get('fct_sales_rows', 0) == 0:
            raise AirflowException("No sales data loaded to warehouse")
        
        # Validate quality score
        quality_score = load_summary.get('quality_score', 0)
        if quality_score < 85:
            raise AirflowException(f"Data quality below acceptable threshold: {quality_score}%")
        
        logger.info(f"Data warehouse validation passed: {load_summary}")
        return True
        
    except Exception as e:
        logger.error(f"Validation task failed: {str(e)}")
        raise

# Create the secure DAG
with DAG(
    'hitex_csv_to_dw',
    default_args=default_args,
    description='HITEx Secure CSV to Data Warehouse Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['hitex', 'csv', 'datawarehouse', 'production', 'secure']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    create_data = PythonOperator(
        task_id='create_sample_data',
        python_callable=create_sample_data
    )
    
    extract = PythonOperator(
        task_id='extract_with_fault_tolerance',
        python_callable=extract_with_fault_tolerance,
        retries=2
    )
    
    transform = PythonOperator(
        task_id='transform_to_dimensional_model',
        python_callable=transform_to_dimensional_model
    )
    
    load = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery
    )
    
    validate = PythonOperator(
        task_id='validate_data_warehouse',
        python_callable=validate_data_warehouse
    )
    
    end = EmptyOperator(task_id='end')
    
    # Define workflow
    start >> create_data >> extract >> transform >> load >> validate >> end