"""
ERP System Pipeline - Daily data sync
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'hitex-data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def extract_erp_data():
    """Extract data from ERP system"""
    # Implementation for ERP API
    return "ERP data extracted"

with DAG(
    'hitex_erp_pipeline',
    default_args=default_args,
    description='ERP system data pipeline - Daily sync',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['erp', 'daily', 'sync']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_erp_data',
        python_callable=extract_erp_data
    )
