from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator  # ✅

sys.path.append('/opt/airflow/scripts')

from extract import main as extract_main
from transform import main as transform_main
from load import main as load_main
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_etl',
    default_args=default_args,
    description='ETL pipeline para datos de e-commerce',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    create_tables = BashOperator(
        task_id='create_tables',
        bash_command='PGPASSWORD=airflow psql -h postgres -U airflow -d airflow -f /opt/airflow/sql/create_tables.sql'
    )
    
    extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_main,
    provide_context=True,  
    execution_timeout=timedelta(minutes=30),
    dag=dag  
    )
    
    transform_data = GlueJobOperator(
        task_id='transform_data_with_glue',
        job_name='Data Transformation ecommerce',  
        aws_conn_id='aws_connection',  
        iam_role_name='AWSGlueServiceRole-data-engineering',
        s3_bucket='ecommerce-data-raw-dataengineer', 
        script_location='s3://ecommerce-data-raw-dataengineer/scripts/data_transformation_ecommerce.py',
        create_job_kwargs={"GlueVersion":"3.0","NumberOfWorkers":2,"WorkerType":"G.1X"}
    )
    
    load_data = GlueJobOperator(
        task_id='load_data_with_glue',
        job_name='Load Data Ecommerce',
        aws_conn_id='aws_connection',
        iam_role_name='AWSGlueServiceRole-data-engineering',
        s3_bucket='ecommerce-data-raw-dataengineer',
        script_location='s3://ecommerce-data-raw-dataengineer/scripts/load_data_ecommerce.py',
        create_job_kwargs={"GlueVersion":"3.0","NumberOfWorkers":2,"WorkerType":"G.1X"}
    )
    create_tables 