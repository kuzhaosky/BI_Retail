# dags/etl_to_dwh.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'email': ['stephane.kpoviessi@student.junia.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_to_dwh',
    default_args=default_args,
    description='ETL process to load data into Data Warehouse using Spark',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command='python /opt/airflow/scripts/etl_to_dwh.py',
        dag=dag
    )

    run_etl
