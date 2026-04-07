from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import subprocess

default_args = {
    'owner': 'vidhesha',
    'start_date': datetime(2026, 2, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True
}

with DAG(
    dag_id='banking_pipeline_pyspark',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:


    def load_data():
        from load_raw_data import generate_data
        generate_data()

    load_raw = PythonOperator(
        task_id='load_raw',
        python_callable=load_data
    )


    process_data = PythonOperator(
        task_id='process_data_pyspark',
        python_callable=lambda: subprocess.run(["python", "pyspark_job.py"], check=True)
    )


    validate = PythonOperator(
        task_id='validate_and_log',
        python_callable=lambda: subprocess.run(["python", "validate_and_log.py"], check=True)
    )


    mark_processed = PythonOperator(
        task_id='mark_processed',
        python_callable=lambda: subprocess.run(["python", "mark_processed.py"], check=True)
    )


    gx = PythonOperator(
        task_id='gx_checkpoint',
        python_callable=lambda: subprocess.run(
            ["great_expectations", "checkpoint", "run", "clean_transactions_checkpoint"],
            check=True
        )
    )

    load_raw >> process_data >> validate >> mark_processed >> gx
