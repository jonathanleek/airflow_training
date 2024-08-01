import json
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Load dataset configurations
with open('include/datasets.json') as f:
    datasets = json.load(f)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 7, 30)  # Replace with an appropriate start date
}

def create_dag(dag_id, schedule, default_args, tasks):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)

    with dag:
        for task in tasks:
            task_id = task['task_id']
            script = task['script']

            def run_task(script, **kwargs):
                os.system(f'python /include/scripts/{script}')

            PythonOperator(
                task_id=task_id,
                python_callable=run_task,
                op_kwargs={'script': script},
                dag=dag
            )
    return dag


# Generate a DAG for each dataset configuration
for dataset in datasets:
    dag_id = f"process_{dataset['dataset_name']}"
    schedule = dataset['schedule_interval']
    tasks = dataset['tasks']

    globals()[dag_id] = create_dag(dag_id, schedule, default_args, tasks)