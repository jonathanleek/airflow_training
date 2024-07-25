from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG documentation
dag_doc_md = """
# Example DAG with Dynamic Task Mapping

This example DAG demonstrates dynamic task mapping in Airflow.
Tasks are dynamically generated based on input data.

## DAG Flow
1. **Start**: Begins the DAG execution.
2. **Generate Tasks**: Dynamically generates tasks based on input data.
3. **Process Data**: Processes the dynamically generated tasks.
4. **End**: Marks the end of the DAG execution.
"""

# Sample data to demonstrate dynamic task mapping
sample_data = [
    {"name": "task_1", "value": 10},
    {"name": "task_2", "value": 20},
    {"name": "task_3", "value": 30},
]

# Instantiate the DAG
with DAG(
    'example_dag_with_dynamic_task_mapping',
    default_args=default_args,
    description='An example DAG with dynamic task mapping',
    schedule_interval=timedelta(days=1),
    doc_md=dag_doc_md,
) as dag:

    @task
    def start():
        print("Starting DAG execution...")

    @task
    def generate_tasks(data):
        return data

    @task
    def process_task(task_data):
        print(f"Processing {task_data['name']} with value {task_data['value']}")

    @task
    def end():
        print("Ending DAG execution...")

    # Define task dependencies
    start_task = start()
    generated_tasks = generate_tasks(sample_data)
    process_tasks = process_task.expand(task_data=generated_tasks)
    end_task = end()

    start_task >> generated_tasks >> process_tasks >> end_task