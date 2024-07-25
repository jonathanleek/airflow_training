from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
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
# Example DAG with Dynamic Task Mapping Using .partial and .expand (without TaskFlow API)

This example DAG demonstrates dynamic task mapping in Airflow using .partial and .expand, without using the TaskFlow API.
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
dag = DAG(
    'example_dag_with_dynamic_task_mapping_partial_expand_no_taskflow',
    default_args=default_args,
    description='An example DAG with dynamic task mapping using .partial and .expand without using the TaskFlow API',
    schedule_interval=timedelta(days=1),
    doc_md=dag_doc_md,
)

# Start Task
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Generate Tasks
def generate_tasks(**kwargs):
    return sample_data

generate_tasks_task = PythonOperator(
    task_id='generate_tasks',
    python_callable=generate_tasks,
    dag=dag,
)

# Process Task
def process_task(task_data, **kwargs):
    print(f"Processing {task_data['name']} with value {task_data['value']}")

# Dynamically create process tasks using .partial and .expand
process_task_partial = PythonOperator.partial(
    task_id='process_task',
    python_callable=process_task,
    dag=dag,
)

process_tasks = process_task_partial.expand(op_args=[{"task_data": data} for data in sample_data])

# End Task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> generate_tasks_task >> process_tasks >> end_task