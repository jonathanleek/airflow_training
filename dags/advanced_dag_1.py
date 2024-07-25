from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.baseoperator import cross_downstream, chain
from airflow.utils.trigger_rule import TriggerRule
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
# Example DAG with Cross Downstream, Chain, ExternalTaskSensor, and Deferrable Operators

This example DAG demonstrates the following concepts:
1. **cross_downstream()**: Connects multiple upstream tasks to multiple downstream tasks.
2. **chain()**: Connects multiple tasks in a sequential chain.
3. **ExternalTaskSensor**: Waits for a task in a different DAG to complete.
4. **Deferrable Operators**: Uses deferrable operators to perform efficient waiting.

## DAG Flow
1. **Start**: Begins the DAG execution.
2. **External Task Sensor**: Waits for a task in another DAG to complete.
3. **Data Preparation and Processing**: Simulates data preparation and processing tasks.
4. **End**: Marks the end of the DAG execution.
"""

# Instantiate the DAG
with DAG(
    'example_dag_with_cross_stream_chain_and_external_tasks',
    default_args=default_args,
    description='An example DAG with cross_downstream, chain, ExternalTaskSensor, and deferrable operators',
    schedule_interval=timedelta(days=1),
    doc_md=dag_doc_md,
) as dag:

    # Start Dummy Task
    start = DummyOperator(
        task_id='start',
    )

    # Additional tasks to demonstrate cross_downstream
    start_task_1 = DummyOperator(
        task_id='start_task_1',
    )

    start_task_2 = DummyOperator(
        task_id='start_task_2',
    )

    start_task_3 = DummyOperator(
        task_id='start_task_3',
    )

    # External Task Sensor
    external_task_sensor = ExternalTaskSensor(
        task_id='external_task_sensor',
        external_dag_id='another_dag',
        external_task_id='external_task_to_wait_for',
        mode='reschedule',
        poke_interval=30,
        timeout=600,
    )

    # Function to simulate data preparation
    def prepare_data():
        print("Preparing data...")

    # Function to simulate data validation
    def validate_data():
        print("Validating data...")

    # Function to simulate data processing
    def process_data():
        print("Processing data...")

    # Function to simulate data storage
    def store_data():
        print("Storing data...")

    # Tasks to prepare, validate, process, and store data
    prepare_data_task = PythonOperator(
        task_id='prepare_data_task',
        python_callable=prepare_data,
    )

    validate_data_task = PythonOperator(
        task_id='validate_data_task',
        python_callable=validate_data,
    )

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
    )

    store_data_task = PythonOperator(
        task_id='store_data_task',
        python_callable=store_data,
    )

    # Dummy Task for successful completion
    end = DummyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # Example of using cross_downstream to connect multiple upstream tasks to multiple downstream tasks
    cross_downstream(
        [start, start_task_1, start_task_2, start_task_3],
        [external_task_sensor, prepare_data_task, validate_data_task]
    )

    # Example of using chain to connect multiple tasks in sequence
    chain(prepare_data_task, validate_data_task, process_data_task, store_data_task, end)

    # Define the task dependencies
    external_task_sensor >> prepare_data_task