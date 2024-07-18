from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# Define the dataset representing the CSV file in the S3 bucket
input_dataset = Dataset(uri='s3://my-bucket/input-data/my_file.csv')

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG documentation
dag_doc_md = """
# Advanced Example DAG with Data-Aware Scheduling

This example DAG demonstrates the following concepts:
1. **Datasets and Data-Aware Scheduling**: Using datasets to trigger tasks based on data availability.
2. **Branching**: Using branching to control the DAG flow based on conditions.
3. **Task Grouping**: Organizing tasks into logical groups.
4. **Trigger Rules**: Managing task execution based on the state of upstream tasks.

## DAG Flow
1. **Start**: Begins the DAG execution.
2. **S3 Key Sensor**: Waits for the CSV file to appear in the S3 bucket.
3. **Data Preparation Group**: Simulates data preparation tasks.
4. **Branching**: Decides the next path based on a condition.
5. **Processing Group**: Executes different tasks based on the branching condition.
6. **End**: Marks the end of the DAG execution.
"""

# Instantiate the DAG
with DAG(
        'data_aware_scheduling_example_dag',
        default_args=default_args,
        description='An advanced example DAG with data-aware scheduling using S3 dataset',
        schedule=[input_dataset],  # Schedule the DAG based on the input dataset
        doc_md=dag_doc_md,
) as dag:
    # Function to simulate data preparation
    def prepare_data():
        print("Preparing data...")


    # Function to decide the branching path
    def branch_condition():
        # Simulating a condition
        condition = True
        if condition:
            return 'process_data_task_group.group_1.process_data_1_task'
        else:
            return 'process_data_task_group.group_2.process_data_3_task'


    # Function to simulate data processing
    def process_data(task_name):
        print(f"Processing data in {task_name}...")


    # Start Dummy Task
    start = DummyOperator(
        task_id='start',
    )

    # S3 Key Sensor to wait for the CSV file
    s3_sensor = S3KeySensor(
        task_id='s3_key_sensor',
        bucket_key='input-data/my_file.csv',
        bucket_name='my-bucket',
        aws_conn_id='aws_default',
        poke_interval=30,
        timeout=600,
    )

    # Task Group for data preparation
    with TaskGroup("data_preparation_group") as data_preparation_group:
        prepare_data_task = PythonOperator(
            task_id='prepare_data_task',
            python_callable=prepare_data,
            outlets=[input_dataset],  # Output dataset to trigger downstream tasks
        )

    # Branching Task
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_condition,
    )

    # Task Group for data processing with branching
    with TaskGroup("process_data_task_group") as process_data_task_group:
        with TaskGroup("group_1") as group_1:
            process_data_1_task = PythonOperator(
                task_id='process_data_1_task',
                python_callable=lambda: process_data("Task Group 1 - Task 1"),
            )
            process_data_2_task = PythonOperator(
                task_id='process_data_2_task',
                python_callable=lambda: process_data("Task Group 1 - Task 2"),
            )

        group_1_tasks = [process_data_1_task, process_data_2_task]

        with TaskGroup("group_2") as group_2:
            process_data_3_task = PythonOperator(
                task_id='process_data_3_task',
                python_callable=lambda: process_data("Task Group 2 - Task 1"),
            )
            process_data_4_task = PythonOperator(
                task_id='process_data_4_task',
                python_callable=lambda: process_data("Task Group 2 - Task 2"),
            )

        group_2_tasks = [process_data_3_task, process_data_4_task]

    # Dummy Task for successful completion
    end = DummyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # Define the task dependencies
    start >> s3_sensor >> data_preparation_group >> branch_task
    branch_task >> group_1_tasks >> end
    branch_task >> group_2_tasks >> end