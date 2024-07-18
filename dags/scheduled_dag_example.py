# DAG documentation
dag_doc_md = """
# Example DAG

This is a comprehensive example DAG demonstrating the following concepts:
1. **Retrieving a connection**: Using `BaseHook.get_connection` to fetch connection details.
2. **Retrieving an Airflow Variable**: Using `Variable.get` to fetch the value of an Airflow Variable.
3. **XComs**: Using XComs to pass information between tasks.
4. **Sensors**: Using a `FileSensor` to wait for a file to appear in a specified directory.

## DAG Flow
1. **Start**: Begins the DAG execution.
2. **Retrieve Connection Info**: Fetches and logs connection details, then pushes them to XCom.
3. **Retrieve Airflow Variable**: Fetches and logs the value of an Airflow Variable, then pushes it to XCom.
4. **Use XComs**: Pulls and logs values from XCom.
5. **File Sensor**: Waits for a specific file to appear before proceeding.
6. **End**: Marks the end of the DAG execution.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable, XCom
from datetime import datetime, timedelta

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

# Instantiate the DAG
# https://www.astronomer.io/docs/learn/airflow-dag-parameters
dag = DAG(
    'scheduled_example_dag',
    default_args=default_args,
    description='A comprehensive example DAG',
    schedule_interval=timedelta(days=1),
    doc_md=dag_doc_md,
)

# Start Dummy Task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task to retrieve connection info

# Function to retrieve a connection
# https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html
def get_connection_info(**kwargs):
    # Retrieve the connection
    conn = BaseHook.get_connection('my_connection_id')
    # Log connection info
    print(f"Host: {conn.host}")
    print(f"Schema: {conn.schema}")
    print(f"Login: {conn.login}")
    print(f"Password: {conn.password}")
    # Push the connection info to XCom
    kwargs['ti'].xcom_push(key='connection_info', value={'host': conn.host, 'schema': conn.schema})

get_connection_info_task = PythonOperator(
    task_id='get_connection_info_task',
    python_callable=get_connection_info,
    provide_context=True,
    dag=dag,
)

# Task to retrieve Airflow Variable

# Function to retrieve an Airflow Variable
# https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html#variables
def get_variable(**kwargs):
    # Retrieve the variable
    my_variable = Variable.get("my_variable_key")
    # Log variable value
    print(f"Variable Value: {my_variable}")
    # Push the variable value to XCom
    kwargs['ti'].xcom_push(key='variable_value', value=my_variable)

get_variable_task = PythonOperator(
    task_id='get_variable_task',
    python_callable=get_variable,
    provide_context=True,
    dag=dag,
)

# Task to use XComs

# Function to use XComs
# https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html#xcoms
def use_xcoms(**kwargs):
    # Pull values from XCom
    connection_info = kwargs['ti'].xcom_pull(key='connection_info', task_ids='get_connection_info_task')
    variable_value = kwargs['ti'].xcom_pull(key='variable_value', task_ids='get_variable_task')
    # Log the pulled values
    print(f"Pulled Connection Info: {connection_info}")
    print(f"Pulled Variable Value: {variable_value}")

use_xcoms_task = PythonOperator(
    task_id='use_xcoms_task',
    python_callable=use_xcoms,
    provide_context=True,
    dag=dag,
)

# Sensor to wait for a file to appear in a specified directory
file_sensor = FileSensor(
    task_id='file_sensor_task',
    filepath='/path/to/my/file.txt',
    fs_conn_id='my_filesystem_connection',
    poke_interval=10,
    timeout=600,
    dag=dag,
)

# End Dummy Task
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define the task dependencies
start >> get_connection_info_task >> get_variable_task >> use_xcoms_task >> file_sensor >> end