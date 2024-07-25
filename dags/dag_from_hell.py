from datetime import datetime
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

schema = Variable.get("schema", default="prod")

# The connection is setup and has dict-cursor enabled
tables = PostgresOperator(
    postgres_conn_id="postgres_default",
    sql=f"""
		SELECT table_name
		from information_schema.tables where table_schema='{schema}';
	"""
).execute({})
tables = [table["table_name"] for table in tables]

from airflow import DAG


def my_dag():
    with DAG(dag_id="test_dag", start_time=datetime.now()) as dag:
        get_id_tasks = []
        delete_old_records_tasks = []

        for table in tables:
            max_id_task = PostgresOperator(
                postgres_conn_id="postgres_default",
                sql="""
					SELECT max(id) from {table} 
					WHERE updated_at < '{datetime.now()}' - INTERVAL '1 day';
				"""
            )
            get_id_tasks.append(max_id_task)

            @task()
            def delete_records(max_id):
                return PostgresOperator(
                    postgres_conn_id="postgres_default",
                    sql="""
						delete from {table} 
						WHERE id < '{max_id}';
					"""
                ).execute({})

            d = delete_records(max_id_task)
            delete_old_records_tasks.append(d)

        get_id_tasks >> delete_old_records_tasks