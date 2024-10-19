import airflow
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

def execute_sql_file(**kwargs):

    sql_file_path = '/opt/airflow/dags/core_sentiment/data_house/load.sql'


    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found at path: {sql_file_path}")

    with open(sql_file_path, 'r') as file:
        sql_script = file.read()

    postgres_hook = PostgresHook(
        postgres_conn_id='postgres_admin',
        schema='airflow'
    )
    postgres_hook.run(sql_script)