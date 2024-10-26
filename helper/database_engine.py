import airflow
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DuplicateDatabase


def create_database(**kwargs):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_admin')
    conn = postgres_hook.get_conn()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = conn.cursor()
    new_db_name = 'airflow'

    try:
        cursor.execute(f'CREATE DATABASE {new_db_name};')
    except DuplicateDatabase:
        pass
    finally:
        cursor.close()
        conn.close()


