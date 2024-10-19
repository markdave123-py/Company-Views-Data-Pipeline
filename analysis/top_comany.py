from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def find_top_company_func(**kwargs):
    postgres_hook = PostgresHook(
        postgres_conn_id='postgres_admin',
        schema='airflow'
    )
    records = postgres_hook.get_records("""
        SELECT
            company,
            SUM(views) AS total_views
        FROM
            companies_views
        GROUP BY
            company
        ORDER BY
            total_views DESC
        LIMIT 1;
    """)
    top_company = records[0]
    print(f"The company with the highest views is {top_company[0]} with {top_company[1]} views.")
