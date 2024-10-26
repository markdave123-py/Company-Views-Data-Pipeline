import airflow
from airflow import DAG
from airflow.utils.dates import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from core_sentiment.helper.getveiws import get_views
from core_sentiment.data_processor.extract import extract
from core_sentiment.data_processor.load import load
from core_sentiment.helper.database_engine import create_database
from core_sentiment.data_processor.excute_sql import execute_sql_file
from core_sentiment.analysis.top_comany import find_top_company_func
from core_sentiment.helper.getveiws import getpage, getpageUrl


page = getpage()
url = getpageUrl()

with DAG(
    dag_id="core_sentiment",
    start_date=datetime(2024, 10, 18),
    schedule_interval=None,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/core_sentiment/data_house']
) as dag:

    get_views_task = PythonOperator(
        task_id="get_views",
        python_callable=get_views
    )

    unzip_views = BashOperator(
        task_id="unzip_views",
        bash_command= f"gunzip /opt/airflow/dags/core_sentiment/views/{page}"
    )

    # create_database_task = PythonOperator(
    #     task_id='create_database',
    #     python_callable=create_database,
    # )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_admin',
        sql="""
        CREATE TABLE IF NOT EXISTS companies_views (
            company TEXT,
            views INTEGER,
            reference TEXT
        );
        """,
        database='airflow',
    )

    create_clean_csv = PythonOperator(
        task_id="create_clean_csv",
        python_callable=extract
    )

    create_sql_script = PythonOperator(
        task_id="create_sql_script",
        python_callable=load
    )

    execute_sql_script = PythonOperator(
        task_id='execute_sql_script',
        python_callable=execute_sql_file,
    )

    find_top_company = PythonOperator(
    task_id='find_top_company',
    python_callable=find_top_company_func,
    )

    get_views_task >> unzip_views
    unzip_views >> create_table
    unzip_views >> create_clean_csv >> create_sql_script
    [create_table, create_sql_script] >> execute_sql_script >> find_top_company
