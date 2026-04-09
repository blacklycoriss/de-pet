from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
import logging
from datetime import datetime, timedelta



def postgres_to_clickhouse(**kwargs):

        logging.info("Выполняется подключение к Посткря...")
        pg_hook = PostgresHook(postgres_conn_id='postgres_db')

        logging.info("Выполняется подключение к Кулику...")
        cl_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_db')

default_args = {
    'owner': 'black lycoriss',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='postgres_to_clickhouse',
    default_args=default_args,
    description='ETL: агрузка данных из Postgres в Clickhouse',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=True,
    tags=['etl', 'postgres', 'clickhouse']
) as dag:

    start_task = EmptyOperator(task_id='start')
    execute_task = PythonOperator(
        task_id='postgres_to_clickhouse',
        python_callable=postgres_to_clickhouse,
    )
    end_task = EmptyOperator(task_id='end')

    start_task >> execute_task >> end_task