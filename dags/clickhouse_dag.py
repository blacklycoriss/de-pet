from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
import logging
from datetime import datetime, timedelta



def connect_to_databases(**kwargs):

        logging.info("Выполняется подключение к Посткря...")
        pg_hook = PostgresHook(postgres_conn_id='postgres_db')

        logging.info("Выполняется подключение к Кулику...")
        cl_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_db')

get_postgres_raw_data = SQLExecuteQueryOperator(
    task_id='get_postgres_raw_data',
    sql="SELECT * FROM public.yf_data;",
    conn_id='postgres_db',
)

ch_create_table = ClickHouseOperator(
    task_id='create_table_if-not_exists',
    clickhouse_conn_id='clickhouse_db',
    sql="""
        CREATE TABLE IF NOT EXISTS raw_pg_data (
            id Int64,
            open Decimal(10, 4),
            high Decimal(10, 4),
            low Decimal(10, 4),
            close Decimal(10, 4),
            volume Int64,
        ) 
        ENGINE = MergeTree()
        ORDER BY (id, date);
    """,
)

#-------------------------------------------------------------------------------
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
    connect_task = PythonOperator(
        task_id='connect_to_databases',
        python_callable=connect_to_databases,
    )
    end_task = EmptyOperator(task_id='end')

    start_task >> connect_task >> get_postgres_raw_data >> ch_create_table >>end_task