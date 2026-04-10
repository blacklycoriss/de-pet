from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.utils.trigger_rule import TriggerRule
import logging
from datetime import datetime, timedelta



def connect_to_databases():

        logging.info("Выполняется подключение к Посткря...")
        pg_hook = PostgresHook(postgres_conn_id='postgres_db')

        logging.info("Выполняется подключение к Кулику...")
        cl_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_db')

def get_max_date(**context):
    hook = ClickHouseHook(clickhouse_conn_id='clickhouse_db')
    result = hook.get_first("SELECT toString(max(date)) FROM ch_yf_data")
    max_date = result[0] if result and result[0] else '1970-01-01'
    context['ti'].xcom_push(key='max_date', value=max_date)
    logging.info(f"Max date in ClickHouse: {max_date}")

# get_postgres_raw_data = SQLExecuteQueryOperator(
#     task_id='get_postgres_raw_data',
#     sql="SELECT * FROM public.yf_data;",
#     conn_id='postgres_db',
# )

check_table = ClickHouseOperator(
    task_id='check_table_empty',
    clickhouse_conn_id='clickhouse_db',
    sql="""
        SELECT count() = 0 AS is_empty 
        FROM system.tables 
        WHERE database = currentDatabase() AND name = 'ch_yf_data'
    """,
    do_xcom_push=True,
)

def choose_branch(**context):
    ti = context['ti']
    result = ti.xcom_pull(task_ids='check_table_empty', key='return_value')
    is_empty = bool(result[0][0]) if result else True
    return 'initial_create' if is_empty else 'incremental_load'
    

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=choose_branch,
)

initial_create = ClickHouseOperator(
    task_id='initial_create',
    clickhouse_conn_id='clickhouse_db',
    sql="""
        CREATE TABLE IF NOT EXISTS ch_yf_data (
            id Int64,
            date Date,
            open Decimal(10, 4),
            high Decimal(10, 4),
            low Decimal(10, 4),
            close Decimal(10, 4),
            adj_close Decimal(10, 4),
            volume Int64
        ) 
        ENGINE = ReplacingMergeTree()
        ORDER BY (id, date);
    """,
)

initial_insert = ClickHouseOperator(
    task_id='initial_insert',
    clickhouse_conn_id='clickhouse_db',
    sql="""
        INSERT INTO ch_yf_data (id, date, open, high, low, close, adj_close, volume)
        SELECT *
        FROM raw_pg_data
    """,
)


incremental_load = ClickHouseOperator(
    task_id='incremental_load',
    clickhouse_conn_id='clickhouse_db',
    sql="""
        INSERT INTO ch_yf_data (id, date, open, high, low, close, adj_close, volume)
        SELECT s.id, s.date, s.open, s.high, s.low, s.close, s.adj_close, s.volume
        FROM raw_pg_data AS s
        LEFT ANTI JOIN ch_yf_data AS t ON s.id = t.id AND s.date = t.date
        WHERE s.date > toDate('{{ ti.xcom_pull(task_ids="get_max_date", key="max_date") }}')
)
    """,
)

join = EmptyOperator(
    task_id='join',
    trigger_rule=TriggerRule.NONE_FAILED    
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
    max_active_runs=1,
    catchup=True,
    tags=['etl', 'postgres', 'clickhouse']
) as dag:

    start_task = EmptyOperator(task_id='start')
    connect_task = PythonOperator(
        task_id='connect_to_databases',
        python_callable=connect_to_databases,
    )
    get_max_date_task = PythonOperator(
        task_id='get_max_date',
        python_callable=get_max_date,
    )
    end_task = EmptyOperator(task_id='end')

    start_task >> connect_task >> check_table >> branching
    branching >> [initial_create, incremental_load]
    initial_create >> initial_insert >> join
    get_max_date_task >> incremental_load >> join
    join >> end_task