from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.utils.trigger_rule import TriggerRule
import logging
from datetime import datetime, timedelta
  
def get_max_date(**context):
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_db')

    max_date = ch_hook.execute("SELECT MAX(date) FROM ch_yf_data")[0][0]
    if max_date is None:
        max_date='2026-01-01'
    context['ti'].xcom_push(key='max_date', value=max_date)

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
    get_max_date_task = PythonOperator(
        task_id='get_max_date_task',
        python_callable=get_max_date
    )
    incremental_load = ClickHouseOperator(
    task_id='incremental_load',
    clickhouse_conn_id='clickhouse_db',
    sql="""
        INSERT INTO ch_yf_data (id, date, open, high, low, close, adj_close, volume)
        SELECT s.id, s.date, s.open, s.high, s.low, s.close, s.adj_close, s.volume
        FROM raw_pg_data AS s
        LEFT ANTI JOIN ch_yf_data AS t ON s.id = t.id AND s.date = t.date
        WHERE s.date > toDate('{{ ti.xcom_pull(task_ids="get_max_date_task", key="max_date") }}')
    """,
)
    
    end_task = EmptyOperator(task_id='end')

    start_task >> get_max_date_task >> incremental_load >> end_task