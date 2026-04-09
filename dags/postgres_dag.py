from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import yfinance as yf
import pandas as pd
import logging


def fetch_and_load_stock_data(**kwargs):

        data_interval_start = kwargs["data_interval_start"]
        data_interval_end = kwargs["data_interval_end"]

        # 1. Выгрузка из источника
        logging.info("Выполняется выгрузка данных...")
        df = yf.download("AAPL", start = data_interval_start, end = data_interval_end, auto_adjust=False)
        if df.empty:
            logging.warning("Нет данных за период")
            raise AirflowSkipException("Задача пропущена по условию.")

        df = df.reset_index()
        # Убираем MultiIndex в колонках (бывает в новых версиях yfinance)
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
        df.dropna(inplace=True)

        # Приводим названия колонок к snake_case
        df = df.rename(columns={
            'Date': 'date', 'Open': 'open', 'High': 'high',
            'Low': 'low', 'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'
        })

        # Явное приведение типов для совместимости с psycopg2
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        for col in ['open', 'high', 'low', 'close', 'adj_close']:
            df[col] = df[col].astype(float)
        df['volume'] = df['volume'].astype(int)
        records = df.values.tolist()
        columns = list(df.columns)

        # 2. Подключение к PostgreSQL через Airflow Connection
        logging.info("Выполняется подключение к БД...")
        pg_hook = PostgresHook(postgres_conn_id='postgres_db')


        # 4️⃣ Вставка данных через PostgresHook
        logging.info("Выполняется пакетная вставка (INSERT)...")

        pg_hook.insert_rows(
            table="public.yf_data",
            rows=records,
            target_fields=columns,
        )

        logging.info("Данные успешно загружены в PostgreSQL")

    






default_args = {
    'owner': 'danila musaev',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='yfinance_to_postgres',
    default_args=default_args,
    description='ETL: загрузка котировок из yfinance в PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=['finance', 'etl', 'postgres'],
) as dag:


    PythonOperator(
        task_id='fetch_and_load',
        python_callable=fetch_and_load_stock_data,
    )