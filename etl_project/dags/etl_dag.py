from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import snowflake.connector

def extract_data(**kwargs):
    api_key = 'IMMWDQS402UL11Z1'
    symbol = 'AAPL'
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'

    response = requests.get(url)
    data = response.json()

    time_series = data.get('Time Series (Daily)', {})
    records = []

    for date, values in time_series.items():
        record = {
            'date': date,
            'open': values['1. open'],
            'high': values['2. high'],
            'low': values['3. low'],
            'close': values['4. close'],
            'volume': values['5. volume']
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    df.to_csv('/tmp/stock_data.csv', index=False)

def transform_data(**kwargs):
    df = pd.read_csv('/tmp/stock_data.csv')
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date')
    df.to_csv('/tmp/transformed_stock_data.csv', index=False)

def load_data(**kwargs):
    df = pd.read_csv('/tmp/transformed_stock_data.csv')

    conn = snowflake.connector.connect(
        user='Nick',
        password='199748nicKK$$',
        account='nalba97',
        warehouse='nicks_warehouse',
        database='stocks',
        schema='stock_schema'
    )

    cur = conn.cursor()

    # Create table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume INT
    );
    """
    cur.execute(create_table_query)

    # Load data
    for index, row in df.iterrows():
        insert_query = f"""
        INSERT INTO stock_data (date, open, high, low, close, volume)
        VALUES ('{row['date']}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']});
        """
        cur.execute(insert_query)
    
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_etl_dag',
    default_args=default_args,
    description='A simple ETL DAG for stock data',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    dag=dag,
)

extract_task >> transform_task >> load_task
