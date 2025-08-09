from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from alphavantage_stock_etl import run_etl  
import pendulum 

# Define IST timezone
local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'akanksha',
    'start_date': datetime(2025, 8, 8, tzinfo=local_tz),
    'retries': 2,
}

with DAG(
    'daily_stock_etl',
    default_args=default_args,
    schedule='0 11 * * *',
    catchup=False,
    tags=['stock', 'etl']
) as dag:
    
    def dynamic_etl_task():
        symbols_str = Variable.get("stock_etl_symbols", default_var="AAPL,GOOGL")
        symbols = [s.strip().upper() for s in symbols_str.split(",")]
        run_etl(symbols)

    run_etl_task = PythonOperator(
        task_id='run_dynamic_stock_etl',
        python_callable=dynamic_etl_task
    )

