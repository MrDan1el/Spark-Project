from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'user',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='daily_mart_build',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['etl', 'spark', 'data mart'],
) as dag:

    run_build_mart = BashOperator(
        task_id='run_build_mart',
        bash_command='cd /opt/airflow && python3 -m src.build_mart',
    )

    # check_data = ClickHouseSensor(
    #     task_id='check_customer_detailed',
    #     sql='SELECT count() FROM default.customer_detailed',
    #     ...
    # )
    # run_build_mart >> check_data