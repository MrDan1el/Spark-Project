from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sensors.python import PythonSensor


def wait_visits(**context) -> bool:
    '''Возвращает True, если в marketing.visits есть данные за ds.'''

    ds = context["ds"]
    resp = requests.post(
        "http://clickhouse:8123/",
        params={"user": "default", "password": "clickhouse"},
        data=f"SELECT count() > 0 FROM marketing.visits WHERE toDate(visitDateTime) = '{ds}'",
    )
    resp.raise_for_status()
    return resp.text.strip() == "1"


def verify_mart(**context):
    '''Проверяет, что данные за ds были загружены в ClickHouse'''
    
    ds = context["ds"]
    resp = requests.post(
        "http://clickhouse:8123/",
        params={"user": "default", "password": "clickhouse"},
        data=f"SELECT count() FROM default.customer_detailed WHERE dt = '{ds}'",
    )
    resp.raise_for_status()

    count = int(resp.text.strip())
    if count == 0:
        raise ValueError(f"Витрина за dt={ds} пуста")
    
    context["ti"].xcom_push(key="mart_count", value=count)


default_args = {
    'owner': 'user',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 11, 1),
}

with DAG(
    dag_id='daily_mart_build',
    description="Собирает витрину customer_detailed после готовности всех источников",
    default_args=default_args,
    schedule="0 6 * * *",
    catchup=False,
    tags=['etl', 'spark', 'data mart'],
) as dag:

    wait_visits = PythonSensor(
        task_id="wait_visits",
        python_callable=wait_visits,
        poke_interval=60,
        timeout=60 * 60 * 2,
        mode="reschedule",
    )

    wait_costs = SqlSensor(
        task_id="wait_costs",
        conn_id="postgres_marketing",
        sql="SELECT count(*) > 0 FROM public.costs WHERE date = '{{ ds }}'",
        poke_interval=60,
        timeout=60 * 60 * 2,
        mode="reschedule",
    )

    wait_submits = S3KeySensor(
        task_id="wait_submits",
        bucket_name="raw",
        bucket_key="submits/dt={{ ds }}/data.parquet",
        aws_conn_id="minio_s3",
        poke_interval=60,
        timeout=60 * 60 * 2,
        mode="reschedule",
    )

    wait_deals = S3KeySensor(
        task_id="wait_deals",
        bucket_name="raw",
        bucket_key="deals/dt={{ ds }}/data.parquet",
        aws_conn_id="minio_s3",
        poke_interval=60,
        timeout=60 * 60 * 2,
        mode="reschedule",
    )

    run_build_mart = BashOperator(
        task_id='run_build_mart',
        bash_command='cd /opt/airflow && python3 -m src.build_mart',
    )

    verify_mart = PythonOperator(
        task_id="verify_mart",
        python_callable=verify_mart,
    )

    [wait_visits, wait_costs, wait_submits, wait_deals] >> run_build_mart >> verify_mart