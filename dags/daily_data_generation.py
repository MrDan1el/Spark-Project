import io
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from faker import Faker

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


CLICKHOUSE_HTTP = "http://clickhouse:8123"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse"
CLICKHOUSE_DB = "marketing"


def _rng(date_str: str) -> np.random.Generator:
    """Детерминированный RNG: повторный запуск за ту же дату даст те же данные."""
    return np.random.default_rng(seed=int(date_str.replace("-", "")))


# ---------- Генерация ----------

def _gen_costs(date_str: str) -> pd.DataFrame:
    rng = _rng(date_str)
    rows = [
        {
            "date": date_str,
            "campaign_id": cid,
            "costs": round(float(rng.uniform(0, 1000)), 2),
            "clicks": int(rng.integers(0, 100)),
            "views": int(rng.integers(0, 1000)),
        }
        for cid in range(1, 101)
    ]
    return pd.DataFrame(rows)


def _gen_visits(date_str: str, campaign_names: list) -> pd.DataFrame:
    rng = _rng(date_str)
    n = 2000
    day_start = datetime.strptime(date_str, "%Y-%m-%d")
    visit_dts = [
        (day_start + timedelta(seconds=int(s))).strftime("%Y-%m-%d %H:%M:%S")
        for s in rng.integers(0, 24 * 3600, n)
    ]

    base_url = "https://our-cool-website.com/"
    paths = [
        "add", "view", "edit", "delete", "checkout", "products", "home",
        "contact", "about", "settings", "sale", "discount", "new-arrivals",
        "top50", "best-sellers", "featured", "reviews", "wishlist",
        "customer-service", "return-policy",
    ]

    params = [
        ["submit", int(rng.integers(1, 10000))] if rng.random() > 0.5 else []
        for _ in range(n)
    ]

    return pd.DataFrame({
        "visitid": rng.integers(100000, 999999, n),
        "visitDateTime": visit_dts,
        "URL": [base_url + rng.choice(paths) for _ in range(n)],
        "duration": rng.integers(0, 101, n),
        "clientID": rng.integers(100, 999, n),
        "source": rng.choice(["ad", "direct", "internal", "organic"], n),
        "UTMCampaign": rng.choice(campaign_names, n),
        "params": [str(p) for p in params],
    })


def _gen_submits(date_str: str) -> pd.DataFrame:
    rng = _rng(date_str)
    fake = Faker()
    n = 500
    return pd.DataFrame({
        "submit_id": rng.integers(1, 10001, n),
        "name": [fake.first_name() for _ in range(n)],
        "phone": [f"+79{fake.msisdn()[4:]}" for _ in range(n)],
    })


def _gen_deals(date_str: str) -> pd.DataFrame:
    """Независима от submits: в проде deals приходят из CRM, submits — из бэкенда сайта."""
    rng = _rng(date_str)
    fake = Faker()
    n = 200
    return pd.DataFrame({
        "deal_id": rng.integers(1, 10_000_000, n),
        "deal_date": [date_str] * n,
        "fio": [fake.name() for _ in range(n)],
        "phone": [f"+79{fake.msisdn()[4:]}" for _ in range(n)],
        "email": [fake.email() for _ in range(n)],
        "address": [fake.address().replace("\n", ", ") for _ in range(n)],
    })


# ---------- Загрузка (идемпотентная) ----------

def _load_costs(df: pd.DataFrame, date_str: str) -> None:
    pg = PostgresHook(postgres_conn_id="postgres_marketing")
    from psycopg2.extras import execute_values
    with pg.get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM public.costs WHERE date = %s", (date_str,))
        execute_values(
            cur,
            "INSERT INTO public.costs (date, campaign_id, costs, clicks, views) VALUES %s",
            [(r.date, r.campaign_id, r.costs, r.clicks, r.views) for r in df.itertuples()],
        )
        conn.commit()


def _load_visits(df: pd.DataFrame, date_str: str) -> None:
    # Idempotency: удалить день, затем вставить
    delete_q = (
        f"ALTER TABLE {CLICKHOUSE_DB}.visits DELETE "
        f"WHERE toDate(visitDateTime) = '{date_str}' "
        f"SETTINGS mutations_sync = 1"
    )
    r = requests.post(
        CLICKHOUSE_HTTP,
        params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD, "query": delete_q},
    )
    r.raise_for_status()

    cols = "visitid, visitDateTime, URL, duration, clientID, source, UTMCampaign, params"
    payload = df[["visitid", "visitDateTime", "URL", "duration", "clientID",
                  "source", "UTMCampaign", "params"]].to_csv(index=False, header=False).encode()
    r = requests.post(
        CLICKHOUSE_HTTP,
        params={
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
            "query": f"INSERT INTO {CLICKHOUSE_DB}.visits ({cols}) FORMAT CSV",
        },
        data=payload,
    )
    r.raise_for_status()


def _load_parquet_to_s3(df: pd.DataFrame, key: str) -> None:
    s3 = S3Hook(aws_conn_id="minio_s3")
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    s3.load_bytes(buf.getvalue(), key=key, bucket_name="raw", replace=True)


# ---------- Task callables ----------

def generate_costs_task(**context):
    date_str = context["ds"]
    _load_costs(_gen_costs(date_str), date_str)


def generate_visits_task(**context):
    date_str = context["ds"]
    s3 = S3Hook(aws_conn_id="minio_s3")
    campaigns = pd.read_csv(io.StringIO(s3.read_key("campaigns_dict.csv", bucket_name="raw")))
    names = campaigns.iloc[:, 1].tolist()
    _load_visits(_gen_visits(date_str, names), date_str)


def generate_submits_task(**context):
    date_str = context["ds"]
    _load_parquet_to_s3(_gen_submits(date_str), f"submits/dt={date_str}/data.parquet")


def generate_deals_task(**context):
    date_str = context["ds"]
    _load_parquet_to_s3(_gen_deals(date_str), f"deals/dt={date_str}/data.parquet")


# ---------- DAG ----------

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 11, 1),
}

with DAG(
    dag_id="daily_data_generation",
    default_args=default_args,
    description="Генерация и загрузка данных за ds в ClickHouse/Postgres/S3 (идемпотентно)",
    schedule="0 5 * * *",        # 05:00 UTC -> ds = вчерашняя дата
    catchup=False,
    max_active_runs=1,
    tags=["daily", "generation"],
) as dag:

    generate_costs = PythonOperator(
        task_id="generate_costs", 
        python_callable=generate_costs_task
        )
    
    generate_visits = PythonOperator(
        task_id="generate_visits", 
        python_callable=generate_visits_task
        )
    
    generate_submits = PythonOperator(
        task_id="generate_submits", 
        python_callable=generate_submits_task
        )
    
    generate_deals = PythonOperator(
        task_id="generate_deals", 
        python_callable=generate_deals_task
        )