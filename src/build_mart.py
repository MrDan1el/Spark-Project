from pyspark.sql import DataFrame
import pyspark.sql.functions as F 

import src.utils.config as cfg
from src.utils.spark_session import get_spark_session

import logging
import requests
from datetime import datetime


logger = logging.getLogger(__name__)


def prepare_visits(visits_raw: DataFrame) -> DataFrame:
    '''Фильтрует и обрабатывает данные visits_raw, возвращает готовый для соединения дата фрейм.'''

    return visits_raw \
        .withColumn('dt', F.date_format(F.col('visitDateTime'), 'yyyy-MM-dd')) \
        .filter(F.col('source').isin('ad', 'direct')) \
        .filter(F.col('URL').rlike('.*checkout.*|.*add.*|.*home.*|.*contact.*|.*top50.*|.*customer-service.*|.*wishlist.*|.*sale.*|.*best-sellers.*|.*view.*|.*discount.*|.*featured.*|.*new-arrivals.*|.*settings.*|.*return-policy.*|.*edit.*|.*delete.*|.*reviews.*|.*products.*|.*about.*')) \
        .withColumn('params_regex', F.regexp_replace(F.col('params'), r'\[|\]', '')) \
        .withColumn('params_split', F.split('params_regex', ', ')) \
        .withColumn('event_type', F.regexp_replace(F.col('params_split')[0], "'", '')) \
        .withColumn('event_id', F.col('params_split')[1].cast('int')) \
        .filter(F.col('event_type') == 'submit') \
        .select(
            'dt',
            F.col('visitid').cast('string'),
            F.col('clientID').cast('string').alias('clientid'),
            'URL',
            F.col('duration').cast('int'),
            'source',
            'UTMCampaign',
            'event_type',
            'event_id'
        ).distinct()


def prepare_costs(costs_raw: DataFrame) -> DataFrame:
    '''Фильтрует и обрабатывает данные costs_raw, возвращает готовый для соединения дата фрейм.'''

    return costs_raw \
        .groupBy(F.col('date').cast('string'), F.col('campaign_id').cast('int')) \
        .agg(
            F.sum(F.col('costs')).cast('decimal(19,2)').alias('costs'),
            F.sum(F.col('clicks')).alias('clicks'),
            F.sum(F.col('views')).alias('views')
        )


def prepare_campaigns(campaigns_raw: DataFrame) -> DataFrame:
    '''Фильтрует и обрабатывает данные campaigns_raw, возвращает готовый для соединения дата фрейм.'''

    return campaigns_raw \
        .withColumn('campaign_id', F.col('campaign_id').cast('int')) \
        .withColumn(
            'campaign_duration',
            F.when(F.col('campaign_name').like('year%'), 'Год')
            .when(F.col('campaign_name').like('quarter%'), 'Квартал')
            .when(F.col('campaign_name').like('month%'), 'Месяц')
            .otherwise(None)
        )


def prepare_submits(submits_raw: DataFrame) -> DataFrame:
    '''Фильтрует и обрабатывает данные submits_raw, возвращает готовый для соединения дата фрейм.'''

    return submits_raw \
        .withColumn('phone', F.col('phone').cast('string')) \
        .withColumn('phone_plus', F.concat(F.lit('+'), F.col('phone'))) \
        .withColumn('phone_md5', F.md5('phone')) \
        .withColumn('phone_plus_md5', F.md5('phone_plus'))


def prepare_deals(deals_raw: DataFrame) -> DataFrame:
    '''Фильтрует и обрабатывает данные deals_raw, возвращает готовый для соединения дата фрейм.'''

    return deals_raw \
        .withColumn('username', F.split(F.col('email'), '@').getItem(0)) \
        .withColumn('domain', F.split(F.col('email'), '@').getItem(1)) \
        .where(F.col('domain').isin('example.com', 'example.org', 'example.net')) \
        .withColumn('phone', F.col('phone').cast('string'))


def data_mart_build(visits_df: DataFrame, costs_df: DataFrame, campaigns_df: DataFrame, submits_df: DataFrame, deals_df: DataFrame) -> DataFrame:
    '''Соединяет полученные дата фреймы в одну итоговую витрину и возвращает ее.'''

    return visits_df.alias('v') \
        .join(
            submits_df.alias('s'),
            F.col('v.event_id') == F.col('s.submit_id'),
            'left'
        ) \
        .join(
            deals_df.alias('d'),
            (F.col('s.phone') == F.col('d.phone')) &
            (F.col('v.dt') <= F.col('d.deal_date')),
            'left'
        ) \
        .join(
            campaigns_df.alias('camp'),
            F.col('v.utmcampaign') == F.col('camp.campaign_name'),
            'left'
        ) \
        .join(
            costs_df.alias('c'),
            (F.col('camp.campaign_id') == F.col('c.campaign_id')) &
            (F.col('v.dt') == F.col('c.date')),
            'left'
        ) \
        .select(
            F.col('v.dt').cast('date'),
            F.col('v.visitid').alias('visit_id'),
            F.col('v.clientid').alias('client_id'),
            'v.url',
            'v.duration',
            'v.source',
            'v.utmcampaign',
            'v.event_type',
            'v.event_id',
            F.col('s.submit_id').cast('int'),
            's.name',
            's.phone',
            's.phone_plus',
            's.phone_md5',
            's.phone_plus_md5',
            F.col('d.deal_id').cast('int'),
            F.col('d.deal_date').cast('date'),
            'd.fio',
            F.col('d.phone').alias('phone_deal'),
            'd.email',
            'd.address',
            'd.username',
            'd.domain',
            'camp.campaign_name',
            'camp.campaign_duration',
            'c.costs',
            F.col('c.clicks').cast('int'),
            F.col('c.views').cast('int')
        )


def drop_partition(table: str, date_str: str) -> None:
    '''Удаляет партицию ClickHouse за указанную дату (идемпотентность перед INSERT).'''
    
    partition_key = date_str.replace("-", "")  # '2026-11-19' -> '20261119'
    query = f"ALTER TABLE {table} DROP PARTITION '{partition_key}'"

    r = requests.post(
        f"http://{cfg.CLICKHOUSE_HOST}:{cfg.CLICKHOUSE_PORT}/",
        params={
            "user": cfg.CLICKHOUSE_USER,
            "password": cfg.CLICKHOUSE_PASSWORD,
            "query": query,
        },
    )
    # Если партиции нет — ClickHouse вернёт 200 с пустым телом. Ошибки — raise.
    r.raise_for_status()


def main(date_str: str) -> None:
    spark = get_spark_session(f"build-mart-{date_str}")

    try:
        logger.info(f"Чтение данных за {date_str}")
        visits_raw = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:clickhouse://{cfg.CLICKHOUSE_HOST}:{cfg.CLICKHOUSE_PORT}/{cfg.CLICKHOUSE_INPUT_DB}") \
            .option("dbtable", f"(SELECT * FROM marketing.visits WHERE toDate(visitDateTime) = '{date_str}') AS t") \
            .option("user", cfg.CLICKHOUSE_USER) \
            .option("password", cfg.CLICKHOUSE_PASSWORD) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .load()
        costs_raw = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{cfg.POSTGRES_HOST}:{cfg.POSTGRES_PORT}/{cfg.POSTGRES_DB}") \
            .option("dbtable", f"(SELECT * FROM public.costs WHERE date = '{date_str}') AS t") \
            .option("user", cfg.POSTGRES_USER) \
            .option("password", cfg.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        campaigns_raw = spark.read.csv("s3a://raw/campaigns_dict.csv", header=True)
        submits_raw = spark.read.parquet(f"s3a://raw/submits/dt={date_str}/")
        deals_raw = spark.read.parquet(f"s3a://raw/deals/dt={date_str}/")

        logger.info("Трансформация полученных данных")
        visits_df = prepare_visits(visits_raw)
        costs_df = prepare_costs(costs_raw)
        campaigns_df = prepare_campaigns(campaigns_raw)
        submits_df = prepare_submits(submits_raw)
        deals_df = prepare_deals(deals_raw)

        logger.info(f"Сборка витрины за {date_str}")
        customer_detailed = data_mart_build(visits_df, costs_df, campaigns_df, submits_df, deals_df)

        drop_partition("default.customer_detailed", date_str)

        logger.info("Загрузка витрины в ClickHouse")
        customer_detailed.write \
            .format("jdbc") \
            .option("url", f"jdbc:clickhouse://{cfg.CLICKHOUSE_HOST}:{cfg.CLICKHOUSE_PORT}/{cfg.CLICKHOUSE_OUTPUT_DB}") \
            .option("dbtable", "default.customer_detailed") \
            .option("user", cfg.CLICKHOUSE_USER) \
            .option("password", cfg.CLICKHOUSE_PASSWORD) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "10000") \
            .mode("append") \
            .save()
        logger.info("Витрина загружена в ClickHouse")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    date_str = datetime.now().strftime("%Y-%m-%d")
    main(date_str)