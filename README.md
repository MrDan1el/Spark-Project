# Data Pipeline

ETL-пайплайн, собирающий аналитическую витрину из пяти разнородных источников для бизнеса по продаже ___.

## Бизнес-задача

Собрать единую витрину по воронке: показ рекламы → визит на сайт →
заявка с формы → оформленная сделка. Витрина используется маркетингом
для анализа эффективности рекламных кампаний и конверсии.

## Архитектура

```
ClickHouse (marketing.visits)  ─┐
Postgres   (public.costs)      ─┤
S3         (campaigns_dict)    ─┼──► Spark ──► ClickHouse (default.customer_detailed)
S3         (submits)           ─┤
S3         (deals)             ─┘
```

### Источники

| Источник | Где хранится | Почему именно там |
|---|---|---|
| `visits` | ClickHouse | Append-only event stream, high-volume.  |
| `costs` | Postgres | Транзакционные данные, малый объём, важна свежесть для финансовой отчётности |
| `campaigns_dict` | S3 | Reference data от маркетинга — один csv файл |
| `submits` | S3 | Выгрузка из бэкенда сайта в data lake  |
| `deals` | S3 | Синк из CRM/ERP в data lake |

Все три источника читаются тремя разными коннекторами (ClickHouse JDBC,
Postgres JDBC, S3A), что позволяет продемонстрировать разные паттерны
работы с источниками.

## Стек

- **Spark 4.3.0** (PySpark) — обработка
- **MinIO** (S3-совместимое хранилище) — data lake
- **ClickHouse 25.8** — OLAP-источник `visits` и БД для итоговой витрины
- **Postgres 16** — OLTP-источник `costs`
- **Docker Compose** — локальная инфраструктура

## Структура репозитория

```
src/
├── build_mart.py            # entrypoint: чтение → трансформации → запись
└── utils/
    ├── config.py            # пути и креды (env override)
    └── spark_session.py     # SparkSession с S3A + JDBC-драйверами
docker-compose.yml           # MinIO + ClickHouse + Postgres
requirements.txt             # необходимые библиотеки
```

## TODO / в работе

- **PII в S3**: `submits` и `deals` лежат в S3 в открытом
  виде (содержат `phone`, `email`, `fio`). Нужен маскирующий
  слой перед загрузкой в S3.
- **CDC**: сейчас `submits`/`deals` попадают в S3 как разовые выгрузки.
- **Инкрементальная загрузка**: сейчас пайплайн пересчитывает витрину целиком
  (`mode="append"`).
- **Airflow**: оркестрация и расписание.
- **Тесты**: юнит-тесты на `prepare_*` функции.