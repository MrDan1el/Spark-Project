import os


# Minio S3
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_PATH_STYLE_ACCESS = os.getenv("S3_PATH_STYLE_ACCESS", "true").lower() == "true"

# ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_INPUT_DB = os.getenv("CLICKHOUSE_INPUT_DB", "marketing")
CLICKHOUSE_OUTPUT_DB = os.getenv("CLICKHOUSE_OUTPUT_DB", "default")