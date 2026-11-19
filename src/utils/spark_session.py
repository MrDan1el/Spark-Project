from pyspark.sql import SparkSession
import src.utils.config as cfg


def get_spark_session(app_name: str = "spark-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.5.0,"
            "com.clickhouse:clickhouse-jdbc:0.9.8,"
            "org.postgresql:postgresql:42.7.4",
        )
        # S3A-конфигурация для MinIO
        .config("spark.hadoop.fs.s3a.endpoint", cfg.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", cfg.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.S3_SECRET_KEY)
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            str(cfg.S3_PATH_STYLE_ACCESS).lower(),
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )