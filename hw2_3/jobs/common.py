import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    jars_dir = os.environ.get("S3_JARS_DIR", "/opt/airflow/s3-jars")
    jars = ",".join([
        os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
        os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.262.jar"),
    ])

    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def utc_naive(value) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).replace(tzinfo=None)
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def run_parameters(context):
    start = utc_naive(context["data_interval_start"])
    end = utc_naive(context["data_interval_end"])
    if (end - start).total_seconds() != 86400:
        raise ValueError(f"Expected a one-day data interval, got {start!r} .. {end!r}")
    return start, end, start.date()


def metadata_columns(F, context, task_id, start, end, loaded_at):
    return [
        F.lit(context["dag"].dag_id).cast("string").alias("_dag_id"),
        F.lit(context["run_id"]).cast("string").alias("_run_id"),
        F.lit(task_id).cast("string").alias("_task_id"),
        F.lit(start).cast("timestamp").alias("_data_interval_start"),
        F.lit(end).cast("timestamp").alias("_data_interval_end"),
        F.lit(loaded_at).cast("timestamp").alias("_loaded_at"),
    ]


def bucket_name():
    return os.environ.get("S3_BUCKET", "hw23")


def dataset_name():
    return os.environ.get("HW23_DATASET", "demo")


def s3_uri(key: str) -> str:
    return f"s3a://{bucket_name()}/{key.lstrip('/')}"
