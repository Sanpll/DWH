from datetime import datetime, timezone

from jobs.common import (
    dataset_name, metadata_columns, run_parameters, s3_uri, create_spark_session,
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# функция, которая которая извлекает весь полезный контекст из airflow
def build_user_scd2(**context):
    start, end, calculation_date = run_parameters(context)
    date_text = calculation_date.isoformat()
    task_id = context["task"].task_id
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    dataset = dataset_name()

    spark = create_spark_session("hw23-build-user-scd2")
    try:
        source_path = s3_uri(f"raw/{dataset}/events/*.csv")
        raw = (
            spark.read.option("header", "true").option("inferSchema", "false")
            .option("mode", "FAILFAST").csv(source_path)
        )

        events = raw.select(
            F.to_timestamp(F.trim("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
            F.col("user_id").cast("long").alias("user_id"),
            F.col("user_fio").cast("string").alias("user_fio"),
            F.col("user_email").cast("string").alias("user_email"),
            F.col("user_login").cast("string").alias("user_login"),
        )

        invalid_rows = events.filter(
            F.col("event_time").isNull() | F.col("user_id").isNull() |
            F.col("user_fio").isNull() | F.col("user_email").isNull() |
            F.col("user_login").isNull()
        ).limit(1).count()
        if invalid_rows:
            raise ValueError("Source events contain NULL values or invalid event_time/user_id")

        events = events.filter(F.col("event_time") < F.lit(end).cast("timestamp"))

        duplicate_event_times = (
            events.groupBy("user_id", "event_time").count()
            .filter(F.col("count") > 1).limit(1).count()
        )
        if duplicate_event_times:
            raise ValueError("More than one event exists for a user at the same event_time")

        by_user_time = Window.partitionBy("user_id").orderBy(F.col("event_time").asc())
        with_previous = (
            events
            .withColumn("_profile", F.struct("user_fio", "user_email", "user_login"))
            .withColumn("_previous_profile", F.lag("_profile").over(by_user_time))
        )
        changed = with_previous.filter(
            F.col("_previous_profile").isNull() |
            (~F.col("_profile").eqNullSafe(F.col("_previous_profile")))
        )

        versions = changed.select(
            "user_id", "user_fio", "user_email", "user_login",
            F.col("event_time").alias("valid_from"),
        )
        by_version = Window.partitionBy("user_id").orderBy(F.col("valid_from").asc())
        scd2 = (
            versions
            .withColumn("valid_to", F.lead("valid_from").over(by_version).cast("timestamp"))
            .withColumn("is_current", F.col("valid_to").isNull())
            .withColumn("snapshot_date", F.lit(date_text).cast("date"))
            .select(
                F.col("user_id").cast("long"), "user_fio", "user_email", "user_login",
                F.col("valid_from").cast("timestamp"), F.col("valid_to").cast("timestamp"),
                F.col("is_current").cast("boolean"), F.col("snapshot_date").cast("date"),
                *metadata_columns(F, context, task_id, start, end, loaded_at),
            )
        )

        duplicate_keys = (
            scd2.groupBy("user_id", "valid_from").count()
            .filter(F.col("count") > 1).limit(1).count()
        )
        if duplicate_keys:
            raise AssertionError("SCD2 contains duplicate (user_id, valid_from) keys")

        target_path = s3_uri(
            f"results/{dataset}/user_scd2/snapshot_date={date_text}"
        )
        scd2.write.mode("overwrite").parquet(target_path)
        saved_count = spark.read.parquet(target_path).count()
        print(
            f"SCD2 snapshot saved: date={date_text}, rows={saved_count}, "
            f"path={target_path}, interval=[{start}, {end})",
            flush=True,
        )
        return target_path
    finally:
        spark.stop()
