"""Deduplicate archived fixture data, build profile SCD2, and validate joins."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)


def main() -> None:
    bucket = os.getenv("S3_BUCKET", "cinema")
    dataset = os.getenv("DATASET", "fixture")
    archive_prefix = os.getenv("ARCHIVE_PREFIX", "raw").strip("/")
    catalogue_dataset = os.getenv("CATALOGUE_DATASET", dataset)
    interval_start = os.getenv("DATA_INTERVAL_START")
    interval_end = os.getenv("DATA_INTERVAL_END")
    run_id = os.getenv("RUN_ID", "manual")
    dag_id = os.getenv("DAG_ID", "manual")
    task_id = os.getenv("TASK_ID", "process_archive")
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    period = interval_start[:10] if interval_start else "all"
    root = f"s3a://{bucket}"
    spark = (SparkSession.builder.appName(f"process-{dataset}-archive")
             .master(os.getenv("SPARK_MASTER", "local[4]"))
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("user_id", LongType(), False),
        StructField("event_type", StringType(), False),
        StructField("content_id", LongType(), True),
        StructField("watch_time_seconds", IntegerType(), True),
        StructField("query", StringType(), True),
        StructField("available_at", TimestampType(), False),
    ])
    profile_schema = StructType([
        StructField("change_id", StringType(), False),
        StructField("user_id", LongType(), False),
        StructField("effective_at", TimestampType(), False),
        StructField("country", StringType(), False),
        StructField("subscription_tier", StringType(), False),
        StructField("available_at", TimestampType(), False),
    ])
    catalogue_schema = StructType([
        StructField("content_id", LongType(), False),
        StructField("category", StringType(), False),
        StructField("duration_seconds", IntegerType(), False),
    ])

    events_topic = os.getenv("EVENTS_TOPIC", "cinema.events")
    profiles_topic = os.getenv("PROFILES_TOPIC", "cinema.profile_changes")
    events_path = f"{root}/{archive_prefix}/{events_topic}/"
    profiles_path = f"{root}/{archive_prefix}/{profiles_topic}/"
    catalogue_path = f"{root}/source/{catalogue_dataset}/catalogue.jsonl"
    events = spark.read.schema(event_schema).json(events_path)
    profiles = spark.read.schema(profile_schema).json(profiles_path)
    catalogue = spark.read.schema(catalogue_schema).json(catalogue_path)

    events = events.dropDuplicates(["event_id"])
    profiles = profiles.dropDuplicates(["change_id"])
    if interval_end:
        interval_end_ts = datetime.fromisoformat(interval_end.replace("Z", "+00:00")).replace(tzinfo=None)
        profiles = profiles.filter(F.col("effective_at") < F.lit(interval_end_ts))
    if interval_start and interval_end:
        interval_start_ts = datetime.fromisoformat(interval_start.replace("Z", "+00:00")).replace(tzinfo=None)
        interval_events = events.filter((F.col("event_time") >= F.lit(interval_start_ts))
                                        & (F.col("event_time") < F.lit(interval_end_ts)))
    else:
        interval_events = events
    state_window = Window.partitionBy("user_id").orderBy("effective_at", "change_id")
    changes = (profiles
        .withColumn("prev_country", F.lag("country").over(state_window))
        .withColumn("prev_tier", F.lag("subscription_tier").over(state_window))
        .filter(F.col("prev_country").isNull()
                | (F.col("country") != F.col("prev_country"))
                | (F.col("subscription_tier") != F.col("prev_tier"))))
    versions = (changes
        .withColumn("valid_to", F.lead("effective_at").over(state_window))
        .withColumn("valid_from", F.col("effective_at"))
        .withColumn("is_current", F.col("valid_to").isNull())
        .select("user_id", "country", "subscription_tier", "valid_from", "valid_to", "is_current"))

    version_window = Window.partitionBy("user_id").orderBy("valid_from")
    version_checks = (versions
        .withColumn("next_from", F.lead("valid_from").over(version_window))
        .filter(~F.col("valid_to").eqNullSafe(F.col("next_from"))))
    if version_checks.limit(1).count():
        raise RuntimeError("SCD2 versions overlap or contain a gap")
    current_checks = (versions.groupBy("user_id")
        .agg(F.sum(F.col("is_current").cast("int")).alias("current_count"))
        .filter(F.col("current_count") != 1))
    if current_checks.limit(1).count():
        raise RuntimeError("Each user must have exactly one current SCD2 version")

    views = interval_events.filter(F.col("event_type") == "view").alias("v")
    c = catalogue.alias("c")
    p = versions.alias("p")
    joined = (views.join(c, F.col("v.content_id") == F.col("c.content_id"), "left")
        .join(p, (F.col("v.user_id") == F.col("p.user_id"))
              & (F.col("v.event_time") >= F.col("p.valid_from"))
              & (F.col("p.valid_to").isNull() | (F.col("v.event_time") < F.col("p.valid_to"))), "left")
        .select(
            F.col("v.event_id"), F.col("v.event_time"), F.col("v.user_id"),
            F.col("v.content_id"), F.col("v.watch_time_seconds"),
            F.col("c.category"), F.col("c.duration_seconds"),
            F.col("p.country"), F.col("p.subscription_tier"),
            F.col("p.valid_from"), F.col("p.valid_to"),
        ))

    event_count = interval_events.count()
    profile_count = profiles.count()
    view_count = views.count()
    joined_count = joined.count()
    missing_catalogue = joined.filter(F.col("category").isNull()).count()
    missing_profile = joined.filter(F.col("country").isNull()).count()
    bad_match_counts = (joined.groupBy("event_id").count().filter(F.col("count") != 1).limit(1).count())
    print(f"Unique events: {event_count}; unique profile changes: {profile_count}")
    print(f"Unique views: {view_count}; joined views: {joined_count}")
    print(f"Views without catalogue match: {missing_catalogue}; without profile match: {missing_profile}")

    if joined_count != view_count or missing_catalogue or missing_profile or bad_match_counts:
        raise RuntimeError("Join validation failed: each unique view must match one content and one profile version")

    minute_metrics = (views
        .withColumn("minute", F.date_trunc("minute", "event_time"))
        .groupBy("minute", "content_id")
        .agg(F.countDistinct("event_id").alias("view_count"),
             F.sum("watch_time_seconds").alias("watch_time_seconds")))
    daily_profile_metrics = (joined
        .withColumn("event_date", F.to_date("event_time"))
        .groupBy("event_date", "country", "subscription_tier")
        .agg(F.countDistinct("event_id").alias("view_count"),
             F.sum("watch_time_seconds").alias("watch_time_seconds")))
    daily_top = (joined
        .withColumn("event_date", F.to_date("event_time"))
        .groupBy("event_date", "content_id")
        .agg(F.countDistinct("event_id").alias("view_count")))
    top_window = Window.partitionBy("event_date").orderBy(F.desc("view_count"), F.asc("content_id"))
    daily_top = daily_top.withColumn("rank", F.row_number().over(top_window)).filter(F.col("rank") <= 5)

    search_words = (interval_events.filter(F.col("event_type") == "search")
        .select("event_id", F.to_date("event_time").alias("event_date"),
                F.explode(F.split(F.lower(F.coalesce("query", F.lit(""))), r"\s+")).alias("word"))
        .filter(F.length("word") > 4)
        .groupBy("event_date", "word")
        .agg(F.count(F.lit(1)).alias("search_count")))

    for name, frame, keys in (
        ("minute_metrics", minute_metrics, ["minute", "content_id"]),
        ("daily_profile_metrics", daily_profile_metrics, ["event_date", "country", "subscription_tier"]),
        ("daily_top5", daily_top, ["event_date", "content_id"]),
        ("search_word_frequencies", search_words, ["event_date", "word"]),
    ):
        if frame.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
            raise RuntimeError(f"Duplicate keys detected in {name}: {keys}")

    start_meta = interval_start_ts if interval_start and interval_end else datetime(1970, 1, 1)
    end_meta = interval_end_ts if interval_start and interval_end else datetime(9999, 1, 1)

    def with_metadata(frame, snapshot=False):
        result = (frame
            .withColumn("dataset", F.lit(dataset))
            .withColumn("_dag_id", F.lit(dag_id))
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_task_id", F.lit(task_id))
            .withColumn("_data_interval_start", F.lit(start_meta).cast("timestamp"))
            .withColumn("_data_interval_end", F.lit(end_meta).cast("timestamp"))
            .withColumn("_loaded_at", F.lit(loaded_at).cast("timestamp")))
        if snapshot:
            result = result.withColumn("snapshot_date", F.to_date(F.lit(start_meta)))
        return result

    output_root = f"{root}/processed/{dataset}/period={period}"
    with_metadata(interval_events).write.mode("overwrite").parquet(f"{output_root}/events_deduplicated")
    with_metadata(versions, snapshot=True).write.mode("overwrite").parquet(f"{output_root}/profile_scd2")
    with_metadata(joined).write.mode("overwrite").parquet(f"{output_root}/views_enriched")
    for name, frame in (("minute_metrics", minute_metrics),
                        ("daily_profile_metrics", daily_profile_metrics),
                        ("daily_top5", daily_top),
                        ("search_word_frequencies", search_words)):
        with_metadata(frame).write.mode("overwrite").parquet(f"{output_root}/{name}")
        print(f"Wrote {name}: {frame.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
