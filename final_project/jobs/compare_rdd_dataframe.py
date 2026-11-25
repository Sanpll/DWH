"""Compare Spark RDD and DataFrame implementations on archived S3 events."""
from __future__ import annotations

import os
import re
from collections import defaultdict

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType, TimestampType,
)


def as_sorted_rows(rows):
    return sorted(tuple(row) for row in rows)


def main() -> None:
    dataset = os.getenv("DATASET", "fixture")
    events_topic = os.getenv("EVENTS_TOPIC", "cinema.events")
    archive_prefix = os.getenv("ARCHIVE_PREFIX", "raw").strip("/")
    root = f"s3a://{os.getenv('S3_BUCKET', 'cinema')}"
    spark = (SparkSession.builder.appName(f"rdd-dataframe-check-{dataset}")
             .master(os.getenv("SPARK_MASTER", "local[4]"))
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("user_id", LongType(), False),
        StructField("event_type", StringType(), False),
        StructField("content_id", LongType(), True),
        StructField("watch_time_seconds", LongType(), True),
        StructField("query", StringType(), True),
        StructField("available_at", TimestampType(), False),
    ])
    events = (spark.read.schema(schema).json(f"{root}/{archive_prefix}/{events_topic}/")
              .dropDuplicates(["event_id"]))
    searches = events.filter(F.col("event_type") == "search")
    views = events.filter(F.col("event_type") == "view")

    # DataFrame implementation: split on whitespace, lowercase and count words
    # longer than four characters. Repeated words in one query remain repeated.
    df_words = (searches
        .select(F.to_date("event_time").cast("string").alias("day"),
                F.explode(F.split(F.lower(F.coalesce("query", F.lit(""))), r"\s+")).alias("word"))
        .filter(F.length("word") > 4)
        .groupBy("day", "word").count())
    df_word_rows = as_sorted_rows(df_words.collect())

    # RDD implementation uses the same already-deduplicated S3 input.
    rdd_word_rows = (searches.select("event_time", "query").rdd
        .flatMap(lambda row: [
            ((row.event_time.date().isoformat(), word.lower()), 1)
            for word in re.split(r"\s+", row.query or "") if len(word) > 4
        ])
        .reduceByKey(lambda left, right: left + right)
        .map(lambda item: (item[0][0], item[0][1], item[1]))
        .collect())
    rdd_word_rows = as_sorted_rows(rdd_word_rows)
    if df_word_rows != rdd_word_rows:
        raise RuntimeError("RDD and DataFrame search word counts differ")

    df_counts = (views
        .withColumn("day", F.to_date("event_time").cast("string"))
        .groupBy("day", "content_id").count())
    df_ranked = (df_counts
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("day").orderBy(F.desc("count"), F.asc("content_id"))))
        .filter(F.col("rank") <= 5)
        .select("day", "content_id", "count", "rank"))
    df_top_rows = as_sorted_rows(df_ranked.collect())

    rdd_counts = (views.select(F.to_date("event_time").cast("string").alias("day"), "content_id").rdd
        .map(lambda row: ((row.day, row.content_id), 1))
        .reduceByKey(lambda left, right: left + right)
        .map(lambda item: (item[0][0], item[0][1], item[1]))
        .collect())
    by_day = defaultdict(list)
    for day, content_id, count in rdd_counts:
        by_day[day].append((content_id, count))
    rdd_top_rows = []
    for day, values in by_day.items():
        for rank, (content_id, count) in enumerate(sorted(values, key=lambda x: (-x[1], x[0]))[:5], 1):
            rdd_top_rows.append((day, content_id, count, rank))
    rdd_top_rows = as_sorted_rows(rdd_top_rows)
    if df_top_rows != rdd_top_rows:
        raise RuntimeError("RDD and DataFrame daily top-5 results differ")

    out = f"{root}/checks/{dataset}/rdd_dataframe"
    spark.createDataFrame(df_word_rows, ["day", "word", "search_count"]).write.mode("overwrite").parquet(f"{out}/search_words")
    spark.createDataFrame(df_top_rows, ["day", "content_id", "view_count", "rank"]).write.mode("overwrite").parquet(f"{out}/daily_top5")
    print(f"Search word rows match: {len(df_word_rows)}")
    print(f"Daily top-5 rows match: {len(df_top_rows)}")
    print("RDD and DataFrame results are identical")
    spark.stop()


if __name__ == "__main__":
    main()
