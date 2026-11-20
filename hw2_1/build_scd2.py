from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    spark = (
        SparkSession.builder
        .appName("SCD2_Homework")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    try:
        source_path = "s3a://dwh/source/event_log/"

        df_raw = (
            spark.read
            .option("header", "true")
            .option("mode", "FAILFAST")
            .csv(source_path)
        )

        print("Количество исходных строк:", df_raw.count())
        df_raw.printSchema()

        df_events = df_raw.select(
            F.to_timestamp(
                F.col("event_time"),
                "yyyy-MM-dd'T'HH:mm:ss'Z'",
            ).alias("event_time"),

            F.col("user_id").cast("long").alias("user_id"),

            F.col("user_fio").cast("string").alias("user_fio"),

            F.col("user_email").cast("string").alias("user_email"),

            F.col("user_login").cast("string").alias("user_login"),
        )

        df_events.printSchema()

        null_rows = df_events.filter(
            F.col("event_time").isNull()
            | F.col("user_id").isNull()
            | F.col("user_fio").isNull()
            | F.col("user_email").isNull()
            | F.col("user_login").isNull()
        ).count()

        print(f"Количество строк с NULL: {null_rows}")

        total = df_events.count()
        distinct = df_events.distinct().count()

        print(
            f"Количество полных дубликатов строк: "
            f"{total - distinct}"
        )

        window_user = (
            Window
            .partitionBy("user_id")
            .orderBy("event_time")
        )

        df_with_profile = (
            df_events
            .withColumn(
                "_profile",
                F.struct(
                    "user_fio",
                    "user_email",
                    "user_login",
                ),
            )
            .withColumn(
                "_previous_profile",
                F.lag("_profile").over(window_user),
            )
        )

        df_changed = df_with_profile.filter(
            F.col("_previous_profile").isNull()
            | (
                ~F.col("_profile").eqNullSafe(
                    F.col("_previous_profile")
                )
            )
        )

        df_versions = df_changed.select(
            "user_id",
            "user_fio",
            "user_email",
            "user_login",
            F.col("event_time").alias("valid_from"),
        )

        window_versions = (
            Window
            .partitionBy("user_id")
            .orderBy("valid_from")
        )

        df_scd2 = (
            df_versions
            .withColumn(
                "valid_to",
                F.lead("valid_from").over(window_versions)
            )
            .withColumn(
                "is_current",
                F.col("valid_to").isNull()
            )
        )

        df_scd2.printSchema()

        df_scd2.orderBy(
            "user_id",
            "valid_from",
        ).show(30, truncate=False)

        target_path = "s3a://dwh/target/users_scd2/"

        df_scd2.write.mode("overwrite").parquet(target_path)

        df_scd2_result = spark.read.parquet(target_path)

        df_scd2_result.printSchema()

        df_scd2_result.orderBy(
            "user_id",
            "valid_from",
        ).show(30, truncate=False)

        print(
            "Количество строк в SCD2-таблице:",
            df_scd2_result.count()
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
