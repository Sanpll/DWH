from datetime import datetime, timezone

from pyspark.sql import functions as F

from jobs.common import (
    create_spark_session, dataset_name, metadata_columns, run_parameters, s3_uri,
)


# функция реализации JOIN
def build_campaign_recipients(**context):
    start, end, calculation_date = run_parameters(context)
    date_text = calculation_date.isoformat()
    task_id = context["task"].task_id
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    dataset = dataset_name()

    spark = create_spark_session("hw23-build-campaign-recipients")
    try:
        scd2_path = s3_uri(
            f"results/{dataset}/user_scd2/snapshot_date={date_text}"
        )
        campaigns_path = s3_uri(f"raw/{dataset}/campaigns.csv")

        scd2 = spark.read.parquet(scd2_path).alias("u")
        campaigns_raw = (
            spark.read.option("header", "true").option("inferSchema", "false")
            .option("mode", "FAILFAST").csv(campaigns_path)
        )
        campaigns = (
            campaigns_raw.select(
                F.col("campaign_id").cast("long").alias("campaign_id"),
                F.col("campaign_name").cast("string").alias("campaign_name"),
                F.to_timestamp(F.trim("send_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("send_at"),
                F.col("email_domain").cast("string").alias("email_domain"),
            )
            .filter(
                (F.col("send_at") >= F.lit(start).cast("timestamp")) &
                (F.col("send_at") < F.lit(end).cast("timestamp"))
            )
            .alias("c")
        )

        invalid_campaigns = campaigns.filter(
            F.col("campaign_id").isNull() | F.col("campaign_name").isNull() |
            F.col("send_at").isNull() | F.col("email_domain").isNull()
        ).limit(1).count()
        if invalid_campaigns:
            raise ValueError("Campaign source contains NULL values or invalid send_at")

        user_domain = F.lower(F.trim(F.split(F.col("u.user_email"), "@").getItem(1)))
        campaign_domain = F.lower(F.trim(F.col("c.email_domain")))
        # главное условие применомости JOIN'а
        condition = (
            (user_domain == campaign_domain) &
            (F.col("u.valid_from") <= F.col("c.send_at")) &
            (F.col("u.valid_to").isNull() | (F.col("c.send_at") < F.col("u.valid_to")))
        )

        joined = scd2.join(F.broadcast(campaigns), condition, "inner")
        recipients = joined.select(
            F.col("c.campaign_id").cast("long").alias("campaign_id"),
            F.col("c.campaign_name").cast("string").alias("campaign_name"),
            F.col("c.send_at").cast("timestamp").alias("send_at"),
            F.col("c.email_domain").cast("string").alias("email_domain"),
            F.col("u.user_id").cast("long").alias("user_id"),
            F.col("u.user_fio").cast("string").alias("user_fio"),
            F.col("u.user_email").cast("string").alias("user_email"),
            F.col("u.user_login").cast("string").alias("user_login"),
            F.col("u.valid_from").cast("timestamp").alias("user_valid_from"),
            F.col("u.valid_to").cast("timestamp").alias("user_valid_to"),
            F.lit(date_text).cast("date").alias("calculation_date"),
            *metadata_columns(F, context, task_id, start, end, loaded_at),
        )

        duplicate_pairs = (
            recipients.groupBy("campaign_id", "user_id").count()
            .filter(F.col("count") > 1).limit(1).count()
        )
        if duplicate_pairs:
            raise AssertionError("Recipients contain duplicate (campaign_id, user_id) pairs")

        target_path = s3_uri(
            f"results/{dataset}/campaign_recipients/calculation_date={date_text}"
        )
        recipients.write.mode("overwrite").parquet(target_path)
        saved_count = spark.read.parquet(target_path).count()

        print("Campaign JOIN physical plan:", flush=True)
        recipients.explain(mode="formatted")
        print(
            f"Campaign recipients saved: date={date_text}, rows={saved_count}, "
            f"path={target_path}, interval=[{start}, {end})",
            flush=True,
        )
        return target_path
    finally:
        spark.stop()
