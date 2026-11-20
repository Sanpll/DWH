from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from jobs.campaign_recipients_job import build_campaign_recipients
from jobs.scd2_job import build_user_scd2

UTC = pendulum.timezone("UTC")

with DAG(
        dag_id="user_campaign_scd2_daily",
        description="Daily SCD2 user snapshots and historical campaign recipient JOIN",
        # ежедневное расписание в 00:00 по UTC
        timetable=CronDataIntervalTimetable("0 0 * * *", timezone=UTC),
        start_date=pendulum.datetime(2026, 1, 23, 0, 0, tz=UTC),
        end_date=pendulum.datetime(2026, 1, 25, 0, 0, tz=UTC),
        # Airflow может автоматическисоздавать исторические запуски за указанный период
        catchup=True,
        # разрешаем только 1 одновременный запуск этого DAG
        max_active_runs=1,
        default_args={
            "owner": "student",
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=2),
        },
        tags=["hw2.3", "scd2", "spark", "s3"],
) as dag:
    build_user_scd2_task = PythonOperator(
        task_id="build_user_scd2",
        python_callable=build_user_scd2,
    )
    build_campaign_recipients_task = PythonOperator(
        task_id="build_campaign_recipients",
        python_callable=build_campaign_recipients,
    )

    # сначала выполняется build_user_scd2_task,
    # и только после него - build_campaign_recipients_task
    build_user_scd2_task >> build_campaign_recipients_task
