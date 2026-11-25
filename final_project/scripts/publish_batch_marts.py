"""Publish Spark Parquet marts from S3 into ClickHouse."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


TABLES = {
    "minute_metrics": ("cinema.batch_minute_metrics",
        "minute, content_id, view_count, watch_time_seconds"),
    "daily_profile_metrics": ("cinema.batch_daily_profile_metrics",
        "event_date, country, subscription_tier, view_count, watch_time_seconds"),
    "daily_top5": ("cinema.batch_daily_top5", "event_date, content_id, view_count, rank"),
    "search_word_frequencies": ("cinema.batch_search_word_frequencies",
        "event_date, word, search_count"),
}


def post(url: str, user: str, password: str, sql: str) -> None:
    query = urlencode({"user": user, "password": password})
    request = Request(f"{url}/?{query}", data=sql.encode(), method="POST")
    try:
        with urlopen(request, timeout=120) as response:
            response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse rejected SQL ({exc.code}): {detail}\nQuery: {sql[:300]}") from exc


def query_text(url: str, user: str, password: str, sql: str) -> str:
    query = urlencode({"user": user, "password": password})
    request = Request(f"{url}/?{query}", data=sql.encode(), method="POST")
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8").strip()


def main() -> None:
    url = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123").rstrip("/")
    user, password = os.getenv("CLICKHOUSE_USER", "cinema"), os.getenv("CLICKHOUSE_PASSWORD", "")
    bucket = os.getenv("S3_BUCKET", "cinema")
    dataset = os.getenv("DATASET", "fixture")
    start = os.getenv("DATA_INTERVAL_START")
    end = os.getenv("DATA_INTERVAL_END")
    period = start[:10] if start else "all"
    dag_id = os.getenv("DAG_ID", "manual")
    task_id = os.getenv("TASK_ID", "publish_batch_marts")
    run_id = os.getenv("RUN_ID", f"manual-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    access_key, secret_key = os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]

    for ddl_path in (Path("/opt/airflow/sql/02_batch_marts.sql"),
                     Path("/opt/airflow/sql/03_unified_report.sql")):
        for statement in (part.strip() for part in ddl_path.read_text(encoding="utf-8").split(";")):
            if statement:
                post(url, user, password, statement)

    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone(timezone.utc) if start else None
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")).astimezone(timezone.utc) if end else None
    if start_dt and end_dt:
        if end_dt <= start_dt:
            raise SystemExit("Data interval end must be later than its start")
        if any((value.second or value.microsecond) for value in (start_dt, end_dt)):
            raise SystemExit("Data interval boundaries must align to whole minutes")
        current_text = query_text(url, user, password,
            "SELECT toString(argMax(cutover_time, updated_at)) FROM cinema.report_cutover "
            "WHERE report_name='views_minute'")
        current_dt = (datetime.strptime(current_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                      if current_text else None)
        if current_dt is None or current_dt == datetime(1970, 1, 1, tzinfo=timezone.utc):
            # On an empty installation the first successful batch interval
            # establishes the initial boundary; subsequent intervals must be contiguous.
            post(url, user, password,
                "INSERT INTO cinema.report_cutover (report_name, cutover_time) "
                f"VALUES ('views_minute', toDateTime('{start_dt.strftime('%Y-%m-%d %H:%M:%S')}', 'UTC'))")
            current_dt = start_dt
        if start_dt > current_dt:
            raise SystemExit(f"Refusing to publish interval with a gap: start={start_dt}, current T={current_dt}")

    # Existing local tables may predate these audit columns.
    for table, _ in TABLES.values():
        for column, kind in (("_dag_id", "String"), ("_task_id", "String"),
                             ("_data_interval_start", "DateTime('UTC')"),
                             ("_data_interval_end", "DateTime('UTC')")):
            post(url, user, password, f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {kind}")

    for name, (table, columns) in TABLES.items():
        object_path = f"http://rustfs:9000/{bucket}/processed/{dataset}/period={period}/{name}/*.parquet"
        insert = (
            f"INSERT INTO {table} ({columns}, dataset, _dag_id, _run_id, _task_id, "
            f"_data_interval_start, _data_interval_end, _loaded_at) "
            f"SELECT {columns}, dataset, _dag_id, _run_id, _task_id, "
            f"_data_interval_start, _data_interval_end, toDateTime64('{loaded_at}', 3, 'UTC') "
            f"FROM s3('{object_path}', '{access_key}', '{secret_key}', 'Parquet')"
        )
        try:
            post(url, user, password, insert)
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SystemExit(f"Publishing {name} failed ({exc.code}):\n{detail}") from exc
        except URLError as exc:
            raise SystemExit(f"Could not connect to ClickHouse: {exc.reason}") from exc
        print(f"Published {name} from s3://{bucket}/processed/{dataset}/{name}/")

    if start_dt and end_dt:
        if start_dt == current_dt:
            post(url, user, password,
                "INSERT INTO cinema.report_cutover (report_name, cutover_time) "
                f"VALUES ('views_minute', toDateTime('{end_dt.strftime('%Y-%m-%d %H:%M:%S')}', 'UTC'))")
            print(f"Advanced report cutover T to {end_dt.isoformat()}")
        else:
            print(f"Historical interval republished; T remains {current_dt.isoformat()}")


if __name__ == "__main__":
    main()
