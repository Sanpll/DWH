"""Create the first streaming tables and materialized views in ClickHouse."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def main() -> None:
    url = os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123").rstrip("/")
    user = os.environ.get("CLICKHOUSE_USER", "cinema")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    sql_file = Path(os.environ.get("CLICKHOUSE_SQL_FILE", "/opt/airflow/sql/01_streaming.sql"))
    sql = sql_file.read_text(encoding="utf-8")
    statements = [part.strip() for part in sql.split(";") if part.strip()]
    credentials = urlencode({"user": user, "password": password})

    for index, statement in enumerate(statements, start=1):
        request = Request(
            f"{url}/?{credentials}", data=statement.encode("utf-8"), method="POST"
        )
        try:
            with urlopen(request, timeout=30) as response:
                response.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SystemExit(
                f"ClickHouse rejected statement {index}/{len(statements)} "
                f"from {sql_file} ({exc.code}):\n{detail}"
            ) from exc
        except URLError as exc:
            raise SystemExit(f"Could not connect to ClickHouse at {url}: {exc.reason}") from exc

    print(f"Applied {len(statements)} statements from {sql_file}")


if __name__ == "__main__":
    main()
