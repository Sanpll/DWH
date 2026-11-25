"""Upload the selected dataset's content catalogue to the S3 source area."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        default=os.getenv("CATALOGUE_DATASET", "fixture"),
        choices=("fixture", "demo"),
    )
    args = parser.parse_args()
    source = Path("/opt/airflow/data") / args.dataset / "catalogue.jsonl"
    if not source.is_file():
        raise SystemExit(f"Catalogue file not found: {source}")

    client = boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    bucket = os.environ["S3_BUCKET"]
    key = f"source/{args.dataset}/catalogue.jsonl"
    client.upload_file(str(source), bucket, key)
    print(f"Uploaded {source} to s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
