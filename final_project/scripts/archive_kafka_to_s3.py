"""Archive one Kafka topic to immutable JSONL objects in S3-compatible storage."""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

import boto3
from confluent_kafka import Consumer, KafkaError


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--prefix", default="raw", help="S3 root prefix for archived topic partitions")
    parser.add_argument("--batch-size", type=int, default=200)
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")

    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        "group.id": args.group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    bucket = os.environ["S3_BUCKET"]
    consumer.subscribe([args.topic])
    prefix = args.prefix.strip("/")
    print(f"Archiving {args.topic} to s3://{bucket}/{prefix}/{args.topic}/", flush=True)
    try:
        while True:
            records = consumer.consume(num_messages=args.batch_size, timeout=1.0)
            batches = defaultdict(list)
            for message in records:
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(str(message.error()))
                batches[(message.topic(), message.partition())].append(message)

            for (topic, partition), messages in batches.items():
                messages.sort(key=lambda msg: msg.offset())
                start, end = messages[0].offset(), messages[-1].offset()
                key = f"{prefix}/{topic}/partition={partition:05d}/offset={start:020d}-{end:020d}.jsonl"
                body = b"\n".join(message.value() for message in messages) + b"\n"
                # The deterministic key makes an upload retry idempotent.
                s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
                print(f"Archived {len(messages)} messages: s3://{bucket}/{key}", flush=True)
            if batches:
                # Commit only after all objects from this poll were uploaded.
                consumer.commit(asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
