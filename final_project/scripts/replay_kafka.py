#!/usr/bin/env python3
"""Отправка подготовленных событий и изменений профилей в два топика Kafka."""
from __future__ import annotations

import argparse
import heapq
import json
import math
import time
from pathlib import Path

from generate_data import parse_time, timestamp


def read_stream(path, topic):
    previous = ""
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            row = json.loads(line)
            arrival = timestamp(parse_time(row["available_at"]))
            if arrival < previous:
                raise ValueError(f"{path}:{line_number}: available_at должен идти по возрастанию")
            previous = arrival
            yield arrival, topic, row


def messages(args):
    streams = [read_stream(args.dataset / "events.jsonl", args.events_topic),
               read_stream(args.dataset / "profile_changes.jsonl", args.profiles_topic)]
    # Heap merge preserves arrival order while reading only one row per stream.
    for arrival, topic, row in heapq.merge(*streams, key=lambda item: item[0]):
        if args.from_arrival and arrival < args.from_arrival:
            continue
        if args.until_arrival and arrival >= args.until_arrival:
            break
        yield topic, row


def replay(args, producer_factory=None):
    if args.dry_run:
        counts = {args.events_topic: 0, args.profiles_topic: 0}
        for topic, row in messages(args):
            counts[topic] += 1
        print(json.dumps({"dry_run": True, "messages": counts}, ensure_ascii=False))
        return
    if producer_factory is None:
        from confluent_kafka import Producer
        producer_factory = Producer
    config = json.loads(args.client_config.read_text(encoding="utf-8")) if args.client_config else {}
    config.update({"bootstrap.servers": args.bootstrap_servers,
                   "enable.idempotence": True, "acks": "all", "message.timeout.ms": 30000})
    producer = producer_factory(config)
    delivered, failed, queued = 0, 0, 0

    def on_delivery(error, message):
        nonlocal delivered, failed
        if error is not None:
            failed += 1
        else:
            delivered += 1

    next_send = time.monotonic()
    try:
        for topic, row in messages(args):
            if failed:
                raise RuntimeError("Kafka сообщила об ошибке доставки; отправка остановлена")
            if args.rate:
                delay = next_send - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                next_send = max(next_send, time.monotonic()) + 1 / args.rate
            value = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            deadline = time.monotonic() + 30
            while True:
                try:
                    producer.produce(topic, key=str(row["user_id"]).encode(),
                                     value=value, on_delivery=on_delivery)
                    queued += 1
                    break
                except BufferError:
                    producer.poll(0.1)
                    if failed or time.monotonic() >= deadline:
                        raise RuntimeError("Не удалось освободить очередь Kafka producer")
            producer.poll(0)
    finally:
        remaining = producer.flush(35)
        print(f"Поставлено в очередь: {queued}; подтверждено: {delivered}; ошибки: {failed}; не завершено: {remaining}")
    if failed or remaining or delivered != queued:
        raise RuntimeError("Не все сообщения подтверждены Kafka; запуск не считается успешным")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--events-topic", default="cinema.events")
    parser.add_argument("--profiles-topic", default="cinema.profile_changes")
    parser.add_argument("--rate", type=float, default=100, help="Общий лимит сообщений/сек; 0 — без ограничения")
    parser.add_argument("--from-arrival", help="Включительная нижняя граница available_at")
    parser.add_argument("--until-arrival", help="Исключительная верхняя граница available_at")
    parser.add_argument("--client-config", type=Path, help="JSON с дополнительными настройками клиента, например SASL/SSL")
    parser.add_argument("--dry-run", action="store_true", help="Проверить чтение и показать количество сообщений без Kafka и без зависимости confluent-kafka")
    args = parser.parse_args()
    try:
        if not math.isfinite(args.rate) or args.rate < 0:
            raise ValueError("rate должен быть конечным неотрицательным числом")
        if not args.events_topic or not args.profiles_topic or args.events_topic == args.profiles_topic:
            raise ValueError("Нужны два разных непустых имени топиков")
        for name in ("from_arrival", "until_arrival"):
            value = getattr(args, name)
            if value:
                setattr(args, name, timestamp(parse_time(value)))
        if args.from_arrival and args.until_arrival and args.from_arrival >= args.until_arrival:
            raise ValueError("from-arrival должен быть меньше until-arrival")
        replay(args)
    except ImportError:
        parser.exit(1, "Для отправки установите зависимости из scripts/requirements.txt\n")
    except (ValueError, KeyError, OSError, RuntimeError) as error:
        parser.exit(1, f"Ошибка: {error}\n")


if __name__ == "__main__":
    main()
