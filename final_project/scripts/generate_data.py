#!/usr/bin/env python3
"""Генерация событий онлайн-кинотеатра и изменений профилей пользователей."""
from __future__ import annotations

import argparse
import hashlib
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COUNTRIES = ("RU", "KZ", "BY", "AM")
TIERS = ("basic", "standard", "premium")
QUERIES = (
    "космос приключения", "новые фильмы", "семейная комедия",
    "история путешествия", "детектив сериал", "документальное кино",
    "космос космос", "фантастика приключения", "комедия про друзей",
)


def timestamp(value):
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.microsecond:
        raise ValueError("Время должно содержать часовой пояс и целые секунды")
    return result.astimezone(timezone.utc)


def write_jsonl(path, records):
    with path.open("w", encoding="utf-8", newline="\n") as target:
        for record in records:
            target.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def load_catalogue(path, legacy=False):
    if legacy:
        source = json.loads(path.read_text(encoding="utf-8"))
        # The project interprets the legacy duration field as minutes.
        rows = [
            {"content_id": int(key), "category": value["type"],
             "duration_seconds": int(value["duration"]) * 60}
            for key, value in sorted(source.items(), key=lambda item: int(item[0]))
            if int(value["duration"]) > 0
        ][:200]
    else:
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(rows) < 7 or len({row["content_id"] for row in rows}) != len(rows):
        raise ValueError("В каталоге должно быть не менее 7 уникальных content_id")
    for row in rows:
        if (not isinstance(row["content_id"], int) or row["content_id"] < 0
                or not isinstance(row["duration_seconds"], int) or row["duration_seconds"] <= 0
                or not isinstance(row["category"], str) or not row["category"]):
            raise ValueError("Некорректная строка каталога")
    return rows


class Dataset:
    def __init__(self, start, catalogue, namespace):
        self.start = start
        self.catalogue = catalogue
        self.namespace = namespace
        self.events = []
        self.profiles = []
        self.event_count = 0
        self.change_count = 0

    def at(self, seconds):
        return timestamp(self.start + timedelta(seconds=seconds))

    def profile(self, user, effective, arrival, country, tier):
        self.change_count += 1
        row = {"change_id": f"{self.namespace}-p{self.change_count:08d}",
               "user_id": user, "effective_at": self.at(effective),
               "country": country, "subscription_tier": tier,
               "available_at": self.at(arrival)}
        self.profiles.append(row)
        return row

    def event(self, user, moment, arrival, content=None, watched=None, query=None):
        self.event_count += 1
        row = {"event_id": f"{self.namespace}-e{self.event_count:08d}",
               "event_time": self.at(moment), "user_id": user,
               "event_type": "search" if query is not None else "view",
               "content_id": content, "watch_time_seconds": watched,
               "query": query, "available_at": self.at(arrival)}
        self.events.append(row)
        return row

    def repeat(self, row, arrival, target):
        target.append({**row, "available_at": self.at(arrival)})


def add_control_scenario(data, delivery_days=3):
    day = 86400
    late_arrival = delivery_days * day
    data.profile(1001, 0, 1, "RU", "basic")
    data.profile(1002, 0, 2, "BY", "standard")
    data.profile(1003, 0, 3, "KZ", "basic")
    change = data.profile(1001, day, day + 2, "RU", "premium")
    # A new message with unchanged state, and an actual duplicate delivery.
    data.profile(1001, day + 6 * 3600, day + 6 * 3600 + 2, "RU", "premium")
    data.repeat(change, day + 7 * 3600, data.profiles)
    data.profile(1001, 2 * day, 2 * day + 2, "RU", "basic")
    # Earlier effective time, but delivery after the main data period.
    data.profile(1001, 12 * 3600, late_arrival + 10, "KZ", "basic")
    data.profile(1002, day, day + 3, "AM", "standard")

    moments = [60, 120, 3600, 7200, 10800, 14400, 18000,
               43199, 43200, 43201, 86399, 86400, 86401,
               90000, 100000, 120000, 172799, 172800, 172801]
    first = None
    for index, moment in enumerate(moments):
        content = data.catalogue[index % 7]
        row = data.event(1001, moment, moment + 5, content["content_id"],
                         min(content["duration_seconds"], 60 + index * 11))
        if first is None:
            first = row
    data.repeat(first, 300, data.events)
    content = data.catalogue[0]
    data.event(1001, 13 * 3600, late_arrival + 20, content["content_id"], min(120, content["duration_seconds"]))
    # An old event is delivered again after the initial batch period has closed.
    data.repeat(first, late_arrival + 30, data.events)
    for index, query in enumerate(QUERIES[:6]):
        moment = 200 + index * 15000
        data.event(1002 + index % 2, moment, moment + 3, query=query)


def generate(args):
    start = parse_time(args.start_time)
    if (start.hour, start.minute, start.second) != (0, 0, 0):
        raise ValueError("Начало набора должно приходиться на полночь UTC")
    catalogue = load_catalogue(args.source_catalogue or args.catalogue, bool(args.source_catalogue))
    config = {"schema_version": 1, "mode": args.mode, "start_time": timestamp(start)}
    if args.mode == "demo":
        if args.events < 26 or args.users < 3 or args.days < 3:
            raise ValueError("Для demo нужны events >= 26, users >= 3, days >= 3")
        config.update(seed=args.seed, events=args.events, users=args.users, days=args.days)
    digest = hashlib.sha256(json.dumps(catalogue, sort_keys=True).encode()).hexdigest()
    config["catalogue_sha256"] = digest
    namespace = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:12]
    data = Dataset(start, catalogue, namespace)
    add_control_scenario(data, args.days if args.mode == "demo" else 3)
    if args.mode == "demo":
        rng = random.Random(args.seed)
        horizon = args.days * 86400
        for user in range(1004, 1001 + args.users):
            country, tier = rng.choice(COUNTRIES), rng.choice(TIERS)
            data.profile(user, 0, 1, country, tier)
            for day in range(1, args.days):
                if rng.random() < 0.5:
                    moment = day * 86400 + 3600 + user % 3600
                    country, tier = rng.choice(COUNTRIES), rng.choice(TIERS)
                    arrival = (horizon + rng.randint(60, 3600) if rng.random() < 0.1
                               else moment + rng.randint(1, 60))
                    data.profile(user, moment, arrival, country, tier)
        while data.event_count < args.events:
            moment = rng.randint(60, horizon - 1)
            user = rng.randrange(1001, 1001 + args.users)
            arrival = (horizon + rng.randint(60, 86400) if rng.random() < 0.05
                       else moment + rng.randint(1, 120))
            if rng.random() < 0.25:
                row = data.event(user, moment, arrival, query=rng.choice(QUERIES))
            else:
                # A popular item creates a realistic skew for joins and grouping.
                content = catalogue[0] if rng.random() < 0.35 else rng.choice(catalogue)
                row = data.event(user, moment, arrival, content["content_id"],
                                 rng.randint(1, content["duration_seconds"]))
            if rng.random() < 0.02:
                data.repeat(row, arrival + rng.randint(1, 600), data.events)

    data.events.sort(key=lambda row: (row["available_at"], row["event_id"]))
    data.profiles.sort(key=lambda row: (row["available_at"], row["change_id"]))
    output = args.output
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError("Каталог результата должен быть новым или пустым; существующие данные не перезаписываются")
    output.mkdir(parents=True, exist_ok=True)
    write_jsonl(output / "catalogue.jsonl", catalogue)
    write_jsonl(output / "events.jsonl", data.events)
    write_jsonl(output / "profile_changes.jsonl", data.profiles)
    manifest = {**config, "dataset_id": namespace,
                "event_messages": len(data.events), "unique_events": data.event_count,
                "profile_messages": len(data.profiles), "unique_changes": data.change_count,
                "users": args.users if args.mode == "demo" else 3,
                "catalogue_rows": len(catalogue),
                "suggested_arrival_cutoff": data.at((args.days if args.mode == "demo" else 3) * 86400),
                "event_time_min": min(row["event_time"] for row in data.events),
                "event_time_max": max(row["event_time"] for row in data.events),
                "available_at_max": max(row["available_at"] for row in data.events + data.profiles)}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Создан {output}: {len(data.events)} сообщений событий, {len(data.profiles)} сообщений профилей")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("fixture", "demo"), default="demo")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--catalogue", type=Path, default=ROOT / "data/catalogue.jsonl")
    parser.add_argument("--source-catalogue", type=Path, help="Исходный catalogue.json из ДЗ 2.2 вместо подготовленного каталога")
    parser.add_argument("--start-time", default="2026-01-01T00:00:00Z")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--events", type=int, default=6000, help="Уникальные события demo; повторные доставки добавляются сверху")
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--days", type=int, default=3)
    args = parser.parse_args()
    try:
        generate(args)
    except (ValueError, KeyError, OSError) as error:
        parser.exit(1, f"Ошибка: {error}\n")


if __name__ == "__main__":
    main()
