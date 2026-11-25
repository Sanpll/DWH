CREATE DATABASE IF NOT EXISTS cinema;

DROP VIEW IF EXISTS cinema.demo_events_kafka_to_raw;
DROP VIEW IF EXISTS cinema.demo_profile_changes_kafka_to_raw;
DROP TABLE IF EXISTS cinema.demo_events_kafka;
DROP TABLE IF EXISTS cinema.demo_profile_changes_kafka;

CREATE TABLE IF NOT EXISTS cinema.events_kafka
(
    event_id String,
    event_time String,
    user_id UInt64,
    event_type LowCardinality(String),
    content_id Nullable(Int64),
    watch_time_seconds Nullable(UInt32),
    query Nullable(String),
    available_at String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'cinema.events',
    kafka_group_name = 'clickhouse_events_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 0;

CREATE TABLE IF NOT EXISTS cinema.events_raw
(
    dataset LowCardinality(String) DEFAULT 'fixture',
    event_id String,
    event_time DateTime64(0, 'UTC'),
    user_id UInt64,
    event_type LowCardinality(String),
    content_id Nullable(Int64),
    watch_time_seconds Nullable(UInt32),
    query Nullable(String),
    available_at DateTime64(0, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, event_id);

ALTER TABLE cinema.events_raw ADD COLUMN IF NOT EXISTS dataset LowCardinality(String) DEFAULT 'fixture' FIRST;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.events_kafka_to_raw
TO cinema.events_raw
AS SELECT
    event_id, parseDateTime64BestEffort(event_time, 0, 'UTC') AS event_time,
    user_id, event_type, content_id, watch_time_seconds, query,
    parseDateTime64BestEffort(available_at, 0, 'UTC') AS available_at,
    now64(3) AS ingested_at
FROM cinema.events_kafka;

CREATE TABLE IF NOT EXISTS cinema.profile_changes_kafka
(
    change_id String,
    user_id UInt64,
    effective_at String,
    country LowCardinality(String),
    subscription_tier LowCardinality(String),
    available_at String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'cinema.profile_changes',
    kafka_group_name = 'clickhouse_profile_changes_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 0;

CREATE TABLE IF NOT EXISTS cinema.profile_changes_raw
(
    dataset LowCardinality(String) DEFAULT 'fixture',
    change_id String,
    user_id UInt64,
    effective_at DateTime64(0, 'UTC'),
    country LowCardinality(String),
    subscription_tier LowCardinality(String),
    available_at DateTime64(0, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(effective_at)
ORDER BY (effective_at, change_id);

ALTER TABLE cinema.profile_changes_raw ADD COLUMN IF NOT EXISTS dataset LowCardinality(String) DEFAULT 'fixture' FIRST;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.profile_changes_kafka_to_raw
TO cinema.profile_changes_raw
AS SELECT
    change_id, user_id,
    parseDateTime64BestEffort(effective_at, 0, 'UTC') AS effective_at,
    country, subscription_tier,
    parseDateTime64BestEffort(available_at, 0, 'UTC') AS available_at,
    now64(3) AS ingested_at
FROM cinema.profile_changes_kafka;

