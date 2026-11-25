CREATE TABLE IF NOT EXISTS cinema.batch_minute_metrics
(
    minute DateTime('UTC'),
    content_id Int64,
    view_count UInt64,
    watch_time_seconds UInt64,
    dataset LowCardinality(String),
    _dag_id String,
    _run_id String,
    _task_id String,
    _data_interval_start DateTime('UTC'),
    _data_interval_end DateTime('UTC'),
    _loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, content_id, dataset);

CREATE TABLE IF NOT EXISTS cinema.batch_daily_profile_metrics
(
    event_date Date,
    country LowCardinality(String),
    subscription_tier LowCardinality(String),
    view_count UInt64,
    watch_time_seconds UInt64,
    dataset LowCardinality(String),
    _dag_id String,
    _run_id String,
    _task_id String,
    _data_interval_start DateTime('UTC'),
    _data_interval_end DateTime('UTC'),
    _loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, country, subscription_tier, dataset);

CREATE TABLE IF NOT EXISTS cinema.batch_daily_top5
(
    event_date Date,
    content_id Int64,
    view_count UInt64,
    rank UInt8,
    dataset LowCardinality(String),
    _dag_id String,
    _run_id String,
    _task_id String,
    _data_interval_start DateTime('UTC'),
    _data_interval_end DateTime('UTC'),
    _loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, content_id, dataset);

CREATE TABLE IF NOT EXISTS cinema.batch_search_word_frequencies
(
    event_date Date,
    word String,
    search_count UInt64,
    dataset LowCardinality(String),
    _dag_id String,
    _run_id String,
    _task_id String,
    _data_interval_start DateTime('UTC'),
    _data_interval_end DateTime('UTC'),
    _loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, word, dataset);
