CREATE TABLE IF NOT EXISTS cinema.report_cutover
(
    report_name LowCardinality(String),
    cutover_time DateTime('UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY report_name;

INSERT INTO cinema.report_cutover (report_name, cutover_time)
SELECT 'views_minute', toDateTime('1970-01-01 00:00:00', 'UTC')
WHERE NOT EXISTS (SELECT 1 FROM cinema.report_cutover WHERE report_name = 'views_minute');

DROP VIEW IF EXISTS cinema.unified_minute_metrics_by_dataset;
DROP VIEW IF EXISTS cinema.unified_minute_metrics;
DROP VIEW IF EXISTS cinema.operational_minute_metrics;

CREATE VIEW cinema.operational_minute_metrics AS
SELECT
    toStartOfMinute(event_time) AS minute,
    content_id,
    count() AS view_count,
    sum(watch_time_seconds) AS watch_time_seconds
FROM
(
    SELECT
        event_id,
        argMax(event_time, ingested_at) AS event_time,
        argMax(event_type, ingested_at) AS event_type,
        argMax(content_id, ingested_at) AS content_id,
        argMax(watch_time_seconds, ingested_at) AS watch_time_seconds
    FROM cinema.events_raw
    GROUP BY event_id
)
WHERE event_type = 'view'
GROUP BY minute, content_id;

CREATE VIEW cinema.unified_minute_metrics AS
WITH (SELECT argMax(cutover_time, updated_at)
      FROM cinema.report_cutover WHERE report_name = 'views_minute') AS cutover
SELECT minute, content_id, view_count, watch_time_seconds
FROM cinema.batch_minute_metrics FINAL
WHERE minute < cutover
UNION ALL
SELECT minute, content_id, view_count, watch_time_seconds
FROM cinema.operational_minute_metrics
WHERE minute >= cutover;
