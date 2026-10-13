-- Phase 1 summary schema for Apache Doris 4.1.3; this does not schedule refresh jobs.
-- Summary partition creation / expiration is owned by the planned usage-worker.
-- Daily: 730 UTC dates including today. Monthly: 24 UTC months including this month.
-- Initial historical summary partitions are created by a controlled backfill job.
-- One-replica / eight-bucket values are development examples.
CREATE TABLE IF NOT EXISTS metering.usage_daily (
    stat_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    application_id VARCHAR(128) NOT NULL,
    api_key_id VARCHAR(128) NOT NULL,
    service_id VARCHAR(128) NOT NULL,
    model VARCHAR(1024) NOT NULL,
    usage_source VARCHAR(32) NOT NULL,
    request_count BIGINT NOT NULL,
    started_request_count BIGINT NOT NULL,
    usage_observed_request_count BIGINT NOT NULL,
    usage_missing_request_count BIGINT NOT NULL,
    eligible_usage_request_count BIGINT NOT NULL,
    ineligible_usage_request_count BIGINT NOT NULL,
    http_2xx_request_count BIGINT NOT NULL,
    http_error_request_count BIGINT NOT NULL,
    terminal_missing_request_count BIGINT NOT NULL,
    input_tokens DECIMAL(38, 0) NULL,
    output_tokens DECIMAL(38, 0) NULL,
    total_tokens DECIMAL(38, 0) NULL,
    cached_read_tokens DECIMAL(38, 0) NULL,
    cached_write_tokens DECIMAL(38, 0) NULL,
    reasoning_tokens DECIMAL(38, 0) NULL,
    reported_total_tokens DECIMAL(38, 0) NULL,
    cached_read_known_requests BIGINT NOT NULL,
    cached_write_known_requests BIGINT NOT NULL,
    reasoning_known_requests BIGINT NOT NULL,
    reported_total_known_requests BIGINT NOT NULL,
    refresh_id VARCHAR(36) NOT NULL,
    source_watermark CHAR(64) NOT NULL,
    refreshed_at DATETIME(3) NOT NULL
)
UNIQUE KEY(stat_date, customer_id, application_id, api_key_id, service_id, model, usage_source)
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(customer_id, api_key_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS metering.usage_monthly (
    stat_month DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    application_id VARCHAR(128) NOT NULL,
    api_key_id VARCHAR(128) NOT NULL,
    service_id VARCHAR(128) NOT NULL,
    model VARCHAR(1024) NOT NULL,
    usage_source VARCHAR(32) NOT NULL,
    request_count BIGINT NOT NULL,
    started_request_count BIGINT NOT NULL,
    usage_observed_request_count BIGINT NOT NULL,
    usage_missing_request_count BIGINT NOT NULL,
    eligible_usage_request_count BIGINT NOT NULL,
    ineligible_usage_request_count BIGINT NOT NULL,
    http_2xx_request_count BIGINT NOT NULL,
    http_error_request_count BIGINT NOT NULL,
    terminal_missing_request_count BIGINT NOT NULL,
    input_tokens DECIMAL(38, 0) NULL,
    output_tokens DECIMAL(38, 0) NULL,
    total_tokens DECIMAL(38, 0) NULL,
    cached_read_tokens DECIMAL(38, 0) NULL,
    cached_write_tokens DECIMAL(38, 0) NULL,
    reasoning_tokens DECIMAL(38, 0) NULL,
    reported_total_tokens DECIMAL(38, 0) NULL,
    cached_read_known_requests BIGINT NOT NULL,
    cached_write_known_requests BIGINT NOT NULL,
    reasoning_known_requests BIGINT NOT NULL,
    reported_total_known_requests BIGINT NOT NULL,
    refresh_id VARCHAR(36) NOT NULL,
    source_watermark CHAR(64) NOT NULL,
    refreshed_at DATETIME(3) NOT NULL
)
UNIQUE KEY(stat_month, customer_id, application_id, api_key_id, service_id, model, usage_source)
PARTITION BY RANGE(stat_month) ()
DISTRIBUTED BY HASH(customer_id, api_key_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- Per-date request projection; predicate on request_date must prune raw partitions.
-- The Consumer must persist cross-date identity/time/event conflicts in request_conflict_ids
-- before publishing dirty dates / committing the corresponding Kafka offset.
-- Conflicted and invalid identities are excluded by refresh SQL, before tenant grouping.
CREATE VIEW IF NOT EXISTS metering.request_statistics AS
SELECT a.*,
       CASE WHEN a.start_variants > 1 OR a.usage_variants > 1
                 OR a.started_count > 1 OR a.identity_count > 1
                 OR c.logical_request_id IS NOT NULL THEN 1 ELSE 0 END AS has_conflict,
       COALESCE(NULLIF(a.response_model, ''), NULLIF(a.request_model, ''), '') AS model,
       CASE WHEN a.usage_variants = 0 THEN 'missing'
            WHEN a.input_tokens IS NULL OR a.output_tokens IS NULL THEN 'unknown'
            WHEN a.input_tokens_source = 'reported' AND a.output_tokens_source = 'reported' THEN 'reported'
            WHEN a.input_tokens_source = 'estimated_model' AND a.output_tokens_source = 'estimated_model' THEN 'estimated_model'
            WHEN a.input_tokens_source = 'estimated_generic' AND a.output_tokens_source = 'estimated_generic' THEN 'estimated_generic'
            ELSE 'mixed' END AS usage_source
FROM (
    SELECT request_date, logical_request_id,
           SUM(CASE WHEN event_type = 'request_started' THEN 1 ELSE 0 END) AS start_variants,
           SUM(CASE WHEN event_type = 'usage_observed' THEN 1 ELSE 0 END) AS usage_variants,
           COUNT(DISTINCT request_started_at_ms) AS started_count,
           COUNT(DISTINCT identity_hash) AS identity_count,
           MAX(customer_id) AS customer_id, MAX(application_id) AS application_id,
           MAX(api_key_id) AS api_key_id, MAX(service_id) AS service_id,
           MAX(identity_status) AS identity_status,
           MAX(request_model) AS request_model,
           MAX(CASE WHEN event_type = 'usage_observed' THEN response_model END) AS response_model,
           MAX(CASE WHEN event_type = 'usage_observed' THEN usage_eligible END) AS usage_eligible,
           MAX(CASE WHEN event_type = 'usage_observed' THEN http_status END) AS http_status,
           MAX(CASE WHEN event_type = 'usage_observed' THEN terminal_observed END) AS terminal_observed,
           MAX(CASE WHEN event_type = 'usage_observed' THEN input_tokens END) AS input_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN output_tokens END) AS output_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN input_tokens_source END) AS input_tokens_source,
           MAX(CASE WHEN event_type = 'usage_observed' THEN output_tokens_source END) AS output_tokens_source,
           MAX(CASE WHEN event_type = 'usage_observed' THEN cached_read_tokens END) AS cached_read_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN cached_write_tokens END) AS cached_write_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN reasoning_tokens END) AS reasoning_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN reported_total_tokens END) AS reported_total_tokens
    FROM metering.events_raw
    GROUP BY request_date, logical_request_id
) a
LEFT JOIN metering.request_conflict_ids c ON a.logical_request_id = c.logical_request_id;
