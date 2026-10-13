-- Apache Doris 4.1.3 schema. Set the SQL session time zone to UTC.
-- This single-BE example uses one replica. Size buckets and replicas for production.
CREATE DATABASE IF NOT EXISTS metering;

-- Same event_id + same payload_hash is a replay. A different hash remains a variant.
CREATE TABLE IF NOT EXISTS metering.events_raw (
    request_date DATE NOT NULL,
    logical_request_id VARCHAR(36) NOT NULL,
    event_id VARCHAR(42) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    schema_version INT NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    x_request_id VARCHAR(128) NULL,
    observation_id VARCHAR(36) NOT NULL,
    observation_scope VARCHAR(32) NOT NULL,
    producer_id VARCHAR(128) NOT NULL,
    gateway_instance_id VARCHAR(128) NOT NULL,
    config_version VARCHAR(128) NOT NULL,
    adapter_id VARCHAR(64) NOT NULL,
    route_name VARCHAR(128) NOT NULL,
    service_id VARCHAR(128) NOT NULL,
    api_key_id VARCHAR(128) NULL,
    customer_id VARCHAR(128) NULL,
    application_id VARCHAR(128) NULL,
    identity_status VARCHAR(16) NOT NULL,
    identity_hash CHAR(64) NOT NULL,
    request_started_at_ms BIGINT NOT NULL,
    observation_ended_at_ms BIGINT NULL,
    request_finished_at_ms BIGINT NULL,
    event_created_at_ms BIGINT NOT NULL,
    request_model VARCHAR(1024) NULL,
    response_model VARCHAR(1024) NULL,
    http_status SMALLINT NULL,
    upstream_attempt_count INT NULL,
    response_flags STRING NULL,
    response_code_details STRING NULL,
    business_outcome VARCHAR(32) NULL,
    terminal_observed TINYINT NOT NULL,
    observation_status VARCHAR(16) NULL,
    usage_status VARCHAR(16) NOT NULL,
    usage_eligible TINYINT NOT NULL,
    input_tokens BIGINT NULL,
    input_tokens_source VARCHAR(16) NULL,
    output_tokens BIGINT NULL,
    output_tokens_source VARCHAR(16) NULL,
    cached_read_tokens BIGINT NULL,
    cached_read_tokens_source VARCHAR(16) NULL,
    cached_write_tokens BIGINT NULL,
    cached_write_tokens_source VARCHAR(16) NULL,
    reasoning_tokens BIGINT NULL,
    reasoning_tokens_source VARCHAR(16) NULL,
    reported_total_tokens BIGINT NULL,
    reported_total_tokens_source VARCHAR(16) NULL,
    raw_usage_json STRING NOT NULL,
    raw_usage_truncated TINYINT NOT NULL,
    anomaly_codes ARRAY<VARCHAR(64)> NOT NULL,
    event_json STRING NOT NULL,
    kafka_topic VARCHAR(249) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    ingested_at DATETIME(3) NOT NULL
)
UNIQUE KEY(request_date, logical_request_id, event_id, payload_hash)
PARTITION BY RANGE(request_date) ()
DISTRIBUTED BY HASH(logical_request_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.time_zone" = "UTC",
    "dynamic_partition.start" = "-31",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "dynamic_partition.create_history_partition" = "true"
);

-- Retain conflict markers: one variant may expire before another.
CREATE TABLE IF NOT EXISTS metering.request_conflict_ids (
    logical_request_id VARCHAR(36) NOT NULL,
    detected_at DATETIME(3) NOT NULL
)
UNIQUE KEY(logical_request_id)
DISTRIBUTED BY HASH(logical_request_id) BUCKETS 16
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.ingest_rejections (
    kafka_record_date DATE NOT NULL,
    kafka_topic VARCHAR(249) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    reason VARCHAR(64) NOT NULL,
    kafka_record_at DATETIME(3) NOT NULL,
    ingested_at DATETIME(3) NOT NULL
)
UNIQUE KEY(kafka_record_date, kafka_topic, kafka_partition, kafka_offset, payload_hash)
PARTITION BY RANGE(kafka_record_date) ()
DISTRIBUTED BY HASH(kafka_topic, kafka_partition) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.time_zone" = "UTC",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE VIEW IF NOT EXISTS metering.event_variants AS
SELECT * FROM metering.events_raw;

-- Conflict grouping spans all online date partitions.
CREATE VIEW IF NOT EXISTS metering.event_conflicts AS
SELECT logical_request_id, event_id, COUNT(*) AS variant_count,
       ARRAY_AGG(payload_hash) AS payload_hashes
FROM metering.event_variants
GROUP BY logical_request_id, event_id
HAVING COUNT(*) > 1;

CREATE VIEW IF NOT EXISTS metering.events_valid AS
SELECT v.* FROM metering.event_variants v
LEFT JOIN metering.event_conflicts c
  ON v.logical_request_id = c.logical_request_id AND v.event_id = c.event_id
WHERE c.event_id IS NULL;

CREATE VIEW IF NOT EXISTS metering.event_details AS
SELECT request_date, logical_request_id, event_id, payload_hash,
       schema_version, event_type, x_request_id, observation_id,
       observation_scope, producer_id, gateway_instance_id, config_version,
       adapter_id, route_name, service_id, api_key_id, customer_id,
       application_id, identity_status, request_started_at_ms,
       observation_ended_at_ms, request_finished_at_ms, event_created_at_ms,
       request_model, response_model, http_status, upstream_attempt_count,
       response_flags, response_code_details, business_outcome,
       terminal_observed, observation_status, usage_status, usage_eligible,
       input_tokens, input_tokens_source, output_tokens, output_tokens_source,
       cached_read_tokens, cached_read_tokens_source,
       cached_write_tokens, cached_write_tokens_source,
       reasoning_tokens, reasoning_tokens_source,
       reported_total_tokens, reported_total_tokens_source,
       raw_usage_json, raw_usage_truncated, anomaly_codes
FROM metering.events_valid;

CREATE VIEW IF NOT EXISTS metering.requests AS
SELECT a.request_date, a.logical_request_id, a.start_variants, a.usage_variants,
       a.request_started_at_ms, a.x_request_id, a.customer_id, a.service_id,
       CASE WHEN a.usage_variants = 1 THEN a.response_model END AS response_model,
       CASE WHEN a.usage_variants = 0 THEN 'not_observed'
            WHEN a.usage_variants = 1 THEN a.usage_status ELSE 'conflict' END AS usage_status,
       CASE WHEN a.usage_variants = 0 THEN 'not_observed'
            WHEN a.usage_variants = 1 THEN a.observation_status ELSE 'conflict' END AS observation_status,
       CASE WHEN a.usage_variants = 1 THEN a.input_tokens END AS input_tokens,
       CASE WHEN a.usage_variants = 1 THEN a.output_tokens END AS output_tokens,
       CASE WHEN a.usage_variants = 1 THEN a.request_finished_at_ms END AS request_finished_at_ms,
       CASE WHEN a.usage_variants = 1 THEN a.terminal_observed ELSE 0 END AS terminal_observed,
       CASE WHEN a.usage_variants = 1 THEN a.business_outcome END AS business_outcome,
       CASE WHEN a.start_variants > 1 OR a.usage_variants > 1
                 OR a.started_count > 1 OR a.date_count > 1 OR a.identity_count > 1
                 OR m.logical_request_id IS NOT NULL THEN 1 ELSE 0 END AS has_conflict
FROM (
    SELECT logical_request_id, MIN(request_date) AS request_date,
           SUM(CASE WHEN event_type = 'request_started' THEN 1 ELSE 0 END) AS start_variants,
           SUM(CASE WHEN event_type = 'usage_observed' THEN 1 ELSE 0 END) AS usage_variants,
           COUNT(DISTINCT request_started_at_ms) AS started_count,
           COUNT(DISTINCT request_date) AS date_count,
           COUNT(DISTINCT identity_hash) AS identity_count,
           MIN(request_started_at_ms) AS request_started_at_ms,
           MAX(x_request_id) AS x_request_id,
           MAX(customer_id) AS customer_id,
           MAX(service_id) AS service_id,
           MAX(CASE WHEN event_type = 'usage_observed' THEN response_model END) AS response_model,
           MAX(CASE WHEN event_type = 'usage_observed' THEN usage_status END) AS usage_status,
           MAX(CASE WHEN event_type = 'usage_observed' THEN observation_status END) AS observation_status,
           MAX(CASE WHEN event_type = 'usage_observed' THEN input_tokens END) AS input_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN output_tokens END) AS output_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN request_finished_at_ms END) AS request_finished_at_ms,
           MAX(CASE WHEN event_type = 'usage_observed' THEN terminal_observed END) AS terminal_observed,
           MAX(CASE WHEN event_type = 'usage_observed' THEN business_outcome END) AS business_outcome
    FROM metering.event_variants
    GROUP BY logical_request_id
) a
LEFT JOIN metering.request_conflict_ids m ON a.logical_request_id = m.logical_request_id;

CREATE VIEW IF NOT EXISTS metering.usage_effective AS
SELECT d.* FROM metering.event_details d
JOIN metering.requests r ON d.logical_request_id = r.logical_request_id
WHERE d.event_type = 'usage_observed' AND d.usage_eligible = 1 AND r.has_conflict = 0;
