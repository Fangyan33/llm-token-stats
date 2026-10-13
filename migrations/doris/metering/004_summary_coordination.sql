-- Planned collection/summary coordination for Apache Doris 4.1.3.
-- See docs/doris-summary-coordination.md; this does not start a Worker.
-- Consumer owns notifications/receipts; one protected publisher owns runs/state.
-- These are application protocols, not database locks, FK constraints or DDL transactions.
-- One replica / four buckets are development examples; validate production capacity.
-- Metadata retention is controlled separately from raw/summary data partition retention.

CREATE TABLE IF NOT EXISTS metering.summary_dirty_notifications (
    stat_date DATE NOT NULL,
    notification_id CHAR(64) NOT NULL,
    reason VARCHAR(32) NOT NULL,
    source_id VARCHAR(512) NOT NULL,
    evidence_hash CHAR(64) NOT NULL,
    kafka_topic VARCHAR(249) NULL,
    kafka_partition INT NULL,
    first_offset BIGINT NULL,
    last_offset BIGINT NULL,
    created_at DATETIME(3) NOT NULL
)
UNIQUE KEY(stat_date, notification_id)
DISTRIBUTED BY HASH(stat_date, notification_id) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.consumer_batch_receipts (
    kafka_topic VARCHAR(249) NOT NULL,
    kafka_partition INT NOT NULL,
    first_offset BIGINT NOT NULL,
    last_offset BIGINT NOT NULL,
    batch_id CHAR(64) NOT NULL,
    next_offset BIGINT NOT NULL,
    notification_count BIGINT NOT NULL,
    notification_digest CHAR(64) NOT NULL,
    confirmed_at DATETIME(3) NOT NULL
)
UNIQUE KEY(kafka_topic, kafka_partition, first_offset, last_offset, batch_id)
DISTRIBUTED BY HASH(kafka_topic, kafka_partition) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.summary_refresh_inputs (
    period_type VARCHAR(8) NOT NULL,
    period_start DATE NOT NULL,
    refresh_id VARCHAR(36) NOT NULL,
    notification_id CHAR(64) NOT NULL,
    captured_at DATETIME(3) NOT NULL
)
UNIQUE KEY(period_type, period_start, refresh_id, notification_id)
DISTRIBUTED BY HASH(period_type, period_start) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.summary_refresh_runs (
    period_type VARCHAR(8) NOT NULL,
    period_start DATE NOT NULL,
    refresh_id VARCHAR(36) NOT NULL,
    status VARCHAR(16) NOT NULL,
    publisher_token VARCHAR(128) NOT NULL,
    input_count BIGINT NOT NULL,
    input_digest CHAR(64) NOT NULL,
    source_watermark CHAR(64) NOT NULL,
    source_watermarks_json STRING NOT NULL,
    daily_versions_json STRING NULL,
    old_partition_id BIGINT NULL,
    temp_partition_id BIGINT NULL,
    formal_partition_id BIGINT NULL,
    temp_partition_name VARCHAR(64) NULL,
    expected_row_count BIGINT NULL,
    validation_digest CHAR(64) NULL,
    error_code VARCHAR(64) NULL,
    started_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    finished_at DATETIME(3) NULL
)
UNIQUE KEY(period_type, period_start, refresh_id)
DISTRIBUTED BY HASH(period_type, period_start) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.summary_refresh_state (
    period_type VARCHAR(8) NOT NULL,
    period_start DATE NOT NULL,
    state_version BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL,
    active_refresh_id VARCHAR(36) NULL,
    publication_uncertain BOOLEAN NOT NULL,
    published_refresh_id VARCHAR(36) NULL,
    published_version BIGINT NULL,
    source_watermark CHAR(64) NULL,
    source_watermarks_json STRING NULL,
    daily_versions_json STRING NULL,
    formal_partition_id BIGINT NULL,
    coverage_start DATE NULL,
    coverage_end DATE NULL,
    error_code VARCHAR(64) NULL,
    refreshed_at DATETIME(3) NULL,
    updated_at DATETIME(3) NOT NULL
)
UNIQUE KEY(period_type, period_start)
DISTRIBUTED BY HASH(period_type, period_start) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
