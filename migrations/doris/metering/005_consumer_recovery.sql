-- Consumer recovery facts are immutable and retained independently of raw dates.
-- Multiple incompatible origins are a blocking conflict, never last-write-wins.
CREATE TABLE IF NOT EXISTS metering.consumer_partition_origins (
 group_id VARCHAR(128) NOT NULL,
 kafka_topic VARCHAR(249) NOT NULL,
 kafka_partition INT NOT NULL,
 origin_id CHAR(64) NOT NULL,
 initial_offset BIGINT NOT NULL,
 created_at DATETIME(3) NOT NULL
) UNIQUE KEY(group_id,kafka_topic,kafka_partition,origin_id)
DISTRIBUTED BY HASH(kafka_topic,kafka_partition) BUCKETS 4
PROPERTIES ("replication_num"="1","enable_unique_key_merge_on_write"="true");
CREATE TABLE IF NOT EXISTS metering.consumer_partition_gaps (
 group_id VARCHAR(128) NOT NULL,
 kafka_topic VARCHAR(249) NOT NULL,
 kafka_partition INT NOT NULL,
 gap_id CHAR(64) NOT NULL,
 first_offset BIGINT NOT NULL,
 next_offset BIGINT NOT NULL,
 reason VARCHAR(64) NOT NULL,
 detected_at DATETIME(3) NOT NULL
) UNIQUE KEY(group_id,kafka_topic,kafka_partition,gap_id)
DISTRIBUTED BY HASH(kafka_topic,kafka_partition) BUCKETS 4
PROPERTIES ("replication_num"="1","enable_unique_key_merge_on_write"="true");
