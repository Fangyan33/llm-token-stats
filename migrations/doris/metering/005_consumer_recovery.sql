-- Consumer recovery facts are immutable and retained independently of raw dates.
-- Multiple incompatible origins are a blocking conflict, never last-write-wins.
CREATE TABLE IF NOT EXISTS metering.consumer_partition_origins (
 group_id VARCHAR(128) NOT NULL COMMENT 'Kafka Consumer 消费组标识，用于隔离各组的可信起点及缺口事实',
 kafka_topic VARCHAR(249) NOT NULL COMMENT '事件或处理证据所属的 Kafka topic 名称',
 kafka_partition INT NOT NULL COMMENT 'Kafka 分区编号，offset 只在同一 topic 和分区内比较',
 origin_id CHAR(64) NOT NULL COMMENT '消费组、topic、分区及可信起点等固定事实的规范 SHA-256',
 initial_offset BIGINT NOT NULL COMMENT '本消费组分区的可信初始 offset，新组取最早可用位置，已有组需显式接管',
 created_at DATETIME(3) NOT NULL COMMENT '本条恢复事实或通知的 UTC 创建审计时间，不参与稳定标识生成'
) UNIQUE KEY(group_id,kafka_topic,kafka_partition,origin_id)
COMMENT '消费分区可信起点表；作为连续消费水位和恢复核验的基准'
DISTRIBUTED BY HASH(kafka_topic,kafka_partition) BUCKETS 4
PROPERTIES ("replication_num"="1","enable_unique_key_merge_on_write"="true");
CREATE TABLE IF NOT EXISTS metering.consumer_partition_gaps (
 group_id VARCHAR(128) NOT NULL COMMENT 'Kafka Consumer 消费组标识，用于隔离各组的可信起点及缺口事实',
 kafka_topic VARCHAR(249) NOT NULL COMMENT '事件或处理证据所属的 Kafka topic 名称',
 kafka_partition INT NOT NULL COMMENT 'Kafka 分区编号，offset 只在同一 topic 和分区内比较',
 gap_id CHAR(64) NOT NULL COMMENT '消费组分区缺口固定事实的规范 SHA-256，实现同一缺口幂等记录',
 first_offset BIGINT NOT NULL COMMENT '所记录 Kafka 分区区间的起始 offset，包含该位置',
 next_offset BIGINT NOT NULL COMMENT '缺口区间的下一 offset，不包含该位置；与 first_offset 组成左闭右开区间',
 reason VARCHAR(64) NOT NULL COMMENT '消费缺口固定原因码；恢复新流量不清除此事实或将历史覆盖变为可信',
 detected_at DATETIME(3) NOT NULL COMMENT 'Consumer 发现冲突或消费缺口的 UTC 时间'
) UNIQUE KEY(group_id,kafka_topic,kafka_partition,gap_id)
COMMENT '消费分区缺口事实表；持久记录无法证明完整覆盖的 offset 区间'
DISTRIBUTED BY HASH(kafka_topic,kafka_partition) BUCKETS 4
PROPERTIES ("replication_num"="1","enable_unique_key_merge_on_write"="true");
