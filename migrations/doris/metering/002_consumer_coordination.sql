-- Consumer notifications, receipts and recovery facts; no summary publication tables.

SET time_zone = '+00:00';

CREATE TABLE IF NOT EXISTS metering.summary_dirty_notifications (
    stat_date DATE NOT NULL COMMENT '日统计或重算通知对应的 UTC 请求日期',
    notification_id CHAR(64) NOT NULL COMMENT '版本化日期、原因、来源及证据元组的规范 SHA-256，实现重算通知幂等',
    reason VARCHAR(32) NOT NULL COMMENT '日期通知原因：INGEST、CONFLICT、INITIALIZE 或 REPAIR，不使用任意错误文本',
    source_id VARCHAR(512) NOT NULL COMMENT '通知来源标识；INGEST 为冻结批次 batch_id，其他原因按各自稳定来源定义',
    evidence_hash CHAR(64) NOT NULL COMMENT '触发本日期通知的事件或冲突证据集合的规范 SHA-256',
    kafka_topic VARCHAR(249) NULL COMMENT '事件或处理证据所属的 Kafka topic 名称',
    kafka_partition INT NULL COMMENT 'Kafka 分区编号，offset 只在同一 topic 和分区内比较',
    first_offset BIGINT NULL COMMENT '所记录 Kafka 分区区间的起始 offset，包含该位置',
    last_offset BIGINT NULL COMMENT '所记录 Kafka 分区批次的最后 offset，包含该位置',
    created_at DATETIME(3) NOT NULL COMMENT '本条恢复事实或通知的 UTC 创建审计时间，不参与稳定标识生成'
)
UNIQUE KEY(stat_date, notification_id)
COMMENT '日期重算通知表；持久记录新增事件、冲突或修复影响的日期'
DISTRIBUTED BY HASH(stat_date, notification_id) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.consumer_batch_receipts (
    kafka_topic VARCHAR(249) NOT NULL COMMENT '事件或处理证据所属的 Kafka topic 名称',
    kafka_partition INT NOT NULL COMMENT 'Kafka 分区编号，offset 只在同一 topic 和分区内比较',
    first_offset BIGINT NOT NULL COMMENT '所记录 Kafka 分区区间的起始 offset，包含该位置',
    last_offset BIGINT NOT NULL COMMENT '所记录 Kafka 分区批次的最后 offset，包含该位置',
    batch_id CHAR(64) NOT NULL COMMENT '冻结 Kafka 坐标范围及按 offset 排序的消息摘要和校验分类元组的规范 SHA-256',
    next_offset BIGINT NOT NULL COMMENT '本批最后 offset 加 1；凭据确认可领先于 Kafka 提交，不能直接当连续已提交水位',
    notification_count BIGINT NOT NULL COMMENT '该批次实际登记并确认可见的日期通知集合大小，可以为 0',
    notification_digest CHAR(64) NOT NULL COMMENT '该批次已确认通知集合按日期和通知 ID 排序后的规范 SHA-256',
    confirmed_at DATETIME(3) NOT NULL COMMENT '批次冻结时记录的 UTC 审计时间，重试沿用；不代表写入确认完成或 Kafka offset 提交的时间'
)
UNIQUE KEY(kafka_topic, kafka_partition, first_offset, last_offset, batch_id)
COMMENT '消费批次处理凭据表；证明入库、冲突维护和通知登记已确认可见'
DISTRIBUTED BY HASH(kafka_topic, kafka_partition) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

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
