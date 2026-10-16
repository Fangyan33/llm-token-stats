-- Planned collection/summary coordination for Apache Doris 4.1.3.
-- See docs/doris-summary-coordination.md; this does not start a Worker.
-- Consumer owns notifications/receipts; one protected publisher owns runs/state.
-- These are application protocols, not database locks, FK constraints or DDL transactions.
-- One replica / four buckets are development examples; validate production capacity.
-- Metadata retention is controlled separately from raw/summary data partition retention.

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
    confirmed_at DATETIME(3) NOT NULL COMMENT '批次必要写入全部确认可见时的 UTC 时间；Kafka offset 可能尚未提交'
)
UNIQUE KEY(kafka_topic, kafka_partition, first_offset, last_offset, batch_id)
COMMENT '消费批次处理凭据表；证明入库、冲突维护和通知登记已确认可见'
DISTRIBUTED BY HASH(kafka_topic, kafka_partition) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.summary_refresh_inputs (
    period_type VARCHAR(8) NOT NULL COMMENT '统计期类型，设计预留 DAY 或 MONTH，结构由未来 Worker 校验',
    period_start DATE NOT NULL COMMENT '统计期 UTC 起始日期；DAY 为当天，MONTH 为自然月首，等待 Worker 实现',
    refresh_id VARCHAR(36) NOT NULL COMMENT '生成本次汇总结果或执行记录的刷新 UUID；Worker 发布版本关联标识',
    notification_id CHAR(64) NOT NULL COMMENT '版本化日期、原因、来源及证据元组的规范 SHA-256，实现重算通知幂等',
    captured_at DATETIME(3) NOT NULL COMMENT '日任务捕获本条通知作为固定输入的 UTC 时间，等待 Worker 实现'
)
UNIQUE KEY(period_type, period_start, refresh_id, notification_id)
COMMENT '汇总任务输入集合表；保存日任务捕获的精确通知，等待 Worker 实现'
DISTRIBUTED BY HASH(period_type, period_start) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.summary_refresh_runs (
    period_type VARCHAR(8) NOT NULL COMMENT '统计期类型，设计预留 DAY 或 MONTH，结构由未来 Worker 校验',
    period_start DATE NOT NULL COMMENT '统计期 UTC 起始日期；DAY 为当天，MONTH 为自然月首，等待 Worker 实现',
    refresh_id VARCHAR(36) NOT NULL COMMENT '生成本次汇总结果或执行记录的刷新 UUID；Worker 发布版本关联标识',
    status VARCHAR(16) NOT NULL COMMENT '设计预留执行阶段：RUNNING、PREPARED、PUBLISHING、PUBLISHED 或 FAILED；Worker 尚未实现',
    publisher_token VARCHAR(128) NOT NULL COMMENT '受保护单个发布者的身份令牌，等待 Worker 实现；不是 Doris 强制 fencing',
    input_count BIGINT NOT NULL COMMENT '本次捕获的输入项数；日任务为通知数，月任务为日版本项数，等待 Worker 实现',
    input_digest CHAR(64) NOT NULL COMMENT '本次排序后固定输入集合的规范 SHA-256，等待 Worker 实现',
    source_watermark CHAR(64) NOT NULL COMMENT '本次汇总输入及连续分区水位向量的规范 SHA-256，不是全局 Kafka 截止 offset',
    source_watermarks_json STRING NOT NULL COMMENT '本次完整连续分区水位向量 JSON，供来源核验与恢复，等待 Worker 实现',
    daily_versions_json STRING NULL COMMENT '月任务依赖的日发布版本和覆盖向量 JSON；非月任务可为 NULL，等待 Worker 实现',
    old_partition_id BIGINT NULL COMMENT '刷新前正式分区的 Doris PartitionId，首次发布可为 NULL，等待 Worker 实现',
    temp_partition_id BIGINT NULL COMMENT '准备完成的临时分区 Doris PartitionId，用于核验替换结果，等待 Worker 实现',
    formal_partition_id BIGINT NULL COMMENT '当前或发布后正式分区的 Doris PartitionId，尚未确定时为 NULL，等待 Worker 实现',
    temp_partition_name VARCHAR(64) NULL COMMENT '本次任务独有的临时分区名称，不接受客户输入，等待 Worker 实现',
    expected_row_count BIGINT NULL COMMENT '准备完成时核验的预期结果行数，0 是有效空分区，等待 Worker 实现',
    validation_digest CHAR(64) NULL COMMENT '准备或发布结果的校验摘要，待验证时为 NULL，等待 Worker 实现',
    error_code VARCHAR(64) NULL COMMENT '固定刷新或发布错误码，无错误时为 NULL，不保存任意异常文本，等待 Worker 实现',
    started_at DATETIME(3) NOT NULL COMMENT '本次刷新任务开始的 UTC 时间，等待 Worker 实现',
    updated_at DATETIME(3) NOT NULL COMMENT '本条任务或统计期状态最近变更的 UTC 时间，等待 Worker 实现',
    finished_at DATETIME(3) NULL COMMENT '本次刷新任务结束的 UTC 时间，尚未结束为 NULL，等待 Worker 实现'
)
UNIQUE KEY(period_type, period_start, refresh_id)
COMMENT '汇总任务执行审计表；保存阶段、输入与分区校验证据，等待 Worker 实现'
DISTRIBUTED BY HASH(period_type, period_start) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.summary_refresh_state (
    period_type VARCHAR(8) NOT NULL COMMENT '统计期类型，设计预留 DAY 或 MONTH，结构由未来 Worker 校验',
    period_start DATE NOT NULL COMMENT '统计期 UTC 起始日期；DAY 为当天，MONTH 为自然月首，等待 Worker 实现',
    state_version BIGINT NOT NULL COMMENT '统计期状态单调递增版本，每次状态转换递增，不使用墙上时钟，等待 Worker 实现',
    status VARCHAR(16) NOT NULL COMMENT '设计预留统计期发布状态，EXPIRED 表示覆盖已过期；Worker 与查询 API 尚未实现',
    active_refresh_id VARCHAR(36) NULL COMMENT '当前活动刷新任务 UUID，无活动任务时为 NULL，等待 Worker 实现',
    publication_uncertain BOOLEAN NOT NULL COMMENT '正式分区替换结果是否尚未核验；为真时暂停该期成功查询，等待 Worker 实现',
    published_refresh_id VARCHAR(36) NULL COMMENT '最近已确认成功发布的刷新 UUID，未发布时为 NULL，等待 Worker 实现',
    published_version BIGINT NULL COMMENT '最近成功发布对应的 state_version，未发布时为 NULL，等待 Worker 实现',
    source_watermark CHAR(64) NULL COMMENT '本次汇总输入及连续分区水位向量的规范 SHA-256，不是全局 Kafka 截止 offset',
    source_watermarks_json STRING NULL COMMENT '本次完整连续分区水位向量 JSON，供来源核验与恢复，等待 Worker 实现',
    daily_versions_json STRING NULL COMMENT '月任务依赖的日发布版本和覆盖向量 JSON；非月任务可为 NULL，等待 Worker 实现',
    formal_partition_id BIGINT NULL COMMENT '当前或发布后正式分区的 Doris PartitionId，尚未确定时为 NULL，等待 Worker 实现',
    coverage_start DATE NULL COMMENT '已确认覆盖日期区间的 UTC 起点，包含该日期，等待 Worker 实现',
    coverage_end DATE NULL COMMENT '已确认覆盖日期区间的 UTC 终点，不包含该日期且不包含未来日期，等待 Worker 实现',
    error_code VARCHAR(64) NULL COMMENT '固定刷新或发布错误码，无错误时为 NULL，不保存任意异常文本，等待 Worker 实现',
    refreshed_at DATETIME(3) NULL COMMENT '本次汇总刷新或成功发布的 UTC 时间；尚未成功发布时状态表可为 NULL',
    updated_at DATETIME(3) NOT NULL COMMENT '本条任务或统计期状态最近变更的 UTC 时间，等待 Worker 实现'
)
UNIQUE KEY(period_type, period_start)
COMMENT '统计期发布状态表；保存版本、覆盖及发布不确定状态，等待 Worker 实现'
DISTRIBUTED BY HASH(period_type, period_start) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
