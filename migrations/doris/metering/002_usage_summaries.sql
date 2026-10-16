-- Phase 1 summary schema for Apache Doris 4.1.3; this does not schedule refresh jobs.
-- Summary partition creation / expiration is owned by one protected Go usage-worker.
-- Coordination tables are in 004_summary_coordination.sql; no PostgreSQL summary store.
-- Daily: 730 UTC dates including today. Monthly: 24 UTC months including this month.
-- Initial historical summary partitions are created by a controlled backfill job.
-- One-replica / eight-bucket values are development examples.
CREATE TABLE IF NOT EXISTS metering.usage_daily (
    stat_date DATE NOT NULL COMMENT '日统计或重算通知对应的 UTC 请求日期',
    cluster_name VARCHAR(128) NOT NULL COMMENT 'Observer 配置固定的集群名称，业务请求不能覆盖',
    deployment_environment VARCHAR(16) NOT NULL COMMENT 'Observer 固定部署环境：production、test 或 development',
    customer_id VARCHAR(128) NOT NULL COMMENT '已成功解析的客户统计维度；未知客户不进入客户汇总，不创建空客户桶',
    provider_id VARCHAR(128) NOT NULL COMMENT '供应商统计维度；空串仅代表未知汇总桶，不回写原始事件归属',
    api_key_id VARCHAR(128) NOT NULL COMMENT '已成功解析的稳定 API Key 统计维度，不是原始密钥，必须与客户归属一起已知',
    service_id VARCHAR(128) NOT NULL COMMENT '服务统计维度；空串仅代表未知汇总桶，不回写原始事件归属',
    model VARCHAR(1024) NOT NULL COMMENT '统计模型维度，优先响应模型、其次请求模型；两者均缺失用空串表示未知',
    usage_source VARCHAR(32) NOT NULL COMMENT '统计用量来源分组：reported、unknown、missing、mixed；estimated_model 和 estimated_generic 为预留估算分类',
    request_count BIGINT NOT NULL COMMENT '该分组无冲突且客户归属已知的已观察逻辑请求数，包含用量不完整请求',
    started_request_count BIGINT NOT NULL COMMENT '该分组中恰有一条 request_started 事件的逻辑请求数',
    usage_observed_request_count BIGINT NOT NULL COMMENT '该分组中恰有一条 usage_observed 事件的逻辑请求数',
    usage_missing_request_count BIGINT NOT NULL COMMENT '该分组中未观察到 usage_observed 事件的逻辑请求数',
    eligible_usage_request_count BIGINT NOT NULL COMMENT '该分组中有唯一结果、usage_eligible=1 且输入输出用量均非 NULL 的请求数',
    ineligible_usage_request_count BIGINT NOT NULL COMMENT '该分组中未满足有效用量统计条件的请求数，与有效请求数合计为 request_count',
    http_2xx_request_count BIGINT NOT NULL COMMENT '该分组中网关可见 HTTP 状态为 200 至 299 的请求数',
    http_error_request_count BIGINT NOT NULL COMMENT '该分组中网关可见 HTTP 状态大于等于 400 的请求数',
    terminal_missing_request_count BIGINT NOT NULL COMMENT '该分组中 terminal_observed 为 0 或 NULL 的请求数',
    input_tokens DECIMAL(38, 0) NULL COMMENT '有效统计候选的输入 token 总量之和；无已知贡献保持 NULL，已知零保持 0',
    output_tokens DECIMAL(38, 0) NULL COMMENT '有效统计候选的输出 token 总量之和；无已知贡献保持 NULL，已知零保持 0',
    total_tokens DECIMAL(38, 0) NULL COMMENT '有效统计候选的输入加输出 token 总量之和；全部未知保持 NULL，不包含子项重复相加',
    cached_read_tokens DECIMAL(38, 0) NULL COMMENT '有效统计候选的输入中的缓存读取 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    cached_write_tokens DECIMAL(38, 0) NULL COMMENT '有效统计候选的输入中的缓存写入 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    reasoning_tokens DECIMAL(38, 0) NULL COMMENT '有效统计候选的输出中的推理 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    reported_total_tokens DECIMAL(38, 0) NULL COMMENT '有效统计候选的服务单独上报的总 token 数之和；无已知贡献保持 NULL，已知零保持 0',
    cached_read_known_requests BIGINT NOT NULL COMMENT '该分组有效统计候选中 输入中的缓存读取 token 子项非 NULL 的请求数',
    cached_write_known_requests BIGINT NOT NULL COMMENT '该分组有效统计候选中 输入中的缓存写入 token 子项非 NULL 的请求数',
    reasoning_known_requests BIGINT NOT NULL COMMENT '该分组有效统计候选中 输出中的推理 token 子项非 NULL 的请求数',
    reported_total_known_requests BIGINT NOT NULL COMMENT '该分组有效统计候选中 服务单独上报的总 token 数非 NULL 的请求数',
    refresh_id VARCHAR(36) NOT NULL COMMENT '生成本次汇总结果或执行记录的刷新 UUID；Worker 发布版本关联标识',
    source_watermark CHAR(64) NOT NULL COMMENT '本次汇总输入及连续分区水位向量的规范 SHA-256，不是全局 Kafka 截止 offset',
    refreshed_at DATETIME(3) NOT NULL COMMENT '本次汇总刷新或成功发布的 UTC 时间；尚未成功发布时状态表可为 NULL'
)
UNIQUE KEY(stat_date, cluster_name, deployment_environment, customer_id, provider_id, api_key_id, service_id, model, usage_source)
COMMENT '日用量汇总表；按客户、服务、供应商及部署维度保存统计，正式 Worker 尚未实现'
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(customer_id, api_key_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS metering.usage_monthly (
    stat_month DATE NOT NULL COMMENT '月统计对应的 UTC 自然月首日',
    cluster_name VARCHAR(128) NOT NULL COMMENT 'Observer 配置固定的集群名称，业务请求不能覆盖',
    deployment_environment VARCHAR(16) NOT NULL COMMENT 'Observer 固定部署环境：production、test 或 development',
    customer_id VARCHAR(128) NOT NULL COMMENT '已成功解析的客户统计维度；未知客户不进入客户汇总，不创建空客户桶',
    provider_id VARCHAR(128) NOT NULL COMMENT '供应商统计维度；空串仅代表未知汇总桶，不回写原始事件归属',
    api_key_id VARCHAR(128) NOT NULL COMMENT '已成功解析的稳定 API Key 统计维度，不是原始密钥，必须与客户归属一起已知',
    service_id VARCHAR(128) NOT NULL COMMENT '服务统计维度；空串仅代表未知汇总桶，不回写原始事件归属',
    model VARCHAR(1024) NOT NULL COMMENT '统计模型维度，优先响应模型、其次请求模型；两者均缺失用空串表示未知',
    usage_source VARCHAR(32) NOT NULL COMMENT '统计用量来源分组：reported、unknown、missing、mixed；estimated_model 和 estimated_generic 为预留估算分类',
    request_count BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组无冲突且客户归属已知的已观察逻辑请求数，包含用量不完整请求',
    started_request_count BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组中恰有一条 request_started 事件的逻辑请求数',
    usage_observed_request_count BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组中恰有一条 usage_observed 事件的逻辑请求数',
    usage_missing_request_count BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组中未观察到 usage_observed 事件的逻辑请求数',
    eligible_usage_request_count BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组中有唯一结果、usage_eligible=1 且输入输出用量均非 NULL 的请求数',
    ineligible_usage_request_count BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组中未满足有效用量统计条件的请求数，与有效请求数合计为 request_count',
    http_2xx_request_count BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组中网关可见 HTTP 状态为 200 至 299 的请求数',
    http_error_request_count BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组中网关可见 HTTP 状态大于等于 400 的请求数',
    terminal_missing_request_count BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组中 terminal_observed 为 0 或 NULL 的请求数',
    input_tokens DECIMAL(38, 0) NULL COMMENT '同维度已确认日统计的月合计；有效统计候选的输入 token 总量之和；无已知贡献保持 NULL，已知零保持 0',
    output_tokens DECIMAL(38, 0) NULL COMMENT '同维度已确认日统计的月合计；有效统计候选的输出 token 总量之和；无已知贡献保持 NULL，已知零保持 0',
    total_tokens DECIMAL(38, 0) NULL COMMENT '同维度已确认日统计的月合计；有效统计候选的输入加输出 token 总量之和；全部未知保持 NULL，不包含子项重复相加',
    cached_read_tokens DECIMAL(38, 0) NULL COMMENT '同维度已确认日统计的月合计；有效统计候选的输入中的缓存读取 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    cached_write_tokens DECIMAL(38, 0) NULL COMMENT '同维度已确认日统计的月合计；有效统计候选的输入中的缓存写入 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    reasoning_tokens DECIMAL(38, 0) NULL COMMENT '同维度已确认日统计的月合计；有效统计候选的输出中的推理 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    reported_total_tokens DECIMAL(38, 0) NULL COMMENT '同维度已确认日统计的月合计；有效统计候选的服务单独上报的总 token 数之和；无已知贡献保持 NULL，已知零保持 0',
    cached_read_known_requests BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组有效统计候选中 输入中的缓存读取 token 子项非 NULL 的请求数',
    cached_write_known_requests BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组有效统计候选中 输入中的缓存写入 token 子项非 NULL 的请求数',
    reasoning_known_requests BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组有效统计候选中 输出中的推理 token 子项非 NULL 的请求数',
    reported_total_known_requests BIGINT NOT NULL COMMENT '同维度已确认日统计的月合计：该分组有效统计候选中 服务单独上报的总 token 数非 NULL 的请求数',
    refresh_id VARCHAR(36) NOT NULL COMMENT '生成本次汇总结果或执行记录的刷新 UUID；Worker 发布版本关联标识',
    source_watermark CHAR(64) NOT NULL COMMENT '本次汇总输入及连续分区水位向量的规范 SHA-256，不是全局 Kafka 截止 offset',
    refreshed_at DATETIME(3) NOT NULL COMMENT '本次汇总刷新或成功发布的 UTC 时间；尚未成功发布时状态表可为 NULL'
)
UNIQUE KEY(stat_month, cluster_name, deployment_environment, customer_id, provider_id, api_key_id, service_id, model, usage_source)
COMMENT '月用量汇总表；从日汇总生成独立保留的月统计，正式 Worker 尚未实现'
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
CREATE VIEW IF NOT EXISTS metering.request_statistics (
    request_date COMMENT 'UTC 请求日期，由 request_started_at_ms 换算，不由事件到达时间或 UUID 推导',
    logical_request_id COMMENT 'Observer 为一次观察生成的规范 UUIDv4；请求级主标识及 Kafka key',
    start_variants COMMENT '该逻辑请求的开始事件内容变体数；大于 1 表示冲突，不是调用次数',
    usage_variants COMMENT '该逻辑请求的结果事件内容变体数；0 表示未观察结果，大于 1 表示冲突',
    started_count COMMENT '该日期内同一逻辑请求的不同开始时间数量；大于 1 表示时间冲突',
    identity_count COMMENT '该日期内同一逻辑请求的不同十五项归属与部署快照数量；大于 1 表示冲突',
    customer_id COMMENT '与 api_key_id 同次成功解析得到的客户 ID；未知为 NULL，不采信客户端自报 ID',
    provider_id COMMENT '服务接口返回的服务所属供应商 ID；未知为 NULL，不代表隐藏的实际执行方',
    api_key_id COMMENT '客户接口返回的稳定 API Key ID；未知为 NULL，绝不是原始 Bearer token',
    service_id COMMENT '服务接口返回的服务 ID；未实现或失败为 NULL，不使用默认服务补值',
    cluster_name COMMENT 'Observer 配置固定的集群名称，业务请求不能覆盖',
    deployment_environment COMMENT 'Observer 固定部署环境：production、test 或 development',
    request_host COMMENT '规范入口 DNS 域名；转小写并去端口及末尾根点，非法输入为 NULL',
    mapping_status COMMENT '服务解析状态：mapped、unknown_host、invalid_host、lookup_not_implemented、lookup_timeout、lookup_unavailable、lookup_invalid_response、lookup_overloaded、lookup_rate_limited；当前合法输入为未实现占位',
    mapping_version COMMENT '成功服务解析快照的规范 SHA-256，绑定 Host、集群、环境及服务和供应商；未知为 NULL',
    customer_mapping_status COMMENT '客户解析状态：mapped、unknown_api_key、invalid_api_key、lookup_not_implemented、lookup_timeout、lookup_unavailable、lookup_invalid_response、lookup_overloaded、lookup_rate_limited；当前合法输入为未实现占位',
    customer_mapping_version COMMENT '成功客户解析快照的规范 SHA-256，绑定部署上下文、API Key ID 和客户；未知为 NULL',
    identity_status COMMENT '客户归属状态：known 或 invalid；仅完整客户解析成功为 known，不表示鉴权判定',
    request_model COMMENT '请求正文中可靠提取的模型名或 ID；未取得或非法时为 NULL',
    response_model COMMENT '响应上报的模型名或 ID，可能不同于请求模型；未取得或非法时为 NULL',
    usage_eligible COMMENT '这条用量是否符合有效统计条件，可以作为统计候选？1 是、0 否；不表示可收费',
    http_status COMMENT '网关可见的 HTTP 响应状态码；未知或开始事件为 NULL',
    terminal_observed COMMENT '是否收到有效网关 End：1 是、0 否；为 0 时请求结束时间必须为 NULL',
    input_tokens COMMENT '输入 token 总量；NULL 表示未知，0 表示服务报告的有效零值；子项不与父项重复相加',
    output_tokens COMMENT '输出 token 总量；NULL 表示未知，0 表示服务报告的有效零值；子项不与父项重复相加',
    input_tokens_source COMMENT '输入 token 总量的来源：reported 或 unknown；开始事件为 NULL，不实现估算',
    output_tokens_source COMMENT '输出 token 总量的来源：reported 或 unknown；开始事件为 NULL，不实现估算',
    cached_read_tokens COMMENT '输入中的缓存读取 token 子项；NULL 表示未知，0 表示服务报告的有效零值；子项不与父项重复相加',
    cached_write_tokens COMMENT '输入中的缓存写入 token 子项；NULL 表示未知，0 表示服务报告的有效零值；子项不与父项重复相加',
    reasoning_tokens COMMENT '输出中的推理 token 子项；NULL 表示未知，0 表示服务报告的有效零值；子项不与父项重复相加',
    reported_total_tokens COMMENT '服务单独上报的总 token 数；NULL 表示未知，0 表示服务报告的有效零值；子项不与父项重复相加',
    has_conflict COMMENT '日期内请求变体、时间或归属冲突，或命中跨日期持久冲突标记时为 1；汇总 SQL 据此排除',
    model COMMENT '统计模型维度，优先响应模型、其次请求模型；两者均缺失用空串表示未知',
    usage_source COMMENT '统计用量来源分组：reported、unknown、missing、mixed；estimated_model 和 estimated_generic 为预留估算分类'
) AS
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
           COUNT(DISTINCT CAST(JSON_ARRAY(service_id, provider_id, api_key_id, customer_id, identity_status, config_version, gateway_instance_id, adapter_id, x_request_id, cluster_name, deployment_environment, mapping_status, mapping_version, customer_mapping_status, customer_mapping_version) AS STRING)) AS identity_count,
           MAX(customer_id) AS customer_id, MAX(provider_id) AS provider_id,
           MAX(api_key_id) AS api_key_id, MAX(service_id) AS service_id,
           MAX(cluster_name) AS cluster_name, MAX(deployment_environment) AS deployment_environment, MAX(request_host) AS request_host, MAX(mapping_status) AS mapping_status, MAX(mapping_version) AS mapping_version, MAX(customer_mapping_status) AS customer_mapping_status, MAX(customer_mapping_version) AS customer_mapping_version,
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
