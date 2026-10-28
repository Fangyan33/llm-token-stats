-- T+1 asynchronous summaries and refresh recovery for Apache Doris 4.1.3.

-- BUILD DEFERRED: usage-worker refreshes ended dates and then their month.

SET time_zone = '+00:00';

-- 日统计输入视图：按 UTC 请求日期和观察 ID 合并开始与结果事件，派生模型、用量来源及统计资格。
-- 保留未知归属和冲突标识，供日物化视图排除冲突后按八个维度聚合；资格判断不修改原始事件。
CREATE VIEW IF NOT EXISTS metering.request_statistics (
    request_date COMMENT 'UTC 请求日期，由 request_started_at_ms 换算，不由事件到达时间或 UUID 推导',
    observation_id COMMENT 'Observer 为一次观察生成的规范 UUIDv4；内部主标识及 Kafka key',
    logical_request_id COMMENT 'Wasm 按配置提取的客户端请求 ID；可空、可重复，不用于内部去重',
    start_variants COMMENT '该观察的开始事件内容变体数；大于 1 表示冲突，不是调用次数',
    usage_variants COMMENT '该观察的结果事件内容变体数；0 表示未观察结果，大于 1 表示冲突',
    started_count COMMENT '该日期内同一观察的不同开始时间数量；大于 1 表示时间冲突',
    identity_count COMMENT '该日期内同一观察的不同十六项归属与部署快照数量；大于 1 表示冲突',
    name_count COMMENT '同请求不同名称快照元组数量；大于 1 表示冲突',
    customer_id COMMENT '与 api_key_id 同次成功解析得到的客户 ID；未知为 NULL，不采信客户端自报 ID',
    provider_id COMMENT '服务接口返回的服务所属供应商 ID；未知为 NULL，不代表隐藏的实际执行方',
    api_key_id COMMENT '客户接口返回的稳定 API Key ID；未知为 NULL，绝不是原始 Bearer token',
    service_id COMMENT '服务接口返回的服务 ID；未实现或失败为 NULL，不使用默认服务补值',
    service_name COMMENT '请求发生时的服务名称快照；解析未知时为 NULL，不用最新名称覆盖历史',
    provider_name COMMENT '请求发生时的供应商名称快照；解析未知时为 NULL，不用最新名称覆盖历史',
    customer_name COMMENT '请求发生时的客户名称快照；解析未知时为 NULL，不用最新名称覆盖历史',
    cluster_name COMMENT 'Observer 配置固定的集群名称，业务请求不能覆盖',
    deployment_environment COMMENT 'Observer 固定部署环境：prd、stg 或 dev',
    request_host COMMENT '规范入口 DNS 域名；转小写并去端口及末尾根点，非法输入为 NULL',
    mapping_status COMMENT '服务解析状态：mapped、unknown_host、invalid_host、lookup_not_implemented、lookup_timeout、lookup_unavailable、lookup_invalid_response、lookup_overloaded、lookup_rate_limited；空 URI 为未实现占位，配置 HTTP/HTTPS 可解析',
    mapping_version COMMENT '成功服务解析快照的规范 SHA-256，绑定 Host、集群、环境及服务和供应商；未知为 NULL',
    customer_mapping_status COMMENT '客户解析状态：mapped、unknown_api_key、invalid_api_key、lookup_not_implemented、lookup_timeout、lookup_unavailable、lookup_invalid_response、lookup_overloaded、lookup_rate_limited；空 URI 为未实现占位，配置 HTTP/HTTPS 可解析',
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
    statistics_usage_eligible COMMENT '按当前质量口径重算的用量资格；不依赖客户身份，不修改原始事件',
    has_conflict COMMENT '日期内请求变体、时间或归属冲突，或命中跨日期持久冲突标记时为 1；汇总 SQL 据此排除',
    model COMMENT '统计模型维度，优先响应模型、其次请求模型；两者均缺失用空串表示未知',
    usage_source COMMENT '统计用量来源分组：reported、unknown、missing、mixed；estimated_model 和 estimated_generic 为预留估算分类'
) AS
SELECT a.*,
       CASE WHEN a.start_variants > 1 OR a.usage_variants > 1
                 OR a.started_count > 1 OR a.identity_count > 1 OR a.name_count > 1
                 OR c.observation_id IS NOT NULL THEN 1 ELSE 0 END AS has_conflict,
       COALESCE(NULLIF(a.response_model, ''), NULLIF(a.request_model, ''), '') AS model,
       CASE WHEN a.usage_variants = 0 THEN 'missing'
            WHEN a.input_tokens IS NULL OR a.output_tokens IS NULL THEN 'unknown'
            WHEN a.input_tokens_source = 'reported' AND a.output_tokens_source = 'reported' THEN 'reported'
            WHEN a.input_tokens_source = 'estimated_model' AND a.output_tokens_source = 'estimated_model' THEN 'estimated_model'
            WHEN a.input_tokens_source = 'estimated_generic' AND a.output_tokens_source = 'estimated_generic' THEN 'estimated_generic'
            ELSE 'mixed' END AS usage_source
FROM (
    SELECT request_date, observation_id, MAX(logical_request_id) AS logical_request_id,
           SUM(CASE WHEN event_type = 'request_started' THEN 1 ELSE 0 END) AS start_variants,
           SUM(CASE WHEN event_type = 'usage_observed' THEN 1 ELSE 0 END) AS usage_variants,
           COUNT(DISTINCT request_started_at_ms) AS started_count,
           COUNT(DISTINCT CAST(JSON_ARRAY(service_id, provider_id, api_key_id, customer_id, identity_status, config_version, gateway_instance_id, adapter_id, x_request_id, logical_request_id, cluster_name, deployment_environment, mapping_status, mapping_version, customer_mapping_status, customer_mapping_version) AS STRING)) AS identity_count,
           COUNT(DISTINCT CAST(JSON_ARRAY(service_name, provider_name, customer_name) AS STRING)) AS name_count,
           MAX(customer_id) AS customer_id, MAX(provider_id) AS provider_id,
           MAX(api_key_id) AS api_key_id, MAX(service_id) AS service_id,
           CASE WHEN COUNT(DISTINCT CAST(JSON_ARRAY(service_name, provider_name, customer_name) AS STRING)) = 1 THEN MAX(service_name) END AS service_name,
           CASE WHEN COUNT(DISTINCT CAST(JSON_ARRAY(service_name, provider_name, customer_name) AS STRING)) = 1 THEN MAX(provider_name) END AS provider_name,
           CASE WHEN COUNT(DISTINCT CAST(JSON_ARRAY(service_name, provider_name, customer_name) AS STRING)) = 1 THEN MAX(customer_name) END AS customer_name,
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
           MAX(CASE WHEN event_type = 'usage_observed' THEN reported_total_tokens END) AS reported_total_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' AND observation_status = 'complete' AND terminal_observed = 1 AND usage_status = 'known' AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND SIZE(ARRAY_EXCEPT(anomaly_codes, ['invalid_model', 'identity_invalid'])) = 0 THEN 1 ELSE 0 END) AS statistics_usage_eligible
    FROM metering.events_raw
    GROUP BY request_date, observation_id
) a
LEFT JOIN metering.request_conflict_ids c ON a.observation_id = c.observation_id;

-- 日用量物化视图：从请求统计投影按日期及八个维度汇总无冲突请求数、有效 token 和质量指标。
-- 由 usage-worker 仅刷新已结束日期；公共统计保留未知归属，客户及 API Key 报表需过滤对应未知 ID。
CREATE MATERIALIZED VIEW IF NOT EXISTS metering.usage_daily_mv (
    stat_date COMMENT '日统计或重算通知对应的 UTC 请求日期',
    cluster_name COMMENT 'Observer 配置固定的集群名称，业务请求不能覆盖',
    deployment_environment COMMENT 'Observer 固定部署环境：prd、stg 或 dev',
    customer_id COMMENT '客户统计维度；未知保持 NULL，客户报表需过滤未知归属',
    provider_id COMMENT '供应商统计维度；未知保持 NULL，已知供应商统计包含客户未知请求',
    api_key_id COMMENT '稳定 API Key 统计维度；未知保持 NULL，API Key 报表需过滤未知归属',
    service_id COMMENT '服务统计维度；未知保持 NULL，已知服务统计包含客户未知请求',
    model COMMENT '统计模型维度，优先响应模型、其次请求模型；两者均缺失用空串表示未知',
    usage_source COMMENT '统计用量来源分组：reported、unknown、missing、mixed；estimated_model 和 estimated_generic 为预留估算分类',
    request_count COMMENT '该分组无冲突的已观察观察请求数，包含客户未知及用量不完整请求',
    started_request_count COMMENT '该分组中恰有一条 request_started 事件的观察请求数',
    usage_observed_request_count COMMENT '该分组中恰有一条 usage_observed 事件的观察请求数',
    usage_missing_request_count COMMENT '该分组中未观察到 usage_observed 事件的观察请求数',
    eligible_usage_request_count COMMENT '该分组中有唯一结果、statistics_usage_eligible=1 且输入输出用量均非 NULL 的请求数',
    ineligible_usage_request_count COMMENT '该分组中未满足有效用量统计条件的请求数，与有效请求数合计为 request_count',
    http_2xx_request_count COMMENT '该分组中网关可见 HTTP 状态为 200 至 299 的请求数',
    http_error_request_count COMMENT '该分组中网关可见 HTTP 状态大于等于 400 的请求数',
    terminal_missing_request_count COMMENT '该分组中 terminal_observed 为 0 或 NULL 的请求数',
    input_tokens COMMENT '有效统计候选的输入 token 总量之和；无已知贡献保持 NULL，已知零保持 0',
    output_tokens COMMENT '有效统计候选的输出 token 总量之和；无已知贡献保持 NULL，已知零保持 0',
    total_tokens COMMENT '有效统计候选的输入加输出 token 总量之和；全部未知保持 NULL，不包含子项重复相加',
    cached_read_tokens COMMENT '有效统计候选的输入中的缓存读取 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    cached_write_tokens COMMENT '有效统计候选的输入中的缓存写入 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    reasoning_tokens COMMENT '有效统计候选的输出中的推理 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    reported_total_tokens COMMENT '有效统计候选的服务单独上报的总 token 数之和；无已知贡献保持 NULL，已知零保持 0',
    cached_read_known_requests COMMENT '该分组有效统计候选中 输入中的缓存读取 token 子项非 NULL 的请求数',
    cached_write_known_requests COMMENT '该分组有效统计候选中 输入中的缓存写入 token 子项非 NULL 的请求数',
    reasoning_known_requests COMMENT '该分组有效统计候选中 输出中的推理 token 子项非 NULL 的请求数',
    reported_total_known_requests COMMENT '该分组有效统计候选中 服务单独上报的总 token 数非 NULL 的请求数'
)
BUILD DEFERRED
REFRESH AUTO ON MANUAL
COMMENT '日用量异步物化视图；含未知归属，按已结束日期手动刷新'
PARTITION BY (stat_date)
DISTRIBUTED BY HASH(customer_id, api_key_id, service_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "use_for_rewrite" = "false",
    "refresh_partition_num" = "1"
) AS
SELECT request_date AS stat_date, cluster_name, deployment_environment, customer_id, provider_id, api_key_id, service_id, model, usage_source,
       COUNT(*) AS request_count,
       SUM(CASE WHEN start_variants = 1 THEN 1 ELSE 0 END) AS started_request_count,
       SUM(CASE WHEN usage_variants = 1 THEN 1 ELSE 0 END) AS usage_observed_request_count,
       SUM(CASE WHEN usage_variants = 0 THEN 1 ELSE 0 END) AS usage_missing_request_count,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN 1 ELSE 0 END) AS eligible_usage_request_count,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN 0 ELSE 1 END) AS ineligible_usage_request_count,
       SUM(CASE WHEN http_status BETWEEN 200 AND 299 THEN 1 ELSE 0 END) AS http_2xx_request_count,
       SUM(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS http_error_request_count,
       SUM(CASE WHEN terminal_observed IS NULL OR terminal_observed = 0 THEN 1 ELSE 0 END) AS terminal_missing_request_count,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(input_tokens AS DECIMAL(38, 0)) END) AS input_tokens,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(output_tokens AS DECIMAL(38, 0)) END) AS output_tokens,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(input_tokens AS DECIMAL(38, 0)) + CAST(output_tokens AS DECIMAL(38, 0)) END) AS total_tokens,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(cached_read_tokens AS DECIMAL(38, 0)) END) AS cached_read_tokens,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(cached_write_tokens AS DECIMAL(38, 0)) END) AS cached_write_tokens,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(reasoning_tokens AS DECIMAL(38, 0)) END) AS reasoning_tokens,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(reported_total_tokens AS DECIMAL(38, 0)) END) AS reported_total_tokens,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND cached_read_tokens IS NOT NULL THEN 1 ELSE 0 END) AS cached_read_known_requests,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND cached_write_tokens IS NOT NULL THEN 1 ELSE 0 END) AS cached_write_known_requests,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND reasoning_tokens IS NOT NULL THEN 1 ELSE 0 END) AS reasoning_known_requests,
       SUM(CASE WHEN usage_variants = 1 AND statistics_usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND reported_total_tokens IS NOT NULL THEN 1 ELSE 0 END) AS reported_total_known_requests
FROM metering.request_statistics
WHERE has_conflict = 0
GROUP BY request_date, cluster_name, deployment_environment, customer_id, provider_id, api_key_id, service_id, model, usage_source;

-- 月用量嵌套物化视图：直接按自然月及相同八个维度累加日物化视图的指标，不重新扫描原始事件。
-- 由 usage-worker 在相关日统计成功后刷新，当前月累计到昨日；全未知用量保持 NULL，已知零保持 0。
CREATE MATERIALIZED VIEW IF NOT EXISTS metering.usage_monthly_mv (
    stat_month COMMENT '统计月对应的 UTC 自然月首日',
    cluster_name COMMENT 'Observer 配置固定的集群名称，业务请求不能覆盖',
    deployment_environment COMMENT 'Observer 固定部署环境：prd、stg 或 dev',
    customer_id COMMENT '客户统计维度；未知保持 NULL，客户报表需过滤未知归属',
    provider_id COMMENT '供应商统计维度；未知保持 NULL，已知供应商统计包含客户未知请求',
    api_key_id COMMENT '稳定 API Key 统计维度；未知保持 NULL，API Key 报表需过滤未知归属',
    service_id COMMENT '服务统计维度；未知保持 NULL，已知服务统计包含客户未知请求',
    model COMMENT '统计模型维度，优先响应模型、其次请求模型；两者均缺失用空串表示未知',
    usage_source COMMENT '统计用量来源分组：reported、unknown、missing、mixed；estimated_model 和 estimated_generic 为预留估算分类',
    request_count COMMENT '该分组无冲突的已观察观察请求数，包含客户未知及用量不完整请求',
    started_request_count COMMENT '该分组中恰有一条 request_started 事件的观察请求数',
    usage_observed_request_count COMMENT '该分组中恰有一条 usage_observed 事件的观察请求数',
    usage_missing_request_count COMMENT '该分组中未观察到 usage_observed 事件的观察请求数',
    eligible_usage_request_count COMMENT '该分组中有唯一结果、statistics_usage_eligible=1 且输入输出用量均非 NULL 的请求数',
    ineligible_usage_request_count COMMENT '该分组中未满足有效用量统计条件的请求数，与有效请求数合计为 request_count',
    http_2xx_request_count COMMENT '该分组中网关可见 HTTP 状态为 200 至 299 的请求数',
    http_error_request_count COMMENT '该分组中网关可见 HTTP 状态大于等于 400 的请求数',
    terminal_missing_request_count COMMENT '该分组中 terminal_observed 为 0 或 NULL 的请求数',
    input_tokens COMMENT '有效统计候选的输入 token 总量之和；无已知贡献保持 NULL，已知零保持 0',
    output_tokens COMMENT '有效统计候选的输出 token 总量之和；无已知贡献保持 NULL，已知零保持 0',
    total_tokens COMMENT '有效统计候选的输入加输出 token 总量之和；全部未知保持 NULL，不包含子项重复相加',
    cached_read_tokens COMMENT '有效统计候选的输入中的缓存读取 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    cached_write_tokens COMMENT '有效统计候选的输入中的缓存写入 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    reasoning_tokens COMMENT '有效统计候选的输出中的推理 token 子项之和；无已知贡献保持 NULL，已知零保持 0',
    reported_total_tokens COMMENT '有效统计候选的服务单独上报的总 token 数之和；无已知贡献保持 NULL，已知零保持 0',
    cached_read_known_requests COMMENT '该分组有效统计候选中 输入中的缓存读取 token 子项非 NULL 的请求数',
    cached_write_known_requests COMMENT '该分组有效统计候选中 输入中的缓存写入 token 子项非 NULL 的请求数',
    reasoning_known_requests COMMENT '该分组有效统计候选中 输出中的推理 token 子项非 NULL 的请求数',
    reported_total_known_requests COMMENT '该分组有效统计候选中 服务单独上报的总 token 数非 NULL 的请求数'
)
BUILD DEFERRED
REFRESH AUTO ON MANUAL
COMMENT '月用量嵌套异步物化视图；直接累加已确认日统计'
PARTITION BY (stat_month)
DISTRIBUTED BY HASH(customer_id, api_key_id, service_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "use_for_rewrite" = "false",
    "refresh_partition_num" = "1"
) AS
SELECT DATE_TRUNC(stat_date, 'month') AS stat_month, cluster_name, deployment_environment, customer_id, provider_id, api_key_id, service_id, model, usage_source,
       SUM(request_count) AS request_count,
       SUM(started_request_count) AS started_request_count,
       SUM(usage_observed_request_count) AS usage_observed_request_count,
       SUM(usage_missing_request_count) AS usage_missing_request_count,
       SUM(eligible_usage_request_count) AS eligible_usage_request_count,
       SUM(ineligible_usage_request_count) AS ineligible_usage_request_count,
       SUM(http_2xx_request_count) AS http_2xx_request_count,
       SUM(http_error_request_count) AS http_error_request_count,
       SUM(terminal_missing_request_count) AS terminal_missing_request_count,
       SUM(input_tokens) AS input_tokens,
       SUM(output_tokens) AS output_tokens,
       SUM(total_tokens) AS total_tokens,
       SUM(cached_read_tokens) AS cached_read_tokens,
       SUM(cached_write_tokens) AS cached_write_tokens,
       SUM(reasoning_tokens) AS reasoning_tokens,
       SUM(reported_total_tokens) AS reported_total_tokens,
       SUM(cached_read_known_requests) AS cached_read_known_requests,
       SUM(cached_write_known_requests) AS cached_write_known_requests,
       SUM(reasoning_known_requests) AS reasoning_known_requests,
       SUM(reported_total_known_requests) AS reported_total_known_requests
FROM metering.usage_daily_mv
GROUP BY DATE_TRUNC(stat_date, 'month'), cluster_name, deployment_environment, customer_id, provider_id, api_key_id, service_id, model, usage_source;

CREATE TABLE IF NOT EXISTS metering.summary_mv_refresh_runs (
    period_type VARCHAR(8) NOT NULL COMMENT '统计期类型 DAY 或 MONTH',
    period_start DATE NOT NULL COMMENT 'UTC 统计期起点',
    refresh_id VARCHAR(36) NOT NULL COMMENT '本次刷新 UUID',
    status VARCHAR(16) NOT NULL COMMENT 'PENDING、RUNNING、SUCCEEDED、FAILED 或 UNCERTAIN',
    record_json STRING NOT NULL COMMENT '任务输入摘要、分区、Doris 任务 ID 及恢复信息，不含正文或密钥',
    updated_at DATETIME(3) NOT NULL COMMENT 'UTC 最近状态变更时间'
)
UNIQUE KEY(period_type, period_start, refresh_id)
COMMENT '物化视图刷新任务审计与恢复'
DISTRIBUTED BY HASH(period_type) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.summary_mv_refresh_inputs (
    stat_date DATE NOT NULL COMMENT 'UTC 请求日期',
    refresh_id VARCHAR(36) NOT NULL COMMENT '捕获通知的日刷新 UUID',
    notification_id CHAR(64) NOT NULL COMMENT '精确捕获的不可变通知 ID'
)
UNIQUE KEY(stat_date, refresh_id, notification_id)
COMMENT '物化视图日刷新输入集合；成功任务才确认这些通知'
DISTRIBUTED BY HASH(stat_date) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS metering.summary_mv_refresh_state (
    period_type VARCHAR(8) NOT NULL COMMENT '统计期类型 DAY 或 MONTH',
    period_start DATE NOT NULL COMMENT 'UTC 统计期起点',
    status VARCHAR(16) NOT NULL COMMENT 'PENDING、RUNNING、UNCERTAIN、FAILED、SUCCEEDED、GAP 或 EXPIRED',
    record_json STRING NOT NULL COMMENT '最新成功版本、覆盖、活动任务及月依赖的日版本集合',
    updated_at DATETIME(3) NOT NULL COMMENT 'UTC 最近状态变更时间'
)
UNIQUE KEY(period_type, period_start)
COMMENT '日月物化视图覆盖与最近成功状态'
DISTRIBUTED BY HASH(period_type) BUCKETS 4
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
