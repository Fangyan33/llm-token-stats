-- Current contract projections; initialize after 001/002. No legacy decoder or hash path.
CREATE OR REPLACE VIEW metering.requests (
    request_date COMMENT 'UTC 请求日期，由 request_started_at_ms 换算，不由事件到达时间或 UUID 推导',
    logical_request_id COMMENT 'Observer 为一次观察生成的规范 UUIDv4；请求级主标识及 Kafka key',
    start_variants COMMENT '该逻辑请求的开始事件内容变体数；大于 1 表示冲突，不是调用次数',
    usage_variants COMMENT '该逻辑请求的结果事件内容变体数；0 表示未观察结果，大于 1 表示冲突',
    request_started_at_ms COMMENT '网关观察到请求头的 Unix 毫秒时间；决定 UTC 统计日期',
    x_request_id COMMENT '网关可见的 X-Request-Id，仅辅助关联；可空、可重复，不用于授权或去重',
    customer_id COMMENT '与 api_key_id 同次成功解析得到的客户 ID；未知为 NULL，不采信客户端自报 ID',
    service_id COMMENT '服务接口返回的服务 ID；未实现或失败为 NULL，不使用默认服务补值',
    service_name COMMENT '请求发生时的服务名称快照；解析未知时为 NULL，不用最新名称覆盖历史',
    provider_name COMMENT '请求发生时的供应商名称快照；解析未知时为 NULL，不用最新名称覆盖历史',
    customer_name COMMENT '请求发生时的客户名称快照；解析未知时为 NULL，不用最新名称覆盖历史',
    provider_id COMMENT '服务接口返回的服务所属供应商 ID；未知为 NULL，不代表隐藏的实际执行方',
    cluster_name COMMENT 'Observer 配置固定的集群名称，业务请求不能覆盖',
    deployment_environment COMMENT 'Observer 固定部署环境：prd、stg 或 dev',
    request_host COMMENT '规范入口 DNS 域名；转小写并去端口及末尾根点，非法输入为 NULL',
    mapping_status COMMENT '服务解析状态：mapped、unknown_host、invalid_host、lookup_not_implemented、lookup_timeout、lookup_unavailable、lookup_invalid_response、lookup_overloaded、lookup_rate_limited；空 URI 为未实现占位，配置 HTTP/HTTPS 可解析',
    mapping_version COMMENT '成功服务解析快照的规范 SHA-256，绑定 Host、集群、环境及服务和供应商；未知为 NULL',
    customer_mapping_status COMMENT '客户解析状态：mapped、unknown_api_key、invalid_api_key、lookup_not_implemented、lookup_timeout、lookup_unavailable、lookup_invalid_response、lookup_overloaded、lookup_rate_limited；空 URI 为未实现占位，配置 HTTP/HTTPS 可解析',
    customer_mapping_version COMMENT '成功客户解析快照的规范 SHA-256，绑定部署上下文、API Key ID 和客户；未知为 NULL',
    response_model COMMENT '响应上报的模型名或 ID，可能不同于请求模型；未取得或非法时为 NULL',
    usage_status COMMENT '唯一结果事件的用量状态；无结果为 not_observed，多结果变体为 conflict',
    observation_status COMMENT '唯一结果事件的观察完整度；无结果为 not_observed，多结果变体为 conflict',
    input_tokens COMMENT '输入 token 总量；NULL 表示未知，0 表示服务报告的有效零值；子项不与父项重复相加',
    output_tokens COMMENT '输出 token 总量；NULL 表示未知，0 表示服务报告的有效零值；子项不与父项重复相加',
    request_finished_at_ms COMMENT '有效网关 End 提供的请求结束 Unix 毫秒时间；未收到有效 End 时为 NULL',
    terminal_observed COMMENT '是否收到有效网关 End：1 是、0 否；为 0 时请求结束时间必须为 NULL',
    business_outcome COMMENT '有没有观察到响应完整结束？response_complete、stream_incomplete 或 unknown；start 为 NULL',
    has_conflict COMMENT '请求是否有内容、时间、日期、归属或持久冲突标记：1 是、0 否；为 1 时禁止用于有效统计'
) AS
SELECT a.request_date, a.logical_request_id, a.start_variants, a.usage_variants,
       a.request_started_at_ms, a.x_request_id, a.customer_id, a.service_id, a.service_name, a.provider_name, a.customer_name, a.provider_id, a.cluster_name, a.deployment_environment, a.request_host, a.mapping_status, a.mapping_version, a.customer_mapping_status, a.customer_mapping_version,
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
                 OR a.started_count > 1 OR a.date_count > 1 OR a.identity_count > 1 OR a.name_count > 1
                 OR m.logical_request_id IS NOT NULL THEN 1 ELSE 0 END AS has_conflict
FROM (
    SELECT logical_request_id, MIN(request_date) AS request_date,
           SUM(CASE WHEN event_type = 'request_started' THEN 1 ELSE 0 END) AS start_variants,
           SUM(CASE WHEN event_type = 'usage_observed' THEN 1 ELSE 0 END) AS usage_variants,
           COUNT(DISTINCT request_started_at_ms) AS started_count,
           COUNT(DISTINCT request_date) AS date_count,
           COUNT(DISTINCT CAST(JSON_ARRAY(service_id, provider_id, api_key_id, customer_id, identity_status, config_version, gateway_instance_id, adapter_id, x_request_id, cluster_name, deployment_environment, mapping_status, mapping_version, customer_mapping_status, customer_mapping_version) AS STRING)) AS identity_count,
           COUNT(DISTINCT CAST(JSON_ARRAY(service_name, provider_name, customer_name) AS STRING)) AS name_count,
           MIN(request_started_at_ms) AS request_started_at_ms,
           MAX(x_request_id) AS x_request_id,
           MAX(customer_id) AS customer_id,
           MAX(provider_id) AS provider_id,
           MAX(service_id) AS service_id,
           CASE WHEN COUNT(DISTINCT CAST(JSON_ARRAY(service_name, provider_name, customer_name) AS STRING)) = 1 THEN MAX(service_name) END AS service_name,
           CASE WHEN COUNT(DISTINCT CAST(JSON_ARRAY(service_name, provider_name, customer_name) AS STRING)) = 1 THEN MAX(provider_name) END AS provider_name,
           CASE WHEN COUNT(DISTINCT CAST(JSON_ARRAY(service_name, provider_name, customer_name) AS STRING)) = 1 THEN MAX(customer_name) END AS customer_name,
           MAX(cluster_name) AS cluster_name, MAX(deployment_environment) AS deployment_environment, MAX(request_host) AS request_host, MAX(mapping_status) AS mapping_status, MAX(mapping_version) AS mapping_version, MAX(customer_mapping_status) AS customer_mapping_status, MAX(customer_mapping_version) AS customer_mapping_version,
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

CREATE OR REPLACE VIEW metering.request_statistics (
    request_date COMMENT 'UTC 请求日期，由 request_started_at_ms 换算，不由事件到达时间或 UUID 推导',
    logical_request_id COMMENT 'Observer 为一次观察生成的规范 UUIDv4；请求级主标识及 Kafka key',
    start_variants COMMENT '该逻辑请求的开始事件内容变体数；大于 1 表示冲突，不是调用次数',
    usage_variants COMMENT '该逻辑请求的结果事件内容变体数；0 表示未观察结果，大于 1 表示冲突',
    started_count COMMENT '该日期内同一逻辑请求的不同开始时间数量；大于 1 表示时间冲突',
    identity_count COMMENT '该日期内同一逻辑请求的不同十五项归属与部署快照数量；大于 1 表示冲突',
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
    has_conflict COMMENT '日期内请求变体、时间或归属冲突，或命中跨日期持久冲突标记时为 1；汇总 SQL 据此排除',
    model COMMENT '统计模型维度，优先响应模型、其次请求模型；两者均缺失用空串表示未知',
    usage_source COMMENT '统计用量来源分组：reported、unknown、missing、mixed；estimated_model 和 estimated_generic 为预留估算分类'
) AS
SELECT a.*,
       CASE WHEN a.start_variants > 1 OR a.usage_variants > 1
                 OR a.started_count > 1 OR a.identity_count > 1 OR a.name_count > 1
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
           MAX(CASE WHEN event_type = 'usage_observed' THEN reported_total_tokens END) AS reported_total_tokens
    FROM metering.events_raw
    GROUP BY request_date, logical_request_id
) a
LEFT JOIN metering.request_conflict_ids c ON a.logical_request_id = c.logical_request_id;
