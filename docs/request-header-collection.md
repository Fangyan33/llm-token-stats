# 请求头采集与用户用量查询

Wasm 对已选中的采集请求读取请求头，结果经过 Observer、Kafka、Consumer 写入 Doris。采集保存插件修改请求头前的原始值；采集失败放行业务。当前项目尚未上线，字段直接纳入现有 `schema_version=1` 契约及最终初始化 DDL。

```json
{
  "user_id_header": ["user-id", "x-user-id", "x-account-id"],
  "extra_request_headers": ["x-team-id", "x-channel"]
}
```

将以上字段加入 Wasm 配置，沿用现有 EnvoyFilter 配置发布方式。配置在新请求开始时生效；一次观察的开始事件和结果事件保持同一请求头快照。

快照在正文改写准备前冻结。例如白名单包含 `content-length` 时，保存客户端原始长度；正文改写仍会删除转发请求中的旧长度，避免与改写后的正文不符。

`user_id_header` 是有序的“或”列表，最多 8 项；依次选择第一个有效、能放入元数据预算的值。缺失、空白、非法、超过 512 UTF-8 字节或多个同名头的候选跳过，继续读取下一项。全部未命中保存 `NULL`。省略配置默认 `["user-id"]`，空数组关闭提取，显式 `null` 无效。用户 ID 按客户端原值保存，不参与鉴权，不代替客户/API Key 归属或内部 `observation_id`。

`user_agent` 固定读取标准 `User-Agent`，最多 2048 UTF-8 字节，缺失或无效保存 `NULL`。扩展列表最多 16 项，每个值最多 1024 UTF-8 字节；key 为小写 HTTP 头名，同名多个值按 Envoy 合并为字符串，不拆分逗号。未配置、未命中或无效的 key 不写入，空集合保存 `{}`。头名最长 128 字节，配置大小写不敏感且拒绝重复。所有字符串必须为有效 UTF-8，不接受 CR、LF、NUL 等控制字符，允许水平制表符。

三个字段规范 JSON 总计最多 4096 字节，并服从既有 6144 字节 Start 帧预算。依次优先保留用户 ID、User-Agent、扩展列表中的字段；超限只跳过完整字段，原始值不截断。Authorization、Proxy-Authorization、Cookie、Set-Cookie、x-api-key、api-key、x-remote-data 不允许配置为采集头。自定义白名单也不应包含其它凭据。诊断不记录头值。

数据库字段为 `user_id VARCHAR(512) NULL`、`user_agent VARCHAR(2048) NULL`、`request_headers MAP<STRING, STRING> NOT NULL`。`event_details`、`requests`、`request_statistics`、`usage_effective` 同步提供这些字段。事件规范编码按 Map key 排序，重排 key 不改变内容摘要。用户 ID 纳入请求快照一致性校验，不一致的同一观察按现有冲突规则隔离。

```sql
-- 请求明细：按用户及扩展 key 筛选；每个 observation_id 对应一次观察。
SELECT observation_id, user_id, user_agent, request_headers['x-channel'] AS channel
FROM metering.requests
WHERE user_id = 'user-123'
  AND request_headers['x-team-id'] = 'team-a';

-- 已结束日期的用户用量；根据实际报表范围增加客户、服务等过滤。
SELECT stat_date, cluster_name, deployment_environment, customer_id, user_id,
       SUM(request_count) AS requests, SUM(total_tokens) AS total_tokens
FROM metering.usage_daily_mv
WHERE user_id = 'user-123'
GROUP BY stat_date, cluster_name, deployment_environment, customer_id, user_id;
```

日/月物化视图在现有八个维度上增加 `user_id`，User-Agent 和扩展头用于明细查询。未知用户保持 `NULL` 分组，整体统计继续包含这些请求；统计资格、未知用量和已知零值的口径保持一致。月统计直接累计相同用户维度的日结果，由 Worker 按完整九个维度核对。刷新仍采用已结束日期的异步计算；查询时结合刷新状态判断覆盖范围。原有服务或客户总计查询继续通过 `SUM` 跨用户分组汇总。
