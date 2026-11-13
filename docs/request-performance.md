# 请求级 TTFT 与 TPOT

性能指标只使用服务端上报的响应 token，不使用请求事件中的输入估算。Wasm 记录响应正文回调时间，Observer 解析 SSE 并提取首末有效生成内容的时间，Consumer 按原值入库。TTFT、TPOT 仅由 Doris 普通明细视图计算，不作为 Kafka 字段或实体表列保存。

## 时间事实

`BodyChunk.observed_elapsed_us` 是响应正文回调入口距请求头回调的微秒耗时。同一回调拆分的全部 gRPC 片段使用相同时间，Observer 的传输、接收和解析排队时间不参与计算。请求上传和 usage 参数改写发生在计时范围内。

结果事件和 `events_raw` 保存三个可空字段：

| 字段 | 含义 |
| --- | --- |
| `response_streaming` | 根据响应 Content-Type 判断：SSE 为 true、JSON 为 false，未知类型或未取得响应头为 NULL |
| `first_output_elapsed_us` | 首个有效生成内容所在完整 SSE 事件的最后字节所属回调时间，距请求头的微秒偏移 |
| `last_output_elapsed_us` | 最后观测到的有效生成内容所在完整 SSE 事件的最后字节所属回调时间，距请求头的微秒偏移 |

有效内容是任意 choice 的非空字符串 `delta.content`、`reasoning_content`、`reasoning`、`refusal`，以及 `tool_calls[].function` 或旧式 `function_call` 中的非空名称、参数。空格字符串属于有效内容；角色、工具 ID、空 delta、心跳、finish_reason、usage 和 `[DONE]` 不推进首末时间。多 choice 使用所有候选中最早和最晚的有效内容时间。

开始事件的三个字段均为 NULL。非流式 JSON 不填写首末内容时间；即使完整 JSON 中包含内容和 usage，也无法从整个响应还原首 token 生成时间。未知或不支持的编码不解析正文，不产生内容时间。

缺失、负值或倒退的回调时间会使后续时间不可信：保留已可靠取得的首输出时间，将末输出时间置 NULL，后续片段不重新开始计时。空正文 EOS 不改变已观测时间。该降级不增加用量异常，也不影响上报 usage 的接纳、统计资格或业务转发。

## 视图与公式

`event_details` 按事件计算并返回可空 DOUBLE 列：

```text
ttft_ms = first_output_elapsed_us / 1000.0
tpot_ms = (last_output_elapsed_us - first_output_elapsed_us) / 1000.0 / (output_tokens - 1)
```

TTFT 只要求流式响应及可靠的首内容时间，不依赖 usage 或有效 End。TPOT 还要求首末时间有效、`terminal_observed=1`、`observation_status=complete`、usage 无冲突、`output_tokens_source=reported` 且 `output_tokens>1`。输入量未知、客户归属未知或 `usage_eligible=false` 本身不阻止 TPOT 计算。

| 场景 | TTFT | TPOT |
| --- | --- | --- |
| 首内容 100ms、末内容 300ms、输出 5 token | 100ms | 50ms/token |
| 仅输出量已知，其余条件满足 | 可计算 | 可计算 |
| usage 缺失 | 保留可测值 | NULL |
| 输出量为 0 或 1 | 保留可测值 | NULL |
| 未收到 End 或响应不完整 | 保留可靠首时间 | NULL |
| 末时间未知 | 保留可靠首时间 | NULL |
| 非流式或首时间未知 | NULL | NULL |

`requests` 从结果事件复用指标，每请求一行；请求冲突时两项均为 NULL。`event_details` 只排除事件级冲突，查询诊断明细时仍需关注请求级冲突。`usage_effective` 透出两项指标，但会过滤缺失或不完整用量，不能用它查询缺失 usage 请求的 TTFT。

```sql
SELECT observation_id, usage_status, observation_status,
       ttft_ms, tpot_ms, has_conflict
FROM metering.requests
WHERE request_date = CURRENT_DATE();
```

这两个指标反映网关观察到的响应表现，不等于模型内部逐 token 生成耗时。一个回调可能合并多个 token，因此 TPOT 可以为零；多 choice 的 TPOT 是请求整体归一口径。服务上报的输出量照原值使用，不推断隐藏消耗。普通视图不物化指标；修改公式会影响历史查询结果。日/月用量汇总不增加延迟指标。

当前仍为唯一的 schema_version=1 契约，直接更新初始化 SQL，不升级旧库或支持旧字段集合；新库与更新后的 Observer、Consumer 配套使用。运行验证边界见[验证记录](spec/plan/request-performance-verification.md)。
