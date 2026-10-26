# Kafka 计量事件应用接入指南

本文面向订阅计量事件的应用开发者，描述[网关请求采集开发实现方案](gateway-request-collection-implementation.md)对应的当前 **schema_version=1 契约**。采集、Kafka 和 Doris 已有真实本地链路验证，记录见[验证记录](spec/plan/gateway-collection-verification.md)；生产 Broker、认证权限及完整 G0–G5 尚需在实际环境验收。

本页描述唯一有效事件模型：应用维度已删除，归属由 Observer 的服务/客户接口解析。两个归属接口支持配置 HTTP/HTTPS POST JSON 调用及 x-remote-data 认证；URI 为空时使用未实现占位。Observer 固定 prd/stg/dev 环境，在有界异步任务中取得 ID 和名称快照；service_name、provider_name、customer_name 经 Kafka 写入 Doris 独立列及 event_json。占位或失败侧的 ID、名称和版本为 null，不回源补写历史；原始 usage 保留，业务 token 不进入事件、数据库或日志。

Observer 默认对两个接口分别缓存完整成功结果 30 分钟；仅瞬时故障时额外沿用旧值最多 15 分钟，每条目失败后 5 秒内不重查。认证失败、业务码 100001/650013 或非法响应作废旧值。缓存命中及旧值回退沿用 mapped 和完整 ID/名称，事件字段不变；缓存来源通过 Observer 指标观察。名称是本次观察使用的归属快照，可能来自缓存，默认变更延迟最多约 30 分钟，持续瞬时故障时最多约 45 分钟；不保证是第三方此刻的最新值，也不改写历史。配置及边界见[归属结果缓存](spec/plan/host-service-provider-implementation.md#410-归属结果缓存)。

当前[归属接口契约](spec/plan/host-service-provider-implementation.md#48-错误契约与-observer-映射)中，HTTP 200 + `responseCode="650013"` 同时表示零条或多条结果，统一映射为 `lookup_invalid_response`；HTTP 200 + `responseCode="100001"` 及 HTTP 401 认证失败映射为 `lookup_unavailable`。当前远端适配器不再从错误码产生 `unknown_host` / `unknown_api_key`，历史事件枚举仍保留。响应包装字段不进入 Kafka，消费者继续读取两类固定映射状态及冻结的 ID、名称快照。

观察取消、超时或 ACK 失败时，已接纳的归属查询继续到完成或原查询截止时间，保留成功侧的 ID、名称及版本，未返回侧复核旧缓存回退资格，无法回退时标为 lookup_timeout。开始事件与结果事件共用冻结快照，迟到结果不回写；归属成功不恢复观察完整性或有效统计资格。该规则限于 Observer 正常运行期间，不保证事件必然投递到 Kafka。

归属查询在定稿前已确认 HTTP 401/403 响应头时，该侧保持 `lookup_unavailable`，ID、名称和版本为 null；诊断正文读取超时不能覆盖认证失败或恢复旧归属的 `mapped`。此规则同样作用于同键尚未冻结的缓存候选，已冻结事件不追溯改写，另一侧成功归属继续保留。

正式字段契约见 [事件 Schema](../api/events/v1/metering-event.schema.json)，合成事件见 [testdata/collection](../testdata/collection)。运行时还须执行 UTF-8 字节、状态关系、来源与 Kafka key 校验；[离线校验器](../scripts/validate_collection_contracts.py)提供可复现参考。Doris Consumer 的写入/冲突/dirty/提交恢复顺序见[闭环清单](gateway-collection-design-closure.md#5-consumer-提交与恢复屏障)。

采集与汇总协调使用 Doris 保存日期通知、消费批次凭据和发布状态，无额外 PostgreSQL 依赖。Consumer 在原始/拒收记录、冲突标记、日期通知及批次凭据确认可见后提交 offset，不等待尚未实现的 Worker 发布汇总；发布状态由未来 Worker 负责，参见[Doris 协调设计](doris-summary-coordination.md)。这些内部处理元数据不加入 schema_version=1 Kafka value，也不要求其他订阅应用建设同样的状态表；各应用仍按自己的幂等处理结果提交进度。

采集范围由 Wasm 配置 `accepted_request_hosts` 统一控制，支持精确域名、后缀通配和 `*`。无效或未匹配的 Host 不建立观察流、不发送事件，业务继续透传。Observer / Consumer 不维护服务或 Host 白名单；事件中的空 Host 与 invalid_host 状态为直接观察及历史诊断保留，不能据此认为当前 Wasm 会采集无效 Host。

## 1. 接入信息

| 项目 | 约定 |
| --- | --- |
| Topic | 由 Observer 和 Consumer 的 `kafka.topic` 配置决定；生产配置示例为 `llm-metering-events`，当前本地联调为 `llm-metering-development` |
| Kafka key | `logical_request_id` 的 UTF-8 字节；同一请求的事件使用同一个 key |
| Kafka value | UTF-8 JSON，固定字段集，`schema_version=1` |
| 事件类型 | `request_started` 与 `usage_observed`；不存在独立的 `request_finished` |
| 事件大小 | JSON 序列化后最多 32 KiB |
| 保留时间 | 设计初值 72 小时；应用应监控消费延迟，超出保留期无法靠 Kafka 补齐 |
| 消费组 | 每个下游应用使用自己的 group ID；不要复用 Doris Consumer 的生产示例组 `llm-metering-doris-v1` 或本地组 `llm-metering-development` |
| 权限与连接 | 申请 Topic 读取、消费组权限，以及实际集群的 Broker 地址和 TLS/SASL 配置；密钥通过环境或 Secret 注入 |

一次符合采集条件的逻辑请求最多形成两种事件：

1. `request_started`：Observer 接到有效请求开始观察消息后尝试发送。它说明观察到请求开始，不包含 token 用量。
2. `usage_observed`：Observer 定稿时尝试发送，合并用量和网关可观察的终态。即使 token 未知或没有收到有效的网关 End，也可能发送这条事件。

两条事件独立发送，可能只有一条到达；下游不能要求先看到 start 才处理 usage，也不能把 start 和 usage 算成两次模型调用。Kafka 重放可能带来物理重复。唯一请求标识是 `logical_request_id`，事件级标识是 `event_id`；`x_request_id` 仅供辅助排查。

### 1.1 `logical_request_id` 由 Observer 生成

Observer 是唯一的请求 ID 生成方。网关 Wasm 只在请求头回调记录 `request_started_at_ms` 并随首帧 `Start` 发送；它不生成或提交 `logical_request_id`。Observer 收到并校验有效 Start 后，通过操作系统安全随机源生成 **UUIDv4**：读取 16 字节随机数，设置版本位为 4、变体位为 RFC 规定值，输出 36 字符小写规范形式 `xxxxxxxx-xxxx-4xxx-[89ab]xxx-xxxxxxxxxxxx`。例如 `7f6c2a90-4b13-4b68-9d21-67283e0c5f44`。随机源失败则拒绝该观察流并报警，不退化为时间戳、Pod UID 或客户端请求头。[RFC 9562 UUIDv4](https://www.rfc-editor.org/rfc/rfc9562.html#section-5.4)

ID 与一个 gRPC 观察流绑定一次，Observer 在有效 Start ACK 中回传；后续 ACK、`request_started` 与 `usage_observed` 复用它。`observation_id` 一期等于该 UUID，Kafka key 是它的 UTF-8 字节，`event_id` 分别追加 `:start` 和 `:usage`。Kafka 发送失败、End 缺失或消息重放都不重新生成 ID；客户端重新发起请求得到新观察流和新 UUID。

UUIDv4 不包含时间。`request_started_at_ms` 仍由网关观察并作为事件字段，Consumer 从该字段计算 UTC `request_date`；不得从 UUID 推导日期。相同 UUID 的开始和结果事件应携带相同开始时间；不一致时按请求冲突处理。响应头 `X-Metering-Request-Id` 仅在网关发出响应头前收到有效 Start ACK 时才能携带该 UUID，故可能缺失；有响应头也不保证 Kafka 已收到事件。下游按事件 ID 去重，不以 `x_request_id` 重新构造请求 ID。具体协议、跨日期冲突和查询方式见[采集开发方案](gateway-request-collection-implementation.md#22-请求标识及时间)。

## 2. Kafka value 字段字典

表中的“可空”指 JSON 值可以为 `null`；可空字段仍应出现在规范事件中。所有 `*_at_ms` 均为 Unix 毫秒，绝对时间按 UTC 理解。字符串 `"0"` 与数字 `0` 不等价，未知 token 用 `null` 表示。

### 2.1 顶层字段

| 字段 | JSON 类型 / 可空 | 含义与使用规则 |
| --- | --- | --- |
| `schema_version` | 整数 / 否 | 事件结构版本，当前为 `1`。不认识的版本应暂停该分区并报警，不能按旧字段默认值继续处理。 |
| `event_id` | 字符串 / 否 | 事件级逻辑 ID；开始事件为 `<logical_request_id>:start`，结果事件为 `<logical_request_id>:usage`。用于去重并检测同 ID 不同内容的冲突。 |
| `event_type` | 字符串 / 否 | 仅 `request_started` 或 `usage_observed`。 |
| `logical_request_id` | 字符串 / 否 | Observer 收到有效 Start 后生成的规范小写 UUIDv4，同时也是 Kafka key 的文本值；同一观察的两种事件共用，不编码请求时间。 |
| `x_request_id` | 字符串 / 是 | 网关当时可见的 `X-Request-Id`，可能缺失、复用或来自客户端；只用于辅助关联，不用于去重或授权。 |
| `observation_id` | 字符串 / 否 | 一期与 `logical_request_id` 相同，表示本次观察。 |
| `observation_scope` | 字符串 / 否 | 当前固定为 `gateway_response`，表示网关可见的一次逻辑响应，并非所有上游尝试。 |
| `producer_id` | 字符串 / 否 | 生成事件的 Observer 实例标识。 |
| `gateway_instance_id` | 字符串 / 否 | 观察该请求的网关实例标识，新版为运行时 `namespace/pod-name`；同名 Pod 重建后不变，历史事件可为 Pod UID。 |
| `config_version` | 字符串 / 否 | 请求开始时冻结的采集配置版本，用于解释当时的采集规则。非空合法 UTF-8，最多 128 字节，需在 Observer 和 Consumer 的 `accepted_config_versions` 中登记；当前配置示例使用 `collection-v1`。 |
| `adapter_id` | 字符串 / 否 | 解析上游协议的适配器 ID；当前接入范围固定为 `chat_completions_v1`。 |
| `route_name` | 字符串 / 否 | 真实 Envoy HTTP 路由名；不可读时为 `unknown`。仅用于诊断、审计，不参与身份一致性、准入或价格选择；不是 VirtualService 名称或后端 ID。字段保留非空字符串，业务上可缺失；当前网关不输出 `*`，Consumer 不按诊断值判断身份。事件不携带原始 URL 或 query。 |
| `request_host` | 字符串 / 是 | 规范入口 DNS 域名；非法输入为 null，不保存 URL/query。 |
| `cluster_name` | 字符串 / 否 | Observer 配置的固定集群名称，客户端不可覆盖。 |
| `deployment_environment` | 字符串 / 否 | Observer 固定 prd/stg/dev。 |
| `provider_id` | 字符串 / 是 | 服务接口返回的供应商归属；未实现或失败为 null。 |
| `mapping_status` / `customer_mapping_status` | 字符串 / 否 | 两类查询各自的固定状态；成功为 mapped，空 URI 为 lookup_not_implemented，非法输入为 invalid_host/invalid_api_key，HTTP 故障按归属接口契约分类。 |
| `mapping_version` / `customer_mapping_version` | 字符串 / 是 | 成功结果的规范 SHA-256 摘要；未知时为 null。 |
| `service_id` | 字符串 / 是 | Observer 按 request_host + 固定集群/环境查询服务；占位或失败为 null，不填默认服务。 |
| `api_key_id` | 字符串 / 是 | 客户查询接口返回的稳定 API Key ID，不是原始 token；未实现或失败时为 null。 |
| `customer_id` | 字符串 / 是 | 与 api_key_id 同次查询得到的客户快照，二者成对成功；不从身份元数据补值。 |
| `service_name` | 字符串 / 是 | 请求发生时的服务名称快照，非空合法 UTF-8、最多 512 字节；对应解析失败为 null。两类事件一致，重命名不覆盖历史。 |
| `provider_name` | 字符串 / 是 | 请求发生时的供应商名称快照，非空合法 UTF-8、最多 512 字节；对应解析失败为 null。两类事件一致，重命名不覆盖历史。 |
| `customer_name` | 字符串 / 是 | 请求发生时的客户名称快照，非空合法 UTF-8、最多 512 字节；对应解析失败为 null。两类事件一致，重命名不覆盖历史。 |
| `identity_status` | 字符串 / 否 | 客户接口完整成功时 known，否则 invalid；不表示鉴权判定，invalid 不进入有效客户汇总。 |
| `request_started_at_ms` | 整数 / 否 | 网关观察到请求头的开始时间，也是统计归属和 UTC `request_date` 的来源；与 UUIDv4 独立，同一请求的两种事件必须一致。 |
| `observation_ended_at_ms` | 整数 / 是 | Observer 结束本次观察的时间；start 尚未结束时为 `null`。它不等于网关确认业务请求结束。 |
| `request_finished_at_ms` | 整数 / 是 | 网关有效 End 中取得的请求结束时间；未收到有效 End 时为 `null`，不能补当前时间。 |
| `event_created_at_ms` | 整数 / 否 | Observer 创建此 Kafka 事件的时间；通常晚于请求开始时间。 |
| `request_model` | 字符串 / 是 | 请求体中的模型名称或 ID；未可靠取得或无效时为 `null`。 |
| `response_model` | 字符串 / 是 | 响应上报的模型名称或 ID；可能与请求值不同，未可靠取得时为 `null`。 |
| `http_status` | 整数 / 是 | 网关可见的 HTTP 响应状态码；不是用量是否为零的判断依据。 |
| `upstream_attempt_count` | 整数 / 是 | 网关能可靠取得的上游尝试次数；未知为 `null`，不能据此推算隐藏重试的 token。 |
| `response_flags` | 字符串 / 是 | 经采集规则限制的网关响应标志，供故障排查。 |
| `response_code_details` | 字符串 / 是 | 经采集规则限制的网关响应诊断码；不保存任意上游错误文本。 |
| `business_outcome` | 字符串 / 是 | 有没有观察到响应完整结束？`response_complete` 表示观察到完整结束；`stream_incomplete` 表示已有部分请求或响应信息，但未观察到完整结束；`unknown` 表示信息不足，无法判断。start 为 `null`。不表示 HTTP 成功，也不保证采集完整。 |
| `terminal_observed` | 布尔 / 否 | 是否收到有效网关 End；为 `false` 时 `request_finished_at_ms` 必须为 `null`。 |
| `observation_status` | 字符串 / 是 | 结果事件为 `complete`（观察完整）、`partial`（缺片/截断等）或 `unknown`（只有有限元数据）；start 尚未定稿时为 `null`。 |
| `usage_status` | 字符串 / 否 | token 数字有没有拿到，是否齐全、是否矛盾？start 为 `not_applicable`；结果事件中，`known` 表示输入、输出总量都有效；`partial` 表示输入输出尚未全部有效，但六个用量字段中至少一个有效；`unknown` 表示六字段全未知；`conflict` 表示用量存在矛盾，优先于其他状态。 |
| `usage_eligible` | 布尔 / 否 | 这条用量是否符合有效统计条件，可以作为统计候选？一期仅在适配成功、`observation_status=complete`、`usage_status=known`、`identity_status=known`，且无用量/身份冲突及使结果失效的异常时为 `true`；否则为 `false`，start 固定为 `false`。不表示收费资格；下游仍须去重并排除事件或请求冲突。 |
| `usage` | 对象 / 是 | start 固定为 `null`；结果事件为含六个 token 子字段的对象。即使用量未知，结果事件也保留各字段的 `value/source` 状态。 |
| `raw_usage_json` | 字符串 / 否 | 白名单保留的上游 usage JSON 依据，最多 4 KiB；它是**JSON 字符串**，不是嵌套对象，也不是完整响应。无依据时为 `"{}"`。 |
| `raw_usage_truncated` | 布尔 / 否 | usage 依据是否因过滤或大小限制丢弃内容；不表示模型回答被截断。 |
| `anomaly_codes` | 字符串数组 / 否 | 固定异常码列表，无异常为 `[]`；已去重排序，不包含正文或错误堆栈。常见值包括 `missing_usage`、`missing_end`、`usage_conflict`、`protocol_incomplete`、`identity_invalid`。 |

`business_outcome`、`usage_status`、`usage_eligible` 分别描述响应结束情况、用量数据状态和统计候选资格，不能互相替代；判定规则与场景示例见[第 5 节常见问答](#5-常见问答)。

### 2.2 `usage` 对象中的每个字段

每个子字段统一为 `{"value": 非负整数或 null, "source": "reported" 或 "unknown"}`。`reported` 表示有效的上游上报值；`unknown` 表示无法可靠取得，此时 `value=null`。上游明确上报的 `0` 必须保留为 `0/reported`，不能与未知混淆。当前采集子项不产生估算来源。

| `usage` 子字段 | 上游 Chat Completions 路径 | 含义与包含关系 |
| --- | --- | --- |
| `input_tokens` | `usage.prompt_tokens` | 本次请求的输入 token 总量，含已命中的缓存输入及已写入缓存的输入部分。 |
| `output_tokens` | `usage.completion_tokens` | 本次请求的输出 token 总量，包含可单独报告的推理 token。 |
| `cached_read_tokens` | `usage.prompt_tokens_details.cached_tokens` | 本次输入中从提示词缓存复用的 token 数，已包含在 `input_tokens` 中。 |
| `cached_write_tokens` | `usage.prompt_tokens_details.cache_write_tokens`（可选） | **本次请求的输入中，有多少 token 对应的可复用前缀被写入提示词缓存。**已包含在 `input_tokens` 中；字段缺失为 `null/unknown`，不从文本或其他总量推断。 |
| `reasoning_tokens` | `usage.completion_tokens_details.reasoning_tokens` | 可报告的推理 token 数，已包含在 `output_tokens` 中。 |
| `reported_total_tokens` | `usage.total_tokens` | 上游原样上报的总量；不使用输入加输出的推导值覆盖。 |

`cached_read_tokens`、`cached_write_tokens` 和 `reasoning_tokens` 是细分量，不能再加到输入/输出总量上。两项缓存值都已知时，其和不得大于输入总量。兼容 OpenAI 的服务未必返回可选的 `cache_write_tokens`；接入时应核对实际响应。[OpenAI Chat Completions 用量定义](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)

### 2.3 两种事件的字段差异

| 字段组 | `request_started` | `usage_observed` |
| --- | --- | --- |
| `event_id` | `<logical_request_id>:start` | `<logical_request_id>:usage` |
| `usage` | `null` | 六个 `{value,source}` 子字段 |
| `usage_status` / `usage_eligible` | `not_applicable` / `false` | 按定稿结果填写；仅满足有效用量统计候选条件时，`usage_eligible` 才为 `true` |
| `raw_usage_json` | `"{}"` | 白名单 usage 依据；没有则 `"{}"` |
| `observation_ended_at_ms` / `request_finished_at_ms` | `null` / `null` | 能取得时填写；没有有效 End 时后者为 `null` |
| `terminal_observed` / `observation_status` | `false` / `null` | 按网关 End 和实际采集完整性填写 |
| 模型、HTTP、结果等后续字段 | 未取得时为 `null` | 已可靠取得时填写，否则仍为 `null` |

两个事件都携带请求开始时间、服务/客户解析状态、部署上下文及路由快照，故仅结果事件也能解释已知或未知归属。这里的“事件到达”仅表示应用从 Kafka 读到记录，不证明另一条事件必然存在。

## 3. 完整的结果事件示例

下例的输入总量为 1,000，其中缓存读取 200、缓存写入 300、其余输入 500；输出总量为 500，其中推理 100。缓存读写与推理均是总量中的细分量。

```json
{
  "schema_version": 1,
  "event_id": "7f6c2a90-4b13-4b68-9d21-67283e0c5f44:usage",
  "event_type": "usage_observed",
  "logical_request_id": "7f6c2a90-4b13-4b68-9d21-67283e0c5f44",
  "x_request_id": "b3e1fba5-16a4-4dfb-aa84-50141e80d473",
  "observation_id": "7f6c2a90-4b13-4b68-9d21-67283e0c5f44",
  "observation_scope": "gateway_response",
  "producer_id": "observer-pod-uid",
  "gateway_instance_id": "istio-system/gateway-example",
  "config_version": "collection-v1",
  "adapter_id": "chat_completions_v1",
  "route_name": "llm-chat",
  "request_host": "model.example.invalid",
  "cluster_name": "fixture-cluster",
  "deployment_environment": "stg",
  "service_id": "model-service-a",
  "provider_id": "provider-a",
  "mapping_version": "e475bfa24c9f16841c8cb11f66586faf008e144eddc1fa7ff3595bae6999905b",
  "mapping_status": "mapped",
  "api_key_id": "key-42",
  "customer_id": "customer-7",
  "customer_mapping_version": "1713a45606930ab59d794cd8ddd92eff6ce2df588201f4df1ec8bf5354748081",
  "customer_mapping_status": "mapped",
  "identity_status": "known",
  "request_started_at_ms": 1789862400000,
  "observation_ended_at_ms": 1789862401200,
  "request_finished_at_ms": 1789862401195,
  "event_created_at_ms": 1789862401201,
  "request_model": "model-a",
  "response_model": "model-a-2026",
  "http_status": 200,
  "upstream_attempt_count": null,
  "response_flags": null,
  "response_code_details": null,
  "business_outcome": "response_complete",
  "terminal_observed": true,
  "observation_status": "complete",
  "usage_status": "known",
  "usage_eligible": true,
  "usage": {
    "input_tokens": {
      "value": 1000,
      "source": "reported"
    },
    "output_tokens": {
      "value": 500,
      "source": "reported"
    },
    "cached_read_tokens": {
      "value": 200,
      "source": "reported"
    },
    "cached_write_tokens": {
      "value": 300,
      "source": "reported"
    },
    "reasoning_tokens": {
      "value": 100,
      "source": "reported"
    },
    "reported_total_tokens": {
      "value": 1500,
      "source": "reported"
    }
  },
  "raw_usage_json": "{\"prompt_tokens\":1000,\"completion_tokens\":500,\"total_tokens\":1500,\"prompt_tokens_details\":{\"cached_tokens\":200,\"cache_write_tokens\":300},\"completion_tokens_details\":{\"reasoning_tokens\":100}}",
  "raw_usage_truncated": false,
  "anomaly_codes": [],
  "service_name": "模型服务 A",
  "provider_name": "供应商 A",
  "customer_name": "客户 A"
}
```

Kafka record 的 key 是上述 `logical_request_id`，不在 JSON 内另设 `kafka_key`。Kafka topic、partition、offset 和消费时间是传输/消费元数据，不属于事件 JSON；Doris 的 `payload_hash`、`identity_hash`、`event_json`、`ingested_at` 等也是消费后产生的列，不要求上游事件携带。

## 4. 消费处理顺序

1. 用独立消费组订阅 Topic，关闭自动提交。新组从最早可用 offset 还是最新 offset 开始，须在接入时明确；示例从保留窗口内最早记录开始。
2. 按 UTF-8 JSON 解析，校验最多 32 KiB、`schema_version`、`logical_request_id` 是规范小写 UUIDv4（`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`）、Kafka key 与该 ID 一致、事件类型和 `event_id` 后缀。未知版本停止处理并报警，不能跳过后提交。
3. 根据 `event_id` 做幂等处理；若同一 `event_id` 出现不同规范事件内容，隔离并报警，不能随意挑一条覆盖。用量只来自 `usage_observed`，而且是否进入正式统计还需检查 `usage_eligible` 和请求冲突。
4. 业务处理或持久化成功后再提交对应分区的下一个 offset。处理成功但提交失败会重放，所以幂等是必要条件；永久非法记录应走有界诊断流程，不能静默跳过。
5. 监控每个分区的消费延迟与 72 小时保留窗口。单个请求缺少 start 或 usage 时保留“缺失/未知”状态，不用零值补齐。

下面的 Go 和 Java 示例只演示连接、读取、关键字段解析及手动提交。示例中的打印处理应替换为应用自己的持久化与冲突检测；生产环境还需加入实际 TLS/SASL、完整 schema 校验、异常隔离和监控。

两个示例都要求设置 `KAFKA_BROKERS` 和 `KAFKA_TOPIC`；`KAFKA_GROUP` 可选，默认 `my-app-metering-v1`。连接当前本地联调栈时可使用：

```sh
export KAFKA_BROKERS=127.0.0.1:29092
export KAFKA_TOPIC=llm-metering-development
export KAFKA_GROUP=my-app-metering-v1
```

### 4.1 Go 示例

依赖：`github.com/twmb/franz-go/pkg/kgo`（方案中使用 v1.18.1）。设置上述连接参数后运行。

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/signal"
    "strings"
    "syscall"

    "github.com/twmb/franz-go/pkg/kgo"
)

type TokenValue struct {
    Value  *int64 `json:"value"`
    Source string `json:"source"`
}

type Event struct {
    SchemaVersion    int    `json:"schema_version"`
    EventID          string `json:"event_id"`
    EventType        string `json:"event_type"`
    LogicalRequestID string `json:"logical_request_id"`
    Usage            *struct {
        CachedWriteTokens TokenValue `json:"cached_write_tokens"`
    } `json:"usage"`
}

func handle(r *kgo.Record) error {
    if len(r.Value) > 32*1024 {
        return fmt.Errorf("event too large")
    }
    var e Event
    if err := json.Unmarshal(r.Value, &e); err != nil {
        return err
    }
    if e.SchemaVersion != 1 {
        return fmt.Errorf("unsupported schema_version: %d", e.SchemaVersion)
    }
    if string(r.Key) != e.LogicalRequestID {
        return fmt.Errorf("Kafka key and logical_request_id differ")
    }
    suffix := map[string]string{
        "request_started": ":start",
        "usage_observed":  ":usage",
    }[e.EventType]
    if suffix == "" || e.EventID != e.LogicalRequestID+suffix {
        return fmt.Errorf("invalid event identity")
    }
    if e.EventType == "request_started" && e.Usage != nil {
        return fmt.Errorf("start event must have usage=null")
    }
    if e.EventType == "usage_observed" && e.Usage == nil {
        return fmt.Errorf("usage event must have a usage object")
    }

    var write any = "unknown"
    if e.Usage != nil && e.Usage.CachedWriteTokens.Value != nil {
        write = *e.Usage.CachedWriteTokens.Value // 0 仍是有效值
    }
    // 将这里替换为按 event_id 幂等写入；同 ID 不同内容应隔离。
    fmt.Printf("%s %s cached_write_tokens=%v\n", e.EventID, e.EventType, write)
    return nil
}

func main() {
    brokers := os.Getenv("KAFKA_BROKERS")
    if brokers == "" {
        log.Fatal("KAFKA_BROKERS is required")
    }
    topic := os.Getenv("KAFKA_TOPIC")
    if topic == "" {
        log.Fatal("KAFKA_TOPIC is required")
    }
    group := os.Getenv("KAFKA_GROUP")
    if group == "" {
        group = "my-app-metering-v1"
    }
    cl, err := kgo.NewClient(
        kgo.SeedBrokers(strings.Split(brokers, ",")...),
        kgo.ConsumerGroup(group),
        kgo.ConsumeTopics(topic),
        kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
        kgo.DisableAutoCommit(),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer cl.Close()

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    for ctx.Err() == nil {
        fetches := cl.PollRecords(ctx, 100)
        if ctx.Err() != nil {
            break
        }
        var fetchErr error
        fetches.EachError(func(topic string, partition int32, err error) {
            fetchErr = fmt.Errorf("%s[%d]: %w", topic, partition, err)
        })
        if fetchErr != nil {
            log.Print(fetchErr)
            return
        }
        for _, r := range fetches.Records() {
            if err := handle(r); err != nil {
                log.Print(err) // 未提交；生产环境按版本/非法事件规则处理
                return
            }
            if err := cl.CommitRecords(ctx, r); err != nil {
                log.Print(err)
                return
            }
        }
    }
}
```

### 4.2 Java 示例

依赖：`org.apache.kafka:kafka-clients` 与 `com.fasterxml.jackson.core:jackson-databind`；示例使用 Java 17+。设置上述连接参数后运行。

```java
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MeteringConsumer {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static void handle(ConsumerRecord<String, byte[]> record) throws Exception {
        if (record.value() == null || record.value().length > 32 * 1024) {
            throw new IllegalArgumentException("invalid event size");
        }
        JsonNode event = JSON.readTree(record.value());
        if (event.path("schema_version").asInt(-1) != 1) {
            throw new IllegalArgumentException("unsupported schema_version");
        }
        String requestId = event.path("logical_request_id").asText();
        String type = event.path("event_type").asText();
        String suffix = switch (type) {
            case "request_started" -> ":start";
            case "usage_observed" -> ":usage";
            default -> throw new IllegalArgumentException("unknown event_type");
        };
        if (!requestId.equals(record.key())
                || !event.path("event_id").asText().equals(requestId + suffix)) {
            throw new IllegalArgumentException("invalid event identity");
        }
        JsonNode usage = event.path("usage");
        if (type.equals("request_started") && !usage.isNull()
                || type.equals("usage_observed") && !usage.isObject()) {
            throw new IllegalArgumentException("invalid usage for event type");
        }
        JsonNode write = usage.path("cached_write_tokens").path("value");
        String writeText = write.isNumber() ? write.asText() : "unknown";
        // 将这里替换为按 event_id 幂等写入；同 ID 不同内容应隔离。
        System.out.printf("%s %s cached_write_tokens=%s%n",
                event.path("event_id").asText(), type, writeText);
    }

    public static void main(String[] args) throws Exception {
        String brokers = System.getenv("KAFKA_BROKERS");
        if (brokers == null || brokers.isBlank()) {
            throw new IllegalArgumentException("KAFKA_BROKERS is required");
        }
        String topic = System.getenv("KAFKA_TOPIC");
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("KAFKA_TOPIC is required");
        }
        String group = System.getenv().getOrDefault("KAFKA_GROUP", "my-app-metering-v1");
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, byte[]> record : records) {
                    handle(record); // 处理失败时抛错并停止，不提交该记录
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    consumer.commitSync(Map.of(
                            partition, new OffsetAndMetadata(record.offset() + 1)));
                }
            }
        }
    }
}
```

两个示例均逐条提交，便于展示“处理成功后提交”。批量提交时只能提交**同一分区已连续处理成功**的下一 offset，不能跳过失败记录。[franz-go 消费与手动提交](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) · [Apache Kafka Java Consumer 手动提交说明](https://kafka.apache.org/34/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

## 5. 常见问答

### Q1：`business_outcome` 表示调用成功或失败吗？

**A：它描述观察到的结束情况，HTTP 成败另看 `http_status`。**

- `response_complete`：观察到完整结束。即使 HTTP 500，错误响应也可能完整结束。
- `stream_incomplete`：已有部分请求或响应信息，但没有观察到完整结束。可能是业务连接中断，也可能是采集链路丢失或超时，不能直接认定客户主动取消。
- `unknown`：信息不足，无法判断结束情况。
- `null`：用于 `request_started`，此时尚未确定结果。

它也不等于 `observation_status`。例如流式响应传输已结束且收到有效网关 End，但缺少协议要求的 `[DONE]` 标记，`business_outcome` 仍可能是 `response_complete`，而采集完整性为 `observation_status=partial`。

### Q2：`usage_status` 的几个值怎么理解？

**A：看用量字段是否已知，以及它们之间是否有冲突。**

| 值 | 含义与示例 |
| --- | --- |
| `not_applicable` | 开始事件不承载用量，此时 `usage=null`。 |
| `known` | 输入、输出总量都有有效值，且无用量冲突，例如输入 1,000、输出 500。 |
| `partial` | 输入输出尚未全部有效，但至少一个用量字段有效，例如输入已知、输出未知；只有上报总量已知也属于此状态。 |
| `unknown` | 六个用量字段全部未知，例如响应未上报任何用量。 |
| `conflict` | 数字或多次上报互相矛盾，例如输入 1,000、输出 500，但上报总量是 1,200。此状态优先于其他状态。 |

`known` 不要求缓存、推理等细分字段全部已知；`partial` 指用量字段不齐全，不表示回答只生成了一半。未知值用 `null`，不能当作 `0`；只有上游明确上报的有效零值才记为零。

### Q3：什么情况下 `usage_eligible=true`？可以直接累加或收费吗？

**A：只有数字可靠、采集完整、身份有效，并通过相关检查，才是有效统计候选。**

条件包括适配成功、`observation_status=complete`、`usage_status=known`、`identity_status=known`，以及没有用量/身份冲突和使结果失效的异常。身份有效指客户接口返回的 API Key ID、客户 ID 齐全且 customer_mapping_status=mapped。当前占位部署不产生有效客户身份。

`true` 不代表收费资格；下游仍需按 `event_id` 去重，并排除事件或请求冲突。`false` 不代表没有消耗 token，也不代表调用失败，记录仍可用于异常排查和采集质量分析。开始事件固定为 `false`。

### Q4：为什么响应完整、用量已知，也可能不能进入有效统计？

**A：三个字段检查的内容不同，任一统计条件不满足都会使 `usage_eligible=false`。** 以下均为结果事件；标为 `true` 的场景假设其他检查也全部通过。

| 场景 | `business_outcome` | `usage_status` | `usage_eligible` |
| --- | --- | --- | --- |
| 响应完整结束，用量齐全，采集和身份正常 | `response_complete` | `known` | `true` |
| 响应完整结束，但六个用量字段全部未知 | `response_complete` | `unknown` | `false` |
| 响应完整结束，只取得输入量 | `response_complete` | `partial` | `false` |
| 已解析到流式终结 usage，之后观察中断，缺少完整结束信息 | `stream_incomplete` | `known` | `false` |
| 响应和用量完整，但客户身份缺失 | `response_complete` | `known` | `false` |
| 响应完整结束，但输入、输出与上报总量对不上 | `response_complete` | `conflict` | `false` |
