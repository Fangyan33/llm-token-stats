# 请求事件的输入 token 估算

显式开启 Wasm 的 `request_body_capture_enabled` 后，Observer 使用完整合法请求正文计算输入 token 估算，只记录到正式 `request_started` 事件的 `usage.input_tokens`，经 Kafka、Consumer 写入 Doris `events_raw` 和 `event_details`。成功值的 source 为 `estimated`；关闭时不读取或传输请求正文。

响应事件 `usage_observed` 继续使用模型服务返回的准确 usage，只允许 `reported/unknown`。上游缺失时保持 null/unknown，不使用请求估算补齐。`requests`、`usage_effective`、日/月汇总及 TPOT 继续使用响应事件的数值。request_model 由同一次请求 JSON 解析提取，仅写请求事件，关闭采集或未知时为 null。

## 配置与额度

```yaml
# Wasm 配置片段，默认 false
request_body_capture_enabled: true

# Observer 配置片段
observer:
  request_body_max_bytes: 33554432
  request_tokenizer_workers: 0
  request_tokenizer_queue_requests: 200
  request_tokenizer_queue_bytes: 536870912
```

| Observer 字段 | 默认值 | 范围/含义 |
| --- | --- | --- |
| request_body_max_bytes | 33554432 | 1–134217728 字节；超过限制停止本次输入计数，业务正文仍透传 |
| request_tokenizer_workers | 0 | 0 自动取 max(1, GOMAXPROCS−1)；显式 1–1024；每实例执行任务数 |
| request_tokenizer_queue_requests | 200 | 1–65536；每实例等待任务数，不是活跃业务/SSE 数 |
| request_tokenizer_queue_bytes | 536870912 | 1–34359738368 字节；等待任务正文及分片开销 |

Wasm 仅在取得额度后读取最多 16 KiB 分片并旁路发送；不解析、校验 JSON，不完整缓冲、改写或暂停业务。请求方向使用独立序号、偏移和 EOS。请求体最多占 Pod 传输字节与条数额度的四分之一，并同时计入总额度；额度不足只停止请求输入采集，响应观察继续。共享传输故障仍可能影响同通道的响应观察，业务按 fail-open 路径透传。

Observer 在独立有界池分词。等待条数和字节先达到任一限制即拒绝本次计数；任务出队后继续持有正文与执行额度。请求方向计入 parser_budget_bytes，最多使用其中一半，拼接/JSON/BPE 前另预留 `32 × body_bytes + 65536` 临时额度。运行任务持有槽位直到返回，关闭时不创建替补 goroutine。逻辑额度不能代替网关 RSS 对照验收。

200 项队列用于吸收突发，不是 200 QPS 的吞吐承诺。持续能力取决于文本、Worker、CPU 和 Observer 分流；扩大队列不能提高分词持续吞吐。

## 事件内容与顺序

关闭采集时，请求事件保持 `usage=null`、`usage_status=not_applicable`。开启且成功时，请求事件带完整六项 usage 对象，仅 input_tokens 有值，其余为 `null/unknown`；usage_status 为 partial，usage_eligible 始终为 false。合法空文本记录为 `0/estimated`。

例如请求 model 为 requested-model、输入估算为 123 时：

```json
{
  "event_type": "request_started",
  "request_model": "requested-model",
  "usage_status": "partial",
  "usage_eligible": false,
  "usage": {
    "input_tokens": {"value": 123, "source": "estimated"},
    "output_tokens": {"value": null, "source": "unknown"},
    "cached_read_tokens": {"value": null, "source": "unknown"},
    "cached_write_tokens": {"value": null, "source": "unknown"},
    "reasoning_tokens": {"value": null, "source": "unknown"},
    "reported_total_tokens": {"value": null, "source": "unknown"}
  }
}
```

这只是字段片段，完整固定字段集见 [Kafka 事件契约](kafka-event-integration-guide.md)。开启后正文缺失、非法 JSON、不完整、压缩编码、超限或计数资源不足时，input_tokens 为 `null/unknown`，usage_status 为 unknown；不产生虚构的 0。固定失败原因只计入 Observer 指标，不追加使响应统计失效的异常码。

归属快照按原有超时冻结，响应解析和 ACK 不等待分词。请求事件等待归属与计数结果后只发送一次，不先发送 null 再覆盖同一 `:start` ID。分词较慢时，`:usage` 可以先于 `:start` 入库；两者通过 observation_id 关联，Consumer 和查询不得依赖事件到达顺序。request_started_at_ms 始终是原始请求头时间，event_created_at_ms 为事件准备发送时间。

## 请求模型

Wasm 与内部传输协议不变。已开启 request_body_capture_enabled 时，Observer 复用现有的完整 JSON 校验及顶层字段扫描提取 model，保存到 request_started.request_model。Consumer 原样写入已有 request_model 类型化列和 event_json，event_details 可直接查询；响应事件的 request_model 仍为 null，response_model 来自上游响应。

该字段仅记录客户端请求的模型标识，不切换固定的分词编码，也不用于推断实际执行模型。仅保存非空白、无控制字符、合法 UTF-8 的字符串，最多 1024 字节。对编码后的字段先按最大 JSON 转义长度限制，再解码，避免恶意超长 model 分配大临时副本。缺失、类型错误、超长或非法 model 只将该字段置 null，不影响输入计数或响应用量。

JSON 完整合法且 model 有效时，即使 messages 不支持计数，仍可保存 model，同时输入 token 保持 null/unknown。正文不完整、非法 JSON、超限、队列或执行额度不足时，不尝试从前缀猜测 model。新增处理只在 Observer 进行，不增加网关请求体读取、复制或旁路传输。

## 计数口径

固定 `o200k_base / text_tools_v1`。各消息字符串 content、数组中的 text 块分别计数；消息内 tool_calls/function_call、顶层 tools/functions 按键排序、保留数组顺序的紧凑 JSON 分别计数后求和。JSON 数字保留原字面形式，字符串使用 Go JSON 转义并关闭 HTML 转义。传输片段不是分词边界。

不计算 role/name 等额外字段，不加聊天模板补偿，不计算图片、音频、视频本身；特殊 token 字面量按普通文本处理。仅完整合法 UTF-8 JSON、非空 messages 数组且包含有效计数项时产生数值。因此这是通用文本与工具估算，与 vLLM/SGLang 的模型 tokenizer、聊天模板产生的准确 prompt_tokens 可能不同。

Observer 原生依赖 github.com/tiktoken-go/tokenizer v0.7.0，不编入 Wasm。Go 1.25.0、TinyGo 0.40.1、proxy-wasm-go-sdk v0.24.0 不升级。正文和凭据不进入 Kafka 事件、Doris 或日志。

## 查询与上线

```sql
SELECT observation_id, request_model, input_tokens, input_tokens_source, usage_status
FROM metering.event_details
WHERE event_type = 'request_started'
  AND observation_id = '<observation_id>';
```

沿用现有 token 类型化列，不新增数据库字段。先升级 Consumer 接受请求输入 estimated，再升级 Observer，最后更新 Wasm 并显式开启开关。schema_version 保持 1；旧 Consumer 会拒绝带估算的请求事件，不能先开启采集。关闭开关可恢复原有请求事件形态，响应契约和汇总口径保持一致。

验证使用 `make test`、`make wasm`、`make k8s-test K8S_TEST_ARGS='--context kind-kind'`。正式集成用例覆盖开启/关闭、1M 输入、合法空文本、无效正文、缺少正文、非法/缺失模型及响应缺 usage，逐请求核对 Kafka 到 Doris，检查请求估算未替代响应数值或进入有效用量。

输入 token 的正式接入证据见[正式事件接入验证](spec/plan/request-body-token-event-verification.md)；后续 request_model 验证见[请求模型验证](spec/plan/request-model-verification.md)。

原型专用证据 CLI、基准程序和压测脚本已移除，回归保留正式单元及集成用例。[原型阶段 RSS 报告](spec/plan/request-body-token-prototype-verification.md)仅记录当时本地 2 QPS 的测量：请求体开关的网关 RSS 稳态增量 16.70–18.06 MiB（16.19%–17.72%），插件相对未安装最大采样增量 67.27 MiB。它不代表本轮正式事件集成后的重新容量验收；单 Pod 200 QPS、60 秒流、额外 RSS ≤2048 MiB 仍未通过。
