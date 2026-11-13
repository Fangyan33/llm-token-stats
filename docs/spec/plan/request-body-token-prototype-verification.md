# 请求体 token 原型实现与网关 RSS 验证

本报告归档 2026-11-12 的原型实验。2026-11-13 已接入正式请求事件，并移除原型专用代码和 CLI；下文实现描述、命令和测量只代表当时版本，当前配置与操作见[正式输入计数说明](../../request-body-tokens.md)。

日期：2026-11-12。实现基于 `ebdde344153d` 工作区；本轮未提交、打包或部署生产。

## 结论与交付范围

已实现默认关闭的请求体旁路开关、Observer 有界分词池、独立参考计数校验，以及真实 Envoy 的三组 RSS 对照工具。**只比较网关 Envoy 进程 RSS，不统计 Observer 内存增长。**

本地 2 QPS、60 秒流、三轮完整对照中，开启请求体采集比关闭该功能的稳态 RSS 增加 **16.70–18.06 MiB，增长 16.19%–17.72%**。插件开启相对完全不安装插件的最大时间对齐采样增量为 **67.27 MiB**。6480 个业务请求全部完成，开启组 2160 个输入计数全部正确；B/C 六组 8640 条正式事件逐请求入库核对通过。

**200 QPS＋60 秒流＋插件额外 RSS ≤2048 MiB 尚未通过容量验收。** 本地高档负载存在输入采集不完整、计数过载与实际吞吐不足，不能以这些负载下较低 RSS 声称通过。2 QPS 的增长比例也不能乘到生产约 12 GiB 的已有 RSS 上推算。

请求估算本轮仅保存为原型证据；Kafka 事件、Consumer、Doris、汇总未接入此估算。响应事件仍只使用服务端 usage，缺失保持 null/unknown。request_model 仍为 null。原型结果不能代替模型 tokenizer 和聊天模板的准确 prompt_tokens。

## 实现

- Wasm 增加 `request_body_capture_enabled`，默认 false；关闭时不读取请求正文。开启时领取额度后按最多 16 KiB 分片旁路发送，回调直接 Continue，不解析 JSON、不完整缓冲、不改写业务正文。
- 请求体使用独立序号、偏移及结束标志；Pod 请求方向最多占现有传输字节及条数额度的四分之一，同时占用总额度。资源不足停止输入采集，响应观察继续；真实共享传输故障可能影响该通道上的响应观察，业务始终按现有 fail-open 路径处理。
- Observer 默认单条请求正文上限 32 MiB，配置最大 128 MiB；与 token 数没有固定换算关系。默认 Worker 数为 `max(1, GOMAXPROCS−1)`，等待队列最多 200 项、512 MiB，先达到任一上限即拒绝该次计数。这是每个 Observer 的分词队列，和网关活跃业务数无直接等价关系。
- 请求正文分片及执行临时额度计入 parser_budget_bytes，请求方向最多占一半；执行前预留 `32 × body_bytes + 65536` 临时额度。运行任务持有槽位和额度直到返回，不因超时创建替补 goroutine。逻辑额度不是 RSS 上限。
- 固定 `o200k_base / text_tools_v1`：字符串 content、文本块，以及规范 JSON 编码的 tool_calls/function_call/tools/functions 分别计数求和；不逐传输片段分词，不添加聊天模板补偿，不计算媒体本身。完整合法正文才给出数值，合法空字符串可为 0；缺失、非法、不完整、超限、过载为 null。
- 显式 `--request-token-evidence` 启用原型证据输出，文件必须不存在，权限 0600，写入队列有界。只记录关联标识、计数、编码、大小、耗时和固定原因，不写正文或凭据。

新增 Observer 原生依赖 `github.com/tiktoken-go/tokenizer v0.7.0` 及间接依赖 regexp2 v1.11.5。Go 1.25.0、TinyGo 0.40.1、proxy-wasm-go-sdk v0.24.0 不升级，tokenizer 不编入 Wasm。本轮 SDK 仅修正原生测试宿主对 HTTP 缓冲复制语义的模拟，生产 SDK 未改；记录见 PATCHES.md。

网关与 Observer 协议增加请求方向状态和停止通知。升级时先部署新 Observer，再更新 Wasm 和开启请求体开关；旧 Observer 不能接收新请求方向协议。配置和计数细则见[使用说明](../../request-body-tokens.md)。

## 对照方法

本地机器 8 CPU、约 11.77 GiB 内存，Envoy 与 Observer、负载端及 fixture 共用主机资源。使用实际 `istio/proxyv2:1.14.6` 的 Envoy V8 Wasm，8 个 Envoy Worker、64 条客户端 HTTP/2 连接。Observer GOMAXPROCS=8，分词 Worker 自动为 7，parser_budget_bytes=10 GiB。Kafka 在压测期间运行，Consumer/Doris 在业务阶段结束后才启动并做精确核对，避免其 CPU 干扰 RSS 比较。

生产的“8 个副本”不等同于本地 8 个 Envoy Worker。此处是本地真实 Envoy 进程的对照，不能称为生产同规格 istio-ingressgateway 容量验证。

每轮三组：A 完全不加载插件；B 加载插件并关闭请求体采集；C 加载插件并开启请求体采集、完成分词。每组重新启动 Envoy 和 Observer，三轮顺序依次为 ABC、CAB、BCA。每组预热 60 秒、稳态 300 秒，停止发压后排空 60 秒流。2 QPS 下每组 720 请求，稳定约 120 条活跃流；9 组共 6480 请求。

输入严格沿用 20% 1M、50% 200K、20% 500K、10% 100K。每组分别为 144、360、144、72 请求。合成 content 为重复 ` word`，由独立官方 Python tiktoken 向量确认四档 token 数；实际正文含协议字段约为 5,000,125、1,000,125、2,500,125、500,125 字节，每组总上传 1,476,090,000 字节。它覆盖大正文传输，但不代表各种自然语言、工具结构或病态超长词的分词性能。

以 `/proc/<envoy-pid>/status` 的 VmRSS 为准。C−B 度量新增请求体功能，C−A 度量插件总增量；预算 **2048 MiB 作用于 C−A**。稳态比例为 `(C 稳态中位数−B 稳态中位数)/B 稳态中位数`。保留峰值差、按负载相对时间对齐的最大采样差、稳态中位数差和排空后残留；采样最大值不能保证采样间没有更高峰值。

Observer 的计数完整率、队列和 CPU 仅用于检查负载是否有效，不计入 RSS 比较。必须同时满足实际吞吐与全部输入计数、正常响应 usage 精确入库，才能将相应 RSS 用于完整采集对照。

## 三轮网关 RSS 结果

单位 MiB；“请求体增长”相对 B，“插件最大增量”相对 A，后者取时间对齐最大采样差。

| 轮次 | A 稳态 | B 稳态 | C 稳态 | 请求体稳态增量 C−B | 请求体稳态增长 | 请求体最大增量 C−B | 插件最大增量 C−A |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 55.84 | 103.09 | 121.14 | 18.06 | 17.52% | 19.86 | 67.11 |
| 2 | 55.66 | 101.60 | 119.61 | 18.01 | 17.72% | 19.53 | 65.47 |
| 3 | 55.34 | 103.10 | 119.80 | 16.70 | 16.19% | 19.51 | 67.27 |

C−B 三轮稳态增量中位数为 18.01 MiB。B 的 RSS 包含原有响应采集和 Wasm 固定开销；C−A 不能误写成“请求体开关新增内存”。每组重启控制了对照条件，也意味着本实验没有验证同一 VM 连续多轮负载后的长期堆保留。

9 组实际稳态均为 2 QPS，全部业务完成、0 错误，客户端逐请求校验原正文 SHA256 不变。C 共 2160 条证据均已知且计数正确，0 缺失、0 重复、0 null，负载结束后等待队列为 0。B 共 2160 条证据均为 disabled/null。B/C 的 4320 请求对应 8640 条 Doris 事件，6 组均无缺失、重复或不合法事件，Kafka lag 归零；响应仍为服务端合成 usage，输入估算未写正式事件。

最终报告标志为 `diagnostic_valid=true`、`capacity_accepted=false`、`rss_evaluation_valid=false`、`rss_within_budget=null`。低负载各组采样差虽小于 2 GiB，目标容量预算结论仍留空。

## 未通过的负载与吞吐边界

自动筛查使用预热 1 秒、发压 10 秒、1 秒 SSE。1/2/5 QPS 的短流满足初筛；10 QPS 在负载后仍有 43 项等待，不能因为随后能排空就认为稳定。更高档位输入采集也开始丢失。

| C 组目标 QPS | 实际稳态 QPS | 正确输入计数/请求数 | 完整率 | 结果 |
| --- | ---: | ---: | ---: | --- |
| 10（短流） | 10.00 | 110/110 | 100.00% | 发压结束仍有 43 项排队，不稳定 |
| 25（短流） | 24.89 | 239/275 | 86.91% | 采集不完整/证据缺失 |
| 50（短流） | 46.56 | 188/550 | 34.18% | 吞吐及采集不完整 |
| 100（短流） | 64.56 | 178/1100 | 16.18% | 吞吐及采集不完整 |
| 200（短流） | 68.33 | 210/2200 | 9.55% | 吞吐及采集不完整 |
| 5（60 秒流） | 5.00 | 1319/1800 | 73.28% | 481 次 queue_full，计数过载 |

5 QPS 长流首轮 A/B/C 业务请求都完成，但 C 的输入计数未达到完整要求；已停止剩余两轮，保留原始结果，未进行这一候选负载的 Doris 审计。重新选择 2 QPS 后，完成上述全部三轮及入库核对。短流选择只提供候选，不能证明长流容量。

原生 Go 分词基准中，四档各重复 3 次，按目标分布加权平均约 0.613 CPU 秒/请求。简单计算 200 QPS 对应约 122.6 CPU 核，70% 利用率约 175.1 核；这是本地特定合成文本、当前 Go tokenizer 的微基准估算，不是部署规格承诺。现有 8 CPU 主机还承担 Envoy/fixture 等，不能靠扩大等待队列达到完整输入计数的 200 QPS。队列满时按设计将本次估算置 null，业务继续转发。本报告不评估 Observer RSS。

## 功能、故障及构建验证

| 层次 | 验证 | 结果与边界 |
| --- | --- | --- |
| 静态/契约/原生 | make test | 契约一致；120 项 Python 测试及主模块、独立插件 Go 竞态测试通过 |
| SDK 测试宿主 | proxywasm Go 竞态测试 | 通过；HTTP 正文复制语义与真实宿主一致 |
| Wasm 制品 | make wasm / request-token-build | 构建与 ABI、缓冲所有权检查通过 |
| 独立参考 | 官方 Python tiktoken 0.12.0 → Go | 12/12 通过，覆盖四档大文本、Unicode、分块边界、工具 JSON 及数字字面量 |
| 隔离 Kubernetes | make k8s-test，context=kind-kind | 27/27 PASS，0 FAIL，0 SKIP；默认请求体关闭路径，覆盖原有加载/故障回归 |
| 真实 Envoy 功能 | A/B/C 各 100 请求、并发 2，16K/150ms SSE | 全部业务成功，C 100 次正确计数，B/C 正式事件逐请求入库通过；不作容量结论 |
| 真实 Envoy 边界 | 默认 32 MiB/最大 128 MiB、各 +1 字节、非法/缺少正文、合法空文本 | 精确边界通过，超限 null、业务完整透传；所有正常请求正式事件入库通过 |
| 真实 Envoy 故障 | 上传中停止 Observer、随后恢复 | 中断请求业务继续、正文 SHA256 不变，恢复后的输入计数及正式事件正常 |
| 本地 RSS | 上述 9 组，2 QPS/60 秒 SSE | 完整诊断对照通过；200 QPS 容量未通过 |

32/128 MiB 精确边界正文使用空 messages 文本加非计数字段 padding，验证传输和大小保护；不能据此声称完成 128 MiB 有效文本的分词性能验证。开启请求体的真实 Envoy 用例不等同于完整 Kubernetes 故障矩阵覆盖；VM/Envoy 进程故障、生产环境和长期复用仍是独立验证边界。

本轮自身 Docker 资源和隔离 Kubernetes 测试命名空间已清理，原始诊断文件保留；已有 kind 控制面和 buildkit 容器保留。

## 复现与证据

```sh
make request-token-build
make test
make k8s-test K8S_TEST_ARGS='--context kind-kind'
make k8s-clean K8S_TEST_ARGS='--context kind-kind'

# 官方参考向量，虚拟环境的 requirements 包含固定 tiktoken 版本
python3 -m venv .tmp/request-tokenizer
.tmp/request-tokenizer/bin/pip install -r tests/capacity/requirements.txt
.tmp/request-tokenizer/bin/python scripts/verify_request_tokens.py --output .tmp/token-vectors.jsonl
dist/request-token-bench --vectors .tmp/token-vectors.jsonl
dist/request-token-bench --iterations 3

python3 scripts/request_token_boundaries.py
python3 scripts/request_token_capacity.py --mode matrix --qps 2
# 目标容量验证须显式保留 200 QPS 和其余门槛；本地目前未通过
python3 scripts/request_token_capacity.py --mode matrix --qps 200
```

以下原始证据在本地 `.tmp/` 忽略目录，未纳入 Git；报告中的结论和数字保存在本文。

- [最终三轮结果](../../../.tmp/request-token-prototype/capacity-valid/384480c7823f/result.json)、[环境/制品摘要](../../../.tmp/request-token-prototype/capacity-valid/384480c7823f/environment.json)、[过程日志](../../../.tmp/request-token-prototype/capacity-valid.log)。各组目录含 requests.jsonl、rss.jsonl、verification.json，根目录保存分词证据。
- [高档探测结果](../../../.tmp/request-token-prototype/capacity-final/3ce1a1ce4283/probes.json)、[5 QPS 长流计数失败](../../../.tmp/request-token-prototype/capacity-final/3ce1a1ce4283/round-1-C/result.json)、[停止原因](../../../.tmp/request-token-prototype/capacity-final/3ce1a1ce4283/interruption.json)。
- [真实 Envoy 边界/中断恢复](../../../.tmp/request-token-prototype/boundaries/e0727c433476/result.json)、[100 请求功能对照](../../../.tmp/request-token-prototype/smoke/d70dc5dcf47a/result.json)。
- [最终 make test](../../../.tmp/request-token-prototype/make-test-final.log)、[12 项官方参考校验](../../../.tmp/request-token-prototype/reference-check-final.jsonl)、[Go 分词成本](../../../.tmp/request-token-prototype/go-benchmark.jsonl)。
- [Kubernetes 27 项汇总](../../../.tmp/request-token-prototype/k8s-summary.json)、[完整测试日志](../../../.tmp/k8s-collection/kind-kind/metering-wasm-verify/e42dce55642d/integration.log)、[清理记录](../../../.tmp/request-token-prototype/k8s-clean.log)。
