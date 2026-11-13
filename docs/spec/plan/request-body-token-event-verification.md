# 请求输入 token 接入正式事件

本文记录输入 token 首次接入时的验证。后续已在不修改 Wasm 的前提下增加请求事件 request_model，当前说明及验证见[请求模型验证](request-model-verification.md)。

日期：2026-11-13。承接 2026-11-12 的请求体计数原型；原型专用代码已移除，历史 RSS 数据仍按原实验范围保留。

## 最终行为

显式开启 Wasm 的 `request_body_capture_enabled` 后，完整合法正文的输入估算写入正式 `request_started.usage.input_tokens`，source 为 estimated，usage_status 为 partial，usage_eligible 为 false。合法空文本保留 0/estimated。正文缺失、非法、不完整、超过大小限制或计数队列/执行额度不足时保留 null/unknown；其余五项 token 始终为 null/unknown。

关闭开关仍不读取请求正文，开始事件保持 usage=null、usage_status=not_applicable。request_model 保持 null。响应事件仅允许 reported/unknown，上游缺 usage 时不采用请求估算补齐；requests、usage_effective 和日/月汇总继续只读取响应结果。

请求事件用一个小型状态对象汇合冻结归属和计数结果，两个结果到齐后只发送一次。分词任务仅持有该状态，不持有观察和响应解析器；响应解析、ACK 和结果事件不等待分词。较慢计数允许 :usage 先于 :start 入库，原始开始时间保持不变，不通过补写同一事件 ID 更新 null，从而避免制造内容变体冲突。

事件 Schema、Go/Python 校验同步约束来源：estimated 只允许开始事件的输入字段，响应或其他 token 出现 estimated 会被拒绝。Consumer 使用已有类型化 token 列及 event_json 原样保存，不新增数据库列；DDL 注释和数据字典已同步。当前 schema_version 保持 1，旧 Consumer 不接受新的开始事件，升级顺序为 Consumer → Observer → Wasm 开关。

计数仍为 o200k_base / text_tools_v1 通用文本与工具估算，不等于模型 tokenizer 与聊天模板的准确 prompt_tokens。详细配置和 SQL 见[请求输入 token 估算](../../request-body-tokens.md)。

## 验证结果

| 层次 | 验证结果 |
| --- | --- |
| 契约及原生 | make test：8 个合成事件样例一致，117 项 Python 测试通过；主模块及独立 Wasm 模块 Go 竞态测试通过 |
| 请求事件竞态/资源边界 | 覆盖归属先到/计数先到、响应先完成、并发汇合仅发送一次、重复结果忽略、队列字节不足、执行额度不足、不完整正文、合法零值，以及响应缺 usage |
| 构建 | make build wasm k8s-test-build 通过，Wasm ABI 与 SDK 缓冲所有权检查通过；相关 Go 包 vet 通过 |
| 隔离 Kubernetes | kind-kind 完整部署及故障回归通过，Go 集成 28/28 PASS，0 FAIL，0 SKIP |
| 新增正式网关链路 | TestRequestInputTokensThroughGatewayToDoris：开启/关闭各 6 项，共 12 项全部通过；包含文本、空文本、1M 输入、无效 messages、无正文、响应无 usage |
| Doris 元数据 | 读取隔离库的实际元数据，校验 17 个表/视图与源码列、类型、注释一致后重新生成数据字典 |

新增真实网关测试逐请求检查业务状态与原始请求正文 SHA256、两条正式事件恰好入库一次、类型化列和事件 JSON 一致。开启时正常文本的输入估算为 3，响应仍为服务端 10/4；1M 输入开始事件为 1000000/estimated，响应仍为 10/4。上游省略 usage 时请求估算可为 1，响应输入/输出保持 null，且不会进入 usage_effective。无效正文不影响正常上报的响应用量，requests.input_tokens 不读取开始事件的估算。

在隔离 Doris 中实际读取到的四条请求估算为 0、1、3、1000000，均为 request_started/partial/estimated，输出均为 NULL。该读取记录和正式集成测试共同验证数据已从 Observer 经 Kafka、Consumer 入库，不以源码或 Job 状态替代运行证据。

## 清理与验证范围

已移除原型证据 JSONL CLI、Evidence 写入器、request-token-bench、独立原型压测/边界/参考生成脚本、对应 Make 目标及仅服务原型脚本的测试。保留生产分词实现、正式单元/契约/集成回归，以及[历史原型报告](request-body-token-prototype-verification.md)。

本轮正式事件接入没有更改原型阶段已验证的 Wasm 制品，SHA256 仍为 `4dcced4cbf2abcd150eb57ce814ef5e3f281ac622d4665691451961cbd339e37`。没有重新运行 200 QPS RSS 容量实验，也未部署生产；此前本地 2 QPS 的 RSS 数值不能充当本轮正式事件或生产容量验收。既定单 Pod 200 QPS、60 秒流、额外 RSS ≤2048 MiB 仍待通过。

完整 Kubernetes 测试后仅清理本轮拥有的隔离命名空间，原始诊断保留。未进行 Git commit、push 或生产部署。

## 本地证据

- [完整 Kubernetes 日志](../../../.tmp/k8s-collection/kind-kind/metering-wasm-verify/9ea661086a13/integration.log)、[部署与故障验收过程](../../../.tmp/request-token-prototype/formal-k8s-test.log)、[清理记录](../../../.tmp/request-token-prototype/formal-k8s-clean.log)。
- [最终 make test](../../../.tmp/request-token-prototype/formal-make-test-final.log)、[请求事件竞态与资源边界](../../../.tmp/request-token-prototype/formal-request-race.log)、[构建与 ABI](../../../.tmp/request-token-prototype/formal-build.log)。
- [实际请求估算列](../../../.tmp/request-token-prototype/formal-estimates.tsv)、[实际 Doris 元数据](../../../.tmp/request-token-prototype/formal-schema-live.tsv)。

原始日志位于本地忽略目录 `.tmp/`，不纳入 Git。生产使用说明和事件契约以源码及本轮文档为准，历史原型命令不再提供执行入口。
