# 请求模型保存与 Wasm 不变验证

日期：2026-11-13。

已在现有请求体采集基础上保存 request_started.request_model。Observer 复用一次完整 JSON 校验和顶层字段扫描，提取 model；不再扫描整份正文，也不增加网关正文读取、复制或旁路传输。仅保存非空白、无控制字符、合法 UTF-8、最多 1024 字节的模型字符串；在解码前按最大 JSON 转义长度限制字段，避免超长模型字段产生大副本。

完整 JSON 中的模型标识可独立保存：即使 messages 无法计数，request_model 仍可已知，输入 token 保持 null/unknown。关闭请求体采集、正文不完整、非法 JSON、超限、队列或执行额度不足时不猜测模型；缺失或非法 model 仅将该字段置 null，不改变输入计数、响应资格或服务端 usage。

request_model 仅写请求事件；响应事件此字段保持 null，response_model 仍来自上游。模型标识不会切换固定 o200k_base 分词器，也不用于推断实际执行的后端模型。继续使用已有 Doris request_model 列，不增加列、事件字段或内部传输字段。Consumer 契约放开了开始事件 request_model 必须为 null 的限制，仍保留类型和 1024 字节边界；更新时先 Consumer、再 Observer。已有请求体采集功能的 Wasm 可直接复用，只需确保 request_body_capture_enabled 已开启。

## 验证

- make test 通过：118 项 Python 测试、8 个事件样例及规范编码/摘要一致性、主模块与独立插件 Go 竞态测试。
- 原生边界覆盖：模型缺失、非字符串、空白、控制字符、1024 字节及超限、UTF-8 和 JSON 转义边界、重复键/非法 JSON；模型合法而 messages 不可计数仍保存模型。Observer 异步汇合、一次发送、正文失败、资源不足和响应缺 usage 回归通过。
- make build k8s-test-build、相关 Go 包 vet 通过。复用已有 Wasm 制品进行真实 Kubernetes 验证，没有重建或替换 Wasm。
- kind-kind 完整部署/故障回归通过，Go 集成 28/28 PASS、0 FAIL、0 SKIP。请求采集开启/关闭各 8 项，共 16 项真实 Envoy → Observer → Kafka → Consumer → Doris 场景通过，涵盖 1M 输入以及非法/缺失 model。
- 集成用例逐请求检查原正文 SHA256、类型化 request_model 与 event_json 一致，以及响应事件没有混入请求模型。直接读取 Doris 得到五条 request_started/requested-model，其中输入分别为 NULL、0、1、3、1000000；NULL 对应合法模型但 messages 不支持计数。输入来源和响应数值保持原规则。

本轮开始前记录了 Wasm、SDK、配置/协议相关源码及制品的 109 项 SHA256，完成后逐项相等。Wasm 制品仍为 `4dcced4cbf2abcd150eb57ce814ef5e3f281ac622d4665691451961cbd339e37`。未修改插件代码、SDK、Wasm 配置字段或 gRPC 传输协议。

仅清理本轮拥有的隔离测试资源，诊断文件保留。本轮没有生产部署或新的 RSS 容量测量。

## 本地证据

- [完整集成日志](../../../.tmp/k8s-collection/kind-kind/metering-wasm-verify/1dfb7d2b8a72/integration.log)、[部署与故障验收](../../../.tmp/request-token-prototype/request-model-k8s.log)、[清理记录](../../../.tmp/request-token-prototype/request-model-clean.log)。
- [最终 make test](../../../.tmp/request-token-prototype/request-model-test-final.log)、[构建](../../../.tmp/request-token-prototype/request-model-build.log)、[实际 Doris 模型列](../../../.tmp/request-token-prototype/request-model-doris.tsv)。
- [Wasm 和传输文件变更前摘要](../../../.tmp/request-token-prototype/request-model-wasm-before.json)。

这些日志位于本地忽略目录 `.tmp/`。当前配置与 SQL 查询见[请求输入与模型采集](../../request-body-tokens.md)。
