# 请求头采集与用户汇总验证

日期：2026-11-03。当前 `schema_version=1` 直接增加 `user_id`、`user_agent` 和 `request_headers`，最终初始化 DDL 同步更新。

## 已实现行为

- `user_id_header` 为最多 8 项的有序“或”列表，缺省 `["user-id"]`，取第一个有效且满足预算的单值；空数组关闭提取，未命中为 NULL。
- User-Agent 使用标准请求头；扩展白名单写入 `MAP<STRING, STRING>`，小写 key、Envoy 合并值，无有效项为 `{}`。
- 请求头快照贯穿 Wasm、Observer、Kafka、Consumer 与 Doris；日/月汇总增加用户维度，未知用户仍贡献整体统计。
- 新增元数据受 4096 字节规范 JSON 和 6144 字节 Start 帧预算约束，超限跳过可选字段；不增加业务暂停。

## 验证结果

| 检查 | 结果 |
| --- | --- |
| `GOFLAGS='-count=1 -p=1' make test` | 通过：契约、96 项 Python 测试、主模块与独立插件 Go 竞态测试 |
| `XDG_CACHE_HOME=/tmp/request-headers-cache make wasm` | 通过：TinyGo 构建、ABI 与 gRPC hostcall 检查 |
| `make k8s-test` 隔离验收 | 通过：命令参数见下，20 项顶层测试全部 PASS，无 SKIP/FAIL |
| Doris 实际元数据核验 | 17 个对象的列、类型、可空性及中文注释通过核对；据此生成数据字典 |
| 汇总独立集成模块编译 | `go test -tags=integration -run '^$' ./internal/summaryworker` 通过；真实汇总执行由 Kubernetes 套件覆盖 |
| `git diff --check` | 通过 |

完整命令：`XDG_CACHE_HOME=/tmp/request-headers-cache GOFLAGS='-count=1 -p=1' make k8s-test K8S_TEST_ARGS='--context kind-kind --namespace metering-wasm-user-headers'`。

成功运行标识为 `e723b4904f20`。本地证据：[result.json](../../../.tmp/k8s-collection/kind-kind/metering-wasm-user-headers/e723b4904f20/result.json)、[integration.log](../../../.tmp/k8s-collection/kind-kind/metering-wasm-user-headers/e723b4904f20/integration.log)。结果包含 `status=passed`、`integration.passed_tests=20` 和 `existing_gateway_unchanged=true`。Wasm SHA-256 为 `9568567039b324f197fa8044660e4d5a37549d4ef98cc07d8dfa682366019356`。

新增真实链路用例验证候选头优先级、回退、重复头、不同配置、标准 User-Agent、扩展 key 查询和两类事件一致性；用户汇总用例验证两位用户及未知用户分组、同用户多次请求、重放幂等、迟到追加、冲突排除及月日累计一致。既有 HTTP/1、HTTP/2 改写故障放行和远程 Wasm 加载失败恢复也全部通过。

首轮 `a03bd51d2d56` 中，新增汇总测试因动态分区表禁止手动加分区而失败；已在测试中先关闭动态分区、结束后恢复，并等待消费凭据屏障后再刷新。首轮既有未知 Schema 用例还出现 Doris 查询超时；第二轮该用例及整套回归均通过。首轮诊断一并保留。

## 验证边界

仅验证本地 `kind-kind` 隔离环境，归属接口使用合成数据。未执行生产 G0–G5、容量压测或 VM/Envoy 进程崩溃验收，不将本次结果视为生产部署证明。测试命名空间使用 `make k8s-clean` 清理，本地证据保留。

## Content-Length 快照修复验收

同日修复 Review 发现的时序问题：正文改写准备会删除 `Content-Length`，现在先构造受预算约束的完整 Start 快照，再准备改写和打开观察流。原有业务改写与超时恢复路径保留；未接纳观察时仍可进行正文改写，已构造的临时 Start 缓冲在回调结束时清理。

新增 `TestRequestHeaderSnapshotPrecedesBodyRewrite`，修复前在正常 JSON 和非法 JSON 两个场景均复现丢失原始长度，修复后通过。测试同时检查用户 ID 候选、客户端请求 ID 候选及扩展白名单读取 `Content-Length` 原值，转发头仍移除旧长度；有效正文添加 include_usage，非法正文按原文透传。

真实链路用例 `TestRequestHeadersThroughGatewayToDoris` 增加 `content-length` 白名单，检查开始及结果两条事件的 Doris MAP 列和规范 event_json 均等于客户端原始长度。

- `GOFLAGS='-count=1 -p=1' make test`：通过，含 96 项 Python 测试及主模块、独立插件 Go 竞态测试。
- Wasm 构建及 ABI 检查：通过。当前 SHA-256：`6acc7b6a32281b4bd060cd9b85aad1cb531ff901c0e9023d61931e24e5a8152e`。
- 完整 Kubernetes 验收：`XDG_CACHE_HOME=/tmp/request-headers-cache GOFLAGS='-count=1 -p=1' make k8s-test K8S_TEST_ARGS='--context kind-kind --namespace metering-wasm-header-snapshot'` 通过。运行 `4b6b600bf0ee` 的 20 项顶层测试全部 PASS，无 SKIP/FAIL，原有网关保持不变。
- 证据：[result.json](../../../.tmp/k8s-collection/kind-kind/metering-wasm-header-snapshot/4b6b600bf0ee/result.json)、[integration.log](../../../.tmp/k8s-collection/kind-kind/metering-wasm-header-snapshot/4b6b600bf0ee/integration.log)。首轮 `009ead230eef` 在测试开始前因 Doris BE 尚未报告磁盘而建表失败；确认容量充足后重跑成功，未修改测试基础设施代码。

本轮测试命名空间单独清理，保留本地证据；生产验证边界与上述说明相同。
