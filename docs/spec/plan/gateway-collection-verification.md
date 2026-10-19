# 采集组件本地验证记录

日期：2026-10-13。实现见[计划](gateway-collection-implementation.md)，使用方法与未验收门槛见[运行说明](../../collection-components.md)。本记录是公开固定 Envoy 的本地合成验证，不代表私有集群或生产容量验收。

本文按日期保留历史证据，旧章节只代表当时的契约，不是当前兼容要求。当前[归属接口实现](host-service-provider-implementation.md)的 Bearer 输入、供应商/部署字段、占位状态及删除应用字段的验证见最后的 2026-10-16 记录；真实第三方 HTTP、真实 Auth、汇总 Worker 和生产 G0–G5 仍未完成。

## 工具与产物

- Go 1.25.0，TinyGo 0.40.1（LLVM 20.1.1），SDK 基底 v0.24.0；扩展摘要与二进制 SHA-256 由 `make manifest` 保存到 `dist/collection-manifest.json`。
- protoc 29.3 / protoc-gen-go v1.36.5 / protoc-gen-go-grpc v1.5.1；生成的标准代码已入仓库。
- 真实 Envoy 版本输出：`725ec273d665639aa9f6e0e9014af562a457c2f4/1.21.3-dev/Clean/RELEASE/BoringSSL`。
- Kafka 3.9.2、Doris 4.1.3；实际镜像摘要见[镜像锁定记录](../../../deploy/local/images.lock.json)。本地 Envoy 使用静态配置，未接入 Istio 控制面。

## 执行结果

| 检查 | 结果 |
| --- | --- |
| `make test build` | 契约一致性、6 份 Python/Go 编码与摘要 golden、20 项 Python 测试、根模块/插件 Go 竞态测试及三个本地程序构建通过 |
| TinyGo wasi/scheduler=none 构建、ABI 检查 | 通过；真实旧版 Envoy 成功加载、调用 gRPC hostcall 与接收 ACK |
| SDK `go test ./proxywasm/internal ./proxywasm/proxytest` | 通过 |
| `go vet ./...`、`go mod verify`、`git diff --check` | 通过 |
| 最终 `go test -tags=integration -race -v ./tests/integration` | 通过，16.000 秒 |
| 普通 JSON、SSE、响应 trailers | 两事件入库；reported 输入 10、输出 4、总量 14，complete、usage_eligible=1 |
| usage 缺失 | unknown，输入/输出 NULL，不进入有效用量统计 |
| 同事件重放、不同变体 | 原始同摘要去重；不同摘要保留，永久冲突标记、请求投影隔离、日期通知与消费凭据可见 |
| 未知 schema | 隔离测试 topic/group 的分区暂停；已提交 next offset 保持 1，不越过未知记录 |
| BE 写成功后断开 HTTP 确认 | 原 label 事务及行键确认成功；同 label 再调用未再次发送 HTTP |
| TCP Kafka 连接黑洞 | 1024 次 Emit 保持非阻塞、有界拒收；超时释放/保护路径触发 |
| HTTP1 正文 256 KiB、256 KiB+1、1 MiB | 上游完整正文 SHA-256 与输入相同，无截断或重复；超限取消观察，业务继续 |
| HTTP1 chunked 中途停顿 2.2 秒 | Root 超时恢复业务原文；尾片观察边界不可信时取消旁路，partial/unknown，NULL、不可统计 |
| 延迟 200ms 的正常 SSE 响应 | Start ACK 的唯一 UUID 进入响应头，与入库事件关联一致；重复伪造头移除 |

## 发现与处理

原请求头 Pause 在 ext_authz 恢复路径中延迟正文回调到超时，正常 SSE 返回 422。用户确认请求头 Continue、正文有界 Pause/改写后修复。慢上传异步 Resume 后尾片可能不完整经过 Wasm，保守取消观察并保持 unknown；不推测完整正文。固定 franz-go 的 RecordRetries(0) 阻止首次发送，使用 1 表示仅首次尝试；发送不确定时不补发。

最终测试停止发压后，Wasm 的 active_streams、inflight_bytes、inflight_messages、active_rewrites、rewrite_reserved_bytes 均为 0；Observer active_streams/parser_reserved_bytes、Producer pending_events 均为 0；Consumer 完成初始水位审计后 `/readyz` 返回 200。该证据证明本次合成测试状态释放，不代表长期 GC/RSS 或热更新存活代数已达标。

## 剩余验收

私有镜像、真实 Auth、生产 Istio/K8s 发布与网格验证、HTTP2 慢上传、断开与热更新交叉、真实进程崩溃/rebalance/保留期丢失恢复、峰值两倍与 24 小时压测、10 分钟 SSE、延迟目标、灰度回退仍待 G0–G5 执行。汇总 Worker/平台 API 属于后续子项。


## 2026-10-15 Observer Wasm 下载验证

本次将 Wasm 随 Observer 镜像发布，网关使用原生 `code.remote`、专用 HTTP 集群和实际文件 SHA-256 下载。本地网关只挂载生成的 Envoy 配置，不挂载 `dist/` 或 Wasm 文件；先前日期的结果保持原义。

| 检查 | 结果 |
| --- | --- |
| `make test build wasm` | 28 项 Python 测试、主模块及插件 Go 竞态测试、三个服务/fixture 构建、TinyGo 构建与 ABI 检查通过 |
| 下载服务单元测试 | GET/HEAD、内容/长度/ETag、固定路径隔离、405、8 并发上限与 503、缺失/非法文件、端口冲突及正常关闭通过 |
| 配置与渲染 | 省略下载配置保持兼容；启用配置默认 9093，文件必填；渲染摘要随实际产物变化，保持 Pod UID 选择器与默认关闭 |
| `make local-up` | 保留原 Kafka/Doris 数据卷，渲染下载摘要；Observer 下载内容核对成功后重建网关 |
| `GOFLAGS='-count=1' make local-test` | 通过，280.754 秒；真实远程下载后的 JSON/SSE、unknown、Doris 入库与重放、正文边界和 Consumer 恢复场景通过 |
| 独立固定 Envoy 下载故障 | 不可达、404、5 秒超时、摘要错误四种冷启动均在下载结束后放行业务，合成请求正文 SHA-256 不变，未产生采集事件；可达故障服务均只有一次下载尝试 |
| 故障恢复 | 修正下载地址/摘要并重启各自临时 Envoy，随后请求生成 known 用量并实际写入 Doris；结束后删除临时容器，未重启共享网关 |
| Observer 镜像运行 | 目录权限 0755、文件权限 0444；非 root 用户 65532，仅挂载服务配置，不挂载 Wasm；Kafka 指向不可达端口时仍下载到镜像内的正确文件，SIGTERM 后退出码为 0 |
| 静态及文档检查 | 新增 Go 包与入口 `go vet`、README/运行说明 shell 示例 `bash -n`、契约文档一致性及 `git diff --check` 通过 |

镜像构建初次因容器直连 Go 模块代理失败；使用现有主机代理与主机网络完成构建，未修改依赖源或提交代理凭据：

```sh
docker build --network=host \
  --build-arg HTTP_PROXY --build-arg HTTPS_PROXY --build-arg NO_PROXY \
  -f deploy/observer/Dockerfile -t metering-observer:wasm-download-test .
```

Wasm SHA-256 为 `a15b181ff3e59385e64aa2ba0e3d3df72be0d84e21a4b12137d37dcc24f7316b`；工具版本与构建文件摘要由本次 `make manifest` 更新至 `dist/collection-manifest.json`。单版本服务不承诺旧网关重启仍取得旧文件，也不承诺下载失败后自动恢复采集。本节是当时公开 Envoy 的静态配置验证；本地真实 Istio 控制面与网格 mTLS 的后续结果见下一节，私有镜像、热更新、生产容量及完整 G0–G5 仍未完成。


## 2026-10-15 本地 Kubernetes / Istio 首次手工验证（历史记录）

环境为 `kind-kind`，单节点 Kubernetes **1.22.17**，Istio 控制面和测试网关均为 **1.13.5**。独立命名空间为 `metering-wasm-verify`，不更新原 `istio-system` 网关 Deployment 或业务 EnvoyFilter。Observer、测试网关、合成鉴权/模型 fixture 在 Kubernetes 中运行；Kafka、Consumer、Doris 复用既有本地 Compose 实例，因此本次不宣称这些基础设施已迁入 Kubernetes，也不代表真实 Auth 或生产容量验收。

### 发现并修复的部署问题

1. 原流程启动后给 Pod 添加 `metering-instance=UID` 标签，但 Istio 1.13 的代理启动标签快照没有该字段，过滤器未命中。移除临时标签后，以 `match.proxy.metadata.NAME/NAMESPACE` 精确匹配实际 Pod，成功下发并加载插件；错误 Pod 名称不插入该过滤器，业务仍返回 200，恢复正确匹配后插件重新出现。渲染器新增必填 `--gateway-pod-name` 与可选 `--gateway-namespace`（默认 `istio-system`）；UID 仍使用 Kubernetes 实际值。
2. 默认 gRPC authority 取集群名，Istio `outbound|...` 名称含竖线，合成鉴权出现 403、Observer 流断开。合成 ext_authz 显式指定有效服务 DNS；插件在保持原集群名的同时，将 Istio 集群的 authority 设置为 `服务DNS:端口`。原生本地集群名保持原值，TLS/SNI 不变。字段行为依据[固定 Envoy gRPC 定义](https://github.com/envoyproxy/envoy/blob/e2e40d8d667e1b7c752d27980307f22d9f73e9ee/api/envoy/config/core/v3/grpc_service.proto)。

下发配置后先确认实际代理已经收敛到目标 Wasm 摘要和 enabled 状态，再发送验收请求；`kubectl apply` 返回成功本身不能代替 XDS 收敛检查。

### 实际结果

| 项目 | 结果 |
| --- | --- |
| 服务部署 | Observer 2/2、fixture 1/1、测试网关 1/1 可用；四个 Pod 全部 Ready，容器重启次数均为 0 |
| 网关镜像 | 原始 `docker.io/istio/proxyv2:1.13.5`，未加入 Wasm 文件或文件挂载 |
| 下载与多副本 | 两个 Observer 镜像内 Wasm 摘要一致；从带 sidecar 的 fixture 下载所得摘要相同，网关原生远程下载成功，无 fetch failure |
| 网格传输 | Observer 入站 PeerAuthentication 为 STRICT；9093 HTTP 下载和 9090 gRPC 都可用，gRPC 集群使用 HTTP/2，连接由 Istio mTLS 传输 |
| 实际过滤器 | config_dump 中顺序为 ext_authz → gateway-observer Wasm → router，code.remote 的 URL、HTTP 集群和 SHA-256 与发布产物一致 |
| 请求与入库 | JSON、SSE、SSE 缺 usage 均 HTTP 200，每请求两条事件实际进入 Doris；complete，正常输入 10/输出 4，unknown 输入/输出为 NULL、usage_eligible=0 |
| 实例身份 | gateway_instance_id 为测试网关实际 Pod UID；三个用例的 producer_id 分别落在两个 Kubernetes Observer Pod UID 上，未使用 Compose Observer |
| 匹配隔离 | 去除后加标签仍正常匹配；错误 NAME 不安装过滤器，恢复正确 NAME 后重新加载 |
| 原网关 | 原 Pod `istio-ingressgateway-656ccc4f69-ggq9j` UID 始终为 `59100d69-0dec-4629-b44d-31a8f9579344`，重启次数前后均为 13 |
| 本地回归 | `make test build wasm` 通过：29 项 Python 测试、主模块/插件竞态测试、新增 gRPC authority 协议测试、构建及 ABI 检查；`GOFLAGS='-count=1' make local-test` 再次通过，273.412 秒，含四类下载故障与恢复 |

| 用例 | logical_request_id | 用量状态 |
| --- | --- | --- |
| json | `194de6ad-d43d-4616-b899-2fc9940312c4` | known |
| sse | `3f4f9bae-f63b-44c0-9873-9785cd304851` | known |
| unknown | `3d51d592-d5f8-42b5-a097-e8ce2931b42d` | unknown |

本次 Wasm SHA-256 为 `c67679d6d16ca029a70fd42d6565b67530295131a2ea9925e4e7dd9fe46e34e2`。Observer 本地验证镜像为 `metering-observer:wasm-k8s-verify`，镜像 ID 为 `sha256:503fa9db35238385240266fb4581e37c1c38136b78a45b6279c008d80a43fbea`（本地镜像 ID，不是已推送的仓库 digest）。服务镜像通过 `kind load docker-image` 导入节点，没有推送到外部仓库。

首次手工验证的清单、config_dump、匹配检查和请求结果保存在已忽略的 `.tmp/k8s-wasm-verify/`。该批临时资源由后续自动用例替换，新的执行及清理方式见[测试用例](../../kubernetes-collection-test.md)。`istioctl analyze` 没有 Warning/Error，仅健康端口命名的 IST0118 Info；这些探针与服务访问均正常。合成鉴权和模型不能代替真实 Auth/模型验证，生产网络与安全策略、性能/容量、崩溃恢复、热更新和完整 G0–G5 仍需另验。

## 2026-10-15 自动用例 K8S-WASM-001：独立 Kubernetes Consumer

将上述实际部署整理为[可重复测试用例](../../kubernetes-collection-test.md)，提供 `make k8s-test`、`make k8s-clean` 和独立清理脚本。Kubernetes 运行 Observer 两副本、Consumer、fixture 和原始 Istio 测试网关；只有 Kafka、Doris 复用本地 Compose。每次创建独立的 `llm-metering-k8s-<run-id>` topic / 消费组，3 分区、24 小时保留期，Compose Consumer 不参与消费。

服务入口原先拒绝所有非默认 topic，现改为校验合法 Kafka 名称；生产样例仍使用 `llm-metering-events`，Observer / Consumer 配置必须一致。没有事件协议或数据库迁移变更。kind 出口经过 SNAT，不能用 broker 所见 HOST 识别 Pod；通过实际停止 / 恢复 Kubernetes Consumer 核验消费组归属。空分区允许没有 committed offset，不将其误判为消费失败。

| 检查 | 实际结果 |
| --- | --- |
| `make local-infra-up` | 仅启动共享 Kafka / Doris 并应用既有本地迁移；无需启动 Compose Consumer 等应用服务 |
| 自动部署 | 5 个 Pod 全部 Ready、容器重启为 0；网关仍使用 `docker.io/istio/proxyv2:1.13.5`，无 Wasm 挂载 |
| 远程 Wasm / mTLS | 两个 Observer 镜像内文件与 fixture 通过 STRICT mTLS 下载的摘要一致；网关已激活配置收敛后再发请求 |
| JSON / SSE / unknown | 三个请求均 200，Doris 各有两条事件；正常用量 10 / 4，缺 usage 为 unknown / NULL，身份及 Kafka topic 均归属于本次 Kubernetes 测试 |
| Consumer 消费提交 | 对应独立消费组提交 6 条记录，三个分区无积压，Doris 分区起点和批次回执可见 |
| Consumer 归属与恢复 | Kubernetes Consumer 缩容至 0 时消费组为 Empty / 0 成员；恢复至 1 时为 Stable / 1 成员，原消费位点保持，Consumer Ready |
| 清理及幂等 | `make k8s-clean` 实际删除测试 namespace / topic / group；第二次执行成功。共享 Kafka / Doris 容器及数据卷、默认 topic / group、原网关保持，6 条 Doris 验证事件仍可查询 |
| `make test` | 38 项 Python 测试、契约校验、主模块及独立插件 Go 竞态测试通过；覆盖清理保护、重入清理、数据来源、unknown 与空分区边界 |
| `make build wasm` | Observer / Consumer / fixture、TinyGo Wasm 及 ABI 检查通过 |
| `GOFLAGS='-count=1' make local-test` | 全部通过，279.547 秒，包含四类原生 Wasm 下载故障与恢复 |

首次完整自动运行 ID 为 `4ba5854f8b82`；结果在 `.tmp/k8s-collection/kind-kind/metering-wasm-verify/4ba5854f8b82/result.json`。清理前后核对保存在 `.tmp/k8s-cleanup-check.json`，本次清理没有删除共享数据库内的测试历史。

清理后 `make k8s-test` 再次完整通过，运行 ID 为 `5e2726078a5b`，结果保存在对应目录。该次资源保留在 `metering-wasm-verify`；当前 Consumer Pod 为 `metering-consumer-f5c49bc7d-zc44s`，UID 为 `f6f69c36-143d-441f-b5b6-4be478fe35d0`。5 个 Pod 均 Ready、重启为 0；原网关 UID / 重启次数保持不变。

| 场景 | logical_request_id | 用量 |
| --- | --- | --- |
| JSON | `02a9e4e6-99d3-4689-b37f-3931744d5beb` | known，10 / 4 |
| SSE | `95ffb9ea-f143-4468-8cfc-99b77a309992` | known，10 / 4 |
| 缺 usage | `acc9bef3-084c-43d7-a1d6-d5d906dd0139` | unknown，NULL / NULL |

Wasm 摘要仍为 `c67679d6d16ca029a70fd42d6565b67530295131a2ea9925e4e7dd9fe46e34e2`。本用例使用合成鉴权 / 模型、明文外部 Kafka 和本地 Doris root 空密码；真实认证、生产网络和容量、完整 G0–G5 仍需单独验收。

## 2026-10-15 入口仅按 POST 与精确路径过滤

新增 `route_name: "*"`：全局及规则启用时，任意路由名或路由属性缺失的 `POST /v1/chat/completions` 均进入有界采集流程，其它方法与路径忽略。路径去掉查询串后精确比较，尾斜杠、子路径和不同路径不命中。事件沿用配置规则名 `*`；service_id / adapter_id 仍取自配置。Observer / Consumer 精确接纳 `*` 与服务组合，同时保留旧 `llm-chat` 登记，事件结构及数据库迁移不变。

| 检查 | 实际结果 |
| --- | --- |
| `make test build wasm` | 46 项 Python 测试、主模块 / 插件 Go 竞态测试、构建及 TinyGo ABI 检查通过；新增规则匹配、无路由属性时发送 Start、忽略请求无 gRPC 调用及正文不变测试 |
| 规则兼容 | 精确路由名仍按原方式匹配；全局 / 规则关闭仍忽略，前序禁用规则不会被后续通配规则绕过 |
| Compose 真实入口 | 移除实际 Envoy Route 的 name 和计量 metadata，JSON / SSE / unknown 仍各产生两条事件并进入 Doris，事件 route_name 为 `*` |
| `GOFLAGS='-count=1' make local-test` | 全部通过，278.517 秒；8 类方法 / 路径反例不增加 selected 计数、不产生事件，其余正文边界、Consumer 恢复及四类 Wasm 下载故障 / 恢复回归通过 |
| `make k8s-test` | 完整通过，5 个 Pod Ready、无重启；实际路由名为 `synthetic-model-route`，未注入计量路由 metadata，三个正例成功入库；JSON 正例附查询串 |
| Kubernetes 忽略验证 | GET / HEAD / OPTIONS / PUT 访问目标路径、POST 访问 embeddings / 尾斜杠 / 子路径、GET 健康检查共 8 个请求，selected 计数 `3 → 3`，Doris 事件为 0 |
| 独立 Consumer | Kubernetes Consumer 消费组提交 6 条事件；缩容至 0 时 Empty / 0 成员，恢复至 1 时 Stable / 1 成员且位点保持，未复用 Compose Consumer |
| 原网关 | `istio-system` 原网关 Pod 名称、UID 和重启次数保持不变 |

Kubernetes 运行 ID 为 `b67af2b4169c`；正例、反例、已激活配置、消费位点与镜像版本保存在 `.tmp/k8s-collection/kind-kind/metering-wasm-verify/b67af2b4169c/`。镜像版本按当前 Git commit 解析为 `84028685dbf8`；Wasm SHA-256 为 `e82dba1f86e859e6af8a68430d738be5468ec8e6cbde088ec51c5a63ad86404e`，`make manifest` 已更新本地产物清单。

| 场景 | logical_request_id | 用量 |
| --- | --- | --- |
| JSON（附查询串） | `9aca5517-f94b-440a-bc48-352f33ea1782` | known，10 / 4 |
| SSE | `7e7ba43d-957f-47fc-b5d6-b51347e0516b` | known，10 / 4 |
| 缺 usage | `139969e0-1c18-4868-bf9a-5bc1819bfa75` | unknown，NULL / NULL |

本次测试资源保持运行，清理仍使用 `make k8s-clean`。上述仅为本地合成验证，不代替生产 G0–G5 验收。

## 2026-10-15 route_name 作为辅助诊断字段

本节为当时的实现与验收结果；上一节记录通配入口的最初版本。入口继续仅按 `POST` 与去掉查询串后的精确路径 `/v1/chat/completions` 筛选，配置 `route_name: "*"` 仅控制选择。事件记录请求头阶段实际可读的 Envoy 路由名，缺失、无效或超长时用 `unknown`；计量 metadata 不冒充实际路由。路由快照在请求内不变，不参与客户归属、服务接纳、稳定身份比较或价格选择。

Observer / Consumer 改用 `accepted_service_ids`，兼容旧 `accepted_routes` 中的 service_id，新旧清单不能同时配置。新身份摘要排除 route_name；006 migration、Consumer 跨日期冲突查询和启动 / 周期审计统一从九个类型化身份字段比较新旧记录，不直接比较不同版本的旧摘要，不回填原始事件或自动清除持久冲突标记。同一 event_id 的不同内容仍保留冲突。已有部署先暂停旧 Consumer、执行 006，再升级 Consumer / Observer 和 Wasm；本地共享 Doris 已应用 006。

| 检查 | 实际结果 |
| --- | --- |
| 契约 / 单元 / 竞态 | `make test` 通过，47 项 Python 测试；新增诊断路由、服务准入及历史通配值接纳测试。后续契约校验也核对 Go / SQL 的身份字段顺序，Python 全量复验通过 |
| 构建与 ABI | Observer / Consumer / fixture 构建通过，`make wasm` 的 ABI 检查通过；Proto 使用原固定工具重新生成，wire 字段不变，`make manifest` 已刷新 |
| Compose 真实链路 | `GOFLAGS='-count=1' make local-test` 通过，耗时 293.156s；含三个采集场景、八个忽略反例、重放 / 冲突、Consumer 攒批 / 再均衡 / 退出恢复、黑洞、请求改写边界与四种下载失败恢复 |
| 新旧身份兼容 | 真实 Doris 中混合旧路由或历史 `*` 的旧摘要与新 unknown 路由事件，两个请求视图均无身份冲突；重放不增行。专项复验在混合记录写入后启动真实 Consumer，其启动审计不产生错误持久冲突；客户变化及同事件路由内容变化仍冲突 |
| Kubernetes 实际部署 | `make k8s-test` 通过，Kubernetes 1.22.17 / Istio 1.13.5，原始 Istio 网关镜像从 Observer 下载 Wasm；两个 Observer、独立 Consumer 和 fixture 在 Kubernetes，Kafka / Doris 复用 Compose |
| Kubernetes 数据 | JSON / SSE 均 known，输入 10、输出 4；缺 usage 保持 unknown、NULL / NULL。三个请求各两条事件；当前请求头阶段路由属性未读到，均记录 unknown。八个忽略请求 selected 计数不变、Doris 事件为 0 |
| Kubernetes Consumer 归属 | 独立 topic/group `llm-metering-k8s-03888bc89ca7`，提交事件合计 6；缩容后 Empty / 0 成员，恢复后 Stable / 1 成员。最终 5 个 Pod 全就绪、0 次重启；原 istio-system 网关未改变 |

本次测试镜像版本为 `84028685dbf8`；Wasm SHA-256 为 `c63cf60efb6bd07c29a4e45676acac1f913179fb872a9d49fa19a75e75434636`。Kubernetes 证据保存在 `.tmp/k8s-collection/kind-kind/metering-wasm-verify/03888bc89ca7/result.json`，资源保留供检查，清理使用 `make k8s-clean`。本节证明本地合成链路，生产 G0–G5 仍须独立验收。

## 2026-10-16 Host / Bearer 归属接口与唯一事件模型

本节为当前契约的实现及本地验收结果，对应[实现计划](host-service-provider-implementation.md)的 P0–P7。删除应用字段、静态服务上报和身份元数据读取，不保留旧消息/配置兼容分支。Wasm 只临时发送入口 authority 和指定 Authorization: Bearer token；Observer 固定集群/环境，调用两个类型明确的 resolver。当前合法输入均立即返回 lookup_not_implemented，不连接 PG、不构造第三方 HTTP 客户端，不做缓存、重试或假成功归属；URI 空/非空均不启用网络调用。

| 验证 | 实际结果 |
| --- | --- |
| `make test` | 契约检查、52 项 Python 测试、主模块和独立插件 Go 竞态测试通过；覆盖 Host 规范化、Bearer/重复头、非法输入跳过查询、独立失败、半成功拒绝、快照和凭据清理 |
| 构建 / ABI / 生成 | `make build wasm` 通过；固定 protoc 29.3、protoc-gen-go 1.36.5、protoc-gen-go-grpc 1.5.1 再生成内容不变；最终 `make manifest` 与全部产物摘要一致 |
| 完整 Compose 链路 | `GOFLAGS='-count=1' make local-test` 全部通过，333.724s；覆盖 JSON/SSE/unknown、八类忽略请求、重放/跨日期与归属冲突、黑洞、确认丢失、正文边界、Consumer 攒批/再均衡/恢复、四类下载故障及恢复 |
| 输入及隐私专项 | 实际旧版 Envoy 拒绝重复 Authorization 的采集身份，业务仍返回 200；六类输入各产生两条事件。独立 Kafka reader 核对全部 12 条记录的 key/value，Doris event_json 和 Observer/Consumer/网关/fixture 日志未包含本次随机合成哨兵 token；拒收诊断不保存敏感/已删除字段另有 Consumer 单元测试 |
| 成功归属与隔离 | 只在单元测试注入成功 resolver；服务/客户失败互不清除对方。Consumer 实际合成事件覆盖客户、供应商和环境快照冲突及启动巡检；空服务清单允许合法未知服务质量事件，已映射未登记服务仍拒收 |
| 汇总参考 SQL | 用新契约合成 Kafka 事件在 Doris 4.1.3 执行日/月准备与发布模板；验证集群/环境/供应商分组、未知服务空串键桶、未知客户排除、未知用量 NULL 和服务上报零。没有实现 Worker 或第三方 API，不将此项描述为真实客户归属或发布状态协议验收 |
| Kubernetes 实际部署 | K8S-WASM-001 在 Kubernetes 1.22.17 / Istio 1.13.5 通过；两 Observer、独立 Consumer、fixture 与原始 Istio 网关共 5 Pod Ready，重启为 0；STRICT mTLS 下远程下载与 gRPC 采集成功 |
| Kubernetes 数据与消费 | 三个请求各两条事件，共 6 条；cluster_name=kind、deployment_environment=test、request_host=wasm-verify.local，两类状态均 lookup_not_implemented。四项 ID 和两个映射版本均 NULL，identity_status=invalid、usage_eligible=0，正常用量仍为 10/4；缺 usage 为 NULL/unknown |
| Kubernetes 隔离与恢复 | 专属 topic/group `llm-metering-k8s-cb5990b85930`，专属库 `metering_k8s_cb5990b85930`；3 个分区起点与批次凭据可见，已提交 6 条事件。缩容 Kubernetes Consumer 后组成员为 0，恢复后为 1，位点保持；Compose Consumer 不参与本次消费 |
| 入口 / 原网关 | 八个忽略请求 selected 保持 3→3，Doris 事件为 0；原 istio-system 网关 Pod 身份和重启次数保持不变 |
| 清理及数据保留 | `make k8s-clean` 实际执行两次均成功；测试 namespace、topic/group 已不存在，共享 Compose 全部服务继续运行；保留本次 Doris 6 条事件。原共享 metering 库仍为旧结构、287 条历史事件，未原地改表或接入新事件 |

Kubernetes 运行 ID：`cb5990b85930`。镜像由当前工作区构建，tag 按 HEAD 解析为 `d02a46c6c97a`，Wasm SHA-256 为 `c4b0ae3e6fd24f5db2a2b69771e6e5cbd2e095b573ab118347a50bc9d046f6bb`。

| 镜像 | 实际本地镜像 ID |
| --- | --- |
| metering-observer:d02a46c6c97a | sha256:c534196c1b8d6625cb08d0285128a0258454583a2697552d56a47b70d5852226 |
| metering-consumer:d02a46c6c97a | sha256:4af1f6163c27fe10a9a403b1083a5fe92c527b7155b26e4cd7b489b85839f6ba |
| metering-fixture:d02a46c6c97a | sha256:aec65a7b1b05ea796c86a722103a80470a799ce8e6fac4b11054dacf7d420d70 |

| 场景 | logical_request_id | 观察用量 |
| --- | --- | --- |
| json | `10e46fff-43eb-4b55-a400-a35955e06b0f` | known，10 / 4 |
| sse | `936912c8-0b44-431a-8123-8102c2a96cc1` | known，10 / 4 |
| unknown | `676405bb-7bc0-45ea-826e-39629cfe66db` | unknown，None / None |

本地诊断在 `.tmp/identity-final-make-test.log`、`.tmp/identity-final-local-test.log`、`.tmp/identity-k8s-final-test.log`；Kubernetes 结构化证据在 `.tmp/k8s-collection/kind-kind/metering-wasm-verify/cb5990b85930/`，清理核对在 `.tmp/identity-k8s-cleanup-check.json`。这些目录不提交，不含原始密钥；本文保留必要摘要及请求 ID。

本节只验收当前接口占位、采集模型和本地合成链路。真实第三方请求/响应、鉴权、异步执行/缓存及外部联调尚未实现；真实 Auth、汇总 Worker、查询 API、生产容量与完整 G0–G5 仍须各自实施及验收。

## 2026-10-16 Doris 字段注释与表职责文档

当前模型补齐 12 张实体表的表用途注释和 211 个字段注释，以及 7 个逻辑视图的 299 个字段注释。[数据库表与字段文档](../../doris-database-schema.md)为每个对象单列作用、写入者、读取者、粒度、键、关系、保留及实现状态；未来 Worker 与查询能力明确标记未实现。001/002/004/005/006 包含新注释，007 仅同步已有当前模型的元数据；本地和 Kubernetes 初始化链纳入 007，使用 utf8mb4 保留中文注释。

| 检查 | 实际结果 |
| --- | --- |
| 目标库 | 仅更新 metering_development；原 metering 及历史 metering_k8s_* 的列结构、默认值、键和注释元数据与更新前一致 |
| 注释覆盖 | information_schema 核对实体表用途 12/12、实体字段 211/211、视图字段 299/299，均非空 |
| 空库初始化 | 在项目专属临时库执行完整初始化及 007；类型、可空性、默认值、字段顺序和键标识与原当前模型一致，全部视图可查询 |
| 重复执行 | 临时库载入合成样本后重复执行 007，两次核对实体与视图完整数据相同；现有开发库重复更新后稳定业务字段及结构相同 |
| 在线审计说明 | 当前 Consumer 补偿巡检持续刷新 request_conflict_ids.detected_at 和 summary_dirty_notifications.created_at。在线数据核对排除这两个后台审计时间；独立临时库则核对完整数据，未为测试停止 Consumer |
| 查询与文档 | 仓库所有视图 SELECT 与注释前逐一核对相同；文档由 SQL 注释与实时编译元数据生成，再生成内容一致；007 可由生成器复现 |
| 采集冒烟 | 注释更新后 JSON、SSE、缺 usage 请求均 HTTP 200，各入库两事件，共 6 条；正常用量仍 known、10/4，缺 usage 仍 unknown、NULL/NULL，身份占位仍 invalid、usage_eligible=0 |
| 静态检查 | make test：61 项 Python 测试、契约校验、主模块与独立插件 Go 竞态测试通过；文档链接和 git diff --check 通过 |
| 清理 | 本次 metering_comments_verify_* 临时验证库已删除；开发数据及已有历史验证库保留 |

注释覆盖与元数据/数据摘要记录在 `.tmp/doris-comments-verification/`。本次未改变采集事件、消费提交屏障、字段类型、唯一键、分区或视图查询口径；未重新执行完整 Kubernetes 部署或生产 G0–G5，本次实际数据库验证使用既有本地 Doris 4.1.3。

## 2026-10-19：HTTPS 归属查询、环境枚举与名称快照

本次实现 [归属接口计划](host-service-provider-implementation.md) 的 HTTP 与名称增量。配置非空 HTTPS URI 后，以 `x-remote-data` 独立凭据调用两个 POST JSON 接口；空 URI 保持 `lookup_not_implemented`。环境统一为 `prd/stg/dev`，不增加旧枚举别名。三个名称为 UTF-8、最多 512 字节，与 ID 一同冻结为请求快照；失败侧显式为 null。

### 实现与故障边界

- 两类解析并行共享截止时间，默认 200 ms；默认最多同时处理 64 个请求，不排队。每个接口默认独立限速 1000 次/秒，响应头和正文各限 8192 字节。禁用环境代理、重定向和自动重试，使用验证证书的 HTTPS；凭据只通过环境变量/Secret 注入。
- Start 和 End 进度 ACK 不等待外部查询。End 先确认接收，归属冻结并输出事件后再发送 CloseStream ACK，避免网关提前取消查询。测试覆盖慢查询时 ACK 仍及时、并发满载立即降级、客户查询成功不被服务查询超时清除，以及 token 释放。
- `service_name`、`provider_name`、`customer_name` 同时进入 Kafka 事件、Doris 独立列和 `event_json`。名称改变影响 `payload_hash`，不改变稳定 `identity_hash` 或两类映射版本；跨事件名称不一致通过独立名称元组比较被隔离。名称不加入日/月汇总分组。
- 新库通过 001 等初始化定义取得名称列和视图；已有当前开发库由 008 添加缺失列并重建视图，再执行 007 更新注释。迁移可由 `scripts/doris_schema.py name-migration` 重现；本地脚本按实际缺失列执行，支持部分加列后重入，不删除或回填原始记录。

### 验证结果

| 验证 | 结果与边界 |
| --- | --- |
| `make test` | 71 项 Python 检查、契约/样例/摘要及 SQL/数据库字典检查、主模块和独立插件 Go 竞态测试通过 |
| `make build wasm` | Observer、Consumer、fixture 构建及 Wasm ABI 检查通过；Wasm SHA-256 为 `c4b0ae3e6fd24f5db2a2b69771e6e5cbd2e095b573ab118347a50bc9d046f6bb` |
| HTTP 适配测试 | 覆盖自签名证书拒绝、私有 CA 信任、认证请求头、原始 token 保真、404 错误体校验、401/403/5xx、429、歧义、畸形/超长/重复字段响应、超时、重定向禁止及本地限流不发送请求 |
| `GOFLAGS='-count=1' make local-test` | 完整集成测试通过，356.729 秒；覆盖 JSON/SSE、未知用量、重放与冲突、位点屏障/恢复/重平衡、SQL 汇总、改写边界及 Wasm 下载故障恢复 |
| 名称真实链路 | `TestHTTPNamesThroughKafkaToDoris` 使用合成 HTTPS 目录和真实 Observer gRPC → Kafka → 运行中 Consumer → Doris；逐字核对 Kafka 消息、独立列、event_json 及请求/用量视图中的中文名称；新请求保存新名称，旧请求仍返回旧名称 |
| 名称冲突 | Doris 请求视图、Consumer 冲突标记和有效用量过滤均覆盖名称不一致；稳定身份相同也不能绕过名称快照一致性检查 |
| 新库初始化 | 随机隔离库 `metering_names_init_f1087e22ce9f` 完整执行 001 → 002 → 004 → 005 → 006 → 007，名称列及全部依赖视图正常，检查后删除该测试库 |
| 当前开发库 | `metering_development` 已更新；12 张表共 214 个字段、7 个视图共 318 个字段，名称列/投影合计 21 个；121 条旧事件的三个名称仍均为 NULL，未回填或重写旧 event_json。legacy metering 及既有历史 Kubernetes 库未做结构变更 |
| `make k8s-test` | Kubernetes 1.22.17 / Istio 1.13.5 隔离验证通过，运行 ID `f2bafd6156ee`；JSON/SSE 用量 10/4，缺失用量保持 unknown/NULL，环境 stg；两个 Observer、独立 Consumer 及恢复场景通过，现有网关未修改 |
| `make k8s-clean` | 删除本次 metering-wasm-verify 命名空间及专属 topic/group `llm-metering-k8s-f2bafd6156ee`，保留共享 Compose、数据卷和 Doris 验证记录 |
| 文档与迁移 | 007/008 可由生成器重现，实际开发库元数据与数据库字典一致，`git diff --check` 通过 |

Kubernetes 诊断保存在本地忽略目录 `.tmp/k8s-collection/kind-kind/metering-wasm-verify/f2bafd6156ee/`；结果为 passed。镜像 tag 为 `57094905bbc8`，Observer 镜像 ID 为 `sha256:bb1ded7da02b5d2af1710d69719cdef659f20cf0b15f8aa81326a95343715f38`，Consumer 为 `sha256:314ee7aea818588849500d22693aa4238d51cbeafb52ffe8f1c4d9c0ccc96d43`。

本地正常配置的两个 URI 仍为空，保持未知归属；已知名称链路通过合成 HTTPS 目录验证。尚未提供真实第三方地址、凭据和生产配额；上述结果不代表真实目录数据、生产 TLS/权限/容量或完整 G0–G5 已验收。真实 Auth、usage-worker 和统计 HTTP API 仍不在本次交付范围。名称历史与 365 个 UTC 请求日期的明细共用保留期，日/月汇总不额外保存名称历史。

## 2026-10-19：HTTP 兼容、运行镜像 CA 与取消收尾修复

本次在既有实现上同时支持 HTTP 和 HTTPS。HTTP 不读取 ca_file；HTTPS 继续校验证书和主机名，并使用 Observer 运行镜像内的系统 CA bundle 及可选附加 CA。两种协议均保留 x-remote-data 认证、大小限制和独立限速，不使用环境代理、不跟随重定向、不自动重试或降级。

已接纳查询从取得并发额度时开始使用独立共享截止时间，观察流取消或超时不重置该时间。协调器收集两侧独立结果，截止时保留已提交的成功侧，仅未返回侧为 lookup_timeout；迟到结果不改写冻结快照。底层任务退出后才释放并发额度，客户任务返回后清理其 token。退出路径只定稿一次；End ACK 失败或超时仍保留已校验的 End 和请求结束时间。

| 验证 | 本次结果与边界 |
| --- | --- |
| `GOFLAGS=-count=1 make test` | 契约检查、71 项 Python 检查、主模块和独立插件 Go 竞态测试全部通过；HTTP/TLS 本地 socket 测试在获得权限后于沙箱外完成 |
| 取消与资源回归 | bufconn 覆盖两种单侧成功、取消后双方成功、双方超时及观察截止早于查询截止；另验证已取消父上下文的 20 次竞争、迟到 resolver 不阻塞收尾、不提前释放额度、不改写快照，且 token 清理无竞态 |
| ACK 回归 | End ACK 写入失败、取消和 ACK 超时均只定稿一次，保留有效 End 和相同名称快照；观察异常不会恢复为有效用量 |
| HTTP/TLS 回归 | 两类 HTTP 查询及认证通过，HTTP 忽略不存在的 CA 文件；独立进程验证 ca_file 为空时的系统信任路径，并覆盖附加 CA、主机名不匹配、重定向禁止及响应正文超时分类 |
| `make build wasm` | Observer、Consumer、fixture 构建及 Wasm ABI 检查通过 |
| 实际运行镜像 | `metering-observer:lookup-fixes` 构建通过；以镜像默认非 root 用户、禁用网络运行 `TestSystemTrustHelper` 的 roots 模式，确认实际 CA bundle 可读且系统根证书池非空 |
| `GOFLAGS=-count=1 make local-test` | 全量真实集成测试通过，392.385 秒；覆盖业务采集、消费恢复、冲突、位点屏障及 Wasm 下载故障恢复 |
| 两种协议的名称链路 | `TestHTTPNamesThroughKafkaToDoris/http` 和 `/https` 均通过；分别验证 Observer gRPC → Kafka → 运行中 Consumer → Doris，核对事件、独立列、event_json 和视图中的名称，保持旧请求的历史名称 |
| 数据库字典 | 刷新本地栈时保留数据卷，仅更新当前开发库注释和视图；只读核对 metering_development 元数据与生成的字典完全一致，结构仍为 12 表 / 214 列、7 视图 / 318 列 |

上述查询收尾保证限于 Observer 正常运行期间。HTTP 的传输保护由部署链路提供；本次使用合成归属服务，未验收真实第三方、进程退出持久化或生产 G0–G5，不扩大 Kafka 投递和 Doris 可见性保证。
