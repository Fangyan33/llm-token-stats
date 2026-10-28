# LLM 请求用量计量与统计设计

> 当前资格口径：客户身份未知不再单独否决 `usage_eligible`；`identity_invalid` 保留为归属诊断。客户/API Key 统计仍过滤未知归属。本文涉及旧资格判定的历史描述以此为准；日/月异步物化视图及每日任务见[汇总说明](daily-monthly-summary-design.md)，生产验收待完成。

状态：评审稿｜修订日期：2026-10-15。目标环境为 **Istio 1.13.5、Kubernetes 1.22**。本文不代表已部署或已通过性能验收。

当前归属接口已实现并以未实现占位运行：Wasm 临时发送入口 authority 与 Authorization: Bearer token，Observer 固定集群/环境并调用服务/客户 resolver；合法输入的四项 ID 和版本为 null，两类状态为 lookup_not_implemented，原始 usage 保留，identity_status=invalid、usage_eligible=false。真实 HTTP、第三方鉴权和网络缓存未实现；应用维度及 PG 归属读取均已删除。 采集/Consumer 已实现，tokenizer、真实 Auth、汇总 Worker 和门户仍待独立工作包，测试边界以运行说明为准。

## 1. 目标与已确认边界

一期建设用量明细与统计能力，为后续 token 计费提供数据，不执行扣款或 quota。

| 项目 | 决策 |
| --- | --- |
| 规模与环境 | 日均约 50,000,000 次调用；Istio 1.13.5；Kubernetes 1.22 |
| 归属主体 | 服务/供应商与客户/API Key 两类事件快照；稳定 ID 和轮换语义由第三方确定，当前占位不制造归属 |
| 身份来源 | Observer 的服务/客户查询接口；Host + 固定集群/环境获取 service/provider，Bearer token 获取 API Key/客户 ID；支持 HTTP/HTTPS 查询，空 URI 为占位 |
| 服务范围 | 只统计入口直接访问的服务 A，不追踪 A 内部调用的 B、C |
| 调用边界 | 一期网关观察逻辑请求及返回的 A 用量；隐藏重试不伪造成已测量执行，额外消耗不计给用户 |
| 客户端重试 | 每次到达平台的请求分别记录；一期不提供通用客户端幂等，不按正文去重 |
| 接口范围 | 兼容 Chat Completions 的 JSON / SSE，包括工具调用内容 |
| 暂不覆盖 | Embedding、Rerank、多模态及未适配协议；未命中当前 POST/精确路径规则的请求完全忽略，不产生元数据占位事件 |
| 用量来源 | 有效的服务 usage 优先，缺失字段分别估算或标记未知 |
| 用量定稿 | 每个可见请求形成一份计量结果，不提供历史补查、补报或事后变更 |
| 故障策略 | 业务不等待观察者或 Kafka，旁路拥塞/失败就放弃采集；未知不等于零 |
| 采集侧 | 不设用于故障恢复的事件缓冲、落盘、积压和补传队列 |
| Kafka | 事件保留 72 小时，仅供已入队事件的消费者恢复与重放 |
| 正文 | 仅在处理过程中临时使用，不持久化 |
| 时间归属 | 归属网关 Wasm 观察到请求头的开始时间；存储和默认报表使用 UTC |
| 数据保留 | 原始事件与日/月统计统一保留前 12 个完整 UTC 自然月加当月；归档为后续可选项 |

服务 A 返回的 usage 作为 A 的用量采信。若包含内部调用消耗，不再拆分或重复累计；若未包含，平台不主动还原内部消耗。

## 2. 当前 Istio / Envoy 能否直连 Kafka

### 2.1 TCP 连接不等于消息生产

| 问题 | 结论 |
| --- | --- |
| Envoy 能否连接 Kafka broker 的 TCP 端口？ | 可以转发客户端已有的 Kafka/TCP 流量，但不因此产生新的计量事件 |
| Istio 1.13.5 能否配置 Kafka 日志输出，将 HTTP usage 直接写入 topic？ | 标准访问日志配置没有这一能力，不能只填 Kafka 地址实现 |
| gRPC ALS 能否直接指向 Kafka broker？ | 不能；ALS 是 gRPC 日志协议，Kafka broker 接收 Kafka 协议，需要适配服务 |
| Kafka 网络过滤器能否将 HTTP usage 转成 Kafka 消息？ | 不能将该功能归于 Kafka 协议过滤器；当前默认构建也没有启用 Kafka 过滤器 |

Istio 1.13.5 的访问日志实现提供文件、gRPC ALS 和 OpenTelemetry 输出，没有 Kafka provider。普通访问日志记录请求元数据，不自动提取 JSON/SSE 正文中的 usage。[固定版本访问日志源码][1]

**本方案中 Envoy 不担任 Kafka producer。真正的 Kafka producer 位于独立观察者服务内，该服务不接收业务路由、不转发请求到 A。**即使 producer 的 TCP 连接经过 Envoy，生成 Kafka 消息的仍然是 producer 客户端。

### 2.2 固定版本核查结果

从发布标签追溯得到：

~~~text
istio/istio tag 1.13.5
  → istio/proxy 725ec273d665639aa9f6e0e9014af562a457c2f4
  → istio/envoy e2e40d8d667e1b7c752d27980307f22d9f73e9ee
  → VERSION 文件：1.21.3-dev
~~~

这是 Istio 固定分支的版本标识，不将它等同于上游 Envoy 1.21.3 正式版。[Istio 依赖][2]、[proxy 依赖][3]、[Envoy VERSION][10]

| 能力 | 对应固定源码结论 |
| --- | --- |
| ext_proc | 在默认编译扩展清单中 |
| ext_proc.observability_mode | 当时的 proto 中不存在 |
| ext_proc.async_mode | 虽有字段，但源码明确标注尚未实现，不能据此实现异步观察 |
| gRPC ALS | HTTP/TCP gRPC 访问日志扩展已启用 |
| Kafka broker / mesh filter | 列于 contrib 候选集合，实际启用的 contrib 只有 mysql_proxy；默认构建未启用 Kafka 过滤器 |

依据为[固定扩展清单][11]和[对应 ext_proc.proto][12]。旧版 ext_proc 异步观察不可用，不等于旧版 Wasm 不能旁路上报。该版本 Wasm 实现具有请求/响应正文回调，以及异步 grpcStream / grpcSend hostcall；可以在业务回调返回 Continue 的同时提交旁路数据，不等待观察者确认。[固定 Wasm 实现][13]

主方案为 **EnvoyFilter 加载 Wasm 观察插件 + 独立观察者服务**。EnvoyFilter 是配置过滤器链的资源，Wasm 是被加载执行的插件，两者不是互斥方案。实际私有镜像仍需验证编译能力和 ABI/SDK 兼容性。

### 2.3 为什么不只用日志收集器

Envoy → gRPC ALS 接收器 → Kafka producer → Kafka 可以传递访问日志元数据，但普通 ALS 不提供用于估算的完整请求/响应正文。仅靠它不能完成本方案的 LLM token 计量。

一期不增加 ALS 接收器作为必需组件，不采用 stdout → 节点日志文件 → 日志采集器的计量链路，避免引入已排除的落盘与补传。

## 3. 观察者架构：业务不经过统计服务

### 3.1 业务路径保持直达 A

~~~mermaid
flowchart LR
    U[调用方] --> X[ingressgateway: ext_authz]
    X -. gRPC Check .-> H[API Key Auth]
    X -->|访问允许 + 原 Bearer| W[ingressgateway: Wasm 观察插件]
    W --> G[ingressgateway: router]
    G --> A[服务 A]
    W -. 异步 gRPC，失败丢弃 .-> O[独立观察者服务]
    O -->|Kafka producer| K[Kafka：72 小时]
    K --> C[独立 Doris Consumer]
    C --> D[Doris 一年明细与日/月汇总]
~~~

**业务：调用方 → ingressgateway ext_authz → 网关内 Wasm/router → A。**Auth 是新增同步鉴权调用，只接收请求头属性、不代理业务正文；Wasm 位于既有 Envoy 进程内，独立观察者服务不在业务路由中。

**计量数据链路：Wasm 回调 → 异步 gRPC 观察流 → Observer 解析和估算 → Kafka producer → Kafka → 独立 Doris Consumer → Doris。**一期由独立 Go usage-worker 生成日/月汇总，默认 K8s CronJob 每分钟调度，单个发布者串行执行；通知、水位及任务状态全部保存在 Doris，采集与汇总无需额外 PostgreSQL。原始分区由 Doris 动态管理；归档是[后续可选设计](archive-optional-design.md)。

不把 VirtualService 的业务目的地改为 Observer，不为统计修改上游重试、取消或超时策略，也不将镜像请求发送给模型再推理一次。鉴权结果决定请求是否进入业务路由；计量统计结果不参与鉴权、路由或响应生成。

### 3.2 Wasm 负责轻量观察，Observer 负责计量

**入口过滤为强制约束**：默认不采集；必须同时满足全局启用、规则显式启用、HTTP 方法和精确路径匹配。当前规则使用 `route_name: "*"` 忽略路由名，采集所有到达该观察位置的 `POST /v1/chat/completions`，不要求路由属性或 metadata；自定义路径须显式登记。插件按请求头阶段可见路径去掉查询串后精确匹配，不额外解码或模糊匹配；路径重写顺序须实测固定。事件路由名记录真实 Envoy HTTP 路由名，不可读时为 `unknown`，仅用于诊断；适配器来自规则，服务/供应商来自 Observer 接口，精确路由规则保留兼容。Observer / Consumer 按 service_id 白名单接纳，不以路由名判断身份一致性或选择价格。客户/API Key、service_id/provider_id 和集群/环境快照承担归属；实际后端、供应商与价格版本需要独立字段，不能从路由名推导。规则、诊断路由和配置版本在请求内不变。

未命中的指标 GET、健康检查、模型列表、OPTIONS 等请求不生成计量 ID、不读取或改写正文、不申请观察额度、不建立旁路流、不添加计量响应头、不产生 Kafka 计量事件，仅增加本地固定原因计数。采集开关不控制鉴权，也不能由客户端 Header 控制。命中的失败响应和断流仍尝试记录可观察事件，未知 usage 不补零。完整率的独立分母和 Kafka 容量基数必须限定为同路由、方法、路径及观察位置的目标请求，不能用全网关流量。配置、回调和验收以[采集开发方案第 2.3 节](gateway-request-collection-implementation.md#23-入口采集过滤强制约束)为准。

| 位置 | 职责 |
| --- | --- |
| ext_authz/Auth | 验证平台 API Key、状态、有效期和访问范围，返回业务访问判定，在 Wasm 读取前保留原 Bearer |
| Wasm 请求头回调 | 先执行规则启用、方法和路径白名单（当前 `route_name: "*"` 忽略路由名）；命中后从 Authorization: Bearer 提取临时 token，并取得入口 authority，记录请求开始时间并准备请求改写和旁路观察流；logical_request_id 由 Observer 在有效 Start 后生成 UUIDv4 |
| Wasm 请求正文回调 | 对目标流式 JSON 有界缓冲并设置 `stream_options.include_usage=true`，再观察最终转发正文 |
| Wasm 响应头回调 | 若响应头回调前已收到有效 Start ACK，向白名单请求返回含 Observer ID 的 `X-Metering-Request-Id`；不等待 ACK，也不以此承诺事件已入库 |
| Wasm 响应正文回调 | 有界复制当前片段，附流内序号、方向及偏移，异步提交；立即返回 Continue |
| Wasm 结束回调 | 报告可观察的终态与最后序号；不等待观察者响应，不延长业务流生命周期 |
| Observer | 已实现有效 Start 后生成 UUIDv4 并回传 ACK，接收片段、解析 JSON/SSE、检查缺段和提取上报 usage；有界 token 估算仍是后续设计 |
| Observer 内 producer | 只把结构化计量事件写入 Kafka；不把正文片段写入 Kafka |
| Doris Consumer | 独立消费 Kafka，校验与规范化事件、计算内容摘要、写入 Doris 并在确认后提交 offset；不负责归档或重新估算 |

Wasm 不做重型 tokenizer 运算，不缓存完整长响应。除对目标流式请求设置 `stream_options.include_usage=true`、在及时取得 Observer ID 时添加计量查询 ID 响应头外，不修改其他业务字段或响应正文，也不发送本地错误来代替业务响应。Observer 不承担转发业务、代替模型生成、管理重试或传播取消。

采集只读取指定 Authorization: Bearer，不枚举头集合，不使用身份 metadata 或伪造 ID 补值。原 token 仅临时通过受保护观察链路交给客户 resolver，处理后清理，绝不落库/日志；当前占位不会发往第三方。鉴权与上游凭据策略独立，见[Auth 职责](api-key-auth-service-implementation.md)。

一期插件只作用于需统计的业务路由。Observer 的上报路由必须排除，避免插件观察自己的上报而形成递归。

### 3.3 异步 gRPC 不是新版 ext_proc

这里使用 Wasm host 提供的通用 gRPC 异步能力，自定义观察消息协议；不是配置旧版尚未实现的 ext_proc.async_mode。固定源码中的 grpcSend 提交消息后返回，不等待服务端确认，但不代表数据已送达或内部没有缓冲。[固定 Wasm 实现][13]

建议按请求建立逻辑 gRPC 观察流，底层 HTTP/2 连接复用；不要为每个 SSE/token 片段创建一次 HTTP 请求。消息包含开始、正文片段和结束标记，这些是临时观察消息，不是 Kafka 的两类计量事件。

必须定义以下流控：

- 每请求/每实例的在途消息数、字节数和观察流数量有上限。
- Observer 的确认仅用于释放发送额度，不是业务继续的条件，也不是 Kafka 持久化确认。
- 达到上限、连接失败或确认超时，取消对应旁路流，丢弃尚未采集的内容；业务回调仍返回 Continue。
- 不把 gRPC 底层缓冲当作无限队列；需验证取消后内存释放，以及慢接收端不会造成网关资源持续增长。
- 不重传丢失的片段，不落盘，不在恢复后补发。片段缺失不能伪装成完整计量。

正文只经过有限内存和旁路网络，Observer 完成定稿或放弃后释放；Kafka、Doris 和归档只保存计量字段。

### 3.4 请求结束后的上下文生命周期

固定 Proxy-Wasm ABI 的 http_call、grpc_stream 和 grpc_send 会经 root_context() 调用，标准异步 hostcall 由 root 持有，不直接随单个业务请求 context 销毁。因此在请求结束时提交异步终态有底层依据，不需要等待统计确认来延长业务请求。[固定 ABI 实现][15]

Root/VM 销毁仍会取消其调用；SDK 回调也不能访问已失效的业务请求对象。实现需按本地流 token 保存有界上报状态，并关联 ACK 返回的计量请求 ID，验证 SDK 回调映射、请求结束、插件重载及 root 生命周期。异步提交不代表可靠送达，不能宣称在 onLog 调用发送后最终计量必达。[固定 Wasm 实现][13]

Root/ACK/结束消息生命周期已有本地实现与合成验证，生产热更新和长期资源门槛仍待验收，具体证据见运行说明。结束消息丢失时，Observer 只能记录已观察的部分用量或缺失状态，不能制造正常完成状态。Observer 内可以有有限的活跃请求解析状态，但不能形成故障恢复队列。

### 3.5 失败放行与资源影响

通过 EnvoyFilter 显式配置 Wasm 插件的 fail_open=true，并要求所有正常请求/响应回调返回 Continue，不返回 StopIteration 或等待统计结果。[固定 Wasm 配置][14]

fail_open 不能覆盖所有安装错误：该版本说明 on_start/on_configure 失败可能导致 xDS 配置被拒绝，初始启动时也可能无法启动代理。因此不把“插件失败放行”宣传为任意发布或资源故障都无风险。

Wasm 随 Observer 镜像发布，由其独立 HTTP 下载端口提供固定文件；网关使用 EnvoyFilter 原生 `code.remote`、HTTP 集群和文件 SHA-256 加载，无需构建定制网关镜像。下载只发生在插件加载时，首次加载有下载等待；单次超时 5 秒、额外重试 0 次。只保留当前版本，下载失败允许暂时缺少采集，恢复后显式重新加载；回退先恢复匹配的 Observer 镜像再恢复网关配置。仍需验证 ABI、配置和初始化，通过单独测试网关及少量业务路由灰度。Istio 1.13.5 的 WasmPlugin 转换代码未给 PluginConfig 设置 FailOpen，因此选择 EnvoyFilter 显式配置，不照搬新版 WasmPlugin 的 failStrategy 字段。[固定 Istio 转换实现][16]

“不影响业务”在本方案中的可验收含义是：不新增业务转发节点、不修改业务内容/路由、不等待 Observer/Kafka、不因旁路故障主动拒绝请求。Wasm 回调本身在 Envoy 工作线程上运行，复制、序列化及发送会使用 CPU/内存，不能承诺零时延或零资源风险。必须压测并设置硬上限。

### 3.6 重试可见性不能靠旁路弥补

单独放在网关 downstream 过滤器链中的插件，能看到逻辑请求及最终返回给客户端的响应，通常看不到被网关透明重试吞掉的各次失败响应。因此不再由统计系统接管重试，也不能继续承诺每次重试都能得到 token。

网关采集结果标记 observation_scope=gateway_response；未知重试消耗不计给用户，也不记成零。能取得的尝试总数只作为元数据，不能代替各次执行 usage。

若必须保留对 A 的逐尝试计量，需要在 A 已有 inbound Envoy 等可见每次尝试的位置再部署只读观察插件，并验证现有请求关联信息。这里仍只观察 A，不展开 B、C；不能假设所有服务已有 sidecar，或未核查关联能力就宣称逐尝试已覆盖。该扩展是独立接入项，不是一期网关插件已经具备的能力。

### 3.7 Observer 到 Kafka

Observer 以 Kubernetes 1.22 支持的普通 Deployment / Service 部署，用 Kafka 原生 producer 写 topic。配置 TLS/SASL、写权限，并确保能访问 bootstrap 及 broker 返回的 advertised.listeners 地址。[Kafka producer 配置][4]

Observer/Kafka 不可用只丢计量，不改变业务流。业务没有新代理一跳，资源仍需为旁路正文吞吐、活跃解析状态和 tokenizer 计算单独测算。

### 3.8 Kafka 到 Doris

Doris Consumer 是与 Observer 分开部署的 Go Deployment，不在业务请求路径中。它使用独立消费组读取 `llm-metering-events`，校验 `schema_version` 与事件字段，规范化并计算 `payload_hash`，写入 Doris；写入确认后才提交 Kafka offset。重放由事件 ID 与内容摘要识别重复或冲突。未知 schema 版本暂停对应分区、报警并等待 Consumer 升级，不能当永久非法事件跳过；永久非法事件按有限诊断规则处理。一期日/月刷新由 usage-worker 负责；Consumer 在落库和冲突标记可见后，确认 Doris 日期通知及批次凭据再提交 Kafka offset，Worker 未启用也登记。Kafka committed offset 是恢复起点，Doris 水位按各分区连续确认范围推导，参见[Doris 协调设计](doris-summary-coordination.md)。归档为后续可选项。[采集开发方案第 8 节](gateway-request-collection-implementation.md#8-consumer-落库与异常事件)

## 4. 一次请求最多产生两条记录

“记录”指 Kafka 原始事件，不是 OpenAI SSE 数据块、gRPC 正文片段或两次模型调用。正常情况下，一次逻辑请求只产生**开始事件和最终结果事件**；Observer 写 Kafka，Wasm 不直接写。

| 事件 | 发送时点 | 记录内容 |
| --- | --- | --- |
| `request_started` | Observer 收到有效 Start 后立即尝试发送 | 请求 ID、固定归属快照/解析状态、网关观察到的开始时间；不含原始密钥或用量 token |
| `usage_observed` | Observer 收到有效 End、核对片段并定稿时发送；无法继续观察而触发超时或断流定稿时也尽力发送一次 | **用量和网关终态合并在同一条记录中**：输入/输出 token 及来源、完整性、`terminal_observed`、可取得的结束时间、HTTP 结果和已知尝试数 |

不再单独发送 `request_finished`。本文的“请求 ID”统一指 `logical_request_id`；两个 Kafka 事件的 `event_id` 分别为 `<logical_request_id>:start` 与 `<logical_request_id>:usage`，对应 `request_started` 与 `usage_observed`。gRPC `End` 是观察消息，`request_finished_at_ms` 是结果事件字段，均不是第三种 Kafka 事件。即使用量未知，该事件仍可记录可观察的终态或缺失原因。每个请求至多有一条定稿结果；Kafka 重放可能产生物理重复，由消费者按事件 ID 去重。

### 4.1 流式请求的时间线

兼容 OpenAI Chat Completions 的请求设置 `stream_options.include_usage=true` 后，正常流式响应会在 `data: [DONE]` 前返回一个包含整次请求 usage 的最终数据块。Observer 先解析并暂存服务上报值，再等待网关观察到流结束。Wasm 在请求结束回调发送 End；Observer 收到有效 End 后，定稿并发送一条 `usage_observed`，同时写入 token 与网关可观察终态。**最终 usage 数据块不是请求结束信号**。OpenAI 说明，流中断时可能收不到最终 usage 块。[OpenAI 流式用量说明][17]

~~~text
logical_request_id = 7f6c2a90-4b13-4b68-9d21-67283e0c5f44
1. 收到 Start → request_started (<logical_request_id>:start)
2. 响应末尾解析最终 usage → 暂存 input=1000、output=500
3. 收到有效 End → usage_observed (<logical_request_id>:usage,
   input_tokens=1000, output_tokens=500,
   terminal_observed=true, request_finished_at_ms=网关观察到的结束时间)
~~~

结果是 1 次逻辑请求、正常路径最多 2 条 Kafka 事件；token 只在结果事件中累计一次，不为每个 SSE/token 片段写事件。结果事件的 Kafka 写入时间可以晚于请求结束时间；网关终态也不证明客户端收齐全部字节。

### 4.2 断流、拒绝和重试

- 没收到 Start，可能整次请求都没有 Kafka 记录。
- 收到 Start 但 Observer 在结果发送前退出或观察流丢失，可能只有开始事件；不会伪造完成结果。
- 若 Observer 仍能完成定稿，但 End 缺失，结果事件的 `terminal_observed=false`、`request_finished_at_ms=null`；可见用量标为上报、部分或未知，不能写成正常完成。
- 收到 End 但最终 usage 缺失或无效时，仍发送同一条结果事件，标明估算、部分或未知；未知不等于零。
- 开始与结果事件独立发送，可能只成功写入一条。结果事件自带身份和开始时间快照，因此没有开始事件也能解释结果；入队前丢失的结果及其用量不能靠重放恢复。
- 网关隐藏重试不计为可见的额外执行，不凭尝试数推断 token。若未来在 A 侧逐尝试观察，应另定关联及去重规则，不能与网关结果重复累计。

### 4.3 一次定稿与去重

Observer 在正文仍可见时完成计量；有效服务上报优先，缺失则估算或未知。活跃解析状态有时间和内存上限，不作为故障恢复队列。结果事件一经定稿，不因后来片段、补查或重放而改变。

两条事件按请求 ID 关联，不依赖先后入库顺序。开始事件仅证明该事件曾成功入队，不证明结果必达；结果事件中的 `terminal_observed` 才表示是否收到有效 End。无独立网关基数时，不能用“有结果 / 有开始”的比例冒充真实采集完整率。

## 5. 无采集缓冲的精确定义

不设业务事件待发送队列，不写本地日志、WAL 或 spool，不在故障恢复后后台补传。没有空闲发送执行容量就放弃事件，不排队等待 Kafka 恢复。

解析状态、临时正文、序列化内存和网络在途数据属于必要瞬时内存，设置硬性资源上限及发送期限，不承担故障积压。

Kafka producer 自带内存、批处理和重试机制，必须配置为有界发送，避免默认行为形成后台积压：不为攒批主动等待，禁止跨故障持续重试，超时事件不在恢复后重新提交。具体参数按客户端语言和版本验证，不能宣称 producer 零内存。[Kafka producer 配置][4]

Kafka 可采用 acks=all 配合 min.insync.replicas；确认等待在计量发送任务内，不阻塞业务响应。客户端超时可能是 broker 已写入但确认丢失，应标记发送结果不确定，不能全部当作确认丢失。

Wasm 到 Observer 的丢失同样不可恢复。旁路流的在途内存和 Observer 活跃请求解析状态仅用于当前处理，不能作为可恢复积压。

**72 小时仅保护已进入 Kafka 的事件，无法恢复写入前丢失的数据。**不得靠重新调用模型补出原请求用量。

## 6. 用量归一化与估算

### 6.1 字段来源与完整性

| 来源 | 含义 |
| --- | --- |
| reported | 服务提供且字段有效；表示采信服务，不是独立证明真实计算量 |
| estimated_model | 使用登记并验证的模型 tokenizer、模板及估算规则 |
| estimated_generic | 使用通用估算，不能当作模型真实计数 |
| unknown | 无法取得或合理估算，数值为 null，不用零代替 |

每字段标记来源，完整性另存。模型匹配估算也可能只覆盖断流前可见输出。维护者不提供 tokenizer 配置仍可接入，但不能将通用估算标为已验证模型匹配估算。

保存 tokenizer、模板、协议适配器和估算器版本，仅用于解释首次结果的计算依据。

### 6.2 计量规则

- 校验类型、非负性、范围及字段关系；合法零值不是缺失。
- 保存有限大小的原始 usage，截断必须标记；不保存完整正文。
- 输入、输出为归一化总量；缓存读/写和推理字段是否已包含在总量中，由适配器声明，不能重复相加。
- 缓存和隐藏推理不能由可见文本可靠推断，缺失时保持未知。
- 服务总量与推导总量冲突时保留依据和异常，不凭猜测凑数。
- 输入估算考虑角色、模板、工具定义与参数，不能只统计 content。
- 多候选输出按协议归一化，不能只处理 choices[0]。

### 6.3 流式与资源上限

跨片段解析 UTF-8、SSE 和 JSON。网络片段不等于 SSE 事件或 token，不能各片段独立分词后直接相加。

使用验证过的增量分词或有界拼接后分词；设置请求级与实例级内存、CPU、时间上限。超限时继续业务，优先保留可取得的上报值，其余部分估算或未知，不能把截断文本计数标为完整用量。

流内累计值、占位值和最终值在定稿前按协议语义处理，不简单取最大值。业务取消仍由既有网关链路处理，观察者不改变取消行为，不为拿 usage 延长生成；观察不到的后端剩余消耗保持未知。

估算必须在正文仍可见时完成。正文不持久化，意味着之后不能更换 tokenizer 重算该请求，或在采集进程重启后恢复未完成估算。

LiteLLM 的计数工具及部分流式聚合路径可作为分词兜底参考，不是恢复隐藏用量的保证。[文档][5]、[源码][6]

## 7. 数据模型、去重与统计

### 7.1 逻辑字段

| 字段组 | 核心字段 |
| --- | --- |
| 事件身份 | `event_id`、`event_type`、`schema_version`、`producer_id`；`event_type` 仅为 `request_started` 或 `usage_observed` |
| 请求/观察 | `logical_request_id`、`x_request_id`、`observation_id`、`observation_scope`、`service_id`、`api_key_id`、`customer_id`、`provider_id`、`request_host`、`cluster_name`、`deployment_environment`、`mapping_status`、`mapping_version`、`customer_mapping_status`、`customer_mapping_version`、`identity_status` |
| 模型 | `request_model`、`response_model`；独立模型版本字段如计费需要，在一期后续明确并升级 schema |
| 时间 | `request_started_at_ms`、`request_finished_at_ms`、`observation_ended_at_ms`、`event_created_at_ms`、`ingested_at`；前四项为 Unix 毫秒，最后一项为 Doris 入库时间 |
| 结果 | `http_status`、`business_outcome`、`terminal_observed`、`observation_status`、`upstream_attempt_count`、`anomaly_codes` |
| 用量 | `usage_status`、`usage_eligible`、`input_tokens`、`output_tokens`、`cached_read_tokens`、`cached_write_tokens`、`reasoning_tokens`、`reported_total_tokens`；仅 `usage_observed` 携带用量，可空 |
| 依据 | Kafka `usage` 子对象中的 `{value, source}`，Doris 中各 token 列及其 `_source` 列；`raw_usage_json`、`raw_usage_truncated`、`adapter_id`、`config_version`；tokenizer/模板/估算器版本为一期估算子项的待定扩展 |
| 归属准备 | 用户正常消耗、平台重试/故障消耗、待核对；一期不扣款 |
| 扩展 | `measurement_unit`、多模态扩展项及平台元数据引用 |

`X-Metering-Request-Id` 响应头在 Start ACK 及时到达时承载 Observer 生成的 UUIDv4 `logical_request_id`；该 ID 不编码时间，统计日期从 `request_started_at_ms` 计算；ACK 迟到时可缺失。可选的 `X-Request-Id` 只映射到 `x_request_id`，不能作为主键。`identity_status` 仅用 `known` 或 `invalid` 表示当前接入结果，客户 resolver 未实现/失败时标 `invalid` 并报警；`usage_eligible` 只表示有效用量统计候选，不表示收费资格。完整字段定义和类型以[采集开发方案第 6、7 节](gateway-request-collection-implementation.md#6-kafka-事件与发送)为准。

每个观察结果不可变，不设置业务用量修订号、历史结果替换接口或后续补查任务。`schema_version` 和一期后续估算器版本只解释原记录结构及算法。

### 7.2 重放仍需去重

消费者落库成功但提交消费位置失败后，可能重复读取事件。重放保持 event_id 和原内容不变，不能因为去掉事后变更功能就去掉幂等处理。

一期使用一个原始事件 topic `llm-metering-events`，按 `logical_request_id` 选择分区；当前仅 Doris Consumer 消费并落库。一期 usage-worker 从 Doris 去重后的请求投影刷新日汇总，再从已发布日汇总刷新月汇总；归档不纳入一期。开始和结果事件分别按 event_id 去重，token 只累计去重后的结果事件。

同一 `event_id`、相同内容视为重复；同一 `event_id`、不同内容为事件冲突。同一 UUIDv4 的开始/结果时间或身份快照不一致属于请求冲突；两类冲突均跨日期隔离报警，不覆盖或重复相加。归一化结果也应在重放时确定一致。

Kafka 事务或 producer 去重不自动保证外部 Doris 写入恰好一次，落库与消费进度必须配合验证。[Kafka 投递语义][7]

### 7.3 Doris 与汇总

在线以类型化列保存事件，保留规范事件 JSON 供冲突核验；请求/观察视图基于类型化列关联，不在常规查询中反复解析 JSON，产品在线窗口为一年（365 个 UTC 日期），按请求开始日分区。Doris Unique Key 包含请求日期、请求 ID、事件 ID 与内容摘要：同内容重放落在同一键，不同内容保留为冲突变体。

Doris Unique Key 的 Merge-on-Write 用于同内容重放幂等，不承载多版本业务用量；事件与请求冲突仍需跨在线分区按 UUID 检测，并长期保留冲突标记。分区、分桶和请求关联的查询性能须在目标集群压测。[Doris Unique Key][8]

日/月统计采用 usage_daily_mv 与 usage_monthly_mv 异步物化视图：日统计从去重、隔离冲突后的请求投影生成，月统计嵌套累加日统计；按集群、环境、客户、API Key、服务、供应商、模型和用量来源聚合。日统计只计算已结束日期，迟到事件和冲突通知触发历史修正，零行结果也清除旧分组。原始、日、月统一保留前 12 个完整自然月加当月。服务和供应商统计包含客户未知但服务归属已知的有效用量，客户报表过滤未知客户。一个逻辑请求计一次。

不将重放事件直接接到求和物化视图；冲突到达后也必须能撤销或重算受影响汇总。一期汇总工作包在 Doris 保存独立通知、刷新状态与输入版本，使用临时分区完整批次发布；日发布版本变化触发月重算，查询按前后状态版本校验。每分钟调度并不保证端到端 60 秒可见，刷新性能仍以可见目标验收；规模压测不达标时引入小时中间结果优化扫描。表结构、字段、NULL/零值、迟到处理、发布状态与验收见[日/月汇总设计](daily-monthly-summary-design.md)。

三个口径分开：

- 逻辑请求量：每次请求一次。
- 服务可见用量：网关最终响应的上报/估算用量；明确不包括不可见的重试消耗，不能声称完整后端计算量。
- 用户应收候选：排除平台重试额外消耗和平台故障应承担部分；不生成实际账单。

用户取消只将已确认消耗列为应收候选，原因或用量不明时标记待核对。一期“待核对”只是标签，不包含自动补查或处理流程。未来收费默认接受服务上报及已验证模型匹配估算，通用估算不直接进入结算。

## 8. 明细、日/月统计与可选归档

一期在 Doris 查询一年原始事件，提供日汇总和月汇总，汇总独立保留约两年。因此一年以前仍可查看已发布日/月统计，不能查看已经到期的请求明细。归档只保留[可选设计](archive-optional-design.md)，其任务、状态库、外部存储和历史查询接口后续另行实现，不作为一期运行依赖。

提供：

- 请求 ID 精确查询及对应服务 A 的观察结果。
- 客户若收到响应头 `X-Metering-Request-Id`，可以 Observer 生成的 UUIDv4 `logical_request_id` 精确查询；查询覆盖在线日期分区，不从 ID 推导日期；该头因 ACK 迟到可能缺失。客户可选保存网关可见的 `X-Request-Id` 作辅助关联。后者可能由客户端提供或跨请求复用，须限定客户与时间窗口，不作唯一键或授权凭据。
- 按时间、API Key、服务、模型、结果和计量来源筛选分页。
- 日/月趋势与服务、API Key 排行；小时趋势按需采用中间汇总。
- 上报、估算、未知、事件缺失分别展示。
- 一年内明细大范围异步导出；超过原始保留期只提供已发布日/月统计，明细归档检索属于可选扩展。

显示数据水位、更新时间和异常时段。首次事件因消费延迟较晚入库时，统计正常刷新，但已定稿用量不被后续服务查询结果替换。两年汇总中超过一年的部分没有完整请求明细可下钻，明确展示保留期边界。

客户查询服务从登录身份注入 `customer_id`，只返回该客户且无冲突的请求；无 usage 事件时展示“未观察到用量”，token 保持未知，不显示 0。插件返回了计量 ID 不保证旁路事件已入库；暂未查到与采集丢失不能仅凭查询结果区分。具体字段和 SQL 见 [网关请求采集开发实现方案](gateway-request-collection-implementation.md)。

## 9. 故障行为

| 场景 | 业务行为 | 采集行为 |
| --- | --- | --- |
| API Key 无效、吊销或越权 | ext_authz 返回 401/403，不进入 A | 不进入 Wasm 采集；使用独立鉴权指标统计 |
| 真实 Auth 超时或不可用（独立待实施方案） | 返回 503 | 不进入采集；Auth 记录具体内部错误，对外只返回固定错误码 |
| 真实 Auth 数据源故障但其有效缓存可用（待实施） | 按缓存结果继续鉴权 | 鉴权允许的请求照常进入采集；缓存不得超出原 TTL |
| 客户查询未实现/失败或 Bearer 输入非法 | 按独立鉴权结果继续 | 标 `identity_status=invalid`、`usage_eligible=false` 并记录解析状态，保留已观察用量，不采信客户端自报 ID |
| Observer 不可用/变慢 | 原路径继续，不等待旁路 | 达上限或超时后取消观察流，丢弃，不积压补传 |
| Wasm 可被 fail_open 处理的运行故障 | 绕过插件，原链路继续 | 本次或后续观察缺失 |
| Wasm 配置/初始化错误 | 可能导致配置拒绝或启动失败 | 不能用 fail_open 掩盖，必须发布前验证 |
| Wasm 回调计算或复制过重 | 可能增加网关延迟和资源压力 | 硬上限、关闭观察和灰度回退；不得声称零影响 |
| Observer 解析/估算超限 | 原路径继续 | 放弃超限计算，计量标部分或未知 |
| Kafka 写入失败 | 原路径继续 | Observer 丢事件，不落盘、不补传 |
| Observer 进程退出 | 业务连接仍由 Envoy 与 A 承载 | 活跃计量状态丢失，不伪造完整结果 |
| Envoy 自身退出 | 原业务连接仍可能失败 | 未上报片段/终态丢失 |
| 消费者/数据库故障 | 原路径继续 | 已入 Kafka 的事件在 72 小时内恢复 |
| 用户取消 | 既有业务链路处理 | 只记录可见结果，不改变取消语义 |
| 事件重放 | 无业务影响 | 去重后统计不增加 |

未知/估算不会等待一段时间后变成已确认，没有补取任务。

## 10. 容量测算方法

### 10.1 请求、观察消息与事件

日均 50,000,000 次约为平均 579 请求/秒。网关插件完整观察一次正常调用，Observer 产生开始、结果两条 Kafka 事件：

~~~text
每日 Kafka 原始事件量 ≈ 50,000,000 × 2 = 100,000,000 条
平均 Kafka 事件速率 ≈ 1,157 条/秒
~~~

这里不包含 Wasm → Observer 的 gRPC 正文片段。旁路消息量由正文分片决定，可能远高于 Kafka 事件量，必须单独测算；不能拿 1,157 条/秒估计旁路解析链路能力。

按应采集量配容量，不扣除预计漏采量。开始事件通常小于包含用量和终态的结果事件；网关不可见的重试不能假设已生成额外的执行记录。

### 10.2 字节量示例

使用十进制 KB/GB/TB，假设平均每条原始事件 1 KB，尚未测量：

| 项目 | 基线数量级 |
| --- | --- |
| 无重试请求 | 2 条事件，示例合计约 2 KB |
| 原始事件每日量 | 约 100 GB |
| Kafka 72 小时 | 约 300 GB 逻辑数据；3 副本约 900 GB，另加运行余量 |
| 原始事件在线 365 天 | 约 36.5 TB，未计压缩、副本、索引及未来增加的采集事件 |
| 日/月汇总 | 按活跃维度组合及前 12 个完整自然月加当月的日/月分组估算；不按原始事件条数计算 |
| 可选明细归档 | 一期不计入，后续按归档期限、压缩比、快照数量另算 |

若额外物化每请求一条约 1 KB 的明细，则另有约 50 GB/日、18.25 TB/365 天数据，不能与两事件存储混为同一口径；是否额外物化由查询压测决定。

生产估算分别测量两类事件大小；合并后的结果事件可能大于原单独用量事件，上表 1 KB/条不能当作实际节省量：

~~~text
每日字节量 = 开始事件数 × 开始事件大小
           + 结果事件数 × 合并结果事件大小
~~~

Kafka 按 72 小时保留并配足磁盘，避免更小的字节上限提前清理。队列副本、Doris 原始表副本/索引和日/月汇总空间分开测算；归档启用后另算。

### 10.3 网关与 Observer 容量

- 业务仍走原 Envoy → A 路径；Wasm 额外复制正文并产生旁路网络流量。
- 旁路字节量约为被观察的请求/响应正文量加协议开销，不是最终计量事件量。
- 活跃观察请求数约为到达率 × 平均时长。579 请求/秒、平均 30 秒约 17,370 个活跃请求；实际流时长未测。
- Observer 负责 tokenizer CPU 与解析状态，必须压测 usage 全缺失的场景。
- 网关限制每请求及每实例在途字节、消息、观察流和回调耗时，Observer 也设置活跃状态与估算上限。
- Kafka/Observer 故障不得通过未受限底层 gRPC 缓冲反向耗尽网关资源。

用实测峰值两倍验收，并测慢 Observer、无确认、断开及持续长输出下的业务延迟、内存增长和丢计量行为。

## 11. 指标与验收

### 11.1 不以采集结果自证完整率

正常运行按日请求记录完整率目标为 99.99%。分母来自独立的原有网关指标，限定为应被观察的同口径业务路由请求；插件未采到的请求也在分母内，前置拒绝按约定另列。

两类事件均来自同一 Wasm → Observer 采集路径，不能用“已收到结果 / 已收到开始”证明没有漏采。独立基数不可靠时，真实完整率标记未知。

展示网关同口径请求数、已记录逻辑请求数、观察结果覆盖情况、上报/估算/未知比例、发送失败/不确定计数和 Doris 消费延迟；归档上线后另展示归档延迟。请求有记录不代表用量已知，计数差也不能还原漏掉的 token。

正常与故障时段分别展示并保留全日结果。日均 5,000 万的 0.01% 对应 5,000 次缺失预算，不等于允许重复累计。

### 11.2 性能目标

- 正常请求结束后 60 秒内可查用量和常用统计。
- Wasm 观察引入的 P99 首包额外延迟目标不超过 10 毫秒，同时测持续流式延迟/吞吐；统计网络等待不得成为业务等待，这是待验证目标，不是零开销承诺。
- 常用在线查询 P95 不超过 3 秒，范围和并发纳入验收。
- 恢复消费速度高于事件到达速度，在 Kafka 保留窗口内追上积压。

### 11.3 必测场景

1. 固定旧版 Envoy 中 EnvoyFilter 加载 Wasm，验证 fail_open、初始化失败行为及 SDK/ABI。
2. 业务路由仍为 Envoy → A，Observer 不承担业务转发；插件不修改内容、重试或取消。
3. 正文/头回调始终 Continue；慢 Observer、断网及 Kafka 故障不引入业务同步等待。
4. 有界在途配额达到上限时丢弃/取消旁路，网关内存不持续增长，底层缓冲按期释放。
5. root context 持有异步调用的有效 context 切换、终态上报、请求销毁及插件重启行为。
6. SSE/JSON/UTF-8 跨片段、缺段、重复观察消息、多候选、工具调用和长文本。
7. 原始 usage 零值、缺失、非法值与缓存/推理未知；超限结果不伪装完整。
8. 正常观察产生开始和合并结果两类 Kafka 事件；结果事件同时携带用量和终态，只有它累计 token。
9. 隐藏上游重试不伪造成完整逐尝试计量，不重复收费候选。
10. 观察流缺少 End 时，结果事件标 terminal_observed=false、结束时间为空；定稿后不补算。
11. 事件重复/重放不重复累计，同 ID 不同内容隔离；跨天按网关观察开始归属。
12. 独立基数失效时不输出虚假完整率；真实流量两倍峰值和观察故障压测。
13. 日/月汇总重放不增量，冲突撤销旧分组，月合计等于已发布日合计；跨日跨月、NULL/有效零值、发布故障和一年后汇总查询通过。

## 12. 实施顺序与待验证项

以下为原范围的任务划分，不是当前待办清单。Consumer 及采集本地链路已完成；真实 Auth、估算、汇总 Worker 和门户仍待各自工作包，生产 G0–G5 另行验收。新归属接口的实施范围以新计划为准，不要求先完成真实第三方 HTTP/鉴权协议。

1. 固定版本鉴权 PoC：已有 Auth + Istio CUSTOM/ext_authz，验证访问判定、401/403/503、Bearer 读取顺序和 fail-closed。
2. 固定版本采集 PoC：EnvoyFilter + Observer HTTP 下载 Wasm（SHA-256 固定），验证过滤器顺序、usage 参数改写、通用异步 gRPC 和 fail_open 的真实边界。
3. 验证 root 持有旁路流、最终消息生命周期、有界流控与取消；Observer 故障时先证明业务不会等待统计。
4. 打通一个 JSON 与一个 SSE 服务：Bearer/Host 及占位状态、片段解析、一次定稿、两事件与 Kafka。
5. 接入 tokenizer、资源限制、消费者幂等、日/月汇总及独立网关/鉴权指标对照。
6. 按少量网关和业务路由灰度，测鉴权、CPU/内存、首包与流式延迟后扩展。

已于 2026-10-15 在本地 kind Kubernetes 1.22.17 / Istio 1.13.5 验证 Observer 两副本、原始网关镜像的 HTTP Wasm 下载、真实 XDS 过滤器顺序、强制 mTLS 下的 gRPC 及合成 JSON/SSE 入库，证据见[验证记录](spec/plan/gateway-collection-verification.md)。私有镜像差异、真实 Auth、生产网络/负载、长期流控及完整 G0–G5 仍需验证。不得直接复制未经实际版本验证的新版本 Istio/Envoy 字段。

网关单点观察不能完整测量隐藏重试；如业务仍要求 A 的逐尝试用量，需另确认 A 侧已有 Envoy 的观察位置及关联信息，不擅自改变业务链路实现该目标。

二期计费另建价格与账单能力，并与调整单同阶段交付；余额账本和 quota 属可选扩展，quota 如建设须另设有明确控制职责的同步检查，不能让本观察链路同时承担请求拦截。

## 参考资料

版本判断使用 Istio 1.13.5 固定源码；其他链接用于组件机制说明，不代表选定其当前版本。

- [Istio 1.13.5 访问日志实现][1]
- [Istio 发布依赖][2]、[proxy WORKSPACE][3]、[Envoy VERSION][10]
- [固定扩展编译清单][11]、[固定 ext_proc 协议][12]
- [固定 Wasm 回调与发送实现][13]、[Wasm 失败策略][14]、[Proxy-Wasm ABI][15]、[Istio WasmPlugin 转换][16]
- [Kafka producer 配置][4]、[Kafka 投递语义][7]
- [LiteLLM tokenizer 文档][5]、[流式聚合源码][6]
- [Doris Unique Key][8]、[Doris 异步物化视图][9]

[1]: https://github.com/istio/istio/blob/1.13.5/pilot/pkg/networking/core/v1alpha3/accesslog.go
[2]: https://github.com/istio/istio/blob/1.13.5/istio.deps
[3]: https://github.com/istio/proxy/blob/725ec273d665639aa9f6e0e9014af562a457c2f4/WORKSPACE
[4]: https://kafka.apache.org/43/configuration/producer-configs/
[5]: https://docs.litellm.ai/docs/completion/token_usage
[6]: https://github.com/BerriAI/litellm/blob/main/litellm/litellm_core_utils/streaming_chunk_builder_utils.py
[7]: https://kafka.apache.org/41/design/design/
[8]: https://doris.apache.org/docs/4.x/table-design/data-model/unique/
[9]: https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/async-materialized-view/CREATE-ASYNC-MATERIALIZED-VIEW/
[10]: https://github.com/istio/envoy/blob/e2e40d8d667e1b7c752d27980307f22d9f73e9ee/VERSION
[11]: https://github.com/istio/proxy/blob/725ec273d665639aa9f6e0e9014af562a457c2f4/bazel/extension_config/extensions_build_config.bzl
[12]: https://github.com/istio/envoy/blob/e2e40d8d667e1b7c752d27980307f22d9f73e9ee/api/envoy/extensions/filters/http/ext_proc/v3/ext_proc.proto

[13]: https://github.com/istio/envoy/blob/e2e40d8d667e1b7c752d27980307f22d9f73e9ee/source/extensions/common/wasm/context.cc
[14]: https://github.com/istio/envoy/blob/e2e40d8d667e1b7c752d27980307f22d9f73e9ee/api/envoy/extensions/wasm/v3/wasm.proto
[15]: https://github.com/proxy-wasm/proxy-wasm-cpp-host/blob/2be7a2800770cceb40c7a22cba342fde59d21e5e/src/exports.cc
[16]: https://github.com/istio/istio/blob/1.13.5/pilot/pkg/model/extensions.go
[17]: https://developers.openai.com/api/reference/resources/chat
