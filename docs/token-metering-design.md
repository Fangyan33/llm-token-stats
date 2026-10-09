# LLM 请求用量计量与统计设计

状态：评审稿。目标环境为 **Istio 1.13.5、Kubernetes 1.22**。本文不代表已部署或已通过性能验收。

## 1. 目标与已确认边界

首期建设用量明细与统计能力，为后续 token 计费提供数据，不执行扣款或 quota。

| 项目 | 决策 |
| --- | --- |
| 规模与环境 | 日均约 50,000,000 次调用；Istio 1.13.5；Kubernetes 1.22 |
| 归属主体 | 稳定的 api_key_id；密钥轮换不改变归属，撤销不删除历史 |
| 身份来源 | 新建 API Key Auth 服务，以 PostgreSQL 为事实源，经 Envoy ext_authz 返回可信身份元数据 |
| 服务范围 | 只统计入口直接访问的服务 A，不追踪 A 内部调用的 B、C |
| 调用边界 | 首期网关观察逻辑请求及返回的 A 用量；隐藏重试不伪造成已测量执行，额外消耗不计给用户 |
| 客户端重试 | 每次到达平台的请求分别记录；首期不提供通用客户端幂等，不按正文去重 |
| 接口范围 | 兼容 Chat Completions 的 JSON / SSE，包括工具调用内容 |
| 暂不覆盖 | Embedding、Rerank、多模态及未适配协议的 token 计量；可记录请求元数据 |
| 用量来源 | 有效的服务 usage 优先，缺失字段分别估算或标记未知 |
| 用量定稿 | 每个可见请求形成一份计量结果，不提供历史补查、补报或事后变更 |
| 故障策略 | 业务不等待观察者或 Kafka，旁路拥塞/失败就放弃采集；未知不等于零 |
| 采集侧 | 不设用于故障恢复的事件缓冲、落盘、积压和补传队列 |
| Kafka | 事件保留 72 小时，仅供已入队事件的消费者恢复与重放 |
| 正文 | 仅在处理过程中临时使用，不持久化 |
| 时间归属 | 归属网关 Wasm 观察到请求头的开始时间；存储和默认报表使用 UTC |
| 数据保留 | 在线明细 30 天，归档明细 1 年，小时/日汇总 2 年 |

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

首期不增加 ALS 接收器作为必需组件，不采用 stdout → 节点日志文件 → 日志采集器的计量链路，避免引入已排除的落盘与补传。

## 3. 观察者架构：业务不经过统计服务

### 3.1 业务路径保持直达 A

~~~mermaid
flowchart LR
    U[调用方] --> X[ingressgateway: ext_authz]
    X -. gRPC Check .-> H[API Key Auth]
    H --> P[(PostgreSQL)]
    X -->|允许 + 身份元数据| W[ingressgateway: Wasm 观察插件]
    W --> G[ingressgateway: router]
    G --> A[服务 A]
    W -. 异步 gRPC，失败丢弃 .-> O[独立观察者服务]
    O -->|Kafka producer| K[Kafka：72 小时]
    K --> C[去重与归一化消费者]
    C --> D[ClickHouse 明细与汇总]
    K --> S[对象存储归档]
~~~

**业务：调用方 → ingressgateway ext_authz → 网关内 Wasm/router → A。**Auth 是新增同步鉴权调用，只接收请求头属性、不代理业务正文；Wasm 位于既有 Envoy 进程内，独立观察者服务不在业务路由中。

**采集：Wasm 回调 → 异步 gRPC 观察流 → Observer 解析和估算 → Kafka producer → Kafka。**

不把 VirtualService 的业务目的地改为 Observer，不为统计修改上游重试、取消或超时策略，也不将镜像请求发送给模型再推理一次。鉴权结果决定请求是否进入业务路由；计量统计结果不参与鉴权、路由或响应生成。

### 3.2 Wasm 负责轻量观察，Observer 负责计量

**入口过滤为强制约束**：默认不采集；必须同时满足全局启用、可信路由规则显式启用、HTTP 方法和精确路径匹配。首批只采集指定路由的 `POST /v1/chat/completions`，自定义前缀须显式登记。插件按请求头阶段可见路径去掉查询串后精确匹配，不额外解码或模糊匹配；路径重写顺序须实测固定。规则和配置版本在请求内不变。

未命中的指标 GET、健康检查、模型列表、OPTIONS 等请求不生成计量 ID、不读取或改写正文、不申请观察额度、不建立旁路流、不添加计量响应头、不产生 Kafka 计量事件，仅增加本地固定原因计数。采集开关不控制鉴权，也不能由客户端 Header 控制。命中的失败响应和断流仍尝试记录可观察事件，未知 usage 不补零。完整率的独立分母和 Kafka 容量基数必须限定为同路由、方法、路径及观察位置的目标请求，不能用全网关流量。配置、回调和验收以[采集开发方案第 2.3 节](gateway-request-collection-implementation.md#23-入口采集过滤强制约束)为准。

| 位置 | 职责 |
| --- | --- |
| ext_authz/Auth | 验证平台 API Key、状态、有效期和访问范围，返回客户/应用可信动态元数据 |
| Wasm 请求头回调 | 先执行路由启用、方法和路径联合白名单；命中后从 ext_authz 元数据取得可信 API Key/客户/应用，生成 logical_request_id，准备请求改写和旁路观察流 |
| Wasm 请求正文回调 | 对目标流式 JSON 有界缓冲并设置 `stream_options.include_usage=true`，再观察最终转发正文 |
| Wasm 响应头回调 | 向白名单请求返回 `X-Metering-Request-Id`，便于客户按计量请求 ID 查询；不以此承诺已采集成功 |
| Wasm 响应正文回调 | 有界复制当前片段，附请求 ID、方向及序号，异步提交；立即返回 Continue |
| Wasm 结束回调 | 报告可观察的终态与最后序号；不等待观察者响应，不延长业务流生命周期 |
| Observer | 接收片段、解析 JSON/SSE、检查缺段、提取 usage、执行有界 token 估算 |
| Observer 内 producer | 只把结构化计量事件写入 Kafka；不把正文片段写入 Kafka |
| 消费者 | 去重、归一化、落库、统计及归档 |

Wasm 不做重型 tokenizer 运算，不缓存完整长响应。除对目标流式请求设置 `stream_options.include_usage=true`、添加计量查询 ID 响应头外，不修改其他业务字段或响应正文，也不发送本地错误来代替业务响应。Observer 不承担转发业务、代替模型生成、管理重试或传播取消。

请求头白名单排除 Authorization、Cookie 和密钥原文；`api_key_id/customer_id/application_id` 必须来自 `envoy.filters.http.ext_authz` 动态元数据。鉴权成功但元数据不全时标记身份无效并报警，不接受客户端伪造头。Auth、PostgreSQL、密钥生命周期和元数据契约见 [API Key Auth 服务开发实现方案](api-key-auth-service-implementation.md)。

首期插件只作用于需统计的业务路由。Observer 的上报路由必须排除，避免插件观察自己的上报而形成递归。

### 3.3 异步 gRPC 不是新版 ext_proc

这里使用 Wasm host 提供的通用 gRPC 异步能力，自定义观察消息协议；不是配置旧版尚未实现的 ext_proc.async_mode。固定源码中的 grpcSend 提交消息后返回，不等待服务端确认，但不代表数据已送达或内部没有缓冲。[固定 Wasm 实现][13]

建议按请求建立逻辑 gRPC 观察流，底层 HTTP/2 连接复用；不要为每个 SSE/token 片段创建一次 HTTP 请求。消息包含开始、正文片段和结束标记，这些是临时观察消息，不是 Kafka 的三类业务事件。

必须定义以下流控：

- 每请求/每实例的在途消息数、字节数和观察流数量有上限。
- Observer 的确认仅用于释放发送额度，不是业务继续的条件，也不是 Kafka 持久化确认。
- 达到上限、连接失败或确认超时，取消对应旁路流，丢弃尚未采集的内容；业务回调仍返回 Continue。
- 不把 gRPC 底层缓冲当作无限队列；需验证取消后内存释放，以及慢接收端不会造成网关资源持续增长。
- 不重传丢失的片段，不落盘，不在恢复后补发。片段缺失不能伪装成完整计量。

正文只经过有限内存和旁路网络，Observer 完成定稿或放弃后释放；Kafka、ClickHouse 和归档只保存计量字段。

### 3.4 请求结束后的上下文生命周期

固定 Proxy-Wasm ABI 的 http_call、grpc_stream 和 grpc_send 会经 root_context() 调用，标准异步 hostcall 由 root 持有，不直接随单个业务请求 context 销毁。因此在请求结束时提交异步终态有底层依据，不需要等待统计确认来延长业务请求。[固定 ABI 实现][15]

Root/VM 销毁仍会取消其调用；SDK 回调也不能访问已失效的业务请求对象。实现需按采集请求 ID 保存有界上报状态，验证 SDK 回调映射、请求结束、插件重载及 root 生命周期。异步提交不代表可靠送达，不能宣称在 onLog 调用发送后最终计量必达。[固定 Wasm 实现][13]

这是上线前必须通过的 PoC 项，不是本文已完成的实现。结束消息丢失时，Observer 只能记录已观察的部分用量或缺失状态，不能制造正常完成状态。Observer 内可以有有限的活跃请求解析状态，但不能形成故障恢复队列。

### 3.5 失败放行与资源影响

通过 EnvoyFilter 显式配置 Wasm 插件的 fail_open=true，并要求所有正常请求/响应回调返回 Continue，不返回 StopIteration 或等待统计结果。[固定 Wasm 配置][14]

fail_open 不能覆盖所有安装错误：该版本说明 on_start/on_configure 失败可能导致 xDS 配置被拒绝，初始启动时也可能无法启动代理。因此不把“插件失败放行”宣传为任意发布或资源故障都无风险。

优先使用随网关镜像/本地文件预置的固定版本 Wasm，避免运行期间依赖远端下载；仍需验证 ABI、配置和初始化，通过单独测试网关及少量业务路由灰度。Istio 1.13.5 的 WasmPlugin 转换代码未给 PluginConfig 设置 FailOpen，因此选择 EnvoyFilter 显式配置，不照搬新版 WasmPlugin 的 failStrategy 字段。[固定 Istio 转换实现][16]

“不影响业务”在本方案中的可验收含义是：不新增业务转发节点、不修改业务内容/路由、不等待 Observer/Kafka、不因旁路故障主动拒绝请求。Wasm 回调本身在 Envoy 工作线程上运行，复制、序列化及发送会使用 CPU/内存，不能承诺零时延或零资源风险。必须压测并设置硬上限。

### 3.6 重试可见性不能靠旁路弥补

单独放在网关 downstream 过滤器链中的插件，能看到逻辑请求及最终返回给客户端的响应，通常看不到被网关透明重试吞掉的各次失败响应。因此不再由统计系统接管重试，也不能继续承诺每次重试都能得到 token。

网关采集结果标记 observation_scope=gateway_response；未知重试消耗不计给用户，也不记成零。能取得的尝试总数只作为元数据，不能代替各次执行 usage。

若必须保留对 A 的逐尝试计量，需要在 A 已有 inbound Envoy 等可见每次尝试的位置再部署只读观察插件，并验证现有请求关联信息。这里仍只观察 A，不展开 B、C；不能假设所有服务已有 sidecar，或未核查关联能力就宣称逐尝试已覆盖。该扩展是独立接入项，不是首期网关插件已经具备的能力。

### 3.7 Observer 到 Kafka

Observer 以 Kubernetes 1.22 支持的普通 Deployment / Service 部署，用 Kafka 原生 producer 写 topic。配置 TLS/SASL、写权限，并确保能访问 bootstrap 及 broker 返回的 advertised.listeners 地址。[Kafka producer 配置][4]

Observer/Kafka 不可用只丢计量，不改变业务流。业务没有新代理一跳，资源仍需为旁路正文吞吐、活跃解析状态和 tokenizer 计算单独测算。

## 4. 一次请求为什么可能产生三条记录

“记录”指 Kafka 原始事件，不是 gRPC 正文片段，也不是三次调用。Observer 根据收到的观察信息生成事件；Wasm 不直接写 Kafka。

| 事件 | 产生条件 | 内容 | 是否携带 token |
| --- | --- | --- | --- |
| request_started | 收到有效开始消息 | 采集请求 ID、API Key ID、服务、网关观察开始时间 | 否 |
| usage_observed | 观察结束或确定无法继续后完成一次定稿 | 可见 usage/估算/未知、观察范围与完整性 | **是，唯一携带本次观察用量的事件** |
| request_finished | 收到明确请求终态 | 所观察状态、结束时间、已知尝试数 | 否 |

原执行事件名 attempt_observed 改为 usage_observed，避免在只观察网关最终响应时误称已得到每次上游执行。

### 4.1 正常请求示例

~~~text
logical_request_id = R1
1. request_started   event_id=R1:start
2. usage_observed    event_id=R1:usage   input_tokens=1000 output_tokens=500
3. request_finished  event_id=R1:finish
~~~

结果为 1 次逻辑请求、3 条事件；输入 1000、输出 500，token 不计算三遍。正常路径每请求三个事件，不为每个 SSE/token 片段写 Kafka。

三类事件依赖同一采集路径，不能作为相互独立的完整率基数。

### 4.2 断流、拒绝和重试

- 没收到开始消息，可能整次调用完全没有记录。
- 收到开始但观察流丢失，只能保留开始及可能的部分/未知用量，不伪造 request_finished。
- 截断、丢片或采集超限时，usage_observed 标明部分或未知，不补查、不补报。
- 仅有网关观察时，即使网关重试，也不能按 N+2 推断有 N 条执行结果；隐藏尝试没有可见用量。
- 只有未来部署并验证 A 侧逐尝试观察，才可另定义开始 + N 次尝试 + 结束的事件模型及容量，且要避免将网关汇总与 A 侧消耗相加。

下游关联成请求视图后，事件数不等于请求行数。

### 4.3 一次定稿

Observer 在正文仍可见时完成计量，既有有效上报优先；缺失则估算或未知。活跃观察片段允许在有限时间内组装，但不排队等待故障恢复。

三类事件按 ID 关联，不依赖先后入库顺序。每个请求观察结果定稿后不被后来片段或服务查询结果替换；超时关闭的观察也不重新开启补算。request_finished 描述网关插件所观察的结果，不证明客户端已收到全部字节。

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
| 事件身份 | event_id、event_type、schema_version、producer_id |
| 请求/观察 | logical_request_id、observation_id、observation_scope、service_id、api_key_id |
| 模型 | 请求模型、实际模型、模型版本 |
| 时间 | 网关观察开始、观察结束、可取得的请求结束、事件生成、入库时间 |
| 结果 | HTTP 状态、观察结果、可取得的尝试数、缺片/终态缺失及可信程度 |
| 用量 | 输入、输出、缓存读/写、推理、服务总量；仅用量观察事件携带，可空 |
| 依据 | 字段来源、完整性、tokenizer/模板/估算器版本、原始 usage |
| 归属准备 | 用户正常消耗、平台重试/故障消耗、待核对；首期不扣款 |
| 扩展 | measurement_unit、多模态扩展项及平台元数据引用 |

每个观察结果不可变，不设置业务用量修订号、历史结果替换接口或后续补查任务。schema_version 和估算器版本只解释原记录结构及算法。

### 7.2 重放仍需去重

消费者落库成功但提交消费位置失败后，可能重复读取事件。重放保持 event_id 和原内容不变，不能因为去掉事后变更功能就去掉幂等处理。

首期使用一个原始事件 topic，例如 llm-metering-events，按 logical_request_id 选择分区；数据库和归档分别消费。三类事件各自去重，token 只累计用量观察结果。

同一 ID、相同内容视为重复；同一 ID、不同内容为冲突，隔离报警，不覆盖或重复相加。归一化结果也应在重放时确定一致。

Kafka 事务或 producer 去重不自动保证外部 ClickHouse 写入恰好一次，落库与消费进度必须配合验证。[Kafka 投递语义][7]

### 7.3 ClickHouse 与汇总

在线以类型化列保存不可变事件，保留规范事件 JSON 供冲突核验；请求/观察视图基于类型化列关联，不在常规查询中反复解析 JSON，保留 30 天。物理写入可能重复，查询必须按事件 ID 与内容摘要去重，并隔离同一事件 ID 的不同内容，不能仅等待后台合并。

如用 ReplacingMergeTree 消除相同事件重复，排序键需包含内容摘要以保留冲突变体，查询使用 FINAL 或等价去重；事件 ID 是逻辑键，不是 ClickHouse 的即时唯一约束。这里用于幂等，不承载多版本业务用量。分区、分片和排序键应保证同一事件的位置一致，类型化写入成本及查询性能都需要压测。[ClickHouse 去重机制][8]

小时统计从去重后的用量观察结果生成，日统计由有效小时统计生成；周期刷新受新到达事件影响的桶，查询只读完整发布的汇总批次。逻辑请求按请求 ID 计一次，不能把三条事件计为三次调用。

不将可能重复的原始插入直接接到求和物化视图；源表后台去重不会自动抵消已累加的重复值。首期按去重明细生成汇总，刷新周期以 60 秒可见目标为约束。[物化视图机制][9]

三个口径分开：

- 逻辑请求量：每次请求一次。
- 服务可见用量：网关最终响应的上报/估算用量；明确不包括不可见的重试消耗，不能声称完整后端计算量。
- 用户应收候选：排除平台重试额外消耗和平台故障应承担部分；不生成实际账单。

用户取消只将已确认消耗列为应收候选，原因或用量不明时标记待核对。首期“待核对”只是标签，不包含自动补查或处理流程。未来收费默认接受服务上报及已验证模型匹配估算，通用估算不直接进入结算。

## 8. 归档与查询

原始计量事件归档到对象存储，保留 1 年；没有正文，不支持重新分词。归档只能恢复已采集事件。

提供：

- 请求 ID 精确查询及对应服务 A 的观察结果。
- 客户保留响应头 `X-Metering-Request-Id`，以插件生成的 `logical_request_id` 精确查询；可选保存网关可见的 `X-Request-Id` 作辅助关联。后者可能由客户端提供或跨请求复用，须限定客户与时间窗口，不作唯一键或授权凭据。
- 按时间、API Key、服务、模型、结果和计量来源筛选分页。
- 小时/日趋势与服务、API Key 排行。
- 上报、估算、未知、事件缺失分别展示。
- 大范围异步导出；超过在线期的明细异步检索。

显示数据水位、更新时间和异常时段。首次事件因消费延迟较晚入库时，统计正常刷新，但已定稿用量不被后续服务查询结果替换。两年汇总中超过一年的部分没有完整请求明细可下钻，明确展示保留期边界。

客户查询服务从登录身份注入 `customer_id`，只返回该客户且无冲突的请求；无 usage 事件时展示“未观察到用量”，token 保持未知，不显示 0。插件返回了计量 ID 不保证旁路事件已入库；暂未查到与采集丢失不能仅凭查询结果区分。具体字段和 SQL 见 [网关请求采集开发实现方案](gateway-request-collection-implementation.md)。

## 9. 故障行为

| 场景 | 业务行为 | 采集行为 |
| --- | --- | --- |
| API Key 无效、吊销或越权 | ext_authz 返回 401/403，不进入 A | 不进入 Wasm 采集；使用独立鉴权指标统计 |
| Auth/PostgreSQL 超时或不可用 | fail-closed 返回 503 | 不进入采集，不把鉴权故障算成采集丢失 |
| 鉴权成功但身份元数据缺失 | 按 ext_authz 结果继续 | 标身份无效并报警，不从客户端头补齐 |
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

日均 50,000,000 次约为平均 579 请求/秒。网关插件完整观察一次正常调用，Observer 产生开始、用量、结束三条 Kafka 事件：

~~~text
每日 Kafka 原始事件量 ≈ 50,000,000 × 3 = 150,000,000 条
平均 Kafka 事件速率 ≈ 1,736 条/秒
~~~

这里不包含 Wasm → Observer 的 gRPC 正文片段。旁路消息量由正文分片决定，可能远高于 Kafka 事件量，必须单独测算；不能拿 1,736 条/秒估计旁路解析链路能力。

按应采集量配容量，不扣除预计漏采量。开始/结束通常比用量事件小。网关不可见的重试不能假设已生成额外的执行记录。

### 10.2 字节量示例

使用十进制 KB/GB/TB，假设平均每条原始事件 1 KB，尚未测量：

| 项目 | 基线数量级 |
| --- | --- |
| 无重试请求 | 3 条事件，约 3 KB |
| 原始事件每日量 | 约 150 GB |
| Kafka 72 小时 | 约 450 GB 逻辑数据；3 副本约 1.35 TB，另加运行余量 |
| 原始事件在线 30 天 | 约 4.5 TB，未计压缩、副本、索引及未来增加的采集事件 |
| 原始事件归档 365 天 | 约 54.75 TB，未计压缩及未来增加的采集事件 |

若额外物化每请求一条约 1 KB 的明细，则另有约 50 GB/日、1.5 TB/30 天数据，不能与三事件存储混为同一口径；是否额外物化由查询压测决定。

生产估算分别测量三类事件大小：

~~~text
每日字节量 = 开始事件数 × 开始事件大小
           + 用量事件数 × 用量事件大小
           + 结束事件数 × 结束事件大小
~~~

Kafka 按 72 小时保留并配足磁盘，避免更小的字节上限提前清理。队列副本、ClickHouse 副本/索引、归档压缩和汇总空间分开测算。

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

三类事件均来自同一 Wasm → Observer 采集路径，不能用“已收到结束 / 已收到开始”证明没有漏采。独立基数不可靠时，真实完整率标记未知。

展示网关同口径请求数、已记录逻辑请求数、观察结果覆盖情况、上报/估算/未知比例、发送失败/不确定计数、消费/归档延迟。请求有记录不代表用量已知，计数差也不能还原漏掉的 token。

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
8. 正常观察产生三类 Kafka 事件，只有 usage_observed 累计 token。
9. 隐藏上游重试不伪造成完整逐尝试计量，不重复收费候选。
10. 观察流缺少结束标记时不伪造 request_finished；定稿后不补算。
11. 事件重复/重放不重复累计，同 ID 不同内容隔离；跨天按网关观察开始归属。
12. 独立基数失效时不输出虚假完整率；真实流量两倍峰值和观察故障压测。

## 12. 实施顺序与待验证项

1. 固定版本鉴权 PoC：PostgreSQL + Auth + Istio CUSTOM/ext_authz，验证 API Key、401/403/503、动态元数据和 fail-closed。
2. 固定版本采集 PoC：EnvoyFilter + 本地 Wasm，验证过滤器顺序、usage 参数改写、通用异步 gRPC 和 fail_open 的真实边界。
3. 验证 root 持有旁路流、最终消息生命周期、有界流控与取消；Observer 故障时先证明业务不会等待统计。
4. 打通一个 JSON 与一个 SSE 服务：可信元数据、片段解析、一次定稿、三事件与 Kafka。
5. 接入 tokenizer、资源限制、消费者幂等、归档和独立网关/鉴权指标对照。
6. 按少量网关和业务路由灰度，测鉴权、CPU/内存、首包与流式延迟后扩展。

未连接实际集群。私有镜像差异、Wasm SDK、ext_authz 元数据路径、过滤器顺序、PostgreSQL/Auth 可用性、实际流控、Kafka 网络和负载仍需验证。不得直接复制未经实际版本验证的新版本 Istio/Envoy 字段。

网关单点观察不能完整测量隐藏重试；如业务仍要求 A 的逐尝试用量，需另确认 A 侧已有 Envoy 的观察位置及关联信息，不擅自改变业务链路实现该目标。

未来计费另建价格、账单和余额账本；quota 另设有明确控制职责的同步检查，不能让本观察链路同时承担请求拦截。

## 参考资料

版本判断使用 Istio 1.13.5 固定源码；其他链接用于组件机制说明，不代表选定其当前版本。

- [Istio 1.13.5 访问日志实现][1]
- [Istio 发布依赖][2]、[proxy WORKSPACE][3]、[Envoy VERSION][10]
- [固定扩展编译清单][11]、[固定 ext_proc 协议][12]
- [固定 Wasm 回调与发送实现][13]、[Wasm 失败策略][14]、[Proxy-Wasm ABI][15]、[Istio WasmPlugin 转换][16]
- [Kafka producer 配置][4]、[Kafka 投递语义][7]
- [LiteLLM tokenizer 文档][5]、[流式聚合源码][6]
- [ClickHouse 去重机制][8]、[物化视图机制][9]

[1]: https://github.com/istio/istio/blob/1.13.5/pilot/pkg/networking/core/v1alpha3/accesslog.go
[2]: https://github.com/istio/istio/blob/1.13.5/istio.deps
[3]: https://github.com/istio/proxy/blob/725ec273d665639aa9f6e0e9014af562a457c2f4/WORKSPACE
[4]: https://kafka.apache.org/43/configuration/producer-configs/
[5]: https://docs.litellm.ai/docs/completion/token_usage
[6]: https://github.com/BerriAI/litellm/blob/main/litellm/litellm_core_utils/streaming_chunk_builder_utils.py
[7]: https://kafka.apache.org/41/design/design/
[8]: https://clickhouse.com/docs/concepts/features/operations/update/replacing-merge-tree
[9]: https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view
[10]: https://github.com/istio/envoy/blob/e2e40d8d667e1b7c752d27980307f22d9f73e9ee/VERSION
[11]: https://github.com/istio/proxy/blob/725ec273d665639aa9f6e0e9014af562a457c2f4/bazel/extension_config/extensions_build_config.bzl
[12]: https://github.com/istio/envoy/blob/e2e40d8d667e1b7c752d27980307f22d9f73e9ee/api/envoy/extensions/filters/http/ext_proc/v3/ext_proc.proto

[13]: https://github.com/istio/envoy/blob/e2e40d8d667e1b7c752d27980307f22d9f73e9ee/source/extensions/common/wasm/context.cc
[14]: https://github.com/istio/envoy/blob/e2e40d8d667e1b7c752d27980307f22d9f73e9ee/api/envoy/extensions/wasm/v3/wasm.proto
[15]: https://github.com/proxy-wasm/proxy-wasm-cpp-host/blob/2be7a2800770cceb40c7a22cba342fde59d21e5e/src/exports.cc
[16]: https://github.com/istio/istio/blob/1.13.5/pilot/pkg/model/extensions.go
