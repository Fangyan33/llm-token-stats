# 网关采集组件运行说明

更新日期：2026-10-14。[实现计划](../spec/plan/gateway-collection-implementation.md)已落地到三个可构建组件。业务仍直接经过网关到模型服务；Wasm 旁路发送有界片段，Observer 解析后使用原生 Producer 写 Kafka，Consumer 确认 Doris 可见性后提交 offset。仅产生 `request_started`、`usage_observed`，未知值保留 null/unknown。

本轮命令、故障处理及资源归零证据见[本地验证记录](../spec/plan/gateway-collection-verification.md)，实际镜像 digest 见[镜像记录](../deploy/local/images.lock.json)。

## 构建与本地运行

需要 Go 1.25.0、TinyGo 0.40.1、Python 3.10+、Docker Compose。插件使用独立模块及仓库内 SDK v0.24.0 扩展副本，依赖由 go.mod/go.sum 锁定。

```sh
make test build wasm
make local-up
make local-test
make manifest
# 停止本次联调栈，保留容器数据：
python3 scripts/local_collection.py stop
```

产物在 `dist/`：`observer`、`consumer`、`fixture`、`gateway-observer-v1.wasm` 和摘要清单。CI 运行契约检查、Go 竞态测试、服务构建及固定 TinyGo 镜像构建/ABI 检查；真实集成测试需单独启动 Docker 栈。

本地栈只绑定 127.0.0.1：网关 18080、Envoy admin 19000、Kafka 29092、Doris SQL 19030/FE HTTP 18030/BE HTTP 18040、Observer 健康端口 19091、Consumer 健康端口 19092。`make local-up` 只初始化该 Compose 栈，应用 001/002/004/005 migration，并创建 24 分区、72 小时保留、delete 策略的测试 topic。副本数 1 与 Doris root 空密码仅用于合成联调；生产按原方案配置副本、min ISR 和最小权限账号。

普通 JSON / SSE 合成请求：

```sh
curl -N -H 'Content-Type: application/json' \
  --data '{"model":"fixture","stream":true,"messages":[{"role":"user","content":"synthetic fixture"}]}' \
  http://127.0.0.1:18080/v1/chat/completions
curl http://127.0.0.1:19091/metrics
curl http://127.0.0.1:19092/readyz
```

fixture 只模拟 ext_authz 可信元数据与模型 usage，不实现真实密钥鉴权或模型调用。集成测试写入合成事件及隔离的未知版本测试 topic/group；这些诊断事实保留在本地库，测试不会清理已有业务数据。all-in-one Doris 的 BE 注册地址是回环地址，因此本地 Consumer 直连 `doris:8040`；生产 FE→BE 重定向需登记全部实际 FE/BE 地址。

## 协议生成与版本

Protobuf 工具：protoc 29.3、protoc-gen-go v1.36.5、protoc-gen-go-grpc v1.5.1。生成文件保存标准工具版本标记；运行时 protobuf v1.36.10、gRPC-Go v1.78.0、franz-go v1.18.1，最终版本以 go.mod/go.sum 为准。

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
# 把以上工具目录加入 PATH，安装 protoc 29.3 后：
make generate
```

`observation.proto` 的 go_package 已固定到本项目。Wasm 使用有界手工编码与标准 Protobuf 互操作测试，禁止将 gRPC-Go 编入插件。基底 SDK 导出 ABI marker `proxy_abi_version_0_2_0`，实际正文回调名为 `proxy_on_request_body`；ABI 脚本检查这些真实符号、gRPC 回调/hostcall 及无 socket imports，保留基底 marker。

## 已确认的旧版网关处理

- 请求头 Continue；符合条件的正文在非 EOS 回调 Pause，EOS 时只修改 `stream_options.include_usage`。请求头可先到上游。这是用户在真实旧版 ext_authz PoC 后确认的调整：暂停头会延迟已缓冲正文回调直到改写超时。
- 改写预算与观察预算独立；Observer 不可用仍可补充参数。超限、超时或 JSON 无效时恢复原文，响应正文一直 Continue。旧版真实慢上传 PoC 中，Root 异步 Resume 后尾片可能不再完整经过 Wasm 回调，因此超时恢复会在有界前缀后取消观察，结果保留 partial/unknown，不宣称完整用量。JSON 校验限制深度 64、元素 100000、单对象键 8192；超限取消解析/原文放行。这些额外保护可能使复杂合法 JSON 降级，不能宣称所有请求均能改写。
- 当前 Wasm 配置资源整数还限制在有符号 32 位正值范围，防止 TinyGo wasm32 的长度转换溢出；给出的初值全部满足。预发布仍必须运行 Schema 与关系校验。
- `route_name` 在 router 前可能不可见。插件首先读取该可信属性，缺失时读取 `route_metadata.filter_metadata.metering.route_name`；必须在实际选中 Route 的 metadata 显式登记与配置一致的名称。两者都缺失时不采集，不读取客户端提供的路由或身份头。
- trailers 只产生零字节 EOS BodyChunk，不上传其内容。End 校验两个方向的片序、偏移、字节总数及 EOS；HTTP context 删除后只由 Root+token 处理迟到 ACK，不能延长业务生命周期。
- 上游重复伪造的计量响应头先全部移除；Start ACK 已到才加入一个 Observer UUID。快响应允许头缺失；有头也不保证事件已入 Kafka/Doris。
- 固定 franz-go 的 `RecordRetries(0)` 阻止首次发送，实际使用 1 表示仅一次尝试。发送超时/重试次数耗尽保守计为 uncertain，不补发。3 秒 watchdog 关闭超龄 Producer，额外 1 秒内不能释放则退出进程。

## 服务配置与部署

入口为 `observer --config PATH`、`consumer --config PATH`。服务配置严格拒绝未知字段；Observer/Consumer 分别读取自己接受的 config_version 和 route/service 注册。网关升级前先让 Observer 接受新旧配置，Consumer 保留在线窗口内的历史注册，避免合法重放被误拒收。

部署模板在 [Observer](../deploy/observer/observer.yaml)、[Consumer](../deploy/consumer/consumer.yaml)、[EnvoyFilter](../deploy/gateway/envoyfilter.yaml)。先构建 Dockerfile 镜像，替换 image digest，建立 metering namespace、对应 ConfigMap/Secret，再使用模板。生产配置示例在 `configs/*.production.example.json`，其中地址是待核对的样例，不代表已部署。Kafka 支持挂载 CA/客户端证书与 SASL PLAIN 环境变量；Doris DSN、HTTP 用户及密码从 Secret 环境变量读取。DSN 必须显式配置连接、读写超时、UTC 会话；禁止将密码写入配置或提交仓库。需要受控网络与网格 mTLS，模板没有代替真实认证、网络策略或 G0 验收。

EnvoyFilter 模板不可原样 apply；必须校验渲染后的 JSON。按一个实际网关 Pod UID 渲染，默认关闭：

```sh
python3 scripts/render_gateway_filter.py \
  --gateway-pod-uid 7f6c2a90-4b13-4b68-9d21-67283e0c5f44 \
  --output dist/envoyfilter.disabled.yaml
# G0/G1 通过后，为该 Pod 使用显式 --enable。
```

将目标网关 Pod 的 `metering-instance` 标签设置为其实际 UID，确保每份渲染只选中那个 Pod；每次 Pod 重建重新渲染，不能把同一个实例 ID 共享给整个 Deployment。Wasm 文件须预置只读挂载/镜像路径；模板插在 router 前，要求 ext_authz 已在它之前，以真实 config_dump 核验。每 Worker 一份 Root，初始 2 Worker；热更新存活代数、Pod 选择及回退需另做灰度验收。路由可信 metadata 由网关路由配置提供，不由这个过滤器自动创建。

Doris Stream Load 只向 `allowed_load_hosts` 发送凭据，拒绝未登记跳转及 HTTPS 降级到 HTTP。每个 label 的冻结内容摘要参与命名；超时或确认丢失后先查询原事务为 VISIBLE，再核对全部行键。`Label Already Exists` 不是成功判据。

## Consumer 恢复与统计边界

每分区顺序执行：冻结记录 → events_raw / ingest_rejections 可见 → 跨在线分区冲突标记可见 → 全部日期通知可见 → 批次凭据可见 → 提交下一 offset。数据库故障暂停对应分区，保留一份冻结批次并有界重试；撤销 owner 后停止新写入/提交，新 owner 从已提交位置重放。相同事件与摘要由 Unique Key 去重；不同变体保留并永久登记冲突。

Consumer 按 Kafka 分区跨多次拉取攒批：达到 `max_batch_rows`（默认 5000）、`max_batch_bytes`（默认 4 MiB），或最早一条记录等待 `flush_interval_ms`（默认 15000 ms，即 15 秒；可配置范围 1–15000 ms）后写入；下一条放不进字节预算时先写已有前缀。字节预算按 Kafka key/value 计量，不是展开后的 Stream Load 正文或进程 RSS 上限。每次拉取最多配置的行数，没有额外的 128 条写入限制；数据库回查仍按 128 条拆分查询。每个已分配分区最多保留一份未满批次或一份失败后的冻结批次，写库并发最多 4 个分区；总内存还包含本次拉取、冻结编码与 Kafka 客户端缓冲，需要按分区数和实际事件大小验收。

即使没有后续消息，未满批次到期也会写入。攒批期间保留 Kafka poll 的 rebalance 保护；一个攒批窗口到期时也会写入其他分区的未满批次，然后允许分区移交，避免持续流量无限推迟 rebalance。写库或巡检耗时可能延长实际可见延迟，15000 ms 是默认攒批等待预算。失败时回退到冻结批次的首 offset、沿用原内容及 label；分区撤销清除未提交缓冲，后续 owner 从 Kafka 已提交位置重放。

新增 [005 migration](../migrations/doris/metering/005_consumer_recovery.sql) 保存消费起点和保留期缺口；这些表及凭据/通知本轮不自动清理。新组持久登记 Kafka earliest；已有组缺少起点时通过 `initial_offsets` 明确接管。多份不兼容起点阻塞分区。已保存的恢复位置落后于 earliest 时登记缺口并暂停；经人工处置后，`gap_resume_offsets` 必须填写该分区当时的 earliest，不能填 latest。恢复新流量不会抹掉缺口或把历史覆盖改为可信。

未知正整数 schema_version 暂停分区且不提交；格式错误及永久非法消息只保存坐标、摘要和固定原因，不持久化原始非法正文。每分钟巡检永久冲突与连续水位：由持久起点、区间凭据和 Kafka committed offset 推导，不使用 MAX(offset) 跨越空洞。存在缺口、缺凭据或巡检失败会使 Consumer `/readyz` 为 503。单次证据/凭据扫描有 100000 行保护，达限即停止确认并计数；生产扩展需验证巡检容量。

本轮没有实现统计/查询 API 或 usage-worker，因此尚无对外有效统计发布。后续 Worker 必须独立核对上述持久证据、通知、Kafka offset 和冲突维护状态并停止不可信发布；不能把 Consumer 健康探针当作持久发布凭据。

固定指标不携带 UUID、身份或正文标签。Wasm 暴露选择/忽略、改写/取消原因与 active_streams/inflight_bytes/inflight_messages/active_rewrites/rewrite_reserved_bytes；Observer 暴露 active_streams/parser_reserved_bytes、定稿与发送结果；Consumer 暴露 batch_committed/batch_failure、partition_paused、audit/watermark 状态。服务均有 `/livez`、`/readyz`、`/metrics`。资源预算是记账上限，尚需加上 Go/VM heap、HTTP2/native buffer 及存活代数，不能等同于 RSS 承诺。

## 验证记录与未完成门槛

本地使用公开 `istio/proxyv2:1.13.5` 中的 Envoy，静态配置运行，过滤器顺序为 ext_authz → Wasm → router；Kafka 3.9.2、Doris 4.1.3。这验证固定 Envoy hostcall 和数据链路，尚未验证用户私有镜像、完整 Istio 控制面、Kubernetes 1.22 的真实发布。

已通过契约/文档/表字段一致性检查、20 项 Python 测试、Go 单元和竞态测试、SDK internal/proxytest 测试、TinyGo 编译与 ABI 检查。事件规范编码与摘要同六份 Python golden 对照；标准/手工 Protobuf 互操作、非法帧、逐字节 JSON/SSE 分片、usage 候选冲突、提交屏障故障及撤销 owner 由自动测试覆盖。

真实本地集成覆盖普通 JSON、SSE/响应 trailers、Start ACK 关联、unknown 保留 NULL、非白名单请求、相同事件重放去重、不同变体冲突隔离、日期通知/凭据、未知版本暂停；HTTP1 chunked 中途停顿超过 2 秒、256 KiB 边界/边界加一/1 MiB 请求的上游正文摘要保持原样，超时观察降为 partial/unknown，正常延迟响应中的计量 ID 与事件一致；真实 BE 写入后断开 HTTP 确认，仍确认原 label 与行键；独立 TCP Kafka 黑洞验证发送不阻塞及有界超时/watchdog。测试及失败诊断由 `make local-test` 重现。

G0–G5 仍须独立完成：私有镜像/真实 Auth、HTTP2 慢上传与断开/热更新故障交叉、真实 Pod 崩溃及 rebalance 压测、Kafka 保留期丢失后运维恢复、峰值两倍/24 小时/10 分钟 SSE、首包/明细/查询延迟、灰度回退与旧代释放。已有本地证据不等于这些门槛全部通过。
