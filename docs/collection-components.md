# 网关采集组件运行说明

更新日期：2026-10-15。[实现计划](spec/plan/gateway-collection-implementation.md)已落地到三个可构建组件。业务仍直接经过网关到模型服务；Wasm 旁路发送有界片段，Observer 解析后使用原生 Producer 写 Kafka，Consumer 确认 Doris 可见性后提交 offset。仅产生 `request_started`、`usage_observed`，未知值保留 null/unknown。

本轮命令、故障处理及资源归零证据见[本地验证记录](spec/plan/gateway-collection-verification.md)，实际镜像 digest 见[镜像记录](../deploy/local/images.lock.json)。

当前已删除 application_id。Wasm 只从 Authorization: Bearer 提取临时 api_key，并发送入口 authority；Observer 使用配置的 cluster_name/deployment_environment，分别通过服务和客户查询接口取得归属。两个接口目前只有未实现占位，合法输入产生 lookup_not_implemented，service_id/provider_id/api_key_id/customer_id 及映射版本为 null，identity_status=invalid、usage_eligible=false，原始 usage 保留。URI 可留空，填写也不会发起 HTTP；不访问 PG、不读身份元数据补值，密钥不进入事件、数据库或日志。 [归属接口实现计划](spec/plan/host-service-provider-implementation.md)定义后续真实 HTTP 对接边界。

## 构建与本地运行

需要 Go 1.25.0、TinyGo 0.40.1、Python 3.10+、Docker Compose。插件使用独立模块及仓库内 SDK v0.24.0 扩展副本，依赖由 go.mod/go.sum 锁定。

已安装 Kubernetes 1.22.17 / Istio 1.13.5 的 kind 环境可执行 `make local-infra-up k8s-test`：仅复用 Compose Kafka / Doris，Observer、Consumer、fixture 和测试网关均在 Kubernetes 中运行，使用独立 topic / 消费组验证 JSON、SSE、unknown 和真实消费提交。`make k8s-clean` 或 `python3 scripts/cleanup_k8s_collection.py` 清理测试命名空间及本次 topic / 消费组，保留共享基础设施和 Doris 历史测试记录。前置条件、断言和参数见[本地 Kubernetes 测试用例](kubernetes-collection-test.md)。

```sh
make test build wasm
make local-up
make local-test
make manifest
# 停止本次联调栈，保留容器数据：
python3 scripts/local_collection.py stop
```

产物在 `dist/`：`observer`、`consumer`、`fixture`、`gateway-observer-v1.wasm` 和摘要清单。CI 运行契约检查、Go 竞态测试、服务构建及固定 TinyGo 镜像构建/ABI 检查；真实集成测试需单独启动 Docker 栈。

镜像版本使用 `python3 scripts/image_tag.py` 解析：当前 HEAD 有 Git tag 时取该 tag，无 tag 时取至少 12 位 commit 短 hash；多个 tag 按 Git 版本排序取最高项，历史 tag 不沿用，不兼容 Docker tag 的名称会报错。README 的服务镜像构建与 `make k8s-test` 使用同一规则；Kubernetes 测试的 topic / 消费组仍通过随机 run-id 隔离。

本地栈的已发布端口统一绑定 `0.0.0.0`，可使用服务器 IP 从远端访问：

| 组件 | 宿主机端口 |
| --- | --- |
| 网关 / Envoy admin | 18080 / 19000 |
| Kafka 客户端 | 29092 |
| Doris SQL（MySQL 协议）/ FE HTTP / BE HTTP | 19030 / 18030 / 18040 |
| Observer gRPC / 健康、指标 / Wasm 下载 | 19090 / 19091 / 19093 |
| Consumer 健康、指标 | 19092 |
| 模型 fixture HTTP / 合成 ext_authz gRPC | 18081 / 19002 |

DBeaver 使用 MySQL 驱动连接 `服务器IP:19030`，数据库 `metering_development`、用户 `root`、密码为空。Kafka 的监听地址为 `0.0.0.0`，但 `advertised.listeners` 必须公布客户端可达的实际 IP 或域名；`make local-up` 默认识别服务器 IPv4 地址并保存到已忽略的 `deploy/local/.env`，也可先设置 `KAFKA_EXTERNAL_HOST`。多网卡或 NAT 环境使用客户端实际可达地址：

```sh
KAFKA_EXTERNAL_HOST=192.168.227.128 make local-up
# 持久设置时，在 deploy/local/.env 中配置 KAFKA_EXTERNAL_HOST=服务器IP或域名。
```

Kafka 容器内客户端仍使用 `kafka:9092`。重建容器时 Kafka 日志及 Doris FE/BE 数据由命名卷保留；首次从旧版无持久卷配置启动时，脚本先停栈，将原数据备份到 `.tmp/local-storage-*` 并迁移到卷，再重建容器。已有不匹配的目标卷会使迁移停止，避免覆盖或合并历史。不要使用 `docker compose down -v` 删除数据卷。

`make local-up` 在 `dist/envoy.remote.json` 渲染实际 Wasm 摘要，重启 Fixture/Observer/Consumer，核对下载内容后重建网关。网关只挂载配置，插件来自 Observer HTTP 下载。更新本地产物后运行 `make build wasm local-up`，再运行 `GOFLAGS='-count=1' make local-test`，不能仅重启旧网关而沿用旧摘要。脚本只初始化该 Compose 栈，应用 001/002/004/005/006 migration，在独立 metering_development 库执行新初始化，并创建 24 分区、72 小时保留、delete 策略的 llm-metering-development topic。明文 Kafka、合成鉴权、副本数 1 与 Doris root 空密码仅用于开发联调；生产按原方案配置认证、副本、min ISR 和最小权限账号。

普通 JSON / SSE 合成请求：

```sh
curl -N -H 'Host: model.example.invalid' -H 'Authorization: Bearer synthetic-test-key' -H 'Content-Type: application/json' \
  --data '{"model":"fixture","stream":true,"messages":[{"role":"user","content":"synthetic fixture"}]}' \
  http://127.0.0.1:18080/v1/chat/completions
curl http://127.0.0.1:19091/metrics
curl http://127.0.0.1:19092/readyz
```

fixture 只模拟 ext_authz 访问允许与模型 usage，不实现真实密钥鉴权或模型调用。集成测试写入合成事件及隔离的未知版本测试 topic/group；这些诊断事实保留在本地库，测试不会清理已有业务数据。all-in-one Doris 的 BE 注册地址是回环地址，因此本地 Consumer 直连 `doris:8040`；生产 FE→BE 重定向需登记全部实际 FE/BE 地址。

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
- 当前默认规则为 `route_name: "*"`，忽略路由名，只采集 `POST /v1/chat/completions`；路由属性或 metadata 缺失也可命中。路径去掉查询串后精确匹配，其它方法、尾斜杠、子路径和不同路径均忽略。精确路由规则仍支持：先读取可信 `route_name`，缺失时读取 `route_metadata.filter_metadata.metering.route_name`，都不可读时该精确规则不命中。规则按配置顺序选择，不读取客户端提供的路由或身份头。
服务及供应商由 Observer 的服务查询接口解析，客户/API Key ID 由客户接口解析，当前均为占位状态。route_name 仅诊断，不参与身份摘要；旧 accepted_routes、身份元数据配置及规则 service_id 已删除。同一事件不同内容仍冲突。
- trailers 只产生零字节 EOS BodyChunk，不上传其内容。End 校验两个方向的片序、偏移、字节总数及 EOS；HTTP context 删除后只由 Root+token 处理迟到 ACK，不能延长业务生命周期。
- 上游重复伪造的计量响应头先全部移除；Start ACK 已到才加入一个 Observer UUID。快响应允许头缺失；有头也不保证事件已入 Kafka/Doris。
- 固定 franz-go 的 `RecordRetries(0)` 阻止首次发送，实际使用 1 表示仅一次尝试。发送超时/重试次数耗尽保守计为 uncertain，不补发。3 秒 watchdog 关闭超龄 Producer，额外 1 秒内不能释放则退出进程。

## 服务配置与部署

入口为 `observer --config PATH`、`consumer --config PATH`。服务配置严格拒绝未知字段；Observer/Consumer 分别读取自己接受的 config_version 和 service 注册。本次采用唯一新契约并初始化隔离库；正常同契约重放仍需保留对应 config/service 注册，禁止混入旧格式消息。

部署模板在 [Observer](../deploy/observer/observer.yaml)、[Consumer](../deploy/consumer/consumer.yaml)、[EnvoyFilter](../deploy/gateway/envoyfilter.yaml)。先 `make wasm`，再构建 Observer/Consumer Dockerfile 镜像，替换 image digest，建立 metering namespace、对应 ConfigMap/Secret，再使用模板。生产配置示例在 `configs/*.production.example.json`，其中地址是待核对的样例，不代表已部署。Kafka 支持挂载 CA/客户端证书与 SASL PLAIN 环境变量；`kafka.topic` 支持合法 Kafka 名称，Observer 与 Consumer 必须配置一致，生产约定使用 `llm-metering-events`，隔离测试使用专属 topic。Doris DSN、HTTP 用户及密码从 Secret 环境变量读取。DSN 必须显式配置连接、读写超时、UTC 会话；禁止将密码写入配置或提交仓库。需要受控网络与网格 mTLS，模板没有代替真实认证、网络策略或 G0 验收。

EnvoyFilter 模板不可原样 apply；必须校验渲染后的 JSON。按一个实际网关 Pod 名称和 UID 渲染，默认关闭：

```sh
export GATEWAY_POD=替换为实际网关Pod名称
export GATEWAY_POD_UID="$(kubectl -n istio-system get pod "$GATEWAY_POD" -o jsonpath='{.metadata.uid}')"
python3 scripts/render_gateway_filter.py \
  --gateway-pod-name "$GATEWAY_POD" \
  --gateway-namespace istio-system \
  --gateway-pod-uid "$GATEWAY_POD_UID" \
  --wasm-file dist/gateway-observer-v1.wasm \
  --wasm-url http://metering-observer.metering.svc.cluster.local:9093/wasm/gateway-observer-v1.wasm \
  --wasm-cluster 'outbound|9093||metering-observer.metering.svc.cluster.local' \
  --output dist/envoyfilter.disabled.yaml
# G0/G1 通过后，为该 Pod 使用显式 --enable。
```

渲染器通过代理启动元数据 `NAME/NAMESPACE` 匹配目标 Pod，实际 Pod UID 只作为该 Pod 的 `gateway_instance_id` 和资源名称后缀；每次 Pod 重建都要获取新名称、UID 并重新渲染，不能共享一个实例 ID。Istio 1.13 启动后补加的 Pod 标签未进入代理标签快照，不能依靠补 UID 标签实现匹配。`--gateway-pod-name` 必填，`--gateway-namespace` 默认 `istio-system`。可通过 config_dump 检查启动元数据和实际过滤器，字段机制见 [Istio 1.13 ProxyMatch](https://istio.io/v1.13/docs/reference/config/networking/envoy-filter/#EnvoyFilter-ProxyMatch)。Wasm 使用远程 URL、HTTP 集群和实际文件 SHA-256 加载，不要求网关镜像预置文件；模板插在 router 前，要求 ext_authz 已在它之前，以真实 config_dump 核验。每 Worker 一份 Root，初始 2 Worker；热更新存活代数、Pod 选择及回退需另做灰度验收。路由可信 metadata 由网关路由配置提供，不由这个过滤器自动创建。

Istio 的 `outbound|端口|子集|服务DNS` 是路由集群名，不能原样作为 Go gRPC 服务的 HTTP/2 `:authority`。插件保持 `cluster_name` 不变，自动将这类集群的 authority 设置为 `服务DNS:端口`；普通本地集群名保持原值，不改变集群 TLS/SNI。自定义 ext_authz 若同样使用 Istio 集群名，必须在其 `grpc_service.envoy_grpc.authority` 显式填写有效服务 DNS。

Doris Stream Load 只向 `allowed_load_hosts` 发送凭据，拒绝未登记跳转及 HTTPS 降级到 HTTP。每个 label 的冻结内容摘要参与命名；超时或确认丢失后先查询原事务为 VISIBLE，再核对全部行键。`Label Already Exists` 不是成功判据。

## Observer Wasm 下载服务

Observer 镜像复制 `make wasm` 的 `dist/gateway-observer-v1.wasm` 到 `/usr/share/metering/wasm/gateway-observer-v1.wasm`；`.dockerignore` 仅放行这一产物。打包、摘要渲染与发布清单应使用同一份文件，避免重新编译后继续使用旧摘要。默认生产配置如下：

```json
"wasm_download": {
  "listen": ":9093",
  "file": "/usr/share/metering/wasm/gateway-observer-v1.wasm"
}
```

字段位于 `observer` 中。省略该对象即关闭服务；启用后 `file` 必填，`listen` 缺省为 `:9093`，文件不可读、文件头非法或端口绑定失败会阻止启动。服务启动时打开固定文件，只有重启才切换产物。仅接受 `GET/HEAD /wasm/gateway-observer-v1.wasm`，返回 `application/wasm`、文件 SHA-256 ETag 与 `Cache-Control: no-store`；其他路径为 404、其他方法为 405。最多 8 个并发下载，超限 503；读头超时 3 秒、写超时 30 秒。处理下载不等待 Kafka，也不经过 gRPC 解析预算；指标为 `metering_wasm_download_requests_total`、`metering_wasm_download_busy_total`，没有动态标签。

```sh
# 本地联调；生产可另开终端执行 kubectl -n metering port-forward service/metering-observer 19093:9093
curl --noproxy '*' --fail --output dist/gateway-observer.download.wasm \
  http://127.0.0.1:19093/wasm/gateway-observer-v1.wasm
sha256sum dist/gateway-observer-v1.wasm dist/gateway-observer.download.wasm
```

网关使用原生 `code.remote` 加载，下载超时 5 秒、额外重试 0 次、`fail_open: true`。保留 HTTP_FILTER 直接插入方式，不依赖 Istio agent 的 ECDS 文件重写。下载 HTTP 集群默认是 `outbound|9093||metering-observer.metering.svc.cluster.local`；采集 gRPC 仍使用 9090 的 HTTP/2 集群，不能混用。生产须核对实际集群存在，NetworkPolicy、网格 mTLS 与授权允许相应流量；下载端口默认 ClusterIP 内网访问，Observer 本身不终止 TLS、不提供下载鉴权。首次加载有下载等待，已运行的业务请求不会逐次下载。

仅保留当前镜像的单个文件。先更新所有 Observer 副本并核对下载摘要，再更新网关配置。滚动期间旧摘要可能校验失败，允许暂时缺少采集；已加载实例可继续运行，但旧网关重启后不保证取得旧文件。下载服务恢复后需要触发插件重新安装或重启网关；Pod 重建后仍须按新 UID 渲染。原样重复 apply 同一配置不保证触发下载，不承诺自动恢复。版本回退先恢复对应 Observer 镜像，再恢复其匹配的网关配置。需要立即停采时关闭或删除该 Pod 的采集过滤器；`fail_open` 不覆盖任意初始化、ABI 或资源故障。

## Consumer 恢复与统计边界

每分区顺序执行：冻结记录 → events_raw / ingest_rejections 可见 → 跨在线分区冲突标记可见 → 全部日期通知可见 → 批次凭据可见 → 提交下一 offset。数据库故障暂停对应分区，保留一份冻结批次并有界重试；撤销 owner 后停止新写入/提交，新 owner 从已提交位置重放。相同事件与摘要由 Unique Key 去重；不同变体保留并永久登记冲突。

Consumer 按 Kafka 分区跨多次拉取攒批：达到 `max_batch_rows`（默认 5000）、`max_batch_bytes`（默认 4 MiB），或最早一条记录等待 `flush_interval_ms`（默认 15000 ms，即 15 秒；可配置范围 1–15000 ms）后写入；下一条放不进字节预算时先写已有前缀。字节预算按 Kafka key/value 计量，不是展开后的 Stream Load 正文或进程 RSS 上限。每次拉取最多配置的行数，没有额外的 128 条写入限制；数据库回查仍按 128 条拆分查询。每个已分配分区最多保留一份未满批次或一份失败后的冻结批次，写库并发最多 4 个分区；总内存还包含本次拉取、冻结编码与 Kafka 客户端缓冲，需要按分区数和实际事件大小验收。

即使没有后续消息，未满批次到期也会写入。攒批期间保留 Kafka poll 的 rebalance 保护；一个攒批窗口到期时也会写入其他分区的未满批次，然后允许分区移交，避免持续流量无限推迟 rebalance。写库或巡检耗时可能延长实际可见延迟，15000 ms 是默认攒批等待预算。失败时回退到冻结批次的首 offset、沿用原内容及 label；分区撤销清除未提交缓冲，后续 owner 从 Kafka 已提交位置重放。

新增 [005 migration](../migrations/doris/metering/005_consumer_recovery.sql) 保存消费起点和保留期缺口；这些表及凭据/通知本轮不自动清理。新组持久登记 Kafka earliest；已有组缺少起点时通过 `initial_offsets` 明确接管。多份不兼容起点阻塞分区。已保存的恢复位置落后于 earliest 时登记缺口并暂停；经人工处置后，`gap_resume_offsets` 必须填写该分区当时的 earliest，不能填 latest。恢复新流量不会抹掉缺口或把历史覆盖改为可信。

未知正整数 schema_version 暂停分区且不提交；格式错误及永久非法消息只保存坐标、摘要和固定原因，不持久化原始非法正文。每分钟巡检永久冲突与连续水位：由持久起点、区间凭据和 Kafka committed offset 推导，不使用 MAX(offset) 跨越空洞。存在缺口、缺凭据或巡检失败会使 Consumer `/readyz` 为 503。单次证据/凭据扫描有 100000 行保护，达限即停止确认并计数；生产扩展需验证巡检容量。

本轮没有实现统计/查询 API 或 usage-worker，因此尚无对外有效统计发布。后续 Worker 必须独立核对上述持久证据、通知、Kafka offset 和冲突维护状态并停止不可信发布；不能把 Consumer 健康探针当作持久发布凭据。

固定指标不携带 UUID、身份或正文标签。Wasm 暴露选择/忽略、改写/取消原因与 active_streams/inflight_bytes/inflight_messages/active_rewrites/rewrite_reserved_bytes；Observer 暴露 active_streams/parser_reserved_bytes、定稿与发送结果；Consumer 暴露 batch_committed/batch_failure、partition_paused、audit/watermark 状态。服务均有 `/livez`、`/readyz`、`/metrics`。资源预算是记账上限，尚需加上 Go/VM heap、HTTP2/native buffer 及存活代数，不能等同于 RSS 承诺。

## 验证记录与未完成门槛

本地使用公开 `istio/proxyv2:1.13.5` 中的 Envoy，静态配置运行并通过 Observer 下载 Wasm，过滤器顺序为 ext_authz → Wasm → router；Kafka 3.9.2、Doris 4.1.3。这验证固定 Envoy hostcall 和数据链路。2026-10-15 另已在本地 kind Kubernetes 1.22.17 / Istio 1.13.5 控制面完成独立命名空间部署、XDS 匹配、强制 mTLS 下载及 gRPC 采集入库验证，见[验证记录](spec/plan/gateway-collection-verification.md)和 README 的本地 Kubernetes 操作说明；私有镜像、真实 Auth、生产容量和完整 G0–G5 仍未验收。

2026-10-14 已将所有本地发布端口切换到 `0.0.0.0`，通过服务器 IPv4 地址验证全部 11 个 TCP 端口、健康检查、Doris SQL 和 Kafka 元数据连接。切换前的 108 条原始事件、129 条消费凭据及 24 个 Kafka 分区的消费位置、日志末尾位置均保持一致；数据备份保留在 `.tmp/local-storage-*`。重新执行 `make test build wasm` 与禁用缓存的 `make local-test` 均通过，集成测试实际写入和查询 Doris；再次调用存储迁移检查不会重建已有挂载卷的容器。

已通过契约/文档/表字段一致性检查、25 项 Python 测试（含本地地址配置、迁移覆盖保护和备份失败恢复）、Go 单元和竞态测试、SDK internal/proxytest 测试、TinyGo 编译与 ABI 检查。事件规范编码与摘要同六份 Python golden 对照；标准/手工 Protobuf 互操作、非法帧、逐字节 JSON/SSE 分片、usage 候选冲突、提交屏障故障及撤销 owner 由自动测试覆盖。

真实本地集成覆盖普通 JSON、SSE/响应 trailers、Start ACK 关联、unknown 保留 NULL、非白名单请求、相同事件重放去重、不同变体冲突隔离、日期通知/凭据、未知版本暂停；HTTP1 chunked 中途停顿超过 2 秒、256 KiB 边界/边界加一/1 MiB 请求的上游正文摘要保持原样，超时观察降为 partial/unknown，正常延迟响应中的计量 ID 与事件一致；真实 BE 写入后断开 HTTP 确认，仍确认原 label 与行键；独立 TCP Kafka 黑洞验证发送不阻塞及有界超时/watchdog。测试及失败诊断由 `make local-test` 重现。

G0–G5 仍须独立完成：私有镜像/真实 Auth、HTTP2 慢上传与断开/热更新故障交叉、真实 Pod 崩溃及 rebalance 压测、Kafka 保留期丢失后运维恢复、峰值两倍/24 小时/10 分钟 SSE、首包/明细/查询延迟、灰度回退与旧代释放。已有本地证据不等于这些门槛全部通过。

## 归属配置与隔离库

Observer 必填 cluster_name、deployment_environment，service_lookup/customer_lookup 各有可空 uri。当前不实现真实 HTTP、鉴权或网络缓存，不要求第三方 Secret。输入非法先记录 invalid_host/invalid_api_key，合法输入的两个占位记录 lookup_not_implemented。两事件复用快照，客户失败使 identity_status=invalid、usage_eligible=false，不清除原始已观察 usage。

Consumer 的 doris.database、DSN、Stream Load 和证据/巡检 SQL 使用同一经过校验的库名。初始化使用 `python3 scripts/render_doris_sql.py --database 数据库 migrations/doris/metering/001_init.sql`，对其余初始化 SQL 同样渲染。本地默认 metering_development，新 Kubernetes 测试每次用 metering_k8s_<run-id>；不修改旧 metering 库及其消息，清理保留测试库作为证据。普通 Docker 联调使用合成 token 和隔离明文网络；生产/真实密钥只允许已验证的网格 mTLS/TLS。
