# 网关采集组件运行说明

更新日期：2026-11-09。[实现计划](spec/plan/gateway-collection-implementation.md)已落地到三个可构建组件。业务仍直接经过网关到模型服务；Wasm 旁路发送有界片段，Observer 解析后使用原生 Producer 写 Kafka，Consumer 确认 Doris 可见性后提交 offset。仅产生 `request_started`、`usage_observed`，未知值保留 null/unknown。

本轮命令、故障处理及资源归零证据见[本地验证记录](spec/plan/gateway-collection-verification.md)，实际镜像 digest 见[镜像记录](../deploy/local/images.lock.json)。

当前已删除 application_id。Wasm 只从 Authorization: Bearer 提取临时 api_key，并发送入口 authority；Observer 使用配置的 cluster_name/deployment_environment，分别通过服务和客户查询接口取得归属。两个归属接口支持配置 HTTP/HTTPS POST JSON 调用及 x-remote-data 认证；URI 为空时使用未实现占位。Observer 固定 prd/stg/dev 环境，在有界异步任务中取得 ID 和名称快照；service_name、provider_name、customer_name 经 Kafka 写入 Doris 独立列及 event_json。占位或失败侧的 ID、名称和版本为 null，不回源补写历史；原始 usage 保留，业务 token 不进入事件、数据库或日志。 [归属接口实现计划](spec/plan/host-service-provider-implementation.md)定义 HTTP 对接和名称保存边界。

当前请求标识分为：Observer 生成的内部 `observation_id`、Wasm 从 `logical_request_id_headers` 有序列表提取的可空客户端 `logical_request_id`，以及网关可见的 `x_request_id`。候选列表缺省或为空时不采集客户端 ID；最多 8 个头，取首个非空且匹配 `[A-Za-z0-9._:-]{1,128}` 的单值，跳过缺失、非法或重复头，不截断、不补值。配置示例按 `x-client-request-id`、`request_id` 排序。客户端 ID 可重复，内部关联和计数统一使用 `observation_id`；采集插件不再注入 `X-Metering-Request-Id`。Kafka 事件保持唯一的 `schema_version=1` 契约，当前模型直接采用上述字段语义。详情见[采集标识契约](gateway-request-collection-implementation.md#22-请求标识及时间)。

## 构建与本地运行

需要 Go 1.25.0、TinyGo 0.40.1、Python 3.10+、Docker、kind、kubectl、istioctl。插件使用独立模块及仓库内 SDK v0.24.0 扩展副本，依赖由 go.mod/go.sum 锁定。

已安装 Kubernetes 1.22.17 / Istio 1.13.5 的 kind 环境执行 `make k8s-test`：Kafka、Doris、Observer、Consumer、fixture、测试网关及集成测试 Job 全部在隔离命名空间内运行。原有本地 E2E 的下载故障、改写 fail-open、批量/rebalance、写入确认丢失、请求 ID、归属和名称缓存场景均由该入口执行。前置条件和断言见[本地 Kubernetes 测试用例](kubernetes-collection-test.md)。

```sh
make test k8s-test
make manifest
# 删除隔离测试环境及临时数据，保留本地诊断：
make k8s-clean
```

Compose 的 `make local-up` 仅保留手工联调用途，原 `make local-test` 已移除。
产物在 `dist/`：`observer`、`consumer`、`fixture`、`gateway-observer-v1.wasm` 和摘要清单。构建与发布工作流仅在推送 `v*` tag 时触发，普通分支推送与 PR 不触发，也不配置手动启动入口。CI 运行契约检查、Go 竞态测试、服务构建及固定 TinyGo 镜像构建/ABI 检查；E2E 需在准备好的本地 kind 集群另行执行 `make k8s-test`。

CI 中的 TinyGo 容器使用 Runner 的 UID/GID，并将容器内的 HOME 设置为 `/tmp`，使 Git 能读取同一用户拥有的检出目录，构建缓存和 Wasm 输出也可写。`collection` 与 Release 工作流采用相同设置；若出现 `error obtaining VCS status: exit status 128`，可先检查容器与挂载仓库的用户身份是否一致。修复后需推送包含新提交的 `v*` tag 触发构建；重跑旧工作流会继续使用旧提交中的配置。

镜像版本使用 `python3 scripts/image_tag.py` 解析：当前 HEAD 有 Git tag 时取该 tag，无 tag 时取至少 12 位 commit 短 hash；多个 tag 按 Git 版本排序取最高项，历史 tag 不沿用，不兼容 Docker tag 的名称会报错。README 的服务镜像构建与 `make k8s-test` 使用同一规则；Kubernetes 测试的 topic / 消费组仍通过随机 run-id 隔离。

`make package` 另构建 Observer / Consumer / usage-worker 镜像，并默认将这三个镜像保存到 `dist/package/images/images.tar`。指定 `--include-dependency-images` 后才额外归档 Kafka、Doris、Istio proxyv2；默认部署前需保证目标节点能拉取或已导入依赖镜像。分发包始终提供单节点 Kafka / Doris 的持久化部署、topic / 数据库初始化 Job、采集服务、全部 ConfigMap / Secret，以及针对网关工作负载渲染的关闭态 / 启用态 EnvoyFilter；全部为可直接 `kubectl apply` 的 YAML。包根目录的 `envoyfilter.yaml` 提供工作负载的关闭态配置，`gateway/disabled/` 与 `gateway/enabled/` 保留两种状态；Observer 就绪后再单独应用 EnvoyFilter。`manifest.json` 分别记录全部镜像引用、归档镜像和需外部准备的镜像。省略 `--accepted-request-host` 时，打包写入 `["*"]`，匹配全部合法 DNS Host；显式提供时完整覆盖默认值，可重复传入目标域名、后缀通配或 `*`，并写入两种状态的 EnvoyFilter、manifest 和包内说明。显式空名单或非法名单在镜像构建前失败。打包不访问集群，也不绑定 Pod；可通过 `PACKAGE_ARGS` 的 `--gateway-namespace`、`--gateway-label KEY=VALUE` 选择目标工作负载。指定输出目录使用 `PACKAGE_DIR`，完整命令、镜像导入和部署顺序见 [README 分发说明](../README.md#分发镜像与完整-kubernetes-清单)。该包采用开发环境的单节点、明文 Kafka 与 Doris root 空密码，生产仍使用下文的独立基础设施和认证配置。

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

`make local-up` 在 `dist/envoy.remote.json` 渲染实际 Wasm 摘要，重启 Fixture/Observer/Consumer，核对下载内容后重建网关。网关只挂载配置，插件来自 Observer HTTP 下载。更新本地产物后运行 `make build wasm local-up`，自动回归则使用独立的 `make k8s-test`，不能仅重启旧网关而沿用旧摘要。脚本只初始化该 Compose 栈，应用 001/002/003 migration，在独立 metering_development 库执行新初始化，并创建 24 分区、72 小时保留、delete 策略的 llm-metering-development topic。明文 Kafka、合成鉴权、副本数 1 与 Doris root 空密码仅用于开发联调；生产按原方案配置认证、副本、min ISR 和最小权限账号。

普通 JSON / SSE 合成请求：

```sh
curl -N -H 'Host: model.example.invalid' -H 'Authorization: Bearer synthetic-test-key' -H 'Content-Type: application/json' \
  --data '{"model":"fixture","stream":true,"messages":[{"role":"user","content":"synthetic fixture"}]}' \
  http://127.0.0.1:18080/v1/chat/completions
curl http://127.0.0.1:19091/metrics
curl http://127.0.0.1:19092/readyz
```

fixture 只模拟 ext_authz 访问允许与模型 usage，不实现真实密钥鉴权或模型调用。集成测试写入合成事件及隔离的未知版本测试 topic/group；这些诊断事实保留在本地库，测试不会清理已有业务数据。all-in-one Doris 的 BE 注册地址是回环地址，因此本地 Consumer 直连 `doris:8040`；生产 FE→BE 重定向需登记全部实际 FE/BE 地址。

## 归属 API 配置与名称查询

Observer 环境仅接受 `prd`、`stg`、`dev`。分别设置 `observer.service_lookup.uri` 和 `observer.customer_lookup.uri` 为完整 HTTP/HTTPS 端点；每项通过 `credential_env` 指定提供 `x-remote-data` 值的环境变量，默认示例分别为 `METERING_SERVICE_LOOKUP_TOKEN`、`METERING_CUSTOMER_LOOKUP_TOKEN`。URI 为空时不要求凭据并继续占位；非空但凭据缺失时启动报固定配置错误。HTTPS 私有 CA 可通过各项 `ca_file` 指定挂载文件，证书验证始终启用。Kubernetes 模板可从可选 Secret `llm-token-stats-observer-lookups` 注入同名环境变量，Compose 从启动环境传入，均不提交真实凭据。

`credential_env` 只能填写环境变量名，不能直接填写密钥。服务查询应设置 `"credential_env": "METERING_SERVICE_LOOKUP_TOKEN"`，将真实密钥放入 `llm-token-stats-observer-lookups` Secret 的 `METERING_SERVICE_LOOKUP_TOKEN` 键；Deployment 已通过 `envFrom` 注入该 Secret。客户查询同理使用 `METERING_CUSTOMER_LOOKUP_TOKEN`，其 URI 为空时无需提供该密钥。变量名为空或格式非法会触发 `config_lookup`；变量名合法但对应凭据缺失或不适合请求头时会触发 `lookup_credential`。更新 ConfigMap 和 Secret 后重启 Observer；离线包的操作命令见包内 README。

两个接口的 HTTP 200 响应使用 `responseCode`、`responseMsg`、`success`、`data` 包装。成功要求 `responseCode="000000"`、`success=true`，原有 ID 与名称位于 `data`。未查询到数据或查询出多条数据使用 HTTP 200、`responseCode="650013"`，统一映射为 `lookup_invalid_response`，不能推断为未登记；第三方入参检查不合法使用 HTTP 200、`responseCode="100001"`，映射为 `lookup_unavailable`。两者 `success=false`，`data` 为 null 或缺省。认证失败使用 HTTP 401、`responseCode="401"`、`success=false`、`responseMsg="Unauthorized"`，映射为 `lookup_unavailable`，合法认证响应的消息同样记录脱敏日志。HTTP 400、429、5xx 保持原故障处理，不兼容旧英文业务码。不接受旧扁平成功响应，详细映射见[归属接口契约](spec/plan/host-service-provider-implementation.md#48-错误契约与-observer-映射)。

每次实际 HTTP 查询完成后，Observer 按统一文本格式记录 `event=identity_lookup_response`，包含查询类型、耗时、HTTP 状态、业务码、成功标志、脱敏后的 `responseMsg` 和本地状态；成功为 INFO，失败为 ERROR，并遵循 `LOG_LEVEL` 和同类故障汇总规则。消息不参与判断，可省略或为空；先替换当前凭据及客户 token，再按 UTF-8 边界限制为 1024 字节，转义换行和控制字符。不打印 URL、请求体、认证头、data 或非法响应的原始正文。响应包装不进入 Kafka/Doris。日志使用进程共享的 256 条有界队列和单个输出协程；只有已脱敏文本入队，阻塞不影响归属返回、token 清理和额度释放。队列满时丢弃新日志，保留 `metering_lookup_logs_dropped_total`，并提供进程级 `metering_logs_dropped_total`；退出排空最多等待 200ms。

`lookup_timeout_ms` 默认 200 ms（最大 2000），两类查询并行使用请求级等待截止时间，从查询协调开始时计时；共享任务沿用首次发起时的截止时间，加入者不延长；`lookup_max_concurrency` 默认 64（最大 256），不排队；每项 `max_requests_per_second` 默认 1000，独立限速且不等待。缓存命中和合并等待先于远端额度检查，不占远端并发或速率额度；只有发起新任务的请求占用并发额度。响应正文和响应头各限 8192 字节，不跟随重定向、不使用环境代理、不自动重试、不缓存原始 token。进度 ACK 和正文处理不等待外部查询；两类事件在相同快照冻结后发送。

每项 `service_lookup.cache` / `customer_lookup.cache` 默认启用实例内 LRU 成功缓存：`max_entries=10000`、`ttl_seconds=1800`、`stale_if_error_seconds=900`、`stale_retry_interval_seconds=5`。两个接口独立配置；省略字段用默认值，`enabled=false` 关闭该接口缓存及查询合并，宽限期设为 0 关闭旧值回退。容量范围 1–100000，TTL 1–86400 秒，宽限期 0–86400 秒，重查间隔 1–300 秒。部署、离线打包配置沿用相同默认值；修改配置后重启 Observer，实例重启会清空缓存。

服务键包含规范 Host、固定集群和环境；客户键是使用进程随机密钥生成的 API Key HMAC-SHA256，不保存原始 token。相同键的并发未命中共享一次查询，加入者立即清理自身 token。TTL 到期等待刷新；仅网络故障、超时、HTTP 5xx、契约有效的 429 或本地额度不足，可在额外 15 分钟内沿用旧值并保持 `mapped`。失败后同条目 5 秒内不重查，到期由下一次观察触发刷新；没有定时后台刷新。HTTP 401/403、100001、650013、非法响应及其他明确错误立即清除旧值。命中或失败不延长寿命，默认最多沿用至成功查询后的 45 分钟；名称与归属变化因此可能延迟反映，历史数据保持原快照。详细规则见[归属结果缓存](spec/plan/host-service-provider-implementation.md#410-归属结果缓存)。

缓存命中不重复记录 responseMsg；实际刷新失败仍记录实际调用结果。缓存指标以 `metering_lookup_cache_service_` / `metering_lookup_cache_customer_` 为前缀，提供 `hits_total`、`misses_total`、`shared_waits_total`、`stale_used_total`、`refresh_failures_total`、`evictions_total` 和 `entries`，不含 Host 或密钥摘要。

HTTP 401/403 在响应头确认后立即登记不可回退的 `lookup_unavailable`，撤销旧条目及同键未冻结的缓存候选；401 的正文仅用于诊断，缓慢、不完整或超时的正文不能使事件回退为旧归属的 `mapped`。原 worker 仍在原查询预算及 8192 字节上限内读取 401 正文，完整有效的 `responseMsg` 继续脱敏入队；不增加后台读取任务，查询额度与 token 仍在 worker 实际退出后释放。同次查询只统计一次失败，已冻结事件保持原快照。

HTTP 客户端总超时为 2 秒，拨号（含 DNS 解析）和 TLS 握手各有独立的 2 秒超时。查询截止后，尚在建立的连接仍受阶段超时约束，故障连接最终关闭并释放每主机连接额度；阶段超时归类为 `lookup_timeout`。

HTTP 不读取 `ca_file`，传输保护由部署链路提供；HTTPS 使用镜像自带的系统根证书及可选附加 CA，持续校验证书和主机名，不自动降级为 HTTP。观察取消、超时或 ACK 失败时，已接纳查询继续到完成或原截止时间，保留成功侧的 ID 和名称，未返回侧按缓存规则复核旧值，无法回退时标为 `lookup_timeout`。事件仅定稿一次，保留已校验的 End；迟到结果不改写快照。查询额度直到底层任务真正退出才释放，客户查询返回后清理 token。该行为不改变业务转发和 Kafka 投递边界，也不保证进程退出时查询完成。

名称从接口返回值保存到 Kafka 必填键及 Doris `service_name`、`provider_name`、`customer_name` 可空列，单项非空 UTF-8、最多 512 字节。`requests`、`request_statistics`、`event_details` 和 `usage_effective` 可直接查询名称；同一请求名称不一致按冲突隔离。重命名保留历史快照，名称不参与稳定身份摘要或日/月汇总分组。明细保留期内可按请求 ID 查询：

```sql
SELECT observation_id, service_id, service_name,
       provider_id, provider_name, customer_id, customer_name
FROM metering_development.requests
WHERE observation_id = '<observation_id>' AND has_conflict = 0;
```

新库初始化直接包含名称列及最终视图；名称只作为请求快照保存。迁移不再提供补列、重建旧视图或补注释脚本，本地入口要求已有库与当前模型一致，否则应选择隔离空库重新初始化。工具不会自动删除旧开发数据。

## 协议生成与版本

Protobuf 工具：protoc 29.3、protoc-gen-go v1.36.5、protoc-gen-go-grpc v1.5.1。生成文件保存标准工具版本标记；运行时 protobuf v1.36.10、gRPC-Go v1.78.0、franz-go v1.18.1，最终版本以 go.mod/go.sum 为准。

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
# 把以上工具目录加入 PATH，安装 protoc 29.3 后：
make generate
```

`observation.proto` 的 go_package 已固定到本项目。Wasm 使用有界手工编码与标准 Protobuf 互操作测试，禁止将 gRPC-Go 编入插件。基底 SDK 导出 ABI marker `proxy_abi_version_0_2_0`，实际正文回调名为 `proxy_on_request_body`；ABI 脚本检查这些真实符号、gRPC 回调/hostcall 及无 socket imports，保留基底 marker。

## Wasm Host 白名单

仅在 Wasm 配置顶层维护 `accepted_request_hosts`。支持精确 DNS 域名、`*.example.com` 和 `*`；后缀通配匹配任意层级子域名，不含根域名。请求从 `:authority` 规范化，先拒绝非 ASCII 字符，再转小写并去端口和末尾点，不读取转发头。规则最多 1024 项，且整体 Wasm 配置仍须满足 64 KiB 上限；加载时转小写、去末尾点；端口、URL、IP、其他通配符和规范化后重复项均为无效配置。缺省或空名单不采集；非法配置停用插件采集，继续放行业务。

`make package` 和 Release 发布脚本会在省略 Host 参数时显式写入 `["*"]`；生成的配置因此匹配全部合法 DNS Host。直接提供给 Wasm 的配置仍按上述缺省、空名单规则处理。

Host 无效或不匹配时，在提取凭据、建立观察流、缓存或改写正文之前直接透传，分别记录 `metering_requests_ignored_invalid_host_total`、`metering_requests_ignored_host_total`，无域名标签。Observer / Consumer 不维护服务或 Host 白名单，仍校验配置版本、协议和字段状态一致性；历史空 Host 诊断事件仍按原事件契约处理。

升级时先构建新 Wasm、按新摘要渲染并启用含 Host 名单的 EnvoyFilter，再更新 Observer / Consumer 二进制和删除旧名单的配置。下游不接受 `accepted_service_ids` 或 `accepted_request_hosts` 字段。已有离线包必须重新生成；这些校验不代表生产验收。

## 已确认的旧版网关处理

- 请求头 Continue；符合条件的正文在非 EOS 回调 Pause，EOS 时只修改 `stream_options.include_usage`。请求头可先到上游。这是用户在真实旧版 ext_authz PoC 后确认的调整：暂停头会延迟已缓冲正文回调直到改写超时。
- 改写从请求头开始计时，默认/最大 450 ms，tick 默认/最大 10 ms，目标在 500 ms 内原文放行；允许调度拥塞或恢复调用暂时失败时短暂超出。正文回调及解析结束均检查截止时间。旧合法配置中的 2000 ms/100 ms 在运行时收紧，新发布配置须通过更严格校验。
- 超时优先恢复业务，不复制缓冲前缀、不等待旁路清理；恢复失败保留状态并在后续 tick 重试，下一次正文回调直接透传。改写恢复队列独立于观察超时，指标失败不会 panic。配置更新先恢复存量请求；取消的观察保持 partial/unknown。VM 或 Envoy 自身失效仍不能由插件定时器保证恢复。
- 改写预算与观察预算独立；Observer 不可用仍可补充参数。超限、超时或 JSON 无效时恢复原文，响应正文一直 Continue。旧版真实慢上传 PoC 中，Root 异步 Resume 后尾片可能不再完整经过 Wasm 回调，因此超时恢复会取消观察且不再复制缓冲前缀，结果保留 partial/unknown，不宣称完整用量。JSON 校验限制深度 64、元素 100000、单对象键 8192；超限取消解析/原文放行。这些额外保护可能使复杂合法 JSON 降级，不能宣称所有请求均能改写。
- 当前 Wasm 配置资源整数还限制在有符号 32 位正值范围，防止 TinyGo wasm32 的长度转换溢出；给出的初值全部满足。预发布仍必须运行 Schema 与关系校验。
- 当前默认规则为 `route_name: "*"`，忽略路由名，采集匹配顶层 `accepted_request_hosts` 的 `POST /v1/chat/completions`；路由属性或 metadata 缺失也可命中。路径去掉查询串后精确匹配，其它方法、尾斜杠、子路径和不同路径均忽略。精确路由规则仍支持：先读取可信 `route_name`，缺失时读取 `route_metadata.filter_metadata.metering.route_name`，都不可读时该精确规则不命中。规则按配置顺序选择，不读取客户端提供的路由或身份头。
服务及供应商由 Observer 的服务查询接口解析，客户/API Key ID 由客户接口解析；空 URI 为占位，启用 HTTP/HTTPS 适配后同时保存名称。route_name 仅诊断，不参与身份摘要；旧 accepted_routes、身份元数据配置及规则 service_id 已删除。同一事件不同内容仍冲突。
- trailers 只产生零字节 EOS BodyChunk，不上传其内容。End 校验两个方向的片序、偏移、字节总数及 EOS；HTTP context 删除后只由 Root+token 处理迟到 ACK，不能延长业务生命周期。
- 上游重复伪造的计量响应头先全部移除；Start ACK 已到才加入一个 Observer UUID。快响应允许头缺失；有头也不保证事件已入 Kafka/Doris。
- 固定 franz-go 的 `RecordRetries(0)` 阻止首次发送，实际使用 1 表示仅一次尝试。发送超时/重试次数耗尽保守计为 uncertain，不补发。3 秒 watchdog 关闭超龄 Producer，额外 1 秒内不能释放则退出进程。

## 服务配置与部署

入口为 `observer --config PATH`、`consumer --config PATH`。服务配置严格拒绝未知字段；Observer/Consumer 分别读取自己接受的 config_version 和 service 注册。本次采用唯一新契约并初始化隔离库；正常同契约重放仍需保留对应 config/service 注册，禁止混入旧格式消息。

部署模板在 [Observer](../deploy/observer/observer.yaml)、[Consumer](../deploy/consumer/consumer.yaml)、[EnvoyFilter](../deploy/gateway/envoyfilter.yaml)。先 `make wasm`，再构建 Observer/Consumer Dockerfile 镜像，替换 image digest，建立 metering namespace、对应 ConfigMap/Secret，再使用模板。生产配置示例在 `configs/*.production.example.json`，其中地址是待核对的样例，不代表已部署。Kafka 支持挂载 CA/客户端证书与 SASL PLAIN 环境变量；`kafka.topic` 支持合法 Kafka 名称，Observer 与 Consumer 必须配置一致，生产约定使用 `llm-metering-events`，隔离测试使用专属 topic。Doris DSN、HTTP 用户及密码从 Secret 环境变量读取。DSN 必须显式配置连接、读写超时、UTC 会话；禁止将密码写入配置或提交仓库。需要受控网络与网格 mTLS，模板没有代替真实认证、网络策略或 G0 验收。

EnvoyFilter 模板不可原样 apply；必须校验渲染后的 JSON。按命名空间和已有工作负载标签渲染，默认关闭：

```sh
python3 scripts/render_gateway_filter.py \
  --gateway-label istio=ingressgateway \
  --gateway-namespace istio-system \
  --wasm-file dist/gateway-observer-v1.wasm \
  --wasm-url http://llm-token-stats-observer.metering.svc.cluster.local:9093/wasm/gateway-observer-v1.wasm \
  --wasm-cluster 'outbound|9093||llm-token-stats-observer.metering.svc.cluster.local' \
  --output dist/envoyfilter.disabled.yaml
# G0/G1 通过后，为该工作负载使用显式 --enable。
```

`gateway_instance_id` 由 Wasm 在插件启动时读取 Envoy 的 `node.metadata.NAMESPACE` 和 `node.metadata.NAME`，组合为 `namespace/pod-name`；配置 JSON 不再接受此字段。Istio 1.13.5 默认提供这两项元数据，身份读取无需额外修改 Pod 模板；ECDS 下载转换开关仍须按下文设置。多个副本共享同一 EnvoyFilter，各自读取实例名称，扩容或重建后无需重新渲染。标识不包含 Pod UID，同名 Pod 重建后标识不变；跨集群分析需结合 `cluster_name`。命名空间按 DNS label 校验（最多 63 字节），Pod 名称按 Kubernetes DNS subdomain 校验（总长最多 253 字节，不另限每段 63 字节）；身份缺失、非法或组合后超过现有协议的 128 字节上限时停止该插件采集，增加 `metering_gateway_identity_unavailable_total`，业务继续放行；不截断名称、不回退到固定值，也不读取客户端请求头。

过滤器通过代理 `metadata.NAMESPACE` 与工作负载启动标签共同匹配，根配置命名空间中的资源也不会跨命名空间生效。资源默认名称为 `llm-token-stats-envoyfilter-wasm`，不自动追加摘要；同一命名空间多网关需要独立配置时，渲染和打包须通过 `--envoyfilter-name` 显式指定不同名称。自定义名称必须以 `llm-token-stats-` 开头且为不超过 63 字符的 DNS 标签；启用态和关闭态名称相同，选择器变更会更新同名对象。默认 `istio-system`、`istio=ingressgateway`，通过 `--gateway-label` 可替换默认标签。Wasm 使用远程 URL、HTTP 集群和实际文件 SHA-256 加载；模板插在 router 前，要求 ext_authz 已在它之前，以真实 config_dump 核验。每 Worker 一份 Root；生产热更新及回退仍需完成灰度验收。从旧版迁移先移除按 UID 命名或固定名 `gateway-observer` 的过滤器，避免新旧重复安装。

Istio 的 `outbound|端口|子集|服务DNS` 是路由集群名，不能原样作为 Go gRPC 服务的 HTTP/2 `:authority`。插件保持 `cluster_name` 不变，自动将这类集群的 authority 设置为 `服务DNS:端口`；普通本地集群名保持原值，不改变集群 TLS/SNI。自定义 ext_authz 若同样使用 Istio 集群名，必须在其 `grpc_service.envoy_grpc.authority` 显式填写有效服务 DNS。

Doris SQL 查询和 HTTP Stream Load（包括允许的重定向）统一使用 `dsn_env` 指向的 DSN 中的用户名和密码，凭据在初始化时读取；更新 Secret 后需重建 Pod。Doris 配置不再接受 `username_env`、`password_env`，独立的 `DORIS_USER`、`DORIS_PASSWORD` 不再生效。Stream Load 只向 `allowed_load_hosts` 发送凭据，拒绝未登记跳转及 HTTPS 降级到 HTTP。每个 label 的冻结内容摘要参与命名；超时或确认丢失后先查询原事务为 VISIBLE，再核对全部行键。`Label Already Exists` 不是成功判据。

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
# 本地联调；生产可另开终端执行 kubectl -n metering port-forward service/llm-token-stats-observer 19093:9093
curl --noproxy '*' --fail --output dist/gateway-observer.download.wasm \
  http://127.0.0.1:19093/wasm/gateway-observer-v1.wasm
sha256sum dist/gateway-observer-v1.wasm dist/gateway-observer.download.wasm
```

网关通过 ECDS 使用原生 `code.remote` 加载，下载超时 5 秒、额外重试 0 次，设置 `fail_open: true` 和 `nack_on_code_cache_miss: true`。HTTP 过滤器配置 Lua 透传默认值及 `apply_default_config_without_warming: true`，首次下载不会阻塞监听器；后续 ECDS 更新被拒绝则保留上一份有效配置。关闭态直接安装 Lua，不依赖 Wasm。

当前 ECDS 候选验收网关使用原始 `istio/proxyv2:1.14.6`（Envoy 1.22.7-dev）；1.13.x 已复现宿主崩溃，不作为 ECDS 启用版本。启用补丁以 `proxyVersion` 排除 1.13.x，旧代理继续业务转发但不安装采集；关闭态 Lua 适用于旧代理。本地控制面仍为 1.13.5，仅是隔离兼容性评估，不代表生产混合版本受支持。生产应选择受支持的完整 Istio 版本组合并完成 G0–G5。

目标网关需设置 `ISTIO_AGENT_ENABLE_WASM_REMOTE_LOAD_CONVERSION=false` 并完成滚动，否则 Istio 1.13.5 默认由 Agent 下载并重写为本地文件，不能据 YAML 中的 outbound 集群推断实际下载路径。开关影响同一网关上的所有 ECDS 远程 Wasm，须核对其他插件。为使重推命令能确认加载，在 Pod 的 `proxy.istio.io/config` 中将 `.*extension_config_discovery.*` 合并到 `proxyStatsMatcher.inclusionRegexps`，保留既有设置并完成滚动；旧版默认统计匹配规则不导出这些指标。下载集群默认是 `outbound|9093||llm-token-stats-observer.metering.svc.cluster.local`，保留网格 mTLS；采集 gRPC 仍使用 9090 的 HTTP/2 集群。下载端口默认 ClusterIP，Observer 本身不终止 TLS、不提供下载鉴权。

ECDS 内部名称按配置摘要隔离；Kubernetes EnvoyFilter 名称仍固定。重推时原子更新 EXTENSION_CONFIG 和 HTTP_FILTER 的订阅名称，保留 Wasm 参数与 VM 初始化配置，避免反复复用旧监听器上下文。固定旧版 Envoy 存在 [ECDS 上下文生命周期问题](https://github.com/envoyproxy/envoy/issues/14930#issuecomment-1240343729)，本地曾复现，不能仅凭 fail_open 或缓存开关认定更新安全；本次回归要求无意外重启。

首次缓存缺失会 NACK 并后台下载。旧版控制面不因 NACK 自动重推；首次安装、下载恢复、网关重启或扩容后运行 `python3 scripts/reload_gateway_filter.py --context CONTEXT --gateway-namespace NAMESPACE --envoyfilter-name NAME`。命令默认等待 120 秒，每 15 秒重试，不重启 Pod，随分发包携带。使用目标 ECDS 加载计数及配置/身份错误增量确认加载；实际激活仍需用 SSE 改写与 Kafka/Doris 事件验证，普通 `config_dump` 中的订阅或业务 200 都不足以证明采集已工作。

清单使用块式 YAML，插件参数通过 `Struct` YAML 映射传入；旧版 Envoy 将 Struct 作为 Protobuf 二进制传递，新 Wasm 已支持该格式；既有独立 JSON 输入继续支持。新 Wasm 支持有界 Protobuf Struct 和严格 JSON，并复用字段、Host、身份与预算校验；解析失败只禁用采集，保持业务透传。Struct 解码无反射并有深度/节点界限，拒绝重复键、非法 UTF-8、非整数/非有限数，维持 64 KiB 上限及 `schema_version=1`。VM 内不使用依赖 panic/recover 的 YAML 解析器。TinyGo 构建固定 `-buildmode=wasi-legacy`，避免命令式 WASI 入口调用 `proc_exit`；ABI 检查同时拒绝套接字、文件系统和进程退出导入。

仅保留当前镜像的单个文件。先更新所有 Observer 副本并核对摘要，再更新匹配的新清单并重推；滚动期间允许缺少采集。回退先恢复对应 Observer 镜像，再恢复匹配清单。停采使用关闭清单或删除过滤器。ECDS 兜底不覆盖 Envoy/节点崩溃或 OOM 等宿主故障。

## Consumer 恢复与统计边界

每分区顺序执行：冻结记录 → events_raw / ingest_rejections 可见 → 跨在线分区冲突标记可见 → 全部日期通知可见 → 批次凭据可见 → 提交下一 offset。数据库故障暂停对应分区，保留一份冻结批次并有界重试；撤销 owner 后停止新写入/提交，新 owner 从已提交位置重放。相同事件与摘要由 Unique Key 去重；不同变体保留并永久登记冲突。

Consumer 按 Kafka 分区跨多次拉取攒批：达到 `max_batch_rows`（默认 5000，可配置范围 1–20000）、`max_batch_bytes`（默认 4 MiB，可配置范围 32768–16777216 字节，即最多 16 MiB），或最早一条记录等待 `flush_interval_ms`（默认 15000 ms，即 15 秒；可配置范围 1–600000 ms，最多 10 分钟）后写入；下一条放不进字节预算时先写已有前缀。字节预算按 Kafka key/value 计量，不是展开后的 Stream Load 正文或进程 RSS 上限。每次拉取最多配置的行数，没有额外的 128 条写入限制；数据库回查仍按 128 条拆分查询，提取组内共同键条件减少重复表达式；不同记录的多列键保持配对匹配，不放宽可见性校验。每个已分配分区保留一份未满批次或一份失败后的冻结批次，写库并发最多 4 个分区；处理轮次到期时最多额外保留一份 poll 结果及其合并的已有缓冲，处理完之前不继续拉取。总内存还包含冻结对象、展开后的编码正文与 Kafka 客户端缓冲，需要按分区数和实际事件大小验收。例如 24 个分区各缓存接近 16 MiB 时，仅记录字节就接近 384 MiB，不能据此推断 512 MiB 的进程内存限制足够。

即使没有后续消息，未满批次到期也会写入；不同分区按各自最早记录计时，后续消息和其他分区到期不会重置该期限或强制刷新本分区。攒批与 Kafka fetch 等待、分区移交解耦：fetch 最长等待固定为 5 秒，应用 poll 最长等待 500 ms。每轮处理保护预算为 45 秒、单批冻结/写库/提交预算为 30 秒，到期取消处理并释放保护，未开始的记录下轮继续；单次拉取仍沿用 4 MiB、每分区 1 MiB、响应读取 8 MiB 的限制，通过跨 poll 累积实现 16 MiB 批次。合作式 rebalance 保留未撤销分区的缓冲和截止时间；撤销分区清除未提交缓冲和冻结批次，由新 owner 从 Kafka 已提交位置重放，不为移交强制写入小批次。退出不等待攒批期限。失败重试沿用冻结内容及 label，确认全部可见后才提交 offset。初次审计及分钟巡检不等待缓冲清空；旧分配的审计结果不能更新新分配的就绪状态。

三个字段省略或为 0 时使用既有默认值；负数、越界值和小于 32768 的非零字节预算拒绝启动。以下片段同时启用三个新上限，须合并到现有 Consumer 配置的 `consumer` 节点中：

```json
{
  "flush_interval_ms": 600000,
  "max_batch_rows": 20000,
  "max_batch_bytes": 16777216
}
```

600000 ms 是最大攒批等待预算，不是每隔 10 分钟才允许写库：条数/字节达到上限会提前写入；实际可见延迟还包括冻结、排队、数据库处理和重试。攒批时尚未发起 Doris 导入，不自动放宽 Doris HTTP/事务超时。提高上限不会自动修改默认参数或部署资源；启用大批次前需按实际分区数验收内存及处理耗时。

20000 条、16 MiB 和历史十分钟到期写入的隔离 Kubernetes 验收结果见[大批次验收记录](spec/plan/consumer-batch-limits-verification.md)。当前空闲到期集成用例使用真实 1 分钟等待，持续检查不得提前写入，并验证到期后的数据可见性与 offset 提交；600000 ms 上限仍由配置与调度单元测试覆盖。

[消费协调初始化](../migrations/doris/metering/002_consumer_coordination.sql)保存消费起点和保留期缺口；这些表及凭据/通知本轮不自动清理。新组持久登记 Kafka earliest；已有组缺少起点时通过 `initial_offsets` 明确接管。多份不兼容起点阻塞分区。已保存的恢复位置落后于 earliest 时登记缺口并暂停；经人工处置后，`gap_resume_offsets` 必须填写该分区当时的 earliest，不能填 latest。恢复新流量不会抹掉缺口或把历史覆盖改为可信。

未知正整数 schema_version 暂停分区且不提交；格式错误及永久非法消息只保存坐标、摘要和固定原因，不持久化原始非法正文。每分钟巡检永久冲突与连续水位：由持久起点、区间凭据和 Kafka committed offset 推导，不使用 MAX(offset) 跨越空洞。存在缺口、缺凭据或巡检失败会使 Consumer `/readyz` 为 503。单次证据/凭据扫描有 100000 行保护，达限即停止确认并计数；生产扩展需验证巡检容量。

统计/查询 HTTP API 尚未实现。usage-worker 已实现基于日期通知的 T+1 日/月物化视图刷新和恢复；成功状态说明对应日期的已观察数据完成计算，不代表 Kafka 已消费到最新或不存在历史缺口。需同时核对消费凭据、offset 和 consumer_partition_gaps，不能把 Consumer 健康探针当作完整性证明。运行及回填见[Doris 调度与恢复](doris-summary-coordination.md)。

固定指标不携带 UUID、身份或正文标签。Wasm 暴露选择/忽略、改写/取消原因与 active_streams/inflight_bytes/inflight_messages/active_rewrites/rewrite_reserved_bytes；Observer 暴露 active_streams/parser_reserved_bytes、定稿与发送结果；Consumer 暴露 batch_committed/batch_failure、partition_paused、audit/watermark 状态。服务均有 `/livez`、`/readyz`、`/metrics`。资源预算是记账上限，尚需加上 Go/VM heap、HTTP2/native buffer 及存活代数，不能等同于 RSS 承诺。

## 验证记录与未完成门槛

本地使用公开 `istio/proxyv2:1.13.5` 中的 Envoy，静态配置运行并通过 Observer 下载 Wasm，过滤器顺序为 ext_authz → Wasm → router；Kafka 3.9.2、Doris 4.1.3。这验证固定 Envoy hostcall 和数据链路。2026-10-15 另已在本地 kind Kubernetes 1.22.17 / Istio 1.13.5 控制面完成独立命名空间部署、XDS 匹配、强制 mTLS 下载及 gRPC 采集入库验证，见[验证记录](spec/plan/gateway-collection-verification.md)和 README 的本地 Kubernetes 操作说明；私有镜像、真实 Auth、生产容量和完整 G0–G5 仍未验收。

2026-10-14 已将所有本地发布端口切换到 `0.0.0.0`，通过服务器 IPv4 地址验证全部 11 个 TCP 端口、健康检查、Doris SQL 和 Kafka 元数据连接。切换前的 108 条原始事件、129 条消费凭据及 24 个 Kafka 分区的消费位置、日志末尾位置均保持一致；数据备份保留在 `.tmp/local-storage-*`。重新执行 `make test build wasm` 与禁用缓存的 `make local-test` 均通过，集成测试实际写入和查询 Doris；再次调用存储迁移检查不会重建已有挂载卷的容器。

已通过契约/文档/表字段一致性检查、25 项 Python 测试（含本地地址配置、迁移覆盖保护和备份失败恢复）、Go 单元和竞态测试、SDK internal/proxytest 测试、TinyGo 编译与 ABI 检查。事件规范编码与摘要同六份 Python golden 对照；标准/手工 Protobuf 互操作、非法帧、逐字节 JSON/SSE 分片、usage 候选冲突、提交屏障故障及撤销 owner 由自动测试覆盖。

真实本地集成覆盖普通 JSON、SSE/响应 trailers、Start ACK 关联、unknown 保留 NULL、非白名单请求、相同事件重放去重、不同变体冲突隔离、日期通知/凭据、未知版本暂停；HTTP1 chunked 中途停顿超过 500 ms、256 KiB 边界/边界加一/1 MiB 请求的上游正文摘要保持原样，超时观察降为 partial/unknown，正常延迟响应中的计量 ID 与事件一致；真实 BE 写入后断开 HTTP 确认，仍确认原 label 与行键；独立 TCP Kafka 黑洞验证发送不阻塞及有界超时/watchdog。上述场景现已迁入 Kubernetes Job，由 `make k8s-test` 重现，结果保存在 `.tmp/k8s-collection/`。

G0–G5 仍须独立完成：私有镜像/真实 Auth、HTTP2 慢上传与断开/热更新故障交叉、真实 Pod 崩溃及 rebalance 压测、Kafka 保留期丢失后运维恢复、峰值两倍/24 小时/10 分钟 SSE、首包/明细/查询延迟、灰度回退与旧代释放。已有本地证据不等于这些门槛全部通过。

## 归属配置与隔离库

Observer 必填 cluster_name、deployment_environment，service_lookup/customer_lookup 各有可空 uri。非空 URI 使用 HTTP/HTTPS 和独立 x-remote-data 凭据，空 URI 使用 lookup_not_implemented 占位；未启用接口时不要求第三方 Secret。Wasm 先按 Host 筛选，非法 Host 不建立观察；invalid_host 仍为历史诊断契约保留。两事件复用快照，客户失败使 identity_status=invalid，但不单独影响 usage_eligible；用量资格由观察完整度、用量状态和质量异常决定。客户/API Key 报表仍需过滤未知归属。

Consumer 的 doris.database、DSN、Stream Load 和证据/巡检 SQL 使用同一经过校验的库名。初始化使用 `python3 scripts/render_doris_sql.py --database 数据库 migrations/doris/metering/001_init.sql`，对其余初始化 SQL 同样渲染。本地默认 metering_development，新 Kubernetes 测试每次用 metering_k8s_<run-id>；不修改旧 metering 库及其消息，清理保留测试库作为证据。普通 Docker 联调使用合成 token 和隔离明文网络；生产/真实密钥只允许已验证的网格 mTLS/TLS。

数据库字段及每张表的作用见[数据库表与字段文档](doris-database-schema.md)。全部表和字段注释直接随初始化脚本创建，数据库字典通过隔离库的实际元数据校验后生成。

## 用户与扩展请求头

事件新增 `user_id`、`user_agent`、`request_headers` 三个必填键：前两项可为 `null`，扩展头为字符串 Map，无有效项为 `{}`。Wasm 的 `user_id_header` 为有序候选列表；默认读取 `user-id`。两个事件保存相同快照；用户 ID 纳入日/月汇总维度。配置、预算与查询示例见[请求头采集说明](request-header-collection.md)。

## 组件日志与 LOG_LEVEL

Observer、Consumer 和 usage-worker 在启动时读取环境变量 `LOG_LEVEL`，支持 `debug`、`info`、`warn`、`error`，忽略大小写和首尾空格。默认 `info`；非法值告警并回退到 `info`，修改后重启生效。Wasm 只输出必要的 INFO/WARN/ERROR；其 `debug` 与 `info` 输出范围相同。

日志使用单行可读文本，Go 服务写 stderr，Wasm 写 Envoy 日志。字段包括时间、等级、组件、事件及调用阶段、耗时、协议状态和安全提取的错误原因。DEBUG 用于观察生命周期、缓存、批次 offset、刷新任务及阶段耗时；不输出正文、密钥、认证头、完整 DSN、SQL 或 SQL 参数。SQL 错误记录错误码和 SQLState，不直接打印可能含数据或凭据的服务端消息。

```sh
LOG_LEVEL=debug ./dist/observer --config configs/observer.local.json
LOG_LEVEL=debug ./dist/consumer --config configs/consumer.local.json
LOG_LEVEL=debug ./dist/usage-worker --config configs/usage-worker.local.json --once
# Compose 将宿主 LOG_LEVEL 传给这三个服务及 gateway，配置变化后重新创建容器
LOG_LEVEL=debug docker compose -f deploy/local/compose.yaml up -d observer consumer gateway
# Kubernetes 环境变量变更会触发 Deployment 滚动
kubectl --context CONTEXT -n metering set env deployment/llm-token-stats-observer LOG_LEVEL=debug
kubectl --context CONTEXT -n metering set env deployment/llm-token-stats-consumer LOG_LEVEL=debug
kubectl --context CONTEXT -n metering set env cronjob/llm-token-stats-usage-worker LOG_LEVEL=debug
```

对生产已有网关，在 `istio-proxy` 容器中设置 `LOG_LEVEL=info`，并将 `wasm:info` 合并到 Pod 模板的 `sidecar.istio.io/componentLogLevel` 注解，保留其他组件的既有设置，完成滚动后再启用或重推插件。独立 Envoy 使用 `--component-log-level wasm:info`；其全局等级可以继续保持 warning。EnvoyFilter 的 `vm_config.environment_variables.host_env_keys` 仅包含 `LOG_LEVEL`。Wasm 使用最小 WASI 环境读取，不引入文件系统或进程退出 ABI。

同类故障首次立即详细输出，之后每 30 秒汇总重复次数及最近原因，成功后记录恢复。聚合状态、日志队列与单条字段长度均有界，不按请求 ID 建聚合键。输出阻塞或队列满时允许丢日志，Observer/Consumer 的 `/metrics` 提供 `metering_logs_dropped_total`，Observer 另保留归属查询丢失计数。Wasm 日志在 tick 中、业务恢复处理之后输出，每轮最多 8 条；最多缓存 64 条，满时丢弃并在后续汇总。关闭或退出阶段可能丢失尚未输出的日志。

客户端初始化日志不代表外部连接成功；网络连接、Kafka 投递、Doris 可见性和汇总刷新各自记录实际结果。正常取消、正常关闭和空轮询超时不当作依赖故障。
