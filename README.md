# LLM Token Stats

大模型请求用量采集与统计项目。通过网关旁路观察请求和响应，提取模型返回的 token usage，经 Kafka 写入 Doris，保留请求明细、用量状态和消费恢复记录。目前支持 Chat Completions 的 JSON 与 SSE 响应。

业务请求直接经过网关到模型服务；采集使用独立的有界通道，采集失败时放行业务。未知用量保留 `null/unknown`，不按零用量计算；请求和响应正文只在采集过程中临时处理，不写入 Kafka、数据库或日志。

## 架构与组件职责

```mermaid
flowchart LR
    Client[调用方] --> Gateway[Istio / Envoy 网关]
    Gateway --> Model[模型入口服务 A]
    Auth[现有鉴权服务 ext_authz] -. 可信身份元数据 .-> Gateway
    Gateway --- Wasm[网关内 Wasm 插件]
    Gateway -. 加载时 HTTP 下载 Wasm .-> Observer
    Wasm -. 有界 gRPC 观察流 .-> Observer
    Observer -->|原生 Kafka Producer| Kafka
    Kafka --> Consumer
    Consumer -->|Stream Load / SQL 确认可见性| Doris
```

| 组件 | 职责 | 运行方式与实现情况 |
| --- | --- | --- |
| Istio / Envoy 网关 | 执行鉴权、路由和业务转发，向 Wasm 提供可信路由及身份元数据 | 复用现有入口网关；本地使用 `istio/proxyv2:1.13.5` 中的 Envoy |
| Gateway Observer Wasm | 当前按 POST 与精确路径白名单选择请求，`route_name: "*"` 忽略路由名；有界观察正文，必要时补充 `stream_options.include_usage`，旁路发送观察消息 | 已实现；加载在网关内，不是独立服务，也不直接连接 Kafka |
| Observer | 接收观察流，分配请求 ID，解析 JSON / SSE usage，生成 `request_started`、`usage_observed` 事件，通过原生 Producer 发送 Kafka | 已实现；入口为 `cmd/observer/`，提供 gRPC、健康检查、指标及独立 HTTP Wasm 下载端口 |
| Kafka | 缓冲结构化事件，按分区保存消费位置，为 Consumer 重放提供数据 | 外部基础设施；本地由 Compose 启动 |
| Consumer | 校验事件与摘要、去重并保留冲突证据；写入 Doris 明细、拒绝诊断、日期通知和批次凭据；全部可见后提交 offset | 已实现；入口为 `cmd/consumer/`，多个副本使用同一 consumer group 分摊分区 |
| Doris | 保存事件、请求视图、冲突标记和消费协调状态，支持 SQL 查询 | 已提供迁移；本地使用单 FE / BE，生产使用独立集群 |
| Fixture | 模拟可信 ext_authz 元数据和模型 JSON / SSE 响应 | 已实现；仅用于开发和测试，不提供真实鉴权或模型调用 |
| Auth、usage-worker、平台 API / 门户 | 分别承担密钥鉴权、日/月汇总刷新、查询与管理交互 | 仓库包含相关设计或目录，尚无可部署的完整实现；采集接入需复用已有鉴权能力 |

采集范围是模型入口服务 A，不重复统计 A 调用内部 B / C 的请求。观察 ACK 或响应中的计量 ID 不代表事件已经写入 Kafka / Doris；完整性还需要结合独立网关请求指标、Consumer 水位和缺口记录判断。

## 编译与构建镜像

### 工具与版本

在仓库根目录执行以下命令。主模块使用 Go **1.25.0**，插件使用 TinyGo **0.40.1** 和仓库内固定的 Proxy-Wasm SDK；Python 脚本需要 **3.10+**。`make test` 包含 Go 竞态测试，需要可用的 C 编译器。开发联调需要 Docker Engine 和 Docker Compose v2。

本地网关、Kafka、Doris 版本分别为 Istio proxyv2 **1.13.5**、Kafka **3.9.2**、Doris **4.1.3**。替换网关或 SDK 版本时，需要重新验证 ABI、hostcall 和过滤器行为。

```sh
make test       # 契约检查、Python 测试、主模块和独立插件模块的 Go 竞态测试
make build      # 生成 dist/observer、dist/consumer、dist/fixture
make wasm       # 生成 dist/gateway-observer-v1.wasm，并检查 ABI
make manifest   # 生成 dist/collection-manifest.json，记录工具版本与文件摘要
```

修改 Protobuf 后使用 `make generate` 重新生成代码；protoc 及插件的固定版本见[组件运行说明](docs/collection-components.md#协议生成与版本)。

### Observer 与 Consumer 镜像

两个 Dockerfile 的 Go 服务二进制都从源码构建；Observer 镜像另外复制宿主机 `make wasm` 生成的插件，构建镜像前必须先生成该文件。镜像版本统一由 `scripts/image_tag.py` 生成：当前提交有 Git tag 时使用该 tag，没有时使用至少 12 位 commit 短 hash，例如 `observer:v1.2.3` 或 `observer:2d36b71036b8`。同一提交有多个 tag 时按 Git 版本排序取最高项；历史提交的 tag 不用于当前版本。Git tag 必须符合 Docker tag 字符与长度限制，不兼容时直接报错。以下以 `linux/amd64` 为例，镜像仓库请按实际发布环境设置：

```sh
export IMAGE_REGISTRY=registry.example.com/llm-token-stats
export IMAGE_TAG="$(python3 scripts/image_tag.py)"
export OBSERVER_IMAGE="$IMAGE_REGISTRY/observer:$IMAGE_TAG"
export CONSUMER_IMAGE="$IMAGE_REGISTRY/consumer:$IMAGE_TAG"

make wasm
docker build --platform linux/amd64 -f deploy/observer/Dockerfile -t "$OBSERVER_IMAGE" .
docker build --platform linux/amd64 -f deploy/consumer/Dockerfile -t "$CONSUMER_IMAGE" .
docker push "$OBSERVER_IMAGE"
docker push "$CONSUMER_IMAGE"
```

### Wasm 打包、下载与摘要

Observer 镜像将同一份 `dist/gateway-observer-v1.wasm` 放在 `/usr/share/metering/wasm/gateway-observer-v1.wasm`。根目录 `.dockerignore` 只放行这份 Wasm，不将其他构建产物送入镜像。网关复用已有 Istio 镜像，通过 Observer 下载插件；插件随 Observer 镜像发布，无需独立 Deployment。

生产配置启用以下可选下载服务；省略 `wasm_download` 时保留原有行为：

```json
"wasm_download": {
  "listen": ":9093",
  "file": "/usr/share/metering/wasm/gateway-observer-v1.wasm"
}
```

接口为 `GET/HEAD /wasm/gateway-observer-v1.wasm`，只提供这一文件。默认集群地址为 `http://metering-observer.metering.svc.cluster.local:9093/wasm/gateway-observer-v1.wasm`。下载服务不调用 Kafka，最多 8 个并发下载，超限返回 503；读头超时 3 秒、写超时 30 秒。启用时文件不可读、Wasm 文件头非法或端口绑定失败会阻止 Observer 启动。

启动本地栈后，下载并核对实际内容；生产可先在另一个终端执行 `kubectl -n metering port-forward service/metering-observer 19093:9093`，再使用相同命令：

```sh
curl --noproxy '*' --fail --output dist/gateway-observer.download.wasm \
  http://127.0.0.1:19093/wasm/gateway-observer-v1.wasm
sha256sum dist/gateway-observer-v1.wasm dist/gateway-observer.download.wasm
```

两份摘要必须相同。渲染器读取打包时的同一份 Wasm 计算 `code.remote.sha256`，配置 HTTP 下载集群及 5 秒下载超时，不额外重试。这里使用固定版本 Envoy 的原生远程加载；现有 EnvoyFilter 不依赖 Istio agent 重写成文件路径。下载只发生在插件加载时，不进入逐请求的业务转发流程，首次加载仍有下载等待。

推送完成后记录两个服务镜像的仓库 digest，部署时使用完整的 `仓库地址@sha256:摘要`：

```sh
docker image inspect --format '{{json .RepoDigests}}' "$OBSERVER_IMAGE"
docker image inspect --format '{{json .RepoDigests}}' "$CONSUMER_IMAGE"
```

Kafka、Doris 使用基础设施镜像；Fixture 由本地 Compose 挂载编译产物运行，无需发布生产镜像。

## 开发环境一键启动与测试

工具安装完成、Docker 可用后，执行：

```sh
make test build wasm local-up local-test
```

该命令依次完成检查、编译和 Wasm ABI 验证，启动 Kafka、Doris、Fixture、Observer、Consumer、Envoy，然后执行真实链路集成测试。`make local-up` 会等待基础设施就绪，应用 `001/002/004/005/006` Doris 迁移，并创建 24 分区、72 小时保留的测试 topic。它本身不编译代码，会重新渲染下载摘要，重启使用挂载产物的 Fixture、Observer、Consumer，校验下载文件后重建网关。网关只挂载生成的配置，不挂载 Wasm。配置与编排位于 [deploy/local/](deploy/local/compose.yaml)。

`make test` 不启动容器、不写 Doris；`make local-test` 验证实际 Envoy → Observer → Kafka → Consumer → Doris 链路，包括 JSON / SSE、未知用量、重放去重、冲突、未知 schema 暂停、写入确认丢失和超时边界。测试使用合成数据，也会留下诊断记录。

本地发布端口绑定 `0.0.0.0`，用于受控开发网络：

| 组件 | 宿主机端口 |
| --- | --- |
| 网关 / Envoy admin | `18080` / `19000` |
| Observer gRPC / 健康与指标 / Wasm 下载 | `19090` / `19091` / `19093` |
| Consumer 健康与指标 | `19092` |
| Kafka 外部客户端 | `29092` |
| Doris SQL / FE HTTP / BE HTTP | `19030` / `18030` / `18040` |
| Fixture 模型 HTTP / 模拟 ext_authz gRPC | `18081` / `19002` |

Kafka 默认公布自动识别的服务器 IPv4；多网卡或 NAT 环境在启动时指定客户端可达地址：

```sh
KAFKA_EXTERNAL_HOST=192.168.227.128 make local-up
```

手动发送合成请求、检查服务并查询入库结果：

```sh
curl -N -H 'Content-Type: application/json' \
  --data '{"model":"fixture","stream":true,"messages":[{"role":"user","content":"synthetic fixture"}]}' \
  http://127.0.0.1:18080/v1/chat/completions
curl --fail http://127.0.0.1:19091/readyz
curl --fail http://127.0.0.1:19092/readyz
curl --fail http://127.0.0.1:19091/metrics
curl --fail http://127.0.0.1:19092/metrics

# Consumer 默认最多等待 15 秒攒批，查询时允许相应写入延迟。
docker compose -f deploy/local/compose.yaml exec -T doris \
  mysql -uroot -h127.0.0.1 -P9030 -e \
  'SELECT event_type, usage_status, input_tokens, output_tokens FROM metering.events_raw ORDER BY ingested_at DESC LIMIT 10;'
```

DBeaver 使用 MySQL 驱动连接 `服务器IP:19030`，数据库 `metering`、用户 `root`、密码为空。明文 Kafka、空密码和单副本只适用于此隔离联调环境。

```sh
# 查看状态和日志
docker compose -f deploy/local/compose.yaml ps
docker compose -f deploy/local/compose.yaml logs --tail=100 observer consumer gateway

# 修改代码后重新构建；local-up 重算下载摘要并重启相关进程
make build wasm local-up
GOFLAGS='-count=1' make local-test

# 停止全部联调组件，保留数据
python3 scripts/local_collection.py stop
```

Kafka、Doris 数据保存在命名卷中；不要用 `down -v` 清理需要保留的数据。启动脚本会在迁移旧容器数据时先备份到 `.tmp/local-storage-*`。服务配置修改后需要重启进程；本地配置位置、迁移行为和恢复边界见[组件运行说明](docs/collection-components.md)。

## Kubernetes 生产部署

部署顺序为：**网关与鉴权前置能力 → Kafka / Doris → 数据库迁移 → 配置与 Secret → Consumer → Observer → 网关 Wasm 灰度启用**。以下命令用于在实际集群中渲染和发布采集组件；地址、账号、镜像及容量均需替换为环境实际值。

### 1. 准备网关与基础设施

| 组件 | Kubernetes 部署要求 |
| --- | --- |
| Istio / Envoy | 使用已验证的网关版本，准备独立灰度网关 Deployment；确保 `istio: ingressgateway` 标签与过滤器选择器一致。初始网关使用 2 Worker，实际并发数与热更新资源需验收 |
| ext_authz / Auth | 通过现有网关鉴权配置接入，返回 `api_key_id`、`customer_id`、`application_id` 的可信 dynamic metadata；仓库中的 Fixture 不用于生产 |
| Kafka | 使用平台维护的有持久卷、多 Broker 的集群或托管服务；配置 TLS / SASL、ACL 和监控。仓库未提供生产 Kafka 集群安装清单 |
| Doris | 使用 Doris Operator 或独立 FE / BE 集群，配置持久卷、备份、副本、动态分区与独立账号；[doris.yaml](deploy/doris/doris.yaml) 是单节点开发样例 |
| 网络与身份 | 网关访问 Observer gRPC（9090）和 Wasm HTTP 下载（9093）；Observer 仅生产指定 topic；Consumer 消费指定 topic/group，并访问 Doris SQL 和 Stream Load。通过 NetworkPolicy 与网格授权限制来源，放通必要 DNS 与基础设施流量 |

本文服务模板固定在 `metering` 命名空间，网关过滤器固定在 `istio-system`。若环境使用其他名称，需要同步修改模板、服务 DNS、Wasm 的 `observer_cluster`，以及渲染参数 `--wasm-url`、`--wasm-cluster`。按现有 Istio 安装方式为 `metering` 配置 sidecar 注入和 mTLS；Observer 自身的 gRPC 和下载 HTTP 监听不提供 TLS，网关到 Observer 的认证与加密由网格承担。模板没有自动创建这些网格和网络策略。

### 2. 初始化 Kafka 与 Doris

在具有管理权限的 Kafka 客户端环境创建 topic。`KAFKA_BOOTSTRAP` 是真实集群地址，`--command-config` 指向安全挂载的客户端认证配置：

```sh
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --command-config /run/secrets/kafka/admin.properties \
  --create --if-not-exists --topic llm-metering-events \
  --partitions 24 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=259200000 --config cleanup.policy=delete

kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --command-config /run/secrets/kafka/admin.properties \
  --describe --topic llm-metering-events
```

上述副本配置需要至少 3 个可用 Broker；24 分区是容量验收初值。同一采集部署周期不在线调整分区数。为 Observer 配置 topic 生产权限，为 Consumer 配置 topic 消费和 group 权限；现有 topic 的参数要另行核对，`--if-not-exists` 不会更新已有配置。

Doris 新库按 `001 → 002 → 004 → 005 → 006` 顺序执行迁移。仓库 SQL 的副本数为开发值 1，需先生成经过审查的生产副本；以下示例改为 3，要求实际 BE 数量和存储资源满足条件：

```sh
export DEPLOY_DIR="$PWD/.private/deploy"
install -d -m 700 "$DEPLOY_DIR/doris"
python3 - <<'PY'
import os
import re
from pathlib import Path

target = Path(os.environ['DEPLOY_DIR']) / 'doris'
for name in ('001_init.sql', '002_usage_summaries.sql',
             '004_summary_coordination.sql', '005_consumer_recovery.sql',
             '006_optional_route_identity.sql'):
    sql = (Path('migrations/doris/metering') / name).read_text()
    sql = re.sub(r'("replication_num"\s*=\s*)"1"', r'\g<1>"3"', sql)
    (target / name).write_text(sql)
PY

# 在能够连接 FE SQL 的管理环境执行；认证配置由 Secret 安全提供。
for migration in 001_init.sql 002_usage_summaries.sql 004_summary_coordination.sql 005_consumer_recovery.sql 006_optional_route_identity.sql; do
  mysql --defaults-extra-file=/run/secrets/doris/admin.cnf \
    --init-command="SET time_zone = '+00:00'" < "$DEPLOY_DIR/doris/$migration" || exit 1
done
mysql --defaults-extra-file=/run/secrets/doris/admin.cnf \
  -e 'SHOW BACKENDS; SHOW TABLES FROM metering; USE metering; SHOW DYNAMIC PARTITION TABLES;'
```

确认每个迁移成功后再部署 Consumer。已有库的保留策略调整使用 [003 迁移](migrations/doris/metering/003_raw_retention_one_year.sql)，执行前停止冲突的归档或分区清理任务；`CREATE TABLE IF NOT EXISTS` 不修改已有表。所有 FE 的 `dynamic_partition_enable=true` 应持久化，并确认 BE Alive、当天及所需历史日期分区存在。副本和分桶等其他生产参数需单独审查，不能仅替换副本数就视为容量验收完成。

明细按 UTC 请求日期保留 365 个日期。日 / 月汇总已定义表和 SQL 模板，但执行这些迁移不会启动汇总刷新任务。

### 3. 准备服务配置与 Secret

```sh
kubectl create namespace metering --dry-run=client -o yaml | kubectl apply -f -
cp configs/observer.production.example.json "$DEPLOY_DIR/observer.json"
cp configs/consumer.production.example.json "$DEPLOY_DIR/consumer.json"
cp configs/gateway-observer.example.json "$DEPLOY_DIR/gateway-observer.json"
```

修改这三份配置，并保持注册信息一致：

| 配置 | 必须确认的内容 |
| --- | --- |
| Observer | Kafka Broker、CA 和认证环境变量名；`accepted_config_versions`、`accepted_adapter_ids`、`accepted_service_ids`；资源预算；`wasm_download` 的镜像内路径与下载端口 |
| Consumer | 同一 Kafka topic、稳定的 `group_id`、历史配置版本及服务白名单；Doris `fe_url`、`allowed_load_hosts`；攒批大小与时间 |
| Wasm | 规则 `route_name`（当前为 `*`，忽略路由名）、方法、精确路径、`service_id`、`adapter_id`、`config_version`、Observer cluster；保持默认关闭，通过渲染器显式启用 |

`allowed_load_hosts` 必须覆盖实际 FE 和所有可能接收重定向的 BE 地址及端口，且 Consumer 可达这些地址。配置只向登记地址发送 Stream Load 凭据，并拒绝 HTTPS 降级。首次使用新 consumer group 会登记 Kafka earliest；接管缺少起点记录的旧组需要明确 `initial_offsets`，不能随意更换 group 或跳到 latest。

由 Secret 管理系统在本地受保护目录准备以下文件，不把凭据填入 JSON 或提交仓库：

| 文件示例 | 内容 |
| --- | --- |
| `.private/secrets/kafka-ca.crt` | Kafka CA 证书 |
| `.private/secrets/observer-kafka-username`、`observer-kafka-password` | Observer 的 SASL PLAIN 账号与密码，无额外换行 |
| `.private/secrets/consumer.env` | Consumer 的 Kafka 与 Doris 凭据，键名见下例；文件权限设为 `0600` |

`consumer.env` 格式如下，所有占位值需由真实凭据替换：

```dotenv
KAFKA_USERNAME=替换为Consumer账号
KAFKA_PASSWORD=替换为Consumer密码
DORIS_DSN=用户名:密码@tcp(doris-fe.doris.svc.cluster.local:9030)/metering?timeout=5s&readTimeout=30s&writeTimeout=10s&interpolateParams=true&time_zone=%27%2B00%3A00%27
DORIS_USER=替换为StreamLoad用户名
DORIS_PASSWORD=替换为StreamLoad密码
```

Doris 使用独立最小权限账号，具备采集表所需的查询与写入权限，DSN 必须包含连接 / 读写超时与 UTC 会话设置。服务不负责运行迁移。Kafka 如需客户端证书，在配置中增加 `cert_file`、`key_file`，并将对应文件加入下述 TLS Secret。

```sh
export SECRET_DIR="$PWD/.private/secrets"

kubectl -n metering create secret generic metering-kafka-tls \
  --from-file=ca.crt="$SECRET_DIR/kafka-ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n metering create secret generic metering-kafka-sasl \
  --from-file=username="$SECRET_DIR/observer-kafka-username" \
  --from-file=password="$SECRET_DIR/observer-kafka-password" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n metering create secret generic metering-consumer-credentials \
  --from-env-file="$SECRET_DIR/consumer.env" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n metering create configmap metering-observer \
  --from-file=observer.json="$DEPLOY_DIR/observer.json" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n metering create configmap metering-consumer \
  --from-file=consumer.json="$DEPLOY_DIR/consumer.json" \
  --dry-run=client -o yaml | kubectl apply -f -
```

### 4. 部署 Consumer 与 Observer

使用构建时记录的真实 digest 替换以下示例值。部署模板已配置探针、资源预算和只读配置 / 证书挂载；私有仓库需要在集群配置 `imagePullSecrets`。

```sh
export OBSERVER_IMAGE_DIGEST='registry.example.com/llm-token-stats/observer@sha256:替换为实际摘要'
export CONSUMER_IMAGE_DIGEST='registry.example.com/llm-token-stats/consumer@sha256:替换为实际摘要'

python3 - <<'PY'
import os
import re
from pathlib import Path

target = Path(os.environ['DEPLOY_DIR'])
for component in ('observer', 'consumer'):
    image = os.environ[f'{component.upper()}_IMAGE_DIGEST']
    if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', image):
        raise SystemExit(f'{component}: 请填写真实镜像 digest')
    manifest = Path(f'deploy/{component}/{component}.yaml').read_text()
    manifest = manifest.replace(f'REPLACE_{component.upper()}_IMAGE_DIGEST', image)
    (target / f'{component}.yaml').write_text(manifest)
PY

kubectl apply -f "$DEPLOY_DIR/consumer.yaml"
kubectl -n metering rollout status deployment/metering-consumer --timeout=5m
kubectl apply -f "$DEPLOY_DIR/observer.yaml"
kubectl -n metering rollout status deployment/metering-observer --timeout=5m
kubectl -n metering get pods,svc
```

模板默认 Observer 2 副本、Consumer 1 副本。扩容 Consumer 时保留同一 group，实际并行度受 topic 分区数限制；Observer 需同时检查 HTTP/2 长连接的负载分布。配置和 Secret 更新后，使用 `kubectl rollout restart deployment/对应名称 -n metering` 重启服务。升级配置版本时先让 Observer 接受新旧版本，再更新网关，Consumer 保留在线数据窗口内的历史注册。

### 5. 核对下载服务并按 Pod 渲染 Wasm 配置

先确认 Observer 所有副本已使用目标镜像且下载内容与发布产物摘要一致，再通过现有 Istio 发布配置准备独立灰度网关，保留已有网关镜像、服务、端口、路由、鉴权与网格设置。以下假设其 Deployment 名为 `istio-ingressgateway-canary`；若名称不同请替换：

```sh
export GATEWAY_DEPLOYMENT=istio-ingressgateway-canary
kubectl -n istio-system rollout status deployment/"$GATEWAY_DEPLOYMENT" --timeout=5m
kubectl -n istio-system get pods -l istio=ingressgateway -o wide

# 从上述 Deployment 选取一个实际 Pod，替换名称。
export GATEWAY_POD=替换为灰度网关Pod名
export GATEWAY_POD_UID="$(kubectl -n istio-system get pod "$GATEWAY_POD" -o jsonpath='{.metadata.uid}')"

python3 scripts/render_gateway_filter.py \
  --config "$DEPLOY_DIR/gateway-observer.json" \
  --wasm-file dist/gateway-observer-v1.wasm \
  --wasm-url http://metering-observer.metering.svc.cluster.local:9093/wasm/gateway-observer-v1.wasm \
  --wasm-cluster 'outbound|9093||metering-observer.metering.svc.cluster.local' \
  --gateway-pod-name "$GATEWAY_POD" \
  --gateway-namespace istio-system \
  --gateway-pod-uid "$GATEWAY_POD_UID" \
  --output "$DEPLOY_DIR/envoyfilter.$GATEWAY_POD_UID.disabled.yaml"
kubectl apply -f "$DEPLOY_DIR/envoyfilter.$GATEWAY_POD_UID.disabled.yaml"
```

渲染器会验证 Wasm 配置及产物文件头、计算文件 SHA-256、写入实际 Pod UID、生成唯一 EnvoyFilter 名称，通过代理启动时的 `metadata.NAME/NAMESPACE` 精确匹配该 Pod；`--gateway-pod-name` 必填，`--gateway-namespace` 默认为 `istio-system`。不要直接 apply [EnvoyFilter 模板](deploy/gateway/envoyfilter.yaml)，也不要给整个 Deployment 共用一个实例 ID。Istio 1.13 的代理标签取自启动快照，Pod 启动后补加 UID 标签不能可靠匹配 EnvoyFilter，因此无需再给 Pod 添加 `metering-instance` 标签。

当前仅采集 `POST /v1/chat/completions`，不限制 VirtualService、HTTP 路由名或域名。入口使用以下规则，`route_name: "*"` 接受任意路由名及路由名不可读的情况，无需配置路由 metadata：

```json
{
  "routes": [{"route_name": "*", "enabled": true, "methods": ["POST"], "paths": ["/v1/chat/completions"], "service_id": "model-service-a", "adapter_id": "chat_completions_v1"}]
}
```

方法必须为 `POST`；路径去掉查询串后精确匹配，不做额外 URL 解码或大小写转换。其它方法、路径、尾斜杠和子路径全部忽略，不读取正文、不建立观察流、不发送事件。`*` 只支持作为完整 `route_name`，不表示路径或其它字段支持通配。全局和规则开关仍必须开启。

配置中的 `route_name: "*"` 仅控制筛选。事件的 `route_name` 单独记录请求头阶段可读的真实 Envoy HTTP 路由名；缺失、超过 128 UTF-8 字节或无效时记录 `unknown`，不使用通配值或计量 metadata 冒充真实路由。它仅用于诊断、审计，不作为客户归属、服务接纳、身份一致性或价格依据，也不代表 VirtualService 名称或实际后端。`service_id` 和 `adapter_id` 仍取自命中规则，当前目标请求统一归入 `model-service-a`。精确路由名筛选仍兼容，规则按配置顺序选择首先命中路由名的规则。

Observer / Consumer 使用 `"accepted_service_ids": ["model-service-a"]`，路由名缺失或变化不会导致拒收；配置版本、适配器和可信身份校验继续保留。旧 `accepted_routes` 仍可读取，但只提取其中的 `service_id`，不再按路由名准入；新旧两种清单不能同时配置。Kafka 历史事件中的 `*` 保留原文，可以正常重放，不会改写为真实路由名。

升级先暂停旧 Consumer，执行 [006 路由身份兼容迁移](migrations/doris/metering/006_optional_route_identity.sql)，更新 Consumer / Observer 及服务白名单，再发布 Wasm。新 `identity_hash` 排除 `route_name`，数据库视图和 Consumer 对所有新旧记录按相同身份字段比较，无需回填原始事件或旧摘要。同一 `event_id` 的不同内容仍保留冲突；既有持久冲突标记也不会自动撤销。

鉴权身份继续来自 `envoy.filters.http.ext_authz` 的 dynamic metadata，不接受客户端请求头代替。检查实际代理配置：

```sh
istioctl proxy-config all "$GATEWAY_POD" -n istio-system -o json \
  > "$DEPLOY_DIR/gateway-config-dump.json"
```

确认过滤器顺序为 **ext_authz → Wasm → router**，远程 Wasm 加载成功，HTTP 下载集群 `outbound|9093||metering-observer.metering.svc.cluster.local` 与 HTTP/2 采集集群 `outbound|9090||metering-observer.metering.svc.cluster.local` 均存在，网格 mTLS、网络策略与授权允许访问相应端口。集群名称依赖实际服务发现；Service 已创建或健康探针成功本身不足以证明下载和采集链路就绪。

使用 Istio `outbound|端口||服务DNS` 集群时，插件自动将 gRPC authority 设置为 `服务DNS:端口`，保留路由集群与 mTLS/SNI。自定义 ext_authz 配置也应显式设置 `grpc_service.envoy_grpc.authority` 为有效服务 DNS，不能让带 `|` 的集群名作为 HTTP/2 authority。

### 6. 启用、验证与回退

完成[验收清单](docs/gateway-collection-design-closure.md)中 G0 / G1 的环境、鉴权与协议验证后，显式启用该 Pod：

```sh
python3 scripts/render_gateway_filter.py \
  --config "$DEPLOY_DIR/gateway-observer.json" \
  --wasm-file dist/gateway-observer-v1.wasm \
  --wasm-url http://metering-observer.metering.svc.cluster.local:9093/wasm/gateway-observer-v1.wasm \
  --wasm-cluster 'outbound|9093||metering-observer.metering.svc.cluster.local' \
  --gateway-pod-name "$GATEWAY_POD" \
  --gateway-namespace istio-system \
  --gateway-pod-uid "$GATEWAY_POD_UID" --enable \
  --output "$DEPLOY_DIR/envoyfilter.$GATEWAY_POD_UID.enabled.yaml"
kubectl apply -f "$DEPLOY_DIR/envoyfilter.$GATEWAY_POD_UID.enabled.yaml"
```

通过灰度入口发送已授权的 JSON / SSE 测试请求，检查业务响应、Observer / Consumer 的 `/metrics`、Kafka group lag，以及 Doris 中对应的 `events_raw`、`requests` 和 `consumer_batch_receipts`。Observer 健康端口为 `9091`，Consumer 为 `9092`，均提供 `/livez`、`/readyz`、`/metrics`；可通过 `kubectl port-forward` 从受控终端访问。`readyz` 成功仍需结合真实请求入库验证。

Consumer 默认按分区最多 5000 条 / 4 MiB，或等待 15 秒后攒批写入。Doris 不可用时暂停相应分区并保留冻结批次；未知 schema 同样暂停，不跳过。恢复位置落后于 Kafka earliest 时记录缺口并暂停，需按[恢复说明](docs/collection-components.md#consumer-恢复与统计边界)处置，不能用 latest 掩盖丢失数据。

服务只提供当前镜像内的一个版本。升级先发布 Observer，等待所有副本下载摘要一致，再更新网关 URL／摘要配置；滚动期间旧摘要可能下载失败，允许暂时缺少采集。已加载插件可继续运行，旧网关重启不保证还能下载旧文件。恢复服务后需触发插件重新安装或重启网关，并按实际 Pod UID 重新渲染配置；原样重复 apply 同一配置不保证触发下载，不承诺自动恢复。版本回退须先恢复对应 Observer 镜像，再恢复对应网关配置。

需要立即停止采集时，先重新应用同一 Pod 的关闭配置；若模块加载或 ABI 错误导致配置无法生效，再移除该 Pod 专属过滤器：

```sh
kubectl apply -f "$DEPLOY_DIR/envoyfilter.$GATEWAY_POD_UID.disabled.yaml"
# 必要时移除采集过滤器，并核验原有鉴权和业务路由仍有效。
kubectl -n istio-system delete envoyfilter "gateway-observer-$GATEWAY_POD_UID"
```

**每个网关 Pod 都需要独立渲染；Pod 重建、扩容后要获取实际 Pod 名称及 UID，重新渲染并发布对应过滤器，同时清理旧 UID 的资源。** 当前仓库未提供自动执行此流程的控制器，需要纳入实际发布流程。关闭配置用于停止采集，`fail_open` 不能保证底层模块加载失败时网关一定成功启动。

本地集成通过不代表生产发布验收完成。扩大灰度前执行真实环境的改写、解析与故障恢复验证，生产验收还需完成性能、数据质量和回退检查；完整 G0–G5 要求见[闭环与验收清单](docs/gateway-collection-design-closure.md)，已有本地证据见[验证记录](docs/spec/plan/gateway-collection-verification.md)。

## 本地 Kubernetes / Istio 部署验证

已将 **Kubernetes 1.22.17 + Istio 1.13.5** 实际部署验证固化为用例 **K8S-WASM-001**。Observer 两副本、Consumer、独立测试网关和合成鉴权/模型 fixture 均部署到 Kubernetes；Kafka、Doris 复用本地 Compose。每次使用独立 Kafka topic 和消费组，不依赖 Compose Consumer。原 `istio-system` 网关不参与发布。

```sh
# 仅启动 Kafka / Doris 并初始化本地迁移；已就绪可跳过。
make local-infra-up
# 构建并实际部署，验证 JSON、SSE、unknown、mTLS 下载及独立 Consumer 入库。
make k8s-test
```

默认 context 为 `kind-kind`，测试命名空间为 `metering-wasm-verify`。Observer、Consumer、fixture 镜像统一使用 Git tag 或 commit 短 hash 作为版本；随机 run-id 继续用于测试 topic、消费组及诊断目录。测试会保留资源与 `.tmp/k8s-collection/` 下的结果，重复执行会清理归属于本测试的旧环境。网关仍使用原始 `istio/proxyv2:1.13.5`，没有挂载或打包 Wasm。完整前置条件、断言与自定义参数见[测试用例](docs/kubernetes-collection-test.md)，实际结果见[验证记录](docs/spec/plan/gateway-collection-verification.md)。

```sh
kubectl --context kind-kind -n metering-wasm-verify get deployment,pod,svc
kubectl --context kind-kind -n metering-wasm-verify port-forward service/wasm-gateway 28080:8080
```

保持端口转发，在另一个终端发送合成 SSE 请求：

```sh
curl --noproxy '*' -N -H 'Host: wasm-verify.local' -H 'Content-Type: application/json' \
  --data '{"model":"fixture","stream":true,"messages":[{"role":"user","content":"synthetic Kubernetes verification"}]}' \
  http://127.0.0.1:28080/v1/chat/completions
```

该入口只用于合成验证，不提供真实 API Key 鉴权或模型调用。清理脚本删除本次测试命名空间及独立 Kafka topic / 消费组，保留共享 Compose 数据卷、Doris 历史测试记录和原网关：

```sh
make k8s-clean
# 或直接运行独立脚本；重复执行安全。
python3 scripts/cleanup_k8s_collection.py
```

## 目录与文档

| 目录 | 内容 |
| --- | --- |
| `cmd/`、`internal/`、`pkg/` | 服务入口、内部实现与共享代码 |
| `plugins/gateway-observer/`、`third_party/proxy-wasm-go-sdk/` | 独立 Go 模块的 Wasm 插件及固定 SDK 扩展 |
| `api/`、`gen/` | Protobuf、事件 Schema 与生成代码 |
| `configs/`、`deploy/`、`migrations/` | 配置样例、镜像与部署模板、数据库迁移 |
| `tests/`、`testdata/`、`scripts/` | 单元与集成测试、合成样例、构建及运维辅助脚本 |
| `docs/`、`docs/spec/plan/` | 架构设计、实现计划与验证记录 |
| `dist/` | 忽略提交的二进制、Wasm 与摘要清单 |

- [组件运行说明：配置、协议、资源及恢复边界](docs/collection-components.md)
- [网关采集实现方案](docs/gateway-request-collection-implementation.md)
- [Kafka 事件接入说明](docs/kafka-event-integration-guide.md)
- [Doris 部署与迁移说明](deploy/doris/README.md)
- [日 / 月汇总设计](docs/daily-monthly-summary-design.md)、[Doris 协调设计](docs/doris-summary-coordination.md)
- [API Key 鉴权设计](docs/api-key-auth-service-implementation.md)
