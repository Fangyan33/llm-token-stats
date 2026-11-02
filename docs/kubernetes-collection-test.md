# Kubernetes 采集端到端测试

统一入口为 `make k8s-test`，环境固定为 Kubernetes 1.22.17 + Istio 1.13.5。该入口依次执行原 K8S-WASM-001 部署验收和原本地 Go 集成测试；`make local-test` 已移除。测试全部使用合成流量，不代表生产 G0–G5 验收。

## 环境与拓扑

需要 Go 1.25.0、TinyGo 0.40.1、Python 3.10+、Docker、kind、kubectl、istioctl，以及已安装 Istio gateway 注入模板的 kind 集群。默认 context 为 `kind-kind`，集群名为 `kind`，隔离命名空间为 `metering-wasm-verify`。

```text
隔离 Kubernetes 命名空间
  Istio 测试网关（1→2 副本）→ fixture
             │ 远程 Wasm 下载 / gRPC 旁路
             ↓
       Observer × 2 → Kafka → Consumer → Doris

  集成测试 Job → 独立 Envoy Pod → 合成上游
             └→ e2e 网关 / Observer / Consumer / fixture
                         └→ 同一命名空间内 Kafka / Doris
```

Kafka、Doris 也在本次命名空间内启动，不连接宿主机端口，不依赖 Compose、`KAFKA_EXTERNAL_HOST` 或旧联调数据。测试数据随命名空间删除；无持久卷、hostPath、hostNetwork 或 Docker socket 挂载。Kafka 明文、Doris root 空密码仅用于隔离合成测试。

原 Istio 网关部署验收保留 STRICT mTLS、真实 XDS、EnvoyFilter 选择器和网关扩容检查。Go 故障回归使用关闭 sidecar 注入的独立 Envoy Pod，以便精确注入下载、暂停恢复及连接故障；这些 Pod 使用相同版本的真实 Envoy，其故障断言不等同于 Istio 控制面故障验收。

## 执行

```sh
make test k8s-test
# 使用另一个已安装相同版本 Istio 的 kind 集群：
make k8s-test K8S_TEST_ARGS='--context kind-dev --kind-cluster dev --namespace metering-wasm-dev'
```

脚本编译服务、Wasm 和 Go 测试二进制，构建 Observer、Consumer、fixture、integration 四个测试镜像，将这些镜像及 Kafka、Doris、Istio proxyv2 导入 kind，再启动测试环境。镜像版本取当前 HEAD 的 Git tag，无 tag 时取至少 12 位 commit 短 hash；相同提交的本地修改会重新构建并导入。

命名空间必须以 `metering-wasm-` 开头。重复执行只清理带有本测试归属标签的同名环境；没有归属标签时拒绝删除。原 `istio-system` 网关不参与发布，测试前后核对其 Pod UID、名称和重启次数。

Job 的 ServiceAccount 只有本命名空间内 Pod、ConfigMap 的创建、读取、列举、删除及 Pod 日志读取权限。临时 Envoy 配置由 ConfigMap 提供；改写故障测试通过 initContainer 将本次 Wasm 复制到 emptyDir，下载故障测试使用远程 Wasm。Job 不编译代码、不调用 Docker，不带宿主机 kubeconfig。

## 覆盖范围

| 场景 | 断言 |
| --- | --- |
| Istio 部署与下载 | 两个 Observer，远程下载摘要与构建一致；STRICT mTLS 下旁路及入库；只接受激活监听器中的 Wasm |
| EnvoyFilter 与扩容 | 错误 selector / NAMESPACE 不安装插件且健康请求正常；工作负载隔离；1→2 副本共享原过滤器，18 条事件入库、消费追平；原网关不变 |
| 正常采集 | JSON、SSE、缺失 usage；改写、事件来源、实际路由名、独立 topic、Doris 可见回执及 offset 正确 |
| 下载故障及恢复 | 不可用、404、超时、摘要不符时业务 200、正文摘要不变且无采集；修复配置、重建测试 Envoy Pod 后恢复采集 |
| 改写 fail-open | HTTP/1、HTTP/2 下正常改写、非法 JSON、超限、trailers、正文停顿；原始正文和 trailer 保留，尾部未发送时上游已收到原始前缀 |
| 正文边界与超时 | 256 KiB、边界加一、1 MiB、慢正文、延迟响应；业务正文摘要不变，未知用量为 NULL |
| Kafka / Doris 故障 | Producer 黑洞下 Emit 有界；未知 schema 不推进 offset；真实 BE 已写入但确认丢失时按可见 label 恢复 |
| 批量消费与恢复 | 5000 行、字节上限、空闲分区刷批、双实例 rebalance、退出后部分批次重放；无漏行或重复行，offset 正确 |
| 请求与事件身份 | 客户端 ID 优先级、非法/重复头、同 ID 重试仍有独立 observation_id；重放去重、身份及不可变快照冲突 |
| 归属与名称缓存 | 合成 HTTP/HTTPS 接口、名称更新、TTL、失败时旧值、401 失效；Kafka 与 Doris 一致 |
| Host 与隐私 | Host 白名单、非法/重复 Bearer；事件、Kafka key/value 和四个组件日志中没有凭据 |

Go 测试保留在 `tests/integration/`，由 Job 执行，禁止直接在宿主机运行。部分 Consumer / Observer 故障在 Job 进程内启动真实 Go 组件，与真实 Kafka、Doris 交互，便于控制 rebalance、黑洞和确认丢失；这些测试与独立 Deployment 的部署验收互补。

Istio 部署验收使用独立的 `llm-metering-k8s-<run-id>` topic / 消费组和 `metering_k8s_<run-id>` 库；迁移场景使用同一隔离命名空间内的 `llm-metering-development` 和 `metering_development`，额外 topic / 消费组按场景随机生成。

## 结果与检查

结果在 `.tmp/k8s-collection/<context>/<namespace>/<run-id>/`，包含版本、镜像、实际清单、config_dump、请求、Doris 回执、消费位点、网关扩容结果、`integration.log` 和最终 `result.json`。只有部署验收与 Go Job 都通过才写整体 `status=passed`；Job 不自动重试，失败返回非零并保留诊断。

2026-11-02 在 `kind-kind` / `metering-wasm-verify` 完成全量验证：部署验收及 18 项顶层 Go 集成测试全部通过，含子测试共 61 个通过条目，无跳过；Kafka、Doris 均为零重启，原网关未变化。HTTP/1、HTTP/2 正文超时分别在约 461 ms、454 ms 恢复原始前缀转发。结果目录为 `.tmp/k8s-collection/kind-kind/metering-wasm-verify/d98fef6deb49/`。

该测试 Doris 的容器限额为 3 GiB，使用 FE 512 MiB / BE 256 MiB JVM 堆、BE 50% 进程内存上限，并将可用内存低水位上限设为 256 MiB，避免默认水位阻塞小容器的 Stream Load。该预算仅服务于本组本地合成测试。

```sh
kubectl --context kind-kind -n metering-wasm-verify get deployment,pod,job,svc
kubectl --context kind-kind -n metering-wasm-verify logs job/collection-integration
kubectl --context kind-kind -n metering-wasm-verify port-forward service/wasm-gateway 28080:8080
```

在另一终端发送合成请求：

```sh
curl --noproxy '*' -N -H 'Host: wasm-verify.local' -H 'Authorization: Bearer synthetic-test-key' -H 'Content-Type: application/json' \
  --data '{"model":"fixture","stream":true,"messages":[{"role":"user","content":"synthetic Kubernetes verification"}]}' \
  http://127.0.0.1:28080/v1/chat/completions
```

## 清理

```sh
make k8s-clean
# 等价命令：
python3 scripts/cleanup_k8s_collection.py
# 自定义环境使用与执行时相同的参数：
make k8s-clean K8S_TEST_ARGS='--context kind-dev --kind-cluster dev --namespace metering-wasm-dev'
```

清理校验命名空间前缀、归属标签和本地状态，删除整个测试命名空间，包括 Kafka、Doris、Job、临时 Envoy 和全部测试数据。其他命名空间、既有 Compose 环境及本地诊断文件保留。脚本创建的端口转发在成功、失败或中断时关闭，手工 port-forward 由调用终端停止。

VM / Envoy 自身崩溃、节点故障、生产第三方归属服务、生产容量和长期稳定性不在本组本地合成测试的验收范围内。
