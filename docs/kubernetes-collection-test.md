# 本地 Kubernetes / Istio 采集部署测试

用例编号：**K8S-WASM-001**。固定环境为 **Kubernetes 1.22.17 + Istio 1.13.5**，通过真实控制面验证网关远程下载 Wasm、旁路采集和 Consumer 入库。脚本位于 [k8s_collection.py](../scripts/k8s_collection.py)，测试清单位于 [deploy/k8s-test](../deploy/k8s-test/fixtures.yaml)。这项本地合成测试不代表生产 G0–G5 验收。

## 拓扑与前置条件

```text
合成请求 → Kubernetes 测试网关 → Kubernetes fixture（合成鉴权与模型）
                     │ 下载 Wasm / gRPC 旁路
                     ↓
             Kubernetes Observer × 2
                     ↓
              Compose Kafka（独立测试 topic）
                     ↓
             Kubernetes Consumer × 1
                     ↓
                 Compose Doris
```

- 已安装的 kind 集群、Istio 控制面和 `gateway` 注入模板；默认 context 为 `kind-kind`，kind 集群名为 `kind`。脚本检查 Kubernetes / Istio 版本，版本不符则退出。
- `kubectl`、`istioctl`、`kind`、Docker Engine / Compose v2，以及仓库规定的 Go 1.25.0、TinyGo 0.40.1、Python 3.10+。
- Pod 可访问宿主机 Kafka `29092`、Doris SQL `19030`、Doris BE HTTP `18040`。Kafka 的 EXTERNAL advertised 地址必须为 Pod 可达的宿主机 IPv4 地址或域名，不能是 `localhost`、`127.*` 或 `0.0.0.0`。
- 复用仓库本地 Compose 的明文 Kafka 与 Doris root 空密码配置，仅适用于开发环境。Consumer 凭据以测试命名空间 Secret 注入。直接使用已发布的 BE HTTP 端口，避免 all-in-one FE 重定向到容器内地址。

## 执行

在仓库根目录运行：

```sh
# 只启动共享 Kafka、Doris，等待就绪并执行本地 Doris 迁移。
# 首次运行或宿主机地址变化时，显式设置 Pod 可达的 IP。
KAFKA_EXTERNAL_HOST=192.168.227.128 make local-infra-up
make k8s-test
```

共享基础设施已运行且迁移完成时，可直接 `make k8s-test`。无需启动 Compose Observer、Consumer、Fixture 或 Envoy；如果它们已经运行，也可以保持运行。该用例为每次执行生成独立的 `llm-metering-k8s-<run-id>` topic 和消费组，使用 3 个分区、24 小时保留期，避免 Compose Consumer 参与测试数据消费。不会复用 `llm-metering-events` 或 `llm-metering-doris-v1`。

`make k8s-test` 编译二进制和 Wasm、构建并导入 Observer / Consumer / fixture 测试镜像，然后在 `metering-wasm-verify` 命名空间部署。网关使用原始 `docker.io/istio/proxyv2:1.13.5`，不挂载 Wasm。重复执行会先清理同名且带有本测试归属标签的旧测试环境；无归属标签时拒绝删除。原 `istio-system` 网关不参与发布。

三个测试镜像的版本统一取当前 HEAD 上的 Git tag，无 tag 时取至少 12 位 commit 短 hash，例如 `metering-observer:v1.2.3` 或 `metering-observer:2d36b71036b8`。同一提交有多个 tag 时按 Git 版本排序取最高项，轻量 / 附注 tag 均支持；不使用历史提交的 tag。不符合 Docker tag 限制的 Git tag 会使构建失败。版本解析由 [image_tag.py](../scripts/image_tag.py) 提供，与 README 的服务镜像构建命令一致。随机 run-id 仅用于测试隔离，镜像版本及完整镜像名另存于 `images.json` 和成功结果 `result.json`。

可指定另一个已安装相同版本 Istio 的 kind 集群及测试命名空间：

```sh
make k8s-test K8S_TEST_ARGS='--context kind-dev --kind-cluster dev --namespace metering-wasm-dev'
```

默认保留测试资源便于检查。脚本创建的临时端口转发在成功、失败或中断时关闭；测试失败返回非零并保留诊断。需要手工请求时：

```sh
kubectl --context kind-kind -n metering-wasm-verify get deployment,pod,svc
kubectl --context kind-kind -n metering-wasm-verify port-forward service/wasm-gateway 28080:8080
```

在另一个终端发送合成 SSE 请求：

```sh
curl --noproxy '*' -N -H 'Host: wasm-verify.local' -H 'Content-Type: application/json' \
  --data '{"model":"fixture","stream":true,"messages":[{"role":"user","content":"synthetic Kubernetes verification"}]}' \
  http://127.0.0.1:28080/v1/chat/completions
```

## 断言与结果

| 检查 | 通过条件 |
| --- | --- |
| 部署 | Observer 两副本、Consumer、fixture、网关共 5 个 Pod Ready，容器重启次数为 0 |
| 镜像与下载 | 原始 Istio 网关镜像无 Wasm 挂载；两个 Observer 镜像内文件及从 Service 实际下载的 SHA-256 与构建产物一致 |
| 网格 | Observer 入站 STRICT mTLS 下，9093 下载、9090 gRPC 及实际采集入库成功 |
| 配置收敛 | 已激活监听器内 `code.remote` 摘要及 enabled 状态正确，等待 XDS 收敛后才发验收请求 |
| Pod 匹配 | 错误 NAME 不安装采集过滤器，业务健康请求仍为 200；恢复实际 Pod NAME / NAMESPACE 后重新加载 |
| 普通 JSON | HTTP 200，Doris 中恰有两条对应事件；用量 known、输入 10、输出 4、complete、usage_eligible=1 |
| SSE | HTTP 200，返回 `[DONE]`，Doris 事件与正常 JSON 用量一致；fixture 要求 include_usage，间接验证网关改写 |
| SSE 缺 usage | HTTP 200，事件 unknown，输入 / 输出 NULL，complete、usage_eligible=0 |
| 数据来源 | 三个请求的 gateway_instance_id 为实际网关 Pod UID，producer_id 属于 Kubernetes Observer，kafka_topic 为本次独立 topic |
| 独立 Consumer | 3 个分区无积压，已有记录的分区提交位点合计为 6（空分区允许尚无提交）；Doris 有对应分区起点及可见批次回执；缩容 Kubernetes Consumer 至 0 后该组为 Empty / 0 成员，恢复至 1 后为 Stable / 1 成员且原消费位点保持 |
| 原网关隔离 | 测试前后 `istio-system` 原网关 Pod 名称、UID 和容器重启次数一致 |

kind 到 Compose 的连接经过 SNAT，broker 记录的 HOST 可能是 Docker 网桥地址。用例通过 Kubernetes Consumer 实际停止 / 恢复与消费组成员数的对应变化核验归属，不用 broker 所见地址推断 Pod IP。

结果在 `.tmp/k8s-collection/<context>/<namespace>/<run-id>/`，包括 `result.json`、请求与 logical_request_id、实际清单、config_dump、Consumer 位点、停止 / 恢复的消费组状态和 Doris 回执。失败时另保存 Pod、事件、describe 和容器日志；均为已忽略的本地文件。最近一次测试的清理信息保存在同级 `state.json`。

## 清理

```sh
make k8s-clean
# 等价的独立清理脚本
python3 scripts/cleanup_k8s_collection.py
# 自定义环境必须使用与执行时相同的参数
make k8s-clean K8S_TEST_ARGS='--context kind-dev --kind-cluster dev --namespace metering-wasm-dev'
```

[清理脚本](../scripts/cleanup_k8s_collection.py)校验测试命名空间前缀与归属标签，先删除整个测试命名空间（包括 Consumer、网关、Observer、fixture、过滤器、ConfigMap、Secret），再删除该次独立 Kafka 消费组及 topic。可重复执行；如果命名空间已被手工删除，使用本地状态继续清理 Kafka。该脚本也支持清理前期同名且带有测试归属标签、尚无 run-id 的临时验证命名空间。

清理保留原网关、Compose 服务和数据卷、原有 topic / 消费组、Doris 历史测试行及本地结果文件；Doris 中的测试行通过请求 ID / 独立 topic 追溯，不执行共享数据库删行。手工启动的 port-forward 由调用终端停止。
