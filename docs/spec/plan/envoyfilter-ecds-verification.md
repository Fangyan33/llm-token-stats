# EnvoyFilter ECDS 与块式 YAML 验证记录

## 最终行为

- Kubernetes EnvoyFilter 名称保持固定；启用态通过 ECDS 加载 Wasm，提供 Lua 透传默认值、立即使用默认配置和代码缓存缺失 NACK。关闭态直接使用 Lua，无下载依赖。
- 清单及插件参数均为块式 YAML。插件参数使用 `google.protobuf.Struct`；Wasm 使用有界、无反射的 Protobuf 解码，继续接受旧 JSON 输入，字段及 `schema_version=1` 不变。
- 保留命名空间、工作负载及鉴权顺序隔离。重推只原子切换指定资源的 ECDS 扩展/订阅名称，以 resourceVersion 防并发覆盖，不重启网关、不改变 VM 初始化参数。默认每 15 秒重试，最多 120 秒。
- 目标网关关闭 Agent 远程下载转换，保留 Istio outbound 集群的 mTLS；导出 ECDS 指标。加载计数仅证明配置安装，SSE 改写与实际事件入库另行验证。

## 运行时与构建调整

早期实现在原始 proxyv2:1.13.5 / Envoy 1.21.3-dev 复现 ECDS 更新段错误；改用独立订阅后，依赖 panic/recover 的 yaml.v3 又在 TinyGo/Wasm 中触发不可恢复 trap。因此最终实现不在 VM 内解析 YAML，改用有界 Struct 解码。早期失败不能证明最终实现仍会崩溃，也不能据此断言整个 1.13.x 无法规避。上游相关生命周期问题见 [Envoy #14930](https://github.com/envoyproxy/envoy/issues/14930)，尚未通过 ASAN 确认本地故障与其同根因。

生产模板暂时保留排除 1.13.x 的版本匹配；`--proxy-version` 只在隔离测试资源中覆盖该限制。旧代理应用原生产模板时继续转发，不安装新采集插件。

候选数据平面使用原始 `istio/proxyv2:1.14.6`，实际 Envoy 为 `a757eae4ff3ad133fc75efa953e6642436316f98/1.22.7-dev`。TinyGo 0.40.1 使用 `-buildmode=wasi-legacy`，避免命令入口返回后调用受限 `proc_exit`；ABI 检查拒绝套接字、文件系统和进程退出导入。旧 JSON 与 Struct 均通过真实 Envoy 离线初始化检查。

## 1.14.6 候选验证结果

- `GOFLAGS='-p=1' make test`：契约、104 项 Python 测试、主模块及独立插件 Go 竞态测试通过。
- `make wasm`：构建与 ABI 检查通过；最终 Wasm SHA-256 为 `a1879c12891867971aab75ec51981883349cb6dff151449b5dca15df306818ae`。
- `make k8s-test K8S_TEST_ARGS='--context kind-kind --namespace metering-wasm-ecds-final'`：完整通过；21 个预期 Go 顶层测试全部 PASS，无跳过/失败。
- 新增 7 类真实 ECDS 故障：延迟下载、404、下载超时、错误摘要、下载不可用、损坏模块、非法 Struct 字段类型。均验证业务正文/响应及无采集；延迟下载与非法配置修复后原地恢复 SSE 改写、已知用量入库。热更新拒绝保留旧有效配置，关闭态无需下载。
- 网关 1→2 副本后显式重推激活，同一 Kubernetes 资源和选择器，18 条正常验收事件消费完成。两个采集网关无意外重启；原 `istio-system` 网关身份与重启次数不变。

本地诊断：`.tmp/k8s-collection/kind-kind/metering-wasm-ecds-final/c9a3983ea7d4/`，包含 `result.json`、完整 Job 日志、故障结果、扩容结果和原网关前后快照。早期失败诊断也保留，候选失败包标记为不可部署。

## 1.13.5 当前集群复测

2026-11-03，在当前 `kind-kind` 集群新建 `metering-wasm-ecds-1135` 隔离命名空间。控制面、隔离 Istio 网关、独立集成网关及所有临时 Envoy 均使用 1.13.5，实际网关 Envoy 为 `725ec273d665639aa9f6e0e9014af562a457c2f4/1.21.3-dev`。继续使用上述 SHA-256 的同一份 Wasm，未为旧版本另行构建插件。测试仅覆盖其拥有的 EnvoyFilter 版本匹配，确保实际执行 ECDS，而非插件未安装时的透传。

本轮执行：

```sh
GOFLAGS='-p=1' make build k8s-test-build
python3 scripts/check_wasm_abi.py dist/gateway-observer-v1.wasm
python3 -m unittest discover -s tests -p 'test_*.py'
python3 scripts/k8s_collection.py test --context kind-kind --namespace metering-wasm-ecds-1135 --proxy-version 1.13.5
```

- 105 项 Python 测试、构建及 ABI 检查通过；整体 `result.json` 为 `status=passed`。
- 21 项预期 Go 顶层测试全部 PASS，含子测试共 65 个 PASS 条目，无跳过或失败。
- 7 类 ECDS 故障全部通过：下载延迟、404、超时、错误摘要、下载不可用、损坏模块及非法 Struct 字段类型。故障时业务正常转发、原文摘要不变且无采集事件；下载延迟和非法配置修复后在原 Pod 恢复 SSE 改写和已知用量入库。
- 运行时热更新被拒绝后保留旧有效配置，切换关闭态不依赖下载；1→2 副本网关扩容与重推通过，18 条事件消费完成。
- 两个隔离 Istio 网关及测试组件均零重启；原 `istio-system` 网关前后 Pod 名称、UID、重启次数完全一致。本次未复现早期两类崩溃。

本地诊断：`.tmp/k8s-collection/kind-kind/metering-wasm-ecds-1135/257edbb47d0f/`，包括完整 `integration.log`、`ecds-faults.result.json`、`gateway-scale.result.json`、真实版本和最终 Pod 镜像/状态。该结果支持最终组合在本次 1.13.5 场景下通过验收，不能外推为所有 1.13.x 或任意 Envoy 生命周期、宿主故障均已覆盖。

## 验证边界

两轮环境均为 Kubernetes 1.22.17，原控制面 1.13.5 保持不变；候选评估的数据面为 1.14.6，本次同版本复测的数据面为 1.13.5。测试使用隔离资源和合成流量，没有修改或滚动实际业务网关，也没有放开生产模板的版本限制。两轮结果均不替代生产 G0–G5、容量与宿主故障验收。
