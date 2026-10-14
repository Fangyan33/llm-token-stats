# 采集组件本地验证记录

日期：2026-10-13。实现见[计划](gateway-collection-implementation.md)，使用方法与未验收门槛见[运行说明](../../docs/collection-components.md)。本记录是公开固定 Envoy 的本地合成验证，不代表私有集群或生产容量验收。

## 工具与产物

- Go 1.25.0，TinyGo 0.40.1（LLVM 20.1.1），SDK 基底 v0.24.0；扩展摘要与二进制 SHA-256 由 `make manifest` 保存到 `dist/collection-manifest.json`。
- protoc 29.3 / protoc-gen-go v1.36.5 / protoc-gen-go-grpc v1.5.1；生成的标准代码已入仓库。
- 真实 Envoy 版本输出：`725ec273d665639aa9f6e0e9014af562a457c2f4/1.21.3-dev/Clean/RELEASE/BoringSSL`。
- Kafka 3.9.2、Doris 4.1.3；实际镜像摘要见[镜像锁定记录](../../deploy/local/images.lock.json)。本地 Envoy 使用静态配置，未接入 Istio 控制面。

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

私有镜像、真实 Auth 与网格 mTLS/HTTP2、完整 Istio/K8s 发布、HTTP2 慢上传、断开与热更新交叉、真实进程崩溃/rebalance/保留期丢失恢复、峰值两倍与 24 小时压测、10 分钟 SSE、延迟目标、灰度回退仍待 G0–G5 执行。汇总 Worker/平台 API 属于后续子项。
