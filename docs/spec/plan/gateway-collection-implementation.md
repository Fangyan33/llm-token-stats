# 网关数据采集组件实现计划

确认日期：2026-10-13。依据 V1.9 采集方案及设计闭环清单。

## 目标

实现 Wasm → 双向 gRPC → Observer → Kafka → Consumer → Doris，交付可构建组件、部署模板和可复现本地联调。Auth fixture 只模拟访问允许，归属通过 Observer 查询占位保持未知；估算、汇总 Worker、查询 API、门户和计费属于后续工作。

## 已确认决策

- 真实旧版 Envoy PoC 后补充确认：请求头 Continue，正文有界 Pause/改写；允许头提前到上游，保持 usage 参数唯一改写及失败透传。
- 旧版 Root 异步恢复后无法证明观察尾片完整，超时只恢复业务原文并取消观察，结果保留 partial/unknown。
- 固定 franz-go 的 RecordRetries 使用 1 表示仅首次尝试，0 会阻止首次发送。

- trailers 回调发送一次零字节 BodyChunk 标记对应方向 EOS，不上传 trailers 内容；End 保持严格一致性校验。
- 新增 Doris 配套迁移，持久保存分区消费起点和保留期缺口。
- 根模块 github.com/Fangyan33/llm-token-stats；插件独立模块，replace 引用固定 SDK v0.24.0 副本。
- 仅 request_started、usage_observed 两类事件；未知用量为 null/unknown，不估算、不补零。

## 实施顺序

1. 共享事件类型、严格 JSON/wire 校验、规范编码和摘要；固定生成工具，更新 go_package，生成标准 Protobuf 代码并做 golden 对照。
2. SDK gRPC hostcall/回调、Root token 注册和删除清理；有界插件编解码、入口过滤、独立请求改写、ACK/预算/deadline 状态机和响应计量头。
3. Observer 有界 JSON/SSE 解析、一次 UUID 分配与定稿；ACK 独立于非阻塞 Kafka Producer，固定资源限额及 watchdog。
4. Consumer 单分区顺序处理；冻结批次 → 原始/拒收表可见 → 跨在线分区冲突标记 → 日期通知可见 → 批次凭据可见 → 提交下一 offset。实现起点、缺口、rebalance 和分钟补偿巡检。
5. 镜像、默认关闭部署模板、构建与 ABI 检查、CI、容器化真实网关/Kafka/Doris 联调及运行说明。

## 接口与故障约束

- 协议服务名和 schema_version=1 保持不变；删除的 Start 字段号保留为 reserved，新增 authority=16/api_key=17；可选事件字段显式输出 null，固定 struct 编码和 SHA-256 摘要。
- Wasm 在规则启用/POST/精确路径过滤后才读取 Host/Bearer 与正文、分配观察资源。当前 `route_name: "*"` 忽略实际路由名及其 metadata；精确路由规则仍兼容。事件记录真实 Envoy 路由名或 unknown，仅供诊断；采集范围由 Wasm 顶层 accepted_request_hosts 筛选，Observer / Consumer 不维护服务或 Host 白名单，旧 accepted_routes 已删除。身份一致性排除路由，006 初始化同一新模型视图；只接纳当前格式事件，同事件不同内容仍冲突。改写只编辑 include_usage，超时/超限恢复透传；正文前缀只观察一次。
- ACK 只确认已处理帧，不等待 Kafka；发送失败不补发，无正文持久化或恢复队列。
- Consumer 未知 schema 暂停该分区；永久非法消息仅保存坐标/摘要/固定原因。事务未知须确认原 label，凭据只发送到允许的 FE/BE。
- 新消费组先持久登记最早可用 offset；已有组缺少可信起点需显式接管配置。保留期缺口登记后暂停，显式恢复，不跳到最新。
- 凭据、起点、缺口及通知本轮不清理；不把 MAX(offset) 当连续水位，不宣称缺口为零。
- 服务提供 --config、健康检查和指标；凭据通过环境/挂载文件，日志不得含正文或密钥。

## 验收

- 原有 Python 契约检查及 20 项测试；新增 Go/Python 编码摘要 golden、标准/手工 Protobuf 互操作及畸形帧。
- 插件过滤、改写差异、慢上传/边界/trailers、预算释放、超时和迟到回调；JSON/SSE 任意字节切片、工具/多候选、零值/unknown、模型长度和候选冲突。
- Kafka 黑洞、Stream Load 确认丢失、逐屏障失败/崩溃、重放、rebalance、跨日期/身份冲突、未知版本和缺口恢复。
- 三个组件产物构建成功；真实本地链路验证入库、去重、隔离、通知/凭据与 offset。mock 不代替网关测试。
- 私有 Istio 1.13.5 兼容性、生产容量、24 小时压测与灰度按 G0–G5 单独验收。固定 SDK/运行时失败保留证据，不自动换架构。

## 实施状态

组件实现、构建与真实本地联调已完成，结果及限制记录在[组件运行说明](../../collection-components.md)。生产 G0–G5 和运行说明列出的故障/容量验收仍待执行；本计划不代替生产发布批准。


## 2026-10-15 Wasm 下载增量

Observer 新增可选固定文件下载服务，Wasm 随 Observer 镜像发布；网关使用原生 HTTP 下载和 SHA-256 校验，保持 `fail_open`。本地网关不再挂载 Wasm，由启动脚本渲染摘要、核对下载后启动。README、运行说明、总体设计及采集实现方案同步这一发布方式。单版本允许下载失败期间缺少采集，恢复与版本回退需显式重载；验证证据追加到[本地验证记录](gateway-collection-verification.md)，本地真实 Istio 部署验证已在同日后续完成，生产验收仍独立执行。


## 2026-10-15 本地 Istio 部署修正

真实 kind 集群验证发现启动后追加 UID 标签不能命中旧版代理标签快照；渲染器改为必填 Pod 名称，按启动元数据 NAME/NAMESPACE 匹配，UID 继续作为采集实例 ID。Istio 的 outbound 集群名包含竖线，插件显式生成服务 DNS/端口形式的 gRPC authority，保留集群路由和 TLS/SNI。独立测试命名空间已部署并完成 JSON/SSE/unknown 入库，具体环境与边界见验证记录。

## 2026-10-16 Host 与 API Key 归属映射增量计划

当前归属接口已实现并以未实现占位运行：Wasm 临时发送入口 authority 与 Authorization: Bearer token，Observer 固定集群/环境并调用服务/客户 resolver；合法输入的四项 ID 和版本为 null，两类状态为 lookup_not_implemented，原始 usage 保留，identity_status=invalid、usage_eligible=false。HTTP/HTTPS、x-remote-data 认证及实例内成功结果缓存已实现；配置 URI 为空时继续占位，真实第三方联调须在部署环境验收；应用维度及 PG 归属读取均已删除。 详见[归属接口计划](host-service-provider-implementation.md)，前面历史记录不作为旧模型兼容要求。
