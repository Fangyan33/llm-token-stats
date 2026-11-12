# 网关性能优化实施与复测

日期：2026-11-12。按 ACK、分配、故障清理、Observer 队列、同 VM 内存及容量复测的顺序实施。历史诊断保存在 [诊断报告](gateway-performance-diagnosis-and-optimization.md)，历史未达标容量结果不覆盖。

五项优化及 SDK 宿主缓冲所有权修复已完成，完整 Kubernetes 回归通过 27/27 项。本轮容量对照的最大时间对齐 RSS 增量为 518.250 MiB，但两组均未达到 200 QPS，Doris 全量核对也未完成，容量验收仍未通过。

## 实施内容

- 普通响应帧使用批次累计确认；Capture ACK 只用于 Start、End 和异常状态。ACK 采用单遍游标和位图校验，保持重复字段、线型、UTF-8、UUID、原因码、长度及序号边界；复用 128 项数组并清理引用。
- Observer 构造 ACK 增量计长，避免反复扫描整个批次。接收队列成功接管后即可累计确认；确认不代表解析完成或 Kafka 持久化。解析、身份冻结和最终事件在串行处理端执行，原始回调时间不变。
- 响应编码前领取 Pod 额度，最多读取 16 KiB，直接向固定上限批缓冲编码；发送复用批缓冲，累计确认清理并复用记录队列。取消控制帧进入相同有界批次。
- metrics_enabled 默认 false；关闭时不注册、更新 Wasm 自定义指标，资源保护及采集不受影响。启用时 Counter 仍按事件更新，资源 Gauge 每秒汇总；隔离 E2E 显式启用以验证激活及 Host 筛选。
- 通道按双向链表索引观察。故障立即使通道失效，回调最多回收 64 项，tick 另有总计 256 项额度；native 取消未确认时仍持有发送额度。卸载无后续 tick，必须排空残余状态，不能将卸载成本等同于正常回调上限。
- Observer 实例接收队列默认 4096 批/128 MiB，每条传输最多排队 128 批；载荷与解码对象按保守开销预留，处理和关闭归还。Producer 应用 pending 与 native 统一条数/字节限制：默认 16384 条、64 MiB、批次 1 MiB、linger 5 ms、投递 10 秒、watchdog 12 秒。

请求正文继续完全透传，不读取、解析或改写；request_model 保持 null，usage 缺失保留 null/unknown。业务回调 Continue，采集过载和故障允许丢弃，禁止等待业务或重发旁路。

## 已完成的组件测量

相同进程内对比 HEAD 原实现与当前 codec，Go/amd64 单 CPU 三轮中位数；这是组件微基准，不能直接换算成 Wasm RSS 或端到端吞吐。原始源码与结果保存在 [成对基准](../../../.tmp/optimization/paired-bench.txt)。

| 场景 | 原实现 | 当前实现 |
| --- | ---: | ---: |
| 解码 128 个 Capture ACK，临时分配 | 220656 B / 1944 次 | 7168 B / 256 次（复用数组） |
| 解码耗时 | 227808 ns | 35546 ns |
| 16 KiB 分片及外层编码，临时分配 | 73776 B / 9 次 | 0 B / 0 次（复用批缓冲） |
| 分片编码耗时 | 11409 ns | 582.9 ns |

编码基准不包括 SDK 响应读取复制、额度 hostcall 与 native 发送。纯累计确认原生解码另测为 0 B/0 次分配。

## GC 保留对象根因与修复

第一轮对同一 Envoy/8 Worker 做三波 100 QPS、60 秒到达、60 秒 SSE，每波 6000 请求全部成功。排空后主动 GC，存活堆依次为 147.171、290.131、471.967 MiB，证明仅减少临时分配仍未消除持续保留。原始证据保存在 [三波诊断](../../../.tmp/optimization/soak/3128c1714d05/result.json)。

分配器元数据诊断找到与每请求 128 个内容事件对应的对象。进一步读取 TinyGo 的 C 分配登记表：1000 请求后保留 145681 个登记、12588448 字节 C 载荷，加上登记表及对象开销，GC 后堆达 21569328 字节。

原因链条已经确认：宿主优先使用模块导出的 malloc；TinyGo 0.40.1 为保证 C 生命周期，将分配放进 runtime.allocs。SDK 把返回指针包装为 Go 切片/字符串后，原实现没有解除 C pin，所以即使 HTTP 上下文和逻辑观察已删除，GC 仍将这些缓冲视为可达。[宿主分配器选择](https://github.com/proxy-wasm/proxy-wasm-cpp-host/blob/624ef2edac2cdc086e0339f36b32ef1a2ed7e270/src/wasm.cc#L135)、[固定 TinyGo 分配器](https://github.com/tinygo-org/tinygo/blob/v0.40.1/src/runtime/arch_tinygowasm_malloc.go)。宿主源码用于解释机制，实际镜像行为另由 C 登记计数与真实 Envoy 对照验证。

SDK 新增固定运行时适配：先建立 Go 类型引用，再仅删除对应缓冲的 C 登记，不增加复制；该版本 free 本身也是删除登记，内存由 Go 引用保护到最后一次使用之后，再由 GC 回收。未知/Go-owned 指针与重复接管均为无操作，无关 C 分配保持原生命周期。没有改变 ABI 标记，也不关闭业务资源保护。变更与适用工具链记录在 third_party/proxy-wasm-go-sdk/PATCHES.md。

同样的真实 Envoy 1000 请求对照，修复后 C 登记数/载荷为 0/0，GC 后堆降至 176448 字节。见 [修复前](../../../.tmp/optimization/forensics-observe/c621936c7dfc/worker-heap.log)、[修复后](../../../.tmp/optimization/forensics-observe/7cf6a35a6d9e/worker-heap.log)。这是短时归因实验，不是容量结论。

make wasm 新增真实 TinyGo/Wasm 回归，分别从宿主调用 malloc 和 proxy_on_memory_allocate，覆盖响应/属性切片、头部字符串、跨 GC 引用有效、重复转换与 Go-owned/空指针，并验证无关 C 分配仍被保留且可正常 free。旧的隔离实验只模拟 proxy_on_memory_allocate，未覆盖宿主实际优先的 malloc，因此没有复现该问题；现在两条路径都必须通过。

## 修复后同一 VM 三轮诊断

同一个 Envoy PID 和同一组 8 个 Worker，三轮各发起 6000 个请求，100 QPS 到达 60 秒，每个 SSE 60 秒，四档输入分布不变。18000 个业务请求全部成功，无业务错误。每轮业务排空后才调用 GC。

| 波次 | 累计请求 | GC 后存活堆 | GC 管理堆容量 | 进程 RSS |
| --- | ---: | ---: | ---: | ---: |
| 1 | 6000 | 2.087 MiB | 21.733 MiB | 238.56 MiB |
| 2 | 12000 | 2.196 MiB | 24.687 MiB | 242.91 MiB |
| 3 | 18000 | 2.264 MiB | 26.656 MiB | 245.20 MiB |

相比修复前的 147.171/290.131/471.967 MiB，本轮没有出现此前明显的按响应分片累计保留。仍有容器、map/队列与分配器高水位，三轮数据不证明无限时长不存在其他泄漏。这里是 100 QPS 归因诊断，不计算额外 RSS 预算，也不代替 200 QPS 容量验收。原始结果见 [同 VM 结果](../../../.tmp/optimization/soak-sdkfix/c083e6402094/result.json)、[逐 Worker 日志](../../../.tmp/optimization/soak-sdkfix/c083e6402094/worker-heap.log)、[汇总](../../../.tmp/optimization/soak-sdkfix-summary.json)。

## 验证边界

SDK 所有权修复后的 make test 已通过 115 个 Python 测试及主模块/插件 Go 竞态检查。新增测试覆盖禁用指标零 hostcall、周期 Gauge、单遍 ACK 严格校验及缓冲所有权、有界清理与邻近通道隔离、接收队列全局条数/字节额度与清理、阻塞事件处理不阻塞累计确认、Producer 满队列编码前拒绝及字节拒绝归还槽。

首次 Kubernetes 回归 26 项通过，Producer 黑洞用例仍假定旧默认 256 条，投递 1024 条已不构成新默认队列的超载。现已显式配置 256 条/4 MiB、2 秒投递/3 秒 watchdog，保留原有满队列与非阻塞断言，最终完整回归已通过 27/27 项，无失败、无跳过，证据见 [最终 Kubernetes 回归](../../../.tmp/k8s-collection/kind-kind/metering-wasm-verify/89e83f1ef058/integration.log)。SDK 原生模块竞态检查及两种分配器的专项所有权回归已通过。SDK 修复后的最终完整 Kubernetes 回归也已通过 27/27 项，无失败、无跳过，证据见 [最终 SDK 修复回归](../../../.tmp/k8s-collection/kind-kind/metering-wasm-verify/33d3f731451d/integration.log)。Wasm 已构建并通过 ABI 检查；Protobuf 使用固定 protoc 29.3 及 Go 插件重新生成。

真实网关回归覆盖请求透传、采集异常、Producer 黑洞以及远程 Wasm 下载失败与恢复；不将 fail_open 配置视为全阶段保证。VM 崩溃、Envoy 进程故障及生产 xDS/资源组合的所有失效方式未获得穷尽验证，仍属于独立边界。

## 最新完整业务容量对照

使用未插桩发布 Wasm，不调用诊断 GC。Wasm SHA-256 为 `87ceccaf7cb52dc8b88b689c0d0e517362ade5f8708a4e0816bf732934405e86`，与本轮保存的副本一致。环境为本地 8 CPU、约 11.77 GiB 内存的共享主机，真实 Envoy/Observer/Kafka；Consumer/Doris 在业务负载结束后启动，不参与业务阶段 RSS/QPS 测量。Envoy 配置 8 Worker、客户端 64 个 HTTP/2 连接；生产 Worker 数仍未知，用户提供的 8 个副本不等于 8 Worker。

两组均按 200 QPS 到达，预热 60 秒、稳态 300 秒，每个响应 SSE 持续 60 秒，各发起并保存 72000 个唯一请求结果，无重复 ID。每组输入分布精确为 14400 个 1M、36000 个 200K、14400 个 500K、7200 个 100K。这里使用合成模型及可单独核验的 prompt token 内容，不代表真实模型的计算吞吐。

| 项目 | 不启用插件 | 启用插件 |
| --- | ---: | ---: |
| 保存的请求结果 | 72000 | 72000 |
| 完整成功请求（无客户端错误） | 53829 | 42090 |
| 业务错误 | 18171 | 29910 |
| 稳态实际完成 QPS | 122.217 | 89.465 |
| Envoy 峰值 RSS | 956.758 MiB | 1418.172 MiB |
| 业务排空后 RSS | 945.461 MiB | 1413.840 MiB |
| 满足目标负载 | 否 | 否 |

错误均为客户端 180 秒期限到期，包含等待响应头或读取响应期间超时；部分请求已返回 HTTP 200 仍未完整读完 SSE，不能只按 HTTP 状态判定成功。`done`/`usage` 与超时临界点也可能同时出现，成功数以无客户端错误的 load 结果为准。

按“启用减去不启用”计算，两组峰值之差为 **461.414 MiB**，相同相对时刻最近样本的最大增量为 **518.250 MiB**。最大样本发生在启用组 281.769 秒/基线组 281.731 秒：1313.047 MiB 减去 794.797 MiB。每组采样过程中 Envoy PID 均未改变。这两个样本差值均小于 2048 MiB，但负载未达标，正式结果保持 `rss_evaluation_valid=false`、`rss_within_budget=null`、`passed=false`，不得将其写成容量通过。

历史最大增量 2926.918 MiB 与本轮 518.250 MiB 分别保留。两轮实际吞吐及运行状态不同，不能据此承诺生产下降比例；本轮无插件基线本身只有约 122 QPS，也不能把启用后的约 89 QPS 外推为生产性能损失比例。

## 本轮事件核对结果与剩余限制

启用组最终 Observer 指标显示 `producer_success=144000`、`observer_finalized=72000`，Producer pending 条数/字节、接收队列条数/字节和活动观察均为零；32 条共享流仍有合计 3 MiB 解析预留。Producer 成功回调不等于 Doris 最终可见，也不能凭这两个总数证明每个请求的事件及 usage 正确。

Doris 核对最后采样仅确认可见 16238 行，随后出现事务 aborted、可见性查询 deadline exceeded；Kafka 管理命令也有请求超时，最终 `kafka-consumer-groups --describe` 超过 60 秒，驱动返回失败，全量逐请求校验未执行。已有日志不足以将所有超时归因于某一个资源瓶颈。本轮未完成 Doris 入库完整性验收，不修改 Consumer/Doris 断言来跳过故障。

测试驱动按原有 finally 流程清理了自身隔离容器，保存了日志、请求台账和 RSS 样本，但未保留 Kafka/Doris 数据卷；这批数据不能继续离线重放核对。后续完整入库核对需要重新运行相应测试。此前通过的 27 项隔离 E2E 保持为独立证据，不能替代这次 72000 请求的核对。

原始证据见 [容量目录](../../../.tmp/optimization/capacity-sdkfix/5d7f2327b3ac)、[业务阶段日志及核对失败](../../../.tmp/optimization/capacity-sdkfix-run.log)、[请求台账与结果汇总](../../../.tmp/optimization/capacity-sdkfix-summary.json)。汇总由已保存的业务结果、RSS 和失败日志生成，显式记录核对未完成，不冒充测试驱动完整成功输出。测试进程及所属隔离容器已清理，既有 kind/buildkit 保留。

验收条件仍为单 Pod 200 QPS、60 秒 SSE，20% 1M/50% 200K/20% 500K/10% 100K，启用 RSS 减无插件 RSS ≤2048 MiB，并完成事件逐请求核对。本地结果与生产同规格验收分开报告。
