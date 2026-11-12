# 网关 QPS 与 RSS 诊断及优化建议

日期：2026-11-11。基于 200 QPS 混合上下文对照、原生 Go 微基准、实际 TinyGo/Wasm 隔离实验和真实 Envoy 的诊断构建。当前阶段仅做诊断和方案，未修改生产采集实现。

后续实现与复测见 [2026-11-12 优化验证](gateway-performance-optimization-verification.md)。本文保留诊断时的源码与实验状态。

## 结论与证据边界

已经确认：

1. Continue 之前的响应编码、复制、指标 hostcall，以及收到 ACK 后的解码和清理，仍同步运行在 Envoy Worker 上。异步 gRPC 发送没有把这些计算移出 Worker。
2. 当前 ACK 编解码存在可复现的分配与计算放大。Observer 反复计算整个 ACK 大小；Wasm 多次遍历同一 ACK，建立临时切片、map 和字符串。
3. ACK 或结束确认超时会触发共享通道或单观察清理；共享通道清理扫描所有活动观察，逐条做额度、指标更新，没有单次工作量上限。这会扩大 Worker 占用和采集事件突发。
4. 内存预算只覆盖逻辑在途载荷、记录和观察，未覆盖 VM 堆容量、临时对象、GC 后保留对象及 native 分配器缓存，因而不构成实际 RSS 上限。
5. 真实 Envoy 诊断中，GC 可以回收部分对象，但 VM 堆容量和 RSS 没有收缩；GC 后仍保留约 496 MiB 堆，其具体引用来源尚未锁定。

不能据此声称：所有性能损失都由 GC 导致、已经找到了某个唯一泄漏对象、微基准加速等于端到端 QPS 加速，或已经满足额外 RSS ≤2048 MiB。原生 CPU profiler 的采样窗口覆盖了人为 GC 诊断，不能用来给出正常稳态回调的 CPU 占比。

## QPS 为什么下降、在哪里排队

Envoy 1.22.7 的连接固定在一个 Worker 上，由该 Worker 执行过滤和转发。过滤回调返回 Continue 之前的工作会推迟同一事件循环继续处理网络事件。[Envoy 线程模型](https://www.envoyproxy.io/docs/envoy/v1.22.7/intro/arch_overview/intro/threading_model)

当前业务回调没有主动等待 Kafka/Doris，也不暂停或改写请求正文。主要同步路径为：

```text
响应到达
  → GetHttpResponseBody 分片复制
  → Chunk / Frame / CaptureFrame / BatchItem 多层编码与追加
  → 额度与 gauge 更新
  → Continue

ACK 到达
  → ValidateBatch / Read / Validate / DecodeAck 重复遍历
  → 分配 Field 切片、校验 map、ACK 字符串和结果数组
  → 释放额度、更新指标、处理 Close

ACK 或结束确认超时
  → 取消传输或观察
  → 扫描并逐项清理观察
  → Observer 集中 finalize、向 Producer 突发提交事件
```

可定位的排队点包括 Worker 事件循环、gRPC 收发及 ACK 队列、Observer 批次处理与 finalize，以及 Producer 的有限 pending 队列。ACK 延迟可能来自任一端未及时得到执行时间或收发背压；仅凭超时计数不能判断全部延迟发生在网络上。

本轮完整对照有 50624 次 Wasm 超时清理计数，以及 33357 条 Producer 明确丢弃事件。Producer pending 容量为 256，满时丢弃，不等待业务。Doris 在两组负载结束后才启动，因此其写入耗时不可能是本轮业务阶段 QPS 下降的原因。

源代码定位：

| 路径 | 位置 | 已确认问题 |
| --- | --- | --- |
| 响应观察与编码 | `plugins/gateway-observer/main.go:613`、`:315` | 先构造多层临时消息，再申请发送额度并追加批缓冲 |
| gauge 更新 | `plugins/gateway-observer/main.go:92` | 每次 send、ACK 和清理都可能执行多个 hostcall |
| ACK 解码 | `pkg/observationwire/batch.go:70`、`wire.go:34`、`:101`、`:245` | 校验和解码重复 Read，嵌套 ACK 还重复校验 |
| ACK 构建 | `internal/observation/server.go:453` | 每追加一个 ACK 都执行 `proto.Size(a)`，重复遍历已加入的 ACK |
| 超时与共享流清理 | `plugins/gateway-observer/main.go:452`、`:265` | 单次扫描并逐条处理大量观察；deadline 循环的 256 上限没有约束共享通道清理 |
| Producer | `internal/producer/producer.go:64` | Canonical 在入队判断前执行，pending 与 native 缓冲都固定为 256 条；故障 finalize 突发时容易丢弃 |

### 不应把本地下降幅度直接外推生产

原对照两组各发起 72000 个请求，无插件实际稳定接收 130.074 QPS，启用后 56.970 QPS。仅 Envoy 的累计 CPU 时间为 919.35 与 1203.66 秒，而 fixture 实际接收字节从约 133.73 GB 降至 79.92 GB。这说明启用组在处理更少业务正文的情况下仍消耗了更多网关 CPU，存在明显额外成本。

同时，无插件基线本身已达不到 200 QPS；负载、fixture、Observer、Kafka、网关及既有环境共享 8 vCPU。Observer 在启用组另消耗约 302 秒 CPU。现有数据没有逐 Worker CPU 火焰图和回调耗时直方图，不能断言所有核始终满载，也不能把 130→57 当作生产必然下降比例。应按 Worker 检查连接分布、回调耗时和 ACK RTT，而不只看 Pod 总 CPU。

## 已量化的编码与 ACK 放大

在相同函数上用原生 Go/amd64、单 CPU、三轮微基准测量，中位数为：

| 操作 | 临时分配 | 分配次数 | 时间 |
| --- | ---: | ---: | ---: |
| 解码仅批次累计确认的 ACK | 96 B | 2 | 0.172 µs |
| 解码含 1 个 Capture ACK | 1704 B | 20 | 2.313 µs |
| 解码含 32 个 Capture ACK | 54000 B | 498 | 75.910 µs |
| 解码含 128 个 Capture ACK | 220656 B | 1944 | 276.809 µs |
| 编码 100 B 响应分片及外层消息 | 768 B | 10 | 0.551 µs |
| 编码 16 KiB 响应分片及外层消息 | 73776 B | 9 | 17.230 µs |

128 个 Capture ACK 的实际报文为 7171 B，解码的临时分配约为报文大小的 30.8 倍。编码基准不包含 SDK 读取响应缓冲和 Envoy native 发送复制，不能当作完整分片处理成本。原生 Go 的对象布局、GC、编译器和 Wasm 不同，表中 B/op 及 ns/op 不直接等于 Wasm RSS 或线上耗时。

Observer ACK 构造中，128 个 ACK 的累积 `proto.Size(a)` 计算从重复扫描变成增量计长的原型，约 0.440 ms 降至 0.022 ms，组件级约 19.8 倍。每一步计算长度均验证与 `proto.Size` 一致；原型没有合入生产代码。此收益只针对该构造场景，不是整个 Observer 或网关的加速倍数。

本轮 fixture 每响应生成 128 个内容事件。在理想 200 QPS 稳态下约为 25600 个内容事件/秒；实际 Envoy 回调数还受合包、分片影响，不能直接等同事件数。当前逐正文分片产生 Capture ACK，又在 Worker 解码这些确认，会持续制造额外小对象和 hostcall。

## RSS 增量扩大的已知机制

### 不是固定预分配 512 MiB，也不是关闭 GC

已检查本轮 Wasm 内存段：初始内存仅 2 页，即 128 KiB，没有声明 memory maximum；本机固定 TinyGo 0.40.1 的 wasip1 目标采用 precise GC。观察到的大映射来自运行期，不是模块初始就固定分配了 512 MiB。

该 TinyGo 版本的 `growHeap()` 按当前页数增加内存，即倍增；`setHeapEnd()` 只处理增长。[TinyGo 0.40.1 growHeap](https://github.com/tinygo-org/tinygo/blob/v0.40.1/src/runtime/arch_tinygowasm.go)、[GC 实现](https://github.com/tinygo-org/tinygo/blob/v0.40.1/src/runtime/gc_blocks.go)

### 隔离实验排除了“ACK 解码器本身必然漏到几百 MiB”

使用同一 TinyGo 编译目标和实际 codec，在 wazero 中进行 2048 次、每次 128 个 ACK 的实验，并测试复用缓冲、新建 SDK 缓冲和嵌套 hostcall。嵌套路径确实调用 SDK `GetGrpcReceiveBuffer`，host 再回调 `proxy_on_memory_allocate` 分配缓冲。

新建缓冲的累计分配约 393 MiB；普通路径线性内存约 0.5 MiB，嵌套路径约 1 MiB，GC 正常回收。此结果证明高分配与不可回收泄漏不是同一个概念。实验没有复现完整 Envoy、SDK 上下文、观察状态和真实回调序列，不能据此排除完整路径中的引用保留问题。

### 真实 Envoy 的 GC 对照

仅在隔离诊断副本中加入堆统计和 SDK 上下文计数。业务负载为 200 QPS 到达 91 秒、60 秒 SSE，共 18200 请求；等待负载结束后，才发送诊断控制请求，分别记录每个 Worker GC 前后的数据。该构建和人为 GC 不用于容量验收或优化收益声明。

8 个 Worker 合计：

| 数据 | GC 前 | GC 后 |
| --- | ---: | ---: |
| GC 管理堆容量 `HeapSys` | 2015.58 MiB | 2015.58 MiB |
| `HeapAlloc` | 1147.17 MiB | 495.69 MiB |
| Envoy 进程 RSS | 2564.66 MiB | 2574.59 MiB |

每个 Worker 的 HeapSys 约 252 MiB。GC 回收约 651 MiB 对象占用，堆容量未收缩，RSS 也未下降。RSS 还包含 native 分配器保留、VM 及其他状态；其前后小幅变化包含诊断与 profiler 开销，不单独归因 GC。

GC 后每个 Worker 仍保留约 48–73 MiB 堆。SDK 在各 Worker 只有当前诊断 HTTP 请求、一个 plugin context，业务观察已清空，不能归因于大量业务 HTTP context 仍未删除。但这些对象具体挂在哪个根上、属于必要缓存、仍被引用的临时对象还是 GC 误保留，尚未定位；单轮数据也不足以认定无限增长的泄漏。

完整容量对照中另观察到 8 块各 512 MiB 的大匿名映射、native heap 缓存，以及逻辑观察/在途记录归零后 RSS 仍约 3819 MiB。最大时间对齐增量为 2926.918 MiB。两轮负载时长和构建不同，不能把诊断副本的总 RSS 减去原基线当作新的容量结论。

因此，内存问题至少有两层：运行期堆扩容后的高水位保留，以及真实回调路径中 GC 后仍保留的对象。Pod SharedData 的 256 MiB 在途额度没有覆盖这两层，也没有覆盖 native 缓存。这是当前预算模型与实际 RSS 之间的确定缺口。

## 优化方案与顺序

保持原目标：单 Pod 200 QPS、约 12000 条 60 秒活跃流、20% 1M/50% 200K/20% 500K/10% 100K，全量采集正常返回的 usage，RSS 增量 ≤2048 MiB。请求正文继续完全透传，原始回调时间保留，未知 usage 不估算或补零。

### 第一批：缩减已确认的热路径成本

1. **稀疏 Capture ACK。** 普通正文帧通过 `accepted_batch_seq` 累计确认并释放传输额度；Capture ACK 用于 Start 分配身份、End/错误/拒绝等状态变化。Observer 仍校验每帧，批次 ACK 必须以完成校验或受控队列成功接管为前提，不代表 Kafka 已持久化。先保留现有消息结构与严格边界测试。
2. **ACK 单遍校验解码。** 使用游标和字段位图，避免先 Validate 再 Read、多层重复解析及临时 map/Field 数组。保持重复字段、长度、序号、身份和 late-frame 检查，不用放宽协议校验换性能。
3. **Observer 增量计长。** 构造 ACK 时维护累计编码长度，不对每次追加都扫描整个 `a`。这是已有组件原型验证的低风险改法。
4. **直接写入复用批缓冲。** 提前计算编码长度、申请额度，直接把分片编码进每条共享传输的有界缓冲，减少 Chunk/Frame/CaptureFrame/外层消息的临时复制。资源不足时先跳过观察，减少已经知道发不出去的分片分配；每片仍 ≤16 KiB，保留原回调时间。
5. **指标周期化。** 回调内只更新本地计数，按独立周期在 tick 汇总 gauge hostcall；传输 flush 的 10ms 时钟与指标刷新周期分开，避免每个分片、每个 ACK 都更新多个 native gauge。

稀疏 ACK 的收益尚未经过真实 Envoy A/B 验证，不据微基准承诺具体 QPS 或 RSS。

### 第二批：控制故障时的同步清理与资源争用

- 通道维护自己的观察链表/索引，避免每次关闭扫描全体观察；使用 generation/失效标志快速停止相关观察，按固定工作量分批回收。native 取消未确认前仍保留相应额度，单观察结束不关闭共享流。
- 取消记录可以进入有界控制批次，减少超时风暴中逐观察单独发送 gRPC。控制帧额度、优先级和失败清理仍需严格有界，不阻塞业务。
- SharedData 额度归还增加适度滞后和租约复用，减少每批清空时立即归还、下一帧又 CAS 申请的往返；Pod 全局额度和卸载时保守回收约束保持不变。
- 在 callback、ACK RTT、队列等待和清理耗时中增加可归因的诊断测量，区分发送端 Worker、Observer 处理和 ACK writer 的延迟；不把调大 2 秒超时当作消除根因。
- 对共享传输流数做 1/2/4 的独立诊断。多个流不会增加同一 Worker 的 Wasm CPU 并行度，固定 10ms flush 下可能增加小包与 ACK；减少流数又会扩大单流故障影响范围，需要实测后选择，不盲目增加。

### 第三批：Observer 侧使用可独立配置的资源

- 将解析、finalize 和写 Kafka 与接收确认解耦。Observer 先用真实字节额度接管有界记录，再由固定工作池处理，回执语义明确区分“已接管”和“解析/持久化完成”。不能先 ACK 再把未预留内存的记录放进无界队列。
- Producer 的 pending 条数、native buffered records/bytes、提交批量与投递时限统一配置，按正常约 400 事件/秒及故障结束突发设计。单独增大 256 条队列、同时维持过短投递时限并不能保证完整性。
- 队列满时继续 fail-open 并明确记录丢弃；扩容发生在 Observer，不让网关等待 Kafka 或 Doris。

### 内存归因与最终验证

先保留已确认的高收益改动，再通过分配点/对象类型统计和同一 VM 重复负载，检查 GC 后存活堆是否持续增长。只记录内存与引用元数据，不将正文、密钥写入诊断日志。单纯主动 GC 不收缩本运行时的线性堆，而且占用 Worker，不能作为 RSS 预算控制方案。

按实际 Worker 数规划每个 VM 的高水位及 native 余量，固定缓冲、临时分配、观察状态和在途 native 状态都应纳入设计。逻辑字节计算仍需 RSS 对照验证；在尚有未归因保留对象时，不能承诺优化后一定 ≤2 GiB。

验收前先获得有效的无插件基线。本地基线不足 200 QPS 时，先诊断负载端、fixture、连接分布和网关资源争用；较低 QPS 的实验仅用于归因，不替代正式 200 QPS 标准。最终仍执行同分布、同协议和 Worker 数的 60 秒预热、300 秒稳态、排空与全量 Doris 核验，分别报告 CPU、实际吞吐、业务错误、采集完整率和 RSS 增量。

## 证据与范围

- [完整容量记录](gateway-capacity-200qps-verification.md)。
- [原生微基准源码](../../../.tmp/gateway-performance-audit/wire_bench_test.go)、[单 CPU 三轮结果](../../../.tmp/gateway-performance-audit/native-bench-cpu1.txt)。
- [实际 TinyGo/Wasm 隔离实验](../../../.tmp/gateway-performance-audit/wasm-heap-probe/result.json)、[嵌套 hostcall 输出](../../../.tmp/gateway-performance-audit/wasm-heap-probe/runtime-result-nested.log)。
- [真实 Envoy 诊断环境](../../../.tmp/gateway-performance-audit/envoy-heap/5853fd2ad7d4/diagnostic-environment.json)、[负载结果](../../../.tmp/gateway-performance-audit/envoy-heap/5853fd2ad7d4/result.json)。
- [GC 前后与微基准汇总](../../../.tmp/gateway-performance-audit/diagnostic-summary.json)、[Worker 堆日志](../../../.tmp/gateway-performance-audit/envoy-heap/5853fd2ad7d4/llm-token-stats-capacity-heap-probe-5853fd2ad7d4.log)。

诊断代码、临时 SDK 副本和 Wasm 均保存在忽略的 `.tmp/gateway-performance-audit/`，未替换发布构建。没有修改生产网关或生产采集源码，未提交或推送代码。GC 控制请求在负载结束后执行，不能把它们用于压低容量验收的 RSS。仅清理了诊断自建容器，保留既有 kind 与 buildkit。
