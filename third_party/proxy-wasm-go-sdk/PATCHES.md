# 项目扩展

基底：github.com/tetratelabs/proxy-wasm-go-sdk v0.24.0，保留原许可证。通过现有 Go module replace 使用本地源码；本次修改缓冲所有权处理，不升级 SDK 版本。
新增 gRPC stream/send/close/cancel ABI、Root+token 回调注册、接收 buffer、native mock；SetEffectiveContext 同步 SDK context；删除回调支持可选 OnDelete() 清理接口。
Root 删除清理 token，迟到回调忽略；不通过 proxy_done 延长业务生命周期。

native emulator 增加 gRPC 调用与迟到 ACK 注入；Resume 清空已转发缓冲，RemoveHeader 删除全部重复值，与真实 Envoy PoC 的行为一致。ABI marker 保持基底的 `proxy_abi_version_0_2_0`，不伪装为 0.2.1。

原生 emulator 支持 request/headers/<name> 属性，按固定 Envoy CEL HeadersWrapper 用逗号合并同名值；用于验证 Authorization 重复头拒绝，不读取整个请求头集合。运行插件通过指定属性读取，避开旧 get_header_map_value 只取首值的行为。

新增非 panic 的 TryDefineCounterMetric/TryDefineGaugeMetric/TryIncrement/TryAdd，指标 hostcall 失败由观察插件忽略，不中断业务恢复。原有指标接口保持兼容。native emulator 新增可计数的 hostcall 故障注入，用于覆盖正文读取/替换、恢复请求、上下文切换、指标及 tick 注册失败。

- 2026-11-10：本地 host emulator 增加 grpc_open/grpc_send/grpc_cancel 故障注入，以及 RemainingHostFailures，用于证明请求正文读写和暂停恢复 hostcall 完全未被调用，验证共享传输故障时业务透传。

- 2026-11-12：固定 TinyGo 0.40.1 的 ABI 输出缓冲所有权适配。Envoy 使用模块导出的 malloc，TinyGo 将这些分配登记在 runtime.allocs，SDK 原先转换为 Go 值后未解除 C 引用，导致响应分片、头部、属性及 ACK 缓冲持续保留。RawBytePtrToByteSlice/String 在建立 Go 强引用后移除 C pin，保持零额外复制；这与该版本 libc_free 的删除登记语义一致，不立即释放或复用仍被 Go 引用的内存。Go-owned/proxy_on_memory_allocate 分配与重复转换为无操作；其他运行时使用空适配。保留原许可证与 ABI 0.2.0，禁止通过升级 ABI 标记掩盖宿主分配器差异。该适配依赖固定运行时，升级工具链必须重跑 malloc/Go allocator 路径的真实 Wasm 所有权检查及 Envoy 回归。

- 同步增加真实 TinyGo/Wasm 回归：分别由宿主调用 malloc 和 proxy_on_memory_allocate，验证响应/属性切片及头部字符串接管后 C pin 归零、跨 GC 内容有效、Go-owned/空缓冲与重复转换无副作用。make wasm 自动运行该检查，测试探针不进入发布产物。

- 2026-11-12：native HTTP host emulator 的 GetBufferBytes 返回读取副本，模拟 Envoy native 正文到 Wasm 线性内存的复制边界。清零 SDK 读取缓冲不再误改模拟业务正文；仅修改测试宿主，不改变生产 SDK 读取实现或基底版本。
