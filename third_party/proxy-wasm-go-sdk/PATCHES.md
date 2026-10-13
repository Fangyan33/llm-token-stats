# 项目扩展

基底：github.com/tetratelabs/proxy-wasm-go-sdk v0.24.0，保留原许可证。
新增 gRPC stream/send/close/cancel ABI、Root+token 回调注册、接收 buffer、native mock；SetEffectiveContext 同步 SDK context；删除回调支持可选 OnDelete() 清理接口。
Root 删除清理 token，迟到回调忽略；不通过 proxy_done 延长业务生命周期。

native emulator 增加 gRPC 调用与迟到 ACK 注入；Resume 清空已转发缓冲，RemoveHeader 删除全部重复值，与真实 Envoy PoC 的行为一致。ABI marker 保持基底的 `proxy_abi_version_0_2_0`，不伪装为 0.2.1。
