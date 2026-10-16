# API Key Auth 服务职责与接入设计

日期：2026-10-16。状态：独立鉴权设计，真实 Auth 服务和管理 API 尚未实现。当前 fixture 只允许合成请求，不验证真实密钥，不用于生产。

## 1. 与采集的职责

业务路径仍为调用方 → Istio/Envoy → 模型服务。已有 ext_authz 负责业务访问判定；Auth 的失败策略由网关鉴权配置管理，不能以采集 fail-open 放开未授权请求。Observer 不承担业务鉴权，也不串入业务请求路径。

采集使用[归属接口](spec/plan/host-service-provider-implementation.md)：Host + Observer 固定集群/环境查询服务及供应商，Bearer token 查询客户及稳定 API Key ID。当前只交付两个类型明确的未实现占位，既不连接 PG，也不调用第三方 HTTP。占位返回未知归属，不表示鉴权通过或凭证无效。

客户下属应用模型已删除；不建立应用表、API Key 应用外键、应用状态校验、应用 scope 或管理接口。

## 2. Bearer 输入与过滤器顺序

1. 原请求使用 `Authorization: Bearer <token>`，不接受 x-api-key 或客户端自报的 ID 作为采集身份。
2. ext_authz → Wasm → router 的顺序以真实 config_dump 核验。Auth 必须在 Wasm 读取之前保留实际用于请求的原 Authorization，不提前删除或替换。
3. Wasm 仅对启用且命中 POST/精确路径的请求提取 token，不改业务 Authorization。它通过 request/headers/authorization 指定属性读取合并后的值，拒绝重复、拼接、非 Bearer、空值和超长 token；不枚举全部请求头。
4. 观察消息只临时传输 token，Observer 处理后尽快清理，不写日志、Kafka、Doris、错误或追踪。现有 Auth 的 customer_id/api_key_id 元数据不作采集补值。
5. 若上游需要独立模型凭据，其替换策略与执行顺序须独立设计和验收，不能假定已经支持；采集本身不改业务认证头。

## 3. 将来建设真实 Auth 时的要求

Auth 数据面实现 Envoy v3 Authorization.Check，与 Observer 独立进程、部署、健康检查和故障策略。接入时确认受保护入口、401/403/503 与超时策略、凭证格式、主体状态、有效期及访问范围；不把普通 token 查询接口当作同步鉴权服务。

主体层级为客户 → API Key。稳定 api_key_id 与实际密钥版本分离，轮换/吊销不重写历史事件。若实现密钥创建/轮换，原文只在创建成功时显示一次；持久化采用经确认的不可逆凭证摘要及受保护服务端密钥，禁止保存可恢复原文。第三方现有 Key 的格式和生命周期尚无契约，不在采集里强制本项目自造的 prefix 格式。

客户信息、凭证、状态、授权、管理审计的事实源由真实 Auth 工作包确定；即使使用 PostgreSQL，也不成为 Observer/Consumer 的归属依赖。本次不提供 Auth PostgreSQL DDL、管理 API 或假成功凭据。

数据面和管理面分 Service、权限与网络策略；管理写操作需要独立管理员认证、幂等 request_id 和审计。创建、轮换、吊销、启停和 scope 变更保留操作者、资源 ID、非敏感前后状态和结果；不能记录 Authorization、secret 摘要或 pepper。缓存、吊销传播时限和故障时行为须单独验证。

## 4. 部署与验收

网关到 Auth、Wasm 到 Observer 使用实际验证的 mTLS/TLS；限制访问来源，Auth 不接收业务正文。故障指标采用固定原因，不使用密钥或任意客户 ID 为标签。日志/trace 不包含整份 CheckRequest、请求头或动态元数据。

当前可执行的采集部署、占位状态及密钥保护测试见[运行说明](collection-components.md)、[本地 Kubernetes 用例](kubernetes-collection-test.md)。真实 Auth 的多实例、状态/访问范围、密钥生命周期、管理幂等、缓存一致性、故障响应及性能仍待实现与验收；fixture 通过不等于这些能力已交付。
