# 采集事件合成样例

六份 JSON 是合成事件；保留具名路由示例，以覆盖历史事件重放。运行时未知诊断路由使用 `unknown`，历史事件中的路由名或 `*` 不改写。缺失、具名和通配历史路由的兼容性由事件、插件及集成测试覆盖。

对应 `.canonical` 保存规范事件字节；`.hashes` 第一行为完整规范事件的 SHA-256，第二行为当前身份摘要。身份摘要按 `service_id`、`api_key_id`、`customer_id`、`application_id`、`identity_status`、`config_version`、`gateway_instance_id`、`adapter_id`、`x_request_id` 构造 JSON 数组，不含诊断 `route_name`。本次仅更新第二行；原事件及内容摘要保持不变。旧数据库摘要的读取兼容由 [006 migration](../../migrations/doris/metering/006_optional_route_identity.sql) 与 Consumer 的类型化字段比较实现。
