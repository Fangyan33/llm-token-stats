# 采集事件合成样例

六份 JSON 是当前唯一 schema_version=1 契约的合成金样，覆盖开始、已知/部分/未知/零用量及冲突。应用字段已删除，包含规范 request_host、固定集群/环境、provider_id、两类状态与版本。

这些样例故意包含 mapped 合成客户和服务，用于检验编码、Consumer 与汇总；正常 Observer 仍注入未实现查询接口，只产生未知归属，不得把样例当成真实第三方返回。

.canonical 保存规范事件字节；.hashes 两行依次为完整事件 SHA-256 与身份摘要。身份元组为 `[service_id, provider_id, api_key_id, customer_id, identity_status, config_version, gateway_instance_id, adapter_id, x_request_id, cluster_name, deployment_environment, mapping_status, mapping_version, customer_mapping_status, customer_mapping_version]`，规范 JSON 保留 null，不含原始密钥、独立路由或模型字段。Go、Python、SQL 使用同一顺序，不保留旧金样或旧摘要兼容路径。
