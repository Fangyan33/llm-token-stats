# Apache Doris 本地验证

本目录使用 Apache Doris 4.1.3 官方 all-in-one 镜像，提供单 FE、单 BE 的开发/测试环境。生产环境应使用 Doris Operator 或独立 FE/BE 集群，按容量测试确定副本、分桶和资源配置。

在仓库根目录启动：

~~~sh
docker compose -f deploy/doris/compose.yaml up -d --wait
docker compose -f deploy/doris/compose.yaml exec doris mysql -uroot -h127.0.0.1 -P9030 -e "SHOW FRONTENDS; SHOW BACKENDS;"
docker compose -f deploy/doris/compose.yaml exec doris mysql -uroot -h127.0.0.1 -P9030 -e 'ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");'
docker compose -f deploy/doris/compose.yaml exec -T doris mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/001_init.sql
docker compose -f deploy/doris/compose.yaml exec -T doris mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/002_usage_summaries.sql
docker compose -f deploy/doris/compose.yaml exec -T doris mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/004_summary_coordination.sql
docker compose -f deploy/doris/compose.yaml exec -T doris mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/005_consumer_recovery.sql
docker compose -f deploy/doris/compose.yaml exec -T doris mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/006_optional_route_identity.sql
docker compose -f deploy/doris/compose.yaml exec doris mysql -uroot -h127.0.0.1 -P9030 -e "SHOW TABLES FROM metering;"
~~~

此独立 Compose 环境的 SQL、FE HTTP、BE HTTP 端口分别为 9030、8030、8040，统一绑定 `0.0.0.0`。远端 DBeaver 使用 MySQL 驱动连接 `服务器IP:9030`，用户 `root`、密码为空。`make local-up` 启动的采集联调栈使用独立端口 19030、18030、18040，详见[组件运行说明](../../docs/collection-components.md)。FE 会将 Stream Load 重定向至 BE；Docker Desktop 宿主机可能无法访问容器内部 BE 地址，联调时从 compose 网络内发起请求或使用可直达的 BE 地址。所有写入时间和 SQL 会话使用 UTC。

Schema 采用 Unique Key (request_date, logical_request_id, event_id, payload_hash)：同内容重放覆盖同一键，不同摘要保留冲突证据。两张明细表按 UTC 日期每日分区，在线保留一年：365 个日期（当天及前 364 天），dynamic_partition.start=-364、end=2；Doris 自动创建并周期清理。各 FE 的 dynamic_partition_enable=true 还须写入持久 fe.conf，ADMIN 修改仅更新运行配置；重启后重新核验。首次写入前确认日期分区实际已创建。已有部署先停归档任务，再应用 003_raw_retention_one_year.sql；扩大窗口不会恢复已删数据。日/月汇总采用独立普通 RANGE 表，由待实现的 usage-worker 管理分区并原子发布；详见[汇总设计](../../docs/daily-monthly-summary-design.md)。归档为[可选设计](../../docs/archive-optional-design.md)，一期无 S3/MinIO 依赖。request_conflict_ids 长期保留 UUIDv4 冲突标记，避免较早证据过期后剩余变体重新变为有效用量。

Consumer 已实现，构建与本地链路见[组件运行说明](../../docs/collection-components.md)；汇总 Worker 和查询 API 留待后续工作包，建表不启动刷新任务。采集与汇总协调表见[004 Doris migration](../../migrations/doris/metering/004_summary_coordination.sql)，通知、消费批次凭据和发布状态保存在同一 Doris 集群，无额外 PostgreSQL 依赖；旧 PostgreSQL usage migration 已退役。调度采用独立 Go usage-worker，默认每分钟 CronJob --once；单个发布者保护、查询版本校验及旧状态切换见[Doris 协调设计](../../docs/doris-summary-coordination.md)。Auth PostgreSQL 按鉴权方案独立部署。写入建议采用 Stream Load：每批固定 label，Expect: 100-continue、format: json、read_json_by_line: true、max_filter_ratio: 0、timezone: +00:00，检查响应 Status、行数和可见性，并确认冲突标记、日期通知及批次凭据后再提交 Kafka offset。label 保留期有限，长期重放依赖 Unique Key；不同内容仍必须保留为独立变体。未知 schema 版本暂停对应 Kafka 分区。

升级时先暂停旧 Consumer，执行 [006 路由身份兼容迁移](../../migrations/doris/metering/006_optional_route_identity.sql)，再升级 Consumer / Observer 和 Wasm。006 更新视图，使路由名不参与身份比较，对新旧摘要统一从类型化字段比较；不回填原始事件、不自动清除持久冲突标记。

本示例不自动执行 migration；避免服务启动时以 root 反复改表。生产采用受控迁移与独立账号。

参考：[Doris all-in-one 镜像](https://doris.apache.org/community/developer-guide/all-in-one-image/)、[Unique Key 模型](https://doris.apache.org/docs/4.x/table-design/data-model/unique/)、[动态分区](https://doris.apache.org/docs/4.x/table-design/data-partitioning/dynamic-partitioning/)、[Stream Load](https://doris.apache.org/docs/4.x/key-features/stream-load/)。

## Kubernetes 开发环境

单节点示例在 deploy/doris/doris.yaml，使用 StatefulSet 保存 FE 元数据和 BE 数据，服务名为 doris。集群需要默认 StorageClass。部署后先检查 BE Alive，再手动执行 migration；FE 的 HTTP 健康检查只能表明 FE 可用。

~~~sh
kubectl apply -f deploy/doris/doris.yaml
kubectl rollout status statefulset/doris --timeout=5m
kubectl exec doris-0 -- mysql -uroot -h127.0.0.1 -P9030 -e "SHOW FRONTENDS; SHOW BACKENDS;"
kubectl exec doris-0 -- mysql -uroot -h127.0.0.1 -P9030 -e 'ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");'
kubectl exec -i doris-0 -- mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/001_init.sql
kubectl exec -i doris-0 -- mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/002_usage_summaries.sql
kubectl exec -i doris-0 -- mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/004_summary_coordination.sql
kubectl exec -i doris-0 -- mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/005_consumer_recovery.sql
kubectl exec -i doris-0 -- mysql -uroot -h127.0.0.1 -P9030 < migrations/doris/metering/006_optional_route_identity.sql
kubectl exec doris-0 -- mysql -uroot -h127.0.0.1 -P9030 -e "USE metering; SHOW DYNAMIC PARTITION TABLES;"
~~~

集群内 SQL 地址为 doris:9030。这个无密码单节点示例仅用于隔离的开发命名空间；生产须使用 Doris Operator 或独立 FE/BE 集群、独立账户和 Secret 管理。

## 从现有 ClickHouse 切换

若已部署 ClickHouse，先保留旧库及 Kafka 消费进度快照。把仍在 Kafka 保留窗口内的事件按原内容重放到 Doris；更早的在线明细需从旧库按规范事件和类型化字段迁入，重算摘要与身份快照，并保持原请求日期。先迁移旧库的持久冲突 UUID 标记，再比较每日期事件键数、有效请求数、token 总量、冲突 UUID 和拒绝诊断数；确认 Doris 查询与水位后再切换读流量。旧库保留到在线窗口和回退期限结束，历史数据不能由空的新库自动恢复。
