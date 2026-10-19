# Apache Doris 本地验证

本目录使用 Apache Doris 4.1.3 官方 all-in-one 镜像，提供单 FE、单 BE 的开发/测试环境。生产环境应使用 Doris Operator 或独立 FE/BE 集群，按容量测试确定副本、分桶和资源配置。

已有当前模型库新增名称字段时，先按实际缺失列执行 `008_name_snapshots.sql` 并重建视图，再执行 `007_schema_comments.sql` 更新注释；新库 001 已有名称列，不重复运行 008。旧数据名称保持 NULL。

本页 SQL 初始化当前唯一契约，应用字段已删除，增加供应商、部署上下文及两类解析状态。使用 render_doris_sql.py --database 库名 SQL文件 渲染到隔离库，Consumer 的 doris.database 和 DSN 必须一致。旧结构共享库不能原样套用新 DDL；本地默认 metering_development，K8s 每次另建测试库。

完整表职责、上下游关系及字段说明见[数据库表与字段文档](../../docs/doris-database-schema.md)。新建库的注释包含在初始化 SQL；已有当前模型库执行 007_schema_comments.sql 补注释，CREATE TABLE IF NOT EXISTS 本身不会更新已有注释。007 不升级旧字段，执行前确认目标库。

在仓库根目录启动：

~~~sh
docker compose -f deploy/doris/compose.yaml up -d --wait
docker compose -f deploy/doris/compose.yaml exec doris mysql -uroot -h127.0.0.1 -P9030 -e "SHOW FRONTENDS; SHOW BACKENDS;"
docker compose -f deploy/doris/compose.yaml exec doris mysql -uroot -h127.0.0.1 -P9030 -e 'ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");'
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/001_init.sql | docker compose -f deploy/doris/compose.yaml exec -T doris mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/002_usage_summaries.sql | docker compose -f deploy/doris/compose.yaml exec -T doris mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/004_summary_coordination.sql | docker compose -f deploy/doris/compose.yaml exec -T doris mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/005_consumer_recovery.sql | docker compose -f deploy/doris/compose.yaml exec -T doris mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/006_optional_route_identity.sql | docker compose -f deploy/doris/compose.yaml exec -T doris mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/007_schema_comments.sql | docker compose -f deploy/doris/compose.yaml exec -T doris mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
docker compose -f deploy/doris/compose.yaml exec doris mysql -uroot -h127.0.0.1 -P9030 -e "SHOW TABLES FROM metering_development;"
~~~

此独立 Compose 环境的 SQL、FE HTTP、BE HTTP 端口分别为 9030、8030、8040，统一绑定 `0.0.0.0`。远端 DBeaver 使用 MySQL 驱动连接 `服务器IP:9030`，用户 `root`、密码为空。`make local-up` 启动的采集联调栈使用独立端口 19030、18030、18040，详见[组件运行说明](../../docs/collection-components.md)。FE 会将 Stream Load 重定向至 BE；Docker Desktop 宿主机可能无法访问容器内部 BE 地址，联调时从 compose 网络内发起请求或使用可直达的 BE 地址。所有写入时间和 SQL 会话使用 UTC。

Schema 采用 Unique Key (request_date, logical_request_id, event_id, payload_hash)：同内容重放覆盖同一键，不同摘要保留冲突证据。两张明细表按 UTC 日期每日分区，在线保留一年：365 个日期（当天及前 364 天），dynamic_partition.start=-364、end=2；Doris 自动创建并周期清理。各 FE 的 dynamic_partition_enable=true 还须写入持久 fe.conf，ADMIN 修改仅更新运行配置；重启后重新核验。首次写入前确认日期分区实际已创建。003_raw_retention_one_year.sql 仅用于同模型保留期调整，不能将旧字段升级为当前模型；扩大窗口不会恢复已删数据。日/月汇总采用独立普通 RANGE 表，由待实现的 usage-worker 管理分区并原子发布；详见[汇总设计](../../docs/daily-monthly-summary-design.md)。归档为[可选设计](../../docs/archive-optional-design.md)，一期无 S3/MinIO 依赖。request_conflict_ids 长期保留 UUIDv4 冲突标记，避免较早证据过期后剩余变体重新变为有效用量。

Consumer 已实现，构建与本地链路见[组件运行说明](../../docs/collection-components.md)；汇总 Worker 和查询 API 留待后续工作包，建表不启动刷新任务。采集与汇总协调表见[004 Doris migration](../../migrations/doris/metering/004_summary_coordination.sql)，通知、消费批次凭据和发布状态保存在同一 Doris 集群，无额外 PostgreSQL 依赖；旧 PostgreSQL usage migration 已退役。调度采用独立 Go usage-worker，默认每分钟 CronJob --once；单个发布者保护、查询版本校验及旧状态切换见[Doris 协调设计](../../docs/doris-summary-coordination.md)。真实 Auth 属独立工作包，不是本次采集部署依赖。写入建议采用 Stream Load：每批固定 label，Expect: 100-continue、format: json、read_json_by_line: true、max_filter_ratio: 0、timezone: +00:00，检查响应 Status、行数和可见性，并确认冲突标记、日期通知及批次凭据后再提交 Kafka offset。label 保留期有限，长期重放依赖 Unique Key；不同内容仍必须保留为独立变体。未知 schema 版本暂停对应 Kafka 分区。

新开发模型统一部署，不接收旧格式或保留旧摘要比较路径。旧数据留在原库；新库按 001/002/004/005/006/007 初始化，006 不承担历史模型迁移。

本示例不自动执行 migration；避免服务启动时以 root 反复改表。生产采用受控迁移与独立账号。

参考：[Doris all-in-one 镜像](https://doris.apache.org/community/developer-guide/all-in-one-image/)、[Unique Key 模型](https://doris.apache.org/docs/4.x/table-design/data-model/unique/)、[动态分区](https://doris.apache.org/docs/4.x/table-design/data-partitioning/dynamic-partitioning/)、[Stream Load](https://doris.apache.org/docs/4.x/key-features/stream-load/)。

## Kubernetes 开发环境

单节点示例在 deploy/doris/doris.yaml，使用 StatefulSet 保存 FE 元数据和 BE 数据，服务名为 doris。集群需要默认 StorageClass。部署后先检查 BE Alive，再手动执行 migration；FE 的 HTTP 健康检查只能表明 FE 可用。

~~~sh
kubectl apply -f deploy/doris/doris.yaml
kubectl rollout status statefulset/doris --timeout=5m
kubectl exec doris-0 -- mysql -uroot -h127.0.0.1 -P9030 -e "SHOW FRONTENDS; SHOW BACKENDS;"
kubectl exec doris-0 -- mysql -uroot -h127.0.0.1 -P9030 -e 'ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");'
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/001_init.sql | kubectl exec -i doris-0 -- mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/002_usage_summaries.sql | kubectl exec -i doris-0 -- mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/004_summary_coordination.sql | kubectl exec -i doris-0 -- mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/005_consumer_recovery.sql | kubectl exec -i doris-0 -- mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/006_optional_route_identity.sql | kubectl exec -i doris-0 -- mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
python3 scripts/render_doris_sql.py --database metering_development migrations/doris/metering/007_schema_comments.sql | kubectl exec -i doris-0 -- mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030
kubectl exec doris-0 -- mysql -uroot -h127.0.0.1 -P9030 -e "USE metering_development; SHOW DYNAMIC PARTITION TABLES;"
~~~

集群内 SQL 地址为 doris:9030。这个无密码单节点示例仅用于隔离的开发命名空间；生产须使用 Doris Operator 或独立 FE/BE 集群、独立账户和 Secret 管理。

## 开发数据边界

本次只维护新契约，不提供旧 ClickHouse/旧事件模型迁移。已有数据留在原环境；为新模型另建隔离库及 Kafka topic/group，不把旧消息送入当前 Consumer，不重算或覆盖旧事件。清理脚本保留 Doris 验证库和共享 Compose 数据卷。
