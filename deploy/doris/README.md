# Apache Doris 开发环境与初始化

本目录使用 Apache Doris 4.1.3 all-in-one 镜像，提供单 FE、单 BE 的开发环境。生产副本、分桶和资源需按目标集群容量验证；仓库默认副本数 1。

迁移只定义当前最终模型，按以下顺序执行，不保留历史补丁或旧结构兼容路径：

| 脚本 | 对象及作用 |
| --- | --- |
| `001_init.sql` | 原始事件、持久冲突、拒收诊断；事件冲突、事件明细、请求诊断及有效用量四个逻辑视图 |
| `002_consumer_coordination.sql` | 重算通知、消费批次凭据、消费起点及缺口四张表 |
| `003_usage_summaries.sql` | 统一请求统计投影、日/月异步物化视图、刷新任务/输入/状态三张表 |

最终共 10 张实体表、5 个逻辑视图、2 个异步物化视图。完整职责和字段见[数据库字典](../../docs/doris-database-schema.md)。名称快照、全部字段和注释直接随初始化创建。旧日/月实体表、旧临时分区发布状态和转发视图不再创建，也不提供 PostgreSQL usage 迁移。

旧开发结构需要选择隔离空库重建；初始化不会自动删库或转换旧数据。`IF NOT EXISTS` 只支持重复执行相同结构，不会更新已有对象。Consumer 的 `doris.database` 与 DSN 数据库名必须一致；本地默认 `metering_development`，Kubernetes 验证使用独立库。

## Docker Compose

在仓库根目录执行，SQL 管道使用 `pipefail`，避免渲染失败被忽略：

```sh
set -o pipefail
docker compose -f deploy/doris/compose.yaml up -d --wait
docker compose -f deploy/doris/compose.yaml exec doris mysql -uroot -h127.0.0.1 -P9030 \
  -e 'SHOW FRONTENDS; SHOW BACKENDS;'
# 各 FE 均需持久开启动态分区；确认 BE 为 Alive 后初始化。
docker compose -f deploy/doris/compose.yaml exec doris mysql -uroot -h127.0.0.1 -P9030 \
  -e 'ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");'
for migration in 001_init.sql 002_consumer_coordination.sql 003_usage_summaries.sql; do
  python3 scripts/render_doris_sql.py --database metering_development \
    "migrations/doris/metering/$migration" | \
    docker compose -f deploy/doris/compose.yaml exec -T doris \
      mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030 || exit 1
done
docker compose -f deploy/doris/compose.yaml exec doris mysql -uroot -h127.0.0.1 -P9030 \
  -e 'SHOW TABLES FROM metering_development;'
```

此独立环境的 SQL、FE HTTP、BE HTTP 端口为 9030、8030、8040，绑定 `0.0.0.0`。远端 MySQL 客户端连接 `服务器IP:9030`；开发用户 root、密码为空。`make local-up` 的采集联调栈使用独立端口 19030、18030、18040，详见[组件运行说明](../../docs/collection-components.md)。FE 会将 Stream Load 重定向至 BE，客户端还需能访问 BE 地址。

## 存储与统计

`events_raw` 使用 Unique Key `(request_date, observation_id, event_id, payload_hash)`：同内容重放幂等，不同摘要保留冲突证据。原始事件和拒收诊断按 UTC 日期分区，Doris 自动预建当天及未来两日，usage-worker 按月首清理。原始、日、月统计统一保留前 12 个完整自然月加当月。冲突标记、通知与消费恢复事实独立保留，防止证据过期后冲突请求恢复有效。

Consumer 确认原始/拒收、冲突、通知和批次凭据全部可见后才提交 Kafka offset。Stream Load 每批固定 label，使用 `format: json`、`read_json_by_line: true`、`max_filter_ratio: 0`、`timezone: +00:00`，核对状态、行数和可见性。未知 schema 暂停对应分区。

日/月视图均延迟构建、手动指定分区刷新。独立 Go usage-worker 每天 01:00 UTC 生成昨日统计并修正历史日期，月视图嵌套读取日视图。初始化不启动计算；任务配置与历史回填见[调度与恢复](../../docs/doris-summary-coordination.md)。统计 HTTP API 尚未实现。

## Kubernetes

单节点示例 [doris.yaml](doris.yaml) 使用 StatefulSet 保存 FE 元数据和 BE 数据，需要默认 StorageClass。服务名 `doris`，集群内 SQL 地址 `doris:9030`。

```sh
set -o pipefail
kubectl apply -f deploy/doris/doris.yaml
kubectl rollout status statefulset/doris --timeout=5m
kubectl exec doris-0 -- mysql -uroot -h127.0.0.1 -P9030 \
  -e 'SHOW FRONTENDS; SHOW BACKENDS;'
kubectl exec doris-0 -- mysql -uroot -h127.0.0.1 -P9030 \
  -e 'ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");'
for migration in 001_init.sql 002_consumer_coordination.sql 003_usage_summaries.sql; do
  python3 scripts/render_doris_sql.py --database metering_development \
    "migrations/doris/metering/$migration" | \
    kubectl exec -i doris-0 -- mysql --default-character-set=utf8mb4 -uroot -h127.0.0.1 -P9030 || exit 1
done
kubectl exec doris-0 -- mysql -uroot -h127.0.0.1 -P9030 \
  -e 'USE metering_development; SHOW DYNAMIC PARTITION TABLES;'
```

这些命令面向隔离开发环境。服务启动不自动以 root 改表；生产初始化使用独立账号和 Secret，并单独完成容量与 G0–G5 验收。
