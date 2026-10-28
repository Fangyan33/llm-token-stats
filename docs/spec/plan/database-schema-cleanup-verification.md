# 数据库迁移精简验证记录

日期：2026-10-28。环境：本地 Apache Doris all-in-one 4.1.3，UTC；全部数据库写入验证使用本次新建的隔离库，结束后删除这些测试库。未删除或转换既有开发库、历史库及消费恢复事实。

## 最终初始化结构

仅保留三个脚本，全部直接创建最终定义，不再执行历史 ALTER 补丁或重复替换视图：

| 顺序 | 文件 | 职责 |
| --- | --- | --- |
| 1 | `001_init.sql` | 原始事件、冲突标记、拒收诊断；冲突、明细、请求及有效用量逻辑视图 |
| 2 | `002_consumer_coordination.sql` | 日期重算通知、消费批次凭据、可信起点及缺口 |
| 3 | `003_usage_summaries.sql` | 请求统计投影、日/月物化视图、刷新任务/输入/状态 |

最终包含 10 张实体表、5 个逻辑视图、2 个异步物化视图。删除对象和依赖处理如下：

- 删除旧 `usage_daily`、`usage_monthly` 实体表定义，以及准备/发布临时分区的四份参考 SQL。
- 删除旧 `summary_refresh_inputs`、`summary_refresh_runs`、`summary_refresh_state`；保留当前 Worker 使用的 `summary_mv_refresh_*`。
- 将 `request_statistics_mv` 与旧 `request_statistics` 合并为一个 `request_statistics`，保留当前质量资格和未知归属统计规则。
- 删除 `event_variants`、`events_valid` 两层视图；`event_conflicts` 和 `requests` 直接读取原表，`event_details` 直接排除同事件冲突。
- 删除固定天数保留、身份投影覆盖、字段注释和名称补列等历史迁移，以及 PostgreSQL usage 空脚本；最终列、注释、保留参数直接定义在初始化中。

本地、Kubernetes 和离线分发入口共用同一迁移列表。旧开发结构需选择隔离空库重建；工具不会自动删库或补列。离线包初始化 Job 会在执行 DDL 前检查已有对象与列的布局。

## 验证结果

| 检查 | 结果及范围 |
| --- | --- |
| 精简 SQL 空库初始化与重复执行 | 真实 Doris 连续执行两次，通过；均只有预期 17 个对象，全部字段、注释及实体类型与源定义一致 |
| 渲染后的离线初始化 Job | 真实 Doris 连续执行两次，通过；增加额外对象后能在 DDL 前拒绝不匹配结构 |
| `make summary-test` | 通过；日/月嵌套、T+1、历史资格、NULL/零、冲突修正、恢复与自然月保留；新增直接明细视图的有效请求及内容冲突过滤检查 |
| `make test` | 通过；89 个 Python 测试，以及主模块和插件 Go 竞态测试 |
| 分发包测试 | 13 个测试通过，覆盖迁移顺序、渲染、镜像范围与校验和 |
| 采集集成测试编译 | 使用 integration 标签编译通过，本轮不执行网关链路用例 |
| 数据库字典 | 从真实隔离库元数据重新生成，源定义与文档一致 |
| 既有离线包 | 更新迁移 ConfigMap、初始化 Job、说明及清单元数据；保留应用镜像归档，SHA256SUMS 全部通过 |
| Kubernetes 清单 | 更新后整包客户端 dry-run 及 Schema 校验通过，未部署到集群 |
| `git diff --check` | 通过 |

本轮没有修改网关或采集运行逻辑，未重跑完整网关链路测试；本地初始化、真实 SQL 与汇总测试不代表生产容量或 G0–G5 验收。诊断日志位于 `.tmp/schema-cleanup/`。
