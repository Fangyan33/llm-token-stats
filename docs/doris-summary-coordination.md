# Doris 日/月统计调度与恢复

采集使用 Kafka 与 Doris；T+1 汇总由 `usage-worker` 调度 Doris 异步物化视图，不新增 PostgreSQL。Consumer 的入库屏障顺序保持为：原始/拒收可见 → 冲突标记可见 → 日期通知可见 → 批次凭据可见 → Kafka offset 提交。Consumer 不等待汇总。

## 状态与责任

| 对象 | 写入者及作用 |
| --- | --- |
| summary_dirty_notifications | Consumer 记录日期变更；INGEST、CONFLICT 通知幂等 |
| consumer_batch_receipts / consumer_partition_* | Consumer 的处理凭据、起点和缺口；不等于汇总版本 |
| summary_mv_refresh_runs | Worker 的刷新 UUID、目标分区、任务 ID、输入摘要和恢复状态 |
| summary_mv_refresh_inputs | Worker 捕获的精确日期通知集合，仅关联 SUCCEEDED 日任务的输入才被确认 |
| summary_mv_refresh_state | 每期最近成功版本、覆盖、活动任务和月份依赖的日版本集合 |

runs/state 的 record_json 保存结构化恢复字段。PENDING/RUNNING/UNCERTAIN 表示尚未确认；SUCCEEDED 表示任务和指标核验通过；FAILED 表示明确失败；GAP 表示缺少源分区或日覆盖；EXPIRED 表示超过自然月窗口。coverage_start/coverage_end 表示已执行统计计算的日期范围，不证明 Kafka 没有丢失或积压；需同时检查 consumer_partition_gaps 与 lag。最近成功 ID/时间不因开始新任务而清空，但查询不能把运行中的当前 MV 当成已确认的旧快照。

日任务将输入集合、个数和摘要持久化后才提交 SQL。捕获中断时放弃未提交任务，通知继续待处理。刷新完成后先保存成功覆盖，再将 run 置为 SUCCEEDED；中断由下轮根据 Doris 任务恢复。月任务比较完整日版本集合，不依赖一条可能丢失的“月 dirty 更新”。

## 运行命令

```sh
make build
# DORIS_DSN 由环境或 Secret 提供；不得写入仓库。
dist/usage-worker --config configs/usage-worker.local.json --once
# 区间左闭右开，结束日期不得超过 UTC 今日。
dist/usage-worker --config configs/usage-worker.local.json --once \
  --backfill-from 2026-10-01 --backfill-to 2026-10-28
```

先将 collection_start_date 设为该库实际采集开始日期；本地示例值不代表所有库的采集起点。默认刷新等待 1800 秒、任务轮询 2 秒、整轮预算 21600 秒。首次回填及停机恢复自动处理尚未成功的已结束日期；缺口不能当作零。

生产入口为 Kubernetes CronJob，每天 01:00 UTC、Forbid、单任务、失败重试。现有 Kubernetes 版本未使用 spec.timeZone，部署时必须核对控制面为 UTC。离线包渲染 `k8s/50-usage-worker.yaml`，可直接 apply；已有环境使用打包参数 --collection-start-date 固定真实起点。

## 并发与接管

所有 K8s 任务入口共用一个预建 Lease。holder 包含 Pod 名、Pod UID 和本次进程 UUID。每 20 秒续约，续约失败立即停止新操作；超过 45 秒无续约确认也停止。Lease 不是 Doris 强制 fencing：过期接管前必须确认旧 Pod 已终止或消失，且 Doris 没有在途刷新及该库在途 SQL（单 FE 检查 PROCESSLIST；多 FE 无法证明全部 SQL 结束时停止自动接管，须人工核验后清理旧 Lease）；无法确认则退出告警。手工补算使用相同配置与 ServiceAccount 创建 Job。

本地模式使用所有入口共用的文件锁，只适合单机/隔离库，不能与 K8s 同库混用。Forbid 仅约束同一 CronJob，不能独立代替 Lease。

## 不确定结果和失败

Worker 提交前记录既有任务 ID 基线，只匹配新出现且目标完全相同的手动任务。提交响应丢失时先查询 tasks('type'='mv')，不能根据全局最近一次成功或 SQL 返回确认通知。任务 ID 一旦确定，后续只检查它。任务完成集合必须与目标分区一致。

任务历史缺失时，确认没有其他在途任务后将该日期留待下一轮强制重算，不伪造成功。超时/进程中断保留恢复信息，下一轮先恢复任务再清理或计算其他日期。零行结果同样记录覆盖，防止旧分组残留。明确失败、覆盖缺口和未知提交状态分别保留诊断。

执行期间 Consumer 可继续入库。新通知下一轮修正，最后成功刷新时间不代表 Kafka 已消费到最新；数据是已观察到的用量统计，不是完整网关调用基数。不得使用 MAX(offset) 宣称连续消费水位。

## 迁移与交付

当前模型按 001_init.sql、002_consumer_coordination.sql、003_usage_summaries.sql 初始化。001 定义原始事实及明细诊断，002 定义通知、消费凭据和恢复事实，003 定义统一 request_statistics 投影、日/月物化视图与刷新状态。全部采用最终定义，不再创建旧汇总表，也不执行历史补丁。

旧开发结构需要选择隔离空库重建；初始化工具不会自动删除数据库或升级旧数据。建库后部署 Consumer、Observer，最后开启汇总任务及历史回填。

查询示例见 scripts/sql/query_usage_mv.sql，状态查询见 scripts/sql/summary_mv_status.sql。监控至少检查最后成功统计日期、最老未完成日期、失败/不确定任务、待更新月份和 GAP；K8s 任务退出非零应告警。
