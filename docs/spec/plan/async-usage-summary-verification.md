# T+1 异步用量汇总验证记录

日期：2026-10-28。环境：本地 Docker Compose，Apache Doris all-in-one 4.1.3；统计日期按 UTC。本记录区分代码检查、真实本地联调、静态清单检查与生产验收。

## 实现交付

- `usage_daily_mv` 按日期与八个维度聚合；`usage_monthly_mv` 嵌套读取日视图。
- 手动指定分区刷新，严格只计算已结束日期；昨日及历史通知独立调度。
- `usage_eligible` 不再依赖客户身份；历史在投影中按当前质量重算，原始 JSON/摘要不变。
- 保留前 12 个完整自然月＋当月；Doris 预建日分区，Worker 按月首清理。
- 输入集合、Doris 任务 ID、日版本和月依赖向量持久化；确认丢失、历史缺失和零行结果可恢复。
- 单机文件锁、K8s Lease 与保守接管。当前镜像无法证明多 FE 全部 SQL 已结束时拒绝自动接管；不得把 Lease 超时当作强制 fencing。
- 交付配置、镜像、CronJob/Lease/RBAC、SQL 查询、当前模型迁移和离线包。

## 检查结果

| 检查 | 结果与范围 |
| --- | --- |
| `make build wasm` | 通过，生成四个 Go 二进制及 Wasm，ABI 检查通过 |
| `make test` | 通过，90 个 Python 测试、主模块和插件 Go 竞态测试 |
| `make summary-test` | 通过，真实隔离 Doris 库；最终一轮约 25 秒 |
| `GOFLAGS='-count=1' make local-test` | 在独立验证消费组下串行完整通过，约 446 秒 |
| Worker Docker 镜像 | 实际构建并纳入三应用离线镜像归档 |
| 包内 SHA256SUMS | 全部通过；归档包括 Observer、Consumer、usage-worker，依赖镜像未归档 |
| Kubernetes / EnvoyFilter | 全部最终清单 client dry-run 与 Schema 校验通过；未部署生产 |
| `git diff --check` | 通过 |

独立 Doris 测试覆盖历史资格、未知客户与已知服务、NULL/零、当天不计算、冲突撤回、空结果移除旧分组、日/月逐维度对账、任务成功但状态未写入、输入捕获不完整、任务历史丢失后安全重算、完整自然月清理及月初归属。测试使用合成事件，退出后删除本次隔离库。

首次直接运行旧联调栈时，原消费组报告 watermark_untrusted 并暂停；该轮未通过。验证未跳过原组恢复屏障，也未修改原组水位。随后使用临时独立消费组消费同一本地 topic，串行重跑全部真实链路测试通过；临时进程已结束。原组恢复状态保持原样，不能将此次通过解释为原组已恢复。

## 产物与运行

本次最终包位于 `dist/async-summary-package-final/`，包含已渲染的 `k8s/50-usage-worker.yaml`、001/002/003 最终初始化 SQL、三应用镜像、根 EnvoyFilter、manifest 和 SHA256SUMS。包为单节点开发验证配置，默认任务时间 01:00 UTC，控制面时区须核对为 UTC。

运行前必须把 collection_start_date 设为实际采集开始日期；示例/本次包中的 2026-10-28 仅表示新环境起点，不能用于声称已有历史完整覆盖。CLI 及回填操作见 [调度说明](../../doris-summary-coordination.md)。

详细诊断保存在本地 `.tmp/async-summary/`：make-test.log、summary-test.log、local-test-final.log、package-checksums.log、package-k8s-dry-run.log 等。这些不是生产运行记录。

## 尚未验证

生产峰值下的扫描成本、执行预算、多 FE 故障接管、目标 Kubernetes 集群中的租约故障注入，以及生产 G0–G5 验收均未完成。统计 HTTP API 不在本次交付范围。

迁移清理后，旧日/月实体表和旧发布状态已从初始化链删除，离线包中的迁移 ConfigMap 与初始化 Job 已同步替换。后续空库及视图合并验证见[迁移精简验证记录](database-schema-cleanup-verification.md)。
