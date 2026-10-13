# 一期日汇总与月汇总设计

状态：设计及 DDL，修订日期：2026-10-13。采集与汇总协调全部使用 Doris，无额外 PostgreSQL 依赖。刷新 Worker、Consumer 衔接和统计 HTTP API 尚待实现；建表不会自动生成汇总数据。

## 1. 一期范围与保留期

| 数据层 | 用途 | 分区与保留 |
| --- | --- | --- |
| events_raw | 原始计量事件、同内容重放去重、全部冲突证据 | request_date 按 UTC 日分区，在线保留一年，口径为 365 个日期（当天及前 364 天） |
| ingest_rejections | 有限拒收诊断，无原 payload | kafka_record_date 按 UTC 日分区，同样保留 365 个日期 |
| request_conflict_ids | UUIDv4 冲突隔离 | 长期保存，不随原始分区过期删除 |
| usage_daily | 客户日趋势、按日统计及月汇总输入 | stat_date 按日分区，独立保留 730 个 UTC 日期 |
| usage_monthly | 月趋势、客户/API Key/服务/模型排行 | stat_month 为 UTC 月首，按自然月分区，独立保留 24 个月（当月及前 23 月） |

原始明细只在一年窗口内查询。一年前的日/月统计可继续查询已生成的汇总，但不能从汇总恢复请求明细。汇总保留期采用原方案两年目标，不会随原始表的分区一起删除。按 UTC 请求开始日归属，跨午夜结束仍计入开始日；跨月同理。

一年采用滚动 365 日口径，不采用“上年同月同日”或固定自然年。Doris 动态分区使用 start=-364；未来两日的空分区不计入已发生数据的保留窗口。清理由周期调度执行，不承诺午夜瞬间完成。[动态分区说明](https://doris.apache.org/docs/4.x/table-design/data-partitioning/dynamic-partitioning/)

归档是[后续可选方案](archive-optional-design.md)，不纳入一期代码、部署、运行依赖或验收。

## 2. 汇总维度与数据口径

日/月采用相同的六个维度：customer_id、application_id、api_key_id、service_id、model、usage_source。唯一键为统计日期/月加这六个维度；不同层级报表通过 GROUP BY 合并，不另存容易重复累计的“客户总计行”。主体使用事件发生时的可信身份快照，密钥轮换或客户重命名不改历史归属。

- model 优先采用结果事件 response_model，其次 request_model；都未知时保存空串，由界面显示“未知模型”。
- usage_source 按输入、输出两项确定：reported、estimated_model、estimated_generic、mixed、unknown、missing。missing 表示没有结果事件；unknown 表示结果已有但输入或输出未知。一个上报、一个估算归 mixed。采集子项当前仅产生 reported/unknown；估算来源须先完成协议升级，再接入汇总。
- 一个 logical_request_id 计一次，不把 request_started 与 usage_observed 计成两次调用。只有结果事件时也记录一次已观察请求，并单独统计是否有开始事件。
- 同内容重放依靠原始表 Unique Key 去重。事件内容冲突、开始时间冲突、跨日期 UUID 冲突或身份冲突整请求排除，不用 MAX 任意挑选版本进入客户统计。
- 无效身份或缺少客户/应用/API Key 的请求不进入客户汇总。管理员数据质量页面从原始/冲突记录统计排除数量，不把它们归给空客户或其他客户。
- request_statistics 按日期与请求 ID 形成一行中间投影，先判断冲突再按客户分组；跨日期冲突依赖 Consumer 持久写入 request_conflict_ids。模型或客户过滤必须发生在冲突判断之后。

两张汇总表使用 Unique Key Merge-on-Write 保存整批结果，不使用对 Kafka 事件直接 SUM 的增量 Aggregate Key 表或同步求和物化视图。

## 3. 指标

| 字段 | 定义 |
| --- | --- |
| request_count | 已观察、身份有效且无冲突的逻辑请求数；不是独立网关请求基数 |
| started_request_count | 有唯一开始事件的请求数 |
| usage_observed_request_count / usage_missing_request_count | 有唯一结果事件 / 未收到结果事件的请求数 |
| eligible_usage_request_count / ineligible_usage_request_count | 结果唯一、usage_eligible=1、输入输出均已知的请求数 / 其余请求数 |
| http_2xx_request_count / http_error_request_count | 结果 HTTP 状态为 2xx / >=400 的请求数；不将未知状态视为成功 |
| terminal_missing_request_count | 未观察到请求终态的请求数 |
| input_tokens / output_tokens | 仅汇总有效用量候选的输入 / 输出 |
| total_tokens | 有效输入与输出之和；不额外加缓存读写、推理子项 |
| cached_read_tokens / cached_write_tokens / reasoning_tokens | 有效候选中已知的子项合计，分别展示，不能再次加到 total_tokens |
| reported_total_tokens | 有效候选中上游报告的总量，独立于推导 total_tokens |
| *_known_requests | 缓存读写、推理、上报总量各自有已知值的有效请求数 |
| refresh_id / source_watermark / refreshed_at | 本期完整批次 UUID、输入水位的 SHA-256、刷新时间 |

所有 token 合计使用 DECIMAL(38,0)，先提升类型再相加，避免 BIGINT 相加溢出。SUM 保留全未知时的 NULL；有上报零值则合计为 0，配合 known_requests 区分“有效零”和“没有已知值”。月汇总也不能 COALESCE 未知合计为零。

必须满足：request_count = usage_observed_request_count + usage_missing_request_count = eligible_usage_request_count + ineligible_usage_request_count；started_request_count <= request_count；每个子项 known_requests <= eligible_usage_request_count。HTTP 成功率如展示，须说明采用已知状态的分母，不能用未知或缺失状态补足成功数。

## 4. 日汇总刷新

由自行实现的独立 Go usage-worker 处理，不在业务请求路径中。默认每分钟由 K8s CronJob 运行一次 `usage-worker --once`，日/月发布及清理由同一发布者串行执行。通知、水位及任务状态均保存在 Doris；不新增 PostgreSQL 汇总状态库。表结构、调度保护、失败恢复及查询协议统一见[Doris 协调设计](doris-summary-coordination.md)。

1. Consumer 确认原始批次和冲突标记可见，再在 Doris 登记新事件日期、迟到结果开始日、跨日期冲突全部受影响日期的幂等通知，并确认批次凭据，最后提交 Kafka offset。Worker 尚未启用也登记通知；登记失败不提交，重放不累计 token。
2. Worker 获取尚未由成功任务确认的通知集合，完整持久化本次 refresh_inputs 与输入水位向量；不共享修改 dirty 布尔值或累加 generation。成功任务只确认其捕获的通知，新到通知留给下一轮。
3. 检查原始日期分区存在、日期在在线明细范围及实际采集起点内；缺失分区为覆盖缺口，不能用空汇总覆盖历史。初始化和每天进入新 UTC 日期时为覆盖范围内日期登记通知，包括零事件日。
4. 创建整日临时分区，从 request_statistics 计算完整结果；普通查询继续访问上一正式分区。检查指标恒等式、NULL/known、refresh_id、日期/唯一键、行数及校验摘要。
5. 记录任务 PREPARED、原正式/临时 PartitionId。先持久写当前状态 publication_uncertain=true，确认可见后执行 REPLACE PARTITION；DDL 与状态写入分阶段恢复，不能合并成一个 Doris 显式事务。
6. 核验正式分区身份、刷新 ID、行数及摘要，再发布当前状态的版本、水位、覆盖范围，清除 uncertain，确认可见后将 run 标为 PUBLISHED。零行同样替换旧分区并用 PartitionId 确认，才能移除已被隔离或维度变化的旧分组。
7. 准备期间可继续入库，本次只确认捕获集合；未确认通知仍 dirty。月任务下一轮通过日发布版本向量发现变化，不依赖跨表事务中的“月 dirty 更新”。

每分钟检查队列，优先当日和最近 72 小时，同时公平处理更早但仍在一年内的迟到通知，不只刷新最近三天。定期巡检在线 Doris 分区版本、持久冲突标记及通知，遗漏时登记修复通知。当天显示已确认水位，不预先锁定为最终值。Kafka committed offset 是恢复起点；Doris 水位按各分区连续成功范围推导，不是全局序号。

60 秒调度周期不是端到端 60 秒可见保证；还包含排队、消费、计算、校验和发布时间。以上整日 SQL 是正确性基线，日均一亿事件须压测；如扫描无法满足可见目标，需实现可重算小时中间结果/请求投影，再整日原子发布，不能把参考 SQL 当成性能验收。

## 5. 月汇总刷新

月汇总只合并已发布日汇总，计数及 token 均 SUM，不重新扫描全年原始事件。月份固定为 UTC 月首。

1. 每轮比较各月所需日的 published_refresh_id、published_version 和覆盖范围向量与上次月输入；日版本变化、首次生成或当前月新增覆盖日期即触发，包括当前月及历史修正月份。日发布后崩溃也不会丢失月刷新触发。
2. 明确实际采集开始日；月任务要求采集范围内每个已发生日期均有确认日覆盖，零事件日也须有状态。缺失一天保留上一成功月批次并提示缺口，不能默认为零。
3. 捕获排序后的日版本向量，存入任务并计算 source_watermark，从 usage_daily 准备整月临时分区。单个 Worker 在月准备至发布期间暂停日发布；前后仍须核对完整日向量，变化或 uncertain 时重建/暂停。
4. 分维度及全月校验月合计等于所用日合计，然后按与日相同的 PREPARED → PUBLISHING → 正式分区核验 → 当前状态发布 → run PUBLISHED 步骤发布。
5. 已结束月份可因迟到或冲突重新计算；超出日数据可重算范围时明确覆盖缺口，不覆盖原已确认月批次为零。这是采集事件纠正，不包括业务补报、正文补查或收费账单修改。

日/月两次发布是独立步骤。月接口显示依赖的日版本；日已更新而月尚未更新时提示“月统计刷新中”，不承诺跨接口任意时刻完全相等。

## 6. 发布状态、失败恢复与生命周期

Doris 的 [004 协调表](../migrations/doris/metering/004_summary_coordination.sql)保存日期通知、消费批次凭据、刷新输入、任务审计及每期当前状态。[协调设计](doris-summary-coordination.md)定义写入责任和恢复协议；Auth PostgreSQL 不参与这些操作。

- PENDING：有未确认通知或日版本变化，可展示上一成功批次和滞后。
- RUNNING / PREPARED：正在准备并校验临时结果，上一成功正式结果仍可查询。
- PUBLISHING / publication_uncertain=true：正式分区可能已经替换但尚未核验，该期统计返回暂不可用。
- PUBLISHED：正式分区身份、内容及当前发布状态已确认相符。
- FAILED：保留错误及上一成功信息；发布结果不明仍维持 uncertain。
- EXPIRED：覆盖已到保留边界，先更新状态版本再清理分区。

查询取消 PostgreSQL 共享/独占锁依赖，改为前后读取请求范围的完整 state_version/refresh_id/覆盖向量，在中间查询指定成功版本并缓冲响应；版本变化或 uncertain 时有限重试，失败返回暂不可用。零行需确认发布覆盖。崩溃恢复先核对活动任务、正式/临时 PartitionId 和刷新 ID，补齐状态/任务之间的中断；身份不明时保持 uncertain，不能猜测成功。

CronJob 的 Forbid 仅防同一调度对象并发，程序仍须统一发布者保护。K8s 共用 Lease，但 Lease 本身不能对 Doris DDL 强制 fencing；租约过期/旧 owner 状态不明时停止自动接管，确认旧进程及在途 SQL 结束后才恢复。可选单副本 Deployment 或单机 cron 与 CronJob 互斥启用，详见协调设计第 4 节。

汇总表使用普通 RANGE，由单个 Worker 提前创建当日及未来两日、当月及下月，并管理清理。日清理边界为 UTC 今日减 729 日，月为本月首减 23 月；先递增状态版本标记过期并确认无活动任务/在途发布，再清理。查询以版本校验发现清理变化。日/月独立保留，不随原始分区删除。

原始事件由 Doris 动态分区管理，过期后不能默认全量重建日汇总。已有更早日仍可参与月重算；日已到期或缺失时保留已确认月版本并标明重算限制。协调记录单独清理，保留未确认通知、活动输入、uncertain 证据及当前覆盖/版本；禁止按原始分区到期一并删除。上线前回填现存明细、逐日核验包括零事件日的覆盖，再核对月合计。

## 7. 查询契约（一期待实现）

- GET /v1/metering/events：一年内在线明细，只访问 Doris，不依赖归档。
- GET /v1/metering/usage/daily：UTC 日期 from/to 左闭右开，按客户/应用/API Key/服务/模型/来源过滤及分页。
- GET /v1/metering/usage/monthly：UTC 月首 from/to 左闭右开，按相同维度过滤及分页。
- GET /v1/metering/summary/status：管理员查看任务状态、水位及覆盖缺口。

客户身份从经过认证的主体注入；不能信任请求头或查询参数自报的 customer_id。日/月返回覆盖范围、数据水位、最后刷新时间、是否仍有 dirty 和缺失/未知指标。零行仅在发布状态确认覆盖时解释为“无已观察的有效请求”，不能解释为采集完整或真实调用量为零。SQL 查询只作为内部联调样例，数据库账号不对客户开放。

## 8. DDL、迁移与验收

- [001 原始表](../migrations/doris/metering/001_init.sql)：新建环境的一年动态日期分区。
- [002 汇总表与请求投影](../migrations/doris/metering/002_usage_summaries.sql)：日/月表与 request_statistics。
- [003 一年保留迁移](../migrations/doris/metering/003_raw_retention_one_year.sql)：已有表属性迁移；CREATE IF NOT EXISTS 不会更新旧表。
- [004 Doris 协调表](../migrations/doris/metering/004_summary_coordination.sql)：日期通知、批次凭据、任务输入/审计和发布状态；旧 PostgreSQL usage 迁移已退役。
- [日准备](../scripts/sql/refresh_usage_daily.sql)、[日发布](../scripts/sql/publish_usage_daily.sql)、[月准备](../scripts/sql/refresh_usage_monthly.sql)、[月发布](../scripts/sql/publish_usage_monthly.sql)：分阶段执行的参考模板，必须由未来 Worker 校验和协调。

迁移与旧 PostgreSQL 试验状态的受控切换见[协调设计第 7 节](doris-summary-coordination.md#7-迁移与验收)。迁移前停止旧汇总/归档 Worker、CronJob 和其他分区清理器。开启各 FE dynamic_partition_enable 并持久化配置；确认分区范围与调度状态后开放 Consumer 写入。扩大期限不能恢复此前已删除的数据。SQL 模板参数只能由 Worker 生成，并验证 ISO 日期、允许的表名、分区名、UUID 与 64 位十六进制水位，不把客户输入直接拼进 SQL。

验收必须覆盖：同内容重放不增量；冲突到达撤销先前分组；开始/结果跨午夜及跨月；缺失结果、无效身份、有效零值与 NULL；混合来源；各维度月合计等于日合计；零行重算删除旧分组；临时准备失败及发布后崩溃；并发新事件不丢 dirty；一年前日/月仍可查；730 日与 24 月边界；客户隔离、查询前后版本变化、CronJob 重复执行、发布者保护失效及保守接管、各 Doris 状态写入分别失败、日发布后崩溃仍触发月刷新、元数据容量及目标峰值压测。

[临时分区与原子替换](https://doris.apache.org/docs/4.x/data-operate/delete/table-temp-partition/)是本发布设计采用的 Doris 机制；当前没有实际数据库连接，DDL 与模板仍需在目标 Doris 4.1.3 验证。
