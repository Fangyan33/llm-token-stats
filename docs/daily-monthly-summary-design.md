# 一期日汇总与月汇总设计

状态：设计及 DDL，修订日期：2026-10-13。刷新 Worker、Consumer 衔接和统计 HTTP API 尚待实现；建表不会自动生成汇总数据。

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

由独立 usage-worker 实现，不在业务请求路径中。任务粒度为一个 UTC 日期；同日期任务串行，不同日期可并行。

1. Consumer 确认原始批次可见，识别当前批次请求的全部历史变体并持久化跨日期/身份冲突。将新事件日期、迟到结果的开始日、冲突请求涉及的所有日期标记 dirty；这些步骤确认完成后才提交 Kafka offset。Doris 已写成功而 dirty 登记失败时不提交 offset，重放仍走原 Unique Key，重新登记 dirty 不重复累计 token。
2. PostgreSQL 的 summary_refresh_state 记录 dirty_generation 和 published_generation；重复 dirty 通知可以触发额外重算，但不会累计额外用量。记录 Kafka 各分区已经确认的水位；不得声称各分区 offset 构成单一全局序号。
3. Worker 捕获当前 generation 和输入水位。要求原始日期分区存在，且日期在当前明细保留范围及实际采集起点内；缺失分区是覆盖缺口，不能用空汇总覆盖历史。分区存在但没有有效请求时可以生成零行结果，并在状态表发布该日期的覆盖状态。
4. 创建整日临时分区，从 request_statistics 计算该日完整结果。普通查询继续读取上一份正式分区。
5. 检查指标恒等式、NULL/known 数关系、同批次 refresh_id、日期/唯一键及行数。零行也必须执行原子替换，才能删除以前存在、后来被冲突隔离或维度改变的分组。
6. 获取该日期的短期发布独占锁，先在 PostgreSQL 提交 publication_uncertain=true，再以 REPLACE PARTITION 原子替换正式日分区；验证正式分区身份及刷新 ID。SQL 准备与发布模板分开，不得直接把临时准备成功当成完成。
7. 一个 PostgreSQL 事务发布 generation、水位、refresh_id、时间并清除 uncertain，同时把对应月份标记 dirty。准备期间有新事件到达，可以发布所捕获的 generation，但仍保留后续 dirty，继续重算；不能因有并发写入永久拒绝发布，也不能错误清空新通知。

任务建议每 60 秒领取 dirty 日期，优先当日和最近 72 小时；更早但仍在一年内的迟到数据同样加入队列，不只刷新最近三天。定期扫描保留范围内 Doris 分区版本和持久冲突标记，核对 dirty 遗漏；恢复时补登记，不能用“每天只跑昨天”漏掉重放。当天值显示“截至水位”，不预先锁定为最终值。

以上整日 SQL 是正确性基线。日均一亿事件下，整日反复扫描可能无法满足 60 秒目标，必须压测；如不达标，一期实现需增加可重算的小时中间结果/请求投影，日汇总再从完整小时批次生成，仍使用整日原子发布。不得把参考 SQL 的存在视为性能已验收。

## 5. 月汇总刷新

月汇总只从已发布日汇总合并，所有计数和 token 合计均为 SUM；不重新扫描全年原始事件。月份标识固定为 UTC 月首。

1. 日批次每次成功发布都标记其所属月份 dirty；刷新包括当前月、上月以及任何受历史日修正影响的旧月。
2. 期初回填需要明确实际采集开始日。月任务核对该月采集范围内每个已发生日期的日发布状态，零事件日也须有状态；缺少一天就保留上一月批次并提示不完整，不能静默把缺失日当零。
3. 捕获每日 published_refresh_id/generation 的有序向量并计算 source_watermark，从 usage_daily 构建整月临时分区。临时准备前后核对输入向量，避免混合不同日版本。
4. 发布时按固定顺序获取月份发布锁、该月日期发布共享锁，再次核对日向量。变化则重建；稳定后执行整月原子替换并发布状态。日发布短暂等待此操作，计算阶段不长期持有日发布锁。
5. 月合计必须等于对应日合计，分维度和全月均校验。已结束月份仍可因 Kafka 迟到或冲突检测重新计算；这属于事件去重纠正，不提供业务补报、正文补查或收费账单修改。

日/月两次发布不能视为同一分布式事务。月接口显示其依赖的日水位；日已更新而月尚未更新时明确提示“月统计刷新中”，不承诺任意时刻跨接口完全相等。

## 6. 发布状态、失败恢复与生命周期

[PostgreSQL schema](../migrations/postgres/usage/001_summary_refresh_state.sql) 保存每期当前状态和任务审计，复用一期可靠 PostgreSQL 基础设施，按权限隔离于 Auth；不需要归档状态库或对象存储。

- PENDING：有待处理 generation；可查询上次成功批次并展示水位。
- RUNNING：准备临时结果；正式结果仍可读。
- PUBLISHED：已确认正式 Doris 分区与状态相符。
- FAILED：准备或发布失败，保留错误类别和上一成功批次信息。
- publication_uncertain=true：可能已替换 Doris、但状态尚未确认，相关统计接口返回暂不可用；不能输出未经确认的零值或旧批次。

查询服务在读取状态和正式分区期间持有对应发布共享锁。Worker 在短期替换/状态提交阶段持有独占锁；PostgreSQL 连接丢失或锁失效时拒绝成功返回。进程在原子替换后退出时，恢复任务通过正式分区和 refresh_id 确认结果；零行日期结合记录的原分区身份、新分区身份与加载/替换状态确认，无法确认则重新准备并发布，不能猜测成功。

汇总分区使用普通 RANGE，由 usage-worker 创建和清理，以免动态分区调度与临时发布相互争用。提前创建当日及未来两日、当月及下月；初始化/历史回填显式创建缺少的汇总分区。日清理边界为 UTC 今日减 729 日，月清理边界为本月首减 23 月；先确认无运行任务/查询发布锁，再清理过期分区与状态。日/月各自独立保留，不将原始分区删除通知当成汇总删除通知。

原始事件由 Doris 动态分区直接管理，过期后不再支持日汇总默认全量重建。已发布的更早日汇总继续服务月重算；缺失且原始已过期的日数据要标记覆盖缺口，不能生成伪造零值。上线前回填现存一年明细、核对全日/全月并确保持续刷新告警有效。

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
- [日准备](../scripts/sql/refresh_usage_daily.sql)、[日发布](../scripts/sql/publish_usage_daily.sql)、[月准备](../scripts/sql/refresh_usage_monthly.sql)、[月发布](../scripts/sql/publish_usage_monthly.sql)：分阶段执行的参考模板，必须由未来 Worker 校验和协调。

迁移前停止之前的归档 Worker/CronJob 和其他分区清理器。开启各 FE dynamic_partition_enable 并持久化配置；确认分区范围与调度状态后开放 Consumer 写入。扩大期限不能恢复此前已删除的数据。SQL 模板参数只能由 Worker 生成，并验证 ISO 日期、允许的表名、分区名、UUID 与 64 位十六进制水位，不把客户输入直接拼进 SQL。

验收必须覆盖：同内容重放不增量；冲突到达撤销先前分组；开始/结果跨午夜及跨月；缺失结果、无效身份、有效零值与 NULL；混合来源；各维度月合计等于日合计；零行重算删除旧分组；临时准备失败及发布后崩溃；并发新事件不丢 dirty；一年前日/月仍可查；730 日与 24 月边界；客户隔离、发布锁故障及目标峰值压测。

[临时分区与原子替换](https://doris.apache.org/docs/4.x/data-operate/delete/table-temp-partition/)是本发布设计采用的 Doris 机制；当前没有实际数据库连接，DDL 与模板仍需在目标 Doris 4.1.3 验证。
