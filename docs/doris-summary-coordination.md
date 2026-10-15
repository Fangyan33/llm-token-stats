# Doris 采集与汇总协调设计

修订日期：2026-10-13。状态：设计与参考 DDL；Consumer、usage-worker、查询 API 和调度保护尚待实现，未执行目标集群验收。

采集与汇总使用 Kafka 和 Doris，通知、处理水位、刷新输入及发布状态全部保存在 Doris。无需为这条链路新增 PostgreSQL 实例、schema、连接或账号。Auth 的 PostgreSQL 仍按[鉴权方案](api-key-auth-service-implementation.md)保存身份和密钥，不参与计量汇总协调。统计口径、保留期及查询 API 见[日/月汇总设计](daily-monthly-summary-design.md)。

## 1. 数据表与写入责任

配套 [004 Doris 协调表](../migrations/doris/metering/004_summary_coordination.sql)使用 Unique Key Merge-on-Write；当前副本数及桶数仅为开发示例。所有时间使用 UTC，coverage_start/coverage_end 是左闭右开的已确认日期范围；DAY 为当天至次日，MONTH 从实际采集起点与月首的较晚者至本次已确认末日的次日，不包含未来日期。日任务 input_count/input_digest 对应精确通知集合，月任务对应排序后的日版本集合。枚举、摘要、月份首日、JSON 结构和状态转换由程序校验，不把建表视为这些业务规则已实现。

| 表 | 键及含义 | 写入者 |
| --- | --- | --- |
| summary_dirty_notifications | (stat_date, notification_id)，不可变的日期变更通知；dirty 表示存在尚未确认处理的通知 | Consumer；补偿巡检与初始化任务 |
| consumer_batch_receipts | (kafka_topic, kafka_partition, first_offset, last_offset, batch_id)，已完成入库、冲突维护及通知登记的批次凭据 | 该分区 Consumer |
| summary_refresh_inputs | (period_type, period_start, refresh_id, notification_id)，本次日任务捕获的精确通知集合 | 单个 usage-worker |
| summary_refresh_runs | (period_type, period_start, refresh_id)，任务阶段、输入向量、临时/正式分区身份和校验结果 | 单个 usage-worker |
| summary_refresh_state | (period_type, period_start)，当前成功版本、覆盖范围、活动任务及发布不确定标记 | 单个 usage-worker |

Consumer 不修改 Worker 的发布状态；Worker 不清空 Consumer 共享的 dirty 布尔值，也不通过并发读改写累加 generation。通知行只做相同内容的幂等写入。任务审计和当前状态仅由一个发布者按确认顺序写入。

`notification_id` 为版本化规范元组的 SHA-256：通知版本、UTC 日期、原因、来源标识及证据摘要。INGEST 来源标识为冻结批次 batch_id，证据是该批影响该日的事件摘要集合；每批每日期合并为一条通知。CONFLICT 按受影响日期合并该批发现的 UUID 及排序后变体摘要，新冲突证据产生新通知。INITIALIZE 按采集起点版本和日期确定；REPAIR 按日期、巡检原因及证据版本确定。时间、Pod ID、随机数和可变的 ingested_at 不参与通知 ID；这些时间仅为审计信息，不作为消费通知的截断条件。同一冻结批次重试幂等；重启重新分批可产生额外通知和重算，不能重复累计用量。跨日期冲突必须通知每个受影响日期。

批次 `batch_id` 是版本化的 topic、partition、offset 范围及按 offset 排序的原消息摘要/校验分类的 SHA-256；重新分批可生成不同批次凭据，通知按冻结批次/日期幂等，不依赖跨次分批边界完全一致。凭据的 notification_digest 是本次实际登记的排序后 (stat_date, notification_id) 集合摘要，notification_count 是该集合大小。同一 batch_id 重试发现新增冲突证据时，先补齐新通知，再幂等更新其通知集合凭据；凭据不是队列消费标记，Worker 仍逐项确认通知。无有效事件的诊断批次也须有凭据，通知集合可以为空。

## 2. Consumer 提交屏障与水位

每个 Kafka 分区顺序处理，步骤如下：

1. 冻结原消息、规范事件及拒绝诊断，按[采集闭环](gateway-collection-design-closure.md#5-consumer-提交与恢复屏障)的固定 label 规则确认所有必要 Doris 写入可见。
2. 跨在线分区回查本批 UUID，确认持久冲突标记可见；通知涵盖新事件日期和冲突请求涉及的全部日期。
3. 写入上述日期通知，检查数量、摘要和可见性。从 Consumer 首次上线就登记通知，即使汇总 Worker 尚未启用。
4. 写 consumer_batch_receipts，记录 first_offset、last_offset、next_offset=last_offset+1 及通知集合摘要；确认可见。来源消息与日期通知、批次凭据是分别确认的写入，失败时保留 offset 重走屏障。
5. 仅提交本分区连续成功段的下一 Kafka offset；分区 owner 已撤销时禁止新写入或提交。

Kafka 消费组 committed offset 是消费恢复起点。Doris 凭据表示该批次已完成计量处理，可能领先于 Kafka 提交进度，不能代替 committed offset 任意跳读。查询或巡检按分区的已知起点合并连续确认范围，再推进 next_offset；不能把任意 MAX(offset) 当成没有缺口的证明，不把多个 Kafka 分区相加成全局水位。未知 schema、未确认批次或保留期缺口阻止相应连续水位推进；永久非法消息须按拒绝诊断规则完成登记后才能越过。

重启先从 Kafka 的提交位置重放，同内容写库和通知均幂等。分区移交后允许确认已启动事务的结果，不允许旧 owner 启动下一批或提交。旧 owner 的晚到凭据不覆盖新的单行“当前水位”，水位由批次事实推导。补偿巡检使用持久凭据、Kafka 进度及 Doris 在线分区，禁止仅使用内存中的最近 UUID。

## 3. 捕获输入与确认通知

日任务先读出尚未被成功任务确认的通知，把精确集合分页写入 summary_refresh_inputs，并记录 count/digest；避免把全天通知装入内存，全部可见后才开始计算。成功判据是关联任务 status=PUBLISHED，且该任务曾完成正式分区核验与状态发布；RUNNING、PREPARED、PUBLISHING、FAILED 的输入均不能视为处理完成。同一通知可能被多次尝试捕获，只有成功发布确认它。

Worker 对该日完整 request_statistics 重算，不按通知数量增加 token。初始化逐日登记通知，包括实际采集范围内的零事件日；每天进入新 UTC 日期后补齐当天初始化通知。实际采集起点、缺失分区和明细已到期均须显式记录，不能把缺口当零。

计算期间 Consumer 可继续写入。成功任务只确认其已捕获集合；后来出现的通知继续待处理，即使结果恰好已经包含相应原始数据。日 source_watermark 是已确认通知集合和连续分区水位向量的规范摘要，用于追溯处理进度；它不是全局 Kafka 截止 offset，也不声称整日查询严格排除了向量之后的可见数据。任务保留完整输入向量，结果行保存摘要。

月任务不依赖“日发布后另写一条月 dirty”成功。按 UTC 日期排序读取该月所需日的 (stat_date, published_refresh_id, published_version, coverage_start, coverage_end) 向量，与上次月任务记录的向量比较；版本变化、首次生成或当前月新增覆盖日期即触发月重算。因此日已发布、进程在月任务登记前退出，下一轮仍能发现变化。月 source_watermark 是该日版本向量的 SHA-256。

## 4. 单个 Go 发布 Worker 与调度

`usage-worker` 是自行实现的独立 Go 程序。默认 K8s CronJob 每分钟运行 `usage-worker --once`，一次调用先恢复未完成发布，再按有界任务预算刷新待处理日和已具备覆盖的月；超过预算的任务留到下轮，须兼顾历史通知及月份，避免持续新流量使旧任务饿死。调度每分钟检查队列，不表示整条链路保证 60 秒完成。

CronJob 采用 batch/v1（Kubernetes 1.22）、schedule="* * * * *"、concurrencyPolicy=Forbid、Job parallelism=1/completions=1；所有统计日期由程序按 UTC 计算，不使用较新版本的 spec.timeZone 字段。镜像、Secret、RBAC、任务预算和超时在实现时固定。也可使用单副本 Deployment 内部 60 秒轮询，或单机 cron；同一环境只启用一种调度入口。

Forbid 仅约束同一个 CronJob 的 Job，不能防止手动 Job、另一 CronJob 或孤立旧进程重叠。程序还需发布者保护：K8s 所有入口共用同一预创建 Lease，记录 owner/token；获取和续约按 resourceVersion 校验，Worker 在每次状态写入及 DDL 前确认 ownership，丢失即停止。单机 cron 使用所有入口共用的进程锁。

Lease 不是 Doris 端强制 fencing，单副本也不消除网络分区。这里采用保守接管：租约过期或 ownership 不明时，不自动让新实例进入写状态/分区发布；先确认旧 Pod/进程已终止且在途 Doris SQL 已结束，再接管并恢复。无法确认则暂停发布并报警。禁止使用 Doris 的读后 UPDATE 模拟全局锁，禁止以仅 Forbid/Lease 超时宣称任意故障下绝不双写。Lease 复用既有 K8s API，不引入另一协调数据库。[CronJob 并发与幂等要求](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)，[Lease](https://kubernetes.io/docs/concepts/architecture/leases/)

一期整个发布、状态更新及汇总分区清理由一个 Worker 串行执行，不同日期也不并行发布。月准备到发布期间暂停该 Worker 的日发布，固定月输入；Consumer 可照常入库并登记下一轮日通知。扩展多发布者须另行提供 Doris 端可验证的 fencing 方案及验收，不能直接增加副本数。

## 5. 分阶段发布与崩溃恢复

Doris 显式事务不能包含 DDL；REPLACE PARTITION 与发布状态写入分成可恢复步骤。不能把 PostgreSQL 锁/事务直接替换为 Doris SQL 的同名操作。[Doris 事务限制](https://doris.apache.org/docs/4.x/data-operate/transaction/)

1. RUNNING：创建唯一 refresh_id，持久记录活动任务和输入；创建、加载整期临时分区，正式分区仍为上次结果。
2. PREPARED：校验计数恒等式、NULL/known、日期/唯一键、统一 refresh_id 和行数；记录旧正式 PartitionId、临时 PartitionId、名称、预期行数及校验摘要。月任务再次确认日版本向量。
3. PUBLISHING：先持久化任务阶段，再写当前状态 publication_uncertain=true 并递增 state_version，确认读回可见后才能执行替换。状态写入确认丢失时先读回核对，不盲目进入 DDL。
4. 单个 REPLACE PARTITION 原子替换正式分区。语句确认丢失或进程退出时，保留 uncertain，不输出已确认统计。
5. 核验正式分区身份及所有行的 refresh_id/行数/摘要。零行也要核验记录的临时 PartitionId 已成为正式分区；不能仅凭“查询没有行”判成功。PartitionId 的替换行为须在目标版本通过故障注入验收。
6. 先写当前状态的 published_refresh_id、published_version、source_watermark、覆盖范围及正式 PartitionId，递增 state_version，清除 uncertain/active_refresh_id，并确认可见；随后把对应 run 标记 PUBLISHED。后一步失败由下轮恢复补齐，在此之前通知不会被错误消费。

首次状态 state_version 从 1 开始，每次修改递增；published_version 为最近成功发布所用 state_version。版本不使用墙上时钟排序，重启从持久状态继续；相同转换重试使用相同版本和内容，上一写入结果不确定时须先确认，禁止启动下一转换。正式状态与任务审计是独立写入，恢复先对齐活动任务及已发布但未终结的 run，然后才领取新任务。

替换前失败可清理该任务临时分区并重试，保留上一成功结果。替换结果未知时，先核对 run、当前状态、正式及临时 PartitionId；已替换且内容匹配则补齐发布状态，仍为旧分区且确认原 SQL 已结束则重新执行或重建。无法证明是哪一版本时继续 uncertain、暂停该期查询并报警；能从完整输入重建后再发布。不得猜测成功或把 uncertain 清掉后返回旧版本。孤儿临时分区须确认无活动任务及在途 SQL 再清理。[临时分区替换](https://doris.apache.org/docs/4.x/data-operate/delete/table-temp-partition/)，[分区元数据](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/SHOW-PARTITIONS/)

## 6. 查询一致性与元数据清理

查询 API 先读取请求涉及的各期状态向量，确认各期仍在保留范围、status 非 EXPIRED、已有成功覆盖且 uncertain=false；按该向量的 refresh_id 查询正式结果，缓冲响应；再读同一范围状态。只有前后 state_version、published_refresh_id、覆盖范围均相同且无 uncertain，才返回成功。状态变化则丢弃结果、有限重试，超过预算返回暂不可用；不以新的零行结果代替旧版本。多日期及多月份均比较完整向量，不能只检查一行。

首次未发布或覆盖缺口不返回确认零值。准备失败可继续展示上一成功批次及滞后状态；发布不确定时暂停该期成功响应。零行成功日期从状态表确认覆盖。分区清理先递增状态版本并标记 EXPIRED，确认可见后删除；前后读取遇到清理的查询须失败或重试，不需要 PostgreSQL 查询共享锁。

协调表不按原始分区自动到期。保留所有未确认通知、活动任务及其输入、uncertain 证据、最新已发布日/月状态和相应输入向量。清理已处理通知必须同步考虑其成功确认关系：可保留确认索引，或删除通知与已终结输入后允许后来重放安全地额外重算；禁止先删确认关系、再把仍存在的通知误判为新工作。批次凭据清理仅可覆盖 Kafka 已提交且已被持续水位检查点确认的范围；最新连续凭据/起点仍保留，恢复不能只靠已删除的日志推断无缺口。初版可先保留协调记录，清理策略及容量上限在实际负载下验收，不预设通知量很小。

已超过明细重建窗口仍未处理的通知升级为覆盖缺口和告警，禁止静默丢弃。日/月保留边界的清理仍由单个 Worker 管理，并保留较长月汇总所需的覆盖与版本元数据；已删除日数据不能用于重建旧月，旧月可继续提供已确认版本并标明可重算范围。

## 7. 迁移与验收

新环境执行 Doris 001、002、004、005、006；已有 Doris 环境按需执行 003 保留期迁移，再执行缺失的后续迁移，并核对实际 schema。006 更新请求视图，诊断 route_name 不参与新旧记录的身份比较；升级前暂停旧 Consumer，再更新 Consumer / Observer 和 Wasm。旧 [PostgreSQL usage 迁移](../migrations/postgres/usage/001_summary_refresh_state.sql)已退役为空操作说明，不执行建库或建表；Auth PostgreSQL migration 不受影响。

若某个环境曾试验旧状态表，先停止所有旧 Worker，保存 Kafka 进度、成功 refresh_id、覆盖范围和未完成任务；将已发布分区核验后迁入 Doris 状态，未确认日期重新生成初始化/修复通知，再从 Kafka committed offset 恢复 Consumer。纯 Doris 切换验证完成前保留旧状态备份；本次文档/DDL 更新不自动删除已部署数据库。仅建新表不能证明历史覆盖。

验收至少覆盖通知重放、批次重新分组可额外重算但不累计用量、所有日期的跨日冲突通知、通知/凭据写入分别失败、提交失败/rebalance、确认丢失、计算期间新通知、零行发布、每个发布阶段崩溃、run/状态写入间崩溃、月份自动发现日版本变化、覆盖缺口、查询前后版本变化、CronJob 漏调/重试/手动并发、Lease 失效与保守接管、孤儿 SQL/临时分区及元数据容量。当前仅交付参考方案及 DDL，不能宣称这些场景已经通过。
