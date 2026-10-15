# 网关采集设计闭环与验收清单

修订日期：2026-10-15。插件、SDK 扩展、Observer、Consumer 已实现并完成公开旧版 Envoy/Kafka/Doris 本地联调；生产完整门槛未验收。实现结果与已确认的请求头 Continue 调整见[运行说明](collection-components.md)。本文与[开发实现方案](gateway-request-collection-implementation.md)共同构成采集子项的实施依据。

## 1. 交付物与变更规则

| 契约 | 唯一文件 | 使用方 |
| --- | --- | --- |
| 双向观察协议 | [observation.proto](../api/observation/v1/observation.proto) | WASM 编解码器、Observer 生成代码、互操作测试 |
| Kafka schema v1 | [事件 Schema](../api/events/v1/metering-event.schema.json) | Observer 固定 struct、Consumer、第三方订阅者 |
| 插件配置 | [配置 Schema](../configs/gateway-observer.schema.json)、[默认关闭样例](../configs/gateway-observer.example.json) | 发布校验、插件配置解码 |
| 合成事件 | [testdata/collection](../testdata/collection) | start、known、partial、unknown、零值、候选冲突的协议测试 |
| 离线校验 | [校验脚本](../scripts/validate_collection_contracts.py)、[边界测试](../tests/test_collection_contracts.py) | 本地开发与 CI |
| 数据与统计衔接 | [Doris 原始表](../migrations/doris/metering/001_init.sql)、[Doris 协调表](../migrations/doris/metering/004_summary_coordination.sql)、[汇总设计](daily-monthly-summary-design.md) | Consumer、usage-worker |

Schema 描述类型和固定字段集；UTF-8 字节上限、跨字段状态、Kafka key、时间窗口、原 usage 白名单和资源关系由校验脚本补充。两层校验都必须在运行时实现，不能仅凭通过 JSON Schema 验收。脚本只实现本仓库 Schema 使用的关键字，使用 Python 3.10+ 标准库；不代替通用 JSON Schema 工具、protoc、业务解析器或数据库执行。

字段顺序以事件 Schema 的 properties 顺序为规范 Go struct 顺序；usage 顺序为 input、output、cached_read、cached_write、reasoning、reported_total，各子项按 value、source 排列。Consumer 重编码顺序固定，使用 Go encoding/json 默认转义；raw_usage_json 的字符串内容作为冻结依据原样保留，不按另一个 JSON 排序器改写。辅助校验器提供相同顺序和转义的摘要参考，正式 Go 实现仍须与它做 golden 对照。

新增字段或改变含义须提升 Kafka schema_version，并先发布可读取新旧版本的 Consumer 与数据库，再发布 Producer；不得直接修改已上线 v1 struct 重算旧摘要。观察协议字段号独立演进，2 已 reserved。go_package 已固定为 github.com/Fangyan33/llm-token-stats/gen/observation/v1；生成工具版本见运行说明。

Wasm 文件随 Observer 镜像发布，由独立 HTTP 下载服务提供单个当前版本；网关以远程 URL 和实际文件 SHA-256 加载。更新文件先完成 Observer 全副本发布和下载摘要核对，再切换网关；旧摘要下载失败允许暂时缺少采集，恢复后显式重新加载。文件版本回退先恢复对应 Observer 镜像，再恢复网关配置。该文件发布顺序与下述采集注册版本兼容要求同时满足。

配置发布先让 Observer 接受新版本及仍存活的旧 Root 版本，再更新网关；回退同样先核查接纳清单。Consumer 保留所有在线窗口内历史 config_version/route/service 登记，不能因为网关移除路由或旧版本而拒收合法重放。白名单限制新采集与历史事件校验分别管理。

## 2. 本轮关闭的设计缺口

| 缺口 | 最终决策 | 验证 |
| --- | --- | --- |
| 协议与配置只有 Markdown 片段 | 落成正式文件；文档片段、配置数值表和契约文件自动对比 | 离线脚本 |
| 256 KiB 改写请求装不进 128 KiB 在途额度 | 单流默认改为 512 KiB；完整请求分片必须预留全部消息与字节再复制，失败取消旁路 | 请求大小 + 分片开销 + 控制预留的交叉校验 |
| 改写超时恢复后请求观察可能丢前缀或重复 | 已缓冲原文仅观察一次，再观察恢复后的尾片；发送失败不重新观察或开流 | G2 慢上传与超限测试 |
| Protobuf 类型没有业务上限 | 固定 Start/头/End/ACK 的校验条件、诊断与原因枚举 | G1 Protobuf golden 与畸形帧 |
| token 状态可能被不同组件理解成不同值 | 输入输出都有效为 known；否则任一六字段有效为 partial；全部未知为 unknown；conflict 优先 | 合成事件与边界测试 |
| 写库成功到提交 offset 的中途失败没有顺序 | 冻结批次 → 必要两表可见 → 冲突回查/标记 → 持久 dirty → 提交下一 offset | G3 逐步崩溃注入 |
| 一分钟冲突任务缺乏可靠重启输入 | 提交前回查是强制屏障；分钟任务只作补偿，按持久水位和在线分区巡检 | G3 跨日期及证据到期测试 |
| 静态检查与上线验收边界不清 | 设计检查与 G0–G5 真实证据分别记录；任何待验收门槛不能标通过 | 发布记录 |

512 KiB 是单流上限，Root 总在途预算仍为 16 MiB；不能按 512 个流同时占满计算成已承诺容量。每个 Worker 的改写预留另计 16 MiB，热更新最多两代，再加 host/HTTP2 缓冲与运行时内存，按主方案压测。

## 3. 观察协议的业务校验

所有限制按解码后 UTF-8 字节计；拒绝非法 UTF-8、重复 singular/oneof 字段、非规范或溢出的 varint，未知字段在消息总上限内有界跳过。标准 Protobuf 解码器不会自动拒绝所有重复字段，Observer 接纳前必须有 wire 检查；不得依赖 last-one-wins。每帧 Protobuf 不超过 32 KiB，BodyChunk.data 不超过 16 KiB；正文分片开销按最多 256 字节预留，实际编码长度必须再次检查。

| 消息 | 强制校验 |
| --- | --- |
| Start | frame_seq=1、每流一次、schema_version=1；时间为 [1,253402300799999] Unix 毫秒且不超前 5 分钟；method=POST；实例/配置/路由/服务与身份 ID 各 ≤128 字节，adapter ≤64；可信身份缺项或无效统一置 null、标 invalid；模型不在 Start；x_request_id 符合主方案 |
| Start 接纳 | adapter/config_version 在 Observer 发布白名单中，route_name/service_id 组合已登记；未接纳不分配 UUID、不产生事件；ID 随有效流只生成一次 |
| Content-Type / Encoding | 只读取指定头，各 ≤128 字节；JSON 仅 application/json，SSE 仅 text/event-stream（忽略媒体类型大小写，允许参数）；无编码或 identity 支持，不尝试猜测或解压；未知类型产出固定异常码 |
| ResponseHeaders | 一次最终响应，状态 200–599，1xx 不发送；必须在响应正文前；同一方向 EOS 后不得继续正文 |
| BodyChunk | Direction 仅 REQUEST/RESPONSE；frame_seq 连续，每方向 body_seq 从1连续、offset 从0连续；加法检查溢出；零字节帧只允许首次宣告 EOS，不能无限空帧占 CPU |
| End | 每流一次、最后一帧、发送半关闭；末片序号、总字节数、EOS 与已处理消息完全一致；若有 status 须与最终头相同；可观察断流允许 EOS=false，此时结果 partial/unknown；attempt_count 如可可靠读取，限 [1,2147483647] |
| ACK | ≤1 KiB，规范 UUIDv4 始终一致；累计确认只覆盖已处理连续帧，不能确认尚未发送序号；重复 ACK 不重复释放；首次有效 ACK 确认 Start |
| 关闭 | 有效 End 即使 EOS=false 也表示观察到终态，terminal_observed=true；无 End 的半关闭/断连按 missing_end 定稿，不能补造结束时间 |

Start/响应头/End 元数据序列化后必须满足插件 metadata_max_bytes，Start/头/End 一共只使用已预留的 4 条、8 KiB 控制额度；不足则取消旁路。HTTP context 删除后不得从迟到回调读取正文或身份。Root 调用 hostcall 时的 context 切换与 SDK current context 同步恢复。

ACK reason 封闭为 accepted、finalized、protocol_error、parse_limit、resource_limit、observation_timeout；正常累计 ACK 用 accepted，End 用 finalized。无有效 Start 时不发送缺 ID 的 ACK，以 gRPC INVALID_ARGUMENT / RESOURCE_EXHAUSTED / UNAVAILABLE 关闭；关闭详情不携带输入值。已接纳流发生错误时可发送带既有 ID 的 close_stream ACK，发送失败仍执行一次定稿与有界清理。gRPC close status、插件 drop/rewrite/ignored 指标原因、Kafka anomaly_codes 分别使用自己的封闭枚举，不能混为同一自由文本字段。

正文业务 JSON 同样拒绝重复键、非法 UTF-8、过深/过多元素。模型超限置 null/invalid_model，不能阻止合法 usage 被保留；合法模型字符串非空。raw_usage_json 只保留六个来源路径的有效非负 Int64 数字及 details 对象；其他键和非法类型/数值丢弃并标 raw_usage_truncated，空依据为 "{}"。含量关系冲突保留合法数字但不进入有效统计；多个不同终结候选则所有归一化值清空，首份过滤依据仍可保留。

响应诊断字段只接受部署登记的有限枚举，每项 ≤128 字节；未登记或读取失败为 null。默认诊断白名单为空。不得保存可能夹带响应文本的 response_code_details 原值，Consumer 再按同一发布清单校验。

## 4. 请求改写与观察衔接

入口白名单先完成，改写与旁路资源分别申请。缓冲期间不发送请求 BodyChunk；改写完成只观察改写后的最终正文。超时/超限/非法 JSON/trailers 降级时，在恢复转发前或同一次回调中，按 offset=0 观察当时已缓冲的原文一次，只在可证明缓冲边界时再以新偏移处理透传尾片；不再次把累计 host buffer 当尾片发送。固定旧版 Envoy 的 Root 异步恢复可能漏掉尾片回调，因此当前超时路径观察有界前缀后取消旁路，恢复业务原文，结果保留 partial/unknown。

完整缓冲观察先计算全部分片数和序列化上界，非阻塞预留全部额度后才复制。原缓冲超出 request_body_max_bytes、无法取得一致 buffer，或额度不足时直接取消观察，不先发送部分前缀再重新观察。只计已成功发送的正文长度与序号；取消后不发送 End。恢复 hostcall 成败、预算释放、重复 tick 和 context 删除均须幂等。

rewrite_timeout_ms=2000 是到期判定点，实际恢复还受 100 ms tick、有界到期任务和 Worker 调度影响；不是已保证 2000 ms 的硬墙钟期限。上线报告实际超时恢复延迟及最坏 tick 排队；超过验收容许值须调实现。任何 Observer/Kafka 等待不得加入恢复路径。

## 5. Consumer 提交与恢复屏障

单分区单 owner、顺序处理，跨分区并行数量与批次总内存有界。Kafka 重试只发生在消费/写库端；采集侧丢弃的事件不能恢复。已支持 schema 的永久非法记录写有限拒收诊断；未知版本暂停该分区。没有可用 Kafka 时间戳或坐标无法形成合法诊断行时暂停，不猜日期，不提交。

1. 固定本次 topic/partition、首末 offset、各行分类、事件规范字节、SHA-256、UTC 日期和整批 Stream Load 内容；同一执行期事务完成前不增删批次、不重新取当前时间改写 ingested_at；重启后重新冻结生成的新写入时间允许改变，事件本身的时间、规范内容与摘要不变。
2. events_raw 与 ingest_rejections 各自使用表名前缀加规范元组（topic、partition、首末 offset、该表冻结 Stream Load 正文字节的 SHA-256）的 SHA-256 作为 label，总长不超过128字符。单执行期重试沿用 label 和内容；重启后批次范围或写入时间变化生成新 label，避免相同 offset 范围对应不同正文。Unique Key 负责跨 label 幂等；已有 label 仍须检查事务可见与行数，不能把 Label Already Exists 当成功。
3. 两表非原子；任何必要事务未确认可见，不提交这段 offset。Publish Timeout、确认丢失、HTTP 重定向失败均先确认原事务，禁止生成新内容重发同 label。只向部署登记的 FE/BE 目标转发数据库凭据。
4. 原始行可见后，用批次涉及的 UUID 在全部在线分区回查事件摘要、开始时间、UTC 日期与身份摘要。将所有发现的冲突写入 request_conflict_ids 并确认可见；已有标记永不撤回。按数据库 snapshot 回查，不以本地缓存代替。
5. 从 Consumer 首次上线即执行步骤4及通知登记，即使汇总 Worker 尚未启用。将新事件日期及冲突 UUID 全部受影响日期的幂等通知写入 Doris summary_dirty_notifications，确认集合可见后写 consumer_batch_receipts 并确认可见；任一步失败都不提交。通知与水位恢复规则见[Doris 协调设计](doris-summary-coordination.md)。重复通知不累计用量，不依赖 PostgreSQL 事务。
6. 最后提交该分区连续成功段的下一 offset。提交失败时不回滚 Doris/dirty；重放按相同屏障再走一遍。rebalance 已撤销 owner 不再启动写入或提交；已启动的冻结批次允许确认状态，交给新 owner 重放。
7. 每分钟补偿巡检使用 Kafka committed offset、Doris 批次凭据推导的连续分区水位及在线分区作输入，不依赖“最近 UUID”内存集合。新冲突可见立即由视图隔离；持久标记必须赶在最早证据分区过期前完成。检测或 dirty 水位不可信时暂停相关有效统计/汇总发布并报警，不能继续返回未确认数据。

| 注入点 | 恢复动作 | offset |
| --- | --- | --- |
| events_raw 成功，拒收诊断失败 | 重试必要两表，原始重复由 Unique Key 消除 | 保留 |
| 两表成功，冲突标记失败 | 重放并重新跨在线分区回查；标记确认后继续 | 保留 |
| 冲突标记成功，Doris 通知/批次凭据失败 | 幂等重放，确认所有影响日期通知及凭据 | 保留 |
| dirty 成功，提交失败/进程退出 | 新 owner 从未提交 offset 重走屏障；可额外重算 | 保留或已提交，均须支持重放 |
| 原始证据到期前仍无法写标记 | 停止有效统计发布，记录覆盖缺口；不假装冲突已解除 | 保留 |
| Kafka offset 已超保留期 | 显式登记丢失范围与数据缺口，按运维决策恢复新流量 | 不自动跳到最新 |

Doris 与 Kafka 不使用分布式事务，此处依靠固定事件、幂等表、Doris 持久通知/批次凭据和最终提交构成恢复闭环；不能承诺瞬时跨系统一致。并发 Consumer 同 UUID 的变体可由后写可见者或补偿巡检识别；对外有效统计必须检查冲突维护水位，汇总按捕获通知集合重算并核验发布版本。采集与汇总不依赖 PostgreSQL；Auth 的身份库独立保留。

## 6. 分阶段验收与责任

责任按角色分配，执行时补实名；没有证据或环境事实为待验收，不填“通过”。设计闭环完成表示实施决策、输入和验收办法明确。

| 门槛 | 主责 | 必须交付的证据 | 当前状态 |
| --- | --- | --- | --- |
| D0 契约冻结 | 采集研发 | 正式文件、离线脚本及边界测试、评审 diff | 已完成离线校验 |
| G0 环境与鉴权 | 网关/SRE、Auth 研发 | 固定镜像 digest/config_dump；Istio/Envoy/TinyGo/SDK 版本；ext_authz 身份读取、路由顺序、模型参数支持；真实 X-Request-Id/CORS 行为 | 待真实环境验收 |
| G1 SDK/协议 PoC | 插件与 Observer 研发 | protoc 编译；标准代码与 WASM 编解码 golden；固定镜像加载、ACK、Root 删除/迟到回调、trap/初始化失败及 mTLS/HTTP2 限额 | 实现及本地子项通过，完整门槛待验收 |
| G2 改写/解析 | 插件与 Observer 研发、测试 | HTTP1/2 慢上传和256 KiB边界、断开/trailers；正文差异仅允许 usage 参数；全字节切片 JSON/SSE、工具、多候选、EOS/End与资源释放 | 实现及本地子项通过，完整门槛待验收 |
| G3 发送/入库/统计衔接 | Observer、Consumer、usage-worker 研发 | Kafka黑洞/watchdog；Stream Load确认丢失；逐步崩溃/rebalance；两事件重放、跨日期身份冲突、证据过期、Doris通知/凭据失败、未知schema暂停；查询版本变化、发布中断、月份触发恢复和发布者保护 | 采集子项本地通过，完整统计衔接待实现与验收 |
| G4 性能和数据质量 | 测试、SRE | 峰值两倍、24小时、10分钟SSE；额外首包P99≤10ms；明细≤60s；10并发查询P95≤3s；独立同口径分母与全日/故障窗口指标 | 待真实环境验收 |
| G5 灰度与回退 | SRE、发布负责人 | 真实生效配置、canary流量与告警、disabled回退/必要移除patch、旧代限时释放、回退后Auth仍生效 | 待真实环境验收 |

G0/G1 未通过不得进入网关灰度；G2/G3 未通过不得扩大采集；G4/G5 未通过不得宣布生产验收。设计阶段不伪造集群地址、镜像 digest、容量或测试报告；SDK PoC 失败须记录事实重新评审方案。

发布记录至少保存：采集开始 UTC 日期、环境版本/digest、WASM及SDK补丁摘要、协议版本、插件/Observer/Consumer接受白名单、Kafka分区/保留/ACL、Doris采集/汇总迁移版本及Auth PostgreSQL迁移版本、单个发布Worker的调度/保护配置、采集Worker数/最大存活代数、资源上限、每个G门槛的报告位置与负责人、上一版镜像/配置及回退验证。记录不含密钥与正文。

## 7. 可复现的离线检查

在仓库根目录执行：

~~~sh
python scripts/validate_collection_contracts.py
python -m unittest discover -s tests -p "test_collection_contracts.py" -v
python scripts/validate_collection_contracts.py --config configs/gateway-observer.example.json
~~~

部署渲染后加 --release 校验占位值；它不自动核查 Pod UID 来源或网关生效配置。单个 Kafka 事件使用 --event FILE --key UUID，消费入口再提供 --now-ms UTC_UNIX_MS 检查365日保留和未来时间。合成历史样例不传当前时间，避免将静态协议样例误当当前可消费数据。

本地未安装 protoc，未连接实际 Istio/Kafka/Doris；离线脚本只做 proto 字段号及文档一致性检查，不宣称已编译或执行数据库。上述真实验收按 G0–G5 执行。
