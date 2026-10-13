# LLM Token Stats

大模型请求鉴权、用量计量、统计、计费与对账平台。当前仓库包含设计文档、Doris 原始事件及日/月汇总 schema、汇总 SQL 模板；采集、统计刷新和平台接口仍处于设计及工程初始化阶段。

## 目录约定

| 目录 | 用途 |
| --- | --- |
| api/ | 跨组件协议、Kafka 事件格式与平台 API 契约 |
| cmd/ | 各独立部署程序的入口 |
| internal/ | Go 后端实现，按业务职责组织 |
| plugins/gateway-observer/ | TinyGo Proxy-Wasm 网关观察插件，使用独立 Go 模块 |
| third_party/proxy-wasm-go-sdk/ | 固定版本的 SDK 及项目所需扩展 |
| web/portal/ | 统一管理门户 |
| migrations/ | Auth 的 PostgreSQL 迁移；采集、汇总及协调状态的 Doris 迁移 |
| deploy/ | 网关及各服务的部署配置 |
| configs/ | 版本化配置样例与校验规则 |
| tests/、testdata/ | 跨组件测试与合成样例 |
| scripts/ | 生成、校验和发布辅助脚本 |
| docs/ | 设计、接入和运维文档 |

空目录由 .gitkeep 保留。其余组件的代码、协议、依赖文件和部署清单会在对应工作包实施时加入；目录存在不表示组件已实现。Doris 开发环境与 schema 见 [部署说明](deploy/doris/README.md) 和 [migration](migrations/doris/metering/001_init.sql)；Consumer 尚未实现。
## 一期数据与统计设计

- 原始事件按 UTC 请求日期每日分区，在线保留一年：365 个日期，包含当天及前 364 天。
- 一期包含日汇总和月汇总：分别独立保留 730 个日期及 24 个自然月；月汇总从已发布日汇总生成。
- 重放、迟到事件和冲突触发对应日期与月份重算，使用临时分区校验后原子发布。
- 采集与汇总无需额外 PostgreSQL：日期通知、消费凭据、水位和发布状态保存在 Doris；独立 Go usage-worker 默认由 K8s CronJob 每分钟调度，单个发布者串行执行。Auth 的 PostgreSQL 按原鉴权方案保留。
- 归档只保留可选设计，后续另行建设，一期不部署归档任务或对象存储。

详见[日/月汇总设计](docs/daily-monthly-summary-design.md)、[Doris 协调设计](docs/doris-summary-coordination.md)和[协调表迁移](migrations/doris/metering/004_summary_coordination.sql)、[可选归档设计](docs/archive-optional-design.md)。建表与 SQL 模板不包含任务调度实现；实际 Doris 联调和容量压测尚待执行。

## 网关采集设计闭环

采集子项正式契约与验收门槛见[闭环清单](docs/gateway-collection-design-closure.md)和[主方案 V1.9](docs/gateway-request-collection-implementation.md)。已交付观察 Protobuf、事件/配置 Schema、默认关闭配置、六种合成事件与离线边界校验；采集组件与真实环境验收仍待实施。

在仓库根目录执行：

~~~sh
python scripts/validate_collection_contracts.py
python -m unittest discover -s tests -p "test_collection_contracts.py" -v
~~~

脚本仅使用 Python 3.10+ 标准库，不接入网关、Kafka 或数据库；生产上线按闭环清单 G0–G5 收集真实证据。
