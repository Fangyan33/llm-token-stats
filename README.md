# LLM Token Stats

大模型请求鉴权、用量计量、统计、计费与对账平台。当前已实现网关 Wasm、Observer、Consumer 采集链路，提供 Doris schema、构建/部署模板与真实本地联调；统计刷新、鉴权服务及平台接口属于后续工作。

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

空目录由 .gitkeep 保留，目录存在不表示对应组件已实现。采集组件的构建、联调、恢复边界见[运行说明](docs/collection-components.md)；计划见[spec/plan](spec/plan/gateway-collection-implementation.md)。Doris 环境与 schema 见[部署说明](deploy/doris/README.md)和[migration](migrations/doris/metering/001_init.sql)。
## 一期数据与统计设计

- 原始事件按 UTC 请求日期每日分区，在线保留一年：365 个日期，包含当天及前 364 天。
- 一期包含日汇总和月汇总：分别独立保留 730 个日期及 24 个自然月；月汇总从已发布日汇总生成。
- 重放、迟到事件和冲突触发对应日期与月份重算，使用临时分区校验后原子发布。
- 采集与汇总无需额外 PostgreSQL：日期通知、消费凭据、水位和发布状态保存在 Doris；独立 Go usage-worker 默认由 K8s CronJob 每分钟调度，单个发布者串行执行。Auth 的 PostgreSQL 按原鉴权方案保留。
- 归档只保留可选设计，后续另行建设，一期不部署归档任务或对象存储。

详见[日/月汇总设计](docs/daily-monthly-summary-design.md)、[Doris 协调设计](docs/doris-summary-coordination.md)和[协调表迁移](migrations/doris/metering/004_summary_coordination.sql)、[可选归档设计](docs/archive-optional-design.md)。Consumer 已登记通知和消费凭据；建表与汇总 SQL 模板尚不包含 Worker 调度实现，生产容量压测待执行。

## 网关采集设计闭环

采集子项正式契约与验收门槛见[闭环清单](docs/gateway-collection-design-closure.md)和[主方案 V1.9](docs/gateway-request-collection-implementation.md)。已交付三个采集组件、SDK gRPC 扩展、标准 Protobuf 代码、默认关闭配置、005 恢复 migration、单元/竞态/真实集成测试；生产 G0–G5 验收仍待执行。

在仓库根目录执行：

~~~sh
make test build wasm
make local-up
make local-test
~~~

本地栈使用合成鉴权和模型 fixture，实际经过旧版 Envoy、Kafka、Doris；构建工具与端口见运行说明。生产上线按闭环清单 G0–G5 收集真实证据。

本地开发组件的发布端口绑定 `0.0.0.0`，DBeaver 可直接连接 `服务器IP:19030`。Kafka 默认公布识别出的服务器 IPv4 地址；多网卡或 NAT 环境用 `KAFKA_EXTERNAL_HOST=客户端可达地址 make local-up` 指定。Kafka 与 Doris 使用持久卷，启动脚本会先备份并迁移旧容器的数据。


|组件|端口|
|--|--|
|网关 / Envoy admin| 18080 / 19000|
|Kafka| 29092|
|Doris SQL / FE HTTP / BE HTTP| 19030 / 18030 / 18040|
|Observer gRPC / 健康检| 19090 / 19091|
|Consumer 健康检查| 19092|
|模型 fixture / 模拟鉴权         | 18081 / 19002|
