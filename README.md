# LLM Token Stats

大模型请求鉴权、用量计量、统计、计费与对账平台。当前仓库处于设计及工程初始化阶段；现有方案见 docs/。

## 目录约定

| 目录 | 用途 |
| --- | --- |
| api/ | 跨组件协议、Kafka 事件格式与平台 API 契约 |
| cmd/ | 各独立部署程序的入口 |
| internal/ | Go 后端实现，按业务职责组织 |
| plugins/gateway-observer/ | TinyGo Proxy-Wasm 网关观察插件，使用独立 Go 模块 |
| third_party/proxy-wasm-go-sdk/ | 固定版本的 SDK 及项目所需扩展 |
| web/portal/ | 统一管理门户 |
| migrations/ | PostgreSQL 与 Doris 数据库迁移 |
| deploy/ | 网关及各服务的部署配置 |
| configs/ | 版本化配置样例与校验规则 |
| tests/、testdata/ | 跨组件测试与合成样例 |
| scripts/ | 生成、校验和发布辅助脚本 |
| docs/ | 设计、接入和运维文档 |

空目录由 .gitkeep 保留。代码、协议、依赖文件和部署清单会在对应工作包实施时加入；目录存在不表示组件已实现。Doris 开发环境与 schema 见 [部署说明](deploy/doris/README.md) 和 [migration](migrations/doris/metering/001_init.sql)；Consumer 尚未实现。