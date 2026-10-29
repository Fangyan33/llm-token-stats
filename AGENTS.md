# Repository Guidelines

## 项目结构与模块组织

- `cmd/observer/`、`cmd/consumer/` 是服务入口；`internal/` 存放实现，`pkg/` 存放共享代码。
- `plugins/gateway-observer/` 是独立 Go 模块；SDK 扩展位于 `third_party/proxy-wasm-go-sdk/`。
- `api/` 定义 Protobuf 与事件 Schema，`gen/` 保存生成代码。
- `tests/` 存放契约与集成测试，`testdata/collection/` 保存合成事件及规范编码、摘要样例。
- `configs/`、`deploy/`、`migrations/` 分别管理配置、部署和数据库迁移；设计在 `docs/`，实现计划在 `docs/spec/plan/`，构建产物在 `dist/`。空目录及 `web/portal/` 不代表功能已实现。

## 构建、测试与本地开发

使用 Go 1.25.0、TinyGo 0.40.1、Python 3.10+；集成测试需要 Docker Compose。以下命令均在仓库根目录执行：

| 命令 | 用途 |
| --- | --- |
| `make build` | 构建 Observer、Consumer 和测试 fixture |
| `make wasm` | 构建 Wasm 并检查 ABI |
| `make test` | 契约校验、Python 测试及主模块和插件的 Go 竞态测试 |
| `make local-up` | 构建完成后启动 Envoy、Kafka、Doris 联调栈 |
| `make local-test` | 执行真实链路集成测试 |
| `make generate` | 从 Protobuf 重新生成 Go 代码 |
| `python3 scripts/local_collection.py stop` | 停止联调栈并保留数据 |

生成工具版本和配置说明见 [组件运行说明](docs/collection-components.md)。

## 编码风格与命名

Go 使用 `gofmt` 和标准 Tab 缩进，包名保持小写。新 Python 文件采用四空格缩进，修改已有文件时保持原风格。配置字段沿用 `snake_case`，迁移使用 `NNN_description.sql`。修改 `.proto` 后重新生成，避免手改 `gen/`；SDK 修改须记录在 `PATCHES.md` 并保留许可证。

## 测试要求

Go 使用标准 `testing`，文件命名为 `*_test.go`、函数命名为 `TestXxx`；Python 使用 `unittest`。根目录 `go test ./...` 不覆盖独立插件模块，优先运行 `make test`。当前没有固定覆盖率门槛；行为变更应覆盖对应边界、故障或重放场景。协议变更同步更新 Schema、配置、迁移、合成样例及相关文档。网关链路变更另运行 `make wasm` 和 `make local-test`；本地通过不代表生产 G0–G5 验收完成。

## 提交与 Pull Request

历史提交使用简短的中英文主题，例如 `Doris 采集与汇总协调设计`，没有统一 Conventional Commits 格式。提交聚焦一项变更，标题说明具体结果。PR 说明问题、行为变化、验证命令及结果；关联已有 issue，并说明协议或迁移的兼容性、上线顺序及未验证项。

## 采集边界与配置安全

保持业务直接经过网关到模型服务；Wasm 有界旁路发送，Observer 原生 Producer 写 Kafka，Consumer 确认 Doris 可见后提交 offset。采集失败应放行业务，未知用量保留 `null/unknown`。正文仅临时处理，不把正文或密钥写入日志；凭据使用环境变量或 Secret，禁止提交。生产模板需按实际环境渲染并完成验收。

## 铁律：采集故障必须 fail-open 放行业务

**业务正常转发优先于采集完整性，允许漏采，禁止因采集故障拒绝、终止或无限暂停业务请求。**

- 覆盖插件加载、初始化、配置、正文改写、观测、指标及 Kafka/Doris 写入各阶段；采集链路必须有界、异步，故障时降级或跳过采集。插件加载不得成为业务网关可用的必需依赖，必须具备可验证的透传兜底。
- 对支持范围内的 `stream=true` 请求，必须自动添加或覆盖 `stream_options.include_usage=true`；改写失败、超限或超时则转发原始正文，保留其他业务字段，不重发模型请求。
- 改写从请求头开始使用一次性截止时间：默认/运行时最多 450ms，检查周期最多 10ms，目标暂停不超过 500ms；调度拥塞或恢复调用暂时失败可短暂超出，但必须持续恢复转发，禁止重新计时或再次暂停。
- 超时先恢复业务，再清理旁路和指标；恢复失败必须保留待恢复状态并重试，后续正文直接透传。指标失败不得 panic；采集资源不足直接放弃采集，未知用量保持 `null/unknown`。
- `fail_open: true` 不是全阶段保证。涉及加载或暂停路径的变更必须验证故障下的真实转发，并明确 VM/Envoy 故障等未覆盖边界，禁止将配置开关或单元测试等同于完整验收。

## Agent 指引

与用户交流使用简体中文；尊重工作区已有修改，不覆盖无关变更。
