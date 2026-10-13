# Repository Guidelines

## 项目结构与模块组织

- `cmd/observer/`、`cmd/consumer/` 是服务入口；`internal/` 存放实现，`pkg/` 存放共享代码。
- `plugins/gateway-observer/` 是独立 Go 模块；SDK 扩展位于 `third_party/proxy-wasm-go-sdk/`。
- `api/` 定义 Protobuf 与事件 Schema，`gen/` 保存生成代码。
- `tests/` 存放契约与集成测试，`testdata/collection/` 保存合成事件及规范编码、摘要样例。
- `configs/`、`deploy/`、`migrations/` 分别管理配置、部署和数据库迁移；设计在 `docs/`，实现计划在 `spec/plan/`，构建产物在 `dist/`。空目录及 `web/portal/` 不代表功能已实现。

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

## Agent 指引

与用户交流使用简体中文；尊重工作区已有修改，不覆盖无关变更。
