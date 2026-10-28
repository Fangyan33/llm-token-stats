# 请求标识调整与验证记录

日期：2026-10-28。

## 已实现的契约

- `observation_id`：Observer 在有效 Start 后生成的 UUIDv4，用于 ACK、Kafka key、事件 ID、Doris 唯一键、事件关联、冲突检测和请求计数。
- `logical_request_id`：客户端关联 ID，由 Wasm 的 `logical_request_id_headers` 有序列表提取，随 Start 字段 18 传给 Observer；未命中显式保存 null，允许客户端重试复用，不按此字段合并观察。
- `x_request_id`：继续保存网关可见的 `X-Request-Id`。
- 候选列表缺省或为空时不采集，最多 8 项；头名称忽略大小写且归一化后不能重复。取首个 1–128 字节、只含 `[A-Za-z0-9._:-]` 的单值，跳过缺失、空值、非法、超长或重复头，不截断、不补值。请求开始时冻结，开始和结果事件保持一致。
- 取消采集插件对 `X-Metering-Request-Id` 响应头的注入、删除及覆盖；上游已有响应头保持原样。
- Kafka 事件、Wasm 配置和观察流均使用 schema_version=1，直接维护当前唯一契约。ACK 字段 1 更名为 observation_id，字段号及内部 UUID 值语义保持一致。
- 同一观察的客户端 ID 变化属于快照冲突；不同观察复用客户端 ID 不产生冲突。

## 已通过的检查

| 命令 | 结果 |
| --- | --- |
| 使用仓库 `.tmp/toolchain` 的 protoc 29.3、protoc-gen-go v1.36.5、protoc-gen-go-grpc v1.5.1 执行 `make generate` | 通过，生成文件已同步 |
| `GOFLAGS='-count=1 -p=1' make test` | 通过：契约和文档一致性、90 项 Python 测试、主模块和独立插件 Go 竞态测试 |
| `make wasm` | 通过：TinyGo 构建及 Wasm ABI、gRPC 回调/hostcall、无 socket import 检查 |
| `go test -tags=integration -run '^$' ./tests/integration ./internal/summaryworker` | 通过，仅证明集成测试可编译 |
| `git diff --check` | 通过 |

回归覆盖优先级、候选头名称大小写、值大小写保留、空/非法/超长/重复头回退、列表缺省/空列表、null 与非 UUID 客户端 ID、ACK 和两类事件使用相同内部 ID、响应头不注入及原样保留、未知事件版本暂停消费。已增加真实链路用例，检查三个 ID 的类型化列与 event_json 一致、客户端重试分别进入请求及统计视图，以及同观察客户端 ID 变化的持久冲突隔离。

## 尚未通过的验证

实际尝试了 `GOFLAGS='-count=1 -p=1' make local-test`。Doris 数据卷所在磁盘可用空间约 77 MB，低于 FE 的 1 GiB 磁盘保护阈值，FE 一直未就绪；SQL 19030 和 BE HTTP 18040 连接被拒绝。数据库相关用例失败后停止了继续等待。Producer 黑洞有界性用例通过，不能代表整条采集链路通过。

本次为排查启动的 Doris 已恢复为停止状态；没有执行任何现有数据库的 DDL 升级，没有删除数据库、数据卷或清理宿主机数据。新 DDL 的实际初始化、Wasm → Observer → Kafka → Consumer → Doris 链路及新视图的实时元数据，均需在空间充足的开发环境重新验证。数据库字典中变更后的视图类型及可空性按 SQL 推导，不声称已重新完成实时元数据核验。

## 当前模型

项目尚未正式上线，本次直接修改现有契约、校验、字段、初始化 SQL 和样例，Kafka 继续使用现有 topic 与消费组配置。表结构以 001–003 SQL 的当前定义为准。

本次代码和本地检查不代表生产 G0–G5 验收完成。
