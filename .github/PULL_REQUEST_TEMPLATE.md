<!-- 感谢提交 PR！请在合并前确认以下清单。 -->

## 改动说明

<!-- 这个 PR 做了什么、为什么做、关联哪个 Issue（如 #123）。 -->

## 自检清单

- [ ] `mvn -B verify` 通过（CI 也会跑）
- [ ] 新增 / 变更行为有对应测试（端到端优先；逻辑层至少覆盖编解码 / 切分 / 配置校验）
- [ ] 文档已同步更新（`docs/connectors.md`、`README.md`、`AGENTS.md` 等）
- [ ] 新增连接器已在 `META-INF/services/me.jayer.hdata.core.spi.TransformProvider` 注册
- [ ] 死信路径用 `ErrorSchemas.failure(...)`，且携带原始行的时间戳与窗口（`ValueInSingleWindow`）
- [ ] 配置项要么真的生效、要么显式 `validate` 报错（不静默降级）
- [ ] DoFn 不捕获不可序列化对象（必要时补 `SerializableUtils.ensureSerializable(...)` 测试）

## 破坏性变更

<!-- 是否有不兼容的配置 / 行为变更？如有请说明迁移方式。 -->
