# Gearbox Gear 下一阶段 OMO 功能对齐执行计划

> 制定日期：2026-07-10  
> 对齐基线：`docs/gearbox-omo-reference.md` 与 `/home/donald/文档/github/oh-my-openagent` 当前实现  
> 前置结果：`cargo test -p gearbox_agent` 已通过 172 个测试

## 1. 目标

本轮不再重复已完成的 worker fallback、category route、completion buffer、统一销毁、启动 reconciliation、LRU、TTL 和 `lost` 保护。下一阶段先修复 TaskManager 控制面与 OMO 的状态机差距，再吸收参考文档中仍缺失的质量门禁和产品能力。

本轮完成后应达到：

- 终态 resident worker 可在同一 session 内正确 revive，新 turn 被 TaskManager 完整纳管。
- cancel、interrupt、send、steer 的结果语义从 runtime 传到 GUI，不再压缩成 `bool`。
- 跨会话操作有显式 scope 校验，默认不能操作其他 parent/root session 的 task。
- OMO 参考中的 review gate、tool policy 和 stop continuation 进入可配置、可验收状态。
- 文档只保留真实未完成项，测试数和实现状态与代码一致。

## 2. 执行原则

1. 状态转移必须先于可失败的 handle 操作，晚到的 worker completion 不得覆盖终态。
2. `TaskManager` 是 task record、residency、epoch、concurrency 和 completion tracking 的唯一真相源；`TaskManagerControl` 不得建立第二套状态机。
3. Gear 专属能力优先留在 `crates/gearbox_agent` 或 `crates/gearbox_settings`。如必须修改共享源码，先核对并同步 `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`。
4. 每个工单先补状态机或端到端回归测试，再改生产路径。
5. 不为对齐 OMO 而复制 TypeScript 模块边界；保留 Gear 的 Rust、GPUI 和 native worker 架构。

## 3. 工单一：收口 TaskManager 控制面状态机

### 3.1 改动范围

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`
- `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`

### 3.2 实现任务

1. 将 `TaskManagerControl` 缩减为命令转发与快照读取层，cancel/interrupt 统一路由 `TaskManager` 执行。
2. 将 `ActionOutcome` 拆分为可区分的 `Cancelled`、`Interrupted`、`NotContinuable { reason }`、`Noop { reason }`。
3. 为 `SendOutcome` / `SteerOutcome` 增加 `task_id`、`reason`、`queue_position`、`run_epoch` 等必要上下文，使其能表达 `Sent/Steered`、`Queued`、`Revived`、`NotContinuable`、`ScopeDenied`、`NotFound/Noop`。
4. `NativeAgentConnection` 直接返回 typed outcome，由 `thread_view` 映射成 Gear-only 状态文案；不得再用 `bool` 丢失原因。
5. 保证 cancel/interrupt 先持久化 terminal transition，再执行 handle abort/cancel/interrupt；handle 失败必须可见，但不能回滚 terminal state。

### 3.3 验收

- interrupt 成功时返回 `Interrupted`，不再返回 `Cancelled`。
- handle 在 cancel/interrupt 中报错时，record 仍保持已写入的 terminal state。
- GUI 能区分 queued、revived、not continuable 和 scope denied，并展示原因。
- 新增竞态测试：cancel 后晚到 complete、interrupt 后晚到 failed 均不改写终态。

## 4. 工单二：实现完整 terminal revive

### 4.1 改动范围

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/runtime.rs`

### 4.2 实现任务

1. 增加单一 `revive_task()` 路径，仅允许 `Messageability::Revive` 且 handle 仍 resident 的 task 进入。
2. revive 时必须原子更新：
   - `status = Running`
   - `residency_state = Resident`
   - `run_epoch += 1`
   - 清理上一轮 `finished_at/error/failure_kind/final result`
   - 创建新 attempt 或明确的 revived-turn record
3. 重新获取 concurrency slot，重建 `running_tasks` 跟踪，按新 epoch 重置 release guard 和 completion notification guard。
4. follow-up 与 steer 复用同一 revive 状态转移，区别只体现在向 handle 下发的命令。
5. 非 current task 的 resident handle 也必须可按 task id 继续，不得依赖单一 `current_task` 槽位。

### 4.3 验收

- completed/failed/interrupted resident task 发送 follow-up 后，snapshot 立即显示新 epoch 的 running task。
- revive 后的 completion 只通知一次，且不被上一 epoch 的 late completion 覆盖。
- concurrency 达上限时 revive 进入可见 queued 状态，不绕过并发限制。
- 新增至少两个端到端测试：follow-up revive 和 steer revive。

## 5. 工单三：补齐 session scope 和 pending 消息所有权

### 5.1 改动范围

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`

### 5.2 实现任务

1. 定义 `TaskCommandContext { caller_session_id, all_scope }`，传入 send、steer、cancel 和 interrupt。
2. 默认仅允许 caller 操作其 `parent_session_id` 或 `root_session_id` 范围内的 task。
3. 跨 session 操作必须显式使用 `all_scope`，并返回可审计的 `ScopeDenied` 或 accepted event。
4. pending message 保存真实 caller session，不得用 task id 冒充 session id。
5. pending drain 前重新校验 task epoch 和 scope；投递失败的剩余消息保持原顺序重新入队，不静默丢失。

### 5.3 验收

- sibling session 默认操作被拒绝，parent/root session 操作通过。
- `all_scope` 路径有独立测试和 lifecycle event。
- pending 期连续发送 3 条消息，worker 启动后按原顺序投递。
- 第 2 条投递失败时，第 2、3 条保持在队列中等待下次 drain。

## 6. 工单四：将 OMO Review Gate 落地为 Gear 硬门禁

### 6.1 改动范围

- `crates/gearbox_agent/src/runtime.rs`
- `crates/gearbox_agent/src/state.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_settings`

### 6.2 实现任务

1. 引入可配置 `ReviewGate` 和 `ReviewDimension`，首批支持 goal verification、code quality、security 和 QA execution。
2. 每个维度使用独立 prompt、worker route、artifact 和 pass/fail reason，不把多维度结果压成单个 provider 布尔值。
3. `require_all_pass=true` 作为默认硬门禁；任一必需维度失败时不得生成 complete。
4. 将 comment checker 作为 code-quality 的可选 check，先用 Gear 内部规则检查明显的组织性、旁白性注释，不引入外部 CLI 强依赖。
5. review 失败必须生成结构化 repair request，并经统一 budget policy 进入下一 worker turn。

### 6.3 验收

- 某一必需维度失败时，goal 不能被判定 complete。
- review artifact 能逐维度展示 model、route、evidence、result 和 repair request。
- review 触发的 worker call、premium call 和 route change 与 budget snapshot 一致。
- comment checker 对正常的非显然设计原因注释不误报。

## 7. 工单五：配置化 category tool policy 与 model variant

### 7.1 改动范围

- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/cli.rs`
- `crates/gearbox_settings`

### 7.2 实现任务

1. 将 `WorkerToolPolicy` 暴露到 category 配置，支持默认策略与用户局部覆盖。
2. 将 write/edit/review/explore/question/task 等能力映射为明确 allow/deny，不只在 prompt 中表述。
3. category route 配置支持 `model` 与可选 `variant`，并在 fallback route 中保留 variant。
4. 配置解析、route artifact、worker packet 和最终报告使用同一份解析后配置。

### 7.3 验收

- read-only category 在真实 tool dispatch 层拒绝 write/edit，不仅依赖 prompt 约束。
- 用户覆盖只改变指定字段，未指定字段保留 category 默认值。
- model variant 穿透到 native worker 和 command worker artifact。
- fallback 后的 model/variant 可在 route-transform artifact 中追溯。

## 8. 工单六：Stop Continuation Guard 与事件驱动续跑基础

### 8.1 改动范围

- `crates/gearbox_agent/src/runtime.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`
- Gear session artifact/state 存储路径

### 8.2 实现任务

1. 定义持久化 continuation state：`running/stopped/completed`，并与 goal id、session id 绑定。
2. GUI Stop Generation 与 Stop Continuation 分离：前者仅停当前 turn，后者设置 stop marker 并级联停止后续 task tree。
3. 仅显式新 goal、restart continuation 或 session deletion 可清除 stop marker。
4. 先提供 session-idle continuation 的内部事件接口，不在本工单中立即删除现有 `GoalLoop`。
5. 新续跑路径必须调用工单二的 revive 机制，不创建绕过 TaskManager 的 session 循环。

### 8.3 验收

- 用户停止 continuation 后，session idle 不会再自动发送 follow-up。
- 重启 GUI 后 stop marker 仍生效。
- Stop Generation 不误伤后续手动 revive，Stop Continuation 会拒绝自动 revive。
- continuation event 含 goal/session/task/run_epoch，可从 artifact 追溯。

## 9. 工单七：文档、回归与下一批分包

### 9.1 文档收口

1. 更新 `docs/gearbox-gear-remaining-gap-dogfood-plan.md`：
   - 把已实现的 destroy/reconciliation/LRU/TTL/lost/pending drain/last output 从“剩余未补”移除。
   - 使用实际测试数，不手工维护累计公式。
2. 更新 `docs/gearbox-omo-reference.md`，将“Gear 实现要点”分成 `已完成/部分完成/未完成`，避免用 v4.16.0 时的判断描述当前代码。
3. 共享源码若有新改动，同步 `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`。
4. 下一批另立计划，不混入本轮：keyword mode、task reminder、`init-deep`、handoff、plan format validator 和完整 Ralph Loop 迁移。

### 9.2 全量验收

```bash
cargo fmt --all -- --check
cargo test -p gearbox_agent -- --nocapture
./script/clippy -p gearbox_agent
```

如修改 `crates/agent` 或 `crates/agent_ui`，追加运行相应 crate 的定向测试和编译检查。

交付前必须完成以下端到端场景：

1. terminal task follow-up revive，新 epoch 完成并通知 parent 一次。
2. cancel/interrupt 与 late completion 并发，terminal state 不被改写。
3. pending 消息有序 drain，中途失败后剩余消息可重试。
4. sibling session 被 scope guard 拒绝，root session 或显式 `all_scope` 通过。
5. review gate 某一必需维度失败，goal 保持未完成并生成 repair artifact。
6. stop marker 存在时 session idle 不续跑，重启后仍不续跑。

## 10. 执行顺序与退出条件

| 顺序 | 工单 | 依赖 | 退出条件 |
|---|---|---|---|
| 1 | TaskManager 控制面状态机 | 无 | typed outcome 贯通 runtime 和 GUI |
| 2 | terminal revive | 工单一 | epoch、concurrency、completion 重新纳管 |
| 3 | session scope 与 pending ownership | 工单一 | 跨会话拒绝和有序重试通过 |
| 4 | Review Gate | 工单一 | 多维度硬门禁贯通 repair loop |
| 5 | Tool Policy 与 variant | 工单四 | 配置、dispatch、artifact 一致 |
| 6 | Stop Continuation Guard | 工单二 | stop marker 持久化且拦截自动续跑 |
| 7 | 文档与全量回归 | 全部 | 文档无已完成的假缺口，全量门槛通过 |

任一工单不得仅以“新增类型或文档”作为完成标准；必须有真实命令路径、持久化状态、失败分支和端到端回归证据。
