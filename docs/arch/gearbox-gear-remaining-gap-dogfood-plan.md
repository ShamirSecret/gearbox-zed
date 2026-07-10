# Gearbox Gear Runtime 狗粮计划（剩余缺口版）

> 目标：给 `docs/gearbox-gear-agent-plan.md` 的缺口阶段产出一个“中等模型可执行”的补全计划。
> 版本：2026-07-09
>
> 入口文档：
> - `docs/gearbox-gear-agent-plan.md`
> - `docs/gearbox-gear-agent-plan-review.md`
> - `docs/gearbox-gear-omo-deep-diff.md`
> - `docs/gearbox-diff-review-2026-07-09.md`

## 0. 执行原则

1. 每条工单都要有“输入/改动文件/验收标准”。
2. 一次只改一类缺口（便于回滚和审计）。
3. 每个阶段至少补一条可复现的回归测试。
4. 任何“看起来可运行，但行为不确定”的地方先写测试后再改。

## 1. 当前可确定已完成的基线（不重做）

- `TaskManager` 已有 `run_epoch`、`notified_epoch`、`messageability`、`killed`、`lost/interrupted`。
- `CompletionNotifier` 已实现：
  - 只在 `Completed/Failed/Lost` 通知；
  - `run_epoch` 去重；
  - buffer + busy re-check。
- `TaskManager` 已有 `queued follow-up/steer` 与 resident revive。
- `workers` 已有分类 prompt policy、tool policy、transcript/tool-events/partial-output 写入链路。
- `GoalLoop` 已有 `GoalDecisionPolicy`，含 independent reviewer gate、no-progress/context 风险、`max_provider_unknown_streak`。
- GUI 已有独立 Gear task panel。

以上不在本次 dogfood 第一轮修改范围。

---

## 2. 剩余缺口总清单（按优先级）

### P0（必须先补）

#### P0-1：`max_worker_calls` 不使用 `Goal.budget` 配置

- **现状问题**：
  - `runtime.rs` 构造 `BudgetController` 时固定用 `Budget::default().max_worker_calls`（值 8），忽略当前 goal 的 `max_worker_calls`。
- **目标**：
  - `BudgetController.max_worker_calls` 优先读 `goal.budget.max_worker_calls`，再 fallback 到全局默认。
- **改动范围**：
  - `crates/gearbox_agent/src/runtime.rs`（`run_with_options` 的 budget 初始化段）。
  - `crates/gearbox_agent/src/state.rs`（`Budget` 默认值与注释已与文档对齐）。
- **验收**：
  - 写一个 `GearGoal`，设置 `budget.max_worker_calls=1`，首个循环触发两次 worker attempts 时必须到 `limited`。
  - 测试文件新增/更新：`crates/gearbox_agent/src/runtime.rs` 内 `GoalDecisionPolicy` 相关预算测试。

#### P0-2：`worker_call_count` 的统计口径错

- **现状问题**：
  - `runtime.rs` 每轮循环 `worker_call_count += worker_task_record.attempts.len()`；一次循环内 retries 被当作多次 worker call。
- **目标**：
  - 每一轮 `GoalLoop` 只增量 1 次 worker-call（与 `max_worker_calls` 语义一致）。
  - 重试 attempts 用于独立维度指标（可加 `attempt_count`）
- **改动范围**：
  - `crates/gearbox_agent/src/runtime.rs`：迭代计数处（约 500 行附近）。
  - 可新增 `attempt_count` 到 `BudgetSnapshot` 输出，和 `worker_call_count` 分离。
- **验收**：
  - 在 one-iteration 多 attempts 场景，worker_call_count 只加 1。
  - 多轮失败/重试时，`max_worker_calls` 可达到期望上限（默认 8 或 goal 指定值）。

#### P0-3：`provider_unknown_streak` 重置逻辑过严

- **现状问题**：
  - `runtime.rs` 只有 `verification_passed && review.goal_satisfied is None && no stop_reason` 不重置；一旦出现 `goal_satisfied==Some(false)` 后续 `unknown` 无法正确累计。
- **目标**：
  - 仅在以下情况下重置：
    - `verification_passed && goal_satisfied == Some(true)`
    - 或出现明确 `STOP_REASON`（needs_user/blocked/limited/complete）
  - `goal_satisfied == Some(false)` 不能重置 streak。
- **改动范围**：
  - `crates/gearbox_agent/src/runtime.rs`（`provider_unknown_streak` 分支）。
- **验收**：
  - 增加测试：`verification_passed -> goal_satisfied=false -> unknown -> unknown` 会被按逻辑升级/停止（不误降到 0）。

#### P0-4：`detect_stagnation` diff 比较太弱

- **现状问题**：
  - `detect_stagnation` 仅比对 `changed_files: Vec<String>`，同文件不同内容会误判为“无进展”。
- **目标**：
  - 记录并比较结构化 diff 语义：
    - diff 的 normalized diff-hash，或
    - git patch 文本摘要（去掉时间戳/无意义噪音）。
- **改动范围**：
  - `crates/gearbox_agent/src/runtime.rs`：
    - `DiffSnapshot` 增加 `diff_hash`/`diff_text_signature`。
    - `detect_stagnation` 在比较 diff 时使用 signature 而非文件列表。
  - 如有必要，补充工具函数在 `ScopeCheck` 外部复用。 
- **验收**：
  - 同一文件名不同内容改动时不再触发 “No file changes...”。
  - 两轮完全一致 diff 仍触发停滞信号。

#### P0-5：`GoalDecisionPolicy` 对“无 fallback”处理不收口

- **现状问题**：
  - 当 `category_resolution` 报告无可用 fallback 时，当前仍可能产生 continue/review 循环而非停顿。
- **目标**：
  - 在 policy 中把“当前 route 已是最后可行路由”显式识别为 `needs_user` 或 `limited`（按 budget 与上下文）。
  - 同时把“是否有真实下一条不同 route”写入 `category_resolution_result`。
- **改动范围**：
  - `crates/gearbox_agent/src/runtime.rs`（`GoalDecisionPolicy::evaluate` 分支）。
  - `crates/gearbox_agent/src/workers.rs` / `category_resolution` 产物。
- **验收**：
  - `category` 无可用 fallback 时不再生成同 route 的下一轮。
  - review/goal artifact 中可见 `nearest_fallback: none` 并进入保守终止。

---

### P1（应在 P0 之后）

#### P1-1：`TaskManagerControl` 与 `TaskManager` 状态控制语义收口

- **现状问题**：
  - `TaskManagerControl` 暂未被统一为与 `TaskManager` 完全一致的 “先 transition 后 handle 操作” 路径。
  - 有些控制接口仍返回 bool，未对用户原因细分。
- **目标**：
  - 把 `cancel_current_task/interrupt_current_task` 等按 `TaskManager` 的状态机语义改造，避免未来 race。
  - 将 `Send/Steer` 的 `bool` 返回升级为 `SendOutcome/SteerOutcome`，至少能返回：`Steer/Revive/Queued/NotContinuable/Noop`。
- **改动范围**：
  - `crates/gearbox_agent/src/task_manager.rs`
  - `crates/agent/src/agent.rs`
  - 需要的话补 GUI 文案映射（`thread_view`）。
- **验收**：
  - 终态任务（Cancelled/Lost）返回 `NotContinuable`，GUI 能展示原因而不是“成功但未生效”。
  - 新增/复用测试覆盖 `pending/queued/steer/revive`。

#### P1-2：父会话 completion 通知再精修

- **现状问题**：
  - 第一版已做了 Streaming/Idle 与 buffer，但还需补更完整的 parent session 序列化与失败重排。
- **目标**：
  - 同一 parent session 的 completion flush 串行执行。
  - 失败重试后的入队要保留顺序，并在再次 flush 时再次校验状态。
  - 统一只在 idle 点注入 completion。
- **改动范围**：
  - `crates/gearbox_agent/src/task_manager.rs`
  - `crates/gearbox_agent/src/runtime.rs`
  - `crates/agent_ui/src/conversation_view/thread_view.rs`
- **验收**：
  - 串行化压力测试（2~3 个 completion 快速到达）不会乱序。
  - Busy 状态期间入 buffer 后 idle 再 flush 一次且去重仍有效。

#### P1-3：provider-aware/depth 统一预算策略（Phase 09 残缺）

- **现状问题**：
  - `child_depth` 与 `max_provider_unknown_streak` 已接入，`provider-aware`/`depth` 在并行与 route 升级时还未全链路统一。
- **目标**：
  - 单一 budget policy 输入点：
    - 每次 route 变更、每次 fallback、每次 review 触发都统一扣减/记录。
  - route upgrade 和 premium worker 使用必须同时写入 budget guard reason。
- **改动范围**：
  - `crates/gearbox_agent/src/runtime.rs`
  - `crates/gearbox_agent/src/workers.rs`
  - `crates/gearbox_agent/src/task_manager.rs`
- **验收**：
  - 触发 provider 多跳 + premium 重试时，停止条件不会只依赖单个子系统。
  - budget snapshot 与 `goal_review_artifact` 一致。

---

### P2（可在 P1 之后并行做）

#### P2-1：停滞信号来源增强（无效迭代更稳）

- 在 `detect_stagnation` 增加“repair request 变体归一化”与“重复但语义等价输出”过滤，避免同义句触发误报。
- 文件：`crates/gearbox_agent/src/runtime.rs`。
- 验收：
  - 同类修复语义（小写/空白差异）不再误判。

#### P2-2：Worker stream 深度（真正 tool-call delta）

- 在 `OpencodeSessionWorker` / `Zed native worker` 的 event stream 中补齐 assistant/tool delta 粒度，给 review 和 context 风险更可靠的数据。
- 文件：
  - `crates/gearbox_agent/src/workers.rs`
  - `crates/gearbox_agent/src/task_manager.rs`
- 验收：
  - 有最小 delta 级的 `transcript` 和 `tool-events` 记录。
  - context 风险不再仅依赖 stdout/result。

#### P2-3：文档化与后续 Phase 分包

- 把本计划每个 P0/P1/P2 小块映射回工单：
  - `phase-05-completion-parent-wake.md`
  - `phase-08-goal-loop-review-budget.md`
  - `phase-09-gui-parallel-worker-pool.md`
- 需要一并补：尚未完成项与完成时间窗口、回归命令。

---

## 3. 推荐执行顺序（dogfood 版）

### 第一轮（P0）

1. `P0-1 max_worker_calls`（单测先行）
2. `P0-2 worker_call_count`（覆盖 attempts/iterations 区分）
3. `P0-3 provider_unknown_streak`
4. `P0-4 detect_stagnation 内容签名`
5. `P0-5 no-fallback 收口`

### 第二轮（P1）

6. `P1-1 TaskManagerControl -> TaskManager 统一返回语义`
7. `P1-2 parent completion 的串行/重排`
8. `P1-3 provider-aware/depth 统一 budget`

### 第三轮（P2）

9. `P2-1 停滞信号增强`
10. `P2-2 stream 深度补齐`
11. `P2-3 文档收口`

---

## 4. 关键验收清单（每一轮都要）

- `cargo fmt`
- `cargo test -p gearbox_agent -- --nocapture`
- 重点回归（按本文件标记新增）至少执行一次。
- 交付前运行：
  - 一个 `max_worker_calls` 与 `max_iterations` 同时命中的 case。
  - 一个 `same file different content` 的 `detect_stagnation` case。
  - 一个 `provider_unknown_streak` 3 轮 unknown / false/unknown 混合 case。
  - 一个 `nearest_fallback none` 的 repair case。

## 5. 完成状态总览（2026-07-10）

### 完成项汇总

| 优先级 | 项 | 状态 | 测试增量 | 关键改动文件 |
|--------|-----|------|---------|-------------|
| P0-1 | `max_worker_calls` 使用 goal budget | ✅ | +1 | `runtime.rs` |
| P0-2 | `worker_call_count` 统计口径修复 | ✅ | +1 | `runtime.rs` |
| P0-3 | `provider_unknown_streak` 重置逻辑 | ✅ | +1 | `runtime.rs` |
| P0-4 | `detect_stagnation` diff hash | ✅ | +1 | `runtime.rs`, `tools.rs` |
| P0-5 | no-fallback 收口 | ✅ | +2 | `runtime.rs`, `workers.rs` |
| P1-1 | TaskManagerControl 返回语义 | ✅ | +2 | `task_manager.rs`, `agent.rs` |
| P1-2 | Completion flush 串行化 | ✅ | +2 | `task_manager.rs`, `runtime.rs` |
| P1-3 | 统一 budget 入口 | ✅ | +3 | `runtime.rs` |
| P2-1 | Stagnation 归一化 | ✅ | +1 | `runtime.rs` |
| P2-2 | Tool-call delta stream | ✅ | +1 | `workers.rs`, `task_manager.rs`, `runtime.rs` |
| **P2-3** | **文档收口** | **✅** | **0** | **各 docs/*.md, learnings.md** |

### 回归命令

```bash
cargo test -p gearbox_agent
```

**结果：** 169 tests pass（基线 153 → P0 轮 +5 → 158 → P1 轮 +7 → 165 → P2 轮 +4 → 169）

### 剩余未补缺口

以下缺口不在本次 P0/P1/P2 范围内，为后续轮次预留：

| # | 缺口 | 来源文档 | 影响 | 建议 |
|---|------|---------|------|------|
| 1 | 单一销毁端口 | part5-lifecycle.md #1 | 每个路径独立销毁，可能泄漏 | 引入 `destroy_task()` 统一入口 |
| 2 | 启动 reconciliation | part5-lifecycle.md #2 | 崩溃重启后残留 `running` 记录 | 启动时扫描 task-record.json |
| 3 | LRU eviction | part5-lifecycle.md #3 | 无 resident 上限控制 | 新增 `admit_resident()` |
| 4 | TTL 清理 | part5-lifecycle.md #5 | 无时间基记录过期 | 启动时删除超 N 天旧记录 |
| 5 | `lost` 记录保护 | part5-lifecycle.md #6 | lost 记录无保护 | 引入 lost 状态 |
| 6 | `reviveTerminal` | part3-steering.md #3 | completed task 不能继续 | 新增 revive 路径 |
| 7 | `notifyStarted()` drain | part3-steering.md #4 | pending 期间消息丢失 | 消息队列 |
| 8 | `scopeDenied()` | part3-steering.md #5 | 无控制路径会话隔离 | 新增 caller_session_id 检查 |
| 9 | 中断时捕获 lastAssistantText | part3-steering.md #2 | 中断后看不到部分进度 | handle.last_output() capture |

### 文档映射

| 完成项 | 记录在 |
|--------|--------|
| P0-1~P0-5, P1-3, P2-1 | `docs/gearbox-omo-part8-11-remaining.md` |
| P0-5, P1-2 | `docs/gearbox-omo-part5-lifecycle.md` |
| P1-1 | `docs/gearbox-omo-part2-task-manager.md` |
| P2-2 | `docs/gearbox-omo-part3-steering.md` |
| P1~P3 全部 | `docs/gearbox-diff-review-2026-07-09.md` 五~六章 |

---

## 6. 里程碑定义（更新版）

- ~~每个 P0 项完成后，更新 `docs/gearbox-diff-review-2026-07-09.md` 或新补丁文件。~~ ✅ 已全部完成
- ~~P1 完成后同步更新 `phase-05/08/09` 工单对应章节。~~ ✅ 已全部完成
- ~~全部完成后，重命名本文件为最终版并启动完整的中等模型闭环跑。~~ ✅ 文档收口已完成，可启动闭环跑
