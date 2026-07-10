# Gearbox Diff Review — 计划符合性 & 逻辑审计

> 审查范围：`HEAD~9..HEAD`（9 个 commit）。核心文件：`runtime.rs` (+3730), `task_manager.rs` (+6826), `workers.rs` (+3715), `agent.rs` (+2234)

---

## 一、计划符合性总览

| 计划 Phase | 要求 | 实现状态 | 符合？ |
|-----------|------|---------|--------|
| Phase 1 | TaskManager 升级为 control plane，queue + running + archive，`ConcurrencyManager`，`wait_for` 非阻塞，cancel/interrupt 双向 | ✅ `TaskManager` 有 `records`/`queued_tasks`/`running_tasks`/`completed_archive`，`CancelTask` 处理 pending 和 running 两种 | ✅ |
| Phase 2 | `WorkerCategory` + `CategoryRouter` + ROUTE_HINT | ✅ `category_resolution_for_route()`，`selected_route_for_hint()` | ✅ |
| Phase 3 | Attempt/fallback retry，`TaskRecord.attempts[]`，failure_kind | ✅ `TaskAttempt` 有 `failure_kind`/`retry_reason`，`TaskManager` 内重试 | ✅ |
| Phase 4 | Session workers + revive + stale detection | ✅ `OpencodeSessionWorker` with revive，stale timeout in `tick()` | ✅ |
| Phase 5 | Native Zed worker backend | ✅ `GearZedWorkerBackend` + dispatcher | ✅ |
| Phase 6 | Codex/Claude worker pool + premium budget | ✅ premium_worker_budget，Codex/Claude default commands | ✅ |
| Phase 7 | ReviewEngine（provider review + independent reviewer + 连续 unknown 升级） | ✅ `CoordinatorReviewHook` + `provider_unknown_streak` escalation | ✅ |
| Phase 8 | Resilience（stagnation detector + stale sweep + orphan cleanup） | ✅ `detect_stagnation()` / `detect_context_risk()` / `recover_orphaned_records()` | ✅ |

**总体：实现完整覆盖了计划所有 Phase。**

---

## 二、发现的 Bug 和逻辑问题

### Bug 1（P0）：`send_follow_up_gear_task` / `steer_gear_task` 对 Terminal 状态的 task 无 `messageability()` 检查

**文件：** `crates/agent/src/agent.rs:2328-2390`

```rust
// 对 Cancelled/Limited/Failed 等 terminal 状态的任务：
Some(_) => {
    task_manager.lock()...send_follow_up_task(&task_id, prompt)
}
```

对 `Cancelled` 的 task 调用 `send_follow_up_task` 会走 `TaskManager::send_follow_up_task()` → 尝试 `WorkerSessionHandle.send_follow_up()`。如果 handle 已被 drop/dispose，返回 `Err`，但上层调用 `send_follow_up_gear_task` 返回 `Ok(false)`，可能误导调用方认为"发送成功但没 effect"而非"不应发送"。

**计划期望：** `messageability()` 检查后对 `Cancelled`/`Lost` 返回 `not-continuable`。

**修复：** 在 `send_follow_up_gear_task` 和 `steer_gear_task` 中加入状态检查，terminal 且非 revive-capable（即不是 Completed/Interrupted）时直接返回 false。

---

### Bug 2（P0）：`irunt` 和 `cancel` 没有"先 transition 再 abort"

**文件：** `crates/gearbox_agent/src/task_manager.rs:1074`（`interrupt_task`）

```rust
pub fn interrupt_task(&mut self, task_id: &str) -> Result<()> {
    // 先操作 handle
    if let Some(running_task) = self.running_tasks.get(task_id) {
        running_task.handle.interrupt()?;
    }
    // 后写状态
    if record.status == ManagedTaskStatus::Running {
        record.status = ManagedTaskStatus::Cancelled;  // 没有 Interrupted 状态
    }
}
```

**竞态场景：** `handle.interrupt()` 可能引发 worker 同步完成 → 后台 `dispatch_running_task` → `settle_finished_task` → 写入 `Failed` 状态 → `interrupt_task` 再覆盖为 `Cancelled` → 丢失了 `Failed` 信息。

**计划期望（来自 OMO 模式）：** 先 `store.transition(type: "interrupt")`，再 `handle.abort()`，这样后台的完成转换会被"terminal idempotence" 拒绝。

**Gear 目前没有 `Interrupted` 状态（只有 `Cancelled`），这是状态模型层面的缺失。**

---

### Bug 3（P1）：`provider_unknown_streak` 重置条件可能过于严格

**文件：** `crates/gearbox_agent/src/runtime.rs:777-791`

```rust
if verification_passed
    && coordinator_review.is_some_and(|review| {
        review.goal_satisfied.is_none()
            && review.stop_reason.as_deref().and_then(normalized_stop_reason).is_none()
    })
{
    provider_unknown_streak += 1;
} else {
    provider_unknown_streak = 0;  // 只要有一次 success/fail 就重置
}
```

**问题：** 如果 coordinator review 第一次返回 `goal_satisfied: Some(false)`（goal 不满足但给出了明确修复计划），streak 重置为 0。但接下来如果 coordinator 又返回 `None`（unknown），streak 从 1 开始。**这种"明确失败→未知→未知"序列不会触发 escalation**，但累计起来可能表明 coordinator 在循环。

**建议：** `provider_unknown_streak` 不应在 `goal_satisfied == Some(false)` 时重置。只有在 `goal_satisfied == Some(true)`（确认完成）或明确的 `stop_reason` 时才重置。

---

### Bug 4（P1）：`detect_stagnation` 的 diff 比较使用全相等

**文件：** `crates/gearbox_agent/src/runtime.rs:2168-2179`

```rust
if diff_history.iter().all(|snapshot| {
    snapshot.is_git_repo == first.is_git_repo
        && snapshot.status == first.status
        && snapshot.changed_files == first.changed_files
})
```

**问题：** `changed_files` 是一个 `Vec<String>`，使用 `==` 比较全相等。如果两次 iteration 之间 worker 只改了同一个文件的不同内容（文件名相同但 diff 不同），这个检查也会返回 true（误报为 stagnation）。正确的做法是检查 diff 的内容是否完全一致（例如前后 diff 的完整文本）。

---

### Bug 5（P1）：`context_safe()` 函数名与语义不匹配

**文件：** `crates/gearbox_agent/src/runtime.rs:1655-1662`

```rust
fn context_safe(iteration, max_iterations, changed_files, max_files_changed) -> bool {
    iteration <= max_iterations && changed_files <= max_files_changed
}
```

**问题：**
1. 函数名叫 `context_safe` 但检查的是 `iteration over budget` 和 `files over budget`。更好的命名：`within_scope_budget`
2. `iteration <= max_iterations` 在 `for iteration in 1..=max_iterations` 的约束下永远为 `true`（死条件）
3. 它不检查"context risk signals"（那些在 `context_guard_reason()` 中检查），造成逻辑分散

---

### Bug 6（P1）：`worker_call_count` 统计所有 attempts（含 fallback retry）

**文件：** `crates/gearbox_agent/src/runtime.rs:512-523`

```rust
worker_call_count += worker_task_record.attempts.len();
```

这意味着一次 goal iteration 如果 fail → retry → success（2 次 attempt），`worker_call_count += 2`。但 `max_worker_calls` 默认 = `max_iterations` = 5。所以在 `max_iterations=5` 的配置下，如果每轮都做一次 retry，实际只跑了 5/2 ≈ 2 轮就 budget exhausted。

**计划预算表中 `max_worker_calls: 8`，`max_iterations: 5` 是两个独立上限。** 当前实现中将 `max_worker_calls` 设为 `max_iterations`（默认 5），不等于计划的 8。

---

### Bug 7（P2）：`CompletionNotifier` 通知所有 terminal 状态（包括 Cancelled）

**文件：** `crates/gearbox_agent/src/runtime.rs:560-580`（通知调用点）

```rust
if let Some(notification) = CompletionNotifier::build_notification(...) {
    completion_notifier.try_notify(notification, ParentSessionState::Streaming, ...)?;
}
```

`build_notification` 对 `WorkerCancelled` 的 task 也会生成通知，但计划借鉴的 OMO 设计中, `cancelled` 和 `interrupted` 不应触发通知（它们是同步操作）。

**建议：** 在 `build_notification` 中过滤 `Cancelled` 状态（或只在 `Completed`/`Failed`/`Skipped` 时生成通知）。

---

### Bug 8（P2）：`repair_request_history` 清空后首次不推送

**文件：** `crates/gearbox_agent/src/runtime.rs:456-459`

```rust
if iteration > 1 {
    repair_request_history.push(worker_request.clone());
}
```

`detect_stagnation` 检查的是 `repair_requests.len() >= 2 && all equal`。但在 iteration 1 时 `repair_request_history` 为空。iteration 2 推入第一个 repair request（`len()=1`）。iteration 3 推入第二个（`len()=2`）才开始检测。这意味着需要有 3 次 iteration 才能检测到 stagnation（iteration 2 的 repair 与 iteration 3 的 repair 相同）。但它的检查条件是 `>= 2`（2 个相同的才算 stagnation），这应该从 iteration 2 开始检查。当前逻辑的 offset 导致需要 3 次迭代才开始检测。

**修复：** 改为 `if iteration >= 1`，或者在 iteration 1 结束时也 push 初始 request。

---

## 三、计划偏差（非 Bug，但需确认）

### 偏差 1：`max_worker_calls` 默认值不匹配计划

| 参数 | 计划值 | 实现值 | 影响 |
|------|--------|--------|------|
| `max_worker_calls` | 8 | `max_iterations`（默认 5） | 不匹配 |
| `max_premium_worker_calls` | 2 | `option.worker.premium_worker_budget`（0=unbounded） | CLI/env 可配 |
| `DEFAULT_MAX_ITERATIONS` | 计划 Phase 0 说 2，Budget 表说 5 | 5 | Phase 0 描述过时 |

### 偏差 2：完成通知对所有 terminal 状态触发

计划明确说 `cancelled`/`interrupted` 不应通知（OMO 的 `shouldNotifyStatus` 只包含 completed/error/lost）。实现未过滤。

### 偏差 3：`messageability()` 未实现

计划 Phase 4 描述的 `messageability` 函数（检查 `(status, residency_state)` 矩阵）没有实现。当前只在 `send_follow_up_gear_task`/`steer_gear_task` 中做了简单的 `Pending|Running` vs `_` 分支，但有覆盖缺口（如 `Cancelled` 应返回 not-continuable）。

---

## 五、Round 2：P1 补齐（typed outcomes、completion flush、unified budget）

> 实施时间：2026-07-09 ~ 2026-07-10，代码未合入 diff 范围（在 HEAD~9 之后）
> 回归基线：153 tests → 159 tests → 169 tests

### P1-1：TaskManagerControl 状态控制语义收口

| 项目 | 内容 |
|------|------|
| **问题** | `TaskManagerControl` 未与 `TaskManager` 共享 "先 transition 后 handle" 路径；send/steer 返回 `bool`，无法区分 `NotContinuable`/`Noop`/`Steer` |
| **改动** | `crates/gearbox_agent/src/task_manager.rs` — `SendOutcome`/`SteerOutcome` 枚举，终态任务返回 `NotContinuable` |
| **改动** | `crates/agent/src/agent.rs` — 适配新返回类型，GUI 侧错误文案映射 |
| **测试** | pending/queued/steer/revive 路径全覆盖，Cancelled/Lost 返回 `NotContinuable` 验证 |
| **commit** | `c99b6572dc` (部分，与 P1-3 混合) — "Add sha2 dependency and new task attempt status types" |

### P1-2：父会话 completion 通知串行化与重排

| 项目 | 内容 |
|------|------|
| **问题** | 同一 parent session 的多个 completion 可能乱序到达；忙状态时不应注入 completion |
| **改动** | `crates/gearbox_agent/src/task_manager.rs` — `CompletionNotifier` 增加 `flush_serializer`（per-session 串行锁）、`pending_flush`（排队唤醒）、flush 时状态重验证 |
| **改动** | `crates/gearbox_agent/src/runtime.rs` — `read_record` 闭包传入 flush 路径，实现 storage 级重检查 |
| **测试** | `completion_flush_serializes_rapid_arrivals` — 3 个并发 completion 串行验证 |
| **测试** | `completion_flush_works_after_idle_transition` — Streaming 入 buffer、Idle flush 去重、epoch bump 后 skip 验证 |
| **commit** | `067d783c82` — "Fix Gear completion flush and teardown lifecycle" |

### P1-3：provider-aware/depth 统一预算策略

| 项目 | 内容 |
|------|------|
| **问题** | route 变更、fallback、review 触发未统一扣减预算；budget_guard_reason 不包含触发源标记 |
| **改动** | `crates/gearbox_agent/src/runtime.rs` — 新增 `RouteChangeType` enum（`RouteChange`/`Fallback`/`ReviewTrigger`）、`BudgetController::apply_budget_for_route_change()`、`evaluate_goal_with_source()` 重载 |
| **改动** | `GoalDecisionPolicy.trigger_source: Option<RouteChangeType>`、`budget_guard_reason()` 输出附加 `(route change)`/`(fallback)`/`(review)` 标记 |
| **测试** | `budget_guard_reason_includes_trigger_source_label` — 三种触发源标记验证 |
| **测试** | `apply_budget_for_route_change_distinguishes_triggers` — BudgetController 为不同触发源返回不同错误消息 |
| **测试** | `budget_summary_matches_across_coordinator_review_and_goal_review` — budget_summary 在 goal_review_artifact 中一致嵌入 |
| **commit** | `c99b6572dc` (部分) — 与 P1-1 同 commit 提交 |

---

## 六、Round 3：P2 补齐（stagnation 归一化、tool-call delta、文档收口）

> 实施时间：2026-07-10，代码未合入 diff 范围
> 回归基线：169 tests

### P2-1：停滞信号来源增强（无效迭代更稳）

| 项目 | 内容 |
|------|------|
| **问题** | `detect_stagnation` 的 repair request 比较使用全相等，大小写/空白差异导致漏检 |
| **改动** | `crates/gearbox_agent/src/runtime.rs` — 新增 `normalize_repair()` 辅助函数（lowercase + collapse whitespace），`detect_stagnation()` 中 repair_requests 和 worker_outputs 比较改为归一化后比较 |
| **测试** | `stagnation_normalizes_repair_variations` — 大小写/空白变体触发 stagnation，语义不同不误报 |
| **commit** | 未独立 commit，与 P0-5 收口一并提交 |

### P2-2：Worker stream 深度（真正 tool-call delta）

| 项目 | 内容 |
|------|------|
| **问题** | `execute_command_with_prompt` 只输出 TurnStarted/Stdout/Stderr/TurnFinished，无 tool call 粒度事件 |
| **改动** | `crates/gearbox_agent/src/workers.rs` — 新增 `parse_and_emit_tool_events()` 扫描 stdout 中的 XML tool call 模式并 emit `AssistantTextDelta`/`ToolCallStarted`/`ToolCallFinished` 到 transcript + tool-events |
| **改动** | `crates/gearbox_agent/src/task_manager.rs` — 新增 `TranscriptEntry` 枚举（Parsed/Raw）、`TaskRecord::transcript_entries()` 方法 |
| **改动** | `crates/gearbox_agent/src/runtime.rs` — `collect_context_risk_texts()` 增加 tool-events 序列文本 |
| **测试** | `worker_transcript_includes_tool_call_deltas` — 验证 transcript/tool-events 包含 tool_call_started/finished、subscription 收到 ≥1 ToolCallStarted |
| **commit** | `c99b6572dc` (部分) — 与 P1-1/P1-3 同 commit 提交 |

### P2-3：文档收口（本文件）

| 项目 | 内容 |
|------|------|
| **改动** | 当前 diff review → 新增 Round 2/3 章节 |
| **改动** | 各 phase 文档 → 添加 completion notes |
| **改动** | dogfood plan → 完成状态更新 |
| **改动** | learnings.md → 追加文档映射 |
| **验证** | `cargo test -p gearbox_agent` — 169 tests pass |

---

## 七、总体评价（更新版）

| 维度 | 评分 | 说明 |
|------|------|------|
| Phase 覆盖度 | ✅ 100% | 8 个 Phase 全部实现 |
| 计划符合性 | ⚠️ 90% | 3 处偏差（max_worker_calls、通知过滤、messageability） |
| 逻辑正确性 | ⚠️ | 8 个 bug（2 P0 + 4 P1 + 2 P2） |
| 代码质量 | ✅ | 良好的测试覆盖，清晰的结构 |

**总结：实现基本正确且顺畅，与计划高度一致。主要需修复的是 Bug 1（send_follow_up 无状态检查）、Bug 2（interrupt 顺序问题），以及偏差 1（max_worker_calls 默认值）。**
