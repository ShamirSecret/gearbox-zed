# Gear ↔ Omo 功能对齐完整工作计划

## TL;DR (For humans)

将 Gearbox 的 agent runtime（`crates/gearbox_agent/`）与 OpenCode 的功能完全对齐。共 11 个缺口项，分三轮执行：

- **第一轮 (P0)**：5 个必须优先修复的 bug/缺口 — 预算配置、计数口径、streak 重置、停滞检测、fallback 终止
- **第二轮 (P1)**：3 个功能收口 — TaskManager 控制语义、completion 通知串行化、统一预算策略
- **第三轮 (P2)**：3 个增强 — 停滞信号降噪、stream 深度、文档收口

原则：TDD（先测试后改）、一次只改一类缺口、每个阶段至少补一条回归测试。

---

## 依赖矩阵

```
P0-1 (max_worker_calls)          → 无依赖
P0-2 (worker_call_count)         → 无依赖（可与 P0-1 并行）
P0-3 (provider_unknown_streak)   → 无依赖
P0-4 (detect_stagnation)         → 无依赖
P0-5 (no-fallback)               → 依赖 P0-3（streak 重置逻辑影响 fallback 判定）
                                   依赖 category_resolution_result 的 nearest_fallback
---
P1-1 (TaskManagerControl)        → 依赖 P0-5（fallback 收口后的状态一致）
P1-2 (completion flush)          → 依赖 P1-1（状态机语义收口后才有可靠的状态判定）
P1-3 (unified budget)            → 依赖 P0-1、P0-2、P0-3（预算字段统一后才能统一扣减）
---
P2-1 (stagnation noise)          → 依赖 P0-4（增强是在 P0-4 基础上的）
P2-2 (stream depth)              → 依赖 P1-1（stream 数据写入需要可靠的状态管理）
P2-3 (documentation)             → 依赖所有 P0、P1（文档收口在功能完成后）
```

---

## Todos

### 第一轮：P0（必须先补）

> ✅ **P0-1 至 P0-4 已完成**（由之前会话实现，153 项测试全部通过）

#### P0-1：`max_worker_calls` 使用 `Goal.budget` 配置 ✅

**WHERE**: `crates/gearbox_agent/src/runtime.rs`

**WHY**: 第 321 行硬编码 `Budget::default().max_worker_calls`（值为 8），忽略了第 191 行 `goal.budget.max_worker_calls`（已在 `state.rs` 结构中存在）。导致用户设置的 `budget.max_worker_calls` 不生效。

**HOW**:
1. 在 `run()` 函数（`runtime.rs`）中，将第 321 行的：
   ```rust
   max_worker_calls: Budget::default().max_worker_calls,
   ```
   改为：
   ```rust
   max_worker_calls: goal.budget.max_worker_calls,
   ```

2. 在测试 `tests` 模块中新增测试 `budget_uses_goal_max_worker_calls()`：
   - 构造 `Goal` 设置 `budget.max_worker_calls = 1`
   - 验证 `BudgetController` 实例化后 `max_worker_calls == 1`
   - 验证第一个 worker call 成功，第二次到 `limited`

**ACCEPTANCE**:
- 写一个 `GearGoal`，设置 `budget.max_worker_calls=1`，首个循环触发两次 worker attempts 时必须到 `limited`
- 单元测试验证 `BudgetController` 构造时正确读取 `goal.budget`

**QA**: `cargo test -p gearbox_agent evaluation_maps_worker_call_budget_limit_to_limited` 仍然通过 + 新测试通过

**COMMIT**: `fix: P0-1 use goal.budget.max_worker_calls in BudgetController`

---

#### P0-2：`worker_call_count` 统计口径修复 ✅

**WHERE**: `crates/gearbox_agent/src/runtime.rs` 第 501 行附近

**WHY**: 第 501 行 `worker_call_count += worker_task_record.attempts.len()` 将每次重试都算作一次 worker call，与 `max_worker_calls` 语义不一致（`max_worker_calls` 应限制独立工作调用次数，而非 attempt 次数）。

**HOW**:
1. 将第 501 行 `worker_call_count += worker_task_record.attempts.len()` 改为 `worker_call_count += 1`（每轮迭代只记 1 次 worker call）

2. 在 `BudgetSnapshot` 结构体（第 1637 行）中新增 `attempt_count: usize` 字段：
   ```rust
   attempt_count: usize,
   ```

3. 在循环中（~500 行附近）新增 `attempt_count` 的累加：
   ```rust
   attempt_count += worker_task_record.attempts.len();
   ```

4. 在 `budget_summary()` 函数（第 1659 行）和 `budget_guard_reason()` 方法（第 1697 行）保持 `worker_call_count` 的限流逻辑不变

5. 更新 `budget_summary` 格式化字符串，新增 `attempts` 指标

**ACCEPTANCE**:
- 在 one-iteration 多 attempts 场景，`worker_call_count` 只加 1（用 `BudgetSnapshot.worker_call_count` 断言）
- 多轮失败/重试时，`max_worker_calls` 可达到期望上限（默认 8 或 goal 指定值）
- `attempt_count` 正确反映所有 attempt 总数

**QA**: 新增测试 `worker_call_count_increments_once_per_iteration()` + 现有 `evaluation_maps_worker_call_budget_limit_to_limited` 仍然通过

**COMMIT**: `fix: P0-2 decouple worker_call_count from attempt count`

---

#### P0-3：`provider_unknown_streak` 重置逻辑修复 ✅

**WHERE**: `crates/gearbox_agent/src/runtime.rs` 第 764-777 行

**WHY**: 当前逻辑：
```rust
if verification_passed
    && coordinator_review.is_some_and(|review| {
        review.goal_satisfied.is_none()
            && review.stop_reason...is_none()
    })
{
    provider_unknown_streak += 1;
} else {
    provider_unknown_streak = 0;  // ← 这里误重置
}
```
当 `goal_satisfied == Some(false)` 时（即 review 确认目标未满足），streak 也被重置为 0，导致 `unknown` 无法正确累计。

**HOW**:
1. 将第 764-777 行的条件改为：
   ```rust
   // Reset provider_unknown_streak only when:
   // 1. verification_passed && goal_satisfied == Some(true), or
   // 2. a concrete STOP_REASON is present (needs_user/blocked/limited/complete)
   let has_concrete_stop_reason = coordinator_review
       .and_then(|review| review.stop_reason.as_deref())
       .and_then(normalized_stop_reason)
       .is_some();
   let goal_verified = verification_passed
       && coordinator_review.is_some_and(|review| review.goal_satisfied == Some(true));
   
   if goal_verified || has_concrete_stop_reason {
       provider_unknown_streak = 0;
   } else if verification_passed
       && coordinator_review.is_some_and(|review| {
           review.goal_satisfied.is_none()
               && review.stop_reason...is_none()
       })
   {
       provider_unknown_streak += 1;
   }
   // Otherwise: provider_unknown_streak unchanged (don't reset)
   ```

**ACCEPTANCE**:
- 序列：`verification_passed → goal_satisfied=false → unknown → unknown` 时 streak 不会降到 0
- 序列：`verification_passed → goal_satisfied=true` 时 streak 重置为 0
- 序列：`stop_reason=limited` 时 streak 重置为 0

**QA**: 新增测试 `provider_unknown_streak_not_reset_on_false_goal_satisfied()` + 现有 `evaluation_honors_provider_unknown_streak_budget_limit` 仍然通过

**COMMIT**: `fix: P0-3 correct provider_unknown_streak reset logic`

---

#### P0-4：`detect_stagnation` diff 内容签名比较 ✅

**WHERE**:
- `crates/gearbox_agent/src/tools.rs` — `DiffSnapshot` 结构体（第 66 行）
- `crates/gearbox_agent/src/runtime.rs` — `detect_stagnation` 函数（第 2153 行）

**WHY**: 当前 `detect_stagnation` 仅比较 `changed_files: Vec<String>`，同文件不同内容会误判为"无进展"。

**HOW**:
1. 在 `DiffSnapshot`（`tools.rs` 第 66 行）中新增字段：
   ```rust
   pub diff_hash: Option<String>,
   ```

2. 在 `git_snapshot()` 函数（`tools.rs` 第 166 行）中生成 diff hash：
   - 运行 `git diff` 获取 patch 文本
   - 用 SHA256 计算 patch 的哈希（去掉时间戳行 `---/+++` 中的时间戳噪音）
   - 将哈希存入 `diff_hash`

3. 新增工具函数 `normalize_diff_patch(patch: &str) -> String` 去掉时间戳行差异

4. 更新 `detect_stagnation`（`runtime.rs` 第 2161-2173 行）：
   - 当 `diff_history.len() >= 2` 时，比较 `diff_hash` 而非 `changed_files`
   - 仅当所有 `diff_hash` 完全一致时触发"无文件变更"信号

**ACCEPTANCE**:
- 同一文件名不同内容改动时不再触发 "No file changes..."（不同 `diff_hash`）
- 两轮完全一致 diff 仍触发停滞信号（相同 `diff_hash`）

**QA**: 新增测试 `stagnation_detects_identical_content_by_diff_hash()` + 更新现有 `stagnation_detects_consecutive_no_diff_iterations` 使用 diff_hash

**COMMIT**: `fix: P0-4 content-aware diff hash for stagnation detection`

---

#### P0-5：`GoalDecisionPolicy` 对"无 fallback"处理收口 ✅

**WHERE**:
- `crates/gearbox_agent/src/runtime.rs` — `GoalDecisionPolicy::evaluate()`（第 1781 行）
- `crates/gearbox_agent/src/workers.rs` — `category_resolution_for_route()`（第 541 行）

**WHY**: 当 `category_resolution` 报告 `nearest_fallback: None` 时，当前仍然可能产生 continue/review 循环而非停顿。

**HOW**:
1. 在 `CategoryResolutionResult`（`workers.rs` 第 839 行）确认所有变体都包含 `nearest_fallback` 字段（已存在）

2. 在 `GoalDecisionPolicy` 结构体（`runtime.rs` 第 1592 行）中新增字段：
   ```rust
   nearest_fallback_available: bool,
   ```
   在 `evaluate_goal()` 函数（第 2477 行）接收此参数

3. 在 `run()` 函数的循环中（~797 行），在调用 `evaluate_goal()` 之前注入：
   ```rust
   let has_fallback = category_resolution_result
       .nearest_fallback()
       .is_some();
   ```

4. 在 `GoalDecisionPolicy::evaluate()` 新增检查分支（~第 1863 行后）：
   ```rust
   if !self.verification_passed
       && !self.nearest_fallback_available
       && self.no_progress_signals.is_empty()
   {
       return GoalEvaluation {
           status: GoalStatus::Limited,
           should_continue: false,
           summary: "Goal reached the last feasible worker route with no alternative fallback.".to_string(),
           route_hint_override: None,
       };
   }
   ```

5. 给 `CategoryResolutionResult` 添加辅助方法 `nearest_fallback()`：
   ```rust
   impl CategoryResolutionResult {
       pub fn nearest_fallback(&self) -> Option<&FallbackRoute> {
           match self {
               CategoryResolutionResult::Resolved { nearest_fallback, .. }
               | CategoryResolutionResult::Disabled { nearest_fallback, .. }
               | CategoryResolutionResult::NotFound { nearest_fallback, .. }
               | CategoryResolutionResult::ModelUnavailable { nearest_fallback, .. } => {
                   nearest_fallback.as_ref()
               }
           }
       }
   }
   ```

**ACCEPTANCE**:
- `category` 无可用 fallback 时不再生成同 route 的下一轮（返回 `Limited` 而非 `Running`）
- review/goal artifact 中可见 `nearest_fallback: none` 并进入保守终止

**QA**: 新增测试 `evaluation_limits_when_no_fallback_available()` + 更新 `category_resolution_for_route_reports_distinct_nearest_fallback`

**COMMIT**: `fix: P0-5 halt loop when no fallback route is available`

---

### 第二轮：P1（应在 P0 之后）

> ✅ **P1-3 已完成**（统一预算入口 `apply_budget_for_route_change()` + 测试）

#### P1-1：`TaskManagerControl` 与 `TaskManager` 状态控制语义收口 ✅

**WHERE**:
- `crates/gearbox_agent/src/task_manager.rs` — `TaskManagerControl`（第 587 行）
- `crates/gearbox_agent/src/runtime.rs` — 调用处

**WHY**: `cancel_current_task` / `interrupt_current_task` / `send_follow_up_current_task` / `steer_current_task` 等返回 `Result<bool>`，用户无法区分操作失败的具体原因（NotContinuable/Noop/Queued/Sent等）。

**HOW**:
1. 在 `task_manager.rs` 中新增返回类型枚举：
   ```rust
   #[derive(Clone, Debug, PartialEq, Eq)]
   pub enum SendOutcome {
       /// Message sent to running worker
       Sent,
       /// Task queued because worker is pending
       Queued,
       /// Worker completed/failed and can be revived with this message
       Revive,
       /// Worker is in terminal state and cannot be continued
       NotContinuable,
       /// No task found
       Noop,
   }
   
   #[derive(Clone, Debug, PartialEq, Eq)]
   pub enum SteerOutcome {
       Steered,
       Revive,
       Queued,
       NotContinuable,
       Noop,
   }
   ```

2. 修改 `TaskManagerControl` 方法签名：
   - `cancel_current_task(&self) -> Result<bool>` → `cancel_current_task(&self) -> Result<ActionOutcome>`
   - `interrupt_current_task(&self) -> Result<ActionOutcome>`
   - `send_follow_up_current_task(&self, prompt) -> Result<SendOutcome>`
   - `steer_current_task(&self, prompt) -> Result<SteerOutcome>`
   - 对应地修改 `cancel_task` / `interrupt_task` / `send_follow_up_task` / `steer_task`

3. 各方法内部逻辑：
   - `cancel_current_task`：当前无任务 → `Noop`；任务已完成/已取消 → `NotContinuable`；成功取消 → 返回类似 `ActionOutcome::Cancelled`
   - `steer_current_task`：Pending → `Queued`；Running → `Steered`；Completed/Failed 且有 handle → `Revive`；终态 → `NotContinuable`

4. 使用一个统一的 `ActionOutcome` 枚举来表达取消/中断的结果。

**ACCEPTANCE**:
- 终态任务（Cancelled/Lost）返回 `NotContinuable`
- GUI 能展示原因而不是"成功但未生效"
- 新增/复用测试覆盖 pending/queued/steer/revive

**QA**: 新增测试至少覆盖 `steer_on_terminal_task_returns_not_continuable()`、`cancel_on_running_task_returns_cancelled()` 等

**COMMIT**: `feat: P1-1 typed outcomes for TaskManagerControl operations`

---

#### P1-2：父会话 completion 通知串行化与重排 ✅

**WHERE**:
- `crates/gearbox_agent/src/task_manager.rs` — `CompletionNotifier`（第 2487 行）
- `crates/gearbox_agent/src/runtime.rs` — `CompletionNotificationFlushGuard`（第 117 行）

**WHY**: 第一版已实现了 Streaming/Idle 与 buffer，但需要：
- 同一 parent session 的 completion flush 串行执行
- 失败重试后入队保留顺序
- 统一只在 idle 点注入 completion

**HOW**:
1. 在 `CompletionNotifier` 中新增 `flush_serializer: Arc<Mutex<HashMap<String, bool>>>`，记录每个 session 是否正在执行 flush

2. 修改 `flush_buffer()`（第 2597 行）：
   - 进入时检查并设置该 session 的 serialization lock
   - 如果已有正在执行的 flush，将当前请求入队到 `pending_flush: Arc<Mutex<HashMap<String, VecDeque<()>>>>`
   - flush 完成后检查 pending 队列，如有则触发下一次 flush

3. 在 buffer flush 前增加状态二次验证：检查 `TaskRecord` 的最新状态与 notification 一致后才发送

4. 确保 `flush_buffer` 只在 `ParentSessionState::Idle` 时通过 `can_wake()` 检查才注入

**ACCEPTANCE**:
- 串行化压力测试（2~3 个 completion 快速到达）不会乱序
- Busy 状态期间入 buffer 后 idle 再 flush 一次且去重仍有效
- 失败重试后重新 flush 仍保持正确的 epoch 去重

**QA**: 新增测试 `completion_flush_serializes_rapid_arrivals()` + `completion_flush_works_after_idle_transition()`

**COMMIT**: `feat: P1-2 serialize parent completion flush with ordered retry`

---

#### P1-3：provider-aware / depth 统一预算策略 ✅

**WHERE**:
- `crates/gearbox_agent/src/runtime.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/task_manager.rs`

**WHY**: `child_depth` 与 `max_provider_unknown_streak` 已接入，但 `provider-aware`/`depth` 在并行与 route 升级时还未全链路统一。

**HOW**:
1. 在 `GoalDecisionPolicy` 或 `BudgetController` 中新增统一入口方法 `apply_budget_for_route_change()`：
   - 每次 route 变更、每次 fallback、每次 review 触发都统一扣减/记录
   - 写入 `budget_guard_reason` 时明确区分触发源

2. 在 `run()` 循环中（~797 行附近）调用此统一入口替代分散的计数更新

3. 确保 route upgrade 和 premium worker 使用同时写入 budget guard reason（现已在 `budget_guard_reason` 中，但统一入口确保不会被遗漏）

4. 让 `BudgetSnapshot` 输出的 `budget_summary` 与 `goal_review_artifact` 的 budget 字段完全一致（校验两者来源为同一 `BudgetController` 实例）

**ACCEPTANCE**:
- 触发 provider 多跳 + premium 重试时，停止条件不会只依赖单个子系统
- budget snapshot 与 `goal_review_artifact` 一致

**QA**: 新增集成测试：模拟 provider 切换 + premium worker 调用，验证所有子系统都在同一 budget 线约束

**COMMIT**: `feat: P1-3 unified budget policy entry for route/fallback/review deductions`

---

### 第三轮：P2（可在 P1 之后并行做）

> ✅ **P2-1 已完成**（repair/output 归一化比较 + 测试）

#### P2-1：停滞信号来源增强（无效迭代更稳） ✅

**WHERE**: `crates/gearbox_agent/src/runtime.rs` — `detect_stagnation` 函数（第 2153 行）

**WHY**: repair request 的文本可能存在微小差异（大小写、空白）但语义相同，导致停滞信号不被触发。

**HOW**:
1. 在 `detect_stagnation` 的 repair_requests 比较（第 2185-2193 行）中，加入归一化：
   ```rust
   fn normalize_repair(text: &str) -> String {
       text.to_lowercase()
           .split_whitespace()
           .collect::<Vec<_>>()
           .join(" ")
   }
   ```
   比较时先归一化再判等。

2. 在 worker_outputs 比较（第 2195-2203 行）中加入相同的归一化处理

3. 可选：为 `DiffSnapshot` 增加 `changed_file_count` 辅助字段，用于更精确的停滞判断

**ACCEPTANCE**:
- 同类修复语义（大小写/空白差异）不再误判为不同内容
- 语义不同的 repair request 仍然正确触发停滞信号

**QA**: 新增测试 `stagnation_normalizes_repair_variations()`

**COMMIT**: `feat: P2-1 normalize repair/output text for stagnation dedup`

---

#### P2-2：Worker stream 深度（真正 tool-call delta） ✅

**WHERE**:
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/task_manager.rs`

**WHY**: 当前 assistant/tool delta 粒度不够细，review 和 context 风险检测无法获得可靠的数据。

**HOW**:
1. 在 `OpencodeSessionWorker` 和 `Zed native worker` 的 event stream 中，补齐 assistant/tool delta 粒度：
   - 每个 tool call 的开始/结束/结果
   - 每个 assistant message 的完整内容

2. 在 `WorkerResult` 或 `TaskRecord` 中增加 `transcript_entries: Vec<TranscriptEntry>` 或使用已有的 `transcript.jsonl` 路径但确保写入粒度足够细

3. 确保 `collect_context_risk_texts()` 函数（第 2208 行）能访问到这些 delta 级数据，让 context 风险检测不再仅依赖 `stdout/result`

**ACCEPTANCE**:
- 有最小 delta 级的 `transcript` 和 `tool-events` 记录
- context 风险不再仅依赖 stdout/result

**QA**: 解析 `transcript.jsonl` 验证包含至少 assistant message 和 tool call 边界

**COMMIT**: `feat: P2-2 granular worker stream with tool-call deltas`

---

#### P2-3：文档化与后续 Phase 分包 ✅

**WHERE**:
- `docs/` 目录下的 phase 工单文件

**WHY**: 需要将本计划的每个 P0/P1/P2 小块映射回到对应的 Phase 工单文档，并补全未完成项与时间窗口。

**HOW**:
1. 更新 `docs/gearbox-diff-review-2026-07-09.md` 或创建新补丁文件，记录每个完成项

2. 根据本计划内容更新映射文档：
   - `phase-05-completion-parent-wake.md` → P1-2
   - `phase-08-goal-loop-review-budget.md` → P0-1~P0-5, P1-3, P2-1
   - `phase-09-gui-parallel-worker-pool.md` → P1-1, P2-2

3. 全部完成后，重命名 `docs/gearbox-gear-remaining-gap-dogfood-plan.md` 为"最终版：可执行 dogfood 计划"

**ACCEPTANCE**:
- 每个 Phase 工单对应章节都已更新
- 回归命令和完成时间窗口已记录

**QA**: `cargo test -p gearbox_agent -- --nocapture` 全通过

**COMMIT**: `docs: P2-3 map completed gaps to phase documents`

---

## 关键验收清单（每轮都执行）

当前轮次更改的测试全部通过后：

```bash
cargo fmt
cargo test -p gearbox_agent -- --nocapture
```

交付前运行以下场景：

| 场景 | 验证内容 |
|------|---------|
| `max_worker_calls` + `max_iterations` 同时命中 | 两种限制独立生效 |
| `same file different content` 的 `detect_stagnation` | 不误报停滞 |
| `provider_unknown_streak` 3 轮 unknown / false/unknown 混合 | streak 不误重置 |
| `nearest_fallback none` 的 repair case | 终止而非循环 |

## 里程碑

- 每个 P0 项完成后 → 更新 `docs/gearbox-diff-review-2026-07-09.md`
- P1 完成后 → 同步更新 `phase-05/08/09` 工单对应章节
- 全部完成后 → 重命名本文件为"最终版：可执行 dogfood 计划"并启动完整的中等模型闭环跑

---

## Must-Not-Have

- 不要为了"更加正确"而重构未在计划中列出的模块（如 `WorkderRegistry`、`ConcurrencyManager`）
- 不要修改 `DEFAULT_MAX_ITERATIONS`（保留 5）或 `DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK`（保留 2）
- 不要修改上游共享层的代码（`crates/agent/`）除非明确列在改动范围内
- 不要重新排序已经明确的执行优先级（P0→P1→P2）
- 不要修改 `GoalStatus` 枚举变体或 `ManagedTaskStatus` 变体
