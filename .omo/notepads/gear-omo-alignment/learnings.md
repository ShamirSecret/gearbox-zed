# Gear ↔ Omo 功能对齐 - 学习笔记

## 项目约定
- 工作目录: /home/donald/文档/github/zed
- 目标 crate: crates/gearbox_agent
- 测试命令: cargo test -p gearbox_agent -- --nocapture
- 格式化: cargo fmt

## 关键文件
- crates/gearbox_agent/src/runtime.rs — 主运行时逻辑
- crates/gearbox_agent/src/workers.rs — worker 路由和解析
- crates/gearbox_agent/src/task_manager.rs — 任务管理
- crates/gearbox_agent/src/tools.rs — 工具/DiffSnapshot

## 已发现模式
- BudgetController 在 runtime.rs 中构造
- worker_call_count 在循环中累加（每轮迭代 +1，不再按 attempt 数累加）
- attempt_count 新增字段，独立追踪所有 attempt 总数（含重试）
- provider_unknown_streak 在 verification 后更新
- detect_stagnation 比较 diff_history
- GoalDecisionPolicy::evaluate() 做最终决策
- budget_guard_reason() 限流判断仍使用 worker_call_count，不受 attempt_count 影响
- budget_summary() 输出格式新增 attempts=N 指标
- BudgetSnapshot 派生 Default，新增字段不影响使用 ..Default 的测试构造

## P0-3: provider_unknown_streak 重置逻辑修复

### 问题
- runtime.rs 中 provider_unknown_streak 的更新逻辑用 if/else 二分：满足"unknown"条件则 +1，否则 reset 为 0
- 当 verification_passed=true 且 goal_satisfied=Some(false) 时，不满足 unknown 条件（goal_satisfied 不是 None），落入 else 分支被重置为 0
- 这导致 unknown 无法正确累计，因为 review 确认"目标未满足"时 streak 被错误清零

### 修复
- 提取 `update_provider_unknown_streak()` 辅助函数，三分支逻辑：
  1. goal_verified (verification_passed && goal_satisfied==Some(true)) 或有 concrete stop_reason → reset 为 0
  2. unknown 条件 (verification_passed && goal_satisfied.is_none() && no stop_reason) → +1
  3. 其他情况（包括 goal_satisfied==Some(false)）→ 保持不变
- 在 product.rs 中修复了 pre-existing 的 DiffSnapshot 初始化缺少 diff_hash 字段的编译错误

### 测试
- 新增 `provider_unknown_streak_not_reset_on_false_goal_satisfied` 覆盖 5 种场景
- 现有 `evaluation_honors_provider_unknown_streak_budget_limit` 仍然通过
- 全部 153 个测试通过

## 当前状态（2026-07-09）
- P0-1 至 P0-4 已完成（代码 + 测试全部就位，153 项测试通过）
- P0-5、P1-3、P2-1 已分派并行执行
- P0-5 需要在 GoalDecisionPolicy::evaluate() 中新增 nearest_fallback_available 检查分支
- P2-1 需要在 detect_stagnation 中对 repair_requests 和 worker_outputs 做归一化比较

## P1-3: 统一预算入口 apply_budget_for_route_change() ✅ DONE

### 设计决策
- 新增 `RouteChangeType` 枚举：`RouteChange` / `Fallback` / `ReviewTrigger`
- 添加 `trigger_source` 字段到 `GoalDecisionPolicy`，让 `budget_guard_reason()` 输出包含触发源标记
- `apply_budget_for_route_change()` 作为 `BudgetController` 的方法
- 不修改 `evaluate_goal()` 的签名以避免变更所有测试调用；使用带额外参数的 `evaluate_goal_with_source()` 重载
- `budget_guard_reason()` 对 `worker_calls` 和 `premium_worker_calls` 检查附加触发源标记（如 `(route change)`、`(fallback)`、`(review)`）

### 实现文件
- `crates/gearbox_agent/src/runtime.rs` — 全部修改在此文件内完成

### 新增类型和方法
- `RouteChangeType` enum: `RouteChange`, `Fallback`, `ReviewTrigger` (+ `label()` method)
- `BudgetController::apply_budget_for_route_change()`: 检查 `worker_calls`, `premium_worker_calls`, `runtime_minutes` 预算限制，返回 `Result<(), String>`，错误消息包含触发源标记
- `GoalDecisionPolicy.trigger_source: Option<RouteChangeType>`: 新增字段
- `budget_guard_reason()`: 对 `worker_calls` 和 `premium_worker_calls` 检查附加 `(route change)`/`(fallback)`/`(review)` 后缀
- `evaluate_goal_with_source()`: 新函数，接受 `trigger_source` 参数；`evaluate_goal()` 包装调用，默认 `None`

### run() 循环集成
- 每次迭代开始（route 选择后）：通过 `worker_route_hint` 和 `route_reason` 确定 `current_route_change_type`
- 计数递增前（line ~509）：调用 `budget_controller.apply_budget_for_route_change()` 做预检查
- `evaluate_goal` 调用（line ~811）：通过 `evaluate_goal_with_source` 传入 `Some(current_route_change_type)`

### 测试
- `budget_guard_reason_includes_trigger_source_label`: 验证三种触发源的 budget_guard_reason 输出都包含对应标记
- `apply_budget_for_route_change_distinguishes_triggers`: 验证 BudgetController 方法为不同触发源返回不同错误消息
- `budget_summary_matches_across_coordinator_review_and_goal_review`: 验证 budget_summary 字符串在 goal_review_artifact 中一致嵌入
- 全部 159 项测试通过（新增 3 项，原 156 项）

### 注意
- `evaluate_goal` 函数保留作为测试兼容包装，lib 编译时出现 `dead_code` 警告，但测试编译时正常使用

### P0-5 — GoalDecisionPolicy halts loop when no fallback route

**Changes:**
- `crates/gearbox_agent/src/workers.rs`: Added `CategoryResolutionResult::nearest_fallback()` helper method (returns `Option<&FallbackRoute>`)
- `crates/gearbox_agent/src/runtime.rs`:
  - Added `nearest_fallback_available: bool` field to `GoalDecisionPolicy`
  - Added parameter to `evaluate_goal()` (passed through to `GoalDecisionPolicy`)
  - In `run()` loop: compute `has_fallback = category_resolution_result.nearest_fallback().is_some()` and pass to `evaluate_goal()`
  - In `GoalDecisionPolicy::evaluate()`: added no-fallback check at the default fallthrough (before "another repair iteration" continuation):
    - fires when `!verification_passed && !nearest_fallback_available && no_progress_signals.is_empty() && iteration > 1`
    - returns `Limited` instead of `Running`
  - `iteration > 1` guard ensures first attempt always gets a chance to run

**Tests:**
- `evaluation_limits_when_no_fallback_available` — iteration=2, no fallback, no progress → Limited
- `evaluation_continues_on_first_iteration_when_no_fallback` — iteration=1, no fallback → Running (first chance)

**Key decision:** Check placed at default fallthrough (not inside `!verification_passed` block) to avoid intercepting `stop_reason` and `require_worker` handling. `iteration > 1` prevents premature halting on first attempt.

### P2-1: Normalize repair request and worker output text for stagnation detection dedup
**Status:** ✅ Completed

**Changes in `crates/gearbox_agent/src/runtime.rs`:**

1. **New helper:** `normalize_repair(text: &str) -> String`
   - Lowercases text and collapses all whitespace to single spaces
   - Used for comparing both repair_requests and worker_outputs in detect_stagnation

2. **detect_stagnation() modifications:**
   - `repair_requests` comparison changed from exact equality to normalized comparison
   - `worker_outputs` comparison changed from exact equality to normalized comparison
   - `diff_hash` and `verification_history` comparisons left unchanged

3. **New test:** `stagnation_normalizes_repair_variations()`
   - Case/whitespace variants of same repair request → stagnation triggered
   - Case/whitespace variants of same worker output → stagnation triggered
   - Semantically different content → no false positive

**Side fix:** Added missing `!nearest_fallback_available` guard to `GoalDecisionPolicy::evaluate()` (inside `!verification_passed` block, before continuation_guard), completing the no-fallback-available feature that was pending.

## P1-2: Serialize parent completion notification flush with ordered retry

### Changes
- **`CompletionNotifier` struct:** Added two new fields:
  - `flush_serializer: Arc<Mutex<HashMap<String, bool>>>` — per-session lock: marks whether a flush is in progress for a parent session
  - `pending_flush: Arc<Mutex<HashMap<String, VecDeque<()>>>>` — per-session queue of pending flush signals for callers that arrived while a flush was running

- **`flush_buffer()` signature:** Added `read_record: &dyn Fn(&str) -> Result<Option<TaskRecord>>` parameter for state re-verification before delivery.

- **`flush_buffer()` logic:**
  1. **Serialization lock** on entry: if another flush is in progress for same session, push `()` onto `pending_flush` queue and return early
  2. **Debounce** retained (100ms per-session cooldown)
  3. **State re-verification** before each delivery: calls `read_record(task_id)`, checks `record.run_epoch == notification.run_epoch && is_notifiable_status(record.status)`. If stale, returns `NotificationResult::Skipped`
  4. **Pending queue drain** after each complete flush: atomically releases serializer, checks for queued signals, re-acquires if pending → loop

- **`CompletionNotificationFlushGuard::drop`** in runtime.rs updated to pass a `read_record` closure that reads from `store.worker_dir(task_id)/task-record.json`

### Tests added (2 new, 169 total)
- `completion_flush_serializes_rapid_arrivals`: manually locks serializer, verifies queuing, then releases and verifies all 3 notifications delivered in order
- `completion_flush_works_after_idle_transition`: buffers during Streaming, verifies no-op at non-Idle flush, flushes at Idle, verifies dedup on re-flush, THEN tests state re-verification by providing a `read_record` with bumped epoch (2 vs notification's 1) → verifies `Skipped`

### Key design decisions
- `VecDeque<()>` used for pending signals (no data needed — flush drains the shared buffer, so any signal is sufficient)
- Serializer is a `HashMap<String, bool>` not a `tokio::sync::Mutex` or RWLock — std `Mutex` is fine since flush_buffer is called from sync contexts (Drop impl, tests)
- State re-verification only applies to the flush path (not immediate `try_notify`) — immediate delivery at Idle doesn't need it because the record is fresh
- `read_record` returns `Result<Option<TaskRecord>>` — `Ok(None)` means record not found (still deliver), `Err` means storage error (still deliver to avoid dropping notifications on transient errors)

## P2-2: Granular worker stream with tool-call deltas ✅

### Problem
`WorkerEvent` already defined `ToolCallStarted`, `ToolCallFinished`, `AssistantTextDelta` variants, but `execute_command_with_prompt` only emitted `TurnStarted`, `WorkerStdout`, `WorkerStderr`, `TurnFinished`, and `Error`. No tool call granularity reached `transcript.jsonl` or `tool-events.jsonl`.

### Changes in `crates/gearbox_agent/src/workers.rs`

1. **`parse_and_emit_tool_events()`** — new method on `CommandWorkerSessionHandle` that scans stdout for XML tool call patterns (`<function_calls>`, `<tool_use>`, `<invoke name="...">`, `<parameter name="...">`) and emits `AssistantTextDelta`, `ToolCallStarted`, `ToolCallFinished` events into both `transcript.jsonl` and `tool-events.jsonl`.

2. **Helper functions**: `find_subsequence()` (generic byte-sequence find), `extract_xml_attr()` (parse `name="value"` from XML), `extract_invoke_arguments()` (parse parameter key=value pairs from `<parameter>` tags).

3. **Integration**: `parse_and_emit_tool_events` called in `execute_command_with_prompt()` right after stdout capture but before `WorkerStdout` emission, so deltas appear in logical order in the transcript.

### Changes in `crates/gearbox_agent/src/task_manager.rs`

4. **`TranscriptEntry` type** — untagged enum (`Parsed` with common fields or `Raw` JSON value) for typed access to transcript lines.
5. **`TaskRecord::transcript_entries()`** — method reading and parsing `transcript.jsonl` from the artifact directory, using `result_path.parent()` to locate it.

### Changes in `crates/gearbox_agent/src/runtime.rs`

6. **`collect_context_risk_texts()`** — now includes a `"tool-events event sequence: start -> finish -> ..."` text entry that lists event names in order, making tool call chain patterns available to risk detection without parsing JSONL.

### New test
7. `worker_transcript_includes_tool_call_deltas` — worker command outputs text with XML tool call patterns; verifies `transcript.jsonl` contains `tool_call_started`, `tool_call_finished`, `assistant_text_delta`; `tool-events.jsonl` contains start/finish; subscription receives exactly 1 `ToolCallStarted` and ≥2 `AssistantTextDelta` events.

### Test results
- 169 tests pass (all existing + 1 new)

---

## Documentation mapping (P2-3)

After all P0/P1/P2 implementation items were completed, the following documentation was generated/updated to record what was done and link back to the phase documents:

### Files modified

| File | Change | Links to |
|------|--------|----------|
| `docs/gearbox-diff-review-2026-07-09.md` | Added Round 2 (P1) and Round 3 (P2) sections with item details, files, test counts, commit stubs | All items |
| `docs/gearbox-omo-part5-lifecycle.md` | Added sec 5.6 with P0-5 (no-fallback halt) and P1-2 (completion flush) completion notes | P0-5, P1-2 |
| `docs/gearbox-omo-part8-11-remaining.md` | Added Gaps completion status section with P0-1~P0-5, P1-3, P2-1 details | P0-1~P0-5, P1-3, P2-1 |
| `docs/gearbox-omo-part2-task-manager.md` | Added sec 2.8 with P1-1 (typed outcomes) completion notes | P1-1 |
| `docs/gearbox-omo-part3-steering.md` | Added sec 3.8 with P2-2 (tool-call delta) completion notes | P2-2 |
| `docs/gearbox-gear-remaining-gap-dogfood-plan.md` | Added sec 5 with completion status table, regression command (169 tests), remaining gap inventory, and doc mapping | All items + gaps |
| `.omo/notepads/gear-omo-alignment/learnings.md` | This section — documentation mapping record | P2-3 |

### Regression

```
cargo test -p gearbox_agent  →  169 passed, 0 failed
```

### Test growth

```
基线 (initial P0):  153 tests
P0 轮 (+5 tests):   158 tests
P1 轮 (+7 tests):   165 tests
P2 轮 (+4 tests):   169 tests
```

### Remaining gaps (not in scope)

| # | Gap | Source document |
|---|-----|-----------------|
| 1 | 单一销毁端口 | part5-lifecycle.md |
| 2 | 启动 reconciliation | part5-lifecycle.md |
| 3 | LRU eviction | part5-lifecycle.md |
| 4 | TTL 清理 | part5-lifecycle.md |
| 5 | `lost` 记录保护 | part5-lifecycle.md |
| 6 | `reviveTerminal` | part3-steering.md |
| 7 | `notifyStarted()` drain | part3-steering.md |
| 8 | `scopeDenied()` | part3-steering.md |
| 9 | 中断时捕获 lastAssistantText | part3-steering.md |
