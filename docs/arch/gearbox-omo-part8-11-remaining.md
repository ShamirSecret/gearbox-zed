# OMO vs Gear 细粒度对比 - Parts 8-11: 剩余模块

> 文件：fallback-retry-handler.ts、stop-continuation-guard/hook.ts

---

## Part 8: Fallback Retry Handler

**文件：** `packages/omo-opencode/src/features/background-agent/fallback-retry-handler.ts`

### 关键模式：no-op fallback 检测

```typescript
// fallback-retry-handler.ts:114-125
const isNoOpFallback =
    candidateProviderID.toLowerCase() === task.model.providerID.toLowerCase() &&
    canonicalizeModelID(candidateModelID) === canonicalizeModelID(task.model.modelID);
```

当 fallback 候选与当前 model 相同时跳过。`canonicalizeModelID` 将 `.` 替换为 `-` 并转小写，所以 `gpt-4.1` 和 `gpt-4-1` 被视为同一模型。

**Gear 现状：** `WorkerSequence` 不会检测 no-op route（同一个 worker_kind 的连续 route 不会自动跳过）。

### 关键模式：team-mode guard

```typescript
// fallback-retry-handler.ts:198-206
if (task.teamRunId && !task.onSessionCreated) {
    throw new TeamModeFallbackError(...);
}
```

**Gear 相关性：** 无（Gear 没有 team mode）。但 principle 可借鉴——在关键状态不完整时抛出结构化错误，而不是静默进入不可恢复状态。

---

## Part 9: Stop-Continuation-Guard

**文件：** `packages/omo-opencode/src/hooks/stop-continuation-guard/hook.ts`

### 关键模式：级联取消 + skipNotification

```typescript
// hook.ts:44-69
const cancellableTasks = backgroundManager
    .getAllDescendantTasks(sessionID)    // 递归找到所有子/孙 task
    .filter(t => t.status === "running" || t.status === "pending");

void Promise.allSettled(
    cancellableTasks.map(task => backgroundManager.cancelTask(task.id, {
        source: "stop-continuation",
        skipNotification: true,  // 防止通知风暴
        abortSession: task.status === "running",
    }))
);
```

**关键设计：**
- `getAllDescendantTasks()` — 递归遍历所有层级
- `skipNotification: true` — 父 session 不需要为每个子 task 收到独立通知
- `abortSession: task.status === "running"` — 只有 running 的 task 需要 abort session；pending 的只需从 queue 移除

**Gear 现状：** `cancel()` 只取消 `current_task`。没有递归后代查找，没有 `skipNotification` 概念。

---

## Parts 10-11: Delegate-Core + Config Schema

这两个模块对 Gear 的直接影响较低：

| 模块 | 内容 | Gear 相关性 |
|------|------|------------|
| `delegate-core/model-selection.ts` | 多层模型回退解析算法 | 低 — Gear 的 model selection 在 coordinator_brief 层 |
| `delegate-core/retry-patterns.ts` | 错误模式检测（run_in_background 缺失、unknown category 等） | 低 — Gear 不通过 opencode tool call 调用 |
| `omo-config-core/schema/task.ts` | Task settings schema（concurrency、depth、residency、TTL） | 中 — Gear 通过 `WorkerConfig` 实现相似配置 |
| `omo-config-core/schema/category.ts` | Category 配置 schema | 低 — Gear 的 CategoryRouter 有内置 policy |

**唯一值得借鉴的细节：** `omo-config-core/schema/task.ts` 中的 `residency_max_children`（默认 8）、`ttl_ms`（默认 24h）、`default_concurrency`（默认 5）等默认值可以作为 Gear 未来配置的参考基线。

---

## Gaps 完成状态

以下 P0/P1/P2 items 已在此文档发表后实现：

### P0-1：`max_worker_calls` 使用 `Goal.budget` 配置 ✅

| 项目 | 内容 |
|------|------|
| **改动** | `runtime.rs` `BudgetController` 优先读 `goal.budget.max_worker_calls`，fallback 到全局默认 |
| **测试** | Goal 设置 `budget.max_worker_calls=1` 时首个循环两次 worker attempts 后到 `limited` |
| **文件** | `crates/gearbox_agent/src/runtime.rs` |

### P0-2：`worker_call_count` 统计口径修复 ✅

| 项目 | 内容 |
|------|------|
| **改动** | 每轮 `GoalLoop` 只增量 1 次 worker-call；新增 `attempt_count` 独立追踪所有 attempt 总数（含重试） |
| **测试** | one-iteration 多 attempts 场景 worker_call_count 只加 1 |
| **文件** | `crates/gearbox_agent/src/runtime.rs` |

### P0-3：`provider_unknown_streak` 重置逻辑修复 ✅

| 项目 | 内容 |
|------|------|
| **改动** | 三分支逻辑：goal_verified 或 concrete stop_reason 时重置；unknown 条件时 +1；其他情况（含 goal_satisfied==Some(false)）保持不变 |
| **测试** | `provider_unknown_streak_not_reset_on_false_goal_satisfied` 覆盖 5 种场景 |
| **文件** | `crates/gearbox_agent/src/runtime.rs` |

### P0-4：`detect_stagnation` diff 比较增强 ✅

| 项目 | 内容 |
|------|------|
| **改动** | `DiffSnapshot` 增加 `diff_hash` 字段；`detect_stagnation` 比较 diff_hash 而非仅文件列表 |
| **测试** | 同一文件名不同内容改动不再误判 stagnation |
| **文件** | `crates/gearbox_agent/src/runtime.rs`, `crates/gearbox_agent/src/tools.rs` |

### P0-5：`GoalDecisionPolicy` 无 fallback 处理收口 ✅

| 项目 | 内容 |
|------|------|
| **改动** | `CategoryResolutionResult::nearest_fallback()` helper；`GoalDecisionPolicy` 新增 `nearest_fallback_available` 字段；无 fallback + 无进展 + iteration>1 时返回 `Limited` |
| **测试** | `evaluation_limits_when_no_fallback_available`, `evaluation_continues_on_first_iteration_when_no_fallback` |
| **文件** | `crates/gearbox_agent/src/runtime.rs`, `crates/gearbox_agent/src/workers.rs` |

### P1-3：provider-aware/depth 统一预算策略 ✅

| 项目 | 内容 |
|------|------|
| **改动** | 新增 `RouteChangeType` enum、`BudgetController::apply_budget_for_route_change()`、`evaluate_goal_with_source()` 重载；`budget_guard_reason()` 输出附加触发源标记 |
| **测试** | 3 项新增测试覆盖触发源标记、预算区分、budget_summary 一致性 |
| **文件** | `crates/gearbox_agent/src/runtime.rs` |

### P2-1：停滞信号来源增强 ✅

| 项目 | 内容 |
|------|------|
| **改动** | 新增 `normalize_repair()` 辅助函数；`detect_stagnation` 中 repair_requests 和 worker_outputs 归一化比较 |
| **测试** | `stagnation_normalizes_repair_variations` |
| **文件** | `crates/gearbox_agent/src/runtime.rs` |
