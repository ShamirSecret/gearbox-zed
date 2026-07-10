# OMO vs Gear 细粒度对比 - Part 3: Steering Engine

> 文件：`packages/senpi-task/src/steering/` — engine.ts、types.ts

---

## 3.1 "先转换，再操作" — 控制路径的核心原则

**这是 OMO 实现中最值得借鉴的模式。**

### OMO `interruptTask()` — 完整流程

```typescript
// steering/engine.ts:115-135
async function interruptTask(idOrName): Promise<InterruptOutcome> {
    // 1. 查找 task
    const record = resolve(idOrName);
    if (record === undefined) return { kind: "not_found" };

    // 2. 检查状态
    if (record.status !== "running")
        return { kind: "noop", ...reason: `Task is ${record.status}, not running.` };

    // 3. 先转换状态！running → interrupted
    //    注释：Transition BEFORE abort so steering is the SINGLE terminal writer.
    //    abort settles the launch outcome tracker, whose late complete/cancel
    //    transition is then rejected by terminal idempotence.
    const result = port.store.transition(record.task_id, { type: "interrupt", timestamp });

    // 4. 如果转换失败（已被其他路径先转换了）→ 返回 noop
    if (!result.applied) return { kind: "noop", ... };

    // 5. 再 abort handle（此时状态已 terminal，任何后续完成通知都会被守卫拒绝）
    if (handle) await handle.abort();

    // 6. 捕获 partial text（中断时可能已有部分输出）
    const partial = handle?.lastAssistantText();
    if (partial) port.store.replace({ ...result.record, final_response: partial });

    // 7. 记录事件
    port.store.appendEvent(task_id, { type: "interrupted", payload: { previous_status: "running" } });
    return { kind: "interrupted", task_id, previous_status: "running" };
}
```

**关键时序保证：**

```
用户点击 interrupt
  → TRANSITION: running → interrupted（写入 store）
  → confirm transition applied
  → handle.abort()（此时 handle 内的 waitForOutcome() 开始 resolve）
      → .then() 回调读取状态：已经 terminal → {applied: false, late_transition_ignored}
      → 正确忽略
  → capture partial text（中断发生时的内容）
```

### Gear `interrupt_task()` — 没有先转换

```rust
// task_manager.rs:1074-1104
pub fn interrupt_task(&mut self, task_id: &str) -> Result<()> {
    // 1. 先 abort handle
    if let Some(running_task) = self.running_tasks.get(task_id) {
        running_task.handle.interrupt()?;  // 先操作
    }
    // 2. 再设置状态
    if record.status == ManagedTaskStatus::Running {
        record.status = ManagedTaskStatus::Cancelled;  // 后设置
    }
}
```

**竞态场景：**

```
用户点击 interrupt
  → handle.interrupt()
      → worker 在 interrupt 回调中同步完成了
      → background dispatcher 收到 outcome
      → settle_running_task() 设置 status = Failed（因为 dispatcher 看到的是 Completed outcome）
      → 然后 interrupt_task() 把 status 改为 Cancelled
      → 后台 tick 再次消费 → 重复尝试
```

Gear 没有 `interrupted` 状态（只有 `Cancelled`），所以 interrupt 和 cancel 在 Gear 中是同一件事。但即使如此，`Failed` 覆盖 `Cancelled` 的竞态仍然存在。

---

## 3.2 `cancelTask()` — 销毁委托给 Lifecycle

### OMO: 三阶段 cancel

```typescript
// steering/engine.ts:137-170
async function cancelTask(idOrName, reason?): Promise<CancelOutcome> {
    // Phase 1: 转换状态（running → cancelled）
    const result = port.store.transition(record.task_id, { type: "cancel", timestamp, error_message: reason });

    // Phase 2: best-effort abort（已忽略错误）
    if (handle !== undefined) {
        try { await handle.abort(); }
        catch (error) { log("abort rejected", { error }); }  // 错误不阻塞后续
    }

    // Phase 3: 委托给 Lifecycle 销毁（NOT steering 自己销毁）
    await port.destruction.destroyResidentTask(record.task_id, "cancel");
    return { kind: "cancelled", task_id, previous_status: "running" };
}
```

**注释关键：** "The record is already terminal (cancelled) above. abort() is best-effort: an rpc child that already exited rejects the abort send, and a rejection here must NOT skip the destruction that moves the record OUT of resident — otherwise it freezes at {cancelled, resident}, un-evictable, leaking a residency slot forever."

### Gear: 直接操作 handle + 直接写记录

```rust
// task_manager.rs:1025-1072
pub fn cancel_task(&mut self, task_id: &str) -> Result<()> {
    // 如果是 queue 中的 → 直接移除
    if let Some(index) = self.queued_tasks.iter().position(|qt| qt.task.id == task_id) {
        queued_store = Some(self.queued_tasks.remove(index).context("...")?.store);
    }
    // 如果是 running → cancel handle
    if let Some(running_task) = self.running_tasks.get(task_id) {
        running_task.handle.cancel()?;  // 先操作，再转换
    }
    // 写状态
    record.status = ManagedTaskStatus::Cancelled;
}
```

**差异：**
- Gear 不委托销毁，只是写状态和事件
- Gear 不处理 abort 被拒绝的情况（没有 catch）
- Gear 的 `queued remove` 是独立的路径，而 OMO 的 pending 消息是通过 steering 的 `enqueuePending()` 处理的

---

## 3.3 `reviveTerminal()` — 同一个 session 的复活

### OMO

```typescript
// steering/engine.ts:80-88
async function reviveTerminal(record, handle, message): Promise<SendOutcome> {
    await handle.followUp(message);         // 同一个 session，不是新的
    const revived = buildRevived(record, nowIso()); // status=running, run_epoch++
    port.store.replace(revived);            // 替换记录
    port.reacquireForRevive(record.task_id); // 重新 acquire 并发槽
    return { kind: "revived", task_id, run_epoch: revived.notification.run_epoch };
}
```

**`buildRevived()` 的精确行为：**
```typescript
function buildRevived(record, timestamp): TaskRecord {
    const { final_response, error_message, ...rest } = record;  // 清除终端字段
    return {
        ...rest,
        status: "running",
        residency_state: "resident",
        updated_at: timestamp,
        notification: { ...record.notification, run_epoch: record.notification.run_epoch + 1 },
    };
}
```

- 清空 `final_response` 和 `error_message`
- 保留 `notified_epoch` 不变（所以新 epoch 的完成会触发通知）
- `run_epoch++`（所以释放守卫不会与旧 epoch 冲突）

### Gear: 没有 reviveTerminal

Gear 的 `send_follow_up_current_task` 只对 running handle 有效。对于已经 completed 的 task，调用会得到 `Err` 或静默失败。

---

## 3.4 消息队列：pending → drain on start

### OMO

```typescript
// engine.ts:20-24 — pending 消息队列
const pending = new Map<string, QueuedMessage[]>();

// engine.ts:90-96 — 对 pending task 发消息 → 缓冲
function enqueuePending(taskId, message, deliverAs): SendOutcome {
    const queue = pending.get(taskId) ?? [];
    queue.push({ message, deliverAs });
    pending.set(taskId, queue);
    return { kind: "queued", queue_position: queue.length };
}

// engine.ts:98-113 — task 启动后按顺序 drain
async function notifyStarted(taskId): Promise<void> {
    const queue = pending.get(taskId);
    if (!queue?.length) return;
    pending.delete(taskId);
    for (const item of queue) {
        try { /* deliver */ }
        catch (error) { log("delivery failed", { error }); }  // 一个失败不影响其他
    }
}
```

**关键设计：** 按 FIFO 顺序 drain，一个失败不阻塞后续。

### Gear: 没有 pending 消息队列

`send_follow_up_current_task` 在 task 没启动时（pending 状态）直接返回错误。用户需要等到 task running 后才能发送 follow-up。

---

## 3.5 `scopeDenied()` — 跨会话访问控制

### OMO

```typescript
function scopeDenied(record, input): SendOutcome | undefined {
    if (input.callerSessionId === undefined || input.allScope === true) return;
    if (caller === record.parent_session_id || caller === record.root_session_id) return;
    return { kind: "scope_denied", ... };
}
```

阻止 task 被不是其 parent/root session 的调用者控制。

### Gear: 无会话范围检查

任何 `TaskManagerControl` 持有者都可以 cancel/interrupt/steer/followUp 任何 task，没有 parent/root session 检查。

---

## 3.6 错误返回类型 vs panic

### OMO 所有控制路径都返回类型化的结果：

```typescript
// SendOutcome 有 6 种变体
{ kind: "steered" | "revived" | "queued" | "not_continuable" | "scope_denied" | "not_found" }

// InterruptOutcome 有 3 种变体
{ kind: "interrupted" | "noop" | "not_found" }

// CancelOutcome 有 3 种变体
{ kind: "cancelled" | "noop" | "not_found" }
```

### Gear 返回 `Result<bool>` 或 `Result<()>`：

```rust
pub fn interrupt_task(&self, task_id: &str) -> Result<bool>
```

- `true` = 中断成功
- `false` = task 不处于 running 状态（实际上 `bail!` 返回 Err，不是 false）
- `Err` = handle 不存在或其他错误

**差异：** OMO 明确区分 `noop`（状态不符合）和 `not_found`（task 不存在），Gear 的 `Result<bool>` 无法表达这些区别。

---

## 3.7 Gear 修复清单

| # | 缺失项 | 影响 | 修复方案 |
|---|--------|------|---------|
| 1 | **先转换再 abort** | 后台 completion 覆盖 cancel/interrupt | `interrupt_task()` 和 `cancel_task()` 第一件事做 `store.transition()` |
| 2 | **中断时捕获 `lastAssistantText`** | 中断后用户看不到部分进度 | `handle.last_output()` 在 interrupt 后 capture |
| 3 | **`reviveTerminal`** | completed 的 task 不能继续 | 新增 `send_task()`：`running→followUp`，`completed/resident→revive` |
| 4 | **`notifyStarted()` drain** | pending 期间的 message 丢失 | 新增消息队列，worker start 后按序投递 |
| 5 | **`scopeDenied()`** | 无控制路径的会话隔离 | 新增 `caller_session_id` 参数和范围检查 |
| 6 | **中断/取消的 `noop` 区分** | Gear 无法区分 task 不存在 vs 状态不符合 | 改用枚举返回类型而非 `Result<bool>` |

---

## 3.8 已补完成项（P2-2）

### P2-2：Worker stream 深度（tool-call delta） ✅

| 项目 | 内容 |
|------|------|
| **问题** | `execute_command_with_prompt` 只输出 TurnStarted/Stdout/Stderr/TurnFinished/Error，无 tool call 粒度事件，`transcript.jsonl` 和 `tool-events.jsonl` 缺少细粒度数据 |
| **改动** | `crates/gearbox_agent/src/workers.rs` — 新增 `parse_and_emit_tool_events()` 方法，扫描 stdout 中 XML tool call 模式（`<function_calls>`、`<tool_use>`、`<invoke>`）并 emit `AssistantTextDelta`/`ToolCallStarted`/`ToolCallFinished` 到 transcript 和 tool-events |
| **改动** | `crates/gearbox_agent/src/task_manager.rs` — 新增 `TranscriptEntry` 枚举（`Parsed`/`Raw`）、`TaskRecord::transcript_entries()` 方法 |
| **改动** | `crates/gearbox_agent/src/runtime.rs` — `collect_context_risk_texts()` 增加 tool-events 事件序列文本 |
| **测试** | `worker_transcript_includes_tool_call_deltas` — 验证 transcript 含 `tool_call_started`/`finished`/`assistant_text_delta`、tool-events 含 start/finish、subscription 收到 ≥1 ToolCallStarted |
| **文件** | `crates/gearbox_agent/src/workers.rs`, `crates/gearbox_agent/src/task_manager.rs`, `crates/gearbox_agent/src/runtime.rs` |
| **commit** | `c99b6572dc` (部分) |
