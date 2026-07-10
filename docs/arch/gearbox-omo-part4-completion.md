# OMO vs Gear 细粒度对比 - Part 4: Completion Notification

> 文件：`packages/senpi-task/src/completion/` — notifier.ts、notification.ts、routing.ts、types.ts

---

## 4.1 通知触发条件

### OMO `shouldNotifyStatus()`（routing.ts:4-10）

```typescript
const notifyingStatuses = new Set(["completed", "error", "lost"]);
// interrupted 和 cancelled 不在其中——它们是同步的，不触发通知
```

**设计理由（注释原文）：** "Only externally-caused terminals (completed/error/lost) notify. Parent-initiated cancel/interrupt return synchronously in the tool result, so they must never push a completion notification."

即：用户点的 cancel/interrupt，按钮回调里已经返回结果了。不需要额外通知。

### Gear: 所有 terminal 事件都发

```rust
// runtime.rs — event_sink 发送所有 EventKind::* 事件
// 包括 WorkerCompleted、WorkerFailed、WorkerCancelled
```

**影响：** 用户点 cancel 后会看到 "Gear: task_004 cancelled" 的消息，即使取消是用户自己触发的。

---

## 4.2 通知投递的五层守卫

### OMO `notifyTerminal()`（notifier.ts:32-56）

```
第一层：同步 task 不通知
  !request.runInBackground → {kind: "skipped", reason: "sync-task"}

第二层：非终端状态不通知
  !TERMINAL_STATUSES.has(status) → {kind: "skipped", reason: "not-terminal"}

第三层：cancelled/interrupted 不通知
  !shouldNotifyStatus(status) → {kind: "skipped", reason: "non-notifying-terminal"}

第四层：已通知过的 epoch 不重复通知
  notified_epoch >= run_epoch → {kind: "skipped", reason: "already-notified"}

第五层：父会话状态决定是否缓冲
  routeCompletion(parentState) === buffer → {kind: "buffered"}
```

### Gear: 无守卫

```rust
// event_sink(event) 在 Orchestrator::run() 中直接调用
// 没有状态检查、没有 epoch 去重、没有 parent 状态检查
```

---

## 4.3 缓冲与刷新

### OMO `flushBuffered()`（notifier.ts:58-79）

```typescript
// 当父会话被替换（新 session_id）→ 丢弃所有缓冲通知
if (input.replaced) {
    for (const entry of entries) dropEntry(store, entry);
    return { kind: "dropped", count: entries.length };
}
// 否则批量投递
const message = buildCompletionMessage(entries.map(e => e.details));
message.triggerTurn = true;
```

**`dropEntry()` 效果：** 写 `notification_dropped` 事件 + log，但不尝试通知。

### Gear: 无缓冲

事件直接发送到 `async_channel`，没有缓冲、去抖、投递确认。

---

## 4.4 通知消息的去重策略

### OMO 双层去重

**第一层 — Epoch 守卫（持久化）：**
```typescript
// notifier.ts:39
if (record.notification.notified_epoch >= epoch) return { kind: "skipped" };
```

**第二层 — 运行时去重（内存）：**
```typescript
// notifier.ts:129-133
if (existing.some(b => b.task_id === entry.task_id && b.epoch === entry.epoch)) return;
```

**第三层 — 投递后加载最新记录：**
```typescript
// notifier.ts:146-147
function persistEntry(store, entry) {
    const fresh = store.load(entry.task_id);   // 重新加载最新记录
    if (fresh !== null) persistNotified(store, fresh, entry.epoch);
}
```

**设计理由：** 缓冲条目在 flush 之前不是持久的。如果 `notifyTerminal` 在 flush 之前被调用两次，epoch 守卫不会阻止（因为没有持久化 notified_epoch）。运行时去重填补了这个间隙。

### Gear: 无去重

`event_sink` 直接在 `append_event` 中触发，没有去重。

---

## 4.5 ParentState 路由决策

### OMO `routeCompletion()`（routing.ts:15-30）

| 父会话状态 | 决策 | 含义 |
|-----------|------|------|
| `idle` | `wake` | 唤醒父会话，触发新 turn |
| `streaming` | `deliver_streaming` | 嵌入到当前 streaming turn 的下一个 tool-call boundary |
| `compacting` | `buffer` | 等压缩完成后发送 |
| `session_switching` | `buffer` | 等会话切换完成后 |
| `session_shutdown` | `buffer` | 等新会话创建后 |

### Gear: 无路由

消息直接写入 markdown stream，无论父会话状态。

**影响：** 如果用户正在与 Agent 对话（streaming 中），Gear worker 完成消息会直接插入到用户正在看的流式回复中间。

---

## 4.6 `deliverWithRetry()` — 无退避的双重重试

```typescript
// notifier.ts:104-111
function deliverWithRetry(notifier, message) {
    const first = tryEnqueue(notifier, message);
    if (first.ok) return first;
    return tryEnqueue(notifier, message);  // 完全相同的调用，无延迟
}
```

**设计疑问：** 如果第一次 `enqueue` 同步抛异常，第二次完全相同调用成功可能性很低。注释没有解释原因。Gear 应该做得更好（指数退避 + 记录失败原因）。

---

## 4.7 `buildCompletionDetails()` — 通知内容

```typescript
// notification.ts:11-22
function buildCompletionDetails(record, options) {
    return {
        task_id: record.task_id,
        name: record.name ?? record.task_id,
        status: record.status,
        duration_ms: durationMs(record),
        final_response_head: (record.final_response ?? record.error_message ?? "").slice(0, 700),
        continuation_hint: continuationHint(record),
    };
}
```

**`continuationHint()` 的输出：**
- `not-continuable` → `"Use task_output({ task_id: ... }) to read the full result"`
- `revive` 或 `steer` → `"Use task_send({ task_id: ..., message: ... }) to continue, or task_output() ..."`

---

## 4.8 Gear 修复清单

| # | 缺失 | 影响 | 建议 |
|---|------|------|------|
| 1 | `cancelled/interrupted` 不通知 | 用户点 cancel 后看到重复消息 | event_sink 过滤 `EventKind::WorkerCancelled` |
| 2 | epoch 去重 | 同 worker 多轮完成导致重复事件 | 引入 `run_epoch` / `notified_epoch` |
| 3 | 父会话状态路由 | Gear 完成通知打断 Agent streaming | 检查 Agent panel 是否在 active streaming |
| 4 | 缓冲刷新 | 无 | 引入 debounce 合并连续完成事件 |
| 5 | `deliverWithRetry` | 无重试 | 加指数退避重试 |
