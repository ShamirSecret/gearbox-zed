# OMO vs Gear 细粒度对比 - Part 6: Runners (ChildHandle)

> 文件：`packages/senpi-task/src/runners/` — in-process/child-handle.ts、in-process.ts、types.ts

---

## 6.1 接口对比

### OMO `ChildHandle`

```typescript
interface ChildHandle {
    task_id: string
    sessionId: string
    steer(text): Promise<void>
    followUp(text): Promise<void>
    abort(): Promise<void>
    subscribe(listener): () => void          // ← Gear 没有
    waitForIdle(): Promise<RunnerOutcome>
    lastAssistantText(): string | undefined
    dispose(): void                          // ← Gear 没有
}
```

### Gear `WorkerSessionHandle`

```rust
trait WorkerSessionHandle: Send + Sync {
    fn session_id(&self) -> Option<String>;
    fn send_follow_up(&self, prompt: String) -> Result<()>;
    fn steer(&self, prompt: String) -> Result<()>;
    fn interrupt(&self) -> Result<()>;
    fn cancel(&self) -> Result<()>;
    fn wait_for_outcome(&self) -> Result<WorkerOutcome>;
    fn wait_for_result(&self) -> Result<WorkerResult>;
    fn last_output(&self) -> Option<String>;
    // 没有 subscribe, dispose, abort
}
```

### 差异

| 方法 | OMO | Gear | 说明 |
|------|-----|------|------|
| `subscribe()` | ✅ 事件流订阅 | ❌ 无 | 无法监听 worker 中间事件 |
| `dispose()` | ✅ 显式释放 | ❌ 隐式 drop | Gear 不能控制释放时机 |
| `abort()` | ✅ 独立于 `cancel()` | ❌ 只有 `cancel()` | OMO 的 `abort()` 不等 outcome，`cancel()` 可能包含 destruction |
| `interrupt()` | ❌ 在 steering 层实现 | ✅ 在 handle 上 | Gear 把 interrupt 直接暴露在 handle 上 |
| `wait_for_result()` | ❌ 无 | ✅ 特有 | Gear 有单独的 result 和 outcome |
| `Send + Sync` | ❌ 无（JS 单线程） | ✅ 必须 | Rust 的并发安全需求 |

---

## 6.2 `runTurn()` — 精确的 abort 检查时机

### OMO

```typescript
async function runTurn(session, text, isAborted): Promise<RunnerOutcome> {
    try {
        await session.prompt(text);
    } catch (error) {
        if (isAborted()) return { status: "cancelled" };  // 检查 1：异常后的 abort 检查
        return { status: "error", failure: { kind: "child-prompt-failed" } };
    }
    if (isAborted()) return { status: "cancelled" };       // 检查 2：成功后的 abort 检查
    return { status: "completed", finalResponse: session.getLastAssistantText() ?? "" };
}
```

**aborted flag 的两次检查：**
1. `catch` 之后：如果 prompt 抛出异常且被 abort，返回 `cancelled` 而非 `error`
2. `try` 之后：如果 prompt 成功但随后被 abort（race condition），返回 `cancelled` 而非 `completed`

**Gear 现状：** `WorkerSessionHandle` 的 `wait_for_outcome()` 不捕获 abort 后的部分输出。如果 worker 被 interrupt，`wait_for_outcome()` 返回的 `WorkerOutcome` 中没有 `lastAssistantText()`。

---

## 6.3 `turnActive` / `beginTurn` 生命周期

### OMO

```typescript
// child-handle.ts:74-90
let turnActive = false;
let running: Promise<RunnerOutcome>;

const beginTurn = (text) => {
    turnActive = true;
    running = runTurn(session, text, () => aborted);
    void running.then(() => turnActive = false, () => turnActive = false);
};

beginTurn(input.promptText);  // 构造时立即开始第一轮

// waitForIdle() 总是返回当前 running promise
// 如果 revive，beginTurn(text) 创建新的 running promise，waitForIdle() 自动指向新的一轮
```

**关键设计：** `waitForIdle()` 返回变量引用（`running`），当 `beginTurn` 替换 `running` 为新 promise 后，后续的 `waitForIdle()` 调用者会等待新的一轮。**这是 revive 能工作的基础。**

### Gear

```rust
// WorkerSessionHandle.wait_for_outcome() 是一个 blocking call
// 一旦 resolve，handle 就完成了。没有"等待新一轮"的概念
fn wait_for_outcome(&self) -> Result<WorkerOutcome>;
fn wait_for_result(&self) -> Result<WorkerResult>;
```

**Gear 的问题：** `wait_for_outcome()` 和 `wait_for_result()` 是两个独立的 blocking call。在背景线程中 `dispatch_running_task` 同时调用了两个：

```rust
// task_manager.rs:1342-1345
let outcome = running_task.handle.wait_for_outcome()?;
let result = running_task.handle.wait_for_result()?;
```

这两个方法都要网络/进程等待。不能实现 OMO 的 `running` promise 替换模式。

---

## 6.4 `followUp()` 的 revive 路径

### OMO

```typescript
// child-handle.ts:98-106
followUp: async (text) => {
    if (turnActive) {
        await session.followUp(text);  // 运行中 → 排队进 session
        return;
    }
    beginTurn(text);                   // idle → 复活，新 turn
},
```

### Gear

```rust
// workers.rs:491
fn send_follow_up(&self, prompt: String) -> Result<()>;
```

对 running task 的 follow-up 是直接传递到 handle。对 settled task 的 follow-up 需要 steering 层的 `messageability()` 检查，但 Gear 没有 revive 路径——settled 状态的 task 无法继续。

---

## 6.5 `subscribe()` — 事件流

### OMO

```typescript
// child-handle.ts:111
subscribe: (listener) => session.subscribe(listener),
// 返回 unsubscribe 函数
```

通过 `session.subscribe(listener)` 可以监听 child session 的所有事件（tool calls、assistant text、error 等）。`TaskManager` 在 `#launch()` 中调用了 `subscribeTranscriptLog(handle, store, task_id)` 将事件写入存储。

### Gear: 无 `subscribe()`

`WorkerSessionHandle` 没有订阅机制。要获取 worker 中间状态，只能轮询 `last_output()`。不能监听 tool call、partial text、state 变化等事件。

---

## 6.6 `dispose()` — 幂等释放

### OMO

```typescript
// child-handle.ts:114-118
dispose: () => {
    if (disposed) return;    // 幂等守卫
    disposed = true;
    session.dispose();
},
```

### Gear: 隐式 drop

`WorkerSessionHandle` 是 `Arc<dyn WorkerSessionHandle>`，引用计数归零时自动 drop。没有显式的 dispose，也没有幂等守卫。

---

## 6.7 总结

| # | OMO 模式 | Gear 现状 | 影响 |
|---|---------|----------|------|
| 1 | `abort` 作为独立操作 | 只有 `cancel`，没有独立 abort | 不能根据业务语义选择操作（cancel=不可恢复，abort=可重新尝试） |
| 2 | `subscribe()` 事件流 | 无 | 不能监听 worker 中间 tool calls、partial text、事件 |
| 3 | `turnActive` + `running` promise 模式 | `wait_for_outcome` blocking call | 不能实现 revive（新 turn 替换旧 running promise） |
| 4 | `dispose()` 幂等守卫 | 隐式 drop | 不能控制释放时机 |
| 5 | `beginTurn()` 构造时自动启动 | 手动 `start()` | 多一步调用，可能遗漏 |
