# OMO vs Gear 细粒度对比 - Part 2: TaskManager

> 文件：`packages/senpi-task/src/manager/` — manager.ts、concurrency.ts、types.ts

---

## 2.1 架构层面的核心差异

### OMO: Promise 驱动的异步完成追踪

```typescript
// manager.ts:242-263
#trackOutcome(taskId, handle, model, epoch) {
  handle.waitForOutcome()
    .then(outcome => {
      this.#releaseSlot(taskId, model, epoch);  // 释放并发槽（带 epoch 守卫）
      store.transition(taskId, outcomeToTransition(outcome)); // 状态转换（有守卫）
      this.#settleWaiters(taskId);  // 唤醒所有 waitFor 调用者
    })
    .catch(error => log("...", { taskId, error }));  // 静默捕获
}
```

完成通知从 worker handle → `.then()` 回调 → 直接走状态机。

### Gear: `mpsc::channel` + 后台线程 + 轮询 tick

```rust
// task_manager.rs:1339-1355
fn dispatch_running_task(&self, task_id, running_task) {
    std::thread::spawn(move || {
        let outcome = running_task.handle.wait_for_outcome()?;
        let result = running_task.handle.wait_for_result()?;
        finished_task_tx.send(FinishedTaskMessage { task_id, run_result });
    });
}

// task_manager.rs:768
pub fn tick(&mut self) -> Result<usize> {
    while let Ok(message) = self.finished_task_rx.try_recv() {
        self.settle_finished_task(message)?;
        settled += 1;
    }
    // ...
}
```

**差异影响：**

| 维度 | OMO | Gear |
|------|-----|------|
| 完成消息消费 | 自动（Promise.then） | 轮询（tick 间隔） |
| 完成到状态转换的时间 | 立即 | 取决于 tick 间隔 |
| cancel/interrupt 期间竞态 | 先转换再 abort → 独占 | 先 abort 再转换 → 后台结果可能覆盖 |
| 对 `wait_for()` 的响应 | Promise 队列，立即 resolve | `recv_timeout` 阻塞轮询 |

### Gear 的问题：`tick()` 周期内发生的完成在 `recv_timeout` 之前不可见

```rust
// task_manager.rs:735
match self.finished_task_rx.recv_timeout(WAIT_FOR_POLL_INTERVAL) {
```

如果 `WAIT_FOR_POLL_INTERVAL` 是 50ms，cancel 发送后最多要等 50ms 才能从 tick 中消费到。但 OMO 的 `.then()` 会在同一事件循环 tick 中消费。

---

## 2.2 并发槽释放守卫

### OMO: `${taskId}:${epoch}` 释放守卫

```typescript
// manager.ts:54-56
// Release guard keyed by `${taskId}:${runEpoch}` so a revived task (new epoch) can
// re-acquire a slot and still have its LATER release counted instead of swallowed.
readonly #released = new Set<string>();

// manager.ts:276-281
#releaseSlot(taskId, model, epoch): void {
    const key = `${taskId}:${epoch}`;
    if (this.#released.has(key)) return;  // 已释放 → 跳过
    this.#released.add(key);
    this.#concurrency.release(model);
}
```

**关键设计点：**
- epoch 使同一 task 的多次 release 可以正确计数。revive 后新 epoch 的 release 不会因为旧 epoch 已记录而被吞掉
- 一个 running handle 在 `#trackOutcome` 和 `#reacquireForRevive` 中各自有一个 epoch，互不冲突

### Gear: 没有释放守卫

Gear 的 `ConcurrencyManager` 只是一个 `max_parallel_workers` 和 `max_parallel_per_key` 计数器。完成一个 task 就是将其从 `running_tasks` HashMap 中移除（隐式释放），但没有显式的 `release()` 调用，也没有释放守卫。

```rust
// task_manager.rs:173-176 — ConcurrencyManager 只是一个阈值检查
struct ConcurrencyManager {
    max_parallel_workers: usize,
    max_parallel_per_key: usize,
}
// 没有 acquire() / release() / 计数器
```

**后果：** 当同一个 task 被意外调用了两次完成的路径（例如 cancel 和 background dispatcher 同时触发），并发槽计数不会出错（因为没有计数），但也没有任何守卫来防止这种重复。

---

## 2.3 `#release()` 的移交模式 vs Gear 的无移交

### OMO `TaskConcurrency.release()`（concurrency.ts:72-86）

```typescript
release(model: string): void {
    const queue = this.#queues.get(key);
    while (queue && queue.length > 0) {
        const next = queue.shift();
        if (!next.settled) {     // 找到第一个未取消的等待者
            next.settled = true;
            next.grant();        // 移交槽位 → 不递减计数器
            return;
        }
    }
    // 没有等待者 → 递减计数器
    const current = this.#counts.get(key) ?? 0;
    if (current > 0) this.#counts.set(key, current - 1);
}
```

**移交模式：** 插槽不会"还给池子"，而是直接转给下一个等待者。如果没有等待者，才递减。

### Gear: 没有等待队列，没有移交

Gear 的 `ConcurrencyManager.can_start()` 只是一个阈值检查。当 task 完成时，`running_tasks.remove(task_id)` 隐式释放槽位。下次轮询 `start_queued_tasks()` 会检查 `can_start()`。

```rust
// task_manager.rs:224-241 — 纯检查，不涉及队列
fn can_start(&self, running_tasks: &HashMap<String, RunningTask>, queued_task: &QueuedTask) -> bool {
    if running_tasks.len() >= self.max_parallel_workers() { return false; }
    let queued_key = concurrency_key_for_task(queued_task);
    let running_for_key = running_tasks.values()
        .filter(|rt| concurrency_key_for_task(&rt.queued_task) == queued_key)
        .count();
    running_for_key < self.max_parallel_per_key()
}
```

**后果：** 没有超时等待、没有优先级、没有 cancelWaiter。一个 task 要等到 tick 轮询时才能启动。

---

## 2.4 `wait_for()` 的实现差异

### OMO: Promise 队列

```typescript
// manager.ts:197-206
waitFor(taskId): Promise<TaskRecord> {
    const current = this.#tryLoad(taskId);
    if (current && isTerminalRecord(current))
        return Promise.resolve(current);  // 已经结束 → 立即返回
    return new Promise(resolve => {
        this.#waiters.get(taskId).push(resolve);  // 加入等待者队列
    });
}
```

所有等待者在 `#settleWaiters(taskId)` 中被批量唤醒（splice + forEach resolve）。

### Gear: `recv_timeout` 阻塞轮询

```rust
// task_manager.rs:730-745
pub fn wait_for(&mut self, task_id: &str) -> Result<ManagedWorkerRun> {
    loop {
        if let Some(run) = self.try_wait_for(task_id)? { return Ok(run); }
        match self.finished_task_rx.recv_timeout(WAIT_FOR_POLL_INTERVAL) {
            Ok(msg) => self.settle_finished_task(msg)?,
            Err(RecvTimeoutError::Timeout) => continue,  // 超时后重试
            Err(RecvTimeoutError::Disconnected) => bail!("..."),
        }
    }
}
```

这是一个**阻塞循环**，持有 `&mut self`。在 `wait_for()` 期间不能调用 `cancel_task()`（需要 `&mut self`）。

**后果：** Gear 的 `wait_for()` 不能被 cancel 打断，因为两者都需要 `&mut self`。OMO 的 Promise 队列则允许：cancel 和 wait 是独立的操作，cancel 发请求，wait 在 settled callback 中醒来。

---

## 2.5 失败类型的安全防护

### OMO `#tryLoad()` 优雅降级

```typescript
// manager.ts:296-302
#tryLoad(taskId): TaskRecord | null {
    try { return this.#options.store.load(taskId); }
    catch { return null; }  // 加载失败 → 返回 null，调用方处理
}
```

在 `#releaseSlotForTask`、`#reacquireForRevive`、`#settleWaiters`、`#recordSpawnFacts` 中都通过 `#tryLoad` 安全加载。

### Gear: 直接操作 `HashMap`

```rust
// task_manager.rs — 通过 self.records.get_mut(task_id) 直接访问
// 如果 task_id 不在 records 中就 panic（unwrap 或 expect）
```

---

## 2.6 `forget()` 清理

### OMO 有显式的清理点：

```typescript
// manager.ts:185-189
forget(taskId): void {
    this.#live.delete(taskId);
    this.#background.delete(taskId);
    // 清理 #released 中所有属于此 task 的过期条目
    for (const key of this.#released)
        if (key.startsWith(`${taskId}:`)) this.#released.delete(key);
}
```

### Gear: 没有显式的 `forget()`

`finished_task.message.running_task` 被 drop 时隐式释放 WorkerSessionHandle，但 `records` 中的 `TaskRecord` 没有被清理（除非 reached completed_archive cap）。

---

## 2.7 总结对比表

| 特性 | OMO | Gear | 差距 |
|------|-----|------|------|
| 完成追踪 | Promise `.then()` | 后台线程 + channel | 架构差异（Gear 不能避免"先 abort 再被 dispatcher 覆盖"的竞态） |
| 释放守卫 | `Set<${taskId}:${epoch}>` | 无 | Gear 可能重复释放 |
| 并发移交 | `grant()` 回调直接移交 | 隐式 remove | Gear 无等待队列 |
| `wait_for()` | Promise 队列，非阻塞 | `recv_timeout` 阻塞 | Gear 持有 `&mut self`，无法并发 cancel |
| 失败安全 | `tryLoad()` catch 降级 | records.get_mut() 直接 | Gear 可能 panic |
| 清理 | 显式 `forget()` | `records` 留在 HashMap 中 | Gear 有内存泄漏风险 |
| `#released` 清理 | `forget()` 中迭代删除 | 无此概念 | Gear 无 |

---

## 2.8 已补完成项（P1-1）

### P1-1：TaskManagerControl 状态控制语义收口 ✅

此补丁将 Gear 的 TaskManager 返回值从 `Result<bool>` 升级为结构化枚举，缩小了与 OMO 返回类型的差距。

| 项目 | 内容 |
|------|------|
| **问题** | `TaskManagerControl` 未与 `TaskManager` 共享统一控制路径；send/steer 返回 `bool`，无法区分 `NotContinuable`/`Noop`/`Steer` |
| **改动** | `crates/gearbox_agent/src/task_manager.rs` — 新增 `SendOutcome`/`SteerOutcome`/`CancelOutcome`/`InterruptOutcome` 枚举；终态任务（Cancelled/Lost）返回 `NotContinuable` |
| **改动** | `crates/agent/src/agent.rs` — 适配新返回类型，GUI 错误文案映射 |
| **测试** | pending/queued/steer/revive 边界覆盖；Cancelled/Lost task 收到 `NotContinuable` |
| **对比 OMO** | 仍缺 `scope_denied` 变体（Gear 无会话范围检查），但 `not_found`/`noop`/`steer`/`revive`/`queued` 已对齐 |
| **commit** | `c99b6572dc` (部分) |
