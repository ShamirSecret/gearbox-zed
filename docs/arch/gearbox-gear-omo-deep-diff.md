# OMO 源码级细粒度对比：Gear 实现偏差与遗漏

> 基于 oh-my-openagent `packages/senpi-task` 和 `packages/omo-opencode/src/features/background-agent` 完整源码阅读。

---

## 一、状态机层面的关键差异

### 1.1 ❗ "先转换，再 abort" — Gear 的 interrupt 顺序可能丢失竞态

**OMO（`steering/engine.ts:115-122`）：**
```typescript
// 先转换：running → interrupted
port.store.transition(record.task_id, { type: "interrupt", timestamp });
// 再 abort
if (handle) await handle.abort();
// 捕获 partial text（由 steering 独占）
handle?.lastAssistantText();
```

注释解释了原因：**"steering is the single terminal writer"** — 先写入终端状态，后续来自后台 completion tracker 的完成/取消转换会被终端幂等性拒绝。

**Gear 现状（`task_manager.rs`）：** `interrupt_task()` 和 `cancel_task()` 直接调用 `handle.interrupt()/cancel()`，没有先做状态转换。如果 worker 在 interrupt/cancel 回调中同步完成，可能产生状态竞争——后台的 completion dispatcher 和处理器的控制路径同时尝试修改 task 状态。

**影响：** Worker 在收到 interrupt 信号后同步返回 outcome 时，背心 completion handler 可能把状态设为 `Completed` 而非 `Interrupted`。

---

### 1.2 ❗ 终端状态只允许 residency 转换

**OMO（`state/transitions.ts`）：**
```
terminalStatuses = { completed, error, cancelled, interrupted, lost }
终端状态 → 只允许: evict, dispose, persist_only, detach_rpc, mark_resident
所有其他转换 → 静默忽略（不是 panic，不是错误，只是 {applied: false}）
```

**Gear 现状（`task_manager.rs`）：** `ManagedTaskStatus` 没有专门的 residency 状态字段，也没有终端幂等性守卫。如果 `settle_running_task()` 在一个已经是 `Cancelled` 的任务上尝试设为 `Completed`，它会直接覆盖。

**修复建议：** 新增 `ResidencyState` 枚举和 `transition_task_record()` 函数，在终端状态上只允许 residency 转换。对于无效转换返回 `Result` 但不 panic。

---

### 1.3 ❗ `#released` 集合：基于 epoch 的释放守卫

**OMO：**
```typescript
released = new Set<string>()  // key 为 `${taskId}:${epoch}`
```
- `release()` 先检查 `released.has(key)` 再执行，防止同一 (task, epoch) 对重复释放
- `revive` 时 epoch + 1，所以新 epoch 的释放不会与旧 epoch 的释放记录冲突

**Gear 现状：** 没有 epoch，没有释放守卫。`ConcurrencyManager.release()` 递减计数，但如果被重复调用，计数会变成负数（或提前释放他人的插槽）。

**影响：** 在 `max_parallel_workers > 1` 场景下，重复 release 会导致并发控制提前释放插槽。

---

## 二、Worker 生命周期管理的细粒度差异

### 2.1 ❗ `messageability` — 任务可消息性判定矩阵

**OMO（`state/messageability.ts:24` 行完整矩阵）：**

| 状态 | resident | evicted | disposed |
|------|----------|---------|----------|
| pending/running | **steer** | not-continuable | not-continuable |
| completed/error/interrupted | **revive** | not-continuable | not-continuable |
| cancelled | not-continuable | not-continuable | not-continuable |
| lost | not-continuable | not-continuable | not-continuable |

其中：
- `steer` = 可接收 steer 和 followUp（同时存活）
- `revive` = 可接收 followUp 来复活（终端但驻留）
- `not-continuable` = 不可消息（已取消、丢失、已释放）

**Gear 现状：** `TaskManagerControl.send_follow_up_current_task` / `steer_current_task` 直接打到 running handle，没有状态检查。如果 worker 已经 settled 或被取消，调用静默失败（handle 可能已 None）。

**修复建议：** 实现 `messageability` 函数，在接受 steer/followUp 之前检查 task 状态。

---

### 2.2 ❗ `cancel()` 级联 — 只取消 `current_task` 不够

**OMO（`stop-continuation-guard/hook.ts`）：**
```typescript
stop(sessionID) {
  // 1. 加入 stoppedSessions Set
  // 2. 递归找到 ALL 子/孙任务
  const descendants = backgroundManager.getAllDescendantTasks(sessionID);
  // 3. 全部取消，skipNotification: true 防止通知风暴
  descendants.forEach(task => cancelTask(task.id, { skipNotification: true }));
}
```

**Gear 现状：** `cancel()` 只取消当前 task。在 `max_parallel_workers=1` 下无影响，但架构不支持 future parallel cancel。

**影响：** Phase 9 的 parallel mode 准备不足。即使现在不做并行，数据结构也应支持 `tasks_by_root_session` 或 `get_descendant_tasks()`。

---

### 2.3 ❗ `"cancelled"` 不可被 LRU 驱逐

**OMO（`lifecycle/residency.ts`）：**
```typescript
EVICTABLE_STATUSES = new Set(["completed", "error", "interrupted"]);
// cancelled 和 lost 不在其中
```

原因：取消是用户发起的，用户可能希望查看结果。LRU 不应自动清除用户显式取消的内容。

**Gear 现状：** `completed_archive` 有 100 条上限但**没有区分状态**，`Cancelled` 的任务也会被 FIFO 挤出。

---

### 2.4 ⚠️ `followUp` 对 idle handle 的 revive 语义

**OMO（`runners/in-process/child-handle.ts`）：**
```typescript
followUp: async (text) => {
  if (turnActive) {
    await session.followUp(text);  // 活跃时：排队进 session
    return;
  }
  beginTurn(text);  // idle 时：复活（start a new turn promise）
}
```

复活后，`run_epoch++`，`waitForIdle()` 自动指向新的 `running` promise（因为闭包更新了 `running` 变量）。

**Gear 现状：** `OpencodeSessionWorker` 的 `send_follow_up` 对 running handle 的 follow-up 和 idle handle 的 revive 有不同的代码路径。但 `run_epoch` 不存在。

**建议：** 即使不暴露 `run_epoch` 给 API，内部也应该追踪，用于释放守卫和通知去重。

---

## 三、并发控制的细节差异

### 3.1 ❗ 两种不同的 acquire 模式

**OMO 有两个 concurrency manager 层：**

| 层次 | acquire() | 行为 |
|------|----------|------|
| senpi-task `TaskConcurrency` | 同步 `void` | 调用者先检查 `hasFreeSlot`，再 `acquire` |
| omo-opencode `ConcurrencyManager` | 异步 `Promise<void>` | 达到上限时返回一个等待中的 Promise，插槽可用时 resolve |

**Gear 现状：** `ConcurrencyManager` 使用 `Mutex` + 条件变量。acquire 返回 `MutexGuard<'_>`（Rust 风格）。**没有等待队列**——如果并发槽满了，`task_manager.start()` 直接返回错误 "concurrency limit reached"，任务不会排队等待槽位释放。

**修复建议：** 实现等待队列：`acquire()` 在槽位不足时注册 waiter，`release()` 唤醒下一个 waiter。否则达到并发上限时任务直接失败。

---

### 3.2 ⚠️ `release()` 的移交模式

**OMO：** `release()` 先检查等待队列。如果有等待者，将槽位直接移交（不递减计数器）。没有等待者时递减。

**Gear 现状：** `release()` 总是递减计数器。没有移交模式。

**影响：** 在 `max_parallel_workers=1` 下不影响。多 worker 时，release 后可能被另一个无关的 key 窃取插槽，而不是分配给正确的等待者。

---

## 四、容错/清理的细节差异

### 4.1 ❗ `"lost"` 状态 + PID 追踪

**OMO 有 `"lost"` 状态（在 `TaskStatus` 和 `ManagedTaskStatus` 中都有）：**
- 用于 reconciliation（进程启动时检测孤儿进程）和 polling 超时
- `"lost"` 的记录在 TTL 清理中受保护：只有对应 OS 进程确认死亡后才删除
- 区分 "crashed unexpectedly"（lost）和 "deterministic failure"（error）

**Gear 现状：** 没有 `"lost"` 状态。Phase 8 的 stale timeout 会把超时 worker 标记为 `"Failed"` 而不是 `"Lost"`。

**影响：** 当 Gear loop 因为网络闪断 timeout 时，用户看到的是 "Failed"（貌似确定性的失败）而非 "Lost"（可能是临时性的，可自动重试）。`GoalLoop` 可能对 Lost → repair 更积极，对 Failed → limited 更保守。

---

### 4.2 ⚠️ `startAttempt()` 失败时回退到 `ensureCurrentAttempt`

**OMO（`attempt-lifecycle.ts`）：**
```typescript
function ensureCurrentAttempt(task, model?) {
  if (task.currentAttemptID) return findAttempt(task, task.currentAttemptID);
  // 从 task 当前状态创建一个新 attempt
  return createAttemptFromTask(task, model);
}
```

当 `scheduleRetryAttempt` 的 `currentAttemptID !== failedAttemptID`（并发访问导致状态偏斜）时，不是 panic，而是返回 `undefined`，调用方静默跳过重试。

**Gear 现状：** 没有 `ensureCurrentAttempt` 模式。attempt 的创建和同步是手动的，可能有时序问题。

---

## 五、通知/事件流的细节差异

### 5.1 ⚠️ Parent wake 的三层架构

**OMO 的 `ParentWakeNotifier` 有三个子组件：**

| 层 | 作用 | 关键参数 |
|----|------|---------|
| `PendingQueue` | 去抖合并，`delayMs` 内多次完成合并为一次通知 | delayMs = 100（默认） |
| `DispatchedTracker` | 投递后 5s 窗口内检测是否被会话实际处理 | failureRequeueWindowMs = 5000 |
| `SessionInspector` | 检查会话是否在 streaming、是否有用户消息进行中 | userMessageInProgressWindowMs |

**Gear 现状：** `async_channel` 无缓冲直接写 markdown stream。没有去抖、没有投递确认、没有会话忙碌检测。

**影响：** Gear worker 完成通知可能：
1. 打断用户正在观看的 assistant streaming 回复
2. 在 Gear 快速连续完成多个 task 时产生大量 markdown 刷屏

---

### 5.2 ⚠️ `completionMessage` 只通知 `completed/error/lost`

**OMO（`completion/notifier.ts`）：**
```typescript
function shouldNotifyStatus(status): boolean {
  return ["completed", "error", "lost"].includes(status);
}
// 取消和中断是同步的 → 不需要通知
```

**Gear 现状：** 所有 terminal 事件（包括 cancelled/interrupted）都通过 event_sink 发送。

**影响：** 用户会看到 "Gear: task_004 cancelled" 这样的事件，即使取消是用户自己触发的。OMO 的设计更简洁：取消是同步操作，不需要额外通知。

---

## 六、安全与配置的细节差异

### 6.1 ❗ `SECRET_LIKE_MODEL_FIELD_NAMES` — 模型元数据防泄漏

**OMO（`category/resolver.ts`）：**
```typescript
SECRET_LIKE_MODEL_FIELD_NAMES = new Set([
  "accesstoken", "apikey", "auth", "authorization", "bearertoken",
  "clientsecret", "password", "privatekey", "privatetoken",
  "secret", "secretkey", "token",
]);
// 如果 model 有任何这些字段（规范化后），拒绝解析
```

每次 model 解析时扫描模型元数据字段名，防止 API key 通过 provider registry metadata 泄露到 worker packet 或 review prompt 中。

**Gear 现状：** 没有类似扫描。如果 `CoordinatorModel` 的 `provider_id` 或 `model_id` 中包含 API key 或其他秘密（配置错误），它们会直接写入 `.gearbox-agent/` 的 goal ledger 和 worker packets。

---

### 6.2 ❓ `hasMoreFallbacks` 的 attempt 计数

**OMO（`fallback-retry-handler.ts`）：**
```typescript
function hasMoreFallbacks(fallbackChain, attemptCount): boolean {
  return attemptCount < fallbackChain.length;
}
```

fallback 链的条目数决定最大重试次数，而不是固定常数。链越长，重试机会越多。

**Gear 现状：** `MAX_SAME_FAILURE_RETRIES = 2`（常数），与 fallback 链长度无关。如果某个 category 配置了 5 个 fallback 但常数是 2，多余的 fallback 不会被利用。

---

## 七、建议立即修复的项

按优先级排列：

### P0 — 可能导致错误的差异

| # | 问题 | 修复方案 |
|---|------|---------|
| 1 | interrupt/cancel 前不做状态转换 | 先设置状态为 Interrupted，再调用 handle 操作 |
| 2 | 终端状态没有幂等性守卫 | 实现 `transition_task_record()`：终端状态只允许 residency 转换 |
| 3 | `release()` 没有移交模式 | 先检查等待队列，移交插槽，无等待者时才递减 |
| 4 | `acquire()` 没有等待队列 | 在 `ConcurrencyManager` 中实现 waiter 注册/唤醒 |
| 5 | `"loss"` 与 `"fail"` 不区分 | 新增 `Lost` 状态，用于 stale/timeout 中断 |

### P1 — 功能完整性的差异

| # | 问题 | 修复方案 |
|---|------|---------|
| 6 | 没有 `messageability` | 实现状态矩阵：pending/running→steer，终端+resident→revive |
| 7 | 没有 `run_epoch` | `TaskRecord` 加 `run_epoch: usize`，revive 时递增 |
| 8 | `cancel()` 只取消当前 task | 新增 `get_descendant_tasks()` 基础设施 |
| 9 | `"cancelled"` 不应被 archive 自动挤出 | completed_archive 中保留 cancelled task |

### P2 — 健壮性的差异

| # | 问题 | 修复方案 |
|---|------|---------|
| 10 | 模型元数据安全扫描 | 新增 `SECRET_LIKE_MODEL_FIELD_NAMES` 检查 |
| 11 | `hasMoreFallbacks` 硬编码 | 改为按 fallback chain 长度计算 |
| 12 | Parent wake 无忙碌检测 | 在 event streaming 中加入去抖和活跃检查 |
| 13 | `ensureCurrentAttempt` 模式 | attempt 工厂函数，时序偏斜时优雅降级 |
