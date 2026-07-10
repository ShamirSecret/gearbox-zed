# OMO vs Gear 细粒度对比 - Part 1: 状态机核心

> 文件：`packages/senpi-task/src/state/` — types.ts、transitions.ts、messageability.ts、record.ts

---

## 1.1 状态枚举

### OMO `TaskStatus`（7 个）

```
pending → running → completed | error | cancelled | interrupted | lost
```

### Gear `ManagedTaskStatus`（6 个）

```
Pending → Running → Completed | Failed | Cancelled | Skipped
```

### 差异矩阵

| 维度 | OMO | Gear | 重要性 |
|------|-----|------|--------|
| **`interrupted`** | ✅ 独立状态，区别于 `cancelled` | ❌ 无。interrupt 和 cancel 都走 `Cancelled` | **P0** — 无法区分"用户取消"和"系统中断"。中断应允许 revive（取消不应） |
| **`lost`** | ✅ 意外丢失（进程崩溃、reconciliation 检测到孤儿） | ❌ 无。timeout/stale 都走 `Failed` | **P1** — Gear 无法区分"确定性失败"和"临时性丢失"。Lost 应自动重试，Failed 应进入 limited |
| **`error`** | ✅ 独立名称 | ❌ 用 `Failed` | 语义等价 |
| **`Skipped`** | ❌ 无此状态 | ✅ Gear 独有 | OMO 通过"不启动 task"处理 skip，不给它状态。Gear 的 `Skipped` 是冗余的 |

### 结论：Gear 应新增 `Interrupted` 和 `Lost` 状态

---

## 1.2 驻留状态（ResidencyState）

**OMO** 有 5 个驻留状态，Gear **完全没有**：

| OMO 状态 | 含义 | 可消息？ |
|----------|------|---------|
| `resident` | 存活在内存中 | 是 |
| `evicted` | 被 LRU 驱逐 | 否 |
| `disposed` | 已释放 | 全局否 |
| `persisted_only` | 仅持久化到磁盘 | 仅 revive |
| `rpc_detached` | RPC 子进程分离 | 仅 revive |

**Gear 缺失的后果：**
- 无法区分"活着的 running handle"和"在队列中等候的 pending task"
- worker session 释放后无法判断是"被驱逐了"还是"正常完成了"
- revive 时无法做 residency 检查

---

## 1.3 通知纪元（epoch）追踪

**OMO `TaskNotification`：**
```typescript
{
  run_epoch: 0,         // 创建时从 0 开始
  notified_epoch: -1,   // 创建时 = -1（从未通知过）
  notification_failed_epoch?: number  // 通知失败时记录
}
```

| 场景 | run_epoch | notified_epoch | 触发通知？ |
|------|-----------|----------------|-----------|
| 初始创建 | 0 | -1 | 否（未完成）|
| 第一次完成 | 0 | → 0 | 是 |
| revive（重启 turn） | → 1 | 0 | 是 |
| revive 后完成 | 1 | → 1 | 是 |
| 同一 epoch 第二次通知 | 1 | 1 | ❌ 跳过 |

**Gear：** 完全无 epoch 概念。每次 worker 完成都发事件，Gear GUI 可能收到重复通知。

---

## 1.4 状态转换守卫

### OMO `transitionTaskRecord()`

严格的两层检查：

```
第一层：终端幂等性守卫
  如果 record.status 是 terminal 且 transition.type 不是 residency-only：
    → {applied: false, audit: late_transition_ignored}

第二层：允许转换检查（isStatusTransitionAllowed）
  pending   → 只允许 start
  running   → 只允许 complete / fail / cancel / interrupt
  lose      → 永远不允许（只能用 markRecordLostForReconciliation 绕过）
  terminal  → 只允许 evict/dispose/persist_only/detach_rpc/mark_resident
```

**无效转换的后果：** 返回 `{applied: false}` 带上 audit，不 panic，不报错，静默忽略。

### Gear 现状：完全无守卫

```rust
// task_manager.rs:633 — 直接覆盖
record.status = ManagedTaskStatus::Failed;
// task_manager.rs:958 — 无状态检查就覆盖
record.status = ManagedTaskStatus::Failed;
record.failure_kind = Some(TaskFailureKind::WorkerFailed);
```

`Cancelled` 状态下被 `settle_running_task()` 覆盖为 `Failed` 的竞态路径：

```
用户取消 → Cancelled
后台 completion dispatcher 收到结果 → settle_running_task() 写入 Failed（覆盖 Cancelled）
```

---

## 1.5 `killed` 标志位

**OMO：** `killed: boolean` 附加在 `TaskRecord` 上。不是一个独立状态，仅作为事实记录。语义：被外部信号终止（kill signal），不是主动失败。

**Gear：** 无此概念。所有意外终止都标记为 `Failed`。

---

## 1.6 `messageability()` 函数

### OMO 完整矩阵

| 状态 | resident | evicted | disposed |
|------|----------|---------|----------|
| pending | **steer** | ❌ | ❌ |
| running | **steer** | ❌ | ❌ |
| completed | **revive** | ❌ | ❌ |
| error | **revive** | ❌ | ❌ |
| interrupted | **revive** | ❌ | ❌ |
| cancelled | ❌ | ❌ | ❌ |
| lost | ❌ | ❌ | ❌ |

其中：
- `steer` = 可接收 followUp（排队）和 steer（注入 mid-turn）
- `revive` = 可接收 followUp（将复活为新的 running turn）
- `not-continuable` = 任何消息都拒绝

### Gear 现状：无 `messageability`

`TaskManagerControl.send_follow_up_current_task()` 和 `steer_current_task()` 直接操作 handle，不检查状态：

```rust
pub fn steer_current_task(&self, prompt: &str) -> Result<SendResult> {
    let handle = self.running_handle.lock()...;  // 直接获取 handle
    handle.steer(prompt)  // 不检查 task 是否可 steer
}
```

如果 task 已经 completed/settled，handle 可能已为 None，此时调用的到的是不确定行为（可能 panic，可能静默失败）。

---

## 1.7 `markRecordLostForReconciliation()` — 恢复特殊路径

OMO 独有的函数。绕过正常的 transition 检查（因为 `lose` 在 `isStatusTransitionAllowed` 中永远返回 false）：

```
输入：任意非终端状态 → 强制设为 lost
输入：终端状态 → 返回 late_transition_ignored
```

用途：进程启动时 reconciliation，检测到上次异常退出遗留的 running/pending task 时使用。

**Gear：** 类似的逻辑在 `TaskManager.tick()` 中处理 stale task（Phase 8），但将其标记为 `Failed` 而非 `Lost`。

---

## 1.8 `createTaskRecord()` 初始值

**OMO：**
```typescript
status: "pending"         // 初始
residency_state: "resident"  // 初始即为驻留
notification: { run_epoch: 0, notified_epoch: -1 }
```

`notified_epoch: -1` 是关键设计：它意味着"创建时我们认为父会话已被通知过 0 次"（因为未完成）。当第一次完成时，`notified_epoch = run_epoch = 0`，通知被触发。如果 revive 后 `run_epoch = 1`，`notified_epoch` 仍为 0，所以第二次通知也会触发。

---

## 1.9 总结：Gear 的修复清单

| # | 缺失项 | 影响 | 建议实现 |
|---|--------|------|---------|
| 1 | `Interrupted` 状态 | interrupt vs cancel 行为不同 | 新增 `ManagedTaskStatus::Interrupted` |
| 2 | `Lost` 状态 | 区分临时性丢失 vs 确定性失败 | 新增 `ManagedTaskStatus::Lost` |
| 3 | 终端状态守卫 | 后台 completion 可能覆盖 cancel | 实现 `transition_record()` 函数 |
| 4 | `killed` 标志 | 无法区分外部终止 vs 内部失败 | 在 `TaskRecord` 加 `killed: bool` |
| 5 | `run_epoch` | 重复通知、释放守卫 | 加 `u64` 字段，revive 时递增 |
| 6 | `notified_epoch` | 父会话通知去重 | 加 `i64` 字段，每次通知后更新 |
| 7 | `messageability()` | 向已 settled 的 task 发消息 | 实现状态矩阵函数 |
| 8 | ResidencyState | 无法判断 handle 存活状态 | 新增 5 值枚举 |
