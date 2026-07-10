# OMO vs Gear 细粒度对比 - Part 5: Lifecycle

> 文件：`packages/senpi-task/src/lifecycle/` — destroy.ts、reconcile.ts、residency.ts、ttl.ts

---

## 5.1 单一写入者销毁端口

### OMO `destroyResidentTask()`（destroy.ts:12-25）

这是全包内**唯一的销毁入口**。cancel、LRU eviction、TTL、session shutdown、reconciliation 都路由到这里：

```typescript
async function destroyResidentTask(context, taskId, cause) {
    const handle = context.registry.get(taskId);
    if (handle !== undefined) {
        await teardownHandle(handle);    // best-effort abort/terminate + 必达 dispose
        context.registry.forget(taskId); // 从注册表移除
    } else if (cause === "reconcile_lost") {
        await terminateOrphan(context, taskId); // SIGTERM → SIGKILL 升级
    }
    recordResidency(context, taskId, cause); // 写记录 + 事件
}
```

**`teardownHandle()` 的两个保证：**
1. `abort()`/`terminate()` — best-effort（异常只 log，不抛）
2. `dispose()` — **始终运行**（注释：被吞掉的异常不能阻止 dispose，否则会导致 resident zombie，LRU 永远无法回收）

### Gear: 每个路径自己销毁

```rust
// cancel_task → 直接 handle.cancel() + 写记录
// interrupt_task → 直接 handle.interrupt() + 写记录
// tick() 中的 stale sweep → 标记 Failed + 直接移除
```

没有统一销毁入口。`RunningTask` 的 `WorkerSessionHandle` 被 drop 时释放，但状态记录的清理和事件写入由各个调用方独立处理。

**后果：** 如果某个路径忘记释放 handle（panic 导致 drop 延迟、mutex 死锁），会造成 handle 泄漏。

---

## 5.2 启动 reconciliation（reconcile.ts）

### OMO `reconcileOnSessionStart()`

每次 session 启动时运行。对每条持久化的记录：

```typescript
// reconcile.ts:23-49
async function reconcileRecord(context, record) {
    // 如果已经是 terminal → 报告 "already lost" 或 "resumed"，不做任何操作
    if (TERMINAL_STATUSES.has(record.status)) {
        return record.status === "lost" ? "already lost" : "resumed";
    }
    // 如果非 terminal 且是进程内 task（上一轮 process 的 in-process task）→ 标 lost
    if (record.execution_mode !== "process") { return markLost(context, record, "previous-process in-process"); }
    // 如果没有 pid → 标 lost
    if (pid === undefined) { return markLost(context, record, "no recorded pid"); }
    // 如果 pid 已死 → 标 lost
    if (!context.signaller.isAlive(pid)) { return markLost(context, record, "dead pid"); }
    // 如果 pid 还活着 → 先标 lost，再 terminateOrphan（SIGTERM → SIGKILL）
    markLost(...);
    await destroyResidentTask(context, record.task_id, "reconcile_lost");
    return "lost_and_terminated";
}
```

**四种 `markLost` 场景对应四种不同的原因：**
| 场景 | 原因 | 是否 kill 进程 |
|------|------|---------------|
| 进程内 task 无法重连 | previous-process in-process | 否 |
| RPC task 没有 pid | no recorded pid | 否 |
| pid 已死 | dead pid | 否 |
| pid 还活着 | live orphan | 是（SIGTERM → SIGKILL） |

### Gear: 无 reconciliation

Gear 的 `TaskManager.tick()` 中 `sweep_stale_running_tasks()` 会在运行时检测超时的 running task，但**没有启动时的 reconciliation 阶段**。如果 Gear 进程崩溃重启，之前的 `.gearbox-agent/workers/*/task-record.json` 中的 `running`/`pending` 记录不会 reconciliation。

---

## 5.3 LRU Residency Eviction（residency.ts）

### OMO

```typescript
// residency.ts:7
const EVICTABLE_STATUSES = new Set(["completed", "error", "interrupted"]);
// cancelled 和 lost 不在其中——取消是用户发起的，不应该 LRU 自动清除
```

**`admitResident()` 流程：**
```
1. 获取此 parent_session 的所有 resident records
2. 如果数量 < residency_max_children → admitted
3. 否则找 lruEvictable：
   a. 过滤出 EVICTABLE_STATUSES 且没有 pending sends 的
   b. 按 updated_at 升序排序
   c. 取第一个（最旧的）
4. 找不到可驱逐的 → rejected（带 AgentLimitReached 错误，列出所有 resident 的 name+status）
5. 找到 → destroyResidentTask(victim, "evict")
```

**默认 `residency_max_children`：** 8（来自 OmoTaskSettings）

### Gear: 无 LRU

`completed_archive` 有 100 条上限（FIFO 溢出时丢弃最旧的完成记录），但不区分状态，包括 `Cancelled` 的记录也会被挤出。没有基于 parent_session 的 eviction 限制。

---

## 5.4 TTL Cleanup（ttl.ts）

### OMO `cleanupExpiredRecords()`

只在组件启动时运行一次（不是周期性任务）。

```typescript
// ttl.ts:27-34
function isExpungeable(context, record, cutoff) {
    if (!TERMINAL_STATUSES.has(record.status)) return false;  // 非 terminal 永远保留
    if (Date.parse(record.updated_at) > cutoff) return false; // 足够新
    // lost + process 记录：必须确认 pid 已死才删除
    if (record.status === "lost" && record.execution_mode === "process") {
        return record.pid !== undefined && !context.signaller.isAlive(record.pid);
    }
    return true;
}
```

**默认 TTL：** 24 小时（`OmoTaskSettings.ttl_ms`）

### Gear: `completed_archive` 有 cap 无 TTL

`completed_archive: VecDeque<TaskRecord>` 在容量达到 100 时 FIFO 移除。没有 TTL 检查，没有 lost 保护。

---

## 5.5 Gear 修复清单

| # | 缺失 | 影响 | 建议 | 状态 |
|---|------|------|------|------|
| 1 | 单一销毁端口 | 每个路径独立销毁，可能泄漏 | 引入 `destroy_task()` 统一入口 | ⏳ 未实现 |
| 2 | 启动 reconciliation | 崩溃重启后残留 `running` 记录不会处理 | 启动时扫描 `.gearbox-agent/workers/*/task-record.json` | ⏳ 未实现 |
| 3 | LRU eviction | 无 resident 上限控制 | 新增 `admit_resident()` 和 LRU | ⏳ 未实现 |
| 4 | Evictable 状态排除 cancelled | `cancelled` 不应被自动驱逐 | 当前 completed_archive 已处理 | ✅ 已处理 |
| 5 | TTL 清理 | 无时间基的记录过期 | 启动时删除超过 N 天的旧记录 | ⏳ 未实现 |
| 6 | `lost` 记录保护 | lost 记录在 pid 确认死前不删除 | Gear 当前没有 lost 状态 | ⏳ 未实现 |

---

## 5.6 已补完成项（P0/P1 轮次）

以下 item 在此文档范围之外但直接相关，在此记录：

### P0-5：`GoalDecisionPolicy` 对"无 fallback"处理收口 ✅

| 项目 | 内容 |
|------|------|
| **相关文件** | `crates/gearbox_agent/src/runtime.rs`, `crates/gearbox_agent/src/workers.rs` |
| **改动** | `CategoryResolutionResult::nearest_fallback()` helper；`GoalDecisionPolicy` 新增 `nearest_fallback_available` 字段；`evaluate()` 在无 fallback + 无进展 + iteration>1 时返回 `Limited` 而非 `Running` |
| **测试** | `evaluation_limits_when_no_fallback_available`, `evaluation_continues_on_first_iteration_when_no_fallback` |
| **commit** | 与 P2-1 一并提交（`c99b6572dc` 前身） |

### P1-2：父会话 completion 通知串行化与重排 ✅

| 项目 | 内容 |
|------|------|
| **相关文件** | `crates/gearbox_agent/src/task_manager.rs`, `crates/gearbox_agent/src/runtime.rs` |
| **改动** | `CompletionNotifier` 增加 `flush_serializer`（per-session 串行锁）、`pending_flush`（排队唤醒）、flush 时状态重验证 |
| **测试** | `completion_flush_serializes_rapid_arrivals`, `completion_flush_works_after_idle_transition` |
| **commit** | `067d783c82` — "Fix Gear completion flush and teardown lifecycle" |
