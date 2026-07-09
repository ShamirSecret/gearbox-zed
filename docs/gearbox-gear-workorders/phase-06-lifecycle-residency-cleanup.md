# Phase 06：Lifecycle、residency、reconciliation 与 TTL

## 目标

把销毁、驱逐、启动恢复、TTL 清理统一到生命周期模块，避免 resident handle 泄漏、旧 running record 残留、cancelled/lost 被错误 FIFO 清掉。

## 主要文件

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/runtime.rs`

## 具体工单

1. 新增统一销毁入口：
   - `destroy_resident_task(task_id, cause) -> DestroyResult`
   - LRU eviction、TTL cleanup、session shutdown、reconciliation、显式 dispose 这类“最终释放 residency”的路径都路由到这里。
   - 普通 cancel / interrupt 先遵守 Phase 03 的控制语义：转换状态、best-effort cancel/interrupt/abort，并保留 `Interrupted + Resident -> Revive` 的可能；只有用户或 lifecycle policy 明确要求最终释放 session 时，才进入 destroy。
   - 已完成一部分：`TaskManager` 现在在 `Drop` 时会做 session shutdown cleanup，best-effort 释放 resident handle，并保留最后 task id 供外部观察；pending/running 的 shutdown current 状态会降级为 `Lost`，不会留下假 running handle。
2. destroy 保证：
   - best-effort `abort` / `cancel` / `terminate`
   - 无论 abort 是否失败，都必须调用 `dispose`
   - dispose 错误写 lifecycle event，但不能阻止 residency transition
   - teardown 错误要进入日志或 lifecycle artifact，不能静默吞掉。
   - 已完成一版：`destroy_resident_task()` 优先使用实际 running handle，不再依赖 `current_task_snapshot()` 里的句柄；如果能从 running task 或 completed resident run 拿到 `StateStore`，会写入 `dispose` lifecycle event。
   - 已完成：`TaskManager` 现在还保留 `task_record_paths` runtime 索引，可从 orphan `task-record.json` 路径反推出 workspace root，让 recovered orphan record 也能写 `dispose` / reconcile lifecycle event。
3. residency admission：
   - 默认 `residency_max_children = 8`
   - 按 parent/root session 统计 resident task。
   - 超过上限时找 LRU evictable task。
4. evictable 状态：
   - 可驱逐：`Completed`、`Failed`、`Interrupted`
   - 不自动驱逐：`Cancelled`、`Lost`
   - 有 pending send 的 task 不驱逐。
5. LRU 失败处理：
   - 找不到可驱逐 task 时返回 `AgentLimitReached` 风格错误。
   - 错误中列出 resident task name/status，便于用户或 Gear 做下一步。
6. 启动 reconciliation：
   - runtime 启动扫描 `.gearbox-agent/workers/*/task-record.json`。
   - 非 terminal 的 `Pending` / `Running` 标记为 `Lost`，而不是 `Failed`。
   - 如果后续记录 pid/session id：
     - pid 缺失：mark lost
     - pid dead：mark lost
     - pid alive orphan：mark lost + terminate orphan
7. TTL cleanup：
   - 默认 `ttl_ms = 24h`。
   - 只删除 terminal 且超过 TTL 的 records。
   - `Lost` + process 记录必须确认 pid dead 后才删除。
8. archive 改造：
   - `completed_archive` 的容量 cap 不能 FIFO 清掉 `Cancelled` / `Lost`。
   - archive 清理与 residency/TTL 的语义分开。

## 测试

1. `destroy_disposes_even_when_abort_fails`
2. `destroy_uses_running_handle_even_without_current_snapshot`
3. `destroy_persists_dispose_lifecycle_event_when_store_available`
4. `task_manager_drop_shuts_down_resident_tasks`
5. `shutdown_downgrades_current_running_to_lost_without_handle`
6. `cancel_interrupt_preserve_resident_revival_semantics`
7. `lru_evicts_oldest_completed_not_cancelled`
8. `lost_record_is_not_ttl_deleted_until_process_dead`
9. `reconcile_marks_running_record_lost`
10. `residency_limit_reports_current_residents`
11. `task_manager_recovers_orphaned_pending_and_running_records_from_disk`

## 验收

- 所有释放 resident handle 的路径都经过一个 destroy 函数。
- restart 后旧 pending/running 不再显示为假运行态。
- cancelled/lost 不会被普通 archive cap 自动挤掉。
- 普通 cancel/interrupt 不会破坏 terminal resident revive；destroy 只表示最终释放 residency。
- 有可解析 `StateStore` 的 destroy 路径会留下 `dispose` lifecycle event；orphan record 现在也能借助 `task_record_paths` runtime 索引恢复到可解析的 store root。
