# Phase 05：Completion notification 与 GUI parent wake

## 目标

实现 OMO completion notification 的关键守卫：只通知 externally-caused terminal、epoch 去重、缓冲、GUI parent session 忙碌检测，避免 worker 完成消息打断用户正在进行的 Agent/Gear 对话。

## 主要文件

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/runtime.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`

## 具体工单

1. 新增 `CompletionNotifier`：
   - 输入：`TaskRecord`、parent session state、event sink。
   - 输出：`NotificationResult::{Skipped, Sent, Buffered, Dropped, Failed}`
   - 已完成：runtime 已接入 turn-end flush，`TaskManager` 侧测试覆盖 buffered / dedupe / failed epoch 语义。
2. 新增通知触发规则：
   - 只通知 `Completed` / `Failed` / `Lost`。
   - 不通知 `Cancelled` / `Interrupted`，因为它们由用户同步控制路径返回。
   - 非 terminal 不通知。
   - `notified_epoch >= run_epoch` 不重复通知。
   - 已完成：`CompletionNotifier::should_notify()` / `build_notification()` / `already_notified()` 已落地并有回归测试。
3. 新增运行时去重缓冲：
   - buffer key: `(task_id, run_epoch)`
   - 同一 key 多次进入 buffer 只保留一条。
   - flush 前重新加载最新 record，再写 `notified_epoch`。
   - flush 前必须重新检查 parent/root busy state；如果 root thread 又进入 generating / compacting / waiting / tool-call / queued 状态，继续留在 buffer。
   - 已完成：runtime 现在在 Gear turn 期间以 `Streaming` 缓冲，turn 结束后以 `Idle` flush；GUI flush 已补 root busy state re-check。
4. 新增 parent session state：
   - `Idle`
   - `Streaming`
   - `Compacting`
   - `SessionSwitching`
   - `SessionShutdown`
   - MVP 如果 GUI 侧无法精确判断，先以保守 `Streaming` / `Idle` 两态接入。
   - 已完成 MVP：当前 runtime 只先接入 `Streaming` / `Idle` 两态，后续再补 GUI 精确状态。
5. 路由策略：
   - `Idle` -> wake / append completion message。
   - `Streaming` -> 延迟到下一个 tool boundary 或当前 turn 结束。
   - `Compacting` / `SessionSwitching` / `SessionShutdown` -> buffer。
   - 已完成 MVP：`Streaming` 只缓冲，`Idle` 只在 flush 阶段落盘并推送消息；GUI 侧已把 root thread generating / compacting / waiting / in-progress tool call / queued message 纳入 busy 检测，其它 parent-session 状态后续补齐。
6. 新增 debounce：
   - 默认 100ms 合并连续 worker completion。
   - 同一 parent session 的完成通知串行化。
   - 已完成：buffer flush 已按 `run_epoch` + `task_id` 排序，避免 turn 结束时 completion 顺序乱跳。
7. 新增 delivery retry：
   - 首次失败记录原因。
   - 用短退避重试，不做 OMO 那种无延迟同调用双重重试。
   - 超过窗口后写 `notification_failed_epoch`。
   - 已完成：短退避重试已接入 `CompletionNotifier` 主链路，第一次失败会再试一次，仍失败才写 `notification_failed_epoch`。
8. completion message 内容：
    - task id / name
    - status
    - duration
    - final response head
    - continuation hint
    - artifact links
   - 已完成一版：task id / name / status / duration / final response head / continuation hint / artifact links 都已入。
9. GUI 集成：
    - 当前 `gear_task_manager_snapshot_to_markdown` 继续作为 snapshot。
    - completion notification 不直接插入正在 streaming 的 assistant 文本中间。
    - 若后续改为专门 Gear panel，则 notifier 写 panel event，不写聊天流。
    - 已完成一版：completion notification 现在会在 turn 结束后进入消息流，并在 root thread 忙碌时入缓冲、idle flush 前再次确认仍然 idle，不会打断 streaming assistant 文本；专门 panel 仍是 follow-up。

## 测试

1. `cancelled_and_interrupted_do_not_emit_completion_notification`
2. `same_epoch_completion_notified_once`
3. `revived_epoch_completion_notifies_again`
4. `streaming_parent_buffers_completion`
5. `buffer_flush_deduplicates_task_epoch`
6. `delivery_failure_records_notification_failed_epoch`
7. `test_notification_buckets_until_root_thread_is_idle`
8. `delivery_retry_succeeds_after_transient_failure`
9. `completion_notification_includes_summary_head_and_continuation_hint`
10. `completion_notification_uses_artifact_hint_when_not_continuable`

## 验收

- 用户 cancel/interrupt 后不再额外刷一条异步 “cancelled” 消息。
- 同一 worker 同一 epoch 只通知一次。
- worker completion 不会插入用户正在看的 streaming 回复中间。
- root thread 仍忙时，已经 buffered 的 completion 不会因为一次 flush tick 被提前弹出。
- completion delivery 失败时会先短退避重试一次，再决定是否写 `notification_failed_epoch`。
- completion 内容会同时包含 final response head 和 continuation hint。
- not-continuable 任务的 continuation hint 会指向结果/产物 artifact，而不是假装还能继续同一 task。
