# Phase 09：GUI 原生 worker 池、小规模并行与级联取消

## 目标

在前面状态机、lifecycle、notification 都稳定后，开放小规模并行与真正 worker 池：opencode 默认执行，Codex/Claude/Zed Agent 作为可调度 worker，GUI 里能观察和控制多个 task，同时保证 write task 不并行改同一 scope。

## 主要文件

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/runtime.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`

## 具体工单

1. `max_parallel_workers > 1`：
   - 默认仍为 1。
   - 仅 read-only explore/review task 可并行。
   - write/repair task 默认串行。
2. dependency model：
   - `TaskRecord.parent_task_id`
   - `root_session_id`
   - `depends_on: Vec<TaskId>`
   - `scope_key` / `write_scope`
3. descendant cancel：
   - 新增 `get_descendant_tasks(session_or_task_id)`。
   - Gear session cancel 时递归取消所有 pending/running 子孙 task。
   - 使用 `skip_notification` 防止通知风暴。
   - pending task 只从 queue 移除；running task 才 abort session。
4. scope guard：
   - read-only tasks 可并行。
   - 两个 write tasks 如果 scope overlap，后者必须等待或被拒绝。
   - worker packet 里写 scope assumption。
5. GUI panel：
   - 已从 `thread_view` 内嵌 markdown 区块升级为独立 Gear task panel。
   - 支持筛选：pending/running/terminal/category/worker。
   - 支持排序：updated time、status、category。
   - 支持打开 artifact：packet、transcript、result、outcome、review。
   - 支持 task control：cancel、interrupt、follow-up、steer。
6. Worker pool：
   - opencode：默认 write worker。
   - Zed Agent：native sibling/subagent worker，不递归 Gear。
   - Codex：先 CLI adapter，后 session adapter。
   - Claude：先 CLI adapter，后 session adapter。
   - custom：显式配置命令。
7. worker routing：
   - review/explore 可优先 Zed Agent/Codex/Claude。
   - repair/write 默认 opencode，失败后按 policy 升级。
   - premium worker 调度必须消耗 budget。
8. concurrency fairness：
   - release 时优先唤醒等待队列。
   - per provider/model/category 限流。
   - 防止一个 category 独占所有 worker slot。
9. team/mailbox 边界：
   - 不复制 OMO tmux/team mailbox 全量能力。
   - MVP 只做 task artifact 和 GUI panel，不做 worker 之间自由聊天。

## 测试

1. `read_only_review_tasks_can_run_in_parallel`
2. `write_tasks_with_overlapping_scope_are_serialized`
3. `session_cancel_cascades_to_descendant_tasks`
4. `skip_notification_prevents_cancel_notification_storm`
5. `worker_pool_routes_review_to_non_writer_worker`
6. GPUI：Gear panel 同时显示两个 running read-only task，且 control 按钮作用到正确 task。

## 验收

- Gear 可以统管 opencode、Codex、Claude、Zed Agent/custom worker。
- 小规模并行只开放给安全的 read-only 类任务。
- 用户 cancel Gear session 时不会留下后台子 task。
- GUI 能清楚显示多个 task 的状态、artifact 和控制入口。

## 当前状态

- 已完成第一轮：`TaskManager::can_start()` 对 read-only review task 放宽了同 worker key 的并发限制；`max_parallel_workers > 1` 时，两个同 key 的 review task 可以并行起跑。
- 已完成第一轮：`read_only_review_tasks_can_run_in_parallel_with_same_key` 回归通过。
- 已完成第二轮：`parent_task_id` 任务树和 descendant cancel 已接通；`session_cancel_cascades_to_descendant_tasks` 回归通过，Gear session cancel 不再只停留在当前 worker handle。
- 已完成第三轮：写任务的显式 scope guard 已接通；重叠写 scope 会串行，`task_manager_serializes_overlapping_write_scopes` 和 `task_manager_allows_disjoint_write_scopes_with_room_in_key_budget` 回归通过。
- 已完成第四轮：独立 Gear task panel 已从 activity bar 拆出并落到 ThreadView 内，task/attempt artifact opener、packet/prompt/transcript/result/outcome、current worker output、interrupt/cancel/view output 和 filter/sort/show-more 也都接通。
- 已完成第五轮：task rows 现在会显示 summary head 和 continuation hint，completion 结果不再只停留在 event/record 层。
- 已完成第五轮：TaskManager panel 头部现在也提供 Follow Up / Steer 按钮，直接复用当前 draft 的 Gear follow-up/steer 控制路径。
- 已完成第五轮：task rows 现在还会显示 messageability 标签，便于区分 Steer / Revive / Locked。
- 已完成第五轮：provider/model 不可用 route 会在 route selection 阶段提前跳过，避免启动前的可避免 `ModelUnavailable` attempt。
- 仍未完成：更完整的 provider-aware/depth 统一 budget 仍然需要继续推进；当前已有第一版 child-depth cap 和 `max_provider_unknown_streak` 接通运行时预算，而 task panel 已能直接打开 packet/prompt/transcript/result/outcome/fallback、goal review/coordinator review/final report 和 current output，summary head / continuation hint / messageability 会直接显示在条目里，任务面板也能直接走 follow-up / steer draft 控制路径，final report 也会把 transcript/tool-events/partial-output 纳入证据链。
