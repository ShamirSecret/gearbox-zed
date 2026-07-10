# Gear Runtime 新计划：借鉴 oh-my-openagent 的原生 goal loop

## 目标

Gear 是 Gearbox 的原生 coordinator agent。用户只需要给出一个需求描述，Gear 持有 goal、计划、预算、worker 调度、审查、修复和完成判定，持续执行：

```text
plan -> dispatch worker -> inspect -> verify -> provider review -> repair/replan -> ask if goal reached
```

直到 Gear 自己认为目标完成，或因为预算、阻塞、取消、需要用户决策而停止。

这份计划借鉴 `/home/donald/文档/github/oh-my-openagent` 的运行机制，但不把 Gear 做成 opencode 插件。Gear 的控制核心必须在 `crates/gearbox_agent`，GUI 只作为原生入口和展示层，opencode/Codex/Claude/Zed Agent 都只是 worker 池成员。

## 从 oh-my-openagent 吸收的机制

### 1. BackgroundManager 不是工具包装器，而是 control plane

oh-my-openagent 的关键不是 `call_omo_agent` 或 opencode plugin API，而是 `BackgroundManager` 这一层：

- 任务先进入 `pending` queue。
- 按 concurrency key 获取并发槽。
- 创建 child session。
- 写入 attempt/session/category/model/fallback 元数据。
- 运行中可 cancel、interrupt、steer、follow-up。
- 完成、错误、取消都必须释放并发槽、清理 session、写终态。
- 结果回到父 session 后，再由父 agent 决定下一步。

Gear 对应实现：

```text
TaskManager
  -> queues_by_key
  -> running handles
  -> TaskRecord / TaskAttempt
  -> WorkerSessionHandle
  -> completion / cancellation cleanup
```

### 2. ManagedChildHandle 是 worker 抽象的中心

oh-my-openagent 把不同 runner 统一成一个 child handle：

```text
steer()
followUp()
abort()
subscribe()
waitForOutcome()
lastAssistantText()
dispose()
```

Gear 已有 `WorkerSessionHandle`，下一步必须让它从 command-backed compatibility layer 升级为真正的 managed child handle。即使 MVP 先使用 opencode command worker，也要按 session 生命周期记录状态，不能把 worker 继续当一次性命令。

### 3. Category routing 优先于 worker kind

oh-my-openagent 先解析 category，再绑定 model/fallback/tools/prompt append。Gear 也应该先判断任务类型，再选择 worker：

```text
quick -> cheap fast worker
deep -> autonomous engineering worker
review -> independent reviewer
repair -> targeted fixer
explore -> read-only code explorer
librarian -> docs/external info worker
visual -> UI/frontend worker
zed-native -> Zed Agent worker
custom -> user configured worker
```

`WorkerKind` 只是执行载体，例如 `opencode`、`codex`、`claude`、`zed_agent`、`custom`。

### 4. Continuation hook 要内化为 GoalLoop

oh-my-openagent 依赖 stop hook、subagent stop hook 和 continuation marker 唤醒父会话。Gear 不应依赖外部 hook，因为 Gear 自己就是 runtime。

Gear 的 `GoalLoop` 必须内置 continuation：

```text
after worker outcome:
  update artifacts
  run deterministic verification
  run provider-backed review
  classify stop reason
  if not complete:
    create next repair/replan task
```

### 5. Fallback retry 和 attempt history 是一等状态

oh-my-openagent 会记录 attempt、失败 model、下一 fallback model、session id，并在失败时重启 child session。Gear 也需要把 worker 失败视为 attempt 失败，而不是整个 goal 失败。

Gear 对应状态：

```text
TaskRecord
  attempts[]
  current_attempt_id
  category
  selected_worker_kind
  selected_model
  fallback_chain
  failure_kind
  retry_reason
```

### 6. Parent wake 对 Gear 来说就是下一轮 coordinator decision

oh-my-openagent 会把 child completion 通知父 agent。Gear 不需要把通知注入聊天上下文，而是写入 event ledger，并直接进入下一轮 coordinator decision。

Gear 对应实现：

```text
TaskManager completion event
  -> GoalLoop consumes outcome
  -> ReviewEngine decides complete / repair / replan / needs_user
  -> GUI streams summarized state
```

### 7. 计划里必须显式保留的 OMO 守卫

这些点已经拆到独立 phase 工单里，主计划只保留映射关系，避免后续实现时把关键语义丢掉：

- Phase 01 / Phase 05：`run_epoch` / `notified_epoch` 去重，completion 只在 epoch 前进时再次投递。
- Phase 03：`messageability` 矩阵，决定 `steer`、`revive` 还是 `not-continuable`。
- Phase 05：parent wake 的 busy 检测、缓冲和重试，避免 completion 打断用户正在看的 streaming 回复。
- Phase 06：LRU residency eviction 和 archive cap 的语义分离，`Cancelled` / `Lost` 不能被普通 FIFO 挤掉。
- Phase 07：`question: false` / tool restriction、secret-like model field scan、fallback 链长度和 no-op skip。
- Phase 09：`get_descendant_tasks()` / 级联取消，`skip_notification` 防止通知风暴。

## Gear runtime 总架构

```text
GearNativeSession
  -> GearCoordinator
      -> GoalStore
      -> ArtifactStore
      -> EventLedger
      -> BudgetController
      -> GoalLoop
          -> Planner
          -> CategoryRouter
          -> WorkerScheduler
              -> TaskManager
                  -> WorkerSessionAdapter
                      -> OpencodeWorker
                      -> CodexWorker
                      -> ClaudeWorker
                      -> ZedAgentWorker
                      -> CustomWorker
          -> VerificationRunner
          -> ReviewEngine
          -> RepairPlanner
```

职责边界：

- `GearNativeSession`：GUI/native/ACP 接入、streaming、cancel、steer、artifact opener。
- `GearCoordinator`：一个 Gear 会话的 owner，持有 goal loop 和 task manager。
- `GoalLoop`：唯一有权判断 goal 是否继续、完成、受限、阻塞或需要用户输入。
- `TaskManager`：唯一有权改变 worker task 状态。
- `WorkerSessionAdapter`：屏蔽 opencode、Codex、Claude、Zed Agent、custom command 的协议差异。
- `CategoryRouter`：把 intent、失败类型、review hint、预算映射为 category、worker、model、fallback chain。
- `ReviewEngine`：每轮 provider-backed 自审、重规划、停止原因判定。
- `VerificationRunner`：确定性 diff/scope/test/build 检查。

## 当前基线（2026-07-08）

已经完成：

- `crates/gearbox_agent` 已有本地 runtime/CLI 原型。
- `gear run "<prompt>" --workspace <path>` 可以生成 `.gearbox-agent/` 状态目录、goal/task/event ledger 和 artifacts。
- Gearbox GUI 已区分两个原生 agent：
  - `Agent`：Zed native agent。
  - `Gear`：Gearbox coordinator agent。
- GUI 的 `Gear` 会话已经接到 `gearbox_agent::runtime::Orchestrator`。
- Gear runtime events 已能流式显示到 Agent Panel。
- GUI cancel 已能触发 Gear cancellation token。
- Gear 会话的原生停止/继续控制已开始接 TaskManager control plane：`Stop Generation` 优先走 `interrupt_current_task()`，运行中的 `Send Immediately` 会优先复用当前 Gear worker handle 做 `send_follow_up`，queue `Send Now` 在 Gear 运行中也会按 queue `steer` 标记走 `steer_current_task()` / `send_follow_up_current_task()`。
- runtime 已有有限轮次 goal pursuit loop：worker -> diff -> verification -> review -> repair。
- `WorkerKind` 已覆盖 `opencode` / `opencode_session` / `codex` / `claude` / `zed_agent` / `custom`。
- `WorkerSessionAdapter` / `WorkerSessionHandle` / `WorkerStartRequest` / `WorkerOutcome` 已建立。
- `WorkerRegistry.start()` 已返回 worker session handle。
- command-backed worker pool 已拆出 `OpencodeCommandWorker` / `OpencodeSessionWorker` / `CodexCommandWorker` / `ClaudeCommandWorker` / `ZedAgentCommandWorker` / `CustomCommandWorker`。
- `OpencodeSessionWorker` 已完成 resident-command MVP：复用 opencode command 配置，在同一个 Gear managed handle 内支持 `send_follow_up` / `steer` 后续 turn，并写入 follow-up/steer prompt、stdout/stderr、result/outcome artifacts。
- resident-command backend 现已支持第一版 stale session detection / revive：取消后会把 resident handle 标记为 stale，下一次 follow-up/steer 前自动重置 token、写 revive artifact，并恢复交互。
- resident-command backend 现已支持第一版 `interrupt` 控制路径：`WorkerSessionHandle::interrupt()`、`TaskManagerControl.interrupt_current_task()`、`TaskManager::interrupt_task()` 已接入；对 resident handle 会写 `interrupt-*.md`，并在下一次 follow-up/steer 前自动 revive。
- 每个 worker task 已写 `.gearbox-agent/workers/<task_id>/outcome.json`。
- 已新增第一版串行 `TaskManager`，支持 `start` / `tick` / `wait_for` / `run_worker_task` / `cancel_task` / `list`。
- `TaskManager.start()` 已保存 running worker handle，`cancel_task()` 已能转发到 worker handle。
- `TaskManagerControl` 已建立，runtime 可把当前 running worker handle 暴露给 GUI 控制路径。
- `TaskManager` 已有第一版 pending queue、`ConcurrencyManager` 槽位、pending cancel、completed archive。
- `ConcurrencyManager` 已支持全局上限 + per-key 上限；MVP key 由 selected worker kind 和 coordinator provider/model 组成，默认仍保持全局 `max_parallel_workers=1`。
- `ConcurrencyManager` 的并发上限现在已可由配置驱动：`WorkerConfig` 携带 `max_parallel_workers` / `max_parallel_per_key`，CLI 支持 `--max-parallel-workers` / `--max-parallel-per-key`，Gear GUI env 支持 `GEARBOX_GEAR_MAX_PARALLEL_WORKERS` / `GEARBOX_GEAR_MAX_PARALLEL_PER_KEY`。
- `TaskManager` 已为每个 worker task 写入 `.gearbox-agent/workers/<task_id>/task-events.jsonl`，记录 pending/running/terminal 生命周期。
- `TaskManager` 已新增后台 worker completion dispatcher：`start_queued_task()` 启动 worker 后由后台线程等待 outcome/result，完成消息可由非阻塞 `tick()` 或阻塞 `wait_for()` 收割，并复用 `settle_running_task()` 统一收敛 terminal、fallback、archive 和 queue pumping。
- GUI `Gear` cancel 已能通过 session 上的 `TaskManagerControl` 取消当前 worker，并同时设置 Gear cancellation token。
- runtime 已从单步 worker run 改为 `TaskManager.start()` -> cancellation check -> `TaskManager.wait_for()`。
- provider-backed review prompt 已包含 task id、worker kind、worker outcome、commands、failures、outcome path、budget。
- review 输出已解析 `ROUTE_HINT` / `STOP_REASON`。
- `WorkerCategory` / `CategoryRouter` 已建立，支持 `quick` / `repair` / `deep` / `review` / `explore` / `librarian` / `visual` / `zed-native` / `custom`。
- `ROUTE_HINT` 已从简单 worker kind hint 升级为 category hint，并能按 category 偏好选择已有 worker route。
- runtime event、goal review artifact、provider-backed review prompt 已写入 `worker_category` 和 `route_reason`。
- `TaskRecord` 已新增 `attempts[]`，记录每个 managed worker attempt 的 worker kind/command/category、route hint、route reason、status、session/result/outcome/error、failure kind 和 retry reason。
- GoalLoop 已消费 TaskManager terminal metadata：`NoFallbackRoute` / `RepeatedFailureLimit` 映射为 `limited`，required worker `WorkerUnavailable` / `WorkerStartFailed` 映射为 `needs_user`。
- `STOP_REASON` 已能保守停止为 `needs_user` / `blocked` / `limited`，且不能覆盖 verification failed 为 complete。
- `gear run` CLI 已支持 `--worker-sequence` 和 per-kind command 参数。
- primary worker 现在也会正确吃到所选 kind 的 command 配置，而不再只对 opencode 默认路径生效；Gear GUI env 和 `gear run` CLI 在 `codex` / `claude` / `zed_agent` 主 worker 场景下都能复用对应 command 配置。
- command-backed worker 现在会在执行前做第一版 binary availability 预检测；像 `codex exec` / `claude -p` 这类命令若 PATH 上缺少主 binary，会产出 `no worker command` 风格的 skipped outcome，并让 TaskManager 走 `WorkerUnavailable` / fallback 路径，而不是等 shell 失败后才发现。
- `WorkerKind` 已内置第一版默认 command 模板：`codex` 会默认生成带 `--skip-git-repo-check`、`--dangerously-bypass-approvals-and-sandbox`、`-o "$GEARBOX_WORKER_LAST_MESSAGE"` 的非交互命令，`claude` 也有基础 prompt-driving 默认模板，因此 CLI / Gear GUI env 即使未显式传 command，也能生成可追踪的 worker contract。
- `WorkerOutcome` 不再只依赖 stdout/stderr 摘要；command-backed worker 现在会优先读取 `GEARBOX_WORKER_LAST_MESSAGE`，并从 `summary` / `changed_files` / `commands_run` / `known_failures` 段落解析结构化 outcome。
- Gear GUI 路径现在会读取 `LanguageModelRegistry` 当前可用模型快照，把缺失的 worker model 预先编码为 `provider/model` 级别的 unavailable entry（例如 `openai/gpt-5`、`anthropic/claude-3-7-sonnet`），并复用现有 TaskManager `ModelUnavailable` / fallback 路径；这让 Codex/Claude worker model 的 availability 不再只依赖静态 env 列表。
- Phase 6 第一轮策略也已落地：`WorkerConfig.premium_worker_budget` 可由 CLI / Gear GUI env 配置；TaskManager 会把 premium worker 超预算记为 `PremiumBudgetExceeded` 并让 GoalLoop 收敛到 `limited`。同时，带 category hint 的失败 attempt 现在会做第一版失败类型驱动升级：`repair` / `deep` 等有 hint 的 opencode 路线可自动提升到 Codex，且 route-local attempt 会跨 queue/start/worker registry 一致传递，不再因为全局 attempt 序号而选错 deep route。

仍不足：

- `TaskManager` 已有 GUI queue/control-plane structured snapshot API：`TaskManager::snapshot()` 输出 pending/running/completed/skipped/failed/cancelled 计数、task/attempt 摘要、goal artifacts root、result/outcome/fallback route-transform artifact path、summary head / continuation hint 和当前 worker output；Gear event stream 现在用这份 snapshot 去重输出 markdown。
- Gear `thread_view` 已把第一版原生 TaskManager 区块升级为独立 Gear panel：直接读取 snapshot，在独立面板内展示 task counts、最近 task/attempt、当前 worker output，并可直接打开 task/attempt `result` / `outcome` / `fallback` artifact，以及 goal 级 `Goal Review` / `Coordinator Review` / `Final Report` / `Artifacts` 入口。
- 该区块已具备第一版面板能力：`All` / `Active` / `Attention` 过滤、running/pending 优先排序，以及超过 6 条时的 `Show More` / `Show Less`。
- `TaskManagerControl` 现已支持第一版 worker interaction control plane：`current_last_output`、`cancel_current_task`、`interrupt_current_task`、`send_follow_up_current_task`、`steer_current_task`。Gear GUI 已接上 stop/send-now 的原生分支，并在独立 TaskManager panel 里提供 `Follow Up` / `Steer` / `Interrupt` / `Cancel Task` / `View Output` 按钮；goal review / coordinator review / final report 入口也已接通，后续继续收口更细粒度的 worker 控件。
- `ConcurrencyManager` 已有 provider/model keyed concurrency MVP，并且已经有 CLI/env policy 覆盖；但还没有动态预算或 team-mode 配置。
- GUI session 已持有当前 `SharedTaskManager` / `TaskManagerTickLoop` / control handle；cancel、interrupt、send-now follow-up/steer 已能打到当前 worker，pending follow-up/steer 也会在 task start 后按序 drain。Gear 消息流仍保留 markdown snapshot，同时 `thread_view` 已有独立原生 TaskManager panel；后续还缺更完整的筛选、worker 路由和专门的 artifact/output 浏览器。
- `OpencodeSessionWorker` 已支持同一 managed handle 的 follow-up/steer 兼容路径，但还不是绑定 opencode 原生长驻 session API 的 backend；普通 command worker 仍明确返回 unsupported。
- Codex/Claude/Zed Agent 目前只有 command-backed adapter 身份，还没有原生 session 协议。
- model availability 已有 MVP policy 输入：worker route 可记录 `worker_model`，CLI/env 可声明 unavailable worker models；TaskManager 会在启动 worker 前把 unavailable model 记为 `ModelUnavailable` attempt，并且 command-backed worker 也会把缺失 PATH binary 预检测为 `WorkerUnavailable`。Gear GUI 路径现在还会把 provider registry 的当前可用模型快照投影到 `provider/model` 级别的 unavailable entry，`CategoryRouter` 也已经会在 route selection 阶段跳过不可用的 provider/model route。仍未完成的是更完整的 premium/depth 联合预算，以及把 Codex/Claude/Zed Agent 从 command worker 升级为真正 session worker。
- fallback chain 已有 MVP：同一 managed task 内 failed/unavailable worker result 可追加下一 attempt 并切换到下一 category/sequence route；同类失败达到上限时会停止。
- `CategoryRouter` 仍是内置 MVP policy，还没有 CLI/env policy 覆盖；provider/model availability 只是第一版内置 skip 规则。
- worker outcome 还没有形成可订阅 event stream。
- 已有 OMO 式 model-level fallback 的 MVP policy/attempt 记录；还没有接 GUI provider registry 的实时 availability、stale session detection、loop detector、crash cleanup。

## 核心状态机

### GoalStatus

```text
planning
running
reviewing
repairing
complete
limited
blocked
needs_user
cancelled
failed
```

规则：

- 只有 `GoalLoop` 可以把 goal 标为 terminal。
- worker 不能直接让 goal complete。
- verification failed 时，provider review 不能强行 complete。
- provider review 可以 veto verification pass，要求 repair 或 independent review。
- 达到预算上限时进入 `limited`，并写明缺口和下一步。

### TaskStatus

```text
pending
running
completed
error
cancelled
interrupted
limited
skipped
```

规则：

- pending task 被 cancel 时必须从 queue 移除。
- running task 被 cancel 时必须调用 worker handle cancel/abort。
- terminal task 必须释放 concurrency slot。
- terminal task 仍可被读取 outcome。
- task 状态只能通过 `TaskManager` public API 修改。

### TaskAttemptStatus

```text
pending
running
completed
error
cancelled
interrupted
stale
```

每次 fallback retry 都是新 attempt，不覆盖旧 attempt。

## TaskManager 设计

第一目标是把当前串行 `TaskManager` 改成 OMO 式轻量 control plane。

### 数据结构

```rust
pub struct TaskManager {
    records: HashMap<TaskId, TaskRecord>,
    queues_by_key: HashMap<ConcurrencyKey, VecDeque<QueueItem>>,
    running: HashMap<TaskId, ManagedWorkerRun>,
    completed_archive: VecDeque<TaskRecord>,
    concurrency: ConcurrencyManager,
}
```

```rust
pub struct TaskRecord {
    task_id: String,
    goal_id: String,
    parent_task_id: Option<String>,
    status: ManagedTaskStatus,
    category: WorkerCategory,
    worker_kind: WorkerKind,
    worker_session_id: Option<String>,
    attempt_count: usize,
    current_attempt_id: Option<String>,
    attempts: Vec<TaskAttempt>,
    queued_at: DateTime,
    started_at: Option<DateTime>,
    completed_at: Option<DateTime>,
    prompt_path: PathBuf,
    output_path: Option<PathBuf>,
    outcome_path: Option<PathBuf>,
    error: Option<String>,
}
```

### API

```rust
impl TaskManager {
    pub fn start(&self, spec: TaskStartSpec) -> Result<TaskStartResult>;
    pub fn wait_for(&self, task_id: &str) -> Result<TaskRecord>;
    pub fn cancel_task(&self, task_id: &str, reason: Option<String>) -> Result<CancelResult>;
    pub fn interrupt_task(&self, task_id: &str, reason: Option<String>) -> Result<InterruptResult>;
    pub fn send_to_task(&self, task_id: &str, prompt: String) -> Result<SendResult>;
    pub fn list(&self, scope: TaskListScope) -> Vec<TaskRecord>;
}
```

> MVP 现状：GUI 交互控制面先通过 `TaskManagerControl` 绑定当前 `current_task`，所以 cancel / interrupt / send_follow_up / steer 都是 current-task scoped。`TaskManager` 保留的是 task_id 级别的管理语义；等 Phase 09 的 parallel worker pool 真正放开后，再把这些控制路径扩展成多 task routing。

### MVP 约束

- `max_parallel_workers = 1`。
- queue 必须存在，即使第一版只串行消费。
- `wait_for()` 不能长期持有全局 manager lock，否则 GUI cancel 无法打进来。
- worker handle 必须可被 manager 在另一个控制路径取消。
- `TaskRecord` 和 `outcome.json` 必须先写 running，再写 terminal。

## WorkerSessionHandle 设计

Gear 的 handle 参考 oh-my-openagent 的 `ManagedChildHandle`。

```rust
pub trait WorkerSessionHandle: Send + Sync {
    fn task_id(&self) -> &str;
    fn session_id(&self) -> Option<String>;
    fn send_follow_up(&self, prompt: String) -> Result<()>;
    fn steer(&self, prompt: String) -> Result<()>;
    fn interrupt(&self) -> Result<()>;
    fn cancel(&self) -> Result<()>;
    fn wait_for_outcome(&self) -> Result<WorkerOutcome>;
    fn last_output(&self) -> Option<String>;
    fn dispose(&self) -> Result<()>;
}
```

MVP：

- command-backed worker 可以把 `steer` / `follow_up` 返回 `unsupported`。
- command-backed worker 必须支持 `cancel`。
- command-backed worker 必须写 `output.md`、`stderr.log`、`outcome.json`。

后续：

- `OpencodeSessionWorker` 接 opencode session API。
- `ZedAgentWorker` 创建 native sibling/subagent thread。
- `CodexWorker` / `ClaudeWorker` 从 CLI adapter 升级为 session adapter。

## CategoryRouter 设计

Gear 不直接让 provider 输出 worker kind。provider 可以输出 route hint，但 runtime 要用 policy 做最终选择。

### WorkerCategory

```text
quick
deep
repair
review
explore
librarian
visual
zed-native
custom
```

### 默认 route policy

```text
quick      -> opencode
repair     -> opencode, codex
deep       -> codex, claude, opencode
review     -> codex, claude, zed_agent
explore    -> zed_agent, opencode
librarian  -> opencode, custom
visual     -> claude, codex, opencode
zed-native -> zed_agent
custom     -> custom
```

### MVP

MVP 先用 opencode 执行所有 write task，但数据模型必须记录 category：

```text
iteration 1 -> category quick/deep -> opencode
verification failed -> category repair -> opencode
provider requests independent review -> category review -> configured worker if available, else coordinator model
opencode repeated failure -> route fallback to codex if configured
```

### CategoryResolution

```rust
pub struct CategoryResolution {
    category: WorkerCategory,
    worker_kind: WorkerKind,
    model: Option<WorkerModel>,
    fallback_chain: Vec<FallbackRoute>,
    tools: WorkerToolPolicy,
    prompt_append: Option<String>,
    reason: String,
}
```

## GoalLoop 设计

Gear 的 goal loop 是 oh-my-openagent continuation hook 的原生替代品。

```text
create goal
write spec
write initial plan
while budget allows:
  classify next category
  resolve route and fallback chain
  start task through TaskManager
  wait task outcome
  inspect changed files
  run scope check
  run verification commands
  run provider-backed review
  ask internal question: is goal satisfied?
  if yes:
    write final report
    stop complete
  if needs user:
    write needs-user report
    stop needs_user
  if blocked:
    write blocked report
    stop blocked
  if limited:
    write limited report
    stop limited
  write repair plan
  continue
```

### Stop reason precedence

```text
cancelled > needs_user > blocked > limited > verification_failed > provider_complete
```

解释：

- 用户 cancel 永远最高优先级。
- 需要用户决策时不能继续假装自动完成。
- verification failed 永远阻止 complete。
- provider `complete` 只能在 deterministic checks 通过时生效。

## ReviewEngine 设计

每轮 review 输入：

```text
goal
spec
plan
iteration
budget
task id
category
worker kind
worker model
worker outcome summary
commands run
known failures
changed files
diff summary
scope result
verification result
previous repair requests
```

provider 输出协议：

```text
GOAL_SATISFIED: yes|no|unknown
SUMMARY: one concise sentence
REPAIR_REQUEST: focused next-worker instruction or none
ROUTE_HINT: quick|repair|deep|review|explore|librarian|visual|zed-native|custom|none
STOP_REASON: complete|limited|blocked|needs_user|none
```

规则：

- `ROUTE_HINT` 是建议，不是命令。
- `STOP_REASON=complete` 不覆盖失败 verification。
- `STOP_REASON=needs_user` 可以停止 loop，并展示缺失信息。
- 连续两轮 `unknown` 且 verification passed 时，Gear 应升级 `review` category 或进入 `needs_user`。

## BudgetController

默认 MVP 预算：

```json
{
  "max_iterations": 5,
  "max_worker_calls": 8,
  "max_premium_worker_calls": 2,
  "max_parallel_workers": 1,
  "max_child_depth": 1,
  "max_runtime_minutes": 60,
  "max_same_failure_retries": 2,
  "max_files_changed": 40
}
```

预算消耗项：

- worker call。
- premium worker call。
- provider review call。
- verification command failure retry。
- repeated same failure。
- changed file scope expansion。

预算耗尽时，Gear 写 `limited` final report，报告已完成、未完成、证据和建议下一步。

## 状态目录

保留 `.gearbox-agent/`，扩展为 control-plane 可审计结构：

```text
.gearbox-agent/
  config.json
  sessions/
    <gear_session_id>.json
  goals/
    <goal_id>.json
  tasks/
    <task_id>.json
  attempts/
    <attempt_id>.json
  events/
    <gear_session_id>.jsonl
  artifacts/
    <goal_id>/
      spec.md
      plan.md
      review-iteration-001.md
      repair-plan-iteration-001.md
      verification-iteration-001.md
      final-report.md
  workers/
    <task_id>/
      start-request.json
      prompt.md
      output.md
      stderr.log
      outcome.json
      task-record.json
  queue/
    pending.json
  archive/
    completed-tasks.jsonl
```

原则：

- worker 原始输出只写 `workers/<task_id>/`。
- Gear 判断只写 `artifacts/<goal_id>/`。
- task/attempt 是机器可读状态。
- artifacts 是人可读证据链。
- GUI 默认展示 Gear 总结，不直接倾倒 worker 原始输出。

## GUI 集成原则

GUI 不直接操作 worker handle。所有控制都走 Gear coordinator 或 TaskManager API。

第一版 markdown streaming：

```text
Gear: 创建 goal
Gear: 写入 spec / plan
Gear: task_003 queued category=repair worker=opencode
Gear: task_003 running session=...
Gear: task_003 completed
Gear: verification failed
Gear: provider review requested repair
Gear: task_004 queued category=repair worker=opencode
Gear: verification passed
Gear: provider review accepted
Gear: goal complete
```

下一步 GUI 控制：

- Gear session 持有当前 `TaskManager` 或 coordinator handle。
- cancel 调 `TaskManager.cancel_task(current_task_id)`，不是只设 cancellation token。
- 后续支持 steer/follow-up 当前 worker。
- 每个 task 显示 category、worker、status、attempt、artifact link。
- `Agent` 和 `Gear` 始终是两个 agent：
  - `Agent` 是用户直接使用的 Zed native agent。
  - `Gear` 是 coordinator，可以把 `Agent` 当 worker 调用，但不能被 worker 递归调用 Gear。

## Worker 接入策略

### Opencode

MVP 默认 worker。

阶段：

1. `OpencodeCommandWorker`：当前 command-backed packet 模式。
2. `OpencodeSessionWorker`：真实 session，支持 follow-up/cancel/wait。
3. `AcpExternalAgentWorker`：可选，把 Zed external agent/ACP 当 worker adapter，不替代 Gear runtime。

### Zed Agent

目标：

```text
Gear task -> ZedAgentWorker -> native sibling/subagent thread -> outcome -> Gear review
```

约束：

- 用户仍能直接打开 `Agent`。
- Gear 调用 `Agent` 时只发 bounded worker task。
- Zed Agent 输出只是 evidence。
- 禁止 worker 再创建 Gear goal loop。

### Codex

适合：

- 复杂工程修复。
- 独立 code review。
- 跨模块 root-cause diagnosis。
- opencode 连续失败后的升级。

### Claude

适合：

- 产品/交互方案。
- 长上下文 spec。
- 文档、体验、前端视觉审查。

### Custom

适合用户配置本地脚本、内部 agent 或实验 worker。

## 分阶段实施计划

本节保留阶段状态总览；具体修改步骤已经拆到独立工单文件，后续执行以这些文件为准：

- [Phase 00：基线冻结与计划对齐](gearbox-gear-workorders/phase-00-baseline-alignment.md)
- [Phase 01：TaskRecord 状态机与驻留语义](gearbox-gear-workorders/phase-01-state-machine.md)
- [Phase 02：TaskManager control plane 与并发释放守卫](gearbox-gear-workorders/phase-02-task-manager-control-plane.md)
- [Phase 03：Steering、messageability、queued delivery 与 revive](gearbox-gear-workorders/phase-03-steering-messageability-revive.md)
- [Phase 04：WorkerSessionHandle 与 runner 生命周期](gearbox-gear-workorders/phase-04-worker-session-runners.md)
- [Phase 05：Completion notification 与 GUI parent wake](gearbox-gear-workorders/phase-05-completion-parent-wake.md)
- [Phase 06：Lifecycle、residency、reconciliation 与 TTL](gearbox-gear-workorders/phase-06-lifecycle-residency-cleanup.md)
- [Phase 07：Category、fallback、provider/model policy](gearbox-gear-workorders/phase-07-category-fallback-model-policy.md)
- [Phase 08：GoalLoop、ReviewEngine、budget 与 stagnation guard](gearbox-gear-workorders/phase-08-goal-loop-review-budget.md)
- [Phase 09：GUI 原生 worker 池、小规模并行与级联取消](gearbox-gear-workorders/phase-09-gui-parallel-worker-pool.md)

### Phase 0：基线固定

状态：已完成。

验收：

- Gear GUI 能创建 Gear session。
- `Agent` 和 `Gear` 两个 agent 都显示。
- Gear 会话接入 `gearbox_agent::runtime::Orchestrator`。
- `cargo test -p gearbox_agent` 通过。
- Gear prompt 测试通过。

### Phase 1：TaskManager 升级为后台 control plane

状态：MVP control plane 已完成。已完成 shared `TaskManagerControl`、running handle 共享、wait 期间 control cancel、GUI Gear cancel 绑定当前 worker、pending queue、pending cancel、`ConcurrencyManager` 全局槽位 + provider/model keyed per-key 槽位、completed archive、task lifecycle event、后台 worker completion dispatcher、非阻塞 `tick()` 收割、`TaskManagerTickLoop` 后台 tick loop primitive、runtime / GUI session 生命周期接入，以及 GUI 可消费的 `TaskManager::snapshot()` 结构化 queue/control-plane observer API。`TaskManagerControl` 的 `send_follow_up` / `steer` / `interrupt` 已接入原生 GUI 交互：Gear `Stop Generation` 走 interrupt，运行中的 `Send Immediately` / queue `Send Now` 会优先复用当前 worker handle。`thread_view` 也已把第一版原生 Gear TaskManager 区块升级为独立 Gear panel、task/attempt artifact opener、当前 worker output 独立查看入口、`Interrupt` / `Cancel Task` 控制按钮，以及第一版 filter/sort/show-more 面板能力。并发上限也已经进入 CLI/env policy，可通过 `WorkerConfig` 注入到共享 `TaskManager`。后续剩更完整的 worker 路由、专门的 artifact/output 浏览器和动态预算/team-mode 调度策略。

任务：

1. 把 `TaskManager` 内部改为 queue + running handle + completed archive。（已完成 MVP）
2. 引入 `ConcurrencyManager`，MVP 先限制为 1。（已完成全局槽位 + per-key 槽位版；key 为 selected worker kind + coordinator provider/model）
3. `wait_for()` 避免直接等待 worker 进程。（已完成：worker outcome/result 由后台 completion dispatcher 等待；`tick()` 可非阻塞收割完成消息；等待期间 running handle 仍可通过 `TaskManagerControl` 取消）
4. `cancel_task()` 支持 pending queue 移除和 running handle cancel。（已完成）
5. 写 task lifecycle events。（已完成 per-task `task-events.jsonl`）
6. GUI Gear session 持有 coordinator/task manager handle，cancel 转发到当前 task。（已完成当前 worker control handle）

验收：

- pending cancel 从 queue 移除。（已覆盖）
- running cancel 调 worker handle 并写 `cancelled`。（已覆盖）
- wait 期间 GUI cancel 能生效。（已覆盖）
- terminal task 不留 running handle。（已覆盖）
- event ledger 能还原 pending -> running -> terminal。（已完成 per-task lifecycle JSONL；顶层 event ledger 汇总待后续）

### Phase 2：WorkerCategory 和 route policy

状态：MVP category router 已完成。已新增 `WorkerCategory` / `CategoryRouter`，provider `ROUTE_HINT` 已按 category 解释，`quick` / `repair` / `deep` / `review` / `explore` / `librarian` / `visual` / `zed-native` / `custom` 已有内置 worker kind 偏好。runtime event、goal review artifact、provider-backed review prompt 已记录 `worker_category` 和 `route_reason`。后续剩 route policy 外部覆盖、fallback chain、预算影响。

任务：

1. 新增 `WorkerCategory`。（已完成）
2. 新增 `CategoryRouter`。（已完成 MVP）
3. 把 provider `ROUTE_HINT` 从 worker kind hint 改为 category hint。（已完成）
4. route artifact 写明选择原因、fallback chain、预算影响。（已完成 route reason；fallback/budget 待 Phase 3/6）
5. CLI/env 支持 route policy 覆盖。（待后续）

验收：

- `quick` 默认 opencode。（已覆盖）
- `repair` 默认 opencode，失败后可 fallback codex。（已覆盖 failed result fallback）
- `review` 默认 coordinator provider 或 Codex/Claude。（已按 Codex/Claude/ZedAgent worker 偏好覆盖 worker route；coordinator provider 已用于 review hook）
- route reason 写入 review artifact。（已完成）

### Phase 3：Attempt / fallback retry

状态：MVP 已完成。`TaskRecord.attempts[]` 已记录 pending/running/terminal attempt history，包含 worker kind/command/model、route category/reason、result/outcome/error、`failure_kind` 和 `retry_reason`。`TaskManager` 已能在同一 managed task 内把 failed 或 unavailable worker result 追加为失败/不可用 attempt，并切换到下一条不同 category/sequence route 继续执行；启动失败路径也会尝试追加下一 attempt。worker route 可携带 `worker_model`，config 可声明 unavailable worker models；不可用 model 会在 worker 启动前记录为 `ModelUnavailable` attempt，并 fallback 到下一 route 或通过 synthetic skipped result 进入 no-fallback/limited 路径。无不同 fallback route 时会标记 `NoFallbackRoute`，同类失败达到 `MAX_SAME_FAILURE_RETRIES=2` 时会标记 `RepeatedFailureLimit`。GoalLoop 已把 `NoFallbackRoute` / `RepeatedFailureLimit` 映射到 `limited`，把 required worker unavailable/start failed 映射到 `needs_user`。下一步是把 model availability 接到 GUI/provider registry 和预算策略。

任务：

1. 新增 `TaskAttempt`。（已完成 MVP）
2. worker 启动失败或 model unavailable 时创建下一 attempt。（已完成 MVP）
3. 记录 failed model、failed session、next worker/model。（worker kind/command/model/category/route、failure kind、retry reason 已记录；真实 provider session/model id 待后续）
4. 相同 failure 达到上限时进入 `limited` 或 `blocked`。（已完成 worker-level repeated limit -> `limited`）

验收：

- attempt history 可读。（已覆盖）
- fallback 不覆盖旧 outcome。（已覆盖 failed worker result）
- fallback retry 后 Gear 继续同一 goal loop。（已覆盖 failed/unavailable worker result）
- no-fallback 终态有机器可读 failure kind。（已覆盖）
- repeated-failure 终态有机器可读 failure kind。（已覆盖）
- GoalLoop 消费 terminal metadata 并停止到 `limited` / `needs_user`。（已覆盖）

### Phase 4：真正 session worker

状态：MVP 已完成，并补上了 resident-command 版本的 revive / stale detection / interrupt 控制路径。`WorkerSessionHandle` 已是 `Send + Sync`，并新增了 `abort` / `dispose` / `subscribe` / `wait_for_idle` 入口；command-backed worker 已支持 cancellation token 和 `last_output` cache；`TaskManagerControl.current_last_output()` 已能读取当前 worker handle 的最近输出；unsupported `send_follow_up` / `steer` 会明确返回错误。`OpencodeSessionWorker` 已作为 resident-command MVP 接入：它复用 `--opencode-command`，在同一个 managed handle 内执行 follow-up/steer turn，并把每轮 prompt、stdout/stderr、result/outcome 写入 artifacts。现在还会写 `transcript.jsonl`、`tool-events.jsonl`、`partial-output.md`，并由 `TaskManager` 持有 subscription token。交互式 resident handle 在取消或 interrupt 后会进入 stale 状态，下一次 follow-up/steer 前自动 revive，并写 `interrupt-*.md` / `revive-*.md` artifact。

任务：

1. 完善 `WorkerSessionHandle: Send + Sync`。（已完成）
2. command worker 支持可取消进程和 last output cache。（已完成）
   - control path 可读取当前 worker `last_output`。（已完成）
3. `OpencodeSessionWorker` 支持 resident-command session MVP。（已完成）
4. `send_follow_up` / `steer` API 在 session worker 中可用。（已完成）
5. 后续把 resident-command backend 替换为 opencode 原生长驻 session API，并补齐真正的 opencode-native `interrupt`；当前只有 resident-command 版本的 interrupt/revive/stale detection。
6. Phase 4 follow-up：补齐 OMO `ManagedChildHandle.subscribe()` 等价的 worker event/transcript stream，让 `TaskManager` 能订阅 worker 中间事件、tool call、partial assistant text，并把它们写成可审计 artifact / GUI snapshot，而不是只等 `last_output` 或最终 `outcome.json`。（第一版已落地：resident handle 现在会发 coarse `transcript.jsonl` / `tool-events.jsonl` / `partial-output.md`，TaskManager 也持有 subscription token；真正的 tool call / assistant delta 级 streaming 仍待补齐）
7. Phase 4 follow-up：把 pending task 的 `send_follow_up` / `steer` 做成 OMO 式 queued delivery，任务真正 start 后按顺序 drain。（已完成）
8. Phase 4 follow-up：把 terminal resident task revive 做成正式语义。OMO 允许 terminal resident task 在同一 session 上 `followUp` 并递增 run epoch；Gear 当前只保证 running 期间复用 session，settled 后继续开新轮仍未定义。

验收：

- opencode session worker 能被 start、cancel、wait。（start/wait/follow-up/steer 已覆盖；cancel 复用 command handle 取消路径）
- follow-up 能继续同一个 managed worker handle。（已覆盖 resident-command MVP）
- cancel 后下一次 follow-up/steer 能 revive resident handle，而不是永久沿用已取消 token。（已完成 resident-command MVP）
- interrupt 控制链路已存在，resident handle 会写 interrupt artifact，并在下一次 follow-up/steer 前 revive。（已完成 resident-command MVP）
- unsupported 能清楚回报，不静默吞掉。（command-backed worker 已覆盖）
- worker event/transcript stream 能还原 worker 中间行为，review 不只能依赖最终 stdout/outcome。（待 Phase 4 follow-up）
- pending follow-up/steer 能在 task start 后按序投递；terminal resident task 能 revive 同一 session，并在 attempt/run epoch 中可追踪。（terminal revive 已完成，queued delivery 已完成）

### Phase 5：ZedAgentWorker

任务：

1. 调研 native sibling/subagent thread 创建接口。
2. 实现 `ZedAgentWorker`。
3. 输出转为 `WorkerOutcome`。
4. 禁止递归 Gear。

验收：

- Gear 能后台创建 bounded Zed Agent worker task。
- Zed Agent worker 完成后 Gear 继续 verification/review。
- GUI 仍清楚区分 `Agent` 和 `Gear`。

状态：

- 已完成第一轮 native backend 骨架：`WorkerRegistry` 现在支持注入 `NativeWorkerBackend`，`zed_agent` route 不再被硬绑死在 command worker 上；没有注入 backend 时仍回退到现有 `ZedAgentCommandWorker`，保证 CLI / 非 GUI 路径不受影响。
- Gear GUI 路径现在会为当前 Gear session 建一个 session-scoped native Zed worker dispatcher。`zed_agent` route 会在 app 线程里创建真实的 Zed subagent session，执行 bounded prompt，再把结果写回 `.gearbox-agent/workers/<task_id>/result.json` / `outcome.json`，让后续 verification/review 继续走现有 runtime。
- 已有第一版递归保护：Gear 调用的 native worker 固定注册为 `ZED_AGENT_ID` subagent，而不是再起一个 Gear session，因此不会在 worker 层再次进入 Gear orchestrator。
- 现已补上第二轮 resident-session 语义：当 native Zed worker 仍在运行时，`send_follow_up` 会把新 prompt 排进同一个 subagent session 的下一轮，`steer` 会同时要求当前回合在下一个 boundary 提前收口，然后继续下一轮 prompt。最终 `wait_for_result()` 会在“当前回合结束且交互队列清空”后再收口，而不是第一轮就直接完成。
- native worker dispatcher 已抽成可复用 helper，并新增 GPUI 回归测试覆盖生产 dispatcher + `GearZedWorkerBackend` 组合：第一轮 Zed Agent worker 运行中发送 `send_follow_up` / `steer` 会复用同一个 native subagent session，后续输出会写入同一 task 的 `follow-up-1.md`、`steer-2.md`、`last-message.md`、`result.json` / `outcome.json`。
- 当前边界：运行中的 `last_output` 仍然只暴露最近一次已完成回合的最终文本，还没有接成增量流；follow-up/steer 也只保证在 managed task 仍处于 running 状态时可复用同一个 native session，不覆盖“任务已经 settled 之后再继续开新轮”的语义。

### Phase 6：Codex / Claude worker pool

任务：

1. 补全 Codex CLI 默认 command 配置和 outcome parser。
2. 补全 Claude CLI 默认 command 配置和 outcome parser。
3. 加入 premium worker budget。
4. route policy 可按失败类型升级 worker。

验收：

- `GEARBOX_GEAR_WORKER_SEQUENCE=opencode,codex` 可在第二轮切 Codex。
- premium worker 超预算时进入 `limited` 或 `needs_user`。

状态：

- 已完成：Codex/Claude 默认 command 模板、worker last message 结构化 outcome parser、Gear GUI provider registry snapshot -> `provider/model` unavailable entry、`premium_worker_budget`、`PremiumBudgetExceeded -> limited`、以及带 category hint 的失败类型驱动 route upgrade 第一版。
- 当前策略边界：自动升级只对显式带 `ROUTE_HINT` / category hint 的任务生效，避免无 hint 的默认任务隐式依赖本机 premium CLI；premium budget 当前按 premium attempt 计数，显式 worker pool 序列仍受预算约束。
- 下一步：把 provider-backed review 的 `ROUTE_HINT` / `STOP_REASON` 更深地接到 premium/depth 预算、worker 池升级链和 independent review worker 选择。

### Phase 7：ReviewEngine 强化

任务：

1. 每轮 review 输入加入 category、attempt、fallback、failure classifier。
2. provider review 可以要求 independent review worker。
3. 连续 unknown/同类失败触发 route upgrade。
4. final report 必须引用 verification/review/artifact 证据。
5. Phase 7 follow-up：引入 OMO `ultrawork` / Oracle 风格的 independent reviewer gate。高风险、用户要求审查、或 provider review 要求 `ROUTE_HINT=review` 时，worker/coordination 的 completion claim 不能直接 complete，必须由独立 reviewer worker 产出可引用 evidence。
6. Phase 7 follow-up：把 fallback policy 从 route sequence MVP 升级为 provider/model policy：跳过 unreachable provider、跳过 no-op fallback、记录 provider/model transform、previous session、failed model、next model，并把这些事实输入 review。
7. Phase 7 follow-up：把 provider-backed review、independent reviewer、fallback retry、premium/depth budget 合成一个统一 policy，而不是分别散落在 evaluation、TaskManager 和 worker router 中；当前已把 `max_provider_unknown_streak` 显式并入 budget 控制器，后续继续把 provider/model fallback 细节和 route upgrade policy 收口到同一层。
8. Phase 7 follow-up：修正 `nearest_fallback` 语义。`nearest_fallback` 只能表示“下一条可尝试的不同 route”，不能在没有可用 fallback 时回填当前 selected route；attempted route / unavailable route 必须单独记录，避免 review 和 GUI 误以为还有 fallback 可走。

验收：

- verification passed + provider says no -> 继续 repair。
- verification failed + provider says yes -> 不能 complete。
- provider route hint 改变下一轮 category。
- independent reviewer gate 能阻止“自称完成但未经审查”的高风险 complete。（待 Phase 7 follow-up）
- fallback artifacts 能显示 skipped unreachable provider、skipped no-op fallback、failed model、next model 和 previous session。（待 Phase 7 follow-up）
- 没有不同 fallback route 时，resolution/result/artifact 明确写 `nearest_fallback: none`，而不是把当前 route 当成 fallback。（待 Phase 7 follow-up）

状态：

- 已完成第一轮：provider-backed review 输入现在已经带上 `worker_attempt`、attempt 总数、`failure_kind`、`retry_reason`、fallback history 摘要；这让 review hook 不再只看到单轮 worker 成败，而能看到 route/fallback 上下文。
- `CategoryRouter` 现在也会下发 category-scoped `prompt_append` 和 `WorkerToolPolicy`，并把 `GEARBOX_GEAR_WORKER_PROMPT_APPEND` 作为用户附加说明合并到 worker packet；review/explore/write 的工具策略边界因此开始显式化。
- worker prompt 和 coordinator review prompt 都接了 sanitized model metadata block，`sanitize_model_fields()` 现在真正用于写入前清洗，而不是只停留在单测里。
- fallback retry 现在会写 `workers/<task_id>/route-transform-*.md` artifact，并在 `goal-review-iteration-*.md` 里引用 fallback history，包含前后 attempt / provider / model / session / decision。
- `queue_next_attempt` 的 no-op fallback 现在会通过共享 route identity canonicalization 比较 `worker_kind` / `worker_model` / `worker_command`，避免重复回到同一 route，并且 provider/model 不可用判断也会把 provider id case-insensitive、model punctuation canonicalize 之后再比较。
- `ROUTE_HINT=review` 现在已经有 runtime 语义：当 verification 已通过但 coordinator review 仍要求独立审查时，GoalLoop 不会直接 complete，而会继续进入下一轮，并按 `review` category 选择 worker；下一轮 prompt 也会改成独立审查请求，而不是沿用 repair prompt。
- `ROUTE_HINT=review` 现在即使伴随 `goal_satisfied: yes` 也会触发至少一轮独立 review worker，避免 provider 抢先给出 completion claim。
- final report 的第一版强制证据引用约束也已接上：`final-report.md` 现在会显式输出 `Evidence Chain`，列出 worker packet/prompt/transcript/result/outcome 以及 spec/plan/verification/review 等 task evidence 路径，同时新增 `Decision` 区块把停止原因和下一步建议写出来，不再只给结论性摘要。
- `CoordinatorReviewInput` 现在会携带 worker transcript head/tail 和 category resolution result，`collect_context_risk_texts` 也会读取 `transcript.jsonl` / `tool-events.jsonl` / `partial-output.md`，review / budget 不再只看最终摘要；transcript/tool-event 流的截断检测已经能识别未正常 finish 的 turn、未 finish 的 tool call、以及 partial output 落盘。
- `max_provider_unknown_streak` 现在已经从 Gear CLI / GUI env 接到 `Goal.budget`，不再只靠内部默认值。
- 连续 unknown / 同类失败的第一版升级策略也已接上：verification passed + provider `unknown` 不再直接 complete；第一次 `unknown` 会继续一轮，第二次 `unknown` 会升级到 `review`，否则进入 `needs_user`。连续相同 `failure_kind` 也会按 `repair/explore -> deep -> review` 方向升级 route，而不是原地空转。
- 仍未完成：真正独立于 command worker 的 review session protocol、OMO Oracle/ultrawork 风格的 reviewer gate，以及把这套 route upgrade 进一步和 premium/depth budget、provider-aware availability 串成统一 policy；`max_provider_unknown_streak` 现在已先收进 budget 控制器，作为 provider-aware review 的第一版显式阈值。

### Phase 8：Resilience

任务：

1. stale session detection。
2. repetitive failure detector。
3. process/crash cleanup。
4. completed archive size limit。
5. orphaned worker cleanup。
6. Phase 8 follow-up：补 OMO 式 no-progress / stagnation detector。Gear 应跟踪连续无有效 diff、无 tool/output 进展、相同失败重复、verification 无变化等信号，避免在“看似还在循环但没有实质进展”的状态里消耗预算。第一版已落地：`detect_stagnation()` 产出的 `no_progress_signals` 现在会进入 coordinator review prompt 和 goal review artifact，后续继续把这些信号收口进 token-limit / context-compaction guard。
7. Phase 8 follow-up：补 token-limit / context-compaction guard。发生 token limit、上下文压缩、会话 agent 信息不可靠、或 worker/session 状态不可判定时，Gear 应保守进入 repair/replan/needs_user，而不是继续注入下一轮。第一版已落地：`detect_context_risk_signals()` 的信号会阻止 repair/review/completion 的继续决策，completion 分支也会在 context 风险存在时暂停并要求 user 介入。
8. Phase 8 follow-up：如果 GUI 后续要把 worker completion 主动注入同一聊天线程，需要吸收 OMO parent-wake 的竞态保护：按 parent session 串行化通知、检测用户消息/assistant/tool 活动、去重、失败重排，避免和用户新 prompt 或已有 assistant turn 竞争。第一版已落地：`ConversationView::notify_with_sound()` 现在会在 root thread 仍在生成、compacting、等待确认、存在 in-progress tool call 或 queued message 时将 completion notification 入缓冲；`flush_pending_notifications()` 在真正 drain 前会重新检查 root thread busy state，只有 idle 才 flush，避免 buffered completion 因状态变化被提前弹出。

验收：

- worker crash 不导致 goal loop 无限等待。
- stale running task 可转为 error/limited。
- archive 不无限增长。
- no-progress/stagnation 第一版已进入 coordinator review prompt 和 goal review artifact；后续继续把它收口成预算消耗、route upgrade、needs_user 或 limited，而不是无限 repair。（待 Phase 8 follow-up）
- token limit / compaction / session state 不可靠时不会盲目继续下一轮；completion claim 也会被 context guard 拦住。（待 Phase 8 follow-up）
- GUI completion notification 不会和用户输入或现有 assistant turn 竞争；flush 时必须重新检查 root busy state。（待 Phase 8 follow-up）

状态：

- 已完成第一轮：`TaskManager.wait_for()` 不再无限阻塞 channel，而是按短轮询间隔持续 `tick()`；`tick()` 现在会主动 sweep stale running task。超过当前 MVP 超时阈值的 running worker 会被标记为失败并写回 `task-record.json`，而不是让 Gear 永久卡死在 `wait_for()`。
- stale / timeout 类失败也不再只会把整个 run 硬崩掉：它们会复用现有 fallback 机制，能继续下一条 route 就继续，不能继续时再通过 `NoFallbackRoute` / `limited` 或 error 收口。
- stale timeout 现在已经从硬编码常量提升到第一版 runtime policy：`WorkerConfig.stale_task_timeout_secs` 会从 CLI / Gear GUI env 进入 `TaskManager`。同时，`tick()` 也会做第一版 orphaned in-memory state cleanup，清掉“有 running/queued 状态但没有 record”的孤儿任务状态，避免 control plane 越跑越脏。
- runtime 启动时现在也会扫描 `.gearbox-agent/workers/*/task-record.json`，把上次异常退出遗留的 `pending/running` record 收口成 `Lost`，同时保留 `WorkerStartFailed` 这类 failure kind 并追加 lifecycle event，避免跨进程重启后残留假运行态。
- 已有 completed archive cap 仍保留 `100` 条上限。
- 仍未完成：更细的 stale session detector、真正的 worker process cleanup、带真实 session/handle 探测的跨进程 orphaned worker cleanup、完整 parent-session 串行通知队列、failure reorder，以及 dedicated completion panel；no-progress/stagnation 和 token/context guard 已有第一版，后续继续把它们收口到预算和 route policy。`delivery retry` 已有第一版短退避。

### Phase 9：小规模 team/parallel mode

非 MVP。

任务：

1. `max_parallel_workers > 1`。
2. read-only explore/review task 并行。
3. write task 默认串行。
4. task dependency 和 mailbox artifact。

验收：

- 两个 read-only explore task 可并行。
- write task 不并行改同一 scope。

## 立刻执行的下一步

当前顺序不是重新跑 Phase 01-03，而是收口仍未完全闭合的 follow-up，优先级如下：

1. [Phase 04：WorkerSessionHandle 与 runner 生命周期](gearbox-gear-workorders/phase-04-worker-session-runners.md)：补 worker event/transcript stream、显式 `dispose()` / `abort()`、`wait_for_idle()` 和真正 session runner 语义。
2. [Phase 05：Completion notification 与 GUI parent wake](gearbox-gear-workorders/phase-05-completion-parent-wake.md)：补 completion epoch 去重、取消/中断不异步通知、GUI parent busy/buffer/retry；runtime MVP 已接上 turn-end flush，notification delivery failure 也会写回 `notification_failed_epoch`，completion 内容已带 final response head / continuation hint / artifact links，GUI completion popup 现在会在 root thread 忙碌时缓冲，并在 idle flush 前再次检查 busy state；短退避 retry 已接入主链路，后续再补更精确的 parent session 串行化、failure reorder 与 dedicated panel。
3. [Phase 06：Lifecycle、residency、reconciliation 与 TTL](gearbox-gear-workorders/phase-06-lifecycle-residency-cleanup.md)：补统一 destroy port、LRU residency、启动 reconciliation、TTL cleanup；当前已补上 `TaskManager` session shutdown drop cleanup、running handle 优先的 best-effort stop/cancel/abort/dispose、`task_record_paths` runtime 索引、dispose lifecycle event，以及 shutdown 时把 pending/running current 降级为 `Lost`。后续不要把普通 cancel/interrupt 强行改成 destroy，因为 Phase 03 仍要求 `Interrupted + Resident -> Revive`；destroy 只负责最终释放 residency。
4. [Phase 07：Category、fallback、provider/model policy](gearbox-gear-workorders/phase-07-category-fallback-model-policy.md)：补 secret-like model field scan、category `prompt_append`、provider/model fallback chain、no-op/unreachable skip，并修正 `nearest_fallback` 只能指向下一条不同 fallback route；当前实现已对齐这一语义。
5. [Phase 08：GoalLoop、ReviewEngine、budget 与 stagnation guard](gearbox-gear-workorders/phase-08-goal-loop-review-budget.md)：补 robust review parser、independent reviewer gate、统一 policy、no-progress/token/context guard；当前已补上 parser warning artifact、fallback history、GoalDecisionPolicy、budget snapshot、child-depth/runtime budget、artifact-backed context guard、worker-output stagnation signal、coordinator review no-progress evidence 和 final report 的 Decision 区块，后续继续收口更强的 token 信号来源。
6. [Phase 09：GUI 原生 worker 池、小规模并行与级联取消](gearbox-gear-workorders/phase-09-gui-parallel-worker-pool.md)：在前面状态机和 lifecycle 稳定后，再开放并行、descendant cancel、完整 worker pool 和独立 GUI panel；当前已完成 read-only review task 的同 key 并行放宽、`parent_task_id` 任务树级联取消，以及显式写作用域 guard；独立 Gear panel 已从 activity bar 拆出并落到 ThreadView 的专用面板里，provider/model 不可用 route 也会在选择阶段提前跳过，goal review / coordinator review / final report 浏览入口也已接通，后续主要继续收更完整的 provider/depth 统一策略和 provider-aware review 阈值收口。

## 不做什么

MVP 不做：

- 不把 Gear 做成 opencode 插件。
- 不让 opencode ACP external agent 成为 Gear 控制核心。
- 不默认并行多个写代码 worker。
- 不默认自动提交或推送 git。
- 不让 worker 决定 goal complete。
- 不复制 oh-my-openagent 的 tmux/team mailbox 全量功能。
- 不把 user-facing `Agent` 和 coordinator `Gear` 合并成一个 agent。

## 最终验收标准

Gear runtime 完成时必须满足：

- 用户一句话创建 goal/spec/plan/task ledger。
- Gear 能按 category 选择 worker 和 model。
- opencode 是默认 worker，但不是 runtime 的唯一能力来源。
- Gear 能调度 Zed Agent/Codex/Claude/custom worker。
- 每轮都有 deterministic verification 和 provider-backed review。
- Gear 能根据 review repair/replan，并继续 goal loop。
- Gear 只在 complete、limited、blocked、needs_user、cancelled、failed 等明确状态停止。
- 所有完成判定都有 artifacts 证据链。
- GUI 中 `Agent` 与 `Gear` 两个原生 agent 始终可区分。
