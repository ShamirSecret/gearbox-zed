# Gear Agent Plan vs oh-my-openagent 对比审查

> 2026-07-09。对比对象：`docs/gearbox-gear-agent-plan.md`、`crates/gearbox_agent/` 实际实现、`/home/donald/文档/github/oh-my-openagent/` 源码。

---

## 总体评价

计划的方向和架构借鉴是正确的：BackgroundManager → TaskManager、ManagedChildHandle → WorkerSessionHandle、CategoryRouter → CategoryRouter、GoalLoop → OMO continuation hook 替代。**没有重大偏离。**

以下是细节层面的对齐偏差和遗漏。

---

## 一、计划描述与代码实现的不一致

### 1.1 计划未反映已完成的类型

计划"当前基线"段未提及以下**代码中已存在**的类型和模块：

| 代码中的实际类型 | 计划中提及？ | 差距 |
|-----------------|------------|------|
| `CoordinatorReviewHook` / `CoordinatorReviewInput` / `CoordinatorReview` | 未提及 | 该 hook 是 ReviewEngine 的实际实现形式，计划只在 Phase 7 抽象描述 |
| `TaskManagerTickLoop` | "后台 tick loop primitive" 含糊提及 | 计划未描述其生命周期（如何在 GUI session 启动/停止） |
| `TaskFailureKind` 枚举（10 个变体） | 计划 Phase 3 部分提及 | 计划未列出完整枚举 |
| `WorkerStartRequest` / `WorkerOutcome` | 未提及 | 这是 worker handle 的输入/输出契约 |
| `TaskManagerControl` 暴露的 4 个方法 | Phase 1 提到"已接入"，但未列出完整 API | 实际 API 更大，见下方 |

**实际 `TaskManagerControl` API（代码中）：**
```rust
pub fn current_last_output(&self) -> Option<String>
pub fn cancel_current_task(&self, reason: Option<String>) -> Result<CancelResult>
pub fn interrupt_current_task(&self, reason: Option<String>) -> Result<InterruptResult>
pub fn send_follow_up_current_task(&self, prompt: &str) -> Result<SendResult>
pub fn steer_current_task(&self, prompt: &str) -> Result<SendResult>
```

**计划描述的 API（行 322-329）：**
```rust
pub fn start(&self, spec: TaskStartSpec) -> Result<TaskStartResult>;
pub fn wait_for(&self, task_id: &str) -> Result<TaskRecord>;
pub fn cancel_task(&self, task_id: &str, reason) -> Result<CancelResult>;
pub fn interrupt_task(&self, task_id: &str, reason) -> Result<InterruptResult>;
pub fn send_to_task(&self, task_id: &str, prompt) -> Result<SendResult>;
pub fn list(&self, scope: TaskListScope) -> Vec<TaskRecord>;
```

**差异：** 计划中的 API 是 `task_id` 级别（多个 task），实现中是 `current_task` 级别（单个 task）。这是合理的 MVP 简化（`max_parallel_workers=1`），但计划应注明这是 MVP 约束的结果。

### 1.2 `DEFAULT_MAX_ITERATIONS` 从 2 → 5

计划 Budget 节：`"max_iterations": 5`。
计划 Phase 0 基线描述：`DEFAULT_MAX_ITERATIONS: usize = 2`。
实际代码（`runtime.rs:28`）：`pub const DEFAULT_MAX_ITERATIONS: usize = 5;`

已修复对齐 ✓。

---

## 二、OMO 核心机制的遗漏

以下是 OMO 中**有、计划中未提及**的机制。按缺失的影响程度排序。

### 2.1 ❗ `run_epoch` / `notified_epoch` — 父会话去重通知

**OMO 机制：**
- `TaskRecord` 有 `run_epoch: number` 和 `notified_epoch: number`
- 每次 worker revive（`followUp` 后重启新 turn），`run_epoch++`
- 父会话只被通知 `notified_epoch < run_epoch` 的结果，避免重复通知
- 这是 OMO 避免"同一 worker 多次完成时父会话被重复唤醒"的核心机制

**Gear 现状：** 没有 epoch 追踪。event stream 每次 completion 都会 emit，依赖 GUI 端去重。

**影响：** 当 worker 经历 revive → 完成 → revive → 完成 多轮后，GUI 会收到多次相同 worker 的完成通知，可能显示为"连续多个相同 task completion"。

**计划中提及：** Phase 4 follow-up 提到"递增 run epoch"，但未描述通知去重机制。

### 2.2 ❗ `notifyParentSession()` — 忙碌会话检测 + 延迟投递

**OMO 机制（三层通知系统）：**
```
PendingQueue(100ms debounce) → DispatchedTracker(5s recovery window) → SessionInspector
```

- `SessionInspector.isSessionActive()`: 检查父会话是否正在 streaming、是否有用户消息进行中、是否有 tool call 刚完成
- 如果父会话忙碌：**延迟投递**，等待下一个 tool-call boundary 或 idle 状态
- 如果父会话 idle：立即投递
- 超时后 requeue 重试

**Gear 现状：** 使用 `async_channel::unbounded` 无缓冲通道，事件直接写入 markdown stream。没有忙碌检测，没有延迟/重试。

**影响：** 当用户正在与 `Agent` 交互（流式输出中）时，Gear worker 完成通知会**打断**用户正在看的 assistant 回复。OMO 的设计避免了这种打断。

**计划中提及：** 行 851-852 的 Phase 8 follow-up 提到 parent-wake 竞态保护，但没有明确描述忙碌检测。

### 2.3 ❗ `messageability` — 任务可消息性判定

**OMO 机制：**
`packages/senpi-task/src/state/messageability.ts` 判断任务能否接收 `steer` / `followUp` / `continue`：
- 只有 `running` 状态的任务可接收 `steer`（注入 mid-turn 指导）
- `running` 或 `completed` 状态的任务可接收 `followUp`（completed 时会 revive）
- `cancelled` / `error` / `interrupted` 状态不可消息
- 某些 terminal 状态只可 `continue`（带新 prompt 重启，重置 run_epoch）

**Gear 现状：** 实现中 `send_follow_up_current_task` / `steer_current_task` 直接打到 running handle，没有状态检查。如果 worker 已经 settled，调用会静默失败。

**计划中提及：** 无。

### 2.4 ❗ `stop(sessionID)` 级联取消

**OMO 机制：**
`packages/omo-opencode/src/hooks/stop-continuation-guard/hook.ts:stop(sessionID)`：

1. 将 session 加入 `stoppedSessions` Set
2. 调用 `backgroundManager.getAllDescendantTasks(sessionID)` — **递归**找到所有子/孙/曾孙 task
3. 对每个 running/pending 子 task：**全部取消**，`skipNotification: true`（防止通知风暴）
4. 写入 continuation marker 文件

**Gear 现状：** `cancel()` 只取消 `current_task`。

**影响：** MVP 无问题（`max_parallel_workers=1`）。但当 `max_parallel_workers > 1` 时（Phase 9），一次 Gear cancel 可能不会停止一起运行的其他 parallel worker。

**计划中提及：** 未直接提及。Phase 9 的 write task 串行设计隐含了此需求。

### 2.5 ❗ Per-agent tool restriction 和 `question: false`

**OMO 机制：**
- 每个 agent 有工具掩码：`{ task: false, call_omo_agent: true, question: false, ... }`
- `question: false` **阻止 worker 向用户提问**，避免 worker 卡在 Q&A 循环
- `task: false` 阻止 worker 无限递归创建子任务

**Gear 现状：** 没有工具限制概念。Worker 可以执行为其配置的任何 shell 命令。

**影响：** 如果 Worker 是一个交互式工具（带 prompt 的 opencode terminal），它可能进入等待用户输入的交互状态，阻塞 Gear loop。

**计划中提及：** 不做什么节提到"不让 worker 决定 goal complete"，但没提阻止 worker 向用户提问。

### 2.6 ❗ LRU residency eviction

**OMO 机制：**
`packages/senpi-task/src/lifecycle/residency.ts:admitResident()`：
- 默认上限 `residency_max_children = 8`
- 达到上限时：**LRU 驱逐最老的 terminal resident**
- 被驱逐的 resident → `evicted` 状态 → 资源释放

**Gear 现状：** `completed_archive` 有 100 条上限（Phase 8），但不是 LRU、不区分 terminal 类型、不驱逐 running handle。

**计划中提及：** Phase 8 提到"completed archive size limit"，但未描述 LRU 驱逐。

---

## 三、OMO 机制的简化（有意为之，属合理差异）

以下 OMO 有但计划未采纳，**属于有意的架构差异，不是遗漏**：

| OMO 机制 | 计划不采纳的原因 | 合理性 |
|----------|----------------|--------|
| `restart` 语义（从失败 attempt 的重启点继续） | 简化：失败 = 新 attempt 从零开始 | ✅ 合理简化 |
| `pid` 追踪 + RPC child process 管理 | 全 in-process，无需 RPC | ✅ 架构差异 |
| checkpoint/restore | 超出 MVP 范围 | ✅ |
| TMUX team mode / mailbox artifact | 明确列为"不做什么" | ✅ |
| `call_omo_agent` tool 作为 opencode plugin API | Gear 不是 opencode 插件，走自己的 runtime | ✅ 核心架构差异 |
| OMO 的异步事件驱动 pattern | Gear 用同步循环 + background_spawn，更简单 | ✅ 合理的 Rust 适配 |
| 多 provider availability tracking | MVP 只有 PATH binary 检测 | ⚠️ 需后续补齐 |

---

## 四、计划内部的不一致性

### 4.1 `ROUTE_HINT` / `STOP_REASON` 协议与 OMO 的差异

**计划描述（行 500-512）：**
```
GOAL_SATISFIED: yes|no|unknown
SUMMARY: one concise sentence
REPAIR_REQUEST: focused next-worker instruction or none
ROUTE_HINT: quick|repair|deep|review|explore|librarian|visual|zed-native|custom|none
STOP_REASON: complete|limited|blocked|needs_user|none
```

**OMO 的实际做法：** `<system-reminder>` 注入 + 工具调用结果解析。OMO 不要求 provider 输出固定格式的 key-value 对，而是通过 tool call 或 system reminder 做路由。

**评估：** 计划的固定格式协议是合理的 MVP 设计，但应注意——在 `language_model` API 返回的文本流中可靠解析这些 key 存在挑战（模型输出格式不保证结构稳定）。计划没有描述解析容错策略（如大小写不敏感、部分 key 缺失、异常输出）。

### 4.2 Phase 1 baseline 中 `TaskManager.snapshot()` 与 plan API 不一致

**代码实际返回的 `TaskSnapshot`：** pending/running/completed/skipped/failed/cancelled 计数 + task 摘要列表 + attempt 摘要

**计划描述的 `list(scope)`：** 接受 `TaskListScope` 过滤参数，返回 `Vec<TaskRecord>`

**差异：** `snapshot()` 去重摘要输出，`list()` 返回全量记录。两个都是有效 API，但计划未区分两者用途。

---

## 五、具体修正建议

### 5.1 紧急：补充缺失的 OMO 概念

| 优先级 | 应补概念 | 建议添加位置 |
|--------|---------|------------|
| P1 | `run_epoch` + 去重通知 | 计划 WorkerSessionHandle 设计节，或新增 Phase 4.5 |
| P1 | Parent-wake 忙碌会话检测 | Phase 1 follow-up 或新增 Phase 8.1 |
| P1 | `messageability` 状态检查 | 计划 WorkerSessionHandle 设计节 API 描述 |
| P2 | Per-agent tool restriction（含 `question: false`） | 计划 Worker 接入策略节 |
| P2 | 级联取消（`getAllDescendantTasks`） | Phase 9 前置条件 |
| P2 | LRU residency eviction | Phase 8 补充描述 |
| P3 | `ROUTE_HINT` 解析容错策略 | ReviewEngine 设计节 |

### 5.2 建议修正计划中的过时描述

| 内容 | 当前描述 | 建议 |
|------|---------|------|
| `DEFAULT_MAX_ITERATIONS` | 计划 Phase 0 说 `= 2` | 改为 `= 5`，匹配 `runtime.rs:28` |
| `TaskManager` API | 接受 `task_id` 参数 | 改为以 `current_task` 为中心（注明 `max_parallel_workers=1` 约束） |
| `CategoryRouter` | 说"内置 MVP policy，还没有 CLI/env policy 覆盖" | 代码已有 `WorkerConfig.max_parallel_workers` / `max_parallel_per_key` 等配置 |
| Phase 5 ZedAgentWorker | 说"已完成第一轮 native backend 骨架" | 实际已完成 resident-session 语义 + dispatcher + GPUI 测试 |
| Phase 7 ROUTE_HINT=review | 说"有 runtime 语义" | 实际已完成 independent review 路径 + 连续 unknown 升级策略 |

### 5.3 建议移除或合并的重复内容

- 计划中"当前基线"段（行 158-203）和"仍不足"段（行 205-219）与各 Phase 的状态描述大量重复。建议合并为每个 Phase 的 `状态：已完成 / 进行中 / 未开始` 标记，删除中间的总览段。
- "立刻执行的下一步"段（行 887-899）与各 Phase 的 follow-up 任务重复，建议删除或合并到 Phase 描述中。
- 行 117-141 的架构图与行 122-155 的职责边界描述重叠。

---

## 六、OMO 中值得后续借鉴但暂不实施的设计

| 设计 | 收益 | 建议时机 |
|------|------|---------|
| `compaction-context-injector`（压缩时注入背景任务状态） | 防止长 Gear 会话中 parent 丢失 worker 上下文 | Phase 8+ |
| `unstable-agent-babysitter`（自动重试/取消不可靠 agent） | 提升 Gear 对不稳定 worker 的容错 | Phase 8 |
| `tryFallbackRetry()`（provider 耗尽检测） | Codex/Claude API quota 耗尽时自动切换 | Phase 6 |
| `task_output` tool（让用户直接读取 worker 输出） | UI 中不用手动找 artifact 文件 | Phase 7 GUI 集成 |
| `restart` 语义 | 允许从失败 attempt 的断点继续，无需完全重做 | Phase 9 |

---

## 总结

| 维度 | 评价 |
|------|------|
| OMO 核心机制吸收 | ✅ BackgroundManager / ManagedChildHandle / category routing / fallback / attempt 正确借鉴 |
| 计划与实现一致性 | ⚠️ 计划滞后于代码（如 `CoordinatorReviewHook`、`TaskManagerControl` API、`DEFAULT_MAX_ITERATIONS` 已改） |
| OMO 遗漏机制 | ⚠️ 缺 `run_epoch` 去重、parent-wake 忙碌检测、`messageability`、级联取消、per-agent tool restriction |
| 有意简化 | ✅ 同步 loop、in-process only、无 TMUX/team mode 等均为合理的架构简化 |
| 计划可维护性 | ⚠️ 多处重复、"当前基线"与 Phase 状态重复、建议精简 |
