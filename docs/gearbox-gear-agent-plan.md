# Gearbox Gear Agent 实施计划

## 一句话目标

Gear 是 Gearbox 的专用编排 agent。它接入 Gearbox/Zed 的 Agent Panel，但不依赖 Zed 原生 Agent 作为控制核心。它的目标是：用户用一句自然语言描述需求后，Gear 能把需求扩展成规格、计划、代码实现、验证记录和可运行交付物。

第一阶段目标不是“任意一句话生成任意复杂商业系统”，而是先稳定完成：

```text
一句话需求
  -> 生成最小 spec
  -> 选择 Python/TypeScript/Rust 技术路径
  -> 创建任务账本
  -> 调 opencode 完成主要代码实现
  -> 调 shell/context/code tools 验证
  -> 修复失败
  -> 生成 README、运行说明和交付总结
```

## 核心判断

Gear 不应一开始复制或深改 Zed Agent。最简单、最稳的切口是先做独立 ACP External Agent：

```text
Gearbox/Zed GUI
  - Agent Panel
  - Threads Sidebar
  - diff/review/checkpoint/worktree
  - LSP/project search/terminal
        |
        | ACP
        v
Gear runtime
  - goal/task ledger
  - product spec
  - language profiles
  - worker router
  - event stream
  - verification loop
        |
        | CLI / MCP / ACP / local APIs
        v
opencode / Codex / Claude Code / CodeGraph / context-mode / browser / shell
```

Gearbox/Zed 负责 IDE 宿主体验。Gear 负责控制层、任务编排、状态监督、预算路由和结果验收。

## 产品定位

Gear 不是一个简单转发器，也不是第一版就要替代 opencode、Codex、Claude Code 的全功能 coding agent。它是 **面向小型程序生成的 orchestration runtime**。

它需要解决单一 agent 容易失败的几个问题：

- 需求太粗：Gear 先把一句话变成可执行 spec。
- 上下文太散：Gear 决定何时用 grep、LSP、CodeGraph、context-mode。
- worker 太自由：Gear 给每个 worker 明确边界和输出契约。
- 多 agent 太贵：Gear 只在关键步骤调用贵 worker。
- 结果不可信：Gear 统一跑验证、检查 diff、记录证据。
- 长任务会丢：Gear 持久化 goal、tasks、trace、artifacts。

## Gear 是否自己编程

Gear 必须有最小编程能力，但第一版不做完整 coding agent。

Gear 自己必须能做：

- 读文件、列目录、查路径。
- grep/project search。
- 读取 `git status`、`git diff`、`git log`。
- 运行确定性命令，例如测试、typecheck、lint、build。
- 写入自己的状态文件、计划文件、trace 文件。
- 解析 worker 输出。
- 提取构建/测试错误摘要。
- 判断 diff 是否超出允许范围。
- 对文档、配置、计划做小范围补丁。

Gear 第一版不需要自己做：

- 大规模代码生成。
- 长链路 debug。
- 复杂多文件编辑。
- 完整 terminal tool loop。
- 替代 opencode 的 coding runtime。

第一版中，复杂代码实现交给 opencode worker；Gear 保留任务边界、验收权和下一步决策权。

## Gear 与 worker 的关系

worker 不是 Gear 的上级，也不是可以自由行动的子进程。worker 是受控执行单元。

Gear 调用 worker 时必须生成 worker packet：

```json
{
  "task_id": "task_003",
  "worker": "opencode",
  "goal": "实现一个带登录和任务列表的本地 Web 应用",
  "scope": {
    "allowed_paths": ["apps/todo-web", "README.md"],
    "forbidden_paths": [".git", "crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md"],
    "max_files_changed": 40
  },
  "constraints": [
    "不要引入未确认的大型框架",
    "优先使用项目已有包管理器",
    "生成的应用必须能本地运行"
  ],
  "required_outputs": [
    "summary",
    "changed_files",
    "commands_run",
    "known_failures",
    "next_steps"
  ],
  "verification": {
    "preferred_commands": ["npm run build", "npm test"],
    "must_not_skip": ["typecheck"]
  },
  "stop_conditions": [
    "需要新增外部付费服务",
    "需要用户提供 API key",
    "验证连续失败两次"
  ]
}
```

worker 返回后，Gear 必须独立做这些事：

- 检查实际 diff。
- 检查是否触碰禁止路径。
- 运行验证命令。
- 将验证结果写入 task ledger。
- 判断是否继续、重试、换 worker、降级方案或请求用户输入。

worker 只能提供 evidence。Gear 才能把 goal 标记为完成。

## Worker 分层

### Tier 0：Gear 内置确定性工具

这些工具由 Gear 自己调用，不需要 LLM worker：

- 文件读取和目录扫描。
- `rg` 搜索。
- `git status` / `git diff`。
- 命令执行和退出码记录。
- JSON/Markdown 状态文件读写。
- 大输出切分和摘要入口。

用途：

- 事实确认。
- 低成本上下文收集。
- 验证和审计。
- worker 结果验收。

### Tier 1：opencode worker

第一版主力 coding worker。

适合：

- 创建项目脚手架。
- 多文件代码修改。
- 修复测试失败。
- 根据明确 spec 实现功能。
- 执行局部工程任务。

不适合：

- 让它决定整个产品方向。
- 让它自由选择所有 worker。
- 让它绕过 Gear 的验证和预算策略。

### Tier 2：CodeGraph 和 context-mode

CodeGraph 是可选代码定位增强，不是 MVP 硬依赖。

使用条件：

- grep/LSP 找不到入口。
- 需要调用链或跨文件关系。
- 大仓库中需要先做结构定位。

context-mode 用于：

- 大日志压缩。
- 测试输出摘要。
- session trace 检索。
- 长上下文归档。

### Tier 3：Codex 和 Claude Code

第二阶段再接。

建议职责：

- Codex：复杂工程修复、严谨代码审查、补丁方案。
- Claude Code：长上下文产品推演、复杂 spec、跨模块方案比较。

Gear 的预算策略应避免“每步都开强模型”。强 worker 只用于：

- 初始架构不确定。
- opencode 连续失败。
- 需要独立 review。
- 需要生成高质量产品说明或交互方案。

## Runtime 架构

建议放在 Gearbox 专属 crate：

```text
crates/gearbox_agent/
  Cargo.toml
  src/
    main.rs
    acp/
      mod.rs
      server.rs
      protocol.rs
      session_updates.rs
    runtime/
      mod.rs
      orchestrator.rs
      planner.rs
      router.rs
      supervisor.rs
      budget.rs
      recovery.rs
    state/
      mod.rs
      store.rs
      goal.rs
      task.rs
      event.rs
      artifact.rs
    tools/
      mod.rs
      filesystem.rs
      grep.rs
      git.rs
      shell.rs
      diff.rs
    workers/
      mod.rs
      opencode.rs
      codex.rs
      claude.rs
      codegraph.rs
      context_mode.rs
    languages/
      mod.rs
      detect.rs
      python.rs
      typescript.rs
      rust.rs
    product/
      mod.rs
      spec.rs
      templates.rs
      acceptance.rs
    workflows/
      intake.md
      plan.md
      execute.md
      verify.md
      review.md
```

短期也可以先做仓库外原型，但进入当前项目后应放在 `crates/gearbox_agent`，避免改 `crates/agent`、`crates/agent_ui` 等上游共享源码。

## ACP 接入边界

Gear 作为 Zed/Gearbox External Agent，需要先实现最小 ACP 能力：

- initialize。
- new session。
- prompt。
- cancel。
- session update streaming。
- plan/status update。
- final response。

第一版不需要深度接入 Zed 内部 LSP API。Gear 可以通过文件系统、shell、opencode、CodeGraph、context-mode 先完成闭环。

Zed/Gearbox 侧只需要一个 custom agent 配置示例：

```json
{
  "agent_servers": {
    "gear": {
      "type": "custom",
      "command": "gear",
      "args": ["--acp"],
      "env": {}
    }
  }
}
```

后续产品化后，再考虑 registry 安装、设置页入口和状态面板。

## 状态目录

Gear 自己维护持久状态，不依赖 Zed 原生 thread DB：

```text
.gearbox-agent/
  config.json
  sessions/
    <session_id>.json
  goals/
    <goal_id>.json
  tasks/
    <goal_id>.tasks.json
  events/
    <session_id>.jsonl
  artifacts/
    <goal_id>/
      spec.md
      plan.md
      acceptance.md
      verification.md
      final-report.md
  workers/
    <task_id>/
      prompt.md
      output.md
      stderr.log
      result.json
```

原则：

- 用户能打开这些文件复查 Gear 的判断。
- worker 原始输出不直接等于事实，必须经 Gear 归纳。
- 每次修改代码前后都记录 diff 摘要。
- 验证失败也要持久化，不能只在 UI 里闪过。

## 数据模型

### Goal

```json
{
  "id": "goal_20260707_001",
  "title": "生成一个客户跟进 CRM 小程序",
  "status": "running",
  "workspace": "/path/to/project",
  "created_at": "2026-07-07T00:00:00+08:00",
  "updated_at": "2026-07-07T00:00:00+08:00",
  "request": "帮我做一个销售客户跟进系统",
  "product_type": "web_app",
  "language_profile": "typescript",
  "success_criteria": [
    "应用可以本地启动",
    "包含客户列表、客户详情、跟进记录",
    "有 README 和验证命令",
    "构建命令通过"
  ],
  "budget": {
    "max_worker_calls": 8,
    "max_premium_worker_calls": 2,
    "max_runtime_minutes": 60
  },
  "current_task_id": "task_003",
  "summary": ""
}
```

Goal 状态：

- `draft`
- `planning`
- `running`
- `verifying`
- `needs_user`
- `blocked`
- `complete`
- `failed`

### Task

```json
{
  "id": "task_003",
  "goal_id": "goal_20260707_001",
  "title": "实现 CRM 前后端最小功能",
  "kind": "edit",
  "status": "running",
  "assigned_worker": "opencode",
  "attempt": 1,
  "scope": {
    "allowed_paths": ["apps/crm"],
    "forbidden_paths": [".git"]
  },
  "inputs": {
    "spec_path": ".gearbox-agent/artifacts/goal_20260707_001/spec.md",
    "plan_path": ".gearbox-agent/artifacts/goal_20260707_001/plan.md"
  },
  "outputs": {
    "changed_files": [],
    "commands_run": [],
    "evidence": [],
    "summary": ""
  }
}
```

Task 类型：

- `intake`
- `spec`
- `plan`
- `scaffold`
- `edit`
- `verify`
- `repair`
- `review`
- `document`
- `handoff`

### Event

```json
{
  "ts": "2026-07-07T00:00:00+08:00",
  "session_id": "ses_001",
  "goal_id": "goal_20260707_001",
  "task_id": "task_003",
  "kind": "worker_started",
  "message": "opencode 开始实现 CRM 最小功能",
  "data": {
    "worker": "opencode",
    "allowed_paths": ["apps/crm"]
  }
}
```

事件类型：

- `goal_created`
- `spec_created`
- `plan_created`
- `task_started`
- `worker_started`
- `worker_output`
- `worker_waiting`
- `worker_finished`
- `worker_failed`
- `diff_detected`
- `verification_started`
- `verification_failed`
- `verification_passed`
- `repair_started`
- `goal_completed`
- `goal_blocked`

## UI 实时显示

第一版通过 ACP message streaming 展示文本化工作流：

```text
Gear: 创建 goal_20260707_001
Gear: 识别为 TypeScript Web App
Gear: 生成 spec.md
Gear: 派发 task_003 给 opencode，范围 apps/crm
opencode: 正在写入前端页面和本地数据层
Gear: 发现 18 个文件变更
Gear: 运行 npm run build
Gear: build 失败，提取 2 个 TypeScript 错误
Gear: 派发 task_004 修复类型错误
Gear: build 通过
Gear: 生成 README 和 final-report.md
```

不要把 worker 原始日志全部推给用户。UI 中默认显示 Gear 的结构化状态；原始日志写入 artifacts，需要时再展开。

后续可以在 Gearbox fork 中增加：

- Goal/Task sidebar。
- Worker timeline。
- Verification panel。
- Budget usage panel。
- “打开 artifacts”按钮。

## 从一句话生成程序的流程

### 1. Intake

输入：

```text
帮我做一个销售客户跟进系统
```

Gear 生成最小需求澄清：

- 应用类型：CRM 小工具。
- 默认技术栈：TypeScript Web App。
- 默认存储：本地 SQLite 或 JSON 文件，除非用户要求云端。
- 默认功能：客户列表、客户详情、跟进记录、搜索、状态筛选。
- 默认交付：可本地运行、README、示例数据、build 通过。

如果需求缺关键约束，Gear 不应阻塞太早。它应采用可逆默认值，并在 spec 中记录假设。

### 2. Spec

生成：

```text
.gearbox-agent/artifacts/<goal_id>/spec.md
```

内容：

- 用户原始需求。
- Gear 的默认假设。
- 功能列表。
- 非目标。
- 数据模型。
- 页面/API 列表。
- 验收标准。

### 3. Plan

生成：

```text
.gearbox-agent/artifacts/<goal_id>/plan.md
```

计划必须可执行：

- scaffold。
- data model。
- UI pages。
- backend/API。
- tests/smoke。
- README。
- verification。

### 4. Build

Gear 选择 worker：

- 简单脚手架：opencode。
- 复杂产品交互：Claude/Codex 可选。
- 代码定位：CodeGraph 可选。
- 大日志：context-mode。

第一版默认只用 opencode + shell。

### 5. Verify

验证必须来自 Gear，而不是只相信 worker。

TypeScript Web App 最小验证：

```text
npm install / pnpm install / bun install
npm run build / pnpm build / bun run build
npm test / pnpm test / bun test
```

如果有浏览器能力，后续加入：

```text
start dev server
open page
run Playwright smoke
capture screenshot
verify no console errors
```

### 6. Repair

如果验证失败：

```text
extract error summary
create repair task
send focused packet to worker
rerun failed command
```

同一错误连续失败两次后，Gear 应升级 worker 或请求用户输入。

### 7. Deliver

最终交付：

- 应用源码。
- README。
- 运行命令。
- 验证命令和结果。
- 已知限制。
- 后续建议。

## 语言 Profile

### TypeScript

优先支持，因为最适合“一句话生成小程序”。

识别：

- `package.json`
- `tsconfig.json`
- `vite.config.*`
- `next.config.*`
- `pnpm-lock.yaml`
- `bun.lock`
- `package-lock.json`

默认栈：

- 新项目：Vite + React + TypeScript，除非用户要求 Next.js。
- 简单后端：Express/Fastify 或本地 JSON/SQLite，根据需求选择。
- UI：先使用普通 CSS/轻量组件，不默认引入重型 UI 框架。

验证：

```text
package manager detect
install if needed
typecheck
build
test if script exists
```

### Python

适合：

- 自动化脚本。
- 数据处理小工具。
- FastAPI 小服务。
- CLI 工具。

识别：

- `pyproject.toml`
- `requirements.txt`
- `uv.lock`
- `poetry.lock`
- `pytest.ini`

默认验证：

```text
uv run pytest
pytest
ruff check .
mypy .
```

### Rust

适合：

- CLI。
- 高可靠服务。
- 本地工具。
- Gearbox/Zed fork 内部开发。

识别：

- `Cargo.toml`
- `Cargo.lock`
- workspace members

默认验证：

```text
cargo check
cargo test -p <crate>
./script/clippy
cargo clippy
```

在当前 Gearbox/Zed fork 中：

- 优先 `./script/clippy`，不优先裸 `cargo clippy`。
- GPUI 测试使用 GPUI executor timer。
- 修改共享源码前先看 `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`。
- Gearbox 专属行为优先放在 `crates/gearbox`、`crates/gearbox_settings` 或 Gear 专属 crate。

## OpenSpec 和 Superpowers 的吸收方式

OpenSpec 和 Superpowers 不作为 runtime。它们提供方法论和 artifact 结构。

### OpenSpec

吸收为 spec/change artifact：

```text
.gearbox-agent/specs/
  changes/
    <change_id>/
      proposal.md
      design.md
      tasks.md
      notes.md
```

适用：

- 大功能。
- 多文件重构。
- 影响产品行为。
- 需要用户确认的架构选择。

小修复不强制走 OpenSpec，否则流程过重。

### Superpowers

吸收为 workflow templates：

```text
crates/gearbox_agent/workflows/
  brainstorming.md
  writing_plan.md
  executing_plan.md
  systematic_debugging.md
  test_driven_development.md
  verification.md
  review.md
```

每个 workflow 必须有：

- 触发条件。
- 输入。
- 输出。
- 停止条件。
- 对应 task 类型。

不能只是复制提示词。

## 成本和路由策略

Gear 的省钱能力来自路由，不来自“多 agent 越多越好”。

默认策略：

- 确定性事实：Gear 内置工具。
- 大输出摘要：context-mode。
- 代码定位：grep/LSP，必要时 CodeGraph。
- 常规实现：opencode。
- 疑难修复：Codex。
- 产品/交互/长文档推演：Claude Code。
- 最终审查：Codex 或 Claude 二选一，不默认都用。

预算字段：

```json
{
  "max_worker_calls": 8,
  "max_premium_worker_calls": 2,
  "max_repair_attempts_per_error": 2,
  "max_runtime_minutes": 60
}
```

超过预算时 Gear 应进入 `needs_user`，说明已经完成什么、还差什么、继续会花费什么。

## 安全边界

Gear 默认不做这些事：

- 不自动提交 git。
- 不自动推送远程。
- 不自动删除大量文件。
- 不自动安装全局依赖。
- 不把密钥写入代码。
- 不把用户私有文件发送给不必要的 worker。

需要明确确认的操作：

- 引入付费云服务。
- 使用外部 API key。
- 大范围重构。
- 删除数据。
- 改动仓库级配置或 CI。

## MVP 验收标准

第一版完成标准：

- Gearbox/Zed 可以把 Gear 配置为 External Agent。
- 用户在 Agent Panel 发起任务后，Gear 能创建 goal/task ledger。
- Gear 能识别 Python/TypeScript/Rust 项目类型。
- Gear 能生成 `spec.md`、`plan.md`、`verification.md`。
- Gear 能调用 opencode worker 完成限定范围代码修改。
- Gear 能实时显示任务状态，而不是只显示 worker 原始输出。
- Gear 能检查 worker 产生的 diff。
- Gear 能独立运行至少一个验证命令。
- Gear 能在验证失败后创建 repair task。
- Gear 能在 `.gearbox-agent/events/` 和 `.gearbox-agent/artifacts/` 留下可复查记录。
- 失败时 Gear 能明确说明阻塞点，而不是只输出泛泛错误。

## V1 里程碑

### Milestone 1：本地 CLI 原型

目标：不接 ACP，先证明 runtime 能跑通。

任务：

1. 新建 `crates/gearbox_agent`。
2. 实现 `gear run "<prompt>" --workspace <path>`。
3. 创建 `.gearbox-agent/` 状态目录。
4. 实现 goal/task/event JSON 写入。
5. 实现 language detection。
6. 实现 shell verification runner。
7. 实现 opencode worker wrapper。
8. 用一个 TypeScript 小项目跑通从 prompt 到 build。

验收：

- 命令行能生成 artifacts。
- opencode 调用有 trace。
- build/test 结果进入 `verification.md`。

### Milestone 2：ACP External Agent

目标：Gear 能在 Gearbox/Zed Agent Panel 里运行。

任务：

1. 实现 ACP stdio server。
2. 实现 session map。
3. 将 CLI orchestrator 接到 prompt handler。
4. 将 events 转成 ACP streaming message。
5. 增加 custom agent 配置示例。

验收：

- Gearbox/Zed 能启动 Gear。
- Agent Panel 能看到 Gear 的阶段性状态。
- cancel 能停止当前 worker。

### Milestone 3：TypeScript 应用生成

目标：一句话生成可运行 Web App。

任务：

1. 固定默认栈。
2. 生成 spec/plan。
3. 调 opencode scaffold 和实现。
4. 跑 build。
5. 失败后修复一次。
6. 生成 README。

验收：

- 至少 3 个样例需求能生成可本地运行项目。

### Milestone 4：Python/Rust Profile

目标：扩展到脚本、CLI、小服务。

验收：

- Python：生成一个带测试的小 CLI 或 FastAPI 服务。
- Rust：生成一个带测试的小 CLI。

### Milestone 5：高级 worker 和成本路由

目标：加入 context-mode、CodeGraph、Codex/Claude。

验收：

- 大日志自动摘要。
- 代码定位失败时能切 CodeGraph。
- opencode 连续失败时能升级到 Codex 或 Claude。

## 第一批实施任务

1. 新建 `crates/gearbox_agent` crate。
2. 实现 `gear run` CLI，不接 ACP。
3. 实现 `GoalStore`、`TaskStore`、`EventStore`。
4. 实现 `.gearbox-agent/` 状态目录。
5. 实现 TypeScript language profile。
6. 实现 shell runner。
7. 实现 opencode worker wrapper。
8. 实现 spec/plan/verification artifact writer。
9. 跑通一个 TypeScript Web App smoke。
10. 再接 ACP stdio server。

## 风险

- 多 agent 不一定更好：必须靠 Gear 的任务边界和验证循环控制。
- opencode 输出不稳定：必须用 worker packet、事件流、task ledger 和 diff 审查约束。
- ACP 协议细节变化：第一版保持最小协议面。
- 上游同步成本：MVP 不改共享源码，只加 Gearbox 专属 crate/文档。
- CodeGraph/context-mode 过早绑定：先做可选 worker，不做硬依赖。
- 一句话需求不完整：Gear 应采用可逆默认值，并在 spec 中记录假设。

## 当前建议

立即从 `crates/gearbox_agent` 的本地 CLI 原型开始，不复制 Zed Agent。

第一版只做：

```text
gear run
+ goal/task/event stores
+ TypeScript profile
+ spec/plan artifacts
+ opencode worker
+ shell verification
+ final report
```

CLI 跑通后再接 ACP。ACP 跑通后再接 Python/Rust、context-mode、CodeGraph、Codex、Claude Code 和 Gearbox UI 深集成。
