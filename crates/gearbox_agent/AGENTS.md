# Gearbox 编排运行时 (gearbox_agent)

## OVERVIEW

Gearbox 编排运行时——任务调度、worker 管理、lineage 追踪、完成判定。不直接写代码，只编排。

## STRUCTURE

```
src/
├── gearbox_agent.rs    # lib root, 声明模块
├── main.rs             # 二进制入口
├── cli.rs              # Cli 结构体, run()
├── runtime.rs          # 6K — Orchestrator::run, 迭代主循环, ReviewGate, GoalDecisionPolicy
├── state.rs            # 600 — WorkLineage, StateStore, Goal, Task, ExecutionOwnership, 持久化
├── workers.rs          # 5.5K — WorkerKind, WorkerCapabilities, WorkerRegistry, WorkerAdapter, WorkerSessionAdapter
├── task_manager.rs     # 9.6K — TaskManager, 重试逻辑, tick loop, CompletionNotifier
├── tools.rs            # 400 — Shell 工具, git_snapshot, check_scope, CancellationToken
├── product.rs          # 550 — spec/plan/verification/final_report 制品模板
└── languages.rs        # 340 — LanguageProfile, LanguageDetection, detect
```

## WHERE TO LOOK

| 符号 | 文件 | 作用 |
|------|------|------|
| `Orchestrator::run` | `runtime.rs:167` | 编排主循环入口 |
| `RunOptions` | `runtime.rs:42` | 运行配置（request, workspace, worker, budget...） |
| `RunOutcome` | `runtime.rs:112` | 运行结果（goal_id, status, artifacts_root） |
| `GoalDecisionPolicy` | `runtime.rs:2024` | 完成判定策略（private, 整合所有门控信号） |
| `ReviewGate` | `runtime.rs:1776` | 合成 review 门控，多维度检查 |
| `ReviewDimension` | `runtime.rs:1735` | review 维度枚举 |
| `WorkLineage` | `state.rs:56` | 任务层级追踪，参与完成判定 |
| `StateStore` | `state.rs:333` | 持久化存储（.gearbox-agent/） |
| `Goal` | `state.rs:113` | 目标：request, success_criteria, budget, status |
| `Task` | `state.rs:202` | 任务：kind, status, assigned_worker, attempt |
| `ExecutionOwnership` | `state.rs:101` | 所有权决策：delegated, worker_kind, route_reason |
| `WorkerKind` | `workers.rs:46` | Worker 类型：Opencode, Codex, Claude, ZedAgent, Custom |
| `WorkerCategory` | `workers.rs:181` | 任务类别：Quick, Deep, Repair, Review, Explore... |
| `WorkerCapabilities` | `workers.rs:1302` | 能力声明 |
| `WorkerRegistry` | `workers.rs:1418` | Worker 注册、启动、查找 |
| `CategoryRouter` | `workers.rs:330` | 类别→worker 路由 |
| `CategoryResolution` | `workers.rs:1034` | 分类解析结果 |
| `TaskManager` | `task_manager.rs:1188` | 任务队列、重试、tick 循环 |

## CONVENTIONS

- **Gear 不直接写代码**——只编排调度，代码修改由 worker 执行
- **所有权门控**——所有改代码的任务必须产生 `ExecutionOwnership { delegated: true }`，否则不能标记 Complete
- **合成 review 门控**——`ReviewGate` 强制执行 verification/scope/security 检查；每个维度必须有独立 review 证据
- **Lineage 完成判定**——`WorkLineage` 追踪父子 session 关系，祖先 session 不能在还有后代活跃时完成
- **Budget 系统**——`BudgetController` 控制迭代次数、worker 调用次数、重试次数、运行时长上限
- **测试内联**——`#[cfg(test)] mod tests { }` 块内联在源文件中

## COMMANDS

```bash
cargo test -p gearbox_agent -- --nocapture
./script/clippy -p gearbox_agent
```

## KNOWN ISSUES

- `evaluate_goal` 测试包装器使用 `delegated: true` 夹具
- `WorkLineage` 无生产调用者（仅测试引用）
- capabilities 使用 catch-all 配置

## ANTI-PATTERNS

- 不修改 `.omo/**` 或计划文件
- 不使用测试夹具绕过生产门禁（如 `delegated: true` 硬编码）
- 不引入新 crate
- 不重复 root AGENTS.md 的 Gearbox fork 规则、Rust 指南、GPUI 指南
