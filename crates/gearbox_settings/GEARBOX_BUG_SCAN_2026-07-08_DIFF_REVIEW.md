# Gearbox 改动审阅记录（2026-07-08）

范围：本地差异对应文件  
`crates/gearbox_agent/src/runtime.rs`、`crates/gearbox_agent/src/workers.rs`、`crates/gearbox_agent/src/cli.rs`、`crates/agent/src/agent.rs`。  
本次仅做静态检查，不执行测试。

## 1）worker 路由与阶段绑定修正（P1）
文件：`crates/gearbox_agent/src/workers.rs:11-274`、`crates/agent/src/agent.rs:2758-2790`  
实现点：
- 新增 `WorkerRoute` 与 `SelectedWorkerRoute`，支持按 `GEARBOX_GEAR_WORKER_SEQUENCE` 定义路由序列。
- `WorkerConfig` 增加 `worker_routes`，`selected_route(attempt)` 根据轮次选择命令来源。
- CLI 与 session 环境变量解析会把 `GEARBOX_GEAR_WORKER*` 映射为路由，若配置了 route，则按序选用命令。
- `WorkerRegistry::run` 保留命令 worker 的统一入口，但不会再固定死 `request.config.worker_kind`，改由 route 决定 worker 名称与命令来源。

问题价值：此前“路由语义与工人实际执行”存在脱节（修复/重试轮次无法可靠切换执行方）；本次改动把任务分配与 worker 实际选择对齐。

## 2）按轮次选择不同 worker 的实现（P1）
文件：`crates/gearbox_agent/src/runtime.rs:132-709`  
实现点：
- `initial_tasks` 增加 `worker_kind` 入参，初始实现任务 `task_003` 按运行配置的首选路由 worker 而非固定 `opencode`。
- `add_repair_task` 增加 `worker_kind` 入参，repair 任务也按当轮路由 worker 分配（`selected_route(worker)`）。
- 循环中每轮取 `selected_route = options.worker.selected_route(iteration)`，并将该路由中的 `require_worker` 传入目标判断。

问题价值：此前 `task_003/task_00X` 与运行配置可能不一致，本次可避免“修复仍写死到 opencode”类行为偏差。

## 3）worker 命令来源解析增强（P2）
文件：`crates/agent/src/agent.rs:2758-2790`  
实现点：
- 新增 `GEARBOX_GEAR_WORKER_SEQUENCE`，可按顺序指定 worker kind。
- 每种 kind 支持对应 env 命令：`GEARBOX_GEAR_OPENCODE_COMMAND`、`..._CODEX_COMMAND`、`..._CLAUDE_COMMAND`、`..._ZED_AGENT_COMMAND`、`..._CUSTOM_COMMAND`。
- 当配置了自定义序列且存在任何含命令项时，`require_worker` 会自动切成 true（可避免误把需要命令的执行当作可选）。

问题价值：为后续“多轮修复 fallback/cascade”场景提供配置基础，降低单点配置失配风险。

## 4）已知剩余差距（仍建议跟进）
- ACP stdio 接入仍未落地（目前依然是 `gear run` CLI 为主链路）。
- 多 worktree/workspace 场景下 workspace 选择与 `coordinator_brief` 刷新仍需复核（本次未覆盖）。
- `GoalNeedsUser` 最终事件语义区分仍未在 runtime 事件映射完全收口（之前审阅中已提及）。

## 附：提交说明
- `verification` 未执行（按要求跳过）
- 建议在后续 PR 中补一组“worker sequence + fallback + 验证结果”最小端到端用例
