# Phase 08：GoalLoop、ReviewEngine、budget 与 stagnation guard

## 目标

把 Gear runtime 的核心特色落到循环策略：plan-code-review-repair 自问自答，provider-backed 自审/重规划，独立 reviewer gate，no-progress/stagnation 检测，token/context guard 和统一 budget policy。

## 主要文件

- `crates/gearbox_agent/src/runtime.rs`
- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/gearbox_agent.rs`

## 具体工单

1. Review parser 容错：
   - `GOAL_SATISFIED` / `ROUTE_HINT` / `STOP_REASON` 大小写不敏感。
   - 缺 key 时使用 `unknown` / `none` 默认值。
   - malformed 输出写 parser warning artifact。
   - raw response 永远保留。
2. Review input 补齐：
   - category resolution result
   - attempted route / skipped route / nearest fallback 区分后的 route metadata
   - fallback history
   - provider/model transform
   - worker transcript head/tail
   - verification result
   - changed files/diff summary
   - no-progress signals
   - budget remaining
3. independent reviewer gate：
   - 高风险任务、用户要求审查、`ROUTE_HINT=review`、连续 unknown、verification 与 provider review 冲突时触发。
   - reviewer 必须是独立 worker/session，不能直接复用刚写代码的 worker claim。
   - reviewer 输出必须进入 Evidence Chain。
4. complete 判定规则：
   - verification failed 永远阻止 complete。
   - provider `STOP_REASON=complete` 只在 deterministic checks 通过时生效。
   - independent reviewer veto 会进入 repair/replan。
5. unified policy：
   - provider-backed review
   - independent reviewer
   - fallback retry
   - premium/depth budget
   - no-progress detector
   - 统一成一个 `GoalDecisionPolicy`，减少逻辑散落。
   - policy 消费 Phase 07 修正后的 route metadata：没有真正 fallback 时不能根据当前 route 生成“可继续 fallback”的下一轮。
6. no-progress/stagnation detector：
   - 连续无 diff。
   - 连续相同 verification failure。
   - worker 输出重复或只解释不改代码。
   - review repair request 与上一轮高度相同。
   - transcript 无 tool/output 进展。
7. token/context guard：
   - 检测 token limit、context compaction、session agent 信息不可靠。
   - 状态不可判定时进入 replan/needs_user，不盲目继续 worker。
8. BudgetController 改造：
   - `max_iterations = 5` 保持默认。
   - `max_worker_calls`、`max_premium_worker_calls`、`max_same_failure_retries`、`max_runtime_minutes` 都进入决策 artifact。
   - `max_child_depth` 作为独立 child-depth budget 进入决策 artifact。
   - fallback chain 长度与 retry 机会绑定，但 premium budget 仍可提前截断。
9. final report：
   - `Evidence Chain` 必须列出 spec/plan/worker packet/transcript/result/outcome/verification/review。
   - `limited` 报告必须写“已完成、未完成、为什么停止、下一步建议”。

## 测试

1. `malformed_review_response_falls_back_to_unknown`
2. `verification_failed_blocks_provider_complete`
3. `review_route_hint_triggers_independent_reviewer`
4. `independent_reviewer_veto_forces_repair`
5. `consecutive_unknown_escalates_to_review_then_needs_user`
6. `no_progress_detector_stops_or_upgrades_route`
7. `premium_budget_exhaustion_returns_limited`
8. `final_report_includes_evidence_chain`
9. `final_report_includes_decision_guidance`
10. `evaluation_maps_worker_call_budget_limit_to_limited`
11. `evaluation_pauses_when_context_becomes_unreliable`
12. `evaluation_maps_child_depth_budget_limit_to_limited`
13. `evaluation_maps_runtime_budget_limit_to_limited`

## 验收

- Gear 能持续 plan-code-review-repair，直到 complete/limited/blocked/needs_user/cancelled。
- 自审不是一句 provider 文本，而是有 verification、review、evidence 和预算共同参与。
- 没有实质进展时不会无限循环。

## 当前状态

- 已完成：Gear coordinator review 解析现在会保留 raw response，并在 malformed / unknown field / invalid value 场景写出 `coordinator-review-iteration-*-warnings.md` parser warning artifact。
- 已完成：`goal_review_artifact` 已携带 fallback history，前后 attempt / provider / model / session / decision 都会进入 review 证据链。
- 已完成：`GoalDecisionPolicy` 已统一 provider review、independent reviewer、fallback retry、stagnation、verification complete 判定与 route hint override。
- 已完成：`ROUTE_HINT=review` 现在会强制插入至少一轮独立 review worker，即使 provider 已经给出 `goal_satisfied: yes` 也不会绕过 review gate。
- 已完成：`final_report` 已新增 `Decision` 区块，会把停止原因和下一步建议写进最终报告。
- 已完成：`BudgetController` 已携带 worker-call / premium-worker / same-failure-retry / runtime budget 摘要，review 输入和 goal review artifact 都会写入这份 budget snapshot。
- 已完成：`GoalDecisionPolicy` 现在会在继续下一轮前检查 worker-call、premium-worker、same-failure-retry、runtime 和 context 风险信号，context 不可靠时会保守停到 `needs_user`。
- 已完成：`BudgetController` 现在也携带独立 `max_child_depth` 预算，`budget_summary` / `budget_guard_reason` 会写入 `child_depth`，超过阈值时会直接收敛到 `limited`；`max_provider_unknown_streak` 也已经显式进入预算控制器，并从 Gear CLI / GUI env 接到 `Goal.budget`，作为 provider review 不确定性的第一版阈值。
- 已完成：context 风险现在也会从 worker stdout/stderr/last_message/result/outcome artifact、`transcript.jsonl` / `tool-events.jsonl` / `partial-output.md` 和 attempt history 中提取，不再只依赖摘要字段；review 输入也会带上 worker transcript head/tail 和 category resolution result。
- 已完成：no-progress detector 现在还会把重复的 worker output summary 识别为 stagnation 信号。
- 已完成：更强的 token-level 失真/截断信号已经接入结构化 transcript/tool-events 解析，能在 turn 未正常 finish、tool call 未 finish、或 partial output 仅部分落盘时保守触发 context 风险信号。
- 待后续：`GoalDecisionPolicy` 和 coordinator review prompt 继续把“没有 fallback”当作没有 fallback 处理；没有可用不同 route 时应进入 repair/replan/limited/needs_user，而不是继续同一路线空转。
