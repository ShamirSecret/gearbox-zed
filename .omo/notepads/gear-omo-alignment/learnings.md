# Gear ↔ Omo 功能对齐 - 学习笔记

## 项目约定
- 工作目录: /home/donald/文档/github/zed
- 目标 crate: crates/gearbox_agent
- 测试命令: cargo test -p gearbox_agent -- --nocapture
- 格式化: cargo fmt

## 关键文件
- crates/gearbox_agent/src/runtime.rs — 主运行时逻辑
- crates/gearbox_agent/src/workers.rs — worker 路由和解析
- crates/gearbox_agent/src/task_manager.rs — 任务管理
- crates/gearbox_agent/src/tools.rs — 工具/DiffSnapshot

## 已发现模式
- BudgetController 在 runtime.rs 中构造
- worker_call_count 在循环中累加（每轮迭代 +1，不再按 attempt 数累加）
- attempt_count 新增字段，独立追踪所有 attempt 总数（含重试）
- provider_unknown_streak 在 verification 后更新
- detect_stagnation 比较 diff_history
- GoalDecisionPolicy::evaluate() 做最终决策
- budget_guard_reason() 限流判断仍使用 worker_call_count，不受 attempt_count 影响
- budget_summary() 输出格式新增 attempts=N 指标
- BudgetSnapshot 派生 Default，新增字段不影响使用 ..Default 的测试构造

## P0-3: provider_unknown_streak 重置逻辑修复

### 问题
- runtime.rs 中 provider_unknown_streak 的更新逻辑用 if/else 二分：满足"unknown"条件则 +1，否则 reset 为 0
- 当 verification_passed=true 且 goal_satisfied=Some(false) 时，不满足 unknown 条件（goal_satisfied 不是 None），落入 else 分支被重置为 0
- 这导致 unknown 无法正确累计，因为 review 确认"目标未满足"时 streak 被错误清零

### 修复
- 提取 `update_provider_unknown_streak()` 辅助函数，三分支逻辑：
  1. goal_verified (verification_passed && goal_satisfied==Some(true)) 或有 concrete stop_reason → reset 为 0
  2. unknown 条件 (verification_passed && goal_satisfied.is_none() && no stop_reason) → +1
  3. 其他情况（包括 goal_satisfied==Some(false)）→ 保持不变
- 在 product.rs 中修复了 pre-existing 的 DiffSnapshot 初始化缺少 diff_hash 字段的编译错误

### 测试
- 新增 `provider_unknown_streak_not_reset_on_false_goal_satisfied` 覆盖 5 种场景
- 现有 `evaluation_honors_provider_unknown_streak_budget_limit` 仍然通过
- 全部 152 个测试通过
