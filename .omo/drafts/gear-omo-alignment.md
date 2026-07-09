# Draft: gear-omo-alignment

## Metadata
- **Source**: `docs/gearbox-gear-remaining-gap-dogfood-plan.md`
- **Intent**: CLEAR
- **Review required**: false (user did not request high accuracy)
- **Status**: awaiting-approval
- **Created**: 2026-07-09

## Decisions
- All P0/P1/P2 items specified in the dogfood plan are accepted as-is without modification
- Priority order: P0 → P1 → P2 (sequential within each tier; P2 items can be parallelized)
- TDD for every change: write/update test FIRST, then implement
- Use `attempt_count` (new field on `BudgetSnapshot`) for tracking retry attempts separate from `worker_call_count`
- For `detect_stagnation` diff comparison: implement `diff_hash` via normalized git patch SHA256 (strip timestamps/noise) stored in `DiffSnapshot`
- For `TaskManagerControl` return types: create `SendOutcome`/`SteerOutcome` enums in `crates/gearbox_agent/src/task_manager.rs`
- For `provider_unknown_streak`: reset only on `verification_passed && goal_satisfied == Some(true)` OR explicit STOP_REASON
- For `BudgetController.max_worker_calls`: read from `goal.budget.max_worker_calls`, fallback to `Budget::default().max_worker_calls`
- Key regressions to run after all changes: `cargo test -p gearbox_agent -- --nocapture`

## Owner-decisions surfaced (none — all specified in source doc)
- File paths, struct names, acceptance criteria all specified in `docs/gearbox-gear-remaining-gap-dogfood-plan.md`

## Pending action
- Write `.omo/plans/gear-omo-alignment.md` with full decision-complete work plan

## Approval gate
**Wait for user explicit approval before writing the plan file.**
