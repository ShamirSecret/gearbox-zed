use crate::languages::{LanguageDetection, LanguageProfile};
use crate::plan_graph::{PlanGraph, PlanTaskContract};
use crate::state::{
    CriterionEvidenceStatus, FinalVerificationDimension, FinalVerificationWaveReceipt, Goal,
    GoalStatus, PlanNodeRunLedger, PlanNodeRunStatus, Task,
};
use crate::tools::{DiffSnapshot, ScopeCheck, ShellCommandResult};
use crate::workers::WorkerResult;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub fn spec(goal: &Goal, detection: &LanguageDetection) -> String {
    let generation_guidance = generation_guidance(detection);
    format!(
        r#"# Spec

## Original Request

{}

## Gear Assumptions

- Product type: {}
- Language profile: {}
- Coordinator model: {}
- Evidence: {}
- Prefer reversible defaults when the prompt leaves details open.
- Keep the first implementation local and runnable.

## Features

- Create the smallest useful product that satisfies the request.
- Include local run instructions.
- Include verification commands and known limits.

## Non-goals

- No paid cloud dependency unless the user explicitly asks.
- No global dependency installation.
- No automatic git commit or push.

## Acceptance Criteria

{}

## Generation Guidance

{}

## Coordinator Brief

{}
"#,
        goal.request,
        goal.product_type,
        detection.profile.as_str(),
        coordinator_model_summary(goal),
        if detection.evidence.is_empty() {
            "none".to_string()
        } else {
            detection.evidence.join(", ")
        },
        goal.success_criteria
            .iter()
            .map(|criterion| format!("- {criterion}"))
            .collect::<Vec<_>>()
            .join("\n"),
        generation_guidance,
        coordinator_brief_summary(goal)
    )
}

pub fn plan(goal: &Goal, plan_graph: &PlanGraph, detection: &LanguageDetection) -> String {
    plan_with_progress(goal, plan_graph, detection, None)
}

pub fn plan_with_progress(
    goal: &Goal,
    plan_graph: &PlanGraph,
    detection: &LanguageDetection,
    progress: Option<&PlanNodeRunLedger>,
) -> String {
    plan_with_progress_and_final_wave(goal, plan_graph, detection, progress, None)
}

pub fn plan_with_progress_and_final_wave(
    goal: &Goal,
    plan_graph: &PlanGraph,
    detection: &LanguageDetection,
    progress: Option<&PlanNodeRunLedger>,
    final_wave: Option<&FinalVerificationWaveReceipt>,
) -> String {
    let generation_guidance = generation_guidance(detection);
    let task_lines = plan_graph
        .draft
        .tasks
        .iter()
        .map(|task| {
            let node = progress.and_then(|ledger| {
                ledger
                    .nodes
                    .iter()
                    .find(|node| node.task_id == task.task_id)
            });
            render_plan_task(
                task,
                progress,
                node.map(|node| &node.status),
                node.and_then(|node| node.commit_boundary_satisfied),
                node.and_then(|node| node.commit_boundary_evidence_path.as_deref()),
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let commands = if detection.verification_commands.is_empty() {
        "- No verification command detected yet.".to_string()
    } else {
        detection
            .verification_commands
            .iter()
            .map(|command| format!("- `{command}`"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let final_verification_wave = [
        (
            "F1. Plan compliance audit",
            FinalVerificationDimension::PlanCompliance,
        ),
        (
            "F2. Code quality review",
            FinalVerificationDimension::CodeQuality,
        ),
        ("F3. Real manual QA", FinalVerificationDimension::RealQa),
        (
            "F4. Scope fidelity",
            FinalVerificationDimension::ScopeFidelity,
        ),
    ]
    .into_iter()
    .map(|(label, dimension)| format!("- [{}] {label}", final_wave_marker(final_wave, dimension)))
    .chain(
        plan_graph
            .draft
            .final_verification
            .iter()
            .map(|check| format!("- [ ] Plan declaration: {check}")),
    )
    .collect::<Vec<_>>()
    .join("\n");
    let dependency_matrix = std::iter::once(
        "| Work order | Dependencies | Parallel wave |\n| --- | --- | --- |".to_string(),
    )
    .chain(plan_graph.draft.tasks.iter().map(|task| {
        format!(
            "| `{}` | {} | {} |",
            task.task_id,
            if task.dependencies.is_empty() {
                "none".to_string()
            } else {
                task.dependencies
                    .iter()
                    .map(|dependency| format!("`{dependency}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
            task.parallel_wave,
        )
    }))
    .collect::<Vec<_>>()
    .join("\n");
    let mut tasks_by_wave = BTreeMap::<usize, Vec<&str>>::new();
    for task in &plan_graph.draft.tasks {
        tasks_by_wave
            .entry(task.parallel_wave)
            .or_default()
            .push(task.task_id.as_str());
    }
    let milestones = tasks_by_wave
        .into_iter()
        .map(|(wave, task_ids)| {
            format!(
                "- Wave {wave}: {}",
                task_ids
                    .into_iter()
                    .map(|task_id| format!("`{task_id}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .chain(std::iter::once(
            "- Final wave: F1-F4 verification and final acceptance".to_string(),
        ))
        .collect::<Vec<_>>()
        .join("\n");
    let acceptance_checklist = plan_graph
        .draft
        .tasks
        .iter()
        .flat_map(|task| {
            task.completion_predicates.iter().map(move |predicate| {
                format!(
                    "- [{}] `{}`: {}",
                    criterion_marker(progress, &task.task_id, predicate),
                    task.task_id,
                    predicate
                )
            })
        })
        .chain(plan_graph.draft.final_acceptance.iter().map(|predicate| {
            format!(
                "- [{}] Final acceptance: {predicate}",
                final_acceptance_marker(progress, final_wave, plan_graph)
            )
        }))
        .collect::<Vec<_>>()
        .join("\n");
    let planner_receipt = plan_graph
        .planner
        .as_ref()
        .map(|receipt| {
            format!(
                "- Provider: `{}`\n- Model: `{}`\n- Session: `{}`",
                receipt.provider_id,
                receipt.model_id,
                receipt.session_id.as_deref().unwrap_or("not recorded")
            )
        })
        .unwrap_or_else(|| {
            "- Planner receipt: not recorded (deterministic fallback or legacy plan)".to_string()
        });

    format!(
        r#"# Plan

## TL;DR (For humans)

{}

Plan: `{}`
Revision: `{}`
Source: `{:?}`
Plan hash: `{}`

## Planning context

### Plan generation receipt

{}

### Open assumptions

{}

### Findings

{}

### Decisions

{}

### Open questions

{}

## Scope

### Must have

{}

### Must NOT have

{}

### Topology lock

{}

## Verification strategy

### Preflight

{}

### Per-work-order verification

{}

### Final verification

{}

## Execution strategy

The runtime dispatches only approved work orders, in dependency order and by
parallel wave. Each work order is closed-world: the worker must not redesign
scope, architecture, tests, or acceptance.

Coordinator model: `{}`
Coordinator brief: `{}`

### Work-order protocol

- Execute one independently verifiable work order at a time.
- Complete ordered execution steps and record evidence before advancing.
- Do not skip, redesign, or silently expand the current work order.
- A failed or unmet step returns the work order to review/revision instead of advancing dependencies.

Rollback actions: {}

## Rollback Plan

{}

## Dependency matrix

{}

## Milestones

{}

## Acceptance checklist

{}

## Todos

{}

## Final verification wave

{}

### Final acceptance

{}

## Commit strategy

{}

## Success criteria

{}

## Plan metadata

- Goal: `{}`
- Plan hash: `{}`
- Generation guidance: {}
"#,
        plan_graph.draft.objective,
        plan_graph.plan_id,
        plan_graph.revision,
        plan_graph.source,
        plan_graph.plan_hash,
        planner_receipt,
        markdown_list(&plan_graph.draft.assumptions),
        markdown_list(&plan_graph.draft.findings),
        markdown_list(&plan_graph.draft.decisions),
        markdown_list(&plan_graph.draft.open_questions),
        markdown_list(&plan_graph.draft.must_have),
        markdown_list(&plan_graph.draft.must_not_have),
        markdown_list(&plan_graph.draft.topology_lock),
        markdown_list(&plan_graph.draft.preflight),
        if detection.verification_commands.is_empty() {
            "- Each work order owns its declared RED/GREEN and QA evidence.".to_string()
        } else {
            commands.clone()
        },
        markdown_list(&plan_graph.draft.final_verification),
        coordinator_model_summary(goal),
        coordinator_brief_summary(goal),
        markdown_list(&plan_graph.draft.rollback),
        markdown_list(&plan_graph.draft.rollback),
        dependency_matrix,
        milestones,
        acceptance_checklist,
        task_lines,
        final_verification_wave,
        markdown_list(&plan_graph.draft.final_acceptance),
        plan_graph
            .draft
            .tasks
            .iter()
            .map(|task| format!("- `{}`: `{:?}`", task.task_id, task.commit_boundary))
            .collect::<Vec<_>>()
            .join("\n"),
        markdown_list(&plan_graph.draft.final_acceptance),
        goal.id,
        plan_graph.plan_hash,
        generation_guidance,
    )
}

fn final_wave_marker(
    receipt: Option<&FinalVerificationWaveReceipt>,
    dimension: FinalVerificationDimension,
) -> &'static str {
    match receipt.and_then(|receipt| {
        receipt
            .dimensions
            .iter()
            .find(|result| result.dimension == dimension)
    }) {
        Some(result) if result.passed => "x",
        Some(_) => "!",
        None => " ",
    }
}

fn criterion_marker(
    progress: Option<&PlanNodeRunLedger>,
    task_id: &str,
    criterion: &str,
) -> &'static str {
    let Some(node) =
        progress.and_then(|ledger| ledger.nodes.iter().find(|node| node.task_id == task_id))
    else {
        return " ";
    };
    match node
        .criterion_evidence
        .iter()
        .find(|evidence| evidence.criterion_id == criterion && evidence.attempt == node.attempt)
        .map(|evidence| &evidence.status)
    {
        Some(CriterionEvidenceStatus::Pass) => "x",
        Some(CriterionEvidenceStatus::Fail | CriterionEvidenceStatus::Blocked) => "!",
        None => " ",
    }
}

fn qa_marker(
    progress: Option<&PlanNodeRunLedger>,
    task: &PlanTaskContract,
    kind: &str,
    scenario_name: &str,
) -> &'static str {
    let criterion = format!("qa:{kind}:{scenario_name}");
    criterion_marker(progress, &task.task_id, &criterion)
}

fn final_acceptance_marker(
    progress: Option<&PlanNodeRunLedger>,
    final_wave: Option<&FinalVerificationWaveReceipt>,
    plan: &PlanGraph,
) -> &'static str {
    let criteria_passed = progress.is_some_and(|ledger| {
        plan.draft.tasks.iter().all(|task| {
            ledger
                .nodes
                .iter()
                .find(|node| node.task_id == task.task_id)
                .is_some_and(|node| node.all_criteria_passed(&task.completion_predicates))
        })
    });
    match (criteria_passed, final_wave.map(|receipt| receipt.passed)) {
        (true, Some(true)) => "x",
        (_, Some(false)) => "!",
        _ => " ",
    }
}

fn render_plan_task(
    task: &PlanTaskContract,
    progress: Option<&PlanNodeRunLedger>,
    status: Option<&PlanNodeRunStatus>,
    commit_satisfied: Option<bool>,
    commit_evidence_path: Option<&str>,
) -> String {
    let checkbox = match status {
        Some(PlanNodeRunStatus::Completed) => "x",
        Some(PlanNodeRunStatus::Failed | PlanNodeRunStatus::NeedsUser) => "!",
        Some(
            PlanNodeRunStatus::Running
            | PlanNodeRunStatus::RedVerified
            | PlanNodeRunStatus::Implemented
            | PlanNodeRunStatus::GreenVerified
            | PlanNodeRunStatus::Reviewed,
        ) => "~",
        Some(PlanNodeRunStatus::Cancelled) => "!",
        Some(PlanNodeRunStatus::Pending | PlanNodeRunStatus::Runnable) | None => " ",
    };
    let role = plan_task_role(task);
    let red = task
        .test
        .red
        .as_ref()
        .map(|command| {
            format!(
                "- RED: `{}` -> {} (evidence: `{}`)",
                command.command, command.expected_observation, command.evidence_path
            )
        })
        .unwrap_or_else(|| "- RED: not required by the selected test strategy".to_string());
    let green = if task.test.green.is_empty() {
        "- GREEN: none".to_string()
    } else {
        task.test
            .green
            .iter()
            .map(|command| {
                format!(
                    "- GREEN: `{}` -> {} (evidence: `{}`)",
                    command.command, command.expected_observation, command.evidence_path
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let references = if task.references.is_empty() {
        "- none".to_string()
    } else {
        task.references
            .iter()
            .map(|reference| {
                format!(
                    "- `{}`{}: {}",
                    reference.path,
                    reference
                        .symbol
                        .as_deref()
                        .map(|symbol| format!("::{symbol}"))
                        .unwrap_or_default(),
                    reference.reason
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let artifacts = task
        .artifacts
        .iter()
        .map(|artifact| {
            format!(
                "- `{}`: {} (required: {})",
                artifact.path, artifact.description, artifact.required
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let happy_qa = task
        .qa
        .happy_path
        .iter()
        .map(|scenario| {
            format!(
                "- [{}] {}: {} -> {} (evidence: `{}`)",
                qa_marker(progress, task, "happy", &scenario.name),
                scenario.name,
                scenario.steps.join("; "),
                scenario.expected_result,
                scenario.evidence_path
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let failure_qa = task
        .qa
        .failure_path
        .iter()
        .map(|scenario| {
            format!(
                "- [{}] {}: {} -> {} (evidence: `{}`)",
                qa_marker(progress, task, "failure", &scenario.name),
                scenario.name,
                scenario.steps.join("; "),
                scenario.expected_result,
                scenario.evidence_path
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let adversarial_qa = task
        .qa
        .adversarial_path
        .iter()
        .map(|scenario| {
            format!(
                "- [{}] {}: {} -> {} (evidence: `{}`)",
                qa_marker(progress, task, "adversarial", &scenario.name),
                scenario.name,
                scenario.steps.join("; "),
                scenario.expected_result,
                scenario.evidence_path
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let execution_steps = task
        .execution_steps_or_legacy()
        .iter()
        .enumerate()
        .map(|(index, step)| {
            format!(
                "- {}. `{}`: {} -> {}{}",
                index + 1,
                step.step_id,
                step.action,
                step.expected_observation,
                step.evidence_path
                    .as_deref()
                    .map(|path| format!(" (evidence: `{path}`)"))
                    .unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let inputs = markdown_list(&task.inputs);
    let preconditions = markdown_list(&task.preconditions);
    let already_in_working_tree = markdown_list(&task.already_in_working_tree);
    let still_needed = markdown_list(&task.still_needed);
    let approach = markdown_list(&task.approach);
    let evidence = markdown_list(&task.evidence);
    let rollback = markdown_list(&task.rollback);
    let budget = format!(
        "max_attempts={} · max_commands={} · max_duration_seconds={}",
        task.budget
            .max_attempts
            .map_or_else(|| "unbounded".to_string(), |value| value.to_string()),
        task.budget
            .max_commands
            .map_or_else(|| "unbounded".to_string(), |value| value.to_string()),
        task.budget
            .max_duration_seconds
            .map_or_else(|| "unbounded".to_string(), |value| value.to_string())
    );
    let blocked_by = blocked_by_summary(task, progress);

    format!(
        r#"### [{}] `{}`: {}

- Goal: {}
- Role: {}
- Deliverable: {}
- WHY: {}
- HOW:
{}
- Already in working tree:
{}
- Still needed:
{}
- Dependencies: {}
- Blocked by: {}
- Step evidence required: {}
- Parallel wave: {}
- Preferred phase profile: `{:?}`
- Required capabilities: {}
- Inputs:
{}
- Preconditions:
{}
- Allowed files: {}
- Forbidden files: {}
- Write scope: {}
- Max files changed: {}
- Commit boundary: `{:?}`
- Commit message: {}
- Commit evidence: [{}] {} ({})

Must do:
{}

Evidence obligations:
{}

Rollback:
{}

Task budget: {}

Execution steps (strict order):
{}

Must not do:
{}

References:
{}

Test strategy: `{:?}`
{}
{}

Happy-path QA:
{}

Failure-path QA:
{}

Adversarial QA:
{}

Artifacts:
{}

Completion predicates:
{}"#,
        checkbox,
        task.task_id,
        task.title,
        task.goal,
        role,
        task.deliverable,
        task.rationale,
        approach,
        already_in_working_tree,
        still_needed,
        comma_list(&task.dependencies),
        blocked_by,
        task.execution_steps_evidence_required,
        task.parallel_wave,
        task.preferred_phase_profile,
        comma_list(&task.required_capabilities),
        inputs,
        preconditions,
        comma_list(&task.scope.allowed_files),
        comma_list(&task.scope.forbidden_files),
        comma_list(&task.scope.write_scope),
        task.scope.max_files_changed,
        task.commit_boundary,
        task.commit_message.as_deref().unwrap_or("not specified"),
        commit_evidence_marker(commit_satisfied),
        commit_evidence_path.unwrap_or("not evaluated"),
        commit_satisfied
            .map(|satisfied| satisfied.to_string())
            .unwrap_or_else(|| "pending".to_string()),
        must_do_checklist(&task.must_do, status),
        evidence,
        rollback,
        budget,
        execution_steps,
        markdown_list(&task.must_not_do),
        references,
        task.test.strategy,
        red,
        green,
        happy_qa,
        failure_qa,
        if adversarial_qa.is_empty() {
            "- none".to_string()
        } else {
            adversarial_qa
        },
        artifacts,
        checkbox_list(&task.completion_predicates),
    )
}

fn blocked_by_summary(task: &PlanTaskContract, progress: Option<&PlanNodeRunLedger>) -> String {
    if task.dependencies.is_empty() {
        return "none".to_string();
    }

    task.dependencies
        .iter()
        .map(|dependency| {
            let status = progress
                .and_then(|ledger| ledger.nodes.iter().find(|node| node.task_id == *dependency))
                .map(|node| format!("{:?}", node.status).to_ascii_lowercase())
                .unwrap_or_else(|| "not_recorded".to_string());
            format!("`{dependency}` ({status})")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn commit_evidence_marker(satisfied: Option<bool>) -> &'static str {
    match satisfied {
        Some(true) => "x",
        Some(false) => "!",
        None => " ",
    }
}

fn plan_task_role(task: &PlanTaskContract) -> &'static str {
    match task.preferred_phase_profile {
        crate::plan_graph::PhaseProfile::ReviewerTask
        | crate::plan_graph::PhaseProfile::ReviewerFinal => "review",
        _ => "build",
    }
}

fn markdown_list(values: &[String]) -> String {
    if values.is_empty() {
        "- none".to_string()
    } else {
        values
            .iter()
            .map(|value| format!("- {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn checkbox_list(values: &[String]) -> String {
    if values.is_empty() {
        "- [ ] none".to_string()
    } else {
        values
            .iter()
            .map(|value| format!("- [ ] {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn must_do_checklist(values: &[String], status: Option<&PlanNodeRunStatus>) -> String {
    let marker = match status {
        Some(PlanNodeRunStatus::Completed) => "x",
        Some(
            PlanNodeRunStatus::Failed | PlanNodeRunStatus::NeedsUser | PlanNodeRunStatus::Cancelled,
        ) => "!",
        Some(
            PlanNodeRunStatus::Running
            | PlanNodeRunStatus::RedVerified
            | PlanNodeRunStatus::Implemented
            | PlanNodeRunStatus::GreenVerified
            | PlanNodeRunStatus::Reviewed,
        ) => "~",
        Some(PlanNodeRunStatus::Pending | PlanNodeRunStatus::Runnable) | None => " ",
    };
    if values.is_empty() {
        "- [ ] none".to_string()
    } else {
        values
            .iter()
            .map(|value| format!("- [{marker}] {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn comma_list(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values.join(", ")
    }
}

fn coordinator_model_summary(goal: &Goal) -> String {
    goal.coordinator_model
        .as_ref()
        .map(|model| format!("{} ({}/{})", model.name, model.provider_id, model.model_id))
        .unwrap_or_else(|| "not configured".to_string())
}

fn coordinator_brief_summary(goal: &Goal) -> String {
    let Some(brief) = goal
        .coordinator_brief
        .as_deref()
        .filter(|brief| !brief.trim().is_empty())
    else {
        return "not generated".to_string();
    };
    crate::plan_graph::parse_planner_draft(brief)
        .map(|draft| {
            format!(
                "Structured PlanGraph draft for `{}` with {} task(s).",
                draft.objective,
                draft.tasks.len()
            )
        })
        .unwrap_or_else(|_| brief.to_string())
}

fn generation_guidance(detection: &LanguageDetection) -> String {
    if detection.profile == LanguageProfile::TypeScript && detection.product_type == "web_app" {
        let existing_project = detection
            .evidence
            .iter()
            .any(|evidence| evidence == "package.json");
        if existing_project {
            return [
                "- Preserve the existing TypeScript/Web stack detected from the workspace.",
                "- Prefer existing package scripts and project layout.",
                "- Ensure README.md documents install, run, build, and test commands.",
            ]
            .join("\n");
        }

        return [
            "- Default stack: Vite + React + TypeScript with npm scripts.",
            "- Use plain CSS unless the prompt explicitly asks for another styling system.",
            "- Scaffold at minimum: package.json, index.html, src/main.tsx, src/App.tsx, src/styles.css, tsconfig.json, vite.config.ts, README.md.",
            "- package.json must include dev, build, and preview scripts.",
            "- README.md must document install, local run, build, and known limits.",
        ]
        .join("\n");
    }

    "- Use the smallest local runnable implementation that matches the detected language profile."
        .to_string()
}

pub fn verification(results: &[ShellCommandResult]) -> String {
    if results.is_empty() {
        return "# Verification\n\nNo verification commands were available.\n".to_string();
    }

    let mut contents = String::from("# Verification\n\n");
    for result in results {
        contents.push_str(&format!(
            "## `{}`\n\n- success: {}\n- exit_code: {}\n- duration_ms: {}\n\n",
            result.command,
            result.success,
            result
                .exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            result.duration_ms
        ));
        if !result.stdout.trim().is_empty() {
            contents.push_str("### stdout\n\n```text\n");
            contents.push_str(&result.stdout);
            contents.push_str("\n```\n\n");
        }
        if !result.stderr.trim().is_empty() {
            contents.push_str("### stderr\n\n```text\n");
            contents.push_str(&result.stderr);
            contents.push_str("\n```\n\n");
        }
    }
    contents
}

pub fn final_report(
    goal: &Goal,
    tasks: &[Task],
    worker_result: &WorkerResult,
    diff: &DiffSnapshot,
    scope_check: &ScopeCheck,
    verification_results: &[ShellCommandResult],
) -> String {
    let goal_summary = goal_summary_text(&goal.summary);
    let next_step = goal_next_step(&goal.status);
    let verification_summary = if verification_results.is_empty() {
        "No verification commands were available.".to_string()
    } else if verification_results.iter().all(|result| result.success) {
        "All verification commands passed.".to_string()
    } else {
        "One or more verification commands failed.".to_string()
    };

    let changed_files = if diff.changed_files.is_empty() {
        "- No code file changes detected outside `.gear/`.".to_string()
    } else {
        diff.changed_files
            .iter()
            .map(|path| format!("- `{path}`"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let scope_summary = if scope_check.forbidden_touches.is_empty()
        && scope_check.outside_allowed_paths.is_empty()
        && !scope_check.max_files_exceeded
    {
        "Scope check passed.".to_string()
    } else {
        format!(
            "Scope check failed. forbidden_touches={}, outside_allowed_paths={}, changed_file_count={}, max_files_exceeded={}",
            scope_check.forbidden_touches.len(),
            scope_check.outside_allowed_paths.len(),
            scope_check.changed_file_count,
            scope_check.max_files_exceeded
        )
    };

    let task_lines = tasks
        .iter()
        .map(|task| format!("- `{}`: {:?} / {:?}", task.id, task.kind, task.status))
        .collect::<Vec<_>>()
        .join("\n");
    let evidence_chain = final_report_evidence(tasks, worker_result);

    format!(
        r#"# Final Report

Goal: `{}`

Status: `{}`

## Decision

- summary: {}
- next step: {}

## Worker

- status: `{}`
- summary: {}
- packet: `{}`
- prompt: `{}`

## Coordinator Model

{}

## Coordinator Brief

{}

## Verification

{}

## Diff

{}

## Scope

{}

## Tasks

{}

## Evidence Chain

{}

## Known Limits

- ACP server integration is intentionally deferred until the local CLI runtime is stable.
- Codex, Claude Code, CodeGraph, and context-mode workers are not hard dependencies in this MVP.
"#,
        goal.id,
        goal.status.as_str(),
        goal_summary,
        next_step,
        worker_result.status.as_str(),
        worker_result.summary,
        worker_result.packet_path.display(),
        worker_result.prompt_path.display(),
        coordinator_model_summary(goal),
        coordinator_brief_summary(goal),
        verification_summary,
        changed_files,
        scope_summary,
        task_lines,
        evidence_chain
    )
}

fn final_report_evidence(tasks: &[Task], worker_result: &WorkerResult) -> String {
    let mut worker_evidence = vec![
        (
            "packet",
            worker_result.packet_path.to_string_lossy().to_string(),
        ),
        (
            "prompt",
            worker_result.prompt_path.to_string_lossy().to_string(),
        ),
        (
            "result",
            worker_result.result_path.to_string_lossy().to_string(),
        ),
        (
            "outcome",
            worker_result.outcome_path.to_string_lossy().to_string(),
        ),
    ];

    for (label, file_name) in [
        ("transcript", "transcript.jsonl"),
        ("tool_events", "tool-events.jsonl"),
        ("partial_output", "partial-output.md"),
    ] {
        if let Some(path) = worker_artifact_path(worker_result, file_name)
            && path.exists()
        {
            worker_evidence.push((label, path.to_string_lossy().to_string()));
        }
    }

    let worker_evidence = worker_evidence
        .into_iter()
        .map(|(label, path)| format!("- worker_{label}: `{path}`"))
        .collect::<Vec<_>>();

    let task_evidence = tasks
        .iter()
        .filter(|task| !task.outputs.evidence.is_empty())
        .flat_map(|task| {
            task.outputs
                .evidence
                .iter()
                .map(move |path| format!("- {} / {:?}: `{path}`", task.id, task.kind))
        })
        .collect::<Vec<_>>();

    worker_evidence
        .into_iter()
        .chain(task_evidence)
        .collect::<Vec<_>>()
        .join("\n")
}

fn worker_artifact_path(worker_result: &WorkerResult, file_name: &str) -> Option<PathBuf> {
    worker_result
        .result_path
        .parent()
        .or_else(|| worker_result.outcome_path.parent())
        .map(|artifact_dir| artifact_dir.join(file_name))
}

fn goal_summary_text(summary: &str) -> String {
    if summary.trim().is_empty() {
        "No final decision summary was recorded.".to_string()
    } else {
        summary.to_string()
    }
}

fn goal_next_step(goal_status: &GoalStatus) -> &'static str {
    match goal_status {
        GoalStatus::Complete => "No further action is required.",
        GoalStatus::Limited => {
            "Split the goal, raise the budget, or narrow the scope before retrying."
        }
        GoalStatus::Blocked => "Resolve the scope or forbidden-path issue, then rerun the goal.",
        GoalStatus::NeedsUser => "Provide the missing user input or required worker configuration.",
        GoalStatus::Failed => {
            "Inspect the failure artifact, repair the root cause, and rerun the goal."
        }
        GoalStatus::Draft | GoalStatus::Planning | GoalStatus::Running | GoalStatus::Verifying => {
            "Continue the goal loop or re-evaluate the current plan before retrying."
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        criterion_marker, goal_next_step, goal_summary_text, plan, plan_with_progress,
        render_plan_task,
    };
    use crate::languages::{LanguageDetection, LanguageProfile};
    use crate::plan_graph::deterministic_fallback_draft;
    use crate::state::{
        Budget, CriterionEvidenceStatus, Goal, GoalStatus, PlanNodeRunLedger, PlanNodeRunStatus,
        Scope, Task, TaskInputs, TaskKind, TaskOutputs, TaskStatus,
    };
    use crate::tools::{DiffSnapshot, ScopeCheck, ShellCommandResult};
    use crate::workers::WorkerResult;

    #[test]
    fn goal_summary_text_uses_fallback_for_empty_summary() {
        assert_eq!(
            goal_summary_text(""),
            "No final decision summary was recorded."
        );
    }

    #[test]
    fn goal_summary_text_preserves_summary_text() {
        assert_eq!(goal_summary_text("done"), "done");
    }

    #[test]
    fn goal_next_step_matches_goal_status() {
        assert_eq!(
            goal_next_step(&GoalStatus::Limited),
            "Split the goal, raise the budget, or narrow the scope before retrying."
        );
        assert_eq!(
            goal_next_step(&GoalStatus::Blocked),
            "Resolve the scope or forbidden-path issue, then rerun the goal."
        );
        assert_eq!(
            goal_next_step(&GoalStatus::NeedsUser),
            "Provide the missing user input or required worker configuration."
        );
    }

    #[test]
    fn product_plan_renders_closed_world_task_contract() {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let draft = deterministic_fallback_draft(
            "Implement feature",
            &scope,
            &["cargo test feature".to_string()],
        );
        let markdown = render_plan_task(&draft.tasks[0], None, None, None, None);
        assert!(markdown.contains("Must do:"));
        assert!(markdown.contains("- Role: build"));
        assert!(markdown.contains("- WHY: The requested change is not implemented"));
        assert!(markdown.contains("- HOW:\n- Inspect the existing seam"));
        assert!(markdown.contains("- Blocked by: none"));
        assert!(markdown.contains("- Inputs:"));
        assert!(markdown.contains("- Preconditions:"));
        assert!(markdown.contains("Evidence obligations:"));
        assert!(markdown.contains("Rollback:"));
        assert!(markdown.contains("Task budget:"));
        assert!(markdown.contains("Execution steps (strict order):"));
        assert!(markdown.contains("Must not do:"));
        assert!(markdown.contains("Test strategy:"));
        assert!(markdown.contains("Happy-path QA:"));
        assert!(markdown.contains("Failure-path QA:"));
        assert!(markdown.contains("Completion predicates:"));
        assert!(markdown.contains("Commit evidence:"));
        assert!(markdown.contains("- [ ]"));
    }

    #[test]
    fn product_plan_marks_unrecorded_dependency_as_blocking() {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let draft = deterministic_fallback_draft(
            "Implement feature",
            &scope,
            &["cargo test feature".to_string()],
        );
        let mut task = draft.tasks[0].clone();
        task.dependencies.push("task_missing".to_string());
        let markdown = render_plan_task(&task, None, None, None, None);
        assert!(markdown.contains("`task_missing` (not_recorded)"));
    }

    #[test]
    fn product_plan_projects_task_status_into_must_do_markers() {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let draft = deterministic_fallback_draft(
            "Implement feature",
            &scope,
            &["cargo test feature".to_string()],
        );
        let markdown = render_plan_task(
            &draft.tasks[0],
            None,
            Some(&PlanNodeRunStatus::Completed),
            None,
            None,
        );
        assert!(markdown.contains("Must do:\n- [x]"));
    }

    #[test]
    fn product_plan_renders_omo_final_verification_wave() {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let draft = deterministic_fallback_draft("Implement feature", &scope, &[]);
        let graph = crate::plan_graph::PlanGraph::seal(
            "goal_plan_format",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            draft,
        )
        .expect("fallback plan should validate");
        let goal = Goal {
            id: "goal_plan_format".to_string(),
            title: "plan format".to_string(),
            status: GoalStatus::Planning,
            workspace: "/workspace".to_string(),
            created_at: "2026-07-15T00:00:00Z".to_string(),
            updated_at: "2026-07-15T00:00:00Z".to_string(),
            request: "Implement feature".to_string(),
            product_type: "app".to_string(),
            language_profile: "rust".to_string(),
            success_criteria: vec!["works".to_string()],
            budget: Budget::default(),
            current_task_id: None,
            coordinator_model: None,
            coordinator_brief: None,
            summary: String::new(),
        };
        let detection = LanguageDetection {
            profile: LanguageProfile::Rust,
            product_type: "app".to_string(),
            evidence: Vec::new(),
            verification_commands: Vec::new(),
        };
        let markdown = plan(&goal, &graph, &detection);
        for check in [
            "F1. Plan compliance audit",
            "F2. Code quality review",
            "F3. Real manual QA",
            "F4. Scope fidelity",
        ] {
            assert!(markdown.contains(check));
        }
        assert!(markdown.contains("## Dependency matrix"));
        assert!(markdown.contains("## Planning context"));
        assert!(markdown.contains("### Plan generation receipt"));
        assert!(markdown.contains("### Work-order protocol"));
        assert!(markdown.contains("| Work order | Dependencies | Parallel wave |"));
        assert!(markdown.contains("## Milestones"));
        assert!(markdown.contains("Final wave: F1-F4 verification and final acceptance"));
        assert!(markdown.contains("## Acceptance checklist"));
        assert!(markdown.contains("## Rollback Plan"));
        assert!(markdown.contains("Already in working tree:"));
        assert!(markdown.contains("Still needed:"));
    }

    #[test]
    fn product_plan_projects_runtime_todo_status() {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let draft = deterministic_fallback_draft("Implement feature", &scope, &[]);
        let graph = crate::plan_graph::PlanGraph::seal(
            "goal_plan_progress",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            draft,
        )
        .expect("fallback plan should validate");
        let goal = Goal {
            id: "goal_plan_progress".to_string(),
            title: "plan progress".to_string(),
            status: GoalStatus::Running,
            workspace: "/workspace".to_string(),
            created_at: "2026-07-15T00:00:00Z".to_string(),
            updated_at: "2026-07-15T00:00:00Z".to_string(),
            request: "Implement feature".to_string(),
            product_type: "app".to_string(),
            language_profile: "rust".to_string(),
            success_criteria: vec!["works".to_string()],
            budget: Budget::default(),
            current_task_id: None,
            coordinator_model: None,
            coordinator_brief: None,
            summary: String::new(),
        };
        let detection = LanguageDetection {
            profile: LanguageProfile::Rust,
            product_type: "app".to_string(),
            evidence: Vec::new(),
            verification_commands: Vec::new(),
        };
        let mut ledger = PlanNodeRunLedger::from_plan("goal_plan_progress", "epoch-1", &graph)
            .expect("ledger should be created");
        ledger.nodes[0].status = PlanNodeRunStatus::Completed;
        let markdown = plan_with_progress(&goal, &graph, &detection, Some(&ledger));
        assert!(markdown.contains("### [x]"));
    }

    #[test]
    fn product_plan_projects_acceptance_evidence_status() {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let draft = deterministic_fallback_draft("Implement feature", &scope, &[]);
        let graph = crate::plan_graph::PlanGraph::seal(
            "goal_acceptance_status",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            draft,
        )
        .expect("fallback plan should validate");
        let task = &graph.draft.tasks[0];
        let mut ledger = PlanNodeRunLedger::from_plan("goal_acceptance_status", "epoch-1", &graph)
            .expect("ledger should be created");
        ledger.nodes[0].attempt = 1;
        ledger.nodes[0]
            .record_criterion_evidence(
                &task.completion_predicates[0],
                CriterionEvidenceStatus::Pass,
                1,
                "evidence.md",
                &"0".repeat(64),
            )
            .expect("criterion evidence should seal");
        assert_eq!(
            criterion_marker(Some(&ledger), &task.task_id, &task.completion_predicates[0]),
            "x"
        );
    }

    #[test]
    fn final_report_includes_decision_guidance() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp_dir.path().join("packet.md"), "packet").expect("packet");
        std::fs::write(temp_dir.path().join("prompt.md"), "prompt").expect("prompt");
        std::fs::write(temp_dir.path().join("result.json"), "{}").expect("result");
        std::fs::write(temp_dir.path().join("outcome.json"), "{}").expect("outcome");
        std::fs::write(
            temp_dir.path().join("transcript.jsonl"),
            "{\"event\":\"turn_started\"}\n{\"event\":\"turn_finished\"}\n",
        )
        .expect("transcript");
        std::fs::write(
            temp_dir.path().join("tool-events.jsonl"),
            "{\"event\":\"tool_call_started\"}\n",
        )
        .expect("tool events");
        std::fs::write(temp_dir.path().join("partial-output.md"), "partial output")
            .expect("partial output");
        let goal = Goal {
            id: "goal_test".to_string(),
            title: "test".to_string(),
            status: GoalStatus::Limited,
            workspace: "/workspace".to_string(),
            created_at: "2026-07-09T00:00:00Z".to_string(),
            updated_at: "2026-07-09T00:00:00Z".to_string(),
            request: "make it work".to_string(),
            product_type: "app".to_string(),
            language_profile: "rust".to_string(),
            success_criteria: vec!["works".to_string()],
            budget: Budget::default(),
            current_task_id: None,
            coordinator_model: None,
            coordinator_brief: None,
            summary: "Goal reached the iteration limit.".to_string(),
        };
        let task = Task {
            id: "task_1".to_string(),
            goal_id: goal.id.clone(),
            parent_task_id: None,
            title: "Document results".to_string(),
            kind: TaskKind::Document,
            status: TaskStatus::Complete,
            assigned_worker: None,
            attempt: 1,
            scope: Scope::new(vec![], vec![], usize::MAX),
            inputs: TaskInputs::default(),
            outputs: TaskOutputs::default(),
        };
        let worker_result = WorkerResult {
            status: crate::workers::WorkerStatus::Succeeded,
            command: Some("worker".to_string()),
            exit_code: Some(0),
            summary: "worker finished".to_string(),
            packet_path: temp_dir.path().join("packet.md"),
            prompt_path: temp_dir.path().join("prompt.md"),
            stdout_path: None,
            stderr_path: None,
            last_message_path: None,
            result_path: temp_dir.path().join("result.json"),
            outcome_path: temp_dir.path().join("outcome.json"),
        };
        let report = super::final_report(
            &goal,
            &[task],
            &worker_result,
            &DiffSnapshot {
                is_git_repo: true,
                status: "M crates/gearbox_agent/src/product.rs".to_string(),
                changed_files: vec!["crates/gearbox_agent/src/product.rs".to_string()],
                diff_hash: None,
            },
            &ScopeCheck::default(),
            &[ShellCommandResult {
                command: "cargo test -p gearbox_agent".to_string(),
                exit_code: Some(0),
                success: true,
                stdout: "ok".to_string(),
                stderr: String::new(),
                duration_ms: 12,
            }],
        );

        assert!(report.contains("## Decision"));
        assert!(report.contains("Goal reached the iteration limit."));
        assert!(
            report
                .contains("Split the goal, raise the budget, or narrow the scope before retrying.")
        );
        assert!(report.contains("worker_transcript"));
        assert!(report.contains("transcript.jsonl"));
        assert!(report.contains("tool-events.jsonl"));
        assert!(report.contains("partial-output.md"));
    }
}
