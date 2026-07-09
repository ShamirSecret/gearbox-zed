use crate::languages::{LanguageDetection, LanguageProfile};
use crate::state::{Goal, GoalStatus, Task};
use crate::tools::{DiffSnapshot, ScopeCheck, ShellCommandResult};
use crate::workers::WorkerResult;
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

pub fn plan(goal: &Goal, tasks: &[Task], detection: &LanguageDetection) -> String {
    let generation_guidance = generation_guidance(detection);
    let task_lines = tasks
        .iter()
        .map(|task| format!("- `{}`: {} ({:?})", task.id, task.title, task.kind))
        .collect::<Vec<_>>()
        .join("\n");
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

    format!(
        r#"# Plan

Goal: `{}`

## Execution Tasks

{}

## Default Build Path

- Use the coordinator model recorded below for Gear planning and review context when available.
- Confirm the workspace facts with deterministic tools.
- Follow the generation guidance below before writing code.
- Send bounded implementation work to the configured worker adapter.
- Inspect diff after the worker returns.
- Run Gear-owned verification commands.
- Create a repair task if verification fails.
- Produce final delivery notes.

## Generation Guidance

{}

## Coordinator Model

{}

## Coordinator Brief

{}

## Verification Commands

{}
"#,
        goal.id,
        task_lines,
        generation_guidance,
        coordinator_model_summary(goal),
        coordinator_brief_summary(goal),
        commands
    )
}

fn coordinator_model_summary(goal: &Goal) -> String {
    goal.coordinator_model
        .as_ref()
        .map(|model| format!("{} ({}/{})", model.name, model.provider_id, model.model_id))
        .unwrap_or_else(|| "not configured".to_string())
}

fn coordinator_brief_summary(goal: &Goal) -> String {
    goal.coordinator_brief
        .as_deref()
        .filter(|brief| !brief.trim().is_empty())
        .unwrap_or("not generated")
        .to_string()
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
        "- No code file changes detected outside `.gearbox-agent/`.".to_string()
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
    use super::{goal_next_step, goal_summary_text};
    use crate::state::{
        Budget, Goal, GoalStatus, Scope, Task, TaskInputs, TaskKind, TaskOutputs, TaskStatus,
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
