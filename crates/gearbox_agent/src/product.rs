use crate::languages::LanguageDetection;
use crate::state::{Goal, Task};
use crate::tools::{DiffSnapshot, ScopeCheck, ShellCommandResult};
use crate::workers::WorkerResult;

pub fn spec(goal: &Goal, detection: &LanguageDetection) -> String {
    format!(
        r#"# Spec

## Original Request

{}

## Gear Assumptions

- Product type: {}
- Language profile: {}
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
"#,
        goal.request,
        goal.product_type,
        detection.profile.as_str(),
        if detection.evidence.is_empty() {
            "none".to_string()
        } else {
            detection.evidence.join(", ")
        },
        goal.success_criteria
            .iter()
            .map(|criterion| format!("- {criterion}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

pub fn plan(goal: &Goal, tasks: &[Task], detection: &LanguageDetection) -> String {
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

- Confirm the workspace facts with deterministic tools.
- Send bounded implementation work to the configured opencode worker.
- Inspect diff after the worker returns.
- Run Gear-owned verification commands.
- Create a repair task if verification fails.
- Produce final delivery notes.

## Verification Commands

{}
"#,
        goal.id, task_lines, commands
    )
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

    format!(
        r#"# Final Report

Goal: `{}`

Status: `{}`

## Worker

- status: `{}`
- summary: {}
- packet: `{}`
- prompt: `{}`

## Verification

{}

## Diff

{}

## Scope

{}

## Tasks

{}

## Known Limits

- ACP server integration is intentionally deferred until the local CLI runtime is stable.
- Codex, Claude Code, CodeGraph, and context-mode workers are not hard dependencies in this MVP.
"#,
        goal.id,
        goal.status.as_str(),
        worker_result.status.as_str(),
        worker_result.summary,
        worker_result.packet_path.display(),
        worker_result.prompt_path.display(),
        verification_summary,
        changed_files,
        scope_summary,
        task_lines
    )
}
