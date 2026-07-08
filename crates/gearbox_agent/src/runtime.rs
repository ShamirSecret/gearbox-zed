use std::{path::PathBuf, sync::Arc};

use anyhow::{Context as _, Result, bail};
use serde_json::json;

use crate::languages::{LanguageDetection, detect_with_request};
use crate::product;
use crate::state::{
    Budget, CoordinatorModel, Event, EventKind, Goal, GoalStatus, Scope, Session, StateStore, Task,
    TaskInputs, TaskKind, TaskOutputs, TaskStatus, event, id_timestamp, timestamp,
};
use crate::tools::{
    CancellationToken, DiffSnapshot, ShellCommandResult, check_scope, git_snapshot,
    run_shell_command_with_env_and_cancellation,
};
use crate::workers::{WorkerConfig, WorkerRegistry, WorkerRunRequest, WorkerStatus};

pub type EventSink = Arc<dyn Fn(&Event) + Send + Sync + 'static>;
pub const DEFAULT_MAX_ITERATIONS: usize = 2;

#[derive(Clone)]
pub struct RunOptions {
    pub request: String,
    pub workspace: PathBuf,
    pub verification_commands: Vec<String>,
    pub worker: WorkerConfig,
    pub allowed_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub max_files_changed: usize,
    pub install_dependencies: bool,
    pub event_sink: Option<EventSink>,
    pub cancellation_token: Option<CancellationToken>,
    pub max_iterations: usize,
    pub coordinator_model: Option<CoordinatorModel>,
    pub coordinator_brief: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RunOutcome {
    pub goal_id: String,
    pub session_id: String,
    pub status: GoalStatus,
    pub artifacts_root: PathBuf,
    pub final_report_path: PathBuf,
    pub events_path: PathBuf,
}

pub struct Orchestrator;

impl Orchestrator {
    pub fn run(options: RunOptions) -> Result<RunOutcome> {
        if options.request.trim().is_empty() {
            bail!("prompt cannot be empty");
        }
        check_run_cancelled(options.cancellation_token.as_ref())?;

        let workspace = options.workspace.canonicalize().with_context(|| {
            format!(
                "failed to resolve workspace {}",
                options.workspace.display()
            )
        })?;
        if !workspace.is_dir() {
            bail!("workspace is not a directory: {}", workspace.display());
        }

        let store = StateStore::new(&workspace);
        store.initialize()?;
        check_run_cancelled(options.cancellation_token.as_ref())?;

        let id_suffix = id_timestamp();
        let session_id = format!("ses_{id_suffix}");
        let goal_id = format!("goal_{id_suffix}");
        let scope = Scope::new(
            options.allowed_paths.clone(),
            options.forbidden_paths.clone(),
            options.max_files_changed,
        );
        let max_iterations = options.max_iterations.max(1);
        let detection = detect_with_request(
            &workspace,
            &options.verification_commands,
            options.install_dependencies,
            &options.request,
        )?;
        let now = timestamp();

        let mut goal = Goal {
            id: goal_id.clone(),
            title: title_from_request(&options.request),
            status: GoalStatus::Planning,
            workspace: workspace.to_string_lossy().to_string(),
            created_at: now.clone(),
            updated_at: now.clone(),
            request: options.request.clone(),
            product_type: detection.product_type.clone(),
            language_profile: detection.profile.as_str().to_string(),
            success_criteria: success_criteria(&detection),
            budget: Budget::default(),
            current_task_id: None,
            coordinator_model: options.coordinator_model.clone(),
            coordinator_brief: options.coordinator_brief.clone(),
            summary: String::new(),
        };

        let session = Session {
            id: session_id.clone(),
            workspace: workspace.to_string_lossy().to_string(),
            created_at: now.clone(),
            updated_at: now,
            current_goal_id: goal_id.clone(),
        };

        store.write_session(&session)?;
        store.write_goal(&goal)?;
        append_event(
            &store,
            &options.event_sink,
            event(
                &session_id,
                Some(&goal_id),
                None,
                EventKind::GoalCreated,
                format!("Created {}", goal.id),
                json!({
                    "workspace": workspace.to_string_lossy(),
                    "language_profile": detection.profile.as_str(),
                    "evidence": &detection.evidence,
                    "coordinator_model": &goal.coordinator_model,
                    "coordinator_brief": &goal.coordinator_brief,
                }),
            ),
        )?;

        let mut tasks = initial_tasks(&goal_id, &scope);
        store.write_tasks(&goal_id, &tasks)?;

        let spec_path =
            store.write_artifact(&goal_id, "spec.md", &product::spec(&goal, &detection))?;
        complete_task(&mut tasks, "task_001", |task| {
            task.outputs.summary = "Spec artifact created.".to_string();
            task.outputs
                .evidence
                .push(spec_path.to_string_lossy().to_string());
        });
        append_event(
            &store,
            &options.event_sink,
            event(
                &session_id,
                Some(&goal_id),
                Some("task_001"),
                EventKind::SpecCreated,
                "Spec artifact created",
                json!({ "path": spec_path.to_string_lossy() }),
            ),
        )?;

        set_task_inputs(&mut tasks, spec_path.to_string_lossy().to_string(), None);
        let plan_path = store.write_artifact(
            &goal_id,
            "plan.md",
            &product::plan(&goal, &tasks, &detection),
        )?;
        complete_task(&mut tasks, "task_002", |task| {
            task.outputs.summary = "Plan artifact created.".to_string();
            task.outputs
                .evidence
                .push(plan_path.to_string_lossy().to_string());
        });
        set_task_inputs(
            &mut tasks,
            spec_path.to_string_lossy().to_string(),
            Some(plan_path.to_string_lossy().to_string()),
        );
        store.write_tasks(&goal_id, &tasks)?;
        append_event(
            &store,
            &options.event_sink,
            event(
                &session_id,
                Some(&goal_id),
                Some("task_002"),
                EventKind::PlanCreated,
                "Plan artifact created",
                json!({ "path": plan_path.to_string_lossy() }),
            ),
        )?;

        let before_diff = git_snapshot(&workspace)?;
        let mut after_diff = before_diff.clone();
        let mut scope_check = check_scope(&after_diff, &scope);
        let mut worker_result = None;
        let mut verification_results = Vec::new();
        let mut last_verification_path = None;
        let mut final_evaluation = None;
        let worker_registry = WorkerRegistry;

        for iteration in 1..=max_iterations {
            check_run_cancelled(options.cancellation_token.as_ref())?;
            let worker_task_id = if iteration == 1 {
                "task_003".to_string()
            } else {
                let verification_path = last_verification_path
                    .as_deref()
                    .context("missing verification artifact for repair iteration")?;
                let repair_task_id =
                    add_repair_task(&mut tasks, &goal_id, &scope, iteration, verification_path);
                store.write_tasks(&goal_id, &tasks)?;
                append_event(
                    &store,
                    &options.event_sink,
                    event(
                        &session_id,
                        Some(&goal_id),
                        Some(&repair_task_id),
                        EventKind::RepairStarted,
                        format!("Repair iteration {iteration} started"),
                        json!({
                            "iteration": iteration,
                            "verification_path": verification_path.to_string_lossy(),
                        }),
                    ),
                )?;
                repair_task_id
            };

            start_task(&mut tasks, &worker_task_id);
            goal.status = GoalStatus::Running;
            goal.current_task_id = Some(worker_task_id.clone());
            goal.updated_at = timestamp();
            store.write_goal(&goal)?;
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&worker_task_id),
                    EventKind::WorkerStarted,
                    if iteration == 1 {
                        "Prepared implementation worker packet".to_string()
                    } else {
                        "Prepared repair worker packet".to_string()
                    },
                    json!({
                        "iteration": iteration,
                        "before": &before_diff,
                        "current": &after_diff,
                    }),
                ),
            )?;

            let worker_task = tasks
                .iter()
                .find(|task| task.id == worker_task_id)
                .context("missing worker task")?
                .clone();
            let worker_request = if iteration == 1 {
                options.request.clone()
            } else {
                repair_request(
                    &options.request,
                    iteration,
                    last_verification_path.as_deref(),
                )
            };
            let iteration_worker_result = worker_registry.run(WorkerRunRequest {
                store: &store,
                workspace: &workspace,
                task: &worker_task,
                goal: &worker_request,
                verification_commands: &detection.verification_commands,
                config: &options.worker,
                cancellation_token: options.cancellation_token.as_ref(),
                coordinator_model: goal.coordinator_model.as_ref(),
                coordinator_brief: goal.coordinator_brief.as_deref(),
            })?;

            update_worker_task(
                &mut tasks,
                &worker_task_id,
                &iteration_worker_result.status,
                &iteration_worker_result.summary,
            );
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&worker_task_id),
                    match iteration_worker_result.status {
                        WorkerStatus::Succeeded => EventKind::WorkerFinished,
                        WorkerStatus::Skipped => EventKind::WorkerWaiting,
                        WorkerStatus::Failed => EventKind::WorkerFailed,
                    },
                    iteration_worker_result.summary.clone(),
                    json!({
                        "iteration": iteration,
                        "status": iteration_worker_result.status.as_str(),
                        "packet_path": iteration_worker_result.packet_path.to_string_lossy(),
                        "prompt_path": iteration_worker_result.prompt_path.to_string_lossy(),
                    }),
                ),
            )?;
            worker_result = Some(iteration_worker_result);

            after_diff = git_snapshot(&workspace)?;
            scope_check = check_scope(&after_diff, &scope);
            check_run_cancelled(options.cancellation_token.as_ref())?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&worker_task_id),
                    EventKind::DiffDetected,
                    "Diff snapshot captured",
                    json!({
                        "iteration": iteration,
                        "before": &before_diff,
                        "after": &after_diff,
                        "scope_check": &scope_check,
                    }),
                ),
            )?;

            start_task(&mut tasks, "task_004");
            goal.status = GoalStatus::Verifying;
            goal.current_task_id = Some("task_004".to_string());
            goal.updated_at = timestamp();
            store.write_goal(&goal)?;
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some("task_004"),
                    EventKind::VerificationStarted,
                    "Verification started",
                    json!({
                        "iteration": iteration,
                        "commands": detection.verification_commands,
                    }),
                ),
            )?;

            verification_results = run_verification(
                &workspace,
                &detection.verification_commands,
                options.cancellation_token.as_ref(),
            )?;
            let verification_artifact = if iteration == 1 {
                "verification.md".to_string()
            } else {
                format!("verification-iteration-{iteration}.md")
            };
            let verification_path = store.write_artifact(
                &goal_id,
                &verification_artifact,
                &product::verification(&verification_results),
            )?;

            let verification_passed = !verification_results.is_empty()
                && verification_results.iter().all(|result| result.success);
            update_verification_task(
                &mut tasks,
                &verification_results,
                verification_path.to_string_lossy().to_string(),
                verification_passed,
            );

            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some("task_004"),
                    if verification_passed {
                        EventKind::VerificationPassed
                    } else {
                        EventKind::VerificationFailed
                    },
                    if verification_passed {
                        "Verification passed".to_string()
                    } else {
                        "Verification failed or was unavailable".to_string()
                    },
                    json!({
                        "iteration": iteration,
                        "verification_path": verification_path.to_string_lossy(),
                    }),
                ),
            )?;

            last_verification_path = Some(verification_path.clone());
            let evaluation = evaluate_goal(
                verification_passed,
                &worker_result
                    .as_ref()
                    .context("missing worker result for goal evaluation")?
                    .status,
                options.worker.require_worker,
                &scope_check,
                iteration,
                max_iterations,
            );
            let review_path = store.write_artifact(
                &goal_id,
                &format!("goal-review-iteration-{iteration}.md"),
                &goal_review_artifact(
                    iteration,
                    max_iterations,
                    &evaluation,
                    worker_result
                        .as_ref()
                        .context("missing worker result for goal review")?,
                    &scope_check,
                    &verification_results,
                ),
            )?;
            add_review_task(
                &mut tasks,
                &goal_id,
                &scope,
                iteration,
                &review_path,
                &evaluation.summary,
            );
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&review_task_id(iteration)),
                    EventKind::TaskStarted,
                    "Goal check completed",
                    json!({
                        "iteration": iteration,
                        "status": evaluation.status.as_str(),
                        "should_continue": evaluation.should_continue,
                        "review_path": review_path.to_string_lossy(),
                    }),
                ),
            )?;

            let should_continue = evaluation.should_continue;
            final_evaluation = Some(evaluation);
            if !should_continue {
                break;
            }
        }

        let final_evaluation = final_evaluation.context("Gear loop did not evaluate the goal")?;
        let worker_result = worker_result.context("Gear loop did not produce a worker result")?;
        goal.status = final_evaluation.status;
        goal.current_task_id = None;
        goal.updated_at = timestamp();
        goal.summary = final_evaluation.summary;

        let final_report = product::final_report(
            &goal,
            &tasks,
            &worker_result,
            &after_diff,
            &scope_check,
            &verification_results,
        );
        let final_report_path = store.write_artifact(&goal_id, "final-report.md", &final_report)?;
        complete_task(&mut tasks, "task_006", |task| {
            task.outputs.summary = "Final report artifact created.".to_string();
            task.outputs
                .evidence
                .push(final_report_path.to_string_lossy().to_string());
        });
        store.write_goal(&goal)?;
        store.write_tasks(&goal_id, &tasks)?;

        let final_event_kind = match goal.status {
            GoalStatus::Complete => EventKind::GoalCompleted,
            GoalStatus::Limited => EventKind::GoalLimited,
            _ => EventKind::GoalBlocked,
        };
        append_event(
            &store,
            &options.event_sink,
            event(
                &session_id,
                Some(&goal_id),
                None,
                final_event_kind,
                goal.summary.clone(),
                json!({
                    "status": goal.status.as_str(),
                    "final_report_path": final_report_path.to_string_lossy(),
                }),
            ),
        )?;

        let status = goal.status.clone();
        let artifacts_root = store.artifact_dir(&goal.id);
        Ok(RunOutcome {
            goal_id,
            session_id: session_id.clone(),
            status,
            artifacts_root,
            final_report_path,
            events_path: store.events_path(&session_id),
        })
    }
}

fn title_from_request(request: &str) -> String {
    let trimmed = request.trim();
    let mut title = String::new();
    for character in trimmed.chars().take(60) {
        title.push(character);
    }
    if title.is_empty() {
        "Gear goal".to_string()
    } else {
        title
    }
}

fn success_criteria(detection: &LanguageDetection) -> Vec<String> {
    let mut criteria = vec![
        "Artifacts include spec, plan, verification, and final report.".to_string(),
        "Diff is checked against the task scope.".to_string(),
        "Known failures are recorded instead of hidden.".to_string(),
    ];
    match detection.profile {
        crate::languages::LanguageProfile::TypeScript => {
            criteria.push("TypeScript project verification is recorded.".to_string());
        }
        crate::languages::LanguageProfile::Python => {
            criteria.push("Python project verification is recorded.".to_string());
        }
        crate::languages::LanguageProfile::Rust => {
            criteria.push("Rust project verification is recorded.".to_string());
        }
        crate::languages::LanguageProfile::Unknown => {
            criteria.push(
                "A verification command is supplied or the goal asks for user input.".to_string(),
            );
        }
    }
    criteria
}

fn initial_tasks(goal_id: &str, scope: &Scope) -> Vec<Task> {
    [
        ("task_001", "Generate minimal spec", TaskKind::Spec, None),
        ("task_002", "Generate executable plan", TaskKind::Plan, None),
        (
            "task_003",
            "Dispatch bounded implementation packet",
            TaskKind::Edit,
            Some("opencode".to_string()),
        ),
        (
            "task_004",
            "Run Gear-owned verification",
            TaskKind::Verify,
            None,
        ),
        (
            "task_006",
            "Write delivery report",
            TaskKind::Document,
            None,
        ),
    ]
    .into_iter()
    .map(|(id, title, kind, assigned_worker)| Task {
        id: id.to_string(),
        goal_id: goal_id.to_string(),
        title: title.to_string(),
        kind,
        status: TaskStatus::Pending,
        assigned_worker,
        attempt: 1,
        scope: scope.clone(),
        inputs: TaskInputs::default(),
        outputs: TaskOutputs::default(),
    })
    .collect()
}

fn start_task(tasks: &mut [Task], task_id: &str) {
    if let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) {
        task.status = TaskStatus::Running;
    }
}

fn complete_task(tasks: &mut [Task], task_id: &str, update: impl FnOnce(&mut Task)) {
    if let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) {
        update(task);
        task.status = TaskStatus::Complete;
    }
}

fn set_task_inputs(tasks: &mut [Task], spec_path: String, plan_path: Option<String>) {
    for task in tasks {
        task.inputs.spec_path = Some(spec_path.clone());
        task.inputs.plan_path = plan_path.clone();
    }
}

fn update_worker_task(tasks: &mut [Task], task_id: &str, status: &WorkerStatus, summary: &str) {
    if let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) {
        task.status = match status {
            WorkerStatus::Succeeded => TaskStatus::Complete,
            WorkerStatus::Skipped => TaskStatus::Skipped,
            WorkerStatus::Failed => TaskStatus::Failed,
        };
        task.outputs.summary = summary.to_string();
    }
}

fn run_verification(
    workspace: &std::path::Path,
    commands: &[String],
    cancellation_token: Option<&CancellationToken>,
) -> Result<Vec<ShellCommandResult>> {
    let env = std::collections::HashMap::new();
    commands
        .iter()
        .map(|command| {
            run_shell_command_with_env_and_cancellation(
                workspace,
                command,
                &env,
                cancellation_token,
            )
        })
        .collect()
}

fn check_run_cancelled(cancellation_token: Option<&CancellationToken>) -> Result<()> {
    if cancellation_token.is_some_and(CancellationToken::is_cancelled) {
        bail!("Gear run cancelled");
    }
    Ok(())
}

fn update_verification_task(
    tasks: &mut [Task],
    results: &[ShellCommandResult],
    verification_path: String,
    verification_passed: bool,
) {
    if let Some(task) = tasks.iter_mut().find(|task| task.id == "task_004") {
        task.status = if verification_passed {
            TaskStatus::Complete
        } else {
            TaskStatus::Failed
        };
        task.outputs.commands_run = results.iter().map(ShellCommandResult::record).collect();
        task.outputs.evidence.push(verification_path);
        task.outputs.summary = if verification_passed {
            "Verification passed.".to_string()
        } else {
            "Verification failed or no verification command was available.".to_string()
        };
    }
}

fn append_event(store: &StateStore, event_sink: &Option<EventSink>, event: Event) -> Result<()> {
    store.append_event(&event)?;
    if let Some(event_sink) = event_sink {
        event_sink(&event);
    }
    Ok(())
}

fn add_repair_task(
    tasks: &mut Vec<Task>,
    goal_id: &str,
    scope: &Scope,
    iteration: usize,
    verification_path: &std::path::Path,
) -> String {
    let task_id = repair_task_id(iteration);
    tasks.push(Task {
        id: task_id.clone(),
        goal_id: goal_id.to_string(),
        title: format!("Repair failed verification iteration {iteration}"),
        kind: TaskKind::Repair,
        status: TaskStatus::Pending,
        assigned_worker: Some("opencode".to_string()),
        attempt: iteration,
        scope: scope.clone(),
        inputs: TaskInputs {
            spec_path: None,
            plan_path: None,
            worker_packet_path: None,
        },
        outputs: TaskOutputs {
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            evidence: vec![verification_path.to_string_lossy().to_string()],
            summary: "Repair task created from failed verification.".to_string(),
        },
    });
    task_id
}

fn repair_task_id(iteration: usize) -> String {
    if iteration == 2 {
        "task_005".to_string()
    } else {
        format!("task_repair_{iteration:03}")
    }
}

fn review_task_id(iteration: usize) -> String {
    format!("task_review_{iteration:03}")
}

fn add_review_task(
    tasks: &mut Vec<Task>,
    goal_id: &str,
    scope: &Scope,
    iteration: usize,
    review_path: &std::path::Path,
    summary: &str,
) {
    tasks.push(Task {
        id: review_task_id(iteration),
        goal_id: goal_id.to_string(),
        title: format!("Review goal after iteration {iteration}"),
        kind: TaskKind::Review,
        status: TaskStatus::Complete,
        assigned_worker: None,
        attempt: iteration,
        scope: scope.clone(),
        inputs: TaskInputs::default(),
        outputs: TaskOutputs {
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            evidence: vec![review_path.to_string_lossy().to_string()],
            summary: summary.to_string(),
        },
    });
}

#[derive(Clone, Debug)]
struct GoalEvaluation {
    status: GoalStatus,
    should_continue: bool,
    summary: String,
}

fn evaluate_goal(
    verification_passed: bool,
    worker_status: &WorkerStatus,
    require_worker: bool,
    scope_check: &crate::tools::ScopeCheck,
    iteration: usize,
    max_iterations: usize,
) -> GoalEvaluation {
    if !scope_check.forbidden_touches.is_empty()
        || !scope_check.outside_allowed_paths.is_empty()
        || scope_check.max_files_exceeded
    {
        return GoalEvaluation {
            status: GoalStatus::Blocked,
            should_continue: false,
            summary: "Goal blocked by scope checks.".to_string(),
        };
    }
    if require_worker && *worker_status != WorkerStatus::Succeeded {
        return GoalEvaluation {
            status: GoalStatus::NeedsUser,
            should_continue: false,
            summary: format!(
                "Goal needs user input because worker status is {}.",
                worker_status.as_str()
            ),
        };
    }
    if verification_passed {
        let summary = if *worker_status == WorkerStatus::Succeeded {
            format!("Goal completed after {iteration} Gear iteration(s).")
        } else {
            format!(
                "Goal completed after {iteration} Gear iteration(s); verification passed while worker status was {}.",
                worker_status.as_str()
            )
        };
        return GoalEvaluation {
            status: GoalStatus::Complete,
            should_continue: false,
            summary,
        };
    }
    if iteration < max_iterations {
        GoalEvaluation {
            status: GoalStatus::Running,
            should_continue: true,
            summary: format!(
                "Goal still incomplete after iteration {iteration}; Gear will plan a repair iteration."
            ),
        }
    } else {
        GoalEvaluation {
            status: GoalStatus::Limited,
            should_continue: false,
            summary: format!(
                "Goal reached the iteration limit ({max_iterations}) before verification passed."
            ),
        }
    }
}

fn repair_request(
    original_request: &str,
    iteration: usize,
    verification_path: Option<&std::path::Path>,
) -> String {
    let verification_path = verification_path
        .map(|path| path.to_string_lossy().to_string())
        .unwrap_or_else(|| "missing verification artifact".to_string());
    format!(
        "Repair iteration {iteration} for Gear goal.\n\nOriginal request:\n{original_request}\n\nReview the failed verification artifact at `{verification_path}` and make the smallest focused repair. Do not expand scope."
    )
}

fn goal_review_artifact(
    iteration: usize,
    max_iterations: usize,
    evaluation: &GoalEvaluation,
    worker_result: &crate::workers::WorkerResult,
    scope_check: &crate::tools::ScopeCheck,
    verification_results: &[ShellCommandResult],
) -> String {
    let verification_summary = if verification_results.is_empty() {
        "No verification command ran.".to_string()
    } else if verification_results.iter().all(|result| result.success) {
        "All verification commands passed.".to_string()
    } else {
        "One or more verification commands failed.".to_string()
    };

    format!(
        r#"# Goal Review

Iteration: `{iteration}` / `{max_iterations}`

## Gear Decision

- status: `{}`
- should_continue: `{}`
- summary: {}

## Worker

- status: `{}`
- summary: {}

## Verification

{}

## Scope

- forbidden_touches: {}
- outside_allowed_paths: {}
- changed_file_count: {}
- max_files_exceeded: {}
"#,
        evaluation.status.as_str(),
        evaluation.should_continue,
        evaluation.summary,
        worker_result.status.as_str(),
        worker_result.summary,
        verification_summary,
        scope_check.forbidden_touches.len(),
        scope_check.outside_allowed_paths.len(),
        scope_check.changed_file_count,
        scope_check.max_files_exceeded,
    )
}

#[allow(dead_code)]
fn _keep_diff_snapshot_for_docs(_: &DiffSnapshot) {}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, Mutex};

    use anyhow::Result;

    use super::*;
    use crate::workers::{WorkerKind, WorkerStatus};

    #[test]
    fn run_creates_ledger_artifacts_and_verification() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{"scripts":{"build":"echo build-ok"}}"#,
        )?;
        let events = Arc::new(Mutex::new(Vec::new()));
        let event_sink = {
            let events = events.clone();
            Arc::new(move |event: &Event| {
                events
                    .lock()
                    .expect("events mutex poisoned")
                    .push(event.message.clone());
            }) as EventSink
        };

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: vec!["src".to_string(), "README.md".to_string()],
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: Some(event_sink),
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: Some(CoordinatorModel {
                provider_id: "openai".to_string(),
                model_id: "gpt-4.1".to_string(),
                name: "GPT-4.1".to_string(),
            }),
            coordinator_brief: Some("Prefer a compact local implementation.".to_string()),
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert!(outcome.final_report_path.exists());
        assert!(outcome.events_path.exists());
        assert!(outcome.artifacts_root.join("spec.md").exists());
        assert!(outcome.artifacts_root.join("plan.md").exists());
        let goal = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent")
                .join("goals")
                .join(format!("{}.json", outcome.goal_id)),
        )?;
        assert!(goal.contains("\"provider_id\": \"openai\""));
        assert!(goal.contains("Prefer a compact local implementation."));
        let packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent")
                .join("workers")
                .join("task_003")
                .join("packet.json"),
        )?;
        assert!(packet.contains("\"model_id\": \"gpt-4.1\""));
        assert!(packet.contains("Prefer a compact local implementation."));
        let final_report = fs::read_to_string(&outcome.final_report_path)?;
        assert!(final_report.contains("GPT-4.1 (openai/gpt-4.1)"));
        assert!(final_report.contains("Prefer a compact local implementation."));
        let verification = fs::read_to_string(outcome.artifacts_root.join("verification.md"))?;
        assert!(verification.contains("verify-ok"));
        let events = events.lock().expect("events mutex poisoned");
        assert!(events.iter().any(|event| event == "Spec artifact created"));
        assert!(events.iter().any(|event| event == "Verification passed"));
        assert!(
            events
                .iter()
                .any(|event| event.contains("Goal completed after 1 Gear iteration(s)"))
        );
        Ok(())
    }

    #[test]
    fn evaluation_mentions_non_required_worker_failure_when_verification_passes() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal(
            true,
            &WorkerStatus::Failed,
            false,
            &scope_check,
            1,
            DEFAULT_MAX_ITERATIONS,
        );

        assert_eq!(evaluation.status, GoalStatus::Complete);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("verification passed"));
        assert!(evaluation.summary.contains("worker status was failed"));
    }

    #[test]
    fn failed_verification_creates_repair_task() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["exit 7".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: None,
            coordinator_brief: None,
        })?;

        assert_eq!(outcome.status, GoalStatus::Limited);
        let tasks_path = temp_dir
            .path()
            .join(".gearbox-agent")
            .join("tasks")
            .join(format!("{}.tasks.json", outcome.goal_id));
        let tasks = fs::read_to_string(tasks_path)?;
        assert!(tasks.contains("task_005"));
        Ok(())
    }

    #[test]
    fn failed_verification_runs_repair_iteration_until_goal_passes() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;
        let marker_path = temp_dir.path().join("repair-marker");
        let verify_command = format!(
            "test -f {} && echo repaired || (touch {}; exit 7)",
            marker_path.display(),
            marker_path.display()
        );

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec![verify_command],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: None,
            coordinator_brief: None,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert!(
            outcome
                .artifacts_root
                .join("verification-iteration-2.md")
                .exists()
        );
        assert!(
            outcome
                .artifacts_root
                .join("goal-review-iteration-2.md")
                .exists()
        );
        Ok(())
    }

    #[test]
    fn cancelled_run_stops_before_artifacts() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let cancellation_token = CancellationToken::new();
        cancellation_token.cancel();

        let error = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo unreachable".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: Vec::new(),
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: Some(cancellation_token),
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: None,
            coordinator_brief: None,
        })
        .expect_err("run should be cancelled");

        assert!(
            error.to_string().contains("Gear run cancelled"),
            "{error:#}"
        );
        Ok(())
    }
}
