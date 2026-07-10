use std::{
    fs as std_fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, bail};
use serde_json::json;

use crate::languages::{LanguageDetection, detect_with_request};
use crate::product;
use crate::state::{
    Budget, ContinuationStatus, CoordinatorModel, Event, EventKind, Goal, GoalStatus, Scope,
    Session, StateStore, Task, TaskInputs, TaskKind, TaskOutputs, TaskStatus, WorkLineage,
    event, id_timestamp, timestamp,
};
use crate::task_manager::{
    CompletionNotifier, ManagedTaskStatus, NotificationResult, ParentSessionState,
    SharedTaskManager, TaskAttempt, TaskFailureKind, TaskManager, TaskManagerControl,
    TaskManagerTickLoop, TaskRecord,
};
use crate::tools::{
    CancellationToken, DiffSnapshot, ShellCommandResult, check_scope, git_snapshot,
    run_shell_command_with_env_and_cancellation,
};
use crate::workers::{
    CategoryResolution, CategoryResolutionResult, FallbackRoute, WorkerCategory, WorkerConfig,
    WorkerKind, WorkerOutcome, WorkerResult, WorkerStartRequest, WorkerStatus,
    category_resolution_for_route,
};

pub type EventSink = Arc<dyn Fn(&Event) + Send + Sync + 'static>;
pub type CoordinatorReviewHook = Arc<
    dyn Fn(CoordinatorReviewInput) -> Result<Option<CoordinatorReview>> + Send + Sync + 'static,
>;
pub const DEFAULT_MAX_ITERATIONS: usize = 5;
pub const DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK: usize = 2;
pub const DEFAULT_MAX_RUNTIME_MINUTES: usize = 60;

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
    pub max_provider_unknown_streak: usize,
    pub max_child_depth: usize,
    pub max_runtime_minutes: usize,
    pub coordinator_model: Option<CoordinatorModel>,
    pub coordinator_brief: Option<String>,
    pub coordinator_review_hook: Option<CoordinatorReviewHook>,
    pub task_manager_control: Option<TaskManagerControl>,
    pub task_manager: Option<SharedTaskManager>,
    /// Stable caller-owned identity for continuation persistence.
    pub session_id: Option<String>,
    pub continuation: bool,
}

#[derive(Clone, Debug)]
pub struct CoordinatorReviewInput {
    pub goal_id: String,
    pub task_id: String,
    pub iteration: usize,
    pub max_iterations: usize,
    pub request: String,
    pub worker_kind: String,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub route_reason: String,
    pub worker_attempt: usize,
    pub worker_attempt_count: usize,
    pub worker_failure_kind: Option<String>,
    pub worker_retry_reason: Option<String>,
    pub worker_fallback_summary: String,
    pub worker_status: String,
    pub worker_summary: String,
    pub worker_outcome_summary: String,
    pub worker_commands_run: Vec<String>,
    pub worker_known_failures: Vec<String>,
    pub worker_outcome_path: Option<String>,
    pub worker_transcript_head: Option<String>,
    pub worker_transcript_tail: Option<String>,
    pub category_resolution: CategoryResolution,
    pub category_resolution_result: CategoryResolutionResult,
    pub no_progress_signals: Vec<String>,
    pub budget_summary: String,
    pub verification_passed: bool,
    pub verification_summary: String,
    pub scope_summary: String,
    pub diff_summary: String,
}

#[derive(Clone, Debug)]
pub struct CoordinatorReview {
    pub goal_satisfied: Option<bool>,
    pub summary: String,
    pub repair_request: Option<String>,
    pub route_hint: Option<String>,
    pub stop_reason: Option<String>,
    pub raw_response: String,
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

struct CompletionNotificationFlushGuard<'a> {
    notifier: &'a CompletionNotifier,
    store: &'a StateStore,
    event_sink: &'a Option<EventSink>,
    session_id: String,
    goal_id: String,
}

impl Drop for CompletionNotificationFlushGuard<'_> {
    fn drop(&mut self) {
        let event_sink = self.event_sink;
        let result = self.notifier.flush_buffer(
            &self.session_id,
            ParentSessionState::Idle,
            &|task_id, run_epoch| {
                append_completion_notification(
                    self.store,
                    event_sink,
                    &self.session_id,
                    &self.goal_id,
                    task_id,
                    run_epoch,
                )
            },
            &|task_id, run_epoch| {
                record_completion_notification_failed_epoch(self.store, task_id, run_epoch)
            },
            &|task_id| {
                let path = self.store.worker_dir(task_id).join("task-record.json");
                if path.exists() {
                    let content = std_fs::read_to_string(&path)?;
                    Ok(Some(serde_json::from_str(&content)?))
                } else {
                    Ok(None)
                }
            },
        );
        if let Err(error) = result {
            eprintln!("failed to flush Gear completion notifications: {error:#}");
        }
    }
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
        let session_id = options
            .session_id
            .clone()
            .unwrap_or_else(|| format!("ses_{id_suffix}"));
        let goal_id = format!("goal_{id_suffix}");

        if options.continuation && store.continuation_is_stopped_for_session(&session_id)? {
            bail!(
                "Gear continuation is stopped; explicitly restart the continuation before running again"
            );
        }

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

        let mut goal_budget = Budget::default();
        goal_budget.max_provider_unknown_streak = options.max_provider_unknown_streak.max(1);
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
            budget: goal_budget,
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
        if options.continuation {
            store.write_continuation_state(&session_id, &goal_id, ContinuationStatus::Running)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    None,
                    EventKind::ContinuationStarted,
                    "Gear continuation started",
                    json!({ "status": "running" }),
                ),
            )?;
        }
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

        let mut tasks = initial_tasks(
            &goal_id,
            &scope,
            options.worker.selected_route(1).worker_kind,
        );
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

        let mut before_diff = git_snapshot(&workspace)?;
        let mut after_diff = before_diff.clone();
        let mut scope_check = check_scope(&after_diff, &scope);
        let mut worker_result = None;
        let mut verification_results = Vec::new();
        let mut last_verification_path = None;
        let mut final_evaluation = None;
        let mut last_coordinator_review: Option<CoordinatorReview> = None;
        let mut next_route_hint_override: Option<String> = None;
        let mut provider_unknown_streak = 0usize;
        let mut repeated_failure_streak = 0usize;
        let mut last_failure_kind: Option<TaskFailureKind> = None;
        let mut diff_history: Vec<DiffSnapshot> = Vec::new();
        let mut verification_history: Vec<Vec<ShellCommandResult>> = Vec::new();
        let mut repair_request_history: Vec<String> = Vec::new();
        let mut worker_output_history: Vec<String> = Vec::new();
        let run_started_at = Instant::now();
        let mut worker_call_count = 0usize;
        let mut premium_worker_call_count = 0usize;
        let mut attempt_count = 0usize;
        let budget_controller = BudgetController {
            max_iterations,
            max_files_changed: options.max_files_changed,
            max_worker_calls: goal.budget.max_worker_calls,
            max_premium_worker_calls: options.worker.premium_worker_budget,
            max_same_failure_retries: 2,
            max_provider_unknown_streak: goal.budget.max_provider_unknown_streak,
            max_child_depth: options.max_child_depth,
            max_runtime_minutes: options.max_runtime_minutes,
        };
        let completion_notifier = CompletionNotifier::new();
        let task_manager = options.task_manager.clone().unwrap_or_else(|| {
            options
                .task_manager_control
                .clone()
                .map(TaskManager::with_control)
                .unwrap_or_else(TaskManager::new)
                .into_shared()
        });
        let artifacts_root = store.artifact_dir(&goal_id);
        {
            let mut task_manager = task_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?;
            task_manager.set_artifacts_root(artifacts_root.clone());
            task_manager.recover_orphaned_records(&store)?;
            task_manager.apply_worker_config(&options.worker);
        }
        let task_manager_tick_loop =
            TaskManagerTickLoop::start(task_manager.clone(), Duration::from_millis(50));
        let _completion_notification_flush_guard = CompletionNotificationFlushGuard {
            notifier: &completion_notifier,
            store: &store,
            event_sink: &options.event_sink,
            session_id: session_id.clone(),
            goal_id: goal_id.clone(),
        };

        // Initialize or restore WorkLineage for this session.
        // Lineage tracks worker lifecycle and participates in completion gating.
        let mut lineage = store.read_lineage(&session_id)?.unwrap_or_else(|| {
            let mut l = WorkLineage::new(session_id.clone());
            // Count non-completed tasks as plan_remaining_items.
            l.plan_remaining_items = tasks
                .iter()
                .filter(|t| t.status != TaskStatus::Complete)
                .count();
            l
        });

        #[allow(clippy::explicit_counter_loop)]
        for iteration in 1..=max_iterations {
            check_run_cancelled(options.cancellation_token.as_ref())?;
            if options.continuation && store.continuation_is_stopped_for_session(&session_id)? {
                final_evaluation = Some(GoalEvaluation {
                    status: GoalStatus::NeedsUser,
                    should_continue: false,
                    summary: "Continuation was stopped by the user before the next worker turn."
                        .to_string(),
                    route_hint_override: None,
                });
                break;
            }
            let parent_task_id = goal.current_task_id.clone();
            let worker_route_hint = next_route_hint_override.as_deref().or_else(|| {
                last_coordinator_review
                    .as_ref()
                    .and_then(|review| review.route_hint.as_deref())
            });
            let selected_route = options.worker.selected_route_for_hint(1, worker_route_hint);
            let (category_resolution, category_resolution_result) = category_resolution_for_route(
                &options.worker,
                1,
                worker_route_hint,
                &selected_route,
            );
            let current_route_change_type = if worker_route_hint == Some("review") {
                RouteChangeType::ReviewTrigger
            } else if selected_route.route_reason.contains("fell back to") {
                RouteChangeType::Fallback
            } else {
                RouteChangeType::RouteChange
            };
            let worker_task_id = if iteration == 1 {
                "task_003".to_string()
            } else {
                let verification_path = last_verification_path
                    .as_deref()
                    .context("missing verification artifact for repair iteration")?;
                let repair_task_id = add_repair_task(
                    &mut tasks,
                    &goal_id,
                    &scope,
                    iteration,
                    verification_path,
                    parent_task_id.clone(),
                    selected_route.worker_kind,
                );
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
                            "route_hint": worker_route_hint,
                            "worker_kind": selected_route.worker_kind.as_str(),
                            "worker_model": selected_route.worker_model,
                            "worker_category": selected_route.category.as_str(),
                            "route_reason": &selected_route.route_reason,
                        }),
                    ),
                )?;
                repair_task_id
            };

            // Generate immutable ownership decision before any execution.
            let ownership = crate::state::ExecutionOwnership {
                delegated: selected_route.require_worker || options.worker.skip_worker,
                worker_kind: Some(selected_route.worker_kind.as_str().to_string()),
                route_reason: selected_route.route_reason.clone(),
                risk_profile: "unknown".to_string(),
                worker_task_id: Some(worker_task_id.clone()),
                decided_at: crate::state::timestamp(),
            };

            // Track this worker in the lineage and persist.
            lineage.worker_session_ids.push(worker_task_id.clone());
            lineage.active_task_ids.push(worker_task_id.clone());
            lineage.updated_at = timestamp();
            store.write_lineage(&lineage)?;

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
                        "route_hint": worker_route_hint,
                        "worker_kind": selected_route.worker_kind.as_str(),
                        "worker_model": selected_route.worker_model,
                        "worker_category": selected_route.category.as_str(),
                        "route_reason": &selected_route.route_reason,
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
                    last_coordinator_review.as_ref(),
                )
            };
            repair_request_history.push(worker_request.clone());
            let managed_worker_task_id = task_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
                .start(WorkerStartRequest {
                    store: &store,
                    workspace: &workspace,
                    task: &worker_task,
                    route_attempt: worker_task.attempt,
                    goal: &worker_request,
                    verification_commands: &detection.verification_commands,
                    config: &options.worker,
                    cancellation_token: options.cancellation_token.clone(),
                    coordinator_model: goal.coordinator_model.as_ref(),
                    coordinator_brief: goal.coordinator_brief.as_deref(),
                    route_hint: worker_route_hint,
                })
                .context("ownership: worker start failed, goal remains incomplete")?;
            if options
                .cancellation_token
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                task_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
                    .cancel_task(&managed_worker_task_id)?;
                check_run_cancelled(options.cancellation_token.as_ref())?;
            }
            let managed_worker_run = loop {
                check_run_cancelled(options.cancellation_token.as_ref())?;
                if let Some(run) = task_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
                    .try_wait_for(&managed_worker_task_id)?
                {
                    break run;
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            let worker_session_id = managed_worker_run.record.session_id.clone();
            let worker_task_record = managed_worker_run.record;
            let iteration_worker_outcome = managed_worker_run.outcome;
            let iteration_worker_result = managed_worker_run.result;
            let iteration_worker_result_for_risk = iteration_worker_result.clone();
            let _budget_check = budget_controller.apply_budget_for_route_change(
                &BudgetSnapshot {
                    worker_call_count,
                    premium_worker_call_count,
                    attempt_count,
                    runtime_elapsed_minutes: run_started_at.elapsed().as_secs() as usize / 60,
                    context_risk_signals: Vec::new(),
                },
                current_route_change_type.clone(),
            );
            worker_call_count += 1;
            attempt_count += worker_task_record.attempts.len();
            premium_worker_call_count += worker_task_record
                .attempts
                .iter()
                .filter(|attempt| {
                    WorkerKind::parse(&attempt.worker_kind)
                        .is_some_and(|worker_kind| worker_kind.is_premium())
                })
                .count();
            let runtime_elapsed_minutes = run_started_at.elapsed().as_secs() as usize / 60;

            update_worker_task(
                &mut tasks,
                &worker_task_id,
                &iteration_worker_result.status,
                &iteration_worker_result.summary,
            );
            append_worker_fallback_evidence(
                &mut tasks,
                &store,
                &worker_task_id,
                &worker_task_record,
            );
            store.write_tasks(&goal_id, &tasks)?;

            // Worker has completed (success, failure, or skip); remove from
            // active_task_ids so lineage no longer blocks completion on this worker.
            lineage.active_task_ids.retain(|id| id != &worker_task_id);
            lineage.updated_at = timestamp();
            store.write_lineage(&lineage)?;

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
                        "session_id": worker_session_id,
                        "route_hint": worker_route_hint,
                        "worker_kind": selected_route.worker_kind.as_str(),
                        "worker_model": selected_route.worker_model,
                        "worker_category": selected_route.category.as_str(),
                        "route_reason": &selected_route.route_reason,
                        "packet_path": iteration_worker_result.packet_path.to_string_lossy(),
                        "prompt_path": iteration_worker_result.prompt_path.to_string_lossy(),
                        "outcome_path": iteration_worker_result.outcome_path.to_string_lossy(),
                        "task_record_path": store.worker_dir(&worker_task_id).join("task-record.json").to_string_lossy(),
                        "managed_status": format!("{:?}", worker_task_record.status),
                        "failure_kind": worker_task_record.failure_kind.as_ref().map(|kind| format!("{kind:?}")),
                        "retry_reason": &worker_task_record.retry_reason,
                        "commands_run": &iteration_worker_outcome.commands_run,
                        "known_failures": &iteration_worker_outcome.known_failures,
                    }),
                ),
            )?;
            worker_result = Some(iteration_worker_result);
            worker_output_history.push(iteration_worker_outcome.summary.clone());
            if let Some(finished_at) = worker_task_record.finished_at.as_deref()
                && let Some(notification) = CompletionNotifier::build_notification(
                    &worker_task_record,
                    &worker_task_record.started_at,
                    finished_at,
                )
            {
                if let NotificationResult::Failed(error) = completion_notifier.try_notify(
                    notification,
                    ParentSessionState::Streaming,
                    &|task_id, run_epoch| {
                        append_completion_notification(
                            &store,
                            &options.event_sink,
                            &session_id,
                            &goal_id,
                            task_id,
                            run_epoch,
                        )
                    },
                    &|task_id, run_epoch| {
                        record_completion_notification_failed_epoch(&store, task_id, run_epoch)
                    },
                )? {
                    eprintln!(
                        "failed to buffer Gear completion notification for {worker_task_id}: {error}"
                    );
                }
            }

            if let Some(current_failure_kind) = worker_task_record.failure_kind.clone() {
                if last_failure_kind.as_ref() == Some(&current_failure_kind) {
                    repeated_failure_streak += 1;
                } else {
                    repeated_failure_streak = 1;
                }
                last_failure_kind = Some(current_failure_kind);
            } else {
                repeated_failure_streak = 0;
                last_failure_kind = None;
            }

            let budget_snapshot_for_review = BudgetSnapshot {
                worker_call_count,
                premium_worker_call_count,
                attempt_count,
                runtime_elapsed_minutes,
                context_risk_signals: Vec::new(),
            };

            after_diff = git_snapshot(&workspace)?;
            diff_history.push(after_diff.clone());
            scope_check = check_scope(&after_diff, &scope);
            let comment_violations = comment_check(&workspace, &after_diff.changed_files)?;
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

            let budget_summary_for_review = budget_summary(
                &budget_controller,
                &budget_snapshot_for_review,
                repeated_failure_streak,
                provider_unknown_streak,
                iteration,
                scope_check.changed_file_count,
            );

            verification_results = run_verification(
                &workspace,
                &detection.verification_commands,
                options.cancellation_token.as_ref(),
            )?;
            verification_history.push(verification_results.clone());
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
            let mut no_progress_signals = detect_stagnation(
                &diff_history,
                &verification_history,
                &repair_request_history,
                &worker_output_history,
            );
            if !comment_violations.is_empty() {
                no_progress_signals.push(format!(
                    "comment_check: organizational comments at {}",
                    comment_violations.join(", ")
                ));
            }
            let coordinator_review = run_coordinator_review(
                &store,
                &options.event_sink,
                &options.coordinator_review_hook,
                &session_id,
                &goal_id,
                iteration,
                max_iterations,
                &options.request,
                &worker_task_id,
                &worker_task_record,
                worker_result
                    .as_ref()
                    .context("missing worker result for coordinator review")?,
                &iteration_worker_outcome,
                &category_resolution,
                &category_resolution_result,
                &no_progress_signals,
                &budget_summary_for_review,
                verification_passed,
                &verification_results,
                &scope_check,
                &before_diff,
                &after_diff,
            )?;
            last_coordinator_review = coordinator_review.clone();
            let coordinator_review = coordinator_review.as_ref();
            let mut context_risk_signals = detect_context_risk_signals(collect_context_risk_texts(
                &iteration_worker_result_for_risk,
                &iteration_worker_outcome,
                &worker_task_record,
                coordinator_review,
            ));
            if !comment_violations.is_empty() {
                context_risk_signals.push(format!(
                    "comment_check: {} violation(s)",
                    comment_violations.len()
                ));
            }
            let budget_snapshot = BudgetSnapshot {
                context_risk_signals,
                ..budget_snapshot_for_review
            };
            let budget_summary = budget_summary(
                &budget_controller,
                &budget_snapshot,
                repeated_failure_streak,
                provider_unknown_streak,
                iteration,
                scope_check.changed_file_count,
            );
            provider_unknown_streak = update_provider_unknown_streak(
                provider_unknown_streak,
                verification_passed,
                coordinator_review,
            );
            let has_fallback = category_resolution_result.nearest_fallback().is_some();
            // Ownership decision was generated earlier in the iteration.
            // The immutable `ownership` is already in scope from line ~468.
            let evaluation = evaluate_goal_with_source(
                verification_passed,
                &worker_result
                    .as_ref()
                    .context("missing worker result for goal evaluation")?
                    .status,
                selected_route.category,
                selected_route.require_worker,
                worker_task_record.failure_kind.as_ref(),
                worker_task_record.retry_reason.as_deref(),
                &scope_check,
                coordinator_review,
                provider_unknown_streak,
                repeated_failure_streak,
                iteration,
                &budget_controller,
                &budget_snapshot,
                &no_progress_signals,
                has_fallback,
                Some(current_route_change_type),
                Some(&ownership),
                None,
            );
            next_route_hint_override = evaluation.route_hint_override.clone();
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
                    selected_route.category,
                    selected_route.worker_model,
                    &selected_route.route_reason,
                    &category_resolution,
                    &category_resolution_result,
                    &no_progress_signals,
                    worker_task_record.failure_kind.as_ref(),
                    worker_task_record.retry_reason.as_deref(),
                    &worker_fallback_summary(&worker_task_record),
                    &budget_summary,
                    &iteration_worker_outcome,
                    &scope_check,
                    &verification_results,
                    coordinator_review,
                ),
            )?;
            let review_gate = ReviewGate::from_inputs(
                verification_passed,
                &worker_result
                    .as_ref()
                    .context("missing worker result for review gate")?
                    .status,
                &scope_check,
                coordinator_review,
                &budget_snapshot.context_risk_signals,
                &worker_task_record.attempts,
            );
            review_gate
                .validate_independent_reviewers()
                .context("review gate validation failed")?;
            let repair_request_path = review_gate.failed_reason().map(|reason| {
                store.write_artifact(
                    &goal_id,
                    &format!("review-repair-request-iteration-{iteration}.md"),
                    &format!(
                        "# Review Gate Repair Request\n\nIteration: `{iteration}`\n\nThe required review dimensions failed:\n\n- {reason}\n\nRepair only the smallest changes needed to satisfy the failed dimensions, then rerun verification.\n"
                    ),
                )
            }).transpose()?;
            add_review_task(
                &mut tasks,
                &goal_id,
                &scope,
                iteration,
                &review_path,
                &evaluation.summary,
                Some(worker_task_id.clone()),
                repair_request_path.as_deref(),
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

            before_diff = after_diff.clone();
        }

        let final_evaluation = final_evaluation.context("Gear loop did not evaluate the goal")?;
        let worker_result = worker_result.context("Gear loop did not produce a worker result")?;
        let final_task_id = goal.current_task_id.clone();
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
        if options.continuation {
            let continuation_status = if store.continuation_is_stopped_for_session(&session_id)? {
                ContinuationStatus::Stopped
            } else {
                ContinuationStatus::Completed
            };
            store.write_continuation_state(&session_id, &goal_id, continuation_status.clone())?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    final_task_id.as_deref(),
                    match &continuation_status {
                        ContinuationStatus::Stopped => EventKind::ContinuationStopped,
                        ContinuationStatus::Completed => EventKind::ContinuationCompleted,
                        ContinuationStatus::Running => EventKind::ContinuationStarted,
                    },
                    "Gear continuation state updated",
                    json!({ "status": &continuation_status }),
                ),
            )?;
        }

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

        if let Some(error) = task_manager_tick_loop.last_error()? {
            bail!("{error}");
        }
        task_manager_tick_loop.stop()?;

        let status = goal.status;
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

fn initial_tasks(goal_id: &str, scope: &Scope, worker_kind: WorkerKind) -> Vec<Task> {
    [
        ("task_001", "Generate minimal spec", TaskKind::Spec, None),
        ("task_002", "Generate executable plan", TaskKind::Plan, None),
        (
            "task_003",
            "Dispatch bounded implementation packet",
            TaskKind::Edit,
            Some(worker_kind.as_str().to_string()),
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
        parent_task_id: None,
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

fn run_coordinator_review(
    store: &StateStore,
    event_sink: &Option<EventSink>,
    hook: &Option<CoordinatorReviewHook>,
    session_id: &str,
    goal_id: &str,
    iteration: usize,
    max_iterations: usize,
    request: &str,
    task_id: &str,
    worker_task_record: &TaskRecord,
    worker_result: &crate::workers::WorkerResult,
    worker_outcome: &WorkerOutcome,
    category_resolution: &CategoryResolution,
    category_resolution_result: &CategoryResolutionResult,
    no_progress_signals: &[String],
    budget_summary: &str,
    verification_passed: bool,
    verification_results: &[ShellCommandResult],
    scope_check: &crate::tools::ScopeCheck,
    before_diff: &DiffSnapshot,
    after_diff: &DiffSnapshot,
) -> Result<Option<CoordinatorReview>> {
    let Some(hook) = hook else {
        return Ok(None);
    };
    let (worker_transcript_head, worker_transcript_tail) =
        worker_transcript_head_tail(worker_result);

    let input = CoordinatorReviewInput {
        goal_id: goal_id.to_string(),
        task_id: task_id.to_string(),
        iteration,
        max_iterations,
        request: request.to_string(),
        worker_kind: worker_task_record.worker_kind.clone(),
        worker_model: worker_task_record.worker_model.clone(),
        worker_category: worker_task_record.worker_category.clone(),
        route_reason: worker_task_record.route_reason.clone(),
        worker_attempt: worker_task_record
            .attempts
            .last()
            .map(|attempt| attempt.attempt)
            .unwrap_or(1),
        worker_attempt_count: worker_task_record.attempts.len(),
        worker_failure_kind: worker_task_record
            .failure_kind
            .as_ref()
            .map(|kind| format!("{kind:?}")),
        worker_retry_reason: worker_task_record.retry_reason.clone(),
        worker_fallback_summary: worker_fallback_summary(worker_task_record),
        worker_status: worker_result.status.as_str().to_string(),
        worker_summary: worker_result.summary.clone(),
        worker_outcome_summary: worker_outcome.summary.clone(),
        worker_commands_run: worker_outcome.commands_run.clone(),
        worker_known_failures: worker_outcome.known_failures.clone(),
        worker_outcome_path: Some(worker_result.outcome_path.to_string_lossy().to_string()),
        worker_transcript_head,
        worker_transcript_tail,
        category_resolution: category_resolution.clone(),
        category_resolution_result: category_resolution_result.clone(),
        no_progress_signals: no_progress_signals.to_vec(),
        budget_summary: budget_summary.to_string(),
        verification_passed,
        verification_summary: verification_summary(verification_results),
        scope_summary: scope_summary(scope_check),
        diff_summary: diff_summary(before_diff, after_diff),
    };

    let review = match hook(input) {
        Ok(review) => review,
        Err(error) => {
            append_event(
                store,
                event_sink,
                event(
                    session_id,
                    Some(goal_id),
                    None,
                    EventKind::TaskStarted,
                    format!("Coordinator review failed: {error:#}"),
                    json!({ "iteration": iteration }),
                ),
            )?;
            return Ok(None);
        }
    };

    let Some(review) = review else {
        return Ok(None);
    };

    let review_path = store.write_artifact(
        goal_id,
        &format!("coordinator-review-iteration-{iteration}.md"),
        &coordinator_review_artifact(iteration, &review),
    )?;
    append_event(
        store,
        event_sink,
        event(
            session_id,
            Some(goal_id),
            None,
            EventKind::TaskStarted,
            "Coordinator review completed",
            json!({
                "iteration": iteration,
                "goal_satisfied": review.goal_satisfied,
                "route_hint": &review.route_hint,
                "stop_reason": &review.stop_reason,
                "review_path": review_path.to_string_lossy(),
            }),
        ),
    )?;

    Ok(Some(review))
}

fn verification_summary(results: &[ShellCommandResult]) -> String {
    if results.is_empty() {
        return "No verification command ran.".to_string();
    }

    results
        .iter()
        .map(|result| {
            format!(
                "- `{}`: {} ({:?})",
                result.command,
                if result.success { "passed" } else { "failed" },
                result.exit_code
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn scope_summary(scope_check: &crate::tools::ScopeCheck) -> String {
    format!(
        "forbidden_touches={}, outside_allowed_paths={}, changed_file_count={}, max_files_exceeded={}",
        scope_check.forbidden_touches.len(),
        scope_check.outside_allowed_paths.len(),
        scope_check.changed_file_count,
        scope_check.max_files_exceeded
    )
}

fn diff_summary(before_diff: &DiffSnapshot, after_diff: &DiffSnapshot) -> String {
    format!(
        "before_files={}, after_files={}, is_git_repo={}",
        before_diff.changed_files.len(),
        after_diff.changed_files.len(),
        after_diff.is_git_repo
    )
}

fn coordinator_review_artifact(iteration: usize, review: &CoordinatorReview) -> String {
    format!(
        r#"# Coordinator Review

Iteration: `{iteration}`

## Decision

- goal_satisfied: `{}`
- summary: {}
- route_hint: `{}`
- stop_reason: `{}`

## Repair Request

{}

## Raw Provider Review

{}
"#,
        review
            .goal_satisfied
            .map(|satisfied| if satisfied { "yes" } else { "no" })
            .unwrap_or("unknown"),
        review.summary,
        review.route_hint.as_deref().unwrap_or("none"),
        review.stop_reason.as_deref().unwrap_or("none"),
        review
            .repair_request
            .as_deref()
            .unwrap_or("No repair request supplied."),
        review.raw_response.trim(),
    )
}

fn worker_fallback_summary(task_record: &TaskRecord) -> String {
    if task_record.attempts.len() == 1
        && task_record.failure_kind.is_none()
        && task_record.retry_reason.is_none()
    {
        return "single-attempt run".to_string();
    }

    task_record
        .attempts
        .iter()
        .enumerate()
        .map(|(index, attempt)| {
            let provider = WorkerKind::parse(&attempt.worker_kind)
                .and_then(|worker_kind| worker_kind.provider_id_hint())
                .unwrap_or("none");
            let artifact_path = if index + 1 < task_record.attempts.len() {
                Some(format!(
                    "workers/{}/route-transform-{}-to-{}.md",
                    task_record.task_id,
                    attempt.attempt,
                    attempt.attempt + 1,
                ))
            } else if attempt.attempt == 1 {
                Some(format!(
                    "workers/{}/route-transform-1-stopped.md",
                    task_record.task_id
                ))
            } else if task_record.failure_kind.is_some()
                && task_record.retry_reason.is_some()
                && !matches!(task_record.status, ManagedTaskStatus::Completed)
            {
                Some(format!(
                    "workers/{}/route-transform-{}-stopped.md",
                    task_record.task_id, attempt.attempt
                ))
            } else {
                None
            };
            format!(
                "- attempt {}: {} provider={} [{}] model={} session={} failure={} retry={}{}",
                attempt.attempt,
                attempt.worker_kind,
                provider,
                attempt.worker_category,
                attempt.worker_model.as_deref().unwrap_or("none"),
                attempt.session_id.as_deref().unwrap_or("pending"),
                attempt
                    .failure_kind
                    .as_ref()
                    .map(|kind| format!("{kind:?}"))
                    .unwrap_or_else(|| "none".to_string()),
                attempt.retry_reason.as_deref().unwrap_or("none"),
                artifact_path
                    .as_deref()
                    .map(|path| format!(" artifact={path}"))
                    .unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn append_worker_fallback_evidence(
    tasks: &mut [Task],
    store: &StateStore,
    task_id: &str,
    task_record: &TaskRecord,
) {
    let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) else {
        return;
    };

    for path in worker_fallback_artifact_paths(store, task_record) {
        let path = path.to_string_lossy().to_string();
        if !task
            .outputs
            .evidence
            .iter()
            .any(|existing| existing == &path)
        {
            task.outputs.evidence.push(path);
        }
    }
}

fn worker_fallback_artifact_paths(store: &StateStore, task_record: &TaskRecord) -> Vec<PathBuf> {
    let worker_dir = store.worker_dir(&task_record.task_id);
    let attempts_len = task_record.attempts.len();
    task_record
        .attempts
        .iter()
        .enumerate()
        .filter_map(|(index, attempt)| {
            if index + 1 < attempts_len {
                Some(worker_dir.join(format!(
                    "route-transform-{}-to-{}.md",
                    attempt.attempt,
                    attempt.attempt + 1,
                )))
            } else if !matches!(task_record.status, ManagedTaskStatus::Completed)
                && (task_record.failure_kind.is_some() || task_record.retry_reason.is_some())
            {
                Some(worker_dir.join(format!("route-transform-{}-stopped.md", attempt.attempt)))
            } else {
                None
            }
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

fn append_completion_notification(
    store: &StateStore,
    event_sink: &Option<EventSink>,
    session_id: &str,
    goal_id: &str,
    task_id: &str,
    run_epoch: u64,
) -> Result<()> {
    let task_record_path = store.worker_dir(task_id).join("task-record.json");
    let task_record_contents = std_fs::read_to_string(&task_record_path)
        .with_context(|| format!("failed to read {}", task_record_path.display()))?;
    let mut task_record: TaskRecord = serde_json::from_str(&task_record_contents)
        .context("failed to deserialize Gear task record")?;
    if task_record.notified_epoch >= 0 && (task_record.notified_epoch as u64) >= run_epoch {
        return Ok(());
    }

    let started_at = task_record.started_at.clone();
    let finished_at = task_record
        .finished_at
        .clone()
        .unwrap_or_else(|| started_at.clone());
    let Some(notification) =
        CompletionNotifier::build_notification(&task_record, &started_at, &finished_at)
    else {
        return Ok(());
    };

    let task_name = notification.task_name.clone();
    let status_label = format!("{:?}", &notification.status);
    let summary = notification.summary.clone();
    let failure_kind = notification
        .failure_kind
        .as_ref()
        .map(|kind| format!("{kind:?}"));
    let result_path = notification
        .result_path
        .as_ref()
        .map(|path| path.to_string_lossy().to_string());
    let outcome_path = notification
        .outcome_path
        .as_ref()
        .map(|path| path.to_string_lossy().to_string());

    task_record.notified_epoch = run_epoch as i64;
    let task_record_json = serde_json::to_string_pretty(&task_record)
        .context("failed to serialize Gear task record")?;
    store.write_worker_file(
        task_id,
        "task-record.json",
        &format!("{task_record_json}\n"),
    )?;

    append_event(
        store,
        event_sink,
        event(
            session_id,
            Some(goal_id),
            Some(task_id),
            EventKind::CompletionNotified,
            format!(
                "{} {} in {}ms: {} ({})",
                task_name.as_str(),
                status_label.as_str(),
                notification.duration_ms,
                notification.summary_head,
                notification.continuation_hint,
            ),
            json!({
                "task_name": task_name,
                "status": status_label,
                "duration_ms": notification.duration_ms,
                "summary": summary,
                "summary_head": notification.summary_head,
                "continuation_hint": notification.continuation_hint,
                "failure_kind": failure_kind,
                "result_path": result_path,
                "outcome_path": outcome_path,
                "task_record_path": task_record_path.to_string_lossy(),
                "run_epoch": notification.run_epoch,
                "notified_epoch": run_epoch,
            }),
        ),
    )?;
    Ok(())
}

fn record_completion_notification_failed_epoch(
    store: &StateStore,
    task_id: &str,
    run_epoch: u64,
) -> Result<()> {
    let task_record_path = store.worker_dir(task_id).join("task-record.json");
    let task_record_contents = std_fs::read_to_string(&task_record_path)
        .with_context(|| format!("failed to read {}", task_record_path.display()))?;
    let mut task_record: TaskRecord = serde_json::from_str(&task_record_contents)
        .context("failed to deserialize Gear task record")?;
    if task_record
        .notification_failed_epoch
        .is_some_and(|failed_epoch| failed_epoch >= run_epoch)
    {
        return Ok(());
    }

    task_record.notification_failed_epoch = Some(run_epoch);
    let task_record_json = serde_json::to_string_pretty(&task_record)
        .context("failed to serialize Gear task record")?;
    store.write_worker_file(
        task_id,
        "task-record.json",
        &format!("{task_record_json}\n"),
    )?;
    Ok(())
}

fn add_repair_task(
    tasks: &mut Vec<Task>,
    goal_id: &str,
    scope: &Scope,
    iteration: usize,
    verification_path: &std::path::Path,
    parent_task_id: Option<String>,
    worker_kind: WorkerKind,
) -> String {
    let task_id = repair_task_id(iteration);
    tasks.push(Task {
        id: task_id.clone(),
        goal_id: goal_id.to_string(),
        parent_task_id,
        title: format!("Repair failed verification iteration {iteration}"),
        kind: TaskKind::Repair,
        status: TaskStatus::Pending,
        assigned_worker: Some(worker_kind.as_str().to_string()),
        attempt: 1,
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
    parent_task_id: Option<String>,
    repair_request_path: Option<&std::path::Path>,
) {
    let mut evidence = vec![review_path.to_string_lossy().to_string()];
    if let Some(repair_request_path) = repair_request_path {
        evidence.push(repair_request_path.to_string_lossy().to_string());
    }
    tasks.push(Task {
        id: review_task_id(iteration),
        goal_id: goal_id.to_string(),
        parent_task_id,
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
            evidence,
            summary: summary.to_string(),
        },
    });
}

#[derive(Clone, Debug)]
struct GoalEvaluation {
    status: GoalStatus,
    should_continue: bool,
    summary: String,
    route_hint_override: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReviewDimension {
    GoalVerification,
    CodeQuality,
    Security,
    QaExecution,
}

impl ReviewDimension {
    fn label(self) -> &'static str {
        match self {
            Self::GoalVerification => "goal_verification",
            Self::CodeQuality => "code_quality",
            Self::Security => "security",
            Self::QaExecution => "qa_execution",
        }
    }
}

/// Evidence linking a review dimension to a specific reviewer execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewerEvidence {
    /// Unique execution ID of the reviewer worker.
    pub execution_id: String,
    /// The route/category of the reviewer (e.g. "deep", "explore", "comment_checker").
    pub route: String,
    /// Path to the reviewer's output artifact.
    pub artifact_path: Option<String>,
    /// Verdict from this reviewer.
    pub verdict: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewDimensionResult {
    pub dimension: ReviewDimension,
    pub passed: bool,
    pub evidence: String,
    /// Optional structured evidence binding to a specific reviewer execution.
    pub reviewer_evidence: Option<ReviewerEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewGate {
    pub require_all_pass: bool,
    pub results: Vec<ReviewDimensionResult>,
}

impl ReviewGate {
    /// Verify that each dimension has unique reviewer_evidence (or none).
    /// Returns Err if the same execution_id is used for multiple dimensions.
    pub fn validate_independent_reviewers(&self) -> Result<()> {
        let mut seen: Vec<&str> = Vec::new();
        for result in &self.results {
            if let Some(ref evidence) = result.reviewer_evidence {
                if seen.contains(&evidence.execution_id.as_str()) {
                    bail!(
                        "duplicate reviewer execution_id '{}' for dimension {}; each dimension must have independent evidence",
                        evidence.execution_id,
                        result.dimension.label(),
                    );
                }
                seen.push(&evidence.execution_id);
            }
        }
        Ok(())
    }

    fn from_inputs(
        verification_passed: bool,
        worker_status: &WorkerStatus,
        scope_check: &crate::tools::ScopeCheck,
        coordinator_review: Option<&CoordinatorReview>,
        context_risk_signals: &[String],
        task_attempts: &[TaskAttempt],
    ) -> Self {
        let review_required = true;
        let goal_satisfied = coordinator_review
            .and_then(|review| review.goal_satisfied)
            .unwrap_or(coordinator_review.is_none());
        let scope_clean = scope_check.forbidden_touches.is_empty()
            && scope_check.outside_allowed_paths.is_empty()
            && !scope_check.max_files_exceeded;
        let comment_check_clean = !context_risk_signals
            .iter()
            .any(|signal| signal.starts_with("comment_check:"));
        // Use real attempt data when available; fall back to synthetic with proper labels.
        let goal_verification_evidence =
            reviewer_evidence_from_attempt(ReviewDimension::GoalVerification, task_attempts)
                .unwrap_or_else(|| ReviewerEvidence {
                    execution_id: "coordinator".to_string(),
                    route: "coordinator".to_string(),
                    artifact_path: None,
                    verdict: if verification_passed && goal_satisfied {
                        "pass".to_string()
                    } else {
                        "fail".to_string()
                    },
                });
        let code_quality_evidence =
            reviewer_evidence_from_attempt(ReviewDimension::CodeQuality, task_attempts)
                .unwrap_or_else(|| ReviewerEvidence {
                    execution_id: "scope-check".to_string(),
                    route: "scope-check".to_string(),
                    artifact_path: None,
                    verdict: if scope_clean && comment_check_clean {
                        "pass".to_string()
                    } else {
                        "fail".to_string()
                    },
                });
        let security_evidence =
            reviewer_evidence_from_attempt(ReviewDimension::Security, task_attempts)
                .unwrap_or_else(|| ReviewerEvidence {
                    execution_id: "security-check".to_string(),
                    route: "security-check".to_string(),
                    artifact_path: None,
                    verdict: if scope_check.forbidden_touches.is_empty() {
                        "pass".to_string()
                    } else {
                        "fail".to_string()
                    },
                });
        let qa_execution_evidence =
            reviewer_evidence_from_attempt(ReviewDimension::QaExecution, task_attempts)
                .unwrap_or_else(|| ReviewerEvidence {
                    execution_id: "verification".to_string(),
                    route: "verification".to_string(),
                    artifact_path: None,
                    verdict: if verification_passed {
                        "pass".to_string()
                    } else {
                        "fail".to_string()
                    },
                });
        Self {
            require_all_pass: review_required,
            results: vec![
                ReviewDimensionResult {
                    dimension: ReviewDimension::GoalVerification,
                    passed: verification_passed && goal_satisfied,
                    evidence: if verification_passed && goal_satisfied {
                        "verification passed and coordinator accepted the goal".to_string()
                    } else {
                        "verification or coordinator goal acceptance failed".to_string()
                    },
                    reviewer_evidence: Some(goal_verification_evidence),
                },
                ReviewDimensionResult {
                    dimension: ReviewDimension::CodeQuality,
                    passed: scope_clean && comment_check_clean,
                    evidence: if scope_clean && comment_check_clean {
                        format!(
                            "worker status `{}` accepted and scope checks are clean",
                            worker_status.as_str()
                        )
                    } else {
                        if !comment_check_clean {
                            "comment checker reported organizational comments".to_string()
                        } else {
                            "scope checks are not clean".to_string()
                        }
                    },
                    reviewer_evidence: Some(code_quality_evidence),
                },
                ReviewDimensionResult {
                    dimension: ReviewDimension::Security,
                    passed: scope_check.forbidden_touches.is_empty(),
                    evidence: if scope_check.forbidden_touches.is_empty() {
                        "no forbidden paths were touched".to_string()
                    } else {
                        format!(
                            "forbidden paths touched: {}",
                            scope_check.forbidden_touches.join(", ")
                        )
                    },
                    reviewer_evidence: Some(security_evidence),
                },
                ReviewDimensionResult {
                    dimension: ReviewDimension::QaExecution,
                    passed: verification_passed,
                    evidence: if verification_passed {
                        "verification commands passed".to_string()
                    } else {
                        "one or more verification commands failed".to_string()
                    },
                    reviewer_evidence: Some(qa_execution_evidence),
                },
            ],
        }
    }

    fn failed_reason(&self) -> Option<String> {
        if !self.require_all_pass {
            return None;
        }
        let failures = self
            .results
            .iter()
            .filter(|result| !result.passed)
            .map(|result| format!("{}: {}", result.dimension.label(), result.evidence))
            .collect::<Vec<_>>();
        (!failures.is_empty()).then(|| failures.join("; "))
    }

    /// Returns `Some(reason)` when ALL reviewer evidence entries are synthetic
    /// (i.e., no real worker attempt produced the evidence). Completion must
    /// require at least one real reviewer artifact.
    fn synthetic_evidence_only_reason(&self) -> Option<String> {
        if self.results.is_empty() {
            return None;
        }
        let all_synthetic = self.results.iter().all(|result| {
            result.reviewer_evidence.as_ref().map_or(true, |evidence| {
                SYNTHETIC_EXECUTION_IDS.contains(&evidence.execution_id.as_str())
            })
        });
        if all_synthetic {
            Some(
                "All reviewer evidence is synthetic (no real worker attempt data). \
                 Completion requires at least one real reviewer artifact."
                    .to_string(),
            )
        } else {
            None
        }
    }

    fn summary(&self) -> String {
        self.results
            .iter()
            .map(|result| {
                format!(
                    "{}={}: {}",
                    result.dimension.label(),
                    if result.passed { "pass" } else { "fail" },
                    result.evidence
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// Known synthetic execution IDs used by `from_inputs()` when no real
/// task attempt data is available.
const SYNTHETIC_EXECUTION_IDS: &[&str] = &[
    "coordinator",
    "scope-check",
    "security-check",
    "verification",
];

/// Build `ReviewerEvidence` from a real task attempt if available.
/// Returns None when no attempt data is available (use synthetic only as fallback).
fn reviewer_evidence_from_attempt(
    dimension: ReviewDimension,
    attempts: &[TaskAttempt],
) -> Option<ReviewerEvidence> {
    let last_attempt = attempts.last()?;
    let execution_id = last_attempt.session_id.clone()?;
    let artifact_path = last_attempt
        .result_path
        .clone()
        .or_else(|| last_attempt.outcome_path.clone());
    Some(ReviewerEvidence {
        execution_id,
        route: dimension.label().to_string(),
        artifact_path: artifact_path.map(|p| p.to_string_lossy().to_string()),
        verdict: "pass".to_string(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RouteChangeType {
    RouteChange,
    Fallback,
    ReviewTrigger,
}

impl RouteChangeType {
    fn label(&self) -> &'static str {
        match self {
            RouteChangeType::RouteChange => "route change",
            RouteChangeType::Fallback => "fallback",
            RouteChangeType::ReviewTrigger => "review",
        }
    }
}

#[derive(Clone, Debug)]
struct GoalDecisionPolicy<'a> {
    verification_passed: bool,
    worker_status: &'a WorkerStatus,
    worker_category: WorkerCategory,
    require_worker: bool,
    worker_failure_kind: Option<&'a TaskFailureKind>,
    worker_retry_reason: Option<&'a str>,
    scope_check: &'a crate::tools::ScopeCheck,
    coordinator_review: Option<&'a CoordinatorReview>,
    provider_unknown_streak: usize,
    repeated_failure_streak: usize,
    iteration: usize,
    budget: &'a BudgetController,
    budget_snapshot: &'a BudgetSnapshot,
    no_progress_signals: &'a [String],
    nearest_fallback_available: bool,
    trigger_source: Option<RouteChangeType>,
    review_gate: &'a ReviewGate,
    /// Ownership decision: whether the work was delegated to a worker.
    /// `None` means no decision was made — completion must be denied.
    ownership: Option<&'a crate::state::ExecutionOwnership>,
    /// WorkLineage for lineage-based completion gating.
    /// `None` means no lineage record exists.
    lineage: Option<&'a WorkLineage>,
}

#[derive(Clone, Debug)]
struct BudgetController {
    max_iterations: usize,
    max_files_changed: usize,
    max_worker_calls: usize,
    max_premium_worker_calls: usize,
    max_same_failure_retries: usize,
    max_provider_unknown_streak: usize,
    max_child_depth: usize,
    max_runtime_minutes: usize,
}

impl Default for BudgetController {
    fn default() -> Self {
        Self {
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_files_changed: usize::MAX,
            max_worker_calls: DEFAULT_MAX_ITERATIONS,
            max_premium_worker_calls: usize::MAX,
            max_same_failure_retries: 2,
            max_provider_unknown_streak: 2,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
        }
    }
}

impl BudgetController {
    fn apply_budget_for_route_change(
        &self,
        snapshot: &BudgetSnapshot,
        route_change_type: RouteChangeType,
    ) -> Result<(), String> {
        if snapshot.worker_call_count >= self.max_worker_calls {
            return Err(format!(
                "worker_calls={}/{} ({})",
                snapshot.worker_call_count,
                budget_limit_label(self.max_worker_calls),
                route_change_type.label()
            ));
        }
        if snapshot.premium_worker_call_count >= self.max_premium_worker_calls {
            return Err(format!(
                "premium_worker_calls={}/{} ({})",
                snapshot.premium_worker_call_count,
                budget_limit_label(self.max_premium_worker_calls),
                route_change_type.label()
            ));
        }
        if snapshot.runtime_elapsed_minutes >= self.max_runtime_minutes {
            return Err(format!(
                "runtime_minutes={}/{} ({})",
                snapshot.runtime_elapsed_minutes,
                budget_limit_label(self.max_runtime_minutes),
                route_change_type.label()
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct BudgetSnapshot {
    worker_call_count: usize,
    premium_worker_call_count: usize,
    attempt_count: usize,
    runtime_elapsed_minutes: usize,
    context_risk_signals: Vec<String>,
}

fn budget_limit_label(limit: usize) -> String {
    if limit == usize::MAX {
        "unbounded".to_string()
    } else {
        limit.to_string()
    }
}

fn within_scope_limits(changed_files: usize, max_files_changed: usize) -> bool {
    changed_files <= max_files_changed
}

fn budget_summary(
    budget: &BudgetController,
    budget_snapshot: &BudgetSnapshot,
    repeated_failure_streak: usize,
    provider_unknown_streak: usize,
    iteration: usize,
    changed_file_count: usize,
) -> String {
    let same_failure_retries = repeated_failure_streak.saturating_sub(1);
    let child_depth = iteration.saturating_sub(1);
    let context_risk_summary = if budget_snapshot.context_risk_signals.is_empty() {
        "none".to_string()
    } else {
        budget_snapshot.context_risk_signals.join("; ")
    };
    format!(
        "iterations={}/{}; changed_files={}/{}; worker_calls={}/{}; premium_worker_calls={}/{}; attempts={}; same_failure_retries={}/{}; provider_unknown_streak={}/{}; child_depth={}/{}; runtime_minutes={}/{}; context_risks={}",
        iteration,
        budget.max_iterations,
        changed_file_count,
        budget.max_files_changed,
        budget_snapshot.worker_call_count,
        budget_limit_label(budget.max_worker_calls),
        budget_snapshot.premium_worker_call_count,
        budget_limit_label(budget.max_premium_worker_calls),
        budget_snapshot.attempt_count,
        same_failure_retries,
        budget.max_same_failure_retries,
        provider_unknown_streak,
        budget.max_provider_unknown_streak,
        child_depth,
        budget_limit_label(budget.max_child_depth),
        budget_snapshot.runtime_elapsed_minutes,
        budget_limit_label(budget.max_runtime_minutes),
        context_risk_summary
    )
}

impl<'a> GoalDecisionPolicy<'a> {
    fn budget_guard_reason(&self) -> Option<String> {
        let same_failure_retries = self.repeated_failure_streak.saturating_sub(1);
        let child_depth = self.iteration.saturating_sub(1);
        let mut reasons = Vec::new();
        let trigger_label = self
            .trigger_source
            .as_ref()
            .map(|t| format!(" ({})", t.label()))
            .unwrap_or_default();

        if self.budget_snapshot.worker_call_count >= self.budget.max_worker_calls {
            reasons.push(format!(
                "worker_calls={}/{}{}",
                self.budget_snapshot.worker_call_count,
                budget_limit_label(self.budget.max_worker_calls),
                trigger_label,
            ));
        }

        if self.budget_snapshot.premium_worker_call_count >= self.budget.max_premium_worker_calls {
            reasons.push(format!(
                "premium_worker_calls={}/{}{}",
                self.budget_snapshot.premium_worker_call_count,
                budget_limit_label(self.budget.max_premium_worker_calls),
                trigger_label,
            ));
        }

        if same_failure_retries >= self.budget.max_same_failure_retries {
            reasons.push(format!(
                "same_failure_retries={}/{}",
                same_failure_retries,
                budget_limit_label(self.budget.max_same_failure_retries)
            ));
        }

        if child_depth > self.budget.max_child_depth {
            reasons.push(format!(
                "child_depth={}/{}",
                child_depth,
                budget_limit_label(self.budget.max_child_depth)
            ));
        }

        if self.budget_snapshot.runtime_elapsed_minutes >= self.budget.max_runtime_minutes {
            reasons.push(format!(
                "runtime_minutes={}/{}",
                self.budget_snapshot.runtime_elapsed_minutes,
                budget_limit_label(self.budget.max_runtime_minutes)
            ));
        }

        if reasons.is_empty() {
            None
        } else {
            Some(reasons.join("; "))
        }
    }

    fn context_guard_reason(&self) -> Option<String> {
        if self.budget_snapshot.context_risk_signals.is_empty() {
            None
        } else {
            Some(self.budget_snapshot.context_risk_signals.join("; "))
        }
    }

    fn continuation_guard(&self, reason: &str) -> Option<GoalEvaluation> {
        if let Some(context_reason) = self.context_guard_reason() {
            return Some(GoalEvaluation {
                status: GoalStatus::NeedsUser,
                should_continue: false,
                summary: format!(
                    "Goal paused before {reason} because the worker context became unreliable: {context_reason}."
                ),
                route_hint_override: None,
            });
        }

        if let Some(budget_reason) = self.budget_guard_reason() {
            return Some(GoalEvaluation {
                status: GoalStatus::Limited,
                should_continue: false,
                summary: format!("Goal reached a budget limit before {reason}: {budget_reason}."),
                route_hint_override: None,
            });
        }

        None
    }

    fn ownership_gate_reason(&self) -> Option<String> {
        // Tasks that modify code require an ownership delegation decision.
        let requires_ownership = self.require_worker
            || matches!(
                self.worker_category,
                WorkerCategory::Quick
                    | WorkerCategory::Deep
                    | WorkerCategory::Repair
                    | WorkerCategory::Visual
            );
        if requires_ownership {
            match self.ownership {
                Some(ownership) if ownership.delegated => None,
                Some(ownership) => Some(format!(
                    "Execution ownership decision exists but delegation was denied: {}. Reason: {}",
                    ownership.worker_kind.as_deref().unwrap_or("none"),
                    ownership.route_reason,
                )),
                None => Some(
                    "No execution ownership decision recorded. All code-modifying work must be delegated to a worker before completion."
                        .to_string(),
                ),
            }
        } else {
            None
        }
    }

    fn lineage_gate_reason(&self) -> Option<String> {
        // If there is a WorkLineage with active descendant tasks, deny completion.
        self.lineage.and_then(|lineage| {
            if !lineage.active_task_ids.is_empty() {
                Some(format!(
                    "WorkLineage has {} active task(s) still running: {:?}",
                    lineage.active_task_ids.len(),
                    lineage.active_task_ids
                ))
            } else if lineage.plan_remaining_items > 0 {
                Some(format!(
                    "WorkLineage has {} remaining plan items",
                    lineage.plan_remaining_items
                ))
            } else {
                None
            }
        })
    }

    fn evaluate(&self) -> GoalEvaluation {
        let independent_review_requested = self.coordinator_review.is_some_and(|review| {
            review.route_hint.as_deref().and_then(WorkerCategory::parse)
                == Some(WorkerCategory::Review)
        });
        if !within_scope_limits(
            self.scope_check.changed_file_count,
            self.budget.max_files_changed,
        ) {
            return GoalEvaluation {
                status: GoalStatus::Limited,
                should_continue: false,
                summary: "Goal reached the file change limit.".to_string(),
                route_hint_override: None,
            };
        }
        if self.scope_check.max_files_exceeded
            || !self.scope_check.forbidden_touches.is_empty()
            || !self.scope_check.outside_allowed_paths.is_empty()
        {
            return GoalEvaluation {
                status: GoalStatus::Blocked,
                should_continue: false,
                summary: "Goal blocked by scope checks.".to_string(),
                route_hint_override: None,
            };
        }
        if !self.verification_passed {
            if let Some(evaluation) = self.continuation_guard("repair/replan") {
                return evaluation;
            }
            if self.repeated_failure_streak >= 2 {
                let upgrade_hint = match self.worker_category {
                    WorkerCategory::Quick | WorkerCategory::Repair | WorkerCategory::Explore => {
                        Some("deep")
                    }
                    WorkerCategory::Deep => Some("review"),
                    WorkerCategory::Review => None,
                    _ => Some("deep"),
                };
                if let Some(route_hint_override) = upgrade_hint
                    && self.iteration < self.budget.max_iterations
                {
                    return GoalEvaluation {
                        status: GoalStatus::Running,
                        should_continue: true,
                        summary: format!(
                            "Gear detected repeated `{}` failures and will escalate to `{route_hint_override}`.",
                            self.worker_failure_kind
                                .map(|kind| format!("{kind:?}"))
                                .unwrap_or_else(|| "worker".to_string())
                        ),
                        route_hint_override: Some(route_hint_override.to_string()),
                    };
                }
            }
            if let Some(worker_failure_kind) = self.worker_failure_kind {
                match worker_failure_kind {
                    TaskFailureKind::NoFallbackRoute
                    | TaskFailureKind::RepeatedFailureLimit
                    | TaskFailureKind::PremiumBudgetExceeded => {
                        return GoalEvaluation {
                            status: GoalStatus::Limited,
                            should_continue: false,
                            summary: format!(
                                "Goal reached a worker fallback limit: {}.",
                                self.worker_retry_reason
                                    .unwrap_or(match worker_failure_kind {
                                        TaskFailureKind::NoFallbackRoute => {
                                            "no different fallback route is available"
                                        }
                                        TaskFailureKind::RepeatedFailureLimit => {
                                            "same worker failure repeated too many times"
                                        }
                                        TaskFailureKind::PremiumBudgetExceeded => {
                                            "premium worker budget was exhausted"
                                        }
                                        _ => "worker fallback stopped",
                                    })
                            ),
                            route_hint_override: None,
                        };
                    }
                    TaskFailureKind::WorkerUnavailable | TaskFailureKind::WorkerStartFailed
                        if self.require_worker =>
                    {
                        return GoalEvaluation {
                            status: GoalStatus::NeedsUser,
                            should_continue: false,
                            summary: format!(
                                "Goal needs user input because the required worker is unavailable: {}.",
                                self.worker_retry_reason
                                    .unwrap_or("configure a worker command or route")
                            ),
                            route_hint_override: None,
                        };
                    }
                    _ => {}
                }
            }
            if !self.no_progress_signals.is_empty() && self.iteration < self.budget.max_iterations {
                return GoalEvaluation {
                    status: GoalStatus::Running,
                    should_continue: true,
                    summary: format!(
                        "Goal detected stagnation signals and will replan: {}",
                        self.no_progress_signals.join("; ")
                    ),
                    route_hint_override: Some("deep".to_string()),
                };
            }
        }
        if self.require_worker && *self.worker_status != WorkerStatus::Succeeded {
            return GoalEvaluation {
                status: GoalStatus::NeedsUser,
                should_continue: false,
                summary: format!(
                    "Goal needs user input because worker status is {}.",
                    self.worker_status.as_str()
                ),
                route_hint_override: None,
            };
        }
        if let Some(stop_reason) = self
            .coordinator_review
            .and_then(|review| review.stop_reason.as_deref())
            .and_then(normalized_stop_reason)
        {
            match stop_reason {
                "needs_user" => {
                    return GoalEvaluation {
                        status: GoalStatus::NeedsUser,
                        should_continue: false,
                        summary: "Coordinator review requested user input before continuing."
                            .to_string(),
                        route_hint_override: None,
                    };
                }
                "blocked" => {
                    return GoalEvaluation {
                        status: GoalStatus::Blocked,
                        should_continue: false,
                        summary: "Coordinator review marked the goal blocked.".to_string(),
                        route_hint_override: None,
                    };
                }
                "limited" => {
                    return GoalEvaluation {
                        status: GoalStatus::Limited,
                        should_continue: false,
                        summary: "Coordinator review stopped the loop at the current budget limit."
                            .to_string(),
                        route_hint_override: None,
                    };
                }
                "complete" => {}
                _ => {}
            }
        }
        if self.verification_passed {
            if independent_review_requested && self.worker_category != WorkerCategory::Review {
                if self.iteration < self.budget.max_iterations {
                    return GoalEvaluation {
                        status: GoalStatus::Running,
                        should_continue: true,
                        summary: format!(
                            "Coordinator review requested an independent review worker after iteration {}.",
                            self.iteration
                        ),
                        route_hint_override: Some("review".to_string()),
                    };
                }

                return GoalEvaluation {
                    status: GoalStatus::Limited,
                    should_continue: false,
                    summary: format!(
                        "Goal reached the iteration limit ({}) before the requested independent review could complete.",
                        self.budget.max_iterations
                    ),
                    route_hint_override: None,
                };
            }
            if self
                .coordinator_review
                .is_some_and(|review| review.goal_satisfied.is_none())
            {
                if self.provider_unknown_streak >= self.budget.max_provider_unknown_streak {
                    if self.worker_category != WorkerCategory::Review
                        && self.iteration < self.budget.max_iterations
                    {
                        if let Some(evaluation) = self.continuation_guard("independent review") {
                            return evaluation;
                        }
                        return GoalEvaluation {
                            status: GoalStatus::Running,
                            should_continue: true,
                            summary: format!(
                                "Coordinator review stayed inconclusive for {} iterations (limit {}); Gear will escalate to an independent review worker.",
                                self.provider_unknown_streak,
                                self.budget.max_provider_unknown_streak
                            ),
                            route_hint_override: Some("review".to_string()),
                        };
                    }

                    return GoalEvaluation {
                        status: GoalStatus::NeedsUser,
                        should_continue: false,
                        summary: format!(
                            "Coordinator review remained inconclusive after repeated passes (limit {}); user input is required.",
                            self.budget.max_provider_unknown_streak
                        ),
                        route_hint_override: None,
                    };
                }

                if self.iteration < self.budget.max_iterations {
                    if let Some(evaluation) = self.continuation_guard("completion review") {
                        return evaluation;
                    }
                    return GoalEvaluation {
                        status: GoalStatus::Running,
                        should_continue: true,
                        summary: format!(
                            "Coordinator review remained inconclusive after iteration {}; Gear will continue before declaring completion.",
                            self.iteration
                        ),
                        route_hint_override: None,
                    };
                }

                return GoalEvaluation {
                    status: GoalStatus::NeedsUser,
                    should_continue: false,
                    summary: format!(
                        "Goal reached the iteration limit ({}) while coordinator review remained inconclusive.",
                        self.budget.max_iterations
                    ),
                    route_hint_override: None,
                };
            }
            if self
                .coordinator_review
                .is_some_and(|review| review.goal_satisfied == Some(false))
            {
                if self.iteration < self.budget.max_iterations {
                    if let Some(evaluation) = self.continuation_guard("repair planning") {
                        return evaluation;
                    }
                    return GoalEvaluation {
                        status: GoalStatus::Running,
                        should_continue: true,
                        summary: format!(
                            "Coordinator review found remaining work after iteration {}; Gear will plan a repair iteration.",
                            self.iteration
                        ),
                        route_hint_override: None,
                    };
                }

                return GoalEvaluation {
                    status: GoalStatus::Limited,
                    should_continue: false,
                    summary: format!(
                        "Goal reached the iteration limit ({}) after coordinator review found remaining work.",
                        self.budget.max_iterations
                    ),
                    route_hint_override: None,
                };
            }

            if let Some(context_reason) = self.context_guard_reason() {
                return GoalEvaluation {
                    status: GoalStatus::NeedsUser,
                    should_continue: false,
                    summary: format!(
                        "Goal paused before completion because the worker context became unreliable: {context_reason}."
                    ),
                    route_hint_override: None,
                };
            }

            if let Some(reason) = self.review_gate.failed_reason() {
                if self.iteration < self.budget.max_iterations {
                    return GoalEvaluation {
                        status: GoalStatus::Running,
                        should_continue: true,
                        summary: format!(
                            "Review gate failed after iteration {}; repair is required: {reason}.",
                            self.iteration
                        ),
                        route_hint_override: Some("review".to_string()),
                    };
                }
                return GoalEvaluation {
                    status: GoalStatus::Limited,
                    should_continue: false,
                    summary: format!("Review gate failed at the iteration limit: {reason}."),
                    route_hint_override: None,
                };
            }

            // Ownership gate: all code-modifying work must be delegated to a worker.
            // Gear itself must not directly write, edit, or create implementation files.
            if let Some(ownership_reason) = self.ownership_gate_reason() {
                if self.iteration < self.budget.max_iterations {
                    return GoalEvaluation {
                        status: GoalStatus::Running,
                        should_continue: true,
                        summary: format!("Ownership gate requires repair: {ownership_reason}"),
                        route_hint_override: Some("deep".to_string()),
                    };
                }
                return GoalEvaluation {
                    status: GoalStatus::NeedsUser,
                    should_continue: false,
                    summary: format!(
                        "Ownership gate blocked at the iteration limit: {ownership_reason}"
                    ),
                    route_hint_override: None,
                };
            }

            // Lineage gate: work cannot complete while there are active descendant
            // tasks still running or remaining plan items that need execution.
            if let Some(lineage_reason) = self.lineage_gate_reason() {
                if self.iteration < self.budget.max_iterations {
                    return GoalEvaluation {
                        status: GoalStatus::Running,
                        should_continue: true,
                        summary: format!("Lineage gate requires repair: {lineage_reason}"),
                        route_hint_override: Some("deep".to_string()),
                    };
                }
                return GoalEvaluation {
                    status: GoalStatus::NeedsUser,
                    should_continue: false,
                    summary: format!(
                        "Lineage gate blocked at the iteration limit: {lineage_reason}"
                    ),
                    route_hint_override: None,
                };
            }

            // Synthetic evidence gate: when the coordinator explicitly requests
            // an independent review (ROUTE_HINT=review) but no real reviewer
            // worker has run (all evidence is still synthetic), deny completion.
            if independent_review_requested
                && let Some(synthetic_reason) = self.review_gate.synthetic_evidence_only_reason()
            {
                if self.iteration < self.budget.max_iterations {
                    return GoalEvaluation {
                        status: GoalStatus::Running,
                        should_continue: true,
                        summary: format!("Synthetic evidence gate: {synthetic_reason}"),
                        route_hint_override: Some("review".to_string()),
                    };
                }
                return GoalEvaluation {
                    status: GoalStatus::NeedsUser,
                    should_continue: false,
                    summary: format!(
                        "Synthetic evidence gate blocked at the iteration limit: {synthetic_reason}"
                    ),
                    route_hint_override: None,
                };
            }

            let summary = if *self.worker_status == WorkerStatus::Succeeded {
                format!(
                    "Goal completed after {} Gear iteration(s). Review gate: {}.",
                    self.iteration,
                    self.review_gate.summary()
                )
            } else {
                format!(
                    "Goal completed after {} Gear iteration(s); verification passed while worker status was {}. Review gate: {}.",
                    self.iteration,
                    self.worker_status.as_str(),
                    self.review_gate.summary()
                )
            };
            return GoalEvaluation {
                status: GoalStatus::Complete,
                should_continue: false,
                summary,
                route_hint_override: None,
            };
        }
        if !self.verification_passed
            && !self.nearest_fallback_available
            && self.no_progress_signals.is_empty()
            && self.iteration > 1
        {
            return GoalEvaluation {
                status: GoalStatus::Limited,
                should_continue: false,
                summary:
                    "Goal reached the last feasible worker route with no alternative fallback."
                        .to_string(),
                route_hint_override: None,
            };
        }
        if self.iteration < self.budget.max_iterations {
            if let Some(evaluation) = self.continuation_guard("another repair iteration") {
                return evaluation;
            }
            GoalEvaluation {
                status: GoalStatus::Running,
                should_continue: true,
                summary: format!(
                    "Goal still incomplete after iteration {}; Gear will plan a repair iteration.",
                    self.iteration
                ),
                route_hint_override: None,
            }
        } else {
            GoalEvaluation {
                status: GoalStatus::Limited,
                should_continue: false,
                summary: format!(
                    "Goal reached the iteration limit ({}) before verification passed.",
                    self.budget.max_iterations
                ),
                route_hint_override: None,
            }
        }
    }
}

#[cfg(test)]
fn parse_coordinator_review(raw: &str) -> (CoordinatorReview, Vec<String>) {
    let mut review = CoordinatorReview {
        goal_satisfied: None,
        summary: raw.trim().to_string(),
        repair_request: None,
        route_hint: None,
        stop_reason: None,
        raw_response: raw.to_string(),
    };
    let mut warnings = Vec::new();

    for line in raw.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "goal_satisfied" => {
                let parsed = match value.to_ascii_lowercase().as_str() {
                    "yes" | "true" | "1" => Some(true),
                    "no" | "false" | "0" => Some(false),
                    _ => None,
                };
                if let Some(parsed) = parsed {
                    review.goal_satisfied = Some(parsed);
                } else if !value.is_empty() {
                    warnings.push(format!("Unrecognized GOAL_SATISFIED value: {value}"));
                }
            }
            "summary" => review.summary = value.to_string(),
            "repair_request" => review.repair_request = Some(value.to_string()),
            "route_hint" => review.route_hint = Some(value.to_string()),
            "stop_reason" => review.stop_reason = Some(value.to_string()),
            _ => {}
        }
    }

    if review.summary.is_empty() {
        review.summary = raw.to_string();
    }

    (review, warnings)
}

fn normalize_repair(text: &str) -> String {
    text.to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn detect_stagnation(
    diff_history: &[DiffSnapshot],
    verification_history: &[Vec<ShellCommandResult>],
    repair_requests: &[String],
    worker_outputs: &[String],
) -> Vec<String> {
    let mut signals = Vec::new();

    if diff_history.len() >= 2
        && let Some(first) = diff_history.first()
        && diff_history.iter().all(|snapshot| {
            snapshot.is_git_repo == first.is_git_repo
                && snapshot.status == first.status
                && snapshot.diff_hash == first.diff_hash
        })
    {
        signals.push(format!(
            "No file changes detected for {} consecutive iterations.",
            diff_history.len()
        ));
    }

    if verification_history.len() >= 2
        && let Some(first) = verification_history.first()
        && verification_history.iter().all(|results| results == first)
    {
        signals.push(format!(
            "Identical verification failures repeated for {} iterations.",
            verification_history.len()
        ));
    }

    if repair_requests.len() >= 2
        && let Some(first) = repair_requests.first()
        && repair_requests
            .iter()
            .all(|request| normalize_repair(request) == normalize_repair(first))
    {
        signals.push(format!(
            "Repair request `{first}` repeated for {} iterations.",
            repair_requests.len()
        ));
    }

    if worker_outputs.len() >= 2
        && let Some(first) = worker_outputs.first()
        && worker_outputs
            .iter()
            .all(|output| normalize_repair(output) == normalize_repair(first))
    {
        signals.push(format!(
            "Worker output repeated for {} iterations.",
            worker_outputs.len()
        ));
    }

    signals
}

fn collect_context_risk_texts(
    worker_result: &WorkerResult,
    worker_outcome: &WorkerOutcome,
    worker_task_record: &TaskRecord,
    coordinator_review: Option<&CoordinatorReview>,
) -> Vec<String> {
    let mut texts = vec![
        worker_result.summary.clone(),
        worker_outcome.summary.clone(),
        worker_task_record.summary.clone(),
    ];

    if let Some(error) = worker_task_record.error.as_deref() {
        texts.push(error.to_string());
    }
    if let Some(retry_reason) = worker_task_record.retry_reason.as_deref() {
        texts.push(retry_reason.to_string());
    }

    for attempt in &worker_task_record.attempts {
        texts.push(attempt.summary.clone());
        if let Some(error) = attempt.error.as_deref() {
            texts.push(error.to_string());
        }
        if let Some(retry_reason) = attempt.retry_reason.as_deref() {
            texts.push(retry_reason.to_string());
        }
        texts.push(attempt.route_reason.clone());
    }

    texts.extend(worker_outcome.changed_files.iter().cloned());
    texts.extend(worker_outcome.commands_run.iter().cloned());
    texts.extend(worker_outcome.known_failures.iter().cloned());

    if let Some(review) = coordinator_review {
        texts.push(review.summary.clone());
        texts.push(review.raw_response.clone());
    }

    for path in [
        worker_result.stdout_path.as_deref(),
        worker_result.stderr_path.as_deref(),
        worker_result.last_message_path.as_deref(),
        worker_outcome.raw_output_path.as_deref(),
        Some(worker_result.result_path.as_path()),
        Some(worker_result.outcome_path.as_path()),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(content) = read_optional_context_text(path) {
            texts.push(content);
        }
    }

    for artifact_name in ["transcript.jsonl", "tool-events.jsonl", "partial-output.md"] {
        if let Some(path) = worker_artifact_path(worker_result, artifact_name)
            && let Some(content) = read_optional_context_text_if_exists(&path)
        {
            texts.push(content);
        }
    }

    let event_names = worker_stream_event_names(worker_result, "tool-events.jsonl");
    if !event_names.is_empty() {
        texts.push(format!(
            "tool-events event sequence: {}",
            event_names.join(" -> ")
        ));
    }

    texts.extend(worker_artifact_truncation_signals(worker_result));

    texts
}

fn read_optional_context_text(path: &std::path::Path) -> Option<String> {
    match std_fs::read_to_string(path) {
        Ok(text) => Some(text),
        Err(error) => {
            eprintln!(
                "failed to read context risk artifact {}: {error}",
                path.display()
            );
            None
        }
    }
}

fn read_optional_context_text_if_exists(path: &std::path::Path) -> Option<String> {
    std_fs::read_to_string(path).ok()
}

fn worker_artifact_path(worker_result: &WorkerResult, file_name: &str) -> Option<PathBuf> {
    worker_result
        .result_path
        .parent()
        .or_else(|| worker_result.outcome_path.parent())
        .map(|artifact_dir| artifact_dir.join(file_name))
}

fn comment_check(workspace: &std::path::Path, changed_files: &[String]) -> Result<Vec<String>> {
    if std::env::var("GEARBOX_GEAR_COMMENT_CHECK").ok().as_deref() != Some("1") {
        return Ok(Vec::new());
    }

    let mut violations = Vec::new();
    for relative_path in changed_files {
        let path = workspace.join(relative_path);
        let Ok(contents) = std_fs::read_to_string(&path) else {
            continue;
        };
        for (line_number, line) in contents.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("///") || trimmed.starts_with("//!") {
                continue;
            }
            let is_organizational_comment = trimmed.starts_with("//")
                && ["assigns ", "this function ", "first, ", "step ", "now we "]
                    .iter()
                    .any(|prefix| {
                        trimmed[2..]
                            .trim_start()
                            .to_ascii_lowercase()
                            .starts_with(prefix)
                    });
            if is_organizational_comment {
                violations.push(format!("{relative_path}:{}", line_number + 1));
            }
        }
    }
    Ok(violations)
}

fn worker_text_head_tail(text: &str, line_limit: usize) -> (String, String) {
    let lines = text.lines().collect::<Vec<_>>();
    if lines.is_empty() {
        return (String::new(), String::new());
    }

    let head_end = lines.len().min(line_limit);
    let tail_start = lines.len().saturating_sub(line_limit);
    let head = lines[..head_end].join("\n");
    let tail = lines[tail_start..].join("\n");
    (head, tail)
}

fn worker_transcript_head_tail(worker_result: &WorkerResult) -> (Option<String>, Option<String>) {
    let Some(transcript_path) = worker_artifact_path(worker_result, "transcript.jsonl") else {
        return (None, None);
    };
    let Some(transcript) = read_optional_context_text_if_exists(&transcript_path) else {
        return (None, None);
    };

    let (head, tail) = worker_text_head_tail(&transcript, 16);
    (Some(head), Some(tail))
}

fn worker_artifact_truncation_signals(worker_result: &WorkerResult) -> Vec<String> {
    let mut signals = Vec::new();

    let transcript_events = worker_stream_event_names(worker_result, "transcript.jsonl");
    if !transcript_events.is_empty() {
        if transcript_events.last().map(String::as_str) != Some("turn_finished") {
            signals.push("transcript missing turn_finished event".to_string());
        }
        if transcript_events
            .iter()
            .any(|event_name| event_name == "tool_call_started")
            && !transcript_events
                .iter()
                .any(|event_name| event_name == "tool_call_finished")
        {
            signals.push("transcript missing tool_call_finished event".to_string());
        }
    }

    let tool_event_names = worker_stream_event_names(worker_result, "tool-events.jsonl");
    if !tool_event_names.is_empty() {
        if tool_event_names.last().map(String::as_str) != Some("tool_call_finished")
            && tool_event_names
                .iter()
                .any(|event_name| event_name == "tool_call_started")
        {
            signals.push("tool-events missing tool_call_finished event".to_string());
        }
    }

    if let Some(partial_output_path) = worker_artifact_path(worker_result, "partial-output.md")
        && let Some(partial_output) = read_optional_context_text_if_exists(&partial_output_path)
        && !partial_output.trim().is_empty()
    {
        signals.push("partial output artifact recorded".to_string());
    }

    signals
}

fn worker_stream_event_names(worker_result: &WorkerResult, file_name: &str) -> Vec<String> {
    let Some(artifact_path) = worker_artifact_path(worker_result, file_name) else {
        return Vec::new();
    };
    let Some(artifact) = read_optional_context_text_if_exists(&artifact_path) else {
        return Vec::new();
    };

    let mut event_names = Vec::new();
    for line in artifact
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(value) => match worker_event_name(&value) {
                Some(event_name) => event_names.push(event_name),
                None => event_names.push("malformed_event_line".to_string()),
            },
            Err(_) => event_names.push("malformed_event_line".to_string()),
        }
    }

    event_names
}

fn worker_event_name(value: &serde_json::Value) -> Option<String> {
    if let Some(event_name) = value.get("event").and_then(serde_json::Value::as_str) {
        return Some(event_name.to_string());
    }

    if let serde_json::Value::Object(object) = value
        && object.len() == 1
    {
        return object.keys().next().cloned();
    }

    if let serde_json::Value::String(event_name) = value {
        return Some(event_name.clone());
    }

    None
}

fn detect_context_risk_signals<I, S>(texts: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    const PATTERNS: &[(&str, &str)] = &[
        ("token limit", "token limit reported"),
        ("max tokens", "max tokens reported"),
        ("context compaction", "context compaction reported"),
        ("context window", "context window reported"),
        ("prompt too long", "prompt length reported as too long"),
        ("message too long", "message length reported as too long"),
        ("truncated", "output truncation reported"),
        (
            "missing turn_finished",
            "worker transcript ended before turn_finished",
        ),
        (
            "missing tool_call_finished",
            "tool event stream ended before tool_call_finished",
        ),
        (
            "malformed event line",
            "worker stream contained malformed event lines",
        ),
        (
            "partial output artifact",
            "partial output artifact recorded",
        ),
        ("insufficient context", "insufficient context reported"),
        ("session state", "session state reported as unreliable"),
        ("agent info", "agent information reported as unreliable"),
        ("context unreliable", "context reported as unreliable"),
    ];

    let normalized_texts: Vec<String> = texts
        .into_iter()
        .map(|text| text.as_ref().to_ascii_lowercase())
        .collect();

    PATTERNS
        .iter()
        .filter_map(|(needle, label)| {
            if normalized_texts.iter().any(|text| text.contains(needle)) {
                Some((*label).to_string())
            } else {
                None
            }
        })
        .collect()
}



fn evaluate_goal_with_source(
    verification_passed: bool,
    worker_status: &WorkerStatus,
    worker_category: WorkerCategory,
    require_worker: bool,
    worker_failure_kind: Option<&TaskFailureKind>,
    worker_retry_reason: Option<&str>,
    scope_check: &crate::tools::ScopeCheck,
    coordinator_review: Option<&CoordinatorReview>,
    provider_unknown_streak: usize,
    repeated_failure_streak: usize,
    iteration: usize,
    budget: &BudgetController,
    budget_snapshot: &BudgetSnapshot,
    no_progress_signals: &[String],
    nearest_fallback_available: bool,
    trigger_source: Option<RouteChangeType>,
    ownership: Option<&crate::state::ExecutionOwnership>,
    lineage: Option<&WorkLineage>,
) -> GoalEvaluation {
    let review_gate = ReviewGate::from_inputs(
        verification_passed,
        worker_status,
        scope_check,
        coordinator_review,
        &budget_snapshot.context_risk_signals,
        &[],
    );
    GoalDecisionPolicy {
        verification_passed,
        worker_status,
        worker_category,
        require_worker,
        worker_failure_kind,
        worker_retry_reason,
        scope_check,
        coordinator_review,
        provider_unknown_streak,
        repeated_failure_streak,
        iteration,
        budget,
        budget_snapshot,
        no_progress_signals,
        nearest_fallback_available,
        trigger_source,
        ownership,
        review_gate: &review_gate,
        lineage,
    }
    .evaluate()
}

fn normalized_stop_reason(value: &str) -> Option<&'static str> {
    let value = value.trim().to_ascii_lowercase();
    match value.as_str() {
        "complete" => Some("complete"),
        "limited" => Some("limited"),
        "blocked" => Some("blocked"),
        "needs_user" | "needs-user" | "user" => Some("needs_user"),
        _ => None,
    }
}

fn update_provider_unknown_streak(
    current: usize,
    verification_passed: bool,
    coordinator_review: Option<&CoordinatorReview>,
) -> usize {
    let has_concrete_stop_reason = coordinator_review
        .and_then(|review| review.stop_reason.as_deref())
        .and_then(normalized_stop_reason)
        .is_some();
    let goal_verified = verification_passed
        && coordinator_review.is_some_and(|review| review.goal_satisfied == Some(true));

    if goal_verified || has_concrete_stop_reason {
        0
    } else if verification_passed
        && coordinator_review.is_some_and(|review| {
            review.goal_satisfied.is_none()
                && review
                    .stop_reason
                    .as_deref()
                    .and_then(normalized_stop_reason)
                    .is_none()
        })
    {
        current + 1
    } else {
        current
    }
}

fn repair_request(
    original_request: &str,
    iteration: usize,
    verification_path: Option<&std::path::Path>,
    coordinator_review: Option<&CoordinatorReview>,
) -> String {
    let verification_path = verification_path
        .map(|path| path.to_string_lossy().to_string())
        .unwrap_or_else(|| "missing verification artifact".to_string());
    let coordinator_guidance = coordinator_review
        .and_then(|review| review.repair_request.as_deref())
        .unwrap_or("Use the verification artifact and goal review to choose the smallest repair.");
    let requested_category = coordinator_review
        .and_then(|review| review.route_hint.as_deref())
        .and_then(WorkerCategory::parse);
    if requested_category == Some(WorkerCategory::Review) {
        return format!(
            "Independent review iteration {iteration} for Gear goal.\n\nOriginal request:\n{original_request}\n\nInspect the current workspace, the verification artifact at `{verification_path}`, and the prior worker evidence. Do not expand scope or make speculative edits. Decide whether the goal is actually complete, and if not, identify the smallest missing fix or risk.\n\nCoordinator review guidance:\n{coordinator_guidance}"
        );
    }
    format!(
        "Repair iteration {iteration} for Gear goal.\n\nOriginal request:\n{original_request}\n\nReview the failed verification artifact at `{verification_path}` and make the smallest focused repair. Do not expand scope.\n\nCoordinator repair guidance:\n{coordinator_guidance}"
    )
}

fn goal_review_artifact(
    iteration: usize,
    max_iterations: usize,
    evaluation: &GoalEvaluation,
    worker_result: &crate::workers::WorkerResult,
    worker_category: WorkerCategory,
    worker_model: Option<&str>,
    route_reason: &str,
    category_resolution: &CategoryResolution,
    category_resolution_result: &CategoryResolutionResult,
    no_progress_signals: &[String],
    worker_failure_kind: Option<&TaskFailureKind>,
    worker_retry_reason: Option<&str>,
    worker_fallback_summary: &str,
    budget_summary: &str,
    worker_outcome: &WorkerOutcome,
    scope_check: &crate::tools::ScopeCheck,
    verification_results: &[ShellCommandResult],
    coordinator_review: Option<&CoordinatorReview>,
) -> String {
    let verification_summary = if verification_results.is_empty() {
        "No verification command ran.".to_string()
    } else if verification_results.iter().all(|result| result.success) {
        "All verification commands passed.".to_string()
    } else {
        "One or more verification commands failed.".to_string()
    };

    let coordinator_summary = coordinator_review
        .map(|review| {
            format!(
                "- goal_satisfied: `{}`\n- route_hint: `{}`\n- stop_reason: `{}`\n- summary: {}",
                review
                    .goal_satisfied
                    .map(|satisfied| if satisfied { "yes" } else { "no" })
                    .unwrap_or("unknown"),
                review.route_hint.as_deref().unwrap_or("none"),
                review.stop_reason.as_deref().unwrap_or("none"),
                review.summary
            )
        })
        .unwrap_or_else(|| "No provider-backed coordinator review ran.".to_string());
    let worker_transcript_summary = worker_transcript_summary(worker_result);
    let review_gate = ReviewGate::from_inputs(
        verification_results.iter().all(|result| result.success),
        &worker_result.status,
        scope_check,
        coordinator_review,
        no_progress_signals,
        &[],
    );
    let review_gate_dimensions = review_gate
        .results
        .iter()
        .map(|result| {
            format!(
                "- {}: `{}` — {}",
                result.dimension.label(),
                if result.passed { "pass" } else { "fail" },
                result.evidence
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"# Goal Review

Iteration: `{iteration}` / `{max_iterations}`

## Gear Decision

- status: `{}`
- should_continue: `{}`
- summary: {}

## Worker

- status: `{}`
- category: `{}`
- model: `{}`
- route_reason: {}
- route_resolution:
{}
- failure_kind: `{}`
- retry_reason: {}
- summary: {}
- outcome: {}
- commands_run: {}
- known_failures: {}
- outcome_path: `{}`

## Worker Transcript

{}

## Fallback History

{}

## Budget

{}

## No Progress

{}

## Verification

{}

## Coordinator Review

{}

## Review Gate

- require_all_pass: `{}`
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
        worker_category.as_str(),
        worker_model.unwrap_or("none"),
        route_reason,
        indent_block(
            &category_resolution_summary(category_resolution, category_resolution_result),
            2,
        ),
        worker_failure_kind
            .map(|failure_kind| format!("{failure_kind:?}"))
            .unwrap_or_else(|| "none".to_string()),
        worker_retry_reason.unwrap_or("none"),
        worker_result.summary,
        worker_outcome.summary,
        if worker_outcome.commands_run.is_empty() {
            "none".to_string()
        } else {
            worker_outcome.commands_run.join(", ")
        },
        if worker_outcome.known_failures.is_empty() {
            "none".to_string()
        } else {
            worker_outcome.known_failures.join("; ")
        },
        worker_result.outcome_path.to_string_lossy(),
        worker_transcript_summary,
        worker_fallback_summary,
        budget_summary,
        if no_progress_signals.is_empty() {
            "none".to_string()
        } else {
            no_progress_signals.join("; ")
        },
        verification_summary,
        coordinator_summary,
        review_gate.require_all_pass,
        review_gate_dimensions,
        scope_check.forbidden_touches.len(),
        scope_check.outside_allowed_paths.len(),
        scope_check.changed_file_count,
        scope_check.max_files_exceeded,
    )
}

fn category_resolution_summary(
    resolution: &CategoryResolution,
    result: &CategoryResolutionResult,
) -> String {
    let prompt_append = resolution
        .prompt_append
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("none");
    let available_categories = if resolution.available_categories.is_empty() {
        "none".to_string()
    } else {
        resolution.available_categories.join(", ")
    };
    let nearest_fallback = resolution
        .nearest_fallback
        .as_ref()
        .map(format_fallback_route)
        .unwrap_or_else(|| "none".to_string());
    let fallback_chain = if resolution.fallback_chain.is_empty() {
        "none".to_string()
    } else {
        resolution
            .fallback_chain
            .iter()
            .map(format_fallback_route)
            .collect::<Vec<_>>()
            .join(" -> ")
    };

    format!(
        r#"- prompt_append: {}
- available_categories: {}
- nearest_fallback: {}
- fallback_chain: {}
- tools:
{}
- result:
{}"#,
        prompt_append,
        available_categories,
        nearest_fallback,
        fallback_chain,
        indent_block(&resolution.tools.to_markdown(), 2),
        indent_block(&category_resolution_result_summary(result), 2),
    )
}

fn category_resolution_result_summary(result: &CategoryResolutionResult) -> String {
    match result {
        CategoryResolutionResult::Resolved {
            requested_category,
            available_categories,
            attempted_provider_model,
            nearest_fallback,
        } => format!(
            "- type: `resolved`\n- requested_category: `{}`\n- available_categories: {}\n- attempted_provider_model: {}\n- nearest_fallback: {}",
            requested_category,
            format_string_list(available_categories),
            attempted_provider_model.as_deref().unwrap_or("none"),
            format_optional_fallback_route(nearest_fallback),
        ),
        CategoryResolutionResult::Disabled {
            requested_category,
            available_categories,
            attempted_provider_model,
            nearest_fallback,
        } => format!(
            "- type: `disabled`\n- requested_category: `{}`\n- available_categories: {}\n- attempted_provider_model: {}\n- nearest_fallback: {}",
            requested_category,
            format_string_list(available_categories),
            attempted_provider_model.as_deref().unwrap_or("none"),
            format_optional_fallback_route(nearest_fallback),
        ),
        CategoryResolutionResult::NotFound {
            requested_category,
            available_categories,
            attempted_provider_model,
            nearest_fallback,
        } => format!(
            "- type: `not_found`\n- requested_category: `{}`\n- available_categories: {}\n- attempted_provider_model: {}\n- nearest_fallback: {}",
            requested_category,
            format_string_list(available_categories),
            attempted_provider_model.as_deref().unwrap_or("none"),
            format_optional_fallback_route(nearest_fallback),
        ),
        CategoryResolutionResult::ModelUnavailable {
            requested_category,
            available_categories,
            attempted_provider_model,
            nearest_fallback,
        } => format!(
            "- type: `model_unavailable`\n- requested_category: `{}`\n- available_categories: {}\n- attempted_provider_model: {}\n- nearest_fallback: {}",
            requested_category,
            format_string_list(available_categories),
            attempted_provider_model.as_deref().unwrap_or("none"),
            format_optional_fallback_route(nearest_fallback),
        ),
    }
}

fn format_string_list(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values
            .iter()
            .map(|value| format!("`{value}`"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_optional_fallback_route(route: &Option<FallbackRoute>) -> String {
    route
        .as_ref()
        .map(format_fallback_route)
        .unwrap_or_else(|| "none".to_string())
}

fn format_fallback_route(route: &FallbackRoute) -> String {
    let worker_model = route
        .worker_model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (route.worker_kind.provider_id_hint(), worker_model) {
        (Some(provider_id), Some(worker_model)) => format!("{provider_id}/{worker_model}"),
        (_, Some(worker_model)) => format!("{}({worker_model})", route.worker_kind.as_str()),
        _ => route.worker_kind.as_str().to_string(),
    }
}

fn indent_block(text: &str, spaces: usize) -> String {
    let indent = " ".repeat(spaces);
    text.lines()
        .map(|line| format!("{indent}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn worker_transcript_summary(worker_result: &WorkerResult) -> String {
    let Some(transcript_path) = worker_artifact_path(worker_result, "transcript.jsonl") else {
        return "No transcript artifact was recorded.".to_string();
    };
    let Some(transcript) = read_optional_context_text_if_exists(&transcript_path) else {
        return format!(
            "Transcript artifact `{}` could not be read.",
            transcript_path.to_string_lossy()
        );
    };

    let (head, tail) = worker_text_head_tail(&transcript, 16);
    format!(
        "- path: `{}`\n- head:\n```text\n{}\n```\n- tail:\n```text\n{}\n```",
        transcript_path.to_string_lossy(),
        head,
        tail
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
    use crate::test_support::test_support as ts;
    use crate::tools::ScopeCheck;
    use crate::workers::{WorkerKind, WorkerStatus};

    fn test_budget(max_iterations: usize) -> BudgetController {
        BudgetController {
            max_iterations,
            max_files_changed: usize::MAX,
            ..BudgetController::default()
        }
    }

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
                worker_model: None,
                worker_routes: Vec::new(),
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            allowed_paths: vec!["src".to_string(), "README.md".to_string()],
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            event_sink: Some(event_sink),
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            coordinator_model: Some(CoordinatorModel {
                provider_id: "openai".to_string(),
                model_id: "gpt-4.1".to_string(),
                name: "GPT-4.1".to_string(),
            }),
            coordinator_brief: Some("Prefer a compact local implementation.".to_string()),
            coordinator_review_hook: None,
            task_manager_control: None,
            task_manager: None,
            session_id: Some("acp-session-1".to_string()),
            continuation: true,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        let continuation_state = StateStore::new(temp_dir.path())
            .read_continuation_state_for_session("acp-session-1")?
            .context("continuation state should use the caller session id")?;
        assert_eq!(continuation_state.goal_id, outcome.goal_id);
        assert_eq!(continuation_state.status, ContinuationStatus::Completed);
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
        assert!(final_report.contains("## Evidence Chain"));
        assert!(final_report.contains("worker_outcome"));
        assert!(final_report.contains("verification.md"));
        assert!(final_report.contains("spec.md"));
        assert!(final_report.contains("plan.md"));
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
        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Failed,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            Some(&crate::state::ExecutionOwnership {
                delegated: true,
                worker_kind: Some("test_worker".to_string()),
                route_reason: "unit test ownership".to_string(),
                risk_profile: "low".to_string(),
                worker_task_id: Some("task_003".to_string()),
                decided_at: crate::state::timestamp(),
            }),
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Complete);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("verification passed"));
        assert!(evaluation.summary.contains("worker status was failed"));
    }

    #[test]
    fn evaluation_honors_provider_needs_user_stop_reason() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: None,
            summary: "The provider needs user input.".to_string(),
            repair_request: None,
            route_hint: None,
            stop_reason: Some("needs_user".to_string()),
            raw_response: "STOP_REASON: needs_user".to_string(),
        };

        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Succeeded,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            0,
            0,
            1,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            true,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::NeedsUser);
        assert!(!evaluation.should_continue);
    }

    #[test]
    fn evaluation_continues_when_independent_review_is_requested() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: None,
            summary: "Run an independent review worker before completion.".to_string(),
            repair_request: Some("Audit the final state independently.".to_string()),
            route_hint: Some("review".to_string()),
            stop_reason: None,
            raw_response: "GOAL_SATISFIED: unknown\nROUTE_HINT: review".to_string(),
        };

        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Deep,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            0,
            0,
            1,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert!(evaluation.summary.contains("independent review worker"));
    }

    #[test]
    fn evaluation_requires_independent_review_even_when_provider_is_confident() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: Some(true),
            summary: "Run an independent review worker before completion.".to_string(),
            repair_request: Some("Audit the final state independently.".to_string()),
            route_hint: Some("review".to_string()),
            stop_reason: Some("complete".to_string()),
            raw_response: "GOAL_SATISFIED: yes\nROUTE_HINT: review\nSTOP_REASON: complete"
                .to_string(),
        };

        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Deep,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            0,
            0,
            1,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert_eq!(evaluation.route_hint_override.as_deref(), Some("review"));
    }

    #[test]
    fn evaluation_continues_on_first_unknown_provider_review() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: None,
            summary: "Still inconclusive.".to_string(),
            repair_request: Some("Inspect the current state again.".to_string()),
            route_hint: None,
            stop_reason: None,
            raw_response: "GOAL_SATISFIED: unknown".to_string(),
        };

        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Repair,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            1,
            0,
            1,
            &test_budget(3),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert_eq!(evaluation.route_hint_override, None);
        assert!(evaluation.summary.contains("inconclusive"));
    }

    #[test]
    fn evaluation_escalates_to_review_after_second_unknown_provider_review() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: None,
            summary: "Still inconclusive.".to_string(),
            repair_request: Some("Request independent review.".to_string()),
            route_hint: None,
            stop_reason: None,
            raw_response: "GOAL_SATISFIED: unknown".to_string(),
        };

        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Repair,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            2,
            0,
            2,
            &test_budget(4),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert_eq!(evaluation.route_hint_override.as_deref(), Some("review"));
    }

    #[test]
    fn evaluation_honors_provider_unknown_streak_budget_limit() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: None,
            summary: "Still inconclusive.".to_string(),
            repair_request: Some("Request independent review.".to_string()),
            route_hint: None,
            stop_reason: None,
            raw_response: "GOAL_SATISFIED: unknown".to_string(),
        };
        let budget = BudgetController {
            max_provider_unknown_streak: 1,
            ..BudgetController::default()
        };

        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Repair,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            1,
            0,
            1,
            &budget,
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert_eq!(evaluation.route_hint_override.as_deref(), Some("review"));
        assert!(evaluation.summary.contains("limit 1"));
    }

    #[test]
    fn evaluation_maps_worker_fallback_limit_to_limited() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Deep,
            true,
            Some(&TaskFailureKind::RepeatedFailureLimit),
            Some("same failure kind `WorkerFailed` reached retry limit 2"),
            &scope_check,
            None,
            0,
            0,
            1,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            true,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Limited);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("retry limit"));
    }

    #[test]
    fn evaluation_maps_premium_budget_limit_to_limited() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Skipped,
            WorkerCategory::Deep,
            false,
            Some(&TaskFailureKind::PremiumBudgetExceeded),
            Some("premium worker budget 1 exhausted before `claude` attempt 2"),
            &scope_check,
            None,
            0,
            0,
            1,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Limited);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("premium worker budget"));
    }

    #[test]
    fn evaluation_maps_worker_call_budget_limit_to_limited() {
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = BudgetController {
            max_worker_calls: 1,
            max_provider_unknown_streak: 2,
            ..BudgetController::default()
        };
        let snapshot = BudgetSnapshot {
            worker_call_count: 1,
            ..BudgetSnapshot::default()
        };
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Deep,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            &budget,
            &snapshot,
            &[],
            true,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Limited);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("worker_calls"));
    }

    #[test]
    fn evaluation_limits_when_no_fallback_available() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            2,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Limited);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("no alternative fallback"));
    }

    #[test]
    fn evaluation_continues_on_first_iteration_when_no_fallback() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
    }

    #[test]
    fn worker_call_count_increments_once_per_iteration() {
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = BudgetController::default();
        let review_gate =
            ReviewGate::from_inputs(false, &WorkerStatus::Failed, &scope_check, None, &[], &[]);
        let snapshot = BudgetSnapshot {
            worker_call_count: 1,
            attempt_count: 3,
            ..BudgetSnapshot::default()
        };
        let summary = budget_summary(&budget, &snapshot, 1, 0, 1, 0);
        assert!(
            summary.contains("worker_calls=1/"),
            "summary should show worker_call_count=1: {}",
            summary
        );
        assert!(
            summary.contains("attempts=3"),
            "summary should show attempts=3: {}",
            summary
        );
        let policy = GoalDecisionPolicy {
            verification_passed: false,
            worker_status: &WorkerStatus::Failed,
            worker_category: WorkerCategory::Deep,
            require_worker: false,
            worker_failure_kind: None,
            worker_retry_reason: None,
            scope_check: &scope_check,
            coordinator_review: None,
            provider_unknown_streak: 0,
            repeated_failure_streak: 1,
            iteration: 1,
            budget: &budget,
            budget_snapshot: &snapshot,
            no_progress_signals: &[],
            nearest_fallback_available: false,
            trigger_source: None,
            ownership: None,
            review_gate: &review_gate,
            lineage: None,
        };
        assert!(
            policy.budget_guard_reason().is_none(),
            "worker_call_count=1 should not trigger guard with default max_worker_calls"
        );
        let limited_budget = BudgetController {
            max_worker_calls: 1,
            ..BudgetController::default()
        };
        let limited_policy = GoalDecisionPolicy {
            verification_passed: false,
            worker_status: &WorkerStatus::Failed,
            worker_category: WorkerCategory::Deep,
            require_worker: false,
            worker_failure_kind: None,
            worker_retry_reason: None,
            scope_check: &scope_check,
            coordinator_review: None,
            provider_unknown_streak: 0,
            repeated_failure_streak: 1,
            iteration: 1,
            budget: &limited_budget,
            budget_snapshot: &snapshot,
            no_progress_signals: &[],
            nearest_fallback_available: false,
            trigger_source: None,
            ownership: None,
            review_gate: &review_gate,
            lineage: None,
        };
        assert!(
            limited_policy.budget_guard_reason().is_some(),
            "worker_call_count=1 should trigger guard when max_worker_calls=1"
        );
        assert!(
            limited_policy
                .budget_guard_reason()
                .unwrap()
                .contains("worker_calls"),
            "guard reason should mention worker_calls"
        );
    }

    #[test]
    fn review_gate_reports_each_required_dimension() {
        let mut scope_check = crate::tools::ScopeCheck::default();
        scope_check.forbidden_touches.push(".env".to_string());
        let review = CoordinatorReview {
            goal_satisfied: Some(true),
            summary: "accepted".to_string(),
            repair_request: None,
            route_hint: None,
            stop_reason: None,
            raw_response: "GOAL_SATISFIED: yes".to_string(),
        };
        let gate = ReviewGate::from_inputs(
            true,
            &WorkerStatus::Succeeded,
            &scope_check,
            Some(&review),
            &[],
            &[],
        );
        assert!(gate.require_all_pass);
        assert_eq!(gate.results.len(), 4);
        assert!(gate.failed_reason().is_some());
        assert!(gate.summary().contains("security=fail"));
    }

    #[test]
    fn test_review_dimensions_have_unique_execution_ids() -> Result<()> {
        let scope_check = crate::tools::ScopeCheck::default();
        let gate =
            ReviewGate::from_inputs(true, &WorkerStatus::Succeeded, &scope_check, None, &[], &[]);
        // Validate — should pass with synthetic IDs
        assert!(gate.validate_independent_reviewers().is_ok());
        Ok(())
    }

    #[test]
    fn test_review_dimensions_reject_duplicate_execution_ids() {
        let gate = ReviewGate {
            require_all_pass: true,
            results: vec![
                ReviewDimensionResult {
                    dimension: ReviewDimension::GoalVerification,
                    passed: true,
                    evidence: "test".to_string(),
                    reviewer_evidence: Some(ReviewerEvidence {
                        execution_id: "same-id".to_string(),
                        route: "coordinator".to_string(),
                        artifact_path: None,
                        verdict: "pass".to_string(),
                    }),
                },
                ReviewDimensionResult {
                    dimension: ReviewDimension::CodeQuality,
                    passed: true,
                    evidence: "test".to_string(),
                    reviewer_evidence: Some(ReviewerEvidence {
                        execution_id: "same-id".to_string(), // DUPLICATE
                        route: "scope-check".to_string(),
                        artifact_path: None,
                        verdict: "pass".to_string(),
                    }),
                },
            ],
        };
        assert!(
            gate.validate_independent_reviewers().is_err(),
            "duplicate execution_id should cause hard fail"
        );
    }

    #[test]
    fn continuation_stop_marker_survives_and_can_be_cleared() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        store.write_continuation_state("session-1", "goal-1", ContinuationStatus::Stopped)?;
        assert!(store.continuation_is_stopped_for_session("session-1")?);
        store.clear_continuation_stop_for_session("session-1")?;
        assert!(!store.continuation_is_stopped_for_session("session-1")?);
        Ok(())
    }

    #[test]
    fn continuation_two_sessions_overwrite_each_other() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        // Session A writes stopped
        let path_a =
            store.write_continuation_state("ses_A", "goal_A", ContinuationStatus::Stopped)?;
        assert!(store.continuation_is_stopped_for_session("ses_A")?);

        // Session B writes running — previously this overwrote A (bug), now it should NOT
        let path_b =
            store.write_continuation_state("ses_B", "goal_B", ContinuationStatus::Running)?;

        // VERIFICATION: Different sessions now have DIFFERENT paths
        assert_ne!(
            path_a, path_b,
            "FIX: different sessions should write to different paths"
        );

        // VERIFICATION: A's state is preserved (file still contains ses_A)
        let state_json_a = std::fs::read_to_string(&path_a)?;
        assert!(
            state_json_a.contains("ses_A"),
            "FIX: ses_A's data should still be present"
        );

        // VERIFICATION: A's stopped status is preserved
        assert!(
            store.continuation_is_stopped_for_session("ses_A")?,
            "FIX: ses_A should still be stopped"
        );

        // VERIFICATION: B is running
        assert!(
            !store.continuation_is_stopped_for_session("ses_B")?,
            "FIX: ses_B should be running"
        );

        // VERIFICATION: Clearing A does not affect B
        store.clear_continuation_stop_for_session("ses_A")?;
        assert!(!store.continuation_is_stopped_for_session("ses_A")?);
        assert!(
            !store.continuation_is_stopped_for_session("ses_B")?,
            "FIX: clearing ses_A should not affect ses_B's running state"
        );

        Ok(())
    }

    #[test]
    fn acp_session_id_to_gear_session_id_mapping_stable() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        // Same session ID "ses_X" always produces the same file path
        let path_1 =
            store.write_continuation_state("ses_X", "goal_1", ContinuationStatus::Running)?;
        let path_2 =
            store.write_continuation_state("ses_X", "goal_2", ContinuationStatus::Stopped)?;

        // Same session writes to the same path (overwrites its own state — OK)
        assert_eq!(path_1, path_2, "same session should write to the same path");

        // DIFFERENT sessions now write to DIFFERENT paths
        let path_b =
            store.write_continuation_state("ses_Y", "goal_Y", ContinuationStatus::Running)?;
        assert_ne!(
            path_1, path_b,
            "FIX: different sessions should write to different paths"
        );

        // ses_X's file still contains ses_X (not overwritten by ses_Y)
        let saved: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&path_1)?)?;
        assert_eq!(
            saved["session_id"], "ses_X",
            "ses_X's file should still contain ses_X"
        );
        Ok(())
    }

    #[test]
    fn test_continuation_isolation_with_caller_session_id() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let path_a = store.write_continuation_state(
            "acp-session-A",
            "goal_A",
            ContinuationStatus::Running,
        )?;
        let path_b = store.write_continuation_state(
            "acp-session-B",
            "goal_B",
            ContinuationStatus::Stopped,
        )?;

        assert_ne!(path_a, path_b);
        assert!(store.continuation_is_stopped_for_session("acp-session-B")?);
        assert!(!store.continuation_is_stopped_for_session("acp-session-A")?);

        let a_content = std::fs::read_to_string(&path_a)?;
        assert!(a_content.contains("acp-session-A"));
        let b_content = std::fs::read_to_string(&path_b)?;
        assert!(b_content.contains("acp-session-B"));

        store.clear_continuation_stop_for_session("acp-session-A")?;
        assert!(!store.continuation_is_stopped_for_session("acp-session-A")?);
        assert!(store.continuation_is_stopped_for_session("acp-session-B")?);

        Ok(())
    }

    #[test]
    fn budget_uses_goal_max_worker_calls() {
        let mut goal_budget = Budget::default();
        goal_budget.max_worker_calls = 1;
        let goal = Goal {
            id: "goal_test".to_string(),
            title: "test".to_string(),
            status: GoalStatus::Running,
            workspace: "/tmp".to_string(),
            created_at: "now".to_string(),
            updated_at: "now".to_string(),
            request: "test request".to_string(),
            product_type: "unknown".to_string(),
            language_profile: "unknown".to_string(),
            success_criteria: vec![],
            budget: goal_budget,
            current_task_id: None,
            coordinator_model: None,
            coordinator_brief: None,
            summary: String::new(),
        };

        let budget_controller = BudgetController {
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_files_changed: usize::MAX,
            max_worker_calls: goal.budget.max_worker_calls,
            max_premium_worker_calls: usize::MAX,
            max_same_failure_retries: 2,
            max_provider_unknown_streak: goal.budget.max_provider_unknown_streak,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
        };

        assert_eq!(budget_controller.max_worker_calls, 1);

        let scope_check = crate::tools::ScopeCheck::default();

        let first_snapshot = BudgetSnapshot {
            worker_call_count: 0,
            ..BudgetSnapshot::default()
        };
        let first_evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Deep,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            &budget_controller,
            &first_snapshot,
            &[],
            true,
            None,
            None,
            None,
        );
        assert!(first_evaluation.should_continue);

        let second_snapshot = BudgetSnapshot {
            worker_call_count: 1,
            ..BudgetSnapshot::default()
        };
        let second_evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Deep,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            &budget_controller,
            &second_snapshot,
            &[],
            true,
            None,
            None,
            None,
        );
        assert_eq!(second_evaluation.status, GoalStatus::Limited);
        assert!(!second_evaluation.should_continue);
    }

    #[test]
    fn budget_guard_reason_includes_trigger_source_label() {
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = BudgetController {
            max_worker_calls: 1,
            max_premium_worker_calls: 1,
            max_provider_unknown_streak: 2,
            ..BudgetController::default()
        };
        let review_gate =
            ReviewGate::from_inputs(false, &WorkerStatus::Failed, &scope_check, None, &[], &[]);
        let route_snapshot = BudgetSnapshot {
            worker_call_count: 1,
            ..BudgetSnapshot::default()
        };
        let policy = GoalDecisionPolicy {
            verification_passed: false,
            worker_status: &WorkerStatus::Failed,
            worker_category: WorkerCategory::Quick,
            require_worker: false,
            worker_failure_kind: None,
            worker_retry_reason: None,
            scope_check: &scope_check,
            coordinator_review: None,
            provider_unknown_streak: 0,
            repeated_failure_streak: 1,
            iteration: 1,
            budget: &budget,
            budget_snapshot: &route_snapshot,
            no_progress_signals: &[],
            nearest_fallback_available: false,
            trigger_source: Some(RouteChangeType::RouteChange),
            ownership: None,
            review_gate: &review_gate,
            lineage: None,
        };
        let reason = policy
            .budget_guard_reason()
            .expect("budget guard should fire");
        assert!(
            reason.contains("(route change)"),
            "RouteChange reason should contain '(route change)': {reason}"
        );

        let fallback_policy = GoalDecisionPolicy {
            trigger_source: Some(RouteChangeType::Fallback),
            ..policy
        };
        let reason = fallback_policy
            .budget_guard_reason()
            .expect("budget guard should fire");
        assert!(
            reason.contains("(fallback)"),
            "Fallback reason should contain '(fallback)': {reason}"
        );

        let premium_snapshot = BudgetSnapshot {
            premium_worker_call_count: 1,
            ..BudgetSnapshot::default()
        };
        let review_policy = GoalDecisionPolicy {
            worker_category: WorkerCategory::Review,
            budget_snapshot: &premium_snapshot,
            trigger_source: Some(RouteChangeType::ReviewTrigger),
            ..policy
        };
        let reason = review_policy
            .budget_guard_reason()
            .expect("budget guard should fire");
        assert!(
            reason.contains("(review)"),
            "ReviewTrigger reason should contain '(review)': {reason}"
        );
    }

    #[test]
    fn apply_budget_for_route_change_distinguishes_triggers() {
        let budget = BudgetController {
            max_worker_calls: 2,
            max_premium_worker_calls: 1,
            ..BudgetController::default()
        };
        let snapshot = BudgetSnapshot {
            worker_call_count: 1,
            premium_worker_call_count: 0,
            ..BudgetSnapshot::default()
        };
        assert!(
            budget
                .apply_budget_for_route_change(&snapshot, RouteChangeType::RouteChange)
                .is_ok(),
            "under budget should be Ok"
        );

        let full_snapshot = BudgetSnapshot {
            worker_call_count: 2,
            ..BudgetSnapshot::default()
        };
        let result =
            budget.apply_budget_for_route_change(&full_snapshot, RouteChangeType::RouteChange);
        assert!(result.is_err());
        assert!(
            result.as_ref().unwrap_err().contains("route change"),
            "Err should mention route change: {:?}",
            result
        );

        let fallback_result =
            budget.apply_budget_for_route_change(&full_snapshot, RouteChangeType::Fallback);
        assert!(fallback_result.is_err());
        assert!(
            fallback_result.as_ref().unwrap_err().contains("fallback"),
            "Err should mention fallback: {:?}",
            fallback_result
        );

        let premium_snapshot = BudgetSnapshot {
            premium_worker_call_count: 1,
            ..BudgetSnapshot::default()
        };
        let review_result =
            budget.apply_budget_for_route_change(&premium_snapshot, RouteChangeType::ReviewTrigger);
        assert!(review_result.is_err());
        assert!(
            review_result.as_ref().unwrap_err().contains("review"),
            "Err should mention review: {:?}",
            review_result
        );
    }

    #[test]
    fn budget_summary_matches_across_coordinator_review_and_goal_review() {
        let budget = BudgetController::default();
        let snapshot = BudgetSnapshot {
            worker_call_count: 3,
            premium_worker_call_count: 1,
            attempt_count: 5,
            context_risk_signals: vec!["token limit".to_string()],
            ..BudgetSnapshot::default()
        };
        let summary = budget_summary(&budget, &snapshot, 2, 1, 3, 4);

        assert!(summary.contains("worker_calls=3/5"));
        assert!(summary.contains("attempts=5"));
        assert!(summary.contains("same_failure_retries=1/2"));
        assert!(summary.contains("token limit"));
        assert!(summary.contains("iterations=3/5"));
        assert!(summary.contains("provider_unknown_streak=1/2"));

        let evaluation = GoalEvaluation {
            status: GoalStatus::Running,
            should_continue: true,
            summary: "keep going".to_string(),
            route_hint_override: None,
        };
        let worker_result = WorkerResult {
            status: WorkerStatus::Succeeded,
            command: None,
            exit_code: None,
            summary: "done".to_string(),
            packet_path: PathBuf::from("/tmp/packet.json"),
            prompt_path: PathBuf::from("/tmp/prompt.md"),
            stdout_path: None,
            stderr_path: None,
            last_message_path: None,
            result_path: PathBuf::from("/tmp/result.json"),
            outcome_path: PathBuf::from("/tmp/outcome.json"),
        };
        let worker_outcome = WorkerOutcome {
            status: WorkerStatus::Succeeded,
            session_id: None,
            session_capability: None,
            summary: "outcome".to_string(),
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            known_failures: Vec::new(),
            raw_output_path: None,
            command: None,
            exit_code: None,
        };
        let scope_check = ScopeCheck {
            forbidden_touches: Vec::new(),
            outside_allowed_paths: Vec::new(),
            max_files_exceeded: false,
            changed_file_count: 4,
        };
        let category_resolution = CategoryResolution::default();
        let category_resolution_result = CategoryResolutionResult::Resolved {
            requested_category: "quick".to_string(),
            available_categories: vec!["quick".to_string()],
            attempted_provider_model: None,
            nearest_fallback: None,
        };
        let artifact = goal_review_artifact(
            3,
            5,
            &evaluation,
            &worker_result,
            WorkerCategory::Quick,
            None,
            "route reason",
            &category_resolution,
            &category_resolution_result,
            &[],
            None,
            None,
            "none",
            &summary,
            &worker_outcome,
            &scope_check,
            &[],
            None,
        );
        assert!(
            artifact.contains(&summary),
            "goal review artifact should embed the exact same budget_summary string"
        );
        assert!(artifact.contains("## Review Gate"));
        assert!(artifact.contains("goal_verification"));
    }

    #[test]
    fn evaluation_maps_child_depth_budget_limit_to_limited() {
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = BudgetController {
            max_child_depth: 1,
            max_provider_unknown_streak: 2,
            ..BudgetController::default()
        };
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            3,
            &budget,
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Limited);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("child_depth"));
    }

    #[test]
    fn evaluation_maps_runtime_budget_limit_to_limited() {
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = BudgetController {
            max_runtime_minutes: 1,
            max_provider_unknown_streak: 2,
            ..BudgetController::default()
        };
        let snapshot = BudgetSnapshot {
            runtime_elapsed_minutes: 1,
            ..BudgetSnapshot::default()
        };
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            &budget,
            &snapshot,
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Limited);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("runtime_minutes"));
    }

    #[test]
    fn context_risk_signals_pick_up_worker_artifact_text() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let stdout_path = temp_dir.path().join("stdout.txt");
        fs::write(&stdout_path, "plain worker output")?;
        let result_path = temp_dir.path().join("result.json");
        fs::write(&result_path, "{}")?;
        let outcome_path = temp_dir.path().join("outcome.json");
        fs::write(&outcome_path, "{}")?;
        fs::write(
            temp_dir.path().join("transcript.jsonl"),
            "{\"turn_started\":{\"kind\":\"opencode\",\"prompt_path\":\"prompt.md\"}}\n{\"tool_call_started\":{\"kind\":\"opencode\",\"tool_name\":\"edit\"}}\n",
        )?;
        fs::write(
            temp_dir.path().join("tool-events.jsonl"),
            "{\"tool_call_started\":{\"kind\":\"opencode\",\"tool_name\":\"edit\"}}\n",
        )?;
        fs::write(
            temp_dir.path().join("partial-output.md"),
            "partial output was recorded",
        )?;
        let worker_result = WorkerResult {
            status: WorkerStatus::Succeeded,
            command: None,
            exit_code: None,
            summary: "worker finished".to_string(),
            packet_path: temp_dir.path().join("packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
            stdout_path: Some(stdout_path),
            stderr_path: None,
            last_message_path: None,
            result_path,
            outcome_path,
        };
        let worker_outcome = WorkerOutcome {
            status: WorkerStatus::Succeeded,
            session_id: None,
            session_capability: None,
            summary: "outcome summary".to_string(),
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            known_failures: Vec::new(),
            raw_output_path: None,
            command: None,
            exit_code: None,
        };
        let signals = detect_context_risk_signals(collect_context_risk_texts(
            &worker_result,
            &worker_outcome,
            &TaskRecord {
                task_id: "task".to_string(),
                worker_kind: "opencode".to_string(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".to_string(),
                route_hint: None,
                route_reason: "route reason".to_string(),
                status: crate::task_manager::ManagedTaskStatus::Running,
                started_at: timestamp(),
                finished_at: None,
                residency_state: crate::task_manager::ResidencyState::Resident,
                run_epoch: 1,
                notified_epoch: -1,
                notification_failed_epoch: None,
                killed: false,
                session_id: None,
                parent_session_id: None,
                root_session_id: None,
                parent_task_id: None,
                result_path: None,
                outcome_path: None,
                summary: "record summary".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
                attempts: Vec::new(),
            },
            None,
        ));

        assert!(
            signals
                .iter()
                .any(|signal| signal.contains("worker transcript ended before turn_finished"))
        );
        assert!(
            signals
                .iter()
                .any(|signal| signal.contains("tool event stream ended before tool_call_finished"))
        );
        assert!(
            signals
                .iter()
                .any(|signal| signal.contains("partial output artifact recorded"))
        );
        Ok(())
    }

    #[test]
    fn context_risk_signals_pick_up_token_limit_and_compaction_text() {
        let signals = detect_context_risk_signals([
            "token limit reported".to_string(),
            "context compaction reported".to_string(),
        ]);

        assert!(
            signals
                .iter()
                .any(|signal| signal.contains("token limit reported"))
        );
        assert!(
            signals
                .iter()
                .any(|signal| signal.contains("context compaction reported"))
        );
    }

    #[test]
    fn notification_delivery_failure_records_failed_epoch() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let task_record = TaskRecord {
            task_id: "task_delivery_fail".to_string(),
            worker_kind: "opencode".to_string(),
            worker_command: None,
            worker_model: None,
            worker_category: "quick".to_string(),
            route_hint: None,
            route_reason: "route reason".to_string(),
            status: crate::task_manager::ManagedTaskStatus::Completed,
            started_at: timestamp(),
            finished_at: Some(timestamp()),
            residency_state: crate::task_manager::ResidencyState::Resident,
            run_epoch: 7,
            notified_epoch: -1,
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            parent_task_id: None,
            result_path: Some(temp_dir.path().join("result.json")),
            outcome_path: Some(temp_dir.path().join("outcome.json")),
            summary: "task summary".to_string(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: Vec::new(),
        };
        let task_record_json = serde_json::to_string_pretty(&task_record)?;
        store.write_worker_file(
            "task_delivery_fail",
            "task-record.json",
            &format!("{task_record_json}\n"),
        )?;

        record_completion_notification_failed_epoch(&store, "task_delivery_fail", 7)?;

        let stored_task_record_path = store
            .worker_dir("task_delivery_fail")
            .join("task-record.json");
        let stored_task_record = fs::read_to_string(&stored_task_record_path)?;
        let stored_task_record: TaskRecord = serde_json::from_str(&stored_task_record)?;
        assert_eq!(stored_task_record.notification_failed_epoch, Some(7));
        assert_eq!(stored_task_record.notified_epoch, -1);
        Ok(())
    }

    #[test]
    fn evaluation_pauses_when_context_becomes_unreliable() {
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = BudgetController::default();
        let snapshot = BudgetSnapshot {
            context_risk_signals: vec!["token limit reported".to_string()],
            ..BudgetSnapshot::default()
        };
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            &budget,
            &snapshot,
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::NeedsUser);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("context became unreliable"));
    }

    #[test]
    fn evaluation_prevents_completion_when_context_becomes_unreliable() {
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = BudgetController::default();
        let snapshot = BudgetSnapshot {
            context_risk_signals: vec![
                "token limit reported".to_string(),
                "context compaction reported".to_string(),
            ],
            ..BudgetSnapshot::default()
        };
        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            &budget,
            &snapshot,
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::NeedsUser);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("Goal paused before completion"));
        assert!(evaluation.summary.contains("token limit reported"));
        assert!(evaluation.summary.contains("context compaction reported"));
    }

    #[test]
    fn evaluation_maps_required_worker_unavailable_to_needs_user() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Skipped,
            WorkerCategory::Repair,
            true,
            Some(&TaskFailureKind::WorkerUnavailable),
            Some("configure a worker command"),
            &scope_check,
            None,
            0,
            0,
            1,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::NeedsUser);
        assert!(!evaluation.should_continue);
        assert!(
            evaluation
                .summary
                .contains("required worker is unavailable")
        );
    }

    #[test]
    fn evaluation_does_not_allow_provider_complete_to_override_failed_verification() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: Some(true),
            summary: "The provider thinks the goal is complete.".to_string(),
            repair_request: None,
            route_hint: None,
            stop_reason: Some("complete".to_string()),
            raw_response: "GOAL_SATISFIED: yes\nSTOP_REASON: complete".to_string(),
        };

        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Succeeded,
            WorkerCategory::Repair,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            0,
            0,
            1,
            &test_budget(DEFAULT_MAX_ITERATIONS),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
    }

    #[test]
    fn evaluation_escalates_repeated_failures_to_deep() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Repair,
            false,
            Some(&TaskFailureKind::WorkerFailed),
            Some("worker failed twice"),
            &scope_check,
            None,
            0,
            2,
            2,
            &test_budget(4),
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert_eq!(evaluation.route_hint_override.as_deref(), Some("deep"));
    }

    #[test]
    fn coordinator_review_can_request_repair_after_passing_verification() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;
        let review_calls = Arc::new(Mutex::new(0usize));
        let hook: CoordinatorReviewHook = {
            let review_calls = review_calls.clone();
            Arc::new(move |input| {
                let mut calls = review_calls.lock().expect("review mutex poisoned");
                *calls += 1;
                if input.iteration == 1 {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(false),
                        summary: "The provider review wants one more repair pass.".to_string(),
                        repair_request: Some("Re-check the minimal deliverable.".to_string()),
                        route_hint: Some("deep".to_string()),
                        stop_reason: None,
                        raw_response: "GOAL_SATISFIED: no\nSUMMARY: The provider review wants one more repair pass.\nREPAIR_REQUEST: Re-check the minimal deliverable.\nROUTE_HINT: deep".to_string(),
                    }))
                } else {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(true),
                        summary: "The goal is now satisfied.".to_string(),
                        repair_request: None,
                        route_hint: None,
                        stop_reason: Some("complete".to_string()),
                        raw_response: "GOAL_SATISFIED: yes\nSUMMARY: The goal is now satisfied.\nREPAIR_REQUEST: none".to_string(),
                    }))
                }
            })
        };

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: vec![
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Opencode,
                        worker_command: None,
                        worker_model: None,
                    },
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Codex,
                        worker_command: None,
                        worker_model: None,
                    },
                ],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: Some(hook),
            task_manager_control: None,
            task_manager: None,
            session_id: None,
            continuation: false,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert_eq!(*review_calls.lock().expect("review mutex poisoned"), 2);
        assert!(
            outcome
                .artifacts_root
                .join("coordinator-review-iteration-1.md")
                .exists()
        );
        assert!(
            outcome
                .artifacts_root
                .join("verification-iteration-2.md")
                .exists()
        );
        let repair_packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_005/packet.json"),
        )?;
        assert!(repair_packet.contains(r#""worker": "codex""#));
        Ok(())
    }

    #[test]
    fn coordinator_review_can_request_independent_review_after_passing_verification() -> Result<()>
    {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;
        let review_calls = Arc::new(Mutex::new(0usize));
        let hook: CoordinatorReviewHook = {
            let review_calls = review_calls.clone();
            Arc::new(move |input| {
                let mut calls = review_calls.lock().expect("review mutex poisoned");
                *calls += 1;
                if input.iteration == 1 {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: None,
                        summary: "Run an independent review worker.".to_string(),
                        repair_request: Some("Audit the current deliverable without expanding scope.".to_string()),
                        route_hint: Some("review".to_string()),
                        stop_reason: None,
                        raw_response: "GOAL_SATISFIED: unknown\nSUMMARY: Run an independent review worker.\nREPAIR_REQUEST: Audit the current deliverable without expanding scope.\nROUTE_HINT: review".to_string(),
                    }))
                } else {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(true),
                        summary: "Independent review accepted the result.".to_string(),
                        repair_request: None,
                        route_hint: None,
                        stop_reason: Some("complete".to_string()),
                        raw_response: "GOAL_SATISFIED: yes\nSUMMARY: Independent review accepted the result.\nSTOP_REASON: complete".to_string(),
                    }))
                }
            })
        };

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: vec![
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Opencode,
                        worker_command: None,
                        worker_model: None,
                    },
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Codex,
                        worker_command: None,
                        worker_model: None,
                    },
                ],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: Some(hook),
            task_manager_control: None,
            task_manager: None,
            session_id: None,
            continuation: false,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert_eq!(*review_calls.lock().expect("review mutex poisoned"), 2);
        let review_packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_005/packet.json"),
        )?;
        assert!(review_packet.contains(r#""worker": "codex""#));
        let review_prompt = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_005/prompt.md"),
        )?;
        assert!(review_prompt.contains("Independent review iteration 2"));
        Ok(())
    }

    #[test]
    fn coordinator_review_route_hint_review_forces_independent_reviewer_even_when_satisfied()
    -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;
        let review_calls = Arc::new(Mutex::new(0usize));
        let hook: CoordinatorReviewHook = {
            let review_calls = review_calls.clone();
            Arc::new(move |input| {
                let mut calls = review_calls.lock().expect("review mutex poisoned");
                *calls += 1;
                if input.iteration == 1 {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(true),
                        summary: "Independent review is still required.".to_string(),
                        repair_request: Some(
                            "Audit the current deliverable without expanding scope.".to_string(),
                        ),
                        route_hint: Some("review".to_string()),
                        stop_reason: Some("complete".to_string()),
                        raw_response: "GOAL_SATISFIED: yes\nSUMMARY: Independent review is still required.\nREPAIR_REQUEST: Audit the current deliverable without expanding scope.\nROUTE_HINT: review\nSTOP_REASON: complete".to_string(),
                    }))
                } else {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(true),
                        summary: "Independent review accepted the result.".to_string(),
                        repair_request: None,
                        route_hint: None,
                        stop_reason: Some("complete".to_string()),
                        raw_response: "GOAL_SATISFIED: yes\nSUMMARY: Independent review accepted the result.\nSTOP_REASON: complete".to_string(),
                    }))
                }
            })
        };

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: vec![
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Opencode,
                        worker_command: None,
                        worker_model: None,
                    },
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Codex,
                        worker_command: None,
                        worker_model: None,
                    },
                ],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: Some(hook),
            task_manager_control: None,
            task_manager: None,
            session_id: None,
            continuation: false,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert_eq!(*review_calls.lock().expect("review mutex poisoned"), 2);
        let review_packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_005/packet.json"),
        )?;
        assert!(review_packet.contains(r#""worker": "codex""#));
        let review_prompt = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_005/prompt.md"),
        )?;
        assert!(review_prompt.contains("Independent review iteration 2"));
        Ok(())
    }

    #[test]
    fn goal_review_artifact_includes_no_progress_signals() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let artifact_dir = temp_dir.path().join("worker");
        let worker_result = WorkerResult {
            status: WorkerStatus::Succeeded,
            command: Some("codex exec".to_string()),
            exit_code: Some(0),
            summary: "Worker completed its pass.".to_string(),
            packet_path: artifact_dir.join("packet.json"),
            prompt_path: artifact_dir.join("prompt.md"),
            stdout_path: None,
            stderr_path: None,
            last_message_path: None,
            result_path: artifact_dir.join("result.json"),
            outcome_path: artifact_dir.join("outcome.json"),
        };
        let worker_outcome = WorkerOutcome {
            status: WorkerStatus::Succeeded,
            session_id: None,
            session_capability: None,
            summary: "Outcome summary".to_string(),
            changed_files: vec!["src/main.rs".to_string()],
            commands_run: vec!["cargo test".to_string()],
            known_failures: Vec::new(),
            raw_output_path: None,
            command: Some("codex exec".to_string()),
            exit_code: Some(0),
        };
        let evaluation = GoalEvaluation {
            status: GoalStatus::Running,
            should_continue: true,
            summary: "Keep iterating.".to_string(),
            route_hint_override: None,
        };
        let scope_check = ScopeCheck {
            forbidden_touches: Vec::new(),
            outside_allowed_paths: Vec::new(),
            max_files_exceeded: false,
            changed_file_count: 1,
        };
        let category_resolution = CategoryResolution::default();
        let category_resolution_result = CategoryResolutionResult::Resolved {
            requested_category: "review".to_string(),
            available_categories: vec!["review".to_string()],
            attempted_provider_model: Some("openai/gpt-5".to_string()),
            nearest_fallback: None,
        };
        let artifact = goal_review_artifact(
            2,
            5,
            &evaluation,
            &worker_result,
            WorkerCategory::Review,
            Some("gpt-5"),
            "category `review` selected attempt 2 configured `codex` route",
            &category_resolution,
            &category_resolution_result,
            &["No file changes detected for 2 consecutive iterations.".to_string()],
            None,
            None,
            "none",
            "iterations=2/5; changed_files=1/10",
            &worker_outcome,
            &scope_check,
            &[],
            None,
        );

        assert!(artifact.contains("## No Progress"));
        assert!(artifact.contains("No file changes detected for 2 consecutive iterations."));
        Ok(())
    }

    #[test]
    fn consecutive_unknown_reviews_escalate_to_review_worker() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;
        let review_calls = Arc::new(Mutex::new(0usize));
        let hook: CoordinatorReviewHook = {
            let review_calls = review_calls.clone();
            Arc::new(move |input| {
                let mut calls = review_calls.lock().expect("review mutex poisoned");
                *calls += 1;
                if input.iteration < 3 {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: None,
                        summary: "Still inconclusive.".to_string(),
                        repair_request: Some("Keep checking the final state.".to_string()),
                        route_hint: None,
                        stop_reason: None,
                        raw_response: "GOAL_SATISFIED: unknown\nSUMMARY: Still inconclusive."
                            .to_string(),
                    }))
                } else {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(true),
                        summary: "Independent review accepted the result.".to_string(),
                        repair_request: None,
                        route_hint: None,
                        stop_reason: Some("complete".to_string()),
                        raw_response: "GOAL_SATISFIED: yes\nSTOP_REASON: complete".to_string(),
                    }))
                }
            })
        };

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: vec![
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Opencode,
                        worker_command: None,
                        worker_model: None,
                    },
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Codex,
                        worker_command: None,
                        worker_model: None,
                    },
                ],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: 3,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: Some(hook),
            task_manager_control: None,
            task_manager: None,
            session_id: None,
            continuation: false,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert_eq!(*review_calls.lock().expect("review mutex poisoned"), 3);
        let third_packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_repair_003/packet.json"),
        )?;
        assert!(third_packet.contains(r#""worker": "codex""#));
        Ok(())
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
                worker_model: None,
                worker_routes: Vec::new(),
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
                default_worker_for_small_tasks: WorkerKind::Opencode,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: None,
            task_manager_control: None,
            task_manager: None,
            session_id: None,
            continuation: false,
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
                worker_model: None,
                worker_routes: Vec::new(),
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
                default_worker_for_small_tasks: WorkerKind::Opencode,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: None,
            task_manager_control: None,
            task_manager: None,
            session_id: None,
            continuation: false,
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
                worker_model: None,
                worker_routes: Vec::new(),
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: Vec::new(),
            max_files_changed: 10,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: Some(cancellation_token),
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: None,
            task_manager_control: None,
            task_manager: None,
            session_id: None,
            continuation: false,
        })
        .expect_err("run should be cancelled");

        assert!(
            error.to_string().contains("Gear run cancelled"),
            "{error:#}"
        );
        Ok(())
    }

    #[test]
    fn coordinator_review_parsing_is_case_insensitive() {
        let raw = "goal_satisfied: YES\nroute_hint: DEEP\nstop_reason: LIMITED\nsummary: Done\nrepair_request: FIX";
        let (review, warnings) = parse_coordinator_review(raw);
        assert_eq!(review.goal_satisfied, Some(true));
        assert_eq!(review.route_hint.as_deref(), Some("DEEP"));
        assert_eq!(review.stop_reason.as_deref(), Some("LIMITED"));
        assert_eq!(review.summary, "Done");
        assert_eq!(review.repair_request.as_deref(), Some("FIX"));
        assert!(warnings.is_empty());
    }

    #[test]
    fn coordinator_review_parser_warns_on_unknown_goal_satisfied() {
        let raw = "goal_satisfied: maybe\nsummary: unclear";
        let (review, warnings) = parse_coordinator_review(raw);
        assert_eq!(review.goal_satisfied, None);
        assert_eq!(review.summary, "unclear");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("Unrecognized GOAL_SATISFIED"));
    }

    #[test]
    fn coordinator_review_raw_response_preserved_on_parse_failure() {
        let raw = "some unparseable text without any known keys";
        let (review, warnings) = parse_coordinator_review(raw);
        assert_eq!(review.raw_response, raw);
        assert_eq!(review.summary, raw);
        assert_eq!(review.goal_satisfied, None);
        assert!(warnings.is_empty());
    }

    #[test]
    fn stagnation_detects_consecutive_no_diff_iterations() {
        let diff1 = DiffSnapshot {
            changed_files: vec!["a.rs".to_string()],
            ..DiffSnapshot::default()
        };
        let diff2 = DiffSnapshot {
            changed_files: vec!["a.rs".to_string()],
            ..DiffSnapshot::default()
        };
        let diff3 = DiffSnapshot {
            changed_files: vec!["a.rs".to_string()],
            ..DiffSnapshot::default()
        };
        let signals = detect_stagnation(&[diff1, diff2, diff3], &[], &[], &[]);
        assert!(!signals.is_empty());
        assert!(signals[0].contains("No file changes detected"));
    }

    #[test]
    fn stagnation_detects_identical_content_by_diff_hash() {
        let diff1 = DiffSnapshot {
            changed_files: vec!["a.rs".to_string()],
            diff_hash: Some("hash_abc".to_string()),
            ..DiffSnapshot::default()
        };
        let diff2 = DiffSnapshot {
            changed_files: vec!["a.rs".to_string()],
            diff_hash: Some("hash_abc".to_string()),
            ..DiffSnapshot::default()
        };
        let signals = detect_stagnation(&[diff1, diff2], &[], &[], &[]);
        assert!(!signals.is_empty());
        assert!(signals[0].contains("No file changes detected"));
    }

    #[test]
    fn stagnation_does_not_trigger_when_diff_hash_differs() {
        let diff1 = DiffSnapshot {
            changed_files: vec!["a.rs".to_string()],
            diff_hash: Some("hash_abc".to_string()),
            ..DiffSnapshot::default()
        };
        let diff2 = DiffSnapshot {
            changed_files: vec!["a.rs".to_string()],
            diff_hash: Some("hash_xyz".to_string()),
            ..DiffSnapshot::default()
        };
        let signals = detect_stagnation(&[diff1, diff2], &[], &[], &[]);
        let no_file_changes = signals
            .iter()
            .any(|s| s.contains("No file changes detected"));
        assert!(!no_file_changes);
    }

    #[test]
    fn stagnation_detects_identical_verification_failures() {
        let v1 = vec![ShellCommandResult {
            command: "cargo test".to_string(),
            success: false,
            exit_code: Some(1),
            stdout: "fail".to_string(),
            stderr: "error".to_string(),
            duration_ms: 0,
        }];
        let v2 = vec![ShellCommandResult {
            command: "cargo test".to_string(),
            success: false,
            exit_code: Some(1),
            stdout: "fail".to_string(),
            stderr: "error".to_string(),
            duration_ms: 0,
        }];
        let signals = detect_stagnation(&[], &[v1, v2], &[], &[]);
        assert!(!signals.is_empty());
        assert!(signals[0].contains("Identical verification failures"));
    }

    #[test]
    fn stagnation_detects_repeated_repair_requests() {
        let signals = detect_stagnation(
            &[],
            &[],
            &["fix foo".to_string(), "fix foo".to_string()],
            &[],
        );
        assert!(!signals.is_empty());
        assert!(signals[0].contains("Repair request `fix foo` repeated"));
    }

    #[test]
    fn stagnation_detects_repeated_worker_output() {
        let signals = detect_stagnation(
            &[],
            &[],
            &[],
            &[
                "still wiring the fix".to_string(),
                "still wiring the fix".to_string(),
            ],
        );
        assert!(!signals.is_empty());
        assert!(signals[0].contains("Worker output repeated"));
    }

    #[test]
    fn stagnation_normalizes_repair_variations() {
        let signals = detect_stagnation(
            &[],
            &[],
            &[
                "Fix the bug".to_string(),
                "  fix THE BUG  ".to_string(),
                "FIX the  bug".to_string(),
            ],
            &[],
        );
        assert!(!signals.is_empty());
        assert!(signals[0].contains("Repair request `Fix the bug` repeated"));

        let signals = detect_stagnation(
            &[],
            &[],
            &[],
            &[
                "still wiring the fix".to_string(),
                "  still WIRING the  fix  ".to_string(),
            ],
        );
        assert!(!signals.is_empty());
        assert!(signals[0].contains("Worker output repeated"));

        let signals = detect_stagnation(
            &[],
            &[],
            &["Fix the bug".to_string(), "Rewrite the module".to_string()],
            &[],
        );
        assert!(signals.is_empty());
    }

    #[test]
    fn within_scope_limits_when_budget_exceeded() {
        assert!(!within_scope_limits(11, 10));
        assert!(within_scope_limits(8, 10));
    }

    #[test]
    fn evaluate_goal_routes_limited_when_context_unsafe() {
        let scope_check = crate::tools::ScopeCheck {
            changed_file_count: 15,
            ..crate::tools::ScopeCheck::default()
        };
        let budget = BudgetController {
            max_iterations: 5,
            max_files_changed: 10,
            max_provider_unknown_streak: 2,
            ..BudgetController::default()
        };
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            3,
            &budget,
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            None,
            None,
        );
        assert_eq!(evaluation.status, GoalStatus::Limited);
        assert!(evaluation.summary.contains("file change limit"));
    }

    #[test]
    fn evaluate_goal_escalates_on_stagnation_signals() {
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = BudgetController {
            max_iterations: 5,
            max_provider_unknown_streak: 2,
            ..BudgetController::default()
        };
        let signals = vec!["No file changes detected for 2 consecutive iterations.".to_string()];
        let evaluation = evaluate_goal_with_source(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            2,
            &budget,
            &BudgetSnapshot::default(),
            &signals,
            false,
            None,
            None,
            None,
        );
        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert!(evaluation.summary.contains("stagnation"));
        assert_eq!(evaluation.route_hint_override.as_deref(), Some("deep"));
    }

    #[test]
    fn provider_unknown_streak_not_reset_on_false_goal_satisfied() {
        let review_false = CoordinatorReview {
            goal_satisfied: Some(false),
            summary: "Goal not satisfied.".to_string(),
            repair_request: None,
            route_hint: None,
            stop_reason: None,
            raw_response: "GOAL_SATISFIED: no".to_string(),
        };

        // goal_satisfied == Some(false), no stop_reason: streak must NOT reset
        let streak = update_provider_unknown_streak(2, true, Some(&review_false));
        assert_eq!(
            streak, 2,
            "streak should remain unchanged when goal_satisfied is Some(false)"
        );

        // goal_satisfied == Some(true): streak resets to 0
        let review_true = CoordinatorReview {
            goal_satisfied: Some(true),
            stop_reason: Some("complete".to_string()),
            ..review_false.clone()
        };
        let streak = update_provider_unknown_streak(2, true, Some(&review_true));
        assert_eq!(
            streak, 0,
            "streak should reset to 0 when goal_satisfied is Some(true)"
        );

        // stop_reason == limited: streak resets to 0
        let review_limited = CoordinatorReview {
            goal_satisfied: None,
            stop_reason: Some("limited".to_string()),
            ..review_false.clone()
        };
        let streak = update_provider_unknown_streak(2, true, Some(&review_limited));
        assert_eq!(
            streak, 0,
            "streak should reset to 0 when stop_reason is limited"
        );

        // goal_satisfied == None, no stop_reason: streak increments
        let review_unknown = CoordinatorReview {
            goal_satisfied: None,
            stop_reason: None,
            ..review_false.clone()
        };
        let streak = update_provider_unknown_streak(1, true, Some(&review_unknown));
        assert_eq!(
            streak, 2,
            "streak should increment when goal_satisfied is None and no stop_reason"
        );

        // verification_passed == false, goal_satisfied == Some(false): streak unchanged
        let streak = update_provider_unknown_streak(2, false, Some(&review_false));
        assert_eq!(
            streak, 2,
            "streak should remain unchanged when verification not passed"
        );
    }

    // ── GBX-003-001 Root-cause repro tests ──
    // Each test asserts the DESIRED behavior (post-fix) and FAILS with
    // current (pre-fix) code. Each failure points to a clear predicate gap.

    #[test]
    fn test_orchestration_policy_ownership_gate() {
        let scope = ScopeCheck::default();
        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope,
            Some(&CoordinatorReview {
                goal_satisfied: Some(true),
                summary: "ok".to_string(),
                repair_request: None,
                route_hint: None,
                stop_reason: Some("complete".to_string()),
                raw_response: "goal_satisfied: yes\nsummary: ok\nstop_reason: complete".to_string(),
            }),
            0,
            0,
            0,
            &test_budget(5),
            &BudgetSnapshot::default(),
            &[],
            true,
            None,
            None,
            None,
        );
        // GBX-003-002 must add an ExecutionOwnershipDecision check before
        // completion. Without a delegating worker task, Complete must be denied.
        // Currently the gate fires (ownership=None, category=Quick).
        // After GBX-003-002, this assertion passes.
        assert!(
            !matches!(evaluation.status, GoalStatus::Complete),
            "GBX-003-002 FAIL: Goal completed without an ownership decision. \
             Gear must record an ExecutionOwnershipDecision before allowing Complete."
        );
    }

    #[test]
    fn test_orchestration_policy_capability_mismatch() {
        // WorkerCapabilities currently has only session-management fields.
        // Code-level capabilities (code_edit, review, explore) don't exist.
        // Without them, Gear cannot verify worker fitness before dispatch.
        let caps = crate::workers::WorkerCapabilities::command();
        let json = serde_json::to_value(&caps).unwrap();
        // GBX-003-003 must add: supports_code_edit, supports_review,
        // supports_explore to WorkerCapabilities. Currently only
        // session-management fields exist.
        // This assertion FAILS because supports_code_edit doesn't exist.
        assert!(
            json.get("supports_code_edit").is_some(),
            "GBX-003-003 FAIL: WorkerCapabilities missing supports_code_edit field.\n\
             Current caps: {json}"
        );
    }

    #[test]
    fn test_orchestration_policy_synthetic_reviewer_rejected() {
        let scope = ScopeCheck::default();
        // No real task attempts → synthetic evidence fallback in from_inputs()
        let gate = ReviewGate::from_inputs(true, &WorkerStatus::Succeeded, &scope, None, &[], &[]);
        // GBX-003-006: synthetic evidence_only_reason() must return Some
        // when all evidence is synthetic. After GBX-003-006, this assertion
        // PASSES because the synthetic check detects no real artifacts.
        assert!(
            gate.synthetic_evidence_only_reason().is_some(),
            "GBX-003-006 FAIL: ReviewGate passed with synthetic evidence. \
             Completion must require real reviewer artifacts. Summary: {}",
            gate.summary()
        );
        // Verify evalute denies completion with synthetic-only evidence
        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Quick,
            true,
            None,
            None,
            &scope,
            None,
            0,
            0,
            0,
            &test_budget(5),
            &BudgetSnapshot::default(),
            &[],
            true,
            None,
            None,
            None,
        );
        assert!(
            !matches!(evaluation.status, GoalStatus::Complete),
            "GBX-003-006 FAIL: evaluate_goal returned Complete with synthetic-only evidence. \
             The synthetic evidence gate must block completion without real reviewer artifacts."
        );
    }

    #[test]
    fn test_orchestration_policy_lineage_incomplete() -> Result<()> {
        // GBX-003-005: ContinuationState must carry lineage fields so Gear
        // can enforce descendant-aware completion gating.
        let state = crate::state::ContinuationState {
            session_id: "child".to_string(),
            goal_id: "child-goal".to_string(),
            status: crate::state::ContinuationStatus::Running,
            updated_at: "now".to_string(),
            parent_session_id: Some("parent".to_string()),
            root_session_id: Some("parent".to_string()),
        };
        let json = serde_json::to_value(&state)?;
        // GBX-003-005: parent_session_id must exist in serialized state.
        // After GBX-003-005 adds lineage fields, this assertion PASSES.
        assert!(
            json.get("parent_session_id").is_some() || json.get("root_session_id").is_some(),
            "GBX-003-005 FAIL: ContinuationState missing parent_session_id field. \
             Without lineage, Gear cannot prevent parent completion while \
             descendant tasks are active. Serialized: {json}"
        );
        // Verify lineage is correctly stored
        assert_eq!(
            json["parent_session_id"].as_str(),
            Some("parent"),
            "GBX-003-005: parent_session_id should be 'parent'"
        );
        Ok(())
    }

    // ── GBX-003 regression tests ─────────────────────────────────────────

    #[test]
    fn test_ownership_not_enforced_before_execution() {
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = test_budget(DEFAULT_MAX_ITERATIONS);
        let ownership = crate::state::ExecutionOwnership {
            delegated: false,
            worker_kind: Some("zed_agent".to_string()),
            route_reason: "test: ownership not enforced".to_string(),
            risk_profile: "low".to_string(),
            worker_task_id: Some("task_test".to_string()),
            decided_at: crate::state::timestamp(),
        };
        // Ownership gate should reject Complete for Quick with delegated=false
        let evaluation = evaluate_goal_with_source(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            &budget,
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            Some(&ownership),
            None,
        );
        assert!(
            !matches!(evaluation.status, GoalStatus::Complete),
            "Ownership gate should reject completion with delegated=false but got {:?}",
            evaluation.status
        );
    }

    #[test]
    fn test_adapter_not_captured_through_real_registry() -> Result<()> {
        // GBX-003 GAP: WorkerRegistry::start returns a handle but does not
        // store/track the binding between session_id and adapter. Command
        // workers return session_id=None when supports_interaction=false.
        let temp_dir = tempfile::tempdir()?;
        let store = crate::state::StateStore::new(temp_dir.path());
        store.initialize()?;

        let registry = ts::worker_registry_for_test();
        let task = ts::default_task();
        let config = ts::make_worker_config(WorkerKind::Opencode);
        let request = ts::make_worker_start_request(
            &store,
            temp_dir.path(),
            &task,
            "test-goal",
            &config,
        );

        let result = registry.start(request);
        assert!(
            result.is_ok(),
            "GBX-003 GAP: WorkerRegistry::start should succeed but got: {:?}",
            result.err()
        );
        let handle = result?;
        assert!(
            handle.session_id().is_some(),
            "GBX-003 GAP: adapter session_id should be captured by registry"
        );
        Ok(())
    }

    #[test]
    fn test_lineage_not_participating_in_completion() {
        // WorkLineage's active_task_ids must gate completion: when there are
        // active descendant tasks, evaluate_goal_with_source must deny Complete.
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = test_budget(DEFAULT_MAX_ITERATIONS);
        let ownership = crate::state::ExecutionOwnership {
            delegated: true,
            worker_kind: Some("test".to_string()),
            route_reason: "test: lineage gate".to_string(),
            risk_profile: "low".to_string(),
            worker_task_id: Some("task_test".to_string()),
            decided_at: crate::state::timestamp(),
        };
        // Create a lineage with an active task → the lineage gate must fire.
        let mut lineage = WorkLineage::new("test_session".to_string());
        lineage.active_task_ids.push("active_task_001".to_string());

        let evaluation = evaluate_goal_with_source(
            true, &WorkerStatus::Succeeded, WorkerCategory::Deep,
            false, None, None, &scope_check, None,
            0, 0, 1, &budget, &BudgetSnapshot::default(),
            &[], false, None, Some(&ownership),
            Some(&lineage),
        );
        // Lineage has active tasks → must NOT be Complete
        assert!(
            !matches!(evaluation.status, GoalStatus::Complete),
            "Lineage gate should reject completion with active tasks but got {:?}",
            evaluation.status
        );
        // Must be Running (since iteration < max_iterations)
        assert!(
            matches!(evaluation.status, GoalStatus::Running),
            "Lineage gate should return Running with active tasks but got {:?}",
            evaluation.status
        );
        assert!(
            evaluation.summary.contains("Lineage gate"),
            "Summary should mention lineage gate but got: {}",
            evaluation.summary
        );
    }

    #[test]
    fn test_synthetic_review_still_completes_goal() {
        // GBX-003 GAP: ReviewGate::from_inputs accepts synthetic evidence
        // (no real task_attempts) for all dimensions, and evaluate_goal
        // returns Complete as long as the coordinator does not set
        // route_hint=review. Synthetic-only evidence should block completion.
        let scope_check = crate::tools::ScopeCheck::default();
        let budget = test_budget(DEFAULT_MAX_ITERATIONS);
        let ownership = crate::state::ExecutionOwnership {
            delegated: true,
            worker_kind: Some("test".to_string()),
            route_reason: "test: synthetic review".to_string(),
            risk_profile: "low".to_string(),
            worker_task_id: Some("task_synthetic".to_string()),
            decided_at: crate::state::timestamp(),
        };
        let review = CoordinatorReview {
            goal_satisfied: Some(true),
            summary: "All looks good (synthetic)".to_string(),
            repair_request: None,
            route_hint: None,
            stop_reason: Some("complete".to_string()),
            raw_response: "GOAL_SATISFIED: yes\nSTOP_REASON: complete".to_string(),
        };
        let evaluation = evaluate_goal_with_source(
            true, &WorkerStatus::Succeeded, WorkerCategory::Deep,
            false, None, None, &scope_check, Some(&review),
            0, 0, 1, &budget, &BudgetSnapshot::default(),
            &[], false, None, Some(&ownership),
            None,
        );
        assert!(
            !matches!(evaluation.status, GoalStatus::Complete),
            "GBX-003 GAP: synthetic review should not allow completion but got {:?}",
            evaluation.status
        );
    }
}
