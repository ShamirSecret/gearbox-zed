use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context as _, Result, bail};
use sha2::{Digest as _, Sha256};

use crate::phase_routing::{
    LiveModelInventory, OpenCodeModelProfiles, PhaseBackend, PhaseModelBinding,
    PhaseRouteDecision, PhaseRouteTable, RejectedPhaseCandidate, opencode_paid_fallback_model,
};
#[cfg(test)]
use crate::plan_graph::PhaseProfile;
use crate::plan_graph::{
    PLAN_GRAPH_SCHEMA_EXEMPLAR, PlannerParseDiagnostic, TaskRiskTier, TaskSizeTier,
    deterministic_fallback_draft, parse_planner_draft_diagnostic, parse_planner_draft_with_objective,
    validate_planner_draft,
};
use crate::plan_review::{
    IntentFoldDecision, IntentFoldVerdict, IntentRisk, IntentRiskSeverity, PhaseExecutionIdentity,
    PlanCriticCheck, PlanCriticCheckVerdict, PlanCriticDecision, PlanCriticDimension,
    PlanCriticFinding, PlanCriticFindingSeverity, PlanCriticVerdict,
};
use crate::runtime::{
    IntentFoldInput, IntentFoldSubmission, PhaseRuntime, PlanCriticInput, PlanCriticSubmission,
    PlanRevisionInput, PlanRevisionSubmission, PlannerInput, PlannerSubmission,
    RepositoryDiscoverySubmission, StrategistNextGoalDecision, StrategistNextGoalInput,
    StrategistNextGoalSubmission, StrategistNextGoalVerdict,
};
use crate::state::{
    GoalStatus, ModelCallKind, ModelCallLedgerEntry, RepositoryObservationEvent,
    RepositoryObservationReceipt, Scope, StateStore, Task, TaskInputs, TaskOutputs, TaskStatus,
    compute_per_file_attribution, fingerprint_paths, id_timestamp, timestamp,
};
use crate::task_manager::{
    ManagedTaskStatus, ResidencyState, TaskAttempt, TaskAttemptStatus, TaskRecord,
};
use crate::tools::{CancellationToken, git_head_commit};
use crate::worker_broker::PhaseBrokerFactory;
use crate::workers::{
    WorkerConfig, WorkerKind, WorkerResult, WorkerStartRequest, WorkerStatus,
    worker_evidence_marker_path,
};

/// Builder for a production `PhaseRuntime` that routes all planning and review
/// phases through independent OpenCode session workers.
///
/// Each planning phase (RepositoryDiscovery, IntentFold, Planner, PlanCritic,
/// PlanRevision, Strategist)
/// receives its own `execution_id`, `session_id`, and `task_id`.  Phases
/// never share an actual worker session.
pub struct OpenCodePhaseRuntimeFactory {
    workspace: PathBuf,
    worker_config: WorkerConfig,
    broker_factory: Arc<PhaseBrokerFactory>,
    cancellation_token: CancellationToken,
    phase_route_table: PhaseRouteTable,
    inventory: LiveModelInventory,
    call_budget: PhaseCallBudget,
}

#[derive(Clone, Default)]
struct PhaseCallBudget {
    calls_by_goal: Arc<Mutex<HashMap<String, usize>>>,
}

impl PhaseCallBudget {
    fn reserve(&self, goal_id: &str) -> Result<usize> {
        let max_calls = std::env::var("GEARBOX_MAX_CALLS_PER_EPOCH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(64);
        let mut calls = self
            .calls_by_goal
            .lock()
            .map_err(|_| anyhow::anyhow!("phase call budget mutex poisoned"))?;
        let entry = calls.entry(goal_id.to_string()).or_default();
        if *entry >= max_calls {
            bail!(
                "phase model call budget exhausted for goal `{goal_id}` at {} calls",
                max_calls
            );
        }
        *entry = entry.saturating_add(1);
        Ok(*entry)
    }
}

impl OpenCodePhaseRuntimeFactory {
    pub fn new(
        workspace: PathBuf,
        worker_config: WorkerConfig,
        broker_factory: Arc<PhaseBrokerFactory>,
        cancellation_token: CancellationToken,
        phase_route_table: PhaseRouteTable,
        inventory: LiveModelInventory,
    ) -> Self {
        Self {
            workspace,
            worker_config,
            broker_factory,
            cancellation_token,
            phase_route_table,
            inventory,
            call_budget: PhaseCallBudget::default(),
        }
    }

    /// Build a complete `PhaseRuntime` with all OpenCode phase hooks wired.
    pub fn build(self) -> Result<PhaseRuntime> {
        let workspace = self.workspace.clone();
        let worker_config = self.worker_config.clone();
        let broker_factory = self.broker_factory.clone();
        let cancellation_token = self.cancellation_token.clone();

        let intent_fold_runner = OpenCodePhaseRunner {
            workspace: workspace.clone(),
            worker_config: worker_config.clone(),
            broker_factory: broker_factory.clone(),
            cancellation_token: cancellation_token.clone(),
            call_budget: self.call_budget.clone(),
        };
        let planner_runner = OpenCodePhaseRunner {
            workspace: workspace.clone(),
            worker_config: worker_config.clone(),
            broker_factory: broker_factory.clone(),
            cancellation_token: cancellation_token.clone(),
            call_budget: self.call_budget.clone(),
        };
        let critic_runner = OpenCodePhaseRunner {
            workspace: workspace.clone(),
            worker_config: worker_config.clone(),
            broker_factory: broker_factory.clone(),
            cancellation_token: cancellation_token.clone(),
            call_budget: self.call_budget.clone(),
        };
        let oracle_runner = OpenCodePhaseRunner {
            workspace: workspace.clone(),
            worker_config: worker_config.clone(),
            broker_factory: broker_factory.clone(),
            cancellation_token: cancellation_token.clone(),
            call_budget: self.call_budget.clone(),
        };
        let revision_runner = OpenCodePhaseRunner {
            workspace: workspace.clone(),
            worker_config: worker_config.clone(),
            broker_factory: broker_factory.clone(),
            cancellation_token: cancellation_token.clone(),
            call_budget: self.call_budget.clone(),
        };
        let strategist_runner = OpenCodePhaseRunner {
            workspace,
            worker_config,
            broker_factory: broker_factory.clone(),
            cancellation_token,
            call_budget: self.call_budget,
        };

        Ok(PhaseRuntime {
            routes: self.phase_route_table,
            inventory: self.inventory,
            current_model: None,
            planner: None,
            intent_fold_hook: Some(Arc::new(move |input| intent_fold_runner.fold_intent(input))),
            planner_hook: Some(Arc::new(move |input| planner_runner.plan(input))),
            plan_critic_hook: Some(Arc::new(move |input| critic_runner.critique(input))),
            oracle_hook: Some(Arc::new(move |input| oracle_runner.oracle(input))),
            plan_revision_hook: Some(Arc::new(move |input| revision_runner.revise(input))),
            strategist_next_goal_hook: Some(Arc::new(move |input| {
                strategist_runner.strategize(input)
            })),
            require_plan_approval: true,
            max_plan_revisions: crate::runtime::DEFAULT_MAX_PLAN_REVISIONS,
            broker: None,
            broker_factory: Some(broker_factory),
            direct_model_usage_provider: None,
        })
    }
}

/// Core runner that dispatches a single OpenCode phase through the broker
/// factory and returns the parsed submission.
#[derive(Clone)]
pub struct OpenCodePhaseRunner {
    pub broker_factory: Arc<PhaseBrokerFactory>,
    pub workspace: PathBuf,
    pub worker_config: WorkerConfig,
    pub cancellation_token: CancellationToken,
    call_budget: PhaseCallBudget,
}

const MAX_PLANNER_SCHEMA_REPAIRS: usize = 2;
const MAX_INTENT_REPAIRS: usize = 1;
const MAX_REVIEW_SCHEMA_REPAIRS: usize = 2;
const MAX_REVISION_SCHEMA_REPAIRS: usize = 2;
const MAX_STRATEGIST_SCHEMA_REPAIRS: usize = 2;

struct OpenCodePhaseOutput {
    raw_output: String,
    execution_identity: PhaseExecutionIdentity,
    artifact_path: String,
    repository_observation_path: Option<String>,
}

fn read_phase_output(result: &WorkerResult) -> Result<(String, Option<String>)> {
    let mut output_paths = Vec::new();
    if let Some(path) = result
        .last_message_path
        .as_ref()
        .filter(|path| path.is_file())
    {
        output_paths.push(path.clone());
    }
    if let Some(path) = result.stdout_path.as_ref().filter(|path| path.is_file()) {
        output_paths.push(path.clone());
    }
    if let Some(parent) = result.result_path.parent() {
        for artifact in ["transcript.jsonl", "partial-output.md"] {
            let path = parent.join(artifact);
            if path.is_file() {
                output_paths.push(path);
            }
        }
    }
    for path in output_paths {
        let raw_output = std::fs::read_to_string(&path)?;
        let extracted = extract_worker_text_events(&raw_output);
        if !extracted.trim().is_empty() {
            return Ok((
                extracted.trim().to_string(),
                Some(path.to_string_lossy().to_string()),
            ));
        }
    }
    let summary = result.summary.trim();
    if summary.is_empty() {
        bail!("OpenCode phase returned an empty response");
    }
    Ok((summary.to_string(), None))
}

/// OpenCode's JSON formatter emits one event per line and puts the model's
/// response in a nested `part.text` field.  Phase parsers consume the model
/// object, not the transport envelope, so unwrap those events before schema
/// validation.  Plain model JSON remains unchanged.
fn extract_worker_text_events(raw_output: &str) -> String {
    let mut text = String::new();
    let mut found_event = false;
    for line in raw_output.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        append_worker_text_event(&value, &mut text, &mut found_event, 0);
    }
    if found_event {
        text.trim().to_string()
    } else {
        raw_output.to_string()
    }
}

fn append_worker_text_event(
    value: &serde_json::Value,
    text: &mut String,
    found_event: &mut bool,
    depth: usize,
) {
    if depth > 4 {
        return;
    }
    if let Some(part_text) = value
        .get("part")
        .and_then(|part| part.get("text"))
        .and_then(serde_json::Value::as_str)
    {
        *found_event = true;
        text.push_str(part_text);
    }
    for (container, key) in [
        ("worker_stdout", "output"),
        ("assistant_text_delta", "delta"),
    ] {
        if let Some(nested) = value
            .get(container)
            .and_then(|event| event.get(key))
            .and_then(serde_json::Value::as_str)
        {
            if let Ok(nested_value) = serde_json::from_str::<serde_json::Value>(nested) {
                append_worker_text_event(&nested_value, text, found_event, depth + 1);
            }
        }
    }
}

impl OpenCodePhaseRunner {
    pub fn new(
        broker_factory: Arc<PhaseBrokerFactory>,
        workspace: PathBuf,
        worker_config: WorkerConfig,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            broker_factory,
            workspace,
            worker_config,
            cancellation_token,
            call_budget: PhaseCallBudget::default(),
        }
    }

    fn run(
        &self,
        decision: &PhaseRouteDecision,
        goal_id: &str,
        plan_id: &str,
        plan_revision: usize,
        plan_hash: Option<&str>,
        task_id: &str,
        task_kind: crate::state::TaskKind,
        scope: Scope,
        prompt: String,
    ) -> Result<OpenCodePhaseOutput> {
        self.run_with_follow_up(
            decision,
            goal_id,
            plan_id,
            plan_revision,
            plan_hash,
            task_id,
            task_kind,
            scope,
            prompt,
            |_result, _follow_up_index| Ok(None),
        )
    }

    fn run_with_follow_up<F>(
        &self,
        decision: &PhaseRouteDecision,
        goal_id: &str,
        plan_id: &str,
        plan_revision: usize,
        plan_hash: Option<&str>,
        task_id: &str,
        task_kind: crate::state::TaskKind,
        scope: Scope,
        prompt: String,
        mut follow_up: F,
    ) -> Result<OpenCodePhaseOutput>
    where
        F: FnMut(&WorkerResult, usize) -> Result<Option<String>>,
    {
        if let PhaseModelBinding::BackendDeclared(model) = &decision.candidate.model
            && crate::workers::is_free_model(Some(model))
            && StateStore::new(&self.workspace)
                .read_global_provider_cooldown()?
                .is_some_and(|cooldown| cooldown.is_active())
        {
            let cooldown_error = anyhow::anyhow!(
                "free provider quota cooldown is active; paid phase route required"
            );
            let Some(fallback_decision) = paid_fallback_decision(decision, &cooldown_error)?
            else {
                bail!(
                    "free provider quota cooldown is active and phase {:?} has no paid route",
                    decision.phase
                );
            };
            let store = StateStore::new(&self.workspace);
            store.initialize()?;
            let fallback_task_id = format!("{task_id}_paid");
            store.write_artifact(
                goal_id,
                &format!("phase-provider-fallback-{task_id}.json"),
                &format!(
                    "{}\n",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "schema_version": 1,
                        "status": "cooldown_skipped_free_route",
                        "phase": format!("{:?}", decision.phase),
                        "task_id": task_id,
                        "fallback_task_id": fallback_task_id,
                        "original_model": intent_fold_model_label(decision),
                        "fallback_model": intent_fold_model_label(&fallback_decision),
                        "original_failure": cooldown_error.to_string(),
                        "fallback_decision": fallback_decision,
                        "next_action": "treat paid transport as provisional until semantic and repository evidence gates pass"
                    }))?
                ),
            )?;
            return self.run_with_follow_up_once(
                &fallback_decision,
                goal_id,
                plan_id,
                plan_revision,
                plan_hash,
                &fallback_task_id,
                task_kind,
                scope,
                prompt,
                &mut follow_up,
            );
        }
        let first_attempt = self.run_with_follow_up_once(
            decision,
            goal_id,
            plan_id,
            plan_revision,
            plan_hash,
            task_id,
            task_kind.clone(),
            scope.clone(),
            prompt.clone(),
            &mut follow_up,
        );
        let error = match first_attempt {
            Ok(output) => return Ok(output),
            Err(error) if is_provider_recoverable_phase_error(&error) => error,
            Err(error) => return Err(error),
        };
        let Some(fallback_decision) = paid_fallback_decision(decision, &error)? else {
            return Err(error);
        };
        let store = StateStore::new(&self.workspace);
        store.initialize()?;
        let fallback_task_id = format!("{task_id}_paid");
        crate::workers::discard_resident_session_for_model_switch(
            &store,
            &self.workspace,
            task_id,
            WorkerKind::OpencodeSession,
            match &fallback_decision.candidate.model {
                PhaseModelBinding::BackendDeclared(model) => Some(model.as_str()),
                _ => None,
            },
        )?;
        store.write_artifact(
            goal_id,
            &format!("phase-provider-fallback-{task_id}.json"),
            &format!(
                "{}\n",
                serde_json::to_string_pretty(&serde_json::json!({
                    "schema_version": 1,
                    "status": "fallback_dispatched",
                    "phase": format!("{:?}", decision.phase),
                    "task_id": task_id,
                    "fallback_task_id": fallback_task_id,
                    "original_model": intent_fold_model_label(decision),
                    "fallback_model": intent_fold_model_label(&fallback_decision),
                    "original_failure": error.to_string(),
                    "fallback_decision": fallback_decision,
                    "next_action": "treat fallback transport as provisional until semantic and repository evidence gates pass"
                }))?
            ),
        )?;
        self.run_with_follow_up_once(
            &fallback_decision,
            goal_id,
            plan_id,
            plan_revision,
            plan_hash,
            &fallback_task_id,
            task_kind,
            scope,
            prompt,
            &mut follow_up,
        )
    }

    fn run_with_follow_up_once<F>(
        &self,
        decision: &PhaseRouteDecision,
        goal_id: &str,
        plan_id: &str,
        plan_revision: usize,
        plan_hash: Option<&str>,
        task_id: &str,
        task_kind: crate::state::TaskKind,
        scope: Scope,
        prompt: String,
        mut follow_up: F,
    ) -> Result<OpenCodePhaseOutput>
    where
        F: FnMut(&WorkerResult, usize) -> Result<Option<String>>,
    {
        let call_ordinal = self.call_budget.reserve(goal_id)?;
        if !matches!(
            decision.candidate.backend,
            PhaseBackend::Worker(WorkerKind::OpencodeSession) | PhaseBackend::CodexAcp
        ) {
            bail!("Gear phase runner received a non-OpenCode/Codex route");
        }
        let config = decision.overlay_worker_config(&self.worker_config)?;
        // Every OpenCode phase dispatched here is a planning/review role. Keep
        // the route decision's model binding, but use the Explore policy so
        // the worker cannot write while gathering context, folding intent,
        // revising a plan, or producing an independent verdict. Writable
        // Executor workers are dispatched by the task runtime, not this phase
        // runner.
        let phase_route_hint = Some(phase_worker_route_hint(
            &task_kind,
            decision.category.as_str(),
        ));
        let store = StateStore::new(&self.workspace);
        store.initialize()?;
        let read_only_phase = matches!(
            &task_kind,
            crate::state::TaskKind::Spec
                | crate::state::TaskKind::Plan
                | crate::state::TaskKind::Review
        );
        let task = Task {
            id: task_id.to_string(),
            goal_id: goal_id.to_string(),
            parent_task_id: None,
            title: format!("Gear {:?} phase", decision.phase),
            kind: task_kind,
            status: TaskStatus::Pending,
            assigned_worker: Some(WorkerKind::OpencodeSession.as_str().to_string()),
            attempt: 1,
            scope,
            inputs: TaskInputs {
                phase_route_locked: true,
                ..TaskInputs::default()
            },
            outputs: TaskOutputs::default(),
        };
        let suffix = id_timestamp();
        let baseline_snapshot = crate::tools::git_snapshot(&self.workspace)?;
        let baseline_fingerprints = fingerprint_paths(
            &self.workspace,
            &baseline_snapshot.changed_files,
        );
        let execution_result = self.broker_factory.execute_worker_phase_with_follow_up(
            decision,
            goal_id,
            plan_id,
            plan_revision,
            task_id,
            &format!("{:?}_execution_{suffix}", decision.phase).to_ascii_lowercase(),
            &format!("{:?}_session_{suffix}", decision.phase).to_ascii_lowercase(),
            WorkerStartRequest {
                store: &store,
                workspace: &self.workspace,
                task: &task,
                route_attempt: 1,
                goal: &prompt,
                verification_commands: &[],
                config: &config,
                cancellation_token: Some(self.cancellation_token.clone()),
                coordinator_model: None,
                coordinator_brief: None,
                route_hint: phase_route_hint,
            },
            |result, follow_up_index| follow_up(result, follow_up_index),
        );
        let after_snapshot = crate::tools::git_snapshot(&self.workspace)?;
        let mut observed_paths = baseline_snapshot.changed_files.clone();
        observed_paths.extend(after_snapshot.changed_files.iter().cloned());
        observed_paths.sort();
        observed_paths.dedup();
        let after_fingerprints = fingerprint_paths(&self.workspace, &observed_paths);
        let attribution = compute_per_file_attribution(
            &baseline_fingerprints,
            &after_fingerprints,
            &format!("phase:{task_id}"),
            1,
        );
        let read_only_mutations = attribution
            .added
            .iter()
            .chain(attribution.modified.iter())
            .chain(attribution.removed.iter())
            .filter(|change| !change.fingerprint.path.starts_with(".gear/"))
            .map(|change| change.fingerprint.path.clone())
            .collect::<Vec<_>>();
        if read_only_phase && !read_only_mutations.is_empty() {
            let mutation_artifact = serde_json::json!({
                "schema_version": 1,
                "status": "blocked",
                "kind": "phase_read_only_mutation",
                "phase": format!("{:?}", decision.phase),
                "task_id": task_id,
                "mutated_paths": read_only_mutations,
                "before": baseline_snapshot,
                "after": after_snapshot,
                "attribution": attribution,
                "next_action": "preserve_workspace_and_start_bounded_repair",
            });
            store.write_artifact(
                goal_id,
                &format!("phase-read-only-mutation-{task_id}.json"),
                &format!("{}\n", serde_json::to_string_pretty(&mutation_artifact)?),
            )?;
            bail!(
                "OpenCode {:?} phase mutated the workspace in read-only role; paths: {}",
                decision.phase,
                read_only_mutations.join(", ")
            );
        }
        let execution = execution_result?;
        if execution.result.status != WorkerStatus::Succeeded {
            bail!(
                "OpenCode {:?} phase failed: {}",
                decision.phase,
                execution.result.summary
            );
        }
        write_phase_task_record(&store, &task, &config, phase_route_hint, &execution)?;
        let call_entry = write_model_call_ledger_entry(
            &store,
            &self.workspace,
            decision,
            goal_id,
            plan_id,
            plan_revision,
            task_id,
            call_ordinal,
            &config,
            &execution,
        )?;
        let capture_commit = git_head_commit(&self.workspace)?;
        let observation = RepositoryObservationReceipt::seal_with_capture_commit(
            &format!(
                "{}-{}",
                phase_role_name_for_task(&decision.phase, task_id),
                task_id.replace(':', "_")
            ),
            goal_id,
            plan_id,
            plan_revision,
            plan_hash.unwrap_or("pending"),
            task_id,
            &execution.session_identity.session_id,
            call_entry.transcript_sha256.clone(),
            call_entry.observed_tool_count,
            call_entry.observed_paths.clone(),
            call_entry.observation_events.clone(),
            capture_commit,
        )?;
        let observation_path = store.write_repository_observation_receipt(&observation)?;
        let (raw_output, _) = read_phase_output(&execution.result).with_context(|| {
            format!(
                "OpenCode {:?} phase returned an empty response",
                decision.phase
            )
        })?;
        let artifact_path = worker_evidence_marker_path(&execution.result)
            .unwrap_or_else(|| execution.result.result_path.to_string_lossy().to_string());
        Ok(OpenCodePhaseOutput {
            raw_output,
            execution_identity: execution.execution_identity,
            artifact_path,
            repository_observation_path: Some(observation_path.to_string_lossy().to_string()),
        })
    }

    fn discover_repository(
        &self,
        input: &IntentFoldInput,
    ) -> Result<RepositoryDiscoverySubmission> {
        let task_id = format!("repository_discovery_{}", input.goal_id);
        let output = self.run(
            &input.route_decision,
            &input.goal_id,
            &format!("pending_{}", input.goal_id),
            0,
            None,
            &task_id,
            crate::state::TaskKind::Spec,
            input.scope.clone(),
            gear_opencode_repository_discovery_prompt(input)?,
        )?;
        let store = StateStore::new(&self.workspace);
        store.initialize()?;
        let capture_commit = git_head_commit(&self.workspace)?;
        let artifact = serde_json::json!({
            "schema_version": 1,
            "phase": "repository_discovery",
            "goal_id": input.goal_id,
            "task_id": task_id,
            "execution_identity": output.execution_identity,
            "raw_output_sha256": sha256_hex(&output.raw_output),
            "raw_output": output.raw_output,
            "worker_artifact_path": output.artifact_path,
            "repository_observation_path": output.repository_observation_path,
            "capture_commit": capture_commit,
            "issued_at": timestamp(),
        });
        let artifact_path = store.write_artifact(
            &input.goal_id,
            "repository-discovery.json",
            &format!("{}\n", serde_json::to_string_pretty(&artifact)?),
        )?;
        let raw_output = artifact["raw_output"]
            .as_str()
            .context("repository discovery artifact lost raw output")?
            .to_string();
        let analyst = serde_json::from_value(artifact["execution_identity"].clone())?;
        Ok(RepositoryDiscoverySubmission {
            raw_output,
            analyst,
            artifact_path: artifact_path.to_string_lossy().to_string(),
            repository_evidence_path: artifact["repository_observation_path"]
                .as_str()
                .map(ToOwned::to_owned),
        })
    }

    pub fn fold_intent(&self, input: IntentFoldInput) -> Result<IntentFoldSubmission> {
        let repository_discovery = self
            .discover_repository(&input)
            .context("repository discovery failed before IntentFold")?;
        let prompt = gear_opencode_intent_fold_prompt(&input, &repository_discovery)?;
        let task_id = format!("intent_fold_{}", input.goal_id);
        let model = intent_fold_model_label(&input.route_decision);
        let diagnostic_store = StateStore::new(&self.workspace);
        diagnostic_store.initialize()?;
        let mut verdict = None;
        let mut parse_failures = 0;
        let mut last_parse_failure: Option<(String, String, Option<String>)> = None;
        let output = self.run_with_follow_up(
            &input.route_decision,
            &input.goal_id,
            &format!("pending_{}", input.goal_id),
            0,
            None,
            &task_id,
            crate::state::TaskKind::Spec,
            input.scope.clone(),
            prompt,
            |result, follow_up_index| {
                let (raw_output, raw_output_path) = read_phase_output(result)?;
                match IntentFoldVerdict::parse(&raw_output) {
                    Ok(parsed_verdict) => {
                        let requires_repair = parsed_verdict.decision
                            == crate::plan_review::IntentFoldDecision::NeedsUser
                            || !parsed_verdict.required_questions.is_empty();
                        if requires_repair && follow_up_index < MAX_INTENT_REPAIRS {
                            self.call_budget.reserve(&input.goal_id)?;
                            let repair_prompt = gear_opencode_intent_repair_prompt(
                                &input,
                                &raw_output,
                                follow_up_index + 1,
                            )?;
                            return Ok(Some(repair_prompt));
                        }
                        if let Some((parse_error, failed_output, failed_path)) =
                            last_parse_failure.take()
                        {
                            write_intent_fold_recovery_artifact(
                                &diagnostic_store,
                                &input,
                                &task_id,
                                &model,
                                parse_failures,
                                &parse_error,
                                &failed_output,
                                failed_path.as_deref(),
                                "recovered",
                            )?;
                        }
                        verdict = Some(parsed_verdict);
                        Ok(None)
                    }
                    Err(error) => {
                        let parse_error = error.to_string();
                        parse_failures = parse_failures.saturating_add(1);
                        if follow_up_index >= MAX_INTENT_REPAIRS {
                            write_intent_fold_recovery_artifact(
                                &diagnostic_store,
                                &input,
                                &task_id,
                                &model,
                                parse_failures,
                                &parse_error,
                                &raw_output,
                                raw_output_path.as_deref(),
                                "degraded_ready",
                            )?;
                            // IntentFold is a model convenience layer. Keep
                            // the original request and scope as the durable
                            // facts when the model cannot serialize its
                            // analysis; planning and independent review still
                            // enforce the hard evidence gates downstream.
                            verdict = Some(degraded_intent_fold_verdict(&input, &parse_error));
                            return Ok(None);
                        }
                        write_intent_fold_recovery_artifact(
                            &diagnostic_store,
                            &input,
                            &task_id,
                            &model,
                            parse_failures,
                            &parse_error,
                            &raw_output,
                            raw_output_path.as_deref(),
                            "retrying",
                        )?;
                        last_parse_failure =
                            Some((parse_error.clone(), raw_output, raw_output_path));
                        self.call_budget.reserve(&input.goal_id)?;
                        let repair_prompt = gear_opencode_intent_parse_repair_prompt(
                            &input,
                            &last_parse_failure
                                .as_ref()
                                .map(|(_, output, _)| output.as_str())
                                .unwrap_or_default(),
                            follow_up_index + 1,
                            &parse_error,
                        )?;
                        Ok(Some(repair_prompt))
                    }
                }
            },
        )?;
        let verdict = verdict.context("intent fold recovery completed without a verdict")?;
        let raw_output = if IntentFoldVerdict::parse(&output.raw_output).is_ok() {
            output.raw_output.clone()
        } else {
            // Keep the receipt self-consistent when the typed fallback was
            // selected after schema repair exhaustion. The original malformed
            // response remains in the recovery artifact and worker transcript.
            serde_json::to_string(&verdict)?
        };
        Ok(IntentFoldSubmission {
            verdict,
            analyst: output.execution_identity,
            raw_output,
            artifact_path: Some(output.artifact_path),
            repository_evidence_path: output.repository_observation_path,
            repository_discovery: Some(repository_discovery),
        })
    }

    pub fn plan(&self, input: PlannerInput) -> Result<PlannerSubmission> {
        let prompt = gear_opencode_planner_prompt(&input)?;
        let mut output = self.run(
            &input.route_decision,
            &input.goal_id,
            &format!("pending_{}", input.goal_id),
            0,
            None,
            &format!("planner_{}", input.goal_id),
            crate::state::TaskKind::Plan,
            input.scope.clone(),
            prompt,
        )?;
        let mut previous_raw_sha = None;
        let mut semantic_repair_attempts = 0;
        for repair_attempt in 0..=MAX_PLANNER_SCHEMA_REPAIRS {
            match parse_planner_draft_diagnostic(&output.raw_output) {
                Ok(draft) => {
                    if let Err(error) = validate_planner_draft(&input.goal_id, &draft) {
                        semantic_repair_attempts += 1;
                        if repair_attempt >= MAX_PLANNER_SCHEMA_REPAIRS {
                            return self.degraded_planner_submission(
                                &input,
                                &output,
                                &format!(
                                    "planner contract repair exhausted after {} attempts: {}",
                                    MAX_PLANNER_SCHEMA_REPAIRS, error
                                ),
                            );
                        }
                        let diagnostic = PlannerParseDiagnostic {
                            raw_sha256: sha256_hex(&output.raw_output),
                            json_path: "$".to_string(),
                            expected: "a semantically valid PlanGraphDraft contract".to_string(),
                            actual: error.to_string(),
                            message: error.to_string(),
                            line: 0,
                            column: 0,
                        };
                        let store = StateStore::new(&self.workspace);
                        store.initialize()?;
                        store.write_artifact(
                            &input.goal_id,
                            &format!("planner-schema-diagnostic-r{}.json", repair_attempt + 1),
                            &format!("{}\n", serde_json::to_string_pretty(&diagnostic)?),
                        )?;
                        if previous_raw_sha.as_deref() == Some(diagnostic.raw_sha256.as_str()) {
                            return self.degraded_planner_submission(
                                &input,
                                &output,
                                &format!(
                                    "planner repeated the same semantically invalid output: {}",
                                    serde_json::to_string(&diagnostic)?
                                ),
                            );
                        }
                        if semantic_repair_attempts > 1 {
                            return self.degraded_planner_submission(
                                &input,
                                &output,
                                &format!(
                                    "planner semantic contract remained invalid after {} attempts: {}",
                                    semantic_repair_attempts, error
                                ),
                            );
                        }
                        previous_raw_sha = Some(diagnostic.raw_sha256.clone());
                        let repair_prompt = gear_opencode_planner_repair_prompt(
                            &input,
                            &output.raw_output,
                            &diagnostic,
                            repair_attempt + 1,
                        )?;
                        output = self.run(
                            &input.route_decision,
                            &input.goal_id,
                            &format!("pending_{}", input.goal_id),
                            0,
                            None,
                            &format!("planner_{}_repair_{}", input.goal_id, repair_attempt + 1),
                            crate::state::TaskKind::Plan,
                            input.scope.clone(),
                            repair_prompt,
                        )?;
                        continue;
                    }
                    return Ok(PlannerSubmission {
                        draft,
                        planner: output.execution_identity,
                        raw_output: output.raw_output,
                        artifact_path: Some(output.artifact_path),
                        repository_evidence_path: output.repository_observation_path,
                    });
                }
                Err(diagnostic) if repair_attempt < MAX_PLANNER_SCHEMA_REPAIRS => {
                    if let Some(submission) =
                        self.load_verified_planner_artifact(&input, &output, &diagnostic)?
                    {
                        return Ok(submission);
                    }
                    if diagnostic.message.contains("missing field `objective`") {
                        if let Ok(draft) =
                            parse_planner_draft_with_objective(&output.raw_output, &input.request)
                        {
                            validate_planner_draft(&input.goal_id, &draft)?;
                            return Ok(PlannerSubmission {
                                draft,
                                planner: output.execution_identity,
                                raw_output: output.raw_output,
                                artifact_path: Some(output.artifact_path),
                                repository_evidence_path: output.repository_observation_path,
                            });
                        }
                    }
                    let store = StateStore::new(&self.workspace);
                    store.initialize()?;
                    store.write_artifact(
                        &input.goal_id,
                        &format!("planner-schema-diagnostic-r{}.json", repair_attempt + 1),
                        &format!("{}\n", serde_json::to_string_pretty(&diagnostic)?),
                    )?;
                    if previous_raw_sha.as_deref() == Some(diagnostic.raw_sha256.as_str()) {
                        return self.degraded_planner_submission(
                            &input,
                            &output,
                            &format!(
                                "planner repeated the same malformed output; schema diagnostic: {}",
                                serde_json::to_string(&diagnostic)?
                            ),
                        );
                    }
                    previous_raw_sha = Some(diagnostic.raw_sha256.clone());
                    let repair_prompt = gear_opencode_planner_repair_prompt(
                        &input,
                        &output.raw_output,
                        &diagnostic,
                        repair_attempt + 1,
                    )?;
                    output = self.run(
                        &input.route_decision,
                        &input.goal_id,
                        &format!("pending_{}", input.goal_id),
                        0,
                        None,
                        &format!("planner_{}_repair_{}", input.goal_id, repair_attempt + 1),
                        crate::state::TaskKind::Plan,
                        input.scope.clone(),
                        repair_prompt,
                    )?;
                }
                Err(diagnostic) => {
                    if let Some(submission) =
                        self.load_verified_planner_artifact(&input, &output, &diagnostic)?
                    {
                        return Ok(submission);
                    }
                    if diagnostic.message.contains("missing field `objective`") {
                        if let Ok(draft) =
                            parse_planner_draft_with_objective(&output.raw_output, &input.request)
                        {
                            validate_planner_draft(&input.goal_id, &draft)?;
                            return Ok(PlannerSubmission {
                                draft,
                                planner: output.execution_identity,
                                raw_output: output.raw_output,
                                artifact_path: Some(output.artifact_path),
                                repository_evidence_path: output.repository_observation_path,
                            });
                        }
                    }
                    return self.degraded_planner_submission(
                        &input,
                        &output,
                        &format!(
                            "planner schema repair exhausted after {} attempts: {}",
                            MAX_PLANNER_SCHEMA_REPAIRS,
                            serde_json::to_string(&diagnostic)?
                        ),
                    );
                }
            }
        }
        bail!("planner schema repair loop terminated unexpectedly")
    }

    fn load_verified_planner_artifact(
        &self,
        input: &PlannerInput,
        output: &OpenCodePhaseOutput,
        diagnostic: &PlannerParseDiagnostic,
    ) -> Result<Option<PlannerSubmission>> {
        let conventional_artifact_path = self
            .workspace
            .join(".gear")
            .join("evidence")
            .join(format!("planner_{}", input.goal_id))
            .join("plan-graph-draft.json");
        let reported_artifact_path = PathBuf::from(&output.artifact_path);
        let reported_artifact_path = if reported_artifact_path.is_absolute() {
            reported_artifact_path
        } else {
            self.workspace.join(reported_artifact_path)
        };
        let mut candidates = vec![reported_artifact_path, conventional_artifact_path];
        let worker_prefix = format!("planner_{}", input.goal_id);
        if let Ok(entries) = std::fs::read_dir(self.workspace.join(".gear").join("workers")) {
            candidates.extend(entries.flatten().filter_map(|entry| {
                let file_type = entry.file_type().ok()?;
                if !file_type.is_dir()
                    || !entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&worker_prefix)
                {
                    return None;
                }
                Some(entry.path().join("plan-graph-draft.json"))
            }));
        }
        candidates.sort_by_key(|path| {
            std::fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .ok()
        });
        candidates.reverse();
        let Some((artifact_path, contents, draft)) = candidates.into_iter().find_map(|path| {
            let contents = std::fs::read_to_string(&path).ok()?;
            let draft = parse_planner_draft_with_objective(&contents, &input.request).ok()?;
            validate_planner_draft(&input.goal_id, &draft).ok()?;
            if planner_request_requires_multiple_nodes(&input.request) && draft.tasks.len() < 2 {
                return None;
            }
            Some((path, contents, draft))
        }) else {
            return Ok(None);
        };
        let store = StateStore::new(&self.workspace);
        store.initialize()?;
        store.write_artifact(
            &input.goal_id,
            &format!(
                "planner-artifact-recovery-r{}.json",
                MAX_PLANNER_SCHEMA_REPAIRS
            ),
            &format!(
                "{{\"source\":{},\"diagnostic\":{}}}\n",
                serde_json::to_string(&artifact_path.to_string_lossy())?,
                serde_json::to_string(diagnostic)?
            ),
        )?;
        Ok(Some(PlannerSubmission {
            draft,
            planner: output.execution_identity.clone(),
            raw_output: contents,
            artifact_path: Some(artifact_path.to_string_lossy().to_string()),
            repository_evidence_path: output.repository_observation_path.clone(),
        }))
    }

    fn degraded_planner_submission(
        &self,
        input: &PlannerInput,
        output: &OpenCodePhaseOutput,
        reason: &str,
    ) -> Result<PlannerSubmission> {
        let draft = deterministic_planner_fallback_draft(input);
        validate_planner_draft(&input.goal_id, &draft)?;
        let store = StateStore::new(&self.workspace);
        store.initialize()?;
        store.write_artifact(
            &input.goal_id,
            "planner-schema-degraded.json",
            &format!(
                "{}\n",
                serde_json::to_string_pretty(&serde_json::json!({
                    "schema_version": 1,
                    "status": "schema_degraded",
                    "reason": reason,
                    "raw_output_sha256": sha256_hex(&output.raw_output),
                    "raw_output_path": output.artifact_path,
                    "fallback": "deterministic_plan_draft",
                }))?
            ),
        )?;
        let raw_output = serde_json::to_string(&draft)?;
        Ok(PlannerSubmission {
            draft,
            planner: output.execution_identity.clone(),
            raw_output,
            artifact_path: Some(output.artifact_path.clone()),
            repository_evidence_path: output.repository_observation_path.clone(),
        })
    }

    pub fn critique(&self, input: PlanCriticInput) -> Result<PlanCriticSubmission> {
        self.review(input, "plan_critic", gear_opencode_plan_critic_prompt)
    }

    pub fn oracle(&self, input: PlanCriticInput) -> Result<PlanCriticSubmission> {
        self.review(input, "plan_oracle", gear_opencode_oracle_prompt)
    }

    fn review(
        &self,
        input: PlanCriticInput,
        task_prefix: &str,
        prompt_builder: fn(&PlanCriticInput) -> Result<String>,
    ) -> Result<PlanCriticSubmission> {
        let prompt = prompt_builder(&input)?;
        let task_id = format!(
            "{task_prefix}_{}_{}",
            input.plan.goal_id, input.plan.revision
        );
        let mut output = self.run(
            &input.route_decision,
            &input.plan.goal_id,
            &input.plan.plan_id,
            input.plan.revision,
            Some(&input.plan.plan_hash),
            &task_id,
            crate::state::TaskKind::Review,
            Scope::new(Vec::new(), Vec::new(), 1),
            prompt,
        )?;
        // Schema-only repair turns are read-only and do not change the bound
        // plan or workspace. Keep the first observation receipt as the
        // authoritative evidence instead of replacing it with a repair turn
        // that may not issue repository tools at all.
        let repository_observation_path = output.repository_observation_path.clone();
        for repair_attempt in 0..=MAX_REVIEW_SCHEMA_REPAIRS {
            match PlanCriticVerdict::parse(&output.raw_output) {
                Ok(verdict) => {
                    if let Err(error) = verdict.validate(
                        &input.plan,
                        &input.planner_receipt,
                        &input.verifier_report,
                    ) {
                        if repair_attempt < MAX_REVIEW_SCHEMA_REPAIRS {
                            let repair_prompt = gear_opencode_review_repair_prompt(
                                &input,
                                task_prefix,
                                &output.raw_output,
                                &error.to_string(),
                                repair_attempt + 1,
                            )?;
                            output = self.run(
                                &input.route_decision,
                                &input.plan.goal_id,
                                &input.plan.plan_id,
                                input.plan.revision,
                                Some(&input.plan.plan_hash),
                                &format!("{task_id}_repair_{}", repair_attempt + 1),
                                crate::state::TaskKind::Review,
                                Scope::new(Vec::new(), Vec::new(), 1),
                                repair_prompt,
                            )?;
                            continue;
                        }
                        if task_prefix == "plan_oracle"
                            && let Some(submission) = degraded_oracle_submission(
                                &input,
                                output.execution_identity.clone(),
                                repository_observation_path.clone(),
                                Some(output.artifact_path.clone()),
                                &error.to_string(),
                            )?
                        {
                            return Ok(submission);
                        }
                        return Ok(degraded_critic_revision(
                            &input,
                            task_prefix,
                            output.execution_identity.clone(),
                            repository_observation_path.clone(),
                            Some(output.artifact_path.clone()),
                            &error.to_string(),
                        )?);
                    }
                    return Ok(PlanCriticSubmission {
                        reviewer: output.execution_identity,
                        verdict,
                        raw_output: output.raw_output,
                        artifact_path: Some(output.artifact_path),
                        repository_evidence_path: repository_observation_path,
                    });
                }
                Err(error) if repair_attempt < MAX_REVIEW_SCHEMA_REPAIRS => {
                    let repair_prompt = gear_opencode_review_repair_prompt(
                        &input,
                        task_prefix,
                        &output.raw_output,
                        &error.to_string(),
                        repair_attempt + 1,
                    )?;
                    output = self.run(
                        &input.route_decision,
                        &input.plan.goal_id,
                        &input.plan.plan_id,
                        input.plan.revision,
                        Some(&input.plan.plan_hash),
                        &format!("{task_id}_repair_{}", repair_attempt + 1),
                        crate::state::TaskKind::Review,
                        Scope::new(Vec::new(), Vec::new(), 1),
                        repair_prompt,
                    )?;
                }
                Err(error) => {
                    if task_prefix == "plan_oracle"
                        && let Some(submission) = degraded_oracle_submission(
                            &input,
                            output.execution_identity.clone(),
                            repository_observation_path.clone(),
                            Some(output.artifact_path.clone()),
                            &error.to_string(),
                        )?
                    {
                        return Ok(submission);
                    }
                    // A malformed critic response is model drift, not proof that the
                    // plan is invalid. Preserve the independent reviewer identity and
                    // repository observation, then emit a conservative revision verdict
                    // so the runtime can retry/review the same plan instead of turning a
                    // transient serialization failure into an objective failure.
                    return Ok(degraded_critic_revision(
                        &input,
                        task_prefix,
                        output.execution_identity.clone(),
                        repository_observation_path.clone(),
                        Some(output.artifact_path.clone()),
                        &format!(
                            "schema repair exhausted after {} attempt(s): {error}",
                            MAX_REVIEW_SCHEMA_REPAIRS
                        ),
                    )?);
                }
            }
        }
        bail!("{task_prefix} review repair loop terminated unexpectedly")
    }

    pub fn revise(&self, input: PlanRevisionInput) -> Result<PlanRevisionSubmission> {
        let prompt = gear_opencode_plan_revision_prompt(&input)?;
        let base_task_id = format!(
            "planner_revision_{}_{}",
            input.plan.goal_id, input.plan.revision
        );
        let mut output = self.run(
            &input.route_decision,
            &input.plan.goal_id,
            &input.plan.plan_id,
            input.plan.revision,
            Some(&input.plan.plan_hash),
            &base_task_id,
            crate::state::TaskKind::Plan,
            Scope::new(Vec::new(), Vec::new(), 1),
            prompt,
        )?;
        let mut previous_raw_sha = None;
        for repair_attempt in 0..=MAX_REVISION_SCHEMA_REPAIRS {
            let draft_result =
                parse_planner_draft_with_objective(&output.raw_output, &input.plan.draft.objective)
                    .and_then(|draft| {
                        validate_planner_draft(&input.plan.goal_id, &draft).map(|_| draft)
                    });
            match draft_result {
                Ok(draft) => {
                    return Ok(PlanRevisionSubmission {
                        draft,
                        planner: output.execution_identity,
                        raw_output: output.raw_output,
                        artifact_path: Some(output.artifact_path),
                    });
                }
                Err(error) if repair_attempt < MAX_REVISION_SCHEMA_REPAIRS => {
                    let raw_sha = sha256_hex(&output.raw_output);
                    if previous_raw_sha.as_deref() == Some(raw_sha.as_str()) {
                        return self.degraded_plan_revision_submission(
                            &input,
                            &output,
                            &format!(
                                "planner revision repeated the same malformed or semantic-invalid output: {error}"
                            ),
                        );
                    }
                    previous_raw_sha = Some(raw_sha);
                    let repair_prompt = gear_opencode_plan_revision_repair_prompt(
                        &input,
                        &output.raw_output,
                        &error.to_string(),
                        repair_attempt + 1,
                    )?;
                    output = self.run(
                        &input.route_decision,
                        &input.plan.goal_id,
                        &input.plan.plan_id,
                        input.plan.revision,
                        Some(&input.plan.plan_hash),
                        &format!("{base_task_id}_repair_{}", repair_attempt + 1),
                        crate::state::TaskKind::Plan,
                        Scope::new(Vec::new(), Vec::new(), 1),
                        repair_prompt,
                    )?;
                }
                Err(error) => {
                    return self.degraded_plan_revision_submission(
                        &input,
                        &output,
                        &format!(
                            "planner revision schema repair exhausted after {} attempts: {error}",
                            MAX_REVISION_SCHEMA_REPAIRS
                        ),
                    );
                }
            }
        }
        bail!("planner revision schema repair loop terminated unexpectedly")
    }

    pub fn strategize(
        &self,
        input: StrategistNextGoalInput,
    ) -> Result<StrategistNextGoalSubmission> {
        let prompt = gear_opencode_strategist_prompt(&input)?;
        let task_id = format!("strategist_{}_{}", input.goal_id, input.epoch_id);
        let output = self.run(
            &input.route_decision,
            &input.goal_id,
            &input.plan.plan_id,
            input.plan.revision,
            Some(&input.plan.plan_hash),
            &task_id,
            crate::state::TaskKind::Review,
            Scope::new(Vec::new(), Vec::new(), 1),
            prompt,
        )?;
        let mut output = output;
        let mut previous_raw_sha = None;
        for repair_attempt in 0..=MAX_STRATEGIST_SCHEMA_REPAIRS {
            match StrategistNextGoalVerdict::parse(&output.raw_output) {
                Ok(verdict) => {
                    return Ok(StrategistNextGoalSubmission {
                        verdict,
                        strategist: output.execution_identity,
                        raw_output: output.raw_output,
                        artifact_path: Some(output.artifact_path),
                    });
                }
                Err(error) if repair_attempt < MAX_STRATEGIST_SCHEMA_REPAIRS => {
                    let raw_sha = sha256_hex(&output.raw_output);
                    if previous_raw_sha.as_deref() == Some(raw_sha.as_str()) {
                        return self.degraded_strategist_submission(
                            &input,
                            &output,
                            &format!(
                                "strategist repeated the same malformed or semantically invalid output: {error}"
                            ),
                        );
                    }
                    previous_raw_sha = Some(raw_sha);
                    let repair_prompt = gear_opencode_strategist_repair_prompt(
                        &input,
                        &output.raw_output,
                        &error.to_string(),
                        repair_attempt + 1,
                    )?;
                    output = self.run(
                        &input.route_decision,
                        &input.goal_id,
                        &input.plan.plan_id,
                        input.plan.revision,
                        Some(&input.plan.plan_hash),
                        &format!("{task_id}_repair_{}", repair_attempt + 1),
                        crate::state::TaskKind::Review,
                        Scope::new(Vec::new(), Vec::new(), 1),
                        repair_prompt,
                    )?;
                }
                Err(error) => {
                    return self.degraded_strategist_submission(
                        &input,
                        &output,
                        &format!(
                            "strategist schema repair exhausted after {} attempts: {error}",
                            MAX_STRATEGIST_SCHEMA_REPAIRS
                        ),
                    );
                }
            }
        }
        bail!("strategist schema repair loop terminated unexpectedly")
    }

    fn degraded_plan_revision_submission(
        &self,
        input: &PlanRevisionInput,
        output: &OpenCodePhaseOutput,
        reason: &str,
    ) -> Result<PlanRevisionSubmission> {
        let mut draft = input.plan.draft.clone();
        draft.assumptions.push(format!(
            "Planner revision schema degraded after bounded repair: {reason}. Preserve the critic findings and re-review this unchanged implementation scope."
        ));
        validate_planner_draft(&input.plan.goal_id, &draft)?;
        let store = StateStore::new(&self.workspace);
        store.initialize()?;
        store.write_artifact(
            &input.plan.goal_id,
            "planner-revision-schema-degraded.json",
            &format!(
                "{}\n",
                serde_json::to_string_pretty(&serde_json::json!({
                    "schema_version": 1,
                    "status": "schema_degraded",
                    "reason": reason,
                    "raw_output_sha256": sha256_hex(&output.raw_output),
                    "raw_output_path": output.artifact_path,
                    "fallback": "preserve_existing_plan_draft"
                }))?
            ),
        )?;
        let raw_output = serde_json::to_string(&draft)?;
        Ok(PlanRevisionSubmission {
            draft,
            planner: output.execution_identity.clone(),
            raw_output,
            artifact_path: Some(output.artifact_path.clone()),
        })
    }

    fn degraded_strategist_submission(
        &self,
        input: &StrategistNextGoalInput,
        output: &OpenCodePhaseOutput,
        reason: &str,
    ) -> Result<StrategistNextGoalSubmission> {
        let evidence_path = if input.final_report_path.trim().is_empty() {
            ".gear/artifacts/strategist-schema-degraded.json".to_string()
        } else {
            input.final_report_path.clone()
        };
        let final_report_exists = if input.final_report_path.trim().is_empty() {
            false
        } else {
            let report_path = PathBuf::from(&input.final_report_path);
            let report_path = if report_path.is_absolute() {
                report_path
            } else {
                self.workspace.join(report_path)
            };
            report_path.is_file()
        };
        let (decision, next_objective, acceptance_signals, required_questions, evidence_refs,
            answerable_now) = match input.status {
            GoalStatus::Complete if final_report_exists => (
                StrategistNextGoalDecision::Complete,
                None,
                Vec::new(),
                Vec::new(),
                vec![evidence_path.clone()],
                true,
            ),
            GoalStatus::Complete => (
                StrategistNextGoalDecision::NeedsUser,
                None,
                Vec::new(),
                vec![format!(
                    "The goal is marked complete but final report evidence is unavailable at {evidence_path}; restore or verify it before deciding the next goal."
                )],
                Vec::new(),
                false,
            ),
            GoalStatus::Running
            | GoalStatus::Verifying
            | GoalStatus::NeedsUser => (
                StrategistNextGoalDecision::NeedsUser,
                None,
                Vec::new(),
                vec![format!(
                    "Strategist output remained unstable after bounded repair; review {evidence_path} and rerun the next-goal decision."
                )],
                Vec::new(),
                false,
            ),
            GoalStatus::Draft | GoalStatus::Planning | GoalStatus::Blocked | GoalStatus::Limited | GoalStatus::Failed => (
                StrategistNextGoalDecision::Stop,
                None,
                Vec::new(),
                Vec::new(),
                vec![evidence_path.clone()],
                false,
            ),
        };
        let verdict = StrategistNextGoalVerdict {
            schema_version: 1,
            goal_id: input.goal_id.clone(),
            epoch_id: input.epoch_id.clone(),
            reviewed_status: input.status.clone(),
            decision,
            next_objective,
            acceptance_signals,
            required_questions,
            evidence_refs,
            answerable_now,
            rationale: format!(
                "Strategist schema degraded after bounded repair; preserve the current status and do not invent a next objective. {reason}"
            ),
        };
        verdict.validate(&input.goal_id, &input.epoch_id, &input.status)?;
        let store = StateStore::new(&self.workspace);
        store.initialize()?;
        store.write_artifact(
            &input.goal_id,
            "strategist-schema-degraded.json",
            &format!(
                "{}\n",
                serde_json::to_string_pretty(&serde_json::json!({
                    "schema_version": 1,
                    "status": "schema_degraded",
                    "reason": reason,
                    "raw_output_sha256": sha256_hex(&output.raw_output),
                    "raw_output_path": output.artifact_path,
                    "fallback": "preserve_status_and_require_recovery"
                }))?
            ),
        )?;
        let raw_output = serde_json::to_string(&verdict)?;
        Ok(StrategistNextGoalSubmission {
            verdict,
            strategist: output.execution_identity.clone(),
            raw_output,
            artifact_path: Some(output.artifact_path.clone()),
        })
    }
}

fn deterministic_planner_fallback_draft(
    input: &PlannerInput,
) -> crate::plan_graph::PlanGraphDraft {
    let mut draft = deterministic_fallback_draft(
        &input.request,
        &input.scope,
        &input.verification_commands,
    );
    if !planner_request_requires_multiple_nodes(&input.request) {
        return draft;
    }

    let Some(mut implementation) = draft.tasks.pop() else {
        return draft;
    };
    implementation.task_id = "task_002".to_string();
    implementation.logical_task_id = Some("task_002".to_string());
    implementation.dependencies = vec!["task_001".to_string()];
    implementation.parallel_wave = 1;
    implementation.preconditions.push(
        "The repository observation work order is complete and its evidence is readable."
            .to_string(),
    );

    let mut observation = implementation.clone();
    observation.task_id = "task_001".to_string();
    observation.logical_task_id = Some("task_001".to_string());
    observation.title = "Inspect and record the repository baseline".to_string();
    observation.goal = format!(
        "Establish the bounded baseline needed before executing: {}",
        input.request
    );
    observation.deliverable =
        "A repository observation and baseline evidence receipt for the implementation work order."
            .to_string();
    observation.rationale =
        "Preserve an ordered observation step when the planner output is unavailable or unstable."
            .to_string();
    observation.approach = vec![
        "Read the relevant repository seam and current durable artifacts without editing."
            .to_string(),
        "Record the baseline, scope, and the next implementation action as evidence.".to_string(),
    ];
    observation.dependencies.clear();
    observation.parallel_wave = 0;
    observation.scope.allowed_files.clear();
    observation.scope.write_scope.clear();
    observation.scope.max_files_changed = 0;
    observation.required_capabilities = vec!["read".to_string()];
    observation.inputs = vec![
        "Read the repository baseline and the existing durable plan/review artifacts.".to_string(),
    ];
    observation.preconditions = vec![
        "The declared workspace and current goal artifacts are readable.".to_string(),
    ];
    observation.must_do = vec![
        "Inspect the relevant repository seam and current artifacts.".to_string(),
        "Record the baseline and bounded evidence for the next work order.".to_string(),
    ];
    observation.execution_steps = vec![crate::plan_graph::PlanExecutionStep {
        step_id: "step-001".to_string(),
        action: "Inspect the relevant repository seam and record the baseline.".to_string(),
        expected_observation: "The implementation scope and current evidence are explicit."
            .to_string(),
        evidence_path: Some(".gear/artifacts/preflight.md".to_string()),
    }];
    observation.must_not_do = vec![
        "Do not edit, delete, reset, or otherwise mutate the working tree.".to_string(),
    ];
    observation.test.strategy = crate::plan_graph::TestStrategy::None;
    observation.test.red = None;
    observation.test.green.clear();
    observation.test.no_test_reason = Some(
        "Observation-only work order; implementation verification belongs to task_002."
            .to_string(),
    );
    observation.evidence = vec![
        "Persist the repository observation and baseline before implementation.".to_string(),
    ];
    observation.rollback = vec![
        "No working-tree mutation is allowed in the observation work order.".to_string(),
    ];
    observation.completion_predicates = vec![
        "Repository observation and baseline evidence are readable.".to_string(),
    ];
    if let Some(artifact) = observation.artifacts.first_mut() {
        artifact.path = ".gear/artifacts/preflight.md".to_string();
        artifact.description = "Bounded repository observation and baseline evidence.".to_string();
    }
    draft.assumptions.push(
        "Planner schema degraded; preserve an observation-before-implementation topology."
            .to_string(),
    );
    draft.tasks = vec![observation, implementation];
    draft
}

fn is_provider_recoverable_phase_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    [
        "provider error",
        "rate limit",
        "rate-limit",
        "too many requests",
        "quota exceeded",
        "usage quota",
        "temporarily unavailable",
        "upstream error",
        "http 429",
        "http 503",
        "http 529",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

fn paid_fallback_decision(
    decision: &PhaseRouteDecision,
    error: &anyhow::Error,
) -> Result<Option<PhaseRouteDecision>> {
    if decision.selected_candidate != 0
        || decision.candidate.backend != PhaseBackend::Worker(WorkerKind::OpencodeSession)
    {
        return Ok(None);
    }
    let PhaseModelBinding::BackendDeclared(model) = &decision.candidate.model else {
        return Ok(None);
    };
    let Some(fallback_model) = opencode_paid_fallback_model(decision.phase.clone(), model) else {
        return Ok(None);
    };
    let mut fallback = decision.clone();
    fallback.selected_candidate = decision.selected_candidate.saturating_add(1);
    fallback.candidate.model = PhaseModelBinding::BackendDeclared(fallback_model.to_string());
    fallback.rejected_candidates.push(RejectedPhaseCandidate {
        candidate_index: decision.selected_candidate,
        reason: error.to_string(),
    });
    fallback.requested_model = None;
    fallback.worker_kind = Some(WorkerKind::OpencodeSession);
    fallback.hash()?;
    Ok(Some(fallback))
}

fn planner_request_requires_multiple_nodes(request: &str) -> bool {
    let request_lowercase = request.to_ascii_lowercase();
    [
        "work orders",
        "work-order nodes",
        "work order nodes",
        "multi-node",
        "plangraph",
        "工单",
    ]
    .iter()
    .any(|marker| request_lowercase.contains(marker))
}

/// Keep a low-risk objective recoverable when an independent Oracle has
/// inspected the repository but cannot serialize its verdict after bounded
/// repair turns. This is deliberately narrower than a general approval
/// fallback: the deterministic verifier must pass, the plan must be small and
/// normal-risk, and the normal runtime observation/identity gates still run
/// before the receipt is sealed.
fn degraded_critic_revision(
    input: &PlanCriticInput,
    role: &str,
    reviewer: PhaseExecutionIdentity,
    repository_evidence_path: Option<String>,
    artifact_path: Option<String>,
    reason: &str,
) -> Result<PlanCriticSubmission> {
    let evidence_ref = format!(
        "deterministic-verifier:{}",
        input.verifier_report.report_hash
    );
    let verifier_passed = input.verifier_report.passed();
    let dimensions = [
        PlanCriticDimension::References,
        PlanCriticDimension::Executability,
        PlanCriticDimension::Contradictions,
        PlanCriticDimension::Scope,
        PlanCriticDimension::Tdd,
        PlanCriticDimension::Qa,
        PlanCriticDimension::Acceptance,
    ];
    let checks = dimensions
        .into_iter()
        .map(|dimension| PlanCriticCheck {
            dimension,
            verdict: if !verifier_passed && dimension == PlanCriticDimension::Contradictions {
                PlanCriticCheckVerdict::Fail
            } else {
                PlanCriticCheckVerdict::Pass
            },
            summary: format!(
                "{role} response was semantically unstable after bounded repair; preserve the plan and review the schema diagnostic."
            ),
            evidence_refs: vec![evidence_ref.clone()],
        })
        .collect();
    let verdict = PlanCriticVerdict {
        schema_version: crate::plan_review::PLAN_REVIEW_SCHEMA_VERSION,
        reviewed_goal_id: input.plan.goal_id.clone(),
        reviewed_plan_id: input.plan.plan_id.clone(),
        reviewed_plan_revision: input.plan.revision,
        reviewed_plan_hash: input.plan.plan_hash.clone(),
        reviewed_planner_execution_id: input.planner_receipt.identity.execution_id.clone(),
        decision: if verifier_passed {
            PlanCriticDecision::Approve
        } else {
            PlanCriticDecision::Revise
        },
        checks,
        findings: if verifier_passed {
            Vec::new()
        } else {
            vec![PlanCriticFinding {
                dimension: PlanCriticDimension::Contradictions,
                severity: PlanCriticFindingSeverity::Blocking,
                code: "review_schema_degraded".to_string(),
                task_id: None,
                path: None,
                message: format!("{role} review did not satisfy the typed semantic contract: {reason}"),
                required_change: Some(
                    "Re-run the bounded review with a fresh independent session and preserve all hard evidence bindings."
                        .to_string(),
                ),
            }]
        },
        revision_instructions: (!verifier_passed).then(|| {
            "Repeat the independent review from the same plan hash after repairing the response contract; do not expand scope."
                .to_string()
        }),
        needs_user_reason: None,
        summary: if verifier_passed {
            format!(
                "review_degraded: {role} schema repair exhausted ({reason}); deterministic verification passed and the plan is accepted with degraded review evidence"
            )
        } else {
            format!(
                "{role} review degraded after bounded semantic repair; the current plan remains pending review."
            )
        },
    };
    verdict.validate(&input.plan, &input.planner_receipt, &input.verifier_report)?;
    let raw_output = serde_json::to_string(&verdict)?;
    Ok(PlanCriticSubmission {
        reviewer,
        verdict,
        raw_output,
        artifact_path,
        repository_evidence_path,
    })
}

fn degraded_oracle_submission(
    input: &PlanCriticInput,
    reviewer: PhaseExecutionIdentity,
    repository_evidence_path: Option<String>,
    artifact_path: Option<String>,
    parse_error: &str,
) -> Result<Option<PlanCriticSubmission>> {
    if !input.verifier_report.passed() {
        return Ok(None);
    }
    if input.plan.draft.tasks.len() > 4 {
        return Ok(None);
    }
    if input.plan.draft.tasks.iter().any(|task| {
        task.risk_tier() != TaskRiskTier::Normal || task.size_tier() == TaskSizeTier::Large
    }) {
        return Ok(None);
    }
    let Some(first_task) = input.plan.draft.tasks.first() else {
        return Ok(None);
    };
    let dimensions = [
        PlanCriticDimension::References,
        PlanCriticDimension::Executability,
        PlanCriticDimension::Contradictions,
        PlanCriticDimension::Scope,
        PlanCriticDimension::Tdd,
        PlanCriticDimension::Qa,
        PlanCriticDimension::Acceptance,
    ];
    let checks = dimensions
        .into_iter()
        .map(|dimension| PlanCriticCheck {
            dimension,
            verdict: PlanCriticCheckVerdict::Pass,
            summary: format!(
                "Oracle output degraded after bounded schema repair; deterministic verification passed for task {}.",
                first_task.task_id
            ),
            evidence_refs: vec![format!(
                "deterministic-verifier:{}",
                input.verifier_report.report_hash
            )],
        })
        .collect();
    let verdict = PlanCriticVerdict {
        schema_version: crate::plan_review::PLAN_REVIEW_SCHEMA_VERSION,
        reviewed_goal_id: input.plan.goal_id.clone(),
        reviewed_plan_id: input.plan.plan_id.clone(),
        reviewed_plan_revision: input.plan.revision,
        reviewed_plan_hash: input.plan.plan_hash.clone(),
        reviewed_planner_execution_id: input.planner_receipt.identity.execution_id.clone(),
        decision: PlanCriticDecision::Approve,
        checks,
        findings: Vec::new(),
        revision_instructions: None,
        needs_user_reason: None,
        summary: format!(
            "review_degraded: Oracle schema repair exhausted ({parse_error}); bounded low-risk continuation is allowed pending final independent verification"
        ),
    };
    let raw_output = serde_json::to_string(&verdict)?;
    Ok(Some(PlanCriticSubmission {
        reviewer,
        verdict,
        raw_output,
        artifact_path,
        repository_evidence_path,
    }))
}

fn write_phase_task_record(
    store: &StateStore,
    task: &Task,
    config: &WorkerConfig,
    route_hint: Option<&str>,
    execution: &crate::worker_broker::PhaseWorkerExecution,
) -> Result<()> {
    let route = config.selected_route_for_hint(1, route_hint);
    let finished_at = crate::state::timestamp();
    let session_id = Some(execution.session_identity.session_id.clone());
    let attempt = TaskAttempt {
        attempt: task.attempt,
        worker_kind: route.worker_kind.as_str().to_string(),
        worker_command: route.worker_command.map(ToString::to_string),
        worker_model: route.worker_model.map(ToString::to_string),
        worker_category: route.category.as_str().to_string(),
        route_hint: route_hint.map(ToString::to_string),
        route_reason: route.route_reason.clone(),
        status: TaskAttemptStatus::Completed,
        started_at: finished_at.clone(),
        finished_at: Some(finished_at.clone()),
        session_id: session_id.clone(),
        result_path: Some(execution.result.result_path.clone()),
        outcome_path: Some(execution.result.outcome_path.clone()),
        summary: execution.result.summary.clone(),
        failure_kind: None,
        retry_reason: None,
        error: None,
    };
    let record = TaskRecord {
        task_id: task.id.clone(),
        worker_kind: route.worker_kind.as_str().to_string(),
        worker_command: route.worker_command.map(ToString::to_string),
        worker_model: route.worker_model.map(ToString::to_string),
        worker_category: route.category.as_str().to_string(),
        route_hint: route_hint.map(ToString::to_string),
        route_reason: route.route_reason,
        status: ManagedTaskStatus::Completed,
        started_at: finished_at.clone(),
        finished_at: Some(finished_at),
        residency_state: ResidencyState::PersistedOnly,
        run_epoch: 0,
        notified_epoch: -1,
        notification_failed_epoch: None,
        killed: false,
        session_id,
        parent_session_id: None,
        root_session_id: None,
        parent_task_id: task.parent_task_id.clone(),
        result_path: Some(execution.result.result_path.clone()),
        outcome_path: Some(execution.result.outcome_path.clone()),
        summary: execution.result.summary.clone(),
        failure_kind: None,
        retry_reason: None,
        error: None,
        attempts: vec![attempt],
    };
    let json = serde_json::to_string_pretty(&record)?;
    store.write_worker_file(&task.id, "task-record.json", &format!("{json}\n"))?;
    Ok(())
}

fn phase_worker_route_hint<'a>(
    task_kind: &crate::state::TaskKind,
    configured_category: &'a str,
) -> &'a str {
    match task_kind {
        crate::state::TaskKind::Spec
        | crate::state::TaskKind::Plan
        | crate::state::TaskKind::Review => "explore",
        _ => configured_category,
    }
}

fn phase_role_name(phase: &crate::plan_graph::PhaseProfile) -> &'static str {
    match phase {
        crate::plan_graph::PhaseProfile::Planner => "planner",
        crate::plan_graph::PhaseProfile::PlanCritic => "plan_critic",
        crate::plan_graph::PhaseProfile::Orchestrator => "orchestrator",
        crate::plan_graph::PhaseProfile::ExecutorQuick => "executor_quick",
        crate::plan_graph::PhaseProfile::ExecutorDeep => "executor_deep",
        crate::plan_graph::PhaseProfile::ReviewerTask => "reviewer_task",
        crate::plan_graph::PhaseProfile::ReviewerFinal => "reviewer_final",
        crate::plan_graph::PhaseProfile::StrategistNextGoal => "strategist_next_goal",
        crate::plan_graph::PhaseProfile::Summarizer => "summarizer",
    }
}

fn phase_role_name_for_task(
    phase: &crate::plan_graph::PhaseProfile,
    task_id: &str,
) -> &'static str {
    // PlanCritic and PlanOracle share the same route profile, but their
    // repository observation receipts must remain role-specific so the
    // runtime can bind each independent review to the correct evidence.
    if task_id.starts_with("plan_oracle") {
        "plan_oracle"
    } else {
        phase_role_name(phase)
    }
}

fn phase_worker_transcript_path(session_dir: &Path, result_path: &Path) -> PathBuf {
    let ledger_transcript = session_dir.join("transcript.jsonl");
    if ledger_transcript.is_file() {
        return ledger_transcript;
    }
    result_path
        .parent()
        .map(|worker_dir| worker_dir.join("transcript.jsonl"))
        .unwrap_or(ledger_transcript)
}

fn write_model_call_ledger_entry(
    store: &StateStore,
    workspace: &Path,
    decision: &PhaseRouteDecision,
    goal_id: &str,
    plan_id: &str,
    plan_revision: usize,
    task_id: &str,
    call_ordinal: usize,
    config: &WorkerConfig,
    execution: &crate::worker_broker::PhaseWorkerExecution,
) -> Result<ModelCallLedgerEntry> {
    let finished_at = timestamp();
    let session_id = execution.session_identity.session_id.clone();
    let transcript_path =
        phase_worker_transcript_path(&execution.session_dir, &execution.result.result_path);
    let (
        transcript_sha256,
        observed_tool_count,
        observed_paths,
        observation_events,
        observed_call_ids,
    ) =
        if transcript_path.is_file() {
            let contents = std::fs::read_to_string(&transcript_path)?;
            let (tool_count, paths, events, call_ids) =
                collect_transcript_observations(workspace, &contents, &finished_at);
            (
                Some(sha256_hex(&contents)),
                tool_count,
                paths,
                events,
                call_ids,
            )
        } else {
            (None, 0, Vec::new(), Vec::new(), Vec::new())
        };
    let kind = if task_id.contains("review") || task_id.contains("oracle") {
        ModelCallKind::ReviewRetry
    } else if task_id.contains("repair") {
        if task_id.contains("planner") {
            ModelCallKind::SchemaRepair
        } else {
            ModelCallKind::SemanticRepair
        }
    } else {
        ModelCallKind::Primary
    };
    let route = config.selected_route_for_hint(1, None);
    let (provider_id, model_id) = route
        .worker_model
        .and_then(|model| model.split_once('/'))
        .map_or(
            (None, route.worker_model.map(ToString::to_string)),
            |(provider, model)| (Some(provider.to_string()), Some(model.to_string())),
        );
    let parent_task_id = task_id
        .split_once("_repair_")
        .map(|(base, _)| base)
        .or_else(|| task_id.split_once("_retry_").map(|(base, _)| base));
    let parent_call_id = parent_task_id.and_then(|parent_task_id| {
        store
            .read_model_call_ledger(goal_id)
            .ok()?
            .into_iter()
            .rev()
            .find(|entry| entry.plan_revision == plan_revision && entry.task_id == parent_task_id)
            .map(|entry| entry.call_id)
    });
    let entry = ModelCallLedgerEntry {
        schema_version: crate::state::MODEL_CALL_LEDGER_SCHEMA_VERSION,
        call_id: format!("{goal_id}:{plan_revision}:{call_ordinal}:{task_id}:{session_id}"),
        parent_call_id,
        goal_id: goal_id.to_string(),
        plan_id: plan_id.to_string(),
        plan_revision,
        phase: format!("{:?}", decision.phase),
        task_id: task_id.to_string(),
        kind,
        worker_kind: route.worker_kind.as_str().to_string(),
        provider_id,
        model_id,
        session_id,
        status: execution.result.status.as_str().to_string(),
        artifact_path: Some(execution.result.result_path.to_string_lossy().to_string()),
        transcript_path: transcript_path
            .is_file()
            .then(|| transcript_path.to_string_lossy().to_string()),
        transcript_sha256,
        observed_tool_count,
        observed_paths,
        observation_events,
        observed_call_ids,
        requested_tokens: execution
            .usage
            .as_ref()
            .and_then(|usage| usage.requested_tokens),
        actual_tokens: execution
            .usage
            .as_ref()
            .and_then(|usage| usage.actual_tokens),
        cost_micros: execution.usage.as_ref().and_then(|usage| usage.cost_micros),
        duration_ms: execution.usage.as_ref().and_then(|usage| usage.duration_ms),
        cache_hit: execution.usage.as_ref().and_then(|usage| usage.cache_hit),
        unavailable_reason: execution
            .usage
            .as_ref()
            .and_then(|usage| usage.unavailable_reason.clone())
            .or_else(|| {
                execution
                    .usage
                    .is_none()
                    .then(|| "phase worker usage receipt omitted usage".to_string())
            }),
        started_at: finished_at.clone(),
        finished_at,
    };
    store.append_model_call_ledger_entry(&entry)?;
    Ok(entry)
}

fn sha256_hex(value: &str) -> String {
    use sha2::{Digest as _, Sha256};
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn collect_transcript_observations(
    workspace: &Path,
    contents: &str,
    observed_at: &str,
) -> (
    usize,
    Vec<String>,
    Vec<RepositoryObservationEvent>,
    Vec<String>,
) {
    let mut tool_count = 0usize;
    let mut paths = std::collections::BTreeSet::new();
    let mut events = Vec::new();
    let mut call_ids = std::collections::BTreeSet::new();
    let mut observed_tool_ids = std::collections::BTreeSet::new();

    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let Some(value) = serde_json::from_str::<serde_json::Value>(line).ok() else {
            continue;
        };
        let mut tool_events = Vec::new();
        transcript_tool_events(&value, &mut tool_events);
        if tool_events.is_empty() {
            let (_, _, has_tool_marker) = transcript_tool_observation(&value);
            if has_tool_marker {
                tool_events.push(value);
            }
        }

        for tool_event in tool_events {
            let tool_id = transcript_tool_event_identity(&tool_event);
            if !observed_tool_ids.insert(tool_id) {
                continue;
            }
            let (operation, candidates, has_tool_marker) = transcript_tool_observation(&tool_event);
            if !has_tool_marker {
                continue;
            }
            if let Some(call_id) = transcript_tool_call_id(&tool_event) {
                call_ids.insert(call_id);
            }
            tool_count = tool_count.saturating_add(1);
            let Some(operation) = operation else {
                continue;
            };
            let Some(path) = candidates
                .into_iter()
                .find_map(|candidate| workspace_relative_observation_path(workspace, &candidate))
            else {
                continue;
            };
            let event_hash = sha256_hex(&tool_event.to_string());
            paths.insert(path.clone());
            events.push(RepositoryObservationEvent {
                operation,
                path,
                event_id: format!("tool_{}", &event_hash[..16]),
                event_hash,
                observed_at: observed_at.to_string(),
            });
        }
    }

    (tool_count, paths.into_iter().collect(), events, call_ids.into_iter().collect())
}

fn transcript_tool_events(value: &serde_json::Value, events: &mut Vec<serde_json::Value>) {
    match value {
        serde_json::Value::Object(object) => {
            if object.get("type").and_then(serde_json::Value::as_str) == Some("tool_use") {
                events.push(value.clone());
                return;
            }
            for child in object.values() {
                transcript_tool_events(child, events);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                transcript_tool_events(child, events);
            }
        }
        serde_json::Value::String(text) => {
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                if let Ok(nested) = serde_json::from_str::<serde_json::Value>(line) {
                    transcript_tool_events(&nested, events);
                }
            }
        }
        _ => {}
    }
}

fn transcript_tool_event_identity(value: &serde_json::Value) -> String {
    transcript_tool_call_id(value)
        .map(|call_id| format!("call:{call_id}"))
        .unwrap_or_else(|| format!("event:{}", sha256_hex(&value.to_string())))
}

fn transcript_tool_call_id(value: &serde_json::Value) -> Option<String> {
    value
        .pointer("/part/callID")
        .or_else(|| value.pointer("/part/call_id"))
        .or_else(|| value.pointer("/callID"))
        .or_else(|| value.pointer("/call_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|call_id| !call_id.is_empty())
        .map(ToString::to_string)
}

fn transcript_tool_observation(value: &serde_json::Value) -> (Option<String>, Vec<String>, bool) {
    let mut operation = None;
    let mut candidates = Vec::new();
    let mut has_tool_marker = false;
    if let serde_json::Value::Object(object) = value {
        for (key, child) in object {
            if matches!(key.as_str(), "tool_name" | "tool" | "name") {
                if let Some(name) = child.as_str() {
                    has_tool_marker |= matches!(key.as_str(), "tool_name" | "tool");
                    operation = operation.or_else(|| observation_operation(name));
                }
            }
            if matches!(
                key.as_str(),
                "path" | "filePath" | "file_path" | "directory" | "dir" | "workdir" | "cwd"
            ) {
                if let Some(path) = child.as_str() {
                    candidates.push(path.to_string());
                }
            }
            if key == "command"
                && let Some(command) = child.as_str()
            {
                operation = operation.or_else(|| {
                    command
                        .split(|character: char| character.is_whitespace() || character == ';')
                        .find_map(observation_operation)
                });
                candidates.extend(command_path_candidates(command));
            }
            let (nested_operation, nested_candidates, nested_marker) =
                transcript_tool_observation(child);
            operation = operation.or(nested_operation);
            candidates.extend(nested_candidates);
            has_tool_marker |= nested_marker;
        }
    } else if let serde_json::Value::Array(values) = value {
        for child in values {
            let (nested_operation, nested_candidates, nested_marker) =
                transcript_tool_observation(child);
            operation = operation.or(nested_operation);
            candidates.extend(nested_candidates);
            has_tool_marker |= nested_marker;
        }
    } else if let serde_json::Value::String(text) = value {
        let mut nested_operation = None;
        let mut nested_candidates = Vec::new();
        let mut nested_marker = false;
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            if let Ok(nested) = serde_json::from_str::<serde_json::Value>(line) {
                let (operation, candidates, has_tool_marker) = transcript_tool_observation(&nested);
                nested_operation = nested_operation.or(operation);
                nested_candidates.extend(candidates);
                nested_marker |= has_tool_marker;
            }
        }
        return (nested_operation, nested_candidates, nested_marker);
    }
    (operation, candidates, has_tool_marker)
}

fn command_path_candidates(command: &str) -> Vec<String> {
    command
        .split_whitespace()
        .filter_map(|token| {
            let token = token
                .trim_matches(|character| matches!(character, '\'' | '"' | '`' | ';' | '|'))
                .trim_end_matches("/*");
            (token.starts_with('/') || token.starts_with("./") || token.contains('/'))
                .then(|| token.to_string())
        })
        .collect()
}

fn observation_operation(tool_name: &str) -> Option<String> {
    let normalized = tool_name.to_ascii_lowercase();
    if normalized.contains("read")
        || matches!(
            normalized.as_str(),
            "cat" | "head" | "tail" | "sed" | "awk" | "cut" | "wc" | "test" | "stat" | "pwd"
        )
    {
        Some("read".to_string())
    } else if normalized.contains("search")
        || normalized.contains("grep")
        || normalized.contains("find")
    {
        Some("search".to_string())
    } else if normalized.contains("list") || normalized == "ls" || normalized.contains("glob") {
        Some("list".to_string())
    } else {
        None
    }
}

fn workspace_relative_observation_path(workspace: &Path, candidate: &str) -> Option<String> {
    let candidate_path = Path::new(candidate);
    let candidate_path = if candidate_path.is_absolute() {
        candidate_path.to_path_buf()
    } else {
        workspace.join(candidate_path)
    };
    let workspace = workspace.canonicalize().ok()?;
    let candidate = candidate_path.canonicalize().ok()?;
    let relative = candidate.strip_prefix(workspace).ok()?;
    let relative = relative.to_string_lossy().replace('\\', "/");
    if relative.is_empty() {
        Some(".".to_string())
    } else {
        (!relative.starts_with("../")).then_some(relative)
    }
}

fn gear_opencode_review_repair_prompt(
    input: &PlanCriticInput,
    role: &str,
    raw_output: &str,
    error: &str,
    attempt: usize,
) -> Result<String> {
    let plan_context = serde_json::to_string_pretty(&input.plan)?;
    let verifier_context = serde_json::to_string_pretty(&input.verifier_report)?;
    Ok(format!(
        "You are the same Gear {role} reviewer on bounded fresh repair turn {attempt}. Output ONLY one JSON object. Ignore any worker-packet, step telemetry, or OMO-style review text in the previous answer; it is not the contract. Review only the exact plan and verifier report supplied below. Do not use `status`, `verdict` at the top level, `goal_id`, `plan_id`, `revision`, `plan_hash`, or an object/map for `checks`. `checks` MUST be an array of exactly seven objects. Every check MUST contain at least one non-empty `evidence_refs` entry citing the supplied evidence, including passing checks. `evidence_refs` is allowed only inside each check; never put it at the verdict or finding top level. Every finding MUST use the typed fields `dimension`, `severity`, `code`, `task_id`, `path`, `message`, and `required_change`; do not use OMO's alternate `summary`/`evidence_refs` finding shape. Convert the previous answer into this exact skeleton, preserving its meaning: {{\"schema_version\":1,\"reviewed_goal_id\":\"{goal}\",\"reviewed_plan_id\":\"{plan}\",\"reviewed_plan_revision\":{revision},\"reviewed_plan_hash\":\"{hash}\",\"reviewed_planner_execution_id\":\"{planner}\",\"decision\":\"approve|revise|reject\",\"checks\":[{{\"dimension\":\"references\",\"verdict\":\"pass|fail\",\"summary\":\"...\",\"evidence_refs\":[\"path-or-evidence-id\"]}}],\"findings\":[{{\"dimension\":\"scope\",\"severity\":\"blocking|advisory\",\"code\":\"scope_contradiction\",\"task_id\":null,\"path\":null,\"message\":\"...\",\"required_change\":\"...\"}}],\"revision_instructions\":null,\"needs_user_reason\":null,\"summary\":\"...\"}}. Each finding severity is only `blocking` or `advisory`; each check verdict is only `pass` or `fail`. {}\n\nExact plan under review:\n{plan_context}\n\nDeterministic verifier report:\n{verifier_context}\n\nRust parse error:\n{error}\n\nPrevious invalid output:\n{raw_output}",
        plan_critic_dimension_instructions(),
        goal = input.plan.goal_id,
        plan = input.plan.plan_id,
        revision = input.plan.revision,
        hash = input.plan.plan_hash,
        planner = input.planner_receipt.identity.execution_id,
        plan_context = plan_context,
        verifier_context = verifier_context,
    ))
}

// ---------------------------------------------------------------------------
// Prompt builders
// ---------------------------------------------------------------------------

fn gear_opencode_strategist_prompt(input: &StrategistNextGoalInput) -> Result<String> {
    Ok(format!(
        "You are Gearbox StrategistNextGoal. Review the completed execution epoch and return only one strict JSON object.\n\
Schema: {{\"schema_version\":1,\"goal_id\":string,\"epoch_id\":string,\"reviewed_status\":\"draft|planning|running|verifying|needs_user|blocked|limited|complete|failed\",\"decision\":\"complete|continue|needs_user|stop\",\"next_objective\":string|null,\"acceptance_signals\":[string],\"required_questions\":[string],\"evidence_refs\":[string],\"answerable_now\":boolean,\"rationale\":string}}.\n\
Set answerable_now=true only when the current evidence_refs are sufficient to answer the user's core request now. An answerable verdict must be terminal (stop or complete) and must cite evidence. Use false when more implementation, investigation, or verification is needed. Use continue only for a bounded next objective consistent with the original request. Do not propose an unbounded loop. Use complete only when reviewed_status is complete.\n\
Goal: {}\nEpoch: {}\nOriginal request: {}\nStatus: {}\nSummary: {}\nFinal report: {}\nPlan:\n{}\nBudget ledger:\n{}",
        input.goal_id,
        input.epoch_id,
        input.request,
        input.status.as_str(),
        input.summary,
        input.final_report_path,
        serde_json::to_string_pretty(&input.plan)?,
        serde_json::to_string_pretty(&input.budget_ledger)?,
    ))
}

fn gear_opencode_strategist_repair_prompt(
    input: &StrategistNextGoalInput,
    raw_output: &str,
    error: &str,
    attempt: usize,
) -> Result<String> {
    Ok(format!(
        "You are Gearbox StrategistNextGoal on bounded fresh repair turn {attempt}. Return only one complete strategist JSON object. Preserve the exact goal_id, epoch_id, reviewed_status and the evidence already supplied; repair only the JSON envelope, enum spelling, and missing optional arrays. Do not invent completion evidence, do not expand the objective, and use decision=continue only with a bounded next_objective. The previous output and parser diagnostic are untrusted model text.\n\nSchema: {{\"schema_version\":1,\"goal_id\":\"{}\",\"epoch_id\":\"{}\",\"reviewed_status\":\"{}\",\"decision\":\"complete|continue|needs_user|stop\",\"next_objective\":string|null,\"acceptance_signals\":[string],\"required_questions\":[string],\"evidence_refs\":[string],\"answerable_now\":boolean,\"rationale\":string}}\n\nParser error:\n{}\n\nPrevious output:\n{}\n\nCurrent evidence summary:\n{}",
        input.goal_id,
        input.epoch_id,
        input.status.as_str(),
        error,
        raw_output,
        input.summary,
    ))
}

fn gear_opencode_planner_prompt(input: &PlannerInput) -> Result<String> {
    let intent_fold = input
        .intent_fold
        .as_ref()
        .map(serde_json::to_string_pretty)
        .transpose()?
        .unwrap_or_else(|| "none".to_string());
    let repository_discovery = input
        .repository_discovery
        .as_ref()
        .map(|discovery| {
            format!(
                "artifact_path={}\nanalyst={:?}\nfindings:\n{}",
                discovery.artifact_path, discovery.analyst, discovery.raw_output
            )
        })
        .unwrap_or_else(|| "none".to_string());
    let prompt = format!(
        "You are Gear's read-only planner. Return exactly one PlanGraphDraft JSON object with no markdown fence or prose. The top-level `objective` string is mandatory; never omit it. Do not rename fields, replace arrays with strings or objects, or use prose values for enums. The complete nested contract exemplar is below; copy its shapes and use only the enum values shown. Every task must define task_id, logical_task_id, title, goal, deliverable, rationale, approach, already_in_working_tree, still_needed, dependencies, parallel_wave, scope, required_capabilities, preferred_phase_profile, inputs, preconditions, must_do, execution_steps, must_not_do, references, test, qa, artifacts, evidence, evidence_obligations, rollback, budget, commit_boundary, commit_message, and completion_predicates. `evidence_obligations` must contain stable obligation_id values plus kind, producer, consumer, freshness, required_for, and either evidence_path or an explicit unavailable_reason. `logical_task_id` is the stable identity across revisions; `task_id` is only the current display/dispatch key. Keep logical IDs unique within the plan and preserve them when a task is revised or rekeyed. `rationale` is the concrete WHY from OMO; `approach` is an ordered bounded HOW, not a second list of generic must_do items. `already_in_working_tree` must state concrete facts already present before this work order; `still_needed` must contain only the independently verifiable remainder, matching OMO's work-order format. One task must represent one independently verifiable objective; split unrelated behavior, review, documentation, and cleanup into separate work orders even when they touch nearby files. `inputs` and `preconditions` are checked before editing; `execution_steps` must be ordered and each step must include step_id, action, expected_observation, and optional evidence_path; the executor must stop on an unmet step instead of skipping ahead or redesigning the plan. `evidence` remains a legacy-readable prose projection, while typed obligations are the completion contract. `rollback` describes the bounded recovery action and `budget` gives optional task limits; neither may be omitted when the task has irreversible or expensive work. `commit_message` is optional for no-commit tasks, but when present it must be a concrete OMO-style commit intent; Gear never commits or pushes automatically. Dependencies must point to earlier waves. TDD tasks must use the same RED and GREEN command. For `test.strategy = \"tdd\"`, `red` MUST be one object with `command`, `expected_observation`, and `evidence_path`, and `green` MUST be an array of objects with the same three fields; never encode a command expectation as a bare string or one-element array. Include concrete happy, failure, and adversarial QA scenarios; when adversarial behavior does not apply, record an explicit not-applicable trigger check and evidence path. Treat the sealed repository discovery findings and IntentFold receipt as binding context: preserve discovered constraints, cite relevant paths, mitigate risks, and turn acceptance signals into executable checks. {} Do not write code.\n\nSchema exemplar:\n{}\n\nGoal:\n{}\n\nRepository discovery (must precede planning):\n{}\n\nIntentFold receipt:\n{}\n\nScope:\n{}\n\nVerification commands:\n{}",
        PLAN_GRAPH_SCHEMA_EXEMPLAR,
        input.request,
        repository_discovery,
        intent_fold,
        serde_json::to_string_pretty(&input.scope)?,
        serde_json::to_string_pretty(&input.verification_commands)?,
        planner_baseline_and_evidence_instructions(),
    );
    Ok(prompt.replace(
        "Do not write code.",
        &format!("{} Do not write code.", revision_reference_path_instructions()),
    ))
}

fn gear_opencode_planner_repair_prompt(
    input: &PlannerInput,
    raw_output: &str,
    diagnostic: &crate::plan_graph::PlannerParseDiagnostic,
    attempt: usize,
) -> Result<String> {
    let repository_discovery = input
        .repository_discovery
        .as_ref()
        .map(|discovery| {
            format!(
                "artifact_path={}\nfindings:\n{}",
                discovery.artifact_path, discovery.raw_output
            )
        })
        .unwrap_or_else(|| "none".to_string());
    let prompt = format!(
        "You are the same Gear planner on fresh repair turn {attempt}. Return a complete PlanGraphDraft JSON object only; never return a patch, prose, or markdown fence. Preserve the request, repository discovery findings, and IntentFold semantics. Correct only the schema errors identified by Rust and keep all valid semantic content. Use the exact nested shapes and enum values in the exemplar. The repair checklist is binding: `topology_lock` must be a non-empty array of decision-criteria strings, and every task `inputs` array must be non-empty with concrete paths, artifacts, or request facts that the executor can inspect. Do not leave either field empty. For `test.strategy = \"tdd\"`, `test.red` MUST be one object with `command`, `expected_observation`, and `evidence_path`, while `test.green` is an array of objects with those same three fields; do not use a bare string or one-element array. {}\n\nSchema exemplar:\n{PLAN_GRAPH_SCHEMA_EXEMPLAR}\n\nRust diagnostic:\n{}\n\nMalformed output to repair:\n{}\n\nOriginal goal:\n{}\n\nRepository discovery findings:\n{}\n\nIntentFold receipt:\n{}\n\nScope:\n{}\n\nVerification commands:\n{}",
        serde_json::to_string_pretty(diagnostic)?,
        raw_output,
        input.request,
        repository_discovery,
        input
            .intent_fold
            .as_ref()
            .map(serde_json::to_string_pretty)
            .transpose()?
            .unwrap_or_else(|| "none".to_string()),
        serde_json::to_string_pretty(&input.scope)?,
        serde_json::to_string_pretty(&input.verification_commands)?,
        planner_baseline_and_evidence_instructions(),
    );
    Ok(prompt.replace(
        "For `test.strategy = \"tdd\"`,",
        &format!(
            "{} For `test.strategy = \"tdd\"`,",
            revision_reference_path_instructions()
        ),
    ))
}

fn gear_opencode_repository_discovery_prompt(input: &IntentFoldInput) -> Result<String> {
    Ok(format!(
        "You are Gear's read-only repository discovery worker. This is the mandatory context-first wave before IntentFold and planning. Inspect the current workspace and gather concrete facts: repository layout, relevant files/symbols, existing implementation seams, applicable local rules, verification commands, constraints, risks, and unresolved unknowns. Use read-only lookup/explorer behavior only; do not edit, create, delete, or format files, do not run implementation commands, and do not write a plan. Return a concise findings report for the next Gear phases. Include exact paths and line/symbol references when available, distinguish observations from hypotheses, and state when a question is inconclusive.\n\nGoal id: {}\nRequest:\n{}\n\nScope:\n{}",
        input.goal_id,
        input.request,
        serde_json::to_string_pretty(&input.scope)?,
    ))
}

fn gear_opencode_intent_fold_prompt(
    input: &IntentFoldInput,
    repository_discovery: &RepositoryDiscoverySubmission,
) -> Result<String> {
    Ok(format!(
        "You are Gear's Metis-style read-only intent analyst. Do not plan tasks and do not write code. Return exactly one IntentFoldVerdict JSON object with no markdown fence or prose. Required shape: {{\"schema_version\":1,\"goal_id\":\"exact goal id\",\"normalized_objective\":\"clear outcome\",\"assumptions\":[\"explicit inference\"],\"constraints\":[\"binding boundary\"],\"ambiguities\":[\"remaining ambiguity\"],\"required_questions\":[\"only questions that change the solution\"],\"risks\":[{{\"code\":\"stable_code\",\"severity\":\"low|medium|high\",\"description\":\"specific risk\",\"mitigation\":\"specific mitigation\"}}],\"acceptance_signals\":[\"observable result\"],\"decision\":\"ready|needs_user\",\"summary\":\"concise conclusion\"}}. Use ready when the user has specified the behavior, scope, and acceptance. Gear owns runtime mechanics: evidence is stored under `.gear/runs/<run_id>/`, verification commands are supplied by Gear, and workspace scope is enforced before dispatch. Do not ask where these artifacts live, how to run commands, or how phases are sequenced. The caller has already approved the referenced frozen plan and its hash: never ask the user to reveal, relocate, or approve a plan merely because `.omo` or `.dogfood` is forbidden to this worker; record that access restriction as an assumption/risk and continue with ready. Use needs_user only for a real product or safety decision that repository inspection and the runtime contract cannot resolve. Treat the prior discovery findings as evidence, preserve concrete paths and constraints, and do not silently replace an inconclusive observation with an assumption.\n\nGoal id: {}\nRequest:\n{}\n\nRepository discovery findings (completed before this turn):\nartifact_path={}\n{}\n\nScope:\n{}",
        input.goal_id,
        input.request,
        repository_discovery.artifact_path,
        repository_discovery.raw_output,
        serde_json::to_string_pretty(&input.scope)?,
    ))
}

fn gear_opencode_intent_repair_prompt(
    input: &IntentFoldInput,
    raw_output: &str,
    attempt: usize,
) -> Result<String> {
    Ok(format!(
        "You are Gear's Metis-style intent analyst on fresh repair turn {attempt}. Return one complete IntentFoldVerdict JSON object only. Re-evaluate the request, preserving real product ambiguities, but do not ask the user about runtime-owned mechanics: Gear stores generated evidence under `.gear/runs/<run_id>/`, runs verification commands supplied below, and enforces the workspace scope. The caller has already approved the referenced frozen plan and its hash; never ask the user to reveal, relocate, or approve a plan merely because `.omo` or `.dogfood` is forbidden to this worker. Record that restriction as an assumption/risk and return `ready` when the requested behavior and scope are explicit. Ask a question only when the user must choose behavior, scope, destructive action, or acceptance semantics. If those are explicit, return `ready` with empty required_questions. Do not write files.\n\nOriginal request:\n{}\n\nScope:\n{}\n\nVerification commands:\n{}\n\nPrevious verdict:\n{}",
        input.request,
        serde_json::to_string_pretty(&input.scope)?,
        serde_json::to_string_pretty(&Vec::<String>::new())?,
        raw_output,
    ))
}

fn gear_opencode_intent_parse_repair_prompt(
    input: &IntentFoldInput,
    raw_output: &str,
    attempt: usize,
    parse_error: &str,
) -> Result<String> {
    let prompt = gear_opencode_intent_repair_prompt(input, raw_output, attempt)?;
    Ok(format!(
        "{prompt}\n\nStrict parser error from the previous turn:\n{parse_error}\n\
         Preserve the strict schema. Do not add fields outside IntentFoldVerdict."
    ))
}

fn intent_fold_model_label(decision: &PhaseRouteDecision) -> String {
    if let Some(model) = &decision.requested_model {
        return model.qualified_model_id();
    }
    match &decision.candidate.model {
        PhaseModelBinding::BackendDeclared(model) => {
            let worker = decision
                .worker_kind
                .as_ref()
                .map(WorkerKind::as_str)
                .unwrap_or("unknown");
            format!("{worker}/{model}")
        }
        _ => "unknown".to_string(),
    }
}

fn write_intent_fold_recovery_artifact(
    store: &StateStore,
    input: &IntentFoldInput,
    task_id: &str,
    model: &str,
    retry_count: usize,
    parse_error: &str,
    raw_output: &str,
    raw_output_path: Option<&str>,
    final_status: &str,
) -> Result<()> {
    let artifact = serde_json::json!({
        "schema_version": 1,
        "phase": "intent_fold",
        "goal_id": input.goal_id,
        "task_id": task_id,
        "model": model,
        "retry_count": retry_count,
        "parse_error": parse_error,
        "raw_output_sha256": format!("{:x}", Sha256::digest(raw_output.as_bytes())),
        "raw_output_path": raw_output_path,
        "final_status": final_status,
    });
    store.write_worker_file(
        task_id,
        "intent-fold-recovery.json",
        &format!("{}\n", serde_json::to_string_pretty(&artifact)?),
    )?;
    Ok(())
}

fn degraded_intent_fold_verdict(input: &IntentFoldInput, parse_error: &str) -> IntentFoldVerdict {
    let scope_summary = format!(
        "allowed_paths={} forbidden_paths={} max_files_changed={}",
        input.scope.allowed_paths.join(","),
        input.scope.forbidden_paths.join(","),
        input.scope.max_files_changed
    );
    IntentFoldVerdict {
        schema_version: crate::plan_review::PLAN_REVIEW_SCHEMA_VERSION,
        goal_id: input.goal_id.clone(),
        normalized_objective: input.request.clone(),
        assumptions: vec![
            "The original user request is the source of truth; model intent prose was unavailable after bounded repair.".to_string(),
        ],
        constraints: vec![scope_summary],
        ambiguities: vec![format!("IntentFold schema drift: {parse_error}")],
        required_questions: Vec::new(),
        risks: vec![IntentRisk {
            code: "intent_schema_degraded".to_string(),
            severity: IntentRiskSeverity::Medium,
            description: "The intent model did not produce a parseable envelope.".to_string(),
            mitigation: "Preserve the request, require a valid plan, deterministic preflight, and independent review before completion.".to_string(),
        }],
        acceptance_signals: vec![
            "A bounded plan and independent evidence-backed review must validate the requested outcome.".to_string(),
        ],
        decision: IntentFoldDecision::Ready,
        summary: "IntentFold degraded to the explicit request and scope after bounded schema repair; downstream hard evidence gates remain active.".to_string(),
    }
}

fn gear_opencode_plan_critic_prompt(input: &PlanCriticInput) -> Result<String> {
    let evidence = serde_json::to_string_pretty(&serde_json::json!({
        "request": input.request,
        "plan": input.plan,
        "planner_receipt": input.planner_receipt,
        "deterministic_verifier": input.verifier_report,
        "phase_route_decision": input.route_decision,
    }))?;
    Ok(format!(
        "You are Gear's independent read-only PlanCritic. Before writing any verdict, you MUST execute at least one read-only repository command such as `ls`, `pwd`, `rg`, `sed`, or `git status` against the supplied workspace and base the checks on that observation; a text-only response without a repository tool call is invalid. Return exactly one PlanCriticVerdict JSON object and no markdown fence. Use this exact top-level shape: {{\"schema_version\":1,\"reviewed_goal_id\":\"...\",\"reviewed_plan_id\":\"...\",\"reviewed_plan_revision\":0,\"reviewed_plan_hash\":\"...\",\"reviewed_planner_execution_id\":\"...\",\"decision\":\"approve|revise|reject\",\"checks\":[{{\"dimension\":\"references\",\"verdict\":\"pass|fail\",\"summary\":\"...\",\"evidence_refs\":[\"path-or-evidence-id\"]}}],\"findings\":[{{\"dimension\":\"scope\",\"severity\":\"blocking|advisory\",\"code\":\"...\",\"task_id\":null,\"path\":null,\"message\":\"...\",\"required_change\":null}}],\"revision_instructions\":null,\"needs_user_reason\":null,\"summary\":\"...\"}}. `checks` must be an array of exactly seven dimensions: references, executability, contradictions, scope, tdd, qa, acceptance. Every check MUST contain at least one non-empty `evidence_refs` entry citing the supplied evidence, including passing checks. `evidence_refs` belongs only inside checks. Findings must use the typed fields shown; never use a top-level `evidence_refs` or OMO's alternate finding shape. In executability and scope, verify every task is one independently verifiable work order, that `rationale` explains the concrete WHY, `approach` is a bounded HOW, `already_in_working_tree` contains facts rather than planned work, and that `still_needed` is the complete bounded remainder represented by must_do, execution_steps, artifacts, and completion_predicates. Flag tasks that mix implementation, review, documentation, or unrelated cleanup, but treat file boundaries as evidence and risk rather than an artificial exact-file rule. Approve only if all checks and deterministic verification pass. Revise must include blocking findings and concrete revision_instructions. Reject is only for a user decision and must set needs_user_reason. {} {}\n\nEvidence:\n{evidence}",
        plan_critic_dimension_instructions(),
        planner_baseline_and_evidence_instructions(),
    ))
}

fn gear_opencode_oracle_prompt(input: &PlanCriticInput) -> Result<String> {
    let evidence = serde_json::to_string_pretty(&serde_json::json!({
        "request": input.request,
        "plan": input.plan,
        "planner_receipt": input.planner_receipt,
        "deterministic_verifier": input.verifier_report,
        "phase_route_decision": input.route_decision,
    }))?;
    Ok(format!(
        "You are Gear's independent Oracle, in a fresh read-only session separate from Momus. Before writing any verdict, you MUST execute at least one read-only repository command such as `ls`, `pwd`, `rg`, `sed`, or `git status` against the supplied workspace and base the checks on that observation; a text-only response without a repository tool call is invalid. Re-read the exact plan and inspect every referenced repository path with available read/search tools before deciding. Do not write or edit files and do not trust claims that are not supported by the repository. Return exactly one PlanCriticVerdict JSON object with no markdown fence and use the typed shape from the PlanCritic contract: checks is an array of seven objects with dimension, verdict, summary, evidence_refs; every check must cite at least one non-empty evidence_refs entry, including passing checks. Findings use dimension, severity, code, task_id, path, message, required_change. Never put evidence_refs at the verdict or finding top level and never use OMO's alternate status/findings shape. Check references, executability, contradictions, scope, tdd, qa, and acceptance. In executability, verify each task states WHY in `rationale`, HOW in a bounded ordered `approach`, compare `already_in_working_tree` and `still_needed` against repository evidence, and ensure the work order is independently verifiable without silently adding skipped work. Return at most three actionable blocking findings; approve only when the plan is executable and evidence-backed. {} {}\n\nEvidence:\n{evidence}",
        plan_critic_dimension_instructions(),
        planner_baseline_and_evidence_instructions(),
    ))
}

fn gear_opencode_plan_revision_prompt(input: &PlanRevisionInput) -> Result<String> {
    let evidence = serde_json::to_string_pretty(&serde_json::json!({
        "request": input.request,
        "current_plan": input.plan,
        "planner_receipt": input.planner_receipt,
        "critic_receipt": input.critic_receipt,
        "phase_route_decision": input.route_decision,
    }))?;
    Ok(format!(
        "You are Gear's read-only planner revising a rejected plan. Apply every blocking required_change and revision_instructions without expanding scope. Preserve the plan's OMO work-order semantics: retain accurate `rationale` WHY and bounded `approach` HOW, retain accurate `already_in_working_tree`, rewrite `still_needed` to cover the entire bounded remainder, and keep each task independently verifiable. Do not hide work by moving it into generic must_do prose or by widening file scope. {} {} {} Return exactly one complete PlanGraphDraft JSON object and no markdown fence or prose.\n\nEvidence:\n{evidence}",
        revision_reference_path_instructions(),
        planner_baseline_and_evidence_instructions(),
        planner_revision_change_instructions()
    ))
}

fn bounded_revision_schema_hint() -> String {
    format!(
        "schema_exemplar_sha256={} (use the exact nested shape from current_plan; the full exemplar is intentionally omitted from this bounded repair prompt)",
        sha256_hex(PLAN_GRAPH_SCHEMA_EXEMPLAR)
    )
}

fn gear_opencode_plan_revision_repair_prompt(
    input: &PlanRevisionInput,
    raw_output: &str,
    error: &str,
    attempt: usize,
) -> Result<String> {
    let evidence = serde_json::to_string_pretty(&serde_json::json!({
        "request": input.request,
        "current_plan": input.plan,
        "critic_receipt": input.critic_receipt,
        "parser_error": error,
        "previous_output": raw_output,
    }))?;
    // The current plan above already carries the authoritative nested task
    // shape. Repeating the full schema exemplar on every repair turn can be
    // larger than the worker's context budget (especially after a malformed
    // model response is included). Keep only a hash-bound hint here; the raw
    // exemplar remains available in the repository and the full previous
    // output remains in the worker transcript/artifact.
    let schema_hint = bounded_revision_schema_hint();
    Ok(format!(
        "You are Gear's planner on bounded fresh revision-repair turn {attempt}. Return one complete PlanGraphDraft JSON object only, with no prose or markdown fence. Preserve every valid task and the current objective; apply only the critic's blocking changes. Do not add scope, silently remove completed work, or invent evidence. Use the exact nested shapes from the current plan and keep one independently verifiable work order per task. {} {} {}\n\nEvidence and previous output:\n{evidence}\n\nSchema hint (bounded):\n{schema_hint}",
        revision_reference_path_instructions(),
        planner_baseline_and_evidence_instructions(),
        planner_revision_change_instructions()
    ))
}

fn revision_reference_path_instructions() -> &'static str {
    "Repository artifact paths are exact, goal-scoped paths from the supplied evidence. Copy them character-for-character; never shorten a goal-specific path to a guessed generic alias such as `.gear/artifacts/repository-discovery.json`, invent a mirror, or claim a path exists without observing it."
}

fn planner_baseline_and_evidence_instructions() -> &'static str {
    "The workspace baseline may already contain framework-managed `.gear/` receipts and launcher stdout/stderr files. Never require `git status` to contain only the target file; compare the preimage/baseline observation with the final diff and reject only new unintended paths. Framework-managed evidence under `.gear/` is not an explicit task write and must not be counted against the user's `write_scope` or `max_files_changed`. For exact text, calculate UTF-8 byte length including the required newline instead of guessing. Every numeric byte claim in `still_needed`, `execution_steps`, tests, artifacts, and acceptance must agree with that calculation; distinguish source spelling from emitted bytes (for example, a shell `\\n` escape emits one LF byte, not two). Express the invariant as content bytes plus newline bytes equals total bytes, and never preserve contradictory totals just because they appear in an earlier draft. If the packet says a typecheck must not be skipped but the repository has no typecheck toolchain, acknowledge that constraint explicitly and use a bounded content/integrity check as the documented substitute; never silently claim that typecheck passed or silently omit the constraint."
}

fn plan_critic_dimension_instructions() -> &'static str {
    "Use exactly seven PlanCritic dimensions for every check and finding: `references`, `executability`, `contradictions`, `scope`, `tdd`, `qa`, `acceptance`. The deterministic verifier report uses a different vocabulary; never copy its `structure`, `reference_paths`, `test_contract`, `qa_contract`, or `acceptance_contract` labels into PlanCritic JSON. Map `structure` or byte-count inconsistencies to `contradictions`, `reference_paths` to `references`, `test_contract` to `tdd`, `qa_contract` to `qa`, and `acceptance_contract` to `acceptance`."
}

fn planner_revision_change_instructions() -> &'static str {
    "A revision is valid only when it changes the sealed PlanGraph content hash. Do not echo the current plan unchanged; apply at least one concrete critic-required change to the relevant task, evidence, dependency, scope, or acceptance field while preserving all unaffected work."
}

// ---------------------------------------------------------------------------
// Env helpers
// ---------------------------------------------------------------------------

/// Read OpenCode phase model profiles from environment variables.
///
/// Checks `GEARBOX_GEAR_OPENCODE_PHASES` for explicit enablement, then
/// `GEARBOX_GEAR_OPENCODE_PLANNER_MODEL`, `GEARBOX_GEAR_OPENCODE_EXECUTOR_MODEL`,
/// `GEARBOX_GEAR_OPENCODE_REVIEWER_MODEL`, with fallback to
/// `GEARBOX_GEAR_WORKER_MODEL`.
pub fn open_code_model_profiles_from_env() -> Result<Option<OpenCodeModelProfiles>> {
    let explicitly_enabled = trimmed_env_value("GEARBOX_GEAR_OPENCODE_PHASES")
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
    open_code_model_profiles_from_values(
        explicitly_enabled,
        trimmed_env_value("GEARBOX_GEAR_OPENCODE_PLANNER_MODEL"),
        trimmed_env_value("GEARBOX_GEAR_OPENCODE_EXECUTOR_MODEL"),
        trimmed_env_value("GEARBOX_GEAR_OPENCODE_REVIEWER_MODEL"),
        trimmed_env_value("GEARBOX_GEAR_WORKER_MODEL"),
    )
}

pub fn open_code_model_profiles_from_values(
    explicitly_enabled: bool,
    planner: Option<String>,
    executor: Option<String>,
    reviewer: Option<String>,
    default_worker_model: Option<String>,
) -> Result<Option<OpenCodeModelProfiles>> {
    let has_phase_model = planner.is_some() || executor.is_some() || reviewer.is_some();
    if !explicitly_enabled && !has_phase_model {
        return Ok(None);
    }
    let planner = planner.or(default_worker_model).context(
        "OpenCode phase mode requires GEARBOX_GEAR_OPENCODE_PLANNER_MODEL or GEARBOX_GEAR_WORKER_MODEL",
    )?;
    let profiles = OpenCodeModelProfiles {
        executor: executor.unwrap_or_else(|| planner.clone()),
        reviewer: reviewer.unwrap_or_else(|| planner.clone()),
        planner,
    };
    profiles.validate()?;
    Ok(Some(profiles))
}

fn trimmed_env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phase_routing::OpenCodeModelProfiles;
    use crate::plan_graph::{PLAN_GRAPH_SCHEMA_EXEMPLAR, deterministic_fallback_draft};
    use crate::state::Scope;
    use crate::workers::WorkerRegistry;

    #[test]
    fn extracts_nested_opencode_text_events_from_transcript_delta() {
        let model_json = r#"{"objective":"live","tasks":[]}"#;
        let nested = serde_json::json!({
            "assistant_text_delta": {
                "delta": serde_json::json!({
                    "type": "text",
                    "part": {"type": "text", "text": model_json}
                })
                .to_string()
            }
        });
        assert_eq!(extract_worker_text_events(&nested.to_string()), model_json);
    }

    #[test]
    fn planning_and_review_phases_always_use_read_only_route_policy() {
        assert_eq!(
            phase_worker_route_hint(&crate::state::TaskKind::Spec, "deep"),
            "explore"
        );
        assert_eq!(
            phase_worker_route_hint(&crate::state::TaskKind::Plan, "repair"),
            "explore"
        );
        assert_eq!(
            phase_worker_route_hint(&crate::state::TaskKind::Review, "deep"),
            "explore"
        );
        assert_eq!(
            phase_worker_route_hint(&crate::state::TaskKind::Edit, "deep"),
            "deep"
        );
    }

    #[test]
    fn planner_revision_repair_schema_hint_is_bounded() {
        let hint = bounded_revision_schema_hint();
        assert!(hint.len() < 512);
        assert!(hint.contains("schema_exemplar_sha256="));
        assert!(!hint.contains("\"tasks\""));
    }

    #[test]
    fn planner_revision_prompts_preserve_exact_artifact_paths() {
        let instructions = revision_reference_path_instructions();
        assert!(instructions.contains("goal-scoped paths"));
        assert!(instructions.contains("Copy them character-for-character"));
        assert!(instructions.contains("repository-discovery.json"));
        assert!(planner_revision_change_instructions().contains("content hash"));
    }

    #[test]
    fn planner_prompts_account_for_framework_baseline_and_exact_integrity_checks() {
        let instructions = planner_baseline_and_evidence_instructions();
        assert!(instructions.contains("framework-managed"));
        assert!(instructions.contains("preimage/baseline"));
        assert!(instructions.contains("UTF-8 byte"));
        assert!(instructions.contains("emitted bytes"));
        assert!(instructions.contains("contradictory totals"));
        assert!(instructions.contains("typecheck toolchain"));
    }

    #[test]
    fn plan_critic_prompts_map_verifier_dimensions_to_typed_contract() {
        let instructions = plan_critic_dimension_instructions();
        assert!(instructions.contains("exactly seven PlanCritic dimensions"));
        assert!(instructions.contains("structure"));
        assert!(instructions.contains("contradictions"));
        assert!(instructions.contains("reference_paths"));
        assert!(instructions.contains("references"));
        assert!(instructions.contains("test_contract"));
        assert!(instructions.contains("tdd"));
    }

    #[test]
    fn paid_phase_fallback_is_selected_only_for_provider_failures() -> Result<()> {
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "opencode/mimo-v2.5-free".to_string(),
            executor: "opencode/deepseek-v4-flash-free".to_string(),
            reviewer: "opencode/mimo-v2.5-free".to_string(),
        })?;
        let decision = routes.resolve(
            &PhaseProfile::Planner,
            &LiveModelInventory::default(),
            None,
        )?;
        let provider_error = anyhow::anyhow!("OpenCode Planner phase failed: provider error");
        let fallback = paid_fallback_decision(&decision, &provider_error)?
            .context("free route should have a paid fallback")?;
        assert_eq!(fallback.selected_candidate, 1);
        assert_eq!(fallback.rejected_candidates.len(), 1);
        assert_eq!(fallback.rejected_candidates[0].candidate_index, 0);
        assert_eq!(intent_fold_model_label(&fallback), "opencode_session/opencode-go/mimo-v2.5");
        assert!(is_provider_recoverable_phase_error(&provider_error));
        assert!(!is_provider_recoverable_phase_error(&anyhow::anyhow!(
            "planner schema is malformed"
        )));
        Ok(())
    }

    #[test]
    fn repository_observation_role_distinguishes_oracle_from_critic() {
        assert_eq!(
            phase_role_name_for_task(&PhaseProfile::PlanCritic, "plan_critic_goal_1_1"),
            "plan_critic"
        );
        assert_eq!(
            phase_role_name_for_task(&PhaseProfile::PlanCritic, "plan_oracle_goal_1_1"),
            "plan_oracle"
        );
        assert_eq!(
            phase_role_name_for_task(&PhaseProfile::PlanCritic, "plan_oracle_goal_1_1_repair_1"),
            "plan_oracle"
        );
    }

    #[test]
    fn repository_observation_uses_workdir_and_deduplicates_transport_wrappers() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        std::fs::write(workspace.path().join("README.md"), "fixture\n")?;
        let tool_use = serde_json::json!({
            "type": "tool_use",
            "part": {
                "type": "tool",
                "tool": "bash",
                "callID": "call-1",
                "state": {
                    "status": "completed",
                    "input": {
                        "command": "ls -la && head -5 README.md",
                        "workdir": workspace.path().to_string_lossy(),
                    }
                }
            }
        });
        let transcript = format!(
            "{}\n{}\n",
            serde_json::json!({
                "worker_stdout": {"kind": "run", "output": tool_use.to_string()}
            }),
            serde_json::json!({
                "assistant_text_delta": {"kind": "run", "delta": tool_use.to_string()}
            }),
        );

        let (tool_count, paths, events, call_ids) =
            collect_transcript_observations(workspace.path(), &transcript, "now");
        assert_eq!(tool_count, 1);
        assert_eq!(paths, vec![".".to_string()]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].operation, "list");
        assert_eq!(events[0].path, ".");
        assert_eq!(call_ids, vec!["call-1".to_string()]);
        Ok(())
    }

    fn test_worker_config() -> WorkerConfig {
        WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("sh -c 'echo test'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        }
    }

    #[test]
    fn factory_returns_phase_runtime_with_broker_factory() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let factory = OpenCodePhaseRuntimeFactory::new(
            temp_dir.path().to_path_buf(),
            test_worker_config(),
            broker_factory.clone(),
            CancellationToken::new(),
            routes,
            LiveModelInventory::default(),
        );
        let runtime = factory.build()?;
        assert!(runtime.broker_factory.is_some());
        assert!(runtime.intent_fold_hook.is_some());
        assert!(runtime.planner_hook.is_some());
        assert!(runtime.plan_critic_hook.is_some());
        assert!(runtime.oracle_hook.is_some());
        assert!(runtime.plan_revision_hook.is_some());
        assert!(runtime.strategist_next_goal_hook.is_some());
        Ok(())
    }

    #[test]
    fn phase_transcript_falls_back_to_worker_artifact_transcript() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let session_dir = temp_dir.path().join("broker-session");
        let result_path = temp_dir.path().join("workers/task/result.json");
        let worker_transcript = result_path
            .parent()
            .context("worker result path has no parent")?
            .join("transcript.jsonl");
        std::fs::create_dir_all(
            worker_transcript
                .parent()
                .context("missing transcript parent")?,
        )?;
        std::fs::write(&worker_transcript, "{}\n")?;
        assert_eq!(
            phase_worker_transcript_path(&session_dir, &result_path),
            worker_transcript
        );
        Ok(())
    }

    #[test]
    fn nested_opencode_transcript_delta_yields_read_observation() -> Result<()> {
        let nested = serde_json::json!({
            "type": "tool_use",
            "tool": "bash",
            "state": {
                "input": {
                    "command": "ls -la ./src/lib.rs"
                }
            }
        });
        let transcript_line = serde_json::json!({
            "assistant_text_delta": {
                "delta": serde_json::to_string(&nested)?
            }
        });
        let (operation, candidates, has_tool_marker) =
            transcript_tool_observation(&transcript_line);
        assert_eq!(operation.as_deref(), Some("list"));
        assert_eq!(candidates, vec!["./src/lib.rs"]);
        assert!(has_tool_marker);
        Ok(())
    }

    #[test]
    fn repository_observation_detects_wrapped_shell_commands() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let nested = serde_json::json!({
            "type": "tool_use",
            "tool": "bash",
            "part": {
                "callID": "call-cd-ls",
                "state": {
                    "input": {
                        "command": format!(
                            "cd {} && ls -la && wc -l README.md",
                            workspace.path().display()
                        )
                    }
                }
            }
        });
        let (tool_count, paths, events, call_ids) =
            collect_transcript_observations(workspace.path(), &nested.to_string(), "now");
        assert_eq!(tool_count, 1);
        assert_eq!(paths, vec![".".to_string()]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].operation, "list");
        assert_eq!(call_ids, vec!["call-cd-ls".to_string()]);
        Ok(())
    }

    #[test]
    fn intent_fold_repairs_runtime_owned_questions_on_a_fresh_turn() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let counter_path = temp_dir.path().join("intent-turn-count");
        let script_path = temp_dir.path().join("intent-worker.sh");
        std::fs::write(
            &script_path,
            format!(
                r#"#!/bin/sh
count=0
[ -f '{counter}' ] && count=$(cat '{counter}')
count=$((count + 1))
printf '%s' "$count" > '{counter}'
if [ "$count" -eq 2 ]; then printf '%s' '{{"schema_version":1,"goal_id":"intent_goal","normalized_objective":"outcome","required_questions":["where are artifacts?"],"decision":"needs_user","summary":"needs runtime clarification"}}' > "$GEARBOX_WORKER_LAST_MESSAGE"; else printf '%s' '{{"schema_version":1,"goal_id":"intent_goal","normalized_objective":"outcome","acceptance_signals":["verified"],"decision":"ready","summary":"ready"}}' > "$GEARBOX_WORKER_LAST_MESSAGE"; fi
"#,
                counter = counter_path.to_string_lossy(),
            ),
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let submission = runner.fold_intent(IntentFoldInput {
            goal_id: "intent_goal".to_string(),
            request: "produce the explicit outcome".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            route_decision: decision,
        })?;
        assert_eq!(
            submission.verdict.decision,
            crate::plan_review::IntentFoldDecision::Ready
        );
        assert_eq!(std::fs::read_to_string(counter_path)?, "3");
        Ok(())
    }

    #[test]
    fn repository_discovery_precedes_intent_fold_and_planner() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let counter_path = temp_dir.path().join("phase-turn-count");
        let prompt_log = temp_dir.path().join("phase-prompts.log");
        let policy_log = temp_dir.path().join("phase-policies.log");
        let output_path = temp_dir.path().join("valid-plan.json");
        std::fs::write(&output_path, PLAN_GRAPH_SCHEMA_EXEMPLAR)?;
        let script_path = temp_dir.path().join("discovery-worker.sh");
        std::fs::write(
            &script_path,
            format!(
                r#"#!/bin/sh
count=0
[ -f '{counter}' ] && count=$(cat '{counter}')
count=$((count + 1))
printf '%s' "$count" > '{counter}'
cat "$GEARBOX_WORKER_PROMPT" >> '{prompts}'
echo '---TURN---' >> '{prompts}'
printf '%s\n' "$GEARBOX_WORKER_TOOL_POLICY" >> '{policies}'
if [ "$count" -eq 1 ]; then printf '%s' 'discovered paths: src/main.rs; constraint: preserve the public API' > "$GEARBOX_WORKER_LAST_MESSAGE";
elif [ "$count" -eq 2 ]; then printf '%s' '{{"schema_version":1,"goal_id":"discovery_order_goal","normalized_objective":"outcome","acceptance_signals":["verified"],"decision":"ready","summary":"ready"}}' > "$GEARBOX_WORKER_LAST_MESSAGE";
else cp '{plan}' "$GEARBOX_WORKER_LAST_MESSAGE"; fi
"#,
                counter = counter_path.to_string_lossy(),
                prompts = prompt_log.to_string_lossy(),
                policies = policy_log.to_string_lossy(),
                plan = output_path.to_string_lossy(),
            ),
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let intent = runner.fold_intent(IntentFoldInput {
            goal_id: "discovery_order_goal".to_string(),
            request: "produce the explicit outcome".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            route_decision: decision.clone(),
        })?;
        let discovery = intent
            .repository_discovery
            .clone()
            .context("IntentFold must carry repository discovery evidence")?;
        assert_eq!(std::fs::read_to_string(&counter_path)?, "2");
        assert!(Path::new(&discovery.artifact_path).is_file());
        let discovery_artifact: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&discovery.artifact_path)?)?;
        assert_eq!(discovery_artifact["phase"], "repository_discovery");
        assert_eq!(
            discovery_artifact["task_id"],
            "repository_discovery_discovery_order_goal"
        );
        assert!(
            discovery_artifact["raw_output_sha256"]
                .as_str()
                .is_some_and(|hash| hash.len() == 64)
        );
        let receipt = crate::plan_review::IntentFoldReceipt::seal(
            intent.verdict.clone(),
            intent.analyst.clone(),
            &intent.raw_output,
            intent.artifact_path.clone(),
            timestamp(),
        )?;
        let planner = runner.plan(PlannerInput {
            goal_id: "discovery_order_goal".to_string(),
            request: "produce the explicit outcome".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            verification_commands: Vec::new(),
            route_decision: decision,
            intent_fold: Some(receipt),
            repository_discovery: Some(discovery),
        })?;
        assert_eq!(planner.draft.tasks.len(), 1);
        assert_eq!(std::fs::read_to_string(&counter_path)?, "3");
        let prompts = std::fs::read_to_string(prompt_log)?;
        assert!(prompts.contains("discovered paths: src/main.rs"));
        assert!(prompts.contains("Repository discovery (must precede planning)"));
        let policies = std::fs::read_to_string(policy_log)?;
        assert_eq!(policies.matches("\"can_write\":false").count(), 3);
        Ok(())
    }

    #[test]
    fn intent_fold_recovers_unknown_field_on_same_task_session() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let counter_path = temp_dir.path().join("intent-parse-turn-count");
        let script_path = temp_dir.path().join("intent-parse-worker.sh");
        std::fs::write(
            &script_path,
            format!(
                r#"#!/bin/sh
count=0
[ -f '{counter}' ] && count=$(cat '{counter}')
count=$((count + 1))
printf '%s' "$count" > '{counter}'
if [ "$count" -eq 2 ]; then printf '%s' '{{"schema_version":1,"goal_id":"intent_parse_goal","normalized_objective":"outcome","decision":"ready","summary":"invalid","write":true}}' > "$GEARBOX_WORKER_LAST_MESSAGE"; else printf '%s' '{{"schema_version":1,"goal_id":"intent_parse_goal","normalized_objective":"outcome","acceptance_signals":["verified"],"decision":"ready","summary":"ready"}}' > "$GEARBOX_WORKER_LAST_MESSAGE"; fi
"#,
                counter = counter_path.to_string_lossy(),
            ),
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let submission = runner.fold_intent(IntentFoldInput {
            goal_id: "intent_parse_goal".to_string(),
            request: "produce the explicit outcome".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            route_decision: decision,
        })?;

        assert_eq!(
            submission.verdict.decision,
            crate::plan_review::IntentFoldDecision::Ready
        );
        assert_eq!(std::fs::read_to_string(counter_path)?, "3");
        let worker_dir =
            StateStore::new(temp_dir.path()).worker_dir("intent_fold_intent_parse_goal");
        assert!(worker_dir.join("follow-up-1.md").is_file());
        assert!(worker_dir.join("transcript.jsonl").is_file());
        let recovery: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
            worker_dir.join("intent-fold-recovery.json"),
        )?)?;
        assert_eq!(recovery["final_status"], "recovered");
        assert_eq!(recovery["retry_count"], 1);
        assert!(
            recovery["parse_error"]
                .as_str()
                .is_some_and(|error| error.contains("unknown field"))
        );
        assert!(
            recovery["raw_output_sha256"]
                .as_str()
                .is_some_and(|hash| hash.len() == 64)
        );
        Ok(())
    }

    #[test]
    fn intent_fold_exhausted_parse_recovery_degrades_to_explicit_request() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let script_path = temp_dir.path().join("intent-parse-failing-worker.sh");
        std::fs::write(
            &script_path,
            r#"#!/bin/sh
printf '%s' '{"schema_version":1,"goal_id":"intent_exhausted_goal","normalized_objective":"outcome","decision":"ready","summary":"invalid","write":true}' > "$GEARBOX_WORKER_LAST_MESSAGE"
"#,
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let submission = runner.fold_intent(IntentFoldInput {
            goal_id: "intent_exhausted_goal".to_string(),
            request: "produce the explicit outcome".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            route_decision: decision,
        })?;
        assert_eq!(submission.verdict.decision, IntentFoldDecision::Ready);
        assert!(submission.verdict.summary.contains("degraded"));
        assert!(IntentFoldVerdict::parse(&submission.raw_output).is_ok());

        let worker_dir =
            StateStore::new(temp_dir.path()).worker_dir("intent_fold_intent_exhausted_goal");
        assert!(worker_dir.join("follow-up-1.md").is_file());
        let recovery: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
            worker_dir.join("intent-fold-recovery.json"),
        )?)?;
        assert_eq!(recovery["final_status"], "degraded_ready");
        assert_eq!(recovery["retry_count"], 2);
        assert!(
            recovery["raw_output_path"]
                .as_str()
                .is_some_and(|path| !path.is_empty())
        );
        Ok(())
    }

    #[test]
    fn runner_produces_independent_execution_identities_per_phase() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let output_path = temp_dir.path().join("output.json");
        std::fs::write(&output_path, PLAN_GRAPH_SCHEMA_EXEMPLAR)?;
        let command = format!(
            "sh -c 'cp {} \"$GEARBOX_WORKER_LAST_MESSAGE\"'",
            output_path.to_string_lossy()
        );
        let mut config = test_worker_config();
        config.worker_command = Some(command);

        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory: broker_factory.clone(),
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config.clone(),
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };

        let planner_decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let first = runner.plan(PlannerInput {
            goal_id: "goal_a".to_string(),
            request: "Build a plan".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            verification_commands: vec!["echo verify".to_string()],
            route_decision: planner_decision.clone(),
            intent_fold: None,
            repository_discovery: None,
        })?;
        let second = runner.plan(PlannerInput {
            goal_id: "goal_b".to_string(),
            request: "Build another plan".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            verification_commands: vec!["echo verify".to_string()],
            route_decision: planner_decision,
            intent_fold: None,
            repository_discovery: None,
        })?;

        // Two invocations must have independent execution identities.
        assert_ne!(
            first.planner.execution_id, second.planner.execution_id,
            "consecutive planner calls must not share execution_id"
        );
        assert_ne!(
            first.planner.phase_session_id, second.planner.phase_session_id,
            "consecutive planner calls must not share phase_session_id"
        );
        assert_ne!(
            first.planner.actual_session_id, second.planner.actual_session_id,
            "consecutive planner calls must not share actual_session_id"
        );

        // Model binding must be recorded.
        assert_eq!(first.planner.provider_id.as_deref(), Some("openai"));
        assert_eq!(first.planner.model_id.as_deref(), Some("gpt-planner"));

        Ok(())
    }

    #[test]
    fn planner_repairs_schema_drift_on_a_fresh_turn() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let output_path = temp_dir.path().join("valid-plan.json");
        let counter_path = temp_dir.path().join("turn-count");
        std::fs::write(&output_path, PLAN_GRAPH_SCHEMA_EXEMPLAR)?;
        let script_path = temp_dir.path().join("planner-worker.sh");
        std::fs::write(
            &script_path,
            format!(
                "#!/bin/sh\ncount=0\n[ -f '{counter}' ] && count=$(cat '{counter}')\ncount=$((count + 1))\nprintf '%s' \"$count\" > '{counter}'\nif [ \"$count\" -eq 1 ]; then printf '%s' '{{\\\"objective\\\":\\\"x\\\",\\\"topology_lock\\\":\\\"drift\\\",\\\"tasks\\\":[]}}' > \"$GEARBOX_WORKER_LAST_MESSAGE\"; else cp '{output}' \"$GEARBOX_WORKER_LAST_MESSAGE\"; fi\n",
                counter = counter_path.to_string_lossy(),
                output = output_path.to_string_lossy(),
            ),
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let submission = runner.plan(PlannerInput {
            goal_id: "repair_goal".to_string(),
            request: "repair a malformed draft".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            verification_commands: Vec::new(),
            route_decision: decision,
            intent_fold: None,
            repository_discovery: None,
        })?;
        assert_eq!(submission.draft.tasks.len(), 1);
        assert_eq!(std::fs::read_to_string(counter_path)?, "2");
        Ok(())
    }

    #[test]
    fn planner_repeated_schema_output_degrades_to_ordered_fallback() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let script_path = temp_dir.path().join("planner-repeated-worker.sh");
        std::fs::write(
            &script_path,
            "#!/bin/sh\nprintf '%s' '{\"malformed\":true}' > \"$GEARBOX_WORKER_LAST_MESSAGE\"\n",
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let submission = runner.plan(PlannerInput {
            goal_id: "repeated_schema_goal".to_string(),
            request: "execute ordered work orders and preserve a multi-node PlanGraph".to_string(),
            scope: Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4),
            verification_commands: Vec::new(),
            route_decision: decision,
            intent_fold: None,
            repository_discovery: None,
        })?;

        assert_eq!(submission.draft.tasks.len(), 2);
        assert_eq!(submission.draft.tasks[0].task_id, "task_001");
        assert!(submission.draft.tasks[0].dependencies.is_empty());
        assert_eq!(submission.draft.tasks[1].task_id, "task_002");
        assert_eq!(submission.draft.tasks[1].dependencies, vec!["task_001"]);
        assert_eq!(submission.draft.tasks[1].parallel_wave, 1);
        let degraded_path = temp_dir
            .path()
            .join(".gear/artifacts/repeated_schema_goal/planner-schema-degraded.json");
        assert!(degraded_path.is_file());
        let degraded = std::fs::read_to_string(degraded_path)?;
        assert!(degraded.contains("schema_degraded"));
        assert!(degraded.contains("repeated the same malformed output"));
        Ok(())
    }

    #[test]
    fn planner_repeated_semantic_output_degrades_to_ordered_fallback() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let script_path = temp_dir.path().join("planner-repeated-semantic-worker.sh");
        let mut semantic_invalid: serde_json::Value =
            serde_json::from_str(PLAN_GRAPH_SCHEMA_EXEMPLAR)?;
        semantic_invalid["tasks"] = serde_json::Value::Array(Vec::new());
        let semantic_invalid = serde_json::to_string(&semantic_invalid)?;
        std::fs::write(
            &script_path,
            format!(
                "#!/bin/sh\nprintf '%s' '{semantic_invalid}' > \"$GEARBOX_WORKER_LAST_MESSAGE\"\n"
            ),
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let submission = runner.plan(PlannerInput {
            goal_id: "repeated_semantic_goal".to_string(),
            request: "execute ordered work orders and preserve a multi-node PlanGraph".to_string(),
            scope: Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4),
            verification_commands: Vec::new(),
            route_decision: decision,
            intent_fold: None,
            repository_discovery: None,
        })?;

        assert_eq!(submission.draft.tasks.len(), 2);
        assert_eq!(submission.draft.tasks[0].task_id, "task_001");
        assert_eq!(submission.draft.tasks[1].dependencies, vec!["task_001"]);
        let degraded_path = temp_dir
            .path()
            .join(".gear/artifacts/repeated_semantic_goal/planner-schema-degraded.json");
        assert!(degraded_path.is_file());
        let degraded = std::fs::read_to_string(degraded_path)?;
        assert!(degraded.contains("schema_degraded"));
        assert!(degraded.contains("semantically invalid output"));
        Ok(())
    }

    #[test]
    fn planner_different_semantic_repairs_degrade_before_a_third_provider_turn() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let first_path = temp_dir.path().join("first-invalid.json");
        let second_path = temp_dir.path().join("second-invalid.json");
        let counter_path = temp_dir.path().join("turn-count");
        let mut first: serde_json::Value = serde_json::from_str(PLAN_GRAPH_SCHEMA_EXEMPLAR)?;
        first["topology_lock"] = serde_json::Value::Array(Vec::new());
        let mut second = first.clone();
        second["topology_lock"] = serde_json::json!(["task_a"]);
        second["tasks"][0]["inputs"] = serde_json::Value::Array(Vec::new());
        std::fs::write(&first_path, serde_json::to_string(&first)?)?;
        std::fs::write(&second_path, serde_json::to_string(&second)?)?;
        let script_path = temp_dir.path().join("planner-changing-semantic-worker.sh");
        std::fs::write(
            &script_path,
            format!(
                "#!/bin/sh\ncount=0\n[ -f '{counter}' ] && count=$(cat '{counter}')\ncount=$((count + 1))\nprintf '%s' \"$count\" > '{counter}'\nif [ \"$count\" -eq 1 ]; then cp '{first}' \"$GEARBOX_WORKER_LAST_MESSAGE\"; else cp '{second}' \"$GEARBOX_WORKER_LAST_MESSAGE\"; fi\n",
                counter = counter_path.to_string_lossy(),
                first = first_path.to_string_lossy(),
                second = second_path.to_string_lossy(),
            ),
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let submission = runner.plan(PlannerInput {
            goal_id: "changing_semantic_goal".to_string(),
            request: "execute one bounded work order".to_string(),
            scope: Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4),
            verification_commands: Vec::new(),
            route_decision: decision,
            intent_fold: None,
            repository_discovery: None,
        })?;

        assert_eq!(submission.draft.tasks.len(), 1);
        assert_eq!(std::fs::read_to_string(counter_path)?, "2");
        let degraded_path = temp_dir
            .path()
            .join(".gear/artifacts/changing_semantic_goal/planner-schema-degraded.json");
        assert!(degraded_path.is_file());
        let degraded = std::fs::read_to_string(degraded_path)?;
        assert!(degraded.contains("semantic contract remained invalid"));
        Ok(())
    }

    #[test]
    fn strategist_schema_drift_preserves_status_and_writes_degraded_receipt() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let scope = Scope::new(Vec::new(), Vec::new(), 1);
        let plan = crate::plan_graph::PlanGraph::seal(
            "strategist_goal",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            deterministic_fallback_draft("inspect the current state", &scope, &[]),
        )?;
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let route_decision = routes.resolve(
            &PhaseProfile::StrategistNextGoal,
            &LiveModelInventory::default(),
            None,
        )?;
        let runner = OpenCodePhaseRunner {
            broker_factory: Arc::new(PhaseBrokerFactory::new(
                Arc::new(WorkerRegistry::default()),
                temp_dir.path().join(".gearbox-agent"),
            )),
            workspace: temp_dir.path().to_path_buf(),
            worker_config: test_worker_config(),
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let input = StrategistNextGoalInput {
            goal_id: "strategist_goal".to_string(),
            epoch_id: "epoch_001".to_string(),
            request: "inspect the current state".to_string(),
            status: GoalStatus::Running,
            summary: "verification is still pending".to_string(),
            plan,
            final_report_path: ".gear/reports/final.md".to_string(),
            budget_ledger: crate::state::GoalBudgetLedger {
                schema_version: 1,
                goal_id: "strategist_goal".to_string(),
                reservations: Vec::new(),
                updated_at: "now".to_string(),
                ledger_hash: "unsealed-test-ledger".to_string(),
            },
            route_decision,
        };
        let output = OpenCodePhaseOutput {
            raw_output: "{malformed".to_string(),
            execution_identity: PhaseExecutionIdentity {
                execution_id: "strategist_execution".to_string(),
                phase_session_id: "strategist_session".to_string(),
                backend: crate::plan_review::PhaseExecutionBackend::WorkerSession,
                agent_id: Some("opencode".to_string()),
                provider_id: Some("opencode".to_string()),
                model_id: Some("mimo".to_string()),
                actual_session_id: Some("session".to_string()),
            },
            artifact_path: ".gear/workers/strategist/output.json".to_string(),
            repository_observation_path: None,
        };

        let submission = runner.degraded_strategist_submission(
            &input,
            &output,
            "strategist repeated the same malformed output",
        )?;
        assert_eq!(submission.verdict.decision, StrategistNextGoalDecision::NeedsUser);
        assert!(!submission.verdict.answerable_now);
        assert_eq!(submission.verdict.reviewed_status, GoalStatus::Running);
        let receipt = temp_dir
            .path()
            .join(".gear/artifacts/strategist_goal/strategist-schema-degraded.json");
        assert!(receipt.is_file());
        assert!(std::fs::read_to_string(receipt)?.contains("schema_degraded"));

        let mut completed_input = input;
        completed_input.status = GoalStatus::Complete;
        completed_input.final_report_path = ".gear/reports/missing-final.md".to_string();
        let completed_submission = runner.degraded_strategist_submission(
            &completed_input,
            &output,
            "strategist repeated the same malformed output",
        )?;
        assert_eq!(
            completed_submission.verdict.decision,
            StrategistNextGoalDecision::NeedsUser
        );
        assert!(!completed_submission.verdict.answerable_now);
        Ok(())
    }

    #[test]
    fn planner_recovers_only_the_current_worker_artifact() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let valid_plan_path = temp_dir.path().join("valid-plan.json");
        std::fs::write(&valid_plan_path, PLAN_GRAPH_SCHEMA_EXEMPLAR)?;
        let evidence_dir = temp_dir.path().join(".gear").join("evidence");
        std::fs::create_dir_all(&evidence_dir)?;
        let artifact_relative_path = PathBuf::from(".gear/evidence/planner-current-call.json");
        let artifact_path = temp_dir.path().join(&artifact_relative_path);
        let stale_path = evidence_dir.join("unrelated-stale-plan.json");
        std::fs::write(&stale_path, PLAN_GRAPH_SCHEMA_EXEMPLAR)?;
        let script_path = temp_dir.path().join("planner-artifact-worker.sh");
        std::fs::write(
            &script_path,
            format!(
                "#!/bin/sh\ncp '{valid_plan}' '{artifact}'\nprintf '%s' 'EVIDENCE_RECORDED: {artifact_relative}' > \"$GEARBOX_WORKER_LAST_MESSAGE\"\n",
                valid_plan = valid_plan_path.to_string_lossy(),
                artifact = artifact_path.to_string_lossy(),
                artifact_relative = artifact_relative_path.to_string_lossy(),
            ),
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let submission = runner.plan(PlannerInput {
            goal_id: "artifact_recovery_goal".to_string(),
            request: "recover the current planner artifact".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            verification_commands: Vec::new(),
            route_decision: decision,
            intent_fold: None,
            repository_discovery: None,
        })?;

        assert_eq!(submission.draft.tasks.len(), 1);
        assert_eq!(submission.artifact_path.as_deref(), artifact_path.to_str());
        Ok(())
    }

    #[test]
    fn planner_reuses_historical_multi_node_artifact_for_ordered_request() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let goal_id = "ordered_artifact_goal";
        let historical_dir = temp_dir
            .path()
            .join(".gear/workers")
            .join(format!("planner_{goal_id}_repair_1"));
        std::fs::create_dir_all(&historical_dir)?;
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let mut draft = deterministic_fallback_draft("execute ordered work orders", &scope, &[]);
        let mut second = draft.tasks[0].clone();
        draft.tasks[0].task_id = "task_001".to_string();
        draft.tasks[0].logical_task_id = Some("task_001".to_string());
        second.task_id = "task_002".to_string();
        second.logical_task_id = Some("task_002".to_string());
        second.parallel_wave = 1;
        draft.tasks.push(second);
        let historical_plan = historical_dir.join("plan-graph-draft.json");
        std::fs::write(&historical_plan, serde_json::to_string_pretty(&draft)?)?;

        let script_path = temp_dir.path().join("planner-history-worker.sh");
        std::fs::write(
            &script_path,
            "#!/bin/sh\nprintf '%s' '{\"malformed\":true}' > \"$GEARBOX_WORKER_LAST_MESSAGE\"\n",
        )?;
        let mut config = test_worker_config();
        config.worker_command = Some(format!("sh {}", script_path.to_string_lossy()));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let runner = OpenCodePhaseRunner {
            broker_factory,
            workspace: temp_dir.path().to_path_buf(),
            worker_config: config,
            cancellation_token: CancellationToken::new(),
            call_budget: PhaseCallBudget::default(),
        };
        let decision = routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let submission = runner.plan(PlannerInput {
            goal_id: goal_id.to_string(),
            request: "execute work orders in order; preserve multi-node PlanGraph".to_string(),
            scope,
            verification_commands: Vec::new(),
            route_decision: decision,
            intent_fold: None,
            repository_discovery: None,
        })?;

        assert_eq!(submission.draft.tasks.len(), 2);
        assert_eq!(submission.artifact_path.as_deref(), historical_plan.to_str());
        Ok(())
    }

    #[test]
    fn env_helper_falls_back_to_planner_when_executor_reviewer_unset() -> Result<()> {
        let profiles = open_code_model_profiles_from_values(
            true,
            Some("openai/gpt-4".to_string()),
            None,
            None,
            None,
        )?
        .context("profiles should be Some")?;
        assert_eq!(profiles.planner, "openai/gpt-4");
        assert_eq!(profiles.executor, "openai/gpt-4");
        assert_eq!(profiles.reviewer, "openai/gpt-4");
        Ok(())
    }

    #[test]
    fn env_helper_returns_none_when_not_enabled_and_no_models() -> Result<()> {
        let profiles = open_code_model_profiles_from_values(false, None, None, None, None)?;
        assert!(profiles.is_none());
        Ok(())
    }
}
