use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context as _, Result, bail};
use sha2::{Digest as _, Sha256};

use crate::phase_routing::{
    LiveModelInventory, OpenCodeModelProfiles, PhaseBackend, PhaseModelBinding, PhaseRouteDecision,
    PhaseRouteTable,
};
#[cfg(test)]
use crate::plan_graph::PhaseProfile;
use crate::plan_graph::{
    PLAN_GRAPH_SCHEMA_EXEMPLAR, PlannerParseDiagnostic, parse_planner_draft_diagnostic,
    parse_planner_draft_with_objective, validate_planner_draft,
};
use crate::plan_review::{IntentFoldVerdict, PhaseExecutionIdentity, PlanCriticVerdict};
use crate::runtime::{
    IntentFoldInput, IntentFoldSubmission, PhaseRuntime, PlanCriticInput, PlanCriticSubmission,
    PlanRevisionInput, PlanRevisionSubmission, PlannerInput, PlannerSubmission,
    RepositoryDiscoverySubmission, StrategistNextGoalInput, StrategistNextGoalSubmission,
    StrategistNextGoalVerdict,
};
use crate::state::{
    ModelCallKind, ModelCallLedgerEntry, RepositoryObservationEvent, RepositoryObservationReceipt,
    Scope, StateStore, Task, TaskInputs, TaskOutputs, TaskStatus, id_timestamp, timestamp,
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
    for (container, key) in [("worker_stdout", "output"), ("assistant_text_delta", "delta")]
    {
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
        let call_ordinal = self.call_budget.reserve(goal_id)?;
        if !matches!(
            decision.candidate.backend,
            PhaseBackend::Worker(WorkerKind::OpencodeSession) | PhaseBackend::CodexAcp
        ) {
            bail!("Gear phase runner received a non-OpenCode/Codex route");
        }
        let config = decision.overlay_worker_config(&self.worker_config)?;
        // Spec phases are read-only discovery/interpretation turns. Keep the
        // route decision's model binding, but use the Explore policy so the
        // worker cannot write while gathering context or folding intent.
        let phase_route_hint = if matches!(task_kind, crate::state::TaskKind::Spec) {
            Some("explore")
        } else {
            Some(decision.category.as_str())
        };
        let store = StateStore::new(&self.workspace);
        store.initialize()?;
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
        let execution = self.broker_factory.execute_worker_phase_with_follow_up(
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
        )?;
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
                phase_role_name(&decision.phase),
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
                                "failed",
                            )?;
                            bail!(
                                "intent fold strict parse failed after {} recovery attempts; diagnostic: {}/{}: {}",
                                follow_up_index,
                                task_id,
                                "intent-fold-recovery.json",
                                parse_error
                            );
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
        Ok(IntentFoldSubmission {
            verdict,
            analyst: output.execution_identity,
            raw_output: output.raw_output,
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
        for repair_attempt in 0..=MAX_PLANNER_SCHEMA_REPAIRS {
            match parse_planner_draft_diagnostic(&output.raw_output) {
                Ok(draft) => {
                    if let Err(error) = validate_planner_draft(&input.goal_id, &draft) {
                        if repair_attempt >= MAX_PLANNER_SCHEMA_REPAIRS {
                            bail!(
                                "planner contract repair exhausted after {} attempts: {}",
                                MAX_PLANNER_SCHEMA_REPAIRS,
                                error
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
                            bail!(
                                "planner repeated the same semantically invalid output: {}",
                                serde_json::to_string(&diagnostic)?
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
                        bail!(
                            "planner repeated the same malformed output; schema diagnostic: {}",
                            serde_json::to_string(&diagnostic)?
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
                    bail!(
                        "planner schema repair exhausted after {} attempts: {}",
                        MAX_PLANNER_SCHEMA_REPAIRS,
                        serde_json::to_string(&diagnostic)?
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
        let candidates = [reported_artifact_path, conventional_artifact_path];
        let Some((artifact_path, contents, draft)) = candidates.into_iter().find_map(|path| {
            let contents = std::fs::read_to_string(&path).ok()?;
            let draft = parse_planner_draft_with_objective(&contents, &input.request).ok()?;
            validate_planner_draft(&input.goal_id, &draft).ok()?;
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
        for repair_attempt in 0..=MAX_REVIEW_SCHEMA_REPAIRS {
            match PlanCriticVerdict::parse(&output.raw_output) {
                Ok(verdict) => {
                    return Ok(PlanCriticSubmission {
                        reviewer: output.execution_identity,
                        verdict,
                        raw_output: output.raw_output,
                        artifact_path: Some(output.artifact_path),
                        repository_evidence_path: output.repository_observation_path,
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
                    bail!(
                        "{task_prefix} schema repair exhausted after {} attempt(s): {}",
                        MAX_REVIEW_SCHEMA_REPAIRS,
                        error
                    );
                }
            }
        }
        bail!("{task_prefix} review repair loop terminated unexpectedly")
    }

    pub fn revise(&self, input: PlanRevisionInput) -> Result<PlanRevisionSubmission> {
        let prompt = gear_opencode_plan_revision_prompt(&input)?;
        let task_id = format!(
            "planner_revision_{}_{}",
            input.plan.goal_id, input.plan.revision
        );
        let output = self.run(
            &input.route_decision,
            &input.plan.goal_id,
            &input.plan.plan_id,
            input.plan.revision,
            Some(&input.plan.plan_hash),
            &task_id,
            crate::state::TaskKind::Plan,
            Scope::new(Vec::new(), Vec::new(), 1),
            prompt,
        )?;
        let draft =
            parse_planner_draft_with_objective(&output.raw_output, &input.plan.draft.objective)?;
        Ok(PlanRevisionSubmission {
            draft,
            planner: output.execution_identity,
            raw_output: output.raw_output,
            artifact_path: Some(output.artifact_path),
        })
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
        let verdict = StrategistNextGoalVerdict::parse(&output.raw_output)?;
        Ok(StrategistNextGoalSubmission {
            verdict,
            strategist: output.execution_identity,
            raw_output: output.raw_output,
            artifact_path: Some(output.artifact_path),
        })
    }
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
    let (transcript_sha256, observed_tool_count, observed_paths, observation_events) =
        if transcript_path.is_file() {
            let contents = std::fs::read_to_string(&transcript_path)?;
            let mut tool_count = 0usize;
            let mut paths = std::collections::BTreeSet::new();
            let mut events = Vec::new();
            for line in contents.lines().filter(|line| !line.trim().is_empty()) {
                let value = serde_json::from_str::<serde_json::Value>(line).ok();
                let serialized = value
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| line.to_string());
                if let Some(value) = value.as_ref() {
                    let (operation, candidates, has_tool_marker) =
                        transcript_tool_observation(value);
                    if has_tool_marker
                        || serialized.contains("tool_name")
                        || serialized.contains("\"tool\"")
                    {
                        tool_count = tool_count.saturating_add(1);
                    }
                    if has_tool_marker {
                        let event_hash = sha256_hex(line);
                        if let Some(operation) = operation {
                            if let Some(path) = candidates.into_iter().find_map(|candidate| {
                                workspace_relative_observation_path(workspace, &candidate)
                            }) {
                                paths.insert(path.clone());
                                events.push(RepositoryObservationEvent {
                                    operation,
                                    path,
                                    event_id: format!("tool_{}", &event_hash[..16]),
                                    event_hash,
                                    observed_at: finished_at.clone(),
                                });
                            }
                        }
                    }
                }
            }
            (
                Some(sha256_hex(&contents)),
                tool_count,
                paths.into_iter().collect(),
                events,
            )
        } else {
            (None, 0, Vec::new(), Vec::new())
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
                "path" | "filePath" | "file_path" | "directory" | "dir"
            ) {
                if let Some(path) = child.as_str() {
                    candidates.push(path.to_string());
                }
            }
            if key == "command"
                && let Some(command) = child.as_str()
            {
                let command_name = command.split_whitespace().next().unwrap_or_default();
                operation = operation.or_else(|| observation_operation(command_name));
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
    (!relative.is_empty() && relative != ".." && !relative.starts_with("../")).then_some(relative)
}

fn gear_opencode_review_repair_prompt(
    input: &PlanCriticInput,
    role: &str,
    raw_output: &str,
    error: &str,
    attempt: usize,
) -> Result<String> {
    Ok(format!(
        "You are the same Gear {role} reviewer on bounded fresh repair turn {attempt}. Output ONLY one JSON object. Do not use `status`, `verdict` at the top level, `goal_id`, `plan_id`, `revision`, `plan_hash`, or an object/map for `checks`. `checks` MUST be an array of exactly seven objects. `evidence_refs` is allowed only inside each check; never put it at the verdict or finding top level. Every finding MUST use the typed fields `dimension`, `severity`, `code`, `task_id`, `path`, `message`, and `required_change`; do not use OMO's alternate `summary`/`evidence_refs` finding shape. Convert the previous answer into this exact skeleton, preserving its meaning: {{\"schema_version\":1,\"reviewed_goal_id\":\"{goal}\",\"reviewed_plan_id\":\"{plan}\",\"reviewed_plan_revision\":{revision},\"reviewed_plan_hash\":\"{hash}\",\"reviewed_planner_execution_id\":\"{planner}\",\"decision\":\"approve|revise|reject\",\"checks\":[{{\"dimension\":\"references\",\"verdict\":\"pass|fail\",\"summary\":\"...\",\"evidence_refs\":[]}}],\"findings\":[{{\"dimension\":\"scope\",\"severity\":\"blocking|advisory\",\"code\":\"scope_contradiction\",\"task_id\":null,\"path\":null,\"message\":\"...\",\"required_change\":\"...\"}}],\"revision_instructions\":null,\"needs_user_reason\":null,\"summary\":\"...\"}}. Each finding severity is only `blocking` or `advisory`; each check verdict is only `pass` or `fail`.\n\nRust parse error:\n{error}\n\nPrevious invalid output:\n{raw_output}",
        goal = input.plan.goal_id,
        plan = input.plan.plan_id,
        revision = input.plan.revision,
        hash = input.plan.plan_hash,
        planner = input.planner_receipt.identity.execution_id,
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
    Ok(format!(
        "You are Gear's read-only planner. Return exactly one PlanGraphDraft JSON object with no markdown fence or prose. The top-level `objective` string is mandatory; never omit it. Do not rename fields, replace arrays with strings or objects, or use prose values for enums. The complete nested contract exemplar is below; copy its shapes and use only the enum values shown. Every task must define task_id, title, goal, deliverable, rationale, approach, already_in_working_tree, still_needed, dependencies, parallel_wave, scope, required_capabilities, preferred_phase_profile, inputs, preconditions, must_do, execution_steps, must_not_do, references, test, qa, artifacts, evidence, rollback, budget, commit_boundary, commit_message, and completion_predicates. `rationale` is the concrete WHY from OMO; `approach` is an ordered bounded HOW, not a second list of generic must_do items. `already_in_working_tree` must state concrete facts already present before this work order; `still_needed` must contain only the independently verifiable remainder, matching OMO's work-order format. One task must represent one independently verifiable objective; split unrelated behavior, review, documentation, and cleanup into separate work orders even when they touch nearby files. `inputs` and `preconditions` are checked before editing; `execution_steps` must be ordered and each step must include step_id, action, expected_observation, and optional evidence_path; the executor must stop on an unmet step instead of skipping ahead or redesigning the plan. `evidence` records observable proof obligations separately from changed-file deliverables. `rollback` describes the bounded recovery action and `budget` gives optional task limits; neither may be omitted when the task has irreversible or expensive work. `commit_message` is optional for no-commit tasks, but when present it must be a concrete OMO-style commit intent; Gear never commits or pushes automatically. Dependencies must point to earlier waves. TDD tasks must use the same RED and GREEN command. Include concrete happy, failure, and adversarial QA scenarios; when adversarial behavior does not apply, record an explicit not-applicable trigger check and evidence path. Treat the sealed repository discovery findings and IntentFold receipt as binding context: preserve discovered constraints, cite relevant paths, mitigate risks, and turn acceptance signals into executable checks. Do not write code.\n\nSchema exemplar:\n{}\n\nGoal:\n{}\n\nRepository discovery (must precede planning):\n{}\n\nIntentFold receipt:\n{}\n\nScope:\n{}\n\nVerification commands:\n{}",
        PLAN_GRAPH_SCHEMA_EXEMPLAR,
        input.request,
        repository_discovery,
        intent_fold,
        serde_json::to_string_pretty(&input.scope)?,
        serde_json::to_string_pretty(&input.verification_commands)?,
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
    Ok(format!(
        "You are the same Gear planner on fresh repair turn {attempt}. Return a complete PlanGraphDraft JSON object only; never return a patch, prose, or markdown fence. Preserve the request, repository discovery findings, and IntentFold semantics. Correct only the schema errors identified by Rust and keep all valid semantic content. Use the exact nested shapes and enum values in the exemplar.\n\nSchema exemplar:\n{PLAN_GRAPH_SCHEMA_EXEMPLAR}\n\nRust diagnostic:\n{}\n\nMalformed output to repair:\n{}\n\nOriginal goal:\n{}\n\nRepository discovery findings:\n{}\n\nIntentFold receipt:\n{}\n\nScope:\n{}\n\nVerification commands:\n{}",
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

fn gear_opencode_plan_critic_prompt(input: &PlanCriticInput) -> Result<String> {
    let evidence = serde_json::to_string_pretty(&serde_json::json!({
        "request": input.request,
        "plan": input.plan,
        "planner_receipt": input.planner_receipt,
        "deterministic_verifier": input.verifier_report,
        "phase_route_decision": input.route_decision,
    }))?;
    Ok(format!(
        "You are Gear's independent read-only PlanCritic. Return exactly one PlanCriticVerdict JSON object and no markdown fence. Use this exact top-level shape: {{\"schema_version\":1,\"reviewed_goal_id\":\"...\",\"reviewed_plan_id\":\"...\",\"reviewed_plan_revision\":0,\"reviewed_plan_hash\":\"...\",\"reviewed_planner_execution_id\":\"...\",\"decision\":\"approve|revise|reject\",\"checks\":[{{\"dimension\":\"references\",\"verdict\":\"pass|fail\",\"summary\":\"...\",\"evidence_refs\":[]}}],\"findings\":[{{\"dimension\":\"scope\",\"severity\":\"blocking|advisory\",\"code\":\"...\",\"task_id\":null,\"path\":null,\"message\":\"...\",\"required_change\":null}}],\"revision_instructions\":null,\"needs_user_reason\":null,\"summary\":\"...\"}}. `checks` must be an array of exactly seven dimensions: references, executability, contradictions, scope, tdd, qa, acceptance. `evidence_refs` belongs only inside checks. Findings must use the typed fields shown; never use a top-level `evidence_refs` or OMO's alternate finding shape. In executability and scope, verify every task is one independently verifiable work order, that `rationale` explains the concrete WHY, `approach` is a bounded HOW, `already_in_working_tree` contains facts rather than planned work, and that `still_needed` is the complete bounded remainder represented by must_do, execution_steps, artifacts, and completion_predicates. Flag tasks that mix implementation, review, documentation, or unrelated cleanup, but treat file boundaries as evidence and risk rather than an artificial exact-file rule. Approve only if all checks and deterministic verification pass. Revise must include blocking findings and concrete revision_instructions. Reject is only for a user decision and must set needs_user_reason.\n\nEvidence:\n{evidence}"
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
        "You are Gear's independent Oracle, in a fresh read-only session separate from Momus. Re-read the exact plan and inspect every referenced repository path with available read/search tools before deciding. Do not write or edit files and do not trust claims that are not supported by the repository. Return exactly one PlanCriticVerdict JSON object with no markdown fence and use the typed shape from the PlanCritic contract: checks is an array of seven objects with dimension, verdict, summary, evidence_refs; findings use dimension, severity, code, task_id, path, message, required_change. Never put evidence_refs at the verdict or finding top level and never use OMO's alternate status/findings shape. Check references, executability, contradictions, scope, tdd, qa, and acceptance. In executability, verify each task states WHY in `rationale`, HOW in a bounded ordered `approach`, compare `already_in_working_tree` and `still_needed` against repository evidence, and ensure the work order is independently verifiable without silently adding skipped work. Return at most three actionable blocking findings; approve only when the plan is executable and evidence-backed.\n\nEvidence:\n{evidence}"
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
        "You are Gear's read-only planner revising a rejected plan. Apply every blocking required_change and revision_instructions without expanding scope. Preserve the plan's OMO work-order semantics: retain accurate `rationale` WHY and bounded `approach` HOW, retain accurate `already_in_working_tree`, rewrite `still_needed` to cover the entire bounded remainder, and keep each task independently verifiable. Do not hide work by moving it into generic must_do prose or by widening file scope. Return exactly one complete PlanGraphDraft JSON object and no markdown fence or prose.\n\nEvidence:\n{evidence}"
    ))
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
    use crate::plan_graph::PLAN_GRAPH_SCHEMA_EXEMPLAR;
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
        assert_eq!(policies.matches("\"can_write\":false").count(), 2);
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
    fn intent_fold_exhausted_parse_recovery_writes_failure_diagnostic() -> Result<()> {
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
        let error = runner
            .fold_intent(IntentFoldInput {
                goal_id: "intent_exhausted_goal".to_string(),
                request: "produce the explicit outcome".to_string(),
                scope: Scope::new(Vec::new(), Vec::new(), 1),
                route_decision: decision,
            })
            .expect_err("malformed output must not become an IntentFold submission");
        assert!(error.to_string().contains("strict parse failed"));

        let worker_dir =
            StateStore::new(temp_dir.path()).worker_dir("intent_fold_intent_exhausted_goal");
        assert!(worker_dir.join("follow-up-1.md").is_file());
        let recovery: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
            worker_dir.join("intent-fold-recovery.json"),
        )?)?;
        assert_eq!(recovery["final_status"], "failed");
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
