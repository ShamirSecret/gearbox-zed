use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};

use crate::phase_routing::{
    LiveModelInventory, OpenCodeModelProfiles, PhaseBackend, PhaseRouteDecision, PhaseRouteTable,
};
#[cfg(test)]
use crate::plan_graph::PhaseProfile;
use crate::plan_graph::{
    PLAN_GRAPH_SCHEMA_EXEMPLAR, PlannerParseDiagnostic, parse_planner_draft,
    parse_planner_draft_diagnostic, validate_planner_draft,
};
use crate::plan_review::{IntentFoldVerdict, PhaseExecutionIdentity, PlanCriticVerdict};
use crate::runtime::{
    IntentFoldInput, IntentFoldSubmission, PhaseRuntime, PlanCriticInput, PlanCriticSubmission,
    PlanRevisionInput, PlanRevisionSubmission, PlannerInput, PlannerSubmission,
    StrategistNextGoalInput, StrategistNextGoalSubmission, StrategistNextGoalVerdict,
};
use crate::state::{Scope, StateStore, Task, TaskInputs, TaskOutputs, TaskStatus, id_timestamp};
use crate::task_manager::{
    ManagedTaskStatus, ResidencyState, TaskAttempt, TaskAttemptStatus, TaskRecord,
};
use crate::tools::CancellationToken;
use crate::worker_broker::PhaseBrokerFactory;
use crate::workers::{WorkerConfig, WorkerKind, WorkerStartRequest, WorkerStatus};

/// Builder for a production `PhaseRuntime` that routes all planning and review
/// phases through independent OpenCode session workers.
///
/// Each phase (IntentFold, Planner, PlanCritic, PlanRevision, Strategist)
/// receives its own `execution_id`, `session_id`, and `task_id`.  Phases
/// never share an actual worker session.
pub struct OpenCodePhaseRuntimeFactory {
    workspace: PathBuf,
    worker_config: WorkerConfig,
    broker_factory: Arc<PhaseBrokerFactory>,
    cancellation_token: CancellationToken,
    phase_route_table: PhaseRouteTable,
    inventory: LiveModelInventory,
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
        };
        let planner_runner = OpenCodePhaseRunner {
            workspace: workspace.clone(),
            worker_config: worker_config.clone(),
            broker_factory: broker_factory.clone(),
            cancellation_token: cancellation_token.clone(),
        };
        let critic_runner = OpenCodePhaseRunner {
            workspace: workspace.clone(),
            worker_config: worker_config.clone(),
            broker_factory: broker_factory.clone(),
            cancellation_token: cancellation_token.clone(),
        };
        let oracle_runner = OpenCodePhaseRunner {
            workspace: workspace.clone(),
            worker_config: worker_config.clone(),
            broker_factory: broker_factory.clone(),
            cancellation_token: cancellation_token.clone(),
        };
        let revision_runner = OpenCodePhaseRunner {
            workspace: workspace.clone(),
            worker_config: worker_config.clone(),
            broker_factory: broker_factory.clone(),
            cancellation_token: cancellation_token.clone(),
        };
        let strategist_runner = OpenCodePhaseRunner {
            workspace,
            worker_config,
            broker_factory: broker_factory.clone(),
            cancellation_token,
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
}

const MAX_PLANNER_SCHEMA_REPAIRS: usize = 2;
const MAX_INTENT_REPAIRS: usize = 1;

struct OpenCodePhaseOutput {
    raw_output: String,
    execution_identity: PhaseExecutionIdentity,
    artifact_path: String,
}

impl OpenCodePhaseRunner {
    fn run(
        &self,
        decision: &PhaseRouteDecision,
        goal_id: &str,
        plan_id: &str,
        plan_revision: usize,
        task_id: &str,
        task_kind: crate::state::TaskKind,
        scope: Scope,
        prompt: String,
    ) -> Result<OpenCodePhaseOutput> {
        if !matches!(
            decision.candidate.backend,
            PhaseBackend::Worker(WorkerKind::OpencodeSession)
        ) {
            bail!("Gear OpenCode phase runner received a non-OpenCode route");
        }
        let config = decision.overlay_worker_config(&self.worker_config)?;
        let phase_route_hint = match &task_kind {
            crate::state::TaskKind::Review => Some("review"),
            crate::state::TaskKind::Spec | crate::state::TaskKind::Plan => Some("explore"),
            _ => None,
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
        let execution = self.broker_factory.execute_worker_phase(
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
        )?;
        if execution.result.status != WorkerStatus::Succeeded {
            bail!(
                "OpenCode {:?} phase failed: {}",
                decision.phase,
                execution.result.summary
            );
        }
        write_phase_task_record(&store, &task, &config, phase_route_hint, &execution)?;
        let raw_output = execution
            .result
            .last_message_path
            .as_ref()
            .filter(|path| path.is_file())
            .or_else(|| {
                execution
                    .result
                    .stdout_path
                    .as_ref()
                    .filter(|path| path.is_file())
            })
            .map(std::fs::read_to_string)
            .transpose()?
            .unwrap_or_else(|| execution.result.summary.clone());
        let raw_output = raw_output.trim().to_string();
        if raw_output.is_empty() {
            bail!(
                "OpenCode {:?} phase returned an empty response",
                decision.phase
            );
        }
        Ok(OpenCodePhaseOutput {
            raw_output,
            execution_identity: execution.execution_identity,
            artifact_path: execution.result.result_path.to_string_lossy().to_string(),
        })
    }

    pub fn fold_intent(&self, input: IntentFoldInput) -> Result<IntentFoldSubmission> {
        let prompt = gear_opencode_intent_fold_prompt(&input)?;
        let mut output = self.run(
            &input.route_decision,
            &input.goal_id,
            &format!("pending_{}", input.goal_id),
            0,
            &format!("intent_fold_{}", input.goal_id),
            crate::state::TaskKind::Spec,
            input.scope.clone(),
            prompt,
        )?;
        for repair_attempt in 0..=MAX_INTENT_REPAIRS {
            let verdict = IntentFoldVerdict::parse(&output.raw_output)?;
            let requires_repair = verdict.decision
                == crate::plan_review::IntentFoldDecision::NeedsUser
                || !verdict.required_questions.is_empty();
            if !requires_repair || repair_attempt >= MAX_INTENT_REPAIRS {
                return Ok(IntentFoldSubmission {
                    verdict,
                    analyst: output.execution_identity,
                    raw_output: output.raw_output,
                    artifact_path: Some(output.artifact_path),
                });
            }
            let repair_prompt =
                gear_opencode_intent_repair_prompt(&input, &output.raw_output, repair_attempt + 1)?;
            output = self.run(
                &input.route_decision,
                &input.goal_id,
                &format!("pending_{}", input.goal_id),
                0,
                &format!(
                    "intent_fold_{}_repair_{}",
                    input.goal_id,
                    repair_attempt + 1
                ),
                crate::state::TaskKind::Spec,
                input.scope.clone(),
                repair_prompt,
            )?;
        }
        bail!("intent fold repair loop terminated unexpectedly")
    }

    pub fn plan(&self, input: PlannerInput) -> Result<PlannerSubmission> {
        let prompt = gear_opencode_planner_prompt(&input)?;
        let mut output = self.run(
            &input.route_decision,
            &input.goal_id,
            &format!("pending_{}", input.goal_id),
            0,
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
                    });
                }
                Err(diagnostic) if repair_attempt < MAX_PLANNER_SCHEMA_REPAIRS => {
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
                        &format!("planner_{}_repair_{}", input.goal_id, repair_attempt + 1),
                        crate::state::TaskKind::Plan,
                        input.scope.clone(),
                        repair_prompt,
                    )?;
                }
                Err(diagnostic) => {
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
        let output = self.run(
            &input.route_decision,
            &input.plan.goal_id,
            &input.plan.plan_id,
            input.plan.revision,
            &task_id,
            crate::state::TaskKind::Review,
            Scope::new(Vec::new(), Vec::new(), 1),
            prompt,
        )?;
        let verdict = PlanCriticVerdict::parse(&output.raw_output)?;
        Ok(PlanCriticSubmission {
            reviewer: output.execution_identity,
            verdict,
            raw_output: output.raw_output,
            artifact_path: Some(output.artifact_path),
        })
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
            &task_id,
            crate::state::TaskKind::Plan,
            Scope::new(Vec::new(), Vec::new(), 1),
            prompt,
        )?;
        let draft = parse_planner_draft(&output.raw_output)?;
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

fn sha256_hex(value: &str) -> String {
    use sha2::{Digest as _, Sha256};
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

// ---------------------------------------------------------------------------
// Prompt builders
// ---------------------------------------------------------------------------

fn gear_opencode_strategist_prompt(input: &StrategistNextGoalInput) -> Result<String> {
    Ok(format!(
        "You are Gearbox StrategistNextGoal. Review the completed execution epoch and return only one strict JSON object.\n\
Schema: {{\"schema_version\":1,\"goal_id\":string,\"epoch_id\":string,\"reviewed_status\":\"draft|planning|running|verifying|needs_user|blocked|limited|complete|failed\",\"decision\":\"complete|continue|needs_user|stop\",\"next_objective\":string|null,\"acceptance_signals\":[string],\"required_questions\":[string],\"evidence_refs\":[string],\"rationale\":string}}.\n\
Use continue only for a bounded next objective consistent with the original request. Do not propose an unbounded loop. Use complete only when reviewed_status is complete.\n\
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
    Ok(format!(
        "You are Gear's read-only planner. Return exactly one PlanGraphDraft JSON object with no markdown fence or prose. Do not rename fields, replace arrays with strings or objects, or use prose values for enums. The complete nested contract exemplar is below; copy its shapes and use only the enum values shown. Every task must define task_id, title, goal, deliverable, dependencies, parallel_wave, scope, required_capabilities, preferred_phase_profile, must_do, must_not_do, references, test, qa, artifacts, commit_boundary, and completion_predicates. Dependencies must point to earlier waves. TDD tasks must use the same RED and GREEN command. Include happy and failure QA evidence paths. Treat the sealed IntentFold receipt as a binding interpretation of the goal: preserve its constraints, mitigate its risks, and turn its acceptance signals into executable checks. Do not write code.\n\nSchema exemplar:\n{}\n\nGoal:\n{}\n\nIntentFold receipt:\n{}\n\nScope:\n{}\n\nVerification commands:\n{}",
        PLAN_GRAPH_SCHEMA_EXEMPLAR,
        input.request,
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
    Ok(format!(
        "You are the same Gear planner on fresh repair turn {attempt}. Return a complete PlanGraphDraft JSON object only; never return a patch, prose, or markdown fence. Preserve the request and IntentFold semantics. Correct only the schema errors identified by Rust and keep all valid semantic content. Use the exact nested shapes and enum values in the exemplar.\n\nSchema exemplar:\n{PLAN_GRAPH_SCHEMA_EXEMPLAR}\n\nRust diagnostic:\n{}\n\nMalformed output to repair:\n{}\n\nOriginal goal:\n{}\n\nIntentFold receipt:\n{}\n\nScope:\n{}\n\nVerification commands:\n{}",
        serde_json::to_string_pretty(diagnostic)?,
        raw_output,
        input.request,
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

fn gear_opencode_intent_fold_prompt(input: &IntentFoldInput) -> Result<String> {
    Ok(format!(
        "You are Gear's Metis-style read-only intent analyst. Do not plan tasks and do not write code. Return exactly one IntentFoldVerdict JSON object with no markdown fence or prose. Required shape: {{\"schema_version\":1,\"goal_id\":\"exact goal id\",\"normalized_objective\":\"clear outcome\",\"assumptions\":[\"explicit inference\"],\"constraints\":[\"binding boundary\"],\"ambiguities\":[\"remaining ambiguity\"],\"required_questions\":[\"only questions that change the solution\"],\"risks\":[{{\"code\":\"stable_code\",\"severity\":\"low|medium|high\",\"description\":\"specific risk\",\"mitigation\":\"specific mitigation\"}}],\"acceptance_signals\":[\"observable result\"],\"decision\":\"ready|needs_user\",\"summary\":\"concise conclusion\"}}. Use ready when the user has specified the behavior, scope, and acceptance. Gear owns runtime mechanics: evidence is stored under `.gearbox-agent/artifacts/<goal_id>`, verification commands are supplied by Gear, and workspace scope is enforced before dispatch. Do not ask where these artifacts live, how to run commands, or how phases are sequenced. Use needs_user only for a real product or safety decision that repository inspection and the runtime contract cannot resolve.\n\nGoal id: {}\nRequest:\n{}\n\nScope:\n{}",
        input.goal_id,
        input.request,
        serde_json::to_string_pretty(&input.scope)?,
    ))
}

fn gear_opencode_intent_repair_prompt(
    input: &IntentFoldInput,
    raw_output: &str,
    attempt: usize,
) -> Result<String> {
    Ok(format!(
        "You are Gear's Metis-style intent analyst on fresh repair turn {attempt}. Return one complete IntentFoldVerdict JSON object only. Re-evaluate the request, preserving real product ambiguities, but do not ask the user about runtime-owned mechanics: Gear stores generated evidence under `.gearbox-agent/artifacts/<goal_id>`, runs verification commands supplied below, and enforces the workspace scope. Ask a question only when the user must choose behavior, scope, destructive action, or acceptance semantics. If those are explicit, return `ready` with empty required_questions. Do not write files.\n\nOriginal request:\n{}\n\nScope:\n{}\n\nVerification commands:\n{}\n\nPrevious verdict:\n{}",
        input.request,
        serde_json::to_string_pretty(&input.scope)?,
        serde_json::to_string_pretty(&Vec::<String>::new())?,
        raw_output,
    ))
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
        "You are Gear's independent read-only PlanCritic. Return exactly one PlanCriticVerdict JSON object and no markdown fence. It must bind the exact goal, plan, revision, hash, and planner execution. Return exactly seven checks: references, executability, contradictions, scope, tdd, qa, acceptance. Approve only if all checks and deterministic verification pass. Revise must include blocking findings and concrete revision_instructions. Reject is only for a user decision and must set needs_user_reason.\n\nEvidence:\n{evidence}"
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
        "You are Gear's independent Oracle, in a fresh read-only session separate from Momus. Re-read the exact plan and inspect every referenced repository path with available read/search tools before deciding. Do not write or edit files and do not trust claims that are not supported by the repository. Return exactly one PlanCriticVerdict JSON object with no markdown fence. Check references, executability, contradictions, scope, tdd, qa, and acceptance. Return at most three actionable blocking findings; approve only when the plan is executable and evidence-backed.\n\nEvidence:\n{evidence}"
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
        "You are Gear's read-only planner revising a rejected plan. Apply every blocking required_change and revision_instructions without expanding scope. Return exactly one complete PlanGraphDraft JSON object and no markdown fence or prose.\n\nEvidence:\n{evidence}"
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
if [ "$count" -eq 1 ]; then printf '%s' '{{"schema_version":1,"goal_id":"intent_goal","normalized_objective":"outcome","required_questions":["where are artifacts?"],"decision":"needs_user","summary":"needs runtime clarification"}}' > "$GEARBOX_WORKER_LAST_MESSAGE"; else printf '%s' '{{"schema_version":1,"goal_id":"intent_goal","normalized_objective":"outcome","acceptance_signals":["verified"],"decision":"ready","summary":"ready"}}' > "$GEARBOX_WORKER_LAST_MESSAGE"; fi
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
        assert_eq!(std::fs::read_to_string(counter_path)?, "2");
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
        })?;
        let second = runner.plan(PlannerInput {
            goal_id: "goal_b".to_string(),
            request: "Build another plan".to_string(),
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            verification_commands: vec!["echo verify".to_string()],
            route_decision: planner_decision,
            intent_fold: None,
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
        })?;
        assert_eq!(submission.draft.tasks.len(), 1);
        assert_eq!(std::fs::read_to_string(counter_path)?, "2");
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
