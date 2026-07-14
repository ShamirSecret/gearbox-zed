use std::{
    collections::{HashMap, HashSet, VecDeque},
    env, fs as std_fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::languages::{LanguageDetection, detect_with_request};
use crate::phase_routing::{
    LiveModelInventory, ModelBindingStatus, ModelSelectorId, PhaseBackend, PhaseModelBinding,
    PhaseRouteDecision, PhaseRouteReceipt, PhaseRouteTable,
};
use crate::plan_graph::{
    PhaseProfile, PlanGraph, PlanGraphDraft, PlanSource, PlannerReceipt,
    deterministic_fallback_draft, parse_planner_draft,
};
use crate::plan_review::{
    IntentFoldDecision, IntentFoldReceipt, IntentFoldVerdict, PhaseExecutionBackend,
    PhaseExecutionIdentity, PlanApprovalState, PlanApprovalStatus, PlanCriticDecision,
    PlanCriticReceipt, PlanCriticVerdict, PlanVerifierReport, PlannerExecutionReceipt,
};
use crate::product;
use crate::state::{
    Budget, ContinuationStatus, CoordinatorModel, CriterionEvidenceStatus, Event, EventKind,
    FinalVerificationDimension, FinalVerificationResult, FinalVerificationWaveReceipt, Goal,
    GoalEpochEventKind, GoalGraphNode, GoalRunLeaseGuard, GoalStatus, ModelCallKind,
    ObjectiveEpochOutcomeReceipt, ObjectiveEventKind, ObjectiveGraph, ObjectivePolicy,
    ObjectiveStatus, PlanNodeRunLedger, PlanNodeRunStatus, PlanWaveNodeStatus, PlanWaveRunLedger,
    PromptSettleAction, PromptSettleEvent, RepositoryObservationReceipt,
    RepositoryObservationStatus, ReviewEpochBundle, ReviewEpochRoleEvidence, Scope, Session,
    SettledBudgetUsage, StateStore, Task, TaskInputs, TaskKind, TaskOutputs,
    TaskRouteDecisionReceipt, TaskStatus, WorkLineage, event, id_timestamp, timestamp,
};
use crate::task_manager::{
    CompletionNotifier, ManagedTaskStatus, NotificationResult, ParentSessionState, SendOutcome,
    SharedTaskManager, TaskAttempt, TaskAttemptStatus, TaskFailureKind, TaskManager,
    TaskManagerControl, TaskManagerTickLoop, TaskRecord,
};
use crate::tools::{
    CancellationToken, DiffSnapshot, ShellCommandResult, check_scope, git_head_commit,
    git_snapshot, run_shell_command_with_env_and_cancellation,
};
use crate::worker_broker::{
    BrokerLifecycleReceipt, BrokerOutcome, BrokerPhaseRequest, BrokerUsage, LifecycleState,
    LifecycleStateName, ModelAvailability, PhaseBrokerFactory, WorkerBroker,
    broker_capabilities_for_kind,
};
use crate::workers::{
    CategoryResolution, CategoryResolutionResult, FallbackRoute, SelectedWorkerRoute,
    WorkerCategory, WorkerConfig, WorkerKind, WorkerOutcome, WorkerResult, WorkerStartRequest,
    WorkerStatus, category_resolution_for_route,
};

pub type EventSink = Arc<dyn Fn(&Event) + Send + Sync + 'static>;
pub type CoordinatorReviewHook = Arc<
    dyn Fn(CoordinatorReviewInput) -> Result<Option<CoordinatorReview>> + Send + Sync + 'static,
>;
pub type IntentFoldHook =
    Arc<dyn Fn(IntentFoldInput) -> Result<IntentFoldSubmission> + Send + Sync + 'static>;
pub type PlannerHook =
    Arc<dyn Fn(PlannerInput) -> Result<PlannerSubmission> + Send + Sync + 'static>;
pub type PlanCriticHook =
    Arc<dyn Fn(PlanCriticInput) -> Result<PlanCriticSubmission> + Send + Sync + 'static>;
pub type PlanRevisionHook =
    Arc<dyn Fn(PlanRevisionInput) -> Result<PlanRevisionSubmission> + Send + Sync + 'static>;
pub type DirectModelUsageProvider = Arc<
    dyn Fn(&BrokerPhaseRequest) -> Result<Option<DirectModelUsageReport>> + Send + Sync + 'static,
>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectModelCostUnit {
    Micros,
    Millis,
    Major,
}

#[derive(Clone, Debug)]
pub struct DirectModelUsageReport {
    pub usage: BrokerUsage,
    pub actual_model: Option<ModelSelectorId>,
    pub cost_unit: DirectModelCostUnit,
}

impl DirectModelUsageReport {
    fn normalized_usage(mut self) -> Result<Self> {
        if let Some(cost) = self.usage.cost_micros {
            let multiplier = match self.cost_unit {
                DirectModelCostUnit::Micros => 1,
                DirectModelCostUnit::Millis => 1_000,
                DirectModelCostUnit::Major => 1_000_000,
            };
            self.usage.cost_micros = Some(
                cost.checked_mul(multiplier)
                    .context("direct-model provider cost overflow during normalization")?,
            );
        }
        self.cost_unit = DirectModelCostUnit::Micros;
        Ok(self)
    }
}
pub type StrategistNextGoalHook = Arc<
    dyn Fn(StrategistNextGoalInput) -> Result<StrategistNextGoalSubmission> + Send + Sync + 'static,
>;
pub const DEFAULT_MAX_ITERATIONS: usize = 5;
pub const DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK: usize = 2;
pub const DEFAULT_MAX_RUNTIME_MINUTES: usize = 60;
pub const DEFAULT_MAX_PLAN_REVISIONS: usize = 2;

/// Terminal states that a phase actor can reach, preventing further dispatch.
///
/// These are distinct from `GoalStatus` because they occur at the phase
/// dispatch level (before or during a single phase interaction), not at
/// the goal level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PhaseActorTerminalState {
    /// The model provider's usage could not be determined (stale/no response).
    UsageUnknown,
    /// Permission is still pending user approval.
    PermissionPending,
    /// The user denied a permission request required by the phase.
    PermissionDenied,
    /// The phase backend does not support a required capability.
    CapabilityUnavailable,
    /// The resolved model does not match the requested/required model,
    /// and the mismatch is not covered by an allowed fallback.
    ModelMismatch,
}

impl PhaseActorTerminalState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PhaseActorTerminalState::UsageUnknown
                | PhaseActorTerminalState::PermissionDenied
                | PhaseActorTerminalState::CapabilityUnavailable
                | PhaseActorTerminalState::ModelMismatch
        )
    }

    pub fn message(&self) -> &'static str {
        match self {
            PhaseActorTerminalState::UsageUnknown => {
                "Model usage information is unavailable; cannot continue dispatch"
            }
            PhaseActorTerminalState::PermissionPending => {
                "Permission request is pending user approval"
            }
            PhaseActorTerminalState::PermissionDenied => {
                "Permission was denied for this phase actor"
            }
            PhaseActorTerminalState::CapabilityUnavailable => {
                "The phase backend does not support a required capability"
            }
            PhaseActorTerminalState::ModelMismatch => {
                "The resolved model does not match the requested model"
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct IntentFoldInput {
    pub goal_id: String,
    pub request: String,
    pub scope: Scope,
    pub route_decision: PhaseRouteDecision,
}

#[derive(Clone, Debug)]
pub struct IntentFoldSubmission {
    pub verdict: IntentFoldVerdict,
    pub analyst: PhaseExecutionIdentity,
    pub raw_output: String,
    pub artifact_path: Option<String>,
    pub repository_evidence_path: Option<String>,
    pub repository_discovery: Option<RepositoryDiscoverySubmission>,
}

#[derive(Clone, Debug)]
pub struct RepositoryDiscoverySubmission {
    pub raw_output: String,
    pub analyst: PhaseExecutionIdentity,
    pub artifact_path: String,
    pub repository_evidence_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PlannerInput {
    pub goal_id: String,
    pub request: String,
    pub scope: Scope,
    pub verification_commands: Vec<String>,
    pub route_decision: PhaseRouteDecision,
    pub intent_fold: Option<IntentFoldReceipt>,
    pub repository_discovery: Option<RepositoryDiscoverySubmission>,
}

#[derive(Clone, Debug)]
pub struct PlannerSubmission {
    pub draft: PlanGraphDraft,
    pub planner: PhaseExecutionIdentity,
    pub raw_output: String,
    pub artifact_path: Option<String>,
    pub repository_evidence_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PlanCriticInput {
    pub request: String,
    pub plan: PlanGraph,
    pub planner_receipt: PlannerExecutionReceipt,
    pub verifier_report: PlanVerifierReport,
    pub route_decision: PhaseRouteDecision,
}

#[derive(Clone, Debug)]
pub struct PlanCriticSubmission {
    pub reviewer: PhaseExecutionIdentity,
    pub verdict: PlanCriticVerdict,
    pub raw_output: String,
    pub artifact_path: Option<String>,
    pub repository_evidence_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PlanRevisionInput {
    pub request: String,
    pub plan: PlanGraph,
    pub planner_receipt: PlannerExecutionReceipt,
    pub critic_receipt: PlanCriticReceipt,
    pub route_decision: PhaseRouteDecision,
}

#[derive(Clone, Debug)]
pub struct PlanRevisionSubmission {
    pub draft: PlanGraphDraft,
    pub planner: PhaseExecutionIdentity,
    pub raw_output: String,
    pub artifact_path: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategistNextGoalDecision {
    Complete,
    Continue,
    NeedsUser,
    Stop,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategistNextGoalVerdict {
    pub schema_version: u32,
    pub goal_id: String,
    pub epoch_id: String,
    pub reviewed_status: GoalStatus,
    pub decision: StrategistNextGoalDecision,
    pub next_objective: Option<String>,
    #[serde(default)]
    pub acceptance_signals: Vec<String>,
    #[serde(default)]
    pub required_questions: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub answerable_now: bool,
    pub rationale: String,
}

impl StrategistNextGoalVerdict {
    pub fn parse(raw_output: &str) -> Result<Self> {
        crate::plan_review::parse_json_object(
            raw_output,
            "strategist did not return one strict next-goal JSON object",
        )
    }

    pub fn validate(&self, goal_id: &str, epoch_id: &str, status: &GoalStatus) -> Result<()> {
        if self.schema_version != 1
            || self.goal_id != goal_id
            || self.epoch_id != epoch_id
            || &self.reviewed_status != status
        {
            bail!("strategist verdict has an invalid schema or review binding");
        }
        if self.rationale.trim().is_empty() {
            bail!("strategist verdict requires a rationale");
        }
        for value in self
            .acceptance_signals
            .iter()
            .chain(&self.required_questions)
            .chain(&self.evidence_refs)
        {
            if value.trim().is_empty() {
                bail!("strategist verdict contains an empty evidence or decision value");
            }
        }
        if matches!(self.decision, StrategistNextGoalDecision::Stop)
            && self.evidence_refs.is_empty()
        {
            bail!("stop strategist verdict requires evidence references");
        }
        if self.answerable_now
            && !matches!(
                self.decision,
                StrategistNextGoalDecision::Complete | StrategistNextGoalDecision::Stop
            )
        {
            bail!("answerable strategist verdict must be terminal");
        }
        if self.answerable_now && self.evidence_refs.is_empty() {
            bail!("answerable strategist verdict requires evidence references");
        }
        match self.decision {
            StrategistNextGoalDecision::Continue => {
                if self
                    .next_objective
                    .as_deref()
                    .is_none_or(|objective| objective.trim().is_empty())
                    || self.acceptance_signals.is_empty()
                    || !self.required_questions.is_empty()
                    || self.answerable_now
                {
                    bail!("continue verdict requires an objective and acceptance signals");
                }
            }
            StrategistNextGoalDecision::NeedsUser => {
                if self.reviewed_status != GoalStatus::NeedsUser
                    || self.required_questions.is_empty()
                    || self.next_objective.is_some()
                    || self.answerable_now
                {
                    bail!("needs-user verdict requires questions and no next objective");
                }
            }
            StrategistNextGoalDecision::Complete => {
                if self.reviewed_status != GoalStatus::Complete
                    || self.next_objective.is_some()
                    || !self.required_questions.is_empty()
                {
                    bail!("complete strategist verdict requires a completed goal");
                }
            }
            StrategistNextGoalDecision::Stop => {
                if self.next_objective.is_some() || !self.required_questions.is_empty() {
                    bail!("terminal strategist verdict cannot carry a next objective or question");
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StrategistNextGoalInput {
    pub goal_id: String,
    pub epoch_id: String,
    pub request: String,
    pub status: GoalStatus,
    pub summary: String,
    pub plan: PlanGraph,
    pub final_report_path: String,
    pub budget_ledger: crate::state::GoalBudgetLedger,
    pub route_decision: PhaseRouteDecision,
}

#[derive(Clone, Debug)]
pub struct StrategistNextGoalSubmission {
    pub verdict: StrategistNextGoalVerdict,
    pub strategist: PhaseExecutionIdentity,
    pub raw_output: String,
    pub artifact_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategistNextGoalReceipt {
    pub schema_version: u32,
    pub verdict: StrategistNextGoalVerdict,
    pub strategist: PhaseExecutionIdentity,
    pub raw_output_sha256: String,
    pub artifact_path: Option<String>,
    pub created_at: String,
    pub receipt_hash: String,
}

impl StrategistNextGoalReceipt {
    fn seal(submission: StrategistNextGoalSubmission) -> Result<Self> {
        let mut receipt = Self {
            schema_version: 1,
            verdict: submission.verdict,
            strategist: submission.strategist,
            raw_output_sha256: format!("{:x}", Sha256::digest(submission.raw_output.as_bytes())),
            artifact_path: submission.artifact_path,
            created_at: timestamp(),
            receipt_hash: String::new(),
        };
        receipt.strategist.validate()?;
        receipt.receipt_hash = receipt.expected_hash()?;
        receipt.validate()?;
        Ok(receipt)
    }

    fn validate(&self) -> Result<()> {
        if self.schema_version != 1
            || self.raw_output_sha256.len() != 64
            || self.receipt_hash != self.expected_hash()?
        {
            bail!("strategist receipt integrity validation failed");
        }
        self.strategist.validate()?;
        self.verdict.validate(
            &self.verdict.goal_id,
            &self.verdict.epoch_id,
            &self.verdict.reviewed_status,
        )?;
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_hash.clear();
        let bytes =
            serde_json::to_vec(&payload).context("failed to serialize strategist receipt")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone)]
pub struct PhaseRuntime {
    pub routes: PhaseRouteTable,
    pub inventory: LiveModelInventory,
    pub current_model: Option<ModelSelectorId>,
    pub planner: Option<PhaseExecutionIdentity>,
    pub intent_fold_hook: Option<IntentFoldHook>,
    pub planner_hook: Option<PlannerHook>,
    pub plan_critic_hook: Option<PlanCriticHook>,
    pub oracle_hook: Option<PlanCriticHook>,
    pub plan_revision_hook: Option<PlanRevisionHook>,
    pub strategist_next_goal_hook: Option<StrategistNextGoalHook>,
    pub require_plan_approval: bool,
    pub max_plan_revisions: usize,
    /// Optional worker broker for phase lifecycle management.
    /// When set, phase interactions (planner, PlanCritic, executor,
    /// reviewer) go through the broker lifecycle.
    pub broker: Option<Arc<WorkerBroker>>,
    /// Optional factory for creating per-phase broker sessions.
    /// When set, orchestrator creates a fresh broker per phase invocation
    /// instead of reusing a single shared broker. This avoids illegal
    /// state transitions (e.g. `Resolved → Resolved`) when multiple
    /// phases run sequentially against the same broker.
    pub broker_factory: Option<Arc<PhaseBrokerFactory>>,
    /// Optional provider callback for native/direct model usage telemetry.
    /// ACP workers continue to report through their session handle.
    pub direct_model_usage_provider: Option<DirectModelUsageProvider>,
}

impl PhaseRuntime {
    pub fn legacy() -> Self {
        Self {
            routes: PhaseRouteTable::legacy_defaults(),
            inventory: LiveModelInventory::default(),
            current_model: None,
            planner: None,
            intent_fold_hook: None,
            planner_hook: None,
            plan_critic_hook: None,
            oracle_hook: None,
            plan_revision_hook: None,
            strategist_next_goal_hook: None,
            require_plan_approval: false,
            max_plan_revisions: DEFAULT_MAX_PLAN_REVISIONS,
            broker: None,
            broker_factory: None,
            direct_model_usage_provider: None,
        }
    }
}

impl Default for PhaseRuntime {
    fn default() -> Self {
        Self::legacy()
    }
}

fn build_broker_phase_request(
    phase_decision: &PhaseRouteDecision,
    goal_id: &str,
    plan_id: &str,
    plan_revision: usize,
    task_id: &str,
) -> Result<BrokerPhaseRequest> {
    BrokerPhaseRequest::from_phase_decision(
        phase_decision,
        goal_id,
        plan_id,
        plan_revision,
        task_id,
    )
}

fn write_direct_execution_receipt(
    broker: &WorkerBroker,
    phase_request: &BrokerPhaseRequest,
    outcome: BrokerOutcome,
    store: &StateStore,
) -> Result<()> {
    let state = broker.current_state()?;
    let session_identity = if let Some(identity) = state.session_identity.as_ref() {
        identity.clone()
    } else {
        crate::worker_broker::BrokerSessionIdentity {
            backend_kind: crate::workers::WorkerKind::Custom,
            session_id: format!("direct-model-{}", crate::state::timestamp()),
            started_at: crate::state::timestamp(),
            capabilities: Some(broker_capabilities_for_kind(
                crate::workers::WorkerKind::Custom,
                false,
            )),
        }
    };
    let direct_usage = broker.direct_model_usage(&phase_request.phase_decision_hash)?;
    let (usage, actual_model) = if let Some(receipt) = direct_usage {
        (Some(receipt.usage), receipt.actual_model)
    } else {
        match &phase_request.requested_model {
            ModelAvailability::Available(model)
                if session_identity.supports(&crate::worker_broker::BrokerCapability::Usage) =>
            {
                (
                    Some(BrokerUsage {
                        requested_tokens: None,
                        actual_tokens: None,
                        model: format!("{}/{}", model.provider_id, model.model_id),
                        duration_ms: None,
                        cost_micros: None,
                        cache_hit: None,
                        unavailable_reason: Some(
                            "direct-model backend completed without provider usage telemetry"
                                .to_string(),
                        ),
                    }),
                    Some(model.clone()),
                )
            }
            _ => (None, None),
        }
    };
    let receipt = BrokerLifecycleReceipt {
        schema_version: crate::worker_broker::BROKER_SCHEMA_VERSION,
        interaction_ordinal: state.interaction_ordinal.max(1),
        phase_decision_hash: phase_request.phase_decision_hash.clone(),
        session_identity,
        request: phase_request.clone(),
        outcome,
        terminal_reason: None,
        usage,
        permission_evidence: None,
        actual_model,
        binding_status: None,
        receipt_hash: String::new(),
    }
    .seal()
    .context("failed to seal direct execution receipt")?;
    let artifacts_root = broker.artifacts_root();
    let receipt_dir = artifacts_root.join("direct-execution-receipts");
    std::fs::create_dir_all(&receipt_dir).with_context(|| {
        format!(
            "failed to create direct execution receipt dir at {}",
            receipt_dir.display()
        )
    })?;
    let receipt_path = receipt_dir.join(format!("{}.json", crate::state::timestamp()));
    crate::state::write_json(&receipt_path, &receipt).with_context(|| {
        format!(
            "failed to write direct execution receipt at {}",
            receipt_path.display()
        )
    })?;
    let ledger_model =
        receipt
            .actual_model
            .as_ref()
            .or_else(|| match &phase_request.requested_model {
                ModelAvailability::Available(model) => Some(model),
                ModelAvailability::Unavailable(_) => None,
            });
    let (provider_id, model_id) = ledger_model.map_or((None, None), |model| {
        (
            Some(model.provider_id.clone()),
            Some(model.model_id.clone()),
        )
    });
    let usage = receipt.usage.as_ref();
    let status = serde_json::to_value(&receipt.outcome)?
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let ledger_entry = crate::state::ModelCallLedgerEntry {
        schema_version: crate::state::MODEL_CALL_LEDGER_SCHEMA_VERSION,
        call_id: format!(
            "direct:{}:{}:{}:{}",
            phase_request.goal_id,
            phase_request.task_id,
            phase_request.phase_decision_hash,
            receipt.interaction_ordinal,
        ),
        parent_call_id: None,
        goal_id: phase_request.goal_id.clone(),
        plan_id: if phase_request.plan_id.trim().is_empty() {
            "direct".to_string()
        } else {
            phase_request.plan_id.clone()
        },
        plan_revision: phase_request.plan_revision,
        phase: format!("direct:{}", phase_request.requested_agent),
        task_id: phase_request.task_id.clone(),
        kind: ModelCallKind::Primary,
        worker_kind: phase_request.requested_agent.clone(),
        provider_id,
        model_id,
        session_id: receipt.session_identity.session_id.clone(),
        status,
        artifact_path: Some(receipt_path.to_string_lossy().to_string()),
        transcript_path: None,
        transcript_sha256: None,
        observed_tool_count: 0,
        observed_paths: Vec::new(),
        observation_events: Vec::new(),
        requested_tokens: usage.and_then(|usage| usage.requested_tokens),
        actual_tokens: usage.and_then(|usage| usage.actual_tokens),
        cost_micros: usage.and_then(|usage| usage.cost_micros),
        duration_ms: usage.and_then(|usage| usage.duration_ms),
        cache_hit: usage.and_then(|usage| usage.cache_hit),
        unavailable_reason: usage.and_then(|usage| usage.unavailable_reason.clone()),
        started_at: receipt.session_identity.started_at.clone(),
        finished_at: timestamp(),
    };
    store
        .append_model_call_ledger_entry(&ledger_entry)
        .context("failed to append direct-model usage to ModelCallLedger")?;
    Ok(())
}

fn run_phase_via_broker_inner<T>(
    broker: Option<&WorkerBroker>,
    store: &StateStore,
    direct_model_usage_provider: Option<&DirectModelUsageProvider>,
    phase_decision: &PhaseRouteDecision,
    goal_id: &str,
    plan_id: &str,
    plan_revision: usize,
    task_id: &str,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let Some(broker) = broker else {
        return f();
    };
    let phase_request =
        build_broker_phase_request(phase_decision, goal_id, plan_id, plan_revision, task_id)?;
    let phase_request_clone = phase_request.clone();
    broker
        .resolve(phase_request)
        .context("broker resolve failed for phase actor")?;
    let result = f();
    match &result {
        Ok(_) => {
            if let Ok(state) = broker.current_state() {
                if state.lifecycle.name() == LifecycleStateName::Active {
                    broker
                        .wait_for_outcome()
                        .context("broker wait_for_outcome failed after phase actor")?;
                } else if state.lifecycle.name() == LifecycleStateName::Resolved {
                    if let Some(provider) = direct_model_usage_provider {
                        if let Some(report) = provider(&phase_request_clone)?
                            .map(DirectModelUsageReport::normalized_usage)
                            .transpose()?
                        {
                            broker.record_direct_model_usage(
                                &phase_request_clone.phase_decision_hash,
                                report.usage,
                                report.actual_model,
                            )?;
                        }
                    }
                    write_direct_execution_receipt(
                        broker,
                        &phase_request_clone,
                        BrokerOutcome::Completed,
                        store,
                    )
                    .context("failed to write direct execution receipt")?;
                }
            }
        }
        Err(_) => {
            let _ = broker.cancel().context("broker cancel");
        }
    }
    result
}

/// Run a phase through the broker lifecycle, optionally using a
/// [`PhaseBrokerFactory`] to create a fresh broker per invocation.
///
/// When a `broker_factory` is provided, a new broker is created for this
/// phase call, used for the lifecycle, then removed from the factory's
/// active sessions after completion. This avoids illegal state transitions
/// (e.g. `Resolved → Resolved`) when phases run sequentially.
///
/// Falls back to the shared `broker` when no factory is available.
fn run_phase_via_broker<T>(
    broker: Option<&WorkerBroker>,
    store: &StateStore,
    direct_model_usage_provider: Option<&DirectModelUsageProvider>,
    broker_factory: Option<&PhaseBrokerFactory>,
    phase_decision: &PhaseRouteDecision,
    goal_id: &str,
    plan_id: &str,
    plan_revision: usize,
    task_id: &str,
    execution_identity: &PhaseExecutionIdentity,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    if matches!(phase_decision.candidate.backend, PhaseBackend::Worker(_)) {
        return f();
    }
    if let Some(factory) = broker_factory {
        let phase_broker = factory.create_broker(
            phase_decision,
            goal_id,
            plan_id,
            plan_revision,
            task_id,
            execution_identity,
        )?;
        let result = run_phase_via_broker_inner(
            Some(phase_broker.as_ref()),
            store,
            direct_model_usage_provider,
            phase_decision,
            goal_id,
            plan_id,
            plan_revision,
            task_id,
            f,
        );
        if let Err(remove_err) =
            factory.remove_session(execution_identity, goal_id, task_id, plan_revision)
        {
            eprintln!("failed to remove broker session: {remove_err}");
        }
        return result;
    }

    run_phase_via_broker_inner(
        broker,
        store,
        direct_model_usage_provider,
        phase_decision,
        goal_id,
        plan_id,
        plan_revision,
        task_id,
        f,
    )
}

fn check_phase_terminal_state(decision: &PhaseRouteDecision) -> Result<()> {
    let phase_name = format!("{:?}", decision.phase);
    if let Some(requested) = &decision.requested_model {
        if decision.candidate.model.is_available() {
            let binding = &decision.candidate.model;
            let available = match binding {
                crate::phase_routing::PhaseModelBinding::ExactLive(selector) => Some(selector),
                crate::phase_routing::PhaseModelBinding::CurrentSession => None,
                _ => None,
            };
            if let Some(available_model) = available {
                if available_model != requested {
                    anyhow::bail!(
                        "PhaseActorTerminalState::ModelMismatch: phase {phase_name} \
                         requested model {requested:?} but route resolved to {available_model:?}"
                    );
                }
            }
        }
    }
    match &decision.candidate.backend {
        crate::phase_routing::PhaseBackend::DirectModel => {
            if !decision.candidate.model.is_available() {
                anyhow::bail!(
                    "PhaseActorTerminalState::CapabilityUnavailable: phase {phase_name} \
                     has no available model binding"
                );
            }
        }
        _ => {}
    }
    Ok(())
}

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
    pub budget: Option<Budget>,
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
    pub epoch_id: String,
    pub session_id: String,
    pub status: GoalStatus,
    pub artifacts_root: PathBuf,
    pub final_report_path: PathBuf,
    pub events_path: PathBuf,
    pub final_verification_wave_path: PathBuf,
    pub final_verification_wave_hash: String,
    pub strategist_receipt: Option<StrategistNextGoalReceipt>,
}

#[derive(Clone, Debug)]
struct FinalReviewInputEvidence {
    execution_id: String,
    result_path: PathBuf,
    outcome_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct ObjectiveRunOutcome {
    pub objective_id: String,
    pub status: ObjectiveStatus,
    pub graph_path: PathBuf,
    pub events_path: PathBuf,
    pub final_report_path: Option<PathBuf>,
    pub goal_outcomes: Vec<RunOutcome>,
}

#[derive(Clone)]
struct ObjectiveEpochContext {
    objective_id: String,
    scope_hash: String,
    policy_hash: String,
}

/// A worker that was submitted as part of the current PlanGraph wave and is
/// waiting for the runtime to reduce its result in deterministic task order.
/// The worker itself may already be terminal; retaining the managed run here
/// prevents a later loop turn from dispatching the same node a second time.
#[derive(Clone)]
struct PrestartedPlanWorker {
    plan_task_id: String,
    worker_task_id: String,
    managed_worker_task_id: String,
    worker_task: Task,
    worker_phase: PhaseProfile,
    route_hint: Option<String>,
    phase_decision: PhaseRouteDecision,
    effective_worker: WorkerConfig,
    session_binding_attempt: usize,
    phase_route_ordinal: usize,
    phase_decision_path: PathBuf,
    route_receipt: TaskRouteDecisionReceipt,
    route_receipt_path: PathBuf,
    budget_reservation_id: String,
    completed_run: Option<crate::task_manager::ManagedWorkerRun>,
}

#[allow(clippy::too_many_arguments)]
fn dispatch_parallel_plan_siblings(
    scheduled_plan_wave: &mut VecDeque<String>,
    prestarted_plan_workers: &mut HashMap<String, PrestartedPlanWorker>,
    prestarted_plan_wave: &mut VecDeque<String>,
    tasks: &mut Vec<Task>,
    plan_node_runs: &mut PlanNodeRunLedger,
    plan_graph: &PlanGraph,
    goal_id: &str,
    epoch_id: &str,
    task_namespace: Option<&str>,
    workspace: &std::path::Path,
    store: &StateStore,
    phase_runtime: &PhaseRuntime,
    options: &RunOptions,
    goal: &Goal,
    detection: &LanguageDetection,
    task_manager: &SharedTaskManager,
    lineage: &mut WorkLineage,
    current_plan_wave: &mut Option<PlanWaveRunLedger>,
    budget_controller: &BudgetController,
    worker_call_count: usize,
    premium_worker_call_count: usize,
    attempt_count: usize,
    run_started_at: Instant,
    goal_run_lease: &GoalRunLeaseGuard,
    iteration: usize,
    session_id: &str,
) -> Result<()> {
    let capacity = options.worker.max_parallel_workers.saturating_sub(1);
    if capacity == 0 || scheduled_plan_wave.is_empty() {
        return Ok(());
    }
    let sibling_count = capacity.min(scheduled_plan_wave.len());
    for sibling_index in 0..sibling_count {
        let plan_task_id = scheduled_plan_wave
            .pop_front()
            .context("parallel wave sibling queue ended unexpectedly")?;
        let plan_task = plan_graph
            .task(&plan_task_id)
            .context("parallel wave sibling disappeared from PlanGraph")?;
        if !plan_task.dependencies.is_empty() {
            bail!("parallel wave sibling `{plan_task_id}` unexpectedly has dependencies");
        }
        let worker_route_hint = plan_task.recommended_route_hint();
        let worker_phase =
            worker_phase_for_route_hint(&plan_task.preferred_phase_profile, worker_route_hint);
        let phase_decision = phase_runtime.routes.resolve_for_worker(
            &worker_phase,
            &phase_runtime.inventory,
            phase_runtime.current_model.as_ref(),
            &options.worker,
        )?;
        if !matches!(phase_decision.candidate.backend, PhaseBackend::Worker(_)) {
            bail!("parallel wave sibling `{plan_task_id}` resolved to a non-worker backend");
        }
        let effective_worker = phase_decision.overlay_worker_config(&options.worker)?;
        let selected_route =
            effective_worker.selected_route_for_hint(1, Some(phase_decision.category.as_str()));
        let route_change_type = if !phase_decision.rejected_candidates.is_empty()
            || selected_route.route_reason.contains("fell back to")
        {
            RouteChangeType::Fallback
        } else {
            RouteChangeType::RouteChange
        };
        budget_controller
            .apply_budget_for_route_change(
                &BudgetSnapshot {
                    worker_call_count: worker_call_count.saturating_add(sibling_index),
                    premium_worker_call_count: premium_worker_call_count
                        .saturating_add(usize::from(selected_route.worker_kind.is_premium())),
                    attempt_count: attempt_count.saturating_add(sibling_index),
                    runtime_elapsed_minutes: run_started_at.elapsed().as_secs() as usize / 60,
                    context_risk_signals: Vec::new(),
                },
                route_change_type,
                selected_route.worker_kind.is_premium(),
            )
            .map_err(|reason| anyhow::anyhow!("parallel sibling dispatch blocked: {reason}"))?;

        let budget_reservation_id = format!(
            "{epoch_id}.worker.wave-{iteration}.{}",
            plan_task_id.replace(':', "_")
        );
        store.reserve_budget_call(
            goal_run_lease,
            &budget_reservation_id,
            "worker",
            true,
            selected_route.worker_kind.is_premium(),
            &goal.budget,
        )?;
        store.append_goal_epoch_event(
            goal_id,
            epoch_id,
            &format!("{budget_reservation_id}.reserved"),
            GoalEpochEventKind::BudgetReserved,
            json!({
                "reservation_id": budget_reservation_id,
                "phase": "worker",
                "wave": true,
                "task_id": plan_task_id,
                "premium": selected_route.worker_kind.is_premium(),
                "reserved_tokens": goal.budget.max_tokens_per_call,
            }),
        )?;

        let ordinal = 100 + iteration.saturating_mul(1000) + sibling_index + 1;
        let phase_decision_path =
            store.write_phase_route_decision(goal_id, ordinal, &phase_decision)?;
        let route_attempt = plan_node_runs
            .nodes
            .iter()
            .find(|node| node.task_id == plan_task_id)
            .map(|node| node.attempt.saturating_add(1))
            .unwrap_or(1);
        let route_receipt = TaskRouteDecisionReceipt::seal(
            goal_id,
            epoch_id,
            plan_graph,
            plan_task,
            route_attempt,
            worker_phase.clone(),
            worker_route_hint.map(ToString::to_string),
            selected_route.worker_kind.as_str().to_string(),
            selected_route.worker_model.map(ToString::to_string),
            selected_route.category.as_str().to_string(),
            selected_route.route_reason.clone(),
            phase_decision.selected_candidate,
            phase_decision.rejected_candidates.len(),
            phase_decision_path.to_string_lossy().to_string(),
            Some(budget_reservation_id.clone()),
        )?;
        let route_receipt_path = store.write_task_route_decision_receipt(&route_receipt)?;

        let worker_task_id = scoped_task_id(task_namespace, &plan_task_id);
        let mut worker_task = tasks
            .iter()
            .find(|task| task.id == worker_task_id)
            .context("parallel wave sibling runtime task is missing")?
            .clone();
        worker_task.inputs.phase_route_locked = !matches!(
            phase_decision.candidate.backend,
            PhaseBackend::LegacyCategory
        );
        if let Some(runtime_task) = tasks.iter_mut().find(|task| task.id == worker_task_id) {
            runtime_task.inputs.phase_route_locked = worker_task.inputs.phase_route_locked;
        }
        if let Some(plan_task) = worker_task.inputs.plan_task.as_mut() {
            plan_task.task_id = worker_task_id.clone();
        }
        if matches!(
            plan_task.test.strategy,
            crate::plan_graph::TestStrategy::Tdd
        ) {
            let red_path = run_plan_red_evidence(
                workspace,
                store,
                goal_id,
                &plan_task_id,
                plan_graph.revision,
                plan_task,
                options.cancellation_token.as_ref(),
            )?;
            let node = plan_node_runs.node_mut(&plan_task_id)?;
            node.red_evidence_path = Some(red_path.to_string_lossy().to_string());
            node.status = PlanNodeRunStatus::RedVerified;
            node.updated_at = timestamp();
            plan_node_runs.updated_at = timestamp();
        }
        start_task(tasks, &worker_task_id);
        if let Some(runtime_task) = tasks.iter_mut().find(|task| task.id == worker_task_id) {
            runtime_task.inputs.phase_route_locked = worker_task.inputs.phase_route_locked;
        }
        store.write_tasks(goal_id, tasks)?;

        {
            let node = plan_node_runs.node_mut(&plan_task_id)?;
            if node.status == PlanNodeRunStatus::Pending {
                node.status = PlanNodeRunStatus::Runnable;
            }
            node.attempt = route_attempt;
            node.worker_task_id = Some(worker_task_id.clone());
            node.implementation_task_id = Some(worker_task_id.clone());
            node.status = PlanNodeRunStatus::Running;
            node.updated_at = timestamp();
            plan_node_runs.updated_at = timestamp();
        }
        store.write_plan_node_runs(plan_node_runs)?;

        let managed_worker_task_id = match task_manager.lock() {
            Ok(mut manager) => {
                manager.set_worker_broker(None);
                manager.start(WorkerStartRequest {
                    store,
                    workspace,
                    task: &worker_task,
                    route_attempt,
                    goal: &goal.request,
                    verification_commands: &detection.verification_commands,
                    config: &effective_worker,
                    cancellation_token: options.cancellation_token.clone(),
                    coordinator_model: goal.coordinator_model.as_ref(),
                    coordinator_brief: goal.coordinator_brief.as_deref(),
                    route_hint: Some(phase_decision.category.as_str()),
                })?
            }
            Err(_) => bail!("task manager mutex poisoned during parallel dispatch"),
        };
        persist_live_plan_node_session_binding(
            store,
            goal_id,
            epoch_id,
            plan_graph,
            &plan_task_id,
            route_attempt,
            &managed_worker_task_id,
            &selected_route,
            &route_receipt.receipt_hash,
        )?;
        if let Some(wave_ledger) = current_plan_wave.as_mut() {
            wave_ledger.mark_dispatched(
                &plan_task_id,
                route_attempt,
                managed_worker_task_id.clone(),
            )?;
            wave_ledger.mark_started(&plan_task_id)?;
            store.write_plan_wave_run(wave_ledger)?;
        }
        if !lineage.active_task_ids.contains(&worker_task_id) {
            lineage.active_task_ids.push(worker_task_id.clone());
        }
        append_event(
            store,
            &options.event_sink,
            event(
                session_id,
                Some(goal_id),
                Some(&worker_task_id),
                EventKind::PhaseRouteSelected,
                format!("Parallel wave route selected for {worker_task_id}"),
                json!({
                    "phase": worker_phase,
                    "wave": true,
                    "decision_path": phase_decision_path.to_string_lossy(),
                    "task_route_receipt_path": route_receipt_path.to_string_lossy(),
                    "selected_candidate": phase_decision.selected_candidate,
                    "fallback_count": phase_decision.rejected_candidates.len(),
                }),
            ),
        )?;
        prestarted_plan_wave.push_back(plan_task_id.clone());
        prestarted_plan_workers.insert(
            plan_task_id.clone(),
            PrestartedPlanWorker {
                plan_task_id,
                worker_task_id,
                managed_worker_task_id,
                worker_task,
                worker_phase,
                route_hint: worker_route_hint.map(ToString::to_string),
                phase_decision,
                effective_worker,
                session_binding_attempt: route_attempt,
                phase_route_ordinal: ordinal,
                phase_decision_path,
                route_receipt,
                route_receipt_path,
                budget_reservation_id,
                completed_run: None,
            },
        );
    }
    Ok(())
}

impl ObjectiveRunOutcome {
    pub fn into_last_goal_outcome(self) -> Result<RunOutcome> {
        let objective_status = self.status.clone();
        if let Some(mut outcome) = self.goal_outcomes.into_iter().last() {
            outcome.status = goal_status_for_objective(&objective_status);
            return Ok(outcome);
        }
        let graph: ObjectiveGraph = serde_json::from_str(
            &std_fs::read_to_string(&self.graph_path)
                .with_context(|| format!("failed to read {}", self.graph_path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", self.graph_path.display()))?;
        let node = graph
            .nodes
            .iter()
            .rev()
            .find(|node| node.final_report_path.is_some())
            .context("objective has no persisted goal outcome")?;
        let workspace = PathBuf::from(&graph.workspace);
        let store = StateStore::new(&workspace);
        let artifacts_root = store.artifact_dir(&node.goal_id);
        let final_report_path = PathBuf::from(
            node.final_report_path
                .as_deref()
                .context("objective goal is missing final report path")?,
        );
        Ok(RunOutcome {
            goal_id: node.goal_id.clone(),
            epoch_id: node.epoch_id.clone(),
            session_id: node.session_id.clone(),
            status: goal_status_for_objective(&objective_status),
            artifacts_root: artifacts_root.clone(),
            final_report_path,
            events_path: store.events_path(&node.session_id),
            final_verification_wave_path: artifacts_root.join("final-verification-wave.json"),
            final_verification_wave_hash: node.final_wave_receipt_hash.clone().unwrap_or_default(),
            strategist_receipt: None,
        })
    }
}

fn goal_status_for_objective(status: &ObjectiveStatus) -> GoalStatus {
    match status {
        ObjectiveStatus::Complete => GoalStatus::Complete,
        ObjectiveStatus::NeedsUser => GoalStatus::NeedsUser,
        ObjectiveStatus::Stopped => GoalStatus::Blocked,
        ObjectiveStatus::Limited => GoalStatus::Limited,
        ObjectiveStatus::Blocked => GoalStatus::Blocked,
        ObjectiveStatus::Failed => GoalStatus::Failed,
        ObjectiveStatus::Running => GoalStatus::Running,
    }
}

/// Read the explicit Gear objective-controller switch and its bounded policy.
/// Normal single-goal callers receive `None` and retain the GBX-008 behavior.
pub fn objective_policy_from_env() -> Result<Option<ObjectivePolicy>> {
    let Some(raw_enabled) = env::var_os("GEARBOX_GEAR_OBJECTIVE") else {
        return Ok(None);
    };
    let enabled = raw_enabled.to_string_lossy().trim().to_ascii_lowercase();
    if matches!(enabled.as_str(), "0" | "false" | "off" | "no") {
        return Ok(None);
    }
    if !matches!(enabled.as_str(), "1" | "true" | "on" | "yes") {
        bail!("GEARBOX_GEAR_OBJECTIVE must be one of 0/1/false/true/off/on");
    }
    let defaults = ObjectivePolicy::rolling_default();
    let policy = ObjectivePolicy {
        auto_continue: objective_bool_env("GEARBOX_GEAR_AUTO_CONTINUE", defaults.auto_continue)?,
        max_epochs: objective_usize_env("GEARBOX_GEAR_MAX_EPOCHS", defaults.max_epochs)?,
        max_calls: objective_usize_env("GEARBOX_GEAR_MAX_OBJECTIVE_CALLS", defaults.max_calls)?,
        max_tokens: objective_u64_env("GEARBOX_GEAR_MAX_OBJECTIVE_TOKENS", defaults.max_tokens)?,
        max_cost_micros: objective_u64_env(
            "GEARBOX_GEAR_MAX_OBJECTIVE_COST_MICROS",
            defaults.max_cost_micros,
        )?,
        max_unknown_usage_calls: objective_usize_env(
            "GEARBOX_GEAR_MAX_OBJECTIVE_UNKNOWN_USAGE_CALLS",
            defaults.max_unknown_usage_calls,
        )?,
        max_consecutive_no_progress: objective_usize_env(
            "GEARBOX_GEAR_MAX_CONSECUTIVE_NO_PROGRESS",
            defaults.max_consecutive_no_progress,
        )?,
        max_consecutive_failures: objective_usize_env(
            "GEARBOX_GEAR_MAX_CONSECUTIVE_FAILURES",
            defaults.max_consecutive_failures,
        )?,
        cooldown_seconds: objective_u64_env(
            "GEARBOX_GEAR_OBJECTIVE_COOLDOWN_SECONDS",
            defaults.cooldown_seconds,
        )?,
    };
    policy.validate()?;
    Ok(Some(policy))
}

fn objective_bool_env(name: &str, default_value: bool) -> Result<bool> {
    let Some(value) = env::var_os(name) else {
        return Ok(default_value);
    };
    match value.to_string_lossy().trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        _ => bail!("{name} must be one of 0/1/false/true/off/on"),
    }
}

fn objective_usize_env(name: &str, default_value: usize) -> Result<usize> {
    let Some(value) = env::var_os(name) else {
        return Ok(default_value);
    };
    let value = value.to_string_lossy();
    let parsed = value
        .trim()
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(parsed)
}

fn objective_u64_env(name: &str, default_value: u64) -> Result<u64> {
    let Some(value) = env::var_os(name) else {
        return Ok(default_value);
    };
    let value = value.to_string_lossy();
    let parsed = value
        .trim()
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(parsed)
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
        Self::run_with_phase_runtime(options, PhaseRuntime::legacy())
    }

    pub fn run_with_phase_runtime(
        options: RunOptions,
        phase_runtime: PhaseRuntime,
    ) -> Result<RunOutcome> {
        Self::run_single_goal_with_phase_runtime(options, phase_runtime, None, None, None)
    }

    pub fn run_objective_with_phase_runtime(
        options: RunOptions,
        phase_runtime: PhaseRuntime,
        policy: ObjectivePolicy,
    ) -> Result<ObjectiveRunOutcome> {
        run_objective_controller(options, phase_runtime, policy)
    }

    fn run_single_goal_with_phase_runtime(
        options: RunOptions,
        phase_runtime: PhaseRuntime,
        fixed_goal_id: Option<String>,
        fixed_epoch_id: Option<String>,
        objective_context: Option<ObjectiveEpochContext>,
    ) -> Result<RunOutcome> {
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
        let task_namespace = fixed_goal_id.clone();
        let goal_id = fixed_goal_id.unwrap_or_else(|| format!("goal_{id_suffix}"));

        if options.continuation && store.continuation_is_stopped_for_session(&session_id)? {
            store.record_prompt_settle_decision(
                &goal_id,
                "continuation",
                &session_id,
                0,
                "runtime.continuation.start",
                PromptSettleEvent::UserStopped,
            )?;
            bail!(
                "Gear continuation is stopped; explicitly restart the continuation before running again"
            );
        }
        if options.continuation {
            let resume_decision = store.prepare_continuation_resume(&session_id, &goal_id)?;
            if !resume_decision.should_resume {
                let reason = resume_decision
                    .state
                    .stuck_reason
                    .unwrap_or_else(|| "continuation resume budget exhausted".to_string());
                store.record_prompt_settle_decision(
                    &goal_id,
                    "continuation",
                    &session_id,
                    0,
                    "runtime.continuation.resume",
                    PromptSettleEvent::Error,
                )?;
                bail!("Gear continuation is stuck: {reason}");
            }
            let settle = store.record_prompt_settle_decision(
                &goal_id,
                "continuation",
                &session_id,
                0,
                "runtime.continuation.resume",
                PromptSettleEvent::Idle,
            )?;
            if settle.decision.action != PromptSettleAction::Dispatch {
                bail!(
                    "continuation dispatch blocked by settle decision: {}",
                    settle.decision.reason
                );
            }
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

        let mut goal_budget = options.budget.clone().unwrap_or_default();
        goal_budget.max_provider_unknown_streak = options.max_provider_unknown_streak.max(1);
        if options.budget.is_none() {
            goal_budget.max_runtime_minutes = options.max_runtime_minutes.max(1);
        }
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

        let epoch_id = fixed_epoch_id.unwrap_or_else(|| format!("epoch_{}", id_timestamp()));
        if options.continuation {
            let guard =
                store.update_continuation_guard(&session_id, &goal_id, &epoch_id, |guard| {
                    // An in-flight marker belongs to the previous process;
                    // a restarted runtime must not treat it as a live turn or
                    // retain a worker-event background marker from that process.
                    guard.in_flight = false;
                    guard.background_pending = false;
                })?;
            if let Some(reason) = guard.blocking_reason() {
                store.record_prompt_settle_decision(
                    &goal_id,
                    "continuation",
                    &session_id,
                    0,
                    "runtime.continuation.guard",
                    prompt_settle_event_for_guard_reason(reason),
                )?;
                bail!("Gear continuation guard blocked dispatch: {reason}");
            }
        }
        let lease_seconds =
            u64::try_from(goal.budget.max_runtime_minutes.max(1).saturating_mul(60))
                .unwrap_or(u64::MAX);
        let goal_run_lease = store.acquire_goal_run_lease(
            &goal_id,
            &epoch_id,
            &session_id,
            Duration::from_secs(lease_seconds),
        )?;
        store.abort_incomplete_goal_epoch(
            &goal_id,
            "previous runtime released its lease without a terminal epoch event",
        )?;
        store.append_goal_epoch_event(
            &goal_id,
            &epoch_id,
            &format!("{epoch_id}.started"),
            GoalEpochEventKind::Started,
            json!({
                "session_id": session_id,
                "request": options.request,
            }),
        )?;

        phase_runtime.routes.validate()?;
        phase_runtime.inventory.validate()?;
        if let Some(current_model) = phase_runtime.current_model.as_ref() {
            current_model.validate()?;
        }
        let phase_routes_path = store.write_phase_route_table(&goal_id, &phase_runtime.routes)?;
        let plan_graph = build_approved_plan_graph_with_budget(
            &mut goal,
            &scope,
            &detection.verification_commands,
            &workspace,
            &store,
            &session_id,
            &options.event_sink,
            options.cancellation_token.as_ref(),
            &phase_runtime,
            &goal_run_lease,
            &epoch_id,
        )?;
        let plan_graph_path = if phase_runtime.require_plan_approval {
            store.write_plan_graph(&plan_graph)?
        } else {
            store.write_unreviewed_plan_graph(&plan_graph)?
        };
        if phase_runtime.require_plan_approval {
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    None,
                    EventKind::PlanApproved,
                    format!("Published approved plan revision {}", plan_graph.revision),
                    json!({
                        "plan_id": plan_graph.plan_id,
                        "plan_hash": plan_graph.plan_hash,
                        "revision": plan_graph.revision,
                        "canonical_path": plan_graph_path.to_string_lossy(),
                    }),
                ),
            )?;
        }
        store.append_goal_epoch_event(
            &goal_id,
            &epoch_id,
            &format!("{epoch_id}.planning.completed"),
            GoalEpochEventKind::PhaseCompleted,
            json!({
                "phase": "planning",
                "plan_id": plan_graph.plan_id,
                "plan_revision": plan_graph.revision,
                "plan_hash": plan_graph.plan_hash,
                "approval_required": phase_runtime.require_plan_approval,
            }),
        )?;
        let plan_tasks = plan_graph.draft.tasks.clone();
        let first_plan_task = plan_tasks
            .first()
            .context("approved PlanGraph has no executable tasks")?;
        let initial_preferred_phase = first_plan_task.preferred_phase_profile.clone();
        let initial_plan_route_hint =
            phase_profile_route_hint(&first_plan_task.preferred_phase_profile);
        let initial_worker_phase =
            worker_phase_for_route_hint(&initial_preferred_phase, initial_plan_route_hint);
        let initial_phase_decision = phase_runtime.routes.resolve_for_worker(
            &initial_worker_phase,
            &phase_runtime.inventory,
            phase_runtime.current_model.as_ref(),
            &options.worker,
        )?;
        let initial_worker_config =
            initial_phase_decision.overlay_worker_config(&options.worker)?;
        let mut tasks = initial_tasks(&goal_id, &scope);
        for task in &mut tasks {
            task.id = scoped_task_id(task_namespace.as_deref(), &task.id);
        }
        tasks.extend(plan_tasks.iter().map(|plan_task| {
            let mut task = plan_task.to_runtime_task(
                &goal_id,
                initial_worker_config
                    .selected_route_for_hint(1, initial_plan_route_hint)
                    .worker_kind,
            );
            task.id = scoped_task_id(task_namespace.as_deref(), &task.id);
            task.inputs.phase_route_locked = false;
            task
        }));
        store.write_tasks(&goal_id, &tasks)?;

        let mut plan_node_runs = if let Some(existing) = store.read_plan_node_runs(&goal_id)? {
            existing.validate()?;
            if existing.epoch_id != epoch_id
                || existing.plan_id != plan_graph.plan_id
                || existing.plan_revision != plan_graph.revision
                || existing.plan_hash != plan_graph.plan_hash
            {
                bail!("persisted PlanNodeRunLedger does not match the active PlanGraph");
            }
            existing
        } else {
            PlanNodeRunLedger::from_plan(&goal_id, &epoch_id, &plan_graph)?
        };
        store.write_plan_node_runs(&plan_node_runs)?;

        let spec_path =
            store.write_artifact(&goal_id, "spec.md", &product::spec(&goal, &detection))?;
        let spec_task_id = scoped_task_id(task_namespace.as_deref(), "task_001");
        let plan_meta_task_id = scoped_task_id(task_namespace.as_deref(), "task_002");
        let verification_task_id = scoped_task_id(task_namespace.as_deref(), "task_004");
        let report_task_id = scoped_task_id(task_namespace.as_deref(), "task_006");
        complete_task(&mut tasks, &spec_task_id, |task| {
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
                Some(&spec_task_id),
                EventKind::SpecCreated,
                "Spec artifact created",
                json!({ "path": spec_path.to_string_lossy() }),
            ),
        )?;

        set_task_inputs(&mut tasks, spec_path.to_string_lossy().to_string(), None);
        let plan_path = store.write_artifact(
            &goal_id,
            "plan.md",
            &product::plan(&goal, &plan_graph, &detection),
        )?;
        complete_task(&mut tasks, &plan_meta_task_id, |task| {
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
                Some(&plan_meta_task_id),
                EventKind::PlanCreated,
                "Plan artifact created",
                json!({
                    "path": plan_path.to_string_lossy(),
                    "plan_graph_path": plan_graph_path.to_string_lossy(),
                    "plan_id": plan_graph.plan_id,
                    "revision": plan_graph.revision,
                    "plan_hash": plan_graph.plan_hash,
                    "task_count": plan_graph.draft.tasks.len(),
                    "source": plan_graph.source,
                    "phase_routes_path": phase_routes_path.to_string_lossy(),
                }),
            ),
        )?;

        let mut before_diff = git_snapshot(&workspace)?;
        let mut after_diff = before_diff.clone();
        let mut scope_check = check_scope(&after_diff, &scope);
        let mut worker_result = None;
        let mut final_worker_outcome = None;
        let mut verification_results = Vec::new();
        let mut last_verification_path = None;
        let mut final_evaluation = None;
        let mut last_coordinator_review: Option<CoordinatorReview> = None;
        let mut next_route_hint_override: Option<String> = None;
        let mut last_executor_evidence: Option<FinalReviewInputEvidence> = None;
        let mut provider_unknown_streak = 0usize;
        let mut repeated_failure_streak = 0usize;
        let mut last_failure_kind: Option<TaskFailureKind> = None;
        let mut diff_history: Vec<DiffSnapshot> = Vec::new();
        let mut verification_history: Vec<Vec<ShellCommandResult>> = Vec::new();
        let mut repair_request_history: Vec<String> = Vec::new();
        let mut worker_output_history: Vec<String> = Vec::new();
        let mut context_pressure_seen = false;
        let run_started_at = Instant::now();
        let mut worker_call_count = 0usize;
        let mut premium_worker_call_count = 0usize;
        let mut attempt_count = 0usize;
        let budget_controller = BudgetController {
            max_iterations,
            max_files_changed: options.max_files_changed,
            max_worker_calls: goal.budget.max_worker_calls,
            max_premium_worker_calls: goal.budget.max_premium_worker_calls,
            max_same_failure_retries: 2,
            max_provider_unknown_streak: goal.budget.max_provider_unknown_streak,
            max_child_depth: options.max_child_depth,
            max_runtime_minutes: goal.budget.max_runtime_minutes,
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
            task_manager.set_goal_epoch_context(
                session_id.clone(),
                goal_id.clone(),
                epoch_id.clone(),
            )?;
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
            l.plan_remaining_items = 1;
            l
        });
        prepare_lineage_for_run(&mut lineage, &session_id);
        lineage.plan_remaining_items = plan_node_runs.nodes.len();
        store.write_lineage(&lineage)?;

        let mut completed_plan_tasks = plan_node_runs.completed_task_ids();
        let mut current_plan_task_id: Option<String> = None;
        let mut scheduled_plan_wave: VecDeque<String> = VecDeque::new();
        let mut current_plan_wave: Option<PlanWaveRunLedger> = None;
        let mut prestarted_plan_workers: HashMap<String, PrestartedPlanWorker> = HashMap::new();
        let mut prestarted_plan_wave: VecDeque<String> = VecDeque::new();

        #[allow(clippy::explicit_counter_loop)]
        for iteration in 1..=max_iterations {
            check_run_cancelled(options.cancellation_token.as_ref())?;
            if options.continuation && store.continuation_is_stopped_for_session(&session_id)? {
                store.record_prompt_settle_decision(
                    &goal_id,
                    current_plan_task_id.as_deref().unwrap_or("continuation"),
                    &session_id,
                    iteration,
                    "runtime.continuation.loop",
                    PromptSettleEvent::UserStopped,
                )?;
                final_evaluation = Some(GoalEvaluation {
                    status: GoalStatus::NeedsUser,
                    should_continue: false,
                    summary: "Continuation was stopped by the user before the next worker turn."
                        .to_string(),
                    route_hint_override: None,
                });
                break;
            }
            if options.continuation {
                let guard =
                    store.update_continuation_guard(&session_id, &goal_id, &epoch_id, |guard| {
                        guard.in_flight = false;
                        guard.background_pending = false;
                    })?;
                if let Some(reason) = guard.blocking_reason() {
                    store.record_prompt_settle_decision(
                        &goal_id,
                        current_plan_task_id.as_deref().unwrap_or("continuation"),
                        &session_id,
                        iteration,
                        "runtime.continuation.guard",
                        prompt_settle_event_for_guard_reason(reason),
                    )?;
                    final_evaluation = Some(GoalEvaluation {
                        status: GoalStatus::NeedsUser,
                        should_continue: false,
                        summary: format!(
                            "Continuation was held by the persisted OMO guard: {reason}."
                        ),
                        route_hint_override: None,
                    });
                    break;
                }
            }
            let plan_task_id = if let Some(task_id) = current_plan_task_id.clone() {
                task_id
            } else if let Some(task_id) = prestarted_plan_wave.pop_front() {
                current_plan_task_id = Some(task_id.clone());
                task_id
            } else {
                let active = plan_node_runs.active_task_ids();
                if scheduled_plan_wave.is_empty() {
                    let wave = plan_graph.runnable_wave(
                        &completed_plan_tasks,
                        &active,
                        options.worker.max_parallel_workers,
                    )?;
                    if !wave.is_empty() {
                        let wave_ids = wave
                            .iter()
                            .map(|task| task.task_id.clone())
                            .collect::<Vec<_>>();
                        let wave_id = format!("wave-{iteration}");
                        let mut wave_ledger = PlanWaveRunLedger::new(
                            &goal_id,
                            &epoch_id,
                            &plan_graph,
                            &wave_id,
                            wave_ids.clone(),
                        )?;
                        wave_ledger.open_barrier()?;
                        store.write_plan_wave_run(&wave_ledger)?;
                        current_plan_wave = Some(wave_ledger);
                        scheduled_plan_wave.extend(wave_ids.iter().cloned());
                        store.append_goal_epoch_event(
                            &goal_id,
                            &epoch_id,
                            &format!("{epoch_id}.plan-wave.{}", iteration),
                            GoalEpochEventKind::PhaseCompleted,
                            json!({
                                "phase": "plan_wave_scheduled",
                                "capacity": options.worker.max_parallel_workers.max(1),
                                "task_ids": wave_ids,
                            }),
                        )?;
                    }
                }
                let next = scheduled_plan_wave.pop_front();
                let Some(task_id) = next else {
                    if completed_plan_tasks.len() == plan_graph.draft.tasks.len() {
                        break;
                    }
                    goal.status = GoalStatus::NeedsUser;
                    goal.summary =
                        "PlanGraph has no runnable node; dependency state requires a user decision."
                            .to_string();
                    goal.updated_at = timestamp();
                    store.write_goal(&goal)?;
                    break;
                };
                plan_node_runs.mark(&task_id, PlanNodeRunStatus::Runnable)?;
                store.write_plan_node_runs(&plan_node_runs)?;
                current_plan_task_id = Some(task_id.clone());
                task_id
            };
            if options.continuation {
                store.update_continuation_guard(&session_id, &goal_id, &epoch_id, |guard| {
                    guard.in_flight = true
                })?;
            }
            let mut prestarted_worker = prestarted_plan_workers.remove(&plan_task_id);
            let current_plan_task = plan_graph
                .task(&plan_task_id)
                .context("scheduled PlanGraph task disappeared before routing")?;
            let parent_task_id = goal.current_task_id.clone();
            let worker_route_hint_owned = prestarted_worker
                .as_ref()
                .and_then(|worker| worker.route_hint.as_deref())
                .map(ToString::to_string)
                .or_else(|| next_route_hint_override.clone())
                .or_else(|| {
                    last_coordinator_review
                        .as_ref()
                        .and_then(|review| review.route_hint.as_deref())
                        .map(ToString::to_string)
                })
                .or_else(|| {
                    current_plan_task
                        .recommended_route_hint()
                        .map(ToString::to_string)
                })
                .or_else(|| initial_plan_route_hint.map(ToString::to_string));
            let worker_route_hint = worker_route_hint_owned.as_deref();
            let worker_route_is_review = worker_route_hint == Some("review");
            let worker_phase = prestarted_worker
                .as_ref()
                .map(|worker| worker.worker_phase.clone())
                .unwrap_or_else(|| {
                    worker_phase_for_route_hint(
                        &current_plan_task.preferred_phase_profile,
                        worker_route_hint,
                    )
                });
            let phase_decision = if let Some(worker) = prestarted_worker.as_ref() {
                worker.phase_decision.clone()
            } else {
                phase_runtime.routes.resolve_for_worker(
                    &worker_phase,
                    &phase_runtime.inventory,
                    phase_runtime.current_model.as_ref(),
                    &options.worker,
                )?
            };
            let effective_worker = if let Some(worker) = prestarted_worker.as_ref() {
                worker.effective_worker.clone()
            } else {
                phase_decision.overlay_worker_config(&options.worker)?
            };
            let phase_decision_path = if let Some(worker) = prestarted_worker.as_ref() {
                worker.phase_decision_path.clone()
            } else {
                store.write_phase_route_decision(&goal_id, 100 + iteration, &phase_decision)?
            };
            let resolved_worker_route_hint = Some(phase_decision.category.as_str());
            let selected_route =
                effective_worker.selected_route_for_hint(1, resolved_worker_route_hint);
            let (category_resolution, category_resolution_result) = category_resolution_for_route(
                &effective_worker,
                1,
                resolved_worker_route_hint,
                &selected_route,
            );
            let current_route_change_type = if worker_route_hint == Some("review") {
                RouteChangeType::ReviewTrigger
            } else if !phase_decision.rejected_candidates.is_empty()
                || selected_route.route_reason.contains("fell back to")
            {
                RouteChangeType::Fallback
            } else {
                RouteChangeType::RouteChange
            };
            if prestarted_worker.is_none() {
                if let Err(reason) = budget_controller.apply_budget_for_route_change(
                    &BudgetSnapshot {
                        worker_call_count,
                        premium_worker_call_count,
                        attempt_count,
                        runtime_elapsed_minutes: run_started_at.elapsed().as_secs() as usize / 60,
                        context_risk_signals: Vec::new(),
                    },
                    current_route_change_type.clone(),
                    selected_route.worker_kind.is_premium(),
                ) {
                    goal.status = GoalStatus::Limited;
                    goal.summary = format!("Worker dispatch blocked before launch: {reason}");
                    goal.updated_at = timestamp();
                    store.write_goal(&goal)?;
                    store.append_goal_epoch_event(
                        &goal_id,
                        &epoch_id,
                        &format!("{epoch_id}.budget-aborted"),
                        GoalEpochEventKind::Aborted,
                        json!({
                            "reason": reason,
                            "worker_calls": worker_call_count,
                            "premium_worker_calls": premium_worker_call_count,
                        }),
                    )?;
                    goal_run_lease.release()?;
                    bail!("{}", goal.summary);
                }
            }
            let budget_reservation_id = prestarted_worker
                .as_ref()
                .map(|worker| worker.budget_reservation_id.clone())
                .unwrap_or_else(|| format!("{epoch_id}.worker.{iteration}"));
            if prestarted_worker.is_none() {
                if let Err(error) = store.reserve_budget_call(
                    &goal_run_lease,
                    &budget_reservation_id,
                    "worker",
                    true,
                    selected_route.worker_kind.is_premium(),
                    &goal.budget,
                ) {
                    goal.status = GoalStatus::Limited;
                    goal.summary = format!("Worker dispatch reservation failed: {error}");
                    goal.updated_at = timestamp();
                    store.write_goal(&goal)?;
                    store.append_goal_epoch_event(
                        &goal_id,
                        &epoch_id,
                        &format!("{epoch_id}.reservation-aborted"),
                        GoalEpochEventKind::Aborted,
                        json!({
                            "reason": error.to_string(),
                            "reservation_id": budget_reservation_id,
                        }),
                    )?;
                    goal_run_lease.release()?;
                    bail!("{}", goal.summary);
                }
                store.append_goal_epoch_event(
                    &goal_id,
                    &epoch_id,
                    &format!("{budget_reservation_id}.reserved"),
                    GoalEpochEventKind::BudgetReserved,
                    json!({
                        "reservation_id": budget_reservation_id,
                        "phase": "worker",
                        "premium": selected_route.worker_kind.is_premium(),
                        "reserved_tokens": goal.budget.max_tokens_per_call,
                    }),
                )?;
            }
            let first_plan_attempt = prestarted_worker.is_some()
                || plan_node_runs
                    .nodes
                    .iter()
                    .find(|node| node.task_id == plan_task_id)
                    .is_some_and(|node| node.attempt == 0);
            let route_attempt = plan_node_runs
                .nodes
                .iter()
                .find(|node| node.task_id == plan_task_id)
                .map(|node| node.attempt.saturating_add(1))
                .unwrap_or(1);
            let route_receipt = if let Some(worker) = prestarted_worker.as_ref() {
                worker.route_receipt.clone()
            } else {
                crate::state::TaskRouteDecisionReceipt::seal(
                    &goal_id,
                    &epoch_id,
                    &plan_graph,
                    plan_graph
                        .task(&plan_task_id)
                        .context("missing PlanGraph task for route receipt")?,
                    route_attempt,
                    worker_phase.clone(),
                    worker_route_hint.map(ToString::to_string),
                    selected_route.worker_kind.as_str().to_string(),
                    selected_route.worker_model.map(ToString::to_string),
                    selected_route.category.as_str().to_string(),
                    selected_route.route_reason.clone(),
                    phase_decision.selected_candidate,
                    phase_decision.rejected_candidates.len(),
                    phase_decision_path.to_string_lossy().to_string(),
                    Some(budget_reservation_id.clone()),
                )?
            };
            let route_receipt_path = if let Some(worker) = prestarted_worker.as_ref() {
                worker.route_receipt_path.clone()
            } else {
                store.write_task_route_decision_receipt(&route_receipt)?
            };
            let worker_task_id = if let Some(worker) = prestarted_worker.as_ref() {
                worker.worker_task_id.clone()
            } else if first_plan_attempt {
                scoped_task_id(task_namespace.as_deref(), &plan_task_id)
            } else {
                let verification_path = last_verification_path
                    .as_deref()
                    .context("missing verification artifact for repair iteration")?;
                let repair_task_id = add_repair_task(
                    &mut tasks,
                    &goal_id,
                    &scope,
                    iteration,
                    &plan_task_id,
                    verification_path,
                    parent_task_id.clone(),
                    selected_route.worker_kind,
                    task_namespace.as_deref(),
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
                            "resolved_route_hint": resolved_worker_route_hint,
                            "worker_kind": selected_route.worker_kind.as_str(),
                            "worker_model": selected_route.worker_model,
                            "worker_category": selected_route.category.as_str(),
                            "route_reason": &selected_route.route_reason,
                        }),
                    ),
                )?;
                repair_task_id
            };
            if prestarted_worker.is_none() {
                let node = plan_node_runs.node_mut(&plan_task_id)?;
                node.attempt = node.attempt.saturating_add(1);
                node.worker_task_id = Some(worker_task_id.clone());
                if first_plan_attempt {
                    node.implementation_task_id = Some(worker_task_id.clone());
                } else if worker_route_hint == Some("review") {
                    node.review_task_id = Some(worker_task_id.clone());
                }
                node.status = PlanNodeRunStatus::Running;
                node.updated_at = timestamp();
                plan_node_runs.updated_at = timestamp();
                store.write_plan_node_runs(&plan_node_runs)?;
            }

            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&worker_task_id),
                    EventKind::PhaseRouteSelected,
                    format!("Phase route selected for {worker_task_id}"),
                    json!({
                        "phase": worker_phase,
                        "decision_path": phase_decision_path.to_string_lossy(),
                        "task_route_receipt_path": route_receipt_path.to_string_lossy(),
                        "selected_candidate": phase_decision.selected_candidate,
                        "fallback_count": phase_decision.rejected_candidates.len(),
                    }),
                ),
            )?;

            // Generate immutable ownership decision before any execution.
            let ownership = crate::state::ExecutionOwnership {
                delegated: selected_route.require_worker || effective_worker.skip_worker,
                worker_kind: Some(selected_route.worker_kind.as_str().to_string()),
                route_reason: selected_route.route_reason.clone(),
                risk_profile: "unknown".to_string(),
                worker_task_id: Some(worker_task_id.clone()),
                decided_at: crate::state::timestamp(),
            };

            if prestarted_worker.is_none() {
                start_task(&mut tasks, &worker_task_id);
            }
            goal.status = GoalStatus::Running;
            goal.current_task_id = Some(worker_task_id.clone());
            goal.updated_at = timestamp();
            store.write_goal(&goal)?;
            store.write_tasks(&goal_id, &tasks)?;
            if prestarted_worker.is_none() {
                append_event(
                    &store,
                    &options.event_sink,
                    event(
                        &session_id,
                        Some(&goal_id),
                        Some(&worker_task_id),
                        EventKind::WorkerStarted,
                        if first_plan_attempt {
                            "Prepared implementation worker packet".to_string()
                        } else {
                            "Prepared repair worker packet".to_string()
                        },
                        json!({
                            "iteration": iteration,
                            "phase": worker_phase,
                            "phase_route_decision_path": phase_decision_path.to_string_lossy(),
                            "before": &before_diff,
                            "current": &after_diff,
                            "route_hint": worker_route_hint,
                            "resolved_route_hint": resolved_worker_route_hint,
                            "worker_kind": selected_route.worker_kind.as_str(),
                            "worker_model": selected_route.worker_model,
                            "worker_category": selected_route.category.as_str(),
                            "route_reason": &selected_route.route_reason,
                        }),
                    ),
                )?;
            }

            let reviewed_execution_id = if worker_route_hint == Some("review") {
                let evidence = last_executor_evidence
                    .as_ref()
                    .context("review route requires a completed executor execution")?;
                validate_final_review_input_artifacts(
                    &evidence.execution_id,
                    Some(&evidence.result_path),
                    Some(&evidence.outcome_path),
                    last_verification_path.as_deref(),
                )?;
                Some(evidence.execution_id.clone())
            } else {
                None
            };
            let mut worker_task = if let Some(worker) = prestarted_worker.as_ref() {
                worker.worker_task.clone()
            } else {
                tasks
                    .iter()
                    .find(|task| task.id == worker_task_id)
                    .context("missing worker task")?
                    .clone()
            };
            if prestarted_worker.is_none() {
                worker_task.inputs.phase_route_locked = !matches!(
                    phase_decision.candidate.backend,
                    PhaseBackend::LegacyCategory
                );
                if let Some(persisted_task) =
                    tasks.iter_mut().find(|task| task.id == worker_task_id)
                {
                    persisted_task.inputs.phase_route_locked =
                        worker_task.inputs.phase_route_locked;
                }
                store.write_tasks(&goal_id, &tasks)?;
            }
            if prestarted_worker.is_none() && first_plan_attempt {
                if let Some(plan_task) = worker_task.inputs.plan_task.as_ref() {
                    if matches!(
                        plan_task.test.strategy,
                        crate::plan_graph::TestStrategy::Tdd
                    ) {
                        let red_path = run_plan_red_evidence(
                            &workspace,
                            &store,
                            &goal_id,
                            &plan_task_id,
                            plan_graph.revision,
                            plan_task,
                            options.cancellation_token.as_ref(),
                        )?;
                        let node = plan_node_runs.node_mut(&plan_task_id)?;
                        node.red_evidence_path = Some(red_path.to_string_lossy().to_string());
                        node.status = PlanNodeRunStatus::RedVerified;
                        node.updated_at = timestamp();
                        plan_node_runs.updated_at = timestamp();
                        store.write_plan_node_runs(&plan_node_runs)?;
                    }
                }
            }
            if let Some(plan_task) = worker_task.inputs.plan_task.as_mut() {
                plan_task.task_id = worker_task_id.clone();
                if let Some(reviewed_execution_id) = reviewed_execution_id.as_deref() {
                    plan_task.preferred_phase_profile = PhaseProfile::ReviewerFinal;
                    plan_task.goal =
                        format!("Independently review executor execution {reviewed_execution_id}");
                    plan_task.deliverable =
                        "A typed, evidence-backed final review receipt".to_string();
                    plan_task.must_do = vec![
                        "Inspect the current workspace, verification artifacts, and prior worker evidence"
                            .to_string(),
                        "Return verdicts for every required review dimension".to_string(),
                    ];
                    plan_task.must_not_do = vec![
                        "Do not edit implementation files during the review phase".to_string(),
                        "Do not claim a pass without concrete findings".to_string(),
                    ];
                    plan_task.scope.write_scope.clear();
                    plan_task.completion_predicates = vec![
                        "The receipt binds to the requested executor execution".to_string(),
                        "All four review dimensions contain a verdict and findings".to_string(),
                    ];
                }
            }
            if prestarted_worker.is_none()
                && first_plan_attempt
                && !worker_route_is_review
                && selected_route.require_worker
            {
                dispatch_parallel_plan_siblings(
                    &mut scheduled_plan_wave,
                    &mut prestarted_plan_workers,
                    &mut prestarted_plan_wave,
                    &mut tasks,
                    &mut plan_node_runs,
                    &plan_graph,
                    &goal_id,
                    &epoch_id,
                    task_namespace.as_deref(),
                    &workspace,
                    &store,
                    &phase_runtime,
                    &options,
                    &goal,
                    &detection,
                    &task_manager,
                    &mut lineage,
                    &mut current_plan_wave,
                    &budget_controller,
                    worker_call_count,
                    premium_worker_call_count,
                    attempt_count,
                    run_started_at,
                    &goal_run_lease,
                    iteration,
                    &session_id,
                )?;
                lineage.updated_at = timestamp();
                store.write_lineage(&lineage)?;
            }
            let base_worker_request = if first_plan_attempt {
                options.request.clone()
            } else {
                repair_request(
                    &options.request,
                    iteration,
                    last_verification_path.as_deref(),
                    last_coordinator_review.as_ref(),
                )
            };
            let worker_request = reviewed_execution_id
                .as_deref()
                .map_or(base_worker_request.clone(), |id| {
                    review_worker_request(&base_worker_request, id)
                });
            repair_request_history.push(worker_request.clone());
            let resumed_managed_worker_task_id = if !first_plan_attempt && !worker_route_is_review {
                try_resume_plan_session(
                    &task_manager,
                    &store,
                    &goal_id,
                    &epoch_id,
                    &plan_graph,
                    &plan_task_id,
                    route_attempt,
                    &selected_route,
                    &worker_request,
                )?
            } else {
                None
            };
            if let Some(resumed_task_id) = resumed_managed_worker_task_id.as_deref() {
                store.append_goal_epoch_event(
                    &goal_id,
                    &epoch_id,
                    &format!("{epoch_id}.worker.{iteration}.follow_up"),
                    GoalEpochEventKind::PhaseCompleted,
                    json!({
                        "phase": "worker_follow_up",
                        "task_id": plan_task_id,
                        "managed_worker_task_id": resumed_task_id,
                        "attempt": route_attempt,
                        "role": selected_route.worker_kind.as_str(),
                    }),
                )?;
            }
            check_phase_terminal_state(&phase_decision)
                .context("worker phase terminal state check failed")?;
            let (executor_broker_arc, executor_broker_identity) =
                if prestarted_worker.is_some() || resumed_managed_worker_task_id.is_some() {
                    (None, None)
                } else if let Some(factory) = phase_runtime.broker_factory.as_deref() {
                    let identity = PhaseExecutionIdentity {
                        execution_id: format!("executor_iter_{}", iteration),
                        phase_session_id: format!("executor_iter_{}", iteration),
                        backend: PhaseExecutionBackend::DeterministicRules,
                        agent_id: None,
                        provider_id: None,
                        model_id: None,
                        actual_session_id: None,
                    };
                    let broker = factory
                        .create_broker(
                            &phase_decision,
                            &goal_id,
                            &plan_graph.plan_id,
                            plan_graph.revision,
                            &worker_task_id,
                            &identity,
                        )
                        .context("failed to create per-phase broker for executor")?;
                    (Some(broker), Some(identity))
                } else {
                    (phase_runtime.broker.clone(), None)
                };
            let executor_broker: Option<&WorkerBroker> = executor_broker_arc.as_deref();
            if let Some(broker) = executor_broker {
                let phase_request = build_broker_phase_request(
                    &phase_decision,
                    &goal_id,
                    &plan_graph.plan_id,
                    plan_graph.revision,
                    &worker_task_id,
                )?;
                broker
                    .resolve(phase_request)
                    .context("broker resolve failed for executor phase")?;
            }
            let managed_worker_task_id = if let Some(worker) = prestarted_worker.as_ref() {
                worker.managed_worker_task_id.clone()
            } else if let Some(resumed_task_id) = resumed_managed_worker_task_id {
                resumed_task_id
            } else {
                let start_result = match task_manager.lock() {
                    Ok(mut task_manager) => {
                        task_manager.set_worker_broker(executor_broker_arc.clone());
                        task_manager.start(WorkerStartRequest {
                            store: &store,
                            workspace: &workspace,
                            task: &worker_task,
                            route_attempt,
                            goal: &worker_request,
                            verification_commands: &detection.verification_commands,
                            config: &effective_worker,
                            cancellation_token: options.cancellation_token.clone(),
                            coordinator_model: goal.coordinator_model.as_ref(),
                            coordinator_brief: goal.coordinator_brief.as_deref(),
                            route_hint: resolved_worker_route_hint,
                        })
                    }
                    Err(_) => {
                        stop_lineage_task(&store, &mut lineage, &worker_task_id)?;
                        bail!("task manager mutex poisoned");
                    }
                };
                match start_result {
                    Ok(task_id) => {
                        #[cfg(test)]
                        test_seams::increment_worker_dispatch();
                        if let Some(wave_ledger) = current_plan_wave.as_mut() {
                            wave_ledger.mark_dispatched(
                                &plan_task_id,
                                route_attempt,
                                task_id.clone(),
                            )?;
                            wave_ledger.mark_started(&plan_task_id)?;
                            store.write_plan_wave_run(wave_ledger)?;
                        }
                        task_id
                    }
                    Err(error) => {
                        if let Some(broker) = executor_broker {
                            let _ = broker.cancel();
                        }
                        if let Some(ref identity) = executor_broker_identity {
                            if let Some(ref factory) = phase_runtime.broker_factory {
                                let _ = factory.remove_session(
                                    identity,
                                    &goal_id,
                                    &worker_task_id,
                                    plan_graph.revision,
                                );
                            }
                        }
                        stop_lineage_task(&store, &mut lineage, &worker_task_id)?;
                        return Err(error)
                            .context("ownership: worker start failed, goal remains incomplete");
                    }
                }
            };
            persist_live_plan_node_session_binding(
                &store,
                &goal_id,
                &epoch_id,
                &plan_graph,
                &plan_task_id,
                route_attempt,
                &managed_worker_task_id,
                &selected_route,
                &route_receipt.receipt_hash,
            )?;
            if !lineage.active_task_ids.contains(&worker_task_id) {
                lineage.active_task_ids.push(worker_task_id.clone());
            }
            lineage.updated_at = timestamp();
            store.write_lineage(&lineage)?;
            if options
                .cancellation_token
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                let cancel_result = match task_manager.lock() {
                    Ok(mut task_manager) => task_manager.cancel_task(&managed_worker_task_id),
                    Err(_) => {
                        stop_lineage_task(&store, &mut lineage, &worker_task_id)?;
                        bail!("task manager mutex poisoned");
                    }
                };
                if let Some(broker) = executor_broker {
                    let _ = broker.cancel();
                }
                stop_lineage_task(&store, &mut lineage, &worker_task_id)?;
                cancel_result?;
                check_run_cancelled(options.cancellation_token.as_ref())?;
            }
            let managed_worker_run = if let Some(worker) = prestarted_worker.as_mut() {
                worker
                    .completed_run
                    .take()
                    .context("prestarted wave worker was not reduced at the barrier")?
            } else {
                let wait_result = match task_manager.lock() {
                    Ok(mut task_manager) => task_manager.wait_for_with_cancellation(
                        &managed_worker_task_id,
                        options.cancellation_token.as_ref(),
                    ),
                    Err(_) => {
                        stop_lineage_task(&store, &mut lineage, &worker_task_id)?;
                        bail!("task manager mutex poisoned");
                    }
                };
                match wait_result {
                    Ok(Some(run)) => run,
                    Ok(None) => {
                        let cancel_result = match task_manager.lock() {
                            Ok(mut task_manager) => {
                                task_manager.cancel_task(&managed_worker_task_id)
                            }
                            Err(_) => {
                                stop_lineage_task(&store, &mut lineage, &worker_task_id)?;
                                bail!("task manager mutex poisoned");
                            }
                        };
                        if let Some(broker) = executor_broker {
                            let _ = broker.cancel();
                        }
                        stop_lineage_task(&store, &mut lineage, &worker_task_id)?;
                        cancel_result?;
                        check_run_cancelled(options.cancellation_token.as_ref())?;
                        unreachable!(
                            "cancellation-aware worker wait returned without cancellation"
                        );
                    }
                    Err(error) => {
                        stop_lineage_task(&store, &mut lineage, &worker_task_id)?;
                        return Err(error).context("failed while waiting for Gear worker task");
                    }
                }
            };
            // A wave is reduced only after every prestarted sibling has
            // reached a terminal worker state. This prevents verification of
            // the first node from observing a partially-written sibling.
            let sibling_ids = prestarted_plan_wave.iter().cloned().collect::<Vec<_>>();
            for sibling_id in sibling_ids {
                let Some(sibling) = prestarted_plan_workers.get_mut(&sibling_id) else {
                    bail!("prestarted wave worker `{sibling_id}` lost its durable metadata");
                };
                if sibling.completed_run.is_none() {
                    let sibling_route = sibling
                        .effective_worker
                        .selected_route_for_hint(1, Some(sibling.phase_decision.category.as_str()));
                    persist_live_plan_node_session_binding(
                        &store,
                        &goal_id,
                        &epoch_id,
                        &plan_graph,
                        &sibling.plan_task_id,
                        sibling.session_binding_attempt,
                        &sibling.managed_worker_task_id,
                        &sibling_route,
                        &sibling.route_receipt.receipt_hash,
                    )?;
                    let sibling_run = match task_manager.lock() {
                        Ok(mut task_manager) => {
                            task_manager.wait_for(&sibling.managed_worker_task_id)
                        }
                        Err(_) => bail!("task manager mutex poisoned at wave barrier"),
                    }?;
                    if let Some(wave_ledger) = current_plan_wave.as_mut() {
                        let terminal_status = match sibling_run.result.status {
                            WorkerStatus::Succeeded | WorkerStatus::Skipped => {
                                PlanWaveNodeStatus::Completed
                            }
                            WorkerStatus::Failed => PlanWaveNodeStatus::Failed,
                        };
                        let error = (terminal_status == PlanWaveNodeStatus::Failed)
                            .then(|| sibling_run.result.summary.clone());
                        wave_ledger.mark_terminal(&sibling.plan_task_id, terminal_status, error)?;
                        store.write_plan_wave_run(wave_ledger)?;
                    }
                    sibling.completed_run = Some(sibling_run);
                }
            }
            if let Some(broker) = executor_broker {
                if let Ok(state) = broker.current_state() {
                    if state.lifecycle.name() == LifecycleStateName::Active {
                        if let Err(error) = broker.wait_for_outcome() {
                            if let (Some(identity), Some(factory)) = (
                                executor_broker_identity.as_ref(),
                                phase_runtime.broker_factory.as_deref(),
                            ) {
                                let _ = factory.remove_session(
                                    identity,
                                    &goal_id,
                                    &worker_task_id,
                                    plan_graph.revision,
                                );
                            }
                            return Err(error)
                                .context("broker wait_for_outcome failed for executor");
                        }
                    }
                }
            }
            if let (Some(broker), Some(identity), Some(factory)) = (
                executor_broker,
                executor_broker_identity.as_ref(),
                phase_runtime.broker_factory.as_deref(),
            ) {
                factory
                    .finalize_session(
                        broker,
                        identity,
                        &goal_id,
                        &worker_task_id,
                        plan_graph.revision,
                    )
                    .context("broker terminal ledger finalization failed for executor")?;
            }
            let settled_budget_usage = if let Some(broker) = executor_broker {
                match broker.latest_receipt()?.and_then(|receipt| receipt.usage) {
                    Some(usage) => {
                        let usage_incomplete = usage.requested_tokens.is_none()
                            || usage.actual_tokens.is_none()
                            || usage.cost_micros.is_none();
                        SettledBudgetUsage {
                            requested_tokens: usage.requested_tokens,
                            actual_tokens: usage.actual_tokens,
                            cost_micros: usage.cost_micros,
                            duration_ms: usage.duration_ms,
                            cache_hit: usage.cache_hit,
                            unavailable_reason: usage.unavailable_reason.or_else(|| {
                                usage_incomplete
                                    .then(|| "broker receipt reported incomplete usage".to_string())
                            }),
                        }
                    }
                    None => SettledBudgetUsage {
                        requested_tokens: None,
                        actual_tokens: None,
                        cost_micros: None,
                        duration_ms: None,
                        cache_hit: None,
                        unavailable_reason: Some(
                            "broker terminal receipt omitted usage".to_string(),
                        ),
                    },
                }
            } else {
                SettledBudgetUsage {
                    requested_tokens: None,
                    actual_tokens: None,
                    cost_micros: None,
                    duration_ms: None,
                    cache_hit: None,
                    unavailable_reason: Some(
                        "worker backend does not expose usage receipts".to_string(),
                    ),
                }
            };
            let settled_reservation = store
                .settle_budget_call(
                    &goal_run_lease,
                    &budget_reservation_id,
                    settled_budget_usage,
                )
                .context("failed to settle worker budget reservation")?;
            store.append_goal_epoch_event(
                &goal_id,
                &epoch_id,
                &format!("{budget_reservation_id}.settled"),
                GoalEpochEventKind::BudgetSettled,
                json!({
                    "reservation_id": budget_reservation_id,
                    "usage": settled_reservation.usage,
                }),
            )?;
            let worker_session_id = managed_worker_run.record.session_id.clone();
            let worker_task_record = managed_worker_run.record;
            let iteration_worker_outcome = managed_worker_run.outcome;
            let iteration_worker_result = managed_worker_run.result;
            let iteration_worker_result_for_risk = iteration_worker_result.clone();
            if let Some(session_id) = worker_session_id.clone() {
                let binding = crate::state::PlanNodeSessionBinding {
                    schema_version: crate::state::PLAN_NODE_SESSION_BINDING_SCHEMA_VERSION,
                    binding_id: format!("{goal_id}:{epoch_id}:{plan_task_id}:{}", route_attempt),
                    goal_id: goal_id.clone(),
                    epoch_id: epoch_id.clone(),
                    plan_id: plan_graph.plan_id.clone(),
                    plan_revision: plan_graph.revision,
                    plan_hash: plan_graph.plan_hash.clone(),
                    task_id: plan_task_id.clone(),
                    attempt: route_attempt,
                    worker_task_id: managed_worker_task_id.clone(),
                    worker_kind: selected_route.worker_kind.as_str().to_string(),
                    provider_id: executor_broker_identity
                        .as_ref()
                        .and_then(|identity| identity.provider_id.clone())
                        .or_else(|| {
                            selected_route
                                .worker_model
                                .and_then(|model| model.split_once('/'))
                                .map(|(provider, _)| provider.to_string())
                        }),
                    model_id: selected_route.worker_model.map(str::to_string),
                    session_id,
                    capability_fingerprint: iteration_worker_outcome
                        .session_capability
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                    route_receipt_hash: Some(route_receipt.receipt_hash.clone()),
                    status: match worker_task_record.residency_state {
                        crate::task_manager::ResidencyState::Resident
                        | crate::task_manager::ResidencyState::RpcDetached => {
                            crate::state::PlanNodeSessionBindingStatus::Active
                        }
                        crate::task_manager::ResidencyState::Evicted => {
                            crate::state::PlanNodeSessionBindingStatus::Suspended
                        }
                        crate::task_manager::ResidencyState::Disposed
                        | crate::task_manager::ResidencyState::PersistedOnly => {
                            crate::state::PlanNodeSessionBindingStatus::Terminal
                        }
                    },
                    supersedes_binding_id: (route_attempt > 1).then(|| {
                        format!(
                            "{goal_id}:{epoch_id}:{plan_task_id}:{}",
                            route_attempt.saturating_sub(1)
                        )
                    }),
                    created_at: timestamp(),
                    updated_at: timestamp(),
                };
                store.write_plan_node_session_binding(&binding)?;
            }
            if prestarted_worker.is_none() {
                if let Some(wave_ledger) = current_plan_wave.as_mut() {
                    let terminal_status = match iteration_worker_result.status {
                        WorkerStatus::Succeeded | WorkerStatus::Skipped => {
                            PlanWaveNodeStatus::Completed
                        }
                        WorkerStatus::Failed => PlanWaveNodeStatus::Failed,
                    };
                    let error = (terminal_status == PlanWaveNodeStatus::Failed)
                        .then(|| iteration_worker_result.summary.clone());
                    wave_ledger.mark_terminal(&plan_task_id, terminal_status, error)?;
                    store.write_plan_wave_run(wave_ledger)?;
                    let wave_is_terminal = wave_ledger.status.is_terminal();
                    if wave_is_terminal {
                        current_plan_wave = None;
                    }
                }
            }
            let (plan_green_paths, plan_green_passed) = if first_plan_attempt
                && iteration_worker_result.status == WorkerStatus::Succeeded
            {
                run_plan_green_evidence(
                    &workspace,
                    &store,
                    &goal_id,
                    &plan_task_id,
                    plan_graph.revision,
                    plan_graph
                        .task(&plan_task_id)
                        .context("missing PlanGraph task for GREEN evidence")?,
                    options.cancellation_token.as_ref(),
                )?
            } else {
                (
                    Vec::new(),
                    iteration_worker_result.status == WorkerStatus::Succeeded
                        || (iteration_worker_result.status == WorkerStatus::Skipped
                            && !options.worker.require_worker),
                )
            };
            {
                let node = plan_node_runs.node_mut(&plan_task_id)?;
                if !plan_green_paths.is_empty() {
                    node.green_evidence_paths = plan_green_paths
                        .iter()
                        .map(|path| path.to_string_lossy().to_string())
                        .collect();
                }
                if plan_green_passed {
                    node.status = PlanNodeRunStatus::GreenVerified;
                } else if iteration_worker_result.status != WorkerStatus::Succeeded {
                    node.status = PlanNodeRunStatus::Failed;
                }
                node.updated_at = timestamp();
                plan_node_runs.updated_at = timestamp();
                store.write_plan_node_runs(&plan_node_runs)?;
            }
            let phase_route_ordinal = prestarted_worker
                .as_ref()
                .map(|worker| worker.phase_route_ordinal)
                .unwrap_or(100 + iteration);
            let phase_route_receipt = phase_route_receipt_for_worker(
                &phase_decision,
                phase_route_ordinal,
                &goal_id,
                &plan_graph,
                &worker_task_id,
                worker_session_id.as_deref(),
                &worker_task_record,
                &store,
            )?;
            let phase_route_receipt_path = store.write_phase_route_receipt(
                &goal_id,
                phase_route_ordinal,
                &phase_route_receipt,
            )?;
            if worker_route_hint != Some("review") {
                last_executor_evidence = Some(FinalReviewInputEvidence {
                    execution_id: worker_session_id
                        .clone()
                        .unwrap_or_else(|| worker_task_id.clone()),
                    result_path: iteration_worker_result.result_path.clone(),
                    outcome_path: iteration_worker_result.outcome_path.clone(),
                });
            }
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

            if let Some(worker_session_id) = worker_session_id.as_ref()
                && !lineage.worker_session_ids.contains(worker_session_id)
            {
                lineage.worker_session_ids.push(worker_session_id.clone());
            }

            // Worker has completed (success, failure, or skip); remove from
            // active_task_ids so lineage no longer blocks completion on this worker.
            lineage.active_task_ids.retain(|id| id != &worker_task_id);
            lineage.plan_remaining_items =
                usize::from(iteration_worker_result.status != WorkerStatus::Succeeded);
            lineage.updated_at = timestamp();
            store.write_lineage(&lineage)?;

            // If this was a review iteration, complete the pending review task
            // from the previous iteration that triggered this review.
            if worker_route_hint == Some("review") && iteration > 1 {
                let prev_review_id = review_task_id(iteration - 1, task_namespace.as_deref());
                if let Some(review_task) = tasks.iter_mut().find(|t| t.id == prev_review_id) {
                    review_task.status = TaskStatus::Complete;
                    review_task.assigned_worker =
                        Some(selected_route.worker_kind.as_str().to_string());
                    review_task.outputs.evidence.push(
                        iteration_worker_result
                            .outcome_path
                            .to_string_lossy()
                            .to_string(),
                    );
                }
            }

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
                        "resolved_route_hint": resolved_worker_route_hint,
                        "worker_kind": selected_route.worker_kind.as_str(),
                        "worker_model": selected_route.worker_model,
                        "worker_category": selected_route.category.as_str(),
                        "route_reason": &selected_route.route_reason,
                        "phase": worker_phase,
                        "phase_route_decision_path": phase_decision_path.to_string_lossy(),
                        "phase_route_receipt_path": phase_route_receipt_path.to_string_lossy(),
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
            store.append_goal_epoch_event(
                &goal_id,
                &epoch_id,
                &format!("{epoch_id}.worker.{iteration}.completed"),
                GoalEpochEventKind::PhaseCompleted,
                json!({
                    "phase": "worker",
                    "iteration": iteration,
                    "task_id": worker_task_id,
                    "status": iteration_worker_result.status.as_str(),
                    "worker_session_id": worker_session_id,
                    "worker_kind": selected_route.worker_kind.as_str(),
                    "worker_model": selected_route.worker_model,
                    "outcome_path": iteration_worker_result.outcome_path.to_string_lossy(),
                }),
            )?;
            let worker_failed = iteration_worker_result.status == WorkerStatus::Failed;
            worker_result = Some(iteration_worker_result);
            final_worker_outcome = Some(iteration_worker_outcome.clone());
            worker_output_history.push(iteration_worker_outcome.summary.clone());
            if options.continuation {
                store.update_continuation_guard(&session_id, &goal_id, &epoch_id, |guard| {
                    guard.in_flight = false;
                    guard.background_pending = false;
                    if worker_failed {
                        guard.consecutive_failures = guard.consecutive_failures.saturating_add(1);
                    } else {
                        guard.consecutive_failures = 0;
                    }
                })?;
            }
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
            let reviewer_changed_workspace =
                review_changed_workspace(worker_route_hint, &before_diff, &after_diff);
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

            start_task(&mut tasks, &verification_task_id);
            goal.status = GoalStatus::Verifying;
            goal.current_task_id = Some(verification_task_id.clone());
            goal.updated_at = timestamp();
            store.write_goal(&goal)?;
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&verification_task_id),
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

            let verification_passed = plan_green_passed
                && !verification_results.is_empty()
                && verification_results.iter().all(|result| result.success);
            update_verification_task(
                &mut tasks,
                &verification_task_id,
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
                    Some(&verification_task_id),
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
            let coordinator_review_budget_key = format!("coordinator-review.{iteration}");
            let coordinator_review_budget = if options.coordinator_review_hook.is_some() {
                reserve_planning_phase_budget(
                    &mut goal,
                    &store,
                    Some((&goal_run_lease, &epoch_id)),
                    &coordinator_review_budget_key,
                )?
            } else {
                None
            };
            let coordinator_review_result = run_coordinator_review(
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
            );
            settle_planning_phase_budget(
                &goal,
                &store,
                Some((&goal_run_lease, &epoch_id)),
                coordinator_review_budget.as_deref(),
                &coordinator_review_budget_key,
            )?;
            let coordinator_review = coordinator_review_result?;
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
            if reviewer_changed_workspace {
                context_risk_signals.push(
                    "review_mutation: the read-only reviewer changed the workspace".to_string(),
                );
            }
            let budget_snapshot = BudgetSnapshot {
                context_risk_signals,
                ..budget_snapshot_for_review
            };
            context_pressure_seen |= !budget_snapshot.context_risk_signals.is_empty();
            if options.continuation {
                let token_limit_detected =
                    budget_snapshot.context_risk_signals.iter().any(|signal| {
                        let signal = signal.to_ascii_lowercase();
                        signal.contains("token limit") || signal.contains("context limit")
                    });
                store.update_continuation_guard(&session_id, &goal_id, &epoch_id, |guard| {
                    guard.context_pressure |= !budget_snapshot.context_risk_signals.is_empty();
                    guard.token_limit_detected |= token_limit_detected;
                    guard.stagnation_count = if no_progress_signals.is_empty() {
                        0
                    } else {
                        guard.stagnation_count.saturating_add(1)
                    };
                    guard.last_progress_marker = store.continuation_progress_marker(&goal_id).ok();
                })?;
            }
            if worker_failed {
                store.record_prompt_settle_decision(
                    &goal_id,
                    &worker_task_id,
                    &session_id,
                    iteration,
                    "runtime.worker",
                    PromptSettleEvent::Error,
                )?;
            }
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
            let mut evaluation = evaluate_goal_with_review_target(
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
                Some(&lineage),
                reviewed_execution_id.as_deref(),
                &worker_task_record.attempts,
            );
            let node_review_pending = phase_runtime.require_plan_approval
                && plan_graph.draft.tasks.len() > 1
                && !worker_route_is_review
                && plan_green_passed
                && verification_passed
                && worker_result
                    .as_ref()
                    .is_some_and(|result| result.status == WorkerStatus::Succeeded)
                && plan_node_runs
                    .nodes
                    .iter()
                    .find(|node| node.task_id == plan_task_id)
                    .is_some_and(|node| node.review_evidence_path.is_none());
            if node_review_pending && iteration < max_iterations {
                evaluation = GoalEvaluation {
                    status: GoalStatus::Running,
                    should_continue: true,
                    summary: format!(
                        "Plan node {plan_task_id} passed GREEN; a fresh node reviewer must run before completion."
                    ),
                    route_hint_override: Some("review".to_string()),
                };
            }
            if evaluation.status == GoalStatus::Complete {
                if let Some(receipt_failure) = verify_broker_receipts_for_goal(
                    phase_runtime.broker.as_deref(),
                    phase_runtime.broker_factory.as_deref(),
                    &goal_id,
                    true,
                ) {
                    evaluation = GoalEvaluation {
                        status: GoalStatus::NeedsUser,
                        should_continue: false,
                        summary: format!(
                            "Broker receipt gate blocked completion: {receipt_failure}"
                        ),
                        route_hint_override: None,
                    };
                }
            }
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
                    reviewed_execution_id.as_deref(),
                    &worker_task_record.attempts,
                ),
            )?;
            let review_gate = ReviewGate::from_inputs_for_execution(
                verification_passed,
                &worker_result
                    .as_ref()
                    .context("missing worker result for review gate")?
                    .status,
                &scope_check,
                coordinator_review,
                &budget_snapshot.context_risk_signals,
                reviewed_execution_id.as_deref(),
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
                selected_route.worker_kind.as_str(),
                task_namespace.as_deref(),
            );
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&review_task_id(iteration, task_namespace.as_deref())),
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

            store.append_goal_epoch_event(
                &goal_id,
                &epoch_id,
                &format!("{epoch_id}.review.{iteration}.completed"),
                GoalEpochEventKind::PhaseCompleted,
                json!({
                    "phase": "review",
                    "iteration": iteration,
                    "status": evaluation.status.as_str(),
                    "should_continue": evaluation.should_continue,
                    "review_path": review_path.to_string_lossy(),
                }),
            )?;

            let node_review_passed = verification_passed
                && worker_result
                    .as_ref()
                    .is_some_and(|result| result.status == WorkerStatus::Succeeded)
                && review_gate.failed_reason().is_none();
            if node_review_passed {
                let node = plan_node_runs.node_mut(&plan_task_id)?;
                node.status = PlanNodeRunStatus::Reviewed;
                node.review_evidence_path = review_gate
                    .results
                    .iter()
                    .find_map(|result| {
                        result
                            .reviewer_evidence
                            .as_ref()
                            .and_then(|evidence| evidence.artifact_path.clone())
                    })
                    .or_else(|| Some(review_path.to_string_lossy().to_string()));
                if worker_route_is_review {
                    node.review_task_id = Some(worker_task_id.clone());
                }
                node.status = PlanNodeRunStatus::Completed;
                node.updated_at = timestamp();
                plan_node_runs.updated_at = timestamp();
                plan_node_runs.validate()?;
                store.write_plan_node_runs(&plan_node_runs)?;
                completed_plan_tasks.insert(plan_task_id.clone());
                current_plan_task_id = None;
                lineage.plan_remaining_items = plan_graph
                    .draft
                    .tasks
                    .len()
                    .saturating_sub(completed_plan_tasks.len());
                lineage.updated_at = timestamp();
                store.write_lineage(&lineage)?;
            }
            let all_plan_tasks_completed =
                completed_plan_tasks.len() == plan_graph.draft.tasks.len();
            if evaluation.status == GoalStatus::Complete && !all_plan_tasks_completed {
                evaluation = GoalEvaluation {
                    status: GoalStatus::Running,
                    should_continue: true,
                    summary: format!(
                        "Plan node {} completed; {} node(s) remain.",
                        plan_task_id,
                        plan_graph.draft.tasks.len() - completed_plan_tasks.len()
                    ),
                    route_hint_override: None,
                };
            } else if all_plan_tasks_completed && evaluation.status == GoalStatus::Complete {
                evaluation.summary = format!(
                    "{} All {} PlanGraph nodes completed with evidence.",
                    evaluation.summary,
                    completed_plan_tasks.len()
                );
            }
            let should_continue = evaluation.should_continue || !all_plan_tasks_completed;
            final_evaluation = Some(evaluation);
            if !should_continue {
                break;
            }

            before_diff = after_diff.clone();
        }

        let mut final_evaluation =
            final_evaluation.context("Gear loop did not evaluate the goal")?;
        let worker_result = worker_result.context("Gear loop did not produce a worker result")?;
        let final_worker_outcome =
            final_worker_outcome.context("Gear loop did not produce worker outcome evidence")?;
        let final_task_id = goal.current_task_id.clone();
        persist_plan_criterion_evidence(&store, &workspace, &plan_graph, &mut plan_node_runs)?;
        let criteria_complete = plan_node_runs.all_criteria_passed(&plan_graph);
        let final_wave_receipt = build_final_verification_wave(
            &goal_id,
            &epoch_id,
            &plan_graph,
            &plan_node_runs,
            &worker_result,
            &final_worker_outcome,
            &verification_results,
            last_verification_path.as_deref(),
            &scope_check,
            criteria_complete,
        )?;
        if final_evaluation.status == GoalStatus::Complete && !final_wave_receipt.passed {
            final_evaluation = GoalEvaluation {
                status: GoalStatus::NeedsUser,
                should_continue: false,
                summary: format!(
                    "Final Verification Wave did not pass: {}",
                    final_wave_receipt
                        .dimensions
                        .iter()
                        .filter(|result| !result.passed)
                        .map(|result| format!("{:?}", result.dimension))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                route_hint_override: None,
            };
        }
        if context_pressure_seen && final_evaluation.status == GoalStatus::NeedsUser {
            store.record_prompt_settle_decision(
                &goal_id,
                final_task_id.as_deref().unwrap_or("continuation"),
                &session_id,
                max_iterations,
                "runtime.continuation.final",
                PromptSettleEvent::ContextPressure,
            )?;
        }
        if options.continuation {
            let completed = final_evaluation.status == GoalStatus::Complete;
            store.update_continuation_guard(&session_id, &goal_id, &epoch_id, |guard| {
                guard.in_flight = false;
                guard.background_pending = false;
                guard.all_todos_completed = completed;
            })?;
        }
        goal.status = final_evaluation.status;
        goal.current_task_id = None;
        goal.updated_at = timestamp();
        goal.summary = final_evaluation.summary;

        let final_wave_path = store.write_artifact(
            &goal_id,
            "final-verification-wave.json",
            &format!("{}\n", serde_json::to_string_pretty(&final_wave_receipt)?),
        )?;
        store.append_goal_epoch_event(
            &goal_id,
            &epoch_id,
            &format!("{epoch_id}.final-verification-wave.completed"),
            GoalEpochEventKind::PhaseCompleted,
            json!({
                "phase": "final_verification_wave",
                "passed": final_wave_receipt.passed,
                "receipt_hash": final_wave_receipt.receipt_hash,
                "receipt_path": final_wave_path.to_string_lossy(),
            }),
        )?;
        let final_report = format!(
            "{}\n\n{}",
            product::final_report(
                &goal,
                &tasks,
                &worker_result,
                &after_diff,
                &scope_check,
                &verification_results,
            ),
            final_verification_wave_markdown(&final_wave_receipt),
        );
        let final_report_path = store.write_artifact(&goal_id, "final-report.md", &final_report)?;
        let mut strategist_prior_execution_ids = Vec::new();
        if let Some(planner_session_id) = plan_graph
            .planner
            .as_ref()
            .and_then(|planner| planner.session_id.clone())
        {
            strategist_prior_execution_ids.push(planner_session_id);
        }
        if let Some(executor_execution_id) = last_executor_evidence
            .as_ref()
            .map(|evidence| evidence.execution_id.clone())
        {
            strategist_prior_execution_ids.push(executor_execution_id);
        }
        let strategist_receipt = run_strategist_next_goal(
            &mut goal,
            &epoch_id,
            &plan_graph,
            &final_report_path,
            &store,
            &session_id,
            &options.event_sink,
            &phase_runtime,
            &goal_run_lease,
            &strategist_prior_execution_ids,
        )?;
        complete_task(&mut tasks, &report_task_id, |task| {
            task.outputs.summary = "Final report artifact created.".to_string();
            task.outputs
                .evidence
                .push(final_report_path.to_string_lossy().to_string());
        });
        lineage.active_task_ids.clear();
        if goal.status == GoalStatus::Complete {
            lineage.plan_remaining_items = 0;
            lineage.status = ContinuationStatus::Completed;
        } else {
            lineage.status = ContinuationStatus::Stopped;
        }
        lineage.updated_at = timestamp();
        store.write_lineage(&lineage)?;
        store.write_goal(&goal)?;
        store.write_tasks(&goal_id, &tasks)?;
        if options.continuation {
            let continuation_status = if store.continuation_is_stopped_for_session(&session_id)? {
                ContinuationStatus::Stopped
            } else if goal.status == GoalStatus::Complete {
                ContinuationStatus::Completed
            } else {
                // Keep unfinished work recoverable. The persisted progress
                // marker and bounded resume counter decide whether a later
                // invocation may continue or must surface a stuck decision.
                ContinuationStatus::Running
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

        store.append_goal_epoch_event(
            &goal_id,
            &epoch_id,
            &format!("{epoch_id}.settled"),
            GoalEpochEventKind::Settled,
            json!({
                "status": goal.status.as_str(),
                "summary": goal.summary,
                "final_report_path": final_report_path.to_string_lossy(),
            }),
        )?;
        #[cfg(test)]
        if test_seams::should_crash_at(test_seams::ObjectiveCrashPoint::BeforeOutcomeReceipt) {
            bail!("test seam: simulated crash before objective outcome receipt");
        }
        if let Some(objective_context) = objective_context {
            let budget_ledger = store.read_goal_budget_ledger(&goal_id)?;
            let strategist_receipt_path = strategist_receipt.as_ref().map(|_| {
                store
                    .artifact_dir(&goal_id)
                    .join("strategist-next-goal-receipt.json")
                    .to_string_lossy()
                    .to_string()
            });
            let strategist_receipt_hash = strategist_receipt
                .as_ref()
                .map(|receipt| receipt.receipt_hash.clone());
            let strategist_decision = strategist_receipt
                .as_ref()
                .map(|receipt| format!("{:?}", receipt.verdict.decision));
            let outcome_receipt = ObjectiveEpochOutcomeReceipt::seal(
                &objective_context.objective_id,
                &goal_id,
                &epoch_id,
                &session_id,
                hash_text(&goal.request),
                objective_context.scope_hash,
                objective_context.policy_hash,
                goal.status.clone(),
                final_wave_path.to_string_lossy().to_string(),
                final_wave_receipt.receipt_hash.clone(),
                final_report_path.to_string_lossy().to_string(),
                sha256_file(&final_report_path)?,
                budget_ledger.ledger_hash,
                strategist_receipt_path,
                strategist_receipt_hash,
                strategist_decision,
            )?;
            store.write_objective_epoch_outcome(&outcome_receipt)?;
            #[cfg(test)]
            if test_seams::should_crash_at(
                test_seams::ObjectiveCrashPoint::AfterOutcomeReceiptBeforeGraph,
            ) {
                bail!("test seam: simulated crash after outcome receipt before graph commit");
            }
        }
        #[cfg(test)]
        test_seams::goal_settled(&goal_id, &epoch_id);
        goal_run_lease.release()?;
        #[cfg(test)]
        test_seams::goal_lease_released(&goal_id, &epoch_id);

        let status = goal.status;
        Ok(RunOutcome {
            goal_id,
            epoch_id,
            session_id: session_id.clone(),
            status,
            artifacts_root,
            final_report_path,
            events_path: store.events_path(&session_id),
            final_verification_wave_path: final_wave_path,
            final_verification_wave_hash: final_wave_receipt.receipt_hash,
            strategist_receipt,
        })
    }
}

fn run_objective_controller(
    mut options: RunOptions,
    phase_runtime: PhaseRuntime,
    policy: ObjectivePolicy,
) -> Result<ObjectiveRunOutcome> {
    policy.validate()?;
    let workspace = options.workspace.canonicalize().with_context(|| {
        format!(
            "failed to resolve objective workspace {}",
            options.workspace.display()
        )
    })?;
    if !workspace.is_dir() {
        bail!(
            "objective workspace is not a directory: {}",
            workspace.display()
        );
    }
    if options.request.trim().is_empty() {
        bail!("objective request cannot be empty");
    }
    let store = StateStore::new(&workspace);
    store.initialize()?;
    let root_session_id = options
        .session_id
        .clone()
        .unwrap_or_else(|| format!("objective-session_{}", id_timestamp()));
    if options.continuation && store.continuation_is_stopped_for_session(&root_session_id)? {
        bail!(
            "Gear objective continuation is stopped; explicitly restart the continuation before running again"
        );
    }
    let objective_id = objective_id_for(&root_session_id, &workspace, &options.request)?;
    let lease_seconds =
        u64::try_from(options.max_runtime_minutes.max(1).saturating_mul(60)).unwrap_or(u64::MAX);
    let objective_lease = store.acquire_objective_lease(
        &objective_id,
        &root_session_id,
        Duration::from_secs(lease_seconds),
    )?;
    let policy_hash = policy.hash()?;
    let mut graph = if let Some(graph) = store.read_objective_graph(&objective_id)? {
        if graph.policy_hash != policy_hash
            || graph.root_session_id != root_session_id
            || graph.workspace != workspace.to_string_lossy()
        {
            bail!("objective resume policy, session, or workspace binding changed");
        }
        graph
    } else {
        let scope = Scope::new(
            options.allowed_paths.clone(),
            options.forbidden_paths.clone(),
            options.max_files_changed,
        );
        let scope_hash = hash_serialized(&scope)?;
        let graph = ObjectiveGraph::new(
            &objective_id,
            &root_session_id,
            &workspace.to_string_lossy(),
            &options.request,
            &scope_hash,
            policy.clone(),
        )?;
        let path = store.write_objective_graph(&graph)?;
        store.append_objective_event(
            &objective_id,
            "objective.started",
            ObjectiveEventKind::Started,
            json!({
                "root_session_id": root_session_id,
                "graph_path": path.to_string_lossy(),
                "policy_hash": policy_hash,
            }),
        )?;
        graph
    };

    reconcile_objective_frontier(
        &store,
        &objective_id,
        &root_session_id,
        &mut graph,
        Some(&objective_lease),
        &policy,
        &options.budget.clone().unwrap_or_default(),
    )?;
    if graph.status.is_terminal() {
        objective_lease.release()?;
        return Ok(ObjectiveRunOutcome {
            objective_id: objective_id.clone(),
            status: graph.status.clone(),
            graph_path: store.objective_graph_path(&objective_id),
            events_path: store.objective_events_path(&objective_id),
            final_report_path: graph
                .nodes
                .iter()
                .rev()
                .find_map(|node| node.final_report_path.as_deref().map(PathBuf::from)),
            goal_outcomes: Vec::new(),
        });
    }

    if graph.nodes.is_empty() {
        let root_goal_id = format!("goal_{objective_id}_000");
        let root_epoch_id = format!("epoch_{objective_id}_000");
        let root_node = objective_goal_node(
            &root_goal_id,
            &root_epoch_id,
            &root_session_id,
            &options.request,
            Vec::new(),
            None,
            None,
            None,
            GoalStatus::Planning,
            None,
            hash_text(&normalize_objective(&options.request)),
        )?;
        graph.add_root_node(root_node)?;
        store.write_objective_graph(&graph)?;
        store.append_objective_event(
            &objective_id,
            &format!("goal-attached:{root_goal_id}"),
            ObjectiveEventKind::GoalAttached,
            json!({
                "goal_id": root_goal_id,
                "epoch_id": root_epoch_id,
                "session_id": root_session_id,
                "parent_goal_id": Value::Null,
            }),
        )?;
    }

    let mut goal_outcomes = Vec::new();
    loop {
        let active_node = graph
            .active_node()
            .cloned()
            .context("running objective has no active goal frontier")?;
        options.request = active_node.request.clone();
        options.session_id = Some(active_node.session_id.clone());
        options.continuation = true;
        let epoch_reservation_id = format!("epoch:{}", active_node.epoch_id);
        ensure_objective_epoch_reservation(
            &store,
            &objective_lease,
            &graph,
            &active_node,
            &policy,
            &options.budget.clone().unwrap_or_default(),
            &epoch_reservation_id,
        )?;
        let persisted_outcome = store.read_objective_epoch_outcome(
            &objective_id,
            &active_node.goal_id,
            &active_node.epoch_id,
        )?;
        let outcome = if let Some(receipt) = persisted_outcome {
            if receipt.request_hash != hash_text(&active_node.request)
                || receipt.scope_hash != graph.scope_hash
                || receipt.policy_hash != graph.policy_hash
            {
                bail!("objective epoch outcome does not match the active objective binding");
            }
            run_outcome_from_objective_receipt(&store, receipt)?
        } else {
            match Orchestrator::run_single_goal_with_phase_runtime(
                options.clone(),
                phase_runtime.clone(),
                Some(active_node.goal_id.clone()),
                Some(active_node.epoch_id.clone()),
                Some(ObjectiveEpochContext {
                    objective_id: objective_id.clone(),
                    scope_hash: graph.scope_hash.clone(),
                    policy_hash: graph.policy_hash.clone(),
                }),
            ) {
                Ok(outcome) => outcome,
                Err(error) => {
                    #[cfg(test)]
                    if format!("{error:#}").contains("test seam: simulated crash") {
                        return Err(error);
                    }

                    let settlement = settle_failed_objective_goal(
                        &store,
                        &options.event_sink,
                        &objective_id,
                        &mut graph,
                        &active_node,
                        &error,
                    );
                    let lease_release = objective_lease.release();
                    if let Err(settlement_error) = settlement {
                        return Err(error).context(format!(
                            "failed to settle objective state after active goal error: {settlement_error:#}"
                        ));
                    }
                    if let Err(release_error) = lease_release {
                        return Err(error).context(format!(
                            "failed to release objective lease after active goal error: {release_error:#}"
                        ));
                    }
                    return Err(error);
                }
            }
        };
        let reservation_id = epoch_reservation_id;
        if store.objective_budget_ledger_path(&objective_id).exists() {
            #[cfg(test)]
            if active_node.parent_goal_id.is_some()
                && test_seams::should_crash_at(
                    test_seams::ObjectiveCrashPoint::AfterChildOutcomeBeforeObjectiveSettled,
                )
            {
                bail!(
                    "test seam: simulated crash after child outcome before objective budget settlement"
                );
            }
            let (
                actual_calls,
                actual_tokens,
                actual_cost_micros,
                actual_unknown_calls,
                actual_premium_calls,
                cache_hits,
                duration_ms,
                fallback_reasons,
            ) = objective_goal_budget_usage(&store, &outcome.goal_id)?;
            let settled = store.settle_objective_epoch(
                &objective_lease,
                &reservation_id,
                actual_calls,
                actual_tokens,
                actual_cost_micros,
                actual_unknown_calls,
                actual_premium_calls,
                cache_hits,
                duration_ms,
                fallback_reasons,
            )?;
            store.append_objective_event(
                &objective_id,
                &format!("budget-settled:{reservation_id}"),
                ObjectiveEventKind::ObjectiveBudgetSettled,
                json!({
                    "reservation_id": reservation_id,
                    "goal_id": outcome.goal_id,
                    "epoch_id": outcome.epoch_id,
                    "status": "settled",
                    "actual_calls": settled.actual_calls,
                    "actual_tokens": settled.actual_tokens,
                    "actual_cost_micros": settled.actual_cost_micros,
                    "actual_unknown_calls": settled.actual_unknown_calls,
                    "actual_premium_calls": settled.actual_premium_calls,
                }),
            )?;
        }
        #[cfg(test)]
        if test_seams::should_intercept_settled_to_graph_commit() {
            bail!(
                "test seam: simulated crash after goal settled but before objective graph commit"
            );
        }
        let strategist_receipt = outcome.strategist_receipt.clone();
        let strategist_receipt_hash = strategist_receipt
            .as_ref()
            .map(|receipt| receipt.receipt_hash.clone());
        let outcome_report_path = outcome.final_report_path.to_string_lossy().to_string();
        let already_committed = graph
            .nodes
            .iter()
            .find(|node| node.goal_id == outcome.goal_id)
            .is_some_and(|node| {
                node.status.is_terminal()
                    && node.final_wave_receipt_hash.as_deref()
                        == Some(outcome.final_verification_wave_hash.as_str())
                    && node.final_report_path.as_deref() == Some(outcome_report_path.as_str())
            });
        let blocker_decision = if !already_committed && outcome.status == GoalStatus::Blocked {
            let blocker_budget = options.budget.clone().unwrap_or_default();
            promote_final_review_blocker_child(
                &store,
                &objective_id,
                &root_session_id,
                &mut graph,
                &objective_lease,
                &policy,
                &blocker_budget,
                &outcome,
            )?
        } else {
            FinalReviewBlockerDecision::NotPromoted
        };
        let blocker_promoted = blocker_decision == FinalReviewBlockerDecision::Promoted;
        if !already_committed && blocker_decision == FinalReviewBlockerDecision::NotPromoted {
            graph.update_active_node(
                &outcome.goal_id,
                outcome.status.clone(),
                Some(outcome.final_verification_wave_hash.clone()),
                Some(outcome_report_path.clone()),
                strategist_receipt_hash,
                Some(format!("goal status: {}", outcome.status.as_str())),
            )?;
            store.write_objective_graph(&graph)?;
        }
        if let Some(receipt) = store.read_objective_epoch_outcome(
            &objective_id,
            &outcome.goal_id,
            &outcome.epoch_id,
        )? {
            store.append_objective_event(
                &objective_id,
                &format!("goal-outcome:{}:{}", outcome.goal_id, outcome.epoch_id),
                ObjectiveEventKind::GoalOutcomeRecorded,
                json!({
                    "goal_id": outcome.goal_id,
                    "epoch_id": outcome.epoch_id,
                    "receipt_hash": receipt.receipt_hash,
                    "status": outcome.status.as_str(),
                    "final_report_path": outcome.final_report_path.to_string_lossy(),
                    "final_verification_wave_hash": outcome.final_verification_wave_hash,
                    "answerable_now": strategist_receipt
                        .as_ref()
                        .map(|receipt| receipt.verdict.answerable_now),
                }),
            )?;
        }
        #[cfg(test)]
        test_seams::objective_graph_commit(&objective_id, &graph);
        goal_outcomes.push(outcome.clone());

        if blocker_decision == FinalReviewBlockerDecision::NeedsUser {
            let reason =
                "the same normalized external final-review blocker repeated beyond the retry cap";
            graph.set_terminal(ObjectiveStatus::NeedsUser, reason.to_string())?;
            store.write_objective_graph(&graph)?;
            append_objective_terminal_event(
                &store,
                &objective_id,
                &ObjectiveStatus::NeedsUser,
                reason,
                &outcome.goal_id,
            )?;
            break;
        }

        if blocker_promoted {
            continue;
        }

        if outcome.status != GoalStatus::Complete {
            let consecutive_failures = graph.consecutive_failures.saturating_add(1);
            graph.record_failure(consecutive_failures)?;
            let objective_status = if consecutive_failures >= policy.max_consecutive_failures {
                ObjectiveStatus::Limited
            } else {
                objective_status_for_goal(&outcome.status)
            };
            let reason = format!("active goal ended with {}", outcome.status.as_str());
            graph.set_terminal(objective_status.clone(), reason.clone())?;
            store.write_objective_graph(&graph)?;
            append_objective_terminal_event(
                &store,
                &objective_id,
                &objective_status,
                &reason,
                &outcome.goal_id,
            )?;
            break;
        }

        let Some(receipt) = strategist_receipt else {
            let reason = "completed goal has no strategist receipt; objective stops safely";
            graph.set_terminal(ObjectiveStatus::Complete, reason.to_string())?;
            store.write_objective_graph(&graph)?;
            append_objective_terminal_event(
                &store,
                &objective_id,
                &ObjectiveStatus::Complete,
                reason,
                &outcome.goal_id,
            )?;
            break;
        };
        match receipt.verdict.decision {
            StrategistNextGoalDecision::Complete => {
                let reason = "strategist marked the objective complete";
                graph.set_terminal(ObjectiveStatus::Complete, reason.to_string())?;
                store.write_objective_graph(&graph)?;
                append_objective_terminal_event(
                    &store,
                    &objective_id,
                    &ObjectiveStatus::Complete,
                    reason,
                    &outcome.goal_id,
                )?;
                break;
            }
            StrategistNextGoalDecision::NeedsUser => {
                let reason = receipt.verdict.required_questions.join("; ");
                graph.set_terminal(ObjectiveStatus::NeedsUser, reason.clone())?;
                store.write_objective_graph(&graph)?;
                append_objective_terminal_event(
                    &store,
                    &objective_id,
                    &ObjectiveStatus::NeedsUser,
                    &reason,
                    &outcome.goal_id,
                )?;
                break;
            }
            StrategistNextGoalDecision::Stop => {
                let reason = receipt.verdict.rationale.clone();
                graph.set_terminal(ObjectiveStatus::Stopped, reason.clone())?;
                store.write_objective_graph(&graph)?;
                append_objective_terminal_event(
                    &store,
                    &objective_id,
                    &ObjectiveStatus::Stopped,
                    &reason,
                    &outcome.goal_id,
                )?;
                break;
            }
            StrategistNextGoalDecision::Continue => {
                let receipt_idempotency = format!("continue:{}", receipt.receipt_hash);
                store.append_objective_event(
                    &objective_id,
                    &receipt_idempotency,
                    ObjectiveEventKind::StrategistContinueAccepted,
                    json!({
                        "parent_goal_id": outcome.goal_id,
                        "parent_epoch_id": outcome.epoch_id,
                        "receipt_hash": receipt.receipt_hash,
                        "next_objective": receipt.verdict.next_objective,
                        "acceptance_signals": receipt.verdict.acceptance_signals,
                    }),
                )?;
                #[cfg(test)]
                test_seams::continue_event(&objective_id, &receipt.receipt_hash);
                if store.continuation_is_stopped_for_session(&root_session_id)? {
                    let reason =
                        "objective continuation was stopped by the user before child dispatch";
                    graph.set_terminal(ObjectiveStatus::Stopped, reason.to_string())?;
                    store.write_objective_graph(&graph)?;
                    append_objective_terminal_event(
                        &store,
                        &objective_id,
                        &ObjectiveStatus::Stopped,
                        reason,
                        &outcome.goal_id,
                    )?;
                    break;
                }
                if !policy.auto_continue {
                    let reason = "objective auto-continue is disabled by policy";
                    graph.set_terminal(ObjectiveStatus::Stopped, reason.to_string())?;
                    store.write_objective_graph(&graph)?;
                    append_objective_terminal_event(
                        &store,
                        &objective_id,
                        &ObjectiveStatus::Stopped,
                        reason,
                        &outcome.goal_id,
                    )?;
                    break;
                }
                if graph.nodes.len() >= policy.max_epochs {
                    let reason = format!("objective reached max_epochs={}", policy.max_epochs);
                    graph.set_terminal(ObjectiveStatus::Limited, reason.to_string())?;
                    store.write_objective_graph(&graph)?;
                    append_objective_terminal_event(
                        &store,
                        &objective_id,
                        &ObjectiveStatus::Limited,
                        &reason,
                        &outcome.goal_id,
                    )?;
                    break;
                }
                let (calls, tokens, cost, unknown_calls) = objective_budget_totals(&store, &graph)?;
                if calls >= policy.max_calls
                    || tokens >= policy.max_tokens
                    || (policy.max_cost_micros != u64::MAX && cost >= policy.max_cost_micros)
                    || unknown_calls >= policy.max_unknown_usage_calls
                {
                    let reason = format!(
                        "objective budget exhausted: calls={calls}, tokens={tokens}, cost_micros={cost}, unknown_calls={unknown_calls}"
                    );
                    graph.set_terminal(ObjectiveStatus::Limited, reason.to_string())?;
                    store.write_objective_graph(&graph)?;
                    append_objective_terminal_event(
                        &store,
                        &objective_id,
                        &ObjectiveStatus::Limited,
                        &reason,
                        &outcome.goal_id,
                    )?;
                    break;
                }
                if policy.cooldown_seconds > 0
                    && cooldown_remaining_seconds(&graph.updated_at, policy.cooldown_seconds)? > 0
                {
                    let reason = format!(
                        "objective cooldown of {} seconds has not elapsed",
                        policy.cooldown_seconds
                    );
                    graph.set_terminal(ObjectiveStatus::Limited, reason.to_string())?;
                    store.write_objective_graph(&graph)?;
                    append_objective_terminal_event(
                        &store,
                        &objective_id,
                        &ObjectiveStatus::Limited,
                        &reason,
                        &outcome.goal_id,
                    )?;
                    break;
                }
                let next_objective = receipt
                    .verdict
                    .next_objective
                    .clone()
                    .context("continue verdict lost its next objective")?;
                let next_hash = hash_text(&normalize_objective(&next_objective));
                let no_progress = graph
                    .nodes
                    .last()
                    .is_some_and(|node| node.objective_hash == next_hash);
                let consecutive_no_progress = if no_progress {
                    graph.consecutive_no_progress.saturating_add(1)
                } else {
                    0
                };
                graph.record_progress(consecutive_no_progress)?;
                if consecutive_no_progress >= policy.max_consecutive_no_progress {
                    let reason = format!(
                        "objective made no measurable progress for {} consecutive epochs",
                        consecutive_no_progress
                    );
                    graph.set_terminal(ObjectiveStatus::Limited, reason.to_string())?;
                    store.write_objective_graph(&graph)?;
                    append_objective_terminal_event(
                        &store,
                        &objective_id,
                        &ObjectiveStatus::Limited,
                        &reason,
                        &outcome.goal_id,
                    )?;
                    break;
                }
                let child_index = graph.nodes.len();
                let child_goal_id = format!("goal_{objective_id}_{child_index:03}");
                let child_epoch_id = format!("epoch_{objective_id}_{child_index:03}");
                let child_session_id = format!("{root_session_id}.epoch{child_index}");
                let child_budget = options.budget.clone().unwrap_or_default();
                let reserved_calls = child_budget
                    .max_calls_per_epoch
                    .min(policy.max_calls.saturating_sub(calls));
                let reserved_tokens = child_budget
                    .max_tokens_per_epoch
                    .min(policy.max_tokens.saturating_sub(tokens));
                let reserved_cost_micros = if policy.max_cost_micros == u64::MAX {
                    u64::MAX
                } else {
                    policy.max_cost_micros.saturating_sub(cost)
                };
                let reserved_unknown_calls = child_budget
                    .max_usage_unknown_calls
                    .min(policy.max_unknown_usage_calls.saturating_sub(unknown_calls));
                let reservation_id = format!("epoch:{child_epoch_id}");
                store.reserve_objective_epoch(
                    &objective_lease,
                    &reservation_id,
                    &child_goal_id,
                    &child_epoch_id,
                    &policy,
                    reserved_calls,
                    reserved_tokens,
                    reserved_cost_micros,
                    reserved_unknown_calls,
                    child_budget.max_premium_worker_calls,
                )?;
                store.append_objective_event(
                    &objective_id,
                    &format!("child-dispatch-reserved:{child_epoch_id}"),
                    ObjectiveEventKind::ChildDispatchReserved,
                    json!({
                        "reservation_id": reservation_id,
                        "goal_id": child_goal_id,
                        "epoch_id": child_epoch_id,
                        "reserved_calls": reserved_calls,
                        "reserved_tokens": reserved_tokens,
                        "reserved_cost_micros": reserved_cost_micros,
                        "reserved_unknown_calls": reserved_unknown_calls,
                    }),
                )?;
                #[cfg(test)]
                if test_seams::should_crash_at(
                    test_seams::ObjectiveCrashPoint::AfterChildReservationBeforeEdge,
                ) {
                    bail!("test seam: simulated crash after child reservation before graph edge");
                }
                let child_node = objective_goal_node(
                    &child_goal_id,
                    &child_epoch_id,
                    &child_session_id,
                    &objective_child_request(&next_objective, &receipt.verdict.acceptance_signals),
                    receipt.verdict.acceptance_signals.clone(),
                    Some(outcome.goal_id.clone()),
                    Some(outcome.epoch_id.clone()),
                    Some(receipt.receipt_hash.clone()),
                    GoalStatus::Planning,
                    None,
                    next_hash,
                )?;
                if let Err(error) = graph.attach_child(child_node) {
                    store.release_objective_epoch(&objective_lease, &reservation_id)?;
                    return Err(error);
                }
                store.write_objective_graph(&graph)?;
                #[cfg(test)]
                test_seams::child_attach(&objective_id, &child_goal_id);
                store.append_objective_event(
                    &objective_id,
                    &format!("goal-attached:{child_goal_id}"),
                    ObjectiveEventKind::GoalAttached,
                    json!({
                        "goal_id": child_goal_id,
                        "epoch_id": child_epoch_id,
                        "session_id": child_session_id,
                        "parent_goal_id": outcome.goal_id,
                        "parent_epoch_id": outcome.epoch_id,
                        "parent_strategist_receipt_hash": receipt.receipt_hash,
                    }),
                )?;
                store.append_objective_event(
                    &objective_id,
                    &format!("frontier-advanced:{child_goal_id}"),
                    ObjectiveEventKind::FrontierAdvanced,
                    json!({ "active_goal_id": child_goal_id }),
                )?;
                #[cfg(test)]
                if test_seams::should_crash_at(
                    test_seams::ObjectiveCrashPoint::AfterChildEdgeBeforeStarted,
                ) {
                    bail!("test seam: simulated crash after child edge before child started");
                }
            }
        }
    }
    let status = graph.status.clone();
    let graph_path = store.write_objective_graph(&graph)?;
    objective_lease.release()?;
    Ok(ObjectiveRunOutcome {
        objective_id: objective_id.clone(),
        status,
        graph_path,
        events_path: store.objective_events_path(&objective_id),
        final_report_path: goal_outcomes
            .last()
            .map(|outcome| outcome.final_report_path.clone()),
        goal_outcomes,
    })
}

fn reconcile_objective_frontier(
    store: &StateStore,
    objective_id: &str,
    root_session_id: &str,
    graph: &mut ObjectiveGraph,
    objective_lease: Option<&crate::state::ObjectiveLeaseGuard>,
    policy: &ObjectivePolicy,
    budget: &Budget,
) -> Result<()> {
    let mut events = store.read_objective_events(objective_id)?;
    if events.is_empty() {
        store.append_objective_event(
            objective_id,
            "objective.started",
            ObjectiveEventKind::Started,
            json!({ "root_session_id": root_session_id }),
        )?;
        events = store.read_objective_events(objective_id)?;
    }

    for node in &graph.nodes {
        let idempotency_key = format!("goal-attached:{}", node.goal_id);
        if events
            .iter()
            .all(|event| event.idempotency_key != idempotency_key)
        {
            store.append_objective_event(
                objective_id,
                &idempotency_key,
                ObjectiveEventKind::GoalAttached,
                json!({
                    "goal_id": node.goal_id,
                    "epoch_id": node.epoch_id,
                    "session_id": node.session_id,
                    "parent_goal_id": node.parent_goal_id,
                    "parent_epoch_id": node.parent_epoch_id,
                    "parent_strategist_receipt_hash": node.parent_strategist_receipt_hash,
                }),
            )?;
            events = store.read_objective_events(objective_id)?;
        }
    }

    for node in graph.nodes.clone() {
        let receipt = match store.read_objective_epoch_outcome(
            objective_id,
            &node.goal_id,
            &node.epoch_id,
        )? {
            Some(receipt) => receipt,
            None => {
                let Some(receipt) =
                    recover_settled_epoch_outcome_receipt(store, objective_id, graph, &node)?
                else {
                    continue;
                };
                store.write_objective_epoch_outcome(&receipt)?;
                receipt
            }
        };
        if receipt.request_hash != hash_text(&node.request)
            || receipt.scope_hash != graph.scope_hash
            || receipt.policy_hash != graph.policy_hash
        {
            bail!("objective epoch outcome does not match the graph binding");
        }
        if let Some(objective_lease) = objective_lease
            && store.objective_budget_ledger_path(objective_id).exists()
        {
            let (
                actual_calls,
                actual_tokens,
                actual_cost_micros,
                actual_unknown_calls,
                actual_premium_calls,
                cache_hits,
                duration_ms,
                fallback_reasons,
            ) = objective_goal_budget_usage(store, &node.goal_id)?;
            let reservation_id = format!("epoch:{}", node.epoch_id);
            let settled = store.settle_objective_epoch(
                objective_lease,
                &reservation_id,
                actual_calls,
                actual_tokens,
                actual_cost_micros,
                actual_unknown_calls,
                actual_premium_calls,
                cache_hits,
                duration_ms,
                fallback_reasons,
            )?;
            store.append_objective_event(
                objective_id,
                &format!("budget-settled:{reservation_id}"),
                ObjectiveEventKind::ObjectiveBudgetSettled,
                json!({
                    "reservation_id": reservation_id,
                    "goal_id": node.goal_id,
                    "epoch_id": node.epoch_id,
                    "status": "settled",
                    "actual_calls": settled.actual_calls,
                    "actual_tokens": settled.actual_tokens,
                    "actual_cost_micros": settled.actual_cost_micros,
                    "actual_unknown_calls": settled.actual_unknown_calls,
                    "actual_premium_calls": settled.actual_premium_calls,
                }),
            )?;
            events = store.read_objective_events(objective_id)?;
        }
        let outcome = run_outcome_from_objective_receipt(store, receipt.clone())?;
        let outcome_report_path = outcome.final_report_path.to_string_lossy().to_string();
        let blocker_decision = if outcome.status == GoalStatus::Blocked
            && graph.active_goal_id.as_deref() == Some(node.goal_id.as_str())
        {
            objective_lease
                .map(|lease| {
                    promote_final_review_blocker_child(
                        store,
                        objective_id,
                        root_session_id,
                        graph,
                        lease,
                        policy,
                        budget,
                        &outcome,
                    )
                })
                .transpose()?
                .unwrap_or(FinalReviewBlockerDecision::NotPromoted)
        } else {
            FinalReviewBlockerDecision::NotPromoted
        };
        if matches!(
            blocker_decision,
            FinalReviewBlockerDecision::Promoted | FinalReviewBlockerDecision::NeedsUser
        ) {
            let outcome_event_key = format!("goal-outcome:{}:{}", node.goal_id, node.epoch_id);
            store.append_objective_event(
                objective_id,
                &outcome_event_key,
                ObjectiveEventKind::GoalOutcomeRecorded,
                json!({
                    "goal_id": node.goal_id,
                    "epoch_id": node.epoch_id,
                    "receipt_hash": receipt.receipt_hash,
                    "status": outcome.status.as_str(),
                    "final_report_path": outcome.final_report_path.to_string_lossy(),
                    "final_verification_wave_hash": outcome.final_verification_wave_hash,
                }),
            )?;
            if blocker_decision == FinalReviewBlockerDecision::NeedsUser {
                let reason = "the same normalized external final-review blocker repeated beyond the retry cap";
                graph.set_terminal(ObjectiveStatus::NeedsUser, reason.to_string())?;
                store.write_objective_graph(graph)?;
                append_objective_terminal_event(
                    store,
                    objective_id,
                    &ObjectiveStatus::NeedsUser,
                    reason,
                    &node.goal_id,
                )?;
            }
            continue;
        }
        if graph.active_goal_id.as_deref() == Some(node.goal_id.as_str()) {
            graph.update_active_node(
                &node.goal_id,
                outcome.status.clone(),
                Some(outcome.final_verification_wave_hash.clone()),
                Some(outcome_report_path.clone()),
                outcome
                    .strategist_receipt
                    .as_ref()
                    .map(|strategist| strategist.receipt_hash.clone()),
                Some("recovered from objective epoch outcome receipt".to_string()),
            )?;
            store.write_objective_graph(graph)?;
        } else if node.status.is_terminal()
            && (node.final_wave_receipt_hash.as_deref()
                != Some(outcome.final_verification_wave_hash.as_str())
                || node.final_report_path.as_deref() != Some(outcome_report_path.as_str()))
        {
            bail!("objective graph terminal node disagrees with its epoch outcome receipt");
        }
        let outcome_event_key = format!("goal-outcome:{}:{}", node.goal_id, node.epoch_id);
        if events
            .iter()
            .all(|event| event.idempotency_key != outcome_event_key)
        {
            store.append_objective_event(
                objective_id,
                &outcome_event_key,
                ObjectiveEventKind::GoalOutcomeRecorded,
                json!({
                    "goal_id": node.goal_id,
                    "epoch_id": node.epoch_id,
                    "receipt_hash": receipt.receipt_hash,
                    "status": outcome.status.as_str(),
                    "final_report_path": outcome.final_report_path.to_string_lossy(),
                    "final_verification_wave_hash": outcome.final_verification_wave_hash,
                }),
            )?;
            events = store.read_objective_events(objective_id)?;
        }
    }

    if graph.status.is_terminal() {
        let has_terminal_event = events.iter().any(|event| {
            matches!(
                event.kind,
                ObjectiveEventKind::NeedsUser
                    | ObjectiveEventKind::Stopped
                    | ObjectiveEventKind::Limited
                    | ObjectiveEventKind::Blocked
                    | ObjectiveEventKind::Completed
                    | ObjectiveEventKind::Failed
                    | ObjectiveEventKind::Aborted
            )
        });
        if !has_terminal_event {
            let goal_id = graph
                .nodes
                .last()
                .map(|node| node.goal_id.as_str())
                .unwrap_or("none");
            append_objective_terminal_event(
                store,
                objective_id,
                &graph.status,
                graph.stop_reason.as_deref().unwrap_or("objective terminal"),
                goal_id,
            )?;
        }
        return Ok(());
    }

    if graph.nodes.is_empty() || graph.active_goal_id.is_some() {
        return Ok(());
    }

    if let Some(node) = graph.nodes.last().cloned()
        && let Some(outcome) =
            store.read_objective_epoch_outcome(objective_id, &node.goal_id, &node.epoch_id)?
        && let Some(strategist_path) = outcome.strategist_receipt_path.as_deref()
    {
        let strategist: StrategistNextGoalReceipt =
            serde_json::from_str(&std_fs::read_to_string(strategist_path).with_context(|| {
                format!("failed to read terminal strategist receipt {strategist_path}")
            })?)
            .context("failed to parse terminal strategist receipt")?;
        let terminal = match strategist.verdict.decision {
            StrategistNextGoalDecision::Complete => Some((
                ObjectiveStatus::Complete,
                "recovered strategist marked the objective complete".to_string(),
            )),
            StrategistNextGoalDecision::NeedsUser => Some((
                ObjectiveStatus::NeedsUser,
                strategist.verdict.required_questions.join("; "),
            )),
            StrategistNextGoalDecision::Stop => Some((
                ObjectiveStatus::Stopped,
                strategist.verdict.rationale.clone(),
            )),
            StrategistNextGoalDecision::Continue => None,
        };
        if let Some((status, reason)) = terminal {
            graph.set_terminal(status.clone(), reason.clone())?;
            store.write_objective_graph(graph)?;
            append_objective_terminal_event(store, objective_id, &status, &reason, &node.goal_id)?;
            return Ok(());
        }
    }

    let mut continue_event = events
        .iter()
        .rev()
        .find(|event| event.kind == ObjectiveEventKind::StrategistContinueAccepted)
        .cloned();
    if continue_event.is_none()
        && let Some(node) = graph.nodes.last().cloned()
        && let Some(outcome) =
            store.read_objective_epoch_outcome(objective_id, &node.goal_id, &node.epoch_id)?
        && let Some(strategist_path) = outcome.strategist_receipt_path.as_deref()
    {
        let strategist: StrategistNextGoalReceipt =
            serde_json::from_str(&std_fs::read_to_string(strategist_path).with_context(|| {
                format!("failed to read recovered strategist receipt {strategist_path}")
            })?)
            .context("failed to parse recovered strategist receipt")?;
        match strategist.verdict.decision {
            StrategistNextGoalDecision::Continue => {
                store.append_objective_event(
                    objective_id,
                    &format!("continue:{}", strategist.receipt_hash),
                    ObjectiveEventKind::StrategistContinueAccepted,
                    json!({
                        "parent_goal_id": node.goal_id,
                        "parent_epoch_id": node.epoch_id,
                        "receipt_hash": strategist.receipt_hash,
                        "next_objective": strategist.verdict.next_objective,
                        "acceptance_signals": strategist.verdict.acceptance_signals,
                    }),
                )?;
                events = store.read_objective_events(objective_id)?;
                continue_event = events
                    .iter()
                    .rev()
                    .find(|event| event.kind == ObjectiveEventKind::StrategistContinueAccepted)
                    .cloned();
            }
            StrategistNextGoalDecision::Complete => {
                let reason = "recovered strategist marked the objective complete";
                graph.set_terminal(ObjectiveStatus::Complete, reason.to_string())?;
                store.write_objective_graph(graph)?;
                append_objective_terminal_event(
                    store,
                    objective_id,
                    &ObjectiveStatus::Complete,
                    reason,
                    &node.goal_id,
                )?;
                return Ok(());
            }
            StrategistNextGoalDecision::NeedsUser => {
                let reason = strategist.verdict.required_questions.join("; ");
                graph.set_terminal(ObjectiveStatus::NeedsUser, reason.clone())?;
                store.write_objective_graph(graph)?;
                append_objective_terminal_event(
                    store,
                    objective_id,
                    &ObjectiveStatus::NeedsUser,
                    &reason,
                    &node.goal_id,
                )?;
                return Ok(());
            }
            StrategistNextGoalDecision::Stop => {
                let reason = strategist.verdict.rationale.clone();
                graph.set_terminal(ObjectiveStatus::Stopped, reason.clone())?;
                store.write_objective_graph(graph)?;
                append_objective_terminal_event(
                    store,
                    objective_id,
                    &ObjectiveStatus::Stopped,
                    &reason,
                    &node.goal_id,
                )?;
                return Ok(());
            }
        }
    }
    let Some(continue_event) = continue_event else {
        let reason = "objective frontier was completed without a durable strategist continuation";
        graph.set_terminal(ObjectiveStatus::Blocked, reason.to_string())?;
        store.write_objective_graph(graph)?;
        append_objective_terminal_event(
            store,
            objective_id,
            &ObjectiveStatus::Blocked,
            reason,
            graph
                .nodes
                .last()
                .map(|node| node.goal_id.as_str())
                .unwrap_or("none"),
        )?;
        return Ok(());
    };
    let parent_goal_id = continue_event
        .payload
        .get("parent_goal_id")
        .and_then(Value::as_str)
        .context("continuation event is missing parent_goal_id")?;
    let parent_epoch_id = continue_event
        .payload
        .get("parent_epoch_id")
        .and_then(Value::as_str)
        .context("continuation event is missing parent_epoch_id")?;
    let receipt_hash = continue_event
        .payload
        .get("receipt_hash")
        .and_then(Value::as_str)
        .context("continuation event is missing receipt_hash")?;
    let next_objective = continue_event
        .payload
        .get("next_objective")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("continuation event is missing next_objective")?;
    let acceptance_signals = continue_event
        .payload
        .get("acceptance_signals")
        .and_then(Value::as_array)
        .context("continuation event is missing acceptance_signals")?
        .iter()
        .map(|signal| {
            signal
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .map(ToString::to_string)
                .context("continuation event contains an invalid acceptance signal")
        })
        .collect::<Result<Vec<_>>>()?;
    let parent = graph
        .nodes
        .iter()
        .find(|node| node.goal_id == parent_goal_id)
        .context("continuation event references an unknown parent goal")?;
    if parent.status != GoalStatus::Complete
        || parent.epoch_id != parent_epoch_id
        || parent.strategist_receipt_hash.as_deref() != Some(receipt_hash)
        || parent.final_wave_receipt_hash.is_none()
    {
        bail!("continuation event is not bound to a completed parent goal");
    }
    let child_index = graph.nodes.len();
    let child_goal_id = format!("goal_{objective_id}_{child_index:03}");
    let child_epoch_id = format!("epoch_{objective_id}_{child_index:03}");
    let child_session_id = format!("{root_session_id}.epoch{child_index}");
    if graph.nodes.iter().any(|node| node.goal_id == child_goal_id) {
        bail!("objective recovery found a duplicate child goal id");
    }
    let child_node = objective_goal_node(
        &child_goal_id,
        &child_epoch_id,
        &child_session_id,
        &objective_child_request(next_objective, &acceptance_signals),
        acceptance_signals.clone(),
        Some(parent_goal_id.to_string()),
        Some(parent_epoch_id.to_string()),
        Some(receipt_hash.to_string()),
        GoalStatus::Planning,
        None,
        hash_text(&normalize_objective(next_objective)),
    )?;
    let objective_lease = objective_lease
        .context("objective recovery requires a live objective lease before child reservation")?;
    let (calls, tokens, cost, unknown_calls) = objective_budget_totals(store, graph)?;
    let reserved_calls = budget
        .max_calls_per_epoch
        .min(policy.max_calls.saturating_sub(calls));
    let reserved_tokens = budget
        .max_tokens_per_epoch
        .min(policy.max_tokens.saturating_sub(tokens));
    let reserved_cost_micros = if policy.max_cost_micros == u64::MAX {
        budget.max_cost_micros_per_epoch
    } else {
        budget
            .max_cost_micros_per_epoch
            .min(policy.max_cost_micros.saturating_sub(cost))
    };
    let reserved_unknown_calls = budget
        .max_usage_unknown_calls
        .min(policy.max_unknown_usage_calls.saturating_sub(unknown_calls));
    let reservation_id = format!("epoch:{child_epoch_id}");
    store.reserve_objective_epoch(
        objective_lease,
        &reservation_id,
        &child_goal_id,
        &child_epoch_id,
        policy,
        reserved_calls,
        reserved_tokens,
        reserved_cost_micros,
        reserved_unknown_calls,
        budget.max_premium_worker_calls,
    )?;
    if let Err(error) = store.append_objective_event(
        objective_id,
        &format!("child-dispatch-reserved:{child_epoch_id}"),
        ObjectiveEventKind::ChildDispatchReserved,
        json!({
            "reservation_id": reservation_id,
            "goal_id": child_goal_id,
            "epoch_id": child_epoch_id,
            "reserved_calls": reserved_calls,
            "reserved_tokens": reserved_tokens,
            "reserved_cost_micros": reserved_cost_micros,
            "reserved_unknown_calls": reserved_unknown_calls,
        }),
    ) {
        store.release_objective_epoch(objective_lease, &reservation_id)?;
        return Err(error);
    }
    graph.attach_child(child_node)?;
    store.write_objective_graph(graph)?;
    store.append_objective_event(
        objective_id,
        &format!("goal-attached:{child_goal_id}"),
        ObjectiveEventKind::GoalAttached,
        json!({
            "goal_id": child_goal_id,
            "epoch_id": child_epoch_id,
            "session_id": child_session_id,
            "parent_goal_id": parent_goal_id,
            "parent_epoch_id": parent_epoch_id,
            "parent_strategist_receipt_hash": receipt_hash,
        }),
    )?;
    store.append_objective_event(
        objective_id,
        &format!("frontier-advanced:{child_goal_id}"),
        ObjectiveEventKind::FrontierAdvanced,
        json!({ "active_goal_id": child_goal_id }),
    )?;
    Ok(())
}

fn objective_child_request(next_objective: &str, acceptance_signals: &[String]) -> String {
    format!(
        "{}\n\nObjective acceptance signals:\n{}",
        next_objective,
        acceptance_signals
            .iter()
            .map(|signal| format!("- {signal}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FinalReviewBlockerDecision {
    NotPromoted,
    Promoted,
    NeedsUser,
}

const MAX_EXTERNAL_BLOCKER_OCCURRENCES: usize = 2;

fn normalize_blocker_text(value: &str) -> String {
    let mut tokens = Vec::new();
    for raw_token in value.split_whitespace() {
        let lowercase = raw_token.to_ascii_lowercase();
        if lowercase.starts_with("http://") || lowercase.starts_with("https://") {
            tokens.push("<url>".to_string());
            continue;
        }
        let cleaned = raw_token
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || character == '_' {
                    character.to_ascii_lowercase()
                } else {
                    ' '
                }
            })
            .collect::<String>();
        for token in cleaned.split_whitespace() {
            tokens.push(
                if token.len() >= 24 && token.chars().all(|character| character.is_ascii_hexdigit())
                {
                    "<token>".to_string()
                } else {
                    token.to_string()
                },
            );
        }
    }
    tokens.join(" ")
}

fn blocker_is_external(summary: &str) -> bool {
    let normalized = normalize_blocker_text(summary);
    [
        "authorization",
        "authentication",
        "permission",
        "credential",
        "api key",
        "access token",
        "oauth",
        "sign in",
        "login",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn promote_final_review_blocker_child(
    store: &StateStore,
    objective_id: &str,
    root_session_id: &str,
    graph: &mut ObjectiveGraph,
    objective_lease: &crate::state::ObjectiveLeaseGuard,
    policy: &ObjectivePolicy,
    budget: &Budget,
    outcome: &RunOutcome,
) -> Result<FinalReviewBlockerDecision> {
    if outcome.status != GoalStatus::Blocked {
        return Ok(FinalReviewBlockerDecision::NotPromoted);
    }
    let final_wave: FinalVerificationWaveReceipt = serde_json::from_str(
        &std_fs::read_to_string(&outcome.final_verification_wave_path).with_context(|| {
            format!(
                "failed to read final verification wave {}",
                outcome.final_verification_wave_path.display()
            )
        })?,
    )
    .context("failed to parse final verification wave for blocker promotion")?;
    if final_wave.receipt_hash != outcome.final_verification_wave_hash || final_wave.passed {
        return Ok(FinalReviewBlockerDecision::NotPromoted);
    }
    let failed_dimensions = final_wave
        .dimensions
        .iter()
        .filter(|dimension| !dimension.passed)
        .collect::<Vec<_>>();
    if failed_dimensions.is_empty()
        || failed_dimensions
            .iter()
            .any(|dimension| dimension.evidence_paths.is_empty())
    {
        return Ok(FinalReviewBlockerDecision::NotPromoted);
    }
    let reviewer_ids = failed_dimensions
        .iter()
        .flat_map(|dimension| dimension.reviewer_execution_ids.iter().cloned())
        .collect::<HashSet<_>>();
    if reviewer_ids.len() < 2 || reviewer_ids.iter().all(|id| id.starts_with("runtime-")) {
        return Ok(FinalReviewBlockerDecision::NotPromoted);
    }
    let parent_goal_id = outcome.goal_id.as_str();
    if !graph
        .nodes
        .iter()
        .any(|node| node.goal_id == parent_goal_id)
    {
        bail!("final blocker outcome references an unknown goal");
    }
    if graph.active_goal_id.as_deref() != Some(parent_goal_id)
        || graph.nodes.len() >= policy.max_epochs
        || objective_node_depth(graph, parent_goal_id) >= budget.max_child_depth
    {
        return Ok(FinalReviewBlockerDecision::NotPromoted);
    }
    let normalized_blocker = failed_dimensions
        .iter()
        .map(|dimension| {
            format!(
                "{:?}:{}",
                dimension.dimension,
                normalize_blocker_text(&dimension.summary)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let blocker_signature = hash_text(&normalized_blocker);
    let external_blocker = failed_dimensions
        .iter()
        .any(|dimension| blocker_is_external(&dimension.summary));
    if external_blocker {
        let occurrences = store
            .read_objective_events(objective_id)?
            .iter()
            .filter(|event| {
                event.kind == ObjectiveEventKind::FinalReviewBlockerPromoted
                    && event
                        .payload
                        .get("blocker_signature")
                        .and_then(Value::as_str)
                        == Some(blocker_signature.as_str())
            })
            .count();
        if occurrences >= MAX_EXTERNAL_BLOCKER_OCCURRENCES {
            return Ok(FinalReviewBlockerDecision::NeedsUser);
        }
    }
    let acceptance_signals = failed_dimensions
        .iter()
        .map(|dimension| {
            format!(
                "Resolve final review blocker for {:?}: {}",
                dimension.dimension, dimension.summary
            )
        })
        .collect::<Vec<_>>();
    let child_request = objective_child_request(
        &format!(
            "Resolve the verified final-review blockers for goal {} before resuming the parent objective.",
            outcome.goal_id
        ),
        &acceptance_signals,
    );
    let child_hash = hash_text(&normalize_objective(&child_request));
    if graph.nodes.iter().any(|node| {
        node.parent_goal_id.as_deref() == Some(parent_goal_id)
            && node.parent_epoch_id.as_deref() == Some(outcome.epoch_id.as_str())
            && node.parent_strategist_receipt_hash.as_deref()
                == Some(outcome.final_verification_wave_hash.as_str())
            && node.objective_hash == child_hash
    }) {
        return Ok(FinalReviewBlockerDecision::Promoted);
    }
    let (calls, tokens, cost, unknown_calls) = objective_budget_totals(store, graph)?;
    if calls >= policy.max_calls
        || tokens >= policy.max_tokens
        || (policy.max_cost_micros != u64::MAX && cost >= policy.max_cost_micros)
        || unknown_calls >= policy.max_unknown_usage_calls
    {
        return Ok(FinalReviewBlockerDecision::NotPromoted);
    }
    let child_index = graph.nodes.len();
    let child_goal_id = format!("goal_{objective_id}_{child_index:03}");
    let child_epoch_id = format!("epoch_{objective_id}_{child_index:03}");
    let child_session_id = format!("{root_session_id}.epoch{child_index}");
    let reserved_calls = budget
        .max_calls_per_epoch
        .min(policy.max_calls.saturating_sub(calls));
    let reserved_tokens = budget
        .max_tokens_per_epoch
        .min(policy.max_tokens.saturating_sub(tokens));
    let reserved_cost_micros = if policy.max_cost_micros == u64::MAX {
        u64::MAX
    } else {
        policy.max_cost_micros.saturating_sub(cost)
    };
    let reserved_unknown_calls = budget
        .max_usage_unknown_calls
        .min(policy.max_unknown_usage_calls.saturating_sub(unknown_calls));
    if reserved_calls == 0 || reserved_tokens == 0 || reserved_unknown_calls == 0 {
        return Ok(FinalReviewBlockerDecision::NotPromoted);
    }
    let reservation_id = format!("epoch:{child_epoch_id}");
    store.reserve_objective_epoch(
        objective_lease,
        &reservation_id,
        &child_goal_id,
        &child_epoch_id,
        policy,
        reserved_calls,
        reserved_tokens,
        reserved_cost_micros,
        reserved_unknown_calls,
        budget.max_premium_worker_calls,
    )?;
    if let Err(error) = store.append_objective_event(
        objective_id,
        &format!("child-dispatch-reserved:{child_epoch_id}"),
        ObjectiveEventKind::ChildDispatchReserved,
        json!({
            "reservation_id": reservation_id,
            "goal_id": child_goal_id,
            "epoch_id": child_epoch_id,
            "trigger": "final_review_blocker",
            "blocker_signature": blocker_signature,
        }),
    ) {
        store.release_objective_epoch(objective_lease, &reservation_id)?;
        return Err(error);
    }
    if let Err(error) = graph.update_active_node(
        parent_goal_id,
        GoalStatus::Verifying,
        Some(outcome.final_verification_wave_hash.clone()),
        Some(outcome.final_report_path.to_string_lossy().to_string()),
        None,
        Some("verified final-review blocker awaiting child epoch".to_string()),
    ) {
        store.release_objective_epoch(objective_lease, &reservation_id)?;
        return Err(error);
    }
    let child = match objective_goal_node(
        &child_goal_id,
        &child_epoch_id,
        &child_session_id,
        &child_request,
        acceptance_signals,
        Some(parent_goal_id.to_string()),
        Some(outcome.epoch_id.clone()),
        Some(outcome.final_verification_wave_hash.clone()),
        GoalStatus::Planning,
        None,
        child_hash,
    ) {
        Ok(child) => child,
        Err(error) => {
            store.release_objective_epoch(objective_lease, &reservation_id)?;
            return Err(error);
        }
    };
    let attached = match graph.append_final_review_blocker_child(
        parent_goal_id,
        &outcome.epoch_id,
        &outcome.final_verification_wave_hash,
        child,
    ) {
        Ok(attached) => attached,
        Err(error) => {
            store.release_objective_epoch(objective_lease, &reservation_id)?;
            return Err(error);
        }
    };
    if !attached {
        store.release_objective_epoch(objective_lease, &reservation_id)?;
        return Ok(FinalReviewBlockerDecision::Promoted);
    }
    store.write_objective_graph(graph)?;
    store.append_objective_event(
        objective_id,
        &format!("final-review-blocker:{blocker_signature}"),
        ObjectiveEventKind::FinalReviewBlockerPromoted,
        json!({
            "parent_goal_id": parent_goal_id,
            "parent_epoch_id": outcome.epoch_id,
            "child_goal_id": child_goal_id,
            "child_epoch_id": child_epoch_id,
            "review_receipt_hash": outcome.final_verification_wave_hash,
            "blocker_signature": blocker_signature,
            "reviewer_execution_ids": reviewer_ids,
        }),
    )?;
    store.append_objective_event(
        objective_id,
        &format!("goal-attached:{child_goal_id}"),
        ObjectiveEventKind::GoalAttached,
        json!({
            "goal_id": child_goal_id,
            "epoch_id": child_epoch_id,
            "session_id": child_session_id,
            "parent_goal_id": parent_goal_id,
            "parent_epoch_id": outcome.epoch_id,
            "parent_strategist_receipt_hash": outcome.final_verification_wave_hash,
        }),
    )?;
    store.append_objective_event(
        objective_id,
        &format!("frontier-advanced:{child_goal_id}"),
        ObjectiveEventKind::FrontierAdvanced,
        json!({ "active_goal_id": child_goal_id }),
    )?;
    Ok(FinalReviewBlockerDecision::Promoted)
}

fn objective_node_depth(graph: &ObjectiveGraph, goal_id: &str) -> usize {
    let mut depth: usize = 0;
    let mut current = goal_id.to_string();
    let mut seen = HashSet::new();
    while seen.insert(current.clone()) {
        let Some(parent) = graph
            .nodes
            .iter()
            .find(|node| node.goal_id == current)
            .and_then(|node| node.parent_goal_id.clone())
        else {
            break;
        };
        depth = depth.saturating_add(1);
        current = parent;
    }
    depth
}

fn sha256_file(path: &std::path::Path) -> Result<String> {
    let bytes = std_fs::read(path)
        .with_context(|| format!("failed to read hash-bound artifact {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn recover_settled_epoch_outcome_receipt(
    store: &StateStore,
    objective_id: &str,
    graph: &ObjectiveGraph,
    node: &GoalGraphNode,
) -> Result<Option<ObjectiveEpochOutcomeReceipt>> {
    let settled_event = store
        .read_goal_epoch_events(&node.goal_id)?
        .into_iter()
        .rev()
        .find(|event| event.epoch_id == node.epoch_id && event.kind == GoalEpochEventKind::Settled);
    let Some(settled_event) = settled_event else {
        return Ok(None);
    };
    let status: GoalStatus = settled_event
        .payload
        .get("status")
        .cloned()
        .context("settled goal epoch is missing status")
        .and_then(|value| {
            serde_json::from_value(value).context("settled goal status is invalid")
        })?;
    let final_report_path = settled_event
        .payload
        .get("final_report_path")
        .and_then(Value::as_str)
        .filter(|path| !path.trim().is_empty())
        .map(PathBuf::from)
        .context("settled goal epoch is missing final report path")?;
    let final_wave_path = store
        .artifact_dir(&node.goal_id)
        .join("final-verification-wave.json");
    if !final_wave_path.is_file() || !final_report_path.is_file() {
        return Ok(None);
    }
    let final_wave: FinalVerificationWaveReceipt =
        serde_json::from_str(&std_fs::read_to_string(&final_wave_path).with_context(|| {
            format!(
                "failed to read recovered final wave {}",
                final_wave_path.display()
            )
        })?)
        .context("failed to parse recovered final wave")?;
    let strategist_path = store
        .artifact_dir(&node.goal_id)
        .join("strategist-next-goal-receipt.json");
    let strategist = if strategist_path.is_file() {
        Some(
            serde_json::from_str::<StrategistNextGoalReceipt>(
                &std_fs::read_to_string(&strategist_path).with_context(|| {
                    format!(
                        "failed to read recovered strategist receipt {}",
                        strategist_path.display()
                    )
                })?,
            )
            .context("failed to parse recovered strategist receipt")?,
        )
    } else {
        None
    };
    let budget_ledger = store.read_goal_budget_ledger(&node.goal_id)?;
    Ok(Some(ObjectiveEpochOutcomeReceipt::seal(
        objective_id,
        &node.goal_id,
        &node.epoch_id,
        &node.session_id,
        hash_text(&node.request),
        graph.scope_hash.clone(),
        graph.policy_hash.clone(),
        status,
        final_wave_path.to_string_lossy().to_string(),
        final_wave.receipt_hash,
        final_report_path.to_string_lossy().to_string(),
        sha256_file(&final_report_path)?,
        budget_ledger.ledger_hash,
        strategist
            .as_ref()
            .map(|_| strategist_path.to_string_lossy().to_string()),
        strategist
            .as_ref()
            .map(|receipt| receipt.receipt_hash.clone()),
        strategist
            .as_ref()
            .map(|receipt| format!("{:?}", receipt.verdict.decision)),
    )?))
}

fn run_outcome_from_objective_receipt(
    store: &StateStore,
    receipt: ObjectiveEpochOutcomeReceipt,
) -> Result<RunOutcome> {
    receipt.validate(&receipt.objective_id, &receipt.goal_id, &receipt.epoch_id)?;
    let final_report_path = PathBuf::from(&receipt.final_report_path);
    if sha256_file(&final_report_path)? != receipt.final_report_hash {
        bail!("objective epoch outcome final report hash does not match the artifact");
    }
    let final_wave_path = PathBuf::from(&receipt.final_wave_path);
    let final_wave: FinalVerificationWaveReceipt =
        serde_json::from_str(&std_fs::read_to_string(&final_wave_path).with_context(|| {
            format!(
                "failed to read objective epoch final wave {}",
                final_wave_path.display()
            )
        })?)
        .context("failed to parse objective epoch final wave")?;
    if final_wave.receipt_hash != receipt.final_wave_hash {
        bail!("objective epoch outcome final wave hash does not match the artifact");
    }
    let budget_ledger = store.read_goal_budget_ledger(&receipt.goal_id)?;
    if budget_ledger.ledger_hash != receipt.goal_budget_ledger_hash {
        bail!("objective epoch outcome budget ledger hash does not match the artifact");
    }
    let strategist_receipt = match (
        receipt.strategist_receipt_path.as_deref(),
        receipt.strategist_receipt_hash.as_deref(),
    ) {
        (Some(path), Some(expected_hash)) => {
            let strategist: StrategistNextGoalReceipt = serde_json::from_str(
                &std_fs::read_to_string(path)
                    .with_context(|| format!("failed to read strategist receipt {}", path))?,
            )
            .context("failed to parse strategist receipt from objective outcome")?;
            if strategist.receipt_hash != expected_hash {
                bail!("objective epoch outcome strategist receipt hash does not match");
            }
            Some(strategist)
        }
        (None, None) => None,
        _ => bail!("objective epoch outcome has an incomplete strategist receipt"),
    };
    Ok(RunOutcome {
        goal_id: receipt.goal_id.clone(),
        epoch_id: receipt.epoch_id.clone(),
        session_id: receipt.session_id.clone(),
        status: receipt.status,
        artifacts_root: store.artifact_dir(&receipt.goal_id),
        final_report_path,
        events_path: store.events_path(&receipt.session_id),
        final_verification_wave_path: final_wave_path,
        final_verification_wave_hash: receipt.final_wave_hash,
        strategist_receipt,
    })
}

fn objective_goal_node(
    goal_id: &str,
    epoch_id: &str,
    session_id: &str,
    request: &str,
    acceptance_signals: Vec<String>,
    parent_goal_id: Option<String>,
    parent_epoch_id: Option<String>,
    parent_strategist_receipt_hash: Option<String>,
    status: GoalStatus,
    final_wave_receipt_hash: Option<String>,
    objective_hash: String,
) -> Result<GoalGraphNode> {
    let now = timestamp();
    let node = GoalGraphNode {
        goal_id: goal_id.to_string(),
        epoch_id: epoch_id.to_string(),
        session_id: session_id.to_string(),
        request: request.to_string(),
        acceptance_signals,
        parent_goal_id,
        parent_epoch_id,
        parent_strategist_receipt_hash,
        request_hash: hash_text(request),
        objective_hash: objective_hash.clone(),
        status,
        final_wave_receipt_hash,
        final_report_path: None,
        strategist_receipt_hash: None,
        progress_fingerprint: objective_hash,
        terminal_reason: None,
        created_at: now.clone(),
        updated_at: now,
    };
    node.validate()?;
    Ok(node)
}

fn objective_id_for(
    root_session_id: &str,
    workspace: &std::path::Path,
    request: &str,
) -> Result<String> {
    let seed = format!(
        "{}\n{}\n{}",
        root_session_id,
        workspace.to_string_lossy(),
        normalize_objective(request)
    );
    Ok(format!("objective_{}", &hash_text(&seed)[..20]))
}

fn normalize_objective(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn hash_text(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn hash_serialized<T: Serialize>(value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value).context("failed to serialize objective binding")?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn objective_status_for_goal(status: &GoalStatus) -> ObjectiveStatus {
    match status {
        GoalStatus::NeedsUser => ObjectiveStatus::NeedsUser,
        GoalStatus::Blocked => ObjectiveStatus::Blocked,
        GoalStatus::Limited => ObjectiveStatus::Limited,
        GoalStatus::Failed => ObjectiveStatus::Failed,
        GoalStatus::Complete => ObjectiveStatus::Complete,
        GoalStatus::Draft | GoalStatus::Planning | GoalStatus::Running | GoalStatus::Verifying => {
            ObjectiveStatus::Failed
        }
    }
}

fn settle_failed_objective_goal(
    store: &StateStore,
    event_sink: &Option<EventSink>,
    objective_id: &str,
    graph: &mut ObjectiveGraph,
    active_node: &crate::state::GoalGraphNode,
    error: &anyhow::Error,
) -> Result<()> {
    let reason = format!("active goal failed before producing a terminal outcome: {error:#}");
    if let Some(mut goal) = store.read_goal(&active_node.goal_id)? {
        goal.status = GoalStatus::Failed;
        goal.current_task_id = None;
        goal.summary = reason.clone();
        goal.updated_at = timestamp();
        store.write_goal(&goal)?;
    }
    store.write_continuation_state(
        &active_node.session_id,
        &active_node.goal_id,
        ContinuationStatus::Stopped,
    )?;
    store.append_goal_epoch_event(
        &active_node.goal_id,
        &active_node.epoch_id,
        &format!("{}.aborted", active_node.epoch_id),
        GoalEpochEventKind::Aborted,
        json!({
            "status": GoalStatus::Failed.as_str(),
            "reason": reason,
        }),
    )?;
    append_event(
        store,
        event_sink,
        event(
            &active_node.session_id,
            Some(&active_node.goal_id),
            None,
            EventKind::GoalBlocked,
            reason.clone(),
            json!({ "status": GoalStatus::Failed.as_str() }),
        ),
    )?;
    graph.update_active_node(
        &active_node.goal_id,
        GoalStatus::Failed,
        None,
        None,
        None,
        Some(reason.clone()),
    )?;
    graph.record_failure(graph.consecutive_failures.saturating_add(1))?;
    graph.set_terminal(ObjectiveStatus::Failed, reason.clone())?;
    store.write_objective_graph(graph)?;
    append_objective_terminal_event(
        store,
        objective_id,
        &ObjectiveStatus::Failed,
        &reason,
        &active_node.goal_id,
    )?;
    Ok(())
}

fn append_objective_terminal_event(
    store: &StateStore,
    objective_id: &str,
    status: &ObjectiveStatus,
    reason: &str,
    goal_id: &str,
) -> Result<()> {
    let kind = match status {
        ObjectiveStatus::NeedsUser => ObjectiveEventKind::NeedsUser,
        ObjectiveStatus::Stopped => ObjectiveEventKind::Stopped,
        ObjectiveStatus::Limited => ObjectiveEventKind::Limited,
        ObjectiveStatus::Blocked => ObjectiveEventKind::Blocked,
        ObjectiveStatus::Complete => ObjectiveEventKind::Completed,
        ObjectiveStatus::Failed => ObjectiveEventKind::Failed,
        ObjectiveStatus::Running => bail!("running objective cannot append a terminal event"),
    };
    store.append_objective_event(
        objective_id,
        &format!("terminal:{goal_id}:{}", status_name(status)),
        kind,
        json!({ "goal_id": goal_id, "reason": reason }),
    )?;
    Ok(())
}

fn status_name(status: &ObjectiveStatus) -> &'static str {
    match status {
        ObjectiveStatus::Running => "running",
        ObjectiveStatus::NeedsUser => "needs_user",
        ObjectiveStatus::Stopped => "stopped",
        ObjectiveStatus::Limited => "limited",
        ObjectiveStatus::Blocked => "blocked",
        ObjectiveStatus::Complete => "complete",
        ObjectiveStatus::Failed => "failed",
    }
}

fn objective_budget_totals(
    store: &StateStore,
    graph: &ObjectiveGraph,
) -> Result<(usize, u64, u64, usize)> {
    let objective_ledger_path = store.objective_budget_ledger_path(&graph.objective_id);
    if objective_ledger_path.exists() {
        let objective_ledger =
            store.read_objective_budget_ledger(&graph.objective_id, &graph.policy_hash)?;
        let mut objective_totals = (0usize, 0u64, 0u64, 0usize);
        for reservation in objective_ledger.reservations {
            if reservation.status != crate::state::ObjectiveBudgetReservationStatus::Settled {
                continue;
            }
            objective_totals.0 = objective_totals.0.saturating_add(
                reservation
                    .actual_calls
                    .unwrap_or(reservation.reserved_calls),
            );
            objective_totals.1 = objective_totals.1.saturating_add(
                reservation
                    .actual_tokens
                    .unwrap_or(reservation.reserved_tokens),
            );
            objective_totals.2 = objective_totals
                .2
                .saturating_add(reservation.actual_cost_micros.unwrap_or(0));
            objective_totals.3 = objective_totals.3.saturating_add(
                reservation
                    .actual_unknown_calls
                    .unwrap_or(reservation.reserved_unknown_calls),
            );
        }
        let goal_totals = objective_budget_totals_from_goal_ledgers(store, graph)?;
        if objective_totals.0 != goal_totals.0
            || objective_totals.2 != goal_totals.2
            || objective_totals.3 != goal_totals.3
            || (objective_totals.1 != goal_totals.1 && objective_totals.3 == 0)
        {
            bail!(
                "objective budget ledger disagrees with goal budget ledgers: objective={objective_totals:?}, goal={goal_totals:?}"
            );
        }
        return Ok(objective_totals);
    }
    objective_budget_totals_from_goal_ledgers(store, graph)
}

fn objective_budget_totals_from_goal_ledgers(
    store: &StateStore,
    graph: &ObjectiveGraph,
) -> Result<(usize, u64, u64, usize)> {
    let mut calls = 0usize;
    let mut tokens = 0u64;
    let mut cost = 0u64;
    let mut unknown_calls = 0usize;
    for node in &graph.nodes {
        if graph.active_goal_id.as_deref() == Some(node.goal_id.as_str())
            && !node.status.is_terminal()
        {
            continue;
        }
        let ledger = store.read_goal_budget_ledger(&node.goal_id)?;
        for reservation in ledger.reservations {
            if reservation.status == crate::state::BudgetReservationStatus::Released {
                continue;
            }
            calls = calls.saturating_add(1);
            if let Some(usage) = reservation.usage {
                tokens = tokens
                    .saturating_add(usage.total_tokens().unwrap_or(reservation.reserved_tokens));
                if let Some(actual_cost) = usage.cost_micros {
                    cost = cost.saturating_add(actual_cost);
                }
                unknown_calls = unknown_calls.saturating_add(usize::from(usage.is_unknown()));
            } else {
                tokens = tokens.saturating_add(reservation.reserved_tokens);
                unknown_calls = unknown_calls.saturating_add(1);
            }
        }
    }
    Ok((calls, tokens, cost, unknown_calls))
}

fn ensure_objective_epoch_reservation(
    store: &StateStore,
    lease: &crate::state::ObjectiveLeaseGuard,
    graph: &ObjectiveGraph,
    active_node: &GoalGraphNode,
    policy: &ObjectivePolicy,
    budget: &Budget,
    reservation_id: &str,
) -> Result<()> {
    let (calls, tokens, cost, unknown_calls) = objective_budget_totals(store, graph)?;
    let reserved_calls = budget
        .max_calls_per_epoch
        .min(policy.max_calls.saturating_sub(calls));
    let reserved_tokens = budget
        .max_tokens_per_epoch
        .min(policy.max_tokens.saturating_sub(tokens));
    let reserved_cost_micros = if policy.max_cost_micros == u64::MAX {
        budget.max_cost_micros_per_epoch
    } else {
        budget
            .max_cost_micros_per_epoch
            .min(policy.max_cost_micros.saturating_sub(cost))
    };
    let reserved_unknown_calls = budget
        .max_usage_unknown_calls
        .min(policy.max_unknown_usage_calls.saturating_sub(unknown_calls));
    store.reserve_objective_epoch(
        lease,
        reservation_id,
        &active_node.goal_id,
        &active_node.epoch_id,
        policy,
        reserved_calls,
        reserved_tokens,
        reserved_cost_micros,
        reserved_unknown_calls,
        budget.max_premium_worker_calls,
    )?;
    Ok(())
}

fn objective_goal_budget_usage(
    store: &StateStore,
    goal_id: &str,
) -> Result<(
    usize,
    Option<u64>,
    Option<u64>,
    usize,
    usize,
    usize,
    Option<u64>,
    Vec<String>,
)> {
    let ledger = store.read_goal_budget_ledger(goal_id)?;
    let mut calls = 0usize;
    let mut tokens = 0u64;
    let mut tokens_known = true;
    let mut cost = 0u64;
    let mut cost_known = true;
    let mut unknown_calls = 0usize;
    let mut premium_calls = 0usize;
    let mut cache_hits = 0usize;
    let mut duration_ms = 0u64;
    let mut has_duration = false;
    let mut fallback_reasons = Vec::new();
    for reservation in ledger.reservations {
        if reservation.status != crate::state::BudgetReservationStatus::Settled {
            continue;
        }
        calls = calls.saturating_add(1);
        premium_calls = premium_calls.saturating_add(usize::from(reservation.premium));
        let Some(usage) = reservation.usage else {
            unknown_calls = unknown_calls.saturating_add(1);
            tokens_known = false;
            cost_known = false;
            continue;
        };
        if usage.is_unknown() {
            unknown_calls = unknown_calls.saturating_add(1);
        }
        if let Some(value) = usage.total_tokens() {
            tokens = tokens.saturating_add(value);
        } else {
            tokens_known = false;
        }
        if let Some(value) = usage.cost_micros {
            cost = cost.saturating_add(value);
        } else {
            cost_known = false;
        }
        cache_hits = cache_hits.saturating_add(usize::from(usage.cache_hit == Some(true)));
        if let Some(value) = usage.duration_ms {
            duration_ms = duration_ms.saturating_add(value);
            has_duration = true;
        }
        if let Some(reason) = usage.unavailable_reason {
            if !reason.trim().is_empty() {
                fallback_reasons.push(reason);
            }
        }
    }
    Ok((
        calls,
        tokens_known.then_some(tokens),
        cost_known.then_some(cost),
        unknown_calls,
        premium_calls,
        cache_hits,
        has_duration.then_some(duration_ms),
        fallback_reasons,
    ))
}

fn cooldown_remaining_seconds(updated_at: &str, cooldown_seconds: u64) -> Result<u64> {
    let updated = DateTime::parse_from_rfc3339(updated_at)
        .context("objective graph has invalid updated_at")?;
    let elapsed = Local::now().timestamp().saturating_sub(updated.timestamp());
    Ok(cooldown_seconds.saturating_sub(u64::try_from(elapsed.max(0)).unwrap_or(0)))
}

#[allow(clippy::too_many_arguments)]
fn reserve_planning_phase_budget(
    goal: &mut Goal,
    store: &StateStore,
    budget_context: Option<(&GoalRunLeaseGuard, &str)>,
    phase_key: &str,
) -> Result<Option<String>> {
    let Some((lease, epoch_id)) = budget_context else {
        return Ok(None);
    };
    let reservation_id = format!("{epoch_id}.{phase_key}");
    if let Err(error) = store.reserve_budget_call(
        lease,
        &reservation_id,
        phase_key,
        false,
        false,
        &goal.budget,
    ) {
        goal.status = GoalStatus::Limited;
        goal.summary = format!("Planning phase budget reservation failed: {error}");
        goal.updated_at = timestamp();
        store.write_goal(goal)?;
        store.append_goal_epoch_event(
            &goal.id,
            epoch_id,
            &format!("{reservation_id}.aborted"),
            GoalEpochEventKind::Aborted,
            json!({
                "phase": phase_key,
                "reason": error.to_string(),
            }),
        )?;
        bail!("{}", goal.summary);
    }
    store.append_goal_epoch_event(
        &goal.id,
        epoch_id,
        &format!("{reservation_id}.reserved"),
        GoalEpochEventKind::BudgetReserved,
        json!({
            "reservation_id": reservation_id,
            "phase": phase_key,
            "worker_call": false,
            "premium": false,
            "reserved_tokens": goal.budget.max_tokens_per_call,
        }),
    )?;
    Ok(Some(reservation_id))
}

fn settle_planning_phase_budget(
    goal: &Goal,
    store: &StateStore,
    budget_context: Option<(&GoalRunLeaseGuard, &str)>,
    reservation_id: Option<&str>,
    phase_key: &str,
) -> Result<()> {
    let (Some((lease, epoch_id)), Some(reservation_id)) = (budget_context, reservation_id) else {
        return Ok(());
    };
    let settlement = store.settle_budget_call(
        lease,
        reservation_id,
        SettledBudgetUsage {
            requested_tokens: None,
            actual_tokens: None,
            cost_micros: None,
            duration_ms: None,
            cache_hit: None,
            unavailable_reason: Some(format!("{phase_key} hook does not expose provider usage")),
        },
    )?;
    store.append_goal_epoch_event(
        &goal.id,
        epoch_id,
        &format!("{reservation_id}.settled"),
        GoalEpochEventKind::BudgetSettled,
        json!({
            "reservation_id": reservation_id,
            "phase": phase_key,
            "usage": settlement.usage,
        }),
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_strategist_next_goal(
    goal: &mut Goal,
    epoch_id: &str,
    plan: &PlanGraph,
    final_report_path: &std::path::Path,
    store: &StateStore,
    session_id: &str,
    event_sink: &Option<EventSink>,
    phase_runtime: &PhaseRuntime,
    lease: &GoalRunLeaseGuard,
    prior_execution_ids: &[String],
) -> Result<Option<StrategistNextGoalReceipt>> {
    let Some(hook) = phase_runtime.strategist_next_goal_hook.as_ref() else {
        return Ok(None);
    };
    let route_decision = phase_runtime.routes.resolve(
        &PhaseProfile::StrategistNextGoal,
        &phase_runtime.inventory,
        phase_runtime.current_model.as_ref(),
    )?;
    check_phase_terminal_state(&route_decision)
        .context("strategist next-goal phase terminal state check failed")?;
    let phase_key = "strategist-next-goal";
    let budget_reservation =
        reserve_planning_phase_budget(goal, store, Some((lease, epoch_id)), phase_key)?;
    let submission_result = hook(StrategistNextGoalInput {
        goal_id: goal.id.clone(),
        epoch_id: epoch_id.to_string(),
        request: goal.request.clone(),
        status: goal.status.clone(),
        summary: goal.summary.clone(),
        plan: plan.clone(),
        final_report_path: final_report_path.to_string_lossy().to_string(),
        budget_ledger: store.read_goal_budget_ledger(&goal.id)?,
        route_decision: route_decision.clone(),
    })
    .context("strategist next-goal hook failed");
    settle_planning_phase_budget(
        goal,
        store,
        Some((lease, epoch_id)),
        budget_reservation.as_deref(),
        phase_key,
    )?;
    let submission = submission_result?;
    let parsed = StrategistNextGoalVerdict::parse(&submission.raw_output)?;
    if parsed != submission.verdict {
        bail!("strategist raw output does not match its typed verdict");
    }
    submission
        .verdict
        .validate(&goal.id, epoch_id, &goal.status)?;
    validate_phase_execution_identity(&route_decision, &submission.strategist)?;
    if prior_execution_ids.iter().any(|prior| {
        prior == &submission.strategist.execution_id
            || prior == &submission.strategist.phase_session_id
            || submission.strategist.actual_session_id.as_ref() == Some(prior)
    }) {
        bail!("strategist must use a fresh execution identity and session");
    }
    let receipt = StrategistNextGoalReceipt::seal(submission)?;
    let receipt_json = serde_json::to_string_pretty(&receipt)?;
    let receipt_path = store.write_artifact(
        &goal.id,
        "strategist-next-goal-receipt.json",
        &format!("{receipt_json}\n"),
    )?;
    store.append_goal_epoch_event(
        &goal.id,
        epoch_id,
        &format!("{epoch_id}.strategist-next-goal.selected"),
        GoalEpochEventKind::NextGoalSelected,
        json!({
            "decision": receipt.verdict.decision,
            "next_objective": receipt.verdict.next_objective,
            "answerable_now": receipt.verdict.answerable_now,
            "evidence_refs": receipt.verdict.evidence_refs,
            "receipt_hash": receipt.receipt_hash,
            "receipt_path": receipt_path.to_string_lossy(),
        }),
    )?;
    append_event(
        store,
        event_sink,
        event(
            session_id,
            Some(&goal.id),
            None,
            EventKind::NextGoalSelected,
            "Strategist next-goal decision recorded",
            json!({
                "decision": receipt.verdict.decision,
                "answerable_now": receipt.verdict.answerable_now,
                "evidence_refs": receipt.verdict.evidence_refs,
                "receipt_path": receipt_path.to_string_lossy(),
            }),
        ),
    )?;
    Ok(Some(receipt))
}

fn build_approved_plan_graph(
    goal: &mut Goal,
    scope: &Scope,
    verification_commands: &[String],
    workspace: &std::path::Path,
    store: &StateStore,
    session_id: &str,
    event_sink: &Option<EventSink>,
    cancellation_token: Option<&CancellationToken>,
    phase_runtime: &PhaseRuntime,
) -> Result<PlanGraph> {
    build_approved_plan_graph_inner(
        goal,
        scope,
        verification_commands,
        workspace,
        store,
        session_id,
        event_sink,
        cancellation_token,
        phase_runtime,
        None,
    )
}

fn build_approved_plan_graph_with_budget(
    goal: &mut Goal,
    scope: &Scope,
    verification_commands: &[String],
    workspace: &std::path::Path,
    store: &StateStore,
    session_id: &str,
    event_sink: &Option<EventSink>,
    cancellation_token: Option<&CancellationToken>,
    phase_runtime: &PhaseRuntime,
    lease: &GoalRunLeaseGuard,
    epoch_id: &str,
) -> Result<PlanGraph> {
    build_approved_plan_graph_inner(
        goal,
        scope,
        verification_commands,
        workspace,
        store,
        session_id,
        event_sink,
        cancellation_token,
        phase_runtime,
        Some((lease, epoch_id)),
    )
}

fn build_approved_plan_graph_inner(
    goal: &mut Goal,
    scope: &Scope,
    verification_commands: &[String],
    workspace: &std::path::Path,
    store: &StateStore,
    session_id: &str,
    event_sink: &Option<EventSink>,
    cancellation_token: Option<&CancellationToken>,
    phase_runtime: &PhaseRuntime,
    budget_context: Option<(&GoalRunLeaseGuard, &str)>,
) -> Result<PlanGraph> {
    if !phase_runtime.require_plan_approval
        && phase_runtime.planner_hook.is_none()
        && phase_runtime.intent_fold_hook.is_none()
    {
        return build_plan_graph(goal, scope, verification_commands);
    }
    let mut intent_fold_artifact_path: Option<String> = None;
    let mut intent_fold_observation_path: Option<String> = None;
    let mut repository_discovery: Option<RepositoryDiscoverySubmission> = None;
    let intent_fold_receipt =
        if let Some(intent_fold_hook) = phase_runtime.intent_fold_hook.as_ref() {
            check_run_cancelled(cancellation_token)?;
            let decision = phase_runtime.routes.resolve(
                &PhaseProfile::Planner,
                &phase_runtime.inventory,
                phase_runtime.current_model.as_ref(),
            )?;
            check_phase_terminal_state(&decision)
                .context("intent fold phase terminal state check failed")?;
            let budget_reservation =
                reserve_planning_phase_budget(goal, store, budget_context, "intent-fold")?;
            let submission_result = intent_fold_hook(IntentFoldInput {
                goal_id: goal.id.clone(),
                request: goal.request.clone(),
                scope: scope.clone(),
                route_decision: decision.clone(),
            })
            .context("IntentFold hook failed before planning");
            settle_planning_phase_budget(
                goal,
                store,
                budget_context,
                budget_reservation.as_deref(),
                "intent-fold",
            )?;
            let submission = submission_result?;
            repository_discovery = submission.repository_discovery.clone();
            if phase_runtime.broker_factory.is_some() {
                let discovery = repository_discovery
                    .as_ref()
                    .context("broker-backed IntentFold completed without repository discovery")?;
                require_repository_discovery_artifact(store, discovery, &goal.id)?;
            }
            intent_fold_observation_path = submission.repository_evidence_path.clone();
            if phase_runtime.broker_factory.is_some() {
                require_verified_repository_observation_for_goal(
                    store,
                    submission.repository_evidence_path.as_deref(),
                    &goal.id,
                    "planner",
                )?;
            }
            let parsed = IntentFoldVerdict::parse(&submission.raw_output)?;
            if parsed != submission.verdict {
                bail!("IntentFold raw output does not match its typed verdict");
            }
            submission.verdict.validate(&goal.id)?;
            validate_phase_execution_identity(&decision, &submission.analyst)?;
            let receipt = IntentFoldReceipt::seal(
                submission.verdict,
                submission.analyst,
                &submission.raw_output,
                submission.artifact_path,
                timestamp(),
            )?;
            let receipt_json = serde_json::to_string_pretty(&receipt)?;
            let intent_fold_artifact = store.write_artifact(
                &goal.id,
                "intent-fold-receipt.json",
                &format!("{receipt_json}\n"),
            )?;
            intent_fold_artifact_path = Some(intent_fold_artifact.to_string_lossy().to_string());
            if receipt.verdict.decision == IntentFoldDecision::NeedsUser {
                goal.status = GoalStatus::NeedsUser;
                goal.summary = receipt.verdict.summary.clone();
                goal.updated_at = timestamp();
                store.write_goal(goal)?;
                bail!(
                    "IntentFold requires user input: {}",
                    receipt.verdict.required_questions.join("; ")
                );
            }
            Some(receipt)
        } else {
            None
        };
    let (
        mut plan,
        mut planner_raw_output,
        mut planner_identity,
        mut planner_artifact_path,
        mut planner_observation_path,
    ) = if let Some(planner_hook) = phase_runtime.planner_hook.as_ref() {
        let planner_decision = phase_runtime.routes.resolve(
            &PhaseProfile::Planner,
            &phase_runtime.inventory,
            phase_runtime.current_model.as_ref(),
        )?;
        check_phase_terminal_state(&planner_decision)
            .context("planner phase terminal state check failed")?;
        let budget_reservation =
            reserve_planning_phase_budget(goal, store, budget_context, "planner")?;
        let submission_result = planner_hook(PlannerInput {
            goal_id: goal.id.clone(),
            request: goal.request.clone(),
            scope: scope.clone(),
            verification_commands: verification_commands.to_vec(),
            route_decision: planner_decision.clone(),
            intent_fold: intent_fold_receipt.clone(),
            repository_discovery: repository_discovery.clone(),
        })
        .context("planner hook failed before plan construction");
        settle_planning_phase_budget(
            goal,
            store,
            budget_context,
            budget_reservation.as_deref(),
            "planner",
        )?;
        let submission = submission_result?;
        if phase_runtime.broker_factory.is_some() {
            require_verified_repository_observation_for_goal(
                store,
                submission.repository_evidence_path.as_deref(),
                &goal.id,
                "planner",
            )?;
        }
        let parsed = parse_planner_draft(&submission.raw_output)
            .context("planner hook raw output is not a PlanGraphDraft")?;
        if parsed != submission.draft {
            bail!("planner hook raw output does not match its typed draft");
        }
        validate_phase_execution_identity(&planner_decision, &submission.planner)?;
        if let Some(intent_fold) = intent_fold_receipt.as_ref()
            && !submission.planner.is_independent_from(&intent_fold.analyst)
        {
            bail!("planner must use a fresh session after IntentFold");
        }
        let provider_id = submission
            .planner
            .provider_id
            .clone()
            .context("planner hook is missing provider identity")?;
        let model_id = submission
            .planner
            .model_id
            .clone()
            .context("planner hook is missing model identity")?;
        let plan = PlanGraph::seal(
            &goal.id,
            1,
            PlanSource::PlannerModel,
            Some(PlannerReceipt {
                provider_id,
                model_id,
                session_id: submission.planner.actual_session_id.clone(),
            }),
            submission.draft,
        )?;
        goal.coordinator_brief = Some(submission.raw_output.clone());
        (
            plan,
            submission.raw_output,
            submission.planner,
            submission.artifact_path,
            submission.repository_evidence_path,
        )
    } else {
        let plan = build_plan_graph(goal, scope, verification_commands)?;
        let planner_raw_output = planner_raw_output(goal, &plan)?;
        let planner_identity = planner_identity_for_plan(&plan, phase_runtime.planner.clone())?;
        (plan, planner_raw_output, planner_identity, None, None)
    };
    if !phase_runtime.require_plan_approval {
        return Ok(plan);
    }

    let critic_hook = phase_runtime
        .plan_critic_hook
        .as_ref()
        .context("plan approval is required but no PlanCritic hook is configured")?;
    let mut seen_phase_identities = vec![planner_identity.clone()];
    let mut revisions_performed = 0usize;

    loop {
        check_run_cancelled(cancellation_token)?;
        let planner_decision = phase_runtime.routes.resolve(
            &PhaseProfile::Planner,
            &phase_runtime.inventory,
            phase_runtime.current_model.as_ref(),
        )?;
        check_phase_terminal_state(&planner_decision)
            .context("planner phase terminal state check failed")?;
        validate_phase_execution_identity(&planner_decision, &planner_identity)?;
        let planner_ordinal = plan.revision.saturating_mul(10).saturating_add(1);
        let planner_decision_path =
            store.write_phase_route_decision(&goal.id, planner_ordinal, &planner_decision)?;
        append_event(
            store,
            event_sink,
            event(
                session_id,
                Some(&goal.id),
                None,
                EventKind::PhaseRouteSelected,
                "Planner phase route selected",
                json!({
                    "phase": PhaseProfile::Planner,
                    "decision_path": planner_decision_path.to_string_lossy(),
                    "selected_candidate": planner_decision.selected_candidate,
                    "fallback_count": planner_decision.rejected_candidates.len(),
                }),
            ),
        )?;
        let candidate_path = store.write_plan_candidate(&plan)?;
        let planner_raw_path = store.write_plan_review_text(
            &goal.id,
            plan.revision,
            "planner-output",
            &planner_raw_output,
        )?;
        let planner_receipt = PlannerExecutionReceipt::seal(
            &plan,
            planner_identity.clone(),
            &planner_raw_output,
            Some(planner_raw_path.to_string_lossy().to_string()),
            timestamp(),
        )?;
        let planner_receipt_path = store.write_planner_execution_receipt(&planner_receipt)?;
        let planner_worker_task_id =
            worker_task_id_from_artifact_path(planner_artifact_path.as_deref());
        let planner_worker_evidence_path = phase_worker_evidence_path(
            store,
            &goal.id,
            planner_worker_task_id.as_deref(),
            planner_artifact_path.as_deref(),
        )?;
        let planner_route_receipt = phase_route_receipt_for_identity(
            &planner_decision,
            planner_ordinal,
            &goal.id,
            &plan,
            &planner_identity,
            planner_worker_task_id.as_deref(),
            planner_worker_evidence_path.as_deref(),
        )?;
        let planner_route_receipt_path =
            store.write_phase_route_receipt(&goal.id, planner_ordinal, &planner_route_receipt)?;

        let verifier = PlanVerifierReport::verify(&plan, workspace)?;
        let verifier_path = store.write_plan_verifier_report(&verifier)?;
        let mut approval_state = PlanApprovalState {
            schema_version: crate::plan_review::PLAN_REVIEW_SCHEMA_VERSION,
            goal_id: goal.id.clone(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            plan_hash: plan.plan_hash.clone(),
            status: PlanApprovalStatus::Reviewing,
            planner_receipt_hash: planner_receipt.receipt_hash.clone(),
            verifier_report_hash: verifier.report_hash.clone(),
            critic_receipt_hash: None,
            secondary_critic_receipt_hash: None,
            revisions_used: revisions_performed,
            updated_at: timestamp(),
        };
        let approval_state_path = store.write_plan_approval_state(&approval_state)?;
        append_event(
            store,
            event_sink,
            event(
                session_id,
                Some(&goal.id),
                None,
                EventKind::PlanReviewStarted,
                format!("Plan revision {} entered independent review", plan.revision),
                json!({
                    "plan_id": plan.plan_id,
                    "plan_hash": plan.plan_hash,
                    "revision": plan.revision,
                    "candidate_path": candidate_path.to_string_lossy(),
                    "planner_receipt_path": planner_receipt_path.to_string_lossy(),
                    "verifier_path": verifier_path.to_string_lossy(),
                    "planner_route_decision_path": planner_decision_path.to_string_lossy(),
                    "planner_route_receipt_path": planner_route_receipt_path.to_string_lossy(),
                    "verifier_passed": verifier.passed(),
                    "approval_state_path": approval_state_path.to_string_lossy(),
                }),
            ),
        )?;

        let critic_decision = phase_runtime.routes.resolve(
            &PhaseProfile::PlanCritic,
            &phase_runtime.inventory,
            phase_runtime.current_model.as_ref(),
        )?;
        let critic_ordinal = plan.revision.saturating_mul(10).saturating_add(2);
        let critic_decision_path =
            store.write_phase_route_decision(&goal.id, critic_ordinal, &critic_decision)?;
        append_event(
            store,
            event_sink,
            event(
                session_id,
                Some(&goal.id),
                None,
                EventKind::PhaseRouteSelected,
                "PlanCritic phase route selected",
                json!({
                    "phase": PhaseProfile::PlanCritic,
                    "decision_path": critic_decision_path.to_string_lossy(),
                    "selected_candidate": critic_decision.selected_candidate,
                    "fallback_count": critic_decision.rejected_candidates.len(),
                }),
            ),
        )?;
        check_run_cancelled(cancellation_token)?;
        check_phase_terminal_state(&critic_decision)
            .context("plan critic phase terminal state check failed")?;
        let broker = phase_runtime.broker.as_deref();
        let broker_factory = phase_runtime.broker_factory.as_deref();
        let critic_identity = PhaseExecutionIdentity {
            execution_id: format!("plan_critic:{}:{}", goal.id, plan.plan_id),
            phase_session_id: format!("plan_critic:{}:{}", goal.id, plan.revision),
            backend: PhaseExecutionBackend::DeterministicRules,
            agent_id: None,
            provider_id: None,
            model_id: None,
            actual_session_id: None,
        };
        let critic_budget_key = format!("plan-critic.{}", plan.revision);
        let budget_reservation =
            reserve_planning_phase_budget(goal, store, budget_context, &critic_budget_key)?;
        let submission_result = run_phase_via_broker(
            broker,
            store,
            phase_runtime.direct_model_usage_provider.as_ref(),
            broker_factory,
            &critic_decision,
            &goal.id,
            &plan.plan_id,
            plan.revision,
            "plan_critic",
            &critic_identity,
            || {
                critic_hook(PlanCriticInput {
                    request: goal.request.clone(),
                    plan: plan.clone(),
                    planner_receipt: planner_receipt.clone(),
                    verifier_report: verifier.clone(),
                    route_decision: critic_decision.clone(),
                })
                .context("PlanCritic hook failed before plan approval")
            },
        );
        settle_planning_phase_budget(
            goal,
            store,
            budget_context,
            budget_reservation.as_deref(),
            &critic_budget_key,
        )?;
        let submission = submission_result?;
        check_run_cancelled(cancellation_token)?;
        validate_phase_execution_identity(&critic_decision, &submission.reviewer)?;
        if broker_factory.is_some() {
            require_verified_repository_observation(
                store,
                &plan,
                "plan_critic",
                submission.repository_evidence_path.as_deref(),
            )?;
        }
        if seen_phase_identities
            .iter()
            .any(|seen| !submission.reviewer.is_independent_from(seen))
        {
            bail!("each plan revision requires a fresh PlanCritic execution identity");
        }
        let critic_raw_path = store.write_plan_review_text(
            &goal.id,
            plan.revision,
            "critic-output",
            &submission.raw_output,
        )?;
        let critic_artifact_path = submission.artifact_path.clone();
        let critic_worker_task_id =
            worker_task_id_from_artifact_path(critic_artifact_path.as_deref());
        let critic_worker_evidence_path = phase_worker_evidence_path(
            store,
            &goal.id,
            critic_worker_task_id.as_deref(),
            critic_artifact_path.as_deref(),
        )?;
        let critic_receipt = PlanCriticReceipt::seal(
            &plan,
            &planner_receipt,
            &planner_raw_output,
            &verifier,
            submission.reviewer.clone(),
            submission.verdict,
            &submission.raw_output,
            submission
                .artifact_path
                .or_else(|| Some(critic_raw_path.to_string_lossy().to_string())),
            timestamp(),
        )?;
        let critic_receipt_path = store.write_plan_critic_receipt(&critic_receipt)?;
        let critic_route_receipt = phase_route_receipt_for_identity(
            &critic_decision,
            critic_ordinal,
            &goal.id,
            &plan,
            &submission.reviewer,
            critic_worker_task_id.as_deref(),
            critic_worker_evidence_path.as_deref(),
        )?;
        let critic_route_receipt_path =
            store.write_phase_route_receipt(&goal.id, critic_ordinal, &critic_route_receipt)?;
        seen_phase_identities.push(submission.reviewer.clone());
        approval_state.critic_receipt_hash = Some(critic_receipt.receipt_hash.clone());

        match critic_receipt.verdict.decision {
            PlanCriticDecision::Approve => {
                // OMO's Momus/Oracle gate is role-based, not task-count-based:
                // production ACP runtimes must obtain a fresh Oracle receipt
                // even for a one-node plan. Legacy test runtimes without an
                // Oracle route retain the historical multi-node fallback so
                // their deterministic fixtures stay compatible.
                if plan.draft.tasks.len() > 1
                    || phase_runtime.oracle_hook.is_some()
                    || phase_runtime.broker_factory.is_some()
                {
                    let oracle_decision = phase_runtime.routes.resolve(
                        &PhaseProfile::PlanCritic,
                        &phase_runtime.inventory,
                        phase_runtime.current_model.as_ref(),
                    )?;
                    check_phase_terminal_state(&oracle_decision)
                        .context("independent plan review phase terminal state check failed")?;
                    let oracle_ordinal = plan.revision.saturating_mul(10).saturating_add(3);
                    let oracle_decision_path = store.write_phase_route_decision(
                        &goal.id,
                        oracle_ordinal,
                        &oracle_decision,
                    )?;
                    let oracle_identity = PhaseExecutionIdentity {
                        execution_id: format!("plan_oracle:{}:{}", goal.id, plan.plan_id),
                        phase_session_id: format!("plan_oracle:{}:{}", goal.id, plan.revision),
                        backend: PhaseExecutionBackend::DeterministicRules,
                        agent_id: None,
                        provider_id: None,
                        model_id: None,
                        actual_session_id: None,
                    };
                    let oracle_budget_key = format!("plan-oracle.{}", plan.revision);
                    let oracle_budget = reserve_planning_phase_budget(
                        goal,
                        store,
                        budget_context,
                        &oracle_budget_key,
                    )?;
                    let oracle_hook = match phase_runtime.oracle_hook.as_ref() {
                        Some(oracle_hook) => oracle_hook,
                        None if phase_runtime.broker_factory.is_some() => {
                            bail!("production plan approval requires an independent Oracle hook");
                        }
                        None => critic_hook,
                    };
                    let oracle_submission_result = run_phase_via_broker(
                        phase_runtime.broker.as_deref(),
                        store,
                        phase_runtime.direct_model_usage_provider.as_ref(),
                        phase_runtime.broker_factory.as_deref(),
                        &oracle_decision,
                        &goal.id,
                        &plan.plan_id,
                        plan.revision,
                        "plan_oracle",
                        &oracle_identity,
                        || {
                            oracle_hook(PlanCriticInput {
                                request: goal.request.clone(),
                                plan: plan.clone(),
                                planner_receipt: planner_receipt.clone(),
                                verifier_report: verifier.clone(),
                                route_decision: oracle_decision.clone(),
                            })
                            .context("independent plan review hook failed")
                        },
                    );
                    settle_planning_phase_budget(
                        goal,
                        store,
                        budget_context,
                        oracle_budget.as_deref(),
                        &oracle_budget_key,
                    )?;
                    let oracle_submission = oracle_submission_result?;
                    validate_phase_execution_identity(
                        &oracle_decision,
                        &oracle_submission.reviewer,
                    )?;
                    if phase_runtime.broker_factory.is_some() {
                        require_verified_repository_observation(
                            store,
                            &plan,
                            "plan_critic",
                            oracle_submission.repository_evidence_path.as_deref(),
                        )?;
                    }
                    if seen_phase_identities
                        .iter()
                        .any(|seen| !oracle_submission.reviewer.is_independent_from(seen))
                    {
                        bail!(
                            "independent plan review must use a fresh execution identity and session"
                        );
                    }
                    let oracle_raw_path = store.write_plan_review_text(
                        &goal.id,
                        plan.revision,
                        "oracle-output",
                        &oracle_submission.raw_output,
                    )?;
                    let oracle_artifact_path = oracle_submission.artifact_path.clone();
                    let oracle_worker_task_id =
                        worker_task_id_from_artifact_path(oracle_artifact_path.as_deref());
                    let oracle_worker_evidence_path = phase_worker_evidence_path(
                        store,
                        &goal.id,
                        oracle_worker_task_id.as_deref(),
                        oracle_artifact_path.as_deref(),
                    )?;
                    let oracle_receipt = PlanCriticReceipt::seal(
                        &plan,
                        &planner_receipt,
                        &planner_raw_output,
                        &verifier,
                        oracle_submission.reviewer.clone(),
                        oracle_submission.verdict,
                        &oracle_submission.raw_output,
                        oracle_artifact_path
                            .clone()
                            .or_else(|| Some(oracle_raw_path.to_string_lossy().to_string())),
                        timestamp(),
                    )?;
                    let oracle_receipt_path = store.write_plan_review_text(
                        &goal.id,
                        plan.revision,
                        "oracle-receipt",
                        &format!("{}\n", serde_json::to_string_pretty(&oracle_receipt)?),
                    )?;
                    if oracle_receipt.verdict.decision != PlanCriticDecision::Approve {
                        goal.status = GoalStatus::NeedsUser;
                        goal.summary = format!(
                            "Independent plan review did not approve revision {}: {}",
                            plan.revision, oracle_receipt.verdict.summary
                        );
                        goal.updated_at = timestamp();
                        store.write_goal(goal)?;
                        bail!("{}", goal.summary);
                    }
                    approval_state.secondary_critic_receipt_hash =
                        Some(oracle_receipt.receipt_hash.clone());
                    seen_phase_identities.push(oracle_submission.reviewer.clone());
                    let oracle_route_receipt = phase_route_receipt_for_identity(
                        &oracle_decision,
                        oracle_ordinal,
                        &goal.id,
                        &plan,
                        &oracle_submission.reviewer,
                        oracle_worker_task_id.as_deref(),
                        oracle_worker_evidence_path.as_deref(),
                    )?;
                    let oracle_route_receipt_path = store.write_phase_route_receipt(
                        &goal.id,
                        oracle_ordinal,
                        &oracle_route_receipt,
                    )?;
                    let review_epoch_id = budget_context
                        .map(|context| context.1)
                        .unwrap_or("planning");
                    let review_epoch_bundle_path = write_review_epoch_bundle(
                        store,
                        &goal.id,
                        review_epoch_id,
                        &plan,
                        intent_fold_receipt.as_ref(),
                        intent_fold_artifact_path.as_deref(),
                        intent_fold_observation_path.clone(),
                        &planner_receipt,
                        &planner_receipt_path,
                        planner_observation_path.clone(),
                        &critic_receipt,
                        &critic_receipt_path,
                        submission.repository_evidence_path.clone(),
                        &oracle_receipt,
                        &oracle_receipt_path,
                        oracle_submission.repository_evidence_path.clone(),
                    )?;
                    append_event(
                        store,
                        event_sink,
                        event(
                            session_id,
                            Some(&goal.id),
                            None,
                            EventKind::PlanReviewApproved,
                            format!(
                                "Independent plan review approved revision {}",
                                plan.revision
                            ),
                            json!({
                                "plan_id": plan.plan_id,
                                "plan_hash": plan.plan_hash,
                                "revision": plan.revision,
                                "review_role": "oracle",
                                "receipt_path": oracle_receipt_path.to_string_lossy(),
                                "route_decision_path": oracle_decision_path.to_string_lossy(),
                                "route_receipt_path": oracle_route_receipt_path.to_string_lossy(),
                                "review_epoch_bundle_path": review_epoch_bundle_path.to_string_lossy(),
                            }),
                        ),
                    )?;
                }
                approval_state.status = PlanApprovalStatus::Approved;
                approval_state.updated_at = timestamp();
                store.write_plan_approval_state(&approval_state)?;
                append_event(
                    store,
                    event_sink,
                    event(
                        session_id,
                        Some(&goal.id),
                        None,
                        EventKind::PlanReviewApproved,
                        format!("Plan revision {} passed review", plan.revision),
                        json!({
                            "plan_id": plan.plan_id,
                            "plan_hash": plan.plan_hash,
                            "revision": plan.revision,
                            "critic_receipt_path": critic_receipt_path.to_string_lossy(),
                            "critic_route_decision_path": critic_decision_path.to_string_lossy(),
                            "critic_route_receipt_path": critic_route_receipt_path.to_string_lossy(),
                        }),
                    ),
                )?;
                goal.status = GoalStatus::Planning;
                goal.updated_at = timestamp();
                store.write_goal(goal)?;
                if let Some(receipt_failure) = verify_broker_receipts_for_goal(
                    phase_runtime.broker.as_deref(),
                    phase_runtime.broker_factory.as_deref(),
                    &goal.id,
                    false,
                ) {
                    bail!("Approval gate blocked by broker receipt failure: {receipt_failure}");
                }
                return Ok(plan);
            }
            PlanCriticDecision::Reject => {
                approval_state.status = PlanApprovalStatus::Rejected;
                approval_state.updated_at = timestamp();
                store.write_plan_approval_state(&approval_state)?;
                goal.status = GoalStatus::NeedsUser;
                goal.summary = critic_receipt
                    .verdict
                    .needs_user_reason
                    .clone()
                    .unwrap_or_else(|| critic_receipt.verdict.summary.clone());
                goal.updated_at = timestamp();
                store.write_goal(goal)?;
                append_event(
                    store,
                    event_sink,
                    event(
                        session_id,
                        Some(&goal.id),
                        None,
                        EventKind::PlanRejected,
                        format!("Plan revision {} requires user input", plan.revision),
                        json!({
                            "plan_id": plan.plan_id,
                            "plan_hash": plan.plan_hash,
                            "revision": plan.revision,
                            "reason": goal.summary,
                            "critic_receipt_path": critic_receipt_path.to_string_lossy(),
                        }),
                    ),
                )?;
                bail!("plan rejected before worker dispatch: {}", goal.summary);
            }
            PlanCriticDecision::Revise => {
                if revisions_performed >= phase_runtime.max_plan_revisions {
                    approval_state.status = PlanApprovalStatus::Limited;
                    approval_state.updated_at = timestamp();
                    store.write_plan_approval_state(&approval_state)?;
                    goal.status = GoalStatus::Limited;
                    goal.summary = format!(
                        "Plan review exhausted {} automatic revision(s)",
                        phase_runtime.max_plan_revisions
                    );
                    goal.updated_at = timestamp();
                    store.write_goal(goal)?;
                    bail!("{}; no worker was started", goal.summary);
                }
                approval_state.status = PlanApprovalStatus::Revising;
                approval_state.revisions_used = revisions_performed.saturating_add(1);
                approval_state.updated_at = timestamp();
                store.write_plan_approval_state(&approval_state)?;
                let revision_hook = phase_runtime
                    .plan_revision_hook
                    .as_ref()
                    .context("PlanCritic requested revision but no planner revision hook exists")?;
                append_event(
                    store,
                    event_sink,
                    event(
                        session_id,
                        Some(&goal.id),
                        None,
                        EventKind::PlanRevisionRequested,
                        format!("Plan revision {} must be revised", plan.revision),
                        json!({
                            "plan_id": plan.plan_id,
                            "plan_hash": plan.plan_hash,
                            "revision": plan.revision,
                            "instructions": critic_receipt.verdict.revision_instructions,
                            "critic_receipt_path": critic_receipt_path.to_string_lossy(),
                        }),
                    ),
                )?;
                check_run_cancelled(cancellation_token)?;
                let broker = phase_runtime.broker.as_deref();
                let broker_factory = phase_runtime.broker_factory.as_deref();
                let revision_identity = PhaseExecutionIdentity {
                    execution_id: format!(
                        "planner_revision:{}:{}:{}",
                        goal.id, plan.plan_id, plan.revision
                    ),
                    phase_session_id: format!("planner_revision:{}:{}", goal.id, plan.revision),
                    backend: PhaseExecutionBackend::DeterministicRules,
                    agent_id: None,
                    provider_id: None,
                    model_id: None,
                    actual_session_id: None,
                };
                let revision_budget_key = format!("planner-revision.{}", plan.revision);
                let budget_reservation = reserve_planning_phase_budget(
                    goal,
                    store,
                    budget_context,
                    &revision_budget_key,
                )?;
                let revision_result = run_phase_via_broker(
                    broker,
                    store,
                    phase_runtime.direct_model_usage_provider.as_ref(),
                    broker_factory,
                    &planner_decision,
                    &goal.id,
                    &plan.plan_id,
                    plan.revision,
                    "planner_revision",
                    &revision_identity,
                    || {
                        revision_hook(PlanRevisionInput {
                            request: goal.request.clone(),
                            plan: plan.clone(),
                            planner_receipt,
                            critic_receipt,
                            route_decision: planner_decision.clone(),
                        })
                        .context("planner revision hook failed")
                    },
                );
                settle_planning_phase_budget(
                    goal,
                    store,
                    budget_context,
                    budget_reservation.as_deref(),
                    &revision_budget_key,
                )?;
                let revision = revision_result?;
                check_run_cancelled(cancellation_token)?;
                if seen_phase_identities
                    .iter()
                    .any(|seen| !revision.planner.is_independent_from(seen))
                {
                    bail!("planner revision must use a globally fresh execution identity");
                }
                let parsed_revision = parse_planner_draft(&revision.raw_output)
                    .context("planner revision raw output is not a PlanGraphDraft")?;
                if parsed_revision != revision.draft {
                    bail!("planner revision raw output does not match its typed draft");
                }
                revision.planner.validate()?;
                let provider_id = revision
                    .planner
                    .provider_id
                    .clone()
                    .context("planner revision is missing provider identity")?;
                let model_id = revision
                    .planner
                    .model_id
                    .clone()
                    .context("planner revision is missing model identity")?;
                let previous_plan_hash = plan.plan_hash.clone();
                let next_revision = plan.revision.saturating_add(1);
                let revised_plan = PlanGraph::seal(
                    &goal.id,
                    next_revision,
                    PlanSource::PlannerModel,
                    Some(PlannerReceipt {
                        provider_id,
                        model_id,
                        session_id: revision.planner.actual_session_id.clone(),
                    }),
                    revision.draft,
                )?;
                if revised_plan.plan_hash == previous_plan_hash {
                    bail!("planner revision must change the sealed PlanGraph content hash");
                }
                plan = revised_plan;
                planner_raw_output = revision.raw_output;
                planner_identity = revision.planner;
                planner_artifact_path = revision.artifact_path;
                planner_observation_path = None;
                seen_phase_identities.push(planner_identity.clone());
                revisions_performed += 1;
            }
        }
    }
}

fn planner_raw_output(goal: &Goal, plan: &PlanGraph) -> Result<String> {
    match plan.source {
        PlanSource::PlannerModel => goal
            .coordinator_brief
            .clone()
            .context("planner-model PlanGraph is missing its raw planner output"),
        PlanSource::DeterministicFallback => serde_json::to_string(&plan.draft)
            .context("failed to serialize deterministic planner output"),
    }
}

fn planner_identity_for_plan(
    plan: &PlanGraph,
    configured: Option<PhaseExecutionIdentity>,
) -> Result<PhaseExecutionIdentity> {
    match plan.source {
        PlanSource::PlannerModel => {
            configured.context("plan approval requires a host-issued planner execution identity")
        }
        PlanSource::DeterministicFallback => Ok(PhaseExecutionIdentity {
            execution_id: format!("deterministic_planner_{}", &plan.plan_hash[..16]),
            phase_session_id: format!("deterministic_plan_{}", &plan.plan_hash[..16]),
            backend: PhaseExecutionBackend::DeterministicRules,
            agent_id: Some("gearbox".to_string()),
            provider_id: None,
            model_id: None,
            actual_session_id: None,
        }),
    }
}

fn validate_phase_execution_identity(
    decision: &PhaseRouteDecision,
    identity: &PhaseExecutionIdentity,
) -> Result<()> {
    identity.validate()?;
    match decision.candidate.backend {
        PhaseBackend::DirectModel => {
            if identity.backend != PhaseExecutionBackend::LanguageModelRequest {
                bail!("direct-model phase must use a language-model-request identity");
            }
        }
        PhaseBackend::NativeZed => {
            bail!(
                "native-Zed planner/PlanCritic sessions require the later ACP broker stage; use DirectModel for this phase"
            );
        }
        PhaseBackend::Deterministic => {
            if identity.backend != PhaseExecutionBackend::DeterministicRules {
                bail!("deterministic phase must use a deterministic-rules identity");
            }
        }
        PhaseBackend::Worker(worker_kind) => {
            if identity.backend != PhaseExecutionBackend::WorkerSession {
                bail!("worker phase must use a worker-session execution identity");
            }
            if identity.agent_id.as_deref() != Some(worker_kind.as_str()) {
                bail!("worker phase execution identity does not match its backend");
            }
            if let PhaseModelBinding::BackendDeclared(model) = &decision.candidate.model {
                let declared = ModelSelectorId::from_qualified(worker_kind.as_str(), model)?;
                if identity.provider_id.as_deref() != Some(declared.provider_id.as_str())
                    || identity.model_id.as_deref() != Some(declared.model_id.as_str())
                {
                    bail!("worker phase execution identity does not match its declared model");
                }
            }
        }
        PhaseBackend::LegacyCategory => {
            bail!("planner and PlanCritic phases cannot use a legacy category route")
        }
    }
    if let Some(requested_model) = decision.requested_model.as_ref()
        && (identity.agent_id.as_deref() != Some(requested_model.agent_id.as_str())
            || identity.provider_id.as_deref() != Some(requested_model.provider_id.as_str())
            || identity.model_id.as_deref() != Some(requested_model.model_id.as_str()))
    {
        bail!("phase execution identity does not match the resolved provider/model route");
    }
    Ok(())
}

fn phase_route_receipt_for_identity(
    decision: &PhaseRouteDecision,
    ordinal: usize,
    goal_id: &str,
    plan: &PlanGraph,
    identity: &PhaseExecutionIdentity,
    worker_task_id: Option<&str>,
    worker_artifact_path: Option<&str>,
) -> Result<PhaseRouteReceipt> {
    let (binding_status, applied_model) =
        match (&decision.candidate.backend, &decision.candidate.model) {
            (PhaseBackend::Deterministic, PhaseModelBinding::None) => {
                (ModelBindingStatus::Deterministic, None)
            }
            (PhaseBackend::DirectModel, PhaseModelBinding::CurrentSession) => (
                ModelBindingStatus::CurrentSession,
                decision.requested_model.clone(),
            ),
            (PhaseBackend::DirectModel, PhaseModelBinding::ExactLive(_)) => (
                ModelBindingStatus::Applied,
                decision.requested_model.clone(),
            ),
            (PhaseBackend::Worker(_), PhaseModelBinding::BackendDeclared(_)) => {
                (ModelBindingStatus::DeclaredUnverified, None)
            }
            (PhaseBackend::Worker(_), PhaseModelBinding::None) => {
                (ModelBindingStatus::LegacyUnverified, None)
            }
            _ => bail!("unsupported trusted planning phase backend/model binding"),
        };
    let worker_binding = match decision.candidate.backend {
        PhaseBackend::Worker(_worker_kind) => {
            let task_id = worker_task_id.context("worker phase receipt is missing its task id")?;
            let task_record_path = worker_artifact_path
                .context("worker phase receipt is missing its task-record evidence")?;
            let task_record_bytes = std::fs::read(task_record_path).with_context(|| {
                format!("failed to read worker phase task record at {task_record_path}")
            })?;
            let task_record: TaskRecord =
                serde_json::from_slice(&task_record_bytes).with_context(|| {
                    format!("failed to parse worker task record at {task_record_path}")
                })?;
            if task_record.task_id != task_id {
                bail!("worker phase task-record evidence belongs to another task");
            }
            let last_attempt = task_record
                .attempts
                .last()
                .context("worker phase task record has no attempts")?;
            let actual_worker_kind = WorkerKind::parse(&last_attempt.worker_kind)
                .context("worker phase task record has an unknown worker kind")?;
            let actual_category = WorkerCategory::parse(&last_attempt.worker_category)
                .context("worker phase task record has an unknown worker category")?;
            Some((
                task_id.to_string(),
                actual_worker_kind,
                actual_category,
                last_attempt.worker_model.clone(),
                last_attempt.route_reason.clone(),
                task_record_path.to_string(),
                format!("{:x}", Sha256::digest(task_record_bytes)),
            ))
        }
        _ => None,
    };
    PhaseRouteReceipt {
        decision: decision.clone(),
        ordinal,
        plan_revision: plan.revision,
        decision_hash: decision.hash()?,
        goal_id: Some(goal_id.to_string()),
        plan_id: Some(plan.plan_id.clone()),
        plan_hash: Some(plan.plan_hash.clone()),
        task_id: worker_binding
            .as_ref()
            .map(|(task_id, _, _, _, _, _, _)| task_id.clone()),
        worker_session_id: identity.actual_session_id.clone(),
        applied_model,
        actual_worker_kind: worker_binding
            .as_ref()
            .map(|(_, worker_kind, _, _, _, _, _)| *worker_kind),
        actual_category: worker_binding
            .as_ref()
            .map(|(_, _, category, _, _, _, _)| *category),
        actual_worker_model: worker_binding
            .as_ref()
            .and_then(|(_, _, _, model, _, _, _)| model.clone()),
        actual_route_reason: worker_binding
            .as_ref()
            .map(|(_, _, _, _, route_reason, _, _)| route_reason.clone()),
        task_record_path: worker_binding
            .as_ref()
            .map(|(_, _, _, _, _, path, _)| path.clone()),
        task_record_sha256: worker_binding
            .as_ref()
            .map(|(_, _, _, _, _, _, hash)| hash.clone()),
        binding_status,
        receipt_hash: String::new(),
    }
    .seal()
}

fn worker_task_id_from_artifact_path(path: Option<&str>) -> Option<String> {
    let path = std::path::Path::new(path?);
    path.parent()?
        .file_name()?
        .to_str()
        .map(ToString::to_string)
}

fn require_verified_repository_observation(
    store: &StateStore,
    plan: &PlanGraph,
    role: &str,
    path: Option<&str>,
) -> Result<()> {
    let path = path.context(format!(
        "{role} review did not produce a repository observation receipt"
    ))?;
    let contents = std_fs::read_to_string(path)
        .with_context(|| format!("failed to read repository observation receipt {path}"))?;
    let receipt: RepositoryObservationReceipt = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse repository observation receipt {path}"))?;
    receipt.validate()?;
    validate_repository_observation_capture_commit(role, &receipt, current_workspace_head(store)?)?;
    if receipt.goal_id != plan.goal_id
        || receipt.plan_id != plan.plan_id
        || receipt.plan_revision != plan.revision
        || receipt.plan_hash != plan.plan_hash
    {
        bail!("{role} repository observation receipt is bound to a different plan revision");
    }
    if receipt.role != role && !receipt.role.starts_with(&format!("{role}-")) {
        bail!("repository observation receipt role is not {role}");
    }
    if receipt.status != RepositoryObservationStatus::Verified {
        bail!("{role} repository observation is unverified");
    }
    Ok(())
}

fn require_verified_repository_observation_for_goal(
    store: &StateStore,
    path: Option<&str>,
    goal_id: &str,
    role: &str,
) -> Result<()> {
    let path = path.context(format!(
        "{role} phase did not produce a repository observation receipt"
    ))?;
    let contents = std_fs::read_to_string(path)
        .with_context(|| format!("failed to read repository observation receipt {path}"))?;
    let receipt: RepositoryObservationReceipt = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse repository observation receipt {path}"))?;
    receipt.validate()?;
    validate_repository_observation_capture_commit(role, &receipt, current_workspace_head(store)?)?;
    if receipt.goal_id != goal_id {
        bail!("{role} repository observation receipt is bound to a different goal");
    }
    if receipt.role != role && !receipt.role.starts_with(&format!("{role}-")) {
        bail!("repository observation receipt role is not {role}");
    }
    if receipt.status != RepositoryObservationStatus::Verified {
        bail!("{role} repository observation is unverified");
    }
    Ok(())
}

fn require_repository_discovery_artifact(
    store: &StateStore,
    discovery: &RepositoryDiscoverySubmission,
    goal_id: &str,
) -> Result<()> {
    if discovery.raw_output.trim().is_empty() || !Path::new(&discovery.artifact_path).is_file() {
        bail!(
            "broker-backed planning requires a readable repository discovery artifact: {}",
            discovery.artifact_path
        );
    }
    let contents = std_fs::read_to_string(&discovery.artifact_path).with_context(|| {
        format!(
            "failed to read repository discovery artifact {}",
            discovery.artifact_path
        )
    })?;
    let artifact: Value = serde_json::from_str(&contents).with_context(|| {
        format!(
            "failed to parse repository discovery artifact {}",
            discovery.artifact_path
        )
    })?;
    if artifact["schema_version"] != 1
        || artifact["phase"] != "repository_discovery"
        || artifact["goal_id"] != goal_id
        || artifact["task_id"] != format!("repository_discovery_{goal_id}")
    {
        bail!("repository discovery artifact binding mismatch");
    }
    let raw_output = artifact["raw_output"]
        .as_str()
        .context("repository discovery artifact is missing raw_output")?;
    if raw_output != discovery.raw_output {
        bail!("repository discovery submission does not match its artifact");
    }
    if artifact["raw_output_sha256"] != hash_text(raw_output) {
        bail!("repository discovery artifact raw output hash mismatch");
    }
    let current_commit = current_workspace_head(store)?;
    validate_discovery_capture_commit(
        artifact["capture_commit"].as_str(),
        current_commit.as_deref(),
    )?;
    let observation_path = artifact["repository_observation_path"]
        .as_str()
        .with_context(|| "repository discovery artifact is missing repository observation path")?;
    require_verified_repository_observation_for_goal(
        store,
        Some(observation_path),
        goal_id,
        "planner",
    )?;
    Ok(())
}

fn validate_discovery_capture_commit(
    captured_commit: Option<&str>,
    current_commit: Option<&str>,
) -> Result<()> {
    let Some(current_commit) = current_commit else {
        return Ok(());
    };
    let captured_commit = captured_commit.context(format!(
        "repository discovery artifact is missing capture commit for current HEAD {current_commit}"
    ))?;
    if captured_commit != current_commit {
        bail!(
            "repository discovery artifact is stale: captured commit {captured_commit}, current HEAD {current_commit}"
        );
    }
    Ok(())
}

fn current_workspace_head(store: &StateStore) -> Result<Option<String>> {
    let workspace = store
        .root()
        .parent()
        .context("Gear StateStore root has no workspace parent")?;
    git_head_commit(workspace)
}

fn validate_repository_observation_capture_commit(
    role: &str,
    receipt: &RepositoryObservationReceipt,
    current_commit: Option<String>,
) -> Result<()> {
    let Some(current_commit) = current_commit else {
        return Ok(());
    };
    let captured_commit = receipt.capture_commit.as_deref().with_context(|| {
        format!(
            "{role} repository observation is missing capture commit for current HEAD {current_commit}; rerun the phase"
        )
    })?;
    if captured_commit != current_commit {
        bail!(
            "{role} repository observation is stale: captured commit {captured_commit}, current HEAD {current_commit}; rerun the phase"
        );
    }
    Ok(())
}

fn review_epoch_role_evidence(
    store: &StateStore,
    goal_id: &str,
    role: &str,
    identity: &PhaseExecutionIdentity,
    receipt_hash: String,
    receipt_path: &std::path::Path,
    observation_path: Option<String>,
) -> Result<ReviewEpochRoleEvidence> {
    let usage = store
        .read_model_call_ledger(goal_id)?
        .into_iter()
        .rev()
        .find(|entry| {
            identity
                .actual_session_id
                .as_deref()
                .is_some_and(|session_id| entry.session_id == session_id)
        });
    let usage_known = usage.as_ref().is_some_and(|entry| {
        entry.requested_tokens.is_some()
            || entry.actual_tokens.is_some()
            || entry.cost_micros.is_some()
            || entry.duration_ms.is_some()
            || entry.cache_hit.is_some()
    });
    Ok(ReviewEpochRoleEvidence {
        role: role.to_string(),
        execution_id: identity.execution_id.clone(),
        phase_session_id: identity.phase_session_id.clone(),
        actual_session_id: identity.actual_session_id.clone(),
        receipt_hash,
        receipt_path: receipt_path.to_string_lossy().to_string(),
        observation_path,
        requested_tokens: usage.as_ref().and_then(|entry| entry.requested_tokens),
        actual_tokens: usage.as_ref().and_then(|entry| entry.actual_tokens),
        cost_micros: usage.as_ref().and_then(|entry| entry.cost_micros),
        duration_ms: usage.as_ref().and_then(|entry| entry.duration_ms),
        cache_hit: usage.as_ref().and_then(|entry| entry.cache_hit),
        unknown_reason: (!usage_known).then(|| {
            usage
                .as_ref()
                .and_then(|entry| entry.unavailable_reason.clone())
                .unwrap_or_else(|| "review role usage was not reported by the provider".to_string())
        }),
    })
}

fn write_review_epoch_bundle(
    store: &StateStore,
    goal_id: &str,
    epoch_id: &str,
    plan: &PlanGraph,
    intent_fold: Option<&crate::plan_review::IntentFoldReceipt>,
    intent_fold_artifact_path: Option<&str>,
    intent_fold_observation_path: Option<String>,
    planner_receipt: &PlannerExecutionReceipt,
    planner_receipt_path: &std::path::Path,
    planner_observation_path: Option<String>,
    critic_receipt: &PlanCriticReceipt,
    critic_receipt_path: &std::path::Path,
    critic_observation_path: Option<String>,
    oracle_receipt: &PlanCriticReceipt,
    oracle_receipt_path: &std::path::Path,
    oracle_observation_path: Option<String>,
) -> Result<std::path::PathBuf> {
    let mut roles = vec![
        review_epoch_role_evidence(
            store,
            goal_id,
            "planner",
            &planner_receipt.identity,
            planner_receipt.receipt_hash.clone(),
            planner_receipt_path,
            planner_observation_path,
        )?,
        review_epoch_role_evidence(
            store,
            goal_id,
            "momus",
            &critic_receipt.reviewer,
            critic_receipt.receipt_hash.clone(),
            critic_receipt_path,
            critic_observation_path,
        )?,
        review_epoch_role_evidence(
            store,
            goal_id,
            "oracle",
            &oracle_receipt.reviewer,
            oracle_receipt.receipt_hash.clone(),
            oracle_receipt_path,
            oracle_observation_path,
        )?,
    ];
    if let Some(intent_fold) = intent_fold {
        let artifact_path = intent_fold_artifact_path
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("intent-fold-receipt.json"));
        roles.push(review_epoch_role_evidence(
            store,
            goal_id,
            "metis",
            &intent_fold.analyst,
            intent_fold.receipt_hash.clone(),
            &artifact_path,
            intent_fold_observation_path,
        )?);
    }
    let bundle = ReviewEpochBundle::seal(goal_id, epoch_id, plan, roles, intent_fold.is_some())?;
    store.write_review_epoch_bundle(&bundle)
}

fn phase_worker_evidence_path(
    store: &StateStore,
    goal_id: &str,
    task_id: Option<&str>,
    artifact_path: Option<&str>,
) -> Result<Option<String>> {
    let (Some(task_id), Some(artifact_path)) = (task_id, artifact_path) else {
        return Ok(None);
    };
    let source = std::path::Path::new(artifact_path)
        .parent()
        .context("worker phase artifact is missing its task directory")?
        .join("task-record.json");
    if !source.is_file() {
        bail!(
            "worker phase task-record evidence is missing at {}",
            source.display()
        );
    }
    let destination = store
        .phase_routes_dir(goal_id)
        .join("worker-evidence")
        .join(format!("{task_id}-task-record.json"));
    if source != destination {
        if let Some(parent) = destination.parent() {
            std_fs::create_dir_all(parent)?;
        }
        std_fs::copy(&source, &destination).with_context(|| {
            format!(
                "failed to copy worker task-record evidence from {} to {}",
                source.display(),
                destination.display()
            )
        })?;
    }
    Ok(Some(destination.to_string_lossy().to_string()))
}

fn worker_phase_for_route_hint(preferred: &PhaseProfile, route_hint: Option<&str>) -> PhaseProfile {
    match route_hint
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("quick") => PhaseProfile::ExecutorQuick,
        Some("deep") | Some("repair") => PhaseProfile::ExecutorDeep,
        Some("review") => PhaseProfile::ReviewerFinal,
        _ => match preferred {
            PhaseProfile::ExecutorQuick | PhaseProfile::ExecutorDeep => preferred.clone(),
            PhaseProfile::ReviewerTask | PhaseProfile::ReviewerFinal => PhaseProfile::ReviewerFinal,
            _ => PhaseProfile::ExecutorQuick,
        },
    }
}

fn persist_live_plan_node_session_binding(
    store: &StateStore,
    goal_id: &str,
    epoch_id: &str,
    plan: &PlanGraph,
    plan_task_id: &str,
    attempt: usize,
    managed_worker_task_id: &str,
    selected_route: &SelectedWorkerRoute<'_>,
    route_receipt_hash: &str,
) -> Result<bool> {
    let task_record_path = store
        .worker_dir(managed_worker_task_id)
        .join("task-record.json");
    if !task_record_path.is_file() {
        return Ok(false);
    }
    let record: TaskRecord = serde_json::from_str(&std_fs::read_to_string(&task_record_path)?)
        .with_context(|| {
            format!(
                "failed to parse live task record at {}",
                task_record_path.display()
            )
        })?;
    let Some(session_id) = record.session_id else {
        return Ok(false);
    };
    let provider_id = selected_route
        .worker_model
        .and_then(|model| model.split_once('/'))
        .map(|(provider, _)| provider.to_string());
    let binding = crate::state::PlanNodeSessionBinding {
        schema_version: crate::state::PLAN_NODE_SESSION_BINDING_SCHEMA_VERSION,
        binding_id: format!("{goal_id}:{epoch_id}:{plan_task_id}:{attempt}"),
        goal_id: goal_id.to_string(),
        epoch_id: epoch_id.to_string(),
        plan_id: plan.plan_id.clone(),
        plan_revision: plan.revision,
        plan_hash: plan.plan_hash.clone(),
        task_id: plan_task_id.to_string(),
        attempt,
        worker_task_id: managed_worker_task_id.to_string(),
        worker_kind: selected_route.worker_kind.as_str().to_string(),
        provider_id,
        model_id: selected_route.worker_model.map(str::to_string),
        session_id,
        capability_fingerprint: "unknown-at-start".to_string(),
        route_receipt_hash: Some(route_receipt_hash.to_string()),
        status: crate::state::PlanNodeSessionBindingStatus::Active,
        supersedes_binding_id: (attempt > 1).then(|| {
            format!(
                "{goal_id}:{epoch_id}:{plan_task_id}:{}",
                attempt.saturating_sub(1)
            )
        }),
        created_at: timestamp(),
        updated_at: timestamp(),
    };
    store.write_plan_node_session_binding(&binding)?;
    Ok(true)
}

fn try_resume_plan_session(
    task_manager: &SharedTaskManager,
    store: &StateStore,
    goal_id: &str,
    epoch_id: &str,
    plan: &PlanGraph,
    plan_task_id: &str,
    attempt: usize,
    selected_route: &SelectedWorkerRoute<'_>,
    prompt: &str,
) -> Result<Option<String>> {
    let previous_attempt = attempt.saturating_sub(1);
    if previous_attempt == 0 {
        return Ok(None);
    }
    let Some(binding) =
        store.read_plan_node_session_binding(goal_id, epoch_id, plan_task_id, previous_attempt)?
    else {
        return Ok(None);
    };
    if binding.status != crate::state::PlanNodeSessionBindingStatus::Active
        || binding.plan_id != plan.plan_id
        || binding.plan_revision != plan.revision
        || binding.plan_hash != plan.plan_hash
        || binding.worker_kind != selected_route.worker_kind.as_str()
        || binding.model_id != selected_route.worker_model.map(str::to_string)
    {
        return Ok(None);
    }
    let previous_record = match task_manager
        .lock()
        .map_err(|_| anyhow::anyhow!("task manager mutex poisoned while selecting session resume"))?
        .list()
        .into_iter()
        .find(|record| record.task_id == binding.worker_task_id)
    {
        Some(record) => record,
        None => return Ok(None),
    };
    if previous_record.session_id.as_deref() != Some(binding.session_id.as_str())
        || previous_record.worker_kind != binding.worker_kind
        || previous_record.worker_model != binding.model_id
    {
        return Ok(None);
    }
    let outcome = task_manager
        .lock()
        .map_err(|_| {
            anyhow::anyhow!("task manager mutex poisoned while sending session follow-up")
        })?
        .send_follow_up_task(&binding.worker_task_id, prompt.to_string())?;
    if matches!(
        outcome,
        SendOutcome::Sent(_)
            | SendOutcome::Queued(_)
            | SendOutcome::Revive(_)
            | SendOutcome::PossiblyAccepted(_)
    ) {
        Ok(Some(binding.worker_task_id))
    } else {
        Ok(None)
    }
}

fn phase_route_receipt_for_worker(
    decision: &PhaseRouteDecision,
    ordinal: usize,
    goal_id: &str,
    plan: &PlanGraph,
    task_id: &str,
    worker_session_id: Option<&str>,
    task_record: &TaskRecord,
    store: &StateStore,
) -> Result<PhaseRouteReceipt> {
    let last_attempt = task_record
        .attempts
        .last()
        .context("phase-routed worker finished without a recorded attempt")?;
    let actual_worker_kind = WorkerKind::parse(&last_attempt.worker_kind)
        .context("phase-routed worker recorded an unknown worker kind")?;
    let actual_category = WorkerCategory::parse(&last_attempt.worker_category)
        .context("phase-routed worker recorded an unknown worker category")?;
    if let Some(expected_worker_kind) = decision.worker_kind
        && actual_worker_kind != expected_worker_kind
    {
        bail!(
            "phase route selected `{}` but task completed on `{}`",
            expected_worker_kind.as_str(),
            last_attempt.worker_kind
        );
    }

    let (binding_status, applied_model) =
        match (&decision.candidate.backend, &decision.candidate.model) {
            (PhaseBackend::LegacyCategory, PhaseModelBinding::None) => {
                (ModelBindingStatus::LegacyUnverified, None)
            }
            (PhaseBackend::Worker(_), PhaseModelBinding::None) => {
                (ModelBindingStatus::LegacyUnverified, None)
            }
            (PhaseBackend::Worker(_), PhaseModelBinding::BackendDeclared(model)) => {
                if last_attempt.worker_model.as_deref() != Some(model.as_str()) {
                    bail!("phase worker attempt did not preserve its declared model");
                }
                (ModelBindingStatus::DeclaredUnverified, None)
            }
            (
                PhaseBackend::NativeZed,
                PhaseModelBinding::CurrentSession | PhaseModelBinding::ExactLive(_),
            ) => {
                let requested_model = decision
                    .requested_model
                    .as_ref()
                    .context("native phase route is missing its requested model")?;
                let selection_path = store.worker_dir(task_id).join("model-selection.json");
                let selection: serde_json::Value = serde_json::from_str(
                    &std_fs::read_to_string(&selection_path).with_context(|| {
                        format!(
                            "missing native phase model evidence at {}",
                            selection_path.display()
                        )
                    })?,
                )
                .with_context(|| {
                    format!(
                        "invalid native model evidence at {}",
                        selection_path.display()
                    )
                })?;
                let applied = selection
                    .get("applied_model")
                    .and_then(serde_json::Value::as_str)
                    .context("native model evidence is missing applied_model")?;
                let requested = selection
                    .get("requested_model")
                    .and_then(serde_json::Value::as_str)
                    .context("native model evidence is missing requested_model")?;
                if requested != requested_model.qualified_model_id() {
                    bail!("native model evidence requested model does not match its route");
                }
                let expected_worker_session_id = worker_session_id
                    .context("native phase route is missing its worker session id")?;
                let evidence_worker_session_id = selection
                    .get("worker_session_id")
                    .and_then(serde_json::Value::as_str)
                    .context("native model evidence is missing worker_session_id")?;
                if evidence_worker_session_id != expected_worker_session_id {
                    bail!("native model evidence belongs to a different worker session");
                }
                let applied_model =
                    ModelSelectorId::from_qualified(requested_model.agent_id.clone(), applied)?;
                if &applied_model != requested_model {
                    bail!("native phase applied model does not match its exact route decision");
                }
                (ModelBindingStatus::Applied, Some(applied_model))
            }
            _ => bail!("non-worker phase backend reached programming worker dispatch"),
        };

    let task_record_path = store
        .phase_routes_dir(goal_id)
        .join("worker-evidence")
        .join(format!("{task_id}-task-record.json"));
    crate::state::write_json(&task_record_path, task_record)
        .context("failed to persist immutable phase route task-record snapshot")?;
    let task_record_bytes = std_fs::read(&task_record_path)
        .context("failed to read phase-routed task-record snapshot for receipt")?;
    PhaseRouteReceipt {
        decision: decision.clone(),
        ordinal,
        plan_revision: plan.revision,
        decision_hash: decision.hash()?,
        goal_id: Some(goal_id.to_string()),
        plan_id: Some(plan.plan_id.clone()),
        plan_hash: Some(plan.plan_hash.clone()),
        task_id: Some(task_id.to_string()),
        worker_session_id: worker_session_id.map(ToOwned::to_owned),
        applied_model,
        actual_worker_kind: Some(actual_worker_kind),
        actual_category: Some(actual_category),
        actual_worker_model: last_attempt.worker_model.clone(),
        actual_route_reason: Some(last_attempt.route_reason.clone()),
        task_record_path: Some(task_record_path.to_string_lossy().to_string()),
        task_record_sha256: Some(format!("{:x}", Sha256::digest(task_record_bytes))),
        binding_status,
        receipt_hash: String::new(),
    }
    .seal()
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

fn build_plan_graph(
    goal: &Goal,
    scope: &Scope,
    verification_commands: &[String],
) -> Result<PlanGraph> {
    match goal.coordinator_brief.as_deref() {
        Some(output) => match parse_planner_draft(output) {
            Ok(draft) => PlanGraph::seal(
                &goal.id,
                1,
                PlanSource::PlannerModel,
                goal.coordinator_model.as_ref().map(|model| PlannerReceipt {
                    provider_id: model.provider_id.clone(),
                    model_id: model.model_id.clone(),
                    session_id: None,
                }),
                draft,
            ),
            Err(error)
                if goal
                    .coordinator_model
                    .as_ref()
                    .is_some_and(|model| model.provider_id != "fake") =>
            {
                Err(error).context("configured Gear planner returned an invalid PlanGraph")
            }
            Err(_) => PlanGraph::seal(
                &goal.id,
                1,
                PlanSource::DeterministicFallback,
                None,
                deterministic_fallback_draft(&goal.request, scope, verification_commands),
            ),
        },
        None if goal
            .coordinator_model
            .as_ref()
            .is_some_and(|model| model.provider_id != "fake") =>
        {
            bail!("configured Gear planner did not return a PlanGraph")
        }
        None => PlanGraph::seal(
            &goal.id,
            1,
            PlanSource::DeterministicFallback,
            None,
            deterministic_fallback_draft(&goal.request, scope, verification_commands),
        ),
    }
}

fn phase_profile_route_hint(profile: &PhaseProfile) -> Option<&'static str> {
    match profile {
        PhaseProfile::ExecutorQuick => Some("quick"),
        PhaseProfile::ExecutorDeep => Some("deep"),
        PhaseProfile::ReviewerTask | PhaseProfile::ReviewerFinal => Some("review"),
        _ => None,
    }
}

fn review_changed_workspace(
    route_hint: Option<&str>,
    before: &DiffSnapshot,
    after: &DiffSnapshot,
) -> bool {
    route_hint == Some("review")
        && (before.status != after.status
            || before.changed_files != after.changed_files
            || before.diff_hash != after.diff_hash)
}

fn stop_lineage_task(store: &StateStore, lineage: &mut WorkLineage, task_id: &str) -> Result<()> {
    lineage.active_task_ids.retain(|active| active != task_id);
    lineage.status = ContinuationStatus::Stopped;
    lineage.updated_at = timestamp();
    store.write_lineage(lineage)?;
    Ok(())
}

fn prepare_lineage_for_run(lineage: &mut WorkLineage, session_id: &str) {
    if !lineage
        .orchestrator_session_ids
        .iter()
        .any(|existing| existing == session_id)
    {
        lineage
            .orchestrator_session_ids
            .push(session_id.to_string());
    }
    lineage.status = ContinuationStatus::Running;
    lineage.plan_remaining_items = 1;
    lineage.active_task_ids.clear();
    lineage.updated_at = timestamp();
}

fn initial_tasks(goal_id: &str, scope: &Scope) -> Vec<Task> {
    [
        ("task_001", "Generate minimal spec", TaskKind::Spec, None),
        ("task_002", "Generate executable plan", TaskKind::Plan, None),
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

fn scoped_task_id(namespace: Option<&str>, base_id: &str) -> String {
    namespace
        .filter(|namespace| !namespace.trim().is_empty())
        .map(|namespace| format!("{namespace}::{base_id}"))
        .unwrap_or_else(|| base_id.to_string())
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

fn plan_artifact_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn write_plan_command_evidence(
    store: &StateStore,
    goal_id: &str,
    task_id: &str,
    revision: usize,
    phase: &str,
    expectation: &crate::plan_graph::CommandExpectation,
    result: Option<&ShellCommandResult>,
) -> Result<std::path::PathBuf> {
    let file_name = format!(
        "plan-node-{}-{}-r{revision}.md",
        plan_artifact_component(task_id),
        phase
    );
    let body = match result {
        Some(result) => format!(
            "# Plan node {phase} evidence\n\nTask: {task_id}\n\nCommand: {}\n\nExpected observation: {}\n\nExit code: {:?}\n\nSuccess: {}\n\n## stdout\n\n{}\n\n## stderr\n\n{}\n",
            expectation.command,
            expectation.expected_observation,
            result.exit_code,
            result.success,
            result.stdout,
            result.stderr,
        ),
        None => format!(
            "# Plan node {phase} evidence\n\nTask: {task_id}\n\nNo command was required. Reason: {}\n",
            expectation.expected_observation
        ),
    };
    store.write_artifact(goal_id, &file_name, &body)
}

fn run_plan_red_evidence(
    workspace: &std::path::Path,
    store: &StateStore,
    goal_id: &str,
    task_id: &str,
    revision: usize,
    plan_task: &crate::plan_graph::PlanTaskContract,
    cancellation_token: Option<&CancellationToken>,
) -> Result<std::path::PathBuf> {
    let expectation = plan_task
        .test
        .red
        .as_ref()
        .with_context(|| format!("TDD task {task_id} is missing RED expectation"))?;
    let result = run_shell_command_with_env_and_cancellation(
        workspace,
        &expectation.command,
        &std::collections::HashMap::new(),
        cancellation_token,
    )
    .with_context(|| format!("failed to execute RED command for plan node {task_id}"))?;
    let evidence_path = write_plan_command_evidence(
        store,
        goal_id,
        task_id,
        revision,
        "red",
        expectation,
        Some(&result),
    )?;
    if result.success {
        bail!(
            "TDD RED command unexpectedly passed for plan node {task_id}; evidence at {}",
            evidence_path.display()
        );
    }
    Ok(evidence_path)
}

fn run_plan_green_evidence(
    workspace: &std::path::Path,
    store: &StateStore,
    goal_id: &str,
    task_id: &str,
    revision: usize,
    plan_task: &crate::plan_graph::PlanTaskContract,
    cancellation_token: Option<&CancellationToken>,
) -> Result<(Vec<std::path::PathBuf>, bool)> {
    if matches!(
        plan_task.test.strategy,
        crate::plan_graph::TestStrategy::None
    ) {
        let reason = plan_task
            .test
            .no_test_reason
            .clone()
            .unwrap_or_else(|| "No test command was requested.".to_string());
        let expectation = crate::plan_graph::CommandExpectation {
            command: "none".to_string(),
            expected_observation: reason,
            evidence_path: format!("plan-node-{task_id}-no-test.md"),
        };
        let path = write_plan_command_evidence(
            store,
            goal_id,
            task_id,
            revision,
            "no-test",
            &expectation,
            None,
        )?;
        return Ok((vec![path], true));
    }

    let mut paths = Vec::new();
    let mut passed = true;
    for (index, expectation) in plan_task.test.green.iter().enumerate() {
        let result = run_shell_command_with_env_and_cancellation(
            workspace,
            &expectation.command,
            &std::collections::HashMap::new(),
            cancellation_token,
        )
        .with_context(|| format!("failed to execute GREEN command for plan node {task_id}"))?;
        let phase = format!("green-{index}");
        paths.push(write_plan_command_evidence(
            store,
            goal_id,
            task_id,
            revision,
            &phase,
            expectation,
            Some(&result),
        )?);
        passed &= result.success;
    }
    Ok((paths, passed))
}

fn final_verification_wave_markdown(receipt: &FinalVerificationWaveReceipt) -> String {
    let mut markdown = format!(
        "## Final Verification Wave\n\nReceipt hash: {}\n\nPassed: {}\n\n",
        receipt.receipt_hash, receipt.passed
    );
    for result in &receipt.dimensions {
        markdown.push_str(&format!(
            "- {:?}: {} — {}\n  - evidence: {}\n  - reviewer executions: {}\n",
            result.dimension,
            if result.passed { "pass" } else { "fail" },
            result.summary,
            result.evidence_paths.join(", "),
            result.reviewer_execution_ids.join(", "),
        ));
    }
    markdown
}

fn persist_plan_criterion_evidence(
    store: &StateStore,
    workspace: &Path,
    plan: &PlanGraph,
    ledger: &mut PlanNodeRunLedger,
) -> Result<()> {
    let ledger_goal_id = ledger.goal_id.clone();
    for task in &plan.draft.tasks {
        let node = ledger
            .nodes
            .iter_mut()
            .find(|node| node.task_id == task.task_id)
            .with_context(|| format!("missing PlanNodeRun for criterion task {}", task.task_id))?;
        if node.attempt == 0 {
            continue;
        }
        let node_attempt = node.attempt;
        let mut candidates = node.green_evidence_paths.clone();
        if let Some(review_path) = node.review_evidence_path.clone() {
            candidates.insert(0, review_path);
        }
        for criterion in &task.completion_predicates {
            let normalized = criterion.to_ascii_lowercase();
            let candidate = candidates.iter().find(|path| {
                let path_text = path.to_ascii_lowercase();
                if normalized.contains("review") {
                    path_text.contains("review")
                } else if normalized.contains("test")
                    || normalized.contains("verification")
                    || normalized.contains("qa")
                {
                    path_text.contains("green")
                        || path_text.contains("verification")
                        || path_text.contains("test")
                } else {
                    true
                }
            });
            let verified_artifact = candidate.cloned().and_then(|candidate| {
                let path = PathBuf::from(&candidate);
                let absolute = if path.is_absolute() {
                    path
                } else {
                    workspace.join(path)
                };
                let canonical_workspace = workspace.canonicalize().ok()?;
                let canonical_artifact = absolute.canonicalize().ok()?;
                let relative = canonical_artifact
                    .strip_prefix(&canonical_workspace)
                    .ok()?
                    .to_string_lossy()
                    .replace('\\', "/");
                if relative.is_empty() || relative == ".." || relative.starts_with("../") {
                    return None;
                }
                Some((relative, canonical_artifact))
            });
            if let Some((relative, canonical_artifact)) = verified_artifact {
                node.record_criterion_evidence(
                    criterion,
                    CriterionEvidenceStatus::Pass,
                    node_attempt,
                    &relative,
                    &sha256_file(&canonical_artifact)?,
                )?;
            } else {
                let status = if matches!(
                    node.status,
                    PlanNodeRunStatus::Failed | PlanNodeRunStatus::Cancelled
                ) {
                    CriterionEvidenceStatus::Fail
                } else {
                    CriterionEvidenceStatus::Blocked
                };
                let marker_id = hash_text(&format!("{}:{node_attempt}:{criterion}", task.task_id));
                let marker_path = store.write_artifact(
                    &ledger_goal_id,
                    &format!("criterion-{marker_id}.json"),
                    &json!({
                        "task_id": task.task_id,
                        "criterion": criterion,
                        "status": status.clone(),
                        "reason": "no verified artifact matched this completion predicate",
                    })
                    .to_string(),
                )?;
                let canonical_workspace = workspace.canonicalize()?;
                let canonical_marker = marker_path.canonicalize()?;
                let relative = canonical_marker
                    .strip_prefix(&canonical_workspace)
                    .context("criterion marker escaped workspace")?
                    .to_string_lossy()
                    .replace('\\', "/");
                node.record_criterion_evidence(
                    criterion,
                    status,
                    node_attempt,
                    &relative,
                    &sha256_file(&canonical_marker)?,
                )?;
            }
        }
    }
    store.write_plan_node_runs(ledger)?;
    Ok(())
}

fn build_final_verification_wave(
    goal_id: &str,
    epoch_id: &str,
    plan: &PlanGraph,
    node_runs: &PlanNodeRunLedger,
    worker_result: &WorkerResult,
    worker_outcome: &WorkerOutcome,
    verification_results: &[ShellCommandResult],
    verification_path: Option<&std::path::Path>,
    scope_check: &crate::tools::ScopeCheck,
    criteria_complete: bool,
) -> Result<FinalVerificationWaveReceipt> {
    let all_nodes_completed = node_runs
        .nodes
        .iter()
        .all(|node| node.status == PlanNodeRunStatus::Completed);
    let node_evidence = node_runs
        .nodes
        .iter()
        .flat_map(|node| {
            node.review_evidence_path
                .iter()
                .chain(node.green_evidence_paths.iter())
        })
        .cloned()
        .collect::<Vec<_>>();
    let node_evidence = if node_evidence.is_empty() {
        vec!["runtime-plan-node-ledger".to_string()]
    } else {
        node_evidence
    };
    let node_reviewer_ids = node_runs
        .nodes
        .iter()
        .filter_map(|node| {
            node.review_task_id.clone().or_else(|| {
                (node_runs.nodes.len() == 1)
                    .then(|| node.worker_task_id.clone())
                    .flatten()
            })
        })
        .collect::<Vec<_>>();
    let worker_evidence = vec![
        worker_result.result_path.to_string_lossy().to_string(),
        worker_result.outcome_path.to_string_lossy().to_string(),
    ];
    let worker_reviewer_id = worker_outcome
        .session_id
        .clone()
        .unwrap_or_else(|| "worker-outcome".to_string());
    let qa_evidence = verification_path
        .map(|path| vec![path.to_string_lossy().to_string()])
        .unwrap_or_default();
    let scope_evidence = vec![worker_result.result_path.to_string_lossy().to_string()];
    let dimensions = vec![
        FinalVerificationResult {
            dimension: FinalVerificationDimension::PlanCompliance,
            passed: criteria_complete
                && all_nodes_completed
                && node_evidence.len() >= node_runs.nodes.len()
                && (node_runs.nodes.len() == 1 || node_reviewer_ids.len() >= node_runs.nodes.len()),
            summary: "Every PlanGraph node has terminal execution, GREEN, and review evidence."
                .to_string(),
            evidence_paths: node_evidence.clone(),
            reviewer_execution_ids: if node_reviewer_ids.is_empty() {
                vec!["runtime-plan-reducer".to_string()]
            } else {
                node_reviewer_ids.clone()
            },
        },
        FinalVerificationResult {
            dimension: FinalVerificationDimension::CodeQuality,
            passed: worker_result.status == WorkerStatus::Succeeded,
            summary: "The final worker result is successful and has a persisted outcome chain."
                .to_string(),
            evidence_paths: if worker_evidence.is_empty() {
                vec!["runtime-worker-result".to_string()]
            } else {
                worker_evidence
            },
            reviewer_execution_ids: vec![worker_reviewer_id.clone()],
        },
        FinalVerificationResult {
            dimension: FinalVerificationDimension::RealQa,
            passed: !verification_results.is_empty()
                && verification_results.iter().all(|result| result.success),
            summary: "Gear-owned verification commands all passed.".to_string(),
            evidence_paths: if qa_evidence.is_empty() {
                vec!["runtime-verification".to_string()]
            } else {
                qa_evidence
            },
            reviewer_execution_ids: vec![worker_reviewer_id.clone()],
        },
        FinalVerificationResult {
            dimension: FinalVerificationDimension::ScopeFidelity,
            passed: !scope_check.max_files_exceeded
                && scope_check.forbidden_touches.is_empty()
                && scope_check.outside_allowed_paths.is_empty(),
            summary: "The final diff remains inside the approved scope.".to_string(),
            evidence_paths: scope_evidence,
            reviewer_execution_ids: vec!["runtime-scope-check".to_string()],
        },
    ];
    FinalVerificationWaveReceipt::seal(goal_id, epoch_id, plan, dimensions)
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

fn prompt_settle_event_for_guard_reason(reason: &str) -> PromptSettleEvent {
    let reason = reason.to_ascii_lowercase();
    if reason.contains("cancel") {
        PromptSettleEvent::UserStopped
    } else if reason.contains("context") || reason.contains("token") || reason.contains("compact") {
        PromptSettleEvent::ContextPressure
    } else if reason.contains("background")
        || reason.contains("question")
        || reason.contains("continuation")
        || reason.contains("flight")
        || reason.contains("recover")
    {
        PromptSettleEvent::Busy
    } else {
        PromptSettleEvent::Error
    }
}

fn update_verification_task(
    tasks: &mut [Task],
    verification_task_id: &str,
    results: &[ShellCommandResult],
    verification_path: String,
    verification_passed: bool,
) {
    if let Some(task) = tasks
        .iter_mut()
        .find(|task| task.id == verification_task_id)
    {
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
    append_completion_notification_with_event_writer(
        store,
        session_id,
        goal_id,
        task_id,
        run_epoch,
        &|completion_event| append_event(store, event_sink, completion_event.clone()),
    )
}

fn append_completion_notification_with_event_writer(
    store: &StateStore,
    session_id: &str,
    goal_id: &str,
    task_id: &str,
    run_epoch: u64,
    event_writer: &dyn Fn(&Event) -> Result<()>,
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

    let completion_event = event(
        session_id,
        Some(goal_id),
        Some(task_id),
        EventKind::CompletionNotified,
        format!(
            "{} {} in {}ms: {} ({})",
            notification.task_name,
            format!("{:?}", notification.status),
            notification.duration_ms,
            notification.summary_head,
            notification.continuation_hint,
        ),
        json!({
            "task_name": notification.task_name,
            "status": format!("{:?}", notification.status),
            "duration_ms": notification.duration_ms,
            "summary": notification.summary,
            "summary_head": notification.summary_head,
            "continuation_hint": notification.continuation_hint,
            "failure_kind": notification.failure_kind.as_ref().map(|kind| format!("{kind:?}")),
            "result_path": notification.result_path.as_ref().map(|path| path.to_string_lossy().to_string()),
            "outcome_path": notification.outcome_path.as_ref().map(|path| path.to_string_lossy().to_string()),
            "task_record_path": task_record_path.to_string_lossy(),
            "run_epoch": notification.run_epoch,
            "notified_epoch": run_epoch,
        }),
    );

    if !completion_notification_event_exists(store, session_id, task_id, run_epoch)? {
        event_writer(&completion_event)?;
    }

    task_record.notified_epoch = run_epoch as i64;
    let task_record_json = serde_json::to_string_pretty(&task_record)
        .context("failed to serialize Gear task record")?;
    store.write_worker_file(
        task_id,
        "task-record.json",
        &format!("{task_record_json}\n"),
    )?;
    Ok(())
}

fn completion_notification_event_exists(
    store: &StateStore,
    session_id: &str,
    task_id: &str,
    run_epoch: u64,
) -> Result<bool> {
    let events_path = store.events_path(session_id);
    if !events_path.exists() {
        return Ok(false);
    }

    let contents = std_fs::read_to_string(&events_path)
        .with_context(|| format!("failed to read {}", events_path.display()))?;
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(existing_event) = serde_json::from_str::<Event>(line) else {
            continue;
        };
        if existing_event.task_id.as_deref() != Some(task_id)
            || !matches!(existing_event.kind, EventKind::CompletionNotified)
        {
            continue;
        }
        if existing_event.data.get("run_epoch").and_then(Value::as_u64) == Some(run_epoch) {
            return Ok(true);
        }
    }
    Ok(false)
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
    plan_task_id: &str,
    verification_path: &std::path::Path,
    parent_task_id: Option<String>,
    worker_kind: WorkerKind,
    task_namespace: Option<&str>,
) -> String {
    let task_id = scoped_task_id(task_namespace, &repair_task_id(iteration));
    let plan_task = tasks
        .iter()
        .find(|task| {
            task.id == plan_task_id
                || task
                    .inputs
                    .plan_task
                    .as_ref()
                    .is_some_and(|plan_task| plan_task.task_id == plan_task_id)
        })
        .and_then(|task| task.inputs.plan_task.clone());
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
            plan_task,
            phase_route_locked: false,
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

fn review_task_id(iteration: usize, task_namespace: Option<&str>) -> String {
    scoped_task_id(task_namespace, &format!("task_review_{iteration:03}"))
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
    worker_kind: &str,
    task_namespace: Option<&str>,
) {
    let mut evidence = vec![review_path.to_string_lossy().to_string()];
    if let Some(repair_request_path) = repair_request_path {
        evidence.push(repair_request_path.to_string_lossy().to_string());
    }
    tasks.push(Task {
        id: review_task_id(iteration, task_namespace),
        goal_id: goal_id.to_string(),
        parent_task_id,
        title: format!("Review goal after iteration {iteration}"),
        kind: TaskKind::Review,
        status: TaskStatus::Pending,
        assigned_worker: Some(worker_kind.to_string()),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// Executor session or task that this reviewer actually inspected.
    pub reviewed_execution_id: String,
    /// The route/category of the reviewer (e.g. "deep", "explore", "comment_checker").
    pub route: String,
    /// Qualified reviewer model selected for this attempt, when one was configured.
    pub model: Option<String>,
    /// Path to the reviewer's output artifact.
    pub artifact_path: Option<String>,
    /// Verdict from this reviewer.
    pub verdict: String,
    /// Reviewer findings supporting this dimension verdict.
    pub findings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReviewReceiptPayload {
    schema_version: u32,
    reviewed_execution_id: String,
    dimensions: Vec<ReviewReceiptDimension>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReviewReceiptDimension {
    dimension: ReviewDimension,
    verdict: String,
    findings: Vec<String>,
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
    /// Verify that repeated dimensions referring to one reviewer execution use
    /// one coherent receipt. A single independent reviewer may issue multiple
    /// dimension verdicts, but one execution ID cannot name different routes
    /// or artifacts.
    pub fn validate_independent_reviewers(&self) -> Result<()> {
        let mut seen: Vec<(&str, &str, &str, Option<&str>, Option<&str>)> = Vec::new();
        for result in &self.results {
            if let Some(ref evidence) = result.reviewer_evidence {
                if evidence.execution_id.trim().is_empty()
                    || evidence.reviewed_execution_id.trim().is_empty()
                    || evidence.route.trim().is_empty()
                    || evidence.artifact_path.as_deref().is_none_or(str::is_empty)
                    || evidence
                        .findings
                        .iter()
                        .all(|finding| finding.trim().is_empty())
                    || !matches!(evidence.verdict.as_str(), "pass" | "fail")
                    || evidence.execution_id == evidence.reviewed_execution_id
                {
                    bail!(
                        "reviewer evidence for dimension {} has an incomplete receipt",
                        result.dimension.label()
                    );
                }
                if let Some((_, reviewed_execution_id, route, model, artifact_path)) = seen
                    .iter()
                    .find(|(execution_id, _, _, _, _)| *execution_id == evidence.execution_id)
                    && (*reviewed_execution_id != evidence.reviewed_execution_id
                        || *route != evidence.route
                        || *model != evidence.model.as_deref()
                        || *artifact_path != evidence.artifact_path.as_deref())
                {
                    bail!(
                        "reviewer execution_id '{}' refers to conflicting receipts",
                        evidence.execution_id
                    );
                }
                seen.push((
                    &evidence.execution_id,
                    &evidence.reviewed_execution_id,
                    &evidence.route,
                    evidence.model.as_deref(),
                    evidence.artifact_path.as_deref(),
                ));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn from_inputs(
        verification_passed: bool,
        worker_status: &WorkerStatus,
        scope_check: &crate::tools::ScopeCheck,
        coordinator_review: Option<&CoordinatorReview>,
        context_risk_signals: &[String],
        task_attempts: &[TaskAttempt],
    ) -> Self {
        Self::from_inputs_for_execution(
            verification_passed,
            worker_status,
            scope_check,
            coordinator_review,
            context_risk_signals,
            None,
            task_attempts,
        )
    }

    fn from_inputs_for_execution(
        verification_passed: bool,
        worker_status: &WorkerStatus,
        scope_check: &crate::tools::ScopeCheck,
        coordinator_review: Option<&CoordinatorReview>,
        context_risk_signals: &[String],
        expected_reviewed_execution_id: Option<&str>,
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
        let goal_verification_evidence = reviewer_evidence_from_attempt(
            ReviewDimension::GoalVerification,
            expected_reviewed_execution_id,
            task_attempts,
        );
        let code_quality_evidence = reviewer_evidence_from_attempt(
            ReviewDimension::CodeQuality,
            expected_reviewed_execution_id,
            task_attempts,
        );
        let security_evidence = reviewer_evidence_from_attempt(
            ReviewDimension::Security,
            expected_reviewed_execution_id,
            task_attempts,
        );
        let qa_execution_evidence = reviewer_evidence_from_attempt(
            ReviewDimension::QaExecution,
            expected_reviewed_execution_id,
            task_attempts,
        );
        let goal_verification_passed = verification_passed
            && goal_satisfied
            && reviewer_evidence_passed(goal_verification_evidence.as_ref());
        let code_quality_passed = scope_clean
            && comment_check_clean
            && reviewer_evidence_passed(code_quality_evidence.as_ref());
        let security_passed = scope_check.forbidden_touches.is_empty()
            && reviewer_evidence_passed(security_evidence.as_ref());
        let qa_execution_passed =
            verification_passed && reviewer_evidence_passed(qa_execution_evidence.as_ref());
        Self {
            require_all_pass: review_required,
            results: vec![
                ReviewDimensionResult {
                    dimension: ReviewDimension::GoalVerification,
                    passed: goal_verification_passed,
                    evidence: if goal_verification_passed {
                        "verification passed and coordinator accepted the goal".to_string()
                    } else {
                        "verification, coordinator acceptance, or typed reviewer verdict failed"
                            .to_string()
                    },
                    reviewer_evidence: goal_verification_evidence,
                },
                ReviewDimensionResult {
                    dimension: ReviewDimension::CodeQuality,
                    passed: code_quality_passed,
                    evidence: if code_quality_passed {
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
                    reviewer_evidence: code_quality_evidence,
                },
                ReviewDimensionResult {
                    dimension: ReviewDimension::Security,
                    passed: security_passed,
                    evidence: if security_passed {
                        "no forbidden paths were touched".to_string()
                    } else {
                        format!(
                            "forbidden paths touched: {}",
                            scope_check.forbidden_touches.join(", ")
                        )
                    },
                    reviewer_evidence: security_evidence,
                },
                ReviewDimensionResult {
                    dimension: ReviewDimension::QaExecution,
                    passed: qa_execution_passed,
                    evidence: if qa_execution_passed {
                        "verification commands passed".to_string()
                    } else {
                        "one or more verification commands failed".to_string()
                    },
                    reviewer_evidence: qa_execution_evidence,
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

    /// Returns `Some(reason)` when any required review dimension lacks a real,
    /// typed reviewer artifact.
    fn synthetic_evidence_only_reason(&self) -> Option<String> {
        if self.results.is_empty() {
            return None;
        }
        let missing_dimensions = self
            .results
            .iter()
            .filter(|result| result.reviewer_evidence.is_none())
            .map(|result| result.dimension.label())
            .collect::<Vec<_>>();
        (!missing_dimensions.is_empty()).then(|| {
            format!(
                "Missing typed reviewer evidence for: {}.",
                missing_dimensions.join(", ")
            )
        })
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

/// Build one receipt from a completed review-category worker attempt. Ordinary
/// executor attempts and review attempts without a session or artifact are not
/// reviewer evidence.
fn reviewer_evidence_from_attempt(
    dimension: ReviewDimension,
    expected_reviewed_execution_id: Option<&str>,
    attempts: &[TaskAttempt],
) -> Option<ReviewerEvidence> {
    let last_attempt = attempts.iter().rev().find(|attempt| {
        WorkerCategory::parse(&attempt.worker_category) == Some(WorkerCategory::Review)
            && attempt.status == TaskAttemptStatus::Completed
    })?;
    let artifact_path = last_attempt
        .result_path
        .clone()
        .or_else(|| last_attempt.outcome_path.clone());
    let artifact_path = artifact_path?;
    let reviewer_model = reviewer_model_from_attempt(last_attempt, &artifact_path);
    let (receipt_path, receipt) = load_review_receipt(&artifact_path)?;
    if receipt.schema_version != 1 || receipt.reviewed_execution_id.trim().is_empty() {
        return None;
    }
    if expected_reviewed_execution_id
        .is_some_and(|expected| expected != receipt.reviewed_execution_id)
    {
        return None;
    }
    if last_attempt.session_id.as_deref() == Some(receipt.reviewed_execution_id.as_str()) {
        return None;
    }
    let dimension_receipt = receipt
        .dimensions
        .into_iter()
        .find(|candidate| candidate.dimension == dimension)?;
    let verdict = dimension_receipt.verdict.trim().to_ascii_lowercase();
    if !matches!(verdict.as_str(), "pass" | "fail")
        || dimension_receipt
            .findings
            .iter()
            .all(|finding| finding.trim().is_empty())
    {
        return None;
    }
    let execution_id = last_attempt
        .session_id
        .clone()
        .unwrap_or_else(|| format!("command-artifact:{}", receipt_path.to_string_lossy()));
    Some(ReviewerEvidence {
        execution_id,
        reviewed_execution_id: receipt.reviewed_execution_id,
        route: if last_attempt.session_id.is_some() {
            last_attempt.worker_category.clone()
        } else {
            format!("{}:command_fallback", last_attempt.worker_category)
        },
        model: reviewer_model,
        artifact_path: Some(receipt_path.to_string_lossy().to_string()),
        verdict,
        findings: dimension_receipt.findings,
    })
}

fn reviewer_model_from_attempt(
    attempt: &TaskAttempt,
    artifact_path: &std::path::Path,
) -> Option<String> {
    attempt.worker_model.clone().or_else(|| {
        let selection_path = artifact_path.parent()?.join("model-selection.json");
        let selection: serde_json::Value =
            serde_json::from_str(&std_fs::read_to_string(selection_path).ok()?).ok()?;
        selection
            .get("applied_model")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string)
    })
}

fn load_review_receipt(path: &std::path::Path) -> Option<(PathBuf, ReviewReceiptPayload)> {
    let artifact = std_fs::read_to_string(path).ok()?;
    if let Some(receipt) = parse_review_receipt(&artifact) {
        return Some((path.to_path_buf(), receipt));
    }
    let worker_result: WorkerResult = serde_json::from_str(&artifact).ok()?;
    let receipt_path = worker_result
        .last_message_path
        .or(worker_result.stdout_path)?;
    let receipt = parse_review_receipt(&std_fs::read_to_string(&receipt_path).ok()?)?;
    Some((receipt_path, receipt))
}

fn parse_review_receipt(output: &str) -> Option<ReviewReceiptPayload> {
    let trimmed = output.trim();
    let json = if let Some(rest) = trimmed.strip_prefix("```json") {
        rest.strip_suffix("```").unwrap_or(rest).trim()
    } else if let Some(rest) = trimmed.strip_prefix("```") {
        rest.strip_suffix("```").unwrap_or(rest).trim()
    } else {
        trimmed
    };
    serde_json::from_str(json).ok()
}

fn reviewer_evidence_passed(receipt: Option<&ReviewerEvidence>) -> bool {
    receipt.is_some_and(|receipt| receipt.verdict == "pass")
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
        next_worker_is_premium: bool,
    ) -> Result<(), String> {
        if snapshot.worker_call_count >= self.max_worker_calls {
            return Err(format!(
                "worker_calls={}/{} ({})",
                snapshot.worker_call_count,
                budget_limit_label(self.max_worker_calls),
                route_change_type.label()
            ));
        }
        if next_worker_is_premium
            && snapshot.premium_worker_call_count >= self.max_premium_worker_calls
        {
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

            // Synthetic evidence gate — always check, not only when review is requested.
            if let Some(synthetic_reason) = self.review_gate.synthetic_evidence_only_reason() {
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

    if worker_result.status != WorkerStatus::Succeeded
        && let Some(partial_output_path) = worker_artifact_path(worker_result, "partial-output.md")
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

#[cfg(test)]
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
    task_attempts: &[TaskAttempt],
) -> GoalEvaluation {
    evaluate_goal_with_review_target(
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
        lineage,
        None,
        task_attempts,
    )
}

fn verify_broker_receipts_for_goal(
    broker: Option<&WorkerBroker>,
    broker_factory: Option<&PhaseBrokerFactory>,
    goal_id: &str,
    require_terminal_receipt: bool,
) -> Option<String> {
    if let Some(factory) = broker_factory {
        return factory
            .validate_goal_receipts(goal_id, require_terminal_receipt)
            .map_err(|error| format!("factory broker receipt validation failed: {error}"))
            .err();
    }
    let broker = broker?;
    let lifecycle = match broker.lifecycle_state() {
        Ok(state) => state,
        Err(e) => return Some(format!("broker state check failed: {e}")),
    };
    match lifecycle {
        LifecycleState::Terminal { ref outcome, .. } => {
            if *outcome == BrokerOutcome::Failed {
                return Some("broker session terminated with Failed outcome".to_string());
            }
            if *outcome == BrokerOutcome::Cancelled {
                return Some("broker session terminated with Cancelled outcome".to_string());
            }
        }
        LifecycleState::Active | LifecycleState::Resolved if !require_terminal_receipt => {}
        LifecycleState::Active | LifecycleState::Resolved => {
            return Some("broker session did not reach Terminal".to_string());
        }
        _ => return Some(format!("broker receipt state is {:?}", lifecycle.name())),
    }
    let session_dir = match broker.session_ledger_dir() {
        Ok(session_dir) => session_dir,
        Err(error) if require_terminal_receipt => {
            return Some(format!("broker session ledger is missing: {error}"));
        }
        Err(_) => return None,
    };
    crate::worker_broker::validate_session_ledger(&session_dir)
        .map_err(|error| format!("broker receipt validation failed: {error}"))
        .err()
}

fn evaluate_goal_with_review_target(
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
    expected_reviewed_execution_id: Option<&str>,
    task_attempts: &[TaskAttempt],
) -> GoalEvaluation {
    let review_gate = ReviewGate::from_inputs_for_execution(
        verification_passed,
        worker_status,
        scope_check,
        coordinator_review,
        &budget_snapshot.context_risk_signals,
        expected_reviewed_execution_id,
        task_attempts,
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

fn validate_final_review_input_artifacts(
    execution_id: &str,
    executor_result_path: Option<&Path>,
    executor_outcome_path: Option<&Path>,
    verification_path: Option<&Path>,
) -> Result<()> {
    if execution_id.trim().is_empty() {
        bail!("final review input is missing the executor execution id");
    }
    for (label, path) in [
        ("executor result", executor_result_path),
        ("executor outcome", executor_outcome_path),
        ("verification", verification_path),
    ] {
        let path =
            path.with_context(|| format!("final review input is missing {label} artifact"))?;
        let metadata = std_fs::metadata(path).with_context(|| {
            format!(
                "final review {label} artifact is missing: {}",
                path.display()
            )
        })?;
        if !metadata.is_file() {
            bail!(
                "final review {label} artifact must be a regular file: {}",
                path.display()
            );
        }
        if metadata.len() == 0 {
            bail!("final review {label} artifact is empty: {}", path.display());
        }
    }
    Ok(())
}

fn review_worker_request(base_request: &str, reviewed_execution_id: &str) -> String {
    let required_receipt = json!({
        "schema_version": 1,
        "reviewed_execution_id": reviewed_execution_id,
        "dimensions": [
            {
                "dimension": "goal_verification",
                "verdict": "pass|fail",
                "findings": ["replace with concrete evidence"]
            },
            {
                "dimension": "code_quality",
                "verdict": "pass|fail",
                "findings": ["replace with concrete evidence"]
            },
            {
                "dimension": "security",
                "verdict": "pass|fail",
                "findings": ["replace with concrete evidence"]
            },
            {
                "dimension": "qa_execution",
                "verdict": "pass|fail",
                "findings": ["replace with concrete evidence"]
            }
        ]
    });
    format!(
        "{base_request}\n\nThis is a read-only final-review phase. Return exactly one JSON object, without Markdown fences or prose. Bind it to reviewed_execution_id `{reviewed_execution_id}`. Include all four dimensions, use only `pass` or `fail`, replace every placeholder with concrete findings, and fail any dimension whose evidence is incomplete. Required shape:\n{}",
        required_receipt
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
    expected_reviewed_execution_id: Option<&str>,
    task_attempts: &[TaskAttempt],
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
    let review_gate = ReviewGate::from_inputs_for_execution(
        verification_results.iter().all(|result| result.success),
        &worker_result.status,
        scope_check,
        coordinator_review,
        no_progress_signals,
        expected_reviewed_execution_id,
        task_attempts,
    );
    let review_gate_dimensions = review_gate
        .results
        .iter()
        .map(|result| {
            let reviewer_receipt = result
                .reviewer_evidence
                .as_ref()
                .map(|evidence| {
                    format!(
                        "; reviewer_execution=`{}`; reviewed_execution=`{}`; route=`{}`; model=`{}`; artifact=`{}`; verdict=`{}`; findings={}",
                        evidence.execution_id,
                        evidence.reviewed_execution_id,
                        evidence.route,
                        evidence.model.as_deref().unwrap_or("unrecorded"),
                        evidence.artifact_path.as_deref().unwrap_or("unrecorded"),
                        evidence.verdict,
                        evidence.findings.join(" | ")
                    )
                })
                .unwrap_or_else(|| "; reviewer_receipt=`missing`".to_string());
            format!(
                "- {}: `{}` — {}{}",
                result.dimension.label(),
                if result.passed { "pass" } else { "fail" },
                result.evidence,
                reviewer_receipt
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
mod test_seams {
    use std::cell::RefCell;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use crate::state::ObjectiveGraph;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ObjectiveCrashPoint {
        BeforeOutcomeReceipt,
        AfterOutcomeReceiptBeforeGraph,
        AfterChildReservationBeforeEdge,
        AfterChildEdgeBeforeStarted,
        AfterChildOutcomeBeforeObjectiveSettled,
    }

    pub struct ObjectiveControllerTestSeam {
        pub on_goal_settled: Option<Arc<dyn Fn(&str, &str) + Send + Sync>>,
        pub on_goal_lease_released: Option<Arc<dyn Fn(&str, &str) + Send + Sync>>,
        pub on_objective_graph_commit: Option<Arc<dyn Fn(&str, &ObjectiveGraph) + Send + Sync>>,
        pub on_continue_event: Option<Arc<dyn Fn(&str, &str) + Send + Sync>>,
        pub on_child_attach: Option<Arc<dyn Fn(&str, &str) + Send + Sync>>,
        pub intercept_settled_to_graph_commit: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
        pub worker_dispatch_count: Arc<AtomicUsize>,
        pub crash_point: Option<ObjectiveCrashPoint>,
    }

    thread_local! {
        static SEAM: RefCell<Option<ObjectiveControllerTestSeam>> = RefCell::new(None);
    }

    pub fn with_seam<F, R>(f: F) -> R
    where
        F: FnOnce(&mut Option<ObjectiveControllerTestSeam>) -> R,
    {
        SEAM.with(|seam| {
            let mut borrow = seam.borrow_mut();
            f(&mut *borrow)
        })
    }

    pub fn reset() {
        with_seam(|seam| *seam = None);
    }

    pub fn install(seam: ObjectiveControllerTestSeam) {
        with_seam(|s| *s = Some(seam));
    }

    pub fn goal_settled(goal_id: &str, epoch_id: &str) {
        with_seam(|seam| {
            if let Some(seam) = seam.as_ref() {
                if let Some(cb) = seam.on_goal_settled.as_ref() {
                    cb(goal_id, epoch_id);
                }
            }
        });
    }

    pub fn goal_lease_released(goal_id: &str, epoch_id: &str) {
        with_seam(|seam| {
            if let Some(seam) = seam.as_ref() {
                if let Some(cb) = seam.on_goal_lease_released.as_ref() {
                    cb(goal_id, epoch_id);
                }
            }
        });
    }

    pub fn objective_graph_commit(objective_id: &str, graph: &ObjectiveGraph) {
        with_seam(|seam| {
            if let Some(seam) = seam.as_ref() {
                if let Some(cb) = seam.on_objective_graph_commit.as_ref() {
                    cb(objective_id, graph);
                }
            }
        });
    }

    pub fn continue_event(objective_id: &str, receipt_hash: &str) {
        with_seam(|seam| {
            if let Some(seam) = seam.as_ref() {
                if let Some(cb) = seam.on_continue_event.as_ref() {
                    cb(objective_id, receipt_hash);
                }
            }
        });
    }

    pub fn child_attach(objective_id: &str, child_goal_id: &str) {
        with_seam(|seam| {
            if let Some(seam) = seam.as_ref() {
                if let Some(cb) = seam.on_child_attach.as_ref() {
                    cb(objective_id, child_goal_id);
                }
            }
        });
    }

    pub fn should_intercept_settled_to_graph_commit() -> bool {
        with_seam(|seam| {
            if let Some(seam) = seam.as_ref() {
                if let Some(cb) = seam.intercept_settled_to_graph_commit.as_ref() {
                    return cb();
                }
            }
            false
        })
    }

    pub fn increment_worker_dispatch() {
        with_seam(|seam| {
            if let Some(seam) = seam.as_ref() {
                seam.worker_dispatch_count.fetch_add(1, Ordering::SeqCst);
            }
        });
    }

    pub fn should_crash_at(point: ObjectiveCrashPoint) -> bool {
        with_seam(|seam| {
            seam.as_ref()
                .and_then(|seam| seam.crash_point)
                .is_some_and(|crash_point| crash_point == point)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;

    use super::*;
    use crate::plan_review::{
        PlanCriticCheck, PlanCriticCheckVerdict, PlanCriticDimension, PlanCriticFinding,
        PlanCriticFindingSeverity,
    };
    use crate::test_support::test_support as ts;
    use crate::tools::ScopeCheck;
    use crate::workers::{WorkerKind, WorkerStatus};

    #[test]
    fn repository_observation_stale_capture_commit_is_rejected() -> Result<()> {
        let capture_commit = "a".repeat(40);
        let receipt = RepositoryObservationReceipt::seal_with_capture_commit(
            "planner",
            "goal-capture-gate",
            "plan-capture-gate",
            1,
            "plan-hash",
            "worker-capture-gate",
            "session-capture-gate",
            Some("transcript-hash".to_string()),
            1,
            vec!["src/lib.rs".to_string()],
            vec![crate::state::RepositoryObservationEvent {
                operation: "read".to_string(),
                path: "src/lib.rs".to_string(),
                event_id: "tool-capture-gate".to_string(),
                event_hash: "event-capture-gate".to_string(),
                observed_at: timestamp(),
            }],
            Some(capture_commit.clone()),
        )?;
        assert!(
            validate_repository_observation_capture_commit(
                "planner",
                &receipt,
                Some(capture_commit.clone()),
            )
            .is_ok()
        );
        let stale = validate_repository_observation_capture_commit(
            "planner",
            &receipt,
            Some("b".repeat(40)),
        )
        .expect_err("stale observation must be rejected");
        assert!(stale.to_string().contains("stale"));

        let legacy = RepositoryObservationReceipt::seal(
            "planner",
            "goal-capture-gate",
            "plan-capture-gate",
            1,
            "plan-hash",
            "worker-capture-gate",
            "session-capture-gate",
            Some("transcript-hash".to_string()),
            1,
            vec!["src/lib.rs".to_string()],
            receipt.observed_events.clone(),
        )?;
        let missing = validate_repository_observation_capture_commit(
            "planner",
            &legacy,
            Some(capture_commit),
        )
        .expect_err("Git evidence without capture commit must be rejected");
        assert!(missing.to_string().contains("missing capture commit"));
        assert!(validate_repository_observation_capture_commit("planner", &legacy, None).is_ok());
        let discovery_commit = "a".repeat(40);
        assert!(
            validate_discovery_capture_commit(Some(&discovery_commit), Some(&discovery_commit))
                .is_ok()
        );
        let discovery_stale =
            validate_discovery_capture_commit(Some(&discovery_commit), Some(&"b".repeat(40)))
                .expect_err("stale discovery must be rejected");
        assert!(discovery_stale.to_string().contains("stale"));
        assert!(validate_discovery_capture_commit(None, None).is_ok());
        Ok(())
    }

    fn test_budget(max_iterations: usize) -> BudgetController {
        BudgetController {
            max_iterations,
            max_files_changed: usize::MAX,
            ..BudgetController::default()
        }
    }

    fn planning_goal(draft: &PlanGraphDraft) -> Result<Goal> {
        Ok(Goal {
            id: "goal_plan_review".to_string(),
            title: "Review a plan".to_string(),
            status: GoalStatus::Planning,
            workspace: "/tmp".to_string(),
            created_at: timestamp(),
            updated_at: timestamp(),
            request: draft.objective.clone(),
            product_type: "test".to_string(),
            language_profile: "rust".to_string(),
            success_criteria: vec!["approved plan".to_string()],
            budget: Budget::default(),
            current_task_id: None,
            coordinator_model: Some(CoordinatorModel {
                provider_id: "test-provider".to_string(),
                model_id: "test-model".to_string(),
                name: "Test Model".to_string(),
            }),
            coordinator_brief: Some(serde_json::to_string(draft)?),
            summary: String::new(),
        })
    }

    fn phase_identity(label: &str) -> PhaseExecutionIdentity {
        PhaseExecutionIdentity {
            execution_id: format!("{label}_execution"),
            phase_session_id: format!("{label}_session"),
            backend: PhaseExecutionBackend::LanguageModelRequest,
            agent_id: Some("zed".to_string()),
            provider_id: Some("test-provider".to_string()),
            model_id: Some("test-model".to_string()),
            actual_session_id: None,
        }
    }

    fn plan_critic_checks(failed: Option<PlanCriticDimension>) -> Vec<PlanCriticCheck> {
        [
            PlanCriticDimension::References,
            PlanCriticDimension::Executability,
            PlanCriticDimension::Contradictions,
            PlanCriticDimension::Scope,
            PlanCriticDimension::Tdd,
            PlanCriticDimension::Qa,
            PlanCriticDimension::Acceptance,
        ]
        .into_iter()
        .map(|dimension| PlanCriticCheck {
            dimension,
            verdict: if failed == Some(dimension) {
                PlanCriticCheckVerdict::Fail
            } else {
                PlanCriticCheckVerdict::Pass
            },
            summary: format!("{dimension:?} checked"),
            evidence_refs: vec![format!("plan:{dimension:?}")],
        })
        .collect()
    }

    fn plan_critic_submission(
        input: &PlanCriticInput,
        execution_suffix: usize,
        decision: PlanCriticDecision,
    ) -> Result<PlanCriticSubmission> {
        let failed =
            (decision != PlanCriticDecision::Approve).then_some(PlanCriticDimension::Acceptance);
        let verdict = PlanCriticVerdict {
            schema_version: crate::plan_review::PLAN_REVIEW_SCHEMA_VERSION,
            reviewed_goal_id: input.plan.goal_id.clone(),
            reviewed_plan_id: input.plan.plan_id.clone(),
            reviewed_plan_revision: input.plan.revision,
            reviewed_plan_hash: input.plan.plan_hash.clone(),
            reviewed_planner_execution_id: input.planner_receipt.identity.execution_id.clone(),
            decision,
            checks: plan_critic_checks(failed),
            findings: failed
                .map(|dimension| {
                    vec![PlanCriticFinding {
                        dimension,
                        severity: PlanCriticFindingSeverity::Blocking,
                        code: "acceptance_not_decidable".to_string(),
                        task_id: input
                            .plan
                            .draft
                            .tasks
                            .first()
                            .map(|task| task.task_id.clone()),
                        path: None,
                        message: "Acceptance must be made more specific.".to_string(),
                        required_change: Some("Add a concrete acceptance observation.".to_string()),
                    }]
                })
                .unwrap_or_default(),
            revision_instructions: (decision == PlanCriticDecision::Revise)
                .then(|| "Make acceptance concrete and resubmit the full draft.".to_string()),
            needs_user_reason: (decision == PlanCriticDecision::Reject)
                .then(|| "The user must choose an acceptance target.".to_string()),
            summary: format!("critic decision: {decision:?}"),
        };
        let raw_output = serde_json::to_string(&verdict)?;
        Ok(PlanCriticSubmission {
            reviewer: phase_identity(&format!("critic_{execution_suffix}")),
            verdict,
            raw_output,
            artifact_path: None,
            repository_evidence_path: None,
        })
    }

    fn phase_runtime_for_test(critic_hook: Option<PlanCriticHook>) -> PhaseRuntime {
        let current_model = ModelSelectorId {
            agent_id: "zed".to_string(),
            provider_id: "test-provider".to_string(),
            model_id: "test-model".to_string(),
        };
        PhaseRuntime {
            routes: PhaseRouteTable::legacy_defaults(),
            inventory: LiveModelInventory {
                models: vec![current_model.clone()],
            },
            current_model: Some(current_model),
            planner: Some(phase_identity("planner")),
            intent_fold_hook: None,
            planner_hook: None,
            plan_critic_hook: critic_hook,
            oracle_hook: None,
            plan_revision_hook: None,
            strategist_next_goal_hook: None,
            require_plan_approval: true,
            max_plan_revisions: 2,
            broker: None,
            broker_factory: None,
            direct_model_usage_provider: None,
        }
    }

    fn objective_worker_for_test() -> WorkerConfig {
        let mut config = WorkerConfig::default();
        config.worker_kind = WorkerKind::Opencode;
        config.worker_command = Some(
            r###"sh -c 'task_id=$(grep -o "\"task_id\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" "$GEARBOX_WORKER_PACKET" | head -1 | cut -d "\"" -f4); reviewed_id=$(grep -o '"'"'reviewed_execution_id\\":\\"[^\\"]*'"'"' "$GEARBOX_WORKER_PACKET" | head -1 | sed '"'"'s/.*\\"//'"'"'); if [ -z "$reviewed_id" ]; then reviewed_id=$task_id; fi; printf "%s" "{\"schema_version\":1,\"reviewed_execution_id\":\"TASK_ID\",\"dimensions\":[{\"dimension\":\"goal_verification\",\"verdict\":\"pass\",\"findings\":[\"verification evidence inspected\"]},{\"dimension\":\"code_quality\",\"verdict\":\"pass\",\"findings\":[\"scope inspected\"]},{\"dimension\":\"security\",\"verdict\":\"pass\",\"findings\":[\"forbidden paths clean\"]},{\"dimension\":\"qa_execution\",\"verdict\":\"pass\",\"findings\":[\"verification passed\",\"EVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\"]}]}" | sed "s|TASK_ID|$reviewed_id|" > "$GEARBOX_WORKER_LAST_MESSAGE"'"###
                .to_string(),
        );
        config.skip_worker = false;
        config.require_worker = true;
        config
    }

    fn crash_matrix_phase_runtime() -> PhaseRuntime {
        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Approve));
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.planner_hook = Some(Arc::new(|input: PlannerInput| {
            let draft = deterministic_fallback_draft(
                &input.request,
                &input.scope,
                &input.verification_commands,
            );
            Ok(PlannerSubmission {
                raw_output: serde_json::to_string(&draft)?,
                draft,
                planner: phase_identity("crash_matrix_planner"),
                artifact_path: None,
                repository_evidence_path: None,
            })
        }));
        phase_runtime.strategist_next_goal_hook = Some({
            Arc::new(move |input: StrategistNextGoalInput| {
                let is_child = input.request.contains("Run the recovered child");
                let decision = if is_child {
                    StrategistNextGoalDecision::Complete
                } else {
                    StrategistNextGoalDecision::Continue
                };
                let verdict = StrategistNextGoalVerdict {
                    schema_version: 1,
                    goal_id: input.goal_id,
                    epoch_id: input.epoch_id,
                    reviewed_status: input.status,
                    decision,
                    next_objective: (!is_child).then(|| "Run the recovered child".to_string()),
                    acceptance_signals: (!is_child)
                        .then(|| vec!["The child survives restart".to_string()])
                        .unwrap_or_default(),
                    required_questions: Vec::new(),
                    evidence_refs: vec![input.final_report_path],
                    answerable_now: false,
                    rationale: "Crash matrix deterministic strategist".to_string(),
                };
                Ok(StrategistNextGoalSubmission {
                    raw_output: serde_json::to_string(&verdict)?,
                    verdict,
                    strategist: phase_identity(if is_child {
                        "crash_matrix_strategist_child"
                    } else {
                        "crash_matrix_strategist_parent"
                    }),
                    artifact_path: None,
                })
            }) as StrategistNextGoalHook
        });
        phase_runtime
    }

    #[test]
    fn strategist_continue_production_repro() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Approve));
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.planner_hook = Some(Arc::new(|input: PlannerInput| {
            let draft = deterministic_fallback_draft(
                &input.request,
                &input.scope,
                &input.verification_commands,
            );
            Ok(PlannerSubmission {
                raw_output: serde_json::to_string(&draft)?,
                draft,
                planner: phase_identity("repro_planner"),
                artifact_path: None,
                repository_evidence_path: None,
            })
        }));
        phase_runtime.strategist_next_goal_hook =
            Some(Arc::new(|input: StrategistNextGoalInput| {
                let verdict = StrategistNextGoalVerdict {
                    schema_version: 1,
                    goal_id: input.goal_id,
                    epoch_id: input.epoch_id,
                    reviewed_status: input.status,
                    decision: StrategistNextGoalDecision::Continue,
                    next_objective: Some("Create the successor objective".to_string()),
                    acceptance_signals: vec!["The successor has a durable edge".to_string()],
                    required_questions: Vec::new(),
                    evidence_refs: vec![input.final_report_path],
                    answerable_now: false,
                    rationale: "The first epoch passed and has a bounded successor".to_string(),
                };
                Ok(StrategistNextGoalSubmission {
                    raw_output: serde_json::to_string(&verdict)?,
                    verdict,
                    strategist: phase_identity("repro_strategist"),
                    artifact_path: None,
                })
            }));
        let outcome = Orchestrator::run_with_phase_runtime(
            RunOptions {
                request: "Reproduce a discarded Continue receipt".to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: vec!["echo verify-ok".to_string()],
                worker: objective_worker_for_test(),
                allowed_paths: Vec::new(),
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 10,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 2,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: 1,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some("repro-session".to_string()),
                continuation: true,
            },
            phase_runtime,
        )?;
        assert_eq!(outcome.status, GoalStatus::Complete);
        assert_eq!(
            outcome
                .strategist_receipt
                .as_ref()
                .map(|receipt| receipt.verdict.decision),
            Some(StrategistNextGoalDecision::Continue)
        );
        let store = StateStore::new(temp_dir.path());
        let epoch_events = store.read_goal_epoch_events(&outcome.goal_id)?;
        assert!(
            epoch_events
                .iter()
                .any(|event| event.kind == GoalEpochEventKind::NextGoalSelected)
        );
        assert_eq!(fs::read_dir(store.goals_dir())?.count(), 1);
        assert_eq!(fs::read_dir(store.objectives_dir())?.count(), 0);
        Ok(())
    }

    fn mock_task_attempt() -> Result<(tempfile::TempDir, TaskAttempt)> {
        let temp_dir = tempfile::tempdir()?;
        let receipt_path = temp_dir.path().join("review-receipt.json");
        let dimensions = [
            ReviewDimension::GoalVerification,
            ReviewDimension::CodeQuality,
            ReviewDimension::Security,
            ReviewDimension::QaExecution,
        ]
        .into_iter()
        .map(|dimension| ReviewReceiptDimension {
            dimension,
            verdict: "pass".to_string(),
            findings: vec![format!("{} evidence inspected", dimension.label())],
        })
        .collect();
        fs::write(
            &receipt_path,
            serde_json::to_vec_pretty(&ReviewReceiptPayload {
                schema_version: 1,
                reviewed_execution_id: "executor-task".to_string(),
                dimensions,
            })?,
        )?;
        let attempt = TaskAttempt {
            attempt: 1,
            worker_kind: "test-worker".to_string(),
            worker_command: None,
            worker_model: None,
            worker_category: "review".to_string(),
            route_hint: None,
            route_reason: "test".to_string(),
            status: crate::task_manager::TaskAttemptStatus::Completed,
            started_at: "2024-01-01T00:00:00Z".to_string(),
            finished_at: Some("2024-01-01T00:01:00Z".to_string()),
            session_id: Some("test-reviewer-session".to_string()),
            result_path: Some(receipt_path),
            outcome_path: None,
            summary: "Mock task attempt".to_string(),
            failure_kind: None,
            retry_reason: None,
            error: None,
        };
        Ok((temp_dir, attempt))
    }

    #[test]
    fn plan_rejects_before_any_worker_dispatch() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft(
            "Implement a reviewed change",
            &scope,
            &["echo verify".to_string()],
        );
        let mut goal = planning_goal(&draft)?;
        let rejected_plan = build_plan_graph(&goal, &scope, &["echo verify".to_string()])?;
        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Reject));
        let phase_runtime = phase_runtime_for_test(Some(critic_hook));
        store.write_phase_route_table(&goal.id, &phase_runtime.routes)?;

        let error = build_approved_plan_graph(
            &mut goal,
            &scope,
            &["echo verify".to_string()],
            temp_dir.path(),
            &store,
            "session-plan-review",
            &None,
            None,
            &phase_runtime,
        )
        .expect_err("reject verdict must stop before worker dispatch");

        assert!(
            error
                .to_string()
                .contains("plan rejected before worker dispatch")
        );
        assert_eq!(goal.status, GoalStatus::NeedsUser);
        let mut approval = store
            .read_plan_approval_state(&goal.id)?
            .context("approval state missing")?;
        assert_eq!(approval.status, PlanApprovalStatus::Rejected);
        approval.status = PlanApprovalStatus::Approved;
        approval.updated_at = timestamp();
        store.write_plan_approval_state(&approval)?;
        let error = store
            .write_plan_graph(&rejected_plan)
            .expect_err("a rejected critic receipt must not publish a canonical plan");
        assert!(format!("{error:#}").contains("requires an approving PlanCritic receipt"));
        assert_eq!(fs::read_dir(store.workers_dir())?.count(), 0);
        assert_eq!(fs::read_dir(store.plans_dir())?.count(), 0);
        Ok(())
    }

    #[test]
    fn planner_hook_builds_a_plan_from_an_opencode_worker_identity() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft(
            "Implement through an OpenCode planner",
            &scope,
            &["echo verify".to_string()],
        );
        let mut goal = planning_goal(&draft)?;
        goal.coordinator_brief = None;
        goal.coordinator_model = None;
        let submitted_draft = draft.clone();
        let planner_hook: PlannerHook = Arc::new(move |_input| {
            Ok(PlannerSubmission {
                raw_output: serde_json::to_string(&submitted_draft)?,
                draft: submitted_draft.clone(),
                planner: PhaseExecutionIdentity {
                    execution_id: "opencode_planner_execution".to_string(),
                    phase_session_id: "opencode_planner_phase".to_string(),
                    backend: PhaseExecutionBackend::WorkerSession,
                    agent_id: Some(WorkerKind::OpencodeSession.as_str().to_string()),
                    provider_id: Some("openai".to_string()),
                    model_id: Some("gpt-planner".to_string()),
                    actual_session_id: Some("opencode_planner_session".to_string()),
                },
                artifact_path: None,
                repository_evidence_path: None,
            })
        });
        let strategist_hook: StrategistNextGoalHook = Arc::new(|input| {
            let verdict = StrategistNextGoalVerdict {
                schema_version: 1,
                goal_id: input.goal_id,
                epoch_id: input.epoch_id,
                reviewed_status: input.status,
                decision: StrategistNextGoalDecision::Continue,
                next_objective: Some("Add persistence to the task tracker".to_string()),
                acceptance_signals: vec!["Tasks survive a restart".to_string()],
                required_questions: Vec::new(),
                evidence_refs: vec![input.final_report_path],
                answerable_now: false,
                rationale: "The first goal passed and the next bounded improvement is clear"
                    .to_string(),
            };
            Ok(StrategistNextGoalSubmission {
                raw_output: serde_json::to_string(&verdict)?,
                verdict,
                strategist: PhaseExecutionIdentity {
                    execution_id: "strategist_execution".to_string(),
                    phase_session_id: "strategist_phase".to_string(),
                    backend: PhaseExecutionBackend::WorkerSession,
                    agent_id: Some(WorkerKind::OpencodeSession.as_str().to_string()),
                    provider_id: Some("openai".to_string()),
                    model_id: Some("gpt-planner".to_string()),
                    actual_session_id: Some("strategist_session".to_string()),
                },
                artifact_path: None,
            })
        });
        let phase_runtime = PhaseRuntime {
            routes: PhaseRouteTable::opencode_only(crate::phase_routing::OpenCodeModelProfiles {
                planner: "openai/gpt-planner".to_string(),
                executor: "deepseek/flash".to_string(),
                reviewer: "openai/gpt-reviewer".to_string(),
            })?,
            inventory: LiveModelInventory::default(),
            current_model: None,
            planner: None,
            intent_fold_hook: None,
            planner_hook: Some(planner_hook),
            plan_critic_hook: None,
            oracle_hook: None,
            plan_revision_hook: None,
            strategist_next_goal_hook: Some(strategist_hook),
            require_plan_approval: false,
            max_plan_revisions: 2,
            broker: None,
            broker_factory: None,
            direct_model_usage_provider: None,
        };
        let epoch_id = "epoch-planner-test";
        let lease = store.acquire_goal_run_lease(
            &goal.id,
            epoch_id,
            "session-opencode-planner",
            Duration::from_secs(60),
        )?;
        store.append_goal_epoch_event(
            &goal.id,
            epoch_id,
            &format!("{epoch_id}.started"),
            GoalEpochEventKind::Started,
            json!({ "session_id": "session-opencode-planner" }),
        )?;

        let plan = build_approved_plan_graph_with_budget(
            &mut goal,
            &scope,
            &["echo verify".to_string()],
            temp_dir.path(),
            &store,
            "session-opencode-planner",
            &None,
            None,
            &phase_runtime,
            &lease,
            epoch_id,
        )?;

        assert_eq!(plan.draft, draft);
        assert_eq!(plan.source, PlanSource::PlannerModel);
        assert_eq!(
            plan.planner
                .as_ref()
                .and_then(|receipt| receipt.session_id.as_deref()),
            Some("opencode_planner_session")
        );
        let budget_ledger = store.read_goal_budget_ledger(&goal.id)?;
        assert_eq!(budget_ledger.reservations.len(), 1);
        assert_eq!(budget_ledger.reservations[0].phase, "planner");
        assert!(!budget_ledger.reservations[0].worker_call);
        assert_eq!(
            budget_ledger.reservations[0].status,
            crate::state::BudgetReservationStatus::Settled
        );
        assert!(goal.coordinator_brief.is_some());
        goal.status = GoalStatus::Complete;
        goal.summary = "Initial objective complete".to_string();
        let final_report_path = store.write_artifact(&goal.id, "final-report.md", "complete\n")?;
        let strategist = run_strategist_next_goal(
            &mut goal,
            epoch_id,
            &plan,
            &final_report_path,
            &store,
            "session-opencode-planner",
            &None,
            &phase_runtime,
            &lease,
            &["opencode_planner_session".to_string()],
        )?
        .context("strategist receipt should be produced")?;
        assert_eq!(
            strategist.verdict.decision,
            StrategistNextGoalDecision::Continue
        );
        assert!(
            store
                .artifact_dir(&goal.id)
                .join("strategist-next-goal-receipt.json")
                .is_file()
        );
        assert_eq!(
            store.read_goal_budget_ledger(&goal.id)?.reservations.len(),
            2
        );
        lease.release()?;
        Ok(())
    }

    #[test]
    fn strategist_answerable_stop_requires_evidence() -> Result<()> {
        let mut verdict = StrategistNextGoalVerdict {
            schema_version: 1,
            goal_id: "goal-answerable".to_string(),
            epoch_id: "epoch-answerable".to_string(),
            reviewed_status: GoalStatus::Running,
            decision: StrategistNextGoalDecision::Stop,
            next_objective: None,
            acceptance_signals: Vec::new(),
            required_questions: Vec::new(),
            evidence_refs: Vec::new(),
            answerable_now: true,
            rationale: "The result should be answerable".to_string(),
        };
        let missing_evidence = verdict
            .validate("goal-answerable", "epoch-answerable", &GoalStatus::Running)
            .expect_err("answerable stop without evidence must be rejected");
        assert!(missing_evidence.to_string().contains("evidence reference"));

        verdict.evidence_refs = vec!["artifacts/final-report.md".to_string()];
        verdict
            .validate("goal-answerable", "epoch-answerable", &GoalStatus::Running)
            .expect("answerable stop with evidence should be accepted");

        verdict.decision = StrategistNextGoalDecision::Continue;
        verdict.next_objective = Some("Continue only if evidence is insufficient".to_string());
        verdict.acceptance_signals = vec!["A bounded follow-up is required".to_string()];
        let non_terminal = verdict
            .validate("goal-answerable", "epoch-answerable", &GoalStatus::Running)
            .expect_err("answerable continue must be rejected");
        assert!(non_terminal.to_string().contains("terminal"));
        Ok(())
    }

    #[test]
    fn objective_preplanning_error_settles_persisted_state() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let request = "Create one bounded artifact";
        let session_id = "objective-preplanning-failure-session";
        let mut phase_runtime = PhaseRuntime::legacy();
        phase_runtime.intent_fold_hook = Some(Arc::new(|_| {
            bail!("injected IntentFold failure before planning")
        }));

        let error = Orchestrator::run_objective_with_phase_runtime(
            RunOptions {
                request: request.to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: vec!["echo verify-ok".to_string()],
                worker: objective_worker_for_test(),
                allowed_paths: Vec::new(),
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 10,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 1,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: 1,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some(session_id.to_string()),
                continuation: true,
            },
            phase_runtime,
            ObjectivePolicy::default(),
        )
        .expect_err("the injected pre-planning failure must be returned");
        assert!(!format!("{error:#}").trim().is_empty());

        let objective_id = objective_id_for(session_id, temp_dir.path(), request)?;
        let goal_id = format!("goal_{objective_id}_000");
        let store = StateStore::new(temp_dir.path());
        let graph = store
            .read_objective_graph(&objective_id)?
            .context("objective graph should remain auditable after failure")?;
        assert_eq!(graph.status, ObjectiveStatus::Failed);
        assert!(graph.active_goal_id.is_none());
        assert_eq!(graph.nodes[0].status, GoalStatus::Failed);

        let goal: Goal = serde_json::from_str(&fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/goals")
                .join(format!("{goal_id}.json")),
        )?)?;
        assert_eq!(goal.status, GoalStatus::Failed);
        assert!(
            store
                .read_objective_events(&objective_id)?
                .iter()
                .any(|event| event.kind == ObjectiveEventKind::Failed)
        );
        Ok(())
    }

    #[test]
    fn rolling_objective_continue_creates_one_child() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let strategist_calls = Arc::new(AtomicUsize::new(0));
        let critic_calls = Arc::new(AtomicUsize::new(0));
        let critic_hook: PlanCriticHook = {
            let critic_calls = critic_calls.clone();
            Arc::new(move |input: PlanCriticInput| {
                let call = critic_calls.fetch_add(1, Ordering::SeqCst) + 1;
                plan_critic_submission(&input, call, PlanCriticDecision::Approve)
            })
        };
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        let planner_calls = Arc::new(AtomicUsize::new(0));
        phase_runtime.planner_hook = Some({
            let planner_calls = planner_calls.clone();
            Arc::new(move |input: PlannerInput| {
                let call = planner_calls.fetch_add(1, Ordering::SeqCst) + 1;
                let draft = deterministic_fallback_draft(
                    &input.request,
                    &input.scope,
                    &input.verification_commands,
                );
                Ok(PlannerSubmission {
                    raw_output: serde_json::to_string(&draft)?,
                    draft,
                    planner: phase_identity(&format!("planner_{call}")),
                    artifact_path: None,
                    repository_evidence_path: None,
                })
            })
        });
        phase_runtime.strategist_next_goal_hook = Some({
            let strategist_calls = strategist_calls.clone();
            Arc::new(move |input: StrategistNextGoalInput| {
                let call = strategist_calls.fetch_add(1, Ordering::SeqCst) + 1;
                let decision = if call == 1 {
                    StrategistNextGoalDecision::Continue
                } else {
                    StrategistNextGoalDecision::Complete
                };
                let (next_objective, acceptance_signals) = if call == 1 {
                    (
                        Some("Add restart persistence".to_string()),
                        vec!["The state survives a process restart".to_string()],
                    )
                } else {
                    (None, Vec::new())
                };
                let verdict = StrategistNextGoalVerdict {
                    schema_version: 1,
                    goal_id: input.goal_id,
                    epoch_id: input.epoch_id,
                    reviewed_status: input.status,
                    decision,
                    next_objective,
                    acceptance_signals,
                    required_questions: Vec::new(),
                    evidence_refs: vec![input.final_report_path],
                    answerable_now: false,
                    rationale: if call == 1 {
                        "The next bounded objective is ready".to_string()
                    } else {
                        "The objective is complete".to_string()
                    },
                };
                Ok(StrategistNextGoalSubmission {
                    raw_output: serde_json::to_string(&verdict)?,
                    verdict,
                    strategist: phase_identity(&format!("strategist_{call}")),
                    artifact_path: None,
                })
            }) as StrategistNextGoalHook
        });
        let outcome = Orchestrator::run_objective_with_phase_runtime(
            RunOptions {
                request: "Build a restart-safe task tracker".to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: vec!["echo verify-ok".to_string()],
                worker: objective_worker_for_test(),
                allowed_paths: Vec::new(),
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 10,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 2,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: 1,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some("objective-root-session".to_string()),
                continuation: true,
            },
            phase_runtime,
            ObjectivePolicy {
                auto_continue: true,
                max_epochs: 2,
                ..ObjectivePolicy::default()
            },
        )?;

        assert_eq!(outcome.status, ObjectiveStatus::Complete);
        assert_eq!(outcome.goal_outcomes.len(), 2);
        assert_ne!(
            outcome.goal_outcomes[0].goal_id,
            outcome.goal_outcomes[1].goal_id
        );
        assert_ne!(
            outcome.goal_outcomes[0].session_id,
            outcome.goal_outcomes[1].session_id
        );
        let store = StateStore::new(temp_dir.path());
        let graph: ObjectiveGraph =
            serde_json::from_str(&fs::read_to_string(&outcome.graph_path)?)?;
        graph.validate()?;
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(
            graph.nodes[1].parent_goal_id.as_deref(),
            Some(graph.nodes[0].goal_id.as_str())
        );
        assert_eq!(
            store
                .read_objective_events(&outcome.objective_id)?
                .iter()
                .filter(|event| event.kind == ObjectiveEventKind::GoalAttached)
                .count(),
            2
        );
        assert_eq!(strategist_calls.load(Ordering::SeqCst), 2);
        let objective_id = objective_id_for(
            "objective-root-session",
            temp_dir.path(),
            "Build a restart-safe task tracker",
        )?;
        let objective_policy = ObjectivePolicy {
            auto_continue: true,
            max_epochs: 2,
            ..ObjectivePolicy::default()
        };
        let objective_ledger =
            store.read_objective_budget_ledger(&objective_id, &objective_policy.hash()?)?;
        assert_eq!(objective_ledger.reservations.len(), 2);
        assert!(objective_ledger.reservations.iter().all(|reservation| {
            reservation.status == crate::state::ObjectiveBudgetReservationStatus::Settled
        }));
        Ok(())
    }

    #[test]
    fn objective_controller_stops_answerable_result_before_child() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let strategist_calls = Arc::new(AtomicUsize::new(0));
        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Approve));
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.planner_hook = Some(Arc::new(|input: PlannerInput| {
            let draft = deterministic_fallback_draft(
                &input.request,
                &input.scope,
                &input.verification_commands,
            );
            Ok(PlannerSubmission {
                raw_output: serde_json::to_string(&draft)?,
                draft,
                planner: phase_identity("answerable_planner"),
                artifact_path: None,
                repository_evidence_path: None,
            })
        }));
        phase_runtime.strategist_next_goal_hook = Some({
            let strategist_calls = strategist_calls.clone();
            Arc::new(move |input: StrategistNextGoalInput| {
                strategist_calls.fetch_add(1, Ordering::SeqCst);
                let verdict = StrategistNextGoalVerdict {
                    schema_version: 1,
                    goal_id: input.goal_id,
                    epoch_id: input.epoch_id,
                    reviewed_status: input.status,
                    decision: StrategistNextGoalDecision::Stop,
                    next_objective: None,
                    acceptance_signals: Vec::new(),
                    required_questions: Vec::new(),
                    evidence_refs: vec![input.final_report_path],
                    answerable_now: true,
                    rationale: "The current evidence answers the core request".to_string(),
                };
                Ok(StrategistNextGoalSubmission {
                    raw_output: serde_json::to_string(&verdict)?,
                    verdict,
                    strategist: phase_identity("answerable_strategist"),
                    artifact_path: None,
                })
            })
        });

        let outcome = Orchestrator::run_objective_with_phase_runtime(
            RunOptions {
                request: "Answer the completed task without another epoch".to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: vec!["echo verify-ok".to_string()],
                worker: objective_worker_for_test(),
                allowed_paths: Vec::new(),
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 10,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 2,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: 1,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some("answerable-root-session".to_string()),
                continuation: true,
            },
            phase_runtime,
            ObjectivePolicy {
                auto_continue: true,
                max_epochs: 3,
                ..ObjectivePolicy::default()
            },
        )?;

        assert_eq!(outcome.status, ObjectiveStatus::Stopped);
        assert_eq!(outcome.goal_outcomes.len(), 1);
        assert_eq!(strategist_calls.load(Ordering::SeqCst), 1);
        let answerable = outcome.goal_outcomes[0]
            .strategist_receipt
            .as_ref()
            .is_some_and(|receipt| receipt.verdict.answerable_now);
        assert!(answerable);

        let store = StateStore::new(temp_dir.path());
        let graph: ObjectiveGraph =
            serde_json::from_str(&fs::read_to_string(&outcome.graph_path)?)?;
        graph.validate()?;
        assert_eq!(graph.nodes.len(), 1);
        assert_eq!(
            store
                .read_objective_events(&outcome.objective_id)?
                .iter()
                .filter(|event| event.kind == ObjectiveEventKind::GoalAttached)
                .count(),
            1
        );
        let epoch_events = store.read_goal_epoch_events(&outcome.goal_outcomes[0].goal_id)?;
        assert!(epoch_events.iter().any(|event| {
            event.kind == GoalEpochEventKind::NextGoalSelected
                && event
                    .payload
                    .get("answerable_now")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
        }));
        Ok(())
    }

    #[test]
    fn objective_controller_stops_before_child_when_epoch_limit_is_reached() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Approve));
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.planner_hook = Some(Arc::new(|input: PlannerInput| {
            let draft = deterministic_fallback_draft(
                &input.request,
                &input.scope,
                &input.verification_commands,
            );
            Ok(PlannerSubmission {
                raw_output: serde_json::to_string(&draft)?,
                draft,
                planner: phase_identity("limited_planner"),
                artifact_path: None,
                repository_evidence_path: None,
            })
        }));
        phase_runtime.strategist_next_goal_hook =
            Some(Arc::new(|input: StrategistNextGoalInput| {
                let verdict = StrategistNextGoalVerdict {
                    schema_version: 1,
                    goal_id: input.goal_id,
                    epoch_id: input.epoch_id,
                    reviewed_status: input.status,
                    decision: StrategistNextGoalDecision::Continue,
                    next_objective: Some("Repeat the same bounded task".to_string()),
                    acceptance_signals: vec!["A stable observation exists".to_string()],
                    required_questions: Vec::new(),
                    evidence_refs: vec![input.final_report_path],
                    answerable_now: false,
                    rationale: "Continue is intentionally blocked by the epoch policy".to_string(),
                };
                Ok(StrategistNextGoalSubmission {
                    raw_output: serde_json::to_string(&verdict)?,
                    verdict,
                    strategist: phase_identity("limited_strategist"),
                    artifact_path: None,
                })
            }));
        let outcome = Orchestrator::run_objective_with_phase_runtime(
            RunOptions {
                request: "Build a bounded artifact".to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: vec!["echo verify-ok".to_string()],
                worker: objective_worker_for_test(),
                allowed_paths: Vec::new(),
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 10,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 2,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: 1,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some("objective-limit-session".to_string()),
                continuation: true,
            },
            phase_runtime,
            ObjectivePolicy {
                auto_continue: true,
                max_epochs: 1,
                ..ObjectivePolicy::default()
            },
        )?;
        assert_eq!(outcome.status, ObjectiveStatus::Limited);
        assert_eq!(outcome.goal_outcomes.len(), 1);
        let graph: ObjectiveGraph =
            serde_json::from_str(&fs::read_to_string(&outcome.graph_path)?)?;
        assert_eq!(graph.nodes.len(), 1);
        assert!(graph.active_goal_id.is_none());
        Ok(())
    }

    #[test]
    fn objective_resume_reconciles_child_after_continue_event() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let objective_id = "objective-recovery";
        let root_session_id = "recovery-root";
        let mut graph = ObjectiveGraph::new(
            objective_id,
            root_session_id,
            &temp_dir.path().to_string_lossy(),
            "Build a recoverable artifact",
            "scope-hash",
            ObjectivePolicy::rolling_default(),
        )?;
        let root = objective_goal_node(
            "goal-recovery-000",
            "epoch-recovery-000",
            root_session_id,
            "Build a recoverable artifact",
            Vec::new(),
            None,
            None,
            None,
            GoalStatus::Planning,
            None,
            hash_text("build a recoverable artifact"),
        )?;
        graph.add_root_node(root)?;
        store.write_objective_graph(&graph)?;
        store.append_objective_event(
            objective_id,
            "objective.started",
            ObjectiveEventKind::Started,
            json!({ "root_session_id": root_session_id }),
        )?;
        store.append_objective_event(
            objective_id,
            "goal-attached:goal-recovery-000",
            ObjectiveEventKind::GoalAttached,
            json!({ "goal_id": "goal-recovery-000", "epoch_id": "epoch-recovery-000" }),
        )?;
        graph.update_active_node(
            "goal-recovery-000",
            GoalStatus::Complete,
            Some("final-wave-recovery".to_string()),
            Some("/tmp/recovery-final-report.md".to_string()),
            Some("strategist-recovery".to_string()),
            Some("complete".to_string()),
        )?;
        store.write_objective_graph(&graph)?;
        store.append_objective_event(
            objective_id,
            "continue:strategist-recovery",
            ObjectiveEventKind::StrategistContinueAccepted,
            json!({
                "parent_goal_id": "goal-recovery-000",
                "parent_epoch_id": "epoch-recovery-000",
                "receipt_hash": "strategist-recovery",
                "next_objective": "Persist the recovered artifact",
                "acceptance_signals": ["The artifact is present after restart"],
            }),
        )?;

        let objective_lease = store.acquire_objective_lease(
            objective_id,
            root_session_id,
            Duration::from_secs(60),
        )?;
        reconcile_objective_frontier(
            &store,
            objective_id,
            root_session_id,
            &mut graph,
            Some(&objective_lease),
            &ObjectivePolicy::rolling_default(),
            &Budget::default(),
        )?;
        objective_lease.release()?;
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(
            graph.active_goal_id.as_deref(),
            Some("goal_objective-recovery_001")
        );
        assert_eq!(
            graph.nodes[1].parent_strategist_receipt_hash.as_deref(),
            Some("strategist-recovery")
        );
        assert_eq!(
            store
                .read_objective_events(objective_id)?
                .iter()
                .filter(|event| event.kind == ObjectiveEventKind::GoalAttached)
                .count(),
            2
        );
        Ok(())
    }

    #[test]
    fn objective_stop_prevents_dispatch_before_frontier_creation() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        store.write_continuation_state(
            "objective-stop-session",
            "stopped-goal",
            ContinuationStatus::Stopped,
        )?;
        let error = Orchestrator::run_objective_with_phase_runtime(
            RunOptions {
                request: "Do not dispatch this stopped objective".to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: Vec::new(),
                worker: WorkerConfig::default(),
                allowed_paths: Vec::new(),
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 1,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 1,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: 1,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some("objective-stop-session".to_string()),
                continuation: true,
            },
            PhaseRuntime::legacy(),
            ObjectivePolicy::default(),
        )
        .expect_err("a stopped objective must not dispatch a goal");
        assert!(error.to_string().contains("continuation is stopped"));
        Ok(())
    }

    #[test]
    fn intent_fold_stops_before_planning_when_user_input_is_required() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft("Choose a deployment target", &scope, &[]);
        let mut goal = planning_goal(&draft)?;
        goal.coordinator_brief = None;
        goal.coordinator_model = None;
        let planner_called = Arc::new(Mutex::new(false));
        let planner_hook: PlannerHook = Arc::new({
            let planner_called = planner_called.clone();
            move |_input| {
                *planner_called.lock().expect("planner flag poisoned") = true;
                anyhow::bail!("planner must not run")
            }
        });
        let intent_fold_hook: IntentFoldHook = Arc::new(|input| {
            let verdict = IntentFoldVerdict {
                schema_version: crate::plan_review::PLAN_REVIEW_SCHEMA_VERSION,
                goal_id: input.goal_id,
                normalized_objective: "Deploy the application".to_string(),
                assumptions: Vec::new(),
                constraints: vec!["Do not choose a provider without user input".to_string()],
                ambiguities: vec!["Target provider is unknown".to_string()],
                required_questions: vec!["Which deployment provider should be used?".to_string()],
                risks: vec![crate::plan_review::IntentRisk {
                    code: "provider_choice".to_string(),
                    severity: crate::plan_review::IntentRiskSeverity::High,
                    description: "Provider choice changes the implementation".to_string(),
                    mitigation: "Ask the user before planning".to_string(),
                }],
                acceptance_signals: vec!["The selected provider is recorded".to_string()],
                decision: IntentFoldDecision::NeedsUser,
                summary: "Deployment provider requires a user decision".to_string(),
            };
            Ok(IntentFoldSubmission {
                raw_output: serde_json::to_string(&verdict)?,
                verdict,
                analyst: PhaseExecutionIdentity {
                    execution_id: "intent_fold_execution".to_string(),
                    phase_session_id: "intent_fold_phase".to_string(),
                    backend: PhaseExecutionBackend::WorkerSession,
                    agent_id: Some(WorkerKind::OpencodeSession.as_str().to_string()),
                    provider_id: Some("openai".to_string()),
                    model_id: Some("gpt-planner".to_string()),
                    actual_session_id: Some("intent_fold_session".to_string()),
                },
                artifact_path: None,
                repository_evidence_path: None,
                repository_discovery: None,
            })
        });
        let phase_runtime = PhaseRuntime {
            routes: PhaseRouteTable::opencode_only(crate::phase_routing::OpenCodeModelProfiles {
                planner: "openai/gpt-planner".to_string(),
                executor: "deepseek/flash".to_string(),
                reviewer: "openai/gpt-reviewer".to_string(),
            })?,
            inventory: LiveModelInventory::default(),
            current_model: None,
            planner: None,
            intent_fold_hook: Some(intent_fold_hook),
            planner_hook: Some(planner_hook),
            plan_critic_hook: None,
            oracle_hook: None,
            plan_revision_hook: None,
            strategist_next_goal_hook: None,
            require_plan_approval: false,
            max_plan_revisions: 2,
            broker: None,
            broker_factory: None,
            direct_model_usage_provider: None,
        };

        let error = build_approved_plan_graph(
            &mut goal,
            &scope,
            &[],
            temp_dir.path(),
            &store,
            "session-intent-fold",
            &None,
            None,
            &phase_runtime,
        )
        .expect_err("IntentFold must stop before planning");

        assert!(error.to_string().contains("requires user input"));
        assert_eq!(goal.status, GoalStatus::NeedsUser);
        assert!(!*planner_called.lock().expect("planner flag poisoned"));
        assert!(
            store
                .artifact_dir(&goal.id)
                .join("intent-fold-receipt.json")
                .is_file()
        );
        Ok(())
    }

    #[test]
    fn broker_backed_planner_requires_repository_discovery_artifact() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft("Require discovery", &scope, &[]);
        let mut goal = planning_goal(&draft)?;
        let planner_called = Arc::new(Mutex::new(false));
        let planner_hook: PlannerHook = Arc::new({
            let planner_called = planner_called.clone();
            move |_input| {
                *planner_called.lock().expect("planner flag poisoned") = true;
                anyhow::bail!("planner must not run without discovery")
            }
        });
        let intent_fold_hook: IntentFoldHook = Arc::new(|input| {
            let verdict = IntentFoldVerdict {
                schema_version: crate::plan_review::PLAN_REVIEW_SCHEMA_VERSION,
                goal_id: input.goal_id,
                normalized_objective: "Require discovery before planning".to_string(),
                assumptions: Vec::new(),
                constraints: vec!["Read-only discovery must precede planning".to_string()],
                ambiguities: Vec::new(),
                required_questions: Vec::new(),
                risks: Vec::new(),
                acceptance_signals: vec!["Discovery artifact exists".to_string()],
                decision: IntentFoldDecision::Ready,
                summary: "Ready after discovery requirement".to_string(),
            };
            Ok(IntentFoldSubmission {
                raw_output: serde_json::to_string(&verdict)?,
                verdict,
                analyst: phase_identity("intent_fold_without_discovery"),
                artifact_path: None,
                repository_evidence_path: None,
                repository_discovery: None,
            })
        });
        let broker_factory = Arc::new(crate::worker_broker::PhaseBrokerFactory::new(
            Arc::new(crate::workers::WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        ));
        let phase_runtime = PhaseRuntime {
            routes: PhaseRouteTable::opencode_only(crate::phase_routing::OpenCodeModelProfiles {
                planner: "openai/gpt-planner".to_string(),
                executor: "deepseek/flash".to_string(),
                reviewer: "openai/gpt-reviewer".to_string(),
            })?,
            inventory: LiveModelInventory::default(),
            current_model: None,
            planner: None,
            intent_fold_hook: Some(intent_fold_hook),
            planner_hook: Some(planner_hook),
            plan_critic_hook: None,
            oracle_hook: None,
            plan_revision_hook: None,
            strategist_next_goal_hook: None,
            require_plan_approval: false,
            max_plan_revisions: DEFAULT_MAX_PLAN_REVISIONS,
            broker: None,
            broker_factory: Some(broker_factory),
            direct_model_usage_provider: None,
        };

        let error = build_approved_plan_graph(
            &mut goal,
            &scope,
            &[],
            temp_dir.path(),
            &store,
            "session-discovery-gate",
            &None,
            None,
            &phase_runtime,
        )
        .expect_err("broker-backed planner must require discovery evidence");
        assert!(error.to_string().contains("without repository discovery"));
        assert!(!*planner_called.lock().expect("planner flag poisoned"));
        Ok(())
    }

    #[test]
    fn plan_approval_is_hash_bound_before_dispatch() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft(
            "Implement an approved change",
            &scope,
            &["echo verify".to_string()],
        );
        let mut goal = planning_goal(&draft)?;
        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Approve));
        let phase_runtime = phase_runtime_for_test(Some(critic_hook));
        store.write_phase_route_table(&goal.id, &phase_runtime.routes)?;
        let plan = build_approved_plan_graph(
            &mut goal,
            &scope,
            &["echo verify".to_string()],
            temp_dir.path(),
            &store,
            "session-plan-review",
            &None,
            None,
            &phase_runtime,
        )?;

        let approval = store
            .read_plan_approval_state(&goal.id)?
            .context("approval state missing")?;
        assert_eq!(approval.status, PlanApprovalStatus::Approved);
        assert_eq!(approval.plan_hash, plan.plan_hash);
        assert_eq!(approval.plan_id, plan.plan_id);
        assert!(approval.critic_receipt_hash.is_some());
        store.write_plan_graph(&plan)?;
        assert_eq!(store.read_plan_graph(&goal.id)?, Some(plan));
        fs::write(
            store
                .plan_review_dir(&goal.id)
                .join("revision-001-critic-output.txt"),
            "tampered critic output",
        )?;
        assert!(store.read_plan_graph(&goal.id).is_err());
        assert_eq!(fs::read_dir(store.workers_dir())?.count(), 0);
        Ok(())
    }

    #[test]
    fn single_task_plan_runs_independent_oracle_when_configured() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft(
            "Require an independent review for one task",
            &scope,
            &["echo verify".to_string()],
        );
        assert_eq!(draft.tasks.len(), 1);
        let mut goal = planning_goal(&draft)?;
        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Approve));
        let oracle_calls = Arc::new(AtomicUsize::new(0));
        let oracle_hook: PlanCriticHook = {
            let oracle_calls = oracle_calls.clone();
            Arc::new(move |input| {
                oracle_calls.fetch_add(1, Ordering::SeqCst);
                plan_critic_submission(&input, 2, PlanCriticDecision::Approve)
            })
        };
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.oracle_hook = Some(oracle_hook);
        store.write_phase_route_table(&goal.id, &phase_runtime.routes)?;
        let plan = build_approved_plan_graph(
            &mut goal,
            &scope,
            &["echo verify".to_string()],
            temp_dir.path(),
            &store,
            "session-single-task-dual-review",
            &None,
            None,
            &phase_runtime,
        )?;
        let approval = store
            .read_plan_approval_state(&goal.id)?
            .context("approval state missing")?;
        assert_eq!(approval.status, PlanApprovalStatus::Approved);
        assert!(approval.critic_receipt_hash.is_some());
        assert!(approval.secondary_critic_receipt_hash.is_some());
        assert_eq!(oracle_calls.load(Ordering::SeqCst), 1);
        assert_eq!(plan.draft.tasks.len(), 1);
        let bundle = store
            .read_review_epoch_bundle(&goal.id, plan.revision)?
            .context("review epoch bundle missing")?;
        assert!(bundle.complete);
        assert_eq!(
            bundle
                .roles
                .iter()
                .map(|role| role.role.as_str())
                .collect::<Vec<_>>(),
            vec!["momus", "oracle", "planner"]
        );
        Ok(())
    }

    #[test]
    fn plan_revision_requires_a_fresh_critic_receipt() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft(
            "Implement a revised change",
            &scope,
            &["echo verify".to_string()],
        );
        let mut goal = planning_goal(&draft)?;
        let critic_calls = Arc::new(AtomicUsize::new(0));
        let critic_hook: PlanCriticHook = {
            let critic_calls = critic_calls.clone();
            Arc::new(move |input| {
                let call = critic_calls.fetch_add(1, Ordering::SeqCst) + 1;
                plan_critic_submission(
                    &input,
                    call,
                    if call == 1 {
                        PlanCriticDecision::Revise
                    } else {
                        PlanCriticDecision::Approve
                    },
                )
            })
        };
        let revision_hook: PlanRevisionHook = Arc::new(|input| {
            let mut draft = input.plan.draft;
            draft
                .final_acceptance
                .push("The revised acceptance observation is recorded.".to_string());
            let raw_output = serde_json::to_string(&draft)?;
            Ok(PlanRevisionSubmission {
                draft,
                planner: phase_identity("planner_revision"),
                raw_output,
                artifact_path: None,
            })
        });
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.plan_revision_hook = Some(revision_hook);
        store.write_phase_route_table(&goal.id, &phase_runtime.routes)?;

        let plan = build_approved_plan_graph(
            &mut goal,
            &scope,
            &["echo verify".to_string()],
            temp_dir.path(),
            &store,
            "session-plan-review",
            &None,
            None,
            &phase_runtime,
        )?;

        assert_eq!(plan.revision, 2);
        assert_eq!(critic_calls.load(Ordering::SeqCst), 2);
        let approval = store
            .read_plan_approval_state(&goal.id)?
            .context("approval state missing")?;
        assert_eq!(approval.status, PlanApprovalStatus::Approved);
        assert_eq!(approval.plan_hash, plan.plan_hash);
        assert_eq!(approval.revisions_used, 1);
        store.write_plan_graph(&plan)?;
        let critic_receipts = fs::read_dir(store.plan_review_dir(&goal.id))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with("critic-receipt.json")
            })
            .count();
        assert_eq!(critic_receipts, 2);
        Ok(())
    }

    #[test]
    fn plan_revision_rejects_a_non_adjacent_critic_identity_replay() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft(
            "Implement two reviewed revisions",
            &scope,
            &["echo verify".to_string()],
        );
        let mut goal = planning_goal(&draft)?;
        let critic_calls = Arc::new(AtomicUsize::new(0));
        let critic_hook: PlanCriticHook = {
            let critic_calls = critic_calls.clone();
            Arc::new(move |input| {
                let call = critic_calls.fetch_add(1, Ordering::SeqCst) + 1;
                plan_critic_submission(
                    &input,
                    if call == 3 { 1 } else { call },
                    if call < 3 {
                        PlanCriticDecision::Revise
                    } else {
                        PlanCriticDecision::Approve
                    },
                )
            })
        };
        let revision_hook: PlanRevisionHook = {
            let revision_calls = Arc::new(AtomicUsize::new(0));
            Arc::new(move |input| {
                let call = revision_calls.fetch_add(1, Ordering::SeqCst) + 1;
                let mut draft = input.plan.draft;
                draft
                    .final_acceptance
                    .push(format!("Revision {call} acceptance evidence is recorded."));
                let raw_output = serde_json::to_string(&draft)?;
                Ok(PlanRevisionSubmission {
                    draft,
                    planner: phase_identity(&format!("planner_revision_{call}")),
                    raw_output,
                    artifact_path: None,
                })
            })
        };
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.plan_revision_hook = Some(revision_hook);
        store.write_phase_route_table(&goal.id, &phase_runtime.routes)?;

        let error = build_approved_plan_graph(
            &mut goal,
            &scope,
            &["echo verify".to_string()],
            temp_dir.path(),
            &store,
            "session-plan-review",
            &None,
            None,
            &phase_runtime,
        )
        .expect_err("a later revision must not reuse an earlier critic identity");

        assert!(
            error
                .to_string()
                .contains("fresh PlanCritic execution identity")
        );
        assert_eq!(critic_calls.load(Ordering::SeqCst), 3);
        assert_eq!(fs::read_dir(store.workers_dir())?.count(), 0);
        Ok(())
    }

    #[test]
    fn plan_revision_rejects_a_non_adjacent_planner_identity_replay() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft(
            "Implement two planner revisions",
            &scope,
            &["echo verify".to_string()],
        );
        let mut goal = planning_goal(&draft)?;
        let critic_calls = Arc::new(AtomicUsize::new(0));
        let critic_hook: PlanCriticHook = {
            let critic_calls = critic_calls.clone();
            Arc::new(move |input| {
                let call = critic_calls.fetch_add(1, Ordering::SeqCst) + 1;
                plan_critic_submission(&input, call, PlanCriticDecision::Revise)
            })
        };
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        let initial_planner = phase_runtime
            .planner
            .clone()
            .context("test phase runtime is missing its planner identity")?;
        let revision_hook: PlanRevisionHook = {
            let revision_calls = Arc::new(AtomicUsize::new(0));
            Arc::new(move |input| {
                let call = revision_calls.fetch_add(1, Ordering::SeqCst) + 1;
                let mut draft = input.plan.draft;
                draft
                    .final_acceptance
                    .push(format!("Planner revision {call} evidence is recorded."));
                let raw_output = serde_json::to_string(&draft)?;
                Ok(PlanRevisionSubmission {
                    draft,
                    planner: if call == 2 {
                        initial_planner.clone()
                    } else {
                        phase_identity("planner_revision_1")
                    },
                    raw_output,
                    artifact_path: None,
                })
            })
        };
        phase_runtime.plan_revision_hook = Some(revision_hook);
        store.write_phase_route_table(&goal.id, &phase_runtime.routes)?;

        let error = build_approved_plan_graph(
            &mut goal,
            &scope,
            &["echo verify".to_string()],
            temp_dir.path(),
            &store,
            "session-plan-review",
            &None,
            None,
            &phase_runtime,
        )
        .expect_err("a later revision must not reuse an earlier planner identity");

        assert!(
            error
                .to_string()
                .contains("globally fresh execution identity")
        );
        assert_eq!(critic_calls.load(Ordering::SeqCst), 2);
        assert_eq!(fs::read_dir(store.workers_dir())?.count(), 0);
        Ok(())
    }

    #[test]
    fn plan_revision_rejects_an_unchanged_plan() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft(
            "Implement a revised change",
            &scope,
            &["echo verify".to_string()],
        );
        let mut goal = planning_goal(&draft)?;
        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Revise));
        let revision_hook: PlanRevisionHook = Arc::new(|input| {
            let draft = input.plan.draft;
            let raw_output = serde_json::to_string(&draft)?;
            Ok(PlanRevisionSubmission {
                draft,
                planner: phase_identity("planner_revision"),
                raw_output,
                artifact_path: None,
            })
        });
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.plan_revision_hook = Some(revision_hook);
        store.write_phase_route_table(&goal.id, &phase_runtime.routes)?;

        let error = build_approved_plan_graph(
            &mut goal,
            &scope,
            &["echo verify".to_string()],
            temp_dir.path(),
            &store,
            "session-plan-review",
            &None,
            None,
            &phase_runtime,
        )
        .expect_err("a revision must not replay the same plan content");

        assert!(
            error
                .to_string()
                .contains("must change the sealed PlanGraph content hash")
        );
        assert_eq!(fs::read_dir(store.workers_dir())?.count(), 0);
        Ok(())
    }

    #[test]
    fn missing_plan_critic_hook_fails_closed() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft("Implement change", &scope, &[]);
        let mut goal = planning_goal(&draft)?;
        let phase_runtime = phase_runtime_for_test(None);
        let error = build_approved_plan_graph(
            &mut goal,
            &scope,
            &[],
            temp_dir.path(),
            &store,
            "session-plan-review",
            &None,
            None,
            &phase_runtime,
        )
        .expect_err("missing critic hook must fail closed");
        assert!(error.to_string().contains("no PlanCritic hook"));
        assert_eq!(fs::read_dir(store.workers_dir())?.count(), 0);
        Ok(())
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
        let planner_draft = deterministic_fallback_draft(
            "Build a tiny task tracker",
            &Scope::new(
                vec!["src".to_string(), "README.md".to_string()],
                vec![".git".to_string()],
                10,
            ),
            &["echo verify-ok".to_string()],
        );

        let options = RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: Some(
                    "sh -c 'cat <<\"EOF\" > \"$GEARBOX_WORKER_LAST_MESSAGE\"\n{\"schema_version\":1,\"reviewed_execution_id\":\"task_003\",\"dimensions\":[{\"dimension\":\"goal_verification\",\"verdict\":\"pass\",\"findings\":[\"requested behavior and verification evidence inspected\"]},{\"dimension\":\"code_quality\",\"verdict\":\"pass\",\"findings\":[\"implementation scope and worker artifacts inspected\"]},{\"dimension\":\"security\",\"verdict\":\"pass\",\"findings\":[\"forbidden path report is clean\"]},{\"dimension\":\"qa_execution\",\"verdict\":\"pass\",\"findings\":[\"verification command passed\"]}]}\nEOF'"
                        .to_string(),
                ),
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
            },
            allowed_paths: vec!["src".to_string(), "README.md".to_string()],
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            budget: None,
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
            coordinator_brief: Some(serde_json::to_string(&planner_draft)?),
            coordinator_review_hook: None,
            task_manager_control: None,
            task_manager: None,
            session_id: Some("acp-session-1".to_string()),
            continuation: true,
        };
        let outcome = Orchestrator::run(options.clone())?;

        assert_eq!(
            outcome.status,
            GoalStatus::Complete,
            "{}",
            fs::read_to_string(&outcome.final_report_path)?
        );
        let state_store = StateStore::new(temp_dir.path());
        let continuation_state = state_store
            .read_continuation_state_for_session("acp-session-1")?
            .context("continuation state should use the caller session id")?;
        assert_eq!(continuation_state.goal_id, outcome.goal_id);
        assert_eq!(continuation_state.status, ContinuationStatus::Completed);
        let epoch_events = state_store.read_goal_epoch_events(&outcome.goal_id)?;
        assert_eq!(
            epoch_events.first().map(|event| &event.kind),
            Some(&GoalEpochEventKind::Started)
        );
        assert!(epoch_events.iter().any(|event| {
            event.kind == GoalEpochEventKind::PhaseCompleted
                && event.payload.get("phase") == Some(&json!("worker"))
        }));
        assert!(epoch_events.iter().any(|event| {
            event.kind == GoalEpochEventKind::PhaseCompleted
                && event.payload.get("phase") == Some(&json!("plan_wave_scheduled"))
        }));
        assert!(epoch_events.iter().any(|event| {
            event.kind == GoalEpochEventKind::PhaseCompleted
                && event.payload.get("phase") == Some(&json!("review"))
        }));
        assert!(
            epoch_events
                .iter()
                .any(|event| event.kind == GoalEpochEventKind::BudgetReserved)
        );
        assert!(
            epoch_events
                .iter()
                .any(|event| event.kind == GoalEpochEventKind::BudgetSettled)
        );
        assert_eq!(
            epoch_events.last().map(|event| &event.kind),
            Some(&GoalEpochEventKind::Settled)
        );
        let budget_ledger = state_store.read_goal_budget_ledger(&outcome.goal_id)?;
        assert!(!budget_ledger.reservations.is_empty());
        assert!(budget_ledger.reservations.iter().all(|reservation| {
            reservation.status == crate::state::BudgetReservationStatus::Settled
        }));
        let task_route_receipt = state_store
            .read_task_route_decision_receipt(&outcome.goal_id, &outcome.epoch_id, "task_003", 1)?
            .context("worker task route decision receipt should be persisted")?;
        assert_eq!(task_route_receipt.task_id, "task_003");
        let expected_reservation = format!("{}.worker.1", outcome.epoch_id);
        assert_eq!(
            task_route_receipt.budget_reservation_id.as_deref(),
            Some(expected_reservation.as_str())
        );
        if let Some(session_binding) = state_store.read_plan_node_session_binding(
            &outcome.goal_id,
            &outcome.epoch_id,
            "task_003",
            1,
        )? {
            assert_eq!(
                session_binding.route_receipt_hash.as_deref(),
                Some(task_route_receipt.receipt_hash.as_str())
            );
        }
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
        assert!(goal.contains("Build a tiny task tracker"));
        let packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent")
                .join("workers")
                .join("task_003")
                .join("packet.json"),
        )?;
        assert!(packet.contains("\"model_id\": \"gpt-4.1\""));
        assert!(packet.contains("\"plan_task\""));
        assert!(packet.contains("\"completion_predicates\""));
        let final_report = fs::read_to_string(&outcome.final_report_path)?;
        assert!(final_report.contains("GPT-4.1 (openai/gpt-4.1)"));
        assert!(final_report.contains("Structured PlanGraph draft"));
        assert!(final_report.contains("## Evidence Chain"));
        assert!(final_report.contains("worker_outcome"));
        assert!(final_report.contains("verification.md"));
        assert!(final_report.contains("spec.md"));
        assert!(final_report.contains("plan.md"));
        let verification = fs::read_to_string(outcome.artifacts_root.join("verification.md"))?;
        assert!(verification.contains("verify-ok"));
        let blocked_marker = temp_dir.path().join("budget-worker-must-not-run");
        let mut blocked_options = options;
        blocked_options.session_id = Some("budget-blocked-session".to_string());
        blocked_options.continuation = false;
        blocked_options.budget = Some(Budget {
            max_tokens_per_call: 100,
            max_tokens_per_epoch: 99,
            ..Budget::default()
        });
        blocked_options.worker.worker_command =
            Some(format!("touch {}", blocked_marker.to_string_lossy()));
        let budget_error = Orchestrator::run(blocked_options)
            .expect_err("token reservation must block before worker launch");
        assert!(budget_error.to_string().contains("token budget"));
        assert!(!blocked_marker.exists());
        let store = StateStore::new(temp_dir.path());
        let mut persisted_route_receipt = None;
        for entry in fs::read_dir(store.phase_routes_dir(&outcome.goal_id))? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if !file_name.ends_with("-receipt.json") {
                continue;
            }
            let receipt: crate::phase_routing::PhaseRouteReceipt =
                serde_json::from_str(&fs::read_to_string(entry.path())?)?;
            if receipt.task_record_path.is_some() {
                persisted_route_receipt = Some((entry.path(), file_name, receipt));
                break;
            }
        }
        let (receipt_path, receipt_file_name, route_receipt) = persisted_route_receipt
            .context("worker phase route receipt should have task-record evidence")?;
        let ordinal = receipt_file_name
            .split('-')
            .next()
            .context("phase route receipt name is missing an ordinal")?
            .parse::<usize>()?;
        assert_eq!(
            store.read_phase_route_receipt(
                &outcome.goal_id,
                ordinal,
                &route_receipt.decision.phase,
            )?,
            Some(route_receipt.clone())
        );
        let original_route_receipt = fs::read(&receipt_path)?;
        let mut hash_tampered_receipt = route_receipt.clone();
        hash_tampered_receipt.plan_hash = Some("f".repeat(64));
        fs::write(
            &receipt_path,
            serde_json::to_vec_pretty(&hash_tampered_receipt)?,
        )?;
        let error = store
            .read_phase_route_receipt(&outcome.goal_id, ordinal, &route_receipt.decision.phase)
            .expect_err("hash-only route receipt tampering must be rejected");
        assert!(format!("{error:#}").contains("integrity hash mismatch"));
        fs::write(&receipt_path, original_route_receipt)?;

        let replay_ordinal = ordinal + 1_000;
        let replay_path = store.phase_routes_dir(&outcome.goal_id).join(format!(
            "{replay_ordinal:03}-{:?}-receipt.json",
            route_receipt.decision.phase
        ));
        fs::copy(&receipt_path, replay_path)?;
        let error = store
            .read_phase_route_receipt(
                &outcome.goal_id,
                replay_ordinal,
                &route_receipt.decision.phase,
            )
            .expect_err("an old route receipt must not replay under a new ordinal");
        assert!(error.to_string().contains("path identity"));

        let task_record_path = std::path::PathBuf::from(
            route_receipt
                .task_record_path
                .as_deref()
                .context("worker phase route receipt is missing its evidence path")?,
        );
        let original_task_record = fs::read(&task_record_path)?;
        let mut escaped_receipt = route_receipt.clone();
        escaped_receipt.task_record_path = Some(
            task_record_path
                .parent()
                .context("task-record evidence is missing its parent")?
                .join("..")
                .join("worker-evidence")
                .join(
                    task_record_path
                        .file_name()
                        .context("task-record evidence is missing its file name")?,
                )
                .to_string_lossy()
                .to_string(),
        );
        escaped_receipt.receipt_hash.clear();
        let escaped_receipt = escaped_receipt.seal()?;
        let error = store
            .validate_phase_route_receipt_evidence(&escaped_receipt)
            .expect_err("lexically escaped task-record evidence must be rejected");
        assert!(
            error
                .to_string()
                .contains("does not match its task identity")
        );

        fs::write(&task_record_path, "{}")?;
        let error = store
            .read_phase_route_receipt(&outcome.goal_id, ordinal, &route_receipt.decision.phase)
            .expect_err("tampered task-record evidence must invalidate the route receipt");
        assert!(error.to_string().contains("evidence hash mismatch"));
        fs::write(&task_record_path, &original_task_record)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let outside_task_record = temp_dir.path().join("outside-task-record.json");
            fs::write(&outside_task_record, &original_task_record)?;
            fs::remove_file(&task_record_path)?;
            symlink(&outside_task_record, &task_record_path)?;
            let error = store
                .read_phase_route_receipt(&outcome.goal_id, ordinal, &route_receipt.decision.phase)
                .expect_err("symlinked task-record evidence must stay inside its goal route");
            assert!(
                error
                    .to_string()
                    .contains("outside its goal route directory")
            );
        }
        let events = events.lock().expect("events mutex poisoned");
        assert!(events.iter().any(|event| event == "Spec artifact created"));
        assert!(events.iter().any(|event| event == "Verification passed"));
        assert!(
            events
                .iter()
                .any(|event| event.contains("Goal completed after 2 Gear iteration(s)"))
        );
        Ok(())
    }

    #[test]
    fn parallel_plan_wave_dispatches_disjoint_workers_before_barrier_reduction() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let scope = Scope::new(
            vec!["a.txt".to_string(), "b.txt".to_string()],
            vec![".git".to_string()],
            2,
        );
        let mut draft = deterministic_fallback_draft(
            "Run two independent workers",
            &scope,
            &["true".to_string()],
        );
        draft.tasks[0].task_id = "task_a".to_string();
        draft.tasks[0].scope.allowed_files = vec!["a.txt".to_string()];
        draft.tasks[0].scope.write_scope = vec!["a.txt".to_string()];
        draft.tasks[0].scope.max_files_changed = 1;
        draft.tasks[0].parallel_wave = 0;
        let mut second = draft.tasks[0].clone();
        second.task_id = "task_b".to_string();
        second.scope.allowed_files = vec!["b.txt".to_string()];
        second.scope.write_scope = vec!["b.txt".to_string()];
        draft.tasks.push(second);

        let critic_count = Arc::new(AtomicUsize::new(0));
        let critic_hook: PlanCriticHook = {
            let critic_count = critic_count.clone();
            Arc::new(move |input| {
                let suffix = critic_count.fetch_add(1, Ordering::SeqCst) + 1;
                plan_critic_submission(&input, suffix, PlanCriticDecision::Approve)
            })
        };
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        for profile in &mut phase_runtime.routes.profiles {
            if matches!(
                profile.phase,
                PhaseProfile::ExecutorQuick
                    | PhaseProfile::ExecutorDeep
                    | PhaseProfile::ReviewerFinal
            ) {
                profile.candidates = vec![crate::phase_routing::PhaseRouteCandidate {
                    backend: PhaseBackend::Worker(WorkerKind::OpencodeSession),
                    model: PhaseModelBinding::BackendDeclared("opencode/test-model".to_string()),
                    command: None,
                }];
                profile.source = crate::phase_routing::PhaseRouteSource::BuiltIn;
            }
        }
        let planner_draft = draft.clone();
        phase_runtime.planner_hook = Some(Arc::new(move |_input: PlannerInput| {
            Ok(PlannerSubmission {
                raw_output: serde_json::to_string(&planner_draft)?,
                draft: planner_draft.clone(),
                planner: phase_identity("parallel_planner"),
                artifact_path: None,
                repository_evidence_path: None,
            })
        }));

        let mut worker = objective_worker_for_test();
        worker.worker_kind = WorkerKind::OpencodeSession;
        worker.worker_model = Some("opencode/test-model".to_string());
        let worker_command = worker
            .worker_command
            .take()
            .context("test worker command should be configured")?;
        let worker_command = worker_command.replacen(
            "sh -c '",
            "sh -c 'mkdir -p .gearbox-agent/evidence; printf verified > .gearbox-agent/evidence/receipt.md; sleep 0.3; ",
            1,
        );
        worker.worker_command = Some(worker_command);
        worker.max_parallel_workers = 2;
        worker.max_parallel_per_key = 2;
        let outcome = Orchestrator::run_with_phase_runtime(
            RunOptions {
                request: "Run two independent workers".to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: vec!["true".to_string()],
                worker,
                allowed_paths: vec!["a.txt".to_string(), "b.txt".to_string()],
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 2,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 12,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
                budget: None,
                coordinator_model: Some(CoordinatorModel {
                    provider_id: "test-provider".to_string(),
                    model_id: "test-model".to_string(),
                    name: "Test Model".to_string(),
                }),
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some("parallel-wave-session".to_string()),
                continuation: false,
            },
            phase_runtime,
        )?;
        assert_eq!(outcome.status, GoalStatus::Complete);

        let store = StateStore::new(temp_dir.path());
        let wave_path = fs::read_dir(store.plan_wave_runs_dir())?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                fs::read_to_string(path)
                    .ok()
                    .and_then(|contents| serde_json::from_str::<PlanWaveRunLedger>(&contents).ok())
                    .is_some_and(|wave| wave.nodes.len() == 2)
            })
            .context("two-node wave ledger was not persisted")?;
        let wave: PlanWaveRunLedger = serde_json::from_str(&fs::read_to_string(wave_path)?)?;
        let starts = wave
            .nodes
            .iter()
            .map(|node| {
                DateTime::parse_from_rfc3339(
                    node.worker_started_at
                        .as_deref()
                        .context("wave node is missing worker start")?,
                )
                .map_err(anyhow::Error::from)
            })
            .collect::<Result<Vec<_>>>()?;
        let terminals = wave
            .nodes
            .iter()
            .map(|node| {
                DateTime::parse_from_rfc3339(
                    node.terminal_at
                        .as_deref()
                        .context("wave node is missing terminal timestamp")?,
                )
                .map_err(anyhow::Error::from)
            })
            .collect::<Result<Vec<_>>>()?;
        let overlap_start = starts.iter().max().context("missing wave start")?;
        let overlap_end = terminals.iter().min().context("missing wave terminal")?;
        assert!(
            overlap_start < overlap_end,
            "wave workers did not overlap: starts={starts:?} terminals={terminals:?}"
        );
        assert_eq!(wave.status, crate::state::PlanWaveStatus::Completed);
        Ok(())
    }

    #[test]
    fn evaluation_mentions_non_required_worker_failure_when_verification_passes() -> Result<()> {
        let scope_check = crate::tools::ScopeCheck::default();
        let (_receipt_dir, review_attempt) = mock_task_attempt()?;
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
            &[review_attempt],
        );

        assert_eq!(evaluation.status, GoalStatus::Complete);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("verification passed"));
        assert!(evaluation.summary.contains("worker status was failed"));
        Ok(())
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
    fn review_dimensions_share_one_real_reviewer_receipt() -> Result<()> {
        let scope_check = crate::tools::ScopeCheck::default();
        let (_receipt_dir, review_attempt) = mock_task_attempt()?;
        let gate = ReviewGate::from_inputs(
            true,
            &WorkerStatus::Succeeded,
            &scope_check,
            None,
            &[],
            &[review_attempt],
        );
        assert!(gate.validate_independent_reviewers().is_ok());
        let execution_ids = gate
            .results
            .iter()
            .filter_map(|result| {
                result
                    .reviewer_evidence
                    .as_ref()
                    .map(|evidence| evidence.execution_id.as_str())
            })
            .collect::<Vec<_>>();
        assert_eq!(execution_ids, vec!["test-reviewer-session"; 4]);
        Ok(())
    }

    #[test]
    fn review_receipt_must_bind_to_the_expected_executor() -> Result<()> {
        let scope_check = crate::tools::ScopeCheck::default();
        let (_receipt_dir, review_attempt) = mock_task_attempt()?;
        let gate = ReviewGate::from_inputs_for_execution(
            true,
            &WorkerStatus::Succeeded,
            &scope_check,
            None,
            &[],
            Some("different-executor-task"),
            &[review_attempt],
        );

        assert!(
            gate.results
                .iter()
                .all(|result| result.reviewer_evidence.is_none())
        );
        assert!(gate.failed_reason().is_some());
        Ok(())
    }

    #[test]
    fn final_review_input_preflight_requires_non_empty_executor_evidence() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let result_path = temp_dir.path().join("result.json");
        let outcome_path = temp_dir.path().join("outcome.json");
        let verification_path = temp_dir.path().join("verification.md");
        std_fs::write(&result_path, "result\n")?;
        std_fs::write(&outcome_path, "outcome\n")?;
        std_fs::write(&verification_path, "verification\n")?;

        assert!(
            validate_final_review_input_artifacts(
                "executor-1",
                Some(&result_path),
                Some(&outcome_path),
                Some(&verification_path),
            )
            .is_ok()
        );

        std_fs::remove_file(&outcome_path)?;
        let missing = validate_final_review_input_artifacts(
            "executor-1",
            Some(&result_path),
            Some(&outcome_path),
            Some(&verification_path),
        )
        .expect_err("missing outcome must block final reviewer admission");
        assert!(missing.to_string().contains("outcome"));

        std_fs::write(&outcome_path, "")?;
        let empty = validate_final_review_input_artifacts(
            "executor-1",
            Some(&result_path),
            Some(&outcome_path),
            Some(&verification_path),
        )
        .expect_err("empty outcome must block final reviewer admission");
        assert!(empty.to_string().contains("empty"));

        std_fs::remove_file(&outcome_path)?;
        std_fs::create_dir(&outcome_path)?;
        let directory = validate_final_review_input_artifacts(
            "executor-1",
            Some(&result_path),
            Some(&outcome_path),
            Some(&verification_path),
        )
        .expect_err("directory outcome must block final reviewer admission");
        assert!(directory.to_string().contains("regular file"));
        Ok(())
    }

    #[test]
    fn read_only_review_detects_workspace_mutation() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: " M src/lib.rs".to_string(),
            changed_files: vec!["src/lib.rs".to_string()],
            diff_hash: Some("before".to_string()),
        };
        let after = DiffSnapshot {
            diff_hash: Some("after".to_string()),
            ..before.clone()
        };

        assert!(review_changed_workspace(Some("review"), &before, &after));
        assert!(!review_changed_workspace(Some("deep"), &before, &after));
        assert!(!review_changed_workspace(Some("review"), &before, &before));
    }

    #[test]
    fn reviewer_cannot_approve_its_own_execution() -> Result<()> {
        let scope_check = crate::tools::ScopeCheck::default();
        let (_receipt_dir, mut review_attempt) = mock_task_attempt()?;
        review_attempt.session_id = Some("executor-task".to_string());
        let gate = ReviewGate::from_inputs_for_execution(
            true,
            &WorkerStatus::Succeeded,
            &scope_check,
            None,
            &[],
            Some("executor-task"),
            &[review_attempt],
        );

        assert!(
            gate.results
                .iter()
                .all(|result| result.reviewer_evidence.is_none())
        );
        Ok(())
    }

    #[test]
    fn reviewer_execution_id_rejects_conflicting_receipts() {
        let gate = ReviewGate {
            require_all_pass: true,
            results: vec![
                ReviewDimensionResult {
                    dimension: ReviewDimension::GoalVerification,
                    passed: true,
                    evidence: "test".to_string(),
                    reviewer_evidence: Some(ReviewerEvidence {
                        execution_id: "same-id".to_string(),
                        reviewed_execution_id: "executor-id".to_string(),
                        route: "coordinator".to_string(),
                        model: Some("provider/reviewer".to_string()),
                        artifact_path: Some("review.md".to_string()),
                        verdict: "pass".to_string(),
                        findings: vec!["reviewed goal evidence".to_string()],
                    }),
                },
                ReviewDimensionResult {
                    dimension: ReviewDimension::CodeQuality,
                    passed: true,
                    evidence: "test".to_string(),
                    reviewer_evidence: Some(ReviewerEvidence {
                        execution_id: "same-id".to_string(),
                        reviewed_execution_id: "executor-id".to_string(),
                        route: "scope-check".to_string(),
                        model: Some("provider/reviewer".to_string()),
                        artifact_path: Some("review.md".to_string()),
                        verdict: "pass".to_string(),
                        findings: vec!["reviewed quality evidence".to_string()],
                    }),
                },
            ],
        };
        assert!(
            gate.validate_independent_reviewers().is_err(),
            "one execution id cannot identify conflicting reviewer receipts"
        );
    }

    #[test]
    fn ordinary_executor_attempt_is_not_reviewer_evidence() -> Result<()> {
        let scope_check = crate::tools::ScopeCheck::default();
        let (_receipt_dir, mut attempt) = mock_task_attempt()?;
        attempt.worker_category = "quick".to_string();
        let gate = ReviewGate::from_inputs(
            true,
            &WorkerStatus::Succeeded,
            &scope_check,
            None,
            &[],
            &[attempt],
        );
        assert!(
            gate.results
                .iter()
                .all(|result| result.reviewer_evidence.is_none())
        );
        assert!(gate.synthetic_evidence_only_reason().is_some());
        Ok(())
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
            &[],
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
            &[],
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
                .apply_budget_for_route_change(&snapshot, RouteChangeType::RouteChange, false)
                .is_ok(),
            "under budget should be Ok"
        );

        let full_snapshot = BudgetSnapshot {
            worker_call_count: 2,
            ..BudgetSnapshot::default()
        };
        let result = budget.apply_budget_for_route_change(
            &full_snapshot,
            RouteChangeType::RouteChange,
            false,
        );
        assert!(result.is_err());
        assert!(
            result.as_ref().unwrap_err().contains("route change"),
            "Err should mention route change: {:?}",
            result
        );

        let fallback_result =
            budget.apply_budget_for_route_change(&full_snapshot, RouteChangeType::Fallback, false);
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
        assert!(
            budget
                .apply_budget_for_route_change(
                    &premium_snapshot,
                    RouteChangeType::RouteChange,
                    false,
                )
                .is_ok(),
            "an exhausted premium budget must not block a non-premium worker"
        );
        let review_result = budget.apply_budget_for_route_change(
            &premium_snapshot,
            RouteChangeType::ReviewTrigger,
            true,
        );
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
            None,
            &[],
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
            &[],
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
            &[],
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
            status: WorkerStatus::Failed,
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
    fn completion_notification_event_failure_does_not_advance_notified_epoch() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let task_record = TaskRecord {
            task_id: "task_notification_commit".to_string(),
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
            "task_notification_commit",
            "task-record.json",
            &format!("{task_record_json}\n"),
        )?;

        let writer_attempts = Arc::new(Mutex::new(0usize));
        let first_result = append_completion_notification_with_event_writer(
            &store,
            "session_notification_commit",
            "goal_notification_commit",
            "task_notification_commit",
            7,
            &{
                let writer_attempts = writer_attempts.clone();
                let event_store = store.clone();
                move |completion_event| {
                    let mut attempts = writer_attempts
                        .lock()
                        .map_err(|_| anyhow::anyhow!("mutex"))?;
                    *attempts += 1;
                    if *attempts == 1 {
                        return Err(anyhow::anyhow!("deliberate event append failure"));
                    }
                    event_store.append_event(completion_event).map(|_| ())
                }
            },
        );
        assert!(first_result.is_err());

        let task_record_path = store
            .worker_dir("task_notification_commit")
            .join("task-record.json");
        let after_failure: TaskRecord =
            serde_json::from_str(&fs::read_to_string(&task_record_path)?)?;
        assert_eq!(after_failure.notified_epoch, -1);

        append_completion_notification_with_event_writer(
            &store,
            "session_notification_commit",
            "goal_notification_commit",
            "task_notification_commit",
            7,
            &{
                let writer_attempts = writer_attempts.clone();
                let event_store = store.clone();
                move |completion_event| {
                    let mut attempts = writer_attempts
                        .lock()
                        .map_err(|_| anyhow::anyhow!("mutex"))?;
                    *attempts += 1;
                    event_store.append_event(completion_event).map(|_| ())
                }
            },
        )?;

        let after_success: TaskRecord =
            serde_json::from_str(&fs::read_to_string(&task_record_path)?)?;
        assert_eq!(after_success.notified_epoch, 7);
        let events = fs::read_to_string(store.events_path("session_notification_commit"))?;
        let completion_events = events
            .lines()
            .filter_map(|line| serde_json::from_str::<Event>(line).ok())
            .filter(|event| matches!(event.kind, EventKind::CompletionNotified))
            .count();
        assert_eq!(completion_events, 1);
        let attempts = writer_attempts
            .lock()
            .map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(*attempts, 2);
        Ok(())
    }

    #[test]
    fn completion_notification_marker_failure_reuses_persisted_event() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let task_record = TaskRecord {
            task_id: "task_notification_marker".to_string(),
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
            run_epoch: 8,
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
            "task_notification_marker",
            "task-record.json",
            &format!("{task_record_json}\n"),
        )?;

        let task_record_path = store
            .worker_dir("task_notification_marker")
            .join("task-record.json");
        let backup_path = task_record_path.with_extension("json.backup");
        let event_store = store.clone();
        let first_result = append_completion_notification_with_event_writer(
            &store,
            "session_notification_marker",
            "goal_notification_marker",
            "task_notification_marker",
            8,
            &{
                let task_record_path = task_record_path.clone();
                let backup_path = backup_path.clone();
                move |completion_event| {
                    event_store.append_event(completion_event)?;
                    fs::rename(&task_record_path, &backup_path)?;
                    fs::create_dir(&task_record_path)?;
                    Ok(())
                }
            },
        );
        assert!(first_result.is_err());
        assert!(task_record_path.is_dir());
        fs::remove_dir(&task_record_path)?;
        fs::rename(&backup_path, &task_record_path)?;

        let event_store = store.clone();
        append_completion_notification_with_event_writer(
            &store,
            "session_notification_marker",
            "goal_notification_marker",
            "task_notification_marker",
            8,
            &move |completion_event| event_store.append_event(completion_event).map(|_| ()),
        )?;

        let stored_task_record: TaskRecord =
            serde_json::from_str(&fs::read_to_string(&task_record_path)?)?;
        assert_eq!(stored_task_record.notified_epoch, 8);
        let events = fs::read_to_string(store.events_path("session_notification_marker"))?;
        let completion_events = events
            .lines()
            .filter_map(|line| serde_json::from_str::<Event>(line).ok())
            .filter(|event| matches!(event.kind, EventKind::CompletionNotified))
            .count();
        assert_eq!(completion_events, 1);
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            budget: None,
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

        assert_eq!(outcome.status, GoalStatus::NeedsUser);
        assert_eq!(
            *review_calls.lock().expect("review mutex poisoned"),
            DEFAULT_MAX_ITERATIONS
        );
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
            budget: None,
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

        assert_eq!(outcome.status, GoalStatus::NeedsUser);
        assert_eq!(
            *review_calls.lock().expect("review mutex poisoned"),
            DEFAULT_MAX_ITERATIONS
        );
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
            budget: None,
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

        assert_eq!(outcome.status, GoalStatus::NeedsUser);
        assert_eq!(
            *review_calls.lock().expect("review mutex poisoned"),
            DEFAULT_MAX_ITERATIONS
        );
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
            None,
            &[],
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
            budget: None,
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

        assert_eq!(outcome.status, GoalStatus::NeedsUser);
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
            budget: None,
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
            budget: None,
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

        assert_eq!(outcome.status, GoalStatus::NeedsUser);
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
            budget: None,
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
            &[],
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
            &[],
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
            &[],
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
            &[],
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
            resume_count: 0,
            last_progress_marker: None,
            stuck_reason: None,
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
            &[],
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
        let request =
            ts::make_worker_start_request(&store, temp_dir.path(), &task, "test-goal", &config);

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
            true,
            &WorkerStatus::Succeeded,
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
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            Some(&ownership),
            Some(&lineage),
            &[],
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
    fn restored_lineage_is_reconciled_before_a_new_run() {
        let mut lineage = WorkLineage::new("root-session".to_string());
        lineage.status = ContinuationStatus::Completed;
        lineage.plan_remaining_items = 0;
        lineage.active_task_ids = vec!["stale-task".to_string()];

        prepare_lineage_for_run(&mut lineage, "resumed-session");

        assert_eq!(lineage.status, ContinuationStatus::Running);
        assert_eq!(lineage.plan_remaining_items, 1);
        assert!(lineage.active_task_ids.is_empty());
        assert!(
            lineage
                .orchestrator_session_ids
                .contains(&"resumed-session".to_string())
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
            &budget,
            &BudgetSnapshot::default(),
            &[],
            false,
            None,
            Some(&ownership),
            None,
            &[],
        );
        assert!(
            !matches!(evaluation.status, GoalStatus::Complete),
            "GBX-003 GAP: synthetic review should not allow completion but got {:?}",
            evaluation.status
        );
    }

    /// Broker-backed phase actor E2E test.
    /// Tests the full 4-phase flow through a WorkerBroker verifying
    /// strict call order, session follow-up, reviewer independence,
    /// canonical approval after completed broker receipt chain, and
    /// tampered receipt rejection.
    #[test]
    fn gearbox_phase_actor_broker_e2e() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let scope = Scope::new(Vec::new(), vec![".git".to_string()], 10);
        let draft = deterministic_fallback_draft(
            "Implement a broker-backed phase actor flow",
            &scope,
            &["echo verify".to_string()],
        );
        let mut goal = planning_goal(&draft)?;

        let backend = Arc::new(crate::test_support::test_support::FakeNativeWorkerBackend::new());
        let registry = Arc::new(crate::workers::WorkerRegistry::with_native_backend(backend));
        let broker = Arc::new(WorkerBroker::new(
            registry,
            temp_dir.path().join(".gearbox-agent"),
        ));

        let phase_order = Arc::new(Mutex::new(Vec::new()));
        let critic_calls = Arc::new(AtomicUsize::new(0));

        let critic_hook: PlanCriticHook = {
            let critic_calls = critic_calls.clone();
            Arc::new(move |input| {
                let call = critic_calls.fetch_add(1, Ordering::SeqCst) + 1;
                phase_order
                    .lock()
                    .unwrap()
                    .push(format!("critic_call_{call}"));
                plan_critic_submission(&input, call, PlanCriticDecision::Approve)
            })
        };

        let revision_hook: PlanRevisionHook = Arc::new(|input| {
            let mut draft = input.plan.draft;
            draft
                .final_acceptance
                .push("Revised acceptance evidence.".to_string());
            let raw_output = serde_json::to_string(&draft)?;
            Ok(PlanRevisionSubmission {
                draft,
                planner: phase_identity("planner_revision"),
                raw_output,
                artifact_path: None,
            })
        });

        let planner_identity = PhaseExecutionIdentity {
            execution_id: "planner_execution".to_string(),
            phase_session_id: "planner_session".to_string(),
            backend: PhaseExecutionBackend::LanguageModelRequest,
            agent_id: Some("zed".to_string()),
            provider_id: Some("test-provider".to_string()),
            model_id: Some("test-model".to_string()),
            actual_session_id: None,
        };

        let current_model = ModelSelectorId {
            agent_id: "zed".to_string(),
            provider_id: "test-provider".to_string(),
            model_id: "test-model".to_string(),
        };

        let direct_usage_provider: DirectModelUsageProvider = Arc::new(|request| {
            let ModelAvailability::Available(model) = &request.requested_model else {
                return Ok(None);
            };
            Ok(Some(DirectModelUsageReport {
                usage: BrokerUsage {
                    requested_tokens: Some(34),
                    actual_tokens: Some(13),
                    model: format!("{}/{}", model.provider_id, model.model_id),
                    duration_ms: Some(240),
                    cost_micros: Some(19),
                    cache_hit: Some(false),
                    unavailable_reason: None,
                },
                actual_model: Some(model.clone()),
                cost_unit: DirectModelCostUnit::Micros,
            }))
        });

        let phase_runtime = PhaseRuntime {
            routes: PhaseRouteTable::legacy_defaults(),
            inventory: LiveModelInventory {
                models: vec![current_model.clone()],
            },
            current_model: Some(current_model),
            planner: Some(planner_identity),
            intent_fold_hook: None,
            planner_hook: None,
            plan_critic_hook: Some(critic_hook),
            oracle_hook: None,
            plan_revision_hook: Some(revision_hook),
            strategist_next_goal_hook: None,
            require_plan_approval: true,
            max_plan_revisions: 2,
            broker: Some(broker.clone()),
            broker_factory: None,
            direct_model_usage_provider: Some(direct_usage_provider),
        };

        store.write_phase_route_table(&goal.id, &phase_runtime.routes)?;

        let plan = build_approved_plan_graph(
            &mut goal,
            &scope,
            &["echo verify".to_string()],
            temp_dir.path(),
            &store,
            "session-broker-e2e",
            &None,
            None,
            &phase_runtime,
        )?;

        let approval = store
            .read_plan_approval_state(&goal.id)?
            .context("approval state missing after broker-backed plan approval")?;
        assert_eq!(
            approval.status,
            PlanApprovalStatus::Approved,
            "broker-backed flow must produce an approved plan"
        );
        assert_eq!(
            approval.plan_hash, plan.plan_hash,
            "approval must match the sealed plan hash"
        );
        assert!(
            approval.critic_receipt_hash.is_some(),
            "approval must include a critic receipt hash"
        );

        let broker_state = broker.current_state()?;
        assert!(
            broker_state.session_identity.is_some()
                || broker_state.lifecycle.name()
                    != crate::worker_broker::LifecycleStateName::Discovered,
            "broker must have made progress through resolve/start"
        );
        let direct_receipt_path = std::fs::read_dir(
            temp_dir
                .path()
                .join(".gearbox-agent")
                .join("direct-execution-receipts"),
        )?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .context("direct-model broker receipt missing")?;
        let direct_receipt: BrokerLifecycleReceipt =
            serde_json::from_slice(&std::fs::read(direct_receipt_path)?)?;
        direct_receipt.validate()?;
        assert_eq!(
            direct_receipt
                .usage
                .as_ref()
                .and_then(|usage| usage.actual_tokens),
            Some(13)
        );
        assert_eq!(
            direct_receipt
                .usage
                .as_ref()
                .and_then(|usage| usage.unavailable_reason.as_deref()),
            None
        );
        assert!(
            !store.read_model_call_ledger(&goal.id)?.is_empty(),
            "direct usage callback must append the workspace ModelCallLedger"
        );

        store.write_plan_graph(&plan)?;
        assert!(
            store.read_plan_graph(&goal.id)?.is_some(),
            "canonical plan must be readable after broker-backed approval"
        );
        assert_eq!(critic_calls.load(Ordering::SeqCst), 1);

        {
            let review_dir = store.plan_review_dir(&goal.id);
            if let Ok(entries) = std::fs::read_dir(&review_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.contains("critic-output") {
                        std::fs::write(entry.path(), "tampered critic output")?;
                        break;
                    }
                }
            }
        }
        let read_result = store.read_plan_graph(&goal.id);
        assert!(
            read_result.is_err(),
            "tampered critic output must invalidate the stored plan graph"
        );

        let broker_state_after = broker.current_state()?;
        assert!(
            broker_state_after.interaction_ordinal > 0
                || broker_state_after.lifecycle.name()
                    != crate::worker_broker::LifecycleStateName::Discovered,
            "broker must record at least one interaction or lifecycle transition"
        );

        assert_eq!(
            std::fs::read_dir(store.workers_dir())?.count(),
            0,
            "no worker should have been dispatched for a tampered/gated plan"
        );

        Ok(())
    }

    #[test]
    fn direct_execution_receipt_consumes_native_usage_sidecar() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let backend = Arc::new(ts::FakeNativeWorkerBackend::new());
        let registry = Arc::new(crate::workers::WorkerRegistry::with_native_backend(backend));
        let broker = WorkerBroker::new(registry, temp_dir.path().join(".gearbox-agent"));
        let model = ModelSelectorId {
            agent_id: "zed".to_string(),
            provider_id: "test-provider".to_string(),
            model_id: "test-model".to_string(),
        };
        let request = BrokerPhaseRequest {
            schema_version: crate::worker_broker::BROKER_SCHEMA_VERSION,
            phase_decision_hash: "a".repeat(64),
            goal_id: "goal-direct-usage".to_string(),
            plan_id: "plan-direct-usage".to_string(),
            plan_revision: 1,
            task_id: "task-direct-usage".to_string(),
            requested_agent: "direct".to_string(),
            requested_model: ModelAvailability::Available(model.clone()),
            allowed_fallback_models: Vec::new(),
        };
        broker.resolve(request.clone())?;
        broker.record_direct_model_usage(
            &request.phase_decision_hash,
            BrokerUsage {
                requested_tokens: Some(34),
                actual_tokens: Some(13),
                model: "test-provider/test-model".to_string(),
                duration_ms: Some(240),
                cost_micros: Some(19),
                cache_hit: Some(false),
                unavailable_reason: None,
            },
            Some(model.clone()),
        )?;

        write_direct_execution_receipt(&broker, &request, BrokerOutcome::Completed, &store)?;
        let receipt_path = fs::read_dir(broker.artifacts_root().join("direct-execution-receipts"))?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .context("direct execution receipt missing")?;
        let receipt: BrokerLifecycleReceipt = serde_json::from_slice(&fs::read(receipt_path)?)?;
        receipt.validate()?;
        assert_eq!(receipt.actual_model, Some(model));
        assert_eq!(
            receipt.usage.as_ref().and_then(|usage| usage.actual_tokens),
            Some(13)
        );
        assert_eq!(
            receipt.usage.as_ref().and_then(|usage| usage.cost_micros),
            Some(19)
        );
        assert_eq!(
            receipt
                .usage
                .as_ref()
                .and_then(|usage| usage.unavailable_reason.as_deref()),
            None
        );
        let ledger = store.read_model_call_ledger("goal-direct-usage")?;
        assert_eq!(ledger.len(), 1);
        assert_eq!(ledger[0].actual_tokens, Some(13));
        assert_eq!(ledger[0].cost_micros, Some(19));
        Ok(())
    }

    #[test]
    fn direct_model_usage_cost_units_normalize_to_micros() -> Result<()> {
        let report = DirectModelUsageReport {
            usage: BrokerUsage {
                requested_tokens: None,
                actual_tokens: None,
                model: "provider/model".to_string(),
                duration_ms: None,
                cost_micros: Some(3),
                cache_hit: None,
                unavailable_reason: None,
            },
            actual_model: None,
            cost_unit: DirectModelCostUnit::Major,
        }
        .normalized_usage()?;
        assert_eq!(report.usage.cost_micros, Some(3_000_000));
        assert_eq!(report.cost_unit, DirectModelCostUnit::Micros);
        Ok(())
    }

    #[test]
    fn objective_production_gap_repro() -> Result<()> {
        let cli_runtime = PhaseRuntime::legacy();
        assert!(
            cli_runtime.broker_factory.is_none(),
            "CLI objective path uses PhaseRuntime::legacy() which lacks broker_factory"
        );
        assert!(
            cli_runtime.broker.is_none(),
            "CLI objective path uses PhaseRuntime::legacy() which lacks broker"
        );
        let cli_routes_hash = cli_runtime.routes.hash()?;
        let legacy_routes_hash = PhaseRouteTable::legacy_defaults().hash()?;
        assert_eq!(
            cli_routes_hash, legacy_routes_hash,
            "CLI objective path routes must be legacy_defaults"
        );

        let profiles = crate::phase_routing::OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        };
        let gui_routes = PhaseRouteTable::opencode_only(profiles)?;
        let gui_routes_hash = gui_routes.hash()?;
        assert_ne!(
            cli_routes_hash, gui_routes_hash,
            "CLI legacy routes must differ from GUI opencode_only production routes"
        );

        let temp_dir = tempfile::tempdir()?;
        let backend = Arc::new(ts::FakeNativeWorkerBackend::new());
        let registry = Arc::new(crate::workers::WorkerRegistry::with_native_backend(backend));
        let broker_factory = Arc::new(crate::worker_broker::PhaseBrokerFactory::new(
            registry,
            temp_dir.path().join(".gearbox-agent"),
        ));
        let gui_runtime = PhaseRuntime {
            routes: gui_routes,
            inventory: LiveModelInventory::default(),
            current_model: None,
            planner: None,
            intent_fold_hook: None,
            planner_hook: None,
            plan_critic_hook: None,
            oracle_hook: None,
            plan_revision_hook: None,
            strategist_next_goal_hook: None,
            require_plan_approval: false,
            max_plan_revisions: DEFAULT_MAX_PLAN_REVISIONS,
            broker: None,
            broker_factory: Some(broker_factory),
            direct_model_usage_provider: None,
        };
        assert!(
            gui_runtime.broker_factory.is_some(),
            "GUI production path must have broker_factory"
        );
        assert_ne!(
            cli_runtime.routes, gui_runtime.routes,
            "CLI and GUI PhaseRuntime routes must not be equivalent"
        );
        Ok(())
    }

    #[test]
    fn objective_crash_after_goal_settle_repro() -> Result<()> {
        test_seams::reset();
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Approve));
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.planner_hook = Some(Arc::new(|input: PlannerInput| {
            let draft = deterministic_fallback_draft(
                &input.request,
                &input.scope,
                &input.verification_commands,
            );
            Ok(PlannerSubmission {
                raw_output: serde_json::to_string(&draft)?,
                draft,
                planner: phase_identity("repro_planner"),
                artifact_path: None,
                repository_evidence_path: None,
            })
        }));
        phase_runtime.strategist_next_goal_hook =
            Some(Arc::new(|input: StrategistNextGoalInput| {
                let verdict = StrategistNextGoalVerdict {
                    schema_version: 1,
                    goal_id: input.goal_id,
                    epoch_id: input.epoch_id,
                    reviewed_status: input.status,
                    decision: StrategistNextGoalDecision::Continue,
                    next_objective: Some("Create the successor objective".to_string()),
                    acceptance_signals: vec!["The successor has a durable edge".to_string()],
                    required_questions: Vec::new(),
                    evidence_refs: vec![input.final_report_path],
                    answerable_now: false,
                    rationale: "The first epoch passed and has a bounded successor".to_string(),
                };
                Ok(StrategistNextGoalSubmission {
                    raw_output: serde_json::to_string(&verdict)?,
                    verdict,
                    strategist: phase_identity("repro_strategist"),
                    artifact_path: None,
                })
            }));

        let intercept_flag = Arc::new(Mutex::new(true));
        let write_order = Arc::new(Mutex::new(Vec::new()));
        let write_order_clone_a = write_order.clone();
        let write_order_clone_b = write_order.clone();
        let intercept_flag_clone = intercept_flag.clone();
        let worker_dispatch_count = Arc::new(AtomicUsize::new(0));

        test_seams::install(test_seams::ObjectiveControllerTestSeam {
            on_goal_settled: Some(Arc::new(move |goal_id, epoch_id| {
                write_order_clone_a
                    .lock()
                    .unwrap()
                    .push(format!("goal_settled:{goal_id}:{epoch_id}"));
            })),
            on_goal_lease_released: Some(Arc::new(move |goal_id, epoch_id| {
                write_order_clone_b
                    .lock()
                    .unwrap()
                    .push(format!("goal_lease_released:{goal_id}:{epoch_id}"));
            })),
            on_objective_graph_commit: Some(Arc::new(move |objective_id, _graph| {
                write_order
                    .lock()
                    .unwrap()
                    .push(format!("objective_graph_commit:{objective_id}"));
            })),
            intercept_settled_to_graph_commit: Some(Arc::new(move || {
                *intercept_flag_clone.lock().unwrap()
            })),
            worker_dispatch_count: worker_dispatch_count.clone(),
            crash_point: None,
            on_continue_event: None,
            on_child_attach: None,
        });

        let result = Orchestrator::run_objective_with_phase_runtime(
            RunOptions {
                request: "Reproduce crash after goal settle".to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: vec!["echo verify-ok".to_string()],
                worker: objective_worker_for_test(),
                allowed_paths: Vec::new(),
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 10,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 2,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: 1,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some("crash-repro-session".to_string()),
                continuation: true,
            },
            phase_runtime.clone(),
            ObjectivePolicy {
                auto_continue: true,
                max_epochs: 3,
                max_calls: 96,
                max_tokens: 12_288_000,
                max_cost_micros: 10_000_000,
                max_unknown_usage_calls: 32,
                max_consecutive_no_progress: 2,
                max_consecutive_failures: 3,
                cooldown_seconds: 0,
            },
        );

        assert!(
            result.is_err(),
            "test seam must simulate a crash after goal settle"
        );
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg
                .contains("simulated crash after goal settled but before objective graph commit"),
            "error must describe the crash gap: {error_msg}"
        );

        let objective_id = objective_id_for(
            "crash-repro-session",
            temp_dir.path(),
            "Reproduce crash after goal settle",
        )?;

        let graph = store
            .read_objective_graph(&objective_id)?
            .context("objective graph must exist after crash")?;
        let active_node = graph.active_node().context("active node must exist")?;
        assert!(
            !active_node.status.is_terminal(),
            "crash gap: objective graph must NOT reflect settled goal status; found {:?}",
            active_node.status
        );

        let goal_epoch_events = store.read_goal_epoch_events(&active_node.goal_id)?;
        assert!(
            goal_epoch_events
                .iter()
                .any(|e| e.kind == GoalEpochEventKind::Settled),
            "goal epoch must have Settled event proving the goal did complete"
        );

        let worker_dispatches = worker_dispatch_count.load(Ordering::SeqCst);
        assert!(
            worker_dispatches > 0,
            "worker must have been dispatched during the goal run"
        );

        *intercept_flag.lock().unwrap() = false;

        let mut graph = store
            .read_objective_graph(&objective_id)?
            .context("graph must exist")?;
        let objective_lease = store.acquire_objective_lease(
            &objective_id,
            "crash-repro-session",
            Duration::from_secs(60),
        )?;
        reconcile_objective_frontier(
            &store,
            &objective_id,
            "crash-repro-session",
            &mut graph,
            Some(&objective_lease),
            &ObjectivePolicy {
                auto_continue: true,
                max_epochs: 3,
                max_calls: 96,
                max_tokens: 12_288_000,
                max_cost_micros: 10_000_000,
                max_unknown_usage_calls: 32,
                max_consecutive_no_progress: 2,
                max_consecutive_failures: 3,
                cooldown_seconds: 0,
            },
            &Budget::default(),
        )?;
        objective_lease.release()?;
        assert!(
            graph.nodes[0].status == GoalStatus::Complete,
            "reconcile must commit the settled parent from the outcome receipt"
        );
        assert!(
            graph.active_goal_id.is_some(),
            "reconcile must advance to the single recovered child frontier"
        );

        let resumed_result = Orchestrator::run_objective_with_phase_runtime(
            RunOptions {
                request: "Reproduce crash after goal settle".to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: vec!["echo verify-ok".to_string()],
                worker: objective_worker_for_test(),
                allowed_paths: Vec::new(),
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 10,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 2,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: 1,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some("crash-repro-session".to_string()),
                continuation: true,
            },
            phase_runtime,
            ObjectivePolicy {
                auto_continue: true,
                max_epochs: 3,
                max_calls: 96,
                max_tokens: 12_288_000,
                max_cost_micros: 10_000_000,
                max_unknown_usage_calls: 32,
                max_consecutive_no_progress: 2,
                max_consecutive_failures: 3,
                cooldown_seconds: 0,
            },
        );

        assert!(
            resumed_result.is_ok(),
            "resumed controller must reuse the settled parent and continue from the child: {:?}",
            resumed_result.err()
        );
        let resumed = resumed_result?;
        assert!(
            matches!(
                resumed.status,
                ObjectiveStatus::Limited | ObjectiveStatus::Complete
            ),
            "resumed controller must reach a bounded terminal state, got {:?}",
            resumed.status
        );
        assert!(
            worker_dispatch_count.load(Ordering::SeqCst) > worker_dispatches,
            "recovery must make forward progress from the settled parent"
        );

        test_seams::reset();

        Ok(())
    }

    #[test]
    fn objective_crash_window_matrix_recovers_exactly_once() -> Result<()> {
        let crash_points = [
            test_seams::ObjectiveCrashPoint::BeforeOutcomeReceipt,
            test_seams::ObjectiveCrashPoint::AfterOutcomeReceiptBeforeGraph,
            test_seams::ObjectiveCrashPoint::AfterChildReservationBeforeEdge,
            test_seams::ObjectiveCrashPoint::AfterChildEdgeBeforeStarted,
            test_seams::ObjectiveCrashPoint::AfterChildOutcomeBeforeObjectiveSettled,
        ];
        let policy = ObjectivePolicy {
            auto_continue: true,
            max_epochs: 2,
            max_calls: 96,
            max_tokens: 12_288_000,
            max_cost_micros: 10_000_000,
            max_unknown_usage_calls: 32,
            max_consecutive_no_progress: 2,
            max_consecutive_failures: 3,
            cooldown_seconds: 0,
        };
        for (index, crash_point) in crash_points.into_iter().enumerate() {
            test_seams::reset();
            let temp_dir = tempfile::tempdir()?;
            let worker_dispatch_count = Arc::new(AtomicUsize::new(0));
            test_seams::install(test_seams::ObjectiveControllerTestSeam {
                on_goal_settled: None,
                on_goal_lease_released: None,
                on_objective_graph_commit: None,
                on_continue_event: None,
                on_child_attach: None,
                intercept_settled_to_graph_commit: None,
                worker_dispatch_count: worker_dispatch_count.clone(),
                crash_point: Some(crash_point),
            });
            let run = |runtime: PhaseRuntime| {
                Orchestrator::run_objective_with_phase_runtime(
                    RunOptions {
                        request: "Crash matrix objective".to_string(),
                        workspace: temp_dir.path().to_path_buf(),
                        verification_commands: vec!["echo verify-ok".to_string()],
                        worker: objective_worker_for_test(),
                        allowed_paths: Vec::new(),
                        forbidden_paths: vec![".git".to_string()],
                        max_files_changed: 10,
                        install_dependencies: false,
                        event_sink: None,
                        cancellation_token: None,
                        max_iterations: 2,
                        max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                        max_child_depth: usize::MAX,
                        max_runtime_minutes: 1,
                        budget: None,
                        coordinator_model: None,
                        coordinator_brief: None,
                        coordinator_review_hook: None,
                        task_manager_control: None,
                        task_manager: None,
                        session_id: Some("crash-matrix-session".to_string()),
                        continuation: true,
                    },
                    runtime,
                    policy.clone(),
                )
            };
            let initial = run(crash_matrix_phase_runtime());
            assert!(
                initial.is_err(),
                "crash point {crash_point:?} must terminate the first controller"
            );
            test_seams::with_seam(|seam| {
                if let Some(seam) = seam.as_mut() {
                    seam.crash_point = None;
                }
            });
            let resumed = run(crash_matrix_phase_runtime())?;
            assert_eq!(resumed.status, ObjectiveStatus::Complete);
            let store = StateStore::new(temp_dir.path());
            let graph = store
                .read_objective_graph(&resumed.objective_id)?
                .context("crash matrix graph must be recoverable")?;
            assert_eq!(
                graph.nodes.len(),
                2,
                "crash point {index} created duplicate goals"
            );
            assert!(graph.nodes.iter().all(|node| node.status.is_terminal()));
            let ledger =
                store.read_objective_budget_ledger(&resumed.objective_id, &policy.hash()?)?;
            assert_eq!(ledger.reservations.len(), 2);
            assert!(ledger.reservations.iter().all(|reservation| {
                reservation.status == crate::state::ObjectiveBudgetReservationStatus::Settled
            }));
            assert_eq!(
                worker_dispatch_count.load(Ordering::SeqCst),
                4,
                "crash point {crash_point:?} reran the wrong goal"
            );
        }
        test_seams::reset();
        Ok(())
    }

    #[test]
    fn objective_budget_reservation_repro() -> Result<()> {
        test_seams::reset();
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let critic_hook: PlanCriticHook =
            Arc::new(|input| plan_critic_submission(&input, 1, PlanCriticDecision::Approve));
        let mut phase_runtime = phase_runtime_for_test(Some(critic_hook));
        phase_runtime.planner_hook = Some(Arc::new(|input: PlannerInput| {
            let draft = deterministic_fallback_draft(
                &input.request,
                &input.scope,
                &input.verification_commands,
            );
            Ok(PlannerSubmission {
                raw_output: serde_json::to_string(&draft)?,
                draft,
                planner: phase_identity("repro_planner"),
                artifact_path: None,
                repository_evidence_path: None,
            })
        }));
        phase_runtime.strategist_next_goal_hook =
            Some(Arc::new(|input: StrategistNextGoalInput| {
                let verdict = StrategistNextGoalVerdict {
                    schema_version: 1,
                    goal_id: input.goal_id,
                    epoch_id: input.epoch_id,
                    reviewed_status: input.status,
                    decision: StrategistNextGoalDecision::Continue,
                    next_objective: Some("Create the successor objective".to_string()),
                    acceptance_signals: vec!["The successor has a durable edge".to_string()],
                    required_questions: Vec::new(),
                    evidence_refs: vec![input.final_report_path],
                    answerable_now: false,
                    rationale: "The first epoch passed and has a bounded successor".to_string(),
                };
                Ok(StrategistNextGoalSubmission {
                    raw_output: serde_json::to_string(&verdict)?,
                    verdict,
                    strategist: phase_identity("repro_strategist"),
                    artifact_path: None,
                })
            }));

        let dispatch_count = Arc::new(AtomicUsize::new(0));
        let dispatch_count_clone = dispatch_count.clone();
        test_seams::install(test_seams::ObjectiveControllerTestSeam {
            on_goal_settled: None,
            on_goal_lease_released: None,
            on_objective_graph_commit: None,
            on_continue_event: None,
            on_child_attach: Some(Arc::new(move |_objective_id, _child_goal_id| {
                dispatch_count_clone.fetch_add(1, Ordering::SeqCst);
            })),
            intercept_settled_to_graph_commit: None,
            worker_dispatch_count: Arc::new(AtomicUsize::new(0)),
            crash_point: None,
        });

        let outcome = Orchestrator::run_objective_with_phase_runtime(
            RunOptions {
                request: "Reproduce budget reservation gap".to_string(),
                workspace: temp_dir.path().to_path_buf(),
                verification_commands: vec!["echo verify-ok".to_string()],
                worker: objective_worker_for_test(),
                allowed_paths: Vec::new(),
                forbidden_paths: vec![".git".to_string()],
                max_files_changed: 10,
                install_dependencies: false,
                event_sink: None,
                cancellation_token: None,
                max_iterations: 2,
                max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
                max_child_depth: usize::MAX,
                max_runtime_minutes: 1,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: Some("budget-repro-session".to_string()),
                continuation: true,
            },
            phase_runtime,
            ObjectivePolicy {
                auto_continue: true,
                max_epochs: 3,
                max_calls: 96,
                max_tokens: 12_288_000,
                max_cost_micros: 10_000_000,
                max_unknown_usage_calls: 32,
                max_consecutive_no_progress: 2,
                max_consecutive_failures: 3,
                cooldown_seconds: 0,
            },
        )?;

        test_seams::reset();

        assert!(
            outcome.goal_outcomes.len() >= 2,
            "auto_continue must produce at least two goal outcomes, got {}",
            outcome.goal_outcomes.len()
        );
        assert!(
            dispatch_count.load(Ordering::SeqCst) >= 1,
            "at least one child attach must have occurred"
        );

        let objective_id = objective_id_for(
            "budget-repro-session",
            temp_dir.path(),
            "Reproduce budget reservation gap",
        )?;

        let objectives_dir = store.objectives_dir();
        let reservation_ledger_path =
            objectives_dir.join(format!("{objective_id}.reservations.json"));
        assert!(
            reservation_ledger_path.exists(),
            "objective-wide reservation ledger must exist before child dispatch: {reservation_ledger_path:?}"
        );
        let reservation_ledger: crate::state::ObjectiveBudgetLedger =
            serde_json::from_str(&std::fs::read_to_string(&reservation_ledger_path)?)?;
        assert!(
            reservation_ledger.reservations.iter().any(|reservation| {
                reservation.status == crate::state::ObjectiveBudgetReservationStatus::Settled
            }),
            "child epoch reservation must settle into the objective ledger"
        );

        let graph = store
            .read_objective_graph(&objective_id)?
            .context("objective graph must exist")?;
        let (calls, _tokens, _cost, _unknown_calls) = objective_budget_totals(&store, &graph)?;
        assert!(
            calls > 0,
            "objective_budget_totals must aggregate from settled goal ledgers (calls={calls})"
        );

        Ok(())
    }

    #[test]
    fn objective_cli_profile_assertion() -> Result<()> {
        let cli_runtime = PhaseRuntime::legacy();
        assert!(
            cli_runtime.broker_factory.is_none(),
            "CLI --objective path must not have broker_factory (legacy is not production)"
        );
        assert!(
            cli_runtime.broker.is_none(),
            "CLI --objective path must not have broker (legacy is not production)"
        );

        let profiles = crate::phase_routing::OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        };
        let production_routes = PhaseRouteTable::opencode_only(profiles)?;
        let production_routes_hash = production_routes.hash()?;
        let cli_routes_hash = cli_runtime.routes.hash()?;
        assert_ne!(
            cli_routes_hash, production_routes_hash,
            "CLI legacy routes must differ from OpenCode production routes"
        );

        let temp_dir = tempfile::tempdir()?;
        let backend = Arc::new(ts::FakeNativeWorkerBackend::new());
        let registry = Arc::new(crate::workers::WorkerRegistry::with_native_backend(backend));
        let broker_factory = Arc::new(crate::worker_broker::PhaseBrokerFactory::new(
            registry,
            temp_dir.path().join(".gearbox-agent"),
        ));
        let production_runtime = PhaseRuntime {
            routes: production_routes,
            inventory: LiveModelInventory::default(),
            current_model: None,
            planner: None,
            intent_fold_hook: None,
            planner_hook: None,
            plan_critic_hook: None,
            oracle_hook: None,
            plan_revision_hook: None,
            strategist_next_goal_hook: None,
            require_plan_approval: false,
            max_plan_revisions: DEFAULT_MAX_PLAN_REVISIONS,
            broker: None,
            broker_factory: Some(broker_factory),
            direct_model_usage_provider: None,
        };
        assert!(
            production_runtime.broker_factory.is_some(),
            "production PhaseRuntime for --objective + OpenCode profile requires Gear-owned broker_factory"
        );
        assert!(
            cli_runtime.broker_factory.is_none(),
            "PhaseRuntime::legacy() is NOT production"
        );
        Ok(())
    }

    #[test]
    fn external_blocker_normalization_collapses_url_case_and_punctuation() {
        let first = normalize_blocker_text(
            "Authorization failed at https://example.test/login?token=abc; request access.",
        );
        let second = normalize_blocker_text(
            "AUTHORIZATION failed at https://other.test/login?token=def, request access!",
        );
        assert_eq!(first, second);
        assert!(blocker_is_external(&first));
        assert_eq!(
            hash_text(&first),
            hash_text(&second),
            "normalized blocker signatures must be stable across provider wording noise"
        );
    }
}
