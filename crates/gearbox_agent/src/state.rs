use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Duration, Local};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::runtime::{DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK, DEFAULT_MAX_RUNTIME_MINUTES};

pub fn timestamp() -> String {
    Local::now().to_rfc3339()
}

pub fn id_timestamp() -> String {
    Local::now().format("%Y%m%d_%H%M%S_%3f").to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub workspace: String,
    pub created_at: String,
    pub updated_at: String,
    pub current_goal_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationStatus {
    Running,
    Stopped,
    Completed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContinuationState {
    pub session_id: String,
    pub goal_id: String,
    pub status: ContinuationStatus,
    pub updated_at: String,
    /// The parent session that spawned this work, if any.
    /// Used to enforce lineage-based completion gating:
    /// ancestor sessions cannot complete while descendant work is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    /// The root orchestrator session for this work tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_session_id: Option<String>,
}

/// Gear-owned work lineage that tracks the hierarchy of related sessions.
/// Written to `.gearbox-agent/continuation/<root-session>/lineage.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkLineage {
    /// Root orchestrator session that started this work tree.
    pub root_session_id: String,
    /// All orchestrator sessions in this work tree (may have multiple after restart).
    pub orchestrator_session_ids: Vec<String>,
    /// Worker session IDs spawned by the orchestrator(s).
    pub worker_session_ids: Vec<String>,
    /// Number of plan items remaining.
    pub plan_remaining_items: usize,
    /// Active (non-terminal) task IDs.
    pub active_task_ids: Vec<String>,
    /// Overall continuation status for this work tree.
    pub status: ContinuationStatus,
    /// When this lineage record was last updated.
    pub updated_at: String,
}

impl WorkLineage {
    pub fn new(root_session_id: String) -> Self {
        Self {
            root_session_id: root_session_id.clone(),
            orchestrator_session_ids: vec![root_session_id],
            worker_session_ids: Vec::new(),
            plan_remaining_items: 0,
            active_task_ids: Vec::new(),
            status: ContinuationStatus::Running,
            updated_at: timestamp(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoordinatorModel {
    pub provider_id: String,
    pub model_id: String,
    pub name: String,
}

/// Ownership decision for execution: was the implementation delegated
/// to a worker, or was it attempted directly by Gear?
///
/// All code-modifying tasks must produce a `delegated: true` decision
/// before Gear may mark the goal Complete. The `route_reason` explains
/// why a particular worker (or no worker) was selected.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionOwnership {
    /// Whether execution was delegated to a worker.
    pub delegated: bool,
    /// The selected worker kind, if delegated.
    pub worker_kind: Option<String>,
    /// Why this ownership decision was made.
    pub route_reason: String,
    /// Risk profile used for routing: "low", "medium", "high", or "unknown".
    pub risk_profile: String,
    /// The worker task ID assigned to handle this execution, if any.
    pub worker_task_id: Option<String>,
    /// Timestamp when this ownership decision was made.
    pub decided_at: String,
}

/// @see ExecutionOwnership
#[deprecated(
    since = "0.1.0",
    note = "use ExecutionOwnership directly with worker_task_id and decided_at fields"
)]
pub type OwnershipDecision = ExecutionOwnership;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Goal {
    pub id: String,
    pub title: String,
    pub status: GoalStatus,
    pub workspace: String,
    pub created_at: String,
    pub updated_at: String,
    pub request: String,
    pub product_type: String,
    pub language_profile: String,
    pub success_criteria: Vec<String>,
    pub budget: Budget,
    pub current_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator_model: Option<CoordinatorModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator_brief: Option<String>,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Budget {
    #[serde(default = "default_max_calls_per_epoch")]
    pub max_calls_per_epoch: usize,
    pub max_worker_calls: usize,
    pub max_premium_worker_calls: usize,
    pub max_repair_attempts_per_error: usize,
    #[serde(default = "default_max_provider_unknown_streak")]
    pub max_provider_unknown_streak: usize,
    #[serde(default = "default_max_child_depth")]
    pub max_child_depth: usize,
    #[serde(default = "default_max_runtime_minutes")]
    pub max_runtime_minutes: usize,
    #[serde(default = "default_max_tokens_per_call")]
    pub max_tokens_per_call: u64,
    #[serde(default = "default_max_tokens_per_epoch")]
    pub max_tokens_per_epoch: u64,
    #[serde(default = "default_max_cost_micros_per_epoch")]
    pub max_cost_micros_per_epoch: u64,
    #[serde(default = "default_max_usage_unknown_calls")]
    pub max_usage_unknown_calls: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_calls_per_epoch: default_max_calls_per_epoch(),
            max_worker_calls: 8,
            max_premium_worker_calls: 8,
            max_repair_attempts_per_error: 2,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            max_tokens_per_call: default_max_tokens_per_call(),
            max_tokens_per_epoch: default_max_tokens_per_epoch(),
            max_cost_micros_per_epoch: default_max_cost_micros_per_epoch(),
            max_usage_unknown_calls: default_max_usage_unknown_calls(),
        }
    }
}

fn default_max_calls_per_epoch() -> usize {
    32
}

fn default_max_provider_unknown_streak() -> usize {
    DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK
}

fn default_max_child_depth() -> usize {
    usize::MAX
}

fn default_max_runtime_minutes() -> usize {
    DEFAULT_MAX_RUNTIME_MINUTES
}

fn default_max_tokens_per_call() -> u64 {
    128_000
}

fn default_max_tokens_per_epoch() -> u64 {
    4_096_000
}

fn default_max_cost_micros_per_epoch() -> u64 {
    u64::MAX
}

fn default_max_usage_unknown_calls() -> usize {
    32
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatus {
    Draft,
    Planning,
    Running,
    Verifying,
    NeedsUser,
    Blocked,
    Limited,
    Complete,
    Failed,
}

impl GoalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Planning => "planning",
            Self::Running => "running",
            Self::Verifying => "verifying",
            Self::NeedsUser => "needs_user",
            Self::Blocked => "blocked",
            Self::Limited => "limited",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub goal_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    pub title: String,
    pub kind: TaskKind,
    pub status: TaskStatus,
    pub assigned_worker: Option<String>,
    pub attempt: usize,
    pub scope: Scope,
    pub inputs: TaskInputs,
    pub outputs: TaskOutputs,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Intake,
    Spec,
    Plan,
    Scaffold,
    Edit,
    Verify,
    Repair,
    Review,
    Document,
    Handoff,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Complete,
    Blocked,
    Failed,
    Skipped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Scope {
    pub allowed_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub max_files_changed: usize,
}

impl Scope {
    pub fn new(
        allowed_paths: Vec<String>,
        forbidden_paths: Vec<String>,
        max_files_changed: usize,
    ) -> Self {
        let forbidden_paths = if forbidden_paths.is_empty() {
            vec![".git".to_string()]
        } else {
            forbidden_paths
        };

        Self {
            allowed_paths,
            forbidden_paths,
            max_files_changed,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TaskInputs {
    pub spec_path: Option<String>,
    pub plan_path: Option<String>,
    pub worker_packet_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_task: Option<crate::plan_graph::PlanTaskContract>,
    #[serde(default)]
    pub phase_route_locked: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TaskOutputs {
    pub changed_files: Vec<String>,
    pub commands_run: Vec<CommandRecord>,
    pub evidence: Vec<String>,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandRecord {
    pub command: String,
    pub exit_code: Option<i32>,
    pub success: bool,
    pub duration_ms: u128,
    pub stdout_excerpt: String,
    pub stderr_excerpt: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub ts: String,
    pub session_id: String,
    pub goal_id: Option<String>,
    pub task_id: Option<String>,
    pub kind: EventKind,
    pub message: String,
    pub data: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalEpochEventKind {
    Started,
    BudgetReserved,
    BudgetSettled,
    NextGoalSelected,
    PhaseCompleted,
    Settled,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalEpochEvent {
    pub schema_version: u32,
    pub goal_id: String,
    pub epoch_id: String,
    pub sequence: u64,
    pub idempotency_key: String,
    pub kind: GoalEpochEventKind,
    pub payload: Value,
    pub previous_hash: String,
    pub created_at: String,
    pub event_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalRunLease {
    pub schema_version: u32,
    pub goal_id: String,
    pub epoch_id: String,
    pub owner_session_id: String,
    pub lease_id: String,
    pub acquired_at: String,
    pub expires_at: String,
}

#[derive(Debug)]
pub struct GoalRunLeaseGuard {
    lease: GoalRunLease,
    file: fs::File,
    path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetReservationStatus {
    Reserved,
    Settled,
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettledBudgetUsage {
    pub requested_tokens: Option<u64>,
    pub actual_tokens: Option<u64>,
    pub cost_micros: Option<u64>,
    pub duration_ms: Option<u64>,
    pub cache_hit: Option<bool>,
    pub unavailable_reason: Option<String>,
}

impl SettledBudgetUsage {
    pub fn total_tokens(&self) -> Option<u64> {
        Some(
            self.requested_tokens?
                .saturating_add(self.actual_tokens.unwrap_or(0)),
        )
    }

    pub fn is_unknown(&self) -> bool {
        self.total_tokens().is_none() || self.cost_micros.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetReservation {
    pub reservation_id: String,
    pub goal_id: String,
    pub epoch_id: String,
    pub phase: String,
    pub worker_call: bool,
    pub premium: bool,
    pub reserved_tokens: u64,
    pub reserved_cost_micros: u64,
    pub status: BudgetReservationStatus,
    pub usage: Option<SettledBudgetUsage>,
    pub created_at: String,
    pub settled_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalBudgetLedger {
    pub schema_version: u32,
    pub goal_id: String,
    pub reservations: Vec<BudgetReservation>,
    pub updated_at: String,
    pub ledger_hash: String,
}

impl GoalBudgetLedger {
    fn seal(mut self) -> Result<Self> {
        self.ledger_hash.clear();
        self.ledger_hash = self.expected_hash()?;
        Ok(self)
    }

    fn validate(&self, goal_id: &str) -> Result<()> {
        if self.schema_version != 1 || self.goal_id != goal_id {
            bail!("goal budget ledger has an invalid schema or goal binding");
        }
        if self.ledger_hash != self.expected_hash()? {
            bail!("goal budget ledger integrity hash mismatch");
        }
        let mut reservation_ids = HashSet::new();
        for reservation in &self.reservations {
            if reservation.goal_id != goal_id
                || reservation.reservation_id.trim().is_empty()
                || reservation.epoch_id.trim().is_empty()
                || reservation.phase.trim().is_empty()
                || !reservation_ids.insert(reservation.reservation_id.as_str())
            {
                bail!("goal budget ledger contains an invalid reservation binding");
            }
            match reservation.status {
                BudgetReservationStatus::Reserved
                    if reservation.usage.is_some() || reservation.settled_at.is_some() =>
                {
                    bail!("reserved budget call cannot contain settlement fields");
                }
                BudgetReservationStatus::Settled
                    if reservation.usage.is_none() || reservation.settled_at.is_none() =>
                {
                    bail!("settled budget call requires usage and settled_at");
                }
                BudgetReservationStatus::Reserved
                | BudgetReservationStatus::Settled
                | BudgetReservationStatus::Released => {}
            }
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.ledger_hash.clear();
        let bytes = serde_json::to_vec(&payload).context("failed to serialize budget ledger")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

impl GoalRunLeaseGuard {
    pub fn lease(&self) -> &GoalRunLease {
        &self.lease
    }

    pub fn release(self) -> Result<()> {
        self.file
            .unlock()
            .with_context(|| format!("failed to unlock {}", self.path.display()))?;
        Ok(())
    }
}

impl GoalRunLease {
    fn validate(&self, goal_id: &str) -> Result<()> {
        if self.schema_version != 1 || self.goal_id != goal_id {
            bail!("goal run lease has an invalid schema or goal binding");
        }
        for (field, value) in [
            ("epoch_id", self.epoch_id.as_str()),
            ("owner_session_id", self.owner_session_id.as_str()),
            ("lease_id", self.lease_id.as_str()),
            ("acquired_at", self.acquired_at.as_str()),
            ("expires_at", self.expires_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("goal run lease requires non-empty {field}");
            }
        }
        DateTime::parse_from_rfc3339(&self.acquired_at)
            .context("goal run lease has invalid acquired_at")?;
        DateTime::parse_from_rfc3339(&self.expires_at)
            .context("goal run lease has invalid expires_at")?;
        Ok(())
    }
}

impl GoalEpochEvent {
    fn seal(
        goal_id: &str,
        epoch_id: &str,
        sequence: u64,
        idempotency_key: &str,
        kind: GoalEpochEventKind,
        payload: Value,
        previous_hash: String,
    ) -> Result<Self> {
        if goal_id.trim().is_empty()
            || epoch_id.trim().is_empty()
            || idempotency_key.trim().is_empty()
        {
            bail!("goal epoch events require non-empty goal, epoch, and idempotency ids");
        }
        let mut event = Self {
            schema_version: 1,
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            sequence,
            idempotency_key: idempotency_key.to_string(),
            kind,
            payload,
            previous_hash,
            created_at: timestamp(),
            event_hash: String::new(),
        };
        event.event_hash = event.expected_hash()?;
        Ok(event)
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.event_hash.clear();
        let bytes = serde_json::to_vec(&payload).context("failed to serialize epoch event")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    GoalCreated,
    SpecCreated,
    PlanCreated,
    PlanReviewStarted,
    PlanRevisionRequested,
    PlanReviewApproved,
    PlanApproved,
    PlanRejected,
    PhaseRouteSelected,
    TaskStarted,
    WorkerStarted,
    WorkerOutput,
    WorkerWaiting,
    WorkerFinished,
    WorkerFailed,
    CompletionNotified,
    ContinuationStarted,
    ContinuationStopped,
    ContinuationCompleted,
    DiffDetected,
    VerificationStarted,
    VerificationFailed,
    VerificationPassed,
    RepairStarted,
    GoalCompleted,
    GoalBlocked,
    GoalLimited,
    NextGoalSelected,
}

#[derive(Clone, Debug)]
pub struct StateStore {
    root: PathBuf,
}

impl StateStore {
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        Self {
            root: workspace.into().join(".gearbox-agent"),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn initialize(&self) -> Result<()> {
        for path in [
            self.sessions_dir(),
            self.goals_dir(),
            self.tasks_dir(),
            self.plans_dir(),
            self.plan_reviews_dir(),
            self.events_dir(),
            self.epochs_dir(),
            self.budgets_dir(),
            self.artifacts_dir(),
            self.workers_dir(),
        ] {
            fs::create_dir_all(&path)
                .with_context(|| format!("failed to create {}", path.display()))?;
        }
        Ok(())
    }

    pub fn sessions_dir(&self) -> PathBuf {
        self.root.join("sessions")
    }

    pub fn goals_dir(&self) -> PathBuf {
        self.root.join("goals")
    }

    pub fn tasks_dir(&self) -> PathBuf {
        self.root.join("tasks")
    }

    pub fn plans_dir(&self) -> PathBuf {
        self.root.join("plans")
    }

    pub fn plan_reviews_dir(&self) -> PathBuf {
        self.root.join("plan-reviews")
    }

    pub fn plan_review_dir(&self, goal_id: &str) -> PathBuf {
        self.plan_reviews_dir().join(goal_id)
    }

    pub fn events_dir(&self) -> PathBuf {
        self.root.join("events")
    }

    pub fn epochs_dir(&self) -> PathBuf {
        self.root.join("epochs")
    }

    pub fn budgets_dir(&self) -> PathBuf {
        self.root.join("budgets")
    }

    pub fn goal_budget_ledger_path(&self, goal_id: &str) -> PathBuf {
        self.budgets_dir().join(format!("{goal_id}.json"))
    }

    pub fn goal_epoch_path(&self, goal_id: &str) -> PathBuf {
        self.epochs_dir().join(format!("{goal_id}.jsonl"))
    }

    pub fn goal_run_lease_path(&self, goal_id: &str) -> PathBuf {
        self.epochs_dir().join(format!("{goal_id}.lease.json"))
    }

    pub fn artifacts_dir(&self) -> PathBuf {
        self.root.join("artifacts")
    }

    pub fn workers_dir(&self) -> PathBuf {
        self.root.join("workers")
    }

    pub fn continuation_dir(&self) -> PathBuf {
        self.root.join("continuation")
    }

    pub fn lineage_dir(&self) -> PathBuf {
        self.root.join("continuation").join("lineage")
    }

    pub fn lineage_path(&self, root_session_id: &str) -> PathBuf {
        self.lineage_dir().join(format!("{root_session_id}.json"))
    }

    pub fn write_lineage(&self, lineage: &WorkLineage) -> Result<PathBuf> {
        let path = self.lineage_path(&lineage.root_session_id);
        write_json(&path, lineage)?;
        Ok(path)
    }

    pub fn read_lineage(&self, root_session_id: &str) -> Result<Option<WorkLineage>> {
        let path = self.lineage_path(root_session_id);
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        Ok(Some(serde_json::from_str(&contents).with_context(
            || format!("failed to parse {}", path.display()),
        )?))
    }

    /// Per-session continuation state path: `.gearbox-agent/continuation/{session_id}/state.json`
    pub fn continuation_state_path_for_session(&self, session_id: &str) -> PathBuf {
        self.continuation_dir().join(session_id).join("state.json")
    }

    /// Read continuation state for a specific session
    pub fn read_continuation_state_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<ContinuationState>> {
        let path = self.continuation_state_path_for_session(session_id);
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        Ok(Some(serde_json::from_str(&contents).with_context(
            || format!("failed to parse {}", path.display()),
        )?))
    }

    /// Write continuation state — path is per-session
    pub fn write_continuation_state(
        &self,
        session_id: &str,
        goal_id: &str,
        status: ContinuationStatus,
    ) -> Result<PathBuf> {
        let state = ContinuationState {
            session_id: session_id.to_string(),
            goal_id: goal_id.to_string(),
            status,
            updated_at: timestamp(),
            parent_session_id: None,
            root_session_id: None,
        };
        let path = self.continuation_state_path_for_session(session_id);
        write_json(&path, &state)?;
        Ok(path)
    }

    /// Check if continuation is stopped for a specific session
    pub fn continuation_is_stopped_for_session(&self, session_id: &str) -> Result<bool> {
        Ok(self
            .read_continuation_state_for_session(session_id)?
            .is_some_and(|state| state.status == ContinuationStatus::Stopped))
    }

    /// Clear continuation stop for a specific session
    pub fn clear_continuation_stop_for_session(&self, session_id: &str) -> Result<()> {
        let path = self.continuation_state_path_for_session(session_id);
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to clear {}", path.display()))?;
            // Also remove the session directory if empty
            if let Some(parent) = path.parent() {
                fs::remove_dir(parent).ok();
            }
        }
        Ok(())
    }

    pub fn artifact_dir(&self, goal_id: &str) -> PathBuf {
        self.artifacts_dir().join(goal_id)
    }

    pub fn worker_dir(&self, task_id: &str) -> PathBuf {
        self.workers_dir().join(task_id)
    }

    pub fn phase_routes_dir(&self, goal_id: &str) -> PathBuf {
        self.artifact_dir(goal_id).join("phase-routes")
    }

    pub fn events_path(&self, session_id: &str) -> PathBuf {
        self.events_dir().join(format!("{session_id}.jsonl"))
    }

    pub fn write_session(&self, session: &Session) -> Result<PathBuf> {
        let path = self.sessions_dir().join(format!("{}.json", session.id));
        write_json(&path, session)?;
        Ok(path)
    }

    pub fn write_goal(&self, goal: &Goal) -> Result<PathBuf> {
        let path = self.goals_dir().join(format!("{}.json", goal.id));
        write_json(&path, goal)?;
        Ok(path)
    }

    pub fn write_tasks(&self, goal_id: &str, tasks: &[Task]) -> Result<PathBuf> {
        let path = self.tasks_dir().join(format!("{goal_id}.tasks.json"));
        write_json(&path, tasks)?;
        Ok(path)
    }

    pub fn read_tasks(&self, goal_id: &str) -> Result<Option<Vec<Task>>> {
        let path = self.tasks_dir().join(format!("{goal_id}.tasks.json"));
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        Ok(Some(serde_json::from_str(&contents).with_context(
            || format!("failed to parse {}", path.display()),
        )?))
    }

    pub fn write_plan_graph(&self, plan_graph: &crate::plan_graph::PlanGraph) -> Result<PathBuf> {
        plan_graph
            .validate()
            .context("refusing to persist an invalid PlanGraph")?;
        self.validate_plan_approval_bundle(plan_graph)
            .context("refusing to persist a PlanGraph without a valid approval bundle")?;
        let path = self
            .plans_dir()
            .join(format!("{}.plan.json", plan_graph.goal_id));
        write_json(&path, plan_graph)?;
        Ok(path)
    }

    pub fn write_unreviewed_plan_graph(
        &self,
        plan_graph: &crate::plan_graph::PlanGraph,
    ) -> Result<PathBuf> {
        plan_graph
            .validate()
            .context("refusing to persist an invalid unreviewed PlanGraph")?;
        let path = self
            .plans_dir()
            .join(format!("{}.unreviewed.plan.json", plan_graph.goal_id));
        write_json(&path, plan_graph)?;
        Ok(path)
    }

    pub fn write_plan_candidate(
        &self,
        plan_graph: &crate::plan_graph::PlanGraph,
    ) -> Result<PathBuf> {
        plan_graph
            .validate()
            .context("refusing to persist an invalid PlanGraph candidate")?;
        let path = self.plan_review_dir(&plan_graph.goal_id).join(format!(
            "revision-{:03}-{}.plan.json",
            plan_graph.revision,
            &plan_graph.plan_hash[..16]
        ));
        write_json(&path, plan_graph)?;
        Ok(path)
    }

    pub fn write_planner_execution_receipt(
        &self,
        receipt: &crate::plan_review::PlannerExecutionReceipt,
    ) -> Result<PathBuf> {
        let path = self.plan_review_dir(&receipt.goal_id).join(format!(
            "revision-{:03}-planner-receipt.json",
            receipt.plan_revision
        ));
        write_json(&path, receipt)?;
        Ok(path)
    }

    pub fn write_plan_approval_state(
        &self,
        state: &crate::plan_review::PlanApprovalState,
    ) -> Result<PathBuf> {
        state
            .validate()
            .context("refusing to persist an invalid plan approval state")?;
        let path = self.plan_review_dir(&state.goal_id).join("approval.json");
        write_json_atomic(&path, state)?;
        Ok(path)
    }

    pub fn read_plan_approval_state(
        &self,
        goal_id: &str,
    ) -> Result<Option<crate::plan_review::PlanApprovalState>> {
        let path = self.plan_review_dir(goal_id).join("approval.json");
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let state: crate::plan_review::PlanApprovalState = serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        state
            .validate()
            .with_context(|| format!("invalid plan approval state at {}", path.display()))?;
        Ok(Some(state))
    }

    pub fn write_plan_verifier_report(
        &self,
        report: &crate::plan_review::PlanVerifierReport,
    ) -> Result<PathBuf> {
        let path = self.plan_review_dir(&report.goal_id).join(format!(
            "revision-{:03}-verifier-report.json",
            report.plan_revision
        ));
        write_json(&path, report)?;
        Ok(path)
    }

    pub fn write_plan_critic_receipt(
        &self,
        receipt: &crate::plan_review::PlanCriticReceipt,
    ) -> Result<PathBuf> {
        let path = self.plan_review_dir(&receipt.goal_id).join(format!(
            "revision-{:03}-critic-receipt.json",
            receipt.plan_revision
        ));
        write_json(&path, receipt)?;
        Ok(path)
    }

    pub fn write_plan_review_text(
        &self,
        goal_id: &str,
        revision: usize,
        label: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let label = label.trim();
        if label.is_empty()
            || !label.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            bail!("plan review artifact label must be a non-empty ASCII identifier");
        }
        let path = self
            .plan_review_dir(goal_id)
            .join(format!("revision-{revision:03}-{label}.txt"));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }

    pub fn write_phase_route_table(
        &self,
        goal_id: &str,
        table: &crate::phase_routing::PhaseRouteTable,
    ) -> Result<PathBuf> {
        table
            .validate()
            .context("refusing to persist an invalid phase route table")?;
        let path = self.phase_routes_dir(goal_id).join("config.json");
        write_json(&path, table)?;
        Ok(path)
    }

    pub fn write_phase_route_decision(
        &self,
        goal_id: &str,
        ordinal: usize,
        decision: &crate::phase_routing::PhaseRouteDecision,
    ) -> Result<PathBuf> {
        self.validate_phase_route_decision(goal_id, decision)?;
        let path = self
            .phase_routes_dir(goal_id)
            .join(format!("{:03}-{:?}-decision.json", ordinal, decision.phase));
        write_json(&path, decision)?;
        Ok(path)
    }

    fn validate_phase_route_decision(
        &self,
        goal_id: &str,
        decision: &crate::phase_routing::PhaseRouteDecision,
    ) -> Result<()> {
        let table_path = self.phase_routes_dir(goal_id).join("config.json");
        let table: crate::phase_routing::PhaseRouteTable = read_json_file(&table_path)
            .with_context(|| format!("failed to load {}", table_path.display()))?;
        let profile = table.profile(&decision.phase)?;
        decision.validate_against(profile)
    }

    fn validate_phase_route_receipt_authority(
        &self,
        goal_id: &str,
        ordinal: usize,
        receipt: &crate::phase_routing::PhaseRouteReceipt,
    ) -> Result<()> {
        if receipt.ordinal != ordinal {
            bail!("phase route receipt ordinal does not match its storage path");
        }
        self.validate_phase_route_decision(goal_id, &receipt.decision)?;
        let decision_path = self.phase_routes_dir(goal_id).join(format!(
            "{:03}-{:?}-decision.json",
            ordinal, receipt.decision.phase
        ));
        let persisted: crate::phase_routing::PhaseRouteDecision = read_json_file(&decision_path)
            .with_context(|| {
                format!(
                    "phase route receipt is missing its persisted decision at {}",
                    decision_path.display()
                )
            })?;
        if persisted != receipt.decision || persisted.hash()? != receipt.decision_hash {
            bail!("phase route receipt does not match its persisted route decision");
        }
        Ok(())
    }

    fn validate_phase_route_receipt_plan(
        &self,
        receipt: &crate::phase_routing::PhaseRouteReceipt,
    ) -> Result<()> {
        let goal_id = receipt
            .goal_id
            .as_deref()
            .context("phase route receipt is missing its goal id")?;
        let plan_id = receipt
            .plan_id
            .as_deref()
            .context("phase route receipt is missing its plan id")?;
        let plan_hash = receipt
            .plan_hash
            .as_deref()
            .context("phase route receipt is missing its plan hash")?;
        let candidate_path = self.plan_review_dir(goal_id).join(format!(
            "revision-{:03}-{}.plan.json",
            receipt.plan_revision,
            &plan_hash[..16]
        ));
        let paths = [
            candidate_path,
            self.plans_dir().join(format!("{goal_id}.plan.json")),
            self.plans_dir()
                .join(format!("{goal_id}.unreviewed.plan.json")),
        ];
        for path in paths.iter().filter(|path| path.exists()) {
            let plan: crate::plan_graph::PlanGraph = read_json_file(path)?;
            plan.validate()?;
            if plan.goal_id == goal_id
                && plan.plan_id == plan_id
                && plan.revision == receipt.plan_revision
                && plan.plan_hash == plan_hash
            {
                return Ok(());
            }
        }
        bail!("phase route receipt does not match a persisted PlanGraph revision")
    }

    pub fn write_phase_route_receipt(
        &self,
        goal_id: &str,
        ordinal: usize,
        receipt: &crate::phase_routing::PhaseRouteReceipt,
    ) -> Result<PathBuf> {
        receipt
            .validate()
            .context("refusing to persist an invalid phase route receipt")?;
        if receipt.goal_id.as_deref() != Some(goal_id) {
            bail!("phase route receipt goal does not match its storage path");
        }
        self.validate_phase_route_receipt_authority(goal_id, ordinal, receipt)?;
        self.validate_phase_route_receipt_plan(receipt)?;
        self.validate_phase_route_receipt_evidence(receipt)?;
        let path = self.phase_routes_dir(goal_id).join(format!(
            "{:03}-{:?}-receipt.json",
            ordinal, receipt.decision.phase
        ));
        write_json(&path, receipt)?;
        Ok(path)
    }

    pub fn read_phase_route_receipt(
        &self,
        goal_id: &str,
        ordinal: usize,
        phase: &crate::plan_graph::PhaseProfile,
    ) -> Result<Option<crate::phase_routing::PhaseRouteReceipt>> {
        let path = self
            .phase_routes_dir(goal_id)
            .join(format!("{ordinal:03}-{phase:?}-receipt.json"));
        if !path.exists() {
            return Ok(None);
        }
        let receipt: crate::phase_routing::PhaseRouteReceipt = read_json_file(&path)?;
        receipt
            .validate()
            .context("persisted phase route receipt failed integrity validation")?;
        if receipt.goal_id.as_deref() != Some(goal_id)
            || receipt.ordinal != ordinal
            || &receipt.decision.phase != phase
        {
            bail!("phase route receipt path identity does not match its contents");
        }
        self.validate_phase_route_receipt_authority(goal_id, ordinal, &receipt)?;
        self.validate_phase_route_receipt_plan(&receipt)?;
        self.validate_phase_route_receipt_evidence(&receipt)?;
        Ok(Some(receipt))
    }

    pub fn validate_phase_route_receipt_evidence(
        &self,
        receipt: &crate::phase_routing::PhaseRouteReceipt,
    ) -> Result<()> {
        receipt.validate()?;
        let Some(task_id) = receipt.task_id.as_deref() else {
            return Ok(());
        };
        let goal_id = receipt
            .goal_id
            .as_deref()
            .context("worker phase receipt is missing goal id")?;
        let evidence_path = PathBuf::from(
            receipt
                .task_record_path
                .as_deref()
                .context("worker phase receipt is missing task record path")?,
        );
        let expected_path = self
            .phase_routes_dir(goal_id)
            .join("worker-evidence")
            .join(format!("{task_id}-task-record.json"));
        if evidence_path != expected_path {
            bail!("worker phase task-record evidence path does not match its task identity");
        }
        let canonical_root = self
            .phase_routes_dir(goal_id)
            .canonicalize()
            .context("failed to canonicalize phase route evidence root")?;
        let canonical_goal_root = self
            .artifact_dir(goal_id)
            .canonicalize()
            .context("failed to canonicalize goal artifact root")?;
        if !canonical_root.starts_with(&canonical_goal_root) {
            bail!("phase route evidence root escaped its goal artifact directory");
        }
        let canonical_evidence = evidence_path
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", evidence_path.display()))?;
        if !canonical_evidence.starts_with(canonical_root) {
            bail!("worker phase task-record evidence is outside its goal route directory");
        }
        let evidence = fs::read(&evidence_path)
            .with_context(|| format!("failed to read {}", evidence_path.display()))?;
        let evidence_hash = format!("{:x}", Sha256::digest(&evidence));
        if receipt.task_record_sha256.as_deref() != Some(evidence_hash.as_str()) {
            bail!("worker phase task-record evidence hash mismatch");
        }
        let record: crate::task_manager::TaskRecord = serde_json::from_slice(&evidence)
            .with_context(|| format!("failed to parse {}", evidence_path.display()))?;
        if record.task_id != task_id {
            bail!("worker phase task-record evidence belongs to another task");
        }
        let last_attempt = record
            .attempts
            .last()
            .context("worker phase task-record evidence has no attempts")?;
        if receipt.actual_worker_kind.map(|kind| kind.as_str())
            != Some(last_attempt.worker_kind.as_str())
            || receipt.actual_category.map(|category| category.as_str())
                != Some(last_attempt.worker_category.as_str())
            || receipt.actual_worker_model.as_deref() != last_attempt.worker_model.as_deref()
            || receipt.actual_route_reason.as_deref() != Some(last_attempt.route_reason.as_str())
        {
            bail!("worker phase receipt does not match its task-record attempt evidence");
        }
        if receipt.worker_session_id.as_deref() != record.session_id.as_deref()
            || (last_attempt.session_id.is_some()
                && receipt.worker_session_id.as_deref() != last_attempt.session_id.as_deref())
        {
            bail!("worker phase receipt session does not match task-record evidence");
        }
        Ok(())
    }

    pub fn read_plan_graph(&self, goal_id: &str) -> Result<Option<crate::plan_graph::PlanGraph>> {
        let path = self.plans_dir().join(format!("{goal_id}.plan.json"));
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let plan_graph: crate::plan_graph::PlanGraph = serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        plan_graph
            .validate()
            .with_context(|| format!("invalid persisted PlanGraph at {}", path.display()))?;
        self.validate_plan_approval_bundle(&plan_graph)
            .with_context(|| format!("invalid approval bundle for {}", path.display()))?;
        Ok(Some(plan_graph))
    }

    pub fn read_unreviewed_plan_graph(
        &self,
        goal_id: &str,
    ) -> Result<Option<crate::plan_graph::PlanGraph>> {
        let path = self
            .plans_dir()
            .join(format!("{goal_id}.unreviewed.plan.json"));
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let plan_graph: crate::plan_graph::PlanGraph = serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        plan_graph
            .validate()
            .with_context(|| format!("invalid persisted PlanGraph at {}", path.display()))?;
        Ok(Some(plan_graph))
    }

    pub fn validate_plan_approval_bundle(
        &self,
        plan_graph: &crate::plan_graph::PlanGraph,
    ) -> Result<()> {
        let approval = self
            .read_plan_approval_state(&plan_graph.goal_id)?
            .context("approved PlanGraph is missing approval.json")?;
        approval.validate_against(plan_graph)?;
        let review_dir = self.plan_review_dir(&plan_graph.goal_id);
        let revision = plan_graph.revision;
        let planner_raw_output = fs::read_to_string(
            review_dir.join(format!("revision-{revision:03}-planner-output.txt")),
        )
        .context("approved PlanGraph is missing planner raw output")?;
        let planner_receipt: crate::plan_review::PlannerExecutionReceipt = read_json_file(
            &review_dir.join(format!("revision-{revision:03}-planner-receipt.json")),
        )?;
        let verifier: crate::plan_review::PlanVerifierReport = read_json_file(
            &review_dir.join(format!("revision-{revision:03}-verifier-report.json")),
        )?;
        let critic_raw_output = fs::read_to_string(
            review_dir.join(format!("revision-{revision:03}-critic-output.txt")),
        )
        .context("approved PlanGraph is missing PlanCritic raw output")?;
        let critic_receipt: crate::plan_review::PlanCriticReceipt = read_json_file(
            &review_dir.join(format!("revision-{revision:03}-critic-receipt.json")),
        )?;

        planner_receipt.validate(plan_graph, &planner_raw_output)?;
        verifier.validate(plan_graph)?;
        critic_receipt.validate(
            plan_graph,
            &planner_receipt,
            &planner_raw_output,
            &verifier,
            &critic_raw_output,
        )?;
        if !critic_receipt.approved() {
            bail!("canonical PlanGraph requires an approving PlanCritic receipt");
        }
        if approval.planner_receipt_hash != planner_receipt.receipt_hash
            || approval.verifier_report_hash != verifier.report_hash
            || approval.critic_receipt_hash.as_deref() != Some(critic_receipt.receipt_hash.as_str())
        {
            bail!("approval manifest does not match its persisted receipt chain");
        }
        Ok(())
    }

    pub fn append_event(&self, event: &Event) -> Result<PathBuf> {
        let path = self.events_path(&event.session_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let line = serde_json::to_string(event).context("failed to serialize event")?;
        writeln!(file, "{line}").with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }

    pub fn append_goal_epoch_event(
        &self,
        goal_id: &str,
        epoch_id: &str,
        idempotency_key: &str,
        kind: GoalEpochEventKind,
        payload: Value,
    ) -> Result<GoalEpochEvent> {
        let existing = self.read_goal_epoch_events(goal_id)?;
        if let Some(recorded) = existing
            .iter()
            .find(|event| event.idempotency_key == idempotency_key)
        {
            if recorded.epoch_id == epoch_id && recorded.kind == kind && recorded.payload == payload
            {
                return Ok(recorded.clone());
            }
            bail!("goal epoch idempotency key conflicts with an existing event");
        }
        let previous_hash = existing
            .last()
            .map(|event| event.event_hash.clone())
            .unwrap_or_else(|| "0".repeat(64));
        let event = GoalEpochEvent::seal(
            goal_id,
            epoch_id,
            existing.len() as u64,
            idempotency_key,
            kind,
            payload,
            previous_hash,
        )?;
        let mut active_epoch = None;
        for existing_event in &existing {
            validate_goal_epoch_transition(&mut active_epoch, existing_event)?;
        }
        validate_goal_epoch_transition(&mut active_epoch, &event)?;
        let path = self.goal_epoch_path(goal_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        writeln!(file, "{}", serde_json::to_string(&event)?)
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync {}", path.display()))?;
        Ok(event)
    }

    pub fn read_goal_budget_ledger(&self, goal_id: &str) -> Result<GoalBudgetLedger> {
        let path = self.goal_budget_ledger_path(goal_id);
        if !path.exists() {
            return Ok(GoalBudgetLedger {
                schema_version: 1,
                goal_id: goal_id.to_string(),
                reservations: Vec::new(),
                updated_at: timestamp(),
                ledger_hash: String::new(),
            });
        }
        let ledger: GoalBudgetLedger = read_json_file(&path)?;
        ledger.validate(goal_id)?;
        Ok(ledger)
    }

    pub fn reserve_budget_call(
        &self,
        lease: &GoalRunLeaseGuard,
        reservation_id: &str,
        phase: &str,
        worker_call: bool,
        premium: bool,
        budget: &Budget,
    ) -> Result<BudgetReservation> {
        for (field, value) in [
            ("goal_id", lease.lease.goal_id.as_str()),
            ("epoch_id", lease.lease.epoch_id.as_str()),
            ("reservation_id", reservation_id),
            ("phase", phase),
        ] {
            if value.trim().is_empty() {
                bail!("budget reservation requires non-empty {field}");
            }
        }
        if budget.max_tokens_per_call == 0 {
            bail!("budget max_tokens_per_call must be greater than zero");
        }
        let goal_id = lease.lease.goal_id.as_str();
        let epoch_id = lease.lease.epoch_id.as_str();
        let mut ledger = self.read_goal_budget_ledger(goal_id)?;
        let (calls, worker_calls, premium_calls, tokens, cost, unknown_calls) =
            budget_ledger_totals(&ledger, epoch_id);
        if let Some(existing) = ledger
            .reservations
            .iter()
            .find(|reservation| reservation.reservation_id == reservation_id)
        {
            if existing.epoch_id == epoch_id
                && existing.phase == phase
                && existing.worker_call == worker_call
                && existing.premium == premium
            {
                return Ok(existing.clone());
            }
            bail!("budget reservation id conflicts with an existing reservation");
        }
        if calls >= budget.max_calls_per_epoch {
            bail!("epoch call budget exhausted before reservation");
        }
        if worker_call && worker_calls >= budget.max_worker_calls {
            bail!("worker call budget exhausted before reservation");
        }
        if premium && premium_calls >= budget.max_premium_worker_calls {
            bail!("premium worker call budget exhausted before reservation");
        }
        if unknown_calls >= budget.max_usage_unknown_calls {
            bail!("usage-unknown call budget exhausted before reservation");
        }
        if tokens.saturating_add(budget.max_tokens_per_call) > budget.max_tokens_per_epoch {
            bail!("epoch token budget exhausted before reservation");
        }
        let reserved_cost_micros = budget.max_cost_micros_per_epoch.saturating_sub(cost);
        if budget.max_cost_micros_per_epoch != u64::MAX && reserved_cost_micros == 0 {
            bail!("epoch cost budget exhausted before reservation");
        }
        let reservation = BudgetReservation {
            reservation_id: reservation_id.to_string(),
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            phase: phase.to_string(),
            worker_call,
            premium,
            reserved_tokens: budget.max_tokens_per_call,
            reserved_cost_micros,
            status: BudgetReservationStatus::Reserved,
            usage: None,
            created_at: timestamp(),
            settled_at: None,
        };
        ledger.reservations.push(reservation.clone());
        ledger.updated_at = timestamp();
        self.write_goal_budget_ledger(ledger)?;
        Ok(reservation)
    }

    pub fn settle_budget_call(
        &self,
        lease: &GoalRunLeaseGuard,
        reservation_id: &str,
        usage: SettledBudgetUsage,
    ) -> Result<BudgetReservation> {
        if usage.is_unknown()
            && usage
                .unavailable_reason
                .as_deref()
                .is_none_or(|reason| reason.trim().is_empty())
        {
            bail!("unknown budget usage requires an unavailable reason");
        }
        let goal_id = lease.lease.goal_id.as_str();
        let mut ledger = self.read_goal_budget_ledger(goal_id)?;
        let reservation = ledger
            .reservations
            .iter_mut()
            .find(|reservation| reservation.reservation_id == reservation_id)
            .context("budget settlement references an unknown reservation")?;
        if reservation.status == BudgetReservationStatus::Settled {
            if reservation.usage.as_ref() == Some(&usage) {
                return Ok(reservation.clone());
            }
            bail!("budget reservation was already settled with different usage");
        }
        if reservation.status != BudgetReservationStatus::Reserved {
            bail!("only a reserved budget call can be settled");
        }
        if usage
            .total_tokens()
            .is_some_and(|tokens| tokens > reservation.reserved_tokens)
        {
            bail!("settled token usage exceeds the reservation");
        }
        if usage
            .cost_micros
            .is_some_and(|cost| cost > reservation.reserved_cost_micros)
        {
            bail!("settled cost exceeds the reservation");
        }
        reservation.status = BudgetReservationStatus::Settled;
        reservation.usage = Some(usage);
        reservation.settled_at = Some(timestamp());
        let settled = reservation.clone();
        ledger.updated_at = timestamp();
        self.write_goal_budget_ledger(ledger)?;
        Ok(settled)
    }

    fn write_goal_budget_ledger(&self, ledger: GoalBudgetLedger) -> Result<()> {
        let goal_id = ledger.goal_id.clone();
        let ledger = ledger.seal()?;
        ledger.validate(&goal_id)?;
        write_json_atomic(&self.goal_budget_ledger_path(&goal_id), &ledger)
    }

    pub fn read_goal_epoch_events(&self, goal_id: &str) -> Result<Vec<GoalEpochEvent>> {
        let path = self.goal_epoch_path(goal_id);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut events = Vec::new();
        let mut previous_hash = "0".repeat(64);
        let mut active_epoch = None;
        let mut idempotency_keys = HashSet::new();
        for (sequence, line) in contents.lines().enumerate() {
            let event: GoalEpochEvent = serde_json::from_str(line).with_context(|| {
                format!("failed to parse {} line {}", path.display(), sequence + 1)
            })?;
            if event.schema_version != 1
                || event.goal_id != goal_id
                || event.sequence != sequence as u64
                || event.idempotency_key.trim().is_empty()
                || !idempotency_keys.insert(event.idempotency_key.clone())
                || event.previous_hash != previous_hash
                || event.event_hash != event.expected_hash()?
            {
                bail!("goal epoch ledger integrity check failed at sequence {sequence}");
            }
            validate_goal_epoch_transition(&mut active_epoch, &event)?;
            previous_hash = event.event_hash.clone();
            events.push(event);
        }
        Ok(events)
    }

    pub fn abort_incomplete_goal_epoch(
        &self,
        goal_id: &str,
        reason: &str,
    ) -> Result<Option<GoalEpochEvent>> {
        if reason.trim().is_empty() {
            bail!("incomplete goal epoch abort requires a reason");
        }
        let events = self.read_goal_epoch_events(goal_id)?;
        let mut active_epoch = None;
        for event in &events {
            validate_goal_epoch_transition(&mut active_epoch, event)?;
        }
        let Some(epoch_id) = active_epoch else {
            return Ok(None);
        };
        let event = self.append_goal_epoch_event(
            goal_id,
            &epoch_id,
            &format!("recovery.{epoch_id}.aborted"),
            GoalEpochEventKind::Aborted,
            serde_json::json!({ "reason": reason }),
        )?;
        Ok(Some(event))
    }

    pub fn acquire_goal_run_lease(
        &self,
        goal_id: &str,
        epoch_id: &str,
        owner_session_id: &str,
        duration: std::time::Duration,
    ) -> Result<GoalRunLeaseGuard> {
        if duration.is_zero() {
            bail!("goal run lease duration must be greater than zero");
        }
        let duration =
            Duration::from_std(duration).context("goal run lease duration is too large")?;
        let path = self.goal_run_lease_path(goal_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to open lease {}", path.display()))?;
        if let Err(error) = file.try_lock() {
            let active = read_json_file::<GoalRunLease>(&path).ok();
            let owner = active
                .as_ref()
                .map(|lease| lease.owner_session_id.as_str())
                .unwrap_or("unknown");
            bail!("goal {goal_id} is already leased by session {owner}: {error}");
        }

        let now = Local::now();
        let lease = GoalRunLease {
            schema_version: 1,
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            owner_session_id: owner_session_id.to_string(),
            lease_id: format!("lease_{}", id_timestamp()),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + duration).to_rfc3339(),
        };
        lease.validate(goal_id)?;
        file.set_len(0)
            .with_context(|| format!("failed to truncate {}", path.display()))?;
        let contents = serde_json::to_string_pretty(&lease)?;
        file.write_all(format!("{contents}\n").as_bytes())
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync {}", path.display()))?;
        Ok(GoalRunLeaseGuard { lease, file, path })
    }

    pub fn write_artifact(
        &self,
        goal_id: &str,
        file_name: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let dir = self.artifact_dir(goal_id);
        fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
        let path = dir.join(file_name);
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }

    pub fn write_worker_file(
        &self,
        task_id: &str,
        file_name: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let dir = self.worker_dir(task_id);
        fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
        let path = dir.join(file_name);
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }

    pub fn append_worker_file(
        &self,
        task_id: &str,
        file_name: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let dir = self.worker_dir(task_id);
        fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
        let path = dir.join(file_name);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }
}

fn budget_ledger_totals(
    ledger: &GoalBudgetLedger,
    epoch_id: &str,
) -> (usize, usize, usize, u64, u64, usize) {
    let mut calls = 0usize;
    let mut worker_calls = 0usize;
    let mut premium_calls = 0usize;
    let mut tokens = 0u64;
    let mut cost = 0u64;
    let mut unknown_calls = 0usize;
    for reservation in ledger.reservations.iter().filter(|reservation| {
        reservation.epoch_id == epoch_id && reservation.status != BudgetReservationStatus::Released
    }) {
        calls = calls.saturating_add(1);
        worker_calls = worker_calls.saturating_add(usize::from(reservation.worker_call));
        premium_calls = premium_calls.saturating_add(usize::from(reservation.premium));
        match reservation.usage.as_ref() {
            Some(usage) => {
                tokens = tokens
                    .saturating_add(usage.total_tokens().unwrap_or(reservation.reserved_tokens));
                cost = cost.saturating_add(
                    usage
                        .cost_micros
                        .unwrap_or(reservation.reserved_cost_micros),
                );
                unknown_calls = unknown_calls.saturating_add(usize::from(usage.is_unknown()));
            }
            None => {
                tokens = tokens.saturating_add(reservation.reserved_tokens);
                cost = cost.saturating_add(reservation.reserved_cost_micros);
            }
        }
    }
    (
        calls,
        worker_calls,
        premium_calls,
        tokens,
        cost,
        unknown_calls,
    )
}

fn validate_goal_epoch_transition(
    active_epoch: &mut Option<String>,
    event: &GoalEpochEvent,
) -> Result<()> {
    match event.kind {
        GoalEpochEventKind::Started => {
            if let Some(active_epoch) = active_epoch.as_deref() {
                bail!(
                    "cannot start epoch {} while epoch {active_epoch} is active",
                    event.epoch_id
                );
            }
            *active_epoch = Some(event.epoch_id.clone());
        }
        GoalEpochEventKind::BudgetReserved
        | GoalEpochEventKind::BudgetSettled
        | GoalEpochEventKind::NextGoalSelected
        | GoalEpochEventKind::PhaseCompleted => {
            if active_epoch.as_deref() != Some(event.epoch_id.as_str()) {
                bail!("phase completion is not bound to the active goal epoch");
            }
        }
        GoalEpochEventKind::Settled | GoalEpochEventKind::Aborted => {
            if active_epoch.as_deref() != Some(event.epoch_id.as_str()) {
                bail!("terminal event is not bound to the active goal epoch");
            }
            *active_epoch = None;
        }
    }
    Ok(())
}

pub fn event(
    session_id: &str,
    goal_id: Option<&str>,
    task_id: Option<&str>,
    kind: EventKind,
    message: impl Into<String>,
    data: Value,
) -> Event {
    Event {
        ts: timestamp(),
        session_id: session_id.to_string(),
        goal_id: goal_id.map(ToOwned::to_owned),
        task_id: task_id.map(ToOwned::to_owned),
        kind,
        message: message.into(),
        data,
    }
}

fn read_json_file<T>(path: &Path) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&contents).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn write_json<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize + ?Sized,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let contents = serde_json::to_string_pretty(value).context("failed to serialize json")?;
    fs::write(path, format!("{contents}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn write_json_atomic<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize + ?Sized,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let contents = serde_json::to_string_pretty(value).context("failed to serialize json")?;
    let temporary_path = path.with_extension(format!("tmp-{}", id_timestamp()));
    fs::write(&temporary_path, format!("{contents}\n"))
        .with_context(|| format!("failed to write {}", temporary_path.display()))?;
    fs::rename(&temporary_path, path).with_context(|| {
        format!(
            "failed to atomically replace {} with {}",
            path.display(),
            temporary_path.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod epoch_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn goal_epoch_ledger_is_ordered_and_hash_chained() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let started = store.append_goal_epoch_event(
            "goal-1",
            "epoch-1",
            "epoch-1.started",
            GoalEpochEventKind::Started,
            json!({ "plan_revision": 1 }),
        )?;
        let replay = store.append_goal_epoch_event(
            "goal-1",
            "epoch-1",
            "epoch-1.started",
            GoalEpochEventKind::Started,
            json!({ "plan_revision": 1 }),
        )?;
        assert_eq!(replay.event_hash, started.event_hash);
        assert_eq!(store.read_goal_epoch_events("goal-1")?.len(), 1);
        assert!(
            store
                .append_goal_epoch_event(
                    "goal-1",
                    "epoch-1",
                    "epoch-1.started",
                    GoalEpochEventKind::Started,
                    json!({ "plan_revision": 2 }),
                )
                .is_err()
        );
        let settled = store.append_goal_epoch_event(
            "goal-1",
            "epoch-1",
            "epoch-1.settled",
            GoalEpochEventKind::Settled,
            json!({ "outcome": "review_required" }),
        )?;

        assert_eq!(started.sequence, 0);
        assert_eq!(settled.sequence, 1);
        assert_eq!(settled.previous_hash, started.event_hash);
        assert_eq!(store.read_goal_epoch_events("goal-1")?.len(), 2);

        let path = store.goal_epoch_path("goal-1");
        let contents = fs::read_to_string(&path)?;
        fs::write(&path, contents.replace("review_required", "complete"))?;
        assert!(store.read_goal_epoch_events("goal-1").is_err());
        Ok(())
    }

    #[test]
    fn goal_run_lease_excludes_concurrent_epochs_and_releases_on_drop() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let duration = std::time::Duration::from_secs(60);

        let first = store.acquire_goal_run_lease("goal-1", "epoch-1", "session-1", duration)?;
        assert_eq!(first.lease().epoch_id, "epoch-1");
        store.append_goal_epoch_event(
            "goal-1",
            "epoch-1",
            "epoch-1.started",
            GoalEpochEventKind::Started,
            json!({ "session_id": "session-1" }),
        )?;
        assert!(
            store
                .acquire_goal_run_lease("goal-1", "epoch-2", "session-2", duration)
                .is_err()
        );

        drop(first);
        let second = store.acquire_goal_run_lease("goal-1", "epoch-2", "session-2", duration)?;
        assert_eq!(second.lease().owner_session_id, "session-2");
        let aborted = store
            .abort_incomplete_goal_epoch("goal-1", "simulated process crash")?
            .context("incomplete epoch should be aborted")?;
        assert_eq!(aborted.epoch_id, "epoch-1");
        store.append_goal_epoch_event(
            "goal-1",
            "epoch-2",
            "epoch-2.started",
            GoalEpochEventKind::Started,
            json!({ "session_id": "session-2" }),
        )?;
        second.release()?;
        Ok(())
    }

    #[test]
    fn budget_ledger_reserves_before_dispatch_and_settles_actual_usage() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let budget = Budget {
            max_worker_calls: 4,
            max_premium_worker_calls: 1,
            max_tokens_per_call: 60,
            max_tokens_per_epoch: 100,
            max_cost_micros_per_epoch: 100,
            max_usage_unknown_calls: 1,
            ..Budget::default()
        };
        let lease = store.acquire_goal_run_lease(
            "goal-1",
            "epoch-1",
            "session-1",
            std::time::Duration::from_secs(60),
        )?;

        let first =
            store.reserve_budget_call(&lease, "epoch-1.worker.1", "worker", true, true, &budget)?;
        assert_eq!(first.reserved_tokens, 60);
        assert_eq!(first.reserved_cost_micros, 100);
        let replay =
            store.reserve_budget_call(&lease, "epoch-1.worker.1", "worker", true, true, &budget)?;
        assert_eq!(replay, first);
        store.settle_budget_call(
            &lease,
            "epoch-1.worker.1",
            SettledBudgetUsage {
                requested_tokens: Some(10),
                actual_tokens: Some(10),
                cost_micros: Some(30),
                duration_ms: Some(50),
                cache_hit: Some(false),
                unavailable_reason: None,
            },
        )?;

        let second = store.reserve_budget_call(
            &lease,
            "epoch-1.worker.2",
            "worker",
            true,
            false,
            &budget,
        )?;
        assert_eq!(second.reserved_cost_micros, 70);
        store.settle_budget_call(
            &lease,
            "epoch-1.worker.2",
            SettledBudgetUsage {
                requested_tokens: None,
                actual_tokens: None,
                cost_micros: None,
                duration_ms: None,
                cache_hit: None,
                unavailable_reason: Some("backend omitted usage".to_string()),
            },
        )?;
        assert!(
            store
                .reserve_budget_call(&lease, "epoch-1.worker.3", "worker", true, false, &budget,)
                .is_err()
        );
        let ledger_path = store.goal_budget_ledger_path("goal-1");
        let ledger_contents = fs::read_to_string(&ledger_path)?;
        fs::write(
            &ledger_path,
            ledger_contents.replace("\"cost_micros\": 30", "\"cost_micros\": 31"),
        )?;
        assert!(store.read_goal_budget_ledger("goal-1").is_err());
        lease.release()?;
        Ok(())
    }

    #[test]
    fn epoch_call_budget_does_not_consume_worker_call_budget() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let lease = store.acquire_goal_run_lease(
            "goal-2",
            "epoch-2",
            "session-2",
            std::time::Duration::from_secs(60),
        )?;
        let budget = Budget {
            max_calls_per_epoch: 2,
            max_worker_calls: 0,
            max_tokens_per_call: 1,
            max_tokens_per_epoch: 4,
            max_usage_unknown_calls: 4,
            ..Budget::default()
        };
        for phase in ["planner", "plan-critic"] {
            let reservation_id = format!("epoch-2.{phase}");
            store.reserve_budget_call(&lease, &reservation_id, phase, false, false, &budget)?;
            store.settle_budget_call(
                &lease,
                &reservation_id,
                SettledBudgetUsage {
                    requested_tokens: Some(1),
                    actual_tokens: Some(0),
                    cost_micros: Some(0),
                    duration_ms: Some(1),
                    cache_hit: Some(false),
                    unavailable_reason: None,
                },
            )?;
        }
        assert!(
            store
                .reserve_budget_call(
                    &lease,
                    "epoch-2.reviewer",
                    "reviewer",
                    false,
                    false,
                    &budget,
                )
                .is_err()
        );
        lease.release()?;
        Ok(())
    }
}
