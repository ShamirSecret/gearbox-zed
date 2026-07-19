use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
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

pub const GLOBAL_PROVIDER_COOLDOWN_SCHEMA_VERSION: u32 = 1;

/// Durable provider-wide cooldown for free-tier quota failures. This is kept
/// outside a goal so a new run cannot immediately retry an exhausted quota.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalProviderCooldown {
    pub schema_version: u32,
    pub provider_scope: String,
    pub failed_models: Vec<String>,
    pub reason: String,
    pub failed_at: String,
    pub cooldown_until_ms: u64,
    pub source_task: String,
    pub source_attempt: usize,
    pub recorded_at: String,
    pub receipt_hash: String,
}

impl GlobalProviderCooldown {
    pub fn seal(mut self) -> Result<Self> {
        self.receipt_hash.clear();
        self.validate_payload()?;
        self.receipt_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        if self.receipt_hash != self.expected_hash()? {
            bail!("global provider cooldown receipt hash mismatch");
        }
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        let now_ms = u64::try_from(Local::now().timestamp_millis()).unwrap_or(0);
        self.cooldown_until_ms > now_ms
    }

    fn validate_payload(&self) -> Result<()> {
        if self.schema_version != GLOBAL_PROVIDER_COOLDOWN_SCHEMA_VERSION {
            bail!("unsupported global provider cooldown schema version");
        }
        for (field, value) in [
            ("provider_scope", self.provider_scope.as_str()),
            ("reason", self.reason.as_str()),
            ("failed_at", self.failed_at.as_str()),
            ("source_task", self.source_task.as_str()),
            ("recorded_at", self.recorded_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("global provider cooldown {field} cannot be empty");
            }
        }
        if self.failed_models.is_empty() || self.cooldown_until_ms == 0 {
            bail!("global provider cooldown must contain models and an expiry");
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.receipt_hash.clear();
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&unsigned)?)))
    }
}

// Leave room for the revision/role separators and the atomic temporary suffix
// in the single filename. The old 64-byte components could make a repair
// observation filename exceed NAME_MAX when both task and session identities
// were long.
const REPOSITORY_OBSERVATION_PATH_COMPONENT_LIMIT: usize = 32;
const LEGACY_REPOSITORY_OBSERVATION_PATH_COMPONENT_LIMIT: usize = 64;

fn repository_observation_path_component(value: &str) -> String {
    repository_observation_path_component_with_limit(
        value,
        REPOSITORY_OBSERVATION_PATH_COMPONENT_LIMIT,
    )
}

fn legacy_repository_observation_path_component(value: &str) -> String {
    repository_observation_path_component_with_limit(
        value,
        LEGACY_REPOSITORY_OBSERVATION_PATH_COMPONENT_LIMIT,
    )
}

fn repository_observation_path_component_with_limit(value: &str, limit: usize) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.len() <= limit {
        return sanitized;
    }

    // Keep long task/session identities distinguishable without exceeding filesystem limits.
    let digest = format!("{:x}", Sha256::digest(value.as_bytes()));
    let suffix = &digest[..16];
    let prefix_length = limit - suffix.len() - 1;
    let prefix = sanitized.chars().take(prefix_length).collect::<String>();
    format!("{prefix}-{suffix}")
}

fn worker_fanout_session_path_component(value: &str) -> String {
    let sanitized = repository_observation_path_component(value);
    let digest = format!("{:x}", Sha256::digest(value.as_bytes()));
    format!("{sanitized}-{}", &digest[..16])
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

pub const MAX_CONTINUATION_AUTO_RESUMES: usize = 2;

pub const CONTINUATION_GUARD_SCHEMA_VERSION: u32 = 1;

/// Durable equivalent of OMO's in-memory idle-continuation guard.
///
/// The runtime may be restarted between two provider events. Persisting the
/// guard makes a restart explainable and prevents a stale idle event from
/// dispatching through a context that was already cancelled, compacted, or
/// waiting for a user/background task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationGuardState {
    pub schema_version: u32,
    pub session_id: String,
    pub goal_id: String,
    pub epoch_id: String,
    #[serde(default)]
    pub all_todos_completed: bool,
    #[serde(default)]
    pub is_recovering: bool,
    #[serde(default)]
    pub was_cancelled: bool,
    #[serde(default)]
    pub token_limit_detected: bool,
    #[serde(default)]
    pub context_pressure: bool,
    #[serde(default)]
    pub compaction_pending: bool,
    #[serde(default)]
    pub background_pending: bool,
    #[serde(default)]
    pub pending_question: bool,
    #[serde(default)]
    pub pending_internal_continuation: bool,
    #[serde(default)]
    pub in_flight: bool,
    #[serde(default)]
    pub consecutive_failures: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_until: Option<String>,
    #[serde(default)]
    pub stagnation_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_progress_marker: Option<String>,
    pub updated_at: String,
    pub guard_hash: String,
}

impl ContinuationGuardState {
    pub fn new(
        session_id: impl Into<String>,
        goal_id: impl Into<String>,
        epoch_id: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: CONTINUATION_GUARD_SCHEMA_VERSION,
            session_id: session_id.into(),
            goal_id: goal_id.into(),
            epoch_id: epoch_id.into(),
            all_todos_completed: false,
            is_recovering: false,
            was_cancelled: false,
            token_limit_detected: false,
            context_pressure: false,
            compaction_pending: false,
            background_pending: false,
            pending_question: false,
            pending_internal_continuation: false,
            in_flight: false,
            consecutive_failures: 0,
            cooldown_until: None,
            stagnation_count: 0,
            last_progress_marker: None,
            updated_at: timestamp(),
            guard_hash: String::new(),
        }
    }

    fn expected_hash(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.guard_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&unsigned)?)
        ))
    }

    fn validate_payload(&self) -> Result<()> {
        if self.schema_version != CONTINUATION_GUARD_SCHEMA_VERSION {
            bail!("unsupported continuation guard schema version");
        }
        for (field, value) in [
            ("session_id", self.session_id.as_str()),
            ("goal_id", self.goal_id.as_str()),
            ("epoch_id", self.epoch_id.as_str()),
            ("updated_at", self.updated_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("continuation guard {field} cannot be empty");
            }
        }
        Ok(())
    }

    pub fn seal(mut self) -> Result<Self> {
        self.guard_hash.clear();
        self.validate_payload()?;
        self.guard_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        if self.guard_hash != self.expected_hash()? {
            bail!("continuation guard integrity hash mismatch");
        }
        Ok(())
    }

    pub fn blocking_reason(&self) -> Option<&'static str> {
        if self.all_todos_completed {
            return Some("all todos completed");
        }
        if self.is_recovering {
            return Some("session is recovering");
        }
        if self.was_cancelled {
            return Some("session was cancelled");
        }
        if self.token_limit_detected {
            return Some("token limit detected");
        }
        if self.context_pressure {
            return Some("context pressure detected");
        }
        if self.compaction_pending {
            return Some("compaction guard is pending");
        }
        if self.background_pending {
            return Some("background work is pending");
        }
        if self.pending_question {
            return Some("a user question is pending");
        }
        if self.pending_internal_continuation {
            return Some("an internal continuation is pending");
        }
        if self.in_flight {
            return Some("a worker turn is already in flight");
        }
        if self.consecutive_failures >= 2 {
            return Some("consecutive failure cooldown is active");
        }
        if self.stagnation_count >= 2 {
            return Some("continuation progress is stagnant");
        }
        if self.cooldown_until.as_deref().is_some_and(|until| {
            DateTime::parse_from_rfc3339(until)
                .map(|until| Local::now() < until.with_timezone(&Local))
                .unwrap_or(true)
        }) {
            return Some("continuation cooldown is active");
        }
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Number of consecutive resume attempts that observed no durable work
    /// progress. This is the persisted equivalent of OMO's auto-resume cap.
    #[serde(default)]
    pub resume_count: usize,
    /// Stable work marker captured from the PlanNodeRun ledger. Event-log
    /// sequence numbers are intentionally excluded because a retry itself
    /// appends events without proving progress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_progress_marker: Option<String>,
    /// Human-readable reason written when the automatic continuation budget is
    /// exhausted. The caller must surface this as a user decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stuck_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContinuationResumeDecision {
    pub state: ContinuationState,
    pub progress_advanced: bool,
    pub should_resume: bool,
}

/// Gear-owned work lineage that tracks the hierarchy of related sessions.
/// Written to `.gear/continuation/<root-session>/lineage.json`.
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
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::NeedsUser | Self::Blocked | Self::Limited | Self::Complete | Self::Failed
        )
    }

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

/// GBX-235: Wrapper around `tools::compute_baseline_aware_scope` that
/// separates baseline-dirty forbidden paths from real new-forbidden touches.
///
/// Pre-existing dirty files in forbidden paths are returned as
/// `baseline_dirty_forbidden_paths` but do NOT count as `forbidden_touches`
/// in the returned `ScopeCheck`. Only NEW changes to forbidden paths trigger
/// `forbidden_touches` (hard block).
pub fn compute_baseline_aware_scope_with_baseline_dirty(
    before_diff: &crate::tools::DiffSnapshot,
    after_diff: &crate::tools::DiffSnapshot,
    scope: &Scope,
) -> (crate::tools::ScopeCheck, crate::tools::ScopeDrift, Vec<String>) {
    let (mut scope_check, drift) =
        crate::tools::compute_baseline_aware_scope(before_diff, after_diff, scope);

    let baseline_set: std::collections::HashSet<&str> =
        before_diff.changed_files.iter().map(String::as_str).collect();
    let mut baseline_dirty: Vec<String> = Vec::new();
    let mut real_forbidden: Vec<String> = Vec::new();

    for path in scope_check.forbidden_touches.drain(..) {
        if baseline_set.contains(path.as_str()) {
            baseline_dirty.push(path);
        } else {
            real_forbidden.push(path);
        }
    }
    scope_check.forbidden_touches = real_forbidden;

    (scope_check, drift, baseline_dirty)
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

/// Durable execution state for one frozen PlanGraph node.
///
/// The worker result is evidence attached to this record; it is not allowed
/// to advance a node by itself. The runtime writes the state transition after
/// validating dependencies, test evidence, review evidence, and scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanNodeRunStatus {
    Pending,
    Runnable,
    Running,
    RedVerified,
    Implemented,
    GreenVerified,
    Reviewed,
    Completed,
    Failed,
    NeedsUser,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStepRunStatus {
    Pending,
    Running,
    Completed,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStepRun {
    pub step_id: String,
    pub action: String,
    pub expected_observation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_path: Option<String>,
    pub status: PlanStepRunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerEvidenceQuality {
    #[default]
    Unclassified,
    Proved,
    FixtureOnly,
    BlockedNotVerified,
    Failed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanWorkOrderDecision {
    #[default]
    NotRecorded,
    Executed,
    Skipped,
    Blocked,
}

impl PlanNodeRunStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::NeedsUser | Self::Cancelled
        )
    }

    fn can_transition_to(&self, next: &Self) -> bool {
        match self {
            Self::Pending => matches!(next, Self::Runnable | Self::NeedsUser | Self::Cancelled),
            Self::Runnable => {
                matches!(next, Self::Running | Self::NeedsUser | Self::Cancelled)
            }
            Self::Running => matches!(
                next,
                Self::RedVerified
                    | Self::Implemented
                    | Self::GreenVerified
                    | Self::Reviewed
                    | Self::Completed
                    | Self::Failed
                    | Self::NeedsUser
                    | Self::Cancelled
            ),
            Self::RedVerified => matches!(
                next,
                Self::Running
                    | Self::Implemented
                    | Self::GreenVerified
                    | Self::Failed
                    | Self::NeedsUser
                    | Self::Cancelled
            ),
            Self::Implemented => matches!(
                next,
                Self::Running
                    | Self::GreenVerified
                    | Self::Failed
                    | Self::NeedsUser
                    | Self::Cancelled
            ),
            Self::GreenVerified => matches!(
                next,
                Self::Reviewed | Self::Completed | Self::Failed | Self::NeedsUser | Self::Cancelled
            ),
            Self::Reviewed => matches!(
                next,
                Self::Completed | Self::Failed | Self::NeedsUser | Self::Cancelled
            ),
            Self::Completed | Self::Failed | Self::NeedsUser | Self::Cancelled => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriterionEvidenceStatus {
    Pass,
    Fail,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanCriterionEvidence {
    pub criterion_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_task_id: Option<String>,
    pub status: CriterionEvidenceStatus,
    pub attempt: usize,
    pub evidence_path: String,
    pub evidence_sha256: String,
    pub captured_at: String,
    pub evidence_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub obligation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_for: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

pub const PLAN_CRITERION_EVIDENCE_SCHEMA_VERSION: u32 = 1;

impl PlanCriterionEvidence {
    pub fn seal(
        criterion_id: &str,
        status: CriterionEvidenceStatus,
        attempt: usize,
        evidence_path: &str,
        evidence_sha256: &str,
    ) -> Result<Self> {
        let mut evidence = Self {
            criterion_id: criterion_id.to_string(),
            logical_task_id: None,
            status,
            attempt,
            evidence_path: evidence_path.to_string(),
            evidence_sha256: evidence_sha256.to_string(),
            captured_at: timestamp(),
            evidence_hash: String::new(),
            obligation_id: None,
            kind: None,
            producer: None,
            consumer: None,
            freshness: None,
            required_for: Vec::new(),
            unavailable_reason: None,
        };
        evidence.evidence_hash = evidence.expected_hash()?;
        evidence.validate()?;
        Ok(evidence)
    }

    pub fn validate(&self) -> Result<()> {
        if self.criterion_id.trim().is_empty()
            || self.evidence_path.trim().is_empty()
            || self.evidence_sha256.trim().is_empty()
            || self.captured_at.trim().is_empty()
            || self.attempt == 0
        {
            bail!("criterion evidence has incomplete identity or attempt binding");
        }
        let path = Path::new(&self.evidence_path);
        if path.is_absolute()
            || self.evidence_path == ".."
            || self.evidence_path.starts_with("../")
            || self.evidence_path.contains("/../")
        {
            bail!("criterion evidence path must be workspace-relative");
        }
        if self.evidence_hash != self.expected_hash()? {
            bail!("criterion evidence hash mismatch");
        }
        if self
            .logical_task_id
            .as_deref()
            .is_some_and(|id| id.trim().is_empty())
            || self
            .obligation_id
            .as_deref()
            .is_some_and(|id| id.trim().is_empty())
            || self
                .unavailable_reason
                .as_deref()
                .is_some_and(|reason| reason.trim().is_empty())
        {
            bail!("criterion evidence has empty obligation metadata");
        }
        if self.obligation_id.is_some()
            && (self.kind.as_deref().is_none_or(str::is_empty)
                || self.producer.as_deref().is_none_or(str::is_empty)
                || self.consumer.as_deref().is_none_or(str::is_empty)
                || self.freshness.as_deref().is_none_or(str::is_empty)
                || self.required_for.is_empty())
        {
            bail!("typed criterion evidence has incomplete obligation metadata");
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.evidence_hash.clear();
        let bytes =
            serde_json::to_vec(&payload).context("failed to serialize criterion evidence")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanNodeRun {
    pub goal_id: String,
    pub epoch_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub task_id: String,
    /// Stable identity across plan revisions; legacy ledgers may omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_task_id: Option<String>,
    pub attempt: usize,
    pub dependencies: Vec<String>,
    pub status: PlanNodeRunStatus,
    /// Durable OMO-style per-work-order preflight receipt. A node cannot be
    /// considered dispatched without this baseline artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preflight_path: Option<String>,
    #[serde(default)]
    pub preflight_satisfied: bool,
    #[serde(default)]
    pub preflight_checks: Vec<PlanPreflightCheck>,
    /// Durable per-step lifecycle projected from the frozen plan contract.
    /// This remains inside the node ledger so GUI and recovery share one source.
    #[serde(default)]
    pub execution_steps: Vec<PlanStepRun>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_result_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_outcome_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_last_message_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_commands_run: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_known_failures: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_next_steps: Vec<String>,
    /// Bounded post-write diagnostics observed from the current worker attempt.
    /// These are feedback signals, not a replacement for scope/verification
    /// evidence or an independent review receipt.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_diagnostics: Vec<PostWriteDiagnostic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_diagnostic_receipt_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_diagnostic_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_plan_gap: Option<String>,
    #[serde(default)]
    pub worker_decision: PlanWorkOrderDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_decision_reason: Option<String>,
    #[serde(default)]
    pub worker_evidence_quality: WorkerEvidenceQuality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementation_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub red_evidence_path: Option<String>,
    #[serde(default)]
    pub green_evidence_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_evidence_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_boundary_evidence_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_boundary_satisfied: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub criterion_evidence: Vec<PlanCriterionEvidence>,
    pub updated_at: String,
}

/// A deterministic, bounded signal emitted after a worker changes files.
/// Diagnostics are intentionally separate from hard completion evidence so a
/// weak model can receive repair feedback without turning a style hint into a
/// fabricated tool failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostWriteDiagnostic {
    pub diagnostic_id: String,
    pub checker: String,
    pub severity: String,
    pub status: String,
    #[serde(default)]
    pub origin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<usize>,
    pub message: String,
    pub diagnostic_hash: String,
    pub diff_hash: String,
    pub attempt: usize,
    pub fresh: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
    pub repair_signature: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPreflightCheck {
    pub check_id: String,
    pub description: String,
    pub passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanNodeRunLedger {
    pub schema_version: u32,
    pub goal_id: String,
    pub epoch_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub nodes: Vec<PlanNodeRun>,
    pub updated_at: String,
}

/// Durable state for one Atlas-style execution wave.
///
/// A wave is persisted before its first worker is dispatched. This makes the
/// dispatch barrier observable after a crash instead of reconstructing it from
/// the in-memory task queue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanWaveStatus {
    Prepared,
    Dispatching,
    Running,
    Completed,
    Failed,
    Recovered,
}

impl PlanWaveStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanWaveNodeStatus {
    Pending,
    Dispatched,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl PlanWaveNodeStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanWaveNode {
    pub task_id: String,
    pub attempt: usize,
    pub status: PlanWaveNodeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanWaveRunLedger {
    pub schema_version: u32,
    pub goal_id: String,
    pub epoch_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub wave_id: String,
    pub status: PlanWaveStatus,
    pub nodes: Vec<PlanWaveNode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub barrier_opened_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub barrier_closed_at: Option<String>,
    pub updated_at: String,
}

pub const PLAN_WAVE_RUN_LEDGER_SCHEMA_VERSION: u32 = 1;

impl PlanWaveRunLedger {
    pub fn new(
        goal_id: &str,
        epoch_id: &str,
        plan: &crate::plan_graph::PlanGraph,
        wave_id: &str,
        task_ids: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        plan.validate()?;
        let mut task_ids = task_ids.into_iter().collect::<Vec<_>>();
        task_ids.sort();
        task_ids.dedup();
        if task_ids.is_empty() {
            bail!("PlanWaveRunLedger requires at least one task");
        }
        for task_id in &task_ids {
            plan.task(task_id)
                .with_context(|| format!("wave references unknown PlanGraph task `{task_id}`"))?;
        }
        let now = timestamp();
        Ok(Self {
            schema_version: PLAN_WAVE_RUN_LEDGER_SCHEMA_VERSION,
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            plan_hash: plan.plan_hash.clone(),
            wave_id: wave_id.to_string(),
            status: PlanWaveStatus::Prepared,
            nodes: task_ids
                .into_iter()
                .map(|task_id| PlanWaveNode {
                    task_id,
                    attempt: 0,
                    status: PlanWaveNodeStatus::Pending,
                    worker_task_id: None,
                    dispatch_started_at: None,
                    worker_started_at: None,
                    terminal_at: None,
                    error: None,
                })
                .collect(),
            barrier_opened_at: None,
            barrier_closed_at: None,
            updated_at: now,
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PLAN_WAVE_RUN_LEDGER_SCHEMA_VERSION {
            bail!(
                "unsupported PlanWaveRunLedger schema version {}",
                self.schema_version
            );
        }
        for (field, value) in [
            ("goal_id", self.goal_id.as_str()),
            ("epoch_id", self.epoch_id.as_str()),
            ("plan_id", self.plan_id.as_str()),
            ("plan_hash", self.plan_hash.as_str()),
            ("wave_id", self.wave_id.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("PlanWaveRunLedger {field} cannot be empty");
            }
        }
        if self.nodes.is_empty() {
            bail!("PlanWaveRunLedger must contain at least one node");
        }
        let mut ids = HashSet::new();
        for node in &self.nodes {
            if node.task_id.trim().is_empty() || !ids.insert(node.task_id.as_str()) {
                bail!("PlanWaveRunLedger contains duplicate or empty task id");
            }
            if node.status == PlanWaveNodeStatus::Running
                && (node.worker_task_id.is_none() || node.worker_started_at.is_none())
            {
                bail!(
                    "running wave node `{}` is missing worker identity or start timestamp",
                    node.task_id
                );
            }
            if node.status.is_terminal() && node.terminal_at.is_none() {
                bail!(
                    "terminal wave node `{}` is missing terminal timestamp",
                    node.task_id
                );
            }
        }
        if self.status == PlanWaveStatus::Running && self.barrier_opened_at.is_none() {
            bail!("running wave is missing barrier opening timestamp");
        }
        if self.status == PlanWaveStatus::Completed
            && !self
                .nodes
                .iter()
                .all(|node| node.status == PlanWaveNodeStatus::Completed)
        {
            bail!("completed wave contains a non-completed node");
        }
        Ok(())
    }

    pub fn node(&self, task_id: &str) -> Result<&PlanWaveNode> {
        self.nodes
            .iter()
            .find(|node| node.task_id == task_id)
            .with_context(|| format!("unknown PlanWave task `{task_id}`"))
    }

    pub fn node_mut(&mut self, task_id: &str) -> Result<&mut PlanWaveNode> {
        self.nodes
            .iter_mut()
            .find(|node| node.task_id == task_id)
            .with_context(|| format!("unknown PlanWave task `{task_id}`"))
    }

    pub fn open_barrier(&mut self) -> Result<()> {
        if self.status != PlanWaveStatus::Prepared && self.status != PlanWaveStatus::Recovered {
            bail!("cannot open wave barrier from {:?}", self.status);
        }
        self.status = PlanWaveStatus::Dispatching;
        let now = timestamp();
        self.barrier_opened_at = Some(now.clone());
        self.updated_at = now;
        Ok(())
    }

    pub fn mark_dispatched(
        &mut self,
        task_id: &str,
        attempt: usize,
        worker_task_id: String,
    ) -> Result<()> {
        if !matches!(
            self.status,
            PlanWaveStatus::Dispatching | PlanWaveStatus::Running
        ) {
            bail!("cannot dispatch wave node while wave is {:?}", self.status);
        }
        let now = timestamp();
        let node = self.node_mut(task_id)?;
        if node.status != PlanWaveNodeStatus::Pending {
            bail!("wave node `{task_id}` was already dispatched");
        }
        node.attempt = attempt;
        node.worker_task_id = Some(worker_task_id);
        node.dispatch_started_at = Some(now.clone());
        node.status = PlanWaveNodeStatus::Dispatched;
        self.status = PlanWaveStatus::Running;
        self.updated_at = now;
        Ok(())
    }

    pub fn mark_started(&mut self, task_id: &str) -> Result<()> {
        let now = timestamp();
        let node = self.node_mut(task_id)?;
        if node.status != PlanWaveNodeStatus::Dispatched {
            bail!("wave node `{task_id}` is not dispatched");
        }
        node.status = PlanWaveNodeStatus::Running;
        node.worker_started_at = Some(now.clone());
        self.updated_at = now;
        Ok(())
    }

    pub fn mark_terminal(
        &mut self,
        task_id: &str,
        status: PlanWaveNodeStatus,
        error: Option<String>,
    ) -> Result<()> {
        if !status.is_terminal() {
            bail!("wave node terminal status must be terminal");
        }
        let now = timestamp();
        let node = self.node_mut(task_id)?;
        if !matches!(
            node.status,
            PlanWaveNodeStatus::Dispatched | PlanWaveNodeStatus::Running
        ) {
            bail!("wave node `{task_id}` is not active");
        }
        node.status = status;
        node.error = error;
        node.terminal_at = Some(now.clone());
        self.updated_at = now;
        if self.nodes.iter().all(|node| node.status.is_terminal()) {
            self.status = if self
                .nodes
                .iter()
                .all(|node| node.status == PlanWaveNodeStatus::Completed)
            {
                PlanWaveStatus::Completed
            } else {
                PlanWaveStatus::Failed
            };
            self.barrier_closed_at = Some(self.updated_at.clone());
        }
        Ok(())
    }

    pub fn barrier_ready(&self) -> bool {
        self.nodes.iter().all(|node| node.status.is_terminal())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanNodeSessionBindingStatus {
    Active,
    Suspended,
    Terminal,
    Superseded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanNodeSessionBinding {
    pub schema_version: u32,
    pub binding_id: String,
    pub goal_id: String,
    pub epoch_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub task_id: String,
    pub attempt: usize,
    pub worker_task_id: String,
    pub worker_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    pub session_id: String,
    pub capability_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_receipt_hash: Option<String>,
    pub status: PlanNodeSessionBindingStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes_binding_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

pub const PLAN_NODE_SESSION_BINDING_SCHEMA_VERSION: u32 = 1;

impl PlanNodeSessionBinding {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PLAN_NODE_SESSION_BINDING_SCHEMA_VERSION {
            bail!(
                "unsupported PlanNodeSessionBinding schema version {}",
                self.schema_version
            );
        }
        for (field, value) in [
            ("binding_id", self.binding_id.as_str()),
            ("goal_id", self.goal_id.as_str()),
            ("epoch_id", self.epoch_id.as_str()),
            ("plan_id", self.plan_id.as_str()),
            ("plan_hash", self.plan_hash.as_str()),
            ("task_id", self.task_id.as_str()),
            ("worker_task_id", self.worker_task_id.as_str()),
            ("worker_kind", self.worker_kind.as_str()),
            ("session_id", self.session_id.as_str()),
            (
                "capability_fingerprint",
                self.capability_fingerprint.as_str(),
            ),
        ] {
            if value.trim().is_empty() {
                bail!("PlanNodeSessionBinding {field} cannot be empty");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCallKind {
    Primary,
    SchemaRepair,
    SemanticRepair,
    ReviewRetry,
    FollowUp,
    Fallback,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryObservationEvent {
    pub operation: String,
    pub path: String,
    pub event_id: String,
    pub event_hash: String,
    pub observed_at: String,
}

impl RepositoryObservationEvent {
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("operation", self.operation.as_str()),
            ("path", self.path.as_str()),
            ("event_id", self.event_id.as_str()),
            ("event_hash", self.event_hash.as_str()),
            ("observed_at", self.observed_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("repository observation event {field} cannot be empty");
            }
        }
        if !matches!(self.operation.as_str(), "read" | "search" | "list") {
            bail!(
                "repository observation event operation `{}` is not read/search/list",
                self.operation
            );
        }
        if self.path.starts_with('/')
            || self.path == ".."
            || self.path.starts_with("../")
            || self.path.contains("/../")
        {
            bail!("repository observation event path escapes the workspace");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCallLedgerEntry {
    pub schema_version: u32,
    pub call_id: String,
    pub parent_call_id: Option<String>,
    pub goal_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub phase: String,
    pub task_id: String,
    pub kind: ModelCallKind,
    pub worker_kind: String,
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub session_id: String,
    pub status: String,
    pub artifact_path: Option<String>,
    pub transcript_path: Option<String>,
    pub transcript_sha256: Option<String>,
    pub observed_tool_count: usize,
    #[serde(default)]
    pub observed_paths: Vec<String>,
    #[serde(default)]
    pub observation_events: Vec<RepositoryObservationEvent>,
    /// Provider/tool call identifiers observed in the worker transcript. The
    /// ledger entry itself binds them to goal/plan/task/session/workspace, so
    /// adapters can accept either `callID` or `call_id` without losing
    /// lineage.
    #[serde(default)]
    pub observed_call_ids: Vec<String>,
    pub requested_tokens: Option<u64>,
    pub actual_tokens: Option<u64>,
    pub cost_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    pub cache_hit: Option<bool>,
    pub unavailable_reason: Option<String>,
    pub started_at: String,
    pub finished_at: String,
}

pub const MODEL_CALL_LEDGER_SCHEMA_VERSION: u32 = 1;

pub const WORKER_FANOUT_COUNTER_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerFanoutCounter {
    pub schema_version: u32,
    pub session_id: String,
    pub count: usize,
    pub updated_at: String,
}

impl WorkerFanoutCounter {
    pub fn new(session_id: &str) -> Self {
        Self {
            schema_version: WORKER_FANOUT_COUNTER_SCHEMA_VERSION,
            session_id: session_id.to_string(),
            count: 0,
            updated_at: timestamp(),
        }
    }

    fn validate(&self, session_id: &str) -> Result<()> {
        if self.schema_version != WORKER_FANOUT_COUNTER_SCHEMA_VERSION {
            bail!("unsupported worker fan-out counter schema version");
        }
        if session_id.trim().is_empty() || self.session_id != session_id {
            bail!("worker fan-out counter session binding mismatch");
        }
        if self.updated_at.trim().is_empty() {
            bail!("worker fan-out counter updated_at cannot be empty");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerFanoutDenialReceipt {
    pub schema_version: u32,
    pub session_id: String,
    pub task_id: String,
    pub count: usize,
    pub limit: usize,
    pub reason: String,
    pub created_at: String,
}

impl WorkerFanoutDenialReceipt {
    fn validate(&self) -> Result<()> {
        if self.schema_version != WORKER_FANOUT_COUNTER_SCHEMA_VERSION {
            bail!("unsupported worker fan-out denial schema version");
        }
        for (field, value) in [
            ("session_id", self.session_id.as_str()),
            ("task_id", self.task_id.as_str()),
            ("reason", self.reason.as_str()),
            ("created_at", self.created_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("worker fan-out denial {field} cannot be empty");
            }
        }
        if self.limit == 0 || self.count <= self.limit {
            bail!("worker fan-out denial must record count above a positive limit");
        }
        Ok(())
    }
}

/// Durable explanation of the worker route selected for one PlanGraph node.
///
/// The tier calculation is deterministic, while the selected phase/model is
/// policy input. Persisting both facts lets a resumed run explain a fallback
/// without relying on an in-memory route decision or model prose.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRouteDecisionReceipt {
    pub schema_version: u32,
    pub receipt_id: String,
    pub goal_id: String,
    pub epoch_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub task_id: String,
    pub attempt: usize,
    pub phase: crate::plan_graph::PhaseProfile,
    pub route_hint: Option<String>,
    pub size_tier: crate::plan_graph::TaskSizeTier,
    pub risk_tier: crate::plan_graph::TaskRiskTier,
    pub worker_kind: String,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub route_reason: String,
    pub selected_candidate: usize,
    pub fallback_count: usize,
    pub phase_decision_path: String,
    pub budget_reservation_id: Option<String>,
    pub policy_version: String,
    pub created_at: String,
    pub receipt_hash: String,
}

pub const TASK_ROUTE_DECISION_RECEIPT_SCHEMA_VERSION: u32 = 1;

impl TaskRouteDecisionReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn seal(
        goal_id: &str,
        epoch_id: &str,
        plan: &crate::plan_graph::PlanGraph,
        task: &crate::plan_graph::PlanTaskContract,
        attempt: usize,
        phase: crate::plan_graph::PhaseProfile,
        route_hint: Option<String>,
        worker_kind: String,
        worker_model: Option<String>,
        worker_category: String,
        route_reason: String,
        selected_candidate: usize,
        fallback_count: usize,
        phase_decision_path: String,
        budget_reservation_id: Option<String>,
    ) -> Result<Self> {
        plan.validate()?;
        if plan.task(&task.task_id).is_none() {
            bail!("task route receipt references a task outside the PlanGraph");
        }
        let mut receipt = Self {
            schema_version: TASK_ROUTE_DECISION_RECEIPT_SCHEMA_VERSION,
            receipt_id: String::new(),
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            plan_hash: plan.plan_hash.clone(),
            task_id: task.task_id.clone(),
            attempt,
            phase,
            route_hint,
            size_tier: task.size_tier(),
            risk_tier: task.risk_tier(),
            worker_kind,
            worker_model,
            worker_category,
            route_reason,
            selected_candidate,
            fallback_count,
            phase_decision_path,
            budget_reservation_id,
            policy_version: "gbx-012-size-risk-v1".to_string(),
            created_at: timestamp(),
            receipt_hash: String::new(),
        };
        receipt.receipt_hash = receipt.expected_hash()?;
        receipt.receipt_id = format!("task_route_{}", &receipt.receipt_hash[..16]);
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != TASK_ROUTE_DECISION_RECEIPT_SCHEMA_VERSION {
            bail!("unsupported TaskRouteDecisionReceipt schema version");
        }
        for (field, value) in [
            ("receipt_id", self.receipt_id.as_str()),
            ("goal_id", self.goal_id.as_str()),
            ("epoch_id", self.epoch_id.as_str()),
            ("plan_id", self.plan_id.as_str()),
            ("plan_hash", self.plan_hash.as_str()),
            ("task_id", self.task_id.as_str()),
            ("worker_kind", self.worker_kind.as_str()),
            ("worker_category", self.worker_category.as_str()),
            ("route_reason", self.route_reason.as_str()),
            ("phase_decision_path", self.phase_decision_path.as_str()),
            ("policy_version", self.policy_version.as_str()),
            ("created_at", self.created_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("TaskRouteDecisionReceipt {field} cannot be empty");
            }
        }
        if self.attempt == 0 {
            bail!("TaskRouteDecisionReceipt attempt must be greater than zero");
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("task route decision receipt hash mismatch");
        }
        if self.receipt_id != format!("task_route_{}", &self.receipt_hash[..16]) {
            bail!("task route decision receipt id mismatch");
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_id.clear();
        payload.receipt_hash.clear();
        let bytes = serde_json::to_vec(&payload)
            .context("failed to serialize task route decision receipt")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptDispatchGateStatus {
    Reserved,
    Held,
    Accepted,
    PossiblyAccepted,
    Failed,
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptDispatchGate {
    pub schema_version: u32,
    pub gate_id: String,
    pub key_hash: String,
    pub goal_id: String,
    pub task_id: String,
    pub session_id: String,
    pub run_epoch: usize,
    pub message_kind: String,
    pub source: String,
    pub prompt_hash: String,
    /// Optional caller-provided semantic key. When present it is used for
    /// dedupe instead of the exact prompt hash, matching OMO's semantic gate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_dedupe_key: Option<String>,
    pub status: PromptDispatchGateStatus,
    /// Reservation expiry used to recover a process that died after reserve
    /// but before dispatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation_expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub gate_hash: String,
}

pub const PROMPT_DISPATCH_GATE_SCHEMA_VERSION: u32 = 1;
pub const PROMPT_DISPATCH_RESERVATION_TTL_MS: i64 = 30_000;
pub const PROMPT_DISPATCH_POST_DISPATCH_HOLD_MS: i64 = 2_000;
pub const PROMPT_DISPATCH_POSSIBLY_ACCEPTED_HOLD_MS: i64 = 30_000;

impl PromptDispatchGate {
    fn blocks_duplicate_dispatch(&self) -> bool {
        match self.status {
            PromptDispatchGateStatus::Reserved => self
                .reservation_expires_at
                .as_deref()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|deadline| deadline > Local::now().fixed_offset())
                .unwrap_or(true),
            PromptDispatchGateStatus::Accepted => self
                .hold_until
                .as_deref()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|deadline| deadline > Local::now().fixed_offset())
                .unwrap_or(true),
            PromptDispatchGateStatus::Held => self
                .hold_until
                .as_deref()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|deadline| deadline > Local::now().fixed_offset())
                .unwrap_or(true),
            PromptDispatchGateStatus::PossiblyAccepted => self
                .hold_until
                .as_deref()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|deadline| deadline > Local::now().fixed_offset())
                .unwrap_or(true),
            PromptDispatchGateStatus::Failed | PromptDispatchGateStatus::Released => false,
        }
    }

    fn validate_payload(&self) -> Result<()> {
        if self.schema_version != PROMPT_DISPATCH_GATE_SCHEMA_VERSION {
            bail!("unsupported PromptDispatchGate schema version");
        }
        for (field, value) in [
            ("gate_id", self.gate_id.as_str()),
            ("key_hash", self.key_hash.as_str()),
            ("goal_id", self.goal_id.as_str()),
            ("task_id", self.task_id.as_str()),
            ("session_id", self.session_id.as_str()),
            ("message_kind", self.message_kind.as_str()),
            ("source", self.source.as_str()),
            ("prompt_hash", self.prompt_hash.as_str()),
            ("created_at", self.created_at.as_str()),
            ("updated_at", self.updated_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("PromptDispatchGate {field} cannot be empty");
            }
        }
        if self
            .semantic_dedupe_key
            .as_deref()
            .is_some_and(|key| key.trim().is_empty())
        {
            bail!("PromptDispatchGate semantic_dedupe_key cannot be empty");
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.gate_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&payload)?)
        ))
    }

    fn seal(mut self) -> Result<Self> {
        self.gate_hash.clear();
        self.validate_payload()?;
        self.gate_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        if self.key_hash.len() < 16 {
            bail!("PromptDispatchGate key_hash is too short");
        }
        if self.gate_hash != self.expected_hash()? {
            bail!("PromptDispatchGate integrity hash mismatch");
        }
        if self.gate_id != format!("prompt_dispatch_{}", &self.key_hash[..16]) {
            bail!("PromptDispatchGate id mismatch");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PromptDispatchDecision {
    Acquired(PromptDispatchGate),
    Duplicate(PromptDispatchGate),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptSettleEvent {
    Busy,
    Idle,
    Error,
    ContextPressure,
    UserStopped,
    BackgroundCompleted,
    FallbackRetry,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptSettleAction {
    Hold,
    Dispatch,
    Stop,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptSettleDecision {
    pub schema_version: u32,
    pub decision_id: String,
    pub key_hash: String,
    pub goal_id: String,
    pub task_id: String,
    pub session_id: String,
    pub run_epoch: usize,
    pub source: String,
    pub event: PromptSettleEvent,
    pub action: PromptSettleAction,
    pub reason: String,
    pub created_at: String,
    pub decision_hash: String,
}

pub const PROMPT_SETTLE_DECISION_SCHEMA_VERSION: u32 = 1;

impl PromptSettleDecision {
    fn validate_payload(&self) -> Result<()> {
        if self.schema_version != PROMPT_SETTLE_DECISION_SCHEMA_VERSION {
            bail!("unsupported PromptSettleDecision schema version");
        }
        for (field, value) in [
            ("decision_id", self.decision_id.as_str()),
            ("key_hash", self.key_hash.as_str()),
            ("goal_id", self.goal_id.as_str()),
            ("task_id", self.task_id.as_str()),
            ("session_id", self.session_id.as_str()),
            ("source", self.source.as_str()),
            ("reason", self.reason.as_str()),
            ("created_at", self.created_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("PromptSettleDecision {field} cannot be empty");
            }
        }
        if self.key_hash.len() < 16 {
            bail!("PromptSettleDecision key_hash is too short");
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.decision_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&payload)?)
        ))
    }

    fn seal(mut self) -> Result<Self> {
        self.decision_hash.clear();
        self.validate_payload()?;
        self.decision_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        if self.decision_hash != self.expected_hash()? {
            bail!("PromptSettleDecision integrity hash mismatch");
        }
        if self.decision_id != format!("prompt_settle_{}", &self.key_hash[..16]) {
            bail!("PromptSettleDecision id mismatch");
        }
        Ok(())
    }

    fn action_for_event(event: &PromptSettleEvent) -> (PromptSettleAction, &'static str) {
        match event {
            PromptSettleEvent::Idle
            | PromptSettleEvent::BackgroundCompleted
            | PromptSettleEvent::FallbackRetry => (
                PromptSettleAction::Dispatch,
                "event permits one continuation dispatch",
            ),
            PromptSettleEvent::Busy | PromptSettleEvent::Error => (
                PromptSettleAction::Hold,
                "event requires settling before another dispatch",
            ),
            PromptSettleEvent::ContextPressure => (
                PromptSettleAction::Stop,
                "context pressure must not trigger another continuation",
            ),
            PromptSettleEvent::UserStopped => (
                PromptSettleAction::Stop,
                "user stop must not be overridden by automatic continuation",
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptSettleDecisionResult {
    pub decision: PromptSettleDecision,
    pub duplicate: bool,
}

impl ModelCallLedgerEntry {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != MODEL_CALL_LEDGER_SCHEMA_VERSION {
            bail!(
                "unsupported ModelCallLedgerEntry schema version {}",
                self.schema_version
            );
        }
        for (field, value) in [
            ("call_id", self.call_id.as_str()),
            ("goal_id", self.goal_id.as_str()),
            ("plan_id", self.plan_id.as_str()),
            ("phase", self.phase.as_str()),
            ("task_id", self.task_id.as_str()),
            ("worker_kind", self.worker_kind.as_str()),
            ("session_id", self.session_id.as_str()),
            ("status", self.status.as_str()),
            ("started_at", self.started_at.as_str()),
            ("finished_at", self.finished_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("ModelCallLedgerEntry {field} cannot be empty");
            }
        }
        for event in &self.observation_events {
            event.validate()?;
        }
        let mut observed_call_ids = HashSet::new();
        for call_id in &self.observed_call_ids {
            if call_id.trim().is_empty() {
                bail!("ModelCallLedgerEntry observed call id cannot be empty");
            }
            if !observed_call_ids.insert(call_id) {
                bail!("ModelCallLedgerEntry observed call ids must be unique");
            }
        }
        Ok(())
    }
}

/// One role's immutable evidence in a Prometheus/Metis/Momus/Oracle review
/// epoch. Usage remains explicitly unknown when a provider omits telemetry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewEpochRoleEvidence {
    pub role: String,
    pub execution_id: String,
    pub phase_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_session_id: Option<String>,
    pub receipt_hash: String,
    pub receipt_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_hit: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unknown_reason: Option<String>,
}

impl ReviewEpochRoleEvidence {
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("role", self.role.as_str()),
            ("execution_id", self.execution_id.as_str()),
            ("phase_session_id", self.phase_session_id.as_str()),
            ("receipt_hash", self.receipt_hash.as_str()),
            ("receipt_path", self.receipt_path.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("review epoch role evidence {field} cannot be empty");
            }
        }
        if self.actual_session_id.as_deref().is_some_and(str::is_empty) {
            bail!("review epoch role evidence actual_session_id cannot be empty");
        }
        let usage_known = self.requested_tokens.is_some()
            || self.actual_tokens.is_some()
            || self.cost_micros.is_some()
            || self.duration_ms.is_some()
            || self.cache_hit.is_some();
        if !usage_known
            && self
                .unknown_reason
                .as_deref()
                .is_none_or(|reason| reason.trim().is_empty())
        {
            bail!("review epoch role evidence needs usage or an unknown reason");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewEpochBundle {
    pub schema_version: u32,
    pub bundle_id: String,
    pub goal_id: String,
    pub epoch_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub roles: Vec<ReviewEpochRoleEvidence>,
    pub metis_required: bool,
    pub complete: bool,
    pub created_at: String,
    pub bundle_hash: String,
}

pub const REVIEW_EPOCH_BUNDLE_SCHEMA_VERSION: u32 = 1;

impl ReviewEpochBundle {
    pub fn seal(
        goal_id: &str,
        epoch_id: &str,
        plan: &crate::plan_graph::PlanGraph,
        mut roles: Vec<ReviewEpochRoleEvidence>,
        metis_required: bool,
    ) -> Result<Self> {
        roles.sort_by(|left, right| left.role.cmp(&right.role));
        let mut bundle = Self {
            schema_version: REVIEW_EPOCH_BUNDLE_SCHEMA_VERSION,
            bundle_id: String::new(),
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            plan_hash: plan.plan_hash.clone(),
            roles,
            metis_required,
            complete: false,
            created_at: timestamp(),
            bundle_hash: String::new(),
        };
        bundle.complete = bundle.has_required_roles();
        bundle.bundle_hash = bundle.expected_hash()?;
        bundle.bundle_id = format!("review_epoch_{}", &bundle.bundle_hash[..16]);
        bundle.validate()?;
        Ok(bundle)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != REVIEW_EPOCH_BUNDLE_SCHEMA_VERSION {
            bail!("unsupported ReviewEpochBundle schema version");
        }
        for (field, value) in [
            ("bundle_id", self.bundle_id.as_str()),
            ("goal_id", self.goal_id.as_str()),
            ("epoch_id", self.epoch_id.as_str()),
            ("plan_id", self.plan_id.as_str()),
            ("plan_hash", self.plan_hash.as_str()),
            ("created_at", self.created_at.as_str()),
            ("bundle_hash", self.bundle_hash.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("ReviewEpochBundle {field} cannot be empty");
            }
        }
        let mut role_names = HashSet::new();
        for role in &self.roles {
            role.validate()?;
            if !role_names.insert(role.role.as_str()) {
                bail!("ReviewEpochBundle contains duplicate role {}", role.role);
            }
        }
        if self.complete && !self.has_required_roles() {
            bail!("complete ReviewEpochBundle is missing a required role");
        }
        if self.bundle_hash.len() < 16
            || self.bundle_id != format!("review_epoch_{}", &self.bundle_hash[..16])
        {
            bail!("ReviewEpochBundle id mismatch");
        }
        if self.bundle_hash != self.expected_hash()? {
            bail!("ReviewEpochBundle hash mismatch");
        }
        Ok(())
    }

    fn has_required_roles(&self) -> bool {
        let mut required = vec!["planner", "momus", "oracle"];
        if self.metis_required {
            required.push("metis");
        }
        required
            .iter()
            .all(|required| self.roles.iter().any(|role| role.role == *required))
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.bundle_id.clear();
        payload.bundle_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&payload)?)
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryObservationStatus {
    Verified,
    Unverified,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryObservationReceipt {
    pub schema_version: u32,
    pub receipt_id: String,
    pub role: String,
    pub goal_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub worker_task_id: String,
    pub session_id: String,
    pub transcript_sha256: String,
    pub observed_tool_count: usize,
    pub observed_paths: Vec<String>,
    #[serde(default)]
    pub observed_events: Vec<RepositoryObservationEvent>,
    /// Git HEAD captured when the repository observation was written.
    /// Production approval gates compare this identity with the current HEAD
    /// so an old observation cannot be relabeled as current evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_commit: Option<String>,
    pub status: RepositoryObservationStatus,
    pub issued_at: String,
    pub receipt_hash: String,
}

pub const REPOSITORY_OBSERVATION_RECEIPT_SCHEMA_VERSION: u32 = 1;

impl RepositoryObservationReceipt {
    pub fn seal(
        role: &str,
        goal_id: &str,
        plan_id: &str,
        plan_revision: usize,
        plan_hash: &str,
        worker_task_id: &str,
        session_id: &str,
        transcript_sha256: Option<String>,
        observed_tool_count: usize,
        observed_paths: Vec<String>,
        observed_events: Vec<RepositoryObservationEvent>,
    ) -> Result<Self> {
        Self::seal_with_capture_commit(
            role,
            goal_id,
            plan_id,
            plan_revision,
            plan_hash,
            worker_task_id,
            session_id,
            transcript_sha256,
            observed_tool_count,
            observed_paths,
            observed_events,
            None,
        )
    }

    pub fn seal_with_capture_commit(
        role: &str,
        goal_id: &str,
        plan_id: &str,
        plan_revision: usize,
        plan_hash: &str,
        worker_task_id: &str,
        session_id: &str,
        transcript_sha256: Option<String>,
        observed_tool_count: usize,
        observed_paths: Vec<String>,
        observed_events: Vec<RepositoryObservationEvent>,
        capture_commit: Option<String>,
    ) -> Result<Self> {
        let mut receipt = Self {
            schema_version: REPOSITORY_OBSERVATION_RECEIPT_SCHEMA_VERSION,
            receipt_id: String::new(),
            role: role.to_string(),
            goal_id: goal_id.to_string(),
            plan_id: plan_id.to_string(),
            plan_revision,
            plan_hash: plan_hash.to_string(),
            worker_task_id: worker_task_id.to_string(),
            session_id: session_id.to_string(),
            transcript_sha256: transcript_sha256.unwrap_or_default(),
            observed_tool_count,
            observed_paths,
            observed_events,
            capture_commit: capture_commit.map(|commit| commit.trim().to_string()),
            status: RepositoryObservationStatus::Unverified,
            issued_at: timestamp(),
            receipt_hash: String::new(),
        };
        receipt.status = if receipt.observed_tool_count > 0
            && !receipt.observed_paths.is_empty()
            && !receipt.observed_events.is_empty()
        {
            RepositoryObservationStatus::Verified
        } else {
            RepositoryObservationStatus::Unverified
        };
        receipt.receipt_hash = receipt.expected_hash()?;
        receipt.receipt_id = format!("repository_observation_{}", &receipt.receipt_hash[..16]);
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != REPOSITORY_OBSERVATION_RECEIPT_SCHEMA_VERSION {
            bail!("unsupported repository observation receipt schema version");
        }
        for (field, value) in [
            ("receipt_id", self.receipt_id.as_str()),
            ("role", self.role.as_str()),
            ("goal_id", self.goal_id.as_str()),
            ("plan_id", self.plan_id.as_str()),
            ("plan_hash", self.plan_hash.as_str()),
            ("worker_task_id", self.worker_task_id.as_str()),
            ("session_id", self.session_id.as_str()),
            ("issued_at", self.issued_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("repository observation receipt {field} cannot be empty");
            }
        }
        if let Some(capture_commit) = self.capture_commit.as_deref()
            && (capture_commit.len() < 7
                || !capture_commit
                    .chars()
                    .all(|character| character.is_ascii_hexdigit()))
        {
            bail!("repository observation capture_commit must be a Git SHA");
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("repository observation receipt hash mismatch");
        }
        if self.receipt_id != format!("repository_observation_{}", &self.receipt_hash[..16]) {
            bail!("repository observation receipt id mismatch");
        }
        if matches!(self.status, RepositoryObservationStatus::Verified)
            && (self.observed_tool_count == 0
                || self.observed_paths.is_empty()
                || self.observed_events.is_empty())
        {
            bail!("verified repository observation is missing tool/path/event evidence");
        }
        for event in &self.observed_events {
            event.validate()?;
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_id.clear();
        payload.receipt_hash.clear();
        let bytes = serde_json::to_vec(&payload)
            .context("failed to serialize repository observation receipt")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

pub const PLAN_NODE_RUN_LEDGER_SCHEMA_VERSION: u32 = 1;

impl PlanNodeRun {
    pub fn record_obligation_evidence(
        &mut self,
        obligation: &crate::plan_graph::PlanEvidenceObligation,
        status: CriterionEvidenceStatus,
        attempt: usize,
        evidence_path: &str,
        evidence_sha256: &str,
    ) -> Result<()> {
        obligation.validate()?;
        let criterion_id = format!("evidence:{}", obligation.obligation_id);
        let mut evidence = PlanCriterionEvidence::seal(
            &criterion_id,
            status,
            attempt,
            evidence_path,
            evidence_sha256,
        )?;
        evidence.logical_task_id = Some(
            self.logical_task_id
                .as_deref()
                .unwrap_or(self.task_id.as_str())
                .to_string(),
        );
        evidence.obligation_id = Some(obligation.obligation_id.clone());
        evidence.kind = Some(obligation.kind.clone());
        evidence.producer = Some(obligation.producer.clone());
        evidence.consumer = Some(obligation.consumer.clone());
        evidence.freshness = Some(obligation.freshness.clone());
        evidence.required_for = obligation.required_for.clone();
        evidence.unavailable_reason = obligation.unavailable_reason.clone();
        evidence.evidence_hash = evidence.expected_hash()?;
        evidence.validate()?;
        if let Some(existing) = self
            .criterion_evidence
            .iter()
            .find(|existing| existing.criterion_id == criterion_id && existing.attempt == attempt)
        {
            if existing != &evidence {
                bail!("typed evidence obligation was rewritten for the same attempt");
            }
            return Ok(());
        }
        self.criterion_evidence.push(evidence);
        self.criterion_evidence
            .sort_by(|left, right| left.criterion_id.cmp(&right.criterion_id));
        self.updated_at = timestamp();
        Ok(())
    }

    pub fn record_criterion_evidence(
        &mut self,
        criterion_id: &str,
        status: CriterionEvidenceStatus,
        attempt: usize,
        evidence_path: &str,
        evidence_sha256: &str,
    ) -> Result<()> {
        let evidence = PlanCriterionEvidence::seal(
            criterion_id,
            status,
            attempt,
            evidence_path,
            evidence_sha256,
        )?;
        if let Some(existing) = self
            .criterion_evidence
            .iter()
            .find(|existing| existing.criterion_id == criterion_id && existing.attempt == attempt)
        {
            if existing != &evidence {
                bail!("criterion evidence was rewritten for the same attempt");
            }
            return Ok(());
        }
        self.criterion_evidence.push(evidence);
        self.criterion_evidence
            .sort_by(|left, right| left.criterion_id.cmp(&right.criterion_id));
        self.updated_at = timestamp();
        Ok(())
    }

    pub fn all_criteria_passed(&self, required_criteria: &[String]) -> bool {
        if self.attempt == 0 || required_criteria.is_empty() {
            return false;
        }
        required_criteria.iter().all(|criterion| {
            self.criterion_evidence.iter().any(|evidence| {
                evidence.criterion_id == *criterion
                    && evidence.attempt == self.attempt
                    && evidence.status == CriterionEvidenceStatus::Pass
            })
        })
    }

    pub fn all_evidence_obligations_passed(
        &self,
        obligations: &[crate::plan_graph::PlanEvidenceObligation],
    ) -> bool {
        if obligations.is_empty() {
            return true;
        }
        self.attempt > 0
            && obligations.iter().all(|obligation| {
                let criterion_id = format!("evidence:{}", obligation.obligation_id);
                self.criterion_evidence.iter().any(|evidence| {
                        evidence.criterion_id == criterion_id
                        && evidence.logical_task_id.as_deref()
                            == Some(
                                self.logical_task_id
                                    .as_deref()
                                    .unwrap_or(self.task_id.as_str()),
                            )
                        && evidence.obligation_id.as_deref()
                            == Some(obligation.obligation_id.as_str())
                        && evidence.kind.as_deref() == Some(obligation.kind.as_str())
                        && evidence.producer.as_deref() == Some(obligation.producer.as_str())
                        && evidence.consumer.as_deref() == Some(obligation.consumer.as_str())
                        && evidence.freshness.as_deref() == Some(obligation.freshness.as_str())
                        && evidence.required_for == obligation.required_for
                        && evidence.attempt == self.attempt
                        && evidence.status == CriterionEvidenceStatus::Pass
                })
            })
    }

    /// QA scenarios are sealed as criterion evidence with a stable `qa:` id.
    /// Keeping the check on the node makes completion and recovery use the
    /// same attempt-bound evidence rules as ordinary acceptance predicates.
    pub fn all_qa_passed(&self, task: &crate::plan_graph::PlanTaskContract) -> bool {
        if self.attempt == 0 {
            return false;
        }
        task.qa
            .happy_path
            .iter()
            .map(|scenario| ("happy", scenario))
            .chain(
                task.qa
                    .failure_path
                    .iter()
                    .map(|scenario| ("failure", scenario)),
            )
            .chain(
                task.qa
                    .adversarial_path
                    .iter()
                    .map(|scenario| ("adversarial", scenario)),
            )
            .all(|(kind, scenario)| {
                let criterion_id = format!("qa:{kind}:{}", scenario.name);
                self.criterion_evidence.iter().any(|evidence| {
                    evidence.criterion_id == criterion_id
                        && evidence.attempt == self.attempt
                        && evidence.status == CriterionEvidenceStatus::Pass
                })
            })
    }

    pub fn sync_step_lifecycle(&mut self, error: Option<&str>) {
        match self.status {
            PlanNodeRunStatus::Running => {
                if let Some(step) = self
                    .execution_steps
                    .iter_mut()
                    .find(|step| step.status == PlanStepRunStatus::Pending)
                {
                    step.status = PlanStepRunStatus::Running;
                    step.updated_at = timestamp();
                }
            }
            PlanNodeRunStatus::Completed => {
                for step in &mut self.execution_steps {
                    step.status = PlanStepRunStatus::Completed;
                    step.updated_at = timestamp();
                }
            }
            PlanNodeRunStatus::Failed
            | PlanNodeRunStatus::NeedsUser
            | PlanNodeRunStatus::Cancelled => {
                if let Some(step) = self
                    .execution_steps
                    .iter_mut()
                    .find(|step| !matches!(step.status, PlanStepRunStatus::Completed))
                {
                    step.status = PlanStepRunStatus::Blocked;
                    step.error = error.map(ToString::to_string);
                    step.updated_at = timestamp();
                }
            }
            PlanNodeRunStatus::Pending
            | PlanNodeRunStatus::Runnable
            | PlanNodeRunStatus::RedVerified
            | PlanNodeRunStatus::Implemented
            | PlanNodeRunStatus::GreenVerified
            | PlanNodeRunStatus::Reviewed => {}
        }
        self.updated_at = timestamp();
    }

    pub fn apply_worker_step_evidence(
        &mut self,
        completed_step_ids: &[String],
        evidence_by_step: &HashMap<String, String>,
    ) -> Result<Vec<String>> {
        let declared = completed_step_ids.iter().collect::<HashSet<_>>();
        for step_id in &declared {
            if !self
                .execution_steps
                .iter()
                .any(|step| &step.step_id == *step_id)
            {
                bail!("worker reported unknown execution step `{step_id}`");
            }
        }
        let mut encountered_uncompleted = false;
        for step in &self.execution_steps {
            let is_completed = step.status == PlanStepRunStatus::Completed;
            let is_declared = declared.contains(&step.step_id);
            if !is_completed && !is_declared {
                encountered_uncompleted = true;
                continue;
            }
            if encountered_uncompleted && (is_completed || is_declared) {
                bail!(
                    "worker skipped ordered execution step `{}` before reporting `{}`",
                    self.execution_steps
                        .iter()
                        .find(|candidate| {
                            candidate.status != PlanStepRunStatus::Completed
                                && !declared.contains(&candidate.step_id)
                        })
                        .map(|candidate| candidate.step_id.as_str())
                        .unwrap_or("unknown"),
                    step.step_id
                );
            }
        }
        for step in &mut self.execution_steps {
            if declared.contains(&step.step_id) {
                step.status = PlanStepRunStatus::Completed;
                step.evidence_path = evidence_by_step
                    .get(&step.step_id)
                    .cloned()
                    .or_else(|| step.evidence_path.clone());
                step.error = None;
                step.updated_at = timestamp();
            }
        }
        let missing = self
            .execution_steps
            .iter()
            .filter(|step| step.status != PlanStepRunStatus::Completed)
            .map(|step| step.step_id.clone())
            .collect::<Vec<_>>();
        self.updated_at = timestamp();
        Ok(missing)
    }
}

impl PlanNodeRunLedger {
    pub fn from_plan(
        goal_id: &str,
        epoch_id: &str,
        plan: &crate::plan_graph::PlanGraph,
    ) -> Result<Self> {
        plan.validate()?;
        Ok(Self {
            schema_version: PLAN_NODE_RUN_LEDGER_SCHEMA_VERSION,
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            plan_hash: plan.plan_hash.clone(),
            nodes: plan
                .draft
                .tasks
                .iter()
                .map(|task| PlanNodeRun {
                    goal_id: goal_id.to_string(),
                    epoch_id: epoch_id.to_string(),
                    plan_id: plan.plan_id.clone(),
                    plan_revision: plan.revision,
                    plan_hash: plan.plan_hash.clone(),
                    task_id: task.task_id.clone(),
                    logical_task_id: Some(task.logical_task_id_or_task_id().to_string()),
                    attempt: 0,
                    dependencies: task.dependencies.clone(),
                    status: PlanNodeRunStatus::Pending,
                    preflight_path: None,
                    preflight_satisfied: false,
                    preflight_checks: Vec::new(),
                    execution_steps: task
                        .execution_steps_or_legacy()
                        .into_iter()
                        .map(|step| PlanStepRun {
                            step_id: step.step_id,
                            action: step.action,
                            expected_observation: step.expected_observation,
                            evidence_path: step.evidence_path,
                            status: PlanStepRunStatus::Pending,
                            error: None,
                            updated_at: timestamp(),
                        })
                        .collect(),
                    worker_result_path: None,
                    worker_outcome_path: None,
                    worker_last_message_path: None,
                    worker_changed_files: Vec::new(),
                    worker_commands_run: Vec::new(),
                    worker_known_failures: Vec::new(),
                    worker_next_steps: Vec::new(),
                    worker_diagnostics: Vec::new(),
                    worker_diagnostic_receipt_path: None,
                    worker_diagnostic_status: None,
                    worker_plan_gap: None,
                    worker_decision: PlanWorkOrderDecision::NotRecorded,
                    worker_decision_reason: None,
                    worker_evidence_quality: WorkerEvidenceQuality::Unclassified,
                    worker_task_id: None,
                    implementation_task_id: None,
                    review_task_id: None,
                    red_evidence_path: None,
                    green_evidence_paths: Vec::new(),
                    review_evidence_path: None,
                    commit_boundary_evidence_path: None,
                    commit_boundary_satisfied: None,
                    error: None,
                    criterion_evidence: Vec::new(),
                    updated_at: timestamp(),
                })
                .collect(),
            updated_at: timestamp(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PLAN_NODE_RUN_LEDGER_SCHEMA_VERSION {
            bail!(
                "unsupported PlanNodeRunLedger schema version {}",
                self.schema_version
            );
        }
        if self.nodes.is_empty() {
            bail!("PlanNodeRunLedger must contain at least one node");
        }
        let mut ids = HashSet::new();
        for node in &self.nodes {
            if node.goal_id != self.goal_id
                || node.epoch_id != self.epoch_id
                || node.plan_id != self.plan_id
                || node.plan_revision != self.plan_revision
                || node.plan_hash != self.plan_hash
            {
                bail!("PlanNodeRun has inconsistent plan binding");
            }
            if !ids.insert(node.task_id.as_str()) {
                bail!("duplicate PlanNodeRun task id `{}`", node.task_id);
            }
            if node.status == PlanNodeRunStatus::Completed
                && (node.attempt == 0
                    || node.green_evidence_paths.is_empty()
                    || node.review_evidence_path.is_none()
                    || (self.nodes.len() > 1 && node.review_task_id.is_none())
                    || node
                        .execution_steps
                        .iter()
                        .any(|step| step.status != PlanStepRunStatus::Completed))
            {
                bail!(
                    "completed PlanNodeRun `{}` is missing attempt, GREEN, or review evidence",
                    node.task_id
                );
            }
            if matches!(node.worker_decision, PlanWorkOrderDecision::Skipped)
                && node
                    .worker_decision_reason
                    .as_deref()
                    .is_none_or(|reason| reason.trim().is_empty())
            {
                bail!(
                    "skipped PlanNodeRun `{}` must record a decision reason",
                    node.task_id
                );
            }
            for evidence in &node.criterion_evidence {
                evidence.validate()?;
                if evidence.attempt > node.attempt && node.attempt > 0 {
                    bail!("PlanNodeRun criterion evidence belongs to a future attempt");
                }
            }
            let mut step_ids = HashSet::new();
            for step in &node.execution_steps {
                if step.step_id.trim().is_empty()
                    || !step_ids.insert(step.step_id.as_str())
                    || step.action.trim().is_empty()
                    || step.expected_observation.trim().is_empty()
                {
                    bail!("PlanNodeRun `{}` has invalid execution step", node.task_id);
                }
            }
        }
        Ok(())
    }

    pub fn completed_task_ids(&self) -> HashSet<String> {
        self.nodes
            .iter()
            .filter(|node| node.status == PlanNodeRunStatus::Completed)
            .map(|node| node.task_id.clone())
            .collect()
    }

    pub fn all_criteria_passed(&self, plan: &crate::plan_graph::PlanGraph) -> bool {
        plan.draft.tasks.iter().all(|task| {
            self.nodes
                .iter()
                .find(|node| node.task_id == task.task_id)
                .is_some_and(|node| {
                    node.all_criteria_passed(&task.completion_predicates)
                        && node.all_evidence_obligations_passed(&task.evidence_obligations)
                })
        })
    }

    pub fn active_task_ids(&self) -> HashSet<String> {
        self.nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.status,
                    PlanNodeRunStatus::Runnable
                        | PlanNodeRunStatus::Running
                        | PlanNodeRunStatus::RedVerified
                        | PlanNodeRunStatus::Implemented
                        | PlanNodeRunStatus::GreenVerified
                        | PlanNodeRunStatus::Reviewed
                )
            })
            .map(|node| node.task_id.clone())
            .collect()
    }

    pub fn node_mut(&mut self, task_id: &str) -> Result<&mut PlanNodeRun> {
        self.nodes
            .iter_mut()
            .find(|node| node.task_id == task_id)
            .with_context(|| format!("unknown PlanNodeRun task `{task_id}`"))
    }

    pub fn mark(&mut self, task_id: &str, status: PlanNodeRunStatus) -> Result<()> {
        let node = self.node_mut(task_id)?;
        if node.status.is_terminal() && node.status != status {
            bail!(
                "cannot transition terminal PlanNodeRun `{task_id}` from {:?} to {:?}",
                node.status,
                status
            );
        }
        if node.status != status && !node.status.can_transition_to(&status) {
            bail!(
                "invalid PlanNodeRun transition `{task_id}` from {:?} to {:?}",
                node.status,
                status
            );
        }
        node.status = status;
        node.sync_step_lifecycle(None);
        node.updated_at = timestamp();
        self.updated_at = timestamp();
        Ok(())
    }

    /// Requeue failed plan nodes when a persisted continuation is resumed.
    ///
    /// `Failed` is terminal for an epoch's evidence, but it must not become a
    /// dead end after a process restart: the next continuation epoch may
    /// retry the same work order without pretending that the failed attempt
    /// passed. The attempt number, previous worker identity and error remain
    /// durable for audit purposes.
    pub fn requeue_failed_for_resume(&mut self) -> Vec<String> {
        let mut requeued = Vec::new();
        for node in &mut self.nodes {
            if node.status == PlanNodeRunStatus::Failed {
                // Leave the node pending so the scheduler can select it on
                // the resumed epoch and persist the normal Pending -> Runnable
                // transition before dispatch.
                node.status = PlanNodeRunStatus::Pending;
                node.preflight_path = None;
                node.preflight_satisfied = false;
                node.preflight_checks.clear();
                for step in &mut node.execution_steps {
                    if step.status == PlanStepRunStatus::Blocked {
                        step.status = PlanStepRunStatus::Pending;
                        step.error = None;
                        step.updated_at = timestamp();
                    }
                }
                node.updated_at = timestamp();
                requeued.push(node.task_id.clone());
            }
        }
        if !requeued.is_empty() {
            self.updated_at = timestamp();
        }
        requeued
    }

    /// Requeue non-terminal nodes that were in flight when the previous
    /// process stopped.  A persisted `Running` (or intermediate review/TDD)
    /// status is evidence of an interrupted attempt, not proof that a worker
    /// is still alive.  Leaving it in `active_task_ids` would make the
    /// scheduler see no runnable work after restart and strand the goal.
    ///
    /// The prior attempt, worker identities, result paths, and criterion
    /// evidence remain durable.  Only the current dispatch cursor is reset so
    /// the next epoch can issue a fresh, ordered work-order attempt.
    pub fn requeue_incomplete_for_resume(&mut self) -> Vec<String> {
        let mut requeued = Vec::new();
        for node in &mut self.nodes {
            if !matches!(
                node.status,
                PlanNodeRunStatus::Runnable
                    | PlanNodeRunStatus::Running
                    | PlanNodeRunStatus::RedVerified
                    | PlanNodeRunStatus::Implemented
                    | PlanNodeRunStatus::GreenVerified
                    | PlanNodeRunStatus::Reviewed
            ) {
                continue;
            }

            node.status = PlanNodeRunStatus::Pending;
            node.preflight_path = None;
            node.preflight_satisfied = false;
            node.preflight_checks.clear();
            for step in &mut node.execution_steps {
                step.status = PlanStepRunStatus::Pending;
                step.error = None;
                step.updated_at = timestamp();
            }
            node.updated_at = timestamp();
            requeued.push(node.task_id.clone());
        }
        if !requeued.is_empty() {
            self.updated_at = timestamp();
        }
        requeued
    }

    /// Bind a recovered ledger to the new continuation epoch while retaining
    /// every node's attempt and evidence history.
    pub fn rebind_epoch_for_resume(&mut self, epoch_id: &str) {
        if self.epoch_id == epoch_id {
            return;
        }
        self.epoch_id = epoch_id.to_string();
        for node in &mut self.nodes {
            node.epoch_id = epoch_id.to_string();
            node.updated_at = timestamp();
        }
        self.updated_at = timestamp();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalVerificationDimension {
    PlanCompliance,
    CodeQuality,
    RealQa,
    ScopeFidelity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalVerificationResult {
    pub dimension: FinalVerificationDimension,
    pub passed: bool,
    pub summary: String,
    pub evidence_paths: Vec<String>,
    pub reviewer_execution_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalVerificationWaveReceipt {
    pub schema_version: u32,
    pub goal_id: String,
    pub epoch_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub dimensions: Vec<FinalVerificationResult>,
    pub passed: bool,
    pub receipt_hash: String,
    pub created_at: String,
}

pub const FINAL_VERIFICATION_WAVE_SCHEMA_VERSION: u32 = 1;

impl FinalVerificationWaveReceipt {
    pub fn seal(
        goal_id: &str,
        epoch_id: &str,
        plan: &crate::plan_graph::PlanGraph,
        dimensions: Vec<FinalVerificationResult>,
    ) -> Result<Self> {
        let mut receipt = Self {
            schema_version: FINAL_VERIFICATION_WAVE_SCHEMA_VERSION,
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            plan_hash: plan.plan_hash.clone(),
            passed: dimensions.len() == 4 && dimensions.iter().all(|result| result.passed),
            dimensions,
            receipt_hash: String::new(),
            created_at: timestamp(),
        };
        receipt.receipt_hash = receipt.expected_hash()?;
        receipt.validate(plan)?;
        Ok(receipt)
    }

    pub fn validate(&self, plan: &crate::plan_graph::PlanGraph) -> Result<()> {
        if self.schema_version != FINAL_VERIFICATION_WAVE_SCHEMA_VERSION
            || self.goal_id != plan.goal_id
            || self.plan_id != plan.plan_id
            || self.plan_revision != plan.revision
            || self.plan_hash != plan.plan_hash
            || self.receipt_hash != self.expected_hash()?
        {
            bail!("final verification wave receipt binding or hash is invalid");
        }
        let required = [
            FinalVerificationDimension::PlanCompliance,
            FinalVerificationDimension::CodeQuality,
            FinalVerificationDimension::RealQa,
            FinalVerificationDimension::ScopeFidelity,
        ];
        if self.dimensions.len() != required.len()
            || required.iter().any(|dimension| {
                !self
                    .dimensions
                    .iter()
                    .any(|result| &result.dimension == dimension)
            })
        {
            bail!("final verification wave must contain exactly four dimensions");
        }
        if self.passed != self.dimensions.iter().all(|result| result.passed) {
            bail!("final verification wave passed flag disagrees with dimensions");
        }
        for result in &self.dimensions {
            if result.summary.trim().is_empty()
                || result.evidence_paths.is_empty()
                || result.reviewer_execution_ids.is_empty()
            {
                bail!("final verification dimension is missing evidence or reviewer identity");
            }
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_hash.clear();
        let bytes = serde_json::to_vec(&payload).context("failed to serialize final wave")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

/// Per-file content fingerprint for baseline attribution.
///
/// Distinguishes pre-existing dirty files from real new changes
/// by comparing content hash, size, and file kind between snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFingerprint {
    /// Repository-relative path.
    pub path: String,
    /// SHA-256 of the file contents at snapshot time.
    pub content_hash: String,
    /// Human-readable file kind (e.g. "rust", "markdown", "binary").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_kind: Option<String>,
    /// File size in bytes at snapshot time.
    pub size_bytes: u64,
}

/// Classification of one file between two snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileAttributionClass {
    /// File was already changed before the worker started (baseline dirty).
    UnchangedBaseline,
    /// File is new (not present in baseline).
    Added,
    /// File was modified from its baseline state.
    Modified,
    /// File was present in baseline but is now deleted or renamed away.
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileAttribution {
    pub fingerprint: FileFingerprint,
    pub classification: FileAttributionClass,
}

/// Result of per-file baseline attribution between two snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerFileAttributionResult {
    /// Session or attempt identifier.
    pub session_id: String,
    pub attempt: usize,
    /// Files whose after-fingerprint matches the before-fingerprint exactly.
    pub unchanged_baseline: Vec<FileAttribution>,
    /// Files present in after but not in before.
    pub added: Vec<FileAttribution>,
    /// Files whose content/size changed between before and after.
    pub modified: Vec<FileAttribution>,
    /// Files present in before but absent from after (deleted or renamed).
    pub removed: Vec<FileAttribution>,
    /// Scope-level verdict: true iff added, modified, and removed are all empty
    /// (only unchanged baseline files remain).
    pub scope_verdict: bool,
}

/// Compute a FileFingerprint for one path by reading it from disk.
/// Returns None when the path cannot be read (e.g. deleted or directory).
pub fn fingerprint_file(workspace: &std::path::Path, path: &str) -> Option<FileFingerprint> {
    let full_path = workspace.join(path);
    let metadata = std::fs::symlink_metadata(&full_path).ok()?;
    if !metadata.is_file() {
        return None;
    }
    let contents = std::fs::read(&full_path).ok()?;
    let content_hash = format!("{:x}", Sha256::digest(&contents));
    let file_kind = full_path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| match ext {
            "rs" => "rust",
            "md" => "markdown",
            "json" | "jsonl" => "json",
            "toml" => "toml",
            "ts" | "tsx" => "typescript",
            "js" | "jsx" => "javascript",
            "css" | "scss" | "less" => "stylesheet",
            "html" => "html",
            "yaml" | "yml" => "yaml",
            "sh" | "bash" | "zsh" => "shell",
            "py" => "python",
            _ => "other",
        })
        .map(ToString::to_string);
    Some(FileFingerprint {
        path: path.to_string(),
        content_hash,
        file_kind,
        size_bytes: metadata.len(),
    })
}

/// Fingerprint every path in the list from disk, returning a map keyed by path.
pub fn fingerprint_paths(
    workspace: &std::path::Path,
    paths: &[String],
) -> std::collections::HashMap<String, FileFingerprint> {
    let mut map = std::collections::HashMap::new();
    for path in paths {
        if let Some(fp) = fingerprint_file(workspace, path) {
            map.insert(path.clone(), fp);
        }
    }
    map
}

/// Compute per-file baseline attribution between two sets of fingerprints.
///
/// `before` fingerprints represent the state before work began (baseline).
/// `after` fingerprints represent the state after work completed.
///
/// Only paths whose after fingerprint differs from before (or is new/removed)
/// produce non-`UnchangedBaseline` classifications.
///
/// Returns a structured result with a scope verdict that is true iff no real
/// change (add/modify/remove) occurred.
pub fn compute_per_file_attribution(
    before: &std::collections::HashMap<String, FileFingerprint>,
    after: &std::collections::HashMap<String, FileFingerprint>,
    session_id: &str,
    attempt: usize,
) -> PerFileAttributionResult {
    let before_paths: std::collections::HashSet<&str> =
        before.keys().map(String::as_str).collect();

    let mut unchanged_baseline = Vec::new();
    let mut added = Vec::new();
    let mut modified = Vec::new();
    let mut removed = Vec::new();

    for (path, before_fp) in before {
        if let Some(after_fp) = after.get(path) {
            if before_fp.content_hash == after_fp.content_hash && before_fp.size_bytes == after_fp.size_bytes {
                unchanged_baseline.push(FileAttribution {
                    fingerprint: after_fp.clone(),
                    classification: FileAttributionClass::UnchangedBaseline,
                });
            } else {
                modified.push(FileAttribution {
                    fingerprint: after_fp.clone(),
                    classification: FileAttributionClass::Modified,
                });
            }
        } else {
            removed.push(FileAttribution {
                fingerprint: before_fp.clone(),
                classification: FileAttributionClass::Removed,
            });
        }
    }

    for (path, after_fp) in after {
        if !before_paths.contains(path.as_str()) {
            added.push(FileAttribution {
                fingerprint: after_fp.clone(),
                classification: FileAttributionClass::Added,
            });
        }
    }

    let scope_verdict = added.is_empty() && modified.is_empty() && removed.is_empty();

    PerFileAttributionResult {
        session_id: session_id.to_string(),
        attempt,
        unchanged_baseline,
        added,
        modified,
        removed,
        scope_verdict,
    }
}

/// Determine whether a shell command is destructive and must be hard-rejected
/// before execution.
///
/// Returns the first matched pattern when the command is destructive, or
/// `None` when the command is safe to execute.
pub fn is_destructive_command(command: &str) -> Option<&'static str> {
    // Tokenize conservatively so shell prefixes, quoted `sh -c` payloads,
    // absolute binary paths, and `git -C <dir> ...` cannot bypass the gate.
    // This is intentionally a hard safety boundary: a false positive is
    // recoverable, while allowing a destructive command can erase user work.
    let words = command
        .split(|character: char| {
            character.is_whitespace()
                || matches!(character, '\'' | '"' | ';' | '|' | '&' | '(' | ')')
        })
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();

    for (index, word) in words.iter().enumerate() {
        let executable = word.rsplit('/').next().unwrap_or(word);
        if executable == "rm" {
            return Some("rm (destructive)");
        }
        if executable != "git" {
            continue;
        }

        let mut subcommand = None;
        let mut cursor = index + 1;
        while cursor < words.len() {
            let token = words[cursor].as_str();
            if token == "-c" {
                cursor = cursor.saturating_add(2);
                continue;
            }
            if token.starts_with("-c") || token.starts_with("--git-") {
                // `-C <dir>` consumes the following path, while the
                // attached `-C<dir>` form consumes only this token. Do not
                // skip the actual subcommand in the attached form.
                cursor += 1;
                continue;
            }
            if token.starts_with('-') {
                cursor += 1;
                continue;
            }
            subcommand = Some(token);
            break;
        }

        match subcommand {
            Some("checkout") => return Some("git checkout (destructive)"),
            Some("restore") => return Some("git restore (destructive)"),
            Some("clean") => return Some("git clean (destructive)"),
            Some("reset") => {
                if words[index + 1..].iter().any(|word| word == "--hard") {
                    return Some("git reset --hard");
                }
                if !words[index + 1..].iter().any(|word| word == "--soft") {
                    return Some("git reset (destructive)");
                }
            }
            _ => {}
        }
    }

    None
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

pub const OBJECTIVE_GRAPH_SCHEMA_VERSION: u32 = 1;
pub const OBJECTIVE_EVENT_SCHEMA_VERSION: u32 = 1;
pub const OBJECTIVE_EPOCH_OUTCOME_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectivePolicy {
    pub auto_continue: bool,
    pub max_epochs: usize,
    pub max_calls: usize,
    pub max_tokens: u64,
    pub max_cost_micros: u64,
    pub max_unknown_usage_calls: usize,
    pub max_consecutive_no_progress: usize,
    pub max_consecutive_failures: usize,
    pub cooldown_seconds: u64,
}

impl Default for ObjectivePolicy {
    fn default() -> Self {
        Self {
            auto_continue: false,
            max_epochs: 1,
            max_calls: 96,
            max_tokens: 12_288_000,
            max_cost_micros: 10_000_000,
            max_unknown_usage_calls: 32,
            max_consecutive_no_progress: 2,
            max_consecutive_failures: 3,
            cooldown_seconds: 0,
        }
    }
}

impl ObjectivePolicy {
    pub fn rolling_default() -> Self {
        Self {
            auto_continue: true,
            max_epochs: 3,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_epochs == 0
            || self.max_calls == 0
            || self.max_tokens == 0
            || self.max_cost_micros == 0
            || self.max_unknown_usage_calls == 0
            || self.max_consecutive_no_progress == 0
            || self.max_consecutive_failures == 0
        {
            bail!("objective policy limits must be greater than zero");
        }
        Ok(())
    }

    pub fn hash(&self) -> Result<String> {
        let bytes = serde_json::to_vec(self).context("failed to serialize objective policy")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveStatus {
    Running,
    NeedsUser,
    Stopped,
    Limited,
    Blocked,
    Complete,
    Failed,
}

impl ObjectiveStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::NeedsUser
                | Self::Stopped
                | Self::Limited
                | Self::Blocked
                | Self::Complete
                | Self::Failed
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalGraphNode {
    pub goal_id: String,
    pub epoch_id: String,
    pub session_id: String,
    pub request: String,
    pub acceptance_signals: Vec<String>,
    pub parent_goal_id: Option<String>,
    pub parent_epoch_id: Option<String>,
    pub parent_strategist_receipt_hash: Option<String>,
    pub request_hash: String,
    pub objective_hash: String,
    pub status: GoalStatus,
    pub final_wave_receipt_hash: Option<String>,
    pub final_report_path: Option<String>,
    pub strategist_receipt_hash: Option<String>,
    pub progress_fingerprint: String,
    pub terminal_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl GoalGraphNode {
    pub(crate) fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("goal_id", self.goal_id.as_str()),
            ("epoch_id", self.epoch_id.as_str()),
            ("session_id", self.session_id.as_str()),
            ("request", self.request.as_str()),
            ("request_hash", self.request_hash.as_str()),
            ("objective_hash", self.objective_hash.as_str()),
            ("progress_fingerprint", self.progress_fingerprint.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("goal graph node requires non-empty {field}");
            }
        }
        if self.status == GoalStatus::Complete
            && (self.final_wave_receipt_hash.is_none() || self.final_report_path.is_none())
        {
            bail!("completed goal graph node requires final wave and report artifacts");
        }
        let expected_request_hash = format!("{:x}", Sha256::digest(self.request.as_bytes()));
        if self.request_hash != expected_request_hash {
            bail!("goal graph node request hash does not match its request");
        }
        match (
            &self.parent_goal_id,
            &self.parent_epoch_id,
            &self.parent_strategist_receipt_hash,
        ) {
            (None, None, None) => {}
            (Some(_), Some(_), Some(hash)) if !hash.trim().is_empty() => {}
            _ => bail!("objective child must bind its parent epoch and strategist receipt"),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveGraph {
    pub schema_version: u32,
    pub objective_id: String,
    pub root_session_id: String,
    pub workspace: String,
    pub request: String,
    pub scope_hash: String,
    pub policy: ObjectivePolicy,
    pub policy_hash: String,
    pub nodes: Vec<GoalGraphNode>,
    pub active_goal_id: Option<String>,
    pub status: ObjectiveStatus,
    pub stop_reason: Option<String>,
    pub consecutive_no_progress: usize,
    pub consecutive_failures: usize,
    pub created_at: String,
    pub updated_at: String,
    pub graph_hash: String,
}

impl ObjectiveGraph {
    pub fn new(
        objective_id: &str,
        root_session_id: &str,
        workspace: &str,
        request: &str,
        scope_hash: &str,
        policy: ObjectivePolicy,
    ) -> Result<Self> {
        policy.validate()?;
        let now = timestamp();
        let policy_hash = policy.hash()?;
        let mut graph = Self {
            schema_version: OBJECTIVE_GRAPH_SCHEMA_VERSION,
            objective_id: objective_id.to_string(),
            root_session_id: root_session_id.to_string(),
            workspace: workspace.to_string(),
            request: request.to_string(),
            scope_hash: scope_hash.to_string(),
            policy,
            policy_hash,
            nodes: Vec::new(),
            active_goal_id: None,
            status: ObjectiveStatus::Running,
            stop_reason: None,
            consecutive_no_progress: 0,
            consecutive_failures: 0,
            created_at: now.clone(),
            updated_at: now,
            graph_hash: String::new(),
        };
        graph.graph_hash = graph.expected_hash()?;
        graph.validate()?;
        Ok(graph)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != OBJECTIVE_GRAPH_SCHEMA_VERSION
            || self.objective_id.trim().is_empty()
            || self.root_session_id.trim().is_empty()
            || self.workspace.trim().is_empty()
            || self.request.trim().is_empty()
            || self.scope_hash.trim().is_empty()
            || self.policy_hash != self.policy.hash()?
            || self.graph_hash != self.expected_hash()?
        {
            bail!("objective graph binding or hash is invalid");
        }
        let mut goal_ids = HashSet::new();
        let mut active_goal = None;
        for node in &self.nodes {
            node.validate()?;
            if !goal_ids.insert(node.goal_id.as_str()) {
                bail!("objective graph contains duplicate goal {}", node.goal_id);
            }
            if let Some(parent_goal_id) = node.parent_goal_id.as_deref()
                && !goal_ids.contains(parent_goal_id)
            {
                bail!(
                    "objective graph node {} references a missing parent",
                    node.goal_id
                );
            }
            if !node.status.is_terminal() {
                if active_goal.replace(node.goal_id.as_str()).is_some() {
                    bail!("objective graph has more than one active frontier");
                }
            }
        }
        if self.active_goal_id.as_deref() != active_goal {
            bail!("objective graph active frontier does not match node statuses");
        }
        if self.status.is_terminal() && active_goal.is_some() {
            bail!("terminal objective cannot retain an active goal frontier");
        }
        Ok(())
    }

    pub fn add_root_node(&mut self, node: GoalGraphNode) -> Result<()> {
        if !self.nodes.is_empty() {
            bail!("objective graph root node already exists");
        }
        if node.parent_goal_id.is_some()
            || node.parent_epoch_id.is_some()
            || node.parent_strategist_receipt_hash.is_some()
        {
            bail!("objective graph root node cannot have a parent");
        }
        self.nodes.push(node);
        self.active_goal_id = self.nodes.first().map(|node| node.goal_id.clone());
        self.updated_at = timestamp();
        self.reseal()
    }

    pub fn attach_child(&mut self, node: GoalGraphNode) -> Result<()> {
        let parent_goal_id = node
            .parent_goal_id
            .as_deref()
            .context("objective child is missing parent goal")?;
        let parent = self
            .nodes
            .iter()
            .find(|candidate| candidate.goal_id == parent_goal_id)
            .context("objective child references an unknown parent goal")?;
        if !parent.status.is_terminal() || parent.status != GoalStatus::Complete {
            bail!("objective child requires a completed parent goal");
        }
        if parent.final_wave_receipt_hash.is_none()
            || parent.strategist_receipt_hash.is_none()
            || node.parent_epoch_id.as_deref() != Some(parent.epoch_id.as_str())
            || node.parent_strategist_receipt_hash.as_deref()
                != parent.strategist_receipt_hash.as_deref()
        {
            bail!("objective child is not bound to the parent's final wave and strategist receipt");
        }
        if self.active_goal_id.is_some() {
            bail!("objective graph already has an active frontier");
        }
        if self
            .nodes
            .iter()
            .any(|candidate| candidate.goal_id == node.goal_id)
        {
            bail!("objective child goal already exists");
        }
        self.nodes.push(node);
        self.active_goal_id = self.nodes.last().map(|node| node.goal_id.clone());
        self.status = ObjectiveStatus::Running;
        self.stop_reason = None;
        self.updated_at = timestamp();
        self.reseal()
    }

    /// Promote one verified final-review blocker into a bounded child goal.
    ///
    /// This differs from strategist continuation: the parent is blocked by a
    /// concrete review receipt, and replaying that same receipt/objective is
    /// idempotent.
    pub fn append_final_review_blocker_child(
        &mut self,
        parent_goal_id: &str,
        parent_epoch_id: &str,
        review_receipt_hash: &str,
        mut child: GoalGraphNode,
    ) -> Result<bool> {
        let parent_index = self
            .nodes
            .iter()
            .position(|node| node.goal_id == parent_goal_id)
            .context("final review blocker references an unknown parent goal")?;
        if self.nodes.iter().any(|node| {
            node.parent_goal_id.as_deref() == Some(parent_goal_id)
                && node.parent_epoch_id.as_deref() == Some(parent_epoch_id)
                && node.parent_strategist_receipt_hash.as_deref() == Some(review_receipt_hash)
                && node.objective_hash == child.objective_hash
        }) {
            return Ok(false);
        }
        let parent = &self.nodes[parent_index];
        if parent.epoch_id != parent_epoch_id
            || !matches!(parent.status, GoalStatus::Running | GoalStatus::Verifying)
        {
            bail!("final review blocker parent is not an active final candidate");
        }
        if parent.final_wave_receipt_hash.is_none() {
            bail!("final review blocker parent is missing final wave evidence");
        }
        if self.active_goal_id.as_deref() != Some(parent_goal_id) {
            bail!("final review blocker parent is not the active objective frontier");
        }
        if review_receipt_hash.trim().is_empty() {
            bail!("final review blocker requires a review receipt hash");
        }
        child.parent_goal_id = Some(parent_goal_id.to_string());
        child.parent_epoch_id = Some(parent_epoch_id.to_string());
        child.parent_strategist_receipt_hash = Some(review_receipt_hash.to_string());
        child.status = GoalStatus::Planning;
        child.validate()?;
        self.nodes[parent_index].status = GoalStatus::Blocked;
        self.nodes[parent_index].terminal_reason =
            Some("final review blocker promoted to a bounded child goal".to_string());
        self.nodes[parent_index].updated_at = timestamp();
        self.nodes.push(child);
        self.active_goal_id = self.nodes.last().map(|node| node.goal_id.clone());
        self.status = ObjectiveStatus::Running;
        self.stop_reason = None;
        self.updated_at = timestamp();
        self.reseal()?;
        Ok(true)
    }

    pub fn active_node(&self) -> Option<&GoalGraphNode> {
        self.active_goal_id
            .as_deref()
            .and_then(|goal_id| self.nodes.iter().find(|node| node.goal_id == goal_id))
    }

    pub fn update_active_node(
        &mut self,
        goal_id: &str,
        status: GoalStatus,
        final_wave_receipt_hash: Option<String>,
        final_report_path: Option<String>,
        strategist_receipt_hash: Option<String>,
        terminal_reason: Option<String>,
    ) -> Result<()> {
        if self.active_goal_id.as_deref() != Some(goal_id) {
            bail!("objective update does not target the active frontier");
        }
        {
            let node = self
                .nodes
                .iter_mut()
                .find(|node| node.goal_id == goal_id)
                .context("objective update references an unknown goal")?;
            node.status = status.clone();
            node.final_wave_receipt_hash = final_wave_receipt_hash;
            node.final_report_path = final_report_path;
            node.strategist_receipt_hash = strategist_receipt_hash;
            node.terminal_reason = terminal_reason;
            node.updated_at = timestamp();
        }
        if status.is_terminal() {
            self.active_goal_id = None;
        }
        self.updated_at = timestamp();
        self.reseal()
    }

    pub fn set_terminal(&mut self, status: ObjectiveStatus, reason: String) -> Result<()> {
        if !status.is_terminal() {
            bail!("objective terminal update requires a terminal status");
        }
        if self.status.is_terminal() && self.status != status {
            bail!("objective terminal status cannot be reversed");
        }
        self.status = status;
        self.stop_reason = Some(reason);
        self.active_goal_id = None;
        self.updated_at = timestamp();
        self.reseal()
    }

    /// Reopen a needs-user, context-pressure-limited, blocked, or replay-failed frontier for a new epoch.
    /// Prior terminal evidence remains in the event ledger; the node itself
    /// becomes the active planning frontier with a fresh request binding.
    pub fn reopen_for_user_answer(
        &mut self,
        goal_id: &str,
        epoch_id: &str,
        request: &str,
    ) -> Result<()> {
        if !matches!(
            self.status,
            ObjectiveStatus::NeedsUser
                | ObjectiveStatus::Limited
                | ObjectiveStatus::Blocked
                | ObjectiveStatus::Failed
        )
            || self.active_goal_id.is_some()
        {
            bail!("objective is not waiting for a resumable answer");
        }
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.goal_id == goal_id)
            .context("user answer references an unknown objective goal")?;
        if !matches!(
            node.status,
            GoalStatus::NeedsUser | GoalStatus::Complete | GoalStatus::Failed
        ) {
            bail!("objective goal is not waiting for a user answer");
        }
        if epoch_id.trim().is_empty() || request.trim().is_empty() {
            bail!("reopened objective requires epoch and request");
        }
        node.epoch_id = epoch_id.to_string();
        node.request = request.to_string();
        node.request_hash = format!("{:x}", Sha256::digest(request.as_bytes()));
        node.objective_hash = format!("{:x}", Sha256::digest(request.as_bytes()));
        node.status = GoalStatus::Planning;
        node.final_wave_receipt_hash = None;
        node.final_report_path = None;
        node.strategist_receipt_hash = None;
        node.terminal_reason = None;
        node.updated_at = timestamp();
        self.active_goal_id = Some(goal_id.to_string());
        self.status = ObjectiveStatus::Running;
        self.stop_reason = None;
        self.updated_at = timestamp();
        self.reseal()
    }

    pub fn record_progress(&mut self, consecutive_no_progress: usize) -> Result<()> {
        self.consecutive_no_progress = consecutive_no_progress;
        self.updated_at = timestamp();
        self.reseal()
    }

    pub fn record_failure(&mut self, consecutive_failures: usize) -> Result<()> {
        self.consecutive_failures = consecutive_failures;
        self.updated_at = timestamp();
        self.reseal()
    }

    fn reseal(&mut self) -> Result<()> {
        self.graph_hash.clear();
        self.graph_hash = self.expected_hash()?;
        self.validate()
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.graph_hash.clear();
        let bytes = serde_json::to_vec(&payload).context("failed to serialize objective graph")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveEpochOutcomeReceipt {
    pub schema_version: u32,
    pub objective_id: String,
    pub goal_id: String,
    pub epoch_id: String,
    pub session_id: String,
    pub request_hash: String,
    pub scope_hash: String,
    pub policy_hash: String,
    pub status: GoalStatus,
    pub final_wave_path: String,
    pub final_wave_hash: String,
    pub final_report_path: String,
    pub final_report_hash: String,
    pub goal_budget_ledger_hash: String,
    pub strategist_receipt_path: Option<String>,
    pub strategist_receipt_hash: Option<String>,
    pub strategist_decision: Option<String>,
    pub settled_at: String,
    pub receipt_hash: String,
}

impl ObjectiveEpochOutcomeReceipt {
    pub fn seal(
        objective_id: &str,
        goal_id: &str,
        epoch_id: &str,
        session_id: &str,
        request_hash: String,
        scope_hash: String,
        policy_hash: String,
        status: GoalStatus,
        final_wave_path: String,
        final_wave_hash: String,
        final_report_path: String,
        final_report_hash: String,
        goal_budget_ledger_hash: String,
        strategist_receipt_path: Option<String>,
        strategist_receipt_hash: Option<String>,
        strategist_decision: Option<String>,
    ) -> Result<Self> {
        let mut receipt = Self {
            schema_version: OBJECTIVE_EPOCH_OUTCOME_SCHEMA_VERSION,
            objective_id: objective_id.to_string(),
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            session_id: session_id.to_string(),
            request_hash,
            scope_hash,
            policy_hash,
            status,
            final_wave_path,
            final_wave_hash,
            final_report_path,
            final_report_hash,
            goal_budget_ledger_hash,
            strategist_receipt_path,
            strategist_receipt_hash,
            strategist_decision,
            settled_at: timestamp(),
            receipt_hash: String::new(),
        };
        receipt.receipt_hash = receipt.expected_hash()?;
        receipt.validate(objective_id, goal_id, epoch_id)?;
        Ok(receipt)
    }

    pub fn validate(&self, objective_id: &str, goal_id: &str, epoch_id: &str) -> Result<()> {
        if self.schema_version != OBJECTIVE_EPOCH_OUTCOME_SCHEMA_VERSION
            || self.objective_id != objective_id
            || self.goal_id != goal_id
            || self.epoch_id != epoch_id
            || !self.status.is_terminal()
            || self.receipt_hash != self.expected_hash()?
        {
            bail!("objective epoch outcome binding or hash is invalid");
        }
        for (field, value) in [
            ("session_id", self.session_id.as_str()),
            ("request_hash", self.request_hash.as_str()),
            ("scope_hash", self.scope_hash.as_str()),
            ("policy_hash", self.policy_hash.as_str()),
            ("final_wave_path", self.final_wave_path.as_str()),
            ("final_wave_hash", self.final_wave_hash.as_str()),
            ("final_report_path", self.final_report_path.as_str()),
            ("final_report_hash", self.final_report_hash.as_str()),
            (
                "goal_budget_ledger_hash",
                self.goal_budget_ledger_hash.as_str(),
            ),
            ("settled_at", self.settled_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("objective epoch outcome requires non-empty {field}");
            }
        }
        match (
            self.strategist_receipt_path.as_deref(),
            self.strategist_receipt_hash.as_deref(),
            self.strategist_decision.as_deref(),
        ) {
            (None, None, None) => {}
            (Some(path), Some(hash), Some(decision))
                if !path.trim().is_empty()
                    && !hash.trim().is_empty()
                    && !decision.trim().is_empty() => {}
            _ => bail!("objective epoch outcome has an incomplete strategist binding"),
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_hash.clear();
        let bytes =
            serde_json::to_vec(&payload).context("failed to serialize objective epoch outcome")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

pub const OBJECTIVE_BUDGET_LEDGER_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveBudgetReservationStatus {
    Reserved,
    Settled,
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveBudgetReservation {
    pub reservation_id: String,
    pub objective_id: String,
    pub goal_id: String,
    pub epoch_id: String,
    pub policy_hash: String,
    pub reserved_calls: usize,
    pub reserved_tokens: u64,
    pub reserved_cost_micros: u64,
    pub reserved_unknown_calls: usize,
    pub reserved_premium_calls: usize,
    pub status: ObjectiveBudgetReservationStatus,
    pub actual_calls: Option<usize>,
    pub actual_tokens: Option<u64>,
    pub actual_cost_micros: Option<u64>,
    pub actual_unknown_calls: Option<usize>,
    pub actual_premium_calls: Option<usize>,
    pub cache_hits: Option<usize>,
    pub duration_ms: Option<u64>,
    pub fallback_reasons: Vec<String>,
    pub created_at: String,
    pub settled_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveBudgetLedger {
    pub schema_version: u32,
    pub objective_id: String,
    pub policy_hash: String,
    pub reservations: Vec<ObjectiveBudgetReservation>,
    pub updated_at: String,
    pub ledger_hash: String,
}

impl ObjectiveBudgetLedger {
    fn seal(mut self) -> Result<Self> {
        self.ledger_hash.clear();
        self.ledger_hash = self.expected_hash()?;
        Ok(self)
    }

    fn validate(&self, objective_id: &str, policy_hash: &str) -> Result<()> {
        if self.schema_version != OBJECTIVE_BUDGET_LEDGER_SCHEMA_VERSION
            || self.objective_id != objective_id
            || self.policy_hash != policy_hash
            || self.ledger_hash != self.expected_hash()?
        {
            bail!("objective budget ledger binding or hash is invalid");
        }
        let mut reservation_ids = HashSet::new();
        for reservation in &self.reservations {
            if reservation.objective_id != objective_id
                || reservation.policy_hash != policy_hash
                || reservation.reservation_id.trim().is_empty()
                || reservation.goal_id.trim().is_empty()
                || reservation.epoch_id.trim().is_empty()
                || !reservation_ids.insert(reservation.reservation_id.as_str())
            {
                bail!("objective budget ledger contains an invalid reservation binding");
            }
            match reservation.status {
                ObjectiveBudgetReservationStatus::Reserved
                    if reservation.actual_calls.is_some()
                        || reservation.actual_tokens.is_some()
                        || reservation.actual_cost_micros.is_some()
                        || reservation.settled_at.is_some() =>
                {
                    bail!("reserved objective budget cannot contain settlement fields");
                }
                ObjectiveBudgetReservationStatus::Settled
                    if reservation.actual_calls.is_none() || reservation.settled_at.is_none() =>
                {
                    bail!("settled objective budget requires actual calls and settled_at");
                }
                ObjectiveBudgetReservationStatus::Released if reservation.settled_at.is_none() => {
                    bail!("released objective budget requires settled_at");
                }
                ObjectiveBudgetReservationStatus::Reserved
                | ObjectiveBudgetReservationStatus::Settled
                | ObjectiveBudgetReservationStatus::Released => {}
            }
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.ledger_hash.clear();
        let bytes =
            serde_json::to_vec(&payload).context("failed to serialize objective budget ledger")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveEventKind {
    Started,
    GoalAttached,
    GoalOutcomeRecorded,
    StrategistContinueAccepted,
    UserAnswerAccepted,
    ContextPressureResumed,
    ChildDispatchReserved,
    FinalReviewBlockerPromoted,
    ObjectiveBudgetSettled,
    FrontierAdvanced,
    NeedsUser,
    Stopped,
    Limited,
    Blocked,
    Completed,
    Failed,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveEvent {
    pub schema_version: u32,
    pub objective_id: String,
    pub sequence: u64,
    pub idempotency_key: String,
    pub kind: ObjectiveEventKind,
    pub payload: Value,
    pub previous_hash: String,
    pub created_at: String,
    pub event_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveLease {
    pub schema_version: u32,
    pub objective_id: String,
    pub owner_session_id: String,
    pub lease_id: String,
    pub acquired_at: String,
    pub expires_at: String,
}

#[derive(Debug)]
pub struct ObjectiveLeaseGuard {
    lease: ObjectiveLease,
    file: fs::File,
    path: PathBuf,
}

impl ObjectiveLeaseGuard {
    pub fn lease(&self) -> &ObjectiveLease {
        &self.lease
    }

    pub fn release(self) -> Result<()> {
        self.file
            .unlock()
            .with_context(|| format!("failed to unlock {}", self.path.display()))?;
        Ok(())
    }
}

impl ObjectiveLease {
    fn validate(&self, objective_id: &str) -> Result<()> {
        if self.schema_version != 1 || self.objective_id != objective_id {
            bail!("objective lease has an invalid schema or objective binding");
        }
        for (field, value) in [
            ("owner_session_id", self.owner_session_id.as_str()),
            ("lease_id", self.lease_id.as_str()),
            ("acquired_at", self.acquired_at.as_str()),
            ("expires_at", self.expires_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("objective lease requires non-empty {field}");
            }
        }
        DateTime::parse_from_rfc3339(&self.acquired_at)
            .context("objective lease has invalid acquired_at")?;
        DateTime::parse_from_rfc3339(&self.expires_at)
            .context("objective lease has invalid expires_at")?;
        Ok(())
    }
}

impl ObjectiveEvent {
    fn seal(
        objective_id: &str,
        sequence: u64,
        idempotency_key: &str,
        kind: ObjectiveEventKind,
        payload: Value,
        previous_hash: String,
    ) -> Result<Self> {
        if objective_id.trim().is_empty() || idempotency_key.trim().is_empty() {
            bail!("objective events require non-empty objective and idempotency ids");
        }
        let mut event = Self {
            schema_version: OBJECTIVE_EVENT_SCHEMA_VERSION,
            objective_id: objective_id.to_string(),
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
        let bytes = serde_json::to_vec(&payload).context("failed to serialize objective event")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

fn validate_objective_event_transition(
    active: &mut bool,
    terminated: &mut bool,
    event: &ObjectiveEvent,
) -> Result<()> {
    validate_objective_event_payload(event)?;
    match event.kind {
        ObjectiveEventKind::Started => {
            if *active || *terminated {
                bail!(
                    "objective cannot start while another objective lifecycle is active or terminal"
                );
            }
            *active = true;
        }
        ObjectiveEventKind::UserAnswerAccepted => {
            if !*terminated || *active {
                bail!("objective user answer requires a terminal objective");
            }
            *active = true;
            *terminated = false;
        }
        ObjectiveEventKind::ContextPressureResumed => {
            if !*terminated || *active {
                bail!("context-pressure resume requires a terminal objective");
            }
            *active = true;
            *terminated = false;
        }
        ObjectiveEventKind::GoalAttached
        | ObjectiveEventKind::GoalOutcomeRecorded
        | ObjectiveEventKind::StrategistContinueAccepted
        | ObjectiveEventKind::ChildDispatchReserved
        | ObjectiveEventKind::FinalReviewBlockerPromoted
        | ObjectiveEventKind::ObjectiveBudgetSettled
        | ObjectiveEventKind::FrontierAdvanced => {
            if !*active {
                bail!("objective event requires an active objective");
            }
        }
        ObjectiveEventKind::NeedsUser
        | ObjectiveEventKind::Stopped
        | ObjectiveEventKind::Limited
        | ObjectiveEventKind::Blocked
        | ObjectiveEventKind::Completed
        | ObjectiveEventKind::Failed
        | ObjectiveEventKind::Aborted => {
            if !*active {
                bail!("objective terminal event requires an active objective");
            }
            *active = false;
            *terminated = true;
        }
    }
    Ok(())
}

fn validate_objective_event_payload(event: &ObjectiveEvent) -> Result<()> {
    let required_non_empty = |field: &str| -> Result<()> {
        if event
            .payload
            .get(field)
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
        {
            bail!(
                "objective event {:?} requires non-empty {field}",
                event.kind
            );
        }
        Ok(())
    };
    match event.kind {
        ObjectiveEventKind::Started => {}
        ObjectiveEventKind::GoalAttached => {
            required_non_empty("goal_id")?;
            required_non_empty("epoch_id")?;
        }
        ObjectiveEventKind::GoalOutcomeRecorded => {
            required_non_empty("goal_id")?;
            required_non_empty("epoch_id")?;
            required_non_empty("receipt_hash")?;
        }
        ObjectiveEventKind::StrategistContinueAccepted => {
            required_non_empty("parent_goal_id")?;
            required_non_empty("parent_epoch_id")?;
            required_non_empty("receipt_hash")?;
            required_non_empty("next_objective")?;
        }
        ObjectiveEventKind::UserAnswerAccepted => {
            required_non_empty("goal_id")?;
            required_non_empty("epoch_id")?;
            required_non_empty("answer")?;
        }
        ObjectiveEventKind::ContextPressureResumed => {
            required_non_empty("active_goal_id")?;
            required_non_empty("epoch_id")?;
            required_non_empty("reason")?;
        }
        ObjectiveEventKind::ChildDispatchReserved => {
            required_non_empty("reservation_id")?;
            required_non_empty("goal_id")?;
            required_non_empty("epoch_id")?;
        }
        ObjectiveEventKind::FinalReviewBlockerPromoted => {
            required_non_empty("parent_goal_id")?;
            required_non_empty("parent_epoch_id")?;
            required_non_empty("child_goal_id")?;
            required_non_empty("child_epoch_id")?;
            required_non_empty("review_receipt_hash")?;
            required_non_empty("blocker_signature")?;
        }
        ObjectiveEventKind::ObjectiveBudgetSettled => {
            required_non_empty("reservation_id")?;
            required_non_empty("status")?;
        }
        ObjectiveEventKind::FrontierAdvanced => required_non_empty("active_goal_id")?,
        ObjectiveEventKind::NeedsUser
        | ObjectiveEventKind::Stopped
        | ObjectiveEventKind::Limited
        | ObjectiveEventKind::Blocked
        | ObjectiveEventKind::Completed
        | ObjectiveEventKind::Failed
        | ObjectiveEventKind::Aborted => {}
    }
    Ok(())
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

struct ObjectiveEventLedgerScan {
    event_count: u64,
    previous_hash: String,
    active: bool,
    terminated: bool,
    duplicate: Option<ObjectiveEvent>,
}

impl ObjectiveEventLedgerScan {
    fn empty() -> Self {
        Self {
            event_count: 0,
            previous_hash: "0".repeat(64),
            active: false,
            terminated: false,
            duplicate: None,
        }
    }
}

struct GoalEpochEventLedgerScan {
    event_count: u64,
    previous_hash: String,
    active_epoch: Option<String>,
    duplicate: Option<GoalEpochEvent>,
}

impl GoalEpochEventLedgerScan {
    fn empty() -> Self {
        Self {
            event_count: 0,
            previous_hash: "0".repeat(64),
            active_epoch: None,
            duplicate: None,
        }
    }
}

fn scan_objective_event_ledger(
    path: &Path,
    objective_id: &str,
    idempotency_key: &str,
) -> Result<ObjectiveEventLedgerScan> {
    if !path.exists() {
        return Ok(ObjectiveEventLedgerScan::empty());
    }

    let file =
        fs::File::open(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut scan = ObjectiveEventLedgerScan::empty();
    let mut idempotency_keys = HashSet::new();
    for (sequence, line) in BufReader::new(file).lines().enumerate() {
        let line = line
            .with_context(|| format!("failed to read {} line {}", path.display(), sequence + 1))?;
        let event: ObjectiveEvent = serde_json::from_str(&line)
            .with_context(|| format!("failed to parse {} line {}", path.display(), sequence + 1))?;
        let unique_idempotency_key = idempotency_keys.insert(event.idempotency_key.clone());
        if event.schema_version != OBJECTIVE_EVENT_SCHEMA_VERSION
            || event.objective_id != objective_id
            || event.sequence != sequence as u64
            || event.idempotency_key.trim().is_empty()
            || !unique_idempotency_key
            || event.previous_hash != scan.previous_hash
            || event.event_hash != event.expected_hash()?
        {
            bail!("objective event ledger integrity check failed at sequence {sequence}");
        }
        validate_objective_event_transition(&mut scan.active, &mut scan.terminated, &event)?;
        if event.idempotency_key == idempotency_key {
            scan.duplicate = Some(event.clone());
        }
        scan.previous_hash = event.event_hash;
        scan.event_count += 1;
    }
    Ok(scan)
}

fn scan_goal_epoch_event_ledger(
    path: &Path,
    goal_id: &str,
    idempotency_key: &str,
) -> Result<GoalEpochEventLedgerScan> {
    if !path.exists() {
        return Ok(GoalEpochEventLedgerScan::empty());
    }

    let file =
        fs::File::open(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut scan = GoalEpochEventLedgerScan::empty();
    let mut idempotency_keys = HashSet::new();
    for (sequence, line) in BufReader::new(file).lines().enumerate() {
        let line = line
            .with_context(|| format!("failed to read {} line {}", path.display(), sequence + 1))?;
        let event: GoalEpochEvent = serde_json::from_str(&line)
            .with_context(|| format!("failed to parse {} line {}", path.display(), sequence + 1))?;
        let unique_idempotency_key = idempotency_keys.insert(event.idempotency_key.clone());
        if event.schema_version != 1
            || event.goal_id != goal_id
            || event.sequence != sequence as u64
            || event.idempotency_key.trim().is_empty()
            || !unique_idempotency_key
            || event.previous_hash != scan.previous_hash
            || event.event_hash != event.expected_hash()?
        {
            bail!("goal epoch ledger integrity check failed at sequence {sequence}");
        }
        validate_goal_epoch_transition(&mut scan.active_epoch, &event)?;
        if event.idempotency_key == idempotency_key {
            scan.duplicate = Some(event.clone());
        }
        scan.previous_hash = event.event_hash;
        scan.event_count += 1;
    }
    Ok(scan)
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
    PlanReused,
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

pub const CANONICAL_PLAN_BUNDLE_SCHEMA_VERSION: u32 = 1;

/// The approved plan and the receipt chain that authorizes it must be read as
/// one durable snapshot. The legacy `.plan.json` file remains a compatibility
/// mirror, while this bundle is the recovery authority after a process stop.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalPlanBundle {
    pub schema_version: u32,
    pub goal_id: String,
    pub plan: crate::plan_graph::PlanGraph,
    pub approval: crate::plan_review::PlanApprovalState,
    pub binding_hash: String,
}

pub const CANONICAL_PLAN_POINTER_SCHEMA_VERSION: u32 = 1;

/// A small, independently atomic pointer prevents a stale compatibility
/// mirror from being mistaken for the active approved plan after a restart.
/// The bundle remains the source of the plan and receipt contents; this file
/// only binds the active identity to that bundle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalPlanPointer {
    pub schema_version: u32,
    pub goal_id: String,
    pub plan_id: String,
    pub plan_hash: String,
    pub revision: usize,
    pub bundle_path: String,
    pub bundle_binding_hash: String,
    pub updated_at: String,
    pub pointer_hash: String,
}

impl CanonicalPlanPointer {
    fn seal(
        goal_id: &str,
        plan: &crate::plan_graph::PlanGraph,
        bundle_path: &Path,
        bundle_binding_hash: &str,
    ) -> Result<Self> {
        let mut pointer = Self {
            schema_version: CANONICAL_PLAN_POINTER_SCHEMA_VERSION,
            goal_id: goal_id.to_string(),
            plan_id: plan.plan_id.clone(),
            plan_hash: plan.plan_hash.clone(),
            revision: plan.revision,
            bundle_path: bundle_path.to_string_lossy().to_string(),
            bundle_binding_hash: bundle_binding_hash.to_string(),
            updated_at: timestamp(),
            pointer_hash: String::new(),
        };
        pointer.pointer_hash = pointer.expected_hash()?;
        pointer.validate(bundle_path)?;
        Ok(pointer)
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.pointer_hash.clear();
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&payload)?)))
    }

    fn validate(&self, expected_bundle_path: &Path) -> Result<()> {
        if self.schema_version != CANONICAL_PLAN_POINTER_SCHEMA_VERSION
            || self.goal_id.trim().is_empty()
            || self.plan_id.trim().is_empty()
            || self.plan_hash.trim().is_empty()
            || self.bundle_binding_hash.trim().is_empty()
            || self.updated_at.trim().is_empty()
            || Path::new(&self.bundle_path) != expected_bundle_path
        {
            bail!("canonical plan pointer has an invalid identity or bundle path");
        }
        if self.pointer_hash != self.expected_hash()? {
            bail!("canonical plan pointer binding hash mismatch");
        }
        Ok(())
    }
}

impl CanonicalPlanBundle {
    fn seal(
        plan: crate::plan_graph::PlanGraph,
        approval: crate::plan_review::PlanApprovalState,
    ) -> Result<Self> {
        let mut bundle = Self {
            schema_version: CANONICAL_PLAN_BUNDLE_SCHEMA_VERSION,
            goal_id: plan.goal_id.clone(),
            plan,
            approval,
            binding_hash: String::new(),
        };
        bundle.binding_hash = bundle.expected_hash()?;
        bundle.validate()?;
        Ok(bundle)
    }

    fn validate(&self) -> Result<()> {
        if self.schema_version != CANONICAL_PLAN_BUNDLE_SCHEMA_VERSION
            || self.goal_id.trim().is_empty()
            || self.goal_id != self.plan.goal_id
            || self.approval.goal_id != self.goal_id
        {
            bail!("canonical plan bundle has an invalid identity or schema");
        }
        self.plan.validate()?;
        self.approval.validate_against(&self.plan)?;
        if self.binding_hash != self.expected_hash()? {
            bail!("canonical plan bundle binding hash mismatch");
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.binding_hash.clear();
        let bytes =
            serde_json::to_vec(&payload).context("failed to serialize canonical plan bundle")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

impl StateStore {
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        Self {
            root: workspace.into().join(".gear"),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn global_provider_cooldown_path(&self) -> PathBuf {
        self.root.join("provider-global-cooldown.json")
    }

    pub fn read_global_provider_cooldown(&self) -> Result<Option<GlobalProviderCooldown>> {
        let path = self.global_provider_cooldown_path();
        if !path.is_file() {
            return self.infer_legacy_free_provider_cooldown();
        }
        let cooldown: GlobalProviderCooldown = read_json_file(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        cooldown
            .validate()
            .with_context(|| format!("invalid global provider cooldown at {}", path.display()))?;
        Ok(Some(cooldown))
    }

    fn infer_legacy_free_provider_cooldown(&self) -> Result<Option<GlobalProviderCooldown>> {
        const COOLDOWN_MS: u64 = 24 * 60 * 60 * 1000;
        let mut failed_models = Vec::new();
        let mut latest_failure_ms = 0;
        let mut source_task = None;
        let mut reason = None;
        let Ok(entries) = fs::read_dir(self.workers_dir()) else {
            return Ok(None);
        };
        for entry in entries.flatten().take(128) {
            let path = entry.path().join("provider-cooldown.json");
            let Ok(contents) = fs::read_to_string(&path) else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(&contents) else {
                continue;
            };
            let Some(model) = value.get("model").and_then(Value::as_str) else {
                continue;
            };
            if !model
                .split('/')
                .next_back()
                .is_some_and(|suffix| suffix.ends_with("-free"))
            {
                continue;
            }
            let failure = value
                .get("failure")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            if !(failure.contains("rate limit")
                || failure.contains("quota")
                || failure.contains("free usage")
                || failure.contains("limit exhausted")
                || failure.contains("too many requests"))
            {
                continue;
            }
            let failed_at = value
                .get("failed_at")
                .and_then(Value::as_str)
                .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
                .map(|timestamp| u64::try_from(timestamp.timestamp_millis()).unwrap_or(0))
                .unwrap_or(0);
            let now_ms = u64::try_from(Local::now().timestamp_millis()).unwrap_or(0);
            if failed_at == 0 || now_ms.saturating_sub(failed_at) >= COOLDOWN_MS {
                continue;
            }
            if !failed_models
                .iter()
                .any(|known: &String| known.eq_ignore_ascii_case(model))
            {
                failed_models.push(model.to_string());
            }
            if failed_at >= latest_failure_ms {
                latest_failure_ms = failed_at;
                source_task = value
                    .get("task_id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                reason = value
                    .get("failure")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
            }
        }
        if failed_models.is_empty() {
            return Ok(None);
        }
        failed_models.sort_unstable();
        GlobalProviderCooldown {
            schema_version: GLOBAL_PROVIDER_COOLDOWN_SCHEMA_VERSION,
            provider_scope: "opencode-free-tier".to_string(),
            failed_models,
            reason: reason.unwrap_or_else(|| "legacy free provider quota".to_string()),
            failed_at: timestamp(),
            cooldown_until_ms: latest_failure_ms.saturating_add(COOLDOWN_MS),
            source_task: source_task.unwrap_or_else(|| "legacy-provider-receipt".to_string()),
            source_attempt: 0,
            recorded_at: timestamp(),
            receipt_hash: String::new(),
        }
        .seal()
        .map(Some)
    }

    pub fn write_global_provider_cooldown(
        &self,
        cooldown: GlobalProviderCooldown,
    ) -> Result<PathBuf> {
        let cooldown = cooldown.seal()?;
        let path = self.global_provider_cooldown_path();
        write_json_atomic(&path, &cooldown)?;
        Ok(path)
    }

    /// The pre-`.gear` runtime root remains available for explicit migration
    /// and forensic inspection. New state is always written under `.gear`.
    pub fn legacy_root(&self) -> PathBuf {
        self.root
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(".gearbox-agent")
    }

    pub fn has_legacy_root(&self) -> bool {
        self.legacy_root().exists()
    }

    pub fn initialize(&self) -> Result<()> {
        for path in [
            self.sessions_dir(),
            self.goals_dir(),
            self.tasks_dir(),
            self.plan_node_runs_dir(),
            self.plan_wave_runs_dir(),
            self.plan_node_session_bindings_dir(),
            self.task_route_receipts_dir(),
            self.model_call_ledger_dir(),
            self.plans_dir(),
            self.plan_reviews_dir(),
            self.events_dir(),
            self.epochs_dir(),
            self.budgets_dir(),
            self.objectives_dir(),
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

    pub fn plan_node_runs_dir(&self) -> PathBuf {
        self.root.join("plan-node-runs")
    }

    pub fn plan_node_runs_path(&self, goal_id: &str) -> PathBuf {
        self.plan_node_runs_dir().join(format!("{goal_id}.json"))
    }

    pub fn plan_wave_runs_dir(&self) -> PathBuf {
        self.root.join("plan-wave-runs")
    }

    pub fn plan_wave_run_path(&self, goal_id: &str, epoch_id: &str, wave_id: &str) -> PathBuf {
        self.plan_wave_runs_dir()
            .join(format!("{goal_id}-{epoch_id}-{wave_id}.json"))
    }

    pub fn plan_node_session_bindings_dir(&self) -> PathBuf {
        self.root.join("plan-node-session-bindings")
    }

    pub fn model_call_ledger_dir(&self) -> PathBuf {
        self.root.join("model-call-ledger")
    }

    pub fn task_route_receipts_dir(&self) -> PathBuf {
        self.root.join("task-route-receipts")
    }

    pub fn task_route_receipt_path(
        &self,
        goal_id: &str,
        epoch_id: &str,
        task_id: &str,
        attempt: usize,
    ) -> PathBuf {
        self.task_route_receipts_dir()
            .join(format!("{goal_id}-{epoch_id}-{task_id}-{attempt}.json"))
    }

    pub fn model_call_ledger_path(&self, goal_id: &str) -> PathBuf {
        self.model_call_ledger_dir()
            .join(format!("{goal_id}.jsonl"))
    }

    pub fn plan_node_session_binding_path(
        &self,
        goal_id: &str,
        epoch_id: &str,
        task_id: &str,
        attempt: usize,
    ) -> PathBuf {
        self.plan_node_session_bindings_dir()
            .join(format!("{goal_id}-{epoch_id}-{task_id}-{attempt}.json"))
    }

    pub fn plans_dir(&self) -> PathBuf {
        self.root.join("plans")
    }

    pub fn canonical_plan_bundle_path(&self, goal_id: &str) -> PathBuf {
        self.plans_dir()
            .join(format!("{goal_id}.canonical.bundle.json"))
    }

    pub fn canonical_plan_pointer_path(&self, goal_id: &str) -> PathBuf {
        self.plans_dir()
            .join(format!("{goal_id}.active-plan.json"))
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

    pub fn objectives_dir(&self) -> PathBuf {
        self.root.join("objectives")
    }

    pub fn objective_graph_path(&self, objective_id: &str) -> PathBuf {
        self.objectives_dir()
            .join(format!("{objective_id}.graph.json"))
    }

    pub fn objective_events_path(&self, objective_id: &str) -> PathBuf {
        self.objectives_dir().join(format!("{objective_id}.jsonl"))
    }

    pub fn objective_lease_path(&self, objective_id: &str) -> PathBuf {
        self.objectives_dir()
            .join(format!("{objective_id}.lease.json"))
    }

    pub fn objective_epoch_outcome_path(
        &self,
        objective_id: &str,
        goal_id: &str,
        epoch_id: &str,
    ) -> PathBuf {
        self.objectives_dir()
            .join(format!("{objective_id}.{goal_id}.{epoch_id}.outcome.json"))
    }

    pub fn objective_budget_ledger_path(&self, objective_id: &str) -> PathBuf {
        self.objectives_dir()
            .join(format!("{objective_id}.reservations.json"))
    }

    pub fn read_objective_budget_ledger(
        &self,
        objective_id: &str,
        policy_hash: &str,
    ) -> Result<ObjectiveBudgetLedger> {
        let path = self.objective_budget_ledger_path(objective_id);
        if !path.exists() {
            return ObjectiveBudgetLedger {
                schema_version: OBJECTIVE_BUDGET_LEDGER_SCHEMA_VERSION,
                objective_id: objective_id.to_string(),
                policy_hash: policy_hash.to_string(),
                reservations: Vec::new(),
                updated_at: timestamp(),
                ledger_hash: String::new(),
            }
            .seal();
        }
        let ledger: ObjectiveBudgetLedger = read_json_file(&path)?;
        ledger.validate(objective_id, policy_hash)?;
        Ok(ledger)
    }

    fn write_objective_budget_ledger(&self, ledger: ObjectiveBudgetLedger) -> Result<PathBuf> {
        let objective_id = ledger.objective_id.clone();
        let policy_hash = ledger.policy_hash.clone();
        let ledger = ledger.seal()?;
        ledger.validate(&objective_id, &policy_hash)?;
        let path = self.objective_budget_ledger_path(&objective_id);
        write_json_atomic(&path, &ledger)?;
        Ok(path)
    }

    pub fn reserve_objective_epoch(
        &self,
        lease: &ObjectiveLeaseGuard,
        reservation_id: &str,
        goal_id: &str,
        epoch_id: &str,
        policy: &ObjectivePolicy,
        reserved_calls: usize,
        reserved_tokens: u64,
        reserved_cost_micros: u64,
        reserved_unknown_calls: usize,
        reserved_premium_calls: usize,
    ) -> Result<ObjectiveBudgetReservation> {
        if lease.lease.objective_id.trim().is_empty()
            || reservation_id.trim().is_empty()
            || goal_id.trim().is_empty()
            || epoch_id.trim().is_empty()
        {
            bail!("objective budget reservation requires non-empty bindings");
        }
        let policy_hash = policy.hash()?;
        let mut ledger =
            self.read_objective_budget_ledger(&lease.lease.objective_id, &policy_hash)?;
        if let Some(existing) = ledger
            .reservations
            .iter()
            .find(|reservation| reservation.reservation_id == reservation_id)
        {
            if existing.goal_id == goal_id && existing.epoch_id == epoch_id {
                return Ok(existing.clone());
            }
            bail!("objective budget reservation id conflicts with an existing reservation");
        }
        let mut calls = 0usize;
        let mut tokens = 0u64;
        let mut cost = 0u64;
        let mut unknown_calls = 0usize;
        let mut premium_calls = 0usize;
        for reservation in &ledger.reservations {
            match reservation.status {
                ObjectiveBudgetReservationStatus::Reserved => {
                    calls = calls.saturating_add(reservation.reserved_calls);
                    tokens = tokens.saturating_add(reservation.reserved_tokens);
                    cost = cost.saturating_add(reservation.reserved_cost_micros);
                    unknown_calls =
                        unknown_calls.saturating_add(reservation.reserved_unknown_calls);
                    premium_calls =
                        premium_calls.saturating_add(reservation.reserved_premium_calls);
                }
                ObjectiveBudgetReservationStatus::Settled => {
                    calls = calls.saturating_add(reservation.actual_calls.unwrap_or(0));
                    tokens = tokens.saturating_add(reservation.actual_tokens.unwrap_or(0));
                    cost = cost.saturating_add(reservation.actual_cost_micros.unwrap_or(0));
                    unknown_calls =
                        unknown_calls.saturating_add(reservation.actual_unknown_calls.unwrap_or(0));
                    premium_calls =
                        premium_calls.saturating_add(reservation.actual_premium_calls.unwrap_or(0));
                }
                ObjectiveBudgetReservationStatus::Released => {}
            }
        }
        if calls.saturating_add(reserved_calls) > policy.max_calls
            || tokens.saturating_add(reserved_tokens) > policy.max_tokens
            || (policy.max_cost_micros != u64::MAX
                && cost.saturating_add(reserved_cost_micros) > policy.max_cost_micros)
            || unknown_calls.saturating_add(reserved_unknown_calls) > policy.max_unknown_usage_calls
        {
            bail!("objective budget exhausted before epoch reservation");
        }
        let reservation = ObjectiveBudgetReservation {
            reservation_id: reservation_id.to_string(),
            objective_id: lease.lease.objective_id.clone(),
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            policy_hash,
            reserved_calls,
            reserved_tokens,
            reserved_cost_micros,
            reserved_unknown_calls,
            reserved_premium_calls,
            status: ObjectiveBudgetReservationStatus::Reserved,
            actual_calls: None,
            actual_tokens: None,
            actual_cost_micros: None,
            actual_unknown_calls: None,
            actual_premium_calls: None,
            cache_hits: None,
            duration_ms: None,
            fallback_reasons: Vec::new(),
            created_at: timestamp(),
            settled_at: None,
        };
        ledger.reservations.push(reservation.clone());
        ledger.updated_at = timestamp();
        self.write_objective_budget_ledger(ledger)?;
        Ok(reservation)
    }

    pub fn settle_objective_epoch(
        &self,
        lease: &ObjectiveLeaseGuard,
        reservation_id: &str,
        actual_calls: usize,
        actual_tokens: Option<u64>,
        actual_cost_micros: Option<u64>,
        actual_unknown_calls: usize,
        actual_premium_calls: usize,
        cache_hits: usize,
        duration_ms: Option<u64>,
        fallback_reasons: Vec<String>,
    ) -> Result<ObjectiveBudgetReservation> {
        let policy_hash = {
            let ledger_path = self.objective_budget_ledger_path(&lease.lease.objective_id);
            let ledger: ObjectiveBudgetLedger = read_json_file(&ledger_path)?;
            ledger.policy_hash
        };
        let mut ledger =
            self.read_objective_budget_ledger(&lease.lease.objective_id, &policy_hash)?;
        let reservation = ledger
            .reservations
            .iter_mut()
            .find(|reservation| reservation.reservation_id == reservation_id)
            .context("objective budget settlement references an unknown reservation")?;
        if reservation.status == ObjectiveBudgetReservationStatus::Settled {
            if reservation.actual_calls == Some(actual_calls)
                && reservation.actual_tokens == actual_tokens
                && reservation.actual_cost_micros == actual_cost_micros
                && reservation.actual_unknown_calls == Some(actual_unknown_calls)
                && reservation.actual_premium_calls == Some(actual_premium_calls)
            {
                return Ok(reservation.clone());
            }
            bail!("objective budget reservation was already settled with different usage");
        }
        if reservation.status != ObjectiveBudgetReservationStatus::Reserved {
            bail!("only a reserved objective budget can be settled");
        }
        if actual_calls > reservation.reserved_calls
            || actual_tokens.is_some_and(|value| value > reservation.reserved_tokens)
            || actual_cost_micros.is_some_and(|value| value > reservation.reserved_cost_micros)
            || actual_unknown_calls > reservation.reserved_unknown_calls
            || actual_premium_calls > reservation.reserved_premium_calls
        {
            bail!("objective budget settlement exceeds its reservation");
        }
        reservation.status = ObjectiveBudgetReservationStatus::Settled;
        reservation.actual_calls = Some(actual_calls);
        reservation.actual_tokens = actual_tokens;
        reservation.actual_cost_micros = actual_cost_micros;
        reservation.actual_unknown_calls = Some(actual_unknown_calls);
        reservation.actual_premium_calls = Some(actual_premium_calls);
        reservation.cache_hits = Some(cache_hits);
        reservation.duration_ms = duration_ms;
        reservation.fallback_reasons = fallback_reasons;
        reservation.settled_at = Some(timestamp());
        let settled = reservation.clone();
        ledger.updated_at = timestamp();
        self.write_objective_budget_ledger(ledger)?;
        Ok(settled)
    }

    pub fn release_objective_epoch(
        &self,
        lease: &ObjectiveLeaseGuard,
        reservation_id: &str,
    ) -> Result<ObjectiveBudgetReservation> {
        let ledger_path = self.objective_budget_ledger_path(&lease.lease.objective_id);
        let ledger: ObjectiveBudgetLedger = read_json_file(&ledger_path)?;
        let policy_hash = ledger.policy_hash;
        let mut ledger =
            self.read_objective_budget_ledger(&lease.lease.objective_id, &policy_hash)?;
        let reservation = ledger
            .reservations
            .iter_mut()
            .find(|reservation| reservation.reservation_id == reservation_id)
            .context("objective budget release references an unknown reservation")?;
        if reservation.status == ObjectiveBudgetReservationStatus::Released {
            return Ok(reservation.clone());
        }
        if reservation.status != ObjectiveBudgetReservationStatus::Reserved {
            bail!("only a reserved objective budget can be released");
        }
        reservation.status = ObjectiveBudgetReservationStatus::Released;
        reservation.settled_at = Some(timestamp());
        let released = reservation.clone();
        ledger.updated_at = timestamp();
        self.write_objective_budget_ledger(ledger)?;
        Ok(released)
    }

    pub fn write_objective_epoch_outcome(
        &self,
        receipt: &ObjectiveEpochOutcomeReceipt,
    ) -> Result<PathBuf> {
        receipt.validate(&receipt.objective_id, &receipt.goal_id, &receipt.epoch_id)?;
        let path = self.objective_epoch_outcome_path(
            &receipt.objective_id,
            &receipt.goal_id,
            &receipt.epoch_id,
        );
        write_json_atomic(&path, receipt)?;
        Ok(path)
    }

    pub fn read_objective_epoch_outcome(
        &self,
        objective_id: &str,
        goal_id: &str,
        epoch_id: &str,
    ) -> Result<Option<ObjectiveEpochOutcomeReceipt>> {
        let path = self.objective_epoch_outcome_path(objective_id, goal_id, epoch_id);
        if !path.exists() {
            return Ok(None);
        }
        let receipt: ObjectiveEpochOutcomeReceipt = read_json_file(&path)?;
        receipt.validate(objective_id, goal_id, epoch_id)?;
        Ok(Some(receipt))
    }

    pub fn write_objective_graph(&self, graph: &ObjectiveGraph) -> Result<PathBuf> {
        graph.validate()?;
        let path = self.objective_graph_path(&graph.objective_id);
        write_json_atomic(&path, graph)?;
        Ok(path)
    }

    pub fn read_objective_graph(&self, objective_id: &str) -> Result<Option<ObjectiveGraph>> {
        let path = self.objective_graph_path(objective_id);
        if !path.exists() {
            return Ok(None);
        }
        let graph: ObjectiveGraph = read_json_file(&path)?;
        graph.validate()?;
        Ok(Some(graph))
    }

    pub fn find_objective_graph_for_root_session(
        &self,
        root_session_id: &str,
    ) -> Result<Option<ObjectiveGraph>> {
        let entries = fs::read_dir(self.objectives_dir())?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json")
                || !path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".graph.json"))
            {
                continue;
            }
            let graph: ObjectiveGraph = read_json_file(&path)?;
            graph.validate()?;
            if graph.root_session_id == root_session_id {
                return Ok(Some(graph));
            }
        }
        Ok(None)
    }

    pub fn append_objective_event(
        &self,
        objective_id: &str,
        idempotency_key: &str,
        kind: ObjectiveEventKind,
        payload: Value,
    ) -> Result<ObjectiveEvent> {
        let path = self.objective_events_path(objective_id);
        let scan = scan_objective_event_ledger(&path, objective_id, idempotency_key)?;
        if let Some(recorded) = scan.duplicate.as_ref() {
            if recorded.kind == kind && recorded.payload == payload {
                return Ok(recorded.clone());
            }
            bail!("objective event idempotency key conflicts with an existing event");
        }
        let event = ObjectiveEvent::seal(
            objective_id,
            scan.event_count,
            idempotency_key,
            kind,
            payload,
            scan.previous_hash,
        )?;
        let mut active = scan.active;
        let mut terminated = scan.terminated;
        validate_objective_event_transition(&mut active, &mut terminated, &event)?;
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

    pub fn read_objective_events(&self, objective_id: &str) -> Result<Vec<ObjectiveEvent>> {
        let path = self.objective_events_path(objective_id);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file =
            fs::File::open(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let mut events = Vec::new();
        let mut previous_hash = "0".repeat(64);
        let mut active = false;
        let mut terminated = false;
        let mut idempotency_keys = HashSet::new();
        for (sequence, line) in BufReader::new(file).lines().enumerate() {
            let line = line.with_context(|| {
                format!("failed to read {} line {}", path.display(), sequence + 1)
            })?;
            let event: ObjectiveEvent = serde_json::from_str(&line).with_context(|| {
                format!("failed to parse {} line {}", path.display(), sequence + 1)
            })?;
            if event.schema_version != OBJECTIVE_EVENT_SCHEMA_VERSION
                || event.objective_id != objective_id
                || event.sequence != sequence as u64
                || event.idempotency_key.trim().is_empty()
                || !idempotency_keys.insert(event.idempotency_key.clone())
                || event.previous_hash != previous_hash
                || event.event_hash != event.expected_hash()?
            {
                bail!("objective event ledger integrity check failed at sequence {sequence}");
            }
            validate_objective_event_transition(&mut active, &mut terminated, &event)?;
            previous_hash = event.event_hash.clone();
            events.push(event);
        }
        Ok(events)
    }

    pub fn acquire_objective_lease(
        &self,
        objective_id: &str,
        owner_session_id: &str,
        duration: std::time::Duration,
    ) -> Result<ObjectiveLeaseGuard> {
        if duration.is_zero() {
            bail!("objective lease duration must be greater than zero");
        }
        let duration =
            Duration::from_std(duration).context("objective lease duration is too large")?;
        let path = self.objective_lease_path(objective_id);
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
            .with_context(|| format!("failed to open objective lease {}", path.display()))?;
        if let Err(error) = file.try_lock() {
            let active = read_json_file::<ObjectiveLease>(&path).ok();
            let owner = active
                .as_ref()
                .map(|lease| lease.owner_session_id.as_str())
                .unwrap_or("unknown");
            bail!("objective {objective_id} is already leased by session {owner}: {error}");
        }
        let now = Local::now();
        let lease = ObjectiveLease {
            schema_version: 1,
            objective_id: objective_id.to_string(),
            owner_session_id: owner_session_id.to_string(),
            lease_id: format!("objective_lease_{}", id_timestamp()),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + duration).to_rfc3339(),
        };
        lease.validate(objective_id)?;
        file.set_len(0)
            .with_context(|| format!("failed to truncate {}", path.display()))?;
        file.write_all(format!("{}\n", serde_json::to_string_pretty(&lease)?).as_bytes())
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync {}", path.display()))?;
        Ok(ObjectiveLeaseGuard { lease, file, path })
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

    pub fn worker_fanout_dir(&self) -> PathBuf {
        self.root.join("worker-fanout")
    }

    pub fn worker_fanout_dir_for_session(&self, session_id: &str) -> PathBuf {
        self.worker_fanout_dir()
            .join(worker_fanout_session_path_component(session_id))
    }

    pub fn worker_fanout_counter_path_for_session(&self, session_id: &str) -> PathBuf {
        self.worker_fanout_dir_for_session(session_id)
            .join("spawn-count.json")
    }

    pub fn read_worker_fanout_counter(&self, session_id: &str) -> Result<WorkerFanoutCounter> {
        if session_id.trim().is_empty() {
            bail!("worker fan-out counter session_id cannot be empty");
        }
        let path = self.worker_fanout_counter_path_for_session(session_id);
        if !path.exists() {
            return Ok(WorkerFanoutCounter::new(session_id));
        }
        let counter: WorkerFanoutCounter = read_json_file(&path)
            .with_context(|| format!("failed to read worker fan-out counter {}", path.display()))?;
        counter.validate(session_id)?;
        Ok(counter)
    }

    pub fn write_worker_fanout_counter(&self, counter: &WorkerFanoutCounter) -> Result<PathBuf> {
        counter.validate(&counter.session_id)?;
        let path = self.worker_fanout_counter_path_for_session(&counter.session_id);
        write_json_atomic(&path, counter)?;
        Ok(path)
    }

    pub fn write_worker_fanout_denial(
        &self,
        receipt: &WorkerFanoutDenialReceipt,
    ) -> Result<PathBuf> {
        receipt.validate()?;
        let path = self
            .worker_fanout_dir_for_session(&receipt.session_id)
            .join(format!("denied-{}.json", receipt.count));
        write_json_atomic(&path, receipt)?;
        Ok(path)
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

    /// Per-session continuation state path: `.gear/continuation/{session_id}/state.json`
    pub fn continuation_state_path_for_session(&self, session_id: &str) -> PathBuf {
        self.continuation_dir().join(session_id).join("state.json")
    }

    pub fn continuation_stuck_path_for_session(&self, session_id: &str) -> PathBuf {
        self.continuation_dir()
            .join(session_id)
            .join("auto-resume.stuck")
    }

    pub fn continuation_guard_path_for_session(&self, session_id: &str) -> PathBuf {
        self.continuation_dir().join(session_id).join("guard.json")
    }

    pub fn read_continuation_guard_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<ContinuationGuardState>> {
        let path = self.continuation_guard_path_for_session(session_id);
        if !path.exists() {
            return Ok(None);
        }
        let guard: ContinuationGuardState =
            read_json_file(&path).with_context(|| format!("failed to read {}", path.display()))?;
        guard.validate()?;
        if guard.session_id != session_id {
            bail!("continuation guard session binding mismatch");
        }
        Ok(Some(guard))
    }

    pub fn write_continuation_guard(&self, guard: &ContinuationGuardState) -> Result<PathBuf> {
        let sealed = guard.clone().seal()?;
        let path = self.continuation_guard_path_for_session(&sealed.session_id);
        write_json_atomic(&path, &sealed)?;
        Ok(path)
    }

    pub fn update_continuation_guard(
        &self,
        session_id: &str,
        goal_id: &str,
        epoch_id: &str,
        update: impl FnOnce(&mut ContinuationGuardState),
    ) -> Result<ContinuationGuardState> {
        let mut guard = self
            .read_continuation_guard_for_session(session_id)?
            .unwrap_or_else(|| ContinuationGuardState::new(session_id, goal_id, epoch_id));
        if guard.goal_id != goal_id {
            guard = ContinuationGuardState::new(session_id, goal_id, epoch_id);
        }
        guard.epoch_id = epoch_id.to_string();
        update(&mut guard);
        guard.updated_at = timestamp();
        let sealed = guard.seal()?;
        self.write_continuation_guard(&sealed)?;
        Ok(sealed)
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
        let previous = self.read_continuation_state_for_session(session_id)?;
        let reset_progress = status == ContinuationStatus::Completed;
        let state = ContinuationState {
            session_id: session_id.to_string(),
            goal_id: goal_id.to_string(),
            status,
            updated_at: timestamp(),
            parent_session_id: previous
                .as_ref()
                .and_then(|state| state.parent_session_id.clone()),
            root_session_id: previous
                .as_ref()
                .and_then(|state| state.root_session_id.clone()),
            resume_count: if reset_progress {
                0
            } else {
                previous.as_ref().map_or(0, |state| state.resume_count)
            },
            last_progress_marker: if reset_progress {
                None
            } else {
                previous
                    .as_ref()
                    .and_then(|state| state.last_progress_marker.clone())
            },
            stuck_reason: if reset_progress {
                None
            } else {
                previous
                    .as_ref()
                    .and_then(|state| state.stuck_reason.clone())
            },
        };
        let path = self.continuation_state_path_for_session(session_id);
        write_json(&path, &state)?;
        if reset_progress {
            let stuck_path = self.continuation_stuck_path_for_session(session_id);
            if stuck_path.exists() {
                fs::remove_file(&stuck_path)
                    .with_context(|| format!("failed to clear {}", stuck_path.display()))?;
            }
        }
        Ok(path)
    }

    /// Return a stable marker for work progress without counting retry-only
    /// bookkeeping such as attempt numbers, timestamps, or event sequence.
    pub fn continuation_progress_marker(&self, goal_id: &str) -> Result<String> {
        let Some(ledger) = self.read_plan_node_runs(goal_id)? else {
            return Ok("missing".to_string());
        };
        let snapshot = ledger
            .nodes
            .iter()
            .map(|node| {
                serde_json::json!({
                    "task_id": node.task_id,
                    "status": node.status,
                    "green_evidence_paths": node.green_evidence_paths,
                    "review_evidence_path": node.review_evidence_path,
                    "error": node.error,
                    "criterion_evidence": node.criterion_evidence,
                })
            })
            .collect::<Vec<_>>();
        let bytes =
            serde_json::to_vec(&snapshot).context("failed to serialize continuation progress")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    /// Persist one continuation decision and enforce OMO's bounded automatic
    /// resume behavior. A retry is considered progress only when the durable
    /// PlanNodeRun projection changes; newly appended runtime events alone do
    /// not reset the counter.
    pub fn prepare_continuation_resume(
        &self,
        session_id: &str,
        goal_id: &str,
    ) -> Result<ContinuationResumeDecision> {
        let previous = self.read_continuation_state_for_session(session_id)?;
        let progress_marker = self.continuation_progress_marker(goal_id)?;
        let progress_advanced = previous.as_ref().is_none_or(|state| {
            state.goal_id != goal_id
                || state.last_progress_marker.as_deref() != Some(progress_marker.as_str())
        });
        let resume_count = if progress_advanced {
            0
        } else {
            previous
                .as_ref()
                .map_or(0, |state| state.resume_count)
                .saturating_add(1)
        };
        let stuck_reason = (resume_count > MAX_CONTINUATION_AUTO_RESUMES).then(|| {
            format!(
                "continuation stopped after {} retries without durable PlanNodeRun progress",
                MAX_CONTINUATION_AUTO_RESUMES
            )
        });
        let state = ContinuationState {
            session_id: session_id.to_string(),
            goal_id: goal_id.to_string(),
            status: if stuck_reason.is_some() {
                ContinuationStatus::Stopped
            } else {
                ContinuationStatus::Running
            },
            updated_at: timestamp(),
            parent_session_id: previous
                .as_ref()
                .and_then(|state| state.parent_session_id.clone()),
            root_session_id: previous
                .as_ref()
                .and_then(|state| state.root_session_id.clone()),
            resume_count,
            last_progress_marker: Some(progress_marker),
            stuck_reason: stuck_reason.clone(),
        };
        let path = self.continuation_state_path_for_session(session_id);
        write_json(&path, &state)?;
        if let Some(reason) = stuck_reason {
            let stuck_path = self.continuation_stuck_path_for_session(session_id);
            write_json(
                &stuck_path,
                &serde_json::json!({
                    "session_id": session_id,
                    "goal_id": goal_id,
                    "resume_count": resume_count,
                    "reason": reason,
                    "updated_at": state.updated_at,
                }),
            )?;
        }
        Ok(ContinuationResumeDecision {
            should_resume: state.status == ContinuationStatus::Running,
            state,
            progress_advanced,
        })
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

    pub fn read_session(&self, session_id: &str) -> Result<Option<Session>> {
        let path = self.sessions_dir().join(format!("{session_id}.json"));
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        Ok(Some(serde_json::from_str(&contents).with_context(
            || format!("failed to parse {}", path.display()),
        )?))
    }

    pub fn write_goal(&self, goal: &Goal) -> Result<PathBuf> {
        let path = self.goals_dir().join(format!("{}.json", goal.id));
        write_json(&path, goal)?;
        Ok(path)
    }

    pub fn read_goal(&self, goal_id: &str) -> Result<Option<Goal>> {
        let path = self.goals_dir().join(format!("{goal_id}.json"));
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        Ok(Some(serde_json::from_str(&contents).with_context(
            || format!("failed to parse {}", path.display()),
        )?))
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

    pub fn write_plan_node_runs(&self, ledger: &PlanNodeRunLedger) -> Result<PathBuf> {
        ledger.validate()?;
        let path = self.plan_node_runs_path(&ledger.goal_id);
        write_json_atomic(&path, ledger)?;
        Ok(path)
    }

    pub fn read_plan_node_runs(&self, goal_id: &str) -> Result<Option<PlanNodeRunLedger>> {
        let path = self.plan_node_runs_path(goal_id);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(read_json_file(&path)?))
    }

    pub fn write_plan_wave_run(&self, ledger: &PlanWaveRunLedger) -> Result<PathBuf> {
        ledger.validate()?;
        let path = self.plan_wave_run_path(&ledger.goal_id, &ledger.epoch_id, &ledger.wave_id);
        write_json_atomic(&path, ledger)?;
        Ok(path)
    }

    pub fn read_plan_wave_run(
        &self,
        goal_id: &str,
        epoch_id: &str,
        wave_id: &str,
    ) -> Result<Option<PlanWaveRunLedger>> {
        let path = self.plan_wave_run_path(goal_id, epoch_id, wave_id);
        if !path.exists() {
            return Ok(None);
        }
        let ledger: PlanWaveRunLedger = read_json_file(&path)?;
        ledger.validate()?;
        if ledger.goal_id != goal_id || ledger.epoch_id != epoch_id || ledger.wave_id != wave_id {
            bail!("plan wave ledger path identity does not match its contents");
        }
        Ok(Some(ledger))
    }

    pub fn write_plan_node_session_binding(
        &self,
        binding: &PlanNodeSessionBinding,
    ) -> Result<PathBuf> {
        binding.validate()?;
        let path = self.plan_node_session_binding_path(
            &binding.goal_id,
            &binding.epoch_id,
            &binding.task_id,
            binding.attempt,
        );
        write_json_atomic(&path, binding)?;
        Ok(path)
    }

    pub fn read_plan_node_session_binding(
        &self,
        goal_id: &str,
        epoch_id: &str,
        task_id: &str,
        attempt: usize,
    ) -> Result<Option<PlanNodeSessionBinding>> {
        let path = self.plan_node_session_binding_path(goal_id, epoch_id, task_id, attempt);
        if !path.exists() {
            return Ok(None);
        }
        let binding: PlanNodeSessionBinding = read_json_file(&path)?;
        binding.validate()?;
        if binding.goal_id != goal_id
            || binding.epoch_id != epoch_id
            || binding.task_id != task_id
            || binding.attempt != attempt
        {
            bail!("plan node session binding path identity does not match its contents");
        }
        Ok(Some(binding))
    }

    pub fn write_task_route_decision_receipt(
        &self,
        receipt: &TaskRouteDecisionReceipt,
    ) -> Result<PathBuf> {
        receipt.validate()?;
        let path = self.task_route_receipt_path(
            &receipt.goal_id,
            &receipt.epoch_id,
            &receipt.task_id,
            receipt.attempt,
        );
        write_json_atomic(&path, receipt)?;
        Ok(path)
    }

    pub fn read_task_route_decision_receipt(
        &self,
        goal_id: &str,
        epoch_id: &str,
        task_id: &str,
        attempt: usize,
    ) -> Result<Option<TaskRouteDecisionReceipt>> {
        let path = self.task_route_receipt_path(goal_id, epoch_id, task_id, attempt);
        if !path.is_file() {
            return Ok(None);
        }
        let receipt: TaskRouteDecisionReceipt = read_json_file(&path)?;
        receipt.validate()?;
        if receipt.goal_id != goal_id
            || receipt.epoch_id != epoch_id
            || receipt.task_id != task_id
            || receipt.attempt != attempt
        {
            bail!("task route decision receipt path identity does not match its contents");
        }
        Ok(Some(receipt))
    }

    pub fn write_repository_observation_receipt(
        &self,
        receipt: &RepositoryObservationReceipt,
    ) -> Result<PathBuf> {
        receipt.validate()?;
        if !receipt
            .role
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
        {
            bail!("repository observation role must be an ASCII identifier");
        }
        let task = repository_observation_path_component(&receipt.worker_task_id);
        let session = repository_observation_path_component(&receipt.session_id);
        let path = self.plan_review_dir(&receipt.goal_id).join(format!(
            "revision-{:03}-{}-{}-{}-repository-observation.json",
            receipt.plan_revision, receipt.role, task, session
        ));
        write_json_atomic(&path, receipt)?;
        Ok(path)
    }

    pub fn read_repository_observation_receipt_for_task(
        &self,
        goal_id: &str,
        revision: usize,
        role: &str,
        worker_task_id: &str,
        session_id: &str,
    ) -> Result<Option<RepositoryObservationReceipt>> {
        let current_path = self.plan_review_dir(goal_id).join(format!(
            "revision-{revision:03}-{role}-{}-{}-repository-observation.json",
            repository_observation_path_component(worker_task_id),
            repository_observation_path_component(session_id)
        ));
        let path = if current_path.is_file() {
            current_path
        } else {
            self.plan_review_dir(goal_id).join(format!(
                "revision-{revision:03}-{role}-{}-{}-repository-observation.json",
                legacy_repository_observation_path_component(worker_task_id),
                legacy_repository_observation_path_component(session_id)
            ))
        };
        if !path.is_file() {
            return Ok(None);
        }
        let receipt: RepositoryObservationReceipt = read_json_file(&path)?;
        receipt.validate()?;
        if receipt.goal_id != goal_id
            || receipt.plan_revision != revision
            || receipt.role != role
            || receipt.worker_task_id != worker_task_id
            || receipt.session_id != session_id
        {
            bail!("repository observation task index binding mismatch");
        }
        Ok(Some(receipt))
    }

    pub fn read_repository_observation_receipt(
        &self,
        goal_id: &str,
        revision: usize,
        role: &str,
    ) -> Result<Option<RepositoryObservationReceipt>> {
        let path = self.plan_review_dir(goal_id).join(format!(
            "revision-{revision:03}-{role}-repository-observation.json"
        ));
        let path = if path.is_file() {
            Some(path)
        } else {
            let prefix = format!("revision-{revision:03}-{role}-");
            let suffix = "-repository-observation.json";
            let mut candidates = Vec::new();
            let review_dir = self.plan_review_dir(goal_id);
            if review_dir.is_dir() {
                for entry in fs::read_dir(&review_dir)? {
                    let entry = entry?;
                    let candidate = entry.path();
                    let Some(name) = candidate.file_name().and_then(|name| name.to_str()) else {
                        continue;
                    };
                    if name.starts_with(&prefix) && name.ends_with(suffix) {
                        candidates.push(candidate);
                    }
                }
            }
            candidates.sort();
            candidates.into_iter().next()
        };
        let Some(path) = path else {
            return Ok(None);
        };
        if !path.is_file() {
            return Ok(None);
        }
        let receipt: RepositoryObservationReceipt = read_json_file(&path)?;
        receipt.validate()?;
        if receipt.goal_id != goal_id || receipt.plan_revision != revision || receipt.role != role {
            bail!("repository observation receipt path identity mismatch");
        }
        Ok(Some(receipt))
    }

    pub fn append_model_call_ledger_entry(&self, entry: &ModelCallLedgerEntry) -> Result<PathBuf> {
        entry.validate()?;
        let path = self.model_call_ledger_path(&entry.goal_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        if path.is_file() {
            for line in fs::read_to_string(&path)?
                .lines()
                .filter(|line| !line.trim().is_empty())
            {
                let existing: ModelCallLedgerEntry = serde_json::from_str(line)
                    .with_context(|| format!("failed to parse {}", path.display()))?;
                existing.validate()?;
                if existing.call_id == entry.call_id {
                    if existing == *entry {
                        return Ok(path);
                    }
                    bail!("model call ledger call id was reused with different content");
                }
            }
        }
        let line = serde_json::to_string(entry).context("failed to serialize model call entry")?;
        use std::io::Write as _;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        writeln!(file, "{line}").with_context(|| format!("failed to append {}", path.display()))?;
        Ok(path)
    }

    pub fn read_model_call_ledger(&self, goal_id: &str) -> Result<Vec<ModelCallLedgerEntry>> {
        let path = self.model_call_ledger_path(goal_id);
        if !path.is_file() {
            return Ok(Vec::new());
        }
        fs::read_to_string(&path)?
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let entry: ModelCallLedgerEntry = serde_json::from_str(line)
                    .with_context(|| format!("failed to parse {}", path.display()))?;
                entry.validate()?;
                Ok(entry)
            })
            .collect()
    }

    pub fn write_plan_graph(&self, plan_graph: &crate::plan_graph::PlanGraph) -> Result<PathBuf> {
        plan_graph
            .validate()
            .context("refusing to persist an invalid PlanGraph")?;
        let approval = self
            .read_plan_approval_state(&plan_graph.goal_id)?
            .context("approved PlanGraph is missing approval.json")?;
        self.validate_plan_approval_bundle_with_approval(plan_graph, &approval)
            .context("refusing to persist a PlanGraph without a valid approval bundle")?;
        let bundle = CanonicalPlanBundle::seal(plan_graph.clone(), approval)?;
        let bundle_path = self.canonical_plan_bundle_path(&plan_graph.goal_id);
        write_json_atomic(
            &bundle_path,
            &bundle,
        )?;
        let pointer = CanonicalPlanPointer::seal(
            &plan_graph.goal_id,
            plan_graph,
            &bundle_path,
            &bundle.binding_hash,
        )?;
        write_json_atomic(
            &self.canonical_plan_pointer_path(&plan_graph.goal_id),
            &pointer,
        )?;
        let path = self
            .plans_dir()
            .join(format!("{}.plan.json", plan_graph.goal_id));
        write_json_atomic(&path, plan_graph)?;
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
        write_json_atomic(&path, plan_graph)?;
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
        write_json_atomic(&path, plan_graph)?;
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

    pub fn review_epoch_bundle_path(&self, goal_id: &str, plan_revision: usize) -> PathBuf {
        self.plan_review_dir(goal_id)
            .join(format!("revision-{plan_revision:03}-review-epoch.json"))
    }

    pub fn write_review_epoch_bundle(&self, bundle: &ReviewEpochBundle) -> Result<PathBuf> {
        bundle.validate()?;
        let path = self.review_epoch_bundle_path(&bundle.goal_id, bundle.plan_revision);
        write_json_atomic(&path, bundle)?;
        Ok(path)
    }

    pub fn read_review_epoch_bundle(
        &self,
        goal_id: &str,
        plan_revision: usize,
    ) -> Result<Option<ReviewEpochBundle>> {
        let path = self.review_epoch_bundle_path(goal_id, plan_revision);
        if !path.is_file() {
            return Ok(None);
        }
        let bundle: ReviewEpochBundle = read_json_file(&path)?;
        bundle.validate()?;
        if bundle.goal_id != goal_id || bundle.plan_revision != plan_revision {
            bail!("review epoch bundle path binding mismatch");
        }
        Ok(Some(bundle))
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

    pub fn read_plan_critic_receipt(
        &self,
        goal_id: &str,
        plan_revision: usize,
    ) -> Result<Option<crate::plan_review::PlanCriticReceipt>> {
        let path = self
            .plan_review_dir(goal_id)
            .join(format!("revision-{plan_revision:03}-critic-receipt.json"));
        if !path.is_file() {
            return Ok(None);
        }
        let receipt: crate::plan_review::PlanCriticReceipt = read_json_file(&path)?;
        if receipt.goal_id != goal_id || receipt.plan_revision != plan_revision {
            bail!("plan critic receipt path binding mismatch");
        }
        Ok(Some(receipt))
    }

    pub fn read_plan_oracle_receipt(
        &self,
        goal_id: &str,
        plan_revision: usize,
    ) -> Result<Option<crate::plan_review::PlanCriticReceipt>> {
        let path = self
            .plan_review_dir(goal_id)
            .join(format!("revision-{plan_revision:03}-oracle-receipt.txt"));
        if !path.is_file() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let receipt: crate::plan_review::PlanCriticReceipt = serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        if receipt.goal_id != goal_id || receipt.plan_revision != plan_revision {
            bail!("plan oracle receipt path binding mismatch");
        }
        Ok(Some(receipt))
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
        let bundle_path = self.canonical_plan_bundle_path(goal_id);
        if bundle_path.exists() {
            let bundle: CanonicalPlanBundle = read_json_file(&bundle_path).with_context(|| {
                format!(
                    "failed to read canonical plan bundle {}",
                    bundle_path.display()
                )
            })?;
            bundle.validate().with_context(|| {
                format!("invalid canonical plan bundle at {}", bundle_path.display())
            })?;
            let pointer_path = self.canonical_plan_pointer_path(goal_id);
            if !pointer_path.is_file() {
                bail!(
                    "canonical plan pointer is missing for bundle {}",
                    bundle_path.display()
                );
            }
            let pointer: CanonicalPlanPointer = read_json_file(&pointer_path).with_context(|| {
                format!("failed to read canonical plan pointer {}", pointer_path.display())
            })?;
            pointer.validate(&bundle_path).with_context(|| {
                format!("invalid canonical plan pointer at {}", pointer_path.display())
            })?;
            if pointer.goal_id != goal_id
                || pointer.plan_id != bundle.plan.plan_id
                || pointer.plan_hash != bundle.plan.plan_hash
                || pointer.revision != bundle.plan.revision
                || pointer.bundle_binding_hash != bundle.binding_hash
            {
                bail!("canonical plan pointer does not match its bundle");
            }
            self.validate_plan_approval_bundle_with_approval(&bundle.plan, &bundle.approval)
                .with_context(|| {
                    format!(
                        "invalid approval receipt chain in canonical plan bundle {}",
                        bundle_path.display()
                    )
                })?;
            return Ok(Some(bundle.plan));
        }
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
        self.validate_plan_approval_bundle_with_approval(plan_graph, &approval)
    }

    fn validate_plan_approval_bundle_with_approval(
        &self,
        plan_graph: &crate::plan_graph::PlanGraph,
        approval: &crate::plan_review::PlanApprovalState,
    ) -> Result<()> {
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
        if let Some(bundle) = self.read_review_epoch_bundle(&plan_graph.goal_id, revision)? {
            if bundle.plan_id != plan_graph.plan_id
                || bundle.plan_hash != plan_graph.plan_hash
                || !bundle.complete
            {
                bail!("review epoch bundle does not bind a complete approved plan");
            }
            let oracle_hash = bundle
                .roles
                .iter()
                .find(|role| role.role == "oracle")
                .map(|role| role.receipt_hash.as_str())
                .context("review epoch bundle is missing Oracle evidence")?;
            if approval.secondary_critic_receipt_hash.as_deref() != Some(oracle_hash) {
                bail!("review epoch bundle Oracle hash disagrees with approval manifest");
            }
        } else if plan_graph.draft.tasks.len() > 1 {
            let oracle_hash = approval
                .secondary_critic_receipt_hash
                .as_deref()
                .context("multi-node PlanGraph requires an independent plan review receipt")?;
            let oracle_raw_output = fs::read_to_string(
                review_dir.join(format!("revision-{revision:03}-oracle-output.txt")),
            )
            .context("multi-node PlanGraph is missing independent reviewer output")?;
            let oracle_receipt_json = fs::read_to_string(
                review_dir.join(format!("revision-{revision:03}-oracle-receipt.txt")),
            )
            .context("multi-node PlanGraph is missing independent reviewer receipt")?;
            let oracle_receipt: crate::plan_review::PlanCriticReceipt =
                serde_json::from_str(&oracle_receipt_json)
                    .context("independent reviewer receipt is invalid JSON")?;
            oracle_receipt.validate(
                plan_graph,
                &planner_receipt,
                &planner_raw_output,
                &verifier,
                &oracle_raw_output,
            )?;
            if !oracle_receipt.approved() || oracle_hash != oracle_receipt.receipt_hash {
                bail!("independent plan review receipt does not approve this PlanGraph");
            }
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
        let path = self.goal_epoch_path(goal_id);
        let mut effective_idempotency_key = idempotency_key.to_string();
        let mut scan = scan_goal_epoch_event_ledger(&path, goal_id, idempotency_key)?;
        if let Some(recorded) = scan.duplicate.as_ref() {
            if recorded.epoch_id == epoch_id && recorded.kind == kind && recorded.payload == payload
            {
                return Ok(recorded.clone());
            }
            if matches!(
                kind,
                GoalEpochEventKind::Settled | GoalEpochEventKind::Aborted
            ) {
                bail!("goal epoch idempotency key conflicts with an existing event");
            }
            // Non-terminal lifecycle observations are replay-safe. A resumed
            // run may produce a different budget/phase receipt after a
            // model/provider retry; preserve both observations under a
            // deterministic payload-derived key instead of turning a
            // transport retry into a corrupt epoch. Terminal events remain
            // strict because they close the active epoch.
            let payload_hash = Sha256::digest(serde_json::to_vec(&(&kind, &payload))?);
            let payload_hash = format!("{payload_hash:x}");
            effective_idempotency_key = format!("{idempotency_key}.replay.{}", &payload_hash[..16]);
            scan = scan_goal_epoch_event_ledger(&path, goal_id, &effective_idempotency_key)?;
            if let Some(replayed) = scan.duplicate.as_ref() {
                if replayed.epoch_id == epoch_id
                    && replayed.kind == kind
                    && replayed.payload == payload
                {
                    return Ok(replayed.clone());
                }
                bail!("goal epoch replay key conflicts with an existing event");
            }
        }
        let event = GoalEpochEvent::seal(
            goal_id,
            epoch_id,
            scan.event_count,
            &effective_idempotency_key,
            kind,
            payload,
            scan.previous_hash,
        )?;
        let mut active_epoch = scan.active_epoch;
        validate_goal_epoch_transition(&mut active_epoch, &event)?;
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
            return GoalBudgetLedger {
                schema_version: 1,
                goal_id: goal_id.to_string(),
                reservations: Vec::new(),
                updated_at: timestamp(),
                ledger_hash: String::new(),
            }
            .seal();
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

    /// Release reservations left open by a process that stopped before the
    /// worker outcome was reduced. A continuation gets a fresh reservation
    /// keyed by the plan-node attempt, so a stale reservation must not block
    /// or collide with the resumed dispatch.
    pub fn release_reserved_budget_calls(&self, goal_id: &str) -> Result<Vec<String>> {
        let mut ledger = self.read_goal_budget_ledger(goal_id)?;
        let mut released = Vec::new();
        for reservation in &mut ledger.reservations {
            if reservation.status != BudgetReservationStatus::Reserved {
                continue;
            }
            reservation.status = BudgetReservationStatus::Released;
            reservation.settled_at = Some(timestamp());
            released.push(reservation.reservation_id.clone());
        }
        if !released.is_empty() {
            ledger.updated_at = timestamp();
            self.write_goal_budget_ledger(ledger)?;
        }
        Ok(released)
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
        let file =
            fs::File::open(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let mut events = Vec::new();
        let mut previous_hash = "0".repeat(64);
        let mut active_epoch = None;
        let mut idempotency_keys = HashSet::new();
        for (sequence, line) in BufReader::new(file).lines().enumerate() {
            let line = line.with_context(|| {
                format!("failed to read {} line {}", path.display(), sequence + 1)
            })?;
            let event: GoalEpochEvent = serde_json::from_str(&line).with_context(|| {
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
        let recovery_key = format!("recovery.{epoch_id}.aborted.{}", events.len());
        let event = self.append_goal_epoch_event(
            goal_id,
            &epoch_id,
            &recovery_key,
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

    pub fn write_artifact_json_atomic<T: Serialize>(
        &self,
        goal_id: &str,
        file_name: &str,
        value: &T,
    ) -> Result<PathBuf> {
        let dir = self.artifact_dir(goal_id);
        fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
        let path = dir.join(file_name);
        write_json_atomic(&path, value)?;
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

    pub fn write_worker_json_atomic<T>(
        &self,
        task_id: &str,
        file_name: &str,
        value: &T,
    ) -> Result<PathBuf>
    where
        T: Serialize + ?Sized,
    {
        let path = self.worker_dir(task_id).join(file_name);
        write_json_atomic(&path, value)?;
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

    pub fn prompt_dispatch_gate_path(&self, gate_id: &str) -> PathBuf {
        self.root
            .join("prompt-dispatch-gates")
            .join(format!("{gate_id}.json"))
    }

    fn active_semantic_prompt_dispatch_gate(
        &self,
        goal_id: &str,
        task_id: &str,
        session_id: &str,
        message_kind: &str,
        source: &str,
        semantic_dedupe_key: &str,
    ) -> Result<Option<PromptDispatchGate>> {
        let directory = self.root.join("prompt-dispatch-gates");
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read {}", directory.display()));
            }
        };

        for entry in entries {
            let path = entry
                .with_context(|| format!("failed to read {}", directory.display()))?
                .path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let gate: PromptDispatchGate = read_json_file(&path).with_context(|| {
                format!("failed to read prompt dispatch gate {}", path.display())
            })?;
            gate.validate()?;
            if gate.goal_id == goal_id
                && gate.task_id == task_id
                && gate.session_id == session_id
                && gate.message_kind == message_kind
                && gate.source == source
                && gate.semantic_dedupe_key.as_deref() == Some(semantic_dedupe_key)
                && gate.blocks_duplicate_dispatch()
            {
                return Ok(Some(gate));
            }
        }

        Ok(None)
    }

    pub fn reserve_prompt_dispatch(
        &self,
        goal_id: &str,
        task_id: &str,
        session_id: &str,
        run_epoch: usize,
        message_kind: &str,
        source: &str,
        prompt: &str,
    ) -> Result<PromptDispatchDecision> {
        self.reserve_prompt_dispatch_with_options(
            goal_id,
            task_id,
            session_id,
            run_epoch,
            message_kind,
            source,
            prompt,
            None,
        )
    }

    pub fn reserve_prompt_dispatch_with_options(
        &self,
        goal_id: &str,
        task_id: &str,
        session_id: &str,
        run_epoch: usize,
        message_kind: &str,
        source: &str,
        prompt: &str,
        semantic_dedupe_key: Option<&str>,
    ) -> Result<PromptDispatchDecision> {
        for (field, value) in [
            ("goal_id", goal_id),
            ("task_id", task_id),
            ("session_id", session_id),
            ("message_kind", message_kind),
            ("source", source),
        ] {
            if value.trim().is_empty() {
                bail!("prompt dispatch {field} cannot be empty");
            }
        }
        let prompt_hash = format!("{:x}", Sha256::digest(prompt.as_bytes()));
        let semantic_dedupe_key = semantic_dedupe_key
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .map(ToString::to_string);
        let dedupe_material = semantic_dedupe_key.as_deref().unwrap_or(&prompt_hash);
        let key_hash = format!(
            "{:x}",
            Sha256::digest(
                format!(
                    "{goal_id}\0{task_id}\0{session_id}\0{run_epoch}\0{message_kind}\0{source}\0{dedupe_material}"
                )
                .as_bytes(),
            )
        );
        let gate_id = format!("prompt_dispatch_{}", &key_hash[..16]);
        let path = self.prompt_dispatch_gate_path(&gate_id);
        let existing = if path.is_file() {
            let gate: PromptDispatchGate = read_json_file(&path)?;
            gate.validate()?;
            if gate.key_hash != key_hash
                || gate.goal_id != goal_id
                || gate.task_id != task_id
                || gate.session_id != session_id
                || gate.run_epoch != run_epoch
                || gate.message_kind != message_kind
                || gate.source != source
                || (gate.semantic_dedupe_key.is_none() && gate.prompt_hash != prompt_hash)
                || gate.semantic_dedupe_key != semantic_dedupe_key
            {
                bail!("prompt dispatch gate binding mismatch");
            }
            Some(gate)
        } else {
            None
        };
        if let Some(gate) = existing.as_ref()
            && gate.blocks_duplicate_dispatch()
        {
            return Ok(PromptDispatchDecision::Duplicate(gate.clone()));
        }
        if existing.is_none() {
            if let Some(semantic_dedupe_key) = semantic_dedupe_key.as_deref()
                && let Some(gate) = self.active_semantic_prompt_dispatch_gate(
                    goal_id,
                    task_id,
                    session_id,
                    message_kind,
                    source,
                    semantic_dedupe_key,
                )?
            {
                return Ok(PromptDispatchDecision::Duplicate(gate));
            }
        }
        let now = timestamp();
        let gate = PromptDispatchGate {
            schema_version: PROMPT_DISPATCH_GATE_SCHEMA_VERSION,
            gate_id,
            key_hash,
            goal_id: goal_id.to_string(),
            task_id: task_id.to_string(),
            session_id: session_id.to_string(),
            run_epoch,
            message_kind: message_kind.to_string(),
            source: source.to_string(),
            prompt_hash,
            semantic_dedupe_key,
            status: PromptDispatchGateStatus::Reserved,
            reservation_expires_at: Some(
                (Local::now() + Duration::milliseconds(PROMPT_DISPATCH_RESERVATION_TTL_MS))
                    .to_rfc3339(),
            ),
            hold_until: None,
            reason: None,
            created_at: existing
                .as_ref()
                .map(|gate| gate.created_at.clone())
                .unwrap_or_else(|| now.clone()),
            updated_at: now,
            gate_hash: String::new(),
        }
        .seal()?;
        write_json_atomic(&path, &gate)?;
        Ok(PromptDispatchDecision::Acquired(gate))
    }

    pub fn settle_prompt_dispatch_gate(
        &self,
        gate: &PromptDispatchGate,
        status: PromptDispatchGateStatus,
        hold_until: Option<String>,
        reason: Option<String>,
    ) -> Result<PromptDispatchGate> {
        let path = self.prompt_dispatch_gate_path(&gate.gate_id);
        let existing: PromptDispatchGate = read_json_file(&path)?;
        existing.validate()?;
        if existing.gate_hash != gate.gate_hash {
            bail!("prompt dispatch gate changed before settlement");
        }
        let hold_until = match status {
            PromptDispatchGateStatus::Held | PromptDispatchGateStatus::Accepted => hold_until
                .or_else(|| {
                    Some(
                        (Local::now()
                            + Duration::milliseconds(PROMPT_DISPATCH_POST_DISPATCH_HOLD_MS))
                        .to_rfc3339(),
                    )
                }),
            PromptDispatchGateStatus::PossiblyAccepted => hold_until.or_else(|| {
                Some(
                    (Local::now()
                        + Duration::milliseconds(PROMPT_DISPATCH_POSSIBLY_ACCEPTED_HOLD_MS))
                    .to_rfc3339(),
                )
            }),
            _ => None,
        };
        let reservation_expires_at =
            matches!(status, PromptDispatchGateStatus::Reserved).then(|| {
                (Local::now() + Duration::milliseconds(PROMPT_DISPATCH_RESERVATION_TTL_MS))
                    .to_rfc3339()
            });
        let updated = PromptDispatchGate {
            status,
            hold_until,
            reservation_expires_at,
            reason,
            updated_at: timestamp(),
            ..existing
        }
        .seal()?;
        write_json_atomic(&path, &updated)?;
        Ok(updated)
    }

    pub fn prompt_settle_decision_path(&self, decision_id: &str) -> PathBuf {
        self.root
            .join("prompt-settle-decisions")
            .join(format!("{decision_id}.json"))
    }

    pub fn record_prompt_settle_decision(
        &self,
        goal_id: &str,
        task_id: &str,
        session_id: &str,
        run_epoch: usize,
        source: &str,
        event: PromptSettleEvent,
    ) -> Result<PromptSettleDecisionResult> {
        for (field, value) in [
            ("goal_id", goal_id),
            ("task_id", task_id),
            ("session_id", session_id),
            ("source", source),
        ] {
            if value.trim().is_empty() {
                bail!("prompt settle {field} cannot be empty");
            }
        }
        let event_key = serde_json::to_string(&event)?;
        let key_hash = format!(
            "{:x}",
            Sha256::digest(
                format!("{goal_id}\0{task_id}\0{session_id}\0{run_epoch}\0{source}\0{event_key}")
                    .as_bytes(),
            )
        );
        let decision_id = format!("prompt_settle_{}", &key_hash[..16]);
        let path = self.prompt_settle_decision_path(&decision_id);
        if path.is_file() {
            let decision: PromptSettleDecision = read_json_file(&path)?;
            decision.validate()?;
            if decision.key_hash != key_hash {
                bail!("prompt settle decision binding mismatch");
            }
            return Ok(PromptSettleDecisionResult {
                decision,
                duplicate: true,
            });
        }
        let (action, reason) = PromptSettleDecision::action_for_event(&event);
        let decision = PromptSettleDecision {
            schema_version: PROMPT_SETTLE_DECISION_SCHEMA_VERSION,
            decision_id,
            key_hash,
            goal_id: goal_id.to_string(),
            task_id: task_id.to_string(),
            session_id: session_id.to_string(),
            run_epoch,
            source: source.to_string(),
            event,
            action,
            reason: reason.to_string(),
            created_at: timestamp(),
            decision_hash: String::new(),
        }
        .seal()?;
        write_json_atomic(&path, &decision)?;
        Ok(PromptSettleDecisionResult {
            decision,
            duplicate: false,
        })
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
mod gbx236_per_file_attribution_tests {
    use super::*;

    #[test]
    fn per_file_attribution_classifies_unchanged_baseline() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let workspace = temp_dir.path();
        let file_path = workspace.join("src/main.rs");
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        std::fs::write(&file_path, "fn main() {}").unwrap();

        let before = fingerprint_paths(workspace, &["src/main.rs".to_string()]);
        let after = fingerprint_paths(workspace, &["src/main.rs".to_string()]);
        let result = compute_per_file_attribution(&before, &after, "session-1", 1);

        assert_eq!(result.unchanged_baseline.len(), 1);
        assert!(result.added.is_empty());
        assert!(result.modified.is_empty());
        assert!(result.removed.is_empty());
        assert!(result.scope_verdict);
        assert_eq!(result.session_id, "session-1");
        assert_eq!(result.attempt, 1);
        assert_eq!(
            result.unchanged_baseline[0].classification,
            FileAttributionClass::UnchangedBaseline
        );
    }

    #[test]
    fn per_file_attribution_detects_new_additions() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let workspace = temp_dir.path();
        std::fs::create_dir_all(workspace.join("src")).unwrap();
        std::fs::write(workspace.join("src/main.rs"), "fn main() {}").unwrap();
        std::fs::write(workspace.join("src/lib.rs"), "pub fn lib() {}").unwrap();

        // Before has only main.rs; after has both
        let before = fingerprint_paths(workspace, &["src/main.rs".to_string()]);
        let after = fingerprint_paths(
            workspace,
            &["src/main.rs".to_string(), "src/lib.rs".to_string()],
        );
        let result = compute_per_file_attribution(&before, &after, "session-2", 1);

        assert_eq!(result.unchanged_baseline.len(), 1);
        assert_eq!(result.added.len(), 1);
        assert_eq!(result.added[0].fingerprint.path, "src/lib.rs");
        assert_eq!(result.added[0].classification, FileAttributionClass::Added);
        assert!(result.modified.is_empty());
        assert!(result.removed.is_empty());
        assert!(!result.scope_verdict);
    }

    #[test]
    fn per_file_attribution_detects_modified_content() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let workspace = temp_dir.path();
        std::fs::create_dir_all(workspace.join("src")).unwrap();

        // Write initial content
        std::fs::write(workspace.join("src/main.rs"), "fn main() {}\n").unwrap();

        // Capture fingerprint before modification
        let before_fp = fingerprint_file(workspace, "src/main.rs")
            .expect("before fingerprint should succeed");
        let before = fingerprint_paths(workspace, &["src/main.rs".to_string()]);

        // Modify the file
        std::fs::write(workspace.join("src/main.rs"), "fn main() { println!(\"changed\"); }\n")
            .unwrap();

        let after = fingerprint_paths(workspace, &["src/main.rs".to_string()]);
        let result = compute_per_file_attribution(&before, &after, "session-3", 1);

        assert!(result.unchanged_baseline.is_empty());
        assert_eq!(result.modified.len(), 1);
        assert_eq!(result.modified[0].fingerprint.path, "src/main.rs");
        assert_eq!(
            result.modified[0].classification,
            FileAttributionClass::Modified
        );
        assert!(result.added.is_empty());
        assert!(result.removed.is_empty());
        assert!(!result.scope_verdict);
        // Content hash must differ
        assert_ne!(
            result.modified[0].fingerprint.content_hash,
            before_fp.content_hash
        );
    }

    #[test]
    fn per_file_attribution_detects_two_session_stale_preimage() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let workspace = temp_dir.path();
        std::fs::create_dir_all(workspace.join("src")).unwrap();
        std::fs::write(workspace.join("src/race.rs"), "baseline\n").unwrap();

        let before = fingerprint_paths(workspace, &["src/race.rs".to_string()]);
        std::fs::write(workspace.join("src/race.rs"), "session-a\n").unwrap();
        let session_a_after = fingerprint_paths(workspace, &["src/race.rs".to_string()]);
        let first = compute_per_file_attribution(&before, &session_a_after, "session-a", 1);
        assert_eq!(first.modified.len(), 1);

        // Session B still holds the original preimage.  Comparing that
        // preimage with the current content must classify the write as a
        // conflict instead of allowing a stale edit to be treated as noop.
        std::fs::write(workspace.join("src/race.rs"), "session-b\n").unwrap();
        let session_b_after = fingerprint_paths(workspace, &["src/race.rs".to_string()]);
        let second = compute_per_file_attribution(&before, &session_b_after, "session-b", 1);
        assert_eq!(second.modified.len(), 1);
        assert_ne!(
            second.modified[0].fingerprint.content_hash,
            first.modified[0].fingerprint.content_hash
        );
        assert!(!second.scope_verdict);
    }

    #[test]
    fn per_file_attribution_detects_removed_paths() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let workspace = temp_dir.path();
        std::fs::create_dir_all(workspace.join("src")).unwrap();
        std::fs::write(workspace.join("src/main.rs"), "fn main() {}").unwrap();

        let before = fingerprint_paths(workspace, &["src/main.rs".to_string()]);
        // Delete the file before building after
        std::fs::remove_file(workspace.join("src/main.rs")).unwrap();
        let after = fingerprint_paths(workspace, &["src/main.rs".to_string()]);

        let result = compute_per_file_attribution(&before, &after, "session-4", 1);

        assert!(result.unchanged_baseline.is_empty());
        assert!(result.added.is_empty());
        assert!(result.modified.is_empty());
        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.removed[0].fingerprint.path, "src/main.rs");
        assert_eq!(
            result.removed[0].classification,
            FileAttributionClass::Removed
        );
        assert!(!result.scope_verdict);
    }

    #[test]
    fn destructive_command_rejects_dangerous_patterns() {
        // Hard git operations
        assert!(is_destructive_command("git checkout main").is_some());
        assert!(is_destructive_command("git checkout -- src/main.rs").is_some());
        assert!(is_destructive_command("git reset --hard HEAD").is_some());
        assert!(is_destructive_command("git restore src/main.rs").is_some());
        assert!(is_destructive_command("git clean -fd").is_some());
        assert!(is_destructive_command("git -C /tmp checkout main").is_some());
        assert!(is_destructive_command("sh -c 'git checkout main'").is_some());
        assert!(is_destructive_command("/bin/rm -f user-file").is_some());

        // Destructive rm
        assert!(is_destructive_command("rm -rf /tmp").is_some());
        assert!(is_destructive_command("rm --recursive --force .").is_some());

        // Safe commands should return None
        assert!(is_destructive_command("cargo check").is_none());
        assert!(is_destructive_command("cargo test -p gearbox_agent").is_none());
        assert!(is_destructive_command("echo hello").is_none());
        assert!(is_destructive_command("git status").is_none());
        assert!(is_destructive_command("git diff").is_none());
        assert!(is_destructive_command("git reset --soft HEAD").is_none());
        assert!(is_destructive_command("git log --oneline").is_none());
        assert!(is_destructive_command("ls -la").is_none());
    }

    #[test]
    fn fingerprint_file_returns_none_for_missing_or_directory() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let workspace = temp_dir.path();

        // Non-existent file
        assert!(fingerprint_file(workspace, "nonexistent.rs").is_none());

        // Directory
        std::fs::create_dir(workspace.join("subdir")).unwrap();
        assert!(fingerprint_file(workspace, "subdir").is_none());
    }

    #[test]
    fn fingerprint_file_returns_content_hash_and_metadata() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let workspace = temp_dir.path();
        std::fs::write(workspace.join("test.rs"), "fn main() {}\n").unwrap();

        let fp = fingerprint_file(workspace, "test.rs")
            .expect("fingerprint should succeed for existing file");
        assert_eq!(fp.path, "test.rs");
        assert_eq!(fp.size_bytes, 13);
        assert_eq!(fp.file_kind.as_deref(), Some("rust"));
        assert_eq!(fp.content_hash.len(), 64); // SHA-256 hex
    }
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
        let settled = store.append_goal_epoch_event(
            "goal-1",
            "epoch-1",
            "epoch-1.settled",
            GoalEpochEventKind::Settled,
            json!({ "outcome": "review_required" }),
        )?;
        assert!(
            store
                .append_goal_epoch_event(
                    "goal-1",
                    "epoch-1",
                    "epoch-1.settled",
                    GoalEpochEventKind::Settled,
                    json!({ "outcome": "changed" }),
                )
                .is_err()
        );

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
    fn goal_epoch_resume_and_failure_abort_keys_allow_changed_payloads() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        store.append_goal_epoch_event(
            "goal-resume",
            "epoch-resume",
            "epoch-resume.started",
            GoalEpochEventKind::Started,
            json!({ "session_id": "session-1" }),
        )?;
        store.append_goal_epoch_event(
            "goal-resume",
            "epoch-resume",
            "recovery.epoch-resume.aborted",
            GoalEpochEventKind::Aborted,
            json!({ "reason": "previous process" }),
        )?;
        store.append_goal_epoch_event(
            "goal-resume",
            "epoch-resume",
            "epoch-resume.started.resume.1",
            GoalEpochEventKind::Started,
            json!({ "session_id": "session-2" }),
        )?;
        store.append_goal_epoch_event(
            "goal-resume",
            "epoch-resume",
            "epoch-resume.review.completed",
            GoalEpochEventKind::PhaseCompleted,
            json!({ "status": "running", "review": 1 }),
        )?;
        let replayed_phase = store.append_goal_epoch_event(
            "goal-resume",
            "epoch-resume",
            "epoch-resume.review.completed",
            GoalEpochEventKind::PhaseCompleted,
            json!({ "status": "complete", "review": 2 }),
        )?;
        assert_ne!(
            replayed_phase.idempotency_key,
            "epoch-resume.review.completed"
        );
        let failure = store.append_goal_epoch_event(
            "goal-resume",
            "epoch-resume",
            "epoch-resume.aborted.failure.deadbeef",
            GoalEpochEventKind::Aborted,
            json!({ "status": "failed", "reason": "new failure" }),
        )?;
        assert_eq!(failure.kind, GoalEpochEventKind::Aborted);
        assert_eq!(store.read_goal_epoch_events("goal-resume")?.len(), 6);
        Ok(())
    }

    #[test]
    fn event_ledger_scanner_tracks_tail_and_idempotent_replay() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let objective_id = "objective-stream";
        store.append_objective_event(
            objective_id,
            "objective-stream.started",
            ObjectiveEventKind::Started,
            json!({ "session_id": "root" }),
        )?;
        for index in 0..128 {
            store.append_objective_event(
                objective_id,
                &format!("goal-attached:{index}"),
                ObjectiveEventKind::GoalAttached,
                json!({
                    "goal_id": format!("goal-{index}"),
                    "epoch_id": format!("epoch-{index}"),
                }),
            )?;
        }
        let completed = store.append_objective_event(
            objective_id,
            "objective-stream.completed",
            ObjectiveEventKind::Completed,
            json!({ "goal_id": "goal-127" }),
        )?;
        let objective_scan = scan_objective_event_ledger(
            &store.objective_events_path(objective_id),
            objective_id,
            "goal-attached:64",
        )?;
        assert_eq!(objective_scan.event_count, 130);
        assert_eq!(objective_scan.previous_hash, completed.event_hash);
        assert!(!objective_scan.active);
        assert!(objective_scan.terminated);
        assert_eq!(
            objective_scan
                .duplicate
                .as_ref()
                .map(|event| event.idempotency_key.as_str()),
            Some("goal-attached:64")
        );

        let goal_id = "goal-stream";
        for index in 0..128 {
            let epoch_id = format!("epoch-{index}");
            store.append_goal_epoch_event(
                goal_id,
                &epoch_id,
                &format!("{epoch_id}.started"),
                GoalEpochEventKind::Started,
                json!({ "plan_revision": index }),
            )?;
            store.append_goal_epoch_event(
                goal_id,
                &epoch_id,
                &format!("{epoch_id}.settled"),
                GoalEpochEventKind::Settled,
                json!({ "outcome": "complete" }),
            )?;
        }
        let goal_scan = scan_goal_epoch_event_ledger(
            &store.goal_epoch_path(goal_id),
            goal_id,
            "epoch-64.settled",
        )?;
        assert_eq!(goal_scan.event_count, 256);
        assert_eq!(goal_scan.active_epoch, None);
        assert_eq!(
            goal_scan
                .duplicate
                .as_ref()
                .map(|event| event.idempotency_key.as_str()),
            Some("epoch-64.settled")
        );
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

    fn graph_node(
        goal_id: &str,
        epoch_id: &str,
        session_id: &str,
        request: &str,
        status: GoalStatus,
        parent_goal_id: Option<String>,
        parent_epoch_id: Option<String>,
        parent_strategist_receipt_hash: Option<String>,
    ) -> GoalGraphNode {
        GoalGraphNode {
            goal_id: goal_id.to_string(),
            epoch_id: epoch_id.to_string(),
            session_id: session_id.to_string(),
            request: request.to_string(),
            acceptance_signals: vec!["observable result".to_string()],
            parent_goal_id,
            parent_epoch_id,
            parent_strategist_receipt_hash,
            request_hash: format!("{:x}", Sha256::digest(request.as_bytes())),
            objective_hash: format!("objective-{goal_id}"),
            status,
            final_wave_receipt_hash: None,
            final_report_path: None,
            strategist_receipt_hash: None,
            progress_fingerprint: format!("progress-{goal_id}"),
            terminal_reason: None,
            created_at: timestamp(),
            updated_at: timestamp(),
        }
    }

    #[test]
    fn objective_graph_enforces_one_frontier_and_parent_receipt_binding() -> Result<()> {
        let policy = ObjectivePolicy::rolling_default();
        let mut graph = ObjectiveGraph::new(
            "objective-1",
            "session-root",
            "/tmp/workspace",
            "Build a product",
            "scope-hash",
            policy,
        )?;
        graph.add_root_node(graph_node(
            "goal-1",
            "epoch-1",
            "session-root",
            "Build a product",
            GoalStatus::Planning,
            None,
            None,
            None,
        ))?;
        assert_eq!(graph.active_goal_id.as_deref(), Some("goal-1"));
        assert!(
            graph
                .attach_child(graph_node(
                    "goal-2",
                    "epoch-2",
                    "session-child",
                    "Add persistence",
                    GoalStatus::Planning,
                    Some("goal-1".to_string()),
                    Some("epoch-1".to_string()),
                    Some("strategist-1".to_string()),
                ))
                .is_err()
        );

        graph.update_active_node(
            "goal-1",
            GoalStatus::Complete,
            Some("final-wave-1".to_string()),
            Some("/tmp/final-report-1.md".to_string()),
            Some("strategist-1".to_string()),
            Some("complete".to_string()),
        )?;
        graph.attach_child(graph_node(
            "goal-2",
            "epoch-2",
            "session-child",
            "Add persistence",
            GoalStatus::Planning,
            Some("goal-1".to_string()),
            Some("epoch-1".to_string()),
            Some("strategist-1".to_string()),
        ))?;
        assert_eq!(graph.active_goal_id.as_deref(), Some("goal-2"));
        assert!(
            graph
                .attach_child(graph_node(
                    "goal-3",
                    "epoch-3",
                    "session-child-2",
                    "Add tests",
                    GoalStatus::Planning,
                    Some("goal-1".to_string()),
                    Some("epoch-1".to_string()),
                    Some("strategist-1".to_string()),
                ))
                .is_err()
        );

        let mut tampered = graph.clone();
        tampered.nodes[0].request = "rewritten request".to_string();
        assert!(tampered.validate().is_err());
        Ok(())
    }

    #[test]
    fn needs_user_frontier_reopens_for_a_new_answer_epoch() -> Result<()> {
        let mut graph = ObjectiveGraph::new(
            "objective-answer",
            "session-answer",
            "/tmp/workspace",
            "Answer a question",
            "scope-hash",
            ObjectivePolicy::rolling_default(),
        )?;
        graph.add_root_node(graph_node(
            "goal-answer",
            "epoch-1",
            "session-answer",
            "Answer a question",
            GoalStatus::Planning,
            None,
            None,
            None,
        ))?;
        graph.update_active_node(
            "goal-answer",
            GoalStatus::NeedsUser,
            Some("final-wave".to_string()),
            Some("/tmp/report.md".to_string()),
            Some("strategist".to_string()),
            Some("missing environment choice".to_string()),
        )?;
        graph.set_terminal(
            ObjectiveStatus::NeedsUser,
            "missing environment choice".to_string(),
        )?;
        graph.reopen_for_user_answer(
            "goal-answer",
            "epoch-2",
            "Answer a question\n\nUser answer: use the local backend",
        )?;
        assert_eq!(graph.status, ObjectiveStatus::Running);
        assert_eq!(graph.active_goal_id.as_deref(), Some("goal-answer"));
        assert_eq!(graph.nodes[0].epoch_id, "epoch-2");
        assert_eq!(graph.nodes[0].status, GoalStatus::Planning);
        graph.validate()?;
        Ok(())
    }

    #[test]
    fn final_review_blocker_child_promotion_is_idempotent() -> Result<()> {
        let policy = ObjectivePolicy::rolling_default();
        let mut graph = ObjectiveGraph::new(
            "objective-blocker",
            "session-root",
            "/tmp/workspace",
            "Ship the product",
            "scope-hash",
            policy,
        )?;
        graph.add_root_node(graph_node(
            "goal-1",
            "epoch-1",
            "session-root",
            "Ship the product",
            GoalStatus::Planning,
            None,
            None,
            None,
        ))?;
        graph.update_active_node(
            "goal-1",
            GoalStatus::Verifying,
            Some("final-wave-hash".to_string()),
            Some("/tmp/final-report.md".to_string()),
            None,
            Some("review blocker".to_string()),
        )?;
        let child = graph_node(
            "goal-2",
            "epoch-2",
            "session-child",
            "Resolve review blockers",
            GoalStatus::Planning,
            None,
            None,
            None,
        );
        assert!(graph.append_final_review_blocker_child(
            "goal-1",
            "epoch-1",
            "final-wave-hash",
            child,
        )?);
        assert_eq!(graph.active_goal_id.as_deref(), Some("goal-2"));
        assert_eq!(graph.nodes.len(), 2);
        let replay = graph_node(
            "goal-2",
            "epoch-2",
            "session-child",
            "Resolve review blockers",
            GoalStatus::Planning,
            Some("goal-1".to_string()),
            Some("epoch-1".to_string()),
            Some("final-wave-hash".to_string()),
        );
        assert!(!graph.append_final_review_blocker_child(
            "goal-1",
            "epoch-1",
            "final-wave-hash",
            replay,
        )?);
        assert_eq!(graph.nodes.len(), 2);
        graph.validate()?;
        Ok(())
    }

    #[test]
    fn objective_event_ledger_is_idempotent_hash_chained_and_terminal() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let started = store.append_objective_event(
            "objective-1",
            "objective-1.started",
            ObjectiveEventKind::Started,
            json!({ "session_id": "root" }),
        )?;
        let attached = store.append_objective_event(
            "objective-1",
            "goal-attached:goal-1",
            ObjectiveEventKind::GoalAttached,
            json!({ "goal_id": "goal-1", "epoch_id": "epoch-1" }),
        )?;
        let replay = store.append_objective_event(
            "objective-1",
            "goal-attached:goal-1",
            ObjectiveEventKind::GoalAttached,
            json!({ "goal_id": "goal-1", "epoch_id": "epoch-1" }),
        )?;
        assert_eq!(attached.event_hash, replay.event_hash);
        let completed = store.append_objective_event(
            "objective-1",
            "terminal:goal-1:complete",
            ObjectiveEventKind::Completed,
            json!({ "goal_id": "goal-1" }),
        )?;
        assert_eq!(started.sequence, 0);
        assert_eq!(completed.previous_hash, attached.event_hash);
        let reopened = store.append_objective_event(
            "objective-1",
            "user-answer:epoch-2",
            ObjectiveEventKind::UserAnswerAccepted,
            json!({
                "goal_id": "goal-1",
                "epoch_id": "epoch-2",
                "answer": "use the local backend"
            }),
        )?;
        assert_eq!(reopened.previous_hash, completed.event_hash);
        let resumed_goal = store.append_objective_event(
            "objective-1",
            "goal-outcome:goal-1:epoch-2",
            ObjectiveEventKind::GoalOutcomeRecorded,
            json!({
                "goal_id": "goal-1",
                "epoch_id": "epoch-2",
                "receipt_hash": "receipt-2"
            }),
        )?;
        assert_eq!(resumed_goal.previous_hash, reopened.event_hash);
        assert!(
            store
                .append_objective_event(
                    "objective-1",
                    "objective-1.restart",
                    ObjectiveEventKind::Started,
                    json!({}),
                )
                .is_err()
        );

        let path = store.objective_events_path("objective-1");
        let contents = fs::read_to_string(&path)?;
        fs::write(&path, contents.replace("goal-1", "goal-rewritten"))?;
        assert!(store.read_objective_events("objective-1").is_err());
        Ok(())
    }

    #[test]
    fn objective_epoch_outcome_receipt_is_hash_bound_and_recoverable() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let receipt = ObjectiveEpochOutcomeReceipt::seal(
            "objective-1",
            "goal-1",
            "epoch-1",
            "session-1",
            "request-hash".to_string(),
            "scope-hash".to_string(),
            "policy-hash".to_string(),
            GoalStatus::Complete,
            "/tmp/final-wave.json".to_string(),
            "wave-hash".to_string(),
            "/tmp/final-report.md".to_string(),
            "report-hash".to_string(),
            "budget-hash".to_string(),
            Some("/tmp/strategist.json".to_string()),
            Some("strategist-hash".to_string()),
            Some("Continue".to_string()),
        )?;
        let path = store.write_objective_epoch_outcome(&receipt)?;
        assert!(path.is_file());
        let recovered = store
            .read_objective_epoch_outcome("objective-1", "goal-1", "epoch-1")?
            .context("outcome receipt should be recoverable")?;
        assert_eq!(recovered.receipt_hash, receipt.receipt_hash);
        let mut tampered = serde_json::to_value(&receipt)?;
        tampered["final_report_hash"] = json!("rewritten");
        fs::write(&path, serde_json::to_vec_pretty(&tampered)?)?;
        assert!(
            store
                .read_objective_epoch_outcome("objective-1", "goal-1", "epoch-1")
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn objective_budget_ledger_reservation_is_idempotent_and_settled() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let policy = ObjectivePolicy::default();
        let lease = store.acquire_objective_lease(
            "objective-1",
            "session-1",
            std::time::Duration::from_secs(60),
        )?;
        let first = store.reserve_objective_epoch(
            &lease,
            "epoch:epoch-1",
            "goal-1",
            "epoch-1",
            &policy,
            4,
            100,
            100,
            1,
            1,
        )?;
        let replay = store.reserve_objective_epoch(
            &lease,
            "epoch:epoch-1",
            "goal-1",
            "epoch-1",
            &policy,
            4,
            100,
            100,
            1,
            1,
        )?;
        assert_eq!(first.reservation_id, replay.reservation_id);
        let settled = store.settle_objective_epoch(
            &lease,
            "epoch:epoch-1",
            2,
            Some(40),
            Some(20),
            0,
            0,
            1,
            Some(25),
            vec!["model fallback unavailable".to_string()],
        )?;
        assert_eq!(settled.status, ObjectiveBudgetReservationStatus::Settled);
        let ledger = store.read_objective_budget_ledger("objective-1", &policy.hash()?)?;
        assert_eq!(ledger.reservations.len(), 1);
        assert_eq!(ledger.reservations[0].actual_cost_micros, Some(20));
        lease.release()?;
        Ok(())
    }

    #[test]
    fn objective_lease_excludes_competing_controllers() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first = store.acquire_objective_lease(
            "objective-1",
            "session-1",
            std::time::Duration::from_secs(60),
        )?;
        assert_eq!(first.lease().owner_session_id, "session-1");
        assert!(
            store
                .acquire_objective_lease(
                    "objective-1",
                    "session-2",
                    std::time::Duration::from_secs(60),
                )
                .is_err()
        );
        first.release()?;
        let second = store.acquire_objective_lease(
            "objective-1",
            "session-2",
            std::time::Duration::from_secs(60),
        )?;
        assert_eq!(second.lease().owner_session_id, "session-2");
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
    fn continuation_releases_open_worker_reservations() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let budget = Budget {
            max_worker_calls: 4,
            max_tokens_per_call: 60,
            max_tokens_per_epoch: 240,
            max_usage_unknown_calls: 4,
            ..Budget::default()
        };
        let lease = store.acquire_goal_run_lease(
            "goal-recovery",
            "epoch-recovery",
            "session-1",
            std::time::Duration::from_secs(60),
        )?;
        store.reserve_budget_call(
            &lease,
            "epoch-recovery.worker.3",
            "worker",
            true,
            false,
            &budget,
        )?;
        lease.release()?;

        let released = store.release_reserved_budget_calls("goal-recovery")?;
        assert_eq!(released, vec!["epoch-recovery.worker.3"]);
        let ledger = store.read_goal_budget_ledger("goal-recovery")?;
        assert_eq!(
            ledger.reservations[0].status,
            BudgetReservationStatus::Released
        );
        assert!(
            store
                .release_reserved_budget_calls("goal-recovery")?
                .is_empty()
        );
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

#[cfg(test)]
mod plan_wave_tests {
    use super::*;
    use crate::plan_graph::{PlanGraph, PlanSource};
    use crate::state::Scope;
    use crate::tools::DiffSnapshot;
    use std::collections::HashMap;

    fn two_task_plan() -> Result<PlanGraph> {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let mut draft = crate::plan_graph::deterministic_fallback_draft(
            "Execute a two node wave",
            &scope,
            &["cargo test".to_string()],
        );
        let mut second = draft.tasks[0].clone();
        second.task_id = "task_004".to_string();
        second.logical_task_id = Some("task_004".to_string());
        second.title = "Execute the second independent node".to_string();
        second.parallel_wave = 0;
        second.scope.allowed_files = vec!["tests".to_string()];
        second.scope.write_scope = vec!["tests".to_string()];
        draft.tasks[0].task_id = "task_003".to_string();
        draft.tasks[0].logical_task_id = Some("task_003".to_string());
        draft.tasks.push(second);
        PlanGraph::seal(
            "goal-wave",
            1,
            PlanSource::DeterministicFallback,
            None,
            draft,
        )
    }

    #[test]
    fn worker_step_evidence_rejects_out_of_order_completion() -> Result<()> {
        let plan = two_task_plan()?;
        let mut ledger = PlanNodeRunLedger::from_plan("goal-wave", "epoch-1", &plan)?;
        assert_eq!(
            ledger
                .nodes
                .iter()
                .map(|node| node.logical_task_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("task_003"), Some("task_004")]
        );
        let node = ledger.node_mut("task_003")?;
        node.execution_steps = vec![
            PlanStepRun {
                step_id: "step-1".to_string(),
                action: "inspect".to_string(),
                expected_observation: "source is understood".to_string(),
                evidence_path: None,
                status: PlanStepRunStatus::Pending,
                error: None,
                updated_at: timestamp(),
            },
            PlanStepRun {
                step_id: "step-2".to_string(),
                action: "edit".to_string(),
                expected_observation: "bounded change is applied".to_string(),
                evidence_path: None,
                status: PlanStepRunStatus::Pending,
                error: None,
                updated_at: timestamp(),
            },
        ];

        let error = node
            .apply_worker_step_evidence(&["step-2".to_string()], &HashMap::new())
            .expect_err("a worker must not skip the first ordered step");
        assert!(error.to_string().contains("skipped ordered execution step"));
        assert!(
            node.execution_steps
                .iter()
                .all(|step| step.status == PlanStepRunStatus::Pending)
        );

        assert!(
            node.apply_worker_step_evidence(&["step-1".to_string()], &HashMap::new())?
                .contains(&"step-2".to_string())
        );
        assert!(
            node.apply_worker_step_evidence(&["step-2".to_string()], &HashMap::new())?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn plan_wave_requires_all_nodes_before_barrier_closes() -> Result<()> {
        let plan = two_task_plan()?;
        let mut ledger = PlanWaveRunLedger::new(
            "goal-wave",
            "epoch-1",
            &plan,
            "wave-0",
            ["task_004".to_string(), "task_003".to_string()],
        )?;
        ledger.open_barrier()?;
        ledger.mark_dispatched("task_003", 1, "worker-003".to_string())?;
        ledger.mark_started("task_003")?;
        ledger.mark_dispatched("task_004", 1, "worker-004".to_string())?;
        ledger.mark_started("task_004")?;
        ledger.mark_terminal("task_003", PlanWaveNodeStatus::Completed, None)?;
        assert!(!ledger.barrier_ready());
        assert!(!ledger.status.is_terminal());
        ledger.mark_terminal("task_004", PlanWaveNodeStatus::Completed, None)?;
        assert!(ledger.barrier_ready());
        assert_eq!(ledger.status, PlanWaveStatus::Completed);
        ledger.validate()?;
        Ok(())
    }

    #[test]
    fn plan_wave_ledger_round_trips_with_path_identity() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let plan = two_task_plan()?;
        let mut ledger = PlanWaveRunLedger::new(
            "goal-wave",
            "epoch-1",
            &plan,
            "wave-0",
            ["task_003".to_string(), "task_004".to_string()],
        )?;
        ledger.open_barrier()?;
        store.write_plan_wave_run(&ledger)?;
        let replay = store
            .read_plan_wave_run("goal-wave", "epoch-1", "wave-0")?
            .context("missing persisted wave ledger")?;
        assert_eq!(replay, ledger);
        assert!(
            store
                .read_plan_wave_run("goal-wave", "epoch-1", "wave-other")?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn criterion_evidence_requires_every_plan_predicate_on_current_attempt() -> Result<()> {
        let plan = two_task_plan()?;
        let mut ledger = PlanNodeRunLedger::from_plan("goal-wave", "epoch-1", &plan)?;
        for node in &mut ledger.nodes {
            node.attempt = 1;
            node.status = PlanNodeRunStatus::Implemented;
        }

        let first_task = &plan.draft.tasks[0];
        let first_criterion = first_task
            .completion_predicates
            .first()
            .context("fallback plan must expose a completion predicate")?
            .clone();
        ledger
            .node_mut(&first_task.task_id)?
            .record_criterion_evidence(
                &first_criterion,
                CriterionEvidenceStatus::Pass,
                1,
                "artifacts/task-003.md",
                "sha-task-003",
            )?;
        assert!(!ledger.all_criteria_passed(&plan));

        for task in &plan.draft.tasks {
            let evidence_path = format!("artifacts/{}.md", task.task_id);
            for criterion in &task.completion_predicates {
                if task.task_id == first_task.task_id && criterion == &first_criterion {
                    continue;
                }
                ledger.node_mut(&task.task_id)?.record_criterion_evidence(
                    criterion,
                    CriterionEvidenceStatus::Pass,
                    1,
                    &evidence_path,
                    &format!("sha-{}", task.task_id),
                )?;
            }
        }
        assert!(ledger.all_criteria_passed(&plan));
        ledger.validate()?;
        Ok(())
    }

    #[test]
    fn typed_evidence_receipt_is_bound_to_obligation_and_attempt() -> Result<()> {
        let plan = two_task_plan()?;
        let mut ledger = PlanNodeRunLedger::from_plan("goal-wave", "epoch-1", &plan)?;
        let obligation = crate::plan_graph::PlanEvidenceObligation {
            obligation_id: "green_001".to_string(),
            kind: "artifact".to_string(),
            producer: "executor".to_string(),
            consumer: "completion_gate".to_string(),
            freshness: "attempt".to_string(),
            required_for: vec!["completion".to_string()],
            evidence_path: Some("artifacts/green.md".to_string()),
            unavailable_reason: None,
        };
        let node = ledger.node_mut("task_003")?;
        node.attempt = 1;
        node.record_obligation_evidence(
            &obligation,
            CriterionEvidenceStatus::Pass,
            1,
            "artifacts/green.md",
            "sha-green",
        )?;
        assert!(node.all_evidence_obligations_passed(&[obligation.clone()]));
        let mut wrong_producer = obligation.clone();
        wrong_producer.producer = "reviewer".to_string();
        assert!(!node.all_evidence_obligations_passed(&[wrong_producer]));
        let mut wrong_id = obligation;
        wrong_id.obligation_id = "other".to_string();
        assert!(!node.all_evidence_obligations_passed(&[wrong_id]));
        Ok(())
    }

    #[test]
    fn failed_plan_nodes_requeue_for_continuation_without_losing_evidence() -> Result<()> {
        let plan = two_task_plan()?;
        let mut ledger = PlanNodeRunLedger::from_plan("goal-wave", "epoch-1", &plan)?;
        let node = ledger.node_mut("task_003")?;
        node.attempt = 2;
        node.status = PlanNodeRunStatus::Failed;
        node.worker_task_id = Some("worker-failed".to_string());
        node.error = Some("worker exited non-zero".to_string());
        node.preflight_path = Some("artifacts/old-preflight.md".to_string());
        node.preflight_satisfied = true;
        node.preflight_checks = vec![PlanPreflightCheck {
            check_id: "scope_check".to_string(),
            description: "old attempt".to_string(),
            passed: true,
            failure: None,
        }];

        let requeued = ledger.requeue_failed_for_resume();
        assert_eq!(requeued, vec!["task_003".to_string()]);
        let node = ledger.node_mut("task_003")?;
        assert_eq!(node.status, PlanNodeRunStatus::Pending);
        assert_eq!(node.attempt, 2);
        assert_eq!(node.worker_task_id.as_deref(), Some("worker-failed"));
        assert_eq!(node.error.as_deref(), Some("worker exited non-zero"));
        assert!(!node.preflight_satisfied);
        assert!(node.preflight_path.is_none());
        assert!(node.preflight_checks.is_empty());
        ledger.validate()?;
        Ok(())
    }

    #[test]
    fn failed_plan_nodes_rebind_to_new_resume_epoch_without_losing_attempts() -> Result<()> {
        let plan = two_task_plan()?;
        let mut ledger = PlanNodeRunLedger::from_plan("goal-wave", "epoch-1", &plan)?;
        let node = ledger.node_mut("task_003")?;
        node.attempt = 1;
        node.status = PlanNodeRunStatus::Failed;
        node.error = Some("first epoch failed".to_string());

        ledger.rebind_epoch_for_resume("epoch-2");
        let requeued = ledger.requeue_failed_for_resume();
        assert_eq!(requeued, vec!["task_003".to_string()]);
        assert_eq!(ledger.epoch_id, "epoch-2");
        assert!(ledger.nodes.iter().all(|node| node.epoch_id == "epoch-2"));
        assert_eq!(ledger.node_mut("task_003")?.attempt, 1);
        ledger.validate()?;
        Ok(())
    }

    #[test]
    fn incomplete_plan_nodes_requeue_after_restart_without_losing_worker_evidence() -> Result<()> {
        let plan = two_task_plan()?;
        let mut ledger = PlanNodeRunLedger::from_plan("goal-wave", "epoch-1", &plan)?;
        let node = ledger.node_mut("task_003")?;
        node.attempt = 3;
        node.status = PlanNodeRunStatus::Running;
        node.worker_task_id = Some("worker-in-flight".to_string());
        node.worker_result_path = Some(".gear/workers/worker-in-flight/result.json".to_string());
        node.worker_outcome_path = Some(".gear/workers/worker-in-flight/outcome.json".to_string());
        node.preflight_path = Some("artifacts/old-preflight.md".to_string());
        node.preflight_satisfied = true;
        node.execution_steps[0].status = PlanStepRunStatus::Completed;
        node.execution_steps[1].status = PlanStepRunStatus::Running;

        let reviewed = ledger.node_mut("task_004")?;
        reviewed.status = PlanNodeRunStatus::Reviewed;
        reviewed.worker_result_path = Some("artifacts/reviewed-result.json".to_string());

        let requeued = ledger.requeue_incomplete_for_resume();
        assert_eq!(
            requeued,
            vec!["task_003".to_string(), "task_004".to_string()]
        );
        let node = ledger.node_mut("task_003")?;
        assert_eq!(node.status, PlanNodeRunStatus::Pending);
        assert_eq!(node.attempt, 3);
        assert_eq!(
            node.worker_result_path.as_deref(),
            Some(".gear/workers/worker-in-flight/result.json")
        );
        assert_eq!(node.worker_task_id.as_deref(), Some("worker-in-flight"));
        assert!(!node.preflight_satisfied);
        assert!(node.preflight_path.is_none());
        assert!(
            node.execution_steps
                .iter()
                .all(|step| step.status == PlanStepRunStatus::Pending)
        );
        assert!(ledger.active_task_ids().is_empty());
        ledger.validate()?;
        Ok(())
    }

    #[test]
    fn qa_evidence_requires_every_declared_scenario_on_current_attempt() -> Result<()> {
        let plan = two_task_plan()?;
        let mut ledger = PlanNodeRunLedger::from_plan("goal-wave", "epoch-1", &plan)?;
        let task = &plan.draft.tasks[0];
        let node = ledger.node_mut(&task.task_id)?;
        node.attempt = 1;
        node.status = PlanNodeRunStatus::Reviewed;
        assert!(!node.all_qa_passed(task));
        for (kind, scenario) in task
            .qa
            .happy_path
            .iter()
            .map(|scenario| ("happy", scenario))
            .chain(
                task.qa
                    .failure_path
                    .iter()
                    .map(|scenario| ("failure", scenario)),
            )
            .chain(
                task.qa
                    .adversarial_path
                    .iter()
                    .map(|scenario| ("adversarial", scenario)),
            )
        {
            let criterion = format!("qa:{kind}:{}", scenario.name);
            node.record_criterion_evidence(
                &criterion,
                CriterionEvidenceStatus::Pass,
                1,
                "artifacts/qa.md",
                "sha-qa",
            )?;
        }
        assert!(node.all_qa_passed(task));
        Ok(())
    }

    #[test]
    fn criterion_evidence_rejects_rewrites_and_workspace_escape() -> Result<()> {
        let mut evidence = PlanCriterionEvidence::seal(
            "compile",
            CriterionEvidenceStatus::Pass,
            1,
            "artifacts/compile.txt",
            "sha-1",
        )?;
        assert!(
            PlanCriterionEvidence::seal(
                "compile",
                CriterionEvidenceStatus::Pass,
                1,
                "../outside.txt",
                "sha-1",
            )
            .is_err()
        );

        evidence.evidence_sha256 = "sha-2".to_string();
        assert!(evidence.validate().is_err());

        let mut node = PlanNodeRun {
            goal_id: "goal-wave".to_string(),
            epoch_id: "epoch-1".to_string(),
            plan_id: "plan-1".to_string(),
            plan_revision: 1,
            plan_hash: "hash-1".to_string(),
            task_id: "task-003".to_string(),
            logical_task_id: None,
            attempt: 1,
            dependencies: Vec::new(),
            status: PlanNodeRunStatus::Implemented,
            preflight_path: None,
            preflight_satisfied: false,
            preflight_checks: Vec::new(),
            execution_steps: Vec::new(),
            worker_result_path: None,
            worker_outcome_path: None,
            worker_last_message_path: None,
            worker_changed_files: Vec::new(),
            worker_commands_run: Vec::new(),
            worker_known_failures: Vec::new(),
            worker_next_steps: Vec::new(),
            worker_diagnostics: Vec::new(),
            worker_diagnostic_receipt_path: None,
            worker_diagnostic_status: None,
            worker_plan_gap: None,
            worker_decision: PlanWorkOrderDecision::NotRecorded,
            worker_decision_reason: None,
            worker_evidence_quality: WorkerEvidenceQuality::Unclassified,
            worker_task_id: None,
            implementation_task_id: None,
            review_task_id: None,
            red_evidence_path: None,
            green_evidence_paths: Vec::new(),
            review_evidence_path: None,
            commit_boundary_evidence_path: None,
            commit_boundary_satisfied: None,
            error: None,
            criterion_evidence: Vec::new(),
            updated_at: timestamp(),
        };
        node.record_criterion_evidence(
            "compile",
            CriterionEvidenceStatus::Pass,
            1,
            "artifacts/compile.txt",
            "sha-1",
        )?;
        assert!(
            node.record_criterion_evidence(
                "compile",
                CriterionEvidenceStatus::Fail,
                1,
                "artifacts/compile.txt",
                "sha-2",
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn continuation_resume_budget_tracks_durable_progress_and_writes_stuck_marker() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let plan = two_task_plan()?;
        let ledger = PlanNodeRunLedger::from_plan("goal-wave", "epoch-1", &plan)?;
        store.write_plan_node_runs(&ledger)?;

        let first = store.prepare_continuation_resume("session-1", "goal-wave")?;
        assert!(first.should_resume);
        assert!(first.progress_advanced);
        assert_eq!(first.state.resume_count, 0);

        let second = store.prepare_continuation_resume("session-1", "goal-wave")?;
        let third = store.prepare_continuation_resume("session-1", "goal-wave")?;
        assert!(second.should_resume);
        assert!(third.should_resume);
        assert_eq!(third.state.resume_count, MAX_CONTINUATION_AUTO_RESUMES);

        let stuck = store.prepare_continuation_resume("session-1", "goal-wave")?;
        assert!(!stuck.should_resume);
        assert_eq!(stuck.state.status, ContinuationStatus::Stopped);
        assert!(stuck.state.stuck_reason.is_some());
        let stuck_path = store.continuation_stuck_path_for_session("session-1");
        assert!(stuck_path.is_file());

        let mut progressed = ledger;
        progressed.node_mut("task_003")?.status = PlanNodeRunStatus::Runnable;
        store.write_plan_node_runs(&progressed)?;
        let resumed = store.prepare_continuation_resume("session-1", "goal-wave")?;
        assert!(resumed.should_resume);
        assert!(resumed.progress_advanced);
        assert_eq!(resumed.state.resume_count, 0);
        Ok(())
    }

    #[test]
    fn review_epoch_bundle_round_trips_required_roles_and_unknown_usage() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let plan = two_task_plan()?;
        let roles = ["planner", "momus", "oracle"]
            .into_iter()
            .map(|role| ReviewEpochRoleEvidence {
                role: role.to_string(),
                execution_id: format!("{role}-execution"),
                phase_session_id: format!("{role}-session"),
                actual_session_id: Some(format!("{role}-actual")),
                receipt_hash: format!("{role}-receipt"),
                receipt_path: format!("artifacts/{role}-receipt.json"),
                observation_path: Some(format!("artifacts/{role}-observation.json")),
                requested_tokens: None,
                actual_tokens: None,
                cost_micros: None,
                duration_ms: None,
                cache_hit: None,
                unknown_reason: Some("provider did not report usage".to_string()),
            })
            .collect();
        let bundle = ReviewEpochBundle::seal("goal-wave", "epoch-1", &plan, roles, false)?;
        assert!(bundle.complete);
        let path = store.write_review_epoch_bundle(&bundle)?;
        assert!(path.is_file());
        assert_eq!(
            store.read_review_epoch_bundle("goal-wave", 1)?,
            Some(bundle)
        );
        Ok(())
    }

    #[test]
    fn plan_node_session_binding_round_trips_without_secrets() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let binding = PlanNodeSessionBinding {
            schema_version: PLAN_NODE_SESSION_BINDING_SCHEMA_VERSION,
            binding_id: "binding-1".to_string(),
            goal_id: "goal-wave".to_string(),
            epoch_id: "epoch-1".to_string(),
            plan_id: "plan-1".to_string(),
            plan_revision: 1,
            plan_hash: "hash-1".to_string(),
            task_id: "task_003".to_string(),
            attempt: 1,
            worker_task_id: "worker-003".to_string(),
            worker_kind: "opencode".to_string(),
            provider_id: Some("opencode".to_string()),
            model_id: Some("free-model".to_string()),
            session_id: "session-1".to_string(),
            capability_fingerprint: "cap-1".to_string(),
            route_receipt_hash: Some("receipt-1".to_string()),
            status: PlanNodeSessionBindingStatus::Active,
            supersedes_binding_id: None,
            created_at: timestamp(),
            updated_at: timestamp(),
        };
        store.write_plan_node_session_binding(&binding)?;
        let replay = store
            .read_plan_node_session_binding("goal-wave", "epoch-1", "task_003", 1)?
            .context("missing persisted session binding")?;
        assert_eq!(replay, binding);
        let serialized = serde_json::to_string(&replay)?;
        assert!(!serialized.contains("secret"));
        Ok(())
    }

    #[test]
    fn task_route_decision_receipt_round_trips_tier_and_budget_binding() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let plan = two_task_plan()?;
        let task = plan.task("task_003").context("missing route test task")?;
        let receipt = TaskRouteDecisionReceipt::seal(
            "goal-wave",
            "epoch-1",
            &plan,
            task,
            1,
            crate::plan_graph::PhaseProfile::ExecutorQuick,
            Some("quick".to_string()),
            "opencode_session".to_string(),
            Some("deepseek-v4-flash-free".to_string()),
            "quick".to_string(),
            "selected configured quick worker".to_string(),
            0,
            1,
            "/tmp/phase-route-decision.json".to_string(),
            Some("epoch-1.worker.1".to_string()),
        )?;
        let path = store.write_task_route_decision_receipt(&receipt)?;
        assert!(path.is_file());
        let replay = store
            .read_task_route_decision_receipt("goal-wave", "epoch-1", "task_003", 1)?
            .context("missing persisted route decision receipt")?;
        assert_eq!(replay, receipt);
        assert_eq!(replay.size_tier, task.size_tier());
        assert_eq!(replay.risk_tier, task.risk_tier());
        assert_eq!(
            replay.budget_reservation_id.as_deref(),
            Some("epoch-1.worker.1")
        );
        Ok(())
    }

    #[test]
    fn repository_observation_receipt_requires_structured_tool_events() -> Result<()> {
        let unverified = RepositoryObservationReceipt::seal(
            "planner",
            "goal-wave",
            "plan-1",
            1,
            "plan-hash",
            "worker-1",
            "session-1",
            Some("transcript-hash".to_string()),
            1,
            vec!["crates/gearbox_agent/src/state.rs".to_string()],
            Vec::new(),
        )?;
        assert_eq!(unverified.status, RepositoryObservationStatus::Unverified);

        let verified = RepositoryObservationReceipt::seal(
            "planner",
            "goal-wave",
            "plan-1",
            1,
            "plan-hash",
            "worker-1",
            "session-1",
            Some("transcript-hash".to_string()),
            1,
            vec!["crates/gearbox_agent/src/state.rs".to_string()],
            vec![RepositoryObservationEvent {
                operation: "read".to_string(),
                path: "crates/gearbox_agent/src/state.rs".to_string(),
                event_id: "tool_1".to_string(),
                event_hash: "event-hash".to_string(),
                observed_at: timestamp(),
            }],
        )?;
        assert_eq!(verified.status, RepositoryObservationStatus::Verified);
        verified.validate()?;
        Ok(())
    }

    #[test]
    fn repository_observation_capture_commit_is_hashed_and_validated() -> Result<()> {
        let capture_commit = "a".repeat(40);
        let receipt = RepositoryObservationReceipt::seal_with_capture_commit(
            "planner",
            "goal-capture-commit",
            "plan-capture-commit",
            1,
            "plan-hash",
            "worker-capture-commit",
            "session-capture-commit",
            Some("transcript-hash".to_string()),
            1,
            vec!["src/lib.rs".to_string()],
            vec![RepositoryObservationEvent {
                operation: "read".to_string(),
                path: "src/lib.rs".to_string(),
                event_id: "tool-capture-commit".to_string(),
                event_hash: "event-capture-commit".to_string(),
                observed_at: timestamp(),
            }],
            Some(capture_commit.clone()),
        )?;
        assert_eq!(
            receipt.capture_commit.as_deref(),
            Some(capture_commit.as_str())
        );
        receipt.validate()?;

        let mut tampered = serde_json::to_value(&receipt)?;
        tampered["capture_commit"] = serde_json::json!("b".repeat(40));
        let tampered: RepositoryObservationReceipt = serde_json::from_value(tampered)?;
        assert!(tampered.validate().is_err());
        Ok(())
    }

    #[test]
    fn repository_observation_index_keeps_same_role_calls_separate() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let event = RepositoryObservationEvent {
            operation: "read".to_string(),
            path: "src/lib.rs".to_string(),
            event_id: "tool-1".to_string(),
            event_hash: "event-1".to_string(),
            observed_at: timestamp(),
        };
        let first = RepositoryObservationReceipt::seal(
            "planner",
            "goal-wave",
            "pending_goal-wave",
            0,
            "pending",
            "planner_goal-wave",
            "session-planner",
            Some("transcript-1".to_string()),
            1,
            vec!["src/lib.rs".to_string()],
            vec![event.clone()],
        )?;
        let second = RepositoryObservationReceipt::seal(
            "planner",
            "goal-wave",
            "pending_goal-wave",
            0,
            "pending",
            "planner_goal-wave_repair",
            "session-planner-repair",
            Some("transcript-2".to_string()),
            1,
            vec!["src/lib.rs".to_string()],
            vec![event],
        )?;
        let first_path = store.write_repository_observation_receipt(&first)?;
        let second_path = store.write_repository_observation_receipt(&second)?;
        assert_ne!(first_path, second_path);
        assert!(
            store
                .read_repository_observation_receipt_for_task(
                    "goal-wave",
                    0,
                    "planner",
                    "planner_goal-wave",
                    "session-planner",
                )?
                .is_some()
        );
        assert!(
            store
                .read_repository_observation_receipt_for_task(
                    "goal-wave",
                    0,
                    "planner",
                    "planner_goal-wave_repair",
                    "session-planner-repair",
                )?
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn repository_observation_paths_bound_long_task_and_session_ids() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = format!("planner_{}", "goal-wave-".repeat(40));
        let session_id = format!("session_{}", "worker-wave-".repeat(40));
        let receipt = RepositoryObservationReceipt::seal(
            "planner",
            "goal-long-identities",
            "pending_goal-long-identities",
            0,
            "pending",
            &task_id,
            &session_id,
            Some("transcript-long-identities".to_string()),
            1,
            vec!["src/lib.rs".to_string()],
            vec![RepositoryObservationEvent {
                operation: "read".to_string(),
                path: "src/lib.rs".to_string(),
                event_id: "tool-long-identities".to_string(),
                event_hash: "event-long-identities".to_string(),
                observed_at: timestamp(),
            }],
        )?;
        let path = store.write_repository_observation_receipt(&receipt)?;
        let filename_length = path
            .file_name()
            .context("repository observation path has no filename")?
            .len();
        // Atomic writes replace `.json` with a timestamped `.tmp-*` suffix;
        // leave enough room for that suffix on filesystems with NAME_MAX=255.
        assert!(filename_length <= 220);
        assert!(
            store
                .read_repository_observation_receipt_for_task(
                    "goal-long-identities",
                    0,
                    "planner",
                    &task_id,
                    &session_id,
                )?
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn prompt_dispatch_gate_deduplicates_and_allows_failed_retry() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let acquired = match store.reserve_prompt_dispatch(
            "goal-gate",
            "task-gate",
            "session-gate",
            2,
            "follow_up",
            "continuation",
            "resume the current task",
        )? {
            PromptDispatchDecision::Acquired(gate) => gate,
            PromptDispatchDecision::Duplicate(_) => {
                bail!("first prompt dispatch unexpectedly deduplicated")
            }
        };
        acquired.validate()?;
        let duplicate = store.reserve_prompt_dispatch(
            "goal-gate",
            "task-gate",
            "session-gate",
            2,
            "follow_up",
            "continuation",
            "resume the current task",
        )?;
        assert!(matches!(duplicate, PromptDispatchDecision::Duplicate(_)));
        let failed = store.settle_prompt_dispatch_gate(
            &acquired,
            PromptDispatchGateStatus::Failed,
            None,
            Some("provider rejected the prompt".to_string()),
        )?;
        failed.validate()?;
        let retry = store.reserve_prompt_dispatch(
            "goal-gate",
            "task-gate",
            "session-gate",
            2,
            "follow_up",
            "continuation",
            "resume the current task",
        )?;
        assert!(matches!(retry, PromptDispatchDecision::Acquired(_)));
        Ok(())
    }

    #[test]
    fn prompt_dispatch_gate_deduplicates_accepted_dispatch() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let acquired = match store.reserve_prompt_dispatch(
            "goal-gate",
            "task-gate",
            "session-gate",
            2,
            "follow_up",
            "runtime_fallback",
            "retry with fallback model",
        )? {
            PromptDispatchDecision::Acquired(gate) => gate,
            PromptDispatchDecision::Duplicate(_) => {
                bail!("first accepted dispatch unexpectedly deduplicated")
            }
        };
        let accepted = store.settle_prompt_dispatch_gate(
            &acquired,
            PromptDispatchGateStatus::Accepted,
            None,
            None,
        )?;
        accepted.validate()?;

        let duplicate = store.reserve_prompt_dispatch(
            "goal-gate",
            "task-gate",
            "session-gate",
            2,
            "follow_up",
            "runtime_fallback",
            "retry with fallback model",
        )?;
        assert!(matches!(duplicate, PromptDispatchDecision::Duplicate(_)));
        Ok(())
    }

    #[test]
    fn prompt_dispatch_gate_preserves_possibly_accepted_hold_until_expiry() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let acquired = match store.reserve_prompt_dispatch(
            "goal-gate",
            "task-gate-ambiguous",
            "session-gate",
            2,
            "follow_up",
            "continuation",
            "resume after an ambiguous response",
        )? {
            PromptDispatchDecision::Acquired(gate) => gate,
            PromptDispatchDecision::Duplicate(_) => {
                bail!("first ambiguous reservation unexpectedly deduplicated")
            }
        };
        let possibly_accepted = store.settle_prompt_dispatch_gate(
            &acquired,
            PromptDispatchGateStatus::PossiblyAccepted,
            None,
            Some("dispatch may have been accepted".to_string()),
        )?;
        assert!(possibly_accepted.hold_until.is_some());
        assert!(matches!(
            store.reserve_prompt_dispatch(
                "goal-gate",
                "task-gate-ambiguous",
                "session-gate",
                2,
                "follow_up",
                "continuation",
                "resume after an ambiguous response",
            )?,
            PromptDispatchDecision::Duplicate(_)
        ));

        let expired = PromptDispatchGate {
            hold_until: Some((Local::now() - Duration::seconds(1)).to_rfc3339()),
            ..possibly_accepted
        }
        .seal()?;
        write_json_atomic(&store.prompt_dispatch_gate_path(&expired.gate_id), &expired)?;
        assert!(matches!(
            store.reserve_prompt_dispatch(
                "goal-gate",
                "task-gate-ambiguous",
                "session-gate",
                2,
                "follow_up",
                "continuation",
                "resume after an ambiguous response",
            )?,
            PromptDispatchDecision::Acquired(_)
        ));
        Ok(())
    }

    #[test]
    fn prompt_dispatch_gate_supports_semantic_dedupe_and_expired_reservation_recovery() -> Result<()>
    {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first = store.reserve_prompt_dispatch_with_options(
            "goal-gate",
            "task-gate",
            "session-gate",
            3,
            "follow_up",
            "continuation",
            "resume task with the next step",
            Some("next-step"),
        )?;
        let acquired = match first {
            PromptDispatchDecision::Acquired(gate) => gate,
            PromptDispatchDecision::Duplicate(_) => {
                bail!("first semantic reservation was duplicate")
            }
        };
        let duplicate = store.reserve_prompt_dispatch_with_options(
            "goal-gate",
            "task-gate",
            "session-gate",
            3,
            "follow_up",
            "continuation",
            "RESUME TASK WITH A DIFFERENT WORDING",
            Some("next-step"),
        )?;
        assert!(matches!(duplicate, PromptDispatchDecision::Duplicate(_)));

        let possibly_accepted = store.settle_prompt_dispatch_gate(
            &acquired,
            PromptDispatchGateStatus::PossiblyAccepted,
            None,
            Some("provider response was ambiguous".to_string()),
        )?;

        let next_epoch_duplicate = store.reserve_prompt_dispatch_with_options(
            "goal-gate",
            "task-gate",
            "session-gate",
            4,
            "follow_up",
            "continuation",
            "resume task with another wording",
            Some("next-step"),
        )?;
        assert!(matches!(
            next_epoch_duplicate,
            PromptDispatchDecision::Duplicate(_)
        ));

        let expired = PromptDispatchGate {
            hold_until: Some((Local::now() - Duration::seconds(1)).to_rfc3339()),
            ..possibly_accepted
        }
        .seal()?;
        let path = store.prompt_dispatch_gate_path(&expired.gate_id);
        write_json_atomic(&path, &expired)?;
        let recovered = store.reserve_prompt_dispatch_with_options(
            "goal-gate",
            "task-gate",
            "session-gate",
            3,
            "follow_up",
            "continuation",
            "resume task with the next step",
            Some("next-step"),
        )?;
        assert!(matches!(recovered, PromptDispatchDecision::Acquired(_)));
        Ok(())
    }

    #[test]
    fn prompt_settle_decision_is_typed_and_idempotent() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let cases = [
            (PromptSettleEvent::Idle, PromptSettleAction::Dispatch),
            (
                PromptSettleEvent::BackgroundCompleted,
                PromptSettleAction::Dispatch,
            ),
            (
                PromptSettleEvent::FallbackRetry,
                PromptSettleAction::Dispatch,
            ),
            (PromptSettleEvent::Busy, PromptSettleAction::Hold),
            (PromptSettleEvent::Error, PromptSettleAction::Hold),
            (PromptSettleEvent::ContextPressure, PromptSettleAction::Stop),
            (PromptSettleEvent::UserStopped, PromptSettleAction::Stop),
        ];
        for (event, expected_action) in cases {
            let first = store.record_prompt_settle_decision(
                "goal-settle",
                "task-settle",
                "session-settle",
                1,
                "test",
                event.clone(),
            )?;
            assert!(!first.duplicate);
            assert_eq!(first.decision.action, expected_action);
            first.decision.validate()?;
            let second = store.record_prompt_settle_decision(
                "goal-settle",
                "task-settle",
                "session-settle",
                1,
                "test",
                event,
            )?;
            assert!(second.duplicate);
            assert_eq!(second.decision, first.decision);
        }
        Ok(())
    }

    #[test]
    fn continuation_guard_state_round_trips_and_blocks_omo_idle_conditions() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let initial =
            store.update_continuation_guard("session-guard", "goal-guard", "epoch-1", |guard| {
                guard.pending_question = true;
                guard.in_flight = true;
                guard.stagnation_count = 2;
            })?;
        assert_eq!(
            initial.blocking_reason(),
            Some("a user question is pending")
        );
        let replay = store
            .read_continuation_guard_for_session("session-guard")?
            .context("missing continuation guard")?;
        replay.validate()?;
        assert_eq!(replay, initial);

        let updated =
            store.update_continuation_guard("session-guard", "goal-guard", "epoch-2", |guard| {
                guard.pending_question = false;
                guard.in_flight = false;
                guard.stagnation_count = 0;
                guard.context_pressure = true;
            })?;
        assert_eq!(updated.epoch_id, "epoch-2");
        assert_eq!(updated.blocking_reason(), Some("context pressure detected"));
        Ok(())
    }

    #[test]
    fn baseline_dirty_forbidden_paths_do_not_trigger_hard_block() -> Result<()> {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["crates/gearbox_agent/src/gui.rs".to_string()],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["crates/gearbox_agent/src/gui.rs".to_string()],
            diff_hash: None,
        };
        let scope = Scope::new(
            vec!["crates/gearbox_agent/src/workers.rs".to_string()],
            vec!["crates/gearbox_agent/src/gui.rs".to_string()],
            10,
        );

        let (scope_check, _drift, baseline_dirty) =
            compute_baseline_aware_scope_with_baseline_dirty(&before, &after, &scope);

        // Baseline-dirty forbidden path should NOT trigger a hard block
        assert!(
            scope_check.forbidden_touches.is_empty(),
            "baseline-dirty forbidden paths must not appear in forbidden_touches: {:?}",
            scope_check.forbidden_touches
        );
        // It SHOULD be tracked as baseline_dirty
        assert_eq!(
            baseline_dirty,
            vec!["crates/gearbox_agent/src/gui.rs".to_string()],
            "baseline-dirty forbidden paths must be reported separately"
        );
        Ok(())
    }

    #[test]
    fn new_forbidden_touches_still_hard_block_after_baseline_filter() -> Result<()> {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["README.md".to_string()],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec![
                "README.md".to_string(),
                ".omo/config.json".to_string(),
            ],
            diff_hash: None,
        };
        let scope = Scope::new(
            vec!["src".to_string()],
            vec![".omo".to_string()],
            10,
        );

        let (scope_check, _drift, baseline_dirty) =
            compute_baseline_aware_scope_with_baseline_dirty(&before, &after, &scope);

        // New forbidden touch (not in baseline) must still hard-block
        assert_eq!(
            scope_check.forbidden_touches,
            vec![".omo/config.json".to_string()],
            "new forbidden touches must still appear in forbidden_touches"
        );
        // No baseline-dirty forbidden paths
        assert!(
            baseline_dirty.is_empty(),
            "no files should be baseline-dirty in this test"
        );
        Ok(())
    }

    #[test]
    fn global_provider_cooldown_round_trips_with_hash_binding() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let store = StateStore::new(workspace.path());
        store.initialize()?;
        let cooldown = GlobalProviderCooldown {
            schema_version: GLOBAL_PROVIDER_COOLDOWN_SCHEMA_VERSION,
            provider_scope: "opencode-free-tier".to_string(),
            failed_models: vec!["opencode/hy3-free".to_string()],
            reason: "provider rate limit".to_string(),
            failed_at: timestamp(),
            cooldown_until_ms: u64::try_from(Local::now().timestamp_millis())
                .unwrap_or(0)
                .saturating_add(86_400_000),
            source_task: "goal::task".to_string(),
            source_attempt: 1,
            recorded_at: timestamp(),
            receipt_hash: String::new(),
        };
        let path = store.write_global_provider_cooldown(cooldown)?;
        assert_eq!(path, store.global_provider_cooldown_path());
        let restored = store
            .read_global_provider_cooldown()?
            .expect("global cooldown should be durable");
        restored.validate()?;
        assert!(restored.is_active());
        assert_eq!(restored.failed_models, ["opencode/hy3-free"]);
        Ok(())
    }

    #[test]
    fn legacy_free_provider_receipt_infers_day_cooldown_without_global_artifact() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let store = StateStore::new(workspace.path());
        store.initialize()?;
        let worker_dir = store.worker_dir("goal::task");
        fs::create_dir_all(&worker_dir)?;
        fs::write(
            worker_dir.join("provider-cooldown.json"),
            serde_json::json!({
                "task_id": "goal::task",
                "model": "opencode/mimo-v2.5-free",
                "failure": "provider quota exceeded",
                "failed_at": timestamp(),
            })
            .to_string(),
        )?;
        let cooldown = store
            .read_global_provider_cooldown()?
            .expect("legacy free quota should infer a cooldown");
        assert!(cooldown.is_active());
        assert_eq!(cooldown.source_task, "goal::task");
        assert_eq!(cooldown.failed_models, ["opencode/mimo-v2.5-free"]);
        Ok(())
    }
}
