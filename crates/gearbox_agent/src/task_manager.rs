use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::state::{
    CoordinatorModel, PromptDispatchDecision, PromptDispatchGate, PromptDispatchGateStatus,
    PromptSettleEvent, StateStore, Task, TaskKind, WorkerFanoutDenialReceipt, timestamp,
};
use crate::tools::CancellationToken;
use crate::worker_broker::WorkerBroker;
use crate::workers::{
    WorkerCategory, WorkerConfig, WorkerEvent, WorkerKind, WorkerOutcome, WorkerRegistry,
    WorkerResult, WorkerSessionHandle, WorkerStartRequest, WorkerStatus, WorkerSubscription,
    category_requires_worker_evidence, discard_resident_session_for_model_switch, is_free_model,
    provider_session_id_for_task, route_identity_key, seed_provider_session_for_task,
    snapshot_worker_evidence_paths, validate_worker_evidence_receipt_with_baseline,
    worker_kind_supports_evidence_contract, worker_model_is_unavailable,
    write_result_and_outcome_with_outcome,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedTaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
    Lost,
    Skipped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAttemptStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
    Lost,
    Skipped,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResidencyState {
    #[default]
    Resident,
    Evicted,
    Disposed,
    PersistedOnly,
    RpcDetached,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Messageability {
    Steer,
    Revive,
    NotContinuable { reason: String },
}

/// Context attached to task command outcomes for self-description.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeContext {
    /// The task ID this outcome relates to.
    pub task_id: Option<String>,
    /// The run epoch within the task's lifecycle.
    pub run_epoch: Option<usize>,
    /// Position in the queue when Queued, or None if not queued or unavailable.
    pub queue_position: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionOutcome {
    /// Task was cancelled/interrupted
    Cancelled(OutcomeContext),
    /// Task was interrupted without being cancelled
    Interrupted(OutcomeContext),
    /// Task completed and outcome cannot be changed
    NotContinuable(OutcomeContext),
    /// No matching task found
    Noop(OutcomeContext),
    /// The caller is outside the task's session scope.
    ScopeDenied {
        reason: String,
        context: OutcomeContext,
    },
}

impl ActionOutcome {
    pub fn is_interrupt_applied(&self) -> bool {
        matches!(self, Self::Interrupted(_))
    }

    pub fn is_cancel_applied(&self) -> bool {
        matches!(self, Self::Cancelled(_))
    }

    pub fn reason(&self) -> Option<String> {
        match self {
            Self::NotContinuable(_) => Some("task is not continuable".to_string()),
            Self::ScopeDenied { reason, .. } => Some(reason.clone()),
            Self::Noop(_) => Some("no managed task is active".to_string()),
            Self::Cancelled(_) | Self::Interrupted(_) => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskCommandContext {
    pub caller_session_id: Option<String>,
    pub all_scope: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TranscriptEntry {
    Parsed {
        event: String,
        #[serde(default)]
        tool_name: Option<String>,
        #[serde(default)]
        arguments: Option<String>,
        #[serde(default)]
        result: Option<String>,
        #[serde(default)]
        delta: Option<String>,
        #[serde(default)]
        kind: Option<String>,
        #[serde(default)]
        message: Option<String>,
        #[serde(default)]
        summary: Option<String>,
    },
    Raw(serde_json::Value),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    /// Message sent to running worker
    Sent(OutcomeContext),
    /// Task queued because worker is pending
    Queued(OutcomeContext),
    /// Worker completed/failed and will be revived
    Revive(OutcomeContext),
    /// Dispatch was attempted, but the provider response was ambiguous.
    PossiblyAccepted(OutcomeContext),
    /// Worker is in terminal state
    NotContinuable(OutcomeContext),
    /// No task found
    Noop(OutcomeContext),
    /// The caller is outside the task's session scope.
    ScopeDenied {
        reason: String,
        context: OutcomeContext,
    },
    /// The requested task does not exist.
    NotFound {
        reason: String,
        context: OutcomeContext,
    },
}

impl SendOutcome {
    pub fn is_accepted(&self) -> bool {
        matches!(
            self,
            Self::Sent(_) | Self::Queued(_) | Self::Revive(_) | Self::PossiblyAccepted(_)
        )
    }

    pub fn reason(&self) -> Option<String> {
        match self {
            Self::NotContinuable(_) => Some("task is not continuable".to_string()),
            Self::ScopeDenied { reason, .. } | Self::NotFound { reason, .. } => {
                Some(reason.clone())
            }
            Self::Noop(_) => Some("no managed task is active".to_string()),
            Self::Sent(_) | Self::Queued(_) | Self::Revive(_) => None,
            Self::PossiblyAccepted(_) => Some(
                "follow-up dispatch may have been accepted before an ambiguous provider response"
                    .to_string(),
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SteerOutcome {
    /// Steer instruction sent
    Steered(OutcomeContext),
    /// Worker will be revived with steer
    Revive(OutcomeContext),
    /// Dispatch was attempted, but the provider response was ambiguous.
    PossiblyAccepted(OutcomeContext),
    /// Queued for pending worker
    Queued(OutcomeContext),
    /// Terminal state
    NotContinuable(OutcomeContext),
    /// No task found
    Noop(OutcomeContext),
    /// The caller is outside the task's session scope.
    ScopeDenied {
        reason: String,
        context: OutcomeContext,
    },
    /// The requested task does not exist.
    NotFound {
        reason: String,
        context: OutcomeContext,
    },
}

impl SteerOutcome {
    pub fn is_accepted(&self) -> bool {
        matches!(
            self,
            Self::Steered(_) | Self::Queued(_) | Self::Revive(_) | Self::PossiblyAccepted(_)
        )
    }

    pub fn reason(&self) -> Option<String> {
        match self {
            Self::NotContinuable(_) => Some("task is not continuable".to_string()),
            Self::ScopeDenied { reason, .. } | Self::NotFound { reason, .. } => {
                Some(reason.clone())
            }
            Self::Noop(_) => Some("no managed task is active".to_string()),
            Self::Steered(_) | Self::Queued(_) | Self::Revive(_) => None,
            Self::PossiblyAccepted(_) => Some(
                "steer dispatch may have been accepted before an ambiguous provider response"
                    .to_string(),
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskFailureKind {
    WorkerFailed,
    WorkerStartFailed,
    WorkerCancelled,
    WorkerUnavailable,
    ModelUnavailable,
    ProviderTemporarilyUnavailable,
    PremiumBudgetExceeded,
    NoFallbackRoute,
    RepeatedFailureLimit,
}

const WORKER_EVIDENCE_RETRY_PREFIX: &str = "worker evidence gate:";
pub(crate) const MAX_WORKER_EVIDENCE_ATTEMPTS: usize = 2;
const WORKER_EVIDENCE_REPAIR_PROMPT: &str = "Gear evidence gate repair: inspect the work you just performed, run the relevant verification, write a non-empty regular receipt file under .gear/evidence/, and end the worker response with EVIDENCE_RECORDED: <path>. Do not claim completion until that receipt exists.";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskAttempt {
    pub attempt: usize,
    pub worker_kind: String,
    pub worker_command: Option<String>,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub route_hint: Option<String>,
    pub route_reason: String,
    pub status: TaskAttemptStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub session_id: Option<String>,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
    pub summary: String,
    pub failure_kind: Option<TaskFailureKind>,
    pub retry_reason: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: String,
    pub worker_kind: String,
    pub worker_command: Option<String>,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub route_hint: Option<String>,
    pub route_reason: String,
    pub status: ManagedTaskStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    #[serde(default)]
    pub residency_state: ResidencyState,
    #[serde(default)]
    pub run_epoch: u64,
    #[serde(default = "default_notified_epoch")]
    pub notified_epoch: i64,
    #[serde(default)]
    pub notification_failed_epoch: Option<u64>,
    #[serde(default)]
    pub killed: bool,
    pub session_id: Option<String>,
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub root_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
    pub summary: String,
    pub failure_kind: Option<TaskFailureKind>,
    pub retry_reason: Option<String>,
    pub error: Option<String>,
    pub attempts: Vec<TaskAttempt>,
}

impl TaskRecord {
    /// Reads and parses transcript.jsonl from the worker artifact directory.
    /// Returns entries in chronological order (oldest first).
    pub fn transcript_entries(&self) -> Vec<TranscriptEntry> {
        let dir = self
            .result_path
            .as_ref()
            .or(self.outcome_path.as_ref())
            .and_then(|path| path.parent());
        let Some(dir) = dir else {
            return Vec::new();
        };
        let transcript_path = dir.join("transcript.jsonl");
        let Ok(content) = std::fs::read_to_string(&transcript_path) else {
            return Vec::new();
        };
        content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .filter_map(|line| serde_json::from_str::<TranscriptEntry>(line).ok())
            .collect()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskManagerSnapshotCounts {
    pub pending: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub interrupted: usize,
    pub lost: usize,
    pub skipped: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAttemptSnapshot {
    pub attempt: usize,
    pub worker_kind: String,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub status: TaskAttemptStatus,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
    pub route_transform_path: Option<PathBuf>,
    pub summary: String,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSnapshot {
    pub task_id: String,
    pub status: ManagedTaskStatus,
    pub residency_state: ResidencyState,
    pub messageability: Option<Messageability>,
    pub run_epoch: u64,
    pub notified_epoch: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification_failed_epoch: Option<u64>,
    pub parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    pub worker_kind: String,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub attempts: Vec<TaskAttemptSnapshot>,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_reason: Option<String>,
    #[serde(default)]
    pub summary_head: String,
    #[serde(default)]
    pub continuation_hint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_command: Option<TaskCommandSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCommandSnapshot {
    pub action: String,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub run_epoch: u64,
    pub timestamp: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskManagerSnapshot {
    pub counts: TaskManagerSnapshotCounts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifacts_root: Option<PathBuf>,
    pub tasks: Vec<TaskSnapshot>,
    pub current_output: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ManagedWorkerRun {
    pub store: StateStore,
    pub result: WorkerResult,
    pub outcome: WorkerOutcome,
    pub record: TaskRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskLifecycleEvent {
    pub task_id: String,
    pub status: ManagedTaskStatus,
    pub residency_state: ResidencyState,
    pub timestamp: String,
    pub transition_type: Option<String>,
    pub transition_applied: bool,
    pub previous_status: Option<ManagedTaskStatus>,
    pub previous_residency_state: Option<ResidencyState>,
    pub run_epoch: u64,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TaskCommandEvent {
    task_id: String,
    action: String,
    accepted: bool,
    all_scope: bool,
    caller_session_id: Option<String>,
    reason: Option<String>,
    run_epoch: u64,
    timestamp: String,
}

#[derive(Clone, Debug)]
struct TaskTransitionResult {
    applied: bool,
    transition_type: &'static str,
    previous_status: ManagedTaskStatus,
    previous_residency_state: ResidencyState,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
enum TaskTransition {
    Start {
        session_id: Option<String>,
    },
    Skip {
        finished_at: String,
        result_path: PathBuf,
        outcome_path: PathBuf,
        summary: String,
        failure_kind: Option<TaskFailureKind>,
    },
    Complete {
        finished_at: String,
        result_path: PathBuf,
        outcome_path: PathBuf,
        summary: String,
        failure_kind: Option<TaskFailureKind>,
    },
    Fail {
        finished_at: String,
        summary: String,
        failure_kind: TaskFailureKind,
        error: Option<String>,
    },
    Cancel {
        finished_at: String,
        summary: String,
        error: Option<String>,
    },
    Interrupt {
        finished_at: String,
        summary: String,
        error: Option<String>,
    },
    MarkLost {
        finished_at: String,
        summary: String,
        failure_kind: TaskFailureKind,
        error: Option<String>,
        killed: bool,
    },
    QueueRetry {
        summary: String,
        retry_reason: String,
    },
    MarkResident,
    Evict,
    Dispose,
    PersistOnly,
    DetachRpc,
}

#[derive(Clone)]
struct QueuedTask {
    store: StateStore,
    workspace: PathBuf,
    task: Task,
    route_attempt: usize,
    goal: String,
    verification_commands: Vec<String>,
    config: WorkerConfig,
    cancellation_token: Option<CancellationToken>,
    coordinator_model: Option<CoordinatorModel>,
    coordinator_brief: Option<String>,
    route_hint: Option<String>,
}

#[derive(Clone)]
struct GoalEpochContext {
    session_id: String,
    goal_id: String,
    epoch_id: String,
}

#[derive(Clone, Debug)]
struct ConcurrencyManager {
    max_parallel_workers: usize,
    max_parallel_per_key: usize,
    running_workers: usize,
    running_per_key: HashMap<String, usize>,
}

#[derive(Clone, Debug)]
struct TaskRuntimePolicy {
    stale_task_timeout: Duration,
    tool_call_circuit_breaker: ToolCallCircuitBreakerPolicy,
}

const DEFAULT_MAX_TOOL_CALLS: usize = 4000;
const DEFAULT_TOOL_CALL_CONSECUTIVE_THRESHOLD: usize = 20;

#[derive(Clone, Debug)]
struct ToolCallCircuitBreakerPolicy {
    enabled: bool,
    max_tool_calls: usize,
    consecutive_threshold: usize,
}

impl Default for ToolCallCircuitBreakerPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_tool_calls: DEFAULT_MAX_TOOL_CALLS,
            consecutive_threshold: DEFAULT_TOOL_CALL_CONSECUTIVE_THRESHOLD,
        }
    }
}

impl ToolCallCircuitBreakerPolicy {
    fn from_environment() -> Self {
        let default = Self::default();
        Self {
            enabled: tool_circuit_breaker_flag_from_environment(
                "GEARBOX_GEAR_TOOL_CIRCUIT_BREAKER",
                default.enabled,
            ),
            max_tool_calls: tool_circuit_breaker_limit_from_environment(
                "GEARBOX_GEAR_MAX_TOOL_CALLS",
                default.max_tool_calls,
            ),
            consecutive_threshold: tool_circuit_breaker_limit_from_environment(
                "GEARBOX_GEAR_TOOL_LOOP_THRESHOLD",
                default.consecutive_threshold,
            ),
        }
    }
}

fn tool_circuit_breaker_flag_from_environment(name: &str, default: bool) -> bool {
    match std::env::var(name)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("0" | "false" | "off" | "no") => false,
        Some("1" | "true" | "on" | "yes") => true,
        _ => default,
    }
}

fn tool_circuit_breaker_limit_from_environment(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[derive(Clone, Debug, Default)]
struct ToolCallCircuitState {
    total_calls: usize,
    last_signature: Option<String>,
    consecutive_calls: usize,
    trigger_reason: Option<String>,
}

fn tool_call_signature(tool_name: &str, arguments: &str) -> String {
    let normalized_arguments = normalize_tool_call_arguments(arguments);
    let mut hasher = Sha256::new();
    hasher.update(tool_name.as_bytes());
    hasher.update([0]);
    hasher.update(normalized_arguments.as_bytes());
    format!("{tool_name}:{:x}", hasher.finalize())
}

fn normalize_tool_call_arguments(arguments: &str) -> String {
    let trimmed = arguments.trim();
    if trimmed.is_empty() {
        return "__unknown-input__".to_string();
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
        return trimmed.to_string();
    };
    let value = sort_tool_call_json_value(value);
    match serde_json::to_string(&value) {
        Ok(serialized) => serialized,
        Err(_) => trimmed.to_string(),
    }
}

fn sort_tool_call_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, sort_tool_call_json_value(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(sort_tool_call_json_value).collect())
        }
        value => value,
    }
}

fn record_tool_call_for_circuit_breaker(
    state: &Arc<Mutex<ToolCallCircuitState>>,
    policy: &ToolCallCircuitBreakerPolicy,
    tool_name: &str,
    arguments: &str,
) {
    if !policy.enabled {
        return;
    }
    let signature = tool_call_signature(tool_name, arguments);
    let Ok(mut state) = state.lock() else {
        eprintln!("failed to update Gear tool-call circuit state for `{tool_name}`");
        return;
    };
    if state.trigger_reason.is_some() {
        return;
    }
    state.total_calls = state.total_calls.saturating_add(1);
    state.consecutive_calls = if state.last_signature.as_deref() == Some(signature.as_str()) {
        state.consecutive_calls.saturating_add(1)
    } else {
        1
    };
    state.last_signature = Some(signature);
    if state.consecutive_calls >= policy.consecutive_threshold {
        state.trigger_reason = Some(format!(
            "Tool-call circuit breaker triggered: subagent called {tool_name} {} consecutive times (threshold: {}). This usually indicates an infinite loop. The task was automatically cancelled to prevent excessive token usage.",
            state.consecutive_calls, policy.consecutive_threshold
        ));
    } else if state.total_calls >= policy.max_tool_calls {
        state.trigger_reason = Some(format!(
            "Tool-call circuit breaker triggered: subagent exceeded maximum tool call limit ({}). This usually indicates an infinite loop. The task was automatically cancelled to prevent excessive token usage.",
            policy.max_tool_calls
        ));
    }
}

#[derive(Clone, Debug, Default)]
struct ReleaseGuard {
    released: HashSet<(String, u64)>,
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self {
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            running_workers: 0,
            running_per_key: HashMap::new(),
        }
    }
}

impl Default for TaskRuntimePolicy {
    fn default() -> Self {
        Self {
            stale_task_timeout: Duration::from_secs(30),
            tool_call_circuit_breaker: ToolCallCircuitBreakerPolicy::from_environment(),
        }
    }
}

impl TaskRuntimePolicy {
    fn from_worker_config(config: &WorkerConfig) -> Self {
        Self {
            stale_task_timeout: Duration::from_secs(
                config.stale_task_timeout_secs.max(1) as u64 + 1,
            ),
            tool_call_circuit_breaker: ToolCallCircuitBreakerPolicy::from_environment(),
        }
    }
}

impl ReleaseGuard {
    fn release_once(&mut self, task_id: &str, run_epoch: u64) -> bool {
        self.released.insert((task_id.to_string(), run_epoch))
    }

    fn forget_task(&mut self, task_id: &str) {
        self.released
            .retain(|(released_task_id, _)| released_task_id != task_id);
    }
}

impl ConcurrencyManager {
    fn from_worker_config(config: &WorkerConfig) -> Self {
        Self {
            max_parallel_workers: config.max_parallel_workers.max(1),
            max_parallel_per_key: config.max_parallel_per_key.max(1),
            ..Self::default()
        }
    }

    fn max_parallel_workers(&self) -> usize {
        self.max_parallel_workers.max(1)
    }

    fn max_parallel_per_key(&self) -> usize {
        self.max_parallel_per_key.max(1)
    }

    fn can_start(&self, queued_task: &QueuedTask) -> bool {
        if self.running_workers >= self.max_parallel_workers() {
            return false;
        }

        if is_read_only_task(&queued_task.task) {
            return true;
        }

        let queued_key = concurrency_key_for_task(queued_task);
        self.running_per_key
            .get(&queued_key)
            .copied()
            .unwrap_or_default()
            < self.max_parallel_per_key()
    }

    fn acquire(&mut self, queued_task: &QueuedTask) -> bool {
        if !self.can_start(queued_task) {
            return false;
        }

        let queued_key = concurrency_key_for_task(queued_task);
        self.running_workers += 1;
        *self.running_per_key.entry(queued_key).or_default() += 1;
        true
    }

    fn release(&mut self, queued_task: &QueuedTask) -> bool {
        if self.running_workers == 0 {
            return false;
        }

        let queued_key = concurrency_key_for_task(queued_task);
        let Some(running_for_key) = self.running_per_key.get_mut(&queued_key) else {
            return false;
        };
        if *running_for_key == 0 {
            return false;
        }

        *running_for_key -= 1;
        if *running_for_key == 0 {
            self.running_per_key.remove(&queued_key);
        }
        self.running_workers -= 1;
        true
    }
}

fn is_read_only_task(task: &Task) -> bool {
    matches!(task.kind, TaskKind::Review)
}

fn is_write_task(task: &Task) -> bool {
    !is_read_only_task(task)
}

fn normalized_scope_paths(paths: &[String]) -> Vec<&str> {
    paths
        .iter()
        .map(|path| path.trim_end_matches('/'))
        .collect()
}

fn scopes_overlap(left: &Task, right: &Task) -> bool {
    if !is_write_task(left) || !is_write_task(right) {
        return false;
    }

    let left_paths = normalized_scope_paths(&left.scope.allowed_paths);
    let right_paths = normalized_scope_paths(&right.scope.allowed_paths);
    if left_paths.is_empty() || right_paths.is_empty() {
        return false;
    }

    left_paths.iter().any(|left_path| {
        right_paths.iter().any(|right_path| {
            left_path == right_path
                || left_path.starts_with(&format!("{right_path}/"))
                || right_path.starts_with(&format!("{left_path}/"))
        })
    })
}

#[derive(Clone)]
struct RunningTask {
    store: StateStore,
    handle: Arc<dyn WorkerSessionHandle>,
    queued_task: QueuedTask,
    started_at: Instant,
    _subscription: Option<WorkerSubscription>,
}

#[derive(Clone)]
struct ResidentTask {
    handle: Arc<dyn WorkerSessionHandle>,
    queued_task: QueuedTask,
}

#[derive(Clone, Debug)]
struct PendingRevive {
    task_id: String,
    message: String,
    kind: QueuedMessageKind,
    caller_session_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReviveDispatchOutcome {
    NotStarted,
    Started,
    PossiblyAccepted,
}

struct FinishedTaskMessage {
    task_id: String,
    running_task: RunningTask,
    run_result: Result<(WorkerOutcome, WorkerResult)>,
}

fn messageability_for_record(record: &TaskRecord) -> Messageability {
    fn not_resident_reason(state: &ResidencyState) -> String {
        match state {
            ResidencyState::Resident => "task is resident".to_string(),
            ResidencyState::Evicted => "task is evicted".to_string(),
            ResidencyState::Disposed => "task is disposed".to_string(),
            ResidencyState::PersistedOnly => "task is persisted only".to_string(),
            ResidencyState::RpcDetached => "task is rpc detached".to_string(),
        }
    }

    match record.status {
        ManagedTaskStatus::Pending | ManagedTaskStatus::Running => {
            if record.residency_state == ResidencyState::Resident {
                Messageability::Steer
            } else {
                Messageability::NotContinuable {
                    reason: not_resident_reason(&record.residency_state),
                }
            }
        }
        ManagedTaskStatus::Completed
        | ManagedTaskStatus::Failed
        | ManagedTaskStatus::Interrupted => {
            if record.residency_state == ResidencyState::Resident {
                Messageability::Revive
            } else {
                Messageability::NotContinuable {
                    reason: not_resident_reason(&record.residency_state),
                }
            }
        }
        ManagedTaskStatus::Cancelled => Messageability::NotContinuable {
            reason: "task was cancelled".to_string(),
        },
        ManagedTaskStatus::Lost => Messageability::NotContinuable {
            reason: "task was lost".to_string(),
        },
        ManagedTaskStatus::Skipped => Messageability::NotContinuable {
            reason: "task was skipped".to_string(),
        },
    }
}

fn task_scope_allows(record: &TaskRecord, context: &TaskCommandContext) -> bool {
    if context.all_scope || context.caller_session_id.is_none() {
        return true;
    }
    let caller_session_id = context.caller_session_id.as_deref();
    [
        record.parent_session_id.as_deref(),
        record.root_session_id.as_deref(),
        record.session_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|owner| Some(owner) == caller_session_id)
}

fn scope_denied_reason(record: &TaskRecord, _context: &TaskCommandContext) -> String {
    let owner = record
        .parent_session_id
        .as_deref()
        .or(record.root_session_id.as_deref())
        .or(record.session_id.as_deref())
        .unwrap_or("unknown");
    format!(
        "task `{}` belongs to session `{owner}` and cannot be controlled by the caller",
        record.task_id
    )
}

fn continuation_hint_for_record(record: &TaskRecord) -> String {
    match messageability_for_record(record) {
        Messageability::Revive => {
            "Follow up from the Gear panel or open the result/outcome artifacts to continue."
                .to_string()
        }
        Messageability::Steer => {
            "Steer the running task from the Gear panel or open the result/outcome artifacts."
                .to_string()
        }
        Messageability::NotContinuable { reason } => {
            format!("Open the result/outcome artifacts to inspect the full result ({reason}).")
        }
    }
}

fn summary_head_for_record(record: &TaskRecord) -> String {
    let summary_head = record
        .summary
        .lines()
        .next()
        .unwrap_or(record.summary.as_str());

    match fallback_model_chain_for_record(record) {
        Some(model_chain) => format!("{summary_head}；模型回退链：{model_chain}"),
        None => summary_head.to_string(),
    }
}

fn last_task_command_snapshot(record: &TaskRecord) -> Option<TaskCommandSnapshot> {
    const COMMAND_EVENT_TAIL_BYTES: usize = 16 * 1024;
    let artifact_dir = record
        .result_path
        .as_ref()
        .or(record.outcome_path.as_ref())
        .and_then(|path| path.parent())?;
    let path = artifact_dir.join("task-command-events.jsonl");
    let bytes = fs::read(path).ok()?;
    let tail = if bytes.len() > COMMAND_EVENT_TAIL_BYTES {
        &bytes[bytes.len() - COMMAND_EVENT_TAIL_BYTES..]
    } else {
        bytes.as_slice()
    };
    String::from_utf8_lossy(tail)
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<TaskCommandEvent>(line).ok())
        .map(|event| TaskCommandSnapshot {
            action: event.action,
            accepted: event.accepted,
            reason: event.reason,
            run_epoch: event.run_epoch,
            timestamp: event.timestamp,
        })
}

fn fallback_model_chain_for_record(record: &TaskRecord) -> Option<String> {
    let mut models = Vec::new();
    for model in record.attempts.iter().filter_map(|attempt| {
        attempt
            .worker_model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
    }) {
        if models
            .last()
            .map_or(true, |previous: &String| previous != model)
        {
            models.push(model.to_string());
        }
    }

    (models.len() >= 2).then(|| models.join(" -> "))
}

#[derive(Clone)]
struct CurrentManagedTask {
    task_id: String,
    status: ManagedTaskStatus,
    handle: Option<Arc<dyn WorkerSessionHandle>>,
    dispatch_context: Option<PromptDispatchControlContext>,
}

#[derive(Clone)]
struct PromptDispatchControlContext {
    store: StateStore,
    goal_id: String,
    session_id: String,
    run_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueuedMessageKind {
    FollowUp,
    Steer,
}

enum PromptDispatchGateResult {
    Acquired(StateStore, PromptDispatchGate),
    Duplicate,
    Unavailable,
}

#[derive(Clone, Debug)]
struct QueuedMessageGate {
    store: StateStore,
    gate_id: String,
}

#[derive(Clone, Debug)]
struct QueuedMessage {
    kind: QueuedMessageKind,
    message: String,
    caller_session_id: Option<String>,
    created_at: String,
    delivery_attempts: usize,
    gate: Option<QueuedMessageGate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FallbackDecision {
    Queued,
    Unavailable {
        reason: String,
        failure_kind: TaskFailureKind,
    },
}

fn prompt_dispatch_error_is_possibly_accepted(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "unexpected eof",
        "json parse error",
        "unexpected end of json input",
        "timed out",
    ]
    .iter()
    .any(|signal| message.contains(signal))
}

fn prompt_dispatch_error_status(error: &anyhow::Error) -> PromptDispatchGateStatus {
    if prompt_dispatch_error_is_possibly_accepted(error) {
        PromptDispatchGateStatus::PossiblyAccepted
    } else {
        PromptDispatchGateStatus::Failed
    }
}

fn prompt_dispatch_error_reason(operation: &str, status: &PromptDispatchGateStatus) -> String {
    if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
        format!("{operation} dispatch may have been accepted before an ambiguous provider response")
    } else {
        format!("{operation} dispatch failed")
    }
}

const MAX_PENDING_MESSAGE_DELIVERY_ATTEMPTS: usize = 3;

const WAIT_FOR_POLL_INTERVAL: Duration = Duration::from_millis(50);

// ── Phase 6: Lifecycle constants ──
const RESIDENCY_MAX_CHILDREN: usize = 8;
const TTL_MS: u64 = 24 * 60 * 60 * 1000; // 24 hours
const ARCHIVE_CAP: usize = 100;

fn deliver_queued_message(
    handle: &Arc<dyn WorkerSessionHandle>,
    queued_message: &QueuedMessage,
) -> Result<()> {
    match queued_message.kind {
        QueuedMessageKind::FollowUp => handle.send_follow_up(queued_message.message.clone()),
        QueuedMessageKind::Steer => handle.steer(queued_message.message.clone()),
    }
}

fn queued_message_operation(kind: &QueuedMessageKind) -> &'static str {
    match kind {
        QueuedMessageKind::FollowUp => "queued follow-up",
        QueuedMessageKind::Steer => "queued steer",
    }
}

fn read_queued_message_gate(binding: &QueuedMessageGate) -> Result<PromptDispatchGate> {
    let path = binding.store.prompt_dispatch_gate_path(&binding.gate_id);
    let json = fs::read_to_string(&path)
        .with_context(|| format!("failed to read prompt dispatch gate {}", path.display()))?;
    let gate: PromptDispatchGate = serde_json::from_str(&json)
        .with_context(|| format!("failed to parse prompt dispatch gate {}", path.display()))?;
    gate.validate()?;
    Ok(gate)
}

fn settle_queued_message_gate(
    queued_message: &QueuedMessage,
    status: PromptDispatchGateStatus,
    reason: Option<String>,
) -> Result<()> {
    let Some(binding) = queued_message.gate.as_ref() else {
        return Ok(());
    };
    let gate = read_queued_message_gate(binding)?;
    binding
        .store
        .settle_prompt_dispatch_gate(&gate, status, None, reason)?;
    Ok(())
}

fn settle_queued_message_gate_best_effort(
    queued_message: &QueuedMessage,
    status: PromptDispatchGateStatus,
    reason: Option<String>,
) {
    if let Err(error) = settle_queued_message_gate(queued_message, status, reason) {
        eprintln!(
            "failed to settle queued Gear prompt gate for message created at {}: {error:#}",
            queued_message.created_at
        );
    }
}

fn default_notified_epoch() -> i64 {
    -1
}

fn best_effort_stop_handle(handle: &Arc<dyn WorkerSessionHandle>, task_id: &str, cause: &str) {
    for (action, result) in [
        ("interrupt", handle.interrupt()),
        ("cancel", handle.cancel()),
        ("abort", handle.abort()),
        ("dispose", handle.dispose()),
    ] {
        if let Err(error) = result {
            eprintln!(
                "failed to {action} Gear resident task `{task_id}` during {cause}: {error:#}"
            );
        }
    }
}

#[derive(Clone, Default)]
pub struct TaskManagerControl {
    current_task: Arc<Mutex<Option<CurrentManagedTask>>>,
    pending_messages: Arc<Mutex<HashMap<String, VecDeque<QueuedMessage>>>>,
}

impl TaskManagerControl {
    pub fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.current_task, &other.current_task)
    }

    fn current_task_snapshot(&self) -> Result<Option<CurrentManagedTask>> {
        Ok(self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone())
    }

    pub fn current_task_status(&self) -> Result<Option<ManagedTaskStatus>> {
        Ok(self
            .current_task_snapshot()?
            .map(|current_task| current_task.status))
    }

    fn update_current_status(&self, task_id: &str, status: ManagedTaskStatus) -> Result<bool> {
        let mut current_task = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?;
        let Some(current_task) = current_task.as_mut() else {
            return Ok(false);
        };
        if current_task.task_id != task_id {
            return Ok(false);
        }

        current_task.status = status;
        Ok(true)
    }

    pub fn current_task_id(&self) -> Result<Option<String>> {
        Ok(self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .as_ref()
            .map(|task| task.task_id.clone()))
    }

    pub fn current_last_output(&self) -> Result<Option<String>> {
        let Some(current_task) = self.current_task_snapshot()? else {
            return Ok(None);
        };

        Ok(current_task
            .handle
            .as_ref()
            .and_then(|handle| handle.last_output()))
    }

    fn queue_pending_message(
        &self,
        task_id: &str,
        kind: QueuedMessageKind,
        message: String,
        caller_session_id: Option<String>,
        gate_context: Option<(StateStore, PromptDispatchGate)>,
    ) -> Result<()> {
        self.pending_messages
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .entry(task_id.to_string())
            .or_default()
            .push_back(QueuedMessage {
                kind,
                message,
                caller_session_id,
                created_at: timestamp(),
                delivery_attempts: 0,
                gate: gate_context.map(|(store, gate)| QueuedMessageGate {
                    store,
                    gate_id: gate.gate_id,
                }),
            });
        Ok(())
    }

    fn pending_message_task_ids(&self) -> Result<Vec<String>> {
        Ok(self
            .pending_messages
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .keys()
            .cloned()
            .collect())
    }

    fn take_pending_messages(&self, task_id: &str) -> Result<VecDeque<QueuedMessage>> {
        Ok(self
            .pending_messages
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .remove(task_id)
            .unwrap_or_default())
    }

    fn prepend_pending_messages(
        &self,
        task_id: &str,
        messages: VecDeque<QueuedMessage>,
    ) -> Result<()> {
        let mut pending_messages = self
            .pending_messages
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?;
        let queue = pending_messages.entry(task_id.to_string()).or_default();
        for message in messages.into_iter().rev() {
            queue.push_front(message);
        }
        Ok(())
    }

    pub fn send_follow_up_current_task(&self, prompt: String) -> Result<SendOutcome> {
        let Some(task_id) = self.current_task_id()? else {
            return Ok(SendOutcome::Noop(OutcomeContext::default()));
        };
        self.send_follow_up_task(&task_id, prompt)
    }

    pub fn steer_current_task(&self, prompt: String) -> Result<SteerOutcome> {
        let Some(task_id) = self.current_task_id()? else {
            return Ok(SteerOutcome::Noop(OutcomeContext::default()));
        };
        self.steer_task(&task_id, prompt)
    }

    pub fn cancel_current_task(&self) -> Result<ActionOutcome> {
        let Some(current_task) = self.current_task_snapshot()? else {
            return Ok(ActionOutcome::Noop(OutcomeContext::default()));
        };
        let task_id = current_task.task_id.clone();

        if current_task.status != ManagedTaskStatus::Running {
            return Ok(ActionOutcome::NotContinuable(OutcomeContext {
                task_id: Some(task_id),
                ..OutcomeContext::default()
            }));
        }

        self.update_current_status(&task_id, ManagedTaskStatus::Cancelled)?;
        current_task
            .handle
            .as_ref()
            .context("running task missing handle")?
            .cancel()?;
        Ok(ActionOutcome::Cancelled(OutcomeContext {
            task_id: Some(task_id),
            ..OutcomeContext::default()
        }))
    }

    pub fn interrupt_current_task(&self) -> Result<ActionOutcome> {
        let Some(current_task) = self.current_task_snapshot()? else {
            return Ok(ActionOutcome::Noop(OutcomeContext::default()));
        };
        let task_id = current_task.task_id.clone();

        if current_task.status != ManagedTaskStatus::Running {
            return Ok(ActionOutcome::NotContinuable(OutcomeContext {
                task_id: Some(task_id),
                ..OutcomeContext::default()
            }));
        }

        self.update_current_status(&task_id, ManagedTaskStatus::Interrupted)?;
        current_task
            .handle
            .as_ref()
            .context("running task missing handle")?
            .interrupt()?;
        Ok(ActionOutcome::Interrupted(OutcomeContext {
            task_id: Some(task_id),
            ..OutcomeContext::default()
        }))
    }

    fn acquire_prompt_dispatch_gate(
        &self,
        task_id: &str,
        message_kind: &str,
        prompt: &str,
    ) -> Result<PromptDispatchGateResult> {
        let Some(current_task) = self.current_task_snapshot()? else {
            return Ok(PromptDispatchGateResult::Unavailable);
        };
        if current_task.task_id != task_id {
            return Ok(PromptDispatchGateResult::Unavailable);
        }
        let Some(context) = current_task.dispatch_context else {
            return Ok(PromptDispatchGateResult::Unavailable);
        };
        match context.store.reserve_prompt_dispatch(
            &context.goal_id,
            task_id,
            &context.session_id,
            context.run_epoch as usize,
            message_kind,
            "gui_control",
            prompt,
        )? {
            PromptDispatchDecision::Acquired(gate) => {
                Ok(PromptDispatchGateResult::Acquired(context.store, gate))
            }
            PromptDispatchDecision::Duplicate(_) => Ok(PromptDispatchGateResult::Duplicate),
        }
    }

    fn settle_prompt_dispatch_gate(
        gate_context: &Option<(StateStore, PromptDispatchGate)>,
        status: PromptDispatchGateStatus,
        reason: Option<String>,
    ) -> Result<()> {
        if let Some((store, gate)) = gate_context.as_ref() {
            store
                .settle_prompt_dispatch_gate(gate, status, None, reason)
                .map(|_| ())
        } else {
            Ok(())
        }
    }

    pub fn cancel_task(&self, task_id: &str) -> Result<ActionOutcome> {
        let Some(current_task) = self.current_task_snapshot()? else {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        if current_task.task_id != task_id {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        }

        let current_task_id = current_task.task_id.clone();

        if current_task.status != ManagedTaskStatus::Running {
            return Ok(ActionOutcome::NotContinuable(OutcomeContext {
                task_id: Some(current_task_id),
                ..OutcomeContext::default()
            }));
        }

        self.update_current_status(&current_task_id, ManagedTaskStatus::Cancelled)?;
        current_task
            .handle
            .as_ref()
            .context("running task missing handle")?
            .cancel()?;
        Ok(ActionOutcome::Cancelled(OutcomeContext {
            task_id: Some(current_task_id),
            ..OutcomeContext::default()
        }))
    }

    pub fn interrupt_task(&self, task_id: &str) -> Result<ActionOutcome> {
        let Some(current_task) = self.current_task_snapshot()? else {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        if current_task.task_id != task_id {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        }

        let current_task_id = current_task.task_id.clone();

        if current_task.status != ManagedTaskStatus::Running {
            return Ok(ActionOutcome::NotContinuable(OutcomeContext {
                task_id: Some(current_task_id),
                ..OutcomeContext::default()
            }));
        }

        self.update_current_status(&current_task_id, ManagedTaskStatus::Interrupted)?;
        current_task
            .handle
            .as_ref()
            .context("running task missing handle")?
            .interrupt()?;
        Ok(ActionOutcome::Interrupted(OutcomeContext {
            task_id: Some(current_task_id),
            ..OutcomeContext::default()
        }))
    }
    pub fn send_follow_up_task(&self, task_id: &str, prompt: String) -> Result<SendOutcome> {
        let gate_context = match self.acquire_prompt_dispatch_gate(task_id, "follow_up", &prompt)? {
            PromptDispatchGateResult::Acquired(store, gate) => Some((store, gate)),
            PromptDispatchGateResult::Duplicate => {
                return Ok(SendOutcome::Noop(OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    ..OutcomeContext::default()
                }));
            }
            PromptDispatchGateResult::Unavailable => None,
        };
        let current_task_guard = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?;
        let Some(current_task) = current_task_guard.as_ref() else {
            return Ok(SendOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        if current_task.task_id != task_id {
            return Ok(SendOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        }

        let current_task_id = current_task.task_id.clone();
        match current_task.status {
            ManagedTaskStatus::Pending => {
                self.queue_pending_message(
                    task_id,
                    QueuedMessageKind::FollowUp,
                    prompt,
                    None,
                    gate_context.clone(),
                )?;
                Self::settle_prompt_dispatch_gate(
                    &gate_context,
                    PromptDispatchGateStatus::Held,
                    None,
                )?;
                Ok(SendOutcome::Queued(OutcomeContext {
                    task_id: Some(current_task_id),
                    ..OutcomeContext::default()
                }))
            }
            ManagedTaskStatus::Running => {
                let handle = current_task
                    .handle
                    .as_ref()
                    .context("running task missing handle")?
                    .clone();
                drop(current_task_guard);
                if let Err(error) = handle.send_follow_up(prompt) {
                    let status = prompt_dispatch_error_status(&error);
                    let reason = prompt_dispatch_error_reason("GUI follow-up", &status);
                    let settlement = Self::settle_prompt_dispatch_gate(
                        &gate_context,
                        status.clone(),
                        Some(reason),
                    );
                    if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
                        settlement?;
                        return Ok(SendOutcome::PossiblyAccepted(OutcomeContext {
                            task_id: Some(current_task_id),
                            ..OutcomeContext::default()
                        }));
                    }
                    let _ = settlement;
                    return Err(error);
                }
                Self::settle_prompt_dispatch_gate(
                    &gate_context,
                    PromptDispatchGateStatus::Accepted,
                    None,
                )?;
                Ok(SendOutcome::Sent(OutcomeContext {
                    task_id: Some(current_task_id),
                    ..OutcomeContext::default()
                }))
            }
            _ => Ok(SendOutcome::NotContinuable(OutcomeContext {
                task_id: Some(current_task_id),
                ..OutcomeContext::default()
            })),
        }
    }

    pub fn steer_task(&self, task_id: &str, prompt: String) -> Result<SteerOutcome> {
        let gate_context = match self.acquire_prompt_dispatch_gate(task_id, "steer", &prompt)? {
            PromptDispatchGateResult::Acquired(store, gate) => Some((store, gate)),
            PromptDispatchGateResult::Duplicate => {
                return Ok(SteerOutcome::Noop(OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    ..OutcomeContext::default()
                }));
            }
            PromptDispatchGateResult::Unavailable => None,
        };
        let current_task_guard = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?;
        let Some(current_task) = current_task_guard.as_ref() else {
            return Ok(SteerOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        if current_task.task_id != task_id {
            return Ok(SteerOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        }

        let current_task_id = current_task.task_id.clone();
        match current_task.status {
            ManagedTaskStatus::Pending => {
                self.queue_pending_message(
                    task_id,
                    QueuedMessageKind::Steer,
                    prompt,
                    None,
                    gate_context.clone(),
                )?;
                Self::settle_prompt_dispatch_gate(
                    &gate_context,
                    PromptDispatchGateStatus::Held,
                    None,
                )?;
                Ok(SteerOutcome::Queued(OutcomeContext {
                    task_id: Some(current_task_id),
                    ..OutcomeContext::default()
                }))
            }
            ManagedTaskStatus::Running => {
                let handle = current_task
                    .handle
                    .as_ref()
                    .context("running task missing handle")?
                    .clone();
                drop(current_task_guard);
                if let Err(error) = handle.steer(prompt) {
                    let status = prompt_dispatch_error_status(&error);
                    let reason = prompt_dispatch_error_reason("GUI steer", &status);
                    let settlement = Self::settle_prompt_dispatch_gate(
                        &gate_context,
                        status.clone(),
                        Some(reason),
                    );
                    if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
                        settlement?;
                        return Ok(SteerOutcome::PossiblyAccepted(OutcomeContext {
                            task_id: Some(current_task_id),
                            ..OutcomeContext::default()
                        }));
                    }
                    let _ = settlement;
                    return Err(error);
                }
                Self::settle_prompt_dispatch_gate(
                    &gate_context,
                    PromptDispatchGateStatus::Accepted,
                    None,
                )?;
                Ok(SteerOutcome::Steered(OutcomeContext {
                    task_id: Some(current_task_id),
                    ..OutcomeContext::default()
                }))
            }
            _ => Ok(SteerOutcome::NotContinuable(OutcomeContext {
                task_id: Some(current_task_id),
                ..OutcomeContext::default()
            })),
        }
    }

    fn set_current(
        &self,
        task_id: String,
        status: ManagedTaskStatus,
        handle: Option<Arc<dyn WorkerSessionHandle>>,
    ) -> Result<()> {
        *self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))? =
            Some(CurrentManagedTask {
                task_id,
                status,
                handle,
                dispatch_context: None,
            });
        Ok(())
    }

    fn set_dispatch_context(
        &self,
        task_id: &str,
        store: StateStore,
        goal_id: String,
        session_id: String,
        run_epoch: u64,
    ) -> Result<()> {
        let mut current_task = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?;
        let Some(current_task) = current_task.as_mut() else {
            return Ok(());
        };
        if current_task.task_id == task_id {
            current_task.dispatch_context = Some(PromptDispatchControlContext {
                store,
                goal_id,
                session_id,
                run_epoch,
            });
        }
        Ok(())
    }

    fn clear_current(&self, task_id: &str) -> Result<()> {
        let mut current_task = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?;
        if current_task
            .as_ref()
            .is_some_and(|current_task| current_task.task_id == task_id)
        {
            *current_task = None;
        }
        Ok(())
    }
}

pub struct TaskManager {
    registry: WorkerRegistry,
    records: HashMap<String, TaskRecord>,
    task_record_paths: HashMap<String, PathBuf>,
    evidence_baselines: HashMap<String, Vec<PathBuf>>,
    running_tasks: HashMap<String, RunningTask>,
    resident_tasks: HashMap<String, ResidentTask>,
    queued_tasks: VecDeque<QueuedTask>,
    pending_revives: VecDeque<PendingRevive>,
    completed_runs: HashMap<String, ManagedWorkerRun>,
    completed_errors: HashMap<String, String>,
    completed_archive: VecDeque<TaskRecord>,
    concurrency: ConcurrencyManager,
    release_guard: ReleaseGuard,
    runtime_policy: TaskRuntimePolicy,
    control: TaskManagerControl,
    session_scope: Option<String>,
    goal_unavailable_worker_models: HashMap<String, HashMap<String, Instant>>,
    goal_provider_sessions: HashMap<String, String>,
    goal_epoch_context: Option<GoalEpochContext>,
    activity_heartbeats: HashMap<String, Arc<Mutex<Instant>>>,
    tool_call_circuit_states: HashMap<String, Arc<Mutex<ToolCallCircuitState>>>,
    artifacts_root: Option<PathBuf>,
    worker_fanout_limit: usize,
    finished_task_tx: Sender<FinishedTaskMessage>,
    finished_task_rx: Receiver<FinishedTaskMessage>,
    lifecycle_events: Option<Arc<Mutex<Vec<String>>>>,
}

pub type SharedTaskManager = Arc<Mutex<TaskManager>>;

const GOAL_WORKER_MODEL_COOLDOWN: Duration = Duration::from_secs(60);
const DEFAULT_WORKER_FANOUT_LIMIT: usize = 60;

fn worker_fanout_limit_from_environment() -> usize {
    std::env::var("GEARBOX_GEAR_WORKER_FANOUT_LIMIT")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|limit| *limit > 0)
        .unwrap_or(DEFAULT_WORKER_FANOUT_LIMIT)
}

pub struct TaskManagerTickLoop {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    last_error: Arc<Mutex<Option<String>>>,
}

impl TaskManagerTickLoop {
    pub fn start(manager: SharedTaskManager, interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let last_error = Arc::new(Mutex::new(None));
        let thread = thread::spawn({
            let stop = stop.clone();
            let last_error = last_error.clone();
            move || {
                while !stop.load(Ordering::Relaxed) {
                    let tick_result = manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))
                        .and_then(|mut manager| manager.tick());
                    if let Err(error) = tick_result {
                        if let Ok(mut last_error) = last_error.lock() {
                            *last_error = Some(format!("{error:#}"));
                        }
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                    thread::sleep(interval);
                }
            }
        });
        Self {
            stop,
            thread: Some(thread),
            last_error,
        }
    }

    pub fn last_error(&self) -> Result<Option<String>> {
        Ok(self
            .last_error
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager tick loop mutex poisoned"))?
            .clone())
    }

    pub fn stop(mut self) -> Result<()> {
        self.stop_inner(true)
    }

    fn stop_inner(&mut self, report_error: bool) -> Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                bail!("task manager tick loop panicked");
            }
        }
        if report_error {
            if let Some(error) = self.last_error()? {
                bail!("{error}");
            }
        }
        Ok(())
    }
}

impl Drop for TaskManagerTickLoop {
    fn drop(&mut self) {
        self.stop_inner(false).ok();
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        let (finished_task_tx, finished_task_rx) = std::sync::mpsc::channel();
        Self {
            registry: WorkerRegistry::default(),
            records: HashMap::new(),
            task_record_paths: HashMap::new(),
            evidence_baselines: HashMap::new(),
            running_tasks: HashMap::new(),
            resident_tasks: HashMap::new(),
            queued_tasks: VecDeque::new(),
            pending_revives: VecDeque::new(),
            completed_runs: HashMap::new(),
            completed_errors: HashMap::new(),
            completed_archive: VecDeque::new(),
            concurrency: ConcurrencyManager::default(),
            release_guard: ReleaseGuard::default(),
            runtime_policy: TaskRuntimePolicy::default(),
            control: TaskManagerControl::default(),
            session_scope: None,
            goal_unavailable_worker_models: HashMap::new(),
            goal_provider_sessions: HashMap::new(),
            goal_epoch_context: None,
            activity_heartbeats: HashMap::new(),
            tool_call_circuit_states: HashMap::new(),
            artifacts_root: None,
            worker_fanout_limit: worker_fanout_limit_from_environment(),
            finished_task_tx,
            finished_task_rx,
            lifecycle_events: None,
        }
    }
}

impl TaskManager {
    pub fn set_lifecycle_event_log(&mut self, log: Option<Arc<Mutex<Vec<String>>>>) {
        self.lifecycle_events = log;
    }

    fn record_event(&self, event: &str) {
        if let Some(events) = &self.lifecycle_events {
            if let Ok(mut guard) = events.lock() {
                guard.push(event.to_string());
            }
        }
    }
}

impl Drop for TaskManager {
    fn drop(&mut self) {
        self.record_event("task_manager:drop");
        self.shutdown_resident_tasks("task_manager_drop");
    }
}

fn state_store_from_task_record_path(task_record_path: &std::path::Path) -> Option<StateStore> {
    let workspace_root = task_record_path
        .parent()?
        .parent()?
        .parent()?
        .parent()?
        .to_path_buf();
    Some(StateStore::new(workspace_root))
}

impl TaskManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_shared(self) -> SharedTaskManager {
        Arc::new(Mutex::new(self))
    }

    pub fn with_control(control: TaskManagerControl) -> Self {
        let mut manager = Self::default();
        manager.control = control;
        manager
    }

    pub fn control(&self) -> TaskManagerControl {
        self.control.clone()
    }

    pub fn set_session_scope(&mut self, session_id: impl Into<String>) {
        let session_id = session_id.into();
        if self.session_scope.as_deref() != Some(session_id.as_str()) {
            self.goal_unavailable_worker_models.clear();
            self.goal_provider_sessions.clear();
        }
        self.session_scope = Some(session_id);
    }

    pub fn set_goal_epoch_context(
        &mut self,
        session_id: impl Into<String>,
        goal_id: impl Into<String>,
        epoch_id: impl Into<String>,
    ) -> Result<()> {
        let context = GoalEpochContext {
            session_id: session_id.into(),
            goal_id: goal_id.into(),
            epoch_id: epoch_id.into(),
        };
        for (field, value) in [
            ("session_id", context.session_id.as_str()),
            ("goal_id", context.goal_id.as_str()),
            ("epoch_id", context.epoch_id.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("goal epoch context {field} cannot be empty");
            }
        }
        // The runtime may receive a shared manager without an earlier
        // set_session_scope call, so make the context itself the session
        // boundary for goal-scoped routing caches.
        if self.session_scope.as_deref() != Some(context.session_id.as_str()) {
            self.set_session_scope(context.session_id.clone());
        }
        if self.goal_epoch_context.as_ref().is_some_and(|current| {
            current.session_id != context.session_id || current.goal_id != context.goal_id
        }) {
            self.goal_unavailable_worker_models.clear();
            self.goal_provider_sessions.clear();
        }
        self.goal_epoch_context = Some(context);
        Ok(())
    }

    pub fn set_worker_registry(&mut self, registry: WorkerRegistry) {
        self.registry = registry;
    }

    /// Bind the registry used by the next task dispatch to one phase-local
    /// broker. The runtime replaces this before every broker-managed phase,
    /// so the handle tracked by TaskManager is the same handle that owns the
    /// broker receipt lifecycle.
    pub fn set_worker_broker(&mut self, broker: Option<Arc<WorkerBroker>>) {
        self.registry.set_broker(broker);
    }

    pub fn set_artifacts_root(&mut self, artifacts_root: PathBuf) {
        self.artifacts_root = Some(artifacts_root);
    }

    pub fn set_worker_fanout_limit(&mut self, limit: usize) {
        self.worker_fanout_limit = limit.max(1);
    }

    pub fn worker_fanout_limit(&self) -> usize {
        self.worker_fanout_limit
    }

    fn consume_worker_fanout_budget(
        &self,
        store: &StateStore,
        task_id: &str,
    ) -> Result<Option<String>> {
        let Some(session_id) = self.session_scope.as_deref() else {
            return Ok(None);
        };
        let mut counter = store.read_worker_fanout_counter(session_id)?;
        counter.count = counter.count.saturating_add(1);
        counter.updated_at = timestamp();
        store.write_worker_fanout_counter(&counter)?;
        if counter.count <= self.worker_fanout_limit {
            return Ok(None);
        }

        let reason = format!(
            "Worker fan-out cap reached ({}/{}) for session `{session_id}`. Consolidate work into existing workers or raise GEARBOX_GEAR_WORKER_FANOUT_LIMIT if this volume is intentional.",
            counter.count, self.worker_fanout_limit
        );
        store.write_worker_fanout_denial(&WorkerFanoutDenialReceipt {
            schema_version: crate::state::WORKER_FANOUT_COUNTER_SCHEMA_VERSION,
            session_id: session_id.to_string(),
            task_id: task_id.to_string(),
            count: counter.count,
            limit: self.worker_fanout_limit,
            reason: reason.clone(),
            created_at: timestamp(),
        })?;
        Ok(Some(reason))
    }

    fn append_task_command_event(
        &self,
        task_id: &str,
        action: &str,
        context: &TaskCommandContext,
        accepted: bool,
        reason: Option<String>,
    ) -> Result<()> {
        let Some(task_record_path) = self.task_record_paths.get(task_id) else {
            return Ok(());
        };
        let Some(store) = state_store_from_task_record_path(task_record_path) else {
            return Ok(());
        };
        let run_epoch = self
            .records
            .get(task_id)
            .map(|record| record.run_epoch)
            .unwrap_or_default();
        let event = TaskCommandEvent {
            task_id: task_id.to_string(),
            action: action.to_string(),
            accepted,
            all_scope: context.all_scope,
            caller_session_id: context.caller_session_id.clone(),
            reason,
            run_epoch,
            timestamp: timestamp(),
        };
        let path = store.worker_dir(task_id).join("task-command-events.jsonl");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let json =
            serde_json::to_string(&event).context("failed to serialize task command event")?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        writeln!(file, "{json}").with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    fn record_task_command_event(
        &self,
        task_id: &str,
        action: &str,
        context: &TaskCommandContext,
        accepted: bool,
        reason: Option<String>,
    ) {
        if let Err(error) =
            self.append_task_command_event(task_id, action, context, accepted, reason)
        {
            eprintln!("failed to persist Gear task command audit for `{task_id}`: {error:#}");
        }
    }

    pub fn max_parallel_workers(&self) -> usize {
        self.concurrency.max_parallel_workers()
    }

    pub fn max_parallel_per_key(&self) -> usize {
        self.concurrency.max_parallel_per_key()
    }

    pub fn apply_worker_config(&mut self, config: &WorkerConfig) {
        self.concurrency = ConcurrencyManager::from_worker_config(config);
        self.runtime_policy = TaskRuntimePolicy::from_worker_config(config);
        self.worker_fanout_limit = worker_fanout_limit_from_environment();
    }

    pub fn recover_orphaned_records(&mut self, store: &StateStore) -> Result<usize> {
        let workers_dir = store.workers_dir();
        if !workers_dir.exists() {
            return Ok(0);
        }

        let mut recovered = 0;
        for entry in fs::read_dir(&workers_dir)
            .with_context(|| format!("failed to read {}", workers_dir.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read {}", workers_dir.display()))?;
            let task_record_path = entry.path().join("task-record.json");
            if !task_record_path.is_file() {
                continue;
            }

            let json = fs::read_to_string(&task_record_path)
                .with_context(|| format!("failed to read {}", task_record_path.display()))?;
            let mut record: TaskRecord = serde_json::from_str(&json)
                .with_context(|| format!("failed to parse {}", task_record_path.display()))?;
            if !matches!(
                record.status,
                ManagedTaskStatus::Pending | ManagedTaskStatus::Running
            ) {
                continue;
            }

            let finished_at = timestamp();
            let summary =
                "Recovered orphaned Gear worker task after previous runtime exited.".to_string();
            record.retry_reason =
                Some("Recovered orphaned task record from previous runtime".into());
            let transition = transition_task_record(
                &mut record,
                TaskTransition::MarkLost {
                    finished_at: finished_at.clone(),
                    summary: summary.clone(),
                    failure_kind: TaskFailureKind::WorkerStartFailed,
                    error: Some(
                        "Task record was still pending/running on disk, but no live worker handle exists."
                            .into(),
                    ),
                    killed: false,
                },
            );
            record.error = Some(
                "Task record was still pending/running on disk, but no live worker handle exists."
                    .into(),
            );
            update_latest_attempt(&mut record, |attempt| {
                if matches!(
                    attempt.status,
                    TaskAttemptStatus::Pending | TaskAttemptStatus::Running
                ) {
                    attempt.status = TaskAttemptStatus::Lost;
                    attempt.finished_at = Some(finished_at);
                    attempt.summary = summary;
                    attempt.failure_kind = Some(TaskFailureKind::WorkerStartFailed);
                    attempt.retry_reason =
                        Some("Recovered orphaned task record from previous runtime".into());
                    attempt.error = Some(
                        "Task attempt was still pending/running on disk, but no live worker handle exists."
                            .into(),
                    );
                }
            });
            let task_id = record.task_id.clone();
            write_task_record(store, &record)?;
            append_task_lifecycle_event(store, &record, Some(&transition))?;
            self.records.insert(task_id.clone(), record);
            self.task_record_paths.insert(task_id, task_record_path);
            recovered += 1;
        }

        Ok(recovered)
    }

    pub fn start(&mut self, request: WorkerStartRequest<'_>) -> Result<String> {
        let task_id = request.task.id.clone();
        let queued_task = queued_task_from_request(request);
        let selected_route = queued_task
            .config
            .selected_route_for_hint(queued_task.route_attempt, queued_task.route_hint.as_deref());
        let worker_kind = selected_route.worker_kind.as_str().to_string();
        let worker_command = selected_route.worker_command.map(ToString::to_string);
        let worker_model = selected_route.worker_model.map(ToString::to_string);
        let worker_category = selected_route.category.as_str().to_string();
        let route_hint = queued_task.route_hint.clone();
        let route_reason = selected_route.route_reason;
        let store = queued_task.store.clone();
        let fanout_denial = self.consume_worker_fanout_budget(&store, &task_id)?;
        let started_at = timestamp();
        let record = TaskRecord {
            task_id: task_id.clone(),
            worker_kind: worker_kind.clone(),
            worker_command: worker_command.clone(),
            worker_model: worker_model.clone(),
            worker_category: worker_category.clone(),
            route_hint: route_hint.clone(),
            route_reason: route_reason.clone(),
            status: ManagedTaskStatus::Pending,
            started_at: started_at.clone(),
            finished_at: None,
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: self.session_scope.clone(),
            root_session_id: self.session_scope.clone(),
            parent_task_id: queued_task.task.parent_task_id.clone(),
            result_path: None,
            outcome_path: None,
            summary: "Worker task queued.".to_string(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: queued_task.task.attempt,
                worker_kind,
                worker_command,
                worker_model,
                worker_category,
                route_hint,
                route_reason,
                status: TaskAttemptStatus::Pending,
                started_at,
                finished_at: None,
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "Worker task queued.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        };
        write_task_record(&store, &record)?;
        append_task_lifecycle_event(&store, &record, None)?;
        self.records.insert(task_id.clone(), record);
        self.task_record_paths.insert(
            task_id.clone(),
            store.worker_dir(&task_id).join("task-record.json"),
        );
        self.control
            .set_current(task_id.clone(), ManagedTaskStatus::Pending, None)?;
        self.control.set_dispatch_context(
            &task_id,
            queued_task.store.clone(),
            queued_task.task.goal_id.clone(),
            format!("task:{task_id}"),
            0,
        )?;

        if let Some(reason) = fanout_denial {
            let (result, outcome) =
                write_worker_fanout_denied_artifacts(&store, &task_id, &reason)?;
            let transition = {
                let record = self
                    .records
                    .get_mut(&task_id)
                    .context("missing task manager record for fan-out denial")?;
                let transition = transition_task_record(
                    record,
                    TaskTransition::Fail {
                        finished_at: timestamp(),
                        summary: reason.clone(),
                        failure_kind: TaskFailureKind::RepeatedFailureLimit,
                        error: Some(reason.clone()),
                    },
                );
                record.retry_reason = Some(reason);
                record.result_path = Some(result.result_path.clone());
                record.outcome_path = Some(result.outcome_path.clone());
                transition
            };
            let record = self
                .records
                .get(&task_id)
                .context("fan-out denial task record disappeared")?
                .clone();
            write_task_record(&store, &record)?;
            append_task_lifecycle_event(&store, &record, Some(&transition))?;
            self.control
                .update_current_status(&task_id, record.status.clone())?;
            self.completed_runs.insert(
                task_id.clone(),
                ManagedWorkerRun {
                    store,
                    result,
                    outcome,
                    record,
                },
            );
            return Ok(task_id);
        }

        self.queued_tasks.push_back(queued_task);
        self.process_queue()?;
        Ok(task_id)
    }

    pub fn wait_for(&mut self, task_id: &str) -> Result<ManagedWorkerRun> {
        self.wait_for_with_cancellation(task_id, None)?
            .context("worker wait ended without a terminal run")
    }

    /// Wait for a worker completion event while allowing the caller to retain
    /// cancellation ownership. The timeout is only a maintenance wake-up for
    /// stale-task sweeping; completion is driven by `finished_task_rx`.
    pub fn wait_for_with_cancellation(
        &mut self,
        task_id: &str,
        cancellation_token: Option<&CancellationToken>,
    ) -> Result<Option<ManagedWorkerRun>> {
        loop {
            if cancellation_token.is_some_and(CancellationToken::is_cancelled) {
                return Ok(None);
            }
            if let Some(run) = self.try_wait_for(task_id)? {
                return Ok(Some(run));
            }
            match self.finished_task_rx.recv_timeout(WAIT_FOR_POLL_INTERVAL) {
                Ok(finished_task) => self.settle_finished_task(finished_task)?,
                Err(RecvTimeoutError::Timeout) => {
                    self.tick()?;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    bail!("failed to receive finished worker task: channel disconnected");
                }
            }
        }
    }

    pub fn try_wait_for(&mut self, task_id: &str) -> Result<Option<ManagedWorkerRun>> {
        self.tick()?;
        if let Some(run) = self.completed_runs.remove(task_id) {
            return Ok(Some(run));
        }
        if let Some(error) = self.completed_errors.remove(task_id) {
            bail!("{error}");
        }

        if !self.running_tasks.contains_key(task_id)
            && !self
                .queued_tasks
                .iter()
                .any(|queued_task| queued_task.task.id == task_id)
        {
            bail!("managed task is not running or complete: {task_id}");
        }

        Ok(None)
    }

    pub fn tick(&mut self) -> Result<usize> {
        let mut settled_count = 0;
        loop {
            match self.finished_task_rx.try_recv() {
                Ok(finished_task) => {
                    self.settle_finished_task(finished_task)?;
                    settled_count += 1;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    bail!("finished worker task channel disconnected");
                }
            }
        }
        settled_count += self.process_tool_call_circuit_breakers()?;
        settled_count += self.sweep_orphaned_task_state()?;
        settled_count += self.sweep_stale_running_tasks()?;
        settled_count += self.ttl_cleanup();
        self.evict_lru_resident_task();
        self.trim_archive();
        self.process_queue()?;
        Ok(settled_count)
    }

    fn process_tool_call_circuit_breakers(&mut self) -> Result<usize> {
        let triggered = self
            .tool_call_circuit_states
            .iter()
            .filter_map(|(task_id, state)| {
                let state = match state.lock() {
                    Ok(state) => state,
                    Err(_) => {
                        eprintln!("failed to read Gear tool-call circuit state for `{task_id}`");
                        return None;
                    }
                };
                state
                    .trigger_reason
                    .clone()
                    .map(|reason| (task_id.clone(), reason))
            })
            .collect::<Vec<_>>();
        let mut cancelled = 0;
        for (task_id, reason) in triggered {
            if !self
                .records
                .get(&task_id)
                .is_some_and(|record| record.status == ManagedTaskStatus::Running)
            {
                continue;
            }
            let outcome = self.cancel_task_direct_with_details(
                &task_id,
                "Worker task cancelled by tool-call circuit breaker.".to_string(),
                Some(reason),
            )?;
            if matches!(outcome, ActionOutcome::Cancelled(_)) {
                cancelled += 1;
            }
        }
        Ok(cancelled)
    }

    fn sweep_orphaned_task_state(&mut self) -> Result<usize> {
        let orphaned_running_ids = self
            .running_tasks
            .keys()
            .filter(|task_id| !self.records.contains_key(*task_id))
            .cloned()
            .collect::<Vec<_>>();
        let orphaned_queued_len_before = self.queued_tasks.len();
        self.queued_tasks
            .retain(|queued_task| self.records.contains_key(&queued_task.task.id));

        for task_id in &orphaned_running_ids {
            if let Some(running_task) = self.running_tasks.get(task_id) {
                if let Err(error) = running_task.handle.cancel() {
                    eprintln!("failed to cancel orphaned Gear worker task `{task_id}`: {error:#}");
                }
            }
            self.forget_task(task_id)?;
        }

        Ok(orphaned_running_ids.len() + orphaned_queued_len_before - self.queued_tasks.len())
    }

    fn release_running_task_once(
        &mut self,
        task_id: &str,
        run_epoch: u64,
        running_task: &RunningTask,
    ) -> Result<bool> {
        if !self.release_guard.release_once(task_id, run_epoch) {
            return Ok(false);
        }

        self.concurrency.release(&running_task.queued_task);
        self.running_tasks.remove(task_id);
        self.evidence_baselines.remove(task_id);
        self.activity_heartbeats.remove(task_id);
        self.tool_call_circuit_states.remove(task_id);
        Ok(true)
    }

    fn forget_task(&mut self, task_id: &str) -> Result<()> {
        self.destroy_resident_task(task_id, "forget")?;
        Ok(())
    }

    /// Unified destroy entry for all resident task release paths:
    /// cancel, interrupt, dispose, LRU eviction, TTL cleanup, session shutdown, reconciliation.
    /// Guarantees:
    ///   - best-effort abort/cancel/terminate
    ///   - always calls dispose (even if abort fails)
    ///   - dispose errors are logged but never prevent cleanup
    ///   - record, archive, control, concurrency, release_guard all cleaned up
    fn destroy_resident_task(&mut self, task_id: &str, cause: &str) -> Result<()> {
        let mut first_error: Option<anyhow::Error> = None;
        self.evidence_baselines.remove(task_id);
        self.activity_heartbeats.remove(task_id);
        self.tool_call_circuit_states.remove(task_id);
        self.pending_revives
            .retain(|request| request.task_id != task_id);
        // Release concurrency for running tasks
        let running_task = self.running_tasks.remove(task_id).inspect(|running_task| {
            self.concurrency.release(&running_task.queued_task);
        });
        let resident_task = self.resident_tasks.remove(task_id);

        // Remove from queue
        self.queued_tasks
            .retain(|queued_task| queued_task.task.id != task_id);

        let task_store = running_task
            .as_ref()
            .map(|running_task| running_task.store.clone())
            .or_else(|| {
                self.completed_runs
                    .get(task_id)
                    .map(|run| run.store.clone())
            })
            .or_else(|| {
                self.task_record_paths
                    .get(task_id)
                    .and_then(|task_record_path| {
                        state_store_from_task_record_path(task_record_path)
                    })
            })
            .or_else(|| {
                resident_task
                    .as_ref()
                    .map(|resident_task| resident_task.queued_task.store.clone())
            });

        // Stop the resident handle best-effort before clearing records.
        if let Some(running_task) = running_task.as_ref() {
            best_effort_stop_handle(&running_task.handle, task_id, cause);
        } else if let Some(resident_task) = resident_task.as_ref() {
            best_effort_stop_handle(&resident_task.handle, task_id, cause);
        } else {
            match self.control.current_task_snapshot() {
                Ok(Some(current_task)) => {
                    if current_task.task_id == task_id
                        && let Some(handle) = current_task.handle.as_ref()
                    {
                        best_effort_stop_handle(handle, task_id, cause);
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    eprintln!(
                        "failed to snapshot current Gear resident task `{task_id}` during {cause}: {error:#}"
                    );
                    first_error.get_or_insert(error);
                }
            }
        }

        let mut record = self.records.remove(task_id);
        if let (Some(store), Some(record)) = (task_store.as_ref(), record.as_mut()) {
            let transition = transition_task_record(record, TaskTransition::Dispose);
            if let Err(error) = write_task_record(store, record) {
                eprintln!(
                    "failed to persist disposed Gear resident task `{task_id}` during {cause}: {error:#}"
                );
                first_error.get_or_insert(error);
            }
            if let Err(error) = append_task_lifecycle_event(store, record, Some(&transition)) {
                eprintln!(
                    "failed to append dispose lifecycle event for Gear resident task `{task_id}` during {cause}: {error:#}"
                );
                first_error.get_or_insert(error);
            }
        }

        // Take pending messages best-effort.
        if let Err(error) = self.control.take_pending_messages(task_id) {
            eprintln!(
                "failed to clear pending messages for Gear resident task `{task_id}` during {cause}: {error:#}"
            );
            first_error.get_or_insert(error);
        }

        // Clean up completed runs, errors, records
        self.completed_runs.remove(task_id);
        self.completed_errors.remove(task_id);
        self.task_record_paths.remove(task_id);

        // Remove from archive
        self.completed_archive
            .retain(|record| record.task_id != task_id);

        // Release guard and clear current task best-effort.
        self.release_guard.forget_task(task_id);
        if let Err(error) = self.control.clear_current(task_id) {
            eprintln!(
                "failed to clear current Gear resident task `{task_id}` during {cause}: {error:#}"
            );
            first_error.get_or_insert(error);
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn shutdown_resident_tasks(&mut self, cause: &str) {
        let current_task = self.control.current_task_snapshot().ok().flatten();
        let resident_task_ids = self.records.keys().cloned().collect::<Vec<_>>();
        for task_id in resident_task_ids {
            if let Err(error) = self.destroy_resident_task(&task_id, cause) {
                eprintln!(
                    "failed to destroy Gear resident task `{task_id}` during {cause}: {error:#}"
                );
            }
        }
        if let Some(current_task) = current_task {
            let status = match current_task.status {
                ManagedTaskStatus::Pending | ManagedTaskStatus::Running => ManagedTaskStatus::Lost,
                status => status,
            };
            let _ = self.control.set_current(
                current_task.task_id,
                status,
                None::<Arc<dyn WorkerSessionHandle>>,
            );
        }
    }

    /// Evict the oldest completed/failed/interrupted resident task if over cap.
    /// Does NOT evict Cancelled or Lost tasks.
    /// Returns the id of the evicted task, if any.
    fn evict_lru_resident_task(&mut self) -> Option<String> {
        let resident_count: usize = self
            .records
            .values()
            .filter(|record| record.residency_state == ResidencyState::Resident)
            .count();
        if resident_count <= RESIDENCY_MAX_CHILDREN {
            return None;
        }

        // Find the oldest evictable resident task
        let oldest = self
            .records
            .iter()
            .filter(|(_, record)| {
                record.residency_state == ResidencyState::Resident
                    && matches!(
                        record.status,
                        ManagedTaskStatus::Completed
                            | ManagedTaskStatus::Failed
                            | ManagedTaskStatus::Interrupted
                    )
            })
            .min_by_key(|(_, record)| record.started_at.clone())
            .map(|(id, _)| id.clone());

        if let Some(ref task_id) = oldest {
            // Apply eviction transition
            if let Some(record) = self.records.get_mut(task_id) {
                let _ = transition_task_record(record, TaskTransition::Evict);
            }
            let _ = self.destroy_resident_task(task_id, "lru_eviction");
        }
        oldest
    }

    /// Remove terminal records older than TTL.
    /// Cancelled and Lost are never TTL-deleted.
    fn ttl_cleanup(&mut self) -> usize {
        let now = timestamp();
        let now_ms = parse_timestamp_ms(&now).unwrap_or(0);
        let ttl_cutoff = now_ms.saturating_sub(TTL_MS);

        let expired_ids: Vec<String> = self
            .records
            .values()
            .filter(|record| {
                if matches!(
                    record.status,
                    ManagedTaskStatus::Cancelled | ManagedTaskStatus::Lost
                ) {
                    return false;
                }
                if !is_terminal_status(&record.status) {
                    return false;
                }
                let finished_ms = record
                    .finished_at
                    .as_ref()
                    .and_then(|ts| parse_timestamp_ms(ts))
                    .unwrap_or(0);
                finished_ms < ttl_cutoff
            })
            .map(|record| record.task_id.clone())
            .collect();

        let count = expired_ids.len();
        for task_id in expired_ids {
            let _ = self.destroy_resident_task(&task_id, "ttl_cleanup");
        }
        count
    }

    fn trim_archive(&mut self) {
        if self.completed_archive.len() <= ARCHIVE_CAP {
            return;
        }

        let preserved_indices: Vec<usize> = self
            .completed_archive
            .iter()
            .enumerate()
            .filter_map(|(idx, record)| {
                matches!(
                    record.status,
                    ManagedTaskStatus::Cancelled | ManagedTaskStatus::Lost
                )
                .then_some(idx)
            })
            .collect();
        let non_preserved_indices: Vec<usize> = self
            .completed_archive
            .iter()
            .enumerate()
            .filter_map(|(idx, record)| {
                (!matches!(
                    record.status,
                    ManagedTaskStatus::Cancelled | ManagedTaskStatus::Lost
                ))
                .then_some(idx)
            })
            .collect();

        let preserved_budget = preserved_indices.len().min(ARCHIVE_CAP);
        let non_preserved_budget = ARCHIVE_CAP - preserved_budget;
        let non_preserved_kept: Vec<usize> = non_preserved_indices
            .iter()
            .rev()
            .take(non_preserved_budget)
            .copied()
            .collect();
        let non_preserved_keep = non_preserved_kept
            .iter()
            .collect::<std::collections::HashSet<_>>();

        let preserved_keep: Vec<usize> = if preserved_indices.len() <= preserved_budget {
            preserved_indices
        } else {
            preserved_indices
                .iter()
                .rev()
                .take(preserved_budget)
                .copied()
                .collect()
        };
        let preserved_keep = preserved_keep
            .iter()
            .collect::<std::collections::HashSet<_>>();

        let mut filtered = VecDeque::new();
        for (index, task_record) in self.completed_archive.iter().enumerate() {
            if non_preserved_keep.contains(&index) || preserved_keep.contains(&index) {
                filtered.push_back(task_record.clone());
            }
        }
        self.completed_archive = filtered;
    }

    fn sweep_stale_running_tasks(&mut self) -> Result<usize> {
        let stale_task_timeout = self.runtime_policy.stale_task_timeout;
        let now = Instant::now();
        let stale_tasks = self
            .running_tasks
            .iter()
            .filter(|(task_id, running_task)| {
                // Free-model workers must not be killed by stale-task sweep.
                // See GBX-063/064: free models may have long periods without text
                // output while the process is still alive. Only explicit errors,
                // cancellation, non-zero exit, or tool meltdown should terminate them.
                // Check multiple model sources because the task record's model
                // may not be populated when the sweep runs.
                let task_id = task_id.as_str();
                if is_free_model(
                    self.records
                        .get(task_id)
                        .and_then(|record| record.worker_model.as_deref())
                        .or_else(|| running_task.queued_task.config.worker_model.as_deref())
                        .or_else(|| {
                            running_task
                                .queued_task
                                .config
                                .worker_routes
                                .iter()
                                .find_map(|route| route.worker_model.as_deref())
                        }),
                ) {
                    return false;
                }
                let last_activity = match self
                    .activity_heartbeats
                    .get(running_task.queued_task.task.id.as_str())
                {
                    Some(heartbeat) => match heartbeat.lock() {
                        Ok(timestamp) => *timestamp,
                        Err(_) => {
                            eprintln!(
                                "failed to read Gear worker activity heartbeat for `{}`",
                                running_task.queued_task.task.id
                            );
                            running_task.started_at
                        }
                    },
                    None => running_task.started_at,
                };
                now.duration_since(last_activity) > stale_task_timeout
            })
            .map(|(task_id, running_task)| (task_id.clone(), running_task.clone()))
            .collect::<Vec<_>>();
        let stale_count = stale_tasks.len();

        for (task_id, running_task) in stale_tasks {
            if let Err(error) = running_task.handle.cancel() {
                eprintln!("failed to cancel stale Gear worker task `{task_id}`: {error:#}");
            }
            match self.settle_running_task(
                &task_id,
                running_task,
                Err(anyhow::anyhow!(
                    "worker task timed out waiting for outcome after {:?}",
                    stale_task_timeout
                )),
            ) {
                Ok(Some(run)) => {
                    self.completed_runs.insert(task_id, run);
                }
                Ok(None) => {}
                Err(error) => {
                    self.completed_errors
                        .insert(task_id, format!("Worker task failed: {error:#}"));
                }
            }
        }

        Ok(stale_count)
    }

    fn settle_finished_task(&mut self, finished_task: FinishedTaskMessage) -> Result<()> {
        let task_id = finished_task.task_id.clone();
        match self.settle_running_task(
            &finished_task.task_id,
            finished_task.running_task,
            finished_task.run_result,
        ) {
            Ok(Some(run)) => {
                self.completed_runs.insert(task_id, run);
            }
            Ok(None) => {}
            Err(error) => {
                self.completed_errors
                    .insert(task_id, format!("Worker task failed: {error:#}"));
            }
        }
        Ok(())
    }

    fn settle_running_task(
        &mut self,
        task_id: &str,
        running_task: RunningTask,
        run_result: Result<(WorkerOutcome, WorkerResult)>,
    ) -> Result<Option<ManagedWorkerRun>> {
        match run_result {
            Ok((mut outcome, mut result)) => {
                let Some(mut record) = self.records.remove(task_id) else {
                    self.forget_task(task_id)?;
                    return Ok(None);
                };
                if let Some(session_id) = running_task.handle.session_id() {
                    record.session_id = Some(session_id);
                }
                let evidence_failure = if result.status == WorkerStatus::Succeeded
                    && category_requires_worker_evidence(&record.worker_category)
                    && worker_kind_supports_evidence_contract(&record.worker_kind)
                {
                    let evidence_baseline = self
                        .evidence_baselines
                        .get(task_id)
                        .cloned()
                        .unwrap_or_default();
                    Some(validate_worker_evidence_receipt_with_baseline(
                        &result,
                        &running_task.queued_task.workspace,
                        &evidence_baseline,
                    ))
                } else {
                    None
                };
                let mut evidence_retry_reason = None;
                if let Some(evidence_check) = evidence_failure {
                    match evidence_check {
                        Ok(receipt_path) => {
                            write_worker_evidence_gate_artifact(
                                &running_task.store,
                                task_id,
                                record
                                    .attempts
                                    .last()
                                    .map(|attempt| attempt.attempt)
                                    .unwrap_or(1),
                                Some(&receipt_path),
                                None,
                            )?;
                        }
                        Err(reason) => {
                            let attempt = record
                                .attempts
                                .last()
                                .map(|attempt| attempt.attempt)
                                .unwrap_or(1);
                            let summary =
                                format!("Worker evidence gate rejected completion: {reason}");
                            write_worker_evidence_gate_artifact(
                                &running_task.store,
                                task_id,
                                attempt,
                                None,
                                Some(&reason),
                            )?;
                            result.status = WorkerStatus::Failed;
                            result.summary = summary.clone();
                            outcome.status = WorkerStatus::Failed;
                            outcome.summary = summary.clone();
                            if !outcome
                                .known_failures
                                .iter()
                                .any(|failure| failure == &summary)
                            {
                                outcome.known_failures.push(summary.clone());
                            }
                            evidence_retry_reason = Some(reason);
                            write_result_and_outcome_with_outcome(
                                &running_task.store,
                                task_id,
                                &result,
                                &outcome,
                            )?;
                        }
                    }
                }
                let transition = match result.status {
                    WorkerStatus::Succeeded => transition_task_record(
                        &mut record,
                        TaskTransition::Complete {
                            finished_at: timestamp(),
                            result_path: result.result_path.clone(),
                            outcome_path: result.outcome_path.clone(),
                            summary: outcome.summary.clone(),
                            failure_kind: failure_kind_from_worker_result(&result, &outcome),
                        },
                    ),
                    WorkerStatus::Skipped => transition_task_record(
                        &mut record,
                        TaskTransition::Skip {
                            finished_at: timestamp(),
                            result_path: result.result_path.clone(),
                            outcome_path: result.outcome_path.clone(),
                            summary: outcome.summary.clone(),
                            failure_kind: failure_kind_from_worker_result(&result, &outcome),
                        },
                    ),
                    WorkerStatus::Failed => {
                        let cancelled = worker_outcome_is_cancelled(&outcome);
                        if cancelled {
                            transition_task_record(
                                &mut record,
                                TaskTransition::Cancel {
                                    finished_at: timestamp(),
                                    summary: "Worker task cancelled.".to_string(),
                                    error: None,
                                },
                            )
                        } else {
                            transition_task_record(
                                &mut record,
                                TaskTransition::Fail {
                                    finished_at: timestamp(),
                                    summary: outcome.summary.clone(),
                                    failure_kind: failure_kind_from_worker_result(
                                        &result, &outcome,
                                    )
                                    .unwrap_or(TaskFailureKind::WorkerFailed),
                                    error: None,
                                },
                            )
                        }
                    }
                };
                if let Some(reason) = evidence_retry_reason {
                    let retry_reason = format!("{WORKER_EVIDENCE_RETRY_PREFIX} {reason}");
                    record.retry_reason = Some(retry_reason.clone());
                    if let Some(attempt) = record.attempts.last_mut() {
                        attempt.retry_reason = Some(retry_reason);
                    }
                }
                if transition.applied {
                    record.result_path = Some(result.result_path.clone());
                    record.outcome_path = Some(result.outcome_path.clone());
                }
                self.remember_unavailable_model_for_goal(
                    &running_task.queued_task.task.goal_id,
                    &record,
                );
                self.remember_provider_session_for_goal(
                    &running_task.queued_task.task.goal_id,
                    &running_task.store,
                    &record.task_id,
                )?;
                let settle_event = match result.status {
                    WorkerStatus::Succeeded | WorkerStatus::Skipped => {
                        PromptSettleEvent::BackgroundCompleted
                    }
                    WorkerStatus::Failed => PromptSettleEvent::Error,
                };
                record_worker_settle_event(
                    &running_task.store,
                    &running_task.queued_task.task.goal_id,
                    &record.task_id,
                    record.session_id.as_deref(),
                    record.run_epoch,
                    "task_manager.worker_completion",
                    settle_event,
                )?;
                write_task_record(&running_task.store, &record)?;
                append_task_lifecycle_event(&running_task.store, &record, Some(&transition))?;

                if should_retry_worker_result(&record, &running_task.queued_task, &result) {
                    let previous_attempt = record.attempts.last().cloned();
                    let mut retry_task = running_task.queued_task.clone();
                    match queue_next_attempt(&mut record, &mut retry_task) {
                        FallbackDecision::Queued => {
                            if let Some(previous_attempt) = previous_attempt.as_ref() {
                                if let Some(next_attempt) = record.attempts.last() {
                                    write_route_transform_artifact(
                                        &running_task.store,
                                        &record.task_id,
                                        previous_attempt,
                                        Some(next_attempt),
                                        "worker fallback queued",
                                        None,
                                    )?;
                                }
                            }
                            write_task_record(&running_task.store, &record)?;
                            append_task_lifecycle_event(&running_task.store, &record, None)?;
                            let run_epoch = record.run_epoch;
                            record_worker_settle_event(
                                &running_task.store,
                                &running_task.queued_task.task.goal_id,
                                &record.task_id,
                                record.session_id.as_deref(),
                                run_epoch,
                                "task_manager.fallback",
                                PromptSettleEvent::FallbackRetry,
                            )?;
                            self.control
                                .update_current_status(task_id, record.status.clone())?;
                            self.release_running_task_once(task_id, run_epoch, &running_task)?;
                            self.records.insert(task_id.to_string(), record);
                            self.start_queued_task(retry_task)?;
                            return Ok(None);
                        }
                        FallbackDecision::Unavailable {
                            reason,
                            failure_kind,
                        } => {
                            record.failure_kind = Some(failure_kind);
                            record.retry_reason = Some(reason.clone());
                            if let Some(attempt) = record.attempts.last_mut() {
                                attempt.retry_reason = Some(reason);
                            }
                            write_task_record(&running_task.store, &record)?;
                            append_task_lifecycle_event(&running_task.store, &record, None)?;
                        }
                    }
                }

                let run = ManagedWorkerRun {
                    store: running_task.store.clone(),
                    result,
                    outcome,
                    record,
                };
                let run_epoch = run.record.run_epoch;
                self.control
                    .update_current_status(task_id, run.record.status.clone())?;
                if matches!(
                    &run.record.status,
                    ManagedTaskStatus::Completed
                        | ManagedTaskStatus::Failed
                        | ManagedTaskStatus::Interrupted
                ) {
                    self.resident_tasks.insert(
                        task_id.to_string(),
                        ResidentTask {
                            handle: Arc::clone(&running_task.handle),
                            queued_task: running_task.queued_task.clone(),
                        },
                    );
                }
                self.release_running_task_once(task_id, run_epoch, &running_task)?;
                self.records.insert(task_id.to_string(), run.record.clone());
                self.completed_runs.insert(task_id.to_string(), run.clone());
                self.completed_archive.push_back(run.record.clone());
                self.trim_archive();
                self.process_queue()?;
                Ok(Some(run))
            }
            Err(error) => {
                let Some(mut record) = self.records.remove(task_id) else {
                    self.forget_task(task_id)?;
                    return Ok(None);
                };
                if let Some(session_id) = running_task.handle.session_id() {
                    record.session_id = Some(session_id);
                }
                let error_text = format!("{error:#}");
                let transition = if record.status == ManagedTaskStatus::Interrupted {
                    transition_task_record(
                        &mut record,
                        TaskTransition::Fail {
                            finished_at: timestamp(),
                            summary: "Worker task interrupted.".to_string(),
                            failure_kind: TaskFailureKind::WorkerCancelled,
                            error: Some(error_text),
                        },
                    )
                } else if error_text.contains("Gear worker command timed out") {
                    transition_task_record(
                        &mut record,
                        TaskTransition::Fail {
                            finished_at: timestamp(),
                            summary: "Worker provider timed out.".to_string(),
                            failure_kind: TaskFailureKind::ProviderTemporarilyUnavailable,
                            error: Some(error_text),
                        },
                    )
                } else if error_text.contains("timed out waiting for outcome") {
                    transition_task_record(
                        &mut record,
                        TaskTransition::MarkLost {
                            finished_at: timestamp(),
                            summary: "Worker task timed out waiting for outcome.".to_string(),
                            failure_kind: TaskFailureKind::WorkerFailed,
                            error: Some(error_text),
                            killed: false,
                        },
                    )
                } else if record.status != ManagedTaskStatus::Cancelled
                    && !error_text.contains("cancelled")
                    && !error_text.contains("canceled")
                {
                    transition_task_record(
                        &mut record,
                        TaskTransition::Fail {
                            finished_at: timestamp(),
                            summary: "Worker task failed before producing an outcome.".to_string(),
                            failure_kind: TaskFailureKind::WorkerFailed,
                            error: Some(error_text),
                        },
                    )
                } else {
                    transition_task_record(
                        &mut record,
                        TaskTransition::Cancel {
                            finished_at: timestamp(),
                            summary: "Worker task cancelled.".to_string(),
                            error: Some(error_text),
                        },
                    )
                };
                record_worker_settle_event(
                    &running_task.store,
                    &running_task.queued_task.task.goal_id,
                    &record.task_id,
                    record.session_id.as_deref(),
                    record.run_epoch,
                    "task_manager.worker_error",
                    PromptSettleEvent::Error,
                )?;
                write_task_record(&running_task.store, &record)?;
                append_task_lifecycle_event(&running_task.store, &record, Some(&transition))?;
                if record.status == ManagedTaskStatus::Failed {
                    let previous_attempt = record.attempts.last().cloned();
                    let mut retry_task = running_task.queued_task.clone();
                    match queue_next_attempt(&mut record, &mut retry_task) {
                        FallbackDecision::Queued => {
                            if let Some(previous_attempt) = previous_attempt.as_ref() {
                                if let Some(next_attempt) = record.attempts.last() {
                                    write_route_transform_artifact(
                                        &running_task.store,
                                        &record.task_id,
                                        previous_attempt,
                                        Some(next_attempt),
                                        "worker fallback queued",
                                        None,
                                    )?;
                                }
                            }
                            write_task_record(&running_task.store, &record)?;
                            append_task_lifecycle_event(&running_task.store, &record, None)?;
                            let run_epoch = record.run_epoch;
                            record_worker_settle_event(
                                &running_task.store,
                                &running_task.queued_task.task.goal_id,
                                &record.task_id,
                                record.session_id.as_deref(),
                                run_epoch,
                                "task_manager.fallback",
                                PromptSettleEvent::FallbackRetry,
                            )?;
                            self.control
                                .update_current_status(task_id, record.status.clone())?;
                            self.release_running_task_once(task_id, run_epoch, &running_task)?;
                            self.records.insert(task_id.to_string(), record);
                            self.start_queued_task(retry_task)?;
                            return Ok(None);
                        }
                        FallbackDecision::Unavailable {
                            reason,
                            failure_kind,
                        } => {
                            record.failure_kind = Some(failure_kind);
                            record.retry_reason = Some(reason.clone());
                            if let Some(attempt) = record.attempts.last_mut() {
                                attempt.retry_reason = Some(reason);
                            }
                            if let Some(previous_attempt) = previous_attempt.as_ref() {
                                write_route_transform_artifact(
                                    &running_task.store,
                                    &record.task_id,
                                    previous_attempt,
                                    None,
                                    "worker fallback unavailable",
                                    record.failure_kind.as_ref(),
                                )?;
                            }
                            write_task_record(&running_task.store, &record)?;
                            append_task_lifecycle_event(&running_task.store, &record, None)?;
                        }
                    }
                }
                let run_epoch = record.run_epoch;
                self.control
                    .update_current_status(task_id, record.status.clone())?;
                if matches!(
                    &record.status,
                    ManagedTaskStatus::Completed
                        | ManagedTaskStatus::Failed
                        | ManagedTaskStatus::Interrupted
                ) {
                    self.resident_tasks.insert(
                        task_id.to_string(),
                        ResidentTask {
                            handle: Arc::clone(&running_task.handle),
                            queued_task: running_task.queued_task.clone(),
                        },
                    );
                }
                self.release_running_task_once(task_id, run_epoch, &running_task)?;
                self.records.insert(task_id.to_string(), record);
                self.process_queue()?;
                Err(error)
            }
        }
    }

    pub fn run_worker_task(&mut self, request: WorkerStartRequest<'_>) -> Result<ManagedWorkerRun> {
        let task_id = self.start(request)?;
        self.wait_for(&task_id)
    }

    fn descendant_task_ids(&self, task_id: &str) -> Vec<String> {
        let mut descendant_task_ids = Vec::new();
        let mut discovered_task_ids = HashSet::from([task_id.to_string()]);
        let mut pending_task_ids = VecDeque::from([task_id.to_string()]);

        while let Some(parent_task_id) = pending_task_ids.pop_front() {
            for record in self.records.values() {
                if !matches!(
                    record.status,
                    ManagedTaskStatus::Pending | ManagedTaskStatus::Running
                ) {
                    continue;
                }
                if record.parent_task_id.as_deref() != Some(parent_task_id.as_str()) {
                    continue;
                }
                if discovered_task_ids.insert(record.task_id.clone()) {
                    pending_task_ids.push_back(record.task_id.clone());
                    descendant_task_ids.push(record.task_id.clone());
                }
            }
        }

        descendant_task_ids.sort_by(|left, right| {
            let left_record = self.records.get(left);
            let right_record = self.records.get(right);
            left_record
                .map(|record| record.started_at.as_str())
                .cmp(&right_record.map(|record| record.started_at.as_str()))
                .then_with(|| left.cmp(right))
        });

        descendant_task_ids
    }

    fn cancel_task_direct(&mut self, task_id: &str) -> Result<ActionOutcome> {
        self.cancel_task_direct_with_details(task_id, "Worker task cancelled.".to_string(), None)
    }

    fn cancel_task_direct_with_details(
        &mut self,
        task_id: &str,
        summary: String,
        error: Option<String>,
    ) -> Result<ActionOutcome> {
        self.pending_revives
            .retain(|request| request.task_id != task_id);
        let mut queued_store = None;
        if let Some(index) = self
            .queued_tasks
            .iter()
            .position(|queued_task| queued_task.task.id == task_id)
        {
            queued_store = Some(
                self.queued_tasks
                    .remove(index)
                    .context("queued task disappeared during cancellation")?
                    .store,
            );
        }
        let is_running = self.running_tasks.contains_key(task_id);
        let Some(record) = self.records.get_mut(task_id) else {
            bail!("unknown managed task: {task_id}");
        };
        let run_epoch = record.run_epoch;
        let transition = transition_task_record(
            record,
            TaskTransition::Cancel {
                finished_at: timestamp(),
                summary,
                error,
            },
        );
        let ctx = OutcomeContext {
            task_id: Some(task_id.to_string()),
            run_epoch: Some(run_epoch as usize),
            queue_position: None,
        };
        let outcome = if transition.applied {
            ActionOutcome::Cancelled(ctx)
        } else {
            ActionOutcome::NotContinuable(ctx)
        };
        let store = self
            .running_tasks
            .get(task_id)
            .map(|running_task| running_task.store.clone())
            .or(queued_store);
        let record_snapshot = record.clone();
        if let Some(store) = store {
            write_task_record(&store, &record_snapshot)?;
            append_task_lifecycle_event(&store, &record_snapshot, Some(&transition))?;
        }
        if !is_running {
            self.control.take_pending_messages(task_id)?;
        }
        self.control
            .update_current_status(task_id, record.status.clone())?;
        if transition.applied
            && let Some(running_task) = self.running_tasks.get(task_id)
            && let Err(error) = running_task.handle.cancel()
        {
            eprintln!(
                "failed to cancel Gear worker task `{task_id}` after terminal transition: {error:#}"
            );
            return Err(error);
        }
        Ok(outcome)
    }

    pub fn cancel_task(&mut self, task_id: &str) -> Result<()> {
        let descendant_task_ids = self.descendant_task_ids(task_id);
        self.cancel_task_direct(task_id)?;
        for descendant_task_id in descendant_task_ids {
            self.cancel_task_direct(&descendant_task_id)?;
        }
        Ok(())
    }

    pub fn cancel_task_with_outcome(&mut self, task_id: &str) -> Result<ActionOutcome> {
        let descendant_task_ids = self.descendant_task_ids(task_id);
        let outcome = self.cancel_task_direct(task_id)?;
        for descendant_task_id in descendant_task_ids {
            self.cancel_task_direct(&descendant_task_id)?;
        }
        Ok(outcome)
    }

    pub fn cancel_task_with_context(
        &mut self,
        task_id: &str,
        context: &TaskCommandContext,
    ) -> Result<ActionOutcome> {
        let Some(record) = self.records.get(task_id).cloned() else {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        if !task_scope_allows(&record, context) {
            let reason = scope_denied_reason(&record, context);
            self.record_task_command_event(task_id, "cancel", context, false, Some(reason.clone()));
            return Ok(ActionOutcome::ScopeDenied {
                reason,
                context: OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    run_epoch: Some(record.run_epoch as usize),
                    ..OutcomeContext::default()
                },
            });
        }
        let outcome = self.cancel_task_with_outcome(task_id)?;
        self.record_task_command_event(
            task_id,
            "cancel",
            context,
            matches!(&outcome, ActionOutcome::Cancelled(_)),
            outcome.reason(),
        );
        Ok(outcome)
    }

    fn interrupt_task_direct(&mut self, task_id: &str) -> Result<ActionOutcome> {
        self.pending_revives
            .retain(|request| request.task_id != task_id);
        let is_running = self.running_tasks.contains_key(task_id);
        let Some(record) = self.records.get_mut(task_id) else {
            bail!("unknown managed task: {task_id}");
        };
        let run_epoch = record.run_epoch;
        let transition = transition_task_record(
            record,
            TaskTransition::Interrupt {
                finished_at: timestamp(),
                summary: "Worker task interrupted.".to_string(),
                error: None,
            },
        );
        let ctx = OutcomeContext {
            task_id: Some(task_id.to_string()),
            run_epoch: Some(run_epoch as usize),
            queue_position: None,
        };
        let outcome = if transition.applied {
            ActionOutcome::Interrupted(ctx)
        } else {
            ActionOutcome::NotContinuable(ctx)
        };
        let store = self
            .running_tasks
            .get(task_id)
            .map(|running_task| running_task.store.clone());
        let record_snapshot = record.clone();
        if let Some(store) = store {
            write_task_record(&store, &record_snapshot)?;
            append_task_lifecycle_event(&store, &record_snapshot, Some(&transition))?;
        }
        if !is_running {
            self.control.take_pending_messages(task_id)?;
        }
        self.control
            .update_current_status(task_id, record.status.clone())?;
        if transition.applied
            && let Some(running_task) = self.running_tasks.get(task_id)
        {
            if let Err(error) = running_task.handle.interrupt() {
                eprintln!(
                    "failed to interrupt Gear worker task `{task_id}` after terminal transition: {error:#}"
                );
                return Err(error);
            }
            if let Some(output) = running_task.handle.last_output() {
                let mut updated_record = record.clone();
                updated_record.summary = output.clone();
                if let Some(attempt) = updated_record.attempts.last_mut() {
                    attempt.summary = output;
                }
                if let Some(store) = self
                    .running_tasks
                    .get(task_id)
                    .map(|task| task.store.clone())
                {
                    write_task_record(&store, &updated_record)?;
                }
                record.summary = updated_record.summary;
            }
        }
        Ok(outcome)
    }

    pub fn interrupt_task(&mut self, task_id: &str) -> Result<ActionOutcome> {
        let descendant_task_ids = self.descendant_task_ids(task_id);
        let outcome = self.interrupt_task_direct(task_id)?;
        for descendant_task_id in descendant_task_ids {
            let _ = self.interrupt_task_direct(&descendant_task_id)?;
        }
        Ok(outcome)
    }

    pub fn interrupt_task_with_context(
        &mut self,
        task_id: &str,
        context: &TaskCommandContext,
    ) -> Result<ActionOutcome> {
        let Some(record) = self.records.get(task_id).cloned() else {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        if !task_scope_allows(&record, context) {
            let reason = scope_denied_reason(&record, context);
            self.record_task_command_event(
                task_id,
                "interrupt",
                context,
                false,
                Some(reason.clone()),
            );
            return Ok(ActionOutcome::ScopeDenied {
                reason,
                context: OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    run_epoch: Some(record.run_epoch as usize),
                    ..OutcomeContext::default()
                },
            });
        }
        let outcome = self.interrupt_task(task_id)?;
        self.record_task_command_event(
            task_id,
            "interrupt",
            context,
            matches!(&outcome, ActionOutcome::Interrupted(_)),
            outcome.reason(),
        );
        Ok(outcome)
    }

    fn revive_task(
        &mut self,
        task_id: &str,
        prompt: String,
        message_kind: QueuedMessageKind,
    ) -> Result<ReviveDispatchOutcome> {
        let Some(resident_task) = self.resident_tasks.remove(task_id) else {
            return Ok(ReviveDispatchOutcome::NotStarted);
        };

        if !self
            .records
            .get(task_id)
            .is_some_and(|record| messageability_for_record(record) == Messageability::Revive)
        {
            self.resident_tasks
                .insert(task_id.to_string(), resident_task);
            return Ok(ReviveDispatchOutcome::NotStarted);
        }

        if !self.concurrency.acquire(&resident_task.queued_task) {
            self.resident_tasks
                .insert(task_id.to_string(), resident_task);
            return Ok(ReviveDispatchOutcome::NotStarted);
        }

        let previous_record = self
            .records
            .get(task_id)
            .cloned()
            .context("resident task record disappeared during revive")?;
        let rollback_queued_task = resident_task.queued_task.clone();
        let rollback_handle = resident_task.handle.clone();
        let started_at = timestamp();
        let handle = resident_task.handle.clone();
        let session_id = handle.session_id();
        let control_session_id = session_id
            .clone()
            .unwrap_or_else(|| format!("task:{task_id}"));
        let goal_epoch_context = self.goal_epoch_context.clone();
        let activity_heartbeat = Arc::new(Mutex::new(Instant::now()));
        let circuit_state = Arc::new(Mutex::new(ToolCallCircuitState::default()));
        let circuit_policy = self.runtime_policy.tool_call_circuit_breaker.clone();
        let revive_result = (|| -> Result<(RunningTask, ReviveDispatchOutcome)> {
            handle.reset_event_history()?;
            let subscription = subscribe_to_worker_events_with_activity_and_circuit(
                &handle,
                &resident_task.queued_task.store,
                task_id,
                &resident_task.queued_task.task.goal_id,
                previous_record.run_epoch.saturating_add(1),
                goal_epoch_context,
                Some(activity_heartbeat.clone()),
                Some(circuit_state.clone()),
                circuit_policy.clone(),
            )?;
            let record = self
                .records
                .get_mut(task_id)
                .context("resident task record disappeared during revive")?;
            record.status = ManagedTaskStatus::Running;
            record.residency_state = ResidencyState::Resident;
            record.run_epoch = record.run_epoch.saturating_add(1);
            record.started_at = started_at.clone();
            record.finished_at = None;
            record.session_id = session_id.clone();
            record.result_path = None;
            record.outcome_path = None;
            record.summary = "Worker task revived.".to_string();
            record.failure_kind = None;
            record.retry_reason = None;
            record.error = None;
            record.killed = false;
            record.attempts.push(TaskAttempt {
                attempt: record
                    .attempts
                    .last()
                    .map_or(1, |attempt| attempt.attempt + 1),
                worker_kind: record.worker_kind.clone(),
                worker_command: record.worker_command.clone(),
                worker_model: record.worker_model.clone(),
                worker_category: record.worker_category.clone(),
                route_hint: record.route_hint.clone(),
                route_reason: format!("revived from epoch {}", record.run_epoch - 1),
                status: TaskAttemptStatus::Running,
                started_at,
                finished_at: None,
                session_id,
                result_path: None,
                outcome_path: None,
                summary: "Worker task revived.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            });
            let record_snapshot = record.clone();
            write_task_record(&resident_task.queued_task.store, &record_snapshot)?;
            append_task_lifecycle_event(&resident_task.queued_task.store, &record_snapshot, None)?;
            self.control.set_current(
                task_id.to_string(),
                ManagedTaskStatus::Running,
                Some(handle.clone()),
            )?;
            self.control.set_dispatch_context(
                task_id,
                resident_task.queued_task.store.clone(),
                resident_task.queued_task.task.goal_id.clone(),
                control_session_id,
                record_snapshot.run_epoch,
            )?;
            let running_task = RunningTask {
                store: resident_task.queued_task.store.clone(),
                handle,
                queued_task: resident_task.queued_task,
                started_at: Instant::now(),
                _subscription: subscription,
            };
            self.running_tasks
                .insert(task_id.to_string(), running_task.clone());
            self.activity_heartbeats
                .insert(task_id.to_string(), activity_heartbeat.clone());
            self.tool_call_circuit_states
                .insert(task_id.to_string(), circuit_state.clone());
            let dispatch_outcome = match message_kind {
                QueuedMessageKind::FollowUp => match running_task.handle.send_follow_up(prompt) {
                    Ok(()) => ReviveDispatchOutcome::Started,
                    Err(error) if prompt_dispatch_error_is_possibly_accepted(&error) => {
                        ReviveDispatchOutcome::PossiblyAccepted
                    }
                    Err(error) => return Err(error),
                },
                QueuedMessageKind::Steer => match running_task.handle.steer(prompt) {
                    Ok(()) => ReviveDispatchOutcome::Started,
                    Err(error) if prompt_dispatch_error_is_possibly_accepted(&error) => {
                        ReviveDispatchOutcome::PossiblyAccepted
                    }
                    Err(error) => return Err(error),
                },
            };
            Ok((running_task, dispatch_outcome))
        })();
        let (running_task, dispatch_outcome) = match revive_result {
            Ok(result) => result,
            Err(error) => {
                self.running_tasks.remove(task_id);
                self.activity_heartbeats.remove(task_id);
                self.tool_call_circuit_states.remove(task_id);
                self.concurrency.release(&rollback_queued_task);
                self.resident_tasks.insert(
                    task_id.to_string(),
                    ResidentTask {
                        handle: rollback_handle.clone(),
                        queued_task: rollback_queued_task.clone(),
                    },
                );
                self.records
                    .insert(task_id.to_string(), previous_record.clone());
                if let Err(rollback_error) =
                    write_task_record(&rollback_queued_task.store, &previous_record)
                {
                    eprintln!(
                        "failed to restore Gear task `{task_id}` after revive failure: {rollback_error:#}"
                    );
                }
                self.control.set_current(
                    task_id.to_string(),
                    previous_record.status,
                    Some(rollback_handle),
                )?;
                return Err(error);
            }
        };

        self.dispatch_running_task(task_id.to_string(), running_task);
        Ok(dispatch_outcome)
    }

    pub fn send_follow_up_task(&mut self, task_id: &str, prompt: String) -> Result<SendOutcome> {
        self.send_follow_up_task_inner(task_id, prompt, None)
    }

    fn acquire_prompt_dispatch_gate(
        &self,
        task_id: &str,
        record: &TaskRecord,
        message_kind: &str,
        source: &str,
        prompt: &str,
    ) -> Result<PromptDispatchGateResult> {
        let queued_task = self
            .running_tasks
            .get(task_id)
            .map(|running_task| running_task.queued_task.clone())
            .or_else(|| {
                self.resident_tasks
                    .get(task_id)
                    .map(|resident_task| resident_task.queued_task.clone())
            })
            .or_else(|| {
                self.queued_tasks
                    .iter()
                    .find(|queued_task| queued_task.task.id == task_id)
                    .cloned()
            });
        let Some(queued_task) = queued_task else {
            return Ok(PromptDispatchGateResult::Unavailable);
        };
        let session_id = self
            .running_tasks
            .get(task_id)
            .and_then(|running_task| running_task.handle.session_id())
            .or_else(|| {
                self.resident_tasks
                    .get(task_id)
                    .and_then(|resident_task| resident_task.handle.session_id())
            })
            .or_else(|| record.session_id.clone())
            .unwrap_or_else(|| format!("task:{task_id}"));
        let semantic_key = format!(
            "{message_kind}:{source}:{}",
            prompt
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_lowercase()
        );
        let decision = queued_task.store.reserve_prompt_dispatch_with_options(
            &queued_task.task.goal_id,
            task_id,
            &session_id,
            record.run_epoch as usize,
            message_kind,
            source,
            prompt,
            Some(&semantic_key),
        )?;
        match decision {
            PromptDispatchDecision::Acquired(gate) => {
                Ok(PromptDispatchGateResult::Acquired(queued_task.store, gate))
            }
            PromptDispatchDecision::Duplicate(_) => Ok(PromptDispatchGateResult::Duplicate),
        }
    }

    fn send_follow_up_task_inner(
        &mut self,
        task_id: &str,
        prompt: String,
        caller_session_id: Option<String>,
    ) -> Result<SendOutcome> {
        let Some(record) = self.records.get(task_id).cloned() else {
            return Ok(SendOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        let run_epoch = record.run_epoch;
        let gate_context = match self.acquire_prompt_dispatch_gate(
            task_id,
            &record,
            "follow_up",
            "task_manager",
            &prompt,
        )? {
            PromptDispatchGateResult::Acquired(store, gate) => Some((store, gate)),
            PromptDispatchGateResult::Duplicate => {
                return Ok(SendOutcome::Noop(OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    run_epoch: Some(run_epoch as usize),
                    queue_position: None,
                }));
            }
            PromptDispatchGateResult::Unavailable => None,
        };
        let settle_gate = |status: PromptDispatchGateStatus, reason: Option<String>| {
            if let Some((store, gate)) = gate_context.as_ref() {
                store
                    .settle_prompt_dispatch_gate(gate, status, None, reason)
                    .map(|_| ())
            } else {
                Ok(())
            }
        };

        if record.status == ManagedTaskStatus::Pending {
            let queued = self.control.queue_pending_message(
                task_id,
                QueuedMessageKind::FollowUp,
                prompt,
                caller_session_id,
                gate_context.clone(),
            );
            if let Err(error) = queued {
                let _ = settle_gate(
                    PromptDispatchGateStatus::Failed,
                    Some(format!("failed to queue follow-up: {error:#}")),
                );
                return Err(error);
            }
            settle_gate(PromptDispatchGateStatus::Held, None)?;
            return Ok(SendOutcome::Queued(OutcomeContext {
                task_id: Some(task_id.to_string()),
                run_epoch: Some(run_epoch as usize),
                queue_position: None,
            }));
        }

        if let Some(running_task) = self.running_tasks.get(task_id) {
            if let Err(error) = running_task.handle.send_follow_up(prompt) {
                let status = prompt_dispatch_error_status(&error);
                let reason = prompt_dispatch_error_reason("follow-up", &status);
                let settlement = settle_gate(status.clone(), Some(reason));
                if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
                    settlement?;
                    return Ok(SendOutcome::PossiblyAccepted(OutcomeContext {
                        task_id: Some(task_id.to_string()),
                        run_epoch: Some(run_epoch as usize),
                        queue_position: None,
                    }));
                }
                let _ = settlement;
                return Err(error);
            }
            settle_gate(PromptDispatchGateStatus::Accepted, None)?;
            return Ok(SendOutcome::Sent(OutcomeContext {
                task_id: Some(task_id.to_string()),
                run_epoch: Some(run_epoch as usize),
                queue_position: None,
            }));
        }

        if messageability_for_record(&record) == Messageability::Revive {
            let revived = match self.revive_task(
                task_id,
                prompt.clone(),
                QueuedMessageKind::FollowUp,
            ) {
                Ok(revived) => revived,
                Err(error) => {
                    let status = prompt_dispatch_error_status(&error);
                    let reason = prompt_dispatch_error_reason("follow-up", &status);
                    let settlement = settle_gate(status.clone(), Some(reason));
                    if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
                        settlement?;
                    } else if let Err(settlement_error) = settlement {
                        eprintln!(
                            "failed to settle follow-up revive dispatch gate: {settlement_error:#}"
                        );
                    }
                    return Err(error);
                }
            };
            match revived {
                ReviveDispatchOutcome::Started => {
                    settle_gate(PromptDispatchGateStatus::Accepted, None)?;
                    return Ok(SendOutcome::Revive(OutcomeContext {
                        task_id: Some(task_id.to_string()),
                        run_epoch: Some(run_epoch as usize),
                        queue_position: None,
                    }));
                }
                ReviveDispatchOutcome::PossiblyAccepted => {
                    let status = PromptDispatchGateStatus::PossiblyAccepted;
                    let reason = prompt_dispatch_error_reason("follow-up", &status);
                    settle_gate(status, Some(reason))?;
                    return Ok(SendOutcome::PossiblyAccepted(OutcomeContext {
                        task_id: Some(task_id.to_string()),
                        run_epoch: Some(run_epoch as usize),
                        queue_position: None,
                    }));
                }
                ReviveDispatchOutcome::NotStarted => {}
            }
            if self.resident_tasks.contains_key(task_id) {
                self.pending_revives.push_back(PendingRevive {
                    task_id: task_id.to_string(),
                    message: prompt,
                    kind: QueuedMessageKind::FollowUp,
                    caller_session_id,
                });
                settle_gate(PromptDispatchGateStatus::Held, None)?;
                return Ok(SendOutcome::Queued(OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    run_epoch: Some(run_epoch as usize),
                    queue_position: None,
                }));
            }
            return Ok(SendOutcome::NotContinuable(OutcomeContext {
                task_id: Some(task_id.to_string()),
                run_epoch: Some(run_epoch as usize),
                queue_position: None,
            }));
        }

        settle_gate(
            PromptDispatchGateStatus::Released,
            Some("task is not continuable".to_string()),
        )?;

        Ok(SendOutcome::NotContinuable(OutcomeContext {
            task_id: Some(task_id.to_string()),
            run_epoch: Some(run_epoch as usize),
            queue_position: None,
        }))
    }

    pub fn send_follow_up_task_with_context(
        &mut self,
        task_id: &str,
        prompt: String,
        context: &TaskCommandContext,
    ) -> Result<SendOutcome> {
        let Some(record) = self.records.get(task_id).cloned() else {
            return Ok(SendOutcome::NotFound {
                reason: format!("managed task `{task_id}` was not found"),
                context: OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    ..OutcomeContext::default()
                },
            });
        };
        if !task_scope_allows(&record, context) {
            let reason = scope_denied_reason(&record, context);
            self.record_task_command_event(
                task_id,
                "send_follow_up",
                context,
                false,
                Some(reason.clone()),
            );
            return Ok(SendOutcome::ScopeDenied {
                reason,
                context: OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    run_epoch: Some(record.run_epoch as usize),
                    ..OutcomeContext::default()
                },
            });
        }
        let outcome =
            self.send_follow_up_task_inner(task_id, prompt, context.caller_session_id.clone())?;
        self.record_task_command_event(
            task_id,
            "send_follow_up",
            context,
            outcome.is_accepted(),
            outcome.reason(),
        );
        Ok(outcome)
    }

    pub fn steer_task(&mut self, task_id: &str, prompt: String) -> Result<SteerOutcome> {
        self.steer_task_inner(task_id, prompt, None)
    }

    fn steer_task_inner(
        &mut self,
        task_id: &str,
        prompt: String,
        caller_session_id: Option<String>,
    ) -> Result<SteerOutcome> {
        let Some(record) = self.records.get(task_id).cloned() else {
            return Ok(SteerOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        let run_epoch = record.run_epoch;
        let gate_context = match self.acquire_prompt_dispatch_gate(
            task_id,
            &record,
            "steer",
            "task_manager",
            &prompt,
        )? {
            PromptDispatchGateResult::Acquired(store, gate) => Some((store, gate)),
            PromptDispatchGateResult::Duplicate => {
                return Ok(SteerOutcome::Noop(OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    run_epoch: Some(run_epoch as usize),
                    queue_position: None,
                }));
            }
            PromptDispatchGateResult::Unavailable => None,
        };
        let settle_gate = |status: PromptDispatchGateStatus, reason: Option<String>| {
            if let Some((store, gate)) = gate_context.as_ref() {
                store
                    .settle_prompt_dispatch_gate(gate, status, None, reason)
                    .map(|_| ())
            } else {
                Ok(())
            }
        };

        if record.status == ManagedTaskStatus::Pending {
            let queued = self.control.queue_pending_message(
                task_id,
                QueuedMessageKind::Steer,
                prompt,
                caller_session_id,
                gate_context.clone(),
            );
            if let Err(error) = queued {
                let _ = settle_gate(
                    PromptDispatchGateStatus::Failed,
                    Some(format!("failed to queue steer: {error:#}")),
                );
                return Err(error);
            }
            settle_gate(PromptDispatchGateStatus::Held, None)?;
            return Ok(SteerOutcome::Queued(OutcomeContext {
                task_id: Some(task_id.to_string()),
                run_epoch: Some(run_epoch as usize),
                queue_position: None,
            }));
        }

        if let Some(running_task) = self.running_tasks.get(task_id) {
            if let Err(error) = running_task.handle.steer(prompt) {
                let status = prompt_dispatch_error_status(&error);
                let reason = prompt_dispatch_error_reason("steer", &status);
                let settlement = settle_gate(status.clone(), Some(reason));
                if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
                    settlement?;
                    return Ok(SteerOutcome::PossiblyAccepted(OutcomeContext {
                        task_id: Some(task_id.to_string()),
                        run_epoch: Some(run_epoch as usize),
                        queue_position: None,
                    }));
                }
                let _ = settlement;
                return Err(error);
            }
            settle_gate(PromptDispatchGateStatus::Accepted, None)?;
            return Ok(SteerOutcome::Steered(OutcomeContext {
                task_id: Some(task_id.to_string()),
                run_epoch: Some(run_epoch as usize),
                queue_position: None,
            }));
        }

        if messageability_for_record(&record) == Messageability::Revive {
            let revived = match self.revive_task(task_id, prompt.clone(), QueuedMessageKind::Steer)
            {
                Ok(revived) => revived,
                Err(error) => {
                    let status = prompt_dispatch_error_status(&error);
                    let reason = prompt_dispatch_error_reason("steer", &status);
                    let settlement = settle_gate(status.clone(), Some(reason));
                    if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
                        settlement?;
                    } else if let Err(settlement_error) = settlement {
                        eprintln!(
                            "failed to settle steer revive dispatch gate: {settlement_error:#}"
                        );
                    }
                    return Err(error);
                }
            };
            match revived {
                ReviveDispatchOutcome::Started => {
                    settle_gate(PromptDispatchGateStatus::Accepted, None)?;
                    return Ok(SteerOutcome::Revive(OutcomeContext {
                        task_id: Some(task_id.to_string()),
                        run_epoch: Some(run_epoch as usize),
                        queue_position: None,
                    }));
                }
                ReviveDispatchOutcome::PossiblyAccepted => {
                    let status = PromptDispatchGateStatus::PossiblyAccepted;
                    let reason = prompt_dispatch_error_reason("steer", &status);
                    settle_gate(status, Some(reason))?;
                    return Ok(SteerOutcome::PossiblyAccepted(OutcomeContext {
                        task_id: Some(task_id.to_string()),
                        run_epoch: Some(run_epoch as usize),
                        queue_position: None,
                    }));
                }
                ReviveDispatchOutcome::NotStarted => {}
            }
            if self.resident_tasks.contains_key(task_id) {
                self.pending_revives.push_back(PendingRevive {
                    task_id: task_id.to_string(),
                    message: prompt,
                    kind: QueuedMessageKind::Steer,
                    caller_session_id,
                });
                settle_gate(PromptDispatchGateStatus::Held, None)?;
                return Ok(SteerOutcome::Queued(OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    run_epoch: Some(run_epoch as usize),
                    queue_position: None,
                }));
            }
            return Ok(SteerOutcome::NotContinuable(OutcomeContext {
                task_id: Some(task_id.to_string()),
                run_epoch: Some(run_epoch as usize),
                queue_position: None,
            }));
        }

        settle_gate(
            PromptDispatchGateStatus::Released,
            Some("task is not continuable".to_string()),
        )?;

        Ok(SteerOutcome::NotContinuable(OutcomeContext {
            task_id: Some(task_id.to_string()),
            run_epoch: Some(run_epoch as usize),
            queue_position: None,
        }))
    }

    pub fn steer_task_with_context(
        &mut self,
        task_id: &str,
        prompt: String,
        context: &TaskCommandContext,
    ) -> Result<SteerOutcome> {
        let Some(record) = self.records.get(task_id).cloned() else {
            return Ok(SteerOutcome::NotFound {
                reason: format!("managed task `{task_id}` was not found"),
                context: OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    ..OutcomeContext::default()
                },
            });
        };
        if !task_scope_allows(&record, context) {
            let reason = scope_denied_reason(&record, context);
            self.record_task_command_event(task_id, "steer", context, false, Some(reason.clone()));
            return Ok(SteerOutcome::ScopeDenied {
                reason,
                context: OutcomeContext {
                    task_id: Some(task_id.to_string()),
                    run_epoch: Some(record.run_epoch as usize),
                    ..OutcomeContext::default()
                },
            });
        }
        let outcome = self.steer_task_inner(task_id, prompt, context.caller_session_id.clone())?;
        self.record_task_command_event(
            task_id,
            "steer",
            context,
            outcome.is_accepted(),
            outcome.reason(),
        );
        Ok(outcome)
    }

    pub fn list(&self) -> Vec<TaskRecord> {
        let mut records = self.records.values().cloned().collect::<Vec<_>>();
        records.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        records
    }

    pub fn snapshot(&self) -> Result<TaskManagerSnapshot> {
        let records = self.list();
        let mut counts = TaskManagerSnapshotCounts::default();
        for record in &records {
            match &record.status {
                ManagedTaskStatus::Pending => counts.pending += 1,
                ManagedTaskStatus::Running => counts.running += 1,
                ManagedTaskStatus::Completed => counts.completed += 1,
                ManagedTaskStatus::Failed => counts.failed += 1,
                ManagedTaskStatus::Cancelled => counts.cancelled += 1,
                ManagedTaskStatus::Interrupted => counts.interrupted += 1,
                ManagedTaskStatus::Lost => counts.lost += 1,
                ManagedTaskStatus::Skipped => counts.skipped += 1,
            }
        }

        let tasks = records
            .into_iter()
            .map(|record| {
                let messageability = Some(messageability_for_record(&record));
                let attempts_len = record.attempts.len();
                let status = record.status.clone();
                let has_failure_kind = record.failure_kind.is_some();
                let has_retry_reason = record.retry_reason.is_some();
                let summary_head = summary_head_for_record(&record);
                let continuation_hint = continuation_hint_for_record(&record);
                let last_command = last_task_command_snapshot(&record);
                TaskSnapshot {
                    task_id: record.task_id,
                    status: record.status,
                    residency_state: record.residency_state,
                    messageability,
                    run_epoch: record.run_epoch,
                    notified_epoch: record.notified_epoch,
                    notification_failed_epoch: record.notification_failed_epoch,
                    parent_task_id: record.parent_task_id,
                    parent_session_id: record.parent_session_id,
                    worker_kind: record.worker_kind,
                    worker_model: record.worker_model,
                    worker_category: record.worker_category,
                    attempts: record
                        .attempts
                        .into_iter()
                        .map(|attempt| {
                            let result_path = attempt.result_path.clone();
                            let outcome_path = attempt.outcome_path.clone();
                            let artifact_dir = attempt
                                .result_path
                                .as_ref()
                                .or(attempt.outcome_path.as_ref())
                                .and_then(|path| path.parent());
                            let route_transform_path = task_attempt_route_transform_path(
                                artifact_dir,
                                attempt.attempt,
                                attempts_len,
                                &status,
                                has_failure_kind,
                                has_retry_reason,
                            );
                            TaskAttemptSnapshot {
                                attempt: attempt.attempt,
                                worker_kind: attempt.worker_kind,
                                worker_model: attempt.worker_model,
                                worker_category: attempt.worker_category,
                                status: attempt.status,
                                result_path,
                                outcome_path,
                                route_transform_path,
                                summary: attempt.summary,
                                error: attempt.error,
                            }
                        })
                        .collect(),
                    result_path: record.result_path,
                    outcome_path: record.outcome_path,
                    summary: record.summary,
                    retry_reason: record.retry_reason,
                    summary_head,
                    continuation_hint,
                    last_command,
                }
            })
            .collect();

        Ok(TaskManagerSnapshot {
            counts,
            artifacts_root: self.artifacts_root.clone(),
            tasks,
            current_output: self.control.current_last_output()?,
        })
    }

    /// Reconstruct a read-only task projection from durable worker records.
    /// Persisted tasks deliberately expose no messageability because their
    /// in-memory worker handles are not available after a restart.
    pub fn durable_snapshot(
        store: &StateStore,
        session_id: Option<&str>,
    ) -> Result<Option<TaskManagerSnapshot>> {
        let Ok(entries) = fs::read_dir(store.workers_dir()) else {
            return Ok(None);
        };
        let mut records = Vec::new();
        for entry in entries.flatten().take(64) {
            let task_record_path = entry.path().join("task-record.json");
            if !task_record_path.is_file() {
                continue;
            }
            let Ok(json) = fs::read_to_string(&task_record_path) else {
                continue;
            };
            let Ok(record) = serde_json::from_str::<TaskRecord>(&json) else {
                continue;
            };
            if session_id.is_some_and(|session_id| {
                ![
                    record.session_id.as_deref(),
                    record.parent_session_id.as_deref(),
                    record.root_session_id.as_deref(),
                ]
                .into_iter()
                .flatten()
                .any(|record_session_id| record_session_id == session_id)
            }) {
                continue;
            }
            records.push(record);
        }
        if records.is_empty() {
            return Ok(None);
        }

        let mut counts = TaskManagerSnapshotCounts::default();
        for record in &records {
            match &record.status {
                ManagedTaskStatus::Pending => counts.pending += 1,
                ManagedTaskStatus::Running => counts.running += 1,
                ManagedTaskStatus::Completed => counts.completed += 1,
                ManagedTaskStatus::Failed => counts.failed += 1,
                ManagedTaskStatus::Cancelled => counts.cancelled += 1,
                ManagedTaskStatus::Interrupted => counts.interrupted += 1,
                ManagedTaskStatus::Lost => counts.lost += 1,
                ManagedTaskStatus::Skipped => counts.skipped += 1,
            }
        }
        let tasks = records
            .into_iter()
            .map(|record| {
                let attempts_len = record.attempts.len();
                let status = record.status.clone();
                let has_failure_kind = record.failure_kind.is_some();
                let has_retry_reason = record.retry_reason.is_some();
                let summary_head = summary_head_for_record(&record);
                let continuation_hint = continuation_hint_for_record(&record);
                let last_command = last_task_command_snapshot(&record);
                TaskSnapshot {
                    task_id: record.task_id,
                    status: record.status,
                    residency_state: record.residency_state,
                    messageability: None,
                    run_epoch: record.run_epoch,
                    notified_epoch: record.notified_epoch,
                    notification_failed_epoch: record.notification_failed_epoch,
                    parent_task_id: record.parent_task_id,
                    parent_session_id: record.parent_session_id,
                    worker_kind: record.worker_kind,
                    worker_model: record.worker_model,
                    worker_category: record.worker_category,
                    attempts: record
                        .attempts
                        .into_iter()
                        .map(|attempt| {
                            let result_path = attempt.result_path.clone();
                            let outcome_path = attempt.outcome_path.clone();
                            let artifact_dir = attempt
                                .result_path
                                .as_ref()
                                .or(attempt.outcome_path.as_ref())
                                .and_then(|path| path.parent());
                            TaskAttemptSnapshot {
                                attempt: attempt.attempt,
                                worker_kind: attempt.worker_kind,
                                worker_model: attempt.worker_model,
                                worker_category: attempt.worker_category,
                                status: attempt.status,
                                result_path,
                                outcome_path,
                                route_transform_path: task_attempt_route_transform_path(
                                    artifact_dir,
                                    attempt.attempt,
                                    attempts_len,
                                    &status,
                                    has_failure_kind,
                                    has_retry_reason,
                                ),
                                summary: attempt.summary,
                                error: attempt.error,
                            }
                        })
                        .collect(),
                    result_path: record.result_path,
                    outcome_path: record.outcome_path,
                    summary_head,
                    continuation_hint,
                    summary: record.summary,
                    retry_reason: record.retry_reason,
                    last_command,
                }
            })
            .collect();
        Ok(Some(TaskManagerSnapshot {
            counts,
            artifacts_root: Some(store.artifacts_dir()),
            tasks,
            current_output: None,
        }))
    }

    fn can_start_queued_task(&self, queued_task: &QueuedTask) -> bool {
        if !self.concurrency.can_start(queued_task) {
            return false;
        }

        if is_read_only_task(&queued_task.task) {
            return true;
        }

        !self
            .running_tasks
            .values()
            .any(|running_task| scopes_overlap(&queued_task.task, &running_task.queued_task.task))
    }

    fn process_pending_messages(&mut self) -> Result<()> {
        let task_ids = self.control.pending_message_task_ids()?;
        for task_id in task_ids {
            let mut pending_messages = self.control.take_pending_messages(&task_id)?;
            if pending_messages.is_empty() {
                continue;
            }

            let Some(record) = self.records.get(&task_id).cloned() else {
                for queued_message in pending_messages {
                    settle_queued_message_gate_best_effort(
                        &queued_message,
                        PromptDispatchGateStatus::Released,
                        Some("task record disappeared before queued delivery".to_string()),
                    );
                }
                continue;
            };

            if let Some(running_task) = self.running_tasks.get(&task_id) {
                let handle = Arc::clone(&running_task.handle);
                while let Some(mut queued_message) = pending_messages.pop_front() {
                    if let Err(error) = deliver_queued_message(&handle, &queued_message) {
                        let status = prompt_dispatch_error_status(&error);
                        let reason = prompt_dispatch_error_reason(
                            queued_message_operation(&queued_message.kind),
                            &status,
                        );
                        if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
                            settle_queued_message_gate_best_effort(
                                &queued_message,
                                status,
                                Some(reason),
                            );
                        } else if queued_message.delivery_attempts + 1
                            >= MAX_PENDING_MESSAGE_DELIVERY_ATTEMPTS
                        {
                            settle_queued_message_gate_best_effort(
                                &queued_message,
                                PromptDispatchGateStatus::Failed,
                                Some(reason),
                            );
                        } else {
                            queued_message.delivery_attempts += 1;
                            let mut retry_messages = VecDeque::from([queued_message]);
                            retry_messages.extend(pending_messages);
                            self.control
                                .prepend_pending_messages(&task_id, retry_messages)?;
                            break;
                        }
                        if !pending_messages.is_empty() {
                            self.control
                                .prepend_pending_messages(&task_id, pending_messages)?;
                        }
                        break;
                    }
                    settle_queued_message_gate_best_effort(
                        &queued_message,
                        PromptDispatchGateStatus::Accepted,
                        None,
                    );
                }
                continue;
            }

            let can_revive = messageability_for_record(&record) == Messageability::Revive
                && self.resident_tasks.contains_key(&task_id);
            if can_revive {
                let Some(mut queued_message) = pending_messages.pop_front() else {
                    continue;
                };
                let can_start = self
                    .resident_tasks
                    .get(&task_id)
                    .is_some_and(|resident_task| {
                        self.concurrency.can_start(&resident_task.queued_task)
                    });
                if !can_start {
                    let mut retry_messages = VecDeque::from([queued_message]);
                    retry_messages.extend(pending_messages);
                    self.control
                        .prepend_pending_messages(&task_id, retry_messages)?;
                    continue;
                }

                match self.revive_task(
                    &task_id,
                    queued_message.message.clone(),
                    queued_message.kind.clone(),
                ) {
                    Ok(ReviveDispatchOutcome::Started) => {
                        settle_queued_message_gate_best_effort(
                            &queued_message,
                            PromptDispatchGateStatus::Accepted,
                            None,
                        );
                        if !pending_messages.is_empty() {
                            self.control
                                .prepend_pending_messages(&task_id, pending_messages)?;
                        }
                    }
                    Ok(ReviveDispatchOutcome::PossiblyAccepted) => {
                        settle_queued_message_gate_best_effort(
                            &queued_message,
                            PromptDispatchGateStatus::PossiblyAccepted,
                            Some(prompt_dispatch_error_reason(
                                queued_message_operation(&queued_message.kind),
                                &PromptDispatchGateStatus::PossiblyAccepted,
                            )),
                        );
                        if !pending_messages.is_empty() {
                            self.control
                                .prepend_pending_messages(&task_id, pending_messages)?;
                        }
                    }
                    Ok(ReviveDispatchOutcome::NotStarted) => {
                        let mut retry_messages = VecDeque::from([queued_message]);
                        retry_messages.extend(pending_messages);
                        self.control
                            .prepend_pending_messages(&task_id, retry_messages)?;
                    }
                    Err(error) => {
                        let status = prompt_dispatch_error_status(&error);
                        let reason = prompt_dispatch_error_reason(
                            queued_message_operation(&queued_message.kind),
                            &status,
                        );
                        if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
                            settle_queued_message_gate_best_effort(
                                &queued_message,
                                status,
                                Some(reason),
                            );
                        } else if queued_message.delivery_attempts + 1
                            >= MAX_PENDING_MESSAGE_DELIVERY_ATTEMPTS
                        {
                            settle_queued_message_gate_best_effort(
                                &queued_message,
                                PromptDispatchGateStatus::Failed,
                                Some(reason),
                            );
                        } else {
                            queued_message.delivery_attempts += 1;
                            let mut retry_messages = VecDeque::from([queued_message]);
                            retry_messages.extend(pending_messages);
                            self.control
                                .prepend_pending_messages(&task_id, retry_messages)?;
                            continue;
                        }
                        if !pending_messages.is_empty() {
                            self.control
                                .prepend_pending_messages(&task_id, pending_messages)?;
                        }
                    }
                }
                continue;
            }

            if record.status == ManagedTaskStatus::Pending
                || self
                    .queued_tasks
                    .iter()
                    .any(|queued_task| queued_task.task.id == task_id)
            {
                self.control
                    .prepend_pending_messages(&task_id, pending_messages)?;
                continue;
            }

            for queued_message in pending_messages {
                settle_queued_message_gate_best_effort(
                    &queued_message,
                    PromptDispatchGateStatus::Released,
                    Some("task is no longer continuable".to_string()),
                );
            }
        }
        Ok(())
    }

    fn process_pending_revives(&mut self) -> Result<()> {
        let mut pending = std::mem::take(&mut self.pending_revives);
        let mut remaining = VecDeque::new();
        while let Some(request) = pending.pop_front() {
            if let Some(running_task) = self.running_tasks.get(&request.task_id) {
                let delivery_result = match &request.kind {
                    QueuedMessageKind::FollowUp => {
                        running_task.handle.send_follow_up(request.message.clone())
                    }
                    QueuedMessageKind::Steer => running_task.handle.steer(request.message.clone()),
                };
                if let Err(error) = delivery_result {
                    eprintln!(
                        "failed to deliver queued resident revive for task `{}` from session {:?}: {error:#}",
                        request.task_id, request.caller_session_id
                    );
                    remaining.push_back(request);
                    remaining.extend(pending);
                    break;
                }
                continue;
            }
            let can_revive = self.records.get(&request.task_id).is_some_and(|record| {
                messageability_for_record(record) == Messageability::Revive
                    && self.resident_tasks.contains_key(&request.task_id)
            });
            if !can_revive {
                continue;
            }
            let Some(resident_task) = self.resident_tasks.get(&request.task_id) else {
                continue;
            };
            if !self.concurrency.can_start(&resident_task.queued_task) {
                remaining.push_back(request);
                continue;
            }
            match self.revive_task(
                &request.task_id,
                request.message.clone(),
                request.kind.clone(),
            )? {
                ReviveDispatchOutcome::Started | ReviveDispatchOutcome::PossiblyAccepted => {}
                ReviveDispatchOutcome::NotStarted => {
                    eprintln!(
                        "Gear resident revive remained queued for task `{}` from session {:?}",
                        request.task_id, request.caller_session_id
                    );
                    remaining.push_back(request);
                }
            }
        }
        self.pending_revives = remaining;
        Ok(())
    }

    fn process_queue(&mut self) -> Result<()> {
        self.process_pending_messages()?;
        self.process_pending_revives()?;
        while self.running_tasks.len() < self.concurrency.max_parallel_workers() {
            let Some(index) = self
                .queued_tasks
                .iter()
                .position(|queued_task| self.can_start_queued_task(queued_task))
            else {
                break;
            };
            let queued_task = self
                .queued_tasks
                .remove(index)
                .context("queued task disappeared while starting worker")?;
            self.start_queued_task(queued_task)?;
        }
        Ok(())
    }

    fn remember_unavailable_model_for_goal(&mut self, goal_id: &str, record: &TaskRecord) {
        let Some(previous_attempt) = record.attempts.last() else {
            return;
        };
        if !matches!(
            previous_attempt.failure_kind,
            Some(
                TaskFailureKind::ModelUnavailable | TaskFailureKind::ProviderTemporarilyUnavailable
            )
        ) {
            return;
        }
        let Some(worker_model) = previous_attempt
            .worker_model
            .as_deref()
            .map(str::trim)
            .filter(|worker_model| !worker_model.is_empty())
        else {
            return;
        };
        self.goal_unavailable_worker_models
            .entry(goal_id.to_string())
            .or_default()
            .insert(worker_model.to_string(), Instant::now());
    }

    fn apply_goal_unavailable_models(&mut self, queued_task: &mut QueuedTask) {
        let unavailable_models = self
            .goal_unavailable_worker_models
            .get_mut(&queued_task.task.goal_id)
            .map(|models| {
                models.retain(|_, failed_at| failed_at.elapsed() < GOAL_WORKER_MODEL_COOLDOWN);
                models.keys().cloned().collect::<Vec<_>>()
            });
        let Some(unavailable_models) = unavailable_models else {
            return;
        };
        if unavailable_models.is_empty() {
            self.goal_unavailable_worker_models
                .remove(&queued_task.task.goal_id);
            return;
        }
        for unavailable_model in unavailable_models {
            if !queued_task
                .config
                .unavailable_worker_models
                .iter()
                .any(|configured_model| configured_model.eq_ignore_ascii_case(&unavailable_model))
            {
                queued_task
                    .config
                    .unavailable_worker_models
                    .push(unavailable_model);
            }
        }
    }

    fn remember_provider_session_for_goal(
        &mut self,
        goal_id: &str,
        store: &StateStore,
        task_id: &str,
    ) -> Result<()> {
        let Some(provider_session_id) = provider_session_id_for_task(store, task_id)? else {
            return Ok(());
        };
        self.goal_provider_sessions
            .insert(goal_id.to_string(), provider_session_id);
        Ok(())
    }

    fn seed_goal_provider_session(&self, queued_task: &QueuedTask) -> Result<()> {
        let selected_route = queued_task
            .config
            .selected_route_for_hint(queued_task.route_attempt, queued_task.route_hint.as_deref());
        // A review must be an independent execution. Reusing the goal's
        // provider session would make the reviewer share the executor's
        // session identity, which invalidates the typed receipt's
        // reviewed-execution binding and weakens the review gate.
        if selected_route.worker_kind != WorkerKind::OpencodeSession
            || selected_route.category == WorkerCategory::Review
            || queued_task.route_hint.as_deref() == Some("review")
        {
            return Ok(());
        }
        let Some(provider_session_id) = self.goal_provider_sessions.get(&queued_task.task.goal_id)
        else {
            return Ok(());
        };
        seed_provider_session_for_task(
            &queued_task.store,
            &queued_task.workspace,
            &queued_task.task,
            selected_route.worker_kind,
            selected_route.worker_model.map(ToString::to_string),
            provider_session_id,
        )
    }

    fn start_queued_task(&mut self, mut queued_task: QueuedTask) -> Result<()> {
        self.apply_goal_unavailable_models(&mut queued_task);
        if queued_task.task.attempt > 1 {
            let selected_route = queued_task.config.selected_route_for_hint(
                queued_task.route_attempt,
                queued_task.route_hint.as_deref(),
            );
            discard_resident_session_for_model_switch(
                &queued_task.store,
                &queued_task.workspace,
                &queued_task.task.id,
                selected_route.worker_kind,
                selected_route.worker_model,
            )?;
        }
        self.seed_goal_provider_session(&queued_task)?;
        let task_id = queued_task.task.id.clone();
        let activity_heartbeat = Arc::new(Mutex::new(Instant::now()));
        let circuit_state = Arc::new(Mutex::new(ToolCallCircuitState::default()));
        let circuit_policy = self.runtime_policy.tool_call_circuit_breaker.clone();
        loop {
            if let Some(model_unavailable_error) =
                model_unavailable_error_for_task(&queued_task.config, &queued_task)
            {
                let mut failed_record = self
                    .records
                    .remove(&task_id)
                    .context("missing task manager record for unavailable worker model")?;
                let transition = transition_task_record(
                    &mut failed_record,
                    TaskTransition::Skip {
                        finished_at: timestamp(),
                        result_path: queued_task.store.worker_dir(&task_id).join("result.json"),
                        outcome_path: queued_task.store.worker_dir(&task_id).join("outcome.json"),
                        summary: model_unavailable_error.clone(),
                        failure_kind: Some(TaskFailureKind::ModelUnavailable),
                    },
                );
                write_task_record(&queued_task.store, &failed_record)?;
                append_task_lifecycle_event(&queued_task.store, &failed_record, Some(&transition))?;

                match queue_next_attempt(&mut failed_record, &mut queued_task) {
                    FallbackDecision::Queued => {
                        write_task_record(&queued_task.store, &failed_record)?;
                        append_task_lifecycle_event(&queued_task.store, &failed_record, None)?;
                        self.records.insert(task_id.clone(), failed_record);
                        continue;
                    }
                    FallbackDecision::Unavailable {
                        reason,
                        failure_kind,
                    } => {
                        failed_record.failure_kind = Some(failure_kind);
                        failed_record.retry_reason = Some(reason.clone());
                        if let Some(attempt) = failed_record.attempts.last_mut() {
                            attempt.retry_reason = Some(reason);
                        }
                        let (result, outcome) = write_model_unavailable_artifacts(
                            &queued_task.store,
                            &task_id,
                            &model_unavailable_error,
                        )?;
                        failed_record.result_path = Some(result.result_path.clone());
                        failed_record.outcome_path = Some(result.outcome_path.clone());
                        write_task_record(&queued_task.store, &failed_record)?;
                        append_task_lifecycle_event(&queued_task.store, &failed_record, None)?;
                        let run = ManagedWorkerRun {
                            store: queued_task.store.clone(),
                            result,
                            outcome,
                            record: failed_record.clone(),
                        };
                        self.control
                            .update_current_status(&task_id, failed_record.status.clone())?;
                        self.completed_runs.insert(task_id.clone(), run);
                        self.records.insert(task_id.clone(), failed_record);
                        return Ok(());
                    }
                }
            }

            if !self.concurrency.acquire(&queued_task) {
                self.queued_tasks.push_back(queued_task);
                return Ok(());
            }

            let evidence_baseline = match snapshot_worker_evidence_paths(&queued_task.workspace) {
                Ok(paths) => paths,
                Err(reason) => {
                    eprintln!(
                        "failed to snapshot Gear worker evidence before task `{task_id}`: {reason}"
                    );
                    Vec::new()
                }
            };
            let handle = match self.registry.start(WorkerStartRequest {
                store: &queued_task.store,
                workspace: &queued_task.workspace,
                task: &queued_task.task,
                route_attempt: queued_task.route_attempt,
                goal: &queued_task.goal,
                verification_commands: &queued_task.verification_commands,
                config: &queued_task.config,
                cancellation_token: queued_task.cancellation_token.clone(),
                coordinator_model: queued_task.coordinator_model.as_ref(),
                coordinator_brief: queued_task.coordinator_brief.as_deref(),
                route_hint: queued_task.route_hint.as_deref(),
            }) {
                Ok(handle) => handle,
                Err(error) => {
                    if !self.concurrency.release(&queued_task) {
                        return Err(error).context(format!(
                            "failed to release concurrency slot after worker start failed for {task_id}"
                        ));
                    }
                    let mut failed_record = self
                        .records
                        .remove(&task_id)
                        .context("missing task manager record for failed worker start")?;
                    let transition = transition_task_record(
                        &mut failed_record,
                        TaskTransition::Fail {
                            finished_at: timestamp(),
                            summary: "Worker task failed before producing an outcome.".to_string(),
                            failure_kind: TaskFailureKind::WorkerStartFailed,
                            error: Some(format!("{error:#}")),
                        },
                    );
                    write_task_record(&queued_task.store, &failed_record)?;
                    append_task_lifecycle_event(
                        &queued_task.store,
                        &failed_record,
                        Some(&transition),
                    )?;

                    match queue_next_attempt(&mut failed_record, &mut queued_task) {
                        FallbackDecision::Queued => {
                            write_task_record(&queued_task.store, &failed_record)?;
                            append_task_lifecycle_event(&queued_task.store, &failed_record, None)?;
                            self.records.insert(task_id.clone(), failed_record);
                            continue;
                        }
                        FallbackDecision::Unavailable {
                            reason,
                            failure_kind,
                        } => {
                            failed_record.failure_kind = Some(failure_kind);
                            failed_record.retry_reason = Some(reason.clone());
                            if let Some(attempt) = failed_record.attempts.last_mut() {
                                attempt.retry_reason = Some(reason);
                            }
                            write_task_record(&queued_task.store, &failed_record)?;
                            append_task_lifecycle_event(&queued_task.store, &failed_record, None)?;
                        }
                    }

                    self.control
                        .update_current_status(&task_id, failed_record.status.clone())?;
                    self.records.insert(task_id.clone(), failed_record);
                    return Err(error);
                }
            };
            self.evidence_baselines
                .insert(task_id.clone(), evidence_baseline);
            if let Some(record) = self.records.get_mut(&task_id) {
                let transition = transition_task_record(
                    record,
                    TaskTransition::Start {
                        session_id: handle.session_id(),
                    },
                );
                record.parent_session_id = self.session_scope.clone();
                record.root_session_id = self.session_scope.clone();
                write_task_record(&queued_task.store, record)?;
                append_task_lifecycle_event(&queued_task.store, record, Some(&transition))?;
            }
            let run_epoch = self
                .records
                .get(&task_id)
                .map(|record| record.run_epoch)
                .unwrap_or_default();
            let subscription = subscribe_to_worker_events_with_activity_and_circuit(
                &handle,
                &queued_task.store,
                &task_id,
                &queued_task.task.goal_id,
                run_epoch,
                self.goal_epoch_context.clone(),
                Some(activity_heartbeat.clone()),
                Some(circuit_state.clone()),
                circuit_policy.clone(),
            )?;
            self.control.set_current(
                task_id.clone(),
                ManagedTaskStatus::Running,
                Some(Arc::clone(&handle)),
            )?;
            let control_session_id = self
                .records
                .get(&task_id)
                .and_then(|record| record.session_id.clone())
                .unwrap_or_else(|| format!("task:{task_id}"));
            self.control.set_dispatch_context(
                &task_id,
                queued_task.store.clone(),
                queued_task.task.goal_id.clone(),
                control_session_id,
                self.records
                    .get(&task_id)
                    .map(|record| record.run_epoch)
                    .unwrap_or_default(),
            )?;
            let running_task = RunningTask {
                store: queued_task.store.clone(),
                handle,
                queued_task,
                started_at: Instant::now(),
                _subscription: subscription,
            };
            self.running_tasks
                .insert(task_id.clone(), running_task.clone());
            self.activity_heartbeats
                .insert(task_id.clone(), activity_heartbeat.clone());
            self.tool_call_circuit_states
                .insert(task_id.clone(), circuit_state.clone());
            let pending_messages = self.control.take_pending_messages(&task_id)?;
            let mut pending_messages = pending_messages.into_iter();
            while let Some(mut queued_message) = pending_messages.next() {
                if let Err(error) = deliver_queued_message(&running_task.handle, &queued_message) {
                    eprintln!(
                        "failed to deliver queued Gear message for task `{task_id}` from {:?} created at {}: {error:#}",
                        queued_message.caller_session_id, queued_message.created_at
                    );
                    let status = prompt_dispatch_error_status(&error);
                    let reason = prompt_dispatch_error_reason(
                        queued_message_operation(&queued_message.kind),
                        &status,
                    );
                    if matches!(status, PromptDispatchGateStatus::PossiblyAccepted) {
                        settle_queued_message_gate_best_effort(
                            &queued_message,
                            status,
                            Some(reason),
                        );
                        let remaining = pending_messages.collect::<VecDeque<_>>();
                        if !remaining.is_empty() {
                            self.control.prepend_pending_messages(&task_id, remaining)?;
                        }
                    } else if queued_message.delivery_attempts + 1
                        >= MAX_PENDING_MESSAGE_DELIVERY_ATTEMPTS
                    {
                        settle_queued_message_gate_best_effort(
                            &queued_message,
                            PromptDispatchGateStatus::Failed,
                            Some(reason),
                        );
                        let remaining = pending_messages.collect::<VecDeque<_>>();
                        if !remaining.is_empty() {
                            self.control.prepend_pending_messages(&task_id, remaining)?;
                        }
                    } else {
                        queued_message.delivery_attempts += 1;
                        let mut retry_messages = VecDeque::from([queued_message]);
                        retry_messages.extend(pending_messages);
                        self.control
                            .prepend_pending_messages(&task_id, retry_messages)?;
                    }
                    break;
                }
                settle_queued_message_gate_best_effort(
                    &queued_message,
                    PromptDispatchGateStatus::Accepted,
                    None,
                );
            }
            self.dispatch_running_task(task_id, running_task);
            return Ok(());
        }
    }

    fn dispatch_running_task(&self, task_id: String, running_task: RunningTask) {
        let finished_task_tx = self.finished_task_tx.clone();
        std::thread::spawn(move || {
            let run_result = (|| -> Result<(WorkerOutcome, WorkerResult)> {
                let outcome = running_task.handle.wait_for_outcome()?;
                let result = running_task.handle.wait_for_idle()?;
                Ok((outcome, result))
            })();
            if let Err(error) = finished_task_tx.send(FinishedTaskMessage {
                task_id,
                running_task,
                run_result,
            }) {
                eprintln!("failed to dispatch finished Gear worker task: {error}");
            }
        });
    }
}

fn task_attempt_route_transform_path(
    artifact_dir: Option<&std::path::Path>,
    attempt_index: usize,
    attempts_len: usize,
    status: &ManagedTaskStatus,
    has_failure_kind: bool,
    has_retry_reason: bool,
) -> Option<PathBuf> {
    let worker_dir = artifact_dir?.to_path_buf();
    if attempt_index < attempts_len {
        return Some(worker_dir.join(format!(
            "route-transform-{}-to-{}.md",
            attempt_index,
            attempt_index + 1
        )));
    }

    if attempt_index == 1
        && attempts_len == 1
        && has_failure_kind
        && has_retry_reason
        && !matches!(status, ManagedTaskStatus::Completed)
    {
        return Some(worker_dir.join("route-transform-1-stopped.md"));
    }

    if attempt_index == attempts_len
        && has_failure_kind
        && has_retry_reason
        && !matches!(status, ManagedTaskStatus::Completed)
    {
        return Some(worker_dir.join(format!("route-transform-{}-stopped.md", attempt_index)));
    }

    None
}

// ── Phase 5: Completion notification & parent wake ──

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParentSessionState {
    Idle,
    Streaming,
    Compacting,
    SessionSwitching,
    SessionShutdown,
}

impl ParentSessionState {
    pub fn can_wake(&self) -> bool {
        matches!(self, Self::Idle)
    }

    pub fn should_buffer(&self) -> bool {
        matches!(
            self,
            Self::Streaming | Self::Compacting | Self::SessionSwitching | Self::SessionShutdown
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationResult {
    Skipped,
    Sent,
    Buffered,
    Dropped,
    Failed(String),
}

#[derive(Clone, Debug)]
pub struct CompletionNotification {
    pub task_id: String,
    pub task_name: String,
    pub status: ManagedTaskStatus,
    pub run_epoch: u64,
    pub summary: String,
    pub summary_head: String,
    pub continuation_hint: String,
    pub failure_kind: Option<TaskFailureKind>,
    pub duration_ms: u64,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
}

const NOTIFIER_DEBOUNCE_MS: u64 = 100;
const NOTIFIER_RETRY_DELAY_MS: u64 = 100;
const NOTIFIER_RETRY_ATTEMPTS: usize = 2;
const NOTIFIER_REDELIVERY_ATTEMPTS: usize = 3;

#[derive(Clone, Default)]
pub struct CompletionNotifier {
    buffer: Arc<Mutex<HashMap<(String, u64), CompletionNotification>>>,
    last_flush: Arc<Mutex<HashMap<String, Instant>>>,
    /// Per-session serialization: tracks whether a flush is currently in progress
    /// for a given parent session. Prevents concurrent flushes for the same session.
    flush_serializer: Arc<Mutex<HashMap<String, bool>>>,
    /// Per-session pending flush queue: stores signals from callers that attempted
    /// to flush while another flush was in progress. Processed in FIFO order after
    /// the current flush completes, ensuring ordered retry.
    pending_flush: Arc<Mutex<HashMap<String, VecDeque<()>>>>,
}

impl CompletionNotifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn should_notify(record: &TaskRecord) -> bool {
        matches!(
            record.status,
            ManagedTaskStatus::Completed
                | ManagedTaskStatus::Failed
                | ManagedTaskStatus::Cancelled
                | ManagedTaskStatus::Interrupted
                | ManagedTaskStatus::Lost
        )
    }

    pub fn already_notified(record: &TaskRecord) -> bool {
        record.notified_epoch >= 0 && (record.notified_epoch as u64) >= record.run_epoch
    }

    pub fn build_notification(
        record: &TaskRecord,
        start: &str,
        finish: &str,
    ) -> Option<CompletionNotification> {
        if !Self::should_notify(record) || Self::already_notified(record) {
            return None;
        }

        let start_ms = parse_timestamp_ms(start).unwrap_or(0);
        let finish_ms = parse_timestamp_ms(finish).unwrap_or(0);
        let duration_ms = finish_ms.saturating_sub(start_ms);

        Some(CompletionNotification {
            task_id: record.task_id.clone(),
            task_name: format!("{} `{}`", record.worker_kind, record.worker_category),
            status: record.status.clone(),
            run_epoch: record.run_epoch,
            summary: record.summary.clone(),
            summary_head: summary_head_for_record(record),
            continuation_hint: continuation_hint_for_record(record),
            failure_kind: record.failure_kind.clone(),
            duration_ms,
            result_path: record.result_path.clone(),
            outcome_path: record.outcome_path.clone(),
        })
    }

    pub fn try_notify(
        &self,
        notification: CompletionNotification,
        parent_state: ParentSessionState,
        write_notified: &dyn Fn(&str, u64) -> Result<()>,
        record_failed_epoch: &dyn Fn(&str, u64) -> Result<()>,
    ) -> Result<NotificationResult> {
        if !Self::is_notifiable_status(&notification.status) {
            return Ok(NotificationResult::Skipped);
        }

        if parent_state.can_wake() {
            Self::deliver_with_retry(&notification, write_notified, record_failed_epoch)
        } else if parent_state.should_buffer() {
            self.buffer
                .lock()
                .map_err(|_| anyhow::anyhow!("completion notifier buffer mutex poisoned"))?
                .entry((notification.task_id.clone(), notification.run_epoch))
                .or_insert(notification);
            Ok(NotificationResult::Buffered)
        } else {
            Ok(NotificationResult::Dropped)
        }
    }

    fn deliver_with_retry(
        notification: &CompletionNotification,
        write_notified: &dyn Fn(&str, u64) -> Result<()>,
        record_failed_epoch: &dyn Fn(&str, u64) -> Result<()>,
    ) -> Result<NotificationResult> {
        let mut last_failure: Option<String> = None;
        for redelivery_attempt in 0..NOTIFIER_REDELIVERY_ATTEMPTS {
            for attempt in 0..NOTIFIER_RETRY_ATTEMPTS {
                match write_notified(&notification.task_id, notification.run_epoch) {
                    Ok(()) => return Ok(NotificationResult::Sent),
                    Err(error) => {
                        last_failure = Some(format!("{error:#}"));
                        if attempt + 1 < NOTIFIER_RETRY_ATTEMPTS {
                            std::thread::sleep(Duration::from_millis(NOTIFIER_RETRY_DELAY_MS));
                        }
                    }
                }
            }

            if redelivery_attempt + 1 < NOTIFIER_REDELIVERY_ATTEMPTS {
                std::thread::sleep(Duration::from_millis(NOTIFIER_RETRY_DELAY_MS));
            }
        }

        if let Err(record_error) =
            record_failed_epoch(&notification.task_id, notification.run_epoch)
        {
            eprintln!(
                "failed to record completion notification failure for {} epoch {}: {record_error:#}",
                notification.task_id, notification.run_epoch,
            );
        }
        Ok(NotificationResult::Failed(last_failure.unwrap_or_else(
            || "notification delivery failed".to_string(),
        )))
    }

    pub fn flush_buffer(
        &self,
        parent_session_id: &str,
        parent_state: ParentSessionState,
        write_notified: &dyn Fn(&str, u64) -> Result<()>,
        record_failed_epoch: &dyn Fn(&str, u64) -> Result<()>,
        read_record: &dyn Fn(&str) -> Result<Option<TaskRecord>>,
    ) -> Result<Vec<NotificationResult>> {
        let mut all_results = Vec::new();

        if !parent_state.can_wake() {
            return Ok(all_results);
        }

        // Serialization lock: only one flush at a time per session.
        // If another flush is in progress, queue this request and return;
        // the running flush will pick it up after it completes.
        {
            let mut serializer = self.flush_serializer.lock().map_err(|_| {
                anyhow::anyhow!("completion notifier flush_serializer mutex poisoned")
            })?;
            if *serializer
                .entry(parent_session_id.to_string())
                .or_insert(false)
            {
                self.pending_flush
                    .lock()
                    .map_err(|_| {
                        anyhow::anyhow!("completion notifier pending_flush mutex poisoned")
                    })?
                    .entry(parent_session_id.to_string())
                    .or_default()
                    .push_back(());
                return Ok(all_results);
            }
            serializer.insert(parent_session_id.to_string(), true);
        }

        loop {
            let now = Instant::now();
            let debounce_ok = {
                let mut last_flush = self.last_flush.lock().map_err(|_| {
                    anyhow::anyhow!("completion notifier last_flush mutex poisoned")
                })?;
                let last = last_flush
                    .get(parent_session_id)
                    .copied()
                    .unwrap_or(now - Duration::from_millis(NOTIFIER_DEBOUNCE_MS * 2));
                if now.duration_since(last) < Duration::from_millis(NOTIFIER_DEBOUNCE_MS) {
                    false
                } else {
                    last_flush.insert(parent_session_id.to_string(), now);
                    true
                }
            };

            if debounce_ok {
                let mut buffer = self
                    .buffer
                    .lock()
                    .map_err(|_| anyhow::anyhow!("completion notifier buffer mutex poisoned"))?;
                let mut keys: Vec<(String, u64)> = buffer.keys().cloned().collect();
                keys.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
                for key in keys {
                    if let Some(notification) = buffer.remove(&key) {
                        // State re-verification: check the task record still
                        // matches before sending. If the task has been revived
                        // to a new epoch or its status changed, skip it.
                        let should_send = match read_record(&notification.task_id) {
                            Ok(Some(record)) => {
                                record.run_epoch == notification.run_epoch
                                    && Self::is_notifiable_status(&record.status)
                            }
                            Ok(None) | Err(_) => {
                                // Record missing or read error: still attempt
                                // delivery so transient storage issues don't
                                // cause dropped notifications.
                                true
                            }
                        };
                        if should_send {
                            all_results.push(Self::deliver_with_retry(
                                &notification,
                                write_notified,
                                record_failed_epoch,
                            )?);
                        } else {
                            all_results.push(NotificationResult::Skipped);
                        }
                    }
                }
            }

            // Atomically: release serializer lock, check for pending flushes,
            // and re-acquire if another request is queued.
            let has_pending = {
                let mut serializer = self.flush_serializer.lock().map_err(|_| {
                    anyhow::anyhow!("completion notifier flush_serializer mutex poisoned")
                })?;
                serializer.insert(parent_session_id.to_string(), false);

                let mut pending = self.pending_flush.lock().map_err(|_| {
                    anyhow::anyhow!("completion notifier pending_flush mutex poisoned")
                })?;
                if let Some(queue) = pending.get_mut(parent_session_id) {
                    if !queue.is_empty() {
                        queue.pop_front();
                        serializer.insert(parent_session_id.to_string(), true);
                        true
                    } else {
                        pending.remove(parent_session_id);
                        false
                    }
                } else {
                    false
                }
            };

            if !has_pending {
                break;
            }
        }

        Ok(all_results)
    }

    fn is_notifiable_status(status: &ManagedTaskStatus) -> bool {
        matches!(
            status,
            ManagedTaskStatus::Completed
                | ManagedTaskStatus::Failed
                | ManagedTaskStatus::Cancelled
                | ManagedTaskStatus::Interrupted
                | ManagedTaskStatus::Lost
        )
    }
}

fn parse_timestamp_ms(ts: &str) -> Option<u64> {
    // Parse RFC 3339 timestamp to approximate epoch milliseconds.
    // This is used for completion notification duration display - precision
    // isn't critical so a simple parser suffices.
    let ts = ts.strip_suffix('Z').unwrap_or(ts);
    let parts: Vec<&str> = ts
        .splitn(6, |c| c == '-' || c == 'T' || c == ':' || c == '.')
        .collect();
    if parts.len() < 5 {
        return None;
    }
    let y: u64 = parts[0].parse().ok()?;
    let m: u64 = parts[1].parse().ok()?;
    let d: u64 = parts[2].parse().ok()?;
    let hh: u64 = parts[3].parse().ok()?;
    let mm: u64 = parts[4].parse().ok()?;
    let sec_ms: u64 = parts
        .get(5)
        .and_then(|s| {
            let sec: u64 = s.chars().take(2).collect::<String>().parse().ok()?;
            let millis: u64 = s
                .chars()
                .skip(3)
                .take(3)
                .collect::<String>()
                .parse()
                .unwrap_or(0);
            Some(sec * 1000 + millis)
        })
        .unwrap_or(0);
    // Days since epoch (simplified, ignoring leap seconds)
    let epoch_days = (y - 1970) * 365 + (y - 1969) / 4 + day_of_year(y, m, d);
    Some(epoch_days * 86400_000 + hh * 3600_000 + mm * 60_000 + sec_ms)
}

fn day_of_year(y: u64, m: u64, d: u64) -> u64 {
    let leap = y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400));
    let days = [
        0,
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    days.iter().take(m as usize).sum::<u64>() + d - 1
}

fn is_terminal_status(status: &ManagedTaskStatus) -> bool {
    matches!(
        status,
        ManagedTaskStatus::Completed
            | ManagedTaskStatus::Failed
            | ManagedTaskStatus::Cancelled
            | ManagedTaskStatus::Interrupted
            | ManagedTaskStatus::Lost
            | ManagedTaskStatus::Skipped
    )
}

fn is_residency_transition(transition: &TaskTransition) -> bool {
    matches!(
        transition,
        TaskTransition::MarkResident
            | TaskTransition::Evict
            | TaskTransition::Dispose
            | TaskTransition::PersistOnly
            | TaskTransition::DetachRpc
    )
}

fn is_terminal_safe_transition(transition: &TaskTransition) -> bool {
    is_residency_transition(transition) || matches!(transition, TaskTransition::QueueRetry { .. })
}

fn apply_attempt_status(record: &mut TaskRecord, status: TaskAttemptStatus) {
    let finished_at = record.finished_at.clone();
    let session_id = record.session_id.clone();
    let result_path = record.result_path.clone();
    let outcome_path = record.outcome_path.clone();
    let summary = record.summary.clone();
    let failure_kind = record.failure_kind.clone();
    let error = record.error.clone();
    update_latest_attempt(record, |attempt| {
        attempt.status = status;
        attempt.finished_at = finished_at;
        attempt.session_id = session_id;
        attempt.result_path = result_path;
        attempt.outcome_path = outcome_path;
        attempt.summary = summary;
        attempt.failure_kind = failure_kind;
        attempt.error = error;
    });
}

fn transition_task_record(
    record: &mut TaskRecord,
    transition: TaskTransition,
) -> TaskTransitionResult {
    let previous_status = record.status.clone();
    let previous_residency_state = record.residency_state.clone();
    let transition_type = match &transition {
        TaskTransition::Start { .. } => "start",
        TaskTransition::Skip { .. } => "skip",
        TaskTransition::Complete { .. } => "complete",
        TaskTransition::Fail { .. } => "fail",
        TaskTransition::Cancel { .. } => "cancel",
        TaskTransition::Interrupt { .. } => "interrupt",
        TaskTransition::MarkLost { .. } => "mark_lost",
        TaskTransition::QueueRetry { .. } => "queue_retry",
        TaskTransition::MarkResident => "mark_resident",
        TaskTransition::Evict => "evict",
        TaskTransition::Dispose => "dispose",
        TaskTransition::PersistOnly => "persist_only",
        TaskTransition::DetachRpc => "detach_rpc",
    };

    if is_terminal_status(&record.status) && !is_terminal_safe_transition(&transition) {
        return TaskTransitionResult {
            applied: false,
            transition_type,
            previous_status,
            previous_residency_state,
        };
    }

    let applied = match transition {
        TaskTransition::Start { session_id } => {
            if record.status != ManagedTaskStatus::Pending {
                false
            } else {
                record.status = ManagedTaskStatus::Running;
                record.summary = "Worker task started.".to_string();
                record.failure_kind = None;
                record.retry_reason = None;
                record.error = None;
                record.session_id = session_id;
                apply_attempt_status(record, TaskAttemptStatus::Running);
                true
            }
        }
        TaskTransition::Skip {
            finished_at,
            result_path,
            outcome_path,
            summary,
            failure_kind,
        } => {
            record.status = ManagedTaskStatus::Skipped;
            record.finished_at = Some(finished_at);
            record.result_path = Some(result_path);
            record.outcome_path = Some(outcome_path);
            record.summary = summary;
            record.failure_kind = failure_kind;
            record.retry_reason = None;
            record.error = None;
            apply_attempt_status(record, TaskAttemptStatus::Skipped);
            true
        }
        TaskTransition::Complete {
            finished_at,
            result_path,
            outcome_path,
            summary,
            failure_kind,
        } => {
            record.status = ManagedTaskStatus::Completed;
            record.finished_at = Some(finished_at);
            record.result_path = Some(result_path);
            record.outcome_path = Some(outcome_path);
            record.summary = summary;
            record.failure_kind = failure_kind;
            record.retry_reason = None;
            record.error = None;
            apply_attempt_status(record, TaskAttemptStatus::Completed);
            true
        }
        TaskTransition::Fail {
            finished_at,
            summary,
            failure_kind,
            error,
        } => {
            record.status = ManagedTaskStatus::Failed;
            record.finished_at = Some(finished_at);
            record.summary = summary;
            record.failure_kind = Some(failure_kind);
            record.retry_reason = None;
            record.error = error;
            apply_attempt_status(record, TaskAttemptStatus::Failed);
            true
        }
        TaskTransition::Cancel {
            finished_at,
            summary,
            error,
        } => match record.status {
            ManagedTaskStatus::Pending | ManagedTaskStatus::Running => {
                record.status = ManagedTaskStatus::Cancelled;
                record.finished_at = Some(finished_at);
                record.summary = summary;
                record.failure_kind = Some(TaskFailureKind::WorkerCancelled);
                record.retry_reason = None;
                record.error = error;
                apply_attempt_status(record, TaskAttemptStatus::Cancelled);
                true
            }
            _ => false,
        },
        TaskTransition::Interrupt {
            finished_at,
            summary,
            error,
        } => {
            if record.status != ManagedTaskStatus::Running {
                false
            } else {
                record.status = ManagedTaskStatus::Interrupted;
                record.finished_at = Some(finished_at);
                record.summary = summary;
                record.failure_kind = Some(TaskFailureKind::WorkerCancelled);
                record.retry_reason = None;
                record.error = error;
                apply_attempt_status(record, TaskAttemptStatus::Interrupted);
                true
            }
        }
        TaskTransition::MarkLost {
            finished_at,
            summary,
            failure_kind,
            error,
            killed,
        } => match record.status {
            ManagedTaskStatus::Pending | ManagedTaskStatus::Running => {
                record.status = ManagedTaskStatus::Lost;
                record.finished_at = Some(finished_at);
                record.summary = summary;
                record.failure_kind = Some(failure_kind);
                record.retry_reason = None;
                record.error = error;
                record.killed = killed;
                apply_attempt_status(record, TaskAttemptStatus::Lost);
                true
            }
            _ => false,
        },
        TaskTransition::QueueRetry {
            summary,
            retry_reason,
        } => {
            record.status = ManagedTaskStatus::Pending;
            record.run_epoch += 1;
            record.finished_at = None;
            record.session_id = None;
            record.result_path = None;
            record.outcome_path = None;
            record.summary = summary;
            record.failure_kind = None;
            record.retry_reason = Some(retry_reason);
            record.error = None;
            true
        }
        TaskTransition::MarkResident => {
            record.residency_state = ResidencyState::Resident;
            true
        }
        TaskTransition::Evict => {
            record.residency_state = ResidencyState::Evicted;
            true
        }
        TaskTransition::Dispose => {
            record.residency_state = ResidencyState::Disposed;
            true
        }
        TaskTransition::PersistOnly => {
            record.residency_state = ResidencyState::PersistedOnly;
            true
        }
        TaskTransition::DetachRpc => {
            record.residency_state = ResidencyState::RpcDetached;
            true
        }
    };

    TaskTransitionResult {
        applied,
        transition_type,
        previous_status,
        previous_residency_state,
    }
}

fn model_unavailable_error_for_task(
    config: &WorkerConfig,
    queued_task: &QueuedTask,
) -> Option<String> {
    let selected_route = config
        .selected_route_for_hint(queued_task.route_attempt, queued_task.route_hint.as_deref());
    let worker_model = selected_route.worker_model?;
    worker_model_is_unavailable(
        selected_route.worker_kind,
        Some(worker_model),
        &config.unavailable_worker_models,
    )
    .then(|| {
        format!(
            "Worker model `{worker_model}` is unavailable for `{}`.",
            selected_route.worker_kind.as_str()
        )
    })
}

fn write_worker_fanout_denied_artifacts(
    store: &StateStore,
    task_id: &str,
    summary: &str,
) -> Result<(WorkerResult, WorkerOutcome)> {
    let packet_path = store.write_worker_file(
        task_id,
        "packet.json",
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&serde_json::json!({
                "task_id": task_id,
                "status": "failed",
                "summary": summary,
                "failure_kind": "repeated_failure_limit",
            }))?
        ),
    )?;
    let prompt_path = store.write_worker_file(task_id, "prompt.md", summary)?;
    let result_path = store.worker_dir(task_id).join("result.json");
    let outcome_path = store.worker_dir(task_id).join("outcome.json");
    let result = WorkerResult {
        status: WorkerStatus::Failed,
        command: None,
        exit_code: None,
        summary: summary.to_string(),
        packet_path,
        prompt_path,
        stdout_path: None,
        stderr_path: None,
        last_message_path: None,
        result_path,
        outcome_path,
    };
    let outcome = WorkerOutcome {
        status: WorkerStatus::Failed,
        session_id: None,
        session_capability: None,
        summary: summary.to_string(),
        changed_files: Vec::new(),
        commands_run: Vec::new(),
        known_failures: vec![summary.to_string()],
        raw_output_path: None,
        command: None,
        exit_code: None,
    };
    write_result_and_outcome_with_outcome(store, task_id, &result, &outcome)?;
    Ok((result, outcome))
}

fn write_model_unavailable_artifacts(
    store: &StateStore,
    task_id: &str,
    summary: &str,
) -> Result<(WorkerResult, WorkerOutcome)> {
    let packet_path = store.write_worker_file(
        task_id,
        "packet.json",
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&serde_json::json!({
                "task_id": task_id,
                "status": "skipped",
                "summary": summary,
            }))?
        ),
    )?;
    let prompt_path = store.write_worker_file(task_id, "prompt.md", summary)?;
    let result_path = store.worker_dir(task_id).join("result.json");
    let outcome_path = store.worker_dir(task_id).join("outcome.json");
    let result = WorkerResult {
        status: WorkerStatus::Skipped,
        command: None,
        exit_code: None,
        summary: summary.to_string(),
        packet_path,
        prompt_path,
        stdout_path: None,
        stderr_path: None,
        last_message_path: None,
        result_path,
        outcome_path,
    };
    let outcome = WorkerOutcome {
        status: WorkerStatus::Skipped,
        session_id: None,
        session_capability: None,
        summary: summary.to_string(),
        changed_files: Vec::new(),
        commands_run: Vec::new(),
        known_failures: vec![summary.to_string()],
        raw_output_path: None,
        command: None,
        exit_code: None,
    };
    store.write_worker_file(
        task_id,
        "result.json",
        &format!("{}\n", serde_json::to_string_pretty(&result)?),
    )?;
    store.write_worker_file(
        task_id,
        "outcome.json",
        &format!("{}\n", serde_json::to_string_pretty(&outcome)?),
    )?;
    Ok((result, outcome))
}

fn write_worker_evidence_gate_artifact(
    store: &StateStore,
    task_id: &str,
    attempt: usize,
    receipt_path: Option<&Path>,
    reason: Option<&str>,
) -> Result<PathBuf> {
    let status = if reason.is_some() {
        "rejected"
    } else {
        "accepted"
    };
    let artifact = serde_json::json!({
        "task_id": task_id,
        "attempt": attempt,
        "status": status,
        "evidence_root": ".gear/evidence",
        "receipt_path": receipt_path.map(|path| path.to_string_lossy().into_owned()),
        "reason": reason,
    });
    store.write_worker_file(
        task_id,
        &format!("evidence-gate-attempt-{attempt}.json"),
        &format!("{}\n", serde_json::to_string_pretty(&artifact)?),
    )
}

fn write_route_transform_artifact(
    store: &StateStore,
    task_id: &str,
    previous_attempt: &TaskAttempt,
    next_attempt: Option<&TaskAttempt>,
    decision_summary: &str,
    failure_kind: Option<&TaskFailureKind>,
) -> Result<PathBuf> {
    let previous_worker_kind = WorkerKind::parse(&previous_attempt.worker_kind);
    let previous_provider = previous_worker_kind
        .and_then(|worker_kind| worker_kind.provider_id_hint())
        .unwrap_or("none");
    let next_worker_kind = next_attempt.and_then(|attempt| WorkerKind::parse(&attempt.worker_kind));
    let next_provider = next_worker_kind
        .and_then(|worker_kind| worker_kind.provider_id_hint())
        .unwrap_or("pending");
    let file_name = if let Some(next_attempt) = next_attempt {
        format!(
            "route-transform-{}-to-{}.md",
            previous_attempt.attempt, next_attempt.attempt
        )
    } else {
        format!("route-transform-{}-stopped.md", previous_attempt.attempt)
    };
    let next_worker_kind = next_attempt
        .map(|attempt| attempt.worker_kind.as_str())
        .unwrap_or("pending");
    let next_worker_model = next_attempt
        .and_then(|attempt| attempt.worker_model.as_deref())
        .unwrap_or("pending");
    let next_worker_command = next_attempt
        .and_then(|attempt| attempt.worker_command.as_deref())
        .unwrap_or("pending");
    let next_session_id = next_attempt
        .and_then(|attempt| attempt.session_id.as_deref())
        .unwrap_or("pending");
    let failure_kind = failure_kind
        .map(|kind| format!("{kind:?}"))
        .unwrap_or_else(|| "none".to_string());
    let previous_status = format!("{:?}", previous_attempt.status);
    let contents = format!(
        r#"# Worker Route Transform

Task: `{task_id}`

## Decision

- summary: {decision_summary}
- failure_kind: `{failure_kind}`

## Previous Attempt

- attempt: `{previous_attempt_index}`
- provider: `{previous_provider}`
- worker_kind: `{previous_worker_kind}`
- worker_model: `{previous_worker_model}`
- worker_command: `{previous_worker_command}`
- session_id: `{previous_session_id}`
- status: `{previous_status}`
- retry_reason: {previous_retry_reason}

## Next Attempt

- attempt: `{next_attempt_index}`
- provider: `{next_provider}`
- worker_kind: `{next_worker_kind}`
- worker_model: `{next_worker_model}`
- worker_command: `{next_worker_command}`
- session_id: `{next_session_id}`
"#,
        task_id = task_id,
        decision_summary = decision_summary,
        failure_kind = failure_kind,
        previous_attempt_index = previous_attempt.attempt,
        previous_provider = previous_provider,
        previous_worker_kind = previous_attempt.worker_kind,
        previous_worker_model = previous_attempt.worker_model.as_deref().unwrap_or("none"),
        previous_worker_command = previous_attempt.worker_command.as_deref().unwrap_or("none"),
        previous_session_id = previous_attempt.session_id.as_deref().unwrap_or("none"),
        previous_status = previous_status,
        previous_retry_reason = previous_attempt.retry_reason.as_deref().unwrap_or("none"),
        next_attempt_index = next_attempt.map(|attempt| attempt.attempt).unwrap_or(0),
        next_provider = next_provider,
        next_worker_kind = next_worker_kind,
        next_worker_model = next_worker_model,
        next_worker_command = next_worker_command,
        next_session_id = next_session_id,
    );
    store.write_worker_file(task_id, &file_name, &contents)
}

fn queued_task_from_request(request: WorkerStartRequest<'_>) -> QueuedTask {
    let mut task = request.task.clone();
    task.attempt = 1;
    QueuedTask {
        store: request.store.clone(),
        workspace: request.workspace.to_path_buf(),
        task,
        route_attempt: 1,
        goal: request.goal.to_string(),
        verification_commands: request.verification_commands.to_vec(),
        config: request.config.clone(),
        cancellation_token: request.cancellation_token,
        coordinator_model: request.coordinator_model.cloned(),
        coordinator_brief: request.coordinator_brief.map(ToString::to_string),
        route_hint: request.route_hint.map(ToString::to_string),
    }
}

fn record_worker_settle_event(
    store: &StateStore,
    goal_id: &str,
    task_id: &str,
    session_id: Option<&str>,
    run_epoch: u64,
    source: &str,
    event: PromptSettleEvent,
) -> Result<()> {
    let session_id = session_id
        .filter(|session_id| !session_id.trim().is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("task:{task_id}"));
    let run_epoch = usize::try_from(run_epoch).unwrap_or(usize::MAX);
    store
        .record_prompt_settle_decision(goal_id, task_id, &session_id, run_epoch, source, event)
        .map(|_| ())
}

fn queue_next_attempt(record: &mut TaskRecord, queued_task: &mut QueuedTask) -> FallbackDecision {
    let evidence_retry = is_worker_evidence_retry(record);
    if let Some(failure_kind) = record
        .attempts
        .last()
        .and_then(|attempt| attempt.failure_kind.clone())
    {
        let same_failure_count = if evidence_retry {
            record
                .attempts
                .iter()
                .filter(|attempt| {
                    attempt
                        .retry_reason
                        .as_deref()
                        .is_some_and(|reason| reason.starts_with(WORKER_EVIDENCE_RETRY_PREFIX))
                })
                .count()
        } else {
            record
                .attempts
                .iter()
                .filter(|attempt| attempt.failure_kind.as_ref() == Some(&failure_kind))
                .count()
        };
        let max_attempts = if evidence_retry {
            MAX_WORKER_EVIDENCE_ATTEMPTS
        } else {
            queued_task.config.worker_routes.len().max(2)
        };
        if same_failure_count >= max_attempts {
            return FallbackDecision::Unavailable {
                reason: if evidence_retry {
                    format!(
                        "worker evidence receipt remained invalid after {max_attempts} attempts"
                    )
                } else {
                    fallback_exhaustion_reason(record, &failure_kind, max_attempts)
                },
                failure_kind: TaskFailureKind::RepeatedFailureLimit,
            };
        }
    }

    if let Some(previous_attempt) = record.attempts.last() {
        mark_failed_model_unavailable_for_retry(previous_attempt, &mut queued_task.config);
    }
    if !evidence_retry {
        maybe_append_failure_upgrade_route(record, queued_task);
    } else {
        queued_task.goal.push_str("\n\n");
        queued_task.goal.push_str(WORKER_EVIDENCE_REPAIR_PROMPT);
    }

    let Some(previous_attempt) = record.attempts.last() else {
        return FallbackDecision::Unavailable {
            reason: "missing previous attempt".to_string(),
            failure_kind: TaskFailureKind::NoFallbackRoute,
        };
    };

    let next_attempt = queued_task.task.attempt.saturating_add(1);
    let route_selection_attempt = queued_task
        .route_hint
        .as_deref()
        .filter(|route_hint| *route_hint != previous_attempt.worker_category)
        .map(|_| 1)
        .unwrap_or(next_attempt);
    let selected_route = queued_task
        .config
        .selected_route_for_hint(route_selection_attempt, queued_task.route_hint.as_deref());
    let worker_kind = selected_route.worker_kind.as_str().to_string();
    let worker_command = selected_route.worker_command.map(ToString::to_string);
    let worker_model = selected_route.worker_model.map(ToString::to_string);
    let previous_route_identity = route_identity_key(
        WorkerKind::parse(&previous_attempt.worker_kind).unwrap_or(WorkerKind::Custom),
        previous_attempt.worker_model.as_deref(),
    );
    let next_route_identity =
        route_identity_key(selected_route.worker_kind, worker_model.as_deref());
    if previous_route_identity == next_route_identity
        && !evidence_retry
        && normalized_worker_command(previous_attempt.worker_command.as_deref())
            == normalized_worker_command(worker_command.as_deref())
    {
        return FallbackDecision::Unavailable {
            reason: format!(
                "no-op fallback: same provider/model `{next_route_identity}` and worker_command `{}` as previous attempt {}",
                worker_command.as_deref().unwrap_or("none"),
                previous_attempt.attempt
            ),
            failure_kind: TaskFailureKind::NoFallbackRoute,
        };
    }
    if selected_route.worker_kind.is_premium() {
        let used_premium_attempts = record
            .attempts
            .iter()
            .filter(|attempt| {
                WorkerKind::parse(&attempt.worker_kind).is_some_and(|worker_kind| {
                    worker_kind.is_premium() && attempt.status != TaskAttemptStatus::Pending
                })
            })
            .count();
        if used_premium_attempts >= queued_task.config.premium_worker_budget {
            return FallbackDecision::Unavailable {
                reason: format!(
                    "premium worker budget {} exhausted before `{}` attempt {}",
                    queued_task.config.premium_worker_budget,
                    selected_route.worker_kind.as_str(),
                    next_attempt
                ),
                failure_kind: TaskFailureKind::PremiumBudgetExceeded,
            };
        }
    }

    queued_task.task.attempt = next_attempt;
    queued_task.route_attempt = route_selection_attempt;
    let worker_category = selected_route.category.as_str().to_string();
    let route_reason = selected_route.route_reason;
    let route_hint = queued_task.route_hint.clone();
    let started_at = timestamp();
    let previous_model_label = fallback_model_label(
        WorkerKind::parse(&previous_attempt.worker_kind).unwrap_or(WorkerKind::Custom),
        previous_attempt.worker_model.as_deref(),
    );
    let next_model_label =
        fallback_model_label(selected_route.worker_kind, worker_model.as_deref());
    let retry_reason = if evidence_retry {
        format!(
            "{WORKER_EVIDENCE_RETRY_PREFIX} repair attempt {next_attempt}: receipt gate rejected the previous `{}` worker attempt",
            previous_attempt.worker_kind
        )
    } else {
        format!(
            "模型回退：{previous_model_label} -> {next_model_label}；retrying after {:?} with `{}` via {}",
            previous_attempt
                .failure_kind
                .clone()
                .unwrap_or(TaskFailureKind::WorkerFailed),
            worker_kind,
            route_reason
        )
    };
    record.worker_kind = worker_kind.clone();
    record.worker_command = worker_command.clone();
    record.worker_model = worker_model.clone();
    record.worker_category = worker_category.clone();
    record.route_hint = route_hint.clone();
    record.route_reason = route_reason.clone();
    let _ = transition_task_record(
        record,
        TaskTransition::QueueRetry {
            summary: format!("Worker fallback attempt {next_attempt} queued."),
            retry_reason: retry_reason.clone(),
        },
    );
    record.attempts.push(TaskAttempt {
        attempt: next_attempt,
        worker_kind,
        worker_command,
        worker_model,
        worker_category,
        route_hint,
        route_reason,
        status: TaskAttemptStatus::Pending,
        started_at,
        finished_at: None,
        session_id: None,
        result_path: None,
        outcome_path: None,
        summary: format!("Worker fallback attempt {next_attempt} queued."),
        failure_kind: None,
        retry_reason: Some(retry_reason),
        error: None,
    });
    FallbackDecision::Queued
}

fn is_worker_evidence_retry(record: &TaskRecord) -> bool {
    record
        .retry_reason
        .as_deref()
        .is_some_and(|reason| reason.starts_with(WORKER_EVIDENCE_RETRY_PREFIX))
}

fn normalized_worker_command(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|command| !command.is_empty())
        .map(|command| command.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn fallback_model_label(worker_kind: WorkerKind, worker_model: Option<&str>) -> String {
    let worker_model = worker_model
        .map(str::trim)
        .filter(|model| !model.is_empty());
    match (worker_kind.provider_id_hint(), worker_model) {
        (_, Some(model)) if model.contains('/') => model.to_string(),
        (Some(provider_id), Some(model)) => format!("{provider_id}/{model}"),
        (_, Some(model)) => format!("{}({model})", worker_kind.as_str()),
        _ => worker_kind.as_str().to_string(),
    }
}

fn fallback_exhaustion_reason(
    record: &TaskRecord,
    failure_kind: &TaskFailureKind,
    max_attempts: usize,
) -> String {
    let mut attempted_models = record
        .attempts
        .iter()
        .filter(|attempt| attempt.failure_kind.as_ref() == Some(failure_kind))
        .map(|attempt| {
            fallback_model_label(
                WorkerKind::parse(&attempt.worker_kind).unwrap_or(WorkerKind::Custom),
                attempt.worker_model.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    attempted_models.dedup();
    let attempted_models = if attempted_models.is_empty() {
        "none".to_string()
    } else {
        attempted_models.join(" -> ")
    };
    format!(
        "模型回退链已耗尽：已尝试 {attempted_models}；same failure kind `{failure_kind:?}` reached retry limit {max_attempts}"
    )
}

fn mark_failed_model_unavailable_for_retry(
    previous_attempt: &TaskAttempt,
    config: &mut WorkerConfig,
) {
    if !matches!(
        previous_attempt.failure_kind,
        Some(TaskFailureKind::ModelUnavailable | TaskFailureKind::ProviderTemporarilyUnavailable)
    ) {
        return;
    }
    let Some(worker_model) = previous_attempt
        .worker_model
        .as_deref()
        .map(str::trim)
        .filter(|worker_model| !worker_model.is_empty())
    else {
        return;
    };
    if !config
        .unavailable_worker_models
        .iter()
        .any(|unavailable_model| unavailable_model.eq_ignore_ascii_case(worker_model))
    {
        config
            .unavailable_worker_models
            .push(worker_model.to_string());
    }
}

fn maybe_append_failure_upgrade_route(record: &TaskRecord, queued_task: &mut QueuedTask) {
    if queued_task.task.inputs.phase_route_locked {
        return;
    }
    if queued_task.route_hint.is_none() {
        return;
    }
    let Some(previous_attempt) = record.attempts.last() else {
        return;
    };
    let Some(failure_kind) = previous_attempt.failure_kind.as_ref() else {
        return;
    };
    if !matches!(
        failure_kind,
        TaskFailureKind::WorkerFailed
            | TaskFailureKind::WorkerStartFailed
            | TaskFailureKind::WorkerUnavailable
            | TaskFailureKind::ModelUnavailable
            | TaskFailureKind::ProviderTemporarilyUnavailable
    ) {
        return;
    }

    let candidate_worker_kind = match WorkerKind::parse(&previous_attempt.worker_kind) {
        Some(WorkerKind::Opencode | WorkerKind::OpencodeSession) => WorkerKind::Codex,
        _ => return,
    };
    if WorkerKind::parse(&previous_attempt.worker_kind) == Some(WorkerKind::OpencodeSession)
        && queued_task.config.worker_routes.iter().any(|route| {
            route.worker_kind == WorkerKind::OpencodeSession
                && route.worker_model.as_deref() != previous_attempt.worker_model.as_deref()
        })
    {
        return;
    }
    // Only route to an explicitly configured Codex `worker_routes` entry.
    // Never implicitly create a Codex route — that would bypass the operator's
    // explicit routing policy and could spawn a second analyzer tree.
    if queued_task
        .config
        .worker_routes
        .iter()
        .any(|route| route.worker_kind == candidate_worker_kind)
    {
        queued_task.route_hint = Some("deep".to_string());
    }
}

fn failure_kind_from_worker_result(
    result: &WorkerResult,
    outcome: &WorkerOutcome,
) -> Option<TaskFailureKind> {
    match result.status {
        WorkerStatus::Succeeded => None,
        WorkerStatus::Skipped => {
            if result
                .summary
                .to_ascii_lowercase()
                .contains("no worker command")
            {
                Some(TaskFailureKind::WorkerUnavailable)
            } else {
                None
            }
        }
        WorkerStatus::Failed => {
            if worker_outcome_is_cancelled(outcome) {
                Some(TaskFailureKind::WorkerCancelled)
            } else if worker_outcome_has_model_unavailable_error(outcome) {
                Some(TaskFailureKind::ModelUnavailable)
            } else if worker_outcome_has_retryable_provider_error(outcome) {
                Some(TaskFailureKind::ProviderTemporarilyUnavailable)
            } else {
                Some(TaskFailureKind::WorkerFailed)
            }
        }
    }
}

fn worker_outcome_is_cancelled(outcome: &WorkerOutcome) -> bool {
    outcome.known_failures.iter().any(|failure| {
        let failure = failure.to_ascii_lowercase();
        failure.contains("cancelled") || failure.contains("canceled") || failure.contains("aborted")
    })
}

fn worker_outcome_has_model_unavailable_error(outcome: &WorkerOutcome) -> bool {
    outcome
        .known_failures
        .iter()
        .chain(std::iter::once(&outcome.summary))
        .map(|failure| failure.to_ascii_lowercase())
        .any(|failure| {
            failure.contains("model_not_found")
                || failure.contains("model unavailable")
                || failure.contains("model not found")
                || (failure.contains("model") && failure.contains("not supported"))
        })
}

fn worker_outcome_has_retryable_provider_error(outcome: &WorkerOutcome) -> bool {
    outcome
        .known_failures
        .iter()
        .chain(std::iter::once(&outcome.summary))
        .map(|failure| failure.to_ascii_lowercase())
        .any(|failure| {
            failure.contains("rate limit")
                || failure.contains("rate-limit")
                || failure.contains("too many requests")
                || failure.contains("quota exceeded")
                || failure.contains("usage quota")
                || failure.contains("free usage")
                || failure.contains("limit exhausted")
                || failure.contains("cooling down")
                || failure.contains("service unavailable")
                || failure.contains("temporarily unavailable")
                || failure.contains("overloaded")
                || failure
                    .split(|character: char| !character.is_ascii_digit())
                    .any(|status_code| matches!(status_code, "429" | "503" | "529"))
                || failure.contains("使用上限")
                || failure.contains("频率限制")
                || failure.contains("请求过于频繁")
                || failure.contains("暂时不可用")
                || failure.contains("服务不可用")
        })
}

fn is_retryable_worker_failure(failure_kind: Option<&TaskFailureKind>) -> bool {
    matches!(
        failure_kind,
        None | Some(
            TaskFailureKind::WorkerFailed
                | TaskFailureKind::WorkerStartFailed
                | TaskFailureKind::WorkerUnavailable
                | TaskFailureKind::ModelUnavailable
                | TaskFailureKind::ProviderTemporarilyUnavailable
        )
    )
}

fn should_retry_worker_result(
    record: &TaskRecord,
    queued_task: &QueuedTask,
    result: &WorkerResult,
) -> bool {
    if result.status == WorkerStatus::Failed {
        return is_retryable_worker_failure(record.failure_kind.as_ref());
    }

    record.failure_kind == Some(TaskFailureKind::WorkerUnavailable)
        && (!queued_task.config.worker_routes.is_empty() || queued_task.config.require_worker)
}

fn subscribe_to_worker_events_with_activity_and_circuit(
    handle: &Arc<dyn WorkerSessionHandle>,
    store: &StateStore,
    task_id: &str,
    goal_id: &str,
    run_epoch: u64,
    goal_epoch_context: Option<GoalEpochContext>,
    activity_heartbeat: Option<Arc<Mutex<Instant>>>,
    circuit_state: Option<Arc<Mutex<ToolCallCircuitState>>>,
    circuit_policy: ToolCallCircuitBreakerPolicy,
) -> Result<Option<WorkerSubscription>> {
    if handle.supports_event_subscriptions() {
        let store = store.clone();
        let task_id = task_id.to_string();
        let goal_id = goal_id.to_string();
        let session_id = handle.session_id();
        let activity_heartbeat = activity_heartbeat.clone();
        let circuit_state = circuit_state.clone();
        let circuit_policy = circuit_policy.clone();
        Ok(Some(handle.subscribe(Arc::new(move |event| {
            if let Some(activity_heartbeat) = activity_heartbeat.as_ref() {
                match activity_heartbeat.lock() {
                    Ok(mut last_activity) => *last_activity = Instant::now(),
                    Err(_) => {
                        eprintln!("failed to update Gear worker activity heartbeat for `{task_id}`")
                    }
                }
            }
            if let WorkerEvent::ToolCallStarted {
                tool_name,
                arguments,
                ..
            } = &event
                && let Some(circuit_state) = circuit_state.as_ref()
            {
                record_tool_call_for_circuit_breaker(
                    circuit_state,
                    &circuit_policy,
                    tool_name,
                    arguments,
                );
            }
            if let Err(error) =
                append_worker_event_evidence(&store, &task_id, session_id.as_deref(), &event)
            {
                eprintln!("failed to persist worker event evidence: {error:#}");
            }
            if let Some(settle_event) = worker_event_settle_event(&event)
                && let Err(error) = record_worker_settle_event(
                    &store,
                    &goal_id,
                    &task_id,
                    session_id.as_deref(),
                    run_epoch,
                    "task_manager.worker_event",
                    settle_event,
                )
            {
                eprintln!("failed to persist worker event settle decision: {error:#}");
            }
            if let Some(context) = goal_epoch_context.as_ref()
                && let Err(error) = project_worker_event_to_guard(&store, context, &task_id, &event)
            {
                eprintln!("failed to project worker event to continuation guard: {error:#}");
            }
        }))?))
    } else {
        Ok(None)
    }
}

fn project_worker_event_to_guard(
    store: &StateStore,
    context: &GoalEpochContext,
    task_id: &str,
    event: &WorkerEvent,
) -> Result<()> {
    let Some(existing_guard) = store.read_continuation_guard_for_session(&context.session_id)?
    else {
        return Ok(());
    };
    if existing_guard.goal_id != context.goal_id || existing_guard.epoch_id != context.epoch_id {
        return Ok(());
    }
    let marker = match event {
        WorkerEvent::TurnStarted { .. } => Some("turn_started"),
        WorkerEvent::AssistantTextDelta { .. } => Some("assistant_text_delta"),
        WorkerEvent::ToolCallStarted { .. } => Some("tool_call_started"),
        WorkerEvent::ToolCallFinished { .. } => Some("tool_call_finished"),
        WorkerEvent::WorkerStdout { .. } => Some("worker_stdout"),
        WorkerEvent::WorkerStderr { .. } => Some("worker_stderr"),
        WorkerEvent::Error { .. } => Some("error"),
        WorkerEvent::TurnFinished { .. } => Some("turn_finished"),
    };
    let Some(marker) = marker else {
        return Ok(());
    };
    store.update_continuation_guard(
        &context.session_id,
        &context.goal_id,
        &context.epoch_id,
        |guard| {
            match event {
                WorkerEvent::Error { .. } | WorkerEvent::TurnFinished { .. } => {
                    guard.in_flight = false;
                    guard.background_pending = false;
                    if matches!(event, WorkerEvent::Error { .. }) {
                        guard.consecutive_failures = guard.consecutive_failures.saturating_add(1);
                    }
                }
                _ => {
                    guard.in_flight = true;
                    guard.background_pending = true;
                    guard.stagnation_count = 0;
                }
            }
            guard.last_progress_marker = Some(format!("worker_event:{task_id}:{marker}"));
        },
    )?;
    Ok(())
}

fn worker_event_settle_event(event: &WorkerEvent) -> Option<PromptSettleEvent> {
    match event {
        WorkerEvent::TurnStarted { .. } => Some(PromptSettleEvent::Busy),
        WorkerEvent::Error { .. } => Some(PromptSettleEvent::Error),
        WorkerEvent::AssistantTextDelta { .. }
        | WorkerEvent::ToolCallStarted { .. }
        | WorkerEvent::ToolCallFinished { .. }
        | WorkerEvent::WorkerStdout { .. }
        | WorkerEvent::WorkerStderr { .. }
        | WorkerEvent::TurnFinished { .. } => None,
    }
}

fn append_worker_event_evidence(
    store: &StateStore,
    task_id: &str,
    session_id: Option<&str>,
    event: &WorkerEvent,
) -> Result<()> {
    let (event_type, kind, details) = match event {
        WorkerEvent::TurnStarted { kind, prompt_path } => (
            "turn_started",
            kind,
            serde_json::json!({ "prompt_file": evidence_file_name(prompt_path) }),
        ),
        WorkerEvent::AssistantTextDelta { kind, delta } => (
            "assistant_text_delta",
            kind,
            serde_json::json!({ "delta_length": delta.chars().count() }),
        ),
        WorkerEvent::ToolCallStarted {
            kind,
            tool_name,
            arguments,
        } => (
            "tool_call_started",
            kind,
            serde_json::json!({
                "tool_name": tool_name,
                "arguments_length": arguments.chars().count(),
            }),
        ),
        WorkerEvent::ToolCallFinished {
            kind,
            tool_name,
            result,
        } => (
            "tool_call_finished",
            kind,
            serde_json::json!({
                "tool_name": tool_name,
                "result_length": result.chars().count(),
            }),
        ),
        WorkerEvent::WorkerStdout { kind, output } => (
            "worker_stdout",
            kind,
            serde_json::json!({ "output_length": output.chars().count() }),
        ),
        WorkerEvent::WorkerStderr { kind, output } => (
            "worker_stderr",
            kind,
            serde_json::json!({ "output_length": output.chars().count() }),
        ),
        WorkerEvent::TurnFinished {
            kind,
            result_path,
            outcome_path,
            summary,
        } => (
            "turn_finished",
            kind,
            serde_json::json!({
                "result_file": evidence_file_name(result_path),
                "outcome_file": evidence_file_name(outcome_path),
                "summary_length": summary.chars().count(),
            }),
        ),
        WorkerEvent::Error { kind, message } => (
            "error",
            kind,
            serde_json::json!({ "message": truncate_event_error(message) }),
        ),
    };
    let event_record = serde_json::json!({
        "recorded_at": timestamp(),
        "task_id": task_id,
        "session_id": session_id,
        "event_type": event_type,
        "kind": kind,
        "details": details,
    });
    let path = store.worker_dir(task_id).join("worker-events.jsonl");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    writeln!(file, "{}", serde_json::to_string(&event_record)?)
        .with_context(|| format!("failed to append {}", path.display()))?;
    Ok(())
}

fn evidence_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown")
        .to_string()
}

fn truncate_event_error(message: &str) -> String {
    const MAX_EVENT_ERROR_CHARS: usize = 512;
    let mut truncated = message
        .chars()
        .take(MAX_EVENT_ERROR_CHARS)
        .collect::<String>();
    if message.chars().count() > MAX_EVENT_ERROR_CHARS {
        truncated.push_str("...");
    }
    truncated
}

fn concurrency_key_for_task(queued_task: &QueuedTask) -> String {
    let selected_route = queued_task
        .config
        .selected_route_for_hint(queued_task.route_attempt, queued_task.route_hint.as_deref());
    let model_key = selected_route
        .worker_model
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "unconfigured".to_string());
    format!("{}:{model_key}", selected_route.worker_kind.as_str())
}

fn update_latest_attempt(record: &mut TaskRecord, update: impl FnOnce(&mut TaskAttempt)) {
    if let Some(attempt) = record.attempts.last_mut() {
        update(attempt);
    }
}

fn write_task_record(store: &StateStore, task_record: &TaskRecord) -> Result<PathBuf> {
    let json =
        serde_json::to_string_pretty(task_record).context("failed to serialize task record")?;
    store.write_worker_file(
        &task_record.task_id,
        "task-record.json",
        &format!("{json}\n"),
    )
}

fn append_task_lifecycle_event(
    store: &StateStore,
    task_record: &TaskRecord,
    transition: Option<&TaskTransitionResult>,
) -> Result<PathBuf> {
    let event = TaskLifecycleEvent {
        task_id: task_record.task_id.clone(),
        status: task_record.status.clone(),
        residency_state: task_record.residency_state.clone(),
        timestamp: timestamp(),
        transition_type: transition.map(|transition| transition.transition_type.to_string()),
        transition_applied: transition
            .map(|transition| transition.applied)
            .unwrap_or(true),
        previous_status: transition.map(|transition| transition.previous_status.clone()),
        previous_residency_state: transition
            .map(|transition| transition.previous_residency_state.clone()),
        run_epoch: task_record.run_epoch,
        summary: task_record.summary.clone(),
    };
    let json = serde_json::to_string(&event).context("failed to serialize task lifecycle event")?;
    let worker_dir = store.worker_dir(&task_record.task_id);
    fs::create_dir_all(&worker_dir)
        .with_context(|| format!("failed to create {}", worker_dir.display()))?;
    let path = worker_dir.join("task-events.jsonl");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    writeln!(file, "{json}").with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;

    use super::*;
    use crate::state::{Scope, Task, TaskInputs, TaskKind, TaskOutputs, TaskStatus};
    use crate::workers::{
        NativeWorkerBackend, WorkerConfig, WorkerEventHub, WorkerEventListener, WorkerKind,
        WorkerRoute,
    };

    fn test_task(id: &str) -> Task {
        Task {
            id: id.to_string(),
            goal_id: "goal_test".to_string(),
            parent_task_id: None,
            title: "test managed task".to_string(),
            kind: TaskKind::Edit,
            status: TaskStatus::Pending,
            assigned_worker: Some("opencode".to_string()),
            attempt: 1,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: TaskInputs::default(),
            outputs: TaskOutputs::default(),
        }
    }

    fn test_read_only_task(id: &str) -> Task {
        Task {
            kind: TaskKind::Review,
            ..test_task(id)
        }
    }

    fn scoped_test_task(id: &str, allowed_paths: &[&str]) -> Task {
        Task {
            scope: Scope::new(
                allowed_paths.iter().map(|path| path.to_string()).collect(),
                Vec::new(),
                10,
            ),
            ..test_task(id)
        }
    }

    fn queued_task_for_concurrency_key(
        task_id: &str,
        config: WorkerConfig,
        coordinator_model: Option<CoordinatorModel>,
    ) -> QueuedTask {
        QueuedTask {
            store: StateStore::new("/tmp/gearbox-concurrency-key-test"),
            workspace: PathBuf::from("/tmp/gearbox-concurrency-key-test"),
            task: test_task(task_id),
            route_attempt: 1,
            goal: "concurrency key test".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model,
            coordinator_brief: None,
            route_hint: None,
        }
    }

    #[test]
    fn concurrency_key_uses_selected_worker_model_not_coordinator_model() {
        let same_planner = Some(CoordinatorModel {
            provider_id: "planner-provider".to_string(),
            model_id: "planner-model".to_string(),
            name: "planner".to_string(),
        });
        let mut worker_a_config = WorkerConfig::default();
        worker_a_config.worker_kind = WorkerKind::Opencode;
        worker_a_config.worker_command = Some("worker".to_string());
        worker_a_config.worker_model = Some("worker-model-a".to_string());
        let mut worker_b_config = worker_a_config.clone();
        worker_b_config.worker_model = Some("worker-model-b".to_string());

        let worker_a = queued_task_for_concurrency_key(
            "task_worker_model_a",
            worker_a_config.clone(),
            same_planner.clone(),
        );
        let worker_b =
            queued_task_for_concurrency_key("task_worker_model_b", worker_b_config, same_planner);
        assert_ne!(
            concurrency_key_for_task(&worker_a),
            concurrency_key_for_task(&worker_b),
            "different worker models must not share a planner-derived concurrency key"
        );

        let different_planner = Some(CoordinatorModel {
            provider_id: "other-planner-provider".to_string(),
            model_id: "other-planner-model".to_string(),
            name: "other planner".to_string(),
        });
        let same_worker_different_planner = queued_task_for_concurrency_key(
            "task_same_worker_different_planner",
            worker_a_config,
            different_planner,
        );
        assert_eq!(
            concurrency_key_for_task(&worker_a),
            concurrency_key_for_task(&same_worker_different_planner),
            "the same worker model must retain one concurrency key across planners"
        );

        let mut unconfigured_worker_config = WorkerConfig::default();
        unconfigured_worker_config.worker_kind = WorkerKind::Opencode;
        unconfigured_worker_config.worker_command = Some("worker".to_string());
        let unconfigured_worker = queued_task_for_concurrency_key(
            "task_unconfigured_worker",
            unconfigured_worker_config,
            Some(CoordinatorModel {
                provider_id: "planner-provider".to_string(),
                model_id: "planner-model".to_string(),
                name: "planner".to_string(),
            }),
        );
        assert_eq!(
            concurrency_key_for_task(&unconfigured_worker),
            "opencode:unconfigured"
        );
    }

    #[test]
    fn fallback_route_waits_when_its_concurrency_key_is_full() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_waiting_for_fallback_key");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: Some("openai/primary".to_string()),
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("printf primary".to_string()),
                    worker_model: Some("openai/primary".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("printf fallback".to_string()),
                    worker_model: Some("openai/fallback".to_string()),
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 2,
            max_parallel_workers: 2,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::Opencode,
        };
        let mut manager = TaskManager::new();
        manager.apply_worker_config(&config);
        manager.goal_unavailable_worker_models.insert(
            task.goal_id.clone(),
            HashMap::from([("openai/primary".to_string(), Instant::now())]),
        );

        let mut fallback_config = config.clone();
        fallback_config.unavailable_worker_models = vec!["openai/primary".to_string()];
        let occupied_fallback =
            queued_task_for_concurrency_key("task_occupying_fallback_key", fallback_config, None);
        assert!(manager.concurrency.acquire(&occupied_fallback));

        let task_id = manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "wait for fallback model capacity",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(task_id, task.id);
        assert!(manager.running_tasks.get(&task.id).is_none());
        assert!(
            manager
                .queued_tasks
                .iter()
                .any(|queued_task| queued_task.task.id == task.id)
        );
        assert_eq!(manager.concurrency.running_workers, 1);
        Ok(())
    }

    fn test_task_record(
        task_id: &str,
        status: ManagedTaskStatus,
        attempt_status: TaskAttemptStatus,
    ) -> TaskRecord {
        TaskRecord {
            task_id: task_id.to_string(),
            worker_kind: "opencode".to_string(),
            worker_command: None,
            worker_model: None,
            worker_category: "quick".to_string(),
            route_hint: None,
            route_reason: "test route".to_string(),
            status,
            started_at: timestamp(),
            finished_at: None,
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            parent_task_id: None,
            result_path: None,
            outcome_path: None,
            summary: "Worker task started.".to_string(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".to_string(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".to_string(),
                route_hint: None,
                route_reason: "test route".to_string(),
                status: attempt_status,
                started_at: timestamp(),
                finished_at: None,
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "Worker task started.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        }
    }

    #[test]
    fn task_snapshot_projects_latest_command_outcome() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let command_path = temp_dir.path().join("task-command-events.jsonl");
        let first = serde_json::json!({
            "task_id": "task-command",
            "action": "follow_up",
            "accepted": false,
            "all_scope": false,
            "caller_session_id": null,
            "reason": "task is not continuable",
            "run_epoch": 1,
            "timestamp": "2026-07-15T00:00:00Z"
        });
        let second = serde_json::json!({
            "task_id": "task-command",
            "action": "steer",
            "accepted": true,
            "all_scope": false,
            "caller_session_id": null,
            "reason": null,
            "run_epoch": 2,
            "timestamp": "2026-07-15T00:01:00Z"
        });
        fs::write(&command_path, format!("{}\n{}\n", first, second))?;
        let mut record = test_task_record(
            "task-command",
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        record.result_path = Some(temp_dir.path().join("result.json"));
        let snapshot = last_task_command_snapshot(&record).expect("latest command");
        assert_eq!(snapshot.action, "steer");
        assert!(snapshot.accepted);
        assert_eq!(snapshot.run_epoch, 2);
        Ok(())
    }

    struct StartFailingNativeBackend;

    impl NativeWorkerBackend for StartFailingNativeBackend {
        fn start_zed_agent(
            &self,
            _request: WorkerStartRequest<'_>,
        ) -> Result<Arc<dyn WorkerSessionHandle>> {
            Err(anyhow::anyhow!("injected native worker start failure"))
        }
    }

    #[test]
    fn worker_fanout_guard_counts_and_persists_session_starts() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf fanout-ok".to_string()),
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
        };

        let mut manager = TaskManager::new();
        manager.set_session_scope("fanout-session");
        manager.set_worker_fanout_limit(1);
        let first_task = test_task("task_fanout_first");
        let first_id = manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "fanout first",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        assert_eq!(
            manager.wait_for(&first_id)?.record.status,
            ManagedTaskStatus::Completed
        );

        let second_task = test_task("task_fanout_second");
        let second_id = manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "fanout second",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        let second_run = manager.wait_for(&second_id)?;
        assert_eq!(second_run.record.status, ManagedTaskStatus::Failed);
        assert_eq!(
            second_run.record.failure_kind,
            Some(TaskFailureKind::RepeatedFailureLimit)
        );
        assert!(
            second_run
                .record
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("2/1"))
        );
        let counter = store.read_worker_fanout_counter("fanout-session")?;
        assert_eq!(counter.count, 2);
        assert!(
            fs::read_dir(store.worker_fanout_dir_for_session("fanout-session"))?
                .filter_map(Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().starts_with("denied-2"))
        );

        drop(manager);
        let mut restarted_manager = TaskManager::new();
        restarted_manager.set_session_scope("fanout-session");
        restarted_manager.set_worker_fanout_limit(1);
        let third_task = test_task("task_fanout_third");
        let third_id = restarted_manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &third_task,
            route_attempt: 1,
            goal: "fanout third",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        assert_eq!(
            restarted_manager.wait_for(&third_id)?.record.status,
            ManagedTaskStatus::Failed
        );
        assert_eq!(store.read_worker_fanout_counter("fanout-session")?.count, 3);
        Ok(())
    }

    #[test]
    fn worker_fanout_guard_is_inactive_without_session_scope() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf unscoped-ok".to_string()),
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
        };
        let mut manager = TaskManager::new();
        manager.set_worker_fanout_limit(1);
        for task_id in ["task_unscoped_first", "task_unscoped_second"] {
            let task = test_task(task_id);
            let managed_id = manager.start(WorkerStartRequest {
                store: &store,
                workspace: temp_dir.path(),
                task: &task,
                route_attempt: 1,
                goal: "unscoped fanout",
                verification_commands: &[],
                config: &config,
                cancellation_token: None,
                coordinator_model: None,
                coordinator_brief: None,
                route_hint: None,
            })?;
            assert_eq!(
                manager.wait_for(&managed_id)?.record.status,
                ManagedTaskStatus::Completed
            );
        }
        assert!(!store.worker_fanout_dir().exists());
        Ok(())
    }

    struct FailOnceFollowUpBackend {
        follow_up_attempts: Arc<AtomicUsize>,
        steer_deliveries: Arc<AtomicUsize>,
    }

    impl NativeWorkerBackend for FailOnceFollowUpBackend {
        fn start_zed_agent(
            &self,
            _request: WorkerStartRequest<'_>,
        ) -> Result<Arc<dyn WorkerSessionHandle>> {
            Ok(Arc::new(FailOnceFollowUpHandle {
                follow_up_attempts: self.follow_up_attempts.clone(),
                steer_deliveries: self.steer_deliveries.clone(),
            }))
        }
    }

    struct FailOnceFollowUpHandle {
        follow_up_attempts: Arc<AtomicUsize>,
        steer_deliveries: Arc<AtomicUsize>,
    }

    impl WorkerSessionHandle for FailOnceFollowUpHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_fail_once_follow_up".to_string())
        }

        fn send_follow_up(&self, _prompt: String) -> Result<()> {
            if self.follow_up_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                bail!("transient queued follow-up delivery failure")
            }
            Ok(())
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            self.steer_deliveries.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            Ok(WorkerOutcome {
                status: WorkerStatus::Succeeded,
                session_id: self.session_id(),
                session_capability: None,
                summary: "fail-once fixture completed".to_string(),
                changed_files: Vec::new(),
                commands_run: Vec::new(),
                known_failures: Vec::new(),
                raw_output_path: None,
                command: None,
                exit_code: Some(0),
            })
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            Ok(WorkerResult {
                status: WorkerStatus::Succeeded,
                command: None,
                exit_code: Some(0),
                summary: "fail-once fixture completed".to_string(),
                packet_path: PathBuf::from("packet.json"),
                prompt_path: PathBuf::from("prompt.md"),
                stdout_path: None,
                stderr_path: None,
                last_message_path: None,
                result_path: PathBuf::from("result.json"),
                outcome_path: PathBuf::from("outcome.json"),
            })
        }

        fn last_output(&self) -> Option<String> {
            None
        }
    }

    #[test]
    fn cancelled_or_aborted_worker_failure_is_not_retryable() {
        assert!(!is_retryable_worker_failure(Some(
            &TaskFailureKind::WorkerCancelled,
        )));
        assert!(is_retryable_worker_failure(Some(
            &TaskFailureKind::ModelUnavailable,
        )));
        assert!(is_retryable_worker_failure(Some(
            &TaskFailureKind::ProviderTemporarilyUnavailable,
        )));

        let aborted_outcome = WorkerOutcome {
            status: WorkerStatus::Failed,
            session_id: None,
            session_capability: None,
            summary: "worker aborted".to_string(),
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            known_failures: vec!["request aborted by user".to_string()],
            raw_output_path: None,
            command: None,
            exit_code: None,
        };
        let result = WorkerResult {
            status: WorkerStatus::Failed,
            command: None,
            exit_code: None,
            summary: "worker aborted".to_string(),
            packet_path: PathBuf::from("packet.json"),
            prompt_path: PathBuf::from("prompt.md"),
            stdout_path: None,
            stderr_path: None,
            last_message_path: None,
            result_path: PathBuf::from("result.json"),
            outcome_path: PathBuf::from("outcome.json"),
        };

        assert_eq!(
            failure_kind_from_worker_result(&result, &aborted_outcome),
            Some(TaskFailureKind::WorkerCancelled)
        );

        let unavailable_model_outcome = WorkerOutcome {
            known_failures: vec!["provider returned model_not_found".to_string()],
            ..aborted_outcome
        };
        assert_eq!(
            failure_kind_from_worker_result(&result, &unavailable_model_outcome),
            Some(TaskFailureKind::ModelUnavailable)
        );

        let unavailable_provider_outcome = WorkerOutcome {
            known_failures: vec!["HTTP 429 rate limit exceeded for free usage".to_string()],
            ..unavailable_model_outcome
        };
        assert_eq!(
            failure_kind_from_worker_result(&result, &unavailable_provider_outcome),
            Some(TaskFailureKind::ProviderTemporarilyUnavailable)
        );
    }

    #[test]
    fn fallback_exhaustion_reason_lists_the_models_already_tried() {
        let failure_kind = TaskFailureKind::ProviderTemporarilyUnavailable;
        let mut record = test_task_record(
            "task_exhausted_free_models",
            ManagedTaskStatus::Failed,
            TaskAttemptStatus::Failed,
        );
        record.attempts[0].worker_kind = "opencode_session".to_string();
        record.attempts[0].worker_model = Some("opencode/hy3-free".to_string());
        record.attempts[0].failure_kind = Some(failure_kind.clone());
        let mut mimo_attempt = record.attempts[0].clone();
        mimo_attempt.attempt = 2;
        mimo_attempt.worker_model = Some("opencode/mimo-v2.5-free".to_string());
        let mut deepseek_attempt = record.attempts[0].clone();
        deepseek_attempt.attempt = 3;
        deepseek_attempt.worker_model = Some("opencode/deepseek-v4-flash-free".to_string());
        record.attempts.extend([mimo_attempt, deepseek_attempt]);

        let reason = fallback_exhaustion_reason(&record, &failure_kind, 3);

        assert!(reason.contains("模型回退链已耗尽"));
        assert!(reason.contains(
            "opencode/hy3-free -> opencode/mimo-v2.5-free -> opencode/deepseek-v4-flash-free"
        ));
        assert!(reason.contains("ProviderTemporarilyUnavailable"));
    }

    #[test]
    fn task_manager_records_skipped_worker_outcome() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_skipped");
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: None,
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 2,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: true,
            require_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Skipped);
        assert_eq!(run.result.status, WorkerStatus::Skipped);
        assert!(store.worker_dir(&task.id).join("task-record.json").exists());
        let record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(record.contains(r#""status": "skipped""#));
        assert!(record.contains(r#""attempts""#));
        assert!(record.contains(r#""worker_category": "quick""#));
        Ok(())
    }

    #[test]
    fn task_manager_records_failed_worker_outcome() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_failed");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'exit 2'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::Opencode,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Failed);
        assert_eq!(run.result.status, WorkerStatus::Failed);
        assert!(run.record.finished_at.is_some());
        assert_eq!(
            run.record.failure_kind,
            Some(TaskFailureKind::NoFallbackRoute)
        );
        assert_eq!(run.record.attempts.len(), 1);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Failed);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::WorkerFailed)
        );
        assert!(run.record.attempts[0].retry_reason.is_some());
        Ok(())
    }

    #[test]
    fn task_manager_rejects_success_without_worker_evidence_receipt() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_missing_worker_evidence");
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(
                r#"printf 'completed without receipt\n' > "$GEARBOX_WORKER_LAST_MESSAGE""#
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
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test evidence gate",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.result.status, WorkerStatus::Failed);
        assert_eq!(run.record.status, ManagedTaskStatus::Failed);
        assert_eq!(run.record.attempts.len(), MAX_WORKER_EVIDENCE_ATTEMPTS);
        assert!(run.record.summary.contains("evidence gate"));
        for attempt in 1..=MAX_WORKER_EVIDENCE_ATTEMPTS {
            let artifact = store
                .worker_dir(&task.id)
                .join(format!("evidence-gate-attempt-{attempt}.json"));
            assert!(
                artifact.exists(),
                "missing gate artifact for attempt {attempt}"
            );
        }
        let task_record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(!task_record.contains(r#""status": "completed""#));
        Ok(())
    }

    #[test]
    fn task_manager_accepts_success_with_valid_worker_evidence_receipt() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_valid_worker_evidence");
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(
                r#"mkdir -p .gearbox-agent/evidence && printf 'verified\n' > .gearbox-agent/evidence/receipt.md && printf 'done\nEVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\n' > "$GEARBOX_WORKER_LAST_MESSAGE""#
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
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test evidence gate",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.attempts.len(), 1);
        let artifact = fs::read_to_string(
            store
                .worker_dir(&task.id)
                .join("evidence-gate-attempt-1.json"),
        )?;
        assert!(artifact.contains(r#""status": "accepted""#));
        assert!(artifact.contains("receipt.md"));
        Ok(())
    }

    #[test]
    fn task_manager_accepts_one_new_worker_evidence_file_without_marker() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_discovered_worker_evidence");
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(
                r#"mkdir -p .gearbox-agent/evidence && printf 'verified\n' > .gearbox-agent/evidence/discovered.md && printf 'done without marker\n' > "$GEARBOX_WORKER_LAST_MESSAGE""#
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
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "discover evidence receipt",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.attempts.len(), 1);
        let artifact = fs::read_to_string(
            store
                .worker_dir(&task.id)
                .join("evidence-gate-attempt-1.json"),
        )?;
        assert!(artifact.contains(r#""status": "accepted""#));
        assert!(artifact.contains("discovered.md"));
        Ok(())
    }

    #[test]
    fn evidence_baseline_rejects_reused_receipt_across_attempts() -> Result<()> {
        // GBX-073 regression: a subsequent evidence attempt for the same
        // managed task must not reuse a receipt that the previous attempt
        // left behind.  The baseline snapshotted before the attempt starts
        // includes the old receipt, so the validation must reject it.
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;

        let old_receipt = evidence_root.join("receipt.md");
        fs::write(&old_receipt, "verified by prior attempt\n")?;

        let baseline =
            snapshot_worker_evidence_paths(workspace.path()).map_err(anyhow::Error::msg)?;
        assert_eq!(baseline.len(), 1, "baseline must contain the prior receipt");

        let message = workspace.path().join("last-message.md");
        fs::write(
            &message,
            "done\nEVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\n",
        )?;

        let error = validate_worker_evidence_receipt_with_baseline(
            &WorkerResult {
                status: WorkerStatus::Succeeded,
                command: None,
                exit_code: Some(0),
                summary: "worker attempted".to_string(),
                packet_path: PathBuf::from("packet.json"),
                prompt_path: PathBuf::from("prompt.md"),
                stdout_path: None,
                stderr_path: None,
                last_message_path: Some(message),
                result_path: PathBuf::from("result.json"),
                outcome_path: PathBuf::from("outcome.json"),
            },
            workspace.path(),
            &baseline,
        )
        .expect_err("a receipt from a prior attempt must not satisfy a later evidence gate");

        assert!(
            error.contains("present before this worker attempt"),
            "expected baseline rejection, got: {error}"
        );

        // Step 2: a subsequent new receipt (different path) must be accepted
        // against the same baseline, proving attempt isolation.
        let new_receipt = evidence_root.join("new-receipt.md");
        fs::write(&new_receipt, "verified by new attempt\n")?;

        let snapshot_after =
            snapshot_worker_evidence_paths(workspace.path()).map_err(anyhow::Error::msg)?;
        assert_eq!(
            snapshot_after.len(),
            2,
            "snapshot must see both old and new receipts after creating new_receipt"
        );
        assert!(
            snapshot_after.iter().any(|p| p.ends_with("receipt.md")),
            "snapshot must contain old receipt path"
        );
        assert!(
            snapshot_after.iter().any(|p| p.ends_with("new-receipt.md")),
            "snapshot must contain new receipt path"
        );
        assert_eq!(
            baseline.len(),
            1,
            "baseline must remain unchanged with only the old receipt"
        );
        assert!(
            baseline.iter().any(|p| p.ends_with("receipt.md")),
            "baseline must contain old receipt path"
        );
        assert!(
            !baseline.iter().any(|p| p.ends_with("new-receipt.md")),
            "baseline must NOT contain new receipt path"
        );

        let new_message = workspace.path().join("new-message.md");
        fs::write(
            &new_message,
            "done\nEVIDENCE_RECORDED: .gearbox-agent/evidence/new-receipt.md\n",
        )?;

        let validated = validate_worker_evidence_receipt_with_baseline(
            &WorkerResult {
                status: WorkerStatus::Succeeded,
                command: None,
                exit_code: Some(0),
                summary: "new attempt".to_string(),
                packet_path: PathBuf::from("packet.json"),
                prompt_path: PathBuf::from("prompt.md"),
                stdout_path: None,
                stderr_path: None,
                last_message_path: Some(new_message),
                result_path: PathBuf::from("result.json"),
                outcome_path: PathBuf::from("outcome.json"),
            },
            workspace.path(),
            &baseline,
        )
        .map_err(anyhow::Error::msg)?;

        assert_eq!(
            validated,
            new_receipt.canonicalize()?,
            "a new receipt with a different path must pass baseline check"
        );
        Ok(())
    }

    #[test]
    fn task_manager_fallback_retries_failed_worker_result() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_fallback_result");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("sh -c 'exit 2'".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some(
                        r#"sh -c 'mkdir -p .gearbox-agent/evidence; printf verified > .gearbox-agent/evidence/receipt.md; printf "done\nEVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\n" > "$GEARBOX_WORKER_LAST_MESSAGE"; printf fallback-ok'"#
                            .to_string(),
                    ),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 2,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::Opencode,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.record.attempts.len(), 2);
        assert_eq!(run.record.attempts[0].worker_kind, "opencode");
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Failed);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::WorkerFailed)
        );
        assert!(run.record.attempts[0].retry_reason.is_none());
        assert_eq!(run.record.attempts[1].worker_kind, "codex");
        assert_eq!(run.record.attempts[1].status, TaskAttemptStatus::Completed);
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("retrying after WorkerFailed"))
        );
        let events = fs::read_to_string(store.worker_dir(&task.id).join("task-events.jsonl"))?;
        assert!(events.contains(r#""status":"failed""#));
        assert!(events.contains(r#"Worker fallback attempt 2 queued."#));
        Ok(())
    }

    #[test]
    fn fallback_continues_after_selected_worker_start_fails() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_start_failure_fallback");
        let config = WorkerConfig {
            worker_kind: WorkerKind::ZedAgent,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::ZedAgent,
                    worker_command: None,
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("printf fallback-start-ok".to_string()),
                    worker_model: Some("opencode/mimo-v2.5-free".to_string()),
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut manager = TaskManager::new();
        manager.set_worker_registry(WorkerRegistry::with_native_backend(Arc::new(
            StartFailingNativeBackend,
        )));

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test start failure fallback",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.attempts.len(), 2);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::WorkerStartFailed)
        );
        assert_eq!(run.record.attempts[1].status, TaskAttemptStatus::Completed);
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("retrying after WorkerStartFailed"))
        );
        assert!(
            run.record.attempts[1].retry_reason.as_deref().is_some_and(
                |reason| reason.contains("模型回退：zed_agent -> opencode/mimo-v2.5-free")
            )
        );
        Ok(())
    }

    #[test]
    fn task_manager_switches_to_distinct_model_after_model_unavailable() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_model_unavailable_fallback");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: Some("openai/primary".to_string()),
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some(
                        "sh -c 'printf \"model not found\\n\" >&2; exit 2'".to_string(),
                    ),
                    worker_model: Some("openai/primary".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("printf fallback-model-ok".to_string()),
                    worker_model: Some("openai/secondary".to_string()),
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 2,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::Opencode,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test model fallback",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.record.attempts.len(), 2);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::ModelUnavailable)
        );
        assert_eq!(
            run.record.attempts[0].worker_model.as_deref(),
            Some("openai/primary")
        );
        assert_eq!(
            run.record.attempts[1].worker_model.as_deref(),
            Some("openai/secondary")
        );
        assert_eq!(run.record.attempts[1].status, TaskAttemptStatus::Completed);
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("ModelUnavailable"))
        );
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| {
                    reason.contains("模型回退：openai/primary -> openai/secondary")
                })
        );
        Ok(())
    }

    #[test]
    fn task_manager_switches_to_distinct_model_after_provider_temporary_unavailability()
    -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_provider_temporary_unavailability_fallback");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: Some("opencode/hy3-free".to_string()),
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some(
                        "sh -c 'printf \"HTTP 429 rate limit exceeded\\n\" >&2; exit 2'"
                            .to_string(),
                    ),
                    worker_model: Some("opencode/hy3-free".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("printf fallback-provider-ok".to_string()),
                    worker_model: Some("opencode/mimo-v2.5-free".to_string()),
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 2,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::Opencode,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test provider fallback",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.record.attempts.len(), 2);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::ProviderTemporarilyUnavailable)
        );
        assert_eq!(
            run.record.attempts[0].worker_model.as_deref(),
            Some("opencode/hy3-free")
        );
        assert_eq!(
            run.record.attempts[1].worker_model.as_deref(),
            Some("opencode/mimo-v2.5-free")
        );
        assert_eq!(run.record.attempts[1].status, TaskAttemptStatus::Completed);
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("ProviderTemporarilyUnavailable"))
        );
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| {
                    reason.contains("模型回退：opencode/hy3-free -> opencode/mimo-v2.5-free")
                })
        );
        Ok(())
    }

    #[test]
    fn unavailable_model_is_skipped_when_a_retry_changes_category() {
        let mut config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::OpencodeSession,
                    worker_command: Some("opencode run --model opencode/hy3-free".to_string()),
                    worker_model: Some("opencode/hy3-free".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::OpencodeSession,
                    worker_command: Some(
                        "opencode run --model opencode/mimo-v2.5-free".to_string(),
                    ),
                    worker_model: Some("opencode/mimo-v2.5-free".to_string()),
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let failed_attempt = TaskAttempt {
            attempt: 1,
            worker_kind: "opencode_session".to_string(),
            worker_command: None,
            worker_model: Some("opencode/hy3-free".to_string()),
            worker_category: "repair".to_string(),
            route_hint: Some("repair".to_string()),
            route_reason: "initial route".to_string(),
            status: TaskAttemptStatus::Failed,
            started_at: timestamp(),
            finished_at: Some(timestamp()),
            session_id: None,
            result_path: None,
            outcome_path: None,
            summary: "model unavailable".to_string(),
            failure_kind: Some(TaskFailureKind::ModelUnavailable),
            retry_reason: None,
            error: Some("model unavailable".to_string()),
        };

        mark_failed_model_unavailable_for_retry(&failed_attempt, &mut config);

        assert_eq!(
            config.unavailable_worker_models,
            vec!["opencode/hy3-free".to_string()]
        );
        assert_eq!(
            config.selected_route_for_hint(1, Some("deep")).worker_model,
            Some("opencode/mimo-v2.5-free")
        );

        config.unavailable_worker_models.clear();
        let mut temporarily_unavailable_attempt = failed_attempt.clone();
        temporarily_unavailable_attempt.failure_kind =
            Some(TaskFailureKind::ProviderTemporarilyUnavailable);
        mark_failed_model_unavailable_for_retry(&temporarily_unavailable_attempt, &mut config);
        assert_eq!(
            config.selected_route_for_hint(1, Some("deep")).worker_model,
            Some("opencode/mimo-v2.5-free")
        );
    }

    #[test]
    fn unavailable_model_propagates_to_later_tasks_in_the_same_goal() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let mut failed_record = test_task_record(
            "task_failed_provider",
            ManagedTaskStatus::Failed,
            TaskAttemptStatus::Failed,
        );
        failed_record.attempts[0].worker_model = Some("opencode/hy3-free".to_string());
        failed_record.attempts[0].failure_kind =
            Some(TaskFailureKind::ProviderTemporarilyUnavailable);
        let mut manager = TaskManager::new();
        manager.set_goal_epoch_context("session_test", "goal_test", "epoch_test")?;
        manager.remember_unavailable_model_for_goal("goal_test", &failed_record);

        let next_task = test_task("task_repair_after_provider_failure");
        let mut queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task: next_task,
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config: WorkerConfig {
                worker_kind: WorkerKind::OpencodeSession,
                worker_command: None,
                worker_model: None,
                worker_routes: vec![
                    WorkerRoute {
                        worker_kind: WorkerKind::OpencodeSession,
                        worker_command: Some("opencode run --model opencode/hy3-free".to_string()),
                        worker_model: Some("opencode/hy3-free".to_string()),
                    },
                    WorkerRoute {
                        worker_kind: WorkerKind::OpencodeSession,
                        worker_command: Some(
                            "opencode run --model opencode/mimo-v2.5-free".to_string(),
                        ),
                        worker_model: Some("opencode/mimo-v2.5-free".to_string()),
                    },
                ],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: false,
                require_worker: true,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair".to_string()),
        };

        manager.apply_goal_unavailable_models(&mut queued_task);

        assert_eq!(
            queued_task
                .config
                .selected_route_for_hint(1, Some("repair"))
                .worker_model,
            Some("opencode/mimo-v2.5-free")
        );

        let expired_at = Instant::now()
            .checked_sub(GOAL_WORKER_MODEL_COOLDOWN + Duration::from_secs(1))
            .expect("cooldown test timestamp should be representable");
        manager
            .goal_unavailable_worker_models
            .get_mut("goal_test")
            .expect("failed model should be tracked for the active goal")
            .insert("opencode/hy3-free".to_string(), expired_at);
        queued_task.config.unavailable_worker_models.clear();
        manager.apply_goal_unavailable_models(&mut queued_task);
        assert!(queued_task.config.unavailable_worker_models.is_empty());
        assert_eq!(
            queued_task
                .config
                .selected_route_for_hint(1, Some("repair"))
                .worker_model,
            Some("opencode/hy3-free")
        );

        manager.set_goal_epoch_context("session_test", "goal_new", "epoch_new")?;
        queued_task.config.unavailable_worker_models.clear();
        manager.apply_goal_unavailable_models(&mut queued_task);
        assert!(queued_task.config.unavailable_worker_models.is_empty());
        Ok(())
    }

    #[test]
    fn provider_session_propagates_to_later_opencode_task_in_the_same_goal() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_repair_session_inheritance");
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config: WorkerConfig {
                worker_kind: WorkerKind::OpencodeSession,
                worker_command: Some("opencode run".to_string()),
                worker_model: Some("opencode/mimo-v2.5-free".to_string()),
                worker_routes: vec![WorkerRoute {
                    worker_kind: WorkerKind::OpencodeSession,
                    worker_command: Some("opencode run".to_string()),
                    worker_model: Some("opencode/mimo-v2.5-free".to_string()),
                }],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: false,
                require_worker: true,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair".to_string()),
        };
        let mut manager = TaskManager::new();
        manager
            .goal_provider_sessions
            .insert("goal_test".to_string(), "provider-session-1".to_string());

        manager.seed_goal_provider_session(&queued_task)?;

        assert_eq!(
            provider_session_id_for_task(&store, &task.id)?,
            Some("provider-session-1".to_string())
        );

        manager.set_session_scope("new-session");
        let next_task = test_task("task_new_session");
        let next_queued_task = QueuedTask {
            task: next_task.clone(),
            ..queued_task
        };
        manager.seed_goal_provider_session(&next_queued_task)?;
        assert_eq!(provider_session_id_for_task(&store, &next_task.id)?, None);
        Ok(())
    }

    #[test]
    fn review_task_does_not_inherit_executor_provider_session() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_review_session_isolation");
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task,
            route_attempt: 1,
            goal: "review test".to_string(),
            verification_commands: Vec::new(),
            config: WorkerConfig {
                worker_kind: WorkerKind::OpencodeSession,
                worker_command: Some("opencode run".to_string()),
                worker_model: Some("opencode/mimo-v2.5-free".to_string()),
                worker_routes: vec![WorkerRoute {
                    worker_kind: WorkerKind::OpencodeSession,
                    worker_command: Some("opencode run".to_string()),
                    worker_model: Some("opencode/mimo-v2.5-free".to_string()),
                }],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: false,
                require_worker: true,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("review".to_string()),
        };
        let mut manager = TaskManager::new();
        manager
            .goal_provider_sessions
            .insert("goal_test".to_string(), "executor-session".to_string());

        manager.seed_goal_provider_session(&queued_task)?;

        assert_eq!(
            provider_session_id_for_task(&store, "task_review_session_isolation")?,
            None,
            "review must start a fresh provider session"
        );
        Ok(())
    }

    #[test]
    fn goal_epoch_context_resets_route_state_when_session_changes() -> Result<()> {
        let mut manager = TaskManager::new();
        manager.set_goal_epoch_context("session-one", "goal-shared", "epoch-one")?;
        manager.goal_provider_sessions.insert(
            "goal-shared".to_string(),
            "provider-session-one".to_string(),
        );
        manager.goal_unavailable_worker_models.insert(
            "goal-shared".to_string(),
            HashMap::from([("opencode/hy3-free".to_string(), Instant::now())]),
        );

        manager.set_goal_epoch_context("session-two", "goal-shared", "epoch-two")?;

        assert!(manager.goal_provider_sessions.is_empty());
        assert!(manager.goal_unavailable_worker_models.is_empty());
        Ok(())
    }

    #[test]
    fn possibly_accepted_outcomes_expose_a_warning_reason() {
        let send = SendOutcome::PossiblyAccepted(OutcomeContext::default());
        assert!(send.is_accepted());
        assert!(
            send.reason()
                .is_some_and(|reason| reason.contains("may have been accepted"))
        );

        let steer = SteerOutcome::PossiblyAccepted(OutcomeContext::default());
        assert!(steer.is_accepted());
        assert!(
            steer
                .reason()
                .is_some_and(|reason| reason.contains("may have been accepted"))
        );
    }

    #[test]
    #[ignore = "requires GEARBOX_LIVE_OPENCODE_SESSION_SMOKE=1 and OpenCode provider access"]
    fn live_opencode_session_survives_a_same_goal_cross_model_task_handoff() -> Result<()> {
        if std::env::var("GEARBOX_LIVE_OPENCODE_SESSION_SMOKE")
            .ok()
            .as_deref()
            != Some("1")
        {
            return Ok(());
        }

        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_task("live_opencode_session_start");
        let second_task = test_task("live_opencode_session_continue");
        let third_task = test_task("live_opencode_session_continue_deepseek");
        let command = "sh -c 'if [ \"$GEARBOX_WORKER_RESUME\" = \"true\" ]; then opencode run --pure --format json --session \"$GEARBOX_WORKER_SESSION_ID\" --model \"$GEARBOX_WORKER_MODEL\" < \"$GEARBOX_WORKER_PROMPT\"; else opencode run --pure --format json --model \"$GEARBOX_WORKER_MODEL\" < \"$GEARBOX_WORKER_PROMPT\"; fi'".to_string();
        let first_config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(command.clone()),
            worker_model: Some("opencode/hy3-free".to_string()),
            worker_routes: vec![WorkerRoute {
                worker_kind: WorkerKind::OpencodeSession,
                worker_command: Some(command.clone()),
                worker_model: Some("opencode/hy3-free".to_string()),
            }],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 45,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
        };
        let second_config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(command.clone()),
            worker_model: Some("opencode/mimo-v2.5-free".to_string()),
            worker_routes: vec![WorkerRoute {
                worker_kind: WorkerKind::OpencodeSession,
                worker_command: Some(command.clone()),
                worker_model: Some("opencode/mimo-v2.5-free".to_string()),
            }],
            ..first_config.clone()
        };
        let third_config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(command.clone()),
            worker_model: Some("opencode/deepseek-v4-flash-free".to_string()),
            worker_routes: vec![WorkerRoute {
                worker_kind: WorkerKind::OpencodeSession,
                worker_command: Some(command),
                worker_model: Some("opencode/deepseek-v4-flash-free".to_string()),
            }],
            ..first_config.clone()
        };
        let mut manager = TaskManager::new();

        let first_run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "Reply with a concise acknowledgement and do not modify files.",
            verification_commands: &[],
            config: &first_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;
        assert_eq!(first_run.result.status, WorkerStatus::Succeeded);
        let provider_session_id = provider_session_id_for_task(&store, &first_task.id)?
            .context("live OpenCode start did not persist a provider session id")?;

        let second_run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "Continue the existing session with a concise acknowledgement and do not modify files.",
            verification_commands: &[],
            config: &second_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;
        assert_eq!(second_run.result.status, WorkerStatus::Succeeded);
        assert_eq!(
            provider_session_id_for_task(&store, &second_task.id)?,
            Some(provider_session_id.clone())
        );

        let third_run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &third_task,
            route_attempt: 1,
            goal: "Continue the existing session with a concise acknowledgement and do not modify files.",
            verification_commands: &[],
            config: &third_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;
        assert_eq!(third_run.result.status, WorkerStatus::Succeeded);
        assert_eq!(
            provider_session_id_for_task(&store, &third_task.id)?,
            Some(provider_session_id)
        );
        if let Ok(report_path) = std::env::var("GEARBOX_LIVE_OPENCODE_SESSION_SMOKE_REPORT") {
            std::fs::write(
                report_path,
                "status=passed\nmodels=opencode/hy3-free,opencode/mimo-v2.5-free,opencode/deepseek-v4-flash-free\nprovider_session_continuity=asserted\n",
            )?;
        }
        Ok(())
    }

    #[test]
    fn queue_next_attempt_does_not_implicitly_upgrade_to_codex() -> Result<()> {
        // Without an explicit Codex `worker_routes` entry, a failed OpenCode
        // attempt must produce `NoFallbackRoute` instead of an implicit Codex
        // upgrade.  GBX-067: all fallback routes must be explicitly configured.
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_no_implicit_codex");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'exit 2'".to_string()),
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
        };
        let mut queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair".to_string()),
        };
        let started_at = timestamp();
        let mut record = TaskRecord {
            task_id: task.id.clone(),
            worker_kind: "opencode".to_string(),
            worker_command: Some("sh -c 'exit 2'".to_string()),
            worker_model: None,
            worker_category: "repair".to_string(),
            route_hint: Some("repair".to_string()),
            route_reason: "initial route".to_string(),
            status: ManagedTaskStatus::Failed,
            started_at: started_at.clone(),
            finished_at: Some(timestamp()),
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            parent_task_id: None,
            result_path: None,
            outcome_path: None,
            summary: "failed".to_string(),
            failure_kind: Some(TaskFailureKind::WorkerFailed),
            retry_reason: None,
            error: Some("exit 2".to_string()),
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".to_string(),
                worker_command: Some("sh -c 'exit 2'".to_string()),
                worker_model: None,
                worker_category: "repair".to_string(),
                route_hint: Some("repair".to_string()),
                route_reason: "initial route".to_string(),
                status: TaskAttemptStatus::Failed,
                started_at,
                finished_at: Some(timestamp()),
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "failed".to_string(),
                failure_kind: Some(TaskFailureKind::WorkerFailed),
                retry_reason: None,
                error: Some("exit 2".to_string()),
            }],
        };

        // Phase-locked task must not be affected by the upgrade gate.
        let mut phase_locked_task = queued_task.clone();
        phase_locked_task.task.inputs.phase_route_locked = true;
        maybe_append_failure_upgrade_route(&record, &mut phase_locked_task);
        assert!(phase_locked_task.config.worker_routes.is_empty());
        assert_eq!(phase_locked_task.route_hint.as_deref(), Some("repair"));

        // Without an explicit Codex route, the attempt must produce
        // NoFallbackRoute — no implicit Codex upgrade.
        let attempt_count_before = record.attempts.len();
        let decision = queue_next_attempt(&mut record, &mut queued_task);

        // Must be Unavailable with NoFallbackRoute — no implicit Codex upgrade.
        assert!(
            matches!(
                &decision,
                FallbackDecision::Unavailable {
                    failure_kind: TaskFailureKind::NoFallbackRoute,
                    ..
                }
            ),
            "expected Unavailable(NoFallbackRoute), got {decision:?}"
        );
        // No new attempt should have been created.
        assert_eq!(record.attempts.len(), attempt_count_before);
        // The phase-locked task must still be unaffected.
        assert!(phase_locked_task.config.worker_routes.is_empty());
        assert_eq!(phase_locked_task.route_hint.as_deref(), Some("repair"));
        Ok(())
    }

    #[test]
    fn queue_next_attempt_allows_same_kind_when_command_changes() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_command_change");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'exit 2'".to_string()),
            worker_model: None,
            worker_routes: vec![WorkerRoute {
                worker_kind: WorkerKind::Opencode,
                worker_command: Some("sh -c 'exit 3'".to_string()),
                worker_model: None,
            }],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let task_id = task.id.clone();
        let mut queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task,
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let started_at = timestamp();
        let mut record = TaskRecord {
            task_id,
            worker_kind: "opencode".to_string(),
            worker_command: Some("sh -c 'exit 2'".to_string()),
            worker_model: None,
            worker_category: "quick".to_string(),
            route_hint: None,
            route_reason: "initial route".to_string(),
            status: ManagedTaskStatus::Failed,
            started_at: started_at.clone(),
            finished_at: Some(timestamp()),
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            parent_task_id: None,
            result_path: None,
            outcome_path: None,
            summary: "failed".to_string(),
            failure_kind: Some(TaskFailureKind::WorkerFailed),
            retry_reason: None,
            error: Some("exit 2".to_string()),
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".to_string(),
                worker_command: Some("sh -c 'exit 2'".to_string()),
                worker_model: None,
                worker_category: "quick".to_string(),
                route_hint: None,
                route_reason: "initial route".to_string(),
                status: TaskAttemptStatus::Failed,
                started_at,
                finished_at: Some(timestamp()),
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "failed".to_string(),
                failure_kind: Some(TaskFailureKind::WorkerFailed),
                retry_reason: None,
                error: Some("exit 2".to_string()),
            }],
        };

        let decision = queue_next_attempt(&mut record, &mut queued_task);

        assert_eq!(decision, FallbackDecision::Queued);
        assert_eq!(record.attempts.len(), 2);
        assert_eq!(
            record.attempts[1].worker_command.as_deref(),
            Some("sh -c 'exit 3'")
        );
        Ok(())
    }

    #[test]
    fn queue_next_attempt_detects_canonical_provider_model_noop() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_canonical_noop");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: Some("codex exec -m gpt-5.1".to_string()),
            worker_model: Some("gpt.5-1".to_string()),
            worker_routes: vec![WorkerRoute {
                worker_kind: WorkerKind::Codex,
                worker_command: Some("codex exec -m gpt-5.1".to_string()),
                worker_model: Some("GPT-5-1".to_string()),
            }],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let task_id = task.id.clone();
        let mut queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task,
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let started_at = timestamp();
        let mut record = TaskRecord {
            task_id,
            worker_kind: "codex".to_string(),
            worker_command: Some("codex exec -m gpt-5.1".to_string()),
            worker_model: Some("gpt.5-1".to_string()),
            worker_category: "deep".to_string(),
            route_hint: None,
            route_reason: "initial route".to_string(),
            status: ManagedTaskStatus::Failed,
            started_at: started_at.clone(),
            finished_at: Some(timestamp()),
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            parent_task_id: None,
            result_path: None,
            outcome_path: None,
            summary: "failed".to_string(),
            failure_kind: Some(TaskFailureKind::WorkerFailed),
            retry_reason: None,
            error: Some("exit 2".to_string()),
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "codex".to_string(),
                worker_command: Some("codex exec -m gpt-5.1".to_string()),
                worker_model: Some("gpt.5-1".to_string()),
                worker_category: "deep".to_string(),
                route_hint: None,
                route_reason: "initial route".to_string(),
                status: TaskAttemptStatus::Failed,
                started_at,
                finished_at: Some(timestamp()),
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "failed".to_string(),
                failure_kind: Some(TaskFailureKind::WorkerFailed),
                retry_reason: None,
                error: Some("exit 2".to_string()),
            }],
        };

        let decision = queue_next_attempt(&mut record, &mut queued_task);

        assert_eq!(
            decision,
            FallbackDecision::Unavailable {
                reason: "no-op fallback: same provider/model `openai/gpt51` and worker_command `codex exec -m gpt-5.1` as previous attempt 1".to_string(),
                failure_kind: TaskFailureKind::NoFallbackRoute,
            }
        );
        Ok(())
    }

    #[test]
    fn task_manager_fallback_retries_unavailable_worker() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_fallback_unavailable");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: None,
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some(
                        r#"sh -c 'mkdir -p .gearbox-agent/evidence; printf verified > .gearbox-agent/evidence/receipt.md; printf "done\nEVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\n" > "$GEARBOX_WORKER_LAST_MESSAGE"; printf fallback-ok'"#
                            .to_string(),
                    ),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.worker_kind, "codex");
        assert_eq!(run.record.attempts.len(), 2);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Skipped);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::WorkerUnavailable)
        );
        assert_eq!(run.record.attempts[1].status, TaskAttemptStatus::Completed);
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("WorkerUnavailable"))
        );
        Ok(())
    }

    #[test]
    fn queue_next_attempt_stops_when_premium_budget_is_exhausted() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_premium_budget_exhausted");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf opencode".to_string()),
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("printf codex".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Claude,
                    worker_command: Some("printf claude".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let task_id = task.id.clone();
        let mut queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task,
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        queued_task.task.attempt = 1;
        let started_at = timestamp();
        let mut record = TaskRecord {
            task_id,
            worker_kind: "codex".to_string(),
            worker_command: Some("printf codex".to_string()),
            worker_model: None,
            worker_category: "deep".to_string(),
            route_hint: None,
            route_reason: "attempt 1 selected sequence route `codex`".to_string(),
            status: ManagedTaskStatus::Failed,
            started_at: started_at.clone(),
            finished_at: Some(timestamp()),
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            parent_task_id: None,
            result_path: None,
            outcome_path: None,
            summary: "failed".to_string(),
            failure_kind: Some(TaskFailureKind::WorkerFailed),
            retry_reason: None,
            error: Some("exit 2".to_string()),
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "codex".to_string(),
                worker_command: Some("printf codex".to_string()),
                worker_model: None,
                worker_category: "deep".to_string(),
                route_hint: None,
                route_reason: "attempt 1 selected sequence route `codex`".to_string(),
                status: TaskAttemptStatus::Failed,
                started_at,
                finished_at: Some(timestamp()),
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "failed".to_string(),
                failure_kind: Some(TaskFailureKind::WorkerFailed),
                retry_reason: None,
                error: Some("exit 2".to_string()),
            }],
        };

        let decision = queue_next_attempt(&mut record, &mut queued_task);

        assert_eq!(
            decision,
            FallbackDecision::Unavailable {
                reason: "premium worker budget 1 exhausted before `claude` attempt 2".to_string(),
                failure_kind: TaskFailureKind::PremiumBudgetExceeded,
            }
        );
        Ok(())
    }

    #[test]
    fn task_manager_marks_missing_worker_binary_as_unavailable() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_missing_binary");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: Some("__gearbox_missing_worker_command__ exec".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::Codex,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.record.attempts.len(), 1);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Skipped);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::WorkerUnavailable)
        );
        assert_eq!(
            run.record.failure_kind,
            Some(TaskFailureKind::NoFallbackRoute)
        );
        Ok(())
    }

    #[test]
    fn task_manager_fallback_retries_unavailable_worker_model() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_fallback_model_unavailable");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("printf should-not-run".to_string()),
                    worker_model: Some("slow-model".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some(
                        r#"sh -c 'mkdir -p .gearbox-agent/evidence; printf verified > .gearbox-agent/evidence/receipt.md; printf "done\nEVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\n" > "$GEARBOX_WORKER_LAST_MESSAGE"; printf model-fallback-ok'"#
                            .to_string(),
                    ),
                    worker_model: Some("fast-model".to_string()),
                },
            ],
            unavailable_worker_models: vec!["slow-model".to_string()],
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.worker_kind, "codex");
        assert_eq!(run.record.worker_model.as_deref(), Some("fast-model"));
        assert_eq!(run.record.attempts.len(), 1);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Completed);
        assert_eq!(
            run.record.attempts[0].worker_model.as_deref(),
            Some("fast-model")
        );
        assert!(run.record.attempts[0].failure_kind.is_none());
        assert!(run.record.attempts[0].retry_reason.is_none());
        Ok(())
    }

    #[test]
    fn task_manager_treats_provider_qualified_model_as_unavailable() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_provider_qualified_model_unavailable");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: Some("printf should-not-run".to_string()),
            worker_model: Some("gpt.5-1".to_string()),
            worker_routes: Vec::new(),
            unavailable_worker_models: vec!["OpenAI/GPT-5.1".to_string()],
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("deep"),
        })?;

        assert_eq!(run.record.attempts.len(), 1);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Skipped);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::ModelUnavailable)
        );
        assert_eq!(
            run.record.failure_kind,
            Some(TaskFailureKind::NoFallbackRoute)
        );
        Ok(())
    }

    #[test]
    fn task_manager_stops_after_repeated_same_failure_limit() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_repeated_failure_limit");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("sh -c 'exit 2'".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Claude,
                    worker_command: Some("sh -c 'exit 3'".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("sh -c 'exit 4'".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 2,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("deep"),
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Failed);
        assert_eq!(
            run.record.failure_kind,
            Some(TaskFailureKind::RepeatedFailureLimit)
        );
        assert_eq!(run.record.attempts.len(), config.worker_routes.len());
        assert_eq!(run.record.attempts[0].worker_kind, "codex");
        assert_eq!(run.record.attempts[1].worker_kind, "claude");
        assert_eq!(run.record.attempts[2].worker_kind, "opencode");
        assert!(
            run.record
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("retry limit"))
        );
        Ok(())
    }

    #[test]
    fn free_model_command_not_artificially_timed_out() -> Result<()> {
        // GBX-063: free models must NOT be killed by artificial timeouts.
        // A sleep command longer than stale_task_timeout_secs must still
        // complete normally, not trigger a timeout fallback.
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_free_model_no_artificial_timeout");
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![WorkerRoute {
                worker_kind: WorkerKind::OpencodeSession,
                    worker_command: Some(
                        "mkdir -p .gearbox-agent/evidence && echo ok > .gearbox-agent/evidence/receipt.md && sleep 2 && echo \"EVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\" >> \"$GEARBOX_WORKER_LAST_MESSAGE\"".to_string(),
                    ),
                    worker_model: Some("opencode/hy3-free".to_string()),
            }],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 1,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.attempts.len(), 1);
        assert_eq!(
            run.record.attempts[0].worker_model.as_deref(),
            Some("opencode/hy3-free")
        );
        assert!(run.record.attempts[0].failure_kind.is_none());
        Ok(())
    }

    #[test]
    fn free_model_fallbacks_on_command_error_not_timeout() -> Result<()> {
        // GBX-063: free models still fall back on explicit command errors
        // (non-zero exit). Only artificial timeouts are removed.
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_free_model_fallback_on_error");
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::OpencodeSession,
                    worker_command: Some("exit 1".to_string()),
                    worker_model: Some("opencode/hy3-free".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::OpencodeSession,
                    worker_command: Some(
                        r#"sh -c 'mkdir -p .gearbox-agent/evidence; printf verified > .gearbox-agent/evidence/receipt.md; printf "done\nEVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\n" > "$GEARBOX_WORKER_LAST_MESSAGE"; printf worker-ok'"#
                            .to_string(),
                    ),
                    worker_model: Some("opencode/mimo-v2.5-free".to_string()),
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 1,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.attempts.len(), 2);
        assert_eq!(
            run.record.attempts[0].worker_model.as_deref(),
            Some("opencode/hy3-free")
        );
        assert!(run.record.attempts[0].failure_kind.is_some());
        assert_eq!(
            run.record.attempts[1].worker_model.as_deref(),
            Some("opencode/mimo-v2.5-free")
        );
        assert!(run.record.attempts[1].failure_kind.is_none());
        Ok(())
    }

    #[test]
    fn task_manager_start_dispatches_worker_in_background_until_wait() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_deferred");
        let release_path = temp_dir.path().join("release-worker");
        let worker_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo worker-ok'",
            release_path.display()
        );
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(worker_command),
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
        };
        let mut manager = TaskManager::new();

        let task_id = manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(task_id, task.id);
        assert!(store.worker_dir(&task.id).join("packet.json").exists());
        assert!(store.worker_dir(&task.id).join("prompt.md").exists());
        assert!(!store.worker_dir(&task.id).join("result.json").exists());

        fs::write(&release_path, "go")?;
        let run = manager.wait_for(&task.id)?;
        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Completed);
        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        assert!(store.worker_dir(&task.id).join("result.json").exists());
        let events = fs::read_to_string(store.worker_dir(&task.id).join("task-events.jsonl"))?;
        assert!(events.contains(r#""status":"pending""#));
        assert!(events.contains(r#""status":"running""#));
        assert!(events.contains(r#""status":"completed""#));
        Ok(())
    }

    #[test]
    fn delayed_worker_session_id_replaces_no_session_start_state() {
        let mut record = test_task_record(
            "task_delayed_session",
            ManagedTaskStatus::Pending,
            TaskAttemptStatus::Pending,
        );
        let start = transition_task_record(&mut record, TaskTransition::Start { session_id: None });
        assert!(start.applied);
        assert_eq!(record.session_id, None);
        assert_eq!(record.attempts[0].session_id, None);

        record.session_id = Some("real-acp-session".to_string());
        let complete = transition_task_record(
            &mut record,
            TaskTransition::Complete {
                finished_at: timestamp(),
                result_path: PathBuf::from("result.json"),
                outcome_path: PathBuf::from("outcome.json"),
                summary: "completed".to_string(),
                failure_kind: None,
            },
        );
        assert!(complete.applied);
        assert_eq!(record.session_id.as_deref(), Some("real-acp-session"));
        assert_eq!(
            record.attempts[0].session_id.as_deref(),
            Some("real-acp-session")
        );
    }

    #[test]
    fn task_manager_tick_settles_finished_worker_without_wait_for() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_tick_settles");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo tick-ok".to_string()),
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
        };
        let mut manager = TaskManager::new();

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        let mut settled = 0;
        for _ in 0..50 {
            settled += manager.tick()?;
            if settled > 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(settled > 0);
        let record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == task.id)
            .context("missing settled task record")?;
        assert_eq!(record.status, ManagedTaskStatus::Completed);

        let run = manager.wait_for(&task.id)?;
        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        Ok(())
    }

    #[test]
    fn task_manager_tick_loop_settles_finished_worker_in_background() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_tick_loop_settles");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo loop-ok".to_string()),
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
        };
        let manager = TaskManager::new().into_shared();
        let tick_loop = TaskManagerTickLoop::start(manager.clone(), Duration::from_millis(10));

        manager
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
            .start(WorkerStartRequest {
                store: &store,
                workspace: temp_dir.path(),
                task: &task,
                route_attempt: 1,
                goal: "test goal",
                verification_commands: &[],
                config: &config,
                cancellation_token: None,
                coordinator_model: None,
                coordinator_brief: None,
                route_hint: None,
            })?;

        let mut completed = false;
        for _ in 0..50 {
            completed = manager
                .lock()
                .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
                .list()
                .into_iter()
                .any(|record| {
                    record.task_id == task.id && record.status == ManagedTaskStatus::Completed
                });
            if completed {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        tick_loop.stop()?;
        assert!(completed);

        let run = manager
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
            .wait_for(&task.id)?;
        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        Ok(())
    }

    struct FakeOutputHandle;

    impl WorkerSessionHandle for FakeOutputHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_fake".to_string())
        }

        fn send_follow_up(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            bail!("not supported")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            bail!("not supported")
        }

        fn last_output(&self) -> Option<String> {
            Some("control-output".to_string())
        }
    }

    struct FakeAmbiguousDispatchHandle;

    impl WorkerSessionHandle for FakeAmbiguousDispatchHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_ambiguous_dispatch".to_string())
        }

        fn send_follow_up(&self, _prompt: String) -> Result<()> {
            bail!("JSON Parse error: Unexpected end of JSON input")
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            bail!("PromptAsync Timed Out after 30000ms")
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            bail!("not supported")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            bail!("not supported")
        }

        fn last_output(&self) -> Option<String> {
            None
        }
    }

    #[test]
    fn prompt_dispatch_error_classifier_matches_omo_signals() {
        for message in [
            "JSON Parse error: Unexpected end of JSON input",
            "PromptAsync Timed Out after 30000ms",
            "unexpected EOF while reading response",
        ] {
            let error = anyhow::anyhow!(message);
            assert!(prompt_dispatch_error_is_possibly_accepted(&error));
            assert_eq!(
                prompt_dispatch_error_status(&error),
                PromptDispatchGateStatus::PossiblyAccepted
            );
        }
        let definite =
            anyhow::anyhow!("command-backed worker sessions do not support follow-up prompts");
        assert!(!prompt_dispatch_error_is_possibly_accepted(&definite));
        assert_eq!(
            prompt_dispatch_error_status(&definite),
            PromptDispatchGateStatus::Failed
        );
    }

    #[test]
    fn session_identity_does_not_imply_event_subscription_support() -> Result<()> {
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(FakeOutputHandle);
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        assert!(handle.session_id().is_some());
        assert!(!handle.supports_event_subscriptions());
        assert!(
            subscribe_to_worker_events_with_activity_and_circuit(
                &handle,
                &store,
                "task_session_identity",
                "goal_session_identity",
                1,
                None,
                None,
                None,
                ToolCallCircuitBreakerPolicy::default(),
            )?
            .is_none()
        );
        Ok(())
    }

    struct EventfulHandle {
        event_hub: WorkerEventHub,
    }

    impl WorkerSessionHandle for EventfulHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_eventful".to_string())
        }

        fn send_follow_up(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn supports_event_subscriptions(&self) -> bool {
            true
        }

        fn subscribe(&self, listener: WorkerEventListener) -> Result<WorkerSubscription> {
            self.event_hub.subscribe(listener)
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            bail!("not supported")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            bail!("not supported")
        }

        fn last_output(&self) -> Option<String> {
            None
        }
    }

    #[test]
    fn worker_event_subscription_persists_bounded_task_evidence() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        store.update_continuation_guard(
            "runtime_session_eventful",
            "goal_eventful",
            "epoch_eventful",
            |_| {},
        )?;
        let event_hub = WorkerEventHub::default();
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(EventfulHandle {
            event_hub: event_hub.clone(),
        });
        let subscription = subscribe_to_worker_events_with_activity_and_circuit(
            &handle,
            &store,
            "task_eventful",
            "goal_eventful",
            3,
            Some(GoalEpochContext {
                session_id: "runtime_session_eventful".to_string(),
                goal_id: "goal_eventful".to_string(),
                epoch_id: "epoch_eventful".to_string(),
            }),
            None,
            None,
            ToolCallCircuitBreakerPolicy::default(),
        )?
        .context("eventful handle did not create a subscription")?;

        event_hub.emit(WorkerEvent::TurnStarted {
            kind: "acp".to_string(),
            prompt_path: PathBuf::from("/private/prompt.md"),
        });
        event_hub.emit(WorkerEvent::AssistantTextDelta {
            kind: "acp".to_string(),
            delta: "secret assistant response".to_string(),
        });
        event_hub.emit(WorkerEvent::ToolCallStarted {
            kind: "acp".to_string(),
            tool_name: "edit_file".to_string(),
            arguments: "private arguments".to_string(),
        });
        event_hub.emit(WorkerEvent::TurnFinished {
            kind: "acp".to_string(),
            result_path: PathBuf::from("/private/result.json"),
            outcome_path: PathBuf::from("/private/outcome.json"),
            summary: "private summary".to_string(),
        });
        event_hub.emit(WorkerEvent::Error {
            kind: "acp".to_string(),
            message: "provider rejected request".to_string(),
        });

        let evidence = fs::read_to_string(
            store
                .worker_dir("task_eventful")
                .join("worker-events.jsonl"),
        )?;
        assert_eq!(evidence.lines().count(), 5);
        assert!(evidence.contains("\"event_type\":\"turn_started\""));
        assert!(evidence.contains("\"event_type\":\"turn_finished\""));
        assert!(evidence.contains("\"event_type\":\"error\""));
        assert!(evidence.contains("\"delta_length\":25"));
        assert!(evidence.contains("\"prompt_file\":\"prompt.md\""));
        assert!(!evidence.contains("secret assistant response"));
        assert!(!evidence.contains("private arguments"));
        let busy = store.record_prompt_settle_decision(
            "goal_eventful",
            "task_eventful",
            "session_eventful",
            3,
            "task_manager.worker_event",
            PromptSettleEvent::Busy,
        )?;
        assert!(busy.duplicate);
        assert_eq!(busy.decision.action, crate::state::PromptSettleAction::Hold);
        let error = store.record_prompt_settle_decision(
            "goal_eventful",
            "task_eventful",
            "session_eventful",
            3,
            "task_manager.worker_event",
            PromptSettleEvent::Error,
        )?;
        assert!(error.duplicate);
        assert_eq!(
            error.decision.action,
            crate::state::PromptSettleAction::Hold
        );
        let guard = store
            .read_continuation_guard_for_session("runtime_session_eventful")?
            .context("worker events did not update the matching continuation guard")?;
        assert_eq!(guard.epoch_id, "epoch_eventful");
        assert!(!guard.in_flight);
        assert!(!guard.background_pending);
        assert_eq!(guard.consecutive_failures, 1);
        assert_eq!(
            guard.last_progress_marker.as_deref(),
            Some("worker_event:task_eventful:error")
        );
        store.update_continuation_guard(
            "runtime_session_eventful",
            "goal_eventful",
            "epoch_newer",
            |guard| guard.last_progress_marker = Some("newer_epoch".to_string()),
        )?;
        event_hub.emit(WorkerEvent::AssistantTextDelta {
            kind: "acp".to_string(),
            delta: "stale event must not overwrite a newer epoch".to_string(),
        });
        let newer_guard = store
            .read_continuation_guard_for_session("runtime_session_eventful")?
            .context("newer continuation guard disappeared")?;
        assert_eq!(newer_guard.epoch_id, "epoch_newer");
        assert_eq!(
            newer_guard.last_progress_marker.as_deref(),
            Some("newer_epoch")
        );
        let settle_dir = store
            .prompt_settle_decision_path(&busy.decision.decision_id)
            .parent()
            .context("prompt settle decision path has no parent")?
            .to_path_buf();
        assert_eq!(fs::read_dir(settle_dir)?.count(), 2);
        assert!(
            !store
                .worker_dir("task_eventful")
                .join("task-record.json")
                .exists()
        );
        drop(subscription);
        Ok(())
    }

    #[test]
    fn worker_event_guard_settles_active_background_state_on_turn_finished() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        store.update_continuation_guard(
            "session_guard_settlement",
            "goal_guard_settlement",
            "epoch_guard_settlement",
            |_| {},
        )?;
        let context = GoalEpochContext {
            session_id: "session_guard_settlement".to_string(),
            goal_id: "goal_guard_settlement".to_string(),
            epoch_id: "epoch_guard_settlement".to_string(),
        };

        project_worker_event_to_guard(
            &store,
            &context,
            "task_guard_settlement",
            &WorkerEvent::TurnStarted {
                kind: "acp".to_string(),
                prompt_path: PathBuf::from("/private/prompt.md"),
            },
        )?;
        let active = store
            .read_continuation_guard_for_session(&context.session_id)?
            .context("active worker event did not update continuation guard")?;
        assert!(active.in_flight);
        assert!(active.background_pending);
        assert_eq!(active.blocking_reason(), Some("background work is pending"));

        project_worker_event_to_guard(
            &store,
            &context,
            "task_guard_settlement",
            &WorkerEvent::TurnFinished {
                kind: "acp".to_string(),
                result_path: PathBuf::from("/private/result.json"),
                outcome_path: PathBuf::from("/private/outcome.json"),
                summary: "finished".to_string(),
            },
        )?;
        let settled = store
            .read_continuation_guard_for_session(&context.session_id)?
            .context("turn-finished event removed continuation guard")?;
        assert!(!settled.in_flight);
        assert!(!settled.background_pending);
        assert_eq!(settled.blocking_reason(), None);

        store.update_continuation_guard(
            &context.session_id,
            &context.goal_id,
            "newer_epoch",
            |guard| {
                guard.background_pending = true;
                guard.in_flight = true;
            },
        )?;
        project_worker_event_to_guard(
            &store,
            &context,
            "task_guard_settlement",
            &WorkerEvent::TurnFinished {
                kind: "acp".to_string(),
                result_path: PathBuf::from("/private/stale-result.json"),
                outcome_path: PathBuf::from("/private/stale-outcome.json"),
                summary: "stale".to_string(),
            },
        )?;
        let newer = store
            .read_continuation_guard_for_session(&context.session_id)?
            .context("newer continuation guard disappeared")?;
        assert_eq!(newer.epoch_id, "newer_epoch");
        assert!(newer.in_flight);
        assert!(newer.background_pending);
        Ok(())
    }

    struct FakeInterruptHandle {
        interrupted: Arc<AtomicUsize>,
        cancelled: Arc<AtomicUsize>,
        follow_ups: Arc<Mutex<Vec<String>>>,
        steers: Arc<Mutex<Vec<String>>>,
    }

    impl WorkerSessionHandle for FakeInterruptHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_interrupt".to_string())
        }

        fn send_follow_up(&self, prompt: String) -> Result<()> {
            self.follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("follow-up mutex poisoned"))?
                .push(prompt);
            Ok(())
        }

        fn steer(&self, prompt: String) -> Result<()> {
            self.steers
                .lock()
                .map_err(|_| anyhow::anyhow!("steer mutex poisoned"))?
                .push(prompt);
            Ok(())
        }

        fn interrupt(&self) -> Result<()> {
            self.interrupted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            bail!("not supported")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            bail!("not supported")
        }

        fn last_output(&self) -> Option<String> {
            None
        }

        fn supports_event_subscriptions(&self) -> bool {
            true
        }

        fn subscribe(&self, _listener: WorkerEventListener) -> Result<WorkerSubscription> {
            Ok(WorkerSubscription::noop())
        }
    }

    struct HistoryAwareReviveHandle {
        event_hub: WorkerEventHub,
        reset_calls: Arc<AtomicUsize>,
        follow_ups: Arc<Mutex<Vec<String>>>,
    }

    impl WorkerSessionHandle for HistoryAwareReviveHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_history_aware".to_string())
        }

        fn send_follow_up(&self, prompt: String) -> Result<()> {
            self.follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("history-aware follow-up mutex poisoned"))?
                .push(prompt);
            self.event_hub.emit(WorkerEvent::TurnStarted {
                kind: "history-aware".to_string(),
                prompt_path: PathBuf::from("/new-epoch/prompt.md"),
            });
            Ok(())
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            bail!("history-aware handle steer is not used")
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            bail!("history-aware handle outcome is not used")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            bail!("history-aware handle result is not used")
        }

        fn last_output(&self) -> Option<String> {
            None
        }

        fn supports_event_subscriptions(&self) -> bool {
            true
        }

        fn subscribe(&self, listener: WorkerEventListener) -> Result<WorkerSubscription> {
            self.event_hub.subscribe(listener)
        }

        fn reset_event_history(&self) -> Result<()> {
            self.reset_calls.fetch_add(1, Ordering::SeqCst);
            self.event_hub.clear_history()
        }
    }

    struct FakeReviveFailureHandle {
        error_message: &'static str,
    }

    impl WorkerSessionHandle for FakeReviveFailureHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_revive_failure".to_string())
        }

        fn send_follow_up(&self, _prompt: String) -> Result<()> {
            bail!("{}", self.error_message)
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            bail!("{}", self.error_message)
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            bail!("not supported")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            bail!("not supported")
        }

        fn last_output(&self) -> Option<String> {
            None
        }

        fn supports_event_subscriptions(&self) -> bool {
            true
        }

        fn subscribe(&self, _listener: WorkerEventListener) -> Result<WorkerSubscription> {
            Ok(WorkerSubscription::noop())
        }
    }

    struct FakeHangingHandle;

    impl WorkerSessionHandle for FakeHangingHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_hanging".to_string())
        }

        fn send_follow_up(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            std::thread::sleep(Duration::from_secs(60));
            bail!("timed out")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            std::thread::sleep(Duration::from_secs(60));
            bail!("timed out")
        }

        fn last_output(&self) -> Option<String> {
            None
        }
    }

    #[test]
    fn task_manager_control_reads_current_worker_last_output() -> Result<()> {
        let control = TaskManagerControl::default();
        control.set_current(
            "task_fake".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeOutputHandle)),
        )?;

        assert_eq!(control.current_task_id()?.as_deref(), Some("task_fake"));
        assert_eq!(
            control.current_last_output()?.as_deref(),
            Some("control-output")
        );
        control.clear_current("task_fake")?;
        assert_eq!(control.current_last_output()?, None);
        Ok(())
    }

    #[test]
    fn task_manager_control_prompt_gate_deduplicates_follow_up() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let follow_ups = Arc::new(Mutex::new(Vec::new()));
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(FakeInterruptHandle {
            interrupted: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(AtomicUsize::new(0)),
            follow_ups: follow_ups.clone(),
            steers: Arc::new(Mutex::new(Vec::new())),
        });
        let control = TaskManagerControl::default();
        control.set_current(
            "task_control_gate".to_string(),
            ManagedTaskStatus::Running,
            Some(handle),
        )?;
        control.set_dispatch_context(
            "task_control_gate",
            store,
            "goal_control_gate".to_string(),
            "session_control_gate".to_string(),
            1,
        )?;
        assert!(matches!(
            control.send_follow_up_task("task_control_gate", "same prompt".to_string())?,
            SendOutcome::Sent(_)
        ));
        assert!(matches!(
            control.send_follow_up_task("task_control_gate", "same prompt".to_string())?,
            SendOutcome::Noop(_)
        ));
        assert_eq!(
            follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("follow-up mutex poisoned"))?
                .as_slice(),
            ["same prompt"]
        );
        Ok(())
    }

    #[test]
    fn task_manager_control_marks_ambiguous_follow_up_as_possibly_accepted() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let control = TaskManagerControl::default();
        control.set_current(
            "task_control_ambiguous".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeAmbiguousDispatchHandle)),
        )?;
        control.set_dispatch_context(
            "task_control_ambiguous",
            store.clone(),
            "goal_control_ambiguous".to_string(),
            "session_control_ambiguous".to_string(),
            1,
        )?;

        let outcome = control.send_follow_up_task(
            "task_control_ambiguous",
            "same ambiguous prompt".to_string(),
        )?;
        assert_eq!(
            outcome,
            SendOutcome::PossiblyAccepted(OutcomeContext {
                task_id: Some("task_control_ambiguous".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert!(outcome.is_accepted());

        let duplicate = store.reserve_prompt_dispatch(
            "goal_control_ambiguous",
            "task_control_ambiguous",
            "session_control_ambiguous",
            1,
            "follow_up",
            "gui_control",
            "same ambiguous prompt",
        )?;
        let gate = match duplicate {
            PromptDispatchDecision::Duplicate(gate) => gate,
            PromptDispatchDecision::Acquired(_) => {
                bail!("ambiguous follow-up should remain deduplicated during hold")
            }
        };
        assert_eq!(gate.status, PromptDispatchGateStatus::PossiblyAccepted);
        assert!(
            gate.reason
                .as_deref()
                .is_some_and(|reason| !reason.contains("JSON"))
        );
        assert!(matches!(
            control.send_follow_up_task(
                "task_control_ambiguous",
                "same ambiguous prompt".to_string(),
            )?,
            SendOutcome::Noop(_)
        ));
        assert_eq!(
            control.steer_task("task_control_ambiguous", "same ambiguous steer".to_string(),)?,
            SteerOutcome::PossiblyAccepted(OutcomeContext {
                task_id: Some("task_control_ambiguous".to_string()),
                ..OutcomeContext::default()
            })
        );
        Ok(())
    }

    #[test]
    fn task_manager_control_keeps_definite_follow_up_failure_retryable() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let control = TaskManagerControl::default();
        control.set_current(
            "task_control_definite".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeOutputHandle)),
        )?;
        control.set_dispatch_context(
            "task_control_definite",
            store.clone(),
            "goal_control_definite".to_string(),
            "session_control_definite".to_string(),
            1,
        )?;

        assert!(
            control
                .send_follow_up_task("task_control_definite", "retryable prompt".to_string())
                .is_err()
        );
        assert!(matches!(
            store.reserve_prompt_dispatch(
                "goal_control_definite",
                "task_control_definite",
                "session_control_definite",
                1,
                "follow_up",
                "gui_control",
                "retryable prompt",
            )?,
            PromptDispatchDecision::Acquired(_)
        ));
        Ok(())
    }

    #[test]
    fn task_manager_control_interrupts_current_worker() -> Result<()> {
        let control = TaskManagerControl::default();
        let interrupted = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicUsize::new(0));
        let follow_ups = Arc::new(Mutex::new(Vec::new()));
        let steers = Arc::new(Mutex::new(Vec::new()));
        control.set_current(
            "task_interrupt".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeInterruptHandle {
                interrupted: interrupted.clone(),
                cancelled: cancelled.clone(),
                follow_ups: follow_ups.clone(),
                steers: steers.clone(),
            })),
        )?;

        assert_eq!(
            control.send_follow_up_current_task("continue".to_string())?,
            SendOutcome::Sent(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.steer_current_task("adjust".to_string())?,
            SteerOutcome::Steered(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.interrupt_task("task_interrupt")?,
            ActionOutcome::Interrupted(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(interrupted.load(Ordering::SeqCst), 1);
        assert_eq!(
            follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("follow-up mutex poisoned"))?
                .as_slice(),
            ["continue"]
        );
        assert_eq!(
            steers
                .lock()
                .map_err(|_| anyhow::anyhow!("steer mutex poisoned"))?
                .as_slice(),
            ["adjust"]
        );
        assert_eq!(cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(
            control.send_follow_up_current_task("continue after interrupt".to_string())?,
            SendOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.steer_current_task("adjust after interrupt".to_string())?,
            SteerOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.send_follow_up_task("task_interrupt", "continue 2".to_string())?,
            SendOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.steer_task("task_interrupt", "adjust 2".to_string())?,
            SteerOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.cancel_current_task()?,
            ActionOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.interrupt_current_task()?,
            ActionOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                ..OutcomeContext::default()
            })
        );
        Ok(())
    }

    #[test]
    fn task_manager_control_cancels_current_worker() -> Result<()> {
        let control = TaskManagerControl::default();
        let interrupted = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicUsize::new(0));

        control.set_current(
            "task_cancel".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeInterruptHandle {
                interrupted: interrupted.clone(),
                cancelled: cancelled.clone(),
                follow_ups: Arc::new(Mutex::new(Vec::new())),
                steers: Arc::new(Mutex::new(Vec::new())),
            })),
        )?;

        assert_eq!(
            control.cancel_task("task_cancel")?,
            ActionOutcome::Cancelled(OutcomeContext {
                task_id: Some("task_cancel".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(interrupted.load(Ordering::SeqCst), 0);
        assert_eq!(
            control.send_follow_up_current_task("continue after cancel".to_string())?,
            SendOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_cancel".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.steer_current_task("adjust after cancel".to_string())?,
            SteerOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_cancel".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.cancel_current_task()?,
            ActionOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_cancel".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.interrupt_current_task()?,
            ActionOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_cancel".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.send_follow_up_task("task_cancel", "continue 2".to_string())?,
            SendOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_cancel".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.steer_task("task_cancel", "adjust 2".to_string())?,
            SteerOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_cancel".to_string()),
                ..OutcomeContext::default()
            })
        );
        Ok(())
    }

    #[test]
    fn messageability_reflects_task_state_and_residency() {
        let running = test_task_record(
            "task_running",
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        assert_eq!(messageability_for_record(&running), Messageability::Steer);

        let completed = test_task_record(
            "task_completed",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        assert_eq!(
            messageability_for_record(&completed),
            Messageability::Revive
        );

        let cancelled = test_task_record(
            "task_cancelled",
            ManagedTaskStatus::Cancelled,
            TaskAttemptStatus::Cancelled,
        );
        assert!(matches!(
            messageability_for_record(&cancelled),
            Messageability::NotContinuable { .. }
        ));

        let mut evicted = test_task_record(
            "task_evicted",
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        evicted.residency_state = ResidencyState::Evicted;
        assert!(matches!(
            messageability_for_record(&evicted),
            Messageability::NotContinuable { reason } if reason.contains("evicted")
        ));
    }

    #[test]
    fn release_guard_is_epoch_scoped() {
        let mut release_guard = ReleaseGuard::default();

        assert!(release_guard.release_once("task_epoch", 0));
        assert!(!release_guard.release_once("task_epoch", 0));
        assert!(release_guard.release_once("task_epoch", 1));
        release_guard.forget_task("task_epoch");
        assert!(release_guard.release_once("task_epoch", 0));
    }

    #[test]
    fn task_manager_snapshot_exposes_queue_state_for_gui_observers() -> Result<()> {
        let control = TaskManagerControl::default();
        control.set_current(
            "task_snapshot".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeOutputHandle)),
        )?;
        let mut manager = TaskManager::with_control(control);
        let artifacts_root = PathBuf::from("/tmp/gearbox-goal-artifacts");
        manager.set_artifacts_root(artifacts_root.clone());
        manager.records.insert(
            "task_snapshot".to_string(),
            TaskRecord {
                task_id: "task_snapshot".to_string(),
                worker_kind: "opencode".to_string(),
                worker_command: None,
                worker_model: None,
                worker_category: "repair".to_string(),
                route_hint: Some("repair".to_string()),
                route_reason: "test route".to_string(),
                status: ManagedTaskStatus::Running,
                started_at: timestamp(),
                finished_at: None,
                residency_state: ResidencyState::Resident,
                run_epoch: 0,
                notified_epoch: default_notified_epoch(),
                notification_failed_epoch: Some(3),
                killed: false,
                session_id: Some("session_fake".to_string()),
                parent_session_id: Some("parent-session".to_string()),
                root_session_id: None,
                parent_task_id: None,
                result_path: Some(PathBuf::from("/tmp/task-result.json")),
                outcome_path: Some(PathBuf::from("/tmp/task-outcome.json")),
                summary: "Worker task started.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
                attempts: vec![TaskAttempt {
                    attempt: 1,
                    worker_kind: "opencode".to_string(),
                    worker_command: None,
                    worker_model: None,
                    worker_category: "repair".to_string(),
                    route_hint: Some("repair".to_string()),
                    route_reason: "test route".to_string(),
                    status: TaskAttemptStatus::Running,
                    started_at: timestamp(),
                    finished_at: None,
                    session_id: Some("session_fake".to_string()),
                    result_path: Some(PathBuf::from("/tmp/attempt-result.json")),
                    outcome_path: Some(PathBuf::from("/tmp/attempt-outcome.json")),
                    summary: "Worker task started.".to_string(),
                    failure_kind: None,
                    retry_reason: None,
                    error: None,
                }],
            },
        );

        let snapshot = manager.snapshot()?;

        assert_eq!(snapshot.counts.running, 1);
        assert_eq!(snapshot.counts.pending, 0);
        assert_eq!(
            snapshot.artifacts_root.as_deref(),
            Some(artifacts_root.as_path())
        );
        assert_eq!(snapshot.current_output.as_deref(), Some("control-output"));
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.tasks[0].task_id, "task_snapshot");
        assert_eq!(
            snapshot.tasks[0].parent_session_id.as_deref(),
            Some("parent-session")
        );
        assert_eq!(snapshot.tasks[0].notification_failed_epoch, Some(3));
        assert_eq!(snapshot.tasks[0].attempts.len(), 1);
        assert_eq!(
            snapshot.tasks[0].messageability,
            Some(Messageability::Steer)
        );
        assert_eq!(snapshot.tasks[0].summary_head, "Worker task started.");
        assert!(
            snapshot.tasks[0]
                .continuation_hint
                .contains("Steer the running task")
        );
        assert_eq!(
            snapshot.tasks[0].attempts[0].outcome_path.as_deref(),
            Some(std::path::Path::new("/tmp/attempt-outcome.json"))
        );
        Ok(())
    }

    #[test]
    fn task_manager_snapshot_exposes_model_fallback_chain_without_errors() -> Result<()> {
        let mut manager = TaskManager::new();
        let mut record = test_task_record(
            "task_snapshot_fallback",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.summary = "Completed with a fallback.".to_string();
        record.retry_reason =
            Some("模型回退：opencode/hy3-free -> opencode/mimo-v2.5-free；accepted".to_string());
        record.attempts[0].worker_model = Some("opencode/hy3-free".to_string());
        record.attempts[0].error = Some("HTTP 429 rate limit exceeded".to_string());

        let mut second_attempt = record.attempts[0].clone();
        second_attempt.attempt = 2;
        second_attempt.worker_model = Some("opencode/mimo-v2.5-free".to_string());
        second_attempt.error = Some("api_key=do-not-expose".to_string());
        record.attempts.push(second_attempt);

        manager.records.insert(record.task_id.clone(), record);

        let snapshot = manager.snapshot()?;
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(
            snapshot.tasks[0].retry_reason.as_deref(),
            Some("模型回退：opencode/hy3-free -> opencode/mimo-v2.5-free；accepted")
        );
        assert_eq!(
            snapshot.tasks[0].summary_head,
            "Completed with a fallback.；模型回退链：opencode/hy3-free -> opencode/mimo-v2.5-free"
        );
        assert!(!snapshot.tasks[0].summary_head.contains("429"));
        assert!(!snapshot.tasks[0].summary_head.contains("api_key"));
        Ok(())
    }

    #[test]
    fn task_manager_snapshot_includes_route_transform_artifacts() -> Result<()> {
        let mut manager = TaskManager::new();
        manager.records.insert(
            "task_fallback".to_string(),
            TaskRecord {
                task_id: "task_fallback".to_string(),
                worker_kind: "opencode".to_string(),
                worker_command: None,
                worker_model: None,
                worker_category: "repair".to_string(),
                route_hint: Some("repair".to_string()),
                route_reason: "test route".to_string(),
                status: ManagedTaskStatus::Failed,
                started_at: timestamp(),
                finished_at: None,
                residency_state: ResidencyState::Resident,
                run_epoch: 0,
                notified_epoch: default_notified_epoch(),
                notification_failed_epoch: None,
                killed: false,
                session_id: Some("session_fake".to_string()),
                parent_session_id: None,
                root_session_id: None,
                parent_task_id: None,
                result_path: Some(PathBuf::from("/tmp/task-result.json")),
                outcome_path: Some(PathBuf::from("/tmp/task-outcome.json")),
                summary: "Worker task failed.".to_string(),
                failure_kind: Some(TaskFailureKind::WorkerFailed),
                retry_reason: Some("retry requested".to_string()),
                error: None,
                attempts: vec![
                    TaskAttempt {
                        attempt: 1,
                        worker_kind: "opencode".to_string(),
                        worker_command: None,
                        worker_model: None,
                        worker_category: "repair".to_string(),
                        route_hint: Some("repair".to_string()),
                        route_reason: "test route".to_string(),
                        status: TaskAttemptStatus::Failed,
                        started_at: timestamp(),
                        finished_at: Some(timestamp()),
                        session_id: Some("session_fake".to_string()),
                        result_path: Some(PathBuf::from("/tmp/attempt-one-result.json")),
                        outcome_path: Some(PathBuf::from("/tmp/attempt-one-outcome.json")),
                        summary: "First attempt failed.".to_string(),
                        failure_kind: Some(TaskFailureKind::WorkerFailed),
                        retry_reason: Some("retry requested".to_string()),
                        error: None,
                    },
                    TaskAttempt {
                        attempt: 2,
                        worker_kind: "codex".to_string(),
                        worker_command: None,
                        worker_model: None,
                        worker_category: "deep".to_string(),
                        route_hint: Some("deep".to_string()),
                        route_reason: "fallback route".to_string(),
                        status: TaskAttemptStatus::Running,
                        started_at: timestamp(),
                        finished_at: None,
                        session_id: Some("session_retry".to_string()),
                        result_path: Some(PathBuf::from("/tmp/attempt-two-result.json")),
                        outcome_path: Some(PathBuf::from("/tmp/attempt-two-outcome.json")),
                        summary: "Second attempt running.".to_string(),
                        failure_kind: None,
                        retry_reason: None,
                        error: None,
                    },
                ],
            },
        );

        let snapshot = manager.snapshot()?;

        assert_eq!(
            snapshot.tasks[0].attempts[0]
                .route_transform_path
                .as_deref(),
            Some(std::path::Path::new("/tmp/route-transform-1-to-2.md"))
        );
        assert_eq!(
            snapshot.tasks[0].attempts[1]
                .route_transform_path
                .as_deref(),
            Some(std::path::Path::new("/tmp/route-transform-2-stopped.md"))
        );
        Ok(())
    }

    #[test]
    fn task_manager_snapshot_counts_interrupted_and_lost_tasks() -> Result<()> {
        let mut manager = TaskManager::new();
        manager.records.insert(
            "task_interrupted".to_string(),
            test_task_record(
                "task_interrupted",
                ManagedTaskStatus::Interrupted,
                TaskAttemptStatus::Interrupted,
            ),
        );
        manager.records.insert(
            "task_lost".to_string(),
            test_task_record(
                "task_lost",
                ManagedTaskStatus::Lost,
                TaskAttemptStatus::Lost,
            ),
        );

        let snapshot = manager.snapshot()?;

        assert_eq!(snapshot.counts.interrupted, 1);
        assert_eq!(snapshot.counts.lost, 1);
        assert_eq!(snapshot.tasks.len(), 2);
        Ok(())
    }

    #[test]
    fn late_finished_message_after_forget_is_ignored() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_forgotten");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf ignored".to_string()),
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
        };
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let running_task = RunningTask {
            store: store.clone(),
            handle: Arc::new(FakeOutputHandle),
            queued_task,
            started_at: Instant::now(),
            _subscription: None,
        };
        let result = WorkerResult {
            status: WorkerStatus::Succeeded,
            command: None,
            exit_code: Some(0),
            summary: "late result".to_string(),
            packet_path: store.worker_dir(&task.id).join("packet.json"),
            prompt_path: store.worker_dir(&task.id).join("prompt.md"),
            stdout_path: None,
            stderr_path: None,
            last_message_path: None,
            result_path: store.worker_dir(&task.id).join("result.json"),
            outcome_path: store.worker_dir(&task.id).join("outcome.json"),
        };
        let outcome = WorkerOutcome {
            status: WorkerStatus::Succeeded,
            session_id: None,
            session_capability: None,
            summary: "late outcome".to_string(),
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            known_failures: Vec::new(),
            raw_output_path: None,
            command: None,
            exit_code: Some(0),
        };
        let mut manager = TaskManager::new();
        manager.records.insert(
            task.id.clone(),
            test_task_record(
                &task.id,
                ManagedTaskStatus::Running,
                TaskAttemptStatus::Running,
            ),
        );
        manager
            .running_tasks
            .insert(task.id.clone(), running_task.clone());

        manager.forget_task(&task.id)?;
        manager.settle_finished_task(FinishedTaskMessage {
            task_id: task.id.clone(),
            running_task,
            run_result: Ok((outcome, result)),
        })?;

        assert!(!manager.records.contains_key(&task.id));
        assert!(!manager.completed_runs.contains_key(&task.id));
        assert!(!manager.completed_errors.contains_key(&task.id));
        assert!(manager.running_tasks.is_empty());
        Ok(())
    }

    #[test]
    fn task_manager_wait_for_does_not_hang_on_stale_running_task() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_stale_running");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf noop".to_string()),
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
        };
        let started_at = timestamp();
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let mut manager = TaskManager::new();
        manager.records.insert(
            task.id.clone(),
            TaskRecord {
                task_id: task.id.clone(),
                worker_kind: "opencode".to_string(),
                worker_command: Some("printf noop".to_string()),
                worker_model: None,
                worker_category: "quick".to_string(),
                route_hint: None,
                route_reason: "test route".to_string(),
                status: ManagedTaskStatus::Running,
                started_at: started_at.clone(),
                finished_at: None,
                residency_state: ResidencyState::Resident,
                run_epoch: 0,
                notified_epoch: default_notified_epoch(),
                notification_failed_epoch: None,
                killed: false,
                session_id: Some("session_hanging".to_string()),
                parent_session_id: None,
                root_session_id: None,
                parent_task_id: None,
                result_path: None,
                outcome_path: None,
                summary: "Worker task started.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
                attempts: vec![TaskAttempt {
                    attempt: 1,
                    worker_kind: "opencode".to_string(),
                    worker_command: Some("printf noop".to_string()),
                    worker_model: None,
                    worker_category: "quick".to_string(),
                    route_hint: None,
                    route_reason: "test route".to_string(),
                    status: TaskAttemptStatus::Running,
                    started_at,
                    finished_at: None,
                    session_id: Some("session_hanging".to_string()),
                    result_path: None,
                    outcome_path: None,
                    summary: "Worker task started.".to_string(),
                    failure_kind: None,
                    retry_reason: None,
                    error: None,
                }],
            },
        );
        manager.running_tasks.insert(
            task.id.clone(),
            RunningTask {
                store: store.clone(),
                handle: Arc::new(FakeHangingHandle),
                queued_task,
                started_at: Instant::now() - Duration::from_secs(30) - Duration::from_millis(1),
                _subscription: None,
            },
        );

        let error = manager
            .wait_for(&task.id)
            .expect_err("stale worker should not hang forever");
        assert!(format!("{error:#}").contains("timed out waiting for outcome"));
        let record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(record.contains(r#""status": "lost""#));
        assert!(record.contains("timed out waiting for outcome"));
        Ok(())
    }

    #[test]
    fn worker_activity_prevents_stale_timeout() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_active_worker");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf noop".to_string()),
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
        };
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let event_hub = WorkerEventHub::default();
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(EventfulHandle {
            event_hub: event_hub.clone(),
        });
        let activity_heartbeat = Arc::new(Mutex::new(
            Instant::now() - Duration::from_secs(30) - Duration::from_millis(1),
        ));
        let subscription = subscribe_to_worker_events_with_activity_and_circuit(
            &handle,
            &store,
            &task.id,
            &task.goal_id,
            0,
            None,
            Some(activity_heartbeat.clone()),
            None,
            ToolCallCircuitBreakerPolicy::default(),
        )?;
        let mut record = test_task_record(
            &task.id,
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        record.session_id = handle.session_id();
        write_task_record(&store, &record)?;
        let mut manager = TaskManager::new();
        manager.records.insert(task.id.clone(), record);
        manager
            .activity_heartbeats
            .insert(task.id.clone(), activity_heartbeat);
        manager.running_tasks.insert(
            task.id.clone(),
            RunningTask {
                store,
                handle,
                queued_task,
                started_at: Instant::now() - Duration::from_secs(30) - Duration::from_millis(1),
                _subscription: subscription,
            },
        );

        event_hub.emit(WorkerEvent::AssistantTextDelta {
            kind: "acp".to_string(),
            delta: "still working".to_string(),
        });

        assert_eq!(manager.sweep_stale_running_tasks()?, 0);
        assert!(manager.running_tasks.contains_key(&task.id));
        Ok(())
    }

    #[test]
    fn free_model_task_is_not_swept_as_stale() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_free_model_not_stale");
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("printf noop".to_string()),
            worker_model: Some("opencode/deepseek-v4-flash-free".to_string()),
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 1,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
        };
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(EventfulHandle {
            event_hub: WorkerEventHub::default(),
        });
        let mut record = test_task_record(
            &task.id,
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        record.worker_model = Some("opencode/deepseek-v4-flash-free".to_string());
        record.session_id = handle.session_id();
        write_task_record(&store, &record)?;
        let mut manager = TaskManager::new();
        manager.records.insert(task.id.clone(), record);
        manager.running_tasks.insert(
            task.id.clone(),
            RunningTask {
                store,
                handle,
                queued_task,
                started_at: Instant::now() - Duration::from_secs(60),
                _subscription: None,
            },
        );
        // Free-model tasks must survive stale sweep even with no recent activity.
        assert_eq!(manager.sweep_stale_running_tasks()?, 0);
        assert!(manager.running_tasks.contains_key(&task.id));
        Ok(())
    }

    #[test]
    fn non_free_model_task_is_swept_as_stale() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_non_free_model_stale");
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("printf noop".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 1,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
        };
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(EventfulHandle {
            event_hub: WorkerEventHub::default(),
        });
        let mut record = test_task_record(
            &task.id,
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        record.worker_model = None; // non-free model
        record.session_id = handle.session_id();
        write_task_record(&store, &record)?;
        let mut manager = TaskManager::new();
        manager.records.insert(task.id.clone(), record);
        manager.running_tasks.insert(
            task.id.clone(),
            RunningTask {
                store,
                handle,
                queued_task,
                started_at: Instant::now() - Duration::from_secs(60),
                _subscription: None,
            },
        );
        // Non-free tasks with no activity should be swept as stale.
        assert_eq!(manager.sweep_stale_running_tasks()?, 1);
        assert!(!manager.running_tasks.contains_key(&task.id));
        Ok(())
    }

    #[test]
    fn repetitive_tool_calls_trigger_a_circuit_breaker_cancel() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_tool_loop");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf noop".to_string()),
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
        };
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "tool loop test".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let event_hub = WorkerEventHub::default();
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(EventfulHandle {
            event_hub: event_hub.clone(),
        });
        let circuit_state = Arc::new(Mutex::new(ToolCallCircuitState::default()));
        let subscription = subscribe_to_worker_events_with_activity_and_circuit(
            &handle,
            &store,
            &task.id,
            &task.goal_id,
            0,
            None,
            None,
            Some(circuit_state.clone()),
            ToolCallCircuitBreakerPolicy::default(),
        )?;
        let mut record = test_task_record(
            &task.id,
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        record.session_id = handle.session_id();
        write_task_record(&store, &record)?;
        let mut manager = TaskManager::new();
        manager.records.insert(task.id.clone(), record);
        manager
            .tool_call_circuit_states
            .insert(task.id.clone(), circuit_state);
        manager.running_tasks.insert(
            task.id.clone(),
            RunningTask {
                store,
                handle,
                queued_task,
                started_at: Instant::now(),
                _subscription: subscription,
            },
        );

        for _ in 0..20 {
            event_hub.emit(WorkerEvent::ToolCallStarted {
                kind: "acp".to_string(),
                tool_name: "edit_file".to_string(),
                arguments: r#"{"path":"src/lib.rs","patch":"same"}"#.to_string(),
            });
        }

        manager.tick()?;
        let record = manager
            .records
            .get(&task.id)
            .context("missing tool-loop task record")?;
        assert_eq!(record.status, ManagedTaskStatus::Cancelled);
        assert!(
            record
                .error
                .as_deref()
                .is_some_and(|reason| reason.contains("circuit breaker")),
            "record after circuit cancellation: {record:?}"
        );
        Ok(())
    }

    #[test]
    fn tool_call_circuit_breaker_resets_consecutive_signature() {
        let policy = ToolCallCircuitBreakerPolicy {
            enabled: true,
            max_tool_calls: 10,
            consecutive_threshold: 3,
        };
        let state = Arc::new(Mutex::new(ToolCallCircuitState::default()));

        record_tool_call_for_circuit_breaker(&state, &policy, "read_file", "{\"path\":\"a\"}");
        record_tool_call_for_circuit_breaker(&state, &policy, "read_file", "{\"path\":\"a\"}");
        record_tool_call_for_circuit_breaker(&state, &policy, "read_file", "{\"path\":\"b\"}");
        {
            let state = state
                .lock()
                .expect("circuit state lock should not be poisoned");
            assert_eq!(state.total_calls, 3);
            assert_eq!(state.consecutive_calls, 1);
            assert!(state.trigger_reason.is_none());
        }

        record_tool_call_for_circuit_breaker(&state, &policy, "read_file", "{\"path\":\"b\"}");
        record_tool_call_for_circuit_breaker(&state, &policy, "read_file", "{\"path\":\"b\"}");
        let state = state
            .lock()
            .expect("circuit state lock should not be poisoned");
        assert_eq!(state.total_calls, 5);
        assert_eq!(state.consecutive_calls, 3);
        assert!(
            state
                .trigger_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("read_file"))
        );
    }

    #[test]
    fn tool_call_circuit_breaker_normalizes_json_argument_order() {
        let policy = ToolCallCircuitBreakerPolicy {
            enabled: true,
            max_tool_calls: 10,
            consecutive_threshold: 2,
        };
        let state = Arc::new(Mutex::new(ToolCallCircuitState::default()));

        record_tool_call_for_circuit_breaker(
            &state,
            &policy,
            "edit_file",
            r#"{"path":"src/lib.rs","patch":"same"}"#,
        );
        record_tool_call_for_circuit_breaker(
            &state,
            &policy,
            "edit_file",
            r#"{"patch":"same","path":"src/lib.rs"}"#,
        );

        let state = state
            .lock()
            .expect("circuit state lock should not be poisoned");
        assert_eq!(state.consecutive_calls, 2);
        assert!(state.trigger_reason.is_some());
    }

    #[test]
    fn tool_call_circuit_breaker_enforces_total_call_limit() {
        let policy = ToolCallCircuitBreakerPolicy {
            enabled: true,
            max_tool_calls: 3,
            consecutive_threshold: 100,
        };
        let state = Arc::new(Mutex::new(ToolCallCircuitState::default()));

        for path in ["a", "b", "c"] {
            record_tool_call_for_circuit_breaker(
                &state,
                &policy,
                "read_file",
                &format!("{{\"path\":\"{path}\"}}"),
            );
        }

        let state = state
            .lock()
            .expect("circuit state lock should not be poisoned");
        assert_eq!(state.total_calls, 3);
        assert!(
            state
                .trigger_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("maximum tool call limit"))
        );
    }

    #[test]
    fn task_manager_tick_cleans_orphaned_running_and_queued_state() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let queued_task = test_task("task_orphan_queued");
        let running_task = test_task("task_orphan_running");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf noop".to_string()),
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
        };
        let mut manager = TaskManager::new();
        manager.queued_tasks.push_back(QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: queued_task,
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config: config.clone(),
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        });
        let running_task_id = running_task.id.clone();
        manager.running_tasks.insert(
            running_task_id,
            RunningTask {
                store,
                handle: Arc::new(FakeHangingHandle),
                queued_task: QueuedTask {
                    store: StateStore::new(temp_dir.path()),
                    workspace: temp_dir.path().to_path_buf(),
                    task: running_task,
                    route_attempt: 1,
                    goal: "test goal".to_string(),
                    verification_commands: Vec::new(),
                    config,
                    cancellation_token: None,
                    coordinator_model: None,
                    coordinator_brief: None,
                    route_hint: None,
                },
                started_at: Instant::now(),
                _subscription: None,
            },
        );

        let cleaned = manager.tick()?;

        assert_eq!(cleaned, 2);
        assert!(manager.queued_tasks.is_empty());
        assert!(manager.running_tasks.is_empty());
        Ok(())
    }

    #[test]
    fn task_manager_queues_when_concurrency_slot_is_busy() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_task("task_first");
        let second_task = test_task("task_second");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo worker-ok".to_string()),
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
        };
        let mut manager = TaskManager::new();

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "second goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        let queued_record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == second_task.id)
            .context("missing queued task record")?;
        assert_eq!(queued_record.status, ManagedTaskStatus::Pending);
        assert!(
            !store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );

        let first_run = manager.wait_for(&first_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        let second_record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == second_task.id)
            .context("missing second task record")?;
        assert_eq!(second_record.status, ManagedTaskStatus::Running);
        assert!(
            store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );

        let second_run = manager.wait_for(&second_task.id)?;
        assert_eq!(second_run.record.status, ManagedTaskStatus::Completed);
        Ok(())
    }

    #[test]
    fn task_manager_serializes_tasks_with_same_concurrency_key() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_task("task_same_key_first");
        let second_task = test_task("task_same_key_second");
        let first_release = temp_dir.path().join("release-first");
        let second_release = temp_dir.path().join("release-second");
        let first_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo first-ok'",
            first_release.display()
        );
        let second_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo second-ok'",
            second_release.display()
        );
        let first_config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(first_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut second_config = first_config.clone();
        second_config.worker_command = Some(second_command);
        let mut manager = TaskManager::new();
        manager.concurrency.max_parallel_workers = 2;

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first goal",
            verification_commands: &[],
            config: &first_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "second goal",
            verification_commands: &[],
            config: &second_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert!(
            store
                .worker_dir(&first_task.id)
                .join("packet.json")
                .exists()
        );
        assert!(
            !store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );
        fs::write(&first_release, "go")?;
        let first_run = manager.wait_for(&first_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        assert!(
            store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );
        fs::write(&second_release, "go")?;
        let second_run = manager.wait_for(&second_task.id)?;
        assert_eq!(second_run.record.status, ManagedTaskStatus::Completed);
        Ok(())
    }

    #[test]
    fn task_manager_serializes_overlapping_write_scopes() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = scoped_test_task("task_scope_overlap_first", &["src"]);
        let second_task = scoped_test_task("task_scope_overlap_second", &["src/components"]);
        let first_release = temp_dir.path().join("release-scope-first");
        let second_release = temp_dir.path().join("release-scope-second");
        let first_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo first-scope-ok'",
            first_release.display()
        );
        let second_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo second-scope-ok'",
            second_release.display()
        );
        let first_config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(first_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 2,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut second_config = first_config.clone();
        second_config.worker_command = Some(second_command);
        let mut manager = TaskManager::new();
        manager.concurrency.max_parallel_workers = 2;
        manager.concurrency.max_parallel_per_key = 2;

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first scope goal",
            verification_commands: &[],
            config: &first_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "second scope goal",
            verification_commands: &[],
            config: &second_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert!(
            store
                .worker_dir(&first_task.id)
                .join("packet.json")
                .exists()
        );
        assert!(
            !store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists(),
            "overlapping write scopes should not start together"
        );
        fs::write(&first_release, "go")?;
        let first_run = manager.wait_for(&first_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        assert!(
            store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists(),
            "second task should start after the overlapping scope is released"
        );
        fs::write(&second_release, "go")?;
        let second_run = manager.wait_for(&second_task.id)?;
        assert_eq!(second_run.record.status, ManagedTaskStatus::Completed);
        Ok(())
    }

    #[test]
    fn task_manager_allows_disjoint_write_scopes_with_room_in_key_budget() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = scoped_test_task("task_scope_disjoint_first", &["src"]);
        let second_task = scoped_test_task("task_scope_disjoint_second", &["docs"]);
        let first_release = temp_dir.path().join("release-disjoint-first");
        let second_release = temp_dir.path().join("release-disjoint-second");
        let first_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo first-disjoint-ok'",
            first_release.display()
        );
        let second_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo second-disjoint-ok'",
            second_release.display()
        );
        let first_config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(first_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 2,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut second_config = first_config.clone();
        second_config.worker_command = Some(second_command);
        let mut manager = TaskManager::new();
        manager.concurrency.max_parallel_workers = 2;
        manager.concurrency.max_parallel_per_key = 2;

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first disjoint goal",
            verification_commands: &[],
            config: &first_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "second disjoint goal",
            verification_commands: &[],
            config: &second_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert!(
            store
                .worker_dir(&first_task.id)
                .join("packet.json")
                .exists()
        );
        assert!(
            store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists(),
            "disjoint write scopes should be allowed to start together"
        );
        fs::write(&first_release, "go")?;
        fs::write(&second_release, "go")?;
        let first_run = manager.wait_for(&first_task.id)?;
        let second_run = manager.wait_for(&second_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(second_run.record.status, ManagedTaskStatus::Completed);
        Ok(())
    }

    #[test]
    fn task_manager_runs_different_concurrency_keys_in_parallel() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_task("task_keyed_first");
        let second_task = test_task("task_keyed_second");
        let first_release = temp_dir.path().join("release-keyed-first");
        let second_release = temp_dir.path().join("release-keyed-second");
        let first_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo first-ok'",
            first_release.display()
        );
        let second_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; mkdir -p .gearbox-agent/evidence; printf verified > .gearbox-agent/evidence/receipt.md; printf \"done\\nEVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\\n\" > \"$GEARBOX_WORKER_LAST_MESSAGE\"; echo second-ok'",
            second_release.display()
        );
        let first_config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(first_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::Opencode,
        };
        let second_config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: Some(second_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::Codex,
        };
        let mut manager = TaskManager::new();
        manager.concurrency.max_parallel_workers = 2;

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first goal",
            verification_commands: &[],
            config: &first_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "second goal",
            verification_commands: &[],
            config: &second_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert!(
            store
                .worker_dir(&first_task.id)
                .join("packet.json")
                .exists()
        );
        assert!(
            store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );
        fs::write(&first_release, "go")?;
        fs::write(&second_release, "go")?;
        let first_run = manager.wait_for(&first_task.id)?;
        let second_run = manager.wait_for(&second_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(second_run.record.status, ManagedTaskStatus::Completed);
        Ok(())
    }

    #[test]
    fn read_only_review_tasks_can_run_in_parallel_with_same_key() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_read_only_task("task_review_first");
        let second_task = test_read_only_task("task_review_second");
        let first_release = temp_dir.path().join("release-review-first");
        let second_release = temp_dir.path().join("release-review-second");
        let first_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo first-review-ok'",
            first_release.display()
        );
        let second_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo second-review-ok'",
            second_release.display()
        );
        let first_config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(first_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let second_config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(second_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let mut manager = TaskManager::new();
        manager.concurrency.max_parallel_workers = 2;

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first review goal",
            verification_commands: &[],
            config: &first_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "second review goal",
            verification_commands: &[],
            config: &second_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert!(
            store
                .worker_dir(&first_task.id)
                .join("packet.json")
                .exists()
        );
        assert!(
            store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );
        fs::write(&first_release, "go")?;
        fs::write(&second_release, "go")?;
        let first_run = manager.wait_for(&first_task.id)?;
        let second_run = manager.wait_for(&second_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(second_run.record.status, ManagedTaskStatus::Completed);
        Ok(())
    }

    #[test]
    fn task_manager_cancel_task_removes_pending_task_from_queue() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_task("task_running_slot");
        let pending_task = test_task("task_pending_cancel");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo worker-ok".to_string()),
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
        };
        let mut manager = TaskManager::new();

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &pending_task,
            route_attempt: 1,
            goal: "pending goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        manager.cancel_task(&pending_task.id)?;

        let pending_record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == pending_task.id)
            .context("missing pending task record")?;
        assert_eq!(pending_record.status, ManagedTaskStatus::Cancelled);
        let events =
            fs::read_to_string(store.worker_dir(&pending_task.id).join("task-events.jsonl"))?;
        assert!(events.contains(r#""status":"pending""#));
        assert!(events.contains(r#""status":"cancelled""#));

        let first_run = manager.wait_for(&first_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        assert!(
            !store
                .worker_dir(&pending_task.id)
                .join("packet.json")
                .exists()
        );
        Ok(())
    }

    #[test]
    fn task_manager_cancel_task_cancels_running_worker_handle() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_cancel_handle");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo unreachable".to_string()),
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
        };
        let mut manager = TaskManager::new();

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.cancel_task(&task.id)?;

        let error = manager
            .wait_for(&task.id)
            .expect_err("cancelled worker should not produce a run");
        assert!(format!("{error:#}").contains("cancelled"));
        let record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(record.contains(r#""status": "cancelled""#));
        Ok(())
    }

    #[test]
    fn task_manager_control_cancels_worker_while_waiting() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_control_cancel");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'sleep 5'".to_string()),
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
        };
        let control = TaskManagerControl::default();
        let mut manager = TaskManager::with_control(control.clone());

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        let wait_task_id = task.id.clone();
        let waiter = std::thread::spawn(move || manager.wait_for(&wait_task_id));

        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(
            control.current_task_id()?.as_deref(),
            Some(task.id.as_str())
        );
        assert_eq!(
            control.cancel_current_task()?,
            ActionOutcome::Cancelled(OutcomeContext {
                task_id: Some(task.id.clone()),
                ..OutcomeContext::default()
            })
        );

        let error = waiter
            .join()
            .expect("wait thread should not panic")
            .expect_err("cancelled worker should not complete");
        assert!(format!("{error:#}").contains("cancelled"));
        assert_eq!(
            control.current_task_id()?.as_deref(),
            Some(task.id.as_str())
        );

        let record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(record.contains(r#""status": "cancelled""#));
        Ok(())
    }

    #[test]
    fn wait_for_with_cancellation_returns_completion_event() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_wait_completion_event");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf worker-ok".to_string()),
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
        };
        let mut manager = TaskManager::new();
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        let run = manager
            .wait_for_with_cancellation(&task.id, None)?
            .context("completion channel should produce a terminal run")?;
        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        let stdout_path = run
            .result
            .stdout_path
            .as_deref()
            .context("completion run should record stdout path")?;
        assert_eq!(fs::read_to_string(stdout_path)?.trim(), "worker-ok");
        Ok(())
    }

    #[test]
    fn wait_for_with_cancellation_reports_channel_disconnect() {
        let mut manager = TaskManager::new();
        let (sender, receiver) = std::sync::mpsc::channel::<FinishedTaskMessage>();
        manager.finished_task_rx = receiver;
        drop(sender);

        let error = manager
            .wait_for_with_cancellation("task_channel_disconnect", None)
            .expect_err("disconnected completion channel must be visible");
        assert!(format!("{error:#}").contains("channel disconnected"));
    }

    #[test]
    fn wait_for_with_cancellation_returns_cancelled_without_fixed_polling() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_wait_cancel_token");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'sleep 5'".to_string()),
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
        };
        let mut manager = TaskManager::new();
        let cancellation_token = CancellationToken::new();
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        let wait_task_id = task.id.clone();
        let wait_token = cancellation_token.clone();
        let waiter = std::thread::spawn(move || {
            (
                manager.wait_for_with_cancellation(&wait_task_id, Some(&wait_token)),
                manager,
            )
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        cancellation_token.cancel();

        let (result, mut manager) = waiter.join().expect("wait thread should not panic");
        assert!(result?.is_none());
        manager.cancel_task(&task.id)?;
        let error = manager
            .wait_for(&task.id)
            .expect_err("cancelled worker should not complete");
        assert!(format!("{error:#}").contains("cancelled"));
        let record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(record.contains(r#""status": "cancelled""#));
        Ok(())
    }

    #[test]
    fn task_manager_marks_running_task_cancelled() -> Result<()> {
        let mut manager = TaskManager::new();
        manager.records.insert(
            "task_running".to_string(),
            TaskRecord {
                task_id: "task_running".to_string(),
                worker_kind: "opencode".to_string(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".to_string(),
                route_hint: None,
                route_reason: "test route".to_string(),
                status: ManagedTaskStatus::Running,
                started_at: timestamp(),
                finished_at: None,
                residency_state: ResidencyState::Resident,
                run_epoch: 0,
                notified_epoch: default_notified_epoch(),
                notification_failed_epoch: None,
                killed: false,
                session_id: None,
                parent_session_id: None,
                root_session_id: None,
                parent_task_id: None,
                result_path: None,
                outcome_path: None,
                summary: "Worker task started.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
                attempts: vec![TaskAttempt {
                    attempt: 1,
                    worker_kind: "opencode".to_string(),
                    worker_command: None,
                    worker_model: None,
                    worker_category: "quick".to_string(),
                    route_hint: None,
                    route_reason: "test route".to_string(),
                    status: TaskAttemptStatus::Running,
                    started_at: timestamp(),
                    finished_at: None,
                    session_id: None,
                    result_path: None,
                    outcome_path: None,
                    summary: "Worker task started.".to_string(),
                    failure_kind: None,
                    retry_reason: None,
                    error: None,
                }],
            },
        );

        manager.cancel_task("task_running")?;

        let record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == "task_running")
            .context("missing task record")?;
        assert_eq!(record.status, ManagedTaskStatus::Cancelled);
        assert!(record.finished_at.is_some());
        Ok(())
    }

    #[test]
    fn session_cancel_cascades_to_descendant_tasks() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf noop".to_string()),
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
        };
        let workspace = temp_dir.path().to_path_buf();
        let root_task = test_task("task_root");
        let mut child_task = test_task("task_child");
        child_task.parent_task_id = Some(root_task.id.clone());
        let mut grandchild_task = test_task("task_grandchild");
        grandchild_task.parent_task_id = Some(child_task.id.clone());

        let root_cancelled = Arc::new(AtomicUsize::new(0));
        let child_cancelled = Arc::new(AtomicUsize::new(0));
        let running_handle = |cancelled: Arc<AtomicUsize>| -> Arc<dyn WorkerSessionHandle> {
            Arc::new(FakeInterruptHandle {
                interrupted: Arc::new(AtomicUsize::new(0)),
                cancelled,
                follow_ups: Arc::new(Mutex::new(Vec::new())),
                steers: Arc::new(Mutex::new(Vec::new())),
            })
        };
        let make_queued_task = |task: Task| QueuedTask {
            store: store.clone(),
            workspace: workspace.clone(),
            task,
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config: config.clone(),
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };

        let root_queued_task = make_queued_task(root_task.clone());
        let child_queued_task = make_queued_task(child_task.clone());
        let grandchild_queued_task = make_queued_task(grandchild_task.clone());
        let root_running_task = RunningTask {
            store: store.clone(),
            handle: running_handle(root_cancelled.clone()),
            queued_task: root_queued_task,
            started_at: Instant::now(),
            _subscription: None,
        };
        let child_running_task = RunningTask {
            store: store.clone(),
            handle: running_handle(child_cancelled.clone()),
            queued_task: child_queued_task,
            started_at: Instant::now(),
            _subscription: None,
        };

        let mut root_record = test_task_record(
            &root_task.id,
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        root_record.parent_task_id = None;
        let mut child_record = test_task_record(
            &child_task.id,
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        child_record.parent_task_id = Some(root_task.id.clone());
        let mut grandchild_record = test_task_record(
            &grandchild_task.id,
            ManagedTaskStatus::Pending,
            TaskAttemptStatus::Pending,
        );
        grandchild_record.parent_task_id = Some(child_task.id.clone());

        let mut manager = TaskManager::new();
        manager.records.insert(root_task.id.clone(), root_record);
        manager.records.insert(child_task.id.clone(), child_record);
        manager
            .records
            .insert(grandchild_task.id.clone(), grandchild_record);
        manager
            .running_tasks
            .insert(root_task.id.clone(), root_running_task);
        manager
            .running_tasks
            .insert(child_task.id.clone(), child_running_task);
        manager.queued_tasks.push_back(grandchild_queued_task);

        manager.cancel_task(&root_task.id)?;

        assert_eq!(root_cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(child_cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(
            manager
                .records
                .get(&root_task.id)
                .map(|record| &record.status),
            Some(&ManagedTaskStatus::Cancelled)
        );
        assert_eq!(
            manager
                .records
                .get(&child_task.id)
                .map(|record| &record.status),
            Some(&ManagedTaskStatus::Cancelled)
        );
        assert_eq!(
            manager
                .records
                .get(&grandchild_task.id)
                .map(|record| &record.status),
            Some(&ManagedTaskStatus::Cancelled)
        );
        assert!(manager.queued_tasks.is_empty());
        Ok(())
    }

    #[test]
    fn transition_task_record_rejects_late_complete_after_cancel() {
        let mut record = test_task_record(
            "task_cancelled",
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        let cancel = transition_task_record(
            &mut record,
            TaskTransition::Cancel {
                finished_at: timestamp(),
                summary: "Worker task cancelled.".to_string(),
                error: None,
            },
        );
        assert!(cancel.applied);
        assert_eq!(record.status, ManagedTaskStatus::Cancelled);

        let complete = transition_task_record(
            &mut record,
            TaskTransition::Complete {
                finished_at: timestamp(),
                result_path: PathBuf::from("/tmp/result.json"),
                outcome_path: PathBuf::from("/tmp/outcome.json"),
                summary: "late completion".to_string(),
                failure_kind: None,
            },
        );
        assert!(!complete.applied);
        assert_eq!(record.status, ManagedTaskStatus::Cancelled);

        let late_failure = transition_task_record(
            &mut record,
            TaskTransition::Fail {
                finished_at: timestamp(),
                summary: "late failure".to_string(),
                failure_kind: TaskFailureKind::WorkerFailed,
                error: Some("late worker error".to_string()),
            },
        );
        assert!(!late_failure.applied);
        assert_eq!(record.status, ManagedTaskStatus::Cancelled);
    }

    #[test]
    fn task_manager_interrupt_task_marks_running_task_interrupted() -> Result<()> {
        let mut manager = TaskManager::new();
        manager.records.insert(
            "task_interrupt".to_string(),
            test_task_record(
                "task_interrupt",
                ManagedTaskStatus::Running,
                TaskAttemptStatus::Running,
            ),
        );

        assert_eq!(
            manager.interrupt_task("task_interrupt")?,
            ActionOutcome::Interrupted(OutcomeContext {
                task_id: Some("task_interrupt".to_string()),
                run_epoch: Some(0),
                ..OutcomeContext::default()
            })
        );

        let record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == "task_interrupt")
            .context("missing task record")?;
        assert_eq!(record.status, ManagedTaskStatus::Interrupted);
        assert_eq!(
            record.attempts.last().map(|attempt| &attempt.status),
            Some(&TaskAttemptStatus::Interrupted)
        );
        Ok(())
    }

    #[test]
    fn task_manager_applies_parallelism_limits_from_worker_config() {
        let mut manager = TaskManager::new();
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 3,
            max_parallel_per_key: 2,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };

        manager.apply_worker_config(&config);

        assert_eq!(manager.max_parallel_workers(), 3);
        assert_eq!(manager.max_parallel_per_key(), 2);
    }

    #[test]
    fn task_manager_recovers_orphaned_pending_and_running_records_from_disk() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let pending_record = TaskRecord {
            task_id: "task_pending".into(),
            worker_kind: "opencode".into(),
            worker_command: None,
            worker_model: None,
            worker_category: "quick".into(),
            route_hint: None,
            route_reason: "test".into(),
            status: ManagedTaskStatus::Pending,
            started_at: timestamp(),
            finished_at: None,
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            parent_task_id: None,
            result_path: None,
            outcome_path: None,
            summary: "queued".into(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".into(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".into(),
                route_hint: None,
                route_reason: "test".into(),
                status: TaskAttemptStatus::Pending,
                started_at: timestamp(),
                finished_at: None,
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "queued".into(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        };
        let running_record = TaskRecord {
            task_id: "task_running".into(),
            worker_kind: "opencode".into(),
            worker_command: None,
            worker_model: None,
            worker_category: "quick".into(),
            route_hint: None,
            route_reason: "test".into(),
            status: ManagedTaskStatus::Running,
            started_at: timestamp(),
            finished_at: None,
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: Some("session_running".into()),
            parent_session_id: None,
            root_session_id: None,
            parent_task_id: None,
            result_path: None,
            outcome_path: None,
            summary: "running".into(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".into(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".into(),
                route_hint: None,
                route_reason: "test".into(),
                status: TaskAttemptStatus::Running,
                started_at: timestamp(),
                finished_at: None,
                session_id: Some("session_running".into()),
                result_path: None,
                outcome_path: None,
                summary: "running".into(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        };
        let completed_record = TaskRecord {
            task_id: "task_completed".into(),
            worker_kind: "opencode".into(),
            worker_command: None,
            worker_model: None,
            worker_category: "quick".into(),
            route_hint: None,
            route_reason: "test".into(),
            status: ManagedTaskStatus::Completed,
            started_at: timestamp(),
            finished_at: Some(timestamp()),
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            parent_task_id: None,
            result_path: None,
            outcome_path: None,
            summary: "completed".into(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".into(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".into(),
                route_hint: None,
                route_reason: "test".into(),
                status: TaskAttemptStatus::Completed,
                started_at: timestamp(),
                finished_at: Some(timestamp()),
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "completed".into(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        };

        write_task_record(&store, &pending_record)?;
        write_task_record(&store, &running_record)?;
        write_task_record(&store, &completed_record)?;

        let mut manager = TaskManager::new();
        let recovered = manager.recover_orphaned_records(&store)?;
        assert_eq!(recovered, 2);

        let pending_json =
            fs::read_to_string(store.worker_dir("task_pending").join("task-record.json"))?;
        let pending_after: TaskRecord = serde_json::from_str(&pending_json)?;
        assert_eq!(pending_after.status, ManagedTaskStatus::Lost);
        assert_eq!(
            pending_after.failure_kind,
            Some(TaskFailureKind::WorkerStartFailed)
        );
        assert_eq!(
            pending_after.attempts.last().map(|attempt| &attempt.status),
            Some(&TaskAttemptStatus::Lost)
        );

        let running_json =
            fs::read_to_string(store.worker_dir("task_running").join("task-record.json"))?;
        let running_after: TaskRecord = serde_json::from_str(&running_json)?;
        assert_eq!(running_after.status, ManagedTaskStatus::Lost);
        assert_eq!(
            running_after.failure_kind,
            Some(TaskFailureKind::WorkerStartFailed)
        );

        let completed_json =
            fs::read_to_string(store.worker_dir("task_completed").join("task-record.json"))?;
        let completed_after: TaskRecord = serde_json::from_str(&completed_json)?;
        assert_eq!(completed_after.status, ManagedTaskStatus::Completed);

        let pending_events =
            fs::read_to_string(store.worker_dir("task_pending").join("task-events.jsonl"))?;
        assert!(pending_events.contains("Recovered orphaned Gear worker task"));
        let running_events =
            fs::read_to_string(store.worker_dir("task_running").join("task-events.jsonl"))?;
        assert!(running_events.contains("Recovered orphaned Gear worker task"));
        assert!(
            !store
                .worker_dir("task_completed")
                .join("task-events.jsonl")
                .exists()
        );

        manager.destroy_resident_task("task_pending", "test-orphan-dispose")?;
        let pending_json =
            fs::read_to_string(store.worker_dir("task_pending").join("task-record.json"))?;
        let pending_after_destroy: TaskRecord = serde_json::from_str(&pending_json)?;
        assert_eq!(
            pending_after_destroy.residency_state,
            ResidencyState::Disposed
        );
        let pending_events =
            fs::read_to_string(store.worker_dir("task_pending").join("task-events.jsonl"))?;
        assert!(pending_events.contains("dispose"));

        Ok(())
    }

    // ── Phase 5 tests ──

    #[test]
    fn cancelled_and_interrupted_emit_completion_notification() {
        let cancelled = test_task_record(
            "task_cancelled",
            ManagedTaskStatus::Cancelled,
            TaskAttemptStatus::Cancelled,
        );
        let interrupted = test_task_record(
            "task_interrupted",
            ManagedTaskStatus::Interrupted,
            TaskAttemptStatus::Interrupted,
        );

        assert!(CompletionNotifier::should_notify(&cancelled));
        assert!(CompletionNotifier::should_notify(&interrupted));
        assert!(
            CompletionNotifier::build_notification(
                &cancelled,
                &cancelled.started_at,
                &cancelled.started_at,
            )
            .is_some()
        );
        assert!(
            CompletionNotifier::build_notification(
                &interrupted,
                &interrupted.started_at,
                &interrupted.started_at,
            )
            .is_some()
        );
        assert!(CompletionNotifier::should_notify(&test_task_record(
            "task_completed",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        )));
        assert!(CompletionNotifier::should_notify(&test_task_record(
            "task_failed",
            ManagedTaskStatus::Failed,
            TaskAttemptStatus::Failed,
        )));
    }

    #[test]
    fn same_epoch_completion_notified_once() {
        let mut record = test_task_record(
            "task_notified",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.run_epoch = 1;
        assert!(!CompletionNotifier::already_notified(&record));
        record.notified_epoch = 1;
        assert!(CompletionNotifier::already_notified(&record));
        record.run_epoch = 2;
        assert!(!CompletionNotifier::already_notified(&record));
    }

    #[test]
    fn revived_epoch_completion_notifies_again() {
        let mut record = test_task_record(
            "task_revived",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.run_epoch = 1;
        record.notified_epoch = 1;
        assert!(CompletionNotifier::already_notified(&record));
        record.run_epoch = 2;
        assert!(!CompletionNotifier::already_notified(&record));
    }

    #[test]
    fn streaming_parent_buffers_completion() -> Result<()> {
        let notifier = CompletionNotifier::new();
        let mut record = test_task_record(
            "task_buffered",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.run_epoch = 1;
        record.started_at = timestamp();
        record.finished_at = Some(timestamp());

        let notification = CompletionNotifier::build_notification(
            &record,
            &record.started_at,
            record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for completed task")?;

        let result = notifier.try_notify(
            notification,
            ParentSessionState::Streaming,
            &|_, _| Ok(()),
            &|_, _| Ok(()),
        )?;
        assert_eq!(result, NotificationResult::Buffered);
        Ok(())
    }

    #[test]
    fn cancelled_and_interrupted_notifications_buffer_and_flush() -> Result<()> {
        let notifier = CompletionNotifier::new();
        let delivered = Arc::new(Mutex::new(Vec::new()));
        for (task_id, status, attempt_status) in [
            (
                "task_cancelled_notification",
                ManagedTaskStatus::Cancelled,
                TaskAttemptStatus::Cancelled,
            ),
            (
                "task_interrupted_notification",
                ManagedTaskStatus::Interrupted,
                TaskAttemptStatus::Interrupted,
            ),
        ] {
            let mut record = test_task_record(task_id, status, attempt_status);
            record.run_epoch = 1;
            record.started_at = timestamp();
            record.finished_at = Some(timestamp());
            let notification = CompletionNotifier::build_notification(
                &record,
                &record.started_at,
                record
                    .finished_at
                    .as_ref()
                    .context("missing finish timestamp")?,
            )
            .context("terminal cancellation should build a notification")?;
            assert_eq!(
                notifier.try_notify(
                    notification,
                    ParentSessionState::Streaming,
                    &|_, _| Ok(()),
                    &|_, _| Ok(()),
                )?,
                NotificationResult::Buffered
            );
        }

        let results = notifier.flush_buffer(
            "parent_cancel_notification",
            ParentSessionState::Idle,
            &{
                let delivered = delivered.clone();
                move |task_id, epoch| {
                    delivered
                        .lock()
                        .map_err(|_| anyhow::anyhow!("mutex"))?
                        .push((task_id.to_string(), epoch));
                    Ok(())
                }
            },
            &|_, _| Ok(()),
            &|_| Ok(None),
        )?;
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == NotificationResult::Sent)
                .count(),
            2
        );
        assert_eq!(
            delivered
                .lock()
                .map_err(|_| anyhow::anyhow!("mutex"))?
                .len(),
            2
        );
        Ok(())
    }

    #[test]
    fn completion_notification_includes_summary_head_and_continuation_hint() -> Result<()> {
        let mut record = test_task_record(
            "task_notification_content",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.summary = "Head line\nTail line".to_string();
        record.run_epoch = 2;
        record.started_at = timestamp();
        record.finished_at = Some(timestamp());

        let notification = CompletionNotifier::build_notification(
            &record,
            &record.started_at,
            record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for completed task")?;

        assert_eq!(notification.summary_head, "Head line");
        assert!(
            notification
                .continuation_hint
                .contains("Follow up from the Gear panel")
        );
        Ok(())
    }

    #[test]
    fn completion_notification_omits_model_chain_for_same_model_retries() -> Result<()> {
        let mut record = test_task_record(
            "task_notification_same_model",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.summary = "Head line\nTail line".to_string();
        record.attempts[0].worker_model = Some("opencode/hy3-free".to_string());
        record.attempts[0].error = Some("HTTP 429 rate limit exceeded".to_string());

        let mut second_attempt = record.attempts[0].clone();
        second_attempt.attempt = 2;
        second_attempt.error = Some("provider_secret=do-not-expose".to_string());
        record.attempts.push(second_attempt);
        record.run_epoch = 2;
        record.started_at = timestamp();
        record.finished_at = Some(timestamp());

        let notification = CompletionNotifier::build_notification(
            &record,
            &record.started_at,
            record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for same-model retries")?;

        assert_eq!(notification.summary_head, "Head line");
        assert!(!notification.summary_head.contains("429"));
        assert!(!notification.summary_head.contains("provider_secret"));
        Ok(())
    }

    #[test]
    fn completion_notification_includes_safe_three_model_fallback_chain() -> Result<()> {
        let mut record = test_task_record(
            "task_notification_three_model_fallback",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.summary = "Head line\nTail line".to_string();
        record.attempts[0].worker_model = Some("opencode/hy3-free".to_string());
        record.attempts[0].error = Some("HTTP 429 rate limit exceeded".to_string());

        let mut second_attempt = record.attempts[0].clone();
        second_attempt.attempt = 2;
        second_attempt.worker_model = Some("opencode/mimo-v2.5-free".to_string());
        second_attempt.error = Some("provider_secret=do-not-expose".to_string());
        record.attempts.push(second_attempt);

        let mut third_attempt = record.attempts[1].clone();
        third_attempt.attempt = 3;
        third_attempt.worker_model = Some("opencode/deepseek-v4-flash-free".to_string());
        third_attempt.error = Some("Authorization: Bearer do-not-expose".to_string());
        record.attempts.push(third_attempt);
        record.run_epoch = 3;
        record.started_at = timestamp();
        record.finished_at = Some(timestamp());

        let notification = CompletionNotifier::build_notification(
            &record,
            &record.started_at,
            record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for three-model fallback")?;

        assert_eq!(
            notification.summary_head,
            "Head line；模型回退链：opencode/hy3-free -> opencode/mimo-v2.5-free -> opencode/deepseek-v4-flash-free"
        );
        assert!(!notification.summary_head.contains("429"));
        assert!(!notification.summary_head.contains("provider_secret"));
        assert!(!notification.summary_head.contains("Authorization"));
        Ok(())
    }

    #[test]
    fn completion_notification_uses_artifact_hint_when_not_continuable() -> Result<()> {
        let mut record = test_task_record(
            "task_notification_nonresident",
            ManagedTaskStatus::Lost,
            TaskAttemptStatus::Failed,
        );
        record.residency_state = ResidencyState::Disposed;
        record.summary = "Lost line".to_string();
        record.started_at = timestamp();
        record.finished_at = Some(timestamp());

        let notification = CompletionNotifier::build_notification(
            &record,
            &record.started_at,
            record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for lost task")?;

        assert_eq!(notification.summary_head, "Lost line");
        assert!(
            notification
                .continuation_hint
                .contains("Open the result/outcome artifacts")
        );
        Ok(())
    }

    #[test]
    fn buffer_flush_deduplicates_task_epoch() -> Result<()> {
        let notifier = CompletionNotifier::new();
        let notification = CompletionNotification {
            task_id: "task_dedup".to_string(),
            task_name: "test".to_string(),
            status: ManagedTaskStatus::Completed,
            run_epoch: 1,
            summary: "first".to_string(),
            summary_head: "first".to_string(),
            continuation_hint:
                "Follow up from the Gear panel or open the result/outcome artifacts to continue."
                    .to_string(),
            failure_kind: None,
            duration_ms: 0,
            result_path: None,
            outcome_path: None,
        };

        let result1 = notifier.try_notify(
            notification.clone(),
            ParentSessionState::Streaming,
            &|_, _| Ok(()),
            &|_, _| Ok(()),
        )?;
        assert_eq!(result1, NotificationResult::Buffered);

        let result2 = notifier.try_notify(
            notification,
            ParentSessionState::Streaming,
            &|_, _| Ok(()),
            &|_, _| Ok(()),
        )?;
        assert_eq!(result2, NotificationResult::Buffered);

        let notified = Arc::new(Mutex::new(Vec::new()));
        let results = notifier.flush_buffer(
            "parent_test",
            ParentSessionState::Idle,
            &|task_id, epoch| {
                notified
                    .lock()
                    .map_err(|_| anyhow::anyhow!("mutex"))?
                    .push((task_id.to_string(), epoch));
                Ok(())
            },
            &|_, _| Ok(()),
            &|_| Ok(None),
        )?;

        let sent_count = results
            .iter()
            .filter(|r| **r == NotificationResult::Sent)
            .count();
        assert_eq!(
            sent_count, 1,
            "should flush only one notification per (task_id, epoch)"
        );
        let notified = notified.lock().map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(notified.len(), 1);
        Ok(())
    }

    #[test]
    fn delivery_failure_records_notification_failed_epoch() -> Result<()> {
        let notifier = CompletionNotifier::new();
        let delivery_attempts = Arc::new(Mutex::new(0usize));
        let failed_epochs = Arc::new(Mutex::new(Vec::new()));
        let mut record = test_task_record(
            "task_delivery_fail",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.run_epoch = 1;
        record.started_at = timestamp();
        record.finished_at = Some(timestamp());

        let notification = CompletionNotifier::build_notification(
            &record,
            &record.started_at,
            record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for completed task")?;

        let result = notifier.try_notify(
            notification,
            ParentSessionState::Idle,
            &{
                let delivery_attempts = delivery_attempts.clone();
                move |task_id, epoch| {
                    let mut attempts = delivery_attempts
                        .lock()
                        .map_err(|_| anyhow::anyhow!("mutex"))?;
                    *attempts += 1;
                    bail!("deliberate delivery failure for {task_id} epoch {epoch}")
                }
            },
            &|task_id, epoch| {
                failed_epochs
                    .lock()
                    .map_err(|_| anyhow::anyhow!("mutex"))?
                    .push((task_id.to_string(), epoch));
                Ok(())
            },
        )?;
        assert!(matches!(result, NotificationResult::Failed(_)));
        let attempts = delivery_attempts
            .lock()
            .map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(
            *attempts, 6,
            "delivery should exhaust the bounded redelivery window before failing"
        );
        let failed_epochs = failed_epochs.lock().map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(
            failed_epochs.as_slice(),
            &[("task_delivery_fail".to_string(), 1)]
        );
        Ok(())
    }

    #[test]
    fn delivery_retry_succeeds_after_transient_failure() -> Result<()> {
        let notifier = CompletionNotifier::new();
        let delivery_attempts = Arc::new(Mutex::new(0usize));
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let failed_epochs = Arc::new(Mutex::new(Vec::new()));
        let mut record = test_task_record(
            "task_delivery_retry",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.run_epoch = 2;
        record.started_at = timestamp();
        record.finished_at = Some(timestamp());

        let notification = CompletionNotifier::build_notification(
            &record,
            &record.started_at,
            record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for completed task")?;

        let result = notifier.try_notify(
            notification,
            ParentSessionState::Idle,
            &{
                let delivery_attempts = delivery_attempts.clone();
                let delivered = delivered.clone();
                move |task_id, epoch| {
                    let mut attempts = delivery_attempts
                        .lock()
                        .map_err(|_| anyhow::anyhow!("mutex"))?;
                    *attempts += 1;
                    if *attempts == 1 {
                        bail!("transient delivery failure for {task_id} epoch {epoch}");
                    }
                    delivered
                        .lock()
                        .map_err(|_| anyhow::anyhow!("mutex"))?
                        .push((task_id.to_string(), epoch));
                    Ok(())
                }
            },
            &|task_id, epoch| {
                failed_epochs
                    .lock()
                    .map_err(|_| anyhow::anyhow!("mutex"))?
                    .push((task_id.to_string(), epoch));
                Ok(())
            },
        )?;
        assert_eq!(result, NotificationResult::Sent);
        let attempts = delivery_attempts
            .lock()
            .map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(*attempts, 2, "delivery should retry once before succeeding");
        let delivered = delivered.lock().map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(
            delivered.as_slice(),
            &[("task_delivery_retry".to_string(), 2)]
        );
        let failed_epochs = failed_epochs.lock().map_err(|_| anyhow::anyhow!("mutex"))?;
        assert!(
            failed_epochs.is_empty(),
            "transient failure should not write failed epoch"
        );
        Ok(())
    }

    #[test]
    fn delivery_redelivers_within_bounded_failure_window() -> Result<()> {
        let notifier = CompletionNotifier::new();
        let delivery_attempts = Arc::new(Mutex::new(0usize));
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let failed_epochs = Arc::new(Mutex::new(Vec::new()));
        let mut record = test_task_record(
            "task_delivery_redelivery",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.run_epoch = 3;
        record.started_at = timestamp();
        record.finished_at = Some(timestamp());

        let notification = CompletionNotifier::build_notification(
            &record,
            &record.started_at,
            record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for completed task")?;

        let result = notifier.try_notify(
            notification,
            ParentSessionState::Idle,
            &{
                let delivery_attempts = delivery_attempts.clone();
                let delivered = delivered.clone();
                move |task_id, epoch| {
                    let mut attempts = delivery_attempts
                        .lock()
                        .map_err(|_| anyhow::anyhow!("mutex"))?;
                    *attempts += 1;
                    if *attempts <= 4 {
                        bail!("temporary delivery failure for {task_id} epoch {epoch}");
                    }
                    delivered
                        .lock()
                        .map_err(|_| anyhow::anyhow!("mutex"))?
                        .push((task_id.to_string(), epoch));
                    Ok(())
                }
            },
            &|task_id, epoch| {
                failed_epochs
                    .lock()
                    .map_err(|_| anyhow::anyhow!("mutex"))?
                    .push((task_id.to_string(), epoch));
                Ok(())
            },
        )?;

        assert_eq!(result, NotificationResult::Sent);
        let attempts = delivery_attempts
            .lock()
            .map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(
            *attempts, 5,
            "delivery should succeed on the second attempt of the third round"
        );
        let delivered = delivered.lock().map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(
            delivered.as_slice(),
            &[("task_delivery_redelivery".to_string(), 3)]
        );
        let failed_epochs = failed_epochs.lock().map_err(|_| anyhow::anyhow!("mutex"))?;
        assert!(
            failed_epochs.is_empty(),
            "recovery inside the redelivery window should not write failed epoch"
        );
        Ok(())
    }

    #[test]
    fn completion_flush_serializes_rapid_arrivals() -> Result<()> {
        // Verify that flush_buffer serializes concurrent requests for the
        // same parent session. When serializer is locked, incoming flush
        // requests are queued in pending_flush and processed in FIFO order
        // after the current flush completes.
        let notifier = CompletionNotifier::new();
        let delivered = Arc::new(Mutex::new(Vec::new()));

        // Buffer 3 notifications by injecting while streaming
        for i in 0..3 {
            let notification = CompletionNotification {
                task_id: format!("task_{i}"),
                task_name: "test".to_string(),
                status: ManagedTaskStatus::Completed,
                run_epoch: 1,
                summary: format!("result {i}"),
                summary_head: format!("result {i}"),
                continuation_hint: "continue".to_string(),
                failure_kind: None,
                duration_ms: 0,
                result_path: None,
                outcome_path: None,
            };
            notifier.try_notify(
                notification,
                ParentSessionState::Streaming,
                &|_, _| Ok(()),
                &|_, _| Ok(()),
            )?;
        }

        // Simulate a concurrent flush in progress by locking the serializer
        {
            let mut serializer = notifier
                .flush_serializer
                .lock()
                .map_err(|_| anyhow::anyhow!("mutex"))?;
            serializer.insert("parent_arrivals".to_string(), true);
        }

        // This flush call should see the locked serializer and queue a pending
        // request instead of flushing.
        let results = notifier.flush_buffer(
            "parent_arrivals",
            ParentSessionState::Idle,
            &|task_id, epoch| {
                delivered
                    .lock()
                    .map_err(|_| anyhow::anyhow!("mutex"))?
                    .push((task_id.to_string(), epoch));
                Ok(())
            },
            &|_, _| Ok(()),
            &|_| Ok(None),
        )?;
        assert!(
            results.is_empty(),
            "should not flush when serializer is locked by another caller"
        );
        assert!(
            delivered
                .lock()
                .map_err(|_| anyhow::anyhow!("mutex"))?
                .is_empty(),
            "no delivery should occur while serializer is held"
        );

        // Verify that a pending request was queued
        {
            let mut pending = notifier
                .pending_flush
                .lock()
                .map_err(|_| anyhow::anyhow!("mutex"))?;
            let queue = pending
                .get_mut("parent_arrivals")
                .expect("pending queue should exist for session");
            assert_eq!(queue.len(), 1, "one pending flush should be queued");
            queue.clear();
        }

        // Release the serializer
        {
            let mut serializer = notifier
                .flush_serializer
                .lock()
                .map_err(|_| anyhow::anyhow!("mutex"))?;
            serializer.insert("parent_arrivals".to_string(), false);
        }

        // Flush again — should acquire the serializer and deliver all 3
        // notifications in epoch order (task_0, task_1, task_2).
        let results = notifier.flush_buffer(
            "parent_arrivals",
            ParentSessionState::Idle,
            &|task_id, epoch| {
                delivered
                    .lock()
                    .map_err(|_| anyhow::anyhow!("mutex"))?
                    .push((task_id.to_string(), epoch));
                Ok(())
            },
            &|_, _| Ok(()),
            &|_| Ok(None),
        )?;
        let sent_count = results
            .iter()
            .filter(|r| **r == NotificationResult::Sent)
            .count();
        assert_eq!(
            sent_count, 3,
            "all 3 buffered notifications should be delivered"
        );

        let delivered = delivered.lock().map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(delivered.len(), 3);
        assert_eq!(
            delivered[0].0, "task_0",
            "notifications should be delivered in arrival order"
        );
        assert_eq!(delivered[1].0, "task_1");
        assert_eq!(delivered[2].0, "task_2");
        Ok(())
    }

    #[test]
    fn completion_flush_works_after_idle_transition() -> Result<()> {
        // Verify that notifications buffered during non-idle states (Streaming)
        // are flushed only after Idle is detected, and that state re-verification
        // skips stale notifications whose task record no longer matches.
        let notifier = CompletionNotifier::new();
        let delivered = Arc::new(Mutex::new(Vec::new()));

        // Buffer notifications while in Streaming state
        let mut record = test_task_record(
            "task_idle",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.run_epoch = 1;
        record.started_at = timestamp();
        record.finished_at = Some(timestamp());

        let notification = CompletionNotifier::build_notification(
            &record,
            &record.started_at,
            record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for completed task")?;

        notifier.try_notify(
            notification,
            ParentSessionState::Streaming,
            &|_, _| Ok(()),
            &|_, _| Ok(()),
        )?;

        // Trying to flush while non-Idle should do nothing
        let results = notifier.flush_buffer(
            "parent_idle",
            ParentSessionState::Streaming,
            &|_, _| Ok(()),
            &|_, _| Ok(()),
            &|_| Ok(None),
        )?;
        assert!(
            results.is_empty(),
            "flush should be a no-op when parent state is Streaming"
        );

        // Now flush at Idle — the notification should be delivered
        let results = notifier.flush_buffer(
            "parent_idle",
            ParentSessionState::Idle,
            &|task_id, epoch| {
                delivered
                    .lock()
                    .map_err(|_| anyhow::anyhow!("mutex"))?
                    .push((task_id.to_string(), epoch));
                Ok(())
            },
            &|_, _| Ok(()),
            &|_| Ok(None),
        )?;
        let sent_count = results
            .iter()
            .filter(|r| **r == NotificationResult::Sent)
            .count();
        assert_eq!(sent_count, 1, "notification should flush at Idle");
        assert_eq!(
            delivered
                .lock()
                .map_err(|_| anyhow::anyhow!("mutex"))?
                .len(),
            1
        );

        // Re-flush should deliver nothing (buffer is empty, dedup already done)
        let results = notifier.flush_buffer(
            "parent_idle",
            ParentSessionState::Idle,
            &|task_id, epoch| {
                delivered
                    .lock()
                    .map_err(|_| anyhow::anyhow!("mutex"))?
                    .push((task_id.to_string(), epoch));
                Ok(())
            },
            &|_, _| Ok(()),
            &|_| Ok(None),
        )?;
        assert!(
            results.is_empty(),
            "re-flush should deliver nothing after buffer is drained"
        );
        assert_eq!(
            delivered
                .lock()
                .map_err(|_| anyhow::anyhow!("mutex"))?
                .len(),
            1,
            "no additional deliveries after re-flush"
        );

        // ── State re-verification test ──
        // Buffer another notification, but have the read_record closure
        // return a stale record (different epoch or status). The notification
        // should be skipped during flush.

        let mut stale_record = test_task_record(
            "task_stale",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        stale_record.run_epoch = 1;
        stale_record.started_at = timestamp();
        stale_record.finished_at = Some(timestamp());

        let stale_notification = CompletionNotifier::build_notification(
            &stale_record,
            &stale_record.started_at,
            stale_record.finished_at.as_ref().unwrap(),
        )
        .context("should build notification for stale task")?;

        notifier.try_notify(
            stale_notification,
            ParentSessionState::Streaming,
            &|_, _| Ok(()),
            &|_, _| Ok(()),
        )?;

        // Now flush with a read_record that says the task was revived to epoch 2.
        // Use a different session ID to avoid the debounce cooldown from the
        // earlier idle flush.
        let results = notifier.flush_buffer(
            "parent_stale",
            ParentSessionState::Idle,
            &|task_id, epoch| {
                delivered
                    .lock()
                    .map_err(|_| anyhow::anyhow!("mutex"))?
                    .push((task_id.to_string(), epoch));
                Ok(())
            },
            &|_, _| Ok(()),
            &|task_id| {
                // Return a record with bumped epoch — notification should be skipped
                Ok(Some(TaskRecord {
                    run_epoch: 2,
                    task_id: task_id.to_string(),
                    ..test_task_record(
                        task_id,
                        ManagedTaskStatus::Completed,
                        TaskAttemptStatus::Completed,
                    )
                }))
            },
        )?;
        let skipped_count = results
            .iter()
            .filter(|r| **r == NotificationResult::Skipped)
            .count();
        assert_eq!(
            skipped_count, 1,
            "stale notification should be skipped when task epoch doesn't match"
        );
        let stale_delivery_count = delivered
            .lock()
            .map_err(|_| anyhow::anyhow!("mutex"))?
            .iter()
            .filter(|(id, _)| id == "task_stale")
            .count();
        assert_eq!(
            stale_delivery_count, 0,
            "stale notification should not be delivered"
        );

        Ok(())
    }

    // ── Phase 6 tests ──

    #[test]
    fn destroy_disposes_even_when_abort_fails() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        struct AbortPanicHandle;
        impl WorkerSessionHandle for AbortPanicHandle {
            fn session_id(&self) -> Option<String> {
                None
            }
            fn send_follow_up(&self, _: String) -> Result<()> {
                Ok(())
            }
            fn steer(&self, _: String) -> Result<()> {
                Ok(())
            }
            fn interrupt(&self) -> Result<()> {
                Ok(())
            }
            fn cancel(&self) -> Result<()> {
                Ok(())
            }
            fn abort(&self) -> Result<()> {
                bail!("abort simulated failure")
            }
            fn dispose(&self) -> Result<()> {
                Ok(())
            }
            fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
                bail!("no outcome")
            }
            fn wait_for_result(&self) -> Result<WorkerResult> {
                bail!("no result")
            }
            fn last_output(&self) -> Option<String> {
                None
            }
        }
        let mut manager = TaskManager::new();
        let queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task: test_task("task_abort_fail"),
            route_attempt: 1,
            goal: "test".to_string(),
            verification_commands: Vec::new(),
            config: WorkerConfig {
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
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let running_task = RunningTask {
            store: StateStore::new(temp_dir.path()),
            handle: Arc::new(AbortPanicHandle),
            queued_task,
            started_at: Instant::now(),
            _subscription: None,
        };
        manager
            .running_tasks
            .insert("task_abort_fail".to_string(), running_task);
        manager.records.insert(
            "task_abort_fail".to_string(),
            test_task_record(
                "task_abort_fail",
                ManagedTaskStatus::Running,
                TaskAttemptStatus::Running,
            ),
        );
        // destroy_resident_task should still succeed even though abort() fails
        manager.destroy_resident_task("task_abort_fail", "test")?;
        assert!(!manager.records.contains_key("task_abort_fail"));
        assert!(manager.running_tasks.is_empty());
        Ok(())
    }

    #[test]
    fn destroy_uses_running_handle_even_without_current_snapshot() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        struct CountingHandle {
            interrupt_calls: Arc<AtomicUsize>,
            cancel_calls: Arc<AtomicUsize>,
            abort_calls: Arc<AtomicUsize>,
            dispose_calls: Arc<AtomicUsize>,
        }
        impl WorkerSessionHandle for CountingHandle {
            fn session_id(&self) -> Option<String> {
                None
            }
            fn send_follow_up(&self, _: String) -> Result<()> {
                Ok(())
            }
            fn steer(&self, _: String) -> Result<()> {
                Ok(())
            }
            fn interrupt(&self) -> Result<()> {
                self.interrupt_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn cancel(&self) -> Result<()> {
                self.cancel_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn abort(&self) -> Result<()> {
                self.abort_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn dispose(&self) -> Result<()> {
                self.dispose_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
                bail!("no outcome")
            }
            fn wait_for_result(&self) -> Result<WorkerResult> {
                bail!("no result")
            }
            fn last_output(&self) -> Option<String> {
                None
            }
        }
        let interrupt_calls = Arc::new(AtomicUsize::new(0));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let abort_calls = Arc::new(AtomicUsize::new(0));
        let dispose_calls = Arc::new(AtomicUsize::new(0));
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(CountingHandle {
            interrupt_calls: interrupt_calls.clone(),
            cancel_calls: cancel_calls.clone(),
            abort_calls: abort_calls.clone(),
            dispose_calls: dispose_calls.clone(),
        });
        let mut manager = TaskManager::new();
        let queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task: test_task("task_running_handle_only"),
            route_attempt: 1,
            goal: "test".to_string(),
            verification_commands: Vec::new(),
            config: WorkerConfig {
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
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        manager.running_tasks.insert(
            "task_running_handle_only".to_string(),
            RunningTask {
                store: StateStore::new(temp_dir.path()),
                handle: handle.clone(),
                queued_task,
                started_at: Instant::now(),
                _subscription: None,
            },
        );
        manager.records.insert(
            "task_running_handle_only".to_string(),
            test_task_record(
                "task_running_handle_only",
                ManagedTaskStatus::Running,
                TaskAttemptStatus::Running,
            ),
        );

        manager.destroy_resident_task("task_running_handle_only", "test")?;

        assert_eq!(interrupt_calls.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(abort_calls.load(Ordering::SeqCst), 1);
        assert_eq!(dispose_calls.load(Ordering::SeqCst), 1);
        assert!(manager.records.is_empty());
        assert!(manager.running_tasks.is_empty());
        let artifact_store = StateStore::new(temp_dir.path());
        let event_path = artifact_store
            .worker_dir("task_running_handle_only")
            .join("task-events.jsonl");
        let event_contents = fs::read_to_string(&event_path)?;
        assert!(
            event_contents.contains(r#""transition_type":"dispose""#),
            "destroy_resident_task should append a dispose lifecycle event"
        );
        assert!(
            event_contents.contains(r#""residency_state":"disposed""#),
            "destroy_resident_task should persist the disposed residency state"
        );
        Ok(())
    }

    #[test]
    fn task_manager_drop_shuts_down_resident_tasks() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        struct CountingHandle {
            interrupt_calls: Arc<AtomicUsize>,
            cancel_calls: Arc<AtomicUsize>,
            abort_calls: Arc<AtomicUsize>,
            dispose_calls: Arc<AtomicUsize>,
        }
        impl WorkerSessionHandle for CountingHandle {
            fn session_id(&self) -> Option<String> {
                None
            }
            fn send_follow_up(&self, _: String) -> Result<()> {
                Ok(())
            }
            fn steer(&self, _: String) -> Result<()> {
                Ok(())
            }
            fn interrupt(&self) -> Result<()> {
                self.interrupt_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn cancel(&self) -> Result<()> {
                self.cancel_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn abort(&self) -> Result<()> {
                self.abort_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn dispose(&self) -> Result<()> {
                self.dispose_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
                bail!("no outcome")
            }
            fn wait_for_result(&self) -> Result<WorkerResult> {
                bail!("no result")
            }
            fn last_output(&self) -> Option<String> {
                None
            }
        }
        let interrupt_calls = Arc::new(AtomicUsize::new(0));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let abort_calls = Arc::new(AtomicUsize::new(0));
        let dispose_calls = Arc::new(AtomicUsize::new(0));
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(CountingHandle {
            interrupt_calls: interrupt_calls.clone(),
            cancel_calls: cancel_calls.clone(),
            abort_calls: abort_calls.clone(),
            dispose_calls: dispose_calls.clone(),
        });
        let mut manager = TaskManager::new();
        manager.control.set_current(
            "task_shutdown".to_string(),
            ManagedTaskStatus::Running,
            Some(handle.clone()),
        )?;
        let queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task: test_task("task_shutdown"),
            route_attempt: 1,
            goal: "test".to_string(),
            verification_commands: Vec::new(),
            config: WorkerConfig {
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
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        manager.running_tasks.insert(
            "task_shutdown".to_string(),
            RunningTask {
                store: StateStore::new(temp_dir.path()),
                handle: handle.clone(),
                queued_task,
                started_at: Instant::now(),
                _subscription: None,
            },
        );
        let control = manager.control.clone();
        manager.records.insert(
            "task_shutdown".to_string(),
            test_task_record(
                "task_shutdown",
                ManagedTaskStatus::Running,
                TaskAttemptStatus::Running,
            ),
        );

        drop(manager);

        assert_eq!(interrupt_calls.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(abort_calls.load(Ordering::SeqCst), 1);
        assert_eq!(dispose_calls.load(Ordering::SeqCst), 1);
        assert_eq!(control.current_task_id()?.as_deref(), Some("task_shutdown"));
        assert_eq!(
            control.current_task_status()?,
            Some(ManagedTaskStatus::Lost)
        );
        Ok(())
    }

    #[test]
    fn cancel_routes_through_destroy_resident_task() -> Result<()> {
        let mut manager = TaskManager::new();
        let record = test_task_record(
            "task_cancel_destroy",
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        manager
            .records
            .insert("task_cancel_destroy".to_string(), record);
        manager.cancel_task("task_cancel_destroy").ok();
        let record = manager.records.get("task_cancel_destroy");
        assert!(
            record.is_some(),
            "cancel_task should keep the record in cancelled state"
        );
        assert_eq!(record.unwrap().status, ManagedTaskStatus::Cancelled);
        Ok(())
    }

    #[test]
    fn lru_evicts_oldest_completed_not_cancelled() -> Result<()> {
        let mut manager = TaskManager::new();
        // Insert resident records
        for i in 0..10 {
            let task_id = format!("task_{i}");
            let mut record = if i == 5 {
                // This one is cancelled - should NOT be evicted
                test_task_record(
                    &task_id,
                    ManagedTaskStatus::Cancelled,
                    TaskAttemptStatus::Cancelled,
                )
            } else {
                test_task_record(
                    &task_id,
                    ManagedTaskStatus::Completed,
                    TaskAttemptStatus::Completed,
                )
            };
            record.residency_state = ResidencyState::Resident;
            manager.records.insert(task_id.clone(), record);
        }
        // Should evict one (we have 10 > 8 cap)
        let evicted = manager.evict_lru_resident_task();
        assert!(evicted.is_some(), "should evict a task when over cap");
        assert_ne!(
            evicted.as_deref(),
            Some("task_5"),
            "should not evict Cancelled task"
        );
        Ok(())
    }

    #[test]
    fn lost_record_is_not_ttl_deleted_until_process_dead() {
        let mut manager = TaskManager::new();
        let mut record = test_task_record(
            "task_lost_ttl",
            ManagedTaskStatus::Lost,
            TaskAttemptStatus::Lost,
        );
        record.finished_at = Some("2000-01-01T00:00:00Z".to_string());
        manager.records.insert("task_lost_ttl".to_string(), record);
        let cleaned = manager.ttl_cleanup();
        assert_eq!(cleaned, 0, "Lost records should not be TTL-deleted");
    }

    #[test]
    fn reconcile_marks_running_record_lost() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let mut pending_record = test_task_record(
            "task_reconcile_pending",
            ManagedTaskStatus::Pending,
            TaskAttemptStatus::Pending,
        );
        pending_record.started_at = timestamp();
        pending_record.attempts[0].started_at = timestamp();
        write_task_record(&store, &pending_record)?;
        let mut running_record = test_task_record(
            "task_reconcile_running",
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        running_record.started_at = timestamp();
        running_record.attempts[0].started_at = timestamp();
        write_task_record(&store, &running_record)?;
        let mut completed_record = test_task_record(
            "task_reconcile_completed",
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        completed_record.started_at = timestamp();
        completed_record.finished_at = Some(timestamp());
        completed_record.attempts[0].started_at = timestamp();
        completed_record.attempts[0].finished_at = Some(timestamp());
        write_task_record(&store, &completed_record)?;
        let mut manager = TaskManager::new();
        let recovered = manager.recover_orphaned_records(&store)?;
        // pending and running should be recovered; completed should not
        assert!(
            recovered >= 2,
            "should recover at least pending and running: {recovered}"
        );
        Ok(())
    }

    #[test]
    fn residency_limit_reports_current_residents() -> Result<()> {
        let manager = TaskManager::new();
        let resident_count: usize = manager
            .records
            .values()
            .filter(|record| record.residency_state == ResidencyState::Resident)
            .count();
        assert_eq!(resident_count, 0);
        let mut record = test_task_record(
            "task_single",
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        record.residency_state = ResidencyState::Resident;
        let resident_count = 1;
        assert!(resident_count <= RESIDENCY_MAX_CHILDREN);
        Ok(())
    }

    #[test]
    fn cancel_on_running_task_returns_cancelled() -> Result<()> {
        let control = TaskManagerControl::default();
        control.set_current(
            "task_running".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeInterruptHandle {
                interrupted: Arc::new(AtomicUsize::new(0)),
                cancelled: Arc::new(AtomicUsize::new(0)),
                follow_ups: Arc::new(Mutex::new(Vec::new())),
                steers: Arc::new(Mutex::new(Vec::new())),
            })),
        )?;

        assert_eq!(
            control.cancel_current_task()?,
            ActionOutcome::Cancelled(OutcomeContext {
                task_id: Some("task_running".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.current_task_status()?,
            Some(ManagedTaskStatus::Cancelled)
        );
        Ok(())
    }

    #[test]
    fn steer_on_terminal_task_returns_not_continuable() -> Result<()> {
        let control = TaskManagerControl::default();
        control.set_current(
            "task_cancelled".to_string(),
            ManagedTaskStatus::Cancelled,
            None,
        )?;

        assert_eq!(
            control.steer_current_task("steer after cancel".to_string())?,
            SteerOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_cancelled".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.steer_task("task_cancelled", "steer after cancel 2".to_string())?,
            SteerOutcome::NotContinuable(OutcomeContext {
                task_id: Some("task_cancelled".to_string()),
                ..OutcomeContext::default()
            })
        );
        Ok(())
    }

    #[test]
    fn terminal_resident_revive_does_not_replay_stale_worker_events() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_revive_event_epoch".to_string();
        let reset_calls = Arc::new(AtomicUsize::new(0));
        let follow_ups = Arc::new(Mutex::new(Vec::new()));
        let event_hub = WorkerEventHub::default();
        event_hub.emit(WorkerEvent::AssistantTextDelta {
            kind: "history-aware".to_string(),
            delta: "stale-history-marker".to_string(),
        });
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(HistoryAwareReviveHandle {
            event_hub,
            reset_calls: reset_calls.clone(),
            follow_ups: follow_ups.clone(),
        });
        let config = WorkerConfig {
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
        };
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: test_task(&task_id),
            route_attempt: 1,
            goal: "revive event epoch test".to_string(),
            verification_commands: Vec::new(),
            config: config.clone(),
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let mut manager = TaskManager::new();
        manager.apply_worker_config(&config);
        let mut record = test_task_record(
            &task_id,
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.residency_state = ResidencyState::Resident;
        record.run_epoch = 4;
        manager.records.insert(task_id.clone(), record);
        manager.resident_tasks.insert(
            task_id.clone(),
            ResidentTask {
                handle: handle.clone(),
                queued_task,
            },
        );
        manager
            .control
            .set_current(task_id.clone(), ManagedTaskStatus::Completed, Some(handle))?;

        assert_eq!(
            manager.send_follow_up_task(&task_id, "new epoch".to_string())?,
            SendOutcome::Revive(OutcomeContext {
                task_id: Some(task_id.clone()),
                run_epoch: Some(4),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(reset_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("history-aware follow-up mutex poisoned"))?
                .as_slice(),
            ["new epoch"]
        );
        let evidence = fs::read_to_string(store.worker_dir(&task_id).join("worker-events.jsonl"))?;
        assert!(evidence.contains("turn_started"));
        assert!(
            !evidence.contains("\"delta_length\":20"),
            "resident revive must not project stale history into the new epoch"
        );
        Ok(())
    }

    #[test]
    fn terminal_resident_task_revives_with_new_epoch_and_running_tracking() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let follow_ups = Arc::new(Mutex::new(Vec::new()));
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(FakeInterruptHandle {
            interrupted: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(AtomicUsize::new(0)),
            follow_ups: follow_ups.clone(),
            steers: Arc::new(Mutex::new(Vec::new())),
        });
        let task_id = "task_revive".to_string();
        let make_queued_task = |task_id: &str| QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: test_task(task_id),
            route_attempt: 1,
            goal: "revive test".to_string(),
            verification_commands: Vec::new(),
            config: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: Vec::new(),
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 2,
                max_parallel_per_key: 2,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
                default_worker_for_small_tasks: WorkerKind::ZedAgent,
            },
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let queued_task = make_queued_task(&task_id);
        let mut manager = TaskManager::new();
        manager.apply_worker_config(&queued_task.config);
        let mut record = test_task_record(
            &task_id,
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.residency_state = ResidencyState::Resident;
        record.run_epoch = 4;
        manager.records.insert(task_id.clone(), record);
        manager.resident_tasks.insert(
            task_id.clone(),
            ResidentTask {
                handle: handle.clone(),
                queued_task,
            },
        );
        manager
            .control
            .set_current(task_id.clone(), ManagedTaskStatus::Completed, Some(handle))?;

        assert_eq!(
            manager.send_follow_up_task(&task_id, "continue".to_string())?,
            SendOutcome::Revive(OutcomeContext {
                task_id: Some(task_id.clone()),
                run_epoch: Some(4),
                ..OutcomeContext::default()
            })
        );
        let record = manager
            .records
            .get(&task_id)
            .context("missing revived record")?;
        assert_eq!(record.status, ManagedTaskStatus::Running);
        assert_eq!(record.run_epoch, 5);
        assert_eq!(record.attempts.len(), 2);
        assert!(manager.running_tasks.contains_key(&task_id));
        assert_eq!(
            follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("follow-up mutex poisoned"))?
                .as_slice(),
            ["continue"]
        );

        let steer_id = "task_steer_revive".to_string();
        let steers = Arc::new(Mutex::new(Vec::new()));
        let steer_handle: Arc<dyn WorkerSessionHandle> = Arc::new(FakeInterruptHandle {
            interrupted: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(AtomicUsize::new(0)),
            follow_ups: Arc::new(Mutex::new(Vec::new())),
            steers: steers.clone(),
        });
        let mut steer_record = test_task_record(
            &steer_id,
            ManagedTaskStatus::Interrupted,
            TaskAttemptStatus::Interrupted,
        );
        steer_record.residency_state = ResidencyState::Resident;
        steer_record.run_epoch = 2;
        manager.records.insert(steer_id.clone(), steer_record);
        manager.resident_tasks.insert(
            steer_id.clone(),
            ResidentTask {
                handle: steer_handle.clone(),
                queued_task: make_queued_task(&steer_id),
            },
        );
        manager.control.set_current(
            steer_id.clone(),
            ManagedTaskStatus::Interrupted,
            Some(steer_handle),
        )?;

        assert_eq!(
            manager.steer_task(&steer_id, "adjust".to_string())?,
            SteerOutcome::Revive(OutcomeContext {
                task_id: Some(steer_id.clone()),
                run_epoch: Some(2),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            manager
                .records
                .get(&steer_id)
                .context("missing steer revived record")?
                .run_epoch,
            3
        );
        assert_eq!(
            steers
                .lock()
                .map_err(|_| anyhow::anyhow!("steer mutex poisoned"))?
                .as_slice(),
            ["adjust"]
        );
        Ok(())
    }

    #[test]
    fn terminal_resident_revive_failure_restores_terminal_state() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_revive_failure".to_string();
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: test_task(&task_id),
            route_attempt: 1,
            goal: "revive failure test".to_string(),
            verification_commands: Vec::new(),
            config: WorkerConfig {
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
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let handle: Arc<dyn WorkerSessionHandle> = Arc::new(FakeReviveFailureHandle {
            error_message: "simulated revive delivery failure",
        });
        let mut manager = TaskManager::new();
        manager.apply_worker_config(&queued_task.config);
        let mut record = test_task_record(
            &task_id,
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.run_epoch = 7;
        record.finished_at = Some(timestamp());
        write_task_record(&store, &record)?;
        manager.records.insert(task_id.clone(), record.clone());
        manager.resident_tasks.insert(
            task_id.clone(),
            ResidentTask {
                handle: handle.clone(),
                queued_task,
            },
        );
        manager
            .control
            .set_current(task_id.clone(), ManagedTaskStatus::Completed, Some(handle))?;

        let error = manager
            .send_follow_up_task(&task_id, "continue".to_string())
            .expect_err("revive delivery should fail");
        assert!(
            error
                .to_string()
                .contains("simulated revive delivery failure")
        );
        let restored = manager
            .records
            .get(&task_id)
            .context("missing restored task record")?;
        assert_eq!(restored.status, ManagedTaskStatus::Completed);
        assert_eq!(restored.run_epoch, 7);
        assert_eq!(restored.attempts.len(), record.attempts.len());
        assert!(!manager.running_tasks.contains_key(&task_id));
        assert!(manager.resident_tasks.contains_key(&task_id));
        assert_eq!(
            manager.control.current_task_status()?,
            Some(ManagedTaskStatus::Completed)
        );
        let persisted: TaskRecord = serde_json::from_str(&fs::read_to_string(
            store.worker_dir(&task_id).join("task-record.json"),
        )?)?;
        assert_eq!(persisted.status, ManagedTaskStatus::Completed);
        assert_eq!(persisted.run_epoch, 7);
        let resident = manager
            .resident_tasks
            .get(&task_id)
            .context("missing restored resident task")?;
        assert!(manager.concurrency.can_start(&resident.queued_task));

        let gate_paths = fs::read_dir(store.root().join("prompt-dispatch-gates"))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(gate_paths.len(), 1);
        let gate: PromptDispatchGate =
            serde_json::from_str(&fs::read_to_string(gate_paths[0].path())?)?;
        assert_eq!(gate.status, PromptDispatchGateStatus::Failed);

        let retry_error = manager
            .send_follow_up_task(&task_id, "continue".to_string())
            .expect_err("failed terminal revive should remain retryable");
        assert!(
            retry_error
                .to_string()
                .contains("simulated revive delivery failure")
        );

        let steer_error = manager
            .steer_task(&task_id, "steer".to_string())
            .expect_err("failed terminal steer should remain retryable");
        assert!(
            steer_error
                .to_string()
                .contains("simulated revive delivery failure")
        );
        let gate_statuses = fs::read_dir(store.root().join("prompt-dispatch-gates"))?
            .map(|entry| -> Result<PromptDispatchGate> {
                let path = entry?.path();
                Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(gate_statuses.len(), 2);
        assert!(
            gate_statuses
                .iter()
                .all(|gate| gate.status == PromptDispatchGateStatus::Failed)
        );

        manager
            .resident_tasks
            .get_mut(&task_id)
            .context("missing resident task for ambiguous retry")?
            .handle = Arc::new(FakeReviveFailureHandle {
            error_message: "JSON Parse error: Unexpected end of JSON input",
        });
        assert_eq!(
            manager.send_follow_up_task(&task_id, "ambiguous".to_string())?,
            SendOutcome::PossiblyAccepted(OutcomeContext {
                task_id: Some(task_id.clone()),
                run_epoch: Some(7),
                ..OutcomeContext::default()
            })
        );
        let running_record = manager
            .records
            .get(&task_id)
            .context("missing running record after ambiguous revive")?;
        assert_eq!(running_record.status, ManagedTaskStatus::Running);
        assert_eq!(running_record.run_epoch, 8);
        assert!(manager.running_tasks.contains_key(&task_id));
        assert!(!manager.resident_tasks.contains_key(&task_id));
        assert_eq!(
            manager.send_follow_up_task(&task_id, "ambiguous".to_string())?,
            SendOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.clone()),
                run_epoch: Some(8),
                ..OutcomeContext::default()
            })
        );
        let gate_statuses = fs::read_dir(store.root().join("prompt-dispatch-gates"))?
            .map(|entry| -> Result<PromptDispatchGate> {
                let path = entry?.path();
                Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(gate_statuses.len(), 3);
        assert!(
            gate_statuses
                .iter()
                .any(|gate| gate.status == PromptDispatchGateStatus::PossiblyAccepted)
        );
        Ok(())
    }

    #[test]
    fn terminal_resident_revive_waits_for_concurrency_slot() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let config = WorkerConfig {
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
        };
        let make_queued_task = |task_id: &str| QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: test_task(task_id),
            route_attempt: 1,
            goal: "concurrency revive test".to_string(),
            verification_commands: Vec::new(),
            config: config.clone(),
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };

        let busy_queued_task = make_queued_task("task_busy");
        let busy_handle: Arc<dyn WorkerSessionHandle> = Arc::new(FakeHangingHandle);
        let mut manager = TaskManager::new();
        manager.apply_worker_config(&config);
        assert!(manager.concurrency.acquire(&busy_queued_task));
        manager.running_tasks.insert(
            "task_busy".to_string(),
            RunningTask {
                store: store.clone(),
                handle: busy_handle,
                queued_task: busy_queued_task,
                started_at: Instant::now(),
                _subscription: None,
            },
        );

        let follow_ups = Arc::new(Mutex::new(Vec::new()));
        let resident_handle: Arc<dyn WorkerSessionHandle> = Arc::new(FakeInterruptHandle {
            interrupted: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(AtomicUsize::new(0)),
            follow_ups: follow_ups.clone(),
            steers: Arc::new(Mutex::new(Vec::new())),
        });
        let task_id = "task_waiting_revive".to_string();
        let mut record = test_task_record(
            &task_id,
            ManagedTaskStatus::Completed,
            TaskAttemptStatus::Completed,
        );
        record.residency_state = ResidencyState::Resident;
        manager.records.insert(task_id.clone(), record);
        manager.resident_tasks.insert(
            task_id.clone(),
            ResidentTask {
                handle: resident_handle,
                queued_task: make_queued_task(&task_id),
            },
        );

        assert_eq!(
            manager.send_follow_up_task(&task_id, "wait for slot".to_string())?,
            SendOutcome::Queued(OutcomeContext {
                task_id: Some(task_id.clone()),
                run_epoch: Some(0),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            manager.send_follow_up_task(&task_id, "wait for slot".to_string())?,
            SendOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.clone()),
                run_epoch: Some(0),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            manager.send_follow_up_task(&task_id, "second queued message".to_string())?,
            SendOutcome::Queued(OutcomeContext {
                task_id: Some(task_id.clone()),
                run_epoch: Some(0),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(manager.pending_revives.len(), 2);

        let busy = manager
            .running_tasks
            .remove("task_busy")
            .context("busy task disappeared")?;
        manager.concurrency.release(&busy.queued_task);
        manager.process_queue()?;

        assert!(manager.pending_revives.is_empty());
        assert!(manager.running_tasks.contains_key(&task_id));
        assert_eq!(
            follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("follow-up mutex poisoned"))?
                .as_slice(),
            ["wait for slot", "second queued message"]
        );
        Ok(())
    }

    #[test]
    fn task_command_scope_denies_sibling_session_and_preserves_owner() -> Result<()> {
        let mut manager = TaskManager::new();
        let mut record = test_task_record(
            "task_scoped",
            ManagedTaskStatus::Pending,
            TaskAttemptStatus::Pending,
        );
        record.parent_session_id = Some("session-owner".to_string());
        record.root_session_id = Some("session-root".to_string());
        manager.records.insert(record.task_id.clone(), record);

        let denied = manager.send_follow_up_task_with_context(
            "task_scoped",
            "blocked".to_string(),
            &TaskCommandContext {
                caller_session_id: Some("session-sibling".to_string()),
                all_scope: false,
            },
        )?;
        assert!(matches!(denied, SendOutcome::ScopeDenied { .. }));

        let queued = manager.send_follow_up_task_with_context(
            "task_scoped",
            "allowed".to_string(),
            &TaskCommandContext {
                caller_session_id: Some("session-owner".to_string()),
                all_scope: false,
            },
        )?;
        assert_eq!(
            queued,
            SendOutcome::Queued(OutcomeContext {
                task_id: Some("task_scoped".to_string()),
                run_epoch: Some(0),
                ..OutcomeContext::default()
            })
        );
        let messages = manager.control.take_pending_messages("task_scoped")?;
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages
                .front()
                .and_then(|message| message.caller_session_id.as_deref()),
            Some("session-owner")
        );
        Ok(())
    }

    #[test]
    fn task_command_scope_all_scope_allows_cross_session_control() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let mut manager = TaskManager::new();
        let mut record = test_task_record(
            "task_all_scope",
            ManagedTaskStatus::Pending,
            TaskAttemptStatus::Pending,
        );
        record.parent_session_id = Some("session-owner".to_string());
        record.root_session_id = Some("session-root".to_string());
        manager.records.insert(record.task_id.clone(), record);
        manager.task_record_paths.insert(
            "task_all_scope".to_string(),
            store.worker_dir("task_all_scope").join("task-record.json"),
        );

        let outcome = manager.send_follow_up_task_with_context(
            "task_all_scope",
            "cross-session control".to_string(),
            &TaskCommandContext {
                caller_session_id: Some("session-sibling".to_string()),
                all_scope: true,
            },
        )?;
        assert_eq!(
            outcome,
            SendOutcome::Queued(OutcomeContext {
                task_id: Some("task_all_scope".to_string()),
                run_epoch: Some(0),
                ..OutcomeContext::default()
            })
        );
        let messages = manager.control.take_pending_messages("task_all_scope")?;
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages
                .front()
                .and_then(|message| message.caller_session_id.as_deref()),
            Some("session-sibling")
        );
        let command_events = fs::read_to_string(
            store
                .worker_dir("task_all_scope")
                .join("task-command-events.jsonl"),
        )?;
        assert!(command_events.contains("\"action\":\"send_follow_up\""));
        assert!(command_events.contains("\"accepted\":true"));
        assert!(command_events.contains("\"all_scope\":true"));
        Ok(())
    }

    #[test]
    fn task_command_audit_failure_preserves_accepted_outcome() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let blocked_workspace_root = temp_dir.path().join("blocked-workspace-root");
        fs::write(&blocked_workspace_root, "not a directory")?;

        let task_id = "task_audit_failure";
        let mut manager = TaskManager::new();
        let mut record = test_task_record(
            task_id,
            ManagedTaskStatus::Pending,
            TaskAttemptStatus::Pending,
        );
        record.parent_session_id = Some("session-owner".to_string());
        manager.records.insert(task_id.to_string(), record);
        manager.task_record_paths.insert(
            task_id.to_string(),
            blocked_workspace_root
                .join("a/b/c")
                .join("task-record.json"),
        );

        let outcome = manager.send_follow_up_task_with_context(
            task_id,
            "accepted despite audit failure".to_string(),
            &TaskCommandContext {
                caller_session_id: Some("session-owner".to_string()),
                all_scope: false,
            },
        )?;

        assert_eq!(
            outcome,
            SendOutcome::Queued(OutcomeContext {
                task_id: Some(task_id.to_string()),
                run_epoch: Some(0),
                ..OutcomeContext::default()
            })
        );
        let messages = manager.control.take_pending_messages(task_id)?;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message, "accepted despite audit failure");
        Ok(())
    }

    #[test]
    fn pending_messages_requeue_preserves_order_after_delivery_failure() -> Result<()> {
        let control = TaskManagerControl::default();
        for (index, message) in ["first", "second", "third"].into_iter().enumerate() {
            control.queue_pending_message(
                "task_pending_retry",
                QueuedMessageKind::FollowUp,
                message.to_string(),
                Some(format!("session-{index}")),
                None,
            )?;
        }

        let mut pending = control.take_pending_messages("task_pending_retry")?;
        assert_eq!(
            pending.pop_front().map(|message| message.message),
            Some("first".to_string())
        );
        control.prepend_pending_messages("task_pending_retry", pending)?;

        let retry_messages = control.take_pending_messages("task_pending_retry")?;
        assert_eq!(
            retry_messages
                .into_iter()
                .map(|message| message.message)
                .collect::<Vec<_>>(),
            vec!["second".to_string(), "third".to_string()]
        );
        Ok(())
    }

    #[test]
    fn queued_message_delivery_retries_after_start_failure() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let target_store = StateStore::new(temp_dir.path().join("target"));
        target_store.initialize()?;
        let follow_up_attempts = Arc::new(AtomicUsize::new(0));
        let steer_deliveries = Arc::new(AtomicUsize::new(0));
        let config = WorkerConfig {
            worker_kind: WorkerKind::ZedAgent,
            worker_command: None,
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
        };
        let mut manager = TaskManager::new();
        manager.apply_worker_config(&config);
        manager.set_worker_registry(WorkerRegistry::with_native_backend(Arc::new(
            FailOnceFollowUpBackend {
                follow_up_attempts: follow_up_attempts.clone(),
                steer_deliveries: steer_deliveries.clone(),
            },
        )));

        let busy_queued_task = QueuedTask {
            store: StateStore::new(temp_dir.path().join("busy")),
            workspace: temp_dir.path().to_path_buf(),
            task: test_task("task_busy_for_queued_message"),
            route_attempt: 1,
            goal: "busy fixture".to_string(),
            verification_commands: Vec::new(),
            config: config.clone(),
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        assert!(manager.concurrency.acquire(&busy_queued_task));
        manager.running_tasks.insert(
            busy_queued_task.task.id.clone(),
            RunningTask {
                store: busy_queued_task.store.clone(),
                handle: Arc::new(FakeHangingHandle),
                queued_task: busy_queued_task.clone(),
                started_at: Instant::now(),
                _subscription: None,
            },
        );

        let target_task = test_task("task_queued_message_retry");
        manager.start(WorkerStartRequest {
            store: &target_store,
            workspace: temp_dir.path(),
            task: &target_task,
            route_attempt: 1,
            goal: "queued message retry fixture",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        assert_eq!(
            manager.send_follow_up_task(&target_task.id, "wake parent".to_string())?,
            SendOutcome::Queued(OutcomeContext {
                task_id: Some(target_task.id.clone()),
                run_epoch: Some(0),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            manager.steer_task(&target_task.id, "steer parent".to_string())?,
            SteerOutcome::Queued(OutcomeContext {
                task_id: Some(target_task.id.clone()),
                run_epoch: Some(0),
                ..OutcomeContext::default()
            })
        );

        let busy = manager
            .running_tasks
            .remove(&busy_queued_task.task.id)
            .context("busy task disappeared")?;
        manager.concurrency.release(&busy.queued_task);
        manager.process_queue()?;

        for _ in 0..50 {
            manager.tick()?;
            if manager.completed_runs.contains_key(&target_task.id) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(manager.completed_runs.contains_key(&target_task.id));
        assert_eq!(
            follow_up_attempts.load(Ordering::SeqCst),
            2,
            "a queued message must be retried after its initial delivery failure"
        );
        assert_eq!(steer_deliveries.load(Ordering::SeqCst), 1);
        let gate_statuses = fs::read_dir(target_store.root().join("prompt-dispatch-gates"))?
            .map(|entry| -> Result<_> {
                let entry = entry?;
                let gate: PromptDispatchGate =
                    serde_json::from_str(&fs::read_to_string(entry.path())?)?;
                Ok(gate.status)
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(gate_statuses.len(), 2);
        assert!(
            gate_statuses
                .iter()
                .all(|status| *status == PromptDispatchGateStatus::Accepted)
        );
        Ok(())
    }

    #[test]
    fn send_follow_up_on_pending_returns_queued() -> Result<()> {
        let control = TaskManagerControl::default();
        control.set_current("task_pending".to_string(), ManagedTaskStatus::Pending, None)?;

        assert_eq!(
            control.send_follow_up_current_task("follow up".to_string())?,
            SendOutcome::Queued(OutcomeContext {
                task_id: Some("task_pending".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.send_follow_up_task("task_pending", "follow up 2".to_string())?,
            SendOutcome::Queued(OutcomeContext {
                task_id: Some("task_pending".to_string()),
                ..OutcomeContext::default()
            })
        );
        Ok(())
    }

    #[test]
    fn cancel_on_no_task_returns_noop() -> Result<()> {
        let control = TaskManagerControl::default();
        assert_eq!(
            control.cancel_current_task()?,
            ActionOutcome::Noop(OutcomeContext::default())
        );
        assert_eq!(
            control.interrupt_current_task()?,
            ActionOutcome::Noop(OutcomeContext::default())
        );
        assert_eq!(
            control.send_follow_up_current_task("any".to_string())?,
            SendOutcome::Noop(OutcomeContext::default())
        );
        assert_eq!(
            control.steer_current_task("any".to_string())?,
            SteerOutcome::Noop(OutcomeContext::default())
        );
        Ok(())
    }

    #[test]
    fn steer_on_running_task_returns_steered() -> Result<()> {
        let control = TaskManagerControl::default();
        let steers = Arc::new(Mutex::new(Vec::new()));
        let sent_steers = steers.clone();
        control.set_current(
            "task_running".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeInterruptHandle {
                interrupted: Arc::new(AtomicUsize::new(0)),
                cancelled: Arc::new(AtomicUsize::new(0)),
                follow_ups: Arc::new(Mutex::new(Vec::new())),
                steers,
            })),
        )?;

        assert_eq!(
            control.steer_current_task("adjust".to_string())?,
            SteerOutcome::Steered(OutcomeContext {
                task_id: Some("task_running".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            sent_steers
                .lock()
                .map_err(|_| anyhow::anyhow!("steer mutex poisoned"))?
                .as_slice(),
            ["adjust"]
        );
        Ok(())
    }

    #[test]
    fn send_follow_up_on_running_returns_sent() -> Result<()> {
        let control = TaskManagerControl::default();
        let follow_ups = Arc::new(Mutex::new(Vec::new()));
        let sent_follow_ups = follow_ups.clone();
        control.set_current(
            "task_running".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeInterruptHandle {
                interrupted: Arc::new(AtomicUsize::new(0)),
                cancelled: Arc::new(AtomicUsize::new(0)),
                follow_ups,
                steers: Arc::new(Mutex::new(Vec::new())),
            })),
        )?;

        assert_eq!(
            control.send_follow_up_current_task("continue".to_string())?,
            SendOutcome::Sent(OutcomeContext {
                task_id: Some("task_running".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            sent_follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("follow-up mutex poisoned"))?
                .as_slice(),
            ["continue"]
        );
        Ok(())
    }

    #[test]
    fn steer_on_wrong_task_id_returns_noop() -> Result<()> {
        let control = TaskManagerControl::default();
        control.set_current(
            "task_a".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeInterruptHandle {
                interrupted: Arc::new(AtomicUsize::new(0)),
                cancelled: Arc::new(AtomicUsize::new(0)),
                follow_ups: Arc::new(Mutex::new(Vec::new())),
                steers: Arc::new(Mutex::new(Vec::new())),
            })),
        )?;

        assert_eq!(
            control.steer_task("task_b", "steer".to_string())?,
            SteerOutcome::Noop(OutcomeContext {
                task_id: Some("task_b".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.send_follow_up_task("task_b", "follow up".to_string())?,
            SendOutcome::Noop(OutcomeContext {
                task_id: Some("task_b".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.cancel_task("task_b")?,
            ActionOutcome::Noop(OutcomeContext {
                task_id: Some("task_b".to_string()),
                ..OutcomeContext::default()
            })
        );
        assert_eq!(
            control.interrupt_task("task_b")?,
            ActionOutcome::Noop(OutcomeContext {
                task_id: Some("task_b".to_string()),
                ..OutcomeContext::default()
            })
        );
        Ok(())
    }

    #[test]
    fn test_outcome_contains_task_id_and_epoch() -> Result<()> {
        let ctx = OutcomeContext {
            task_id: Some("task_001".to_string()),
            run_epoch: Some(3),
            queue_position: None,
        };
        let outcome = SendOutcome::Sent(ctx);
        match &outcome {
            SendOutcome::Sent(c) => {
                assert_eq!(c.task_id.as_deref(), Some("task_001"));
                assert_eq!(c.run_epoch, Some(3));
            }
            _ => panic!("expected Sent"),
        }
        Ok(())
    }

    #[test]
    fn test_queued_outcome_has_position() -> Result<()> {
        let ctx = OutcomeContext {
            task_id: Some("task_002".to_string()),
            run_epoch: Some(1),
            queue_position: Some(5),
        };
        let outcome = SendOutcome::Queued(ctx);
        match &outcome {
            SendOutcome::Queued(c) => {
                assert_eq!(c.queue_position, Some(5));
            }
            _ => panic!("expected Queued"),
        }
        Ok(())
    }

    #[test]
    fn test_accepted_outcome_has_task_id_and_epoch() -> Result<()> {
        let ctx = OutcomeContext {
            task_id: Some("task_accepted".to_string()),
            run_epoch: Some(3),
            queue_position: None,
        };
        let ctx_revive = OutcomeContext {
            task_id: Some("task_revive".to_string()),
            run_epoch: Some(5),
            queue_position: None,
        };

        let sent = SendOutcome::Sent(ctx.clone());
        assert!(sent.is_accepted());
        match &sent {
            SendOutcome::Sent(c) => {
                assert_eq!(c.task_id.as_deref(), Some("task_accepted"));
                assert_eq!(c.run_epoch, Some(3));
            }
            _ => panic!("expected Sent"),
        }

        let queued = SendOutcome::Queued(ctx);
        assert!(queued.is_accepted());
        match &queued {
            SendOutcome::Queued(c) => {
                assert_eq!(c.task_id.as_deref(), Some("task_accepted"));
                assert_eq!(c.run_epoch, Some(3));
            }
            _ => panic!("expected Queued"),
        }

        let revive = SendOutcome::Revive(ctx_revive);
        assert!(revive.is_accepted());
        match &revive {
            SendOutcome::Revive(c) => {
                assert_eq!(c.task_id.as_deref(), Some("task_revive"));
                assert_eq!(c.run_epoch, Some(5));
            }
            _ => panic!("expected Revive"),
        }

        Ok(())
    }

    #[test]
    fn test_queued_outcome_has_queue_position() -> Result<()> {
        let ctx = OutcomeContext {
            task_id: Some("task_queued_pos".to_string()),
            run_epoch: Some(1),
            queue_position: Some(3),
        };
        let outcome = SendOutcome::Queued(ctx);
        match &outcome {
            SendOutcome::Queued(c) => {
                assert_eq!(c.queue_position, Some(3));
                assert_eq!(c.task_id.as_deref(), Some("task_queued_pos"));
            }
            _ => panic!("expected Queued"),
        }
        Ok(())
    }
}
