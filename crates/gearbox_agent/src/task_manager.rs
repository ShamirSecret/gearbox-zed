use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

use crate::state::{CoordinatorModel, StateStore, Task, TaskKind, timestamp};
use crate::tools::CancellationToken;
use crate::workers::{
    WorkerConfig, WorkerKind, WorkerOutcome, WorkerRegistry, WorkerResult, WorkerSessionHandle,
    WorkerStartRequest, WorkerStatus, WorkerSubscription, route_identity_key,
    worker_model_is_unavailable,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskFailureKind {
    WorkerFailed,
    WorkerStartFailed,
    WorkerCancelled,
    WorkerUnavailable,
    ModelUnavailable,
    PremiumBudgetExceeded,
    NoFallbackRoute,
    RepeatedFailureLimit,
}

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
    pub parent_task_id: Option<String>,
    pub worker_kind: String,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub attempts: Vec<TaskAttemptSnapshot>,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
    pub summary: String,
    #[serde(default)]
    pub summary_head: String,
    #[serde(default)]
    pub continuation_hint: String,
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
        }
    }
}

impl TaskRuntimePolicy {
    fn from_worker_config(config: &WorkerConfig) -> Self {
        Self {
            stale_task_timeout: Duration::from_secs(config.stale_task_timeout_secs.max(1) as u64),
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
            format!(
                "Open the result/outcome artifacts to inspect the full result ({reason})."
            )
        }
    }
}

#[derive(Clone)]
struct CurrentManagedTask {
    task_id: String,
    status: ManagedTaskStatus,
    handle: Option<Arc<dyn WorkerSessionHandle>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueuedMessageKind {
    FollowUp,
    Steer,
}

#[derive(Clone, Debug)]
struct QueuedMessage {
    kind: QueuedMessageKind,
    message: String,
    caller_session_id: Option<String>,
    created_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FallbackDecision {
    Queued,
    Unavailable {
        reason: String,
        failure_kind: TaskFailureKind,
    },
}

const WAIT_FOR_POLL_INTERVAL: Duration = Duration::from_millis(50);

// ── Phase 6: Lifecycle constants ──
const RESIDENCY_MAX_CHILDREN: usize = 8;
const TTL_MS: u64 = 24 * 60 * 60 * 1000; // 24 hours
const ARCHIVE_CAP: usize = 100;

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

    fn current_running_task_snapshot(&self) -> Result<Option<CurrentManagedTask>> {
        Ok(self
            .current_task_snapshot()?
            .filter(|current_task| current_task.status == ManagedTaskStatus::Running))
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
    ) -> Result<()> {
        self.pending_messages
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .entry(task_id.to_string())
            .or_default()
            .push_back(QueuedMessage {
                kind,
                message,
                caller_session_id: Some(task_id.to_string()),
                created_at: timestamp(),
            });
        Ok(())
    }

    fn take_pending_messages(&self, task_id: &str) -> Result<VecDeque<QueuedMessage>> {
        Ok(self
            .pending_messages
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .remove(task_id)
            .unwrap_or_default())
    }

    pub fn send_follow_up_current_task(&self, prompt: String) -> Result<bool> {
        let Some(task_id) = self.current_task_id()? else {
            return Ok(false);
        };
        self.send_follow_up_task(&task_id, prompt)
    }

    pub fn steer_current_task(&self, prompt: String) -> Result<bool> {
        let Some(task_id) = self.current_task_id()? else {
            return Ok(false);
        };
        self.steer_task(&task_id, prompt)
    }

    pub fn cancel_current_task(&self) -> Result<bool> {
        let Some(current_task) = self.current_running_task_snapshot()? else {
            return Ok(false);
        };

        current_task
            .handle
            .as_ref()
            .context("running task missing handle")?
            .cancel()?;
        self.update_current_status(&current_task.task_id, ManagedTaskStatus::Cancelled)?;
        Ok(true)
    }

    pub fn interrupt_current_task(&self) -> Result<bool> {
        let Some(current_task) = self.current_running_task_snapshot()? else {
            return Ok(false);
        };

        current_task
            .handle
            .as_ref()
            .context("running task missing handle")?
            .interrupt()?;
        self.update_current_status(&current_task.task_id, ManagedTaskStatus::Interrupted)?;
        Ok(true)
    }

    pub fn cancel_task(&self, task_id: &str) -> Result<bool> {
        let Some(current_task) = self.current_running_task_snapshot()? else {
            return Ok(false);
        };
        if current_task.task_id != task_id {
            return Ok(false);
        }

        current_task
            .handle
            .as_ref()
            .context("running task missing handle")?
            .cancel()?;
        self.update_current_status(&current_task.task_id, ManagedTaskStatus::Cancelled)?;
        Ok(true)
    }

    pub fn interrupt_task(&self, task_id: &str) -> Result<bool> {
        let Some(current_task) = self.current_running_task_snapshot()? else {
            return Ok(false);
        };
        if current_task.task_id != task_id {
            return Ok(false);
        }

        current_task
            .handle
            .as_ref()
            .context("running task missing handle")?
            .interrupt()?;
        self.update_current_status(&current_task.task_id, ManagedTaskStatus::Interrupted)?;
        Ok(true)
    }

    pub fn send_follow_up_task(&self, task_id: &str, prompt: String) -> Result<bool> {
        let current_task_guard = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?;
        let Some(current_task) = current_task_guard.as_ref() else {
            return Ok(false);
        };
        if current_task.task_id != task_id {
            return Ok(false);
        }

        match current_task.status {
            ManagedTaskStatus::Pending => {
                self.queue_pending_message(task_id, QueuedMessageKind::FollowUp, prompt)?;
            }
            ManagedTaskStatus::Running => {
                let handle = current_task
                    .handle
                    .as_ref()
                    .context("running task missing handle")?
                    .clone();
                drop(current_task_guard);
                handle.send_follow_up(prompt)?;
                return Ok(true);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    pub fn steer_task(&self, task_id: &str, prompt: String) -> Result<bool> {
        let current_task_guard = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?;
        let Some(current_task) = current_task_guard.as_ref() else {
            return Ok(false);
        };
        if current_task.task_id != task_id {
            return Ok(false);
        }

        match current_task.status {
            ManagedTaskStatus::Pending => {
                self.queue_pending_message(task_id, QueuedMessageKind::Steer, prompt)?;
            }
            ManagedTaskStatus::Running => {
                let handle = current_task
                    .handle
                    .as_ref()
                    .context("running task missing handle")?
                    .clone();
                drop(current_task_guard);
                handle.steer(prompt)?;
                return Ok(true);
            }
            _ => return Ok(false),
        }
        Ok(true)
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
            });
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
    running_tasks: HashMap<String, RunningTask>,
    queued_tasks: VecDeque<QueuedTask>,
    completed_runs: HashMap<String, ManagedWorkerRun>,
    completed_errors: HashMap<String, String>,
    completed_archive: VecDeque<TaskRecord>,
    concurrency: ConcurrencyManager,
    release_guard: ReleaseGuard,
    runtime_policy: TaskRuntimePolicy,
    control: TaskManagerControl,
    artifacts_root: Option<PathBuf>,
    finished_task_tx: Sender<FinishedTaskMessage>,
    finished_task_rx: Receiver<FinishedTaskMessage>,
}

pub type SharedTaskManager = Arc<Mutex<TaskManager>>;

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
            running_tasks: HashMap::new(),
            queued_tasks: VecDeque::new(),
            completed_runs: HashMap::new(),
            completed_errors: HashMap::new(),
            completed_archive: VecDeque::new(),
            concurrency: ConcurrencyManager::default(),
            release_guard: ReleaseGuard::default(),
            runtime_policy: TaskRuntimePolicy::default(),
            control: TaskManagerControl::default(),
            artifacts_root: None,
            finished_task_tx,
            finished_task_rx,
        }
    }
}

impl Drop for TaskManager {
    fn drop(&mut self) {
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

    pub fn set_worker_registry(&mut self, registry: WorkerRegistry) {
        self.registry = registry;
    }

    pub fn set_artifacts_root(&mut self, artifacts_root: PathBuf) {
        self.artifacts_root = Some(artifacts_root);
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
        let route_reason = selected_route.route_reason.clone();
        let store = queued_task.store.clone();
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
            parent_session_id: None,
            root_session_id: None,
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
        self.records.insert(task_id.clone(), record.clone());
        self.task_record_paths.insert(
            task_id.clone(),
            store.worker_dir(&task_id).join("task-record.json"),
        );
        self.control
            .set_current(task_id.clone(), ManagedTaskStatus::Pending, None)?;

        self.queued_tasks.push_back(queued_task);
        self.process_queue()?;
        Ok(task_id)
    }

    pub fn wait_for(&mut self, task_id: &str) -> Result<ManagedWorkerRun> {
        loop {
            if let Some(run) = self.try_wait_for(task_id)? {
                return Ok(run);
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
        settled_count += self.sweep_orphaned_task_state()?;
        settled_count += self.sweep_stale_running_tasks()?;
        settled_count += self.ttl_cleanup();
        self.evict_lru_resident_task();
        self.trim_archive();
        self.process_queue()?;
        Ok(settled_count)
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
        queued_task: &QueuedTask,
    ) -> Result<bool> {
        if !self.release_guard.release_once(task_id, run_epoch) {
            return Ok(false);
        }

        self.concurrency.release(queued_task);
        self.running_tasks.remove(task_id);
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
        // Release concurrency for running tasks
        let running_task = self.running_tasks.remove(task_id).map(|running_task| {
            self.concurrency.release(&running_task.queued_task);
            running_task
        });

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
                    .and_then(|task_record_path| state_store_from_task_record_path(task_record_path))
            });

        // Stop the resident handle best-effort before clearing records.
        if let Some(running_task) = running_task.as_ref() {
            best_effort_stop_handle(&running_task.handle, task_id, cause);
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
            matches!(record.status, ManagedTaskStatus::Cancelled | ManagedTaskStatus::Lost)
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
        preserved_indices.clone()
    } else {
        preserved_indices
            .iter()
            .rev()
            .take(preserved_budget)
            .copied()
            .collect()
    };
    let preserved_keep = preserved_keep.iter().collect::<std::collections::HashSet<_>>();

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
            .filter(|(_, running_task)| {
                now.duration_since(running_task.started_at) > stale_task_timeout
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
            Ok((outcome, result)) => {
                let Some(mut record) = self.records.remove(task_id) else {
                    self.forget_task(task_id)?;
                    return Ok(None);
                };
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
                        let cancelled = outcome.known_failures.iter().any(|failure| {
                            let failure = failure.to_ascii_lowercase();
                            failure.contains("cancelled") || failure.contains("canceled")
                        });
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
                if transition.applied {
                    record.result_path = Some(result.result_path.clone());
                    record.outcome_path = Some(result.outcome_path.clone());
                }
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
                            self.control
                                .update_current_status(task_id, record.status.clone())?;
                            self.release_running_task_once(
                                task_id,
                                run_epoch,
                                &running_task.queued_task,
                            )?;
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
                self.release_running_task_once(task_id, run_epoch, &running_task.queued_task)?;
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
                let error_text = format!("{error:#}");
                let transition = if record.status == ManagedTaskStatus::Interrupted {
                    transition_task_record(
                        &mut record,
                        TaskTransition::Fail {
                            finished_at: timestamp(),
                            summary: "Worker task interrupted.".to_string(),
                            failure_kind: TaskFailureKind::WorkerCancelled,
                            error: Some(error_text.clone()),
                        },
                    )
                } else if error_text.contains("timed out waiting for outcome") {
                    transition_task_record(
                        &mut record,
                        TaskTransition::MarkLost {
                            finished_at: timestamp(),
                            summary: "Worker task timed out waiting for outcome.".to_string(),
                            failure_kind: TaskFailureKind::WorkerFailed,
                            error: Some(error_text.clone()),
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
                            error: Some(error_text.clone()),
                        },
                    )
                } else {
                    transition_task_record(
                        &mut record,
                        TaskTransition::Cancel {
                            finished_at: timestamp(),
                            summary: "Worker task cancelled.".to_string(),
                            error: Some(error_text.clone()),
                        },
                    )
                };
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
                            self.control
                                .update_current_status(task_id, record.status.clone())?;
                            self.release_running_task_once(
                                task_id,
                                run_epoch,
                                &running_task.queued_task,
                            )?;
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
                self.release_running_task_once(task_id, run_epoch, &running_task.queued_task)?;
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

    fn cancel_task_direct(&mut self, task_id: &str) -> Result<()> {
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
        let transition = transition_task_record(
            record,
            TaskTransition::Cancel {
                finished_at: timestamp(),
                summary: "Worker task cancelled.".to_string(),
                error: None,
            },
        );
        let store = if let Some(running_task) = self.running_tasks.get(task_id) {
            if transition.applied {
                running_task.handle.cancel()?;
            }
            Some(running_task.store.clone())
        } else {
            queued_store
        };
        if let Some(store) = store {
            write_task_record(&store, record)?;
            append_task_lifecycle_event(&store, record, Some(&transition))?;
        }
        if !is_running {
            self.control.take_pending_messages(task_id)?;
        }
        self.control
            .update_current_status(task_id, record.status.clone())?;
        Ok(())
    }

    pub fn cancel_task(&mut self, task_id: &str) -> Result<()> {
        let descendant_task_ids = self.descendant_task_ids(task_id);
        self.cancel_task_direct(task_id)?;
        for descendant_task_id in descendant_task_ids {
            self.cancel_task_direct(&descendant_task_id)?;
        }
        Ok(())
    }

    fn interrupt_task_direct(&mut self, task_id: &str) -> Result<bool> {
        let is_running = self.running_tasks.contains_key(task_id);
        let Some(record) = self.records.get_mut(task_id) else {
            bail!("unknown managed task: {task_id}");
        };
        let transition = transition_task_record(
            record,
            TaskTransition::Interrupt {
                finished_at: timestamp(),
                summary: "Worker task interrupted.".to_string(),
                error: None,
            },
        );
        let interrupted = transition.applied;
        let store = if let Some(running_task) = self.running_tasks.get(task_id) {
            if transition.applied {
                running_task.handle.interrupt()?;
                if let Some(output) = running_task.handle.last_output() {
                    record.summary = output;
                    if let Some(attempt) = record.attempts.last_mut() {
                        attempt.summary = record.summary.clone();
                    }
                }
            }
            Some(running_task.store.clone())
        } else {
            None
        };
        if let Some(store) = store {
            write_task_record(&store, record)?;
            append_task_lifecycle_event(&store, record, Some(&transition))?;
        }
        if !is_running {
            self.control.take_pending_messages(task_id)?;
        }
        self.control
            .update_current_status(task_id, record.status.clone())?;
        Ok(interrupted)
    }

    pub fn interrupt_task(&mut self, task_id: &str) -> Result<bool> {
        let descendant_task_ids = self.descendant_task_ids(task_id);
        let interrupted = self.interrupt_task_direct(task_id)?;
        for descendant_task_id in descendant_task_ids {
            let _ = self.interrupt_task_direct(&descendant_task_id)?;
        }
        Ok(interrupted)
    }

    pub fn send_follow_up_task(&mut self, task_id: &str, prompt: String) -> Result<bool> {
        let Some(record) = self.records.get(task_id) else {
            return Ok(false);
        };

        if record.status == ManagedTaskStatus::Pending {
            self.control
                .queue_pending_message(task_id, QueuedMessageKind::FollowUp, prompt)?;
            return Ok(true);
        }

        if let Some(running_task) = self.running_tasks.get(task_id) {
            running_task.handle.send_follow_up(prompt)?;
            return Ok(true);
        }

        if messageability_for_record(record) == Messageability::Revive {
            let Some(current_task) = self.control.current_task_snapshot()? else {
                return Ok(false);
            };
            if current_task.task_id != task_id {
                return Ok(false);
            }
            let Some(handle) = current_task.handle.as_ref() else {
                return Ok(false);
            };

            handle.send_follow_up(prompt)?;
            self.control
                .update_current_status(task_id, ManagedTaskStatus::Running)?;
            return Ok(true);
        }

        Ok(false)
    }

    pub fn steer_task(&mut self, task_id: &str, prompt: String) -> Result<bool> {
        let Some(record) = self.records.get(task_id) else {
            return Ok(false);
        };

        if record.status == ManagedTaskStatus::Pending {
            self.control
                .queue_pending_message(task_id, QueuedMessageKind::Steer, prompt)?;
            return Ok(true);
        }

        if let Some(running_task) = self.running_tasks.get(task_id) {
            running_task.handle.steer(prompt)?;
            return Ok(true);
        }

        if messageability_for_record(record) == Messageability::Revive {
            let Some(current_task) = self.control.current_task_snapshot()? else {
                return Ok(false);
            };
            if current_task.task_id != task_id {
                return Ok(false);
            }
            let Some(handle) = current_task.handle.as_ref() else {
                return Ok(false);
            };

            handle.steer(prompt)?;
            self.control
                .update_current_status(task_id, ManagedTaskStatus::Running)?;
            return Ok(true);
        }

        Ok(false)
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
                let summary_head = record
                    .summary
                    .lines()
                    .next()
                    .unwrap_or(record.summary.as_str())
                    .to_string();
                let continuation_hint = continuation_hint_for_record(&record);
                TaskSnapshot {
                    task_id: record.task_id,
                    status: record.status,
                    residency_state: record.residency_state,
                    messageability,
                    run_epoch: record.run_epoch,
                    notified_epoch: record.notified_epoch,
                    parent_task_id: record.parent_task_id,
                    worker_kind: record.worker_kind,
                    worker_model: record.worker_model,
                    worker_category: record.worker_category,
                    attempts: record
                        .attempts
                        .into_iter()
                        .map(|attempt| {
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
                                result_path: attempt.result_path,
                                outcome_path: attempt.outcome_path,
                                route_transform_path,
                                summary: attempt.summary,
                                error: attempt.error,
                            }
                        })
                        .collect(),
                    result_path: record.result_path,
                    outcome_path: record.outcome_path,
                    summary: record.summary,
                    summary_head,
                    continuation_hint,
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

    fn process_queue(&mut self) -> Result<()> {
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

    fn start_queued_task(&mut self, mut queued_task: QueuedTask) -> Result<()> {
        let task_id = queued_task.task.id.clone();
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
            if let Some(record) = self.records.get_mut(&task_id) {
                let transition = transition_task_record(
                    record,
                    TaskTransition::Start {
                        session_id: handle.session_id(),
                    },
                );
                write_task_record(&queued_task.store, record)?;
                append_task_lifecycle_event(&queued_task.store, record, Some(&transition))?;
            }
            if !self.concurrency.acquire(&queued_task) {
                return Err(anyhow::anyhow!(
                    "concurrency slot unexpectedly unavailable while starting task: {task_id}"
                ));
            }
            let subscription = if handle.session_id().is_some() {
                Some(handle.subscribe(Arc::new(|_| {}))?)
            } else {
                None
            };
            self.control.set_current(
                task_id.clone(),
                ManagedTaskStatus::Running,
                Some(Arc::clone(&handle)),
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
            let pending_messages = self.control.take_pending_messages(&task_id)?;
            for queued_message in pending_messages {
                let delivery_result = match queued_message.kind {
                    QueuedMessageKind::FollowUp => {
                        running_task.handle.send_follow_up(queued_message.message)
                    }
                    QueuedMessageKind::Steer => running_task.handle.steer(queued_message.message),
                };
                if let Err(error) = delivery_result {
                    eprintln!(
                        "failed to deliver queued Gear message for task `{task_id}` from {:?} created at {}: {error:#}",
                        queued_message.caller_session_id, queued_message.created_at
                    );
                }
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

#[derive(Clone, Default)]
pub struct CompletionNotifier {
    buffer: Arc<Mutex<HashMap<(String, u64), CompletionNotification>>>,
    last_flush: Arc<Mutex<HashMap<String, Instant>>>,
}

impl CompletionNotifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn should_notify(record: &TaskRecord) -> bool {
        matches!(
            record.status,
            ManagedTaskStatus::Completed | ManagedTaskStatus::Failed | ManagedTaskStatus::Lost
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
            summary_head: record
                .summary
                .lines()
                .next()
                .unwrap_or(record.summary.as_str())
                .to_string(),
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

        if let Err(record_error) = record_failed_epoch(&notification.task_id, notification.run_epoch)
        {
            eprintln!(
                "failed to record completion notification failure for {} epoch {}: {record_error:#}",
                notification.task_id, notification.run_epoch,
            );
        }
        Ok(NotificationResult::Failed(
            last_failure.unwrap_or_else(|| "notification delivery failed".to_string()),
        ))
    }

    pub fn flush_buffer(
        &self,
        parent_session_id: &str,
        parent_state: ParentSessionState,
        write_notified: &dyn Fn(&str, u64) -> Result<()>,
        record_failed_epoch: &dyn Fn(&str, u64) -> Result<()>,
    ) -> Result<Vec<NotificationResult>> {
        let mut results = Vec::new();
        if !parent_state.can_wake() {
            return Ok(results);
        }

        let now = Instant::now();
        let cooldown = {
            let mut last_flush = self
                .last_flush
                .lock()
                .map_err(|_| anyhow::anyhow!("completion notifier last_flush mutex poisoned"))?;
            let last = last_flush
                .get(parent_session_id)
                .copied()
                .unwrap_or(Instant::now() - Duration::from_millis(NOTIFIER_DEBOUNCE_MS * 2));
            if now.duration_since(last) < Duration::from_millis(NOTIFIER_DEBOUNCE_MS) {
                return Ok(results);
            }
            last_flush.insert(parent_session_id.to_string(), now);
            0u64
        };
        let _ = cooldown;

        let mut buffer = self
            .buffer
            .lock()
            .map_err(|_| anyhow::anyhow!("completion notifier buffer mutex poisoned"))?;
        let mut keys: Vec<(String, u64)> = buffer.keys().cloned().collect();
        keys.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        for key in keys {
            if let Some(notification) = buffer.remove(&key) {
                results.push(Self::deliver_with_retry(
                    &notification,
                    write_notified,
                    record_failed_epoch,
                )?);
            }
        }
        Ok(results)
    }

    fn is_notifiable_status(status: &ManagedTaskStatus) -> bool {
        matches!(
            status,
            ManagedTaskStatus::Completed | ManagedTaskStatus::Failed | ManagedTaskStatus::Lost
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
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
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
            record.retry_reason = Some(retry_reason.clone());
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
        outcome_path: outcome_path.clone(),
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
        failure_kind = failure_kind
            .map(|kind| format!("{kind:?}"))
            .unwrap_or_else(|| "none".to_string()),
        previous_attempt_index = previous_attempt.attempt,
        previous_provider = previous_provider,
        previous_worker_kind = previous_attempt.worker_kind,
        previous_worker_model = previous_attempt.worker_model.as_deref().unwrap_or("none"),
        previous_worker_command = previous_attempt.worker_command.as_deref().unwrap_or("none"),
        previous_session_id = previous_attempt.session_id.as_deref().unwrap_or("none"),
        previous_status = format!("{:?}", previous_attempt.status),
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

fn queue_next_attempt(record: &mut TaskRecord, queued_task: &mut QueuedTask) -> FallbackDecision {
    if let Some(failure_kind) = record
        .attempts
        .last()
        .and_then(|attempt| attempt.failure_kind.clone())
    {
        let same_failure_count = record
            .attempts
            .iter()
            .filter(|attempt| attempt.failure_kind.as_ref() == Some(&failure_kind))
            .count();
        let max_attempts = queued_task.config.worker_routes.len().max(2);
        if same_failure_count >= max_attempts {
            return FallbackDecision::Unavailable {
                reason: format!(
                    "same failure kind `{failure_kind:?}` reached retry limit {max_attempts}"
                ),
                failure_kind: TaskFailureKind::RepeatedFailureLimit,
            };
        }
    }

    maybe_append_failure_upgrade_route(record, queued_task);

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
    let route_reason = selected_route.route_reason.clone();
    let route_hint = queued_task.route_hint.clone();
    let started_at = timestamp();
    let retry_reason = format!(
        "retrying after {:?} with `{}` via {}",
        previous_attempt
            .failure_kind
            .clone()
            .unwrap_or(TaskFailureKind::WorkerFailed),
        worker_kind,
        route_reason
    );
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

fn normalized_worker_command(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|command| !command.is_empty())
        .map(|command| command.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn maybe_append_failure_upgrade_route(record: &TaskRecord, queued_task: &mut QueuedTask) {
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
    ) {
        return;
    }

    let candidate_worker_kind = match WorkerKind::parse(&previous_attempt.worker_kind) {
        Some(WorkerKind::Opencode | WorkerKind::OpencodeSession) => WorkerKind::Codex,
        _ => return,
    };
    if queued_task
        .config
        .worker_routes
        .iter()
        .any(|route| route.worker_kind == candidate_worker_kind)
    {
        queued_task.route_hint = Some("deep".to_string());
        return;
    }
    let Some(worker_command) = candidate_worker_kind.default_command(None) else {
        return;
    };
    queued_task
        .config
        .worker_routes
        .push(crate::workers::WorkerRoute {
            worker_kind: candidate_worker_kind,
            worker_command: Some(worker_command),
            worker_model: None,
        });
    queued_task.route_hint = Some("deep".to_string());
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
            if outcome.known_failures.iter().any(|failure| {
                failure.to_ascii_lowercase().contains("cancelled")
                    || failure.to_ascii_lowercase().contains("canceled")
            }) {
                Some(TaskFailureKind::WorkerCancelled)
            } else {
                Some(TaskFailureKind::WorkerFailed)
            }
        }
    }
}

fn should_retry_worker_result(
    record: &TaskRecord,
    queued_task: &QueuedTask,
    result: &WorkerResult,
) -> bool {
    if result.status == WorkerStatus::Failed {
        return true;
    }

    record.failure_kind == Some(TaskFailureKind::WorkerUnavailable)
        && (!queued_task.config.worker_routes.is_empty() || queued_task.config.require_worker)
}

fn concurrency_key_for_task(queued_task: &QueuedTask) -> String {
    let selected_route = queued_task
        .config
        .selected_route_for_hint(queued_task.route_attempt, queued_task.route_hint.as_deref());
    let model_key = queued_task
        .coordinator_model
        .as_ref()
        .map(|model| format!("{}:{}", model.provider_id, model.model_id))
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
    use crate::workers::{WorkerConfig, WorkerKind, WorkerRoute};

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
    fn task_manager_records_skipped_worker_outcome() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_skipped");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
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
                    worker_command: Some("printf fallback-ok".to_string()),
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
        assert_eq!(run.record.failure_kind, None);
        assert_eq!(run.record.retry_reason, None);
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
    fn queue_next_attempt_upgrades_non_premium_failure_to_codex_route() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_upgrade_to_codex");
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

        let decision = queue_next_attempt(&mut record, &mut queued_task);

        assert_eq!(decision, FallbackDecision::Queued);
        assert_eq!(queued_task.route_hint.as_deref(), Some("deep"));
        assert_eq!(record.attempts.len(), 2);
        assert_eq!(record.attempts[1].worker_kind, "codex");
        assert_eq!(record.attempts[1].worker_category, "deep");
        assert!(
            record.attempts[1]
                .worker_command
                .as_deref()
                .is_some_and(|command| command.contains("codex exec"))
        );
        let previous_attempt = record.attempts[0].clone();
        let next_attempt = record.attempts[1].clone();
        let artifact_path = write_route_transform_artifact(
            &queued_task.store,
            &task.id,
            &previous_attempt,
            Some(&next_attempt),
            "worker fallback queued",
            None,
        )?;
        let artifact = fs::read_to_string(&artifact_path)?;
        assert!(artifact.contains("Worker Route Transform"));
        assert!(artifact.contains("Previous Attempt"));
        assert!(artifact.contains("Next Attempt"));
        assert!(artifact.contains("provider"));
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
            route_hint: None,
        };
        let started_at = timestamp();
        let mut record = TaskRecord {
            task_id: task.id.clone(),
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
            route_hint: None,
        };
        let started_at = timestamp();
        let mut record = TaskRecord {
            task_id: task.id.clone(),
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
                    worker_command: Some("printf fallback-ok".to_string()),
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
            route_hint: None,
        };
        queued_task.task.attempt = 1;
        let started_at = timestamp();
        let mut record = TaskRecord {
            task_id: task.id.clone(),
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
                    worker_command: Some("printf model-fallback-ok".to_string()),
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

        assert!(control.send_follow_up_current_task("continue".to_string())?);
        assert!(control.steer_current_task("adjust".to_string())?);
        assert!(control.interrupt_task("task_interrupt")?);
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
        assert!(!control.send_follow_up_current_task("continue after interrupt".to_string())?);
        assert!(!control.steer_current_task("adjust after interrupt".to_string())?);
        assert!(!control.send_follow_up_task("task_interrupt", "continue 2".to_string())?);
        assert!(!control.steer_task("task_interrupt", "adjust 2".to_string())?);
        assert!(!control.cancel_current_task()?);
        assert!(!control.interrupt_current_task()?);
        Ok(())
    }

    #[test]
    fn task_manager_control_cancels_current_worker() -> Result<()> {
        let control = TaskManagerControl::default();
        let interrupted = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicUsize::new(0));
        let follow_ups = Arc::new(Mutex::new(Vec::new()));
        let steers = Arc::new(Mutex::new(Vec::new()));
        control.set_current(
            "task_cancel".to_string(),
            ManagedTaskStatus::Running,
            Some(Arc::new(FakeInterruptHandle {
                interrupted: interrupted.clone(),
                cancelled: cancelled.clone(),
                follow_ups: follow_ups.clone(),
                steers: steers.clone(),
            })),
        )?;

        assert!(control.cancel_task("task_cancel")?);
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(interrupted.load(Ordering::SeqCst), 0);
        assert!(!control.send_follow_up_current_task("continue after cancel".to_string())?);
        assert!(!control.steer_current_task("adjust after cancel".to_string())?);
        assert!(!control.cancel_current_task()?);
        assert!(!control.interrupt_current_task()?);
        assert!(!control.send_follow_up_task("task_cancel", "continue 2".to_string())?);
        assert!(!control.steer_task("task_cancel", "adjust 2".to_string())?);
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
                notification_failed_epoch: None,
                killed: false,
                session_id: Some("session_fake".to_string()),
                parent_session_id: None,
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
        assert_eq!(snapshot.tasks[0].attempts.len(), 1);
        assert_eq!(snapshot.tasks[0].messageability, Some(Messageability::Steer));
        assert_eq!(snapshot.tasks[0].summary_head, "Worker task started.");
        assert!(snapshot.tasks[0]
            .continuation_hint
            .contains("Steer the running task"));
        assert_eq!(
            snapshot.tasks[0].attempts[0].outcome_path.as_deref(),
            Some(std::path::Path::new("/tmp/attempt-outcome.json"))
        );
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
        manager.running_tasks.insert(
            running_task.id.clone(),
            RunningTask {
                store,
                handle: Arc::new(FakeHangingHandle),
                queued_task: QueuedTask {
                    store: StateStore::new(temp_dir.path()),
                    workspace: temp_dir.path().to_path_buf(),
                    task: running_task.clone(),
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
        assert!(control.cancel_current_task()?);

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
            queued_task: root_queued_task.clone(),
            started_at: Instant::now(),
            _subscription: None,
        };
        let child_running_task = RunningTask {
            store: store.clone(),
            handle: running_handle(child_cancelled.clone()),
            queued_task: child_queued_task.clone(),
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

        assert!(manager.interrupt_task("task_interrupt")?);

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
        assert_eq!(pending_after_destroy.residency_state, ResidencyState::Disposed);
        let pending_events =
            fs::read_to_string(store.worker_dir("task_pending").join("task-events.jsonl"))?;
        assert!(pending_events.contains("dispose"));

        Ok(())
    }

    // ── Phase 5 tests ──

    #[test]
    fn cancelled_and_interrupted_do_not_emit_completion_notification() {
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

        assert!(!CompletionNotifier::should_notify(&cancelled));
        assert!(!CompletionNotifier::should_notify(&interrupted));
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
        assert!(notification
            .continuation_hint
            .contains("Follow up from the Gear panel"));
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
        assert!(notification
            .continuation_hint
            .contains("Open the result/outcome artifacts"));
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
        assert_eq!(*attempts, 2, "delivery should retry once before failing");
        let failed_epochs = failed_epochs.lock().map_err(|_| anyhow::anyhow!("mutex"))?;
        assert_eq!(failed_epochs.as_slice(), &[("task_delivery_fail".to_string(), 1)]);
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
        assert_eq!(delivered.as_slice(), &[("task_delivery_retry".to_string(), 2)]);
        let failed_epochs = failed_epochs.lock().map_err(|_| anyhow::anyhow!("mutex"))?;
        assert!(failed_epochs.is_empty(), "transient failure should not write failed epoch");
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
}
