use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub max_worker_calls: usize,
    pub max_premium_worker_calls: usize,
    pub max_repair_attempts_per_error: usize,
    #[serde(default = "default_max_provider_unknown_streak")]
    pub max_provider_unknown_streak: usize,
    #[serde(default = "default_max_child_depth")]
    pub max_child_depth: usize,
    #[serde(default = "default_max_runtime_minutes")]
    pub max_runtime_minutes: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_worker_calls: 8,
            max_premium_worker_calls: 2,
            max_repair_attempts_per_error: 2,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
        }
    }
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    GoalCreated,
    SpecCreated,
    PlanCreated,
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
            self.events_dir(),
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

    pub fn events_dir(&self) -> PathBuf {
        self.root.join("events")
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
