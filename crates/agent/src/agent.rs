mod db;
mod legacy_thread;
mod native_agent_server;
pub mod outline;
mod pattern_extraction;
mod sandboxing;
mod templates;
#[cfg(test)]
mod tests;
mod thread;
mod thread_store;
mod tool_permissions;
mod tools;

use context_server::ContextServerId;
pub use db::*;
pub use gearbox_agent::gui::{
    GearRuntimeBudgetSummary, GearRuntimeEventClass, GearRuntimeEventEnvelope, GearRuntimeHealth,
    GearRuntimeLifecycle, GearRuntimeSnapshot,
};
pub use gearbox_agent::task_manager::ManagedTaskStatus as GearManagedTaskStatus;
pub use gearbox_agent::task_manager::{
    Messageability as GearTaskMessageability, TaskAttemptSnapshot as GearTaskAttemptSnapshot,
    TaskAttemptStatus as GearTaskAttemptStatus, TaskManagerSnapshot as GearTaskManagerSnapshot,
    TaskSnapshot as GearTaskSnapshot,
};

const GEAR_RESUME_CONTINUATION_MARKER: &str = "__gear_resume_continuation__";
// Broker contract types — re-exported for external backends to use
pub use gearbox_agent::worker_broker::{
    BrokerCapability, BrokerLifecycleReceipt, BrokerOutcome, BrokerPermissionEvidence,
    BrokerPermissionType, BrokerPhaseRequest, BrokerSessionIdentity, BrokerUsage,
    ModelAvailability, PhaseBrokerFactory, UnavailableReason, WorkerBroker,
};
use itertools::Itertools;
pub use native_agent_server::NativeAgentServer;
pub use pattern_extraction::*;
pub use sandboxing::{
    ThreadSandbox, sandbox_worktree_writable_paths, settings_sandbox_policy,
    settings_thread_sandbox,
};
pub use shell_command_parser::extract_commands;
pub use templates::*;
pub use thread::*;
pub use thread_store::*;
pub use tool_permissions::*;
pub use tools::*;

use acp_thread::{
    AcpThread, AcpThreadEvent, AgentModelId, AgentModelSelector, AgentSessionInfo,
    AgentSessionList, AgentSessionListRequest, AgentSessionListResponse, ClientUserMessageId,
    TokenUsageRatio,
};
use agent_client_protocol::schema::v1 as acp;
use agent_skills::{
    AGENTS_DIR_NAME, MAX_SKILL_DESCRIPTIONS_SIZE, MAX_SKILL_FILE_SIZE, ProjectSkillGroup,
    SKILL_FILE_NAME, Skill, SkillIndex, SkillLoadError, SkillLoadWarning, SkillScopeId,
    SkillSource, SkillSummary, builtin_skills, global_skills_dir, load_skills_from_directory,
    parse_skill_frontmatter, project_skills_relative_path, read_skill_body_from_content,
};
use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use collections::{HashMap, HashSet, IndexMap};

use fs::Fs;
use futures::channel::{mpsc, oneshot};
use futures::future::Shared;
use futures::{FutureExt as _, StreamExt as _, future};
use gearbox_agent::gui::{
    GEAR_GUI_EVENT_BUFFER_CAPACITY, GEAR_GUI_REVIEW_QUEUE_CAPACITY,
    GEAR_GUI_WORKER_DISPATCH_CAPACITY,
};
use gearbox_agent::phase_routing::{
    CodexAcpModelProfiles, LiveModelInventory, ModelSelectorId, OpenCodeModelProfiles,
    PhaseBackend, PhaseRouteCandidate, PhaseRouteSource, PhaseRouteTable,
};
use gearbox_agent::plan_graph::PhaseProfile;
use gearbox_agent::plan_review::{
    PhaseExecutionBackend, PhaseExecutionIdentity, PlanCriticVerdict,
};
use gearbox_agent::runtime::{
    CoordinatorReview, CoordinatorReviewHook, CoordinatorReviewInput, DEFAULT_MAX_ITERATIONS,
    DEFAULT_MAX_PLAN_REVISIONS, DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK, DEFAULT_MAX_RUNTIME_MINUTES,
    IntentFoldHook, IntentFoldInput, IntentFoldSubmission, Orchestrator, PhaseRuntime,
    PlanCriticHook, PlanCriticInput, PlanCriticSubmission, PlanRevisionHook, PlanRevisionInput,
    PlanRevisionSubmission, PlannerHook, PlannerInput, PlannerSubmission, RunOptions,
    StrategistNextGoalHook, StrategistNextGoalInput, StrategistNextGoalSubmission,
    objective_policy_from_env,
};
use gearbox_agent::state::{
    Budget, ContinuationStatus, CoordinatorModel, EventKind, StateStore, event, id_timestamp,
};
use gearbox_agent::task_manager::{
    ActionOutcome, ManagedTaskStatus, OutcomeContext, SendOutcome, SharedTaskManager, SteerOutcome,
    TaskAttemptSnapshot, TaskCommandContext, TaskManager, TaskManagerControl, TaskManagerSnapshot,
    TaskManagerTickLoop, TaskSnapshot,
};
use gearbox_agent::tools::CancellationToken;
use gearbox_agent::workers::{
    Intensity, NativeWorkerBackend, VerificationContract, WorkerCategory, WorkerConfig,
    WorkerEvent, WorkerEventHub, WorkerKind, WorkerOutcome, WorkerPacket, WorkerRegistry,
    WorkerResult, WorkerRoute, WorkerSessionHandle, WorkerStartRequest, WorkerStatus,
    category_resolution_for_route, discover_workspace_rules, discover_workspace_skills,
    sanitize_model_fields,
    worker_outcome_from_result, worker_prompt, write_result_and_outcome,
};
use gpui::{
    App, AppContext, AsyncApp, Context, Entity, EntityId, SharedString, Subscription, Task,
    TaskExt, WeakEntity,
};
use language_model::{
    CompletionIntent, IconOrSvg, LanguageModel, LanguageModelId, LanguageModelProvider,
    LanguageModelProviderId, LanguageModelRegistry, LanguageModelRequest,
    LanguageModelRequestMessage, Role,
};
use project::{
    AgentId, Project, ProjectItem, ProjectPath, Worktree, WorktreeId,
    trusted_worktrees::TrustedWorktrees,
};
use prompt_store::{ProjectContext, RULES_FILE_NAMES, RulesFileContext, WorktreeContext};
use serde::{Deserialize, Serialize};
use settings::{LanguageModelSelection, Settings as _, update_settings_file};
use std::any::Any;
use std::collections::VecDeque;
use std::fs as std_fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::Instant;
use util::ResultExt;
use util::path_list::PathList;
use util::rel_path::RelPath;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProjectSnapshot {
    pub worktree_snapshots: Vec<project::telemetry_snapshot::TelemetryWorktreeSnapshot>,
    pub timestamp: DateTime<Utc>,
}

pub struct RulesLoadingError {
    pub message: SharedString,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SkillLoadingIssueKind {
    LoadFailed,
    DescriptionTooLong,
    CatalogBudgetExceeded,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SkillLoadingIssue {
    pub project_id: EntityId,
    pub path: PathBuf,
    pub message: SharedString,
    pub kind: SkillLoadingIssueKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SkillLoadingIssueData {
    path: PathBuf,
    message: String,
    kind: SkillLoadingIssueKind,
}

impl SkillLoadingIssueData {
    fn from_load_error(error: SkillLoadError) -> Self {
        Self {
            path: error.path,
            message: error.message,
            kind: SkillLoadingIssueKind::LoadFailed,
        }
    }

    fn from_load_warning(skill: &Skill, warning: &SkillLoadWarning) -> Self {
        let kind = match warning {
            SkillLoadWarning::DescriptionTooLong { .. } => {
                SkillLoadingIssueKind::DescriptionTooLong
            }
        };
        Self {
            path: skill.skill_file_path.clone(),
            message: warning.message(),
            kind,
        }
    }

    fn catalog_budget_exceeded(path: PathBuf, message: String) -> Self {
        Self {
            path,
            message,
            kind: SkillLoadingIssueKind::CatalogBudgetExceeded,
        }
    }
}

/// Emitted whenever the set of skill loading issues for a project changes.
/// The `issues` field is the full replacement list; subscribers should treat
/// it as a snapshot rather than appending. An empty `issues` list means all
/// previously-reported issues have been resolved.
#[derive(Clone, Debug)]
pub struct SkillLoadingIssuesUpdated {
    pub project_id: EntityId,
    pub issues: Vec<SkillLoadingIssue>,
}

#[derive(Clone, Debug)]
pub struct NativeAvailableSkill {
    pub name: String,
    pub description: String,
    pub source: SharedString,
    pub skill_file_path: PathBuf,
    pub warning: Option<SharedString>,
}

impl From<&Skill> for NativeAvailableSkill {
    fn from(skill: &Skill) -> Self {
        Self {
            name: skill.name.clone(),
            description: skill.description.clone(),
            source: skill.source.display_label().to_string().into(),
            skill_file_path: skill.skill_file_path.clone(),
            warning: skill
                .load_warnings
                .first()
                .map(|warning| warning.message().into()),
        }
    }
}

pub const COMPACT_COMMAND_NAME: &str = "compact";

/// Returns the set of MCP prompt names that must be server-qualified
/// (`/<server>.<name>`) to stay unambiguous in the slash-command popup: names
/// shared by more than one MCP prompt, or names colliding with a reserved
/// built-in command (e.g. `/compact`). A built-in always wins an unqualified
/// invocation, so colliding MCP prompts are only reachable when prefixed.
fn ambiguous_mcp_prompt_names<'a>(
    reserved: impl IntoIterator<Item = &'a str>,
    prompt_names: impl IntoIterator<Item = &'a str>,
) -> HashSet<&'a str> {
    let mut counts: HashMap<&str, usize> = HashMap::default();
    for name in reserved.into_iter().chain(prompt_names) {
        *counts.entry(name).or_insert(0) += 1;
    }
    counts
        .into_iter()
        .filter_map(|(name, count)| (count > 1).then_some(name))
        .collect()
}

struct ProjectState {
    project: Entity<Project>,
    project_context: Entity<ProjectContext>,
    skills: Arc<Vec<Skill>>,
    skill_loading_issues: Vec<SkillLoadingIssue>,
    project_context_needs_refresh: watch::Sender<()>,
    _maintain_project_context: Task<Result<()>>,
    context_server_registry: Entity<ContextServerRegistry>,
    _subscriptions: Vec<Subscription>,
}

/// Holds both the internal Thread and the AcpThread for a session
struct Session {
    /// The internal thread that processes messages
    thread: Entity<Thread>,
    /// The ACP thread that handles protocol communication
    acp_thread: Entity<acp_thread::AcpThread>,
    work_dirs: Option<PathList>,
    gear_cancellation_token: Option<CancellationToken>,
    gear_task_manager_control: Option<TaskManagerControl>,
    gear_task_manager: Option<SharedTaskManager>,
    gear_task_manager_tick_loop: Option<TaskManagerTickLoop>,
    gear_runtime_snapshot: Option<GearRuntimeSnapshot>,
    gear_runtime_snapshot_error: Option<String>,
    gear_runtime_snapshot_task: Option<Task<()>>,
    #[cfg(test)]
    gear_lifecycle_events: Option<Arc<Mutex<Vec<String>>>>,
    project_id: EntityId,
    pending_save: Task<Result<()>>,
    _subscriptions: Vec<Subscription>,
    ref_count: usize,
}

struct PendingSession {
    task: Shared<Task<Result<Entity<AcpThread>, Arc<anyhow::Error>>>>,
    ref_count: usize,
}

pub struct LanguageModels {
    /// Access language model by ID
    models: HashMap<AgentModelId, Arc<dyn LanguageModel>>,
    /// Cached list for returning language model information
    model_list: acp_thread::AgentModelList,
    refresh_models_rx: watch::Receiver<()>,
    refresh_models_tx: watch::Sender<()>,
    _authenticate_all_providers_task: Task<()>,
}

impl LanguageModels {
    fn new(cx: &mut App) -> Self {
        let (refresh_models_tx, refresh_models_rx) = watch::channel(());

        let mut this = Self {
            models: HashMap::default(),
            model_list: acp_thread::AgentModelList::Grouped(IndexMap::default()),
            refresh_models_rx,
            refresh_models_tx,
            _authenticate_all_providers_task: Self::authenticate_all_language_model_providers(cx),
        };
        this.refresh_list(cx);
        this
    }

    fn refresh_list(&mut self, cx: &App) {
        let providers = LanguageModelRegistry::global(cx)
            .read(cx)
            .visible_providers()
            .into_iter()
            .filter(|provider| provider.is_authenticated(cx))
            .collect::<Vec<_>>();

        let mut language_model_list = IndexMap::default();
        let mut recommended_models = HashSet::default();

        let mut recommended = Vec::new();
        for provider in &providers {
            for model in provider.recommended_models(cx) {
                recommended_models.insert((model.provider_id(), model.id()));
                recommended.push(Self::map_language_model_to_info(&model, provider));
            }
        }
        if !recommended.is_empty() {
            language_model_list.insert(
                acp_thread::AgentModelGroupName("Recommended".into()),
                recommended,
            );
        }

        let mut models = HashMap::default();
        for provider in providers {
            let mut provider_models = Vec::new();
            for model in provider.provided_models(cx) {
                let model_info = Self::map_language_model_to_info(&model, &provider);
                let model_id = model_info.id.clone();
                provider_models.push(model_info);
                models.insert(model_id, model);
            }
            if !provider_models.is_empty() {
                language_model_list.insert(
                    acp_thread::AgentModelGroupName(provider.name().0.clone()),
                    provider_models,
                );
            }
        }

        self.models = models;
        self.model_list = acp_thread::AgentModelList::Grouped(language_model_list);
        self.refresh_models_tx.send(()).ok();
    }

    fn watch(&self) -> watch::Receiver<()> {
        self.refresh_models_rx.clone()
    }

    pub fn notify_model_selection_changed(&mut self) {
        self.refresh_models_tx.send(()).ok();
    }

    pub fn model_from_id(&self, model_id: &AgentModelId) -> Option<Arc<dyn LanguageModel>> {
        self.models.get(model_id).cloned()
    }

    fn map_language_model_to_info(
        model: &Arc<dyn LanguageModel>,
        provider: &Arc<dyn LanguageModelProvider>,
    ) -> acp_thread::AgentModelInfo {
        acp_thread::AgentModelInfo {
            id: Self::model_id(model),
            name: model.name().0,
            description: None,
            icon: Some(match provider.icon() {
                IconOrSvg::Svg(path) => acp_thread::AgentModelIcon::Path(path),
                IconOrSvg::Icon(name) => acp_thread::AgentModelIcon::Named(name),
            }),
            is_latest: model.is_latest(),
            cost: model.model_cost_info().map(|cost| cost.to_shared_string()),
            disabled: model.is_disabled(),
        }
    }

    fn model_id(model: &Arc<dyn LanguageModel>) -> AgentModelId {
        AgentModelId::new(format!("{}/{}", model.provider_id().0, model.id().0))
    }

    fn authenticate_all_language_model_providers(cx: &mut App) -> Task<()> {
        let authenticate_all_providers = LanguageModelRegistry::global(cx)
            .read(cx)
            .visible_providers()
            .iter()
            .map(|provider| (provider.id(), provider.name(), provider.authenticate(cx)))
            .collect::<Vec<_>>();

        cx.spawn(async move |cx| {
            for (provider_id, provider_name, authenticate_task) in authenticate_all_providers {
                if let Err(err) = authenticate_task.await {
                    match err {
                        language_model::AuthenticateError::CredentialsNotFound => {
                            // Since we're authenticating these providers in the
                            // background for the purposes of populating the
                            // language selector, we don't care about providers
                            // where the credentials are not found.
                        }
                        language_model::AuthenticateError::ConnectionRefused => {
                            // Not logging connection refused errors as they are mostly from LM Studio's noisy auth failures.
                            // LM Studio only has one auth method (endpoint call) which fails for users who haven't enabled it.
                            // TODO: Better manage LM Studio auth logic to avoid these noisy failures.
                        }
                        _ => {
                            // Some providers have noisy failure states that we
                            // don't want to spam the logs with every time the
                            // language model selector is initialized.
                            //
                            // Ideally these should have more clear failure modes
                            // that we know are safe to ignore here, like what we do
                            // with `CredentialsNotFound` above.
                            match provider_id.0.as_ref() {
                                "lmstudio" | "ollama" => {
                                    // LM Studio and Ollama both make fetch requests to the local APIs to determine if they are "authenticated".
                                    //
                                    // These fail noisily, so we don't log them.
                                }
                                "copilot_chat" => {
                                    // Copilot Chat returns an error if Copilot is not enabled, so we don't log those errors.
                                }
                                _ => {
                                    log::error!(
                                        "Failed to authenticate provider: {}: {err:#}",
                                        provider_name.0
                                    );
                                }
                            }
                        }
                    }
                }
            }

            cx.update(|cx| {
                LanguageModelRegistry::global(cx)
                    .update(cx, |registry, cx| registry.refresh_fallback_model(cx))
            });
        })
    }
}

/// Implemented by the UI layer to provide the ability for agent tools to create
/// sibling threads that appear in the agent panel.
///
/// `agent_ui::AgentPanel` installs an implementation of this trait on the
/// `NativeAgent` when it sets up a connection. Tools in a native-agent thread
/// then discover and use the host via `NativeThreadEnvironment`. The UI side
/// is responsible for keeping the installed host current; a host whose
/// backing UI has been torn down will fail its first request with a clear
/// error rather than being detected up front.
pub trait SiblingThreadHost {
    fn create_sibling_thread(
        &self,
        request: SiblingThreadRequest,
        cx: &mut AsyncApp,
    ) -> Task<Result<SiblingThreadInfo>>;

    fn list_available_agents(&self, cx: &mut App) -> Result<AvailableAgents>;
}

pub struct NativeAgent {
    /// Session ID -> Session mapping
    sessions: HashMap<acp::SessionId, Session>,
    pending_sessions: HashMap<acp::SessionId, PendingSession>,
    thread_store: Entity<ThreadStore>,
    /// Project-specific state keyed by project EntityId
    projects: HashMap<EntityId, ProjectState>,
    /// Shared templates for all threads
    templates: Arc<Templates>,
    /// Cached model information
    models: LanguageModels,
    /// Handler installed by the UI for `create_thread` / `list_agents_and_models` tools.
    sibling_thread_host: Option<Rc<dyn SiblingThreadHost>>,
    fs: Arc<dyn Fs>,
    _subscriptions: Vec<Subscription>,
    /// Tracks the lifecycle of global skills directory observation. We
    /// don't eagerly watch (or even check for) `~/.agents/skills/` at
    /// startup; users who never engage with the agent panel pay zero
    /// filesystem cost. The watch is kicked off lazily by
    /// [`Self::ensure_skills_scan_started`], which is called from the
    /// three agent-panel interaction points: input box focus, slash
    /// autocomplete, and conversation submit.
    skills_state: SkillsState,
    #[cfg(test)]
    gear_worker_config_override: Option<WorkerConfig>,
}

#[derive(Default)]
enum SkillsState {
    /// No scan or watch is active. A user-interaction trigger will kick
    /// off a fresh scan.
    #[default]
    Idle,
    /// A one-shot scan task is in flight. It checks whether
    /// `~/.agents/skills/` exists; if so, transitions to `Watching`,
    /// otherwise back to `Idle`.
    Scanning,
    /// A watch task is observing `~/.agents/skills/`. It transitions
    /// back to `Idle` if the watched directory itself is removed.
    Watching,
}

impl gpui::EventEmitter<SkillLoadingIssuesUpdated> for NativeAgent {}

static RULES_FILE_REL_PATHS: LazyLock<Vec<Arc<RelPath>>> = LazyLock::new(|| {
    RULES_FILE_NAMES
        .iter()
        .filter_map(|name| RelPath::unix(name).ok().map(|path| path.into_arc()))
        .collect()
});

static AGENTS_PREFIX: LazyLock<Option<Arc<RelPath>>> = LazyLock::new(|| {
    RelPath::unix(AGENTS_DIR_NAME)
        .ok()
        .map(|path| path.into_arc())
});

static SKILLS_PREFIX: LazyLock<Option<Arc<RelPath>>> = LazyLock::new(|| {
    RelPath::unix(project_skills_relative_path())
        .ok()
        .map(|path| path.into_arc())
});

struct ProjectSkillFile {
    relative_path: Arc<RelPath>,
    display_path: PathBuf,
    size: u64,
}

async fn expand_worktree_directory(
    worktree: &Entity<Worktree>,
    path: &RelPath,
    cx: &mut AsyncApp,
) -> Result<()> {
    let expand_task = worktree.update(cx, |worktree, cx| {
        let entry_id = worktree
            .entry_for_path(path)
            .filter(|entry| entry.is_dir())
            .map(|entry| entry.id);
        entry_id.and_then(|entry_id| worktree.expand_entry(entry_id, cx))
    });

    if let Some(expand_task) = expand_task {
        expand_task.await?;
    }

    Ok(())
}

async fn expand_project_skills_directories(
    worktree: &Entity<Worktree>,
    cx: &mut AsyncApp,
) -> Result<()> {
    let agents_dir = RelPath::unix(AGENTS_DIR_NAME)?;
    let Some(skills_prefix) = SKILLS_PREFIX.as_ref() else {
        return Ok(());
    };

    expand_worktree_directory(worktree, agents_dir, cx).await?;
    expand_worktree_directory(worktree, skills_prefix, cx).await?;

    let skill_dirs = worktree.update(cx, |worktree, _cx| {
        worktree
            .child_entries(skills_prefix)
            .filter(|entry| entry.is_dir())
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>()
    });
    for skill_dir in skill_dirs {
        expand_worktree_directory(worktree, &skill_dir, cx).await?;
    }

    Ok(())
}

fn project_skill_files_from_worktree(worktree: &Worktree) -> Vec<ProjectSkillFile> {
    let Some(skills_prefix) = SKILLS_PREFIX.as_ref() else {
        return Vec::new();
    };
    let Ok(skill_file_name) = RelPath::unix(SKILL_FILE_NAME) else {
        return Vec::new();
    };

    let mut skill_files = Vec::new();
    for skill_dir in worktree.child_entries(skills_prefix) {
        if !skill_dir.is_dir() {
            continue;
        }

        let relative_path = skill_dir.path.join(skill_file_name);
        let Some(skill_file) = worktree.entry_for_path(&relative_path) else {
            continue;
        };
        if !skill_file.is_file() {
            continue;
        }

        skill_files.push(ProjectSkillFile {
            display_path: worktree.absolutize(&relative_path),
            relative_path,
            size: skill_file.size,
        });
    }

    skill_files.sort_by(|a, b| {
        a.relative_path
            .as_unix_str()
            .cmp(b.relative_path.as_unix_str())
    });
    skill_files
}

impl NativeAgent {
    pub fn new(
        thread_store: Entity<ThreadStore>,
        templates: Arc<Templates>,
        fs: Arc<dyn Fs>,
        cx: &mut App,
    ) -> Entity<NativeAgent> {
        log::debug!("Creating new NativeAgent");

        cx.new(|cx| {
            let subscriptions = vec![
                cx.subscribe(
                    &LanguageModelRegistry::global(cx),
                    Self::handle_models_updated_event,
                ),
                // Flush thread content on quit so an in-flight async save
                // can't leave a thread orphaned ("no thread found with ID").
                cx.on_app_quit(Self::flush_threads_on_quit),
            ];

            if !cx.has_global::<SkillIndex>() {
                cx.set_global(SkillIndex::default());
            }

            Self {
                sessions: HashMap::default(),
                pending_sessions: HashMap::default(),
                thread_store,
                projects: HashMap::default(),
                templates,
                models: LanguageModels::new(cx),
                sibling_thread_host: None,
                fs,
                _subscriptions: subscriptions,
                skills_state: SkillsState::default(),
                #[cfg(test)]
                gear_worker_config_override: None,
            }
        })
    }

    /// Kicks off a one-time scan of the global skills directory if one
    /// isn't already in progress and a watch isn't already active.
    ///
    /// Idempotent and cheap: returns immediately if a scan or watch is
    /// already running. The expected callers are user-interaction events
    /// from the agent panel (input focus, slash autocomplete, conversation
    /// submit); firing this from any of them is equivalent and safe to
    /// repeat.
    ///
    /// The scan itself runs detached on the foreground executor. If
    /// `~/.agents/skills/` exists it transitions state to
    /// [`SkillsState::Watching`] and starts a recursive watch;
    /// otherwise it transitions back to [`SkillsState::Idle`] so the
    /// next trigger retries (covering the case where the user creates
    /// the directory after the first scan).
    pub fn ensure_skills_scan_started(&mut self, cx: &mut Context<Self>) {
        if !matches!(self.skills_state, SkillsState::Idle) {
            return;
        }
        self.skills_state = SkillsState::Scanning;
        let fs = self.fs.clone();
        cx.spawn(async move |this, cx| Self::run_skills_scan(this, fs, cx).await)
            .detach();
    }

    async fn run_skills_scan(this: WeakEntity<Self>, fs: Arc<dyn Fs>, cx: &mut AsyncApp) {
        let skills_dir = global_skills_dir();
        if !fs.is_dir(&skills_dir).await {
            // Skills directory doesn't exist; revert state so the next
            // user trigger retries.
            let _ = this.update(cx, |this, _cx| {
                this.skills_state = SkillsState::Idle;
            });
            return;
        }

        // Skills directory exists. Start a watch and trigger a refresh
        // of every project's context so the freshly-discovered skills
        // get loaded.
        let _ = this.update(cx, |this, cx| {
            cx.spawn({
                let fs = fs.clone();
                let skills_dir = skills_dir.clone();
                async move |this, cx| Self::run_skills_watch(this, fs, skills_dir, cx).await
            })
            .detach();
            this.skills_state = SkillsState::Watching;
            for state in this.projects.values_mut() {
                state.project_context_needs_refresh.send(()).ok();
            }
        });
    }

    async fn run_skills_watch(
        this: WeakEntity<Self>,
        fs: Arc<dyn Fs>,
        skills_dir: PathBuf,
        cx: &mut AsyncApp,
    ) {
        let (mut events, watcher) = fs
            .watch(&skills_dir, std::time::Duration::from_millis(500))
            .await;

        // Linux's inotify backend is non-recursive, so a watch on
        // `skills_dir` only fires for direct children. Skill discovery
        // is intentionally one level deep (`<skills_dir>/<skill>/SKILL.md`),
        // so we only register watches on each immediate child directory
        // and deliberately do NOT recurse: a stray `node_modules`,
        // `target`, or `.git` inside a skill folder would otherwise
        // register watches for tens of thousands of subdirectories.
        // These per-child adds are cheap no-ops on macOS/Windows where
        // the OS-level watch is already recursive.
        if let Ok(mut entries) = fs.read_dir(&skills_dir).await {
            while let Some(entry) = entries.next().await {
                let Ok(path) = entry else { continue };
                if let Ok(Some(metadata)) = fs.metadata(&path).await
                    && metadata.is_dir
                {
                    watcher.add(&path).ok();
                }
            }
        }

        while let Some(events) = events.next().await {
            // When a new immediate child directory of `skills_dir` is
            // created, add a single watch for it so changes to its
            // `SKILL.md` are observed on Linux. We intentionally do not
            // recurse into the new directory — skill discovery is only
            // one level deep.
            for event in &events {
                if event.kind == Some(fs::PathEventKind::Created)
                    && event.path.parent() == Some(skills_dir.as_path())
                    && fs.is_dir(&event.path).await
                {
                    watcher.add(&event.path).ok();
                }
            }

            let watched_root_removed = events.iter().any(|event| {
                event.path == skills_dir && event.kind == Some(fs::PathEventKind::Removed)
            });

            let updated = this.update(cx, |this, _cx| {
                for state in this.projects.values_mut() {
                    state.project_context_needs_refresh.send(()).ok();
                }
                if watched_root_removed {
                    // Drop back to Idle so the next user trigger
                    // retries the scan; the next trigger will rediscover
                    // the directory if the user has recreated it.
                    this.skills_state = SkillsState::Idle;
                }
            });
            if updated.is_err() || watched_root_removed {
                return;
            }
        }
    }

    pub fn set_sibling_thread_host(&mut self, host: Rc<dyn SiblingThreadHost>) {
        self.sibling_thread_host = Some(host);
    }

    pub fn sibling_thread_host(&self) -> Option<Rc<dyn SiblingThreadHost>> {
        self.sibling_thread_host.clone()
    }

    fn new_session(
        &mut self,
        project: Entity<Project>,
        work_dirs: PathList,
        agent_id: AgentId,
        telemetry_id: SharedString,
        cx: &mut Context<Self>,
    ) -> Entity<AcpThread> {
        let project_id = self.get_or_create_project_state(&project, cx);
        let project_state = &self.projects[&project_id];

        let registry = LanguageModelRegistry::read_global(cx);
        let available_count = registry.available_models(cx).count();
        log::debug!("Total available models: {}", available_count);

        let default_model = registry.default_model().and_then(|default_model| {
            self.models
                .model_from_id(&LanguageModels::model_id(&default_model.model))
        });
        let thread = cx.new(|cx| {
            Thread::new(
                project,
                project_state.project_context.clone(),
                project_state.context_server_registry.clone(),
                self.templates.clone(),
                default_model,
                cx,
            )
        });

        self.register_session(
            thread,
            project_id,
            1,
            Some(work_dirs),
            agent_id,
            telemetry_id,
            cx,
        )
    }

    fn register_session(
        &mut self,
        thread_handle: Entity<Thread>,
        project_id: EntityId,
        ref_count: usize,
        work_dirs: Option<PathList>,
        agent_id: AgentId,
        telemetry_id: SharedString,
        cx: &mut Context<Self>,
    ) -> Entity<AcpThread> {
        let is_gear_session = agent_id == *GEAR_AGENT_ID;
        let connection = Rc::new(NativeAgentConnection::with_identity(
            cx.entity(),
            agent_id,
            telemetry_id,
        ));

        let session_id = thread_handle.read(cx).id().clone();
        let thread = thread_handle.read(cx);
        let parent_session_id = thread.parent_thread_id();
        let title = thread.title();
        let draft_prompt = thread.draft_prompt().map(Vec::from);
        let scroll_position = thread.ui_scroll_position();
        let token_usage = thread.latest_token_usage();
        let project = thread.project.clone();
        let action_log = thread.action_log.clone();
        let prompt_capabilities_rx = thread.prompt_capabilities_rx.clone();
        let acp_thread = cx.new(|cx| {
            let mut acp_thread = acp_thread::AcpThread::new(
                parent_session_id,
                title,
                work_dirs.clone(),
                connection,
                project.clone(),
                action_log.clone(),
                session_id.clone(),
                prompt_capabilities_rx,
                cx,
            );
            acp_thread.set_draft_prompt(draft_prompt, cx);
            acp_thread.set_ui_scroll_position(scroll_position);
            acp_thread.update_token_usage(token_usage, cx);
            acp_thread
        });

        let registry = LanguageModelRegistry::read_global(cx);
        let summarization_model = registry.thread_summary_model(cx).map(|c| c.model);

        let weak = cx.weak_entity();
        let weak_thread = thread_handle.downgrade();
        thread_handle.update(cx, |thread, cx| {
            thread.set_summarization_model(summarization_model, cx);
            thread.add_default_tools(
                Rc::new(NativeThreadEnvironment {
                    acp_thread: acp_thread.downgrade(),
                    thread: weak_thread,
                    agent: weak.clone(),
                }) as _,
                cx,
            );
            // The resolver closure reads `state.skills` at invocation
            // time, so skills added or removed by the SKILL.md watcher
            // after the thread is constructed are still visible to the
            // model — without this, the catalog and tool would drift out
            // of sync until the session was reopened.
            thread.add_tool(SkillTool::with_body_resolver(
                skills_resolver_for_project(weak.clone(), project_id),
                skill_body_resolver_for_project(project.clone(), self.fs.clone()),
            ));
        });

        let subscriptions = vec![
            cx.subscribe(&thread_handle, Self::handle_thread_title_updated),
            cx.subscribe(&thread_handle, Self::handle_thread_token_usage_updated),
            cx.observe(&thread_handle, move |this, thread, cx| {
                this.save_thread(thread, cx)
            }),
        ];

        self.sessions.insert(
            session_id.clone(),
            Session {
                thread: thread_handle,
                acp_thread: acp_thread.clone(),
                work_dirs,
                gear_cancellation_token: None,
                gear_task_manager_control: None,
                gear_task_manager: None,
                gear_task_manager_tick_loop: None,
                gear_runtime_snapshot: None,
                gear_runtime_snapshot_error: None,
                gear_runtime_snapshot_task: None,
                #[cfg(test)]
                gear_lifecycle_events: None,
                project_id,
                _subscriptions: subscriptions,
                pending_save: Task::ready(Ok(())),
                ref_count,
            },
        );

        if is_gear_session
            && let Some(session) = self.sessions.get(&session_id)
            && let Ok(workspace) = gear_workspace_for_session(session, self, cx)
        {
            self.ensure_gear_runtime_snapshot_task(session_id, workspace, None, cx);
        }

        self.update_available_commands_for_project(project_id, cx);

        acp_thread
    }

    fn ensure_gear_runtime_snapshot_task(
        &mut self,
        session_id: acp::SessionId,
        workspace: PathBuf,
        task_manager: Option<SharedTaskManager>,
        cx: &mut Context<Self>,
    ) {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return;
        };
        if session.gear_runtime_snapshot_task.is_some() {
            return;
        }

        let snapshot_agent = cx.entity().downgrade();
        let snapshot_workspace = workspace;
        let snapshot_session_id = session_id.clone();
        let runtime_snapshot_task = cx.spawn(async move |_this, cx| {
            loop {
                let workspace = snapshot_workspace.clone();
                let session_id = snapshot_session_id.clone();
                let task_manager = task_manager.clone();
                let snapshot = cx
                    .background_spawn(async move {
                        let task_snapshot = task_manager
                            .as_ref()
                            .map(|task_manager| {
                                task_manager
                                    .lock()
                                    .map_err(|_| anyhow!("gear task manager mutex poisoned"))?
                                    .snapshot()
                            })
                            .transpose()?;
                        gearbox_agent::gui::GearRuntimeSnapshot::from_store(
                            &StateStore::new(&workspace),
                            workspace.display().to_string(),
                            session_id.to_string(),
                            task_snapshot,
                        )
                    })
                    .await;
                let update_result = snapshot_agent.update(cx, |agent, cx| {
                    if let Some(session) = agent.sessions.get_mut(&snapshot_session_id) {
                        match snapshot {
                            Ok(snapshot) => {
                                session.gear_runtime_snapshot = Some(snapshot);
                                session.gear_runtime_snapshot_error = None;
                            }
                            Err(error) => {
                                session.gear_runtime_snapshot_error =
                                    Some(gear_truncate_text(&format!("{error:#}"), 1200));
                            }
                        }
                        cx.notify();
                    }
                });
                if update_result.is_err() {
                    break;
                }
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(200))
                    .await;
            }
        });
        session.gear_runtime_snapshot_task = Some(runtime_snapshot_task);
    }

    pub fn models(&self) -> &LanguageModels {
        &self.models
    }

    fn get_or_create_project_state(
        &mut self,
        project: &Entity<Project>,
        cx: &mut Context<Self>,
    ) -> EntityId {
        let project_id = project.entity_id();
        if self.projects.contains_key(&project_id) {
            return project_id;
        }

        let project_context = cx.new(|_| ProjectContext::new(vec![]));
        self.register_project_with_initial_context(project.clone(), project_context, cx);
        if let Some(state) = self.projects.get_mut(&project_id) {
            state.project_context_needs_refresh.send(()).ok();
        }
        project_id
    }

    fn register_project_with_initial_context(
        &mut self,
        project: Entity<Project>,
        project_context: Entity<ProjectContext>,
        cx: &mut Context<Self>,
    ) {
        let project_id = project.entity_id();

        let context_server_store = project.read(cx).context_server_store();
        let context_server_registry =
            cx.new(|cx| ContextServerRegistry::new(context_server_store.clone(), cx));

        let mut subscriptions = vec![
            cx.subscribe(&project, Self::handle_project_event),
            cx.subscribe(
                &context_server_store,
                Self::handle_context_server_store_updated,
            ),
            cx.subscribe(
                &context_server_registry,
                Self::handle_context_server_registry_event,
            ),
        ];
        // When the user trusts a worktree (or revokes trust), project-local
        // skills become eligible (or ineligible) for loading. Trigger a
        // refresh so the catalog and slash-command list update without a
        // restart. This is unconditional — a `Trusted` event for any
        // worktree under any project is cheap to handle and keeps the
        // logic straightforward.
        if let Some(trusted_worktrees) = TrustedWorktrees::try_get_global(cx) {
            subscriptions.push(
                cx.subscribe(&trusted_worktrees, move |this, _, _event, _cx| {
                    if let Some(state) = this.projects.get_mut(&project_id) {
                        state.project_context_needs_refresh.send(()).ok();
                    }
                }),
            );
        }

        let (project_context_needs_refresh_tx, project_context_needs_refresh_rx) =
            watch::channel(());

        self.projects.insert(
            project_id,
            ProjectState {
                project,
                project_context,
                skills: Arc::new(Vec::new()),
                skill_loading_issues: Vec::new(),
                project_context_needs_refresh: project_context_needs_refresh_tx,
                _maintain_project_context: cx.spawn(async move |this, cx| {
                    Self::maintain_project_context(
                        this,
                        project_id,
                        project_context_needs_refresh_rx,
                        cx,
                    )
                    .await
                }),
                context_server_registry,
                _subscriptions: subscriptions,
            },
        );
    }

    fn session_project_state(&self, session_id: &acp::SessionId) -> Option<&ProjectState> {
        self.sessions
            .get(session_id)
            .and_then(|session| self.projects.get(&session.project_id))
    }

    async fn maintain_project_context(
        this: WeakEntity<Self>,
        project_id: EntityId,
        mut needs_refresh: watch::Receiver<()>,
        cx: &mut AsyncApp,
    ) -> Result<()> {
        while needs_refresh.changed().await.is_ok() {
            let task = this.update(cx, |this, cx| {
                let state = this
                    .projects
                    .get(&project_id)
                    .context("project state not found")?;
                anyhow::Ok(Self::build_project_context(
                    &state.project,
                    this.fs.clone(),
                    cx,
                ))
            })??;
            let (project_context, skills, skill_issue_data) = task.await;
            let skills = Arc::new(skills);
            let skill_loading_issues: Vec<SkillLoadingIssue> = skill_issue_data
                .into_iter()
                .map(|issue| SkillLoadingIssue {
                    project_id,
                    path: issue.path,
                    message: issue.message.into(),
                    kind: issue.kind,
                })
                .collect();
            this.update(cx, |this, cx| {
                // Only emit SkillLoadingIssuesUpdated when the issue list
                // actually changed. Refreshes happen frequently (prompt-store
                // updates, rules-file edits, worktree events, trust-state
                // changes), and re-emitting an unchanged list causes the UI
                // to redisplay issues the user has already dismissed.
                // Transitions from non-empty to empty still count as a change,
                // so subscribers continue to receive an empty list to clear
                // previously-displayed issues when they get resolved.
                let issues_changed = this
                    .projects
                    .get(&project_id)
                    .map(|state| state.skill_loading_issues != skill_loading_issues)
                    .unwrap_or(true);

                if let Some(state) = this.projects.get_mut(&project_id) {
                    state.skills = skills;
                    state.skill_loading_issues = skill_loading_issues.clone();
                    // Only push the new `ProjectContext` through if it
                    // differs from the current one. The system prompt is
                    // re-rendered from this on every turn, so an unchanged
                    // `ProjectContext` means a byte-identical system prompt
                    // and a continued hit on the model API's prompt cache.
                    // Refreshes fire on many events that don't actually
                    // change what the model sees (e.g. a SKILL.md body edit
                    // that leaves the catalog — name, description, location
                    // — untouched), so this check matters in practice.
                    state
                        .project_context
                        .update(cx, |current_project_context, cx| {
                            if *current_project_context != project_context {
                                *current_project_context = project_context;
                                cx.notify();
                            }
                        });
                }
                if issues_changed {
                    cx.emit(SkillLoadingIssuesUpdated {
                        project_id,
                        issues: skill_loading_issues,
                    });
                }
                // Skills appear in the slash-command list, so a change in
                // the loaded skills needs to be pushed out to active sessions.
                // This runs unconditionally because MCP prompts (also part of
                // the available commands) can change without affecting the
                // skill error list.
                this.update_available_commands_for_project(project_id, cx);
                this.publish_skill_index(cx);
            })?;
        }

        Ok(())
    }

    fn build_project_context(
        project: &Entity<Project>,
        fs: Arc<dyn Fs>,
        cx: &mut App,
    ) -> Task<(ProjectContext, Vec<Skill>, Vec<SkillLoadingIssueData>)> {
        let worktrees = project.read(cx).visible_worktrees(cx).collect::<Vec<_>>();
        let worktree_tasks = worktrees
            .iter()
            .map(|worktree| {
                Self::load_worktree_info_for_system_prompt(worktree.clone(), project.clone(), cx)
            })
            .collect::<Vec<_>>();

        // Load global skills
        let global_skills_task = {
            let global_skills_dir = global_skills_dir();
            let global_skills_fs = fs.clone();
            cx.background_spawn(async move {
                load_skills_from_directory(
                    &global_skills_fs,
                    &global_skills_dir,
                    SkillSource::Global,
                )
                .await
            })
        };

        // Load project-local skills, but only from worktrees the user has
        // trusted. Skills in `.agents/skills/` ship with the project; a
        // freshly cloned untrusted repo can carry hostile descriptions or
        // bodies, so we keep them out of the catalog and the slash-command
        // list until trust is granted. The subscription in
        // `register_project_with_initial_context` triggers a context
        // refresh when a worktree's trust state changes, so newly trusted
        // worktrees pick up their skills without restarting.
        let trusted_worktrees = TrustedWorktrees::try_get_global(cx);
        let worktree_store = project.read(cx).worktree_store();
        let project_skills_task = {
            let project = project.clone();
            let trusted_worktrees = worktrees
                .iter()
                .filter_map(|worktree| {
                    let worktree_id = worktree.read(cx).id();
                    let is_trusted = trusted_worktrees.as_ref().is_none_or(|trusted_worktrees| {
                        trusted_worktrees.update(cx, |trusted_worktrees, cx| {
                            trusted_worktrees.can_trust(&worktree_store, worktree_id, cx)
                        })
                    });
                    if !is_trusted {
                        return None;
                    }

                    let worktree_snapshot = worktree.read(cx);
                    let worktree_root_name: Arc<str> = worktree_snapshot.root_name_str().into();
                    let scan_complete = worktree_snapshot
                        .as_local()
                        .map(|local| local.scan_complete());
                    Some((
                        worktree.clone(),
                        worktree_id,
                        worktree_root_name,
                        scan_complete,
                    ))
                })
                .collect::<Vec<_>>();

            cx.spawn(async move |cx| {
                let mut project_skills_results = Vec::new();
                for (worktree, worktree_id, worktree_root_name, scan_complete) in trusted_worktrees
                {
                    if let Some(scan_complete) = scan_complete {
                        scan_complete.await;
                    }
                    if let Err(error) = expand_project_skills_directories(&worktree, cx).await {
                        project_skills_results.push(vec![Err(SkillLoadError {
                            path: PathBuf::from(project_skills_relative_path()),
                            message: format!("Failed to scan project skills: {}", error),
                        })]);
                        continue;
                    }

                    let skill_files = worktree.update(cx, |worktree, _cx| {
                        project_skill_files_from_worktree(worktree)
                    });
                    let source = SkillSource::ProjectLocal {
                        worktree_id: SkillScopeId(worktree_id.to_usize()),
                        worktree_root_name,
                    };

                    let mut worktree_results = Vec::new();
                    for skill_file in skill_files {
                        if skill_file.size > MAX_SKILL_FILE_SIZE as u64 {
                            worktree_results.push(Err(SkillLoadError {
                                path: skill_file.display_path.clone(),
                                message: format!(
                                    "SKILL.md file exceeds maximum size of {}KB",
                                    MAX_SKILL_FILE_SIZE / 1024
                                ),
                            }));
                            continue;
                        }

                        let buffer = match project
                            .update(cx, |project, cx| {
                                project.open_buffer(
                                    (worktree_id, skill_file.relative_path.clone()),
                                    cx,
                                )
                            })
                            .await
                        {
                            Ok(buffer) => buffer,
                            Err(error) => {
                                worktree_results.push(Err(SkillLoadError {
                                    path: skill_file.display_path.clone(),
                                    message: format!("Failed to read file: {}", error),
                                }));
                                continue;
                            }
                        };

                        let content = cx
                            .update(|cx| buffer.read(cx).as_text_snapshot().as_rope().to_string());

                        worktree_results.push(
                            parse_skill_frontmatter(
                                &skill_file.display_path,
                                &content,
                                source.clone(),
                            )
                            .map_err(|error| SkillLoadError {
                                path: skill_file.display_path,
                                message: error.to_string(),
                            }),
                        );
                    }
                    project_skills_results.push(worktree_results);
                }
                project_skills_results
            })
        };
        cx.spawn(async move |_cx| {
            let worktrees = future::join_all(worktree_tasks).await;

            let worktrees = worktrees
                .into_iter()
                .map(|(worktree, _rules_error)| {
                    // TODO: show error message
                    // if let Some(rules_error) = rules_error {
                    //     this.update(cx, |_, cx| cx.emit(rules_error)).ok();
                    // }
                    worktree
                })
                .collect::<Vec<_>>();

            // Load and combine skills. `combine_skills` deliberately
            // does NOT deduplicate — the autocomplete popup needs to
            // see every entry so users can disambiguate same-named
            // global vs. project-local skills via the source label.
            // Project-overrides-global is applied below, only for the
            // model-facing catalog.
            let global_skills = global_skills_task.await;
            let project_skills_results = project_skills_task.await;
            let (skills, skill_errors) =
                combine_skills(global_skills, project_skills_results.into_iter().flatten());
            let mut skill_issues = skill_errors
                .into_iter()
                .map(SkillLoadingIssueData::from_load_error)
                .collect::<Vec<_>>();
            for skill in &skills {
                skill_issues.extend(
                    skill
                        .load_warnings
                        .iter()
                        .map(|warning| SkillLoadingIssueData::from_load_warning(skill, warning)),
                );
            }

            // Apply project-overrides-global before catalog selection
            // so the model sees at most one entry per name. The full
            // `skills` list is still stored on `ProjectState` and used
            // by the autocomplete popup.
            let overridden = apply_skill_overrides(&skills);

            // Enforce the catalog size budget here so that skills which
            // don't fit produce an issue in the UI rather than being
            // silently swallowed by ProjectContext.
            let (catalog_skills, budget_issues) = select_catalog_skills(&overridden);
            skill_issues.extend(budget_issues);

            let project_context = ProjectContext::new(worktrees).with_skills(catalog_skills);
            (project_context, skills, skill_issues)
        })
    }

    fn load_worktree_info_for_system_prompt(
        worktree: Entity<Worktree>,
        project: Entity<Project>,
        cx: &mut App,
    ) -> Task<(WorktreeContext, Option<RulesLoadingError>)> {
        let tree = worktree.read(cx);
        let root_name = tree.root_name_str().into();
        let abs_path = tree.abs_path();
        let scan_complete = tree.as_local().map(|local| local.scan_complete());

        let mut context = WorktreeContext {
            root_name,
            abs_path,
            rules_file: None,
        };

        cx.spawn(async move |cx| {
            if let Some(scan_complete) = scan_complete {
                scan_complete.await;
            }

            let rules_task = cx.update(|cx| Self::load_worktree_rules_file(worktree, project, cx));

            let (rules_file, rules_file_error) = match rules_task {
                Some(rules_task) => match rules_task.await {
                    Ok(rules_file) => (Some(rules_file), None),
                    Err(err) => (
                        None,
                        Some(RulesLoadingError {
                            message: format!("{err}").into(),
                        }),
                    ),
                },
                None => (None, None),
            };
            context.rules_file = rules_file;
            (context, rules_file_error)
        })
    }

    fn load_worktree_rules_file(
        worktree: Entity<Worktree>,
        project: Entity<Project>,
        cx: &mut App,
    ) -> Option<Task<Result<RulesFileContext>>> {
        let worktree = worktree.read(cx);
        let worktree_id = worktree.id();
        let selected_rules_file = RULES_FILE_REL_PATHS
            .iter()
            .filter_map(|name| {
                worktree
                    .entry_for_path(name)
                    .filter(|entry| entry.is_file())
                    .map(|entry| entry.path.clone())
            })
            .next();

        // Note that Cline supports `.clinerules` being a directory, but that is not currently
        // supported. This doesn't seem to occur often in GitHub repositories.
        selected_rules_file.map(|path_in_worktree| {
            let project_path = ProjectPath {
                worktree_id,
                path: path_in_worktree.clone(),
            };
            let buffer_task =
                project.update(cx, |project, cx| project.open_buffer(project_path, cx));
            let rope_task = cx.spawn(async move |cx| {
                let buffer = buffer_task.await?;
                let (project_entry_id, rope) = buffer.read_with(cx, |buffer, cx| {
                    let project_entry_id = buffer.entry_id(cx).context("buffer has no file")?;
                    anyhow::Ok((project_entry_id, buffer.as_rope().clone()))
                })?;
                anyhow::Ok((project_entry_id, rope))
            });
            // Build a string from the rope on a background thread.
            cx.background_spawn(async move {
                let (project_entry_id, rope) = rope_task.await?;
                anyhow::Ok(RulesFileContext {
                    path_in_worktree,
                    text: rope.to_string().trim().to_string(),
                    project_entry_id: project_entry_id.to_usize(),
                })
            })
        })
    }

    fn handle_thread_title_updated(
        &mut self,
        thread: Entity<Thread>,
        _: &TitleUpdated,
        cx: &mut Context<Self>,
    ) {
        let session_id = thread.read(cx).id();
        let Some(session) = self.sessions.get(session_id) else {
            return;
        };

        let thread = thread.downgrade();
        let acp_thread = session.acp_thread.downgrade();
        cx.spawn(async move |_, cx| {
            let title = thread.read_with(cx, |thread, _| thread.title())?;
            if let Some(title) = title {
                let task =
                    acp_thread.update(cx, |acp_thread, cx| acp_thread.set_title(title, cx))?;
                task.await?;
            }
            anyhow::Ok(())
        })
        .detach_and_log_err(cx);
    }

    fn handle_thread_token_usage_updated(
        &mut self,
        thread: Entity<Thread>,
        usage: &TokenUsageUpdated,
        cx: &mut Context<Self>,
    ) {
        let Some(session) = self.sessions.get(thread.read(cx).id()) else {
            return;
        };
        session.acp_thread.update(cx, |acp_thread, cx| {
            acp_thread.update_token_usage(usage.0.clone(), cx);
        });
    }

    fn handle_project_event(
        &mut self,
        project: Entity<Project>,
        event: &project::Event,
        _cx: &mut Context<Self>,
    ) {
        let project_id = project.entity_id();
        let Some(state) = self.projects.get_mut(&project_id) else {
            return;
        };
        match event {
            project::Event::WorktreeAdded(_) | project::Event::WorktreeRemoved(_) => {
                state.project_context_needs_refresh.send(()).ok();
            }
            project::Event::WorktreeUpdatedEntries(_, items) => {
                if items.iter().any(|(path, _, _)| {
                    let path_ref = path.as_ref();
                    RULES_FILE_REL_PATHS
                        .iter()
                        .any(|rules_path| path_ref == rules_path.as_ref())
                        || AGENTS_PREFIX
                            .as_ref()
                            .is_some_and(|prefix| path_ref.starts_with(prefix))
                }) {
                    state.project_context_needs_refresh.send(()).ok();
                }
            }
            _ => {}
        }
    }

    fn handle_models_updated_event(
        &mut self,
        _registry: Entity<LanguageModelRegistry>,
        event: &language_model::Event,
        cx: &mut Context<Self>,
    ) {
        self.models.refresh_list(cx);

        let registry = LanguageModelRegistry::read_global(cx);
        let default_model = registry.default_model().map(|m| m.model);
        let summarization_model = registry.thread_summary_model(cx).map(|m| m.model);

        for session in self.sessions.values_mut() {
            session.thread.update(cx, |thread, cx| {
                thread.ensure_model(default_model.as_ref(), cx);

                if let Some(model) = summarization_model.clone() {
                    if thread.summarization_model().is_none()
                        || matches!(event, language_model::Event::ThreadSummaryModelChanged)
                    {
                        thread.set_summarization_model(Some(model), cx);
                    }
                }
            });
        }
    }

    fn handle_context_server_store_updated(
        &mut self,
        store: Entity<project::context_server_store::ContextServerStore>,
        _event: &project::context_server_store::ServerStatusChangedEvent,
        cx: &mut Context<Self>,
    ) {
        let project_id = self.projects.iter().find_map(|(id, state)| {
            if *state.context_server_registry.read(cx).server_store() == store {
                Some(*id)
            } else {
                None
            }
        });
        if let Some(project_id) = project_id {
            self.update_available_commands_for_project(project_id, cx);
        }
    }

    fn handle_context_server_registry_event(
        &mut self,
        registry: Entity<ContextServerRegistry>,
        event: &ContextServerRegistryEvent,
        cx: &mut Context<Self>,
    ) {
        match event {
            ContextServerRegistryEvent::ToolsChanged => {}
            ContextServerRegistryEvent::PromptsChanged => {
                let project_id = self.projects.iter().find_map(|(id, state)| {
                    if state.context_server_registry == registry {
                        Some(*id)
                    } else {
                        None
                    }
                });
                if let Some(project_id) = project_id {
                    self.update_available_commands_for_project(project_id, cx);
                }
            }
        }
    }

    fn publish_skill_index(&self, cx: &mut Context<Self>) {
        let mut global_skills = Vec::new();
        let mut project_groups: Vec<ProjectSkillGroup> = Vec::new();
        let mut seen_global = false;

        for state in self.projects.values() {
            for skill in state.skills.iter() {
                match &skill.source {
                    SkillSource::BuiltIn => {}
                    SkillSource::Global => {
                        if !seen_global {
                            global_skills.push(skill.clone());
                        }
                    }
                    SkillSource::ProjectLocal {
                        worktree_id,
                        worktree_root_name,
                    } => {
                        if let Some(group) = project_groups
                            .iter_mut()
                            .find(|g| g.worktree_id == *worktree_id)
                        {
                            group.skills.push(skill.clone());
                        } else {
                            project_groups.push(ProjectSkillGroup {
                                worktree_id: *worktree_id,
                                worktree_root_name: SharedString::from(worktree_root_name.clone()),
                                skills: vec![skill.clone()],
                            });
                        }
                    }
                }
            }
            if !global_skills.is_empty() {
                seen_global = true;
            }
        }

        cx.set_global(SkillIndex {
            global_skills,
            project_skills: project_groups,
        });
    }

    fn update_available_commands_for_project(&self, project_id: EntityId, cx: &mut Context<Self>) {
        let available_commands =
            Self::build_available_commands_for_project(self.projects.get(&project_id), cx);
        for session in self.sessions.values() {
            if session.project_id != project_id {
                continue;
            }
            session.acp_thread.update(cx, |thread, cx| {
                thread
                    .handle_session_update(
                        acp::SessionUpdate::AvailableCommandsUpdate(
                            acp::AvailableCommandsUpdate::new(available_commands.clone()),
                        ),
                        cx,
                    )
                    .log_err();
            });
        }
    }

    fn build_available_commands_for_project(
        project_state: Option<&ProjectState>,
        cx: &App,
    ) -> Vec<acp::AvailableCommand> {
        let Some(state) = project_state else {
            return Vec::new();
        };
        let compact_command = acp::AvailableCommand::new(
            COMPACT_COMMAND_NAME,
            "Summarize the conversation so far to free up context",
        )
        .meta(acp_thread::meta_with_command_category(
            acp_thread::CommandCategory::Native,
        ));

        let registry = state.context_server_registry.read(cx);

        // Reserve the built-in command name so a same-named MCP prompt is
        // force-prefixed (`/<server>.compact`) and stays reachable: an
        // unqualified `/compact` always routes to the native command.
        let ambiguous_prompt_names = ambiguous_mcp_prompt_names(
            [COMPACT_COMMAND_NAME],
            registry.prompts().map(|p| p.prompt.name.as_str()),
        );

        let mcp_commands = registry.prompts().flat_map(|context_server_prompt| {
            let prompt = &context_server_prompt.prompt;

            let should_prefix = ambiguous_prompt_names.contains(prompt.name.as_str());

            let name = if should_prefix {
                format!("{}.{}", context_server_prompt.server_id, prompt.name)
            } else {
                prompt.name.clone()
            };

            let mut command =
                acp::AvailableCommand::new(name, prompt.description.clone().unwrap_or_default())
                    .meta(acp_thread::meta_with_command_category(
                        acp_thread::CommandCategory::Mcp,
                    ));

            match prompt.arguments.as_deref() {
                Some([arg]) => {
                    let hint = format!("<{}>", arg.name);

                    command = command.input(acp::AvailableCommandInput::Unstructured(
                        acp::UnstructuredCommandInput::new(hint),
                    ));
                }
                Some([]) | None => {}
                Some(_) => {
                    // skip >1 argument commands since we don't support them yet
                    return None;
                }
            }

            Some(command)
        });

        std::iter::once(compact_command)
            .chain(mcp_commands)
            .collect()
    }

    pub fn load_thread(
        &mut self,
        id: acp::SessionId,
        project: Entity<Project>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<Thread>>> {
        let database_future = ThreadsDatabase::connect(cx);
        cx.spawn(async move |this, cx| {
            let database = database_future.await.map_err(|err| anyhow!(err))?;
            let db_thread = database
                .load_thread(id.clone())
                .await?
                .with_context(|| format!("no thread found with ID: {id:?}"))?;

            this.update(cx, |this, cx| {
                let project_id = this.get_or_create_project_state(&project, cx);
                let project_state = this
                    .projects
                    .get(&project_id)
                    .context("project state not found")?;
                let summarization_model = LanguageModelRegistry::read_global(cx)
                    .thread_summary_model(cx)
                    .map(|c| c.model);

                Ok(cx.new(|cx| {
                    let mut thread = Thread::from_db(
                        id.clone(),
                        db_thread,
                        project_state.project.clone(),
                        project_state.project_context.clone(),
                        project_state.context_server_registry.clone(),
                        this.templates.clone(),
                        cx,
                    );
                    thread.set_summarization_model(summarization_model, cx);
                    thread
                }))
            })?
        })
    }

    pub fn open_thread(
        &mut self,
        id: acp::SessionId,
        project: Entity<Project>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<AcpThread>>> {
        self.open_thread_with_identity(id, project, None, ZED_AGENT_ID.clone(), "zed".into(), cx)
    }

    pub fn open_thread_with_identity(
        &mut self,
        id: acp::SessionId,
        project: Entity<Project>,
        work_dirs: Option<PathList>,
        agent_id: AgentId,
        telemetry_id: SharedString,
        cx: &mut Context<Self>,
    ) -> Task<Result<Entity<AcpThread>>> {
        if let Some(session) = self.sessions.get_mut(&id) {
            session.ref_count += 1;
            return Task::ready(Ok(session.acp_thread.clone()));
        }

        if let Some(pending) = self.pending_sessions.get_mut(&id) {
            pending.ref_count += 1;
            let task = pending.task.clone();
            return cx.background_spawn(async move { task.await.map_err(|err| anyhow!(err)) });
        }

        let task = self.load_thread(id.clone(), project.clone(), cx);
        let shared_task = cx
            .spawn({
                let id = id.clone();
                async move |this, cx| {
                    let thread = match task.await {
                        Ok(thread) => thread,
                        Err(err) => {
                            this.update(cx, |this, _cx| {
                                this.pending_sessions.remove(&id);
                            })
                            .ok();
                            return Err(Arc::new(err));
                        }
                    };
                    let acp_thread = this
                        .update(cx, |this, cx| {
                            let project_id = this.get_or_create_project_state(&project, cx);
                            let ref_count = this
                                .pending_sessions
                                .remove(&id)
                                .map_or(1, |pending| pending.ref_count);
                            this.register_session(
                                thread.clone(),
                                project_id,
                                ref_count,
                                work_dirs,
                                agent_id,
                                telemetry_id,
                                cx,
                            )
                        })
                        .map_err(Arc::new)?;
                    let events = thread.update(cx, |thread, cx| thread.replay(cx));
                    cx.update(|cx| {
                        NativeAgentConnection::handle_thread_events(
                            events,
                            acp_thread.downgrade(),
                            None,
                            cx,
                        )
                    })
                    .await
                    .map_err(Arc::new)?;
                    acp_thread.update(cx, |thread, cx| {
                        thread.snapshot_completed_plan(cx);
                    });
                    Ok(acp_thread)
                }
            })
            .shared();
        self.pending_sessions.insert(
            id,
            PendingSession {
                task: shared_task.clone(),
                ref_count: 1,
            },
        );

        cx.background_spawn(async move { shared_task.await.map_err(|err| anyhow!(err)) })
    }

    pub fn thread_summary(
        &mut self,
        id: acp::SessionId,
        project: Entity<Project>,
        cx: &mut Context<Self>,
    ) -> Task<Result<SharedString>> {
        let thread = self.open_thread(id.clone(), project, cx);
        cx.spawn(async move |this, cx| {
            let acp_thread = thread.await?;
            let result = this
                .update(cx, |this, cx| {
                    this.sessions
                        .get(&id)
                        .unwrap()
                        .thread
                        .update(cx, |thread, cx| thread.summary(cx))
                })?
                .await
                .context("Failed to generate summary")?;

            this.update(cx, |this, cx| this.close_session(&id, cx))?
                .await?;
            drop(acp_thread);
            Ok(result)
        })
    }

    fn close_session(
        &mut self,
        session_id: &acp::SessionId,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return Task::ready(Ok(()));
        };

        session.ref_count -= 1;
        if session.ref_count > 0 {
            return Task::ready(Ok(()));
        }

        let thread = session.thread.clone();
        self.save_thread(thread, cx);
        let Some(session) = self.sessions.remove(session_id) else {
            return Task::ready(Ok(()));
        };
        let project_id = session.project_id;

        let has_remaining = self.sessions.values().any(|s| s.project_id == project_id);
        if !has_remaining {
            self.projects.remove(&project_id);
            self.publish_skill_index(cx);
        }

        session.pending_save
    }

    fn save_thread(&mut self, thread: Entity<Thread>, cx: &mut Context<Self>) {
        let id = thread.read(cx).id().clone();
        let Some(session) = self.sessions.get(&id) else {
            return;
        };
        let Some((id, folder_paths, db_thread)) = self.thread_save_payload(session, cx) else {
            return;
        };

        let database_future = ThreadsDatabase::connect(cx);
        let thread_store = self.thread_store.clone();
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        session.pending_save = cx.spawn(async move |_, cx| {
            let Some(database) = database_future.await.map_err(|err| anyhow!(err)).log_err() else {
                return Ok(());
            };
            let db_thread = db_thread.await;
            database
                .save_thread(id, db_thread, folder_paths)
                .await
                .log_err();
            thread_store.update(cx, |store, cx| store.reload(cx));
            Ok(())
        });
    }

    /// Builds everything needed to persist a session's thread content,
    /// capturing the current draft prompt from the ACP thread. Returns `None`
    /// if the thread is empty or its project state is gone.
    fn thread_save_payload(
        &self,
        session: &Session,
        cx: &mut App,
    ) -> Option<(acp::SessionId, PathList, Task<DbThread>)> {
        if session.thread.read(cx).is_empty() {
            return None;
        }
        let state = self.projects.get(&session.project_id)?;
        let folder_paths = PathList::new(
            &state
                .project
                .read(cx)
                .visible_worktrees(cx)
                .map(|worktree| worktree.read(cx).abs_path().to_path_buf())
                .collect::<Vec<_>>(),
        );
        let draft_prompt = session.acp_thread.read(cx).draft_prompt().map(Vec::from);
        let id = session.thread.read(cx).id().clone();
        let db_thread = session.thread.update(cx, |thread, cx| {
            thread.set_draft_prompt(draft_prompt);
            thread.to_db(cx)
        });
        Some((id, folder_paths, db_thread))
    }

    /// Commits every non-empty thread's content on shutdown so the async
    /// `save_thread` losing the race can't leave metadata without content.
    fn flush_threads_on_quit(
        &mut self,
        cx: &mut Context<Self>,
    ) -> impl Future<Output = ()> + use<> {
        let database_future = ThreadsDatabase::connect(cx);

        let mut saves = Vec::new();
        for session in self.sessions.values() {
            saves.extend(self.thread_save_payload(session, cx));
        }

        async move {
            let Some(database) = database_future.await.map_err(|err| anyhow!(err)).log_err() else {
                return;
            };
            // All quit observers share `gpui::SHUTDOWN_TIMEOUT`, so run the
            // saves concurrently instead of one at a time.
            future::join_all(saves.into_iter().map(|(id, folder_paths, db_thread)| {
                let database = database.clone();
                async move {
                    let db_thread = db_thread.await;
                    database
                        .save_thread(id, db_thread, folder_paths)
                        .await
                        .log_err();
                }
            }))
            .await;
        }
    }

    fn send_mcp_prompt(
        &self,
        client_user_message_id: ClientUserMessageId,
        session_id: acp::SessionId,
        prompt_name: String,
        server_id: ContextServerId,
        arguments: HashMap<String, String>,
        original_content: Vec<acp::ContentBlock>,
        cx: &mut Context<Self>,
    ) -> Task<Result<acp::PromptResponse>> {
        let Some(state) = self.session_project_state(&session_id) else {
            return Task::ready(Err(anyhow!("Project state not found for session")));
        };
        let server_store = state
            .context_server_registry
            .read(cx)
            .server_store()
            .clone();
        let path_style = state.project.read(cx).path_style(cx);

        cx.spawn(async move |this, cx| {
            let prompt =
                crate::get_prompt(&server_store, &server_id, &prompt_name, arguments, cx).await?;

            let (acp_thread, thread) = this.update(cx, |this, _cx| {
                let session = this
                    .sessions
                    .get(&session_id)
                    .context("Failed to get session")?;
                anyhow::Ok((session.acp_thread.clone(), session.thread.clone()))
            })??;

            let mut last_is_user = true;

            thread.update(cx, |thread, cx| {
                thread.push_acp_user_block(
                    client_user_message_id,
                    original_content.into_iter().skip(1),
                    path_style,
                    cx,
                );
            });

            for message in prompt.messages {
                let context_server::types::PromptMessage { role, content } = message;
                let block = mcp_message_content_to_acp_content_block(content);

                match role {
                    context_server::types::Role::User => {
                        let id = acp_thread::ClientUserMessageId::new();

                        acp_thread.update(cx, |acp_thread, cx| {
                            acp_thread.push_user_content_block_with_indent(
                                Some(id.clone()),
                                block.clone(),
                                true,
                                cx,
                            );
                        });

                        thread.update(cx, |thread, cx| {
                            thread.push_acp_user_block(id, [block], path_style, cx);
                        });
                    }
                    context_server::types::Role::Assistant => {
                        acp_thread.update(cx, |acp_thread, cx| {
                            acp_thread.push_assistant_content_block_with_indent(
                                block.clone(),
                                false,
                                true,
                                cx,
                            );
                        });

                        thread.update(cx, |thread, cx| {
                            thread.push_acp_agent_block(block, cx);
                        });
                    }
                }

                last_is_user = role == context_server::types::Role::User;
            }

            let response_stream = thread.update(cx, |thread, cx| {
                if last_is_user {
                    thread.send_existing(cx)
                } else {
                    // Resume if MCP prompt did not end with a user message
                    thread.resume(cx)
                }
            })?;

            let connection = this.upgrade().map(NativeAgentConnection::new);
            cx.update(|cx| {
                NativeAgentConnection::handle_thread_events(
                    response_stream,
                    acp_thread.downgrade(),
                    connection,
                    cx,
                )
            })
            .await
        })
    }

    /// Run a summary-based context compaction in response to the built-in
    /// `/compact` slash command.
    fn send_compact_command(
        &self,
        client_user_message_id: ClientUserMessageId,
        session_id: acp::SessionId,
        cx: &mut Context<Self>,
    ) -> Task<Result<acp::PromptResponse>> {
        cx.spawn(async move |this, cx| {
            let (acp_thread, thread) = this.update(cx, |this, _cx| {
                let session = this
                    .sessions
                    .get(&session_id)
                    .context("Failed to get session")?;
                anyhow::Ok((session.acp_thread.clone(), session.thread.clone()))
            })??;

            let response_stream =
                thread.update(cx, |thread, cx| thread.compact(client_user_message_id, cx))?;
            acp_thread.update(cx, |acp_thread, cx| {
                acp_thread.update_token_usage(None, cx);
            });

            let connection = this.upgrade().map(NativeAgentConnection::new);
            cx.update(|cx| {
                NativeAgentConnection::handle_thread_events(
                    response_stream,
                    acp_thread.downgrade(),
                    connection,
                    cx,
                )
            })
            .await
        })
    }

    /// Activate a skill in response to a `/skill-name` slash command. The
    /// skill body is wrapped in the same `<skill_content>` envelope the
    /// model-driven `skill` tool uses, so the conversation looks the same
    /// regardless of who initiated the load. Any text the user typed after
    /// the command on the same line — plus any additional content blocks
    /// they attached (file mentions, etc.) — is appended to the same user
    /// message after the skill envelope, so the model sees the skill
    /// instructions followed by the user's request.
    fn send_skill_invocation(
        &self,
        client_user_message_id: ClientUserMessageId,
        session_id: acp::SessionId,
        skill: Skill,
        original_content: Vec<acp::ContentBlock>,
        cx: &mut Context<Self>,
    ) -> Task<Result<acp::PromptResponse>> {
        let Some(state) = self.session_project_state(&session_id) else {
            return Task::ready(Err(anyhow!("Project state not found for session")));
        };
        let path_style = state.project.read(cx).path_style(cx);
        let read_skill_body =
            skill_body_resolver_for_project(state.project.clone(), self.fs.clone());

        cx.spawn(async move |this, cx| {
            let (acp_thread, thread) = this.update(cx, |this, _cx| {
                let session = this
                    .sessions
                    .get(&session_id)
                    .context("Failed to get session")?;
                anyhow::Ok((session.acp_thread.clone(), session.thread.clone()))
            })??;

            // Build the model-context message: skill envelope first, then
            // anything the user wrote after the slash command. The first
            // text block has its leading `/cmd` stripped so the literal
            // command name isn't echoed into the model's context, but any
            // text the user typed after it on the same line is preserved
            // verbatim and appended after the envelope.
            //
            // Read the body on demand here — bodies live on disk between
            // materializations to keep memory cost O(total frontmatter)
            // rather than O(total file size).
            let body = if let Some(embedded) = skill.embedded_body {
                embedded.to_string()
            } else {
                read_skill_body(skill.clone(), cx).await.with_context(|| {
                    format!(
                        "Failed to read skill body from {}",
                        skill.skill_file_path.display()
                    )
                })?
            };
            let envelope = crate::tools::render_skill_envelope(&skill, &body);
            let envelope_block = acp::ContentBlock::Text(acp::TextContent::new(envelope));

            let mut user_blocks = original_content;
            if let Some(acp::ContentBlock::Text(text_content)) = user_blocks.first_mut() {
                let stripped = strip_slash_command_prefix(&text_content.text);
                if stripped.trim().is_empty() {
                    user_blocks.remove(0);
                } else {
                    text_content.text = stripped;
                }
            }

            // UI: show the rendered envelope as a sibling user message so
            // the user can see what context was loaded for the skill. The
            // user's own typed message is already rendered by the normal
            // prompt flow, so we don't push it to the UI again here.
            let injected_id = acp_thread::ClientUserMessageId::new();
            acp_thread.update(cx, |acp_thread, cx| {
                acp_thread.push_user_content_block_with_indent(
                    Some(injected_id),
                    envelope_block.clone(),
                    true,
                    cx,
                );
            });

            // Model context: a single user message containing the skill
            // envelope followed by the user's appended content.
            let mut combined = Vec::with_capacity(user_blocks.len() + 1);
            combined.push(envelope_block);
            combined.extend(user_blocks);

            thread.update(cx, |thread, cx| {
                thread.push_acp_user_block(client_user_message_id, combined, path_style, cx);
            });

            let response_stream = thread.update(cx, |thread, cx| thread.send_existing(cx))?;

            let connection = this.upgrade().map(NativeAgentConnection::new);
            cx.update(|cx| {
                NativeAgentConnection::handle_thread_events(
                    response_stream,
                    acp_thread.downgrade(),
                    connection,
                    cx,
                )
            })
            .await
        })
    }
}

/// Wrapper struct that implements the AgentConnection trait
#[derive(Clone)]
pub struct NativeAgentConnection {
    agent: Entity<NativeAgent>,
    agent_id: AgentId,
    telemetry_id: SharedString,
}

impl NativeAgentConnection {
    pub fn new(agent: Entity<NativeAgent>) -> Self {
        Self::with_identity(agent, ZED_AGENT_ID.clone(), "zed".into())
    }

    pub fn gear(agent: Entity<NativeAgent>) -> Self {
        Self::with_identity(agent, GEAR_AGENT_ID.clone(), "gear".into())
    }

    pub(crate) fn with_identity(
        agent: Entity<NativeAgent>,
        agent_id: AgentId,
        telemetry_id: SharedString,
    ) -> Self {
        Self {
            agent,
            agent_id,
            telemetry_id,
        }
    }

    pub fn agent(&self) -> &Entity<NativeAgent> {
        &self.agent
    }

    fn is_gear(&self) -> bool {
        self.agent_id.as_ref() == GEAR_AGENT_ID.as_ref()
    }
}

impl NativeAgentConnection {
    pub fn thread(&self, session_id: &acp::SessionId, cx: &App) -> Option<Entity<Thread>> {
        self.agent
            .read(cx)
            .sessions
            .get(session_id)
            .map(|session| session.thread.clone())
    }

    pub fn gear_task_manager_snapshot(
        &self,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Option<GearTaskManagerSnapshot> {
        if !self.is_gear() {
            return None;
        }

        let task_manager = self
            .agent
            .read(cx)
            .sessions
            .get(session_id)
            .and_then(|session| session.gear_task_manager.clone())?;
        match task_manager.lock() {
            Ok(task_manager) => match task_manager.snapshot() {
                Ok(snapshot) => Some(snapshot),
                Err(error) => {
                    log::warn!(
                        "failed to snapshot Gear task manager for session {session_id}: {error:#}"
                    );
                    None
                }
            },
            Err(_) => {
                log::warn!(
                    "failed to snapshot Gear task manager for session {session_id}: task manager mutex poisoned"
                );
                None
            }
        }
    }

    pub fn gear_runtime_snapshot(
        &self,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Option<GearRuntimeSnapshot> {
        if !self.is_gear() {
            return None;
        }
        self.agent
            .read(cx)
            .sessions
            .get(session_id)
            .and_then(|session| session.gear_runtime_snapshot.clone())
    }

    pub fn gear_runtime_snapshot_error(
        &self,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Option<String> {
        if !self.is_gear() {
            return None;
        }
        self.agent
            .read(cx)
            .sessions
            .get(session_id)
            .and_then(|session| session.gear_runtime_snapshot_error.clone())
    }

    fn gear_task_manager(
        &self,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Option<SharedTaskManager> {
        if !self.is_gear() {
            return None;
        }

        self.agent
            .read(cx)
            .sessions
            .get(session_id)
            .and_then(|session| session.gear_task_manager.clone())
    }

    fn gear_task_manager_control(
        &self,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Option<TaskManagerControl> {
        if !self.is_gear() {
            return None;
        }

        self.agent
            .read(cx)
            .sessions
            .get(session_id)
            .and_then(|session| session.gear_task_manager_control.clone())
    }

    pub fn interrupt_gear_task(
        &self,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Result<ActionOutcome> {
        let Some(control) = self.gear_task_manager_control(session_id, cx) else {
            return Ok(ActionOutcome::Noop(OutcomeContext::default()));
        };
        let Some(task_id) = control.current_task_id()? else {
            return Ok(ActionOutcome::Noop(OutcomeContext::default()));
        };
        let Some(task_manager) = self.gear_task_manager(session_id, cx) else {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id),
                ..OutcomeContext::default()
            }));
        };
        task_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("gear task manager mutex poisoned"))?
            .interrupt_task_with_context(
                &task_id,
                &TaskCommandContext {
                    caller_session_id: Some(session_id.to_string()),
                    all_scope: false,
                },
            )
    }

    pub fn cancel_gear_task(&self, session_id: &acp::SessionId, cx: &App) -> Result<ActionOutcome> {
        let Some(control) = self.gear_task_manager_control(session_id, cx) else {
            return Ok(ActionOutcome::Noop(OutcomeContext::default()));
        };
        let Some(task_id) = control.current_task_id()? else {
            return Ok(ActionOutcome::Noop(OutcomeContext::default()));
        };
        let Some(task_manager) = self.gear_task_manager(session_id, cx) else {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id),
                ..OutcomeContext::default()
            }));
        };
        task_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("gear task manager mutex poisoned"))?
            .cancel_task_with_context(
                &task_id,
                &TaskCommandContext {
                    caller_session_id: Some(session_id.to_string()),
                    all_scope: false,
                },
            )
    }

    pub fn cancel_gear_task_for(
        &self,
        session_id: &acp::SessionId,
        task_id: &str,
        run_epoch: u64,
        cx: &App,
    ) -> Result<ActionOutcome> {
        let Some(task_manager) = self.gear_task_manager(session_id, cx) else {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        let mut task_manager = task_manager
            .lock()
            .map_err(|_| anyhow!("gear task manager mutex poisoned"))?;
        let task = task_manager
            .snapshot()?
            .tasks
            .into_iter()
            .find(|task| task.task_id == task_id)
            .context("selected Gear task no longer exists")?;
        if task.run_epoch != run_epoch {
            bail!("Gear task {task_id} epoch changed; refresh the runtime panel");
        }
        task_manager.cancel_task_with_context(
            task_id,
            &TaskCommandContext {
                caller_session_id: Some(session_id.to_string()),
                all_scope: false,
            },
        )
    }

    pub fn interrupt_gear_task_for(
        &self,
        session_id: &acp::SessionId,
        task_id: &str,
        run_epoch: u64,
        cx: &App,
    ) -> Result<ActionOutcome> {
        let Some(task_manager) = self.gear_task_manager(session_id, cx) else {
            return Ok(ActionOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        let mut task_manager = task_manager
            .lock()
            .map_err(|_| anyhow!("gear task manager mutex poisoned"))?;
        let task = task_manager
            .snapshot()?
            .tasks
            .into_iter()
            .find(|task| task.task_id == task_id)
            .context("selected Gear task no longer exists")?;
        if task.run_epoch != run_epoch {
            bail!("Gear task {task_id} epoch changed; refresh the runtime panel");
        }
        task_manager.interrupt_task_with_context(
            task_id,
            &TaskCommandContext {
                caller_session_id: Some(session_id.to_string()),
                all_scope: false,
            },
        )
    }

    pub fn stop_gear_continuation(&self, session_id: &acp::SessionId, cx: &App) -> Result<bool> {
        let (workspace, control, task_manager, cancellation_token) = self
            .agent
            .read_with(
                cx,
                |agent,
                 cx|
                 -> Result<
                    Option<(
                        std::path::PathBuf,
                        Option<TaskManagerControl>,
                        Option<SharedTaskManager>,
                        Option<CancellationToken>,
                    )>,
                > {
                    let Some(session) = agent.sessions.get(session_id) else {
                        return Ok(None);
                    };
                    let workspace = gear_workspace_for_session(session, agent, cx)?;
                    Ok(Some((
                        workspace,
                        session.gear_task_manager_control.clone(),
                        session.gear_task_manager.clone(),
                        session.gear_cancellation_token.clone(),
                    )))
                },
            )?
            .context("Gear session not found")?;
        let store = StateStore::new(workspace);
        store.initialize()?;
        let goal_id = store
            .read_continuation_state_for_session(&session_id.to_string())?
            .map(|state| state.goal_id)
            .unwrap_or_else(|| "active".to_string());
        store.write_continuation_state(
            &session_id.to_string(),
            &goal_id,
            ContinuationStatus::Stopped,
        )?;
        let current_task_id = control
            .as_ref()
            .map(|control| control.current_task_id())
            .transpose()?
            .flatten();
        store.append_event(&event(
            &session_id.to_string(),
            Some(&goal_id),
            current_task_id.as_deref(),
            EventKind::ContinuationStopped,
            "Gear continuation stopped by the user",
            serde_json::json!({
                "status": "stopped",
                "task_id": current_task_id,
            }),
        ))?;
        if let Some(cancellation_token) = cancellation_token {
            cancellation_token.cancel();
        }
        if let (Some(control), Some(task_manager)) = (control, task_manager)
            && let Some(task_id) = control.current_task_id()?
        {
            task_manager
                .lock()
                .map_err(|_| anyhow!("gear task manager mutex poisoned"))?
                .cancel_task(&task_id)?;
        }
        Ok(true)
    }

    pub fn restart_gear_continuation(
        &self,
        session_id: &acp::SessionId,
        cx: &mut App,
    ) -> Result<bool> {
        let workspace = self
            .agent
            .read_with(cx, |agent, cx| -> Result<Option<std::path::PathBuf>> {
                let Some(session) = agent.sessions.get(session_id) else {
                    return Ok(None);
                };
                Ok(Some(gear_workspace_for_session(session, agent, cx)?))
            })?
            .context("Gear session not found")?;
        let store = StateStore::new(&workspace);
        store.clear_continuation_stop_for_session(&session_id.to_string())?;
        let has_live_task_manager = self
            .agent
            .read(cx)
            .sessions
            .get(session_id)
            .is_some_and(|session| session.gear_task_manager.is_some());
        if !has_live_task_manager {
            let params = acp::PromptRequest::new(
                session_id.clone(),
                vec![GEAR_RESUME_CONTINUATION_MARKER.into()],
            );
            self.send_gear_prompt(acp_thread::ClientUserMessageId::new(), params, cx)
                .detach_and_log_err(cx);
        }
        Ok(true)
    }

    /// Confirm a durable rollback request without performing an implicit
    /// workspace mutation. The confirmation is an auditable hand-off to a
    /// future rollback executor and keeps the current safety boundary explicit.
    pub fn confirm_gear_rollback(&self, session_id: &acp::SessionId, cx: &App) -> Result<bool> {
        let workspace = self
            .agent
            .read_with(cx, |agent, cx| -> Result<Option<std::path::PathBuf>> {
                let Some(session) = agent.sessions.get(session_id) else {
                    return Ok(None);
                };
                Ok(Some(gear_workspace_for_session(session, agent, cx)?))
            })?
            .context("Gear session not found")?;
        let store = StateStore::new(&workspace);
        store.initialize()?;
        let continuation = store
            .read_continuation_state_for_session(&session_id.to_string())?
            .context("Gear session has no continuation goal")?;
        let epoch_id = store
            .read_goal_epoch_events(&continuation.goal_id)?
            .last()
            .map(|event| event.epoch_id.clone())
            .context("Gear goal has no durable epoch")?;
        let artifacts_dir = store.artifact_dir(&continuation.goal_id);
        let mut rollback_paths = std_fs::read_dir(&artifacts_dir)?
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("plan-rollback-iteration-"))
            })
            .collect::<Vec<_>>();
        rollback_paths.sort();
        let rollback_path = rollback_paths
            .pop()
            .context("Gear has no pending rollback request")?;
        let confirmation_path = store.write_artifact(
            &continuation.goal_id,
            "plan-rollback-confirmed.md",
            &format!(
                "# Plan Rollback Confirmation\n\nConfirmed by session `{}`.\n\nRequested artifact: `{}`\n\nNo workspace mutation was performed by this confirmation.\n",
                session_id,
                rollback_path.display()
            ),
        )?;
        store.append_goal_epoch_event(
            &continuation.goal_id,
            &epoch_id,
            &format!("{}.plan.rollback.confirmed", epoch_id),
            gearbox_agent::state::GoalEpochEventKind::PhaseCompleted,
            serde_json::json!({
                "phase": "plan_rollback_confirmed",
                "requested_artifact": rollback_path.to_string_lossy(),
                "confirmation_artifact": confirmation_path.to_string_lossy(),
                "automatic": false,
            }),
        )?;
        Ok(true)
    }

    pub fn send_follow_up_gear_task(
        &self,
        session_id: &acp::SessionId,
        prompt: String,
        cx: &App,
    ) -> Result<SendOutcome> {
        let Some(task_manager) = self.gear_task_manager(session_id, cx) else {
            return Ok(SendOutcome::Noop(OutcomeContext::default()));
        };
        let Some(task_id) = self
            .gear_task_manager_control(session_id, cx)
            .and_then(|control| control.current_task_id().ok().flatten())
        else {
            return Ok(SendOutcome::Noop(OutcomeContext::default()));
        };
        task_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("gear task manager mutex poisoned"))?
            .send_follow_up_task_with_context(
                &task_id,
                prompt,
                &TaskCommandContext {
                    caller_session_id: Some(session_id.to_string()),
                    all_scope: false,
                },
            )
    }

    pub fn steer_gear_task(
        &self,
        session_id: &acp::SessionId,
        prompt: String,
        cx: &App,
    ) -> Result<SteerOutcome> {
        let Some(task_manager) = self.gear_task_manager(session_id, cx) else {
            return Ok(SteerOutcome::Noop(OutcomeContext::default()));
        };
        let Some(task_id) = self
            .gear_task_manager_control(session_id, cx)
            .and_then(|control| control.current_task_id().ok().flatten())
        else {
            return Ok(SteerOutcome::Noop(OutcomeContext::default()));
        };
        task_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("gear task manager mutex poisoned"))?
            .steer_task_with_context(
                &task_id,
                prompt,
                &TaskCommandContext {
                    caller_session_id: Some(session_id.to_string()),
                    all_scope: false,
                },
            )
    }

    pub fn send_follow_up_gear_task_for(
        &self,
        session_id: &acp::SessionId,
        task_id: &str,
        run_epoch: u64,
        prompt: String,
        cx: &App,
    ) -> Result<SendOutcome> {
        let Some(task_manager) = self.gear_task_manager(session_id, cx) else {
            return Ok(SendOutcome::Noop(OutcomeContext::default()));
        };
        let mut task_manager = task_manager
            .lock()
            .map_err(|_| anyhow!("gear task manager mutex poisoned"))?;
        let Some(task) = task_manager
            .snapshot()?
            .tasks
            .into_iter()
            .find(|task| task.task_id == task_id)
        else {
            return Ok(SendOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        if task.run_epoch != run_epoch {
            bail!("Gear task {task_id} epoch changed; refresh the runtime panel");
        }
        task_manager.send_follow_up_task_with_context(
            task_id,
            prompt,
            &TaskCommandContext {
                caller_session_id: Some(session_id.to_string()),
                all_scope: false,
            },
        )
    }

    pub fn steer_gear_task_for(
        &self,
        session_id: &acp::SessionId,
        task_id: &str,
        run_epoch: u64,
        prompt: String,
        cx: &App,
    ) -> Result<SteerOutcome> {
        let Some(task_manager) = self.gear_task_manager(session_id, cx) else {
            return Ok(SteerOutcome::Noop(OutcomeContext::default()));
        };
        let mut task_manager = task_manager
            .lock()
            .map_err(|_| anyhow!("gear task manager mutex poisoned"))?;
        let Some(task) = task_manager
            .snapshot()?
            .tasks
            .into_iter()
            .find(|task| task.task_id == task_id)
        else {
            return Ok(SteerOutcome::Noop(OutcomeContext {
                task_id: Some(task_id.to_string()),
                ..OutcomeContext::default()
            }));
        };
        if task.run_epoch != run_epoch {
            bail!("Gear task {task_id} epoch changed; refresh the runtime panel");
        }
        task_manager.steer_task_with_context(
            task_id,
            prompt,
            &TaskCommandContext {
                caller_session_id: Some(session_id.to_string()),
                all_scope: false,
            },
        )
    }

    pub fn revive_gear_task_for(
        &self,
        session_id: &acp::SessionId,
        task_id: &str,
        run_epoch: u64,
        cx: &App,
    ) -> Result<SendOutcome> {
        self.send_follow_up_gear_task_for(
            session_id,
            task_id,
            run_epoch,
            "Retry the selected Gear task using its original goal, worker route, and verification contract. Preserve the previous failure as evidence and report the new attempt.".to_string(),
            cx,
        )
    }

    /// Forwards to [`NativeAgent::ensure_skills_scan_started`]. The
    /// agent panel calls this from its three user-interaction trigger
    /// points (input box focus, slash-autocomplete invocation, and
    /// conversation submit) so that the skills directory is observed
    /// only when the user is actually engaging with the panel.
    pub fn ensure_skills_scan_started(&self, cx: &mut App) {
        self.agent
            .update(cx, |agent, cx| agent.ensure_skills_scan_started(cx));
    }

    pub fn refresh_skills_for_project(&self, project: Entity<Project>, cx: &mut App) {
        self.agent.update(cx, |agent, cx| {
            let project_id = agent.get_or_create_project_state(&project, cx);
            agent.ensure_skills_scan_started(cx);
            if let Some(state) = agent.projects.get_mut(&project_id) {
                state.project_context_needs_refresh.send(()).ok();
            }
        });
    }

    pub fn available_skills(
        &self,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Vec<NativeAvailableSkill> {
        self.agent
            .read(cx)
            .session_project_state(session_id)
            .map(|state| {
                state
                    .skills
                    .iter()
                    .map(NativeAvailableSkill::from)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn load_thread(
        &self,
        id: acp::SessionId,
        project: Entity<Project>,
        cx: &mut App,
    ) -> Task<Result<Entity<Thread>>> {
        self.agent
            .update(cx, |this, cx| this.load_thread(id, project, cx))
    }

    fn run_turn(
        &self,
        session_id: acp::SessionId,
        cx: &mut App,
        f: impl 'static
        + FnOnce(Entity<Thread>, &mut App) -> Result<mpsc::UnboundedReceiver<Result<ThreadEvent>>>,
    ) -> Task<Result<acp::PromptResponse>> {
        let Some((thread, acp_thread)) = self.agent.update(cx, |agent, _cx| {
            agent
                .sessions
                .get_mut(&session_id)
                .map(|s| (s.thread.clone(), s.acp_thread.clone()))
        }) else {
            log::error!("Session not found in run_turn: {}", session_id);
            return Task::ready(Err(anyhow!("Session not found")));
        };
        log::debug!("Found session for: {}", session_id);

        let response_stream = match f(thread, cx) {
            Ok(stream) => stream,
            Err(err) => return Task::ready(Err(err)),
        };
        Self::handle_thread_events(
            response_stream,
            acp_thread.downgrade(),
            Some(self.clone()),
            cx,
        )
    }

    fn handle_thread_events(
        mut events: mpsc::UnboundedReceiver<Result<ThreadEvent>>,
        acp_thread: WeakEntity<AcpThread>,
        connection: Option<NativeAgentConnection>,
        cx: &App,
    ) -> Task<Result<acp::PromptResponse>> {
        cx.spawn(async move |cx| {
            // Handle response stream and forward to session.acp_thread
            while let Some(result) = events.next().await {
                match result {
                    Ok(event) => {
                        log::trace!("Received completion event: {:?}", event);

                        match event {
                            ThreadEvent::UserMessage(message) => {
                                acp_thread.update(cx, |thread, cx| {
                                    for content in &*message.content {
                                        thread.push_user_content_block(
                                            Some(message.id.clone()),
                                            content.clone().into(),
                                            cx,
                                        );
                                    }
                                })?;
                            }
                            ThreadEvent::AgentText(text) => {
                                acp_thread.update(cx, |thread, cx| {
                                    thread.push_assistant_content_block(text.into(), false, cx)
                                })?;
                            }
                            ThreadEvent::AgentThinking(text) => {
                                acp_thread.update(cx, |thread, cx| {
                                    thread.push_assistant_content_block(text.into(), true, cx)
                                })?;
                            }
                            ThreadEvent::ToolCallAuthorization(ToolCallAuthorization {
                                tool_call,
                                options,
                                response,
                                context: _,
                                kind,
                            }) => {
                                let outcome_task = acp_thread.update(cx, |thread, cx| {
                                    thread.request_tool_call_authorization(
                                        tool_call, options, kind, cx,
                                    )
                                })??;
                                cx.background_spawn(async move {
                                    if let acp_thread::RequestPermissionOutcome::Selected(outcome) =
                                        outcome_task.await
                                    {
                                        response
                                            .send(outcome)
                                            .map_err(|_| {
                                                anyhow!("authorization receiver was dropped")
                                            })
                                            .log_err();
                                    }
                                })
                                .detach();
                            }
                            ThreadEvent::ToolCallAuthorizationResolved {
                                tool_call_id,
                                outcome,
                            } => {
                                acp_thread.update(cx, |thread, cx| {
                                    thread.authorize_tool_call(tool_call_id, outcome, cx);
                                })?;
                            }
                            ThreadEvent::ToolCall(tool_call) => {
                                acp_thread.update(cx, |thread, cx| {
                                    thread.upsert_tool_call(tool_call, cx)
                                })??;
                            }
                            ThreadEvent::ToolCallUpdate(update) => {
                                acp_thread.update(cx, |thread, cx| {
                                    thread.update_tool_call(update, cx)
                                })??;
                            }
                            ThreadEvent::SubagentSpawned(session_id) => {
                                acp_thread.update(cx, |thread, cx| {
                                    thread.subagent_spawned(session_id, cx);
                                })?;
                            }
                            ThreadEvent::Retry(status) => {
                                if acp_thread::refusal_fallback_model_from_meta(&status.meta)
                                    .is_some()
                                {
                                    if let Some(connection) = &connection {
                                        cx.update(|cx| {
                                            connection.agent.update(cx, |agent, _| {
                                                agent.models.notify_model_selection_changed();
                                            });
                                        });
                                    }
                                }
                                acp_thread.update(cx, |thread, cx| {
                                    thread.update_retry_status(status, cx)
                                })?;
                            }
                            ThreadEvent::ContextCompaction(compaction) => {
                                acp_thread.update(cx, |thread, cx| {
                                    thread.push_context_compaction(compaction, cx);
                                })?;
                            }
                            ThreadEvent::ContextCompactionUpdate(update) => {
                                acp_thread.update(cx, |thread, cx| {
                                    thread.update_context_compaction(update, cx);
                                })?;
                            }
                            ThreadEvent::Stop(stop_reason) => {
                                log::debug!("Assistant message complete: {:?}", stop_reason);
                                return Ok(acp::PromptResponse::new(stop_reason));
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error in model response stream: {:?}", e);
                        return Err(e);
                    }
                }
            }

            log::debug!("Response stream completed");
            anyhow::Ok(acp::PromptResponse::new(acp::StopReason::EndTurn))
        })
    }

    fn send_gear_prompt(
        &self,
        client_user_message_id: acp_thread::ClientUserMessageId,
        params: acp::PromptRequest,
        cx: &mut App,
    ) -> Task<Result<acp::PromptResponse>> {
        let session_id = params.session_id.clone();
        let mut prompt_blocks = params.prompt;
        let resume_requested = gear_request_from_prompt(&prompt_blocks)
            .trim()
            .eq(GEAR_RESUME_CONTINUATION_MARKER);
        let mut request = gear_request_from_prompt(&prompt_blocks);
        let cancellation_token = CancellationToken::new();
        let task_manager_control = TaskManagerControl::default();

        if request.trim().is_empty() {
            return Task::ready(Err(anyhow!("Gear prompt cannot be empty")));
        }

        let session = self.agent.update(cx, |agent, cx| {
            let session = agent
                .sessions
                .get(&session_id)
                .context("Gear session not found")?;
            let state = agent
                .projects
                .get(&session.project_id)
                .context("Gear project state not found")?;
            let workspace = gear_workspace_for_session(session, agent, cx)?;
            let path_style = state.project.read(cx).path_style(cx);
            anyhow::Ok((
                session.acp_thread.clone(),
                session.thread.clone(),
                workspace,
                path_style,
            ))
        });
        let (acp_thread, thread, workspace, path_style) = match session {
            Ok(session) => session,
            Err(error) => return Task::ready(Err(error)),
        };

        if resume_requested {
            let store = StateStore::new(&workspace);
            let goal_id = match store.read_continuation_state_for_session(&session_id.to_string()) {
                Ok(Some(state)) => state.goal_id,
                Ok(None) => {
                    return Task::ready(Err(anyhow!("Gear continuation has no durable goal")));
                }
                Err(error) => return Task::ready(Err(error)),
            };
            request = match store.read_goal(&goal_id) {
                Ok(Some(goal)) if !goal.request.trim().is_empty() => goal.request,
                Ok(_) => {
                    return Task::ready(Err(anyhow!("Gear continuation goal has no request")));
                }
                Err(error) => return Task::ready(Err(error)),
            };
            prompt_blocks = vec![acp::ContentBlock::Text(acp::TextContent::new(
                "继续执行已持久化的 Gear 目标。",
            ))];
        }

        let (_thread_model, coordinator_language_model) = thread.update(cx, |thread, cx| {
            thread.push_acp_user_block(
                client_user_message_id,
                prompt_blocks.clone(),
                path_style,
                cx,
            );
            gear_coordinator_from_thread(thread)
        });

        if !is_gear_executable_goal(&request) {
            return cx.spawn(async move |cx| {
                push_gear_assistant_markdown(
                    &acp_thread,
                    &thread,
                    "你好，我是 Gear。请告诉我你想完成的目标，例如要生成、修改、修复或审查哪一项工作。"
                        .to_string(),
                    cx,
                );
                Ok(acp::PromptResponse::new(acp::StopReason::EndTurn))
            });
        }

        let phase_models =
            match gear_phase_models(&self.agent, coordinator_language_model.clone(), cx) {
                Ok(models) => models,
                Err(error) => return Task::ready(Err(error)),
            };
        let coordinator_model = Some(coordinator_model_for_language_model(
            &phase_models.planner_model,
        ));

        #[cfg(test)]
        let event_log = self
            .agent
            .read(cx)
            .sessions
            .get(&session_id)
            .and_then(|s| s.gear_lifecycle_events.clone());
        #[cfg(test)]
        let gear_worker_config_override = self.agent.read(cx).gear_worker_config_override.clone();
        #[cfg(test)]
        let phase_worker_config = gear_worker_config_override
            .clone()
            .unwrap_or_else(|| gear_worker_config_from_env(cx));
        #[cfg(not(test))]
        let phase_worker_config = gear_worker_config_from_env(cx);
        let phase_worker_config = if gear_phase_table_uses_codex_acp(&phase_models.routes) {
            gear_codex_acp_phase_worker_config(phase_worker_config)
        } else if gear_phase_table_uses_opencode(&phase_models.routes) {
            gear_open_code_phase_worker_config(phase_worker_config)
        } else {
            phase_worker_config
        };

        let (native_worker_tx, native_worker_rx) =
            async_channel::bounded::<GearZedWorkerDispatch>(GEAR_GUI_WORKER_DISPATCH_CAPACITY);
        let running_native_zed_sessions = Arc::new(Mutex::new(HashMap::default()));
        spawn_gear_zed_worker_dispatcher(
            self.agent.downgrade(),
            session_id.clone(),
            native_worker_rx,
            running_native_zed_sessions,
            #[cfg(test)]
            event_log.clone(),
            cx,
        );

        let (acp_broker_tx, acp_broker_rx) =
            async_channel::bounded::<GearAcpBrokerDispatch>(GEAR_GUI_WORKER_DISPATCH_CAPACITY);
        let running_acp_sessions: Arc<Mutex<HashMap<String, acp::SessionId>>> =
            Arc::new(Mutex::new(HashMap::default()));
        spawn_gear_acp_broker_dispatcher(
            self.agent.downgrade(),
            session_id.clone(),
            acp_broker_rx,
            running_acp_sessions,
            cx,
        );

        let native_backend: Arc<dyn NativeWorkerBackend> = Arc::new(
            GearZedWorkerBackend::new(native_worker_tx).with_acp_backend(acp_broker_tx.clone()),
        );
        let broker_registry = Arc::new(WorkerRegistry::with_native_backend(native_backend.clone()));
        let broker = Arc::new(WorkerBroker::new(
            broker_registry.clone(),
            workspace.join(".gear").join("artifacts"),
        ));
        let broker_factory = Arc::new(PhaseBrokerFactory::new(
            broker_registry,
            workspace.join(".gear"),
        ));
        let task_registry =
            WorkerRegistry::with_native_backend(native_backend).with_broker(broker.clone());

        let mut task_manager = TaskManager::with_control(task_manager_control.clone());
        task_manager.set_session_scope(session_id.to_string());
        task_manager.set_worker_registry(task_registry);
        let task_manager = task_manager.into_shared();
        let task_manager_tick_loop =
            TaskManagerTickLoop::start(task_manager.clone(), std::time::Duration::from_millis(50));

        self.agent.update(cx, |agent, _cx| {
            if let Some(session) = agent.sessions.get_mut(&session_id) {
                session.gear_cancellation_token = Some(cancellation_token.clone());
                session.gear_task_manager_control = Some(task_manager_control.clone());
                session.gear_task_manager = Some(task_manager.clone());
                session.gear_task_manager_tick_loop = Some(task_manager_tick_loop);
            }
        });

        self.agent.update(cx, |agent, cx| {
            agent.ensure_gear_runtime_snapshot_task(
                session_id.clone(),
                workspace.clone(),
                Some(task_manager.clone()),
                cx,
            );
        });

        let (event_tx, event_rx) = async_channel::bounded::<String>(GEAR_GUI_EVENT_BUFFER_CAPACITY);
        let (review_tx, review_rx) =
            async_channel::bounded::<GearCoordinatorReviewJob>(GEAR_GUI_REVIEW_QUEUE_CAPACITY);
        let (plan_critic_tx, plan_critic_rx) =
            async_channel::bounded::<GearPlanCriticJob>(GEAR_GUI_REVIEW_QUEUE_CAPACITY);
        let (plan_revision_tx, plan_revision_rx) =
            async_channel::bounded::<GearPlanRevisionJob>(GEAR_GUI_REVIEW_QUEUE_CAPACITY);

        let agent = self.agent.clone();
        let cancellation_session_id = session_id.clone();
        let run_broker = broker;
        let run_broker_factory = broker_factory;
        #[cfg(not(test))]
        let gui_gear_session = self.is_gear();
        #[cfg(test)]
        let gui_gear_session = false;
        cx.spawn(async move |cx| {
            let planner_uses_opencode =
                gear_phase_uses_opencode_worker(&phase_models, PhaseProfile::Planner)?;
            let critic_uses_opencode =
                gear_phase_uses_opencode_worker(&phase_models, PhaseProfile::PlanCritic)?;
            let opencode_phase_runner = GearOpenCodePhaseRunner {
                broker_factory: run_broker_factory.clone(),
                workspace: workspace.clone(),
                worker_config: phase_worker_config.clone(),
                cancellation_token: cancellation_token.clone(),
            };
            let review_language_model = phase_models.critic_model.clone();
            let review_workspace = workspace.clone();
            let review_task = cx.spawn(async move |cx| {
                while let Ok(job) = review_rx.recv().await {
                    let review = generate_gear_coordinator_review(
                        Some(review_language_model.clone()),
                        job.input,
                        &job.workspace,
                        cx,
                    )
                    .await;
                    if job.response_tx.send(Ok(review)).await.is_err() {
                        break;
                    }
                }
            });
            let critic_root_session_id = cancellation_session_id.to_string();
            let critic_language_model = phase_models.critic_model.clone();
            let plan_critic_task = cx.spawn(async move |cx| {
                while let Ok(job) = plan_critic_rx.recv().await {
                    let result = generate_gear_plan_critic(
                        critic_language_model.clone(),
                        job.input,
                        &critic_root_session_id,
                        cx,
                    )
                    .await;
                    if job.response_tx.send(result).await.is_err() {
                        break;
                    }
                }
            });
            let revision_root_session_id = cancellation_session_id.to_string();
            let revision_language_model = phase_models.planner_model.clone();
            let plan_revision_task = cx.spawn(async move |cx| {
                while let Ok(job) = plan_revision_rx.recv().await {
                    let result = generate_gear_plan_revision(
                        revision_language_model.clone(),
                        job.input,
                        &revision_root_session_id,
                        cx,
                    )
                    .await;
                    if job.response_tx.send(result).await.is_err() {
                        break;
                    }
                }
            });

            let coordinator_review_hook = if !critic_uses_opencode
                && gear_provider_review_enabled(Some(&phase_models.critic_model))
            {
                let review_tx = review_tx.clone();
                Some(Arc::new(move |input: CoordinatorReviewInput| {
                    let (response_tx, response_rx) = async_channel::bounded(1);
                    review_tx
                        .send_blocking(GearCoordinatorReviewJob {
                            input,
                            workspace: review_workspace.clone(),
                            response_tx,
                        })
                        .context("failed to send Gear coordinator review request")?;
                    response_rx
                        .recv_blocking()
                        .context("failed to receive Gear coordinator review response")?
                }) as CoordinatorReviewHook)
            } else {
                None
            };
            drop(review_tx);

            let plan_critic_hook: PlanCriticHook = if critic_uses_opencode {
                let runner = opencode_phase_runner.clone();
                Arc::new(move |input| runner.critique(input))
            } else {
                let plan_critic_tx = plan_critic_tx.clone();
                Arc::new(move |input: PlanCriticInput| {
                    let (response_tx, response_rx) = async_channel::bounded(1);
                    plan_critic_tx
                        .send_blocking(GearPlanCriticJob { input, response_tx })
                        .context("failed to send Gear PlanCritic request")?;
                    response_rx
                        .recv_blocking()
                        .context("failed to receive Gear PlanCritic response")?
                })
            };
            let plan_revision_hook: PlanRevisionHook = if planner_uses_opencode {
                let runner = opencode_phase_runner.clone();
                Arc::new(move |input| runner.revise(input))
            } else {
                let plan_revision_tx = plan_revision_tx.clone();
                Arc::new(move |input: PlanRevisionInput| {
                    let (response_tx, response_rx) = async_channel::bounded(1);
                    plan_revision_tx
                        .send_blocking(GearPlanRevisionJob { input, response_tx })
                        .context("failed to send Gear plan revision request")?;
                    response_rx
                        .recv_blocking()
                        .context("failed to receive Gear plan revision response")?
                })
            };
            let strategist_next_goal_hook = if planner_uses_opencode {
                let runner = opencode_phase_runner.clone();
                Some(Arc::new(move |input| runner.strategize(input)) as StrategistNextGoalHook)
            } else {
                None
            };
            drop(plan_critic_tx);
            drop(plan_revision_tx);

            let (planner_identity, intent_fold_hook, planner_hook, coordinator_brief) =
                if planner_uses_opencode {
                    let intent_runner = opencode_phase_runner.clone();
                    let planner_runner = opencode_phase_runner.clone();
                    (
                        None,
                        Some(Arc::new(move |input| intent_runner.fold_intent(input))
                            as IntentFoldHook),
                        Some(Arc::new(move |input| planner_runner.plan(input)) as PlannerHook),
                        None,
                    )
                } else {
                    (
                        Some(phase_execution_identity_for_model(
                            "planner",
                            &cancellation_session_id.to_string(),
                            &phase_models.planner_model,
                        )),
                        None,
                        None,
                        generate_gear_coordinator_brief(
                            Some(phase_models.planner_model.clone()),
                            &request,
                            cx,
                        )
                        .await,
                    )
                };
            let run_cancellation_token = cancellation_token.clone();
            let run_task_manager_control = task_manager_control.clone();
            let run_task_manager = task_manager.clone();
            let run_worker_config = phase_worker_config.clone();
            let run_phase_runtime = PhaseRuntime {
                routes: phase_models.routes.clone(),
                inventory: phase_models.inventory.clone(),
                current_model: phase_models.current_model.clone(),
                planner: planner_identity,
                intent_fold_hook,
                planner_hook,
                oracle_hook: Some(plan_critic_hook.clone()),
                plan_critic_hook: Some(plan_critic_hook),
                plan_revision_hook: Some(plan_revision_hook),
                strategist_next_goal_hook,
                require_plan_approval: true,
                max_plan_revisions: gear_max_plan_revisions_from_env(),
                broker: Some(run_broker),
                broker_factory: Some(run_broker_factory),
                direct_model_usage_provider: None,
            };
            let continuation_session_id = cancellation_session_id.to_string();
            let objective_policy = objective_policy_from_env()?;
            let objective_policy = if gui_gear_session && objective_policy.is_none() {
                Some(gearbox_agent::state::ObjectivePolicy::default())
            } else {
                objective_policy
            };
            let run_task = cx.background_spawn(smol::unblock(move || {
                if objective_policy.is_none() {
                    StateStore::new(&workspace)
                        .clear_continuation_stop_for_session(&continuation_session_id)?;
                }
                let event_sink = {
                    let event_tx = event_tx.clone();
                    Arc::new(move |event: &gearbox_agent::state::Event| {
                        event_tx.try_send(gear_event_status_markdown(event)).ok();
                    }) as gearbox_agent::runtime::EventSink
                };
                let run_options = RunOptions {
                    request,
                    workspace,
                    verification_commands: gear_verification_commands_from_env(),
                    worker: run_worker_config,
                    allowed_paths: Vec::new(),
                    forbidden_paths: Vec::new(),
                    max_files_changed: gear_max_files_changed_from_env(),
                    install_dependencies: false,
                    event_sink: Some(event_sink),
                    cancellation_token: Some(run_cancellation_token),
                    max_iterations: gear_max_iterations_from_env(),
                    max_provider_unknown_streak: gear_max_provider_unknown_streak_from_env(),
                    max_child_depth: gear_max_child_depth_from_env(),
                    max_runtime_minutes: gear_max_runtime_minutes_from_env(),
                    budget: Some(gear_budget_from_env()),
                    coordinator_model,
                    coordinator_brief,
                    coordinator_review_hook,
                    task_manager_control: Some(run_task_manager_control),
                    task_manager: Some(run_task_manager),
                    session_id: Some(continuation_session_id),
                    continuation: true,
                    intensity: trimmed_env_value("GEARBOX_GEAR_WORKER_INTENSITY")
                        .as_deref()
                        .and_then(Intensity::parse),
                };
                let outcome = if let Some(policy) = objective_policy {
                    Orchestrator::run_objective_with_phase_runtime(
                        run_options,
                        run_phase_runtime,
                        policy,
                    )?
                    .into_last_goal_outcome()?
                } else {
                    Orchestrator::run_with_phase_runtime(run_options, run_phase_runtime)?
                };
                let final_report = std_fs::read_to_string(&outcome.final_report_path)
                    .with_context(|| {
                        format!(
                            "failed to read Gear final report {}",
                            outcome.final_report_path.display()
                        )
                    })?;
                anyhow::Ok((outcome, final_report))
            }));

            let mut events_open = true;
            let mut last_task_manager_snapshot = None;
            let run_task = run_task.fuse();
            futures::pin_mut!(run_task);
            let run_result = loop {
                if events_open {
                    futures::select! {
                        message = event_rx.recv().fuse() => match message {
                            Ok(message) => {
                                push_gear_assistant_markdown(&acp_thread, &thread, message, cx);
                                push_gear_task_manager_snapshot_if_changed(
                                    &task_manager,
                                    &mut last_task_manager_snapshot,
                                    &acp_thread,
                                    &thread,
                                    cx,
                                );
                            }
                            Err(_) => events_open = false,
                        },
                        result = run_task => break result,
                    }
                } else {
                    break run_task.await;
                }
            };
            while let Ok(message) = event_rx.try_recv() {
                push_gear_assistant_markdown(&acp_thread, &thread, message, cx);
                push_gear_task_manager_snapshot_if_changed(
                    &task_manager,
                    &mut last_task_manager_snapshot,
                    &acp_thread,
                    &thread,
                    cx,
                );
            }
            push_gear_task_manager_snapshot_if_changed(
                &task_manager,
                &mut last_task_manager_snapshot,
                &acp_thread,
                &thread,
                cx,
            );

            let response = match run_result {
                Ok((outcome, final_report)) => {
                    let message = gear_response_markdown(&outcome, &final_report);
                    push_gear_assistant_markdown(&acp_thread, &thread, message, cx);
                    Ok(acp::PromptResponse::new(acp::StopReason::EndTurn))
                }
                Err(_) if cancellation_token.is_cancelled() => {
                    push_gear_assistant_markdown(
                        &acp_thread,
                        &thread,
                        "Gear run cancelled.\n\n".to_string(),
                        cx,
                    );
                    Ok(acp::PromptResponse::new(acp::StopReason::Cancelled))
                }
                Err(error) => {
                    let message = format!("# Gear run failed\n\n```text\n{error:#}\n```");
                    push_gear_assistant_markdown(&acp_thread, &thread, message, cx);
                    Err(error)
                }
            };

            clear_gear_cancellation_token(
                &agent,
                &cancellation_session_id,
                &cancellation_token,
                &task_manager_control,
                &task_manager,
                #[cfg(test)]
                event_log,
                cx,
            );
            drop(review_task);
            drop(plan_critic_task);
            drop(plan_revision_task);
            response
        })
    }
}

fn clear_gear_cancellation_token(
    agent: &Entity<NativeAgent>,
    session_id: &acp::SessionId,
    cancellation_token: &CancellationToken,
    task_manager_control: &TaskManagerControl,
    task_manager: &SharedTaskManager,
    #[cfg(test)] lifecycle_events: Option<Arc<Mutex<Vec<String>>>>,
    cx: &mut AsyncApp,
) {
    #[cfg(test)]
    if let Some(events) = &lifecycle_events {
        if let Ok(mut guard) = events.lock() {
            guard.push("clear_gear_token:enter".to_string());
        }
    }
    agent.update(cx, |agent, _cx| {
        if let Some(session) = agent.sessions.get_mut(session_id) {
            let should_clear_token = session
                .gear_cancellation_token
                .as_ref()
                .is_some_and(|token| token.is_same(cancellation_token));
            if should_clear_token {
                session.gear_cancellation_token = None;
            }
            let should_clear_control = session
                .gear_task_manager_control
                .as_ref()
                .is_some_and(|control| control.is_same(task_manager_control));
            if should_clear_control {
                session.gear_task_manager_control = None;
            }
            let should_clear_manager = session
                .gear_task_manager
                .as_ref()
                .is_some_and(|manager| Arc::ptr_eq(manager, task_manager));
            if should_clear_manager {
                session.gear_task_manager_tick_loop = None;
                session.gear_task_manager = None;
                session.gear_runtime_snapshot_task = None;
            }
        }
    });
}

fn push_gear_assistant_markdown(
    acp_thread: &Entity<AcpThread>,
    thread: &Entity<Thread>,
    message: String,
    cx: &mut AsyncApp,
) {
    let block = acp::ContentBlock::Text(acp::TextContent::new(message));
    acp_thread.update(cx, |acp_thread, cx| {
        acp_thread.push_assistant_content_block(block.clone(), false, cx);
    });
    thread.update(cx, |thread, cx| {
        thread.push_acp_agent_block(block, cx);
    });
}

fn gear_request_from_prompt(prompt: &[acp::ContentBlock]) -> String {
    prompt
        .iter()
        .filter_map(|block| match block {
            acp::ContentBlock::Text(text) => Some(text.text.trim()),
            _ => None,
        })
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn gear_coordinator_from_thread(
    thread: &Thread,
) -> (Option<CoordinatorModel>, Option<Arc<dyn LanguageModel>>) {
    let model = thread.model().cloned();
    let metadata = model.as_ref().map(|model| CoordinatorModel {
        provider_id: model.provider_id().0.to_string(),
        model_id: model.id().0.to_string(),
        name: model.name().0.to_string(),
    });
    (metadata, model)
}

fn coordinator_model_for_language_model(model: &Arc<dyn LanguageModel>) -> CoordinatorModel {
    CoordinatorModel {
        provider_id: model.provider_id().0.to_string(),
        model_id: model.id().0.to_string(),
        name: model.name().0.to_string(),
    }
}

fn phase_execution_identity_for_model(
    phase: &str,
    root_session_id: &str,
    model: &Arc<dyn LanguageModel>,
) -> PhaseExecutionIdentity {
    let suffix = id_timestamp();
    PhaseExecutionIdentity {
        execution_id: format!("{phase}_execution_{suffix}"),
        phase_session_id: format!("{root_session_id}:{phase}:{suffix}"),
        backend: PhaseExecutionBackend::LanguageModelRequest,
        agent_id: Some("zed".to_string()),
        provider_id: Some(model.provider_id().0.to_string()),
        model_id: Some(model.id().0.to_string()),
        actual_session_id: None,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GearPhaseRouteOverride {
    candidates: Vec<PhaseRouteCandidate>,
}

fn gear_phase_models(
    agent: &Entity<NativeAgent>,
    current_language_model: Option<Arc<dyn LanguageModel>>,
    cx: &App,
) -> Result<GearPhaseModels> {
    let routes = gear_phase_route_table_from_env()?;
    let inventory = LiveModelInventory {
        models: LanguageModelRegistry::read_global(cx)
            .available_models(cx)
            .map(|model| ModelSelectorId {
                agent_id: "zed".to_string(),
                provider_id: model.provider_id().0.to_string(),
                model_id: model.id().0.to_string(),
            })
            .collect(),
    };
    inventory.validate()?;
    let current_model = current_language_model
        .as_ref()
        .map(|model| ModelSelectorId {
            agent_id: "zed".to_string(),
            provider_id: model.provider_id().0.to_string(),
            model_id: model.id().0.to_string(),
        });
    let planner_decision =
        routes.resolve(&PhaseProfile::Planner, &inventory, current_model.as_ref())?;
    let critic_decision = routes.resolve(
        &PhaseProfile::PlanCritic,
        &inventory,
        current_model.as_ref(),
    )?;
    let models = &agent.read(cx).models;
    let planner_model = resolve_phase_host_language_model(
        &planner_decision,
        current_language_model.as_ref(),
        models,
    )?;
    let critic_model = resolve_phase_host_language_model(
        &critic_decision,
        current_language_model.as_ref(),
        models,
    )?;
    Ok(GearPhaseModels {
        routes,
        inventory,
        current_model,
        planner_model,
        critic_model,
    })
}

fn resolve_phase_host_language_model(
    decision: &gearbox_agent::phase_routing::PhaseRouteDecision,
    current_language_model: Option<&Arc<dyn LanguageModel>>,
    models: &LanguageModels,
) -> Result<Arc<dyn LanguageModel>> {
    if matches!(
        decision.candidate.backend,
        PhaseBackend::Worker(_) | PhaseBackend::CodexAcp
    ) {
        return current_language_model.cloned().context(
            "Gear requires a host model while an OpenCode/Codex Acp phase session is active",
        );
    }
    resolve_phase_language_model(decision, current_language_model, models)
}

fn resolve_phase_language_model(
    decision: &gearbox_agent::phase_routing::PhaseRouteDecision,
    current_language_model: Option<&Arc<dyn LanguageModel>>,
    models: &LanguageModels,
) -> Result<Arc<dyn LanguageModel>> {
    let requested = decision
        .requested_model
        .as_ref()
        .context("direct-model phase did not resolve a concrete provider/model")?;
    if let Some(current) = current_language_model
        && current.provider_id().0.as_ref() == requested.provider_id
        && current.id().0.as_ref() == requested.model_id
    {
        return Ok(current.clone());
    }
    let model_id = AgentModelId::new(requested.qualified_model_id());
    models.model_from_id(&model_id).with_context(|| {
        format!(
            "resolved phase model `{}` is not available in the native Agent model registry",
            requested.qualified_model_id()
        )
    })
}

fn gear_codex_acp_model_profiles_from_env() -> Option<CodexAcpModelProfiles> {
    let planner = trimmed_env_value("GEARBOX_GEAR_CODEX_ACP_PLANNER_MODEL")?;
    let executor = trimmed_env_value("GEARBOX_GEAR_CODEX_ACP_EXECUTOR_MODEL")
        .unwrap_or_else(|| planner.clone());
    let reviewer = trimmed_env_value("GEARBOX_GEAR_CODEX_ACP_REVIEWER_MODEL")
        .unwrap_or_else(|| planner.clone());
    let profiles = CodexAcpModelProfiles {
        codex_planner: planner,
        opencode_executor: executor,
        codex_reviewer: reviewer,
    };
    profiles.validate().ok()?;
    Some(profiles)
}

fn gear_phase_route_table_from_env() -> Result<PhaseRouteTable> {
    if let Some(raw) = trimmed_env_value("GEARBOX_GEAR_PHASE_ROUTES") {
        let table: PhaseRouteTable = serde_json::from_str(&raw)
            .context("GEARBOX_GEAR_PHASE_ROUTES is not a valid PhaseRouteTable JSON object")?;
        table.validate()?;
        return Ok(table);
    }

    let mut table = match gear_codex_acp_model_profiles_from_env() {
        Some(profiles) => PhaseRouteTable::codex_acp_opencode(profiles)?,
        None => match gear_opencode_model_profiles_from_env()? {
            Some(models) => PhaseRouteTable::opencode_only(models)?,
            None => PhaseRouteTable::legacy_defaults(),
        },
    };
    for (phase, environment_name) in [
        (PhaseProfile::Planner, "GEARBOX_GEAR_PHASE_PLANNER"),
        (PhaseProfile::PlanCritic, "GEARBOX_GEAR_PHASE_PLAN_CRITIC"),
        (
            PhaseProfile::Orchestrator,
            "GEARBOX_GEAR_PHASE_ORCHESTRATOR",
        ),
        (
            PhaseProfile::ExecutorQuick,
            "GEARBOX_GEAR_PHASE_EXECUTOR_QUICK",
        ),
        (
            PhaseProfile::ExecutorDeep,
            "GEARBOX_GEAR_PHASE_EXECUTOR_DEEP",
        ),
        (
            PhaseProfile::ReviewerTask,
            "GEARBOX_GEAR_PHASE_REVIEWER_TASK",
        ),
        (
            PhaseProfile::ReviewerFinal,
            "GEARBOX_GEAR_PHASE_REVIEWER_FINAL",
        ),
        (
            PhaseProfile::StrategistNextGoal,
            "GEARBOX_GEAR_PHASE_STRATEGIST_NEXT_GOAL",
        ),
        (PhaseProfile::Summarizer, "GEARBOX_GEAR_PHASE_SUMMARIZER"),
    ] {
        let Some(raw) = trimmed_env_value(environment_name) else {
            continue;
        };
        let route_override: GearPhaseRouteOverride = serde_json::from_str(&raw)
            .with_context(|| format!("{environment_name} is not a valid phase route override"))?;
        if route_override.candidates.is_empty() {
            anyhow::bail!("{environment_name} must define at least one explicit candidate");
        }
        let profile = table
            .profiles
            .iter_mut()
            .find(|profile| profile.phase == phase)
            .with_context(|| format!("missing built-in profile for phase {phase:?}"))?;
        profile.candidates = route_override.candidates;
        profile.source = PhaseRouteSource::Environment;
    }
    table.validate()?;
    Ok(table)
}

fn gear_phase_table_uses_opencode(table: &PhaseRouteTable) -> bool {
    table.profiles.iter().any(|profile| {
        profile.candidates.iter().any(|candidate| {
            matches!(
                candidate.backend,
                PhaseBackend::Worker(WorkerKind::OpencodeSession)
            )
        })
    })
}

fn gear_phase_table_uses_codex_acp(table: &PhaseRouteTable) -> bool {
    table.profiles.iter().any(|profile| {
        profile
            .candidates
            .iter()
            .any(|candidate| matches!(candidate.backend, PhaseBackend::CodexAcp))
    })
}

fn gear_opencode_model_profiles_from_env() -> Result<Option<OpenCodeModelProfiles>> {
    let explicitly_enabled = trimmed_env_value("GEARBOX_GEAR_OPENCODE_PHASES")
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
    gear_opencode_model_profiles_from_values(
        explicitly_enabled,
        trimmed_env_value("GEARBOX_GEAR_OPENCODE_PLANNER_MODEL"),
        trimmed_env_value("GEARBOX_GEAR_OPENCODE_EXECUTOR_MODEL"),
        trimmed_env_value("GEARBOX_GEAR_OPENCODE_REVIEWER_MODEL"),
        trimmed_env_value("GEARBOX_GEAR_WORKER_MODEL"),
    )
}

fn gear_opencode_model_profiles_from_values(
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

fn gear_provider_review_enabled(model: Option<&Arc<dyn LanguageModel>>) -> bool {
    let Some(_) = model else {
        return false;
    };

    #[cfg(test)]
    if let Some(model) = model
        && model.provider_id().0.as_ref() == "fake"
    {
        return false;
    }

    true
}

struct GearCoordinatorReviewJob {
    input: CoordinatorReviewInput,
    workspace: PathBuf,
    response_tx: async_channel::Sender<Result<Option<CoordinatorReview>>>,
}

struct GearPlanCriticJob {
    input: PlanCriticInput,
    response_tx: async_channel::Sender<Result<PlanCriticSubmission>>,
}

struct GearPlanRevisionJob {
    input: PlanRevisionInput,
    response_tx: async_channel::Sender<Result<PlanRevisionSubmission>>,
}

struct GearPhaseModels {
    routes: PhaseRouteTable,
    inventory: LiveModelInventory,
    current_model: Option<ModelSelectorId>,
    planner_model: Arc<dyn LanguageModel>,
    critic_model: Arc<dyn LanguageModel>,
}

fn gear_phase_uses_opencode_worker(models: &GearPhaseModels, phase: PhaseProfile) -> Result<bool> {
    let decision =
        models
            .routes
            .resolve(&phase, &models.inventory, models.current_model.as_ref())?;
    Ok(matches!(
        decision.candidate.backend,
        PhaseBackend::Worker(WorkerKind::OpencodeSession) | PhaseBackend::CodexAcp
    ))
}

#[derive(Clone)]
struct GearOpenCodePhaseRunner {
    broker_factory: Arc<PhaseBrokerFactory>,
    workspace: PathBuf,
    worker_config: WorkerConfig,
    cancellation_token: CancellationToken,
}

impl GearOpenCodePhaseRunner {
    fn to_inner(&self) -> gearbox_agent::open_code_phase_runtime::OpenCodePhaseRunner {
        gearbox_agent::open_code_phase_runtime::OpenCodePhaseRunner::new(
            self.broker_factory.clone(),
            self.workspace.clone(),
            self.worker_config.clone(),
            self.cancellation_token.clone(),
        )
    }

    fn fold_intent(&self, input: IntentFoldInput) -> Result<IntentFoldSubmission> {
        self.to_inner().fold_intent(input)
    }

    fn plan(&self, input: PlannerInput) -> Result<PlannerSubmission> {
        self.to_inner().plan(input)
    }

    fn critique(&self, input: PlanCriticInput) -> Result<PlanCriticSubmission> {
        self.to_inner().critique(input)
    }

    fn revise(&self, input: PlanRevisionInput) -> Result<PlanRevisionSubmission> {
        self.to_inner().revise(input)
    }

    fn strategize(&self, input: StrategistNextGoalInput) -> Result<StrategistNextGoalSubmission> {
        self.to_inner().strategize(input)
    }
}

async fn generate_gear_coordinator_brief(
    model: Option<Arc<dyn LanguageModel>>,
    request: &str,
    cx: &AsyncApp,
) -> Option<String> {
    let Some(model) = model else {
        return None;
    };

    let request = LanguageModelRequest {
        intent: Some(CompletionIntent::UserPrompt),
        temperature: Some(0.2),
        messages: vec![
            LanguageModelRequestMessage {
                role: Role::System,
                content: vec![
                    r#"You are Gear's high-reasoning planner. Do not write code. Return exactly one JSON object and no prose or markdown fences. The object must match this PlanGraphDraft contract. Include evidence-backed findings, adopted assumptions, decisions with rationale, and unresolved open questions in the top-level context fields. For QA, enumerate applicable adversarial trigger classes or record a concrete not-applicable reason with evidence:
{
  "objective": "string",
  "assumptions": ["adopted reversible default with rationale"],
  "findings": ["path:line — verified repository fact"],
  "decisions": ["decision — rationale"],
  "open_questions": [],
  "must_have": ["string"],
  "must_not_have": ["string"],
  "topology_lock": ["string"],
  "preflight": ["baseline and scope check"],
  "rollback": ["bounded recovery action if verification fails"],
  "final_verification": ["final verification wave and evidence"],
  "tasks": [{
    "task_id": "ascii_identifier",
    "title": "string",
    "goal": "string",
    "deliverable": "string",
    "dependencies": ["task_id"],
    "parallel_wave": 0,
    "scope": {"allowed_files": ["path"], "forbidden_files": ["path"], "write_scope": ["path"], "max_files_changed": 8},
    "required_capabilities": ["read", "edit", "test"],
    "preferred_phase_profile": "executor_quick|executor_deep|reviewer_task",
    "inputs": ["path or repository fact to read before editing"],
    "preconditions": ["observable condition required before starting"],
    "must_do": ["string"],
    "execution_steps": [{"step_id": "step-001", "action": "string", "expected_observation": "string", "evidence_path": null}],
    "execution_steps_evidence_required": true,
    "must_not_do": ["string"],
    "references": [{"path": "path", "reason": "string", "symbol": null}],
    "test": {
      "strategy": "tdd|tests_after|none",
      "red": {"command": "command", "expected_observation": "specific missing-behavior failure", "evidence_path": "path"},
      "green": [{"command": "command", "expected_observation": "specific success", "evidence_path": "path"}],
      "no_test_reason": null
    },
    "qa": {
      "happy_path": [{"name": "string", "steps": ["string"], "expected_result": "string", "evidence_path": "path"}],
      "failure_path": [{"name": "string", "steps": ["string"], "expected_result": "string", "evidence_path": "path"}],
      "adversarial_path": [{"name": "trigger class or not-applicable", "steps": ["string"], "expected_result": "observable or explicit not-applicable reason", "evidence_path": "path"}]
    },
    "artifacts": [{"path": "path", "description": "string", "required": true}],
    "evidence": ["observable proof obligation"],
    "rollback": ["bounded recovery action for this work order"],
    "budget": {"max_attempts": 2, "max_commands": 3, "max_duration_seconds": null},
    "commit_boundary": "no_commit|after_task|after_wave",
    "completion_predicates": ["agent-executable predicate"]
  }],
  "final_acceptance": ["agent-executable predicate"]
}
Before emitting JSON, decompose in this order: (1) derive one observable objective and its must-have/must-not-have boundaries, (2) split the objective into the smallest independently verifiable work orders, (3) connect dependencies and execution waves, and (4) assign each work order its own inputs, preconditions, scope, test, QA, evidence, rollback, budget, artifact, completion predicates, and commit intent. Each work order must have one primary deliverable and be executable by a weaker worker without redesigning the architecture. Do not combine unrelated discovery, implementation, review, or cleanup into one work order. Treat more than 8 explicitly scoped files or more than 12 must_do steps as a signal to split the work order. Every dependency must reference an earlier wave. Same-wave write scopes must not overlap. TDD requires RED and GREEN to use the same test command. tests_after requires GREEN. Include happy, failure, and adversarial QA with evidence paths; when adversarial behavior is not applicable, record a not-applicable trigger check. When a commit is requested, provide a concrete `commit_message`; Gear never commits or pushes automatically. The plan-level preflight, rollback, and final_verification lists are mandatory and must contain concrete, agent-executable evidence steps. Keep cheap executors inside a closed-world contract: do not leave architecture, scope, tests, acceptance, or commit intent for them to redesign. If a repository fact is unknown, keep the scope conservative and encode the missing fact as a must_do inspection step; never invent a path or symbol."#.into(),
                ],
                cache: false,
                reasoning_details: None,
            },
            LanguageModelRequestMessage {
                role: Role::User,
                content: vec![request.to_string().into()],
                cache: false,
                reasoning_details: None,
            },
        ],
        ..Default::default()
    };

    let mut stream = match model.stream_completion_text(request, cx).await {
        Ok(stream) => stream.stream,
        Err(error) => {
            log::warn!("Gear coordinator model request failed: {error}");
            return None;
        }
    };
    let mut brief = String::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(text) => {
                brief.push_str(&text);
                if brief.len() >= 65_536 {
                    break;
                }
            }
            Err(error) => {
                log::warn!("Gear coordinator model stream failed: {error}");
                return None;
            }
        }
    }

    let brief = brief.trim();
    if brief.is_empty() {
        None
    } else {
        Some(brief.to_string())
    }
}

async fn generate_gear_plan_critic(
    model: Arc<dyn LanguageModel>,
    input: PlanCriticInput,
    root_session_id: &str,
    cx: &AsyncApp,
) -> Result<PlanCriticSubmission> {
    let evidence = serde_json::to_string_pretty(&serde_json::json!({
        "request": input.request,
        "plan": input.plan,
        "planner_receipt": input.planner_receipt,
        "deterministic_verifier": input.verifier_report,
        "phase_route_decision": input.route_decision,
    }))
    .context("failed to serialize Gear PlanCritic evidence")?;
    let request = LanguageModelRequest {
        intent: Some(CompletionIntent::UserPrompt),
        temperature: Some(0.1),
        messages: vec![
            LanguageModelRequestMessage {
                role: Role::System,
                content: vec![
                    r#"You are Gear's read-only PlanCritic. You cannot inspect repository contents or use tools in this phase. Judge only the sealed PlanGraph and the deterministic verifier evidence, and preserve that limitation in your reasoning. Return exactly one JSON object with no markdown fences:
{
  "schema_version": 1,
  "reviewed_goal_id": "exact goal id",
  "reviewed_plan_id": "exact plan id",
  "reviewed_plan_revision": 1,
  "reviewed_plan_hash": "exact plan hash",
  "reviewed_planner_execution_id": "exact planner execution id",
  "decision": "approve|revise|reject",
  "checks": [
    {"dimension":"references|executability|contradictions|scope|tdd|qa|acceptance","verdict":"pass|fail","summary":"specific result","evidence_refs":["verifier:<dimension> or plan:<task_id>"]}
  ],
  "findings": [
    {"dimension":"references|executability|contradictions|scope|tdd|qa|acceptance","severity":"blocking|advisory","code":"stable_code","task_id":null,"path":null,"message":"specific issue","required_change":null}
  ],
  "revision_instructions": null,
  "needs_user_reason": null,
  "summary": "specific review summary"
}
Return exactly seven checks, one per dimension. Approve only when every check passes, deterministic verification passes, and there are no blocking findings. Revise requires failed checks, one to three blocking findings, and concrete revision_instructions. Reject is only for a blocker requiring user input and must set needs_user_reason."#.into(),
                ],
                cache: false,
                reasoning_details: None,
            },
            LanguageModelRequestMessage {
                role: Role::User,
                content: vec![evidence.into()],
                cache: false,
                reasoning_details: None,
            },
        ],
        ..Default::default()
    };
    let raw_output = stream_gear_phase_model_text(&model, request, "PlanCritic", cx).await?;
    let verdict = PlanCriticVerdict::parse(&raw_output)?;
    Ok(PlanCriticSubmission {
        reviewer: phase_execution_identity_for_model("plan_critic", root_session_id, &model),
        verdict,
        raw_output,
        artifact_path: None,
        repository_evidence_path: None,
    })
}

async fn generate_gear_plan_revision(
    model: Arc<dyn LanguageModel>,
    input: PlanRevisionInput,
    root_session_id: &str,
    cx: &AsyncApp,
) -> Result<PlanRevisionSubmission> {
    let evidence = serde_json::to_string_pretty(&serde_json::json!({
        "request": input.request,
        "current_plan": input.plan,
        "planner_receipt": input.planner_receipt,
        "critic_receipt": input.critic_receipt,
        "phase_route_decision": input.route_decision,
    }))
    .context("failed to serialize Gear plan revision evidence")?;
    let request = LanguageModelRequest {
        intent: Some(CompletionIntent::UserPrompt),
        temperature: Some(0.15),
        messages: vec![
            LanguageModelRequestMessage {
                role: Role::System,
                content: vec![
                    r#"You are Gear's high-reasoning planner revising a rejected PlanGraphDraft. Do not write code. Preserve and repair the top-level findings, assumptions, decisions, and open_questions context while applying every blocking PlanCritic required_change and revision_instructions without expanding the original objective. Re-run the decomposition order: preserve one observable objective, split oversized or multi-deliverable work orders into independently verifiable nodes, then repair dependencies, inputs, preconditions, scopes, ordered execution_steps, tests, QA, evidence, rollback, budget, artifacts, and completion predicates. Each execution step must retain stable identity, one action, one expected observation, and optional evidence; workers must not skip or reorder it. Treat more than 8 explicitly scoped files or more than 12 execution_steps as a split request unless critic evidence proves the node is atomic. Return exactly one complete PlanGraphDraft JSON object, with the same field contract as current_plan.draft, and no prose or markdown fences. Preserve decision-complete task scope, dependency waves, TDD RED/GREEN commands, happy/failure QA, required artifacts, and decidable acceptance. Do not return a patch."#.into(),
                ],
                cache: false,
                reasoning_details: None,
            },
            LanguageModelRequestMessage {
                role: Role::User,
                content: vec![evidence.into()],
                cache: false,
                reasoning_details: None,
            },
        ],
        ..Default::default()
    };
    let raw_output = stream_gear_phase_model_text(&model, request, "plan revision", cx).await?;
    let draft = gearbox_agent::plan_graph::parse_planner_draft(&raw_output)?;
    Ok(PlanRevisionSubmission {
        draft,
        planner: phase_execution_identity_for_model("planner_revision", root_session_id, &model),
        raw_output,
        artifact_path: None,
    })
}

async fn stream_gear_phase_model_text(
    model: &Arc<dyn LanguageModel>,
    request: LanguageModelRequest,
    phase_label: &str,
    cx: &AsyncApp,
) -> Result<String> {
    let mut stream = model
        .stream_completion_text(request, cx)
        .await
        .with_context(|| format!("Gear {phase_label} model request failed"))?
        .stream;
    let mut output = String::new();
    while let Some(chunk) = stream.next().await {
        output.push_str(&chunk.with_context(|| format!("Gear {phase_label} model stream failed"))?);
        if output.len() >= 131_072 {
            anyhow::bail!("Gear {phase_label} model response exceeded 128 KiB");
        }
    }
    let output = output.trim();
    if output.is_empty() {
        anyhow::bail!("Gear {phase_label} model returned an empty response");
    }
    Ok(output.to_string())
}

fn format_list_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values
            .iter()
            .map(|value| format!("- {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

async fn generate_gear_coordinator_review(
    model: Option<Arc<dyn LanguageModel>>,
    input: CoordinatorReviewInput,
    workspace: &Path,
    cx: &AsyncApp,
) -> Option<CoordinatorReview> {
    let Some(model) = model else {
        return None;
    };

    let mut model_fields = std::collections::HashMap::new();
    model_fields.insert("worker_kind".to_string(), input.worker_kind.clone());
    model_fields.insert("worker_category".to_string(), input.worker_category.clone());
    if let Some(worker_model) = input.worker_model.as_ref() {
        model_fields.insert("worker_model".to_string(), worker_model.clone());
    }
    let sanitized_model_fields = sanitize_model_fields(&model_fields);
    let model_metadata = if sanitized_model_fields.is_empty() {
        "none".to_string()
    } else {
        let mut entries = sanitized_model_fields.into_iter().collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
            .into_iter()
            .map(|(key, value)| format!("- {key}: {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let review_request = coordinator_review_request_text(&input, &model_metadata);

    let request = LanguageModelRequest {
        intent: Some(CompletionIntent::UserPrompt),
        temperature: Some(0.1),
        messages: vec![
            LanguageModelRequestMessage {
                role: Role::System,
                content: vec![
                    "You are Gear's coordinator review hook. Review whether the goal is actually satisfied after this iteration. Do not write code. Be conservative: if deterministic verification failed, do not mark the goal satisfied. Use ROUTE_HINT=review when Gear should schedule an independent review worker before declaring completion. Keep the response structured exactly as requested.".into(),
                ],
                cache: false,
                reasoning_details: None,
            },
            LanguageModelRequestMessage {
                role: Role::User,
                content: vec![review_request.into()],
                cache: false,
                reasoning_details: None,
            },
        ],
        ..Default::default()
    };

    let mut stream = match model.stream_completion_text(request, cx).await {
        Ok(stream) => stream.stream,
        Err(error) => {
            log::warn!("Gear coordinator review request failed: {error}");
            return None;
        }
    };
    let mut review = String::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(text) => {
                review.push_str(&text);
                if review.len() >= 4000 {
                    break;
                }
            }
            Err(error) => {
                log::warn!("Gear coordinator review stream failed: {error}");
                return None;
            }
        }
    }

    let (parsed_review, warnings) = parse_gear_coordinator_review_with_warnings(&review);
    if !warnings.is_empty() {
        let store = StateStore::new(workspace);
        if let Err(error) = store.write_artifact(
            &input.goal_id,
            &format!(
                "coordinator-review-iteration-{}-warnings.md",
                input.iteration
            ),
            &coordinator_review_warning_artifact(input.iteration, &warnings, &review),
        ) {
            log::warn!("Failed to write Gear coordinator review warning artifact: {error}");
        }
    }
    parsed_review
}

fn coordinator_review_request_text(input: &CoordinatorReviewInput, model_metadata: &str) -> String {
    format!(
        r#"Goal id: {goal_id}
Task id: {task_id}
Iteration: {iteration}/{max_iterations}
Budget: {budget_summary}

Original request:
{request}

Model metadata:
{model_metadata}

Worker:
- kind: {worker_kind}
- model: {worker_model}
- category: {worker_category}
- route_reason: {route_reason}
- route_resolution:
{route_resolution}
- attempt: {worker_attempt} of {worker_attempt_count}
- failure_kind: {worker_failure_kind}
- retry_reason: {worker_retry_reason}
- fallback_history:
{worker_fallback_summary}
- status: {worker_status}
- summary: {worker_summary}
- outcome: {worker_outcome_summary}
- commands_run:
{worker_commands_run}
- known_failures:
{worker_known_failures}
- outcome_path: {worker_outcome_path}

Verification passed: {verification_passed}
Verification:
{verification_summary}

Scope:
{scope_summary}

Diff:
{diff_summary}

No-progress signals:
{no_progress_signals}

Return exactly these fields:
GOAL_SATISFIED: yes|no|unknown
SUMMARY: one concise sentence
REPAIR_REQUEST: one concise instruction for the next worker, or none
ROUTE_HINT: quick|repair|deep|review|explore|librarian|visual|zed-native|custom|needs_user|none
STOP_REASON: complete|limited|blocked|needs_user|none
"#,
        goal_id = input.goal_id,
        task_id = input.task_id,
        iteration = input.iteration,
        max_iterations = input.max_iterations,
        budget_summary = input.budget_summary,
        request = input.request,
        model_metadata = model_metadata,
        worker_kind = input.worker_kind,
        worker_model = input.worker_model.as_deref().unwrap_or("none"),
        worker_category = input.worker_category,
        route_reason = input.route_reason,
        route_resolution = serde_json::to_string_pretty(&serde_json::json!({
            "category_resolution": &input.category_resolution,
            "category_resolution_result": &input.category_resolution_result,
        }))
        .unwrap_or_else(|_| "unavailable".to_string()),
        worker_attempt = input.worker_attempt,
        worker_attempt_count = input.worker_attempt_count,
        worker_failure_kind = input.worker_failure_kind.as_deref().unwrap_or("none"),
        worker_retry_reason = input.worker_retry_reason.as_deref().unwrap_or("none"),
        worker_fallback_summary = input.worker_fallback_summary,
        worker_status = input.worker_status,
        worker_summary = input.worker_summary,
        worker_outcome_summary = input.worker_outcome_summary,
        worker_commands_run = format_list_or_none(&input.worker_commands_run),
        worker_known_failures = format_list_or_none(&input.worker_known_failures),
        worker_outcome_path = input.worker_outcome_path.as_deref().unwrap_or("none"),
        verification_passed = input.verification_passed,
        verification_summary = input.verification_summary,
        scope_summary = input.scope_summary,
        diff_summary = input.diff_summary,
        no_progress_signals = format_list_or_none(&input.no_progress_signals),
    )
}

fn coordinator_review_warning_artifact(
    iteration: usize,
    warnings: &[String],
    raw_response: &str,
) -> String {
    let warnings = warnings
        .iter()
        .map(|warning| format!("- {warning}"))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"# Coordinator Review Parser Warnings

Iteration: `{iteration}`

## Warnings

{}

## Raw Response

```text
{}
```
"#,
        if warnings.is_empty() {
            "- none".to_string()
        } else {
            warnings
        },
        raw_response.trim()
    )
}

#[cfg(test)]
fn parse_gear_coordinator_review(review: &str) -> Option<CoordinatorReview> {
    parse_gear_coordinator_review_with_warnings(review).0
}

fn parse_gear_coordinator_review_with_warnings(
    review: &str,
) -> (Option<CoordinatorReview>, Vec<String>) {
    let raw_response = review.trim();
    if raw_response.is_empty() {
        return (None, vec!["Empty coordinator review response.".to_string()]);
    }

    let mut goal_satisfied = None;
    let mut summary = None;
    let mut repair_request = None;
    let mut route_hint = None;
    let mut stop_reason = None;
    let mut warnings = Vec::new();

    for line in raw_response.lines() {
        let trimmed_line = line.trim();
        if trimmed_line.is_empty() {
            continue;
        }
        let Some((key, value)) = trimmed_line.split_once(':') else {
            warnings.push(format!("Ignored malformed review line: {trimmed_line}"));
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "goal_satisfied" => {
                goal_satisfied = match value.to_ascii_lowercase().as_str() {
                    "yes" | "true" | "complete" => Some(true),
                    "no" | "false" | "incomplete" => Some(false),
                    other => {
                        warnings.push(format!("Unrecognized GOAL_SATISFIED value: {other}"));
                        None
                    }
                };
            }
            "summary" if !value.is_empty() => summary = Some(value.to_string()),
            "repair_request" if !value.is_empty() && !value.eq_ignore_ascii_case("none") => {
                repair_request = Some(value.to_string());
            }
            "route_hint" if !value.is_empty() && !value.eq_ignore_ascii_case("none") => {
                if WorkerCategory::parse(value).is_none() {
                    warnings.push(format!("Unrecognized ROUTE_HINT value: {value}"));
                }
                route_hint = Some(value.to_string());
            }
            "stop_reason" if !value.is_empty() && !value.eq_ignore_ascii_case("none") => {
                if !matches!(
                    value.to_ascii_lowercase().as_str(),
                    "complete" | "limited" | "blocked" | "needs_user"
                ) {
                    warnings.push(format!("Unrecognized STOP_REASON value: {value}"));
                }
                stop_reason = Some(value.to_string());
            }
            other => warnings.push(format!("Unrecognized coordinator review field: {other}")),
        }
    }

    if summary.is_none() {
        warnings.push("SUMMARY missing; using first line fallback.".to_string());
    }

    (
        Some(CoordinatorReview {
            goal_satisfied,
            summary: summary
                .unwrap_or_else(|| raw_response.lines().next().unwrap_or("").to_string()),
            repair_request,
            route_hint,
            stop_reason,
            raw_response: raw_response.to_string(),
        }),
        warnings,
    )
}

fn is_gear_executable_goal(request: &str) -> bool {
    let request = request.trim();
    if request.is_empty() {
        return false;
    }

    let normalized = request
        .trim_matches(|character: char| {
            character.is_whitespace()
                || character.is_ascii_punctuation()
                || matches!(
                    character,
                    '。' | '，'
                        | '、'
                        | '；'
                        | '：'
                        | '？'
                        | '！'
                        | '（'
                        | '）'
                        | '“'
                        | '”'
                        | '‘'
                        | '’'
                )
        })
        .to_lowercase();
    if normalized.is_empty() {
        return false;
    }

    const SMALL_TALK: &[&str] = &[
        "hi",
        "hello",
        "hey",
        "你好",
        "您好",
        "嗨",
        "哈喽",
        "在吗",
        "谢谢",
        "thanks",
        "thank you",
    ];
    if SMALL_TALK.iter().any(|phrase| normalized == *phrase) {
        return false;
    }

    const ACTION_WORDS: &[&str] = &[
        "add",
        "build",
        "change",
        "create",
        "debug",
        "fix",
        "implement",
        "refactor",
        "review",
        "test",
        "update",
        "生成",
        "创建",
        "实现",
        "修改",
        "修复",
        "调试",
        "重构",
        "审查",
        "检查",
        "测试",
        "更新",
        "继续",
    ];

    ACTION_WORDS
        .iter()
        .any(|action_word| normalized.contains(action_word))
}

fn gear_workspace_for_session(session: &Session, agent: &NativeAgent, cx: &App) -> Result<PathBuf> {
    if let Some(path) = session.work_dirs.as_ref().and_then(|work_dirs| {
        work_dirs
            .paths()
            .iter()
            .find(|path| !path.as_os_str().is_empty())
    }) {
        return Ok(path.clone());
    }

    let state = agent
        .projects
        .get(&session.project_id)
        .context("Gear project state not found")?;
    let Some(worktree) = state.project.read(cx).visible_worktrees(cx).next() else {
        return Err(anyhow!("Gear requires an open local worktree"));
    };
    Ok(worktree.read(cx).abs_path().to_path_buf())
}

fn gear_worker_config_from_env(cx: &App) -> WorkerConfig {
    let worker = trimmed_env_value("GEARBOX_GEAR_WORKER");
    let worker_kind = worker
        .as_deref()
        .and_then(WorkerKind::parse)
        .unwrap_or_default();
    let explicit_worker_command = trimmed_env_value("GEARBOX_GEAR_WORKER_COMMAND");
    let legacy_opencode_command = trimmed_env_value("GEARBOX_OPENCODE_COMMAND");
    let per_kind_worker_command = gear_worker_command_for_kind(worker_kind);
    let mut config = gear_worker_config_from_values(
        worker.as_deref(),
        explicit_worker_command
            .as_deref()
            .or(per_kind_worker_command.as_deref()),
        legacy_opencode_command.as_deref(),
        trimmed_env_value("GEARBOX_GEAR_WORKER_MODEL").as_deref(),
        gear_unavailable_worker_models_from_env(),
        gear_usize_from_env("GEARBOX_GEAR_PREMIUM_WORKER_BUDGET", 1),
        gear_parallel_limit_from_env("GEARBOX_GEAR_MAX_PARALLEL_WORKERS"),
        gear_parallel_limit_from_env("GEARBOX_GEAR_MAX_PARALLEL_PER_KEY"),
        gear_usize_from_env("GEARBOX_GEAR_STALE_TASK_TIMEOUT_SECS", 30),
    );
    config.worker_routes = gear_worker_routes_from_env(
        config.worker_kind,
        config.worker_command.as_deref(),
        config.worker_model.as_deref(),
    );
    if !config.worker_routes.is_empty() {
        config.require_worker = config
            .worker_routes
            .iter()
            .any(|route| route.worker_command.is_some());
    }
    gear_apply_provider_model_availability(&mut config, gear_available_provider_models(cx));
    config
}

fn gear_open_code_phase_worker_config(mut config: WorkerConfig) -> WorkerConfig {
    if config
        .worker_routes
        .iter()
        .any(|route| route.worker_kind == WorkerKind::OpencodeSession)
    {
        return config;
    }
    let command = gear_worker_command_for_kind(WorkerKind::OpencodeSession).or_else(|| {
        matches!(
            config.worker_kind,
            WorkerKind::Opencode | WorkerKind::OpencodeSession
        )
        .then(|| config.worker_command.clone())
        .flatten()
    });
    if let Some(command) = command {
        config.worker_routes.push(WorkerRoute {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(command),
            worker_model: None,
        });
        config.require_worker = true;
    }
    config
}

fn gear_codex_acp_phase_worker_config(mut config: WorkerConfig) -> WorkerConfig {
    let has_codex = config
        .worker_routes
        .iter()
        .any(|route| route.worker_kind == WorkerKind::Codex);
    let has_opencode_session = config
        .worker_routes
        .iter()
        .any(|route| route.worker_kind == WorkerKind::OpencodeSession);
    if has_codex && has_opencode_session {
        return config;
    }
    if !has_codex {
        let codex_command = gear_worker_command_for_kind(WorkerKind::Codex)
            .or_else(|| {
                matches!(config.worker_kind, WorkerKind::Codex)
                    .then(|| config.worker_command.clone())
                    .flatten()
            })
            .or_else(|| WorkerKind::Codex.default_command(config.worker_model.as_deref()));
        if let Some(command) = codex_command {
            config.worker_routes.push(WorkerRoute {
                worker_kind: WorkerKind::Codex,
                worker_command: Some(command),
                worker_model: config.worker_model.clone(),
            });
            config.require_worker = true;
        }
    }
    if !has_opencode_session {
        let opencode_command =
            gear_worker_command_for_kind(WorkerKind::OpencodeSession).or_else(|| {
                matches!(
                    config.worker_kind,
                    WorkerKind::Opencode | WorkerKind::OpencodeSession
                )
                .then(|| config.worker_command.clone())
                .flatten()
            });
        if let Some(command) = opencode_command {
            config.worker_routes.push(WorkerRoute {
                worker_kind: WorkerKind::OpencodeSession,
                worker_command: Some(command),
                worker_model: None,
            });
            config.require_worker = true;
        }
    }
    config
}

fn gear_worker_config_from_values(
    worker: Option<&str>,
    worker_command: Option<&str>,
    opencode_command: Option<&str>,
    worker_model: Option<&str>,
    unavailable_worker_models: Vec<String>,
    premium_worker_budget: usize,
    max_parallel_workers: usize,
    max_parallel_per_key: usize,
    stale_task_timeout_secs: usize,
) -> WorkerConfig {
    let worker_kind = match worker {
        Some(worker) => WorkerKind::parse(worker).unwrap_or_else(|| {
            log::warn!("Ignoring unknown GEARBOX_GEAR_WORKER value `{worker}`; using opencode");
            WorkerKind::default()
        }),
        None => WorkerKind::default(),
    };
    let worker_command = worker_command
        .or(opencode_command)
        .map(str::to_string)
        .filter(|command| !command.is_empty())
        .or_else(|| worker_kind.default_command(worker_model));
    let require_worker = worker_command.is_some();

    WorkerConfig {
        worker_kind,
        worker_command,
        worker_model: worker_model.map(ToString::to_string),
        worker_routes: Vec::new(),
        unavailable_worker_models,
        premium_worker_budget,
        max_parallel_workers: max_parallel_workers.max(1),
        max_parallel_per_key: max_parallel_per_key.max(1),
        stale_task_timeout_secs: stale_task_timeout_secs.max(1),
        skip_worker: false,
        require_worker,
        default_worker_for_small_tasks: WorkerKind::ZedAgent,
    }
}

fn gear_parallel_limit_from_env(env_name: &str) -> usize {
    trimmed_env_value(env_name)
        .and_then(|value| match value.parse::<usize>() {
            Ok(limit) => Some(limit.max(1)),
            Err(error) => {
                log::warn!("Ignoring invalid {env_name} value `{value}`: {error}");
                None
            }
        })
        .unwrap_or(1)
}

fn gear_worker_routes_from_env(
    default_worker_kind: WorkerKind,
    default_worker_command: Option<&str>,
    default_worker_model: Option<&str>,
) -> Vec<WorkerRoute> {
    if let Some(sequence) = trimmed_env_value("GEARBOX_GEAR_WORKER_SEQUENCE") {
        return gear_worker_routes_from_sequence(
            &sequence,
            default_worker_kind,
            default_worker_command,
            default_worker_model,
        );
    }

    if gear_opencode_free_fallbacks_enabled() {
        return gear_opencode_free_fallback_routes(gear_worker_command_for_kind(
            WorkerKind::Opencode,
        ));
    };

    Vec::new()
}

fn gear_worker_routes_from_sequence(
    sequence: &str,
    default_worker_kind: WorkerKind,
    default_worker_command: Option<&str>,
    default_worker_model: Option<&str>,
) -> Vec<WorkerRoute> {
    sequence
        .split(',')
        .filter_map(|worker| {
            let worker = worker.trim();
            if worker.is_empty() {
                return None;
            }
            let (worker, worker_model) = worker
                .split_once(':')
                .map(|(worker, worker_model)| {
                    (
                        worker.trim(),
                        Some(worker_model.trim().to_string()).filter(|model| !model.is_empty()),
                    )
                })
                .unwrap_or((worker, None));
            let Some(worker_kind) = WorkerKind::parse(worker) else {
                log::warn!("Ignoring unknown GEARBOX_GEAR_WORKER_SEQUENCE value `{worker}`");
                return None;
            };
            let worker_command = gear_worker_command_for_kind(worker_kind)
                .or_else(|| {
                    (worker_kind == default_worker_kind)
                        .then(|| default_worker_command.map(ToString::to_string))
                        .flatten()
                })
                .or_else(|| worker_kind.default_command(worker_model.as_deref()));
            let worker_model = worker_model.or_else(|| {
                (worker_kind == default_worker_kind)
                    .then(|| default_worker_model.map(ToString::to_string))
                    .flatten()
            });
            Some(WorkerRoute {
                worker_kind,
                worker_command,
                worker_model,
            })
        })
        .collect()
}

fn gear_opencode_free_fallbacks_enabled() -> bool {
    matches!(
        trimmed_env_value("GEARBOX_GEAR_OPENCODE_FREE_FALLBACKS").as_deref(),
        Some("1" | "true" | "yes")
    )
}

fn gear_opencode_free_fallback_routes(command: Option<String>) -> Vec<WorkerRoute> {
    let command = command.unwrap_or_else(|| {
        "if [ \"$GEARBOX_WORKER_RESUME\" = \"true\" ]; then opencode run --pure --format json --session \"$GEARBOX_WORKER_SESSION_ID\" --model \"$GEARBOX_WORKER_MODEL\" < \"$GEARBOX_WORKER_PROMPT\"; else opencode run --pure --format json --model \"$GEARBOX_WORKER_MODEL\" < \"$GEARBOX_WORKER_PROMPT\"; fi"
            .to_string()
    });
    [
        "opencode/hy3-free",
        "opencode/mimo-v2.5-free",
        "opencode/deepseek-v4-flash-free",
    ]
    .into_iter()
    .map(|worker_model| WorkerRoute {
        worker_kind: WorkerKind::OpencodeSession,
        worker_command: Some(command.clone()),
        worker_model: Some(worker_model.to_string()),
    })
    .collect()
}

fn gear_opencode_free_fallback_uses_command_backend(
    worker_kind: WorkerKind,
    worker_model: Option<&str>,
) -> bool {
    worker_kind == WorkerKind::OpencodeSession
        && matches!(
            worker_model.map(str::trim),
            Some(
                "opencode/hy3-free" | "opencode/mimo-v2.5-free" | "opencode/deepseek-v4-flash-free"
            )
        )
}

fn gear_unavailable_worker_models_from_env() -> Vec<String> {
    trimmed_env_value("GEARBOX_GEAR_UNAVAILABLE_WORKER_MODELS")
        .map(|models| {
            models
                .split([',', '\n'])
                .map(str::trim)
                .filter(|model| !model.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn gear_worker_command_for_kind(worker_kind: WorkerKind) -> Option<String> {
    let env_name = match worker_kind {
        WorkerKind::Opencode | WorkerKind::OpencodeSession => "GEARBOX_GEAR_OPENCODE_COMMAND",
        WorkerKind::Codex => "GEARBOX_GEAR_CODEX_COMMAND",
        WorkerKind::Claude => "GEARBOX_GEAR_CLAUDE_COMMAND",
        WorkerKind::ZedAgent => "GEARBOX_GEAR_ZED_AGENT_COMMAND",
        WorkerKind::Custom => "GEARBOX_GEAR_CUSTOM_COMMAND",
    };
    trimmed_env_value(env_name)
}

fn gear_available_provider_models(cx: &App) -> Vec<(String, String)> {
    LanguageModelRegistry::read_global(cx)
        .available_models(cx)
        .map(|model| (model.provider_id().0.to_string(), model.id().0.to_string()))
        .collect()
}

fn gear_apply_provider_model_availability(
    config: &mut WorkerConfig,
    available_models: impl IntoIterator<Item = (String, String)>,
) {
    let available_models = available_models
        .into_iter()
        .map(|(provider_id, model_id)| {
            (
                provider_id.trim().to_ascii_lowercase(),
                model_id.trim().to_ascii_lowercase(),
            )
        })
        .collect::<std::collections::HashSet<_>>();

    let mut unavailable_models = config.unavailable_worker_models.clone();
    gear_mark_unavailable_worker_model(
        &mut unavailable_models,
        config.worker_kind,
        config.worker_model.as_deref(),
        &available_models,
    );
    for route in &config.worker_routes {
        gear_mark_unavailable_worker_model(
            &mut unavailable_models,
            route.worker_kind,
            route.worker_model.as_deref(),
            &available_models,
        );
    }
    config.unavailable_worker_models = unavailable_models;
}

fn gear_mark_unavailable_worker_model(
    unavailable_models: &mut Vec<String>,
    worker_kind: WorkerKind,
    worker_model: Option<&str>,
    available_models: &std::collections::HashSet<(String, String)>,
) {
    let Some(provider_id) = worker_kind.provider_id_hint() else {
        return;
    };
    let Some(worker_model) = worker_model
        .map(str::trim)
        .filter(|model| !model.is_empty())
    else {
        return;
    };
    let qualified_model = format!("{provider_id}/{worker_model}");
    let normalized_qualified_model = (
        provider_id.to_ascii_lowercase(),
        worker_model.to_ascii_lowercase(),
    );
    if available_models.contains(&normalized_qualified_model) {
        return;
    }
    if unavailable_models
        .iter()
        .any(|entry| entry.eq_ignore_ascii_case(&qualified_model))
    {
        return;
    }
    unavailable_models.push(qualified_model);
}

fn trimmed_env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn gear_verification_commands_from_env() -> Vec<String> {
    std::env::var("GEARBOX_GEAR_VERIFY_COMMANDS")
        .ok()
        .map(|commands| {
            commands
                .lines()
                .map(str::trim)
                .filter(|command| !command.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn gear_max_iterations_from_env() -> usize {
    gear_usize_from_env("GEARBOX_GEAR_MAX_ITERATIONS", DEFAULT_MAX_ITERATIONS)
}

fn gear_max_plan_revisions_from_env() -> usize {
    gear_usize_from_env(
        "GEARBOX_GEAR_MAX_PLAN_REVISIONS",
        DEFAULT_MAX_PLAN_REVISIONS,
    )
}

fn gear_max_provider_unknown_streak_from_env() -> usize {
    gear_usize_from_env(
        "GEARBOX_GEAR_MAX_PROVIDER_UNKNOWN_STREAK",
        DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
    )
}

fn gear_max_files_changed_from_env() -> usize {
    gear_usize_from_env("GEARBOX_GEAR_MAX_FILES_CHANGED", 40)
}

fn gear_max_child_depth_from_env() -> usize {
    gear_usize_from_env("GEARBOX_GEAR_MAX_CHILD_DEPTH", usize::MAX)
}

fn gear_max_runtime_minutes_from_env() -> usize {
    gear_usize_from_env(
        "GEARBOX_GEAR_MAX_RUNTIME_MINUTES",
        DEFAULT_MAX_RUNTIME_MINUTES,
    )
}

fn gear_budget_from_env() -> Budget {
    Budget {
        max_calls_per_epoch: gear_usize_from_env("GEARBOX_GEAR_MAX_CALLS_PER_EPOCH", 32),
        max_worker_calls: gear_usize_from_env("GEARBOX_GEAR_MAX_WORKER_CALLS", 8),
        max_premium_worker_calls: gear_usize_from_env("GEARBOX_GEAR_MAX_PREMIUM_WORKER_CALLS", 8),
        max_tokens_per_call: gear_u64_from_env("GEARBOX_GEAR_MAX_TOKENS_PER_CALL", 128_000),
        max_tokens_per_epoch: gear_u64_from_env("GEARBOX_GEAR_MAX_TOKENS_PER_EPOCH", 4_096_000),
        max_cost_micros_per_epoch: gear_u64_from_env(
            "GEARBOX_GEAR_MAX_COST_MICROS_PER_EPOCH",
            u64::MAX,
        ),
        max_usage_unknown_calls: gear_usize_from_env("GEARBOX_GEAR_MAX_USAGE_UNKNOWN_CALLS", 32),
        max_runtime_minutes: gear_max_runtime_minutes_from_env(),
        ..Budget::default()
    }
}

fn gear_u64_from_env(name: &str, default_value: u64) -> u64 {
    let Some(value) = trimmed_env_value(name) else {
        return default_value;
    };
    match value.parse::<u64>() {
        Ok(value) if value > 0 => value,
        _ => {
            log::warn!("Ignoring invalid {name} value `{value}`; using {default_value}");
            default_value
        }
    }
}

fn gear_usize_from_env(name: &str, default_value: usize) -> usize {
    let Some(value) = trimmed_env_value(name) else {
        return default_value;
    };
    match value.parse::<usize>() {
        Ok(value) if value > 0 => value,
        _ => {
            log::warn!("Ignoring invalid {name} value `{value}`; using {default_value}");
            default_value
        }
    }
}

fn gear_event_status_markdown(event: &gearbox_agent::state::Event) -> String {
    let task = event
        .task_id
        .as_ref()
        .map(|task_id| format!(" `{task_id}`"))
        .unwrap_or_default();
    let path = event
        .data
        .get("path")
        .or_else(|| event.data.get("result_path"))
        .or_else(|| event.data.get("outcome_path"))
        .or_else(|| event.data.get("verification_path"))
        .or_else(|| event.data.get("final_report_path"))
        .or_else(|| event.data.get("task_record_path"))
        .and_then(|value| value.as_str())
        .map(|path| format!(" (`{path}`)"))
        .unwrap_or_default();

    if task.is_empty() {
        format!("Gear: {}{path}\n\n", event.message)
    } else {
        format!("Gear:{task} {}{path}\n\n", event.message)
    }
}

fn push_gear_task_manager_snapshot_if_changed(
    task_manager: &SharedTaskManager,
    last_snapshot: &mut Option<String>,
    acp_thread: &Entity<AcpThread>,
    thread: &Entity<Thread>,
    cx: &mut AsyncApp,
) {
    match gear_task_manager_snapshot_markdown(task_manager) {
        Ok(Some(snapshot)) if last_snapshot.as_ref() != Some(&snapshot) => {
            push_gear_assistant_markdown(acp_thread, thread, snapshot.clone(), cx);
            *last_snapshot = Some(snapshot);
        }
        Ok(_) => {}
        Err(error) => {
            log::warn!("failed to render Gear task manager snapshot: {error:#}");
        }
    }
}

fn gear_task_manager_snapshot_markdown(task_manager: &SharedTaskManager) -> Result<Option<String>> {
    let snapshot = task_manager
        .lock()
        .map_err(|_| anyhow!("task manager mutex poisoned"))?
        .snapshot()?;
    gear_task_manager_snapshot_to_markdown(&snapshot)
}

fn gear_task_manager_snapshot_to_markdown(
    snapshot: &TaskManagerSnapshot,
) -> Result<Option<String>> {
    if snapshot.tasks.is_empty()
        && snapshot.artifacts_root.is_none()
        && snapshot.current_output.is_none()
    {
        return Ok(None);
    }

    let mut message = format!(
        "Gear task manager: pending {}, running {}, completed {}, failed {}, cancelled {}, interrupted {}, lost {}, skipped {}\n\n",
        snapshot.counts.pending,
        snapshot.counts.running,
        snapshot.counts.completed,
        snapshot.counts.failed,
        snapshot.counts.cancelled,
        snapshot.counts.interrupted,
        snapshot.counts.lost,
        snapshot.counts.skipped,
    );
    if let Some(artifacts_root) = snapshot.artifacts_root.as_deref() {
        let artifacts = gear_goal_artifact_links(artifacts_root);
        if !artifacts.is_empty() {
            message.push_str(&format!("Goal artifacts:{}\n\n", artifacts));
        }
    }
    for record in snapshot.tasks.iter().take(6) {
        let artifacts = gear_task_artifact_links(record);
        let worker =
            gear_worker_snapshot_label(&record.worker_kind, record.worker_model.as_deref());
        let parent_task = record
            .parent_task_id
            .as_deref()
            .map(|parent_task_id| format!("; parent `{parent_task_id}`"))
            .unwrap_or_default();
        message.push_str(&format!(
            "- `{}` {} via `{}` / `{}`; attempts: {}; {}{}{}\n",
            record.task_id,
            gear_managed_task_status_label(&record.status),
            worker,
            record.worker_category,
            record.attempts.len(),
            record.summary,
            parent_task,
            artifacts
        ));
        for attempt in record.attempts.iter().rev().take(2).rev() {
            let worker =
                gear_worker_snapshot_label(&attempt.worker_kind, attempt.worker_model.as_deref());
            message.push_str(&format!(
                "  - attempt {} {} via `{}` / `{}`{}{}\n",
                attempt.attempt,
                gear_task_attempt_status_label(&attempt.status),
                worker,
                attempt.worker_category,
                gear_task_attempt_artifact_links(attempt),
                gear_task_attempt_error_text(attempt),
            ));
        }
    }
    if snapshot.tasks.len() > 6 {
        message.push_str(&format!("- ... {} more tasks\n", snapshot.tasks.len() - 6));
    }

    if let Some(output) = snapshot.current_output.as_deref() {
        let output = gear_truncate_text(output.trim(), 1200);
        if !output.is_empty() {
            message.push_str("\nCurrent worker output:\n\n```text\n");
            message.push_str(&output);
            message.push_str("\n```\n");
        }
    }
    message.push('\n');
    Ok(Some(message))
}

fn gear_goal_artifact_links(artifacts_root: &Path) -> String {
    let mut links = vec![format!("[Artifacts]({})", artifacts_root.display())];
    if let Some(path) = gear_latest_goal_artifact(artifacts_root, "goal-review-iteration-") {
        links.push(format!("[Goal Review]({})", path.display()));
    }
    if let Some(path) = gear_latest_goal_artifact(artifacts_root, "coordinator-review-iteration-") {
        links.push(format!("[Coordinator Review]({})", path.display()));
    }
    let final_report = artifacts_root.join("final-report.md");
    if final_report.is_file() {
        links.push(format!("[Final Report]({})", final_report.display()));
    }
    format!(" ({})", links.join(", "))
}

fn gear_latest_goal_artifact(artifacts_root: &Path, prefix: &str) -> Option<PathBuf> {
    let mut latest: Option<(usize, PathBuf)> = None;
    for entry in std_fs::read_dir(artifacts_root).ok()? {
        let entry = entry.ok()?;
        let path = entry.path();
        let name = path.file_name()?.to_str()?;
        let iteration = name
            .strip_prefix(prefix)?
            .strip_suffix(".md")?
            .parse::<usize>()
            .ok()?;
        match &mut latest {
            Some((current_iteration, _)) if *current_iteration >= iteration => {}
            _ => {
                latest = Some((iteration, path));
            }
        }
    }
    latest.map(|(_, path)| path)
}

fn gear_worker_snapshot_label(worker_kind: &str, worker_model: Option<&str>) -> String {
    worker_model
        .map(|worker_model| format!("{worker_kind}:{worker_model}"))
        .unwrap_or_else(|| worker_kind.to_string())
}

fn gear_managed_task_status_label(status: &ManagedTaskStatus) -> &'static str {
    match status {
        ManagedTaskStatus::Pending => "pending",
        ManagedTaskStatus::Running => "running",
        ManagedTaskStatus::Completed => "completed",
        ManagedTaskStatus::Failed => "failed",
        ManagedTaskStatus::Cancelled => "cancelled",
        ManagedTaskStatus::Interrupted => "interrupted",
        ManagedTaskStatus::Lost => "lost",
        ManagedTaskStatus::Skipped => "skipped",
    }
}

fn gear_task_attempt_status_label(
    status: &gearbox_agent::task_manager::TaskAttemptStatus,
) -> &'static str {
    match status {
        gearbox_agent::task_manager::TaskAttemptStatus::Pending => "pending",
        gearbox_agent::task_manager::TaskAttemptStatus::Running => "running",
        gearbox_agent::task_manager::TaskAttemptStatus::Completed => "completed",
        gearbox_agent::task_manager::TaskAttemptStatus::Failed => "failed",
        gearbox_agent::task_manager::TaskAttemptStatus::Cancelled => "cancelled",
        gearbox_agent::task_manager::TaskAttemptStatus::Interrupted => "interrupted",
        gearbox_agent::task_manager::TaskAttemptStatus::Lost => "lost",
        gearbox_agent::task_manager::TaskAttemptStatus::Skipped => "skipped",
    }
}

fn gear_task_artifact_links(record: &TaskSnapshot) -> String {
    let mut links = Vec::new();
    let artifact_dir = record
        .result_path
        .as_ref()
        .or(record.outcome_path.as_ref())
        .and_then(|path| path.parent());
    if let Some(artifact_dir) = artifact_dir {
        links.push(format!(
            "[packet]({})",
            artifact_dir.join("packet.json").display()
        ));
        links.push(format!(
            "[prompt]({})",
            artifact_dir.join("prompt.md").display()
        ));
        links.push(format!(
            "[transcript]({})",
            artifact_dir.join("transcript.jsonl").display()
        ));
    }
    if let Some(path) = record.result_path.as_ref() {
        links.push(format!("[result]({})", path.display()));
    }
    if let Some(path) = record.outcome_path.as_ref() {
        links.push(format!("[outcome]({})", path.display()));
    }
    if let Some(path) = record
        .attempts
        .last()
        .and_then(|attempt| attempt.route_transform_path.as_ref())
    {
        links.push(format!("[fallback]({})", path.display()));
    }
    if links.is_empty() {
        String::new()
    } else {
        format!(" ({})", links.join(", "))
    }
}

fn gear_task_attempt_artifact_links(attempt: &TaskAttemptSnapshot) -> String {
    let mut links = Vec::new();
    let artifact_dir = attempt
        .result_path
        .as_ref()
        .or(attempt.outcome_path.as_ref())
        .and_then(|path| path.parent());
    if let Some(artifact_dir) = artifact_dir {
        links.push(format!(
            "[packet]({})",
            artifact_dir.join("packet.json").display()
        ));
        links.push(format!(
            "[prompt]({})",
            artifact_dir.join("prompt.md").display()
        ));
        links.push(format!(
            "[transcript]({})",
            artifact_dir.join("transcript.jsonl").display()
        ));
    }
    if let Some(path) = attempt.result_path.as_ref() {
        links.push(format!("[result]({})", path.display()));
    }
    if let Some(path) = attempt.outcome_path.as_ref() {
        links.push(format!("[outcome]({})", path.display()));
    }
    if let Some(path) = attempt.route_transform_path.as_ref() {
        links.push(format!("[fallback]({})", path.display()));
    }
    if links.is_empty() {
        String::new()
    } else {
        format!(" ({})", links.join(", "))
    }
}

fn gear_task_attempt_error_text(attempt: &TaskAttemptSnapshot) -> String {
    let Some(error) = attempt.error.as_deref() else {
        return String::new();
    };
    let error = gear_truncate_text(error, 160).replace('\n', " ");
    format!("; error: {error}")
}

fn gear_truncate_text(text: &str, max_chars: usize) -> String {
    let mut truncated = String::new();
    for (index, character) in text.chars().enumerate() {
        if index >= max_chars {
            truncated.push_str("\n... truncated ...");
            return truncated;
        }
        truncated.push(character);
    }
    truncated
}

fn gear_response_markdown(
    outcome: &gearbox_agent::runtime::RunOutcome,
    final_report: &str,
) -> String {
    format!(
        "# Gear run complete\n\n- Goal: `{}`\n- Session: `{}`\n- Status: `{}`\n- Artifacts: `{}`\n- Events: `{}`\n- Final report: `{}`\n\n{}",
        outcome.goal_id,
        outcome.session_id,
        outcome.status.as_str(),
        outcome.artifacts_root.display(),
        outcome.events_path.display(),
        outcome.final_report_path.display(),
        final_report.trim(),
    )
}

struct Command<'a> {
    prompt_name: &'a str,
    arg_value: &'a str,
    /// MCP server prefix from `/<server>.<prompt>` syntax. Mutually
    /// exclusive with `skill_scope` — the two grammars use different
    /// delimiters (`.` for MCP, `:` for skill scopes) so they can't
    /// collide.
    explicit_server_id: Option<&'a str>,
    /// Skill scope qualifier from `/<scope>:<name>` syntax, where
    /// `<scope>` is either the literal `global` or a worktree root
    /// name. The `:` separator namespaces these against MCP server
    /// prefixes (which use `.`) so an MCP server literally named
    /// `global` or named after a worktree still parses unambiguously.
    skill_scope: Option<&'a str>,
}

impl<'a> Command<'a> {
    fn is_unqualified(&self, prompt_name: &str) -> bool {
        self.prompt_name == prompt_name
            && self.explicit_server_id.is_none()
            && self.skill_scope.is_none()
    }

    fn parse(prompt: &'a [acp::ContentBlock]) -> Option<Self> {
        let acp::ContentBlock::Text(text_content) = prompt.first()? else {
            return None;
        };
        let text = text_content.text.trim();
        let command = text.strip_prefix('/')?;
        let (command, arg_value) = command
            .split_once(char::is_whitespace)
            .unwrap_or((command, ""));

        // Skill scope qualifier: `/<scope>:<name>`. Checked before the
        // MCP `.` grammar because `:` and `.` are different delimiters
        // — the two namespaces can't collide. Skill names are
        // restricted to `[a-z0-9-]+` (no colons), so the LAST `:` is
        // always the scope/name boundary; using `rsplit_once` lets
        // scope labels (e.g. a worktree root name) themselves contain
        // colons without breaking the parse.
        //
        // An empty scope (`/:<name>`) is the qualified form for a
        // global skill — see `SkillSource::scope_prefix`. The name
        // must be non-empty for the colon to be meaningful.
        if let Some((scope, prompt_name)) = command.rsplit_once(':')
            && !prompt_name.is_empty()
        {
            return Some(Self {
                prompt_name,
                arg_value,
                explicit_server_id: None,
                skill_scope: Some(scope),
            });
        }

        if let Some((server_id, prompt_name)) = command.split_once('.') {
            Some(Self {
                prompt_name,
                arg_value,
                explicit_server_id: Some(server_id),
                skill_scope: None,
            })
        } else {
            Some(Self {
                prompt_name: command,
                arg_value,
                explicit_server_id: None,
                skill_scope: None,
            })
        }
    }
}

/// Strip a leading `/cmd` slash command from the start of a text block,
/// returning whatever text comes after it. Mirrors the parsing in
/// [`Command::parse`]: leading whitespace is ignored when locating the `/`,
/// then everything up to (and including) the first whitespace inside the
/// stripped text is dropped. The remainder is preserved verbatim — including
/// any embedded newlines — because users may format their continuation
/// intentionally.
///
/// If the input doesn't begin with `/`, it is returned unchanged so callers
/// degrade gracefully rather than silently mangling unrelated text.
fn strip_slash_command_prefix(text: &str) -> String {
    let trimmed_start = text.trim_start();
    let Some(rest) = trimmed_start.strip_prefix('/') else {
        return text.to_string();
    };
    rest.split_once(char::is_whitespace)
        .map(|(_, after)| after.to_string())
        .unwrap_or_default()
}

struct NativeAgentModelSelector {
    session_id: acp::SessionId,
    connection: NativeAgentConnection,
}

impl acp_thread::AgentModelSelector for NativeAgentModelSelector {
    fn list_models(&self, cx: &mut App) -> Task<Result<acp_thread::AgentModelList>> {
        log::debug!("NativeAgentConnection::list_models called");
        let list = self.connection.agent.read(cx).models.model_list.clone();
        Task::ready(if list.is_empty() {
            Err(anyhow::anyhow!("No models available"))
        } else {
            Ok(list)
        })
    }

    fn select_model(&self, model_id: AgentModelId, cx: &mut App) -> Task<Result<()>> {
        log::debug!(
            "Setting model for session {}: {}",
            self.session_id,
            model_id
        );
        let Some(thread) = self
            .connection
            .agent
            .read(cx)
            .sessions
            .get(&self.session_id)
            .map(|session| session.thread.clone())
        else {
            return Task::ready(Err(anyhow!("Session not found")));
        };

        let Some(model) = self
            .connection
            .agent
            .read(cx)
            .models
            .model_from_id(&model_id)
        else {
            return Task::ready(Err(anyhow!("Invalid model ID {}", model_id)));
        };

        let favorite = agent_settings::AgentSettings::get_global(cx)
            .favorite_models
            .iter()
            .find(|favorite| {
                favorite.provider.0 == model.provider_id().0.as_ref()
                    && favorite.model == model.id().0.as_ref()
            })
            .cloned();

        let LanguageModelSelection {
            enable_thinking,
            effort,
            speed,
            ..
        } = agent_settings::language_model_to_selection(&model, favorite.as_ref());

        thread.update(cx, |thread, cx| {
            thread.set_model(model.clone(), cx);
            thread.set_thinking_effort(effort.clone(), cx);
            thread.set_thinking_enabled(enable_thinking, cx);
            if let Some(speed) = speed {
                thread.set_speed(speed, cx);
            }
        });

        update_settings_file(
            self.connection.agent.read(cx).fs.clone(),
            cx,
            move |settings, cx| {
                let provider = model.provider_id().0.to_string();
                let model = model.id().0.to_string();
                let enable_thinking = thread.read(cx).thinking_enabled();
                let speed = thread.read(cx).speed();
                settings
                    .agent
                    .get_or_insert_default()
                    .set_model(LanguageModelSelection {
                        provider: provider.into(),
                        model,
                        enable_thinking,
                        effort,
                        speed,
                    });
            },
        );

        Task::ready(Ok(()))
    }

    fn selected_model(&self, cx: &mut App) -> Task<Result<acp_thread::AgentModelInfo>> {
        let Some(thread) = self
            .connection
            .agent
            .read(cx)
            .sessions
            .get(&self.session_id)
            .map(|session| session.thread.clone())
        else {
            return Task::ready(Err(anyhow!("Session not found")));
        };
        let Some(model) = thread.read(cx).model() else {
            return Task::ready(Err(anyhow!("Model not found")));
        };
        let Some(provider) = LanguageModelRegistry::read_global(cx).provider(&model.provider_id())
        else {
            return Task::ready(Err(anyhow!("Provider not found")));
        };
        Task::ready(Ok(LanguageModels::map_language_model_to_info(
            model, &provider,
        )))
    }

    fn favorite_model_ids(&self, cx: &mut App) -> HashSet<AgentModelId> {
        agent_settings::AgentSettings::get_global(cx)
            .favorite_model_ids()
            .into_iter()
            .map(AgentModelId::from)
            .collect()
    }

    fn toggle_favorite_model(&self, model_id: AgentModelId, should_be_favorite: bool, cx: &App) {
        let selection = model_id_to_selection(&model_id, cx);
        let fs = self.connection.agent.read(cx).fs.clone();
        update_settings_file(fs, cx, move |settings, _| {
            let agent = settings.agent.get_or_insert_default();
            if should_be_favorite {
                agent.add_favorite_model(selection.clone());
            } else {
                agent.remove_favorite_model(&selection);
            }
        });
    }

    fn watch(&self, cx: &mut App) -> Option<watch::Receiver<()>> {
        Some(self.connection.agent.read(cx).models.watch())
    }

    fn should_render_footer(&self) -> bool {
        true
    }
}

fn model_id_to_selection(model_id: &AgentModelId, cx: &App) -> LanguageModelSelection {
    let id = model_id.as_ref();
    let (provider, model) = id.split_once('/').unwrap_or(("", id));

    let provider_id = LanguageModelProviderId(provider.to_string().into());
    let model_id = LanguageModelId(model.to_string().into());
    let resolved = LanguageModelRegistry::global(cx)
        .read(cx)
        .provider(&provider_id)
        .and_then(|provider| {
            provider
                .provided_models(cx)
                .into_iter()
                .find(|model| model.id() == model_id)
        });

    let Some(resolved) = resolved else {
        return LanguageModelSelection {
            provider: provider.to_owned().into(),
            model: model.to_owned(),
            enable_thinking: false,
            effort: None,
            speed: None,
        };
    };

    let current_user_selection = agent_settings::AgentSettings::get_global(cx)
        .default_model
        .as_ref()
        .filter(|selection| {
            selection.provider.0 == resolved.provider_id().0.as_ref()
                && selection.model == resolved.id().0.as_ref()
        })
        .cloned();

    agent_settings::language_model_to_selection(&resolved, current_user_selection.as_ref())
}

pub static ZED_AGENT_ID: LazyLock<AgentId> = LazyLock::new(|| AgentId::new("Zed Agent"));
pub static GEAR_AGENT_ID: LazyLock<AgentId> = LazyLock::new(|| AgentId::new("Gear"));

impl acp_thread::AgentConnection for NativeAgentConnection {
    fn agent_id(&self) -> AgentId {
        self.agent_id.clone()
    }

    fn telemetry_id(&self) -> SharedString {
        self.telemetry_id.clone()
    }

    fn new_session(
        self: Rc<Self>,
        project: Entity<Project>,
        work_dirs: PathList,
        cx: &mut App,
    ) -> Task<Result<Entity<acp_thread::AcpThread>>> {
        log::debug!("Creating new thread for project at: {work_dirs:?}");
        Task::ready(Ok(self.agent.update(cx, |agent, cx| {
            agent.new_session(
                project,
                work_dirs,
                self.agent_id.clone(),
                self.telemetry_id.clone(),
                cx,
            )
        })))
    }

    fn supports_load_session(&self) -> bool {
        true
    }

    fn load_session(
        self: Rc<Self>,
        session_id: acp::SessionId,
        project: Entity<Project>,
        work_dirs: PathList,
        _title: Option<SharedString>,
        cx: &mut App,
    ) -> Task<Result<Entity<acp_thread::AcpThread>>> {
        self.agent.update(cx, |agent, cx| {
            agent.open_thread_with_identity(
                session_id,
                project,
                Some(work_dirs),
                self.agent_id.clone(),
                self.telemetry_id.clone(),
                cx,
            )
        })
    }

    fn supports_close_session(&self) -> bool {
        true
    }

    fn close_session(
        self: Rc<Self>,
        session_id: &acp::SessionId,
        cx: &mut App,
    ) -> Task<Result<()>> {
        self.agent
            .update(cx, |agent, cx| agent.close_session(session_id, cx))
    }

    fn auth_methods(&self) -> &[acp::AuthMethod] {
        &[] // No auth for in-process
    }

    fn authenticate(&self, _method: acp::AuthMethodId, _cx: &mut App) -> Task<Result<()>> {
        Task::ready(Ok(()))
    }

    fn model_selector(&self, session_id: &acp::SessionId) -> Option<Rc<dyn AgentModelSelector>> {
        Some(Rc::new(NativeAgentModelSelector {
            session_id: session_id.clone(),
            connection: self.clone(),
        }) as Rc<dyn AgentModelSelector>)
    }

    fn client_user_message_ids(
        &self,
        _cx: &App,
    ) -> Option<Rc<dyn acp_thread::AgentSessionClientUserMessageIds>> {
        let prompt: Rc<dyn acp_thread::AgentSessionClientUserMessageIds> = Rc::new(self.clone());
        Some(prompt)
    }

    fn prompt(
        &self,
        params: acp::PromptRequest,
        cx: &mut App,
    ) -> Task<Result<acp::PromptResponse>> {
        acp_thread::AgentSessionClientUserMessageIds::prompt(
            self,
            acp_thread::AgentSessionClientUserMessageIds::new_id(self),
            params,
            cx,
        )
    }

    fn retry(
        &self,
        session_id: &acp::SessionId,
        _cx: &App,
    ) -> Option<Rc<dyn acp_thread::AgentSessionRetry>> {
        Some(Rc::new(NativeAgentSessionRetry {
            connection: self.clone(),
            session_id: session_id.clone(),
        }) as _)
    }

    fn cancel(&self, session_id: &acp::SessionId, cx: &mut App) {
        log::info!("Cancelling on session: {}", session_id);
        self.agent.update(cx, |agent, cx| {
            if let Some(session) = agent.sessions.get(session_id) {
                if let Some(task_manager_control) = session.gear_task_manager_control.as_ref()
                    && let Some(task_id) = task_manager_control.current_task_id().ok().flatten()
                    && let Some(task_manager) = session.gear_task_manager.as_ref()
                    && let Err(error) = task_manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("gear task manager mutex poisoned"))
                        .and_then(|mut task_manager| task_manager.cancel_task(&task_id))
                {
                    log::warn!("failed to cancel current Gear task tree: {error:#}");
                }
                if let Some(cancellation_token) = session.gear_cancellation_token.as_ref() {
                    cancellation_token.cancel();
                }
                session
                    .thread
                    .update(cx, |thread, cx| thread.cancel(cx))
                    .detach();
            }
        });
    }

    fn truncate(
        &self,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Option<Rc<dyn acp_thread::AgentSessionTruncate>> {
        self.agent.read_with(cx, |agent, _cx| {
            agent.sessions.get(session_id).map(|session| {
                Rc::new(NativeAgentSessionTruncate {
                    thread: session.thread.clone(),
                    acp_thread: session.acp_thread.downgrade(),
                }) as _
            })
        })
    }

    fn set_title(
        &self,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Option<Rc<dyn acp_thread::AgentSessionSetTitle>> {
        self.agent.read_with(cx, |agent, _cx| {
            agent
                .sessions
                .get(session_id)
                .filter(|s| !s.thread.read(cx).is_subagent())
                .map(|session| {
                    Rc::new(NativeAgentSessionSetTitle {
                        thread: session.thread.clone(),
                    }) as _
                })
        })
    }

    fn session_list(&self, cx: &mut App) -> Option<Rc<dyn AgentSessionList>> {
        let thread_store = self.agent.read(cx).thread_store.clone();
        Some(Rc::new(NativeAgentSessionList::new(thread_store, cx)) as _)
    }

    fn telemetry(&self) -> Option<Rc<dyn acp_thread::AgentTelemetry>> {
        Some(Rc::new(self.clone()) as Rc<dyn acp_thread::AgentTelemetry>)
    }

    fn into_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

impl acp_thread::AgentSessionClientUserMessageIds for NativeAgentConnection {
    fn prompt(
        &self,
        client_user_message_id: acp_thread::ClientUserMessageId,
        params: acp::PromptRequest,
        cx: &mut App,
    ) -> Task<Result<acp::PromptResponse>> {
        let session_id = params.session_id.clone();
        log::info!("Received prompt request for session: {}", session_id);
        log::debug!("Prompt blocks count: {}", params.prompt.len());

        if self.is_gear() {
            return self.send_gear_prompt(client_user_message_id, params, cx);
        }

        let Some(project_state) = self.agent.read(cx).session_project_state(&session_id) else {
            log::error!("Session not found in prompt: {}", session_id);
            if self.agent.read(cx).sessions.contains_key(&session_id) {
                log::error!(
                    "Session found in sessions map, but not in project state: {}",
                    session_id
                );
            }
            return Task::ready(Err(anyhow::anyhow!("Session not found")));
        };

        if let Some(parsed_command) = Command::parse(&params.prompt) {
            if parsed_command.is_unqualified(COMPACT_COMMAND_NAME) {
                return self.agent.update(cx, |agent, cx| {
                    agent.send_compact_command(client_user_message_id, session_id, cx)
                });
            }

            // Skill scope qualifiers (`/:<name>` and
            // `/<worktree>:<name>`) use a colon separator that can't
            // collide with MCP's `/<server>.<name>` grammar. The popup
            // inserts a qualified form for every skill so picking the
            // global row unambiguously runs the global skill even when
            // a same-named project-local one exists.
            if let Some(scope) = parsed_command.skill_scope
                && let Some(skill) = project_state.skills.iter().find(|skill| {
                    skill.name == parsed_command.prompt_name && skill.source.matches_scope(scope)
                })
            {
                let skill = skill.clone();
                return self.agent.update(cx, |agent, cx| {
                    agent.send_skill_invocation(
                        client_user_message_id,
                        session_id.clone(),
                        skill,
                        params.prompt,
                        cx,
                    )
                });
            }

            // MCP prompts and skills both register slash commands. MCP
            // prompts are checked first — if a user has both an MCP prompt
            // and a skill with the same name, the MCP prompt wins (matching
            // the order they appear in the catalog).
            let registry = project_state.context_server_registry.read(cx);

            let explicit_server_id = parsed_command
                .explicit_server_id
                .map(|server_id| ContextServerId(server_id.into()));

            if let Some(prompt) =
                registry.find_prompt(explicit_server_id.as_ref(), parsed_command.prompt_name)
            {
                let arguments = if !parsed_command.arg_value.is_empty()
                    && let Some(arg_name) = prompt
                        .prompt
                        .arguments
                        .as_ref()
                        .and_then(|args| args.first())
                        .map(|arg| arg.name.clone())
                {
                    HashMap::from_iter([(arg_name, parsed_command.arg_value.to_string())])
                } else {
                    Default::default()
                };

                let prompt_name = prompt.prompt.name.clone();
                let server_id = prompt.server_id.clone();

                return self.agent.update(cx, |agent, cx| {
                    agent.send_mcp_prompt(
                        client_user_message_id,
                        session_id.clone(),
                        prompt_name,
                        server_id,
                        arguments,
                        params.prompt,
                        cx,
                    )
                });
            }

            // Unqualified skill match (`/skill-name` with no scope
            // prefix and no MCP server prefix). Slash commands work
            // for *all* skills regardless of `disable_model_invocation`
            // — that flag only hides the skill from the model's catalog.
            // The user explicitly typed the name, so they get to invoke
            // it.
            //
            // Inlined rather than calling `apply_skill_overrides` so
            // we don't clone the entire skill list on every prompt
            // (including prompts like `/help` that aren't skills at
            // all). The resolution rule matches the override-applied
            // view: among skills with the matching name, pick the one
            // with the highest source precedence, so the slash command
            // picks the same entry the model sees in its catalog.
            // Ties (e.g. two project-local skills from different
            // worktrees) resolve to the first in iteration order to
            // match `apply_skill_overrides`.
            if parsed_command.explicit_server_id.is_none()
                && parsed_command.skill_scope.is_none()
                && !project_state.skills.is_empty()
            {
                let prompt_name = parsed_command.prompt_name;
                let resolved = project_state
                    .skills
                    .iter()
                    .filter(|skill| skill.name == prompt_name)
                    .reduce(|best, candidate| {
                        if candidate.source.precedence() > best.source.precedence() {
                            candidate
                        } else {
                            best
                        }
                    });
                if let Some(skill) = resolved {
                    let skill = skill.clone();
                    return self.agent.update(cx, |agent, cx| {
                        agent.send_skill_invocation(
                            client_user_message_id,
                            session_id.clone(),
                            skill,
                            params.prompt,
                            cx,
                        )
                    });
                }
            }
        };

        let path_style = project_state.project.read(cx).path_style(cx);

        self.run_turn(session_id, cx, move |thread, cx| {
            let content: Vec<UserMessageContent> = params
                .prompt
                .into_iter()
                .map(|block| UserMessageContent::from_content_block(block, path_style))
                .collect::<Vec<_>>();
            log::debug!("Converted prompt to message: {} chars", content.len());
            log::debug!("Client user message id: {:?}", client_user_message_id);
            log::debug!("Message content: {:?}", content);

            thread.update(cx, |thread, cx| {
                thread.send(client_user_message_id, content, cx)
            })
        })
    }
}

impl acp_thread::AgentTelemetry for NativeAgentConnection {
    fn thread_data(
        &self,
        session_id: &acp::SessionId,
        cx: &mut App,
    ) -> Task<Result<serde_json::Value>> {
        let Some(session) = self.agent.read(cx).sessions.get(session_id) else {
            return Task::ready(Err(anyhow!("Session not found")));
        };

        let task = session.thread.read(cx).to_db(cx);
        cx.background_spawn(async move {
            serde_json::to_value(task.await).context("Failed to serialize thread")
        })
    }
}

pub struct NativeAgentSessionList {
    thread_store: Entity<ThreadStore>,
    updates_tx: async_channel::Sender<acp_thread::SessionListUpdate>,
    updates_rx: async_channel::Receiver<acp_thread::SessionListUpdate>,
    _subscription: Subscription,
}

impl NativeAgentSessionList {
    fn new(thread_store: Entity<ThreadStore>, cx: &mut App) -> Self {
        let (tx, rx) = async_channel::unbounded();
        let this_tx = tx.clone();
        let subscription = cx.observe(&thread_store, move |_, _| {
            this_tx
                .try_send(acp_thread::SessionListUpdate::Refresh)
                .ok();
        });
        Self {
            thread_store,
            updates_tx: tx,
            updates_rx: rx,
            _subscription: subscription,
        }
    }

    pub fn thread_store(&self) -> &Entity<ThreadStore> {
        &self.thread_store
    }
}

impl AgentSessionList for NativeAgentSessionList {
    fn list_sessions(
        &self,
        _request: AgentSessionListRequest,
        cx: &mut App,
    ) -> Task<Result<AgentSessionListResponse>> {
        let sessions = self
            .thread_store
            .read(cx)
            .entries()
            .map(|entry| AgentSessionInfo::from(&entry))
            .collect();
        Task::ready(Ok(AgentSessionListResponse::new(sessions)))
    }

    fn supports_delete(&self) -> bool {
        true
    }

    fn delete_session(&self, session_id: &acp::SessionId, cx: &mut App) -> Task<Result<()>> {
        self.thread_store
            .update(cx, |store, cx| store.delete_thread(session_id.clone(), cx))
    }

    fn delete_sessions(&self, cx: &mut App) -> Task<Result<()>> {
        self.thread_store
            .update(cx, |store, cx| store.delete_threads(cx))
    }

    fn watch(
        &self,
        _cx: &mut App,
    ) -> Option<async_channel::Receiver<acp_thread::SessionListUpdate>> {
        Some(self.updates_rx.clone())
    }

    fn notify_refresh(&self) {
        self.updates_tx
            .try_send(acp_thread::SessionListUpdate::Refresh)
            .ok();
    }

    fn into_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

struct NativeAgentSessionTruncate {
    thread: Entity<Thread>,
    acp_thread: WeakEntity<AcpThread>,
}

impl acp_thread::AgentSessionTruncate for NativeAgentSessionTruncate {
    fn run(
        &self,
        client_user_message_id: acp_thread::ClientUserMessageId,
        cx: &mut App,
    ) -> Task<Result<()>> {
        match self.thread.update(cx, |thread, cx| {
            thread.truncate(client_user_message_id.clone(), cx)?;
            Ok(thread.latest_token_usage())
        }) {
            Ok(usage) => {
                self.acp_thread
                    .update(cx, |thread, cx| {
                        thread.update_token_usage(usage, cx);
                    })
                    .ok();
                Task::ready(Ok(()))
            }
            Err(error) => Task::ready(Err(error)),
        }
    }
}

struct NativeAgentSessionRetry {
    connection: NativeAgentConnection,
    session_id: acp::SessionId,
}

impl acp_thread::AgentSessionRetry for NativeAgentSessionRetry {
    fn run(&self, cx: &mut App) -> Task<Result<acp::PromptResponse>> {
        self.connection
            .run_turn(self.session_id.clone(), cx, |thread, cx| {
                thread.update(cx, |thread, cx| thread.resume(cx))
            })
    }
}

struct NativeAgentSessionSetTitle {
    thread: Entity<Thread>,
}

impl acp_thread::AgentSessionSetTitle for NativeAgentSessionSetTitle {
    fn run(&self, title: SharedString, cx: &mut App) -> Task<Result<()>> {
        self.thread
            .update(cx, |thread, cx| thread.set_title(title, cx));
        Task::ready(Ok(()))
    }
}

pub struct NativeThreadEnvironment {
    agent: WeakEntity<NativeAgent>,
    thread: WeakEntity<Thread>,
    acp_thread: WeakEntity<AcpThread>,
}

struct GearZedWorkerBackend {
    request_tx: async_channel::Sender<GearZedWorkerDispatch>,
    acp_request_tx: Option<async_channel::Sender<GearAcpBrokerDispatch>>,
}

#[derive(Default)]
struct GearZedWorkerState {
    session_id: Option<acp::SessionId>,
    result: Option<std::result::Result<WorkerResult, String>>,
    last_output: Option<String>,
    pending_interactions: VecDeque<GearZedInteraction>,
    interaction_count: usize,
}

struct GearZedWorkerSessionHandle {
    task_id: String,
    request_tx: async_channel::Sender<GearZedWorkerDispatch>,
    state: Arc<(Mutex<GearZedWorkerState>, Condvar)>,
    cancellation_token: CancellationToken,
}

struct GearZedWorkerJob {
    store: gearbox_agent::state::StateStore,
    task_id: String,
    prompt: String,
    packet_path: PathBuf,
    prompt_path: PathBuf,
    worker_model: Option<String>,
    cancellation_token: CancellationToken,
    state: Arc<(Mutex<GearZedWorkerState>, Condvar)>,
}

enum GearZedWorkerDispatch {
    Run(GearZedWorkerJob),
    Cancel { task_id: String },
    SetEndTurnAtBoundary { task_id: String, enabled: bool },
}

#[derive(Clone)]
struct GearZedInteraction {
    kind: GearZedInteractionKind,
    prompt: String,
}

#[derive(Clone, Copy)]
enum GearZedInteractionKind {
    FollowUp,
    Steer,
}

impl GearZedInteractionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::FollowUp => "follow-up",
            Self::Steer => "steer",
        }
    }
}

impl GearZedWorkerBackend {
    fn new(request_tx: async_channel::Sender<GearZedWorkerDispatch>) -> Self {
        Self {
            request_tx,
            acp_request_tx: None,
        }
    }

    fn with_acp_backend(
        mut self,
        request_tx: async_channel::Sender<GearAcpBrokerDispatch>,
    ) -> Self {
        self.acp_request_tx = Some(request_tx);
        self
    }
}

fn validate_native_worker_model_id(model: Option<&str>) -> Result<Option<String>> {
    let Some(model) = model.map(str::trim).filter(|model| !model.is_empty()) else {
        return Ok(None);
    };
    let Some((provider_id, model_id)) = model.split_once('/') else {
        anyhow::bail!(
            "native Gear worker model `{model}` must use the fully-qualified `provider/model` id"
        );
    };
    if provider_id.trim().is_empty() || model_id.trim().is_empty() {
        anyhow::bail!(
            "native Gear worker model `{model}` must include non-empty provider and model ids"
        );
    }
    Ok(Some(format!("{}/{}", provider_id.trim(), model_id.trim())))
}

impl NativeWorkerBackend for GearZedWorkerBackend {
    fn start_zed_agent(
        &self,
        request: WorkerStartRequest<'_>,
    ) -> Result<Arc<dyn WorkerSessionHandle>> {
        let route = request
            .config
            .selected_route_for_hint(request.route_attempt, request.route_hint);
        let (category_resolution, category_resolution_result) = category_resolution_for_route(
            request.config,
            request.route_attempt,
            request.route_hint,
            &route,
        );
        let worker_model = validate_native_worker_model_id(route.worker_model)?;
        let plan_task = request.task.inputs.plan_task.as_ref();
        let current_step_id = plan_task.and_then(|plan_task| {
            plan_task
                .execution_steps_or_legacy()
                .first()
                .map(|step| step.step_id.clone())
        });
        let packet_goal = plan_task
            .map(|plan_task| plan_task.worker_goal(request.goal))
            .unwrap_or_else(|| request.goal.to_string());
        let constraints = plan_task
            .map(gearbox_agent::plan_graph::PlanTaskContract::worker_constraints)
            .unwrap_or_else(|| {
                vec![
                    "Stay inside the allowed paths when they are provided.".to_string(),
                    "Prefer the package manager already used by the project.".to_string(),
                    "Read the provided spec and plan artifacts before changing code.".to_string(),
                    "Leave runnable local instructions in the final output.".to_string(),
                ]
            });
        let required_outputs = plan_task
            .map(gearbox_agent::plan_graph::PlanTaskContract::worker_required_outputs)
            .unwrap_or_else(|| {
                vec![
                    "summary".to_string(),
                    "changed_files".to_string(),
                    "commands_run".to_string(),
                    "known_failures".to_string(),
                    "next_steps".to_string(),
                ]
            });
        let planned_verification = plan_task
            .map(gearbox_agent::plan_graph::PlanTaskContract::worker_verification_commands)
            .filter(|commands| !commands.is_empty())
            .unwrap_or_else(|| request.verification_commands.to_vec());
        let stop_conditions = plan_task
            .map(gearbox_agent::plan_graph::PlanTaskContract::worker_stop_conditions)
            .unwrap_or_else(|| {
                vec![
                    "Requires a paid external service.".to_string(),
                    "Requires a user-provided API key.".to_string(),
                    "The same verification fails twice.".to_string(),
                ]
            });
        let (injected_rules, rules_injection_path) =
            discover_workspace_rules(request.store, request.workspace, request.task)?;
        let (injected_skills, skills_injection_path) =
            discover_workspace_skills(request.store, request.workspace, request.task)?;
        let packet = WorkerPacket {
            task_id: request.task.id.clone(),
            worker: route.worker_kind.as_str().to_string(),
            current_step_id,
            worker_model: worker_model.clone(),
            variant: route.variant.clone(),
            variant_applied: route.variant.clone(),
            prompt_append: route.prompt_append.clone(),
            injected_rules,
            rules_injection_path,
            injected_skills,
            skills_injection_path,
            tools: route.tools.clone(),
            category_resolution,
            category_resolution_result,
            goal: packet_goal,
            coordinator_model: request.coordinator_model.cloned(),
            coordinator_brief: request.coordinator_brief.map(ToString::to_string),
            scope: request.task.scope.clone(),
            inputs: request.task.inputs.clone(),
            constraints,
            required_outputs,
            verification: VerificationContract {
                preferred_commands: planned_verification,
                must_not_skip: vec!["typecheck".to_string()],
            },
            stop_conditions,
            prompt_manifest_path: None,
            prompt_reconcile_path: None,
            prompt_capsule_path: None,
        };
        let packet_json =
            serde_json::to_string_pretty(&packet).context("failed to serialize worker packet")?;
        let packet_path = request.store.write_worker_file(
            &request.task.id,
            "packet.json",
            &format!("{packet_json}\n"),
        )?;
        let prompt = worker_prompt(&packet)?;
        let prompt_path =
            request
                .store
                .write_worker_file(&request.task.id, "prompt.md", &prompt)?;

        let cancellation_token = request
            .cancellation_token
            .clone()
            .unwrap_or_else(CancellationToken::new);
        let state = Arc::new((Mutex::new(GearZedWorkerState::default()), Condvar::new()));
        self.request_tx
            .send_blocking(GearZedWorkerDispatch::Run(GearZedWorkerJob {
                store: request.store.clone(),
                task_id: request.task.id.clone(),
                prompt,
                packet_path: packet_path.clone(),
                prompt_path: prompt_path.clone(),
                worker_model: worker_model.clone(),
                cancellation_token: cancellation_token.clone(),
                state: state.clone(),
            }))
            .context("failed to queue native zed worker job")?;

        Ok(Arc::new(GearZedWorkerSessionHandle {
            task_id: request.task.id.clone(),
            request_tx: self.request_tx.clone(),
            state,
            cancellation_token,
        }))
    }

    fn start_acp_worker(
        &self,
        worker_kind: WorkerKind,
        request: WorkerStartRequest<'_>,
    ) -> Result<Option<Arc<dyn WorkerSessionHandle>>> {
        let Some(request_tx) = self.acp_request_tx.as_ref() else {
            return Ok(None);
        };
        if !matches!(
            worker_kind,
            WorkerKind::Opencode
                | WorkerKind::OpencodeSession
                | WorkerKind::Codex
                | WorkerKind::Claude
        ) {
            return Ok(None);
        }
        let route = request
            .config
            .selected_route_for_hint(request.route_attempt, request.route_hint);
        if gear_opencode_free_fallback_uses_command_backend(worker_kind, route.worker_model) {
            return Ok(None);
        }
        let backend = GearAcpBrokerBackend::new(request_tx.clone());
        backend.start_zed_agent(request).map(Some)
    }

    fn native_broker_capabilities(&self, worker_kind: WorkerKind) -> Option<Vec<BrokerCapability>> {
        self.acp_request_tx.as_ref().and_then(|_| {
            matches!(
                worker_kind,
                WorkerKind::Opencode
                    | WorkerKind::OpencodeSession
                    | WorkerKind::Codex
                    | WorkerKind::Claude
            )
            .then(|| {
                gearbox_agent::worker_broker::native_acp_broker_capabilities_for_kind(
                    WorkerKind::OpencodeSession,
                )
            })
        })
    }
}

impl WorkerSessionHandle for GearZedWorkerSessionHandle {
    fn session_id(&self) -> Option<String> {
        self.state
            .0
            .lock()
            .ok()
            .and_then(|state| state.session_id.as_ref().map(ToString::to_string))
    }

    fn send_follow_up(&self, prompt: String) -> Result<()> {
        self.enqueue_interaction(GearZedInteractionKind::FollowUp, prompt)
    }

    fn steer(&self, prompt: String) -> Result<()> {
        self.enqueue_interaction(GearZedInteractionKind::Steer, prompt)?;
        self.request_tx
            .send_blocking(GearZedWorkerDispatch::SetEndTurnAtBoundary {
                task_id: self.task_id.clone(),
                enabled: true,
            })
            .ok();
        Ok(())
    }

    fn interrupt(&self) -> Result<()> {
        self.cancel()
    }

    fn cancel(&self) -> Result<()> {
        self.cancellation_token.cancel();
        self.request_tx
            .send_blocking(GearZedWorkerDispatch::Cancel {
                task_id: self.task_id.clone(),
            })
            .ok();
        Ok(())
    }

    fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
        worker_outcome_from_result(&self.wait_for_result()?)
    }

    fn wait_for_result(&self) -> Result<WorkerResult> {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().expect("zed worker state poisoned");
        loop {
            if let Some(result) = state.result.clone() {
                return result.map_err(anyhow::Error::msg);
            }
            state = wake.wait(state).expect("zed worker state poisoned");
        }
    }

    fn last_output(&self) -> Option<String> {
        self.state
            .0
            .lock()
            .ok()
            .and_then(|state| state.last_output.clone())
    }
}

impl GearZedWorkerSessionHandle {
    fn enqueue_interaction(&self, kind: GearZedInteractionKind, prompt: String) -> Result<()> {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().expect("zed worker state poisoned");
        state.result = None;
        state
            .pending_interactions
            .push_back(GearZedInteraction { kind, prompt });
        Ok(())
    }
}

fn spawn_gear_zed_worker_dispatcher(
    agent: WeakEntity<NativeAgent>,
    parent_session_id: acp::SessionId,
    native_worker_rx: async_channel::Receiver<GearZedWorkerDispatch>,
    running_native_zed_sessions: Arc<Mutex<HashMap<String, acp::SessionId>>>,
    #[cfg(test)] lifecycle_events: Option<Arc<Mutex<Vec<String>>>>,
    cx: &mut App,
) {
    cx.spawn(async move |cx| {
        #[cfg(test)]
        if let Some(events) = &lifecycle_events {
            if let Ok(mut guard) = events.lock() {
                guard.push("dispatcher:start".to_string());
            }
        }
        while let Ok(dispatch) = native_worker_rx.recv().await {
            #[cfg(test)]
            if let Some(events) = &lifecycle_events {
                if let Ok(mut guard) = events.lock() {
                    guard.push("dispatcher:receive".to_string());
                }
            }
            match dispatch {
                GearZedWorkerDispatch::Run(job) => {
                    let agent = agent.clone();
                    let parent_session_id = parent_session_id.clone();
                    let running_native_zed_sessions = running_native_zed_sessions.clone();
                    let state = job.state.clone();
                    #[cfg(test)]
                    let lifecycle_events = lifecycle_events.clone();
                    cx.spawn(async move |cx| {
                        let result = run_native_zed_worker(
                            agent,
                            parent_session_id,
                            job,
                            running_native_zed_sessions,
                            cx,
                        )
                        .await;
                        #[cfg(test)]
                        if let Some(events) = &lifecycle_events
                            && let Err(error) = &result
                            && let Ok(mut guard) = events.lock()
                        {
                            guard.push(format!("worker:error:{error:#}"));
                        }
                        let (lock, wake) = &*state;
                        let mut state = lock.lock().expect("zed worker state poisoned");
                        state.result = Some(result.map_err(|error| format!("{error:#}")));
                        wake.notify_all();
                    })
                    .detach();
                }
                GearZedWorkerDispatch::Cancel { task_id } => {
                    let Some(worker_session_id) = running_native_zed_sessions
                        .lock()
                        .expect("running native zed sessions poisoned")
                        .get(&task_id)
                        .cloned()
                    else {
                        continue;
                    };
                    let Some(worker_thread) = agent
                        .read_with(cx, |agent, _| {
                            agent
                                .sessions
                                .get(&worker_session_id)
                                .map(|session| session.thread.clone())
                        })
                        .ok()
                        .flatten()
                    else {
                        continue;
                    };
                    let _ = worker_thread
                        .update(cx, |thread, cx| thread.cancel(cx))
                        .await;
                }
                GearZedWorkerDispatch::SetEndTurnAtBoundary { task_id, enabled } => {
                    let Some(worker_session_id) = running_native_zed_sessions
                        .lock()
                        .expect("running native zed sessions poisoned")
                        .get(&task_id)
                        .cloned()
                    else {
                        continue;
                    };
                    let Some(worker_thread) = agent
                        .read_with(cx, |agent, _| {
                            agent
                                .sessions
                                .get(&worker_session_id)
                                .map(|session| session.thread.clone())
                        })
                        .ok()
                        .flatten()
                    else {
                        continue;
                    };
                    worker_thread.update(cx, |thread, _cx| {
                        thread.set_end_turn_at_next_boundary(enabled);
                    });
                }
            }
        }
        #[cfg(test)]
        if let Some(events) = &lifecycle_events {
            if let Ok(mut guard) = events.lock() {
                guard.push("dispatcher:exit".to_string());
            }
        }
    })
    .detach();
}

async fn run_native_zed_worker(
    agent: WeakEntity<NativeAgent>,
    parent_session_id: acp::SessionId,
    job: GearZedWorkerJob,
    running_sessions: Arc<Mutex<HashMap<String, acp::SessionId>>>,
    cx: &mut AsyncApp,
) -> Result<WorkerResult> {
    let (parent_thread, subagent_thread, acp_thread, worker_session_id, applied_worker_model) =
        agent.update(cx, |agent, cx| -> Result<_> {
            let requested_model = job
                .worker_model
                .as_ref()
                .map(|model_id| {
                    agent
                        .models
                        .model_from_id(&AgentModelId::new(model_id.clone()))
                        .with_context(|| {
                            format!("native Gear worker requested unavailable model `{model_id}`")
                        })
                })
                .transpose()?;
            let parent_session = agent
                .sessions
                .get(&parent_session_id)
                .context("parent Gear session not found for native zed worker")?;
            let parent_thread = parent_session.thread.clone();
            let current_depth = parent_thread.read(cx).depth();
            if current_depth >= MAX_SUBAGENT_DEPTH {
                anyhow::bail!("Maximum subagent depth ({MAX_SUBAGENT_DEPTH}) reached");
            }

            let subagent_thread = cx.new(|cx| {
                let mut thread = Thread::new_subagent(&parent_thread, cx);
                if let Some(model) = requested_model {
                    thread.set_pinned_model(model, cx);
                }
                thread.set_title(format!("Gear Worker {}", job.task_id).into(), cx);
                thread
            });
            let worker_session_id = subagent_thread.read(cx).id().clone();
            let applied_worker_model = subagent_thread
                .read(cx)
                .model()
                .map(|model| format!("{}/{}", model.provider_id().0, model.id().0));
            let acp_thread = agent.register_session(
                subagent_thread.clone(),
                parent_session.project_id,
                1,
                None,
                ZED_AGENT_ID.clone(),
                "zed".into(),
                cx,
            );
            parent_thread.update(cx, |thread, _cx| {
                thread.register_running_subagent(subagent_thread.downgrade())
            });
            Ok((
                parent_thread,
                subagent_thread,
                acp_thread,
                worker_session_id,
                applied_worker_model,
            ))
        })??;

    {
        let (lock, _) = &*job.state;
        let mut state = lock.lock().expect("zed worker state poisoned");
        state.session_id = Some(worker_session_id.clone());
    }
    running_sessions
        .lock()
        .expect("running native zed sessions poisoned")
        .insert(job.task_id.clone(), worker_session_id.clone());
    if let Some(applied_worker_model) = applied_worker_model.as_deref() {
        let selection = serde_json::to_string_pretty(&serde_json::json!({
            "requested_model": job.worker_model,
            "applied_model": applied_worker_model,
            "worker_session_id": worker_session_id.to_string(),
        }))
        .context("failed to serialize native Gear worker model selection evidence")?;
        job.store.write_worker_file(
            &job.task_id,
            "model-selection.json",
            &format!("{selection}\n"),
        )?;
    }

    let run_result = async {
        let mut next_prompt = job.prompt.clone();
        let mut next_prompt_path = job.prompt_path.clone();
        loop {
            if job.cancellation_token.is_cancelled() {
                anyhow::bail!("native zed worker cancelled before prompt started");
            }

            let response = acp_thread
                .update(cx, |acp_thread, cx| {
                    acp_thread.send(vec![next_prompt.clone().into()], cx)
                })
                .await?;
            let assistant_text = subagent_thread.read_with(cx, |thread, _cx| {
                thread
                    .last_message()
                    .and_then(|message| {
                        let content = message
                            .as_agent_message()?
                            .content
                            .iter()
                            .filter_map(|content| match content {
                                AgentMessageContent::Text(text) => Some(text.as_str()),
                                _ => None,
                            })
                            .join("\n\n");
                        (!content.is_empty()).then_some(content)
                    })
                    .unwrap_or_default()
            });
            let next_interaction = {
                let (lock, _) = &*job.state;
                let mut state = lock.lock().expect("zed worker state poisoned");
                state.last_output = Some(assistant_text.clone());
                state.pending_interactions.pop_front().map(|interaction| {
                    state.interaction_count += 1;
                    (interaction, state.interaction_count)
                })
            };

            if let Some((interaction, interaction_index)) = next_interaction {
                next_prompt = interaction.prompt.clone();
                next_prompt_path = job.store.write_worker_file(
                    &job.task_id,
                    &format!("{}-{}.md", interaction.kind.as_str(), interaction_index),
                    &format!(
                        "# Gear native zed worker {}\n\n{}\n",
                        interaction.kind.as_str(),
                        interaction.prompt.trim()
                    ),
                )?;
                if matches!(interaction.kind, GearZedInteractionKind::Steer) {
                    subagent_thread.update(cx, |thread, _cx| {
                        thread.set_end_turn_at_next_boundary(false);
                    });
                }
                continue;
            }

            break match response {
                Some(response) if response.stop_reason == acp::StopReason::EndTurn => {
                    build_native_zed_worker_result(
                        &job.store,
                        &job.task_id,
                        job.packet_path,
                        next_prompt_path,
                        assistant_text,
                        WorkerStatus::Succeeded,
                        None,
                    )
                }
                Some(response) if response.stop_reason == acp::StopReason::Cancelled => {
                    build_native_zed_worker_result(
                        &job.store,
                        &job.task_id,
                        job.packet_path,
                        next_prompt_path,
                        assistant_text,
                        WorkerStatus::Failed,
                        Some("cancelled".to_string()),
                    )
                }
                Some(response) => {
                    let failure = match response.stop_reason {
                        acp::StopReason::MaxTokens => {
                            "native zed worker reached the maximum number of tokens".to_string()
                        }
                        acp::StopReason::MaxTurnRequests => {
                            "native zed worker reached the maximum number of turn requests"
                                .to_string()
                        }
                        acp::StopReason::Refusal => {
                            "native zed worker refused to process the prompt".to_string()
                        }
                        _ => format!("native zed worker stopped: {:?}", response.stop_reason),
                    };
                    build_native_zed_worker_result(
                        &job.store,
                        &job.task_id,
                        job.packet_path,
                        next_prompt_path,
                        assistant_text,
                        WorkerStatus::Failed,
                        Some(failure),
                    )
                }
                None => build_native_zed_worker_result(
                    &job.store,
                    &job.task_id,
                    job.packet_path,
                    next_prompt_path,
                    String::new(),
                    WorkerStatus::Failed,
                    Some("native zed worker returned no response".to_string()),
                ),
            };
        }
    }
    .await;

    parent_thread.update(cx, |thread, cx| {
        thread.unregister_running_subagent(&worker_session_id, cx)
    });
    running_sessions
        .lock()
        .expect("running native zed sessions poisoned")
        .remove(&job.task_id);
    run_result
}

fn build_native_zed_worker_result(
    store: &gearbox_agent::state::StateStore,
    task_id: &str,
    packet_path: PathBuf,
    prompt_path: PathBuf,
    assistant_text: String,
    status: WorkerStatus,
    failure: Option<String>,
) -> Result<WorkerResult> {
    let summary = assistant_text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(ToString::to_string)
        .or_else(|| failure.clone())
        .unwrap_or_else(|| "native zed worker completed without output".to_string());
    let last_message_path = if assistant_text.trim().is_empty() && failure.is_none() {
        None
    } else {
        let body = native_zed_worker_message_body(&assistant_text, failure.as_deref());
        Some(store.write_worker_file(task_id, "last-message.md", &body)?)
    };
    let result = WorkerResult {
        status,
        command: Some("zed-agent-native".to_string()),
        exit_code: None,
        summary,
        packet_path,
        prompt_path,
        stdout_path: None,
        stderr_path: None,
        last_message_path,
        result_path: store.worker_dir(task_id).join("result.json"),
        outcome_path: store.worker_dir(task_id).join("outcome.json"),
    };
    write_result_and_outcome(store, task_id, &result)?;
    Ok(result)
}

// ── ACP Broker Backend ──────────────────────────────────────────────────────

/// Typed channel dispatch for the ACP broker foreground dispatcher.
enum GearAcpBrokerDispatch {
    /// Start a new ACP broker worker session.
    Run(GearAcpBrokerJob),
    /// Cancel a running session by task ID.
    Cancel { task_id: String },
    /// Set end-turn-at-boundary on a session.
    SetEndTurnAtBoundary { task_id: String, enabled: bool },
    /// Release a session only when its Gear worker handle is explicitly disposed.
    Dispose {
        task_id: String,
        session_id: acp::SessionId,
        state: Arc<(Mutex<GearAcpBrokerState>, Condvar)>,
    },
}

/// Job passed through the channel to start an ACP broker worker session.
struct GearAcpBrokerJob {
    store: gearbox_agent::state::StateStore,
    task_id: String,
    prompt: String,
    packet_path: PathBuf,
    prompt_path: PathBuf,
    worker_model: Option<String>,
    cancellation_token: CancellationToken,
    state: Arc<(Mutex<GearAcpBrokerState>, Condvar)>,
    session_id: Option<acp::SessionId>,
}

/// Mutable state shared between the foreground dispatcher and the session handle.
#[derive(Default)]
struct GearAcpBrokerState {
    session_id: Option<acp::SessionId>,
    result: Option<std::result::Result<WorkerResult, String>>,
    last_output: Option<String>,
    usage: Option<BrokerUsage>,
    pending_interactions: VecDeque<GearAcpBrokerInteraction>,
    interaction_count: usize,
    observed_tool_call_ids: HashSet<String>,
    /// Permission events recorded during the interaction.
    permission_events: Vec<BrokerPermissionEvidence>,
    observed_permission_resolution_ids: HashSet<String>,
    event_hub: WorkerEventHub,
}

#[derive(Clone)]
struct GearAcpBrokerInteraction {
    kind: GearAcpBrokerInteractionKind,
    prompt: String,
}

#[derive(Clone, Copy)]
enum GearAcpBrokerInteractionKind {
    FollowUp,
    Steer,
}

impl GearAcpBrokerInteractionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::FollowUp => "acp-follow-up",
            Self::Steer => "acp-steer",
        }
    }
}

/// Backend that delegates ACP broker worker operations to the foreground thread.
struct GearAcpBrokerBackend {
    request_tx: async_channel::Sender<GearAcpBrokerDispatch>,
}

impl GearAcpBrokerBackend {
    fn new(request_tx: async_channel::Sender<GearAcpBrokerDispatch>) -> Self {
        Self { request_tx }
    }
}

/// Session handle returned by `GearAcpBrokerBackend::start_zed_agent`.
struct GearAcpBrokerSessionHandle {
    task_id: String,
    store: gearbox_agent::state::StateStore,
    packet_path: PathBuf,
    worker_model: Option<String>,
    request_tx: async_channel::Sender<GearAcpBrokerDispatch>,
    state: Arc<(Mutex<GearAcpBrokerState>, Condvar)>,
    cancellation_token: CancellationToken,
}

impl GearAcpBrokerSessionHandle {
    fn enqueue_interaction(
        &self,
        kind: GearAcpBrokerInteractionKind,
        prompt: String,
    ) -> Result<bool> {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().expect("acp broker state poisoned");
        let resume_session_id = state.result.as_ref().and_then(|_| state.session_id.clone());
        state.result = None;
        if resume_session_id.is_none() {
            state
                .pending_interactions
                .push_back(GearAcpBrokerInteraction { kind, prompt });
            return Ok(false);
        }
        state.interaction_count = state.interaction_count.saturating_add(1);
        let interaction_index = state.interaction_count;
        drop(state);

        let prompt_path = self.store.write_worker_file(
            &self.task_id,
            &format!("{}-{}.md", kind.as_str(), interaction_index),
            &format!(
                "# ACP broker worker {}\n\n{}\n",
                kind.as_str(),
                prompt.trim()
            ),
        )?;
        self.request_tx
            .send_blocking(GearAcpBrokerDispatch::Run(GearAcpBrokerJob {
                store: self.store.clone(),
                task_id: self.task_id.clone(),
                prompt,
                packet_path: self.packet_path.clone(),
                prompt_path,
                worker_model: self.worker_model.clone(),
                cancellation_token: self.cancellation_token.clone(),
                state: self.state.clone(),
                session_id: resume_session_id,
            }))
            .context("failed to queue terminal ACP worker revive")?;
        Ok(true)
    }
}

impl NativeWorkerBackend for GearAcpBrokerBackend {
    fn start_zed_agent(
        &self,
        request: WorkerStartRequest<'_>,
    ) -> Result<Arc<dyn WorkerSessionHandle>> {
        let route = request
            .config
            .selected_route_for_hint(request.route_attempt, request.route_hint);
        let (_category_resolution, _category_resolution_result) = category_resolution_for_route(
            request.config,
            request.route_attempt,
            request.route_hint,
            &route,
        );
        let worker_model = validate_native_worker_model_id(route.worker_model)?;
        let plan_task = request.task.inputs.plan_task.as_ref();
        let current_step_id = plan_task.and_then(|plan_task| {
            plan_task
                .execution_steps_or_legacy()
                .first()
                .map(|step| step.step_id.clone())
        });
        let packet_goal = plan_task
            .map(|plan_task| plan_task.worker_goal(request.goal))
            .unwrap_or_else(|| request.goal.to_string());
        let constraints = plan_task
            .map(gearbox_agent::plan_graph::PlanTaskContract::worker_constraints)
            .unwrap_or_else(|| {
                vec![
                    "Stay inside the allowed paths when they are provided.".to_string(),
                    "Prefer the package manager already used by the project.".to_string(),
                    "Read the provided spec and plan artifacts before changing code.".to_string(),
                    "Leave runnable local instructions in the final output.".to_string(),
                ]
            });
        let required_outputs = plan_task
            .map(gearbox_agent::plan_graph::PlanTaskContract::worker_required_outputs)
            .unwrap_or_else(|| {
                vec![
                    "summary".to_string(),
                    "changed_files".to_string(),
                    "commands_run".to_string(),
                    "known_failures".to_string(),
                    "next_steps".to_string(),
                ]
            });
        let planned_verification = plan_task
            .map(gearbox_agent::plan_graph::PlanTaskContract::worker_verification_commands)
            .filter(|commands| !commands.is_empty())
            .unwrap_or_else(|| request.verification_commands.to_vec());
        let stop_conditions = plan_task
            .map(gearbox_agent::plan_graph::PlanTaskContract::worker_stop_conditions)
            .unwrap_or_else(|| {
                vec![
                    "Requires a paid external service.".to_string(),
                    "Requires a user-provided API key.".to_string(),
                    "The same verification fails twice.".to_string(),
                ]
            });
        let (injected_rules, rules_injection_path) =
            discover_workspace_rules(request.store, request.workspace, request.task)?;
        let (injected_skills, skills_injection_path) =
            discover_workspace_skills(request.store, request.workspace, request.task)?;
        let packet = WorkerPacket {
            task_id: request.task.id.clone(),
            worker: route.worker_kind.as_str().to_string(),
            current_step_id,
            worker_model: worker_model.clone(),
            variant: route.variant.clone(),
            variant_applied: route.variant.clone(),
            prompt_append: route.prompt_append.clone(),
            injected_rules,
            rules_injection_path,
            injected_skills,
            skills_injection_path,
            tools: route.tools.clone(),
            category_resolution: _category_resolution,
            category_resolution_result: _category_resolution_result,
            goal: packet_goal,
            coordinator_model: request.coordinator_model.cloned(),
            coordinator_brief: request.coordinator_brief.map(ToString::to_string),
            scope: request.task.scope.clone(),
            inputs: request.task.inputs.clone(),
            constraints,
            required_outputs,
            verification: VerificationContract {
                preferred_commands: planned_verification,
                must_not_skip: vec!["typecheck".to_string()],
            },
            stop_conditions,
            prompt_manifest_path: None,
            prompt_reconcile_path: None,
            prompt_capsule_path: None,
        };
        let packet_json =
            serde_json::to_string_pretty(&packet).context("failed to serialize worker packet")?;
        let packet_path = request.store.write_worker_file(
            &request.task.id,
            "packet.json",
            &format!("{packet_json}\n"),
        )?;
        let prompt = worker_prompt(&packet)?;
        let prompt_path =
            request
                .store
                .write_worker_file(&request.task.id, "prompt.md", &prompt)?;

        let cancellation_token = request
            .cancellation_token
            .clone()
            .unwrap_or_else(CancellationToken::new);
        let state = Arc::new((Mutex::new(GearAcpBrokerState::default()), Condvar::new()));
        self.request_tx
            .send_blocking(GearAcpBrokerDispatch::Run(GearAcpBrokerJob {
                store: request.store.clone(),
                task_id: request.task.id.clone(),
                prompt,
                packet_path: packet_path.clone(),
                prompt_path: prompt_path.clone(),
                worker_model: worker_model.clone(),
                cancellation_token: cancellation_token.clone(),
                state: state.clone(),
                session_id: None,
            }))
            .context("failed to queue acp broker worker job")?;

        Ok(Arc::new(GearAcpBrokerSessionHandle {
            task_id: request.task.id.clone(),
            store: request.store.clone(),
            packet_path,
            worker_model,
            request_tx: self.request_tx.clone(),
            state,
            cancellation_token,
        }))
    }
}

impl WorkerSessionHandle for GearAcpBrokerSessionHandle {
    fn session_id(&self) -> Option<String> {
        self.state
            .0
            .lock()
            .ok()
            .and_then(|state| state.session_id.as_ref().map(ToString::to_string))
    }

    fn send_follow_up(&self, prompt: String) -> Result<()> {
        self.enqueue_interaction(GearAcpBrokerInteractionKind::FollowUp, prompt)
            .map(|_| ())
    }

    fn steer(&self, prompt: String) -> Result<()> {
        let terminal_revive =
            self.enqueue_interaction(GearAcpBrokerInteractionKind::Steer, prompt)?;
        if !terminal_revive {
            self.request_tx
                .send_blocking(GearAcpBrokerDispatch::SetEndTurnAtBoundary {
                    task_id: self.task_id.clone(),
                    enabled: true,
                })
                .ok();
        }
        Ok(())
    }

    fn interrupt(&self) -> Result<()> {
        self.cancel()
    }

    fn cancel(&self) -> Result<()> {
        self.cancellation_token.cancel();
        self.request_tx
            .send_blocking(GearAcpBrokerDispatch::Cancel {
                task_id: self.task_id.clone(),
            })
            .ok();
        Ok(())
    }

    fn dispose(&self) -> Result<()> {
        let session_id = self
            .state
            .0
            .lock()
            .map_err(|_| anyhow!("acp broker state poisoned"))?
            .session_id
            .clone();
        if let Some(session_id) = session_id {
            self.request_tx
                .send_blocking(GearAcpBrokerDispatch::Dispose {
                    task_id: self.task_id.clone(),
                    session_id,
                    state: self.state.clone(),
                })
                .context("failed to dispose ACP broker worker session")?;
        }
        Ok(())
    }

    fn supports_event_subscriptions(&self) -> bool {
        true
    }

    fn subscribe(
        &self,
        listener: gearbox_agent::workers::WorkerEventListener,
    ) -> Result<gearbox_agent::workers::WorkerSubscription> {
        let event_hub = self
            .state
            .0
            .lock()
            .map_err(|_| anyhow!("acp broker state poisoned"))?
            .event_hub
            .clone();
        event_hub.subscribe(listener)
    }

    fn reset_event_history(&self) -> Result<()> {
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| anyhow!("acp broker state poisoned"))?;
        state.event_hub.clear_history()
    }

    fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
        worker_outcome_from_result(&self.wait_for_result()?)
    }

    fn wait_for_result(&self) -> Result<WorkerResult> {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().expect("acp broker state poisoned");
        loop {
            if let Some(result) = state.result.clone() {
                return result.map_err(anyhow::Error::msg);
            }
            state = wake.wait(state).expect("acp broker state poisoned");
        }
    }

    fn last_output(&self) -> Option<String> {
        self.state
            .0
            .lock()
            .ok()
            .and_then(|state| state.last_output.clone())
    }

    fn usage(&self) -> Option<BrokerUsage> {
        self.state
            .0
            .lock()
            .ok()
            .and_then(|state| state.usage.clone())
    }
}

/// Spawn the foreground dispatcher loop for ACP broker operations.
///
/// Runs on the GPUI foreground thread, receiving dispatch requests and
/// calling into `NativeAgent` for actual ACP operations. Uses
/// `LanguageModelRegistry` for live model discovery and maps between
/// broker contract types and ACP connection types.
fn spawn_gear_acp_broker_dispatcher(
    agent: WeakEntity<NativeAgent>,
    parent_session_id: acp::SessionId,
    acp_broker_rx: async_channel::Receiver<GearAcpBrokerDispatch>,
    running_acp_sessions: Arc<Mutex<HashMap<String, acp::SessionId>>>,
    cx: &mut App,
) {
    cx.spawn(async move |cx| {
        while let Ok(dispatch) = acp_broker_rx.recv().await {
            match dispatch {
                GearAcpBrokerDispatch::Run(job) => {
                    let agent = agent.clone();
                    let parent_session_id = parent_session_id.clone();
                    let running_acp_sessions = running_acp_sessions.clone();
                    let state = job.state.clone();
                    cx.spawn(async move |cx| {
                        let result = run_gear_acp_broker_worker(
                            agent,
                            parent_session_id,
                            job,
                            running_acp_sessions,
                            cx,
                        )
                        .await;
                        let (lock, wake) = &*state;
                        let mut state = lock.lock().expect("acp broker state poisoned");
                        state.result = Some(result.map_err(|error| format!("{error:#}")));
                        wake.notify_all();
                    })
                    .detach();
                }
                GearAcpBrokerDispatch::Cancel { task_id } => {
                    let Some(worker_session_id) = running_acp_sessions
                        .lock()
                        .expect("running acp sessions poisoned")
                        .get(&task_id)
                        .cloned()
                    else {
                        continue;
                    };
                    let Some(worker_thread) = agent
                        .read_with(cx, |agent, _| {
                            agent
                                .sessions
                                .get(&worker_session_id)
                                .map(|session| session.thread.clone())
                        })
                        .ok()
                        .flatten()
                    else {
                        continue;
                    };
                    let _ = worker_thread
                        .update(cx, |thread, cx| thread.cancel(cx))
                        .await;
                }
                GearAcpBrokerDispatch::Dispose {
                    task_id,
                    session_id,
                    state,
                } => {
                    running_acp_sessions
                        .lock()
                        .expect("running acp sessions poisoned")
                        .remove(&task_id);
                    match agent.update(cx, |agent, cx| agent.close_session(&session_id, cx)) {
                        Ok(close_task) => {
                            if let Err(error) = close_task.await {
                                eprintln!(
                                    "failed to dispose Gear ACP worker session {session_id}: {error:#}"
                                );
                            }
                        }
                        Err(error) => eprintln!(
                            "failed to schedule disposal for Gear ACP worker session {session_id}: {error:#}"
                        ),
                    }
                    let (lock, wake) = &*state;
                    let mut state = lock.lock().expect("acp broker state poisoned");
                    state.session_id = None;
                    wake.notify_all();
                }
                GearAcpBrokerDispatch::SetEndTurnAtBoundary { task_id, enabled } => {
                    let Some(worker_session_id) = running_acp_sessions
                        .lock()
                        .expect("running acp sessions poisoned")
                        .get(&task_id)
                        .cloned()
                    else {
                        continue;
                    };
                    let Some(worker_thread) = agent
                        .read_with(cx, |agent, _| {
                            agent
                                .sessions
                                .get(&worker_session_id)
                                .map(|session| session.thread.clone())
                        })
                        .ok()
                        .flatten()
                    else {
                        continue;
                    };
                    worker_thread.update(cx, |thread, _cx| {
                        thread.set_end_turn_at_next_boundary(enabled);
                    });
                }
            }
        }
    })
    .detach();
}

fn emit_gear_acp_worker_event(job: &GearAcpBrokerJob, event: WorkerEvent) -> Result<()> {
    let event_json =
        serde_json::to_string(&event).context("failed to serialize ACP worker event")?;
    let line = format!("{event_json}\n");
    job.store
        .append_worker_file(&job.task_id, "transcript.jsonl", &line)?;
    if matches!(
        event,
        WorkerEvent::TurnStarted { .. }
            | WorkerEvent::TurnFinished { .. }
            | WorkerEvent::ToolCallStarted { .. }
            | WorkerEvent::ToolCallFinished { .. }
            | WorkerEvent::Error { .. }
    ) {
        job.store
            .append_worker_file(&job.task_id, "tool-events.jsonl", &line)?;
    }
    let event_hub = job
        .state
        .0
        .lock()
        .map_err(|_| anyhow!("acp broker state poisoned"))?
        .event_hub
        .clone();
    event_hub.emit(event);
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GearAcpToolCallObservation {
    id: String,
    tool_name: String,
    arguments: String,
}

fn gear_acp_tool_call_observations(message: Option<&Message>) -> Vec<GearAcpToolCallObservation> {
    let Some(agent_message) = message.and_then(Message::as_agent_message) else {
        return Vec::new();
    };

    agent_message
        .content
        .iter()
        .filter_map(|content| {
            let AgentMessageContent::ToolUse(tool_use) = content else {
                return None;
            };
            let arguments = if tool_use.raw_input.trim().is_empty() {
                serde_json::to_string(&tool_use.input.to_display_json()).unwrap_or_default()
            } else {
                tool_use.raw_input.clone()
            };
            Some(GearAcpToolCallObservation {
                id: tool_use.id.to_string(),
                tool_name: tool_use.name.to_string(),
                arguments,
            })
        })
        .collect()
}

fn emit_gear_acp_tool_call_observations(
    job: &GearAcpBrokerJob,
    observations: impl IntoIterator<Item = GearAcpToolCallObservation>,
) -> Result<()> {
    for observation in observations {
        let should_emit = {
            let (lock, _) = &*job.state;
            let mut state = lock.lock().expect("acp broker state poisoned");
            state.observed_tool_call_ids.insert(observation.id)
        };
        if should_emit {
            emit_gear_acp_worker_event(
                job,
                WorkerEvent::ToolCallStarted {
                    kind: "acp".to_string(),
                    tool_name: observation.tool_name,
                    arguments: observation.arguments,
                },
            )?;
        }
    }
    Ok(())
}

fn broker_permission_type_for_tool_kind(kind: &acp::ToolKind) -> BrokerPermissionType {
    match kind {
        acp::ToolKind::Read | acp::ToolKind::Search => BrokerPermissionType::ReadFile,
        acp::ToolKind::Edit | acp::ToolKind::Delete | acp::ToolKind::Move => {
            BrokerPermissionType::WriteFile
        }
        acp::ToolKind::Execute => BrokerPermissionType::ExecuteCommand,
        acp::ToolKind::Fetch => BrokerPermissionType::NetworkAccess,
        _ => BrokerPermissionType::EnvironmentAccess,
    }
}

fn broker_permission_granted(status: &acp_thread::ToolCallStatus) -> bool {
    matches!(
        status,
        acp_thread::ToolCallStatus::InProgress
            | acp_thread::ToolCallStatus::Completed
            | acp_thread::ToolCallStatus::Failed
    )
}

fn record_gear_acp_permission_resolution(
    store: &gearbox_agent::state::StateStore,
    task_id: &str,
    state: &Arc<(Mutex<GearAcpBrokerState>, Condvar)>,
    acp_thread: &Entity<AcpThread>,
    cx: &App,
    tool_call_id: &acp::ToolCallId,
) -> Result<()> {
    let acp_thread = acp_thread.read(cx);
    let Some((_, tool_call)) = acp_thread.tool_call(tool_call_id) else {
        return Ok(());
    };

    let tool_name = tool_call
        .tool_name
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "unknown".to_string());
    let status = tool_call.status.to_string();
    let permission_type = broker_permission_type_for_tool_kind(&tool_call.kind);
    let granted = broker_permission_granted(&tool_call.status);

    let model_context = {
        let (lock, _) = &**state;
        let mut state = lock.lock().expect("acp broker state poisoned");
        let id = tool_call_id.to_string();
        if !state.observed_permission_resolution_ids.insert(id) {
            return Ok(());
        }
        state.session_id.as_ref().map(ToString::to_string)
    };

    let evidence = BrokerPermissionEvidence {
        permission_type,
        granted,
        timestamp: gearbox_agent::state::timestamp(),
        agent_name: "acp-broker".to_string(),
        model_context,
        reason: Some(format!(
            "tool `{tool_name}` ({tool_call_id}) authorization resolved as {status}"
        )),
    };
    let line =
        serde_json::to_string(&evidence).context("failed to serialize ACP permission evidence")?;
    if let Err(error) =
        store.append_worker_file(task_id, "permission-events.jsonl", &format!("{line}\n"))
    {
        let (lock, _) = &**state;
        lock.lock()
            .expect("acp broker state poisoned")
            .observed_permission_resolution_ids
            .remove(&tool_call_id.to_string());
        return Err(error);
    }
    let (lock, _) = &**state;
    lock.lock()
        .expect("acp broker state poisoned")
        .permission_events
        .push(evidence);
    Ok(())
}

fn broker_usage_from_thread(thread: &Thread, model: Option<&str>, duration_ms: u64) -> BrokerUsage {
    broker_usage_from_token_usage(thread.latest_token_usage().as_ref(), model, duration_ms)
}

fn broker_usage_from_token_usage(
    usage: Option<&acp_thread::TokenUsage>,
    model: Option<&str>,
    duration_ms: u64,
) -> BrokerUsage {
    let (requested_tokens, actual_tokens, unavailable_reason) = match usage {
        Some(usage) => (
            Some(usage.input_tokens),
            Some(usage.output_tokens),
            Some("native ACP provider did not report cost or cache telemetry".to_string()),
        ),
        None => (
            None,
            None,
            Some("native ACP provider did not report token, cost, or cache telemetry".to_string()),
        ),
    };
    BrokerUsage {
        requested_tokens,
        actual_tokens,
        model: model.unwrap_or("unknown").to_string(),
        duration_ms: Some(duration_ms),
        cost_micros: None,
        cache_hit: None,
        unavailable_reason,
    }
}

fn merge_broker_usage(previous: Option<BrokerUsage>, current: BrokerUsage) -> BrokerUsage {
    let add = |left: Option<u64>, right: Option<u64>| match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        _ => None,
    };
    if let Some(previous) = previous {
        BrokerUsage {
            requested_tokens: add(previous.requested_tokens, current.requested_tokens),
            actual_tokens: add(previous.actual_tokens, current.actual_tokens),
            model: current.model,
            duration_ms: add(previous.duration_ms, current.duration_ms),
            cost_micros: add(previous.cost_micros, current.cost_micros),
            cache_hit: current.cache_hit.or(previous.cache_hit),
            unavailable_reason: merge_broker_unavailable_reasons(
                previous.unavailable_reason,
                current.unavailable_reason,
            ),
        }
    } else {
        current
    }
}

fn merge_broker_unavailable_reasons(
    previous: Option<String>,
    current: Option<String>,
) -> Option<String> {
    let mut reasons = Vec::new();
    for reason in [previous, current].into_iter().flatten() {
        if !reasons.iter().any(|existing| existing == &reason) {
            reasons.push(reason);
        }
    }
    (!reasons.is_empty()).then(|| reasons.join("; "))
}

/// Run an ACP broker worker session on the foreground thread.
///
/// Validates the requested model against `LanguageModelRegistry`, creates
/// an ACP session via `NativeAgent`, sends the prompt, and handles
/// follow-up and steer interactions. Records permission events in the
/// broker state.
async fn run_gear_acp_broker_worker(
    agent: WeakEntity<NativeAgent>,
    parent_session_id: acp::SessionId,
    job: GearAcpBrokerJob,
    running_sessions: Arc<Mutex<HashMap<String, acp::SessionId>>>,
    cx: &mut AsyncApp,
) -> Result<WorkerResult> {
    let (parent_thread, subagent_thread, acp_thread, worker_session_id, applied_worker_model) =
        agent.update(cx, |agent, cx| -> Result<_> {
            let parent_session = agent
                .sessions
                .get(&parent_session_id)
                .context("parent ACP broker session not found")?;
            let parent_thread = parent_session.thread.clone();
            let (subagent_thread, acp_thread, worker_session_id, applied_worker_model) =
                if let Some(resume_session_id) = job.session_id.clone() {
                    let session = agent
                        .sessions
                        .get(&resume_session_id)
                        .with_context(|| {
                            format!(
                                "ACP broker resident session `{resume_session_id}` is no longer available"
                            )
                        })?;
                    let subagent_thread = session.thread.clone();
                    let acp_thread = session.acp_thread.clone();
                    let applied_worker_model = subagent_thread
                        .read(cx)
                        .model()
                        .map(|model| format!("{}/{}", model.provider_id().0, model.id().0));
                    (
                        subagent_thread,
                        acp_thread,
                        resume_session_id,
                        applied_worker_model,
                    )
                } else {
                    let requested_model = job
                        .worker_model
                        .as_ref()
                        .map(|model_id| {
                            let available: Vec<_> = LanguageModelRegistry::global(cx)
                                .read(cx)
                                .available_models(cx)
                                .collect();
                            available
                                .into_iter()
                                .find(|m| {
                                    format!("{}/{}", m.provider_id().0, m.id().0) == *model_id
                                })
                                .ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "ACP broker requested unavailable model `{model_id}`"
                                    )
                                })
                        })
                        .transpose()?;
                    let current_depth = parent_thread.read(cx).depth();
                    if current_depth >= MAX_SUBAGENT_DEPTH {
                        anyhow::bail!("Maximum subagent depth ({MAX_SUBAGENT_DEPTH}) reached");
                    }

                    let subagent_thread = cx.new(|cx| {
                        let mut thread = Thread::new_subagent(&parent_thread, cx);
                        if let Some(model) = requested_model {
                            thread.set_pinned_model(model, cx);
                        }
                        thread.set_title(format!("ACP Broker Worker {}", job.task_id).into(), cx);
                        thread
                    });
                    let worker_session_id = subagent_thread.read(cx).id().clone();
                    let applied_worker_model = subagent_thread
                        .read(cx)
                        .model()
                        .map(|model| format!("{}/{}", model.provider_id().0, model.id().0));
                    let acp_thread = agent.register_session(
                        subagent_thread.clone(),
                        parent_session.project_id,
                        1,
                        None,
                        crate::ZED_AGENT_ID.clone(),
                        "zed".into(),
                        cx,
                    );
                    (
                        subagent_thread,
                        acp_thread,
                        worker_session_id,
                        applied_worker_model,
                    )
                };
            parent_thread.update(cx, |thread, _cx| {
                thread.register_running_subagent(subagent_thread.downgrade())
            });
            Ok((
                parent_thread,
                subagent_thread,
                acp_thread,
                worker_session_id,
                applied_worker_model,
            ))
        })??;

    {
        let (lock, _) = &*job.state;
        let mut state = lock.lock().expect("acp broker state poisoned");
        state.session_id = Some(worker_session_id.clone());
    }
    running_sessions
        .lock()
        .expect("running acp sessions poisoned")
        .insert(job.task_id.clone(), worker_session_id.clone());
    if let Some(applied_worker_model) = applied_worker_model.as_deref() {
        let selection = serde_json::to_string_pretty(&serde_json::json!({
            "requested_model": job.worker_model,
            "applied_model": applied_worker_model,
            "worker_session_id": worker_session_id.to_string(),
        }))
        .context("failed to serialize ACP broker model selection evidence")?;
        job.store.write_worker_file(
            &job.task_id,
            "acp-model-selection.json",
            &format!("{selection}\n"),
        )?;
    }

    let _permission_subscription = {
        let store = job.store.clone();
        let task_id = job.task_id.clone();
        let state = job.state.clone();
        cx.subscribe(&acp_thread, move |acp_thread, event, cx| {
            if let AcpThreadEvent::ToolAuthorizationReceived(tool_call_id) = event {
                record_gear_acp_permission_resolution(
                    &store,
                    &task_id,
                    &state,
                    &acp_thread,
                    cx,
                    tool_call_id,
                )
                .log_err();
            }
        })
    };

    let run_result = async {
        let mut next_prompt = job.prompt.clone();
        let mut next_prompt_path = job.prompt_path.clone();
        loop {
            if job.cancellation_token.is_cancelled() {
                anyhow::bail!("acp broker worker cancelled before prompt started");
            }

            let turn_started_at = Instant::now();
            emit_gear_acp_worker_event(
                &job,
                WorkerEvent::TurnStarted {
                    kind: "acp".to_string(),
                    prompt_path: next_prompt_path.clone(),
                },
            )?;

            let response = match acp_thread
                .update(cx, |acp_thread, cx| {
                    acp_thread.send(vec![next_prompt.clone().into()], cx)
                })
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    emit_gear_acp_worker_event(
                        &job,
                        WorkerEvent::Error {
                            kind: "acp".to_string(),
                            message: format!("{error:#}"),
                        },
                    )?;
                    return Err(error);
                }
            };

            let (assistant_text, tool_call_observations) =
                subagent_thread.read_with(cx, |thread, _cx| {
                    let message = thread.last_message();
                    let assistant_text = message
                        .and_then(Message::as_agent_message)
                        .map(|agent_message| {
                            agent_message
                                .content
                                .iter()
                                .filter_map(|content| match content {
                                    AgentMessageContent::Text(text) => Some(text.as_str()),
                                    _ => None,
                                })
                                .join("\n\n")
                        })
                        .filter(|content| !content.is_empty())
                        .unwrap_or_default();
                    (assistant_text, gear_acp_tool_call_observations(message))
                });
            emit_gear_acp_tool_call_observations(&job, tool_call_observations)?;
            if !assistant_text.trim().is_empty() {
                emit_gear_acp_worker_event(
                    &job,
                    WorkerEvent::AssistantTextDelta {
                        kind: "acp".to_string(),
                        delta: assistant_text.clone(),
                    },
                )?;
            }
            let usage = subagent_thread.read_with(cx, |thread, _cx| {
                broker_usage_from_thread(
                    thread,
                    applied_worker_model.as_deref(),
                    turn_started_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
                )
            });
            let usage = {
                let (lock, _) = &*job.state;
                let mut state = lock.lock().expect("acp broker state poisoned");
                let usage = merge_broker_usage(state.usage.take(), usage);
                state.usage = Some(usage.clone());
                usage
            };
            job.store.write_worker_file(
                &job.task_id,
                "usage.json",
                &format!("{}\n", serde_json::to_string_pretty(&usage)?),
            )?;
            emit_gear_acp_worker_event(
                &job,
                WorkerEvent::TurnFinished {
                    kind: "acp".to_string(),
                    result_path: job.store.worker_dir(&job.task_id).join("result.json"),
                    outcome_path: job.store.worker_dir(&job.task_id).join("outcome.json"),
                    summary: "ACP worker turn completed".to_string(),
                },
            )?;
            let next_interaction = {
                let (lock, _) = &*job.state;
                let mut state = lock.lock().expect("acp broker state poisoned");
                state.last_output = Some(assistant_text.clone());
                state.pending_interactions.pop_front().map(|interaction| {
                    state.interaction_count += 1;
                    (interaction, state.interaction_count)
                })
            };

            if let Some((interaction, interaction_index)) = next_interaction {
                next_prompt = interaction.prompt.clone();
                next_prompt_path = job.store.write_worker_file(
                    &job.task_id,
                    &format!("{}-{}.md", interaction.kind.as_str(), interaction_index),
                    &format!(
                        "# ACP broker worker {}\n\n{}\n",
                        interaction.kind.as_str(),
                        interaction.prompt.trim()
                    ),
                )?;
                if matches!(interaction.kind, GearAcpBrokerInteractionKind::Steer) {
                    subagent_thread.update(cx, |thread, _cx| {
                        thread.set_end_turn_at_next_boundary(false);
                    });
                }
                continue;
            }

            break match response {
                Some(response) if response.stop_reason == acp::StopReason::EndTurn => {
                    build_native_zed_worker_result(
                        &job.store,
                        &job.task_id,
                        job.packet_path.clone(),
                        next_prompt_path.clone(),
                        assistant_text,
                        WorkerStatus::Succeeded,
                        None,
                    )
                }
                Some(response) if response.stop_reason == acp::StopReason::Cancelled => {
                    build_native_zed_worker_result(
                        &job.store,
                        &job.task_id,
                        job.packet_path.clone(),
                        next_prompt_path.clone(),
                        assistant_text,
                        WorkerStatus::Failed,
                        Some("cancelled".to_string()),
                    )
                }
                Some(response) => {
                    let failure = match response.stop_reason {
                        acp::StopReason::MaxTokens => {
                            "acp broker worker reached the maximum number of tokens".to_string()
                        }
                        acp::StopReason::MaxTurnRequests => {
                            "acp broker worker reached the maximum number of turn requests"
                                .to_string()
                        }
                        acp::StopReason::Refusal => {
                            "acp broker worker refused to process the prompt".to_string()
                        }
                        _ => format!("acp broker worker stopped: {:?}", response.stop_reason),
                    };
                    build_native_zed_worker_result(
                        &job.store,
                        &job.task_id,
                        job.packet_path.clone(),
                        next_prompt_path.clone(),
                        assistant_text,
                        WorkerStatus::Failed,
                        Some(failure),
                    )
                }
                None => build_native_zed_worker_result(
                    &job.store,
                    &job.task_id,
                    job.packet_path.clone(),
                    next_prompt_path.clone(),
                    String::new(),
                    WorkerStatus::Failed,
                    Some("acp broker worker returned no response".to_string()),
                ),
            };
        }
    }
    .await;

    parent_thread.update(cx, |thread, cx| {
        thread.unregister_running_subagent(&worker_session_id, cx)
    });

    run_result
}

/// Discover available ACP agents and their model selectors from
/// `LanguageModelRegistry`.
pub fn gear_acp_broker_discover_agents(cx: &App) -> Vec<(String, ModelAvailability)> {
    let registry = LanguageModelRegistry::read_global(cx);
    let available: Vec<_> = registry.available_models(cx).collect();

    if available.is_empty() {
        return vec![(
            "acp-broker".to_string(),
            ModelAvailability::Unavailable(UnavailableReason::NotConfigured),
        )];
    }

    let mut results: Vec<_> = available
        .iter()
        .filter_map(|model| {
            let qualified = format!("{}/{}", model.provider_id().0, model.id().0);
            let selector_id = ModelSelectorId::from_qualified("acp-broker", &qualified).ok()?;
            Some((
                format!("acp-broker/{}", model.provider_id().0),
                ModelAvailability::Available(selector_id),
            ))
        })
        .collect();

    if results.is_empty() {
        results.push((
            "acp-broker".to_string(),
            ModelAvailability::Unavailable(UnavailableReason::NotSupported),
        ));
    }

    results
}

fn native_zed_worker_message_body(assistant_text: &str, failure: Option<&str>) -> String {
    let mut body = String::new();
    if !assistant_text.trim().is_empty() {
        body.push_str(assistant_text.trim());
        body.push('\n');
    }
    if let Some(failure) = failure {
        if !body.is_empty() {
            body.push('\n');
        }
        body.push_str("## Known Failures\n");
        body.push_str(&format!("- {failure}\n"));
    }
    body
}

impl NativeThreadEnvironment {
    pub(crate) fn create_subagent_thread(
        &self,
        label: String,
        cx: &mut App,
    ) -> Result<Rc<dyn SubagentHandle>> {
        let Some(parent_thread_entity) = self.thread.upgrade() else {
            anyhow::bail!("Parent thread no longer exists".to_string());
        };
        let parent_thread = parent_thread_entity.read(cx);
        let current_depth = parent_thread.depth();
        let parent_session_id = parent_thread.id().clone();

        if current_depth >= MAX_SUBAGENT_DEPTH {
            return Err(anyhow!(
                "Maximum subagent depth ({}) reached",
                MAX_SUBAGENT_DEPTH
            ));
        }

        let subagent_thread: Entity<Thread> = cx.new(|cx| {
            let mut thread = Thread::new_subagent(&parent_thread_entity, cx);
            thread.set_title(label.into(), cx);
            thread
        });

        let session_id = subagent_thread.read(cx).id().clone();

        let acp_thread = self
            .agent
            .update(cx, |agent, cx| -> Result<Entity<AcpThread>> {
                let project_id = agent
                    .sessions
                    .get(&parent_session_id)
                    .map(|s| s.project_id)
                    .context("parent session not found")?;
                Ok(agent.register_session(
                    subagent_thread.clone(),
                    project_id,
                    1,
                    None,
                    ZED_AGENT_ID.clone(),
                    "zed".into(),
                    cx,
                ))
            })??;

        let depth = current_depth + 1;

        telemetry::event!(
            "Subagent Started",
            session = parent_thread_entity.read(cx).id().to_string(),
            subagent_session = session_id.to_string(),
            depth,
            is_resumed = false,
        );

        self.prompt_subagent(session_id, subagent_thread, acp_thread)
    }

    pub(crate) fn resume_subagent_thread(
        &self,
        session_id: acp::SessionId,
        cx: &mut App,
    ) -> Result<Rc<dyn SubagentHandle>> {
        let (subagent_thread, acp_thread) = self.agent.update(cx, |agent, _cx| {
            let session = agent
                .sessions
                .get(&session_id)
                .ok_or_else(|| anyhow!("No subagent session found with id {session_id}"))?;
            anyhow::Ok((session.thread.clone(), session.acp_thread.clone()))
        })??;

        let depth = subagent_thread.read(cx).depth();

        if let Some(parent_thread_entity) = self.thread.upgrade() {
            telemetry::event!(
                "Subagent Started",
                session = parent_thread_entity.read(cx).id().to_string(),
                subagent_session = session_id.to_string(),
                depth,
                is_resumed = true,
            );
        }

        self.prompt_subagent(session_id, subagent_thread, acp_thread)
    }

    fn prompt_subagent(
        &self,
        session_id: acp::SessionId,
        subagent_thread: Entity<Thread>,
        acp_thread: Entity<acp_thread::AcpThread>,
    ) -> Result<Rc<dyn SubagentHandle>> {
        let Some(parent_thread_entity) = self.thread.upgrade() else {
            anyhow::bail!("Parent thread no longer exists".to_string());
        };
        Ok(Rc::new(NativeSubagentHandle::new(
            session_id,
            subagent_thread,
            acp_thread,
            parent_thread_entity,
        )) as _)
    }
}

impl ThreadEnvironment for NativeThreadEnvironment {
    fn create_terminal(
        &self,
        command: String,
        extra_env: Vec<acp::EnvVariable>,
        cwd: Option<PathBuf>,
        output_byte_limit: Option<u64>,
        sandbox_wrap: Option<acp_thread::SandboxWrap>,
        cx: &mut AsyncApp,
    ) -> Task<Result<Rc<dyn TerminalHandle>>> {
        // On Seatbelt-style sandboxes (macOS) there's no tmpfs overlay, so to
        // give the command a writable temp area we point `$TMPDIR`/`$TMP`/
        // `$TEMP` at a per-thread directory inside the sandbox's writable
        // scope. Doing this even when sandboxing is disabled keeps `$TMPDIR`
        // stable so the model can't infer sandbox state from it.
        //
        // Only do this for local projects. For remote projects the temp
        // directory would be created on the client, but the terminal runs on
        // the remote host, so pointing `$TMPDIR` (and the sandbox writable
        // scope) at a client-side path would leak client environment into the
        // remote terminal and reference a directory that doesn't exist there.
        //
        // Linux and Windows are excluded: the bwrap sandbox (run directly on
        // Linux, and via WSL on Windows) already mounts a fresh, writable
        // `tmpfs` over `/tmp`, so the environment looks like a normal
        // filesystem with no special `$TMPDIR` (which would only make the
        // sandbox more obviously Zed-specific). On Windows a per-thread
        // `$TMPDIR` would also be a Windows path that's meaningless inside
        // WSL, and adding it to the writable scope would bind a stray
        // `/mnt/<drive>/...` path.
        #[cfg_attr(any(target_os = "linux", target_os = "windows"), allow(unused_mut))]
        let mut extra_env = extra_env;
        #[cfg_attr(any(target_os = "linux", target_os = "windows"), allow(unused_mut))]
        let mut sandbox_wrap = sandbox_wrap;
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            let temp_dir = self.thread.update(cx, |thread, cx| {
                thread
                    .project()
                    .read(cx)
                    .is_local()
                    .then(|| thread.sandboxed_terminal_temp_dir(cx))
            });
            match temp_dir {
                Ok(Some(Ok(temp_dir))) => {
                    // Canonicalize so the path matches what the sandbox
                    // resolves symlinks to (e.g. `/var` -> `/private/var` on
                    // macOS). `$TMPDIR` and the writable-scope entry below must
                    // agree, and they must agree with the path the kernel
                    // actually checks.
                    let temp_dir = temp_dir.canonicalize().unwrap_or(temp_dir);
                    let temp_dir_string = temp_dir.to_string_lossy().into_owned();
                    extra_env.extend([
                        acp::EnvVariable::new("TMPDIR", &temp_dir_string),
                        acp::EnvVariable::new("TMP", &temp_dir_string),
                        acp::EnvVariable::new("TEMP", &temp_dir_string),
                    ]);
                    // The command's `$TMPDIR` must live inside the sandbox's
                    // writable scope. The per-thread temp directory is owned
                    // here (not in the terminal tool that assembles the rest
                    // of the writable set), so add it whenever the command is
                    // sandboxed.
                    if let Some(sandbox_wrap) = &mut sandbox_wrap {
                        sandbox_wrap.writable_paths.push(temp_dir);
                    }
                }
                Ok(None) => {}
                Ok(Some(Err(error))) => return Task::ready(Err(error)),
                Err(error) => return Task::ready(Err(error)),
            };
        }
        let task = self.acp_thread.update(cx, |thread, cx| {
            thread.create_terminal(
                command,
                vec![],
                extra_env,
                cwd,
                output_byte_limit,
                sandbox_wrap,
                cx,
            )
        });

        let acp_thread = self.acp_thread.clone();
        cx.spawn(async move |cx| {
            let terminal = task?.await?;

            let (drop_tx, drop_rx) = oneshot::channel();
            let terminal_id = terminal.read_with(cx, |terminal, _cx| terminal.id().clone());

            cx.spawn(async move |cx| {
                drop_rx.await.ok();
                acp_thread.update(cx, |thread, cx| thread.release_terminal(terminal_id, cx))
            })
            .detach();

            let handle = AcpTerminalHandle {
                terminal,
                _drop_tx: Some(drop_tx),
            };

            Ok(Rc::new(handle) as _)
        })
    }

    fn create_subagent(&self, label: String, cx: &mut App) -> Result<Rc<dyn SubagentHandle>> {
        self.create_subagent_thread(label, cx)
    }

    fn resume_subagent(
        &self,
        session_id: acp::SessionId,
        cx: &mut App,
    ) -> Result<Rc<dyn SubagentHandle>> {
        self.resume_subagent_thread(session_id, cx)
    }

    fn create_sibling_thread(
        &self,
        request: SiblingThreadRequest,
        cx: &mut AsyncApp,
    ) -> Task<Result<SiblingThreadInfo>> {
        let host = match self
            .agent
            .read_with(cx, |agent, _| agent.sibling_thread_host())
        {
            Ok(Some(host)) => host,
            Ok(None) => {
                return Task::ready(Err(anyhow!(
                    "No sibling-thread host is registered. This usually means the \
                     agent panel hasn't been initialized in this workspace."
                )));
            }
            Err(err) => return Task::ready(Err(err)),
        };
        host.create_sibling_thread(request, cx)
    }

    fn list_available_agents(&self, cx: &mut App) -> Result<AvailableAgents> {
        let host = self
            .agent
            .read_with(cx, |agent, _| agent.sibling_thread_host())?
            .ok_or_else(|| {
                anyhow!(
                    "No sibling-thread host is registered. This usually means the \
                     agent panel hasn't been initialized in this workspace."
                )
            })?;
        host.list_available_agents(cx)
    }
}

#[derive(Debug, Clone)]
enum SubagentPromptResult {
    Completed,
    Cancelled,
    ContextWindowWarning,
    Error(String),
}

pub struct NativeSubagentHandle {
    session_id: acp::SessionId,
    parent_thread: WeakEntity<Thread>,
    subagent_thread: Entity<Thread>,
    acp_thread: Entity<acp_thread::AcpThread>,
}

impl NativeSubagentHandle {
    fn new(
        session_id: acp::SessionId,
        subagent_thread: Entity<Thread>,
        acp_thread: Entity<acp_thread::AcpThread>,
        parent_thread_entity: Entity<Thread>,
    ) -> Self {
        NativeSubagentHandle {
            session_id,
            subagent_thread,
            parent_thread: parent_thread_entity.downgrade(),
            acp_thread,
        }
    }
}

impl SubagentHandle for NativeSubagentHandle {
    fn id(&self) -> acp::SessionId {
        self.session_id.clone()
    }

    fn num_entries(&self, cx: &App) -> usize {
        self.acp_thread.read(cx).entries().len()
    }

    fn send(&self, message: String, cx: &AsyncApp) -> Task<Result<String>> {
        let thread = self.subagent_thread.clone();
        let acp_thread = self.acp_thread.clone();
        let subagent_session_id = self.session_id.clone();
        let parent_thread = self.parent_thread.clone();

        cx.spawn(async move |cx| {
            let (task, _subscription) = cx.update(|cx| {
                let ratio_before_prompt = thread
                    .read(cx)
                    .latest_token_usage()
                    .map(|usage| usage.ratio());

                parent_thread
                    .update(cx, |parent_thread, _cx| {
                        parent_thread.register_running_subagent(thread.downgrade())
                    })
                    .ok();

                let task = acp_thread.update(cx, |acp_thread, cx| {
                    acp_thread.send(vec![message.into()], cx)
                });

                let (token_limit_tx, token_limit_rx) = oneshot::channel::<()>();
                let mut token_limit_tx = Some(token_limit_tx);

                let subscription = cx.subscribe(
                    &thread,
                    move |_thread, event: &TokenUsageUpdated, _cx| {
                        if let Some(usage) = &event.0 {
                            let old_ratio = ratio_before_prompt
                                .clone()
                                .unwrap_or(TokenUsageRatio::Normal);
                            let new_ratio = usage.ratio();
                            if old_ratio == TokenUsageRatio::Normal
                                && new_ratio == TokenUsageRatio::Warning
                            {
                                if let Some(tx) = token_limit_tx.take() {
                                    tx.send(()).ok();
                                }
                            }
                        }
                    },
                );

                let wait_for_prompt = cx
                    .background_spawn(async move {
                        futures::select! {
                            response = task.fuse() => match response {
                                Ok(Some(response)) => {
                                    match response.stop_reason {
                                        acp::StopReason::Cancelled => SubagentPromptResult::Cancelled,
                                        acp::StopReason::MaxTokens => SubagentPromptResult::Error("The agent reached the maximum number of tokens.".into()),
                                        acp::StopReason::MaxTurnRequests => SubagentPromptResult::Error("The agent reached the maximum number of allowed requests between user turns. Try prompting again.".into()),
                                        acp::StopReason::Refusal => SubagentPromptResult::Error("The agent refused to process that prompt. Try again.".into()),
                                        acp::StopReason::EndTurn | _ => SubagentPromptResult::Completed,
                                    }
                                }
                                Ok(None) => SubagentPromptResult::Error("No response from the agent. You can try messaging again.".into()),
                                Err(error) => SubagentPromptResult::Error(error.to_string()),
                            },
                            _ = token_limit_rx.fuse() => SubagentPromptResult::ContextWindowWarning,
                        }
                    });

                (wait_for_prompt, subscription)
            });

            let result = match task.await {
                SubagentPromptResult::Completed => thread.read_with(cx, |thread, _cx| {
                    thread
                        .last_message()
                        .and_then(|message| {
                            let content = message.as_agent_message()?
                                .content
                                .iter()
                                .filter_map(|c| match c {
                                    AgentMessageContent::Text(text) => Some(text.as_str()),
                                    _ => None,
                                })
                                .join("\n\n");
                            if content.is_empty() {
                                None
                            } else {
                                Some( content)
                            }
                        })
                        .context("No response from subagent")
                }),
                SubagentPromptResult::Cancelled => Err(anyhow!("User canceled")),
                SubagentPromptResult::Error(message) => Err(anyhow!("{message}")),
                SubagentPromptResult::ContextWindowWarning => {
                    thread.update(cx, |thread, cx| thread.cancel(cx)).await;
                    Err(anyhow!(
                        "The agent is nearing the end of its context window and has been \
                         stopped. You can prompt the thread again to have the agent wrap up \
                         or hand off its work."
                    ))
                }
            };

            parent_thread
                .update(cx, |parent_thread, cx| {
                    parent_thread.unregister_running_subagent(&subagent_session_id, cx)
                })
                .ok();

            result
        })
    }
}

pub struct AcpTerminalHandle {
    terminal: Entity<acp_thread::Terminal>,
    _drop_tx: Option<oneshot::Sender<()>>,
}

impl TerminalHandle for AcpTerminalHandle {
    fn id(&self, cx: &AsyncApp) -> Result<acp::TerminalId> {
        Ok(self.terminal.read_with(cx, |term, _cx| term.id().clone()))
    }

    fn wait_for_exit(&self, cx: &AsyncApp) -> Result<Shared<Task<acp::TerminalExitStatus>>> {
        Ok(self
            .terminal
            .read_with(cx, |term, _cx| term.wait_for_exit()))
    }

    fn current_output(&self, cx: &AsyncApp) -> Result<acp::TerminalOutputResponse> {
        Ok(self
            .terminal
            .read_with(cx, |term, cx| term.current_output(cx)))
    }

    fn kill(&self, cx: &AsyncApp) -> Result<()> {
        cx.update(|cx| {
            self.terminal.update(cx, |terminal, cx| {
                terminal.kill(cx);
            });
        });
        Ok(())
    }

    fn was_stopped_by_user(&self, cx: &AsyncApp) -> Result<bool> {
        Ok(self
            .terminal
            .read_with(cx, |term, _cx| term.was_stopped_by_user()))
    }
}

/// Build the catalog the model sees in its system prompt: filter out hidden
/// (`disable_model_invocation`) skills, then drop the rest if they would push
/// the catalog past the description budget.
///
/// Returns `SkillSummary` values rather than full `Skill`s so that the
/// (potentially ~100KB) skill bodies aren't cloned just to be discarded by
/// `ProjectContext::new`, which only needs the summary fields.
fn select_catalog_skills(skills: &[Skill]) -> (Vec<SkillSummary>, Vec<SkillLoadingIssueData>) {
    let mut kept = Vec::new();
    let mut issues = Vec::new();
    let mut dropped: Vec<&Skill> = Vec::new();
    let mut total_size = 0usize;
    let mut budget_exceeded = false;

    for skill in skills {
        if skill.disable_model_invocation {
            continue;
        }

        let entry_size = skill.name.len() + skill.description.len();
        if !budget_exceeded && total_size.saturating_add(entry_size) <= MAX_SKILL_DESCRIPTIONS_SIZE
        {
            total_size += entry_size;
            kept.push(SkillSummary::from(skill));
        } else {
            // Once any model-invocable skill overflows the budget, stop
            // packing entirely so the cutoff is deterministic by sort order
            // rather than dependent on which skills happen to be small
            // enough to fit in the remaining space.
            budget_exceeded = true;
            dropped.push(skill);
        }
    }

    if !dropped.is_empty() {
        let budget_kb = MAX_SKILL_DESCRIPTIONS_SIZE / 1024;
        let first = dropped[0];
        let message = if dropped.len() == 1 {
            let entry_size = first.name.len() + first.description.len();
            format!(
                "Skill '{}' ({:.1}KB description) was dropped from the catalog because the previous skills already used the entire {}KB description budget.",
                first.name,
                entry_size as f64 / 1024.0,
                budget_kb,
            )
        } else {
            let mut message = format!(
                "{} skills were dropped from the catalog because they exceeded the {}KB description budget:",
                dropped.len(),
                budget_kb,
            );
            for skill in &dropped {
                let entry_size = skill.name.len() + skill.description.len();
                message.push('\n');
                message.push_str(&format!(
                    "- {} ({:.1}KB description)",
                    skill.name,
                    entry_size as f64 / 1024.0,
                ));
            }
            message
        };
        issues.push(SkillLoadingIssueData::catalog_budget_exceeded(
            first.skill_file_path.clone(),
            message,
        ));
    }

    (kept, issues)
}

/// Build a closure that, when called, reads the latest `state.skills`
/// for the given project from the `NativeAgent` and applies
/// project-overrides-global so the `SkillTool` resolves a name to the
/// same entry the model sees in its catalog. Run at invocation time
/// (not thread-build time) so skill changes after thread construction
/// become visible without re-registering the tool.
pub fn skills_resolver_for_project(
    weak_agent: WeakEntity<NativeAgent>,
    project_id: EntityId,
) -> impl Fn(&App) -> Arc<Vec<Skill>> + Send + Sync + 'static {
    move |cx: &App| {
        weak_agent
            .upgrade()
            .and_then(|agent| {
                agent
                    .read(cx)
                    .projects
                    .get(&project_id)
                    .map(|state| Arc::new(apply_skill_overrides(&state.skills)))
            })
            .unwrap_or_else(|| Arc::new(Vec::new()))
    }
}

pub fn skill_body_resolver_for_project(
    project: Entity<Project>,
    fs: Arc<dyn Fs>,
) -> impl Fn(Skill, &mut AsyncApp) -> Task<Result<String>> + Send + Sync + 'static {
    move |skill, cx| match skill.source.clone() {
        SkillSource::ProjectLocal { worktree_id, .. } => {
            let project = project.clone();
            cx.spawn(async move |cx| {
                let worktree_id = WorktreeId::from_usize(worktree_id.0);
                let worktree = project
                    .update(cx, |project, cx| project.worktree_for_id(worktree_id, cx))
                    .context("no such worktree")?;
                expand_project_skills_directories(&worktree, cx).await?;
                let relative_path = worktree.update(cx, |worktree, _cx| {
                    let worktree_root = worktree.abs_path();
                    worktree
                        .path_style()
                        .strip_prefix(&skill.skill_file_path, &worktree_root)
                        .map(|relative_path| relative_path.into_arc())
                        .context("skill file is not inside its worktree")
                })?;

                let buffer = project
                    .update(cx, |project, cx| {
                        project.open_buffer((worktree_id, relative_path), cx)
                    })
                    .await?;
                let content =
                    cx.update(|cx| buffer.read(cx).as_text_snapshot().as_rope().to_string());

                read_skill_body_from_content(&skill.skill_file_path, &content).map_err(Into::into)
            })
        }
        SkillSource::BuiltIn | SkillSource::Global => {
            let fs = fs.clone();
            cx.background_spawn(async move {
                agent_skills::read_skill_body(fs.as_ref(), &skill.skill_file_path)
                    .await
                    .map_err(Into::into)
            })
        }
    }
}

/// Collect successfully-loaded global and project-local skills into a
/// single list, preserving every entry — even when two skills share a
/// name. The autocomplete popup shows the full list with origin labels
/// so users can tell same-named skills apart; override resolution
/// (project-local wins over global) happens later via
/// [`apply_skill_overrides`] at the boundaries where the model
/// interacts with skills (system-prompt catalog, `SkillTool` lookup,
/// slash-command invocation).
///
/// Global versions of skills will be before the local versions
fn combine_skills(
    global: Vec<Result<Skill, SkillLoadError>>,
    project: impl Iterator<Item = Result<Skill, SkillLoadError>>,
) -> (Vec<Skill>, Vec<SkillLoadError>) {
    // Built-in skills go first (lowest priority) so that global and
    // project-local skills with the same name shadow them.
    let mut skills = builtin_skills();
    let mut errors = Vec::new();
    for result in global.into_iter().chain(project) {
        match result {
            Ok(skill) => skills.push(skill),
            Err(e) => errors.push(e),
        }
    }
    log_skill_conflicts(&skills);
    (skills, errors)
}

/// Emit a warning for each name collision between skills. Called once
/// per skill load (not per query), so the log isn't spammed by repeated
/// catalog rebuilds.
fn log_skill_conflicts(skills: &[Skill]) {
    let mut by_name: HashMap<&str, &Skill> = HashMap::default();
    for skill in skills {
        match by_name.get(skill.name.as_str()) {
            Some(existing) => {
                if skill.source.precedence() > existing.source.precedence() {
                    log::warn!(
                        "Skill '{}' at '{}' overrides skill at '{}' for the model; both appear in the slash-command popup with their source",
                        skill.name,
                        skill.skill_file_path.display(),
                        existing.skill_file_path.display(),
                    );
                    by_name.insert(skill.name.as_str(), skill);
                } else {
                    log::warn!(
                        "Skill '{}' at '{}' conflicts with skill at '{}'; the model will see the first one, but both appear in the slash-command popup with their source",
                        skill.name,
                        skill.skill_file_path.display(),
                        existing.skill_file_path.display(),
                    );
                }
            }
            None => {
                by_name.insert(skill.name.as_str(), skill);
            }
        }
    }
}

/// Project-local skills override same-named global skills. Returns a
/// new list with at most one entry per name. Two skills of the same
/// source colliding (e.g. two globals or two project-locals) keep the
/// first one to match the historical behavior.
///
/// This is the projection of `state.skills` used by everything the
/// model interacts with: the system-prompt catalog, the `SkillTool`'s
/// name resolver, and slash-command invocation. The autocomplete popup
/// deliberately does *not* go through this — it shows the full list so
/// users can see what's shadowed.
fn apply_skill_overrides(skills: &[Skill]) -> Vec<Skill> {
    let mut result: Vec<Skill> = Vec::new();
    // Borrow names from the input slice so the dedup index doesn't
    // need to allocate a `String` per skill. The borrow is valid for
    // the body of the function because `skills` outlives `indices`.
    let mut indices: HashMap<&str, usize> = HashMap::default();
    for skill in skills {
        match indices.get(skill.name.as_str()).copied() {
            Some(idx) => {
                if skill.source.precedence() > result[idx].source.precedence() {
                    result[idx] = skill.clone();
                }
            }
            None => {
                indices.insert(skill.name.as_str(), result.len());
                result.push(skill.clone());
            }
        }
    }
    result
}

#[cfg(test)]
mod internal_tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;
    use acp_thread::{AgentConnection, AgentModelGroupName, AgentModelInfo, MentionUri};
    use agent_settings::COMPACTION_PROMPT;
    use fs::FakeFs;
    use gearbox_agent::state::Scope;
    use gearbox_agent::workers::{CategoryResolution, CategoryResolutionResult, FallbackRoute};
    use gpui::TestAppContext;
    use indoc::formatdoc;
    use language_model::fake_provider::{FakeLanguageModel, FakeLanguageModelProvider};
    use language_model::{
        CompletionIntent, LanguageModelCompletionEvent, LanguageModelProviderId,
        LanguageModelProviderName,
    };
    use serde_json::json;
    use settings::SettingsStore;
    use util::{path, rel_path::rel_path};

    fn make_global_skill(name: &str, description: &str) -> Skill {
        Skill {
            name: name.to_string(),
            description: description.to_string(),
            source: SkillSource::Global,
            directory_path: PathBuf::from(format!("/home/user/.agents/skills/{name}")),
            skill_file_path: PathBuf::from(format!("/home/user/.agents/skills/{name}/SKILL.md")),
            load_warnings: Vec::new(),
            disable_model_invocation: false,
            embedded_body: None,
        }
    }

    #[test]
    fn gearbox_acp_tool_call_observations_preserve_ids_and_arguments() {
        let message = Message::Agent(AgentMessage {
            content: vec![
                AgentMessageContent::ToolUse(language_model::LanguageModelToolUse {
                    id: "tool-read".into(),
                    name: "read_file".into(),
                    raw_input: r#"{"path":"src/lib.rs"}"#.to_string(),
                    input: language_model::LanguageModelToolUseInput::Json(
                        json!({"path": "src/lib.rs"}),
                    ),
                    is_input_complete: true,
                    thought_signature: None,
                }),
                AgentMessageContent::ToolUse(language_model::LanguageModelToolUse {
                    id: "tool-edit".into(),
                    name: "edit_file".into(),
                    raw_input: String::new(),
                    input: language_model::LanguageModelToolUseInput::Json(
                        json!({"path": "src/main.rs", "patch": "same"}),
                    ),
                    is_input_complete: true,
                    thought_signature: None,
                }),
            ],
            tool_results: IndexMap::default(),
            reasoning_details: None,
        });

        let observations = gear_acp_tool_call_observations(Some(&message));
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].id, "tool-read");
        assert_eq!(observations[0].tool_name, "read_file");
        assert_eq!(observations[0].arguments, r#"{"path":"src/lib.rs"}"#);
        assert_eq!(observations[1].id, "tool-edit");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&observations[1].arguments).ok(),
            Some(json!({"path": "src/main.rs", "patch": "same"}))
        );
    }

    #[test]
    fn gearbox_acp_tool_call_events_dedupe_replayed_ids() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let state = Arc::new((Mutex::new(GearAcpBrokerState::default()), Condvar::new()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let _subscription = {
            let event_hub = state
                .0
                .lock()
                .map_err(|_| anyhow!("acp broker state poisoned"))?
                .event_hub
                .clone();
            let events = events.clone();
            event_hub.subscribe(Arc::new(move |event| {
                events
                    .lock()
                    .expect("acp broker event capture mutex poisoned")
                    .push(event);
            }))?
        };
        let job = GearAcpBrokerJob {
            store,
            task_id: "task_acp_tool_events".to_string(),
            prompt: "prompt".to_string(),
            packet_path: temp_dir.path().join("packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
            worker_model: None,
            cancellation_token: CancellationToken::new(),
            state,
            session_id: None,
        };
        let observations = vec![GearAcpToolCallObservation {
            id: "tool-1".to_string(),
            tool_name: "read_file".to_string(),
            arguments: r#"{"path":"src/lib.rs"}"#.to_string(),
        }];
        emit_gear_acp_tool_call_observations(&job, observations.clone())?;
        emit_gear_acp_tool_call_observations(&job, observations)?;

        let events = events
            .lock()
            .map_err(|_| anyhow!("acp broker event capture mutex poisoned"))?;
        let tool_events = events
            .iter()
            .filter(|event| matches!(event, WorkerEvent::ToolCallStarted { .. }))
            .count();
        assert_eq!(tool_events, 1);
        Ok(())
    }

    #[test]
    fn gearbox_acp_permission_type_tracks_tool_kind() {
        assert_eq!(
            broker_permission_type_for_tool_kind(&acp::ToolKind::Read),
            BrokerPermissionType::ReadFile
        );
        assert_eq!(
            broker_permission_type_for_tool_kind(&acp::ToolKind::Edit),
            BrokerPermissionType::WriteFile
        );
        assert_eq!(
            broker_permission_type_for_tool_kind(&acp::ToolKind::Execute),
            BrokerPermissionType::ExecuteCommand
        );
        assert_eq!(
            broker_permission_type_for_tool_kind(&acp::ToolKind::Fetch),
            BrokerPermissionType::NetworkAccess
        );
        assert_eq!(
            broker_permission_type_for_tool_kind(&acp::ToolKind::Think),
            BrokerPermissionType::EnvironmentAccess
        );
    }

    #[test]
    fn gearbox_acp_permission_grant_only_accepts_non_terminal_denials() {
        assert!(broker_permission_granted(
            &acp_thread::ToolCallStatus::InProgress
        ));
        assert!(broker_permission_granted(
            &acp_thread::ToolCallStatus::Completed
        ));
        assert!(broker_permission_granted(
            &acp_thread::ToolCallStatus::Failed
        ));
        assert!(!broker_permission_granted(
            &acp_thread::ToolCallStatus::Rejected
        ));
        assert!(!broker_permission_granted(
            &acp_thread::ToolCallStatus::Canceled
        ));
    }

    fn coordinator_review_input(no_progress_signals: Vec<String>) -> CoordinatorReviewInput {
        CoordinatorReviewInput {
            goal_id: "goal_001".to_string(),
            task_id: "task_001".to_string(),
            iteration: 2,
            max_iterations: 5,
            request: "Build a tiny task tracker".to_string(),
            worker_kind: "codex".to_string(),
            worker_model: Some("gpt-5".to_string()),
            worker_category: "review".to_string(),
            route_reason: "category `review` selected attempt 2 configured `codex` route"
                .to_string(),
            worker_attempt: 2,
            worker_attempt_count: 2,
            worker_failure_kind: None,
            worker_retry_reason: None,
            worker_fallback_summary: "none".to_string(),
            worker_status: "succeeded".to_string(),
            worker_summary: "worker summary".to_string(),
            worker_outcome_summary: "outcome summary".to_string(),
            worker_commands_run: vec!["echo verify-ok".to_string()],
            worker_known_failures: Vec::new(),
            worker_outcome_path: Some("/tmp/outcome.json".to_string()),
            worker_transcript_head: None,
            worker_transcript_tail: None,
            category_resolution: CategoryResolution::default(),
            category_resolution_result: CategoryResolutionResult::Resolved {
                requested_category: "review".to_string(),
                available_categories: vec!["review".to_string()],
                attempted_provider_model: Some("openai/gpt5".to_string()),
                nearest_fallback: Some(FallbackRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_model: Some("gpt-5".to_string()),
                }),
            },
            no_progress_signals,
            budget_summary: "iterations=2/5; changed_files=1/10".to_string(),
            verification_passed: true,
            verification_summary: "all verification commands passed.".to_string(),
            scope_summary: "scope ok".to_string(),
            diff_summary: "diff ok".to_string(),
        }
    }

    #[test]
    fn gear_worker_config_uses_explicit_worker_and_command_precedence() {
        let config = gear_worker_config_from_values(
            Some("claude-code"),
            Some("gear worker"),
            Some("legacy opencode"),
            Some("claude-sonnet"),
            vec!["old-model".to_string()],
            2,
            3,
            2,
            30,
        );

        assert_eq!(config.worker_kind, WorkerKind::Claude);
        assert_eq!(config.worker_command.as_deref(), Some("gear worker"));
        assert_eq!(config.worker_model.as_deref(), Some("claude-sonnet"));
        assert_eq!(config.unavailable_worker_models, vec!["old-model"]);
        assert_eq!(config.premium_worker_budget, 2);
        assert_eq!(config.max_parallel_workers, 3);
        assert_eq!(config.max_parallel_per_key, 2);
        assert!(config.require_worker);
    }

    #[test]
    fn gear_worker_config_falls_back_to_legacy_opencode_command() {
        let config = gear_worker_config_from_values(
            None,
            None,
            Some("opencode run"),
            None,
            Vec::new(),
            1,
            1,
            1,
            30,
        );

        assert_eq!(config.worker_kind, WorkerKind::Opencode);
        assert_eq!(config.worker_command.as_deref(), Some("opencode run"));
        assert!(config.require_worker);
    }

    #[test]
    fn gear_open_code_phase_config_reuses_the_opencode_command() {
        let config = gear_worker_config_from_values(
            Some("opencode"),
            Some("opencode run"),
            None,
            None,
            Vec::new(),
            1,
            1,
            1,
            30,
        );
        let config = gear_open_code_phase_worker_config(config);
        let route = config
            .worker_routes
            .iter()
            .find(|route| route.worker_kind == WorkerKind::OpencodeSession)
            .expect("OpenCode phase mode must install a resident route");
        assert_eq!(route.worker_command.as_deref(), Some("opencode run"));
        assert!(config.require_worker);
    }

    #[test]
    fn generic_worker_model_does_not_enable_open_code_phase_mode() -> Result<()> {
        let profiles = gear_opencode_model_profiles_from_values(
            false,
            None,
            None,
            None,
            Some("openai/general-worker".to_string()),
        )?;
        assert!(profiles.is_none());
        assert!(!gear_phase_table_uses_opencode(
            &PhaseRouteTable::legacy_defaults()
        ));
        Ok(())
    }

    #[test]
    fn explicit_open_code_phase_mode_can_reuse_the_generic_worker_model() -> Result<()> {
        let profiles = gear_opencode_model_profiles_from_values(
            true,
            None,
            Some("deepseek/flash".to_string()),
            None,
            Some("openai/planner".to_string()),
        )?
        .expect("explicit OpenCode phase mode must resolve profiles");
        assert_eq!(profiles.planner, "openai/planner");
        assert_eq!(profiles.executor, "deepseek/flash");
        assert_eq!(profiles.reviewer, "openai/planner");
        assert!(gear_phase_table_uses_opencode(
            &PhaseRouteTable::opencode_only(profiles)?
        ));
        Ok(())
    }

    #[test]
    fn gear_open_code_phase_runner_returns_a_real_worker_identity() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let scope = Scope::new(Vec::new(), Vec::new(), 4);
        let draft = gearbox_agent::plan_graph::deterministic_fallback_draft(
            "Build an OpenCode plan",
            &scope,
            &["echo verify".to_string()],
        );
        let output_path = temp_dir.path().join("planner-output.json");
        std_fs::write(&output_path, serde_json::to_string(&draft)?)?;
        let command = format!(
            "sh -c 'cp {} \"$GEARBOX_WORKER_LAST_MESSAGE\"'",
            output_path.to_string_lossy()
        );
        let worker_config = gear_open_code_phase_worker_config(gear_worker_config_from_values(
            Some("opencode"),
            Some(&command),
            None,
            None,
            Vec::new(),
            1,
            1,
            1,
            30,
        ));
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let runner = GearOpenCodePhaseRunner {
            broker_factory: Arc::new(PhaseBrokerFactory::new(
                Arc::new(WorkerRegistry::default()),
                temp_dir.path().join(".gearbox-agent"),
            )),
            workspace: temp_dir.path().to_path_buf(),
            worker_config,
            cancellation_token: CancellationToken::new(),
        };
        let submission = runner.plan(PlannerInput {
            goal_id: "goal_opencode_runner".to_string(),
            request: "Build an OpenCode plan".to_string(),
            scope,
            verification_commands: vec!["echo verify".to_string()],
            route_decision: decision,
            intent_fold: None,
            repository_discovery: None,
        })?;

        assert_eq!(submission.draft, draft);
        assert_eq!(
            submission.planner.backend,
            PhaseExecutionBackend::WorkerSession
        );
        assert_eq!(submission.planner.provider_id.as_deref(), Some("openai"));
        assert_eq!(
            submission.planner.actual_session_id.as_deref(),
            Some("planner_goal_opencode_runner_session")
        );
        assert!(
            submission
                .artifact_path
                .as_deref()
                .is_some_and(|path| Path::new(path).is_file())
        );
        Ok(())
    }

    #[test]
    fn gear_worker_config_warns_and_uses_default_for_unknown_worker() {
        let config = gear_worker_config_from_values(
            Some("unknown-worker"),
            None,
            None,
            None,
            Vec::new(),
            1,
            1,
            1,
            30,
        );

        assert_eq!(config.worker_kind, WorkerKind::Opencode);
        assert_eq!(config.worker_command, None);
        assert!(!config.require_worker);
    }

    #[test]
    fn gear_opencode_free_fallback_routes_preserve_the_verified_order() {
        let routes = gear_opencode_free_fallback_routes(None);

        assert_eq!(
            routes
                .iter()
                .map(|route| route.worker_model.as_deref())
                .collect::<Vec<_>>(),
            vec![
                Some("opencode/hy3-free"),
                Some("opencode/mimo-v2.5-free"),
                Some("opencode/deepseek-v4-flash-free"),
            ]
        );
        assert!(routes.iter().all(|route| {
            route.worker_kind == WorkerKind::OpencodeSession
                && route.worker_command.as_deref().is_some_and(|command| {
                    command.contains("--model \"$GEARBOX_WORKER_MODEL\"")
                        && command.contains("< \"$GEARBOX_WORKER_PROMPT\"")
                        && command.contains("--session \"$GEARBOX_WORKER_SESSION_ID\"")
                        && command.contains("GEARBOX_WORKER_RESUME")
                        && !command.contains("$(cat")
                })
                && gear_opencode_free_fallback_uses_command_backend(
                    route.worker_kind,
                    route.worker_model.as_deref(),
                )
        }));
        assert!(!gear_opencode_free_fallback_uses_command_backend(
            WorkerKind::OpencodeSession,
            Some("opencode/custom-free"),
        ));
    }

    #[test]
    fn gear_worker_routes_from_env_enables_free_fallbacks_only_when_requested() {
        let original_free_fallbacks = std::env::var("GEARBOX_GEAR_OPENCODE_FREE_FALLBACKS").ok();
        let original_sequence = std::env::var("GEARBOX_GEAR_WORKER_SEQUENCE").ok();
        unsafe {
            std::env::set_var("GEARBOX_GEAR_OPENCODE_FREE_FALLBACKS", "1");
            std::env::remove_var("GEARBOX_GEAR_WORKER_SEQUENCE");
        }

        let routes = gear_worker_routes_from_env(WorkerKind::Opencode, None, None);

        unsafe {
            if let Some(value) = original_free_fallbacks {
                std::env::set_var("GEARBOX_GEAR_OPENCODE_FREE_FALLBACKS", value);
            } else {
                std::env::remove_var("GEARBOX_GEAR_OPENCODE_FREE_FALLBACKS");
            }
            if let Some(value) = original_sequence {
                std::env::set_var("GEARBOX_GEAR_WORKER_SEQUENCE", value);
            } else {
                std::env::remove_var("GEARBOX_GEAR_WORKER_SEQUENCE");
            }
        }

        assert_eq!(routes.len(), 3);
        assert!(routes.iter().all(|route| route.worker_command.is_some()));
    }

    #[test]
    fn gear_worker_routes_from_sequence_keeps_explicit_routes_over_free_defaults() {
        let routes = gear_worker_routes_from_sequence(
            "codex:gpt-5,opencode:opencode/custom-free",
            WorkerKind::Opencode,
            Some("opencode run"),
            None,
        );

        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].worker_kind, WorkerKind::Codex);
        assert_eq!(routes[0].worker_model.as_deref(), Some("gpt-5"));
        assert_eq!(routes[1].worker_kind, WorkerKind::Opencode);
        assert_eq!(
            routes[1].worker_model.as_deref(),
            Some("opencode/custom-free")
        );
    }

    #[test]
    fn gear_worker_config_uses_kind_specific_command_for_non_opencode_worker() {
        let config = gear_worker_config_from_values(
            Some("codex"),
            Some("codex exec"),
            Some("legacy opencode"),
            None,
            Vec::new(),
            1,
            1,
            1,
            30,
        );

        assert_eq!(config.worker_kind, WorkerKind::Codex);
        assert_eq!(config.worker_command.as_deref(), Some("codex exec"));
        assert!(config.require_worker);
    }

    #[test]
    fn gear_worker_config_uses_default_codex_command_when_unspecified() {
        let config = gear_worker_config_from_values(
            Some("codex"),
            None,
            None,
            Some("gpt-5"),
            Vec::new(),
            1,
            1,
            1,
            30,
        );

        assert_eq!(config.worker_kind, WorkerKind::Codex);
        assert!(
            config
                .worker_command
                .as_deref()
                .is_some_and(|command| command.contains("codex exec"))
        );
        assert!(
            config
                .worker_command
                .as_deref()
                .is_some_and(|command| command.contains("-m 'gpt-5'"))
        );
        assert!(config.require_worker);
    }

    #[test]
    fn coordinator_review_request_text_includes_no_progress_signals() {
        let input = coordinator_review_input(vec![
            "No file changes detected for 2 consecutive iterations.".to_string(),
        ]);
        let request = coordinator_review_request_text(&input, "- worker_kind: codex");

        assert!(request.contains("No-progress signals:"));
        assert!(request.contains("No file changes detected for 2 consecutive iterations."));
    }

    #[test]
    fn gear_apply_provider_model_availability_marks_missing_qualified_models() {
        let mut config = gear_worker_config_from_values(
            Some("codex"),
            Some("codex exec"),
            None,
            Some("gpt-5"),
            Vec::new(),
            1,
            1,
            1,
            30,
        );
        config.worker_routes = vec![
            WorkerRoute {
                worker_kind: WorkerKind::Claude,
                worker_command: Some("claude -p".to_string()),
                worker_model: Some("claude-3-7-sonnet".to_string()),
            },
            WorkerRoute {
                worker_kind: WorkerKind::Codex,
                worker_command: Some("codex exec".to_string()),
                worker_model: Some("gpt-4.1".to_string()),
            },
        ];

        gear_apply_provider_model_availability(
            &mut config,
            vec![
                ("openai".to_string(), "gpt-4.1".to_string()),
                ("anthropic".to_string(), "claude-3-5-sonnet".to_string()),
            ],
        );

        assert!(
            config
                .unavailable_worker_models
                .contains(&"openai/gpt-5".to_string())
        );
        assert!(
            config
                .unavailable_worker_models
                .contains(&"anthropic/claude-3-7-sonnet".to_string())
        );
        assert!(
            !config
                .unavailable_worker_models
                .contains(&"openai/gpt-4.1".to_string())
        );
    }

    #[test]
    fn gear_executable_goal_ignores_greetings_and_long_small_talk() {
        assert!(!is_gear_executable_goal("你好。"));
        assert!(!is_gear_executable_goal("我昨天看了一部很有趣的电影。"));
        assert!(is_gear_executable_goal("请帮我修复这个 bug。"));
    }

    #[test]
    fn parses_gear_coordinator_review_fields() {
        let review = parse_gear_coordinator_review(
            "GOAL_SATISFIED: no\nSUMMARY: Needs another repair pass.\nREPAIR_REQUEST: Fix the failing build.\nROUTE_HINT: repair\nSTOP_REASON: none",
        )
        .expect("review should parse");

        assert_eq!(review.goal_satisfied, Some(false));
        assert_eq!(review.summary, "Needs another repair pass.");
        assert_eq!(
            review.repair_request.as_deref(),
            Some("Fix the failing build.")
        );
        assert_eq!(review.route_hint.as_deref(), Some("repair"));
        assert_eq!(review.stop_reason, None);
    }

    #[test]
    fn parses_gear_coordinator_review_reports_warnings_for_malformed_fields() {
        let (review, warnings) = parse_gear_coordinator_review_with_warnings(
            "GOAL_SATISFIED: maybe\nSUMMARY: Needs another repair pass.\nROUTE_HINT: not-a-category\nSTOP_REASON: perhaps\nunexpected line",
        );

        let review = review.expect("review should still parse");
        assert_eq!(review.summary, "Needs another repair pass.");
        assert!(!warnings.is_empty());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("GOAL_SATISFIED"))
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("ROUTE_HINT"))
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("STOP_REASON"))
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("malformed review line"))
        );
    }

    async fn setup_native_agent_session(
        cx: &mut TestAppContext,
    ) -> (
        Rc<NativeAgentConnection>,
        Entity<NativeAgent>,
        Entity<Project>,
        Entity<AcpThread>,
    ) {
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [Path::new("/a")], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs, cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));
        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/a")]),
                    cx,
                )
            })
            .await
            .unwrap();

        (connection, agent, project, acp_thread)
    }

    fn native_thread_for_session(
        agent: &Entity<NativeAgent>,
        session_id: &acp::SessionId,
        cx: &App,
    ) -> Entity<Thread> {
        agent.read_with(cx, |agent, _cx| {
            agent.sessions.get(session_id).unwrap().thread.clone()
        })
    }

    fn request_texts_after_system(
        messages: &[language_model::LanguageModelRequestMessage],
    ) -> Vec<String> {
        messages
            .iter()
            .skip(1)
            .map(language_model::LanguageModelRequestMessage::string_contents)
            .collect()
    }

    async fn wait_for_fake_completion(model: &FakeLanguageModel, cx: &mut TestAppContext) {
        if wait_for_optional_fake_completion(model, cx, 100).await {
            return;
        }
        panic!("timed out waiting for fake model completion request");
    }

    async fn wait_for_optional_fake_completion(
        model: &FakeLanguageModel,
        cx: &mut TestAppContext,
        attempts: usize,
    ) -> bool {
        for _ in 0..attempts {
            cx.run_until_parked();
            if model.completion_count() > 0 {
                return true;
            }
            cx.background_executor
                .timer(Duration::from_millis(10))
                .await;
        }
        false
    }

    fn respond_to_fake_completions(
        model: Arc<dyn LanguageModel>,
        finished: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<usize> {
        std::thread::spawn(move || {
            let model = model.as_fake();
            let mut completion_count = 0;
            loop {
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while model.completion_count() == 0 {
                    if finished.load(Ordering::SeqCst) {
                        return completion_count;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for native Gear worker model request"
                    );
                    std::thread::yield_now();
                }
                let request = model.pending_completions().last().cloned().unwrap();
                let request_text = request
                    .messages
                    .iter()
                    .map(language_model::LanguageModelRequestMessage::string_contents)
                    .collect::<Vec<_>>()
                    .join("\n");
                let response = if request_text.contains("Gear's high-reasoning planner") {
                    assert!(
                        request_text.contains("decompose in this order")
                            || request_text.contains("Re-run the decomposition order")
                    );
                    assert!(request_text.contains("independently verifiable"));
                    let objective = request
                        .messages
                        .last()
                        .map(LanguageModelRequestMessage::string_contents)
                        .unwrap_or_else(|| "Build the requested feature".to_string());
                    serde_json::to_string(&gearbox_agent::plan_graph::deterministic_fallback_draft(
                        &objective,
                        &gearbox_agent::state::Scope::new(Vec::new(), vec![".git".to_string()], 10),
                        &["npm run build".to_string()],
                    ))
                    .unwrap()
                } else if request_text.contains("Gear's read-only PlanCritic") {
                    let evidence = request
                        .messages
                        .last()
                        .map(LanguageModelRequestMessage::string_contents)
                        .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok())
                        .unwrap();
                    let plan_hash = evidence["plan"]["plan_hash"].as_str().unwrap();
                    let goal_id = evidence["plan"]["goal_id"].as_str().unwrap();
                    let plan_id = evidence["plan"]["plan_id"].as_str().unwrap();
                    let plan_revision = evidence["plan"]["revision"].as_u64().unwrap();
                    let planner_execution_id =
                        evidence["planner_receipt"]["identity"]["execution_id"]
                            .as_str()
                            .unwrap();
                    json!({
                        "schema_version": 1,
                        "reviewed_goal_id": goal_id,
                        "reviewed_plan_id": plan_id,
                        "reviewed_plan_revision": plan_revision,
                        "reviewed_plan_hash": plan_hash,
                        "reviewed_planner_execution_id": planner_execution_id,
                        "decision": "approve",
                        "checks": [
                            {"dimension":"references","verdict":"pass","summary":"reference path evidence passed","evidence_refs":["verifier:reference_paths"]},
                            {"dimension":"executability","verdict":"pass","summary":"task contracts are executable","evidence_refs":["plan:tasks"]},
                            {"dimension":"contradictions","verdict":"pass","summary":"no contract contradiction found","evidence_refs":["plan:must_have"]},
                            {"dimension":"scope","verdict":"pass","summary":"scope evidence passed","evidence_refs":["verifier:scope"]},
                            {"dimension":"tdd","verdict":"pass","summary":"test contract evidence passed","evidence_refs":["verifier:test_contract"]},
                            {"dimension":"qa","verdict":"pass","summary":"QA contract evidence passed","evidence_refs":["verifier:qa_contract"]},
                            {"dimension":"acceptance","verdict":"pass","summary":"acceptance evidence passed","evidence_refs":["verifier:acceptance_contract"]}
                        ],
                        "findings": [],
                        "revision_instructions": null,
                        "needs_user_reason": null,
                        "summary": "sealed plan and deterministic evidence are decision complete"
                    })
                    .to_string()
                } else if request_text.contains("Gear's coordinator review hook") {
                    "GOAL_SATISFIED: yes\nSUMMARY: deterministic verification and worker evidence are ready for the required independent review\nREPAIR_REQUEST: none\nROUTE_HINT: none\nSTOP_REASON: complete"
                        .to_string()
                } else if request_text.contains("read-only final-review phase") {
                    let reviewed_execution_id = request_text
                        .split("reviewed_execution_id `")
                        .nth(1)
                        .and_then(|value| value.split('`').next())
                        .unwrap_or("missing-executor-id");
                    json!({
                        "schema_version": 1,
                        "reviewed_execution_id": reviewed_execution_id,
                        "dimensions": [
                            {"dimension": "goal_verification", "verdict": "pass", "findings": ["goal and verification artifacts inspected"]},
                            {"dimension": "code_quality", "verdict": "pass", "findings": ["bounded implementation evidence inspected"]},
                            {"dimension": "security", "verdict": "pass", "findings": ["forbidden path evidence inspected"]},
                            {"dimension": "qa_execution", "verdict": "pass", "findings": ["build verification evidence inspected"]}
                        ]
                    })
                    .to_string()
                } else {
                    "## Summary\nImplemented the bounded worker task.\n\n## Changed Files\n- none\n\n## Commands Run\n- npm run build\n\n## Known Failures\n- none"
                        .to_string()
                };
                model.send_completion_stream_text_chunk(&request, response);
                model.end_last_completion_stream();
                completion_count += 1;
            }
        })
    }

    fn native_gear_test_worker_config() -> WorkerConfig {
        WorkerConfig {
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
        }
    }

    #[test]
    fn native_gear_worker_model_id_requires_provider_qualification() {
        assert_eq!(
            validate_native_worker_model_id(Some("provider/model")).unwrap(),
            Some("provider/model".to_string())
        );
        assert!(validate_native_worker_model_id(Some("model-only")).is_err());
        assert!(validate_native_worker_model_id(Some("/model")).is_err());
        assert!(validate_native_worker_model_id(Some("provider/")).is_err());
        assert_eq!(validate_native_worker_model_id(None).unwrap(), None);
    }

    #[gpui::test]
    async fn test_compact_command_is_available(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        let project = Project::test(fs.clone(), [], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        let connection = NativeAgentConnection::new(agent.clone());
        let acp_thread = cx
            .update(|cx| {
                Rc::new(connection.clone()).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        cx.update(|cx| {
            let commands = acp_thread.read(cx).available_commands();

            let compact = commands.iter().find(|command| command.name == "compact");
            let compact = compact.expect("compact command should be available");
            assert_eq!(
                acp_thread::command_category_from_meta(&compact.meta),
                Some(acp_thread::CommandCategory::Native),
            );
        });
    }

    #[gpui::test]
    async fn test_compact_prompt_routes_to_manual_compaction(cx: &mut TestAppContext) {
        init_test(cx);
        let (connection, agent, project, acp_thread) = setup_native_agent_session(cx).await;
        let session_id = cx.update(|cx| acp_thread.read(cx).session_id().clone());
        let thread = cx.update(|cx| native_thread_for_session(&agent, &session_id, cx));
        let model = Arc::new(FakeLanguageModel::default());
        let old_message_id = ClientUserMessageId::new();

        cx.update(|cx| {
            let path_style = project.read(cx).path_style(cx);
            thread.update(cx, |thread, cx| {
                thread.set_model(model.clone(), cx);
                thread.push_acp_user_block(
                    old_message_id,
                    [acp::ContentBlock::from("old user")],
                    path_style,
                    cx,
                );
                thread.push_acp_agent_block("old assistant".into(), cx);
            });
        });

        let compact_message_id = ClientUserMessageId::new();
        let prompt_task = cx.update(|cx| {
            acp_thread::AgentSessionClientUserMessageIds::prompt(
                connection.as_ref(),
                compact_message_id,
                acp::PromptRequest::new(session_id.clone(), vec!["/compact".into()]),
                cx,
            )
        });
        cx.run_until_parked();

        let request = model.pending_completions().pop().unwrap();
        assert_eq!(
            request.intent,
            Some(CompletionIntent::ThreadContextSummarization)
        );
        assert_eq!(
            request_texts_after_system(&request.messages),
            vec![
                "old user".to_string(),
                "old assistant".to_string(),
                COMPACTION_PROMPT.to_string(),
            ]
        );

        model.send_completion_stream_text_chunk(&request, "summary");
        model.end_completion_stream(&request);
        cx.run_until_parked();
        prompt_task.await.unwrap();
    }

    #[gpui::test]
    async fn test_gear_prompt_runs_gearbox_orchestrator(cx: &mut TestAppContext) {
        init_test(cx);

        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("README.md"), "# Gear test\n").unwrap();
        std::fs::write(
            workspace.path().join("package.json"),
            r#"{"scripts":{"build":"echo build-ok"}}"#,
        )
        .unwrap();

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [Path::new("/a")], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs, cx));
        agent.update(cx, |agent, _cx| {
            agent.gear_worker_config_override = Some(native_gear_test_worker_config());
        });
        let connection = Rc::new(NativeAgentConnection::gear(agent.clone()));

        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[workspace.path()]),
                    cx,
                )
            })
            .await
            .unwrap();
        let model = cx.update(|cx| {
            LanguageModelRegistry::read_global(cx)
                .default_model()
                .map(|default_model| default_model.model)
                .expect("default test model should be available")
        });
        let fake_model = model.as_fake();
        let prompt_task = cx.update(|cx| {
            acp_thread.update(cx, |thread, cx| {
                thread.send(vec!["Build a tiny notes app MVP".into()], cx)
            })
        });
        let prompt_task = cx.foreground_executor().spawn(prompt_task);
        wait_for_fake_completion(fake_model, cx).await;
        let planner_draft = gearbox_agent::plan_graph::deterministic_fallback_draft(
            "Build a tiny notes app MVP",
            &gearbox_agent::state::Scope::new(Vec::new(), vec![".git".to_string()], 10),
            &["npm run build".to_string()],
        );
        fake_model
            .send_last_completion_stream_text_chunk(serde_json::to_string(&planner_draft).unwrap());
        fake_model.end_last_completion_stream();
        let gear_finished = Arc::new(AtomicBool::new(false));
        let worker_responder = respond_to_fake_completions(model, gear_finished.clone());
        cx.executor().allow_parking();
        prompt_task.await.unwrap();
        gear_finished.store(true, Ordering::SeqCst);
        assert_eq!(
            worker_responder.join().unwrap(),
            4,
            "Gear should run planner/PlanCritic/independent Oracle, execute one native implementation worker, and run one final reviewer"
        );
        cx.run_until_parked();

        let gearbox_root = workspace.path().join(".gear");
        assert!(gearbox_root.join("sessions").is_dir());
        assert!(gearbox_root.join("goals").is_dir());

        let final_report = std::fs::read_dir(gearbox_root.join("artifacts"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path().join("final-report.md"))
            .find(|path| path.exists())
            .expect("Gear should write a final report artifact");
        let final_report = std::fs::read_to_string(final_report).unwrap();
        assert!(!final_report.trim().is_empty());

        let goal = std::fs::read_dir(gearbox_root.join("goals"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| std::fs::read_to_string(entry.path()).unwrap())
            .find(|content| content.contains("Build a tiny notes app MVP"))
            .expect("Gear should persist the original request in the goal ledger");
        assert!(goal.contains("\"request\""));

        let plan_graph = std::fs::read_dir(gearbox_root.join("plans"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| std::fs::read_to_string(entry.path()).unwrap())
            .next()
            .expect("Gear should persist a typed PlanGraph");
        assert!(plan_graph.contains("\"source\": \"planner_model\""));
        assert!(plan_graph.contains("\"plan_hash\""));

        let worker_packet =
            std::fs::read_to_string(gearbox_root.join("workers/task_003/packet.json")).unwrap();
        assert!(worker_packet.contains("\"plan_task\""));
        assert!(worker_packet.contains("\"completion_predicates\""));
        let model_selection =
            std::fs::read_to_string(gearbox_root.join("workers/task_003/model-selection.json"))
                .unwrap();
        assert!(model_selection.contains("\"applied_model\": \"fake/fake\""));

        let lineage = std::fs::read_dir(gearbox_root.join("continuation/lineage"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| std::fs::read_to_string(entry.path()).unwrap())
            .next()
            .expect("Gear should persist WorkLineage");
        assert!(lineage.contains("\"plan_remaining_items\": 0"));
        assert!(!lineage.contains("\"task_003\""));
    }

    #[gpui::test]
    async fn gearbox_native_worker_lifecycle(cx: &mut TestAppContext) {
        init_test(cx);

        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("README.md"), "# Gear test\n").unwrap();
        std::fs::write(
            workspace.path().join("package.json"),
            r#"{"scripts":{"build":"echo build-ok"}}"#,
        )
        .unwrap();

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [Path::new("/a")], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs, cx));
        agent.update(cx, |agent, _cx| {
            agent.gear_worker_config_override = Some(native_gear_test_worker_config());
        });
        let connection = Rc::new(NativeAgentConnection::gear(agent.clone()));

        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[workspace.path()]),
                    cx,
                )
            })
            .await
            .unwrap();

        let events: Arc<Mutex<Vec<String>>> = Default::default();
        let session_id = cx.update(|cx| acp_thread.read(cx).session_id().clone());
        connection.agent().update(cx, |connection_agent, _cx| {
            if let Some(session) = connection_agent.sessions.get_mut(&session_id) {
                session.gear_lifecycle_events = Some(events.clone());
            }
        });

        let model = cx.update(|cx| {
            LanguageModelRegistry::read_global(cx)
                .default_model()
                .map(|default_model| default_model.model)
                .expect("default test model should be available")
        });
        let fake_model = model.as_fake();

        let prompt_task = cx.update(|cx| {
            acp_thread.update(cx, |thread, cx| {
                thread.send(vec!["Build a tiny notes app MVP".into()], cx)
            })
        });
        let prompt_task = cx.foreground_executor().spawn(prompt_task);
        wait_for_fake_completion(fake_model, cx).await;
        let planner_draft = gearbox_agent::plan_graph::deterministic_fallback_draft(
            "Build a tiny notes app MVP",
            &gearbox_agent::state::Scope::new(Vec::new(), vec![".git".to_string()], 10),
            &["npm run build".to_string()],
        );
        fake_model
            .send_last_completion_stream_text_chunk(serde_json::to_string(&planner_draft).unwrap());
        fake_model.end_last_completion_stream();
        let gear_finished = Arc::new(AtomicBool::new(false));
        let worker_responder = respond_to_fake_completions(model, gear_finished.clone());
        cx.executor().allow_parking();
        prompt_task.await.unwrap();
        gear_finished.store(true, Ordering::SeqCst);
        assert_eq!(
            worker_responder.join().unwrap(),
            4,
            "Gear should run planner/PlanCritic/independent Oracle, execute one native implementation worker, and run one final reviewer"
        );
        cx.run_until_parked();

        let event_log = events.lock().expect("event log lock");

        let gearbox_root = workspace.path().join(".gear");
        let _final_report = std::fs::read_dir(gearbox_root.join("artifacts"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path().join("final-report.md"))
            .find(|path| path.exists())
            .expect("Gear should write a final report artifact");

        // Verify lifecycle events from the GPUI foreground executor.
        // Background thread events (tick, dispatch_running_task,
        // TaskManager::Drop) are NOT recorded because the GPUI test
        // scheduler forbids cross-thread Arc operations.
        assert_eq!(
            event_log.first().map(|s| s.as_str()),
            Some("dispatcher:start"),
            "First event should be dispatcher:start"
        );
        assert!(
            event_log.contains(&"dispatcher:receive".to_string()),
            "dispatcher:receive should appear in the event log"
        );
    }

    #[gpui::test]
    async fn test_native_zed_worker_reuses_session_for_follow_up_and_steer(
        cx: &mut TestAppContext,
    ) {
        use gearbox_agent::state::{
            Scope, StateStore, Task as GearTask, TaskInputs, TaskKind, TaskOutputs, TaskStatus,
        };

        init_test(cx);

        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(
            workspace.path().join("README.md"),
            "# Native Gear worker test\n",
        )
        .unwrap();
        let store = StateStore::new(workspace.path());
        store.initialize().unwrap();

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [Path::new("/a")], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs, cx));
        let connection = Rc::new(NativeAgentConnection::gear(agent.clone()));
        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[workspace.path()]),
                    cx,
                )
            })
            .await
            .unwrap();
        let parent_session_id = cx.update(|cx| acp_thread.read(cx).session_id().clone());
        let model = cx.update(|cx| {
            LanguageModelRegistry::read_global(cx)
                .default_model()
                .map(|default_model| default_model.model)
                .expect("default test model should be available")
        });
        let fake_model = model.as_fake();

        let (native_worker_tx, native_worker_rx) =
            async_channel::unbounded::<GearZedWorkerDispatch>();
        cx.update(|cx| {
            spawn_gear_zed_worker_dispatcher(
                agent.downgrade(),
                parent_session_id,
                native_worker_rx,
                Arc::new(Mutex::new(HashMap::default())),
                #[cfg(test)]
                None,
                cx,
            );
        });
        let backend = GearZedWorkerBackend::new(native_worker_tx);
        let task = GearTask {
            id: "task_native_zed_follow_up".to_string(),
            goal_id: "goal_native_zed_follow_up".to_string(),
            parent_task_id: None,
            title: "native zed follow up".to_string(),
            kind: TaskKind::Edit,
            status: TaskStatus::Pending,
            assigned_worker: Some("zed_agent".to_string()),
            attempt: 1,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: TaskInputs::default(),
            outputs: TaskOutputs::default(),
        };
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
            require_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let handle = backend
            .start_zed_agent(WorkerStartRequest {
                store: &store,
                workspace: workspace.path(),
                task: &task,
                route_attempt: 0,
                goal: "Use a native Zed worker and then refine the result.",
                verification_commands: &[],
                config: &config,
                cancellation_token: None,
                coordinator_model: None,
                coordinator_brief: None,
                route_hint: None,
            })
            .unwrap();

        wait_for_fake_completion(fake_model, cx).await;
        let first_session_id = handle
            .session_id()
            .expect("native zed worker should expose its session id after first prompt starts");
        handle
            .send_follow_up(
                "Refine the worker result without opening a new worker task.".to_string(),
            )
            .unwrap();
        fake_model.send_last_completion_stream_text_chunk("first worker response");
        fake_model.end_last_completion_stream();

        wait_for_fake_completion(fake_model, cx).await;
        assert_eq!(
            handle.session_id().as_deref(),
            Some(first_session_id.as_str())
        );
        handle
            .steer("Steer the same worker session into a final review pass.".to_string())
            .unwrap();
        fake_model.send_last_completion_stream_text_chunk("second worker response");
        fake_model.end_last_completion_stream();

        wait_for_fake_completion(fake_model, cx).await;
        assert_eq!(
            handle.session_id().as_deref(),
            Some(first_session_id.as_str())
        );

        let result_waiter = std::thread::spawn({
            let handle = handle.clone();
            move || handle.wait_for_result()
        });
        fake_model.send_last_completion_stream_text_chunk("third worker response");
        fake_model.end_last_completion_stream();
        for _ in 0..100 {
            cx.run_until_parked();
            if result_waiter.is_finished() {
                break;
            }
            cx.background_executor
                .timer(Duration::from_millis(10))
                .await;
        }
        assert!(result_waiter.is_finished());
        let result = result_waiter.join().unwrap().unwrap();

        assert_eq!(result.status, WorkerStatus::Succeeded);
        assert_eq!(result.summary, "third worker response");
        assert_eq!(result.prompt_path.file_name().unwrap(), "steer-2.md");
        assert!(store.worker_dir(&task.id).join("follow-up-1.md").exists());
        assert!(store.worker_dir(&task.id).join("steer-2.md").exists());
        let last_message = std::fs::read_to_string(
            result
                .last_message_path
                .expect("native zed worker should persist the final assistant message"),
        )
        .unwrap();
        assert!(last_message.contains("third worker response"));
    }

    #[gpui::test]
    async fn test_gear_prompt_greeting_does_not_start_orchestrator(cx: &mut TestAppContext) {
        init_test(cx);

        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("README.md"), "# Gear greeting test\n").unwrap();

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [Path::new("/a")], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs, cx));
        let connection = Rc::new(NativeAgentConnection::gear(agent));

        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[workspace.path()]),
                    cx,
                )
            })
            .await
            .unwrap();
        let prompt_task = cx.update(|cx| {
            acp_thread.update(cx, |thread, cx| thread.send(vec!["你好。".into()], cx))
        });
        prompt_task.await.unwrap();
        cx.run_until_parked();

        assert!(
            !workspace.path().join(".gearbox-agent").exists(),
            "Gear should not create runtime artifacts for a greeting"
        );
    }

    #[gpui::test]
    async fn test_threads_flushed_to_database_on_app_quit(cx: &mut TestAppContext) {
        init_test(cx);

        let (connection, agent, project, acp_thread) = setup_native_agent_session(cx).await;
        let session_id = cx.update(|cx| acp_thread.read(cx).session_id().clone());
        let thread = cx.update(|cx| native_thread_for_session(&agent, &session_id, cx));

        // A second session whose thread stays empty must be skipped by the
        // quit flush rather than persisted as an empty row.
        let empty_acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/a")]),
                    cx,
                )
            })
            .await
            .unwrap();
        let empty_session_id = cx.update(|cx| empty_acp_thread.read(cx).session_id().clone());

        // Give the first thread content so it's no longer an empty draft, plus
        // an in-progress draft prompt that the flush must capture.
        cx.update(|cx| {
            let path_style = project.read(cx).path_style(cx);
            thread.update(cx, |thread, cx| {
                thread.push_acp_user_block(
                    ClientUserMessageId::new(),
                    [acp::ContentBlock::from("hello from the user")],
                    path_style,
                    cx,
                );
            });
            acp_thread.update(cx, |acp_thread, cx| {
                acp_thread
                    .set_draft_prompt(Some(vec![acp::ContentBlock::from("draft in progress")]), cx);
            });
        });
        cx.run_until_parked();

        // Reproduce the orphaned state from the bug: the sidebar metadata and
        // serialized panel still reference the session, but the per-session
        // async content save never landed, so the content row is absent.
        let database = cx.update(|cx| ThreadsDatabase::connect(cx)).await.unwrap();
        database.delete_thread(session_id.clone()).await.unwrap();
        assert!(
            database
                .load_thread(session_id.clone())
                .await
                .unwrap()
                .is_none(),
            "precondition: content row should be missing before the quit flush"
        );

        // Quit through the real shutdown path so the `on_app_quit`
        // registration is exercised, not just the flush itself.
        cx.update(|cx| cx.shutdown());

        let restored = database
            .load_thread(session_id.clone())
            .await
            .unwrap()
            .expect("thread content should be persisted to the database on quit");
        assert_eq!(
            restored.messages.len(),
            1,
            "the user message should survive the quit flush"
        );
        assert_eq!(
            restored.draft_prompt,
            Some(vec![acp::ContentBlock::from("draft in progress")]),
            "the current draft prompt should be captured by the quit flush"
        );
        assert!(
            database
                .load_thread(empty_session_id)
                .await
                .unwrap()
                .is_none(),
            "empty threads should not be persisted by the quit flush"
        );
    }

    #[test]
    fn test_ambiguous_mcp_prompt_names() {
        // Reserving the built-in `/compact` forces a same-named MCP prompt to be
        // server-qualified so it stays reachable; unique names stay bare.
        let ambiguous = ambiguous_mcp_prompt_names([COMPACT_COMMAND_NAME], ["compact", "deploy"]);
        assert!(ambiguous.contains("compact"));
        assert!(!ambiguous.contains("deploy"));

        // Without the reservation, a unique MCP prompt is left bare.
        let ambiguous = ambiguous_mcp_prompt_names([], ["compact", "deploy"]);
        assert!(ambiguous.is_empty());

        // Two MCP prompts sharing a name are both qualified regardless of
        // reservation.
        let ambiguous = ambiguous_mcp_prompt_names([], ["dup", "dup", "unique"]);
        assert!(ambiguous.contains("dup"));
        assert!(!ambiguous.contains("unique"));
    }

    #[test]
    fn test_qualified_compact_commands_are_not_native_compact() {
        let unqualified_blocks = [acp::ContentBlock::from("/compact")];
        let unqualified = Command::parse(&unqualified_blocks).unwrap();
        assert!(unqualified.is_unqualified("compact"));

        let mcp_blocks = [acp::ContentBlock::from("/server.compact")];
        let mcp_qualified = Command::parse(&mcp_blocks).unwrap();
        assert_eq!(mcp_qualified.prompt_name, "compact");
        assert_eq!(mcp_qualified.explicit_server_id, Some("server"));
        assert!(!mcp_qualified.is_unqualified("compact"));

        let skill_blocks = [acp::ContentBlock::from("/:compact")];
        let skill_qualified = Command::parse(&skill_blocks).unwrap();
        assert_eq!(skill_qualified.prompt_name, "compact");
        assert_eq!(skill_qualified.skill_scope, Some(""));
        assert!(!skill_qualified.is_unqualified("compact"));
    }

    fn make_project_skill(name: &str, description: &str, worktree: &str) -> Skill {
        Skill {
            name: name.to_string(),
            description: description.to_string(),
            source: SkillSource::ProjectLocal {
                worktree_id: SkillScopeId(1),
                worktree_root_name: worktree.into(),
            },
            directory_path: PathBuf::from(format!("/{worktree}/.agents/skills/{name}")),
            skill_file_path: PathBuf::from(format!("/{worktree}/.agents/skills/{name}/SKILL.md")),
            load_warnings: Vec::new(),
            disable_model_invocation: false,
            embedded_body: None,
        }
    }

    fn make_builtin_skill(name: &str, description: &str) -> Skill {
        Skill {
            name: name.to_string(),
            description: description.to_string(),
            source: SkillSource::BuiltIn,
            directory_path: PathBuf::from(format!("/builtin/{name}")),
            skill_file_path: PathBuf::from(format!("/builtin/{name}/SKILL.md")),
            load_warnings: Vec::new(),
            disable_model_invocation: false,
            embedded_body: Some("built-in body"),
        }
    }

    /// Filter to only user-defined (non-built-in) skills for test assertions.
    fn user_skills(skills: &[Skill]) -> Vec<&Skill> {
        skills
            .iter()
            .filter(|s| !matches!(s.source, SkillSource::BuiltIn))
            .collect()
    }

    #[test]
    fn test_combine_skills_keeps_every_entry_for_autocomplete() {
        // The autocomplete popup needs both same-named entries so the
        // source label can disambiguate them. `combine_skills` must not
        // drop the global when a project-local shares its name.
        let global = make_global_skill("review", "Global review");
        let project = make_project_skill("review", "Project review", "project");

        let (skills, errors) = combine_skills(vec![Ok(global)], vec![Ok(project)].into_iter());

        assert!(errors.is_empty());
        let user = user_skills(&skills);
        assert_eq!(user.len(), 2);
        assert!(matches!(user[0].source, SkillSource::Global));
        assert!(matches!(user[1].source, SkillSource::ProjectLocal { .. }));
    }

    #[test]
    fn test_apply_skill_overrides_project_wins_over_global() {
        // The model-facing projection collapses the same name to a
        // single entry, with the project-local winning. This is what
        // `select_catalog_skills`, `SkillTool`, and the slash-command
        // resolver all see.
        let global = make_global_skill("review", "Global review");
        let project = make_project_skill("review", "Project review", "project");

        let resolved = apply_skill_overrides(&[global, project]);

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].description, "Project review");
        assert!(matches!(
            resolved[0].source,
            SkillSource::ProjectLocal { .. }
        ));
    }

    #[test]
    fn test_apply_skill_overrides_same_source_collision_keeps_first() {
        // Two globals (or two project-locals from different worktrees)
        // colliding don't have a clear winner; preserve the historical
        // "first one wins" behavior.
        let first = make_global_skill("review", "First");
        let second = make_global_skill("review", "Second");

        let resolved = apply_skill_overrides(&[first, second]);

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].description, "First");
    }

    #[test]
    fn test_apply_skill_overrides_global_wins_over_builtin() {
        // A global skill with the same name as a built-in must shadow
        // the built-in in the model-facing projection, regardless of
        // iteration order.
        let built_in = make_builtin_skill("create-skill", "Built-in version");
        let global = make_global_skill("create-skill", "User override");

        let resolved = apply_skill_overrides(&[built_in, global]);

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].description, "User override");
        assert!(matches!(resolved[0].source, SkillSource::Global));
    }

    #[test]
    fn test_apply_skill_overrides_project_wins_over_builtin() {
        let built_in = make_builtin_skill("create-skill", "Built-in version");
        let project = make_project_skill("create-skill", "Project override", "my-project");

        let resolved = apply_skill_overrides(&[built_in, project]);

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].description, "Project override");
        assert!(matches!(
            resolved[0].source,
            SkillSource::ProjectLocal { .. }
        ));
    }

    #[test]
    fn test_apply_skill_overrides_project_wins_over_builtin_and_global() {
        // All three sources present — the project-local must win and
        // both lower-precedence entries must be dropped from the
        // model-facing projection.
        let built_in = make_builtin_skill("create-skill", "Built-in");
        let global = make_global_skill("create-skill", "Global");
        let project = make_project_skill("create-skill", "Project", "my-project");

        let resolved = apply_skill_overrides(&[built_in, global, project]);

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].description, "Project");
    }

    #[test]
    fn test_apply_skill_overrides_preserves_unique_skills() {
        let global_a = make_global_skill("alpha", "a");
        let global_b = make_global_skill("beta", "b");
        let project_c = make_project_skill("gamma", "c", "project");

        let resolved = apply_skill_overrides(&[global_a, global_b, project_c]);

        assert_eq!(resolved.len(), 3);
        let names: Vec<&str> = resolved.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn test_skill_source_scope_prefix_and_matches_scope() {
        // The popup inserts `/<prefix>:<name>` using `scope_prefix`,
        // and the resolver routes via `matches_scope`. This test pins
        // the contract that the two stay in sync.
        let global = SkillSource::Global;
        // Globals use an empty prefix, so the popup inserts `/:<name>`.
        assert_eq!(global.scope_prefix(), "");
        assert!(global.matches_scope(""));
        // Hand-typed `/global:<name>` is not aliased to the global
        // source; it looks for a worktree literally named `global`.
        assert!(!global.matches_scope("global"));
        assert!(!global.matches_scope("zed"));

        let project = SkillSource::ProjectLocal {
            worktree_id: SkillScopeId(1),
            worktree_root_name: "zed".into(),
        };
        // Project-local skills are scoped by their worktree root name
        // so multiple open worktrees with same-named skills can each
        // be addressed unambiguously.
        assert_eq!(project.scope_prefix(), "zed");
        assert!(project.matches_scope("zed"));
        // The empty scope is reserved for globals.
        assert!(!project.matches_scope(""));
        // An unrelated worktree name (or MCP server name) must not
        // match a project skill from a different worktree.
        assert!(!project.matches_scope("extensions"));

        // A worktree literally named `global` is no longer ambiguous
        // with the global source: its skills are invoked as
        // `/global:<name>` while globals are invoked as `/:<name>`.
        let project_named_global = SkillSource::ProjectLocal {
            worktree_id: SkillScopeId(2),
            worktree_root_name: "global".into(),
        };
        assert_eq!(project_named_global.scope_prefix(), "global");
        assert!(project_named_global.matches_scope("global"));
        assert!(!project_named_global.matches_scope(""));
    }

    #[test]
    fn test_select_catalog_skills_emits_issue_for_dropped_skills() {
        // Each skill's name + description occupies ~10KB. With a 50KB
        // budget, only the first ~5 visible skills fit; the rest must
        // appear as loading issues so the UI can surface them.
        let description = "x".repeat(10 * 1024);
        let mut skills = Vec::new();
        let total = 10;
        for i in 0..total {
            let name = format!("skill-{i:02}");
            skills.push(Skill {
                name: name.clone(),
                description: description.clone(),
                source: SkillSource::Global,
                directory_path: PathBuf::from(format!("/skills/{name}")),
                skill_file_path: PathBuf::from(format!("/skills/{name}/SKILL.md")),
                load_warnings: Vec::new(),
                disable_model_invocation: false,
                embedded_body: None,
            });
        }

        let (kept, issues) = select_catalog_skills(&skills);

        assert!(
            kept.len() < skills.len(),
            "some skills should be dropped due to the budget (kept {} of {})",
            kept.len(),
            skills.len(),
        );
        assert_eq!(
            issues.len(),
            1,
            "all dropped skills should be consolidated into a single issue, got {issues:?}",
        );

        let kept_size: usize = kept
            .iter()
            .map(|s| s.name.len() + s.description.len())
            .sum();
        assert!(
            kept_size <= MAX_SKILL_DESCRIPTIONS_SIZE,
            "kept skills must fit in the budget (got {kept_size} bytes)",
        );

        let issue = &issues[0];
        assert_eq!(issue.kind, SkillLoadingIssueKind::CatalogBudgetExceeded);
        assert!(
            issue.message.contains("50KB") && issue.message.contains("budget"),
            "issue message {:?} should describe the budget",
            issue.message,
        );
        assert_eq!(
            issue.path,
            skills[kept.len()].skill_file_path,
            "issue path should match the first dropped skill",
        );

        for dropped_skill in &skills[kept.len()..total] {
            let name = &dropped_skill.name;
            assert!(
                issue.message.contains(name.as_str()),
                "issue message {:?} should mention the dropped skill name {name:?}",
                issue.message,
            );
            let bullet_line = format!("- {name}");
            assert!(
                issue
                    .message
                    .lines()
                    .any(|line| line.starts_with(&bullet_line)),
                "issue message {:?} should contain a bullet line starting with {bullet_line:?}",
                issue.message,
            );
        }
    }

    #[test]
    fn test_select_catalog_skills_stops_packing_after_first_overflow() {
        // Once a model-invocable skill overflows the budget, no later
        // skills should be admitted, even if they're small enough to fit
        // in the remaining sliver. This keeps the cutoff deterministic by
        // sort order rather than dependent on individual skill sizes.
        let half_description = "a".repeat(MAX_SKILL_DESCRIPTIONS_SIZE / 2);
        let big_description = "b".repeat(MAX_SKILL_DESCRIPTIONS_SIZE);
        let small_description = "c".repeat(100);

        let first = Skill {
            name: "skill-01-first".to_string(),
            description: half_description,
            source: SkillSource::Global,
            directory_path: PathBuf::from("/skills/skill-01-first"),
            skill_file_path: PathBuf::from("/skills/skill-01-first/SKILL.md"),
            load_warnings: Vec::new(),
            disable_model_invocation: false,
            embedded_body: None,
        };
        let second = Skill {
            name: "skill-02-overflows".to_string(),
            description: big_description,
            source: SkillSource::Global,
            directory_path: PathBuf::from("/skills/skill-02-overflows"),
            skill_file_path: PathBuf::from("/skills/skill-02-overflows/SKILL.md"),
            load_warnings: Vec::new(),
            disable_model_invocation: false,
            embedded_body: None,
        };
        let third = Skill {
            name: "skill-03-would-fit".to_string(),
            description: small_description,
            source: SkillSource::Global,
            directory_path: PathBuf::from("/skills/skill-03-would-fit"),
            skill_file_path: PathBuf::from("/skills/skill-03-would-fit/SKILL.md"),
            load_warnings: Vec::new(),
            disable_model_invocation: false,
            embedded_body: None,
        };

        // Sanity-check the test setup: the third skill is small enough
        // that a greedy packer would have squeezed it in alongside the
        // first one.
        let leftover_after_first =
            MAX_SKILL_DESCRIPTIONS_SIZE - (first.name.len() + first.description.len());
        assert!(
            third.name.len() + third.description.len() <= leftover_after_first,
            "third skill must fit in the leftover sliver for this test to be meaningful",
        );

        let skills = vec![first.clone(), second.clone(), third.clone()];
        let (kept, issues) = select_catalog_skills(&skills);

        let kept_names: Vec<&str> = kept.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(kept_names, vec![first.name.as_str()]);

        assert_eq!(issues.len(), 1, "expected a single consolidated issue");
        assert_eq!(issues[0].kind, SkillLoadingIssueKind::CatalogBudgetExceeded);
        assert_eq!(issues[0].path, second.skill_file_path);
        assert!(
            issues[0].message.contains(second.name.as_str()),
            "issue message {:?} should mention {:?}",
            issues[0].message,
            second.name,
        );
        assert!(
            issues[0].message.contains(third.name.as_str()),
            "issue message {:?} should mention {:?}",
            issues[0].message,
            third.name,
        );
        assert!(
            issues[0].message.contains("- "),
            "issue message {:?} should use bullet form when multiple skills are dropped",
            issues[0].message,
        );
    }

    #[test]
    fn test_select_catalog_skills_excludes_hidden_skills_from_catalog() {
        // Hidden skills (`disable_model_invocation: true`) are slash-only and
        // must not appear in the catalog returned by `select_catalog_skills`,
        // even when they would otherwise fit in the budget. They also don't
        // count against the budget, so a hidden skill larger than the entire
        // budget shouldn't generate a loading issue or prevent later visible
        // skills from fitting.
        let huge_description = "y".repeat(MAX_SKILL_DESCRIPTIONS_SIZE * 2);
        let hidden = Skill {
            name: "hidden-huge".to_string(),
            description: huge_description,
            source: SkillSource::Global,
            directory_path: PathBuf::from("/skills/hidden-huge"),
            skill_file_path: PathBuf::from("/skills/hidden-huge/SKILL.md"),
            load_warnings: Vec::new(),
            disable_model_invocation: true,
            embedded_body: None,
        };
        let visible = Skill {
            name: "visible".to_string(),
            description: "short".to_string(),
            source: SkillSource::Global,
            directory_path: PathBuf::from("/skills/visible"),
            skill_file_path: PathBuf::from("/skills/visible/SKILL.md"),
            load_warnings: Vec::new(),
            disable_model_invocation: false,
            embedded_body: None,
        };

        let (kept, issues) = select_catalog_skills(&[hidden, visible]);

        assert!(issues.is_empty(), "expected no issues, got: {issues:?}");
        let kept_names: Vec<&str> = kept.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(kept_names, vec!["visible"]);
    }

    #[gpui::test]
    async fn test_maintaining_project_context(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            "/",
            json!({
                "a": {}
            }),
        )
        .await;
        let project = Project::test(fs.clone(), [], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        // Creating a session registers the project and triggers context building.
        let connection = NativeAgentConnection::new(agent.clone());
        let _acp_thread = cx
            .update(|cx| {
                Rc::new(connection).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        let thread = agent.read_with(cx, |agent, _cx| {
            agent.sessions.values().next().unwrap().thread.clone()
        });

        agent.read_with(cx, |agent, cx| {
            let project_id = project.entity_id();
            let state = agent.projects.get(&project_id).unwrap();
            assert_eq!(state.project_context.read(cx).worktrees, vec![]);
            assert_eq!(thread.read(cx).project_context().read(cx).worktrees, vec![]);
        });

        let worktree = project
            .update(cx, |project, cx| project.create_worktree("/a", true, cx))
            .await
            .unwrap();
        cx.run_until_parked();
        agent.read_with(cx, |agent, cx| {
            let project_id = project.entity_id();
            let state = agent.projects.get(&project_id).unwrap();
            let expected_worktrees = vec![WorktreeContext {
                root_name: "a".into(),
                abs_path: Path::new("/a").into(),
                rules_file: None,
            }];
            assert_eq!(state.project_context.read(cx).worktrees, expected_worktrees);
            assert_eq!(
                thread.read(cx).project_context().read(cx).worktrees,
                expected_worktrees
            );
        });

        // Creating `/a/.rules` updates the project context.
        fs.insert_file("/a/.rules", Vec::new()).await;
        cx.run_until_parked();
        agent.read_with(cx, |agent, cx| {
            let project_id = project.entity_id();
            let state = agent.projects.get(&project_id).unwrap();
            let rules_entry = worktree
                .read(cx)
                .entry_for_path(rel_path(".rules"))
                .unwrap();
            let expected_worktrees = vec![WorktreeContext {
                root_name: "a".into(),
                abs_path: Path::new("/a").into(),
                rules_file: Some(RulesFileContext {
                    path_in_worktree: rel_path(".rules").into(),
                    text: "".into(),
                    project_entry_id: rules_entry.id.to_usize(),
                }),
            }];
            assert_eq!(state.project_context.read(cx).worktrees, expected_worktrees);
            assert_eq!(
                thread.read(cx).project_context().read(cx).worktrees,
                expected_worktrees
            );
        });
    }

    #[gpui::test]
    async fn test_global_skills_load_and_reload(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        let skills_dir = global_skills_dir();
        let initial_skill_dir = skills_dir.join("my-skill");
        let initial_skill_path = initial_skill_dir.join("SKILL.md");
        fs.create_dir(&initial_skill_dir).await.unwrap();
        fs.insert_file(
            &initial_skill_path,
            b"---\nname: my-skill\ndescription: First version\n---\n\nbody-v1".to_vec(),
        )
        .await;

        let project = Project::test(fs.clone(), [], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        // Simulate the user-interaction trigger that the agent panel
        // fires (input focus, slash autocomplete, or submit). In tests
        // we call it directly because there's no panel.
        cx.update(|cx| {
            agent.update(cx, |agent, cx| agent.ensure_skills_scan_started(cx));
        });

        let connection = NativeAgentConnection::new(agent.clone());
        let _acp_thread = cx
            .update(|cx| {
                Rc::new(connection).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        // The pre-existing skill should be loaded into the project state.
        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project.entity_id()).unwrap();
            let user = user_skills(&state.skills);
            assert_eq!(user.len(), 1);
            assert_eq!(user[0].name, "my-skill");
            assert_eq!(user[0].description, "First version");
        });

        // Modify the SKILL.md and verify the project context refreshes.
        fs.write(
            &initial_skill_path,
            b"---\nname: my-skill\ndescription: Second version\n---\n\nbody-v2",
        )
        .await
        .unwrap();
        cx.run_until_parked();

        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project.entity_id()).unwrap();
            let user = user_skills(&state.skills);
            assert_eq!(user.len(), 1);
            assert_eq!(user[0].description, "Second version");
        });
    }

    #[gpui::test]
    async fn test_global_skill_with_long_description_loads_with_warning(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        let skills_dir = global_skills_dir();
        let skill_dir = skills_dir.join("long-description");
        let skill_path = skill_dir.join("SKILL.md");
        let long_description = "a".repeat(agent_skills::MAX_SKILL_DESCRIPTION_LEN + 1);
        fs.create_dir(&skill_dir).await.unwrap();
        fs.insert_file(
            &skill_path,
            format!("---\nname: long-description\ndescription: {long_description}\n---\n\nbody")
                .into_bytes(),
        )
        .await;

        let project = Project::test(fs.clone(), [], cx).await;
        let project_id = project.entity_id();
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        cx.update(|cx| {
            agent.update(cx, |agent, cx| agent.ensure_skills_scan_started(cx));
        });

        let connection = NativeAgentConnection::new(agent.clone());
        let acp_thread = cx
            .update(|cx| {
                Rc::new(connection.clone()).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        let loaded_skill = agent.read_with(cx, |agent, cx| {
            let state = agent.projects.get(&project_id).unwrap();
            let user = user_skills(&state.skills);
            assert_eq!(user.len(), 1);
            assert_eq!(user[0].name, "long-description");
            assert_eq!(user[0].description, long_description);

            let catalog_names: Vec<&str> = state
                .project_context
                .read(cx)
                .skills()
                .iter()
                .map(|skill| skill.name.as_str())
                .collect();
            assert!(
                catalog_names.contains(&"long-description"),
                "long-description skill should remain in the model catalog: {catalog_names:?}"
            );

            assert!(
                state.skill_loading_issues.iter().any(|issue| {
                    issue.kind == SkillLoadingIssueKind::DescriptionTooLong
                        && issue.path == skill_path
                        && issue.message.to_string().contains("1024-byte limit")
                }),
                "expected a description-length warning issue, got {:?}",
                state.skill_loading_issues
            );

            (*user[0]).clone()
        });

        let session_id = acp_thread.read_with(cx, |thread, _cx| thread.session_id().clone());
        cx.update(|cx| {
            let available_skills = connection.available_skills(&session_id, cx);
            let available_skill = available_skills
                .iter()
                .find(|skill| skill.name == "long-description")
                .expect("long-description should appear in available skills");
            assert_eq!(available_skill.description, long_description);
            assert!(
                available_skill
                    .warning
                    .as_ref()
                    .is_some_and(|warning| warning.contains("1024-byte limit")),
                "available skill should expose warning text, got {:?}",
                available_skill.warning
            );
        });

        let body = agent_skills::read_skill_body(fs.as_ref(), &loaded_skill.skill_file_path)
            .await
            .expect("body should load despite description-length warning");
        assert_eq!(body, "body");
    }

    #[gpui::test]
    async fn test_symlinked_global_skills_load_and_reload(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        let skills_dir = global_skills_dir();
        let external_skill_dir = PathBuf::from(path!("/external/my-skill"));
        let skill_link_dir = skills_dir.join("my-skill");
        let skill_link_path = skill_link_dir.join("SKILL.md");

        fs.insert_tree(
            &external_skill_dir,
            json!({
                "SKILL.md": "---\nname: my-skill\ndescription: First symlinked version\n---\n\nbody-v1"
            }),
        )
        .await;
        fs.create_dir(&skills_dir).await.unwrap();
        fs.create_symlink(&skill_link_dir, external_skill_dir)
            .await
            .unwrap();

        let project = Project::test(fs.clone(), [], cx).await;
        let project_id = project.entity_id();
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        cx.update(|cx| {
            agent.update(cx, |agent, cx| agent.ensure_skills_scan_started(cx));
        });

        let connection = NativeAgentConnection::new(agent.clone());
        let _acp_thread = cx
            .update(|cx| {
                Rc::new(connection).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        let loaded_skill = agent.read_with(cx, |agent, cx| {
            let state = agent.projects.get(&project_id).unwrap();
            let user = user_skills(&state.skills);
            assert_eq!(user.len(), 1);
            assert_eq!(user[0].name, "my-skill");
            assert_eq!(user[0].description, "First symlinked version");
            assert_eq!(user[0].source, SkillSource::Global);
            assert_eq!(user[0].skill_file_path, skill_link_path);

            let catalog_skills = state.project_context.read(cx).skills();
            let catalog_skill = catalog_skills
                .iter()
                .find(|skill| skill.name == "my-skill")
                .expect("symlinked skill should be included in the model-facing catalog");
            assert_eq!(catalog_skill.description, "First symlinked version");
            assert_eq!(
                catalog_skill.location,
                skill_link_path.to_string_lossy().as_ref()
            );

            (*user[0]).clone()
        });
        let body = agent_skills::read_skill_body(fs.as_ref(), &loaded_skill.skill_file_path)
            .await
            .unwrap();
        assert_eq!(body, "body-v1");

        fs.write(
            &skill_link_path,
            b"---\nname: my-skill\ndescription: Second symlinked version\n---\n\nbody-v2",
        )
        .await
        .unwrap();
        cx.run_until_parked();

        let reloaded_skill = agent.read_with(cx, |agent, cx| {
            let state = agent.projects.get(&project_id).unwrap();
            let user = user_skills(&state.skills);
            assert_eq!(user.len(), 1);
            assert_eq!(user[0].name, "my-skill");
            assert_eq!(user[0].description, "Second symlinked version");
            assert_eq!(user[0].source, SkillSource::Global);
            assert_eq!(user[0].skill_file_path, skill_link_path);

            let catalog_skills = state.project_context.read(cx).skills();
            let catalog_skill = catalog_skills
                .iter()
                .find(|skill| skill.name == "my-skill")
                .expect("reloaded symlinked skill should be included in the model-facing catalog");
            assert_eq!(catalog_skill.description, "Second symlinked version");
            assert_eq!(
                catalog_skill.location,
                skill_link_path.to_string_lossy().as_ref()
            );

            (*user[0]).clone()
        });
        let body = agent_skills::read_skill_body(fs.as_ref(), &reloaded_skill.skill_file_path)
            .await
            .unwrap();
        assert_eq!(body, "body-v2");
    }

    #[gpui::test]
    async fn test_global_skills_dir_created_after_startup(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        let skills_dir = global_skills_dir();

        // Intentionally do NOT pre-create `skills_dir`. The first scan
        // trigger should find no directory and leave the watch state
        // idle; a later trigger after the directory is created should
        // attach to the deepest existing ancestor and react when the
        // directory is created later.

        let project = Project::test(fs.clone(), [], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        // First scan trigger: nothing on disk yet, state stays idle.
        cx.update(|cx| {
            agent.update(cx, |agent, cx| agent.ensure_skills_scan_started(cx));
        });

        let connection = NativeAgentConnection::new(agent.clone());
        let _acp_thread = cx
            .update(|cx| {
                Rc::new(connection).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        // No skills directory exists yet, so no skills should be loaded.
        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project.entity_id()).unwrap();
            assert!(
                user_skills(&state.skills).is_empty(),
                "expected no user skills before the global skills dir exists, got {:?}",
                state.skills
            );
        });

        // Create the global skills directory and a skill within it.
        let new_skill_dir = skills_dir.join("late-skill");
        fs.create_dir(&new_skill_dir).await.unwrap();
        fs.insert_file(
            &new_skill_dir.join("SKILL.md"),
            b"---\nname: late-skill\ndescription: Created after startup\n---\n\nbody".to_vec(),
        )
        .await;

        // Fire the trigger again, simulating the user interacting with
        // the agent panel after creating the skills directory. The
        // second scan should find the directory and start the watch,
        // which refreshes project context.
        cx.update(|cx| {
            agent.update(cx, |agent, cx| agent.ensure_skills_scan_started(cx));
        });
        cx.run_until_parked();

        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project.entity_id()).unwrap();
            let user = user_skills(&state.skills);
            assert_eq!(user.len(), 1);
            assert_eq!(user[0].name, "late-skill");
            assert_eq!(user[0].description, "Created after startup");
        });
    }

    /// Regression test for the case where a skill is added (e.g. by the
    /// SKILL.md file watcher) AFTER a session is registered. The system
    /// prompt and slash-command list both read live state, so they pick
    /// up the new skill automatically. The `SkillTool` registered on the
    /// thread used to hold a stale snapshot of `state.skills` taken at
    /// thread-construction time, which meant the model would see the new
    /// skill in `<available_skills>` but get "not found" when it tried to
    /// invoke it. The fix wires the tool to a dynamic resolver closure
    /// that re-reads `state.skills` for the project on every invocation.
    #[gpui::test]
    async fn test_skills_added_after_session_visible_to_skill_tool(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        let skills_dir = global_skills_dir();

        // No skills directory exists at startup; the watcher should
        // create one and pick up SKILL.md when it's added later.
        let project = Project::test(fs.clone(), [], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        // First scan trigger: nothing on disk yet.
        cx.update(|cx| {
            agent.update(cx, |agent, cx| agent.ensure_skills_scan_started(cx));
        });

        let connection = NativeAgentConnection::new(agent.clone());
        let _acp_thread = cx
            .update(|cx| {
                Rc::new(connection).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        let project_id = project.entity_id();
        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project_id).unwrap();
            assert!(
                user_skills(&state.skills).is_empty(),
                "expected no user skills before the global skills dir exists, got {:?}",
                state.skills
            );
        });

        // Build the same resolver closure that `register_session` uses.
        // This is the production resolver factored into a helper so the
        // test can verify resolution behavior directly without setting
        // up the full tool-call plumbing (`ToolInput`,
        // `ToolCallEventStream`, authorization channel, ...).
        let resolve =
            cx.update(|_cx| super::skills_resolver_for_project(agent.downgrade(), project_id));

        // Sanity check: before any skills exist, the resolver returns an
        // empty list — NOT the snapshot that `Thread::new` would have
        // captured.
        cx.update(|cx| {
            let all = resolve(cx);
            let user: Vec<_> = all
                .iter()
                .filter(|s| !matches!(s.source, SkillSource::BuiltIn))
                .collect();
            assert!(user.is_empty());
        });

        // Now create a SKILL.md AFTER the session was registered. With
        // the old code this would be invisible to the `SkillTool`
        // because the tool held an `Arc<Vec<Skill>>` snapshot taken at
        // thread construction time.
        let new_skill_dir = skills_dir.join("my-skill");
        fs.create_dir(&new_skill_dir).await.unwrap();
        fs.insert_file(
            &new_skill_dir.join("SKILL.md"),
            b"---\nname: my-skill\ndescription: Created after session\n---\n\nbody".to_vec(),
        )
        .await;

        // Second scan trigger: now the directory exists, so the scan
        // starts the watch and refreshes project context.
        cx.update(|cx| {
            agent.update(cx, |agent, cx| agent.ensure_skills_scan_started(cx));
        });
        cx.run_until_parked();

        // `state.skills` reflects the new skill (the watcher ran).
        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project_id).unwrap();
            let user = user_skills(&state.skills);
            assert_eq!(user.len(), 1);
            assert_eq!(user[0].name, "my-skill");
        });

        // The resolver the `SkillTool` uses must see it too. This is the
        // crux of the regression test: the tool's view of skills is
        // resolved at invocation time, not at thread-construction time.
        cx.update(|cx| {
            let all = resolve(cx);
            let snapshot: Vec<_> = all
                .iter()
                .filter(|s| !matches!(s.source, SkillSource::BuiltIn))
                .collect();
            assert_eq!(
                snapshot.len(),
                1,
                "dynamic resolver should see the new skill"
            );
            assert_eq!(snapshot[0].name, "my-skill");
            assert_eq!(snapshot[0].description, "Created after session");
        });

        // And rendering the envelope through the same path the tool uses
        // produces a `<skill_content name="my-skill">` block, confirming
        // the model would see the new skill if it invoked the tool.
        let skill_for_render = cx.update(|cx| {
            let snapshot = resolve(cx);
            snapshot
                .iter()
                .find(|s| s.name == "my-skill" && !s.disable_model_invocation)
                .cloned()
                .expect("my-skill should be model-invocable")
        });
        let body = agent_skills::read_skill_body(fs.as_ref(), &skill_for_render.skill_file_path)
            .await
            .expect("skill body should load");
        let rendered = render_skill_envelope(&skill_for_render, &body);
        assert!(
            rendered.contains("<skill_content name=\"my-skill\">"),
            "rendered envelope missing skill_content tag: {rendered}"
        );
    }

    /// Subagents must inherit access to the same skills as their parent.
    /// Production wires this up in `NativeThreadEnvironment::create_subagent_thread`,
    /// which calls `agent.register_session(subagent, project_id, ...)` —
    /// `register_session` is what installs the `SkillTool` on the thread
    /// using a resolver closure keyed on `project_id`. Because the
    /// subagent shares its parent's `project_id`, both threads end up
    /// resolving skills against the same `state.skills`.
    ///
    /// This test exercises that production path directly: it creates a
    /// parent session via the agent connection, builds a subagent thread
    /// the same way `create_subagent_thread` does, and runs it through
    /// `register_session`. It then asserts that the `SkillTool` is
    /// registered on the subagent thread and that resolving against the
    /// same `project_id` produces the same skill set the parent sees.
    #[gpui::test]
    async fn test_subagent_skills_lookup_matches_parent(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        let skills_dir = global_skills_dir();
        let skill_dir = skills_dir.join("shared-skill");
        fs.create_dir(&skill_dir).await.unwrap();
        fs.insert_file(
            &skill_dir.join("SKILL.md"),
            b"---\nname: shared-skill\ndescription: A shared skill\n---\n\nbody".to_vec(),
        )
        .await;

        let project = Project::test(fs.clone(), [], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        // Open a parent session through the connection, the same way
        // production does. This triggers project-context refresh which
        // populates `state.skills` for the project.
        let connection = NativeAgentConnection::new(agent.clone());
        let _parent_acp = cx
            .update(|cx| {
                Rc::new(connection).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        let project_id = project.entity_id();

        // Sanity check: resolving against the parent's project sees the skill.
        let parent_resolve =
            cx.update(|_cx| super::skills_resolver_for_project(agent.downgrade(), project_id));
        cx.update(|cx| {
            let all = parent_resolve(cx);
            let parent_skills: Vec<_> = all
                .iter()
                .filter(|s| !matches!(s.source, SkillSource::BuiltIn))
                .collect();
            assert_eq!(parent_skills.len(), 1);
            assert_eq!(parent_skills[0].name, "shared-skill");
        });

        // Grab the parent thread out of the agent's session map. This
        // mirrors what `create_subagent_thread` does internally — it
        // looks up the parent session by `parent_session_id` and reads
        // its `project_id` to forward to `register_session`.
        let (parent_thread, parent_project_id) = agent.read_with(cx, |agent, _cx| {
            let session = agent
                .sessions
                .values()
                .next()
                .expect("parent session should exist");
            (session.thread.clone(), session.project_id)
        });
        assert_eq!(parent_project_id, project_id);

        // Build the subagent thread the same way
        // `NativeThreadEnvironment::create_subagent_thread` does.
        let subagent_thread = cx.update(|cx| cx.new(|cx| Thread::new_subagent(&parent_thread, cx)));

        // Run the subagent through the production registration path.
        // This is what installs the `SkillTool` on the thread.
        let _subagent_acp = agent.update(cx, |agent, cx| {
            agent.register_session(
                subagent_thread.clone(),
                parent_project_id,
                1,
                None,
                ZED_AGENT_ID.clone(),
                "zed".into(),
                cx,
            )
        });

        // Verify the subagent thread has the `SkillTool` installed —
        // without `register_session`, it would not.
        subagent_thread.read_with(cx, |thread, _cx| {
            assert!(thread.is_subagent());
            assert!(
                thread.has_registered_tool(SkillTool::NAME),
                "subagent should have SkillTool registered after register_session"
            );
        });

        // The subagent's `SkillTool` is wired to a resolver closure keyed
        // on the same `project_id` the parent used, so it sees the same
        // skill set. We check this by constructing an equivalent resolver
        // against the same project_id and asserting it matches.
        let subagent_resolve = cx
            .update(|_cx| super::skills_resolver_for_project(agent.downgrade(), parent_project_id));
        cx.update(|cx| {
            let all = subagent_resolve(cx);
            let subagent_skills: Vec<_> = all
                .iter()
                .filter(|s| !matches!(s.source, SkillSource::BuiltIn))
                .collect();
            assert_eq!(subagent_skills.len(), 1);
            assert_eq!(subagent_skills[0].name, "shared-skill");
        });
    }

    #[gpui::test]
    async fn test_skills_appear_as_available_skills(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        let skills_dir = global_skills_dir();

        // Two skills: one model-invocable (default), one slash-only via
        // `disable-model-invocation: true`. Both should still appear in
        // the slash menu as first-class skills.
        let visible_dir = skills_dir.join("visible-skill");
        fs.create_dir(&visible_dir).await.unwrap();
        fs.insert_file(
            &visible_dir.join("SKILL.md"),
            b"---\nname: visible-skill\ndescription: Visible skill\n---\n\nbody".to_vec(),
        )
        .await;

        let hidden_dir = skills_dir.join("deploy");
        fs.create_dir(&hidden_dir).await.unwrap();
        fs.insert_file(
            &hidden_dir.join("SKILL.md"),
            b"---\nname: deploy\ndescription: Deploy to prod\ndisable-model-invocation: true\n---\n\nbody"
                .to_vec(),
        )
        .await;

        let project = Project::test(fs.clone(), [], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        let connection = NativeAgentConnection::new(agent.clone());
        let acp_thread = cx
            .update(|cx| {
                Rc::new(connection.clone()).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        let project_id = project.entity_id();
        let session_id = acp_thread.read_with(cx, |thread, _cx| thread.session_id().clone());

        agent.read_with(cx, |agent, cx| {
            let commands = NativeAgent::build_available_commands_for_project(
                agent.projects.get(&project_id),
                cx,
            );
            let names: Vec<&str> = commands.iter().map(|c| c.name.as_str()).collect();
            assert!(
                !names.contains(&"visible-skill"),
                "skills should not be exposed as ACP slash commands: {names:?}"
            );
            assert!(
                !names.contains(&"deploy"),
                "slash-only skills should not be exposed as ACP slash commands: {names:?}"
            );
        });

        cx.update(|cx| {
            let skills = connection.available_skills(&session_id, cx);
            let names: Vec<&str> = skills.iter().map(|skill| skill.name.as_str()).collect();
            assert!(
                names.contains(&"visible-skill"),
                "visible skill missing from available skills: {names:?}"
            );
            assert!(
                names.contains(&"deploy"),
                "slash-only skill missing from available skills: {names:?}"
            );
        });

        // The model's catalog (ProjectContext.skills) should NOT include
        // `deploy` since it has disable_model_invocation set.
        agent.read_with(cx, |agent, cx| {
            let state = agent.projects.get(&project_id).unwrap();
            let catalog: Vec<&str> = state
                .project_context
                .read(cx)
                .skills()
                .iter()
                .map(|s| s.name.as_str())
                .collect();
            assert!(
                catalog.contains(&"visible-skill"),
                "visible skill missing from catalog: {catalog:?}"
            );
            assert!(
                !catalog.contains(&"deploy"),
                "deploy should be excluded from catalog: {catalog:?}"
            );
        });
    }

    #[gpui::test]
    async fn test_project_skills_require_worktree_trust(cx: &mut TestAppContext) {
        use collections::{HashMap, HashSet};
        use project::trusted_worktrees::{self, PathTrust, TrustedWorktrees};

        init_test(cx);
        cx.update(|cx| {
            // The trust global isn't created by `init_test`. We need it
            // for `Project::test_with_worktree_trust` to actually wire up
            // trust tracking and for our subscription in
            // `register_project_with_initial_context` to fire.
            trusted_worktrees::init(HashMap::default(), cx);
        });

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            "/project",
            json!({
                ".agents": {
                    "skills": {
                        "my-skill": {
                            "SKILL.md": "---\nname: my-skill\ndescription: A project skill\n---\n\nbody"
                        }
                    }
                }
            }),
        )
        .await;

        // `test_with_worktree_trust` initializes the trust system and
        // starts every worktree as restricted, mirroring production
        // behavior on a freshly opened folder.
        let project =
            Project::test_with_worktree_trust(fs.clone(), [Path::new("/project")], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        let connection = NativeAgentConnection::new(agent.clone());
        let acp_thread = cx
            .update(|cx| {
                Rc::new(connection.clone()).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/project")]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        let project_id = project.entity_id();
        let session_id = acp_thread.read_with(cx, |thread, _cx| thread.session_id().clone());
        let worktree_id = project.read_with(cx, |project, cx| {
            project.worktrees(cx).next().unwrap().read(cx).id()
        });

        // Untrusted: project skills are excluded from the loaded list and
        // never make it into the catalog or slash commands.
        agent.read_with(cx, |agent, cx| {
            let state = agent.projects.get(&project_id).unwrap();
            assert!(
                user_skills(&state.skills).is_empty(),
                "untrusted worktree skills should not load: {:?}",
                state
                    .skills
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<Vec<_>>()
            );
            let commands = NativeAgent::build_available_commands_for_project(Some(state), cx);
            let names: Vec<&str> = commands.iter().map(|c| c.name.as_str()).collect();
            assert!(
                !names.contains(&"my-skill"),
                "untrusted skill leaked into slash commands: {names:?}"
            );
        });

        // Granting trust should trigger a context refresh; the skill then
        // appears in both the catalog and the slash-command list.
        cx.update(|cx| {
            let trusted_worktrees = TrustedWorktrees::try_get_global(cx)
                .expect("trusted worktrees global initialized by test_with_worktree_trust");
            trusted_worktrees.update(cx, |trusted_worktrees, cx| {
                trusted_worktrees.trust(
                    &project.read(cx).worktree_store(),
                    HashSet::from_iter([PathTrust::Worktree(worktree_id)]),
                    cx,
                );
            });
        });
        cx.run_until_parked();

        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project_id).unwrap();
            let user = user_skills(&state.skills);
            let names: Vec<&str> = user.iter().map(|s| s.name.as_str()).collect();
            assert_eq!(names, vec!["my-skill"]);
        });

        cx.update(|cx| {
            let skills = connection.available_skills(&session_id, cx);
            let skill_names: Vec<&str> = skills.iter().map(|s| s.name.as_str()).collect();
            assert!(
                skill_names.contains(&"my-skill"),
                "trusted skill should appear in available skills: {skill_names:?}"
            );
        });
    }

    /// Open a session against a freshly created project and trust its only
    /// worktree, so project-local skills load. Returns the agent, the
    /// project, and the worktree id of the project root.
    async fn open_trusted_project_skills(
        cx: &mut TestAppContext,
        fs: Arc<FakeFs>,
        root: &str,
    ) -> (Entity<NativeAgent>, Entity<Project>, WorktreeId) {
        use collections::{HashMap, HashSet};
        use project::trusted_worktrees::{self, PathTrust, TrustedWorktrees};

        cx.update(|cx| {
            trusted_worktrees::init(HashMap::default(), cx);
        });

        let project = Project::test_with_worktree_trust(fs.clone(), [Path::new(root)], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));

        let connection = NativeAgentConnection::new(agent.clone());
        let _acp_thread = cx
            .update(|cx| {
                Rc::new(connection).new_session(
                    project.clone(),
                    PathList::new(&[Path::new(root)]),
                    cx,
                )
            })
            .await
            .unwrap();
        cx.run_until_parked();

        let worktree_id = project.read_with(cx, |project, cx| {
            project.worktrees(cx).next().unwrap().read(cx).id()
        });
        cx.update(|cx| {
            let trusted_worktrees = TrustedWorktrees::try_get_global(cx)
                .expect("trusted worktrees global initialized by test_with_worktree_trust");
            trusted_worktrees.update(cx, |trusted_worktrees, cx| {
                trusted_worktrees.trust(
                    &project.read(cx).worktree_store(),
                    HashSet::from_iter([PathTrust::Worktree(worktree_id)]),
                    cx,
                );
            });
        });
        cx.run_until_parked();

        (agent, project, worktree_id)
    }

    /// The body resolver for a project-local skill must read the file
    /// through a project buffer rather than the local filesystem. This is
    /// what makes project skills resolvable in remote workspaces, where
    /// the `fs` the agent holds is the client's filesystem and not where
    /// the project files actually live. We prove the buffer path is used
    /// by editing the buffer in memory (without saving) and asserting the
    /// resolver returns the edited body, not the on-disk body.
    #[gpui::test]
    async fn test_project_skill_body_resolves_through_buffer(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            "/project",
            json!({
                ".agents": {
                    "skills": {
                        "my-skill": {
                            "SKILL.md": "---\nname: my-skill\ndescription: A project skill\n---\n\ndisk body"
                        }
                    }
                }
            }),
        )
        .await;

        let (agent, project, worktree_id) =
            open_trusted_project_skills(cx, fs.clone(), "/project").await;
        let project_id = project.entity_id();

        let skill = agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project_id).unwrap();
            user_skills(&state.skills)
                .into_iter()
                .find(|s| s.name == "my-skill")
                .cloned()
                .expect("project skill should be loaded")
        });
        assert!(matches!(skill.source, SkillSource::ProjectLocal { .. }));

        let resolver =
            cx.update(|_cx| super::skill_body_resolver_for_project(project.clone(), fs.clone()));

        let body = cx
            .update(|cx| resolver(skill.clone(), &mut cx.to_async()))
            .await
            .unwrap();
        assert_eq!(body, "disk body");

        // Edit the buffer in memory without writing to disk.
        let relative_path: Arc<RelPath> = rel_path(".agents/skills/my-skill/SKILL.md").into();
        let buffer = project
            .update(cx, |project, cx| {
                project.open_buffer((worktree_id, relative_path), cx)
            })
            .await
            .unwrap();
        buffer.update(cx, |buffer, cx| {
            buffer.set_text(
                "---\nname: my-skill\ndescription: A project skill\n---\n\nedited body",
                cx,
            );
        });

        let body = cx
            .update(|cx| resolver(skill.clone(), &mut cx.to_async()))
            .await
            .unwrap();
        assert_eq!(
            body, "edited body",
            "resolver must read the in-memory buffer, not the on-disk file"
        );
    }

    /// A project SKILL.md whose on-disk size exceeds the cap must be
    /// rejected with a size-limit error and excluded from the loaded
    /// skills, exercising the size guard in `load_project_skills`.
    #[gpui::test]
    async fn test_oversized_project_skill_reports_error(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        let oversized = format!(
            "---\nname: huge-skill\ndescription: Too big\n---\n\n{}",
            "a".repeat(MAX_SKILL_FILE_SIZE + 1)
        );
        fs.insert_tree(
            "/project",
            json!({
                ".agents": { "skills": { "huge-skill": { "SKILL.md": oversized } } }
            }),
        )
        .await;

        let (agent, project, _worktree_id) =
            open_trusted_project_skills(cx, fs.clone(), "/project").await;
        let project_id = project.entity_id();

        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project_id).unwrap();
            assert!(
                user_skills(&state.skills).is_empty(),
                "oversized skill must not load: {:?}",
                user_skills(&state.skills)
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<Vec<_>>()
            );
            assert!(
                state
                    .skill_loading_issues
                    .iter()
                    .any(|issue| issue.kind == SkillLoadingIssueKind::LoadFailed
                        && issue.message.to_string().contains("maximum size")),
                "expected a size-limit error, got {:?}",
                state.skill_loading_issues
            );
        });
    }

    /// A malformed project SKILL.md must surface a per-skill load error
    /// without preventing sibling skills in the same worktree from
    /// loading.
    #[gpui::test]
    async fn test_malformed_project_skill_reports_error(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            "/project",
            json!({
                ".agents": {
                    "skills": {
                        "good": {
                            "SKILL.md": "---\nname: good\ndescription: Fine\n---\n\nbody"
                        },
                        "bad": {
                            "SKILL.md": "this file has no frontmatter"
                        }
                    }
                }
            }),
        )
        .await;

        let (agent, project, _worktree_id) =
            open_trusted_project_skills(cx, fs.clone(), "/project").await;
        let project_id = project.entity_id();

        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project_id).unwrap();
            let names: Vec<&str> = user_skills(&state.skills)
                .iter()
                .map(|s| s.name.as_str())
                .collect();
            assert_eq!(names, vec!["good"], "only the valid skill should load");
            assert!(
                state
                    .skill_loading_issues
                    .iter()
                    .any(|issue| issue.kind == SkillLoadingIssueKind::LoadFailed
                        && issue.path.ends_with("bad/SKILL.md")),
                "expected an error for the malformed skill, got {:?}",
                state.skill_loading_issues
            );
        });
    }

    /// The skill catalog (metadata) is also loaded through project
    /// buffers, and the broadened `.agents` refresh trigger must rebuild
    /// it when files under `.agents` change. We edit the SKILL.md buffer
    /// in memory, then touch an unrelated file directly under `.agents`
    /// (not under `.agents/skills`) and assert the catalog reflects the
    /// in-memory edit. Under the previous `.agents/skills`-only trigger
    /// this refresh would not have fired.
    #[gpui::test]
    async fn test_project_skill_metadata_refreshes_from_buffer(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            "/project",
            json!({
                ".agents": {
                    "skills": {
                        "my-skill": {
                            "SKILL.md": "---\nname: my-skill\ndescription: Original\n---\n\nbody"
                        }
                    }
                }
            }),
        )
        .await;

        let (agent, project, worktree_id) =
            open_trusted_project_skills(cx, fs.clone(), "/project").await;
        let project_id = project.entity_id();

        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project_id).unwrap();
            let skill = user_skills(&state.skills)
                .into_iter()
                .find(|s| s.name == "my-skill")
                .expect("skill should be loaded");
            assert_eq!(skill.description, "Original");
        });

        let relative_path: Arc<RelPath> = rel_path(".agents/skills/my-skill/SKILL.md").into();
        let buffer = project
            .update(cx, |project, cx| {
                project.open_buffer((worktree_id, relative_path), cx)
            })
            .await
            .unwrap();
        buffer.update(cx, |buffer, cx| {
            buffer.set_text(
                "---\nname: my-skill\ndescription: Edited in buffer\n---\n\nbody",
                cx,
            );
        });

        // Touch a file directly under `.agents` (not under
        // `.agents/skills`) to trigger the broadened refresh path.
        fs.insert_file("/project/.agents/marker.txt", b"hello".to_vec())
            .await;
        cx.run_until_parked();

        agent.read_with(cx, |agent, _cx| {
            let state = agent.projects.get(&project_id).unwrap();
            let skill = user_skills(&state.skills)
                .into_iter()
                .find(|s| s.name == "my-skill")
                .expect("skill should still be loaded");
            assert_eq!(
                skill.description, "Edited in buffer",
                "catalog must reflect the in-memory buffer after a refresh"
            );
        });
    }

    #[gpui::test]
    async fn test_listing_models(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {}  })).await;
        let project = Project::test(fs.clone(), [], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let connection = NativeAgentConnection::new(
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx)),
        );

        // Create a thread/session
        let acp_thread = cx
            .update(|cx| {
                Rc::new(connection.clone()).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/a")]),
                    cx,
                )
            })
            .await
            .unwrap();

        let session_id = cx.update(|cx| acp_thread.read(cx).session_id().clone());

        let models = cx
            .update(|cx| {
                connection
                    .model_selector(&session_id)
                    .unwrap()
                    .list_models(cx)
            })
            .await
            .unwrap();

        let acp_thread::AgentModelList::Grouped(models) = models else {
            panic!("Unexpected model group");
        };
        assert_eq!(
            models,
            IndexMap::from_iter([(
                AgentModelGroupName("Fake".into()),
                vec![AgentModelInfo {
                    id: AgentModelId::new("fake/fake"),
                    name: "Fake".into(),
                    description: None,
                    icon: Some(acp_thread::AgentModelIcon::Named(
                        ui::IconName::ZedAssistant
                    )),
                    is_latest: false,
                    disabled: None,
                    cost: None,
                }]
            )])
        );
    }

    #[gpui::test]
    async fn test_model_selection_persists_to_settings(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.create_dir(paths::settings_file().parent().unwrap())
            .await
            .unwrap();
        fs.insert_file(
            paths::settings_file(),
            json!({
                "agent": {
                    "default_model": {
                        "provider": "foo",
                        "model": "bar"
                    }
                }
            })
            .to_string()
            .into_bytes(),
        )
        .await;
        let project = Project::test(fs.clone(), [], cx).await;

        let thread_store = cx.new(|cx| ThreadStore::new(cx));

        // Create the agent and connection
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));
        let connection = NativeAgentConnection::new(agent.clone());

        // Create a thread/session
        let acp_thread = cx
            .update(|cx| {
                Rc::new(connection.clone()).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/a")]),
                    cx,
                )
            })
            .await
            .unwrap();

        let session_id = cx.update(|cx| acp_thread.read(cx).session_id().clone());

        // Select a model
        let selector = connection.model_selector(&session_id).unwrap();
        let model_id = AgentModelId::new("fake/fake");
        cx.update(|cx| selector.select_model(model_id.clone(), cx))
            .await
            .unwrap();

        // Verify the thread has the selected model
        agent.read_with(cx, |agent, _| {
            let session = agent.sessions.get(&session_id).unwrap();
            session.thread.read_with(cx, |thread, _| {
                assert_eq!(thread.model().unwrap().id().0, "fake");
            });
        });

        cx.run_until_parked();

        // Verify settings file was updated
        let settings_content = fs.load(paths::settings_file()).await.unwrap();
        let settings_json: serde_json::Value = serde_json::from_str(&settings_content).unwrap();

        // Check that the agent settings contain the selected model
        assert_eq!(
            settings_json["agent"]["default_model"]["model"],
            json!("fake")
        );
        assert_eq!(
            settings_json["agent"]["default_model"]["provider"],
            json!("fake")
        );

        // Register a thinking model and select it.
        cx.update(|cx| {
            let thinking_model = Arc::new(FakeLanguageModel::with_id_and_thinking(
                "fake-corp",
                "fake-thinking",
                "Fake Thinking",
                true,
            ));
            let thinking_provider = Arc::new(
                FakeLanguageModelProvider::new(
                    LanguageModelProviderId::from("fake-corp".to_string()),
                    LanguageModelProviderName::from("Fake Corp".to_string()),
                )
                .with_models(vec![thinking_model]),
            );
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.register_provider(thinking_provider, cx);
            });
        });
        agent.update(cx, |agent, cx| agent.models.refresh_list(cx));

        let selector = connection.model_selector(&session_id).unwrap();
        cx.update(|cx| selector.select_model(AgentModelId::new("fake-corp/fake-thinking"), cx))
            .await
            .unwrap();
        cx.run_until_parked();

        // Verify enable_thinking was written to settings as true.
        let settings_content = fs.load(paths::settings_file()).await.unwrap();
        let settings_json: serde_json::Value = serde_json::from_str(&settings_content).unwrap();
        assert_eq!(
            settings_json["agent"]["default_model"]["enable_thinking"],
            json!(true),
            "selecting a thinking model should persist enable_thinking: true to settings"
        );
    }

    #[gpui::test]
    async fn test_select_model_updates_thinking_enabled(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.create_dir(paths::settings_file().parent().unwrap())
            .await
            .unwrap();
        fs.insert_file(paths::settings_file(), b"{}".to_vec()).await;
        let project = Project::test(fs.clone(), [], cx).await;

        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));
        let connection = NativeAgentConnection::new(agent.clone());

        let acp_thread = cx
            .update(|cx| {
                Rc::new(connection.clone()).new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/a")]),
                    cx,
                )
            })
            .await
            .unwrap();
        let session_id = cx.update(|cx| acp_thread.read(cx).session_id().clone());

        // Register a second provider with a thinking model.
        cx.update(|cx| {
            let thinking_model = Arc::new(FakeLanguageModel::with_id_and_thinking(
                "fake-corp",
                "fake-thinking",
                "Fake Thinking",
                true,
            ));
            let thinking_provider = Arc::new(
                FakeLanguageModelProvider::new(
                    LanguageModelProviderId::from("fake-corp".to_string()),
                    LanguageModelProviderName::from("Fake Corp".to_string()),
                )
                .with_models(vec![thinking_model]),
            );
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.register_provider(thinking_provider, cx);
            });
        });
        // Refresh the agent's model list so it picks up the new provider.
        agent.update(cx, |agent, cx| agent.models.refresh_list(cx));

        // Thread starts with thinking_enabled = false (the default).
        agent.read_with(cx, |agent, _| {
            let session = agent.sessions.get(&session_id).unwrap();
            session.thread.read_with(cx, |thread, _| {
                assert!(!thread.thinking_enabled(), "thinking defaults to false");
            });
        });

        // Select the thinking model via select_model.
        let selector = connection.model_selector(&session_id).unwrap();
        cx.update(|cx| selector.select_model(AgentModelId::new("fake-corp/fake-thinking"), cx))
            .await
            .unwrap();

        // select_model should have enabled thinking based on the model's supports_thinking().
        agent.read_with(cx, |agent, _| {
            let session = agent.sessions.get(&session_id).unwrap();
            session.thread.read_with(cx, |thread, _| {
                assert!(
                    thread.thinking_enabled(),
                    "select_model should enable thinking when model supports it"
                );
            });
        });

        // Switch back to the non-thinking model.
        let selector = connection.model_selector(&session_id).unwrap();
        cx.update(|cx| selector.select_model(AgentModelId::new("fake/fake"), cx))
            .await
            .unwrap();

        // select_model should have disabled thinking.
        agent.read_with(cx, |agent, _| {
            let session = agent.sessions.get(&session_id).unwrap();
            session.thread.read_with(cx, |thread, _| {
                assert!(
                    !thread.thinking_enabled(),
                    "select_model should disable thinking when model does not support it"
                );
            });
        });
    }

    #[gpui::test]
    async fn test_summarization_model_survives_transient_registry_clearing(
        cx: &mut TestAppContext,
    ) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [], cx).await;

        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent =
            cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs.clone(), cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));

        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/a")]),
                    cx,
                )
            })
            .await
            .unwrap();
        let session_id = acp_thread.read_with(cx, |thread, _| thread.session_id().clone());

        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });

        thread.read_with(cx, |thread, _| {
            assert!(
                thread.summarization_model().is_some(),
                "session should have a summarization model from the test registry"
            );
        });

        // Simulate what happens during a provider blip:
        // update_active_language_model_from_settings calls set_default_model(None)
        // when it can't resolve the model, clearing all fallbacks.
        cx.update(|cx| {
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.set_default_model(None, cx);
            });
        });
        cx.run_until_parked();

        thread.read_with(cx, |thread, _| {
            assert!(
                thread.summarization_model().is_some(),
                "summarization model should survive a transient default model clearing"
            );
        });
    }

    #[gpui::test]
    async fn test_loaded_thread_preserves_thinking_enabled(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [path!("/a").as_ref()], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx
            .update(|cx| NativeAgent::new(thread_store.clone(), Templates::new(), fs.clone(), cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));

        // Register a thinking model.
        let thinking_model = Arc::new(FakeLanguageModel::with_id_and_thinking(
            "fake-corp",
            "fake-thinking",
            "Fake Thinking",
            true,
        ));
        let thinking_provider = Arc::new(
            FakeLanguageModelProvider::new(
                LanguageModelProviderId::from("fake-corp".to_string()),
                LanguageModelProviderName::from("Fake Corp".to_string()),
            )
            .with_models(vec![thinking_model.clone()]),
        );
        cx.update(|cx| {
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.register_provider(thinking_provider, cx);
            });
        });
        agent.update(cx, |agent, cx| agent.models.refresh_list(cx));

        // Create a thread and select the thinking model.
        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/a")]),
                    cx,
                )
            })
            .await
            .unwrap();
        let session_id = acp_thread.read_with(cx, |thread, _| thread.session_id().clone());

        let selector = connection.model_selector(&session_id).unwrap();
        cx.update(|cx| selector.select_model(AgentModelId::new("fake-corp/fake-thinking"), cx))
            .await
            .unwrap();

        // Verify thinking is enabled after selecting the thinking model.
        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });
        thread.read_with(cx, |thread, _| {
            assert!(
                thread.thinking_enabled(),
                "thinking should be enabled after selecting thinking model"
            );
        });

        // Send a message so the thread gets persisted.
        let send = acp_thread.update(cx, |thread, cx| thread.send(vec!["Hello".into()], cx));
        let send = cx.foreground_executor().spawn(send);
        cx.run_until_parked();

        thinking_model.send_last_completion_stream_text_chunk("Response.");
        thinking_model.end_last_completion_stream();

        send.await.unwrap();
        cx.run_until_parked();

        // Close the session so it can be reloaded from disk.
        cx.update(|cx| connection.clone().close_session(&session_id, cx))
            .await
            .unwrap();
        drop(thread);
        drop(acp_thread);
        agent.read_with(cx, |agent, _| {
            assert!(agent.sessions.is_empty());
        });

        // Reload the thread and verify thinking_enabled is still true.
        let reloaded_acp_thread = agent
            .update(cx, |agent, cx| {
                agent.open_thread(session_id.clone(), project.clone(), cx)
            })
            .await
            .unwrap();
        let reloaded_thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });
        reloaded_thread.read_with(cx, |thread, _| {
            assert!(
                thread.thinking_enabled(),
                "thinking_enabled should be preserved when reloading a thread with a thinking model"
            );
        });

        drop(reloaded_acp_thread);
    }

    #[gpui::test]
    async fn test_loaded_thread_preserves_model(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [path!("/a").as_ref()], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx
            .update(|cx| NativeAgent::new(thread_store.clone(), Templates::new(), fs.clone(), cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));

        // Register a model where id() != name(), like real Anthropic models
        // (e.g. id="claude-sonnet-4-5-thinking-latest", name="Claude Sonnet 4.5 Thinking").
        let model = Arc::new(FakeLanguageModel::with_id_and_thinking(
            "fake-corp",
            "custom-model-id",
            "Custom Model Display Name",
            false,
        ));
        let provider = Arc::new(
            FakeLanguageModelProvider::new(
                LanguageModelProviderId::from("fake-corp".to_string()),
                LanguageModelProviderName::from("Fake Corp".to_string()),
            )
            .with_models(vec![model.clone()]),
        );
        cx.update(|cx| {
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.register_provider(provider, cx);
            });
        });
        agent.update(cx, |agent, cx| agent.models.refresh_list(cx));

        // Create a thread and select the model.
        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/a")]),
                    cx,
                )
            })
            .await
            .unwrap();
        let session_id = acp_thread.read_with(cx, |thread, _| thread.session_id().clone());

        let selector = connection.model_selector(&session_id).unwrap();
        cx.update(|cx| selector.select_model(AgentModelId::new("fake-corp/custom-model-id"), cx))
            .await
            .unwrap();

        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });
        thread.read_with(cx, |thread, _| {
            assert_eq!(
                thread.model().unwrap().id().0.as_ref(),
                "custom-model-id",
                "model should be set before persisting"
            );
        });

        // Send a message so the thread gets persisted.
        let send = acp_thread.update(cx, |thread, cx| thread.send(vec!["Hello".into()], cx));
        let send = cx.foreground_executor().spawn(send);
        cx.run_until_parked();

        model.send_last_completion_stream_text_chunk("Response.");
        model.end_last_completion_stream();

        send.await.unwrap();
        cx.run_until_parked();

        // Close the session so it can be reloaded from disk.
        cx.update(|cx| connection.clone().close_session(&session_id, cx))
            .await
            .unwrap();
        drop(thread);
        drop(acp_thread);
        agent.read_with(cx, |agent, _| {
            assert!(agent.sessions.is_empty());
        });

        // Reload the thread and verify the model was preserved.
        let reloaded_acp_thread = agent
            .update(cx, |agent, cx| {
                agent.open_thread(session_id.clone(), project.clone(), cx)
            })
            .await
            .unwrap();
        let reloaded_thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });
        reloaded_thread.read_with(cx, |thread, _| {
            let reloaded_model = thread
                .model()
                .expect("model should be present after reload");
            assert_eq!(
                reloaded_model.id().0.as_ref(),
                "custom-model-id",
                "reloaded thread should have the same model, not fall back to the default"
            );
        });

        drop(reloaded_acp_thread);
    }

    async fn persist_thread_with_fake_corp_model(
        cx: &mut TestAppContext,
    ) -> (
        Entity<NativeAgent>,
        Rc<NativeAgentConnection>,
        Entity<Project>,
        acp::SessionId,
        Arc<FakeLanguageModelProvider>,
    ) {
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [path!("/a").as_ref()], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx
            .update(|cx| NativeAgent::new(thread_store.clone(), Templates::new(), fs.clone(), cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));

        let model = Arc::new(FakeLanguageModel::with_id_and_thinking(
            "fake-corp",
            "custom-model-id",
            "Custom Model Display Name",
            false,
        ));
        let provider = Arc::new(
            FakeLanguageModelProvider::new(
                LanguageModelProviderId::from("fake-corp".to_string()),
                LanguageModelProviderName::from("Fake Corp".to_string()),
            )
            .with_models(vec![model.clone()]),
        );
        cx.update(|cx| {
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.register_provider(provider.clone(), cx);
            });
        });
        agent.update(cx, |agent, cx| agent.models.refresh_list(cx));

        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[Path::new("/a")]),
                    cx,
                )
            })
            .await
            .unwrap();
        let session_id = acp_thread.read_with(cx, |thread, _| thread.session_id().clone());

        let selector = connection.model_selector(&session_id).unwrap();
        cx.update(|cx| selector.select_model(AgentModelId::new("fake-corp/custom-model-id"), cx))
            .await
            .unwrap();

        let send = acp_thread.update(cx, |thread, cx| thread.send(vec!["Hello".into()], cx));
        let send = cx.foreground_executor().spawn(send);
        cx.run_until_parked();
        model.send_last_completion_stream_text_chunk("Response.");
        model.end_last_completion_stream();
        send.await.unwrap();
        cx.run_until_parked();

        cx.update(|cx| connection.clone().close_session(&session_id, cx))
            .await
            .unwrap();
        drop(acp_thread);

        (agent, connection, project, session_id, provider)
    }

    fn unregister_fake_corp(cx: &mut TestAppContext) {
        cx.update(|cx| {
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.unregister_provider(
                    LanguageModelProviderId::from("fake-corp".to_string()),
                    cx,
                );
            });
        });
    }

    #[gpui::test]
    async fn test_loaded_thread_resolves_model_when_provider_loads_late(cx: &mut TestAppContext) {
        init_test(cx);
        let (agent, _connection, project, session_id, provider) =
            persist_thread_with_fake_corp_model(cx).await;

        // Simulate a restart where the provider hasn't fetched its model list
        // yet, so the saved selection can't be resolved at load time.
        unregister_fake_corp(cx);

        let reloaded_acp_thread = agent
            .update(cx, |agent, cx| {
                agent.open_thread(session_id.clone(), project.clone(), cx)
            })
            .await
            .unwrap();
        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });
        thread.read_with(cx, |thread, _| {
            assert!(
                thread.model().is_none(),
                "should not fall back to an unrelated model"
            );
        });

        // The original selection is persisted even while unresolved, so a save
        // during the window can't overwrite the user's choice with a fallback.
        let db_thread = thread.read_with(cx, |thread, cx| thread.to_db(cx)).await;
        let saved = db_thread.model.expect("selection should be persisted");
        assert_eq!(saved.provider, "fake-corp");
        assert_eq!(saved.model, "custom-model-id");

        cx.update(|cx| {
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.register_provider(provider.clone(), cx);
            });
        });
        cx.run_until_parked();

        thread.read_with(cx, |thread, _| {
            assert_eq!(
                thread
                    .model()
                    .expect("model should resolve once provider loads")
                    .id()
                    .0
                    .as_ref(),
                "custom-model-id"
            );
        });

        drop(reloaded_acp_thread);
    }

    #[gpui::test]
    async fn test_explicit_model_selection_cancels_pending(cx: &mut TestAppContext) {
        init_test(cx);
        let (agent, connection, project, session_id, provider) =
            persist_thread_with_fake_corp_model(cx).await;

        unregister_fake_corp(cx);

        let reloaded_acp_thread = agent
            .update(cx, |agent, cx| {
                agent.open_thread(session_id.clone(), project.clone(), cx)
            })
            .await
            .unwrap();
        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });
        thread.read_with(cx, |thread, _| {
            assert!(thread.model().is_none());
        });

        // The user explicitly picks a different, available model.
        let other_model = Arc::new(FakeLanguageModel::with_id_and_thinking(
            "other-corp",
            "other-model-id",
            "Other Model",
            false,
        ));
        let other_provider = Arc::new(
            FakeLanguageModelProvider::new(
                LanguageModelProviderId::from("other-corp".to_string()),
                LanguageModelProviderName::from("Other Corp".to_string()),
            )
            .with_models(vec![other_model.clone()]),
        );
        cx.update(|cx| {
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.register_provider(other_provider, cx);
            });
        });
        cx.run_until_parked();

        let selector = connection.model_selector(&session_id).unwrap();
        cx.update(|cx| selector.select_model(AgentModelId::new("other-corp/other-model-id"), cx))
            .await
            .unwrap();

        thread.read_with(cx, |thread, _| {
            assert_eq!(thread.model().unwrap().id().0.as_ref(), "other-model-id");
        });

        // The original provider returning must not clobber the explicit choice.
        cx.update(|cx| {
            LanguageModelRegistry::global(cx).update(cx, |registry, cx| {
                registry.register_provider(provider.clone(), cx);
            });
        });
        cx.run_until_parked();

        thread.read_with(cx, |thread, _| {
            assert_eq!(
                thread.model().unwrap().id().0.as_ref(),
                "other-model-id",
                "a late provider load must not override the explicit selection"
            );
        });

        drop(reloaded_acp_thread);
    }

    #[gpui::test]
    async fn test_save_load_thread(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            "/",
            json!({
                "a": {
                    "b.md": "Lorem"
                }
            }),
        )
        .await;
        let project = Project::test(fs.clone(), [path!("/a").as_ref()], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx
            .update(|cx| NativeAgent::new(thread_store.clone(), Templates::new(), fs.clone(), cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));

        let acp_thread = cx
            .update(|cx| {
                connection
                    .clone()
                    .new_session(project.clone(), PathList::new(&[Path::new("")]), cx)
            })
            .await
            .unwrap();
        let session_id = acp_thread.read_with(cx, |thread, _| thread.session_id().clone());
        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });

        // Ensure empty threads are not saved, even if they get mutated.
        let model = Arc::new(FakeLanguageModel::default());
        let summary_model = Arc::new(FakeLanguageModel::default());
        thread.update(cx, |thread, cx| {
            thread.set_model(model.clone(), cx);
            thread.set_summarization_model(Some(summary_model.clone()), cx);
        });
        cx.run_until_parked();
        assert_eq!(thread_entries(&thread_store, cx), vec![]);

        let send = acp_thread.update(cx, |thread, cx| {
            thread.send(
                vec![
                    "What does ".into(),
                    acp::ContentBlock::ResourceLink(acp::ResourceLink::new(
                        "b.md",
                        MentionUri::File {
                            abs_path: path!("/a/b.md").into(),
                        }
                        .to_uri()
                        .to_string(),
                    )),
                    " mean?".into(),
                ],
                cx,
            )
        });
        let send = cx.foreground_executor().spawn(send);
        cx.run_until_parked();

        model.send_last_completion_stream_text_chunk("Lorem.");
        model.send_last_completion_stream_event(LanguageModelCompletionEvent::UsageUpdate(
            language_model::TokenUsage {
                input_tokens: 150,
                output_tokens: 75,
                ..Default::default()
            },
        ));
        model.end_last_completion_stream();
        cx.run_until_parked();
        summary_model
            .send_last_completion_stream_text_chunk(&format!("Explaining {}", path!("/a/b.md")));
        summary_model.end_last_completion_stream();

        send.await.unwrap();
        let uri = MentionUri::File {
            abs_path: path!("/a/b.md").into(),
        }
        .to_uri();
        acp_thread.read_with(cx, |thread, cx| {
            assert_eq!(
                thread.to_markdown(cx),
                formatdoc! {"
                    ## User

                    What does [@b.md]({uri}) mean?

                    ## Assistant

                    Lorem.

                "}
            )
        });

        cx.run_until_parked();

        // Set a draft prompt with rich content blocks and scroll position
        // AFTER run_until_parked, so the only save that captures these
        // changes is the one performed by close_session itself.
        let draft_blocks = vec![
            acp::ContentBlock::Text(acp::TextContent::new("Check out ")),
            acp::ContentBlock::ResourceLink(acp::ResourceLink::new("b.md", uri.to_string())),
            acp::ContentBlock::Text(acp::TextContent::new(" please")),
        ];
        acp_thread.update(cx, |thread, cx| {
            thread.set_draft_prompt(Some(draft_blocks.clone()), cx);
        });
        thread.update(cx, |thread, _cx| {
            thread.set_ui_scroll_position(Some(gpui::ListOffset {
                item_ix: 5,
                offset_in_item: gpui::px(12.5),
            }));
        });

        // Close the session so it can be reloaded from disk.
        cx.update(|cx| connection.clone().close_session(&session_id, cx))
            .await
            .unwrap();
        drop(thread);
        drop(acp_thread);
        agent.read_with(cx, |agent, _| {
            assert_eq!(agent.sessions.keys().cloned().collect::<Vec<_>>(), []);
        });

        // Ensure the thread can be reloaded from disk.
        assert_eq!(
            thread_entries(&thread_store, cx),
            vec![(
                session_id.clone(),
                format!("Explaining {}", path!("/a/b.md"))
            )]
        );
        let acp_thread = agent
            .update(cx, |agent, cx| {
                agent.open_thread(session_id.clone(), project.clone(), cx)
            })
            .await
            .unwrap();
        acp_thread.read_with(cx, |thread, cx| {
            assert_eq!(
                thread.to_markdown(cx),
                formatdoc! {"
                    ## User

                    What does [@b.md]({uri}) mean?

                    ## Assistant

                    Lorem.

                "}
            )
        });

        // Ensure the draft prompt with rich content blocks survived the round-trip.
        acp_thread.read_with(cx, |thread, _| {
            assert_eq!(thread.draft_prompt(), Some(draft_blocks.as_slice()));
        });

        // Ensure token usage survived the round-trip.
        acp_thread.read_with(cx, |thread, _| {
            let usage = thread
                .token_usage()
                .expect("token usage should be restored after reload");
            assert_eq!(usage.input_tokens, 150);
            assert_eq!(usage.output_tokens, 75);
        });

        // Ensure scroll position survived the round-trip.
        acp_thread.read_with(cx, |thread, _| {
            let scroll = thread
                .ui_scroll_position()
                .expect("scroll position should be restored after reload");
            assert_eq!(scroll.item_ix, 5);
            assert_eq!(scroll.offset_in_item, gpui::px(12.5));
        });
    }

    #[gpui::test]
    async fn test_close_session_saves_thread(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            "/",
            json!({
                "a": {
                    "file.txt": "hello"
                }
            }),
        )
        .await;
        let project = Project::test(fs.clone(), [path!("/a").as_ref()], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx
            .update(|cx| NativeAgent::new(thread_store.clone(), Templates::new(), fs.clone(), cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));

        let acp_thread = cx
            .update(|cx| {
                connection
                    .clone()
                    .new_session(project.clone(), PathList::new(&[Path::new("")]), cx)
            })
            .await
            .unwrap();
        let session_id = acp_thread.read_with(cx, |thread, _| thread.session_id().clone());
        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });

        let model = Arc::new(FakeLanguageModel::default());
        thread.update(cx, |thread, cx| {
            thread.set_model(model.clone(), cx);
        });

        // Send a message so the thread is non-empty (empty threads aren't saved).
        let send = acp_thread.update(cx, |thread, cx| thread.send(vec!["hello".into()], cx));
        let send = cx.foreground_executor().spawn(send);
        cx.run_until_parked();

        model.send_last_completion_stream_text_chunk("world");
        model.end_last_completion_stream();
        send.await.unwrap();
        cx.run_until_parked();

        // Set a draft prompt WITHOUT calling run_until_parked afterwards.
        // This means no observe-triggered save has run for this change.
        // The only way this data gets persisted is if close_session
        // itself performs the save.
        let draft_blocks = vec![acp::ContentBlock::Text(acp::TextContent::new(
            "unsaved draft",
        ))];
        acp_thread.update(cx, |thread, cx| {
            thread.set_draft_prompt(Some(draft_blocks.clone()), cx);
        });

        // Close the session immediately — no run_until_parked in between.
        cx.update(|cx| connection.clone().close_session(&session_id, cx))
            .await
            .unwrap();
        cx.run_until_parked();

        // Reopen and verify the draft prompt was saved.
        let reloaded = agent
            .update(cx, |agent, cx| {
                agent.open_thread(session_id.clone(), project.clone(), cx)
            })
            .await
            .unwrap();
        reloaded.read_with(cx, |thread, _| {
            assert_eq!(
                thread.draft_prompt(),
                Some(draft_blocks.as_slice()),
                "close_session must save the thread; draft prompt was lost"
            );
        });
    }

    #[gpui::test]
    async fn test_thread_summary_releases_loaded_session(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            "/",
            json!({
                "a": {
                    "file.txt": "hello"
                }
            }),
        )
        .await;
        let project = Project::test(fs.clone(), [path!("/a").as_ref()], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx
            .update(|cx| NativeAgent::new(thread_store.clone(), Templates::new(), fs.clone(), cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));

        let acp_thread = cx
            .update(|cx| {
                connection
                    .clone()
                    .new_session(project.clone(), PathList::new(&[Path::new("")]), cx)
            })
            .await
            .unwrap();
        let session_id = acp_thread.read_with(cx, |thread, _| thread.session_id().clone());
        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });

        let model = Arc::new(FakeLanguageModel::default());
        let summary_model = Arc::new(FakeLanguageModel::default());
        thread.update(cx, |thread, cx| {
            thread.set_model(model.clone(), cx);
            thread.set_summarization_model(Some(summary_model.clone()), cx);
        });

        let send = acp_thread.update(cx, |thread, cx| thread.send(vec!["hello".into()], cx));
        let send = cx.foreground_executor().spawn(send);
        cx.run_until_parked();

        model.send_last_completion_stream_text_chunk("world");
        model.end_last_completion_stream();
        send.await.unwrap();
        cx.run_until_parked();

        let summary = agent.update(cx, |agent, cx| {
            agent.thread_summary(session_id.clone(), project.clone(), cx)
        });
        cx.run_until_parked();

        summary_model.send_last_completion_stream_text_chunk("summary");
        summary_model.end_last_completion_stream();

        assert_eq!(summary.await.unwrap(), "summary");
        cx.run_until_parked();

        agent.read_with(cx, |agent, _| {
            let session = agent
                .sessions
                .get(&session_id)
                .expect("thread_summary should not close the active session");
            assert_eq!(
                session.ref_count, 1,
                "thread_summary should release its temporary session reference"
            );
        });

        cx.update(|cx| connection.clone().close_session(&session_id, cx))
            .await
            .unwrap();
        cx.run_until_parked();

        agent.read_with(cx, |agent, _| {
            assert!(
                agent.sessions.is_empty(),
                "closing the active session after thread_summary should unload it"
            );
        });
    }

    #[gpui::test]
    async fn test_loaded_sessions_keep_state_until_last_close(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            "/",
            json!({
                "a": {
                    "file.txt": "hello"
                }
            }),
        )
        .await;
        let project = Project::test(fs.clone(), [path!("/a").as_ref()], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx
            .update(|cx| NativeAgent::new(thread_store.clone(), Templates::new(), fs.clone(), cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));

        let acp_thread = cx
            .update(|cx| {
                connection
                    .clone()
                    .new_session(project.clone(), PathList::new(&[Path::new("")]), cx)
            })
            .await
            .unwrap();
        let session_id = acp_thread.read_with(cx, |thread, _| thread.session_id().clone());
        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });

        let model = cx.update(|cx| {
            LanguageModelRegistry::read_global(cx)
                .default_model()
                .map(|default_model| default_model.model)
                .expect("default test model should be available")
        });
        let fake_model = model.as_fake();
        thread.update(cx, |thread, cx| {
            thread.set_model(model.clone(), cx);
        });

        let send = acp_thread.update(cx, |thread, cx| thread.send(vec!["hello".into()], cx));
        let send = cx.foreground_executor().spawn(send);
        cx.run_until_parked();

        fake_model.send_last_completion_stream_text_chunk("world");
        fake_model.end_last_completion_stream();
        send.await.unwrap();
        cx.run_until_parked();

        cx.update(|cx| connection.clone().close_session(&session_id, cx))
            .await
            .unwrap();
        drop(thread);
        drop(acp_thread);
        agent.read_with(cx, |agent, _| {
            assert!(agent.sessions.is_empty());
        });

        let first_loaded_thread = cx.update(|cx| {
            connection.clone().load_session(
                session_id.clone(),
                project.clone(),
                PathList::new(&[Path::new("")]),
                None,
                cx,
            )
        });
        let second_loaded_thread = cx.update(|cx| {
            connection.clone().load_session(
                session_id.clone(),
                project.clone(),
                PathList::new(&[Path::new("")]),
                None,
                cx,
            )
        });

        let first_loaded_thread = first_loaded_thread.await.unwrap();
        let second_loaded_thread = second_loaded_thread.await.unwrap();

        cx.run_until_parked();

        assert_eq!(
            first_loaded_thread.entity_id(),
            second_loaded_thread.entity_id(),
            "concurrent loads for the same session should share one AcpThread"
        );

        cx.update(|cx| connection.clone().close_session(&session_id, cx))
            .await
            .unwrap();

        agent.read_with(cx, |agent, _| {
            assert!(
                agent.sessions.contains_key(&session_id),
                "closing one loaded session should not drop shared session state"
            );
        });

        let follow_up = second_loaded_thread.update(cx, |thread, cx| {
            thread.send(vec!["still there?".into()], cx)
        });
        let follow_up = cx.foreground_executor().spawn(follow_up);
        cx.run_until_parked();

        fake_model.send_last_completion_stream_text_chunk("yes");
        fake_model.end_last_completion_stream();
        follow_up.await.unwrap();
        cx.run_until_parked();

        second_loaded_thread.read_with(cx, |thread, cx| {
            assert_eq!(
                thread.to_markdown(cx),
                formatdoc! {"
                    ## User

                    hello

                    ## Assistant

                    world

                    ## User

                    still there?

                    ## Assistant

                    yes

                "}
            );
        });

        cx.update(|cx| connection.clone().close_session(&session_id, cx))
            .await
            .unwrap();

        cx.run_until_parked();

        drop(first_loaded_thread);
        drop(second_loaded_thread);
        agent.read_with(cx, |agent, _| {
            assert!(agent.sessions.is_empty());
        });
    }

    #[gpui::test]
    async fn test_rapid_title_changes_do_not_loop(cx: &mut TestAppContext) {
        // Regression test: rapid title changes must not cause a propagation loop
        // between Thread and AcpThread via handle_thread_title_updated.
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx
            .update(|cx| NativeAgent::new(thread_store.clone(), Templates::new(), fs.clone(), cx));
        let connection = Rc::new(NativeAgentConnection::new(agent.clone()));

        let acp_thread = cx
            .update(|cx| {
                connection
                    .clone()
                    .new_session(project.clone(), PathList::new(&[Path::new("")]), cx)
            })
            .await
            .unwrap();

        let session_id = acp_thread.read_with(cx, |thread, _| thread.session_id().clone());
        let thread = agent.read_with(cx, |agent, _| {
            agent.sessions.get(&session_id).unwrap().thread.clone()
        });

        let title_updated_count = Rc::new(std::cell::RefCell::new(0usize));
        cx.update(|cx| {
            let count = title_updated_count.clone();
            cx.subscribe(
                &thread,
                move |_entity: Entity<Thread>, _event: &TitleUpdated, _cx: &mut App| {
                    let new_count = {
                        let mut count = count.borrow_mut();
                        *count += 1;
                        *count
                    };
                    assert!(
                        new_count <= 2,
                        "TitleUpdated fired {new_count} times; \
                         title updates are looping"
                    );
                },
            )
            .detach();
        });

        thread.update(cx, |thread, cx| thread.set_title("first".into(), cx));
        thread.update(cx, |thread, cx| thread.set_title("second".into(), cx));

        cx.run_until_parked();

        thread.read_with(cx, |thread, _| {
            assert_eq!(thread.title(), Some("second".into()));
        });
        acp_thread.read_with(cx, |acp_thread, _| {
            assert_eq!(acp_thread.title(), Some("second".into()));
        });

        assert_eq!(*title_updated_count.borrow(), 2);
    }

    fn thread_entries(
        thread_store: &Entity<ThreadStore>,
        cx: &mut TestAppContext,
    ) -> Vec<(acp::SessionId, String)> {
        thread_store.read_with(cx, |store, _| {
            store
                .entries()
                .map(|entry| (entry.id.clone(), entry.title.to_string()))
                .collect::<Vec<_>>()
        })
    }

    fn init_test(cx: &mut TestAppContext) {
        env_logger::try_init().ok();
        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);

            LanguageModelRegistry::test(cx);
        });
    }

    #[test]
    fn test_strip_slash_command_prefix_keeps_inline_args() {
        // The bug being guarded against: skill slash invocation used to
        // discard the entire first text block, which threw away anything
        // the user typed on the same line as the command.
        assert_eq!(
            strip_slash_command_prefix("/fix-review #1, #2, #3"),
            "#1, #2, #3",
        );
    }

    #[test]
    fn test_strip_slash_command_prefix_preserves_newlines() {
        // Continuations across newlines are common when users compose
        // structured prompts; the first newline is the command terminator,
        // but everything after it must reach the model verbatim.
        assert_eq!(
            strip_slash_command_prefix("/fix-review\nline 1\nline 2"),
            "line 1\nline 2",
        );
    }

    #[test]
    fn test_strip_slash_command_prefix_command_only_is_empty() {
        assert_eq!(strip_slash_command_prefix("/fix-review"), "");
        assert_eq!(strip_slash_command_prefix("/fix-review "), "");
    }

    #[test]
    fn test_strip_slash_command_prefix_ignores_leading_whitespace() {
        assert_eq!(strip_slash_command_prefix("   /fix-review hello"), "hello",);
    }

    #[test]
    fn test_strip_slash_command_prefix_passes_through_non_command_text() {
        // Defense in depth: if somehow we're called with a non-slash-prefixed
        // block, the safe behavior is to return it unchanged rather than
        // silently mangling unrelated user text.
        assert_eq!(strip_slash_command_prefix("hello world"), "hello world",);
    }

    #[test]
    fn merge_broker_usage_preserves_unknown_totals() {
        let previous = BrokerUsage {
            requested_tokens: None,
            actual_tokens: Some(5),
            model: "previous-model".to_string(),
            duration_ms: Some(3),
            cost_micros: None,
            cache_hit: Some(false),
            unavailable_reason: Some("previous unavailable reason".to_string()),
        };
        let current = BrokerUsage {
            requested_tokens: Some(8),
            actual_tokens: None,
            model: "current-model".to_string(),
            duration_ms: Some(7),
            cost_micros: Some(11),
            cache_hit: Some(true),
            unavailable_reason: None,
        };

        let merged = merge_broker_usage(Some(previous), current);

        assert_eq!(merged.requested_tokens, None);
        assert_eq!(merged.actual_tokens, None);
        assert_eq!(merged.duration_ms, Some(10));
        assert_eq!(merged.cost_micros, None);
        assert_eq!(merged.model, "current-model");
        assert_eq!(merged.cache_hit, Some(true));
        assert_eq!(
            merged.unavailable_reason.as_deref(),
            Some("previous unavailable reason")
        );
    }

    #[test]
    fn merge_broker_usage_sums_known_totals() {
        let previous = BrokerUsage {
            requested_tokens: Some(3),
            actual_tokens: Some(5),
            model: "previous-model".to_string(),
            duration_ms: Some(7),
            cost_micros: Some(11),
            cache_hit: Some(true),
            unavailable_reason: None,
        };
        let current = BrokerUsage {
            requested_tokens: Some(13),
            actual_tokens: Some(17),
            model: "current-model".to_string(),
            duration_ms: Some(19),
            cost_micros: Some(23),
            cache_hit: None,
            unavailable_reason: Some("current unavailable reason".to_string()),
        };

        let merged = merge_broker_usage(Some(previous), current);

        assert_eq!(merged.requested_tokens, Some(16));
        assert_eq!(merged.actual_tokens, Some(22));
        assert_eq!(merged.duration_ms, Some(26));
        assert_eq!(merged.cost_micros, Some(34));
        assert_eq!(merged.model, "current-model");
        assert_eq!(merged.cache_hit, Some(true));
        assert_eq!(
            merged.unavailable_reason.as_deref(),
            Some("current unavailable reason")
        );
    }

    #[test]
    fn broker_usage_from_missing_token_telemetry_is_explicit() {
        let unknown = broker_usage_from_token_usage(None, Some("provider/model"), 17);
        assert_eq!(unknown.requested_tokens, None);
        assert_eq!(unknown.actual_tokens, None);
        assert_eq!(unknown.duration_ms, Some(17));
        assert_eq!(unknown.cost_micros, None);
        assert_eq!(unknown.cache_hit, None);
        assert_eq!(
            unknown.unavailable_reason.as_deref(),
            Some("native ACP provider did not report token, cost, or cache telemetry")
        );

        let token_usage = acp_thread::TokenUsage {
            max_tokens: 100,
            used_tokens: 30,
            input_tokens: 20,
            output_tokens: 10,
            max_output_tokens: Some(50),
        };
        let known = broker_usage_from_token_usage(Some(&token_usage), Some("provider/model"), 19);
        assert_eq!(known.requested_tokens, Some(20));
        assert_eq!(known.actual_tokens, Some(10));
        assert_eq!(known.duration_ms, Some(19));
        assert_eq!(
            known.unavailable_reason.as_deref(),
            Some("native ACP provider did not report cost or cache telemetry")
        );
    }

    #[test]
    fn merge_broker_usage_retains_unknown_reasons_across_turns() {
        let previous = BrokerUsage {
            requested_tokens: None,
            actual_tokens: None,
            model: "provider/model".to_string(),
            duration_ms: Some(5),
            cost_micros: None,
            cache_hit: None,
            unavailable_reason: Some(
                "native ACP provider did not report token, cost, or cache telemetry".to_string(),
            ),
        };
        let current = BrokerUsage {
            requested_tokens: Some(20),
            actual_tokens: Some(10),
            model: "provider/model".to_string(),
            duration_ms: Some(7),
            cost_micros: None,
            cache_hit: None,
            unavailable_reason: Some(
                "native ACP provider did not report cost or cache telemetry".to_string(),
            ),
        };

        let merged = merge_broker_usage(Some(previous), current);

        assert_eq!(merged.requested_tokens, None);
        assert_eq!(merged.actual_tokens, None);
        let reason = merged.unavailable_reason.as_deref().unwrap_or_default();
        assert!(reason.contains("token, cost, or cache"));
        assert!(reason.contains("cost or cache"));
    }

    #[gpui::test]
    async fn gearbox_acp_worker_broker_lifecycle(cx: &mut TestAppContext) {
        use gearbox_agent::state::{
            Scope, StateStore, Task as GearTask, TaskInputs, TaskKind, TaskOutputs, TaskStatus,
        };

        init_test(cx);

        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(
            workspace.path().join("README.md"),
            "# ACP broker worker test\n",
        )
        .unwrap();
        let store = StateStore::new(workspace.path());
        store.initialize().unwrap();

        let fs = FakeFs::new(cx.executor());
        fs.insert_tree("/", json!({ "a": {} })).await;
        let project = Project::test(fs.clone(), [Path::new("/a")], cx).await;
        let thread_store = cx.new(|cx| ThreadStore::new(cx));
        let agent = cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs, cx));
        let connection = Rc::new(NativeAgentConnection::gear(agent.clone()));
        let acp_thread = cx
            .update(|cx| {
                connection.clone().new_session(
                    project.clone(),
                    PathList::new(&[workspace.path()]),
                    cx,
                )
            })
            .await
            .unwrap();
        let parent_session_id = cx.update(|cx| acp_thread.read(cx).session_id().clone());
        let model = cx.update(|cx| {
            LanguageModelRegistry::read_global(cx)
                .default_model()
                .map(|default_model| default_model.model)
                .expect("default test model should be available")
        });
        let fake_model = model.as_fake();

        // Phase 1: discover available agents
        let discovered = cx.update(|cx| gear_acp_broker_discover_agents(cx));
        assert!(
            !discovered.is_empty(),
            "ACP broker discovery should return at least the test model"
        );

        // Phase 2: start an ACP broker worker
        let (acp_broker_tx, acp_broker_rx) = async_channel::unbounded::<GearAcpBrokerDispatch>();
        cx.update(|cx| {
            spawn_gear_acp_broker_dispatcher(
                agent.downgrade(),
                parent_session_id,
                acp_broker_rx,
                Arc::new(Mutex::new(HashMap::default())),
                cx,
            );
        });
        let backend = GearAcpBrokerBackend::new(acp_broker_tx);
        let task = GearTask {
            id: "task_acp_broker_001".to_string(),
            goal_id: "goal_acp_broker_001".to_string(),
            parent_task_id: None,
            title: "acp broker lifecycle".to_string(),
            kind: TaskKind::Edit,
            status: TaskStatus::Pending,
            assigned_worker: Some("zed_agent".to_string()),
            attempt: 1,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: TaskInputs::default(),
            outputs: TaskOutputs::default(),
        };
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
            require_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        };
        let handle = backend
            .start_zed_agent(WorkerStartRequest {
                store: &store,
                workspace: workspace.path(),
                task: &task,
                route_attempt: 0,
                goal: "Test the ACP broker backend lifecycle.",
                verification_commands: &[],
                config: &config,
                cancellation_token: None,
                coordinator_model: None,
                coordinator_brief: None,
                route_hint: None,
            })
            .unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let _event_subscription = handle
            .subscribe({
                let events = events.clone();
                Arc::new(move |event| {
                    events
                        .lock()
                        .expect("acp broker event capture mutex poisoned")
                        .push(event);
                })
            })
            .unwrap();

        // Phase 3: wait for the first prompt — the dispatcher should have
        // created a subagent session via the ACP thread.
        wait_for_fake_completion(fake_model, cx).await;
        for _ in 0..20 {
            cx.run_until_parked();
            cx.background_executor
                .timer(Duration::from_millis(10))
                .await;
        }
        let first_session_id = handle
            .session_id()
            .expect("acp broker worker should expose its session id after first prompt starts");

        fake_model.send_last_completion_stream_text_chunk("broker initial response");
        fake_model.end_last_completion_stream();
        for _ in 0..100 {
            cx.run_until_parked();
            let received_first_turn_finished = events.lock().is_ok_and(|events| {
                events.iter().any(|event| {
                    matches!(
                        event,
                        WorkerEvent::TurnFinished { kind, .. } if kind == "acp"
                    )
                })
            });
            if received_first_turn_finished {
                break;
            }
            cx.background_executor
                .timer(Duration::from_millis(10))
                .await;
        }
        let usage_path = store.worker_dir("task_acp_broker_001").join("usage.json");
        let usage: BrokerUsage = serde_json::from_str(
            &std::fs::read_to_string(&usage_path)
                .expect("native ACP worker must persist an explicit usage artifact"),
        )
        .expect("native ACP usage artifact must remain valid JSON");
        assert!(
            usage.unavailable_reason.is_some(),
            "fake ACP provider must not turn missing cost telemetry into a precise-looking record"
        );

        // Phase 4: follow-up
        let first_completion_count = fake_model.completion_count();
        handle
            .send_follow_up("Refine the result.".to_string())
            .unwrap();
        for _ in 0..100 {
            cx.run_until_parked();
            if fake_model.completion_count() > first_completion_count {
                break;
            }
            cx.background_executor
                .timer(Duration::from_millis(10))
                .await;
        }
        assert!(
            fake_model.completion_count() > first_completion_count,
            "terminal follow-up should dispatch a new ACP turn"
        );
        fake_model.send_last_completion_stream_text_chunk("broker follow-up response");
        fake_model.end_last_completion_stream();
        for _ in 0..100 {
            cx.run_until_parked();
            let received_follow_up = events.lock().is_ok_and(|events| {
                events.iter().any(|event| {
                    matches!(
                        event,
                        WorkerEvent::AssistantTextDelta { kind, delta }
                            if kind == "acp" && delta.contains("broker follow-up response")
                    )
                })
            });
            if received_follow_up {
                break;
            }
            cx.background_executor
                .timer(Duration::from_millis(10))
                .await;
        }
        assert_eq!(
            handle.session_id().as_deref(),
            Some(first_session_id.as_str()),
            "follow-up should reuse the same session"
        );

        // Phase 5: steer
        let second_completion_count = fake_model.completion_count();
        handle
            .steer("Steer into final review.".to_string())
            .unwrap();
        for _ in 0..100 {
            cx.run_until_parked();
            if fake_model.completion_count() > second_completion_count {
                break;
            }
            cx.background_executor
                .timer(Duration::from_millis(10))
                .await;
        }
        assert!(
            fake_model.completion_count() > second_completion_count,
            "terminal steer should dispatch a new ACP turn"
        );
        fake_model.send_last_completion_stream_text_chunk("broker steer response");
        fake_model.end_last_completion_stream();
        for _ in 0..100 {
            cx.run_until_parked();
            let received_steer = events.lock().is_ok_and(|events| {
                events.iter().any(|event| {
                    matches!(
                        event,
                        WorkerEvent::AssistantTextDelta { kind, delta }
                            if kind == "acp" && delta.contains("broker steer response")
                    )
                })
            });
            if received_steer {
                break;
            }
            cx.background_executor
                .timer(Duration::from_millis(10))
                .await;
        }
        assert_eq!(
            handle.session_id().as_deref(),
            Some(first_session_id.as_str()),
            "steer should reuse the same session"
        );
        let observed_events = events
            .lock()
            .expect("acp broker event capture mutex poisoned");
        assert!(observed_events.iter().any(|event| matches!(
            event,
            WorkerEvent::AssistantTextDelta { kind, delta }
                if kind == "acp" && delta.contains("broker follow-up response")
        )));
        assert!(observed_events.iter().any(|event| matches!(
            event,
            WorkerEvent::TurnFinished { kind, .. } if kind == "acp"
        )));
        drop(observed_events);

        // Phase 6: cancel — verify the session still existed before cancel
        let _ = handle.session_id();
        handle.cancel().unwrap();
        // After cancel, wait_for_result should return the cancelled result
        let result = std::thread::spawn({
            let handle = handle.clone();
            move || handle.wait_for_result()
        });
        for _ in 0..50 {
            cx.run_until_parked();
            if result.is_finished() {
                break;
            }
            cx.background_executor
                .timer(Duration::from_millis(10))
                .await;
        }
        assert!(result.is_finished());
        let worker_result = result.join().unwrap();
        // Cancellation may produce Failed status or Succeeded if the
        // worker had already completed; either is valid.
        assert!(
            matches!(
                worker_result.as_ref().map(|r| &r.status),
                Ok(WorkerStatus::Failed) | Ok(WorkerStatus::Succeeded)
            ),
            "cancel should produce a terminal result, got {:?}",
            worker_result.as_ref().map(|r| &r.status)
        );

        // Phase 7: disposal is distinct from cancellation and clears the
        // handle's provider-session identity once the dispatcher has run.
        handle.dispose().unwrap();
        cx.run_until_parked();
        assert!(
            handle.session_id().is_none(),
            "disposed ACP handles must not expose a stale session identity"
        );

        // Phase 8: model discovery produces valid ModelAvailability entries
        for (agent_name, availability) in &discovered {
            assert!(
                !agent_name.is_empty(),
                "discovered agent name must not be empty"
            );
            match availability {
                ModelAvailability::Available(selector) => {
                    assert!(
                        !selector.agent_id.is_empty(),
                        "available model selector must have a non-empty agent_id"
                    );
                }
                ModelAvailability::Unavailable(reason) => {
                    // Unavailable entries are valid when no providers are
                    // configured; the test environment always has a fake
                    // provider, so this branch won't fire in normal test runs.
                    let _ = reason;
                }
            }
        }

        // Phase 8: verify the foreground entity never leaked across threads
        // — the WeakEntity<NativeAgent> should still be valid, but the
        // Entity itself must not be Send.
        assert!(
            agent.downgrade().upgrade().is_some(),
            "Agent must still be alive after the broker lifecycle"
        );
    }
}

/// Create a Zed native agent sub-session for a one-shot quick task.
///
/// The subagent is created under the given parent Gear session and runs the
/// provided prompt through a fresh Zed Agent thread. Returns the assistant's
/// response text on success.
///
/// This is a simplified wrapper around the native Zed worker sub-agent
/// creation path, intended for small/low-risk "小修" tasks dispatched by
/// the Gearbox orchestrator.
pub fn create_gearbox_agent_session(
    agent: WeakEntity<NativeAgent>,
    parent_session_id: &acp::SessionId,
    task_id: String,
    prompt: String,
    cx: &mut App,
) -> Task<Result<String>> {
    let parent_session_id = parent_session_id.clone();
    cx.spawn(async move |cx| {
        let (_parent_thread, subagent_thread, acp_thread) =
            agent.update(cx, |agent, cx| -> Result<_> {
                let parent_session = agent
                    .sessions
                    .get(&parent_session_id)
                    .context("parent Gear session not found")?;
                let parent_thread = parent_session.thread.clone();
                let subagent_thread = cx.new(|cx| {
                    let mut thread = Thread::new_subagent(&parent_thread, cx);
                    thread.set_title(format!("Gear Quick Worker {task_id}").into(), cx);
                    thread
                });
                let acp_thread = agent.register_session(
                    subagent_thread.clone(),
                    parent_session.project_id,
                    1,
                    None,
                    ZED_AGENT_ID.clone(),
                    "zed".into(),
                    cx,
                );
                parent_thread.update(cx, |thread, _cx| {
                    thread.register_running_subagent(subagent_thread.downgrade())
                });
                Ok((parent_thread, subagent_thread, acp_thread))
            })??;

        let acp_response = acp_thread
            .update(cx, |acp_thread, cx| {
                acp_thread.send(vec![prompt.into()], cx)
            })
            .await?;

        if let Some(ref acp_response) = acp_response {
            match acp_response.stop_reason {
                acp::StopReason::EndTurn | acp::StopReason::MaxTokens => {}
                acp::StopReason::Cancelled => {
                    anyhow::bail!("Zed agent sub-session was cancelled");
                }
                _ => {
                    anyhow::bail!(
                        "Zed agent sub-session stopped: {:?}",
                        acp_response.stop_reason
                    );
                }
            }
        }

        let assistant_text = subagent_thread.read_with(cx, |thread, _cx| {
            thread
                .last_message()
                .and_then(|message| {
                    let content = message
                        .as_agent_message()?
                        .content
                        .iter()
                        .filter_map(|content| match content {
                            AgentMessageContent::Text(text) => Some(text.as_str()),
                            _ => None,
                        })
                        .join("\n\n");
                    (!content.is_empty()).then_some(content)
                })
                .unwrap_or_default()
        });
        Ok(assistant_text)
    })
}

fn mcp_message_content_to_acp_content_block(
    content: context_server::types::MessageContent,
) -> acp::ContentBlock {
    match content {
        context_server::types::MessageContent::Text {
            text,
            annotations: _,
        } => text.into(),
        context_server::types::MessageContent::Image {
            data,
            mime_type,
            annotations: _,
        } => acp::ContentBlock::Image(acp::ImageContent::new(data, mime_type)),
        context_server::types::MessageContent::Audio {
            data,
            mime_type,
            annotations: _,
        } => acp::ContentBlock::Audio(acp::AudioContent::new(data, mime_type)),
        context_server::types::MessageContent::Resource {
            resource,
            annotations: _,
        } => {
            let mut link =
                acp::ResourceLink::new(resource.uri.to_string(), resource.uri.to_string());
            if let Some(mime_type) = resource.mime_type {
                link = link.mime_type(mime_type);
            }
            acp::ContentBlock::ResourceLink(link)
        }
    }
}
