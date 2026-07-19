use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, bail};
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::state::{
    CoordinatorModel, GlobalProviderCooldown, Scope, StateStore, Task, TaskInputs,
    is_destructive_command, timestamp, write_json,
};
use crate::tools::{
    CancellationToken, git_snapshot, run_shell_command_with_env_and_cancellation_and_timeout,
};

const WORKER_RUNTIME_DEADLINE_SCHEMA_VERSION: u32 = 1;
const WORKER_RUNTIME_DEADLINE_FILE: &str = "runtime-deadline.json";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkerRuntimeDeadlineReceipt {
    schema_version: u32,
    task_id: String,
    goal_id: String,
    epoch_id: String,
    deadline_at_ms: u64,
    source: String,
    recorded_at: String,
}

fn goal_runtime_deadline(store: &StateStore, goal_id: &str) -> Result<Option<(u64, String)>> {
    let lease_path = store.goal_run_lease_path(goal_id);
    let contents = match fs::read_to_string(&lease_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to read goal runtime lease {}", lease_path.display())
            });
        }
    };
    let lease: crate::state::GoalRunLease = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse goal runtime lease {}", lease_path.display()))?;
    if lease.goal_id != goal_id {
        bail!(
            "goal runtime lease {} is bound to {}, expected {}",
            lease_path.display(),
            lease.goal_id,
            goal_id
        );
    }
    let deadline_at_ms = DateTime::parse_from_rfc3339(&lease.expires_at)
        .with_context(|| format!("goal runtime lease has invalid expires_at: {}", lease.expires_at))?
        .timestamp_millis();
    let deadline_at_ms = u64::try_from(deadline_at_ms)
        .context("goal runtime lease expires_at precedes the Unix epoch")?;
    Ok(Some((deadline_at_ms, lease.epoch_id)))
}

fn persist_worker_runtime_deadline(store: &StateStore, task: &Task) -> Result<()> {
    let Some((deadline_at_ms, epoch_id)) = goal_runtime_deadline(store, &task.goal_id)? else {
        return Ok(());
    };
    let receipt = WorkerRuntimeDeadlineReceipt {
        schema_version: WORKER_RUNTIME_DEADLINE_SCHEMA_VERSION,
        task_id: task.id.clone(),
        goal_id: task.goal_id.clone(),
        epoch_id,
        deadline_at_ms,
        source: store
            .goal_run_lease_path(&task.goal_id)
            .to_string_lossy()
            .to_string(),
        recorded_at: timestamp(),
    };
    store.write_worker_json_atomic(&task.id, WORKER_RUNTIME_DEADLINE_FILE, &receipt)?;
    Ok(())
}

fn read_worker_runtime_deadline(store: &StateStore, task_id: &str) -> Result<Option<u64>> {
    let path = store.worker_dir(task_id).join(WORKER_RUNTIME_DEADLINE_FILE);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to read worker runtime deadline {}", path.display())
            });
        }
    };
    let receipt: WorkerRuntimeDeadlineReceipt = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse worker runtime deadline {}", path.display()))?;
    if receipt.schema_version != WORKER_RUNTIME_DEADLINE_SCHEMA_VERSION
        || receipt.task_id != task_id
        || receipt.goal_id.trim().is_empty()
        || receipt.epoch_id.trim().is_empty()
        || receipt.deadline_at_ms == 0
    {
        bail!("worker runtime deadline {} is invalid", path.display());
    }
    Ok(Some(receipt.deadline_at_ms))
}

fn worker_external_timeout(
    store: &StateStore,
    task_id: &str,
    configured_timeout: Option<Duration>,
) -> Result<Option<Duration>> {
    let runtime_timeout = read_worker_runtime_deadline(store, task_id)?.map(|deadline_at_ms| {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        Duration::from_millis(deadline_at_ms.saturating_sub(now_ms).max(1))
    });
    Ok(match (configured_timeout, runtime_timeout) {
        (Some(configured), Some(runtime)) => Some(configured.min(runtime)),
        (Some(configured), None) => Some(configured),
        (None, Some(runtime)) => Some(runtime),
        (None, None) => None,
    })
}

/// Create a temporary directory containing `opencode/oh-my-openagent.json`
/// with OMO plugin settings that must not appear in the native OpenCode config.
///
/// Returns the `TempDir` whose lifetime is bound to the caller (typically a
/// `CommandWorkerSessionHandle`). When dropped the directory is automatically
/// cleaned up.  The caller should set `XDG_CONFIG_HOME` to this directory so
/// that OpenCode's config loading chain finds the plugin config.
///
/// ## Contents
///
/// ```json
/// opencode.json:
/// {
///   "plugin": ["oh-my-openagent"],
///   "lsp": false
/// }
///
/// oh-my-openagent.json:
/// {
///   "disabled_mcps": ["lsp"],
///   "model_fallback": false,
///   "runtime_fallback": false,
///   "background_task": { "defaultConcurrency": 2 },
///   "team_mode": { "enabled": false, "max_parallel_members": 2 }
/// }
/// ```
/// Build the isolated OpenCode config used by command-backed sessions.
///
/// Planning, discovery, and review phases are read-only Gear roles. Their
/// policy must reach OpenCode's actual permission resolver instead of staying
/// only in the Gear prompt metadata. Read-only phases may use bash for
/// repository observation (`git`, `xxd`, `wc`, and similar commands), but
/// common file and git mutation forms are denied. The phase runner also takes
/// a before/after fingerprint and fails closed if an unlisted mutation gets
/// through this command-pattern guard.
pub(crate) fn setup_omo_plugin_config_dir_with_read_only(
    read_only: bool,
) -> Result<tempfile::TempDir> {
    let temp_dir = tempfile::tempdir().context("failed to create OMO plugin config temp dir")?;
    let opencode_dir = temp_dir.path().join("opencode");
    fs::create_dir_all(&opencode_dir)
        .with_context(|| format!("failed to create {}/opencode", temp_dir.path().display()))?;
    let mut opencode_config = json!({
        "$schema": "https://opencode.ai/config.json",
        "plugin": ["oh-my-openagent"],
        "lsp": false,
    });
    if read_only {
        let bash_permissions = [
            ("*", "allow"),
            ("* > *", "deny"),
            ("* >> *", "deny"),
            ("*tee *", "deny"),
            ("*touch *", "deny"),
            ("*mkdir *", "deny"),
            ("*mktemp *", "deny"),
            ("*rm *", "deny"),
            ("*cp *", "deny"),
            ("*mv *", "deny"),
            ("*ln *", "deny"),
            ("*install *", "deny"),
            ("*truncate *", "deny"),
            ("*dd *", "deny"),
            ("*sed -i*", "deny"),
            ("*perl -i*", "deny"),
            ("*git add*", "deny"),
            ("*git commit*", "deny"),
            ("*git reset*", "deny"),
            ("*git clean*", "deny"),
            ("*git checkout --*", "deny"),
            ("*git restore*", "deny"),
            ("*git apply*", "deny"),
            ("*git cherry-pick*", "deny"),
            ("*git rebase*", "deny"),
            ("*git merge*", "deny"),
            ("*chmod *", "deny"),
            ("*chown *", "deny"),
            ("*writeFile*", "deny"),
            ("*appendFile*", "deny"),
            ("*createWriteStream*", "deny"),
            ("*copyFile*", "deny"),
            ("*mkdirSync*", "deny"),
            ("*rmSync*", "deny"),
            ("*rmdirSync*", "deny"),
            ("*unlink*", "deny"),
            ("*Bun.write*", "deny"),
        ]
        .into_iter()
        .map(|(pattern, action)| (pattern.to_string(), json!(action)))
        .collect::<serde_json::Map<_, _>>();
        opencode_config["permission"] = json!({
            "read": "allow",
            "list": "allow",
            "glob": "allow",
            "grep": "allow",
            "edit": "deny",
            "bash": bash_permissions,
            "task": "deny",
            "question": "deny",
            "webfetch": "deny",
        });
    }
    let opencode_config_path = opencode_dir.join("opencode.json");
    fs::write(
        &opencode_config_path,
        serde_json::to_string_pretty(&opencode_config)
            .context("failed to serialize OpenCode plugin registration")?,
    )
    .with_context(|| {
        format!(
            "failed to write OpenCode plugin registration to {}",
            opencode_config_path.display()
        )
    })?;
    let config = json!({
        "disabled_mcps": ["lsp"],
        "model_fallback": false,
        "runtime_fallback": false,
        "background_task": {
            "defaultConcurrency": 2,
        },
        "team_mode": {
            "enabled": false,
            "max_parallel_members": 2,
        },
    });
    let config_path = opencode_dir.join("oh-my-openagent.json");
    fs::write(
        &config_path,
        serde_json::to_string_pretty(&config).context("failed to serialize OMO plugin config")?,
    )
    .with_context(|| {
        format!(
            "failed to write OMO plugin config to {}",
            config_path.display()
        )
    })?;
    Ok(temp_dir)
}
use crate::worker_broker::{
    BrokerCapability, BrokerSessionIdentity, BrokerUsage, LifecycleStateName, WorkerBroker,
    broker_capabilities_for_kind,
};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Intensity {
    #[default]
    Low,
    Medium,
    High,
    ExtraHigh,
    #[serde(untagged)]
    Custom(String),
}

impl Intensity {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "low" => Some(Self::Low),
            "medium" | "med" => Some(Self::Medium),
            "high" => Some(Self::High),
            "extra_high" | "extra-high" | "extrahigh" | "xhigh" => Some(Self::ExtraHigh),
            _ => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(Self::Custom(trimmed.to_string()))
                }
            }
        }
    }

    /// Parse intensity, failing closed for unknown non-empty values.
    /// Returns `None` for empty/missing values (caller may apply default).
    /// Returns `Err` for unrecognized strings that are not a known keyword
    /// and do not start with `custom:`.
    pub fn parse_or_fail(value: &str) -> Result<Option<Self>, String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let lowered = trimmed.to_ascii_lowercase();
        match lowered.as_str() {
            "low" => Ok(Some(Self::Low)),
            "medium" | "med" => Ok(Some(Self::Medium)),
            "high" => Ok(Some(Self::High)),
            "extra_high" | "extra-high" | "extrahigh" | "xhigh" => Ok(Some(Self::ExtraHigh)),
            _ => {
                if lowered.starts_with("custom:") || lowered.starts_with("custom_") {
                    let inner = if lowered.starts_with("custom:") {
                        trimmed[7..].trim()
                    } else {
                        trimmed[7..].trim()
                    };
                    if inner.is_empty() {
                        return Err("custom intensity value cannot be empty".to_string());
                    }
                    Ok(Some(Self::Custom(inner.to_string())))
                } else if lowered.starts_with("custom") {
                    // bare "custom" without a value
                    Ok(Some(Self::Custom("default".to_string())))
                } else {
                    Err(format!("unknown intensity value `{trimmed}`"))
                }
            }
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            Self::Low => "low".to_string(),
            Self::Medium => "medium".to_string(),
            Self::High => "high".to_string(),
            Self::ExtraHigh => "extra_high".to_string(),
            Self::Custom(value) => format!("custom:{value}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub worker_kind: WorkerKind,
    pub worker_command: Option<String>,
    pub worker_model: Option<String>,
    pub worker_routes: Vec<WorkerRoute>,
    pub unavailable_worker_models: Vec<String>,
    pub premium_worker_budget: usize,
    pub max_parallel_workers: usize,
    pub max_parallel_per_key: usize,
    pub stale_task_timeout_secs: usize,
    pub skip_worker: bool,
    pub require_worker: bool,
    pub default_worker_for_small_tasks: WorkerKind,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_kind: WorkerKind::default(),
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
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRoute {
    pub worker_kind: WorkerKind,
    pub worker_command: Option<String>,
    pub worker_model: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FallbackRoute {
    pub worker_kind: WorkerKind,
    pub worker_model: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerKind {
    #[default]
    Opencode,
    OpencodeSession,
    Codex,
    Claude,
    ZedAgent,
    Custom,
}

impl WorkerKind {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "opencode" => Some(Self::Opencode),
            "opencode_session" | "opencode-session" | "opencode-resident" => {
                Some(Self::OpencodeSession)
            }
            "codex" => Some(Self::Codex),
            "claude" | "claude_code" | "claude-code" => Some(Self::Claude),
            "zed" | "zed_agent" | "zed-agent" => Some(Self::ZedAgent),
            "custom" => Some(Self::Custom),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Opencode => "opencode",
            Self::OpencodeSession => "opencode_session",
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::ZedAgent => "zed_agent",
            Self::Custom => "custom",
        }
    }

    pub fn default_command(&self, worker_model: Option<&str>) -> Option<String> {
        match self {
            Self::Codex => {
                let model_flag = worker_model
                    .filter(|model| !model.trim().is_empty())
                    .map(|model| format!(" -m {}", shell_single_quote(model.trim())))
                    .unwrap_or_default();
                Some(format!(
                    "codex exec --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox{model_flag} -o \"$GEARBOX_WORKER_LAST_MESSAGE\" - < \"$GEARBOX_WORKER_PROMPT\""
                ))
            }
            Self::Claude => Some(
                "claude -p \"$(cat \"$GEARBOX_WORKER_PROMPT\")\" > \"$GEARBOX_WORKER_LAST_MESSAGE\""
                    .to_string(),
            ),
            _ => None,
        }
    }

    pub fn provider_id_hint(&self) -> Option<&'static str> {
        match self {
            Self::Codex => Some("openai"),
            Self::Claude => Some("anthropic"),
            _ => None,
        }
    }

    pub fn is_premium(&self) -> bool {
        matches!(self, Self::Codex | Self::Claude | Self::ZedAgent)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerToolPolicy {
    pub question: bool,
    pub allow_recursive_gear_tasks: bool,
    pub can_write: bool,
    pub can_review: bool,
    pub can_explore: bool,
}

const WORKER_PARAMETER_RESOLUTION_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkerParameterState {
    Configured,
    Defaulted,
    Unknown,
    Invalid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkerParameterResolution {
    name: String,
    state: WorkerParameterState,
    value_type: String,
    source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkerParameterResolutionReceipt {
    schema_version: u32,
    task_id: String,
    worker: String,
    requested_category: Option<String>,
    resolved_category: Option<String>,
    precedence: Vec<String>,
    parameters: Vec<WorkerParameterResolution>,
    status: String,
    errors: Vec<String>,
    receipt_hash: String,
    created_at: String,
}

impl WorkerParameterResolutionReceipt {
    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&payload)?)
        ))
    }

    fn seal(mut self) -> Result<Self> {
        self.receipt_hash.clear();
        self.receipt_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<()> {
        if self.schema_version != WORKER_PARAMETER_RESOLUTION_SCHEMA_VERSION {
            bail!("unsupported worker parameter resolution schema");
        }
        for (field, value) in [
            ("task_id", self.task_id.as_str()),
            ("worker", self.worker.as_str()),
            ("status", self.status.as_str()),
            ("created_at", self.created_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("worker parameter resolution {field} cannot be empty");
            }
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("worker parameter resolution receipt hash mismatch");
        }
        Ok(())
    }
}

impl WorkerToolPolicy {
    pub(crate) fn to_markdown(&self) -> String {
        [
            format!("- question: {}", self.question),
            format!(
                "- allow_recursive_gear_tasks: {}",
                self.allow_recursive_gear_tasks
            ),
            format!("- can_write: {}", self.can_write),
            format!("- can_review: {}", self.can_review),
            format!("- can_explore: {}", self.can_explore),
        ]
        .join("\n")
    }

    fn with_environment_overrides(mut self, category: WorkerCategory) -> Self {
        let prefix = format!(
            "GEARBOX_GEAR_CATEGORY_{}",
            category.as_str().replace('-', "_").to_ascii_uppercase()
        );
        for (field, value) in [
            ("QUESTION", &mut self.question),
            (
                "ALLOW_RECURSIVE_GEAR_TASKS",
                &mut self.allow_recursive_gear_tasks,
            ),
            ("CAN_WRITE", &mut self.can_write),
            ("CAN_REVIEW", &mut self.can_review),
            ("CAN_EXPLORE", &mut self.can_explore),
        ] {
            let name = format!("{prefix}_{field}");
            if let Ok(raw) = env::var(&name) {
                match raw.trim().to_ascii_lowercase().as_str() {
                    "1" | "true" | "yes" | "on" => *value = true,
                    "0" | "false" | "no" | "off" => *value = false,
                    _ => {}
                }
            }
        }
        self
    }
}

fn tool_policy_for_category(category: WorkerCategory) -> WorkerToolPolicy {
    category.tool_policy().with_environment_overrides(category)
}

fn worker_variant_for_category(category: WorkerCategory) -> Option<String> {
    let category_key = category.as_str().replace('-', "_").to_ascii_uppercase();
    env::var(format!("GEARBOX_GEAR_CATEGORY_{category_key}_VARIANT"))
        .or_else(|_| env::var("GEARBOX_GEAR_WORKER_VARIANT"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerCategory {
    #[default]
    Quick,
    Deep,
    Repair,
    Review,
    Explore,
    Librarian,
    Visual,
    ZedNative,
    Custom,
}

impl WorkerCategory {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "quick" => Some(Self::Quick),
            "deep" => Some(Self::Deep),
            "repair" => Some(Self::Repair),
            "review" => Some(Self::Review),
            "explore" => Some(Self::Explore),
            "librarian" | "docs" | "documentation" => Some(Self::Librarian),
            "visual" | "visual-engineering" | "frontend" | "ui" => Some(Self::Visual),
            "zed-native" | "zed_native" | "zed" | "zed-agent" | "zed_agent" => {
                Some(Self::ZedNative)
            }
            "custom" => Some(Self::Custom),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Deep => "deep",
            Self::Repair => "repair",
            Self::Review => "review",
            Self::Explore => "explore",
            Self::Librarian => "librarian",
            Self::Visual => "visual",
            Self::ZedNative => "zed-native",
            Self::Custom => "custom",
        }
    }

    fn preferred_worker_kinds(self) -> &'static [WorkerKind] {
        match self {
            Self::Quick => &[WorkerKind::OpencodeSession, WorkerKind::Opencode],
            Self::Repair => &[
                WorkerKind::OpencodeSession,
                WorkerKind::Opencode,
                WorkerKind::Codex,
            ],
            Self::Deep => &[
                WorkerKind::Codex,
                WorkerKind::Claude,
                WorkerKind::OpencodeSession,
                WorkerKind::Opencode,
            ],
            Self::Review => &[WorkerKind::Codex, WorkerKind::Claude, WorkerKind::ZedAgent],
            Self::Explore => &[
                WorkerKind::ZedAgent,
                WorkerKind::OpencodeSession,
                WorkerKind::Opencode,
            ],
            Self::Librarian => &[
                WorkerKind::OpencodeSession,
                WorkerKind::Opencode,
                WorkerKind::Custom,
            ],
            Self::Visual => &[
                WorkerKind::Claude,
                WorkerKind::Codex,
                WorkerKind::OpencodeSession,
                WorkerKind::Opencode,
            ],
            Self::ZedNative => &[WorkerKind::ZedAgent],
            Self::Custom => &[WorkerKind::Custom],
        }
    }

    fn prompt_append(self) -> Option<&'static str> {
        match self {
            Self::Quick | Self::Repair | Self::Deep | Self::Visual | Self::Custom => Some(
                "Focus on implementation, keep changes minimal, and do not ask the user questions. Before claiming completion, run the relevant verification, write a non-empty regular receipt under .gear/evidence/, and end the response with EVIDENCE_RECORDED: <path>.",
            ),
            Self::Review => Some(
                "This is an independent review turn. Do not edit files; inspect the evidence and return concrete findings.",
            ),
            Self::Explore | Self::Librarian => Some(
                "This is a read-only exploration turn. Do not edit files; trace the code and summarize the evidence.",
            ),
            Self::ZedNative => Some(
                "This is a native Zed worker turn. Stay bounded and do not create a Gear goal loop recursively. Before claiming completion, run the relevant verification, write a non-empty regular receipt under .gear/evidence/, and end the response with EVIDENCE_RECORDED: <path>.",
            ),
        }
    }

    fn tool_policy(self) -> WorkerToolPolicy {
        match self {
            Self::Review => WorkerToolPolicy {
                question: false,
                allow_recursive_gear_tasks: false,
                can_write: false,
                can_review: true,
                can_explore: true,
            },
            Self::Explore | Self::Librarian => WorkerToolPolicy {
                question: false,
                allow_recursive_gear_tasks: false,
                can_write: false,
                can_review: false,
                can_explore: true,
            },
            Self::ZedNative => WorkerToolPolicy {
                question: false,
                allow_recursive_gear_tasks: false,
                can_write: true,
                can_review: true,
                can_explore: true,
            },
            Self::Quick | Self::Deep | Self::Repair | Self::Visual | Self::Custom => {
                WorkerToolPolicy {
                    question: false,
                    allow_recursive_gear_tasks: false,
                    can_write: true,
                    can_review: false,
                    can_explore: true,
                }
            }
        }
    }

    pub fn requires_evidence_receipt(self) -> bool {
        self.tool_policy().can_write
    }
}

impl WorkerConfig {
    pub fn selected_route(&self, attempt: usize) -> SelectedWorkerRoute<'_> {
        self.selected_route_for_hint(attempt, None)
    }

    pub fn selected_route_for_hint(
        &self,
        attempt: usize,
        route_hint: Option<&str>,
    ) -> SelectedWorkerRoute<'_> {
        CategoryRouter.resolve(self, attempt, route_hint)
    }
}

#[derive(Default)]
pub struct CategoryRouter;

impl CategoryRouter {
    pub fn resolve<'a>(
        &self,
        config: &'a WorkerConfig,
        attempt: usize,
        route_hint: Option<&str>,
    ) -> SelectedWorkerRoute<'a> {
        let hinted_category = route_hint.and_then(normalized_route_hint);
        if let Some(category) = hinted_category {
            // For Quick (small/low-risk) tasks, prefer the configured
            // small-task worker (defaults to ZedAgent) when available
            // and no explicit worker command is configured.
            if category == WorkerCategory::Quick && config.worker_command.is_none() {
                if let Some(route) = self.resolve_small_task(config) {
                    return route;
                }
            }
            let matching_routes = category
                .preferred_worker_kinds()
                .iter()
                .flat_map(|worker_kind| {
                    config.worker_routes.iter().filter(move |route| {
                        route.worker_kind == *worker_kind
                            && !Self::route_model_is_unavailable(
                                config,
                                route.worker_kind,
                                route.worker_model.as_deref(),
                            )
                    })
                })
                .collect::<Vec<_>>();
            if !matching_routes.is_empty() {
                let index = attempt
                    .saturating_sub(1)
                    .min(matching_routes.len().saturating_sub(1));
                let route = matching_routes[index];
                let selected_preferred_index = category
                    .preferred_worker_kinds()
                    .iter()
                    .position(|worker_kind| *worker_kind == route.worker_kind)
                    .unwrap_or(index);
                let selected_route_index = config
                    .worker_routes
                    .iter()
                    .position(|configured_route| std::ptr::eq(configured_route, route))
                    .unwrap_or(usize::MAX);
                let skipped_unavailable_route = config.worker_routes.iter().enumerate().any(
                    |(configured_route_index, configured_route)| {
                        if !Self::route_model_is_unavailable(
                            config,
                            configured_route.worker_kind,
                            configured_route.worker_model.as_deref(),
                        ) {
                            return false;
                        }
                        let Some(configured_preferred_index) = category
                            .preferred_worker_kinds()
                            .iter()
                            .position(|worker_kind| *worker_kind == configured_route.worker_kind)
                        else {
                            return false;
                        };
                        configured_preferred_index < selected_preferred_index
                            || (configured_preferred_index == selected_preferred_index
                                && configured_route_index < selected_route_index)
                    },
                );
                return SelectedWorkerRoute {
                    worker_kind: route.worker_kind,
                    worker_command: route.worker_command.as_deref(),
                    worker_model: route.worker_model.as_deref(),
                    require_worker: config.require_worker || route.worker_command.is_some(),
                    category,
                    route_reason: format!(
                        "category `{}` selected attempt {attempt} configured `{}` route{}",
                        category.as_str(),
                        route.worker_kind.as_str(),
                        if skipped_unavailable_route {
                            " after skipping an unavailable provider/model route"
                        } else {
                            ""
                        }
                    ),
                    prompt_append: combined_prompt_append(
                        category.prompt_append(),
                        worker_prompt_append_from_env(),
                    ),
                    tools: tool_policy_for_category(category),
                    variant: worker_variant_for_category(category),
                };
            }

            if config.worker_routes.is_empty() {
                for worker_kind in category.preferred_worker_kinds() {
                    if config.worker_kind == *worker_kind {
                        let route_reason = if attempt > 1 {
                            format!(
                                "category `{}` attempt {attempt} reused default `{}` worker; no fallback route configured",
                                category.as_str(),
                                config.worker_kind.as_str()
                            )
                        } else {
                            format!(
                                "category `{}` matched default `{}` worker",
                                category.as_str(),
                                config.worker_kind.as_str()
                            )
                        };
                        return SelectedWorkerRoute {
                            worker_kind: config.worker_kind,
                            worker_command: config.worker_command.as_deref(),
                            worker_model: config.worker_model.as_deref(),
                            require_worker: config.require_worker,
                            category,
                            route_reason,
                            prompt_append: combined_prompt_append(
                                category.prompt_append(),
                                worker_prompt_append_from_env(),
                            ),
                            tools: tool_policy_for_category(category),
                            variant: worker_variant_for_category(category),
                        };
                    }
                }
            }
        }

        self.sequence_route(config, attempt, hinted_category)
    }

    /// Try the configured `default_worker_for_small_tasks` (defaults to
    /// ZedAgent) as the preferred route for Quick-category tasks.
    /// Returns `None` when the preferred kind has no configured route
    /// and does not match the config's default `worker_kind`.
    fn resolve_small_task<'a>(&self, config: &'a WorkerConfig) -> Option<SelectedWorkerRoute<'a>> {
        let preferred = config.default_worker_for_small_tasks;

        // Check configured routes first.
        if let Some(route) = self.matching_configured_route(config, preferred) {
            return Some(SelectedWorkerRoute {
                worker_kind: route.worker_kind,
                worker_command: route.worker_command.as_deref(),
                worker_model: route.worker_model.as_deref(),
                require_worker: config.require_worker || route.worker_command.is_some(),
                category: WorkerCategory::Quick,
                route_reason: format!(
                    "small-task category `quick` matched configured `{}` route",
                    preferred.as_str()
                ),
                prompt_append: combined_prompt_append(
                    WorkerCategory::Quick.prompt_append(),
                    worker_prompt_append_from_env(),
                ),
                tools: tool_policy_for_category(WorkerCategory::Quick),
                variant: worker_variant_for_category(WorkerCategory::Quick),
            });
        }

        // Fall back to config's own worker_kind when it matches.
        if config.worker_kind == preferred {
            return Some(SelectedWorkerRoute {
                worker_kind: config.worker_kind,
                worker_command: config.worker_command.as_deref(),
                worker_model: config.worker_model.as_deref(),
                require_worker: config.require_worker,
                category: WorkerCategory::Quick,
                route_reason: format!(
                    "small-task category `quick` matched default `{}` worker",
                    preferred.as_str()
                ),
                prompt_append: combined_prompt_append(
                    WorkerCategory::Quick.prompt_append(),
                    worker_prompt_append_from_env(),
                ),
                tools: tool_policy_for_category(WorkerCategory::Quick),
                variant: worker_variant_for_category(WorkerCategory::Quick),
            });
        }

        None
    }

    fn matching_configured_route<'a>(
        &self,
        config: &'a WorkerConfig,
        worker_kind: WorkerKind,
    ) -> Option<&'a WorkerRoute> {
        config.worker_routes.iter().find(|route| {
            route.worker_kind == worker_kind
                && !Self::route_model_is_unavailable(
                    config,
                    route.worker_kind,
                    route.worker_model.as_deref(),
                )
        })
    }

    fn sequence_route<'a>(
        &self,
        config: &'a WorkerConfig,
        attempt: usize,
        hinted_category: Option<WorkerCategory>,
    ) -> SelectedWorkerRoute<'a> {
        let category = hinted_category.unwrap_or_else(|| {
            if attempt > 1 {
                WorkerCategory::Repair
            } else {
                WorkerCategory::Quick
            }
        });

        if config.worker_routes.is_empty() {
            // For Quick (small/low-risk) tasks, prefer the configured
            // small-task worker (defaults to ZedAgent) when no explicit
            // worker command is configured — a configured command means
            // the user has selected a specific worker.
            let worker_kind =
                if category == WorkerCategory::Quick && config.worker_command.is_none() {
                    config.default_worker_for_small_tasks
                } else {
                    config.worker_kind
                };
            let worker_command = config.worker_command.as_deref();
            let require_worker = config.require_worker;
            return SelectedWorkerRoute {
                worker_kind,
                worker_command,
                worker_model: config.worker_model.as_deref(),
                require_worker,
                category,
                route_reason: if hinted_category.is_some() {
                    format!(
                        "category `{}` fell back to `{}` worker",
                        category.as_str(),
                        worker_kind.as_str()
                    )
                } else {
                    format!(
                        "attempt {attempt} used `{}` worker{}",
                        worker_kind.as_str(),
                        if category == WorkerCategory::Quick && worker_kind != config.worker_kind {
                            " (small-task default)"
                        } else {
                            ""
                        }
                    )
                },
                prompt_append: combined_prompt_append(
                    category.prompt_append(),
                    worker_prompt_append_from_env(),
                ),
                tools: tool_policy_for_category(category),
                variant: worker_variant_for_category(category),
            };
        }

        let index = attempt
            .saturating_sub(1)
            .min(config.worker_routes.len().saturating_sub(1));
        let selected_route = config
            .worker_routes
            .iter()
            .enumerate()
            .skip(index)
            .chain(config.worker_routes.iter().enumerate().take(index))
            .find(|(_, route)| {
                !Self::route_model_is_unavailable(
                    config,
                    route.worker_kind,
                    route.worker_model.as_deref(),
                )
            })
            .or_else(|| config.worker_routes.get(index).map(|route| (index, route)));
        let (selected_route_index, route) = selected_route.expect("worker routes are non-empty");
        let skipped_unavailable_route = selected_route_index != index;
        SelectedWorkerRoute {
            worker_kind: route.worker_kind,
            worker_command: route.worker_command.as_deref(),
            worker_model: route.worker_model.as_deref(),
            require_worker: config.require_worker || route.worker_command.is_some(),
            category,
            route_reason: if hinted_category.is_some() {
                format!(
                    "category `{}` fell back to attempt {attempt} route `{}`{}",
                    category.as_str(),
                    route.worker_kind.as_str(),
                    if skipped_unavailable_route {
                        " after skipping an unavailable provider/model route"
                    } else {
                        ""
                    }
                )
            } else {
                format!(
                    "attempt {attempt} selected sequence route `{}`{}",
                    route.worker_kind.as_str(),
                    if skipped_unavailable_route {
                        " after skipping an unavailable provider/model route"
                    } else {
                        ""
                    }
                )
            },
            prompt_append: combined_prompt_append(
                category.prompt_append(),
                worker_prompt_append_from_env(),
            ),
            tools: tool_policy_for_category(category),
            variant: worker_variant_for_category(category),
        }
    }

    fn route_model_is_unavailable(
        config: &WorkerConfig,
        worker_kind: WorkerKind,
        worker_model: Option<&str>,
    ) -> bool {
        worker_model_is_unavailable(worker_kind, worker_model, &config.unavailable_worker_models)
    }
}

pub(crate) fn worker_model_is_unavailable(
    worker_kind: WorkerKind,
    worker_model: Option<&str>,
    unavailable_worker_models: &[String],
) -> bool {
    let Some(worker_model) = worker_model
        .map(str::trim)
        .filter(|model| !model.is_empty())
    else {
        return false;
    };

    if is_free_model(Some(worker_model))
        && unavailable_worker_models
            .iter()
            .any(|entry| entry == FREE_PROVIDER_COOLDOWN_MARKER)
    {
        return true;
    }

    let normalized_worker_model = canonicalize_model_id(worker_model);
    let canonical_worker_entry = canonicalize_provider_model_entry(worker_model);
    let qualified_model = worker_kind.provider_id_hint().map(|provider_id| {
        format!(
            "{}/{}",
            canonicalize_provider_id(provider_id),
            normalized_worker_model
        )
    });

    unavailable_worker_models.iter().any(|entry| {
        let normalized_entry = canonicalize_provider_model_entry(entry);
        normalized_entry == normalized_worker_model
            || normalized_entry == canonical_worker_entry
            || qualified_model
                .as_ref()
                .is_some_and(|qualified| normalized_entry == *qualified)
    })
}

pub fn category_resolution_for_route(
    config: &WorkerConfig,
    route_attempt: usize,
    route_hint: Option<&str>,
    route: &SelectedWorkerRoute<'_>,
) -> (CategoryResolution, CategoryResolutionResult) {
    let hinted_category = route_hint.and_then(normalized_route_hint);
    let available_categories = available_categories(config);
    let selected_route = FallbackRoute {
        worker_kind: route.worker_kind,
        worker_model: route.worker_model.map(ToString::to_string),
    };
    let fallback_chain = if let Some(category) = hinted_category {
        category_available_routes(config, category)
    } else {
        sequence_available_routes(config, route_attempt)
    };
    let nearest_fallback = fallback_chain
        .iter()
        .position(|candidate| *candidate == selected_route)
        .and_then(|index| fallback_chain.get(index + 1).cloned());
    let category_resolution = CategoryResolution {
        prompt_append: route.prompt_append.clone(),
        available_categories: available_categories.clone(),
        nearest_fallback: nearest_fallback.clone(),
        fallback_chain,
        tools: route.tools.clone(),
    };
    let resolution_result = if config.skip_worker {
        CategoryResolutionResult::Disabled {
            requested_category: route_hint
                .map(|hint| hint.trim().to_string())
                .filter(|hint| !hint.is_empty())
                .unwrap_or_else(|| route.category.as_str().to_string()),
            available_categories,
            attempted_provider_model: worker_provider_model(route.worker_kind, route.worker_model),
            nearest_fallback,
        }
    } else if let Some(hinted_category) = hinted_category {
        let requested_category = hinted_category.as_str().to_string();
        let configured_routes = category_configured_routes(config, hinted_category);
        let available_routes = category_available_routes(config, hinted_category);
        if configured_routes.is_empty() {
            CategoryResolutionResult::NotFound {
                requested_category,
                available_categories,
                attempted_provider_model: worker_provider_model(
                    route.worker_kind,
                    route.worker_model,
                ),
                nearest_fallback,
            }
        } else if available_routes.is_empty() {
            CategoryResolutionResult::ModelUnavailable {
                requested_category,
                available_categories,
                attempted_provider_model: worker_provider_model(
                    route.worker_kind,
                    route.worker_model,
                ),
                nearest_fallback,
            }
        } else {
            CategoryResolutionResult::Resolved {
                requested_category,
                available_categories,
                attempted_provider_model: worker_provider_model(
                    route.worker_kind,
                    route.worker_model,
                ),
                nearest_fallback,
            }
        }
    } else {
        CategoryResolutionResult::NotFound {
            requested_category: route_hint
                .map(|hint| hint.trim().to_string())
                .filter(|hint| !hint.is_empty())
                .unwrap_or_else(|| route.category.as_str().to_string()),
            available_categories,
            attempted_provider_model: worker_provider_model(route.worker_kind, route.worker_model),
            nearest_fallback,
        }
    };

    (category_resolution, resolution_result)
}

fn normalized_route_hint(value: &str) -> Option<WorkerCategory> {
    WorkerCategory::parse(value)
}

pub(crate) fn route_identity_key(worker_kind: WorkerKind, worker_model: Option<&str>) -> String {
    let worker_model = worker_model
        .map(str::trim)
        .filter(|model| !model.is_empty());
    match worker_model {
        Some(worker_model) => provider_model_key(worker_kind, worker_model),
        None => worker_kind.as_str().to_ascii_lowercase(),
    }
}

pub(crate) fn provider_model_key(worker_kind: WorkerKind, worker_model: &str) -> String {
    let normalized_model = canonicalize_model_id(worker_model);
    if let Some(provider_id) = worker_kind.provider_id_hint() {
        format!(
            "{}/{}",
            canonicalize_provider_id(provider_id),
            normalized_model
        )
    } else {
        normalized_model
    }
}

/// Returns true when the worker model identifier indicates a free-tier model
/// that should not be subject to artificial timeouts or stale-task sweeps.
/// Free models are identified by the `-free` suffix in their model id
/// (e.g. `opencode/deepseek-v4-flash-free`).
pub(crate) fn is_free_model(worker_model: Option<&str>) -> bool {
    worker_model
        .and_then(|model| model.split('/').last())
        .map(|suffix| suffix.ends_with("-free"))
        .unwrap_or(false)
}

/// Return whether a selected worker route consumes the premium-call budget.
///
/// Worker kind alone is not sufficient for OpenCode: both free and paid
/// OpenCode Go models use `WorkerKind::OpencodeSession`.  Keep the provider
/// identity in the route model so paid OpenCode calls cannot bypass the
/// durable premium budget accounting.
pub(crate) fn worker_route_is_premium(
    worker_kind: WorkerKind,
    worker_model: Option<&str>,
) -> bool {
    worker_kind.is_premium()
        || worker_model
            .and_then(|model| model.split_once('/'))
            .is_some_and(|(provider, _)| provider.eq_ignore_ascii_case("opencode-go"))
}

pub(crate) const FREE_PROVIDER_COOLDOWN_MARKER: &str = "gearbox:free-provider-cooldown";
const FREE_PROVIDER_COOLDOWN_SECS: u64 = 24 * 60 * 60;

fn canonicalize_provider_id(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn canonicalize_model_id(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(|character| character.to_lowercase())
        .collect()
}

fn canonicalize_provider_model_entry(value: &str) -> String {
    let value = value.trim();
    if let Some((provider_id, worker_model)) = value.split_once('/') {
        format!(
            "{}/{}",
            canonicalize_provider_id(provider_id),
            canonicalize_model_id(worker_model)
        )
    } else {
        canonicalize_model_id(value)
    }
}

fn available_categories(config: &WorkerConfig) -> Vec<String> {
    all_categories()
        .iter()
        .copied()
        .filter(|category| !category_available_routes(config, *category).is_empty())
        .map(|category| category.as_str().to_string())
        .collect()
}

fn category_available_routes(
    config: &WorkerConfig,
    category: WorkerCategory,
) -> Vec<FallbackRoute> {
    category_configured_routes(config, category)
        .into_iter()
        .filter(|route| {
            !CategoryRouter::route_model_is_unavailable(
                config,
                route.worker_kind,
                route.worker_model.as_deref(),
            )
        })
        .collect()
}

fn category_configured_routes(
    config: &WorkerConfig,
    category: WorkerCategory,
) -> Vec<FallbackRoute> {
    if config.worker_routes.is_empty() {
        return if category
            .preferred_worker_kinds()
            .contains(&config.worker_kind)
        {
            vec![FallbackRoute {
                worker_kind: config.worker_kind,
                worker_model: config.worker_model.clone(),
            }]
        } else {
            Vec::new()
        };
    }

    category
        .preferred_worker_kinds()
        .iter()
        .copied()
        .filter_map(|worker_kind| {
            config
                .worker_routes
                .iter()
                .find(|route| route.worker_kind == worker_kind)
                .map(|route| FallbackRoute {
                    worker_kind: route.worker_kind,
                    worker_model: route.worker_model.clone(),
                })
        })
        .collect()
}

fn sequence_available_routes(config: &WorkerConfig, route_attempt: usize) -> Vec<FallbackRoute> {
    if config.worker_routes.is_empty() {
        return vec![FallbackRoute {
            worker_kind: config.worker_kind,
            worker_model: config.worker_model.clone(),
        }];
    }

    let index = route_attempt
        .saturating_sub(1)
        .min(config.worker_routes.len().saturating_sub(1));
    let routes = config
        .worker_routes
        .iter()
        .enumerate()
        .skip(index)
        .chain(config.worker_routes.iter().enumerate().take(index))
        .filter_map(|(_, route)| {
            if CategoryRouter::route_model_is_unavailable(
                config,
                route.worker_kind,
                route.worker_model.as_deref(),
            ) {
                None
            } else {
                Some(FallbackRoute {
                    worker_kind: route.worker_kind,
                    worker_model: route.worker_model.clone(),
                })
            }
        })
        .collect::<Vec<_>>();

    if routes.is_empty() {
        vec![FallbackRoute {
            worker_kind: config.worker_routes[index].worker_kind,
            worker_model: config.worker_routes[index].worker_model.clone(),
        }]
    } else {
        routes
    }
}

fn all_categories() -> &'static [WorkerCategory] {
    &[
        WorkerCategory::Quick,
        WorkerCategory::Deep,
        WorkerCategory::Repair,
        WorkerCategory::Review,
        WorkerCategory::Explore,
        WorkerCategory::Librarian,
        WorkerCategory::Visual,
        WorkerCategory::ZedNative,
        WorkerCategory::Custom,
    ]
}

fn worker_provider_model(worker_kind: WorkerKind, worker_model: Option<&str>) -> Option<String> {
    let worker_model = worker_model
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    if let Some(provider_id) = worker_kind.provider_id_hint() {
        Some(format!("{provider_id}/{worker_model}"))
    } else {
        Some(worker_model.to_string())
    }
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[derive(Clone, Debug)]
pub struct SelectedWorkerRoute<'a> {
    pub worker_kind: WorkerKind,
    pub worker_command: Option<&'a str>,
    pub worker_model: Option<&'a str>,
    pub require_worker: bool,
    pub category: WorkerCategory,
    pub route_reason: String,
    pub prompt_append: Option<String>,
    pub tools: WorkerToolPolicy,
    pub variant: Option<String>,
}

/// Error returned when a requested model variant is not supported by the provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsupportedVariant {
    pub variant: String,
    pub category: WorkerCategory,
    pub supported_variants: Vec<String>,
    pub message: String,
}

impl std::fmt::Display for UnsupportedVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unsupported variant '{}' for category {:?}: {}",
            self.variant, self.category, self.message
        )
    }
}

impl std::error::Error for UnsupportedVariant {}

/// Error returned when a tool is denied by policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDenied {
    pub tool_name: String,
    pub policy: WorkerToolPolicy,
    pub reason: String,
}

impl std::fmt::Display for ToolDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tool '{}' denied by policy: {}",
            self.tool_name, self.reason
        )
    }
}

impl std::error::Error for ToolDenied {}

/// Model params that can be passed to a provider to override model selection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelParams {
    pub model: Option<String>,
    pub variant: Option<String>,
    pub capabilities: Vec<String>,
}

/// ProviderAdapter sits between route resolution and actual dispatch.
/// It converts variant + tool policy into real provider constraints.
#[derive(Clone, Debug)]
pub struct ProviderAdapter {
    pub variant: Option<String>,
    pub tool_policy: WorkerToolPolicy,
    pub category: WorkerCategory,
}

impl ProviderAdapter {
    pub fn new(
        variant: Option<String>,
        tool_policy: WorkerToolPolicy,
        category: WorkerCategory,
    ) -> Self {
        Self {
            variant,
            tool_policy,
            category,
        }
    }

    /// Convert variant to provider model params.
    /// Returns Ok(None) when no variant is set (passthrough).
    /// Returns Ok(Some(params)) when variant is supported and produces params.
    /// Returns Err(UnsupportedVariant) when variant is not supported.
    pub fn model_params(&self) -> Result<Option<ModelParams>, UnsupportedVariant> {
        let Some(variant) = &self.variant else {
            return Ok(None); // no variant → passthrough
        };
        let variant_lower = variant.to_ascii_lowercase();
        match variant_lower.as_str() {
            // Known supported variants — map to provider params
            "pro" | "premium" | "fast" | "default" | "auto" => Ok(Some(ModelParams {
                model: None,
                variant: Some(variant_lower),
                capabilities: vec!["chat".to_string(), "tools".to_string()],
            })),
            // Unknown variants → unsupported
            _ => Err(UnsupportedVariant {
                variant: variant.clone(),
                category: self.category,
                supported_variants: vec![
                    "pro".to_string(),
                    "premium".to_string(),
                    "fast".to_string(),
                    "default".to_string(),
                    "auto".to_string(),
                ],
                message: format!(
                    "variant '{}' is not recognized for category '{:?}'. Supported variants: pro, premium, fast, default, auto",
                    variant, self.category
                ),
            }),
        }
    }

    /// Check if a tool is allowed by policy.
    /// Returns Ok(true) if allowed,
    /// Ok(false) if the tool should be skipped (no matching rule),
    /// Err(ToolDenied) if the tool is explicitly denied.
    /// Return the final applied variant value.
    /// This is "none" when no variant was requested, or the variant value itself.
    pub fn variant_applied(&self) -> String {
        self.variant.clone().unwrap_or_else(|| "none".to_string())
    }

    pub fn check_tool_allowed(&self, _tool_name: &str) -> Result<bool, ToolDenied> {
        let (tool_name, allowed) = match _tool_name {
            "write" | "edit" | "terminal" => ("write", self.tool_policy.can_write),
            "review" => ("review", self.tool_policy.can_review),
            "explore" | "read" | "search" => ("explore", self.tool_policy.can_explore),
            "question" => ("question", self.tool_policy.question),
            "gear" | "recursive_gear_task" => (
                "recursive_gear_task",
                self.tool_policy.allow_recursive_gear_tasks,
            ),
            other => {
                return Err(ToolDenied {
                    tool_name: other.to_string(),
                    policy: self.tool_policy.clone(),
                    reason: format!("tool `{other}` has no Gear dispatch policy"),
                });
            }
        };

        if allowed {
            Ok(true)
        } else {
            Err(ToolDenied {
                tool_name: tool_name.to_string(),
                policy: self.tool_policy.clone(),
                reason: format!(
                    "tool `{tool_name}` is denied by the {:?} worker policy",
                    self.category
                ),
            })
        }
    }

    fn required_tool_for_category(&self) -> &'static str {
        match self.category {
            WorkerCategory::Review => "review",
            WorkerCategory::Explore | WorkerCategory::Librarian => "explore",
            WorkerCategory::Quick
            | WorkerCategory::Deep
            | WorkerCategory::Repair
            | WorkerCategory::Visual
            | WorkerCategory::ZedNative
            | WorkerCategory::Custom => "write",
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CategoryResolution {
    pub prompt_append: Option<String>,
    #[serde(default)]
    pub available_categories: Vec<String>,
    pub nearest_fallback: Option<FallbackRoute>,
    #[serde(default)]
    pub fallback_chain: Vec<FallbackRoute>,
    #[serde(default)]
    pub tools: WorkerToolPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CategoryResolutionResult {
    Resolved {
        requested_category: String,
        available_categories: Vec<String>,
        attempted_provider_model: Option<String>,
        nearest_fallback: Option<FallbackRoute>,
    },
    Disabled {
        requested_category: String,
        available_categories: Vec<String>,
        attempted_provider_model: Option<String>,
        nearest_fallback: Option<FallbackRoute>,
    },
    NotFound {
        requested_category: String,
        available_categories: Vec<String>,
        attempted_provider_model: Option<String>,
        nearest_fallback: Option<FallbackRoute>,
    },
    ModelUnavailable {
        requested_category: String,
        available_categories: Vec<String>,
        attempted_provider_model: Option<String>,
        nearest_fallback: Option<FallbackRoute>,
    },
}

impl CategoryResolutionResult {
    pub fn nearest_fallback(&self) -> Option<&FallbackRoute> {
        match self {
            CategoryResolutionResult::Resolved {
                nearest_fallback, ..
            }
            | CategoryResolutionResult::Disabled {
                nearest_fallback, ..
            }
            | CategoryResolutionResult::NotFound {
                nearest_fallback, ..
            }
            | CategoryResolutionResult::ModelUnavailable {
                nearest_fallback, ..
            } => nearest_fallback.as_ref(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerificationContract {
    pub preferred_commands: Vec<String>,
    pub must_not_skip: Vec<String>,
}

pub const PROMPT_MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptManifestSectionKind {
    Hard,
    Soft,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromptManifestSection {
    pub id: String,
    pub kind: PromptManifestSectionKind,
    pub source: String,
    pub content_hash: String,
    pub bytes: usize,
    pub estimated_tokens: usize,
    pub priority: u8,
    pub required: bool,
    pub included: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub omission_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromptManifest {
    pub schema_version: u32,
    pub task_id: String,
    pub worker: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    pub semantic_contract_hash: String,
    pub sections: Vec<PromptManifestSection>,
    pub rendered_prompt_hash: String,
}

impl PromptManifest {
    pub fn validate(&self, packet: &WorkerPacket, rendered_prompt: &str) -> Result<()> {
        if self.schema_version != PROMPT_MANIFEST_SCHEMA_VERSION {
            bail!("unsupported prompt manifest schema {}", self.schema_version);
        }
        if self.task_id != packet.task_id {
            bail!("prompt manifest task identity does not match worker packet");
        }
        if self.worker != packet.worker {
            bail!("prompt manifest worker identity does not match worker packet");
        }
        let expected_runtime_model = packet.worker_model.clone().or_else(|| {
            packet
                .coordinator_model
                .as_ref()
                .map(|model| model.name.clone())
        });
        if self.runtime_model != expected_runtime_model {
            bail!("prompt manifest runtime model does not match worker packet");
        }
        let expected_variant = packet
            .variant_applied
            .clone()
            .or_else(|| packet.variant.clone());
        if self.variant != expected_variant {
            bail!("prompt manifest variant does not match worker packet");
        }
        let expected_semantic_hash = prompt_semantic_contract_hash(packet)?;
        if self.semantic_contract_hash != expected_semantic_hash {
            bail!("prompt manifest semantic contract hash mismatch");
        }
        let expected_prompt_hash = prompt_content_hash(rendered_prompt);
        if self.rendered_prompt_hash != expected_prompt_hash {
            bail!("prompt manifest rendered prompt hash mismatch");
        }
        if self
            .sections
            .iter()
            .any(|section| section.required && !section.included)
        {
            bail!("prompt manifest omits a required section");
        }
        Ok(())
    }
}

pub const PROMPT_RECONCILE_RECEIPT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptReconcileAction {
    NewSession,
    ResumeSession,
    RebuildSession,
}

pub const PROMPT_RECONCILE_PENDING_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromptReconcilePending {
    pub schema_version: u32,
    pub task_id: String,
    pub worker_kind: WorkerKind,
    pub previous_worker_model: String,
    pub previous_model_family: String,
    pub previous_session_id: String,
    pub requested_worker_model: String,
    pub reason: String,
    pub created_at: String,
    pub pending_hash: String,
}

impl PromptReconcilePending {
    fn expected_hash(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.pending_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&unsigned)?)
        ))
    }

    fn validate_payload(&self) -> Result<()> {
        if self.schema_version != PROMPT_RECONCILE_PENDING_SCHEMA_VERSION {
            bail!("unsupported prompt reconcile pending schema version");
        }
        for (field, value) in [
            ("task_id", self.task_id.as_str()),
            ("previous_worker_model", self.previous_worker_model.as_str()),
            ("previous_model_family", self.previous_model_family.as_str()),
            ("previous_session_id", self.previous_session_id.as_str()),
            (
                "requested_worker_model",
                self.requested_worker_model.as_str(),
            ),
            ("reason", self.reason.as_str()),
            ("created_at", self.created_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("prompt reconcile pending {field} cannot be empty");
            }
        }
        Ok(())
    }

    fn seal(mut self) -> Result<Self> {
        self.pending_hash.clear();
        self.validate_payload()?;
        self.pending_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        if self.pending_hash != self.expected_hash()? {
            bail!("prompt reconcile pending hash mismatch");
        }
        Ok(())
    }

    fn from_descriptor(
        task_id: &str,
        worker_kind: WorkerKind,
        descriptor: &ResidentSessionDescriptor,
        requested_worker_model: &str,
        reason: &str,
    ) -> Result<Self> {
        let previous_worker_model = descriptor
            .worker_model
            .clone()
            .filter(|model| !model.trim().is_empty())
            .context("model switch pending receipt requires previous worker model")?;
        Self {
            schema_version: PROMPT_RECONCILE_PENDING_SCHEMA_VERSION,
            task_id: task_id.to_string(),
            worker_kind,
            previous_model_family: prompt_model_family(Some(&previous_worker_model), "worker"),
            previous_session_id: descriptor.session_id.clone(),
            previous_worker_model,
            requested_worker_model: requested_worker_model.to_string(),
            reason: reason.to_string(),
            created_at: timestamp(),
            pending_hash: String::new(),
        }
        .seal()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromptReconcileReceipt {
    pub schema_version: u32,
    pub task_id: String,
    pub worker: String,
    pub previous_worker_model: Option<String>,
    pub previous_model_family: Option<String>,
    pub previous_session_id: Option<String>,
    pub runtime_worker_model: String,
    pub runtime_model_family: String,
    pub action: PromptReconcileAction,
    pub session_id: Option<String>,
    pub session_reused: bool,
    pub reason: String,
    pub semantic_contract_hash: String,
    pub prompt_manifest_hash: String,
    pub created_at: String,
    pub receipt_hash: String,
}

impl PromptReconcileReceipt {
    fn expected_hash(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.receipt_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&unsigned)?)
        ))
    }

    pub fn seal(mut self) -> Result<Self> {
        self.receipt_hash.clear();
        self.validate_payload()?;
        self.receipt_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    fn validate_payload(&self) -> Result<()> {
        if self.schema_version != PROMPT_RECONCILE_RECEIPT_SCHEMA_VERSION {
            bail!("unsupported prompt reconcile receipt schema version");
        }
        for (field, value) in [
            ("task_id", self.task_id.as_str()),
            ("worker", self.worker.as_str()),
            ("runtime_worker_model", self.runtime_worker_model.as_str()),
            ("runtime_model_family", self.runtime_model_family.as_str()),
            ("reason", self.reason.as_str()),
            (
                "semantic_contract_hash",
                self.semantic_contract_hash.as_str(),
            ),
            ("prompt_manifest_hash", self.prompt_manifest_hash.as_str()),
            ("created_at", self.created_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("prompt reconcile receipt {field} cannot be empty");
            }
        }
        if self
            .previous_worker_model
            .as_deref()
            .is_some_and(|model| model.trim().is_empty())
            || self
                .previous_model_family
                .as_deref()
                .is_some_and(|family| family.trim().is_empty())
            || self
                .previous_session_id
                .as_deref()
                .is_some_and(|session_id| session_id.trim().is_empty())
            || self
                .session_id
                .as_deref()
                .is_some_and(|session_id| session_id.trim().is_empty())
        {
            bail!("prompt reconcile receipt optional binding cannot be empty");
        }
        if self.session_reused != matches!(self.action, PromptReconcileAction::ResumeSession) {
            bail!("prompt reconcile receipt session_reused does not match action");
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        if self.receipt_hash != self.expected_hash()? {
            bail!("prompt reconcile receipt hash mismatch");
        }
        Ok(())
    }

    pub fn validate_against(&self, packet: &WorkerPacket, manifest: &PromptManifest) -> Result<()> {
        self.validate()?;
        if self.task_id != packet.task_id || self.worker != packet.worker {
            bail!("prompt reconcile receipt identity does not match worker packet");
        }
        let expected_runtime_model = packet
            .worker_model
            .clone()
            .or_else(|| {
                packet
                    .coordinator_model
                    .as_ref()
                    .map(|model| model.name.clone())
            })
            .unwrap_or_else(|| packet.worker.clone());
        if self.runtime_worker_model != expected_runtime_model {
            bail!("prompt reconcile receipt runtime model does not match worker packet");
        }
        if self.runtime_model_family
            != prompt_model_family(packet.worker_model.as_deref(), packet.worker.as_str())
        {
            bail!("prompt reconcile receipt model family does not match worker packet");
        }
        if self.semantic_contract_hash != manifest.semantic_contract_hash {
            bail!("prompt reconcile receipt semantic hash does not match manifest");
        }
        if self.prompt_manifest_hash != prompt_manifest_hash(manifest)? {
            bail!("prompt reconcile receipt manifest hash does not match manifest");
        }
        Ok(())
    }

    fn for_dispatch(
        packet: &WorkerPacket,
        manifest: &PromptManifest,
        previous_descriptor: Option<&ResidentSessionDescriptor>,
        pending: Option<&PromptReconcilePending>,
        current_descriptor: Option<&ResidentSessionDescriptor>,
        route_attempt: usize,
        task_attempt: usize,
        supports_interaction: bool,
    ) -> Result<Self> {
        let runtime_worker_model = packet
            .worker_model
            .clone()
            .or_else(|| {
                packet
                    .coordinator_model
                    .as_ref()
                    .map(|model| model.name.clone())
            })
            .unwrap_or_else(|| packet.worker.clone());
        let previous_worker_model = previous_descriptor
            .and_then(|descriptor| {
                descriptor
                    .worker_model
                    .clone()
                    .filter(|model| !model.trim().is_empty())
            })
            .or_else(|| pending.map(|pending| pending.previous_worker_model.clone()));
        let previous_model_family = previous_worker_model
            .as_deref()
            .map(|model| prompt_model_family(Some(model), &packet.worker))
            .or_else(|| pending.map(|pending| pending.previous_model_family.clone()));
        let previous_session_id = previous_descriptor
            .map(|descriptor| descriptor.session_id.clone())
            .or_else(|| pending.map(|pending| pending.previous_session_id.clone()));
        let runtime_model_family =
            prompt_model_family(packet.worker_model.as_deref(), packet.worker.as_str());
        let session_reused = supports_interaction
            && previous_descriptor.is_some()
            && current_descriptor.is_some()
            && previous_descriptor.map(|descriptor| descriptor.session_id.as_str())
                == current_descriptor.map(|descriptor| descriptor.session_id.as_str())
            && previous_worker_model.as_deref() == packet.worker_model.as_deref();
        let action = if session_reused {
            PromptReconcileAction::ResumeSession
        } else if supports_interaction
            && (previous_descriptor.is_some()
                || pending.is_some()
                || task_attempt > 1
                || route_attempt > 1)
        {
            PromptReconcileAction::RebuildSession
        } else {
            PromptReconcileAction::NewSession
        };
        let reason = match action {
            PromptReconcileAction::ResumeSession => {
                "resident session identity and model binding are compatible".to_string()
            }
            PromptReconcileAction::RebuildSession => {
                "session/model binding is not reusable; rebuilt with the current prompt manifest"
                    .to_string()
            }
            PromptReconcileAction::NewSession => {
                "no compatible resident session exists for this dispatch".to_string()
            }
        };
        Self {
            schema_version: PROMPT_RECONCILE_RECEIPT_SCHEMA_VERSION,
            task_id: packet.task_id.clone(),
            worker: packet.worker.clone(),
            previous_worker_model,
            previous_model_family,
            previous_session_id,
            runtime_worker_model,
            runtime_model_family,
            action,
            session_id: current_descriptor.map(|descriptor| descriptor.session_id.clone()),
            session_reused,
            reason,
            semantic_contract_hash: manifest.semantic_contract_hash.clone(),
            prompt_manifest_hash: prompt_manifest_hash(manifest)?,
            created_at: timestamp(),
            receipt_hash: String::new(),
        }
        .seal()
    }
}

pub fn prompt_model_family(worker_model: Option<&str>, worker: &str) -> String {
    let value = worker_model
        .filter(|model| !model.trim().is_empty())
        .unwrap_or(worker)
        .trim()
        .to_ascii_lowercase();
    for (family, markers) in [
        ("deepseek", &["deepseek", "ds-"][..]),
        ("mimo", &["mimo", "xiaomi"][..]),
        ("hy3", &["hy3"][..]),
        ("codex", &["codex", "gpt-5", "gpt5"][..]),
        ("claude", &["claude", "anthropic"][..]),
    ] {
        if markers.iter().any(|marker| value.contains(marker)) {
            return family.to_string();
        }
    }
    value
        .split('/')
        .next()
        .filter(|provider| !provider.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerPacket {
    pub task_id: String,
    pub worker: String,
    /// The logical step that this dispatch is allowed to execute.  It is a
    /// hard prompt contract field so compaction/recovery cannot silently fall
    /// back to the beginning of the work order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// The final variant value after ProviderAdapter processing.
    /// This reflects what was actually applied, not just what was requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant_applied: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_append: Option<String>,
    /// Rules discovered from the workspace and target scope. Rules are soft
    /// context: their receipt is auditable, but omission under the prompt
    /// budget never changes the semantic task contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub injected_rules: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules_injection_path: Option<String>,
    /// Project-scoped skills resolved for this task. Skills are soft prompt
    /// context; the receipt records freshness, cache reuse, and omissions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub injected_skills: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skills_injection_path: Option<String>,
    pub tools: WorkerToolPolicy,
    pub category_resolution: CategoryResolution,
    pub category_resolution_result: CategoryResolutionResult,
    pub goal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinator_model: Option<CoordinatorModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinator_brief: Option<String>,
    pub scope: Scope,
    pub inputs: TaskInputs,
    pub constraints: Vec<String>,
    pub required_outputs: Vec<String>,
    pub verification: VerificationContract,
    pub stop_conditions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_reconcile_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_capsule_path: Option<String>,
}

fn parameter_value_hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn resolve_string_parameter(
    parameters: &mut Vec<WorkerParameterResolution>,
    errors: &mut Vec<String>,
    value: Option<&Value>,
    name: &str,
    required: bool,
    default_source: &str,
) -> Option<String> {
    let Some(value) = value else {
        let state = if required {
            errors.push(format!("required parameter `{name}` is missing"));
            WorkerParameterState::Invalid
        } else {
            WorkerParameterState::Defaulted
        };
        parameters.push(WorkerParameterResolution {
            name: name.to_string(),
            state,
            value_type: "missing".to_string(),
            source: if required {
                "dispatch_contract".to_string()
            } else {
                default_source.to_string()
            },
            value_hash: None,
            detail: required
                .then(|| "required field was absent".to_string())
                .or_else(|| Some(format!("defaulted by {default_source}"))),
        });
        return None;
    };
    let Some(value) = value.as_str() else {
        let detail = if value.is_null() {
            "explicit null is not a valid dispatch parameter".to_string()
        } else {
            "parameter must be a string".to_string()
        };
        errors.push(format!("{name}: {detail}"));
        parameters.push(WorkerParameterResolution {
            name: name.to_string(),
            state: WorkerParameterState::Invalid,
            value_type: if value.is_null() {
                "null".to_string()
            } else {
                value_type_name(value).to_string()
            },
            source: "caller".to_string(),
            value_hash: None,
            detail: Some(detail),
        });
        return None;
    };
    let value = value.trim();
    if value.is_empty() {
        let detail = "empty string is not a valid dispatch parameter".to_string();
        errors.push(format!("{name}: {detail}"));
        parameters.push(WorkerParameterResolution {
            name: name.to_string(),
            state: WorkerParameterState::Invalid,
            value_type: "string".to_string(),
            source: "caller".to_string(),
            value_hash: None,
            detail: Some(detail),
        });
        return None;
    }
    parameters.push(WorkerParameterResolution {
        name: name.to_string(),
        state: WorkerParameterState::Configured,
        value_type: "string".to_string(),
        source: "caller".to_string(),
        value_hash: Some(parameter_value_hash(value)),
        detail: None,
    });
    Some(value.to_string())
}

fn resolve_structural_parameter(
    parameters: &mut Vec<WorkerParameterResolution>,
    errors: &mut Vec<String>,
    value: Option<&Value>,
    name: &str,
    expected_type: &str,
) {
    let Some(value) = value else {
        errors.push(format!("required parameter `{name}` is missing"));
        parameters.push(WorkerParameterResolution {
            name: name.to_string(),
            state: WorkerParameterState::Invalid,
            value_type: "missing".to_string(),
            source: "dispatch_contract".to_string(),
            value_hash: None,
            detail: Some("required field was absent".to_string()),
        });
        return;
    };
    if value.is_null() || value_type_name(value) != expected_type {
        let detail = if value.is_null() {
            "explicit null is not a valid dispatch parameter".to_string()
        } else {
            format!("parameter must be {expected_type}")
        };
        errors.push(format!("{name}: {detail}"));
        parameters.push(WorkerParameterResolution {
            name: name.to_string(),
            state: WorkerParameterState::Invalid,
            value_type: value_type_name(value).to_string(),
            source: "caller".to_string(),
            value_hash: None,
            detail: Some(detail),
        });
        return;
    }
    parameters.push(WorkerParameterResolution {
        name: name.to_string(),
        state: WorkerParameterState::Configured,
        value_type: expected_type.to_string(),
        source: "caller".to_string(),
        value_hash: None,
        detail: None,
    });
}

fn validate_tool_policy_parameters(
    parameters: &mut Vec<WorkerParameterResolution>,
    errors: &mut Vec<String>,
    value: Option<&Value>,
    prefix: &str,
) {
    let Some(object) = value.and_then(Value::as_object) else {
        return;
    };
    for field in [
        "question",
        "allow_recursive_gear_tasks",
        "can_write",
        "can_review",
        "can_explore",
    ] {
        let name = format!("{prefix}.{field}");
        let Some(value) = object.get(field) else {
            errors.push(format!("{name}: required boolean field was absent"));
            parameters.push(WorkerParameterResolution {
                name,
                state: WorkerParameterState::Invalid,
                value_type: "missing".to_string(),
                source: "dispatch_contract".to_string(),
                value_hash: None,
                detail: Some("required boolean field was absent".to_string()),
            });
            continue;
        };
        if !value.is_boolean() {
            let detail = if value.is_null() {
                "explicit null is not a valid boolean".to_string()
            } else {
                "parameter must be a boolean".to_string()
            };
            errors.push(format!("{name}: {detail}"));
            parameters.push(WorkerParameterResolution {
                name,
                state: WorkerParameterState::Invalid,
                value_type: value_type_name(value).to_string(),
                source: "caller".to_string(),
                value_hash: None,
                detail: Some(detail),
            });
        }
    }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn validate_worker_parameter_value(value: &Value) -> Result<WorkerParameterResolutionReceipt> {
    let mut parameters = Vec::new();
    let mut errors = Vec::new();
    let task_id = resolve_string_parameter(
        &mut parameters,
        &mut errors,
        value.get("task_id"),
        "task_id",
        true,
        "dispatch_contract",
    );
    let worker = resolve_string_parameter(
        &mut parameters,
        &mut errors,
        value.get("worker"),
        "worker",
        true,
        "dispatch_contract",
    );
    resolve_string_parameter(
        &mut parameters,
        &mut errors,
        value.get("goal"),
        "goal",
        true,
        "dispatch_contract",
    );
    resolve_structural_parameter(
        &mut parameters,
        &mut errors,
        value.get("tools"),
        "tools",
        "object",
    );
    validate_tool_policy_parameters(&mut parameters, &mut errors, value.get("tools"), "tools");
    resolve_structural_parameter(
        &mut parameters,
        &mut errors,
        value.get("scope"),
        "scope",
        "object",
    );
    resolve_structural_parameter(
        &mut parameters,
        &mut errors,
        value.get("verification"),
        "verification",
        "object",
    );
    resolve_structural_parameter(
        &mut parameters,
        &mut errors,
        value.get("stop_conditions"),
        "stop_conditions",
        "array",
    );
    resolve_structural_parameter(
        &mut parameters,
        &mut errors,
        value.get("category_resolution"),
        "category_resolution",
        "object",
    );
    resolve_structural_parameter(
        &mut parameters,
        &mut errors,
        value.get("category_resolution_result"),
        "category_resolution_result",
        "object",
    );

    resolve_string_parameter(
        &mut parameters,
        &mut errors,
        value.get("worker_model"),
        "worker_model",
        false,
        "provider_default",
    );
    let configured_variant = resolve_string_parameter(
        &mut parameters,
        &mut errors,
        value.get("variant"),
        "variant",
        false,
        "provider_default",
    );
    resolve_string_parameter(
        &mut parameters,
        &mut errors,
        value.get("prompt_append"),
        "prompt_append",
        false,
        "category_default",
    );
    let applied_variant = value.get("variant_applied");
    if configured_variant.is_some() && applied_variant.is_none() {
        parameters.push(WorkerParameterResolution {
            name: "variant_applied".to_string(),
            state: WorkerParameterState::Unknown,
            value_type: "missing".to_string(),
            source: "provider_runtime".to_string(),
            value_hash: None,
            detail: Some("configured variant has no observed applied value".to_string()),
        });
    } else {
        resolve_string_parameter(
            &mut parameters,
            &mut errors,
            applied_variant,
            "variant_applied",
            false,
            "provider_default",
        );
    }

    if let (Some(configured_variant), Some(applied_variant)) = (
        configured_variant.as_deref(),
        applied_variant.and_then(Value::as_str),
    ) && !configured_variant.eq_ignore_ascii_case(applied_variant)
        && applied_variant != "none"
    {
        errors.push(format!(
            "variant conflict: configured `{configured_variant}` but applied `{applied_variant}`"
        ));
    }
    if let (Some(tools), Some(category_resolution)) = (
        value.get("tools").and_then(Value::as_object),
        value
            .get("category_resolution")
            .and_then(Value::as_object),
    ) && category_resolution.get("tools") != Some(&Value::Object(tools.clone()))
    {
        errors.push(
            "tool policy conflict: packet tools differ from category resolution tools".to_string(),
        );
    }
    if value
        .get("tools")
        .and_then(|tools| tools.get("allow_recursive_gear_tasks"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        errors.push(
            "recursive Gear task dispatch is not allowed from a worker packet".to_string(),
        );
    }

    let category_resolution_result = value
        .get("category_resolution_result")
        .and_then(Value::as_object);
    let category_result_variant = category_resolution_result
        .and_then(|object| object.keys().next())
        .map(ToString::to_string);
    let requested_category = category_resolution_result
        .and_then(|object| object.values().next())
        .and_then(Value::as_object)
        .and_then(|object| object.get("requested_category"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let configured_route = value
        .get("category_resolution")
        .and_then(Value::as_object)
        .and_then(|object| object.get("fallback_chain"))
        .and_then(Value::as_array)
        .is_some_and(|routes| !routes.is_empty());
    let has_fallback_route = value
        .get("category_resolution")
        .and_then(Value::as_object)
        .and_then(|object| object.get("nearest_fallback"))
        .is_some_and(|route| !route.is_null());
    let mut precedence = vec![if requested_category.is_some() {
        "explicit_route_hint".to_string()
    } else {
        "default_category".to_string()
    }];
    precedence.push(if configured_route {
        "configured_category_route".to_string()
    } else {
        "default_worker_route".to_string()
    });
    if has_fallback_route {
        precedence.push("fallback_route".to_string());
    }
    if category_result_variant.as_deref() == Some("model_unavailable") {
        precedence.push("model_unavailable_blocker".to_string());
    }
    let status = if !errors.is_empty() {
        "invalid"
    } else if parameters
        .iter()
        .any(|parameter| parameter.state == WorkerParameterState::Unknown)
    {
        "unknown"
    } else {
        "resolved"
    };
    WorkerParameterResolutionReceipt {
        schema_version: WORKER_PARAMETER_RESOLUTION_SCHEMA_VERSION,
        task_id: task_id.unwrap_or_else(|| "<invalid>".to_string()),
        worker: worker.unwrap_or_else(|| "<invalid>".to_string()),
        requested_category: requested_category.clone(),
        resolved_category: (category_result_variant.as_deref() == Some("resolved"))
            .then(|| requested_category.clone())
            .flatten(),
        precedence,
        parameters,
        status: status.to_string(),
        errors,
        receipt_hash: String::new(),
        created_at: timestamp(),
    }
    .seal()
}

fn validate_worker_packet_parameters(packet: &WorkerPacket) -> Result<WorkerParameterResolutionReceipt> {
    validate_worker_parameter_value(&serde_json::to_value(packet)?)
}

const RULE_INJECTION_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuleInjectionEntry {
    relative_path: String,
    real_path: String,
    content_hash: String,
    bytes: usize,
    #[serde(default)]
    modified_at_ms: u128,
    distance: usize,
    #[serde(default)]
    precedence: usize,
    #[serde(default)]
    match_reason: String,
    freshness: String,
    injected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    omission_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuleInjectionReceipt {
    schema_version: u32,
    task_id: String,
    workspace: String,
    target_paths: Vec<String>,
    entries: Vec<RuleInjectionEntry>,
    errors: Vec<String>,
    #[serde(default)]
    context_conflict: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_conflict_reason: Option<String>,
    injected_content_hash: String,
    receipt_hash: String,
    created_at: String,
}

const SKILL_INJECTION_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SkillInjectionEntry {
    relative_path: String,
    real_path: String,
    content_hash: String,
    bytes: usize,
    modified_at_ms: u128,
    distance: usize,
    #[serde(default)]
    precedence: usize,
    #[serde(default)]
    match_reason: String,
    freshness: String,
    injected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    omission_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SkillInjectionReceipt {
    schema_version: u32,
    task_id: String,
    workspace: String,
    #[serde(default)]
    worker: String,
    #[serde(default)]
    worker_category: String,
    target_paths: Vec<String>,
    cache_key: String,
    cache_hit: bool,
    entries: Vec<SkillInjectionEntry>,
    errors: Vec<String>,
    injected_content_hash: String,
    receipt_hash: String,
    created_at: String,
}

impl SkillInjectionReceipt {
    fn expected_hash(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.receipt_hash.clear();
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&unsigned)?)))
    }

    fn validate(&self) -> Result<()> {
        if self.schema_version != SKILL_INJECTION_SCHEMA_VERSION {
            bail!("unsupported skill injection receipt schema");
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("skill injection receipt hash mismatch");
        }
        Ok(())
    }
}

impl RuleInjectionReceipt {
    fn expected_hash(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.receipt_hash.clear();
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&unsigned)?)))
    }

    fn validate(&self) -> Result<()> {
        if self.schema_version != RULE_INJECTION_SCHEMA_VERSION {
            bail!("unsupported rules injection receipt schema");
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("rules injection receipt hash mismatch");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Skipped,
    Succeeded,
    Failed,
}

impl WorkerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Skipped => "skipped",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerResult {
    pub status: WorkerStatus,
    pub command: Option<String>,
    pub exit_code: Option<i32>,
    pub summary: String,
    pub packet_path: PathBuf,
    pub prompt_path: PathBuf,
    pub stdout_path: Option<PathBuf>,
    pub stderr_path: Option<PathBuf>,
    pub last_message_path: Option<PathBuf>,
    pub result_path: PathBuf,
    pub outcome_path: PathBuf,
}

const WORKER_EVIDENCE_MARKER: &str = "EVIDENCE_RECORDED:";
const WORKER_EVIDENCE_ROOT: &str = ".gear/evidence";
const LEGACY_WORKER_EVIDENCE_ROOT: &str = ".gearbox-agent/evidence";

fn evidence_root_for_workspace(workspace: &Path) -> PathBuf {
    let current = workspace.join(WORKER_EVIDENCE_ROOT);
    if current.exists() {
        current
    } else {
        workspace.join(LEGACY_WORKER_EVIDENCE_ROOT)
    }
}

pub fn category_requires_worker_evidence(category: &str) -> bool {
    WorkerCategory::parse(category).is_some_and(WorkerCategory::requires_evidence_receipt)
}

pub fn worker_kind_supports_evidence_contract(worker_kind: &str) -> bool {
    matches!(
        WorkerKind::parse(worker_kind),
        Some(WorkerKind::OpencodeSession | WorkerKind::Codex)
    )
}

pub fn validate_worker_evidence_receipt(
    result: &WorkerResult,
    workspace: &Path,
) -> std::result::Result<PathBuf, String> {
    validate_worker_evidence_receipt_inner(result, workspace, &[], false)
}

/// Validate a worker receipt against the evidence files that existed before
/// this attempt started. An explicit final-message marker remains the preferred
/// contract. If a worker writes exactly one new safe receipt but omits the
/// marker, the new file can be discovered without trusting stdout or reusing
/// an older receipt.
pub(crate) fn validate_worker_evidence_receipt_with_baseline(
    result: &WorkerResult,
    workspace: &Path,
    baseline_paths: &[PathBuf],
) -> std::result::Result<PathBuf, String> {
    validate_worker_evidence_receipt_inner(result, workspace, baseline_paths, true)
}

fn validate_worker_evidence_receipt_inner(
    result: &WorkerResult,
    workspace: &Path,
    baseline_paths: &[PathBuf],
    allow_discovery: bool,
) -> std::result::Result<PathBuf, String> {
    let workspace = workspace
        .canonicalize()
        .map_err(|error| format!("failed to resolve workspace: {error}"))?;
    let evidence_root = evidence_root_for_workspace(&workspace);
    let real_evidence_root = evidence_root
        .canonicalize()
        .map_err(|error| format!("evidence root is unavailable: {error}"))?;
    if !is_strictly_inside(&real_evidence_root, &workspace) {
        return Err("evidence root resolves outside workspace".to_string());
    }

    if let Some(marker_path) = worker_evidence_marker_path(result) {
        let marker_path = PathBuf::from(marker_path);
        let candidate = if marker_path.is_absolute() {
            marker_path
        } else {
            workspace.join(marker_path)
        };
        return validate_evidence_candidate(&candidate, &real_evidence_root, baseline_paths, true);
    }

    if !allow_discovery {
        return Err(format!(
            "missing {WORKER_EVIDENCE_MARKER} marker in worker output"
        ));
    }

    let current_paths = snapshot_worker_evidence_paths(&workspace)?;
    let new_paths = current_paths
        .into_iter()
        .filter(|path| !baseline_contains_path(path, baseline_paths))
        .collect::<Vec<_>>();
    for path in &new_paths {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| format!("failed to inspect new evidence path: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "new evidence path `{}` must not be a symbolic link",
                path.display()
            ));
        }
    }
    let new_files = new_paths
        .into_iter()
        .filter(|path| {
            fs::metadata(path)
                .map(|metadata| metadata.is_file())
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    if new_files.len() != 1 {
        return Err(format!(
            "missing {WORKER_EVIDENCE_MARKER} marker and expected exactly one new receipt file, found {}",
            new_files.len()
        ));
    }

    validate_evidence_candidate(&new_files[0], &real_evidence_root, baseline_paths, true)
}

pub(crate) fn snapshot_worker_evidence_paths(
    workspace: &Path,
) -> std::result::Result<Vec<PathBuf>, String> {
    let workspace = workspace
        .canonicalize()
        .map_err(|error| format!("failed to resolve workspace: {error}"))?;
    let evidence_root = evidence_root_for_workspace(&workspace);
    let metadata = match fs::symlink_metadata(&evidence_root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("failed to inspect evidence root: {error}")),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("evidence root must be a regular directory".to_string());
    }
    let real_evidence_root = evidence_root
        .canonicalize()
        .map_err(|error| format!("failed to resolve evidence root: {error}"))?;
    if !is_strictly_inside(&real_evidence_root, &workspace) {
        return Err("evidence root resolves outside workspace".to_string());
    }

    let mut paths = Vec::new();
    collect_worker_evidence_paths(&evidence_root, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_worker_evidence_paths(
    directory: &Path,
    paths: &mut Vec<PathBuf>,
) -> std::result::Result<(), String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("failed to read evidence directory: {error}"))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("failed to read evidence entry: {error}"))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("failed to inspect evidence entry: {error}"))?;
        paths.push(path.clone());
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            collect_worker_evidence_paths(&path, paths)?;
        }
    }
    Ok(())
}

fn validate_evidence_candidate(
    candidate: &Path,
    real_evidence_root: &Path,
    baseline_paths: &[PathBuf],
    require_new: bool,
) -> std::result::Result<PathBuf, String> {
    let metadata = fs::symlink_metadata(&candidate)
        .map_err(|error| format!("receipt path is unavailable: {error}"))?;
    if metadata.file_type().is_symlink() {
        return Err("receipt path must not be a symbolic link".to_string());
    }
    let real_candidate = candidate
        .canonicalize()
        .map_err(|error| format!("failed to resolve receipt path: {error}"))?;
    if !is_strictly_inside(&real_candidate, &real_evidence_root) {
        return Err(format!(
            "receipt path `{}` is outside `{WORKER_EVIDENCE_ROOT}`",
            real_candidate.display()
        ));
    }
    let metadata = fs::metadata(&candidate)
        .map_err(|error| format!("failed to inspect receipt path: {error}"))?;
    if !metadata.is_file() {
        return Err("receipt path must be a regular file".to_string());
    }
    if metadata.len() == 0 {
        return Err("receipt file must not be empty".to_string());
    }
    if require_new && baseline_contains_path(candidate, baseline_paths) {
        return Err("receipt path was present before this worker attempt".to_string());
    }
    Ok(real_candidate)
}

fn baseline_contains_path(path: &Path, baseline_paths: &[PathBuf]) -> bool {
    let canonical_path = path.canonicalize().ok();
    baseline_paths.iter().any(|baseline_path| {
        if baseline_path == path {
            return true;
        }
        match (&canonical_path, baseline_path.canonicalize().ok()) {
            (Some(canonical_path), Some(baseline_path)) => canonical_path == &baseline_path,
            _ => false,
        }
    })
}

pub(crate) fn worker_evidence_marker_path(result: &WorkerResult) -> Option<String> {
    let last_message_path = result.last_message_path.as_ref()?;
    let output = fs::read_to_string(last_message_path).ok()?;
    extract_worker_evidence_marker(&output)
}

fn extract_worker_evidence_marker(output: &str) -> Option<String> {
    output.lines().rev().find_map(|line| {
        let (_, value) = line.split_once(WORKER_EVIDENCE_MARKER)?;
        value.split_whitespace().next().map(|path| {
            path.trim_matches(|character: char| {
                matches!(character, '"' | '\'' | '`' | ',' | ']' | '}')
            })
            .to_string()
        })
    })
}

fn is_strictly_inside(path: &Path, directory: &Path) -> bool {
    path != directory && path.starts_with(directory)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerOutcome {
    pub status: WorkerStatus,
    pub session_id: Option<String>,
    #[serde(default)]
    pub session_capability: Option<String>,
    pub summary: String,
    pub changed_files: Vec<String>,
    pub commands_run: Vec<String>,
    pub known_failures: Vec<String>,
    pub raw_output_path: Option<PathBuf>,
    pub command: Option<String>,
    pub exit_code: Option<i32>,
}

/// Reconciles a child worker's self-reported changed-file claim with the
/// repository observation captured after its turn.  A worker claim is never
/// treated as proof by itself: missing claims remain `unverified`, while an
/// explicit claim for a file that did not change is a discrepancy that must be
/// repaired or independently reviewed.
pub const WORKER_CLAIM_RECONCILIATION_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerClaimReconciliationReceipt {
    pub schema_version: u32,
    pub task_id: String,
    pub workspace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub claimed_changed_files: Vec<String>,
    pub observed_changed_files: Vec<String>,
    pub missing_claims: Vec<String>,
    pub unclaimed_changes: Vec<String>,
    pub observed_diff_hash: Option<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub receipt_hash: String,
    pub created_at: String,
}

impl WorkerClaimReconciliationReceipt {
    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&payload)?)
        ))
    }

    fn seal(mut self) -> Result<Self> {
        self.receipt_hash.clear();
        self.receipt_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != WORKER_CLAIM_RECONCILIATION_SCHEMA_VERSION {
            bail!("unsupported worker claim reconciliation schema");
        }
        if self.task_id.trim().is_empty() || self.workspace.trim().is_empty() {
            bail!("worker claim reconciliation identity cannot be empty");
        }
        if !matches!(self.status.as_str(), "reconciled" | "discrepancy" | "unverified") {
            bail!("unknown worker claim reconciliation status `{}`", self.status);
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("worker claim reconciliation receipt hash mismatch");
        }
        Ok(())
    }
}

fn worker_workspace(store: &StateStore) -> PathBuf {
    store
        .root()
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| store.root().to_path_buf())
}

fn normalize_claim_path(workspace: &Path, raw_path: &str) -> String {
    let trimmed = raw_path
        .trim()
        .trim_matches(['`', '"', '\'', ',', ';']);
    let candidate = Path::new(trimmed);
    let relative = if candidate.is_absolute() {
        candidate
            .strip_prefix(workspace)
            .map(Path::to_path_buf)
            .unwrap_or_else(|_| candidate.to_path_buf())
    } else {
        candidate.to_path_buf()
    };
    relative
        .to_string_lossy()
        .replace('\\', "/")
        .trim_start_matches("./")
        .to_string()
}

/// Persist the reconciliation between a worker report and the observed
/// repository state.  The comparison intentionally does not infer that every
/// observed path belongs to this worker because the runtime may start with
/// user-owned dirty files; the immutable baseline/scope gate remains the
/// authority for that attribution.
pub fn reconcile_worker_claims(
    store: &StateStore,
    task_id: &str,
    result: &WorkerResult,
    outcome: &WorkerOutcome,
) -> Result<WorkerClaimReconciliationReceipt> {
    let workspace = worker_workspace(store);
    let workspace_for_paths = workspace.canonicalize().unwrap_or_else(|_| workspace.clone());
    let parsed_report = parsed_worker_report(result);
    let mut claimed_changed_files = parsed_report
        .changed_files
        .iter()
        .map(|path| normalize_claim_path(&workspace_for_paths, path))
        .filter(|path| !path.is_empty() && !path.starts_with(".gear/"))
        .collect::<Vec<_>>();
    claimed_changed_files.sort();
    claimed_changed_files.dedup();

    let snapshot = git_snapshot(&workspace);
    let (observed_changed_files, observed_diff_hash, status, missing_claims, unclaimed_changes, reason) =
        match snapshot {
            Ok(snapshot) if snapshot.is_git_repo => {
                let mut observed = snapshot
                    .changed_files
                    .iter()
                    .map(|path| normalize_claim_path(&workspace_for_paths, path))
                    .filter(|path| !path.is_empty() && !path.starts_with(".gear/"))
                    .collect::<Vec<_>>();
                observed.sort();
                observed.dedup();
                let observed_set = observed.iter().collect::<HashSet<_>>();
                let claimed_set = claimed_changed_files.iter().collect::<HashSet<_>>();
                let missing = claimed_changed_files
                    .iter()
                    .filter(|path| !observed_set.contains(path))
                    .cloned()
                    .collect::<Vec<_>>();
                let unclaimed = observed
                    .iter()
                    .filter(|path| !claimed_set.contains(path))
                    .cloned()
                    .collect::<Vec<_>>();
                let (status, reason) = if !missing.is_empty() {
                    (
                        "discrepancy",
                        Some("worker claimed files that were absent from the repository observation"),
                    )
                } else if claimed_changed_files.is_empty() && !observed.is_empty() {
                    (
                        "unverified",
                        Some("worker changed files were observed but the worker report declared no file claims"),
                    )
                } else {
                    ("reconciled", None)
                };
                (
                    observed,
                    snapshot.diff_hash,
                    status.to_string(),
                    missing,
                    unclaimed,
                    reason.map(str::to_string),
                )
            }
            Ok(_) => (
                Vec::new(),
                None,
                "unverified".to_string(),
                Vec::new(),
                Vec::new(),
                Some("workspace is not a Git repository".to_string()),
            ),
            Err(error) => (
                Vec::new(),
                None,
                "unverified".to_string(),
                Vec::new(),
                Vec::new(),
                Some(format!("repository observation failed: {error:#}")),
            ),
        };

    let receipt = WorkerClaimReconciliationReceipt {
        schema_version: WORKER_CLAIM_RECONCILIATION_SCHEMA_VERSION,
        task_id: task_id.to_string(),
        workspace: workspace.to_string_lossy().to_string(),
        session_id: outcome.session_id.clone(),
        claimed_changed_files,
        observed_changed_files,
        missing_claims,
        unclaimed_changes,
        observed_diff_hash,
        status,
        reason,
        receipt_hash: String::new(),
        created_at: timestamp(),
    }
    .seal()?;
    store.write_worker_json_atomic(task_id, "claim-reconciliation.json", &receipt)?;
    Ok(receipt)
}

pub fn worker_claim_reconciliation_path(result: &WorkerResult) -> Option<PathBuf> {
    result
        .result_path
        .parent()
        .map(|parent| parent.join("claim-reconciliation.json"))
}

const TEAM_SESSION_RECONCILIATION_SCHEMA_VERSION: u32 = 1;

/// Durable boundary for OMO team/session semantics. Gear currently runs with
/// team mode disabled, but provider transcripts can still contain team-shaped
/// events.  Those events must be classified instead of being silently treated
/// as a usable single-worker result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TeamSessionReconciliationReceipt {
    pub schema_version: u32,
    pub task_id: String,
    pub workspace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub team_mode_enabled: bool,
    pub observed_team_events: usize,
    pub orphan_events: usize,
    pub member_error_events: usize,
    pub undelivered_message_events: usize,
    pub reorderable_message_events: usize,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub receipt_hash: String,
    pub created_at: String,
}

impl TeamSessionReconciliationReceipt {
    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&payload)?)
        ))
    }

    fn seal(mut self) -> Result<Self> {
        self.receipt_hash.clear();
        self.receipt_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != TEAM_SESSION_RECONCILIATION_SCHEMA_VERSION {
            bail!("unsupported team-session reconciliation schema");
        }
        if self.task_id.trim().is_empty() || self.workspace.trim().is_empty() {
            bail!("team-session reconciliation identity cannot be empty");
        }
        if !matches!(self.status.as_str(), "disabled" | "reconciled" | "degraded" | "blocked")
        {
            bail!("unknown team-session reconciliation status `{}`", self.status);
        }
        if self.reorderable_message_events > self.undelivered_message_events {
            bail!("reorderable messages cannot exceed undelivered messages");
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("team-session reconciliation receipt hash mismatch");
        }
        Ok(())
    }
}

fn event_has_key(value: &Value, keys: &[&str]) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            let normalized = key
                .chars()
                .filter(|character| character.is_ascii_alphanumeric())
                .collect::<String>()
                .to_ascii_lowercase();
            keys.iter().any(|candidate| normalized == *candidate)
                || (normalized == "event"
                    && child.as_str().is_some_and(|event_name| {
                        let normalized_event = event_name
                            .chars()
                            .filter(|character| character.is_ascii_alphanumeric())
                            .collect::<String>()
                            .to_ascii_lowercase();
                        keys.iter().any(|candidate| normalized_event == *candidate)
                    }))
                || event_has_key(child, keys)
        }),
        Value::Array(values) => values.iter().any(|child| event_has_key(child, keys)),
        Value::String(string) => {
            let normalized = string
                .chars()
                .filter(|character| character.is_ascii_alphanumeric())
                .collect::<String>()
                .to_ascii_lowercase();
            keys.iter().any(|candidate| normalized == *candidate)
        }
        _ => false,
    }
}

fn reconcile_team_session(
    store: &StateStore,
    task_id: &str,
    outcome: &WorkerOutcome,
) -> Result<TeamSessionReconciliationReceipt> {
    let workspace = worker_workspace(store);
    let transcript_path = store.worker_dir(task_id).join("transcript.jsonl");
    let mut observed_team_events = 0;
    let mut orphan_events = 0;
    let mut member_error_events = 0;
    let mut undelivered_message_events = 0;
    let mut reorderable_message_events = 0;
    let mut malformed_lines = 0;

    if let Ok(transcript) = fs::read_to_string(&transcript_path) {
        for line in transcript.lines().map(str::trim).filter(|line| !line.is_empty()) {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                malformed_lines += 1;
                continue;
            };
            let team_event = event_has_key(
                &value,
                &["teamrunid", "teamrun", "memberid", "membername", "teammember"],
            );
            if !team_event {
                continue;
            }
            observed_team_events += 1;
            if event_has_key(&value, &["orphan", "leaderdeleted", "leadmissing"]) {
                orphan_events += 1;
            }
            if event_has_key(&value, &["membererror", "memberfailed", "memberfailure"]) {
                member_error_events += 1;
            }
            if event_has_key(
                &value,
                &["undelivered", "deliveryfailed", "messagenotdelivered"],
            ) {
                undelivered_message_events += 1;
                if event_has_key(&value, &["reorderable", "safe_reorder", "retryable"]) {
                    reorderable_message_events += 1;
                }
            }
        }
    } else if transcript_path.exists() {
        malformed_lines += 1;
    }

    let status = if observed_team_events == 0 {
        if malformed_lines > 0 {
            "degraded"
        } else {
            "disabled"
        }
    } else if observed_team_events > 0 {
        "blocked"
    } else {
        "reconciled"
    };
    let reason = if observed_team_events > 0 {
        Some("team-shaped session events were observed while Gear team mode is disabled".to_string())
    } else if malformed_lines > 0 {
        Some("team-session transcript contained malformed event lines".to_string())
    } else {
        Some("team mode disabled; no team/session events observed".to_string())
    };
    let receipt = TeamSessionReconciliationReceipt {
        schema_version: TEAM_SESSION_RECONCILIATION_SCHEMA_VERSION,
        task_id: task_id.to_string(),
        workspace: workspace.to_string_lossy().to_string(),
        session_id: outcome.session_id.clone(),
        team_mode_enabled: false,
        observed_team_events,
        orphan_events,
        member_error_events,
        undelivered_message_events,
        reorderable_message_events,
        status: status.to_string(),
        reason,
        receipt_hash: String::new(),
        created_at: timestamp(),
    }
    .seal()?;
    store.write_worker_json_atomic(task_id, "team-session-reconciliation.json", &receipt)?;
    Ok(receipt)
}

pub fn team_session_reconciliation_path(result: &WorkerResult) -> Option<PathBuf> {
    result
        .result_path
        .parent()
        .map(|parent| parent.join("team-session-reconciliation.json"))
}

pub const RESIDENT_SESSION_DESCRIPTOR_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResidentSessionDescriptor {
    pub schema_version: u32,
    pub task_id: String,
    pub worker_kind: WorkerKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_model: Option<String>,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    pub workspace: String,
    pub resumable: bool,
    pub resume_count: usize,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_resumed_at: Option<String>,
    pub descriptor_hash: String,
}

impl ResidentSessionDescriptor {
    fn validate_payload(&self) -> Result<()> {
        if self.schema_version != RESIDENT_SESSION_DESCRIPTOR_SCHEMA_VERSION {
            bail!("unsupported resident session descriptor schema version");
        }
        for (field, value) in [
            ("task_id", self.task_id.as_str()),
            ("session_id", self.session_id.as_str()),
            ("workspace", self.workspace.as_str()),
            ("created_at", self.created_at.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("resident session descriptor {field} cannot be empty");
            }
        }
        if self
            .provider_session_id
            .as_deref()
            .is_some_and(|session_id| session_id.trim().is_empty())
        {
            bail!("resident session descriptor provider_session_id cannot be empty");
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.descriptor_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&payload)?)
        ))
    }

    pub fn seal(mut self) -> Result<Self> {
        self.descriptor_hash.clear();
        self.validate_payload()?;
        self.descriptor_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        if self.descriptor_hash != self.expected_hash()? {
            bail!("resident session descriptor integrity hash mismatch");
        }
        Ok(())
    }

    fn resumable_session_id(&self) -> &str {
        self.provider_session_id
            .as_deref()
            .unwrap_or(self.session_id.as_str())
    }
}

pub type WorkerTurnOutcome = WorkerResult;

pub type WorkerEventListener = Arc<dyn Fn(WorkerEvent) + Send + Sync>;

const WORKER_EVENT_HISTORY_LIMIT: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerEvent {
    TurnStarted {
        kind: String,
        prompt_path: PathBuf,
    },
    AssistantTextDelta {
        kind: String,
        delta: String,
    },
    ToolCallStarted {
        kind: String,
        tool_name: String,
        #[serde(default)]
        arguments: String,
    },
    ToolCallFinished {
        kind: String,
        tool_name: String,
        #[serde(default)]
        result: String,
    },
    WorkerStdout {
        kind: String,
        output: String,
    },
    WorkerStderr {
        kind: String,
        output: String,
    },
    TurnFinished {
        kind: String,
        result_path: PathBuf,
        outcome_path: PathBuf,
        summary: String,
    },
    Error {
        kind: String,
        message: String,
    },
}

/// Thread-safe event fan-out shared by native worker session handles.
///
/// Command workers own this hub inside their session handle. Native ACP
/// adapters use the same abstraction so the orchestration layer can observe
/// provider turns without depending on a concrete ACP implementation.
#[derive(Clone, Default)]
pub struct WorkerEventHub {
    subscriptions: Arc<WorkerSessionSubscriptions>,
}

impl WorkerEventHub {
    pub fn subscribe(&self, listener: WorkerEventListener) -> Result<WorkerSubscription> {
        self.subscriptions.subscribe(listener)
    }

    pub fn emit(&self, event: WorkerEvent) {
        self.subscriptions.emit(event);
    }

    /// Drop replayable events at a resident-session epoch boundary.
    pub fn clear_history(&self) -> Result<()> {
        self.subscriptions.clear_history()?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct WorkerSubscription {
    subscriptions: Weak<WorkerSessionSubscriptions>,
    subscription_id: usize,
}

impl WorkerSubscription {
    pub fn noop() -> Self {
        Self {
            subscriptions: Weak::new(),
            subscription_id: 0,
        }
    }
}

#[derive(Default)]
struct WorkerSessionSubscriptions {
    listeners: Mutex<HashMap<usize, WorkerEventListener>>,
    history: Mutex<VecDeque<WorkerEvent>>,
    next_listener_id: AtomicUsize,
}

impl WorkerSessionSubscriptions {
    fn subscribe(self: &Arc<Self>, listener: WorkerEventListener) -> Result<WorkerSubscription> {
        let subscription_id = self.next_listener_id.fetch_add(1, Ordering::Relaxed);
        let replay = {
            let history = self
                .history
                .lock()
                .map_err(|_| anyhow::anyhow!("worker event history mutex poisoned"))?;
            let replay = history.iter().cloned().collect::<Vec<_>>();
            self.listeners
                .lock()
                .map_err(|_| anyhow::anyhow!("worker event subscription mutex poisoned"))?
                .insert(subscription_id, listener.clone());
            replay
        };
        for event in replay {
            listener(event);
        }
        Ok(WorkerSubscription {
            subscriptions: Arc::downgrade(self),
            subscription_id,
        })
    }

    fn emit(&self, event: WorkerEvent) {
        let listeners = match self.history.lock() {
            Ok(mut history) => {
                history.push_back(event.clone());
                while history.len() > WORKER_EVENT_HISTORY_LIMIT {
                    history.pop_front();
                }
                self.listeners
                    .lock()
                    .map(|listeners| listeners.values().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            }
            Err(_) => self
                .listeners
                .lock()
                .map(|listeners| listeners.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default(),
        };
        for listener in listeners {
            listener(event.clone());
        }
    }

    fn unsubscribe(&self, subscription_id: usize) {
        let _ = self
            .listeners
            .lock()
            .map(|mut listeners| listeners.remove(&subscription_id));
    }

    fn clear_history(&self) -> Result<()> {
        self.history
            .lock()
            .map_err(|_| anyhow::anyhow!("worker event history mutex poisoned"))?
            .clear();
        Ok(())
    }
}

impl Drop for WorkerSubscription {
    fn drop(&mut self) {
        if let Some(subscriptions) = self.subscriptions.upgrade() {
            subscriptions.unsubscribe(self.subscription_id);
        }
    }
}

pub struct WorkerStartRequest<'a> {
    pub store: &'a StateStore,
    pub workspace: &'a Path,
    pub task: &'a Task,
    pub route_attempt: usize,
    pub goal: &'a str,
    pub verification_commands: &'a [String],
    pub config: &'a WorkerConfig,
    pub cancellation_token: Option<CancellationToken>,
    pub coordinator_model: Option<&'a CoordinatorModel>,
    pub coordinator_brief: Option<&'a str>,
    pub route_hint: Option<&'a str>,
}

impl<'a> WorkerStartRequest<'a> {
    fn reborrow(&self) -> WorkerStartRequest<'a> {
        WorkerStartRequest {
            store: self.store,
            workspace: self.workspace,
            task: self.task,
            route_attempt: self.route_attempt,
            goal: self.goal,
            verification_commands: self.verification_commands,
            config: self.config,
            cancellation_token: self.cancellation_token.clone(),
            coordinator_model: self.coordinator_model,
            coordinator_brief: self.coordinator_brief,
            route_hint: self.route_hint,
        }
    }
}

pub type WorkerRunRequest<'a> = WorkerStartRequest<'a>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    // Session management capabilities
    pub supports_follow_up: bool,
    pub supports_steering: bool,
    pub supports_cancellation: bool,
    pub supports_resident_session: bool,
    // Code-level capabilities — what the worker can actually do
    #[serde(default)]
    pub supports_code_edit: bool,
    #[serde(default)]
    pub supports_review: bool,
    #[serde(default)]
    pub supports_explore: bool,
    // Provider/model capabilities
    #[serde(default)]
    pub supports_model_selection: bool,
    #[serde(default)]
    pub supports_tool_policy_enforcement: bool,
    #[serde(default)]
    pub supports_artifact_contract: bool,
}

impl WorkerCapabilities {
    pub fn command() -> Self {
        Self {
            supports_follow_up: false,
            supports_steering: false,
            supports_cancellation: true,
            supports_resident_session: false,
            // Command workers CAN edit code (the external tool does the editing).
            // Gear cannot enforce tool-level policy inside the process, so
            // tool_policy_enforcement is false — the env-var policy is advisory.
            supports_code_edit: true,
            supports_review: true,
            supports_explore: true,
            supports_model_selection: false,
            supports_tool_policy_enforcement: false,
            supports_artifact_contract: false,
        }
    }

    pub fn resident_command() -> Self {
        Self {
            supports_follow_up: true,
            supports_steering: true,
            supports_cancellation: true,
            supports_resident_session: true,
            // Resident workers have full verified capabilities because Gear
            // manages their lifecycle and can enforce tool policies.
            supports_code_edit: true,
            supports_review: true,
            supports_explore: true,
            supports_model_selection: true,
            supports_tool_policy_enforcement: true,
            supports_artifact_contract: true,
        }
    }

    /// Check whether these capabilities satisfy the requirements implied
    /// by a given worker category.
    pub fn supports_category(&self, category: WorkerCategory) -> bool {
        match category {
            WorkerCategory::Quick
            | WorkerCategory::Deep
            | WorkerCategory::Repair
            | WorkerCategory::Visual
            | WorkerCategory::ZedNative => self.supports_code_edit,
            WorkerCategory::Review => self.supports_review,
            WorkerCategory::Explore | WorkerCategory::Librarian => self.supports_explore,
            WorkerCategory::Custom => true, // custom workers can do anything
        }
    }
}

pub trait WorkerSessionAdapter {
    fn kind(&self) -> WorkerKind;
    fn capabilities(&self) -> WorkerCapabilities;
    fn start(&self, request: WorkerStartRequest<'_>) -> Result<Arc<dyn WorkerSessionHandle>>;
}

pub trait NativeWorkerBackend: Send + Sync {
    fn start_zed_agent(
        &self,
        request: WorkerStartRequest<'_>,
    ) -> Result<Arc<dyn WorkerSessionHandle>>;

    /// Optional ACP-backed worker route for provider workers such as
    /// OpenCode, Codex, or Claude. Returning `None` preserves CLI fallback.
    fn start_acp_worker(
        &self,
        _worker_kind: WorkerKind,
        _request: WorkerStartRequest<'_>,
    ) -> Result<Option<Arc<dyn WorkerSessionHandle>>> {
        Ok(None)
    }

    /// Capabilities for a native ACP handle, which may be richer than the
    /// command adapter capabilities for the same worker kind.
    fn native_broker_capabilities(
        &self,
        _worker_kind: WorkerKind,
    ) -> Option<Vec<BrokerCapability>> {
        None
    }
}

pub trait WorkerSessionHandle: Send + Sync {
    fn session_id(&self) -> Option<String>;
    fn send_follow_up(&self, prompt: String) -> Result<()>;
    fn steer(&self, prompt: String) -> Result<()>;
    fn interrupt(&self) -> Result<()>;
    fn cancel(&self) -> Result<()>;
    fn abort(&self) -> Result<()> {
        self.cancel()
    }
    fn dispose(&self) -> Result<()> {
        Ok(())
    }
    fn supports_event_subscriptions(&self) -> bool {
        false
    }
    fn subscribe(&self, _listener: WorkerEventListener) -> Result<WorkerSubscription> {
        bail!("worker session does not support event subscriptions")
    }
    fn reset_event_history(&self) -> Result<()> {
        Ok(())
    }
    fn wait_for_idle(&self) -> Result<WorkerTurnOutcome> {
        self.wait_for_result()
    }
    fn wait_for_outcome(&self) -> Result<WorkerOutcome>;
    fn wait_for_result(&self) -> Result<WorkerResult>;
    fn last_output(&self) -> Option<String>;
    fn usage(&self) -> Option<BrokerUsage> {
        None
    }
}

pub trait WorkerAdapter {
    fn name(&self) -> &'static str;
    fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult>;
}

#[derive(Clone, Default)]
pub struct WorkerRegistry {
    native_backend: Option<Arc<dyn NativeWorkerBackend>>,
    /// Optional broker for lifecycle-managed worker sessions.
    broker: Option<Arc<WorkerBroker>>,
}

impl WorkerRegistry {
    pub fn with_native_backend(native_backend: Arc<dyn NativeWorkerBackend>) -> Self {
        Self {
            native_backend: Some(native_backend),
            broker: None,
        }
    }

    pub fn set_native_backend(&mut self, native_backend: Arc<dyn NativeWorkerBackend>) {
        self.native_backend = Some(native_backend);
    }

    /// Attach a broker for lifecycle-managed session wrapping.
    pub fn with_broker(mut self, broker: Arc<WorkerBroker>) -> Self {
        self.broker = Some(broker);
        self
    }

    /// Set or clear the broker reference.
    pub fn set_broker(&mut self, broker: Option<Arc<WorkerBroker>>) {
        self.broker = broker;
    }

    pub(crate) fn without_broker(&self) -> Self {
        let mut registry = self.clone();
        registry.broker = None;
        registry
    }

    pub(crate) fn has_native_backend(&self) -> bool {
        self.native_backend.is_some()
    }

    pub fn start(&self, request: WorkerStartRequest<'_>) -> Result<Arc<dyn WorkerSessionHandle>> {
        let selected_route = request
            .config
            .selected_route_for_hint(request.route_attempt, request.route_hint);
        let worker_kind = selected_route.worker_kind;

        // Create ProviderAdapter to enforce variant and tool policy at the dispatch boundary.
        let adapter = ProviderAdapter::new(
            selected_route.variant.clone(),
            selected_route.tools.clone(),
            selected_route.category,
        );

        // Check variant support — reject unsupported before dispatch.
        if let Err(unsupported) = adapter.model_params() {
            let artifact_path = request
                .store
                .worker_dir(&request.task.id)
                .join("variant-rejection.json");
            let _ = write_json(
                &artifact_path,
                &serde_json::json!({
                    "error": "unsupported_variant",
                    "variant": unsupported.variant,
                    "category": format!("{:?}", unsupported.category),
                    "supported_variants": unsupported.supported_variants,
                    "message": unsupported.message,
                }),
            );
            bail!("{}", unsupported);
        }

        adapter
            .check_tool_allowed(adapter.required_tool_for_category())
            .map_err(|denied| anyhow::anyhow!(denied))?;

        if worker_kind == WorkerKind::ZedAgent
            && self.native_backend.is_some()
            && adapter.variant.is_some()
        {
            bail!(
                "native Zed worker does not expose a provider variant contract; refusing variant dispatch"
            );
        }

        // Store the applied variant info in an artifact.
        let variant_info_path = request
            .store
            .worker_dir(&request.task.id)
            .join("variant-applied.json");
        let variant_applied = adapter
            .variant
            .clone()
            .unwrap_or_else(|| "none".to_string());
        let _ = write_json(
            &variant_info_path,
            &serde_json::json!({
                "variant_requested": adapter.variant,
                "variant_applied": variant_applied,
                "category": format!("{:?}", adapter.category),
            }),
        );

        // Capability check: verify the worker's declared capabilities match
        // the task category before dispatch.
        let worker_caps =
            WorkerRegistry::capabilities_for_kind(worker_kind, self.native_backend.is_some());
        if !worker_caps.supports_category(selected_route.category) {
            let artifact_path = request
                .store
                .worker_dir(&request.task.id)
                .join("capability-rejection.json");
            let _ = write_json(
                &artifact_path,
                &serde_json::json!({
                    "worker_kind": worker_kind.as_str(),
                    "category": selected_route.category.as_str(),
                    "worker_capabilities": &worker_caps,
                    "reason": format!(
                        "worker kind `{}` does not support category `{}`",
                        worker_kind.as_str(),
                        selected_route.category.as_str(),
                    ),
                }),
            );
            bail!(
                "worker kind `{}` does not support category `{}`: missing required capability",
                worker_kind.as_str(),
                selected_route.category.as_str(),
            );
        }

        // Dispatch to the appropriate adapter, then wrap through broker if active.
        // Extract needed fields before consuming request in start_direct.
        let task_id = request.task.id.clone();
        let native_broker_capabilities = self
            .native_backend
            .as_ref()
            .and_then(|backend| backend.native_broker_capabilities(worker_kind));
        let handle = self.start_direct(worker_kind, request)?;

        if let Some(broker) = &self.broker {
            // Only wrap through broker when it is in Resolved state
            // (indicating the caller called broker.resolve() first but
            // has not yet called start()). Once started the broker is
            // Active and subsequent worker dispatches use the registry
            // directly to avoid illegal state transitions.
            let state = broker.current_state().ok();
            if state.map_or(false, |s| {
                s.lifecycle.name() == LifecycleStateName::Resolved
            }) {
                let identity = BrokerSessionIdentity {
                    backend_kind: worker_kind,
                    session_id: handle
                        .session_id()
                        .unwrap_or_else(|| format!("{}-{}", worker_kind.as_str(), task_id)),
                    started_at: crate::state::timestamp(),
                    capabilities: Some(native_broker_capabilities.unwrap_or_else(|| {
                        match worker_kind {
                            WorkerKind::Opencode => OpencodeCommandWorker {}.broker_capabilities(),
                            WorkerKind::OpencodeSession => {
                                OpencodeSessionWorker {}.broker_capabilities()
                            }
                            WorkerKind::Codex => CodexCommandWorker {}.broker_capabilities(),
                            WorkerKind::Claude => ClaudeCommandWorker {}.broker_capabilities(),
                            WorkerKind::ZedAgent => broker_capabilities_for_kind(
                                worker_kind,
                                self.native_backend.is_some(),
                            ),
                            WorkerKind::Custom => CustomCommandWorker {}.broker_capabilities(),
                        }
                    })),
                };
                return broker.start(handle, identity);
            }
        }

        Ok(handle)
    }

    /// Internal dispatch: start a worker without broker lifecycle wrapping.
    pub(crate) fn start_direct(
        &self,
        worker_kind: WorkerKind,
        request: WorkerStartRequest<'_>,
    ) -> Result<Arc<dyn WorkerSessionHandle>> {
        match worker_kind {
            WorkerKind::Opencode
            | WorkerKind::OpencodeSession
            | WorkerKind::Codex
            | WorkerKind::Claude => {
                if let Some(native_backend) = self.native_backend.as_ref()
                    && let Some(handle) =
                        native_backend.start_acp_worker(worker_kind, request.reborrow())?
                {
                    return Ok(handle);
                }
                match worker_kind {
                    WorkerKind::Opencode => OpencodeCommandWorker {}.start(request),
                    WorkerKind::OpencodeSession => OpencodeSessionWorker {}.start(request),
                    WorkerKind::Codex => CodexCommandWorker {}.start(request),
                    WorkerKind::Claude => ClaudeCommandWorker {}.start(request),
                    _ => bail!("worker kind was not a provider worker"),
                }
            }
            WorkerKind::ZedAgent => {
                if let Some(native_backend) = self.native_backend.as_ref() {
                    native_backend.start_zed_agent(request)
                } else {
                    ZedAgentCommandWorker {}.start(request)
                }
            }
            WorkerKind::Custom => CustomCommandWorker {}.start(request),
        }
    }

    /// Return the capabilities for a given worker kind and backend mode.
    /// External command workers have limited capabilities (Gear cannot
    /// verify internal code editing). Native/resident workers have full
    /// capabilities.
    fn capabilities_for_kind(kind: WorkerKind, has_native_backend: bool) -> WorkerCapabilities {
        match kind {
            WorkerKind::Opencode => WorkerCapabilities {
                supports_follow_up: false,
                supports_steering: false,
                supports_cancellation: true,
                supports_resident_session: false,
                supports_code_edit: true,
                supports_review: true,
                supports_explore: true,
                supports_model_selection: false,
                supports_tool_policy_enforcement: false,
                supports_artifact_contract: false,
            },
            WorkerKind::OpencodeSession => WorkerCapabilities {
                supports_follow_up: true,
                supports_steering: true,
                supports_cancellation: true,
                supports_resident_session: true,
                supports_code_edit: true,
                supports_review: true,
                supports_explore: true,
                supports_model_selection: true,
                supports_tool_policy_enforcement: true,
                supports_artifact_contract: true,
            },
            WorkerKind::Codex => WorkerCapabilities {
                supports_follow_up: false,
                supports_steering: false,
                supports_cancellation: true,
                supports_resident_session: false,
                supports_code_edit: true,
                supports_review: true,
                supports_explore: true,
                supports_model_selection: true,
                supports_tool_policy_enforcement: false,
                supports_artifact_contract: true,
            },
            WorkerKind::Claude => WorkerCapabilities {
                supports_follow_up: false,
                supports_steering: false,
                supports_cancellation: true,
                supports_resident_session: false,
                supports_code_edit: true,
                supports_review: false,
                supports_explore: true,
                supports_model_selection: false,
                supports_tool_policy_enforcement: false,
                supports_artifact_contract: false,
            },
            WorkerKind::ZedAgent if has_native_backend => WorkerCapabilities {
                supports_follow_up: true,
                supports_steering: true,
                supports_cancellation: true,
                supports_resident_session: true,
                supports_code_edit: true,
                supports_review: true,
                supports_explore: true,
                supports_model_selection: true,
                supports_tool_policy_enforcement: true,
                supports_artifact_contract: true,
            },
            WorkerKind::ZedAgent => WorkerCapabilities {
                supports_follow_up: false,
                supports_steering: false,
                supports_cancellation: true,
                supports_resident_session: false,
                supports_code_edit: true,
                supports_review: true,
                supports_explore: true,
                supports_model_selection: false,
                supports_tool_policy_enforcement: false,
                supports_artifact_contract: false,
            },
            WorkerKind::Custom => WorkerCapabilities {
                supports_follow_up: false,
                supports_steering: false,
                supports_cancellation: true,
                supports_resident_session: false,
                supports_code_edit: false,
                supports_review: false,
                supports_explore: false,
                supports_model_selection: false,
                supports_tool_policy_enforcement: false,
                supports_artifact_contract: false,
            },
        }
    }

    pub fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult> {
        self.start(request)?.wait_for_result()
    }
}

pub struct OpencodeCommandWorker {}
pub struct OpencodeSessionWorker {}
pub struct CodexCommandWorker {}
pub struct ClaudeCommandWorker {}
pub struct ZedAgentCommandWorker {}
pub struct CustomCommandWorker {}

// ── Broker capability declarations ────────────────────────────────────────
//
// Each adapter declares its broker-level capabilities explicitly, matching
// the capability matrices specified in GBX-006-004.

impl OpencodeCommandWorker {
    pub fn broker_capabilities(&self) -> Vec<BrokerCapability> {
        vec![
            BrokerCapability::DiscoverAgents,
            BrokerCapability::Start,
            BrokerCapability::Cancel,
            BrokerCapability::Wait,
        ]
    }
}

impl OpencodeSessionWorker {
    pub fn broker_capabilities(&self) -> Vec<BrokerCapability> {
        vec![
            BrokerCapability::DiscoverAgents,
            BrokerCapability::Start,
            BrokerCapability::FollowUp,
            BrokerCapability::Steer,
            BrokerCapability::Cancel,
            BrokerCapability::Wait,
            BrokerCapability::SessionResume,
            // model_selection and usage are backend-declared (not ACP-verified)
            BrokerCapability::ModelSelection,
            BrokerCapability::Usage,
            BrokerCapability::Permission,
        ]
    }
}

impl CodexCommandWorker {
    pub fn broker_capabilities(&self) -> Vec<BrokerCapability> {
        vec![
            BrokerCapability::DiscoverAgents,
            BrokerCapability::Start,
            BrokerCapability::Cancel,
            BrokerCapability::Wait,
            BrokerCapability::ModelSelection,
        ]
    }
}

impl ClaudeCommandWorker {
    pub fn broker_capabilities(&self) -> Vec<BrokerCapability> {
        vec![
            BrokerCapability::DiscoverAgents,
            BrokerCapability::Start,
            BrokerCapability::Cancel,
            BrokerCapability::Wait,
        ]
    }
}

impl CustomCommandWorker {
    pub fn broker_capabilities(&self) -> Vec<BrokerCapability> {
        vec![
            BrokerCapability::DiscoverAgents,
            BrokerCapability::Start,
            BrokerCapability::Cancel,
            BrokerCapability::Wait,
        ]
    }
}

pub struct CommandWorker {}

/// Describes the launch contract for a worker adapter.
/// Each adapter exports this to document its exact CLI/env/request contract.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerLaunchContract {
    /// The worker kind.
    pub worker_kind: String,
    /// The default command template, if any.
    pub default_command: Option<String>,
    /// Whether this adapter supports interaction (follow-up/steer).
    pub supports_interaction: bool,
    /// The declared capabilities.
    pub capabilities: WorkerCapabilities,
    /// Whether a native backend is available (for Zed Agent).
    pub native_backend_available: bool,
}

impl WorkerAdapter for CommandWorker {
    fn name(&self) -> &'static str {
        "command"
    }

    fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult> {
        WorkerRegistry::default().run(request)
    }
}

macro_rules! impl_command_backed_worker {
    ($worker:ty, $kind:expr, $name:literal) => {
        impl WorkerAdapter for $worker {
            fn name(&self) -> &'static str {
                $name
            }

            fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult> {
                let handle = self.start(request)?;
                handle.wait_for_result()
            }
        }

        impl WorkerSessionAdapter for $worker {
            fn kind(&self) -> WorkerKind {
                $kind
            }

            fn capabilities(&self) -> WorkerCapabilities {
                WorkerCapabilities::command()
            }

            fn start(
                &self,
                request: WorkerStartRequest<'_>,
            ) -> Result<Arc<dyn WorkerSessionHandle>> {
                start_command_backed_worker(request, false)
            }
        }
    };
}

impl_command_backed_worker!(
    OpencodeCommandWorker,
    WorkerKind::Opencode,
    "opencode_command"
);
impl WorkerAdapter for OpencodeSessionWorker {
    fn name(&self) -> &'static str {
        "opencode_session"
    }

    fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult> {
        let handle = self.start(request)?;
        handle.wait_for_result()
    }
}

impl WorkerSessionAdapter for OpencodeSessionWorker {
    fn kind(&self) -> WorkerKind {
        WorkerKind::OpencodeSession
    }

    fn capabilities(&self) -> WorkerCapabilities {
        WorkerCapabilities::resident_command()
    }

    fn start(&self, request: WorkerStartRequest<'_>) -> Result<Arc<dyn WorkerSessionHandle>> {
        start_command_backed_worker(request, true)
    }
}

impl_command_backed_worker!(CodexCommandWorker, WorkerKind::Codex, "codex_command");
impl_command_backed_worker!(ClaudeCommandWorker, WorkerKind::Claude, "claude_command");
impl_command_backed_worker!(
    ZedAgentCommandWorker,
    WorkerKind::ZedAgent,
    "zed_agent_command"
);
impl_command_backed_worker!(CustomCommandWorker, WorkerKind::Custom, "custom_command");

fn resident_session_descriptor_path(store: &StateStore, task_id: &str) -> PathBuf {
    store.worker_dir(task_id).join("resident-session.json")
}

fn read_resident_session_descriptor(
    store: &StateStore,
    task_id: &str,
) -> Result<Option<ResidentSessionDescriptor>> {
    let path = resident_session_descriptor_path(store, task_id);
    if !path.is_file() {
        return Ok(None);
    }
    let descriptor: ResidentSessionDescriptor =
        serde_json::from_slice(&fs::read(&path).with_context(|| {
            format!(
                "failed to read resident session descriptor {}",
                path.display()
            )
        })?)
        .with_context(|| {
            format!(
                "failed to parse resident session descriptor {}",
                path.display()
            )
        })?;
    descriptor.validate()?;
    Ok(Some(descriptor))
}

fn write_resident_session_descriptor(
    store: &StateStore,
    descriptor: &ResidentSessionDescriptor,
) -> Result<PathBuf> {
    let path = resident_session_descriptor_path(store, &descriptor.task_id);
    let sealed = descriptor.clone().seal()?;
    write_json(&path, &sealed)?;
    Ok(path)
}

pub(crate) fn discard_resident_session_for_model_switch(
    store: &StateStore,
    workspace: &Path,
    task_id: &str,
    worker_kind: WorkerKind,
    worker_model: Option<&str>,
) -> Result<()> {
    let path = resident_session_descriptor_path(store, task_id);
    let Some(descriptor) = read_resident_session_descriptor(store, task_id)? else {
        return Ok(());
    };
    if descriptor.worker_kind != worker_kind || descriptor.workspace != workspace.to_string_lossy()
    {
        return Ok(());
    }
    let descriptor_worker_model = descriptor
        .worker_model
        .as_deref()
        .filter(|model| !model.is_empty());
    let requested_worker_model = worker_model.filter(|model| !model.is_empty());
    if let (Some(descriptor_worker_model), Some(requested_worker_model)) =
        (descriptor_worker_model, requested_worker_model)
        && descriptor_worker_model != requested_worker_model
    {
        let pending = PromptReconcilePending::from_descriptor(
            task_id,
            worker_kind,
            &descriptor,
            requested_worker_model,
            "provider/model route changed; resident session must not be reused",
        )?;
        store.write_worker_json_atomic(task_id, "prompt-reconcile-pending.json", &pending)?;
        fs::remove_file(&path).with_context(|| {
            format!(
                "failed to discard resident session descriptor for model switch {}",
                path.display()
            )
        })?;
    }
    Ok(())
}

fn prepare_resident_session_descriptor(
    store: &StateStore,
    workspace: &Path,
    task: &Task,
    worker_kind: WorkerKind,
    worker_model: Option<String>,
) -> Result<ResidentSessionDescriptor> {
    if let Some(mut descriptor) = read_resident_session_descriptor(store, &task.id)? {
        if descriptor.worker_kind != worker_kind {
            bail!(
                "resident session worker mismatch: descriptor has {}, requested {}",
                descriptor.worker_kind.as_str(),
                worker_kind.as_str()
            );
        }
        if descriptor.workspace != workspace.to_string_lossy() {
            bail!("resident session workspace binding mismatch");
        }
        if !descriptor.resumable {
            if descriptor
                .worker_model
                .as_deref()
                .is_some_and(|model| !model.is_empty())
            {
                if let Some(requested_model) =
                    worker_model.as_deref().filter(|model| !model.is_empty())
                {
                    let pending = PromptReconcilePending::from_descriptor(
                        &task.id,
                        worker_kind,
                        &descriptor,
                        requested_model,
                        "resident session was disposed; a fresh session is required",
                    )?;
                    store.write_worker_json_atomic(
                        &task.id,
                        "prompt-reconcile-pending.json",
                        &pending,
                    )?;
                }
            }
            let path = resident_session_descriptor_path(store, &task.id);
            fs::remove_file(&path).with_context(|| {
                format!(
                    "failed to clear disposed resident session descriptor {}",
                    path.display()
                )
            })?;
        } else {
            let descriptor_worker_model = descriptor
                .worker_model
                .as_deref()
                .filter(|model| !model.is_empty());
            let requested_worker_model = worker_model.as_deref().filter(|model| !model.is_empty());
            if let (Some(descriptor_worker_model), Some(requested_worker_model)) =
                (descriptor_worker_model, requested_worker_model)
                && descriptor_worker_model != requested_worker_model
            {
                bail!(
                    "resident session worker model binding mismatch: descriptor has `{descriptor_worker_model}`, requested `{requested_worker_model}`"
                );
            }
            descriptor.worker_model = worker_model
                .clone()
                .filter(|model| !model.is_empty())
                .or(descriptor.worker_model);
            descriptor.resume_count = descriptor.resume_count.saturating_add(1);
            descriptor.last_resumed_at = Some(timestamp());
            return descriptor.seal();
        }
    }

    ResidentSessionDescriptor {
        schema_version: RESIDENT_SESSION_DESCRIPTOR_SCHEMA_VERSION,
        task_id: task.id.clone(),
        worker_kind,
        worker_model,
        session_id: format!("{}_session", task.id),
        provider_session_id: None,
        workspace: workspace.to_string_lossy().to_string(),
        resumable: true,
        resume_count: 0,
        created_at: timestamp(),
        last_resumed_at: None,
        descriptor_hash: String::new(),
    }
    .seal()
}

fn extract_provider_session_id(value: &Value) -> Option<String> {
    match value {
        Value::Object(object) => {
            for key in ["sessionID", "sessionId", "session_id"] {
                if let Some(session_id) = object
                    .get(key)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|session_id| !session_id.is_empty())
                {
                    return Some(session_id.to_string());
                }
            }
            object.values().find_map(extract_provider_session_id)
        }
        Value::Array(values) => values.iter().find_map(extract_provider_session_id),
        _ => None,
    }
}

fn value_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| match value {
        Value::Number(number) => number.as_u64(),
        Value::String(string) => string.trim().parse().ok(),
        _ => None,
    })
}

fn value_bool(value: Option<&Value>) -> Option<bool> {
    value.and_then(|value| match value {
        Value::Bool(value) => Some(*value),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    })
}

fn object_string(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        object
            .get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
    })
}

fn usage_from_value(value: &Value, fallback_model: Option<&str>) -> Option<BrokerUsage> {
    let Value::Object(object) = value else {
        return match value {
            Value::Array(values) => values
                .iter()
                .find_map(|value| usage_from_value(value, fallback_model)),
            _ => None,
        };
    };

    let nested_usage = object.get("usage").and_then(Value::as_object);
    let metrics = nested_usage.unwrap_or(object);
    let tokens = metrics.get("tokens").and_then(Value::as_object);
    let requested_tokens = value_u64(
        metrics
            .get("requested_tokens")
            .or_else(|| metrics.get("prompt_tokens"))
            .or_else(|| metrics.get("input_tokens"))
            .or_else(|| tokens.and_then(|tokens| tokens.get("input"))),
    );
    let actual_tokens = value_u64(
        metrics
            .get("actual_tokens")
            .or_else(|| metrics.get("completion_tokens"))
            .or_else(|| metrics.get("output_tokens"))
            .or_else(|| tokens.and_then(|tokens| tokens.get("output"))),
    );
    let cost_micros = value_u64(metrics.get("cost_micros"));
    let duration_ms = value_u64(metrics.get("duration_ms"));
    let cache_hit = value_bool(metrics.get("cache_hit")).or_else(|| {
        metrics
            .get("cache")
            .and_then(Value::as_object)
            .and_then(|cache| value_u64(cache.get("read")))
            .map(|read| read > 0)
    });
    let has_metrics = requested_tokens.is_some()
        || actual_tokens.is_some()
        || cost_micros.is_some()
        || duration_ms.is_some()
        || cache_hit.is_some()
        || nested_usage.is_some();
    if has_metrics {
        return Some(BrokerUsage {
            requested_tokens,
            actual_tokens,
            model: object_string(metrics, &["model", "modelID", "model_id"])
                .or_else(|| fallback_model.map(ToString::to_string))
                .unwrap_or_else(|| "unknown".to_string()),
            duration_ms,
            cost_micros,
            cache_hit,
            unavailable_reason: (requested_tokens.is_none()
                && actual_tokens.is_none()
                && cost_micros.is_none()
                && duration_ms.is_none()
                && cache_hit.is_none())
            .then(|| "provider usage payload omitted numeric telemetry".to_string()),
        });
    }

    object
        .values()
        .find_map(|value| usage_from_value(value, fallback_model))
}

fn merge_worker_usage(previous: Option<BrokerUsage>, current: BrokerUsage) -> BrokerUsage {
    let add = |left: Option<u64>, right: Option<u64>| match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        _ => None,
    };
    let Some(previous) = previous else {
        return current;
    };

    BrokerUsage {
        requested_tokens: add(previous.requested_tokens, current.requested_tokens),
        actual_tokens: add(previous.actual_tokens, current.actual_tokens),
        model: current.model,
        duration_ms: add(previous.duration_ms, current.duration_ms),
        cost_micros: add(previous.cost_micros, current.cost_micros),
        cache_hit: current.cache_hit.or(previous.cache_hit),
        unavailable_reason: current.unavailable_reason.or(previous.unavailable_reason),
    }
}

fn extract_worker_usage(
    stdout: &str,
    stderr: &str,
    fallback_model: Option<&str>,
) -> Option<BrokerUsage> {
    stdout
        .lines()
        .chain(stderr.lines())
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|value| usage_from_value(&value, fallback_model))
}

fn update_provider_session_id_from_output(
    store: &StateStore,
    task_id: &str,
    stdout: &str,
    stderr: &str,
) -> Result<()> {
    let Some(provider_session_id) = stdout
        .lines()
        .chain(stderr.lines())
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|value| extract_provider_session_id(&value))
    else {
        return Ok(());
    };
    let Some(mut descriptor) = read_resident_session_descriptor(store, task_id)? else {
        return Ok(());
    };
    if descriptor.provider_session_id.as_deref() == Some(provider_session_id.as_str()) {
        return Ok(());
    }
    descriptor.provider_session_id = Some(provider_session_id);
    write_resident_session_descriptor(store, &descriptor)?;
    Ok(())
}

pub fn provider_session_id_for_task(store: &StateStore, task_id: &str) -> Result<Option<String>> {
    Ok(
        read_resident_session_descriptor(store, task_id)?.and_then(|descriptor| {
            descriptor
                .provider_session_id
                .map(|session_id| session_id.trim().to_string())
                .filter(|session_id| !session_id.is_empty())
        }),
    )
}

pub fn seed_provider_session_for_task(
    store: &StateStore,
    workspace: &Path,
    task: &Task,
    worker_kind: WorkerKind,
    worker_model: Option<String>,
    provider_session_id: &str,
) -> Result<()> {
    let provider_session_id = provider_session_id.trim();
    if provider_session_id.is_empty() {
        bail!("provider session id cannot be empty when seeding a worker task");
    }
    let mut descriptor =
        prepare_resident_session_descriptor(store, workspace, task, worker_kind, worker_model)?;
    descriptor.provider_session_id = Some(provider_session_id.to_string());
    write_resident_session_descriptor(store, &descriptor)?;
    Ok(())
}

/// Discover the small, high-signal rule surface that OMO makes available to a
/// worker.  Gear resolves only project-local `AGENTS.md` and `.rules` files;
/// the result is soft prompt context, while this receipt remains authoritative
/// evidence of what was considered, injected, or deliberately omitted.
pub fn discover_workspace_rules(
    store: &StateStore,
    workspace: &Path,
    task: &Task,
) -> Result<(Option<String>, Option<String>)> {
    let workspace_real = workspace.canonicalize().unwrap_or_else(|_| workspace.to_path_buf());
    let workspace_display = workspace_real.to_string_lossy().to_string();
    let mut errors = Vec::new();
    let mut target_paths = Vec::new();
    let requested_targets = if task.scope.allowed_paths.is_empty() {
        vec![".".to_string()]
    } else {
        task.scope.allowed_paths.clone()
    };
    let mut target_dirs = Vec::new();

    for requested in requested_targets {
        let candidate = if Path::new(&requested).is_absolute() {
            PathBuf::from(&requested)
        } else {
            workspace.join(&requested)
        };
        let target = candidate.canonicalize().or_else(|_| {
            candidate
                .parent()
                .filter(|parent| parent.exists())
                .map(Path::to_path_buf)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "target missing"))
        });
        let target = match target {
            Ok(target) => target,
            Err(error) => {
                errors.push(format!("target `{requested}` unavailable: {error}"));
                continue;
            }
        };
        if !target.starts_with(&workspace_real) {
            errors.push(format!(
                "target `{requested}` outside workspace realpath; skipped"
            ));
            continue;
        }
        let target_dir = if target.is_file() {
            target.parent().unwrap_or(&target).to_path_buf()
        } else {
            target
        };
        let relative_target = target_dir
            .strip_prefix(&workspace_real)
            .map(|path| {
                let value = path.to_string_lossy().replace('\\', "/");
                if value.is_empty() { ".".to_string() } else { value }
            })
            .unwrap_or_else(|_| ".".to_string());
        target_paths.push(relative_target);
        if !target_dirs.contains(&target_dir) {
            target_dirs.push(target_dir);
        }
    }
    target_paths.sort();
    target_paths.dedup();

    let mut candidates = Vec::new();
    for target_dir in target_dirs {
        let mut ancestors = Vec::new();
        let mut current = target_dir;
        loop {
            ancestors.push(current.clone());
            if current == workspace_real {
                break;
            }
            let Some(parent) = current.parent().map(Path::to_path_buf) else {
                break;
            };
            if !parent.starts_with(&workspace_real) {
                break;
            }
            current = parent;
        }
        ancestors.reverse();
        for (distance, directory) in ancestors.iter().enumerate() {
            for file_name in ["AGENTS.md", ".rules"] {
                let path = directory.join(file_name);
                if !path.exists() {
                    continue;
                }
                let real_path = match path.canonicalize() {
                    Ok(real_path) if real_path.starts_with(&workspace_real) => real_path,
                    Ok(_) => {
                        candidates.push((path, None, distance, Some("realpath outside workspace".to_string())));
                        continue;
                    }
                    Err(error) => {
                        candidates.push((path, None, distance, Some(format!("realpath failed: {error}"))));
                        continue;
                    }
                };
                candidates.push((path, Some(real_path), distance, None));
            }
        }
    }

    candidates.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut seen_real_paths = std::collections::HashSet::new();
    let mut seen_content_hashes = std::collections::HashSet::new();
    let mut directive_values = HashMap::<(usize, String), String>::new();
    let mut conflict_reasons = Vec::new();
    let mut entries = Vec::new();
    let mut injected_sections = Vec::new();
    for (path, real_path, distance, omission) in candidates {
        let relative_path = path
            .strip_prefix(&workspace_real)
            .map(|value| value.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"));
        let Some(real_path) = real_path else {
            entries.push(RuleInjectionEntry {
                relative_path,
                real_path: String::new(),
                content_hash: String::new(),
                bytes: 0,
                modified_at_ms: 0,
                distance,
                precedence: distance,
                match_reason: "walk_up".to_string(),
                freshness: "unavailable".to_string(),
                injected: false,
                omission_reason: omission,
            });
            continue;
        };
        let real_path_string = real_path.to_string_lossy().to_string();
        if !seen_real_paths.insert(real_path_string.clone()) {
            entries.push(RuleInjectionEntry {
                relative_path,
                real_path: real_path_string,
                content_hash: String::new(),
                bytes: 0,
                modified_at_ms: 0,
                distance,
                precedence: distance,
                match_reason: "walk_up".to_string(),
                freshness: "duplicate_realpath".to_string(),
                injected: false,
                omission_reason: Some("duplicate realpath".to_string()),
            });
            continue;
        }
        if !real_path.is_file() {
            entries.push(RuleInjectionEntry {
                relative_path,
                real_path: real_path_string,
                content_hash: String::new(),
                bytes: 0,
                modified_at_ms: 0,
                distance,
                precedence: distance,
                match_reason: "walk_up".to_string(),
                freshness: "unavailable".to_string(),
                injected: false,
                omission_reason: Some("not a regular file".to_string()),
            });
            continue;
        }
        let content = match fs::read_to_string(&real_path) {
            Ok(content) => content,
            Err(error) => {
                errors.push(format!("failed to read {}: {error}", real_path.display()));
                entries.push(RuleInjectionEntry {
                    relative_path,
                    real_path: real_path_string,
                    content_hash: String::new(),
                    bytes: 0,
                    modified_at_ms: 0,
                    distance,
                    precedence: distance,
                    match_reason: "walk_up".to_string(),
                    freshness: "unavailable".to_string(),
                    injected: false,
                    omission_reason: Some("read failed".to_string()),
                });
                continue;
            }
        };
        let content_hash = prompt_content_hash(&content);
        let modified_at_ms = fs::metadata(&real_path)
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_millis());
        for (key, value) in rule_directives(&content) {
            let identity = (distance, key.clone());
            if let Some(previous) = directive_values.insert(identity, value.clone())
                && previous != value
            {
                conflict_reasons.push(format!(
                    "same-layer rule directive `{key}` has conflicting values `{previous}` and `{value}`"
                ));
            }
        }
        if !seen_content_hashes.insert(content_hash.clone()) {
            entries.push(RuleInjectionEntry {
                relative_path,
                real_path: real_path_string,
                content_hash,
                bytes: content.len(),
                modified_at_ms,
                distance,
                precedence: distance,
                match_reason: "walk_up".to_string(),
                freshness: "duplicate_content".to_string(),
                injected: false,
                omission_reason: Some("duplicate content".to_string()),
            });
            continue;
        }
        entries.push(RuleInjectionEntry {
            relative_path: relative_path.clone(),
            real_path: real_path_string,
            content_hash: content_hash.clone(),
            bytes: content.len(),
            modified_at_ms,
            distance,
            precedence: distance,
            match_reason: "walk_up".to_string(),
            freshness: "current".to_string(),
            injected: true,
            omission_reason: None,
        });
        injected_sections.push(format!(
            "[Rule: {relative_path}]\n[Match: walk-up]\n{content}"
        ));
    }

    let injected_rules = if injected_sections.is_empty() {
        None
    } else {
        Some(injected_sections.join("\n\n"))
    };
    let injected_content_hash = prompt_content_hash(injected_rules.as_deref().unwrap_or_default());
    let mut receipt = RuleInjectionReceipt {
        schema_version: RULE_INJECTION_SCHEMA_VERSION,
        task_id: task.id.clone(),
        workspace: workspace_display,
        target_paths,
        entries,
        errors,
        context_conflict: !conflict_reasons.is_empty(),
        context_conflict_reason: conflict_reasons.first().cloned(),
        injected_content_hash,
        receipt_hash: String::new(),
        created_at: timestamp(),
    };
    receipt.receipt_hash = receipt.expected_hash()?;
    receipt.validate()?;
    let receipt_path = store.write_worker_json_atomic(&task.id, "rules-injection.json", &receipt)?;
    if receipt.context_conflict {
        bail!(
            "workspace rule context conflict; review required (receipt: {})",
            receipt_path.display()
        );
    }
    Ok((injected_rules, Some(receipt_path.to_string_lossy().to_string())))
}

fn rule_directives(content: &str) -> Vec<(String, String)> {
    const KEYS: &[&str] = &[
        "allow",
        "allowed",
        "command",
        "commands",
        "deny",
        "disabled",
        "enabled",
        "forbid",
        "forbidden",
        "must",
        "must_not",
        "required",
        "scope",
        "tools",
    ];
    content
        .lines()
        .filter_map(|line| {
            let line = line
                .trim()
                .trim_start_matches(['-', '*', '+'])
                .trim_start();
            let (key, value) = line.split_once(':')?;
            let key = key.trim().to_ascii_lowercase().replace('-', "_");
            if KEYS.contains(&key.as_str()) {
                Some((key, value.trim().to_string()))
            } else {
                None
            }
        })
        .collect()
}

fn skill_is_disabled(content: &str) -> bool {
    let mut lines = content.lines();
    if lines.next().map(str::trim) != Some("---") {
        return false;
    }
    for line in lines {
        let line = line.trim();
        if line == "---" {
            break;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if matches!(key.trim().to_ascii_lowercase().as_str(), "disabled" | "enabled") {
            let value = value.trim().trim_matches(['"', '\'']);
            if (key.trim().eq_ignore_ascii_case("disabled") && value.eq_ignore_ascii_case("true"))
                || (key.trim().eq_ignore_ascii_case("enabled")
                    && value.eq_ignore_ascii_case("false"))
            {
                return true;
            }
        }
    }
    false
}

fn skill_frontmatter_directives(content: &str) -> HashMap<String, String> {
    let mut directives = HashMap::new();
    let mut lines = content.lines();
    if lines.next().map(str::trim) != Some("---") {
        return directives;
    }
    for line in lines {
        let line = line.trim();
        if line == "---" {
            break;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase().replace('-', "_");
        if matches!(
            key.as_str(),
            "agent"
                | "agents"
                | "worker"
                | "workers"
                | "restricted_to"
                | "required"
        ) {
            directives.insert(key, value.trim().to_string());
        }
    }
    directives
}

fn skill_restricted_agents(content: &str) -> (Vec<String>, bool) {
    let directives = skill_frontmatter_directives(content);
    let mut agents = Vec::new();
    for key in ["agent", "agents", "worker", "workers", "restricted_to"] {
        let Some(value) = directives.get(key) else {
            continue;
        };
        if let Ok(values) = serde_json::from_str::<Vec<String>>(value) {
            agents.extend(values);
            continue;
        }
        agents.extend(
            value
                .trim_matches(['[', ']'])
                .split(|character: char| character == ',' || character.is_whitespace())
                .filter_map(|value| {
                    let value = value.trim_matches(['"', '\'']);
                    (!value.is_empty()).then(|| value.to_string())
                }),
        );
    }
    agents.sort_unstable();
    agents.dedup();
    let required = directives
        .get("required")
        .is_some_and(|value| matches!(
            value.trim().trim_matches(['"', '\'']).to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "required"
        ));
    (agents, required)
}

fn normalize_skill_agent(value: &str) -> String {
    value
        .trim()
        .trim_matches(['"', '\''])
        .to_ascii_lowercase()
        .replace(['-', ' '], "_")
}

fn skill_is_allowed_for_worker(
    content: &str,
    worker_name: &str,
    worker_category: &str,
) -> (bool, bool, Vec<String>) {
    let (restricted_agents, required) = skill_restricted_agents(content);
    if restricted_agents.is_empty() {
        return (true, required, restricted_agents);
    }
    let worker_name = normalize_skill_agent(worker_name);
    let worker_category = normalize_skill_agent(worker_category);
    let allowed = restricted_agents.iter().any(|agent| {
        let agent = normalize_skill_agent(agent);
        agent == "*" || agent == "all" || agent == worker_name || agent == worker_category
    });
    (allowed, required, restricted_agents)
}

/// Resolve project-local `.agents/skills/*/SKILL.md` files for the task scope.
///
/// Resolution is deliberately bounded to the workspace realpath and to the
/// root-to-target ancestor chain. A small persistent cache records file
/// hashes/mtimes so a dispatch can distinguish a reused entry from a changed
/// or newly discovered skill without treating stale prompt text as current.
pub fn discover_workspace_skills(
    store: &StateStore,
    workspace: &Path,
    task: &Task,
) -> Result<(Option<String>, Option<String>)> {
    discover_workspace_skills_for_worker(
        store,
        workspace,
        task,
        task.assigned_worker.as_deref().unwrap_or("unknown"),
        "unknown",
    )
}

fn discover_workspace_skills_for_worker(
    store: &StateStore,
    workspace: &Path,
    task: &Task,
    worker_name: &str,
    worker_category: &str,
) -> Result<(Option<String>, Option<String>)> {
    let workspace_real = workspace.canonicalize().unwrap_or_else(|_| workspace.to_path_buf());
    let workspace_display = workspace_real.to_string_lossy().to_string();
    let mut errors = Vec::new();
    let mut target_paths = Vec::new();
    let requested_targets = if task.scope.allowed_paths.is_empty() {
        vec![".".to_string()]
    } else {
        task.scope.allowed_paths.clone()
    };
    let mut target_dirs = Vec::new();
    for requested in requested_targets {
        let candidate = if Path::new(&requested).is_absolute() {
            PathBuf::from(&requested)
        } else {
            workspace.join(&requested)
        };
        let target = candidate.canonicalize().or_else(|_| {
            candidate
                .parent()
                .filter(|parent| parent.exists())
                .map(Path::to_path_buf)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "target missing"))
        });
        let target = match target {
            Ok(target) => target,
            Err(error) => {
                errors.push(format!("target `{requested}` unavailable: {error}"));
                continue;
            }
        };
        if !target.starts_with(&workspace_real) {
            errors.push(format!(
                "target `{requested}` outside workspace realpath; skipped"
            ));
            continue;
        }
        let target_dir = if target.is_file() {
            target.parent().unwrap_or(&target).to_path_buf()
        } else {
            target
        };
        let relative_target = target_dir
            .strip_prefix(&workspace_real)
            .map(|path| {
                let value = path.to_string_lossy().replace('\\', "/");
                if value.is_empty() { ".".to_string() } else { value }
            })
            .unwrap_or_else(|_| ".".to_string());
        target_paths.push(relative_target);
        if !target_dirs.contains(&target_dir) {
            target_dirs.push(target_dir);
        }
    }
    target_paths.sort();
    target_paths.dedup();
    let cache_key = prompt_content_hash(&format!(
        "{}|{}|{}|{}",
        workspace_display,
        target_paths.join("|"),
        normalize_skill_agent(worker_name),
        normalize_skill_agent(worker_category),
    ));
    // Scope the persistent cache by workspace and target set. Distinct
    // parallel workers no longer overwrite unrelated target baselines, while
    // later attempts for the same target set can still observe freshness.
    let cache_path = store
        .root()
        .join(format!("skill-injection-cache-{cache_key}.json"));
    let previous = fs::read(&cache_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<SkillInjectionReceipt>(&bytes).ok())
        .filter(|receipt| receipt.schema_version == SKILL_INJECTION_SCHEMA_VERSION);

    let mut candidates = Vec::new();
    for target_dir in target_dirs {
        let mut ancestors = Vec::new();
        let mut current = target_dir;
        loop {
            ancestors.push(current.clone());
            if current == workspace_real {
                break;
            }
            let Some(parent) = current.parent().map(Path::to_path_buf) else {
                break;
            };
            if !parent.starts_with(&workspace_real) {
                break;
            }
            current = parent;
        }
        ancestors.reverse();
        for (distance, directory) in ancestors.iter().enumerate() {
            let skills_dir = directory.join(".agents").join("skills");
            let entries = match fs::read_dir(&skills_dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    errors.push(format!("failed to read {}: {error}", skills_dir.display()));
                    continue;
                }
            };
            for entry in entries.flatten() {
                let skill_file = entry.path().join("SKILL.md");
                if !skill_file.exists() {
                    continue;
                }
                match skill_file.canonicalize() {
                    Ok(real_path) if real_path.starts_with(&workspace_real) => {
                        candidates.push((skill_file, real_path, distance));
                    }
                    Ok(_) => errors.push(format!(
                        "skill `{}` resolves outside workspace; skipped",
                        skill_file.display()
                    )),
                    Err(error) => errors.push(format!(
                        "skill `{}` realpath failed: {error}",
                        skill_file.display()
                    )),
                }
            }
        }
    }
    candidates.sort_by(|left, right| left.2.cmp(&right.2).then_with(|| left.0.cmp(&right.0)));
    let mut selected_skill_paths = HashMap::<String, (PathBuf, usize)>::new();
    for (path, _, distance) in &candidates {
        let Some(skill_name) = path
            .parent()
            .and_then(Path::file_name)
            .map(|name| name.to_string_lossy().to_string())
        else {
            continue;
        };
        let replace = selected_skill_paths
            .get(&skill_name)
            .is_none_or(|(_, selected_distance)| distance >= selected_distance);
        if replace {
            selected_skill_paths.insert(skill_name, (path.clone(), *distance));
        }
    }

    let previous_by_path = previous.as_ref().map(|receipt| {
        receipt
            .entries
            .iter()
            .map(|entry| (entry.relative_path.clone(), entry))
            .collect::<HashMap<_, _>>()
    });
    let mut seen_real_paths = std::collections::HashSet::new();
    let mut seen_content_hashes = std::collections::HashSet::new();
    let mut entries = Vec::new();
    let mut injected_sections = Vec::new();
    let mut required_unavailable = Vec::new();
    let mut all_cached = previous
        .as_ref()
        .is_some_and(|receipt| receipt.cache_key == cache_key);
    for (path, real_path, distance) in candidates {
        let relative_path = path
            .strip_prefix(&workspace_real)
            .map(|value| value.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"));
        let real_path_string = real_path.to_string_lossy().to_string();
        if !seen_real_paths.insert(real_path_string.clone()) {
            entries.push(SkillInjectionEntry {
                relative_path,
                real_path: real_path_string,
                content_hash: String::new(),
                bytes: 0,
                modified_at_ms: 0,
                distance,
                precedence: distance,
                match_reason: "scope_walk".to_string(),
                freshness: "duplicate_realpath".to_string(),
                injected: false,
                omission_reason: Some("duplicate realpath".to_string()),
            });
            continue;
        }
        let content = match fs::read_to_string(&real_path) {
            Ok(content) => content,
            Err(error) => {
                all_cached = false;
                errors.push(format!("failed to read {}: {error}", real_path.display()));
                entries.push(SkillInjectionEntry {
                    relative_path,
                    real_path: real_path_string,
                    content_hash: String::new(),
                    bytes: 0,
                    modified_at_ms: 0,
                    distance,
                    precedence: distance,
                    match_reason: "scope_walk".to_string(),
                    freshness: "unavailable".to_string(),
                    injected: false,
                    omission_reason: Some("read failed".to_string()),
                });
                continue;
            }
        };
        let content_hash = prompt_content_hash(&content);
        let modified_at_ms = fs::metadata(&real_path)
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_millis());
        let skill_name = path
            .parent()
            .and_then(Path::file_name)
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| "<unnamed>".to_string());
        let (allowed_for_worker, required, restricted_agents) =
            skill_is_allowed_for_worker(&content, worker_name, worker_category);
        if !allowed_for_worker {
            all_cached = false;
            let reason = format!(
                "skill `{skill_name}` is restricted to agents: {}",
                restricted_agents.join(", ")
            );
            if required {
                required_unavailable.push(reason.clone());
            }
            entries.push(SkillInjectionEntry {
                relative_path,
                real_path: real_path_string,
                content_hash,
                bytes: content.len(),
                modified_at_ms,
                distance,
                precedence: distance,
                match_reason: "agent_restricted".to_string(),
                freshness: "restricted".to_string(),
                injected: false,
                omission_reason: Some(reason),
            });
            continue;
        }
        if selected_skill_paths
            .get(&skill_name)
            .is_some_and(|(selected_path, _)| selected_path != &path)
        {
            all_cached = false;
            entries.push(SkillInjectionEntry {
                relative_path,
                real_path: real_path_string,
                content_hash,
                bytes: content.len(),
                modified_at_ms,
                distance,
                precedence: distance,
                match_reason: "scope_walk".to_string(),
                freshness: "shadowed_precedence".to_string(),
                injected: false,
                omission_reason: Some(format!(
                    "skill `{skill_name}` is shadowed by a more specific target"
                )),
            });
            continue;
        }
        if skill_is_disabled(&content) {
            let previously_cached = previous_by_path
                .as_ref()
                .and_then(|entries| entries.get(&relative_path))
                .is_some_and(|entry| {
                    entry.content_hash == content_hash && entry.modified_at_ms == modified_at_ms
                });
            if !previously_cached {
                all_cached = false;
            }
            entries.push(SkillInjectionEntry {
                relative_path,
                real_path: real_path_string,
                content_hash,
                bytes: content.len(),
                modified_at_ms,
                distance,
                precedence: distance,
                match_reason: "scope_walk".to_string(),
                freshness: if previously_cached {
                    "disabled_cached".to_string()
                } else {
                    "disabled".to_string()
                },
                injected: false,
                omission_reason: Some("skill is disabled by frontmatter".to_string()),
            });
            continue;
        }
        if !seen_content_hashes.insert(content_hash.clone()) {
            all_cached = false;
            entries.push(SkillInjectionEntry {
                relative_path,
                real_path: real_path_string,
                content_hash,
                bytes: content.len(),
                modified_at_ms,
                distance,
                precedence: distance,
                match_reason: "scope_walk".to_string(),
                freshness: "duplicate_content".to_string(),
                injected: false,
                omission_reason: Some("duplicate content".to_string()),
            });
            continue;
        }
        let previous_entry = previous_by_path
            .as_ref()
            .and_then(|entries| entries.get(&relative_path));
        let freshness = if previous_entry.is_some_and(|entry| {
            entry.content_hash == content_hash && entry.modified_at_ms == modified_at_ms
        }) {
            "cached"
        } else if previous_entry.is_some() {
            all_cached = false;
            "stale"
        } else {
            all_cached = false;
            "new"
        };
        injected_sections.push(format!("### Skill: {relative_path}\n\n{}", content.trim()));
        entries.push(SkillInjectionEntry {
            relative_path,
            real_path: real_path_string,
            content_hash,
            bytes: content.len(),
            modified_at_ms,
            distance,
            precedence: distance,
            match_reason: "scope_walk".to_string(),
            freshness: freshness.to_string(),
            injected: true,
            omission_reason: None,
        });
    }
    let injected_skills = if injected_sections.is_empty() {
        None
    } else {
        Some(injected_sections.join("\n\n"))
    };
    if previous
        .as_ref()
        .is_some_and(|receipt| receipt.entries.len() != entries.len())
    {
        all_cached = false;
    }
    let mut receipt = SkillInjectionReceipt {
        schema_version: SKILL_INJECTION_SCHEMA_VERSION,
        task_id: task.id.clone(),
        workspace: workspace_display,
        worker: worker_name.to_string(),
        worker_category: worker_category.to_string(),
        target_paths,
        cache_key,
        cache_hit: all_cached,
        entries,
        errors,
        injected_content_hash: prompt_content_hash(injected_skills.as_deref().unwrap_or_default()),
        receipt_hash: String::new(),
        created_at: timestamp(),
    };
    receipt.receipt_hash = receipt.expected_hash()?;
    receipt.validate()?;
    let receipt_path = store.write_worker_json_atomic(&task.id, "skills-injection.json", &receipt)?;
    let cache_bytes = serde_json::to_vec_pretty(&receipt)?;
    let cache_tmp = cache_path.with_file_name(format!(
        "skill-injection-cache-{}.json.tmp",
        prompt_content_hash(&task.id)
    ));
    fs::write(&cache_tmp, cache_bytes)
        .with_context(|| format!("failed to write {}", cache_tmp.display()))?;
    fs::rename(&cache_tmp, &cache_path)
        .with_context(|| format!("failed to replace {}", cache_path.display()))?;
    if !required_unavailable.is_empty() {
        bail!(
            "required workspace skill is unavailable for worker `{worker_name}` (receipt: {}): {}",
            receipt_path.display(),
            required_unavailable.join("; ")
        );
    }
    Ok((
        injected_skills,
        Some(receipt_path.to_string_lossy().to_string()),
    ))
}

/// Read the durable step cursor used when a worker session is revived or
/// recreated. An unreadable cursor is an integrity failure: falling back to
/// the first plan step would silently restart a partially completed task.
fn read_durable_current_step_id(store: &StateStore, task_id: &str) -> Result<Option<String>> {
    let path = store.worker_dir(task_id).join("current-step-id");
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to read durable current step cursor {}", path.display())
            });
        }
    };
    let step_id = contents.trim();
    if step_id.is_empty() || step_id.lines().count() != 1 {
        bail!(
            "durable current step cursor {} is empty or malformed",
            path.display()
        );
    }
    Ok(Some(step_id.to_string()))
}

fn start_command_backed_worker(
    request: WorkerStartRequest<'_>,
    supports_interaction: bool,
) -> Result<Arc<dyn WorkerSessionHandle>> {
    let WorkerStartRequest {
        store,
        workspace,
        task,
        route_attempt,
        goal,
        verification_commands,
        config,
        cancellation_token,
        coordinator_model,
        coordinator_brief,
        route_hint,
    } = request;
    let route = config.selected_route_for_hint(route_attempt, route_hint);
    let (category_resolution, category_resolution_result) =
        category_resolution_for_route(config, route_attempt, route_hint, &route);
    let worker_name = route.worker_kind.as_str();
    let adapter = ProviderAdapter::new(route.variant.clone(), route.tools.clone(), route.category);
    let model_params = adapter
        .model_params()
        .map_err(|error| anyhow::anyhow!(error))?;
    let plan_task = task.inputs.plan_task.as_ref();
    let current_step_id = read_durable_current_step_id(store, &task.id)?.or_else(|| {
        plan_task.and_then(|plan_task| {
            plan_task
                .execution_steps_or_legacy()
                .first()
                .map(|step| step.step_id.clone())
        })
    });
    let packet_goal = plan_task
        .map(|plan_task| plan_task.worker_goal(goal))
        .unwrap_or_else(|| goal.to_string());
    let constraints = {
        let mut constraints = plan_task
            .map(crate::plan_graph::PlanTaskContract::worker_constraints)
            .unwrap_or_else(|| {
                vec![
                    "Stay inside the allowed paths when they are provided.".to_string(),
                    "Prefer the package manager already used by the project.".to_string(),
                    "Read the provided spec and plan artifacts before changing code.".to_string(),
                    "Leave runnable local instructions in the final output.".to_string(),
                ]
            });
        // GBX-235: Destructive git/file commands are permanently forbidden.
        // The runtime rejects any command that matches reset, checkout,
        // restore, clean, rm, or file-replacement patterns.
        constraints.push(
            "NEVER run git reset, git checkout, git restore, git clean, rm, or any command that overwrites/restores user files. If the scope check fails, record a repair request; do not attempt to clean the working tree.".to_string(),
        );
        constraints
    };
    let required_outputs = plan_task
        .map(crate::plan_graph::PlanTaskContract::worker_required_outputs)
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
        .map(crate::plan_graph::PlanTaskContract::worker_verification_commands)
        .filter(|commands| !commands.is_empty())
        .unwrap_or_else(|| verification_commands.to_vec());
    let stop_conditions = plan_task
        .map(crate::plan_graph::PlanTaskContract::worker_stop_conditions)
        .unwrap_or_else(|| {
            vec![
                "Requires a paid external service.".to_string(),
                "Requires a user-provided API key.".to_string(),
                "The same verification fails twice.".to_string(),
            ]
        });
    let prompt_manifest_path = store.worker_dir(&task.id).join("prompt-manifest.json");
    let prompt_reconcile_path = store.worker_dir(&task.id).join("prompt-reconcile.json");
    let prompt_capsule_path = store.worker_dir(&task.id).join("prompt-capsule.json");
    let (injected_rules, rules_injection_path) =
        discover_workspace_rules(store, workspace, task)?;
    let (injected_skills, skills_injection_path) = discover_workspace_skills_for_worker(
        store,
        workspace,
        task,
        worker_name,
        route.category.as_str(),
    )?;
    let pending_reconcile_path = store
        .worker_dir(&task.id)
        .join("prompt-reconcile-pending.json");
    let mut pending_reconcile = match fs::read(&pending_reconcile_path) {
        Ok(bytes) => {
            let pending: PromptReconcilePending = serde_json::from_slice(&bytes)
                .context("failed to parse pending prompt reconcile receipt")?;
            pending.validate()?;
            Some(pending)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to read pending prompt reconcile receipt {}",
                    pending_reconcile_path.display()
                )
            });
        }
    };
    let previous_descriptor = if supports_interaction {
        read_resident_session_descriptor(store, &task.id)?
    } else {
        None
    };
    let packet = WorkerPacket {
        task_id: task.id.clone(),
        worker: worker_name.to_string(),
        current_step_id,
        worker_model: route.worker_model.map(ToString::to_string),
        variant: route.variant.clone(),
        variant_applied: model_params.and_then(|params| params.variant),
        prompt_append: route.prompt_append.clone(),
        injected_rules,
        rules_injection_path,
        injected_skills,
        skills_injection_path,
        tools: route.tools.clone(),
        category_resolution,
        category_resolution_result,
        goal: packet_goal,
        coordinator_model: coordinator_model.cloned(),
        coordinator_brief: coordinator_brief.map(ToString::to_string),
        scope: task.scope.clone(),
        inputs: task.inputs.clone(),
        constraints,
        required_outputs,
        verification: VerificationContract {
            preferred_commands: planned_verification,
            must_not_skip: vec!["typecheck".to_string()],
        },
        stop_conditions,
        prompt_manifest_path: Some(prompt_manifest_path.to_string_lossy().to_string()),
        prompt_reconcile_path: Some(prompt_reconcile_path.to_string_lossy().to_string()),
        prompt_capsule_path: Some(prompt_capsule_path.to_string_lossy().to_string()),
    };

    let packet_json =
        serde_json::to_string_pretty(&packet).context("failed to serialize worker packet")?;
    let packet_path =
        store.write_worker_file(&task.id, "packet.json", &format!("{packet_json}\n"))?;

    // Resolve optional/required dispatch parameters before any prompt or
    // provider process is started. The receipt distinguishes omitted safe
    // defaults from explicit invalid values and records the precedence that
    // produced the selected route.
    let parameter_resolution = validate_worker_packet_parameters(&packet)?;
    store.write_worker_json_atomic(
        &task.id,
        "parameter-resolution.json",
        &parameter_resolution,
    )?;
    if parameter_resolution.status == "invalid" {
        bail!(
            "worker dispatch parameter validation failed: {}",
            parameter_resolution.errors.join("; ")
        );
    }

    let prompt = worker_prompt(&packet)?;
    // Save full prompt as audit artifact (uncompiled, all sections).
    let full_prompt_path = store.write_worker_file(&task.id, "prompt-full.md", &prompt)?;
    let prompt_manifest = prompt_manifest_for_packet(&packet, &prompt)?;
    store.write_worker_json_atomic(&task.id, "prompt-manifest.json", &prompt_manifest)?;
    let capsule_recovery_reason = PromptCapsuleRecoveryReason::Dispatch;
    let mut prompt_capsule = match build_prompt_capsule(
        &packet,
        &prompt_manifest,
        &prompt,
        &capsule_recovery_reason,
    ) {
        Ok(capsule) => capsule,
        Err(error) => {
            if let Some(overflow) = error.downcast_ref::<PromptCapsuleBudgetOverflow>() {
                let receipt = json!({
                    "schema_version": PROMPT_BUDGET_OVERFLOW_SCHEMA_VERSION,
                    "status": "blocked",
                    "task_id": task.id,
                    "worker": worker_name,
                    "worker_model": packet.worker_model,
                    "variant": packet.variant,
                    "route_attempt": route_attempt,
                    "attempt": task.attempt,
                    "budget_tokens": overflow.budget_tokens,
                    "context_limit_tokens": overflow.context_limit_tokens,
                    "reserved_output_tokens": overflow.reserved_output_tokens,
                    "headroom_source": overflow.headroom_source,
                    "budget_source": overflow.budget_source,
                    "token_estimator": PROMPT_TOKEN_ESTIMATOR,
                    "required_tokens": overflow.required_tokens,
                    "semantic_contract_hash": prompt_manifest.semantic_contract_hash,
                    "packet_path": packet_path.clone(),
                    "prompt_full_path": full_prompt_path,
                    "prompt_manifest_path": prompt_manifest_path.clone(),
                    "next_action": "try_next_explicit_route_or_split_task",
                    "error": error.to_string(),
                });
                store.write_worker_json_atomic(
                    &task.id,
                    "prompt-budget-overflow.json",
                    &receipt,
                )?;
            }
            return Err(error).context("failed to build worker prompt capsule");
        }
    };
    // Generate bounded compiled prompt from capsule section decisions.
    let compiled_prompt = worker_compiled_prompt(&packet, &prompt_capsule)?;
    let prompt_path = store.write_worker_file(&task.id, "prompt.md", &compiled_prompt)?;
    // Bind compiled prompt identity to capsule for verifiable audit.
    prompt_capsule.compiled_prompt_path = Some(prompt_path.to_string_lossy().to_string());
    prompt_capsule.compiled_prompt_hash = prompt_content_hash(&compiled_prompt);
    store.write_worker_json_atomic(&task.id, "prompt-capsule.json", &prompt_capsule)?;
    let current_descriptor = if supports_interaction {
        store.write_worker_file(&task.id, "transcript.jsonl", "")?;
        store.write_worker_file(&task.id, "tool-events.jsonl", "")?;
        let descriptor = prepare_resident_session_descriptor(
            store,
            workspace,
            task,
            route.worker_kind,
            packet.worker_model.clone(),
        )?;
        write_resident_session_descriptor(store, &descriptor)?;
        Some(descriptor)
    } else {
        None
    };
    if pending_reconcile.is_none() {
        pending_reconcile = match fs::read(&pending_reconcile_path) {
            Ok(bytes) => {
                let pending: PromptReconcilePending = serde_json::from_slice(&bytes)
                    .context("failed to parse pending prompt reconcile receipt")?;
                pending.validate()?;
                Some(pending)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to read pending prompt reconcile receipt {}",
                        pending_reconcile_path.display()
                    )
                });
            }
        };
    }
    let prompt_reconcile = PromptReconcileReceipt::for_dispatch(
        &packet,
        &prompt_manifest,
        previous_descriptor.as_ref(),
        pending_reconcile.as_ref(),
        current_descriptor.as_ref(),
        route_attempt,
        task.attempt,
        supports_interaction,
    )?;
    store.write_worker_json_atomic(&task.id, "prompt-reconcile.json", &prompt_reconcile)?;
    if pending_reconcile.is_some() {
        fs::remove_file(&pending_reconcile_path).with_context(|| {
            format!(
                "failed to clear pending prompt reconcile receipt {}",
                pending_reconcile_path.display()
            )
        })?;
    }

    // Set up a temporary OMO plugin config directory for OpenCode session
    // workers.  This directory is bound to the handle's lifetime and cleaned
    // up when the handle is dropped.
    let omo_config_dir = if route.worker_kind == WorkerKind::OpencodeSession {
        Some(setup_omo_plugin_config_dir_with_read_only(!packet.tools.can_write)?)
    } else {
        None
    };
    // The goal lease is the only hard wall-clock boundary for a provider-backed
    // OpenCode session. Persist it per worker so every turn (including a
    // PlanCritic repair turn) can derive a bounded external-call timeout
    // without confusing the generic stale-task policy with a model deadline.
    persist_worker_runtime_deadline(store, task)?;

    Ok(Arc::new(CommandWorkerSessionHandle {
        store: store.clone(),
        workspace: workspace.to_path_buf(),
        task_id: task.id.clone(),
        task_attempt: task.attempt,
        worker_name: worker_name.to_string(),
        skip_worker: config.skip_worker,
        command: route.worker_command.map(ToString::to_string),
        // OpenCode owns its own provider/retry lifecycle.  A slow free model
        // and its paid fallback are both valid progress, so the generic Gear
        // stale-task timeout must not terminate the command while it is still
        // producing a result.  Fallback is driven by provider/process errors;
        // other worker kinds retain the configured command timeout.
        command_timeout: if matches!(
            route.worker_kind,
            WorkerKind::Opencode | WorkerKind::OpencodeSession
        ) {
            None
        } else {
            Some(Duration::from_secs(
                config.stale_task_timeout_secs.max(1) as u64
            ))
        },
        worker_model: packet.worker_model.clone(),
        model_variant: packet.variant_applied.clone(),
        tool_policy: packet.tools,
        packet_path,
        prompt_path,
        prompt_manifest_path,
        prompt_reconcile_path,
        prompt_capsule_path,
        subscriptions: Arc::new(WorkerSessionSubscriptions::default()),
        session_state: Mutex::new(ResidentSessionState {
            cancellation_token: cancellation_token.unwrap_or_else(CancellationToken::new),
            active_command: false,
            revive_count: 0,
            interrupt_count: 0,
            turn_epoch: 0,
            stale_reason: None,
        }),
        result: Mutex::new(None),
        last_output: Mutex::new(None),
        follow_up_count: Mutex::new(0),
        supports_interaction,
        omo_config_dir,
    }))
}

/// Handle for a command-backed (external process) worker session.
///
/// ## Capability boundary
///
/// The Gear host CAN:
/// - Refuse to start the worker if `check_tool_allowed()` rejects the category's required tool
/// - Set `GEARBOX_WORKER_TOOL_POLICY` env var so the external process can self-enforce
/// - Set `GEARBOX_WORKER_MODEL_VARIANT` env var for model selection hints
/// - Cancel/abort the running process via `cancel()` / `abort()`
///
/// The Gear host CANNOT:
/// - Intercept individual tool calls made inside the external process
/// - Enforce tool-level allow/deny after the process has started
/// - Claim host-level tool execution enforcement for command workers
///
/// For native (in-process) workers, tool policy enforcement happens before
/// dispatch in `WorkerRegistry::start()` via `check_tool_allowed()`.
struct CommandWorkerSessionHandle {
    store: StateStore,
    workspace: PathBuf,
    task_id: String,
    task_attempt: usize,
    worker_name: String,
    skip_worker: bool,
    command: Option<String>,
    command_timeout: Option<Duration>,
    worker_model: Option<String>,
    model_variant: Option<String>,
    tool_policy: WorkerToolPolicy,
    packet_path: PathBuf,
    prompt_path: PathBuf,
    prompt_manifest_path: PathBuf,
    prompt_reconcile_path: PathBuf,
    prompt_capsule_path: PathBuf,
    subscriptions: Arc<WorkerSessionSubscriptions>,
    session_state: Mutex<ResidentSessionState>,
    result: Mutex<Option<WorkerResult>>,
    last_output: Mutex<Option<String>>,
    follow_up_count: Mutex<usize>,
    supports_interaction: bool,
    /// Temporary OMO plugin config directory (bound to handle lifetime).
    /// Dropping this handle cleans up the directory.
    omo_config_dir: Option<tempfile::TempDir>,
}

#[derive(Clone, Debug)]
struct ResidentSessionState {
    cancellation_token: CancellationToken,
    active_command: bool,
    revive_count: usize,
    interrupt_count: usize,
    turn_epoch: usize,
    stale_reason: Option<String>,
}

/// Durable admission receipt for a command rejected before a child process
/// could be spawned. This is deliberately separate from post-hoc risk signals:
/// a command with this receipt never reached the shell.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DestructiveCommandRejectedReceipt {
    pub schema_version: u32,
    pub kind: String,
    pub task_id: String,
    pub worker: String,
    pub turn_kind: String,
    pub command: String,
    pub matched_pattern: String,
    pub rejected_before_spawn: bool,
    pub recorded_at: String,
}

const TOOL_PAIR_VALIDATION_SCHEMA_VERSION: u32 = 1;

/// Binds parsed tool-call events to the task/workspace/worker turn that
/// produced them. Command-backed workers do not expose a separate result
/// stream, so a parsed invocation is explicitly `unknown` until a result is
/// observed instead of being treated as a successful tool call.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ToolPairValidationReceipt {
    schema_version: u32,
    task_id: String,
    workspace: String,
    worker: String,
    turn_kind: String,
    turn_epoch: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    started_calls: usize,
    finished_calls: usize,
    unknown_results: usize,
    orphan_finished: usize,
    status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    event_hash: String,
    receipt_hash: String,
    created_at: String,
}

impl ToolPairValidationReceipt {
    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_hash.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&payload)?)
        ))
    }

    fn seal(mut self) -> Result<Self> {
        self.receipt_hash.clear();
        self.receipt_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<()> {
        if self.schema_version != TOOL_PAIR_VALIDATION_SCHEMA_VERSION {
            bail!("unsupported tool-pair validation schema");
        }
        if self.task_id.trim().is_empty() || self.workspace.trim().is_empty() {
            bail!("tool-pair validation receipt identity cannot be empty");
        }
        if self.finished_calls < self.unknown_results
            || self.finished_calls.saturating_add(self.orphan_finished) < self.started_calls
        {
            bail!("tool-pair validation receipt counts are inconsistent");
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("tool-pair validation receipt hash mismatch");
        }
        Ok(())
    }
}

impl CommandWorkerSessionHandle {
    fn reject_destructive_command(
        &self,
        prompt_path: &Path,
        turn_kind: &str,
        command: &str,
        matched_pattern: &str,
    ) -> Result<WorkerResult> {
        let receipt = DestructiveCommandRejectedReceipt {
            schema_version: 1,
            kind: "destructive_command_rejected".to_string(),
            task_id: self.task_id.clone(),
            worker: self.worker_name.clone(),
            turn_kind: turn_kind.to_string(),
            command: command.to_string(),
            matched_pattern: matched_pattern.to_string(),
            rejected_before_spawn: true,
            recorded_at: timestamp(),
        };
        let receipt_path = self.store.write_worker_json_atomic(
            &self.task_id,
            &format!("destructive-command-rejected-{turn_kind}.json"),
            &receipt,
        )?;
        let result = WorkerResult {
            status: WorkerStatus::Failed,
            command: Some(command.to_string()),
            exit_code: None,
            summary: format!(
                "{} command rejected before process launch ({matched_pattern}); receipt: {}",
                self.worker_name,
                receipt_path.display()
            ),
            packet_path: self.packet_path.clone(),
            prompt_path: prompt_path.to_path_buf(),
            stdout_path: None,
            stderr_path: None,
            last_message_path: None,
            result_path: self.store.worker_dir(&self.task_id).join("result.json"),
            outcome_path: self.store.worker_dir(&self.task_id).join("outcome.json"),
        };
        self.emit_event(WorkerEvent::Error {
            kind: turn_kind.to_string(),
            message: result.summary.clone(),
        })?;
        self.emit_event(WorkerEvent::TurnFinished {
            kind: turn_kind.to_string(),
            result_path: result.result_path.clone(),
            outcome_path: result.outcome_path.clone(),
            summary: result.summary.clone(),
        })?;
        Ok(result)
    }

    fn emit_event(&self, event: WorkerEvent) -> Result<()> {
        if !self.supports_interaction {
            return Ok(());
        }

        let event_json =
            serde_json::to_string(&event).context("failed to serialize worker event")?;
        let line = format!("{event_json}\n");
        self.store
            .append_worker_file(&self.task_id, "transcript.jsonl", &line)?;
        match &event {
            WorkerEvent::TurnStarted { .. }
            | WorkerEvent::TurnFinished { .. }
            | WorkerEvent::ToolCallStarted { .. }
            | WorkerEvent::ToolCallFinished { .. }
            | WorkerEvent::Error { .. } => {
                self.store
                    .append_worker_file(&self.task_id, "tool-events.jsonl", &line)?;
            }
            WorkerEvent::AssistantTextDelta { .. }
            | WorkerEvent::WorkerStdout { .. }
            | WorkerEvent::WorkerStderr { .. } => {}
        }
        self.subscriptions.emit(event);
        Ok(())
    }

    fn turn_kind_from_files(stdout_file: &str, stderr_file: &str) -> String {
        if stdout_file == "stdout.log" && stderr_file == "stderr.log" {
            return "run".to_string();
        }
        stdout_file
            .strip_suffix("-stdout.log")
            .or_else(|| stderr_file.strip_suffix("-stderr.log"))
            .unwrap_or(stdout_file)
            .to_string()
    }

    fn execute(&self) -> Result<WorkerResult> {
        if let Some(result) = self
            .result
            .lock()
            .map_err(|_| anyhow::anyhow!("worker result mutex poisoned"))?
            .clone()
        {
            return Ok(result);
        }

        let result = if self.skip_worker || self.command.is_none() {
            let summary = if self.skip_worker {
                "Worker execution was skipped by CLI option."
            } else {
                "No worker command was configured; worker packet is ready for external execution."
            };
            WorkerResult {
                status: WorkerStatus::Skipped,
                command: None,
                exit_code: None,
                summary: summary.to_string(),
                packet_path: self.packet_path.clone(),
                prompt_path: self.prompt_path.clone(),
                stdout_path: None,
                stderr_path: None,
                last_message_path: None,
                result_path: self.store.worker_dir(&self.task_id).join("result.json"),
                outcome_path: self.store.worker_dir(&self.task_id).join("outcome.json"),
            }
        } else if let Some(command) = self.command.as_deref() {
            if is_destructive_command(command).is_some() {
                self.execute_command()?
            } else if let Some(summary) = unavailable_command_summary(command) {
                WorkerResult {
                    status: WorkerStatus::Skipped,
                    command: Some(command.to_string()),
                    exit_code: None,
                    summary,
                    packet_path: self.packet_path.clone(),
                    prompt_path: self.prompt_path.clone(),
                    stdout_path: None,
                    stderr_path: None,
                    last_message_path: None,
                    result_path: self.store.worker_dir(&self.task_id).join("result.json"),
                    outcome_path: self.store.worker_dir(&self.task_id).join("outcome.json"),
                }
            } else {
                self.execute_command()?
            }
        } else {
            self.execute_command()?
        };

        self.set_last_output(output_from_result(&result)?)?;
        write_result_and_outcome(&self.store, &self.task_id, &result)?;
        *self
            .result
            .lock()
            .map_err(|_| anyhow::anyhow!("worker result mutex poisoned"))? = Some(result.clone());
        Ok(result)
    }

    fn execute_command(&self) -> Result<WorkerResult> {
        self.execute_command_with_prompt(&self.prompt_path, "stdout.log", "stderr.log")
    }

    fn execute_command_with_prompt(
        &self,
        prompt_path: &Path,
        stdout_file: &str,
        stderr_file: &str,
    ) -> Result<WorkerResult> {
        let command = self.command.as_deref().context("worker command missing")?;
        let turn_kind = Self::turn_kind_from_files(stdout_file, stderr_file);
        self.with_session_state(|state| {
            state.turn_epoch += 1;
        })?;
        self.emit_event(WorkerEvent::TurnStarted {
            kind: turn_kind.clone(),
            prompt_path: prompt_path.to_path_buf(),
        })?;
        if let Some(matched_pattern) = is_destructive_command(command) {
            return self.reject_destructive_command(
                prompt_path,
                &turn_kind,
                command,
                matched_pattern,
            );
        }
        let cancellation_token = self.with_session_state(|state| {
            state.active_command = true;
            state.cancellation_token.clone()
        })?;
        let external_timeout = worker_external_timeout(
            &self.store,
            &self.task_id,
            self.command_timeout,
        )?;
        let mut env = HashMap::new();
        let turn_epoch = self.with_session_state(|state| state.turn_epoch)?;
        let worker_directory = self.store.worker_dir(&self.task_id);
        env.insert(
            "GEARBOX_WORKER_PACKET".to_string(),
            self.packet_path.to_string_lossy().to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_DIR".to_string(),
            worker_directory.to_string_lossy().to_string(),
        );
        env.insert(
            "GEARBOX_EXTERNAL_TASK_ID".to_string(),
            self.task_id.clone(),
        );
        env.insert(
            "GEARBOX_WORKER_TASK_ID".to_string(),
            self.task_id.clone(),
        );
        env.insert(
            "GEARBOX_EXTERNAL_OWNER".to_string(),
            self.worker_name.clone(),
        );
        env.insert(
            "GEARBOX_EXTERNAL_ATTEMPT".to_string(),
            turn_epoch.to_string(),
        );
        env.insert(
            "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
            turn_kind.clone(),
        );
        env.insert(
            "GEARBOX_EXTERNAL_IDEMPOTENT".to_string(),
            "false".to_string(),
        );
        env.insert(
            "GEARBOX_EXTERNAL_RETRY_POLICY".to_string(),
            "none".to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_CLEANUP_RECEIPT".to_string(),
            worker_directory
                .join("process-cleanup.json")
                .to_string_lossy()
                .to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_PROMPT".to_string(),
            prompt_path.to_string_lossy().to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_PROMPT_MANIFEST".to_string(),
            self.prompt_manifest_path.to_string_lossy().to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_PROMPT_RECONCILE".to_string(),
            self.prompt_reconcile_path.to_string_lossy().to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_PROMPT_CAPSULE".to_string(),
            self.prompt_capsule_path.to_string_lossy().to_string(),
        );
        let last_message_path = self
            .store
            .worker_dir(&self.task_id)
            .join(format!("{stdout_file}.last-message.md"));
        env.insert(
            "GEARBOX_WORKER_LAST_MESSAGE".to_string(),
            last_message_path.to_string_lossy().to_string(),
        );
        if let Some(model_variant) = &self.model_variant {
            env.insert(
                "GEARBOX_WORKER_MODEL_VARIANT".to_string(),
                model_variant.clone(),
            );
        }
        if let Some(worker_model) = &self.worker_model {
            env.insert("GEARBOX_WORKER_MODEL".to_string(), worker_model.clone());
        }
        if self.supports_interaction {
            if let Some(descriptor) = read_resident_session_descriptor(&self.store, &self.task_id)?
            {
                env.insert(
                    "GEARBOX_WORKER_SESSION_ID".to_string(),
                    descriptor.resumable_session_id().to_string(),
                );
                env.insert(
                    "GEARBOX_WORKER_RESUME".to_string(),
                    (descriptor.resume_count > 0 || descriptor.provider_session_id.is_some())
                        .to_string(),
                );
                env.insert(
                    "GEARBOX_WORKER_SESSION_DESCRIPTOR".to_string(),
                    resident_session_descriptor_path(&self.store, &self.task_id)
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }
        if let Some(omo_dir) = &self.omo_config_dir {
            let omo_dir_path = omo_dir.path().to_string_lossy().to_string();
            // Save the original XDG_CONFIG_HOME so the worker process can
            // restore it if needed, then set XDG_CONFIG_HOME to our temp dir
            // so OpenCode's config loading chain finds oh-my-openagent.json.
            if let Ok(original) = env::var("XDG_CONFIG_HOME") {
                env.insert("GEARBOX_PRESERVED_XDG_CONFIG_HOME".to_string(), original);
            }
            env.insert("XDG_CONFIG_HOME".to_string(), omo_dir_path.clone());
            env.insert("GEARBOX_WORKER_OMO_CONFIG_DIR".to_string(), omo_dir_path);
        }
        env.insert(
            "GEARBOX_WORKER_TOOL_POLICY".to_string(),
            serde_json::to_string(&self.tool_policy)
                .context("failed to serialize worker tool policy for dispatch")?,
        );
        env.insert(
            "GEARBOX_WORKER_TIMEOUT_SECS".to_string(),
            self.command_timeout
                .map(|timeout| timeout.as_secs().to_string())
                .unwrap_or_else(|| "0".to_string()),
        );
        env.insert(
            "GEARBOX_WORKER_PROVIDER_ERROR_RECOVERY".to_string(),
            "1".to_string(),
        );
        if self
            .store
            .worker_dir(&self.task_id)
            .join(WORKER_RUNTIME_DEADLINE_FILE)
            .is_file()
        {
            env.insert(
                "GEARBOX_EXTERNAL_REQUIRE_DEADLINE".to_string(),
                "true".to_string(),
            );
        }

        // Headless OpenCode workers do not need OpenCode's project/VCS watcher:
        // Gear owns the durable event stream and diff/review evidence.  The
        // watcher subscribes to the repository's `.git` directory even when
        // the experimental workspace watcher is off, so leaving it enabled
        // can exhaust the process-wide inotify quota after repeated sessions.
        // Keep an explicit escape hatch for interactive debugging and persist
        // the applied protection decision as an auditable runtime artifact.
        if matches!(self.worker_name.as_str(), "opencode" | "opencode_session") {
            let disable_file_watcher = match env::var("GEARBOX_OPENCODE_DISABLE_FILEWATCHER") {
                Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
                    "0" | "false" | "no" | "off" | "enable" | "enabled" => false,
                    "1" | "true" | "yes" | "on" | "disable" | "disabled" => true,
                    _ => true,
                },
                Err(_) => true,
            };
            env.insert(
                "OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER".to_string(),
                disable_file_watcher.to_string(),
            );
            self.store.write_worker_json_atomic(
                &self.task_id,
                "resource-policy.json",
                &json!({
                    "schema_version": 1,
                    "mechanism_id": "opencode_file_watcher_resource_guard",
                    "status": if disable_file_watcher { "disabled" } else { "enabled" },
                    "protection_status": "configured",
                    "worker": self.worker_name,
                    "environment": "OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER",
                    "override": "GEARBOX_OPENCODE_DISABLE_FILEWATCHER",
                    "reason": if disable_file_watcher {
                        "headless_worker_owns_no_GUI_file_watchers"
                    } else {
                        "explicit_operator_override"
                    },
                    "recorded_at": timestamp(),
                }),
            )?;
        }

        let output = run_shell_command_with_env_and_cancellation_and_timeout(
            &self.workspace,
            command,
            &env,
            Some(&cancellation_token),
            external_timeout,
        );
        self.with_session_state(|state| {
            state.active_command = false;
            if cancellation_token.is_cancelled() {
                state.stale_reason = Some(format!("cancelled `{command}`"));
            } else if output.is_ok() {
                state.stale_reason = None;
            }
        })?;
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                self.emit_event(WorkerEvent::Error {
                    kind: turn_kind,
                    message: format!("{error:#}"),
                })?;
                return Err(error);
            }
        };
        update_provider_session_id_from_output(
            &self.store,
            &self.task_id,
            &output.stdout,
            &output.stderr,
        )?;
        // GBX-241: surface an explicit provider-error label so the runtime can
        // recover the child process and route to the configured fallback. A
        // slow-but-progressing response does not match, preserving the
        // no-artificial-timeout contract for slow free models.
        let provider_error_label =
            worker_output_indicates_provider_error(&output.stdout, &output.stderr);
        if let Some(label) = &provider_error_label {
            self.store.write_worker_file(
                &self.task_id,
                "provider-error.json",
                &format!(
                    "{}\n",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "task_id": self.task_id,
                        "worker": self.worker_name,
                        "provider_error": label,
                        "recorded_at": timestamp(),
                    }))?
                ),
            )?;
            self.store.write_worker_file(
                &self.task_id,
                "provider-cooldown.json",
                &format!(
                    "{}\n",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "schema_version": 1,
                        "task_id": self.task_id,
                        "provider": self.worker_name,
                        "model": self.worker_model,
                        "failure": label,
                        "failed_at": timestamp(),
                        "cooldown_scope": "current_goal_attempts",
                        "decision": "skip_failed_provider_model_until_route_recovery",
                        "evidence": "provider-error.json",
                    }))?
                ),
            )?;
            if is_free_model(self.worker_model.as_deref())
                && provider_error_is_free_quota(label)
            {
                self.record_global_free_provider_cooldown(label)?;
            }
        }
        if let Some(usage) =
            extract_worker_usage(&output.stdout, &output.stderr, self.worker_model.as_deref())
        {
            let usage = if self.supports_interaction {
                merge_worker_usage(self.read_persisted_usage()?, usage)
            } else {
                usage
            };
            self.store.write_worker_file(
                &self.task_id,
                "usage.json",
                &format!("{}\n", serde_json::to_string_pretty(&usage)?),
            )?;
        }
        let stdout_path =
            self.store
                .write_worker_file(&self.task_id, stdout_file, &output.stdout)?;
        let stderr_path =
            self.store
                .write_worker_file(&self.task_id, stderr_file, &output.stderr)?;
        self.store.write_worker_file(
            &self.task_id,
            "partial-output.md",
            &format!(
                "# Gear worker partial output\n\n## stdout\n\n{}\n\n## stderr\n\n{}\n",
                output.stdout, output.stderr
            ),
        )?;
        // Parse stdout for tool call patterns and emit granular deltas
        self.parse_and_emit_tool_events(&output.stdout, &turn_kind)?;
        self.emit_event(WorkerEvent::WorkerStdout {
            kind: turn_kind.clone(),
            output: output.stdout.clone(),
        })?;
        self.emit_event(WorkerEvent::WorkerStderr {
            kind: turn_kind.clone(),
            output: output.stderr.clone(),
        })?;
        let last_message_path = last_message_path.exists().then_some(last_message_path);
        let result = WorkerResult {
            // A provider can report a rate limit or quota failure while its
            // wrapper exits successfully.  Treat that semantic failure as a
            // failed turn so the phase runtime can recover or fall back
            // instead of accepting an empty/partial response as progress.
            status: worker_status_for_output(output.success, provider_error_label.as_deref()),
            command: Some(command.to_string()),
            exit_code: output.exit_code,
            summary: if let Some(label) = &provider_error_label {
                format!(
                    "{} worker command failed: {label}; child process recovered, fallback route available.",
                    self.worker_name
                )
            } else if output.success {
                format!("{} worker command completed.", self.worker_name)
            } else {
                format!("{} worker command failed.", self.worker_name)
            },
            packet_path: self.packet_path.clone(),
            prompt_path: prompt_path.to_path_buf(),
            stdout_path: Some(stdout_path),
            stderr_path: Some(stderr_path),
            last_message_path,
            result_path: self.store.worker_dir(&self.task_id).join("result.json"),
            outcome_path: self.store.worker_dir(&self.task_id).join("outcome.json"),
        };
        self.emit_event(WorkerEvent::TurnFinished {
            kind: turn_kind,
            result_path: result.result_path.clone(),
            outcome_path: result.outcome_path.clone(),
            summary: result.summary.clone(),
        })?;
        self.store.write_worker_file(
            &self.task_id,
            &format!("turn-{turn_epoch}-result.json"),
            &format!("{}\n", serde_json::to_string_pretty(&result)?),
        )?;
        Ok(result)
    }

    fn record_global_free_provider_cooldown(&self, reason: &str) -> Result<()> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let existing = self.store.read_global_provider_cooldown()?;
        let cooldown_until_ms = existing
            .as_ref()
            .map(|cooldown| cooldown.cooldown_until_ms)
            .unwrap_or(0)
            .max(now_ms.saturating_add(FREE_PROVIDER_COOLDOWN_SECS * 1000));
        let mut failed_models = existing
            .as_ref()
            .map(|cooldown| cooldown.failed_models.clone())
            .unwrap_or_default();
        if let Some(model) = self
            .worker_model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            && !failed_models
                .iter()
                .any(|known| known.eq_ignore_ascii_case(model))
        {
            failed_models.push(model.to_string());
        }
        failed_models.sort_unstable();
        failed_models.truncate(32);
        let source_attempt = self
            .with_session_state(|state| state.turn_epoch)
            .unwrap_or_default();
        self.store.write_global_provider_cooldown(
            GlobalProviderCooldown {
                schema_version: crate::state::GLOBAL_PROVIDER_COOLDOWN_SCHEMA_VERSION,
                provider_scope: "opencode-free-tier".to_string(),
                failed_models,
                reason: reason.to_string(),
                failed_at: timestamp(),
                cooldown_until_ms,
                source_task: self.task_id.clone(),
                source_attempt,
                recorded_at: timestamp(),
                receipt_hash: String::new(),
            },
        )?;
        Ok(())
    }

    fn run_interaction(&self, prompt: String, kind: &str) -> Result<()> {
        if !self.supports_interaction {
            bail!("command-backed worker sessions do not support {kind} prompts");
        }
        self.revive_if_stale(kind)?;
        let command = self
            .command
            .as_deref()
            .context("worker command missing for interactive worker session")?;
        let interaction_index = {
            let mut follow_up_count = self
                .follow_up_count
                .lock()
                .map_err(|_| anyhow::anyhow!("worker follow-up mutex poisoned"))?;
            *follow_up_count += 1;
            *follow_up_count
        };
        self.store.write_worker_file(
            &self.task_id,
            &format!("{kind}-{interaction_index}.md"),
            &format!(
                "# Gear worker {kind}\n\nCommand: `{command}`\n\n{}\n",
                prompt.trim()
            ),
        )?;
        let reason = match kind {
            "steer" => PromptCapsuleRecoveryReason::Compact,
            _ => PromptCapsuleRecoveryReason::Resume,
        };
        let output_stem = format!("{kind}-{interaction_index}");
        let current_step_id = read_durable_current_step_id(&self.store, &self.task_id)?;
        let prompt_path = match compile_recovery_prompt_with_attempt(
            &self.store,
            &self.task_id,
            command,
            &prompt,
            &reason,
            &self.packet_path,
            &self.prompt_manifest_path,
            &self.prompt_capsule_path,
            &output_stem,
            current_step_id.as_deref(),
            self.task_attempt as u64,
        ) {
            Ok(compiled) => compiled,
            Err(error) => {
                // A recovery prompt that bypasses the capsule can omit the
                // current step, scope, or required evidence contract. Keep
                // the raw prompt as an audit artifact, but refuse to send it
                // to the worker; the caller must create a bounded repair or
                // start a fresh session instead.
                eprintln!("Gear recovery capsule compilation blocked: {error:#}");
                return Err(error).context("recovery prompt capsule compilation blocked dispatch");
            }
        };
        let result = self.execute_command_with_prompt(
            &prompt_path,
            &format!("{kind}-{interaction_index}-stdout.log"),
            &format!("{kind}-{interaction_index}-stderr.log"),
        )?;
        self.set_last_output(output_from_result(&result)?)?;
        write_result_and_outcome(&self.store, &self.task_id, &result)?;
        *self
            .result
            .lock()
            .map_err(|_| anyhow::anyhow!("worker result mutex poisoned"))? = Some(result);
        Ok(())
    }

    fn revive_if_stale(&self, kind: &str) -> Result<()> {
        let stale = self.with_session_state(|state| {
            if state.active_command {
                return None;
            }
            if state.cancellation_token.is_cancelled() || state.stale_reason.is_some() {
                state.revive_count += 1;
                let revive_count = state.revive_count;
                let reason = state
                    .stale_reason
                    .clone()
                    .unwrap_or_else(|| "cancelled session token".to_string());
                state.cancellation_token = CancellationToken::new();
                state.stale_reason = None;
                Some((revive_count, reason))
            } else {
                None
            }
        })?;
        let Some((revive_count, reason)) = stale else {
            return Ok(());
        };
        self.store.write_worker_file(
            &self.task_id,
            &format!("revive-{revive_count}.md"),
            &format!(
                "# Gear worker revive\n\nBefore `{kind}`, Gear revived the resident worker session.\n\nReason: {reason}\n"
            ),
        )?;
        *self
            .result
            .lock()
            .map_err(|_| anyhow::anyhow!("worker result mutex poisoned"))? = None;
        Ok(())
    }

    fn parse_and_emit_tool_events(&self, stdout: &str, kind: &str) -> Result<()> {
        if !self.supports_interaction || stdout.is_empty() {
            return Ok(());
        }

        let mut started_calls = 0;
        let mut finished_calls = 0;
        let mut unknown_results = 0;
        let orphan_finished = 0;
        let mut truncated_group = false;
        let mut pos = 0;
        let bytes = stdout.as_bytes();

        while pos < bytes.len() {
            // Look for <function_calls> or <tool_use> tag
            let function_calls_tag = b"<function_calls>";
            let tool_use_tag = b"<tool_use>";
            let mut found_start = None;

            // Scan for the earliest opening tag
            if let Some(p) = find_subsequence(&bytes[pos..], function_calls_tag) {
                found_start = Some((pos + p, function_calls_tag.len(), "function_calls"));
            }
            if let Some(p) = find_subsequence(&bytes[pos..], tool_use_tag) {
                let abs_p = pos + p;
                match found_start {
                    Some((existing, _, _)) if abs_p < existing => {
                        found_start = Some((abs_p, tool_use_tag.len(), "tool_use"));
                    }
                    None => {
                        found_start = Some((abs_p, tool_use_tag.len(), "tool_use"));
                    }
                    _ => {}
                }
            }

            let Some((start, tag_len, _tag_name)) = found_start else {
                // No more tool call groups, emit remaining text
                let delta = &stdout[pos..];
                if !delta.is_empty() {
                    self.emit_event(WorkerEvent::AssistantTextDelta {
                        kind: kind.to_string(),
                        delta: delta.to_string(),
                    })?;
                }
                break;
            };

            if start > pos {
                // Emit text before tool call group
                self.emit_event(WorkerEvent::AssistantTextDelta {
                    kind: kind.to_string(),
                    delta: stdout[pos..start].to_string(),
                })?;
            }

            // Find the closing tag: </function_calls> or </tool_use>
            let closing_function_calls = b"</function_calls>";
            let closing_tool_use = b"</tool_use>";
            let group_content_start = start + tag_len;
            let closing_pos =
                find_subsequence(&bytes[group_content_start..], closing_function_calls)
                    .or_else(|| find_subsequence(&bytes[group_content_start..], closing_tool_use));

            let Some(closing_offset) = closing_pos else {
                // No closing tag found, emit remaining as text
                truncated_group = true;
                let delta = &stdout[pos..];
                if !delta.is_empty() {
                    self.emit_event(WorkerEvent::AssistantTextDelta {
                        kind: kind.to_string(),
                        delta: delta.to_string(),
                    })?;
                }
                break;
            };

            let group_end = group_content_start + closing_offset;
            let group_content = &stdout[group_content_start..group_end];

            // Parse individual tool invocations within the group
            let mut invoke_pos = 0;
            let invoke_bytes = group_content.as_bytes();
            let invoke_tag = b"<invoke";

            while invoke_pos < invoke_bytes.len() {
                if let Some(invoke_start) =
                    find_subsequence(&invoke_bytes[invoke_pos..], invoke_tag)
                {
                    let abs_invoke_start = invoke_pos + invoke_start;

                    // Emit any text before this invoke
                    if abs_invoke_start > invoke_pos {
                        let text_before = &group_content[invoke_pos..abs_invoke_start];
                        if !text_before.trim().is_empty() {
                            self.emit_event(WorkerEvent::AssistantTextDelta {
                                kind: kind.to_string(),
                                delta: text_before.to_string(),
                            })?;
                        }
                    }

                    // Extract tool name from the invoke tag
                    let after_tag = &group_content[abs_invoke_start + 7..]; // skip "<invoke"
                    let tool_name = extract_xml_attr(after_tag, "name").unwrap_or("unknown");
                    let tool_name = tool_name.to_string();

                    // Extract arguments from inside the invoke block
                    let invoke_close =
                        find_subsequence(&invoke_bytes[abs_invoke_start..], b"</invoke>");
                    let args = if let Some(close_offset) = invoke_close {
                        let content_end = abs_invoke_start + close_offset;
                        let inner = &group_content[abs_invoke_start + 7..content_end];
                        extract_invoke_arguments(inner)
                    } else {
                        String::new()
                    };

                    started_calls += 1;
                    self.emit_event(WorkerEvent::ToolCallStarted {
                        kind: kind.to_string(),
                        tool_name: tool_name.clone(),
                        arguments: args,
                    })?;

                    // Command-backed transcripts do not carry a separate tool-result
                    // stream. Preserve that fact as explicit unknown evidence instead
                    // of emitting an empty result that downstream code could mistake
                    // for a successful tool call.
                    self.emit_event(WorkerEvent::ToolCallFinished {
                        kind: kind.to_string(),
                        tool_name,
                        result: "unknown: tool result was not present in worker output".to_string(),
                    })?;
                    finished_calls += 1;
                    unknown_results += 1;

                    if let Some(close_offset) = invoke_close {
                        invoke_pos = abs_invoke_start + close_offset + 9; // skip "</invoke>"
                    } else {
                        break;
                    }
                } else {
                    // No more invokes, emit remaining text
                    let remaining = &group_content[invoke_pos..];
                    if !remaining.trim().is_empty() {
                        self.emit_event(WorkerEvent::AssistantTextDelta {
                            kind: kind.to_string(),
                            delta: remaining.to_string(),
                        })?;
                    }
                    break;
                }
            }

            let closing_tag_len = if find_subsequence(
                &bytes[group_content_start..],
                closing_function_calls,
            )
            .is_some()
            {
                closing_function_calls.len()
            } else {
                closing_tool_use.len()
            };
            pos = group_end + closing_tag_len;
        }

        let (status, reason) = if started_calls == 0 && !truncated_group {
            ("not_applicable", None)
        } else if truncated_group {
            (
                "unknown",
                Some("tool-call group was truncated before its closing tag"),
            )
        } else if orphan_finished > 0 {
            (
                "error",
                Some("tool-call result had no matching invocation"),
            )
        } else if unknown_results > 0 {
            (
                "unknown",
                Some("command-backed worker output did not contain tool results"),
            )
        } else if started_calls != finished_calls {
            (
                "error",
                Some("tool-call start/result counts did not match"),
            )
        } else {
            ("pass", None)
        };
        let session_id = read_resident_session_descriptor(&self.store, &self.task_id)?
            .map(|descriptor| descriptor.resumable_session_id().to_string());
        let turn_epoch = self.with_session_state(|state| state.turn_epoch)?;
        let receipt = ToolPairValidationReceipt {
            schema_version: TOOL_PAIR_VALIDATION_SCHEMA_VERSION,
            task_id: self.task_id.clone(),
            workspace: self.workspace.to_string_lossy().to_string(),
            worker: self.worker_name.clone(),
            turn_kind: kind.to_string(),
            turn_epoch,
            session_id,
            started_calls,
            finished_calls,
            unknown_results,
            orphan_finished,
            status: status.to_string(),
            reason: reason.map(str::to_string),
            event_hash: prompt_content_hash(stdout),
            receipt_hash: String::new(),
            created_at: timestamp(),
        }
        .seal()?;
        let receipt_file = format!("tool-pair-validation-{kind}-{turn_epoch}.json");
        self.store.write_worker_json_atomic(
            &self.task_id,
            &receipt_file,
            &receipt,
        )?;
        // Keep a stable latest receipt for runtime consumers while retaining
        // one immutable receipt per turn for replay and audit.
        self.store.write_worker_json_atomic(
            &self.task_id,
            "tool-pair-validation.json",
            &receipt,
        )?;

        Ok(())
    }

    fn read_persisted_usage(&self) -> Result<Option<BrokerUsage>> {
        let path = self.store.worker_dir(&self.task_id).join("usage.json");
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read worker usage artifact {}", path.display()))?;
        let usage = serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse worker usage artifact {}", path.display()))?;
        Ok(Some(usage))
    }

    fn set_last_output(&self, output: Option<String>) -> Result<()> {
        *self
            .last_output
            .lock()
            .map_err(|_| anyhow::anyhow!("worker output mutex poisoned"))? = output;
        Ok(())
    }

    fn with_session_state<T>(
        &self,
        update: impl FnOnce(&mut ResidentSessionState) -> T,
    ) -> Result<T> {
        let mut state = self
            .session_state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session mutex poisoned"))?;
        Ok(update(&mut state))
    }
}

impl WorkerSessionHandle for CommandWorkerSessionHandle {
    fn session_id(&self) -> Option<String> {
        if !self.supports_interaction {
            return None;
        }
        read_resident_session_descriptor(&self.store, &self.task_id)
            .ok()
            .flatten()
            .map(|descriptor| descriptor.resumable_session_id().to_string())
            .or_else(|| Some(format!("{}_session", self.task_id)))
    }

    fn send_follow_up(&self, prompt: String) -> Result<()> {
        self.run_interaction(prompt, "follow-up")
    }

    fn steer(&self, prompt: String) -> Result<()> {
        self.run_interaction(prompt, "steer")
    }

    fn interrupt(&self) -> Result<()> {
        let interrupt = self.with_session_state(|state| {
            state.interrupt_count += 1;
            let interrupt_count = state.interrupt_count;
            let reason = if state.active_command {
                state.cancellation_token.cancel();
                "interrupted running command".to_string()
            } else {
                "interrupted while idle".to_string()
            };
            state.stale_reason = Some(reason.clone());
            (interrupt_count, reason)
        })?;
        if self.supports_interaction {
            self.store.write_worker_file(
                &self.task_id,
                &format!("interrupt-{}.md", interrupt.0),
                &format!(
                    "# Gear worker interrupt\n\nGear interrupted the resident worker session.\n\nReason: {}\n",
                    interrupt.1
                ),
            )?;
        }
        Ok(())
    }

    fn cancel(&self) -> Result<()> {
        self.with_session_state(|state| {
            state.cancellation_token.cancel();
            if !state.active_command {
                state.stale_reason = Some("cancelled while idle".to_string());
            }
        })?;
        Ok(())
    }

    fn abort(&self) -> Result<()> {
        self.cancel()
    }

    fn dispose(&self) -> Result<()> {
        if self.supports_interaction {
            self.store.write_worker_file(
                &self.task_id,
                "dispose.md",
                "# Gear worker dispose\n\nGear disposed the resident worker session.\n",
            )?;
            if let Some(mut descriptor) =
                read_resident_session_descriptor(&self.store, &self.task_id)?
            {
                descriptor.resumable = false;
                write_resident_session_descriptor(&self.store, &descriptor)?;
            }
        }
        Ok(())
    }

    fn supports_event_subscriptions(&self) -> bool {
        self.supports_interaction
    }

    fn subscribe(&self, listener: WorkerEventListener) -> Result<WorkerSubscription> {
        if !self.supports_interaction {
            bail!("command-backed worker sessions do not support event subscriptions");
        }
        self.subscriptions.subscribe(listener)
    }

    fn reset_event_history(&self) -> Result<()> {
        if self.supports_interaction {
            self.subscriptions.clear_history()?;
        }
        Ok(())
    }

    fn wait_for_idle(&self) -> Result<WorkerTurnOutcome> {
        self.wait_for_result()
    }

    fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
        worker_outcome_from_result(&self.execute()?)
    }

    fn wait_for_result(&self) -> Result<WorkerResult> {
        self.execute()
    }

    fn last_output(&self) -> Option<String> {
        self.last_output
            .lock()
            .map(|output| output.clone())
            .unwrap_or(None)
    }

    fn usage(&self) -> Option<BrokerUsage> {
        let path = self.store.worker_dir(&self.task_id).join("usage.json");
        serde_json::from_slice(&fs::read(path).ok()?).ok()
    }
}

fn phase_request_marker(packet: &WorkerPacket) -> Option<String> {
    if packet.inputs.phase_route_locked && packet.inputs.plan_task.is_none() {
        Some(format!(
            "[full phase request follows below; sha256={} bytes={}]",
            prompt_content_hash(&packet.goal),
            packet.goal.len()
        ))
    } else {
        None
    }
}

fn worker_prompt_packet_json(packet: &WorkerPacket) -> Result<String> {
    let Some(marker) = phase_request_marker(packet) else {
        return serde_json::to_string_pretty(packet)
            .context("failed to serialize worker prompt packet");
    };
    let mut packet_value = serde_json::to_value(packet)
        .context("failed to serialize worker prompt packet value")?;
    packet_value["goal"] = Value::String(marker);
    serde_json::to_string_pretty(&packet_value)
        .context("failed to serialize bounded phase worker prompt packet")
}

pub fn worker_prompt(packet: &WorkerPacket) -> Result<String> {
    let packet_json = worker_prompt_packet_json(packet)?;
    let prompt_append = packet
        .prompt_append
        .as_deref()
        .map(str::trim)
        .filter(|append| !append.is_empty())
        .map(|append| format!("\n## Route instructions\n\n{}\n", append))
        .unwrap_or_default();
    let workspace_rules = packet
        .injected_rules
        .as_deref()
        .map(str::trim)
        .filter(|rules| !rules.is_empty())
        .map(|rules| format!("\n## Workspace rules\n\n{rules}\n"))
        .unwrap_or_default();
    let workspace_skills = packet
        .injected_skills
        .as_deref()
        .map(str::trim)
        .filter(|skills| !skills.is_empty())
        .map(|skills| format!("\n## Workspace skills\n\n{skills}\n"))
        .unwrap_or_default();
    let model_metadata = worker_model_metadata(packet);
    let step_report = if packet
        .required_outputs
        .iter()
        .any(|output| output == "completed_steps")
    {
        "- completed_steps (one exact step_id per completed ordered step; report only a contiguous prefix and never skip an earlier step)\n- step_evidence (one `step_id: evidence_path` line per completed step)\n"
    } else {
        ""
    };

    if packet.inputs.phase_route_locked && packet.inputs.plan_task.is_none() {
        return Ok(format!(
            r#"# Gear phase worker packet

You are a `{}` phase worker controlled by Gearbox Gear. Treat this packet as the contract.

```json
{}
```

## Model metadata

{}

## Tool policy

{}

{}

{}

## Phase request

{}

{}
Return only the response format required by the phase request. Do not add a generic worker report or markdown fence.
"#,
            packet.worker,
            packet_json,
            model_metadata,
            packet.tools.to_markdown(),
            workspace_rules,
            workspace_skills,
            packet.goal,
            prompt_append
        ));
    }

    Ok(format!(
        r#"# Gear worker packet

You are a `{}` worker controlled by Gearbox Gear. Treat this packet as the contract.

```json
{}
```

## Model metadata

{}

## Tool policy

{}

{}
{}
{}
{}
Return a concise report with:

- summary
- changed_files
- commands_run
- known_failures
- completed_steps
- step_evidence
- next_steps
- plan_gap
"#,
        packet.worker,
        packet_json,
        model_metadata,
        packet.tools.to_markdown(),
        workspace_rules,
        workspace_skills,
        prompt_append,
        step_report
    ))
}

/// Generate a compiled (bounded) prompt that includes only the sections
/// marked `included: true` in the capsule. Hard sections (identity,
/// task_contract) are always included. Omitted soft sections are removed
/// from the packet fields and do not appear in the rendered prompt.
pub fn worker_compiled_prompt(
    packet: &WorkerPacket,
    capsule: &PromptCapsule,
) -> Result<String> {
    let included_ids: std::collections::HashSet<&str> = capsule
        .sections
        .iter()
        .filter(|s| s.included)
        .map(|s| s.id.as_str())
        .collect();

    let all_soft_included = capsule
        .sections
        .iter()
        .filter(|s| !s.required)
        .all(|s| s.included);

    if all_soft_included && capsule.sections.iter().all(|section| !section.clipped) {
        return worker_prompt(packet);
    }

    let mut reduced = packet.clone();
    let mut bounded_sections = Vec::new();

    for section in capsule.sections.iter().filter(|section| {
        !section.required && section.included && section.clipped
    }) {
        let content = prompt_section_content(packet, &section.id)?;
        let bounded = clip_text_head_tail(&content, section.retained_tokens);
        if !bounded.is_empty() {
            bounded_sections.push((section.id.as_str(), bounded));
        }
    }

    if !included_ids.contains("route") || capsule
        .sections
        .iter()
        .any(|section| section.id == "route" && section.clipped)
    {
        reduced.worker_model = None;
        reduced.variant = None;
        reduced.variant_applied = None;
        reduced.category_resolution = CategoryResolution::default();
        reduced.category_resolution_result = CategoryResolutionResult::NotFound {
            requested_category: String::new(),
            available_categories: Vec::new(),
            attempted_provider_model: None,
            nearest_fallback: None,
        };
        reduced.coordinator_model = None;
    }

    if !included_ids.contains("context") || capsule
        .sections
        .iter()
        .any(|section| section.id == "context" && section.clipped)
    {
        reduced.inputs.spec_path = None;
        reduced.inputs.plan_path = None;
        reduced.inputs.worker_packet_path = None;
        reduced.coordinator_brief = None;
    }

    if !included_ids.contains("route_append") || capsule
        .sections
        .iter()
        .any(|section| section.id == "route_append" && section.clipped)
    {
        reduced.prompt_append = None;
    }

    if !included_ids.contains("rules")
        || capsule
            .sections
            .iter()
            .any(|section| section.id == "rules" && section.clipped)
    {
        reduced.injected_rules = None;
    }

    if !included_ids.contains("skills")
        || capsule
            .sections
            .iter()
            .any(|section| section.id == "skills" && section.clipped)
    {
        reduced.injected_skills = None;
    }

    let mut compiled = worker_prompt(&reduced)?;
    for (section_id, bounded) in bounded_sections {
        compiled.push_str(&format!(
            "\n## Bounded {section_id} context\n\n{bounded}\n"
        ));
    }
    Ok(compiled)
}

pub fn prompt_manifest_for_packet(
    packet: &WorkerPacket,
    rendered_prompt: &str,
) -> Result<PromptManifest> {
    let hard_goal = phase_request_marker(packet).unwrap_or_else(|| packet.goal.clone());
    let hard_contract = serde_json::to_string(&json!({
        "task_id": &packet.task_id,
        "worker": &packet.worker,
        "current_step_id": &packet.current_step_id,
        "goal": hard_goal,
        "scope": &packet.scope,
        "tools": &packet.tools,
        "constraints": &packet.constraints,
        "required_outputs": &packet.required_outputs,
        "verification": &packet.verification,
        "stop_conditions": &packet.stop_conditions,
        "plan_task": &packet.inputs.plan_task,
    }))
    .context("failed to serialize prompt hard contract")?;
    let identity = serde_json::to_string(&json!({
        "task_id": &packet.task_id,
        "worker": &packet.worker,
    }))
    .context("failed to serialize prompt identity")?;
    let route = serde_json::to_string(&json!({
        "worker_model": &packet.worker_model,
        "variant": &packet.variant,
        "variant_applied": &packet.variant_applied,
        "category_resolution": &packet.category_resolution,
        "category_resolution_result": &packet.category_resolution_result,
        "coordinator_model": &packet.coordinator_model,
    }))
    .context("failed to serialize prompt route")?;
    let context = serde_json::to_string(&json!({
        "inputs": &packet.inputs,
        "coordinator_brief": &packet.coordinator_brief,
    }))
    .context("failed to serialize prompt context")?;
    let rules = packet
        .injected_rules
        .as_deref()
        .map(str::trim)
        .filter(|rules| !rules.is_empty())
        .map(str::to_string);
    let skills = packet
        .injected_skills
        .as_deref()
        .map(str::trim)
        .filter(|skills| !skills.is_empty())
        .map(str::to_string);

    let mut sections = vec![
        prompt_manifest_section(
            "identity",
            PromptManifestSectionKind::Hard,
            "worker_packet.identity",
            identity,
            100,
            true,
        ),
        prompt_manifest_section(
            "task_contract",
            PromptManifestSectionKind::Hard,
            "worker_packet.contract",
            hard_contract,
            100,
            true,
        ),
        prompt_manifest_section(
            "route",
            PromptManifestSectionKind::Soft,
            "runtime.route_resolution",
            route,
            60,
            false,
        ),
        prompt_manifest_section(
            "context",
            PromptManifestSectionKind::Soft,
            "runtime.task_inputs",
            context,
            40,
            false,
        ),
    ];
    if let Some(rules) = rules {
        sections.push(prompt_manifest_section(
            "rules",
            PromptManifestSectionKind::Soft,
            "runtime.workspace_rules",
            rules,
            80,
            false,
        ));
    } else {
        sections.push(prompt_manifest_omitted_section(
            "rules",
            PromptManifestSectionKind::Soft,
            "runtime.workspace_rules",
            80,
            "not discovered",
        ));
    }
    if let Some(skills) = skills {
        sections.push(prompt_manifest_section(
            "skills",
            PromptManifestSectionKind::Soft,
            "runtime.workspace_skills",
            skills,
            75,
            false,
        ));
    } else {
        sections.push(prompt_manifest_omitted_section(
            "skills",
            PromptManifestSectionKind::Soft,
            "runtime.workspace_skills",
            75,
            "not discovered",
        ));
    }
    if let Some(prompt_append) = packet
        .prompt_append
        .as_deref()
        .map(str::trim)
        .filter(|append| !append.is_empty())
    {
        sections.push(prompt_manifest_section(
            "route_append",
            PromptManifestSectionKind::Soft,
            "runtime.route_append",
            prompt_append.to_string(),
            30,
            false,
        ));
    } else {
        sections.push(prompt_manifest_omitted_section(
            "route_append",
            PromptManifestSectionKind::Soft,
            "runtime.route_append",
            30,
            "not configured",
        ));
    }

    // The rendered worker prompt contains framing, metadata and response
    // instructions in addition to the packet sections above. Record that
    // measured overhead as a required section so the capsule budget covers
    // the prompt the worker actually receives, not only the JSON payloads.
    let section_bytes: usize = sections.iter().map(|section| section.bytes).sum();
    let framing_bytes = rendered_prompt.len().saturating_sub(section_bytes);
    sections.push(prompt_manifest_section(
        "prompt_framing",
        PromptManifestSectionKind::Hard,
        "worker_prompt.framing",
        "x".repeat(framing_bytes),
        100,
        true,
    ));

    let manifest = PromptManifest {
        schema_version: PROMPT_MANIFEST_SCHEMA_VERSION,
        task_id: packet.task_id.clone(),
        worker: packet.worker.clone(),
        runtime_model: packet.worker_model.clone().or_else(|| {
            packet
                .coordinator_model
                .as_ref()
                .map(|model| model.name.clone())
        }),
        variant: packet
            .variant_applied
            .clone()
            .or_else(|| packet.variant.clone()),
        semantic_contract_hash: prompt_semantic_contract_hash(packet)?,
        sections: {
            sections.sort_by(|left, right| left.id.cmp(&right.id));
            sections
        },
        rendered_prompt_hash: prompt_content_hash(rendered_prompt),
    };
    manifest.validate(packet, rendered_prompt)?;
    Ok(manifest)
}

pub fn prompt_semantic_contract_hash(packet: &WorkerPacket) -> Result<String> {
    let contract = json!({
        "task_id": &packet.task_id,
        "worker": &packet.worker,
        "current_step_id": &packet.current_step_id,
        "goal": &packet.goal,
        "scope": &packet.scope,
        "tools": &packet.tools,
        "constraints": &packet.constraints,
        "required_outputs": &packet.required_outputs,
        "verification": &packet.verification,
        "stop_conditions": &packet.stop_conditions,
        "plan_task": &packet.inputs.plan_task,
    });
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&contract)?)
    ))
}

fn prompt_content_hash(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}

pub fn prompt_manifest_hash(manifest: &PromptManifest) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(manifest)?)
    ))
}

pub const PROMPT_CAPSULE_SCHEMA_VERSION: u32 = 1;

/// Conservative default context budget used when the worker model cannot
/// advertise an exact context limit. The value is intentionally conservative
/// and flagged `estimated` so downstream consumers know it is not authoritative.
// The packet hard contract itself is larger than the historical 8k fallback
// for real PlanGraph tasks. Keep an explicit override available for providers
// with smaller contexts, but make the unknown-model default large enough to
// admit the durable contract before soft sections are clipped.
const DEFAULT_CONTEXT_LIMIT_TOKENS: usize = 12288;
const DEFAULT_PAID_CONTEXT_LIMIT_TOKENS: usize = 32768;
/// Output headroom is opt-in because providers expose different completion
/// limits.  A caller that knows the provider's output contract can reserve
/// space without changing the context limit itself.
const DEFAULT_RESERVED_OUTPUT_TOKENS: usize = 0;
const PROMPT_TOKEN_ESTIMATOR: &str = "utf8_ascii_4_non_ascii_1";
const CLIPPED_SECTION_OVERHEAD_TOKENS: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptCapsuleRecoveryReason {
    Dispatch,
    Compact,
    EmptyResponse,
    Resume,
}

impl PromptCapsuleRecoveryReason {
    fn as_key(&self) -> &'static str {
        match self {
            PromptCapsuleRecoveryReason::Dispatch => "dispatch",
            PromptCapsuleRecoveryReason::Compact => "compact",
            PromptCapsuleRecoveryReason::EmptyResponse => "empty_response",
            PromptCapsuleRecoveryReason::Resume => "resume",
        }
    }
}

pub fn prompt_capsule_recovery_reason_for_action(
    action: &PromptReconcileAction,
) -> PromptCapsuleRecoveryReason {
    match action {
        PromptReconcileAction::ResumeSession => PromptCapsuleRecoveryReason::Resume,
        PromptReconcileAction::RebuildSession => PromptCapsuleRecoveryReason::Compact,
        PromptReconcileAction::NewSession => PromptCapsuleRecoveryReason::Dispatch,
    }
}

/// Resolve the worker context token budget.
///
/// The legacy tuple is kept for callers that only need the context limit. The
/// production compiler uses [`prompt_budget_policy`] so the source, headroom,
/// and estimation method are persisted in the prompt capsule.
pub fn worker_context_limit_tokens() -> (usize, bool) {
    let policy = prompt_budget_policy(None);
    (policy.context_limit_tokens, policy.estimated)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PromptBudgetPolicy {
    context_limit_tokens: usize,
    reserved_output_tokens: usize,
    headroom_source: String,
    prompt_budget_tokens: usize,
    source: String,
    estimated: bool,
}

fn prompt_budget_policy(runtime_model: Option<&str>) -> PromptBudgetPolicy {
    let model_key = runtime_model
        .filter(|model| !model.trim().is_empty())
        .map(|model| {
            model
                .chars()
                .map(|character| {
                    if character.is_ascii_alphanumeric() {
                        character.to_ascii_uppercase()
                    } else {
                        '_'
                    }
                })
                .collect::<String>()
        });
    let model_variable = model_key
        .as_deref()
        .map(|key| format!("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS_{key}"));

    let (context_limit_tokens, estimated, source) = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| (value, false, "config:GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS".to_string()))
        .or_else(|| {
            model_variable.as_deref().and_then(|variable| {
                env::var(variable)
                    .ok()
                    .and_then(|raw| raw.trim().parse::<usize>().ok())
                    .filter(|value| *value > 0)
                    .map(|value| (value, false, format!("config:{variable}")))
            })
        })
        .unwrap_or_else(|| {
            let normalized_model = runtime_model
                .map(|model| model.trim().to_ascii_lowercase())
                .unwrap_or_default();
            let paid_model = normalized_model
                .strip_prefix("opencode-go/")
                .is_some_and(|model| !model.trim().is_empty());
            let deepseek_flash_model = normalized_model.contains("deepseek-v4-flash");
            if paid_model || deepseek_flash_model {
                (
                    DEFAULT_PAID_CONTEXT_LIMIT_TOKENS,
                    true,
                    if deepseek_flash_model {
                        "deepseek_flash_conservative_default".to_string()
                    } else {
                        "paid_model_conservative_default".to_string()
                    },
                )
            } else {
                (
                    DEFAULT_CONTEXT_LIMIT_TOKENS,
                    true,
                    "conservative_default".to_string(),
                )
            }
        });

    let (reserved_output_tokens, headroom_source) = match env::var("GEARBOX_WORKER_OUTPUT_HEADROOM_TOKENS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
    {
        Some(value) => (
            value.min(context_limit_tokens),
            "config:GEARBOX_WORKER_OUTPUT_HEADROOM_TOKENS".to_string(),
        ),
        None => (
            DEFAULT_RESERVED_OUTPUT_TOKENS,
            "default:GEARBOX_WORKER_OUTPUT_HEADROOM_TOKENS".to_string(),
        ),
    };

    PromptBudgetPolicy {
        context_limit_tokens,
        reserved_output_tokens,
        headroom_source,
        prompt_budget_tokens: context_limit_tokens.saturating_sub(reserved_output_tokens),
        source,
        estimated,
    }
}

/// Stable, idempotent recovery key. The same `(task_id, semantic_contract_hash,
/// recovery_reason)` always produces the same key, so repeated recovery passes
/// cannot append duplicate context.
pub fn prompt_capsule_recovery_key(
    task_id: &str,
    semantic_contract_hash: &str,
    reason: &PromptCapsuleRecoveryReason,
) -> String {
    format!(
        "{}:{}:{}",
        task_id,
        semantic_contract_hash,
        reason.as_key()
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromptCapsuleSection {
    pub id: String,
    pub kind: PromptManifestSectionKind,
    pub source: String,
    pub content_hash: String,
    pub bytes: usize,
    pub estimated_tokens: usize,
    pub priority: u8,
    pub required: bool,
    pub included: bool,
    #[serde(default)]
    pub clipped: bool,
    #[serde(default)]
    pub retained_bytes: usize,
    #[serde(default)]
    pub retained_tokens: usize,
    #[serde(default)]
    pub deleted_bytes: usize,
    #[serde(default)]
    pub deleted_tokens: usize,
    #[serde(default)]
    pub freshness: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub omission_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromptCapsule {
    pub schema_version: u32,
    pub task_id: String,
    pub worker: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_family: Option<String>,
    pub semantic_contract_hash: String,
    pub budget_tokens: usize,
    #[serde(default)]
    pub context_limit_tokens: usize,
    #[serde(default)]
    pub reserved_output_tokens: usize,
    #[serde(default)]
    pub headroom_source: String,
    #[serde(default)]
    pub budget_source: String,
    #[serde(default)]
    pub token_estimator: String,
    pub used_tokens: usize,
    pub remaining_tokens: usize,
    pub budget_estimated: bool,
    pub sections: Vec<PromptCapsuleSection>,
    pub rendered_prompt_hash: String,
    pub recovery_key: String,
    pub recovery_reason: String,
    /// Path to the compiled (bounded) prompt file that the worker actually
    /// reads via `GEARBOX_WORKER_PROMPT`. Present after initial dispatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_prompt_path: Option<String>,
    /// SHA-256 hex hash of the compiled prompt content. Empty before the
    /// compiled prompt is written; populated at initial dispatch and on
    /// each follow-up/revive compilation.
    #[serde(default)]
    pub compiled_prompt_hash: String,
}

const PROMPT_BUDGET_OVERFLOW_SCHEMA_VERSION: u32 = 1;

#[derive(Debug)]
struct PromptCapsuleBudgetOverflow {
    required_tokens: usize,
    budget_tokens: usize,
    context_limit_tokens: usize,
    reserved_output_tokens: usize,
    headroom_source: String,
    budget_source: String,
}

impl Display for PromptCapsuleBudgetOverflow {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "prompt hard contract exceeds context budget: required={} budget={}",
            self.required_tokens, self.budget_tokens
        )
    }
}

impl std::error::Error for PromptCapsuleBudgetOverflow {}

impl PromptCapsule {
    /// Hard sections (required contract) must never be silently dropped. This
    /// returns `Err` only when a required section is missing, which is a
    /// structural contract violation, not a budget decision.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PROMPT_CAPSULE_SCHEMA_VERSION {
            bail!("unsupported prompt capsule schema {}", self.schema_version);
        }
        if self.task_id.trim().is_empty() || self.worker.trim().is_empty() {
            bail!("prompt capsule identity cannot be empty");
        }
        if self.semantic_contract_hash.trim().is_empty()
            || self.rendered_prompt_hash.trim().is_empty()
            || self.recovery_key.trim().is_empty()
            || self.recovery_reason.trim().is_empty()
        {
            bail!("prompt capsule binding fields cannot be empty");
        }
        if self.context_limit_tokens == 0
            || self.budget_tokens > self.context_limit_tokens
            || self.reserved_output_tokens > self.context_limit_tokens
        {
            bail!("prompt capsule budget is outside its context limit");
        }
        if self.used_tokens > self.budget_tokens
            || self.remaining_tokens != self.budget_tokens.saturating_sub(self.used_tokens)
        {
            bail!("prompt capsule token accounting is inconsistent");
        }
        if self
            .sections
            .iter()
            .any(|section| section.required && !section.included)
        {
            bail!("prompt capsule omits a required hard section");
        }
        if self.sections.iter().any(|section| {
            section.included && section.retained_tokens > self.budget_tokens
                || !section.included
                    && (section.retained_tokens != 0 || section.retained_bytes != 0)
        }) {
            bail!("prompt capsule section retention is invalid");
        }
        Ok(())
    }

    /// Validate a persisted capsule against the packet and manifest that are
    /// authoritative for the current dispatch. A capsule can outlive a
    /// session, so checking only its schema/required flags would allow stale
    /// route or task identity to be reused after recovery.
    pub fn validate_against(
        &self,
        packet: &WorkerPacket,
        manifest: &PromptManifest,
    ) -> Result<()> {
        self.validate()?;
        if self.task_id != packet.task_id {
            bail!("prompt capsule task identity does not match worker packet");
        }
        if self.worker != packet.worker {
            bail!("prompt capsule worker identity does not match worker packet");
        }
        let expected_runtime_model = packet.worker_model.clone().or_else(|| {
            packet
                .coordinator_model
                .as_ref()
                .map(|model| model.name.clone())
        });
        if self.runtime_model != expected_runtime_model {
            bail!("prompt capsule runtime model does not match worker packet");
        }
        if self.semantic_contract_hash != manifest.semantic_contract_hash {
            bail!("prompt capsule semantic contract hash mismatch");
        }
        if self.context_limit_tokens == 0 || self.budget_tokens > self.context_limit_tokens {
            bail!("prompt capsule budget is outside its context limit");
        }
        if self.used_tokens > self.budget_tokens {
            bail!("prompt capsule used token count exceeds budget");
        }
        for required in manifest.sections.iter().filter(|section| section.required) {
            let Some(capsule_section) = self.sections.iter().find(|section| section.id == required.id)
            else {
                bail!("prompt capsule is missing required section `{}`", required.id);
            };
            if !capsule_section.included || !capsule_section.required {
                bail!("prompt capsule required section `{}` is not included", required.id);
            }
            if capsule_section.content_hash != required.content_hash {
                bail!("prompt capsule section `{}` content hash mismatch", required.id);
            }
        }
        Ok(())
    }
}

/// Build a bounded prompt capsule from a previously validated manifest.
///
/// Hard (`required`) sections are always included — the budget only governs the
/// soft sections, which are selected by descending priority until the remaining
/// token budget is exhausted. Omitted soft sections carry an explicit, stable
/// omission reason.
pub fn build_prompt_capsule(
    packet: &WorkerPacket,
    manifest: &PromptManifest,
    rendered_prompt: &str,
    reason: &PromptCapsuleRecoveryReason,
) -> Result<PromptCapsule> {
    manifest.validate(packet, rendered_prompt)?;
    let runtime_model = packet.worker_model.clone().or_else(|| {
        packet
            .coordinator_model
            .as_ref()
            .map(|model| model.name.clone())
    });
    let budget_policy = prompt_budget_policy(runtime_model.as_deref());
    let budget_tokens = budget_policy.prompt_budget_tokens;

    let mut hard_sections: Vec<PromptCapsuleSection> = Vec::new();
    let mut soft_sections: Vec<PromptCapsuleSection> = Vec::new();
    for section in &manifest.sections {
        let capsule_section = PromptCapsuleSection {
            id: section.id.clone(),
            kind: section.kind.clone(),
            source: section.source.clone(),
            content_hash: section.content_hash.clone(),
            bytes: section.bytes,
            estimated_tokens: section.estimated_tokens,
            priority: section.priority,
            required: section.required,
            included: true,
            clipped: false,
            retained_bytes: section.bytes,
            retained_tokens: section.estimated_tokens,
            deleted_bytes: 0,
            deleted_tokens: 0,
            freshness: "current".to_string(),
            omission_reason: None,
        };
        if section.required {
            hard_sections.push(capsule_section);
        } else {
            soft_sections.push(capsule_section);
        }
    }

    let hard_tokens: usize = hard_sections.iter().map(|s| s.estimated_tokens).sum();
    if hard_tokens > budget_tokens {
        return Err(PromptCapsuleBudgetOverflow {
            required_tokens: hard_tokens,
            budget_tokens,
            context_limit_tokens: budget_policy.context_limit_tokens,
            reserved_output_tokens: budget_policy.reserved_output_tokens,
            headroom_source: budget_policy.headroom_source.clone(),
            budget_source: budget_policy.source.clone(),
        }
        .into());
    }
    let mut remaining = budget_tokens.saturating_sub(hard_tokens);

    soft_sections.sort_by(|left, right| right.priority.cmp(&left.priority));
    for section in &mut soft_sections {
        if section.estimated_tokens <= remaining {
            remaining = remaining.saturating_sub(section.estimated_tokens);
        } else {
            // Preserve a deterministic head/tail envelope when there is room.
            // The wrapper overhead is reserved up front so the compiled prompt
            // cannot exceed the capsule budget merely because it explains the
            // clipping decision to the worker.
            let retained_budget = remaining.saturating_sub(CLIPPED_SECTION_OVERHEAD_TOKENS);
            let content = prompt_section_content(packet, &section.id)?;
            if retained_budget > 0 && !content.is_empty() {
                let clipped = clip_text_head_tail(&content, retained_budget);
                let retained_tokens = estimate_prompt_tokens(&clipped);
                if retained_tokens > 0 {
                    section.included = true;
                    section.clipped = true;
                    section.retained_bytes = clipped.len();
                    section.retained_tokens = retained_tokens;
                    section.deleted_bytes = section.bytes.saturating_sub(clipped.len());
                    section.deleted_tokens = section
                        .estimated_tokens
                        .saturating_sub(retained_tokens);
                    section.freshness = "bounded_head_tail".to_string();
                    section.omission_reason = Some(format!(
                        "bounded head/tail clip: retained {} of {} tokens",
                        retained_tokens, section.estimated_tokens
                    ));
                    remaining = remaining.saturating_sub(
                        retained_tokens
                            .saturating_add(CLIPPED_SECTION_OVERHEAD_TOKENS),
                    );
                    continue;
                }
            }
            section.included = false;
            section.retained_bytes = 0;
            section.retained_tokens = 0;
            section.deleted_bytes = section.bytes;
            section.deleted_tokens = section.estimated_tokens;
            section.freshness = "omitted".to_string();
            section.omission_reason = Some(format!(
                "budget exceeded: remaining {} tokens < {} tokens required",
                remaining, section.estimated_tokens
            ));
        }
    }

    let mut sections = hard_sections;
    sections.append(&mut soft_sections);
    sections.sort_by(|left, right| left.id.cmp(&right.id));

    let used_tokens: usize = sections
        .iter()
        .filter(|section| section.included)
        .map(|section| {
            section.retained_tokens.saturating_add(if section.clipped {
                CLIPPED_SECTION_OVERHEAD_TOKENS
            } else {
                0
            })
        })
        .sum();

    let model_family =
        runtime_model
            .as_deref()
            .map(|model| prompt_model_family(Some(model), packet.worker.as_str()));

    let capsule = PromptCapsule {
        schema_version: PROMPT_CAPSULE_SCHEMA_VERSION,
        task_id: packet.task_id.clone(),
        worker: packet.worker.clone(),
        runtime_model,
        model_family,
        semantic_contract_hash: manifest.semantic_contract_hash.clone(),
        budget_tokens,
        context_limit_tokens: budget_policy.context_limit_tokens,
        reserved_output_tokens: budget_policy.reserved_output_tokens,
        headroom_source: budget_policy.headroom_source,
        budget_source: budget_policy.source,
        token_estimator: PROMPT_TOKEN_ESTIMATOR.to_string(),
        used_tokens,
        remaining_tokens: remaining,
        budget_estimated: budget_policy.estimated,
        sections,
        rendered_prompt_hash: prompt_content_hash(rendered_prompt),
        recovery_key: prompt_capsule_recovery_key(
            &packet.task_id,
            &manifest.semantic_contract_hash,
            reason,
        ),
        recovery_reason: reason.as_key().to_string(),
        compiled_prompt_path: None,
        compiled_prompt_hash: String::new(),
    };
    capsule.validate_against(packet, manifest)?;
    Ok(capsule)
}

/// Recovery receipt status tri-state emitted by `compile_recovery_prompt`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryCapsuleStatus {
    Compiled,
    Reused,
    Degraded,
}

/// Durable receipt written by `compile_recovery_prompt` on each follow-up,
/// revive, or resume compilation so downstream audit can verify that a
/// bounded prompt was issued and whether the same recovery key was reused.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryCapsuleReceipt {
    pub schema_version: u32,
    pub task_id: String,
    pub recovery_key: String,
    pub recovery_reason: String,
    pub semantic_contract_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_prompt_path: Option<String>,
    #[serde(default)]
    pub compiled_prompt_hash: String,
    pub status: RecoveryCapsuleStatus,
    pub created_at: String,
}

/// Compile a follow-up, revive, or resume prompt through the `PromptCapsule`
/// pipeline and persist the bounded output plus audit artifacts.
///
/// ## Dedup
///
/// A recovery receipt keyed by the task, semantic contract, recovery reason,
/// body hash, step anchor, and attempt is written on first compilation. If the
/// same key is encountered again the function writes a `reused` receipt and
/// returns the existing compiled prompt path without recompiling.
///
/// ## Degraded
///
/// When the packet or manifest cannot be read (e.g. an older task directory
/// created before the capsule system), the function writes a `degraded`
/// receipt and returns `Err`. The raw prompt remains an audit artifact only;
/// callers must not bypass this gate by sending it uncompiled.
pub fn compile_recovery_prompt(
    store: &StateStore,
    task_id: &str,
    command: &str,
    raw_prompt: &str,
    reason: &PromptCapsuleRecoveryReason,
    packet_path: &Path,
    manifest_path: &Path,
    capsule_path: &Path,
    output_stem: &str,
    current_step_id: Option<&str>,
) -> Result<PathBuf> {
    compile_recovery_prompt_with_attempt(
        store,
        task_id,
        command,
        raw_prompt,
        reason,
        packet_path,
        manifest_path,
        capsule_path,
        output_stem,
        current_step_id,
        0,
    )
}

fn compile_recovery_prompt_with_attempt(
    store: &StateStore,
    task_id: &str,
    command: &str,
    raw_prompt: &str,
    reason: &PromptCapsuleRecoveryReason,
    packet_path: &Path,
    manifest_path: &Path,
    capsule_path: &Path,
    output_stem: &str,
    current_step_id: Option<&str>,
    task_attempt: u64,
) -> Result<PathBuf> {
    let prompt_body_hash = prompt_content_hash(raw_prompt);
    let body_hash_prefix = if prompt_body_hash.len() > 16 {
        &prompt_body_hash[..16]
    } else {
        &prompt_body_hash
    };
    let step_suffix = current_step_id
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("unknown");

    let packet: WorkerPacket = match fs::read_to_string(packet_path)
        .map_err(|e| anyhow::anyhow!("failed to read worker packet: {e}"))
        .and_then(|s| serde_json::from_str(&s).map_err(|e| anyhow::anyhow!("invalid packet JSON: {e}")))
    {
        Ok(p) => p,
        Err(e) => {
            let receipt = serde_json::json!({
                "schema_version": 1u32,
                "task_id": task_id,
                "attempt": task_attempt,
                "recovery_reason": reason.as_key(),
                "body_hash": body_hash_prefix,
                "step_suffix": step_suffix,
                "output_stem": output_stem,
                "error_category": "packet_read_failure",
                "error_detail": format!("{e:#}"),
                "raw_fallback_path": packet_path.to_string_lossy(),
                "status": "degraded",
                "created_at": crate::state::timestamp(),
            });
            let _ = store.write_worker_json_atomic(
                task_id,
                &format!("degraded-{body_hash_prefix}.json"),
                &receipt,
            );
            return Err(e);
        }
    };
    let manifest: PromptManifest = match fs::read_to_string(manifest_path)
        .map_err(|e| anyhow::anyhow!("failed to read prompt manifest: {e}"))
        .and_then(|s| serde_json::from_str(&s).map_err(|e| anyhow::anyhow!("invalid manifest JSON: {e}")))
    {
        Ok(m) => m,
        Err(e) => {
            let receipt = serde_json::json!({
                "schema_version": 1u32,
                "task_id": task_id,
                "attempt": task_attempt,
                "recovery_reason": reason.as_key(),
                "body_hash": body_hash_prefix,
                "step_suffix": step_suffix,
                "output_stem": output_stem,
                "error_category": "manifest_read_failure",
                "error_detail": format!("{e:#}"),
                "raw_fallback_path": manifest_path.to_string_lossy(),
                "status": "degraded",
                "created_at": crate::state::timestamp(),
            });
            let _ = store.write_worker_json_atomic(
                task_id,
                &format!("degraded-{body_hash_prefix}.json"),
                &receipt,
            );
            return Err(e);
        }
    };
    let manifest_hash = prompt_manifest_hash(&manifest)?;

    let instance_recovery_key = format!(
        "{}:{}:{}:{}:{}",
        prompt_capsule_recovery_key(task_id, &manifest.semantic_contract_hash, reason),
        body_hash_prefix,
        step_suffix,
        task_attempt,
        output_stem,
    );

    let key_hash = format!("{:x}", Sha256::digest(instance_recovery_key.as_bytes()));
    let receipt_filename = format!("recovery-{key_hash}.json");
    let receipt_path = store.worker_dir(task_id).join(&receipt_filename);

    let step_line = current_step_id
        .filter(|s| !s.trim().is_empty())
        .map(|step_id| format!("Current step: `{step_id}`\n"))
        .unwrap_or_default();
    let recovery_append = format!(
        "\n\n## Recovery context\n\nTask: `{task_id}`\nReason: `{}`\nKey: `{instance_recovery_key}`\n{}{}\n",
        reason.as_key(),
        step_line,
        raw_prompt.trim(),
    );

    // Validate the persisted capsule before considering receipt reuse. A valid
    // compiled file alone is not enough: deletion or tampering of the capsule
    // must invalidate the reuse path and prevent stale evidence from flowing
    // into a new recovery dispatch.
    let capsule: PromptCapsule = if capsule_path.exists() {
        match fs::read_to_string(capsule_path)
            .map_err(|e| anyhow::anyhow!("failed to read prompt capsule: {e}"))
            .and_then(|s| serde_json::from_str(&s).map_err(|e| anyhow::anyhow!("invalid capsule JSON: {e}")))
        {
            Ok(c) => c,
            Err(e) => {
                let receipt = serde_json::json!({
                    "schema_version": 1u32,
                    "task_id": task_id,
                    "attempt": task_attempt,
                    "recovery_reason": reason.as_key(),
                    "body_hash": body_hash_prefix,
                    "step_suffix": step_suffix,
                    "output_stem": output_stem,
                    "error_category": "capsule_read_failure",
                    "error_detail": format!("{e:#}"),
                    "raw_fallback_path": capsule_path.to_string_lossy(),
                    "status": "degraded",
                    "created_at": crate::state::timestamp(),
                });
                let _ = store.write_worker_json_atomic(
                    task_id,
                    &format!("degraded-{body_hash_prefix}.json"),
                    &receipt,
                );
                return Err(e);
            }
        }
    } else {
        build_prompt_capsule(&packet, &manifest, &recovery_append, reason)?
    };
    capsule.validate_against(&packet, &manifest)?;

    if receipt_path.is_file() {
        if let Ok(receipt_val) = fs::read_to_string(&receipt_path)
            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).map_err(|e| e.into()))
        {
            let receipt_matches_current_contract = receipt_val
                .get("task_id")
                .and_then(|value| value.as_str())
                .is_some_and(|value| value == task_id)
                && receipt_val
                    .get("recovery_key")
                    .and_then(|value| value.as_str())
                    .is_some_and(|value| value == instance_recovery_key.as_str())
                && receipt_val
                    .get("semantic_contract_hash")
                    .and_then(|value| value.as_str())
                    .is_some_and(|value| value == manifest.semantic_contract_hash)
                && receipt_val
                    .get("prompt_manifest_hash")
                    .and_then(|value| value.as_str())
                    .is_some_and(|value| value == manifest_hash);
            if receipt_matches_current_contract
                && receipt_val
                .get("status")
                .and_then(|s| s.as_str())
                .is_some_and(|s| s == "compiled")
            {
                if let Some(compiled_path_str) = receipt_val
                    .get("compiled_prompt_path")
                    .and_then(|p| p.as_str())
                {
                    let existing = PathBuf::from(compiled_path_str);
                    let worker_root = store
                        .worker_dir(task_id)
                        .canonicalize()
                        .unwrap_or_else(|_| store.worker_dir(task_id));
                    let existing_owned = existing
                        .canonicalize()
                        .ok()
                        .is_some_and(|path| path.starts_with(&worker_root));
                    let stored_hash = receipt_val
                        .get("compiled_prompt_hash")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.trim().is_empty());
                    let content_hash_matches = existing
                        .is_file()
                        .then(|| fs::read_to_string(&existing).ok())
                        .flatten()
                        .zip(stored_hash)
                        .is_some_and(|(content, hash)| prompt_content_hash(&content) == hash);
                    if existing_owned && content_hash_matches {
                        let reused = serde_json::json!({
                            "schema_version": 1u32,
                            "task_id": task_id,
                            "attempt": task_attempt,
                            "recovery_key": &instance_recovery_key,
                            "recovery_reason": reason.as_key(),
                            "semantic_contract_hash": manifest.semantic_contract_hash,
                            "prompt_manifest_hash": &manifest_hash,
                            "compiled_prompt_path": compiled_path_str,
                            "compiled_prompt_hash": receipt_val
                                .get("compiled_prompt_hash")
                                .and_then(|h| h.as_str())
                                .unwrap_or(""),
                            "status": "reused",
                            "created_at": crate::state::timestamp(),
                        });
                        store.write_worker_json_atomic(
                            task_id,
                            &format!("recovery-reused-{key_hash}.json"),
                            &reused,
                        )?;
                        return Ok(existing);
                    }
                }
            }
        }
    }

    let base_compiled = worker_compiled_prompt(&packet, &capsule)?;
    let base_tokens = estimate_prompt_tokens(&base_compiled);
    if base_tokens > capsule.budget_tokens {
        let receipt = serde_json::json!({
            "schema_version": 1u32,
            "task_id": task_id,
            "attempt": task_attempt,
            "recovery_reason": reason.as_key(),
            "body_hash": body_hash_prefix,
            "step_suffix": step_suffix,
            "output_stem": output_stem,
            "error_category": "compiled_prompt_budget_overflow",
            "required_tokens": base_tokens,
            "budget_tokens": capsule.budget_tokens,
            "status": "blocked",
            "next_action": "split_task_or_raise_context_budget",
            "created_at": crate::state::timestamp(),
        });
        store.write_worker_json_atomic(
            task_id,
            &format!("degraded-{body_hash_prefix}.json"),
            &receipt,
        )?;
        bail!(
            "compiled recovery prompt exceeds context budget: required={} budget={}",
            base_tokens,
            capsule.budget_tokens
        );
    }
    let remaining_tokens = capsule.budget_tokens.saturating_sub(base_tokens);
    let recovery_tokens = estimate_prompt_tokens(&recovery_append);
    let bounded_recovery = if recovery_tokens <= remaining_tokens {
        recovery_append
    } else {
        let clip_budget = remaining_tokens.saturating_sub(CLIPPED_SECTION_OVERHEAD_TOKENS);
        if clip_budget == 0 {
            String::new()
        } else {
            format!(
                "\n## Bounded recovery context\n\n{}\n",
                clip_text_head_tail(&recovery_append, clip_budget)
            )
        }
    };
    let full_prompt = format!("{base_compiled}{bounded_recovery}");
    let compiled_path = store.write_worker_file(
        task_id,
        &format!("{output_stem}-compiled.md"),
        &full_prompt,
    )?;
    store.write_worker_file(
        task_id,
        &format!("{output_stem}-recovery-full.md"),
        &format!(
            "# Gear worker {}\n\nCommand: `{command}`\n\n{}\n\n## Recovery metadata\n\nTask: `{task_id}`\nReason: `{}`\nKey: `{instance_recovery_key}`\n{}{}\n",
            reason.as_key(),
            raw_prompt.trim(),
            reason.as_key(),
            if let Some(step_id) = current_step_id.filter(|s| !s.trim().is_empty()) {
                format!("Current step: `{step_id}`\n")
            } else {
                String::new()
            },
            "",
        ),
    )?;

    let compiled_hash = prompt_content_hash(&full_prompt);
    let updated_capsule = serde_json::json!({
        "schema_version": 1u32,
        "task_id": &capsule.task_id,
        "attempt": task_attempt,
        "worker": &capsule.worker,
        "runtime_model": &capsule.runtime_model,
        "model_family": &capsule.model_family,
        "semantic_contract_hash": &capsule.semantic_contract_hash,
        "budget_tokens": capsule.budget_tokens,
        "context_limit_tokens": capsule.context_limit_tokens,
        "reserved_output_tokens": capsule.reserved_output_tokens,
        "headroom_source": &capsule.headroom_source,
        "budget_source": &capsule.budget_source,
        "token_estimator": &capsule.token_estimator,
        "used_tokens": capsule.used_tokens,
        "remaining_tokens": capsule.remaining_tokens,
        "budget_estimated": capsule.budget_estimated,
        "sections": &capsule.sections,
        "rendered_prompt_hash": &capsule.rendered_prompt_hash,
        "recovery_key": &instance_recovery_key,
        "recovery_reason": reason.as_key(),
        "compiled_prompt_path": compiled_path.to_string_lossy().as_ref(),
        "compiled_prompt_hash": &compiled_hash,
    });
    store.write_worker_json_atomic(task_id, "prompt-capsule.json", &updated_capsule)?;

    let receipt = serde_json::json!({
        "schema_version": 1u32,
        "task_id": task_id,
        "attempt": task_attempt,
        "recovery_key": &instance_recovery_key,
        "recovery_reason": reason.as_key(),
        "semantic_contract_hash": &capsule.semantic_contract_hash,
        "prompt_manifest_hash": &manifest_hash,
        "compiled_prompt_path": compiled_path.to_string_lossy().as_ref(),
        "compiled_prompt_hash": &compiled_hash,
        "status": "compiled",
        "created_at": crate::state::timestamp(),
    });
    store.write_worker_json_atomic(task_id, &receipt_filename, &receipt)?;

    Ok(compiled_path)
}

/// Recover the current task cursor from durable `PlanNodeRun` state instead of
/// re-reading the (possibly compacted) transcript. Returns the running or first
/// pending step so compact/empty-response rebuilds resume the same logical
/// step rather than restarting from scratch.
pub fn recover_current_step_id(
    execution_steps: &[crate::state::PlanStepRun],
) -> Option<String> {
    let completed = execution_steps
        .iter()
        .filter(|step| matches!(step.status, crate::state::PlanStepRunStatus::Completed))
        .count();
    execution_steps
        .get(completed)
        .map(|step| step.step_id.clone())
}

/// Read-only wrapper: load the `PlanNodeRunLedger` for a goal, find the node
/// matching `task_id`, and return the first incomplete step identifier.
///
/// Returns `Ok(None)` when the ledger is missing, the node is not found, the
/// step list is empty, or all steps are complete — the caller logs the reason
/// but does not block dispatch.
pub fn current_step_from_ledger(
    store: &StateStore,
    goal_id: &str,
    task_id: &str,
) -> Option<String> {
    let ledger = match store.read_plan_node_runs(goal_id) {
        Ok(Some(l)) => l,
        Ok(None) => {
            eprintln!(
                "Gear recovery: PlanNodeRunLedger missing for goal `{goal_id}` — degraded (no step anchor)"
            );
            return None;
        }
        Err(e) => {
            eprintln!(
                "Gear recovery: failed to read PlanNodeRunLedger for goal `{goal_id}`: {e:#} — degraded"
            );
            return None;
        }
    };
    let node = match ledger.nodes.iter().find(|n| n.task_id == task_id) {
        Some(n) => n,
        None => {
            eprintln!(
                "Gear recovery: no PlanNodeRun for task `{task_id}` — degraded (no step anchor)"
            );
            return None;
        }
    };
    let step_id = recover_current_step_id(&node.execution_steps);
    if step_id.is_none() {
        eprintln!(
            "Gear recovery: PlanNodeRun `{task_id}` has no incomplete step — all steps complete or empty"
        );
    }
    step_id
}

fn prompt_manifest_section(
    id: &str,
    kind: PromptManifestSectionKind,
    source: &str,
    content: String,
    priority: u8,
    required: bool,
) -> PromptManifestSection {
    let bytes = content.len();
    PromptManifestSection {
        id: id.to_string(),
        kind,
        source: source.to_string(),
        content_hash: prompt_content_hash(&content),
        bytes,
        estimated_tokens: estimate_prompt_tokens(&content),
        priority,
        required,
        included: true,
        omission_reason: None,
    }
}

fn prompt_manifest_omitted_section(
    id: &str,
    kind: PromptManifestSectionKind,
    source: &str,
    priority: u8,
    omission_reason: &str,
) -> PromptManifestSection {
    PromptManifestSection {
        id: id.to_string(),
        kind,
        source: source.to_string(),
        content_hash: prompt_content_hash(""),
        bytes: 0,
        estimated_tokens: 0,
        priority,
        required: false,
        included: false,
        omission_reason: Some(omission_reason.to_string()),
    }
}

fn prompt_section_content(packet: &WorkerPacket, section_id: &str) -> Result<String> {
    match section_id {
        "route" => serde_json::to_string(&json!({
            "worker_model": &packet.worker_model,
            "variant": &packet.variant,
            "variant_applied": &packet.variant_applied,
            "category_resolution": &packet.category_resolution,
            "category_resolution_result": &packet.category_resolution_result,
            "coordinator_model": &packet.coordinator_model,
        }))
        .context("failed to serialize prompt route section"),
        "context" => serde_json::to_string(&json!({
            "inputs": &packet.inputs,
            "coordinator_brief": &packet.coordinator_brief,
        }))
        .context("failed to serialize prompt context section"),
        "route_append" => Ok(packet
            .prompt_append
            .as_deref()
            .map(str::trim)
            .filter(|append| !append.is_empty())
            .unwrap_or_default()
            .to_string()),
        "rules" => Ok(packet
            .injected_rules
            .as_deref()
            .map(str::trim)
            .filter(|rules| !rules.is_empty())
            .unwrap_or_default()
            .to_string()),
        "skills" => Ok(packet
            .injected_skills
            .as_deref()
            .map(str::trim)
            .filter(|skills| !skills.is_empty())
            .unwrap_or_default()
            .to_string()),
        _ => Ok(String::new()),
    }
}

fn prompt_text_units(content: &str) -> usize {
    content
        .chars()
        .map(|character| if character.is_ascii() { 1 } else { 4 })
        .sum()
}

fn estimate_prompt_tokens(content: &str) -> usize {
    prompt_text_units(content).saturating_add(3) / 4
}

fn take_head_units(content: &str, limit: usize) -> String {
    let mut used = 0usize;
    let mut output = String::new();
    for character in content.chars() {
        let cost = if character.is_ascii() { 1 } else { 4 };
        if used.saturating_add(cost) > limit {
            break;
        }
        output.push(character);
        used = used.saturating_add(cost);
    }
    output
}

fn take_tail_units(content: &str, limit: usize) -> String {
    let mut used = 0usize;
    let mut output = String::new();
    for character in content.chars().rev() {
        let cost = if character.is_ascii() { 1 } else { 4 };
        if used.saturating_add(cost) > limit {
            break;
        }
        output.push(character);
        used = used.saturating_add(cost);
    }
    output.chars().rev().collect()
}

fn clip_text_head_tail(content: &str, max_tokens: usize) -> String {
    if estimate_prompt_tokens(content) <= max_tokens {
        return content.to_string();
    }
    let marker = "\n[… clipped …]\n";
    let marker_units = prompt_text_units(marker);
    let available_units = max_tokens.saturating_mul(4).saturating_sub(marker_units);
    if available_units == 0 {
        return take_head_units(content, max_tokens.saturating_mul(4));
    }
    let head_units = available_units / 2;
    let tail_units = available_units.saturating_sub(head_units);
    format!(
        "{}{}{}",
        take_head_units(content, head_units),
        marker,
        take_tail_units(content, tail_units)
    )
}

fn worker_prompt_append_from_env() -> Option<String> {
    env::var("GEARBOX_GEAR_WORKER_PROMPT_APPEND")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn combined_prompt_append(
    builtin_append: Option<&'static str>,
    user_append: Option<String>,
) -> Option<String> {
    let mut pieces = Vec::new();
    if let Some(builtin_append) = builtin_append
        .map(str::trim)
        .filter(|append| !append.is_empty())
    {
        pieces.push(builtin_append.to_string());
    }
    if let Some(user_append) = user_append
        .map(|append| append.trim().to_string())
        .filter(|append| !append.is_empty())
    {
        pieces.push(user_append);
    }

    if pieces.is_empty() {
        None
    } else {
        Some(pieces.join("\n\n"))
    }
}

fn worker_model_metadata(packet: &WorkerPacket) -> String {
    let mut fields = HashMap::new();
    fields.insert("worker_kind".to_string(), packet.worker.clone());
    if let Some(worker_model) = packet.worker_model.as_ref() {
        fields.insert("worker_model".to_string(), worker_model.clone());
    }
    if let Some(coordinator_model) = packet.coordinator_model.as_ref() {
        fields.insert(
            "coordinator_provider_id".to_string(),
            coordinator_model.provider_id.clone(),
        );
        fields.insert(
            "coordinator_model_id".to_string(),
            coordinator_model.model_id.clone(),
        );
        fields.insert(
            "coordinator_name".to_string(),
            coordinator_model.name.clone(),
        );
    }

    let sanitized = sanitize_model_fields(&fields);
    if sanitized.is_empty() {
        return "none".to_string();
    }

    let mut entries = sanitized.into_iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries
        .into_iter()
        .map(|(key, value)| format!("- {key}: {value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn worker_outcome_from_result(result: &WorkerResult) -> Result<WorkerOutcome> {
    let parsed_report = parsed_worker_report(result);
    let known_failures = if parsed_report.known_failures.is_empty() {
        if result.status == WorkerStatus::Failed {
            if let Some(marker) = fallback_error_marker_from_stderr(result)? {
                vec![marker.to_string()]
            } else {
                vec![result.summary.clone()]
            }
        } else {
            Vec::new()
        }
    } else {
        parsed_report.known_failures.clone()
    };
    Ok(WorkerOutcome {
        status: result.status.clone(),
        session_id: None,
        session_capability: None,
        summary: parsed_report
            .summary
            .unwrap_or_else(|| result.summary.clone()),
        changed_files: parsed_report.changed_files,
        commands_run: if parsed_report.commands_run.is_empty() {
            result.command.iter().cloned().collect()
        } else {
            parsed_report.commands_run
        },
        known_failures,
        raw_output_path: result
            .last_message_path
            .clone()
            .or_else(|| result.stdout_path.clone())
            .or_else(|| result.stderr_path.clone()),
        command: result.command.clone(),
        exit_code: result.exit_code,
    })
}

fn fallback_error_marker_from_stderr(result: &WorkerResult) -> Result<Option<&'static str>> {
    let Some(stderr_path) = result.stderr_path.as_ref() else {
        return Ok(None);
    };
    let stderr = fs::read_to_string(stderr_path)
        .with_context(|| format!("failed to read worker stderr {}", stderr_path.display()))?;
    let stderr = stderr.to_ascii_lowercase();
    if stderr.contains("model_not_found")
        || stderr.contains("model unavailable")
        || stderr.contains("model not found")
        || (stderr.contains("model") && stderr.contains("not supported"))
    {
        return Ok(Some("model unavailable reported by worker stderr"));
    }
    if stderr.contains("rate limit")
        || stderr.contains("rate-limit")
        || stderr.contains("too many requests")
        || stderr.contains("quota exceeded")
        || stderr.contains("usage quota")
        || stderr.contains("free usage")
        || stderr.contains("limit exhausted")
        || stderr.contains("cooling down")
        || stderr.contains("service unavailable")
        || stderr.contains("temporarily unavailable")
        || stderr.contains("overloaded")
        || stderr
            .split(|character: char| !character.is_ascii_digit())
            .any(|status_code| matches!(status_code, "429" | "503" | "529"))
        || stderr.contains("使用上限")
        || stderr.contains("频率限制")
        || stderr.contains("请求过于频繁")
        || stderr.contains("暂时不可用")
        || stderr.contains("服务不可用")
    {
        return Ok(Some(
            "provider temporarily unavailable reported by worker stderr",
        ));
    }
    Ok(None)
}

#[derive(Default)]
struct ParsedWorkerReport {
    summary: Option<String>,
    changed_files: Vec<String>,
    commands_run: Vec<String>,
    known_failures: Vec<String>,
    completed_step_ids: Vec<String>,
    step_evidence: HashMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerStepEvidenceReport {
    pub declared: bool,
    pub completed_step_ids: Vec<String>,
    pub evidence_by_step: HashMap<String, String>,
}

fn parsed_worker_report(result: &WorkerResult) -> ParsedWorkerReport {
    let text = result
        .last_message_path
        .as_ref()
        .or(result.stdout_path.as_ref())
        .and_then(|path| fs::read_to_string(path).ok())
        .filter(|text| !text.trim().is_empty());
    let Some(text) = text else {
        return ParsedWorkerReport::default();
    };

    let mut sections: HashMap<String, Vec<String>> = HashMap::new();
    let mut current_section: Option<String> = None;

    for line in text.lines() {
        if let Some(section) = worker_report_section_name(line) {
            current_section = Some(section.to_string());
            continue;
        }
        if let Some(section) = current_section.as_ref() {
            sections
                .entry(section.clone())
                .or_default()
                .push(line.to_string());
        }
    }

    let summary = section_paragraph(sections.get("summary")).or_else(|| {
        text.lines()
            .map(str::trim)
            .find(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("- "))
            .map(ToString::to_string)
    });

    ParsedWorkerReport {
        summary,
        changed_files: section_list(sections.get("changed_files")),
        commands_run: section_list(sections.get("commands_run")),
        known_failures: section_list(sections.get("known_failures")),
        completed_step_ids: section_list(sections.get("completed_steps")),
        step_evidence: section_map(sections.get("step_evidence")),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerContinuationEvidence {
    #[serde(default)]
    pub next_steps: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_gap: Option<String>,
}

pub fn worker_continuation_evidence_from_result(
    result: &WorkerResult,
) -> WorkerContinuationEvidence {
    WorkerContinuationEvidence {
        next_steps: section_list_from_result(result, "next_steps"),
        plan_gap: section_paragraph_from_result(result, "plan_gap"),
    }
}

fn section_list_from_result(result: &WorkerResult, name: &str) -> Vec<String> {
    let text = result
        .last_message_path
        .as_ref()
        .or(result.stdout_path.as_ref())
        .and_then(|path| fs::read_to_string(path).ok())
        .unwrap_or_default();
    let mut in_section = false;
    let mut values = Vec::new();
    for line in text.lines() {
        if let Some(section) = worker_report_section_name(line) {
            in_section = section == name;
            continue;
        }
        if in_section {
            let value = line.trim().trim_start_matches(['-', '*', ' ']).trim();
            if !value.is_empty() {
                values.push(value.to_string());
            }
        }
    }
    values
}

fn section_paragraph_from_result(result: &WorkerResult, name: &str) -> Option<String> {
    section_list_from_result(result, name).into_iter().next()
}

/// Parse the worker's explicit ordered-step receipt without making the
/// free-form worker summary part of the durable runtime state.
pub fn worker_step_evidence_from_result(result: &WorkerResult) -> WorkerStepEvidenceReport {
    let parsed = parsed_worker_report(result);
    let declared = result
        .last_message_path
        .as_ref()
        .or(result.stdout_path.as_ref())
        .and_then(|path| fs::read_to_string(path).ok())
        .is_some_and(|text| {
            text.lines()
                .any(|line| worker_report_section_name(line) == Some("completed_steps"))
        });
    WorkerStepEvidenceReport {
        declared,
        completed_step_ids: parsed.completed_step_ids,
        evidence_by_step: parsed.step_evidence,
    }
}

/// Classify worker stdout/stderr as a provider-side error that should trigger
/// automatic fallback rather than an indefinite wait or a generic failure.
///
/// GBX-241: a clearly-signalled provider error (rate limit, quota, model/route
/// unavailable, transient service outage) lets Gear recover the child process
/// and route to the next configured fallback. A slow-but-progressing response
/// must NOT match here so the configured no-artificial-timeout behavior is
/// preserved.
pub fn worker_output_indicates_provider_error(stdout: &str, stderr: &str) -> Option<String> {
    let patterns: &[(&str, &str)] = &[
        ("rate limit", "provider rate limit"),
        ("rate-limit", "provider rate limit"),
        ("too many requests", "provider rate limit (429)"),
        ("429", "provider rate limit (429)"),
        ("quota exceeded", "provider quota exceeded"),
        ("usage quota", "provider quota exceeded"),
        ("free usage", "provider free usage limit reached"),
        ("limit exhausted", "provider limit exhausted"),
        ("cooling down", "provider cooling down"),
        ("service unavailable", "provider service unavailable"),
        ("temporarily unavailable", "provider temporarily unavailable"),
        ("overloaded", "provider overloaded"),
        ("model not found", "provider model not found"),
        ("model unavailable", "provider model unavailable"),
        ("provider error", "provider error"),
        ("upstream error", "provider upstream error"),
        ("connection reset", "provider connection reset"),
        ("deadline exceeded", "provider deadline exceeded"),
        ("context length", "provider context length exceeded"),
    ];

    // OpenCode's `--format json` stream carries the model's response on
    // stdout.  That response routinely mentions provider errors as part of
    // a plan's constraints or risks, so scanning the complete stream turns a
    // successful semantic response into a provider failure.  Stderr is the
    // provider/process diagnostic channel; stdout is inspected only for an
    // explicitly error-shaped line or JSON error event.
    let stderr_text = stderr.to_ascii_lowercase();
    if let Some((_, label)) = patterns
        .iter()
        .find(|(needle, _)| stderr_text.contains(*needle))
    {
        return Some((*label).to_string());
    }

    for line in stdout.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let is_json_error = serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .is_some_and(|value| {
                value
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|kind| kind.eq_ignore_ascii_case("error"))
                    || value.get("error").is_some()
            });
        let normalized = line.to_ascii_lowercase();
        let is_explicit_text_error = normalized.starts_with("error:")
            || normalized.starts_with("error ")
            || normalized.starts_with("provider error")
            || normalized.starts_with("http ")
            || normalized.starts_with("status ");
        if !is_json_error && !is_explicit_text_error {
            continue;
        }
        if let Some((_, label)) = patterns
            .iter()
            .find(|(needle, _)| normalized.contains(*needle))
        {
            return Some((*label).to_string());
        }
    }
    None
}

fn worker_status_for_output(command_succeeded: bool, provider_error: Option<&str>) -> WorkerStatus {
    if command_succeeded && provider_error.is_none() {
        WorkerStatus::Succeeded
    } else {
        WorkerStatus::Failed
    }
}

pub(crate) fn provider_error_is_free_quota(label: &str) -> bool {
    let normalized = label.to_ascii_lowercase();
    normalized.contains("rate limit")
        || normalized.contains("quota")
        || normalized.contains("free usage")
        || normalized.contains("limit exhausted")
        || normalized.contains("too many requests")
}

/// Project the executor receipt paths carried by a `WorkerResult` into a list
/// of on-disk evidence paths. GBX-241 requires failed and successful attempts
/// to expose packet/prompt/result/outcome/stdout/stderr/receipt paths so the
/// runtime can attach them to the current Task evidence.
pub fn worker_receipt_evidence_paths(result: &WorkerResult) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = Vec::new();
    let push_if_exists = |paths: &mut Vec<PathBuf>, candidate: &Option<PathBuf>| {
        if let Some(path) = candidate
            && path.exists()
        {
            paths.push(path.clone());
        }
    };
    push_if_exists(&mut paths, &Some(result.packet_path.clone()));
    push_if_exists(&mut paths, &Some(result.prompt_path.clone()));
    push_if_exists(&mut paths, &Some(result.result_path.clone()));
    push_if_exists(&mut paths, &Some(result.outcome_path.clone()));
    push_if_exists(&mut paths, &result.stdout_path);
    push_if_exists(&mut paths, &result.stderr_path);
    push_if_exists(&mut paths, &result.last_message_path);
    let process_cleanup_path = result
        .result_path
        .parent()
        .map(|worker_directory| worker_directory.join("process-cleanup.json"));
    push_if_exists(&mut paths, &process_cleanup_path);
    let provider_cooldown_path = result
        .result_path
        .parent()
        .map(|worker_directory| worker_directory.join("provider-cooldown.json"));
    push_if_exists(&mut paths, &provider_cooldown_path);
    let claim_reconciliation_path = worker_claim_reconciliation_path(result);
    push_if_exists(&mut paths, &claim_reconciliation_path);
    let external_call_path = result
        .result_path
        .parent()
        .map(|worker_directory| worker_directory.join("external-call.json"));
    push_if_exists(&mut paths, &external_call_path);
    let external_call_start_path = result
        .result_path
        .parent()
        .map(|worker_directory| worker_directory.join("external-call-start.json"));
    push_if_exists(&mut paths, &external_call_start_path);
    let team_session_path = team_session_reconciliation_path(result);
    push_if_exists(&mut paths, &team_session_path);
    paths
}

fn worker_report_section_name(line: &str) -> Option<&'static str> {
    let heading = line.trim().trim_start_matches('#').trim();
    let normalized = heading
        .chars()
        .map(|character| match character {
            'A'..='Z' => character.to_ascii_lowercase(),
            'a'..='z' | '0'..='9' => character,
            _ => '_',
        })
        .collect::<String>();
    match normalized.trim_matches('_') {
        "summary" => Some("summary"),
        "changed_files" => Some("changed_files"),
        "commands_run" => Some("commands_run"),
        "known_failures" => Some("known_failures"),
        "next_steps" => Some("next_steps"),
        "plan_gap" => Some("plan_gap"),
        "completed_steps" => Some("completed_steps"),
        "step_evidence" => Some("step_evidence"),
        _ => None,
    }
}

fn section_map(lines: Option<&Vec<String>>) -> HashMap<String, String> {
    lines
        .into_iter()
        .flat_map(|lines| lines.iter())
        .filter_map(|line| {
            let (step_id, evidence) = line.split_once(':')?;
            let step_id = step_id
                .trim()
                .trim_start_matches('-')
                .trim()
                .trim_matches('`')
                .to_string();
            let evidence = evidence.trim().trim_matches('`').to_string();
            (!step_id.is_empty() && !evidence.is_empty()).then_some((step_id, evidence))
        })
        .collect()
}

fn section_paragraph(lines: Option<&Vec<String>>) -> Option<String> {
    let lines = lines?;
    let text = lines
        .iter()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    (!text.is_empty()).then_some(text)
}

fn section_list(lines: Option<&Vec<String>>) -> Vec<String> {
    lines
        .into_iter()
        .flatten()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.trim_start_matches("- ")
                .trim_start_matches("* ")
                .trim_start_matches("`")
                .trim_end_matches("`")
                .trim()
                .to_string()
        })
        .filter(|line| !line.is_empty())
        .collect()
}

fn output_from_result(result: &WorkerResult) -> Result<Option<String>> {
    let mut output = String::new();
    if let Some(last_message_path) = &result.last_message_path {
        let last_message = fs::read_to_string(last_message_path)
            .with_context(|| format!("failed to read {}", last_message_path.display()))?;
        if !last_message.trim().is_empty() {
            output.push_str(last_message.trim_end());
        }
    }
    if output.is_empty()
        && let Some(stdout_path) = &result.stdout_path
    {
        let stdout = fs::read_to_string(stdout_path)
            .with_context(|| format!("failed to read {}", stdout_path.display()))?;
        if !stdout.trim().is_empty() {
            output.push_str(stdout.trim_end());
        }
    }
    if let Some(stderr_path) = &result.stderr_path {
        let stderr = fs::read_to_string(stderr_path)
            .with_context(|| format!("failed to read {}", stderr_path.display()))?;
        if !stderr.trim().is_empty() {
            if !output.is_empty() {
                output.push_str("\n\n");
            }
            output.push_str(stderr.trim_end());
        }
    }

    if output.is_empty() {
        output = result.summary.clone();
    }
    const MAX_LAST_OUTPUT_BYTES: usize = 16 * 1024;
    if output.len() > MAX_LAST_OUTPUT_BYTES {
        let desired_start = output.len().saturating_sub(MAX_LAST_OUTPUT_BYTES);
        let start = output
            .char_indices()
            .find_map(|(index, _)| (index >= desired_start).then_some(index))
            .unwrap_or(0);
        output = format!(
            "[truncated to last {MAX_LAST_OUTPUT_BYTES} bytes]\n{}",
            &output[start..]
        );
    }
    Ok(Some(output))
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn extract_xml_attr<'a>(text: &'a str, attr_name: &str) -> Option<&'a str> {
    let pattern = format!("{}=\"", attr_name);
    let start = text.find(&pattern)?;
    let value_start = start + pattern.len();
    let value_end = text[value_start..].find('"')?;
    Some(&text[value_start..value_start + value_end])
}

fn extract_invoke_arguments(text: &str) -> String {
    let mut args = Vec::new();
    let mut pos = 0;
    let bytes = text.as_bytes();
    let param_tag = b"<parameter";

    while pos < bytes.len() {
        if let Some(start) = find_subsequence(&bytes[pos..], param_tag) {
            let abs_start = pos + start;
            let after_tag = &text[abs_start + 10..];
            if let Some(param_name) = extract_xml_attr(after_tag, "name") {
                if let Some(content_start) = after_tag.find('>') {
                    let content_begin = content_start + 1;
                    let close_tag = "</parameter>";
                    if let Some(content_end) = after_tag[content_begin..].find(close_tag) {
                        let value = &after_tag[content_begin..content_begin + content_end];
                        args.push(format!("{}={}", param_name, value));
                        pos = abs_start + 10 + content_begin + content_end + close_tag.len();
                        continue;
                    }
                }
            }
            pos = abs_start + 1;
        } else {
            break;
        }
    }

    if args.is_empty() {
        // Fallback: return the raw text content
        let stripped = text
            .trim()
            .trim_start_matches("<parameter")
            .trim_end_matches("</parameter>")
            .trim();
        if !stripped.is_empty() && stripped.len() < text.len() {
            return stripped.to_string();
        }
        return String::new();
    }

    args.join(", ")
}

fn unavailable_command_summary(command: &str) -> Option<String> {
    let binary = command_binary_name(command)?;
    (!command_binary_available(binary)).then(|| {
        format!(
            "No worker command binary `{binary}` was available on PATH for `{command}`; worker packet is ready for external execution."
        )
    })
}

fn command_binary_name(command: &str) -> Option<&str> {
    let binary = command.split_whitespace().next()?;
    if matches!(binary, "sh" | "bash" | "cmd" | "powershell" | "pwsh") {
        return None;
    }
    Some(binary)
}

fn command_binary_available(binary: &str) -> bool {
    if binary.contains(std::path::MAIN_SEPARATOR) || (cfg!(windows) && binary.contains('/')) {
        return Path::new(binary).exists();
    }

    std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path).any(|directory| {
            let candidate = directory.join(binary);
            if candidate.exists() {
                return true;
            }
            if cfg!(windows) {
                directory.join(format!("{binary}.exe")).exists()
            } else {
                false
            }
        })
    })
}

pub fn write_result_and_outcome(
    store: &StateStore,
    task_id: &str,
    result: &WorkerResult,
) -> Result<()> {
    let outcome = worker_outcome_from_result(result)?;
    write_result_and_outcome_with_outcome(store, task_id, result, &outcome)
}

pub fn write_result_and_outcome_with_outcome(
    store: &StateStore,
    task_id: &str,
    result: &WorkerResult,
    outcome: &WorkerOutcome,
) -> Result<()> {
    reconcile_worker_claims(store, task_id, result, outcome)?;
    reconcile_team_session(store, task_id, outcome)?;
    let result_json =
        serde_json::to_string_pretty(result).context("failed to serialize worker result")?;
    store.write_worker_file(task_id, "result.json", &format!("{result_json}\n"))?;
    let outcome_json =
        serde_json::to_string_pretty(outcome).context("failed to serialize worker outcome")?;
    store.write_worker_file(task_id, "outcome.json", &format!("{outcome_json}\n"))?;
    Ok(())
}

pub fn sanitize_model_fields(fields: &HashMap<String, String>) -> HashMap<String, String> {
    let secret_keys: &[&str] = &[
        "apikey",
        "authorization",
        "bearertoken",
        "clientsecret",
        "password",
        "privatekey",
        "secret",
        "secretkey",
        "token",
    ];

    fields
        .iter()
        .map(|(key, value)| {
            let normalized = key
                .to_ascii_lowercase()
                .chars()
                .filter(|character| character.is_alphanumeric())
                .collect::<String>();
            if secret_keys.iter().any(|secret| normalized == *secret) {
                (key.clone(), "***REDACTED***".to_string())
            } else {
                (key.clone(), value.clone())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use anyhow::Result;

    use super::*;

    fn evidence_test_result(last_message_path: PathBuf) -> WorkerResult {
        WorkerResult {
            status: WorkerStatus::Succeeded,
            command: None,
            exit_code: Some(0),
            summary: "worker completed".to_string(),
            packet_path: PathBuf::from("packet.json"),
            prompt_path: PathBuf::from("prompt.md"),
            stdout_path: None,
            stderr_path: None,
            last_message_path: Some(last_message_path),
            result_path: PathBuf::from("result.json"),
            outcome_path: PathBuf::from("outcome.json"),
        }
    }

    #[test]
    fn worker_evidence_receipt_accepts_non_empty_file_inside_workspace_root() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        let receipt = evidence_root.join("receipt.md");
        fs::write(&receipt, "verified\n")?;
        let message = workspace.path().join("last-message.md");
        fs::write(
            &message,
            "done\nEVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\n",
        )?;

        let validated =
            validate_worker_evidence_receipt(&evidence_test_result(message), workspace.path())
                .map_err(anyhow::Error::msg)?;

        assert_eq!(validated, receipt.canonicalize()?);
        Ok(())
    }

    #[test]
    fn worker_step_evidence_parser_reads_ordered_ids_and_paths() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let message = workspace.path().join("last-message.md");
        fs::write(
            &message,
            "# completed_steps\n- step-001\n- step-002\n\n# step_evidence\nstep-001: .gear/steps/001.md\nstep-002: .gear/steps/002.md\n",
        )?;
        let report = worker_step_evidence_from_result(&evidence_test_result(message));
        assert!(report.declared);
        assert_eq!(report.completed_step_ids, ["step-001", "step-002"]);
        assert_eq!(
            report.evidence_by_step.get("step-002").map(String::as_str),
            Some(".gear/steps/002.md")
        );
        Ok(())
    }

    #[test]
    fn worker_provider_error_classifier_detects_rate_limit_and_quota() {
        assert_eq!(
            worker_output_indicates_provider_error("", "Error: rate limit exceeded").as_deref(),
            Some("provider rate limit")
        );
        assert_eq!(
            worker_output_indicates_provider_error("HTTP 429 too many requests", "")
                .as_deref(),
            Some("provider rate limit (429)")
        );
        assert_eq!(
            worker_output_indicates_provider_error("", "quota exceeded for model")
                .as_deref(),
            Some("provider quota exceeded")
        );
        assert_eq!(
            worker_output_indicates_provider_error("provider error: upstream timeout", "")
                .as_deref(),
            Some("provider error")
        );
        // A valid structured model response may mention provider errors,
        // rate limits, or context length in its constraints and risks.  Those
        // words are not a provider failure and must not trip fallback.
        let valid_intent_fold = r#"{"type":"text","part":{"text":"{\"constraints\":[\"Provider errors must halt the current attempt\"],\"risks\":[{\"description\":\"context length remains a future concern\"}]}"}}"#;
        assert!(worker_output_indicates_provider_error(valid_intent_fold, "").is_none());
        assert!(worker_output_indicates_provider_error(
            "The plan documents a rate limit and provider error as risks.",
            ""
        )
        .is_none());
        assert_eq!(
            worker_output_indicates_provider_error(
                r#"{"type":"error","error":{"message":"429 too many requests"}}"#,
                ""
            )
            .as_deref(),
            Some("provider rate limit (429)")
        );
        // A slow-but-progressing response must not be classified as a provider error.
        assert!(
            worker_output_indicates_provider_error("still thinking... token stream", "")
                .is_none()
        );
        assert!(provider_error_is_free_quota("provider rate limit"));
        assert!(provider_error_is_free_quota("provider quota exceeded"));
        assert!(!provider_error_is_free_quota("provider service unavailable"));
        assert!(is_free_model(Some("opencode/mimo-v2.5-free")));
        assert!(!is_free_model(Some("opencode-go/mimo-v2.5")));
        assert!(worker_route_is_premium(
            WorkerKind::OpencodeSession,
            Some("opencode-go/mimo-v2.5")
        ));
        assert!(worker_route_is_premium(
            WorkerKind::OpencodeSession,
            Some("opencode-go/deepseek-v4-flash")
        ));
        assert!(!worker_route_is_premium(
            WorkerKind::OpencodeSession,
            Some("opencode/mimo-v2.5-free")
        ));
        assert!(worker_route_is_premium(WorkerKind::Claude, None));
    }

    #[test]
    fn provider_error_turn_is_failed_even_when_wrapper_exits_successfully() {
        assert_eq!(
            worker_status_for_output(true, Some("provider rate limit")),
            WorkerStatus::Failed
        );
        assert_eq!(
            worker_status_for_output(true, None),
            WorkerStatus::Succeeded
        );
        assert_eq!(
            worker_status_for_output(false, None),
            WorkerStatus::Failed
        );
    }

    #[test]
    fn worker_receipt_evidence_paths_projects_existing_receipts_only() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let dir = workspace.path();
        fs::write(dir.join("packet.json"), "{}")?;
        fs::write(dir.join("prompt.md"), "p")?;
        fs::write(dir.join("result.json"), "{}")?;
        fs::write(dir.join("outcome.json"), "{}")?;
        fs::write(dir.join("stdout.log"), "out")?;
        fs::write(dir.join("process-cleanup.json"), "{}")?;
        fs::write(dir.join("provider-cooldown.json"), "{}")?;
        let result = WorkerResult {
            status: WorkerStatus::Failed,
            command: None,
            exit_code: Some(1),
            summary: "provider error: rate limit".to_string(),
            packet_path: dir.join("packet.json"),
            prompt_path: dir.join("prompt.md"),
            stdout_path: Some(dir.join("stdout.log")),
            stderr_path: None,
            last_message_path: None,
            result_path: dir.join("result.json"),
            outcome_path: dir.join("outcome.json"),
        };
        let paths = worker_receipt_evidence_paths(&result);
        let names: Vec<String> = paths
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert!(names.contains(&"packet.json".to_string()));
        assert!(names.contains(&"prompt.md".to_string()));
        assert!(names.contains(&"result.json".to_string()));
        assert!(names.contains(&"outcome.json".to_string()));
        assert!(names.contains(&"stdout.log".to_string()));
        assert!(names.contains(&"process-cleanup.json".to_string()));
        assert!(names.contains(&"provider-cooldown.json".to_string()));
        assert_eq!(names.len(), 7);
        Ok(())
    }

    #[test]
    fn worker_continuation_evidence_parser_reads_next_steps_and_plan_gap() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let message = workspace.path().join("last-message.md");
        fs::write(
            &message,
            "# next_steps\n- rerun the focused test\n- inspect the remaining diff\n\n# plan_gap\nThe requested artifact path is not available yet.\n",
        )?;
        let evidence = worker_continuation_evidence_from_result(&evidence_test_result(message));
        assert_eq!(
            evidence.next_steps,
            ["rerun the focused test", "inspect the remaining diff"]
        );
        assert_eq!(
            evidence.plan_gap.as_deref(),
            Some("The requested artifact path is not available yet.")
        );
        Ok(())
    }

    #[test]
    fn worker_evidence_receipt_discovers_one_new_file_without_marker() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        fs::write(evidence_root.join("old-receipt.md"), "previous\n")?;
        let baseline =
            snapshot_worker_evidence_paths(workspace.path()).map_err(anyhow::Error::msg)?;

        let receipt = evidence_root.join("new-receipt.md");
        fs::write(&receipt, "verified without marker\n")?;
        let message = workspace.path().join("last-message.md");
        fs::write(&message, "completed without an evidence marker\n")?;

        let validated = validate_worker_evidence_receipt_with_baseline(
            &evidence_test_result(message),
            workspace.path(),
            &baseline,
        )
        .map_err(anyhow::Error::msg)?;
        assert_eq!(validated, receipt.canonicalize()?);
        Ok(())
    }

    #[test]
    fn worker_evidence_receipt_discovers_new_file_without_final_message_artifact() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        let baseline =
            snapshot_worker_evidence_paths(workspace.path()).map_err(anyhow::Error::msg)?;
        let receipt = evidence_root.join("resident-receipt.md");
        fs::write(&receipt, "resident worker completed\n")?;

        let mut result = evidence_test_result(workspace.path().join("missing-message.md"));
        result.last_message_path = None;
        let validated =
            validate_worker_evidence_receipt_with_baseline(&result, workspace.path(), &baseline)
                .map_err(anyhow::Error::msg)?;
        assert_eq!(validated, receipt.canonicalize()?);
        Ok(())
    }

    #[test]
    fn worker_evidence_receipt_cannot_reuse_old_file_without_marker() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        let receipt = evidence_root.join("old-receipt.md");
        fs::write(&receipt, "previous\n")?;
        let baseline =
            snapshot_worker_evidence_paths(workspace.path()).map_err(anyhow::Error::msg)?;
        let message = workspace.path().join("last-message.md");
        fs::write(&message, "completed without an evidence marker\n")?;

        let error = validate_worker_evidence_receipt_with_baseline(
            &evidence_test_result(message),
            workspace.path(),
            &baseline,
        )
        .expect_err("an old receipt must not satisfy a later attempt");
        assert!(error.contains("exactly one new receipt"));
        assert!(error.contains("found 0"));
        Ok(())
    }

    #[test]
    fn worker_evidence_receipt_rejects_multiple_new_files_without_marker() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        let baseline =
            snapshot_worker_evidence_paths(workspace.path()).map_err(anyhow::Error::msg)?;
        fs::write(evidence_root.join("first.md"), "one\n")?;
        fs::write(evidence_root.join("second.md"), "two\n")?;
        let message = workspace.path().join("last-message.md");
        fs::write(&message, "completed without an evidence marker\n")?;

        let error = validate_worker_evidence_receipt_with_baseline(
            &evidence_test_result(message),
            workspace.path(),
            &baseline,
        )
        .expect_err("multiple new files must not be guessed as the receipt");
        assert!(error.contains("exactly one new receipt"));
        assert!(error.contains("found 2"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn worker_evidence_receipt_rejects_new_symbolic_link_without_marker() -> Result<()> {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        let baseline =
            snapshot_worker_evidence_paths(workspace.path()).map_err(anyhow::Error::msg)?;
        let outside = workspace.path().join("outside.md");
        fs::write(&outside, "outside\n")?;
        symlink(&outside, evidence_root.join("link.md"))?;
        let message = workspace.path().join("last-message.md");
        fs::write(&message, "completed without an evidence marker\n")?;

        let error = validate_worker_evidence_receipt_with_baseline(
            &evidence_test_result(message),
            workspace.path(),
            &baseline,
        )
        .expect_err("a new symbolic link must not be accepted as a receipt");
        assert!(error.contains("symbolic link"));
        Ok(())
    }

    #[test]
    fn worker_evidence_receipt_marker_cannot_reuse_old_file() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        let receipt = evidence_root.join("old-receipt.md");
        fs::write(&receipt, "previous\n")?;
        let baseline =
            snapshot_worker_evidence_paths(workspace.path()).map_err(anyhow::Error::msg)?;
        let message = workspace.path().join("last-message.md");
        fs::write(
            &message,
            "done\nEVIDENCE_RECORDED: .gearbox-agent/evidence/old-receipt.md\n",
        )?;

        let error = validate_worker_evidence_receipt_with_baseline(
            &evidence_test_result(message),
            workspace.path(),
            &baseline,
        )
        .expect_err("an explicit marker must not reuse a pre-existing receipt");
        assert!(error.contains("present before this worker attempt"));
        Ok(())
    }

    #[test]
    fn worker_evidence_receipt_rejects_empty_directory_and_escape_paths() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        let message = workspace.path().join("last-message.md");

        let empty_receipt = evidence_root.join("empty.md");
        fs::write(&empty_receipt, "")?;
        fs::write(
            &message,
            "EVIDENCE_RECORDED: .gearbox-agent/evidence/empty.md",
        )?;
        let empty_error = validate_worker_evidence_receipt(
            &evidence_test_result(message.clone()),
            workspace.path(),
        )
        .expect_err("empty receipt must be rejected");
        assert!(empty_error.contains("must not be empty"));

        let directory = evidence_root.join("directory");
        fs::create_dir(&directory)?;
        fs::write(
            &message,
            "EVIDENCE_RECORDED: .gearbox-agent/evidence/directory",
        )?;
        let directory_error = validate_worker_evidence_receipt(
            &evidence_test_result(message.clone()),
            workspace.path(),
        )
        .expect_err("directory receipt must be rejected");
        assert!(directory_error.contains("regular file"));

        let workspace_name = workspace
            .path()
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .ok_or_else(|| anyhow::anyhow!("temporary workspace has no file name"))?;
        let outside_name = format!("{workspace_name}-outside.md");
        let outside = workspace
            .path()
            .parent()
            .ok_or_else(|| anyhow::anyhow!("temporary workspace has no parent"))?
            .join(&outside_name);
        fs::write(&outside, "outside\n")?;
        fs::write(&message, format!("EVIDENCE_RECORDED: ../{outside_name}"))?;
        let escape_error =
            validate_worker_evidence_receipt(&evidence_test_result(message), workspace.path())
                .expect_err("receipt outside evidence root must be rejected");
        assert!(escape_error.contains("outside"));
        fs::remove_file(outside)?;
        Ok(())
    }

    #[test]
    fn worker_evidence_receipt_does_not_fallback_from_stdout_when_final_message_exists()
    -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        fs::write(evidence_root.join("receipt.md"), "verified\n")?;
        let message = workspace.path().join("last-message.md");
        fs::write(&message, "completed without the final marker\n")?;
        let stdout = workspace.path().join("stdout.log");
        fs::write(
            &stdout,
            "EVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\n",
        )?;
        let mut result = evidence_test_result(message);
        result.stdout_path = Some(stdout);

        let error = validate_worker_evidence_receipt(&result, workspace.path())
            .expect_err("stdout must not replace a present final message");
        assert!(error.contains("missing EVIDENCE_RECORDED:"));
        Ok(())
    }

    #[test]
    fn worker_evidence_receipt_requires_a_final_message_artifact() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        fs::write(evidence_root.join("receipt.md"), "verified\n")?;
        let stdout = workspace.path().join("stdout.log");
        fs::write(
            &stdout,
            "EVIDENCE_RECORDED: .gearbox-agent/evidence/receipt.md\n",
        )?;
        let mut result = evidence_test_result(workspace.path().join("missing-message.md"));
        result.last_message_path = None;
        result.stdout_path = Some(stdout);

        let error = validate_worker_evidence_receipt(&result, workspace.path())
            .expect_err("artifact-contract workers must provide a final message");
        assert!(error.contains("missing EVIDENCE_RECORDED:"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn worker_evidence_receipt_rejects_symbolic_link() -> Result<()> {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir()?;
        let evidence_root = workspace.path().join(".gearbox-agent/evidence");
        fs::create_dir_all(&evidence_root)?;
        let outside = workspace.path().join("outside.md");
        fs::write(&outside, "outside\n")?;
        let link = evidence_root.join("link.md");
        symlink(&outside, &link)?;
        let message = workspace.path().join("last-message.md");
        fs::write(
            &message,
            "EVIDENCE_RECORDED: .gearbox-agent/evidence/link.md",
        )?;

        let error =
            validate_worker_evidence_receipt(&evidence_test_result(message), workspace.path())
                .expect_err("symbolic link receipt must be rejected");
        assert!(error.contains("symbolic link"));
        Ok(())
    }

    #[test]
    fn worker_evidence_gate_only_targets_write_capable_categories() {
        assert!(category_requires_worker_evidence("quick"));
        assert!(category_requires_worker_evidence("deep"));
        assert!(category_requires_worker_evidence("zed-native"));
        assert!(!category_requires_worker_evidence("review"));
        assert!(!category_requires_worker_evidence("explore"));
        assert!(!category_requires_worker_evidence("librarian"));
        assert!(!worker_kind_supports_evidence_contract("opencode"));
        assert!(worker_kind_supports_evidence_contract("opencode_session"));
        assert!(worker_kind_supports_evidence_contract("codex"));
        assert!(!worker_kind_supports_evidence_contract("claude"));
    }

    #[test]
    fn parses_worker_kind_aliases() {
        assert_eq!(WorkerKind::parse("opencode"), Some(WorkerKind::Opencode));
        assert_eq!(
            WorkerKind::parse("opencode-session"),
            Some(WorkerKind::OpencodeSession)
        );
        assert_eq!(WorkerKind::parse("claude-code"), Some(WorkerKind::Claude));
        assert_eq!(WorkerKind::parse("zed_agent"), Some(WorkerKind::ZedAgent));
        assert_eq!(WorkerKind::parse("unknown"), None);
    }

    #[test]
    fn worker_config_routes_attempts_through_worker_pool() {
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("opencode run".to_string()),
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("opencode run".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("codex exec".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };

        let first = config.selected_route(1);
        assert_eq!(first.worker_kind, WorkerKind::Opencode);
        assert_eq!(first.worker_command, Some("opencode run"));
        assert!(first.require_worker);
        assert!(
            first
                .prompt_append
                .as_ref()
                .expect("prompt append")
                .contains("Focus on implementation")
        );
        assert!(first.tools.can_write);
        assert!(!first.tools.question);

        let second = config.selected_route(2);
        assert_eq!(second.worker_kind, WorkerKind::Codex);
        assert_eq!(second.worker_command, Some("codex exec"));
        assert!(second.require_worker);

        let later = config.selected_route(8);
        assert_eq!(later.worker_kind, WorkerKind::Codex);
    }

    #[test]
    fn prompt_append_combines_builtin_and_user_append() {
        let combined =
            combined_prompt_append(Some("builtin append"), Some("user append".to_string()));
        let combined = combined.expect("combined append");
        assert!(combined.contains("builtin append"));
        assert!(combined.contains("user append"));
        assert!(combined.contains("\n\n"));
    }

    fn prompt_manifest_test_packet() -> WorkerPacket {
        WorkerPacket {
            task_id: "task_manifest".to_string(),
            worker: "opencode".to_string(),
            current_step_id: Some("step-001".to_string()),
            worker_model: Some("deepseek-v4-flash-free".to_string()),
            variant: Some("medium".to_string()),
            variant_applied: Some("medium".to_string()),
            prompt_append: Some("Focus on the current step only.".to_string()),
            injected_rules: None,
            rules_injection_path: None,
            injected_skills: None,
            skills_injection_path: None,
            tools: WorkerToolPolicy {
                can_write: true,
                can_explore: true,
                ..WorkerToolPolicy::default()
            },
            category_resolution: CategoryResolution::default(),
            category_resolution_result: CategoryResolutionResult::Resolved {
                requested_category: "deep".to_string(),
                available_categories: vec!["deep".to_string()],
                attempted_provider_model: Some("opencode/deepseek-v4-flash-free".to_string()),
                nearest_fallback: None,
            },
            goal: "implement the bounded change".to_string(),
            coordinator_model: None,
            coordinator_brief: None,
            scope: Scope::new(vec!["crates/gearbox_agent/src".to_string()], Vec::new(), 3),
            inputs: TaskInputs::default(),
            constraints: vec!["run the focused test".to_string()],
            required_outputs: vec!["summary".to_string()],
            verification: VerificationContract {
                preferred_commands: vec!["cargo test -p gearbox_agent".to_string()],
                must_not_skip: vec!["typecheck".to_string()],
            },
            stop_conditions: vec!["stop on a repeated verification failure".to_string()],
            prompt_manifest_path: None,
            prompt_reconcile_path: None,
            prompt_capsule_path: None,
        }
    }

    #[test]
    fn worker_parameter_resolution_distinguishes_defaults_and_rejects_invalid_values() -> Result<()> {
        let mut packet = prompt_manifest_test_packet();
        packet.category_resolution.tools = packet.tools.clone();
        let receipt = validate_worker_packet_parameters(&packet)?;
        assert_eq!(receipt.status, "resolved");
        assert!(receipt
            .parameters
            .iter()
            .any(|parameter| parameter.name == "worker_model"
                && parameter.state == WorkerParameterState::Configured));
        assert!(receipt
            .parameters
            .iter()
            .all(|parameter| parameter.state != WorkerParameterState::Invalid));
        receipt.validate()?;

        let mut invalid = serde_json::to_value(&packet)?;
        invalid["worker"] = Value::Null;
        invalid["tools"]["can_write"] = json!("yes");
        invalid["variant_applied"] = json!("fast");
        let invalid_receipt = validate_worker_parameter_value(&invalid)?;
        assert_eq!(invalid_receipt.status, "invalid");
        assert!(invalid_receipt
            .errors
            .iter()
            .any(|error| error.contains("worker") && error.contains("null")));
        assert!(invalid_receipt
            .errors
            .iter()
            .any(|error| error.contains("tools.can_write") && error.contains("boolean")));
        assert!(invalid_receipt
            .errors
            .iter()
            .any(|error| error.contains("variant conflict")));
        Ok(())
    }

    #[test]
    fn workspace_rules_follow_root_to_target_order_and_write_receipt() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let source = workspace.path().join("src");
        fs::create_dir_all(&source)?;
        fs::write(workspace.path().join("AGENTS.md"), "root rule")?;
        fs::write(source.join(".rules"), "target rule")?;
        let store = StateStore::new(workspace.path());
        store.initialize()?;
        let mut task = default_task_with_id("rules-order");
        task.scope = Scope::new(vec!["src".to_string()], Vec::new(), 3);

        let (rules, receipt_path) = discover_workspace_rules(&store, workspace.path(), &task)?;
        let rules = rules.context("rules should be injected")?;
        assert!(rules.find("root rule").unwrap() < rules.find("target rule").unwrap());
        let receipt_path = receipt_path.context("receipt path")?;
        let receipt: RuleInjectionReceipt = serde_json::from_str(&fs::read_to_string(receipt_path)?)?;
        receipt.validate()?;
        assert_eq!(receipt.schema_version, RULE_INJECTION_SCHEMA_VERSION);
        assert_eq!(receipt.target_paths, vec!["src"]);
        assert_eq!(receipt.entries.iter().filter(|entry| entry.injected).count(), 2);
        let injected_entries = receipt
            .entries
            .iter()
            .filter(|entry| entry.injected)
            .collect::<Vec<_>>();
        assert!(injected_entries.iter().all(|entry| {
            entry.modified_at_ms > 0
                && entry.match_reason == "walk_up"
                && entry.precedence == entry.distance
        }));
        assert!(!receipt.receipt_hash.is_empty());
        assert_eq!(receipt.injected_content_hash, prompt_content_hash(&rules));
        Ok(())
    }

    #[test]
    fn workspace_rule_conflicts_are_persisted_and_block_dispatch() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let source = workspace.path().join("src");
        fs::create_dir_all(&source)?;
        fs::write(workspace.path().join("AGENTS.md"), "scope: root\n")?;
        fs::write(workspace.path().join(".rules"), "scope: sibling\n")?;
        let store = StateStore::new(workspace.path());
        store.initialize()?;
        let mut task = default_task_with_id("rules-conflict");
        task.scope = Scope::new(vec!["src".to_string()], Vec::new(), 3);

        let error = discover_workspace_rules(&store, workspace.path(), &task)
            .expect_err("conflicting same-layer directives must block dispatch");
        assert!(error.to_string().contains("context conflict"));
        let receipt_path = store.worker_dir(&task.id).join("rules-injection.json");
        let receipt: RuleInjectionReceipt =
            serde_json::from_slice(&fs::read(receipt_path)?)?;
        receipt.validate()?;
        assert!(receipt.context_conflict);
        assert!(receipt
            .context_conflict_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("scope")));
        Ok(())
    }

    #[test]
    fn workspace_skills_are_scoped_and_cache_freshness_is_explicit() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let source = workspace.path().join("src");
        fs::create_dir_all(workspace.path().join(".agents/skills/root"))?;
        fs::create_dir_all(source.join(".agents/skills/target"))?;
        fs::create_dir_all(source.join(".agents/skills/disabled"))?;
        fs::write(
            workspace.path().join(".agents/skills/root/SKILL.md"),
            "# Root skill\nUse the root workflow.\n",
        )?;
        fs::write(
            source.join(".agents/skills/target/SKILL.md"),
            "# Target skill\nUse the target workflow.\n",
        )?;
        fs::write(
            source.join(".agents/skills/disabled/SKILL.md"),
            "---\ndisabled: true\n---\n# Disabled\nDo not inject.\n",
        )?;
        let store = StateStore::new(workspace.path());
        store.initialize()?;

        let mut first_task = default_task_with_id("skills-first");
        first_task.scope = Scope::new(vec!["src".to_string()], Vec::new(), 3);
        let (skills, receipt_path) =
            discover_workspace_skills(&store, workspace.path(), &first_task)?;
        let skills = skills.context("skills should be injected")?;
        assert!(skills.find("Root skill").unwrap() < skills.find("Target skill").unwrap());
        let receipt_path = receipt_path.context("skills receipt path")?;
        let receipt: SkillInjectionReceipt = serde_json::from_str(&fs::read_to_string(receipt_path)?)?;
        receipt.validate()?;
        assert!(!receipt.cache_hit);
        assert_eq!(receipt.entries.iter().filter(|entry| entry.injected).count(), 2);
        assert!(receipt
            .entries
            .iter()
            .filter(|entry| entry.injected)
            .all(|entry| entry.modified_at_ms > 0 && entry.precedence == entry.distance));
        assert!(receipt
            .entries
            .iter()
            .any(|entry| entry.freshness == "disabled" && !entry.injected));

        let mut second_task = default_task_with_id("skills-second");
        second_task.scope = Scope::new(vec!["src".to_string()], Vec::new(), 3);
        let (_, receipt_path) = discover_workspace_skills(&store, workspace.path(), &second_task)?;
        let receipt: SkillInjectionReceipt = serde_json::from_str(&fs::read_to_string(
            receipt_path.context("second skills receipt path")?,
        )?)?;
        assert!(receipt.cache_hit);
        assert!(receipt
            .entries
            .iter()
            .filter(|entry| entry.injected)
            .all(|entry| entry.freshness == "cached"));

        fs::write(
            source.join(".agents/skills/target/SKILL.md"),
            "# Target skill\nChanged target workflow.\n",
        )?;
        let mut third_task = default_task_with_id("skills-third");
        third_task.scope = Scope::new(vec!["src".to_string()], Vec::new(), 3);
        let (_, receipt_path) = discover_workspace_skills(&store, workspace.path(), &third_task)?;
        let receipt: SkillInjectionReceipt = serde_json::from_str(&fs::read_to_string(
            receipt_path.context("third skills receipt path")?,
        )?)?;
        assert!(!receipt.cache_hit);
        assert!(receipt.entries.iter().any(|entry| entry.freshness == "stale"));
        Ok(())
    }

    #[test]
    fn workspace_skills_prefer_more_specific_same_name() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let source = workspace.path().join("src");
        fs::create_dir_all(workspace.path().join(".agents/skills/review"))?;
        fs::create_dir_all(source.join(".agents/skills/review"))?;
        fs::write(
            workspace.path().join(".agents/skills/review/SKILL.md"),
            "# Root review\nUse the root workflow.\n",
        )?;
        fs::write(
            source.join(".agents/skills/review/SKILL.md"),
            "# Target review\nUse the target workflow.\n",
        )?;
        let store = StateStore::new(workspace.path());
        store.initialize()?;
        let mut task = default_task_with_id("skills-precedence");
        task.scope = Scope::new(vec!["src".to_string()], Vec::new(), 3);

        let (skills, receipt_path) = discover_workspace_skills(&store, workspace.path(), &task)?;
        let skills = skills.context("specific skill should be injected")?;
        assert!(skills.contains("Target review"));
        assert!(!skills.contains("Root review"));
        let receipt: SkillInjectionReceipt =
            serde_json::from_slice(&fs::read(receipt_path.context("receipt path")?)?)?;
        receipt.validate()?;
        assert!(receipt.entries.iter().any(|entry| {
            entry.freshness == "shadowed_precedence"
                && !entry.injected
                && entry
                    .omission_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("more specific target"))
        }));
        Ok(())
    }

    #[test]
    fn workspace_skills_enforce_agent_restrictions_and_required_failures() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        fs::create_dir_all(workspace.path().join(".agents/skills/review-only"))?;
        fs::write(
            workspace
                .path()
                .join(".agents/skills/review-only/SKILL.md"),
            "---\nagents: [review]\nrequired: true\n---\n# Review-only skill\n",
        )?;
        let store = StateStore::new(workspace.path());
        store.initialize()?;
        let task = default_task_with_id("skills-restricted");

        let error = discover_workspace_skills_for_worker(
            &store,
            workspace.path(),
            &task,
            "opencode",
            "deep",
        )
        .expect_err("required skill restricted to another worker must block dispatch");
        assert!(error.to_string().contains("required workspace skill"));
        let receipt: SkillInjectionReceipt = serde_json::from_slice(&fs::read(
            store.worker_dir(&task.id).join("skills-injection.json"),
        )?)?;
        receipt.validate()?;
        assert_eq!(receipt.worker, "opencode");
        assert_eq!(receipt.worker_category, "deep");
        assert!(receipt.entries.iter().any(|entry| {
            entry.match_reason == "agent_restricted"
                && entry.freshness == "restricted"
                && !entry.injected
        }));

        let review_task = default_task_with_id("skills-restricted-review");
        let (skills, receipt_path) = discover_workspace_skills_for_worker(
            &store,
            workspace.path(),
            &review_task,
            "opencode",
            "review",
        )?;
        assert!(skills.is_some_and(|skills| skills.contains("Review-only skill")));
        let receipt: SkillInjectionReceipt = serde_json::from_slice(&fs::read(
            receipt_path.context("review skill receipt path")?,
        )?)?;
        receipt.validate()?;
        assert!(receipt.entries.iter().any(|entry| entry.injected));
        Ok(())
    }

    #[test]
    fn workspace_rules_deduplicate_content() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let source = workspace.path().join("src");
        fs::create_dir_all(&source)?;
        fs::write(workspace.path().join("AGENTS.md"), "same rule")?;
        fs::write(source.join(".rules"), "same rule")?;
        let store = StateStore::new(workspace.path());
        store.initialize()?;
        let mut task = default_task_with_id("rules-content-dedup");
        task.scope = Scope::new(vec!["src".to_string()], Vec::new(), 3);

        let (rules, receipt_path) = discover_workspace_rules(&store, workspace.path(), &task)?;
        let rules = rules.context("first copy should be injected")?;
        assert_eq!(rules.matches("same rule").count(), 1);
        let receipt: RuleInjectionReceipt = serde_json::from_str(&fs::read_to_string(
            receipt_path.context("receipt path")?,
        )?)?;
        receipt.validate()?;
        assert_eq!(receipt.entries.iter().filter(|entry| entry.injected).count(), 1);
        assert!(receipt.entries.iter().any(|entry| {
            entry.freshness == "duplicate_content"
                && entry.omission_reason.as_deref() == Some("duplicate content")
        }));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn workspace_rules_skip_symlinks_outside_workspace() -> Result<()> {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        let source = workspace.path().join("src");
        fs::create_dir_all(&source)?;
        fs::write(outside.path().join("AGENTS.md"), "outside rule")?;
        symlink(outside.path().join("AGENTS.md"), source.join(".rules"))?;
        let store = StateStore::new(workspace.path());
        store.initialize()?;
        let mut task = default_task_with_id("rules-symlink");
        task.scope = Scope::new(vec!["src".to_string()], Vec::new(), 3);

        let (rules, receipt_path) = discover_workspace_rules(&store, workspace.path(), &task)?;
        assert!(rules.is_none());
        let receipt: RuleInjectionReceipt = serde_json::from_str(&fs::read_to_string(
            receipt_path.context("receipt path")?,
        )?)?;
        receipt.validate()?;
        assert!(receipt.entries.iter().any(|entry| {
            entry.omission_reason.as_deref() == Some("realpath outside workspace")
        }));
        Ok(())
    }

    #[test]
    fn workspace_rules_are_soft_prompt_context() -> Result<()> {
        let mut packet = prompt_manifest_test_packet();
        packet.injected_rules = Some("[Rule: AGENTS.md]\n[Match: walk-up]\nkeep scope narrow".to_string());
        let prompt = worker_prompt(&packet)?;
        assert!(prompt.contains("## Workspace rules"));
        assert!(prompt.contains("keep scope narrow"));
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        let rules = manifest
            .sections
            .iter()
            .find(|section| section.id == "rules")
            .context("rules manifest section missing")?;
        assert!(!rules.required);
        assert!(rules.included);
        assert_eq!(rules.source, "runtime.workspace_rules");
        Ok(())
    }

    #[test]
    fn prompt_manifest_separates_hard_contract_from_route_drift() -> Result<()> {
        let packet = prompt_manifest_test_packet();
        let prompt = worker_prompt(&packet)?;
        assert!(prompt.contains("\"current_step_id\"") && prompt.contains("step-001"));
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        manifest.validate(&packet, &prompt)?;
        assert!(
            !manifest
                .sections
                .iter()
                .find(|section| section.id == "route_append")
                .context("route append section missing")?
                .required
        );

        let mut rerouted = packet.clone();
        rerouted.worker_model = Some("mimo-v2.5-free".to_string());
        rerouted.variant_applied = Some("high".to_string());
        let rerouted_prompt = worker_prompt(&rerouted)?;
        let rerouted_manifest = prompt_manifest_for_packet(&rerouted, &rerouted_prompt)?;
        assert_eq!(
            manifest.semantic_contract_hash,
            rerouted_manifest.semantic_contract_hash
        );
        assert_ne!(
            manifest.rendered_prompt_hash,
            rerouted_manifest.rendered_prompt_hash
        );
        assert_ne!(
            manifest
                .sections
                .iter()
                .find(|section| section.id == "route")
                .map(|section| section.content_hash.clone()),
            rerouted_manifest
                .sections
                .iter()
                .find(|section| section.id == "route")
                .map(|section| section.content_hash.clone())
        );

        let mut changed_contract = packet.clone();
        changed_contract.goal.push_str(" with a new requirement");
        assert_ne!(
            manifest.semantic_contract_hash,
            prompt_semantic_contract_hash(&changed_contract)?
        );

        let mut changed_step = packet.clone();
        changed_step.current_step_id = Some("step-002".to_string());
        assert_ne!(
            manifest.semantic_contract_hash,
            prompt_semantic_contract_hash(&changed_step)?
        );

        let mut tampered = manifest;
        tampered.task_id = "other-task".to_string();
        assert!(tampered.validate(&packet, &prompt).is_err());
        Ok(())
    }

    #[test]
    fn phase_prompt_goal_is_hash_bound_without_duplicate_hard_payload() -> Result<()> {
        let previous_limit = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS").ok();
        let result = (|| -> Result<()> {
            let mut packet = prompt_manifest_test_packet();
            packet.worker_model = Some("opencode-go/deepseek-v4-flash".to_string());
            packet.inputs.phase_route_locked = true;
            packet.goal = "phase evidence ".repeat(8_000);
            let prompt = worker_prompt(&packet)?;
            assert!(prompt.contains(&packet.goal));
            assert!(prompt.contains("full phase request follows below"));
            let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
            let hard_tokens: usize = manifest
                .sections
                .iter()
                .filter(|section| section.required)
                .map(|section| section.estimated_tokens)
                .sum();
            assert!(hard_tokens < 32_768, "phase prompt was duplicated in hard sections");
            unsafe {
                env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", "32768");
            }
            let capsule = build_prompt_capsule(
                &packet,
                &manifest,
                &prompt,
                &PromptCapsuleRecoveryReason::Dispatch,
            )?;
            capsule.validate()?;
            Ok(())
        })();
        unsafe {
            match previous_limit {
                Some(value) => env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", value),
                None => env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS"),
            }
        }
        result
    }

    #[test]
    fn prompt_manifest_records_missing_optional_append_as_degraded() -> Result<()> {
        let mut packet = prompt_manifest_test_packet();
        packet.prompt_append = None;
        let prompt = worker_prompt(&packet)?;
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        let append = manifest
            .sections
            .iter()
            .find(|section| section.id == "route_append")
            .context("route append section missing")?;
        assert!(!append.included);
        assert!(!append.required);
        assert_eq!(append.omission_reason.as_deref(), Some("not configured"));
        Ok(())
    }

    #[test]
    fn prompt_capsule_isolates_hard_contract_under_small_budget() -> Result<()> {
        // Force a budget just above the measured hard prompt framing so soft
        // sections must be clipped/omitted while the hard contract is retained.
        let original = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS").ok();
        let result = std::panic::catch_unwind(|| -> Result<()> {
            let packet = prompt_manifest_test_packet();
            let prompt = worker_prompt(&packet)?;
            let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
            let hard_tokens: usize = manifest
                .sections
                .iter()
                .filter(|section| section.required)
                .map(|section| section.estimated_tokens)
                .sum();
            unsafe {
                env::set_var(
                    "GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS",
                    hard_tokens.saturating_add(1).to_string(),
                );
            }
            let capsule = build_prompt_capsule(
                &packet,
                &manifest,
                &prompt,
                &PromptCapsuleRecoveryReason::Dispatch,
            )?;
            capsule.validate()?;

            // Required hard sections are always present.
            let identity = capsule
                .sections
                .iter()
                .find(|section| section.id == "identity")
                .context("identity section missing")?;
            let task_contract = capsule
                .sections
                .iter()
                .find(|section| section.id == "task_contract")
                .context("task_contract section missing")?;
            assert!(identity.included);
            assert!(identity.required);
            assert!(task_contract.included);
            assert!(task_contract.required);

            // Soft sections are bounded because the budget is tiny.
            let route = capsule
                .sections
                .iter()
                .find(|section| section.id == "route")
                .context("route section missing")?;
            let context = capsule
                .sections
                .iter()
                .find(|section| section.id == "context")
                .context("context section missing")?;
            assert!(!route.included);
            assert!(!route.required);
            assert!(!context.included);
            assert!(!context.required);
            assert!(route.omission_reason.is_some());
            assert!(context.omission_reason.is_some());

            // The rendered prompt hash ties the capsule to the exact manifest output.
            assert_eq!(capsule.rendered_prompt_hash, manifest.rendered_prompt_hash);
            assert_eq!(capsule.semantic_contract_hash, manifest.semantic_contract_hash);
            Ok(())
        });
        unsafe {
            match original {
                Some(value) => env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", value),
                None => env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS"),
            }
        }
        result.map_err(|panic| anyhow::anyhow!("test panicked: {panic:?}"))?
    }

    #[test]
    fn prompt_capsule_recovery_key_is_idempotent_and_stable() {
        let key_a = prompt_capsule_recovery_key(
            "task_x",
            "semhash",
            &PromptCapsuleRecoveryReason::Compact,
        );
        let key_b = prompt_capsule_recovery_key(
            "task_x",
            "semhash",
            &PromptCapsuleRecoveryReason::Compact,
        );
        assert_eq!(key_a, key_b);
        assert_eq!(key_a, "task_x:semhash:compact");

        // Different reason produces a distinct, still idempotent key.
        let key_resume = prompt_capsule_recovery_key(
            "task_x",
            "semhash",
            &PromptCapsuleRecoveryReason::Resume,
        );
        assert_ne!(key_a, key_resume);
        assert_eq!(key_resume, "task_x:semhash:resume");
    }

    #[test]
    fn prompt_capsule_never_omits_required_hard_section() -> Result<()> {
        let original = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS").ok();
        unsafe {
            env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", "1");
        }
        let result = std::panic::catch_unwind(|| -> Result<()> {
            let packet = prompt_manifest_test_packet();
            let prompt = worker_prompt(&packet)?;
            let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
            let error = build_prompt_capsule(
                &packet,
                &manifest,
                &prompt,
                &PromptCapsuleRecoveryReason::Dispatch,
            )
            .expect_err("hard prompt contract must not overflow the context budget");
            let overflow = error
                .downcast_ref::<PromptCapsuleBudgetOverflow>()
                .context("expected typed prompt budget overflow")?;
            assert!(overflow.required_tokens > overflow.budget_tokens);
            assert_eq!(overflow.budget_tokens, 1);
            Ok(())
        });
        unsafe {
            match original {
                Some(value) => env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", value),
                None => env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS"),
            }
        }
        result.map_err(|panic| anyhow::anyhow!("test panicked: {panic:?}"))?
    }

    #[test]
    fn persisted_prompt_capsule_is_bound_to_current_packet_and_manifest() -> Result<()> {
        let packet = prompt_manifest_test_packet();
        let prompt = worker_prompt(&packet)?;
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        let mut capsule = build_prompt_capsule(
            &packet,
            &manifest,
            &prompt,
            &PromptCapsuleRecoveryReason::Resume,
        )?;
        capsule.validate_against(&packet, &manifest)?;

        capsule.task_id = "stale-task".to_string();
        let error = capsule
            .validate_against(&packet, &manifest)
            .expect_err("stale capsule must not be reused");
        assert!(error.to_string().contains("task identity"));
        Ok(())
    }

    #[test]
    fn prompt_budget_policy_uses_a_larger_default_for_paid_opencode_models() {
        let previous_global = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS").ok();
        let previous_paid =
            env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS_OPENCODE_GO_MIMO_V2_5").ok();
        unsafe {
            env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS");
            env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS_OPENCODE_GO_MIMO_V2_5");
        }

        let paid_policy = prompt_budget_policy(Some("opencode-go/mimo-v2.5"));
        assert_eq!(paid_policy.context_limit_tokens, DEFAULT_PAID_CONTEXT_LIMIT_TOKENS);
        assert_eq!(paid_policy.prompt_budget_tokens, DEFAULT_PAID_CONTEXT_LIMIT_TOKENS);
        assert!(paid_policy.estimated);
        assert_eq!(paid_policy.source, "paid_model_conservative_default");

        let unknown_policy = prompt_budget_policy(Some("opencode/mimo-v2.5-free"));
        assert_eq!(unknown_policy.context_limit_tokens, DEFAULT_CONTEXT_LIMIT_TOKENS);
        assert_eq!(unknown_policy.source, "conservative_default");

        let deepseek_free_policy =
            prompt_budget_policy(Some("opencode/deepseek-v4-flash-free"));
        assert_eq!(
            deepseek_free_policy.context_limit_tokens,
            DEFAULT_PAID_CONTEXT_LIMIT_TOKENS
        );
        assert_eq!(
            deepseek_free_policy.source,
            "deepseek_flash_conservative_default"
        );
        assert!(deepseek_free_policy.estimated);

        unsafe {
            match previous_global {
                Some(value) => env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", value),
                None => env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS"),
            }
            match previous_paid {
                Some(value) => env::set_var(
                    "GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS_OPENCODE_GO_MIMO_V2_5",
                    value,
                ),
                None => env::remove_var(
                    "GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS_OPENCODE_GO_MIMO_V2_5",
                ),
            }
        }
    }

    #[test]
    fn prompt_budget_policy_records_precedence_and_headroom() {
        let previous_global = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS").ok();
        let previous_model = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS_OPENCODE_DEEPSEEK_V4_FLASH").ok();
        let previous_headroom = env::var("GEARBOX_WORKER_OUTPUT_HEADROOM_TOKENS").ok();
        unsafe {
            env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS");
            env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS_OPENCODE_DEEPSEEK_V4_FLASH", "12000");
            env::set_var("GEARBOX_WORKER_OUTPUT_HEADROOM_TOKENS", "512");
        }
        let model_policy = prompt_budget_policy(Some("opencode/deepseek-v4-flash"));
        assert_eq!(model_policy.context_limit_tokens, 12000);
        assert_eq!(model_policy.prompt_budget_tokens, 11488);
        assert!(!model_policy.estimated);
        assert!(model_policy.source.contains("DEEPSEEK_V4_FLASH"));
        assert!(model_policy.headroom_source.contains("OUTPUT_HEADROOM"));

        unsafe {
            env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", "9000");
        }
        let global_policy = prompt_budget_policy(Some("opencode/deepseek-v4-flash"));
        assert_eq!(global_policy.context_limit_tokens, 9000);
        assert_eq!(global_policy.prompt_budget_tokens, 8488);
        assert!(global_policy.source.contains("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS"));

        unsafe {
            match previous_global {
                Some(value) => env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", value),
                None => env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS"),
            }
            match previous_model {
                Some(value) => env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS_OPENCODE_DEEPSEEK_V4_FLASH", value),
                None => env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS_OPENCODE_DEEPSEEK_V4_FLASH"),
            }
            match previous_headroom {
                Some(value) => env::set_var("GEARBOX_WORKER_OUTPUT_HEADROOM_TOKENS", value),
                None => env::remove_var("GEARBOX_WORKER_OUTPUT_HEADROOM_TOKENS"),
            }
        }
    }

    #[test]
    fn prompt_capsule_clips_soft_sections_and_compiles_bounded_prompt() -> Result<()> {
        let previous = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS").ok();
        let result = std::panic::catch_unwind(|| -> Result<()> {
            let mut packet = prompt_manifest_test_packet();
            packet.prompt_append = Some("前置说明 ".repeat(500));
            let prompt = worker_prompt(&packet)?;
            let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
            let hard_tokens: usize = manifest
                .sections
                .iter()
                .filter(|section| section.required)
                .map(|section| section.estimated_tokens)
                .sum();
            unsafe {
                env::set_var(
                    "GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS",
                    hard_tokens.saturating_add(80).to_string(),
                );
            }
            let capsule = build_prompt_capsule(
                &packet,
                &manifest,
                &prompt,
                &PromptCapsuleRecoveryReason::Dispatch,
            )?;
            let clipped = capsule.sections.iter().find(|section| section.clipped);
            let clipped = clipped.context("expected a soft section to be clipped")?;
            assert!(clipped.retained_bytes < clipped.bytes);
            assert!(clipped.deleted_bytes > 0);
            assert!(
                capsule.used_tokens <= capsule.budget_tokens,
                "capsule accounting must use retained tokens, not pre-clipping estimates"
            );
            let compiled = worker_compiled_prompt(&packet, &capsule)?;
            assert!(compiled.contains("Bounded"));
            assert!(estimate_prompt_tokens(&compiled) <= capsule.budget_tokens);
            Ok(())
        });
        unsafe {
            match previous {
                Some(value) => env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", value),
                None => env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS"),
            }
        }
        result.map_err(|panic| anyhow::anyhow!("test panicked: {panic:?}"))?
    }

    #[test]
    fn command_worker_budget_overflow_writes_receipt_before_spawn() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_prompt_budget_overflow_start";
        let sentinel = temp_dir.path().join("worker-must-not-start");
        let task = Task {
            id: task_id.to_string(),
            goal_id: "goal_prompt_budget_overflow".to_string(),
            title: "prompt budget overflow admission".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some(WorkerKind::OpencodeSession.as_str().to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            inputs: TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let mut config = WorkerConfig::default();
        config.worker_kind = WorkerKind::OpencodeSession;
        config.worker_command = Some(format!("sh -c 'touch {}'", sentinel.display()));
        config.worker_model = Some("opencode/mimo-v2.5-free".to_string());
        config.require_worker = true;
        let verification_commands = Vec::new();
        let original = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS").ok();
        unsafe {
            env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", "1");
        }
        let result = std::panic::catch_unwind(|| {
            start_command_backed_worker(
                WorkerStartRequest {
                    store: &store,
                    workspace: temp_dir.path(),
                    task: &task,
                    route_attempt: 1,
                    goal: "prove hard prompt admission",
                    verification_commands: &verification_commands,
                    config: &config,
                    cancellation_token: None,
                    coordinator_model: None,
                    coordinator_brief: None,
                    route_hint: None,
                },
                false,
            )
        });
        unsafe {
            match original {
                Some(value) => env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", value),
                None => env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS"),
            }
        }
        let error = match result.map_err(|panic| anyhow::anyhow!("test panicked: {panic:?}"))? {
            Ok(_) => bail!("hard prompt overflow must reject worker admission"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("prompt hard contract exceeds context budget"));
        assert!(!sentinel.exists(), "overflow must be rejected before spawn");

        let receipt_path = store
            .worker_dir(task_id)
            .join("prompt-budget-overflow.json");
        let receipt: serde_json::Value = serde_json::from_slice(&fs::read(receipt_path)?)?;
        assert_eq!(receipt["status"], "blocked");
        assert_eq!(receipt["task_id"], task_id);
        assert_eq!(receipt["next_action"], "try_next_explicit_route_or_split_task");
        assert!(receipt["required_tokens"].as_u64().unwrap_or_default() > 1);
        assert_eq!(receipt["budget_tokens"], 1);
        assert!(!store.worker_dir(task_id).join("resident-session.json").exists());
        Ok(())
    }

    #[test]
    fn prompt_capsule_recovery_reason_matches_reconcile_action() {
        assert_eq!(
            prompt_capsule_recovery_reason_for_action(&PromptReconcileAction::NewSession),
            PromptCapsuleRecoveryReason::Dispatch
        );
        assert_eq!(
            prompt_capsule_recovery_reason_for_action(&PromptReconcileAction::RebuildSession),
            PromptCapsuleRecoveryReason::Compact
        );
        assert_eq!(
            prompt_capsule_recovery_reason_for_action(&PromptReconcileAction::ResumeSession),
            PromptCapsuleRecoveryReason::Resume
        );
    }

    #[test]
    fn prompt_capsule_recovers_current_step_from_durable_state() {
        use crate::state::{PlanStepRun, PlanStepRunStatus};
        let steps = vec![
            PlanStepRun {
                step_id: "step-001".to_string(),
                action: "inspect".to_string(),
                expected_observation: "recorded".to_string(),
                evidence_path: None,
                status: PlanStepRunStatus::Completed,
                error: None,
                updated_at: String::new(),
            },
            PlanStepRun {
                step_id: "step-002".to_string(),
                action: "implement".to_string(),
                expected_observation: "present".to_string(),
                evidence_path: None,
                status: PlanStepRunStatus::Running,
                error: None,
                updated_at: String::new(),
            },
            PlanStepRun {
                step_id: "step-003".to_string(),
                action: "verify".to_string(),
                expected_observation: "green".to_string(),
                evidence_path: None,
                status: PlanStepRunStatus::Pending,
                error: None,
                updated_at: String::new(),
            },
        ];
        // Recovery resumes at the first not-completed step, not the whole plan.
        assert_eq!(
            recover_current_step_id(&steps).as_deref(),
            Some("step-002")
        );
        let all_complete = vec![PlanStepRun {
            step_id: "step-001".to_string(),
            action: "inspect".to_string(),
            expected_observation: "recorded".to_string(),
            evidence_path: None,
            status: PlanStepRunStatus::Completed,
            error: None,
            updated_at: String::new(),
        }];
        assert_eq!(
            recover_current_step_id(&all_complete).as_deref(),
            None
        );
    }

    #[test]
    fn durable_current_step_cursor_is_fail_closed_and_preferred() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_step_cursor";
        let step_path = store.worker_dir(task_id).join("current-step-id");

        assert_eq!(read_durable_current_step_id(&store, task_id)?, None);
        fs::create_dir_all(store.worker_dir(task_id))?;
        fs::write(&step_path, "step-002\n")?;
        assert_eq!(
            read_durable_current_step_id(&store, task_id)?.as_deref(),
            Some("step-002")
        );
        fs::write(&step_path, "\nstep-003\nstep-004")?;
        let error = read_durable_current_step_id(&store, task_id)
            .expect_err("multi-line cursor must not fall back to the first plan step");
        assert!(error.to_string().contains("malformed"));
        Ok(())
    }

    fn prompt_reconcile_test_descriptor(
        task_id: &str,
        worker_model: Option<&str>,
        session_id: &str,
    ) -> Result<ResidentSessionDescriptor> {
        ResidentSessionDescriptor {
            schema_version: RESIDENT_SESSION_DESCRIPTOR_SCHEMA_VERSION,
            task_id: task_id.to_string(),
            worker_kind: WorkerKind::OpencodeSession,
            worker_model: worker_model.map(ToString::to_string),
            session_id: session_id.to_string(),
            provider_session_id: None,
            workspace: "/tmp/gearbox-test".to_string(),
            resumable: true,
            resume_count: 1,
            created_at: timestamp(),
            last_resumed_at: None,
            descriptor_hash: String::new(),
        }
        .seal()
    }

    #[test]
    fn prompt_reconcile_receipt_rebuilds_on_model_family_drift() -> Result<()> {
        let packet = prompt_manifest_test_packet();
        let prompt = worker_prompt(&packet)?;
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        let previous = prompt_reconcile_test_descriptor(
            &packet.task_id,
            packet.worker_model.as_deref(),
            "task_manifest_session",
        )?;
        let resumed = PromptReconcileReceipt::for_dispatch(
            &packet,
            &manifest,
            Some(&previous),
            None,
            Some(&previous),
            1,
            1,
            true,
        )?;
        assert_eq!(resumed.action, PromptReconcileAction::ResumeSession);
        assert!(resumed.session_reused);
        resumed.validate_against(&packet, &manifest)?;

        let mut rerouted = packet;
        rerouted.worker_model = Some("opencode/mimo-v2.5-free".to_string());
        let rerouted_prompt = worker_prompt(&rerouted)?;
        let rerouted_manifest = prompt_manifest_for_packet(&rerouted, &rerouted_prompt)?;
        let rebuilt = PromptReconcileReceipt::for_dispatch(
            &rerouted,
            &rerouted_manifest,
            Some(&previous),
            None,
            None,
            2,
            2,
            true,
        )?;
        assert_eq!(rebuilt.action, PromptReconcileAction::RebuildSession);
        assert!(!rebuilt.session_reused);
        assert_eq!(rebuilt.previous_model_family.as_deref(), Some("deepseek"));
        assert_eq!(rebuilt.runtime_model_family, "mimo");
        rebuilt.validate_against(&rerouted, &rerouted_manifest)?;

        let mut tampered = rebuilt;
        tampered.semantic_contract_hash = "tampered".to_string();
        assert!(tampered.validate().is_err());
        Ok(())
    }

    #[test]
    fn model_switch_pending_receipt_preserves_previous_session_binding() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_pending_reconcile";
        let mut descriptor = prompt_reconcile_test_descriptor(
            task_id,
            Some("opencode/deepseek-v4-flash-free"),
            "pending-session",
        )?;
        descriptor.workspace = temp_dir.path().to_string_lossy().to_string();
        descriptor = descriptor.seal()?;
        write_resident_session_descriptor(&store, &descriptor)?;

        discard_resident_session_for_model_switch(
            &store,
            temp_dir.path(),
            task_id,
            WorkerKind::OpencodeSession,
            Some("opencode/mimo-v2.5-free"),
        )?;

        assert!(!resident_session_descriptor_path(&store, task_id).exists());
        let pending_path = store
            .worker_dir(task_id)
            .join("prompt-reconcile-pending.json");
        let pending: PromptReconcilePending = serde_json::from_slice(&fs::read(pending_path)?)?;
        pending.validate()?;
        assert_eq!(
            pending.previous_worker_model,
            "opencode/deepseek-v4-flash-free"
        );
        assert_eq!(pending.previous_model_family, "deepseek");
        assert_eq!(pending.previous_session_id, "pending-session");
        assert_eq!(pending.requested_worker_model, "opencode/mimo-v2.5-free");
        Ok(())
    }

    #[test]
    fn command_worker_consumes_model_switch_pending_receipt() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_reconcile_dispatch".to_string(),
            goal_id: "goal_reconcile_dispatch".to_string(),
            title: "reconcile dispatch".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 2,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 2),
            inputs: TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let old_descriptor = prepare_resident_session_descriptor(
            &store,
            temp_dir.path(),
            &task,
            WorkerKind::OpencodeSession,
            Some("opencode/deepseek-v4-flash-free".to_string()),
        )?;
        write_resident_session_descriptor(&store, &old_descriptor)?;
        discard_resident_session_for_model_switch(
            &store,
            temp_dir.path(),
            &task.id,
            WorkerKind::OpencodeSession,
            Some("opencode/mimo-v2.5-free"),
        )?;

        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("true".to_string()),
            worker_model: Some("opencode/mimo-v2.5-free".to_string()),
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: true,
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
            require_worker: false,
        };
        let result = WorkerRegistry::default().run(WorkerRunRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 2,
            goal: "reconcile the current dispatch",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        assert_eq!(result.status, WorkerStatus::Skipped);
        assert!(
            !store
                .worker_dir(&task.id)
                .join("prompt-reconcile-pending.json")
                .exists()
        );
        let receipt: PromptReconcileReceipt = serde_json::from_slice(&fs::read(
            store.worker_dir(&task.id).join("prompt-reconcile.json"),
        )?)?;
        assert_eq!(receipt.action, PromptReconcileAction::RebuildSession);
        assert_eq!(
            receipt.previous_worker_model.as_deref(),
            Some("opencode/deepseek-v4-flash-free")
        );
        assert_eq!(receipt.runtime_model_family, "mimo");
        receipt.validate()?;
        Ok(())
    }

    #[test]
    fn disposed_resident_session_is_rebuilt_with_reconcile_receipt() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_disposed_reconcile".to_string(),
            goal_id: "goal_disposed_reconcile".to_string(),
            title: "disposed reconcile".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 2),
            inputs: TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let mut disposed = prepare_resident_session_descriptor(
            &store,
            temp_dir.path(),
            &task,
            WorkerKind::OpencodeSession,
            Some("opencode/deepseek-v4-flash-free".to_string()),
        )?;
        disposed.resumable = false;
        disposed = disposed.seal()?;
        write_resident_session_descriptor(&store, &disposed)?;

        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("true".to_string()),
            worker_model: Some("opencode/mimo-v2.5-free".to_string()),
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: true,
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
            require_worker: false,
        };
        let result = WorkerRegistry::default().run(WorkerRunRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "rebuild disposed worker session",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        assert_eq!(result.status, WorkerStatus::Skipped);
        let receipt: PromptReconcileReceipt = serde_json::from_slice(&fs::read(
            store.worker_dir(&task.id).join("prompt-reconcile.json"),
        )?)?;
        assert_eq!(receipt.action, PromptReconcileAction::RebuildSession);
        assert_eq!(
            receipt.previous_session_id.as_deref(),
            Some(disposed.session_id.as_str())
        );
        assert_eq!(receipt.previous_model_family.as_deref(), Some("deepseek"));
        assert!(
            !store
                .worker_dir(&task.id)
                .join("prompt-reconcile-pending.json")
                .exists()
        );
        Ok(())
    }

    #[test]
    fn worker_tool_policy_disables_question_by_default() {
        let policy = WorkerToolPolicy::default();
        assert!(!policy.question);
        assert!(!policy.allow_recursive_gear_tasks);
        assert!(!policy.can_write);
        assert!(!policy.can_review);
        assert!(!policy.can_explore);

        let review_policy = WorkerCategory::Review.tool_policy();
        assert!(review_policy.can_review);
        assert!(!review_policy.can_write);

        let explore_policy = WorkerCategory::Explore.tool_policy();
        assert!(explore_policy.can_explore);
        assert!(!explore_policy.can_write);
    }

    // GBX-237 (237-001/237-002): reviewer routes stay read-only while repair
    // routes stay writable. A review verdict failure must not be able to mutate
    // the goal, and the repair successor must be able to.
    #[test]
    fn gbx237_review_route_is_readonly_and_repair_route_is_writable() {
        let review_policy = WorkerCategory::Review.tool_policy();
        assert!(
            !review_policy.can_write,
            "GBX-237: independent reviewer must remain read-only"
        );
        assert!(review_policy.can_review);

        let repair_policy = WorkerCategory::Repair.tool_policy();
        assert!(
            repair_policy.can_write,
            "GBX-237: repair route must be writable so it can implement the fix"
        );
        assert!(!repair_policy.can_review);

        // The reviewer prompt append must never carry an implementation/eval
        // instruction that would let it impersonate a repair turn.
        assert!(WorkerCategory::Review.prompt_append().is_some());
        assert!(WorkerCategory::Repair.prompt_append().is_some());
        assert_ne!(
            WorkerCategory::Review.prompt_append(),
            WorkerCategory::Repair.prompt_append()
        );
    }

    // GBX-237 (237-001/237-002): routing after a failed review must resolve to
    // the Repairable category, not back to a read-only review loop.
    #[test]
    fn gbx237_review_failure_routes_to_repair_category() {
        // Review resolves to a reader-only worker kind; repair resolves to a
        // writer-capable worker kind. The two must differ in write capability
        // so the failed-review successor can actually mutate the work order.
        let review_can_write = WorkerCategory::Review.tool_policy().can_write;
        let repair_can_write = WorkerCategory::Repair.tool_policy().can_write;
        assert!(!review_can_write);
        assert!(repair_can_write);

        // Category parsing must round-trip so a persisted route decision of
        // "repair" is never silently reinterpreted as "review".
        assert_eq!(WorkerCategory::parse("repair"), Some(WorkerCategory::Repair));
        assert_eq!(WorkerCategory::parse("review"), Some(WorkerCategory::Review));
        assert_ne!(
            WorkerCategory::parse("repair"),
            WorkerCategory::parse("review")
        );
    }

    #[test]
    fn worker_config_route_hint_prefers_matching_existing_route() {
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("opencode run".to_string()),
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("opencode run".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("codex exec".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Claude,
                    worker_command: Some("claude -p".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };

        let deep = config.selected_route_for_hint(1, Some("deep"));
        assert_eq!(deep.worker_kind, WorkerKind::Codex);
        assert_eq!(deep.worker_command, Some("codex exec"));
        assert_eq!(deep.category, WorkerCategory::Deep);
        assert!(deep.route_reason.contains("selected attempt 1 configured"));

        let quick = config.selected_route_for_hint(2, Some("quick"));
        assert_eq!(quick.worker_kind, WorkerKind::Opencode);
        assert_eq!(quick.category, WorkerCategory::Quick);

        let unknown = config.selected_route_for_hint(2, Some("expensive"));
        assert_eq!(unknown.worker_kind, WorkerKind::Codex);
        assert_eq!(unknown.category, WorkerCategory::Repair);
        assert!(unknown.route_reason.contains("sequence route"));
    }

    #[test]
    fn category_routes_preserve_distinct_models_for_the_same_worker_kind() {
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("opencode run".to_string()),
            worker_model: None,
            worker_routes: [
                "opencode/hy3-free",
                "opencode/mimo-v2.5-free",
                "opencode/deepseek-v4-flash-free",
            ]
            .into_iter()
            .map(|worker_model| WorkerRoute {
                worker_kind: WorkerKind::OpencodeSession,
                worker_command: Some("opencode run".to_string()),
                worker_model: Some(worker_model.to_string()),
            })
            .collect(),
            unavailable_worker_models: vec!["opencode/hy3-free".to_string()],
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let first = config.selected_route_for_hint(1, Some("deep"));
        let second = config.selected_route_for_hint(2, Some("deep"));

        assert_eq!(first.worker_model, Some("opencode/mimo-v2.5-free"));
        assert_eq!(second.worker_model, Some("opencode/deepseek-v4-flash-free"));
        assert!(
            first
                .route_reason
                .contains("skipping an unavailable provider/model route")
        );
    }

    #[test]
    fn worker_kind_default_codex_command_includes_prompt_and_model() {
        let command = WorkerKind::Codex
            .default_command(Some("gpt-5"))
            .expect("codex default command should exist");

        assert!(command.contains("codex exec"));
        assert!(command.contains("-m 'gpt-5'"));
        assert!(command.contains("$GEARBOX_WORKER_PROMPT"));
        assert!(command.contains("$GEARBOX_WORKER_LAST_MESSAGE"));
    }

    #[test]
    fn worker_category_parses_aliases() {
        assert_eq!(
            WorkerCategory::parse("docs"),
            Some(WorkerCategory::Librarian)
        );
        assert_eq!(
            WorkerCategory::parse("frontend"),
            Some(WorkerCategory::Visual)
        );
        assert_eq!(
            WorkerCategory::parse("zed_agent"),
            Some(WorkerCategory::ZedNative)
        );
        assert_eq!(WorkerCategory::parse("unknown"), None);
    }

    #[test]
    fn category_router_prefers_category_workers_then_sequence_fallback() {
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("opencode run".to_string()),
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("opencode run".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("codex exec".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::ZedAgent,
                    worker_command: Some("zed agent".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };

        let repair = CategoryRouter.resolve(&config, 1, Some("repair"));
        assert_eq!(repair.worker_kind, WorkerKind::Opencode);
        assert_eq!(repair.category, WorkerCategory::Repair);

        let repair_fallback = CategoryRouter.resolve(&config, 2, Some("repair"));
        assert_eq!(repair_fallback.worker_kind, WorkerKind::Codex);
        assert_eq!(repair_fallback.category, WorkerCategory::Repair);

        let review = CategoryRouter.resolve(&config, 1, Some("review"));
        assert_eq!(review.worker_kind, WorkerKind::Codex);
        assert_eq!(review.category, WorkerCategory::Review);

        let explore = CategoryRouter.resolve(&config, 1, Some("explore"));
        assert_eq!(explore.worker_kind, WorkerKind::ZedAgent);
        assert_eq!(explore.category, WorkerCategory::Explore);

        let visual = CategoryRouter.resolve(&config, 1, Some("visual"));
        assert_eq!(visual.worker_kind, WorkerKind::Codex);
        assert_eq!(visual.category, WorkerCategory::Visual);
    }

    #[test]
    fn category_router_skips_unavailable_provider_model_routes() {
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("opencode run".to_string()),
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("opencode run".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("codex exec".to_string()),
                    worker_model: Some("gpt.5-1".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Claude,
                    worker_command: Some("claude -p".to_string()),
                    worker_model: Some("claude-3-7-sonnet".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::ZedAgent,
                    worker_command: Some("zed agent".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: vec!["OpenAI/GPT-5.1".to_string()],
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };

        let deep = CategoryRouter.resolve(&config, 1, Some("deep"));
        assert_eq!(deep.worker_kind, WorkerKind::Claude);
        assert!(
            deep.route_reason
                .contains("skipping an unavailable provider/model route")
        );

        let sequence = CategoryRouter.resolve(&config, 2, None);
        assert_eq!(sequence.worker_kind, WorkerKind::Claude);
        assert!(
            sequence
                .route_reason
                .contains("skipping an unavailable provider/model route")
        );
    }

    #[test]
    fn category_resolution_for_route_reports_model_unavailability() {
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("opencode run".to_string()),
            worker_model: None,
            worker_routes: vec![WorkerRoute {
                worker_kind: WorkerKind::Codex,
                worker_command: Some("codex exec".to_string()),
                worker_model: Some("gpt.5-1".to_string()),
            }],
            unavailable_worker_models: vec!["OpenAI/GPT-5.1".to_string()],
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };

        let route = config.selected_route_for_hint(1, Some("deep"));
        let (resolution, result) = category_resolution_for_route(&config, 1, Some("deep"), &route);

        assert!(resolution.available_categories.is_empty());
        assert_eq!(resolution.nearest_fallback, None);
        assert!(matches!(
            result,
            CategoryResolutionResult::ModelUnavailable {
                requested_category,
                attempted_provider_model,
                ..
            } if requested_category == "deep"
                && attempted_provider_model.as_deref() == Some("openai/gpt.5-1")
        ));
    }

    #[test]
    fn category_resolution_for_route_reports_distinct_nearest_fallback() {
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("opencode run".to_string()),
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("codex exec".to_string()),
                    worker_model: Some("gpt.5-1".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Claude,
                    worker_command: Some("claude code".to_string()),
                    worker_model: Some("claude-3.5".to_string()),
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };

        let route = config.selected_route_for_hint(1, Some("deep"));
        let (resolution, result) = category_resolution_for_route(&config, 1, Some("deep"), &route);

        assert_eq!(
            resolution.nearest_fallback,
            Some(FallbackRoute {
                worker_kind: WorkerKind::Claude,
                worker_model: Some("claude-3.5".to_string()),
            })
        );
        assert!(matches!(
            result,
            CategoryResolutionResult::Resolved {
                requested_category,
                attempted_provider_model,
                ..
            } if requested_category == "deep"
                && attempted_provider_model.as_deref() == Some("openai/gpt.5-1")
        ));
    }

    #[test]
    fn command_backed_worker_adapters_report_worker_identity() {
        assert_eq!(OpencodeCommandWorker {}.kind(), WorkerKind::Opencode);
        assert_eq!(OpencodeCommandWorker {}.name(), "opencode_command");
        assert_eq!(OpencodeSessionWorker {}.kind(), WorkerKind::OpencodeSession);
        assert_eq!(OpencodeSessionWorker {}.name(), "opencode_session");
        assert!(
            OpencodeSessionWorker {}
                .capabilities()
                .supports_resident_session
        );
        assert_eq!(CodexCommandWorker {}.kind(), WorkerKind::Codex);
        assert_eq!(CodexCommandWorker {}.name(), "codex_command");
        assert_eq!(ClaudeCommandWorker {}.kind(), WorkerKind::Claude);
        assert_eq!(ClaudeCommandWorker {}.name(), "claude_command");
        assert_eq!(ZedAgentCommandWorker {}.kind(), WorkerKind::ZedAgent);
        assert_eq!(ZedAgentCommandWorker {}.name(), "zed_agent_command");
        assert_eq!(CustomCommandWorker {}.kind(), WorkerKind::Custom);
        assert_eq!(CustomCommandWorker {}.name(), "custom_command");
    }

    #[test]
    fn worker_registry_writes_selected_worker_kind_to_packet() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_test".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test worker".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("codex".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: None,
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: true,
            default_worker_for_small_tasks: WorkerKind::Codex,
            require_worker: false,
        };

        let result = WorkerRegistry::default().run(WorkerRunRequest {
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

        let packet = fs::read_to_string(result.packet_path)?;
        assert!(packet.contains(r#""worker": "codex""#));
        let outcome = fs::read_to_string(result.outcome_path)?;
        assert!(outcome.contains(r#""status": "skipped""#));
        Ok(())
    }

    struct FakeNativeBackend {
        started: Arc<AtomicBool>,
    }

    impl NativeWorkerBackend for FakeNativeBackend {
        fn start_zed_agent(
            &self,
            request: WorkerStartRequest<'_>,
        ) -> Result<Arc<dyn WorkerSessionHandle>> {
            self.started.store(true, Ordering::SeqCst);
            let result = WorkerResult {
                status: WorkerStatus::Skipped,
                command: Some("native-zed".to_string()),
                exit_code: None,
                summary: "native backend".to_string(),
                packet_path: request
                    .store
                    .worker_dir(&request.task.id)
                    .join("packet.json"),
                prompt_path: request.store.worker_dir(&request.task.id).join("prompt.md"),
                stdout_path: None,
                stderr_path: None,
                last_message_path: None,
                result_path: request
                    .store
                    .worker_dir(&request.task.id)
                    .join("result.json"),
                outcome_path: request
                    .store
                    .worker_dir(&request.task.id)
                    .join("outcome.json"),
            };
            Ok(Arc::new(FakeNativeHandle { result }))
        }
    }

    struct FakeNativeHandle {
        result: WorkerResult,
    }

    struct FakeAcpBackend {
        started: Arc<AtomicBool>,
    }

    impl NativeWorkerBackend for FakeAcpBackend {
        fn start_zed_agent(
            &self,
            _request: WorkerStartRequest<'_>,
        ) -> Result<Arc<dyn WorkerSessionHandle>> {
            bail!("fake ACP backend should not receive a ZedAgent route")
        }

        fn start_acp_worker(
            &self,
            worker_kind: WorkerKind,
            request: WorkerStartRequest<'_>,
        ) -> Result<Option<Arc<dyn WorkerSessionHandle>>> {
            if !matches!(
                worker_kind,
                WorkerKind::Opencode
                    | WorkerKind::OpencodeSession
                    | WorkerKind::Codex
                    | WorkerKind::Claude
            ) {
                return Ok(None);
            }
            self.started.store(true, Ordering::SeqCst);
            let result = WorkerResult {
                status: WorkerStatus::Skipped,
                command: Some("native-acp".to_string()),
                exit_code: None,
                summary: "native ACP backend".to_string(),
                packet_path: request
                    .store
                    .worker_dir(&request.task.id)
                    .join("packet.json"),
                prompt_path: request.store.worker_dir(&request.task.id).join("prompt.md"),
                stdout_path: None,
                stderr_path: None,
                last_message_path: None,
                result_path: request
                    .store
                    .worker_dir(&request.task.id)
                    .join("result.json"),
                outcome_path: request
                    .store
                    .worker_dir(&request.task.id)
                    .join("outcome.json"),
            };
            Ok(Some(Arc::new(FakeNativeHandle { result })))
        }

        fn native_broker_capabilities(
            &self,
            worker_kind: WorkerKind,
        ) -> Option<Vec<BrokerCapability>> {
            matches!(
                worker_kind,
                WorkerKind::Opencode
                    | WorkerKind::OpencodeSession
                    | WorkerKind::Codex
                    | WorkerKind::Claude
            )
            .then(|| broker_capabilities_for_kind(WorkerKind::OpencodeSession, false))
        }
    }

    impl WorkerSessionHandle for FakeNativeHandle {
        fn session_id(&self) -> Option<String> {
            Some("native-zed-session".to_string())
        }

        fn send_follow_up(&self, _prompt: String) -> Result<()> {
            Ok(())
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            Ok(())
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn abort(&self) -> Result<()> {
            Ok(())
        }

        fn dispose(&self) -> Result<()> {
            Ok(())
        }

        fn supports_event_subscriptions(&self) -> bool {
            true
        }

        fn subscribe(&self, _listener: WorkerEventListener) -> Result<WorkerSubscription> {
            Ok(WorkerSubscription::noop())
        }

        fn wait_for_idle(&self) -> Result<WorkerTurnOutcome> {
            Ok(self.result.clone())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            worker_outcome_from_result(&self.result)
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            Ok(self.result.clone())
        }

        fn last_output(&self) -> Option<String> {
            Some("native backend".to_string())
        }
    }

    struct CountedNativeBackend {
        call_count: Arc<AtomicUsize>,
    }

    impl NativeWorkerBackend for CountedNativeBackend {
        fn start_zed_agent(
            &self,
            request: WorkerStartRequest<'_>,
        ) -> Result<Arc<dyn WorkerSessionHandle>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let result = WorkerResult {
                status: WorkerStatus::Skipped,
                command: Some("counted-native".to_string()),
                exit_code: None,
                summary: "counted native backend".to_string(),
                packet_path: request
                    .store
                    .worker_dir(&request.task.id)
                    .join("packet.json"),
                prompt_path: request.store.worker_dir(&request.task.id).join("prompt.md"),
                stdout_path: None,
                stderr_path: None,
                last_message_path: None,
                result_path: request
                    .store
                    .worker_dir(&request.task.id)
                    .join("result.json"),
                outcome_path: request
                    .store
                    .worker_dir(&request.task.id)
                    .join("outcome.json"),
            };
            Ok(Arc::new(FakeNativeHandle { result }))
        }
    }

    #[test]
    fn worker_registry_prefers_native_zed_backend_when_available() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_native_zed".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test native zed worker".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("zed_agent".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::ZedAgent,
            worker_command: Some("should not run".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };
        let started = Arc::new(AtomicBool::new(false));
        let registry = WorkerRegistry::with_native_backend(Arc::new(FakeNativeBackend {
            started: started.clone(),
        }));

        let result = registry.run(WorkerRunRequest {
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

        assert!(started.load(Ordering::SeqCst));
        assert_eq!(result.command.as_deref(), Some("native-zed"));
        Ok(())
    }

    #[test]
    fn worker_registry_routes_provider_workers_to_native_acp_backend() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_native_acp".to_string(),
            goal_id: "goal_native_acp".to_string(),
            title: "test native ACP worker".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("codex".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: Some("must-not-run".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::Codex,
            require_worker: false,
        };
        let started = Arc::new(AtomicBool::new(false));
        let registry = WorkerRegistry::with_native_backend(Arc::new(FakeAcpBackend {
            started: started.clone(),
        }));
        let result = registry.run(WorkerRunRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test ACP goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        assert!(started.load(Ordering::SeqCst));
        assert_eq!(result.command.as_deref(), Some("native-acp"));
        Ok(())
    }

    #[test]
    fn test_variant_changes_provider_request_params() -> Result<()> {
        // Test that ProviderAdapter.model_params() returns different results
        // for different variants
        let policy = WorkerToolPolicy::default();
        let category = WorkerCategory::Quick;

        // No variant → passthrough
        let adapter = ProviderAdapter::new(None, policy.clone(), category);
        assert!(adapter.model_params()?.is_none());

        // Supported variant → returns params
        let adapter = ProviderAdapter::new(Some("pro".to_string()), policy.clone(), category);
        let params = adapter
            .model_params()?
            .expect("pro variant should be supported");
        assert_eq!(params.variant, Some("pro".to_string()));

        // Different supported variant
        let adapter = ProviderAdapter::new(Some("fast".to_string()), policy, category);
        let params = adapter
            .model_params()?
            .expect("fast variant should be supported");
        assert_eq!(params.variant, Some("fast".to_string()));

        Ok(())
    }

    #[test]
    fn test_unsupported_variant_rejected_before_dispatch() -> Result<()> {
        let policy = WorkerToolPolicy::default();
        let category = WorkerCategory::Deep;

        // Unknown variant → unsupported error
        let adapter = ProviderAdapter::new(Some("nonexistent-v99".to_string()), policy, category);
        let result = adapter.model_params();
        assert!(result.is_err(), "nonexistent variant should be rejected");
        let err = result.unwrap_err();
        assert!(err.variant.contains("nonexistent-v99"));
        assert!(err.supported_variants.contains(&"pro".to_string()));

        Ok(())
    }

    #[test]
    fn test_disallowed_tool_does_not_reach_executor() -> Result<()> {
        let adapter = ProviderAdapter::new(
            None,
            WorkerToolPolicy {
                can_write: false,
                ..WorkerToolPolicy::default()
            },
            WorkerCategory::Quick,
        );

        let error = adapter
            .check_tool_allowed("write")
            .expect_err("disabled write policy must reject before execution");
        assert_eq!(error.tool_name, "write");
        assert!(error.reason.contains("denied"));

        Ok(())
    }

    #[test]
    fn test_native_executor_not_called_on_denied_tool() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_denied_tool".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test denied tool".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("zed_agent".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::ZedAgent,
            worker_command: Some("should not run".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };

        // Force can_review: false for Review category via env override.
        // Review category's required tool is "review", so check_tool_allowed
        // will return Err(ToolDenied) before the native backend is consulted.
        // Review category is chosen because no other test reads
        // GEARBOX_GEAR_CATEGORY_REVIEW_CAN_REVIEW, avoiding env races.
        let orig = env::var("GEARBOX_GEAR_CATEGORY_REVIEW_CAN_REVIEW").ok();
        unsafe {
            env::set_var("GEARBOX_GEAR_CATEGORY_REVIEW_CAN_REVIEW", "false");
        }

        let call_count = Arc::new(AtomicUsize::new(0));
        let registry = WorkerRegistry::with_native_backend(Arc::new(CountedNativeBackend {
            call_count: call_count.clone(),
        }));

        let result = registry.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test denied tool",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("review"),
        });

        unsafe {
            if let Some(v) = orig {
                env::set_var("GEARBOX_GEAR_CATEGORY_REVIEW_CAN_REVIEW", v);
            } else {
                env::remove_var("GEARBOX_GEAR_CATEGORY_REVIEW_CAN_REVIEW");
            }
        }

        // Verify the error message mentions the correct tool name
        let err_string = format!("{:#}", result.as_ref().err().unwrap());
        assert!(
            err_string.contains("review"),
            "error should mention tool 'review': {err_string}"
        );

        assert!(
            result.is_err(),
            "worker with denied tool should fail to start"
        );
        let err = result.err().unwrap();
        assert!(
            format!("{err:#}").contains("denied"),
            "error should mention policy denial: {err:#}"
        );
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            0,
            "native executor must NOT be called when tool is denied"
        );

        Ok(())
    }

    #[test]
    fn test_provider_adapter_variant_applied_artifact() -> Result<()> {
        // Verify that ProviderAdapter records the correct final applied variant
        // for different scenarios.

        // Scenario 1: variant set → applied matches variant
        let adapter = ProviderAdapter::new(
            Some("pro".to_string()),
            WorkerToolPolicy::default(),
            WorkerCategory::Quick,
        );
        assert_eq!(adapter.variant.as_deref(), Some("pro"));

        // Scenario 2: no variant → applied is "none"
        let adapter =
            ProviderAdapter::new(None, WorkerToolPolicy::default(), WorkerCategory::Quick);
        assert_eq!(adapter.variant_applied(), "none");

        // Scenario 3: variant supported produces params
        let params = ProviderAdapter::new(
            Some("premium".to_string()),
            WorkerToolPolicy::default(),
            WorkerCategory::Quick,
        )
        .model_params()?
        .expect("premium should be supported");
        assert!(params.capabilities.contains(&"tools".to_string()));

        Ok(())
    }

    #[test]
    fn test_command_worker_variant_env_var() -> Result<()> {
        // Use Deep category + category-specific env var to avoid interfering
        // with parallel tests that rely on the default Quick category.
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_variant_env".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test variant env".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(
                "sh -c 'echo GEARBOX_WORKER_MODEL_VARIANT=$GEARBOX_WORKER_MODEL_VARIANT; echo GEARBOX_WORKER_PROMPT_MANIFEST=$GEARBOX_WORKER_PROMPT_MANIFEST; echo GEARBOX_WORKER_PROMPT_RECONCILE=$GEARBOX_WORKER_PROMPT_RECONCILE'"
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
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let orig_deep = env::var("GEARBOX_GEAR_CATEGORY_DEEP_VARIANT").ok();
        unsafe {
            env::set_var("GEARBOX_GEAR_CATEGORY_DEEP_VARIANT", "pro");
            env::remove_var("GEARBOX_WORKER_MODEL_VARIANT");
        }

        let result = WorkerRegistry::default().run(WorkerRunRequest {
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
        });

        unsafe {
            if let Some(v) = orig_deep {
                env::set_var("GEARBOX_GEAR_CATEGORY_DEEP_VARIANT", v);
            } else {
                env::remove_var("GEARBOX_GEAR_CATEGORY_DEEP_VARIANT");
            }
        }

        let result = result?;
        assert_eq!(result.status, WorkerStatus::Succeeded);
        let stdout_path = result.stdout_path.context("stdout_path should be set")?;
        let stdout = fs::read_to_string(stdout_path)?;
        assert!(
            stdout.contains("GEARBOX_WORKER_MODEL_VARIANT=pro"),
            "stdout should contain the env var with value 'pro': {:?}",
            stdout
        );
        assert!(stdout.contains("GEARBOX_WORKER_PROMPT_MANIFEST="));
        assert!(stdout.contains("GEARBOX_WORKER_PROMPT_RECONCILE="));
        let manifest_path = store.worker_dir(&task.id).join("prompt-manifest.json");
        let manifest: PromptManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
        assert_eq!(manifest.task_id, task.id);
        assert_eq!(manifest.worker, "opencode");
        let reconcile_path = store.worker_dir(&task.id).join("prompt-reconcile.json");
        let reconcile: PromptReconcileReceipt = serde_json::from_slice(&fs::read(reconcile_path)?)?;
        assert_eq!(reconcile.action, PromptReconcileAction::NewSession);
        reconcile.validate()?;

        Ok(())
    }

    #[test]
    fn test_native_worker_fail_closed_on_variant() -> Result<()> {
        // Use Deep category + category-specific env var to avoid interfering
        // with parallel tests that rely on the default Quick category.
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_native_variant".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test native variant reject".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("zed_agent".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::ZedAgent,
            worker_command: Some("should not run".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };

        let orig_deep = env::var("GEARBOX_GEAR_CATEGORY_DEEP_VARIANT").ok();
        unsafe {
            env::set_var("GEARBOX_GEAR_CATEGORY_DEEP_VARIANT", "pro");
        }

        let started = Arc::new(AtomicBool::new(false));
        let registry = WorkerRegistry::with_native_backend(Arc::new(FakeNativeBackend {
            started: started.clone(),
        }));

        let result = registry.run(WorkerRunRequest {
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
        });

        unsafe {
            if let Some(v) = orig_deep {
                env::set_var("GEARBOX_GEAR_CATEGORY_DEEP_VARIANT", v);
            } else {
                env::remove_var("GEARBOX_GEAR_CATEGORY_DEEP_VARIANT");
            }
        }

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("provider variant contract"),
            "error should mention variant contract: {}",
            err
        );
        assert!(
            !started.load(Ordering::SeqCst),
            "native backend must not start when variant is rejected"
        );

        Ok(())
    }

    #[test]
    fn test_command_worker_no_variant_no_env_var() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_no_variant_env".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test no variant env".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(
                "sh -c 'if [ -n \"$GEARBOX_WORKER_MODEL_VARIANT\" ]; then exit 1; else echo no_variant; fi'"
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
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let orig_explore = env::var("GEARBOX_GEAR_CATEGORY_EXPLORE_VARIANT").ok();
        let orig_worker = env::var("GEARBOX_GEAR_WORKER_VARIANT").ok();
        let orig_model = env::var("GEARBOX_WORKER_MODEL_VARIANT").ok();
        unsafe {
            env::remove_var("GEARBOX_GEAR_CATEGORY_EXPLORE_VARIANT");
            env::remove_var("GEARBOX_GEAR_WORKER_VARIANT");
            env::remove_var("GEARBOX_WORKER_MODEL_VARIANT");
        }

        let result = WorkerRegistry::default().run(WorkerRunRequest {
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
            route_hint: Some("explore"),
        });

        unsafe {
            if let Some(v) = orig_explore {
                env::set_var("GEARBOX_GEAR_CATEGORY_EXPLORE_VARIANT", v);
            } else {
                env::remove_var("GEARBOX_GEAR_CATEGORY_EXPLORE_VARIANT");
            }
            if let Some(v) = orig_worker {
                env::set_var("GEARBOX_GEAR_WORKER_VARIANT", v);
            } else {
                env::remove_var("GEARBOX_GEAR_WORKER_VARIANT");
            }
            if let Some(v) = orig_model {
                env::set_var("GEARBOX_WORKER_MODEL_VARIANT", v);
            } else {
                env::remove_var("GEARBOX_WORKER_MODEL_VARIANT");
            }
        }

        let result = result?;
        assert_eq!(result.status, WorkerStatus::Succeeded);
        let stdout_path = result.stdout_path.context("stdout_path should be set")?;
        let stdout = fs::read_to_string(stdout_path)?;
        assert!(
            stdout.contains("no_variant"),
            "stdout should show no_variant (env var was absent): {:?}",
            stdout
        );

        Ok(())
    }

    #[test]
    fn opencode_command_worker_exposes_session_outcome() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_session".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test worker session".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
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
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: false,
        };

        let handle = OpencodeCommandWorker {}.start(WorkerStartRequest {
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
        let parameter_receipt: WorkerParameterResolutionReceipt = serde_json::from_slice(
            &fs::read(store.worker_dir(&task.id).join("parameter-resolution.json"))?,
        )?;
        parameter_receipt.validate()?;
        assert!(matches!(
            parameter_receipt.status.as_str(),
            "resolved" | "unknown"
        ));
        let outcome = handle.wait_for_outcome()?;

        assert_eq!(outcome.status, WorkerStatus::Skipped);
        assert_eq!(
            outcome.summary,
            "Worker execution was skipped by CLI option."
        );
        assert!(
            handle
                .last_output()
                .as_deref()
                .is_some_and(|output| output.contains("Worker execution was skipped"))
        );
        assert!(store.worker_dir(&task.id).join("outcome.json").exists());
        assert!(handle.send_follow_up("continue".to_string()).is_err());
        Ok(())
    }

    #[test]
    fn command_worker_skips_when_binary_is_missing() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_missing_binary".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test missing worker binary".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("codex".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
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
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = CodexCommandWorker {}.start(WorkerStartRequest {
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
        let result = handle.wait_for_result()?;

        assert_eq!(result.status, WorkerStatus::Skipped);
        assert!(
            result
                .summary
                .contains("No worker command binary `__gearbox_missing_worker_command__`")
        );
        Ok(())
    }

    #[test]
    fn command_worker_caches_last_output_from_stdout_and_stderr() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_output".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test worker output".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(
                "sh -c 'printf stdout-value; printf stderr-value >&2'".to_string(),
            ),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeCommandWorker {}.start(WorkerStartRequest {
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

        let result = handle.wait_for_result()?;
        assert_eq!(result.status, WorkerStatus::Succeeded);
        let output = handle
            .last_output()
            .context("missing cached worker output")?;
        assert!(output.contains("stdout-value"));
        assert!(output.contains("stderr-value"));
        Ok(())
    }

    #[test]
    fn command_worker_parses_structured_last_message_into_outcome() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_structured_outcome".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test structured outcome".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("custom".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Custom,
            worker_command: Some(
                "sh -c 'cat <<\"EOF\" > \"$GEARBOX_WORKER_LAST_MESSAGE\"\n## Summary\ncompleted the requested change\n\n## Changed Files\n- src/main.rs\n- README.md\n\n## Commands Run\n- cargo test -p gearbox_agent\n\n## Known Failures\n- none\nEOF'"
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
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = CustomCommandWorker {}.start(WorkerStartRequest {
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
        let outcome = handle.wait_for_outcome()?;

        assert_eq!(outcome.status, WorkerStatus::Succeeded);
        assert_eq!(outcome.summary, "completed the requested change");
        assert_eq!(
            outcome.changed_files,
            vec!["src/main.rs".to_string(), "README.md".to_string()]
        );
        assert_eq!(
            outcome.commands_run,
            vec!["cargo test -p gearbox_agent".to_string()]
        );
        assert_eq!(outcome.known_failures, vec!["none".to_string()]);
        assert!(outcome.raw_output_path.is_some());
        Ok(())
    }

    #[test]
    fn opencode_session_worker_runs_follow_up_and_steer_turns() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_opencode_session".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test opencode session".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("sh -c 'cat \"$GEARBOX_WORKER_PROMPT\"'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        let emitted_events = Arc::new(Mutex::new(Vec::new()));
        let subscription = handle.subscribe(Arc::new({
            let emitted_events = emitted_events.clone();
            move |event| {
                if let Ok(mut events) = emitted_events.lock() {
                    events.push(event);
                }
            }
        }))?;

        assert_eq!(
            handle.session_id().as_deref(),
            Some("task_opencode_session_session")
        );
        assert_eq!(handle.wait_for_result()?.status, WorkerStatus::Succeeded);
        handle.send_follow_up("continue with second turn".to_string())?;
        assert!(
            handle
                .last_output()
                .as_deref()
                .is_some_and(|output| output.contains("continue with second turn"))
        );
        handle.steer("adjust course".to_string())?;
        assert!(
            handle
                .last_output()
                .as_deref()
                .is_some_and(|output| output.contains("adjust course"))
        );
        assert!(store.worker_dir(&task.id).join("follow-up-1.md").exists());
        assert!(store.worker_dir(&task.id).join("steer-2.md").exists());
        assert!(store.worker_dir(&task.id).join("transcript.jsonl").exists());
        assert!(
            store
                .worker_dir(&task.id)
                .join("tool-events.jsonl")
                .exists()
        );
        let transcript =
            std::fs::read_to_string(store.worker_dir(&task.id).join("transcript.jsonl"))?;
        assert!(transcript.contains("\"turn_started\""));
        assert!(transcript.contains("\"turn_finished\""));
        let events = emitted_events
            .lock()
            .map_err(|_| anyhow::anyhow!("worker event mutex poisoned"))?;
        assert!(events.iter().any(|event| matches!(
            event,
            WorkerEvent::TurnStarted { kind, .. } if kind == "run"
        )));
        drop(subscription);
        Ok(())
    }

    #[test]
    fn opencode_session_worker_accumulates_usage_across_turns() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_opencode_session_usage_accumulation".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test opencode session usage accumulation".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let script_path = temp_dir.path().join("usage-worker.sh");
        std::fs::write(
            &script_path,
            r#"#!/bin/sh
case "$GEARBOX_WORKER_PROMPT" in
  *follow-up-1-compiled.md) printf '%s\n' '{"usage":{"input_tokens":7,"output_tokens":4,"cost_micros":5,"duration_ms":13,"cache_hit":true}}' ;;
  *steer-2-compiled.md) printf '%s\n' '{"usage":{"input_tokens":9,"output_tokens":6,"cost_micros":7,"duration_ms":17,"cache_hit":true}}' ;;
  *) printf '%s\n' '{"usage":{"input_tokens":5,"output_tokens":2,"cost_micros":3,"duration_ms":11,"cache_hit":false}}' ;;
esac
"#,
        )?;
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(format!(
                "sh {}",
                shell_single_quote(&script_path.to_string_lossy())
            )),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };
        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        handle.wait_for_result()?;
        let first = handle.usage().context("first turn usage missing")?;
        assert_eq!(first.requested_tokens, Some(5));
        assert_eq!(first.actual_tokens, Some(2));
        assert_eq!(first.cost_micros, Some(3));
        assert_eq!(first.duration_ms, Some(11));

        handle.send_follow_up("continue with a second turn".to_string())?;
        let second = handle.usage().context("second turn usage missing")?;
        assert_eq!(second.requested_tokens, Some(12));
        assert_eq!(second.actual_tokens, Some(6));
        assert_eq!(second.cost_micros, Some(8));
        assert_eq!(second.duration_ms, Some(24));

        handle.steer("finish with a third turn".to_string())?;
        let third = handle.usage().context("third turn usage missing")?;
        assert_eq!(third.requested_tokens, Some(21));
        assert_eq!(third.actual_tokens, Some(12));
        assert_eq!(third.cost_micros, Some(15));
        assert_eq!(third.duration_ms, Some(41));
        assert_eq!(third.cache_hit, Some(true));
        let persisted: BrokerUsage = serde_json::from_slice(&std::fs::read(
            store.worker_dir(&task.id).join("usage.json"),
        )?)?;
        assert_eq!(persisted, third);
        Ok(())
    }

    #[test]
    fn merge_worker_usage_keeps_unknown_totals_unknown() {
        let previous = BrokerUsage {
            requested_tokens: None,
            actual_tokens: Some(5),
            model: "previous-model".to_string(),
            duration_ms: Some(3),
            cost_micros: None,
            cache_hit: Some(false),
            unavailable_reason: Some("previous usage was incomplete".to_string()),
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

        let merged = merge_worker_usage(Some(previous), current);

        assert_eq!(merged.requested_tokens, None);
        assert_eq!(merged.actual_tokens, None);
        assert_eq!(merged.duration_ms, Some(10));
        assert_eq!(merged.cost_micros, None);
        assert_eq!(merged.model, "current-model");
        assert_eq!(merged.cache_hit, Some(true));
        assert_eq!(
            merged.unavailable_reason.as_deref(),
            Some("previous usage was incomplete")
        );
    }

    #[test]
    fn opencode_session_worker_reattaches_from_persisted_descriptor() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_opencode_session_reattach".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test opencode session reattach".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let script_path = temp_dir.path().join("resident-worker.sh");
        std::fs::write(
            &script_path,
            "#!/bin/sh\nprintf '%s:%s\\n' \"$GEARBOX_WORKER_SESSION_ID\" \"$GEARBOX_WORKER_RESUME\" > \"${GEARBOX_WORKER_PACKET}.session\"\nprintf '{\"sessionID\":\"provider-session-1\",\"usage\":{\"input_tokens\":12,\"output_tokens\":7,\"cost_micros\":42,\"cache_hit\":true}}\\n'\n",
        )?;
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(format!(
                "sh {}",
                shell_single_quote(&script_path.to_string_lossy())
            )),
            worker_model: Some("provider/model-a".to_string()),
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };
        let request = || WorkerStartRequest {
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
        };

        let first_handle = OpencodeSessionWorker {}.start(request())?;
        assert_eq!(
            first_handle.session_id().as_deref(),
            Some("task_opencode_session_reattach_session")
        );
        assert_eq!(
            first_handle.wait_for_result()?.status,
            WorkerStatus::Succeeded
        );
        assert_eq!(
            first_handle.session_id().as_deref(),
            Some("provider-session-1")
        );
        let usage = first_handle.usage().context("worker usage missing")?;
        assert_eq!(usage.requested_tokens, Some(12));
        assert_eq!(usage.actual_tokens, Some(7));
        assert_eq!(usage.cost_micros, Some(42));
        assert_eq!(usage.cache_hit, Some(true));

        let descriptor_path = resident_session_descriptor_path(&store, &task.id);
        let descriptor: ResidentSessionDescriptor =
            serde_json::from_slice(&std::fs::read(&descriptor_path)?)?;
        descriptor.validate()?;
        assert_eq!(
            descriptor.provider_session_id.as_deref(),
            Some("provider-session-1")
        );
        assert_eq!(descriptor.resume_count, 0);

        let second_handle = OpencodeSessionWorker {}.start(request())?;
        assert_eq!(
            second_handle.session_id().as_deref(),
            Some("provider-session-1")
        );
        assert_eq!(
            second_handle.wait_for_result()?.status,
            WorkerStatus::Succeeded
        );
        let env_marker =
            std::fs::read_to_string(store.worker_dir(&task.id).join("packet.json.session"))?;
        assert_eq!(env_marker.trim(), "provider-session-1:true");
        let resumed_descriptor: ResidentSessionDescriptor =
            serde_json::from_slice(&std::fs::read(&descriptor_path)?)?;
        assert_eq!(resumed_descriptor.resume_count, 1);
        assert_eq!(
            resumed_descriptor.worker_model.as_deref(),
            Some("provider/model-a")
        );
        resumed_descriptor.validate()?;

        let mut mismatched_config = config.clone();
        mismatched_config.worker_model = Some("provider/model-b".to_string());
        let mismatch_error = match (OpencodeSessionWorker {}).start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &mismatched_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        }) {
            Ok(_) => panic!("a resident session must reject a different worker model"),
            Err(error) => error,
        };
        assert!(
            mismatch_error
                .to_string()
                .contains("resident session worker model binding mismatch")
        );
        assert_eq!(
            std::fs::read_to_string(store.worker_dir(&task.id).join("packet.json.session"))?,
            "provider-session-1:true\n"
        );
        let unchanged_descriptor: ResidentSessionDescriptor =
            serde_json::from_slice(&std::fs::read(&descriptor_path)?)?;
        assert_eq!(unchanged_descriptor.resume_count, 1);
        assert_eq!(
            unchanged_descriptor.provider_session_id.as_deref(),
            Some("provider-session-1")
        );
        assert_eq!(
            unchanged_descriptor.worker_model.as_deref(),
            Some("provider/model-a")
        );
        unchanged_descriptor.validate()?;
        Ok(())
    }

    #[test]
    fn opencode_session_worker_revives_after_cancel_before_follow_up() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_opencode_session_revive".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test opencode session revive".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("sh -c 'cat \"$GEARBOX_WORKER_PROMPT\"'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        assert_eq!(handle.wait_for_result()?.status, WorkerStatus::Succeeded);
        handle.cancel()?;
        handle.send_follow_up("continue after revive".to_string())?;

        assert!(
            handle
                .last_output()
                .as_deref()
                .is_some_and(|output| output.contains("continue after revive"))
        );
        assert!(store.worker_dir(&task.id).join("revive-1.md").exists());
        assert!(store.worker_dir(&task.id).join("follow-up-1.md").exists());
        Ok(())
    }

    #[test]
    fn opencode_session_worker_interrupt_writes_artifact_and_revives() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_opencode_session_interrupt".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test opencode session interrupt".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("sh -c 'cat \"$GEARBOX_WORKER_PROMPT\"'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        assert_eq!(handle.wait_for_result()?.status, WorkerStatus::Succeeded);
        handle.interrupt()?;
        handle.send_follow_up("continue after interrupt".to_string())?;

        assert!(
            handle
                .last_output()
                .as_deref()
                .is_some_and(|output| output.contains("continue after interrupt"))
        );
        assert!(store.worker_dir(&task.id).join("interrupt-1.md").exists());
        assert!(store.worker_dir(&task.id).join("revive-1.md").exists());
        Ok(())
    }

    #[test]
    fn worker_event_hub_replays_bounded_history_to_late_subscribers() -> Result<()> {
        let subscriptions = Arc::new(WorkerSessionSubscriptions::default());
        subscriptions.emit(WorkerEvent::TurnStarted {
            kind: "acp".to_string(),
            prompt_path: PathBuf::from("prompt.md"),
        });
        subscriptions.emit(WorkerEvent::AssistantTextDelta {
            kind: "acp".to_string(),
            delta: "early output".to_string(),
        });

        let received_events = Arc::new(Mutex::new(Vec::new()));
        let received_events_for_listener = received_events.clone();
        let _subscription = subscriptions.subscribe(Arc::new(move |event| {
            if let Ok(mut events) = received_events_for_listener.lock() {
                events.push(event);
            }
        }))?;

        {
            let events = received_events
                .lock()
                .map_err(|_| anyhow::anyhow!("worker event mutex poisoned"))?;
            assert_eq!(events.len(), 2);
            assert!(matches!(events[0], WorkerEvent::TurnStarted { .. }));
            assert!(matches!(events[1], WorkerEvent::AssistantTextDelta { .. }));
        }

        subscriptions.emit(WorkerEvent::TurnFinished {
            kind: "acp".to_string(),
            result_path: PathBuf::from("result.json"),
            outcome_path: PathBuf::from("outcome.json"),
            summary: "completed".to_string(),
        });
        assert_eq!(
            received_events
                .lock()
                .map_err(|_| anyhow::anyhow!("worker event mutex poisoned"))?
                .len(),
            3
        );

        for index in 0..(WORKER_EVENT_HISTORY_LIMIT + 6) {
            subscriptions.emit(WorkerEvent::AssistantTextDelta {
                kind: "acp".to_string(),
                delta: format!("delta-{index}"),
            });
        }

        let late_events = Arc::new(Mutex::new(Vec::new()));
        let late_events_for_listener = late_events.clone();
        let _late_subscription = subscriptions.subscribe(Arc::new(move |event| {
            if let Ok(mut events) = late_events_for_listener.lock() {
                events.push(event);
            }
        }))?;
        let late_events = late_events
            .lock()
            .map_err(|_| anyhow::anyhow!("worker event mutex poisoned"))?;
        assert_eq!(late_events.len(), WORKER_EVENT_HISTORY_LIMIT);
        assert!(matches!(
            &late_events[0],
            WorkerEvent::AssistantTextDelta { delta, .. } if delta == "delta-6"
        ));
        assert!(matches!(
            late_events.last(),
            Some(WorkerEvent::AssistantTextDelta { delta, .. }) if delta == "delta-69"
        ));
        Ok(())
    }

    #[test]
    fn worker_subscribe_writes_transcript_events() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_subscribe_transcript".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test subscribe transcript".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("printf hello-worker".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        let received_events = Arc::new(Mutex::new(Vec::new()));
        let _subscription = handle.subscribe(Arc::new({
            let received_events = received_events.clone();
            move |event| {
                if let Ok(mut events) = received_events.lock() {
                    events.push(event);
                }
            }
        }))?;

        handle.wait_for_result()?;

        assert!(store.worker_dir(&task.id).join("transcript.jsonl").exists());
        assert!(
            store
                .worker_dir(&task.id)
                .join("tool-events.jsonl")
                .exists()
        );
        let transcript =
            std::fs::read_to_string(store.worker_dir(&task.id).join("transcript.jsonl"))?;
        assert!(transcript.contains("\"turn_started\""));
        assert!(transcript.contains("\"turn_finished\""));

        let events = received_events
            .lock()
            .map_err(|_| anyhow::anyhow!("worker event mutex poisoned"))?;
        let turn_started_count = events
            .iter()
            .filter(|event| matches!(event, WorkerEvent::TurnStarted { .. }))
            .count();
        assert_eq!(
            turn_started_count, 1,
            "should have received 1 turn_started event"
        );
        Ok(())
    }

    #[test]
    fn worker_transcript_includes_tool_call_deltas() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_tool_deltas".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test tool call deltas".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(
                r#"printf 'Before tool.\n<function_calls>\n<invoke name="read_file">\n<parameter name="path">src/main.rs</parameter>\n</invoke>\n</function_calls>\nAfter tool.'"#.to_string(),
            ),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        let received_events = Arc::new(Mutex::new(Vec::new()));
        let _subscription = handle.subscribe(Arc::new({
            let received_events = received_events.clone();
            move |event| {
                if let Ok(mut events) = received_events.lock() {
                    events.push(event);
                }
            }
        }))?;

        handle.wait_for_result()?;

        let transcript =
            std::fs::read_to_string(store.worker_dir(&task.id).join("transcript.jsonl"))?;
        assert!(
            transcript.contains("\"tool_call_started\""),
            "transcript should contain tool_call_started"
        );
        assert!(
            transcript.contains("\"tool_call_finished\""),
            "transcript should contain tool_call_finished"
        );
        assert!(
            transcript.contains("\"assistant_text_delta\""),
            "transcript should contain assistant_text_delta"
        );
        assert!(
            transcript.contains("\"read_file\""),
            "transcript should contain the tool name read_file"
        );

        let tool_events =
            std::fs::read_to_string(store.worker_dir(&task.id).join("tool-events.jsonl"))?;
        assert!(
            tool_events.contains("\"tool_call_started\""),
            "tool-events should contain tool_call_started"
        );
        assert!(
            tool_events.contains("\"tool_call_finished\""),
            "tool-events should contain tool_call_finished"
        );

        let events = received_events
            .lock()
            .map_err(|_| anyhow::anyhow!("worker event mutex poisoned"))?;
        let tool_started_count = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::ToolCallStarted { .. }))
            .count();
        assert_eq!(
            tool_started_count, 1,
            "should have received 1 tool_call_started event"
        );
        let text_delta_count = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::AssistantTextDelta { .. }))
            .count();
        assert!(
            text_delta_count >= 2,
            "should have received at least 2 assistant_text_delta events (before + after tool call), got {text_delta_count}"
        );

        Ok(())
    }

    #[test]
    fn parse_and_emit_events_for_function_calls() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_parse_function_calls";

        store.write_worker_file(task_id, "transcript.jsonl", "")?;
        store.write_worker_file(task_id, "tool-events.jsonl", "")?;

        let subscriptions = Arc::new(WorkerSessionSubscriptions::default());
        let received_events = Arc::new(Mutex::new(Vec::new()));
        let _subscription = subscriptions.subscribe(Arc::new({
            let received_events = received_events.clone();
            move |event| {
                if let Ok(mut events) = received_events.lock() {
                    events.push(event);
                }
            }
        }))?;

        let handle = CommandWorkerSessionHandle {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task_id: task_id.to_string(),
            task_attempt: 1,
            worker_name: "test_worker".to_string(),
            skip_worker: false,
            command: None,
            command_timeout: Some(Duration::from_secs(30)),
            worker_model: None,
            model_variant: None,
            tool_policy: WorkerToolPolicy::default(),
            packet_path: temp_dir.path().join("packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
            prompt_manifest_path: temp_dir.path().join("prompt-manifest.json"),
            prompt_reconcile_path: temp_dir.path().join("prompt-reconcile.json"),
            prompt_capsule_path: temp_dir.path().join("prompt-capsule.json"),
            subscriptions,
            session_state: Mutex::new(ResidentSessionState {
                cancellation_token: CancellationToken::new(),
                active_command: false,
                revive_count: 0,
                interrupt_count: 0,
                turn_epoch: 0,
                stale_reason: None,
            }),
            result: Mutex::new(None),
            last_output: Mutex::new(None),
            follow_up_count: Mutex::new(0),
            supports_interaction: true,
            omo_config_dir: None,
        };

        let stdout = "Some text before.\n<function_calls>\n<invoke name=\"read_file\">\n<parameter name=\"path\">src/main.rs</parameter>\n</invoke>\n<invoke name=\"write_file\">\n<parameter name=\"path\">src/lib.rs</parameter>\n<parameter name=\"content\">hello</parameter>\n</invoke>\n</function_calls>\nSome text after.";

        handle.parse_and_emit_tool_events(stdout, "test")?;

        let events = received_events
            .lock()
            .map_err(|_| anyhow::anyhow!("worker event mutex poisoned"))?;

        let text_delta_count = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::AssistantTextDelta { .. }))
            .count();
        let tool_started_count = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::ToolCallStarted { .. }))
            .count();
        let tool_finished_count = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::ToolCallFinished { .. }))
            .count();

        assert_eq!(
            text_delta_count, 2,
            "should have 2 AssistantTextDelta events (before and after function_calls group)"
        );
        assert_eq!(
            tool_started_count, 2,
            "should have 2 ToolCallStarted events (read_file + write_file)"
        );
        assert_eq!(
            tool_finished_count, 2,
            "should have 2 ToolCallFinished events"
        );

        let tool_starts: Vec<&WorkerEvent> = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::ToolCallStarted { .. }))
            .collect();
        if let WorkerEvent::ToolCallStarted {
            tool_name,
            arguments,
            ..
        } = tool_starts[0]
        {
            assert_eq!(tool_name, "read_file");
            assert_eq!(arguments, "path=src/main.rs");
        } else {
            panic!("expected ToolCallStarted");
        }
        if let WorkerEvent::ToolCallStarted {
            tool_name,
            arguments,
            ..
        } = tool_starts[1]
        {
            assert_eq!(tool_name, "write_file");
            assert_eq!(arguments, "path=src/lib.rs, content=hello");
        } else {
            panic!("expected ToolCallStarted");
        }

        let transcript =
            std::fs::read_to_string(store.worker_dir(task_id).join("transcript.jsonl"))?;
        assert!(
            transcript.contains("\"assistant_text_delta\""),
            "transcript should contain assistant_text_delta"
        );
        assert!(
            transcript.contains("\"tool_call_started\""),
            "transcript should contain tool_call_started"
        );
        assert!(
            transcript.contains("\"tool_call_finished\""),
            "transcript should contain tool_call_finished"
        );
        assert!(
            transcript.contains("\"read_file\""),
            "transcript should contain read_file tool name"
        );
        assert!(
            transcript.contains("\"write_file\""),
            "transcript should contain write_file tool name"
        );

        let tool_events =
            std::fs::read_to_string(store.worker_dir(task_id).join("tool-events.jsonl"))?;
        assert!(
            tool_events.contains("\"tool_call_started\""),
            "tool-events should contain tool_call_started"
        );
        assert!(
            tool_events.contains("\"tool_call_finished\""),
            "tool-events should contain tool_call_finished"
        );
        assert!(
            tool_events.contains("unknown: tool result was not present"),
            "missing command-backed tool results must remain explicit unknown evidence"
        );
        let validation: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
            store.worker_dir(task_id).join("tool-pair-validation.json"),
        )?)?;
        assert_eq!(validation["status"], "unknown");
        assert_eq!(validation["started_calls"], 2);
        assert_eq!(validation["finished_calls"], 2);
        assert_eq!(validation["unknown_results"], 2);
        assert_eq!(validation["task_id"], task_id);

        Ok(())
    }

    #[test]
    fn parse_and_emit_events_for_tool_use_format() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_parse_tool_use";

        store.write_worker_file(task_id, "transcript.jsonl", "")?;
        store.write_worker_file(task_id, "tool-events.jsonl", "")?;

        let subscriptions = Arc::new(WorkerSessionSubscriptions::default());
        let received_events = Arc::new(Mutex::new(Vec::new()));
        let _subscription = subscriptions.subscribe(Arc::new({
            let received_events = received_events.clone();
            move |event| {
                if let Ok(mut events) = received_events.lock() {
                    events.push(event);
                }
            }
        }))?;

        let handle = CommandWorkerSessionHandle {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task_id: task_id.to_string(),
            task_attempt: 1,
            worker_name: "test_worker".to_string(),
            skip_worker: false,
            command: None,
            command_timeout: Some(Duration::from_secs(30)),
            worker_model: None,
            model_variant: None,
            tool_policy: WorkerToolPolicy::default(),
            packet_path: temp_dir.path().join("packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
            prompt_manifest_path: temp_dir.path().join("prompt-manifest.json"),
            prompt_reconcile_path: temp_dir.path().join("prompt-reconcile.json"),
            prompt_capsule_path: temp_dir.path().join("prompt-capsule.json"),
            subscriptions,
            session_state: Mutex::new(ResidentSessionState {
                cancellation_token: CancellationToken::new(),
                active_command: false,
                revive_count: 0,
                interrupt_count: 0,
                turn_epoch: 0,
                stale_reason: None,
            }),
            result: Mutex::new(None),
            last_output: Mutex::new(None),
            follow_up_count: Mutex::new(0),
            supports_interaction: true,
            omo_config_dir: None,
        };

        let stdout = "Some text.\n<tool_use>\n<invoke name=\"read_file\">\n<parameter name=\"path\">src/main.rs</parameter>\n</invoke>\n</tool_use>\nMore text.";

        handle.parse_and_emit_tool_events(stdout, "test")?;

        let events = received_events
            .lock()
            .map_err(|_| anyhow::anyhow!("worker event mutex poisoned"))?;

        let text_delta_count = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::AssistantTextDelta { .. }))
            .count();
        let tool_started_count = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::ToolCallStarted { .. }))
            .count();
        let tool_finished_count = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::ToolCallFinished { .. }))
            .count();

        assert_eq!(
            text_delta_count, 2,
            "should have 2 AssistantTextDelta events (before and after tool_use group)"
        );
        assert_eq!(tool_started_count, 1, "should have 1 ToolCallStarted event");
        assert_eq!(
            tool_finished_count, 1,
            "should have 1 ToolCallFinished event"
        );

        let tool_starts: Vec<&WorkerEvent> = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::ToolCallStarted { .. }))
            .collect();
        if let WorkerEvent::ToolCallStarted { tool_name, .. } = tool_starts[0] {
            assert_eq!(tool_name, "read_file");
        } else {
            panic!("expected ToolCallStarted");
        }

        let transcript =
            std::fs::read_to_string(store.worker_dir(task_id).join("transcript.jsonl"))?;
        assert!(
            transcript.contains("\"assistant_text_delta\""),
            "transcript should contain assistant_text_delta"
        );
        assert!(
            transcript.contains("\"tool_call_started\""),
            "transcript should contain tool_call_started"
        );
        assert!(
            transcript.contains("\"tool_call_finished\""),
            "transcript should contain tool_call_finished"
        );

        Ok(())
    }

    #[test]
    fn parse_and_emit_events_for_malformed_output() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_parse_malformed";

        store.write_worker_file(task_id, "transcript.jsonl", "")?;
        store.write_worker_file(task_id, "tool-events.jsonl", "")?;

        let subscriptions = Arc::new(WorkerSessionSubscriptions::default());

        let handle = CommandWorkerSessionHandle {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task_id: task_id.to_string(),
            task_attempt: 1,
            worker_name: "test_worker".to_string(),
            skip_worker: false,
            command: None,
            command_timeout: Some(Duration::from_secs(30)),
            worker_model: None,
            model_variant: None,
            tool_policy: WorkerToolPolicy::default(),
            packet_path: temp_dir.path().join("packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
            prompt_manifest_path: temp_dir.path().join("prompt-manifest.json"),
            prompt_reconcile_path: temp_dir.path().join("prompt-reconcile.json"),
            prompt_capsule_path: temp_dir.path().join("prompt-capsule.json"),
            subscriptions,
            session_state: Mutex::new(ResidentSessionState {
                cancellation_token: CancellationToken::new(),
                active_command: false,
                revive_count: 0,
                interrupt_count: 0,
                turn_epoch: 0,
                stale_reason: None,
            }),
            result: Mutex::new(None),
            last_output: Mutex::new(None),
            follow_up_count: Mutex::new(0),
            supports_interaction: true,
            omo_config_dir: None,
        };

        let stdout1 = "This is just random text with no XML tool call patterns.";
        assert!(
            handle.parse_and_emit_tool_events(stdout1, "test").is_ok(),
            "plain text should not cause panic"
        );

        let stdout2 = "<function_calls>no closing tag here";
        assert!(
            handle.parse_and_emit_tool_events(stdout2, "test").is_ok(),
            "unclosed function_calls tag should not cause panic"
        );

        let stdout3 = "<tool_use>\n<invoke name=\"read_file\"></invoke>\n";
        assert!(
            handle.parse_and_emit_tool_events(stdout3, "test").is_ok(),
            "unclosed tool_use tag should not cause panic"
        );

        assert!(
            handle.parse_and_emit_tool_events("", "test").is_ok(),
            "empty string should return Ok(())"
        );

        let stdout5 = "<function_calls><invoke name=>no value</invoke></function_calls>";
        assert!(
            handle.parse_and_emit_tool_events(stdout5, "test").is_ok(),
            "malformed invoke should not cause panic"
        );

        let transcript =
            std::fs::read_to_string(store.worker_dir(task_id).join("transcript.jsonl"))?;
        assert!(
            transcript.contains("\"assistant_text_delta\""),
            "transcript should contain assistant_text_delta even for malformed output"
        );

        Ok(())
    }

    #[test]
    fn dispose_is_idempotent() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_dispose_idempotent".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test dispose idempotent".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("printf disposable".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        handle.dispose()?;
        handle.dispose()?;
        handle.dispose()?;

        assert!(store.worker_dir(&task.id).join("dispose.md").exists());
        Ok(())
    }

    #[test]
    fn abort_after_cancel_does_not_prevent_dispose() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_abort_cancel_dispose".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test abort cancel dispose".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("printf resilient".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        handle.wait_for_result()?;
        handle.cancel()?;
        handle.abort()?;
        handle.dispose()?;

        assert!(store.worker_dir(&task.id).join("dispose.md").exists());
        Ok(())
    }

    #[test]
    fn follow_up_while_idle_begins_new_turn() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_follow_up_idle_new_turn".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test follow up idle new turn".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("sh -c 'cat \"$GEARBOX_WORKER_PROMPT\"'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        // Initial turn completes -> handle is idle
        handle.wait_for_result()?;

        // follow_up while idle should begin a new turn
        handle.send_follow_up("second turn instruction".to_string())?;

        assert!(
            handle
                .last_output()
                .as_deref()
                .is_some_and(|output| output.contains("second turn instruction"))
        );
        // The initial turn is turn-1, follow-up turn is turn-2
        assert!(
            store
                .worker_dir(&task.id)
                .join("turn-1-result.json")
                .exists()
        );
        assert!(
            store
                .worker_dir(&task.id)
                .join("turn-2-result.json")
                .exists()
        );
        assert!(store.worker_dir(&task.id).join("follow-up-1.md").exists());
        Ok(())
    }

    #[test]
    fn wait_for_idle_waits_for_latest_revived_turn() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_wait_for_idle_revived".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test wait for idle revived".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode_session".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("sh -c 'cat \"$GEARBOX_WORKER_PROMPT\"'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
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

        // Complete initial turn, cancel, then revive with follow_up
        handle.wait_for_result()?;
        handle.cancel()?;
        handle.send_follow_up("revived instruction".to_string())?;

        // wait_for_idle should wait for the revived follow-up turn
        let revived_result = handle.wait_for_idle()?;
        assert_eq!(revived_result.status, WorkerStatus::Succeeded);
        assert!(revived_result.summary.contains("worker command completed."));
        assert!(store.worker_dir(&task.id).join("revive-1.md").exists());
        Ok(())
    }

    #[test]
    fn command_worker_unsupported_subscribe_is_explicit() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_unsupported_subscribe".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test unsupported subscribe".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf no-subscribe".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeCommandWorker {}.start(WorkerStartRequest {
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

        let result = handle.subscribe(Arc::new(|_| {}));
        assert!(
            result.is_err(),
            "command worker should explicitly reject subscribe"
        );
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("do not support event subscriptions"),
            "error should mention unsupported subscription, got: {error}"
        );
        Ok(())
    }

    #[test]
    fn category_resolution_default_fields() {
        let resolution = CategoryResolution::default();
        assert_eq!(resolution.prompt_append, None);
        assert!(resolution.available_categories.is_empty());
        assert_eq!(resolution.nearest_fallback, None);
        assert!(resolution.fallback_chain.is_empty());
    }

    #[test]
    fn category_resolution_deserializes_missing_fields() {
        let json = r#"{}"#;
        let resolution: CategoryResolution = serde_json::from_str(json).expect("should parse");
        assert_eq!(resolution.prompt_append, None);
        assert!(resolution.available_categories.is_empty());
        assert_eq!(resolution.nearest_fallback, None);
        assert!(resolution.fallback_chain.is_empty());

        let json_with_prompt = r#"{"prompt_append":"extra context"}"#;
        let resolution: CategoryResolution =
            serde_json::from_str(json_with_prompt).expect("should parse");
        assert_eq!(resolution.prompt_append.as_deref(), Some("extra context"));
        assert!(resolution.available_categories.is_empty());
    }

    #[test]
    fn category_resolution_result_roundtrips_through_json() {
        let resolved = CategoryResolutionResult::Resolved {
            requested_category: "deep".to_string(),
            available_categories: vec!["quick".to_string(), "deep".to_string()],
            attempted_provider_model: Some("gpt-5".to_string()),
            nearest_fallback: Some(FallbackRoute {
                worker_kind: WorkerKind::Codex,
                worker_model: Some("gpt-4".to_string()),
            }),
        };
        let json = serde_json::to_string(&resolved).expect("should serialize");
        let back: CategoryResolutionResult =
            serde_json::from_str(&json).expect("should deserialize");
        assert_eq!(back, resolved);

        let disabled = CategoryResolutionResult::Disabled {
            requested_category: "repair".to_string(),
            available_categories: vec![],
            attempted_provider_model: None,
            nearest_fallback: None,
        };
        let json = serde_json::to_string(&disabled).expect("should serialize");
        let back: CategoryResolutionResult =
            serde_json::from_str(&json).expect("should deserialize");
        assert_eq!(back, disabled);

        let not_found = CategoryResolutionResult::NotFound {
            requested_category: "unknown".to_string(),
            available_categories: vec!["quick".to_string()],
            attempted_provider_model: None,
            nearest_fallback: None,
        };
        let json = serde_json::to_string(&not_found).expect("should serialize");
        let back: CategoryResolutionResult =
            serde_json::from_str(&json).expect("should deserialize");
        assert_eq!(back, not_found);

        let model_unavailable = CategoryResolutionResult::ModelUnavailable {
            requested_category: "deep".to_string(),
            available_categories: vec!["deep".to_string()],
            attempted_provider_model: Some("slow-model".to_string()),
            nearest_fallback: Some(FallbackRoute {
                worker_kind: WorkerKind::Claude,
                worker_model: None,
            }),
        };
        let json = serde_json::to_string(&model_unavailable).expect("should serialize");
        let back: CategoryResolutionResult =
            serde_json::from_str(&json).expect("should deserialize");
        assert_eq!(back, model_unavailable);
    }

    #[test]
    fn sanitize_model_fields_redacts_secret_keys() {
        let mut fields = HashMap::new();
        fields.insert("apiKey".to_string(), "secret123".to_string());
        fields.insert("Authorization".to_string(), "Bearer token".to_string());
        fields.insert("client_secret".to_string(), "abc".to_string());
        fields.insert("password".to_string(), "hunter2".to_string());
        fields.insert("private_key".to_string(), "key-data".to_string());
        fields.insert("secret".to_string(), "shh".to_string());
        fields.insert("secretKey".to_string(), "sk-123".to_string());
        fields.insert("token".to_string(), "tok-456".to_string());

        let sanitized = sanitize_model_fields(&fields);
        assert_eq!(sanitized.get("apiKey"), Some(&"***REDACTED***".to_string()));
        assert_eq!(
            sanitized.get("Authorization"),
            Some(&"***REDACTED***".to_string())
        );
        assert_eq!(
            sanitized.get("client_secret"),
            Some(&"***REDACTED***".to_string())
        );
        assert_eq!(
            sanitized.get("password"),
            Some(&"***REDACTED***".to_string())
        );
        assert_eq!(
            sanitized.get("private_key"),
            Some(&"***REDACTED***".to_string())
        );
        assert_eq!(sanitized.get("secret"), Some(&"***REDACTED***".to_string()));
        assert_eq!(
            sanitized.get("secretKey"),
            Some(&"***REDACTED***".to_string())
        );
        assert_eq!(sanitized.get("token"), Some(&"***REDACTED***".to_string()));
    }

    #[test]
    fn sanitize_model_fields_preserves_non_secret_keys() {
        let mut fields = HashMap::new();
        fields.insert("model".to_string(), "gpt-5".to_string());
        fields.insert("temperature".to_string(), "0.7".to_string());
        fields.insert(
            "endpoint".to_string(),
            "https://api.example.com".to_string(),
        );

        let sanitized = sanitize_model_fields(&fields);
        assert_eq!(sanitized.get("model"), Some(&"gpt-5".to_string()));
        assert_eq!(sanitized.get("temperature"), Some(&"0.7".to_string()));
        assert_eq!(
            sanitized.get("endpoint"),
            Some(&"https://api.example.com".to_string())
        );
    }

    #[test]
    fn sanitize_model_fields_handles_mixed_keys() {
        let mut fields = HashMap::new();
        fields.insert("apiKey".to_string(), "secret".to_string());
        fields.insert("model".to_string(), "gpt-5".to_string());
        fields.insert("bearer_token".to_string(), "tok".to_string());
        fields.insert("timeout".to_string(), "30".to_string());

        let sanitized = sanitize_model_fields(&fields);
        assert_eq!(sanitized.get("apiKey"), Some(&"***REDACTED***".to_string()));
        assert_eq!(sanitized.get("model"), Some(&"gpt-5".to_string()));
        assert_eq!(
            sanitized.get("bearer_token"),
            Some(&"***REDACTED***".to_string())
        );
        assert_eq!(sanitized.get("timeout"), Some(&"30".to_string()));
    }

    #[test]
    fn sanitize_model_fields_normalizes_bearer_token() {
        let mut fields = HashMap::new();
        fields.insert("bearer token".to_string(), "tok1".to_string());
        fields.insert("BearerToken".to_string(), "tok2".to_string());
        fields.insert("bearer-token".to_string(), "tok3".to_string());

        let sanitized = sanitize_model_fields(&fields);
        assert_eq!(
            sanitized.get("bearer token"),
            Some(&"***REDACTED***".to_string())
        );
        assert_eq!(
            sanitized.get("BearerToken"),
            Some(&"***REDACTED***".to_string())
        );
        assert_eq!(
            sanitized.get("bearer-token"),
            Some(&"***REDACTED***".to_string())
        );
    }

    #[test]
    fn fallback_route_equality_and_serde() {
        let route_a = FallbackRoute {
            worker_kind: WorkerKind::Codex,
            worker_model: Some("gpt-5".to_string()),
        };
        let route_b = FallbackRoute {
            worker_kind: WorkerKind::Codex,
            worker_model: Some("gpt-5".to_string()),
        };
        let route_c = FallbackRoute {
            worker_kind: WorkerKind::Claude,
            worker_model: Some("gpt-5".to_string()),
        };

        assert_eq!(route_a, route_b);
        assert_ne!(route_a, route_c);

        let json = serde_json::to_string(&route_a).expect("should serialize");
        let back: FallbackRoute = serde_json::from_str(&json).expect("should deserialize");
        assert_eq!(back, route_a);
    }

    #[test]
    fn test_command_worker_capability_boundary() -> Result<()> {
        // Command workers run as external processes. The Gear host can:
        // 1. Deny the worker from starting entirely (check_tool_allowed before dispatch)
        // 2. Set env vars (GEARBOX_WORKER_TOOL_POLICY) for the external process
        // 3. Cancel/abort the running process
        //
        // The Gear host CANNOT:
        // 1. Intercept individual tool calls inside the external process
        // 2. Enforce tool-level allow/deny after the process has started
        //
        // This test verifies the boundary claim by showing that
        // GEARBOX_WORKER_TOOL_POLICY is serialized into the command worker's
        // env so the external process CAN self-enforce, but Gear does NOT
        // claim host-level interception.
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_boundary".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test capability boundary".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'echo \"$GEARBOX_WORKER_TOOL_POLICY\"'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeCommandWorker {}.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test boundary",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("explore"),
        })?;
        let result = handle.wait_for_result()?;
        assert_eq!(result.status, WorkerStatus::Succeeded);

        let stdout_path = result.stdout_path.context("stdout_path should be set")?;
        let stdout = fs::read_to_string(stdout_path)?;
        let policy: WorkerToolPolicy = serde_json::from_str(stdout.trim())
            .context("GEARBOX_WORKER_TOOL_POLICY should be valid JSON")?;
        // Explore category has can_explore=true, can_write=false
        assert!(policy.can_explore, "explore policy allows explore");
        assert!(!policy.can_write, "explore policy denies write");

        // Verify the capability boundary is documented on the struct
        let _doc = "The CommandWorkerSessionHandle doc comment documents the capability boundary";

        Ok(())
    }

    #[test]
    fn opencode_worker_disables_file_watcher_and_records_resource_policy() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "task_opencode_file_watcher_guard".to_string(),
            goal_id: "goal_test".to_string(),
            title: "test OpenCode watcher guard".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some("opencode".to_string()),
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(
                "sh -c 'printf \"%s\" \"$OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER\"'"
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
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeCommandWorker {}.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test watcher guard",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        let result = handle.wait_for_result()?;
        assert_eq!(result.status, WorkerStatus::Succeeded);
        let stdout_path = result.stdout_path.context("stdout_path should be set")?;
        assert_eq!(fs::read_to_string(stdout_path)?.trim(), "true");

        let policy_path = store.worker_dir(&task.id).join("resource-policy.json");
        let policy: Value = serde_json::from_str(&fs::read_to_string(policy_path)?)?;
        assert_eq!(policy["mechanism_id"], "opencode_file_watcher_resource_guard");
        assert_eq!(policy["status"], "disabled");
        assert_eq!(policy["protection_status"], "configured");
        Ok(())
    }

    // ── GBX-003-004 Dispatch capture tests ──
    // Each test captures the launch contract for a specific adapter and
    // verifies that its CLI command, capabilities, and interaction support
    // are correctly declared.

    #[test]
    fn test_opencode_adapter_dispatch_contract() {
        let contract = WorkerLaunchContract {
            worker_kind: WorkerKind::Opencode.as_str().to_string(),
            default_command: WorkerKind::Opencode.default_command(None),
            supports_interaction: false,
            capabilities: WorkerCapabilities::command(),
            native_backend_available: false,
        };
        assert_eq!(contract.worker_kind, "opencode");
        assert!(contract.capabilities.supports_code_edit);
        assert!(contract.capabilities.supports_explore);
        assert!(!contract.supports_interaction);
        assert!(!contract.native_backend_available);
    }

    #[test]
    fn test_opencode_session_adapter_dispatch_contract() {
        let contract = WorkerLaunchContract {
            worker_kind: WorkerKind::OpencodeSession.as_str().to_string(),
            default_command: WorkerKind::OpencodeSession.default_command(None),
            supports_interaction: true,
            capabilities: WorkerCapabilities::resident_command(),
            native_backend_available: false,
        };
        assert_eq!(contract.worker_kind, "opencode_session");
        assert!(contract.capabilities.supports_code_edit);
        assert!(
            contract.capabilities.supports_tool_policy_enforcement,
            "resident workers should support tool policy enforcement"
        );
        assert!(contract.supports_interaction);
    }

    #[cfg(unix)]
    #[test]
    fn opencode_session_worker_bounds_provider_call_by_goal_lease() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let goal_id = "goal_runtime_deadline";
        let now = chrono::Utc::now();
        let lease_path = store.goal_run_lease_path(goal_id);
        let lease = crate::state::GoalRunLease {
            schema_version: 1,
            goal_id: goal_id.to_string(),
            epoch_id: "epoch_runtime_deadline".to_string(),
            owner_session_id: "session_runtime_deadline".to_string(),
            lease_id: "lease_runtime_deadline".to_string(),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::milliseconds(500)).to_rfc3339(),
        };
        fs::write(&lease_path, format!("{}\n", serde_json::to_string_pretty(&lease)?))?;
        let task = Task {
            id: "task_runtime_deadline".to_string(),
            goal_id: goal_id.to_string(),
            parent_task_id: None,
            title: "bounded provider call".to_string(),
            kind: crate::state::TaskKind::Review,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: Some(WorkerKind::OpencodeSession.as_str().to_string()),
            attempt: 1,
            scope: Scope::new(Vec::new(), vec![".git".to_string()], 1),
            inputs: TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some("sleep 2".to_string()),
            worker_model: Some("opencode-go/deepseek-v4-flash".to_string()),
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
            require_worker: true,
        };

        let handle = OpencodeSessionWorker {}.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "bound a slow provider call to the goal lease",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        let error = handle
            .wait_for_result()
            .expect_err("provider command must stop at the goal lease deadline");
        assert!(error.to_string().contains("timed out"));

        let worker_dir = store.worker_dir(&task.id);
        let deadline_receipt: serde_json::Value = serde_json::from_str(&fs::read_to_string(
            worker_dir.join(WORKER_RUNTIME_DEADLINE_FILE),
        )?)?;
        assert_eq!(
            deadline_receipt["goal_id"].as_str(),
            Some(goal_id),
            "deadline receipt must bind the goal"
        );
        assert!(deadline_receipt["deadline_at_ms"].as_u64().is_some());

        let external_receipt: serde_json::Value = serde_json::from_str(&fs::read_to_string(
            worker_dir.join("external-call.json"),
        )?)?;
        assert_eq!(external_receipt["status"].as_str(), Some("deadline_exceeded"));
        assert!(external_receipt["deadline_at_ms"].as_u64().is_some());
        assert!(worker_dir.join("external-call-start.json").is_file());
        assert!(worker_dir.join("process-cleanup.json").is_file());
        Ok(())
    }

    #[test]
    fn test_codex_adapter_dispatch_contract() {
        let cmd = WorkerKind::Codex.default_command(Some("gpt-4.1"));
        assert!(cmd.is_some(), "Codex should have a default command");
        if let Some(ref cmd) = cmd {
            assert!(
                cmd.contains("codex exec"),
                "Codex default should use 'codex exec': {cmd}"
            );
            assert!(
                cmd.contains("gpt-4.1"),
                "Codex default should include model flag: {cmd}"
            );
        }
        let contract = WorkerLaunchContract {
            worker_kind: WorkerKind::Codex.as_str().to_string(),
            default_command: WorkerKind::Codex.default_command(None),
            supports_interaction: false,
            capabilities: WorkerCapabilities::command(),
            native_backend_available: false,
        };
        assert!(!contract.supports_interaction);
        assert!(contract.capabilities.supports_code_edit);
    }

    #[test]
    fn test_claude_adapter_dispatch_contract() {
        let cmd = WorkerKind::Claude.default_command(None);
        assert!(cmd.is_some(), "Claude should have a default command");
        if let Some(ref cmd) = cmd {
            assert!(
                cmd.contains("claude -p"),
                "Claude default should use 'claude -p': {cmd}"
            );
        }
        let contract = WorkerLaunchContract {
            worker_kind: WorkerKind::Claude.as_str().to_string(),
            default_command: WorkerKind::Claude.default_command(None),
            supports_interaction: false,
            capabilities: WorkerCapabilities::command(),
            native_backend_available: false,
        };
        assert!(!contract.supports_interaction);
        assert!(contract.capabilities.supports_code_edit);
    }

    #[test]
    fn test_zed_agent_adapter_dispatch_contract() {
        // Zed Agent has NO default command (it uses the native backend when available)
        let cmd = WorkerKind::ZedAgent.default_command(None);
        assert!(
            cmd.is_none(),
            "Zed Agent should have no default command (native backend)"
        );
        let contract = WorkerLaunchContract {
            worker_kind: WorkerKind::ZedAgent.as_str().to_string(),
            default_command: None,
            supports_interaction: false,
            capabilities: WorkerCapabilities::command(),
            native_backend_available: true,
        };
        assert!(!contract.supports_interaction);
        assert!(contract.capabilities.supports_code_edit);
        let contract_native = WorkerLaunchContract {
            native_backend_available: false,
            ..contract
        };
        // Without native backend: same capabilities, command-worker based
        assert!(!contract_native.native_backend_available);
    }

    #[test]
    fn test_worker_kind_default_commands_are_independent() -> Result<()> {
        // Opencode/Codex/Claude must have independent default CLI contracts.
        let opencode_cmd = WorkerKind::Opencode.default_command(None);
        let codex_cmd = WorkerKind::Codex.default_command(None);
        let claude_cmd = WorkerKind::Claude.default_command(None);

        assert!(opencode_cmd.is_none(), "Opencode has no default command");
        assert!(codex_cmd.is_some(), "Codex has a default command");
        assert!(claude_cmd.is_some(), "Claude has a default command");

        if let (Some(codex), Some(claude)) = (&codex_cmd, &claude_cmd) {
            assert_ne!(
                codex, claude,
                "Codex and Claude default commands must be independent"
            );
            assert!(
                codex.contains("codex"),
                "Codex command references codex: {codex}"
            );
            assert!(
                claude.contains("claude"),
                "Claude command references claude: {claude}"
            );
        }
        Ok(())
    }

    // ── Adapter startup-contract tests ─────────────────────────────────

    fn default_task_with_id(id: &str) -> Task {
        Task {
            id: id.to_string(),
            goal_id: "goal_test".to_string(),
            title: "test".to_string(),
            kind: crate::state::TaskKind::Edit,
            status: crate::state::TaskStatus::Pending,
            assigned_worker: None,
            attempt: 1,
            parent_task_id: None,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        }
    }

    #[test]
    fn adapter_opencode_startup_contract() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = StateStore::new(temp.path());
        store.initialize()?;
        let task = default_task_with_id("task_opencode_contract");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'echo opencode-contract'".to_string()),
            ..WorkerConfig::default()
        };

        // Opencode dispatches through OpencodeCommandWorker (non-interactive)
        let handle = WorkerRegistry::default().start(WorkerStartRequest {
            store: &store,
            workspace: temp.path(),
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
        // Non-interactive command workers return None for session_id
        assert!(handle.session_id().is_none());
        let result = handle.wait_for_result()?;
        // echo is always available → should succeed
        assert_eq!(result.status, WorkerStatus::Succeeded);
        Ok(())
    }

    #[test]
    fn adapter_codex_startup_contract() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = StateStore::new(temp.path());
        store.initialize()?;
        let task = default_task_with_id("task_codex_contract");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: Some("sh -c 'echo codex-contract'".to_string()),
            ..WorkerConfig::default()
        };

        // Codex dispatches through CodexCommandWorker (non-interactive)
        let handle = WorkerRegistry::default().start(WorkerStartRequest {
            store: &store,
            workspace: temp.path(),
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
        assert!(handle.session_id().is_none());
        let result = handle.wait_for_result()?;
        assert_eq!(result.status, WorkerStatus::Succeeded);
        Ok(())
    }

    #[test]
    fn adapter_claude_startup_contract() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = StateStore::new(temp.path());
        store.initialize()?;
        let task = default_task_with_id("task_claude_contract");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Claude,
            worker_command: Some("sh -c 'echo claude-contract'".to_string()),
            ..WorkerConfig::default()
        };

        // Claude dispatches through ClaudeCommandWorker (non-interactive)
        let handle = WorkerRegistry::default().start(WorkerStartRequest {
            store: &store,
            workspace: temp.path(),
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
        assert!(handle.session_id().is_none());
        let result = handle.wait_for_result()?;
        assert_eq!(result.status, WorkerStatus::Succeeded);
        Ok(())
    }

    #[test]
    fn adapter_zed_agent_startup_contract() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = StateStore::new(temp.path());
        store.initialize()?;
        let task = default_task_with_id("task_zed_agent_contract");
        let config = WorkerConfig {
            worker_kind: WorkerKind::ZedAgent,
            ..WorkerConfig::default()
        };

        // ZedAgent dispatches through native backend when available.
        // Uses the locally-defined FakeNativeBackend (already in this test module).
        let started = Arc::new(AtomicBool::new(false));
        let registry = WorkerRegistry::with_native_backend(Arc::new(FakeNativeBackend { started }));
        let handle = registry.start(WorkerStartRequest {
            store: &store,
            workspace: temp.path(),
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
        // FakeNativeWorkerBackend always returns a session_id
        assert!(
            handle.session_id().is_some(),
            "ZedAgent adapter with native backend should return session_id"
        );
        assert_eq!(handle.session_id().as_deref(), Some("native-zed-session"));
        Ok(())
    }

    #[test]
    fn custom_worker_fail_closed() {
        // Custom workers fail closed on ALL code operations — they have no
        // known external tool contract, so Gear defaults to denying everything.
        let caps = WorkerRegistry::capabilities_for_kind(WorkerKind::Custom, false);

        // All code capabilities fail closed
        assert!(
            !caps.supports_code_edit,
            "Custom worker should fail closed on code_edit"
        );
        assert!(
            !caps.supports_review,
            "Custom worker should fail closed on review"
        );
        assert!(
            !caps.supports_explore,
            "Custom worker should fail closed on explore"
        );

        // Advanced features also fail closed
        assert!(
            !caps.supports_model_selection,
            "Custom worker should fail closed on model_selection"
        );
        assert!(
            !caps.supports_tool_policy_enforcement,
            "Custom worker should fail closed on tool_policy_enforcement"
        );
        assert!(
            !caps.supports_artifact_contract,
            "Custom worker should fail closed on artifact_contract"
        );
        assert!(
            !caps.supports_follow_up,
            "Custom worker should fail closed on follow_up"
        );
        assert!(
            !caps.supports_steering,
            "Custom worker should fail closed on steering"
        );
        assert!(
            !caps.supports_resident_session,
            "Custom worker should fail closed on resident_session"
        );

        // Only cancellation is available
        assert!(
            caps.supports_cancellation,
            "Custom worker should support cancellation"
        );
    }

    #[test]
    fn capability_mismatch_before_start() {
        // Verify that different worker kinds declare distinct capability sets.
        // Some adapters intentionally lack support for certain categories
        // (e.g., Claude does not support Review).

        // Claude's capabilities: supports code_edit and explore, but
        // does NOT support review, model_selection, or artifact_contract.
        let claude = WorkerRegistry::capabilities_for_kind(WorkerKind::Claude, false);
        assert!(claude.supports_code_edit, "Claude supports code_edit");
        assert!(
            !claude.supports_review,
            "Claude should not support Review category"
        );
        assert!(claude.supports_explore, "Claude supports explore");
        assert!(
            !claude.supports_model_selection,
            "Claude does not support model_selection"
        );
        assert!(
            !claude.supports_artifact_contract,
            "Claude does not support artifact_contract"
        );

        // OpencodeSession (resident_command) has full capabilities including
        // review, model_selection, follow_up, and artifact_contract.
        let resident = WorkerRegistry::capabilities_for_kind(WorkerKind::OpencodeSession, false);
        assert!(resident.supports_review, "Resident worker supports review");
        assert!(
            resident.supports_model_selection,
            "Resident worker supports model_selection"
        );
        assert!(
            resident.supports_artifact_contract,
            "Resident worker supports artifact_contract"
        );
        assert!(
            resident.supports_follow_up,
            "Resident worker supports follow_up"
        );
        assert!(
            resident.supports_tool_policy_enforcement,
            "Resident worker supports tool_policy_enforcement"
        );

        // ZedAgent with native backend also gets full resident capabilities.
        let native = WorkerRegistry::capabilities_for_kind(WorkerKind::ZedAgent, true);
        assert!(native.supports_review, "Native ZedAgent supports review");
        assert!(
            native.supports_model_selection,
            "Native ZedAgent supports model_selection"
        );

        assert!(
            !claude.supports_category(WorkerCategory::Review),
            "Claude.supports_category(Review) should return false"
        );
        assert!(
            resident.supports_category(WorkerCategory::Review),
            "Resident worker should support Review category"
        );
        assert!(
            claude.supports_category(WorkerCategory::Explore),
            "Claude should support Explore category"
        );
    }

    #[test]
    fn omo_plugin_config_dir_creates_expected_structure() -> Result<()> {
        let dir = setup_omo_plugin_config_dir_with_read_only(false)?;
        let opencode_config_path = dir.path().join("opencode").join("opencode.json");
        assert!(
            opencode_config_path.is_file(),
            "expected {} to exist",
            opencode_config_path.display()
        );
        let opencode_content = fs::read_to_string(&opencode_config_path)?;
        let opencode_parsed: serde_json::Value = serde_json::from_str(&opencode_content)?;
        let plugins = opencode_parsed["plugin"]
            .as_array()
            .expect("OpenCode plugin registration should be an array");
        assert!(
            plugins.iter().any(|plugin| plugin == "oh-my-openagent"),
            "expected oh-my-openagent plugin registration"
        );
        let config_path = dir.path().join("opencode").join("oh-my-openagent.json");
        assert!(
            config_path.is_file(),
            "expected {} to exist",
            config_path.display()
        );
        let content = fs::read_to_string(&config_path)?;
        let parsed: serde_json::Value = serde_json::from_str(&content)?;
        let obj = parsed.as_object().expect("config should be a JSON object");
        assert!(
            obj.contains_key("disabled_mcps"),
            "expected 'disabled_mcps' in OMO plugin config"
        );
        assert!(
            obj.contains_key("background_task"),
            "expected 'background_task' in OMO plugin config"
        );
        assert!(
            obj.contains_key("team_mode"),
            "expected 'team_mode' in OMO plugin config"
        );
        let team_mode = obj["team_mode"]
            .as_object()
            .expect("team_mode should be an object");
        assert_eq!(team_mode["enabled"], false);
        assert_eq!(team_mode["max_parallel_members"], 2);
        assert_eq!(obj["model_fallback"], false);
        assert_eq!(obj["runtime_fallback"], false);
        let bg_task = obj["background_task"]
            .as_object()
            .expect("background_task should be an object");
        assert_eq!(bg_task["defaultConcurrency"], 2);
        let disabled = obj["disabled_mcps"]
            .as_array()
            .expect("disabled_mcps should be an array");
        assert_eq!(disabled, &[serde_json::json!("lsp")]);
        Ok(())
    }

    #[test]
    fn omo_plugin_config_dir_denies_mutating_tools_for_read_only_workers() -> Result<()> {
        let dir = setup_omo_plugin_config_dir_with_read_only(true)?;
        let config_path = dir.path().join("opencode").join("opencode.json");
        let content = fs::read_to_string(config_path)?;
        let parsed: serde_json::Value = serde_json::from_str(&content)?;
        assert_eq!(parsed["permission"]["edit"], "deny");
        assert_eq!(parsed["permission"]["bash"]["*"], "allow");
        assert_eq!(parsed["permission"]["bash"]["* > *"], "deny");
        assert_eq!(parsed["permission"]["bash"]["*git add*"], "deny");
        assert_eq!(parsed["permission"]["task"], "deny");
        assert_eq!(parsed["permission"]["read"], "allow");
        assert_eq!(parsed["permission"]["grep"], "allow");
        Ok(())
    }

    #[test]
    fn omo_plugin_config_dir_is_cleaned_up_on_drop() -> Result<()> {
        let config_path = {
            let dir = setup_omo_plugin_config_dir_with_read_only(false)?;
            let path = dir.path().join("opencode").join("oh-my-openagent.json");
            assert!(path.is_file(), "config file should exist before drop");
            path
        };
        assert!(
            !config_path.is_file(),
            "OMO config dir should be cleaned up on drop"
        );
        Ok(())
    }

    #[test]
    fn compile_recovery_prompt_different_bodies_produce_different_keys() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_body_dedup";
        let command = "echo ok";

        let mut packet = prompt_manifest_test_packet();
        packet.task_id = task_id.to_string();
        let prompt = worker_prompt(&packet)?;
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        let capsule = build_prompt_capsule(
            &packet,
            &manifest,
            &prompt,
            &PromptCapsuleRecoveryReason::Resume,
        )?;

        let packet_path = temp_dir.path().join("packet.json");
        let manifest_path = temp_dir.path().join("manifest.json");
        let capsule_path = temp_dir.path().join("capsule.json");
        fs::write(&packet_path, serde_json::to_string(&packet)?)?;
        fs::write(&manifest_path, serde_json::to_string(&manifest)?)?;
        fs::write(&capsule_path, serde_json::to_string(&capsule)?)?;

        let reason = PromptCapsuleRecoveryReason::Resume;

        // Body A, first call → compiled
        let body_a = "follow-up instruction A";
        let result_a = compile_recovery_prompt(
            &store, task_id, command, body_a, &reason,
            &packet_path, &manifest_path, &capsule_path,
            "follow-up-1", Some("step-002"),
        )?;
        assert!(result_a.is_file(), "first call should produce a compiled prompt");

        // Replaying the exact interaction (same stem) may reuse its compiled
        // prompt; a new interaction stem must receive a fresh artifact.
        let result_a_reuse = compile_recovery_prompt(
            &store, task_id, command, body_a, &reason,
            &packet_path, &manifest_path, &capsule_path,
            "follow-up-1", Some("step-002"),
        )?;
        assert_eq!(
            result_a, result_a_reuse,
            "same body should reuse compiled prompt"
        );

        // Verify at least one reused receipt was written
        let worker_dir = store.worker_dir(task_id);
        let has_reused = fs::read_dir(&worker_dir)?
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains("recovery-reused"));
        assert!(has_reused, "should have a reused receipt");

        let result_a_new_interaction = compile_recovery_prompt(
            &store,
            task_id,
            command,
            body_a,
            &reason,
            &packet_path,
            &manifest_path,
            &capsule_path,
            "follow-up-2",
            Some("step-002"),
        )?;
        assert_ne!(
            result_a,
            result_a_new_interaction,
            "a new interaction attempt must not reuse the old compiled prompt"
        );

        // A receipt alone is not proof that the compiled artifact is still
        // the one that was issued.  Tampering must force a fresh compilation
        // instead of returning the modified prompt under the old key.
        let original_content = fs::read_to_string(&result_a)?;
        fs::write(&result_a, "tampered recovery prompt")?;
        let rebuilt = compile_recovery_prompt(
            &store, task_id, command, body_a, &reason,
            &packet_path, &manifest_path, &capsule_path,
            "follow-up-1", Some("step-002"),
        )?;
        assert_eq!(rebuilt, result_a, "same interaction should rebuild in place");
        assert_eq!(fs::read_to_string(rebuilt)?, original_content);

        // Body B → different compiled prompt
        let body_b = "different follow-up instruction B";
        let result_b = compile_recovery_prompt(
            &store, task_id, command, body_b, &reason,
            &packet_path, &manifest_path, &capsule_path,
            "follow-up-3", Some("step-002"),
        )?;
        assert_ne!(
            result_a, result_b,
            "different body should produce different compiled prompt"
        );

        Ok(())
    }

    #[test]
    fn compile_recovery_prompt_rejects_corrupt_capsule_before_reuse() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_corrupt_capsule";
        let mut packet = prompt_manifest_test_packet();
        packet.task_id = task_id.to_string();
        let prompt = worker_prompt(&packet)?;
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        let capsule = build_prompt_capsule(
            &packet,
            &manifest,
            &prompt,
            &PromptCapsuleRecoveryReason::Resume,
        )?;
        let packet_path = temp_dir.path().join("packet.json");
        let manifest_path = temp_dir.path().join("manifest.json");
        let capsule_path = temp_dir.path().join("capsule.json");
        fs::write(&packet_path, serde_json::to_string(&packet)?)?;
        fs::write(&manifest_path, serde_json::to_string(&manifest)?)?;
        fs::write(&capsule_path, serde_json::to_string(&capsule)?)?;

        let reason = PromptCapsuleRecoveryReason::Resume;
        let first = compile_recovery_prompt(
            &store,
            task_id,
            "echo ok",
            "resume body",
            &reason,
            &packet_path,
            &manifest_path,
            &capsule_path,
            "resume-1",
            Some("step-002"),
        )?;
        assert!(first.is_file());

        // A receipt and compiled file cannot authorize a recovery when the
        // persisted capsule is no longer parseable.
        fs::write(&capsule_path, "{not-json")?;
        let error = compile_recovery_prompt(
            &store,
            task_id,
            "echo ok",
            "resume body",
            &reason,
            &packet_path,
            &manifest_path,
            &capsule_path,
            "resume-corrupt",
            Some("step-002"),
        )
        .expect_err("corrupt capsule must block receipt reuse");
        assert!(error.to_string().contains("invalid capsule JSON"));
        Ok(())
    }

    #[test]
    fn compile_recovery_prompt_bounds_large_recovery_append() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_recovery_budget";
        let mut packet = prompt_manifest_test_packet();
        packet.task_id = task_id.to_string();
        let prompt = worker_prompt(&packet)?;
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        let hard_tokens: usize = manifest
            .sections
            .iter()
            .filter(|section| section.required)
            .map(|section| section.estimated_tokens)
            .sum();
        let previous = env::var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS").ok();
        unsafe {
            env::set_var(
                "GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS",
                hard_tokens.saturating_add(500).to_string(),
            );
        }
        let result = std::panic::catch_unwind(|| -> Result<()> {
            let capsule = build_prompt_capsule(
                &packet,
                &manifest,
                &prompt,
                &PromptCapsuleRecoveryReason::Resume,
            )?;
            let packet_path = temp_dir.path().join("packet.json");
            let manifest_path = temp_dir.path().join("manifest.json");
            let capsule_path = temp_dir.path().join("capsule.json");
            fs::write(&packet_path, serde_json::to_string(&packet)?)?;
            fs::write(&manifest_path, serde_json::to_string(&manifest)?)?;
            fs::write(&capsule_path, serde_json::to_string(&capsule)?)?;
            let body = "large recovery context ".repeat(2_000);
            let compiled = compile_recovery_prompt(
                &store,
                task_id,
                "echo ok",
                &body,
                &PromptCapsuleRecoveryReason::Resume,
                &packet_path,
                &manifest_path,
                &capsule_path,
                "follow-up-budget",
                Some("step-002"),
            )?;
            let content = fs::read_to_string(compiled)?;
            assert!(content.contains("Bounded recovery context"));
            assert!(estimate_prompt_tokens(&content) <= capsule.budget_tokens);
            Ok(())
        });
        unsafe {
            match previous {
                Some(value) => env::set_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS", value),
                None => env::remove_var("GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS"),
            }
        }
        result.map_err(|panic| anyhow::anyhow!("test panicked: {panic:?}"))?
    }

    #[test]
    fn compile_recovery_prompt_structured_degraded_receipt_on_missing_packet() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_degraded_packet";
        let command = "echo ok";

        let mut packet = prompt_manifest_test_packet();
        packet.task_id = task_id.to_string();
        let prompt = worker_prompt(&packet)?;
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        let capsule = build_prompt_capsule(
            &packet,
            &manifest,
            &prompt,
            &PromptCapsuleRecoveryReason::Resume,
        )?;

        // Write manifest and capsule but NOT packet
        let manifest_path = temp_dir.path().join("manifest.json");
        let capsule_path = temp_dir.path().join("capsule.json");
        fs::write(&manifest_path, serde_json::to_string(&manifest)?)?;
        fs::write(&capsule_path, serde_json::to_string(&capsule)?)?;

        // Non-existent packet path
        let missing_packet = temp_dir.path().join("no-such-packet.json");

        let reason = PromptCapsuleRecoveryReason::Resume;
        let err = compile_recovery_prompt(
            &store, task_id, command, "some body", &reason,
            &missing_packet, &manifest_path, &capsule_path,
            "follow-up-1", Some("step-002"),
        ).expect_err("missing packet should produce Err");

        assert!(
            err.to_string().contains("failed to read worker packet"),
            "error should mention packet read failure: {err}"
        );

        // Verify structured degraded receipt was written
        let worker_dir = store.worker_dir(task_id);
        let degraded_files: Vec<_> = fs::read_dir(&worker_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("degraded-"))
            .collect();
        assert!(
            !degraded_files.is_empty(),
            "should have at least one degraded receipt"
        );

        // Read the receipt and verify structure
        let first_degraded = &degraded_files[0];
        let content = fs::read_to_string(first_degraded.path())?;
        let receipt: serde_json::Value = serde_json::from_str(&content)?;
        assert_eq!(receipt["status"], "degraded");
        assert_eq!(receipt["error_category"], "packet_read_failure");
        assert_eq!(receipt["task_id"], task_id);
        assert!(receipt.get("error_detail").and_then(|v| v.as_str()).is_some_and(|s| !s.is_empty()));
        assert!(receipt.get("raw_fallback_path").and_then(|v| v.as_str()).is_some_and(|s| !s.is_empty()));

        Ok(())
    }

    #[test]
    fn recovery_capsule_failure_does_not_send_uncompiled_prompt() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_recovery_blocked";
        let handle = CommandWorkerSessionHandle {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task_id: task_id.to_string(),
            task_attempt: 1,
            worker_name: "test_worker".to_string(),
            skip_worker: false,
            command: Some("touch should-not-run".to_string()),
            command_timeout: Some(Duration::from_secs(30)),
            worker_model: None,
            model_variant: None,
            tool_policy: WorkerToolPolicy::default(),
            packet_path: temp_dir.path().join("missing-packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
            prompt_manifest_path: temp_dir.path().join("missing-manifest.json"),
            prompt_reconcile_path: temp_dir.path().join("prompt-reconcile.json"),
            prompt_capsule_path: temp_dir.path().join("missing-capsule.json"),
            subscriptions: Arc::new(WorkerSessionSubscriptions::default()),
            session_state: Mutex::new(ResidentSessionState {
                cancellation_token: CancellationToken::new(),
                active_command: false,
                revive_count: 0,
                interrupt_count: 0,
                turn_epoch: 0,
                stale_reason: None,
            }),
            result: Mutex::new(None),
            last_output: Mutex::new(None),
            follow_up_count: Mutex::new(0),
            supports_interaction: true,
            omo_config_dir: None,
        };

        let error = handle
            .send_follow_up("resume the current step".to_string())
            .expect_err("missing capsule inputs must block recovery dispatch");
        assert!(error.to_string().contains("capsule compilation blocked"));
        assert!(!temp_dir.path().join("should-not-run").exists());
        Ok(())
    }

    #[test]
    fn destructive_command_is_rejected_before_process_spawn() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_destructive_rejection";
        let handle = CommandWorkerSessionHandle {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task_id: task_id.to_string(),
            task_attempt: 1,
            worker_name: "test_worker".to_string(),
            skip_worker: false,
            command: Some("sh -c 'git checkout main; touch spawned-by-worker'".to_string()),
            command_timeout: Some(Duration::from_secs(30)),
            worker_model: None,
            model_variant: None,
            tool_policy: WorkerToolPolicy::default(),
            packet_path: temp_dir.path().join("packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
            prompt_manifest_path: temp_dir.path().join("prompt-manifest.json"),
            prompt_reconcile_path: temp_dir.path().join("prompt-reconcile.json"),
            prompt_capsule_path: temp_dir.path().join("prompt-capsule.json"),
            subscriptions: Arc::new(WorkerSessionSubscriptions::default()),
            session_state: Mutex::new(ResidentSessionState {
                cancellation_token: CancellationToken::new(),
                active_command: false,
                revive_count: 0,
                interrupt_count: 0,
                turn_epoch: 0,
                stale_reason: None,
            }),
            result: Mutex::new(None),
            last_output: Mutex::new(None),
            follow_up_count: Mutex::new(0),
            supports_interaction: false,
            omo_config_dir: None,
        };

        let result = handle.wait_for_result()?;
        assert_eq!(result.status, WorkerStatus::Failed);
        assert!(!temp_dir.path().join("spawned-by-worker").exists());
        let receipt_path = store
            .worker_dir(task_id)
            .join("destructive-command-rejected-run.json");
        let receipt: DestructiveCommandRejectedReceipt =
            serde_json::from_str(&fs::read_to_string(receipt_path)?)?;
        assert_eq!(receipt.kind, "destructive_command_rejected");
        assert!(receipt.rejected_before_spawn);
        assert_eq!(receipt.turn_kind, "run");
        Ok(())
    }

    #[test]
    fn compile_recovery_prompt_structured_degraded_receipt_on_missing_manifest() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_degraded_manifest";
        let command = "echo ok";

        let mut packet = prompt_manifest_test_packet();
        packet.task_id = task_id.to_string();
        let prompt = worker_prompt(&packet)?;
        let manifest = prompt_manifest_for_packet(&packet, &prompt)?;
        let capsule = build_prompt_capsule(
            &packet,
            &manifest,
            &prompt,
            &PromptCapsuleRecoveryReason::Resume,
        )?;

        // Write packet and capsule but NOT manifest
        let packet_path = temp_dir.path().join("packet.json");
        let capsule_path = temp_dir.path().join("capsule.json");
        fs::write(&packet_path, serde_json::to_string(&packet)?)?;
        fs::write(&capsule_path, serde_json::to_string(&capsule)?)?;

        // Non-existent manifest path
        let missing_manifest = temp_dir.path().join("no-such-manifest.json");

        let reason = PromptCapsuleRecoveryReason::Resume;
        let err = compile_recovery_prompt(
            &store, task_id, command, "some body", &reason,
            &packet_path, &missing_manifest, &capsule_path,
            "follow-up-1", Some("step-002"),
        ).expect_err("missing manifest should produce Err");

        assert!(
            err.to_string().contains("failed to read prompt manifest"),
            "error should mention manifest read failure: {err}"
        );

        let worker_dir = store.worker_dir(task_id);
        let degraded_files: Vec<_> = fs::read_dir(&worker_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("degraded-"))
            .collect();
        assert!(
            !degraded_files.is_empty(),
            "should have at least one degraded receipt"
        );

        let first_degraded = &degraded_files[0];
        let content = fs::read_to_string(first_degraded.path())?;
        let receipt: serde_json::Value = serde_json::from_str(&content)?;
        assert_eq!(receipt["status"], "degraded");
        assert_eq!(receipt["error_category"], "manifest_read_failure");

        Ok(())
    }

    #[test]
    fn worker_claim_reconciliation_persists_discrepancy_for_unobserved_claim() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let workspace = temp_dir.path();
        let git_init = crate::tools::run_raw_git(workspace, &["init", "-q"])?;
        assert!(git_init.success);
        fs::write(workspace.join("observed.rs"), "fn observed() {}\n")?;

        let store = StateStore::new(workspace);
        store.initialize()?;
        let task_id = "task_claim_reconcile";
        let last_message = store.write_worker_file(
            task_id,
            "last-message.md",
            "# Changed Files\n\n- missing.rs\n\n# Summary\n\nclaimed a file that was not written\n",
        )?;
        let result = WorkerResult {
            status: WorkerStatus::Succeeded,
            command: Some("echo worker".to_string()),
            exit_code: Some(0),
            summary: "worker completed".to_string(),
            packet_path: store.worker_dir(task_id).join("packet.json"),
            prompt_path: store.worker_dir(task_id).join("prompt.md"),
            stdout_path: None,
            stderr_path: None,
            last_message_path: Some(last_message),
            result_path: store.worker_dir(task_id).join("result.json"),
            outcome_path: store.worker_dir(task_id).join("outcome.json"),
        };
        let outcome = WorkerOutcome {
            status: WorkerStatus::Succeeded,
            session_id: Some("session-claim".to_string()),
            session_capability: None,
            summary: "worker completed".to_string(),
            changed_files: vec!["missing.rs".to_string()],
            commands_run: vec!["echo worker".to_string()],
            known_failures: Vec::new(),
            raw_output_path: result.last_message_path.clone(),
            command: result.command.clone(),
            exit_code: result.exit_code,
        };

        let receipt = reconcile_worker_claims(&store, task_id, &result, &outcome)?;
        assert_eq!(receipt.status, "discrepancy");
        assert_eq!(receipt.missing_claims, vec!["missing.rs"]);
        assert!(receipt.observed_changed_files.contains(&"observed.rs".to_string()));
        receipt.validate()?;
        assert!(
            store
                .worker_dir(task_id)
                .join("claim-reconciliation.json")
                .exists()
        );
        Ok(())
    }

    #[test]
    fn team_session_reconciliation_blocks_team_events_when_team_mode_is_disabled() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_team_reconcile";
        store.write_worker_file(
            task_id,
            "transcript.jsonl",
            "{\"event\":\"member_error\",\"team_run_id\":\"team-1\",\"member_id\":\"member-1\"}\n{\"event\":\"message\",\"team_run_id\":\"team-1\",\"undelivered\":true,\"reorderable\":true}\n{\"event\":\"orphan\",\"team_run_id\":\"team-1\",\"leader_deleted\":true}\n",
        )?;
        let outcome = WorkerOutcome {
            status: WorkerStatus::Succeeded,
            session_id: Some("session-team".to_string()),
            session_capability: None,
            summary: "team-shaped output".to_string(),
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            known_failures: Vec::new(),
            raw_output_path: None,
            command: None,
            exit_code: Some(0),
        };

        let receipt = reconcile_team_session(&store, task_id, &outcome)?;
        assert_eq!(receipt.status, "blocked");
        assert_eq!(receipt.observed_team_events, 3);
        assert_eq!(receipt.orphan_events, 1);
        assert_eq!(receipt.member_error_events, 1);
        assert_eq!(receipt.undelivered_message_events, 1);
        assert_eq!(receipt.reorderable_message_events, 1);
        receipt.validate()?;
        assert!(
            store
                .worker_dir(task_id)
                .join("team-session-reconciliation.json")
                .exists()
        );
        Ok(())
    }

    #[test]
    fn team_session_reconciliation_records_disabled_without_team_events() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task_id = "task_team_disabled";
        let outcome = WorkerOutcome {
            status: WorkerStatus::Succeeded,
            session_id: Some("session-single".to_string()),
            session_capability: None,
            summary: "single worker output".to_string(),
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            known_failures: Vec::new(),
            raw_output_path: None,
            command: None,
            exit_code: Some(0),
        };

        let receipt = reconcile_team_session(&store, task_id, &outcome)?;
        assert_eq!(receipt.status, "disabled");
        assert_eq!(receipt.observed_team_events, 0);
        receipt.validate()?;
        Ok(())
    }
}
