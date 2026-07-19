use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command as StdCommand, Stdio as StdStdio};
use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use crate::state::{CommandRecord, Scope};

const OUTPUT_LIMIT: usize = 12_000;
// Structured OpenCode worker events need a larger bounded envelope: cutting
// a JSON event in the middle makes planner/executor responses unparsable. The
// limit remains finite to keep long worker sessions from retaining unbounded
// output, while ordinary shell command excerpts keep the smaller limit.
const WORKER_OUTPUT_LIMIT: usize = 64_000;
const PROCESS_RESOURCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const PROCESS_RESOURCE_SAMPLE_LIMIT: usize = 64;
const PROCESS_RESOURCE_SCHEMA_VERSION: u32 = 1;
static COMMAND_OUTPUT_COUNTER: AtomicU64 = AtomicU64::new(1);
static GEAR_RUST_COMMAND_GATE: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    is_cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::SeqCst)
    }

    pub fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.is_cancelled, &other.is_cancelled)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShellCommandResult {
    pub command: String,
    pub exit_code: Option<i32>,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u128,
    #[serde(default)]
    pub stdout_truncated: bool,
    #[serde(default)]
    pub stderr_truncated: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalEffectKind {
    Shell,
    WebFetch,
    Lsp,
    Mcp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalEffectRequest {
    pub kind: ExternalEffectKind,
    pub owner: String,
    pub workspace: PathBuf,
    pub target: String,
    pub deadline_at_ms: Option<u64>,
    pub cancellation_requested: bool,
    pub terminal_session: bool,
    pub redirect_count: usize,
    pub max_redirects: usize,
    pub idempotent: bool,
    pub retry_requested: bool,
    pub require_deadline: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalEffectDecision {
    pub status: String,
    pub reason: String,
    pub retry_allowed: bool,
}

impl ExternalEffectDecision {
    fn admitted(reason: impl Into<String>, retry_allowed: bool) -> Self {
        Self {
            status: "admitted".to_string(),
            reason: reason.into(),
            retry_allowed,
        }
    }

    fn blocked(reason: impl Into<String>) -> Self {
        Self {
            status: "blocked".to_string(),
            reason: reason.into(),
            retry_allowed: false,
        }
    }
}

/// Validate the common admission contract shared by shell, WebFetch, LSP and
/// MCP effects.  The concrete transports remain optional, but they all use
/// the same cancellation/deadline/redirect/replay boundary when present.
pub fn admit_external_effect(
    request: &ExternalEffectRequest,
    now_ms: u64,
) -> ExternalEffectDecision {
    if request.owner.trim().is_empty() {
        return ExternalEffectDecision::blocked("external effect owner is empty");
    }
    if request.target.trim().is_empty() {
        return ExternalEffectDecision::blocked("external effect target is empty");
    }
    if request.cancellation_requested {
        return ExternalEffectDecision::blocked("external effect was cancelled before admission");
    }
    if request.terminal_session {
        return ExternalEffectDecision::blocked("external effect belongs to a terminal session");
    }
    if request.require_deadline
        && request
            .deadline_at_ms
            .is_none_or(|deadline_at_ms| deadline_at_ms <= now_ms)
    {
        return ExternalEffectDecision::blocked("external effect deadline is missing or expired");
    }
    if request.redirect_count > request.max_redirects {
        return ExternalEffectDecision::blocked("external effect redirect limit exceeded");
    }
    if matches!(request.kind, ExternalEffectKind::WebFetch) {
        if !request.target.starts_with("https://") && !request.target.starts_with("http://") {
            return ExternalEffectDecision::blocked("WebFetch target must use http(s)");
        }
        if request.target.contains("..") {
            return ExternalEffectDecision::blocked(
                "WebFetch target contains a path traversal segment",
            );
        }
    } else {
        let workspace = match request.workspace.canonicalize() {
            Ok(workspace) => workspace,
            Err(error) => {
                return ExternalEffectDecision::blocked(format!(
                    "external effect workspace cannot be resolved: {error}"
                ));
            }
        };
        let target_path = Path::new(&request.target);
        if target_path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return ExternalEffectDecision::blocked(
                "external effect target contains a parent-directory escape",
            );
        }
        let target_path = if target_path.is_absolute() {
            target_path.to_path_buf()
        } else {
            workspace.join(target_path)
        };
        let Some(target) = canonicalize_with_missing_tail(&target_path) else {
            return ExternalEffectDecision::blocked(
                "external effect target cannot be resolved for containment",
            );
        };
        if !target.starts_with(&workspace) {
            return ExternalEffectDecision::blocked(
                "external effect target escapes the workspace boundary",
            );
        }
    }
    if request.retry_requested && !request.idempotent {
        return ExternalEffectDecision::blocked(
            "automatic retry is forbidden for a non-idempotent external effect",
        );
    }
    if request.retry_requested {
        ExternalEffectDecision::admitted("idempotent retry admitted", true)
    } else {
        ExternalEffectDecision::admitted("external effect admitted", false)
    }
}

fn canonicalize_with_missing_tail(path: &Path) -> Option<PathBuf> {
    let mut missing_components: Vec<OsString> = Vec::new();
    let mut current = path;
    loop {
        if let Ok(mut canonical) = current.canonicalize() {
            for component in missing_components.iter().rev() {
                canonical.push(component);
            }
            return Some(canonical);
        }
        let component = current.file_name()?.to_os_string();
        missing_components.push(component);
        current = current.parent()?;
    }
}

fn external_effect_kind(request_kind: &str) -> ExternalEffectKind {
    match request_kind.trim().to_ascii_lowercase().as_str() {
        "webfetch" | "web_fetch" | "fetch" => ExternalEffectKind::WebFetch,
        "lsp" => ExternalEffectKind::Lsp,
        "mcp" => ExternalEffectKind::Mcp,
        _ => ExternalEffectKind::Shell,
    }
}

fn external_effect_request(
    workspace: &Path,
    command: &str,
    env: &HashMap<String, String>,
    timeout: Option<Duration>,
    started_at_ms: u64,
) -> ExternalEffectRequest {
    let request_kind = env
        .get("GEARBOX_EXTERNAL_REQUEST_KIND")
        .map(String::as_str)
        .unwrap_or("shell");
    let kind = external_effect_kind(request_kind);
    let deadline_at_ms = timeout.map(|duration| {
        started_at_ms.saturating_add(duration.as_millis().try_into().unwrap_or(u64::MAX))
    });
    let target = env
        .get("GEARBOX_EXTERNAL_TARGET")
        .cloned()
        .unwrap_or_else(|| command.to_string());
    ExternalEffectRequest {
        kind,
        owner: env
            .get("GEARBOX_EXTERNAL_OWNER")
            .cloned()
            .unwrap_or_else(|| "gear-worker".to_string()),
        workspace: workspace.to_path_buf(),
        target,
        deadline_at_ms,
        cancellation_requested: env
            .get("GEARBOX_EXTERNAL_CANCELLED")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true")),
        terminal_session: env
            .get("GEARBOX_EXTERNAL_TERMINAL")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true")),
        redirect_count: env
            .get("GEARBOX_EXTERNAL_REDIRECT_COUNT")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        max_redirects: env
            .get("GEARBOX_EXTERNAL_MAX_REDIRECTS")
            .and_then(|value| value.parse().ok())
            .unwrap_or(5),
        idempotent: env
            .get("GEARBOX_EXTERNAL_IDEMPOTENT")
            .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")),
        retry_requested: env
            .get("GEARBOX_EXTERNAL_RETRY_REQUESTED")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true")),
        // Network/protocol effects must always carry a deadline. Shell
        // commands retain the caller's explicit opt-in because verification
        // commands may intentionally be unbounded; the concrete transport
        // kinds never inherit that permissive default.
        require_deadline: env
            .get("GEARBOX_EXTERNAL_REQUIRE_DEADLINE")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            || !matches!(kind, ExternalEffectKind::Shell),
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn external_transport_command(
    command: &str,
    env: &HashMap<String, String>,
    timeout: Option<Duration>,
) -> Result<Option<String>> {
    let kind = external_effect_kind(
        env.get("GEARBOX_EXTERNAL_REQUEST_KIND")
            .map(String::as_str)
            .unwrap_or("shell"),
    );
    match kind {
        ExternalEffectKind::Shell => Ok(None),
        ExternalEffectKind::WebFetch => {
            let target = env
                .get("GEARBOX_EXTERNAL_TARGET")
                .filter(|target| !target.trim().is_empty())
                .map(String::as_str)
                .unwrap_or(command);
            let max_redirects = env
                .get("GEARBOX_EXTERNAL_MAX_REDIRECTS")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(5);
            let max_time = timeout
                .map(|duration| duration.as_secs_f64().max(0.001).to_string())
                .unwrap_or_else(|| "300".to_string());
            Ok(Some(format!(
                "curl --fail --silent --show-error --location --proto '=http,https' --max-redirs {} --max-time {} -- {}",
                max_redirects,
                shell_quote(&max_time),
                shell_quote(target),
            )))
        }
        ExternalEffectKind::Lsp | ExternalEffectKind::Mcp => {
            let request = env
                .get("GEARBOX_EXTERNAL_PROTOCOL_REQUEST")
                .context("protocol external effect is missing GEARBOX_EXTERNAL_PROTOCOL_REQUEST")?;
            if request.trim().is_empty() {
                bail!("protocol external effect request cannot be empty");
            }
            if matches!(kind, ExternalEffectKind::Lsp) {
                // LSP uses the JSON-RPC stream framing defined by the
                // language-server protocol.  Sending bare JSON happens to
                // work with a permissive test command but leaves a real
                // language server waiting forever for Content-Length.
                let content_length = request.len();
                Ok(Some(format!(
                    "printf 'Content-Length: %s\r\n\r\n%s' {} {} | {}",
                    shell_quote(&content_length.to_string()),
                    shell_quote(request),
                    command,
                )))
            } else {
                // MCP transports exchange newline-delimited JSON rather than
                // LSP's header-framed stream.
                Ok(Some(format!(
                    "printf '%s' {} | {}",
                    shell_quote(request),
                    command,
                )))
            }
        }
    }
}

pub const EXTERNAL_CALL_RECEIPT_SCHEMA_VERSION: u32 = 1;

/// Durable lifecycle evidence for a shell/subprocess call owned by one Gear
/// worker.  The receipt deliberately records retry policy rather than
/// retrying blindly: only callers that explicitly mark an operation
/// idempotent may opt into a retry route.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExternalCallReceipt {
    pub schema_version: u32,
    pub call_id: String,
    pub task_id: String,
    pub owner: String,
    pub workspace: String,
    pub command_hash: String,
    pub request_kind: String,
    #[serde(default)]
    pub target: String,
    pub attempt: u64,
    pub idempotent: bool,
    pub retry_policy: String,
    pub retry_allowed: bool,
    pub started_at_ms: u64,
    pub deadline_at_ms: Option<u64>,
    pub finished_at_ms: u64,
    pub cancellation_requested: bool,
    pub status: String,
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_receipt_path: Option<String>,
    pub receipt_hash: String,
}

impl ExternalCallReceipt {
    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_hash.clear();
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&payload)?)))
    }

    fn seal(mut self) -> Result<Self> {
        self.receipt_hash.clear();
        self.receipt_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != EXTERNAL_CALL_RECEIPT_SCHEMA_VERSION {
            bail!("unsupported external call receipt schema");
        }
        for (field, value) in [
            ("call_id", self.call_id.as_str()),
            ("task_id", self.task_id.as_str()),
            ("owner", self.owner.as_str()),
            ("workspace", self.workspace.as_str()),
            ("command_hash", self.command_hash.as_str()),
            ("request_kind", self.request_kind.as_str()),
            ("target", self.target.as_str()),
            ("retry_policy", self.retry_policy.as_str()),
            ("status", self.status.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("external call receipt {field} cannot be empty");
            }
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("external call receipt hash mismatch");
        }
        if self.retry_allowed && (!self.idempotent || self.retry_policy == "none") {
            bail!("external call receipt grants retry for a non-idempotent or disabled policy");
        }
        if self.finished_at_ms < self.started_at_ms {
            bail!("external call receipt finished_at_ms precedes started_at_ms");
        }
        if let Some(deadline_at_ms) = self.deadline_at_ms {
            if deadline_at_ms < self.started_at_ms {
                bail!("external call receipt deadline precedes started_at_ms");
            }
            if self.status == "deadline_exceeded" && self.finished_at_ms < deadline_at_ms {
                bail!("deadline-exceeded receipt finished before its deadline");
            }
        }
        Ok(())
    }
}

struct ExternalCallContext {
    receipt_path: PathBuf,
    start_receipt_path: PathBuf,
    cleanup_receipt_path: Option<PathBuf>,
    call_id: String,
    task_id: String,
    owner: String,
    workspace: String,
    command_hash: String,
    request_kind: String,
    target: String,
    attempt: u64,
    idempotent: bool,
    retry_policy: String,
    started_at_ms: u64,
    deadline_at_ms: Option<u64>,
}

fn now_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn external_call_context(
    workspace: &Path,
    command: &str,
    env: &HashMap<String, String>,
    timeout: Option<Duration>,
) -> Option<ExternalCallContext> {
    let worker_dir = env
        .get("GEARBOX_WORKER_DIR")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())?;
    let started_at_ms = now_epoch_millis();
    let task_id = env
        .get("GEARBOX_EXTERNAL_TASK_ID")
        .or_else(|| env.get("GEARBOX_WORKER_TASK_ID"))
        .cloned()
        .unwrap_or_else(|| "unknown-task".to_string());
    let owner = env
        .get("GEARBOX_EXTERNAL_OWNER")
        .cloned()
        .unwrap_or_else(|| "gear-worker".to_string());
    let request_kind = env
        .get("GEARBOX_EXTERNAL_REQUEST_KIND")
        .cloned()
        .unwrap_or_else(|| "shell".to_string());
    let target = env
        .get("GEARBOX_EXTERNAL_TARGET")
        .cloned()
        .unwrap_or_else(|| command.to_string());
    let attempt = env
        .get("GEARBOX_EXTERNAL_ATTEMPT")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let idempotent = env
        .get("GEARBOX_EXTERNAL_IDEMPOTENT")
        .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
    let retry_policy = env
        .get("GEARBOX_EXTERNAL_RETRY_POLICY")
        .cloned()
        .unwrap_or_else(|| "none".to_string());
    let receipt_stem = env
        .get("GEARBOX_EXTERNAL_RECEIPT_STEM")
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| "external-call".to_string());
    let command_hash = format!("{:x}", Sha256::digest(command.as_bytes()));
    let call_id = format!(
        "external-{:x}",
        Sha256::digest(
            format!("{task_id}|{owner}|{command_hash}|{attempt}|{started_at_ms}").as_bytes(),
        )
    );
    let deadline_at_ms = timeout.map(|duration| started_at_ms.saturating_add(duration.as_millis() as u64));
    Some(ExternalCallContext {
        receipt_path: worker_dir.join(format!("{receipt_stem}.json")),
        start_receipt_path: worker_dir.join(format!("{receipt_stem}-start.json")),
        cleanup_receipt_path: env
            .get("GEARBOX_WORKER_CLEANUP_RECEIPT")
            .map(PathBuf::from),
        call_id,
        task_id,
        owner,
        workspace: workspace.to_string_lossy().to_string(),
        command_hash,
        request_kind,
        target,
        attempt,
        idempotent,
        retry_policy,
        started_at_ms,
        deadline_at_ms,
    })
}

fn write_external_call_receipt(path: &Path, receipt: &ExternalCallReceipt) -> Result<()> {
    let parent = path
        .parent()
        .context("external call receipt has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create external call receipt directory {}", parent.display()))?;
    let temporary_path = parent.join(format!(".{}.tmp", path.file_name().and_then(|name| name.to_str()).unwrap_or("external-call")));
    let contents = format!("{}\n", serde_json::to_string_pretty(receipt)?);
    fs::write(&temporary_path, contents)
        .with_context(|| format!("failed to write temporary external call receipt {}", temporary_path.display()))?;
    fs::rename(&temporary_path, path)
        .with_context(|| format!("failed to publish external call receipt {}", path.display()))?;
    Ok(())
}

fn persist_external_call_state(
    context: &ExternalCallContext,
    status: &str,
    finished_at_ms: u64,
    cancellation_requested: bool,
    exit_code: Option<i32>,
    error: Option<String>,
) -> Result<()> {
    let receipt = ExternalCallReceipt {
        schema_version: EXTERNAL_CALL_RECEIPT_SCHEMA_VERSION,
        call_id: context.call_id.clone(),
        task_id: context.task_id.clone(),
        owner: context.owner.clone(),
        workspace: context.workspace.clone(),
        command_hash: context.command_hash.clone(),
        request_kind: context.request_kind.clone(),
        target: context.target.clone(),
        attempt: context.attempt,
        idempotent: context.idempotent,
        retry_policy: context.retry_policy.clone(),
        retry_allowed: context.idempotent && context.retry_policy != "none",
        started_at_ms: context.started_at_ms,
        deadline_at_ms: context.deadline_at_ms,
        finished_at_ms,
        cancellation_requested,
        status: status.to_string(),
        exit_code,
        error,
        cleanup_receipt_path: context
            .cleanup_receipt_path
            .as_ref()
            .map(|path| path.to_string_lossy().to_string()),
        receipt_hash: String::new(),
    }
    .seal()?;
    write_external_call_receipt(&context.receipt_path, &receipt)
}

impl ShellCommandResult {
    pub fn record(&self) -> CommandRecord {
        CommandRecord {
            command: self.command.clone(),
            exit_code: self.exit_code,
            success: self.success,
            duration_ms: self.duration_ms,
            stdout_excerpt: truncate(&self.stdout, OUTPUT_LIMIT),
            stderr_excerpt: truncate(&self.stderr, OUTPUT_LIMIT),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DiffSnapshot {
    pub is_git_repo: bool,
    pub status: String,
    pub changed_files: Vec<String>,
    pub diff_hash: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScopeCheck {
    pub forbidden_touches: Vec<String>,
    pub outside_allowed_paths: Vec<String>,
    pub max_files_exceeded: bool,
    pub changed_file_count: usize,
}

/// Structured scope drift information for reviewable soft-scope signals.
///
/// Unlike `ScopeCheck` (which aggregates all violations), `ScopeDrift`
/// records only the worker-relative drift that the runtime should
/// escalate to a review instead of immediately blocking.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScopeDrift {
    /// Paths outside the allowed scope that are new (not in baseline diff).
    pub drifted_paths: Vec<String>,
    /// Human-readable explanation of the drift.
    pub drift_reason: String,
}

/// Baseline-aware scope check.
///
/// Files already present in the baseline (`before_diff`) are excluded from
/// scope drift computation so that pre-existing user dirty diffs are not
/// mis-counted as worker scope violations.
///
/// Hard `forbidden_paths` are still enforced on **all** files (including
/// baseline and worker additions) to preserve the hard safety boundary.
pub fn compute_baseline_aware_scope(
    before_diff: &DiffSnapshot,
    after_diff: &DiffSnapshot,
    scope: &Scope,
) -> (ScopeCheck, ScopeDrift) {
    // Build a set of files that were already changed before the worker started.
    let baseline_set: HashSet<&str> = before_diff
        .changed_files
        .iter()
        .map(String::as_str)
        .collect();

    // Forbidden touches checked against ALL files (hard boundary).
    let forbidden_touches: Vec<String> = after_diff
        .changed_files
        .iter()
        .filter(|path| {
            scope.forbidden_paths.iter().any(|forbidden_path| {
                path == &forbidden_path || path.starts_with(&format!("{forbidden_path}/"))
            })
        })
        .cloned()
        .collect();

    // New files: present in after_diff but not in baseline.
    let new_files: Vec<&str> = after_diff
        .changed_files
        .iter()
        .filter(|path| !baseline_set.contains(path.as_str()))
        .map(String::as_str)
        .collect();

    // Outside-allowed check only on new files (soft drift).
    // Files that already hit a forbidden path are excluded from drift
    // because they are handled by the hard-boundary block.
    let forbidden_set: HashSet<&str> = forbidden_touches.iter().map(String::as_str).collect();
    let outside_allowed_paths: Vec<String> = if scope.allowed_paths.is_empty() {
        Vec::new()
    } else {
        new_files
            .iter()
            .filter(|path| !forbidden_set.contains(*path))
            .filter(|path| {
                !scope
                    .allowed_paths
                    .iter()
                    .any(|allowed_path| path_is_within_allowed_scope(path, allowed_path))
            })
            .map(ToString::to_string)
            .collect()
    };

    let new_file_count = new_files.len();
    let max_files_exceeded = new_file_count > scope.max_files_changed;

    let scope_check = ScopeCheck {
        forbidden_touches,
        outside_allowed_paths: outside_allowed_paths.clone(),
        max_files_exceeded,
        changed_file_count: new_file_count,
    };

    let drift_parts: Vec<String> = {
        let mut parts = Vec::new();
        if !outside_allowed_paths.is_empty() {
            parts.push(format!(
                "{} file(s) outside scope: [{}]",
                outside_allowed_paths.len(),
                outside_allowed_paths.join(", ")
            ));
        }
        if max_files_exceeded {
            parts.push(format!(
                "new file count {} exceeds budget {}",
                new_file_count, scope.max_files_changed
            ));
        }
        parts
    };

    let drift = if drift_parts.is_empty() {
        ScopeDrift::default()
    } else {
        ScopeDrift {
            drifted_paths: outside_allowed_paths,
            drift_reason: drift_parts.join("; "),
        }
    };

    (scope_check, drift)
}

fn path_is_within_allowed_scope(path: &str, allowed_path: &str) -> bool {
    let normalized_allowed_path = allowed_path.trim_end_matches('/');
    normalized_allowed_path.is_empty()
        || normalized_allowed_path == "."
        || path == normalized_allowed_path
        || path.starts_with(&format!("{normalized_allowed_path}/"))
}

pub fn run_shell_command(workspace: &Path, command: &str) -> Result<ShellCommandResult> {
    run_shell_command_with_env(workspace, command, &HashMap::new())
}

pub fn run_shell_command_with_env(
    workspace: &Path,
    command: &str,
    env: &HashMap<String, String>,
) -> Result<ShellCommandResult> {
    run_shell_command_with_env_and_cancellation(workspace, command, env, None)
}

pub fn run_shell_command_with_env_and_cancellation(
    workspace: &Path,
    command: &str,
    env: &HashMap<String, String>,
    cancellation_token: Option<&CancellationToken>,
) -> Result<ShellCommandResult> {
    run_shell_command_with_env_and_cancellation_and_timeout(
        workspace,
        command,
        env,
        cancellation_token,
        None,
    )
}

pub fn run_shell_command_with_env_and_cancellation_and_timeout(
    workspace: &Path,
    command: &str,
    env: &HashMap<String, String>,
    cancellation_token: Option<&CancellationToken>,
    timeout: Option<Duration>,
) -> Result<ShellCommandResult> {
    let context = external_call_context(workspace, command, env, timeout);
    if let Some(context) = context.as_ref() {
        let admission = admit_external_effect(
            &external_effect_request(workspace, command, env, timeout, context.started_at_ms),
            context.started_at_ms,
        );
        if admission.status == "blocked" {
            let reason = format!("external effect admission blocked: {}", admission.reason);
            // A pre-spawn rejection is still a durable external-effect
            // observation.  Persist it before returning so the GUI, review,
            // and recovery paths can distinguish a guarded command from an
            // unrecorded crash or a command that actually ran.
            persist_external_call_state(
                context,
                "blocked",
                now_epoch_millis(),
                cancellation_token.is_some_and(CancellationToken::is_cancelled),
                None,
                Some(reason.clone()),
            )
            .with_context(|| "failed to persist blocked external call receipt")?;
            bail!("{reason}");
        }
        persist_external_call_state(
            context,
            "started",
            context.started_at_ms,
            cancellation_token.is_some_and(CancellationToken::is_cancelled),
            None,
            None,
        )?;
        let start_path = &context.start_receipt_path;
        let final_path = &context.receipt_path;
        if start_path != final_path {
            fs::copy(final_path, start_path).with_context(|| {
                format!(
                    "failed to preserve external call start receipt {}",
                    start_path.display()
                )
            })?;
        }
    }

    let transport_command = external_transport_command(command, env, timeout);
    let result = match transport_command {
        Ok(Some(transport_command)) => run_shell_command_with_env_and_cancellation_and_timeout_inner(
            workspace,
            &transport_command,
            env,
            cancellation_token,
            timeout,
        )
        .map(|mut result| {
            result.command = command.to_string();
            result
        }),
        Ok(None) => run_shell_command_with_env_and_cancellation_and_timeout_inner(
            workspace,
            command,
            env,
            cancellation_token,
            timeout,
        ),
        Err(error) => Err(error),
    };

    let receipt_result = context.as_ref().map(|context| {
        let (status, exit_code, error) = match &result {
            Ok(output) if output.success => ("succeeded", output.exit_code, None),
            Ok(output) => ("failed", output.exit_code, None),
            Err(error) if cancellation_token.is_some_and(CancellationToken::is_cancelled) => (
                "cancelled",
                None,
                Some(error.to_string()),
            ),
            Err(error)
                if timeout.is_some() && error.to_string().contains("timed out") =>
            {
                ("deadline_exceeded", None, Some(error.to_string()))
            }
            Err(error) => ("error", None, Some(error.to_string())),
        };
        persist_external_call_state(
            context,
            status,
            now_epoch_millis(),
            cancellation_token.is_some_and(CancellationToken::is_cancelled),
            exit_code,
            error,
        )
    });

    match (result, receipt_result) {
        (Ok(_result), Some(Err(error))) => Err(error.context("failed to persist external call receipt")),
        (Err(error), Some(Err(receipt_error))) => Err(error.context(format!(
            "failed to persist external call receipt: {receipt_error:#}"
        ))),
        (result, _) => result,
    }
}

fn run_shell_command_with_env_and_cancellation_and_timeout_inner(
    workspace: &Path,
    command: &str,
    env: &HashMap<String, String>,
    cancellation_token: Option<&CancellationToken>,
    timeout: Option<Duration>,
) -> Result<ShellCommandResult> {
    let started_at = Instant::now();
    check_cancelled(cancellation_token, command)?;

    // Gear owns this admission gate. It serializes its own Cargo/Rust
    // commands without inspecting or terminating unrelated IDE processes.
    // The file lease extends that protection to another Gear process using
    // the same workspace, while the in-process mutex avoids needless polling
    // when two workers belong to this process.
    let _rust_command_lease = is_rust_build_command(command)
        .then(|| acquire_rust_command_lease(workspace, cancellation_token, timeout, started_at))
        .transpose()?;

    let stdout_path = command_output_path(workspace, "stdout")?;
    let stderr_path = command_output_path(workspace, "stderr")?;
    let stdout = fs::File::create(&stdout_path)
        .with_context(|| format!("failed to create {}", stdout_path.display()))?;
    let stderr = fs::File::create(&stderr_path)
        .with_context(|| format!("failed to create {}", stderr_path.display()))?;

    let mut process = cancellable_shell_command(command);
    process
        .current_dir(workspace)
        .stdout(StdStdio::from(stdout))
        .stderr(StdStdio::from(stderr));
    for (key, value) in env {
        process.env(key, value);
    }

    let mut child = process
        .spawn()
        .with_context(|| format!("failed to run command `{command}`"))?;
    let mut owned_processes = OwnedProcessTree::new(child.id());
    let cleanup_artifact_path = worker_cleanup_artifact_path(env);
    let resource_artifact_path = worker_resource_artifact_path(env);
    let mut resource_evidence = resource_artifact_path
        .as_ref()
        .map(|_| ProcessResourceEvidence::new(command, env));
    let mut last_resource_sample_at = Instant::now();
    record_process_resource_sample(
        resource_artifact_path.as_deref(),
        &mut resource_evidence,
        &owned_processes,
        "start",
        true,
        &mut last_resource_sample_at,
    )?;
    let monitor_provider_errors = env
        .get("GEARBOX_WORKER_PROVIDER_ERROR_RECOVERY")
        .is_some_and(|value| value == "1");
    let status = loop {
        owned_processes.observe(child.id());
        record_process_resource_sample(
            resource_artifact_path.as_deref(),
            &mut resource_evidence,
            &owned_processes,
            "mid",
            false,
            &mut last_resource_sample_at,
        )?;
        if cancellation_token.is_some_and(CancellationToken::is_cancelled) {
            record_process_resource_sample(
                resource_artifact_path.as_deref(),
                &mut resource_evidence,
                &owned_processes,
                "cancel_requested",
                true,
                &mut last_resource_sample_at,
            )?;
            terminate_command_process_group(
                &mut child,
                &owned_processes,
                cleanup_artifact_path.as_deref(),
                "cancelled",
            )?;
            finalize_process_resource_evidence(
                resource_artifact_path.as_deref(),
                &mut resource_evidence,
                "cancelled",
                Some("cancellation requested"),
            )?;
            cleanup_command_output(&stdout_path);
            cleanup_command_output(&stderr_path);
            bail!("Gear run cancelled while running `{command}`");
        }

        if let Some(timeout) = timeout.filter(|timeout| started_at.elapsed() >= *timeout) {
            record_process_resource_sample(
                resource_artifact_path.as_deref(),
                &mut resource_evidence,
                &owned_processes,
                "deadline_exceeded",
                true,
                &mut last_resource_sample_at,
            )?;
            terminate_command_process_group(
                &mut child,
                &owned_processes,
                cleanup_artifact_path.as_deref(),
                "explicit_timeout",
            )?;
            finalize_process_resource_evidence(
                resource_artifact_path.as_deref(),
                &mut resource_evidence,
                "deadline_exceeded",
                Some("explicit timeout"),
            )?;
            cleanup_command_output(&stdout_path);
            cleanup_command_output(&stderr_path);
            bail!(
                "Gear worker command timed out after {} seconds",
                timeout.as_secs()
            );
        }

        if monitor_provider_errors
            && command_output_indicates_provider_error(&stdout_path, &stderr_path)
        {
            owned_processes.observe(child.id());
            record_process_resource_sample(
                resource_artifact_path.as_deref(),
                &mut resource_evidence,
                &owned_processes,
                "provider_error",
                true,
                &mut last_resource_sample_at,
            )?;
            terminate_command_process_group(
                &mut child,
                &owned_processes,
                cleanup_artifact_path.as_deref(),
                "provider_error",
            )?;
            let status = child
                .wait()
                .with_context(|| format!("failed to reap provider-error command `{command}`"))?;
            finalize_process_resource_evidence(
                resource_artifact_path.as_deref(),
                &mut resource_evidence,
                "provider_error",
                Some("provider error detected in command output"),
            )?;
            break status;
        }

        if let Some(status) = child
            .try_wait()
            .with_context(|| format!("failed to poll command `{command}`"))?
        {
            break status;
        }

        std::thread::sleep(Duration::from_millis(50));
    };

    owned_processes.observe(child.id());
    record_process_resource_sample(
        resource_artifact_path.as_deref(),
        &mut resource_evidence,
        &owned_processes,
        "finish",
        true,
        &mut last_resource_sample_at,
    )?;
    if resource_evidence
        .as_ref()
        .is_none_or(|evidence| evidence.status == "recording")
    {
        finalize_process_resource_evidence(
            resource_artifact_path.as_deref(),
            &mut resource_evidence,
            if status.success() { "succeeded" } else { "failed" },
            (!status.success()).then_some("command exited unsuccessfully"),
        )?;
    }

    let stdout = fs::read_to_string(&stdout_path)
        .with_context(|| format!("failed to read {}", stdout_path.display()))?;
    let stderr = fs::read_to_string(&stderr_path)
        .with_context(|| format!("failed to read {}", stderr_path.display()))?;
    cleanup_command_output(&stdout_path);
    cleanup_command_output(&stderr_path);

    let preserve_worker_tail = env.contains_key("GEARBOX_WORKER_SESSION_ID");
    let stdout_truncated = if preserve_worker_tail {
        stdout.len() > WORKER_OUTPUT_LIMIT
    } else {
        stdout.len() > OUTPUT_LIMIT
    };
    let stderr_truncated = if preserve_worker_tail {
        stderr.len() > WORKER_OUTPUT_LIMIT
    } else {
        stderr.len() > OUTPUT_LIMIT
    };
    Ok(ShellCommandResult {
        command: command.to_string(),
        exit_code: status.code(),
        success: status.success(),
        stdout: if preserve_worker_tail {
            truncate_with_tail(&stdout, WORKER_OUTPUT_LIMIT)
        } else {
            truncate(&stdout, OUTPUT_LIMIT)
        },
        stderr: if preserve_worker_tail {
            truncate_with_tail(&stderr, WORKER_OUTPUT_LIMIT)
        } else {
            truncate(&stderr, OUTPUT_LIMIT)
        },
        duration_ms: started_at.elapsed().as_millis(),
        stdout_truncated,
        stderr_truncated,
    })
}

fn acquire_rust_command_lease(
    workspace: &Path,
    cancellation_token: Option<&CancellationToken>,
    timeout: Option<Duration>,
    started_at: Instant,
) -> Result<RustCommandLease> {
    let lock_directory = workspace.join(".gear").join("locks");
    fs::create_dir_all(&lock_directory).with_context(|| {
        format!(
            "failed to create Rust command lock directory {}",
            lock_directory.display()
        )
    })?;
    let lock_path = lock_directory.join("rust-build.lock");
    let lock_file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open Rust command lock {}", lock_path.display()))?;
    let process_guard = GEAR_RUST_COMMAND_GATE.get_or_init(|| Mutex::new(()));

    loop {
        check_cancelled(cancellation_token, "Rust command admission")?;
        if let Some(timeout) = timeout {
            if started_at.elapsed() >= timeout {
                bail!(
                    "Gear Rust command admission timed out after {} seconds",
                    timeout.as_secs()
                );
            }
        }

        let process_guard = match process_guard.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::sleep(Duration::from_millis(25));
                continue;
            }
        };

        if try_lock_rust_command_file(&lock_file)? {
            return Ok(RustCommandLease {
                _process_guard: process_guard,
                _lock_file: lock_file,
            });
        }

        drop(process_guard);
        std::thread::sleep(Duration::from_millis(25));
    }
}

struct RustCommandLease {
    _process_guard: MutexGuard<'static, ()>,
    _lock_file: fs::File,
}

#[cfg(unix)]
fn try_lock_rust_command_file(lock_file: &fs::File) -> Result<bool> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(true)
    } else if std::io::Error::last_os_error().kind() == std::io::ErrorKind::WouldBlock {
        Ok(false)
    } else {
        Err(std::io::Error::last_os_error()).context("failed to acquire Rust command lock")
    }
}

#[cfg(not(unix))]
fn try_lock_rust_command_file(_lock_file: &fs::File) -> Result<bool> {
    Ok(true)
}

fn is_rust_build_command(command: &str) -> bool {
    let mut tokens = command.split_whitespace();
    let mut token = tokens.next();
    while token.is_some_and(|value| value == "env" || value.contains('=')) {
        token = tokens.next();
    }
    token
        .and_then(|value| value.rsplit('/').next())
        .is_some_and(|value| matches!(value, "cargo" | "rustc" | "rust-analyzer"))
}

pub fn git_snapshot(workspace: &Path) -> Result<DiffSnapshot> {
    let rev_parse = run_raw_git(workspace, &["rev-parse", "--is-inside-work-tree"])?;
    if !rev_parse.success {
        return Ok(DiffSnapshot {
            is_git_repo: false,
            status: rev_parse.stderr,
            changed_files: Vec::new(),
            diff_hash: None,
        });
    }

    let status = run_raw_git(workspace, &["status", "--short"])?;
    let changed_files = parse_status_paths(&status.stdout);

    let diff_hash = {
        let diff_result = run_raw_git(workspace, &["diff"])?;
        if diff_result.success && !diff_result.stdout.trim().is_empty() {
            let normalized = normalize_diff_patch(&diff_result.stdout);
            let mut hasher = Sha256::new();
            hasher.update(normalized.as_bytes());
            Some(format!("{:x}", hasher.finalize()))
        } else {
            None
        }
    };

    Ok(DiffSnapshot {
        is_git_repo: true,
        status: status.stdout,
        changed_files,
        diff_hash,
    })
}

/// Return the repository HEAD used to bind evidence captured in this workspace.
/// Non-Git directories return `None`; callers decide whether that is compatible
/// with the evidence gate they are enforcing.
pub fn git_head_commit(workspace: &Path) -> Result<Option<String>> {
    let repository_check = run_raw_git(workspace, &["rev-parse", "--is-inside-work-tree"])?;
    if !repository_check.success {
        let diagnostic = format!(
            "{}{}",
            repository_check.stdout.trim(),
            repository_check.stderr.trim()
        );
        if diagnostic
            .to_ascii_lowercase()
            .contains("not a git repository")
        {
            return Ok(None);
        }
        bail!(
            "failed to determine whether {} is a Git workspace: {}",
            workspace.display(),
            diagnostic.trim()
        );
    }
    let result = run_raw_git(workspace, &["rev-parse", "HEAD"])?;
    if !result.success {
        bail!(
            "failed to resolve Git HEAD in {}: {}",
            workspace.display(),
            result.stderr.trim()
        );
    }
    if result.stdout.trim().is_empty() {
        return Ok(None);
    }
    let commit = result.stdout.trim();
    if commit.is_empty() {
        return Ok(None);
    }
    Ok(Some(commit.to_string()))
}

/// Strip timestamp noise from `---`/`+++` header lines so that semantically
/// identical diffs produced at different times hash to the same value.
pub fn normalize_diff_patch(patch: &str) -> String {
    patch
        .lines()
        .map(|line| {
            if line.starts_with("--- ") || line.starts_with("+++ ") {
                // Drop everything after the first tab, which is where git puts
                // the timestamp (e.g. "--- a/foo.rs\t2024-01-01 12:00:00.000000000 +0000").
                if let Some(tab_idx) = line.find('\t') {
                    return line[..tab_idx].to_string();
                }
            }
            line.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn check_scope(snapshot: &DiffSnapshot, scope: &Scope) -> ScopeCheck {
    let forbidden_touches = snapshot
        .changed_files
        .iter()
        .filter(|path| {
            scope.forbidden_paths.iter().any(|forbidden_path| {
                path == &forbidden_path || path.starts_with(&format!("{forbidden_path}/"))
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let outside_allowed_paths = if scope.allowed_paths.is_empty() {
        Vec::new()
    } else {
        snapshot
            .changed_files
            .iter()
            .filter(|path| {
                !scope
                    .allowed_paths
                    .iter()
                    .any(|allowed_path| path_is_within_allowed_scope(path, allowed_path))
            })
            .cloned()
            .collect()
    };

    ScopeCheck {
        forbidden_touches,
        outside_allowed_paths,
        max_files_exceeded: snapshot.changed_files.len() > scope.max_files_changed,
        changed_file_count: snapshot.changed_files.len(),
    }
}

pub(crate) fn run_raw_git(workspace: &Path, args: &[&str]) -> Result<ShellCommandResult> {
    let command = format!(
        "git{}",
        args.iter()
            .map(|argument| format!(" {}", shell_quote(argument)))
            .collect::<String>()
    );
    let worker_dir = workspace.join(".gear").join("internal-git");
    fs::create_dir_all(&worker_dir).with_context(|| {
        format!("failed to create internal Git worker directory {}", worker_dir.display())
    })?;
    let packet_path = worker_dir.join("worker-packet.json");
    if !packet_path.exists() {
        fs::write(
            &packet_path,
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "task_id": "internal-git",
                "owner": "gear-runtime",
                "kind": "repository-observation",
            }))?,
        )
        .with_context(|| format!("failed to write {}", packet_path.display()))?;
    }
    let receipt_stem = format!(
        "git-{}",
        COMMAND_OUTPUT_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let mut env = HashMap::new();
    env.insert(
        "GEARBOX_WORKER_DIR".to_string(),
        worker_dir.to_string_lossy().to_string(),
    );
    env.insert(
        "GEARBOX_WORKER_PACKET".to_string(),
        packet_path.to_string_lossy().to_string(),
    );
    env.insert(
        "GEARBOX_EXTERNAL_TASK_ID".to_string(),
        "internal-git".to_string(),
    );
    env.insert(
        "GEARBOX_EXTERNAL_OWNER".to_string(),
        "gear-runtime".to_string(),
    );
    env.insert(
        "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
        "shell".to_string(),
    );
    env.insert(
        "GEARBOX_EXTERNAL_TARGET".to_string(),
        "git".to_string(),
    );
    env.insert("GEARBOX_EXTERNAL_ATTEMPT".to_string(), "0".to_string());
    env.insert("GEARBOX_EXTERNAL_RECEIPT_STEM".to_string(), receipt_stem);
    let mut result = run_shell_command_with_env_and_cancellation_and_timeout(
        workspace,
        &command,
        &env,
        None,
        Some(Duration::from_secs(30)),
    )?;
    result.command = format!("git {}", args.join(" "));
    Ok(result)
}

fn parse_status_paths(status: &str) -> Vec<String> {
    status
        .lines()
        .filter_map(|line| {
            let path = line.get(3..)?.trim();
            let path = path
                .split(" -> ")
                .last()
                .map(str::trim)
                .unwrap_or(path)
                .trim_matches('"');
            if path.is_empty() || path.starts_with(".gear/") || path.starts_with(".gearbox-agent/")
            {
                None
            } else {
                Some(path.to_string())
            }
        })
        .collect()
}

fn cancellable_shell_command(command: &str) -> StdCommand {
    if cfg!(windows) {
        let mut process = StdCommand::new("cmd");
        process.args(["/C", command]);
        process
    } else {
        let mut process = StdCommand::new("sh");
        process.args(["-lc", command]);
        #[cfg(unix)]
        unsafe {
            use std::os::unix::process::CommandExt as _;

            process.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        process
    }
}

fn terminate_command_process_group(
    child: &mut Child,
    owned_processes: &OwnedProcessTree,
    cleanup_artifact_path: Option<&Path>,
    reason: &str,
) -> Result<()> {
    #[cfg(unix)]
    {
        let process_group = child.id() as libc::pid_t;
        signal_command_process_group(process_group, libc::SIGTERM)?;
        owned_processes.signal(libc::SIGTERM)?;
        let mut signals = vec!["SIGTERM".to_string()];
        let graceful_deadline = Instant::now() + Duration::from_millis(100);
        let mut root_reaped = false;
        while Instant::now() < graceful_deadline {
            if child.try_wait()?.is_some() {
                root_reaped = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        if !root_reaped {
            signal_command_process_group(process_group, libc::SIGKILL)?;
            signals.push("SIGKILL".to_string());
            child.wait()?;
            root_reaped = true;
        }
        owned_processes.signal(libc::SIGKILL)?;
        if !signals.iter().any(|signal| signal == "SIGKILL") {
            signals.push("SIGKILL".to_string());
        }
        if let Some(path) = cleanup_artifact_path {
            write_process_cleanup_evidence(
                path,
                owned_processes,
                process_group,
                reason,
                &signals,
                root_reaped,
            )?;
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        if let Err(error) = child.kill()
            && error.kind() != std::io::ErrorKind::InvalidInput
        {
            return Err(error).context("failed to stop worker command");
        }
        owned_processes.signal(0)?;
        if let Err(error) = child.wait()
            && error.kind() != std::io::ErrorKind::InvalidInput
        {
            return Err(error).context("failed to wait for worker command shutdown");
        }
        if let Some(path) = cleanup_artifact_path {
            write_process_cleanup_evidence(
                path,
                owned_processes,
                child.id() as libc::pid_t,
                reason,
                &["kill".to_string()],
                true,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct OwnedProcessTree {
    root_pid: u32,
    #[cfg(target_os = "linux")]
    root_identity: Option<LinuxProcessIdentity>,
    #[cfg(target_os = "linux")]
    descendants: HashMap<libc::pid_t, LinuxProcessIdentity>,
}

impl OwnedProcessTree {
    fn new(root_pid: u32) -> Self {
        let mut tree = Self {
            root_pid,
            ..Default::default()
        };
        tree.observe(root_pid);
        tree
    }

    fn observe(&mut self, root_pid: u32) {
        #[cfg(target_os = "linux")]
        {
            let root_pid = root_pid as libc::pid_t;
            let snapshot = linux_process_snapshot();
            if let Some(identity) = snapshot.get(&root_pid) {
                self.root_identity = Some(identity.clone());
            }
            let mut children_by_parent: HashMap<
                libc::pid_t,
                Vec<(libc::pid_t, LinuxProcessIdentity)>,
            > = HashMap::new();
            for (pid, identity) in snapshot {
                children_by_parent
                    .entry(identity.parent_pid)
                    .or_default()
                    .push((pid, identity));
            }

            let mut pending = vec![root_pid];
            let mut visited = HashSet::from([root_pid]);
            while let Some(parent_pid) = pending.pop() {
                for (pid, identity) in children_by_parent
                    .get(&parent_pid)
                    .into_iter()
                    .flatten()
                    .cloned()
                {
                    if visited.insert(pid) {
                        self.descendants.insert(pid, identity);
                        pending.push(pid);
                    }
                }
            }
        }
    }

    fn signal(&self, signal: libc::c_int) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            if signal == 0 {
                return Ok(());
            }
            for (&pid, identity) in &self.descendants {
                if linux_process_start_time(pid) != Some(identity.start_time) {
                    continue;
                }
                if unsafe { libc::kill(pid, signal) } == -1 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error).with_context(|| {
                            format!("failed to signal owned worker process {pid}")
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn resource_processes(&self) -> Vec<ProcessResourceProcess> {
        #[cfg(target_os = "linux")]
        {
            let mut processes = Vec::with_capacity(self.descendants.len() + 1);
            if let Some(identity) = self.root_identity.as_ref() {
                if linux_process_start_time(self.root_pid as libc::pid_t)
                    == Some(identity.start_time)
                {
                    processes.push(process_resource_process(
                        self.root_pid as libc::pid_t,
                        identity,
                    ));
                }
            }
            for (&pid, identity) in &self.descendants {
                if linux_process_start_time(pid) == Some(identity.start_time) {
                    processes.push(process_resource_process(pid, identity));
                }
            }
            processes.sort_by_key(|process| process.pid);
            processes
        }

        #[cfg(not(target_os = "linux"))]
        {
            vec![ProcessResourceProcess {
                pid: self.root_pid,
                parent_pid: None,
                process_group: None,
                session_id: None,
                start_time: None,
                rss_bytes: None,
                command: None,
                ownership: "worker_process_tree".to_string(),
            }]
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ProcessCleanupEvidence {
    schema_version: u32,
    reason: String,
    root_pid: u32,
    process_group: i32,
    session_id: Option<i32>,
    root_start_time: Option<u64>,
    owned_descendants: Vec<ProcessIdentityEvidence>,
    signals: Vec<String>,
    root_reaped: bool,
    remaining_owned_pids: Vec<u32>,
    platform: String,
    recorded_at: String,
}

#[derive(Clone, Debug, Serialize)]
struct ProcessIdentityEvidence {
    pid: u32,
    start_time: u64,
    process_group: Option<i32>,
    session_id: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProcessResourceEvidence {
    schema_version: u32,
    mechanism_id: String,
    status: String,
    task_id: String,
    owner: String,
    attempt: u64,
    command_hash: String,
    #[serde(default)]
    samples: Vec<ProcessResourceSample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<String>,
    recorded_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProcessResourceSample {
    phase: String,
    recorded_at: String,
    #[serde(default)]
    processes: Vec<ProcessResourceProcess>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProcessResourceProcess {
    pid: u32,
    parent_pid: Option<i32>,
    process_group: Option<i32>,
    session_id: Option<i32>,
    start_time: Option<u64>,
    rss_bytes: Option<u64>,
    command: Option<String>,
    ownership: String,
}

impl ProcessResourceEvidence {
    fn new(command: &str, env: &HashMap<String, String>) -> Self {
        Self {
            schema_version: PROCESS_RESOURCE_SCHEMA_VERSION,
            mechanism_id: "owned_process_resource_sampling".to_string(),
            status: "recording".to_string(),
            task_id: env
                .get("GEARBOX_WORKER_TASK_ID")
                .or_else(|| env.get("GEARBOX_EXTERNAL_TASK_ID"))
                .cloned()
                .unwrap_or_else(|| "unknown-task".to_string()),
            owner: env
                .get("GEARBOX_EXTERNAL_OWNER")
                .cloned()
                .unwrap_or_else(|| "gear-worker".to_string()),
            attempt: env
                .get("GEARBOX_EXTERNAL_ATTEMPT")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
            command_hash: format!("{:x}", Sha256::digest(command.as_bytes())),
            samples: Vec::new(),
            failure: None,
            recorded_at: crate::state::timestamp(),
        }
    }

    fn push_sample(&mut self, sample: ProcessResourceSample) {
        self.samples.push(sample);
        if self.samples.len() > PROCESS_RESOURCE_SAMPLE_LIMIT {
            if self
                .samples
                .first()
                .is_some_and(|sample| sample.phase == "start")
            {
                self.samples.remove(1);
            } else {
                self.samples.remove(0);
            }
        }
        self.recorded_at = crate::state::timestamp();
    }
}

fn record_process_resource_sample(
    path: Option<&Path>,
    evidence: &mut Option<ProcessResourceEvidence>,
    owned_processes: &OwnedProcessTree,
    phase: &str,
    force: bool,
    last_sample_at: &mut Instant,
) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let Some(evidence) = evidence.as_mut() else {
        return Ok(());
    };
    if !force && last_sample_at.elapsed() < PROCESS_RESOURCE_SAMPLE_INTERVAL {
        return Ok(());
    }
    evidence.push_sample(ProcessResourceSample {
        phase: phase.to_string(),
        recorded_at: crate::state::timestamp(),
        processes: owned_processes.resource_processes(),
    });
    write_process_resource_evidence(path, evidence)?;
    *last_sample_at = Instant::now();
    Ok(())
}

fn finalize_process_resource_evidence(
    path: Option<&Path>,
    evidence: &mut Option<ProcessResourceEvidence>,
    status: &str,
    failure: Option<&str>,
) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let Some(evidence) = evidence.as_mut() else {
        return Ok(());
    };
    evidence.status = status.to_string();
    evidence.failure = failure.map(str::to_string);
    evidence.recorded_at = crate::state::timestamp();
    write_process_resource_evidence(path, evidence)
}

fn write_process_resource_evidence(path: &Path, evidence: &ProcessResourceEvidence) -> Result<()> {
    let parent = path
        .parent()
        .context("process resource evidence has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create process resource evidence directory {}",
            parent.display()
        )
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("process-resources.json");
    let temporary_path = parent.join(format!(".{file_name}.tmp"));
    let contents = format!(
        "{}\n",
        serde_json::to_string_pretty(evidence)
            .context("failed to serialize process resource evidence")?
    );
    fs::write(&temporary_path, contents).with_context(|| {
        format!(
            "failed to write temporary process resource evidence {}",
            temporary_path.display()
        )
    })?;
    fs::rename(&temporary_path, path).with_context(|| {
        format!(
            "failed to publish process resource evidence {}",
            path.display()
        )
    })?;
    Ok(())
}

fn write_process_cleanup_evidence(
    path: &Path,
    owned_processes: &OwnedProcessTree,
    process_group: libc::pid_t,
    reason: &str,
    signals: &[String],
    root_reaped: bool,
) -> Result<()> {
    #[cfg(target_os = "linux")]
    let owned_descendants = owned_processes
        .descendants
        .iter()
        .map(|(&pid, identity)| ProcessIdentityEvidence {
            pid: pid as u32,
            start_time: identity.start_time,
            process_group: Some(identity.process_group),
            session_id: Some(identity.session_id),
        })
        .collect::<Vec<_>>();
    #[cfg(not(target_os = "linux"))]
    let owned_descendants = Vec::new();

    #[cfg(target_os = "linux")]
    let remaining_owned_pids = {
        // SIGKILL is asynchronous for detached descendants. Give the kernel
        // a short bounded window to reap them before freezing the receipt, so
        // a process that disappears immediately after the first snapshot is
        // not falsely reported as an orphan.
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            let remaining = owned_processes
                .descendants
                .iter()
                .filter_map(|(&pid, identity)| {
                    (linux_process_start_time(pid) == Some(identity.start_time))
                        .then_some(pid as u32)
                })
                .collect::<Vec<_>>();
            if remaining.is_empty() || Instant::now() >= deadline {
                break remaining;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    #[cfg(not(target_os = "linux"))]
    let remaining_owned_pids = Vec::new();

    let evidence = ProcessCleanupEvidence {
        schema_version: 1,
        reason: reason.to_string(),
        root_pid: owned_processes.root_pid,
        process_group: process_group as i32,
        #[cfg(target_os = "linux")]
        session_id: owned_processes.root_identity.as_ref().map(|identity| identity.session_id),
        #[cfg(not(target_os = "linux"))]
        session_id: None,
        #[cfg(target_os = "linux")]
        root_start_time: owned_processes
            .root_identity
            .as_ref()
            .map(|identity| identity.start_time),
        #[cfg(not(target_os = "linux"))]
        root_start_time: None,
        owned_descendants,
        signals: signals.to_vec(),
        root_reaped,
        remaining_owned_pids,
        platform: std::env::consts::OS.to_string(),
        recorded_at: crate::state::timestamp(),
    };
    let contents = format!(
        "{}\n",
        serde_json::to_string_pretty(&evidence).context("failed to serialize process cleanup evidence")?
    );
    fs::write(path, contents)
        .with_context(|| format!("failed to write process cleanup evidence {}", path.display()))?;
    Ok(())
}

fn worker_cleanup_artifact_path(env: &HashMap<String, String>) -> Option<PathBuf> {
    let packet_path = env.get("GEARBOX_WORKER_PACKET")?;
    Path::new(packet_path)
        .parent()
        .map(|worker_directory| worker_directory.join("process-cleanup.json"))
}

fn worker_resource_artifact_path(env: &HashMap<String, String>) -> Option<PathBuf> {
    let packet_path = env.get("GEARBOX_WORKER_PACKET")?;
    let worker_directory = Path::new(packet_path).parent()?;
    let stem = env
        .get("GEARBOX_EXTERNAL_RECEIPT_STEM")
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .or_else(|| {
            env.get("GEARBOX_EXTERNAL_ATTEMPT")
                .filter(|value| !value.trim().is_empty())
                .map(|attempt| format!("attempt-{attempt}"))
        });
    Some(match stem {
        Some(stem) => worker_directory.join(format!("process-resources-{stem}.json")),
        None => worker_directory.join("process-resources.json"),
    })
}

#[cfg(target_os = "linux")]
fn process_resource_process(
    pid: libc::pid_t,
    identity: &LinuxProcessIdentity,
) -> ProcessResourceProcess {
    ProcessResourceProcess {
        pid: pid as u32,
        parent_pid: Some(identity.parent_pid),
        process_group: Some(identity.process_group),
        session_id: Some(identity.session_id),
        start_time: Some(identity.start_time),
        rss_bytes: linux_process_rss_bytes(pid),
        command: linux_process_command(pid),
        ownership: "worker_process_tree".to_string(),
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct LinuxProcessIdentity {
    parent_pid: libc::pid_t,
    process_group: libc::pid_t,
    session_id: libc::pid_t,
    start_time: u64,
}

#[cfg(target_os = "linux")]
fn linux_process_snapshot() -> HashMap<libc::pid_t, LinuxProcessIdentity> {
    let mut snapshot = HashMap::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return snapshot;
    };
    for entry in entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<libc::pid_t>() else {
            continue;
        };
        let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let Some(close_paren) = stat.rfind(')') else {
            continue;
        };
        let fields = stat[close_paren + 1..].split_whitespace().collect::<Vec<_>>();
        let (Some(parent_pid), Some(process_group), Some(session_id), Some(start_time)) =
            (fields.get(1), fields.get(2), fields.get(3), fields.get(19))
        else {
            continue;
        };
        let (Ok(parent_pid), Ok(process_group), Ok(session_id), Ok(start_time)) = (
            parent_pid.parse::<libc::pid_t>(),
            process_group.parse::<libc::pid_t>(),
            session_id.parse::<libc::pid_t>(),
            start_time.parse::<u64>(),
        ) else {
            continue;
        };
        snapshot.insert(
            pid,
            LinuxProcessIdentity {
                parent_pid,
                process_group,
                session_id,
                start_time,
            },
        );
    }
    snapshot
}

#[cfg(target_os = "linux")]
fn linux_process_start_time(pid: libc::pid_t) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close_paren = stat.rfind(')')?;
    stat[close_paren + 1..]
        .split_whitespace()
        .nth(19)
        .and_then(|value| value.parse::<u64>().ok())
}

#[cfg(target_os = "linux")]
fn linux_process_rss_bytes(pid: libc::pid_t) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?.trim();
        let kilobytes = value.strip_suffix(" kB")?.trim().parse::<u64>().ok()?;
        Some(kilobytes.saturating_mul(1024))
    })
}

#[cfg(target_os = "linux")]
fn linux_process_command(pid: libc::pid_t) -> Option<String> {
    fs::read_to_string(format!("/proc/{pid}/comm"))
        .ok()
        .map(|command| command.trim().to_string())
        .filter(|command| !command.is_empty())
}

#[cfg(unix)]
fn signal_command_process_group(process_group: libc::pid_t, signal: libc::c_int) -> Result<()> {
    if unsafe { libc::killpg(process_group, signal) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(error).context("failed to signal worker command process group")
}

fn command_output_path(workspace: &Path, stream: &str) -> Result<PathBuf> {
    let output_dir = workspace.join(".gear").join("tmp");
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;
    let sequence = COMMAND_OUTPUT_COUNTER.fetch_add(1, Ordering::SeqCst);
    Ok(output_dir.join(format!(
        "command-{}-{sequence}-{stream}.log",
        std::process::id()
    )))
}

fn check_cancelled(cancellation_token: Option<&CancellationToken>, command: &str) -> Result<()> {
    if cancellation_token.is_some_and(CancellationToken::is_cancelled) {
        bail!("Gear run cancelled before running `{command}`");
    }
    Ok(())
}

fn command_output_indicates_provider_error(stdout_path: &Path, stderr_path: &Path) -> bool {
    let patterns = [
        "rate limit",
        "rate-limit",
        "too many requests",
        "429",
        "quota exceeded",
        "usage quota",
        "free usage",
        "limit exhausted",
        "cooling down",
        "service unavailable",
        "temporarily unavailable",
        "overloaded",
        "model not found",
        "model unavailable",
        "provider error",
        "upstream error",
        "connection reset",
        "deadline exceeded",
        "context length",
    ];

    // OpenCode streams model text as JSON events on stdout.  The model may
    // quote these phrases in a plan's constraints or risks, which is not a
    // provider failure.  Stderr remains the process/provider diagnostic
    // channel and can be scanned as a whole; stdout is limited to explicit
    // error-shaped lines or structured error events.
    let stderr = fs::read_to_string(stderr_path).unwrap_or_default();
    if patterns
        .iter()
        .any(|pattern| stderr.to_ascii_lowercase().contains(pattern))
    {
        return true;
    }

    let stdout = fs::read_to_string(stdout_path).unwrap_or_default();
    stdout.lines().map(str::trim).filter(|line| !line.is_empty()).any(|line| {
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
        (is_json_error || is_explicit_text_error)
            && patterns.iter().any(|pattern| normalized.contains(pattern))
    })
}

fn cleanup_command_output(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => eprintln!("failed to remove {}: {error}", path.display()),
    }
}

pub fn truncate(input: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for character in input.chars().take(max_chars) {
        output.push(character);
    }
    if output.len() < input.len() {
        output.push_str("\n[gearbox-agent output truncated]");
    }
    output
}

pub fn truncate_with_tail(input: &str, max_chars: usize) -> String {
    let character_count = input.chars().count();
    if character_count <= max_chars {
        return input.to_string();
    }

    const MARKER: &str = "\n[gearbox-agent output truncated]\n";
    let marker_length = MARKER.chars().count();
    if max_chars <= marker_length {
        return input.chars().take(max_chars).collect();
    }

    let retained = max_chars - marker_length;
    // Worker protocols put the final assistant receipt at the end of stdout.
    // Keep a small head for diagnostics and most of the bounded budget for
    // that final event so a JSON line is less likely to be cut in half.
    let prefix_length = (retained / 4).max(6).min(retained);
    let suffix_length = retained - prefix_length;
    let prefix = input.chars().take(prefix_length).collect::<String>();
    let suffix = input
        .chars()
        .rev()
        .take(suffix_length)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{prefix}{MARKER}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_git_status_paths() {
        let paths = parse_status_paths(
            " M src/main.rs\n?? .gearbox-agent/events/x.jsonl\nR  old.rs -> new.rs\n",
        );

        assert_eq!(paths, vec!["src/main.rs".to_string(), "new.rs".to_string()]);
    }

    #[test]
    fn truncate_with_tail_preserves_bounded_head_and_tail() {
        let output = truncate_with_tail(
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
            50,
        );
        assert!(output.contains("012345"), "head should remain available");
        assert!(output.contains("UVWXYZ"), "tail should remain available");
        assert!(output.contains("[gearbox-agent output truncated]"));
        assert!(output.chars().count() <= 50);
    }

    #[test]
    fn shell_command_result_records_output_truncation() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let result = run_shell_command(workspace.path(), "printf '%13000s' x")?;
        assert!(result.success);
        assert!(result.stdout_truncated);
        assert!(!result.stderr_truncated);
        assert!(result.stdout.contains("[gearbox-agent output truncated]"));
        Ok(())
    }

    #[test]
    fn git_head_commit_distinguishes_repository_and_non_repository() {
        let repository = git_head_commit(Path::new(env!("CARGO_MANIFEST_DIR")))
            .expect("repository HEAD lookup should succeed")
            .expect("gearbox_agent should be inside a Git repository");
        assert!(repository.len() >= 7);
        assert!(
            repository
                .chars()
                .all(|character| character.is_ascii_hexdigit())
        );

        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        assert_eq!(
            git_head_commit(temp_dir.path()).expect("non-Git lookup should not error"),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn internal_git_observation_keeps_owned_receipts_and_resource_samples() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let init = run_raw_git(workspace.path(), &["init", "-q"])?;
        assert!(init.success);
        let status = run_raw_git(workspace.path(), &["status", "--short"])?;
        assert!(status.success);
        let internal_dir = workspace.path().join(".gear/internal-git");
        let receipt_count = fs::read_dir(&internal_dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("git-")
                    && entry.file_name().to_string_lossy().ends_with(".json")
            })
            .count();
        assert!(receipt_count >= 2, "internal Git receipts should be durable");
        let resource_receipt = fs::read_dir(&internal_dir)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("process-resources-git-") && name.ends_with(".json"))
            })
            .context("internal Git process resource receipt should exist")?;
        let resource: ProcessResourceEvidence =
            serde_json::from_slice(&fs::read(resource_receipt)?)?;
        assert_eq!(resource.status, "succeeded");
        assert!(resource
            .samples
            .iter()
            .any(|sample| sample.phase == "start"));
        assert!(resource
            .samples
            .iter()
            .any(|sample| sample.phase == "finish"));
        Ok(())
    }

    #[test]
    fn checks_allowed_paths() {
        let snapshot = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["src/main.rs".to_string(), "README.md".to_string()],
            diff_hash: None,
        };
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 10);

        let check = check_scope(&snapshot, &scope);

        assert_eq!(check.outside_allowed_paths, vec!["README.md".to_string()]);
    }

    #[test]
    fn allowed_dot_scope_includes_workspace_root_and_runtime_artifacts() {
        let snapshot = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["LIVE_MARKER.txt".to_string(), ".gear/events/run.json".to_string()],
            diff_hash: None,
        };
        let scope = Scope::new(vec![".".to_string()], vec![".omo".to_string()], 10);

        let check = check_scope(&snapshot, &scope);

        assert!(check.outside_allowed_paths.is_empty());
        assert!(check.forbidden_touches.is_empty());
    }

    #[test]
    fn baseline_aware_scope_ignores_dirty_baseline_files() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["Cargo.lock".to_string(), "README.md".to_string()],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec![
                "Cargo.lock".to_string(),
                "README.md".to_string(),
                "src/main.rs".to_string(),
            ],
            diff_hash: None,
        };
        let scope = Scope::new(vec!["src".to_string()], vec![".omo".to_string()], 10);
        let (check, drift) = compute_baseline_aware_scope(&before, &after, &scope);

        // Baseline files (Cargo.lock, README.md) should not count as drift.
        assert!(
            check.outside_allowed_paths.is_empty(),
            "baseline files should not appear in outside_allowed_paths: {:?}",
            check.outside_allowed_paths
        );
        // Only new file (src/main.rs) is counted.
        assert_eq!(check.changed_file_count, 1);
        // No forbidden touches.
        assert!(check.forbidden_touches.is_empty());
        // No drift because new file is inside allowed paths.
        assert!(drift.drifted_paths.is_empty());
        assert!(drift.drift_reason.is_empty());
    }

    #[test]
    fn baseline_aware_scope_detects_drift_on_new_outside_files() {
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
                "new_file.py".to_string(),
                "Cargo.toml".to_string(),
            ],
            diff_hash: None,
        };
        let scope = Scope::new(
            vec!["src".to_string(), "Cargo.toml".to_string()],
            vec![".omo".to_string()],
            10,
        );
        let (check, drift) = compute_baseline_aware_scope(&before, &after, &scope);

        // new_file.py is new and outside allowed paths.
        assert_eq!(check.outside_allowed_paths, vec!["new_file.py".to_string()]);
        assert_eq!(check.changed_file_count, 2);
        assert!(!check.max_files_exceeded);
        // Drift should have the outside path.
        assert_eq!(drift.drifted_paths, vec!["new_file.py".to_string()]);
        assert!(!drift.drift_reason.is_empty());
    }

    #[test]
    fn baseline_aware_scope_hard_boundary_still_blocks() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["src/lib.rs".to_string()],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["src/lib.rs".to_string(), ".omo/config.json".to_string()],
            diff_hash: None,
        };
        let scope = Scope::new(vec!["src".to_string()], vec![".omo".to_string()], 10);
        let (check, _) = compute_baseline_aware_scope(&before, &after, &scope);

        // .omo/config.json is a forbidden path touch (hard boundary).
        assert_eq!(
            check.forbidden_touches,
            vec![".omo/config.json".to_string()]
        );
        // outside_allowed_paths should NOT include baseline file src/lib.rs.
        assert!(check.outside_allowed_paths.is_empty());
    }

    #[test]
    fn baseline_aware_scope_exceeded_file_budget() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["existing.rs".to_string()],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec![
                "existing.rs".to_string(),
                "a.rs".to_string(),
                "b.rs".to_string(),
                "c.rs".to_string(),
            ],
            diff_hash: None,
        };
        // max_files_changed = 2, but only 3 new files from baseline.
        let scope = Scope::new(Vec::new(), Vec::new(), 2);
        let (check, drift) = compute_baseline_aware_scope(&before, &after, &scope);

        assert!(check.max_files_exceeded);
        assert_eq!(check.changed_file_count, 3);
        assert!(drift.drift_reason.contains("exceeds budget"));
    }

    #[test]
    fn baseline_aware_scope_no_baseline_no_change() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec![],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["src/main.rs".to_string()],
            diff_hash: None,
        };
        let scope = Scope::new(vec!["src".to_string()], vec![".omo".to_string()], 10);
        let (check, drift) = compute_baseline_aware_scope(&before, &after, &scope);

        assert!(check.forbidden_touches.is_empty());
        assert!(check.outside_allowed_paths.is_empty());
        assert!(!check.max_files_exceeded);
        assert_eq!(check.changed_file_count, 1);
        assert!(drift.drifted_paths.is_empty());
    }

    #[test]
    fn rust_command_gate_only_matches_owned_rust_build_tokens() {
        assert!(is_rust_build_command("cargo test -p gearbox_agent"));
        assert!(is_rust_build_command(
            "env CARGO_BUILD_JOBS=1 rustc src/main.rs"
        ));
        assert!(!is_rust_build_command("echo cargo"));
        assert!(!is_rust_build_command("python build.py"));
    }

    #[test]
    fn cancelled_command_returns_error_before_spawn() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let cancellation_token = CancellationToken::new();
        cancellation_token.cancel();

        let error = run_shell_command_with_env_and_cancellation(
            temp_dir.path(),
            "echo unreachable",
            &HashMap::new(),
            Some(&cancellation_token),
        )
        .expect_err("command should be cancelled");

        assert!(
            error.to_string().contains("Gear run cancelled"),
            "{error:#}"
        );
    }

    #[test]
    fn timed_out_command_returns_a_stable_error() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let error = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "sleep 5",
            &HashMap::new(),
            None,
            Some(Duration::from_millis(20)),
        )
        .expect_err("command should time out");

        assert_eq!(
            error.to_string(),
            "Gear worker command timed out after 0 seconds"
        );
    }

    #[cfg(unix)]
    #[test]
    fn provider_error_output_terminates_worker_process_group_without_timeout() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let worker_dir = temp_dir.path().join(".gear").join("workers").join("task");
        fs::create_dir_all(&worker_dir).expect("worker evidence directory should exist");
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_PROVIDER_ERROR_RECOVERY".to_string(),
            "1".to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_PACKET".to_string(),
            worker_dir.join("packet.json").to_string_lossy().to_string(),
        );
        let started_at = Instant::now();
        let result = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "printf 'rate limit exceeded' >&2; sleep 5",
            &env,
            None,
            None,
        )
        .expect("provider error should return a failed result, not a timeout");

        assert!(!result.success);
        assert!(result.stderr.contains("rate limit exceeded"));
        assert!(
            started_at.elapsed() < Duration::from_secs(2),
            "provider error should reap the child promptly"
        );
        let cleanup: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("process-cleanup.json"))
                .expect("provider cleanup evidence should be persisted"),
        )
        .expect("provider cleanup evidence should be valid JSON");
        assert_eq!(cleanup["reason"], "provider_error");
        assert_eq!(cleanup["root_reaped"], true);
        assert!(cleanup["remaining_owned_pids"].as_array().is_some_and(Vec::is_empty));
        let resources: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("process-resources.json"))
                .expect("process resource evidence should be persisted"),
        )
        .expect("process resource evidence should be valid JSON");
        assert_eq!(resources["schema_version"], 1);
        assert_eq!(resources["mechanism_id"], "owned_process_resource_sampling");
        assert_eq!(resources["status"], "provider_error");
        let samples = resources["samples"]
            .as_array()
            .expect("resource samples should be an array");
        assert!(samples.iter().any(|sample| sample["phase"] == "start"));
        assert!(samples.iter().any(|sample| sample["phase"] == "finish"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn successful_worker_records_owned_process_resource_samples() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let worker_dir = temp_dir.path().join(".gear").join("workers").join("task");
        fs::create_dir_all(&worker_dir).expect("worker evidence directory should exist");
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_PACKET".to_string(),
            worker_dir.join("packet.json").to_string_lossy().to_string(),
        );
        env.insert("GEARBOX_WORKER_TASK_ID".to_string(), "task".to_string());
        env.insert("GEARBOX_EXTERNAL_OWNER".to_string(), "executor".to_string());
        env.insert("GEARBOX_EXTERNAL_ATTEMPT".to_string(), "2".to_string());

        let result = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "sleep 0.35",
            &env,
            None,
            Some(Duration::from_secs(2)),
        )
        .expect("worker command should succeed");
        assert!(result.success);
        let resources: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("process-resources-attempt-2.json"))
                .expect("process resource evidence should be persisted"),
        )
        .expect("process resource evidence should be valid JSON");
        assert_eq!(resources["status"], "succeeded");
        assert_eq!(resources["task_id"], "task");
        assert_eq!(resources["owner"], "executor");
        assert_eq!(resources["attempt"], 2);
        let samples = resources["samples"]
            .as_array()
            .expect("resource samples should be an array");
        assert!(samples.iter().any(|sample| sample["phase"] == "start"));
        assert!(samples.iter().any(|sample| sample["phase"] == "mid"));
        assert!(samples.iter().any(|sample| sample["phase"] == "finish"));
        assert!(samples.iter().flat_map(|sample| sample["processes"].as_array()).flatten().any(
            |process| process["rss_bytes"].as_u64().is_some()
                && process["ownership"] == "worker_process_tree"
        ));
    }

    #[test]
    fn bounded_resource_samples_keep_start_and_latest_finish() {
        let mut evidence = ProcessResourceEvidence::new("sleep 1", &HashMap::new());
        evidence.push_sample(ProcessResourceSample {
            phase: "start".to_string(),
            recorded_at: crate::state::timestamp(),
            processes: Vec::new(),
        });
        for index in 0..PROCESS_RESOURCE_SAMPLE_LIMIT {
            evidence.push_sample(ProcessResourceSample {
                phase: format!("mid-{index}"),
                recorded_at: crate::state::timestamp(),
                processes: Vec::new(),
            });
        }
        evidence.push_sample(ProcessResourceSample {
            phase: "finish".to_string(),
            recorded_at: crate::state::timestamp(),
            processes: Vec::new(),
        });

        assert_eq!(evidence.samples.len(), PROCESS_RESOURCE_SAMPLE_LIMIT);
        assert_eq!(evidence.samples.first().map(|sample| sample.phase.as_str()), Some("start"));
        assert_eq!(evidence.samples.last().map(|sample| sample.phase.as_str()), Some("finish"));
    }

    #[test]
    fn model_text_that_mentions_provider_errors_does_not_trigger_recovery() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let stdout_path = temp_dir.path().join("stdout.log");
        let stderr_path = temp_dir.path().join("stderr.log");
        fs::write(
            &stdout_path,
            r#"{"type":"text","part":{"text":"{\"constraints\":[\"Provider errors must halt the current attempt\"],\"risks\":[{\"description\":\"context length remains a future concern\"}]}"}}"#,
        )
        .expect("stdout should be writable");
        fs::write(&stderr_path, "timestamp=... level=INFO message=streaming\n")
            .expect("stderr should be writable");

        assert!(!command_output_indicates_provider_error(
            &stdout_path,
            &stderr_path
        ));
    }

    #[test]
    fn structured_provider_error_event_still_triggers_recovery() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let stdout_path = temp_dir.path().join("stdout.log");
        let stderr_path = temp_dir.path().join("stderr.log");
        fs::write(
            &stdout_path,
            r#"{"type":"error","error":{"message":"429 too many requests"}}"#,
        )
        .expect("stdout should be writable");
        fs::write(&stderr_path, "").expect("stderr should be writable");

        assert!(command_output_indicates_provider_error(
            &stdout_path,
            &stderr_path
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn provider_error_terminates_owned_detached_child_without_touching_unrelated_process() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let detached_pid_path = temp_dir.path().join("detached.pid");
        let command = format!(
            "setsid sh -c 'sleep 5' >/dev/null 2>&1 & printf '%s' \"$!\" > {}; sleep 0.2; printf '%s' 'rate limit exceeded' >&2; sleep 5",
            detached_pid_path.display(),
        );
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_PROVIDER_ERROR_RECOVERY".to_string(),
            "1".to_string(),
        );
        let mut unrelated = StdCommand::new("sleep")
            .arg("5")
            .spawn()
            .expect("unrelated process should start");
        let unrelated_pid = unrelated.id() as libc::pid_t;

        let result = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            &command,
            &env,
            None,
            None,
        )
        .expect("provider error should return a failed result");
        assert!(!result.success);

        let detached_pid = fs::read_to_string(&detached_pid_path)
            .expect("detached child pid should be recorded")
            .trim()
            .parse::<libc::pid_t>()
            .expect("detached child pid should be numeric");
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let detached_exists = fs::metadata(format!("/proc/{detached_pid}")).is_ok();
            let unrelated_exists = fs::metadata(format!("/proc/{unrelated_pid}")).is_ok();
            if !detached_exists && unrelated_exists {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let detached_survived = fs::metadata(format!("/proc/{detached_pid}")).is_ok();
        let unrelated_survived = fs::metadata(format!("/proc/{unrelated_pid}")).is_ok();
        unrelated.kill().expect("test-owned unrelated process should stop");
        unrelated.wait().expect("test-owned unrelated process should reap");
        assert!(
            !detached_survived,
            "detached worker child {detached_pid} survived owned cleanup"
        );
        assert!(
            unrelated_survived,
            "unrelated process {unrelated_pid} should survive owned cleanup"
        );
    }

    #[cfg(unix)]
    #[test]
    fn timed_out_command_terminates_its_process_group() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let child_pid_path = temp_dir.path().join("child.pid");
        let command = format!(
            "sleep 5 & printf '%s' \"$!\" > {}; wait",
            child_pid_path.display()
        );

        run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            &command,
            &HashMap::new(),
            None,
            Some(Duration::from_millis(500)),
        )
        .expect_err("command should time out");

        let child_pid = fs::read_to_string(&child_pid_path)
            .expect("background child pid should be recorded")
            .trim()
            .parse::<libc::pid_t>()
            .expect("background child pid should be numeric");
        std::thread::sleep(Duration::from_millis(20));
        let process_exists = unsafe { libc::kill(child_pid, 0) == 0 };
        assert!(
            !process_exists,
            "background command child {child_pid} survived"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rust_command_admission_times_out_when_another_process_holds_workspace_lease() {
        use std::os::fd::AsRawFd;

        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let lock_directory = temp_dir.path().join(".gear").join("locks");
        fs::create_dir_all(&lock_directory).expect("failed to create lock directory");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(lock_directory.join("rust-build.lock"))
            .expect("failed to open lock file");
        let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(result, 0, "test must own the workspace lease");

        let error = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "cargo --version",
            &HashMap::new(),
            None,
            Some(Duration::from_millis(40)),
        )
        .expect_err("held workspace lease should prevent command admission");

        assert_eq!(
            error.to_string(),
            "Gear Rust command admission timed out after 0 seconds"
        );
    }

    #[test]
    fn external_call_receipt_binds_owner_deadline_and_non_idempotent_retry_policy() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let worker_dir = temp_dir.path().join(".gear").join("workers").join("task");
        fs::create_dir_all(&worker_dir).expect("failed to create worker directory");
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_DIR".to_string(),
            worker_dir.to_string_lossy().to_string(),
        );
        env.insert("GEARBOX_EXTERNAL_TASK_ID".to_string(), "task".to_string());
        env.insert("GEARBOX_EXTERNAL_OWNER".to_string(), "executor".to_string());
        env.insert("GEARBOX_EXTERNAL_ATTEMPT".to_string(), "3".to_string());
        env.insert(
            "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
            "verification".to_string(),
        );
        env.insert("GEARBOX_EXTERNAL_IDEMPOTENT".to_string(), "false".to_string());
        env.insert("GEARBOX_EXTERNAL_RETRY_POLICY".to_string(), "none".to_string());

        let result = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "printf 'ok'",
            &env,
            None,
            Some(Duration::from_secs(2)),
        )
        .expect("command should succeed");
        assert!(result.success);

        let receipt: ExternalCallReceipt = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("external-call.json"))
                .expect("external call receipt should exist"),
        )
        .expect("external call receipt should parse");
        receipt.validate().expect("receipt should be sealed");
        assert_eq!(receipt.status, "succeeded");
        assert_eq!(receipt.owner, "executor");
        assert_eq!(receipt.request_kind, "verification");
        assert_eq!(receipt.attempt, 3);
        assert!(!receipt.retry_allowed);
        assert!(receipt.deadline_at_ms.is_some());
        assert!(worker_dir.join("external-call-start.json").exists());

        let mut forged_retry = receipt;
        forged_retry.idempotent = false;
        forged_retry.retry_allowed = true;
        forged_retry.receipt_hash.clear();
        forged_retry.receipt_hash = forged_retry.expected_hash().expect("hash receipt");
        assert!(
            forged_retry.validate().is_err(),
            "receipt validation must reject a retry grant for a non-idempotent call"
        );
    }

    #[cfg(unix)]
    #[test]
    fn external_call_receipt_records_deadline_failure() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let worker_dir = temp_dir.path().join(".gear").join("workers").join("task");
        fs::create_dir_all(&worker_dir).expect("failed to create worker directory");
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_DIR".to_string(),
            worker_dir.to_string_lossy().to_string(),
        );
        env.insert("GEARBOX_EXTERNAL_TASK_ID".to_string(), "task".to_string());
        env.insert("GEARBOX_EXTERNAL_OWNER".to_string(), "executor".to_string());

        let error = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "sleep 2",
            &env,
            None,
            Some(Duration::from_millis(50)),
        )
        .expect_err("command should hit its deadline");
        assert!(error.to_string().contains("timed out"));
        let receipt: ExternalCallReceipt = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("external-call.json"))
                .expect("deadline receipt should exist"),
        )
        .expect("deadline receipt should parse");
        assert_eq!(receipt.status, "deadline_exceeded");
        assert!(receipt.error.is_some());
        receipt.validate().expect("deadline receipt should be sealed");
    }

    #[test]
    fn blocked_external_effect_persists_a_pre_spawn_receipt() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let worker_dir = temp_dir.path().join(".gear").join("workers").join("task");
        fs::create_dir_all(&worker_dir).expect("failed to create worker directory");
        let marker = temp_dir.path().join("must-not-run");
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_DIR".to_string(),
            worker_dir.to_string_lossy().to_string(),
        );
        env.insert("GEARBOX_EXTERNAL_TASK_ID".to_string(), "task".to_string());
        env.insert("GEARBOX_EXTERNAL_OWNER".to_string(), "executor".to_string());
        env.insert(
            "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
            "webfetch".to_string(),
        );
        env.insert(
            "GEARBOX_EXTERNAL_TARGET".to_string(),
            "https://example.test/a".to_string(),
        );
        env.insert(
            "GEARBOX_EXTERNAL_REQUIRE_DEADLINE".to_string(),
            "true".to_string(),
        );

        let error = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            &format!("touch {}", marker.display()),
            &env,
            None,
            None,
        )
        .expect_err("missing deadline must block before spawn");
        assert!(error.to_string().contains("admission blocked"));
        assert!(!marker.exists(), "blocked command must not spawn");

        let receipt: ExternalCallReceipt = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("external-call.json"))
                .expect("blocked call receipt should exist"),
        )
        .expect("blocked call receipt should parse");
        receipt.validate().expect("blocked receipt should be sealed");
        assert_eq!(receipt.status, "blocked");
        assert_eq!(receipt.request_kind, "webfetch");
        assert!(receipt
            .error
            .as_deref()
            .is_some_and(|error| error.contains("deadline")));
        assert!(!worker_dir.join("external-call-start.json").exists());
    }

    #[test]
    fn external_effect_admission_blocks_redirects_cancel_and_non_idempotent_retry() {
        let workspace = tempfile::tempdir().expect("workspace should be created");
        let base = ExternalEffectRequest {
            kind: ExternalEffectKind::WebFetch,
            owner: "worker".to_string(),
            workspace: workspace.path().to_path_buf(),
            target: "https://example.test/a".to_string(),
            deadline_at_ms: Some(200),
            cancellation_requested: false,
            terminal_session: false,
            redirect_count: 6,
            max_redirects: 5,
            idempotent: false,
            retry_requested: false,
            require_deadline: true,
        };
        assert_eq!(
            admit_external_effect(&base, 100).status,
            "blocked",
            "redirect loops must not be admitted"
        );

        let mut cancelled = base.clone();
        cancelled.redirect_count = 0;
        cancelled.cancellation_requested = true;
        assert_eq!(admit_external_effect(&cancelled, 100).status, "blocked");

        let mut replay = cancelled;
        replay.cancellation_requested = false;
        replay.retry_requested = true;
        assert_eq!(admit_external_effect(&replay, 100).status, "blocked");

        let mut relative_escape = base;
        relative_escape.redirect_count = 0;
        relative_escape.target = "../outside".to_string();
        relative_escape.kind = ExternalEffectKind::Lsp;
        assert_eq!(admit_external_effect(&relative_escape, 100).status, "blocked");
    }

    #[cfg(unix)]
    #[test]
    fn external_effect_admission_blocks_missing_target_through_external_symlink() {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir().expect("workspace should be created");
        let outside = tempfile::tempdir().expect("outside directory should be created");
        symlink(outside.path(), workspace.path().join("linked"))
            .expect("external symlink should be created");
        let request = ExternalEffectRequest {
            kind: ExternalEffectKind::Shell,
            owner: "worker".to_string(),
            workspace: workspace.path().to_path_buf(),
            target: "linked/new-file.txt".to_string(),
            deadline_at_ms: Some(200),
            cancellation_requested: false,
            terminal_session: false,
            redirect_count: 0,
            max_redirects: 0,
            idempotent: false,
            retry_requested: false,
            require_deadline: true,
        };
        let decision = admit_external_effect(&request, 100);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("escapes the workspace"));
    }

    #[test]
    fn external_effect_admission_requires_deadline_and_allows_idempotent_retry() {
        let workspace = tempfile::tempdir().expect("workspace should be created");
        let request = ExternalEffectRequest {
            kind: ExternalEffectKind::Mcp,
            owner: "worker".to_string(),
            workspace: workspace.path().to_path_buf(),
            target: "mcp://server/tool".to_string(),
            deadline_at_ms: None,
            cancellation_requested: false,
            terminal_session: false,
            redirect_count: 0,
            max_redirects: 0,
            idempotent: true,
            retry_requested: true,
            require_deadline: true,
        };
        assert_eq!(admit_external_effect(&request, 100).status, "blocked");

        let mut admitted = request;
        admitted.deadline_at_ms = Some(200);
        let decision = admit_external_effect(&admitted, 100);
        assert_eq!(decision.status, "admitted");
        assert!(decision.retry_allowed);
    }

    #[test]
    fn protocol_external_effects_require_a_deadline_by_default() {
        let workspace = tempfile::tempdir().expect("workspace should be created");
        let env = HashMap::from([
            (
                "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
                "mcp".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_TARGET".to_string(),
                "mcp://server/tool".to_string(),
            ),
        ]);
        let request = external_effect_request(
            workspace.path(),
            "printf ok",
            &env,
            None,
            100,
        );
        assert!(request.require_deadline);
        assert_eq!(admit_external_effect(&request, 100).status, "blocked");
    }

    #[test]
    fn external_transport_commands_are_bounded_and_protocol_specific() {
        let mut webfetch_env = HashMap::from([
            (
                "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
                "webfetch".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_TARGET".to_string(),
                "https://example.test/a'b".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_MAX_REDIRECTS".to_string(),
                "2".to_string(),
            ),
        ]);
        let webfetch = external_transport_command(
            "ignored",
            &webfetch_env,
            Some(Duration::from_secs(3)),
        )
        .expect("WebFetch transport should compile")
        .expect("WebFetch should have a transport command");
        assert!(webfetch.starts_with("curl --fail --silent --show-error --location"));
        assert!(webfetch.contains("--max-redirs 2"));
        assert!(webfetch.contains("example.test/a'\\''b"));

        webfetch_env.insert(
            "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
            "lsp".to_string(),
        );
        webfetch_env.insert(
            "GEARBOX_EXTERNAL_PROTOCOL_REQUEST".to_string(),
            "{\"jsonrpc\":\"2.0\"}".to_string(),
        );
        let protocol = external_transport_command("server", &webfetch_env, Some(Duration::from_secs(1)))
            .expect("protocol transport should compile")
            .expect("protocol should have a transport command");
        assert!(protocol.contains("printf 'Content-Length:"));
        assert!(protocol.contains("Content-Length: %s\r\n\r\n%s"));
        assert!(protocol.ends_with("| server"));
    }

    #[cfg(unix)]
    #[test]
    fn lsp_transport_runs_content_length_framed_request_through_owned_worker() {
        let temp_dir = tempfile::tempdir().expect("workspace should be created");
        let worker_dir = temp_dir.path().join(".gear").join("workers").join("task");
        fs::create_dir_all(&worker_dir).expect("worker directory should be created");
        let request = r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#;
        let env = HashMap::from([
            (
                "GEARBOX_WORKER_DIR".to_string(),
                worker_dir.to_string_lossy().to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_TASK_ID".to_string(),
                "task".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_OWNER".to_string(),
                "lsp-worker".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
                "lsp".to_string(),
            ),
            ("GEARBOX_EXTERNAL_TARGET".to_string(), "cat".to_string()),
            (
                "GEARBOX_EXTERNAL_PROTOCOL_REQUEST".to_string(),
                request.to_string(),
            ),
        ]);
        let result = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "cat",
            &env,
            None,
            Some(Duration::from_secs(2)),
        )
        .expect("LSP protocol transport should succeed");
        assert!(result.success);
        assert!(result.stdout.starts_with(&format!(
            "Content-Length: {}\r\n\r\n",
            request.len()
        )));
        assert!(result.stdout.ends_with(request));
        let receipt: ExternalCallReceipt = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("external-call.json"))
                .expect("LSP receipt should exist"),
        )
        .expect("LSP receipt should parse");
        receipt.validate().expect("LSP receipt should be sealed");
        assert_eq!(receipt.request_kind, "lsp");
        assert_eq!(receipt.status, "succeeded");
    }

    #[cfg(unix)]
    #[test]
    fn webfetch_transport_runs_bounded_local_http_through_owned_worker() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let temp_dir = tempfile::tempdir().expect("workspace should be created");
        let worker_dir = temp_dir.path().join(".gear").join("workers").join("task");
        fs::create_dir_all(&worker_dir).expect("worker directory should be created");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("local HTTP listener should bind");
        let address = listener
            .local_addr()
            .expect("local HTTP listener should expose an address");
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("curl should connect to local HTTP");
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nwebfetch-test",
                )
                .expect("local HTTP response should be writable");
        });
        let target = format!("http://{address}/probe");
        let env = HashMap::from([
            (
                "GEARBOX_WORKER_DIR".to_string(),
                worker_dir.to_string_lossy().to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_TASK_ID".to_string(),
                "task".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_OWNER".to_string(),
                "webfetch-worker".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
                "webfetch".to_string(),
            ),
            ("GEARBOX_EXTERNAL_TARGET".to_string(), target.clone()),
            (
                "GEARBOX_EXTERNAL_MAX_REDIRECTS".to_string(),
                "1".to_string(),
            ),
        ]);
        let result = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "ignored",
            &env,
            None,
            Some(Duration::from_secs(3)),
        )
        .expect("WebFetch transport should reach the local server");
        server.join().expect("local HTTP server should exit cleanly");
        assert!(result.success);
        assert_eq!(result.stdout, "webfetch-test");
        let receipt: ExternalCallReceipt = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("external-call.json"))
                .expect("WebFetch receipt should exist"),
        )
        .expect("WebFetch receipt should parse");
        receipt.validate().expect("WebFetch receipt should be sealed");
        assert_eq!(receipt.request_kind, "webfetch");
        assert_eq!(receipt.target, target);
        assert_eq!(receipt.status, "succeeded");
    }

    #[cfg(unix)]
    #[test]
    fn mcp_transport_runs_protocol_request_through_owned_worker() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let worker_dir = temp_dir.path().join(".gear").join("workers").join("task");
        fs::create_dir_all(&worker_dir).expect("failed to create worker directory");
        let mut env = HashMap::from([
            (
                "GEARBOX_WORKER_DIR".to_string(),
                worker_dir.to_string_lossy().to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_TASK_ID".to_string(),
                "task".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_OWNER".to_string(),
                "mcp-worker".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_REQUEST_KIND".to_string(),
                "mcp".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_TARGET".to_string(),
                "cat".to_string(),
            ),
            (
                "GEARBOX_EXTERNAL_PROTOCOL_REQUEST".to_string(),
                "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\"}".to_string(),
            ),
        ]);
        let result = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "cat",
            &env,
            None,
            Some(Duration::from_secs(2)),
        )
        .expect("MCP protocol transport should succeed");
        assert!(result.success);
        assert!(result.stdout.contains("tools/list"));
        assert_eq!(result.command, "cat");
        let receipt: ExternalCallReceipt = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("external-call.json"))
                .expect("MCP receipt should exist"),
        )
        .expect("MCP receipt should parse");
        receipt.validate().expect("MCP receipt should be sealed");
        assert_eq!(receipt.request_kind, "mcp");
        assert_eq!(receipt.target, "cat");
        assert_eq!(receipt.status, "succeeded");
        env.insert(
            "GEARBOX_EXTERNAL_PROTOCOL_REQUEST".to_string(),
            String::new(),
        );
        let error = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "cat",
            &env,
            None,
            Some(Duration::from_secs(2)),
        )
        .expect_err("empty MCP payload must be rejected");
        assert!(error.to_string().contains("cannot be empty"));
        let receipt: ExternalCallReceipt = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("external-call.json"))
                .expect("failed MCP receipt should remain durable"),
        )
        .expect("failed MCP receipt should parse");
        assert_eq!(receipt.status, "error");
        assert!(receipt.error.is_some());
    }
}
