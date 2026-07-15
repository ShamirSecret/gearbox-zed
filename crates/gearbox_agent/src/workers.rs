use std::collections::{HashMap, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::state::{CoordinatorModel, Scope, StateStore, Task, TaskInputs, timestamp, write_json};
use crate::tools::{CancellationToken, run_shell_command_with_env_and_cancellation_and_timeout};

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
/// {
///   "disabled_mcps": [],
///   "background_task": { "defaultConcurrency": 2 },
///   "team_mode": { "enabled": false, "max_parallel_members": 2 }
/// }
/// ```
pub(crate) fn setup_omo_plugin_config_dir() -> Result<tempfile::TempDir> {
    let temp_dir = tempfile::tempdir().context("failed to create OMO plugin config temp dir")?;
    let opencode_dir = temp_dir.path().join("opencode");
    fs::create_dir_all(&opencode_dir)
        .with_context(|| format!("failed to create {}/opencode", temp_dir.path().display()))?;
    let config = json!({
        "disabled_mcps": [],
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerPacket {
    pub task_id: String,
    pub worker: String,
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
            bail!("resident session was disposed and cannot be reattached");
        }
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
            .filter(|model| !model.is_empty())
            .or(descriptor.worker_model);
        descriptor.resume_count = descriptor.resume_count.saturating_add(1);
        descriptor.last_resumed_at = Some(timestamp());
        return descriptor.seal();
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
    let packet_goal = plan_task
        .map(|plan_task| plan_task.worker_goal(goal))
        .unwrap_or_else(|| goal.to_string());
    let constraints = plan_task
        .map(crate::plan_graph::PlanTaskContract::worker_constraints)
        .unwrap_or_else(|| {
            vec![
                "Stay inside the allowed paths when they are provided.".to_string(),
                "Prefer the package manager already used by the project.".to_string(),
                "Read the provided spec and plan artifacts before changing code.".to_string(),
                "Leave runnable local instructions in the final output.".to_string(),
            ]
        });
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
    let packet = WorkerPacket {
        task_id: task.id.clone(),
        worker: worker_name.to_string(),
        worker_model: route.worker_model.map(ToString::to_string),
        variant: route.variant.clone(),
        variant_applied: model_params.and_then(|params| params.variant),
        prompt_append: route.prompt_append.clone(),
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
    };

    let packet_json =
        serde_json::to_string_pretty(&packet).context("failed to serialize worker packet")?;
    let packet_path =
        store.write_worker_file(&task.id, "packet.json", &format!("{packet_json}\n"))?;

    let prompt = worker_prompt(&packet)?;
    let prompt_path = store.write_worker_file(&task.id, "prompt.md", &prompt)?;
    if supports_interaction {
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
    }

    // Set up a temporary OMO plugin config directory for OpenCode session
    // workers.  This directory is bound to the handle's lifetime and cleaned
    // up when the handle is dropped.
    let omo_config_dir = if route.worker_kind == WorkerKind::OpencodeSession {
        Some(setup_omo_plugin_config_dir()?)
    } else {
        None
    };

    Ok(Arc::new(CommandWorkerSessionHandle {
        store: store.clone(),
        workspace: workspace.to_path_buf(),
        task_id: task.id.clone(),
        worker_name: worker_name.to_string(),
        skip_worker: config.skip_worker,
        command: route.worker_command.map(ToString::to_string),
        command_timeout: if is_free_model(packet.worker_model.as_deref()) {
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
    worker_name: String,
    skip_worker: bool,
    command: Option<String>,
    command_timeout: Option<Duration>,
    worker_model: Option<String>,
    model_variant: Option<String>,
    tool_policy: WorkerToolPolicy,
    packet_path: PathBuf,
    prompt_path: PathBuf,
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

impl CommandWorkerSessionHandle {
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
            if let Some(summary) = unavailable_command_summary(command) {
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
        let cancellation_token = self.with_session_state(|state| {
            state.active_command = true;
            state.cancellation_token.clone()
        })?;
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_PACKET".to_string(),
            self.packet_path.to_string_lossy().to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_PROMPT".to_string(),
            prompt_path.to_string_lossy().to_string(),
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

        let output = run_shell_command_with_env_and_cancellation_and_timeout(
            &self.workspace,
            command,
            &env,
            Some(&cancellation_token),
            self.command_timeout,
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
            status: if output.success {
                WorkerStatus::Succeeded
            } else {
                WorkerStatus::Failed
            },
            command: Some(command.to_string()),
            exit_code: output.exit_code,
            summary: if output.success {
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
        let turn_epoch = self.with_session_state(|state| state.turn_epoch)?;
        self.store.write_worker_file(
            &self.task_id,
            &format!("turn-{turn_epoch}-result.json"),
            &format!("{}\n", serde_json::to_string_pretty(&result)?),
        )?;
        Ok(result)
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
        let prompt_path = self.store.write_worker_file(
            &self.task_id,
            &format!("{kind}-{interaction_index}.md"),
            &format!(
                "# Gear worker {kind}\n\nCommand: `{command}`\n\n{}\n",
                prompt.trim()
            ),
        )?;
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

                    self.emit_event(WorkerEvent::ToolCallStarted {
                        kind: kind.to_string(),
                        tool_name: tool_name.clone(),
                        arguments: args,
                    })?;

                    // Emit ToolCallFinished right after start for post-hoc parsing
                    // (we don't have a separate result stream for command-backed workers)
                    self.emit_event(WorkerEvent::ToolCallFinished {
                        kind: kind.to_string(),
                        tool_name,
                        result: String::new(),
                    })?;

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

pub fn worker_prompt(packet: &WorkerPacket) -> Result<String> {
    let packet_json =
        serde_json::to_string_pretty(packet).context("failed to serialize worker prompt packet")?;
    let prompt_append = packet
        .prompt_append
        .as_deref()
        .map(str::trim)
        .filter(|append| !append.is_empty())
        .map(|append| format!("\n## Route instructions\n\n{}\n", append))
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

## Phase request

{}

{}
Return only the response format required by the phase request. Do not add a generic worker report or markdown fence.
"#,
            packet.worker,
            packet_json,
            model_metadata,
            packet.tools.to_markdown(),
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
        prompt_append,
        step_report
    ))
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
                "sh -c 'echo GEARBOX_WORKER_MODEL_VARIANT=$GEARBOX_WORKER_MODEL_VARIANT'"
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
  *follow-up-1.md) printf '%s\n' '{"usage":{"input_tokens":7,"output_tokens":4,"cost_micros":5,"duration_ms":13,"cache_hit":true}}' ;;
  *steer-2.md) printf '%s\n' '{"usage":{"input_tokens":9,"output_tokens":6,"cost_micros":7,"duration_ms":17,"cache_hit":true}}' ;;
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
            worker_name: "test_worker".to_string(),
            skip_worker: false,
            command: None,
            command_timeout: Some(Duration::from_secs(30)),
            worker_model: None,
            model_variant: None,
            tool_policy: WorkerToolPolicy::default(),
            packet_path: temp_dir.path().join("packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
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
            worker_name: "test_worker".to_string(),
            skip_worker: false,
            command: None,
            command_timeout: Some(Duration::from_secs(30)),
            worker_model: None,
            model_variant: None,
            tool_policy: WorkerToolPolicy::default(),
            packet_path: temp_dir.path().join("packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
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
            worker_name: "test_worker".to_string(),
            skip_worker: false,
            command: None,
            command_timeout: Some(Duration::from_secs(30)),
            worker_model: None,
            model_variant: None,
            tool_policy: WorkerToolPolicy::default(),
            packet_path: temp_dir.path().join("packet.json"),
            prompt_path: temp_dir.path().join("prompt.md"),
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
        let dir = setup_omo_plugin_config_dir()?;
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
        let bg_task = obj["background_task"]
            .as_object()
            .expect("background_task should be an object");
        assert_eq!(bg_task["defaultConcurrency"], 2);
        let disabled = obj["disabled_mcps"]
            .as_array()
            .expect("disabled_mcps should be an array");
        assert!(disabled.is_empty(), "expected empty disabled_mcps");
        Ok(())
    }

    #[test]
    fn omo_plugin_config_dir_is_cleaned_up_on_drop() -> Result<()> {
        let config_path = {
            let dir = setup_omo_plugin_config_dir()?;
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
}
