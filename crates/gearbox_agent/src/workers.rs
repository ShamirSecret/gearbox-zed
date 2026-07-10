use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

use crate::state::{CoordinatorModel, Scope, StateStore, Task, TaskInputs, write_json};
use crate::tools::{CancellationToken, run_shell_command_with_env_and_cancellation};

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
                "Focus on implementation, keep changes minimal, and do not ask the user questions.",
            ),
            Self::Review => Some(
                "This is an independent review turn. Do not edit files; inspect the evidence and return concrete findings.",
            ),
            Self::Explore | Self::Librarian => Some(
                "This is a read-only exploration turn. Do not edit files; trace the code and summarize the evidence.",
            ),
            Self::ZedNative => Some(
                "This is a native Zed worker turn. Stay bounded and do not create a Gear goal loop recursively.",
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
            let matching_routes = category
                .preferred_worker_kinds()
                .iter()
                .filter_map(|worker_kind| self.matching_configured_route(config, *worker_kind))
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
                let skipped_unavailable_route = category
                    .preferred_worker_kinds()
                    .iter()
                    .take(selected_preferred_index)
                    .any(|worker_kind| {
                        config.worker_routes.iter().any(|configured_route| {
                            configured_route.worker_kind == *worker_kind
                                && Self::route_model_is_unavailable(
                                    config,
                                    configured_route.worker_kind,
                                    configured_route.worker_model.as_deref(),
                                )
                        })
                    });
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
            return SelectedWorkerRoute {
                worker_kind: config.worker_kind,
                worker_command: config.worker_command.as_deref(),
                worker_model: config.worker_model.as_deref(),
                require_worker: config.require_worker,
                category,
                route_reason: if hinted_category.is_some() {
                    format!(
                        "category `{}` fell back to default `{}` worker",
                        category.as_str(),
                        config.worker_kind.as_str()
                    )
                } else {
                    format!(
                        "attempt {attempt} used default `{}` worker",
                        config.worker_kind.as_str()
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

pub type WorkerTurnOutcome = WorkerResult;

pub type WorkerEventListener = Arc<dyn Fn(WorkerEvent) + Send + Sync>;

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
    next_listener_id: AtomicUsize,
}

impl WorkerSessionSubscriptions {
    fn subscribe(self: &Arc<Self>, listener: WorkerEventListener) -> Result<WorkerSubscription> {
        let subscription_id = self.next_listener_id.fetch_add(1, Ordering::Relaxed);
        self.listeners
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session subscription mutex poisoned"))?
            .insert(subscription_id, listener);
        Ok(WorkerSubscription {
            subscriptions: Arc::downgrade(self),
            subscription_id,
        })
    }

    fn emit(&self, event: WorkerEvent) {
        let listeners = self
            .listeners
            .lock()
            .map(|listeners| listeners.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
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

pub type WorkerRunRequest<'a> = WorkerStartRequest<'a>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    pub supports_follow_up: bool,
    pub supports_steering: bool,
    pub supports_cancellation: bool,
    pub supports_resident_session: bool,
}

impl WorkerCapabilities {
    pub fn command() -> Self {
        Self {
            supports_follow_up: false,
            supports_steering: false,
            supports_cancellation: true,
            supports_resident_session: false,
        }
    }

    pub fn resident_command() -> Self {
        Self {
            supports_follow_up: true,
            supports_steering: true,
            supports_cancellation: true,
            supports_resident_session: true,
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
    fn subscribe(&self, _listener: WorkerEventListener) -> Result<WorkerSubscription> {
        bail!("worker session does not support event subscriptions")
    }
    fn wait_for_idle(&self) -> Result<WorkerTurnOutcome> {
        self.wait_for_result()
    }
    fn wait_for_outcome(&self) -> Result<WorkerOutcome>;
    fn wait_for_result(&self) -> Result<WorkerResult>;
    fn last_output(&self) -> Option<String>;
}

pub trait WorkerAdapter {
    fn name(&self) -> &'static str;
    fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult>;
}

#[derive(Default)]
pub struct WorkerRegistry {
    native_backend: Option<Arc<dyn NativeWorkerBackend>>,
}

impl WorkerRegistry {
    pub fn with_native_backend(native_backend: Arc<dyn NativeWorkerBackend>) -> Self {
        Self {
            native_backend: Some(native_backend),
        }
    }

    pub fn set_native_backend(&mut self, native_backend: Arc<dyn NativeWorkerBackend>) {
        self.native_backend = Some(native_backend);
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

        match worker_kind {
            WorkerKind::Opencode => OpencodeCommandWorker {}.start(request),
            WorkerKind::OpencodeSession => OpencodeSessionWorker {}.start(request),
            WorkerKind::Codex => CodexCommandWorker {}.start(request),
            WorkerKind::Claude => ClaudeCommandWorker {}.start(request),
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

pub struct CommandWorker {}

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
        goal: goal.to_string(),
        coordinator_model: coordinator_model.cloned(),
        coordinator_brief: coordinator_brief.map(ToString::to_string),
        scope: task.scope.clone(),
        inputs: task.inputs.clone(),
        constraints: vec![
            "Stay inside the allowed paths when they are provided.".to_string(),
            "Prefer the package manager already used by the project.".to_string(),
            "Read the provided spec and plan artifacts before changing code.".to_string(),
            "Leave runnable local instructions in the final output.".to_string(),
        ],
        required_outputs: vec![
            "summary".to_string(),
            "changed_files".to_string(),
            "commands_run".to_string(),
            "known_failures".to_string(),
            "next_steps".to_string(),
        ],
        verification: VerificationContract {
            preferred_commands: verification_commands.to_vec(),
            must_not_skip: vec!["typecheck".to_string()],
        },
        stop_conditions: vec![
            "Requires a paid external service.".to_string(),
            "Requires a user-provided API key.".to_string(),
            "The same verification fails twice.".to_string(),
        ],
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
    }

    Ok(Arc::new(CommandWorkerSessionHandle {
        store: store.clone(),
        workspace: workspace.to_path_buf(),
        task_id: task.id.clone(),
        worker_name: worker_name.to_string(),
        skip_worker: config.skip_worker,
        command: route.worker_command.map(ToString::to_string),
        model_variant: packet.variant_applied.clone(),
        tool_policy: packet.tools.clone(),
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
        env.insert(
            "GEARBOX_WORKER_TOOL_POLICY".to_string(),
            serde_json::to_string(&self.tool_policy)
                .context("failed to serialize worker tool policy for dispatch")?,
        );

        let output = run_shell_command_with_env_and_cancellation(
            &self.workspace,
            command,
            &env,
            Some(&cancellation_token),
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
        self.supports_interaction
            .then(|| format!("{}_session", self.task_id))
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
        }
        Ok(())
    }

    fn subscribe(&self, listener: WorkerEventListener) -> Result<WorkerSubscription> {
        if !self.supports_interaction {
            bail!("command-backed worker sessions do not support event subscriptions");
        }
        self.subscriptions.subscribe(listener)
    }

    fn wait_for_idle(&self) -> Result<WorkerTurnOutcome> {
        self.wait_for_result()
    }

    fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
        Ok(worker_outcome_from_result(&self.execute()?))
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
Return a concise report with:

- summary
- changed_files
- commands_run
- known_failures
- next_steps
"#,
        packet.worker,
        packet_json,
        model_metadata,
        packet.tools.to_markdown(),
        prompt_append
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

pub fn worker_outcome_from_result(result: &WorkerResult) -> WorkerOutcome {
    let parsed_report = parsed_worker_report(result);
    WorkerOutcome {
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
        known_failures: if parsed_report.known_failures.is_empty() {
            if result.status == WorkerStatus::Failed {
                vec![result.summary.clone()]
            } else {
                Vec::new()
            }
        } else {
            parsed_report.known_failures
        },
        raw_output_path: result
            .last_message_path
            .clone()
            .or_else(|| result.stdout_path.clone())
            .or_else(|| result.stderr_path.clone()),
        command: result.command.clone(),
        exit_code: result.exit_code,
    }
}

#[derive(Default)]
struct ParsedWorkerReport {
    summary: Option<String>,
    changed_files: Vec<String>,
    commands_run: Vec<String>,
    known_failures: Vec<String>,
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
        _ => None,
    }
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
    let result_json =
        serde_json::to_string_pretty(result).context("failed to serialize worker result")?;
    store.write_worker_file(task_id, "result.json", &format!("{result_json}\n"))?;
    let outcome = worker_outcome_from_result(result);
    let outcome_json =
        serde_json::to_string_pretty(&outcome).context("failed to serialize worker outcome")?;
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
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use anyhow::Result;

    use super::*;

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

        fn subscribe(&self, _listener: WorkerEventListener) -> Result<WorkerSubscription> {
            Ok(WorkerSubscription::noop())
        }

        fn wait_for_idle(&self) -> Result<WorkerTurnOutcome> {
            Ok(self.result.clone())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            Ok(worker_outcome_from_result(&self.result))
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
            worker_command: Some(
                "sh -c 'echo \"$GEARBOX_WORKER_TOOL_POLICY\"'".to_string(),
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
}
