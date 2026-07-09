use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Args, Parser, Subcommand};

use crate::runtime::{
    DEFAULT_MAX_ITERATIONS, DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK, DEFAULT_MAX_RUNTIME_MINUTES,
    Orchestrator, RunOptions,
};
use crate::workers::{WorkerConfig, WorkerKind, WorkerRoute};

#[derive(Debug, Parser)]
#[command(name = "gear")]
#[command(about = "Gearbox Gear local orchestration runtime")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run(RunCommand),
}

#[derive(Debug, Args)]
struct RunCommand {
    prompt: String,

    #[arg(long, default_value = ".")]
    workspace: PathBuf,

    #[arg(long = "verify-command")]
    verification_commands: Vec<String>,

    #[arg(long)]
    opencode_command: Option<String>,

    #[arg(long)]
    codex_command: Option<String>,

    #[arg(long)]
    claude_command: Option<String>,

    #[arg(long)]
    zed_agent_command: Option<String>,

    #[arg(long)]
    custom_command: Option<String>,

    #[arg(long, default_value = "opencode")]
    worker: String,

    #[arg(long)]
    worker_command: Option<String>,

    #[arg(long)]
    worker_model: Option<String>,

    #[arg(long = "worker-sequence")]
    worker_sequence: Option<String>,

    #[arg(long = "unavailable-worker-model")]
    unavailable_worker_models: Vec<String>,

    #[arg(long, default_value_t = 1)]
    premium_worker_budget: usize,

    #[arg(long, default_value_t = 1)]
    max_parallel_workers: usize,

    #[arg(long, default_value_t = 1)]
    max_parallel_per_key: usize,

    #[arg(long, default_value_t = 30)]
    stale_task_timeout_secs: usize,

    #[arg(long)]
    skip_worker: bool,

    #[arg(long)]
    require_worker: bool,

    #[arg(long = "allowed-path")]
    allowed_paths: Vec<String>,

    #[arg(long = "forbidden-path")]
    forbidden_paths: Vec<String>,

    #[arg(long, default_value_t = 40)]
    max_files_changed: usize,

    #[arg(long)]
    install_dependencies: bool,

    #[arg(long, default_value_t = DEFAULT_MAX_ITERATIONS)]
    max_iterations: usize,

    #[arg(long, default_value_t = DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK)]
    max_provider_unknown_streak: usize,

    #[arg(long, default_value_t = usize::MAX)]
    max_child_depth: usize,

    #[arg(long, default_value_t = DEFAULT_MAX_RUNTIME_MINUTES)]
    max_runtime_minutes: usize,
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Run(command) => {
            let worker = worker_config_from_command(&command)?;
            let outcome = Orchestrator::run(RunOptions {
                request: command.prompt,
                workspace: command.workspace,
                verification_commands: command.verification_commands,
                worker,
                allowed_paths: command.allowed_paths,
                forbidden_paths: command.forbidden_paths,
                max_files_changed: command.max_files_changed,
                install_dependencies: command.install_dependencies,
                event_sink: None,
                cancellation_token: None,
                max_iterations: command.max_iterations,
                max_provider_unknown_streak: command.max_provider_unknown_streak,
                max_child_depth: command.max_child_depth,
                max_runtime_minutes: command.max_runtime_minutes,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
            })?;

            println!("Gear goal: {}", outcome.goal_id);
            println!("Status: {}", outcome.status.as_str());
            println!("Artifacts: {}", outcome.artifacts_root.display());
            println!("Final report: {}", outcome.final_report_path.display());
            println!("Events: {}", outcome.events_path.display());
        }
    }

    Ok(())
}

fn worker_config_from_command(command: &RunCommand) -> Result<WorkerConfig> {
    let worker_kind = WorkerKind::parse(&command.worker)
        .ok_or_else(|| anyhow!("unknown Gear worker kind `{}`", command.worker))?;
    let worker_model = command
        .worker_model
        .clone()
        .filter(|model| !model.trim().is_empty());
    let worker_command = command
        .worker_command
        .clone()
        .or_else(|| worker_command_for_kind(worker_kind, command))
        .or_else(|| worker_kind.default_command(worker_model.as_deref()))
        .filter(|command| !command.trim().is_empty());
    let worker_routes = worker_routes_from_sequence(
        command.worker_sequence.as_deref(),
        worker_kind,
        &worker_command,
        &worker_model,
        command,
    )?;
    let require_worker = command.require_worker
        || worker_command.is_some()
        || worker_routes
            .iter()
            .any(|route| route.worker_command.is_some());

    Ok(WorkerConfig {
        worker_kind,
        worker_command,
        worker_model,
        worker_routes,
        unavailable_worker_models: command
            .unavailable_worker_models
            .iter()
            .map(|model| model.trim().to_string())
            .filter(|model| !model.is_empty())
            .collect(),
        premium_worker_budget: command.premium_worker_budget,
        max_parallel_workers: command.max_parallel_workers.max(1),
        max_parallel_per_key: command.max_parallel_per_key.max(1),
        stale_task_timeout_secs: command.stale_task_timeout_secs.max(1),
        skip_worker: command.skip_worker,
        require_worker,
    })
}

fn worker_routes_from_sequence(
    worker_sequence: Option<&str>,
    default_worker_kind: WorkerKind,
    default_worker_command: &Option<String>,
    default_worker_model: &Option<String>,
    command: &RunCommand,
) -> Result<Vec<WorkerRoute>> {
    let Some(worker_sequence) = worker_sequence else {
        return Ok(Vec::new());
    };

    worker_sequence
        .split(',')
        .filter_map(|worker| {
            let worker = worker.trim();
            (!worker.is_empty()).then_some(worker)
        })
        .map(|worker| {
            let (worker_kind, worker_model) = worker_route_from_sequence_entry(worker)?;
            let worker_command = worker_command_for_kind(worker_kind, command)
                .or_else(|| {
                    (worker_kind == default_worker_kind)
                        .then(|| default_worker_command.clone())
                        .flatten()
                })
                .or_else(|| worker_kind.default_command(worker_model.as_deref()));
            let worker_model = worker_model.or_else(|| {
                (worker_kind == default_worker_kind)
                    .then(|| default_worker_model.clone())
                    .flatten()
            });
            Ok(WorkerRoute {
                worker_kind,
                worker_command,
                worker_model,
            })
        })
        .collect()
}

fn worker_route_from_sequence_entry(worker: &str) -> Result<(WorkerKind, Option<String>)> {
    let (worker, worker_model) = worker
        .split_once(':')
        .map(|(worker, worker_model)| {
            (
                worker.trim(),
                Some(worker_model.trim().to_string()).filter(|model| !model.is_empty()),
            )
        })
        .unwrap_or((worker.trim(), None));
    let worker_kind = WorkerKind::parse(worker)
        .ok_or_else(|| anyhow!("unknown Gear worker kind in sequence `{worker}`"))?;
    Ok((worker_kind, worker_model))
}

fn worker_command_for_kind(worker_kind: WorkerKind, command: &RunCommand) -> Option<String> {
    match worker_kind {
        WorkerKind::Opencode | WorkerKind::OpencodeSession => command.opencode_command.clone(),
        WorkerKind::Codex => command.codex_command.clone(),
        WorkerKind::Claude => command.claude_command.clone(),
        WorkerKind::ZedAgent => command.zed_agent_command.clone(),
        WorkerKind::Custom => command.custom_command.clone(),
    }
    .filter(|command| !command.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_command() -> RunCommand {
        RunCommand {
            prompt: "build a test app".to_string(),
            workspace: PathBuf::from("."),
            verification_commands: Vec::new(),
            opencode_command: None,
            codex_command: None,
            claude_command: None,
            zed_agent_command: None,
            custom_command: None,
            worker: "opencode".to_string(),
            worker_command: None,
            worker_model: None,
            worker_sequence: None,
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: false,
            allowed_paths: Vec::new(),
            forbidden_paths: Vec::new(),
            max_files_changed: 40,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            max_iterations: DEFAULT_MAX_ITERATIONS,
        }
    }

    #[test]
    fn cli_worker_config_builds_sequence_with_kind_commands() -> Result<()> {
        let mut command = run_command();
        command.worker_sequence = Some("opencode,codex:gpt-5.4,claude".to_string());
        command.opencode_command = Some("opencode run".to_string());
        command.codex_command = Some("codex exec".to_string());
        command.claude_command = Some("claude -p".to_string());

        let config = worker_config_from_command(&command)?;

        assert_eq!(config.worker_routes.len(), 3);
        assert_eq!(config.worker_routes[0].worker_kind, WorkerKind::Opencode);
        assert_eq!(
            config.worker_routes[1].worker_command.as_deref(),
            Some("codex exec")
        );
        assert_eq!(
            config.worker_routes[1].worker_model.as_deref(),
            Some("gpt-5.4")
        );
        assert_eq!(
            config.worker_routes[2].worker_command.as_deref(),
            Some("claude -p")
        );
        assert_eq!(config.max_parallel_workers, 1);
        assert_eq!(config.max_parallel_per_key, 1);
        assert_eq!(config.premium_worker_budget, 1);
        assert!(config.require_worker);
        Ok(())
    }

    #[test]
    fn cli_worker_config_keeps_parallelism_limits() -> Result<()> {
        let mut command = run_command();
        command.max_parallel_workers = 3;
        command.max_parallel_per_key = 2;
        command.premium_worker_budget = 4;

        let config = worker_config_from_command(&command)?;

        assert_eq!(config.premium_worker_budget, 4);
        assert_eq!(config.max_parallel_workers, 3);
        assert_eq!(config.max_parallel_per_key, 2);
        Ok(())
    }

    #[test]
    fn cli_worker_config_uses_default_codex_command_when_unspecified() -> Result<()> {
        let mut command = run_command();
        command.worker = "codex".to_string();
        command.worker_model = Some("gpt-5".to_string());

        let config = worker_config_from_command(&command)?;

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
        Ok(())
    }

    #[test]
    fn cli_worker_config_uses_primary_kind_command_for_non_opencode_worker() -> Result<()> {
        let mut command = run_command();
        command.worker = "codex".to_string();
        command.codex_command = Some("codex exec".to_string());

        let config = worker_config_from_command(&command)?;

        assert_eq!(config.worker_kind, WorkerKind::Codex);
        assert_eq!(config.worker_command.as_deref(), Some("codex exec"));
        assert!(config.require_worker);
        Ok(())
    }

    #[test]
    fn cli_worker_config_rejects_unknown_sequence_worker() {
        let mut command = run_command();
        command.worker_sequence = Some("opencode,unknown".to_string());

        assert!(worker_config_from_command(&command).is_err());
    }
}
