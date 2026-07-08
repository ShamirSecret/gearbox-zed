use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Args, Parser, Subcommand};

use crate::runtime::{DEFAULT_MAX_ITERATIONS, Orchestrator, RunOptions};
use crate::workers::{WorkerConfig, WorkerKind};

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

    #[arg(long, default_value = "opencode")]
    worker: String,

    #[arg(long)]
    worker_command: Option<String>,

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
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Run(command) => {
            let worker_kind = WorkerKind::parse(&command.worker)
                .ok_or_else(|| anyhow!("unknown Gear worker kind `{}`", command.worker))?;
            let outcome = Orchestrator::run(RunOptions {
                request: command.prompt,
                workspace: command.workspace,
                verification_commands: command.verification_commands,
                worker: WorkerConfig {
                    worker_kind,
                    worker_command: command.worker_command.or(command.opencode_command),
                    worker_routes: Vec::new(),
                    skip_worker: command.skip_worker,
                    require_worker: command.require_worker,
                },
                allowed_paths: command.allowed_paths,
                forbidden_paths: command.forbidden_paths,
                max_files_changed: command.max_files_changed,
                install_dependencies: command.install_dependencies,
                event_sink: None,
                cancellation_token: None,
                max_iterations: command.max_iterations,
                coordinator_model: None,
                coordinator_brief: None,
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
