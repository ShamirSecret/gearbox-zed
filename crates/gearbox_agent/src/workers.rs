use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::state::{Scope, StateStore, Task};
use crate::tools::{CancellationToken, run_shell_command_with_env_and_cancellation};

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub opencode_command: Option<String>,
    pub skip_worker: bool,
    pub require_worker: bool,
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
    pub goal: String,
    pub scope: Scope,
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
    pub result_path: PathBuf,
}

pub struct OpencodeWorker;

impl OpencodeWorker {
    pub fn run(
        store: &StateStore,
        workspace: &std::path::Path,
        task: &Task,
        request: &str,
        verification_commands: &[String],
        config: &WorkerConfig,
        cancellation_token: Option<&CancellationToken>,
    ) -> Result<WorkerResult> {
        let packet = WorkerPacket {
            task_id: task.id.clone(),
            worker: "opencode".to_string(),
            goal: request.to_string(),
            scope: task.scope.clone(),
            constraints: vec![
                "Stay inside the allowed paths when they are provided.".to_string(),
                "Prefer the package manager already used by the project.".to_string(),
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

        let prompt = worker_prompt(&packet);
        let prompt_path = store.write_worker_file(&task.id, "prompt.md", &prompt)?;

        if config.skip_worker || config.opencode_command.is_none() {
            let summary = if config.skip_worker {
                "Worker execution was skipped by CLI option."
            } else {
                "No opencode command was configured; worker packet is ready for external execution."
            };
            let result = WorkerResult {
                status: WorkerStatus::Skipped,
                command: None,
                exit_code: None,
                summary: summary.to_string(),
                packet_path,
                prompt_path,
                stdout_path: None,
                stderr_path: None,
                result_path: store.worker_dir(&task.id).join("result.json"),
            };
            write_result(store, &task.id, &result)?;
            return Ok(result);
        }

        let command = config
            .opencode_command
            .as_ref()
            .context("opencode command missing")?;
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_PACKET".to_string(),
            packet_path.to_string_lossy().to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_PROMPT".to_string(),
            prompt_path.to_string_lossy().to_string(),
        );

        let output = run_shell_command_with_env_and_cancellation(
            workspace,
            command,
            &env,
            cancellation_token,
        )?;
        let stdout_path = store.write_worker_file(&task.id, "stdout.log", &output.stdout)?;
        let stderr_path = store.write_worker_file(&task.id, "stderr.log", &output.stderr)?;

        let result = WorkerResult {
            status: if output.success {
                WorkerStatus::Succeeded
            } else {
                WorkerStatus::Failed
            },
            command: Some(command.clone()),
            exit_code: output.exit_code,
            summary: if output.success {
                "opencode worker command completed.".to_string()
            } else {
                "opencode worker command failed.".to_string()
            },
            packet_path,
            prompt_path,
            stdout_path: Some(stdout_path),
            stderr_path: Some(stderr_path),
            result_path: store.worker_dir(&task.id).join("result.json"),
        };
        write_result(store, &task.id, &result)?;
        Ok(result)
    }
}

fn worker_prompt(packet: &WorkerPacket) -> String {
    format!(
        r#"# Gear worker packet

You are an opencode worker controlled by Gearbox Gear. Treat this packet as the contract.

```json
{}
```

Return a concise report with:

- summary
- changed_files
- commands_run
- known_failures
- next_steps
"#,
        serde_json::to_string_pretty(packet).unwrap_or_else(|_| "{}".to_string())
    )
}

fn write_result(store: &StateStore, task_id: &str, result: &WorkerResult) -> Result<()> {
    let result_json =
        serde_json::to_string_pretty(result).context("failed to serialize worker result")?;
    store.write_worker_file(task_id, "result.json", &format!("{result_json}\n"))?;
    Ok(())
}
