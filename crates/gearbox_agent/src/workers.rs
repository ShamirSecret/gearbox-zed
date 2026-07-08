use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::state::{CoordinatorModel, Scope, StateStore, Task, TaskInputs};
use crate::tools::{CancellationToken, run_shell_command_with_env_and_cancellation};

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub worker_kind: WorkerKind,
    pub worker_command: Option<String>,
    pub worker_routes: Vec<WorkerRoute>,
    pub skip_worker: bool,
    pub require_worker: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRoute {
    pub worker_kind: WorkerKind,
    pub worker_command: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerKind {
    #[default]
    Opencode,
    Codex,
    Claude,
    ZedAgent,
    Custom,
}

impl WorkerKind {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "opencode" => Some(Self::Opencode),
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
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::ZedAgent => "zed_agent",
            Self::Custom => "custom",
        }
    }
}

impl WorkerConfig {
    pub fn selected_route(&self, attempt: usize) -> SelectedWorkerRoute<'_> {
        if self.worker_routes.is_empty() {
            return SelectedWorkerRoute {
                worker_kind: self.worker_kind,
                worker_command: self.worker_command.as_deref(),
                require_worker: self.require_worker,
            };
        }

        let index = attempt
            .saturating_sub(1)
            .min(self.worker_routes.len().saturating_sub(1));
        let route = &self.worker_routes[index];
        SelectedWorkerRoute {
            worker_kind: route.worker_kind,
            worker_command: route.worker_command.as_deref(),
            require_worker: self.require_worker || route.worker_command.is_some(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SelectedWorkerRoute<'a> {
    pub worker_kind: WorkerKind,
    pub worker_command: Option<&'a str>,
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
    pub result_path: PathBuf,
}

pub struct WorkerRunRequest<'a> {
    pub store: &'a StateStore,
    pub workspace: &'a Path,
    pub task: &'a Task,
    pub goal: &'a str,
    pub verification_commands: &'a [String],
    pub config: &'a WorkerConfig,
    pub cancellation_token: Option<&'a CancellationToken>,
    pub coordinator_model: Option<&'a CoordinatorModel>,
    pub coordinator_brief: Option<&'a str>,
}

pub trait WorkerAdapter {
    fn name(&self) -> &'static str;
    fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult>;
}

#[derive(Default)]
pub struct WorkerRegistry;

impl WorkerRegistry {
    pub fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult> {
        CommandWorker.run(request)
    }
}

pub struct CommandWorker {
}

impl WorkerAdapter for CommandWorker {
    fn name(&self) -> &'static str {
        "command"
    }

    fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult> {
        let WorkerRunRequest {
            store,
            workspace,
            task,
            goal,
            verification_commands,
            config,
            cancellation_token,
            coordinator_model,
            coordinator_brief,
        } = request;
        let route = config.selected_route(task.attempt);
        let worker_name = route.worker_kind.as_str();
        let packet = WorkerPacket {
            task_id: task.id.clone(),
            worker: worker_name.to_string(),
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

        if config.skip_worker || route.worker_command.is_none() {
            let summary = if config.skip_worker {
                "Worker execution was skipped by CLI option."
            } else {
                "No worker command was configured; worker packet is ready for external execution."
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

        let command = route.worker_command.context("worker command missing")?;
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
            command: Some(command.to_string()),
            exit_code: output.exit_code,
            summary: if output.success {
                format!("{worker_name} worker command completed.")
            } else {
                format!("{worker_name} worker command failed.")
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

fn worker_prompt(packet: &WorkerPacket) -> Result<String> {
    let packet_json =
        serde_json::to_string_pretty(packet).context("failed to serialize worker prompt packet")?;

    Ok(format!(
        r#"# Gear worker packet

You are a `{}` worker controlled by Gearbox Gear. Treat this packet as the contract.

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
        packet.worker, packet_json
    ))
}

fn write_result(store: &StateStore, task_id: &str, result: &WorkerResult) -> Result<()> {
    let result_json =
        serde_json::to_string_pretty(result).context("failed to serialize worker result")?;
    store.write_worker_file(task_id, "result.json", &format!("{result_json}\n"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;

    use super::*;

    #[test]
    fn parses_worker_kind_aliases() {
        assert_eq!(WorkerKind::parse("opencode"), Some(WorkerKind::Opencode));
        assert_eq!(WorkerKind::parse("claude-code"), Some(WorkerKind::Claude));
        assert_eq!(WorkerKind::parse("zed_agent"), Some(WorkerKind::ZedAgent));
        assert_eq!(WorkerKind::parse("unknown"), None);
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
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: crate::state::TaskInputs::default(),
            outputs: crate::state::TaskOutputs::default(),
        };
        let config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: None,
            worker_routes: Vec::new(),
            skip_worker: true,
            require_worker: false,
        };

        let result = WorkerRegistry.run(WorkerRunRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
        })?;

        let packet = fs::read_to_string(result.packet_path)?;
        assert!(packet.contains(r#""worker": "codex""#));
        Ok(())
    }
}
