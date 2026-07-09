use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use smol::process::{Command, Stdio};

use crate::state::{CommandRecord, Scope};

const OUTPUT_LIMIT: usize = 12_000;
static COMMAND_OUTPUT_COUNTER: AtomicU64 = AtomicU64::new(1);

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
    check_cancelled(cancellation_token, command)?;

    let started_at = Instant::now();
    let stdout_path = command_output_path(workspace, "stdout")?;
    let stderr_path = command_output_path(workspace, "stderr")?;
    let stdout = fs::File::create(&stdout_path)
        .with_context(|| format!("failed to create {}", stdout_path.display()))?;
    let stderr = fs::File::create(&stderr_path)
        .with_context(|| format!("failed to create {}", stderr_path.display()))?;

    let mut process = shell_command(command);
    process
        .current_dir(workspace)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    for (key, value) in env {
        process.env(key, value);
    }

    let mut child = process
        .spawn()
        .with_context(|| format!("failed to run command `{command}`"))?;
    let status = loop {
        if cancellation_token.is_some_and(CancellationToken::is_cancelled) {
            if let Err(error) = child.kill()
                && error.kind() != std::io::ErrorKind::InvalidInput
            {
                return Err(error)
                    .with_context(|| format!("failed to kill cancelled command `{command}`"));
            }
            if let Err(error) = smol::block_on(child.status())
                && error.kind() != std::io::ErrorKind::InvalidInput
            {
                return Err(error)
                    .with_context(|| format!("failed to wait for cancelled command `{command}`"));
            }
            cleanup_command_output(&stdout_path);
            cleanup_command_output(&stderr_path);
            bail!("Gear run cancelled while running `{command}`");
        }

        if let Some(status) = child
            .try_status()
            .with_context(|| format!("failed to poll command `{command}`"))?
        {
            break status;
        }

        std::thread::sleep(Duration::from_millis(50));
    };

    let stdout = fs::read_to_string(&stdout_path)
        .with_context(|| format!("failed to read {}", stdout_path.display()))?;
    let stderr = fs::read_to_string(&stderr_path)
        .with_context(|| format!("failed to read {}", stderr_path.display()))?;
    cleanup_command_output(&stdout_path);
    cleanup_command_output(&stderr_path);

    Ok(ShellCommandResult {
        command: command.to_string(),
        exit_code: status.code(),
        success: status.success(),
        stdout: truncate(&stdout, OUTPUT_LIMIT),
        stderr: truncate(&stderr, OUTPUT_LIMIT),
        duration_ms: started_at.elapsed().as_millis(),
    })
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
                !scope.allowed_paths.iter().any(|allowed_path| {
                    path == &allowed_path || path.starts_with(&format!("{allowed_path}/"))
                })
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

fn run_raw_git(workspace: &Path, args: &[&str]) -> Result<ShellCommandResult> {
    let command = format!("git {}", args.join(" "));
    let started_at = Instant::now();
    let output = Command::new("git")
        .args(args)
        .current_dir(workspace)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();
    let output = smol::block_on(output).with_context(|| format!("failed to run `{command}`"))?;

    Ok(ShellCommandResult {
        command,
        exit_code: output.status.code(),
        success: output.status.success(),
        stdout: truncate(&String::from_utf8_lossy(&output.stdout), OUTPUT_LIMIT),
        stderr: truncate(&String::from_utf8_lossy(&output.stderr), OUTPUT_LIMIT),
        duration_ms: started_at.elapsed().as_millis(),
    })
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
            if path.is_empty() || path.starts_with(".gearbox-agent/") {
                None
            } else {
                Some(path.to_string())
            }
        })
        .collect()
}

fn shell_command(command: &str) -> Command {
    if cfg!(windows) {
        let mut process = Command::new("cmd");
        process.args(["/C", command]);
        process
    } else {
        let mut process = Command::new("sh");
        process.args(["-lc", command]);
        process
    }
}

fn command_output_path(workspace: &Path, stream: &str) -> Result<PathBuf> {
    let output_dir = workspace.join(".gearbox-agent").join("tmp");
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
}
