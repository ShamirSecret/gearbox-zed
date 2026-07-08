use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LanguageProfile {
    TypeScript,
    Python,
    Rust,
    Unknown,
}

impl LanguageProfile {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TypeScript => "typescript",
            Self::Python => "python",
            Self::Rust => "rust",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LanguageDetection {
    pub profile: LanguageProfile,
    pub product_type: String,
    pub evidence: Vec<String>,
    pub verification_commands: Vec<String>,
}

pub fn detect(
    workspace: &Path,
    override_commands: &[String],
    install_dependencies: bool,
) -> Result<LanguageDetection> {
    detect_with_request(workspace, override_commands, install_dependencies, "")
}

pub fn detect_with_request(
    workspace: &Path,
    override_commands: &[String],
    install_dependencies: bool,
    request: &str,
) -> Result<LanguageDetection> {
    let mut evidence = Vec::new();

    if exists(workspace, "package.json") {
        evidence.push("package.json".to_string());
        for file_name in [
            "tsconfig.json",
            "vite.config.ts",
            "vite.config.js",
            "next.config.ts",
            "next.config.js",
            "pnpm-lock.yaml",
            "bun.lock",
            "bun.lockb",
            "package-lock.json",
        ] {
            if exists(workspace, file_name) {
                evidence.push(file_name.to_string());
            }
        }

        let commands = if override_commands.is_empty() {
            typescript_verification_commands(workspace, install_dependencies)?
        } else {
            override_commands.to_vec()
        };

        return Ok(LanguageDetection {
            profile: LanguageProfile::TypeScript,
            product_type: "web_app".to_string(),
            evidence,
            verification_commands: commands,
        });
    }

    for file_name in [
        "pyproject.toml",
        "requirements.txt",
        "uv.lock",
        "poetry.lock",
        "pytest.ini",
    ] {
        if exists(workspace, file_name) {
            evidence.push(file_name.to_string());
        }
    }
    if !evidence.is_empty() {
        let commands = if override_commands.is_empty() {
            python_verification_commands(workspace)
        } else {
            override_commands.to_vec()
        };
        return Ok(LanguageDetection {
            profile: LanguageProfile::Python,
            product_type: "local_tool".to_string(),
            evidence,
            verification_commands: commands,
        });
    }

    if exists(workspace, "Cargo.toml") {
        evidence.push("Cargo.toml".to_string());
        if exists(workspace, "Cargo.lock") {
            evidence.push("Cargo.lock".to_string());
        }
        let commands = if override_commands.is_empty() {
            vec!["cargo check".to_string()]
        } else {
            override_commands.to_vec()
        };
        return Ok(LanguageDetection {
            profile: LanguageProfile::Rust,
            product_type: "local_tool".to_string(),
            evidence,
            verification_commands: commands,
        });
    }

    if looks_like_typescript_web_app_request(request) {
        let commands = if override_commands.is_empty() {
            typescript_scaffold_verification_commands(install_dependencies)
        } else {
            override_commands.to_vec()
        };
        return Ok(LanguageDetection {
            profile: LanguageProfile::TypeScript,
            product_type: "web_app".to_string(),
            evidence: vec!["prompt:web_app".to_string()],
            verification_commands: commands,
        });
    }

    Ok(LanguageDetection {
        profile: LanguageProfile::Unknown,
        product_type: "small_product".to_string(),
        evidence,
        verification_commands: override_commands.to_vec(),
    })
}

fn looks_like_typescript_web_app_request(request: &str) -> bool {
    let request = request.to_ascii_lowercase();
    let asks_for_app = [
        "app",
        "web app",
        "website",
        "dashboard",
        "page",
        "site",
        "frontend",
        "react",
        "vite",
        "页面",
        "网站",
        "应用",
        "前端",
        "仪表盘",
    ]
    .iter()
    .any(|keyword| request.contains(keyword));

    asks_for_app && !request.contains("cli") && !request.contains("command line")
}

fn exists(workspace: &Path, relative_path: &str) -> bool {
    workspace.join(relative_path).exists()
}

fn typescript_verification_commands(
    workspace: &Path,
    install_dependencies: bool,
) -> Result<Vec<String>> {
    let package_manager = detect_package_manager(workspace);
    let package_json_path = workspace.join("package.json");
    let package_json = fs::read_to_string(&package_json_path)
        .with_context(|| format!("failed to read {}", package_json_path.display()))?;
    let value: Value = serde_json::from_str(&package_json)
        .with_context(|| format!("failed to parse {}", package_json_path.display()))?;

    let scripts = value.get("scripts").and_then(Value::as_object);
    let mut commands = Vec::new();

    if install_dependencies && !workspace.join("node_modules").exists() {
        commands.push(format!("{} install", package_manager.executable));
    }

    for script in ["typecheck", "build", "test"] {
        if scripts.is_some_and(|scripts| scripts.contains_key(script)) {
            commands.push(package_manager.run_command(script));
        }
    }

    Ok(commands)
}

fn typescript_scaffold_verification_commands(install_dependencies: bool) -> Vec<String> {
    let mut commands = Vec::new();
    if install_dependencies {
        commands.push(guarded_package_json_command("npm install"));
    }
    commands.push(guarded_package_json_command("npm run build"));
    commands
}

fn guarded_package_json_command(command: &str) -> String {
    format!(
        "if test -f package.json; then {command}; else echo 'package.json was not generated'; exit 1; fi"
    )
}

fn python_verification_commands(workspace: &Path) -> Vec<String> {
    let mut commands = Vec::new();
    if workspace.join("uv.lock").exists() {
        commands.push("uv run pytest".to_string());
    } else if workspace.join("pytest.ini").exists() || workspace.join("tests").exists() {
        commands.push("pytest".to_string());
    }
    if workspace.join("ruff.toml").exists() || workspace.join(".ruff.toml").exists() {
        commands.push("ruff check .".to_string());
    }
    commands
}

#[derive(Clone, Debug)]
struct PackageManager {
    executable: &'static str,
}

impl PackageManager {
    fn run_command(&self, script: &str) -> String {
        match self.executable {
            "npm" if script == "test" => "npm test".to_string(),
            executable => format!("{executable} run {script}"),
        }
    }
}

fn detect_package_manager(workspace: &Path) -> PackageManager {
    let candidates = [
        ("bun.lock", "bun"),
        ("bun.lockb", "bun"),
        ("pnpm-lock.yaml", "pnpm"),
        ("package-lock.json", "npm"),
    ];

    for (file_name, executable) in candidates {
        if workspace.join(PathBuf::from(file_name)).exists() {
            return PackageManager { executable };
        }
    }

    PackageManager { executable: "npm" }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    #[test]
    fn detects_typescript_scripts() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{"scripts":{"typecheck":"tsc --noEmit","build":"vite build"}}"#,
        )?;

        let detection = detect(temp_dir.path(), &[], false)?;

        assert_eq!(detection.profile, LanguageProfile::TypeScript);
        assert_eq!(
            detection.verification_commands,
            vec!["npm run typecheck".to_string(), "npm run build".to_string()]
        );
        Ok(())
    }

    #[test]
    fn override_commands_take_precedence() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(
            temp_dir.path().join("Cargo.toml"),
            "[package]\nname = \"x\"\n",
        )?;

        let detection = detect(temp_dir.path(), &["echo ok".to_string()], false)?;

        assert_eq!(detection.profile, LanguageProfile::Rust);
        assert_eq!(detection.verification_commands, vec!["echo ok".to_string()]);
        Ok(())
    }

    #[test]
    fn empty_workspace_web_app_request_uses_typescript_default_stack() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let detection = detect_with_request(
            temp_dir.path(),
            &[],
            true,
            "Build a tiny habit tracker web app",
        )?;

        assert_eq!(detection.profile, LanguageProfile::TypeScript);
        assert_eq!(detection.product_type, "web_app");
        assert_eq!(detection.evidence, vec!["prompt:web_app".to_string()]);
        assert_eq!(
            detection.verification_commands,
            vec![
                "if test -f package.json; then npm install; else echo 'package.json was not generated'; exit 1; fi".to_string(),
                "if test -f package.json; then npm run build; else echo 'package.json was not generated'; exit 1; fi".to_string(),
            ]
        );
        Ok(())
    }

    #[test]
    fn empty_workspace_cli_request_stays_unknown_without_files() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let detection = detect_with_request(temp_dir.path(), &[], false, "Build a small CLI tool")?;

        assert_eq!(detection.profile, LanguageProfile::Unknown);
        assert_eq!(detection.verification_commands, Vec::<String>::new());
        Ok(())
    }
}
