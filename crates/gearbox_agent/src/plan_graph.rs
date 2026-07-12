use crate::state::{Scope, Task, TaskInputs, TaskKind, TaskOutputs, TaskStatus, timestamp};
use crate::workers::WorkerKind;
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::{HashMap, HashSet};

pub const PLAN_GRAPH_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanSource {
    PlannerModel,
    DeterministicFallback,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerReceipt {
    pub provider_id: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanGraphDraft {
    pub objective: String,
    #[serde(default)]
    pub must_have: Vec<String>,
    #[serde(default)]
    pub must_not_have: Vec<String>,
    #[serde(default)]
    pub topology_lock: Vec<String>,
    pub tasks: Vec<PlanTaskContract>,
    #[serde(default)]
    pub final_acceptance: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanGraph {
    pub schema_version: u32,
    pub plan_id: String,
    pub goal_id: String,
    pub revision: usize,
    pub generated_at: String,
    pub source: PlanSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner: Option<PlannerReceipt>,
    pub plan_hash: String,
    pub draft: PlanGraphDraft,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanTaskContract {
    pub task_id: String,
    pub title: String,
    pub goal: String,
    pub deliverable: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub parallel_wave: usize,
    pub scope: PlanTaskScope,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    pub preferred_phase_profile: PhaseProfile,
    pub must_do: Vec<String>,
    pub must_not_do: Vec<String>,
    #[serde(default)]
    pub references: Vec<PlanReference>,
    pub test: PlanTestContract,
    pub qa: PlanQaContract,
    pub artifacts: Vec<PlanArtifactContract>,
    pub commit_boundary: CommitBoundary,
    pub completion_predicates: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanTaskScope {
    #[serde(default)]
    pub allowed_files: Vec<String>,
    #[serde(default)]
    pub forbidden_files: Vec<String>,
    #[serde(default)]
    pub write_scope: Vec<String>,
    pub max_files_changed: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseProfile {
    Planner,
    PlanCritic,
    Orchestrator,
    ExecutorQuick,
    ExecutorDeep,
    ReviewerTask,
    ReviewerFinal,
    StrategistNextGoal,
    Summarizer,
}

impl PhaseProfile {
    pub const fn all() -> [Self; 9] {
        [
            Self::Planner,
            Self::PlanCritic,
            Self::Orchestrator,
            Self::ExecutorQuick,
            Self::ExecutorDeep,
            Self::ReviewerTask,
            Self::ReviewerFinal,
            Self::StrategistNextGoal,
            Self::Summarizer,
        ]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStrategy {
    Tdd,
    TestsAfter,
    None,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanTestContract {
    pub strategy: TestStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub red: Option<CommandExpectation>,
    #[serde(default)]
    pub green: Vec<CommandExpectation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_test_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandExpectation {
    pub command: String,
    pub expected_observation: String,
    pub evidence_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanQaContract {
    pub happy_path: Vec<QaScenario>,
    pub failure_path: Vec<QaScenario>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QaScenario {
    pub name: String,
    pub steps: Vec<String>,
    pub expected_result: String,
    pub evidence_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanReference {
    pub path: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanArtifactContract {
    pub path: String,
    pub description: String,
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitBoundary {
    NoCommit,
    AfterTask,
    AfterWave,
}

impl PlanGraph {
    pub fn seal(
        goal_id: &str,
        revision: usize,
        source: PlanSource,
        planner: Option<PlannerReceipt>,
        draft: PlanGraphDraft,
    ) -> Result<Self> {
        let plan_hash = draft_hash(&draft)?;
        let plan_id = format!("plan_{}", &plan_hash[..16]);
        let graph = Self {
            schema_version: PLAN_GRAPH_SCHEMA_VERSION,
            plan_id,
            goal_id: goal_id.to_string(),
            revision,
            generated_at: timestamp(),
            source,
            planner,
            plan_hash,
            draft,
        };
        graph.validate()?;
        Ok(graph)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PLAN_GRAPH_SCHEMA_VERSION {
            bail!(
                "unsupported PlanGraph schema version {}",
                self.schema_version
            );
        }
        if self.draft.objective.trim().is_empty() {
            bail!("PlanGraph objective cannot be empty");
        }
        if self.draft.tasks.is_empty() {
            bail!("PlanGraph must contain at least one task");
        }
        let expected_hash = draft_hash(&self.draft)?;
        if self.plan_hash != expected_hash {
            bail!("PlanGraph hash does not match its draft");
        }
        let expected_plan_id = format!("plan_{}", &expected_hash[..16]);
        if self.plan_id != expected_plan_id {
            bail!("PlanGraph id does not match its draft hash");
        }
        match (&self.source, &self.planner) {
            (PlanSource::PlannerModel, Some(planner))
                if !planner.provider_id.trim().is_empty()
                    && !planner.model_id.trim().is_empty() => {}
            (PlanSource::PlannerModel, _) => {
                bail!("planner-model PlanGraph requires a concrete planner receipt")
            }
            (PlanSource::DeterministicFallback, None) => {}
            (PlanSource::DeterministicFallback, Some(_)) => {
                bail!("deterministic fallback PlanGraph cannot claim a planner receipt")
            }
        }

        let mut tasks_by_id = HashMap::new();
        for task in &self.draft.tasks {
            task.validate()?;
            if tasks_by_id.insert(task.task_id.as_str(), task).is_some() {
                bail!("duplicate PlanGraph task id `{}`", task.task_id);
            }
        }

        for task in &self.draft.tasks {
            for dependency in &task.dependencies {
                let dependency_task = tasks_by_id.get(dependency.as_str()).with_context(|| {
                    format!(
                        "PlanGraph task `{}` depends on missing task `{dependency}`",
                        task.task_id
                    )
                })?;
                if dependency == &task.task_id {
                    bail!("PlanGraph task `{}` cannot depend on itself", task.task_id);
                }
                if dependency_task.parallel_wave >= task.parallel_wave {
                    bail!(
                        "PlanGraph dependency `{dependency}` must be in an earlier wave than `{}`",
                        task.task_id
                    );
                }
            }
        }
        validate_acyclic(&self.draft.tasks)?;
        validate_wave_write_scopes(&self.draft.tasks)?;
        Ok(())
    }

    pub fn task(&self, task_id: &str) -> Option<&PlanTaskContract> {
        self.draft.tasks.iter().find(|task| task.task_id == task_id)
    }

    pub fn next_runnable_task(
        &self,
        completed: &HashSet<String>,
    ) -> Result<Option<&PlanTaskContract>> {
        Ok(self.runnable_tasks(completed, &HashSet::new())?.into_iter().next())
    }

    /// Return every task whose dependencies are complete and which is not
    /// already active. The runtime uses this as the scheduler input; model
    /// output and Markdown projections never participate in this decision.
    pub fn runnable_tasks(
        &self,
        completed: &HashSet<String>,
        active: &HashSet<String>,
    ) -> Result<Vec<&PlanTaskContract>> {
        self.validate()?;
        let mut runnable = self
            .draft
            .tasks
            .iter()
            .filter(|task| {
                !completed.contains(&task.task_id) && !active.contains(&task.task_id)
            })
            .filter(|task| {
                task.dependencies
                    .iter()
                    .all(|dependency| completed.contains(dependency))
            })
            .collect::<Vec<_>>();
        runnable.sort_by_key(|task| (task.parallel_wave, task.task_id.as_str()));
        Ok(runnable)
    }

    /// Select the earliest dependency-ready wave up to the caller's worker
    /// capacity. The returned order is stable, so a resumed runtime can
    /// persist the same dispatch order without consulting a model.
    pub fn runnable_wave(
        &self,
        completed: &HashSet<String>,
        active: &HashSet<String>,
        capacity: usize,
    ) -> Result<Vec<&PlanTaskContract>> {
        let capacity = capacity.max(1);
        let runnable = self.runnable_tasks(completed, active)?;
        let Some(first_wave) = runnable.first().map(|task| task.parallel_wave) else {
            return Ok(Vec::new());
        };
        Ok(runnable
            .into_iter()
            .filter(|task| task.parallel_wave == first_wave)
            .take(capacity)
            .collect())
    }

    pub fn closed_world_contract(&self) -> PlanTaskContract {
        let first = &self.draft.tasks[0];
        let mut contract = first.clone();
        contract.task_id = "task_003".to_string();
        contract.title = format!("Execute approved plan {}", self.plan_id);
        contract.goal = self.draft.objective.clone();
        contract.deliverable = self
            .draft
            .tasks
            .iter()
            .map(|task| format!("{}: {}", task.task_id, task.deliverable))
            .collect::<Vec<_>>()
            .join("; ");
        contract.dependencies.clear();
        contract.parallel_wave = 0;
        contract.must_do = self
            .draft
            .tasks
            .iter()
            .flat_map(|task| task.must_do.iter().cloned())
            .collect();
        contract.must_not_do = self
            .draft
            .must_not_have
            .iter()
            .cloned()
            .chain(
                self.draft
                    .tasks
                    .iter()
                    .flat_map(|task| task.must_not_do.iter().cloned()),
            )
            .collect();
        contract.references = self
            .draft
            .tasks
            .iter()
            .flat_map(|task| task.references.iter().cloned())
            .collect();
        contract.artifacts = self
            .draft
            .tasks
            .iter()
            .flat_map(|task| task.artifacts.iter().cloned())
            .collect();
        contract.completion_predicates = self
            .draft
            .final_acceptance
            .iter()
            .cloned()
            .chain(
                self.draft
                    .tasks
                    .iter()
                    .flat_map(|task| task.completion_predicates.iter().cloned()),
            )
            .collect();
        contract
    }
}

fn draft_hash(draft: &PlanGraphDraft) -> Result<String> {
    let canonical = serde_json::to_vec(draft).context("failed to serialize PlanGraph draft")?;
    Ok(format!("{:x}", Sha256::digest(canonical)))
}

impl PlanTaskContract {
    pub fn validate(&self) -> Result<()> {
        if self.task_id.trim().is_empty()
            || !self.task_id.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            })
        {
            bail!("PlanGraph task id must be a non-empty ASCII identifier");
        }
        for (field, value) in [
            ("title", self.title.as_str()),
            ("goal", self.goal.as_str()),
            ("deliverable", self.deliverable.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("PlanGraph task `{}` has empty {field}", self.task_id);
            }
        }
        if self.must_do.is_empty()
            || self.artifacts.is_empty()
            || self.completion_predicates.is_empty()
        {
            bail!(
                "PlanGraph task `{}` must define must_do, artifacts, and completion_predicates",
                self.task_id
            );
        }
        if self.qa.happy_path.is_empty() || self.qa.failure_path.is_empty() {
            bail!(
                "PlanGraph task `{}` must define happy and failure QA",
                self.task_id
            );
        }
        for scenario in self.qa.happy_path.iter().chain(self.qa.failure_path.iter()) {
            if scenario.name.trim().is_empty()
                || scenario.steps.is_empty()
                || scenario.expected_result.trim().is_empty()
                || scenario.evidence_path.trim().is_empty()
            {
                bail!(
                    "PlanGraph task `{}` has incomplete QA scenario",
                    self.task_id
                );
            }
        }
        match self.test.strategy {
            TestStrategy::Tdd => {
                let red = self.test.red.as_ref().with_context(|| {
                    format!("TDD task `{}` must define a RED command", self.task_id)
                })?;
                let green = self.test.green.first().with_context(|| {
                    format!("TDD task `{}` must define a GREEN command", self.task_id)
                })?;
                if red.command != green.command {
                    bail!(
                        "TDD task `{}` must use the same command for RED and first GREEN evidence",
                        self.task_id
                    );
                }
            }
            TestStrategy::TestsAfter if self.test.green.is_empty() => {
                bail!(
                    "tests-after task `{}` must define GREEN commands",
                    self.task_id
                );
            }
            TestStrategy::None
                if self
                    .test
                    .no_test_reason
                    .as_deref()
                    .is_none_or(|reason| reason.trim().is_empty()) =>
            {
                bail!("no-test task `{}` must explain why", self.task_id);
            }
            TestStrategy::TestsAfter | TestStrategy::None => {}
        }
        Ok(())
    }

    pub fn to_runtime_task(&self, goal_id: &str, worker_kind: WorkerKind) -> Task {
        Task {
            id: self.task_id.clone(),
            goal_id: goal_id.to_string(),
            parent_task_id: None,
            title: self.title.clone(),
            kind: TaskKind::Edit,
            status: TaskStatus::Pending,
            assigned_worker: Some(worker_kind.as_str().to_string()),
            attempt: 1,
            scope: Scope::new(
                self.scope.allowed_files.clone(),
                self.scope.forbidden_files.clone(),
                self.scope.max_files_changed,
            ),
            inputs: TaskInputs {
                plan_task: Some(self.clone()),
                ..TaskInputs::default()
            },
            outputs: TaskOutputs::default(),
        }
    }

    pub fn worker_goal(&self, execution_request: &str) -> String {
        format!(
            "Approved goal: {}\n\nApproved deliverable: {}\n\nCurrent turn instruction: {}\n\nDo not redesign the plan. Return a typed plan gap if the contract cannot be executed as written.",
            self.goal, self.deliverable, execution_request
        )
    }

    pub fn worker_constraints(&self) -> Vec<String> {
        self.must_do
            .iter()
            .map(|requirement| format!("MUST: {requirement}"))
            .chain(
                self.must_not_do
                    .iter()
                    .map(|requirement| format!("MUST NOT: {requirement}")),
            )
            .chain(std::iter::once(format!(
                "Write scope: {}",
                if self.scope.write_scope.is_empty() {
                    "no writes allowed".to_string()
                } else {
                    self.scope.write_scope.join(", ")
                }
            )))
            .collect()
    }

    pub fn worker_verification_commands(&self) -> Vec<String> {
        self.test
            .red
            .iter()
            .map(|command| command.command.clone())
            .chain(
                self.test
                    .green
                    .iter()
                    .map(|command| command.command.clone()),
            )
            .fold(Vec::new(), |mut commands, command| {
                if !commands.contains(&command) {
                    commands.push(command);
                }
                commands
            })
    }

    pub fn worker_required_outputs(&self) -> Vec<String> {
        [
            "summary",
            "changed_files",
            "commands_run",
            "known_failures",
            "next_steps",
            "plan_gap",
        ]
        .into_iter()
        .map(ToString::to_string)
        .chain(
            self.artifacts
                .iter()
                .filter(|artifact| artifact.required)
                .map(|artifact| format!("artifact:{}", artifact.path)),
        )
        .collect()
    }

    pub fn worker_stop_conditions(&self) -> Vec<String> {
        vec![
            "The approved scope, dependency, or acceptance contract is incomplete.".to_string(),
            "Execution requires a forbidden path or unapproved irreversible action.".to_string(),
            "RED fails for an environment or syntax reason instead of the planned missing behavior."
                .to_string(),
            "The same root cause fails twice without new evidence.".to_string(),
        ]
    }
}

pub fn deterministic_fallback_draft(
    objective: &str,
    scope: &Scope,
    verification_commands: &[String],
) -> PlanGraphDraft {
    let test = if verification_commands.is_empty() {
        PlanTestContract {
            strategy: TestStrategy::None,
            red: None,
            green: Vec::new(),
            no_test_reason: Some(
                "No project verification command was detected; deterministic inspection is required."
                    .to_string(),
            ),
        }
    } else {
        PlanTestContract {
            strategy: TestStrategy::TestsAfter,
            red: None,
            green: verification_commands
                .iter()
                .map(|command| CommandExpectation {
                    command: command.clone(),
                    expected_observation: "command exits successfully".to_string(),
                    evidence_path: ".gearbox-agent/artifacts/verification.md".to_string(),
                })
                .collect(),
            no_test_reason: None,
        }
    };
    PlanGraphDraft {
        objective: objective.to_string(),
        must_have: vec!["Satisfy the original objective with inspectable evidence.".to_string()],
        must_not_have: vec!["Do not expand scope beyond the original objective.".to_string()],
        topology_lock: vec![
            "Preserve the existing repository architecture unless required.".to_string(),
        ],
        tasks: vec![PlanTaskContract {
            task_id: "task_003".to_string(),
            title: "Execute the bounded implementation contract".to_string(),
            goal: objective.to_string(),
            deliverable: "A minimal verified implementation of the requested change.".to_string(),
            dependencies: Vec::new(),
            parallel_wave: 0,
            scope: PlanTaskScope {
                allowed_files: scope.allowed_paths.clone(),
                forbidden_files: scope.forbidden_paths.clone(),
                write_scope: scope.allowed_paths.clone(),
                max_files_changed: scope.max_files_changed,
            },
            required_capabilities: vec!["read".to_string(), "edit".to_string(), "test".to_string()],
            preferred_phase_profile: PhaseProfile::ExecutorQuick,
            must_do: vec![
                "Inspect relevant repository code before editing.".to_string(),
                "Make the smallest change that satisfies the objective.".to_string(),
                "Record verification and known failures.".to_string(),
            ],
            must_not_do: vec!["Do not redesign unrelated code.".to_string()],
            references: Vec::new(),
            test,
            qa: PlanQaContract {
                happy_path: vec![QaScenario {
                    name: "requested behavior".to_string(),
                    steps: vec![
                        "Run the relevant verification command or deterministic check.".to_string(),
                    ],
                    expected_result: "The requested behavior is present and inspectable."
                        .to_string(),
                    evidence_path: ".gearbox-agent/artifacts/verification.md".to_string(),
                }],
                failure_path: vec![QaScenario {
                    name: "verification failure".to_string(),
                    steps: vec!["Capture the failing command and root-cause evidence.".to_string()],
                    expected_result: "The task remains incomplete with an explicit repair request."
                        .to_string(),
                    evidence_path: ".gearbox-agent/artifacts/verification.md".to_string(),
                }],
            },
            artifacts: vec![PlanArtifactContract {
                path: ".gearbox-agent/artifacts/final-report.md".to_string(),
                description: "Final implementation and verification report.".to_string(),
                required: true,
            }],
            commit_boundary: CommitBoundary::NoCommit,
            completion_predicates: vec![
                "The requested change is implemented within scope.".to_string(),
                "Verification evidence is recorded.".to_string(),
            ],
        }],
        final_acceptance: vec![
            "All required artifacts are readable.".to_string(),
            "No forbidden path was modified.".to_string(),
        ],
    }
}

pub fn parse_planner_draft(output: &str) -> Result<PlanGraphDraft> {
    let trimmed = output.trim();
    let json = if let Some(rest) = trimmed.strip_prefix("```json") {
        rest.strip_suffix("```").unwrap_or(rest).trim()
    } else if let Some(rest) = trimmed.strip_prefix("```") {
        rest.strip_suffix("```").unwrap_or(rest).trim()
    } else {
        trimmed
    };
    serde_json::from_str(json).context("planner did not return a valid PlanGraphDraft JSON object")
}

fn validate_acyclic(tasks: &[PlanTaskContract]) -> Result<()> {
    fn visit<'a>(
        task_id: &'a str,
        tasks: &HashMap<&'a str, &'a PlanTaskContract>,
        visiting: &mut HashSet<&'a str>,
        visited: &mut HashSet<&'a str>,
    ) -> Result<()> {
        if visited.contains(task_id) {
            return Ok(());
        }
        if !visiting.insert(task_id) {
            bail!("PlanGraph dependency cycle includes `{task_id}`");
        }
        let task = tasks
            .get(task_id)
            .with_context(|| format!("missing PlanGraph task `{task_id}`"))?;
        for dependency in &task.dependencies {
            visit(dependency, tasks, visiting, visited)?;
        }
        visiting.remove(task_id);
        visited.insert(task_id);
        Ok(())
    }

    let tasks_by_id = tasks
        .iter()
        .map(|task| (task.task_id.as_str(), task))
        .collect::<HashMap<_, _>>();
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    for task in tasks {
        visit(&task.task_id, &tasks_by_id, &mut visiting, &mut visited)?;
    }
    Ok(())
}

fn validate_wave_write_scopes(tasks: &[PlanTaskContract]) -> Result<()> {
    for (index, left) in tasks.iter().enumerate() {
        for right in tasks.iter().skip(index + 1) {
            if left.parallel_wave != right.parallel_wave {
                continue;
            }
            for left_scope in &left.scope.write_scope {
                for right_scope in &right.scope.write_scope {
                    let left_scope = left_scope.trim_end_matches('/');
                    let right_scope = right_scope.trim_end_matches('/');
                    if left_scope == right_scope
                        || left_scope.starts_with(&format!("{right_scope}/"))
                        || right_scope.starts_with(&format!("{left_scope}/"))
                    {
                        bail!(
                            "PlanGraph wave {} has overlapping write scopes `{}` and `{}`",
                            left.parallel_wave,
                            left.scope.write_scope.join(", "),
                            right.scope.write_scope.join(", ")
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_draft() -> PlanGraphDraft {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let mut draft =
            deterministic_fallback_draft("Implement feature", &scope, &["cargo test".to_string()]);
        let task = &mut draft.tasks[0];
        task.test.strategy = TestStrategy::Tdd;
        task.test.red = Some(CommandExpectation {
            command: "cargo test feature".to_string(),
            expected_observation: "feature test fails for the missing behavior".to_string(),
            evidence_path: "evidence/red.txt".to_string(),
        });
        task.test.green = vec![CommandExpectation {
            command: "cargo test feature".to_string(),
            expected_observation: "feature test passes".to_string(),
            evidence_path: "evidence/green.txt".to_string(),
        }];
        draft
    }

    fn planner_receipt() -> Option<PlannerReceipt> {
        Some(PlannerReceipt {
            provider_id: "test-provider".to_string(),
            model_id: "test-model".to_string(),
            session_id: None,
        })
    }

    #[test]
    fn plan_graph_validates_decision_complete_tdd_contract() -> Result<()> {
        PlanGraph::seal(
            "goal-1",
            1,
            PlanSource::PlannerModel,
            planner_receipt(),
            valid_draft(),
        )?;
        Ok(())
    }

    #[test]
    fn plan_graph_rejects_missing_dependency() {
        let mut draft = valid_draft();
        draft.tasks[0].dependencies.push("missing".to_string());
        assert!(
            PlanGraph::seal(
                "goal-1",
                1,
                PlanSource::PlannerModel,
                planner_receipt(),
                draft,
            )
            .is_err()
        );
    }

    #[test]
    fn plan_graph_rejects_tdd_without_matching_red_green() {
        let mut draft = valid_draft();
        draft.tasks[0].test.green[0].command = "cargo test other".to_string();
        assert!(
            PlanGraph::seal(
                "goal-1",
                1,
                PlanSource::PlannerModel,
                planner_receipt(),
                draft,
            )
            .is_err()
        );
    }

    #[test]
    fn plan_graph_rejects_same_wave_write_scope_collision() {
        let mut draft = valid_draft();
        let mut second = draft.tasks[0].clone();
        second.task_id = "task_004".to_string();
        second.title = "Second task".to_string();
        draft.tasks.push(second);
        assert!(
            PlanGraph::seal(
                "goal-1",
                1,
                PlanSource::PlannerModel,
                planner_receipt(),
                draft,
            )
            .is_err()
        );
    }

    #[test]
    fn plan_graph_hash_is_stable_across_round_trip() -> Result<()> {
        let graph = PlanGraph::seal(
            "goal-1",
            1,
            PlanSource::PlannerModel,
            planner_receipt(),
            valid_draft(),
        )?;
        let round_trip: PlanGraph = serde_json::from_str(&serde_json::to_string(&graph)?)?;
        let resealed = PlanGraph::seal(
            "goal-1",
            1,
            PlanSource::PlannerModel,
            planner_receipt(),
            round_trip.draft,
        )?;
        assert_eq!(graph.plan_hash, resealed.plan_hash);
        Ok(())
    }

    #[test]
    fn state_store_round_trips_plan_graph() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = crate::state::StateStore::new(temp_dir.path());
        store.initialize()?;
        let graph = PlanGraph::seal(
            "goal-1",
            1,
            PlanSource::PlannerModel,
            planner_receipt(),
            valid_draft(),
        )?;
        assert!(store.write_plan_graph(&graph).is_err());
        let path = store.write_unreviewed_plan_graph(&graph)?;
        assert!(path.exists());
        assert_eq!(store.read_unreviewed_plan_graph("goal-1")?, Some(graph));
        Ok(())
    }

    #[test]
    fn plan_graph_rejects_tampered_hash_and_missing_planner_receipt() -> Result<()> {
        assert!(
            PlanGraph::seal("goal-1", 1, PlanSource::PlannerModel, None, valid_draft()).is_err()
        );
        let mut graph = PlanGraph::seal(
            "goal-1",
            1,
            PlanSource::PlannerModel,
            planner_receipt(),
            valid_draft(),
        )?;
        graph.draft.objective.push_str(" tampered");
        assert!(graph.validate().is_err());
        Ok(())
    }

    #[test]
    fn runnable_tasks_returns_all_ready_nodes_without_active_nodes() -> Result<()> {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let mut draft = deterministic_fallback_draft("graph", &scope, &[]);
        draft.tasks[0].task_id = "node_a".to_string();
        draft.tasks[0].title = "A".to_string();
        draft.tasks[0].scope.write_scope = vec!["src/a".to_string()];
        let mut node_b = draft.tasks[0].clone();
        node_b.task_id = "node_b".to_string();
        node_b.title = "B".to_string();
        node_b.scope.write_scope = vec!["src/b".to_string()];
        let mut node_c = draft.tasks[0].clone();
        node_c.task_id = "node_c".to_string();
        node_c.title = "C".to_string();
        node_c.scope.write_scope = vec!["src/c".to_string()];
        let first = draft.tasks.remove(0);
        draft.tasks = vec![first, node_b, node_c];
        let graph = PlanGraph::seal(
            "goal",
            1,
            PlanSource::DeterministicFallback,
            None,
            draft,
        )?;

        let ready = graph.runnable_tasks(&HashSet::new(), &HashSet::new())?;
        assert_eq!(
            ready
                .iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node_a", "node_b", "node_c"]
        );
        let active = HashSet::from([String::from("node_b")]);
        let ready = graph.runnable_tasks(&HashSet::new(), &active)?;
        assert_eq!(
            ready
                .iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node_a", "node_c"]
        );
        let wave = graph.runnable_wave(&HashSet::new(), &HashSet::new(), 2)?;
        assert_eq!(
            wave.iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node_a", "node_b"]
        );
        Ok(())
    }

    #[test]
    fn runnable_tasks_respects_dependencies() -> Result<()> {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let mut draft = deterministic_fallback_draft("graph", &scope, &[]);
        draft.tasks[0].task_id = "node_a".to_string();
        draft.tasks[0].scope.write_scope = vec!["src/a".to_string()];
        let mut node_b = draft.tasks[0].clone();
        node_b.task_id = "node_b".to_string();
        node_b.dependencies = vec!["node_a".to_string()];
        node_b.parallel_wave = 1;
        node_b.scope.write_scope = vec!["src/b".to_string()];
        draft.tasks.push(node_b);
        let graph = PlanGraph::seal(
            "goal",
            1,
            PlanSource::DeterministicFallback,
            None,
            draft,
        )?;

        let ready = graph.runnable_tasks(&HashSet::new(), &HashSet::new())?;
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, "node_a");
        let completed = HashSet::from([String::from("node_a")]);
        let ready = graph.runnable_tasks(&completed, &HashSet::new())?;
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, "node_b");
        Ok(())
    }

    #[test]
    fn plan_node_run_ledger_is_persisted_and_rejects_evidence_less_completion() -> Result<()> {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let graph = PlanGraph::seal(
            "goal",
            1,
            PlanSource::DeterministicFallback,
            None,
            deterministic_fallback_draft("graph", &scope, &[]),
        )?;
        let mut ledger =
            crate::state::PlanNodeRunLedger::from_plan("goal", "epoch", &graph)?;
        assert!(ledger
            .mark("task_003", crate::state::PlanNodeRunStatus::Completed)
            .is_err());

        let node = ledger.node_mut("task_003")?;
        node.status = crate::state::PlanNodeRunStatus::Completed;
        node.attempt = 1;
        node.green_evidence_paths.push("green.md".to_string());
        node.review_evidence_path = Some("review.md".to_string());
        ledger.validate()?;
        let temp_dir = tempfile::tempdir()?;
        let store = crate::state::StateStore::new(temp_dir.path());
        store.initialize()?;
        let path = store.write_plan_node_runs(&ledger)?;
        assert!(path.is_file());
        assert_eq!(store.read_plan_node_runs("goal")?, Some(ledger));
        Ok(())
    }

    #[test]
    fn final_verification_wave_receipt_is_typed_and_hash_bound() -> Result<()> {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let graph = PlanGraph::seal(
            "goal",
            1,
            PlanSource::DeterministicFallback,
            None,
            deterministic_fallback_draft("graph", &scope, &[]),
        )?;
        let dimensions = [
            crate::state::FinalVerificationDimension::PlanCompliance,
            crate::state::FinalVerificationDimension::CodeQuality,
            crate::state::FinalVerificationDimension::RealQa,
            crate::state::FinalVerificationDimension::ScopeFidelity,
        ]
        .into_iter()
        .map(|dimension| crate::state::FinalVerificationResult {
            dimension,
            passed: true,
            summary: "evidence-backed pass".to_string(),
            evidence_paths: vec!["evidence.md".to_string()],
            reviewer_execution_ids: vec!["reviewer-1".to_string()],
        })
        .collect();
        let receipt = crate::state::FinalVerificationWaveReceipt::seal(
            "goal",
            "epoch",
            &graph,
            dimensions,
        )?;
        receipt.validate(&graph)?;
        let mut tampered = receipt.clone();
        tampered.plan_hash = "f".repeat(64);
        assert!(tampered.validate(&graph).is_err());
        Ok(())
    }
}
