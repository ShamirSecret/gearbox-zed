use crate::state::{Scope, Task, TaskInputs, TaskKind, TaskOutputs, TaskStatus, timestamp};
use crate::workers::WorkerKind;
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::collections::{HashMap, HashSet};

pub const PLAN_GRAPH_SCHEMA_VERSION: u32 = 1;

/// Compact contract example embedded in planner repair prompts. Keep this
/// synchronized with the typed draft by exercising it in the unit tests below.
pub const PLAN_GRAPH_SCHEMA_EXEMPLAR: &str = r#"{
  "objective": "observable outcome",
  "assumptions": ["reversible default and rationale"],
  "findings": ["path:line — repository fact"],
  "decisions": ["decision — rationale"],
  "open_questions": [],
  "must_have": ["acceptance signal"],
  "must_not_have": ["forbidden change"],
  "topology_lock": ["task_a"],
  "preflight": ["record baseline before editing"],
  "rollback": ["restore the bounded change if final verification fails"],
  "final_verification": ["run the final verification wave and persist evidence"],
  "tasks": [{
    "task_id": "task_a",
    "title": "Implement the bounded change",
    "goal": "Deliver the requested behavior",
    "deliverable": "verified implementation",
    "rationale": "The requested behavior is missing from the current repository baseline",
    "approach": ["Inspect the existing seam, implement the bounded change, and verify it"],
    "already_in_working_tree": ["the existing seam is present"],
    "still_needed": ["add the missing behavior and evidence"],
    "dependencies": [],
    "parallel_wave": 0,
    "scope": {
      "allowed_files": ["src/example.rs"],
      "forbidden_files": [".git"],
      "write_scope": ["src/example.rs"],
      "max_files_changed": 1
    },
    "required_capabilities": ["file_write"],
    "preferred_phase_profile": "executor_quick",
    "inputs": ["read the repository discovery artifact"],
    "preconditions": ["the baseline has been recorded"],
    "must_do": ["implement the behavior"],
    "execution_steps": [{"step_id": "step-001", "action": "implement the behavior", "expected_observation": "the bounded change is present", "evidence_path": null}],
    "execution_steps_evidence_required": true,
    "must_not_do": ["modify forbidden paths"],
    "references": [{"path": "src/example.rs", "reason": "implementation entry point"}],
    "test": {
      "strategy": "tests_after",
      "red": null,
      "green": [{"command": "cargo test", "expected_observation": "tests pass", "evidence_path": ".gear/artifacts/green.log"}],
      "no_test_reason": null
    },
    "qa": {
      "happy_path": [{"name": "happy", "steps": ["run the verification"], "expected_result": "behavior is present", "evidence_path": ".gear/artifacts/qa.log"}],
      "failure_path": [{"name": "failure", "steps": ["capture the failure"], "expected_result": "failure is diagnosable", "evidence_path": ".gear/artifacts/qa.log"}],
      "adversarial_path": [{"name": "adversarial-not-applicable", "steps": ["check the trigger map"], "expected_result": "no applicable adversarial trigger", "evidence_path": ".gear/artifacts/qa.log"}]
    },
    "artifacts": [{"path": ".gear/artifacts/final-report.md", "description": "verification report", "required": true}],
    "evidence": ["record the command exit status and changed paths"],
    "rollback": ["restore the task-scoped changes if verification fails"],
    "budget": {"max_attempts": 2, "max_commands": 3, "max_duration_seconds": null},
    "commit_boundary": "no_commit",
    "commit_message": null,
    "completion_predicates": ["verification evidence exists"]
  }],
  "final_acceptance": ["the observable outcome is verified"]
}"#;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerParseDiagnostic {
    pub raw_sha256: String,
    pub json_path: String,
    pub expected: String,
    pub actual: String,
    pub message: String,
    pub line: usize,
    pub column: usize,
}

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
    /// OMO-style planning context. These explain the evidence and reversible
    /// defaults behind the sealed task graph without becoming worker TODOs.
    #[serde(default)]
    pub assumptions: Vec<String>,
    #[serde(default)]
    pub findings: Vec<String>,
    #[serde(default)]
    pub decisions: Vec<String>,
    #[serde(default)]
    pub open_questions: Vec<String>,
    #[serde(default)]
    pub must_have: Vec<String>,
    #[serde(default)]
    pub must_not_have: Vec<String>,
    #[serde(default)]
    pub topology_lock: Vec<String>,
    /// OMO-style plan-level execution controls. These are intentionally
    /// declarative: runtime records their evidence, while workers execute
    /// only the approved task contracts.
    #[serde(default)]
    pub preflight: Vec<String>,
    #[serde(default)]
    pub rollback: Vec<String>,
    #[serde(default)]
    pub final_verification: Vec<String>,
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
    /// OMO-style task rationale: why this work order is needed now.
    #[serde(default)]
    pub rationale: String,
    /// OMO-style bounded approach: how the worker should reach the deliverable.
    #[serde(default)]
    pub approach: Vec<String>,
    /// OMO-compatible incremental context for this work order.
    #[serde(default)]
    pub already_in_working_tree: Vec<String>,
    #[serde(default)]
    pub still_needed: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub parallel_wave: usize,
    pub scope: PlanTaskScope,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    pub preferred_phase_profile: PhaseProfile,
    /// OMO's explicit task intake: what the executor must read before editing.
    #[serde(default)]
    pub inputs: Vec<String>,
    /// Conditions that must be true before this work order can start.
    #[serde(default)]
    pub preconditions: Vec<String>,
    pub must_do: Vec<String>,
    /// Ordered OMO-style execution instructions. Workers must complete these
    /// in order and report the expected observation for each step.
    #[serde(default)]
    pub execution_steps: Vec<PlanExecutionStep>,
    /// New planner contracts require explicit worker step receipts. Legacy
    /// deterministic plans keep stage-derived compatibility until regenerated.
    #[serde(default)]
    pub execution_steps_evidence_required: bool,
    pub must_not_do: Vec<String>,
    #[serde(default)]
    pub references: Vec<PlanReference>,
    pub test: PlanTestContract,
    pub qa: PlanQaContract,
    pub artifacts: Vec<PlanArtifactContract>,
    /// Evidence obligations are separate from deliverables so a task cannot
    /// claim completion merely because a file was changed.
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub rollback: Vec<String>,
    #[serde(default)]
    pub budget: PlanTaskBudget,
    pub commit_boundary: CommitBoundary,
    /// Optional OMO-style commit intent. Gear never commits automatically;
    /// this is an auditable instruction for the delegated worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_message: Option<String>,
    pub completion_predicates: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanTaskBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_commands: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_duration_seconds: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanExecutionStep {
    pub step_id: String,
    pub action: String,
    pub expected_observation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_path: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskSizeTier {
    Small,
    Medium,
    Large,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRiskTier {
    Normal,
    Elevated,
    High,
}

impl PlanTaskContract {
    /// Derive a stable worker size from the sealed task contract, not from a
    /// model's subjective difficulty label.
    pub fn size_tier(&self) -> TaskSizeTier {
        let file_count = self
            .scope
            .allowed_files
            .len()
            .max(self.scope.write_scope.len());
        let dependency_count = self.dependencies.len();
        if file_count <= 1 && dependency_count <= 1 && self.scope.max_files_changed <= 1 {
            TaskSizeTier::Small
        } else if file_count <= 4 && dependency_count <= 3 && self.scope.max_files_changed <= 4 {
            TaskSizeTier::Medium
        } else {
            TaskSizeTier::Large
        }
    }

    /// Risk is independent from size: a one-file concurrency or security
    /// change must still receive a high-rigor route.
    pub fn risk_tier(&self) -> TaskRiskTier {
        let text = format!(
            "{} {} {} {}",
            self.title,
            self.goal,
            self.deliverable,
            self.required_capabilities.join(" ")
        )
        .to_ascii_lowercase();
        if [
            "concurr",
            "security",
            "migration",
            "irreversible",
            "protocol",
        ]
        .iter()
        .any(|keyword| text.contains(keyword))
        {
            TaskRiskTier::High
        } else if self.dependencies.len() > 2 || self.scope.write_scope.len() > 2 {
            TaskRiskTier::Elevated
        } else {
            TaskRiskTier::Normal
        }
    }

    /// Map deterministic task facts to the phase hint used by the worker
    /// router. Explicit review/repair hints remain caller-owned; this method
    /// only supplies the default executor profile for a fresh node.
    pub fn recommended_route_hint(&self) -> Option<&'static str> {
        if self.risk_tier() == TaskRiskTier::High || self.size_tier() == TaskSizeTier::Large {
            Some("deep")
        } else if self.size_tier() == TaskSizeTier::Small
            && self.risk_tier() == TaskRiskTier::Normal
        {
            Some("quick")
        } else {
            None
        }
    }
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
    #[serde(default)]
    pub adversarial_path: Vec<QaScenario>,
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
        for (field, values) in [
            ("must_have", &self.draft.must_have),
            ("must_not_have", &self.draft.must_not_have),
            ("topology_lock", &self.draft.topology_lock),
            ("preflight", &self.draft.preflight),
            ("rollback", &self.draft.rollback),
            ("final_verification", &self.draft.final_verification),
            ("final_acceptance", &self.draft.final_acceptance),
        ] {
            if values.is_empty() || values.iter().any(|value| value.trim().is_empty()) {
                bail!("PlanGraph {field} must contain non-empty decision criteria");
            }
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
            // A session-bound planner receipt identifies the new OMO-style
            // planner protocol. Sessionless coordinator briefs are legacy
            // persisted plans and remain readable during migration.
            // WHY/HOW context is part of every planner contract, including
            // sessionless validation and persisted legacy migration drafts.
            if self.source == PlanSource::PlannerModel
                && self
                    .planner
                    .as_ref()
                    .and_then(|planner| planner.session_id.as_ref())
                    .is_some()
                && !task.execution_steps_evidence_required
            {
                bail!(
                    "planner-model task `{}` must require ordered step evidence",
                    task.task_id
                );
            }
            if self.source == PlanSource::PlannerModel
                && (task.rationale.trim().is_empty()
                    || task.approach.is_empty()
                    || task.approach.iter().any(|item| item.trim().is_empty()))
            {
                bail!(
                    "planner-model task `{}` must define a rationale and bounded approach",
                    task.task_id
                );
            }
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
        Ok(self
            .runnable_tasks(completed, &HashSet::new())?
            .into_iter()
            .next())
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
            .filter(|task| !completed.contains(&task.task_id) && !active.contains(&task.task_id))
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
        contract.already_in_working_tree = self
            .draft
            .tasks
            .iter()
            .flat_map(|task| task.already_in_working_tree.iter().cloned())
            .collect();
        contract.still_needed = self
            .draft
            .tasks
            .iter()
            .flat_map(|task| task.still_needed.iter().cloned())
            .collect();
        contract.dependencies.clear();
        contract.parallel_wave = 0;
        contract.must_do = self
            .draft
            .tasks
            .iter()
            .flat_map(|task| task.must_do.iter().cloned())
            .collect();
        contract.execution_steps = self
            .draft
            .tasks
            .iter()
            .flat_map(|task| {
                task.execution_steps
                    .iter()
                    .cloned()
                    .map(|step| PlanExecutionStep {
                        step_id: format!("{}::{}", task.task_id, step.step_id),
                        ..step
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        if contract.execution_steps.is_empty() {
            contract.execution_steps = execution_steps_from_must_do(&contract.must_do);
        }
        contract.execution_steps_evidence_required = self
            .draft
            .tasks
            .iter()
            .any(|task| task.execution_steps_evidence_required);
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
        for (field, values) in [
            ("already_in_working_tree", &self.already_in_working_tree),
            ("still_needed", &self.still_needed),
        ] {
            if values.iter().any(|value| value.trim().is_empty()) {
                bail!("PlanGraph task `{}` has a blank {field} item", self.task_id);
            }
        }
        for (field, values) in [
            ("inputs", &self.inputs),
            ("preconditions", &self.preconditions),
            ("evidence", &self.evidence),
            ("rollback", &self.rollback),
        ] {
            if values.iter().any(|value| value.trim().is_empty()) {
                bail!("PlanGraph task `{}` has a blank {field} item", self.task_id);
            }
        }
        if self.execution_steps_evidence_required {
            for (field, values) in [
                ("inputs", &self.inputs),
                ("preconditions", &self.preconditions),
                ("evidence", &self.evidence),
                ("rollback", &self.rollback),
            ] {
                if values.is_empty() {
                    bail!(
                        "strict PlanGraph task `{}` must define non-empty {field}",
                        self.task_id
                    );
                }
            }
        }
        if self.budget.max_attempts == Some(0)
            || self.budget.max_commands == Some(0)
            || self.budget.max_duration_seconds == Some(0)
        {
            bail!(
                "PlanGraph task `{}` has a zero execution budget",
                self.task_id
            );
        }
        if !self.execution_steps.is_empty() {
            let mut seen = HashSet::new();
            for step in &self.execution_steps {
                if step.step_id.trim().is_empty()
                    || step.action.trim().is_empty()
                    || step.expected_observation.trim().is_empty()
                    || !seen.insert(step.step_id.as_str())
                {
                    bail!(
                        "PlanGraph task `{}` has invalid or duplicate execution step",
                        self.task_id
                    );
                }
            }
        }
        if self
            .commit_message
            .as_deref()
            .is_some_and(|message| message.trim().is_empty())
        {
            bail!(
                "PlanGraph task `{}` has an empty commit message",
                self.task_id
            );
        }
        if self.qa.happy_path.is_empty() || self.qa.failure_path.is_empty() {
            bail!(
                "PlanGraph task `{}` must define happy and failure QA",
                self.task_id
            );
        }
        for scenario in self
            .qa
            .happy_path
            .iter()
            .chain(self.qa.failure_path.iter())
            .chain(self.qa.adversarial_path.iter())
        {
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
            "Approved goal: {}\n\nWHY: {}\n\nApproved deliverable: {}\n\nHOW:\n{}\n\nCurrent turn instruction: {}\n\nExecute the numbered STEP constraints in order. Do not skip, reorder, or replace a step; report a typed plan gap when a step or its expected observation cannot be completed. Do not redesign the plan.",
            self.goal,
            self.rationale,
            self.deliverable,
            self.approach.join("\n"),
            execution_request
        )
    }

    pub fn worker_constraints(&self) -> Vec<String> {
        self.inputs
            .iter()
            .map(|input| format!("INPUT: read `{input}` before editing"))
            .chain(
                self.preconditions
                    .iter()
                    .map(|condition| format!("PRECONDITION: verify {condition}")),
            )
            .chain(std::iter::once(format!("WHY: {}", self.rationale)))
            .chain(self.approach.iter().map(|step| format!("HOW: {step}")))
            .chain(
                self.execution_steps_or_legacy()
                    .into_iter()
                    .enumerate()
                    .map(|(index, step)| {
                        format!(
                            "STEP {:02} [{}]: {} -> expect {}{}",
                            index + 1,
                            step.step_id,
                            step.action,
                            step.expected_observation,
                            step.evidence_path
                                .as_deref()
                                .map(|path| format!("; evidence `{path}`"))
                                .unwrap_or_default()
                        )
                    }),
            )
            .chain(
                self.must_do
                    .iter()
                    .map(|requirement| format!("MUST: {requirement}")),
            )
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
            .chain(
                self.evidence
                    .iter()
                    .map(|requirement| format!("EVIDENCE: {requirement}")),
            )
            .chain(
                self.rollback
                    .iter()
                    .map(|instruction| format!("ROLLBACK: {instruction}")),
            )
            .chain(
                self.budget
                    .max_attempts
                    .map(|value| format!("BUDGET: max_attempts={value}")),
            )
            .chain(
                self.budget
                    .max_commands
                    .map(|value| format!("BUDGET: max_commands={value}")),
            )
            .chain(
                self.budget
                    .max_duration_seconds
                    .map(|value| format!("BUDGET: max_duration_seconds={value}")),
            )
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
            self.execution_steps_evidence_required
                .then(|| "completed_steps".to_string()),
        )
        .chain(
            self.execution_steps_evidence_required
                .then(|| "step_evidence".to_string()),
        )
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
            "Execute the approved steps in order; stop and report a plan gap when the next step cannot be completed or its expected observation is absent.".to_string(),
            "The approved scope, dependency, or acceptance contract is incomplete.".to_string(),
            "Execution requires a forbidden path or unapproved irreversible action.".to_string(),
            "RED fails for an environment or syntax reason instead of the planned missing behavior."
                .to_string(),
            "The same root cause fails twice without new evidence.".to_string(),
        ]
    }

    pub fn execution_steps_or_legacy(&self) -> Vec<PlanExecutionStep> {
        if self.execution_steps.is_empty() {
            execution_steps_from_must_do(&self.must_do)
        } else {
            self.execution_steps.clone()
        }
    }
}

fn execution_steps_from_must_do(must_do: &[String]) -> Vec<PlanExecutionStep> {
    must_do
        .iter()
        .enumerate()
        .map(|(index, action)| PlanExecutionStep {
            step_id: format!("step-{:03}", index + 1),
            action: action.clone(),
            expected_observation: "the step's stated change or check is complete".to_string(),
            evidence_path: None,
        })
        .collect()
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
                    evidence_path: ".gear/artifacts/verification.md".to_string(),
                })
                .collect(),
            no_test_reason: None,
        }
    };
    PlanGraphDraft {
        objective: objective.to_string(),
        assumptions: vec![
            "Prefer a reversible local implementation when the request leaves details open."
                .to_string(),
        ],
        findings: Vec::new(),
        decisions: vec![
            "Keep the first implementation inside the declared repository scope.".to_string(),
        ],
        open_questions: Vec::new(),
        must_have: vec!["Satisfy the original objective with inspectable evidence.".to_string()],
        must_not_have: vec!["Do not expand scope beyond the original objective.".to_string()],
        topology_lock: vec![
            "Preserve the existing repository architecture unless required.".to_string(),
        ],
        preflight: vec![
            "Record the repository baseline and verify the requested scope before editing."
                .to_string(),
        ],
        rollback: vec![
            "If final verification fails, preserve evidence and revert only this plan's changes."
                .to_string(),
        ],
        final_verification: vec![
            "Run the final verification wave and persist its receipt before completion."
                .to_string(),
        ],
        tasks: vec![PlanTaskContract {
            task_id: "task_003".to_string(),
            title: "Execute the bounded implementation contract".to_string(),
            goal: objective.to_string(),
            deliverable: "A minimal verified implementation of the requested change.".to_string(),
            rationale: "The requested change is not implemented in the current repository baseline.".to_string(),
            approach: vec![
                "Inspect the existing seam, implement only the requested behavior, then verify the result.".to_string(),
            ],
            already_in_working_tree: vec![
                "The repository baseline and discovery evidence are already recorded.".to_string(),
            ],
            still_needed: vec![
                "Implement the requested behavior and persist verification evidence.".to_string(),
            ],
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
            inputs: vec![
                "Read the repository baseline and the referenced implementation seam before editing."
                    .to_string(),
            ],
            preconditions: vec![
                "The declared scope and verification commands are available.".to_string(),
            ],
            must_do: vec![
                "Inspect relevant repository code before editing.".to_string(),
                "Make the smallest change that satisfies the objective.".to_string(),
                "Record verification and known failures.".to_string(),
            ],
            execution_steps: vec![
                PlanExecutionStep {
                    step_id: "step-001".to_string(),
                    action: "Inspect relevant repository code before editing.".to_string(),
                    expected_observation: "The implementation seam and baseline are recorded."
                        .to_string(),
                    evidence_path: Some(".gear/artifacts/verification.md".to_string()),
                },
                PlanExecutionStep {
                    step_id: "step-002".to_string(),
                    action: "Make the smallest change that satisfies the objective.".to_string(),
                    expected_observation: "The requested behavior is implemented within scope."
                        .to_string(),
                    evidence_path: None,
                },
                PlanExecutionStep {
                    step_id: "step-003".to_string(),
                    action: "Record verification and known failures.".to_string(),
                    expected_observation: "Verification evidence and remaining failures are explicit."
                        .to_string(),
                    evidence_path: Some(".gear/artifacts/verification.md".to_string()),
                },
            ],
            execution_steps_evidence_required: false,
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
                    evidence_path: ".gear/artifacts/verification.md".to_string(),
                }],
                failure_path: vec![QaScenario {
                    name: "verification failure".to_string(),
                    steps: vec!["Capture the failing command and root-cause evidence.".to_string()],
                    expected_result: "The task remains incomplete with an explicit repair request."
                        .to_string(),
                    evidence_path: ".gear/artifacts/verification.md".to_string(),
                }],
                adversarial_path: vec![QaScenario {
                    name: "adversarial-not-applicable".to_string(),
                    steps: vec!["Check the OMO trigger map for this narrow fallback task.".to_string()],
                    expected_result: "No additional adversarial trigger applies; the reason is recorded."
                        .to_string(),
                    evidence_path: ".gear/artifacts/verification.md".to_string(),
                }],
            },
            evidence: vec![
                "Record changed paths, commands, exit status, and known failures.".to_string(),
            ],
            rollback: vec![
                "Preserve evidence and revert only this task's bounded changes if verification fails."
                    .to_string(),
            ],
            budget: PlanTaskBudget {
                max_attempts: Some(2),
                max_commands: Some(3),
                max_duration_seconds: None,
            },
            artifacts: vec![PlanArtifactContract {
                path: ".gear/artifacts/final-report.md".to_string(),
                description: "Final implementation and verification report.".to_string(),
                required: true,
            }],
            commit_boundary: CommitBoundary::NoCommit,
            commit_message: None,
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
    parse_planner_draft_diagnostic(output).map_err(|diagnostic| {
        anyhow::anyhow!(
            "planner did not return a valid PlanGraphDraft JSON object: {}",
            serde_json::to_string(&diagnostic).unwrap_or_else(|_| diagnostic.message.clone())
        )
    })
}

/// Accept a planner response that omitted only the top-level objective.
/// Models often preserve every task contract but drop this redundant field;
/// the runtime already owns the canonical objective, so restoring it here
/// keeps strict nested-schema validation without flattening the task graph.
pub fn parse_planner_draft_with_objective(output: &str, objective: &str) -> Result<PlanGraphDraft> {
    match parse_planner_draft(output) {
        Ok(draft) => Ok(draft),
        Err(original) => {
            let trimmed = output.trim();
            let json = if let Some(rest) = trimmed.strip_prefix("```json") {
                rest.strip_suffix("```").unwrap_or(rest).trim()
            } else if let Some(rest) = trimmed.strip_prefix("```") {
                rest.strip_suffix("```").unwrap_or(rest).trim()
            } else {
                trimmed
            };
            let json = json.find('{').map(|index| &json[index..]).unwrap_or(json);
            let mut value: Value =
                serde_json::from_str(json).with_context(|| original.to_string())?;
            let object = value
                .as_object_mut()
                .context("planner response is not a JSON object")?;
            if object.contains_key("objective") {
                return Err(original);
            }
            object.insert(
                "objective".to_string(),
                Value::String(objective.to_string()),
            );
            serde_json::from_value(value).with_context(|| original.to_string())
        }
    }
}

pub fn parse_planner_draft_diagnostic(
    output: &str,
) -> std::result::Result<PlanGraphDraft, PlannerParseDiagnostic> {
    let trimmed = output.trim();
    let json = if let Some(rest) = trimmed.strip_prefix("```json") {
        rest.strip_suffix("```").unwrap_or(rest).trim()
    } else if let Some(rest) = trimmed.strip_prefix("```") {
        rest.strip_suffix("```").unwrap_or(rest).trim()
    } else {
        trimmed
    };
    let json = json.find('{').map(|index| &json[index..]).unwrap_or(json);
    let mut hasher = Sha256::new();
    hasher.update(output.as_bytes());
    let raw_sha256 = format!("{:x}", hasher.finalize());
    let mut deserializer = serde_json::Deserializer::from_str(json);
    let mut track = serde_path_to_error::Track::new();
    let path = serde_path_to_error::Deserializer::new(&mut deserializer, &mut track);
    match PlanGraphDraft::deserialize(path) {
        Ok(draft) => Ok(draft),
        Err(error) => {
            let message = error.to_string();
            let (actual, expected) = message
                .split_once(", expected ")
                .map(|(actual, expected)| (actual.to_string(), expected.to_string()))
                .unwrap_or_else(|| (message.clone(), "valid PlanGraphDraft JSON".to_string()));
            Err(PlannerParseDiagnostic {
                raw_sha256,
                json_path: track.path().to_string(),
                expected,
                actual,
                message,
                line: error.line(),
                column: error.column(),
            })
        }
    }
}

pub fn validate_planner_draft(goal_id: &str, draft: &PlanGraphDraft) -> Result<()> {
    PlanGraph::seal(
        goal_id,
        1,
        PlanSource::PlannerModel,
        Some(PlannerReceipt {
            provider_id: "planner-validation".to_string(),
            model_id: "planner-validation".to_string(),
            // Draft validation must not invent a live planner session. The
            // session-bound ordered-evidence gate is enforced when the
            // runtime seals an actual planner submission.
            session_id: None,
        }),
        draft.clone(),
    )?;
    Ok(())
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

    #[test]
    fn planner_parser_restores_missing_objective_without_flattening_tasks() {
        let mut value: Value = serde_json::from_str(PLAN_GRAPH_SCHEMA_EXEMPLAR).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("objective");
        let raw = serde_json::to_string(&value).unwrap();
        let draft = parse_planner_draft_with_objective(&raw, "canonical objective").unwrap();
        assert_eq!(draft.objective, "canonical objective");
        assert_eq!(draft.tasks.len(), 1);
        assert_eq!(draft.tasks[0].task_id, "task_a");
        assert_eq!(draft.tasks[0].commit_message, None);
    }

    fn valid_draft() -> PlanGraphDraft {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let mut draft =
            deterministic_fallback_draft("Implement feature", &scope, &["cargo test".to_string()]);
        let task = &mut draft.tasks[0];
        task.execution_steps_evidence_required = true;
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

    #[test]
    fn planner_protocol_contract_exemplar_is_typed() -> Result<()> {
        let draft = parse_planner_draft(PLAN_GRAPH_SCHEMA_EXEMPLAR)?;
        assert_eq!(draft.tasks.len(), 1);
        assert_eq!(
            draft.tasks[0].preferred_phase_profile,
            PhaseProfile::ExecutorQuick
        );
        assert_eq!(draft.tasks[0].test.strategy, TestStrategy::TestsAfter);
        assert_eq!(draft.tasks[0].inputs.len(), 1);
        assert_eq!(draft.tasks[0].preconditions.len(), 1);
        assert_eq!(draft.tasks[0].evidence.len(), 1);
        assert_eq!(draft.tasks[0].rollback.len(), 1);
        assert_eq!(draft.tasks[0].budget.max_attempts, Some(2));
        Ok(())
    }

    #[test]
    fn commit_message_is_optional_but_cannot_be_blank() -> Result<()> {
        let mut draft = valid_draft();
        draft.tasks[0].commit_message = Some("feat: implement bounded behavior".to_string());
        PlanGraph::seal(
            "goal-1",
            1,
            PlanSource::DeterministicFallback,
            None,
            draft.clone(),
        )?;
        draft.tasks[0].commit_message = Some("  ".to_string());
        let error = PlanGraph::seal("goal-1", 1, PlanSource::DeterministicFallback, None, draft)
            .expect_err("blank commit intent must be rejected");
        assert!(error.to_string().contains("commit message"));
        Ok(())
    }

    #[test]
    fn planner_schema_diagnostic_reports_path_types_and_raw_hash() {
        let malformed = r#"{"objective":"x","topology_lock":"must be an array","tasks":[]}"#;
        let error = parse_planner_draft_diagnostic(malformed).expect_err("schema drift must fail");
        assert_eq!(error.json_path, "topology_lock");
        assert!(error.expected.contains("sequence"));
        assert!(error.actual.contains("string"));
        assert_eq!(error.raw_sha256.len(), 64);
        assert!(error.line >= 1 && error.column >= 1);
    }

    fn planner_receipt() -> Option<PlannerReceipt> {
        Some(PlannerReceipt {
            provider_id: "test-provider".to_string(),
            model_id: "test-model".to_string(),
            session_id: Some("planner-session".to_string()),
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
    fn execution_steps_are_ordered_worker_constraints_and_invalid_steps_reject() -> Result<()> {
        let draft = valid_draft();
        let task = &draft.tasks[0];
        let constraints = task.worker_constraints();
        assert!(constraints.iter().any(|line| line.starts_with("STEP 01")));
        assert!(constraints.iter().any(|line| line.starts_with("STEP 02")));
        assert!(constraints.iter().any(|line| line.starts_with("WHY: ")));
        assert!(constraints.iter().any(|line| line.starts_with("HOW: ")));
        assert!(task.worker_goal("continue").contains("WHY: "));

        let mut invalid = draft;
        invalid.tasks[0].execution_steps[1].step_id =
            invalid.tasks[0].execution_steps[0].step_id.clone();
        assert!(
            PlanGraph::seal(
                "goal-steps",
                1,
                PlanSource::PlannerModel,
                planner_receipt(),
                invalid,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn strict_step_evidence_is_an_explicit_new_plan_contract() {
        let mut draft = valid_draft();
        draft.tasks[0].execution_steps_evidence_required = true;
        let outputs = draft.tasks[0].worker_required_outputs();
        assert!(outputs.iter().any(|output| output == "completed_steps"));
        assert!(outputs.iter().any(|output| output == "step_evidence"));
    }

    #[test]
    fn strict_planner_contract_rejects_missing_omo_task_context() {
        let mut draft = valid_draft();
        draft.tasks[0].execution_steps_evidence_required = true;
        draft.tasks[0].rollback.clear();
        let error = PlanGraph::seal(
            "goal-strict-context",
            1,
            PlanSource::PlannerModel,
            planner_receipt(),
            draft,
        )
        .expect_err("strict planner tasks must carry rollback context");
        assert!(error.to_string().contains("rollback"));
    }

    #[test]
    fn strict_planner_contract_requires_why_and_how() {
        let mut draft = valid_draft();
        draft.tasks[0].rationale.clear();
        let error = validate_planner_draft("goal-strict-why-how", &draft)
            .expect_err("strict planner tasks must carry OMO WHY/HOW context");
        assert!(error.to_string().contains("rationale"));
    }

    #[test]
    fn draft_validation_does_not_invent_a_live_session_evidence_gate() -> Result<()> {
        let mut draft = valid_draft();
        draft.tasks[0].execution_steps_evidence_required = false;
        validate_planner_draft("goal-sessionless-draft", &draft)?;

        let error = PlanGraph::seal(
            "goal-session-bound",
            1,
            PlanSource::PlannerModel,
            planner_receipt(),
            draft,
        )
        .expect_err("a live planner receipt must require ordered step evidence");
        assert!(error.to_string().contains("ordered step evidence"));
        Ok(())
    }

    #[test]
    fn planner_model_rejects_legacy_non_strict_work_order() {
        let mut draft = valid_draft();
        draft.tasks[0].execution_steps_evidence_required = false;
        let error = PlanGraph::seal(
            "goal-strict-steps",
            1,
            PlanSource::PlannerModel,
            planner_receipt(),
            draft,
        )
        .expect_err("planner-model tasks must opt into ordered step evidence");
        assert!(error.to_string().contains("ordered step evidence"));
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
    fn plan_graph_requires_goal_level_decision_criteria() -> Result<()> {
        let mut draft = valid_draft();
        draft.final_acceptance.clear();
        let error = PlanGraph::seal(
            "goal-1",
            1,
            PlanSource::PlannerModel,
            planner_receipt(),
            draft,
        )
        .expect_err("a plan without final acceptance must be rejected");
        assert!(error.to_string().contains("final_acceptance"));
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
        let graph = PlanGraph::seal("goal", 1, PlanSource::DeterministicFallback, None, draft)?;

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
        let serial_wave = graph.runnable_wave(&HashSet::new(), &HashSet::new(), 1)?;
        assert_eq!(serial_wave.len(), 1);
        assert_eq!(serial_wave[0].task_id, "node_a");
        let active = HashSet::from([String::from("node_a")]);
        let serial_wave = graph.runnable_wave(&HashSet::new(), &active, 1)?;
        assert_eq!(serial_wave.len(), 1);
        assert_eq!(serial_wave[0].task_id, "node_b");
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
        let graph = PlanGraph::seal("goal", 1, PlanSource::DeterministicFallback, None, draft)?;

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
        let mut ledger = crate::state::PlanNodeRunLedger::from_plan("goal", "epoch", &graph)?;
        assert!(
            ledger
                .mark("task_003", crate::state::PlanNodeRunStatus::Completed)
                .is_err()
        );

        let node = ledger.node_mut("task_003")?;
        node.status = crate::state::PlanNodeRunStatus::Completed;
        node.attempt = 1;
        node.green_evidence_paths.push("green.md".to_string());
        node.review_evidence_path = Some("review.md".to_string());
        for step in &mut node.execution_steps {
            step.status = crate::state::PlanStepRunStatus::Completed;
        }
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
    fn plan_node_steps_persist_running_blocked_and_resume_states() -> Result<()> {
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let graph = PlanGraph::seal(
            "goal-steps",
            1,
            PlanSource::DeterministicFallback,
            None,
            deterministic_fallback_draft("graph", &scope, &[]),
        )?;
        let mut ledger = crate::state::PlanNodeRunLedger::from_plan("goal-steps", "epoch", &graph)?;
        let node = ledger.node_mut("task_003")?;
        assert!(
            node.execution_steps
                .iter()
                .all(|step| { step.status == crate::state::PlanStepRunStatus::Pending })
        );
        node.status = crate::state::PlanNodeRunStatus::Running;
        node.sync_step_lifecycle(None);
        assert_eq!(
            node.execution_steps[0].status,
            crate::state::PlanStepRunStatus::Running
        );
        let completed = vec![node.execution_steps[0].step_id.clone()];
        let evidence = HashMap::from([(completed[0].clone(), ".gear/steps/001.md".to_string())]);
        assert_eq!(
            node.apply_worker_step_evidence(&completed, &evidence)?
                .len(),
            2
        );
        assert_eq!(
            node.execution_steps[0].evidence_path.as_deref(),
            Some(".gear/steps/001.md")
        );
        node.status = crate::state::PlanNodeRunStatus::Failed;
        node.sync_step_lifecycle(Some("worker stopped"));
        assert_eq!(
            node.execution_steps[1].status,
            crate::state::PlanStepRunStatus::Blocked
        );
        ledger.requeue_failed_for_resume();
        assert_eq!(
            ledger.nodes[0].execution_steps[0].status,
            crate::state::PlanStepRunStatus::Completed
        );
        assert!(
            ledger.nodes[0].execution_steps[1..]
                .iter()
                .all(|step| { step.status == crate::state::PlanStepRunStatus::Pending })
        );
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
        let receipt =
            crate::state::FinalVerificationWaveReceipt::seal("goal", "epoch", &graph, dimensions)?;
        receipt.validate(&graph)?;
        let mut tampered = receipt.clone();
        tampered.plan_hash = "f".repeat(64);
        assert!(tampered.validate(&graph).is_err());
        Ok(())
    }

    #[test]
    fn task_size_and_risk_tiers_are_deterministic_and_independent() -> Result<()> {
        let scope = Scope::new(vec!["src/main.rs".to_string()], vec![".git".to_string()], 1);
        let mut draft = deterministic_fallback_draft("small change", &scope, &[]);
        let task = &draft.tasks[0];
        assert_eq!(task.size_tier(), TaskSizeTier::Small);
        assert_eq!(task.risk_tier(), TaskRiskTier::Normal);

        draft.tasks[0].required_capabilities = vec!["concurrency".to_string()];
        assert_eq!(draft.tasks[0].size_tier(), TaskSizeTier::Small);
        assert_eq!(draft.tasks[0].risk_tier(), TaskRiskTier::High);

        draft.tasks[0].scope.allowed_files = (0..5).map(|i| format!("src/{i}.rs")).collect();
        draft.tasks[0].scope.write_scope = draft.tasks[0].scope.allowed_files.clone();
        draft.tasks[0].scope.max_files_changed = 5;
        assert_eq!(draft.tasks[0].size_tier(), TaskSizeTier::Large);
        Ok(())
    }

    #[test]
    fn task_tiers_map_to_default_executor_hints_without_overriding_review() -> Result<()> {
        let scope = Scope::new(vec!["src/main.rs".to_string()], Vec::new(), 1);
        let mut draft = deterministic_fallback_draft("small task", &scope, &[]);
        let small = &draft.tasks[0];
        assert_eq!(small.recommended_route_hint(), Some("quick"));

        draft.tasks[0].title = "large migration".to_string();
        draft.tasks[0].scope.allowed_files = vec![
            "src/a.rs".to_string(),
            "src/b.rs".to_string(),
            "src/c.rs".to_string(),
            "src/d.rs".to_string(),
            "src/e.rs".to_string(),
        ];
        let allowed_files = draft.tasks[0].scope.allowed_files.clone();
        draft.tasks[0].scope.write_scope = allowed_files;
        draft.tasks[0].scope.max_files_changed = 5;
        assert_eq!(draft.tasks[0].recommended_route_hint(), Some("deep"));

        draft.tasks[0].scope.allowed_files = vec!["src/main.rs".to_string()];
        draft.tasks[0].scope.write_scope = vec!["src/main.rs".to_string()];
        draft.tasks[0].scope.max_files_changed = 1;
        draft.tasks[0].required_capabilities = vec!["concurrency".to_string()];
        assert_eq!(draft.tasks[0].recommended_route_hint(), Some("deep"));
        Ok(())
    }
}
