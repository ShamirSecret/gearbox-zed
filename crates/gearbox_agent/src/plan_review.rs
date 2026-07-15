use crate::plan_graph::{PlanGraph, PlanSource, TestStrategy, parse_planner_draft_with_objective};
use anyhow::{Context as _, Result, bail};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashSet,
    path::{Component, Path},
};

pub const PLAN_REVIEW_SCHEMA_VERSION: u32 = 1;
pub const PLAN_VERIFIER_LIMITATION: &str = "Reference verification checks repository-relative path existence and containment only; it does not read file contents or verify symbols.";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanApprovalStatus {
    Reviewing,
    Revising,
    Approved,
    Rejected,
    Limited,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanApprovalState {
    pub schema_version: u32,
    pub goal_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub status: PlanApprovalStatus,
    pub planner_receipt_hash: String,
    pub verifier_report_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub critic_receipt_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary_critic_receipt_hash: Option<String>,
    pub revisions_used: usize,
    pub updated_at: String,
}

impl PlanApprovalState {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PLAN_REVIEW_SCHEMA_VERSION {
            bail!(
                "unsupported plan approval state schema version {}",
                self.schema_version
            );
        }
        require_non_empty("goal_id", &self.goal_id)?;
        require_non_empty("plan_id", &self.plan_id)?;
        require_non_empty("updated_at", &self.updated_at)?;
        validate_sha256("plan approval plan hash", &self.plan_hash)?;
        validate_sha256(
            "plan approval planner receipt hash",
            &self.planner_receipt_hash,
        )?;
        validate_sha256(
            "plan approval verifier report hash",
            &self.verifier_report_hash,
        )?;
        if let Some(critic_receipt_hash) = self.critic_receipt_hash.as_deref() {
            validate_sha256("plan approval critic receipt hash", critic_receipt_hash)?;
        }
        if let Some(secondary_critic_receipt_hash) = self.secondary_critic_receipt_hash.as_deref() {
            validate_sha256(
                "plan approval secondary critic receipt hash",
                secondary_critic_receipt_hash,
            )?;
        }
        if matches!(
            self.status,
            PlanApprovalStatus::Revising
                | PlanApprovalStatus::Approved
                | PlanApprovalStatus::Rejected
                | PlanApprovalStatus::Limited
        ) && self.critic_receipt_hash.is_none()
        {
            bail!("terminal or revising plan approval state requires a critic receipt hash");
        }
        Ok(())
    }

    pub fn validate_against(&self, plan: &PlanGraph) -> Result<()> {
        self.validate()?;
        validate_plan_binding(
            &self.goal_id,
            &self.plan_id,
            self.plan_revision,
            &self.plan_hash,
            plan,
        )?;
        if self.status != PlanApprovalStatus::Approved {
            bail!("canonical PlanGraph requires an approved plan approval state");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseExecutionBackend {
    LanguageModelRequest,
    NativeAgent,
    Acp,
    WorkerSession,
    DeterministicRules,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseExecutionIdentity {
    pub execution_id: String,
    pub phase_session_id: String,
    pub backend: PhaseExecutionBackend,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_session_id: Option<String>,
}

impl PhaseExecutionIdentity {
    pub fn validate(&self) -> Result<()> {
        require_non_empty("execution_id", &self.execution_id)?;
        require_non_empty("phase_session_id", &self.phase_session_id)?;
        validate_optional_non_empty("agent_id", self.agent_id.as_deref())?;
        validate_optional_non_empty("provider_id", self.provider_id.as_deref())?;
        validate_optional_non_empty("model_id", self.model_id.as_deref())?;
        validate_optional_non_empty("actual_session_id", self.actual_session_id.as_deref())?;

        match self.backend {
            PhaseExecutionBackend::LanguageModelRequest => {
                require_some_non_empty("provider_id", self.provider_id.as_deref())?;
                require_some_non_empty("model_id", self.model_id.as_deref())?;
            }
            PhaseExecutionBackend::NativeAgent
            | PhaseExecutionBackend::Acp
            | PhaseExecutionBackend::WorkerSession => {
                require_some_non_empty("agent_id", self.agent_id.as_deref())?;
                require_some_non_empty("provider_id", self.provider_id.as_deref())?;
                require_some_non_empty("model_id", self.model_id.as_deref())?;
                require_some_non_empty("actual_session_id", self.actual_session_id.as_deref())?;
            }
            PhaseExecutionBackend::DeterministicRules => {
                if self.provider_id.is_some()
                    || self.model_id.is_some()
                    || self.actual_session_id.is_some()
                {
                    bail!(
                        "deterministic-rules execution cannot claim provider, model, or actual session identity"
                    );
                }
            }
        }
        Ok(())
    }

    pub fn is_independent_from(&self, other: &Self) -> bool {
        self.execution_id != other.execution_id
            && self.phase_session_id != other.phase_session_id
            && match (
                self.actual_session_id.as_deref(),
                other.actual_session_id.as_deref(),
            ) {
                (Some(left), Some(right)) => left != right,
                _ => true,
            }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentFoldDecision {
    Ready,
    NeedsUser,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentRiskSeverity {
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentRisk {
    pub code: String,
    pub severity: IntentRiskSeverity,
    pub description: String,
    pub mitigation: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentFoldVerdict {
    pub schema_version: u32,
    pub goal_id: String,
    pub normalized_objective: String,
    #[serde(default)]
    pub assumptions: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub ambiguities: Vec<String>,
    #[serde(default)]
    pub required_questions: Vec<String>,
    #[serde(default)]
    pub risks: Vec<IntentRisk>,
    #[serde(default)]
    pub acceptance_signals: Vec<String>,
    pub decision: IntentFoldDecision,
    pub summary: String,
}

pub(crate) fn parse_json_object<T: DeserializeOwned>(raw_output: &str, context: &str) -> Result<T> {
    let trimmed = raw_output.trim();
    let mut last_error = None;
    for (index, character) in trimmed.char_indices() {
        if character != '{' {
            continue;
        }
        let candidate = &trimmed[index..];
        let mut deserializer = serde_json::Deserializer::from_str(candidate);
        match T::deserialize(&mut deserializer) {
            Ok(value) => return Ok(value),
            Err(error) => last_error = Some(error.to_string()),
        }
    }
    bail!(
        "{context}: {}",
        last_error.unwrap_or_else(|| "no JSON object found".to_string())
    )
}

impl IntentFoldVerdict {
    pub fn parse(raw_output: &str) -> Result<Self> {
        parse_json_object(
            raw_output,
            "intent fold did not return one strict IntentFoldVerdict JSON object",
        )
    }

    pub fn validate(&self, goal_id: &str) -> Result<()> {
        if self.schema_version != PLAN_REVIEW_SCHEMA_VERSION {
            bail!(
                "unsupported intent fold schema version {}",
                self.schema_version
            );
        }
        if self.goal_id != goal_id {
            bail!("intent fold verdict is bound to a different goal");
        }
        require_non_empty(
            "intent fold normalized objective",
            &self.normalized_objective,
        )?;
        require_non_empty("intent fold summary", &self.summary)?;
        for (label, values) in [
            ("assumption", &self.assumptions),
            ("constraint", &self.constraints),
            ("ambiguity", &self.ambiguities),
            ("required question", &self.required_questions),
            ("acceptance signal", &self.acceptance_signals),
        ] {
            validate_non_empty_unique_values(label, values)?;
        }
        for risk in &self.risks {
            require_non_empty("intent risk code", &risk.code)?;
            require_non_empty("intent risk description", &risk.description)?;
            require_non_empty("intent risk mitigation", &risk.mitigation)?;
        }
        match self.decision {
            IntentFoldDecision::Ready if !self.required_questions.is_empty() => {
                bail!("ready intent fold cannot contain required questions")
            }
            IntentFoldDecision::NeedsUser if self.required_questions.is_empty() => {
                bail!("needs-user intent fold requires at least one question")
            }
            IntentFoldDecision::Ready | IntentFoldDecision::NeedsUser => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentFoldReceipt {
    pub schema_version: u32,
    pub goal_id: String,
    pub verdict: IntentFoldVerdict,
    pub analyst: PhaseExecutionIdentity,
    pub raw_output_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<String>,
    pub created_at: String,
    pub receipt_hash: String,
}

impl IntentFoldReceipt {
    pub fn seal(
        verdict: IntentFoldVerdict,
        analyst: PhaseExecutionIdentity,
        raw_output: &str,
        artifact_path: Option<String>,
        created_at: String,
    ) -> Result<Self> {
        let mut receipt = Self {
            schema_version: PLAN_REVIEW_SCHEMA_VERSION,
            goal_id: verdict.goal_id.clone(),
            verdict,
            analyst,
            raw_output_sha256: sha256_bytes(raw_output.as_bytes()),
            artifact_path,
            created_at,
            receipt_hash: String::new(),
        };
        receipt.validate_payload()?;
        receipt.receipt_hash = receipt.expected_hash()?;
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        validate_sha256("intent fold receipt hash", &self.receipt_hash)?;
        if self.receipt_hash != self.expected_hash()? {
            bail!("intent fold receipt integrity hash mismatch");
        }
        Ok(())
    }

    fn validate_payload(&self) -> Result<()> {
        if self.schema_version != PLAN_REVIEW_SCHEMA_VERSION {
            bail!("unsupported intent fold receipt schema version");
        }
        require_non_empty("intent fold receipt goal id", &self.goal_id)?;
        self.verdict.validate(&self.goal_id)?;
        self.analyst.validate()?;
        validate_sha256("intent fold raw output hash", &self.raw_output_sha256)?;
        validate_optional_non_empty("intent fold artifact path", self.artifact_path.as_deref())?;
        require_non_empty("intent fold created_at", &self.created_at)?;
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        let mut payload = self.clone();
        payload.receipt_hash.clear();
        Ok(sha256_bytes(&serde_json::to_vec(&payload)?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerExecutionReceipt {
    pub schema_version: u32,
    pub receipt_id: String,
    pub goal_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub identity: PhaseExecutionIdentity,
    pub raw_output_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<String>,
    pub issued_at: String,
    pub receipt_hash: String,
}

impl PlannerExecutionReceipt {
    pub fn seal(
        plan: &PlanGraph,
        identity: PhaseExecutionIdentity,
        raw_output: &str,
        artifact_path: Option<String>,
        issued_at: impl Into<String>,
    ) -> Result<Self> {
        let mut receipt = Self {
            schema_version: PLAN_REVIEW_SCHEMA_VERSION,
            receipt_id: String::new(),
            goal_id: plan.goal_id.clone(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            plan_hash: plan.plan_hash.clone(),
            identity,
            raw_output_sha256: sha256_bytes(raw_output.as_bytes()),
            artifact_path,
            issued_at: issued_at.into(),
            receipt_hash: String::new(),
        };
        receipt.receipt_hash = receipt.expected_hash()?;
        receipt.receipt_id = format!("planner_receipt_{}", &receipt.receipt_hash[..16]);
        receipt.validate(plan, raw_output)?;
        Ok(receipt)
    }

    pub fn validate(&self, plan: &PlanGraph, raw_output: &str) -> Result<()> {
        if self.schema_version != PLAN_REVIEW_SCHEMA_VERSION {
            bail!(
                "unsupported planner receipt schema version {}",
                self.schema_version
            );
        }
        plan.validate()
            .context("planner receipt cannot bind an invalid PlanGraph")?;
        validate_plan_binding(
            &self.goal_id,
            &self.plan_id,
            self.plan_revision,
            &self.plan_hash,
            plan,
        )?;
        self.identity.validate()?;
        require_non_empty("issued_at", &self.issued_at)?;
        validate_optional_non_empty("artifact_path", self.artifact_path.as_deref())?;

        match plan.source {
            PlanSource::PlannerModel => {
                if self.identity.backend == PhaseExecutionBackend::DeterministicRules {
                    bail!("planner-model plan cannot claim deterministic planner execution");
                }
                let graph_planner = plan
                    .planner
                    .as_ref()
                    .context("planner-model plan is missing its graph planner receipt")?;
                if self.identity.provider_id.as_deref() != Some(graph_planner.provider_id.as_str())
                    || self.identity.model_id.as_deref() != Some(graph_planner.model_id.as_str())
                {
                    bail!("planner execution identity does not match PlanGraph planner model");
                }
                if let Some(session_id) = graph_planner.session_id.as_deref()
                    && self.identity.actual_session_id.as_deref() != Some(session_id)
                {
                    bail!("planner execution session does not match PlanGraph planner session");
                }
            }
            PlanSource::DeterministicFallback => {
                if self.identity.backend != PhaseExecutionBackend::DeterministicRules {
                    bail!("deterministic fallback requires deterministic planner identity");
                }
            }
        }

        let parsed = parse_planner_draft_with_objective(raw_output, &plan.draft.objective)
            .context("planner receipt raw output is not a PlanGraphDraft")?;
        if parsed != plan.draft {
            bail!("planner receipt raw output does not match the sealed PlanGraph draft");
        }
        let parsed_hash = hash_serializable(&parsed)?;
        if parsed_hash != plan.plan_hash {
            bail!("planner receipt raw output does not reproduce the PlanGraph hash");
        }
        if self.raw_output_sha256 != sha256_bytes(raw_output.as_bytes()) {
            bail!("planner receipt raw output hash mismatch");
        }
        validate_sha256("planner receipt hash", &self.receipt_hash)?;
        let expected_hash = self.expected_hash()?;
        if self.receipt_hash != expected_hash {
            bail!("planner receipt integrity hash mismatch");
        }
        if self.receipt_id != format!("planner_receipt_{}", &expected_hash[..16]) {
            bail!("planner receipt id does not match its integrity hash");
        }
        Ok(())
    }

    fn expected_hash(&self) -> Result<String> {
        #[derive(Serialize)]
        struct HashInput<'a> {
            schema_version: u32,
            goal_id: &'a str,
            plan_id: &'a str,
            plan_revision: usize,
            plan_hash: &'a str,
            identity: &'a PhaseExecutionIdentity,
            raw_output_sha256: &'a str,
            artifact_path: &'a Option<String>,
            issued_at: &'a str,
        }

        hash_serializable(&HashInput {
            schema_version: self.schema_version,
            goal_id: &self.goal_id,
            plan_id: &self.plan_id,
            plan_revision: self.plan_revision,
            plan_hash: &self.plan_hash,
            identity: &self.identity,
            raw_output_sha256: &self.raw_output_sha256,
            artifact_path: &self.artifact_path,
            issued_at: &self.issued_at,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanVerifierDimension {
    Structure,
    ReferencePaths,
    Scope,
    TestContract,
    QaContract,
    AcceptanceContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanVerifierCheck {
    pub dimension: PlanVerifierDimension,
    pub passed: bool,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanVerifierReport {
    pub schema_version: u32,
    pub goal_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub workspace_root: String,
    pub checks: Vec<PlanVerifierCheck>,
    pub limitations: Vec<String>,
    pub report_hash: String,
}

impl PlanVerifierReport {
    pub fn verify(plan: &PlanGraph, workspace_root: &Path) -> Result<Self> {
        let checks = vec![
            verifier_check(
                PlanVerifierDimension::Structure,
                "PlanGraph structural invariants are valid.",
                structure_findings(plan),
            ),
            verifier_check(
                PlanVerifierDimension::ReferencePaths,
                "Repository-relative reference paths exist and remain within the workspace.",
                reference_findings(plan, workspace_root),
            ),
            verifier_check(
                PlanVerifierDimension::Scope,
                "Task write scopes are repository-relative and respect allowed and forbidden paths.",
                scope_findings(plan),
            ),
            verifier_check(
                PlanVerifierDimension::TestContract,
                "Test commands, observations, and evidence paths are structurally complete.",
                test_findings(plan),
            ),
            verifier_check(
                PlanVerifierDimension::QaContract,
                "Happy-path and failure-path QA contracts contain concrete structured evidence targets.",
                qa_findings(plan),
            ),
            verifier_check(
                PlanVerifierDimension::AcceptanceContract,
                "Task artifacts, completion predicates, and final acceptance contracts are structurally decidable.",
                acceptance_findings(plan),
            ),
        ];
        let mut report = Self {
            schema_version: PLAN_REVIEW_SCHEMA_VERSION,
            goal_id: plan.goal_id.clone(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            plan_hash: plan.plan_hash.clone(),
            workspace_root: workspace_root.to_string_lossy().to_string(),
            checks,
            limitations: vec![PLAN_VERIFIER_LIMITATION.to_string()],
            report_hash: String::new(),
        };
        report.report_hash = report.expected_hash()?;
        report.validate(plan)?;
        Ok(report)
    }

    pub fn validate(&self, plan: &PlanGraph) -> Result<()> {
        if self.schema_version != PLAN_REVIEW_SCHEMA_VERSION {
            bail!(
                "unsupported plan verifier schema version {}",
                self.schema_version
            );
        }
        validate_plan_binding(
            &self.goal_id,
            &self.plan_id,
            self.plan_revision,
            &self.plan_hash,
            plan,
        )?;
        require_non_empty("workspace_root", &self.workspace_root)?;
        if !self
            .limitations
            .iter()
            .any(|limitation| limitation == PLAN_VERIFIER_LIMITATION)
        {
            bail!("plan verifier report must disclose its path-only reference limitation");
        }
        validate_verifier_checks(&self.checks)?;
        validate_sha256("plan verifier report hash", &self.report_hash)?;
        if self.report_hash != self.expected_hash()? {
            bail!("plan verifier report integrity hash mismatch");
        }
        Ok(())
    }

    pub fn passed(&self) -> bool {
        self.checks.iter().all(|check| check.passed)
    }

    pub fn check(&self, dimension: PlanVerifierDimension) -> Option<&PlanVerifierCheck> {
        self.checks
            .iter()
            .find(|check| check.dimension == dimension)
    }

    fn expected_hash(&self) -> Result<String> {
        #[derive(Serialize)]
        struct HashInput<'a> {
            schema_version: u32,
            goal_id: &'a str,
            plan_id: &'a str,
            plan_revision: usize,
            plan_hash: &'a str,
            workspace_root: &'a str,
            checks: &'a [PlanVerifierCheck],
            limitations: &'a [String],
        }

        hash_serializable(&HashInput {
            schema_version: self.schema_version,
            goal_id: &self.goal_id,
            plan_id: &self.plan_id,
            plan_revision: self.plan_revision,
            plan_hash: &self.plan_hash,
            workspace_root: &self.workspace_root,
            checks: &self.checks,
            limitations: &self.limitations,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanCriticDecision {
    Approve,
    Revise,
    Reject,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanCriticDimension {
    References,
    Executability,
    Contradictions,
    Scope,
    Tdd,
    Qa,
    Acceptance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanCriticCheckVerdict {
    Pass,
    Fail,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanCriticCheck {
    pub dimension: PlanCriticDimension,
    pub verdict: PlanCriticCheckVerdict,
    pub summary: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanCriticFindingSeverity {
    Blocking,
    Advisory,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanCriticFinding {
    pub dimension: PlanCriticDimension,
    pub severity: PlanCriticFindingSeverity,
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_change: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanCriticVerdict {
    pub schema_version: u32,
    pub reviewed_goal_id: String,
    pub reviewed_plan_id: String,
    pub reviewed_plan_revision: usize,
    pub reviewed_plan_hash: String,
    pub reviewed_planner_execution_id: String,
    pub decision: PlanCriticDecision,
    pub checks: Vec<PlanCriticCheck>,
    #[serde(default)]
    pub findings: Vec<PlanCriticFinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needs_user_reason: Option<String>,
    pub summary: String,
}

impl PlanCriticVerdict {
    pub fn parse(raw_output: &str) -> Result<Self> {
        parse_json_object(
            raw_output,
            "plan critic did not return one strict PlanCriticVerdict JSON object",
        )
    }

    pub fn validate(
        &self,
        plan: &PlanGraph,
        planner: &PlannerExecutionReceipt,
        verifier: &PlanVerifierReport,
    ) -> Result<()> {
        if self.schema_version != PLAN_REVIEW_SCHEMA_VERSION {
            bail!(
                "unsupported plan critic verdict schema version {}",
                self.schema_version
            );
        }
        if self.reviewed_goal_id != plan.goal_id
            || self.reviewed_plan_id != plan.plan_id
            || self.reviewed_plan_revision != plan.revision
        {
            bail!("plan critic verdict is bound to a different goal, plan, or revision");
        }
        if self.reviewed_plan_hash != plan.plan_hash {
            bail!("plan critic verdict is bound to a different plan hash");
        }
        if self.reviewed_planner_execution_id != planner.identity.execution_id {
            bail!("plan critic verdict is bound to a different planner execution");
        }
        require_non_empty("plan critic summary", &self.summary)?;
        validate_optional_non_empty(
            "revision_instructions",
            self.revision_instructions.as_deref(),
        )?;
        validate_optional_non_empty("needs_user_reason", self.needs_user_reason.as_deref())?;
        validate_critic_checks(&self.checks)?;
        validate_critic_findings(&self.findings, &self.checks)?;

        let failed_checks = self
            .checks
            .iter()
            .filter(|check| check.verdict == PlanCriticCheckVerdict::Fail)
            .count();
        let blocking_findings = self
            .findings
            .iter()
            .filter(|finding| finding.severity == PlanCriticFindingSeverity::Blocking)
            .count();
        if blocking_findings > 3 {
            bail!("plan critic verdict may report at most three blocking findings");
        }

        match self.decision {
            PlanCriticDecision::Approve => {
                if !verifier.passed() {
                    bail!(
                        "plan critic cannot approve a plan that failed deterministic verification"
                    );
                }
                if failed_checks != 0 || blocking_findings != 0 {
                    bail!("approved plan critic verdict cannot contain failed checks or blockers");
                }
                if self.revision_instructions.is_some() || self.needs_user_reason.is_some() {
                    bail!("approved plan critic verdict cannot request revision or user input");
                }
            }
            PlanCriticDecision::Revise => {
                if failed_checks == 0 || blocking_findings == 0 {
                    bail!("revise verdict requires a failed check and a blocking finding");
                }
                require_some_non_empty(
                    "revision_instructions",
                    self.revision_instructions.as_deref(),
                )?;
                if self.needs_user_reason.is_some() {
                    bail!("revise verdict cannot also require user input");
                }
            }
            PlanCriticDecision::Reject => {
                if failed_checks == 0 || blocking_findings == 0 {
                    bail!("reject verdict requires a failed check and a blocking finding");
                }
                require_some_non_empty("needs_user_reason", self.needs_user_reason.as_deref())?;
                if self.revision_instructions.is_some() {
                    bail!("reject verdict cannot also request automatic revision");
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanCriticReceipt {
    pub schema_version: u32,
    pub receipt_id: String,
    pub goal_id: String,
    pub plan_id: String,
    pub plan_revision: usize,
    pub plan_hash: String,
    pub planner_receipt_hash: String,
    pub verifier_report_hash: String,
    pub reviewer: PhaseExecutionIdentity,
    pub verdict: PlanCriticVerdict,
    pub raw_output_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<String>,
    pub issued_at: String,
    pub receipt_hash: String,
}

impl PlanCriticReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn seal(
        plan: &PlanGraph,
        planner: &PlannerExecutionReceipt,
        planner_raw_output: &str,
        verifier: &PlanVerifierReport,
        reviewer: PhaseExecutionIdentity,
        verdict: PlanCriticVerdict,
        raw_output: &str,
        artifact_path: Option<String>,
        issued_at: impl Into<String>,
    ) -> Result<Self> {
        let mut receipt = Self {
            schema_version: PLAN_REVIEW_SCHEMA_VERSION,
            receipt_id: String::new(),
            goal_id: plan.goal_id.clone(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            plan_hash: plan.plan_hash.clone(),
            planner_receipt_hash: planner.receipt_hash.clone(),
            verifier_report_hash: verifier.report_hash.clone(),
            reviewer,
            verdict,
            raw_output_sha256: sha256_bytes(raw_output.as_bytes()),
            artifact_path,
            issued_at: issued_at.into(),
            receipt_hash: String::new(),
        };
        receipt.receipt_hash = receipt.expected_hash()?;
        receipt.receipt_id = format!("plan_critic_receipt_{}", &receipt.receipt_hash[..16]);
        receipt.validate(plan, planner, planner_raw_output, verifier, raw_output)?;
        Ok(receipt)
    }

    pub fn validate(
        &self,
        plan: &PlanGraph,
        planner: &PlannerExecutionReceipt,
        planner_raw_output: &str,
        verifier: &PlanVerifierReport,
        raw_output: &str,
    ) -> Result<()> {
        if self.schema_version != PLAN_REVIEW_SCHEMA_VERSION {
            bail!(
                "unsupported plan critic receipt schema version {}",
                self.schema_version
            );
        }
        validate_plan_binding(
            &self.goal_id,
            &self.plan_id,
            self.plan_revision,
            &self.plan_hash,
            plan,
        )?;
        planner.validate(plan, planner_raw_output)?;
        verifier.validate(plan)?;
        if self.planner_receipt_hash != planner.receipt_hash {
            bail!("plan critic receipt is bound to a different planner receipt");
        }
        if self.verifier_report_hash != verifier.report_hash {
            bail!("plan critic receipt is bound to a different verifier report");
        }
        self.reviewer.validate()?;
        if !self.reviewer.is_independent_from(&planner.identity) {
            bail!("plan critic execution must be independent from planner execution");
        }
        self.verdict.validate(plan, planner, verifier)?;
        let parsed = PlanCriticVerdict::parse(raw_output)?;
        if parsed != self.verdict {
            bail!("plan critic receipt raw output does not match its typed verdict");
        }
        if self.raw_output_sha256 != sha256_bytes(raw_output.as_bytes()) {
            bail!("plan critic receipt raw output hash mismatch");
        }
        require_non_empty("issued_at", &self.issued_at)?;
        validate_optional_non_empty("artifact_path", self.artifact_path.as_deref())?;
        validate_sha256("plan critic receipt hash", &self.receipt_hash)?;
        let expected_hash = self.expected_hash()?;
        if self.receipt_hash != expected_hash {
            bail!("plan critic receipt integrity hash mismatch");
        }
        if self.receipt_id != format!("plan_critic_receipt_{}", &expected_hash[..16]) {
            bail!("plan critic receipt id does not match its integrity hash");
        }
        Ok(())
    }

    pub fn approved(&self) -> bool {
        self.verdict.decision == PlanCriticDecision::Approve
    }

    fn expected_hash(&self) -> Result<String> {
        #[derive(Serialize)]
        struct HashInput<'a> {
            schema_version: u32,
            goal_id: &'a str,
            plan_id: &'a str,
            plan_revision: usize,
            plan_hash: &'a str,
            planner_receipt_hash: &'a str,
            verifier_report_hash: &'a str,
            reviewer: &'a PhaseExecutionIdentity,
            verdict: &'a PlanCriticVerdict,
            raw_output_sha256: &'a str,
            artifact_path: &'a Option<String>,
            issued_at: &'a str,
        }

        hash_serializable(&HashInput {
            schema_version: self.schema_version,
            goal_id: &self.goal_id,
            plan_id: &self.plan_id,
            plan_revision: self.plan_revision,
            plan_hash: &self.plan_hash,
            planner_receipt_hash: &self.planner_receipt_hash,
            verifier_report_hash: &self.verifier_report_hash,
            reviewer: &self.reviewer,
            verdict: &self.verdict,
            raw_output_sha256: &self.raw_output_sha256,
            artifact_path: &self.artifact_path,
            issued_at: &self.issued_at,
        })
    }
}

fn verifier_check(
    dimension: PlanVerifierDimension,
    passed_summary: &str,
    findings: Vec<String>,
) -> PlanVerifierCheck {
    let passed = findings.is_empty();
    PlanVerifierCheck {
        dimension,
        passed,
        summary: if passed {
            passed_summary.to_string()
        } else {
            format!("{} deterministic issue(s) found.", findings.len())
        },
        findings,
    }
}

fn structure_findings(plan: &PlanGraph) -> Vec<String> {
    let mut findings = plan
        .validate()
        .err()
        .map(|error| vec![format!("PlanGraph validation failed: {error:#}")])
        .unwrap_or_default();
    findings.extend(
        plan.draft
            .open_questions
            .iter()
            .map(|question| format!("unresolved planning question blocks approval: {}", question)),
    );
    findings.extend(decomposition_findings(plan));
    findings
}

/// Keep the planner's work orders independently executable without imposing
/// an artificial exact file boundary. These are deliberately high ceilings:
/// normal multi-file tasks remain valid, while a single node that effectively
/// contains an entire feature is sent back for decomposition.
fn decomposition_findings(plan: &PlanGraph) -> Vec<String> {
    const MAX_FILES_PER_WORK_ORDER: usize = 8;
    const MAX_MUST_DO_ITEMS_PER_WORK_ORDER: usize = 12;
    let mut findings = Vec::new();
    for task in &plan.draft.tasks {
        let file_count = task
            .scope
            .allowed_files
            .len()
            .max(task.scope.write_scope.len());
        if file_count > MAX_FILES_PER_WORK_ORDER {
            findings.push(format!(
                "task `{}` is too broad for one work order ({} scoped files, max_files_changed={}): split it into independently verifiable work orders",
                task.task_id,
                file_count,
                task.scope.max_files_changed
            ));
        }
        if task.must_do.len() > MAX_MUST_DO_ITEMS_PER_WORK_ORDER {
            findings.push(format!(
                "task `{}` contains {} must-do steps; split hidden substeps into separate work orders",
                task.task_id,
                task.must_do.len()
            ));
        }
    }
    findings
}

fn reference_findings(plan: &PlanGraph, workspace_root: &Path) -> Vec<String> {
    let mut findings = Vec::new();
    let canonical_root = match workspace_root.canonicalize() {
        Ok(root) if root.is_dir() => Some(root),
        Ok(_) => {
            findings.push(format!(
                "workspace root is not a directory: {}",
                workspace_root.display()
            ));
            None
        }
        Err(error) => {
            findings.push(format!(
                "workspace root cannot be resolved: {} ({error})",
                workspace_root.display()
            ));
            None
        }
    };

    for task in &plan.draft.tasks {
        for reference in &task.references {
            if let Err(error) = validate_repository_relative_path(&reference.path) {
                findings.push(format!(
                    "task `{}` reference `{}` is unsafe: {error}",
                    task.task_id, reference.path
                ));
                continue;
            }
            let Some(canonical_root) = canonical_root.as_deref() else {
                continue;
            };
            let candidate = workspace_root.join(&reference.path);
            let canonical_candidate = match candidate.canonicalize() {
                Ok(candidate) => candidate,
                Err(error) => {
                    findings.push(format!(
                        "task `{}` reference `{}` does not resolve: {error}",
                        task.task_id, reference.path
                    ));
                    continue;
                }
            };
            if !canonical_candidate.starts_with(canonical_root) {
                findings.push(format!(
                    "task `{}` reference `{}` resolves outside the workspace",
                    task.task_id, reference.path
                ));
            }
        }
    }
    findings
}

fn scope_findings(plan: &PlanGraph) -> Vec<String> {
    let mut findings = Vec::new();
    for task in &plan.draft.tasks {
        for (label, paths) in [
            ("allowed_files", &task.scope.allowed_files),
            ("forbidden_files", &task.scope.forbidden_files),
            ("write_scope", &task.scope.write_scope),
        ] {
            for path in paths {
                if let Err(error) = validate_repository_relative_path(path) {
                    findings.push(format!(
                        "task `{}` {label} path `{path}` is unsafe: {error}",
                        task.task_id
                    ));
                }
            }
        }
        if !task.scope.write_scope.is_empty() && task.scope.max_files_changed == 0 {
            findings.push(format!(
                "task `{}` has write scope but max_files_changed is zero",
                task.task_id
            ));
        }
        for write_path in &task.scope.write_scope {
            if !task.scope.allowed_files.is_empty()
                && !task
                    .scope
                    .allowed_files
                    .iter()
                    .any(|allowed_path| path_is_within(write_path, allowed_path))
            {
                findings.push(format!(
                    "task `{}` write scope `{write_path}` is outside allowed_files",
                    task.task_id
                ));
            }
            for forbidden_path in &task.scope.forbidden_files {
                if paths_overlap(write_path, forbidden_path) {
                    findings.push(format!(
                        "task `{}` write scope `{write_path}` overlaps forbidden path `{forbidden_path}`",
                        task.task_id
                    ));
                }
            }
        }
    }
    findings
}

fn test_findings(plan: &PlanGraph) -> Vec<String> {
    let mut findings = Vec::new();
    for task in &plan.draft.tasks {
        match task.test.strategy {
            TestStrategy::Tdd => {
                if let Some(red) = task.test.red.as_ref() {
                    findings.extend(command_findings(&task.task_id, "RED", red));
                } else {
                    findings.push(format!(
                        "task `{}` is TDD but has no RED command",
                        task.task_id
                    ));
                }
                if task.test.green.is_empty() {
                    findings.push(format!(
                        "task `{}` is TDD but has no GREEN command",
                        task.task_id
                    ));
                }
                for command in &task.test.green {
                    findings.extend(command_findings(&task.task_id, "GREEN", command));
                }
            }
            TestStrategy::TestsAfter => {
                if task.test.red.is_some() {
                    findings.push(format!(
                        "task `{}` uses tests_after but also defines a RED command",
                        task.task_id
                    ));
                }
                if task.test.green.is_empty() {
                    findings.push(format!(
                        "task `{}` uses tests_after but has no GREEN command",
                        task.task_id
                    ));
                }
                for command in &task.test.green {
                    findings.extend(command_findings(&task.task_id, "GREEN", command));
                }
            }
            TestStrategy::None => {
                if task.test.red.is_some() || !task.test.green.is_empty() {
                    findings.push(format!(
                        "task `{}` declares no tests but includes test commands",
                        task.task_id
                    ));
                }
                if task
                    .test
                    .no_test_reason
                    .as_deref()
                    .is_none_or(|reason| reason.trim().is_empty())
                {
                    findings.push(format!(
                        "task `{}` declares no tests without a reason",
                        task.task_id
                    ));
                }
            }
        }
    }
    findings
}

fn command_findings(
    task_id: &str,
    label: &str,
    command: &crate::plan_graph::CommandExpectation,
) -> Vec<String> {
    let mut findings = Vec::new();
    if command.command.trim().is_empty() {
        findings.push(format!("task `{task_id}` {label} command is empty"));
    }
    if command.expected_observation.trim().is_empty() {
        findings.push(format!(
            "task `{task_id}` {label} expected observation is empty"
        ));
    }
    if let Err(error) = validate_repository_relative_path(&command.evidence_path) {
        findings.push(format!(
            "task `{task_id}` {label} evidence path `{}` is unsafe: {error}",
            command.evidence_path
        ));
    }
    findings
}

fn qa_findings(plan: &PlanGraph) -> Vec<String> {
    let mut findings = Vec::new();
    for task in &plan.draft.tasks {
        for (kind, scenarios) in [
            ("happy", &task.qa.happy_path),
            ("failure", &task.qa.failure_path),
        ] {
            if scenarios.is_empty() {
                findings.push(format!(
                    "task `{}` has no {kind}-path QA scenario",
                    task.task_id
                ));
            }
            let mut names = HashSet::new();
            for scenario in scenarios {
                if scenario.name.trim().is_empty() {
                    findings.push(format!(
                        "task `{}` has an unnamed {kind}-path QA scenario",
                        task.task_id
                    ));
                } else if !names.insert(scenario.name.as_str()) {
                    findings.push(format!(
                        "task `{}` repeats {kind}-path QA scenario `{}`",
                        task.task_id, scenario.name
                    ));
                }
                if scenario.steps.is_empty()
                    || scenario.steps.iter().any(|step| step.trim().is_empty())
                {
                    findings.push(format!(
                        "task `{}` QA scenario `{}` has empty steps",
                        task.task_id, scenario.name
                    ));
                }
                if scenario.expected_result.trim().is_empty() {
                    findings.push(format!(
                        "task `{}` QA scenario `{}` has no expected result",
                        task.task_id, scenario.name
                    ));
                }
                if let Err(error) = validate_repository_relative_path(&scenario.evidence_path) {
                    findings.push(format!(
                        "task `{}` QA scenario `{}` evidence path `{}` is unsafe: {error}",
                        task.task_id, scenario.name, scenario.evidence_path
                    ));
                }
            }
        }
    }
    findings
}

fn acceptance_findings(plan: &PlanGraph) -> Vec<String> {
    let mut findings = Vec::new();
    if plan.draft.must_have.is_empty()
        || plan
            .draft
            .must_have
            .iter()
            .any(|item| item.trim().is_empty())
    {
        findings.push("plan must_have contract is empty or contains a blank item".to_string());
    }
    if plan.draft.final_acceptance.is_empty()
        || plan
            .draft
            .final_acceptance
            .iter()
            .any(|item| item.trim().is_empty())
    {
        findings
            .push("plan final_acceptance contract is empty or contains a blank item".to_string());
    }
    for task in &plan.draft.tasks {
        if task.completion_predicates.is_empty()
            || task
                .completion_predicates
                .iter()
                .any(|predicate| predicate.trim().is_empty())
        {
            findings.push(format!(
                "task `{}` completion predicates are empty or blank",
                task.task_id
            ));
        }
        if !task.artifacts.iter().any(|artifact| artifact.required) {
            findings.push(format!("task `{}` has no required artifact", task.task_id));
        }
        let mut artifact_paths = HashSet::new();
        for artifact in &task.artifacts {
            if artifact.description.trim().is_empty() {
                findings.push(format!(
                    "task `{}` artifact `{}` has no description",
                    task.task_id, artifact.path
                ));
            }
            if let Err(error) = validate_repository_relative_path(&artifact.path) {
                findings.push(format!(
                    "task `{}` artifact path `{}` is unsafe: {error}",
                    task.task_id, artifact.path
                ));
            }
            if !artifact_paths.insert(artifact.path.as_str()) {
                findings.push(format!(
                    "task `{}` repeats artifact path `{}`",
                    task.task_id, artifact.path
                ));
            }
        }
    }
    findings
}

fn validate_verifier_checks(checks: &[PlanVerifierCheck]) -> Result<()> {
    let required = [
        PlanVerifierDimension::Structure,
        PlanVerifierDimension::ReferencePaths,
        PlanVerifierDimension::Scope,
        PlanVerifierDimension::TestContract,
        PlanVerifierDimension::QaContract,
        PlanVerifierDimension::AcceptanceContract,
    ];
    let mut seen = HashSet::new();
    for check in checks {
        if !seen.insert(check.dimension) {
            bail!("duplicate plan verifier check for {:?}", check.dimension);
        }
        require_non_empty("plan verifier check summary", &check.summary)?;
        if check.passed && !check.findings.is_empty() {
            bail!("passing plan verifier check cannot contain findings");
        }
        if !check.passed && check.findings.is_empty() {
            bail!("failing plan verifier check must contain findings");
        }
        for finding in &check.findings {
            require_non_empty("plan verifier finding", finding)?;
        }
    }
    if seen.len() != required.len() || required.iter().any(|dimension| !seen.contains(dimension)) {
        bail!("plan verifier report is missing one or more required checks");
    }
    Ok(())
}

fn validate_critic_checks(checks: &[PlanCriticCheck]) -> Result<()> {
    let required = [
        PlanCriticDimension::References,
        PlanCriticDimension::Executability,
        PlanCriticDimension::Contradictions,
        PlanCriticDimension::Scope,
        PlanCriticDimension::Tdd,
        PlanCriticDimension::Qa,
        PlanCriticDimension::Acceptance,
    ];
    let mut seen = HashSet::new();
    for check in checks {
        if !seen.insert(check.dimension) {
            bail!("duplicate plan critic check for {:?}", check.dimension);
        }
        require_non_empty("plan critic check summary", &check.summary)?;
        if check.evidence_refs.is_empty() {
            bail!("plan critic check must cite at least one evidence reference");
        }
        for evidence_ref in &check.evidence_refs {
            require_non_empty("plan critic evidence reference", evidence_ref)?;
        }
    }
    if seen.len() != required.len() || required.iter().any(|dimension| !seen.contains(dimension)) {
        bail!("plan critic verdict is missing one or more required checks");
    }
    Ok(())
}

fn validate_critic_findings(
    findings: &[PlanCriticFinding],
    checks: &[PlanCriticCheck],
) -> Result<()> {
    for finding in findings {
        require_non_empty("plan critic finding code", &finding.code)?;
        require_non_empty("plan critic finding message", &finding.message)?;
        validate_optional_non_empty("plan critic finding task_id", finding.task_id.as_deref())?;
        validate_optional_non_empty("plan critic finding path", finding.path.as_deref())?;
        validate_optional_non_empty(
            "plan critic finding required_change",
            finding.required_change.as_deref(),
        )?;
        if finding.severity == PlanCriticFindingSeverity::Blocking {
            require_some_non_empty(
                "blocking finding required_change",
                finding.required_change.as_deref(),
            )?;
            let matching_check = checks
                .iter()
                .find(|check| check.dimension == finding.dimension)
                .context("blocking finding has no matching critic check")?;
            if matching_check.verdict != PlanCriticCheckVerdict::Fail {
                bail!("blocking finding must belong to a failed critic check");
            }
        }
    }
    for check in checks
        .iter()
        .filter(|check| check.verdict == PlanCriticCheckVerdict::Fail)
    {
        if !findings.iter().any(|finding| {
            finding.dimension == check.dimension
                && finding.severity == PlanCriticFindingSeverity::Blocking
        }) {
            bail!("every failed critic check must have a blocking finding");
        }
    }
    Ok(())
}

fn validate_plan_binding(
    goal_id: &str,
    plan_id: &str,
    plan_revision: usize,
    plan_hash: &str,
    plan: &PlanGraph,
) -> Result<()> {
    if goal_id != plan.goal_id {
        bail!("receipt goal id does not match PlanGraph");
    }
    if plan_id != plan.plan_id {
        bail!("receipt plan id does not match PlanGraph");
    }
    if plan_revision != plan.revision {
        bail!("receipt plan revision does not match PlanGraph");
    }
    if plan_hash != plan.plan_hash {
        bail!("receipt plan hash does not match PlanGraph");
    }
    validate_sha256("PlanGraph hash", plan_hash)?;
    Ok(())
}

fn validate_repository_relative_path(path: &str) -> Result<()> {
    require_non_empty("repository-relative path", path)?;
    if path.contains('\0') {
        bail!("path contains a NUL byte");
    }
    if path.len() > 1 && path.as_bytes().get(1) == Some(&b':') {
        bail!("path contains a Windows drive prefix");
    }
    let path = Path::new(path);
    if path.is_absolute() {
        bail!("path is absolute");
    }
    for component in path.components() {
        if matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            bail!("path escapes the repository root");
        }
    }
    Ok(())
}

fn path_is_within(path: &str, parent: &str) -> bool {
    let path = normalize_repository_path(path);
    let parent = normalize_repository_path(parent);
    parent.is_empty() || path == parent || path.starts_with(&format!("{parent}/"))
}

fn paths_overlap(left: &str, right: &str) -> bool {
    path_is_within(left, right) || path_is_within(right, left)
}

fn normalize_repository_path(path: &str) -> String {
    path.trim()
        .trim_start_matches("./")
        .trim_matches('/')
        .to_string()
}

fn require_non_empty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} cannot be empty");
    }
    Ok(())
}

fn require_some_non_empty(field: &str, value: Option<&str>) -> Result<()> {
    let value = value.with_context(|| format!("{field} is required"))?;
    require_non_empty(field, value)
}

fn validate_optional_non_empty(field: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        require_non_empty(field, value)?;
    }
    Ok(())
}

fn validate_non_empty_unique_values(field: &str, values: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for value in values {
        require_non_empty(field, value)?;
        if !seen.insert(value) {
            bail!("intent fold contains duplicate {field} `{value}`");
        }
    }
    Ok(())
}

fn validate_sha256(field: &str, hash: &str) -> Result<()> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{field} must be a lowercase SHA-256 hex digest");
    }
    Ok(())
}

fn hash_serializable(value: &(impl Serialize + ?Sized)) -> Result<String> {
    let bytes = serde_json::to_vec(value).context("failed to serialize review hash input")?;
    Ok(sha256_bytes(&bytes))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_phase_parser_accepts_provider_prose_before_json() -> Result<()> {
        let raw = r#"I inspected the workspace first.
{"schema_version":1,"goal_id":"goal","normalized_objective":"outcome","assumptions":[],"constraints":[],"ambiguities":[],"required_questions":[],"risks":[],"acceptance_signals":["verified"],"decision":"ready","summary":"ready"}"#;
        let verdict = IntentFoldVerdict::parse(raw)?;
        assert_eq!(verdict.goal_id, "goal");
        Ok(())
    }
    use crate::{
        plan_graph::{
            CommandExpectation, PlanReference, PlannerReceipt, QaScenario,
            deterministic_fallback_draft,
        },
        state::Scope,
    };
    use std::fs;

    struct Fixture {
        _temp_dir: tempfile::TempDir,
        plan: PlanGraph,
        planner_raw_output: String,
        planner: PlannerExecutionReceipt,
        verifier: PlanVerifierReport,
    }

    fn fixture() -> Result<Fixture> {
        let temp_dir = tempfile::tempdir()?;
        fs::create_dir_all(temp_dir.path().join("src"))?;
        fs::write(temp_dir.path().join("src/lib.rs"), "pub fn sample() {}\n")?;
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let mut draft =
            deterministic_fallback_draft("Implement feature", &scope, &["cargo test".to_string()]);
        let task = draft.tasks.first_mut().context("fixture task missing")?;
        task.execution_steps_evidence_required = true;
        task.references.push(PlanReference {
            path: "src/lib.rs".to_string(),
            reason: "Existing implementation entry point".to_string(),
            symbol: Some("sample".to_string()),
        });
        let plan = PlanGraph::seal(
            "goal-1",
            1,
            PlanSource::PlannerModel,
            Some(PlannerReceipt {
                provider_id: "provider".to_string(),
                model_id: "planner-model".to_string(),
                session_id: Some("planner-actual-session".to_string()),
            }),
            draft,
        )?;
        let planner_raw_output = serde_json::to_string(&plan.draft)?;
        let planner = PlannerExecutionReceipt::seal(
            &plan,
            planner_identity(),
            &planner_raw_output,
            Some("artifacts/planner-output.json".to_string()),
            "2026-07-11T00:00:00Z",
        )?;
        let verifier = PlanVerifierReport::verify(&plan, temp_dir.path())?;
        Ok(Fixture {
            _temp_dir: temp_dir,
            plan,
            planner_raw_output,
            planner,
            verifier,
        })
    }

    fn planner_identity() -> PhaseExecutionIdentity {
        PhaseExecutionIdentity {
            execution_id: "planner-execution-1".to_string(),
            phase_session_id: "planner-phase-session-1".to_string(),
            backend: PhaseExecutionBackend::NativeAgent,
            agent_id: Some("zed-agent".to_string()),
            provider_id: Some("provider".to_string()),
            model_id: Some("planner-model".to_string()),
            actual_session_id: Some("planner-actual-session".to_string()),
        }
    }

    fn critic_identity() -> PhaseExecutionIdentity {
        PhaseExecutionIdentity {
            execution_id: "critic-execution-1".to_string(),
            phase_session_id: "critic-phase-session-1".to_string(),
            backend: PhaseExecutionBackend::NativeAgent,
            agent_id: Some("zed-agent".to_string()),
            provider_id: Some("provider".to_string()),
            model_id: Some("critic-model".to_string()),
            actual_session_id: Some("critic-actual-session".to_string()),
        }
    }

    fn checks(verdict: PlanCriticCheckVerdict) -> Vec<PlanCriticCheck> {
        [
            PlanCriticDimension::References,
            PlanCriticDimension::Executability,
            PlanCriticDimension::Contradictions,
            PlanCriticDimension::Scope,
            PlanCriticDimension::Tdd,
            PlanCriticDimension::Qa,
            PlanCriticDimension::Acceptance,
        ]
        .into_iter()
        .map(|dimension| PlanCriticCheck {
            dimension,
            verdict,
            summary: format!("{dimension:?} reviewed"),
            evidence_refs: vec!["verifier-report.json".to_string()],
        })
        .collect()
    }

    fn approved_verdict(fixture: &Fixture) -> PlanCriticVerdict {
        PlanCriticVerdict {
            schema_version: PLAN_REVIEW_SCHEMA_VERSION,
            reviewed_goal_id: fixture.plan.goal_id.clone(),
            reviewed_plan_id: fixture.plan.plan_id.clone(),
            reviewed_plan_revision: fixture.plan.revision,
            reviewed_plan_hash: fixture.plan.plan_hash.clone(),
            reviewed_planner_execution_id: fixture.planner.identity.execution_id.clone(),
            decision: PlanCriticDecision::Approve,
            checks: checks(PlanCriticCheckVerdict::Pass),
            findings: Vec::new(),
            revision_instructions: None,
            needs_user_reason: None,
            summary: "The plan is executable without a blocking gap.".to_string(),
        }
    }

    fn blocking_finding(dimension: PlanCriticDimension) -> PlanCriticFinding {
        PlanCriticFinding {
            dimension,
            severity: PlanCriticFindingSeverity::Blocking,
            code: "missing-reference".to_string(),
            task_id: Some("task_003".to_string()),
            path: Some("src/missing.rs".to_string()),
            message: "The referenced starting point is missing.".to_string(),
            required_change: Some("Replace it with an existing repository path.".to_string()),
        }
    }

    #[test]
    fn trusted_receipts_round_trip_and_approve_exact_plan() -> Result<()> {
        let fixture = fixture()?;
        assert!(fixture.verifier.passed());
        assert!(
            fixture
                .verifier
                .limitations
                .contains(&PLAN_VERIFIER_LIMITATION.to_string())
        );
        let verdict = approved_verdict(&fixture);
        let raw_output = serde_json::to_string(&verdict)?;
        let receipt = PlanCriticReceipt::seal(
            &fixture.plan,
            &fixture.planner,
            &fixture.planner_raw_output,
            &fixture.verifier,
            critic_identity(),
            verdict,
            &raw_output,
            Some("artifacts/critic-receipt.json".to_string()),
            "2026-07-11T00:01:00Z",
        )?;
        assert!(receipt.approved());

        let round_trip: PlanCriticReceipt =
            serde_json::from_str(&serde_json::to_string(&receipt)?)?;
        round_trip.validate(
            &fixture.plan,
            &fixture.planner,
            &fixture.planner_raw_output,
            &fixture.verifier,
            &raw_output,
        )?;
        Ok(())
    }

    #[test]
    fn intent_fold_receipt_rejects_goal_rebinding() -> Result<()> {
        let verdict = IntentFoldVerdict {
            schema_version: PLAN_REVIEW_SCHEMA_VERSION,
            goal_id: "goal-1".to_string(),
            normalized_objective: "Implement the requested change".to_string(),
            assumptions: Vec::new(),
            constraints: vec!["Preserve existing behavior".to_string()],
            ambiguities: Vec::new(),
            required_questions: Vec::new(),
            risks: vec![IntentRisk {
                code: "regression".to_string(),
                severity: IntentRiskSeverity::Medium,
                description: "Existing behavior could regress".to_string(),
                mitigation: "Run focused regression tests".to_string(),
            }],
            acceptance_signals: vec!["Focused tests pass".to_string()],
            decision: IntentFoldDecision::Ready,
            summary: "The request is ready for planning".to_string(),
        };
        let mut receipt = IntentFoldReceipt::seal(
            verdict,
            planner_identity(),
            "intent-fold-output",
            None,
            "2026-07-12T00:00:00Z".to_string(),
        )?;
        receipt.goal_id = "goal-2".to_string();

        assert!(receipt.validate().is_err());
        Ok(())
    }

    #[test]
    fn planner_receipt_rejects_raw_output_and_identity_mismatch() -> Result<()> {
        let fixture = fixture()?;
        assert!(
            fixture
                .planner
                .validate(&fixture.plan, "{\"objective\":\"different\"}")
                .is_err()
        );
        let mut planner = fixture.planner.clone();
        planner.identity.model_id = Some("other-model".to_string());
        assert!(
            planner
                .validate(&fixture.plan, &fixture.planner_raw_output)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn verifier_reports_missing_references_without_claiming_content_review() -> Result<()> {
        let fixture = fixture()?;
        let mut draft = fixture.plan.draft.clone();
        let task = draft.tasks.first_mut().context("fixture task missing")?;
        let reference = task
            .references
            .first_mut()
            .context("fixture reference missing")?;
        reference.path = "src/missing.rs".to_string();
        let plan = PlanGraph::seal(
            "goal-1",
            2,
            PlanSource::PlannerModel,
            fixture.plan.planner.clone(),
            draft,
        )?;
        let report = PlanVerifierReport::verify(&plan, fixture._temp_dir.path())?;
        assert!(!report.passed());
        let reference_check = report
            .check(PlanVerifierDimension::ReferencePaths)
            .context("reference check missing")?;
        assert!(!reference_check.passed);
        assert!(
            report
                .limitations
                .iter()
                .any(|limitation| limitation == PLAN_VERIFIER_LIMITATION)
        );
        Ok(())
    }

    #[test]
    fn verifier_blocks_plans_with_unresolved_open_questions() -> Result<()> {
        let fixture = fixture()?;
        let mut draft = fixture.plan.draft.clone();
        draft.open_questions = vec!["Which persistence boundary owns the migration?".to_string()];
        let plan = PlanGraph::seal(
            "goal-1",
            2,
            PlanSource::PlannerModel,
            fixture.plan.planner.clone(),
            draft,
        )?;
        let report = PlanVerifierReport::verify(&plan, fixture._temp_dir.path())?;
        let structure = report
            .check(PlanVerifierDimension::Structure)
            .context("structure check missing")?;
        assert!(!report.passed());
        assert!(!structure.passed);
        assert!(
            structure
                .findings
                .iter()
                .any(|finding| finding.contains("unresolved planning question"))
        );
        Ok(())
    }

    #[test]
    fn verifier_catches_structural_tdd_qa_and_acceptance_gaps() -> Result<()> {
        let fixture = fixture()?;
        let mut draft = fixture.plan.draft.clone();
        let task = draft.tasks.first_mut().context("fixture task missing")?;
        task.test.strategy = TestStrategy::Tdd;
        task.test.red = Some(CommandExpectation {
            command: String::new(),
            expected_observation: String::new(),
            evidence_path: "../red.txt".to_string(),
        });
        task.test.green = vec![CommandExpectation {
            command: String::new(),
            expected_observation: String::new(),
            evidence_path: "../green.txt".to_string(),
        }];
        task.qa.happy_path = vec![QaScenario {
            name: "happy".to_string(),
            steps: vec![String::new()],
            expected_result: "result".to_string(),
            evidence_path: "../qa.txt".to_string(),
        }];
        task.completion_predicates = vec![String::new()];
        let plan = PlanGraph::seal(
            "goal-1",
            2,
            PlanSource::PlannerModel,
            fixture.plan.planner.clone(),
            draft,
        )?;
        let report = PlanVerifierReport::verify(&plan, fixture._temp_dir.path())?;
        assert!(!report.passed());
        assert!(
            !report
                .check(PlanVerifierDimension::TestContract)
                .context("test check missing")?
                .passed
        );
        assert!(
            !report
                .check(PlanVerifierDimension::QaContract)
                .context("QA check missing")?
                .passed
        );
        assert!(
            !report
                .check(PlanVerifierDimension::AcceptanceContract)
                .context("acceptance check missing")?
                .passed
        );
        Ok(())
    }

    #[test]
    fn verifier_rejects_work_orders_that_hide_too_many_steps_or_files() -> Result<()> {
        let fixture = fixture()?;
        let mut draft = fixture.plan.draft.clone();
        let task = draft.tasks.first_mut().context("fixture task missing")?;
        task.scope.allowed_files = (0..9).map(|index| format!("src/file-{index}.rs")).collect();
        task.scope.max_files_changed = 9;
        task.must_do = (0..13).map(|index| format!("step-{index}")).collect();
        let plan = PlanGraph::seal(
            "goal-1",
            2,
            PlanSource::PlannerModel,
            fixture.plan.planner.clone(),
            draft,
        )?;
        let report = PlanVerifierReport::verify(&plan, fixture._temp_dir.path())?;
        let structure = report
            .check(PlanVerifierDimension::Structure)
            .context("structure check missing")?;
        assert!(!structure.passed);
        assert!(
            structure
                .findings
                .iter()
                .any(|finding| finding.contains("too broad"))
        );
        assert!(
            structure
                .findings
                .iter()
                .any(|finding| finding.contains("hidden substeps"))
        );
        Ok(())
    }

    #[test]
    fn critic_receipt_rejects_same_planner_and_reviewer_execution() -> Result<()> {
        let fixture = fixture()?;
        let verdict = approved_verdict(&fixture);
        let raw_output = serde_json::to_string(&verdict)?;
        assert!(
            PlanCriticReceipt::seal(
                &fixture.plan,
                &fixture.planner,
                &fixture.planner_raw_output,
                &fixture.verifier,
                fixture.planner.identity.clone(),
                verdict,
                &raw_output,
                None,
                "2026-07-11T00:01:00Z",
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn critic_verdict_enforces_approve_revise_reject_consistency() -> Result<()> {
        let fixture = fixture()?;
        let mut approve = approved_verdict(&fixture);
        let reference_check = approve
            .checks
            .iter_mut()
            .find(|check| check.dimension == PlanCriticDimension::References)
            .context("reference check missing")?;
        reference_check.verdict = PlanCriticCheckVerdict::Fail;
        approve
            .findings
            .push(blocking_finding(PlanCriticDimension::References));
        assert!(
            approve
                .validate(&fixture.plan, &fixture.planner, &fixture.verifier)
                .is_err()
        );

        let mut revise = approve.clone();
        revise.decision = PlanCriticDecision::Revise;
        revise.revision_instructions = None;
        assert!(
            revise
                .validate(&fixture.plan, &fixture.planner, &fixture.verifier)
                .is_err()
        );
        revise.revision_instructions = Some("Repair the missing reference.".to_string());
        revise.validate(&fixture.plan, &fixture.planner, &fixture.verifier)?;

        let mut reject = revise;
        reject.decision = PlanCriticDecision::Reject;
        reject.revision_instructions = None;
        assert!(
            reject
                .validate(&fixture.plan, &fixture.planner, &fixture.verifier)
                .is_err()
        );
        reject.needs_user_reason = Some("The user must choose a valid subsystem.".to_string());
        reject.validate(&fixture.plan, &fixture.planner, &fixture.verifier)?;
        Ok(())
    }

    #[test]
    fn critic_receipt_detects_stale_binding_and_tampering() -> Result<()> {
        let fixture = fixture()?;
        let verdict = approved_verdict(&fixture);
        let raw_output = serde_json::to_string(&verdict)?;
        let receipt = PlanCriticReceipt::seal(
            &fixture.plan,
            &fixture.planner,
            &fixture.planner_raw_output,
            &fixture.verifier,
            critic_identity(),
            verdict,
            &raw_output,
            None,
            "2026-07-11T00:01:00Z",
        )?;

        let mut changed_draft = fixture.plan.draft.clone();
        changed_draft.objective.push_str(" changed");
        let changed_plan = PlanGraph::seal(
            "goal-1",
            2,
            PlanSource::PlannerModel,
            fixture.plan.planner.clone(),
            changed_draft,
        )?;
        assert!(
            receipt
                .validate(
                    &changed_plan,
                    &fixture.planner,
                    &fixture.planner_raw_output,
                    &fixture.verifier,
                    &raw_output,
                )
                .is_err()
        );

        let mut tampered = receipt;
        tampered.verdict.summary.push_str(" tampered");
        assert!(
            tampered
                .validate(
                    &fixture.plan,
                    &fixture.planner,
                    &fixture.planner_raw_output,
                    &fixture.verifier,
                    &raw_output,
                )
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn verifier_report_detects_tampering() -> Result<()> {
        let fixture = fixture()?;
        let mut report = fixture.verifier.clone();
        let check = report
            .checks
            .first_mut()
            .context("verifier check missing")?;
        check.summary.push_str(" tampered");
        assert!(report.validate(&fixture.plan).is_err());
        Ok(())
    }

    #[test]
    fn approval_state_round_trips_without_polluting_canonical_plans() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = crate::state::StateStore::new(temp_dir.path());
        store.initialize()?;
        let state = PlanApprovalState {
            schema_version: PLAN_REVIEW_SCHEMA_VERSION,
            goal_id: "goal-1".to_string(),
            plan_id: "plan-1".to_string(),
            plan_revision: 2,
            plan_hash: "1".repeat(64),
            status: PlanApprovalStatus::Approved,
            planner_receipt_hash: "2".repeat(64),
            verifier_report_hash: "3".repeat(64),
            critic_receipt_hash: Some("4".repeat(64)),
            secondary_critic_receipt_hash: None,
            revisions_used: 1,
            updated_at: "2026-07-11T00:00:00Z".to_string(),
        };
        let path = store.write_plan_approval_state(&state)?;
        assert!(path.starts_with(store.plan_reviews_dir()));
        assert_eq!(store.read_plan_approval_state("goal-1")?, Some(state));
        assert_eq!(std::fs::read_dir(store.plans_dir())?.count(), 0);
        Ok(())
    }

    #[test]
    fn approval_state_rejects_a_stale_plan_revision() -> Result<()> {
        let fixture = fixture()?;
        let state = PlanApprovalState {
            schema_version: PLAN_REVIEW_SCHEMA_VERSION,
            goal_id: fixture.plan.goal_id.clone(),
            plan_id: fixture.plan.plan_id.clone(),
            plan_revision: fixture.plan.revision,
            plan_hash: fixture.plan.plan_hash.clone(),
            status: PlanApprovalStatus::Approved,
            planner_receipt_hash: fixture.planner.receipt_hash.clone(),
            verifier_report_hash: fixture.verifier.report_hash.clone(),
            critic_receipt_hash: Some("4".repeat(64)),
            secondary_critic_receipt_hash: None,
            revisions_used: 0,
            updated_at: "2026-07-11T00:00:00Z".to_string(),
        };
        state.validate_against(&fixture.plan)?;
        let mut changed_draft = fixture.plan.draft.clone();
        changed_draft.objective.push_str(" changed");
        let changed = PlanGraph::seal(
            &fixture.plan.goal_id,
            fixture.plan.revision + 1,
            fixture.plan.source.clone(),
            fixture.plan.planner.clone(),
            changed_draft,
        )?;
        assert!(state.validate_against(&changed).is_err());
        Ok(())
    }
}
