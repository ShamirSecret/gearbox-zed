use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::plan_graph::PhaseProfile;
use crate::workers::{
    WorkerCategory, WorkerConfig, WorkerKind, WorkerRoute, worker_model_is_unavailable,
};

pub const PHASE_ROUTE_SCHEMA_VERSION: u32 = 1;

pub const ALL_PHASE_PROFILES: &[PhaseProfile] = &[
    PhaseProfile::Planner,
    PhaseProfile::PlanCritic,
    PhaseProfile::Orchestrator,
    PhaseProfile::ExecutorQuick,
    PhaseProfile::ExecutorDeep,
    PhaseProfile::ReviewerTask,
    PhaseProfile::ReviewerFinal,
    PhaseProfile::StrategistNextGoal,
    PhaseProfile::Summarizer,
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseRouteTable {
    pub schema_version: u32,
    pub profiles: Vec<PhaseRouteProfile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseRouteProfile {
    pub phase: PhaseProfile,
    pub category: WorkerCategory,
    pub candidates: Vec<PhaseRouteCandidate>,
    pub source: PhaseRouteSource,
    pub reasoning_tier: ReasoningTier,
    pub context_tier: ContextTier,
    pub can_write: bool,
    pub can_review: bool,
    pub cache_supported: bool,
    pub latency_tier: LatencyTier,
    pub independence_group: String,
    pub max_calls_per_epoch: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens_per_call: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_micros_per_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseRouteCandidate {
    pub backend: PhaseBackend,
    pub model: PhaseModelBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PhaseBackend {
    Deterministic,
    DirectModel,
    NativeZed,
    Worker(WorkerKind),
    LegacyCategory,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PhaseModelBinding {
    None,
    CurrentSession,
    ExactLive(ModelSelectorId),
    BackendDeclared(String),
}

impl PhaseModelBinding {
    pub fn is_available(&self) -> bool {
        matches!(
            self,
            PhaseModelBinding::CurrentSession | PhaseModelBinding::ExactLive(_)
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelSelectorId {
    pub agent_id: String,
    pub provider_id: String,
    pub model_id: String,
}

impl ModelSelectorId {
    pub fn from_qualified(agent_id: impl Into<String>, qualified_id: &str) -> Result<Self> {
        let qualified_id = qualified_id.trim();
        let Some((provider_id, model_id)) = qualified_id.split_once('/') else {
            bail!("phase model `{qualified_id}` must use a qualified `provider/model` id");
        };
        let agent_id = agent_id.into();
        let agent_id = agent_id.trim();
        if agent_id.trim().is_empty() || provider_id.trim().is_empty() || model_id.trim().is_empty()
        {
            bail!("phase model identity must include non-empty agent, provider, and model ids");
        }
        Ok(Self {
            agent_id: agent_id.to_string(),
            provider_id: provider_id.trim().to_string(),
            model_id: model_id.trim().to_string(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        for (label, value) in [
            ("agent", self.agent_id.as_str()),
            ("provider", self.provider_id.as_str()),
            ("model", self.model_id.as_str()),
        ] {
            if value.trim().is_empty() || value.trim() != value {
                bail!("phase model {label} id must be non-empty and trimmed");
            }
        }
        if self.agent_id.contains('/') || self.provider_id.contains('/') {
            bail!("phase model agent and provider ids cannot contain `/`");
        }
        Ok(())
    }

    pub fn qualified_model_id(&self) -> String {
        format!("{}/{}", self.provider_id, self.model_id)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveModelInventory {
    pub models: Vec<ModelSelectorId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenCodeModelProfiles {
    pub planner: String,
    pub executor: String,
    pub reviewer: String,
}

impl OpenCodeModelProfiles {
    pub fn validate(&self) -> Result<()> {
        for (role, model) in [
            ("planner", self.planner.as_str()),
            ("executor", self.executor.as_str()),
            ("reviewer", self.reviewer.as_str()),
        ] {
            ModelSelectorId::from_qualified("opencode_session", model)
                .map_err(|error| anyhow::anyhow!("OpenCode {role} model is invalid: {error}"))?;
        }
        Ok(())
    }
}

impl LiveModelInventory {
    pub fn validate(&self) -> Result<()> {
        for (index, model) in self.models.iter().enumerate() {
            model.validate()?;
            if self.models[..index].contains(model) {
                bail!(
                    "duplicate live phase model `{}` for agent `{}`",
                    model.qualified_model_id(),
                    model.agent_id
                );
            }
        }
        Ok(())
    }

    pub fn contains(&self, model: &ModelSelectorId) -> bool {
        self.models.iter().any(|candidate| candidate == model)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseRouteSource {
    LegacyDefault,
    BuiltIn,
    Environment,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningTier {
    Deterministic,
    Low,
    Medium,
    High,
    ExtraHigh,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTier {
    Minimal,
    Task,
    Plan,
    Goal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyTier {
    Immediate,
    Interactive,
    Background,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedPhaseCandidate {
    pub candidate_index: usize,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseRouteDecision {
    pub phase: PhaseProfile,
    pub category: WorkerCategory,
    pub selected_candidate: usize,
    pub candidate: PhaseRouteCandidate,
    pub rejected_candidates: Vec<RejectedPhaseCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<ModelSelectorId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_kind: Option<WorkerKind>,
    pub profile_hash: String,
    pub source: PhaseRouteSource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelBindingStatus {
    Applied,
    DeclaredUnverified,
    CurrentSession,
    Deterministic,
    LegacyUnverified,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseRouteReceipt {
    pub decision: PhaseRouteDecision,
    pub ordinal: usize,
    pub plan_revision: usize,
    pub decision_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_model: Option<ModelSelectorId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_worker_kind: Option<WorkerKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_category: Option<WorkerCategory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_worker_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_route_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_record_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_record_sha256: Option<String>,
    pub binding_status: ModelBindingStatus,
    pub receipt_hash: String,
}

impl PhaseRouteTable {
    pub fn opencode_only(models: OpenCodeModelProfiles) -> Result<Self> {
        models.validate()?;
        let mut table = Self::legacy_defaults();
        for profile in &mut table.profiles {
            let model = match profile.phase {
                PhaseProfile::Planner | PhaseProfile::StrategistNextGoal => {
                    Some(models.planner.as_str())
                }
                PhaseProfile::ExecutorQuick
                | PhaseProfile::ExecutorDeep
                | PhaseProfile::Summarizer => Some(models.executor.as_str()),
                PhaseProfile::PlanCritic
                | PhaseProfile::ReviewerTask
                | PhaseProfile::ReviewerFinal => Some(models.reviewer.as_str()),
                PhaseProfile::Orchestrator => None,
            };
            let Some(model) = model else {
                profile.source = PhaseRouteSource::BuiltIn;
                continue;
            };
            profile.candidates = vec![PhaseRouteCandidate {
                backend: PhaseBackend::Worker(WorkerKind::OpencodeSession),
                model: PhaseModelBinding::BackendDeclared(model.to_string()),
                command: None,
            }];
            profile.source = PhaseRouteSource::BuiltIn;
        }
        table.validate()?;
        Ok(table)
    }

    pub fn legacy_defaults() -> Self {
        let profile = |phase,
                       category,
                       backend,
                       reasoning_tier,
                       context_tier,
                       can_write,
                       can_review,
                       independence_group: &str| {
            let model = match backend {
                PhaseBackend::DirectModel | PhaseBackend::NativeZed => {
                    PhaseModelBinding::CurrentSession
                }
                PhaseBackend::Deterministic
                | PhaseBackend::Worker(_)
                | PhaseBackend::LegacyCategory => PhaseModelBinding::None,
            };
            PhaseRouteProfile {
                phase,
                category,
                candidates: vec![PhaseRouteCandidate {
                    backend,
                    model,
                    command: None,
                }],
                source: PhaseRouteSource::LegacyDefault,
                reasoning_tier,
                context_tier,
                can_write,
                can_review,
                cache_supported: false,
                latency_tier: LatencyTier::Interactive,
                independence_group: independence_group.to_string(),
                max_calls_per_epoch: 1,
                max_tokens_per_call: None,
                max_cost_micros_per_epoch: None,
            }
        };
        Self {
            schema_version: PHASE_ROUTE_SCHEMA_VERSION,
            profiles: vec![
                profile(
                    PhaseProfile::Planner,
                    WorkerCategory::Deep,
                    PhaseBackend::DirectModel,
                    ReasoningTier::High,
                    ContextTier::Goal,
                    false,
                    false,
                    "planning",
                ),
                profile(
                    PhaseProfile::PlanCritic,
                    WorkerCategory::Review,
                    PhaseBackend::DirectModel,
                    ReasoningTier::High,
                    ContextTier::Plan,
                    false,
                    true,
                    "plan_review",
                ),
                profile(
                    PhaseProfile::Orchestrator,
                    WorkerCategory::Custom,
                    PhaseBackend::Deterministic,
                    ReasoningTier::Deterministic,
                    ContextTier::Minimal,
                    false,
                    false,
                    "orchestrator",
                ),
                profile(
                    PhaseProfile::ExecutorQuick,
                    WorkerCategory::Quick,
                    PhaseBackend::LegacyCategory,
                    ReasoningTier::Low,
                    ContextTier::Task,
                    true,
                    false,
                    "execution",
                ),
                profile(
                    PhaseProfile::ExecutorDeep,
                    WorkerCategory::Deep,
                    PhaseBackend::LegacyCategory,
                    ReasoningTier::Medium,
                    ContextTier::Task,
                    true,
                    false,
                    "execution",
                ),
                profile(
                    PhaseProfile::ReviewerTask,
                    WorkerCategory::Review,
                    PhaseBackend::LegacyCategory,
                    ReasoningTier::High,
                    ContextTier::Task,
                    false,
                    true,
                    "task_review",
                ),
                profile(
                    PhaseProfile::ReviewerFinal,
                    WorkerCategory::Review,
                    PhaseBackend::LegacyCategory,
                    ReasoningTier::High,
                    ContextTier::Goal,
                    false,
                    true,
                    "final_review",
                ),
                profile(
                    PhaseProfile::StrategistNextGoal,
                    WorkerCategory::Deep,
                    PhaseBackend::DirectModel,
                    ReasoningTier::High,
                    ContextTier::Goal,
                    false,
                    true,
                    "strategy",
                ),
                profile(
                    PhaseProfile::Summarizer,
                    WorkerCategory::Quick,
                    PhaseBackend::DirectModel,
                    ReasoningTier::Low,
                    ContextTier::Plan,
                    false,
                    false,
                    "summarization",
                ),
            ],
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PHASE_ROUTE_SCHEMA_VERSION {
            bail!(
                "unsupported phase route schema version {}",
                self.schema_version
            );
        }
        let mut phases = Vec::new();
        for profile in &self.profiles {
            if phases.contains(&profile.phase) {
                bail!("duplicate phase route profile for {:?}", profile.phase);
            }
            phases.push(profile.phase.clone());
            profile.validate()?;
        }
        if phases.len() != ALL_PHASE_PROFILES.len() {
            bail!(
                "phase route table must define exactly {} profiles, found {}",
                ALL_PHASE_PROFILES.len(),
                phases.len()
            );
        }
        for required in ALL_PHASE_PROFILES {
            if !phases.contains(required) {
                bail!("missing phase route profile for {required:?}");
            }
        }
        Ok(())
    }

    pub fn hash(&self) -> Result<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    pub fn profile(&self, phase: &PhaseProfile) -> Result<&PhaseRouteProfile> {
        self.validate()?;
        self.profiles
            .iter()
            .find(|profile| &profile.phase == phase)
            .ok_or_else(|| anyhow::anyhow!("missing phase route profile for {phase:?}"))
    }

    pub fn resolve(
        &self,
        phase: &PhaseProfile,
        inventory: &LiveModelInventory,
        current_model: Option<&ModelSelectorId>,
    ) -> Result<PhaseRouteDecision> {
        self.resolve_inner(phase, inventory, current_model, None)
    }

    pub fn resolve_for_worker(
        &self,
        phase: &PhaseProfile,
        inventory: &LiveModelInventory,
        current_model: Option<&ModelSelectorId>,
        worker_config: &WorkerConfig,
    ) -> Result<PhaseRouteDecision> {
        self.resolve_inner(phase, inventory, current_model, Some(worker_config))
    }

    fn resolve_inner(
        &self,
        phase: &PhaseProfile,
        inventory: &LiveModelInventory,
        current_model: Option<&ModelSelectorId>,
        worker_config: Option<&WorkerConfig>,
    ) -> Result<PhaseRouteDecision> {
        inventory.validate()?;
        if let Some(current_model) = current_model {
            current_model.validate()?;
        }
        let profile = self.profile(phase)?;
        let profile_hash = profile.hash()?;
        let mut rejected_candidates = Vec::new();
        for (candidate_index, candidate) in profile.candidates.iter().enumerate() {
            match candidate.resolve(inventory, current_model) {
                Ok(requested_model) => {
                    let worker_kind = match candidate.backend {
                        PhaseBackend::NativeZed => Some(WorkerKind::ZedAgent),
                        PhaseBackend::Worker(worker_kind) => Some(worker_kind),
                        PhaseBackend::Deterministic
                        | PhaseBackend::DirectModel
                        | PhaseBackend::LegacyCategory => None,
                    };
                    let decision = PhaseRouteDecision {
                        phase: phase.clone(),
                        category: profile.category,
                        selected_candidate: candidate_index,
                        candidate: candidate.clone(),
                        rejected_candidates: rejected_candidates.clone(),
                        requested_model,
                        worker_kind,
                        profile_hash: profile_hash.clone(),
                        source: profile.source.clone(),
                    };
                    if let Some(worker_config) = worker_config {
                        if let Err(error) = decision.overlay_worker_config(worker_config) {
                            rejected_candidates.push(RejectedPhaseCandidate {
                                candidate_index,
                                reason: error.to_string(),
                            });
                            continue;
                        }
                    }
                    return Ok(decision);
                }
                Err(error) => rejected_candidates.push(RejectedPhaseCandidate {
                    candidate_index,
                    reason: error.to_string(),
                }),
            }
        }
        bail!(
            "no usable candidate for phase {phase:?}: {}",
            rejected_candidates
                .iter()
                .map(|rejected| format!(
                    "candidate {}: {}",
                    rejected.candidate_index, rejected.reason
                ))
                .collect::<Vec<_>>()
                .join("; ")
        )
    }
}

impl PhaseRouteProfile {
    pub fn validate(&self) -> Result<()> {
        if self.candidates.is_empty() {
            bail!("phase {:?} must define at least one candidate", self.phase);
        }
        if self.independence_group.trim().is_empty() || self.max_calls_per_epoch == 0 {
            bail!(
                "phase {:?} must define an independence group and positive call budget",
                self.phase
            );
        }
        if matches!(
            self.phase,
            PhaseProfile::PlanCritic | PhaseProfile::ReviewerTask | PhaseProfile::ReviewerFinal
        ) && (self.can_write || !self.can_review)
        {
            bail!(
                "review phase {:?} must be read-only and review-capable",
                self.phase
            );
        }
        if matches!(
            self.phase,
            PhaseProfile::ExecutorQuick | PhaseProfile::ExecutorDeep
        ) && !self.can_write
        {
            bail!("executor phase {:?} must allow writes", self.phase);
        }
        for (index, candidate) in self.candidates.iter().enumerate() {
            if self.candidates[..index].contains(candidate) {
                bail!(
                    "phase {:?} contains duplicate route candidate at index {index}",
                    self.phase
                );
            }
            candidate.validate()?;
        }
        Ok(())
    }

    pub fn hash(&self) -> Result<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

impl PhaseRouteCandidate {
    fn validate(&self) -> Result<()> {
        if let Some(command) = self.command.as_deref() {
            if command.trim().is_empty() {
                bail!("phase worker command cannot be empty");
            }
            if !matches!(self.backend, PhaseBackend::Worker(_)) {
                bail!("only a command worker phase may define a worker command");
            }
        }
        match &self.model {
            PhaseModelBinding::ExactLive(model) => model.validate()?,
            PhaseModelBinding::BackendDeclared(model)
                if model.trim().is_empty() || model.trim() != model =>
            {
                bail!("backend-declared phase model must be non-empty and trimmed");
            }
            _ => {}
        }
        match (&self.backend, &self.model) {
            (PhaseBackend::Deterministic, PhaseModelBinding::None)
            | (PhaseBackend::LegacyCategory, PhaseModelBinding::None)
            | (PhaseBackend::DirectModel, PhaseModelBinding::CurrentSession)
            | (PhaseBackend::DirectModel, PhaseModelBinding::ExactLive(_))
            | (PhaseBackend::NativeZed, PhaseModelBinding::CurrentSession)
            | (PhaseBackend::NativeZed, PhaseModelBinding::ExactLive(_)) => Ok(()),
            (PhaseBackend::Worker(worker_kind), PhaseModelBinding::None)
            | (PhaseBackend::Worker(worker_kind), PhaseModelBinding::BackendDeclared(_))
                if *worker_kind != WorkerKind::ZedAgent =>
            {
                Ok(())
            }
            _ => bail!(
                "phase backend {:?} cannot use model binding {:?}",
                self.backend,
                self.model
            ),
        }
    }

    fn resolve(
        &self,
        inventory: &LiveModelInventory,
        current_model: Option<&ModelSelectorId>,
    ) -> Result<Option<ModelSelectorId>> {
        self.validate()?;
        match &self.model {
            PhaseModelBinding::None => Ok(None),
            PhaseModelBinding::BackendDeclared(model) => {
                if model.trim().is_empty() {
                    bail!("backend-declared phase model cannot be empty");
                }
                Ok(None)
            }
            PhaseModelBinding::CurrentSession => current_model
                .cloned()
                .map(Some)
                .ok_or_else(|| anyhow::anyhow!("current phase model is unavailable")),
            PhaseModelBinding::ExactLive(model) if inventory.contains(model) => {
                Ok(Some(model.clone()))
            }
            PhaseModelBinding::ExactLive(model) => bail!(
                "configured live model `{}` for agent `{}` is unavailable",
                model.qualified_model_id(),
                model.agent_id
            ),
        }
    }
}

impl PhaseRouteDecision {
    fn validate_shape(&self) -> Result<()> {
        self.candidate.validate()?;
        if self.profile_hash.len() != 64
            || !self
                .profile_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("phase route decision has an invalid profile hash");
        }
        if self.rejected_candidates.len() != self.selected_candidate
            || self
                .rejected_candidates
                .iter()
                .enumerate()
                .any(|(index, rejected)| {
                    rejected.candidate_index != index || rejected.reason.trim().is_empty()
                })
        {
            bail!("phase route decision has an invalid explicit fallback trail");
        }
        let expected_worker_kind = match self.candidate.backend {
            PhaseBackend::NativeZed => Some(WorkerKind::ZedAgent),
            PhaseBackend::Worker(worker_kind) => Some(worker_kind),
            PhaseBackend::Deterministic
            | PhaseBackend::DirectModel
            | PhaseBackend::LegacyCategory => None,
        };
        if self.worker_kind != expected_worker_kind {
            bail!("phase route decision worker kind does not match its backend");
        }
        if let Some(requested_model) = self.requested_model.as_ref() {
            requested_model.validate()?;
        }
        match &self.candidate.model {
            PhaseModelBinding::CurrentSession => {
                if self.requested_model.is_none() {
                    bail!("phase route decision is missing its current session model");
                }
            }
            PhaseModelBinding::ExactLive(expected_model) => {
                if self.requested_model.as_ref() != Some(expected_model) {
                    bail!("phase route decision did not resolve its exact live model");
                }
            }
            PhaseModelBinding::None | PhaseModelBinding::BackendDeclared(_) => {
                if self.requested_model.is_some() {
                    bail!("unverified phase model cannot be recorded as a resolved live model");
                }
            }
        }
        Ok(())
    }

    pub fn validate_against(&self, profile: &PhaseRouteProfile) -> Result<()> {
        self.validate_shape()?;
        profile.validate()?;
        if self.phase != profile.phase
            || self.category != profile.category
            || self.source != profile.source
        {
            bail!("phase route decision does not match its source profile");
        }
        if self.profile_hash != profile.hash()? {
            bail!("phase route decision profile hash does not match its source profile");
        }
        let candidate = profile
            .candidates
            .get(self.selected_candidate)
            .ok_or_else(|| {
                anyhow::anyhow!("phase route decision selected candidate is out of range")
            })?;
        if candidate != &self.candidate {
            bail!("phase route decision candidate does not match its source profile");
        }
        Ok(())
    }

    pub fn hash(&self) -> Result<String> {
        self.validate_shape()?;
        let bytes = serde_json::to_vec(self)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    pub fn overlay_worker_config(&self, base: &WorkerConfig) -> Result<WorkerConfig> {
        let mut overlay = base.clone();
        match self.candidate.backend {
            PhaseBackend::LegacyCategory => return Ok(overlay),
            PhaseBackend::NativeZed => {
                overlay.worker_kind = WorkerKind::ZedAgent;
                overlay.worker_command = None;
                overlay.worker_model = self
                    .requested_model
                    .as_ref()
                    .map(ModelSelectorId::qualified_model_id);
                overlay.worker_routes.clear();
                overlay.require_worker = true;
                overlay.default_worker_for_small_tasks = WorkerKind::ZedAgent;
            }
            PhaseBackend::Worker(worker_kind) => {
                let declared_model = match &self.candidate.model {
                    PhaseModelBinding::BackendDeclared(model) => Some(model.trim().to_string()),
                    PhaseModelBinding::None => None,
                    _ => bail!("worker phase cannot apply a live model binding"),
                };
                let configured_route = base
                    .worker_routes
                    .iter()
                    .find(|route| route.worker_kind == worker_kind);
                let worker_command = self
                    .candidate
                    .command
                    .clone()
                    .or_else(|| configured_route.and_then(|route| route.worker_command.clone()))
                    .or_else(|| {
                        (base.worker_kind == worker_kind)
                            .then(|| base.worker_command.clone())
                            .flatten()
                    })
                    .or_else(|| worker_kind.default_command(declared_model.as_deref()));
                let worker_command = worker_command
                    .map(|command| command.trim().to_string())
                    .filter(|command| !command.is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "phase {:?} worker `{}` has no configured command",
                            self.phase,
                            worker_kind.as_str()
                        )
                    })?;
                if worker_model_is_unavailable(
                    worker_kind,
                    declared_model.as_deref(),
                    &base.unavailable_worker_models,
                ) {
                    bail!(
                        "phase {:?} worker `{}` declared model `{}` is unavailable",
                        self.phase,
                        worker_kind.as_str(),
                        declared_model.as_deref().unwrap_or("none")
                    );
                }
                overlay.worker_kind = worker_kind;
                overlay.worker_command = Some(worker_command.clone());
                overlay.worker_model = declared_model.clone();
                overlay.worker_routes = vec![WorkerRoute {
                    worker_kind,
                    worker_command: Some(worker_command),
                    worker_model: declared_model,
                }];
                overlay.require_worker = true;
                overlay.default_worker_for_small_tasks = worker_kind;
            }
            PhaseBackend::Deterministic | PhaseBackend::DirectModel => {
                bail!(
                    "phase {:?} does not dispatch a programming worker",
                    self.phase
                )
            }
        }
        Ok(overlay)
    }
}

impl PhaseRouteReceipt {
    pub fn seal(mut self) -> Result<Self> {
        self.receipt_hash.clear();
        self.validate_payload()?;
        self.receipt_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        if self.receipt_hash.len() != 64
            || !self
                .receipt_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("phase route receipt has an invalid receipt hash");
        }
        if self.receipt_hash != self.expected_hash()? {
            bail!("phase route receipt integrity hash mismatch");
        }
        Ok(())
    }

    fn validate_payload(&self) -> Result<()> {
        self.decision.validate_shape()?;
        if self.ordinal == 0 {
            bail!("phase route receipt ordinal must be positive");
        }
        if self.decision_hash.len() != 64
            || !self
                .decision_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.decision_hash != self.decision.hash()?
        {
            bail!("phase route receipt decision hash mismatch");
        }
        for (label, value) in [
            ("goal", self.goal_id.as_deref()),
            ("plan", self.plan_id.as_deref()),
            ("plan hash", self.plan_hash.as_deref()),
            ("task", self.task_id.as_deref()),
            ("worker session", self.worker_session_id.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                bail!("phase route receipt {label} id cannot be empty");
            }
        }
        if self.plan_id.is_some() != self.plan_hash.is_some() {
            bail!("phase route receipt plan id and hash must be recorded together");
        }
        if let Some(plan_hash) = self.plan_hash.as_deref() {
            if self.plan_revision == 0
                || plan_hash.len() != 64
                || !plan_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                bail!("phase route receipt has an invalid plan revision or hash");
            }
        } else if self.plan_revision != 0 {
            bail!("phase route receipt cannot record a revision without a plan");
        }
        if let Some(applied_model) = self.applied_model.as_ref() {
            applied_model.validate()?;
        }
        for (label, value) in [
            ("actual worker model", self.actual_worker_model.as_deref()),
            ("actual route reason", self.actual_route_reason.as_deref()),
            ("task record path", self.task_record_path.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                bail!("phase route receipt {label} cannot be empty");
            }
        }
        if self.task_id.is_some() {
            if self.actual_worker_kind.is_none()
                || self.actual_category.is_none()
                || self.actual_route_reason.is_none()
                || self.task_record_path.is_none()
                || self.task_record_sha256.is_none()
            {
                bail!("worker phase receipt must bind its actual route and task record");
            }
            let task_record_sha256 = self.task_record_sha256.as_deref().unwrap_or_default();
            if task_record_sha256.len() != 64
                || !task_record_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                bail!("worker phase receipt has an invalid task record hash");
            }
            if let Some(expected_worker_kind) = self.decision.worker_kind
                && self.actual_worker_kind != Some(expected_worker_kind)
            {
                bail!("worker phase receipt actual worker violates its route decision");
            }
            if self.decision.candidate.backend != PhaseBackend::LegacyCategory
                && self.actual_category != Some(self.decision.category)
            {
                bail!("worker phase receipt actual category violates its route decision");
            }
        } else if self.actual_worker_kind.is_some()
            || self.actual_category.is_some()
            || self.actual_worker_model.is_some()
            || self.actual_route_reason.is_some()
            || self.task_record_path.is_some()
            || self.task_record_sha256.is_some()
        {
            bail!("non-worker phase receipt cannot claim an actual worker route");
        }

        match (
            &self.decision.candidate.backend,
            &self.decision.candidate.model,
        ) {
            (PhaseBackend::Deterministic, PhaseModelBinding::None) => {
                self.require_status(ModelBindingStatus::Deterministic)?;
                self.require_no_applied_model()?;
            }
            (PhaseBackend::LegacyCategory, PhaseModelBinding::None) => {
                self.require_status(ModelBindingStatus::LegacyUnverified)?;
                self.require_no_applied_model()?;
            }
            (PhaseBackend::DirectModel, PhaseModelBinding::CurrentSession) => {
                self.require_status(ModelBindingStatus::CurrentSession)?;
                self.require_applied_requested_model()?;
            }
            (PhaseBackend::DirectModel, PhaseModelBinding::ExactLive(_)) => {
                self.require_status(ModelBindingStatus::Applied)?;
                self.require_applied_requested_model()?;
            }
            (PhaseBackend::NativeZed, PhaseModelBinding::CurrentSession)
            | (PhaseBackend::NativeZed, PhaseModelBinding::ExactLive(_)) => {
                self.require_status(ModelBindingStatus::Applied)?;
                self.require_applied_requested_model()?;
                if self.worker_session_id.is_none() {
                    bail!("native Zed phase receipt must record its worker session id");
                }
                if self.actual_worker_model.as_deref()
                    != self
                        .decision
                        .requested_model
                        .as_ref()
                        .map(ModelSelectorId::qualified_model_id)
                        .as_deref()
                {
                    bail!("native Zed phase receipt actual model does not match its route");
                }
            }
            (PhaseBackend::Worker(_), PhaseModelBinding::BackendDeclared(model)) => {
                self.require_status(ModelBindingStatus::DeclaredUnverified)?;
                self.require_no_applied_model()?;
                if self.actual_worker_model.as_deref() != Some(model.as_str()) {
                    bail!("command phase receipt actual model does not match its declaration");
                }
            }
            (PhaseBackend::Worker(_), PhaseModelBinding::None) => {
                self.require_status(ModelBindingStatus::LegacyUnverified)?;
                self.require_no_applied_model()?;
                if self.actual_worker_model.is_some() {
                    bail!("unmodeled command phase receipt cannot claim an actual model");
                }
            }
            _ => bail!("phase route receipt contains an invalid backend/model binding"),
        }
        Ok(())
    }

    pub fn hash(&self) -> Result<String> {
        self.validate()?;
        Ok(self.receipt_hash.clone())
    }

    fn expected_hash(&self) -> Result<String> {
        self.validate_payload()?;
        let mut unsigned = self.clone();
        unsigned.receipt_hash.clear();
        let bytes = serde_json::to_vec(&unsigned)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    fn require_status(&self, expected: ModelBindingStatus) -> Result<()> {
        if self.binding_status != expected {
            bail!(
                "phase route receipt binding status {:?} does not match expected {:?}",
                self.binding_status,
                expected
            );
        }
        Ok(())
    }

    fn require_no_applied_model(&self) -> Result<()> {
        if self.applied_model.is_some() {
            bail!("unverified phase route receipt cannot claim an applied model");
        }
        Ok(())
    }

    fn require_applied_requested_model(&self) -> Result<()> {
        if self.applied_model.is_none()
            || self.applied_model.as_ref() != self.decision.requested_model.as_ref()
        {
            bail!("phase route receipt applied model does not match the requested model");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile_mut(
        table: &mut PhaseRouteTable,
        phase: PhaseProfile,
    ) -> Result<&mut PhaseRouteProfile> {
        table
            .profiles
            .iter_mut()
            .find(|profile| profile.phase == phase)
            .ok_or_else(|| anyhow::anyhow!("missing phase profile"))
    }

    fn live_model(qualified_id: &str) -> Result<ModelSelectorId> {
        ModelSelectorId::from_qualified("zed", qualified_id)
    }

    #[test]
    fn legacy_table_defines_every_phase_once() -> Result<()> {
        let table = PhaseRouteTable::legacy_defaults();
        table.validate()?;
        assert_eq!(table.profiles.len(), ALL_PHASE_PROFILES.len());
        for phase in [
            PhaseProfile::Planner,
            PhaseProfile::PlanCritic,
            PhaseProfile::StrategistNextGoal,
            PhaseProfile::Summarizer,
        ] {
            assert!(matches!(
                table.profile(&phase)?.candidates[0].model,
                PhaseModelBinding::CurrentSession
            ));
        }
        Ok(())
    }

    #[test]
    fn phase_table_rejects_missing_and_duplicate_profiles() {
        let mut missing = PhaseRouteTable::legacy_defaults();
        missing
            .profiles
            .retain(|profile| profile.phase != PhaseProfile::Summarizer);
        assert!(missing.validate().is_err());

        let mut duplicate = PhaseRouteTable::legacy_defaults();
        duplicate.profiles.push(duplicate.profiles[0].clone());
        assert!(duplicate.validate().is_err());
    }

    #[test]
    fn legacy_direct_model_requires_a_current_session_model() -> Result<()> {
        let table = PhaseRouteTable::legacy_defaults();
        assert!(
            table
                .resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)
                .is_err()
        );
        let current = live_model("live/planner")?;
        let decision = table.resolve(
            &PhaseProfile::Planner,
            &LiveModelInventory::default(),
            Some(&current),
        )?;
        assert_eq!(decision.requested_model, Some(current));
        Ok(())
    }

    #[test]
    fn exact_live_model_falls_back_only_to_explicit_candidate() -> Result<()> {
        let missing = ModelSelectorId::from_qualified("zed", "missing/model")?;
        let available = ModelSelectorId::from_qualified("zed", "live/model")?;
        let mut table = PhaseRouteTable::legacy_defaults();
        let profile = profile_mut(&mut table, PhaseProfile::PlanCritic)?;
        profile.source = PhaseRouteSource::Environment;
        profile.candidates = vec![
            PhaseRouteCandidate {
                backend: PhaseBackend::DirectModel,
                model: PhaseModelBinding::ExactLive(missing),
                command: None,
            },
            PhaseRouteCandidate {
                backend: PhaseBackend::DirectModel,
                model: PhaseModelBinding::ExactLive(available.clone()),
                command: None,
            },
        ];
        let source_profile = profile.clone();
        let decision = table.resolve(
            &PhaseProfile::PlanCritic,
            &LiveModelInventory {
                models: vec![available.clone()],
            },
            None,
        )?;
        assert_eq!(decision.selected_candidate, 1);
        assert_eq!(decision.requested_model, Some(available));
        assert_eq!(decision.rejected_candidates.len(), 1);
        assert_eq!(decision.rejected_candidates[0].candidate_index, 0);
        decision.validate_against(&source_profile)?;
        Ok(())
    }

    #[test]
    fn unavailable_exact_model_without_explicit_fallback_fails_closed() -> Result<()> {
        let missing = live_model("missing/model")?;
        let mut table = PhaseRouteTable::legacy_defaults();
        let profile = profile_mut(&mut table, PhaseProfile::Planner)?;
        profile.candidates = vec![PhaseRouteCandidate {
            backend: PhaseBackend::DirectModel,
            model: PhaseModelBinding::ExactLive(missing),
            command: None,
        }];
        let error = table
            .resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)
            .expect_err("missing live model must fail closed");
        assert!(error.to_string().contains("candidate 0"));
        Ok(())
    }

    #[test]
    fn native_overlay_is_scoped_and_does_not_mutate_base() -> Result<()> {
        let model = ModelSelectorId::from_qualified("zed", "live/model")?;
        let mut table = PhaseRouteTable::legacy_defaults();
        let profile = profile_mut(&mut table, PhaseProfile::ReviewerFinal)?;
        profile.candidates = vec![PhaseRouteCandidate {
            backend: PhaseBackend::NativeZed,
            model: PhaseModelBinding::ExactLive(model.clone()),
            command: None,
        }];
        let decision = table.resolve(
            &PhaseProfile::ReviewerFinal,
            &LiveModelInventory {
                models: vec![model],
            },
            None,
        )?;
        let base = WorkerConfig::default();
        let overlay = decision.overlay_worker_config(&base)?;
        assert_eq!(base.worker_kind, WorkerKind::Opencode);
        assert_eq!(overlay.worker_kind, WorkerKind::ZedAgent);
        assert_eq!(overlay.worker_model.as_deref(), Some("live/model"));
        Ok(())
    }

    #[test]
    fn worker_resolution_falls_back_when_the_first_backend_has_no_command() -> Result<()> {
        let mut table = PhaseRouteTable::legacy_defaults();
        let profile = profile_mut(&mut table, PhaseProfile::ExecutorQuick)?;
        profile.source = PhaseRouteSource::Environment;
        profile.candidates = vec![
            PhaseRouteCandidate {
                backend: PhaseBackend::Worker(WorkerKind::Opencode),
                model: PhaseModelBinding::None,
                command: None,
            },
            PhaseRouteCandidate {
                backend: PhaseBackend::Worker(WorkerKind::Codex),
                model: PhaseModelBinding::BackendDeclared("gpt-test".to_string()),
                command: None,
            },
        ];
        let base = WorkerConfig::default();
        let decision = table.resolve_for_worker(
            &PhaseProfile::ExecutorQuick,
            &LiveModelInventory::default(),
            None,
            &base,
        )?;
        assert_eq!(decision.selected_candidate, 1);
        assert_eq!(decision.rejected_candidates.len(), 1);
        let overlay = decision.overlay_worker_config(&base)?;
        assert_eq!(overlay.worker_kind, WorkerKind::Codex);
        assert_eq!(overlay.worker_model.as_deref(), Some("gpt-test"));
        assert_eq!(overlay.worker_routes.len(), 1);
        assert_eq!(base.worker_kind, WorkerKind::Opencode);
        assert!(base.worker_routes.is_empty());
        Ok(())
    }

    #[test]
    fn backend_declared_model_is_unverified_and_honors_unavailable_list() -> Result<()> {
        let mut table = PhaseRouteTable::legacy_defaults();
        let profile = profile_mut(&mut table, PhaseProfile::ExecutorDeep)?;
        profile.candidates = vec![
            PhaseRouteCandidate {
                backend: PhaseBackend::Worker(WorkerKind::Codex),
                model: PhaseModelBinding::BackendDeclared("gpt-test".to_string()),
                command: None,
            },
            PhaseRouteCandidate {
                backend: PhaseBackend::Worker(WorkerKind::Claude),
                model: PhaseModelBinding::None,
                command: None,
            },
        ];
        let mut base = WorkerConfig::default();
        base.unavailable_worker_models = vec!["openai/gpt-test".to_string()];
        let decision = table.resolve_for_worker(
            &PhaseProfile::ExecutorDeep,
            &LiveModelInventory::default(),
            None,
            &base,
        )?;
        assert_eq!(decision.selected_candidate, 1);
        assert!(decision.requested_model.is_none());
        assert!(
            decision.rejected_candidates[0]
                .reason
                .contains("unavailable")
        );
        Ok(())
    }

    #[test]
    fn exact_live_binding_rejects_command_backend() -> Result<()> {
        let model = ModelSelectorId::from_qualified("zed", "live/model")?;
        let candidate = PhaseRouteCandidate {
            backend: PhaseBackend::Worker(WorkerKind::Codex),
            model: PhaseModelBinding::ExactLive(model),
            command: Some("codex exec".to_string()),
        };
        assert!(candidate.validate().is_err());
        Ok(())
    }

    #[test]
    fn invalid_model_and_backend_declarations_are_rejected() {
        let invalid_exact = PhaseRouteCandidate {
            backend: PhaseBackend::DirectModel,
            model: PhaseModelBinding::ExactLive(ModelSelectorId {
                agent_id: String::new(),
                provider_id: "live".to_string(),
                model_id: "model".to_string(),
            }),
            command: None,
        };
        assert!(invalid_exact.validate().is_err());

        let empty_declared = PhaseRouteCandidate {
            backend: PhaseBackend::Worker(WorkerKind::Codex),
            model: PhaseModelBinding::BackendDeclared("  ".to_string()),
            command: None,
        };
        assert!(empty_declared.validate().is_err());

        let deterministic_command = PhaseRouteCandidate {
            backend: PhaseBackend::Deterministic,
            model: PhaseModelBinding::None,
            command: Some("ignored".to_string()),
        };
        assert!(deterministic_command.validate().is_err());
    }

    #[test]
    fn live_inventory_rejects_duplicate_models() -> Result<()> {
        let model = live_model("live/model")?;
        let inventory = LiveModelInventory {
            models: vec![model.clone(), model],
        };
        assert!(inventory.validate().is_err());
        Ok(())
    }

    #[test]
    fn hashes_are_stable_and_reject_invalid_profiles() -> Result<()> {
        let table = PhaseRouteTable::legacy_defaults();
        assert_eq!(table.hash()?, table.hash()?);

        let mut changed = table.clone();
        profile_mut(&mut changed, PhaseProfile::Planner)?.max_calls_per_epoch = 2;
        assert_ne!(table.hash()?, changed.hash()?);

        let mut invalid = table;
        profile_mut(&mut invalid, PhaseProfile::Planner)?
            .candidates
            .clear();
        assert!(invalid.hash().is_err());
        Ok(())
    }

    #[test]
    fn exact_direct_model_receipt_requires_the_applied_model() -> Result<()> {
        let model = live_model("live/planner")?;
        let mut table = PhaseRouteTable::legacy_defaults();
        let profile = profile_mut(&mut table, PhaseProfile::Planner)?;
        profile.candidates = vec![PhaseRouteCandidate {
            backend: PhaseBackend::DirectModel,
            model: PhaseModelBinding::ExactLive(model.clone()),
            command: None,
        }];
        let decision = table.resolve(
            &PhaseProfile::Planner,
            &LiveModelInventory {
                models: vec![model.clone()],
            },
            None,
        )?;
        let decision_hash = decision.hash()?;
        let mut receipt = PhaseRouteReceipt {
            decision,
            ordinal: 11,
            plan_revision: 1,
            decision_hash,
            goal_id: Some("goal_test".to_string()),
            plan_id: Some("plan_test".to_string()),
            plan_hash: Some("0".repeat(64)),
            task_id: None,
            worker_session_id: None,
            applied_model: Some(model),
            actual_worker_kind: None,
            actual_category: None,
            actual_worker_model: None,
            actual_route_reason: None,
            task_record_path: None,
            task_record_sha256: None,
            binding_status: ModelBindingStatus::Applied,
            receipt_hash: String::new(),
        }
        .seal()?;
        assert_eq!(receipt.hash()?.len(), 64);
        receipt.applied_model = Some(live_model("live/other")?);
        assert!(receipt.validate().is_err());
        Ok(())
    }

    #[test]
    fn native_receipt_requires_a_session_and_exact_model_match() -> Result<()> {
        let model = live_model("live/reviewer")?;
        let mut table = PhaseRouteTable::legacy_defaults();
        let profile = profile_mut(&mut table, PhaseProfile::ReviewerFinal)?;
        profile.candidates = vec![PhaseRouteCandidate {
            backend: PhaseBackend::NativeZed,
            model: PhaseModelBinding::ExactLive(model.clone()),
            command: None,
        }];
        let decision = table.resolve(
            &PhaseProfile::ReviewerFinal,
            &LiveModelInventory {
                models: vec![model.clone()],
            },
            None,
        )?;
        let decision_hash = decision.hash()?;
        let mut receipt = PhaseRouteReceipt {
            decision,
            ordinal: 12,
            plan_revision: 0,
            decision_hash,
            goal_id: Some("goal_test".to_string()),
            plan_id: None,
            plan_hash: None,
            task_id: Some("task_review".to_string()),
            worker_session_id: None,
            applied_model: Some(model),
            actual_worker_kind: Some(WorkerKind::ZedAgent),
            actual_category: Some(WorkerCategory::Review),
            actual_worker_model: Some("live/reviewer".to_string()),
            actual_route_reason: Some("review route".to_string()),
            task_record_path: Some("task-record.json".to_string()),
            task_record_sha256: Some("0".repeat(64)),
            binding_status: ModelBindingStatus::Applied,
            receipt_hash: String::new(),
        };
        assert!(receipt.clone().seal().is_err());
        receipt.worker_session_id = Some("session_review".to_string());
        receipt.seal()?.validate()?;
        Ok(())
    }

    #[test]
    fn backend_declared_receipt_cannot_claim_an_applied_model() -> Result<()> {
        let mut table = PhaseRouteTable::legacy_defaults();
        let profile = profile_mut(&mut table, PhaseProfile::ExecutorDeep)?;
        profile.candidates = vec![PhaseRouteCandidate {
            backend: PhaseBackend::Worker(WorkerKind::Codex),
            model: PhaseModelBinding::BackendDeclared("gpt-test".to_string()),
            command: Some("codex exec".to_string()),
        }];
        let decision = table.resolve_for_worker(
            &PhaseProfile::ExecutorDeep,
            &LiveModelInventory::default(),
            None,
            &WorkerConfig::default(),
        )?;
        let decision_hash = decision.hash()?;
        let mut receipt = PhaseRouteReceipt {
            decision,
            ordinal: 103,
            plan_revision: 0,
            decision_hash,
            goal_id: Some("goal_test".to_string()),
            plan_id: None,
            plan_hash: None,
            task_id: Some("task_exec".to_string()),
            worker_session_id: None,
            applied_model: None,
            actual_worker_kind: Some(WorkerKind::Codex),
            actual_category: Some(WorkerCategory::Deep),
            actual_worker_model: Some("gpt-test".to_string()),
            actual_route_reason: Some("deep route".to_string()),
            task_record_path: Some("task-record.json".to_string()),
            task_record_sha256: Some("0".repeat(64)),
            binding_status: ModelBindingStatus::DeclaredUnverified,
            receipt_hash: String::new(),
        }
        .seal()?;
        receipt.applied_model = Some(live_model("live/model")?);
        assert!(receipt.validate().is_err());
        Ok(())
    }

    #[test]
    fn legacy_receipt_allows_an_explicit_task_manager_category_fallback() -> Result<()> {
        let table = PhaseRouteTable::legacy_defaults();
        let decision = table.resolve_for_worker(
            &PhaseProfile::ExecutorQuick,
            &LiveModelInventory::default(),
            None,
            &WorkerConfig::default(),
        )?;
        let decision_hash = decision.hash()?;
        let receipt = PhaseRouteReceipt {
            decision,
            ordinal: 101,
            plan_revision: 0,
            decision_hash,
            goal_id: Some("goal_test".to_string()),
            plan_id: None,
            plan_hash: None,
            task_id: Some("task_exec".to_string()),
            worker_session_id: None,
            applied_model: None,
            actual_worker_kind: Some(WorkerKind::Codex),
            actual_category: Some(WorkerCategory::Deep),
            actual_worker_model: None,
            actual_route_reason: Some("legacy fallback".to_string()),
            task_record_path: Some("task-record.json".to_string()),
            task_record_sha256: Some("0".repeat(64)),
            binding_status: ModelBindingStatus::LegacyUnverified,
            receipt_hash: String::new(),
        }
        .seal()?;

        receipt.validate()?;
        Ok(())
    }

    #[test]
    fn opencode_only_routes_every_model_phase_to_resident_sessions() -> Result<()> {
        let table = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;

        for (phase, expected_model) in [
            (PhaseProfile::Planner, "openai/gpt-planner"),
            (PhaseProfile::PlanCritic, "openai/gpt-reviewer"),
            (PhaseProfile::ExecutorQuick, "deepseek/flash"),
            (PhaseProfile::ExecutorDeep, "deepseek/flash"),
            (PhaseProfile::ReviewerTask, "openai/gpt-reviewer"),
            (PhaseProfile::ReviewerFinal, "openai/gpt-reviewer"),
            (PhaseProfile::StrategistNextGoal, "openai/gpt-planner"),
            (PhaseProfile::Summarizer, "deepseek/flash"),
        ] {
            let decision = table.resolve(&phase, &LiveModelInventory::default(), None)?;
            assert_eq!(decision.worker_kind, Some(WorkerKind::OpencodeSession));
            assert_eq!(
                decision.candidate.model,
                PhaseModelBinding::BackendDeclared(expected_model.to_string())
            );
        }

        let orchestrator = table.resolve(
            &PhaseProfile::Orchestrator,
            &LiveModelInventory::default(),
            None,
        )?;
        assert_eq!(orchestrator.candidate.backend, PhaseBackend::Deterministic);
        Ok(())
    }

    #[test]
    fn opencode_only_requires_qualified_models() {
        let error = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "unqualified".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })
        .expect_err("an unqualified OpenCode model must be rejected");
        assert!(error.to_string().contains("planner"));
    }
}
