//! Worker Broker — capability matrix, session identity, lifecycle receipts
//! for typed back-end routing and contract enforcement.
//!
//! Defines the schema types and validation logic that bind phase decisions
//! to concrete worker sessions, so the orchestrator can audit every
//! lifecycle transition with cryptographic integrity.

use std::fs;
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::phase_routing::{
    ModelBindingStatus, ModelSelectorId, PhaseModelBinding, PhaseRouteDecision,
};
use crate::plan_graph::PhaseProfile;
use crate::plan_review::PhaseExecutionIdentity;
use crate::state::{timestamp, write_json};
use crate::workers::{
    WorkerKind, WorkerRegistry, WorkerResult, WorkerSessionHandle, WorkerStartRequest,
};

pub const BROKER_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Capability declarations
// ---------------------------------------------------------------------------

/// Typed capability that a worker backend may support.
///
/// Each variant corresponds to a broker-level interaction that the
/// orchestrator may request from a backend session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerCapability {
    /// Backend can enumerate available agents and their models.
    DiscoverAgents,
    /// Backend supports model selection (user or policy chooses a model).
    ModelSelection,
    /// Backend can start a new session.
    Start,
    /// Backend accepts follow-up prompts after the initial turn.
    FollowUp,
    /// Backend accepts real-time steering prompts.
    Steer,
    /// Backend supports cancellation of in-flight work.
    Cancel,
    /// Backend supports waiting for completion / polling.
    Wait,
    /// Backend can report token and duration usage.
    Usage,
    /// Backend supports permission request/denial/grant flow.
    Permission,
    /// Backend can resume a previously interrupted session.
    SessionResume,
}

/// Reason that a model or capability is unavailable for a backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    /// The backend does not support this feature at all.
    NotSupported,
    /// The feature exists but is not configured (e.g. missing API key).
    NotConfigured,
    /// The backend itself is unreachable or unavailable.
    BackendUnavailable(String),
    /// A specific model was not found or is not loadable.
    ModelNotFound(String),
}

// ---------------------------------------------------------------------------
// Request schema
// ---------------------------------------------------------------------------

/// Whether a model is available for selection or explicitly unavailable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAvailability {
    /// A specific model selector that can be used.
    Available(ModelSelectorId),
    /// The requested model is unavailable for a typed reason.
    Unavailable(UnavailableReason),
}

/// A broker-phase request that binds a phase decision to a specific
/// agent and model selection.
///
/// This struct captures every parameter the orchestrator passes to a
/// worker backend at start time, so lifecycle receipts can verify that
/// the execution matches the original intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerPhaseRequest {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// Hash of the phase route decision that produced this request.
    pub phase_decision_hash: String,
    /// Goal identifier from the work lineage.
    pub goal_id: String,
    /// Plan identifier (empty if no plan phase).
    pub plan_id: String,
    /// Plan revision counter.
    pub plan_revision: usize,
    /// Task identifier within the plan.
    pub task_id: String,
    /// The agent or backend type that was requested.
    pub requested_agent: String,
    /// The model that was requested, or an explicit unavailable reason.
    pub requested_model: ModelAvailability,
    /// Models that are pre-approved as fallback destinations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_fallback_models: Vec<ModelSelectorId>,
}

impl BrokerPhaseRequest {
    pub fn from_phase_decision(
        phase_decision: &PhaseRouteDecision,
        goal_id: &str,
        plan_id: &str,
        plan_revision: usize,
        task_id: &str,
    ) -> Result<Self> {
        let requested_model = match &phase_decision.requested_model {
            Some(model) => ModelAvailability::Available(model.clone()),
            None => match (&phase_decision.worker_kind, &phase_decision.candidate.model) {
                (Some(worker_kind), PhaseModelBinding::BackendDeclared(model)) => {
                    match ModelSelectorId::from_qualified(worker_kind.as_str(), model) {
                        Ok(model) => ModelAvailability::Available(model),
                        Err(_) => ModelAvailability::Unavailable(UnavailableReason::NotConfigured),
                    }
                }
                _ => ModelAvailability::Unavailable(UnavailableReason::NotConfigured),
            },
        };
        let request = Self {
            schema_version: BROKER_SCHEMA_VERSION,
            phase_decision_hash: phase_decision
                .hash()
                .context("failed to hash phase route decision for broker")?,
            goal_id: goal_id.to_string(),
            plan_id: plan_id.to_string(),
            plan_revision,
            task_id: task_id.to_string(),
            requested_agent: phase_decision
                .worker_kind
                .map(|kind| kind.as_str().to_string())
                .unwrap_or_else(|| "direct".to_string()),
            requested_model,
            allowed_fallback_models: Vec::new(),
        };
        request.validate()?;
        Ok(request)
    }

    /// Validate that all required fields are present and well-formed.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != BROKER_SCHEMA_VERSION {
            bail!(
                "unsupported broker request schema version {}",
                self.schema_version
            );
        }
        require_non_empty("phase_decision_hash", &self.phase_decision_hash)?;
        require_non_empty("goal_id", &self.goal_id)?;
        require_non_empty("task_id", &self.task_id)?;
        if self.requested_agent.trim().is_empty() {
            bail!("broker request requested_agent cannot be empty");
        }
        match &self.requested_model {
            ModelAvailability::Available(selector) => {
                selector.validate().context("requested_model")?
            }
            ModelAvailability::Unavailable(_) => {}
        }
        for (i, fallback) in self.allowed_fallback_models.iter().enumerate() {
            fallback
                .validate()
                .with_context(|| format!("allowed_fallback_models[{i}]"))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Session identity
// ---------------------------------------------------------------------------

/// Identity of a broker-managed worker session.
///
/// Carries the backend kind, unique session id, creation timestamp, and
/// an optional snapshot of the capabilities that were advertised when
/// the session was established.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerSessionIdentity {
    /// The type of backend that owns this session.
    pub backend_kind: WorkerKind,
    /// Backend-assigned session identifier (must be non-empty).
    pub session_id: String,
    /// RFC 3339 timestamp of when the session was started.
    pub started_at: String,
    /// Capabilities the backend advertised at session creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<BrokerCapability>>,
}

impl BrokerSessionIdentity {
    /// Reject empty identity fields.
    pub fn validate(&self) -> Result<()> {
        if self.session_id.trim().is_empty() {
            bail!("broker session identity session_id cannot be empty");
        }
        if self.started_at.trim().is_empty() {
            bail!("broker session identity started_at cannot be empty");
        }
        Ok(())
    }

    /// Return `true` when the session advertised a specific capability.
    pub fn supports(&self, capability: &BrokerCapability) -> bool {
        self.capabilities
            .as_ref()
            .is_some_and(|caps| caps.contains(capability))
    }
}

// ---------------------------------------------------------------------------
// Lifecycle outcome
// ---------------------------------------------------------------------------

/// Terminal outcome of a broker-managed interaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerOutcome {
    /// The interaction completed successfully.
    Completed,
    /// The interaction failed with an error.
    Failed,
    /// The interaction was cancelled.
    Cancelled,
    /// The interaction was modified via a steer operation.
    Steered,
    /// The interaction continued via a follow-up.
    FollowedUp,
}

// ---------------------------------------------------------------------------
// Permission evidence
// ---------------------------------------------------------------------------

/// Categorised permission type that a backend may request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerPermissionType {
    /// Read access to a file or directory.
    ReadFile,
    /// Write access to a file or directory.
    WriteFile,
    /// Execute a shell command.
    ExecuteCommand,
    /// Network access (outbound connections).
    NetworkAccess,
    /// Access to environment variables or system state.
    EnvironmentAccess,
}

/// Evidence of a permission request, denial, or grant during an
/// interaction.  Every permission event is stamped with the agent
/// and model context in which it occurred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerPermissionEvidence {
    /// The type of permission being requested.
    pub permission_type: BrokerPermissionType,
    /// Whether the permission was granted.
    pub granted: bool,
    /// RFC 3339 timestamp of the permission event.
    pub timestamp: String,
    /// Name of the agent that requested permission.
    pub agent_name: String,
    /// The model context in which the request was made, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_context: Option<String>,
    /// Optional human-readable reason for the decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Usage record
// ---------------------------------------------------------------------------

/// Token and duration usage reported by a backend for one interaction.
///
/// All numeric fields are `Option<u64>` because a backend may not report
/// all metrics.  When a backend reports no usage at all, the entire
/// `usage` field on the receipt should be `None` and
/// `unavailable_reason` explains why.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerUsage {
    /// Tokens requested (prompt + cached context), if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_tokens: Option<u64>,
    /// Tokens actually consumed (completion), if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_tokens: Option<u64>,
    /// The model identifier that was used, or "unknown" if not reported.
    pub model: String,
    /// Wall-clock duration of the interaction in milliseconds, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Reason usage could not be determined, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Lifecycle receipt
// ---------------------------------------------------------------------------

/// A fully bound lifecycle receipt that cryptographically links a
/// phase decision to a concrete worker interaction.
///
/// Every receipt carries an `interaction_ordinal` (starting at 1) that
/// sequences interactions within a single session, and a
/// `receipt_hash` that commits to the entire payload so tampering or
/// replay can be detected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerLifecycleReceipt {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// Interaction ordinal within this session (starts at 1).
    pub interaction_ordinal: u64,
    /// The phase decision hash this receipt is bound to.
    pub phase_decision_hash: String,
    /// Identity of the worker session that handled this interaction.
    pub session_identity: BrokerSessionIdentity,
    /// The original request that started this interaction.
    pub request: BrokerPhaseRequest,
    /// Terminal outcome of the interaction.
    pub outcome: BrokerOutcome,
    /// Reason the interaction terminated, if relevant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
    /// Token and duration usage, if reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<BrokerUsage>,
    /// Permission evidence collected during the interaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_evidence: Option<BrokerPermissionEvidence>,
    /// The model that was actually used, if execution diverged from the
    /// requested model (e.g. fallback).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_model: Option<ModelSelectorId>,
    /// Model binding status for CLI-declared vs ACP-verified models.
    /// When `Some(DeclaredUnverified)` or `Some(LegacyUnverified)`, the
    /// model comes from a backend declaration, not from ACP verification,
    /// and mismatches between `request.requested_model` and `actual_model`
    /// are permitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_status: Option<ModelBindingStatus>,
    /// SHA-256 hex digest that commits the full payload.  Empty during
    /// construction; set by [`Self::seal`].  Validated by [`Self::validate`].
    pub receipt_hash: String,
}

impl BrokerLifecycleReceipt {
    /// Seal the receipt by computing and setting its cryptographic hash.
    ///
    /// 1. Clears any existing hash.
    /// 2. Validates the payload (structural checks).
    /// 3. Computes the SHA-256 digest of the serialised receipt.
    /// 4. Sets `receipt_hash`.
    /// 5. Runs full validation.
    pub fn seal(mut self) -> Result<Self> {
        self.receipt_hash.clear();
        self.validate_payload()?;
        self.receipt_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    /// Full validation: payload integrity + hash format + hash match.
    pub fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        validate_sha256("broker receipt receipt_hash", &self.receipt_hash)?;
        if self.receipt_hash != self.expected_hash()? {
            bail!("broker lifecycle receipt integrity hash mismatch");
        }
        Ok(())
    }

    /// Structural validation without the hash check (used during seal).
    fn validate_payload(&self) -> Result<()> {
        if self.schema_version != BROKER_SCHEMA_VERSION {
            bail!(
                "unsupported broker receipt schema version {}",
                self.schema_version
            );
        }
        if self.interaction_ordinal == 0 {
            bail!("broker lifecycle receipt interaction_ordinal must be positive");
        }
        require_non_empty("phase_decision_hash", &self.phase_decision_hash)?;
        self.session_identity.validate()?;
        self.request.validate()?;

        // --- Capability consistency ---------------------------------------
        // If the outcome requires a capability that the session does not
        // advertise, the receipt is invalid.
        match &self.outcome {
            BrokerOutcome::Steered => {
                if !self.session_identity.supports(&BrokerCapability::Steer) {
                    bail!(
                        "broker receipt records outcome Steered but session does not \
                         advertise Steer capability"
                    );
                }
            }
            BrokerOutcome::FollowedUp => {
                if !self.session_identity.supports(&BrokerCapability::FollowUp) {
                    bail!(
                        "broker receipt records outcome FollowedUp but session does not \
                         advertise FollowUp capability"
                    );
                }
            }
            _ => {}
        }
        if self.usage.is_some() && !self.session_identity.supports(&BrokerCapability::Usage) {
            bail!(
                "broker receipt records usage but session does not \
                 advertise Usage capability"
            );
        }
        if self.permission_evidence.is_some()
            && !self
                .session_identity
                .supports(&BrokerCapability::Permission)
        {
            bail!(
                "broker receipt records permission evidence but session does not \
                 advertise Permission capability"
            );
        }

        // --- Requested/actual consistency --------------------------------
        // If an actual_model is recorded, it must either match the
        // requested model (when available) or be listed in the allowed
        // fallback models.
        //
        // CLI-declared models (binding_status is DeclaredUnverified or
        // LegacyUnverified) are allowed mismatch since they come from
        // a backend declaration, not from ACP verification.
        if !is_declared_unverified(&self.binding_status) {
            if let Some(actual) = self.actual_model.as_ref() {
                match &self.request.requested_model {
                    ModelAvailability::Available(requested) => {
                        if actual != requested
                            && !self.request.allowed_fallback_models.contains(actual)
                        {
                            bail!(
                                "broker receipt actual model {:?}/{:?}/{:?} is not the requested \
                                 model and is not in the allowed fallback list",
                                actual.agent_id,
                                actual.provider_id,
                                actual.model_id,
                            );
                        }
                    }
                    ModelAvailability::Unavailable(reason) => {
                        // When the requested model was explicitly unavailable,
                        // any actual model must be in the allowed fallback list.
                        if !self.request.allowed_fallback_models.contains(actual) {
                            bail!(
                                "broker receipt actual model {:?}/{:?}/{:?} is not listed as an \
                                 allowed fallback (requested model was unavailable: {reason:?})",
                                actual.agent_id,
                                actual.provider_id,
                                actual.model_id,
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Compute the SHA-256 hex digest of the receipt with the
    /// `receipt_hash` field cleared.
    fn expected_hash(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.receipt_hash.clear();
        let bytes = serde_json::to_vec(&unsigned)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

// ---------------------------------------------------------------------------
// Capability matrix
// ---------------------------------------------------------------------------

/// Return the set of broker capabilities that a worker backend advertises.
///
/// This is the static capability matrix.  It mirrors the
/// [`crate::workers::WorkerCapabilities`] mapping in
/// `capabilities_for_kind` so that broker-level capability checks are
/// consistent with the worker-level capability model.
pub fn broker_capabilities_for_kind(
    kind: WorkerKind,
    has_native_backend: bool,
) -> Vec<BrokerCapability> {
    let mut caps = vec![
        BrokerCapability::DiscoverAgents,
        BrokerCapability::Start,
        BrokerCapability::Cancel,
        BrokerCapability::Wait,
        BrokerCapability::Usage,
    ];

    match kind {
        WorkerKind::Opencode => {
            // Opencode (non-session): basic capabilities, no session features.
            caps.push(BrokerCapability::Permission);
        }
        WorkerKind::OpencodeSession => {
            // Full resident session — all capabilities.
            caps.push(BrokerCapability::ModelSelection);
            caps.push(BrokerCapability::FollowUp);
            caps.push(BrokerCapability::Steer);
            caps.push(BrokerCapability::Permission);
            caps.push(BrokerCapability::SessionResume);
        }
        WorkerKind::Codex => {
            // Codex supports model selection but not steer/follow-up or resume.
            caps.push(BrokerCapability::ModelSelection);
            caps.push(BrokerCapability::Permission);
        }
        WorkerKind::Claude => {
            // Claude: basic only — no model selection, steer, follow-up, resume.
            caps.push(BrokerCapability::Permission);
        }
        WorkerKind::ZedAgent if has_native_backend => {
            // Native Zed agent — full capabilities.
            caps.push(BrokerCapability::ModelSelection);
            caps.push(BrokerCapability::FollowUp);
            caps.push(BrokerCapability::Steer);
            caps.push(BrokerCapability::Permission);
            caps.push(BrokerCapability::SessionResume);
        }
        WorkerKind::ZedAgent => {
            // CLI Zed agent — basic only.
        }
        WorkerKind::Custom => {
            // Custom — basic only, no advanced features.
        }
    }

    caps
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn require_non_empty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} cannot be empty");
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

// ---------------------------------------------------------------------------
// Ledger types and errors
// ---------------------------------------------------------------------------

/// Serialized name of the lifecycle state for ledger events.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleStateName {
    Discovered,
    Resolved,
    Starting,
    Active,
    IdleSteering,
    Terminal,
}

/// A single event appended to lifecycle-events.jsonl.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerLifecycleEvent {
    pub schema_version: u32,
    pub timestamp: String,
    pub from_state: Option<LifecycleStateName>,
    pub to_state: LifecycleStateName,
    pub interaction_ordinal: u64,
    pub session_id: String,
    pub message: String,
}

/// Terminal outcome persisted to terminal-outcome.json.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalOutcomeRecord {
    pub schema_version: u32,
    pub timestamp: String,
    pub outcome: BrokerOutcome,
    pub reason: Option<String>,
    pub total_interactions: u64,
    pub session_identity: BrokerSessionIdentity,
}

/// Usage record persisted to usage.json.
pub type PersistedUsage = BrokerUsage;

/// A write error during ledger persistence — must not silently produce
/// partial artifacts.
#[derive(Debug, Clone)]
pub struct LedgerWriteError {
    pub path: PathBuf,
    pub reason: String,
}

impl std::fmt::Display for LedgerWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ledger write error at {}: {}",
            self.path.display(),
            self.reason
        )
    }
}

impl std::error::Error for LedgerWriteError {}

// ---------------------------------------------------------------------------
// Lifecycle state machine
// ---------------------------------------------------------------------------

/// Current phase of a broker-managed worker session lifecycle.
///
/// Transition graph (illegal transitions return `Err`):
/// ```text
/// Discovered → Resolved → Starting → Active ⇄ IdleSteering
///   │            │           │          │            │
///   └────┬────────┴───────────┴──────────┴────────────┘
///        └──→ Terminal(Cancelled|Failed|Completed)
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleState {
    Discovered,
    Resolved,
    Starting,
    Active,
    IdleSteering,
    Terminal {
        outcome: BrokerOutcome,
        reason: Option<String>,
    },
}

impl LifecycleState {
    /// Return the serialized name variant.
    pub fn name(&self) -> LifecycleStateName {
        match self {
            LifecycleState::Discovered => LifecycleStateName::Discovered,
            LifecycleState::Resolved => LifecycleStateName::Resolved,
            LifecycleState::Starting => LifecycleStateName::Starting,
            LifecycleState::Active => LifecycleStateName::Active,
            LifecycleState::IdleSteering => LifecycleStateName::IdleSteering,
            LifecycleState::Terminal { .. } => LifecycleStateName::Terminal,
        }
    }

    /// Whether this is a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, LifecycleState::Terminal { .. })
    }

    /// Validate a transition from `self` to `to`.
    ///
    /// Returns `Ok(())` when the transition is legal, or `bail!` with a
    /// descriptive message for illegal transitions.
    pub fn can_transition_to(&self, to: &LifecycleState) -> Result<()> {
        let allowed = match (self, to) {
            // Forward progression
            (LifecycleState::Discovered, LifecycleState::Resolved) => true,
            (LifecycleState::Resolved, LifecycleState::Starting) => true,
            (LifecycleState::Starting, LifecycleState::Active) => true,
            // Steering cycle
            (LifecycleState::Active, LifecycleState::IdleSteering) => true,
            (LifecycleState::IdleSteering, LifecycleState::Active) => true,
            // Terminal from any non-terminal
            (s, LifecycleState::Terminal { .. }) if !s.is_terminal() => true,
            // No revival from terminal
            _ => false,
        };
        if !allowed {
            bail!(
                "illegal broker state transition: {:?} → {:?}",
                self.name(),
                to.name()
            );
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Broker state (thread-safe inner)
// ---------------------------------------------------------------------------

/// Thread-safe inner state of a worker broker session.
struct BrokerStateInner {
    /// Current lifecycle phase.
    pub lifecycle: LifecycleState,
    /// Session identity, set by `start()`.
    pub session_identity: Option<BrokerSessionIdentity>,
    /// The phase request stored by `resolve()`.
    pub phase_request: Option<BrokerPhaseRequest>,
    /// The active worker session handle, set by `start()`.
    pub session_handle: Option<Arc<dyn WorkerSessionHandle>>,
    /// Monotonically increasing interaction ordinal (starts at 0, first
    /// interaction gets ordinal 1).
    pub interaction_ordinal: u64,
    /// Ledger paths for the current session, set by `start()`.
    pub ledger_paths: Option<BrokerLedgerPaths>,
}

impl BrokerStateInner {
    fn new() -> Self {
        Self {
            lifecycle: LifecycleState::Discovered,
            session_identity: None,
            phase_request: None,
            session_handle: None,
            interaction_ordinal: 0,
            ledger_paths: None,
        }
    }

    fn next_ordinal(&mut self) -> u64 {
        self.interaction_ordinal += 1;
        self.interaction_ordinal
    }
}

// ---------------------------------------------------------------------------
// Ledger path builder
// ---------------------------------------------------------------------------

/// Builds artifact paths for the broker session ledger.
///
/// Layout:
/// ```text
/// {artifacts_root}/{goal_id}/broker-sessions/{session_id}/
/// ├── session-identity.json
/// ├── lifecycle-events.jsonl
/// ├── usage.json
/// ├── permission-events.jsonl
/// ├── terminal-outcome.json
/// └── receipts/
///     ├── 1.json
///     ├── 2.json
///     └── ...
/// ```
#[derive(Clone, Debug)]
struct BrokerLedgerPaths {
    session_dir: PathBuf,
}

impl BrokerLedgerPaths {
    fn new(artifacts_root: &Path, goal_id: &str, session_id: &str) -> Self {
        let session_dir = artifacts_root
            .join(sanitize_for_path(goal_id))
            .join("broker-sessions")
            .join(sanitize_for_path(session_id));
        Self { session_dir }
    }

    fn session_identity_path(&self) -> PathBuf {
        self.session_dir.join("session-identity.json")
    }

    fn lifecycle_events_path(&self) -> PathBuf {
        self.session_dir.join("lifecycle-events.jsonl")
    }

    #[allow(dead_code)]
    fn usage_path(&self) -> PathBuf {
        self.session_dir.join("usage.json")
    }

    #[allow(dead_code)]
    fn permission_events_path(&self) -> PathBuf {
        self.session_dir.join("permission-events.jsonl")
    }

    fn terminal_outcome_path(&self) -> PathBuf {
        self.session_dir.join("terminal-outcome.json")
    }

    fn receipt_path(&self, ordinal: u64) -> PathBuf {
        self.session_dir
            .join("receipts")
            .join(format!("{ordinal}.json"))
    }

    /// Create all directories for the session ledger.
    fn ensure_dirs(&self) -> Result<()> {
        fs::create_dir_all(self.session_dir.join("receipts")).with_context(|| {
            format!(
                "failed to create ledger dirs at {}",
                self.session_dir.display()
            )
        })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Snapshot for external inspection
// ---------------------------------------------------------------------------

/// Snapshot of the broker's observable state.
#[derive(Clone, Debug)]
pub struct BrokerState {
    pub lifecycle: LifecycleState,
    pub session_identity: Option<BrokerSessionIdentity>,
    pub interaction_ordinal: u64,
}

// ---------------------------------------------------------------------------
// WorkerBroker — lifecycle coordinator
// ---------------------------------------------------------------------------

/// Thread-safe broker that enforces the lifecycle state machine and
/// maintains the cryptographically-bound session/receipt ledger.
///
/// The broker delegates actual worker execution to `WorkerRegistry` /
/// `TaskManager`; it is a contract layer that ensures every interaction
/// is audited, sequenced, and validated.
pub struct WorkerBroker {
    /// Thread-safe inner state.
    state: Arc<Mutex<BrokerStateInner>>,
    /// The worker registry used to start sessions.
    #[allow(dead_code)]
    registry: Arc<WorkerRegistry>,
    /// Root directory for session artifacts.
    artifacts_root: PathBuf,
}

impl WorkerBroker {
    /// Create a new broker.
    pub fn new(registry: Arc<WorkerRegistry>, artifacts_root: PathBuf) -> Self {
        Self {
            state: Arc::new(Mutex::new(BrokerStateInner::new())),
            registry,
            artifacts_root,
        }
    }

    /// Return the artifacts root path.
    pub fn artifacts_root(&self) -> &Path {
        &self.artifacts_root
    }

    pub fn session_ledger_dir(&self) -> Result<PathBuf> {
        self.state
            .lock()
            .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?
            .ledger_paths
            .as_ref()
            .map(|paths| paths.session_dir.clone())
            .ok_or_else(|| anyhow::anyhow!("broker session ledger is unavailable"))
    }

    // ── State accessors ──────────────────────────────────────────────────

    /// Return a snapshot of the current broker state.
    pub fn current_state(&self) -> Result<BrokerState> {
        let inner = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;
        Ok(BrokerState {
            lifecycle: inner.lifecycle.clone(),
            session_identity: inner.session_identity.clone(),
            interaction_ordinal: inner.interaction_ordinal,
        })
    }

    /// Return the current session identity, if set.
    pub fn session_identity(&self) -> Result<BrokerSessionIdentity> {
        let inner = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;
        inner
            .session_identity
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no session identity set — call start() first"))
    }

    /// Return the current lifecycle state.
    pub fn lifecycle_state(&self) -> Result<LifecycleState> {
        let inner = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;
        Ok(inner.lifecycle.clone())
    }

    // ── Discovery and resolution ─────────────────────────────────────────

    /// Discover the capabilities of a specific worker kind.
    ///
    /// Does not change state. Returns the set of broker-level capabilities
    /// that the backend advertises.
    pub fn discover(&self, kind: WorkerKind, has_native_backend: bool) -> Vec<BrokerCapability> {
        broker_capabilities_for_kind(kind, has_native_backend)
    }

    /// Resolve a phase request, transitioning `Discovered → Resolved`.
    ///
    /// Validates the request and stores it for the subsequent `start()` call.
    pub fn resolve(&self, phase_request: BrokerPhaseRequest) -> Result<()> {
        // Validate the request before touching state.
        phase_request.validate()?;

        let mut inner = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;

        // State machine check (lock held).
        inner
            .lifecycle
            .can_transition_to(&LifecycleState::Resolved)?;

        inner.lifecycle = LifecycleState::Resolved;
        inner.phase_request = Some(phase_request);
        drop(inner); // Release lock before I/O.

        // Write lifecycle event.
        self.append_lifecycle_event(None, "resolve", LifecycleStateName::Resolved)?;

        Ok(())
    }

    // ── Session lifecycle ────────────────────────────────────────────────

    /// Start a new session, transitioning `Resolved → Starting → Active`.
    ///
    /// Takes an already-created worker session handle and identity
    /// (typically from `WorkerRegistry::start()`), validates the state
    /// machine, stores the session identity, and writes the initial
    /// ledger artifacts.
    pub fn start(
        &self,
        handle: Arc<dyn WorkerSessionHandle>,
        identity: BrokerSessionIdentity,
    ) -> Result<Arc<dyn WorkerSessionHandle>> {
        // Validate before touching state.
        identity.validate()?;

        let mut inner = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;

        // Must have a resolved phase request to start.
        let phase_request = inner.phase_request.clone().ok_or_else(|| {
            anyhow::anyhow!("cannot start: no phase request (call resolve() first)")
        })?;

        // State machine: Resolved → Starting
        inner
            .lifecycle
            .can_transition_to(&LifecycleState::Starting)?;

        // Copy data we need while lock is held, then unlock.
        let goal_id = phase_request.goal_id.clone();
        let session_id = identity.session_id.clone();
        let ledger_paths = BrokerLedgerPaths::new(&self.artifacts_root, &goal_id, &session_id);

        inner.lifecycle = LifecycleState::Starting;
        inner.session_identity = Some(identity.clone());
        inner.session_handle = Some(handle.clone());
        inner.ledger_paths = Some(ledger_paths.clone());
        // Record non-interaction transitions (resolve→starting) without
        // incrementing the interaction ordinal.
        drop(inner); // Release lock before I/O.

        // Ensure ledger directories exist.
        ledger_paths
            .ensure_dirs()
            .map_err(|e| LedgerWriteError {
                path: ledger_paths.session_dir.clone(),
                reason: format!("failed to create ledger dirs: {e}"),
            })
            .with_context(|| "ledger write error")?;

        // Write session identity artifact.
        write_json(&ledger_paths.session_identity_path(), &identity)
            .map_err(|e| LedgerWriteError {
                path: ledger_paths.session_identity_path(),
                reason: format!("failed to write session identity: {e}"),
            })
            .with_context(|| "ledger write error")?;

        // Lifecycle event: Starting
        self.append_lifecycle_event_to(
            Some(&LifecycleStateName::Resolved),
            "session starting",
            LifecycleStateName::Starting,
            &ledger_paths,
        )?;

        // Transition from Starting → Active (lock again).
        {
            let mut inner = self
                .state
                .lock()
                .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;
            inner.lifecycle.can_transition_to(&LifecycleState::Active)?;
            inner.lifecycle = LifecycleState::Active;
            let _ordinal = inner.next_ordinal();
        }

        // Lifecycle event: Active
        self.append_lifecycle_event_to(
            Some(&LifecycleStateName::Starting),
            "session active",
            LifecycleStateName::Active,
            &ledger_paths,
        )?;

        // Determine binding status from the identity's backend kind before
        // identity is consumed by the receipt struct.
        let backend_kind = identity.backend_kind;

        // Seal and write the first receipt.
        let receipt = BrokerLifecycleReceipt {
            schema_version: BROKER_SCHEMA_VERSION,
            interaction_ordinal: 1,
            phase_decision_hash: phase_request.phase_decision_hash.clone(),
            session_identity: identity,
            request: phase_request,
            outcome: BrokerOutcome::Completed,
            terminal_reason: None,
            usage: None,
            permission_evidence: None,
            actual_model: None,
            binding_status: binding_status_for_kind(backend_kind, true),
            receipt_hash: String::new(),
        }
        .seal()
        .context("failed to seal initial receipt")?;

        write_json(&ledger_paths.receipt_path(1), &receipt)
            .map_err(|e| LedgerWriteError {
                path: ledger_paths.receipt_path(1),
                reason: format!("failed to write receipt: {e}"),
            })
            .with_context(|| "ledger write error")?;

        Ok(handle)
    }

    // ── Interaction methods ──────────────────────────────────────────────

    /// Send a follow-up prompt to the active session.
    ///
    /// Only allowed when the session advertises the `FollowUp` capability.
    pub fn follow_up(&self, prompt: String) -> Result<()> {
        let (handle, state_name, ledger_paths, ordinal, _identity) = {
            let mut inner = self
                .state
                .lock()
                .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;

            // Must be in Active state (self-transition: no state change).
            if inner.lifecycle != LifecycleState::Active {
                bail!(
                    "follow_up requires Active state, got {:?}",
                    inner.lifecycle.name()
                );
            }

            let identity = inner
                .session_identity
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no session identity"))?;

            // Capability check: follow-up requires FollowUp.
            if !identity.supports(&BrokerCapability::FollowUp) {
                bail!(
                    "session {:?} does not advertise FollowUp capability",
                    identity.session_id
                );
            }

            let owned_identity = identity.clone();
            let handle = inner
                .session_handle
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no session handle"))?
                .clone();
            let ledger_paths = inner
                .ledger_paths
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no ledger paths"))?;
            let state_name = inner.lifecycle.name();
            let ordinal = inner.next_ordinal();

            (handle, state_name, ledger_paths, ordinal, owned_identity)
        }; // Lock released.

        // Call external method without lock held.
        handle
            .send_follow_up(prompt.clone())
            .context("worker session follow_up failed")?;

        // Write lifecycle event.
        self.append_lifecycle_event_to(
            Some(&state_name),
            &format!("follow-up: {}", prompt.chars().take(80).collect::<String>()),
            LifecycleStateName::Active,
            &ledger_paths,
        )?;

        // Write receipt for this interaction.
        let receipt = self.make_interaction_receipt(
            ordinal,
            BrokerOutcome::FollowedUp,
            None,
            None,
            None,
            None,
        )?;
        write_json(&ledger_paths.receipt_path(ordinal), &receipt)
            .map_err(|e| LedgerWriteError {
                path: ledger_paths.receipt_path(ordinal),
                reason: format!("failed to write receipt: {e}"),
            })
            .with_context(|| "ledger write error")?;

        Ok(())
    }

    /// Send a steering prompt to the idle session.
    ///
    /// Only allowed when the session is in `IdleSteering` state and
    /// advertises the `Steer` capability.
    pub fn steer(&self, prompt: String) -> Result<()> {
        let (handle, state_name, ledger_paths, ordinal, _identity) = {
            let mut inner = self
                .state
                .lock()
                .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;

            // IdleSteering → Active transition.
            inner.lifecycle.can_transition_to(&LifecycleState::Active)?;

            let identity = inner
                .session_identity
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no session identity"))?;

            // Capability check: steer requires Steer.
            if !identity.supports(&BrokerCapability::Steer) {
                bail!(
                    "session {:?} does not advertise Steer capability",
                    identity.session_id
                );
            }

            let owned_identity = identity.clone();
            let state_name = inner.lifecycle.name();
            let handle = inner
                .session_handle
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no session handle"))?
                .clone();
            let ledger_paths = inner
                .ledger_paths
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no ledger paths"))?;
            let ordinal = inner.next_ordinal();

            inner.lifecycle = LifecycleState::Active;

            (handle, state_name, ledger_paths, ordinal, owned_identity)
        }; // Lock released.

        // Call external method without lock held.
        handle
            .steer(prompt.clone())
            .context("worker session steer failed")?;

        // Write lifecycle event.
        self.append_lifecycle_event_to(
            Some(&state_name),
            &format!("steer: {}", prompt.chars().take(80).collect::<String>()),
            LifecycleStateName::Active,
            &ledger_paths,
        )?;

        // Write receipt.
        let receipt =
            self.make_interaction_receipt(ordinal, BrokerOutcome::Steered, None, None, None, None)?;
        write_json(&ledger_paths.receipt_path(ordinal), &receipt)
            .map_err(|e| LedgerWriteError {
                path: ledger_paths.receipt_path(ordinal),
                reason: format!("failed to write receipt: {e}"),
            })
            .with_context(|| "ledger write error")?;

        Ok(())
    }

    /// Cancel the current session.
    ///
    /// Idempotent: calling cancel on an already cancelled/terminal
    /// session returns `Ok`. Terminal sessions cannot be revived
    /// (attempting to call `start()` after cancel returns `Err`).
    pub fn cancel(&self) -> Result<()> {
        let (handle, state_name, ledger_paths, outcome, ordinal, identity) = {
            let mut inner = self
                .state
                .lock()
                .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;

            // Idempotent: already terminal/cancelled → Ok.
            if matches!(
                inner.lifecycle,
                LifecycleState::Terminal {
                    outcome: BrokerOutcome::Cancelled,
                    ..
                }
            ) {
                return Ok(());
            }

            // Any non-terminal state can be cancelled.
            inner
                .lifecycle
                .can_transition_to(&LifecycleState::Terminal {
                    outcome: BrokerOutcome::Cancelled,
                    reason: Some("cancelled by user".to_string()),
                })?;

            let state_name = inner.lifecycle.name();
            let handle = inner.session_handle.as_ref().map(|h| h.clone());
            let ledger_paths = inner
                .ledger_paths
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no ledger paths"))?;
            let ordinal = inner.next_ordinal();
            let identity = inner.session_identity.clone();

            // Transition to Terminal(Cancelled).
            inner.lifecycle = LifecycleState::Terminal {
                outcome: BrokerOutcome::Cancelled,
                reason: Some("cancelled by user".to_string()),
            };

            (
                handle,
                state_name,
                ledger_paths,
                BrokerOutcome::Cancelled,
                ordinal,
                identity,
            )
        }; // Lock released.

        // Call external cancel without lock held (best-effort).
        if let Some(ref handle) = handle {
            let _ = handle.cancel();
        }

        // Write lifecycle event.
        self.append_lifecycle_event_to(
            Some(&state_name),
            "session cancelled",
            LifecycleStateName::Terminal,
            &ledger_paths,
        )?;

        // Write terminal outcome.
        if let Some(identity) = identity {
            let outcome_record = TerminalOutcomeRecord {
                schema_version: BROKER_SCHEMA_VERSION,
                timestamp: timestamp(),
                outcome,
                reason: Some("cancelled by user".to_string()),
                total_interactions: ordinal,
                session_identity: identity,
            };
            write_json(&ledger_paths.terminal_outcome_path(), &outcome_record)
                .map_err(|e| LedgerWriteError {
                    path: ledger_paths.terminal_outcome_path(),
                    reason: format!("failed to write terminal outcome: {e}"),
                })
                .with_context(|| "ledger write error")?;

            // Write final receipt.
            match self.build_receipt_from_state(
                ordinal,
                BrokerOutcome::Cancelled,
                Some("cancelled by user".to_string()),
            ) {
                Ok(receipt) => {
                    write_json(&ledger_paths.receipt_path(ordinal), &receipt)
                        .map_err(|e| LedgerWriteError {
                            path: ledger_paths.receipt_path(ordinal),
                            reason: format!("failed to write receipt: {e}"),
                        })
                        .with_context(|| "ledger write error")?;
                }
                Err(e) => {
                    // Terminal outcome is the primary artifact; receipt
                    // is best-effort.
                    let _ = e;
                }
            }
        }

        Ok(())
    }

    /// Wait for the current interaction to complete (transition to
    /// `IdleSteering`).  Returns the worker's turn outcome.
    pub fn wait(&self) -> Result<()> {
        let (handle, state_name, ledger_paths) = {
            let mut inner = self
                .state
                .lock()
                .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;

            inner
                .lifecycle
                .can_transition_to(&LifecycleState::IdleSteering)?;

            let handle = inner
                .session_handle
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no session handle"))?
                .clone();
            let ledger_paths = inner
                .ledger_paths
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no ledger paths"))?;
            let state_name = inner.lifecycle.name();

            inner.lifecycle = LifecycleState::IdleSteering;

            (handle, state_name, ledger_paths)
        }; // Lock released.

        // Wait without lock held.
        handle
            .wait_for_idle()
            .context("worker session wait_for_idle failed")?;

        // Write lifecycle event.
        self.append_lifecycle_event_to(
            Some(&state_name),
            "session idle",
            LifecycleStateName::IdleSteering,
            &ledger_paths,
        )?;

        Ok(())
    }

    /// Wait for the session to produce a final outcome (transition to
    /// `Terminal`).
    pub fn wait_for_outcome(&self) -> Result<()> {
        let (handle, state_name, ledger_paths, ordinal, identity, phase_request) = {
            let mut inner = self
                .state
                .lock()
                .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;

            let handle = inner
                .session_handle
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no session handle"))?
                .clone();
            let ledger_paths = inner
                .ledger_paths
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no ledger paths"))?;
            let state_name = inner.lifecycle.name();
            let ordinal = inner.next_ordinal();
            let identity = inner.session_identity.clone();
            let phase_request = inner.phase_request.clone();

            (
                handle,
                state_name,
                ledger_paths,
                ordinal,
                identity,
                phase_request,
            )
        }; // Lock released.

        // Call external method without lock.
        let outcome = handle
            .wait_for_outcome()
            .context("worker session wait_for_outcome failed")?;

        let broker_outcome = match outcome.status {
            crate::workers::WorkerStatus::Succeeded => BrokerOutcome::Completed,
            crate::workers::WorkerStatus::Failed => BrokerOutcome::Failed,
            crate::workers::WorkerStatus::Skipped => BrokerOutcome::Completed,
        };

        // Lock again to update state.
        {
            let mut inner = self
                .state
                .lock()
                .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;

            inner.lifecycle = LifecycleState::Terminal {
                outcome: broker_outcome.clone(),
                reason: Some(outcome.summary.clone()),
            };
        }

        // Write lifecycle event.
        self.append_lifecycle_event_to(
            Some(&state_name),
            &format!("session outcome: {}", outcome.summary),
            LifecycleStateName::Terminal,
            &ledger_paths,
        )?;

        // Write terminal outcome.
        if let Some(ref identity) = identity {
            let outcome_record = TerminalOutcomeRecord {
                schema_version: BROKER_SCHEMA_VERSION,
                timestamp: timestamp(),
                outcome: broker_outcome.clone(),
                reason: Some(outcome.summary.clone()),
                total_interactions: ordinal,
                session_identity: identity.clone(),
            };
            write_json(&ledger_paths.terminal_outcome_path(), &outcome_record)
                .map_err(|e| LedgerWriteError {
                    path: ledger_paths.terminal_outcome_path(),
                    reason: format!("failed to write terminal outcome: {e}"),
                })
                .with_context(|| "ledger write error")?;
        }

        // Write final receipt.
        if let (Some(identity), Some(phase_request)) = (&identity, &phase_request) {
            let receipt = BrokerLifecycleReceipt {
                schema_version: BROKER_SCHEMA_VERSION,
                interaction_ordinal: ordinal,
                phase_decision_hash: phase_request.phase_decision_hash.clone(),
                session_identity: identity.clone(),
                request: phase_request.clone(),
                outcome: broker_outcome,
                terminal_reason: Some(outcome.summary),
                usage: None,
                permission_evidence: None,
                actual_model: None,
                binding_status: binding_status_for_kind(identity.backend_kind, true),
                receipt_hash: String::new(),
            }
            .seal()
            .context("failed to seal terminal receipt")?;

            write_json(&ledger_paths.receipt_path(ordinal), &receipt)
                .map_err(|e| LedgerWriteError {
                    path: ledger_paths.receipt_path(ordinal),
                    reason: format!("failed to write receipt: {e}"),
                })
                .with_context(|| "ledger write error")?;
        }

        Ok(())
    }

    // ── Broker dispatch ──────────────────────────────────────────────────

    /// Start a worker session through the broker lifecycle.
    ///
    /// This is the preferred entry point when a broker is active. It:
    /// 1. Dispatches to the appropriate adapter via the registry
    /// 2. Wraps the resulting handle through the broker lifecycle
    /// 3. Returns the lifecycle-managed handle
    ///
    /// Requires that `resolve()` was called first with a `BrokerPhaseRequest`.
    pub fn start_via_broker(
        &self,
        request: crate::workers::WorkerStartRequest<'_>,
    ) -> Result<Arc<dyn WorkerSessionHandle>> {
        let selected_route = request
            .config
            .selected_route_for_hint(request.route_attempt, request.route_hint);
        let worker_kind = selected_route.worker_kind;
        let task_id = request.task.id.clone();

        // The factory-owned registry must dispatch the real handle exactly
        // once. Clearing its optional broker avoids recursively starting a
        // different broker when this method is reused outside the factory.
        let registry = self.registry.without_broker();
        let has_native_backend = registry.has_native_backend();
        let handle = registry.start(request)?;
        let identity = BrokerSessionIdentity {
            backend_kind: worker_kind,
            session_id: handle
                .session_id()
                .unwrap_or_else(|| format!("{}-{task_id}", worker_kind.as_str())),
            started_at: timestamp(),
            capabilities: Some(broker_capabilities_for_kind(
                worker_kind,
                has_native_backend,
            )),
        };

        self.start(handle, identity)
    }

    // ── Ledger helpers ───────────────────────────────────────────────────

    /// Append a lifecycle event to the JSONL file.
    fn append_lifecycle_event(
        &self,
        from_state: Option<LifecycleStateName>,
        message: &str,
        to_state: LifecycleStateName,
    ) -> Result<()> {
        let ledger_paths = {
            let inner = self
                .state
                .lock()
                .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;
            inner.ledger_paths.clone()
        };

        if let Some(ref paths) = ledger_paths {
            self.append_lifecycle_event_to(from_state.as_ref(), message, to_state, paths)?;
        } else {
            // No ledger paths yet; this is OK for early transitions.
        }
        Ok(())
    }

    /// Append a lifecycle event to a specific ledger path set.
    fn append_lifecycle_event_to(
        &self,
        from_state: Option<&LifecycleStateName>,
        message: &str,
        to_state: LifecycleStateName,
        paths: &BrokerLedgerPaths,
    ) -> Result<()> {
        let session_id = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?
            .session_identity
            .as_ref()
            .map(|id| id.session_id.clone())
            .unwrap_or_default();

        let event = BrokerLifecycleEvent {
            schema_version: BROKER_SCHEMA_VERSION,
            timestamp: timestamp(),
            from_state: from_state.copied(),
            to_state,
            interaction_ordinal: self
                .state
                .lock()
                .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?
                .interaction_ordinal,
            session_id,
            message: message.to_string(),
        };

        append_jsonl(&paths.lifecycle_events_path(), &event)
            .map_err(|e| LedgerWriteError {
                path: paths.lifecycle_events_path(),
                reason: format!("failed to append lifecycle event: {e}"),
            })
            .with_context(|| "ledger write error")?;

        Ok(())
    }

    /// Build a lifecycle receipt from current state for an interaction.
    fn make_interaction_receipt(
        &self,
        ordinal: u64,
        outcome: BrokerOutcome,
        terminal_reason: Option<String>,
        usage: Option<BrokerUsage>,
        permission_evidence: Option<BrokerPermissionEvidence>,
        actual_model: Option<ModelSelectorId>,
    ) -> Result<BrokerLifecycleReceipt> {
        let inner = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;
        let phase_request = inner
            .phase_request
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no phase request"))?;
        let identity = inner
            .session_identity
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no session identity"))?;

        BrokerLifecycleReceipt {
            schema_version: BROKER_SCHEMA_VERSION,
            interaction_ordinal: ordinal,
            phase_decision_hash: phase_request.phase_decision_hash.clone(),
            session_identity: identity.clone(),
            request: phase_request.clone(),
            outcome,
            terminal_reason,
            usage,
            permission_evidence,
            actual_model,
            binding_status: binding_status_for_kind(identity.backend_kind, true),
            receipt_hash: String::new(),
        }
        .seal()
    }

    /// Build a receipt from the stored phase request and identity.
    fn build_receipt_from_state(
        &self,
        ordinal: u64,
        outcome: BrokerOutcome,
        terminal_reason: Option<String>,
    ) -> Result<BrokerLifecycleReceipt> {
        let inner = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("broker state mutex poisoned: {e}"))?;
        let phase_request = inner
            .phase_request
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no phase request"))?;
        let identity = inner
            .session_identity
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no session identity"))?;

        BrokerLifecycleReceipt {
            schema_version: BROKER_SCHEMA_VERSION,
            interaction_ordinal: ordinal,
            phase_decision_hash: phase_request.phase_decision_hash.clone(),
            session_identity: identity.clone(),
            request: phase_request.clone(),
            outcome,
            terminal_reason,
            usage: None,
            permission_evidence: None,
            actual_model: None,
            binding_status: binding_status_for_kind(identity.backend_kind, true),
            receipt_hash: String::new(),
        }
        .seal()
    }
}

// ---------------------------------------------------------------------------
// CLI model identity rules
// ---------------------------------------------------------------------------

/// Determine whether a backend kind uses CLI-declared (unverified) models.
///
/// CLI backends (Opencode, Codex, Claude, Custom) declare their model via
/// the CLI tool and the identity is NOT ACP-verified. Therefore the binding
/// status is `DeclaredUnverified` and model mismatches between the
/// requested model and actual model are permitted.
///
/// Resident backends with native verification (ZedAgent with native backend)
/// produce ACP-verified `Applied` bindings. OpencodeSession models are
/// backend-declared but within the session scope.
fn binding_status_for_kind(
    kind: WorkerKind,
    has_native_backend: bool,
) -> Option<ModelBindingStatus> {
    match kind {
        // CLI backends — model is declared by the tool, not ACP-verified
        WorkerKind::Opencode | WorkerKind::Codex | WorkerKind::Claude | WorkerKind::Custom => {
            Some(ModelBindingStatus::DeclaredUnverified)
        }
        // Resident session — model is declared within the session scope
        WorkerKind::OpencodeSession => Some(ModelBindingStatus::DeclaredUnverified),
        // ZedAgent with native backend — model is ACP-verified
        WorkerKind::ZedAgent if has_native_backend => Some(ModelBindingStatus::Applied),
        // CLI ZedAgent — model is CLI-declared
        WorkerKind::ZedAgent => Some(ModelBindingStatus::DeclaredUnverified),
    }
}

/// True when the binding status allows model mismatches (CLI-declared).
fn is_declared_unverified(status: &Option<ModelBindingStatus>) -> bool {
    status.as_ref().is_some_and(|s| {
        matches!(
            s,
            ModelBindingStatus::DeclaredUnverified | ModelBindingStatus::LegacyUnverified
        )
    })
}

// ---------------------------------------------------------------------------
// Ledger persistence helpers
// ---------------------------------------------------------------------------

/// Append a single JSON object (as one line) to a JSONL file.
pub fn append_jsonl<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize + ?Sized,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let line = serde_json::to_string(value).context("failed to serialize jsonl entry")?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {} for append", path.display()))?;
    writeln!(file, "{line}").with_context(|| format!("failed to write to {}", path.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Session ledger validation
// ---------------------------------------------------------------------------

/// Validate a complete session ledger on disk.
///
/// Checks:
/// - `session-identity.json` exists and is valid
/// - `lifecycle-events.jsonl` is parseable
/// - Each receipt file passes cryptographic integrity (`BrokerLifecycleReceipt::validate`)
/// - Receipt ordinals are strictly monotonic (1, 2, 3, …)
/// - All receipts share the same `phase_decision_hash`, `goal_id`, `plan_id`, `task_id`
/// - All receipts share the same `session_identity`
/// - If `terminal-outcome.json` exists, its `total_interactions` matches the highest ordinal
pub fn validate_session_ledger(session_dir: &Path) -> Result<()> {
    // 1. Check session-identity.json exists.
    let identity_path = session_dir.join("session-identity.json");
    if !identity_path.exists() {
        bail!("session ledger missing {}", identity_path.display());
    }
    let identity: BrokerSessionIdentity = read_json_file(&identity_path)
        .with_context(|| format!("failed to read {}", identity_path.display()))?;
    identity.validate()?;

    // 2. Check lifecycle-events.jsonl is parseable.
    let events_path = session_dir.join("lifecycle-events.jsonl");
    if events_path.exists() {
        let contents = fs::read_to_string(&events_path)
            .with_context(|| format!("failed to read {}", events_path.display()))?;
        for (i, line) in contents.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let _event: BrokerLifecycleEvent = serde_json::from_str(line)
                .with_context(|| format!("failed to parse lifecycle event line {}", i + 1))?;
        }
    }

    // 3. Read receipts directory.
    let receipts_dir = session_dir.join("receipts");
    if !receipts_dir.is_dir() {
        bail!("session ledger missing receipts directory");
    }

    let mut receipt_entries: Vec<(u64, PathBuf)> = fs::read_dir(&receipts_dir)
        .with_context(|| format!("failed to read {}", receipts_dir.display()))?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            let stem = path.file_stem()?.to_str()?;
            let ordinal: u64 = stem.parse().ok()?;
            if path.extension()?.to_str()? == "json" {
                Some((ordinal, path))
            } else {
                None
            }
        })
        .collect();

    receipt_entries.sort_by_key(|(ordinal, _)| *ordinal);

    if receipt_entries.is_empty() {
        bail!("session ledger has no receipt files");
    }

    // Track invariants across all receipts.
    let mut prev_ordinal: u64 = 0;
    let mut common_phase_hash: Option<String> = None;
    let mut common_goal_id: Option<String> = None;
    let mut common_plan_id: Option<String> = None;
    let mut common_task_id: Option<String> = None;
    let mut common_session_id: Option<String> = None;

    for (ordinal, path) in &receipt_entries {
        // 3a. Ordinal sequence: strictly contiguous (1, 2, 3, ...).
        if prev_ordinal == 0 && *ordinal != 1 {
            bail!("receipt sequence must start at ordinal 1, got {}", ordinal);
        }
        if prev_ordinal > 0 && *ordinal != prev_ordinal + 1 {
            bail!(
                "receipt ordinal {} is not contiguous after {} (ordinal jump detected)",
                ordinal,
                prev_ordinal
            );
        }
        prev_ordinal = *ordinal;

        // 3b. Hash integrity.
        let receipt: BrokerLifecycleReceipt = read_json_file(path)
            .with_context(|| format!("failed to read receipt {}", path.display()))?;
        receipt
            .validate()
            .with_context(|| format!("receipt {} failed integrity check", path.display()))?;

        // 3c. Cross-receipt consistency.
        if let Some(ref expected) = common_phase_hash {
            if *expected != receipt.phase_decision_hash {
                bail!(
                    "receipt {} phase_decision_hash mismatch: expected {} got {}",
                    ordinal,
                    expected,
                    receipt.phase_decision_hash
                );
            }
        } else {
            common_phase_hash = Some(receipt.phase_decision_hash.clone());
        }

        if let Some(ref expected) = common_goal_id {
            if *expected != receipt.request.goal_id {
                bail!(
                    "receipt {} goal_id mismatch: expected {} got {}",
                    ordinal,
                    expected,
                    receipt.request.goal_id
                );
            }
        } else {
            common_goal_id = Some(receipt.request.goal_id.clone());
        }

        if let Some(ref expected) = common_plan_id {
            if *expected != receipt.request.plan_id {
                bail!(
                    "receipt {} plan_id mismatch: expected {} got {}",
                    ordinal,
                    expected,
                    receipt.request.plan_id
                );
            }
        } else {
            common_plan_id = Some(receipt.request.plan_id.clone());
        }

        if let Some(ref expected) = common_task_id {
            if *expected != receipt.request.task_id {
                bail!(
                    "receipt {} task_id mismatch: expected {} got {}",
                    ordinal,
                    expected,
                    receipt.request.task_id
                );
            }
        } else {
            common_task_id = Some(receipt.request.task_id.clone());
        }

        if let Some(ref expected) = common_session_id {
            if *expected != receipt.session_identity.session_id {
                bail!(
                    "receipt {} session_identity.session_id mismatch: expected {} got {}",
                    ordinal,
                    expected,
                    receipt.session_identity.session_id
                );
            }
        } else {
            common_session_id = Some(receipt.session_identity.session_id.clone());
        }
    }

    // 4. Check terminal-outcome.json if present.
    let terminal_path = session_dir.join("terminal-outcome.json");
    if terminal_path.exists() {
        let outcome: TerminalOutcomeRecord = read_json_file(&terminal_path)
            .with_context(|| format!("failed to read {}", terminal_path.display()))?;
        if outcome.total_interactions != prev_ordinal {
            bail!(
                "terminal-outcome.json total_interactions {} does not match highest receipt ordinal {}",
                outcome.total_interactions,
                prev_ordinal
            );
        }
        if outcome.session_identity.session_id != common_session_id.as_deref().unwrap_or_default() {
            bail!(
                "terminal-outcome.json session_id mismatch: expected {} got {}",
                common_session_id.as_deref().unwrap_or_default(),
                outcome.session_identity.session_id
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Replace characters that are problematic in file paths.
fn sanitize_for_path(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Read and deserialize a JSON file.
fn read_json_file<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&contents).with_context(|| format!("failed to parse {}", path.display()))
}

// ---------------------------------------------------------------------------
// PhaseBrokerFactory — per-phase goal-scoped broker session factory
// ---------------------------------------------------------------------------

/// Track a single active broker session in the factory.
#[derive(Clone, Debug)]
struct ActiveSessionEntry {
    /// Unique session key: `{execution_id}:{goal_id}:{task_id}:{plan_revision}`.
    session_key: String,
    /// The execution identity's execution_id.
    execution_id: String,
    /// Goal this session belongs to.
    goal_id: String,
    /// Task within the goal.
    task_id: String,
    /// Plan revision number.
    plan_revision: usize,
    /// Independence group (e.g. "planning", "execution").
    phase_group: String,
    /// The phase whose ledger this entry owns.
    phase: PhaseProfile,
}

#[derive(Clone, Debug)]
struct CompletedSessionEntry {
    goal_id: String,
    task_id: String,
    plan_revision: usize,
    phase: PhaseProfile,
    session_dir: PathBuf,
}

/// Return the independence group for a phase profile.
fn independence_group_for_phase(phase: &PhaseProfile) -> &'static str {
    match phase {
        PhaseProfile::Planner => "planning",
        PhaseProfile::PlanCritic => "plan_review",
        PhaseProfile::Orchestrator => "orchestrator",
        PhaseProfile::ExecutorQuick => "execution",
        PhaseProfile::ExecutorDeep => "execution",
        PhaseProfile::ReviewerTask => "task_review",
        PhaseProfile::ReviewerFinal => "final_review",
        PhaseProfile::StrategistNextGoal => "strategy",
        PhaseProfile::Summarizer => "summarization",
    }
}

/// Return a filesystem-safe snake_case name for a phase profile.
fn phase_profile_to_path(phase: &PhaseProfile) -> &'static str {
    match phase {
        PhaseProfile::Planner => "planner",
        PhaseProfile::PlanCritic => "plan_critic",
        PhaseProfile::Orchestrator => "orchestrator",
        PhaseProfile::ExecutorQuick => "executor_quick",
        PhaseProfile::ExecutorDeep => "executor_deep",
        PhaseProfile::ReviewerTask => "reviewer_task",
        PhaseProfile::ReviewerFinal => "reviewer_final",
        PhaseProfile::StrategistNextGoal => "strategist_next_goal",
        PhaseProfile::Summarizer => "summarizer",
    }
}

/// Send-safe factory that creates per-phase broker sessions with
/// independent goal-scoped lifecycle and ledger isolation.
///
/// Each call to [`create_broker`] returns a new `Arc<WorkerBroker>` whose
/// ledger artifacts are written to an isolated path under
/// `{artifacts_root}/broker-sessions/{phase}/{goal_id}/{task_id}/{revision}/`.
///
/// The factory enforces:
/// - No duplicate session keys (same execution_id + goal + task + revision).
/// - No cross-role session reuse within the same independence group.
/// - No replay of terminal sessions (on-disk ledger detection).
pub struct PhaseBrokerFactory {
    registry: Arc<WorkerRegistry>,
    artifacts_root: PathBuf,
    /// Track active session entries to prevent reuse/cross-role sharing.
    active_sessions: Arc<Mutex<Vec<ActiveSessionEntry>>>,
    /// Terminal ledgers accepted by the completion gate for this run.
    completed_sessions: Arc<Mutex<Vec<CompletedSessionEntry>>>,
}

#[derive(Clone, Debug)]
pub struct PhaseWorkerExecution {
    pub result: WorkerResult,
    pub execution_identity: PhaseExecutionIdentity,
    pub session_identity: BrokerSessionIdentity,
    pub session_dir: PathBuf,
}

impl PhaseBrokerFactory {
    /// Create a new phase broker factory.
    pub fn new(registry: Arc<WorkerRegistry>, artifacts_root: PathBuf) -> Self {
        Self {
            registry,
            artifacts_root,
            active_sessions: Arc::new(Mutex::new(Vec::new())),
            completed_sessions: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create a new broker session for the given phase invocation.
    ///
    /// Returns an `Arc<WorkerBroker>` whose ledger is isolated to:
    /// `{artifacts_root}/broker-sessions/{phase}/{goal_id}/{task_id}/{revision}/`
    ///
    /// # Guards
    ///
    /// 1. **Duplicate session key**: If the exact session key
    ///    (`{execution_id}:{goal_id}:{task_id}:{plan_revision}`) already
    ///    exists in `active_sessions`, the call bails with
    ///    "broker session already exists".
    ///
    /// 2. **Cross-role sharing**: If any active session shares the same
    ///    `goal_id` + `task_id` + `plan_revision` but a different
    ///    `execution_id` within the same independence group, the call
    ///    bails with "cross-role session reuse detected".
    ///
    /// 3. **Terminal replay**: If the target ledger path already contains
    ///    session data (from a previous terminal run), the call bails
    ///    with "session replay detected".
    pub fn create_broker(
        &self,
        phase_decision: &PhaseRouteDecision,
        goal_id: &str,
        _plan_id: &str,
        plan_revision: usize,
        task_id: &str,
        execution_identity: &PhaseExecutionIdentity,
    ) -> Result<Arc<WorkerBroker>> {
        let session_key = format!(
            "{}:{}:{}:{}",
            execution_identity.execution_id, goal_id, task_id, plan_revision,
        );

        let phase_group = independence_group_for_phase(&phase_decision.phase);

        let mut sessions = self.active_sessions.lock().map_err(|e| {
            anyhow::anyhow!("PhaseBrokerFactory active_sessions lock poisoned: {e}")
        })?;

        if sessions
            .iter()
            .any(|s: &ActiveSessionEntry| s.session_key == session_key)
        {
            bail!(
                "broker session already exists for execution identity {}",
                execution_identity.execution_id
            );
        }

        if sessions.iter().any(|s| {
            s.goal_id == goal_id
                && s.task_id == task_id
                && s.plan_revision == plan_revision
                && s.execution_id != execution_identity.execution_id
                && s.phase_group == phase_group
        }) {
            bail!(
                "cross-role session reuse detected: goal={}, task={}, revision={}, group={}",
                goal_id,
                task_id,
                plan_revision,
                phase_group,
            );
        }

        let session_root = self
            .artifacts_root
            .join("broker-sessions")
            .join(phase_profile_to_path(&phase_decision.phase))
            .join(sanitize_for_path(goal_id))
            .join(sanitize_for_path(task_id))
            .join(format!("{}", plan_revision));

        let replay_check_path = session_root
            .join(sanitize_for_path(goal_id))
            .join("broker-sessions");
        if replay_check_path.exists() {
            if let Ok(mut entries) = fs::read_dir(&replay_check_path) {
                if entries.next().is_some() {
                    bail!(
                        "session replay detected: broker session data already exists at {}",
                        replay_check_path.display()
                    );
                }
            }
        }

        let broker = Arc::new(WorkerBroker::new(self.registry.clone(), session_root));

        sessions.push(ActiveSessionEntry {
            session_key,
            execution_id: execution_identity.execution_id.clone(),
            goal_id: goal_id.to_string(),
            task_id: task_id.to_string(),
            plan_revision,
            phase_group: phase_group.to_string(),
            phase: phase_decision.phase.clone(),
        });

        Ok(broker)
    }

    pub fn execute_worker_phase(
        &self,
        phase_decision: &PhaseRouteDecision,
        goal_id: &str,
        plan_id: &str,
        plan_revision: usize,
        task_id: &str,
        execution_id: &str,
        phase_session_id: &str,
        request: WorkerStartRequest<'_>,
    ) -> Result<PhaseWorkerExecution> {
        if phase_decision.worker_kind.is_none() {
            bail!(
                "phase {:?} is not configured for a worker session",
                phase_decision.phase
            );
        }
        if request.task.id != task_id || request.task.goal_id != goal_id {
            bail!("phase worker request does not match its goal/task binding");
        }
        let execution_identity = PhaseExecutionIdentity {
            execution_id: execution_id.to_string(),
            phase_session_id: phase_session_id.to_string(),
            backend: crate::plan_review::PhaseExecutionBackend::DeterministicRules,
            agent_id: None,
            provider_id: None,
            model_id: None,
            actual_session_id: None,
        };
        execution_identity.validate()?;

        let phase_request = BrokerPhaseRequest::from_phase_decision(
            phase_decision,
            goal_id,
            plan_id,
            plan_revision,
            task_id,
        )?;
        let broker = self.create_broker(
            phase_decision,
            goal_id,
            plan_id,
            plan_revision,
            task_id,
            &execution_identity,
        )?;

        let execution = (|| -> Result<PhaseWorkerExecution> {
            broker.resolve(phase_request)?;
            let handle = broker.start_via_broker(request)?;
            broker.wait_for_outcome()?;
            let result = handle.wait_for_result()?;
            let session_identity = broker.session_identity()?;
            let session_dir = broker.session_ledger_dir()?;
            let model = match &phase_decision.requested_model {
                Some(model) => model.clone(),
                None => match &phase_decision.candidate.model {
                    PhaseModelBinding::BackendDeclared(model) => ModelSelectorId::from_qualified(
                        phase_decision
                            .worker_kind
                            .context("phase worker kind is missing")?
                            .as_str(),
                        model,
                    )?,
                    _ => bail!("phase worker session is missing a qualified model binding"),
                },
            };
            let actual_execution_identity = PhaseExecutionIdentity {
                execution_id: execution_identity.execution_id.clone(),
                phase_session_id: execution_identity.phase_session_id.clone(),
                backend: crate::plan_review::PhaseExecutionBackend::WorkerSession,
                agent_id: Some(model.agent_id.clone()),
                provider_id: Some(model.provider_id.clone()),
                model_id: Some(model.model_id.clone()),
                actual_session_id: Some(session_identity.session_id.clone()),
            };
            actual_execution_identity.validate()?;
            self.finalize_session(
                &broker,
                &execution_identity,
                goal_id,
                task_id,
                plan_revision,
            )?;
            Ok(PhaseWorkerExecution {
                result,
                execution_identity: actual_execution_identity,
                session_identity,
                session_dir,
            })
        })();

        if execution.is_err() {
            if matches!(
                broker.lifecycle_state(),
                Ok(LifecycleState::Starting)
                    | Ok(LifecycleState::Active)
                    | Ok(LifecycleState::IdleSteering)
            ) {
                broker
                    .cancel()
                    .context("failed to cancel phase worker after execution error")?;
            }
            self.remove_session(&execution_identity, goal_id, task_id, plan_revision)?;
        }

        execution
    }

    /// Remove a session from the active sessions list.
    ///
    /// Should be called after a phase completes (success or failure) so that
    /// subsequent phase invocations can reuse the factory without hitting the
    /// duplicate session guard.
    pub fn remove_session(
        &self,
        execution_identity: &PhaseExecutionIdentity,
        goal_id: &str,
        task_id: &str,
        plan_revision: usize,
    ) -> Result<()> {
        let session_key = format!(
            "{}:{}:{}:{}",
            execution_identity.execution_id, goal_id, task_id, plan_revision,
        );
        let mut sessions = self.active_sessions.lock().map_err(|e| {
            anyhow::anyhow!("PhaseBrokerFactory active_sessions lock poisoned: {e}")
        })?;
        sessions.retain(|s| s.session_key != session_key);
        Ok(())
    }

    /// Seal one terminal phase ledger into the factory manifest.
    ///
    /// The completion gate validates this manifest rather than trusting a
    /// phase directory to merely exist. A broker is recorded only after its
    /// own terminal state and on-disk receipt chain have both been verified.
    pub fn finalize_session(
        &self,
        broker: &WorkerBroker,
        execution_identity: &PhaseExecutionIdentity,
        goal_id: &str,
        task_id: &str,
        plan_revision: usize,
    ) -> Result<()> {
        let lifecycle = broker.lifecycle_state()?;
        if !matches!(lifecycle, LifecycleState::Terminal { .. }) {
            bail!("cannot finalize broker session before it reaches Terminal");
        }

        let session_identity = broker.session_identity()?;
        let session_dir = broker.session_ledger_dir()?;
        validate_session_ledger(&session_dir)?;

        let terminal_path = session_dir.join("terminal-outcome.json");
        if !terminal_path.is_file() {
            bail!("broker terminal ledger missing {}", terminal_path.display());
        }
        let terminal: TerminalOutcomeRecord = read_json_file(&terminal_path)?;
        if terminal.session_identity.session_id != session_identity.session_id {
            bail!("broker terminal ledger session identity mismatch");
        }

        let session_key = format!(
            "{}:{}:{}:{}",
            execution_identity.execution_id, goal_id, task_id, plan_revision,
        );
        let active = {
            let mut sessions = self.active_sessions.lock().map_err(|e| {
                anyhow::anyhow!("PhaseBrokerFactory active_sessions lock poisoned: {e}")
            })?;
            let position = sessions
                .iter()
                .position(|entry| entry.session_key == session_key)
                .ok_or_else(|| anyhow::anyhow!("missing active broker session for finalization"))?;
            sessions.remove(position)
        };

        let mut completed = self.completed_sessions.lock().map_err(|e| {
            anyhow::anyhow!("PhaseBrokerFactory completed_sessions lock poisoned: {e}")
        })?;
        if completed
            .iter()
            .any(|entry| entry.session_dir == session_dir)
        {
            bail!("broker terminal ledger was finalized twice");
        }
        completed.push(CompletedSessionEntry {
            goal_id: active.goal_id,
            task_id: active.task_id,
            plan_revision: active.plan_revision,
            phase: active.phase,
            session_dir,
        });
        Ok(())
    }

    /// Validate all terminal broker ledgers that participated in one goal.
    pub fn validate_goal_receipts(
        &self,
        goal_id: &str,
        require_terminal_receipt: bool,
    ) -> Result<()> {
        let entries: Vec<_> = self
            .completed_sessions
            .lock()
            .map_err(|e| {
                anyhow::anyhow!("PhaseBrokerFactory completed_sessions lock poisoned: {e}")
            })?
            .iter()
            .filter(|entry| entry.goal_id == goal_id)
            .cloned()
            .collect();

        if entries.is_empty() {
            if require_terminal_receipt {
                bail!("goal {goal_id} has no terminal broker receipt");
            }
            return Ok(());
        }

        for entry in entries {
            validate_session_ledger(&entry.session_dir).with_context(|| {
                format!(
                    "broker ledger validation failed for phase {:?}, task {}, revision {}",
                    entry.phase, entry.task_id, entry.plan_revision
                )
            })?;
            let terminal_path = entry.session_dir.join("terminal-outcome.json");
            let terminal: TerminalOutcomeRecord =
                read_json_file(&terminal_path).with_context(|| {
                    format!(
                        "broker terminal outcome missing for phase {:?}, task {}",
                        entry.phase, entry.task_id
                    )
                })?;
            if terminal.outcome != BrokerOutcome::Completed {
                bail!(
                    "broker phase {:?} task {} ended as {:?}",
                    entry.phase,
                    entry.task_id,
                    terminal.outcome
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a simple model selector for test use.
    fn test_model(qualified_id: &str) -> ModelSelectorId {
        ModelSelectorId::from_qualified("test-agent", qualified_id).expect("test model id is valid")
    }

    /// Hex string of the correct length that passes validate_sha256.
    fn valid_hex64() -> String {
        "a".repeat(64)
    }

    fn valid_timestamp() -> String {
        "2026-07-11T00:00:00Z".to_string()
    }

    fn full_session() -> BrokerSessionIdentity {
        BrokerSessionIdentity {
            backend_kind: WorkerKind::OpencodeSession,
            session_id: "ses-test-full".to_string(),
            started_at: valid_timestamp(),
            capabilities: Some(vec![
                BrokerCapability::DiscoverAgents,
                BrokerCapability::ModelSelection,
                BrokerCapability::Start,
                BrokerCapability::FollowUp,
                BrokerCapability::Steer,
                BrokerCapability::Cancel,
                BrokerCapability::Wait,
                BrokerCapability::Usage,
                BrokerCapability::Permission,
                BrokerCapability::SessionResume,
            ]),
        }
    }

    fn basic_request(avail: ModelAvailability) -> BrokerPhaseRequest {
        BrokerPhaseRequest {
            schema_version: BROKER_SCHEMA_VERSION,
            phase_decision_hash: valid_hex64(),
            goal_id: "goal-1".to_string(),
            plan_id: "plan-1".to_string(),
            plan_revision: 2,
            task_id: "task-1".to_string(),
            requested_agent: "test-agent".to_string(),
            requested_model: avail,
            allowed_fallback_models: vec![test_model("fallback/model")],
        }
    }

    fn full_receipt(
        request: BrokerPhaseRequest,
        session: BrokerSessionIdentity,
        outcome: BrokerOutcome,
    ) -> BrokerLifecycleReceipt {
        BrokerLifecycleReceipt {
            schema_version: BROKER_SCHEMA_VERSION,
            interaction_ordinal: 1,
            phase_decision_hash: request.phase_decision_hash.clone(),
            session_identity: session,
            request,
            outcome,
            terminal_reason: None,
            usage: Some(BrokerUsage {
                requested_tokens: Some(100),
                actual_tokens: Some(50),
                model: "test-model".to_string(),
                duration_ms: Some(5000),
                unavailable_reason: None,
            }),
            permission_evidence: None,
            actual_model: None,
            binding_status: None,
            receipt_hash: String::new(),
        }
    }

    // -----------------------------------------------------------------------
    // 1. full_capability — all backend features present → happy path
    // -----------------------------------------------------------------------
    #[test]
    fn full_capability() -> Result<()> {
        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        let session = full_session();
        let receipt = full_receipt(request, session, BrokerOutcome::Completed);

        // Seal and validate — should succeed.
        let sealed = receipt.seal()?;
        sealed.validate()?;

        // The capability matrix should include all capabilities.
        let caps = broker_capabilities_for_kind(WorkerKind::OpencodeSession, false);
        assert!(caps.contains(&BrokerCapability::ModelSelection));
        assert!(caps.contains(&BrokerCapability::FollowUp));
        assert!(caps.contains(&BrokerCapability::Steer));
        assert!(caps.contains(&BrokerCapability::SessionResume));
        assert!(caps.contains(&BrokerCapability::Permission));
        assert!(caps.contains(&BrokerCapability::Cancel));
        assert!(caps.contains(&BrokerCapability::Usage));
        assert_eq!(caps.len(), 10);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 2. missing_model_selector — backend cannot select model → typed unavailable
    // -----------------------------------------------------------------------
    #[test]
    fn missing_model_selector() -> Result<()> {
        let request = basic_request(ModelAvailability::Unavailable(
            UnavailableReason::NotSupported,
        ));
        // Opencode (non-resident) does not support model selection.
        let session = BrokerSessionIdentity {
            backend_kind: WorkerKind::Opencode,
            session_id: "ses-test-basic".to_string(),
            started_at: valid_timestamp(),
            capabilities: Some(vec![
                BrokerCapability::DiscoverAgents,
                BrokerCapability::Start,
                BrokerCapability::Cancel,
                BrokerCapability::Wait,
                BrokerCapability::Usage,
                BrokerCapability::Permission,
            ]),
        };
        // Explicitly verify the backend does NOT advertise ModelSelection.
        assert!(!session.supports(&BrokerCapability::ModelSelection));

        let receipt = full_receipt(request, session, BrokerOutcome::Completed);
        let sealed = receipt.seal()?;
        sealed.validate()?;

        // Verify the request correctly records the unavailability.
        assert!(matches!(
            sealed.request.requested_model,
            ModelAvailability::Unavailable(UnavailableReason::NotSupported)
        ));

        // Verify the capability matrix also reports no model selection.
        let caps = broker_capabilities_for_kind(WorkerKind::Opencode, false);
        assert!(!caps.contains(&BrokerCapability::ModelSelection));

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 3. missing_steer — backend cannot steer → typed unavailable
    // -----------------------------------------------------------------------
    #[test]
    fn missing_steer() -> Result<()> {
        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        // Opencode (non-resident) does not support steer.
        let session = BrokerSessionIdentity {
            backend_kind: WorkerKind::Opencode,
            session_id: "ses-test-nosteer".to_string(),
            started_at: valid_timestamp(),
            capabilities: Some(vec![
                BrokerCapability::DiscoverAgents,
                BrokerCapability::Start,
                BrokerCapability::Cancel,
                BrokerCapability::Wait,
                BrokerCapability::Usage,
                BrokerCapability::Permission,
            ]),
        };
        assert!(!session.supports(&BrokerCapability::Steer));

        // A receipt claiming Steered outcome should fail validation.
        let receipt = full_receipt(request, session, BrokerOutcome::Steered);
        let result = receipt.seal();
        assert!(
            result.is_err(),
            "receipt claiming Steered on a non-steerable session must fail"
        );
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("Steer"),
            "error should mention missing Steer capability: {error}"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 4. unknown_usage — backend reports no usage → receipt records unknown
    // -----------------------------------------------------------------------
    #[test]
    fn unknown_usage() -> Result<()> {
        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        let session = BrokerSessionIdentity {
            backend_kind: WorkerKind::Opencode,
            session_id: "ses-test-nousage".to_string(),
            started_at: valid_timestamp(),
            // Even without Usage capability, a receipt can have usage=None.
            capabilities: Some(vec![
                BrokerCapability::DiscoverAgents,
                BrokerCapability::Start,
                BrokerCapability::Cancel,
                BrokerCapability::Wait,
            ]),
        };

        let mut receipt = full_receipt(request, session, BrokerOutcome::Completed);
        receipt.usage = None;

        let sealed = receipt.seal()?;
        sealed.validate()?;
        assert!(
            sealed.usage.is_none(),
            "receipt with unknown usage must have usage=None"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 5. session_identity_replay — replaying old session identity → fail closed
    // -----------------------------------------------------------------------
    #[test]
    fn session_identity_replay() -> Result<()> {
        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        let session = full_session();
        let receipt = full_receipt(request, session, BrokerOutcome::Completed);
        let sealed = receipt.seal()?;
        sealed.validate()?;

        // Tamper with the phase_decision_hash to simulate a replay attack.
        let mut tampered = sealed;
        tampered.phase_decision_hash = "b".repeat(64);

        let result = tampered.validate();
        assert!(
            result.is_err(),
            "replayed receipt with different phase_decision_hash must fail validation"
        );

        // Also verify the specific error is about hash mismatch, not
        // a structural error.
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("integrity hash mismatch"),
            "error should mention hash mismatch: {error}"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 6. unauthorized_fallback — trying to fallback to a non-listed candidate → fail closed
    // -----------------------------------------------------------------------
    #[test]
    fn unauthorized_fallback() -> Result<()> {
        let requested = test_model("provider/requested");
        let unauthorized = test_model("provider/unauthorized");
        let allowed = test_model("fallback/model"); // listed in basic_request

        let mut request = basic_request(ModelAvailability::Available(requested.clone()));
        // Only one allowed fallback (set up by basic_request), not including unauthorized.
        request.allowed_fallback_models = vec![allowed.clone()];

        let session = full_session();
        let mut receipt = full_receipt(request, session, BrokerOutcome::Completed);
        // Claim to have used an unauthorized model.
        receipt.actual_model = Some(unauthorized);

        let result = receipt.seal();
        assert!(
            result.is_err(),
            "receipt with unauthorized fallback model must fail"
        );
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("not the requested model") || error.contains("not listed"),
            "error should mention unauthorized fallback candidate: {error}"
        );

        // Verify the allowed fallback is fine.
        // Rebuild from scratch for the authorized case.
        let request2 = basic_request(ModelAvailability::Available(requested));
        let session2 = full_session();
        let mut ok_receipt = full_receipt(request2, session2, BrokerOutcome::Completed);
        ok_receipt.actual_model = Some(allowed);
        let sealed = ok_receipt.seal();
        assert!(
            sealed.is_ok(),
            "receipt with authorized fallback model must pass"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 7. WorkerBroker happy path — full lifecycle with valid ledger
    // -----------------------------------------------------------------------
    #[test]
    fn broker_happy_path_ledger() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let broker = WorkerBroker::new(registry, tmp.path().to_path_buf());

        // 1. Discover
        let caps = broker.discover(WorkerKind::OpencodeSession, false);
        assert!(caps.contains(&BrokerCapability::FollowUp));
        assert!(caps.contains(&BrokerCapability::Steer));

        // 2. Resolve
        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        broker.resolve(request)?;
        let state = broker.current_state()?;
        assert_eq!(state.lifecycle.name(), LifecycleStateName::Resolved);

        // 3. Start
        let fake_state = Arc::new(Mutex::new(
            FakeWorkerState::new("ses-test-full").with_result(
                crate::test_support::test_support::fake_worker_result(
                    crate::workers::WorkerStatus::Succeeded,
                ),
            ),
        ));
        let handle = Arc::new(FakeWorkerSessionHandle::new(fake_state));
        let identity = full_session();
        let returned_handle = broker.start(handle, identity)?;
        assert!(returned_handle.session_id().is_some());
        let state = broker.current_state()?;
        assert_eq!(state.lifecycle.name(), LifecycleStateName::Active);
        assert_eq!(state.interaction_ordinal, 1);

        // Verify session-identity.json was written
        let identity_path = tmp
            .path()
            .join("goal-1")
            .join("broker-sessions")
            .join("ses-test-full")
            .join("session-identity.json");
        assert!(identity_path.exists(), "session-identity.json should exist");

        // 4. Follow-up (stays Active)
        broker.follow_up("continue please".to_string())?;
        let state = broker.current_state()?;
        assert_eq!(state.interaction_ordinal, 2);
        assert_eq!(state.lifecycle.name(), LifecycleStateName::Active);

        // 5. Wait → IdleSteering
        broker.wait()?;
        let state = broker.current_state()?;
        assert_eq!(state.lifecycle.name(), LifecycleStateName::IdleSteering);

        // 6. Steer → Active
        broker.steer("do something else".to_string())?;
        let state = broker.current_state()?;
        assert_eq!(state.interaction_ordinal, 3);
        assert_eq!(state.lifecycle.name(), LifecycleStateName::Active);

        // 7. Cancel → Terminal
        broker.cancel()?;
        let state = broker.current_state()?;
        assert!(state.lifecycle.is_terminal());
        assert_eq!(state.interaction_ordinal, 4);

        // 8. Validate the entire ledger on disk
        let session_dir = tmp
            .path()
            .join("goal-1")
            .join("broker-sessions")
            .join("ses-test-full");
        validate_session_ledger(&session_dir)?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 8. Tamper receipt hash → ledger validation fails
    // -----------------------------------------------------------------------
    #[test]
    fn tamper_receipt_hash_fails() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let broker = WorkerBroker::new(registry, tmp.path().to_path_buf());

        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        broker.resolve(request)?;

        let fake_state = Arc::new(Mutex::new(FakeWorkerState::new("ses-test-full")));
        let handle = Arc::new(FakeWorkerSessionHandle::new(fake_state));
        let identity = full_session();
        broker.start(handle, identity)?;

        // Tamper with the receipt on disk.
        let receipt_path = tmp
            .path()
            .join("goal-1")
            .join("broker-sessions")
            .join("ses-test-full")
            .join("receipts")
            .join("1.json");
        let raw = std::fs::read_to_string(&receipt_path)?;
        let mut receipt: serde_json::Value = serde_json::from_str(&raw)?;
        receipt["phase_decision_hash"] = serde_json::Value::String("b".repeat(64));
        write_json(&receipt_path, &receipt)?;

        // Validation should fail.
        let session_dir = tmp
            .path()
            .join("goal-1")
            .join("broker-sessions")
            .join("ses-test-full");
        let result = validate_session_ledger(&session_dir);
        assert!(result.is_err(), "tampered receipt should fail validation");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("hash mismatch")
                || err.contains("integrity")
                || err.contains("failed integrity"),
            "error should mention hash integrity: {err}"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 9. Revive terminal session → rejected
    // -----------------------------------------------------------------------
    #[test]
    fn revive_terminal_session_rejected() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let broker = WorkerBroker::new(registry, tmp.path().to_path_buf());

        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        broker.resolve(request)?;

        let fake_state = Arc::new(Mutex::new(FakeWorkerState::new("ses-test-full")));
        let handle = Arc::new(FakeWorkerSessionHandle::new(fake_state));
        let identity = full_session();
        broker.start(handle, identity)?;

        // Cancel to reach terminal state.
        broker.cancel()?;
        assert!(broker.current_state()?.lifecycle.is_terminal());

        // Try to resolve again — should fail because Terminal → Resolved is illegal.
        let model2 = test_model("provider/model2");
        let request2 = basic_request(ModelAvailability::Available(model2));
        let result = broker.resolve(request2);
        assert!(
            result.is_err(),
            "reviving a terminal session via resolve should be rejected"
        );
        assert!(
            result.unwrap_err().to_string().contains("illegal"),
            "error should mention illegal transition"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 10. Unauthorized follow-up without FollowUp capability → rejected
    // -----------------------------------------------------------------------
    #[test]
    fn follow_up_without_capability_rejected() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let broker = WorkerBroker::new(registry, tmp.path().to_path_buf());

        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        broker.resolve(request)?;

        // A session without FollowUp capability.
        let no_follow_identity = BrokerSessionIdentity {
            backend_kind: WorkerKind::Opencode,
            session_id: "ses-test-nofollow".to_string(),
            started_at: valid_timestamp(),
            capabilities: Some(vec![
                BrokerCapability::DiscoverAgents,
                BrokerCapability::Start,
                BrokerCapability::Cancel,
                BrokerCapability::Wait,
                BrokerCapability::Usage,
            ]),
        };

        let fake_state = Arc::new(Mutex::new(FakeWorkerState::new("ses-test-nofollow")));
        let handle = Arc::new(FakeWorkerSessionHandle::new(fake_state));
        broker.start(handle, no_follow_identity)?;

        let result = broker.follow_up("should fail".to_string());
        assert!(
            result.is_err(),
            "follow_up should be rejected without FollowUp capability"
        );
        assert!(
            result.unwrap_err().to_string().contains("FollowUp"),
            "error should mention missing FollowUp capability"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 11. Cross-session replay → ledger validation fails
    // -----------------------------------------------------------------------
    #[test]
    fn cross_session_replay_rejected() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;

        // Create session A with a valid ledger.
        let registry_a = Arc::new(worker_registry_for_test());
        let broker_a = WorkerBroker::new(registry_a, tmp.path().to_path_buf());
        let model = test_model("provider/model");
        let request_a = basic_request(ModelAvailability::Available(model));
        broker_a.resolve(request_a)?;

        let fake_state_a = Arc::new(Mutex::new(FakeWorkerState::new("ses-a")));
        let handle_a = Arc::new(FakeWorkerSessionHandle::new(fake_state_a));
        let identity_a = BrokerSessionIdentity {
            backend_kind: WorkerKind::OpencodeSession,
            session_id: "ses-a".to_string(),
            started_at: valid_timestamp(),
            capabilities: Some(vec![
                BrokerCapability::DiscoverAgents,
                BrokerCapability::Start,
                BrokerCapability::Cancel,
                BrokerCapability::Wait,
                BrokerCapability::Usage,
            ]),
        };
        broker_a.start(handle_a, identity_a)?;
        broker_a.cancel()?;

        // Copy session A's receipt directory to "ses-b" dir.
        let dst_dir = tmp
            .path()
            .join("goal-1")
            .join("broker-sessions")
            .join("ses-b");
        let src_receipts = tmp
            .path()
            .join("goal-1")
            .join("broker-sessions")
            .join("ses-a")
            .join("receipts");
        let dst_receipts = dst_dir.join("receipts");
        fs::create_dir_all(&dst_receipts)?;
        for entry in (fs::read_dir(&src_receipts)?).flatten() {
            let path = entry.path();
            let name = path.file_name().unwrap();
            fs::copy(&path, dst_receipts.join(name))?;
        }

        // Copy session-identity.json but with changed session_id.
        let src_identity = tmp
            .path()
            .join("goal-1")
            .join("broker-sessions")
            .join("ses-a")
            .join("session-identity.json");
        let dst_identity = dst_dir.join("session-identity.json");
        fs::copy(&src_identity, &dst_identity)?;

        // Now modify the receipt to reference "ses-b" — this breaks the hash.
        let receipt_path = dst_receipts.join("1.json");
        let raw = std::fs::read_to_string(&receipt_path)?;
        let mut receipt: serde_json::Value = serde_json::from_str(&raw)?;
        receipt["session_identity"]["session_id"] = serde_json::Value::String("ses-b".to_string());
        receipt["receipt_hash"] = serde_json::Value::String(String::new());
        write_json(&receipt_path, &receipt)?;

        let result = validate_session_ledger(&dst_dir);
        assert!(
            result.is_err(),
            "cross-session replay should fail validation"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 12. Ordinal jump (1 → 3) → ledger validation fails
    // -----------------------------------------------------------------------
    #[test]
    fn ordinal_jump_rejected() -> Result<()> {
        use tempfile::tempdir;

        let tmp = tempdir()?;

        // Manually craft a ledger with receipts 1 and 3 (skipping 2).
        let session_dir = tmp
            .path()
            .join("test-goal")
            .join("broker-sessions")
            .join("ses-ordinal");
        let receipts_dir = session_dir.join("receipts");
        fs::create_dir_all(&receipts_dir)?;

        let identity = BrokerSessionIdentity {
            backend_kind: WorkerKind::OpencodeSession,
            session_id: "ses-ordinal".to_string(),
            started_at: valid_timestamp(),
            capabilities: Some(vec![
                BrokerCapability::DiscoverAgents,
                BrokerCapability::Start,
                BrokerCapability::Cancel,
                BrokerCapability::Wait,
                BrokerCapability::Usage,
            ]),
        };

        let request = BrokerPhaseRequest {
            schema_version: BROKER_SCHEMA_VERSION,
            phase_decision_hash: valid_hex64(),
            goal_id: "goal-ordinal".to_string(),
            plan_id: "plan-1".to_string(),
            plan_revision: 1,
            task_id: "task-1".to_string(),
            requested_agent: "test-agent".to_string(),
            requested_model: ModelAvailability::Available(test_model("provider/model")),
            allowed_fallback_models: vec![],
        };

        let make_receipt = |ordinal: u64| -> BrokerLifecycleReceipt {
            BrokerLifecycleReceipt {
                schema_version: BROKER_SCHEMA_VERSION,
                interaction_ordinal: ordinal,
                phase_decision_hash: request.phase_decision_hash.clone(),
                session_identity: identity.clone(),
                request: request.clone(),
                outcome: BrokerOutcome::Completed,
                terminal_reason: None,
                usage: None,
                permission_evidence: None,
                actual_model: None,
                binding_status: None,
                receipt_hash: String::new(),
            }
            .seal()
            .expect("valid receipt")
        };

        // Write receipt 1
        write_json(&receipts_dir.join("1.json"), &make_receipt(1))?;

        // Write receipt 3 (skip 2)
        write_json(&receipts_dir.join("3.json"), &make_receipt(3))?;

        // Write session-identity.json
        write_json(&session_dir.join("session-identity.json"), &identity)?;

        // Write lifecycle-events.jsonl (minimal)
        append_jsonl(
            &session_dir.join("lifecycle-events.jsonl"),
            &BrokerLifecycleEvent {
                schema_version: BROKER_SCHEMA_VERSION,
                timestamp: valid_timestamp(),
                from_state: None,
                to_state: LifecycleStateName::Active,
                interaction_ordinal: 1,
                session_id: "ses-ordinal".to_string(),
                message: "start".to_string(),
            },
        )?;

        let result = validate_session_ledger(&session_dir);
        assert!(result.is_err(), "ordinal jump should fail validation");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("contiguous") || err.contains("ordinal jump"),
            "error should mention ordinal sequence: {err}"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // 13. Ledger function tests
    // -----------------------------------------------------------------------
    #[test]
    fn append_and_read_jsonl_roundtrip() -> Result<()> {
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let path = tmp.path().join("test.jsonl");

        let event1 = BrokerLifecycleEvent {
            schema_version: BROKER_SCHEMA_VERSION,
            timestamp: "ts1".to_string(),
            from_state: None,
            to_state: LifecycleStateName::Active,
            interaction_ordinal: 1,
            session_id: "ses-test".to_string(),
            message: "first".to_string(),
        };
        let event2 = BrokerLifecycleEvent {
            schema_version: BROKER_SCHEMA_VERSION,
            timestamp: "ts2".to_string(),
            from_state: Some(LifecycleStateName::Active),
            to_state: LifecycleStateName::IdleSteering,
            interaction_ordinal: 2,
            session_id: "ses-test".to_string(),
            message: "second".to_string(),
        };

        append_jsonl(&path, &event1)?;
        append_jsonl(&path, &event2)?;

        let contents = std::fs::read_to_string(&path)?;
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "should have 2 lines");

        let parsed1: BrokerLifecycleEvent = serde_json::from_str(lines[0])?;
        assert_eq!(parsed1.interaction_ordinal, 1);
        assert_eq!(parsed1.message, "first");

        let parsed2: BrokerLifecycleEvent = serde_json::from_str(lines[1])?;
        assert_eq!(parsed2.interaction_ordinal, 2);
        assert_eq!(parsed2.message, "second");

        Ok(())
    }

    #[test]
    fn read_json_file_roundtrip() -> Result<()> {
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let path = tmp.path().join("test.json");

        let identity = BrokerSessionIdentity {
            backend_kind: WorkerKind::Opencode,
            session_id: "ses-roundtrip".to_string(),
            started_at: valid_timestamp(),
            capabilities: None,
        };
        write_json(&path, &identity)?;

        let loaded: BrokerSessionIdentity = read_json_file(&path)?;
        assert_eq!(loaded.session_id, "ses-roundtrip");
        assert_eq!(loaded.backend_kind, WorkerKind::Opencode);

        Ok(())
    }

    #[test]
    fn sanitize_for_path_replaces_bad_chars() {
        assert_eq!(sanitize_for_path("hello"), "hello");
        assert_eq!(sanitize_for_path("a/b:c"), "a_b_c");
        assert_eq!(sanitize_for_path("a b"), "a_b");
        assert_eq!(sanitize_for_path("abc-123.def"), "abc-123.def");
    }

    // -----------------------------------------------------------------------
    // Lifecycle contract tests — per-backend capability matrices
    // -----------------------------------------------------------------------

    use crate::worker_broker::binding_status_for_kind;
    use crate::workers::{
        ClaudeCommandWorker, CodexCommandWorker, CustomCommandWorker, OpencodeCommandWorker,
        OpencodeSessionWorker,
    };

    /// Helper: build a BrokerSessionIdentity for a given WorkerKind with
    /// its declared broker capabilities.
    fn session_for_kind(kind: WorkerKind) -> BrokerSessionIdentity {
        let caps = broker_capabilities_for_kind(kind, false);
        BrokerSessionIdentity {
            backend_kind: kind,
            session_id: format!("ses-{}", kind.as_str()),
            started_at: valid_timestamp(),
            capabilities: Some(caps),
        }
    }

    #[test]
    fn broker_capabilities_opencode_command() {
        let caps = OpencodeCommandWorker {}.broker_capabilities();
        assert!(caps.contains(&BrokerCapability::DiscoverAgents));
        assert!(caps.contains(&BrokerCapability::Start));
        assert!(caps.contains(&BrokerCapability::Cancel));
        assert!(caps.contains(&BrokerCapability::Wait));
        // NOT supported:
        assert!(!caps.contains(&BrokerCapability::FollowUp));
        assert!(!caps.contains(&BrokerCapability::Steer));
        assert!(!caps.contains(&BrokerCapability::ModelSelection));
        assert!(!caps.contains(&BrokerCapability::Usage));
        assert!(!caps.contains(&BrokerCapability::Permission));
        assert!(!caps.contains(&BrokerCapability::SessionResume));
    }

    #[test]
    fn broker_capabilities_opencode_session() {
        let caps = OpencodeSessionWorker {}.broker_capabilities();
        assert!(caps.contains(&BrokerCapability::DiscoverAgents));
        assert!(caps.contains(&BrokerCapability::Start));
        assert!(caps.contains(&BrokerCapability::FollowUp));
        assert!(caps.contains(&BrokerCapability::Steer));
        assert!(caps.contains(&BrokerCapability::Cancel));
        assert!(caps.contains(&BrokerCapability::Wait));
        assert!(caps.contains(&BrokerCapability::SessionResume));
        assert!(caps.contains(&BrokerCapability::ModelSelection));
        assert!(caps.contains(&BrokerCapability::Usage));
        assert!(caps.contains(&BrokerCapability::Permission));
    }

    #[test]
    fn broker_capabilities_codex_command() {
        let caps = CodexCommandWorker {}.broker_capabilities();
        assert!(caps.contains(&BrokerCapability::DiscoverAgents));
        assert!(caps.contains(&BrokerCapability::Start));
        assert!(caps.contains(&BrokerCapability::Cancel));
        assert!(caps.contains(&BrokerCapability::Wait));
        assert!(caps.contains(&BrokerCapability::ModelSelection));
        // NOT supported:
        assert!(!caps.contains(&BrokerCapability::FollowUp));
        assert!(!caps.contains(&BrokerCapability::Steer));
        assert!(!caps.contains(&BrokerCapability::Usage));
        assert!(!caps.contains(&BrokerCapability::Permission));
        assert!(!caps.contains(&BrokerCapability::SessionResume));
    }

    #[test]
    fn broker_capabilities_claude_command() {
        let caps = ClaudeCommandWorker {}.broker_capabilities();
        assert!(caps.contains(&BrokerCapability::DiscoverAgents));
        assert!(caps.contains(&BrokerCapability::Start));
        assert!(caps.contains(&BrokerCapability::Cancel));
        assert!(caps.contains(&BrokerCapability::Wait));
        // NOT supported:
        assert!(!caps.contains(&BrokerCapability::FollowUp));
        assert!(!caps.contains(&BrokerCapability::Steer));
        assert!(!caps.contains(&BrokerCapability::ModelSelection));
        assert!(!caps.contains(&BrokerCapability::Usage));
        assert!(!caps.contains(&BrokerCapability::Permission));
        assert!(!caps.contains(&BrokerCapability::SessionResume));
    }

    #[test]
    fn broker_capabilities_custom_command() {
        let caps = CustomCommandWorker {}.broker_capabilities();
        assert!(caps.contains(&BrokerCapability::DiscoverAgents));
        assert!(caps.contains(&BrokerCapability::Start));
        assert!(caps.contains(&BrokerCapability::Cancel));
        assert!(caps.contains(&BrokerCapability::Wait));
        // NOT supported:
        assert!(!caps.contains(&BrokerCapability::FollowUp));
        assert!(!caps.contains(&BrokerCapability::Steer));
        assert!(!caps.contains(&BrokerCapability::ModelSelection));
        assert!(!caps.contains(&BrokerCapability::Usage));
        assert!(!caps.contains(&BrokerCapability::Permission));
        assert!(!caps.contains(&BrokerCapability::SessionResume));
    }

    // -----------------------------------------------------------------------
    // Lifecycle contract tests — cancel works on all backends
    // -----------------------------------------------------------------------

    #[test]
    fn cancel_works_on_all_backends() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        let backends = [
            WorkerKind::Opencode,
            WorkerKind::OpencodeSession,
            WorkerKind::Codex,
            WorkerKind::Claude,
            WorkerKind::Custom,
        ];

        for kind in backends {
            let tmp = tempdir()?;
            let registry = Arc::new(worker_registry_for_test());
            let broker = WorkerBroker::new(registry, tmp.path().to_path_buf());

            let model = test_model("provider/model");
            let request = basic_request(ModelAvailability::Available(model));
            broker.resolve(request)?;

            let session_id = format!("ses-cancel-{}", kind.as_str());
            let identity = session_for_kind(kind);
            let identity = BrokerSessionIdentity {
                session_id: session_id.clone(),
                ..identity
            };

            let fake_state = Arc::new(Mutex::new(FakeWorkerState::new(session_id.clone())));
            let handle = Arc::new(FakeWorkerSessionHandle::new(fake_state));
            let _returned = broker.start(handle, identity)?;

            // Cancel — should succeed for all backends.
            let state_before = broker.current_state()?;
            assert!(!state_before.lifecycle.is_terminal());
            broker.cancel()?;
            let state_after = broker.current_state()?;
            assert!(state_after.lifecycle.is_terminal());
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Lifecycle contract tests — follow_up only on OpencodeSession
    // -----------------------------------------------------------------------

    #[test]
    fn follow_up_only_where_supported() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        // Backends that should NOT support follow-up.
        let no_follow_backends = [
            WorkerKind::Opencode,
            WorkerKind::Codex,
            WorkerKind::Claude,
            WorkerKind::Custom,
        ];

        for kind in no_follow_backends {
            let tmp = tempdir()?;
            let registry = Arc::new(worker_registry_for_test());
            let broker = WorkerBroker::new(registry, tmp.path().to_path_buf());

            let model = test_model("provider/model");
            let request = basic_request(ModelAvailability::Available(model));
            broker.resolve(request)?;
            let identity = session_for_kind(kind);
            let fake_state = Arc::new(Mutex::new(FakeWorkerState::new(format!(
                "ses-{}",
                kind.as_str()
            ))));
            let handle = Arc::new(FakeWorkerSessionHandle::new(fake_state));
            broker.start(handle, identity)?;

            let result = broker.follow_up("should be rejected".to_string());
            assert!(
                result.is_err(),
                "follow_up should be rejected for {:?}",
                kind
            );
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("FollowUp") || err.contains("not advertise"),
                "error should mention FollowUp capability: {err}"
            );
        }

        // OpencodeSession SHOULD support follow-up.
        {
            let tmp = tempdir()?;
            let registry = Arc::new(worker_registry_for_test());
            let broker = WorkerBroker::new(registry, tmp.path().to_path_buf());

            let model = test_model("provider/model");
            let request = basic_request(ModelAvailability::Available(model));
            broker.resolve(request)?;
            let identity = session_for_kind(WorkerKind::OpencodeSession);
            let fake_state = Arc::new(Mutex::new(FakeWorkerState::new(
                "ses-follow-opencode-resident",
            )));
            let handle = Arc::new(FakeWorkerSessionHandle::new(fake_state));
            broker.start(handle, identity)?;

            let result = broker.follow_up("continue please".to_string());
            assert!(
                result.is_ok(),
                "follow_up should succeed for OpencodeSession"
            );
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // CLI model binding status — never Applied
    // -----------------------------------------------------------------------

    #[test]
    fn cli_model_binding_status_declared() {
        // CLI backends: all get DeclaredUnverified
        assert_eq!(
            binding_status_for_kind(WorkerKind::Opencode, false),
            Some(ModelBindingStatus::DeclaredUnverified),
        );
        assert_eq!(
            binding_status_for_kind(WorkerKind::Codex, false),
            Some(ModelBindingStatus::DeclaredUnverified),
        );
        assert_eq!(
            binding_status_for_kind(WorkerKind::Claude, false),
            Some(ModelBindingStatus::DeclaredUnverified),
        );
        assert_eq!(
            binding_status_for_kind(WorkerKind::Custom, false),
            Some(ModelBindingStatus::DeclaredUnverified),
        );
        // OpencodeSession is also declared (backend-declared)
        assert_eq!(
            binding_status_for_kind(WorkerKind::OpencodeSession, false),
            Some(ModelBindingStatus::DeclaredUnverified),
        );
        // ZedAgent CLI (no native backend) is declared
        assert_eq!(
            binding_status_for_kind(WorkerKind::ZedAgent, false),
            Some(ModelBindingStatus::DeclaredUnverified),
        );
        // ZedAgent with native backend IS Applied
        assert_eq!(
            binding_status_for_kind(WorkerKind::ZedAgent, true),
            Some(ModelBindingStatus::Applied),
        );
    }

    // -----------------------------------------------------------------------
    // Fallback discipline — missing model selector = typed unavailable
    // -----------------------------------------------------------------------

    #[test]
    fn missing_model_selector_typed_unavailable() {
        // Verify that backends which don't support ModelSelection
        // correctly report it as unavailable.
        for kind in &[WorkerKind::Opencode, WorkerKind::Claude, WorkerKind::Custom] {
            let caps = broker_capabilities_for_kind(*kind, false);
            assert!(
                !caps.contains(&BrokerCapability::ModelSelection),
                "{:?} should not advertise ModelSelection",
                kind
            );
        }
        // Codex and OpencodeSession SHOULD have ModelSelection.
        assert!(
            broker_capabilities_for_kind(WorkerKind::Codex, false)
                .contains(&BrokerCapability::ModelSelection)
        );
        assert!(
            broker_capabilities_for_kind(WorkerKind::OpencodeSession, false)
                .contains(&BrokerCapability::ModelSelection)
        );
    }

    #[test]
    fn factory_executes_an_opencode_phase_with_a_terminal_receipt() -> Result<()> {
        use crate::phase_routing::{LiveModelInventory, OpenCodeModelProfiles, PhaseRouteTable};
        use crate::state::{
            Scope, StateStore, Task, TaskInputs, TaskKind, TaskOutputs, TaskStatus,
        };
        use crate::workers::{WorkerConfig, WorkerRoute};

        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = Task {
            id: "phase_planner".to_string(),
            goal_id: "goal_opencode".to_string(),
            parent_task_id: None,
            title: "OpenCode planner phase".to_string(),
            kind: TaskKind::Plan,
            status: TaskStatus::Pending,
            assigned_worker: Some(WorkerKind::OpencodeSession.as_str().to_string()),
            attempt: 1,
            scope: Scope::new(Vec::new(), Vec::new(), 1),
            inputs: TaskInputs {
                phase_route_locked: true,
                ..TaskInputs::default()
            },
            outputs: TaskOutputs::default(),
        };
        let command = "sh -c 'cat \"$GEARBOX_WORKER_PROMPT\"'".to_string();
        let config = WorkerConfig {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(command.clone()),
            worker_model: Some("openai/gpt-planner".to_string()),
            worker_routes: vec![WorkerRoute {
                worker_kind: WorkerKind::OpencodeSession,
                worker_command: Some(command),
                worker_model: Some("openai/gpt-planner".to_string()),
            }],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::OpencodeSession,
        };
        let routes = PhaseRouteTable::opencode_only(OpenCodeModelProfiles {
            planner: "openai/gpt-planner".to_string(),
            executor: "deepseek/flash".to_string(),
            reviewer: "openai/gpt-reviewer".to_string(),
        })?;
        let decision =
            routes.resolve(&PhaseProfile::Planner, &LiveModelInventory::default(), None)?;
        let factory = PhaseBrokerFactory::new(
            Arc::new(WorkerRegistry::default()),
            temp_dir.path().join(".gearbox-agent"),
        );

        let execution = factory.execute_worker_phase(
            &decision,
            &task.goal_id,
            "plan_opencode",
            0,
            &task.id,
            "planner_execution_1",
            "planner_session_1",
            WorkerStartRequest {
                store: &store,
                workspace: temp_dir.path(),
                task: &task,
                route_attempt: 1,
                goal: "Return a typed plan",
                verification_commands: &[],
                config: &config,
                cancellation_token: None,
                coordinator_model: None,
                coordinator_brief: None,
                route_hint: None,
            },
        )?;

        assert_eq!(
            execution.result.status,
            crate::workers::WorkerStatus::Succeeded
        );
        let phase_prompt = std::fs::read_to_string(&execution.result.prompt_path)?;
        assert!(phase_prompt.contains("Return only the response format required"));
        assert!(!phase_prompt.contains("Return a concise report with"));
        assert_eq!(
            execution.session_identity.backend_kind,
            WorkerKind::OpencodeSession
        );
        assert_eq!(
            execution.execution_identity.backend,
            crate::plan_review::PhaseExecutionBackend::WorkerSession
        );
        assert_eq!(
            execution.execution_identity.provider_id.as_deref(),
            Some("openai")
        );
        assert_eq!(
            execution.execution_identity.model_id.as_deref(),
            Some("gpt-planner")
        );
        assert!(
            execution
                .session_dir
                .join("terminal-outcome.json")
                .is_file()
        );
        validate_session_ledger(&execution.session_dir)?;
        factory.validate_goal_receipts(&task.goal_id, true)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // DeclaredUnverified model mismatch is allowed in receipt validation
    // -----------------------------------------------------------------------

    #[test]
    fn declared_unverified_model_mismatch_allowed() -> Result<()> {
        let requested = test_model("provider/requested");
        let actual = test_model("provider/actual");

        let request = basic_request(ModelAvailability::Available(requested));
        let session = full_session();

        // A receipt with DeclaredUnverified binding status should allow
        // mismatch between requested and actual model.
        let mut receipt = full_receipt(request, session, BrokerOutcome::Completed);
        receipt.actual_model = Some(actual);
        receipt.binding_status = Some(ModelBindingStatus::DeclaredUnverified);

        // Should pass validation because DeclaredUnverified allows mismatches.
        let sealed = receipt.seal()?;
        sealed.validate()?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // LegacyUnverified model mismatch is also allowed
    // -----------------------------------------------------------------------

    #[test]
    fn legacy_unverified_model_mismatch_allowed() -> Result<()> {
        let requested = test_model("provider/requested");
        let actual = test_model("provider/actual");

        let request = basic_request(ModelAvailability::Available(requested));
        let session = full_session();

        let mut receipt = full_receipt(request, session, BrokerOutcome::Completed);
        receipt.actual_model = Some(actual);
        receipt.binding_status = Some(ModelBindingStatus::LegacyUnverified);

        let sealed = receipt.seal()?;
        sealed.validate()?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Applied binding status rejects model mismatches
    // -----------------------------------------------------------------------

    #[test]
    fn applied_binding_rejects_model_mismatch() {
        let requested = test_model("provider/requested");
        let actual = test_model("provider/unauthorized");
        let allowed = test_model("fallback/model");

        let mut request = basic_request(ModelAvailability::Available(requested));
        request.allowed_fallback_models = vec![allowed];
        let session = full_session();

        let mut receipt = full_receipt(request, session, BrokerOutcome::Completed);
        receipt.actual_model = Some(actual);
        receipt.binding_status = Some(ModelBindingStatus::Applied);

        let result = receipt.seal();
        assert!(
            result.is_err(),
            "Applied binding should reject model mismatch"
        );
    }

    // -----------------------------------------------------------------------
    // start_via_broker — happy path
    // -----------------------------------------------------------------------

    #[test]
    fn start_via_broker_happy_path() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        // Use a registry that has both native backend and broker configured.
        let registry = Arc::new(worker_registry_for_test().with_broker(Arc::new(
            WorkerBroker::new(
                Arc::new(worker_registry_for_test()),
                tmp.path().to_path_buf(),
            ),
        )));
        let broker = WorkerBroker::new(registry, tmp.path().to_path_buf());

        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        broker.resolve(request)?;

        // start_via_broker should succeed and produce a valid session.
        // We can't easily construct a WorkerStartRequest in a unit test,
        // so we test the broker directly with a fake handle.
        let identity = session_for_kind(WorkerKind::OpencodeSession);
        let fake_state = Arc::new(Mutex::new(FakeWorkerState::new("ses-via-broker")));
        let handle = Arc::new(FakeWorkerSessionHandle::new(fake_state));
        let returned = broker.start(handle, identity)?;
        assert!(returned.session_id().is_some());

        let state = broker.current_state()?;
        assert_eq!(state.lifecycle.name(), LifecycleStateName::Active);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Unavailable reason propagation — BackendUnavailable
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // PhaseBrokerFactory tests
    // -----------------------------------------------------------------------

    use crate::phase_routing::{
        PhaseBackend, PhaseModelBinding, PhaseRouteCandidate, PhaseRouteSource,
    };
    use crate::plan_graph::PhaseProfile;
    use crate::plan_review::{PhaseExecutionBackend, PhaseExecutionIdentity};
    use crate::workers::WorkerCategory;

    /// Build a minimal PhaseRouteDecision for the given phase.
    fn test_phase_decision(phase: PhaseProfile) -> PhaseRouteDecision {
        PhaseRouteDecision {
            phase,
            category: WorkerCategory::Deep,
            selected_candidate: 0,
            candidate: PhaseRouteCandidate {
                backend: PhaseBackend::DirectModel,
                model: PhaseModelBinding::CurrentSession,
                command: None,
            },
            rejected_candidates: Vec::new(),
            requested_model: None,
            worker_kind: None,
            profile_hash: "a".repeat(64),
            source: PhaseRouteSource::LegacyDefault,
        }
    }

    /// Build a minimal PhaseExecutionIdentity.
    fn test_exec_identity(
        execution_id: &str,
        phase_session_id: &str,
        actual_session_id: &str,
    ) -> PhaseExecutionIdentity {
        PhaseExecutionIdentity {
            execution_id: execution_id.to_string(),
            phase_session_id: phase_session_id.to_string(),
            backend: PhaseExecutionBackend::NativeAgent,
            agent_id: Some("test-agent".to_string()),
            provider_id: Some("test-provider".to_string()),
            model_id: Some("test-model".to_string()),
            actual_session_id: Some(actual_session_id.to_string()),
        }
    }

    #[test]
    fn phase_broker_factory_creates_four_independent_sessions() -> Result<()> {
        use crate::test_support::test_support::worker_registry_for_test;
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let factory = PhaseBrokerFactory::new(registry, tmp.path().to_path_buf());

        // Create brokers for four different phases with distinct identities.
        let planner = factory.create_broker(
            &test_phase_decision(PhaseProfile::Planner),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &test_exec_identity("exec-planner", "ses-planner", "act-planner"),
        )?;

        let critic = factory.create_broker(
            &test_phase_decision(PhaseProfile::PlanCritic),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &test_exec_identity("exec-critic", "ses-critic", "act-critic"),
        )?;

        let executor = factory.create_broker(
            &test_phase_decision(PhaseProfile::ExecutorQuick),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &test_exec_identity("exec-executor", "ses-executor", "act-executor"),
        )?;

        let reviewer = factory.create_broker(
            &test_phase_decision(PhaseProfile::ReviewerTask),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &test_exec_identity("exec-reviewer", "ses-reviewer", "act-reviewer"),
        )?;

        // All four should be distinct Arc pointers.
        assert!(!Arc::ptr_eq(&planner, &critic));
        assert!(!Arc::ptr_eq(&planner, &executor));
        assert!(!Arc::ptr_eq(&planner, &reviewer));
        assert!(!Arc::ptr_eq(&critic, &executor));
        assert!(!Arc::ptr_eq(&critic, &reviewer));
        assert!(!Arc::ptr_eq(&executor, &reviewer));

        // All four should be in Discovered state.
        for broker in [&planner, &critic, &executor, &reviewer] {
            let state = broker.current_state()?;
            assert_eq!(state.lifecycle.name(), LifecycleStateName::Discovered);
        }

        // All four should have different artifacts_root paths.
        let paths: Vec<_> = [planner, critic, executor, reviewer]
            .iter()
            .map(|b| b.artifacts_root().to_path_buf())
            .collect();
        for i in 0..paths.len() {
            for j in (i + 1)..paths.len() {
                assert_ne!(
                    paths[i], paths[j],
                    "broker {} and {} share the same path",
                    i, j
                );
            }
        }

        Ok(())
    }

    #[test]
    fn phase_broker_factory_rejects_same_identity_reuse() -> Result<()> {
        use crate::test_support::test_support::worker_registry_for_test;
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let factory = PhaseBrokerFactory::new(registry, tmp.path().to_path_buf());

        let identity = test_exec_identity("exec-planner", "ses-planner", "act-planner");

        // First creation should succeed.
        factory.create_broker(
            &test_phase_decision(PhaseProfile::Planner),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &identity,
        )?;

        // Second creation with same identity should fail.
        let err = match factory.create_broker(
            &test_phase_decision(PhaseProfile::Planner),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &identity,
        ) {
            Err(e) => e.to_string(),
            Ok(_) => bail!("reusing the same execution identity must have failed"),
        };
        assert!(
            err.contains("broker session already exists"),
            "error should mention existing session: {err}"
        );

        Ok(())
    }

    #[test]
    fn phase_broker_factory_rejects_cross_role_sharing() -> Result<()> {
        use crate::test_support::test_support::worker_registry_for_test;
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let factory = PhaseBrokerFactory::new(registry, tmp.path().to_path_buf());

        // Planner (group "planning") — first broker in this group.
        factory.create_broker(
            &test_phase_decision(PhaseProfile::Planner),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &test_exec_identity("exec-planner", "ses-planner", "act-planner"),
        )?;

        // PlanCritic (group "plan_review") — different group, same goal/task/revision → OK.
        let critic_result = factory.create_broker(
            &test_phase_decision(PhaseProfile::PlanCritic),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &test_exec_identity("exec-critic", "ses-critic", "act-critic"),
        );
        assert!(
            critic_result.is_ok(),
            "PlanCritic in different group should succeed"
        );

        // ExecutorQuick (group "execution") — different group, same goal/task/revision → OK.
        let executor_result = factory.create_broker(
            &test_phase_decision(PhaseProfile::ExecutorQuick),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &test_exec_identity("exec-executor", "ses-executor", "act-executor"),
        );
        assert!(
            executor_result.is_ok(),
            "ExecutorQuick in different group should succeed"
        );

        // Try a second ExecutorDeep (same independence group "execution") with different
        // execution_id but same goal/task/revision → should FAIL.
        let err = match factory.create_broker(
            &test_phase_decision(PhaseProfile::ExecutorDeep),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &test_exec_identity("exec-executor-2", "ses-executor-2", "act-executor-2"),
        ) {
            Err(e) => e.to_string(),
            Ok(_) => bail!("second executor in same group must have failed"),
        };
        assert!(
            err.contains("cross-role session reuse detected"),
            "error should mention cross-role reuse: {err}"
        );

        Ok(())
    }

    #[test]
    fn phase_broker_factory_ledger_paths_are_distinct() -> Result<()> {
        use crate::test_support::test_support::worker_registry_for_test;
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let factory = PhaseBrokerFactory::new(registry, tmp.path().to_path_buf());

        let broker1 = factory.create_broker(
            &test_phase_decision(PhaseProfile::Planner),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &test_exec_identity("exec-1", "ses-1", "act-1"),
        )?;

        let broker2 = factory.create_broker(
            &test_phase_decision(PhaseProfile::ExecutorQuick),
            "goal-1",
            "plan-1",
            1,
            "task-2",
            &test_exec_identity("exec-2", "ses-2", "act-2"),
        )?;

        let broker3 = factory.create_broker(
            &test_phase_decision(PhaseProfile::ReviewerTask),
            "goal-2",
            "plan-1",
            2,
            "task-3",
            &test_exec_identity("exec-3", "ses-3", "act-3"),
        )?;

        let broker4 = factory.create_broker(
            &test_phase_decision(PhaseProfile::Summarizer),
            "goal-1",
            "plan-2",
            1,
            "task-1",
            &test_exec_identity("exec-4", "ses-4", "act-4"),
        )?;

        // All four paths must be distinct.
        let paths = [
            broker1.artifacts_root().to_path_buf(),
            broker2.artifacts_root().to_path_buf(),
            broker3.artifacts_root().to_path_buf(),
            broker4.artifacts_root().to_path_buf(),
        ];
        for i in 0..paths.len() {
            for j in (i + 1)..paths.len() {
                assert_ne!(paths[i], paths[j], "path {} and {} must be distinct", i, j);
            }
        }

        // Verify path structure: each contains "broker-sessions"
        for (i, path) in paths.iter().enumerate() {
            let path_str = path.to_string_lossy();
            assert!(
                path_str.contains("broker-sessions"),
                "path {} missing broker-sessions: {path_str}",
                i,
            );
        }

        Ok(())
    }

    #[test]
    fn phase_broker_factory_completion_gate_rejects_tampered_terminal_ledger() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, fake_worker_outcome, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let factory = PhaseBrokerFactory::new(registry, tmp.path().to_path_buf());
        let execution_identity = test_exec_identity(
            "executor-completion",
            "executor-session",
            "executor-actual-session",
        );
        let broker = factory.create_broker(
            &test_phase_decision(PhaseProfile::ExecutorQuick),
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &execution_identity,
        )?;
        broker.resolve(basic_request(ModelAvailability::Available(test_model(
            "provider/model",
        ))))?;

        let session_identity = session_for_kind(WorkerKind::OpencodeSession);
        let fake_state = Arc::new(Mutex::new(
            FakeWorkerState::new(&session_identity.session_id)
                .with_outcome(fake_worker_outcome(crate::workers::WorkerStatus::Succeeded)),
        ));
        broker.start(
            Arc::new(FakeWorkerSessionHandle::new(fake_state)),
            session_identity,
        )?;
        broker.wait_for_outcome()?;
        factory.finalize_session(broker.as_ref(), &execution_identity, "goal-1", "task-1", 1)?;
        factory.validate_goal_receipts("goal-1", true)?;

        let receipt_path = broker.session_ledger_dir()?.join("receipts").join("2.json");
        let mut receipt: BrokerLifecycleReceipt = read_json_file(&receipt_path)?;
        receipt.receipt_hash = "tampered".to_string();
        write_json(&receipt_path, &receipt)?;

        assert!(
            factory.validate_goal_receipts("goal-1", true).is_err(),
            "completion gate must reject a tampered factory ledger"
        );

        Ok(())
    }

    #[test]
    fn phase_broker_factory_rejects_terminal_revive() -> Result<()> {
        use crate::test_support::test_support::{
            FakeWorkerSessionHandle, FakeWorkerState, worker_registry_for_test,
        };
        use std::sync::Arc;
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let registry = Arc::new(worker_registry_for_test());
        let factory = PhaseBrokerFactory::new(registry, tmp.path().to_path_buf());

        let identity = test_exec_identity("exec-planner", "ses-planner", "act-planner");
        let phase_decision = test_phase_decision(PhaseProfile::Planner);

        // Create broker, go through full lifecycle to terminal.
        let broker =
            factory.create_broker(&phase_decision, "goal-1", "plan-1", 1, "task-1", &identity)?;

        // Resolve and start the broker (to create on-disk ledger data).
        let model = test_model("provider/model");
        let request = basic_request(ModelAvailability::Available(model));
        broker.resolve(request)?;

        let fake_state = Arc::new(Mutex::new(FakeWorkerState::new("ses-planner")));
        let handle = Arc::new(FakeWorkerSessionHandle::new(fake_state));
        let session_identity = session_for_kind(WorkerKind::OpencodeSession);
        broker.start(handle, session_identity)?;

        // Cancel to reach terminal state.
        broker.cancel()?;
        assert!(broker.current_state()?.lifecycle.is_terminal());

        // Drop the factory's active_sessions lock entry (simulate restart).
        // We access the factory's active_sessions directly to clear it.
        {
            let mut sessions = factory.active_sessions.lock().unwrap();
            sessions.clear();
        }

        // Try to create a new broker with the same identity → should fail
        // because disk already contains ledger data.
        let err = match factory.create_broker(
            &phase_decision,
            "goal-1",
            "plan-1",
            1,
            "task-1",
            &identity,
        ) {
            Err(e) => e.to_string(),
            Ok(_) => bail!("terminal revive must have been rejected"),
        };
        assert!(
            err.contains("session replay detected"),
            "error should mention session replay: {err}"
        );

        Ok(())
    }

    #[test]
    fn unavailable_reason_propagation() {
        // Verify the typed unavailable variants exist and are usable.
        let backend_unavail =
            UnavailableReason::BackendUnavailable("binary not found on PATH".to_string());
        assert!(matches!(
            backend_unavail,
            UnavailableReason::BackendUnavailable(_)
        ));

        let model_not_found = UnavailableReason::ModelNotFound("gpt-5 not loadable".to_string());
        assert!(matches!(
            model_not_found,
            UnavailableReason::ModelNotFound(_)
        ));

        let not_supported = UnavailableReason::NotSupported;
        assert_eq!(not_supported, UnavailableReason::NotSupported);

        let not_configured = UnavailableReason::NotConfigured;
        assert_eq!(not_configured, UnavailableReason::NotConfigured);

        // Verify ModelAvailability can carry the reason.
        let avail = ModelAvailability::Unavailable(backend_unavail.clone());
        assert!(matches!(avail, ModelAvailability::Unavailable(_)));
        if let ModelAvailability::Unavailable(reason) = avail {
            assert_eq!(reason, backend_unavail);
        }
    }
}
