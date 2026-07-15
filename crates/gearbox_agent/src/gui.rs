use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fs,
    io::{Read, Seek},
    path::Path,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    phase_routing::{PhaseBackend, PhaseModelBinding, PhaseRouteReceipt},
    plan_graph::{PlanGraph, PlanTaskBudget},
    plan_review::PlanApprovalState,
    plan_review::PlanVerifierReport,
    state::{
        CriterionEvidenceStatus, Event, EventKind, FinalVerificationWaveReceipt, Goal,
        GoalBudgetLedger, ObjectiveGraph, PlanNodeRun, PlanNodeRunLedger, PlanNodeRunStatus,
        RepositoryObservationReceipt, RepositoryObservationStatus, ReviewEpochBundle, StateStore,
    },
    task_manager::{TaskManager, TaskManagerSnapshot},
};

pub const GEAR_GUI_SNAPSHOT_SCHEMA_VERSION: u32 = 2;
pub const GEAR_GUI_EVENT_BUFFER_CAPACITY: usize = 256;
pub const GEAR_GUI_WORKER_DISPATCH_CAPACITY: usize = 64;
pub const GEAR_GUI_REVIEW_QUEUE_CAPACITY: usize = 16;
pub const GEAR_GUI_TIMELINE_CAPACITY: usize = 500;
pub const GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES: usize = 64 * 1024;
pub const GEAR_GUI_TERMINAL_SUMMARY_BYTES: usize = 16 * 1024;
pub const GEAR_GUI_MAX_CONVERSATION_SUMMARIES_PER_EPOCH: usize = 12;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GearRuntimeEventClass {
    Critical,
    Milestone,
    Telemetry,
    ConversationSummary,
}

impl GearRuntimeEventClass {
    fn is_lossless(self) -> bool {
        matches!(self, Self::Critical | Self::ConversationSummary)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeEventEnvelope {
    pub sequence: u64,
    pub class: GearRuntimeEventClass,
    pub semantic_key: String,
    pub session_id: String,
    pub objective_id: Option<String>,
    pub goal_id: Option<String>,
    pub task_id: Option<String>,
    pub run_epoch: Option<u64>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

impl GearRuntimeEventEnvelope {
    pub fn bounded_message(message: impl Into<String>, max_bytes: usize) -> String {
        let message = message.into();
        if message.len() <= max_bytes {
            return message;
        }
        let mut end = max_bytes.saturating_sub("\n[truncated]".len());
        while end > 0 && !message.is_char_boundary(end) {
            end -= 1;
        }
        let mut bounded = message[..end].to_string();
        bounded.push_str("\n[truncated]");
        bounded
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeHealth {
    pub last_activity_at: Option<String>,
    pub dropped_telemetry: u64,
    pub coalesced_telemetry: u64,
    pub refresh_required: bool,
    pub owned_child_processes: usize,
    pub rust_work_state: Option<String>,
    pub last_error: Option<String>,
    #[serde(default)]
    pub processes: GearRuntimeProcessHealth,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeProcessHealth {
    pub cargo: usize,
    pub rustc: usize,
    pub rust_analyzer: usize,
    pub opencode: usize,
    pub codex: usize,
    pub rust_processes: usize,
    pub rust_process_over_limit: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeLifecycle {
    pub objective_status: Option<String>,
    pub goal_status: Option<String>,
    pub continuation_status: Option<String>,
    pub phase: Option<String>,
    pub stop_reason: Option<String>,
    pub recovery_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intensity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership_delegated: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership_worker_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership_worker_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership_route_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phase_routes: Vec<GearRuntimePhaseRouteSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub broker_sessions: Vec<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub phase_route_errors: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_plan_revision: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_plan_source: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub visible_plan_is_candidate: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_preflight: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_rollback: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_final_verification: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_must_have: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_must_not_have: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_topology_lock: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_final_acceptance: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_assumptions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_findings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_decisions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_open_questions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_milestones: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_acceptance_checklist: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_artifact_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_artifact_status: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub plan_reused: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_quality_status: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_quality_findings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_quality_artifact: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_revision_diff: Option<GearRuntimePlanRevisionDiff>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_coverage: Option<GearRuntimePlanCoverageSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_approval_status: Option<String>,
    #[serde(default)]
    pub plan_revisions_used: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_critic_receipt_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_approval_artifact: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_final_receipt_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_final_receipt_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_final_receipt_artifact: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plan_final_checks: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub rollback_pending: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_artifact: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_goal: Option<GearRuntimeNextGoalSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimePlanCoverageSummary {
    pub work_orders_total: usize,
    pub work_orders_completed: usize,
    pub acceptance_total: usize,
    pub acceptance_satisfied: usize,
    pub qa_total: usize,
    pub qa_satisfied: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimePlanRevisionDiff {
    pub from_revision: usize,
    pub to_revision: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added_tasks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_tasks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_tasks: Vec<String>,
    pub objective_changed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeNextGoalSummary {
    pub decision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_objective: Option<String>,
    pub answerable_now: bool,
    #[serde(default)]
    pub acceptance_signals: Vec<String>,
    #[serde(default)]
    pub required_questions: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimePhaseRouteSummary {
    pub ordinal: usize,
    pub phase: String,
    pub backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_kind: Option<String>,
    pub selected_candidate: usize,
    pub fallback_count: usize,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_path: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeBudgetSummary {
    pub calls_reserved: Option<u64>,
    pub calls_used: Option<u64>,
    pub tokens_reserved: Option<u64>,
    pub tokens_used: Option<u64>,
    pub cost_micros_reserved: Option<u64>,
    pub cost_micros_used: Option<u64>,
    pub unknown_usage_calls: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeGoalSummary {
    pub id: String,
    pub title: String,
    pub status: String,
    pub current_task_id: Option<String>,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intensity: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeGoalHistorySummary {
    pub goal_id: String,
    pub epoch_id: String,
    pub status: String,
    pub request: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_goal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_epoch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_report_path: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeObjectiveSummary {
    pub id: String,
    pub status: String,
    pub active_goal_id: Option<String>,
    pub stop_reason: Option<String>,
    pub consecutive_failures: usize,
    pub consecutive_no_progress: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub goal_history: Vec<GearRuntimeGoalHistorySummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeReviewSummary {
    pub status: String,
    pub epoch_events: usize,
    pub latest_event: Option<String>,
    pub plan_revision: Option<usize>,
    pub bundle_complete: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub critic_findings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub oracle_findings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oracle_revision_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub critic_decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oracle_decision: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blockers: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeRecoverySummary {
    pub continuation_status: Option<String>,
    pub resume_count: usize,
    pub stuck_reason: Option<String>,
    pub last_progress_marker: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeFeedbackSummary {
    pub tool_calls: usize,
    pub permission_events: usize,
    pub task_events: usize,
    pub worker_errors: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeFeedbackEvent {
    pub task_id: String,
    pub kind: String,
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimePlanStepSummary {
    pub step_id: String,
    pub action: String,
    pub expected_observation: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimePlanSessionSummary {
    pub attempt: usize,
    pub worker_task_id: String,
    pub worker_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    pub session_id: String,
    pub status: String,
    pub started_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_reason: Option<String>,
    #[serde(default)]
    pub route_fallback_count: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimePlanTaskSummary {
    pub task_id: String,
    pub title: String,
    #[serde(default)]
    pub goal: String,
    #[serde(default)]
    pub deliverable: String,
    #[serde(default)]
    pub rationale: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub approach: Vec<String>,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_worker_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_worker_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_hint: Option<String>,
    pub status: String,
    #[serde(default)]
    pub contract_status: String,
    pub dependencies: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preconditions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub already_in_working_tree: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub still_needed: Vec<String>,
    pub parallel_wave: usize,
    pub current: bool,
    #[serde(default)]
    pub attempt: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_session_id: Option<String>,
    /// Durable PlanNodeSessionBinding lifecycle projection. `updated_at` is
    /// the terminal timestamp when the binding is no longer active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_session_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_session_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_session_updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_session_ended_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_session_elapsed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_session_history: Vec<GearRuntimePlanSessionSummary>,
    #[serde(default)]
    pub worker_session_attempt_count: usize,
    #[serde(default)]
    pub worker_session_fallback_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_session_elapsed_total_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_brief_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preflight_path: Option<String>,
    #[serde(default)]
    pub preflight_satisfied: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preflight_checks: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_result_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_outcome_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_last_message_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_last_message_excerpt: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_commands_run: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_known_failures: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_next_steps: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_plan_gap: Option<String>,
    #[serde(default)]
    pub worker_decision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_decision_reason: Option<String>,
    #[serde(default)]
    pub worker_evidence_quality: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub red_evidence_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub green_evidence_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_evidence_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_boundary_evidence_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_boundary_satisfied: Option<bool>,
    #[serde(default)]
    pub size_tier: String,
    #[serde(default)]
    pub risk_tier: String,
    #[serde(default)]
    pub commit_boundary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_message: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub must_do: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rollback: Vec<String>,
    #[serde(default)]
    pub budget: PlanTaskBudget,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub execution_steps: Vec<GearRuntimePlanStepSummary>,
    #[serde(default)]
    pub execution_steps_evidence_required: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub must_not_do: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completion_predicates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_artifacts: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write_scope: Vec<String>,
    #[serde(default)]
    pub max_files_changed: usize,
    #[serde(default)]
    pub test_strategy: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_commands: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub qa_scenarios: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GearRuntimeSnapshot {
    pub schema_version: u32,
    pub sequence: u64,
    pub workspace: String,
    pub session_id: String,
    pub objective_id: Option<String>,
    pub goal_id: Option<String>,
    pub epoch_id: Option<String>,
    pub objective: Option<GearRuntimeObjectiveSummary>,
    pub goal: Option<GearRuntimeGoalSummary>,
    pub request_summary: String,
    pub lifecycle: GearRuntimeLifecycle,
    pub budget: GearRuntimeBudgetSummary,
    pub review: Option<GearRuntimeReviewSummary>,
    pub recovery: GearRuntimeRecoverySummary,
    pub feedback: GearRuntimeFeedbackSummary,
    pub feedback_events: Vec<GearRuntimeFeedbackEvent>,
    #[serde(default)]
    pub plan_tasks: Vec<GearRuntimePlanTaskSummary>,
    #[serde(default)]
    pub plan_total: usize,
    #[serde(default)]
    pub plan_completed: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_plan_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_plan_task_title: Option<String>,
    #[serde(default)]
    pub plan_execution_mode: String,
    pub task_manager: Option<TaskManagerSnapshot>,
    pub timeline: Vec<GearRuntimeEventEnvelope>,
    pub health: GearRuntimeHealth,
}

impl GearRuntimeSnapshot {
    pub fn from_store(
        store: &StateStore,
        workspace: impl Into<String>,
        session_id: impl Into<String>,
        task_manager: Option<TaskManagerSnapshot>,
    ) -> anyhow::Result<Self> {
        let workspace = workspace.into();
        let session_id = session_id.into();
        let continuation = store.read_continuation_state_for_session(&session_id)?;
        let timeline = read_timeline(store, &session_id);
        let ownership = ownership_from_timeline(&timeline);
        let session_goal_id = store
            .read_session(&session_id)?
            .map(|session| session.current_goal_id)
            .filter(|goal_id| !goal_id.trim().is_empty());
        let goal_id = continuation
            .as_ref()
            .map(|state| state.goal_id.clone())
            .or(session_goal_id)
            .or_else(|| {
                timeline
                    .iter()
                    .rev()
                    .find_map(|event| event.goal_id.clone())
            });
        let goal = goal_id
            .as_deref()
            .map(|id| store.read_goal(id))
            .transpose()?
            .flatten();
        let objective = goal_id
            .as_deref()
            .and_then(|id| find_objective_graph(store.objectives_dir().as_path(), id))
            .map(|graph| {
                (
                    graph.objective_id.clone(),
                    GearRuntimeObjectiveSummary {
                        id: graph.objective_id.clone(),
                        status: format!("{:?}", graph.status),
                        active_goal_id: graph.active_goal_id.clone(),
                        stop_reason: graph.stop_reason.clone(),
                        consecutive_failures: graph.consecutive_failures,
                        consecutive_no_progress: graph.consecutive_no_progress,
                        goal_history: objective_goal_history(&graph),
                    },
                    graph,
                )
            });
        let objective_id = objective.as_ref().map(|(id, _, _)| id.clone());
        let objective_summary = objective.as_ref().map(|(_, summary, _)| summary.clone());
        let graph = objective.as_ref().map(|(_, _, graph)| graph);
        let canonical_plan = goal
            .as_ref()
            .and_then(|goal| store.read_plan_graph(&goal.id).ok().flatten());
        let plan = goal.as_ref().and_then(|goal| visible_plan(store, &goal.id));
        let visible_plan_is_candidate = plan.as_ref().is_some_and(|visible| {
            canonical_plan
                .as_ref()
                .is_none_or(|canonical| visible.plan_hash != canonical.plan_hash)
        });
        let plan_node_runs = goal
            .as_ref()
            .and_then(|goal| store.read_plan_node_runs(&goal.id).ok().flatten());
        let plan_node_runs = matching_plan_node_runs(plan_node_runs, plan.as_ref());
        let plan_tasks =
            plan_task_summaries(store, goal.as_ref(), plan.as_ref(), plan_node_runs.as_ref());
        let (plan_total, plan_completed, next_plan_task_id, next_plan_task_title) =
            plan_progress_from_graph(plan.as_ref(), plan_node_runs.as_ref());
        let epoch_id = graph
            .and_then(|graph| {
                graph
                    .nodes
                    .iter()
                    .find(|node| Some(&node.goal_id) == goal_id.as_ref())
            })
            .map(|node| node.epoch_id.clone());
        let goal_budget = goal_id
            .as_deref()
            .map(|id| store.read_goal_budget_ledger(id))
            .transpose()?;
        let budget = goal_budget_summary(goal_budget.as_ref());
        let task_manager = match task_manager {
            Some(task_manager) => Some(task_manager),
            None => TaskManager::durable_snapshot(store, Some(&session_id))?,
        };
        let feedback = feedback_summary(store, task_manager.as_ref());
        let feedback_events = feedback_events(store, task_manager.as_ref());
        let epoch_events = goal_id
            .as_deref()
            .map(|id| store.read_goal_epoch_events(id))
            .transpose()?
            .unwrap_or_default();
        let review = review_summary(
            store,
            goal.as_ref(),
            plan.as_ref(),
            epoch_id.as_deref(),
            &epoch_events,
        );
        let recovery =
            continuation
                .as_ref()
                .map_or_else(GearRuntimeRecoverySummary::default, |state| {
                    GearRuntimeRecoverySummary {
                        continuation_status: Some(format!("{:?}", state.status)),
                        resume_count: state.resume_count,
                        stuck_reason: state.stuck_reason.clone(),
                        last_progress_marker: state.last_progress_marker.clone(),
                    }
                });
        let goal_summary = goal.as_ref().map(goal_summary);
        let request_summary = goal
            .as_ref()
            .map(|goal| goal.request.clone())
            .unwrap_or_default();
        let lifecycle_worker_kind = task_manager
            .as_ref()
            .and_then(|tm| tm.tasks.first())
            .map(|task| task.worker_kind.clone())
            .or_else(|| goal_summary.as_ref().map(|_| "gearbox".to_string()));
        let lifecycle_worker_model = task_manager
            .as_ref()
            .and_then(|tm| tm.tasks.first())
            .and_then(|task| task.worker_model.clone());
        let (phase_routes, phase_route_errors) = phase_route_summaries(store, goal.as_ref());
        let broker_sessions =
            broker_session_summaries(store, goal.as_ref().map(|goal| goal.id.as_str()));
        let next_goal = strategist_next_goal_summary(store, goal.as_ref(), epoch_id.as_deref());
        let plan_quality = plan
            .as_ref()
            .and_then(|plan| plan_quality_summary(store, goal.as_ref(), plan));
        let plan_revision_diff = plan.as_ref().and_then(|plan| {
            plan_revision_diff(store, goal.as_ref().map(|goal| goal.id.as_str()), plan)
        });
        let plan_approval = canonical_plan
            .as_ref()
            .and_then(|plan| plan_approval_summary(store, goal.as_ref(), plan));
        // Final verification belongs to the approved canonical graph. When
        // an unreviewed candidate is visible, hide the canonical receipt
        // rather than presenting evidence for a different plan revision.
        let plan_final_receipt = (!visible_plan_is_candidate)
            .then(|| goal.as_ref())
            .flatten()
            .and_then(|goal| final_verification_summary(store, goal));
        let plan_final_checks = if visible_plan_is_candidate {
            Vec::new()
        } else {
            goal.as_ref()
                .map(|goal| final_verification_checks(store, goal))
                .unwrap_or_default()
        };
        let final_receipt = plan_final_receipt.as_ref().and_then(|summary| {
            fs::File::open(&summary.2).ok().and_then(|file| {
                serde_json::from_reader::<_, FinalVerificationWaveReceipt>(file).ok()
            })
        });
        let plan_coverage = plan.as_ref().map(|plan| {
            plan_coverage_summary(plan, plan_node_runs.as_ref(), final_receipt.as_ref())
        });
        let (plan_artifact_path, plan_artifact_status) =
            plan_artifact_summary(store, goal.as_ref(), plan.as_ref());
        let plan_reused = timeline
            .iter()
            .any(|event| event.semantic_key.starts_with("PlanReused:"));
        let lifecycle = GearRuntimeLifecycle {
            objective_status: objective_summary
                .as_ref()
                .map(|summary| summary.status.clone()),
            goal_status: goal_summary.as_ref().map(|summary| summary.status.clone()),
            continuation_status: recovery.continuation_status.clone(),
            phase: epoch_events.last().map(|event| format!("{:?}", event.kind)),
            stop_reason: objective_summary
                .as_ref()
                .and_then(|summary| summary.stop_reason.clone()),
            recovery_state: recovery.stuck_reason.clone(),
            intensity: std::env::var("GEARBOX_GEAR_WORKER_INTENSITY")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            worker_kind: lifecycle_worker_kind,
            worker_model: lifecycle_worker_model.or_else(|| {
                std::env::var("GEARBOX_GEAR_WORKER_MODEL")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            }),
            ownership_delegated: ownership.as_ref().and_then(|value| value.delegated),
            ownership_worker_kind: ownership
                .as_ref()
                .and_then(|value| value.worker_kind.clone()),
            ownership_worker_task_id: ownership
                .as_ref()
                .and_then(|value| value.worker_task_id.clone()),
            ownership_route_reason: ownership
                .as_ref()
                .and_then(|value| value.route_reason.clone()),
            phase_routes,
            broker_sessions,
            phase_route_errors,
            visible_plan_revision: plan.as_ref().map(|plan| plan.revision),
            visible_plan_source: plan.as_ref().map(|plan| {
                if visible_plan_is_candidate {
                    "unreviewed".to_string()
                } else {
                    format!("{:?}", plan.source)
                }
            }),
            visible_plan_is_candidate,
            plan_preflight: plan
                .as_ref()
                .map(|plan| plan.draft.preflight.iter().take(16).cloned().collect())
                .unwrap_or_default(),
            plan_rollback: plan
                .as_ref()
                .map(|plan| plan.draft.rollback.iter().take(16).cloned().collect())
                .unwrap_or_default(),
            plan_final_verification: plan
                .as_ref()
                .map(|plan| {
                    plan.draft
                        .final_verification
                        .iter()
                        .take(16)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default(),
            plan_must_have: plan
                .as_ref()
                .map(|plan| plan.draft.must_have.iter().take(16).cloned().collect())
                .unwrap_or_default(),
            plan_must_not_have: plan
                .as_ref()
                .map(|plan| plan.draft.must_not_have.iter().take(16).cloned().collect())
                .unwrap_or_default(),
            plan_topology_lock: plan
                .as_ref()
                .map(|plan| plan.draft.topology_lock.iter().take(16).cloned().collect())
                .unwrap_or_default(),
            plan_final_acceptance: plan
                .as_ref()
                .map(|plan| {
                    plan.draft
                        .final_acceptance
                        .iter()
                        .take(16)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default(),
            plan_assumptions: plan
                .as_ref()
                .map(|plan| plan.draft.assumptions.iter().take(16).cloned().collect())
                .unwrap_or_default(),
            plan_findings: plan
                .as_ref()
                .map(|plan| plan.draft.findings.iter().take(16).cloned().collect())
                .unwrap_or_default(),
            plan_decisions: plan
                .as_ref()
                .map(|plan| plan.draft.decisions.iter().take(16).cloned().collect())
                .unwrap_or_default(),
            plan_open_questions: plan
                .as_ref()
                .map(|plan| plan.draft.open_questions.iter().take(16).cloned().collect())
                .unwrap_or_default(),
            plan_milestones: plan
                .as_ref()
                .map(plan_milestone_summaries)
                .unwrap_or_default(),
            plan_acceptance_checklist: plan
                .as_ref()
                .map(|plan| {
                    plan_acceptance_checklist(
                        plan,
                        plan_node_runs.as_ref(),
                        plan_final_receipt
                            .as_ref()
                            .and_then(|summary| {
                                fs::File::open(&summary.2).ok().and_then(|file| {
                                    serde_json::from_reader::<_, FinalVerificationWaveReceipt>(file)
                                        .ok()
                                })
                            })
                            .as_ref(),
                    )
                })
                .unwrap_or_default(),
            plan_artifact_path,
            plan_artifact_status,
            plan_reused,
            plan_quality_status: plan_quality.as_ref().map(|summary| summary.0.clone()),
            plan_quality_findings: plan_quality
                .as_ref()
                .map(|summary| summary.1.clone())
                .unwrap_or_default(),
            plan_quality_artifact: plan_quality.as_ref().map(|summary| summary.2.clone()),
            plan_revision_diff,
            plan_coverage,
            plan_approval_status: plan_approval.as_ref().map(|summary| summary.0.clone()),
            plan_revisions_used: plan_approval.as_ref().map_or(0, |summary| summary.1),
            plan_critic_receipt_hash: plan_approval.as_ref().and_then(|summary| summary.2.clone()),
            plan_approval_artifact: plan_approval.as_ref().map(|summary| summary.3.clone()),
            plan_final_receipt_status: plan_final_receipt.as_ref().map(|summary| summary.0.clone()),
            plan_final_receipt_hash: plan_final_receipt
                .as_ref()
                .and_then(|summary| summary.1.clone()),
            plan_final_receipt_artifact: plan_final_receipt
                .as_ref()
                .map(|summary| summary.2.clone()),
            plan_final_checks,
            rollback_pending: goal.as_ref().is_some_and(|goal| {
                let artifact_dir = store.artifact_dir(&goal.id);
                if artifact_dir.join("plan-rollback-confirmed.md").exists() {
                    return false;
                }
                fs::read_dir(artifact_dir)
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(Result::ok)
                    .any(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("plan-rollback-iteration-")
                    })
            }),
            rollback_artifact: goal.as_ref().and_then(|goal| {
                let mut paths = fs::read_dir(store.artifact_dir(&goal.id))
                    .ok()?
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .filter(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.starts_with("plan-rollback-iteration-"))
                    })
                    .collect::<Vec<_>>();
                paths.sort();
                paths.pop().map(|path| path.display().to_string())
            }),
            next_goal,
        };
        let mut health = runtime_health(task_manager.as_ref());
        health.last_activity_at = last_event_timestamp(store, &session_id);
        let sequence = timeline.last().map(|event| event.sequence).unwrap_or(0);
        Ok(Self {
            schema_version: GEAR_GUI_SNAPSHOT_SCHEMA_VERSION,
            sequence,
            workspace,
            session_id,
            objective_id,
            goal_id,
            epoch_id,
            objective: objective_summary,
            goal: goal_summary,
            request_summary,
            lifecycle,
            budget,
            review,
            recovery,
            feedback,
            feedback_events,
            plan_tasks,
            plan_total,
            plan_completed,
            next_plan_task_id,
            next_plan_task_title,
            plan_execution_mode: "serial_work_orders".to_string(),
            task_manager,
            timeline,
            health,
        }
        .bounded_for_ui())
    }

    pub fn bounded_for_ui(mut self) -> Self {
        if self.timeline.len() > GEAR_GUI_TIMELINE_CAPACITY {
            let keep_from = self.timeline.len() - GEAR_GUI_TIMELINE_CAPACITY;
            self.timeline.drain(..keep_from);
        }

        if let Some(task_manager) = self.task_manager.as_mut() {
            task_manager.tasks.truncate(32);
            for task in &mut task_manager.tasks {
                task.attempts.truncate(8);
                task.summary = GearRuntimeEventEnvelope::bounded_message(
                    std::mem::take(&mut task.summary),
                    GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                );
                task.summary_head = GearRuntimeEventEnvelope::bounded_message(
                    std::mem::take(&mut task.summary_head),
                    GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                );
                task.continuation_hint = GearRuntimeEventEnvelope::bounded_message(
                    std::mem::take(&mut task.continuation_hint),
                    GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                );
                for attempt in &mut task.attempts {
                    attempt.summary = GearRuntimeEventEnvelope::bounded_message(
                        std::mem::take(&mut attempt.summary),
                        GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                    );
                    if let Some(error) = attempt.error.take() {
                        attempt.error = Some(GearRuntimeEventEnvelope::bounded_message(
                            error,
                            GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                        ));
                    }
                }
            }
            task_manager.current_output = task_manager.current_output.take().map(|output| {
                GearRuntimeEventEnvelope::bounded_message(output, GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES)
            });
        }

        self.request_summary = GearRuntimeEventEnvelope::bounded_message(
            self.request_summary,
            GEAR_GUI_TERMINAL_SUMMARY_BYTES,
        );
        self.feedback_events.truncate(32);
        for event in &mut self.feedback_events {
            event.message = GearRuntimeEventEnvelope::bounded_message(
                std::mem::take(&mut event.message),
                GEAR_GUI_TERMINAL_SUMMARY_BYTES,
            );
        }
        self
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.schema_version != GEAR_GUI_SNAPSHOT_SCHEMA_VERSION {
            anyhow::bail!(
                "unsupported Gear GUI snapshot schema {}",
                self.schema_version
            );
        }
        if self.session_id.trim().is_empty() || self.workspace.trim().is_empty() {
            anyhow::bail!("Gear GUI snapshot requires session and workspace");
        }
        if self.timeline.len() > GEAR_GUI_TIMELINE_CAPACITY {
            anyhow::bail!("Gear GUI timeline exceeds its bounded capacity");
        }
        let serialized_size = serde_json::to_vec(self)?.len();
        if serialized_size > 512 * 1024 {
            anyhow::bail!("Gear GUI snapshot exceeds 512KiB");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct OwnershipProjection {
    delegated: Option<bool>,
    worker_kind: Option<String>,
    worker_task_id: Option<String>,
    route_reason: Option<String>,
}

fn ownership_from_timeline(timeline: &[GearRuntimeEventEnvelope]) -> Option<OwnershipProjection> {
    timeline.iter().rev().find_map(|event| {
        if !event.semantic_key.starts_with("PhaseRouteSelected:") {
            return None;
        }
        let payload = event.payload.as_ref()?;
        Some(OwnershipProjection {
            delegated: payload.get("ownership_delegated").and_then(Value::as_bool),
            worker_kind: payload
                .get("ownership_worker_kind")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            worker_task_id: payload
                .get("ownership_worker_task_id")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            route_reason: payload
                .get("ownership_route_reason")
                .and_then(Value::as_str)
                .map(|reason| GearRuntimeEventEnvelope::bounded_message(reason, 600)),
        })
    })
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn strategist_next_goal_summary(
    store: &StateStore,
    goal: Option<&Goal>,
    epoch_id: Option<&str>,
) -> Option<GearRuntimeNextGoalSummary> {
    let goal = goal?;
    let path = store
        .artifact_dir(&goal.id)
        .join("strategist-next-goal-receipt.json");
    let value: Value = serde_json::from_slice(&fs::read(path).ok()?).ok()?;
    let typed_receipt = if value.get("schema_version").is_some() {
        let receipt =
            serde_json::from_value::<crate::runtime::StrategistNextGoalReceipt>(value.clone())
                .ok()?;
        receipt.validate().ok()?;
        receipt
            .verdict
            .validate(
                &goal.id,
                epoch_id.unwrap_or(receipt.verdict.epoch_id.as_str()),
                &goal.status,
            )
            .ok()?;
        Some(receipt)
    } else {
        None
    };
    let verdict_value = if let Some(receipt) = typed_receipt.as_ref() {
        serde_json::to_value(&receipt.verdict).ok()?
    } else {
        value
            .get("verdict")
            .cloned()
            .unwrap_or_else(|| value.clone())
    };
    let verdict = &verdict_value;
    let string_array = |key: &str| {
        verdict
            .get(key)
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .take(16)
                    .map(|value| GearRuntimeEventEnvelope::bounded_message(value, 240))
                    .collect()
            })
            .unwrap_or_default()
    };
    Some(GearRuntimeNextGoalSummary {
        decision: verdict
            .get("decision")
            .and_then(Value::as_str)
            .map_or_else(|| "unknown".to_string(), ToString::to_string),
        next_objective: verdict
            .get("next_objective")
            .and_then(Value::as_str)
            .map(|value| GearRuntimeEventEnvelope::bounded_message(value, 600)),
        answerable_now: verdict
            .get("answerable_now")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        acceptance_signals: string_array("acceptance_signals"),
        required_questions: string_array("required_questions"),
        evidence_refs: string_array("evidence_refs"),
    })
}

fn phase_route_summaries(
    store: &StateStore,
    goal: Option<&Goal>,
) -> (Vec<GearRuntimePhaseRouteSummary>, usize) {
    let Some(goal) = goal else {
        return (Vec::new(), 0);
    };
    let Ok(entries) = fs::read_dir(store.phase_routes_dir(&goal.id)) else {
        return (Vec::new(), 0);
    };
    let mut receipt_count = 0usize;
    let mut summaries = Vec::new();
    for entry in entries.flatten().take(64) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with("-receipt.json") {
            continue;
        }
        receipt_count += 1;
        let Some(ordinal) = name.split('-').next().and_then(|value| value.parse().ok()) else {
            continue;
        };
        let Ok(contents) = fs::read(entry.path()) else {
            continue;
        };
        let Ok(receipt) = serde_json::from_slice::<PhaseRouteReceipt>(&contents) else {
            continue;
        };
        if receipt.goal_id.as_deref() != Some(goal.id.as_str())
            || receipt.ordinal != ordinal
            || receipt.validate().is_err()
        {
            continue;
        }
        let candidate = &receipt.decision.candidate;
        let backend = match &candidate.backend {
            PhaseBackend::Worker(kind) => kind.as_str().to_string(),
            backend => format!("{backend:?}").to_ascii_lowercase(),
        };
        let model = match &candidate.model {
            PhaseModelBinding::ExactLive(model) => Some(model.qualified_model_id()),
            PhaseModelBinding::BackendDeclared(model) => Some(model.clone()),
            PhaseModelBinding::CurrentSession => Some("current-session".to_string()),
            PhaseModelBinding::None => None,
        };
        summaries.push(GearRuntimePhaseRouteSummary {
            ordinal,
            phase: format!("{:?}", receipt.decision.phase),
            backend,
            model,
            worker_kind: receipt
                .decision
                .worker_kind
                .as_ref()
                .map(|kind| kind.as_str().to_string()),
            selected_candidate: receipt.decision.selected_candidate,
            fallback_count: receipt.decision.rejected_candidates.len(),
            source: format!("{:?}", receipt.decision.source),
            receipt_path: Some(entry.path().display().to_string()),
        });
    }
    summaries.sort_by_key(|summary| summary.ordinal);
    let valid_count = summaries.len();
    summaries.truncate(32);
    let errors = receipt_count.saturating_sub(valid_count);
    (summaries, errors)
}

/// Project the broker ledger without making the GUI depend on a live broker
/// handle.  A session can outlive the process, so the durable terminal file
/// is authoritative; a session directory without one is reported as active.
fn broker_session_summaries(store: &StateStore, goal_id: Option<&str>) -> Vec<String> {
    let root = store.root().join("broker-sessions");
    if !root.is_dir() {
        return Vec::new();
    }
    let mut pending = VecDeque::from([root.clone()]);
    let mut statuses = BTreeMap::new();
    while let Some(directory) = pending.pop_front() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if pending.len() < 256 {
                    pending.push_back(path);
                }
                continue;
            }
            let file_name = path.file_name().and_then(|name| name.to_str());
            if !matches!(
                file_name,
                Some("session-identity.json") | Some("terminal-outcome.json")
            ) {
                continue;
            }
            let relative = path
                .parent()
                .and_then(|parent| parent.strip_prefix(&root).ok())
                .map(|path| path.to_string_lossy().replace('\\', "/"))
                .unwrap_or_else(|| "unknown".to_string());
            if let Some(goal_id) = goal_id
                && !relative.split('/').any(|segment| segment == goal_id)
            {
                continue;
            }
            if file_name == Some("session-identity.json") {
                let detail = fs::read(&path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .map(|value| {
                        let backend = value
                            .get("backend_kind")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown-backend");
                        let session = value
                            .get("session_id")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown-session");
                        format!("{backend}/{session} · active")
                    })
                    .unwrap_or_else(|| "active".to_string());
                statuses.entry(relative).or_insert(detail);
                continue;
            }
            let status = fs::read(&path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|value| {
                    ["outcome", "status", "state"]
                        .into_iter()
                        .find_map(|key| value.get(key).and_then(Value::as_str))
                        .map(ToString::to_string)
                })
                .unwrap_or_else(|| "terminal".to_string());
            statuses.insert(relative, status);
        }
    }
    statuses
        .into_iter()
        .take(32)
        .map(|(path, status)| format!("{path} · {status}"))
        .collect()
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn plan_session_history(
    store: &StateStore,
    ledger: Option<&PlanNodeRunLedger>,
    node: Option<&crate::state::PlanNodeRun>,
) -> Vec<GearRuntimePlanSessionSummary> {
    let (Some(ledger), Some(node)) = (ledger, node) else {
        return Vec::new();
    };
    let first_attempt = node.attempt.saturating_add(1).saturating_sub(8);
    (first_attempt..=node.attempt)
        .filter_map(|attempt| {
            let binding = store
                .read_plan_node_session_binding(&ledger.goal_id, &ledger.epoch_id, &node.task_id, attempt)
                .ok()
                .flatten()?;
            if binding.plan_id != ledger.plan_id
                || binding.plan_revision != ledger.plan_revision
                || binding.plan_hash != ledger.plan_hash
            {
                return None;
            }
            let route_receipt = store
                .read_task_route_decision_receipt(
                    &ledger.goal_id,
                    &ledger.epoch_id,
                    &node.task_id,
                    attempt,
                )
                .ok()
                .flatten()
                .filter(|receipt| {
                    receipt.plan_id == ledger.plan_id
                        && receipt.plan_revision == ledger.plan_revision
                        && receipt.plan_hash == ledger.plan_hash
                        && binding
                            .route_receipt_hash
                            .as_deref()
                            .is_none_or(|hash| hash == receipt.receipt_hash)
                });
            let status = format!("{:?}", binding.status);
            let ended_at = matches!(
                &binding.status,
                crate::state::PlanNodeSessionBindingStatus::Terminal
                    | crate::state::PlanNodeSessionBindingStatus::Superseded
            )
            .then(|| binding.updated_at.clone());
            let elapsed_ms = ended_at
                .as_deref()
                .and_then(parse_rfc3339)
                .unwrap_or_else(Utc::now)
                .signed_duration_since(parse_rfc3339(&binding.created_at)?)
                .num_milliseconds();
            let elapsed_ms = Some(elapsed_ms.max(0) as u64);
            Some(GearRuntimePlanSessionSummary {
                attempt: binding.attempt,
                worker_task_id: binding.worker_task_id,
                worker_kind: binding.worker_kind,
                worker_category: route_receipt
                    .as_ref()
                    .map(|receipt| receipt.worker_category.clone()),
                provider_id: binding.provider_id,
                model_id: binding.model_id,
                session_id: binding.session_id,
                status,
                started_at: binding.created_at,
                ended_at,
                elapsed_ms,
                route_reason: route_receipt
                    .as_ref()
                    .map(|receipt| receipt.route_reason.clone()),
                route_fallback_count: route_receipt
                    .as_ref()
                    .map_or(0, |receipt| receipt.fallback_count),
            })
        })
        .collect()
}

fn plan_session_aggregate(
    history: &[GearRuntimePlanSessionSummary],
) -> (usize, usize, Option<u64>) {
    let attempt_count = history.len();
    let inferred_fallback_count = history
        .windows(2)
        .filter(|sessions| {
            sessions[0].worker_kind != sessions[1].worker_kind
                || sessions[0].model_id != sessions[1].model_id
        })
        .count();
    let reported_fallback_count = history
        .iter()
        .map(|session| session.route_fallback_count)
        .fold(0usize, usize::saturating_add);
    let fallback_count = if history.iter().any(|session| session.worker_category.is_some()) {
        reported_fallback_count
    } else {
        inferred_fallback_count
    };
    let elapsed_total_ms = (!history.is_empty()).then(|| {
        history
            .iter()
            .filter_map(|session| session.elapsed_ms)
            .fold(0u64, u64::saturating_add)
    });
    (attempt_count, fallback_count, elapsed_total_ms)
}

#[cfg(test)]
fn plan_progress_summary(
    tasks: &[GearRuntimePlanTaskSummary],
) -> (usize, usize, Option<String>, Option<String>) {
    let completed = tasks
        .iter()
        .filter(|task| {
            matches!(
                task.status.as_str(),
                "Completed" | "Reviewed" | "GreenVerified"
            )
        })
        .count();
    let next = tasks.iter().find(|task| {
        matches!(
            task.status.as_str(),
            "Pending" | "Runnable" | "Running" | "RedVerified" | "Implemented"
        )
    });
    (
        tasks.len(),
        completed,
        next.map(|task| task.task_id.clone()),
        next.map(|task| task.title.clone()),
    )
}

fn plan_progress_from_graph(
    plan: Option<&PlanGraph>,
    ledger: Option<&crate::state::PlanNodeRunLedger>,
) -> (usize, usize, Option<String>, Option<String>) {
    let Some(plan) = plan else {
        return (0, 0, None, None);
    };
    let completed = plan
        .draft
        .tasks
        .iter()
        .filter(|task| {
            ledger
                .and_then(|ledger| {
                    ledger
                        .nodes
                        .iter()
                        .find(|node| node.task_id == task.task_id)
                })
                .is_some_and(|node| node.status == PlanNodeRunStatus::Completed)
        })
        .count();
    let next = plan.draft.tasks.iter().find(|task| {
        ledger
            .and_then(|ledger| {
                ledger
                    .nodes
                    .iter()
                    .find(|node| node.task_id == task.task_id)
            })
            .map(|node| {
                !matches!(
                    node.status,
                    PlanNodeRunStatus::Completed
                        | PlanNodeRunStatus::Failed
                        | PlanNodeRunStatus::NeedsUser
                        | PlanNodeRunStatus::Cancelled
                )
            })
            .unwrap_or(true)
    });
    (
        plan.draft.tasks.len(),
        completed,
        next.map(|task| task.task_id.clone()),
        next.map(|task| task.title.clone()),
    )
}

fn runtime_health(task_manager: Option<&TaskManagerSnapshot>) -> GearRuntimeHealth {
    let processes = process_health_snapshot();
    let Some(task_manager) = task_manager else {
        return GearRuntimeHealth {
            processes,
            ..GearRuntimeHealth::default()
        };
    };
    let owned_child_processes = task_manager
        .tasks
        .iter()
        .filter(|task| matches!(task.status, crate::task_manager::ManagedTaskStatus::Running))
        .count();
    let last_error = task_manager
        .tasks
        .iter()
        .rev()
        .flat_map(|task| task.attempts.iter().rev())
        .find_map(|attempt| attempt.error.clone());
    GearRuntimeHealth {
        owned_child_processes,
        last_error,
        processes,
        ..GearRuntimeHealth::default()
    }
}

fn classify_process_name(name: &str) -> Option<&'static str> {
    match name.trim() {
        "cargo" => Some("cargo"),
        "rustc" => Some("rustc"),
        "rust-analyzer" | "rust_analyzer" => Some("rust_analyzer"),
        "opencode" => Some("opencode"),
        "codex" => Some("codex"),
        _ => None,
    }
}

fn process_health_snapshot() -> GearRuntimeProcessHealth {
    let mut health = GearRuntimeProcessHealth::default();
    #[cfg(target_os = "linux")]
    {
        let Ok(entries) = fs::read_dir("/proc") else {
            return health;
        };
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !name.bytes().all(|byte| byte.is_ascii_digit()) {
                continue;
            }
            let Ok(command) = fs::read_to_string(entry.path().join("comm")) else {
                continue;
            };
            match classify_process_name(command.trim()) {
                Some("cargo") => health.cargo += 1,
                Some("rustc") => health.rustc += 1,
                Some("rust_analyzer") => health.rust_analyzer += 1,
                Some("opencode") => health.opencode += 1,
                Some("codex") => health.codex += 1,
                _ => {}
            }
        }
    }
    health.rust_processes = health
        .cargo
        .saturating_add(health.rustc)
        .saturating_add(health.rust_analyzer);
    health.rust_process_over_limit = health.rust_processes > 2;
    health
}

fn last_event_timestamp(store: &StateStore, session_id: &str) -> Option<String> {
    bounded_file_tail(&store.events_path(session_id))
        .lines()
        .rev()
        .find_map(|line| {
            serde_json::from_str::<Event>(line)
                .ok()
                .filter(|event| event.session_id == session_id)
                .map(|event| event.ts)
        })
}

fn goal_summary(goal: &Goal) -> GearRuntimeGoalSummary {
    GearRuntimeGoalSummary {
        id: goal.id.clone(),
        title: goal.title.clone(),
        status: format!("{:?}", goal.status),
        current_task_id: goal.current_task_id.clone(),
        summary: GearRuntimeEventEnvelope::bounded_message(
            goal.summary.clone(),
            GEAR_GUI_TERMINAL_SUMMARY_BYTES,
        ),
        intensity: std::env::var("GEARBOX_GEAR_WORKER_INTENSITY")
            .ok()
            .filter(|value| !value.trim().is_empty()),
    }
}

fn goal_budget_summary(ledger: Option<&GoalBudgetLedger>) -> GearRuntimeBudgetSummary {
    let Some(ledger) = ledger else {
        return GearRuntimeBudgetSummary::default();
    };
    let mut summary = GearRuntimeBudgetSummary::default();
    summary.calls_reserved = Some(ledger.reservations.len() as u64);
    summary.calls_used = Some(
        ledger
            .reservations
            .iter()
            .filter(|reservation| {
                reservation.status != crate::state::BudgetReservationStatus::Reserved
            })
            .count() as u64,
    );
    summary.tokens_reserved = Some(
        ledger
            .reservations
            .iter()
            .map(|reservation| reservation.reserved_tokens)
            .fold(0u64, u64::saturating_add),
    );
    summary.tokens_used = Some(
        ledger
            .reservations
            .iter()
            .filter_map(|reservation| reservation.usage.as_ref())
            .filter_map(|usage| usage.total_tokens())
            .fold(0u64, u64::saturating_add),
    );
    summary.cost_micros_reserved = Some(
        ledger
            .reservations
            .iter()
            .map(|reservation| reservation.reserved_cost_micros)
            .fold(0u64, u64::saturating_add),
    );
    summary.cost_micros_used = Some(
        ledger
            .reservations
            .iter()
            .filter_map(|reservation| reservation.usage.as_ref())
            .filter_map(|usage| usage.cost_micros)
            .fold(0u64, u64::saturating_add),
    );
    summary.unknown_usage_calls = ledger
        .reservations
        .iter()
        .filter(|reservation| {
            reservation
                .usage
                .as_ref()
                .is_some_and(|usage| usage.is_unknown())
        })
        .count() as u64;
    summary
}

fn plan_task_summaries(
    store: &StateStore,
    goal: Option<&Goal>,
    plan: Option<&PlanGraph>,
    matching_ledger: Option<&PlanNodeRunLedger>,
) -> Vec<GearRuntimePlanTaskSummary> {
    let Some(plan) = plan else {
        return Vec::new();
    };
    let durable_tasks = goal
        .and_then(|goal| store.read_tasks(&goal.id).ok().flatten())
        .unwrap_or_default();
    plan.draft
        .tasks
        .iter()
        .take(128)
        .map(|task| {
            let node = matching_ledger.and_then(|ledger| {
                ledger
                    .nodes
                    .iter()
                    .find(|node| node.task_id == task.task_id)
            });
            let status = node
                .map(|node| format!("{:?}", node.status))
                .unwrap_or_else(|| format!("{:?}", PlanNodeRunStatus::Pending));
            let contract_status = plan_contract_status(node).to_string();
            let current = goal
                .and_then(|goal| goal.current_task_id.as_deref())
                .is_some_and(|current_task_id| {
                    current_task_id == task.task_id
                        || matching_ledger.is_some_and(|ledger| {
                            ledger
                                .nodes
                                .iter()
                                .find(|node| node.task_id == task.task_id)
                                .is_some_and(|node| {
                                    node.worker_task_id.as_deref() == Some(current_task_id)
                                        || node.implementation_task_id.as_deref()
                                            == Some(current_task_id)
                                        || node.review_task_id.as_deref() == Some(current_task_id)
                                })
                        })
                });
            let session_binding = node.and_then(|node| {
                let ledger = matching_ledger?;
                store
                    .read_plan_node_session_binding(
                        &ledger.goal_id,
                        &ledger.epoch_id,
                        &node.task_id,
                        node.attempt,
                    )
                    .ok()
                    .flatten()
            });
            let worker_session_id = session_binding.as_ref().map(|binding| binding.session_id.clone());
            let worker_session_status = session_binding
                .as_ref()
                .map(|binding| format!("{:?}", binding.status));
            let worker_session_started_at = session_binding
                .as_ref()
                .map(|binding| binding.created_at.clone());
            let worker_session_updated_at = session_binding
                .as_ref()
                .map(|binding| binding.updated_at.clone());
            let worker_session_ended_at = session_binding.as_ref().and_then(|binding| {
                matches!(
                    binding.status,
                    crate::state::PlanNodeSessionBindingStatus::Terminal
                        | crate::state::PlanNodeSessionBindingStatus::Superseded
                )
                .then(|| binding.updated_at.clone())
            });
            let worker_session_elapsed_ms = session_binding.as_ref().and_then(|binding| {
                let end = worker_session_ended_at
                    .as_deref()
                    .and_then(parse_rfc3339)
                    .unwrap_or_else(Utc::now);
                parse_rfc3339(&binding.created_at).and_then(|start| {
                    Some(
                        end.signed_duration_since(start)
                            .num_milliseconds()
                            .max(0) as u64,
                    )
                })
            });
            let worker_session_history = plan_session_history(store, matching_ledger, node);
            let (worker_session_attempt_count, worker_session_fallback_count, worker_session_elapsed_total_ms) =
                plan_session_aggregate(&worker_session_history);
            let route_receipt = node.and_then(|node| {
                let goal = goal?;
                let ledger = matching_ledger?;
                store
                    .read_task_route_decision_receipt(
                        &goal.id,
                        &ledger.epoch_id,
                        &node.task_id,
                        node.attempt,
                    )
                    .ok()
                    .flatten()
            });
            let routing_brief_path = durable_tasks
                .iter()
                .find(|candidate| {
                    candidate
                        .inputs
                        .plan_task
                        .as_ref()
                        .is_some_and(|plan_task| plan_task.task_id == task.task_id)
                        || candidate.id
                            == node
                                .and_then(|node| node.worker_task_id.as_deref())
                                .unwrap_or_default()
                })
                .and_then(|candidate| candidate.inputs.worker_packet_path.clone());
            let preflight_path = node
                .and_then(|node| node.preflight_path.clone())
                .or_else(|| {
                    node.and_then(|node| {
                        let prefix = format!(
                            "work-order-{}-attempt-{}-preflight",
                            plan_artifact_component_for_gui(&task.task_id),
                            node.attempt
                        );
                        fs::read_dir(goal.map(|goal| store.artifact_dir(&goal.id))?)
                            .ok()?
                            .filter_map(Result::ok)
                            .map(|entry| entry.path())
                            .find(|path| {
                                path.file_stem()
                                    .and_then(|name| name.to_str())
                                    .is_some_and(|name| name == prefix)
                            })
                            .map(|path| path.to_string_lossy().to_string())
                    })
                });
            GearRuntimePlanTaskSummary {
                task_id: task.task_id.clone(),
                title: task.title.clone(),
                goal: task.goal.chars().take(1200).collect(),
                deliverable: task.deliverable.chars().take(1200).collect(),
                rationale: task.rationale.chars().take(1200).collect(),
                approach: task
                    .approach
                    .iter()
                    .take(16)
                    .map(|item| item.chars().take(600).collect())
                    .collect(),
                role: plan_task_role(task).to_string(),
                actual_worker_kind: route_receipt
                    .as_ref()
                    .map(|receipt| receipt.worker_kind.clone()),
                actual_worker_model: route_receipt
                    .as_ref()
                    .and_then(|receipt| receipt.worker_model.clone()),
                route_hint: route_receipt
                    .as_ref()
                    .and_then(|receipt| receipt.route_hint.clone()),
                status,
                contract_status,
                dependencies: task.dependencies.iter().take(32).cloned().collect(),
                inputs: task.inputs.iter().take(16).cloned().collect(),
                preconditions: task.preconditions.iter().take(16).cloned().collect(),
                already_in_working_tree: task
                    .already_in_working_tree
                    .iter()
                    .take(16)
                    .cloned()
                    .collect(),
                still_needed: task.still_needed.iter().take(16).cloned().collect(),
                parallel_wave: task.parallel_wave,
                current,
                attempt: node.map_or(0, |node| node.attempt),
                worker_task_id: node.and_then(|node| node.worker_task_id.clone()),
                worker_session_id,
                worker_session_status,
                worker_session_started_at,
                worker_session_updated_at,
                worker_session_ended_at,
                worker_session_elapsed_ms,
                worker_session_history,
                worker_session_attempt_count,
                worker_session_fallback_count,
                worker_session_elapsed_total_ms,
                error: node.and_then(|node| {
                    node.error
                        .as_ref()
                        .map(|error| GearRuntimeEventEnvelope::bounded_message(error.clone(), 1200))
                }),
                routing_brief_path,
                preflight_path,
                preflight_satisfied: node.is_some_and(|node| node.preflight_satisfied),
                preflight_checks: node
                    .map(|node| {
                        node.preflight_checks
                            .iter()
                            .map(|check| {
                                format!(
                                    "[{}] {}{}",
                                    if check.passed { "x" } else { "!" },
                                    check.check_id,
                                    check
                                        .failure
                                        .as_deref()
                                        .map(|failure| format!(": {failure}"))
                                        .unwrap_or_default()
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                worker_result_path: node.and_then(|node| node.worker_result_path.clone()),
                worker_outcome_path: node.and_then(|node| node.worker_outcome_path.clone()),
                worker_last_message_path: node
                    .and_then(|node| node.worker_last_message_path.clone()),
                worker_last_message_excerpt: node
                    .and_then(|node| node.worker_last_message_path.as_deref())
                    .and_then(worker_last_message_excerpt),
                worker_changed_files: node
                    .map(|node| node.worker_changed_files.iter().take(32).cloned().collect())
                    .unwrap_or_default(),
                worker_commands_run: node
                    .map(|node| node.worker_commands_run.iter().take(32).cloned().collect())
                    .unwrap_or_default(),
                worker_known_failures: node
                    .map(|node| {
                        node.worker_known_failures
                            .iter()
                            .take(16)
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default(),
                worker_next_steps: node
                    .map(|node| node.worker_next_steps.iter().take(16).cloned().collect())
                    .unwrap_or_default(),
                worker_plan_gap: node.and_then(|node| node.worker_plan_gap.clone()),
                worker_decision: node
                    .map(|node| format!("{:?}", node.worker_decision))
                    .unwrap_or_else(|| "NotRecorded".to_string()),
                worker_decision_reason: node.and_then(|node| node.worker_decision_reason.clone()),
                worker_evidence_quality: node
                    .map(|node| format!("{:?}", node.worker_evidence_quality))
                    .unwrap_or_else(|| "Unclassified".to_string()),
                red_evidence_path: node.and_then(|node| node.red_evidence_path.clone()),
                green_evidence_paths: node
                    .map(|node| node.green_evidence_paths.iter().take(8).cloned().collect())
                    .unwrap_or_default(),
                review_evidence_path: node.and_then(|node| node.review_evidence_path.clone()),
                commit_boundary_evidence_path: node
                    .and_then(|node| node.commit_boundary_evidence_path.clone()),
                commit_boundary_satisfied: node.and_then(|node| node.commit_boundary_satisfied),
                size_tier: format!("{:?}", task.size_tier()),
                risk_tier: format!("{:?}", task.risk_tier()),
                commit_boundary: format!("{:?}", task.commit_boundary),
                commit_message: task.commit_message.clone(),
                must_do: task.must_do.iter().take(12).cloned().collect(),
                evidence: task.evidence.iter().take(16).cloned().collect(),
                rollback: task.rollback.iter().take(16).cloned().collect(),
                budget: task.budget.clone(),
                execution_steps: task
                    .execution_steps_or_legacy()
                    .iter()
                    .take(16)
                    .enumerate()
                    .map(|(index, step)| {
                        let run = node.and_then(|node| {
                            node.execution_steps
                                .iter()
                                .find(|run| run.step_id == step.step_id)
                        });
                        GearRuntimePlanStepSummary {
                            step_id: format!("{:02}:{}", index + 1, step.step_id),
                            action: step.action.clone(),
                            expected_observation: step.expected_observation.clone(),
                            status: run
                                .map(|run| format!("{:?}", run.status))
                                .unwrap_or_else(|| "Pending".to_string()),
                            evidence_path: run
                                .and_then(|run| run.evidence_path.clone())
                                .or_else(|| step.evidence_path.clone()),
                            error: run.and_then(|run| run.error.clone()),
                        }
                    })
                    .collect(),
                execution_steps_evidence_required: task.execution_steps_evidence_required,
                must_not_do: task.must_not_do.iter().take(12).cloned().collect(),
                completion_predicates: task
                    .completion_predicates
                    .iter()
                    .take(12)
                    .cloned()
                    .collect(),
                required_capabilities: task.required_capabilities.iter().take(8).cloned().collect(),
                references: task
                    .references
                    .iter()
                    .take(8)
                    .map(|reference| {
                        format!(
                            "{}{} — {}",
                            reference.path,
                            reference
                                .symbol
                                .as_deref()
                                .map(|symbol| format!("::{symbol}"))
                                .unwrap_or_default(),
                            reference.reason
                        )
                    })
                    .collect(),
                required_artifacts: task
                    .artifacts
                    .iter()
                    .filter(|artifact| artifact.required)
                    .take(8)
                    .map(|artifact| required_artifact_summary(store, artifact))
                    .collect(),
                allowed_files: task.scope.allowed_files.iter().take(8).cloned().collect(),
                forbidden_files: task.scope.forbidden_files.iter().take(8).cloned().collect(),
                write_scope: task.scope.write_scope.iter().take(8).cloned().collect(),
                max_files_changed: task.scope.max_files_changed,
                test_strategy: format!("{:?}", task.test.strategy),
                verification_commands: task
                    .test
                    .green
                    .iter()
                    .take(8)
                    .map(|expectation| expectation.command.clone())
                    .collect(),
                qa_scenarios: task
                    .qa
                    .happy_path
                    .iter()
                    .map(|scenario| ("happy", scenario))
                    .chain(
                        task.qa
                            .failure_path
                            .iter()
                            .map(|scenario| ("failure", scenario)),
                    )
                    .chain(
                        task.qa
                            .adversarial_path
                            .iter()
                            .map(|scenario| ("adversarial", scenario)),
                    )
                    .take(8)
                    .map(|(kind, scenario)| {
                        let criterion_id = format!("qa:{kind}:{}", scenario.name);
                        format!(
                            "[{}] {kind}/{}: {} — evidence: {}",
                            qa_evidence_marker(node, &criterion_id),
                            scenario.name,
                            scenario.expected_result,
                            scenario.evidence_path
                        )
                    })
                    .collect(),
            }
        })
        .collect()
}

fn plan_contract_status(node: Option<&PlanNodeRun>) -> &'static str {
    match node.map(|node| &node.status) {
        Some(PlanNodeRunStatus::Completed) => "accepted",
        Some(PlanNodeRunStatus::Failed) => "failed",
        Some(PlanNodeRunStatus::NeedsUser | PlanNodeRunStatus::Cancelled) => "blocked",
        Some(_) | None => "pending",
    }
}

fn plan_artifact_summary(
    store: &StateStore,
    goal: Option<&Goal>,
    plan: Option<&PlanGraph>,
) -> (Option<String>, Option<String>) {
    let Some(goal) = goal else {
        return (None, None);
    };
    let path = store.artifact_dir(&goal.id).join("plan.md");
    let path_text = path.to_string_lossy().to_string();
    let Some(plan) = plan else {
        return (Some(path_text), Some("missing_plan_graph".to_string()));
    };
    let Ok(contents) = fs::read_to_string(&path) else {
        return (Some(path_text), Some("missing".to_string()));
    };
    let expected_hash = format!("Plan hash: `{}`", plan.plan_hash);
    if contents.contains(&expected_hash) {
        (Some(path_text), Some("current".to_string()))
    } else if contents.contains("Plan hash:") {
        (Some(path_text), Some("stale".to_string()))
    } else {
        (Some(path_text), Some("invalid".to_string()))
    }
}

fn worker_last_message_excerpt(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|message| GearRuntimeEventEnvelope::bounded_message(message, 1200))
}

fn plan_milestone_summaries(plan: &PlanGraph) -> Vec<String> {
    let mut tasks_by_wave = BTreeMap::<usize, Vec<&str>>::new();
    for task in &plan.draft.tasks {
        tasks_by_wave
            .entry(task.parallel_wave)
            .or_default()
            .push(task.task_id.as_str());
    }
    tasks_by_wave
        .into_iter()
        .map(|(wave, task_ids)| format!("Wave {wave}: {}", task_ids.join(", ")))
        .chain(std::iter::once(
            "Final wave: F1-F4 verification and final acceptance".to_string(),
        ))
        .take(32)
        .collect()
}

fn plan_acceptance_checklist(
    plan: &PlanGraph,
    ledger: Option<&PlanNodeRunLedger>,
    final_receipt: Option<&FinalVerificationWaveReceipt>,
) -> Vec<String> {
    plan.draft
        .tasks
        .iter()
        .flat_map(|task| {
            let criteria = task.completion_predicates.iter().map(move |predicate| {
                format!(
                    "[{}] {}: {}",
                    criterion_marker(ledger, &task.task_id, predicate),
                    task.task_id,
                    predicate
                )
            });
            let qa = task
                .qa
                .happy_path
                .iter()
                .map(|scenario| ("happy", scenario))
                .chain(
                    task.qa
                        .failure_path
                        .iter()
                        .map(|scenario| ("failure", scenario)),
                )
                .chain(
                    task.qa
                        .adversarial_path
                        .iter()
                        .map(|scenario| ("adversarial", scenario)),
                )
                .map(move |(kind, scenario)| {
                    let criterion = format!("qa:{kind}:{}", scenario.name);
                    format!(
                        "[{}] {} QA/{}: {} (evidence: {})",
                        criterion_marker(ledger, &task.task_id, &criterion),
                        task.task_id,
                        kind,
                        scenario.name,
                        scenario.evidence_path
                    )
                });
            criteria.chain(qa)
        })
        .chain(plan.draft.final_acceptance.iter().map(|predicate| {
            format!(
                "[{}] Final acceptance: {predicate}",
                final_acceptance_marker(ledger, final_receipt, plan)
            )
        }))
        .take(32)
        .collect()
}

fn criterion_marker(
    ledger: Option<&PlanNodeRunLedger>,
    task_id: &str,
    criterion: &str,
) -> &'static str {
    let Some(node) =
        ledger.and_then(|ledger| ledger.nodes.iter().find(|node| node.task_id == task_id))
    else {
        return " ";
    };
    match node
        .criterion_evidence
        .iter()
        .find(|evidence| evidence.criterion_id == criterion && evidence.attempt == node.attempt)
        .map(|evidence| &evidence.status)
    {
        Some(CriterionEvidenceStatus::Pass) => "x",
        Some(CriterionEvidenceStatus::Fail | CriterionEvidenceStatus::Blocked) => "!",
        None => " ",
    }
}

fn qa_evidence_marker(node: Option<&PlanNodeRun>, criterion_id: &str) -> &'static str {
    let Some(node) = node else {
        return " ";
    };
    match node
        .criterion_evidence
        .iter()
        .find(|evidence| evidence.criterion_id == criterion_id && evidence.attempt == node.attempt)
        .map(|evidence| &evidence.status)
    {
        Some(CriterionEvidenceStatus::Pass) => "x",
        Some(CriterionEvidenceStatus::Fail | CriterionEvidenceStatus::Blocked) => "!",
        None => " ",
    }
}

fn plan_task_role(task: &crate::plan_graph::PlanTaskContract) -> &'static str {
    match task.preferred_phase_profile {
        crate::plan_graph::PhaseProfile::ReviewerTask
        | crate::plan_graph::PhaseProfile::ReviewerFinal => "review",
        _ => "build",
    }
}

fn required_artifact_summary(
    store: &StateStore,
    artifact: &crate::plan_graph::PlanArtifactContract,
) -> String {
    let workspace = store.root().parent().unwrap_or_else(|| Path::new("."));
    let declared = Path::new(&artifact.path);
    let candidate = if declared.is_absolute() {
        declared.to_path_buf()
    } else {
        let workspace_candidate = workspace.join(declared);
        if workspace_candidate.exists() {
            workspace_candidate
        } else {
            store.root().join(declared)
        }
    };
    format!(
        "[{}] {} — {}",
        if candidate.exists() { "x" } else { " " },
        artifact.path,
        artifact.description
    )
}

fn final_acceptance_marker(
    ledger: Option<&PlanNodeRunLedger>,
    final_receipt: Option<&FinalVerificationWaveReceipt>,
    plan: &PlanGraph,
) -> &'static str {
    let criteria_passed = ledger.is_some_and(|ledger| {
        plan.draft.tasks.iter().all(|task| {
            ledger
                .nodes
                .iter()
                .find(|node| node.task_id == task.task_id)
                .is_some_and(|node| node.all_criteria_passed(&task.completion_predicates))
        })
    });
    match (criteria_passed, final_receipt.map(|receipt| receipt.passed)) {
        (true, Some(true)) => "x",
        (_, Some(false)) => "!",
        _ => " ",
    }
}

const GEAR_GUI_GOAL_HISTORY_CAPACITY: usize = 32;

fn plan_artifact_component_for_gui(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn objective_goal_history(graph: &ObjectiveGraph) -> Vec<GearRuntimeGoalHistorySummary> {
    graph
        .nodes
        .iter()
        .rev()
        .take(GEAR_GUI_GOAL_HISTORY_CAPACITY)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|node| GearRuntimeGoalHistorySummary {
            goal_id: node.goal_id.clone(),
            epoch_id: node.epoch_id.clone(),
            status: format!("{:?}", node.status),
            request: GearRuntimeEventEnvelope::bounded_message(node.request.clone(), 2400),
            parent_goal_id: node.parent_goal_id.clone(),
            parent_epoch_id: node.parent_epoch_id.clone(),
            terminal_reason: node
                .terminal_reason
                .clone()
                .map(|reason| GearRuntimeEventEnvelope::bounded_message(reason, 1200)),
            final_report_path: node.final_report_path.clone(),
        })
        .collect()
}

fn plan_quality_summary(
    store: &StateStore,
    goal: Option<&Goal>,
    plan: &PlanGraph,
) -> Option<(String, Vec<String>, String)> {
    let goal = goal?;
    let path = store.plan_review_dir(&goal.id).join(format!(
        "revision-{:03}-verifier-report.json",
        plan.revision
    ));
    let report: PlanVerifierReport = serde_json::from_reader(fs::File::open(&path).ok()?).ok()?;
    if report.validate(plan).is_err() {
        return Some((
            "invalid".to_string(),
            vec!["persisted plan verifier report failed validation".to_string()],
            path.to_string_lossy().to_string(),
        ));
    }
    let findings = report
        .checks
        .iter()
        .flat_map(|check| {
            check
                .findings
                .iter()
                .map(move |finding| format!("{:?}: {finding}", check.dimension))
        })
        .take(16)
        .collect();
    Some((
        if report.passed() {
            "passed".to_string()
        } else {
            "failed".to_string()
        },
        findings,
        path.to_string_lossy().to_string(),
    ))
}

fn plan_revision_diff(
    store: &StateStore,
    goal_id: Option<&str>,
    current: &PlanGraph,
) -> Option<GearRuntimePlanRevisionDiff> {
    let goal_id = goal_id?;
    let previous_revision = current.revision.checked_sub(1)?;
    let prefix = format!("revision-{previous_revision:03}-");
    let previous_path = fs::read_dir(store.plan_review_dir(goal_id))
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".plan.json"))
        })?;
    let previous: PlanGraph = serde_json::from_reader(fs::File::open(previous_path).ok()?).ok()?;
    previous.validate().ok()?;
    let previous_tasks = previous
        .draft
        .tasks
        .iter()
        .map(|task| (task.task_id.as_str(), task))
        .collect::<std::collections::HashMap<_, _>>();
    let current_tasks = current
        .draft
        .tasks
        .iter()
        .map(|task| (task.task_id.as_str(), task))
        .collect::<std::collections::HashMap<_, _>>();
    let mut diff = GearRuntimePlanRevisionDiff {
        from_revision: previous.revision,
        to_revision: current.revision,
        objective_changed: previous.draft.objective != current.draft.objective,
        ..GearRuntimePlanRevisionDiff::default()
    };
    for task_id in current_tasks.keys() {
        match previous_tasks.get(task_id) {
            None => diff.added_tasks.push((*task_id).to_string()),
            Some(previous_task) => {
                let Some(current_task) = current_tasks.get(task_id) else {
                    continue;
                };
                if serde_json::to_vec(previous_task).ok() != serde_json::to_vec(current_task).ok() {
                    diff.changed_tasks.push((*task_id).to_string());
                }
            }
        }
    }
    for task_id in previous_tasks.keys() {
        if !current_tasks.contains_key(task_id) {
            diff.removed_tasks.push((*task_id).to_string());
        }
    }
    diff.added_tasks.sort_unstable();
    diff.removed_tasks.sort_unstable();
    diff.changed_tasks.sort_unstable();
    diff.added_tasks.truncate(32);
    diff.removed_tasks.truncate(32);
    diff.changed_tasks.truncate(32);
    Some(diff)
}

fn plan_coverage_summary(
    plan: &PlanGraph,
    ledger: Option<&PlanNodeRunLedger>,
    final_receipt: Option<&FinalVerificationWaveReceipt>,
) -> GearRuntimePlanCoverageSummary {
    let mut summary = GearRuntimePlanCoverageSummary {
        work_orders_total: plan.draft.tasks.len(),
        ..GearRuntimePlanCoverageSummary::default()
    };
    for task in &plan.draft.tasks {
        let node = ledger.and_then(|ledger| {
            ledger
                .nodes
                .iter()
                .find(|node| node.task_id == task.task_id)
        });
        if node.is_some_and(|node| node.status == PlanNodeRunStatus::Completed) {
            summary.work_orders_completed += 1;
        }
        summary.acceptance_total += task.completion_predicates.len();
        summary.acceptance_satisfied += task
            .completion_predicates
            .iter()
            .filter(|criterion| criterion_is_passed(node, criterion))
            .count();
        for (kind, scenarios) in [
            ("happy", &task.qa.happy_path),
            ("failure", &task.qa.failure_path),
            ("adversarial", &task.qa.adversarial_path),
        ] {
            summary.qa_total += scenarios.len();
            summary.qa_satisfied += scenarios
                .iter()
                .filter(|scenario| {
                    criterion_is_passed(node, &format!("qa:{kind}:{}", scenario.name))
                })
                .count();
        }
    }
    summary.acceptance_total += plan.draft.final_acceptance.len();
    if !plan.draft.final_acceptance.is_empty()
        && final_acceptance_marker(ledger, final_receipt, plan) == "x"
    {
        summary.acceptance_satisfied += plan.draft.final_acceptance.len();
    }
    summary
}

fn criterion_is_passed(node: Option<&PlanNodeRun>, criterion: &str) -> bool {
    let Some(node) = node else {
        return false;
    };
    node.criterion_evidence.iter().any(|evidence| {
        evidence.criterion_id == criterion
            && evidence.attempt == node.attempt
            && evidence.status == CriterionEvidenceStatus::Pass
    })
}

fn plan_approval_summary(
    store: &StateStore,
    goal: Option<&Goal>,
    plan: &PlanGraph,
) -> Option<(String, usize, Option<String>, String)> {
    let goal = goal?;
    let path = store.plan_review_dir(&goal.id).join("approval.json");
    let approval: PlanApprovalState = serde_json::from_reader(fs::File::open(&path).ok()?).ok()?;
    let status = if approval.validate_against(plan).is_ok() {
        format!("{:?}", approval.status)
    } else if approval.plan_hash != plan.plan_hash {
        "stale".to_string()
    } else {
        "invalid".to_string()
    };
    Some((
        status,
        approval.revisions_used,
        approval.critic_receipt_hash,
        path.to_string_lossy().to_string(),
    ))
}

fn final_verification_summary(
    store: &StateStore,
    goal: &Goal,
) -> Option<(String, Option<String>, String)> {
    let plan = store.read_plan_graph(&goal.id).ok().flatten()?;
    let path = store
        .artifact_dir(&goal.id)
        .join("final-verification-wave.json");
    let receipt: FinalVerificationWaveReceipt =
        serde_json::from_reader(fs::File::open(&path).ok()?).ok()?;
    let status = if receipt.validate(&plan).is_ok() {
        if receipt.passed { "passed" } else { "failed" }
    } else {
        "invalid"
    };
    Some((
        status.to_string(),
        Some(receipt.receipt_hash),
        path.to_string_lossy().to_string(),
    ))
}

fn final_verification_checks(store: &StateStore, goal: &Goal) -> Vec<String> {
    let Some(plan) = store.read_plan_graph(&goal.id).ok().flatten() else {
        return Vec::new();
    };
    let path = store
        .artifact_dir(&goal.id)
        .join("final-verification-wave.json");
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    let Ok(receipt) = serde_json::from_reader::<_, FinalVerificationWaveReceipt>(file) else {
        return Vec::new();
    };
    if receipt.validate(&plan).is_err() {
        return vec!["FINAL VERIFICATION: invalid receipt".to_string()];
    }
    receipt
        .dimensions
        .iter()
        .take(4)
        .enumerate()
        .map(|(index, result)| {
            format!(
                "F{} {:?}: {} — {}",
                index + 1,
                result.dimension,
                if result.passed { "pass" } else { "fail" },
                GearRuntimeEventEnvelope::bounded_message(result.summary.clone(), 320)
            )
        })
        .collect()
}

fn find_objective_graph(objectives_dir: &Path, goal_id: &str) -> Option<ObjectiveGraph> {
    let entries = fs::read_dir(objectives_dir).ok()?;
    entries.filter_map(Result::ok).find_map(|entry| {
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json")
            || !path.file_name()?.to_str()?.ends_with(".graph.json")
        {
            return None;
        }
        let graph: ObjectiveGraph = serde_json::from_reader(fs::File::open(path).ok()?).ok()?;
        (graph.active_goal_id.as_deref() == Some(goal_id)
            || graph.nodes.iter().any(|node| node.goal_id == goal_id))
        .then_some(graph)
    })
}

fn visible_plan(store: &StateStore, goal_id: &str) -> Option<PlanGraph> {
    let canonical = store.read_plan_graph(goal_id).ok().flatten();
    let unreviewed = store.read_unreviewed_plan_graph(goal_id).ok().flatten();
    select_visible_plan(canonical, unreviewed)
}

fn select_visible_plan(
    canonical: Option<PlanGraph>,
    unreviewed: Option<PlanGraph>,
) -> Option<PlanGraph> {
    match (canonical, unreviewed) {
        (Some(canonical), Some(unreviewed)) if unreviewed.revision > canonical.revision => {
            Some(unreviewed)
        }
        (Some(canonical), _) => Some(canonical),
        (None, unreviewed) => unreviewed,
    }
}

fn matching_plan_node_runs(
    ledger: Option<PlanNodeRunLedger>,
    plan: Option<&PlanGraph>,
) -> Option<PlanNodeRunLedger> {
    ledger.filter(|ledger| {
        plan.is_some_and(|plan| {
            ledger.plan_id == plan.plan_id && ledger.plan_hash == plan.plan_hash
        })
    })
}

fn review_summary(
    store: &StateStore,
    goal: Option<&Goal>,
    plan: Option<&PlanGraph>,
    epoch_id: Option<&str>,
    events: &[crate::state::GoalEpochEvent],
) -> Option<GearRuntimeReviewSummary> {
    let goal = goal?;
    let plan_revision = plan.map(|plan| plan.revision);
    let bundle = plan_revision.and_then(|revision| {
        store
            .read_review_epoch_bundle(&goal.id, revision)
            .ok()
            .flatten()
    });
    let bundle_complete = bundle.as_ref().map(|bundle| bundle.complete);
    let roles = bundle
        .as_ref()
        .map(|bundle| {
            bundle
                .roles
                .iter()
                .map(|role| {
                    format!(
                        "{} execution={} session={} receipt={}",
                        role.role,
                        role.execution_id,
                        role.actual_session_id
                            .as_deref()
                            .unwrap_or(&role.phase_session_id),
                        role.receipt_path
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let critic = plan_revision
        .and_then(|revision| {
            store
                .read_plan_critic_receipt(&goal.id, revision)
                .ok()
                .flatten()
        })
        .filter(|receipt| {
            plan.is_some_and(|plan| {
                receipt.plan_id == plan.plan_id && receipt.plan_hash == plan.plan_hash
            })
        });
    let oracle = plan_revision
        .and_then(|revision| {
            store
                .read_plan_oracle_receipt(&goal.id, revision)
                .ok()
                .flatten()
        })
        .filter(|receipt| {
            plan.is_some_and(|plan| {
                receipt.plan_id == plan.plan_id && receipt.plan_hash == plan.plan_hash
            })
        });
    let critic_findings = critic
        .as_ref()
        .map(|receipt| bounded_critic_findings("PlanCritic", receipt))
        .unwrap_or_default();
    let revision_instructions = critic.as_ref().and_then(|receipt| {
        receipt
            .verdict
            .revision_instructions
            .as_ref()
            .map(|text| text.chars().take(1200).collect())
    });
    let oracle_findings = oracle
        .as_ref()
        .map(|receipt| bounded_critic_findings("Oracle", receipt))
        .unwrap_or_default();
    let oracle_revision_instructions = oracle.as_ref().and_then(|receipt| {
        receipt
            .verdict
            .revision_instructions
            .as_ref()
            .map(|text| text.chars().take(1200).collect())
    });
    let critic_decision = critic.map(|receipt| format!("{:?}", receipt.verdict.decision));
    let oracle_decision = oracle.map(|receipt| format!("{:?}", receipt.verdict.decision));
    let latest_event = events.last().map(|event| format!("{:?}", event.kind));
    Some(GearRuntimeReviewSummary {
        status: if bundle_complete == Some(true) {
            "complete".to_string()
        } else {
            "pending".to_string()
        },
        epoch_events: events
            .iter()
            .filter(|event| epoch_id.is_none_or(|id| id == event.epoch_id))
            .count(),
        latest_event,
        plan_revision,
        bundle_complete,
        roles,
        critic_findings,
        revision_instructions,
        oracle_findings,
        oracle_revision_instructions,
        critic_decision,
        oracle_decision,
        blockers: repository_observation_review_blockers(bundle.as_ref()),
    })
}

fn repository_observation_review_blockers(bundle: Option<&ReviewEpochBundle>) -> Vec<String> {
    let Some(bundle) = bundle else {
        return Vec::new();
    };
    bundle
        .roles
        .iter()
        .filter_map(|role| {
            let path = role.observation_path.as_deref()?;
            let contents = match fs::read_to_string(path) {
                Ok(contents) => contents,
                Err(error) => {
                    return Some(format!(
                        "{} repository observation unavailable · {} · {}",
                        role.role, path, error
                    ));
                }
            };
            let receipt: RepositoryObservationReceipt = match serde_json::from_str(&contents) {
                Ok(receipt) => receipt,
                Err(error) => {
                    return Some(format!(
                        "{} repository observation invalid · {} · {}",
                        role.role, path, error
                    ));
                }
            };
            if let Err(error) = receipt.validate() {
                return Some(format!(
                    "{} repository observation invalid · {} · {}",
                    role.role, path, error
                ));
            }
            if receipt.status == RepositoryObservationStatus::Unverified {
                return Some(format!(
                    "{} repository observation unverified · {}",
                    role.role, path
                ));
            }
            None
        })
        .take(8)
        .collect()
}

fn bounded_critic_findings(
    role: &str,
    receipt: &crate::plan_review::PlanCriticReceipt,
) -> Vec<String> {
    receipt
        .verdict
        .findings
        .iter()
        .take(8)
        .map(|finding| {
            let task = finding.task_id.as_deref().unwrap_or("plan");
            let detail = finding
                .required_change
                .as_deref()
                .unwrap_or(finding.message.as_str());
            let text = format!(
                "{} · {:?} · {} · {}: {}",
                role, finding.severity, finding.code, task, detail
            );
            text.chars().take(600).collect()
        })
        .collect()
}

fn feedback_summary(
    store: &StateStore,
    task_manager: Option<&TaskManagerSnapshot>,
) -> GearRuntimeFeedbackSummary {
    let mut summary = GearRuntimeFeedbackSummary::default();
    let mut observed_task_ids = HashSet::new();
    if let Some(task_manager) = task_manager {
        for task in task_manager.tasks.iter().take(32) {
            observed_task_ids.insert(task.task_id.clone());
            add_worker_feedback(&mut summary, &store.worker_dir(&task.task_id));
        }
    }
    // Durable projections may be rendered after the live TaskManager has been
    // dropped. Include bounded worker artifacts in that case, while avoiding
    // double counting task directories already represented above.
    let Ok(entries) = fs::read_dir(store.workers_dir()) else {
        return summary;
    };
    for entry in entries.flatten().take(64) {
        let task_id = entry.file_name().to_string_lossy().into_owned();
        if !observed_task_ids.insert(task_id) {
            continue;
        }
        if entry.file_type().is_ok_and(|file_type| file_type.is_dir()) {
            add_worker_feedback(&mut summary, &entry.path());
        }
    }
    summary
}

fn add_worker_feedback(summary: &mut GearRuntimeFeedbackSummary, worker_dir: &Path) {
    let tool_events = bounded_line_count(&worker_dir.join("tool-events.jsonl"));
    let permission_events = bounded_line_count(&worker_dir.join("permission-events.jsonl"));
    let task_events = bounded_line_count(&worker_dir.join("task-events.jsonl"));
    let worker_events = bounded_line_count(&worker_dir.join("worker-events.jsonl"));
    summary.tool_calls = summary.tool_calls.saturating_add(tool_events);
    summary.permission_events = summary.permission_events.saturating_add(permission_events);
    summary.task_events = summary.task_events.saturating_add(task_events);
    summary.worker_errors = summary.worker_errors.saturating_add(worker_events);
}

fn feedback_events(
    store: &StateStore,
    task_manager: Option<&TaskManagerSnapshot>,
) -> Vec<GearRuntimeFeedbackEvent> {
    let mut task_ids = task_manager
        .into_iter()
        .flat_map(|snapshot| snapshot.tasks.iter().map(|task| task.task_id.clone()))
        .collect::<Vec<_>>();
    if task_ids.is_empty() {
        if let Ok(entries) = fs::read_dir(store.workers_dir()) {
            task_ids.extend(
                entries
                    .flatten()
                    .take(64)
                    .filter(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_dir()))
                    .map(|entry| entry.file_name().to_string_lossy().into_owned()),
            );
        }
    }
    task_ids.sort();
    task_ids.dedup();

    let mut events = Vec::new();
    for task_id in task_ids.into_iter().take(32) {
        let worker_dir = store.worker_dir(&task_id);
        for (kind, file_name) in [
            ("tool", "tool-events.jsonl"),
            ("permission", "permission-events.jsonl"),
            ("task", "task-events.jsonl"),
            ("worker", "worker-events.jsonl"),
        ] {
            for message in bounded_file_tail(&worker_dir.join(file_name))
                .lines()
                .rev()
                .take(4)
                .map(str::to_string)
            {
                events.push(GearRuntimeFeedbackEvent {
                    task_id: task_id.clone(),
                    kind: kind.to_string(),
                    message: GearRuntimeEventEnvelope::bounded_message(
                        message,
                        GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                    ),
                });
                if events.len() >= 32 {
                    return events;
                }
            }
        }
    }
    events.reverse();
    events
}

fn bounded_line_count(path: &Path) -> usize {
    let Ok(metadata) = fs::metadata(path) else {
        return 0;
    };
    let Ok(mut file) = fs::File::open(path) else {
        return 0;
    };
    let start = metadata
        .len()
        .saturating_sub(GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES as u64);
    if file.seek(std::io::SeekFrom::Start(start)).is_err() {
        return 0;
    }
    let mut tail = String::new();
    if file.read_to_string(&mut tail).is_err() {
        return 0;
    }
    tail.lines().count()
}

fn read_timeline(store: &StateStore, session_id: &str) -> Vec<GearRuntimeEventEnvelope> {
    let path = store.events_path(session_id);
    let sequence_base = fs::metadata(&path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let tail = bounded_file_tail(&path);
    tail.lines()
        .enumerate()
        .filter_map(|(sequence, line)| {
            serde_json::from_str::<Event>(line).ok().map(|event| {
                let class = match &event.kind {
                    EventKind::WorkerOutput => GearRuntimeEventClass::Telemetry,
                    EventKind::WorkerFailed
                    | EventKind::GoalBlocked
                    | EventKind::GoalLimited
                    | EventKind::ContinuationStopped
                    | EventKind::VerificationFailed => GearRuntimeEventClass::Critical,
                    _ => GearRuntimeEventClass::Milestone,
                };
                GearRuntimeEventEnvelope {
                    // Use the durable byte position as a monotonic cursor
                    // base. The tail is bounded, so line indexes alone would
                    // reset to zero on every refresh and hide new events.
                    sequence: sequence_base.saturating_add(sequence as u64),
                    class,
                    semantic_key: format!(
                        "{:?}:{}",
                        event.kind,
                        event.task_id.as_deref().unwrap_or("")
                    ),
                    session_id: event.session_id,
                    objective_id: None,
                    goal_id: event.goal_id,
                    task_id: event.task_id,
                    run_epoch: None,
                    message: GearRuntimeEventEnvelope::bounded_message(
                        event.message,
                        GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                    ),
                    payload: Some(event.data),
                }
            })
        })
        .collect()
}

fn bounded_file_tail(path: &Path) -> String {
    let Ok(metadata) = fs::metadata(path) else {
        return String::new();
    };
    let Ok(mut file) = fs::File::open(path) else {
        return String::new();
    };
    let start = metadata
        .len()
        .saturating_sub((GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES * 4) as u64);
    if file.seek(std::io::SeekFrom::Start(start)).is_err() {
        return String::new();
    }
    let mut tail = String::new();
    if file.read_to_string(&mut tail).is_err() {
        return String::new();
    }
    if start > 0 {
        if let Some(newline) = tail.find('\n') {
            tail.drain(..=newline);
        }
    }
    tail
}

#[derive(Clone, Debug)]
pub struct GearRuntimeEventBuffer {
    events: VecDeque<GearRuntimeEventEnvelope>,
    capacity: usize,
    dropped_telemetry: u64,
    coalesced_telemetry: u64,
    refresh_required: bool,
}

impl Default for GearRuntimeEventBuffer {
    fn default() -> Self {
        Self::new(GEAR_GUI_EVENT_BUFFER_CAPACITY)
    }
}

impl GearRuntimeEventBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            events: VecDeque::with_capacity(capacity),
            capacity: capacity.max(1),
            dropped_telemetry: 0,
            coalesced_telemetry: 0,
            refresh_required: false,
        }
    }

    pub fn push(&mut self, event: GearRuntimeEventEnvelope) {
        if event.class == GearRuntimeEventClass::Telemetry {
            if let Some(existing) = self.events.iter_mut().rev().find(|existing| {
                existing.class == GearRuntimeEventClass::Telemetry
                    && existing.semantic_key == event.semantic_key
            }) {
                *existing = event;
                self.coalesced_telemetry = self.coalesced_telemetry.saturating_add(1);
                return;
            }
        }

        if self.events.len() >= self.capacity {
            if let Some(index) = self
                .events
                .iter()
                .position(|existing| !existing.class.is_lossless())
            {
                self.events.remove(index);
                self.dropped_telemetry = self.dropped_telemetry.saturating_add(1);
            } else if event.class.is_lossless() {
                self.refresh_required = true;
                return;
            } else {
                self.dropped_telemetry = self.dropped_telemetry.saturating_add(1);
                return;
            }
        }
        self.events.push_back(event);
    }

    pub fn drain(&mut self) -> Vec<GearRuntimeEventEnvelope> {
        self.events.drain(..).collect()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn dropped_telemetry(&self) -> u64 {
        self.dropped_telemetry
    }

    pub fn coalesced_telemetry(&self) -> u64 {
        self.coalesced_telemetry
    }

    pub fn refresh_required(&self) -> bool {
        self.refresh_required
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Budget, EventKind, Goal, GoalStatus, Session, event as state_event};
    use anyhow::Context;
    use serde_json::json;

    fn event(class: GearRuntimeEventClass, key: &str, sequence: u64) -> GearRuntimeEventEnvelope {
        GearRuntimeEventEnvelope {
            sequence,
            class,
            semantic_key: key.to_string(),
            session_id: "session".to_string(),
            objective_id: Some("objective".to_string()),
            goal_id: Some("goal".to_string()),
            task_id: Some("task".to_string()),
            run_epoch: Some(0),
            message: "event".to_string(),
            payload: None,
        }
    }

    #[test]
    fn telemetry_is_coalesced_without_growing_the_buffer() {
        let mut buffer = GearRuntimeEventBuffer::new(4);
        for sequence in 0..100_000 {
            buffer.push(event(
                GearRuntimeEventClass::Telemetry,
                "task/output",
                sequence,
            ));
        }
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.coalesced_telemetry(), 99_999);
        assert_eq!(buffer.dropped_telemetry(), 0);
    }

    #[test]
    fn plan_coverage_counts_declared_obligations_without_inventing_evidence() {
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let draft = crate::plan_graph::deterministic_fallback_draft(
            "Implement the bounded feature",
            &scope,
            &["cargo test".to_string()],
        );
        let plan = crate::plan_graph::PlanGraph::seal(
            "goal-coverage",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            draft,
        )
        .expect("fallback plan should validate");
        let mut ledger = PlanNodeRunLedger::from_plan("goal-coverage", "epoch-1", &plan)
            .expect("ledger should be created");
        ledger.nodes[0].status = PlanNodeRunStatus::Completed;

        let coverage = plan_coverage_summary(&plan, Some(&ledger), None);
        assert_eq!(coverage.work_orders_total, plan.draft.tasks.len());
        assert_eq!(coverage.work_orders_completed, 1);
        assert!(coverage.acceptance_total > 0);
        assert!(coverage.qa_total > 0);
        assert_eq!(coverage.acceptance_satisfied, 0);
        assert_eq!(coverage.qa_satisfied, 0);
    }

    #[test]
    fn plan_revision_diff_reads_the_adjacent_durable_candidate() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let store = StateStore::new(root.path());
        store.initialize()?;
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let old = crate::plan_graph::PlanGraph::seal(
            "goal-revision-diff",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("old objective", &scope, &[]),
        )?;
        let mut new_draft =
            crate::plan_graph::deterministic_fallback_draft("new objective", &scope, &[]);
        new_draft.tasks[0].title = "changed task".to_string();
        let new = crate::plan_graph::PlanGraph::seal(
            "goal-revision-diff",
            2,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            new_draft,
        )?;
        store.write_plan_candidate(&old)?;
        store.write_plan_candidate(&new)?;

        let diff = plan_revision_diff(&store, Some("goal-revision-diff"), &new)
            .context("adjacent revision diff should be readable")?;
        assert_eq!(diff.from_revision, 1);
        assert_eq!(diff.to_revision, 2);
        assert!(diff.objective_changed);
        assert_eq!(diff.changed_tasks, vec![new.draft.tasks[0].task_id.clone()]);
        assert!(diff.added_tasks.is_empty());
        assert!(diff.removed_tasks.is_empty());
        Ok(())
    }

    #[test]
    fn critical_events_survive_telemetry_pressure() {
        let mut buffer = GearRuntimeEventBuffer::new(2);
        buffer.push(event(GearRuntimeEventClass::Telemetry, "a", 1));
        buffer.push(event(GearRuntimeEventClass::Telemetry, "b", 2));
        buffer.push(event(GearRuntimeEventClass::Critical, "terminal", 3));
        let events = buffer.drain();
        assert!(events.iter().any(|event| event.semantic_key == "terminal"));
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn bounded_message_respects_utf8_and_byte_limit() {
        let message = GearRuntimeEventEnvelope::bounded_message("你好世界".repeat(100), 32);
        assert!(message.len() <= 32);
        assert!(message.is_char_boundary(message.len()));
        assert!(message.ends_with("[truncated]"));
    }

    #[test]
    fn worker_feedback_excerpt_is_bounded() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("worker-last-message.md");
        fs::write(&path, "worker feedback ".repeat(200))?;
        let excerpt = worker_last_message_excerpt(path.to_str().context("utf8 path")?)
            .context("feedback excerpt should be readable")?;
        assert!(excerpt.len() <= 1200);
        assert!(excerpt.ends_with("[truncated]"));
        Ok(())
    }

    #[test]
    fn ownership_projection_reads_durable_route_event() {
        let mut route_event = event(GearRuntimeEventClass::Milestone, "unused", 1);
        route_event.semantic_key = "PhaseRouteSelected:worker-task".to_string();
        route_event.payload = Some(json!({
            "ownership_delegated": true,
            "ownership_worker_kind": "opencode",
            "ownership_worker_task_id": "worker-task",
            "ownership_route_reason": "configured worker route",
        }));
        let ownership = ownership_from_timeline(&[route_event]).expect("ownership projection");
        assert_eq!(ownership.delegated, Some(true));
        assert_eq!(ownership.worker_kind.as_deref(), Some("opencode"));
        assert_eq!(ownership.worker_task_id.as_deref(), Some("worker-task"));
        assert_eq!(
            ownership.route_reason.as_deref(),
            Some("configured worker route")
        );
    }

    #[test]
    fn process_classifier_keeps_runtime_health_names_bounded() {
        assert_eq!(classify_process_name("cargo"), Some("cargo"));
        assert_eq!(
            classify_process_name("rust-analyzer"),
            Some("rust_analyzer")
        );
        assert_eq!(
            classify_process_name("rust_analyzer"),
            Some("rust_analyzer")
        );
        assert_eq!(classify_process_name("opencode"), Some("opencode"));
        assert_eq!(classify_process_name("bash"), None);
    }

    #[test]
    fn broker_session_projection_reports_active_and_terminal() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let store = StateStore::new(root.path());
        let active = root
            .path()
            .join(".gear/broker-sessions/planner/goal/task/1/session-active");
        fs::create_dir_all(&active)?;
        fs::write(
            active.join("session-identity.json"),
            br#"{"backend_kind":"zed_agent","session_id":"session-active"}"#,
        )?;
        let terminal = root
            .path()
            .join(".gear/broker-sessions/reviewer/goal/task/1/session-done");
        fs::create_dir_all(&terminal)?;
        fs::write(
            terminal.join("session-identity.json"),
            br#"{"backend_kind":"zed_agent","session_id":"session-done"}"#,
        )?;
        fs::write(
            terminal.join("terminal-outcome.json"),
            br#"{"status":"completed"}"#,
        )?;
        let foreign = root
            .path()
            .join(".gear/broker-sessions/executor/foreign-goal/task/1/session-foreign");
        fs::create_dir_all(&foreign)?;
        fs::write(
            foreign.join("session-identity.json"),
            br#"{"backend_kind":"zed_agent","session_id":"session-foreign"}"#,
        )?;

        let summaries = broker_session_summaries(&store, Some("goal"));
        assert!(
            summaries
                .iter()
                .any(|summary| summary.contains("session-active · active"))
        );
        assert!(
            summaries
                .iter()
                .any(|summary| summary.contains("session-done · completed"))
        );
        assert!(
            !summaries
                .iter()
                .any(|summary| summary.contains("session-foreign"))
        );
        Ok(())
    }

    #[test]
    fn required_artifact_projection_tracks_durable_file_presence() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let store = StateStore::new(root.path());
        let artifact = crate::plan_graph::PlanArtifactContract {
            path: ".gear/artifacts/report.md".to_string(),
            description: "report".to_string(),
            required: true,
        };
        assert!(required_artifact_summary(&store, &artifact).starts_with("[ ]"));
        fs::create_dir_all(root.path().join(".gear/artifacts"))?;
        fs::write(root.path().join(".gear/artifacts/report.md"), "done")?;
        assert!(required_artifact_summary(&store, &artifact).starts_with("[x]"));
        Ok(())
    }

    #[test]
    fn visible_plan_projects_unreviewed_graph_before_approval() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let store = StateStore::new(root.path());
        store.initialize()?;
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let graph = crate::plan_graph::PlanGraph::seal(
            "goal-unreviewed-gui",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft(
                "show the pending plan",
                &scope,
                &["echo verify".to_string()],
            ),
        )?;
        store.write_unreviewed_plan_graph(&graph)?;
        assert_eq!(visible_plan(&store, "goal-unreviewed-gui"), Some(graph));
        Ok(())
    }

    #[test]
    fn visible_plan_prefers_newer_unreviewed_revision() -> anyhow::Result<()> {
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let canonical = crate::plan_graph::PlanGraph::seal(
            "goal-revision-gui",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("old plan", &scope, &[]),
        )?;
        let unreviewed = crate::plan_graph::PlanGraph::seal(
            "goal-revision-gui",
            2,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("new plan", &scope, &[]),
        )?;
        assert_eq!(
            select_visible_plan(Some(canonical), Some(unreviewed.clone())),
            Some(unreviewed)
        );
        Ok(())
    }

    #[test]
    fn review_projection_uses_the_visible_candidate_revision() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let store = StateStore::new(root.path());
        store.initialize()?;
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let candidate = crate::plan_graph::PlanGraph::seal(
            "goal-review-candidate",
            2,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("candidate", &scope, &[]),
        )?;
        store.write_unreviewed_plan_graph(&candidate)?;
        let goal = Goal {
            id: "goal-review-candidate".to_string(),
            title: "candidate review".to_string(),
            status: GoalStatus::Planning,
            workspace: root.path().display().to_string(),
            created_at: "2026-07-16T00:00:00Z".to_string(),
            updated_at: "2026-07-16T00:00:00Z".to_string(),
            request: "review candidate".to_string(),
            product_type: "tool".to_string(),
            language_profile: "rust".to_string(),
            success_criteria: vec!["candidate is visible".to_string()],
            budget: Budget::default(),
            current_task_id: None,
            coordinator_model: None,
            coordinator_brief: None,
            summary: String::new(),
        };
        let events = Vec::new();
        let visible = visible_plan(&store, &goal.id);
        let review = review_summary(&store, Some(&goal), visible.as_ref(), None, &events)
            .expect("visible plan should produce a review projection");
        assert_eq!(review.plan_revision, Some(2));
        Ok(())
    }

    #[test]
    fn review_projection_exposes_unverified_repository_observation() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let store = StateStore::new(root.path());
        store.initialize()?;
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let plan = crate::plan_graph::PlanGraph::seal(
            "goal-review-blocker",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("blocker", &scope, &[]),
        )?;
        let observation = RepositoryObservationReceipt::seal(
            "plan_critic",
            "goal-review-blocker",
            &plan.plan_id,
            plan.revision,
            &plan.plan_hash,
            "critic-task",
            "critic-session",
            None,
            0,
            Vec::new(),
            Vec::new(),
        )?;
        let observation_path = store.write_repository_observation_receipt(&observation)?;
        let roles = ["planner", "momus", "oracle"]
            .into_iter()
            .map(|role| crate::state::ReviewEpochRoleEvidence {
                role: role.to_string(),
                execution_id: format!("{role}-execution"),
                phase_session_id: format!("{role}-session"),
                actual_session_id: None,
                receipt_hash: format!("{role}-receipt"),
                receipt_path: format!("{role}-receipt.json"),
                observation_path: (role == "plan_critic")
                    .then(|| observation_path.to_string_lossy().into_owned()),
                requested_tokens: None,
                actual_tokens: None,
                cost_micros: None,
                duration_ms: None,
                cache_hit: None,
                unknown_reason: Some("provider did not report usage".to_string()),
            })
            .chain(std::iter::once(crate::state::ReviewEpochRoleEvidence {
                role: "plan_critic".to_string(),
                execution_id: "critic-execution".to_string(),
                phase_session_id: "critic-session".to_string(),
                actual_session_id: None,
                receipt_hash: "critic-receipt".to_string(),
                receipt_path: "critic-receipt.json".to_string(),
                observation_path: Some(observation_path.to_string_lossy().into_owned()),
                requested_tokens: None,
                actual_tokens: None,
                cost_micros: None,
                duration_ms: None,
                cache_hit: None,
                unknown_reason: Some("provider did not report usage".to_string()),
            }))
            .collect();
        let bundle = ReviewEpochBundle::seal(
            "goal-review-blocker",
            "epoch-1",
            &plan,
            roles,
            false,
        )?;
        let blockers = repository_observation_review_blockers(Some(&bundle));
        assert_eq!(blockers.len(), 1);
        assert!(blockers
            .iter()
            .all(|blocker| blocker.contains("repository observation unverified")));
        Ok(())
    }

    #[test]
    fn visible_plan_does_not_reuse_a_previous_revision_ledger() -> anyhow::Result<()> {
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let old_plan = crate::plan_graph::PlanGraph::seal(
            "goal-ledger-gui",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("old plan", &scope, &[]),
        )?;
        let new_plan = crate::plan_graph::PlanGraph::seal(
            "goal-ledger-gui",
            2,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("new plan", &scope, &[]),
        )?;
        let ledger = PlanNodeRunLedger::from_plan("goal-ledger-gui", "epoch-1", &old_plan)?;
        assert!(matching_plan_node_runs(Some(ledger), Some(&new_plan)).is_none());
        Ok(())
    }

    #[test]
    fn plan_task_projection_does_not_reuse_a_previous_revision_ledger() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let store = StateStore::new(root.path());
        store.initialize()?;
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let old_plan = crate::plan_graph::PlanGraph::seal(
            "goal-task-projection",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("old plan", &scope, &[]),
        )?;
        let new_plan = crate::plan_graph::PlanGraph::seal(
            "goal-task-projection",
            2,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("new plan", &scope, &[]),
        )?;
        let old_ledger =
            PlanNodeRunLedger::from_plan("goal-task-projection", "epoch-1", &old_plan)?;
        let matching = matching_plan_node_runs(Some(old_ledger), Some(&new_plan));
        let summaries = plan_task_summaries(&store, None, Some(&new_plan), matching.as_ref());
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].status, "Pending");
        assert_eq!(summaries[0].attempt, 0);
        Ok(())
    }

    #[test]
    fn plan_task_projection_includes_durable_session_lifecycle() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let store = StateStore::new(root.path());
        store.initialize()?;
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 4);
        let plan = crate::plan_graph::PlanGraph::seal(
            "goal-session-projection",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("session", &scope, &[]),
        )?;
        let mut ledger = PlanNodeRunLedger::from_plan(
            "goal-session-projection",
            "epoch-1",
            &plan,
        )?;
        ledger.nodes[0].attempt = 1;
        let node = ledger.nodes.first().cloned().expect("fallback task");
        let first_binding = crate::state::PlanNodeSessionBinding {
            schema_version: crate::state::PLAN_NODE_SESSION_BINDING_SCHEMA_VERSION,
            binding_id: "binding-gui-session".to_string(),
            goal_id: ledger.goal_id.clone(),
            epoch_id: ledger.epoch_id.clone(),
            plan_id: ledger.plan_id.clone(),
            plan_revision: ledger.plan_revision,
            plan_hash: ledger.plan_hash.clone(),
            task_id: node.task_id.clone(),
            attempt: 0,
            worker_task_id: "worker-gui-session-0".to_string(),
            worker_kind: "opencode_session".to_string(),
            provider_id: Some("opencode".to_string()),
            model_id: Some("deepseek-v4-flash-free".to_string()),
            session_id: "session-gui-0".to_string(),
            capability_fingerprint: "cap-gui".to_string(),
            route_receipt_hash: None,
            status: crate::state::PlanNodeSessionBindingStatus::Terminal,
            supersedes_binding_id: None,
            created_at: "2026-07-16T00:00:00Z".to_string(),
            updated_at: "2026-07-16T00:05:00Z".to_string(),
        };
        store.write_plan_node_session_binding(&first_binding)?;
        let mut second_binding = first_binding;
        second_binding.binding_id = "binding-gui-session-1".to_string();
        second_binding.attempt = 1;
        second_binding.worker_task_id = "worker-gui-session-1".to_string();
        second_binding.session_id = "session-gui-1".to_string();
        second_binding.status = crate::state::PlanNodeSessionBindingStatus::Active;
        second_binding.created_at = "2026-07-16T00:06:00Z".to_string();
        second_binding.updated_at = "2026-07-16T00:07:00Z".to_string();
        store.write_plan_node_session_binding(&second_binding)?;
        let summaries = plan_task_summaries(&store, None, Some(&plan), Some(&ledger));
        assert_eq!(summaries[0].worker_session_id.as_deref(), Some("session-gui-1"));
        assert_eq!(summaries[0].worker_session_status.as_deref(), Some("Active"));
        assert_eq!(
            summaries[0].worker_session_started_at.as_deref(),
            Some("2026-07-16T00:06:00Z")
        );
        assert_eq!(
            summaries[0].worker_session_updated_at.as_deref(),
            Some("2026-07-16T00:07:00Z")
        );
        assert_eq!(
            summaries[0].worker_session_ended_at.as_deref(),
            None
        );
        assert!(summaries[0].worker_session_elapsed_ms.is_some());
        assert_eq!(summaries[0].worker_session_history.len(), 2);
        assert_eq!(summaries[0].worker_session_history[0].attempt, 0);
        assert_eq!(summaries[0].worker_session_history[1].attempt, node.attempt);
        assert_eq!(summaries[0].worker_session_attempt_count, 2);
        assert_eq!(summaries[0].worker_session_fallback_count, 0);
        assert_eq!(summaries[0].worker_session_elapsed_total_ms, Some(300_000));
        assert_eq!(
            summaries[0].worker_session_history[0].worker_kind,
            "opencode_session"
        );
        Ok(())
    }

    #[test]
    fn plan_session_aggregate_counts_route_changes_without_double_counting_retries() {
        let history = vec![
            GearRuntimePlanSessionSummary {
                attempt: 0,
                worker_task_id: "worker-0".to_string(),
                worker_kind: "opencode_session".to_string(),
                model_id: Some("mimo".to_string()),
                session_id: "session-0".to_string(),
                status: "Terminal".to_string(),
                started_at: "2026-07-15T00:00:00Z".to_string(),
                ended_at: Some("2026-07-15T00:01:00Z".to_string()),
                elapsed_ms: Some(60_000),
                ..Default::default()
            },
            GearRuntimePlanSessionSummary {
                attempt: 1,
                worker_task_id: "worker-1".to_string(),
                worker_kind: "opencode_session".to_string(),
                model_id: Some("mimo".to_string()),
                session_id: "session-1".to_string(),
                status: "Terminal".to_string(),
                started_at: "2026-07-15T00:02:00Z".to_string(),
                ended_at: Some("2026-07-15T00:03:00Z".to_string()),
                elapsed_ms: Some(60_000),
                ..Default::default()
            },
            GearRuntimePlanSessionSummary {
                attempt: 2,
                worker_task_id: "worker-2".to_string(),
                worker_kind: "opencode_session".to_string(),
                worker_category: Some("deep".to_string()),
                model_id: Some("deepseek".to_string()),
                session_id: "session-2".to_string(),
                status: "Active".to_string(),
                started_at: "2026-07-15T00:04:00Z".to_string(),
                elapsed_ms: Some(30_000),
                route_fallback_count: 1,
                ..Default::default()
            },
        ];
        assert_eq!(plan_session_aggregate(&history), (3, 1, Some(150_000)));
    }

    #[test]
    fn plan_progress_exposes_the_next_non_terminal_work_order() {
        let tasks = vec![
            GearRuntimePlanTaskSummary {
                task_id: "task-1".to_string(),
                title: "done".to_string(),
                goal: String::new(),
                deliverable: String::new(),
                rationale: String::new(),
                approach: Vec::new(),
                role: "build".to_string(),
                actual_worker_kind: None,
                actual_worker_model: None,
                route_hint: None,
                status: "Completed".to_string(),
                contract_status: "accepted".to_string(),
                dependencies: Vec::new(),
                inputs: Vec::new(),
                preconditions: Vec::new(),
                already_in_working_tree: Vec::new(),
                still_needed: Vec::new(),
                parallel_wave: 0,
                current: false,
                attempt: 1,
                worker_task_id: Some("worker-1".to_string()),
                worker_session_id: Some("session-1".to_string()),
                worker_session_status: Some("Terminal".to_string()),
                worker_session_started_at: Some("2026-07-16T00:00:00Z".to_string()),
                worker_session_updated_at: Some("2026-07-16T00:05:00Z".to_string()),
                worker_session_ended_at: Some("2026-07-16T00:05:00Z".to_string()),
                worker_session_elapsed_ms: Some(300_000),
                worker_session_history: Vec::new(),
                worker_session_attempt_count: 0,
                worker_session_fallback_count: 0,
                worker_session_elapsed_total_ms: None,
                error: None,
                routing_brief_path: None,
                preflight_path: None,
                preflight_satisfied: false,
                preflight_checks: Vec::new(),
                worker_result_path: None,
                worker_outcome_path: None,
                worker_last_message_path: None,
                worker_last_message_excerpt: None,
                worker_changed_files: Vec::new(),
                worker_commands_run: Vec::new(),
                worker_known_failures: Vec::new(),
                worker_next_steps: Vec::new(),
                worker_plan_gap: None,
                worker_decision: "NotRecorded".to_string(),
                worker_decision_reason: None,
                worker_evidence_quality: "Unclassified".to_string(),
                red_evidence_path: None,
                green_evidence_paths: Vec::new(),
                review_evidence_path: None,
                commit_boundary_evidence_path: None,
                commit_boundary_satisfied: None,
                size_tier: "Small".to_string(),
                risk_tier: "Normal".to_string(),
                commit_boundary: "NoCommit".to_string(),
                commit_message: None,
                must_do: Vec::new(),
                evidence: Vec::new(),
                rollback: Vec::new(),
                budget: PlanTaskBudget::default(),
                execution_steps: Vec::new(),
                execution_steps_evidence_required: false,
                must_not_do: Vec::new(),
                completion_predicates: Vec::new(),
                required_capabilities: Vec::new(),
                references: Vec::new(),
                required_artifacts: Vec::new(),
                allowed_files: Vec::new(),
                forbidden_files: Vec::new(),
                write_scope: Vec::new(),
                max_files_changed: 0,
                test_strategy: "TestsAfter".to_string(),
                verification_commands: Vec::new(),
                qa_scenarios: Vec::new(),
            },
            GearRuntimePlanTaskSummary {
                task_id: "task-2".to_string(),
                title: "next".to_string(),
                goal: String::new(),
                deliverable: String::new(),
                rationale: String::new(),
                approach: Vec::new(),
                role: "build".to_string(),
                actual_worker_kind: None,
                actual_worker_model: None,
                route_hint: None,
                status: "Runnable".to_string(),
                contract_status: "pending".to_string(),
                dependencies: vec!["task-1".to_string()],
                inputs: Vec::new(),
                preconditions: Vec::new(),
                already_in_working_tree: Vec::new(),
                still_needed: Vec::new(),
                parallel_wave: 1,
                current: true,
                attempt: 1,
                worker_task_id: Some("worker-2".to_string()),
                worker_session_id: Some("session-2".to_string()),
                worker_session_status: Some("Active".to_string()),
                worker_session_started_at: Some("2026-07-16T00:06:00Z".to_string()),
                worker_session_updated_at: Some("2026-07-16T00:07:00Z".to_string()),
                worker_session_ended_at: None,
                worker_session_elapsed_ms: Some(60_000),
                worker_session_history: Vec::new(),
                worker_session_attempt_count: 0,
                worker_session_fallback_count: 0,
                worker_session_elapsed_total_ms: None,
                error: None,
                routing_brief_path: None,
                preflight_path: None,
                preflight_satisfied: false,
                preflight_checks: Vec::new(),
                worker_result_path: None,
                worker_outcome_path: None,
                worker_last_message_path: None,
                worker_last_message_excerpt: None,
                worker_changed_files: Vec::new(),
                worker_commands_run: Vec::new(),
                worker_known_failures: Vec::new(),
                worker_next_steps: Vec::new(),
                worker_plan_gap: None,
                worker_decision: "NotRecorded".to_string(),
                worker_decision_reason: None,
                worker_evidence_quality: "Unclassified".to_string(),
                red_evidence_path: None,
                green_evidence_paths: Vec::new(),
                review_evidence_path: None,
                commit_boundary_evidence_path: None,
                commit_boundary_satisfied: None,
                size_tier: "Small".to_string(),
                risk_tier: "Normal".to_string(),
                commit_boundary: "NoCommit".to_string(),
                commit_message: None,
                must_do: Vec::new(),
                evidence: Vec::new(),
                rollback: Vec::new(),
                budget: PlanTaskBudget::default(),
                execution_steps: Vec::new(),
                execution_steps_evidence_required: false,
                must_not_do: Vec::new(),
                completion_predicates: Vec::new(),
                required_capabilities: Vec::new(),
                references: Vec::new(),
                required_artifacts: Vec::new(),
                allowed_files: Vec::new(),
                forbidden_files: Vec::new(),
                write_scope: Vec::new(),
                max_files_changed: 0,
                test_strategy: "TestsAfter".to_string(),
                verification_commands: Vec::new(),
                qa_scenarios: Vec::new(),
            },
        ];
        assert_eq!(tasks[0].contract_status, "accepted");
        assert_eq!(tasks[1].contract_status, "pending");
        assert_eq!(
            plan_progress_summary(&tasks),
            (2, 1, Some("task-2".to_string()), Some("next".to_string()))
        );
    }

    #[test]
    fn plan_contract_status_distinguishes_blocked_from_failed() -> anyhow::Result<()> {
        let scope = crate::state::Scope::new(vec!["src".to_string()], vec![".git".to_string()], 1);
        let plan = crate::plan_graph::PlanGraph::seal(
            "goal-contract-status",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            crate::plan_graph::deterministic_fallback_draft("status", &scope, &[]),
        )?;
        let mut ledger =
            crate::state::PlanNodeRunLedger::from_plan("goal-contract-status", "epoch-1", &plan)?;
        assert_eq!(plan_contract_status(None), "pending");
        ledger.nodes[0].status = crate::state::PlanNodeRunStatus::Failed;
        assert_eq!(plan_contract_status(Some(&ledger.nodes[0])), "failed");
        ledger.nodes[0].status = crate::state::PlanNodeRunStatus::NeedsUser;
        assert_eq!(plan_contract_status(Some(&ledger.nodes[0])), "blocked");
        ledger.nodes[0].status = crate::state::PlanNodeRunStatus::Completed;
        assert_eq!(plan_contract_status(Some(&ledger.nodes[0])), "accepted");
        Ok(())
    }

    #[test]
    fn plan_progress_uses_full_graph_and_only_terminal_completion() -> anyhow::Result<()> {
        let scope = crate::state::Scope::new(Vec::new(), vec![".git".to_string()], 256);
        let mut draft = crate::plan_graph::deterministic_fallback_draft("large plan", &scope, &[]);
        for index in 1..130 {
            let mut task = draft.tasks[0].clone();
            task.task_id = format!("task-{index:03}");
            task.title = format!("task {index}");
            task.scope.write_scope = vec![format!("src/file-{index:03}.rs")];
            draft.tasks.push(task);
        }
        let plan = PlanGraph::seal(
            "goal-large",
            1,
            crate::plan_graph::PlanSource::DeterministicFallback,
            None,
            draft,
        )?;
        let mut ledger =
            crate::state::PlanNodeRunLedger::from_plan("goal-large", "epoch-1", &plan)?;
        ledger.nodes[0].status = PlanNodeRunStatus::GreenVerified;
        let progress = plan_progress_from_graph(Some(&plan), Some(&ledger));
        assert_eq!(progress.0, 130);
        assert_eq!(progress.1, 0);
        assert_eq!(progress.2.as_deref(), Some("task_003"));
        Ok(())
    }

    #[test]
    fn bounded_snapshot_keeps_recent_timeline_and_worker_tail() {
        let mut snapshot = GearRuntimeSnapshot {
            schema_version: GEAR_GUI_SNAPSHOT_SCHEMA_VERSION,
            sequence: 1,
            workspace: "workspace".to_string(),
            session_id: "session".to_string(),
            objective_id: None,
            goal_id: None,
            epoch_id: None,
            objective: None,
            goal: None,
            request_summary: "request".to_string(),
            lifecycle: GearRuntimeLifecycle::default(),
            budget: GearRuntimeBudgetSummary::default(),
            review: None,
            recovery: GearRuntimeRecoverySummary::default(),
            feedback: GearRuntimeFeedbackSummary::default(),
            feedback_events: Vec::new(),
            plan_tasks: Vec::new(),
            plan_total: 0,
            plan_completed: 0,
            next_plan_task_id: None,
            next_plan_task_title: None,
            plan_execution_mode: "serial_work_orders".to_string(),
            task_manager: None,
            timeline: Vec::new(),
            health: GearRuntimeHealth::default(),
        };
        for sequence in 0..(GEAR_GUI_TIMELINE_CAPACITY + 7) {
            snapshot.timeline.push(event(
                GearRuntimeEventClass::Milestone,
                "milestone",
                sequence as u64,
            ));
        }
        snapshot.task_manager = Some(TaskManagerSnapshot {
            counts: Default::default(),
            artifacts_root: None,
            tasks: Vec::new(),
            current_output: Some("x".repeat(GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES + 100)),
        });

        let bounded = snapshot.bounded_for_ui();
        assert_eq!(bounded.timeline.len(), GEAR_GUI_TIMELINE_CAPACITY);
        assert_eq!(bounded.timeline[0].sequence, 7);
        assert!(
            bounded
                .task_manager
                .as_ref()
                .and_then(|tasks| tasks.current_output.as_ref())
                .is_some_and(|output| output.len() <= GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES)
        );
        assert!(bounded.validate().is_ok());
    }

    #[test]
    fn long_session_snapshot_serialization_stays_bounded() {
        let mut snapshot = GearRuntimeSnapshot {
            schema_version: GEAR_GUI_SNAPSHOT_SCHEMA_VERSION,
            sequence: 0,
            workspace: "workspace".to_string(),
            session_id: "session".to_string(),
            objective_id: Some("objective".to_string()),
            goal_id: Some("goal".to_string()),
            epoch_id: Some("epoch".to_string()),
            objective: None,
            goal: None,
            request_summary: "request".to_string(),
            lifecycle: GearRuntimeLifecycle::default(),
            budget: GearRuntimeBudgetSummary::default(),
            review: None,
            recovery: GearRuntimeRecoverySummary::default(),
            feedback: GearRuntimeFeedbackSummary::default(),
            feedback_events: (0..100_000)
                .map(|index| GearRuntimeFeedbackEvent {
                    task_id: format!("task-{}", index % 32),
                    kind: "worker".to_string(),
                    message: "bounded worker output".to_string(),
                })
                .collect(),
            plan_tasks: Vec::new(),
            plan_total: 0,
            plan_completed: 0,
            next_plan_task_id: None,
            next_plan_task_title: None,
            plan_execution_mode: "serial_work_orders".to_string(),
            task_manager: None,
            timeline: (0..100_000)
                .map(|sequence| event(GearRuntimeEventClass::Telemetry, "worker/output", sequence))
                .collect(),
            health: GearRuntimeHealth::default(),
        };

        snapshot = snapshot.bounded_for_ui();
        let serialized = serde_json::to_vec(&snapshot).expect("serialize bounded snapshot");
        assert!(serialized.len() <= 512 * 1024);
        assert_eq!(snapshot.timeline.len(), GEAR_GUI_TIMELINE_CAPACITY);
        assert_eq!(snapshot.feedback_events.len(), 32);
        snapshot.validate().expect("validate bounded snapshot");
    }

    #[test]
    fn event_ledger_tail_is_bounded_and_line_aligned() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("events.jsonl");
        let content = (0..20_000)
            .map(|index| format!("{{\"sequence\":{index}}}\n"))
            .collect::<String>();
        std::fs::write(&path, content).expect("write event fixture");
        let tail = bounded_file_tail(&path);
        assert!(tail.len() <= GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES * 4);
        assert!(tail.starts_with('{'));
        assert!(tail.ends_with('\n'));
    }

    #[test]
    fn durable_projection_survives_without_live_tasks() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = StateStore::new(directory.path());
        store.initialize().expect("initialize state store");
        store
            .write_goal(&Goal {
                id: "goal-gui".to_string(),
                title: "GUI projection".to_string(),
                status: GoalStatus::Running,
                workspace: directory.path().display().to_string(),
                created_at: "2026-07-14T00:00:00Z".to_string(),
                updated_at: "2026-07-14T00:00:01Z".to_string(),
                request: "show runtime state".to_string(),
                product_type: "tool".to_string(),
                language_profile: "rust".to_string(),
                success_criteria: vec!["snapshot".to_string()],
                budget: Budget::default(),
                current_task_id: None,
                coordinator_model: None,
                coordinator_brief: None,
                summary: "running without a live task".to_string(),
            })
            .expect("write goal");
        store
            .write_session(&Session {
                id: "session-gui".to_string(),
                workspace: directory.path().display().to_string(),
                created_at: "2026-07-14T00:00:00Z".to_string(),
                updated_at: "2026-07-14T00:00:01Z".to_string(),
                current_goal_id: "goal-gui".to_string(),
            })
            .expect("write session");
        store
            .append_event(&state_event(
                "session-gui",
                None,
                None,
                EventKind::ContinuationStarted,
                "continuation started",
                json!({"sequence": 7}),
            ))
            .expect("write timeline event");
        store
            .write_artifact(
                "goal-gui",
                "strategist-next-goal-receipt.json",
                &serde_json::to_string(&json!({
                    "verdict": {
                        "decision": "continue",
                        "next_objective": "show the next objective",
                        "answerable_now": false,
                        "acceptance_signals": ["receipt is visible"],
                        "required_questions": [],
                        "evidence_refs": ["artifact:final-report.md"]
                    }
                }))
                .expect("serialize strategist receipt"),
            )
            .expect("write strategist receipt");
        let task_record = serde_json::to_string(&json!({
            "task_id": "task-gui",
            "worker_kind": "opencode_session",
            "worker_command": null,
            "worker_model": "opencode/deepseek-v4-flash-free",
            "worker_category": "execute",
            "route_hint": "execute",
            "route_reason": "durable fixture",
            "status": "failed",
            "started_at": "2026-07-14T00:00:00Z",
            "finished_at": "2026-07-14T00:00:01Z",
            "residency_state": "persisted_only",
            "run_epoch": 2,
            "notified_epoch": 2,
            "notification_failed_epoch": null,
            "killed": false,
            "session_id": "worker-session",
            "parent_session_id": null,
                    "root_session_id": "session-gui",
            "parent_task_id": null,
            "result_path": null,
            "outcome_path": null,
            "summary": "worker failed durably",
            "failure_kind": null,
            "retry_reason": "retry from GUI",
            "error": "provider unavailable",
            "attempts": []
        }))
        .expect("serialize task fixture");
        store
            .write_worker_file("task-gui", "task-record.json", &format!("{task_record}\n"))
            .expect("write durable task");
        store
            .write_worker_file(
                "task-gui",
                "permission-events.jsonl",
                "{\"status\":\"approved\",\"tool\":\"write\"}\n",
            )
            .expect("write feedback event");

        let snapshot = GearRuntimeSnapshot::from_store(
            &store,
            directory.path().display().to_string(),
            "session-gui",
            None,
        )
        .expect("project durable state");
        assert_eq!(snapshot.goal_id.as_deref(), Some("goal-gui"));
        assert_eq!(
            snapshot.goal.as_ref().map(|goal| goal.status.as_str()),
            Some("Running")
        );
        assert!(snapshot.recovery.continuation_status.is_none());
        let task_manager = snapshot
            .task_manager
            .as_ref()
            .expect("durable task projection should be visible after restart");
        assert_eq!(task_manager.tasks.len(), 1);
        assert_eq!(task_manager.tasks[0].task_id, "task-gui");
        assert_eq!(
            task_manager.tasks[0].worker_model.as_deref(),
            Some("opencode/deepseek-v4-flash-free")
        );
        assert!(task_manager.tasks[0].messageability.is_none());
        assert_eq!(snapshot.feedback_events.len(), 1);
        assert_eq!(snapshot.feedback_events[0].kind, "permission");
        let next_goal = snapshot
            .lifecycle
            .next_goal
            .as_ref()
            .expect("next-goal receipt should be projected");
        assert_eq!(next_goal.decision, "continue");
        assert_eq!(
            next_goal.next_objective.as_deref(),
            Some("show the next objective")
        );
        assert!(snapshot.sequence > 0);
        assert!(snapshot.health.last_activity_at.is_some());
        snapshot.validate().expect("validate snapshot");

        let first_sequence = snapshot.sequence;
        store
            .append_event(&state_event(
                "session-gui",
                Some("goal-gui"),
                Some("task-gui"),
                EventKind::WorkerOutput,
                "worker output appended",
                json!({"delta":"bounded"}),
            ))
            .expect("append second timeline event");
        let refreshed = GearRuntimeSnapshot::from_store(
            &store,
            directory.path().display().to_string(),
            "session-gui",
            None,
        )
        .expect("refresh durable projection");
        assert!(refreshed.sequence > first_sequence);
    }

    #[test]
    fn corrupt_session_state_surfaces_as_snapshot_error() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = StateStore::new(directory.path());
        store.initialize().expect("initialize state store");
        std::fs::write(store.sessions_dir().join("broken.json"), "{\n")
            .expect("write corrupt session");

        let error = GearRuntimeSnapshot::from_store(
            &store,
            directory.path().display().to_string(),
            "broken",
            None,
        )
        .expect_err("corrupt session must not silently disappear");
        assert!(error.to_string().contains("failed to parse"));

        std::fs::write(
            store.sessions_dir().join("broken.json"),
            serde_json::to_string(&Session {
                id: "broken".to_string(),
                workspace: directory.path().display().to_string(),
                created_at: "2026-07-15T00:00:00Z".to_string(),
                updated_at: "2026-07-15T00:00:01Z".to_string(),
                current_goal_id: String::new(),
            })
            .expect("serialize repaired session"),
        )
        .expect("repair session");
        GearRuntimeSnapshot::from_store(
            &store,
            directory.path().display().to_string(),
            "broken",
            None,
        )
        .expect("repaired session should recover snapshot");
    }
}
