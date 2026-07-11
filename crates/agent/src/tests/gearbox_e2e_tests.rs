//! Gearbox orchestration component tests.
//!
//! 1. Small task delegation to Zed Agent
//! 2. Medium task capability routing to external workers
//! 3. Session close/recovery with lineage consistency
//! 4. Worker complete but review missing → incomplete status
//! 5. Review passes → complete status with readable artifacts
//!
//! These tests isolate registry, persistence, and review-gate behavior with fake
//! collaborators. The real ACP/GPUI lifecycle coverage lives in `agent.rs`.

use super::*;
use gearbox_agent::plan_graph::deterministic_fallback_draft;
use gearbox_agent::runtime::{
    ReviewDimension, ReviewDimensionResult, ReviewGate, ReviewerEvidence,
};
use gearbox_agent::state::{Scope, StateStore, WorkLineage};
use gearbox_agent::workers::{
    NativeWorkerBackend, WorkerCapabilities, WorkerCategory, WorkerConfig, WorkerKind,
    WorkerOutcome, WorkerRegistry, WorkerResult, WorkerSessionHandle, WorkerStartRequest,
};
use language_model::LanguageModelRegistry;
use pretty_assertions::assert_eq;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

// ─── Test helpers ───────────────────────────────────────────────────────────

struct FakeWorkerShared {
    last_start_request_summary: Option<String>,
    session_id: Option<String>,
}

struct FakeWorkerSession {
    state: Arc<Mutex<FakeWorkerShared>>,
}

impl WorkerSessionHandle for FakeWorkerSession {
    fn session_id(&self) -> Option<String> {
        self.state.lock().ok().and_then(|s| s.session_id.clone())
    }
    fn send_follow_up(&self, _prompt: String) -> anyhow::Result<()> {
        Ok(())
    }
    fn steer(&self, _prompt: String) -> anyhow::Result<()> {
        Ok(())
    }
    fn interrupt(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn cancel(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn wait_for_outcome(&self) -> anyhow::Result<WorkerOutcome> {
        anyhow::bail!("not supported")
    }
    fn wait_for_result(&self) -> anyhow::Result<WorkerResult> {
        anyhow::bail!("not supported")
    }
    fn last_output(&self) -> Option<String> {
        None
    }
}

struct FakeNativeBackend {
    state: Arc<Mutex<FakeWorkerShared>>,
}

impl FakeNativeBackend {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeWorkerShared {
                last_start_request_summary: None,
                session_id: Some("fake-session-id".to_string()),
            })),
        }
    }
}

impl NativeWorkerBackend for FakeNativeBackend {
    fn start_zed_agent(
        &self,
        request: WorkerStartRequest<'_>,
    ) -> anyhow::Result<Arc<dyn WorkerSessionHandle>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        state.last_start_request_summary = Some(format!(
            "task={}, goal={}, route_attempt={}",
            request.task.id, request.goal, request.route_attempt
        ));
        drop(state);
        Ok(Arc::new(FakeWorkerSession {
            state: self.state.clone(),
        }))
    }
}

fn test_task(id: &str) -> gearbox_agent::state::Task {
    gearbox_agent::state::Task {
        id: id.to_string(),
        goal_id: "goal_test".to_string(),
        parent_task_id: None,
        title: "test task".to_string(),
        kind: gearbox_agent::state::TaskKind::Edit,
        status: gearbox_agent::state::TaskStatus::Pending,
        assigned_worker: Some("zed_agent".to_string()),
        attempt: 1,
        scope: gearbox_agent::state::Scope::new(Vec::new(), Vec::new(), 10),
        inputs: gearbox_agent::state::TaskInputs::default(),
        outputs: gearbox_agent::state::TaskOutputs::default(),
    }
}

fn test_worker_config() -> WorkerConfig {
    WorkerConfig {
        worker_kind: WorkerKind::ZedAgent,
        worker_command: None,
        worker_model: None,
        worker_routes: Vec::new(),
        unavailable_worker_models: Vec::new(),
        premium_worker_budget: 1,
        max_parallel_workers: 1,
        max_parallel_per_key: 1,
        stale_task_timeout_secs: 30,
        skip_worker: false,
        require_worker: true,
        default_worker_for_small_tasks: WorkerKind::ZedAgent,
    }
}

fn make_request<'a>(
    store: &'a StateStore,
    workspace: &'a Path,
    task: &'a gearbox_agent::state::Task,
    goal: &'a str,
    config: &'a WorkerConfig,
) -> WorkerStartRequest<'a> {
    WorkerStartRequest {
        store,
        workspace,
        task,
        route_attempt: 1,
        goal,
        verification_commands: &[],
        config,
        cancellation_token: None,
        coordinator_model: None,
        coordinator_brief: None,
        route_hint: None,
    }
}

fn gate_without_review_evidence() -> ReviewGate {
    ReviewGate {
        require_all_pass: true,
        results: vec![
            ReviewDimensionResult {
                dimension: ReviewDimension::GoalVerification,
                passed: true,
                evidence: "verification passed and coordinator accepted the goal".to_string(),
                reviewer_evidence: None,
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::CodeQuality,
                passed: true,
                evidence: "scope checks are clean".to_string(),
                reviewer_evidence: None,
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::Security,
                passed: true,
                evidence: "no forbidden paths were touched".to_string(),
                reviewer_evidence: None,
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::QaExecution,
                passed: true,
                evidence: "verification commands passed".to_string(),
                reviewer_evidence: None,
            },
        ],
    }
}

fn gate_with_review_evidence() -> ReviewGate {
    ReviewGate {
        require_all_pass: true,
        results: vec![
            ReviewDimensionResult {
                dimension: ReviewDimension::GoalVerification,
                passed: true,
                evidence: "verification passed and coordinator accepted the goal".to_string(),
                reviewer_evidence: Some(ReviewerEvidence {
                    execution_id: "worker-session-42_GoalVerification".to_string(),
                    reviewed_execution_id: "executor-session-41".to_string(),
                    route: "GoalVerification".to_string(),
                    model: Some("test/reviewer".to_string()),
                    artifact_path: Some("/tmp/goal-verification.md".to_string()),
                    verdict: "pass".to_string(),
                    findings: vec!["goal evidence inspected".to_string()],
                }),
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::CodeQuality,
                passed: true,
                evidence: "scope checks are clean".to_string(),
                reviewer_evidence: Some(ReviewerEvidence {
                    execution_id: "worker-session-42_CodeQuality".to_string(),
                    reviewed_execution_id: "executor-session-41".to_string(),
                    route: "CodeQuality".to_string(),
                    model: Some("test/reviewer".to_string()),
                    artifact_path: Some("/tmp/code-quality.md".to_string()),
                    verdict: "pass".to_string(),
                    findings: vec!["quality evidence inspected".to_string()],
                }),
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::Security,
                passed: true,
                evidence: "no forbidden paths were touched".to_string(),
                reviewer_evidence: Some(ReviewerEvidence {
                    execution_id: "worker-session-42_Security".to_string(),
                    reviewed_execution_id: "executor-session-41".to_string(),
                    route: "Security".to_string(),
                    model: Some("test/reviewer".to_string()),
                    artifact_path: Some("/tmp/security.md".to_string()),
                    verdict: "pass".to_string(),
                    findings: vec!["security evidence inspected".to_string()],
                }),
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::QaExecution,
                passed: true,
                evidence: "verification commands passed".to_string(),
                reviewer_evidence: Some(ReviewerEvidence {
                    execution_id: "worker-session-42_QaExecution".to_string(),
                    reviewed_execution_id: "executor-session-41".to_string(),
                    route: "QaExecution".to_string(),
                    model: Some("test/reviewer".to_string()),
                    artifact_path: Some("/tmp/qa-execution.md".to_string()),
                    verdict: "pass".to_string(),
                    findings: vec!["qa evidence inspected".to_string()],
                }),
            },
        ],
    }
}

// ─── Test 1: Small task delegation to Zed Agent ────────────────────────────

#[gpui::test]
async fn test_gearbox_component_small_task_delegation(_cx: &mut TestAppContext) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let store = StateStore::new(tmp.path());
    store.initialize().expect("failed to initialize StateStore");

    let backend = FakeNativeBackend::new();
    let registry = WorkerRegistry::with_native_backend(Arc::new(backend));

    let task = test_task("test-task-id");
    let config = test_worker_config();

    let request = make_request(&store, tmp.path(), &task, "test small task goal", &config);
    let handle = registry
        .start(request)
        .expect("WorkerRegistry::start should succeed");

    let session_id = handle.session_id();
    assert!(
        session_id.is_some(),
        "Worker session handle should have a session_id"
    );
    assert_eq!(
        session_id.as_deref(),
        Some("fake-session-id"),
        "Worker session should report correct session ID"
    );

    let caps = WorkerCapabilities::command();
    assert!(
        caps.supports_category(WorkerCategory::Quick),
        "command worker should support Quick"
    );
}

// ─── Test 2: Capability routing ─────────────────────────────────────────────

#[gpui::test]
async fn test_gearbox_component_capability_routing(_cx: &mut TestAppContext) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let store = StateStore::new(tmp.path());
    store.initialize().expect("failed to initialize StateStore");

    let backend = FakeNativeBackend::new();
    let registry = WorkerRegistry::with_native_backend(Arc::new(backend));

    let task = test_task("test-task-id");
    let config = test_worker_config();

    for category in &[
        WorkerCategory::Quick,
        WorkerCategory::Deep,
        WorkerCategory::Repair,
        WorkerCategory::Review,
        WorkerCategory::Explore,
    ] {
        let goal = format!("test category routing for {c:?}", c = category);
        let request = make_request(&store, tmp.path(), &task, &goal, &config);
        let result = registry.start(request);
        assert!(
            result.is_ok(),
            "WorkerRegistry::start should succeed for category {category:?}"
        );
    }

    let code_caps = WorkerCapabilities::command();
    for edit_category in &[
        WorkerCategory::Quick,
        WorkerCategory::Deep,
        WorkerCategory::Repair,
    ] {
        assert!(
            code_caps.supports_category(*edit_category),
            "command worker should support {edit_category:?}"
        );
    }
}

// ─── Test 3: Session recovery with lineage consistency ──────────────────────

#[gpui::test]
async fn test_gearbox_component_session_recovery(_cx: &mut TestAppContext) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let store = StateStore::new(tmp.path());
    store.initialize().expect("failed to initialize StateStore");

    let root_session_id = "gear-session-root-1";
    let mut lineage = WorkLineage::new(root_session_id.to_string());
    lineage.worker_session_ids.push("worker-1".to_string());
    lineage.worker_session_ids.push("worker-2".to_string());
    lineage.active_task_ids.push("task-1".to_string());
    lineage.active_task_ids.push("task-2".to_string());
    lineage.plan_remaining_items = 2;
    lineage.updated_at = "2025-01-01T00:01:00Z".to_string();

    let path = store
        .write_lineage(&lineage)
        .expect("should persist lineage");
    assert!(path.exists(), "Lineage file should exist at {path:?}");

    let session_record = gearbox_agent::state::Session {
        id: root_session_id.to_string(),
        workspace: tmp.path().to_string_lossy().to_string(),
        created_at: "2025-01-01T00:00:00Z".to_string(),
        updated_at: "2025-01-01T00:01:00Z".to_string(),
        current_goal_id: "goal-1".to_string(),
    };
    store
        .write_session(&session_record)
        .expect("should write session");

    drop(store);

    let recovered_store = StateStore::new(tmp.path());
    recovered_store.initialize().expect("should re-initialize");

    let recovered_lineage = recovered_store
        .read_lineage(root_session_id)
        .expect("should read lineage after recovery")
        .expect("lineage should exist after recovery");

    assert_eq!(
        recovered_lineage.root_session_id, root_session_id,
        "Root session ID should match after recovery"
    );
    assert_eq!(
        recovered_lineage.worker_session_ids.len(),
        2,
        "Should have 2 workers after recovery, got {}",
        recovered_lineage.worker_session_ids.len()
    );
    assert_eq!(
        recovered_lineage.active_task_ids.len(),
        2,
        "Should have 2 active tasks after recovery"
    );
    assert_eq!(
        recovered_lineage.plan_remaining_items, 2,
        "plan_remaining_items should be preserved"
    );
    assert!(
        recovered_lineage
            .worker_session_ids
            .contains(&"worker-1".to_string()),
        "Lineage should include worker-1"
    );
    assert!(
        recovered_lineage
            .worker_session_ids
            .contains(&"worker-2".to_string()),
        "Lineage should include worker-2"
    );
}

// ─── Test 4: Incomplete without review ──────────────────────────────────────

#[gpui::test]
async fn test_gearbox_component_incomplete_without_review(_cx: &mut TestAppContext) {
    let gate = gate_without_review_evidence();

    assert!(
        gate.require_all_pass,
        "ReviewGate should require all dimensions"
    );
    assert!(
        gate.results.iter().all(|r| r.passed),
        "All dimensions should pass individually"
    );

    for result in &gate.results {
        assert!(
            result.reviewer_evidence.is_none(),
            "Dimension {:?} should have NO reviewer evidence (no real review ran)",
            result.dimension,
        );
    }
}

// ─── Test 5: Complete after review ──────────────────────────────────────────

#[gpui::test]
async fn test_gearbox_component_complete_after_review(_cx: &mut TestAppContext) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let store = StateStore::new(tmp.path());
    store.initialize().expect("failed to initialize StateStore");

    let artifact_path = store
        .write_artifact("goal-1", "review-output.md", "All checks passed.")
        .expect("should write artifact");
    assert!(
        artifact_path.exists(),
        "Artifact should exist on disk at {artifact_path:?}"
    );

    let gate = gate_with_review_evidence();

    assert!(
        gate.require_all_pass,
        "ReviewGate should require all dimensions"
    );
    assert!(
        gate.results.iter().all(|r| r.passed),
        "All dimensions should pass"
    );

    for result in &gate.results {
        assert!(
            result.reviewer_evidence.is_some(),
            "Dimension {:?} should have real reviewer evidence",
            result.dimension,
        );
        if let Some(ref evidence) = result.reviewer_evidence {
            assert!(
                evidence.execution_id.contains("worker-session-42"),
                "Evidence execution_id should contain the worker session ID: {}",
                evidence.execution_id,
            );
            assert_eq!(
                evidence.verdict, "pass",
                "All dimensions should have 'pass' verdict"
            );
        }
    }

    let artifact_content = std::fs::read_to_string(&artifact_path).expect("should read artifact");
    assert_eq!(
        artifact_content.trim(),
        "All checks passed.",
        "Artifact content should match what was written"
    );
}

// ─── Test 6: Production broker e2e — fails now, passes after GBX-007-003/004 ─

async fn broker_e2e_wait_for_completion(
    model: &FakeLanguageModel,
    cx: &mut TestAppContext,
) {
    for _ in 0..100 {
        cx.run_until_parked();
        if model.completion_count() > 0 {
            return;
        }
        cx.background_executor
            .timer(Duration::from_millis(10))
            .await;
    }
    panic!("timed out waiting for fake model completion request");
}

fn broker_e2e_respond_to_completions(
    model: Arc<dyn LanguageModel>,
    finished: Arc<AtomicBool>,
) -> thread::JoinHandle<usize> {
    thread::spawn(move || {
        let model = model.as_fake();
        let mut completion_count = 0;
        loop {
            let deadline = Instant::now() + Duration::from_secs(10);
            while model.completion_count() == 0 {
                if finished.load(Ordering::SeqCst) {
                    return completion_count;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for native Gear worker model request"
                );
                thread::yield_now();
            }
            let request = model.pending_completions().last().cloned().unwrap();
            let request_text = request
                .messages
                .iter()
                .map(|m| m.string_contents())
                .collect::<Vec<_>>()
                .join("\n");
            let response = if request_text.contains("Gear's high-reasoning planner") {
                let objective = request
                    .messages
                    .last()
                    .map(|m| m.string_contents())
                    .unwrap_or_else(|| "Build the requested feature".to_string());
                serde_json::to_string(&deterministic_fallback_draft(
                    &objective,
                    &Scope::new(Vec::new(), vec![".git".to_string()], 10),
                    &["npm run build".to_string()],
                ))
                .unwrap()
            } else if request_text.contains("Gear's read-only PlanCritic") {
                let evidence = request
                    .messages
                    .last()
                    .map(|m| m.string_contents())
                    .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok())
                    .unwrap();
                let plan_hash = evidence["plan"]["plan_hash"].as_str().unwrap();
                let goal_id = evidence["plan"]["goal_id"].as_str().unwrap();
                let plan_id = evidence["plan"]["plan_id"].as_str().unwrap();
                let plan_revision = evidence["plan"]["revision"].as_u64().unwrap();
                let planner_execution_id =
                    evidence["planner_receipt"]["identity"]["execution_id"]
                        .as_str()
                        .unwrap();
                json!({
                    "schema_version": 1,
                    "reviewed_goal_id": goal_id,
                    "reviewed_plan_id": plan_id,
                    "reviewed_plan_revision": plan_revision,
                    "reviewed_plan_hash": plan_hash,
                    "reviewed_planner_execution_id": planner_execution_id,
                    "decision": "approve",
                    "checks": [
                        {"dimension":"references","verdict":"pass","summary":"ok","evidence_refs":["verifier:reference_paths"]},
                        {"dimension":"executability","verdict":"pass","summary":"ok","evidence_refs":["plan:tasks"]},
                        {"dimension":"contradictions","verdict":"pass","summary":"ok","evidence_refs":["plan:must_have"]},
                        {"dimension":"scope","verdict":"pass","summary":"ok","evidence_refs":["verifier:scope"]},
                        {"dimension":"tdd","verdict":"pass","summary":"ok","evidence_refs":["verifier:test_contract"]},
                        {"dimension":"qa","verdict":"pass","summary":"ok","evidence_refs":["verifier:qa_contract"]},
                        {"dimension":"acceptance","verdict":"pass","summary":"ok","evidence_refs":["verifier:acceptance_contract"]}
                    ],
                    "findings": [],
                    "revision_instructions": null,
                    "needs_user_reason": null,
                    "summary": "sealed plan and deterministic evidence are decision complete"
                })
                .to_string()
            } else if request_text.contains("Gear's coordinator review hook") {
                "GOAL_SATISFIED: yes\nSUMMARY: deterministic verification and worker evidence are ready for the required independent review\nREPAIR_REQUEST: none\nROUTE_HINT: none\nSTOP_REASON: complete"
                    .to_string()
            } else if request_text.contains("read-only final-review phase") {
                let reviewed_execution_id = request_text
                    .split("reviewed_execution_id `")
                    .nth(1)
                    .and_then(|value| value.split('`').next())
                    .unwrap_or("missing-executor-id");
                json!({
                    "schema_version": 1,
                    "reviewed_execution_id": reviewed_execution_id,
                    "dimensions": [
                        {"dimension": "goal_verification", "verdict": "pass", "findings": ["goal and verification artifacts inspected"]},
                        {"dimension": "code_quality", "verdict": "pass", "findings": ["bounded implementation evidence inspected"]},
                        {"dimension": "security", "verdict": "pass", "findings": ["forbidden path evidence inspected"]},
                        {"dimension": "qa_execution", "verdict": "pass", "findings": ["build verification evidence inspected"]}
                    ]
                })
                .to_string()
            } else {
                "## Summary\nImplemented the bounded worker task.\n\n## Changed Files\n- none\n\n## Commands Run\n- npm run build\n\n## Known Failures\n- none"
                    .to_string()
            };
            model.send_completion_stream_text_chunk(&request, response);
            model.end_last_completion_stream();
            completion_count += 1;
        }
    })
}

#[gpui::test]
async fn gearbox_production_phase_broker_e2e(cx: &mut TestAppContext) {
    init_test(cx);
    cx.update(|cx| {
        LanguageModelRegistry::test(cx);
    });

    let workspace = tempfile::tempdir().unwrap();
    std::fs::write(workspace.path().join("README.md"), "# Gear test\n").unwrap();
    std::fs::write(
        workspace.path().join("package.json"),
        r#"{"scripts":{"build":"echo build-ok"}}"#,
    )
    .unwrap();

    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/", json!({ "a": {} })).await;
    let project = Project::test(fs.clone(), [Path::new("/a")], cx).await;
    let thread_store = cx.new(|cx| ThreadStore::new(cx));
    let agent = cx.update(|cx| NativeAgent::new(thread_store, Templates::new(), fs, cx));
    agent.update(cx, |agent, _cx| {
        agent.gear_worker_config_override = Some(WorkerConfig {
            worker_kind: WorkerKind::ZedAgent,
            worker_command: None,
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
            default_worker_for_small_tasks: WorkerKind::ZedAgent,
        });
    });
    let connection = Rc::new(NativeAgentConnection::gear(agent.clone()));

    let acp_thread = cx
        .update(|cx| {
            connection.clone().new_session(
                project.clone(),
                PathList::new(&[workspace.path()]),
                cx,
            )
        })
        .await
        .unwrap();
    let model = cx.update(|cx| {
        LanguageModelRegistry::read_global(cx)
            .default_model()
            .map(|default_model| default_model.model)
            .expect("default test model should be available")
    });
    let fake_model = model.as_fake();
    let prompt_task = cx.update(|cx| {
        acp_thread.update(cx, |thread, cx| {
            thread.send(vec!["Build a tiny notes app MVP".into()], cx)
        })
    });
    let prompt_task = cx.foreground_executor().spawn(prompt_task);
    broker_e2e_wait_for_completion(fake_model, cx).await;
    let planner_draft = deterministic_fallback_draft(
        "Build a tiny notes app MVP",
        &Scope::new(Vec::new(), vec![".git".to_string()], 10),
        &["npm run build".to_string()],
    );
    fake_model
        .send_last_completion_stream_text_chunk(serde_json::to_string(&planner_draft).unwrap());
    fake_model.end_last_completion_stream();
    let gear_finished = Arc::new(AtomicBool::new(false));
    let worker_responder = broker_e2e_respond_to_completions(model, gear_finished.clone());
    cx.executor().allow_parking();
    prompt_task.await.unwrap();
    gear_finished.store(true, Ordering::SeqCst);
    assert_eq!(
        worker_responder.join().unwrap(),
        3,
        "Gear should approve the plan, execute one native implementation worker, and run one independent final reviewer"
    );
    cx.run_until_parked();

    let gearbox_root = workspace.path().join(".gearbox-agent");
    assert!(
        gearbox_root.join("artifacts").is_dir(),
        "Orchestrator must have run and created artifacts directory"
    );
    assert!(
        gearbox_root.join("goals").is_dir(),
        "Orchestrator must have persisted goals"
    );

    let goal_dirs: Vec<_> = std::fs::read_dir(gearbox_root.join("artifacts"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    assert!(
        !goal_dirs.is_empty(),
        "At least one goal-id directory must exist under artifacts/"
    );

    let broker_sessions_exists = goal_dirs
        .iter()
        .any(|e| e.path().join("broker-sessions").is_dir());

    assert!(
        broker_sessions_exists,
        "FAILS NOW — broker-sessions/ does not exist in any goal directory under artifacts/. \
         This proves PhaseRuntime.broker is None in send_gear_prompt. \
         \
         After GBX-007-003/004 wiring (broker factory + ACP production registration), \
         broker-sessions/ WILL be created and this assertion PASSES. \
         \
         Current code path: send_gear_prompt → PhaseRuntime {{ broker: None, ... }} → \
         run_phase_via_broker(None, ...) → immediate `else` return → no broker artifacts."
    );
}
