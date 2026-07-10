#[cfg(test)]
pub mod test_support {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use anyhow::Result;

    use crate::state::{ContinuationStatus, CoordinatorModel, Scope, StateStore, Task, TaskInputs, TaskKind, TaskOutputs, TaskStatus, WorkLineage};
    use crate::task_manager::{ManagedTaskStatus, ResidencyState, TaskAttempt, TaskFailureKind, TaskRecord};
    use crate::tools::CancellationToken;
    use crate::workers::{
        NativeWorkerBackend, WorkerAdapter, WorkerCapabilities, WorkerConfig, WorkerKind,
        WorkerOutcome, WorkerResult, WorkerRoute, WorkerRunRequest, WorkerSessionHandle,
        WorkerStartRequest, WorkerStatus, WorkerSubscription,
    };

    // ── FakeWorkerState ──────────────────────────────────────────────────

    /// Shared mutable state for all fake worker implementations.
    /// Test code can inspect this after exercising code that uses the fakes.
    #[derive(Clone, Debug)]
    pub struct FakeWorkerState {
        pub session_id: Option<String>,
        pub cancelled: bool,
        pub interrupted: bool,
        pub last_follow_up: Option<String>,
        pub last_steer: Option<String>,
        pub stored_result: Option<WorkerResult>,
        pub stored_outcome: Option<WorkerOutcome>,
        /// Human-readable summary of the most recent `WorkerStartRequest`.
        pub last_start_request_summary: Option<String>,
    }

    impl FakeWorkerState {
        pub fn new(session_id: impl Into<String>) -> Self {
            Self {
                session_id: Some(session_id.into()),
                cancelled: false,
                interrupted: false,
                last_follow_up: None,
                last_steer: None,
                stored_result: None,
                stored_outcome: None,
                last_start_request_summary: None,
            }
        }

        /// Set a result that `wait_for_result()` should return.
        pub fn with_result(mut self, result: WorkerResult) -> Self {
            self.stored_result = Some(result);
            self
        }

        /// Set an outcome that `wait_for_outcome()` should return.
        pub fn with_outcome(mut self, outcome: WorkerOutcome) -> Self {
            self.stored_outcome = Some(outcome);
            self
        }
    }

    impl Default for FakeWorkerState {
        fn default() -> Self {
            Self {
                session_id: Some("fake-session-id".to_string()),
                cancelled: false,
                interrupted: false,
                last_follow_up: None,
                last_steer: None,
                stored_result: None,
                stored_outcome: None,
                last_start_request_summary: None,
            }
        }
    }

    // ── FakeWorkerSessionHandle ──────────────────────────────────────────

    /// A `WorkerSessionHandle` backed by shared `FakeWorkerState`.
    /// Does not perform any real async I/O.
    pub struct FakeWorkerSessionHandle {
        state: Arc<Mutex<FakeWorkerState>>,
    }

    impl FakeWorkerSessionHandle {
        pub fn new(state: Arc<Mutex<FakeWorkerState>>) -> Self {
            Self { state }
        }

        /// Return a clone of the shared state handle for inspection.
        pub fn state(&self) -> Arc<Mutex<FakeWorkerState>> {
            self.state.clone()
        }
    }

    impl WorkerSessionHandle for FakeWorkerSessionHandle {
        fn session_id(&self) -> Option<String> {
            self.state
                .lock()
                .ok()
                .and_then(|s| s.session_id.clone())
        }

        fn send_follow_up(&self, prompt: String) -> Result<()> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("FakeWorkerState mutex poisoned"))?;
            state.last_follow_up = Some(prompt);
            Ok(())
        }

        fn steer(&self, prompt: String) -> Result<()> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("FakeWorkerState mutex poisoned"))?;
            state.last_steer = Some(prompt);
            Ok(())
        }

        fn interrupt(&self) -> Result<()> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("FakeWorkerState mutex poisoned"))?;
            state.interrupted = true;
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("FakeWorkerState mutex poisoned"))?;
            state.cancelled = true;
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            let state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("FakeWorkerState mutex poisoned"))?;
            state
                .stored_outcome
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no outcome stored in FakeWorkerState"))
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            let state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("FakeWorkerState mutex poisoned"))?;
            state
                .stored_result
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no result stored in FakeWorkerState"))
        }

        fn last_output(&self) -> Option<String> {
            self.state
                .lock()
                .ok()
                .and_then(|s| s.stored_result.as_ref().map(|r| r.summary.clone()))
        }
    }

    // ── FakeNativeWorkerBackend ──────────────────────────────────────────

    /// A `NativeWorkerBackend` that captures the start request and returns
    /// a `FakeWorkerSessionHandle`.
    pub struct FakeNativeWorkerBackend {
        state: Arc<Mutex<FakeWorkerState>>,
    }

    impl FakeNativeWorkerBackend {
        pub fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeWorkerState::default())),
            }
        }

        pub fn with_state(state: FakeWorkerState) -> Self {
            Self {
                state: Arc::new(Mutex::new(state)),
            }
        }

        /// Provide access to the shared state for test assertions.
        pub fn state(&self) -> Arc<Mutex<FakeWorkerState>> {
            self.state.clone()
        }
    }

    impl Default for FakeNativeWorkerBackend {
        fn default() -> Self {
            Self::new()
        }
    }

    impl NativeWorkerBackend for FakeNativeWorkerBackend {
        fn start_zed_agent(
            &self,
            request: WorkerStartRequest<'_>,
        ) -> Result<Arc<dyn WorkerSessionHandle>> {
            {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("FakeWorkerState mutex poisoned"))?;
                state.last_start_request_summary = Some(format!(
                    "task={}, goal={}, route_attempt={}",
                    request.task.id, request.goal, request.route_attempt
                ));
            }
            Ok(Arc::new(FakeWorkerSessionHandle::new(self.state.clone())))
        }
    }

    // ── worker_registry_for_test ─────────────────────────────────────────

    /// Build a `WorkerRegistry` wired to a `FakeNativeWorkerBackend`.
    pub fn worker_registry_for_test() -> WorkerRegistry {
        WorkerRegistry::with_native_backend(Arc::new(FakeNativeWorkerBackend::new()))
    }

    /// Build a `WorkerRegistry` wired to a specific `FakeNativeWorkerBackend`.
    pub fn worker_registry_for_test_with_backend(
        backend: FakeNativeWorkerBackend,
    ) -> WorkerRegistry {
        WorkerRegistry::with_native_backend(Arc::new(backend))
    }

    // ── fake_work_lineage ────────────────────────────────────────────────

    /// Build a `WorkLineage` for testing with the given root session ID.
    pub fn fake_work_lineage(root_session_id: impl Into<String>) -> WorkLineage {
        WorkLineage::new(root_session_id.into())
    }

    // ── FakeCommandWorker ────────────────────────────────────────────────

    /// A `WorkerAdapter` that returns a preset result without executing any
    /// real command.
    pub struct FakeCommandWorker {
        name: &'static str,
        stored_result: Mutex<Option<WorkerResult>>,
    }

    impl FakeCommandWorker {
        pub fn new(name: &'static str) -> Self {
            Self {
                name,
                stored_result: Mutex::new(None),
            }
        }

        pub fn with_result(name: &'static str, result: WorkerResult) -> Self {
            Self {
                name,
                stored_result: Mutex::new(Some(result)),
            }
        }

        pub fn set_result(&self, result: WorkerResult) -> Result<()> {
            *self
                .stored_result
                .lock()
                .map_err(|_| anyhow::anyhow!("FakeCommandWorker mutex poisoned"))? = Some(result);
            Ok(())
        }
    }

    impl WorkerAdapter for FakeCommandWorker {
        fn name(&self) -> &'static str {
            self.name
        }

        fn run(&self, _request: WorkerRunRequest<'_>) -> Result<WorkerResult> {
            self.stored_result
                .lock()
                .map_err(|_| anyhow::anyhow!("FakeCommandWorker mutex poisoned"))?
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no result stored in FakeCommandWorker"))
        }
    }

    // ── Convenience builders ─────────────────────────────────────────────

    /// Build a minimal `WorkerStartRequest` from the given parts.
    ///
    /// All other fields are set to sensible defaults:
    /// - `route_attempt`: 1
    /// - `verification_commands`: empty slice
    /// - `cancellation_token`: `None`
    /// - `coordinator_model`: `None`
    /// - `coordinator_brief`: `None`
    /// - `route_hint`: `None`
    pub fn make_worker_start_request<'a>(
        store: &'a StateStore,
        workspace: &'a Path,
        task: &'a Task,
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

    /// Build a minimal `WorkerConfig` for the given `WorkerKind`.
    ///
    /// All routes and model settings are left empty / default.
    pub fn make_worker_config(kind: WorkerKind) -> WorkerConfig {
        WorkerConfig {
            worker_kind: kind,
            worker_command: None,
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: false,
        }
    }

    /// Build a minimal `Task` for testing.
    pub fn default_task() -> Task {
        Task {
            id: "test-task-id".to_string(),
            goal_id: "test-goal-id".to_string(),
            parent_task_id: None,
            title: "Test task".to_string(),
            kind: TaskKind::Edit,
            status: TaskStatus::Pending,
            assigned_worker: None,
            attempt: 1,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: TaskInputs::default(),
            outputs: TaskOutputs::default(),
        }
    }

    /// Build a minimal `WorkerResult` that can be used as a fake return value.
    pub fn fake_worker_result(status: WorkerStatus) -> WorkerResult {
        WorkerResult {
            status,
            command: None,
            exit_code: None,
            summary: "fake worker result".to_string(),
            packet_path: PathBuf::from("/tmp/test-packet.json"),
            prompt_path: PathBuf::from("/tmp/test-prompt.md"),
            stdout_path: None,
            stderr_path: None,
            last_message_path: None,
            result_path: PathBuf::from("/tmp/test-result.json"),
            outcome_path: PathBuf::from("/tmp/test-outcome.json"),
        }
    }

    /// Build a minimal `WorkerOutcome` that can be used as a fake return value.
    pub fn fake_worker_outcome(status: WorkerStatus) -> WorkerOutcome {
        WorkerOutcome {
            status,
            session_id: None,
            session_capability: None,
            summary: "fake worker outcome".to_string(),
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            known_failures: Vec::new(),
            raw_output_path: None,
            command: None,
            exit_code: None,
        }
    }
}
