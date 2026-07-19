# Gearbox Upstream Sync Notes

Shared-source files modified for the Gearbox fork.  When syncing with upstream:
- Keep upstream behavior unchanged when `GEARBOX_GUI` is not set.
- Gearbox text → `GEARBOX_GUI=1`.  Large resources → `crates/gearbox_settings`.
- **Never** rename upstream internal identifiers (Rust types, action/enum/context names, protocols, test fixtures).  Only rename with clear functional impact and upstream merge cost documented.
- User-visible Gearbox UI/text → Gearbox brand + Chinese.

---

## Translation Architecture

Two new entrypoints in `crates/ui/src/gearbox_text.rs`, exported via `crates/ui/src/ui.rs`:

| Export | Tiers (tried in order) | Used by |
|--------|----------------------|---------|
| `gearbox_translate_text()` | exact → multiline → visible-sentence → title-token → brand-replace | Labels, buttons, tooltips, menus, modals, list items, chips, tree items, loading labels, copy buttons, update buttons, headlines, agent-setup, API cards, thread items, AI-setting items |
| `gearbox_translate_setting_description()` | exact → multiline → sentence-fragment → brand-replace | Settings descriptions |

`translate()` guards on `GEARBOX_GUI` internally.  Each tier:
- **exact**: ~350 static exact-match pairs (English → Chinese)
- **multiline**: line-by-line exact-match for multi-line runtime text
- **visible-sentence**: full sentences ending in `.`/`!`/`?` → `settings_sentence_translation`
- **title-token**: tokenizes CamelCase/space-separated labels, translates via `title_token_translation` table, then fallback `sentence_token_translation`
- **sentence-fragment** (setting descriptions only): strips known prefixes, then token-by-token + phrase replacement + punctuation localization
- **brand-replace**: standalone `Zed` → `Gearbox` (boundary-aware, preserves `ZedGraph` etc.)

`HighlightedLabel` intentionally excluded (byte-offset dependency on original text).

---

## Merge Guide

- `[NEW]` = entire function/struct/file added by Gearbox → **keep during merge**
- `[MOD]` = upstream function modified → **careful diff needed**; keep Gearbox additions, merge upstream logic
- `[NO-OP]` = cosmetic/text-only inside `GEARBOX_GUI=1` guard → **accept Gearbox side**, text is intentional
- Files NOT listed here have no Gearbox changes and can be accepted from upstream.

---

## Modified Upstream Files

### Build & workspace

| File | Change |
|------|--------|
| `Cargo.toml` | `+workspace deps: gearbox, gearbox_settings, gearbox_agent` |
| `Cargo.lock` | Auto-updated |
| `[NEW]` `.github/workflows/gearbox_release.yml` | New workflow. Builds `--package gearbox` on GH runners (Linux/macOS/Windows). Produces `.deb`, `.dmg`, `.exe`. Publishes to GH Release (no Sentry/Slack/notarization/signing). Keep separate from upstream. |

### Settings infrastructure

| File | Change |
|------|--------|
| `[MOD]` `crates/settings/src/settings.rs` | `+set_settings_asset_loader`, `+settings_asset_str`. Upstream default still loads `SettingsAssets`. Gearbox calls `gearbox_settings::load` before `settings::init`. |
| `[MOD]` `crates/settings/src/keymap_file.rs` | Keymap loading → `settings_asset_str` (keeps default Zed keymaps unchanged) |

### UI components — routed through `gearbox_translate_text`

All user-visible text in these components goes through the shared translation layer:

| File | Text type | Marker |
|------|-----------|--------|
| `crates/ui/src/components/label/label.rs` | Label | `[MOD]` |
| `crates/ui/src/components/label/loading_label.rs` | Loading label | `[MOD]` |
| `crates/ui/src/components/button/button.rs` | Button | `[MOD]` |
| `crates/ui/src/components/button/button_link.rs` | Button link | `[MOD]` |
| `crates/ui/src/components/button/toggle_button.rs` | Toggle button | `[MOD]` |
| `crates/ui/src/components/button/copy_button.rs` | Copy button messages/tooltips | `[MOD]` |
| `crates/ui/src/components/button/icon_button.rs` | Icon button | `[MOD]` |
| `crates/ui/src/components/tooltip.rs` | Tooltip | `[MOD]` |
| `crates/ui/src/components/context_menu.rs` | Context menu | `[MOD]` |
| `crates/ui/src/components/modal.rs` | Modal | `[MOD]` |
| `crates/ui/src/components/chip.rs` | Chip | `[MOD]` |
| `crates/ui/src/components/tree_view_item.rs` | Tree item | `[MOD]` |
| `crates/ui/src/components/project_empty_state.rs` | Empty state | `[MOD]` |
| `crates/ui/src/components/collab/update_button.rs` | Update button | `[MOD]` |
| `crates/ui/src/components/ai/agent_setup_button.rs` | Agent setup | `[MOD]` |
| `crates/ui/src/components/ai/configured_api_card.rs` | API card | `[MOD]` |
| `crates/ui/src/components/ai/thread_item.rs` | Thread item | `[MOD]` |
| `crates/ui/src/components/ai/ai_setting_item.rs` | AI setting | `[MOD]` |
| `crates/ui/src/components/list/list_header.rs` | List header | `[MOD]` |
| `crates/ui/src/components/list/list_sub_header.rs` | List sub-header | `[MOD]` |
| `crates/ui/src/components/list/list_bullet_item.rs` | List bullet | `[MOD]` |
| `crates/ui/src/styles/typography.rs` | Headline | `[MOD]` |

### GUI crates — own `gearbox_label` / `gearbox_text` helpers

Each crate has a local helper that checks `GEARBOX_GUI` and returns Chinese or English.  These are `[MOD]` — upstream functions with Gearbox text added inside `GEARBOX_GUI=1` guards.  Merge tip: accept Gearbox side for the guarded text blocks; the upstream logic around them should be merged normally.

| File | What's localized | Notes |
|------|-----------------|-------|
| `crates/onboarding/src/onboarding.rs` | Title, subtitle, finish button | |
| `crates/onboarding/src/basics_page.rs` | Setup labels, descriptions | Theme/font IDs left as upstream values |
| `crates/onboarding/src/base_keymap_picker.rs` | Placeholder | |
| `crates/workspace/src/welcome.rs` | Welcome page sections, buttons, subtitle, agent card, recent header, tab title | |
| `crates/workspace/src/notifications.rs` | Notification titles, secondary content, primary action labels | Catches text not entering via `Label::new` / `Button::new` |
| `crates/workspace/src/pane_group.rs` | Dynamic collab location/share labels with usernames | Cannot exact-match |
| `crates/workspace/src/security_modal.rs` | Restricted-mode text and buttons | |
| `crates/project_panel/src/project_panel.rs` | Context menu (36 labels), discard-changes prompt, restore/cancel buttons | File-manager labels from `ui::utils` intentionally not changed |
| `crates/recent_projects/src/recent_projects.rs` | Picker placeholder, section headers, no-match text, footer/action labels | |
| `crates/recent_projects/src/sidebar_recent_projects.rs` | Picker placeholder, no-match, tooltip, error prompt | |
| `crates/recent_projects/src/wsl_picker.rs` | Distro picker placeholder | |
| `crates/recent_projects/src/remote_connections.rs` | Connection failure prompt titles, retry/cancel buttons | |
| `crates/recent_projects/src/remote_servers.rs` | Remote-server and Dev Container action labels | |
| `crates/command_palette/src/command_palette.rs` | Placeholder, run/add-keybinding buttons | Command names not localized (derived from action metadata; needs separate action-name translation layer) |
| `crates/file_finder/src/file_finder.rs` | Placeholder, filter tooltip, create-file prompt, split/open labels | |
| `crates/open_path_prompt/src/open_path_prompt.rs` | Create/replace confirmation body, buttons, empty-state text | Prompt title keeps target path, mostly upstream-formatted |
| `crates/collab_ui/src/collab_panel.rs` | CLA error path; participant labels, tooltips, context menu entries | Removes `zed.dev/cla` branding from Gearbox path |
| `crates/collab_ui/src/notifications/incoming_call_notification.rs` | Call notification text with username | Dynamic format |
| `crates/debugger_ui/src/session/running.rs` | Debugger scenario error | |
| `crates/debugger_ui/src/new_process_modal.rs` | Command placeholder (`ENV=Gearbox ~/bin/program`) | |
| `crates/debugger_ui/src/debugger_panel.rs` | Empty-state labels | |
| `crates/extensions_ui/src/extensions_ui.rs` | Version compatibility tooltips, doc/install labels | |
| `crates/extensions_ui/src/extension_version_selector.rs` | Compatibility labels | |
| `crates/oauth_callback_server/src/oauth_callback_server.rs` | OAuth success/failure browser pages | Zed wording kept when not `GEARBOX_GUI` |

### LSP store (memory leak fix)

| File | Change |
|------|--------|
| `crates/project/src/lsp_store.rs` | Fix memory leak: `lsp_requests` and `chunk_lsp_requests` in `BufferLspData` were never cleaned up when a language server stops/restarts, causing unbounded growth of in-flight LSP task references across all buffers. Added `retain` to filter out entries for the stopped server in `remove_server_data()`.

### Settings UI

All files are `[MOD]` — Gearbox text layers added inside `GEARBOX_GUI=1` guards; upstream data model and JSON paths unchanged.

| File | What | Notes |
|------|------|-------|
| `crates/settings_ui/src/settings_ui.rs` | Page names, section headers, item titles, descriptions, subpage links, action links, navigation entries, breadcrumbs, window title, search placeholder, settings-file buttons, user/project/server scope labels, workspace-restoration text, telemetry labels, scoped settings | Fallback → `gearbox_translate_text` / `gearbox_translate_setting_description`. `Zed`→`Gearbox` in descriptions. |
| `crates/settings_ui/src/components/dropdown.rs` | Enum labels: `Last Session`→`上次会话`, etc. | Enum values/settings unchanged |
| `crates/settings_ui/src/components/font_picker.rs` | Placeholder | |
| `crates/settings_ui/src/components/theme_picker.rs` | Placeholder; `Zed` theme names→`Gearbox` display | Internal theme IDs unchanged |
| `crates/settings_ui/src/components/icon_theme_picker.rs` | Placeholder; `Zed` icon theme names→`Gearbox` display | Internal IDs unchanged |
| `crates/settings_ui/src/components/ollama_model_picker.rs` | Placeholder | |
| `crates/settings_ui/src/pages/edit_prediction_provider_setup.rs` | Restart instruction→Gearbox | |
| `crates/settings_ui/src/pages/llm_providers_page.rs` | Restart instruction→Gearbox | |
| `crates/settings_ui/src/pages/tool_permissions_setup.rs` | Native-agent disclaimer→Gearbox | |
| `crates/settings_ui/src/pages/sandbox_settings.rs` | Sandbox explanation→Gearbox | |
| `crates/settings_ui/src/pages/skill_creator.rs` | Private-file retry→Gearbox | |

### Language model & OAuth providers

All are `[MOD]` — visible text replace `Zed`→`Gearbox` only. Internal type/plan/enum identifiers unchanged.

| File(s) | What |
|---------|------|
| `crates/language_models/src/provider.rs` | Visible provider/help/error wording |
| `crates/language_models/src/provider/{api_compatible,bedrock,cloud,llama_cpp,lmstudio,mistral,ollama,openai_subscribed,opencode,vercel_ai_gateway}.rs` | Same pattern across all 10 providers |
| `crates/context_server/src/context_server.rs` | OAuth/client metadata→Gearbox; endpoint constants kept |
| `crates/context_server/src/oauth.rs` | Same |

---

## `[NEW]` Gear Native Agent (`crates/gearbox_agent/`)

New runtime crate.  Functions as the orchestration engine for the `Gear` agent.

### Key modules

| Module | Purpose |
|--------|---------|
| `runtime.rs` | `Orchestrator::run()` — goal-pursuit loop: spec→plan→worker session→verify→provider review→repair/replan. Sync, runs on `background_spawn`. `DEFAULT_MAX_ITERATIONS=5` and `DEFAULT_MAX_RUNTIME_MINUTES=60`, aligned with the Gear runtime MVP budget in `docs/gearbox-gear-agent-plan.md`, and the runtime now also wires explicit `max_child_depth=1` and `max_provider_unknown_streak=2` budgets for child-loop nesting and inconclusive provider-review streaks. Context-risk signals are now collected from worker stdout/stderr/last_message/result/outcome artifacts and per-attempt history as well as summary strings, and repeated worker output summaries now count as a no-progress signal, so token/context guard is no longer limited to heuristic review text alone. The final completion branch now also checks `context_guard_reason()` so token-limit/context-compaction/session-reliability warnings prevent a premature completion claim. `append_completion_notification()` now also records `notification_failed_epoch` on delivery failure via `record_completion_notification_failed_epoch()`, so a failed popup/ledger write leaves a durable retry marker. Accepts `coordinator_model`/`coordinator_brief`. `CoordinatorReviewInput` includes task id, worker kind/model/category/route reason, attempt index/count, failure kind, retry reason, fallback history, worker outcome, commands, failures, outcome path, iteration budget summary, and `no_progress_signals`. `CoordinatorReview` records optional `route_hint` and `stop_reason`; `STOP_REASON` can conservatively stop as needs_user/blocked/limited but cannot override failed verification as complete. Runtime events and goal review artifacts include worker model when configured, and goal review artifacts now surface a dedicated no-progress section so stagnation evidence stays visible in the recorded loop history. `evaluate_goal()` consumes TaskManager terminal metadata: `NoFallbackRoute` / `RepeatedFailureLimit` / `PremiumBudgetExceeded` -> limited, required worker unavailable/start failure -> needs_user. When provider review returns `ROUTE_HINT=review` without affirming completion, GoalLoop now schedules an independent review iteration instead of auto-completing after verification passes; the next worker prompt switches from repair wording to independent-review wording. Verification-passed `GOAL_SATISFIED=unknown` no longer auto-completes: the first inconclusive review continues the loop, and repeated inconclusive reviews escalate to `review` or `needs_user`. Repeated same `failure_kind` also escalates route selection (`repair/explore -> deep -> review`) before the loop gives up. Repair/review task creation now stamps `parent_task_id` so TaskManager can walk descendant trees for cancel/interrupt. Final report generation also now depends on the runtime-collected task evidence chain, not only summary strings. |
| `workers.rs` | `WorkerRegistry.start()`→`WorkerSessionAdapter`→command-backed adapters (`OpencodeCommandWorker`, `OpencodeSessionWorker`, `CodexCommandWorker`, `ClaudeCommandWorker`, `ZedAgentCommandWorker`, `CustomCommandWorker`). `WorkerKind`: opencode/opencode_session/codex/claude/zed_agent/custom. `WorkerCategory`: quick/repair/deep/review/explore/librarian/visual/zed-native/custom. `WorkerPacket` JSON contract remains, and now carries a category-scoped `WorkerToolPolicy` plus combined builtin/user `prompt_append` text (user override via `GEARBOX_GEAR_WORKER_PROMPT_APPEND`); `worker_prompt()` now also renders sanitized model metadata blocks before write. `WorkerOutcome` is persisted to `.gearbox-agent/workers/<task_id>/outcome.json`. `CommandWorker` remains only as a compatibility wrapper. Command-backed handles support cancellation tokens, explicit unsupported follow-up/steer errors, and `last_output()` cache from stdout/stderr/summary. `OpencodeSessionWorker` is the Phase 4 resident-command MVP: it reuses the opencode command path, supports follow-up/steer turns in the same managed handle, persists per-turn prompt/stdout/stderr/result/outcome artifacts, and now has resident-command interrupt/stale-session detection/revive. After cancel or interrupt, the resident handle marks itself stale; the next follow-up/steer resets the token, writes `interrupt-*.md` / `revive-*.md`, and continues. `WorkerRegistry` now also supports an injected `NativeWorkerBackend`; when present, `zed_agent` routes can bypass `ZedAgentCommandWorker` and start a repo-native backend instead, while CLI/non-GUI paths still fall back to the existing command worker. `worker_prompt()`, `worker_outcome_from_result()`, and `write_result_and_outcome()` are now public helpers so native backends can emit the same packet/prompt/result/outcome artifact contract as command workers. `WorkerKind::default_command()` now provides the first built-in command templates for non-opencode workers: Codex gets a noninteractive `codex exec` template with `--skip-git-repo-check`, `--dangerously-bypass-approvals-and-sandbox`, and `-o "$GEARBOX_WORKER_LAST_MESSAGE"`; Claude gets a basic prompt-driving default. `WorkerKind::provider_id_hint()` maps worker kinds such as Codex/Claude back to the GUI provider namespace so higher layers can derive provider-qualified availability entries. `WorkerKind::is_premium()` marks Codex/Claude/ZedAgent for premium budget policy. Command-backed workers also do a first-pass PATH binary availability preflight for direct commands; a missing binary is emitted as a `no worker command` skipped outcome so TaskManager can classify it as `WorkerUnavailable` and fallback instead of only surfacing a shell failure. `WorkerOutcome` parsing now prefers `GEARBOX_WORKER_LAST_MESSAGE` and extracts `summary` / `changed_files` / `commands_run` / `known_failures` sections into structured outcome fields. `CategoryRouter` interprets provider `ROUTE_HINT` as category and records selected route reason. Fallback retries now emit `workers/<task_id>/route-transform-*.md` artifacts and the goal review summary includes fallback history. `WorkerStartRequest.route_attempt` keeps route-local attempt selection stable across queue/start/registry boundaries. `require_worker`/`skip_worker` flags. |
| `task_manager.rs` | First TaskManager control plane. Provides `start` / `tick` / `try_wait_for` / `wait_for` / `run_worker_task` / `cancel_task` / `list` / `snapshot`, records `TaskRecord`, persists `.gearbox-agent/workers/<task_id>/task-record.json`, stores the current running worker handle, and forwards `cancel_task()` to that handle. `TaskRecord` now includes `attempts[]` with worker kind/command/model/category, route hint/reason, status, session/result/outcome/error, `failure_kind`, and `retry_reason` metadata, plus `parent_task_id` lineage for descendant cancel/interrupt. `CompletionNotifier` now also accepts a failure recorder, and failed delivery paths can persist `notification_failed_epoch` instead of silently dropping the failed attempt. `TaskManager::snapshot()` returns GUI-facing structured counts, task/attempt summaries, summary head / continuation hint, result/outcome artifact paths, and current worker output. `TaskManagerControl` exposes the current running worker handle to GUI/control paths so `current_last_output()`, `send_follow_up_current_task()`, `steer_current_task()`, `interrupt_current_task()`, and cancellation can land while `wait_for()` is active. Includes MVP pending queue, `ConcurrencyManager` global slot plus per-key slot, pending cancel, completed archive, per-task `.gearbox-agent/workers/<task_id>/task-events.jsonl`, fallback retry to the next different route on failed/unavailable worker results, unavailable worker models, and start failures, explicit `NoFallbackRoute` terminal metadata, and `RepeatedFailureLimit` after `MAX_SAME_FAILURE_RETRIES=2`. Runtime now uses explicit start/cancel-check/try-wait. `start_queued_task()` dispatches a background worker completion thread; nonblocking `tick()` / `try_wait_for()` and blocking `wait_for()` consume finished-task messages, and `settle_running_task()` handles terminal settling/fallback/archive/queue pumping. `wait_for()` no longer blocks forever on the finished-task channel: it polls with a short timeout, re-enters `tick()`, and `tick()` now sweeps stale running tasks. Running tasks older than the configured `WorkerConfig.stale_task_timeout_secs` are cancelled best-effort, settled as failed timeout errors, and still reuse the existing fallback path before surfacing an error/limited state. `tick()` also now removes first-pass orphaned in-memory state (running/queued entries whose task record disappeared). On runtime startup, `recover_orphaned_records()` also scans existing worker `task-record.json` files and converts leftover disk `pending/running` records from a previous crashed runtime into failed records plus lifecycle events, so restart does not inherit fake live state. `TaskManager` also keeps a runtime `task_record_paths` index so recovered orphan records can resolve their workspace root again and persist `dispose` / reconcile lifecycle events even when no live running/completed run handle exists. Unavailable worker models are checked before launching a worker and recorded as `ModelUnavailable`; comparison now accepts both legacy plain model ids and provider-qualified entries like `openai/gpt-5`. `WorkerConfig` now also carries `premium_worker_budget` and `stale_task_timeout_secs`; premium route attempts beyond that budget are recorded as `PremiumBudgetExceeded`. Failed category-hinted opencode/opencode_session attempts can append a first-pass automatic Codex upgrade route, and route-local selection is preserved via `QueuedTask.route_attempt`. If no fallback exists, a synthetic skipped result/outcome lets GoalLoop produce `limited` instead of crashing the run. `TaskManagerTickLoop` is a reusable background tick loop primitive around `SharedTaskManager` and is now attached to Gear runtime / GUI session lifetime. Concurrency keys are selected worker kind plus coordinator provider/model, and explicit write-scope overlap checks on queued/running tasks serialize overlapping write scopes while disjoint tasks can still run when key budget allows. Global/per-key limits now come from `WorkerConfig.max_parallel_workers` / `max_parallel_per_key`, with current CLI/env policy surface `--max-parallel-workers`, `--max-parallel-per-key`, `GEARBOX_GEAR_MAX_PARALLEL_WORKERS`, and `GEARBOX_GEAR_MAX_PARALLEL_PER_KEY`. Gear message stream now includes a deduplicated markdown queue/control-plane snapshot with task/attempt result/outcome artifact links; descendant cancel/interrupt now walks `parent_task_id` trees so Gear session cancel does not leave child tasks behind, and task snapshots now expose packet/prompt/transcript/result/outcome artifact links from the worker directory for the GUI panel. `destroy_resident_task()` now uses the actual running handle first and best-effort runs `interrupt()` / `cancel()` / `abort()` / `dispose()` before clearing control and records, so shutdown/LRU/TTL cleanup does not depend on a stale current-task snapshot. |
| `state.rs` | `Goal`, `Session`, `Task`, `Event`, `CoordinatorModel` data models. `Task.parent_task_id` now carries task-tree lineage for Gear descendant cancel. `StateStore` — JSON files under `.gearbox-agent/`. |
| `tools.rs` | `git_snapshot`, `check_scope`, `run_shell_command_with_env_and_cancellation`, `CancellationToken` (`Arc<AtomicBool>`) |
| `languages.rs` | `LanguageDetection` — TypeScript/Python/Rust detection. `detect_with_request()` falls back to request text for empty workspaces (web/app prompts→TypeScript scaffold). |
| `product.rs` | Markdown artifacts: spec, plan, verification, final-report. Includes `coordinator_model`/`coordinator_brief` summaries. `final_report()` now emits an explicit `Evidence Chain` section listing worker packet/prompt/result/outcome plus task evidence paths for spec/plan/verification/review artifacts, so completion claims are backed by concrete artifact references. Web App stack guidance. |
| `cli.rs` | `gear` binary (name may conflict with system `gear`). `gear run <prompt>` with worker/scope/verify args, `--worker-sequence`, and per-kind command args (`--opencode-command`, `--codex-command`, `--claude-command`, `--zed-agent-command`, `--custom-command`). Primary worker selection now also resolves the matching per-kind command (for example `--worker codex --codex-command ...`), instead of only sequence routes inheriting those commands. If no explicit command is given, CLI now falls back to `WorkerKind::default_command()` for supported worker kinds such as Codex. `--premium-worker-budget` now exposes the first premium worker budget policy. `--stale-task-timeout-secs` now exposes the first resilience timeout policy. |
| `Cargo.toml` | Deps: `smol`, `chrono`, `clap`, `serde`, `serde_json`, `anyhow`. Binary: `gear`. |

### Recent additions
- `CoordinatorModel` (provider_id/model_id/name) persisted in goals and worker packets
- `coordinator_brief` (optional LLM planning context, generated before run)
- Empty-workspace prompts→TypeScript Web App default stack + npm verify commands
- `TaskInputs` (spec/plan packet paths) in worker packets
- `WorkerSessionAdapter` / `WorkerSessionHandle` / `WorkerStartRequest` / `WorkerOutcome`
- Command-backed session adapters for opencode/opencode_session/codex/claude/zed_agent/custom
- Command-backed worker handles now cache `last_output()` from stdout/stderr/summary while non-session command workers still clearly reject unsupported follow-up/steer calls
- `OpencodeSessionWorker` added as a resident-command MVP. It parses `opencode_session` / `opencode-session` / `opencode-resident`, uses `--opencode-command`, exposes a session id, and supports `send_follow_up` / `steer` by writing per-turn prompt artifacts and re-running the configured opencode command inside the same managed handle. Resident-command interrupt/stale session detection/revive is now implemented around the cancellation token and `interrupt-*.md` / `revive-*.md` artifacts; true opencode-native interrupt/session API is still follow-up work.
- `WorkerSessionHandle` now includes `interrupt()`. `TaskManagerControl` and `TaskManager` expose `interrupt_current_task()` / `interrupt_task()` as the first Gear control-plane interrupt path; Gear `thread_view` now routes `Stop Generation` through `interrupt_current_task()` before falling back to generic thread cancel.
- `TaskManagerControl` now also exposes `send_follow_up_current_task()` / `steer_current_task()` plus task-id variants, so GUI-native control surfaces can reuse the same resident worker handle instead of inventing a second control path.
- `TaskManagerControl.current_last_output()` added so GUI/control paths can read the current worker handle output while a managed task is active
- Runtime consumes worker sessions through `WorkerRegistry.start()` and records worker outcome paths in events/review artifacts
- Gear cancel / interrupt now goes through `TaskManager` so descendant `parent_task_id` trees can be cancelled from the GUI/session entrypoint.
- Provider-backed review prompt now receives task/worker kind/category/route reason/outcome/budget context, parses optional `ROUTE_HINT` / `STOP_REASON`, feeds `ROUTE_HINT` into category-based next worker route selection, and uses safe `STOP_REASON` values in goal evaluation
- Provider-backed review prompt now also receives attempt/failure/fallback context, and explicitly documents `ROUTE_HINT=review` as the way to request an independent review worker before completion
- Runtime executes worker sessions through `TaskManager.start()` + cancellation check + `TaskManager.wait_for()` and records `task-record.json`; TaskManager internally separates start/wait and can cancel a saved running handle
- GUI `Gear` sessions store `TaskManagerControl`; `cancel()` cancels the current Gear worker handle and then sets the Gear cancellation token
- `crates/agent_ui/src/conversation_view/thread_view.rs` now has a Gear-only native control branch: `interrupt_and_send()` first tries `send_follow_up_current_task()`, running queue `Send Now` reuses `send_follow_up_current_task()` / `steer_current_task()` according to the queue steer flag, and `cancel_generation()` first tries `interrupt_current_task()`. Non-Gear native/external agents keep the upstream cancel-and-send behavior.
- TaskManager now records pending/running/terminal task lifecycle events to `task-events.jsonl`, queues tasks when the global concurrency slot is occupied, removes pending tasks on cancel, and starts the next queued task after a running task settles
- `TaskManager` now has an MVP background worker completion dispatcher: running worker outcome/result waiting happens off the `wait_for()` call stack; finished-task messages can be settled by nonblocking `tick()` or blocking `wait_for()` through `settle_running_task()`
- `TaskManagerTickLoop` and `SharedTaskManager` added as the Phase 1 scheduler primitive. Gear GUI sessions now store the shared manager and tick loop, pass the manager into `RunOptions`, and clear both after the matching Gear run completes.
- Gear event streaming now renders a deduplicated TaskManager markdown snapshot from `SharedTaskManager`: pending/running/completed/failed/cancelled/skipped counts, task/attempt summaries, task/attempt result/outcome links, and current worker output. The markdown renderer consumes `TaskManager::snapshot()` instead of deriving GUI state directly from `TaskRecord`.
- `crates/agent_ui/src/conversation_view/thread_view.rs` now also consumes `TaskManager::snapshot()` directly for a dedicated Gear task panel, not an activity-bar section: task counts, recent task/attempt summaries, summary head / continuation hint / messageability, current worker output, direct open buttons for task/attempt `result` / `outcome` / `fallback` artifacts, and goal-level `Goal Review` / `Coordinator Review` / `Final Report` / `Artifacts` entries driven by the new snapshot `artifacts_root`. The panel now renders both messageability and task/attempt status labels from typed shared enums instead of inferring labels from `Debug` output.
- That Gear-only panel now also exposes `Follow Up`, `Steer`, `View Output`, `Interrupt`, and `Cancel Task` controls backed by the current Gear draft control path plus `open_markdown_in_workspace()`, `interrupt_gear_task()`, and `cancel_gear_task()`.
- The same Gear-only panel now adds first-pass behavior: `All` / `Active` / `Attention` filters, running/pending-first sorting, and `Show More` / `Show Less` when more than six tasks match.
- `ConcurrencyManager` now supports a per-key slot in addition to the global slot. The MVP key is selected worker kind plus coordinator provider/model; queue pumping can start a later task with a different key while an earlier same-key task remains queued.
- CLI can build route sequences for opencode/opencode_session/codex/claude/zed_agent/custom command-backed workers
- `WorkerCategory` / `CategoryRouter` added for quick/repair/deep/review/explore/librarian/visual/zed-native/custom routing. Runtime event metadata and goal review artifacts include `worker_category` and `route_reason`.
- `WorkerConfig.default_worker_for_small_tasks` (default `WorkerKind::ZedAgent`) added. `CategoryRouter::resolve()` and `sequence_route()` now prefer the configured small-task worker for `Category::Quick` tasks, redirecting to ZedAgent before falling back to the existing preferred kinds. Non-Quick routing unchanged.
- `TaskRecord.attempts[]` added as the Phase 3 attempt-history base. It records worker kind/command/category, route hint/reason, status transitions, session/result/outcome paths, failure kind, retry reason, and errors for each managed worker attempt. Failed or unavailable worker results can now append a fallback attempt and continue the same managed task without overwriting the failed attempt outcome; no-fallback terminal records use `NoFallbackRoute`, and repeated same failure records use `RepeatedFailureLimit`.
- Worker routes now support optional `worker_model` metadata. CLI accepts sequence entries such as `codex:gpt-5.4`; Gear GUI env accepts `GEARBOX_GEAR_WORKER_MODEL`, `GEARBOX_GEAR_WORKER_SEQUENCE` entries with `worker:model`, and `GEARBOX_GEAR_UNAVAILABLE_WORKER_MODELS`. `TaskFailureKind::ModelUnavailable` records pre-start model availability failures and can fallback to the next route, and `CategoryRouter` now skips unavailable provider/model routes before it has to emit that fallback when a later configured route is healthy.
- GoalLoop now maps worker fallback terminal metadata to final status: no fallback/repeated failure becomes `limited`; required worker unavailable/start failure becomes `needs_user`. Goal review artifacts include worker failure kind and retry reason.
- Completion notification phase-05 follow-up now buffers worker-complete events during the Gear turn and flushes them at turn end instead of injecting them mid-stream. `runtime.rs` now owns the turn-end flush guard, `CompletionNotifier` flushes buffered notifications in `(run_epoch, task_id)` order, and the emitted event data includes `result_path` / `outcome_path` / `task_record_path` so the GUI can render artifact links.
- Completion notification delivery failure now records `TaskRecord.notification_failed_epoch`, giving Gear a durable marker when popup/ledger delivery fails instead of silently dropping the attempt.
- `EventKind::CompletionNotified` was added for the Gear completion ledger, and `gear_event_status_markdown()` now recognizes `result_path` / `outcome_path` / `task_record_path` when building the visible status line.
- `runtime.rs` also restored the budget / stagnation helpers used by the GoalLoop tests: `BudgetController`, `context_safe()`, `detect_stagnation()`, and the coordinator-review parser helper; `BudgetController` now includes explicit `max_provider_unknown_streak` in addition to `max_child_depth`, so the first provider-aware review threshold is part of the persisted budget story instead of being a hidden constant.
- `ConversationView::notify_with_sound()` now buffers agent completion popups/sounds when the root thread is still generating, compacting, waiting for confirmation, has in-progress tool calls, or has queued messages, and flushes them once the root thread returns to idle, so Gear completion wakeups do not compete with active parent-thread work.
- `destroy_resident_task()` now prefers the actual running handle and best-effort runs `interrupt()` / `cancel()` / `abort()` / `dispose()` before cleanup, so lifecycle teardown no longer depends on `current_task_snapshot()` being intact.
- Phase 06 session shutdown cleanup now runs from `TaskManager` drop: resident tasks are best-effort destroyed on shutdown, and the coverage test `task_manager_drop_shuts_down_resident_tasks` locks the behavior in.
- `ConversationView::flush_pending_notifications()` now rechecks the root thread busy state before draining buffered completion popups, so a `StatusChanged` wake does not surface the popup while the parent thread is still busy.
- `TaskManager::destroy_resident_task()` now persists a `dispose` lifecycle event whenever a resident task store is available, and shutdown keeps the last task id visible while downgrading the dangling current control state to `Lost` instead of advertising a dead running handle.

---

## Agent Integration Changes

### 2026-07-12 Objective controller bridge

`crates/agent/src/agent.rs` is modified by Gearbox to opt into the Gear-owned
`ObjectiveGraph` rolling controller only when `GEARBOX_GEAR_OBJECTIVE` is
explicitly enabled. The normal upstream/single-goal path remains unchanged;
the bridge passes the existing `PhaseRuntime`/worker broker into
`Orchestrator::run_objective_with_phase_runtime`, preserves Gear cancellation
and task-manager ownership, and maps the durable objective terminal state back
to the existing response contract. Objective mode also preserves a user
continuation stop instead of clearing it automatically. Upstream merges must
keep this branch conditional and must not make ObjectiveGraph a dependency of
the non-Gear agent path.

### `[NEW]` `crates/agent/Cargo.toml`
- `+dep: gearbox_agent`

### `[MOD]` `crates/agent/src/agent.rs`

| Marker | Function / Change | What |
|--------|------------------|------|
| `[NEW]` | `GEAR_AGENT_ID` | Static `LazyLock<AgentId>` for `"Gear"` |
| `[MOD]` | `struct Session` | `+gear_cancellation_token: Option<CancellationToken>`, `+gear_task_manager_control: Option<TaskManagerControl>`, `+gear_task_manager: Option<SharedTaskManager>`, `+gear_task_manager_tick_loop: Option<TaskManagerTickLoop>`, `+work_dirs: Option<PathList>` |
| `[NEW]` | `NativeAgentConnection::gear()` | Constructor for Gear native connection |
| `[NEW]` | `send_gear_prompt()` | Routes prompts → `Orchestrator::run()` on `cx.background_spawn` |
| `[NEW]` | `gear_coordinator_from_thread()` | Reads thread's model → `CoordinatorModel` metadata |
| `[NEW]` | `generate_gear_coordinator_brief()` | Async LLM call for planning brief; skips `"fake"` provider |
| `[NEW]` | `generate_gear_coordinator_review()` | Async LLM review call after each Gear iteration. Prompt includes task id, worker kind/category, route reason, sanitized model metadata, worker outcome, commands, failures, outcome path, verification, scope, and diff. Malformed coordinator review responses now write a parser warning artifact alongside the raw response. |
| `[NEW]` | `gear_provider_review_enabled()` | Disables provider-backed review for missing model and test fake provider, preventing test-only fake stream hangs. |
| `[NEW]` | `is_gear_executable_goal()` | Filters greetings; trims ASCII+CJK punctuation; checks action words + length |
| `[NEW]` | `gear_workspace_for_session()` | Resolves workspace: `work_dirs` → `visible_worktree` fallback |
| `[NEW]` | `push_gear_assistant_markdown()` | Pushes markdown block into ACP thread + internal Thread |
| `[NEW]` | `gear_request_from_prompt()` | Extracts text content from ACP prompt blocks |
| `[NEW]` | `gear_worker_config_from_env()` | Reads `GEARBOX_GEAR_WORKER`, `GEARBOX_GEAR_WORKER_COMMAND`, `GEARBOX_GEAR_WORKER_MODEL`, `GEARBOX_GEAR_WORKER_SEQUENCE`, `GEARBOX_GEAR_UNAVAILABLE_WORKER_MODELS`, `GEARBOX_GEAR_PREMIUM_WORKER_BUDGET`, `GEARBOX_GEAR_STALE_TASK_TIMEOUT_SECS`, fallback `GEARBOX_OPENCODE_COMMAND`. Warns on invalid kinds. `require_worker=true` when command set. Now also projects `LanguageModelRegistry::read_global(cx).available_models(cx)` into provider-qualified unavailable worker model entries before launching Gear. |
| `[NEW]` | `gear_verification_commands_from_env()` | Reads `GEARBOX_GEAR_VERIFY_COMMANDS` |
| `[NEW]` | `gear_event_status_markdown()` | Event → markdown status line |
| `[NEW]` | `gear_task_manager_snapshot_markdown()` / `gear_task_manager_snapshot_to_markdown()` | Shared TaskManager structured snapshot → markdown queue/control-plane snapshot |
| `[NEW]` | `NativeAgentConnection::gear_task_manager_snapshot()` | Gear-only snapshot bridge for GUI-native observer surfaces |
| `[NEW]` | `NativeAgentConnection::cancel_gear_task()` | Gear-only bridge from GUI-native observer controls to `TaskManager::cancel_task()` so descendant task trees are cancelled too |
| `[NEW]` | `NativeAgentConnection::{interrupt_gear_task,send_follow_up_gear_task,steer_gear_task}` | Gear-only bridge from GUI/native session controls to `TaskManagerControl` |
| `[NEW]` | `GearZedWorkerBackend` / `GearZedWorkerDispatch` / `GearZedWorkerSessionHandle` / `spawn_gear_zed_worker_dispatcher()` | Gear session-scoped native Zed worker bridge. `send_gear_prompt()` now creates a dispatcher channel on the app thread, injects it into `TaskManager` via `WorkerRegistry::with_native_backend(...)`, and lets `zed_agent` routes create real Zed subagent sessions instead of shelling out. The native worker writes the same `packet.json` / `prompt.md` / `result.json` / `outcome.json` artifact contract as command workers and uses `ZED_AGENT_ID` subagents, preventing recursive Gear-in-Gear execution. It now also keeps a per-worker interaction queue so `send_follow_up` can reuse the same native subagent session for the next turn, while `steer` additionally flips `Thread::set_end_turn_at_next_boundary(true)` to cut the current turn short before continuing. The dispatcher helper is intentionally shared by production and GPUI tests so regression coverage exercises the same app-thread routing path. Current remaining gap is streaming/incremental output, not basic follow-up/steer reuse. |
| `[NEW]` | `gear_response_markdown()` | Final report → markdown summary |
| `[NEW]` | `clear_gear_cancellation_token()` | Clears session token if same reference |
| `[MOD]` | `cancel()` | +Gear TaskManager cancel for the current worker tree, +Gear token cancel; still calls `thread.cancel()` for native thread cleanup |
| `[NEW]` | `test_gear_prompt_runs_gearbox_orchestrator` | GPUI integration test |
| `[NEW]` | `test_gear_prompt_greeting_does_not_start_orchestrator` | GPUI test for small-talk filtering |
| `[NEW]` | `test_native_zed_worker_reuses_session_for_follow_up_and_steer` | GPUI regression test for native Zed worker follow-up/steer: verifies the production dispatcher + backend can keep the same native subagent session running across follow-up and steer turns and persist the later-turn worker artifacts |
| `[NEW]` | `create_gearbox_agent_session()` | Public function that creates a one-shot Zed Agent sub-session for quick/low-risk "小修" tasks. Spawns a subagent thread under a parent Gear session, runs the prompt, and returns the assistant response text. Used by Gearbox orchestrator for Category::Quick routing. |

**Imports added:** `gearbox_agent::runtime::*`, `gearbox_agent::tools::CancellationToken`, `gearbox_agent::workers::*`, `gearbox_agent::state::CoordinatorModel`, `language_model::{CompletionIntent, LanguageModelRequest, LanguageModelRequestMessage, Role}`

### `[NEW]` `crates/agent/src/native_agent_server.rs`
- `NativeAgentServer::gear()`: `agent_id: GEAR_AGENT_ID`, `telemetry_id: "gear"`, `logo: Sparkle`

### `[MOD]` `crates/agent/src/tests/mod.rs`
- Native agent tests updated for explicit identity metadata struct fields (was tuple access)

### `[MOD]` `crates/agent_ui/src/agent_ui.rs`
- `+Agent::GearAgent` variant, serde alias `"GearAgent"`
- `Agent::label()`: `"Agent"` under GEARBOX_GUI (from `"Zed Agent"`), `"Gear"` for GearAgent
- `Agent::server()`: returns `NativeAgentServer::gear()`
- `Agent::icon()`: `Sparkle` for GearAgent and Custom
- `Agent::is_native()`: includes GearAgent

### `[MOD]` `crates/agent_ui/src/agent_panel.rs`
- Gear in `list_agents_and_models`: only when `GEARBOX_GUI=1`; shares native model list
- Context menu entry: `"Gear"` with `Sparkle` icon, launches new thread
- Agent ID routing for sibling thread creation

### `[MOD]` `crates/agent_ui/src/agent_connection_store.rs`
- `Agent::GearAgent` entries always retained

### `[MOD]` `crates/agent_ui/src/conversation_view/thread_view.rs`, `agent_ui/src/mention_set.rs`
- Updated for native connection identity metadata (struct fields instead of tuple access)
- `thread_view.rs`: Gear-only send/control integration. `Stop Generation` now prefers `TaskManagerControl.interrupt_current_task()`. While Gear is running, `Send Immediately` reuses the current worker via `send_follow_up_current_task()`, and queue `Send Now` uses `steer_current_task()` when the queue entry is marked steer, otherwise `send_follow_up_current_task()`. Generic `Agent` / external sessions keep upstream cancel-and-send semantics.
- `conversation_view.rs`: the Gear task panel no longer renders from `ConversationView`; it now renders from `ThreadView` so the Gear-specific `Context<Self>` callbacks stay valid and the panel can live as an independent block below the activity bar.
- `thread_view.rs`: Gear-only native TaskManager observer panel in the main thread render path. It uses `NativeAgentConnection::gear_task_manager_snapshot()` and adds task/attempt summary rows plus task/attempt artifact open buttons without changing upstream `Agent` / external session rendering. Task rows now surface snapshot `summary_head` / `continuation_hint` so completion guidance is visible in the panel, not only in the event trail.
- `thread_view.rs`: that Gear-only panel also adds explicit `Interrupt`, `Cancel Task`, and full-output open actions for the current worker.
- `thread_view.rs`: that same panel now keeps its own Gear-only filter/show-more view state and reorders task rows so `running`/`pending` tasks surface first.

---

## Gearbox Branding & Packaging

### `[NEW]` Binary entry point (`crates/gearbox/`)

| File | Change |
|------|--------|
| `src/main.rs` | Sets `GEARBOX_GUI=1` at startup (unsafe, before multi-threading). Data dir→`~/.local/share/gearbox`. User-Agent→`Gearbox/{version}`. Error messages→"Gearbox". Env aliases: `GEARBOX_EXPERIMENTAL_A11Y`, `GEARBOX_STATELESS`, `GEARBOX_GENERATE_MINIDUMPS`, `GEARBOX_WINDOW_DECORATIONS`, `GEARBOX_ALLOW_EMULATED_GPU` (with `ZED_*` fallbacks). Build-time vars (`ZED_BUNDLE`, `ZED_BUILD_ID`, `ZED_COMMIT_SHA`) unchanged. |
| `src/zed.rs` | `DOCS_URL`, `STATUS_URL`, `MERCH_URL`→`github.com/ShamirSecret/gearbox-zed`. Internal action names (`OpenZedUrl`, `RegisterZedScheme`) unchanged. |
| `src/zed/app_menus.rs` | Full Chinese menu items: 视图→放大/缩小/重置缩放, 编辑器布局→拆分, 面板→项目/大纲/协作/终端/调试, etc. |
| `src/zed/open_listener.rs` | (no Gearbox-specific changes) |
| `src/zed/quick_action_bar/repl_menu.rs` | `ZED_REPL_DOCUMENTATION` const→gearbox repo URL |
| `build.rs` | Diagnostic prefix→`"gearbox build.rs:"` |

### `[NEW]` Packaging resources

| File | Change |
|------|--------|
| `crates/gearbox/resources/app-icon.icns` | macOS icon from Gearbox PNG |
| `crates/gearbox/resources/flatpak/manifest-template.json` | Command/path→`gearbox`; `ZED_BUNDLE_TYPE` kept |
| `crates/gearbox/resources/snap/snapcraft.yaml.in` | Entry/command→`gearbox`; `ZED_BUNDLE_TYPE` kept |
| `crates/gearbox_settings/assets/settings/*` | Settings with Gearbox comments/docs/menu strings. Internal IDs (`.ZedMono`, `Zed (Default)`, `ZedPredictModal`) kept; Gearbox display layer renames at render. |
| `crates/gearbox_settings/assets/keymaps/*` | Keymaps with Gearbox strings. Internal context IDs kept. |
| `[NEW]` `docs/gearbox-gear-agent-plan.md` | Gear runtime plan rewritten around the oh-my-openagent control-plane mechanism: native goal loop, TaskManager queue/control plane, WorkerSessionHandle, category routing, fallback attempts, and GUI TaskManager control. |

---

## Follow-up Targets

### 2026-07-10 Gear control-plane alignment

| Shared file | Gear-only behavior and upstream boundary |
|---|---|
| `[MOD]` `crates/agent/src/agent.rs` | Gear sessions expose typed `ActionOutcome`/`SendOutcome`/`SteerOutcome`, route task commands through the shared `TaskManager`, and add Stop Continuation persistence plus continuation lifecycle events. The ACP session ID is passed to `RunOptions` as the stable continuation key, so Stop/Restart and the runtime read/write the same per-session state. Non-Gear agent behavior is unchanged. |
| `crates/agent_ui/src/conversation_view/thread_view.rs` | Gear-only controls display structured rejection reasons and expose Stop Continuation, including while the continuation loop is idle; upstream conversation controls retain their existing paths. |

The shared changes above are an adapter boundary for `GEARBOX_GUI` native Gear sessions. Internal upstream action names, ACP protocol identifiers, and non-Gear cancellation behavior remain unchanged.

- Action-name translation layer for command palette entries
- Continue expanding settings item title/description mappings in `settings_ui.rs` and `gearbox_text.rs`
- Continue localizing Agent panel labels
- Continue localizing editor/project prompts and confirmation dialogs
- Gear runtime Phase 1 follow-up: upgrade the markdown queue/control-plane snapshot into a full panel-style observer that can open attempt/result/outcome artifacts from the Gear session's shared TaskManager
- Gear runtime Phase 1 follow-up: expose concurrency policy through CLI/env settings for worker/provider/model/category limits
- Gear runtime Phase 3 follow-up: keep extending model availability policy and provider-aware budget routing beyond the current CLI/env unavailable-model lists; live provider-registry refresh and richer budget strategy are still follow-up
- Gear runtime Phase 4 follow-up: replace resident-command interrupt/revive/stale detection with opencode-native session interrupt/revive/stale detection, and wire GUI to explicit interrupt controls
- Gear runtime Phase 7/8 sync: worker packet and coordinator review input now carry structured category resolution metadata; continue tightening the independent reviewer gate and provider-aware budget routing on top of that evidence
- Gear runtime Phase 7 sync: canonicalize provider/model comparisons across `workers.rs` and `task_manager.rs` so unavailable-route checks and no-op fallback detection stay consistent
- `crates/gearbox_agent/src/runtime.rs` `[MOD]`: Gear continuation state uses an explicit caller-owned session ID when supplied by the Gear ACP bridge; CLI and tests retain generated IDs. The legacy workspace-singleton continuation path has no compatibility reader or writer. Non-Gear agent behavior is unchanged.
- `crates/gearbox_agent/src/runtime.rs` `[MOD]`: `ReviewDimensionResult` now carries optional `reviewer_evidence: Option<ReviewerEvidence>` with execution_id, route, artifact_path, verdict. `ReviewGate::validate_independent_reviewers()` enforces unique execution_id per dimension. `from_inputs()` populates synthetic evidence for each dimension (coordinator, scope-check, security-check, qa-execution). Non-Gear agent behavior is unchanged.
- `crates/gearbox_agent/src/task_manager.rs` `[MOD]`: `ActionOutcome`, `SendOutcome`, `SteerOutcome` variants now carry `OutcomeContext` with `task_id`, `run_epoch`, `queue_position`. All construction sites and `impl` methods updated. Non-Gear agent behavior is unchanged.
- `crates/agent/src/agent.rs` `[MOD]`: Early-return `Noop` outcomes carry `OutcomeContext`. Non-Gear agent behavior unchanged.
- Gear runtime Phase 8 sync: `ROUTE_HINT=review` must still force at least one independent review worker turn even if the coordinator model already says `goal_satisfied: yes`
- `crates/agent/src/agent.rs` `[MOD]`: Gear moves the synchronous `Orchestrator::run` call onto a real blocking thread via `smol::unblock`, so the GPUI foreground dispatcher remains available to create and drive native Zed worker sessions. The change is confined to the Gear prompt path; upstream native Agent prompt execution is unchanged.
- `crates/agent/Cargo.toml` `[MOD]`: adds the workspace `smol` dependency required by the Gear-only blocking boundary above.
- `crates/agent/src/agent.rs` `[MOD]`: Gear planner prompts now require a typed `PlanGraphDraft`, and native Gear worker jobs validate, resolve, pin, and record the exact qualified `provider/model` selected by the route. Missing or unavailable routed models fail closed before a worker session starts. The changes remain inside the Gear prompt/worker path; upstream Agent routing and default-model behavior are unchanged.
- `crates/agent/src/thread.rs` `[MOD]`: adds a crate-private Gear worker hook that disables parent-model inheritance and pins an already-resolved model on the child thread. It is called only by the Gear native-worker adapter and does not alter ordinary upstream subagent construction.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-11 Plan Review + Phase Routing): the Gear-only prompt path now resolves a complete `PhaseRouteTable` before planning, supports exact live `provider/model` selection for separate Planner and PlanCritic direct-model calls, and passes a `PhaseRuntime` into the Gear orchestrator. A host-issued planner identity, independent PlanCritic identity, strict typed verdict, bounded revision hook, and phase route decisions/receipts are confined to Gear sessions; ordinary native Agent and external ACP prompt paths are unchanged. Per-phase JSON overrides use `GEARBOX_GEAR_PHASE_<PHASE>`, while `GEARBOX_GEAR_PHASE_ROUTES` accepts a complete table. Planner/PlanCritic ACP sessions remain a later broker-stage extension; this change does not add a generic upstream model router.
- `crates/agent/src/agent.rs` `[ADD]` (2026-07-11 ACP Broker Backend): new `GearAcpBrokerBackend` implements `NativeWorkerBackend` with typed channel dispatch to the foreground thread. Includes `spawn_gear_acp_broker_dispatcher`, `run_gear_acp_broker_worker`, `gear_acp_broker_discover_agents` (enumeration from `LanguageModelRegistry`), and `gearbox_acp_worker_broker_lifecycle` GPUI test. Broker contract types (`BrokerCapability`, `BrokerLifecycleReceipt`, `ModelAvailability`, etc.) re-exported from `gearbox_agent::worker_broker`. Non-Gear agent behavior unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-11 Production ACP broker wiring): Production `send_gear_prompt` now constructs `GearAcpBrokerBackend`, `spawn_gear_acp_broker_dispatcher`, and `PhaseBrokerFactory`; replaces `broker: None` with wired broker in `PhaseRuntime`. Removed `#[allow(dead_code)]` from ACP broker types. Removed `DirectModel` guard from `resolve_phase_language_model`. All non-Gear agent behavior unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-12 OpenCode-only phase vertical slice): Gear sessions can opt into qualified Planner/Executor/Reviewer OpenCode model profiles through `GEARBOX_GEAR_OPENCODE_PHASES` and `GEARBOX_GEAR_OPENCODE_*_MODEL`. Planner, PlanCritic, and plan revision then execute as broker-owned `opencode_session` workers with typed session identities and terminal artifacts; ordinary Agent sessions and the legacy direct-model Gear route are unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-12 IntentFold stage): OpenCode phase mode now runs a fresh read-only Metis-style IntentFold session before Planner, persists a hash-bound typed receipt, stops at `NeedsUser` before planning, and binds the accepted fold into the Planner prompt. Legacy direct-model Gear sessions and ordinary Agent sessions are unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-17 WorkerPacket rules parity): Gear's native Zed worker path now reuses `gearbox_agent::workers::discover_workspace_rules`, writes the same scoped rules receipt as command-backed workers, and populates `WorkerPacket`'s `injected_rules`/`rules_injection_path` fields. This remains confined to Gear worker construction; ordinary Agent behavior is unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-18 WorkerPacket step anchor): Gear native worker packet construction now carries the first runnable `current_step_id` into the shared hard prompt contract, matching the command-worker packet and preventing compaction/recovery from silently restarting a work order. Ordinary Agent behavior is unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-12 executable budget ledger): the Gear-only orchestrator call now supplies the optional `RunOptions::budget` field, allowing the Gear runtime to enforce persisted call/token/cost reservations before worker dispatch. Ordinary Agent prompt execution is unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-12 unified phase budget): Gear budget environment parsing now exposes `GEARBOX_GEAR_MAX_CALLS_PER_EPOCH` and aligns token/unknown defaults with the unified IntentFold, Planner, PlanCritic, revision, and worker call ledger. Ordinary Agent sessions remain unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-12 StrategistNextGoal gate): OpenCode phase mode now runs a fresh planner-model strategist session after final review, returning a strict typed next-goal verdict bound to the current goal, epoch, final report, plan, and budget ledger. Direct-model and ordinary Agent sessions remain unchanged.
- `[NEW]` `crates/gearbox_agent/src/open_code_phase_runtime.rs`: Extracted Gear-owned `GearOpenCodePhaseRunner` body from `crates/agent/src/agent.rs` into a reusable `OpenCodePhaseRuntimeFactory` builder in `gearbox_agent`. The factory accepts workspace, WorkerConfig, PhaseBrokerFactory, CancellationToken, PhaseRouteTable, and LiveModelInventory, and returns a complete `PhaseRuntime` with independent OpenCode session hooks for IntentFold, Planner, PlanCritic, PlanRevision, and Strategist. Each phase gets its own execution_id, session_id, task_id, and terminal broker receipt. Model fallback (executor/reviewer unset → planner) is recorded in the receipt. CLI `gear run --objective --opencode-phases` now uses the production factory; without `--opencode-phases` the legacy `PhaseRuntime::legacy()` path is preserved.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-12 OpenCode phase factory extraction): `GearOpenCodePhaseRunner` struct and methods are thinned to a bridge — each method delegates to `gearbox_agent::open_code_phase_runtime::OpenCodePhaseRunner`. The five prompt builder functions (`gear_opencode_*_prompt`) were moved to `gearbox_agent::open_code_phase_runtime`. The `send_gear_prompt` production path can now call `OpenCodePhaseRuntimeFactory::build()` directly when opencode phases are enabled. Upstream non-Gear Agent path is completely unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-13 ACP worker routing): the Gear-only native backend now multiplexes the existing foreground ACP broker into OpenCode, OpenCodeSession, Codex, and Claude worker routes. Its session handle forwards worker lifecycle events and preserves unknown per-turn usage totals rather than recording them as zero; terminal artifact paths are cloned before session cleanup. `WorkerRegistry` keeps command fallback when no ACP backend is injected; ordinary non-Gear Agent routing is unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-13 ACP capability truthfulness): the Gear-only native ACP route now omits `SessionResume` from its capability receipt. The current handle supports follow-up and steering only while live; it has no durable provider reattach operation and terminal cleanup still closes the session. This prevents Gear UI/runtime policy from promising OMO-style resume before an explicit OpenCode descriptor and reattach contract exists. Ordinary non-Gear Agent behavior is unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-13 ACP disposal boundary): Gear ACP handle disposal is no longer an alias for cancellation. A Gear-only foreground dispatcher operation explicitly closes the registered worker ACP session, clears its running-session mapping and clears the handle identity; cancellation remains an interrupt request for the current turn. Ordinary non-Gear Agent lifecycle behavior is unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-13 OpenCode free fallback): Gear-only worker configuration now has an opt-in `GEARBOX_GEAR_OPENCODE_FREE_FALLBACKS=1` route set for the verified OpenCode selectors `opencode/hy3-free`, `opencode/mimo-v2.5-free`, and `opencode/deepseek-v4-flash-free`. An explicit `GEARBOX_GEAR_WORKER_SEQUENCE` still takes precedence. The built-in command uses the existing worker packet/prompt/model environment contract; ordinary Agent sessions and an unset Gear environment keep their previous behavior.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-13 OpenCode free fallback backend boundary): those three CLI-verified free selectors deliberately return `None` from the Gear ACP-native backend so `WorkerRegistry` takes its existing command-worker fallback. This prevents an available ACP dispatcher from silently bypassing `opencode run` and claiming native-provider evidence for a CLI-only route. Other OpenCode routes retain their existing ACP behavior.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-13 OpenCode free fallback prompt transport): the built-in command now sends `GEARBOX_WORKER_PROMPT` through stdin rather than expanding it into a shell command argument. A live `opencode/hy3-free` smoke proved the stdin form, avoiding argument-length failures on real Gear worker packets. This affects only the opt-in free command route.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-13 OpenCode free session continuation): the opt-in free routes now use `OpencodeSession` command workers. They persist the OpenCode JSON `sessionID` into the existing sealed resident descriptor and, on a later turn, invoke `opencode run --session "$GEARBOX_WORKER_SESSION_ID"` only when that provider id exists. A same-workspace DeepSeek Free start/continue smoke returned the identical provider session id with a cache-read receipt. The free selector command still bypasses the native ACP path; unrelated routes are unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-14 ACP terminal resident lifecycle): the Gear-only native ACP broker now retains its registered worker session after a normal `EndTurn` and dispatches terminal `follow_up`/`steer` turns through the same `session_id`/`AcpThread`; only the explicit Gear `dispose` dispatch removes the running-session mapping and closes the provider session. Upstream merges must preserve this Gear-only resident behavior without changing non-Gear session close semantics.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-14 ACP unknown usage evidence): the Gear-only native ACP broker now persists a `BrokerUsage` record on every completed turn, including explicit `unavailable_reason` and `None` token/cost/cache fields when provider telemetry is missing. It never converts unknown values to zero; non-Gear Agent usage behavior is unchanged.
- `crates/gearbox_agent/src/workers.rs` / `crates/gearbox_agent/src/task_manager.rs` `[MOD]` (2026-07-14 resident revive event epoch): Gear resident revive clears bounded replay history before installing the next epoch listener, while initial late-subscription replay remains enabled. Command-backed and native ACP event handles expose the Gear-only reset hook; upstream non-Gear event behavior is unchanged.
- `crates/gearbox_agent/src/task_manager.rs` `[MOD]` (2026-07-14 resident revive prompt gate settlement): Gear terminal resident follow-up/steer failures now settle the already-reserved prompt dispatch gate as `Failed` or `PossiblyAccepted` before returning the revive error, preserving rollback and retry/idempotency semantics. Upstream TaskManager and non-Gear behavior are unchanged.
- `crates/gearbox_agent/src/state.rs` / `crates/gearbox_agent/src/task_manager.rs` `[MOD]` (2026-07-14 ambiguous resident revive): Gear keeps a terminal revive in the new Running epoch when dispatch reports an OMO-style ambiguous post-dispatch error, returns typed `PossiblyAccepted`, and carries semantic prompt dedupe across the epoch using the stable resident session identity. Explicit failures still rollback; non-Gear behavior is unchanged.
- `crates/gearbox_agent/src/task_manager.rs` `[MOD]` (2026-07-14 activity-aware stale timeout): Gear event-capable worker tasks now keep an in-process activity heartbeat from worker events, and stale sweeping uses the last observed activity before falling back to `started_at`. Command workers without event subscriptions retain the existing timeout; non-Gear behavior is unchanged.
- `crates/gearbox_agent/src/task_manager.rs` `[MOD]` (2026-07-14 tool-call circuit breaker): Gear event-capable worker tasks now count tool calls per task/epoch, hash tool name plus normalized arguments for consecutive-loop detection, enforce OMO-aligned defaults of 20 repeated signatures and 4000 total calls, and cancel on the next TaskManager tick with an auditable reason. The policy is configurable through `GEARBOX_GEAR_TOOL_CIRCUIT_BREAKER`, `GEARBOX_GEAR_TOOL_LOOP_THRESHOLD`, and `GEARBOX_GEAR_MAX_TOOL_CALLS`; command workers without event subscriptions cannot claim this enforcement, and non-Gear behavior is unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-14 ACP tool event bridge): the Gear-only ACP broker now projects each newly observed native `AgentMessageContent::ToolUse` into one `WorkerEvent::ToolCallStarted`, deduped by ACP tool id before entering the existing TaskManager evidence/circuit chain. Ordinary non-Gear Agent execution and provider permission/session behavior are unchanged.
- `crates/gearbox_agent/src/task_manager.rs` `[MOD]` (2026-07-14 cancellation notifications): Gear completion notifications now include `Cancelled` and `Interrupted` terminal records, while pending/running tasks remain excluded; existing epoch dedupe, parent-state buffering, retry and `CompletionNotified` audit behavior are unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-14 ACP permission evidence): Gear-only native ACP workers now subscribe to real `AcpThreadEvent::ToolAuthorizationReceived` events, map the resolved tool kind/status into one deduplicated `permission-events.jsonl` record, and no longer append a synthetic denial after every response. Ordinary Agent authorization behavior is unchanged.
- `crates/gearbox_agent/src/task_manager.rs` `[MOD]` (2026-07-14 notification retry window): Gear completion notifier now performs a bounded three-round redelivery window after the existing two-attempt round, records `notification_failed_epoch` only after the window is exhausted, and preserves existing epoch dedupe/parent-state behavior. Ordinary non-Gear behavior is unchanged.
- `crates/gearbox_agent/src/runtime.rs` `[MOD]` (2026-07-14 notification commit boundary): Gear completion notification now appends or reuses the durable `CompletionNotified` event before advancing `notified_epoch`, and retries after a marker write failure reuse the matching session/task/epoch event instead of duplicating it. Ordinary non-Gear behavior is unchanged.
- `crates/gearbox_agent/src/task_manager.rs` `[MOD]` (2026-07-14 worker concurrency key): Gear task concurrency now keys the selected worker route model (with worker-kind fallback when unconfigured) instead of the coordinator/planner model, keeping queue limits attached to the actual execution worker. Ordinary non-Gear behavior is unchanged.
- `crates/gearbox_agent/src/task_manager.rs` `[MOD]` (2026-07-14 fallback concurrency admission): Gear task scheduling now acquires the final selected route's concurrency slot before starting a worker, releases it when worker startup fails, and requeues an internally selected fallback route when that route's key is already full. This prevents fallback workers from starting outside TaskManager ownership. Ordinary non-Gear behavior is unchanged.
- `crates/gearbox_agent/src/runtime.rs`, `crates/gearbox_agent/src/state.rs` `[MOD]` (2026-07-14 objective pre-planning failure settlement): when an active objective goal returns an ordinary execution error before producing a terminal outcome, Gear now persists the Goal, epoch, ObjectiveGraph, continuation, and terminal objective event as failed before returning the original error. Explicit crash-test seams remain recoverable instead of being terminalized. Ordinary non-Gear behavior is unchanged.
- `[NEW]` `crates/gearbox_agent/src/gui.rs` (2026-07-14 GBX-057): defines the typed Gear GUI runtime snapshot/event contract, bounded timeline and worker-output limits, lossless-vs-telemetry event buffering, and snapshot validation. Gear-only crate; no upstream UI behavior changes.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-14 GBX-057): Gear prompt dispatch and review/event queues use bounded async channels so worker backpressure cannot grow unbounded conversation/runtime memory. Ordinary non-Gear Agent paths are unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-14 GBX-057 durable projection/task epoch): Gear exposes a bounded runtime snapshot sourced from durable continuation, goal, objective, budget, epoch and review ledgers; selected task follow-up/steer commands validate `task_id` and `run_epoch` before dispatch. Ordinary non-Gear Agent paths are unchanged.
- `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 durable projection/task epoch): Gear task panel renders objective/goal/epoch/budget/review/recovery summaries, tracks a selected task, and routes resident controls through the typed task epoch boundary. Ordinary Agent panels are unchanged.
- `crates/agent/src/agent.rs` `[MOD]` (2026-07-14 GBX-057 background snapshot cache): Gear session runtime snapshots are refreshed on a bounded 200ms background task; GUI render reads the cached typed projection instead of performing durable-state disk I/O on the GPUI foreground. Ordinary non-Gear Agent paths are unchanged.
- `crates/gearbox_agent/src/gui.rs` / `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 feedback/recovery): runtime projection counts bounded tool, permission, task and worker event artifacts; Gear panel exposes Restart Continuation alongside stop/recovery state. Gear-only behavior.
- `crates/agent/src/agent.rs` / `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 selected-task controls): interrupt and cancel now validate and target the selected task/run epoch instead of silently acting on the manager's current task. Ordinary Agent behavior is unchanged.
- `crates/gearbox_agent/src/gui.rs` / `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 runtime timeline): GUI projection reads a bounded tail of session event ledger, classifies critical/milestone/telemetry events, and renders recent runtime timeline entries. Gear-only behavior.
- `crates/gearbox_agent/src/gui.rs` `[MOD]` (2026-07-14 GBX-057 projection regression): adds deterministic durable-state and bounded event-tail tests for the Gear GUI projection contract. Gear-only behavior.
- `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 action receipts): Gear controls surface the typed outcome/debug receipt of follow-up, steer, interrupt, cancel, stop, and restart actions in the runtime panel. Ordinary Agent behavior is unchanged.
- `crates/agent/src/agent.rs` / `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 revive control): a selected task with `Messageability::Revive` now exposes a Retry action that dispatches through the existing resident follow-up gate with task epoch validation. Ordinary Agent behavior is unchanged.
- `crates/gearbox_agent/src/tools.rs` `[MOD]` (2026-07-14 GBX-057 Rust admission): Gear-owned shell execution serializes its own Cargo/rustc commands through a process-local admission gate; it never inspects or kills unrelated IDE/rust-analyzer processes. Gear-only behavior.
- `crates/gearbox_agent/src/tools.rs` `[MOD]` (2026-07-14 GBX-057 Rust lease hardening): the Gear-owned Cargo/rustc admission boundary now also uses a workspace `.gearbox-agent/locks/rust-build.lock` file lease, with cancellation/timeout-aware polling, so separate Gear processes cannot silently overlap or wait forever. It still never inspects or kills unrelated IDE/rust-analyzer processes. Gear-only behavior.
- `crates/gearbox_agent/src/task_manager.rs` / `crates/gearbox_agent/src/gui.rs` `[MOD]` (2026-07-14 GBX-057 durable task projection): after GUI/runtime restart, Gear reconstructs a bounded read-only task/attempt/model/route projection from durable `task-record.json` artifacts, filtered to the current root/parent/worker session. Persisted tasks expose no messageability unless a live worker handle is present; ordinary non-Gear behavior is unchanged.
- `crates/agent/src/agent.rs` / `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 restart action): Gear's Restart Continuation control now clears the durable stop marker and, when no live TaskManager exists, re-enters the Gear prompt path from the persisted Goal request. The resume marker is Gear-only; ordinary Agent restart/cancel behavior is unchanged.
- `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 GUI identity): Gear task entry Restart buttons now use task-specific GPUI element IDs, preventing duplicate IDs when multiple runtime tasks are rendered. Ordinary Agent UI is unchanged.
- `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 runtime health display): Gear runtime panel now displays a bounded Goal request preview and health/resource feedback (owned child count, Rust work state, telemetry pressure, refresh requirement, and latest runtime error). Ordinary Agent UI is unchanged.
- `crates/gearbox_agent/src/gui.rs` `[MOD]` (2026-07-14 GBX-057 runtime health projection): durable GUI snapshots derive owned-child count and latest worker error from bounded task/attempt state; unknown Rust/refresh fields remain explicit rather than fabricated. Gear-only behavior.
- `crates/gearbox_agent/src/gui.rs` / `crates/agent_ui/src/conversation_view/thread_view.rs` `[MOD]` (2026-07-14 GBX-057 worker feedback tail): GUI snapshots now carry a bounded typed tail of tool, permission, task, and worker event lines, and the Gear panel renders the latest feedback with permission highlighting. Ordinary Agent UI is unchanged.
- 2026-07-14：Gearbox 默认 terminal 设置补齐 `open_links_in_mouse_mode`，修复开发模式启动时 `TerminalSettings::from_settings` 对缺失字段的 unwrap panic；仅涉及 Gearbox 独立设置资源。
- 2026-07-14：Gear runtime 面板增加独立的 `Resume Gear` 入口，即使 durable snapshot 没有可展示的 task entry 也能触发 continuation recovery；属于 Gear UI 控制层，不改变上游 Agent 行为。
- 2026-07-14：Gear runtime 面板顶部同时提供 `Resume Gear` 与 `Stop Gear`，确保无 task entry 的 durable runtime 仍可被用户控制；属于 Gear UI 控制层。
- 2026-07-14：Gear GUI 生产路径在没有显式 `GEARBOX_GEAR_OBJECTIVE` 时采用 `ObjectivePolicy::default()`（单 epoch、有界预算）；headless/CLI 仍保持环境变量开关语义，测试路径不改变既有 fixture 行为。
- 2026-07-14：PlanCritic/Oracle 的 repository observation 门禁仅对 OpenCode worker-backed phase 强制；native deterministic/direct-model review 没有仓库工具，不能伪造 observation receipt。同步修正 Gear native E2E 断言以覆盖独立 Oracle 调用。
- 2026-07-14：Worker Feedback 事件在 Gear runtime 面板中改为可点击的 bounded button，点击后选择对应 task，便于从 permission/tool/worker 反馈直接进入 task 控制面；Gear UI-only。
- 2026-07-14：Gear runtime 面板补显示已有 typed snapshot 的 cost、unknown usage、last activity 和 recovery state；仅 Gear UI 展示层。
- 2026-07-14：Resume/Stop 控件根据 durable continuation status 禁用无效重复动作；属于 Gear UI 状态门禁。
- 2026-07-14：Gear runtime 面板补显示 snapshot schema/sequence、session、objective、goal identity，便于用户识别刷新和跨会话串线；Gear UI-only。
- 2026-07-14：修复 Gear GUI snapshot sequence 生产路径恒为 0 的问题，改用 durable event ledger 的单调字节游标，避免 GUI refresh 无法识别新事件；Gear projection-only。
- 2026-07-14：runtime 面板 identity/feedback 行补显示 workspace、task event 和 worker error 计数，避免 snapshot 字段只在内部存在；Gear UI-only。
- 2026-07-14：新增 100k event 的 Gear GUI snapshot 序列化 boundedness 回归，确认 timeline/feedback projection 在长会话下仍受硬上限；Gear-only test。
- 2026-07-14：GUI snapshot durable cursor 回归增加“追加 event 后 sequence 严格递增”断言，防止刷新游标静止/回退；Gear-only test。
- 2026-07-14：GUI snapshot 从有界 session event ledger 尾部推导 `health.last_activity_at`，让重启后的 runtime 面板仍能显示最近活动时间；Gear-only projection/test。
- 2026-07-14：Gear runtime snapshot 尚未就绪时，GUI 显示明确的初始化状态而不是隐藏整个 runtime 面板；Gear UI-only。
- 2026-07-14：Gear runtime timeline 条目改为可点击并携带序号，点击带 task identity 的事件可选中对应任务进入后续控制；Gear UI-only。
- 2026-07-14：Gear runtime timeline 在保留有界消息的同时显示最多 180 字符的 JSON payload 摘要，提升结构化事件证据可见性；Gear UI-only。
- 2026-07-14：Gear runtime 面板补显示 review 最新事件/epoch 计数/plan revision/bundle 状态，以及 recovery resume/stuck/progress 细节；Gear UI-only。
- 2026-07-14：Gear snapshot 后台刷新失败现在持久于 session 的有界错误字段并显示在 runtime 面板，避免 GUI 静默展示陈旧状态；Gear UI/bridge-only。
- 2026-07-14：首次 snapshot 刷新失败时，Gear 初始化占位面板也显示刷新错误，不再把失败状态伪装成正常初始化；Gear UI-only。
- 2026-07-14：Gear 初始化占位面板增加非 Gear 会话隔离门禁，保持普通 Agent UI 不显示 Gear Runtime；Gear UI-only。
- 2026-07-14：最新 `gearbox` 原生构建成功，15 秒 `GEARBOX_GUI=1` 启动烟测无 panic，仅有 X/DRI3 EGL 加速警告；Gear GUI smoke evidence。
- 2026-07-14：清理 Gear bridge 生产路径中仅测试使用的 `Scope` 导入，agent/agent_ui 编译不再产生该 unused import warning；Gear bridge hygiene。
- 2026-07-14：Gear GUI bounded snapshot 测试二进制串行重复 20 次，全部通过，最大 RSS 65,964 KiB；该证据属于短程重复压力，不替代长时 soak。
- 2026-07-14：Gear GUI bounded snapshot 测试二进制串行重复 200 次，全部通过，最大 RSS 66,156 KiB，未见随轮次线性增长；仍不替代长时真实 runtime soak。
- 2026-07-14：Gear runtime 面板为 Worker Feedback/Runtime Timeline 增加有界 Show More/Show Less，分别可查看最多 32/500 条保留记录；Gear UI-only。
- 2026-07-14：完整 `agent_ui` library 串行回归 402 passed、0 failed，确认 Gear runtime UI 改动未破坏普通 Agent UI；验证使用单 Cargo/单测试线程。
- 2026-07-14：GBX-059 operator preflight 将 production Gear phase-broker fixture 的 stale completion expectation 从 3 修正为 4；该测试边界修复只恢复当前 runtime review 拓扑的一致性，不计入 GBX-058 GUI subject work。
- 2026-07-14 GBX-075：`crates/agent/src/agent.rs` `[MOD]` — 新增 `gear_codex_acp_model_profiles_from_env()`、`gear_phase_table_uses_codex_acp()`、`gear_codex_acp_phase_worker_config()`；`gear_phase_route_table_from_env()` 在 OpenCode-only/legacy 之前优先尝试 CodexAcp route；`gear_phase_uses_opencode_worker()` 同时识别 `PhaseBackend::CodexAcp`；`resolve_phase_host_language_model()` 跳过 CodexAcp 的 direct model 解析。非 Gear Agent 行为不变。
- 2026-07-14 Gear runtime-root：`StateStore`、Gear CLI/GUI bridge、ACP broker artifact path、worker evidence、Rust command lock/tmp 和 planner prompt 的新写入根统一为用户可见的 `.gear/`；旧 `.gearbox-agent/` 仅保留为迁移/取证兼容名称，未重命名 `gearbox_agent` crate 或上游内部标识。后续上游同步需保留 Gear-specific runtime-root 路由。
- 2026-07-14 GBX-077 runtime/GUI alignment：`GearRuntimeSnapshot` now projects the persisted PlanGraph plus `PlanNodeRunLedger` as bounded work-order TODOs, and the Gear conversation panel renders task ids, dependencies, waves, and durable node status from that projection. This is a Gear-only UI surface; upstream Agent behavior remains unchanged.
- 2026-07-15 GBX-084 GUI refresh bridge：`crates/agent/src/agent.rs` 的 Gear snapshot poll 在更新成功或失败状态后调用 Agent entity `cx.notify()`，确保 Gear-only runtime panel 能立即消费 durable snapshot/error 变化；普通 Agent 行为与数据路径不变。
- 2026-07-15 GBX-085 session reopen bridge：`crates/agent/src/agent.rs` 为 Gear session 抽出可幂等恢复的 runtime snapshot poll；新建/重开 Gear session 均从 `.gear` 持久状态持续投影，运行中的 worker manager 仍额外提供 live task snapshot；普通 Agent 行为与数据路径不变。
- 2026-07-15 GBX-086 work-order control bridge：共享 `crates/agent_ui/src/conversation_view/thread_view.rs` 的 Gear 计划工单条目现在可点击并选中对应 worker task（优先 durable `worker_task_id`），随后复用已有 Gear interrupt/cancel/retry/feedback 控制；普通 Agent UI 不改变。
- 2026-07-15 GBX-087 route receipt projection：`crates/gearbox_agent/src/gui.rs` 从 `.gear` 的 phase-route receipts 投影每个 phase 的实际 backend/model/candidate/fallback/source 到 Gear lifecycle，`thread_view.rs` 显示 durable route receipts；普通 Agent UI 行为不变。
- 2026-07-15 GBX-088 route evidence bridge：Gear GUI route 行增加可打开的 receipt artifact 路径；projection 对最多 64 个 receipt 做 bounded 解析并把损坏/不一致数量作为 `phase_route_errors` 显示，避免审计证据静默缺失；普通 Agent UI 行为不变。
- 2026-07-15 GBX-089 serial work-order marker：`crates/gearbox_agent/src/plan_graph.rs` 增加 active-node serial scheduler regression，确认 capacity=1 且当前 node active 时只选择同一最早 wave 的下一个 node；`thread_view.rs` 用菱形 marker 显示当前 GUI-selected worker task，保持计划 TODO 与控制选择同步。
- 2026-07-15 GBX-090 continuation failure recovery：共享 `crates/gearbox_agent/src/runtime.rs` 在 continuation resume 时把 durable `Failed` PlanNode 显式 requeue 为 `Runnable`，保留 attempt/worker/error 证据并记录 recovery epoch event；`state.rs` 提供受控 requeue API，Gear GUI 通过同一 snapshot 显示可重试工单；普通 Agent 行为不变。
- 2026-07-15 GBX-091 plan contract hardening：`crates/gearbox_agent/src/plan_graph.rs` 现在要求 PlanGraph 顶层同时提供 `must_have`、`must_not_have`、`topology_lock` 和 `final_acceptance` 决策标准，避免模型只生成任务列表而缺少 OMO 式目标边界/最终验收；同文件增加缺失顶层验收的回归测试。Gear-only 行为；普通 Agent 不变。
- 2026-07-15 GBX-090 continuation evidence repair：`crates/gearbox_agent/src/runtime.rs` 恢复跨进程 continuation 时继承上一轮 verification artifact，并在失败节点的后续成功尝试重新生成 GREEN evidence；`crates/gearbox_agent/src/gui.rs` 对预算 token/cost 汇总使用饱和加法，避免多轮默认预算导致 GUI snapshot 溢出 panic。Gear-only 行为；普通 Agent 不变。
- 2026-07-15 GBX-092 plan projection cardinality：`crates/gearbox_agent/src/gui.rs` 的 Gear 计划进度改从完整 PlanGraph/PlanNodeRunLedger 计算，不再从 GUI 展示用的 128 条有界列表计算；只有 `Completed` 才计入完成，`GreenVerified/Reviewed` 保持中间态。新增 130 工单投影回归，保证长 OMO 式计划的 runtime 总数与 GUI 一致。Gear-only 行为；普通 Agent 不变。
- 2026-07-15 GBX-093 OMO plan control contract：`PlanGraphDraft` 新增并校验顶层 `preflight`、`rollback`、`final_verification`；planner prompt、确定性 fallback、Markdown plan projection 和 Gear GUI lifecycle 均显示这些控制项。runtime 在计划开始时写入 preflight baseline，失败工单写入 rollback request，最终 verification wave 写入 plan-check artifact；rollback 默认只记录请求，不隐式修改用户工作树。Gear-only 行为；普通 Agent 不变。
- 2026-07-15 GBX-094 rollback confirmation gate：Gear GUI 在存在 durable rollback request 且尚未确认时显示 `Confirm rollback`；共享 `crates/agent/src/agent.rs` 只记录带 session/epoch/请求 artifact 的确认 receipt，不执行隐式工作树 mutation，确认后 GUI 隐藏重复按钮。该安全门禁属于 Gear 控制层；普通 Agent 不变。
- 2026-07-15 GBX-095 strategist next-goal projection：`crates/gearbox_agent/src/gui.rs` 有界解析 durable `strategist-next-goal-receipt.json`，把 decision、next objective、answerability、acceptance/questions/evidence 投影到 Gear lifecycle；`crates/agent_ui/src/conversation_view/thread_view.rs` 显示当前下一目标，保持 OMO goal pursuit 与 runtime/GUI 同源。普通 Agent UI 不变。
- 2026-07-15 GBX-096 needs-user question projection：Gear GUI 在 next-goal verdict 含 `required_questions` 时显示有界问题列表；用户可复用已有 Gear composer 的 Follow Up/Steer 通道回答，问题仍来自 durable strategist receipt，不新增绕过 runtime 的写入口。普通 Agent UI 不变。
- 2026-07-15 GBX-097 next-question composer bridge：`crates/agent_ui/src/conversation_view/thread_view.rs` 在 Gear next-goal questions 下增加 `Answer next question`，只把第一条问题填入现有 MessageEditor；发送仍复用 Follow Up/Steer 的 task/run-epoch 门禁，未新增直接 runtime 写入路径。普通 Agent UI 不变。
- 2026-07-15 GBX-098 needs-user epoch resume：`ObjectiveGraph`/objective event ledger 新增 `UserAnswerAccepted` reopen transition；continuation controller 按 root session 找回 terminal needs_user objective，绑定新 epoch、保留旧证据并把用户回答追加到 resumed request 后再继续 active frontier。新增 graph/event 回归，防止回答形成全新孤立 objective。普通 Agent 行为不变。
- 2026-07-15 GBX-099 objective frontier/history projection：`crates/gearbox_agent/src/gui.rs` 将 `.gear` ObjectiveGraph 最近 32 个 goal/epoch 节点投影为有界 history，保留 parent、status、request 和 terminal evidence；`crates/agent_ui/src/conversation_view/thread_view.rs` 在 Gear runtime 面板显示完整目标演进并高亮 active frontier。普通 Agent UI 不变。
- 2026-07-15 GBX-100 work-order routing brief：`crates/gearbox_agent/src/runtime.rs` 在每个 PlanGraph 工单 dispatch 前生成绑定 goal/epoch/plan revision/attempt、路由模型、依赖、范围、TDD、验收和 artifact 的 `.gear` routing brief，并写入 durable task input；`crates/gearbox_agent/src/gui.rs` 与 `crates/agent_ui/src/conversation_view/thread_view.rs` 投影并显示 brief 路径。普通 Agent 行为不变。
- 2026-07-15 GBX-101 planner decomposition quality gate：`crates/gearbox_agent/src/plan_review.rs` 在 deterministic verifier 的 Structure check 中加入高上限拆解检查，只有明确超过 8 个 scoped files 或 12 个 must-do steps 的单工单被拒绝；`crates/gearbox_agent/src/gui.rs` 从 durable verifier report 投影质量门状态、findings 和 artifact，`crates/agent_ui/src/conversation_view/thread_view.rs` 显示结果。Gear-only 行为；普通 Agent 不变。
- 2026-07-15 GBX-102 planner decomposition protocol：共享 `crates/agent/src/agent.rs` 的 Gear planner/revision prompt 现在先按“目标边界→最小可验收工单→依赖波次→工单 contract”生成，明确单 deliverable、弱 worker 可执行、超过 8 scoped files/12 must-do 时拆分；新增 fake-model prompt contract 断言。普通 Agent planner 行为不变；Gear 测试路径同步使用 `.gear` runtime root。
- 2026-07-15 GBX-103 work-order evidence projection：`crates/gearbox_agent/src/gui.rs` 从 durable `PlanNodeRun` 有界投影 RED、最多 8 条 GREEN 和 Review evidence 路径；`crates/agent_ui/src/conversation_view/thread_view.rs` 在每个 Gear TODO 行显示证据状态/路径摘要。普通 Agent UI 不变。
- 2026-07-16 GBX-104 work-order completion gate：`crates/gearbox_agent/src/runtime.rs` 在 PlanNodeRun 进入 Completed 前先绑定 Review artifact、持久化每个 completion predicate 的 criterion evidence，并要求 GREEN、Review 与全部 criterion receipts 通过；不满足时节点保持 Failed 并记录可恢复原因，GUI 通过已有 task error/evidence projection 显示。普通 Agent 行为不变。
- 2026-07-16 GBX-105 completion-gate repair receipt：当工单 evidence gate 失败时，`crates/gearbox_agent/src/runtime.rs` 额外写入有界 work-order repair request artifact，并把路径绑定到 PlanNodeRun error；continuation 仍只重排 Failed 节点，GUI 通过当前工单错误/证据行显示 repair 路径。普通 Agent 行为不变。
- 2026-07-16 GBX-106 work-order contract metadata projection：`crates/gearbox_agent/src/gui.rs` 将 PlanTaskContract 的 size/risk/commit boundary 投影到有界 GUI TODO；`crates/agent_ui/src/conversation_view/thread_view.rs` 显示这些执行边界，并在 Failed 工单显示 Resume Gear requeue 提示。普通 Agent UI 不变。
- 2026-07-16 GBX-107 plan boundary projection：`crates/gearbox_agent/src/gui.rs` 将 PlanGraph 顶层 `must_have`、`must_not_have`、`topology_lock`、`final_acceptance` 有界投影到 Gear lifecycle；`crates/agent_ui/src/conversation_view/thread_view.rs` 在 Work Orders 面板显示这些目标边界。普通 Agent UI 不变。
- 2026-07-16 GBX-108 plan approval projection：`crates/gearbox_agent/src/gui.rs` 读取 durable `PlanApprovalState`，投影 approval status、revision count、critic receipt hash 和 approval artifact；`crates/agent_ui/src/conversation_view/thread_view.rs` 显示 PlanCritic/Oracle approval gate。普通 Agent UI 不变。
- 2026-07-16 GBX-109 per-work-order preflight：`crates/gearbox_agent/src/runtime.rs` 在每次 PlanGraph 工单 dispatch 前记录 baseline changed-file count/hash、allowed/forbidden/write scope、max files 和 dependencies 到 `.gear` preflight artifact；`crates/gearbox_agent/src/gui.rs` 与 `thread_view.rs` 投影并显示该路径。普通 Agent UI 不变。
- 2026-07-16 GBX-110 final verification receipt projection：`crates/gearbox_agent/src/gui.rs` 校验 durable `final-verification-wave.json` 与当前 PlanGraph 绑定，投影 passed/failed/invalid、receipt hash 和 artifact；`crates/agent_ui/src/conversation_view/thread_view.rs` 显示最终 verification receipt 状态。普通 Agent UI 不变。
- 2026-07-16 GBX-111 selected work-order contract projection：`crates/gearbox_agent/src/gui.rs` 将每个 PlanTaskContract 的 bounded `must_do`、`must_not_do` 和 completion predicates 投影到 snapshot；`crates/agent_ui/src/conversation_view/thread_view.rs` 在选中工单下直接展开执行 contract。普通 Agent UI 不变。
- 2026-07-16 GBX-112 selected work-order test/QA projection：`crates/gearbox_agent/src/gui.rs` 有界投影 test strategy、最多 8 条 verification commands 和 QA scenarios；`crates/agent_ui/src/conversation_view/thread_view.rs` 在选中工单详情中显示 TEST/VERIFY/QA contract。普通 Agent UI 不变。
- 2026-07-15 GBX-113 selected work-order boundary projection：`crates/gearbox_agent/src/gui.rs` 将 PlanTaskContract 的 required capabilities、references、required artifacts、allowed/forbidden/write scope 和 max files changed 有界投影到 Gear snapshot；`crates/agent_ui/src/conversation_view/thread_view.rs` 在选中工单详情显示这些契约边界。普通 Agent UI 不变。
- 2026-07-15 GBX-114 canonical OMO plan projection：`crates/gearbox_agent/src/product.rs` 从同一个 PlanGraph 输出 OMO 风格的 TL;DR/Scope/Verification/Execution/Todos/Final verification/Commit/Success sections，并用 `[ ]` 标记工单与 completion predicates；`crates/gearbox_agent/src/gui.rs` 和 `crates/agent_ui/src/conversation_view/thread_view.rs` 投影 canonical `plan.md` 路径，同时继续以 runtime ledger 作为实时状态源。普通 Agent 行为不变。
- 2026-07-15 GBX-115 continuation plan reuse：`crates/gearbox_agent/src/runtime.rs` 在普通 continuation 中读取并校验已批准/未审阅 PlanGraph，复用同一 plan revision/hash 和 PlanNodeRun ledger；仅 NeedsUser 且请求改变时重新规划。新增 `PlanReused` event，`gui.rs`/`thread_view.rs` 投影并显示复用状态。普通 Agent 行为不变。
- 2026-07-15 GBX-116 final verification wave projection：`crates/gearbox_agent/src/gui.rs` 从绑定的 `FinalVerificationWaveReceipt` 有界投影四个 final dimensions；`product.rs` canonical `plan.md` 固定输出 OMO F1-F4 checklist；`agent_ui` 逐项显示 final checks，实时状态仍来自 `.gear` receipt。普通 Agent 行为不变。
- 2026-07-15 GBX-117 plan context projection：`PlanGraphDraft`/planner schema 新增可选 assumptions、findings、decisions、open_questions；`product.rs` canonical Markdown 与 `gui.rs`/`thread_view.rs` 有界投影这些 OMO durable-draft context。普通 Agent 行为不变。
- 2026-07-15 GBX-118 adversarial QA contract：`PlanQaContract` 新增兼容旧计划的 `adversarial_path`，planner/exemplar 要求记录适用 trigger 或 not-applicable 证据；`product.rs`、`gui.rs`、`thread_view.rs` 有界投影该 QA 类别。普通 Agent 行为不变。
- 2026-07-15 GBX-119 open-question approval gate：`plan_review.rs` 的 deterministic Structure verifier 现在把 `PlanGraphDraft.open_questions` 中的未解决问题作为阻塞 finding，因而 PlanCritic 不能批准 decision-incomplete 计划；Gear GUI 将该上下文明确标为 approval blocked。普通 Agent 行为不变。
- 2026-07-15 GBX-120 live plan checkbox projection：`product.rs` 新增从 `PlanNodeRunLedger` 投影 `[ ]/[~]/[x]/[!]` 的 canonical plan 渲染；runtime 在工单状态持久化后刷新同一 `.gear` `plan.md`，使 OMO 风格 TODO 文件、runtime ledger 与 GUI 状态保持同源。普通 Agent 行为不变。
- 2026-07-15 GBX-121 final-wave checkbox projection：`product.rs` 的 F1-F4 Final Verification Wave 现在可从 `FinalVerificationWaveReceipt` 投影 `[x]/[!]`，runtime 写入最终 receipt 后刷新 canonical `plan.md`；GUI 继续读取同一 receipt。普通 Agent 行为不变。
- 2026-07-15 GBX-122 dependency matrix projection：canonical `plan.md` 新增 OMO 风格依赖矩阵，直接从 PlanGraph task dependencies/parallel wave 生成；Gear GUI 已从同一 PlanGraph 显示工单 dependencies/wave，不新增状态副本。普通 Agent 行为不变。
- 2026-07-15 GBX-123 milestone projection：canonical `plan.md` 新增按 parallel wave 聚合的 `## Milestones`，Gear GUI lifecycle/thread view 从同一 PlanGraph 投影 bounded milestone 摘要；最终 F1-F4 波次也纳入同一视图。普通 Agent 行为不变。
- 2026-07-15 GBX-124 acceptance checklist projection：canonical `plan.md` 新增从每个 task completion predicate 与顶层 final acceptance 聚合的 `## Acceptance checklist`；Gear GUI lifecycle/thread view 投影同一有界清单，避免只显示分散工单验收。普通 Agent 行为不变。
- 2026-07-15 GBX-125 acceptance evidence status projection：`product.rs`、`gui.rs` 从 `PlanNodeRun.criterion_evidence` 投影验收清单 `[ ]/[x]/[!]`；最终 acceptance 同时绑定所有 criterion receipts 与 `FinalVerificationWaveReceipt`，runtime 写 evidence/receipt 后 canonical plan 与 GUI 均反映同一结果。普通 Agent 行为不变。
- 2026-07-15 GBX-126 commit boundary execution brief：runtime 的 work-order routing brief 和 preflight artifact 现在显式记录 `PlanTaskContract.commit_boundary`；不自动 commit/push，worker 仍按计划承担提交或保持 no-commit，GUI 继续显示同一边界。普通 Agent 行为不变。
- 2026-07-15 GBX-127 commit boundary evidence gate：`PlanNodeRun` 新增兼容旧 ledger 的 commit-boundary evidence/status；runtime 捕获 dispatch 前后 HEAD，对 `NoCommit`/`AfterTask`/`AfterWave` 生成 evidence，并在 completion gate 拒绝明确失败的边界；GUI 显示 evidence path 与 satisfied 状态。Gear 仍不自动 commit/push。普通 Agent 行为不变。
- 2026-07-15 GBX-128 plan commit evidence projection：`product.rs` 的每个 canonical 工单详情现在同时显示 commit boundary evidence marker/path/status；runtime ledger、`.gear/plan.md` 和 GUI 使用同一 commit evidence。普通 Agent 行为不变。
- 2026-07-15 GBX-129 rollback plan section：canonical `plan.md` 将 rollback actions 单独投影为 `## Rollback Plan`；Gear GUI 已从同一 durable plan/rollback artifacts 显示 rollback actions 与 pending confirmation。普通 Agent 行为不变。
- 2026-07-15 GBX-130 plan artifact freshness projection：Gear GUI 对 `.gear/plan.md` 读取并校验当前 PlanGraph.plan_hash，投影 `current/stale/invalid/missing` 状态；thread view 显示该状态，避免只因文件存在就误报计划与 runtime 同步。普通 Agent 行为不变。
- 2026-07-15 GBX-131 QA scenario evidence gate：`PlanNodeRun` 复用 attempt-bound criterion evidence 记录每个 happy/failure/adversarial QA scenario 的 `qa:<kind>:<name>` receipt；runtime 仅在声明 evidence path 对应 workspace artifact 存在时标记 Pass，缺失时写入 Blocked/Fail marker，并在工单 completion gate 中要求全部 QA scenario 通过。canonical `plan.md` 与 Gear GUI acceptance checklist 显示场景级 `[ ]/[x]/[!]` 状态。普通 Agent 行为不变。
- 2026-07-15 GBX-132 QA 状态与 planner 提示同步：Gear GUI 的选中工单详情现在显示每个 QA 场景的 `[ ]/[x]/[!]`、类别和 evidence path；Gear OpenCode planner 与 Zed Agent planner prompt 都明确要求 happy/failure/adversarial QA，adversarial 不适用时必须留下 not-applicable trigger check。普通 Agent 的非 Gear planner 行为不变。
- 2026-07-15 GBX-133 broker lifecycle GUI projection：`crates/gearbox_agent/src/gui.rs` 有界扫描 `.gear/broker-sessions` 的 durable `session-identity.json`/`terminal-outcome.json`，把 active/terminal phase session 状态投影到 `GearRuntimeLifecycle`；`crates/agent_ui/src/conversation_view/thread_view.rs` 在 Work Orders 面板显示 broker session lifecycle。状态只读自 runtime ledger，不创建 GUI 副本。普通 Agent UI 不变。
- 2026-07-16 GBX-134 broker lifecycle goal isolation：broker session GUI projection 现在按当前 `Goal.id` 过滤 `.gear/broker-sessions`，避免多目标长会话把其他目标的 worker lifecycle 混入当前 Work Orders 面板；无 goal 时不构造目标摘要。普通 Agent UI 不变。
- 2026-07-16 GBX-135 OMO role projection：canonical `plan.md`、`GearRuntimePlanTaskSummary` 和 Gear thread view 从 `preferred_phase_profile` 推导并显示 OMO 风格 `build/review` Role；不新增计划字段或 GUI 状态副本。普通 Agent UI 行为不变。
- 2026-07-16 GBX-136 commit intent projection：`PlanTaskContract` 新增向后兼容的可选 `commit_message`，planner prompt、canonical plan、routing brief/preflight artifact、Gear snapshot 和 thread view 均显示该 OMO-style commit intent；空消息被拒绝，Gear 仍不自动 commit/push。普通 Agent 行为不变。
- 2026-07-16 GBX-137 required artifact final gate：runtime final PlanCompliance verification 现在检查每个 required task artifact（延后 runtime 自己在 final wave 后生成的收尾文件），缺失时 final receipt 失败并记录 missing artifact evidence；Gear GUI selected work order 将 required artifact 从静态路径投影为 `[ ]/[x]` 文件存在状态。普通 Agent 行为不变。
### GBX-138 — OMO `Blocked by` dependency projection

- `crates/gearbox_agent/src/product.rs` now renders the existing task dependency list as both `Dependencies` and the OMO-compatible `Blocked by` label.
- `crates/agent_ui/src/conversation_view/thread_view.rs` exposes the same dependency list in task rows and selected-task details, without introducing a second state source.
- This is a presentation alignment only; scheduling and completion continue to read `PlanTaskContract.dependencies`.
### GBX-139 — OMO ordered execution steps in plan contracts

- `crates/gearbox_agent/src/plan_graph.rs` adds typed `execution_steps` with stable IDs, expected observations, optional evidence paths, legacy fallback, validation, and worker stop/constraint instructions.
- `crates/gearbox_agent/src/product.rs` and `crates/gearbox_agent/src/gui.rs` project the ordered steps into canonical plan output and runtime GUI snapshots; `crates/agent_ui/src/conversation_view/thread_view.rs` displays them for the selected task.
- Planner prompts in `crates/agent/src/agent.rs` and `crates/gearbox_agent/src/open_code_phase_runtime.rs` require step-complete JSON and prohibit skipping/reordering; Gear still delegates execution and never commits automatically.
### GBX-140 — execution step lifecycle ledger and GUI projection

- `crates/gearbox_agent/src/state.rs` adds durable `PlanStepRun` records under each `PlanNodeRun`, with pending/running/completed/blocked states and resume reset semantics; old ledgers remain readable through serde defaults.
- `crates/gearbox_agent/src/runtime.rs` updates the step ledger at worker start, failure, completion-gate failure, successful completion, and continuation requeue. The node ledger remains the single source of truth.
- `crates/gearbox_agent/src/gui.rs` and `crates/agent_ui/src/conversation_view/thread_view.rs` project step status, evidence, and blocking errors into the runtime GUI.
### GBX-141 — worker step evidence receipt gate

- `crates/gearbox_agent/src/workers.rs` parses `completed_steps` and `step_evidence` sections from the worker's durable output and advertises them in the worker report contract.
- `crates/gearbox_agent/src/plan_graph.rs` adds `execution_steps_evidence_required`; new planner contracts opt into explicit step receipts while legacy deterministic plans remain readable and stage-compatible.
- `crates/gearbox_agent/src/runtime.rs` rejects a strict worker completion with missing, unknown, or incomplete step IDs, persists evidence paths, and includes all step receipts in the completion gate.
- `crates/gearbox_agent/src/gui.rs` and `crates/agent_ui/src/conversation_view/thread_view.rs` show step evidence paths and errors from the durable ledger.
### GBX-142 — WORK_ORDER_EVIDENCE 路径与质量分类投影

- `crates/gearbox_agent/src/state.rs` extends `PlanNodeRun` with worker result/outcome/last-message paths and `WorkerEvidenceQuality` (`proved`, `fixture_only`, `blocked_not_verified`, `failed`).
- `crates/gearbox_agent/src/runtime.rs` records those paths at worker settle and upgrades quality only after the full completion gate; failed or incomplete evidence remains visibly unverified.
- The same node record now keeps worker `changed_files`, `commands_run`, and `known_failures`, matching OMO's exact evidence report without trusting summary prose.
- `crates/gearbox_agent/src/gui.rs` and `crates/agent_ui/src/conversation_view/thread_view.rs` project the same paths and classification into task rows and selected-task details.

### GBX-143 — OMO 工单契约字段补齐

- `crates/gearbox_agent/src/plan_graph.rs` 为 Gear 专属 `PlanTaskContract` 增加向后兼容的 `inputs`、`preconditions`、`evidence`、`rollback` 和 `budget` 字段，并将它们投影为 worker 约束；旧计划缺省为空仍可读取。
- `crates/gearbox_agent/src/open_code_phase_runtime.rs` 与共享 `crates/agent/src/agent.rs` 的 Gear planner 提示同步要求这些字段，避免把 OMO 工单级证据、恢复和资源边界隐藏在 `must_do` 或 prose 中。
- 这是 Gear 计划协议增强，不改变上游 Agent 的默认 planner 行为；共享提示修改仅在 Gear planner 路径生效。

### GBX-144 — OMO 资源门禁的只读健康快照

- `crates/gearbox_agent/src/gui.rs` 增加兼容旧快照的进程健康投影，读取 Linux `/proc/*/comm` 中的 `cargo`、`rustc`、`rust-analyzer`、`opencode`、`codex` 计数，并标记 Rust 进程是否超过两项；不会由 Gear 自动杀进程或把观察结果升级为硬门禁。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 显示同一 runtime 快照中的进程计数和超限提示，避免 GUI 只显示 TaskManager 子任务数而遗漏外部 analyzer/build 进程。
- 这是 Gearbox GUI 隔离层的资源可观测性修改；普通 Zed Agent UI 不改变。

### GBX-145 — OMO 工单级计划上下文 GUI/Canonical 投影

- `crates/gearbox_agent/src/product.rs` 将 task-level `inputs`、`preconditions`、`evidence`、`rollback`、`budget` 写入 canonical `plan.md`，不再只保留在 JSON worker contract 中。
- `crates/gearbox_agent/src/gui.rs` 从同一个 `PlanTaskContract` 投影这些字段；`crates/agent_ui/src/conversation_view/thread_view.rs` 在选中工单中显示它们，保持 runtime、canonical plan 与 GUI 同源。
- 共享 UI 仅增加 Gear 快照字段的显示，不改变普通 Zed Agent UI。

### GBX-146 — OMO 工单 preflight durable gate

- `crates/gearbox_agent/src/state.rs` 的 `PlanNodeRun` 现在持久化 `preflight_path` 与 `preflight_satisfied`，旧 ledger 缺省为未满足。
- `crates/gearbox_agent/src/runtime.rs` 在 worker dispatch 前写入并保存工单 preflight receipt；completion gate 要求该 receipt 存在且满足，避免只写文件但状态不可恢复。
- `crates/gearbox_agent/src/gui.rs` 优先从 durable node ledger 投影 preflight 状态，GUI 不再依赖目录扫描推断状态。

### GBX-147 — OMO preflight 四项检查结构化门禁

- `crates/gearbox_agent/src/state.rs` 增加 `PlanPreflightCheck`，`PlanNodeRun` 持久化 `scope_check`、`forbidden_check`、`dependency_check`、`acceptance_check`、`validation_check` 结果。
- `crates/gearbox_agent/src/runtime.rs` 在 worker dispatch 前计算这些检查，任一失败即停止 dispatch；preflight artifact 同时记录逐项结果，completion gate 继续要求整组 receipt。
- `crates/gearbox_agent/src/gui.rs` 与 `crates/agent_ui/src/conversation_view/thread_view.rs` 从同一 ledger 显示逐项检查，避免 GUI 只显示一个笼统的 preflight 标记。

### GBX-148 — OMO 工单实际路由回执投影

- `crates/gearbox_agent/src/gui.rs` 对每个工单读取已校验的 `TaskRouteDecisionReceipt`，投影实际 worker、model 和 route hint；旧快照字段通过 serde 默认兼容。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 在工单行显示 actual route，避免只显示全局 phase route 而无法逐工单审计 fallback。
- Gear 仍以 durable route receipt 为实际事实，不把计划中的 phase profile 或 UI 标签冒充实际模型。

### GBX-149 — OMO preflight attempt 隔离

- `PlanNodeRunLedger::requeue_failed_for_resume` 在新 continuation attempt 前清除旧 `preflight_path`、`preflight_satisfied` 与逐项检查，保留失败 worker identity/error 供审计。
- 下一个 attempt 必须重新执行并持久化 preflight，避免旧 receipt 在恢复后被 completion gate 或 GUI 误认为当前尝试证据。

### GBX-150 — 严格 planner 工单上下文校验

- `crates/gearbox_agent/src/plan_graph.rs` 对启用 `execution_steps_evidence_required` 的新 planner 工单强制要求非空 `inputs`、`preconditions`、`evidence`、`rollback`。
- 旧 deterministic/legacy planner 计划仍按 serde 默认值读取；只有新严格契约触发门禁，避免兼容性破坏。

### GBX-151 — 独立 review bundle GUI 投影

- `crates/gearbox_agent/src/gui.rs` 从 durable `ReviewEpochBundle.roles` 投影 reviewer role、execution/session identity 与 receipt path，不再只显示 bundle 是否 complete。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 显示每个 review role 的证据摘要，保持 OMO 独立审查链与 GUI 同源。

### GBX-152 — WORK_ORDER_EVIDENCE 后继与 plan gap 投影

- `crates/gearbox_agent/src/workers.rs` 解析 worker `next_steps` 与 `plan_gap` 段落，并把它们加入明确的 continuation evidence contract。
- `crates/gearbox_agent/src/state.rs`、`runtime.rs`、`gui.rs` 和 `crates/agent_ui/src/conversation_view/thread_view.rs` 持久化并显示这些后继信息；plan gap 不会被 summary 文本吞掉。

### GBX-153 — OMO 工单 execution decision 与 skip reason

- `PlanNodeRun` 新增 `PlanWorkOrderDecision` 与 `worker_decision_reason`，明确区分 executed、skipped、blocked、not_recorded。
- runtime 从 worker terminal status 和 plan gap/known failure 写入决策原因；skipped 没有原因时 ledger validation 失败，不允许无理由跳过。
- Gear GUI 显示决策和原因；skipped 不会被映射为已验证完成。

### GBX-154 — OMO 依赖阻塞状态 canonical 计划投影

- `crates/gearbox_agent/src/product.rs` 的 canonical `plan.md` 现在将 `PlanNodeRunLedger` 中的依赖状态投影到 `Blocked by`，不再重复打印静态 dependencies；缺失 ledger 记录显示为 `not_recorded`。
- 这是 Gear 专属计划文档投影，不改变上游共享计划或普通 Zed UI。

### GBX-155 — PlannerModel 强制逐工单步骤证据

- `crates/gearbox_agent/src/plan_graph.rs` 现在对带 session-bound planner receipt 的新 `PlannerModel` 工单拒绝未启用 `execution_steps_evidence_required` 的计划，避免新模型通过关闭字段绕过 OMO 式逐步执行和证据回执；sessionless coordinator brief 与 `DeterministicFallback` 保留旧计划兼容路径。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 的 Gear 任务行和选中任务详情同步显示依赖实际状态及 `STEP EVIDENCE REQUIRED`，避免 GUI 将弱计划显示成可执行计划。

### GBX-156 — OMO TODO 状态的 canonical 计划投影

- `crates/gearbox_agent/src/product.rs` 根据 `PlanNodeRunStatus` 渲染 `must_do` 的 `[ ]`、`[~]`、`[x]`、`[!]` 标记，避免 runtime 已完成而 `plan.md` 仍显示全部未完成。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 在选中工单的 `MUST` 条目上显示同一任务状态，保持 canonical 计划和 GUI 使用同一 ledger 语义。
- 这是 ledger 到 Gear canonical 文档的状态投影；逐步骤状态仍以 `PlanStepRun` 为权威，不新增第二套可写状态。

### GBX-157 — OMO 串行工单调度审计

- 审计确认新 PlanGraph runtime 使用 `runnable_wave(..., 1)`，只有当前工单通过 terminal completion gate 后才加入 `completed_plan_tasks`，因此不会批量跳过工单或并行执行同一 wave。
- GUI 的 `serial_work_orders`、完成数、当前工单和 next 工单来自同一 runtime snapshot；旧并行 helper 无调用点，不代表当前执行模式。

### GBX-158 — PlanCritic 审批链 GUI 投影

- `crates/agent_ui/src/conversation_view/thread_view.rs` 显示 runtime snapshot 中的计划审批状态、修订次数、Critic receipt 短 hash 和最终验证 receipt 状态。
- 共享 UI 只消费 Gear snapshot，不改变上游审批逻辑；GUI 不从临时目录自行推断审批状态。

### GBX-159 — Worker 反馈摘要 GUI 投影

- `crates/gearbox_agent/src/gui.rs` 从 durable `worker_last_message_path` 读取最多 1200 字节的 `worker_last_message_excerpt`，并保持原始路径与 ledger 证据不变。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 显示实际 `WORKER FEEDBACK`，同时保留 next steps、plan gap 和 known failures；受限摘要避免长对话将全文复制进 GUI snapshot。

### GBX-160 — Worker 路由与模型事实审计

- 审计确认每个工单的实际 worker/model 来自 durable `TaskRouteDecisionReceipt`，GUI 显示 `actual-route`，不会使用计划 profile 冒充实际模型。
- lifecycle 同时投影当前 worker kind/model、intensity、phase route receipts 和 route errors；本轮未发现需要改变路由事实的缺口。

### GBX-161 — 最终验收与下一目标 GUI 审计

- 审计确认 GUI 已显示 final receipt 状态/hash/artifact、逐项 final checks、stop reason、Next goal 的 objective/questions/signals/evidence refs。
- 这些值全部来自 `GearRuntimeSnapshot`；本轮未发现需要修改的 runtime/GUI 状态偏差。

### GBX-162 — Runtime/GUI 持续刷新审计

- 审计确认 `agent/src/agent.rs` 为 Gear session 启动 200ms snapshot loop，从 durable `.gear` state 和 live TaskManager 重建快照并 `cx.notify()`；ledger 变化不会因事件 sequence 不变而停留在旧 GUI。
- snapshot 错误通过 `gear_runtime_snapshot_error` 投影到 GUI；本轮未发现 stale snapshot 缺口。

### GBX-163 — Execution ownership durable/GUI 投影

- runtime 在 durable `PhaseRouteSelected` event 中记录 delegated、worker kind/task、route reason；ownership gate 仍由 runtime 负责。
- `crates/gearbox_agent/src/gui.rs` 与 `crates/agent_ui/src/conversation_view/thread_view.rs` 投影 execution ownership，明确 Gear 是否把实现委派给 worker，避免 GUI 无法审计 direct-edit 边界。

### GBX-164 — OMO 增量工单上下文与 Gear 计划投影

- `PlanTaskContract` 增加 `already_in_working_tree` 与 `still_needed`，保留 OMO 工单中“已有内容/剩余工作”的增量语义，同时继续以 typed PlanGraph JSON 为唯一真源。
- planner prompt 明确要求一个工单对应一个可独立验证的目标；canonical `plan.md` 与 Gear GUI 同步显示两类上下文，避免 worker 把已完成内容重复实现或跳过剩余步骤。
- 该字段为 serde 默认兼容字段，不改变上游共享源码；GUI 只投影 runtime snapshot，不建立第二套计划状态。

### GBX-165 — PlanCritic/Revision 增量语义审查

- `crates/gearbox_agent/src/open_code_phase_runtime.rs` 的 PlanCritic、Oracle 和 Revision prompts 现在明确检查 `already_in_working_tree`、`still_needed` 与 must_do/steps/artifacts/completion predicates 的覆盖关系。
- 审查要求每个工单保持一个可独立验证目标，但将文件边界作为证据和风险，而不是精准硬门禁；Revision 不得通过泛化 must_do 或扩大范围隐藏遗漏工作。
- 该行为只增强 Gearbox 专属 phase prompt，不修改上游共享源码；GUI 继续读取同一 PlanGraph snapshot。

### GBX-166 — 下一目标验收信号与证据引用 GUI 投影

- `crates/agent_ui/src/conversation_view/thread_view.rs` 在 Gear 的 Next goal 区域显示 strategist receipt 中的 `acceptance_signals` 与 `evidence_refs`。
- 这些值与 `next_objective`、`required_questions` 一样来自 durable `strategist-next-goal-receipt.json` 的 runtime snapshot，避免 GUI 只显示下一目标文字而丢失目标完成依据。
- 这是 Gear 条件界面投影，不改变上游 agent 语义或计划状态。

### GBX-167 — TaskManager 操作结果 durable/GUI 投影

- `crates/gearbox_agent/src/task_manager.rs` 从 bounded `task-command-events.jsonl` 投影最近一次 command 的 action、accepted、reason、epoch 和 timestamp 到 `TaskSnapshot.last_command`。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 在任务管理器中显示最近一次 command 的结果和原因，区分 accepted/rejected；GUI 不再把 messageability 当成 command 已生效的证明。
- 读取只保留有限尾部，避免长期 follow-up/steer 事件使 snapshot 或 GUI 内存增长。

### GBX-168 — PlanCritic findings/revision instructions GUI 投影

- `crates/gearbox_agent/src/state.rs` 增加按 goal/revision 读取持久化 `PlanCriticReceipt` 的 bounded API；`crates/gearbox_agent/src/gui.rs` 将最多 8 条 findings（单条最多 600 字符）和 revision instructions（最多 1200 字符）投影到 `GearRuntimeReviewSummary`。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 在 review detail 中显示审查发现与修订指令，使 GUI 能审计 OMO 式“计划—审查—修订”闭环，而不是只显示 Approved/Rejected 状态。
- 同时补齐 durable TaskManager snapshot 的 `continuation_hint` 字段初始化，修复本轮 `cargo check` 暴露的结构体初始化错误。
- 以上均为 Gearbox 专属 runtime/条件 GUI 层，不改变上游 agent 默认行为。

### GBX-169 — OMO 工单 WHY/HOW 结构化对齐

- `crates/gearbox_agent/src/plan_graph.rs` 的 `PlanTaskContract` 增加 `rationale`（WHY）和有序 `approach`（HOW）；session-bound PlannerModel 计划必须提供两者，避免把动机、方案和 must_do 混成一段泛化描述。
- `crates/gearbox_agent/src/open_code_phase_runtime.rs` 的 Planner、PlanCritic、Oracle、Revision prompts 都把 WHY/HOW 纳入生成、审查和修订语义；worker goal/constraints 也收到同一份上下文。
- `crates/gearbox_agent/src/product.rs` 的 canonical `plan.md`、`crates/gearbox_agent/src/gui.rs` 与 `crates/agent_ui/src/conversation_view/thread_view.rs` 同步显示 WHY/HOW，保持 PlanGraph、Markdown、runtime snapshot、GUI 同源。
- 这是 OMO 计划格式的增强性对齐，不改变文件边界软约束或 runtime 的证据门禁；旧持久化计划通过 serde 默认字段继续可读，只有新 session-bound planner 计划要求完整字段。

### GBX-170 — Planner draft strict validation 对齐

- 修复 `validate_planner_draft` 使用 sessionless `PlannerReceipt` 导致新 planner draft 绕过 WHY/HOW 严格校验的问题；planner 输出验证现在显式使用 session-bound validation receipt。
- 旧 sessionless 持久化计划仍可读取；只有新 planner 输出、repair 输出和 artifact recovery 必须满足 rationale/approach，避免模型生成不完整计划后才在更晚阶段失败。
- 增加 worker prompt、canonical plan 渲染和 strict validation 回归断言；GUI 继续使用同一 PlanTaskContract 投影。

### GBX-171 — 真实 build_plan_graph 入口严格校验

- `crates/gearbox_agent/src/runtime.rs::build_plan_graph` 在解析 coordinator brief 后显式调用 `validate_planner_draft`，修复实际 legacy planner 入口使用 sessionless `PlannerReceipt` 而绕过新工单契约的问题。
- `PLAN_GRAPH_SCHEMA_EXEMPLAR` 补齐 rationale/approach；OpenCode artifact recovery、schema repair、repository discovery/planner 测试均继续通过。
- 仅更新 Gearbox runtime 测试夹具以声明新 planner 工单的 step evidence；不放宽严格校验。

### GBX-172 — 独立 Oracle 审查反馈 GUI 投影

- `crates/gearbox_agent/src/state.rs` 增加有界 `read_plan_oracle_receipt`，读取与当前 goal/revision 绑定的持久化 Oracle receipt。
- `crates/gearbox_agent/src/gui.rs` 将 Oracle findings 和 revision instructions 投影到 `GearRuntimeReviewSummary`，与主 PlanCritic 反馈分开保留。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 显示 Oracle findings/修订指令；读取只保留最多 8 条、每条 600 字符、指令 1200 字符，避免长审查输出拖垮 GUI。
- Oracle receipt 原本已参与 runtime approval/complete 门禁，本轮只补齐用户可见的同源投影，不改变审查判定策略。

### GBX-173 — 多审查者 decision GUI 投影

- `GearRuntimeReviewSummary` 增加 `critic_decision` 与 `oracle_decision`，从各自 typed receipt 直接投影；GUI review detail 不再要求用户通过 receipt hash 推断审查结论。
- decision 与 findings/instructions 分字段保持 durable review 语义，仍受 bounded snapshot 约束；不改变 runtime approval gate。

### GBX-174 — Review receipt PlanGraph binding GUI 校验

- `crates/gearbox_agent/src/gui.rs` 在投影 Critic/Oracle receipt 前同时校验 `plan_id` 和 `plan_hash` 与当前 PlanGraph，避免同一 goal/revision 下残留的旧 receipt 污染 GUI。
- 不匹配的 receipt 被视为不可投影；runtime 原有 approval/receipt 验证仍是最终门禁，GUI 不自行批准计划。

### GBX-175 — Unreviewed PlanGraph planning GUI 投影

- `crates/gearbox_agent/src/gui.rs` 新增 `visible_plan`：GUI 计划列表优先读取 canonical PlanGraph，批准前没有 canonical 时回退到 durable `.unreviewed.plan.json`。
- approval、review receipt 和 final verification 仍只使用 canonical PlanGraph；本轮只让 planning 阶段的 TODO/工单可见，不改变任何执行或批准门禁。
- 增加 unreviewed graph GUI projection 回归测试，证明 runtime durable planning state 能在批准前显示。

### GBX-176 — Unreviewed revision 版本选择 GUI 对齐

- `visible_plan` 在 canonical 与 unreviewed graph 同时存在时选择更高 revision；同 revision 仍 canonical 优先。
- 这样 PlanCritic revision 期间 GUI 能显示最新候选 TODO，而 approval、review receipt 和 final verification 继续只绑定 canonical graph。
- 增加 newer-unreviewed revision 回归测试，保持 runtime 与 GUI 的计划版本语义一致。

### GBX-177 — Visible plan provenance GUI 对齐

- `GearRuntimeLifecycle` 增加 visible plan revision/source/candidate 标记；当 GUI 显示 unreviewed revision 时明确标注 candidate，避免与旧 canonical approval 状态混淆。
- approval、review receipt 和 final verification 仍使用 canonical graph；provenance 只解释 GUI 当前显示内容，不改变门禁。

### GBX-178 — Work-order goal/deliverable GUI 投影

- `GearRuntimePlanTaskSummary` 增加 bounded `goal` 与 `deliverable`，从 PlanTaskContract 投影到 GUI 选中工单详情。
- GUI 现在同时显示 GOAL、DELIVERABLE、WHY、HOW、步骤和证据，完整呈现 OMO 工单的执行意图；不新增第二套计划状态。

### GBX-179 — Candidate/approval lifecycle 语义对齐

- `crates/gearbox_agent/src/gui.rs` 的 `plan_approval_summary` 改为始终绑定 canonical PlanGraph；visible unreviewed candidate 由 provenance 单独标注，不再把旧 approval 误报为 stale。
- GUI 同时表达“canonical approval 状态”和“当前可见 candidate revision”，保持 OMO 计划修订中的审查语义清晰。

### GBX-180 — Candidate execution ledger 绑定 GUI 对齐

- `crates/gearbox_agent/src/gui.rs` 仅在 `PlanNodeRunLedger.plan_id/plan_hash` 与当前 visible PlanGraph 一致时投影执行状态；显示新 unreviewed revision 时不再复用旧 revision 的 Completed/Running 状态。
- 旧 ledger 仍保留在 durable state，等待对应 canonical/visible plan；GUI 不删除、不重写执行证据。

### GBX-181 — Parent session provenance GUI 投影

- `TaskSnapshot` 增加 durable `parent_session_id`，live/durable snapshot 均从 TaskRecord 投影。
- Agent UI task manager 显示 parent session，补齐 OMO completion notification/continuation 回到哪个父对话的可审计信息；不改变 notifier 调度。

### GBX-182 — Completion notification epoch GUI 投影

- `TaskSnapshot` 增加 durable `notification_failed_epoch`，live/durable snapshot 均从 `TaskRecord` 投影，GUI 可区分 pending、notified 与 failed completion notification。
- Agent UI task manager 在任务头部显示通知状态与 epoch；失败状态来自 runtime 已记录的失败 epoch，不改变 `CompletionNotifier` 的调度、重试或持久化语义。

### GBX-183 — OMO 计划视图与生成回执对齐

- `crates/gearbox_agent/src/product.rs` 的 PlanGraph Markdown 视图新增计划生成回执（provider/model/session）与明确的 work-order protocol，直接呈现 OMO 的“单工单、严格步骤、证据后推进、失败回审”语义。
- 仍以结构化 PlanGraph 为唯一真实源；Markdown、GUI 对话和 runtime 执行状态均从同一 graph/ledger 投影，不引入第二套计划状态。

### GBX-184 — Ordered execution step prefix gate

- `PlanNodeRun::apply_worker_step_evidence` 现在拒绝报告后续步骤而未完成前置步骤的 worker 证据；已完成步骤可在后续 attempt 中继续提交，保持可恢复性。
- runtime 将该错误写入 `PlanNodeRun` 的 failed/error 与 step lifecycle，现有 GUI execution step projection 会同步显示阻塞步骤；不改变计划 schema 或 worker 路由。

### GBX-185 — Worker step receipt prompt 对齐

- `workers.rs` 的 worker packet 明确要求 `completed_steps` 只能是连续前缀，禁止跳过早期步骤；与 runtime 的 prefix gate 保持同一契约。
- 该提示只强化执行者协议，实际状态仍由 runtime 验证，失败会沿既有 PlanNodeRun/GUI projection 路径持久化。

### GBX-186 — Step contract failure event audit

- `WorkerFinished` event payload 现在同时记录 `step_evidence_error` 与 `plan_contract_status`，区分 worker 进程成功和计划契约接受，避免 durable event stream 把跳步证据误读为工单成功。
- PlanNode ledger 与 GUI 仍是状态真源；本轮只补充事件审计字段，不改变 worker 重试/continuation 行为。

### GBX-187 — Plan contract status GUI projection

- `GearRuntimePlanTaskSummary` 增加 `contract_status`，从 `PlanNodeRunStatus` 投影 `pending/accepted/failed`；Agent UI 在工单摘要中同时显示 worker status 与 contract status。
- 这样 worker 进程成功但 step/证据契约失败时，GUI 不再只显示笼统 worker 状态；不新增第二套状态机，仍以 PlanNode ledger 为真源。

### GBX-188 — Blocked contract status semantics

- `contract_status` 将 `NeedsUser/Cancelled` 投影为 `blocked`，与 `Failed` 区分；GUI 不把等待用户或取消误报为契约失败。
- 状态仍由 `PlanNodeRunStatus` 单向投影，兼容旧 GUI payload 的 serde default。

### GBX-189 — Visible plan review revision 对齐

- `GearRuntimePlanTaskSummary` 只使用与 visible PlanGraph 的 `plan_id/plan_hash` 匹配的 `PlanNodeRunLedger`；candidate revision 不再复用旧版本的工单状态、worker session 或 attempt。
- `GearRuntimeReviewSummary` 同样绑定 visible PlanGraph 的 revision/hash，candidate 计划显示自身的 critic/oracle 结果；canonical approval 与 final verification 仍保持 canonical 语义。

### GBX-190 — Candidate final verification GUI 隔离

- visible plan 是 unreviewed candidate 时，GUI 不再投影 canonical PlanGraph 的 final-verification receipt/checks；candidate 只显示自身的验证要求，避免把旧版本证据误报为新版本已验证。
- visible plan 与 canonical graph 一致时保持原有 approval/final-verification 投影；不新增 GUI 状态副本。

### GBX-191 — Strategist next-goal receipt GUI 对齐

- GUI 对带 schema 的 `StrategistNextGoalReceipt` 校验 receipt hash 以及当前 goal/epoch/status 绑定；无效 typed receipt 不进入 snapshot。
- 无 schema 的旧 receipt 保留兼容读取路径；下一目标决策仍以 runtime 持久化 receipt 为唯一来源，不创建 GUI 状态副本。

### GBX-192 — OMO 计划覆盖率 GUI 投影

- `crates/gearbox_agent/src/gui.rs` 从当前 visible `PlanGraph` 与匹配的 `PlanNodeRunLedger` 计算工单、完成谓词和 QA 场景覆盖率；没有当前 attempt 的证据时不计为满足。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 在 Work Orders 面板显示覆盖率，继续只消费 runtime snapshot，不维护 GUI 计划状态副本。
- 共享 UI 改动仅增加 Gearbox runtime 面板字段；上游普通 Agent 对话路径不变。

### GBX-193 — OMO 计划 revision diff GUI 投影

- GUI 从 `.gear/plans/revision-*.plan.json` 读取当前 visible revision 的相邻候选，只比较已持久化且通过 PlanGraph 校验的任务和 objective。
- `GearRuntimePlanRevisionDiff` 仅投影 added/removed/changed task id 与 objective 是否变化；不改变 canonical plan、approval 或 runtime 调度。
- Work Orders 面板显示 revision 变化摘要；仍以 runtime snapshot 为唯一 GUI 来源。

### GBX-194 — Live PlanCritic repair observation path 边界

- 真实 OpenCode 单 epoch smoke 暴露 PlanCritic repair 的 repository-observation atomic 临时文件名超过 `NAME_MAX`；task/session 组件改为 32 字节有界并保留旧 64 字节路径读取兼容。
- 共享状态路径策略只影响 Gearbox observation artifact；上游普通 Agent 路径不变。

### GBX-195 — PlanCritic typed schema repair prompt

- 真实 Hy3 PlanCritic 输出采用 OMO 风格的顶层 `verdict/status/findings.evidence_refs`，导致严格 Gear receipt repair 失败；初始 PlanCritic、Oracle 和 repair prompt 现在都嵌入完整 typed verdict/finding skeleton。
- 明确 `evidence_refs` 只能位于 check 内，finding 必须包含 `dimension/severity/code/task_id/path/message/required_change`；不放宽 `deny_unknown_fields`，继续 fail closed。

### GBX-196 — Review observation blocker GUI projection

- `GearRuntimeReviewSummary` 从 durable `ReviewEpochBundle` 的 observation receipt 投影不可用、无效或 `Unverified` 阻断原因及路径；不把缺少 repository tool evidence 的审查显示成普通 pending。
- Agent UI Work Orders 面板显示 `Review blocker`，保留 runtime 的 fail-closed approval gate；只增加解释性投影，不放宽 observation 验证。

### GBX-197 — OMO Todo session lifecycle GUI projection

- OMO `boulder.json` 为每个 Todo 持久化 agent、session、started/ended/status；Gear 的 `PlanNodeSessionBinding` 已有同等 durable 生命周期，但之前 GUI 只显示 session ID。
- `GearRuntimePlanTaskSummary` 现在投影 binding 的 `status`、`created_at`（started）和 `updated_at`（terminal/last update），Agent UI Work Orders 卡片同步显示这些字段。
- 仍以 PlanNodeSessionBinding 为 runtime 真源；没有创建 GUI 私有 session 状态，也不改变 worker 生命周期或恢复语义。
- 对终止或 superseded binding，GUI 额外派生 `ended_at` 与 `elapsed_ms`；活跃 binding 的 elapsed 从 created_at 计算到当前 snapshot 时间，不能伪造终止时间。

### GBX-198 — OMO Todo session history GUI projection

- OMO `boulder.json.task_sessions` 保留同一 Todo 的多次 session/agent/category 记录；Gear GUI 之前只读取当前 attempt 的 binding。
- `GearRuntimePlanTaskSummary.worker_session_history` 现在从同一 PlanNode 的 bounded durable bindings（最多 8 次 attempt）投影 session、worker kind/model、状态、时间和 elapsed；当前 session 字段继续指向最新 attempt。
- Work Orders 选中工单时显示 session history；历史列表只读 runtime artifacts，不维护 GUI 私有状态，也不改变重试/恢复调度。

### GBX-199 — OMO Todo session aggregate GUI projection

- 从 GBX-198 的 bounded session history 派生 attempt 总数、route 变化 fallback 次数和累计 elapsed；不重复持久化或把 GUI 计算结果用于预算/计费。
- Work Orders 选中工单时显示 session aggregate，fallback 仅按相邻 binding 的 worker kind/model 变化计数，普通同 route retry 不会被误报为 fallback。

### GBX-200 — OMO Todo category and fallback reason GUI projection

- `TaskRouteDecisionReceipt` 已持久化 `worker_category`、`route_reason` 和每次 route 的 `fallback_count`；之前 session history 只显示 worker kind/model，丢失 OMO 的 agent/category/fallback 解释。
- session history 现在绑定并校验对应 route receipt 的 plan identity/hash，再投影 category、route reason 和 receipt fallback count；没有匹配 receipt 时不猜测、不伪造。
- aggregate 优先使用 receipt fallback count；只有全历史缺少 route metadata 时才使用 worker kind/model 变化作为 bounded 兼容推断。

### GBX-202 — OMO current execution step GUI projection

- `crates/agent_ui/src/conversation_view/thread_view.rs` 从 runtime snapshot 的 durable `execution_steps` 派生工单摘要中的 `current-step`；第一条非 `Completed` 步骤作为当前步骤，全部完成时显示 complete。
- 不新增 GUI 状态副本、不改变 PlanNode 调度；上游普通对话路径不受影响，Gearbox Work Orders 继续只读 runtime 投影。

### GBX-203 — OMO strict step evidence worker gate

- `crates/gearbox_agent/src/runtime.rs` 对声明 `execution_steps_evidence_required` 的计划统一要求 delegated worker；fixture/non-delegated 执行不再绕过 `completed_steps` 证据而进入 `GreenVerified`。
- 普通兼容计划继续允许 deterministic fallback；严格计划失败原因写入 PlanNode，现有 GUI `contract_status`、step error 和 `current-step` 投影会同步显示阻塞。

### GBX-204 — OpenCode phase transport output recovery

- `crates/gearbox_agent/src/open_code_phase_runtime.rs` 递归解包 OpenCode `--format json` 的 `part.text`、`worker_stdout.output` 和 `assistant_text_delta.delta`，并在 stdout 不含模型文本时读取同一 worker 的 transcript/partial-output。
- 这只修复 phase transport 到 typed parser 的边界，不放宽 PlanGraph schema 或 PlanCritic 的 repository-observation fail-closed 语义；上游普通 Agent 输出路径不变。

### GBX-205 — repository observation bootstrap and in-progress review projection

- `crates/gearbox_agent/src/open_code_phase_runtime.rs` 现在从嵌套 OpenCode `tool_use` 事件提取可信 workspace/workdir，按 call identity 去除 `worker_stdout` 与 `assistant_text_delta` 的重复 transport，并保留 workspace 外路径与无绑定路径的 fail-closed 行为。
- `crates/gearbox_agent/src/gui.rs` 在 review bundle 尚未封存时也扫描当前 plan revision 的绑定 repository observation receipt，投影 bounded 的 unverified/invalid blocker；旧 revision 和不匹配 plan hash 的 receipt 不会污染当前 GUI。
- 这些是 Gear runtime/GUI 投影的最小边界，不改变上游普通 Agent 路径、PlanCritic 门禁或 `.omo/**`。
- PlanCritic/Oracle 的结构化提示现在明确要求七个检查维度都引用至少一个非空证据项（包括通过检查），与 `PlanCriticVerdict` 的 fail-closed 校验保持一致；这是对免费模型输出稳定性的引导性修复，不放宽证据门禁。
- Planner 及其 schema repair prompt 现在显式给出 TDD `test.red`/`test.green` 的 `CommandExpectation` 对象形状，避免免费模型把 RED 命令写成裸字符串或单元素数组；解析器仍保持严格，不做静默放宽。
- PlanCritic/Oracle 的 schema-only repair 不再用没有仓库工具调用的修复会话覆盖首次有效 observation receipt；同一只读计划审查继续绑定首次观测，避免“修 JSON”被误判成仓库观测缺失。
- 仓库观测解析现在识别 `cd ... && ls/wc` 这类常见 shell 包装命令：不再只看第一个 `cd` token，仍只记录工作区内的真实路径并保持去重和 fail-closed。
- PlanCritic schema repair prompt 现在重新提供精确的 PlanGraph 与 deterministic verifier 上下文，并明确丢弃 worker-packet/step telemetry 误导内容；repair 仍只修 JSON 合约，不放宽严格解析。
- PlanCritic/Oracle 初始提示现在明确要求先执行至少一个只读仓库命令，再输出 verdict；没有 repository tool call 的文本回答继续按 fail-closed 处理。
- 2026-07-17 GBX-116 resource-protection GUI projection：`crates/gearbox_agent/src/gui.rs` 从 durable `resource-policy.json` 与 `process-cleanup.json` 有界投影 watcher/protection/cleanup 状态，`crates/agent_ui/src/conversation_view/thread_view.rs` 在 Gear health 面板显示保护与孤儿进程状态；共享 `crates/agent/src/agent.rs` 的 WorkerPacket 测试夹具同步新增 prompt manifest/reconcile/capsule 可选字段。普通 Agent 行为不变。

### 2026-07-17 GBX-251 — Plan revision manifest GUI projection

- `crates/gearbox_agent/src/gui.rs` 从当前 revision 的 durable `plan-revision-manifest-<revision>.json` 有界读取并校验 canonical applied、`requires_re_review`、risk/evidence refs 及受保护工单 lineage；缺失、超限或无效 JSON 只投影明确状态，不伪造 manifest。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 在 Gear Work Orders 面板显示 manifest 状态、canonical/re-review 状态、受保护 lineage 变化和受限原因；只增加 Gearbox runtime 投影，上游普通 Agent 对话路径不变。

### 2026-07-17 GBX-252 — Stable logical task lineage projection

- `crates/gearbox_agent/src/gui.rs` 将 manifest 的 bounded logical task lineage 投影到 Gear snapshot；展示型 task ID 改名不会绕过 runtime 的 protected lineage 校验。
- `crates/agent_ui/src/conversation_view/thread_view.rs` 在 Gear revision 面板显示 affected logical task 与 base/next relation；只消费 runtime snapshot，不改变上游普通 Agent 对话路径。

### 2026-07-17 GBX-253 — Typed evidence obligations and attempt-bound receipts

- `crates/gearbox_agent/src/plan_graph.rs` 为新计划增加 `PlanEvidenceObligation`，保留旧 `evidence` 文案并在 live planner 边界做确定性兼容归一化；字段包含 obligation、kind、producer、consumer、freshness、required_for、evidence_path/unavailable_reason。
- `crates/gearbox_agent/src/state.rs` 将 obligation metadata 绑定到当前 PlanNode attempt 的 criterion receipt；完成门只在存在 typed obligations 时检查对应 `evidence:<obligation_id>` receipt，旧 deterministic/legacy 计划继续可读取。
- `crates/gearbox_agent/src/runtime.rs` 只在 planner/revision runtime 边界重封归一化后的 raw output，避免 seal 隐式改写模型回执；缺证据写入 bounded marker 和 `Blocked/Fail`，不伪造通过。
- `crates/gearbox_agent/src/gui.rs` 与 `crates/gearbox_agent/src/product.rs` 只读投影 obligation 状态、receipt 路径和 unavailable reason；不改变上游普通 Agent 行为或 `.omo/**`。

### 2026-07-17 GBX-254 — Scoped skill injection and command evidence metadata

- `crates/gearbox_agent/src/workers.rs` 新增项目内 `.agents/skills/*/SKILL.md` 的 workspace/target scoped resolver，带 realpath containment、content 去重、持久 cache key、mtime/hash freshness 与 `skills-injection.json` receipt；skills 与 rules 一样是 soft prompt context，不改变 hard task contract。
- `crates/agent/src/agent.rs` 的 Gear 原生 WorkerPacket 路径与命令 worker 共用 skills resolver；普通 Agent skill index 与上游对话路径不变。
- `crates/gearbox_agent/src/tools.rs` 与 `runtime.rs` 为命令结果记录 stdout/stderr truncation flags，并将 flags 写入 verification metadata；旧 JSON 读取通过 serde 默认值保持兼容。
- `crates/gearbox_agent/src/workers.rs` 将未知模型的保守 prompt context 默认提高到 12288 tokens；8k 默认会在真实 PlanGraph hard contract 尚未裁剪前直接拒绝派发，显式 `GEARBOX_WORKER_CONTEXT_LIMIT_TOKENS*` 仍优先。
- `crates/gearbox_agent/src/runtime.rs` 的 planning phase budget receipt 现在从实际 route decision 派生 `worker_call`/`premium`，付费 OpenCode phase 不再以非 worker 调用绕过全局预算统计；该逻辑仅属于 Gear runtime，不改变上游 Agent 路径。
- `crates/gearbox_agent/src/runtime.rs` 在 planning/IntentFold 尚未产生终态时遇到 provider rate-limit/quota/暂时不可用错误，会把 goal/epoch durable settle 为 `Limited` 并发出 `GoalLimited`，保留原 provider blocker；一般编排错误仍保持 `Failed`，不改变上游 Agent 行为。

### 2026-07-17 GBX-255 — Paid OpenCode budget identity and scoped skill cache

- `crates/gearbox_agent/src/workers.rs` 将付费判定绑定到实际 route model：`opencode-go/*` 即使使用 `WorkerKind::OpencodeSession` 也计入 premium worker budget；免费 OpenCode 模型不计入。`phase_routing` 在 premium budget 为零时拒绝显式付费 route，避免预算门禁在 dispatch 后才失效。
- skills freshness cache 按 workspace 与 target set 分片，保留跨 task 的 freshness 观察并避免并行 worker 使用不同 scope 时互相覆盖；命令 worker 与 Gear 原生 worker 仍共用同一 receipt 语义。
- 变更只影响 Gear runtime 的路由、预算和上下文 receipt，不改变上游 Agent 默认行为或 `.omo/**`。

### 2026-07-19 GBX-256 — Epoch identity, scope hard boundary and symlink containment

- `crates/gearbox_agent/src/runtime.rs`：目标 continuation 的 task namespace 绑定 `goal_id::epoch_id`，并在终端 `GoalOutcomeRecorded` 遗留时补齐 objective terminal event；context-pressure 同义摘要统一进入有界恢复。scope drift 不再允许 Review 的 `goal_satisfied=true` 豁免，越界路径或超文件预算只能停在 bounded `Limited`。
- `crates/gearbox_agent/src/task_manager.rs`：provider session/unavailable/recovery cache 的失效边界加入 `epoch_id`，同一 goal 的新 epoch 不复用旧 broker/provider session。
- `crates/gearbox_agent/src/tools.rs`：外部 effect admission 通过“最近存在祖先 + 缺失尾部”做 canonical containment，拒绝工作区外 symlink 指向的不存在目标；workspace 无法 canonicalize 时 fail closed。
- 这些改动属于 Gear runtime 的恢复、安全和证据边界，不改变上游普通 Agent 行为或 `.omo/**`；共享文件列表和策略变化记录于本节，供后续 upstream 冲突解决复核。

### 2026-07-17 — Upstream sync review through `1d2a4b3f7f`

- 本轮从 fork 与 `upstream/main` 的 merge-base `b0da438545` 复核到 `1d2a4b3f7f`，共 127 个上游提交；没有直接执行整段 merge，因为上游会删除 Gearbox 专属 crate/覆盖层，且共享源码存在策略重叠。
- 已逐项合入并保留 upstream provenance 的提交：`94e59ce2f8`、`6d66d5749d`、`fd013f7f56`（git graph 长提交布局与行悬停预览）；`9552acc2bc`（GPUI 静态图片 EXIF 方向）；`a3f6ef252b`（主题参数色）；`c7ee116ead`（VTSLS TypeScript 7 安装错误）；`aa42d3adce`（Rust 嵌套 tabstop）；`625b29d243`（ProjectSearchBar 焦点）；`7d07d39f19`（仅焦点编辑器闪烁光标）；`058f01fa93`（GPUI prompt 阻止后层 popover 收到 mouse-down-out）。
- `fd013f7f56` 与本 fork 的既有 git graph 上下文菜单实现发生上游/上游语义冲突；仅移除行悬停预览并删除不存在的上游 `commit_context_menu` 导入，保留 Gearbox 现有菜单边界，修复提交为 `1f88ce3191`。
- 验证：`cargo check -p git_ui --lib`、git graph 长提交回归测试、GPUI EXIF 回归测试、GPUI prompt 回归测试、`cargo check -p languages`、`cargo check -p search`、编辑器焦点光标回归测试均通过；主题 JSON 通过 `jq empty`。
- 其余上游提交继续保留在候选队列，涉及 Gearbox 覆盖层或依赖未吸收的上游重构时不自动合入；下一轮应从新的 clean worktree 和本节记录继续逐项验证。
