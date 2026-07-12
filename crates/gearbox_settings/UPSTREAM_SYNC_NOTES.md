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
