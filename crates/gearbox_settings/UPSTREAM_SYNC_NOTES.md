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

## Modified Upstream Files

### Build & workspace

| File | Change |
|------|--------|
| `Cargo.toml` | `+workspace deps: gearbox, gearbox_settings, gearbox_agent` |
| `Cargo.lock` | Auto-updated |
| `.github/workflows/gearbox_release.yml` | New workflow. Builds `--package gearbox` on GH runners (Linux/macOS/Windows). Produces `.deb`, `.dmg`, `.exe`. Publishes to GH Release (no Sentry/Slack/notarization/signing). Keep separate from upstream. |

### Settings infrastructure

| File | Change |
|------|--------|
| `crates/settings/src/settings.rs` | `+set_settings_asset_loader`, `+settings_asset_str`. Upstream default still loads `SettingsAssets`. Gearbox calls `gearbox_settings::load` before `settings::init`. |
| `crates/settings/src/keymap_file.rs` | Keymap loading → `settings_asset_str` (keeps default Zed keymaps unchanged) |

### UI components — routed through `gearbox_translate_text`

All user-visible text in these components goes through the shared translation layer:

| File | Text type |
|------|-----------|
| `crates/ui/src/components/label/label.rs` | Label |
| `crates/ui/src/components/label/loading_label.rs` | Loading label |
| `crates/ui/src/components/button/button.rs` | Button |
| `crates/ui/src/components/button/button_link.rs` | Button link |
| `crates/ui/src/components/button/toggle_button.rs` | Toggle button |
| `crates/ui/src/components/button/copy_button.rs` | Copy button messages/tooltips |
| `crates/ui/src/components/button/icon_button.rs` | Icon button |
| `crates/ui/src/components/tooltip.rs` | Tooltip |
| `crates/ui/src/components/context_menu.rs` | Context menu |
| `crates/ui/src/components/modal.rs` | Modal |
| `crates/ui/src/components/chip.rs` | Chip |
| `crates/ui/src/components/tree_view_item.rs` | Tree item |
| `crates/ui/src/components/project_empty_state.rs` | Empty state labels, action buttons |
| `crates/ui/src/components/collab/update_button.rs` | Update progress labels; inline GEARBOX_GUI Chinese text |
| `crates/ui/src/components/ai/agent_setup_button.rs` | `"Gearbox Agent"` under GEARBOX_GUI |
| `crates/ui/src/components/ai/configured_api_card.rs` | API card labels |
| `crates/ui/src/components/ai/thread_item.rs` | Thread item labels |
| `crates/ui/src/components/ai/ai_setting_item.rs` | AI setting labels |
| `crates/ui/src/components/list/list_header.rs` | List header |
| `crates/ui/src/components/list/list_sub_header.rs` | List sub-header |
| `crates/ui/src/components/list/list_bullet_item.rs` | List bullet |
| `crates/ui/src/styles/typography.rs` | Headline text |

### GUI crates — own `gearbox_label` / `gearbox_text` helpers

Each crate has a local helper that checks `GEARBOX_GUI` and returns Chinese or English.  All changes preserve upstream behavior outside `GEARBOX_GUI=1`.

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

### Settings UI — own `gearbox_text` / `gearbox_shared_text` / `gearbox_setting_description`

| File | What | Notes |
|------|------|-------|
| `crates/settings_ui/src/settings_ui.rs` | Page names, section headers, item titles, descriptions, subpage links, action links, navigation entries, breadcrumbs, window title, search placeholder, settings-file buttons, user/project/server scope labels, workspace-restoration text, telemetry labels, scoped settings | Fallback → `gearbox_translate_text` / `gearbox_translate_setting_description`. `Zed`→`Gearbox` in descriptions. Data model unchanged. |
| `crates/settings_ui/src/components/dropdown.rs` | Enum labels: `Last Session`→`上次会话`, `Last Workspace`→`上次工作区`, `Empty Tab`→`空白标签页`, on/off, light/dark/system | Enum values/settings unchanged |
| `crates/settings_ui/src/components/font_picker.rs` | Placeholder `"Search fonts…"`→`"搜索字体..."` | |
| `crates/settings_ui/src/components/theme_picker.rs` | Placeholder; `Zed` theme names→`Gearbox` display | Internal theme IDs unchanged |
| `crates/settings_ui/src/components/icon_theme_picker.rs` | Placeholder; `Zed` icon theme names→`Gearbox` display | Internal IDs unchanged |
| `crates/settings_ui/src/components/ollama_model_picker.rs` | Placeholder | |
| `crates/settings_ui/src/pages/edit_prediction_provider_setup.rs` | Restart instruction→Gearbox wording | |
| `crates/settings_ui/src/pages/llm_providers_page.rs` | Restart instruction→Gearbox wording | |
| `crates/settings_ui/src/pages/tool_permissions_setup.rs` | Native-agent disclaimer→Gearbox wording | |
| `crates/settings_ui/src/pages/sandbox_settings.rs` | Sandbox explanation→Gearbox wording | |
| `crates/settings_ui/src/pages/skill_creator.rs` | Private-file retry→Gearbox wording | |

### Language model & OAuth providers

| File(s) | What | Notes |
|---------|------|-------|
| `crates/language_models/src/provider.rs` | Visible provider/help/error wording: `Zed`→`Gearbox` | |
| `crates/language_models/src/provider/api_compatible.rs` | Same pattern | |
| `crates/language_models/src/provider/bedrock.rs` | Same pattern | |
| `crates/language_models/src/provider/cloud.rs` | Same pattern | |
| `crates/language_models/src/provider/llama_cpp.rs` | Same pattern | |
| `crates/language_models/src/provider/lmstudio.rs` | Same pattern | |
| `crates/language_models/src/provider/mistral.rs` | Same pattern | |
| `crates/language_models/src/provider/ollama.rs` | Same pattern | |
| `crates/language_models/src/provider/openai_subscribed.rs` | Same pattern | |
| `crates/language_models/src/provider/opencode.rs` | Same pattern | |
| `crates/language_models/src/provider/vercel_ai_gateway.rs` | Same pattern | |
| | **All providers:** internal type names, provider type names, plan enum variants unchanged. | |
| `crates/context_server/src/context_server.rs` | Visible OAuth/client metadata→Gearbox wording | Endpoint constants kept |
| `crates/context_server/src/oauth.rs` | Same | Endpoint constants kept |

---

## Gear Native Agent (`crates/gearbox_agent/`)

New runtime crate.  Functions as the orchestration engine for the `Gear` agent.

### Key modules

| Module | Purpose |
|--------|---------|
| `runtime.rs` | `Orchestrator::run()` — goal-pursuit loop: spec→plan→worker→verify→repair→review. Sync, runs on `background_spawn`. `DEFAULT_MAX_ITERATIONS=2`. Accepts `coordinator_model`/`coordinator_brief`. |
| `workers.rs` | `WorkerRegistry`→`CommandWorker`→external commands. `WorkerKind`: opencode/codex/claude/zed_agent/custom. `WorkerPacket` JSON contract. `require_worker`/`skip_worker` flags. |
| `state.rs` | `Goal`, `Session`, `Task`, `Event`, `CoordinatorModel` data models. `StateStore` — JSON files under `.gearbox-agent/`. |
| `tools.rs` | `git_snapshot`, `check_scope`, `run_shell_command_with_env_and_cancellation`, `CancellationToken` (`Arc<AtomicBool>`) |
| `languages.rs` | `LanguageDetection` — TypeScript/Python/Rust detection. `detect_with_request()` falls back to request text for empty workspaces (web/app prompts→TypeScript scaffold). |
| `product.rs` | Markdown artifacts: spec, plan, verification, final-report. Includes `coordinator_model`/`coordinator_brief` summaries. Web App stack guidance. |
| `cli.rs` | `gear` binary (name may conflict with system `gear`). `gear run <prompt>` with worker/scope/verify args. |
| `Cargo.toml` | Deps: `smol`, `chrono`, `clap`, `serde`, `serde_json`, `anyhow`. Binary: `gear`. |

### Recent additions
- `CoordinatorModel` (provider_id/model_id/name) persisted in goals and worker packets
- `coordinator_brief` (optional LLM planning context, generated before run)
- Empty-workspace prompts→TypeScript Web App default stack + npm verify commands
- `TaskInputs` (spec/plan packet paths) in worker packets

---

## Agent Integration Changes

### `crates/agent/Cargo.toml`
- `+dep: gearbox_agent`

### `crates/agent/src/agent.rs`
- `+GEAR_AGENT_ID = AgentId::new("Gear")`
- `+Session.gear_cancellation_token: Option<CancellationToken>`, `+Session.work_dirs: Option<PathList>`
- `NativeAgentConnection::gear(agent)` constructor
- `send_gear_prompt()`: routes prompts → `Orchestrator::run()` on `cx.background_spawn`
  - Reads thread's selected model→`gear_coordinator_from_thread()`→`CoordinatorModel`
  - `generate_gear_coordinator_brief()`: async LLM call for planning brief; skips test-provider (`"fake"`)
  - `is_gear_executable_goal()`: filters greetings/small-talk; trims ASCII+CJK punctuation; checks action words + char count (≥12)
  - Greeting path: `"你好，我是 Gear。请告诉我你想完成的目标..."` Chinese response
  - `gear_workspace_for_session()`: work_dirs→visible_worktree fallback
  - Event streaming: `async_channel` from orchestrator→ACP thread for live progress
- `gear_worker_config_from_env()`: `GEARBOX_GEAR_WORKER` (kind), `GEARBOX_GEAR_WORKER_COMMAND`, fallback to legacy `GEARBOX_OPENCODE_COMMAND`. Warns on invalid kinds. `require_worker: true` when command configured.
- `cancel()`: cancels Gear token + `thread.cancel()` for cleanup
- `clear_gear_cancellation_token()`: clears token if same reference

### `crates/agent/src/native_agent_server.rs`
- `NativeAgentServer::gear()`: `agent_id: GEAR_AGENT_ID`, `telemetry_id: "gear"`, `logo: Sparkle`

### `crates/agent/src/tests/mod.rs`
- Updated native agent tests for explicit identity metadata fields (struct destructuring)

### `crates/agent_ui/src/agent_ui.rs`
- `+Agent::GearAgent` variant, serde alias `"GearAgent"`
- `Agent::label()`: `"Agent"` under GEARBOX_GUI (from `"Zed Agent"`), `"Gear"` for GearAgent
- `Agent::server()`: returns `NativeAgentServer::gear()`
- `Agent::icon()`: `Sparkle` for GearAgent and Custom
- `Agent::is_native()`: includes GearAgent

### `crates/agent_ui/src/agent_panel.rs`
- Gear in `list_agents_and_models`: only when `GEARBOX_GUI=1`; shares native model list
- Context menu entry: `"Gear"` with `Sparkle` icon, launches new thread
- Agent ID routing for sibling thread creation

### `crates/agent_ui/src/agent_connection_store.rs`
- `Agent::GearAgent` entries always retained

### `crates/agent_ui/src/conversation_view/thread_view.rs`, `agent_ui/src/mention_set.rs`
- Updated for native connection identity metadata (struct fields instead of tuple access)

---

## Gearbox Branding & Packaging

### Binary entry point (`crates/gearbox/`)

| File | Change |
|------|--------|
| `src/main.rs` | Sets `GEARBOX_GUI=1` at startup (unsafe, before multi-threading). Data dir→`~/.local/share/gearbox`. User-Agent→`Gearbox/{version}`. Error messages→"Gearbox". Env aliases: `GEARBOX_EXPERIMENTAL_A11Y`, `GEARBOX_STATELESS`, `GEARBOX_GENERATE_MINIDUMPS`, `GEARBOX_WINDOW_DECORATIONS`, `GEARBOX_ALLOW_EMULATED_GPU` (with `ZED_*` fallbacks). Build-time vars (`ZED_BUNDLE`, `ZED_BUILD_ID`, `ZED_COMMIT_SHA`) unchanged. |
| `src/zed.rs` | `DOCS_URL`, `STATUS_URL`, `MERCH_URL`→`github.com/ShamirSecret/gearbox-zed`. Internal action names (`OpenZedUrl`, `RegisterZedScheme`) unchanged. |
| `src/zed/app_menus.rs` | Full Chinese menu items: 视图→放大/缩小/重置缩放, 编辑器布局→拆分, 面板→项目/大纲/协作/终端/调试, etc. |
| `src/zed/open_listener.rs` | (no Gearbox-specific changes) |
| `src/zed/quick_action_bar/repl_menu.rs` | `ZED_REPL_DOCUMENTATION` const→gearbox repo URL |
| `build.rs` | Diagnostic prefix→`"gearbox build.rs:"` |

### Packaging resources

| File | Change |
|------|--------|
| `crates/gearbox/resources/app-icon.icns` | macOS icon from Gearbox PNG |
| `crates/gearbox/resources/flatpak/manifest-template.json` | Command/path→`gearbox`; `ZED_BUNDLE_TYPE` kept |
| `crates/gearbox/resources/snap/snapcraft.yaml.in` | Entry/command→`gearbox`; `ZED_BUNDLE_TYPE` kept |
| `crates/gearbox_settings/assets/settings/*` | Settings with Gearbox comments/docs/menu strings. Internal IDs (`.ZedMono`, `Zed (Default)`, `ZedPredictModal`) kept; Gearbox display layer renames at render. |
| `crates/gearbox_settings/assets/keymaps/*` | Keymaps with Gearbox strings. Internal context IDs kept. |

---

## Follow-up Targets

- Action-name translation layer for command palette entries
- Continue expanding settings item title/description mappings in `settings_ui.rs` and `gearbox_text.rs`
- Continue localizing Agent panel labels
- Continue localizing editor/project prompts and confirmation dialogs
- Per-iteration provider-backed review (Milestone 3)
- TypeScript Web App sample generation (Milestone 3)
