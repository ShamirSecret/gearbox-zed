# Gearbox Upstream Sync Notes

This file records Gearbox-specific changes made outside `crates/gearbox` and `crates/gearbox_settings`.
When syncing with upstream Zed, check these files first. The intended rule is:

- Keep upstream Zed behavior unchanged when `GEARBOX_GUI` is not set.
- Put Gearbox-only localized text behind `GEARBOX_GUI=1`.
- Keep large Gearbox resource copies under `crates/gearbox_settings`.

## Shared Source Changes

### `Cargo.toml`

- Adds `crates/gearbox` as the copied Gearbox GUI crate.
- Adds `crates/gearbox_settings` as the Gearbox-only settings/keymap asset crate.
- Adds `crates/gearbox_agent` as the Gearbox-only Gear runtime CLI prototype crate.
- Adds `gearbox_settings` to workspace dependencies.
- Adds `gearbox_agent` to workspace dependencies.

### `Cargo.lock`

- Updated by Cargo after adding the `gearbox`, `gearbox_settings`, and `gearbox_agent` workspace crates.

### `crates/settings/src/settings.rs`

- Adds `set_settings_asset_loader`.
- Adds `settings_asset_str`.
- Default behavior still loads upstream `SettingsAssets`.
- Gearbox registers `gearbox_settings::load` before `settings::init`, so Gearbox reads localized settings/keymaps without changing upstream assets.

### `crates/settings/src/keymap_file.rs`

- Built-in keymap loading now goes through `settings_asset_str`.
- This keeps default Zed keymaps unchanged and allows Gearbox to load copied keymaps from `crates/gearbox_settings`.

### `crates/onboarding/src/onboarding.rs`

- Adds `gearbox_text`.
- Localizes the onboarding title, subtitle, and finish button only when `GEARBOX_GUI=1`.

### `crates/onboarding/src/basics_page.rs`

- Localizes visible onboarding setup labels and descriptions only when `GEARBOX_GUI=1`.
- Theme names and editor family names are mostly left as upstream values when they are identifiers or existing theme names.

### `crates/onboarding/src/base_keymap_picker.rs`

- Localizes the base keymap picker placeholder only when `GEARBOX_GUI=1`.

### `crates/workspace/src/welcome.rs`

- Adds `gearbox_text`, `gearbox_label`-style welcome helpers.
- Localizes the workspace welcome page sections, buttons, subtitle, agent card, recent projects header, and tab title only when `GEARBOX_GUI=1`.

### `crates/project_panel/src/project_panel.rs`

- Adds `gearbox_label`.
- Localizes the project panel context menu only when `GEARBOX_GUI=1`.
- File-manager labels from shared `ui::utils` are intentionally not changed yet.

### `crates/command_palette/src/command_palette.rs`

- Adds a small Gearbox label helper.
- Localizes the command palette placeholder only when `GEARBOX_GUI=1`.
- Command names are not fully localized yet because they are derived from action metadata and should be handled by a separate action-name translation layer.

### `crates/file_finder/src/file_finder.rs`

- Adds `gearbox_label`.
- Localizes the file finder placeholder, ignored-file filter tooltip, create-file preview prompt, and split/open actions only when `GEARBOX_GUI=1`.

### `crates/open_path_prompt/src/open_path_prompt.rs`

- Adds `gearbox_label`.
- Localizes the create/replace path confirmation body, confirmation buttons, and empty-state text only when `GEARBOX_GUI=1`.
- The prompt title still includes the target path and remains mostly upstream-formatted.

### `crates/recent_projects/src/recent_projects.rs`

- Adds `gearbox_label`.
- Localizes the recent-project picker placeholder, section headers, no-match text, and several high-frequency footer/action labels only when `GEARBOX_GUI=1`.

### `crates/recent_projects/src/sidebar_recent_projects.rs`

- Adds `gearbox_label`.
- Localizes the sidebar recent-project picker placeholder, no-match text, open-project tooltip, and failed-open prompt title only when `GEARBOX_GUI=1`.

### `crates/recent_projects/src/wsl_picker.rs`

- Adds `gearbox_label`.
- Localizes the WSL distro picker placeholder only when `GEARBOX_GUI=1`.

### `crates/settings_ui/src/components/font_picker.rs`

- Localizes the font picker placeholder only when `GEARBOX_GUI=1`.

### `crates/settings_ui/src/components/theme_picker.rs`

- Localizes the theme picker placeholder only when `GEARBOX_GUI=1`.
- Displays `Zed` theme names as `Gearbox` theme names only when `GEARBOX_GUI=1`; saved/internal theme IDs remain unchanged.

### `crates/settings_ui/src/components/icon_theme_picker.rs`

- Localizes the icon theme picker placeholder only when `GEARBOX_GUI=1`.
- Displays `Zed` icon theme names as `Gearbox` icon theme names only when `GEARBOX_GUI=1`; saved/internal theme IDs remain unchanged.

### `crates/settings_ui/src/components/ollama_model_picker.rs`

- Localizes the Ollama model picker placeholder only when `GEARBOX_GUI=1`.

### `crates/settings_ui/src/components/dropdown.rs`

- Adds Gearbox-only display translations for common enum dropdown values such as `Last Session`, `Last Workspace`, `Empty Tab`, and simple on/off/system labels.
- Internal enum values and serialized settings remain unchanged.

### `crates/settings_ui/src/settings_ui.rs`

- Adds Gearbox display helpers for settings pages, section headers, item titles, descriptions, subpage links, action links, navigation entries, breadcrumbs, and the settings window title.
- Keeps the upstream settings data model unchanged; translations happen at render time when `GEARBOX_GUI=1`.
- Dynamically displays `Zed` as `Gearbox` in settings descriptions only when `GEARBOX_GUI=1`.
- Localizes deeper settings UI labels such as search placeholder, settings-file buttons, user/project/server scope labels, workspace restoration settings, telemetry settings, and scoped settings only when `GEARBOX_GUI=1`.
- Adds display mappings for the regular `page_data.rs` setting section headers and setting titles so Gearbox settings pages show Chinese labels while preserving upstream JSON paths and setting metadata.

### `crates/settings_ui/src/pages/edit_prediction_provider_setup.rs`

- Replaces the visible restart instruction with Gearbox wording.

### `crates/settings_ui/src/pages/llm_providers_page.rs`

- Replaces the visible restart instruction with Gearbox wording.

### `crates/settings_ui/src/pages/tool_permissions_setup.rs`

- Replaces the visible native-agent disclaimer with Gearbox wording.

### `crates/settings_ui/src/pages/sandbox_settings.rs`

- Replaces the visible sandbox permission explanation with Gearbox wording.

### `crates/settings_ui/src/pages/skill_creator.rs`

- Replaces the visible private-file retry explanation with Gearbox wording.

### `crates/agent_ui/src/agent_ui.rs`

- Displays the native agent label as `Gearbox Agent` only when `GEARBOX_GUI=1`.

### `crates/ui/src/components/ai/agent_setup_button.rs`

- Displays the agent setup button preview/name as `Gearbox Agent` only when `GEARBOX_GUI=1`.

### `crates/ui/src/components/collab/update_button.rs`

- Displays update progress labels as Gearbox Chinese text only when `GEARBOX_GUI=1`.

### `crates/language_models/src/provider.rs`
### `crates/language_models/src/provider/api_compatible.rs`
### `crates/language_models/src/provider/bedrock.rs`
### `crates/language_models/src/provider/cloud.rs`
### `crates/language_models/src/provider/llama_cpp.rs`
### `crates/language_models/src/provider/lmstudio.rs`
### `crates/language_models/src/provider/mistral.rs`
### `crates/language_models/src/provider/ollama.rs`
### `crates/language_models/src/provider/openai_subscribed.rs`
### `crates/language_models/src/provider/opencode.rs`
### `crates/language_models/src/provider/vercel_ai_gateway.rs`

- Replaces visible provider/help/error wording that referenced Zed with Gearbox wording.
- Internal provider type names and plan enum variants remain unchanged to avoid breaking upstream code paths.

### `crates/context_server/src/context_server.rs`
### `crates/context_server/src/oauth.rs`

- Replaces visible OAuth/client metadata names with Gearbox wording.
- OAuth endpoint constants are not fully rebranded yet because they may be functional upstream service endpoints.

## Gearbox Resource and Branding Changes

### `crates/gearbox/resources/*`

- Replaces visible Flatpak, Snap, Windows installer, and desktop-entry metadata with Gearbox names, descriptions, and repository links.
- Keeps file names such as `zed.desktop.in` where they are inherited by upstream build scripts.

### `crates/gearbox/src/main.rs`
### `crates/gearbox/src/zed.rs`
### `crates/gearbox/src/zed/app_menus.rs`
### `crates/gearbox/src/zed/open_listener.rs`
### `crates/gearbox/src/zed/quick_action_bar/repl_menu.rs`

- Replaces visible docs/help/menu/test URLs with Gearbox repository links or Gearbox wording.
- Keeps internal action/type names such as `OpenZedUrl` and `RegisterZedScheme` unchanged for now.

### `crates/gearbox_settings/assets/settings/*`
### `crates/gearbox_settings/assets/keymaps/*`

- Replaces copied Gearbox settings/keymap comments, docs links, and visible menu argument strings with Gearbox wording where safe.
- Keeps internal keymap contexts such as `ZedPredictModal` unchanged because they are code identifiers.
- Keeps default font/theme identifiers such as `.ZedMono`, `.ZedSans`, and `Zed (Default)` internally; Gearbox display layers rename them at render time.

## Follow-up Localization Targets

- Add an action-name translation layer for command palette entries.
- Continue expanding settings item title mappings in `crates/settings_ui/src/settings_ui.rs`.
- Continue localizing deeper Agent panel labels in `crates/agent_ui`.
- Continue localizing editor/project prompts and confirmation dialogs as they are encountered.

## 2026-07-07 Gearbox Shared GUI Localization Layer

### `crates/ui/src/gearbox_text.rs`

- Adds the shared Gearbox GUI text translation entrypoint used only when `GEARBOX_GUI=1`.
- Keeps translation display-only: exact UI strings, safe short title phrases, and safe `Zed` to `Gearbox` visible-brand replacement.
- Avoids changing settings schemas, action identifiers, JSON paths, URLs, or internal type names.

### `crates/ui/src/ui.rs`

- Exports `gearbox_translate_text` so other GUI crates can reuse the shared display-text translation layer.

### `crates/ui/src/components/label/label.rs`
### `crates/ui/src/components/button/button.rs`
### `crates/ui/src/components/button/button_link.rs`
### `crates/ui/src/components/button/toggle_button.rs`
### `crates/ui/src/components/tooltip.rs`
### `crates/ui/src/components/context_menu.rs`
### `crates/ui/src/components/modal.rs`
### `crates/ui/src/components/project_empty_state.rs`
### `crates/ui/src/components/tree_view_item.rs`
### `crates/ui/src/components/chip.rs`
### `crates/ui/src/components/list/list_header.rs`
### `crates/ui/src/components/list/list_sub_header.rs`
### `crates/ui/src/components/list/list_bullet_item.rs`
### `crates/ui/src/components/ai/configured_api_card.rs`
### `crates/ui/src/components/ai/thread_item.rs`
### `crates/ui/src/components/ai/ai_setting_item.rs`

- Routes common label/title/button/menu/tooltip/list/modal strings through the shared Gearbox translation entrypoint.
- This intentionally centralizes broad GUI localization instead of patching each screen independently.

### `crates/settings_ui/src/settings_ui.rs`

- Routes Settings shared text and fallback descriptions through `ui::gearbox_translate_text`.
- Keeps the existing Settings-specific exact translations, but lets the shared UI table cover deeper titles, enum labels, and descriptions.

## 2026-07-07 Gearbox Settings Description Localization Follow-up

### `crates/ui/src/gearbox_text.rs`

- Adds multiline display-text localization so runtime-generated settings descriptions can be translated line by line.
- Adds Gearbox Chinese translations for the deeper language-server settings descriptions shown under Settings > Languages & Tools > LSP.
- Adds common dropdown labels such as `Find All References` and `Center` to the shared Gearbox GUI translation table.

## 2026-07-07 Gearbox GUI Brand and Sentence Leakage Scan

### `crates/ui/src/gearbox_text.rs`

- Adds `translate_setting_description` for Settings descriptions so non-exact English descriptions use a restricted sentence-level Chinese fallback.
- Adds a restricted visible-sentence fallback for ordinary GUI labels that look like complete English sentences.
- Adds Gearbox translations for visible Zed AI, Copilot, update, extension built-in-support, Git support, and source-linking brand strings.
- Keeps protocol strings, extension ABI identifiers, and service identifiers such as `zed://`, `x-zed-*`, and `zed:extension/*` out of this display translation path.

### `crates/ui/src/ui.rs`

- Exports `gearbox_translate_setting_description` for GUI crates that render Settings descriptions.

### `crates/settings_ui/src/settings_ui.rs`

- Routes Settings description fallback through `ui::gearbox_translate_setting_description` instead of the generic label/title translator.

### `crates/ui/src/components/label/loading_label.rs`
### `crates/ui/src/components/button/copy_button.rs`
### `crates/ui/src/components/collab/update_button.rs`
### `crates/ui/src/styles/typography.rs`

- Routes loading labels, copy-button messages/tooltips, update-button messages, and headline text through the shared Gearbox display translator.
- `HighlightedLabel` was intentionally not routed through translation because its highlight indices are byte offsets into the original text.

## 2026-07-07 Gearbox GUI Leakage Follow-up

### `crates/workspace/src/notifications.rs`

- Routes message notification titles, secondary content, and primary action labels through the shared Gearbox display translator.
- This catches notification text that does not directly enter through `Label::new` or `Button::new` call sites.

### `crates/workspace/src/pane_group.rs`
### `crates/collab_ui/src/notifications/incoming_call_notification.rs`

- Adds Gearbox Chinese wording for dynamic collaboration location/share labels that include usernames and cannot be translated by exact string matching.

### `crates/oauth_callback_server/src/oauth_callback_server.rs`

- Adds Gearbox Chinese OAuth success/failure browser pages when `GEARBOX_GUI=1`.
- Keeps the Zed wording when running the original GUI path.

### `crates/debugger_ui/src/session/running.rs`
### `crates/debugger_ui/src/new_process_modal.rs`

- Adds Gearbox Chinese wording for a debugger scenario error and rebrands the debugger command placeholder when `GEARBOX_GUI=1`.

### `crates/collab_ui/src/collab_panel.rs`

- Removes visible `zed.dev/cla` branding from the Gearbox CLA error path.

### `crates/extensions_ui/src/extensions_ui.rs`

- Adds Gearbox Chinese wording for dynamic extension-version compatibility tooltips.

### `crates/ui/src/gearbox_text.rs`

- Adds exact translations for update, portal, Pro/payment, and notification strings that were found in the GUI leakage scan.

## 2026-07-07 Gearbox settings localization coverage

### `crates/settings_ui/src/settings_ui.rs`

- Adds Gearbox Chinese overrides for settings descriptions that are rendered from shared Settings UI metadata.
- Adds exact Gearbox Chinese overrides for project panel, debugger, terminal, Git, collaboration, AI, and network setting descriptions that otherwise fall back to mixed word-by-word translation.
- Keeps the translations in the `GEARBOX_GUI=1` display layer so upstream Zed settings behavior remains unchanged.

### `crates/ui/src/gearbox_text.rs`

- Extends the shared Gearbox text translation layer with exact settings strings and sentence-token vocabulary used by the settings UI.
- Preserves this shared-source override so future upstream syncs can keep Gearbox Chinese settings coverage without renaming upstream internals.

## 2026-07-07 Gearbox GitHub Release Workflow

### `.github/workflows/gearbox_release.yml`

- Adds a Gearbox-only GitHub Actions release workflow instead of modifying Zed's upstream `.github/workflows/release.yml` or `.github/workflows/run_bundling.yml`.
- Builds `--package gearbox` on GitHub-hosted runners for Linux x86_64, macOS aarch64, macOS x86_64, and Windows x86_64.
- Uploads platform archives and installer packages as workflow artifacts on every run.
- Publishes those artifacts to a GitHub Release when the workflow runs from a tag or when `workflow_dispatch` provides `release_tag`.
- Produces Linux `.deb`, macOS `.dmg`, and Windows Inno Setup `.exe` installer artifacts, while keeping zip/tar archives as fallback assets.
- Uses GitHub Release assets instead of Zed's blob store, Sentry, Slack, self-hosted runners, official signing, notarization, and store publication steps.
- Current installers are community Gearbox packages: macOS artifacts are not notarized, Windows artifacts are not code-signed, and Linux only emits a `.deb` package.
- Keep this workflow separate when syncing upstream so Zed's official release workflow can continue to be compared or copied forward without merge noise.

## 2026-07-07 Gearbox GUI Localization Leakage Sweep

### `crates/ui/src/gearbox_text.rs`

- Adds exact Gearbox Chinese translations for recent-project open/remove actions, keybinding buttons, command run labels, and Agent quota/feedback text.
- Keeps the translations in the shared display layer so upstream identifiers and action names remain unchanged.

### `crates/recent_projects/src/remote_connections.rs`

- Adds Gearbox-only Chinese wording for remote connection failure prompt titles and retry/cancel buttons when `GEARBOX_GUI=1`.
- Keeps upstream Zed wording unchanged outside the Gearbox GUI path.

### `crates/project_panel/src/project_panel.rs`

- Adds Gearbox-only Chinese wording for the discard-changes restore prompt and buttons when `GEARBOX_GUI=1`.

## 2026-07-07 Gearbox Packaging Brand and Icon Sweep

### `crates/gearbox/resources/app-icon.icns`

- Adds the Gearbox macOS app icon resource generated from the Gearbox PNG app icon.
- Matches the GitHub Release DMG packaging step that copies `app-icon.icns` into `Gearbox.app`.

### `crates/gearbox/resources/flatpak/manifest-template.json`

- Switches the Flatpak command/module/resource path from upstream `zed` resources to Gearbox command and Gearbox resources.
- Keeps internal `ZED_BUNDLE_TYPE` unchanged because it is still an application runtime environment key.

### `crates/gearbox/resources/snap/snapcraft.yaml.in`

- Switches the Snap app entry and command from `zed` to `gearbox`.
- Keeps internal `ZED_BUNDLE_TYPE` unchanged because it is still an application runtime environment key.

## 2026-07-07 Gearbox Native Agent Split

### `crates/agent/src/agent.rs`
### `crates/agent/src/native_agent_server.rs`

- Adds a second native agent identity, `Gear`, alongside the existing upstream native agent identity.
- Keeps the upstream native connection path as `Zed Agent` internally while allowing Gearbox GUI surfaces to display it as `Agent`.
- Gives the Gear native server its own agent id and telemetry id.
- Routes Gear prompts through `gearbox_agent::runtime::Orchestrator`, using the ACP/native thread shell only for session hosting and UI rendering.
- Persists native session work directories so Gear runs in the project workspace instead of the process current directory.
- Streams Gear runtime events into the native ACP thread so the Gearbox GUI shows Gear-owned progress instead of only the final worker report.
- Wires GUI cancel to a Gear cancellation token so Gear orchestrator, worker, and verification commands can stop from the native Agent Panel cancel action.
- Passes the Gear runtime's default iteration limit from the GUI so Gear runs as a bounded goal-pursuit loop instead of a one-shot worker wrapper.
- Reads `GEARBOX_GEAR_WORKER` and `GEARBOX_GEAR_WORKER_COMMAND` for Gear worker selection, while keeping `GEARBOX_OPENCODE_COMMAND` as a compatibility fallback.

### `crates/agent_ui/src/agent_ui.rs`
### `crates/agent_ui/src/agent_panel.rs`
### `crates/agent_ui/src/agent_connection_store.rs`
### `crates/agent_ui/src/conversation_view/thread_view.rs`
### `crates/agent_ui/src/mention_set.rs`

- Adds `Agent::GearAgent` as a native GUI agent option that appears only when `GEARBOX_GUI=1`.
- Keeps the original native agent visible in Gearbox GUI as `Agent`, and adds `Gear` as the second native agent in the agent picker and `list_agents_and_models` output.
- Updates native connection access to avoid tuple-field assumptions now that native connections carry explicit identity metadata.

### `crates/ui/src/gearbox_text.rs`
### `crates/ui/src/components/ai/agent_setup_button.rs`

- Updates Gearbox GUI text so upstream Zed agent surfaces display as `Agent`, leaving the new `Gear` label available for the Gear agent entry.

## 2026-07-08 Gear Runtime and GUI Fixes

### `crates/agent/Cargo.toml`
### `crates/agent/src/tests/mod.rs`

- Records the shared-agent integration files touched by the Gear native agent split.
- `crates/agent/Cargo.toml` depends on `gearbox_agent` so the Gear native agent can call the Gear runtime orchestrator.
- `crates/agent/src/tests/mod.rs` updates native agent connection tests for explicit native agent identity metadata and the additional Gear native connection.

### `crates/gearbox_agent/src/workers.rs`
### `crates/gearbox_agent/src/runtime.rs`

- Keeps worker prompt packet serialization fallible instead of silently writing `{}` when serialization fails.
- Adds clearer test failure context for Gear runtime event collection lock failures.

### `crates/ui/src/gearbox_text.rs`

- Narrows Gearbox GUI sentence punctuation localization so identifiers and numeric versions such as `package.json`, `1.0`, and `1,000` are not rewritten by broad display translation.

## 2026-07-08 Gear Fix Plan Follow-up and Milestone 3 Start

### `crates/command_palette/src/command_palette.rs`
### `crates/debugger_ui/src/debugger_panel.rs`
### `crates/extensions_ui/src/extensions_ui.rs`
### `crates/extensions_ui/src/extension_version_selector.rs`
### `crates/recent_projects/src/remote_servers.rs`
### `crates/ui/src/components/project_empty_state.rs`
### `crates/workspace/src/security_modal.rs`

- Routes the first Gearbox localization audit batch through `ui::gearbox_translate_text` / `crate::gearbox_text::translate` for user-visible labels and buttons while preserving upstream action IDs, telemetry event names, URLs, and non-Gearbox English behavior.
- Covers project empty state, restricted mode, command palette run button, debugger empty state, extension documentation/dev-install labels, extension compatibility labels, and remote server/dev-container actions.

### `crates/ui/src/gearbox_text.rs`

- Preserves meaning for fallback settings prefixes such as `Amount of`, `Number of`, and `A mapping from` instead of dropping those words.
- Keeps common abbreviation punctuation such as `e.g.,`, `i.e.`, and `etc.` out of Chinese sentence punctuation localization.
- Replaces visible `Zed` branding only as a standalone word so names such as `ZedGraph` are not rewritten.
- Expands sentence and title token translations from `GEARBOX_L10N_AUDIT.md` to reduce mixed English/Chinese settings descriptions.
- Adds exact translations for the first localization audit batch and regression tests for high-frequency settings sentence tokens.

### `crates/agent/src/agent.rs`

- Warns when `GEARBOX_GEAR_WORKER` contains an unknown worker kind instead of silently treating the value as `opencode`.
- Adds testable Gear worker env parsing coverage for explicit worker command, legacy `GEARBOX_OPENCODE_COMMAND` fallback, and invalid worker kind fallback.
- Avoids re-entering `AcpThread` updates when Gear sends a prompt; the ACP thread already records the user message before calling the Gear connection, while Gear still mirrors the user block into its internal native `Thread` for persistence and runtime context.
- Keeps short greetings and small-talk in the Gear GUI session instead of starting a Gear runtime goal and writing `.gearbox-agent` artifacts.
- Passes the Gear native thread's selected provider/model into `gearbox_agent` as coordinator metadata so the Gear runtime can distinguish the planning/review model from implementation workers.
- Calls the selected provider/model once before starting a Gear run to generate a bounded `coordinator_brief`; fake test providers are skipped so GPUI tests do not wait on unfinished model streams.

### `crates/gearbox/src/main.rs`
### `crates/gearbox/src/zed.rs`

- Adds Gearbox runtime environment aliases for user-facing Gearbox app switches while retaining upstream `ZED_*` fallbacks:
  - `GEARBOX_EXPERIMENTAL_A11Y`
  - `GEARBOX_STATELESS`
  - `GEARBOX_GENERATE_MINIDUMPS`
  - `GEARBOX_WINDOW_DECORATIONS`
  - `GEARBOX_ALLOW_EMULATED_GPU`
- Keeps build-time/upstream compatibility variables such as `ZED_BUNDLE`, `ZED_BUILD_ID`, and `ZED_COMMIT_SHA` unchanged.

### `crates/gearbox_agent/src/languages.rs`
### `crates/gearbox_agent/src/product.rs`
### `crates/gearbox_agent/src/runtime.rs`
### `crates/gearbox_agent/src/workers.rs`

- Starts Milestone 3 by treating empty-workspace Web/App prompts as TypeScript Web App generation targets.
- Writes TypeScript Web App default stack guidance into generated spec and plan artifacts.
- Adds guarded npm build verification commands for new TypeScript scaffold targets.
- Includes task input artifact paths in worker packets so workers can read the generated spec and plan before editing.
- Persists optional `coordinator_model` and `coordinator_brief` data in Gear goal ledgers, generated artifacts, and worker packets without depending on Gearbox GUI provider types inside the runtime crate.

### `docs/gearbox-gear-agent-plan.md`

- Updates current progress to reflect that the goal-pursuit runtime loop exists, GUI selected model metadata and an initial provider-backed coordinator brief now reach the runtime, and the next active track is per-iteration provider-backed review plus TypeScript Web App sample generation.

## 2026-07-08 Gear Detailed Bug Fixes

### `crates/agent/src/agent.rs`

- Tightens Gear prompt routing so Chinese punctuation is trimmed before small-talk checks and long casual text no longer starts a Gear runtime goal solely because of length.
- Treats configured Gear worker commands as required workers so worker failures are surfaced instead of being silently ignored when verification passes.
- Removes the production `"fake"` provider id bypass from coordinator brief generation; GPUI tests now drive fake model streams explicitly.

### `crates/gearbox_agent/src/runtime.rs`

- Keeps the MVP verification-passed completion policy, but includes skipped/failed worker status in the final goal summary when the worker was not required.

### `crates/ui/src/gearbox_text.rs`

- Preserves useful spacing for mixed English/Chinese title translations while keeping fully translated Chinese titles compact.
- Adds title/exact translation coverage for common Agent, profile, call, repository, provider, permission, checkpoint, and subagent labels surfaced by `GEARBOX_DETAILED_BUGS.md`.

### `crates/gearbox/build.rs`

- Rebrands the visible Linux pkg-config diagnostic prefix from `zed build.rs` to `gearbox build.rs`.
