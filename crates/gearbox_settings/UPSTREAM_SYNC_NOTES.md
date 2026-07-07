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
- Adds `gearbox_settings` to workspace dependencies.

### `Cargo.lock`

- Updated by Cargo after adding the `gearbox` and `gearbox_settings` workspace crates.

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
