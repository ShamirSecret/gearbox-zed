# Gearbox 汉化质量审计报告

> 基于 HEAD `f3217b05eb` 完整扫描。重点：中英夹杂（Chinglish）问题、未翻译的 UI 区域、句子级翻译词汇表缺口。

---

## 执行进度（2026-07-08）

本轮已执行：

- 修复 P0 句子级翻译高频 token 缺口：补充 `not`、`if`、`all`、`never`、`always`、`first`、`over`、`during`、`without`、`than`、`more`、`less`、`both`、`some`、`other`、`your`、`any`，并额外补充 `cannot`、`many`、`may`、`updates`、`provide`、`requiring`、`request` 等典型设置描述残留词。
- 修复 title token 缺口：补充 `All`、`Always`、`First`、`Never`、`Run`，避免 dropdown/title fallback 继续显示英文。
- 增加 `gearbox_text` 单元测试，覆盖本报告列出的典型中英夹杂设置句和标题 token。
- 执行 P1 第一批小范围裸英文接入：
  - `crates/ui/src/components/project_empty_state.rs`
  - `crates/workspace/src/security_modal.rs`
  - `crates/command_palette/src/command_palette.rs`
  - `crates/debugger_ui/src/debugger_panel.rs`
  - `crates/extensions_ui/src/extensions_ui.rs`
  - `crates/extensions_ui/src/extension_version_selector.rs`
  - `crates/recent_projects/src/remote_servers.rs`
- 补充上述 UI 文本所需 exact translations，例如 `Open Project`、`Clone Repository`、`Stay in Restricted Mode`、`Trust and Continue`、`Debugger Docs`、`No Breakpoints Set`、`View Documentation`、`Install Dev Extension`、`Open Zed Log`、`Creating Dev Container`、`Copy Server Address`、`Remove Server`。

已验证：

- `cargo test -p ui gearbox_text`
- `cargo check -p command_palette -p debugger_ui -p extensions_ui -p recent_projects -p workspace`

仍未完成：

- `agent_ui/src/`、`collab_ui/src/`、`debugger_ui/src/new_process_modal.rs`、`extensions_ui` 更大范围字符串仍需分批接入共享翻译层。
- P2 的翻译模式统一尚未完成；本轮只修复点名裸字符串，未重构各 crate 的本地 `gearbox_label()` helper。
- 报告中提到的 `is_safe_brand_text` 子串误替换和缩写标点问题，经复查当前代码已经有边界匹配与缩写处理，不再是当前 HEAD 的未修项。

---

## P0 — 句子翻译 Token 表关键缺失导致大量设置描述中英夹杂

`crates/ui/src/gearbox_text.rs` 的 `sentence_token_translation`（行 371–625）是设置描述自动翻译的核心。以下高频英文单词**完全缺失**，导致这些单词在中文翻译中保留为英文，形成刺眼的中英夹杂。

### 缺失的 Top 高频词汇

| 缺失单词 | 出现场景举例 | 当前翻译输出（错误） | 应有翻译 |
|----------|-------------|---------------------|----------|
| **`not`** | `"whether or not to"`, `"is not compatible"`, `"do not request"`, `"Cannot be offered with Zero Data Retention"` | 输出中出现英文 `not` | `"不"`, `"未"` |
| **`if`** | `"if the LS supports it"`, `"if the file is private"` | `"如果"` 应为 `"如果"` | `"如果"` |
| **`all`** | `"Highlight all occurrences"`, `"all of your current conversation"`, `"Trust all projects"` | 输出中出现英文 `all` | `"所有"` |
| **`never`** | `"Never"` (dropdown 选项) | `title_token_translation` 中无 `"Never"` → 退回英文 | `"从不"` |
| **`always`** | `"always show"` | 仅在 `title_token_translation` 中有 `"Always"`，但句子首字母小写时找不到 | `"始终"` |
| **`first`** | `"first implementation"` | 输出中出现英文 `first` | `"第一个"` 或 `"首次"` |
| **`over`** | `"scroll over the edge"` | 输出中出现英文 `over` | `"超过"` 或 `"在…上"` |
| **`during`** | `"during rendering"` | 输出中出现英文 `during` | `"在…期间"` |
| **`without`** | `"without having to give permission"` | 输出中出现英文 `without` | `"无需"` |
| **`than`** | `"more than one"` | 输出中出现英文 `than` | `"比"` |
| **`more`** | `"more lines"`, `"more features"` | 输出中出现英文 `more` | `"更多"` |
| **`less`** | `"less memory"` | 输出中出现英文 `less` | `"更少"` |
| **`both`** | `"for both horizontal and vertical"` | 输出中出现英文 `both` | `"同时"` 或 `"两者"` |
| **`some`** | `"some of its excerpts"` | 输出中出现英文 `some` | `"部分"` |
| **`other`** | `"other completion items"` | 输出中出现英文 `other` | `"其他"` |
| **`your`** | `"your selected theme"` | 输出中出现英文 `your` | `"您"` 或 `"你的"` |
| **`any`** | `"any number of"`, `"without requiring"` | 输出中出现英文 `any` | `"任何"` |

### 影响范围

所有走 `translate_setting_description` → `settings_sentence_translation` → `translate_sentence_fragment` 链路的设置描述都会受影响。出现 `not`/`if`/`all` 等词时，句子变成类似：

> 输出示例（当前）："Whether or not to automatically check for updates" → "是否 not 自动检查更新"
> 
> 应有输出："是否自动检查更新"

---

## P1 — 大量 UI 区域未接入翻译层

以下 crate 的大部分 Label/Button/Tooltip 文本是**纯英文**，未调用 `gearbox_translate_text` 也未使用 `GEARBOX_GUI` 检查。

### 1. `crates/agent_ui/src/` — Agent 面板（影响最大，100+ 条字符串）

整个 crate **完全没有导入 `gearbox_translate_text`**。包括：

- **Agent 面板**：`"Terminal"`, `"Agent"`, `"Open Global Rules"`, `"Open Project Rules"`, `"(AGENTS.md)"`
- **会话视图**（`thread_view.rs`）：`"Subagents Awaiting Permission:"`, `"Scroll to Subagent"`, `"Plan"`, `"Completed Plan"`, `"Edits"`, `"Change Thinking Effort"`, `"Cycle Thinking Effort"`, `"Queue and Send"`, `"Send Immediately"`, `"Context"`, `"Cost"`, `"Rules"`, `"Unavailable Editing"`, `"Subagent Output"`, `"Awaiting Confirmation"`, `"Run Command"`, `"Truncated"`, `"Network access"`, `"Write access"`, `"Runs without the OS sandbox"`, `"Reason from agent"`, `"Couldn't create a sandbox"`, `"Anthropic will retain inference logs."`, `"Review"`, `"Scroll"`, `"Clear All"`, `"Reject All"`, `"Keep All"`, `"Restore Checkpoint"`, `"Open File"`, `"Apply"`, `"Configure Provider"`, `"Select Model"`, `"Retry"`, `"New Thread"`, `"Upgrade"`, `"Authenticate"`, `"Open in WSL"`, `"Thinking"` 等
- **模式选择器**（`mode_selector.rs`）：`"Change Mode"`, `"Cycle Through Modes"`
- **配置画布**（`profile_selector.rs`）：`"Tools Unsupported"`, `"Change Profile"`, `"Cycle Through Profiles"`, `"Disabled in Restricted Mode"`, `"Configure"`, `"Restricted Mode"`
- **Agent diff 视图**（`agent_diff.rs`）：`"No changes to review"`, `"Continue Iterating"`, `"Reject All"`, `"Keep All"`
- **MCP / Context Server 配置**：`"Open Repository"`, `"Configure Server"`, `"Authenticate to connect this server"`, `"Authenticate"`, `"Submit"`, `"Authenticating…"`, `"Cancel"`
- **Profile 管理**：`"Customize"`, `"Custom Profiles"`, `"Add New Profile"`, `"Fork Profile"`, `"Configure Default Model"`, `"Configure Built-in Tools"`, `"Configure MCP Tools"`, `"Delete Profile"`, `"Go Back"`
- **付费升级弹窗**（`end_trial_upsell.rs`）：`"Pro"`, `"Upgrade to Zed Pro"`, `"Free"`, `"(Current Plan)"`, `"You've been automatically reset to the Free plan."`
- **沙箱提示**（`sandbox_status_tooltip.rs`）：`"You have sandboxing disabled in settings."`, `"Sandboxing is disabled for this thread"`, `"Sandboxing"`
- **模型选择器**（`model_selector_components.rs`）：`"Configure"`, `"Change Model"`, `"Cycle Favorite Models"`
- **通知**（`agent_notification.rs`）：`"View"`, `"Dismiss"`

### 2. `crates/collab_ui/src/` — 协作面板（40+ 条字符串）

- **参与者标签**（`collab_panel.rs`）：`"Follow {}"`, `"Calling"`, `"Leave Call"`, `"Guest"`, `"Mic only"`, `"Click to Follow"`, `"Open {}"`, `"Screen"`, `"Open Shared Screen"`, `"notes"`, `"Open Channel Notes"`, `"Add a Contact"`, `"Join Channel"`
- **右键菜单**：`"Grant Mic Access"`, `"Grant Write Access"`, `"Mute"`, `"Revoke Access"`, `"Invite {} to join"`, `"Call {}"`, `"Remove Contact"`, `"Clear Filter"`, `"Create Channel"`
- **频道管理**（`channel_modal.rs`）：`"Copy Link"`, `"Manage Members"`, `"Invite Members"`, `"Invited"`, `"Admin"`, `"Guest"`, `"Member"`, `"You"`
- **通话统计**（`call_stats_modal.rs`）：`"Call Diagnostics"`, `"Not in a call"`, `"Network"`

### 3. `crates/debugger_ui/src/` — 调试器面板（16+ 条字符串）

- **调试面板**（`debugger_panel.rs`）：`"New Session"`, `"Edit debug.json"`, `"Debugger Docs"`, `"Debugger Extensions"`, `"Breakpoints"`, `"No Breakpoints Set"`
- **新建进程**（`new_process_modal.rs`）：`"Edit in debug.json"`, `"Start"`, `"Program"`（输入框标签）, `"Working Directory"`（输入框标签）, `"Debugger:"`, `"Launch Custom"`, `"Evaluate"`

### 4. `crates/extensions_ui/src/` — 扩展面板（5+ 条字符串）

`"Incompatible"`, `"View Documentation"`, `"Enable Vim mode"`, `"Install Dev Extension"`, `"All"`, `"Overridden by dev extension."`

### 5. `crates/recent_projects/src/remote_servers.rs` — 远程服务器管理（9 条字符串）

**完全没有翻译函数**。`"Error Creating Dev Container:"`, `"Open Zed Log"`, `"Exit"`, `"Go Back"`, `"Creating Dev Container"`, `"Remove Distro"`, `"Copy Server Address"`, `"Remove Server"`, `"WSL:"`

### 6. `crates/ui/src/components/project_empty_state.rs`

`"Open Project"`, `"or"`, `"Clone Repository"` —— 以及多个调用点传入的面板名称（如 `"Agent Panel"`, `"Git Panel"`, `"Project Panel"`, `"Threads Sidebar"`）

### 7. `crates/workspace/src/security_modal.rs`

`"Restricted Mode prevents:"`, `"Stay in Restricted Mode"`, `"Trust and Continue"`

### 8. `crates/command_palette/src/` 残留英文

`"Change Keybinding…"`, `"Add Keybinding…"`, `"Run"`（直接 `Button::new` 未走翻译）

---

## P2 — 翻译质量 / 不一致问题

### 9. `settings_sentence_prefix` 部分 prefix 映射为无意义内容

文件：`crates/ui/src/gearbox_text.rs:131`

```rust
("The ", ""),  // 直接删掉 "The"，无中文替代
```

以 `The` 开头的句子会直接丢失冠词（中文不依赖冠词，影响较小），但语义可读性不受损。

### 10. 不同文件使用不同翻译模式

当前存在**三种**翻译模式：

| 模式 | 使用方 | 问题 |
|------|--------|------|
| `gearbox_translate_text()` (共享翻译层) | `ui/src/components` 基础组件, `settings_ui` fallback | ✅ 统一 |
| 自有的 `gearbox_label()` 函数 | `project_panel`, `recent_projects`, `file_finder`, `command_palette`, `onboarding` | ⚠️ 重复代码，但覆盖完整 |
| 裸 `GEARBOX_GUI` 三元检查 | `collab_ui`, `debugger_ui`, `settings_ui` 部分位置, `update_button` | ❌ 不一致，不易维护 |
| **无翻译** | `agent_ui`, `collab_ui` 大部分, `debugger_ui`, `remote_servers` | ❌ 完全缺失 |

### 11. `is_safe_brand_text` 子串匹配可能误替换

```rust
text.contains("Zed")  // 子串匹配
```

`"ZedGraph"` → `"GearboxGraph"`（虽为 fallback，但仍属不当）

### 12. `crates/agent_ui/src/agent_ui.rs` 中 `"Zed Agent"` 显示为 `"Agent"` 而非 `"Gearbox Agent"`

行 474-480：`Zed Agent` → `Agent`，但 Gearbox 品牌理应显示为 `Gearbox Agent`。

### 13. `"Upgrade to Zed Pro"` 未改成 `"Gearbox Pro"`

在 `crates/agent_ui/src/ui/end_trial_upsell.rs:36` 和 `gearbox_text.rs:1187`（exact_translation 中已正确翻译但 UI 中还有未翻译的原始文）

---

## 附录：典型中英夹杂输出示例

基于当前词表，未走 `exact_translation` 的长句子会输出类似：

| 输入英文 | 当前输出（中英夹杂） | 应有输出 |
|----------|-------------------|---------|
| `"Whether or not to automatically check for updates."` | `"是否 not 自动检查 updates。"` | `"是否自动检查更新。"` |
| `"How many lines to expand the multibuffer excerpts by default."` | `"如何 many lines 展开 the multibuffer excerpts 按 default。"` | `"默认展开多缓冲区摘录的行数。"` |
| `"Controls whether to use language servers to provide code intelligence."` | `"控制是否 to use language servers 提供代码智能能力。"` | `"控制是否使用语言服务器提供代码智能能力。"` |
| `"Whether the cursor blinks in the editor."` | `"是否 the cursor blinks in the editor。"` | `"编辑器中的光标是否闪烁。"` |

---

## 修复优先级

```
P0 - 句子级翻译（修复中英夹杂）：
  - sentence_token_translation 添加 "not"/"if"/"all"/"never"/"always" 等高频词
  - 补齐 title_token_translation 中的 "Never"/"First"/"Always"

P1 - 大面积缺失翻译的区域：
  - agent_ui/src/ 整体接入 gearbox_translate_text
  - collab_ui/src/ 协作面板
  - debugger_ui/src/ 调试器面板
  - extensions_ui/src/ 扩展面板
  - remote_servers.rs 远程服务器管理

P2 - 统一翻译模式：
  - 将各处 gearbox_label() 函数统一为 ui::gearbox_translate_text
  - 将裸 GEARBOX_GUI 三元检查替换为共享翻译层
  - 清理 "Zed Agent" → "Agent" 的品牌替换逻辑
```
