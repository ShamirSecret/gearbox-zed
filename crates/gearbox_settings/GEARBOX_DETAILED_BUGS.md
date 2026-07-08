# Gearbox 深度 Bug 报告（2026-07-08 第三轮）

> HEAD `e8f6cd122e`。本轮聚焦**非常具体的代码级缺陷**，包含重现步骤和修复方案。

---

## 修复审查结果（2026-07-08）

本报告的 P0/P1 主干判断成立，已按 MVP 风险优先级修复：

- 已修复 `title_translation()` 多 token 标题的中英粘连问题。实际实现不是简单保留源字符串空格，而是在 token 拼接时按 ASCII/非 ASCII 边界插入空格：中文 token 之间保持连写，例如 `Select Model` -> `选择模型`；中英混排保留空格，例如 `Git Panel` -> `Git 面板`、`Subagent Output` -> `子 Agent 输出`。
- 已修复 `is_gear_executable_goal()` 的中文标点归一化，并移除“长度 >= 12 即可执行”的宽松判定。Gear 现在只在请求含明确 action word 时启动 runtime，`你好。` 和长闲聊不会创建 `.gearbox-agent` 产物。
- 已修复配置 worker 命令后 `require_worker` 仍为 `false` 的问题。只要 `GEARBOX_GEAR_WORKER_COMMAND` 或 legacy `GEARBOX_OPENCODE_COMMAND` 生效，worker 失败就会阻止目标被静默视为完成。
- 已修复 `evaluate_goal()` 在 verification 通过但非必需 worker 失败/跳过时摘要不透明的问题；MVP 仍允许 verification 通过后 Complete，但 summary 会记录 worker status。
- 已移除 `generate_gear_coordinator_brief()` 中按 provider id 字符串 `"fake"` 跳过模型调用的生产逻辑，测试改为显式喂 fake model stream。
- 已补充报告列出的 agent/profile/collab 常用标题和精确短语翻译。
- 已修复 `crates/gearbox/build.rs` 中 `zed build.rs` 的可见诊断文案。
- `.github/workflows/gearbox_release.yml` 已检查；当前仅有一句说明 Gearbox 是 Zed fork 的上下文文字，不属于 workflow 名称或步骤名称，暂不修改。

暂缓项：

- `gear_workspace_for_session()` 多 worktree 排序一致性仍需结合真实多 worktree UI 行为单独验证，未在本轮扩大 runtime 路径。
- `close/closed/closing` 的上下文翻译差异属于低风险语言质量问题，当前译法不影响主要理解，未改。

---

## P0 — 功能缺陷

### 1. `title_translation()` 丢弃 token 间空格，导致中英粘连

**文件：** `crates/ui/src/gearbox_text.rs:695`

```rust
' ' => {}  // 空格被完全忽略
```

当 `title_translation` 处理多词标签时，token 之间的空格被丢弃，导致翻译结果粘连：

| 输入 | 当前输出（错误） | 应有输出 |
|------|----------------|---------|
| `"Select Model"` | `"Select模型"` | `"选择模型"` |
| `"Configure Provider"` | `"配置Provider"` | `"配置提供商"` |
| `"Clear All"` | `"Clear全部"` | `"清除全部"` |
| `"Change Mode"` | `"Change模式"` | `"切换模式"` |
| `"Open Global Rules"` | `"打开GlobalRules"` | `"打开全局规则"` |

**原因：** `title_translation` 的 `' ' => {}` 分支（行 695）跳过空格，而 `flush_title_token` 在 token 之间不插入分隔符。

**影响范围：** 所有调用 `Label::new("Multi Word Label")` 且未命中 `exact_translation` 的 UI 组件。影响遍及 agent_ui、collab_ui、debugger_ui、settings_ui。

**修复：**
```rust
' ' => { translated.push(' '); }
```

---

### 2. `is_gear_executable_goal()` 不处理中文标点，中文问候语带句号时被误判

**文件：** `crates/agent/src/agent.rs:2642-2646`

```rust
let normalized = request
    .trim_matches(|character: char| {
        character.is_whitespace() || character.is_ascii_punctuation()
    })
    .to_lowercase();
```

`trim_matches` 只 trim ASCII 标点（`!`、`?`、`.` 等），不 trim 中文标点（`。`、`？`、`！`、`，` 等）。

| 用户输入 | `normalized` | SMALL_TALK 匹配 | 结果 |
|----------|-------------|-----------------|------|
| `"你好"` | `"你好"` | 匹配 `"你好"` | ✔ 礼貌回应 |
| `"你好。"` | `"你好。"` | 不匹配（含句号） | ✘ 命中 `chars().count() >= 12`? 否→无操作。**实际无害只因长度<12** |
| `"你好吗？"` | `"你好吗？"` | 不匹配 | 长度=4<12，无 action word → OK |
| `"谢谢！"` | `"谢谢！"` | 不匹配 | 长度=4<12 → OK |

当前因中文句子通常较短（<12字符），实际影响有限。但如果用户输入 `"请问你能帮我修复这个bug吗？"`（18字符），`chars().count() >= 12` 为 true，即使含有"修复" action word，也会直接进入 orchestrator。这不是真正的 bug（因为用户确实表达了修复意图），但会导致 `is_gear_executable_goal` 对纯闲聊长句（如 `"我昨天看了一部很有趣的电影"`, 无 action word但长度>=12）返回 true。

**修复：** 在 `trim_matches` 中添加中文标点，或改用更精确的 NLP 判断。

---

### 3. `gear_worker_config_from_env()` 始终设置 `require_worker: false`，即使配置了 worker 命令

**文件：** `crates/agent/src/agent.rs:2803-2819`

```rust
WorkerConfig {
    worker_kind,
    worker_command,
    skip_worker: false,
    require_worker: false,  // 始终为 false
}
```

即使用户配置了 `GEARBOX_GEAR_WORKER_COMMAND`，`require_worker` 仍为 `false`。这意味着如果 worker 命令执行失败，`evaluate_goal` 不会阻止 goal 标记为 `Complete`（只要 verification 通过）。

**潜在影响：** 用户配置了自定义 worker 命令，worker 失败（exit code != 0），但验证命令巧合通过（或无验证命令），goal 报告为"成功完成"，用户不知道 worker 失败。

**修复：** 当 `worker_command` 为 `Some(_)` 时，应设置 `require_worker: true`。

---

### 4. `evaluate_goal()` 在 `require_worker=false` 且 verification_passed 时将 Failed worker 报告为 Complete

**文件：** `crates/gearbox_agent/src/runtime.rs:792-798`

```rust
if verification_passed {
    return GoalEvaluation {
        status: GoalStatus::Complete,
        should_continue: false,
        summary: format!("Goal completed after {iteration} Gear iteration(s)."),
    };
}
```

worker 状态为 `Failed` 但 verification 通过 → goal 标记为 `Complete`。用户看到"目标已完成"但 worker 实际上失败了。这是有意设计的 MVP 行为，但应记录在 summary 中（例如 "Worker failed but verification passed"）。

---

## P1 — 逻辑缺陷

### 5. `generate_gear_coordinator_brief()` 使用字符串 `"fake"` 检测测试环境

**文件：** `crates/agent/src/agent.rs:2579`

```rust
if model.provider_id().0.as_ref() == "fake" {
    return None;
}
```

脆弱的字符串比较。如果任何正式 provider 被命名为包含 "fake" 的 ID，会被错误跳过。应使用 feature gate 或 test 标志。

---

### 6. `is_gear_executable_goal()` 中 `request.chars().count() >= 12` 使用原始 `request` 而非 `normalized`

**文件：** `crates/agent/src/agent.rs:2694`

```rust
request.chars().count() >= 12  // 使用原始 request
    || ACTION_WORDS.iter().any(|action_word| normalized.contains(action_word))  // 使用 normalized
```

变量使用不一致。`request` 是未经 `trim_matches` 处理的原始输入，包含首尾空白和标点。应改为使用 `normalized`：

```rust
normalized.chars().count() >= 12
```

当前影响小（trim 通常只去掉少数字符），但语义错误。

---

### 7. `gear_workspace_for_session()` 在不同代码路径中使用不同类型的 worktree 排序

**文件：** `crates/agent/src/agent.rs:2940-2955`

```rust
// 路径 A：从 visible_worktrees 取第一个
let Some(worktree) = state.project.read(cx).visible_worktrees(cx).next() else {
    return Err(anyhow!("Gear requires an open local worktree"));
};

// 路径 B：从 work_dirs 路径列表中取第一个非空路径
let path = session.work_dirs.as_ref().and_then(|work_dirs| {
    work_dirs.paths().iter().find(|path| !path.as_os_str().is_empty())
});
```

路径 A 使用 `visible_worktrees()`（project 提供的默认 worktree 排序），路径 B 使用 `PathList` 中的顺序。如果用户有多个 worktree，这两个排序可能不同，导致 Gear 使用不同的工作目录。

---

### 8. `close` 和 `close` 在 `sentence_token_translation` 中缺乏区分，导致上下文翻译不准确

**文件：** `crates/ui/src/gearbox_text.rs:434-436`

```rust
"close" => "关闭",
"closed" => "关闭",
"closing" => "关闭",
```

这三个词的翻译完全一致，但 "close" 在动词和形容词上下文中有不同含义。例如 "close file"（关闭文件）vs "close button"（关闭按钮）——两者都译为"关闭"是正确的。但 `"close"` 也出现在设置描述中如 `"When to close the tab"`，句法翻译无法区分时态和词性，不影响主要理解。

---

## P2 — 翻译覆盖缺口

### 9. `agent_ui` 大量字符串未被 `title_token_translation` 覆盖，导致英文原型保留

以下常用 UI 标签词不在 `title_token_translation` 表中，在 `title_translation` 处理后仍保留英文：

| 缺失词 | 出现位置示例 | 当前输出 |
|--------|------------|---------|
| `"Change"` | "Change Mode" | "Change模式" |
| `"Select"` | "Select Model" | "Select模型" |
| `"Clear"` | "Clear All" | "Clear全部" |
| `"Reject"` | "Reject All" | "Reject全部" |
| `"Keep"` | "Keep All" | "Keep全部" |
| `"Restore"` | "Restore Checkpoint" | "恢复Checkpoint" |
| `"Rules"` | "Open Global Rules" | "打开GlobalRules" |
| `"Provider"` | "Configure Provider" | "配置Provider" |
| `"Subagent"` | "Subagent Output" | "SubagentOutput" |
| `"Permission"` | "Subagents Awaiting Permission" | "SubagentsAwaitingPermission" |
| `"Awaiting"` | "Awaiting Confirmation" | "Awaiting确认" |
| `"Checkpoint"` | "Restore Checkpoint" | "恢复Checkpoint" |
| `"Terminal"` | (在句子翻译中) | 已在title_token_translation中有"Terminal" → "终端" |
| `"Retry"` | "Retry" | 已在exact_translation中有"Retry" → "重试" |

**需要添加到 `title_token_translation`：**
```rust
"Change" => "切换",    // 或 "更改"
"Select" => "选择",
"Clear" => "清除",
"Reject" => "拒绝",
"Keep" => "保留",
"Checkpoint" => "检查点",
"Provider" => "提供商",
"Permission" => "权限",
"Awaiting" => "等待中",
"Rules" => "规则",
"Subagent" => "子 Agent",
```

---

### 10. `exact_translation` 中缺少以下常见设置/UI 标签

| 英文 | 位置 |
|------|------|
| `"Add New Profile"` | agent_ui profile 管理 |
| `"Custom Profiles"` | agent_ui profile 管理 |
| `"Delete Profile"` | agent_ui profile 管理 |
| `"Fork Profile"` | agent_ui profile 管理 |
| `"Open Repository"` | MCP 配置 |
| `"Call Diagnostics"` | collab_ui |
| `"Not in a call"` | collab_ui |
| `"Audio"` | collab_ui |
| `"Calls"` | collab_ui 设置 |

---

## P3 — 资源/配置问题

### 11. `build.rs` 中 eprintln 输出仍引用 `zed`

**文件：** `crates/gearbox/build.rs:17`

```rust
eprintln!("zed build.rs: {lib} not found in pkg-config's path");
```

应改为 `gearbox build.rs:`。

### 12. `gearbox_release.yml` 工作流中的名称

**文件：** `.github/workflows/gearbox_release.yml`

应检查在 workflow 的名称和步骤名称中是否仍包含 `zed` 字样。

---

## 修复优先级

```
P0：
  1. title_translation 空格丢失 → 行 695
  2. is_gear_executable_goal 中文标点 → 行 2642-2646
  3. require_worker 始终 false → 行 2803-2819

P1：
  4. evaluate_goal Failed+pass 报告 Complete
  5. "fake" 字符串比较 → 行 2579
  6. request vs normalized 不一致 → 行 2694
  7. worktree 排序不一致 → 行 2940-2955
  8. title_token_translation 缺失词
  9. exact_translation 缺失 UI 标签

P2：
  10. build.rs 残留 "zed" → 行 17
  12. HighlightedLabel 不翻译（已知限制）
```
