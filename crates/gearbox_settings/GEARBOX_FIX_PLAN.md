# Gearbox Fork Deep Bug Hunt

> 检查 HEAD `f3217b05eb`。第一轮报告中的 P0 bug 已修复（`translate_sentence_fragment` 已改用 `localize_sentence_punctuation`；`worker_prompt` 错误已传播）。以下为**第二轮深入审计**发现的全部问题。

---

## P0 — 运行时行为异常

### 1. `settings_sentence_prefix` 中 3 个 prefix 映射到空字符串，导致关键语义丢失

**文件：** `crates/ui/src/gearbox_text.rs:125-131`

```rust
("Amount of ", ""),               // 行 125
("Files or globs of files that will be ", ""),  // 行 129
("A mapping from ", ""),          // 行 131
```

当这些前缀被匹配时，整个前缀从中文输出中**消失**。例如：
- `"Amount of timeout before showing a warning"` → `"timeout before showing a warning"` → 中文输出 `"timeout 之前 show 一个 warning"`（丢失了"数量"含义）
- `"A mapping from languages to files..."` → `"languages to files..."` → 中文丢失"映射自"含义

**修复：** 将空串替换为有意义的中文等价物：
- `"Amount of "` → `"数量： "` 或直接保留英文
- `"Files or globs of files that will be "` → `"将被处理的文件："`
- `"A mapping from "` → `"从···映射："`

---

### 2. `is_phrase_comma` 不处理缩写逗号（如 `e.g.`），会错误替换

**文件：** `crates/ui/src/gearbox_text.rs:289-300`

```rust
fn is_phrase_comma(characters: &[(usize, char)], index: usize) -> bool {
    let previous = previous_character(characters, index);
    let next = next_character(characters, index);
    if previous.is_some_and(|character| character.is_ascii_digit())
        && next.is_some_and(|character| character.is_ascii_digit())
    {
        return false;  // 跳过数字中的逗号，如 1,000
    }
    next.is_none_or(|character| character.is_whitespace())
}
```

输入 `"e.g., search"`：逗号在 `g` 之后，空格之前 → 会被替换为中文逗号 `，`，输出变成 `"e.g， search"`。

设置描述中存在 `e.g.`、`i.e.`、`etc.` 时均受影响。`is_sentence_period` 也有类似问题：缩写结尾句点后跟空格或句尾时也会被替换。

**修复方案：** 在 `is_phrase_comma` 中添加 `previous == 'g'` 或 `previous == 'c'` 等启发式规则，或检测两字母缩写模式 `\w\.` 后的逗号。在 `is_sentence_period` 中添加白名单模式。

---

### 3. `gear_worker_config_from_env()` 无效值静默回退到 `Opencode`

**文件：** `crates/agent/src/agent.rs:2563-2580`

```rust
let worker_kind = std::env::var("GEARBOX_GEAR_WORKER")
    .ok()
    .and_then(|worker| WorkerKind::parse(&worker))
    .unwrap_or_default();  // 静默回退到 Opencode
```

如果用户设置 `GEARBOX_GEAR_WORKER=invalid`，不会收到任何错误。Worker 以 `Opencode` 运行，用户不知自己的配置被忽略。

**修复方案：** 添加 `log::warn!` 或返回 `Result<WorkerConfig>`。

---

## P1 — 逻辑 / 设计问题

### 4. `cancel()` 对 Gear session 同时调用 `token.cancel()` 和 `thread.cancel()`，后者无用且可能干扰

**文件：** `crates/agent/src/agent.rs:3024-3037`

Gear session 的取消只依赖 `CancellationToken`，`thread.cancel()` 对 Gear 不生效（Gear 不使用 thread 的 prompt 处理路径）。但对所有 session 类型都执行 `thread.cancel(cx).detach()`，该调用会触发 Thread 内部状态重置，虽然对 Gear 无实质影响，但在调试时造成困惑：cancel 事件流中出现两次中断信号。

**修复方案：** 在 `cancel()` 中识别 Gear session（检查 `gear_cancellation_token` 是否存在），仅调用 `token.cancel()` 并跳过 `thread.cancel()`。

---

### 5. Gear 测试混用 `FakeFs` 和真实文件系统，构成维护风险

**文件：** `crates/agent/src/agent.rs:4252-4306`

```rust
let workspace = tempfile::tempdir().unwrap();                    // 真实 FS
let fs = FakeFs::new(cx.executor());
fs.insert_tree("/", json!({ "a": {} })).await;
let project = Project::test(fs.clone(), [Path::new("/a")], cx).await;
// ...
connection.clone().new_session(project.clone(), PathList::new(&[workspace.path()]), cx)
```

`Project` 的 worktree 指向 FakeFS 的 `/a`，但传入 `new_session` 的 `PathList` 使用真实临时目录。Orchestrator 直接在真实 FS 上运行（`temp_dir.path()`）。意味着 Project 的 FS 层完全不参与 Gear 操作——测试这样通过是因为 Gear 忽略 `project` API。

若未来 Orchestrator 开始通过 `project` 读取文件或验证，测试会意外失败。

**修复方案：** 至少让 workspace 路径与 FakeFS 一致，或添加注释说明此限制。

---

### 6. `run_raw_git` 不继承父进程环境变量

**文件：** `crates/gearbox_agent/src/tools.rs:220-239`

```rust
fn run_raw_git(workspace: &Path, args: &[&str]) -> Result<ShellCommandResult> {
    let output = Command::new("git")     // 没有设置环境变量
```

对比 `run_shell_command_with_env_and_cancellation` 显式传递 env：`git_snapshot` 调用的 `run_raw_git` 未传递任何 env。在需要 `SSH_AUTH_SOCK`、`GIT_SSH_COMMAND`、`GIT_DIR` 的环境中可能失败。

**修复方案：** 继承当前进程环境（`process.env("KEY", "VALUE")` 对每个需要传递的 key，或 `process.env_clear(false)` 保留父进程环境）。

---

### 7. `WorkerRegistry` 名不副实——没有注册功能，`WorkerAdapter` trait 未用于动态分发

**文件：** `crates/gearbox_agent/src/workers.rs:116-126`

```rust
#[derive(Default)]
pub struct WorkerRegistry;
impl WorkerRegistry {
    pub fn run(&self, request: WorkerRunRequest<'_>) -> Result<WorkerResult> {
        CommandWorker { kind: request.config.worker_kind }.run(request)
    }
}
```

`WorkerAdapter` trait 已定义（行 111-113），但只有 `CommandWorker` 一个实现，且 `WorkerRegistry` 总是硬编码使用它。对于涉及多个 Worker 类型的架构来说这是死代码/过度设计。

**修复方案：** 要么删除 `WorkerAdapter` trait，要么实现真正的注册表（`HashMap<WorkerKind, Box<dyn WorkerAdapter>>`）。

---

## P2 — 代码质量 / 一致性

### 8. `ZED_*` 环境变量在 Gearbox 二进制中未改为 `GEARBOX_*` 前缀

**文件：** `crates/gearbox/src/main.rs`, `crates/gearbox/src/zed.rs`

以下环境变量仍使用 `ZED_` 前缀：

| 变量 | 文件 | 行 |
|------|------|----|
| `ZED_EXPERIMENTAL_A11Y` | main.rs | 78 |
| `ZED_BUNDLE` | main.rs | 282, 428 |
| `ZED_BUILD_ID` | main.rs | 307 |
| `ZED_COMMIT_SHA` | main.rs | 309 |
| `ZED_STATELESS` | main.rs | 360 |
| `ZED_GENERATE_MINIDUMPS` | main.rs | 387 |
| `ZED_WINDOW_DECORATIONS` | zed.rs | 345 |
| `ZED_ALLOW_EMULATED_GPU` | zed.rs | 704 |

用户设置 `GEARBOX_ALLOW_EMULATED_GPU=1` 不会生效，必须用 `ZED_ALLOW_EMULATED_GPU=1`。增加用户困惑。

**修复方案：** 对运行时环境变量增加 `GEARBOX_*` 别名（fallback 到 `ZED_*`）。构建时环境变量（`ZED_BUNDLE`、`ZED_COMMIT_SHA`）可保留不变。

---

### 9. `is_safe_brand_text` 使用子串匹配 `"Zed"`，可能误替换包含 "Zed" 的专有名词

**文件：** `crates/ui/src/gearbox_text.rs:57-64`

```rust
fn is_safe_brand_text(text: &str) -> bool {
    text.contains("Zed")  // 子串匹配
```

`"ZedGraph"` → `"GearboxGraph"`、`"ZedBoard"` → `"GearboxBoard"`。作为精确翻译的最后一个 fallback，影响较小，但仍可能在罕见场景中产生不当替换。

**修复方案：** 使用正则 `\bZed\b` 或检查前后字符是否为非字母（类似 `is_sentence_period` 的做法）。

---

### 10. `ZED_REPL_DOCUMENTATION` 常量名仍为 `ZED` 前缀

**文件：** `crates/gearbox/src/zed/quick_action_bar/repl_menu.rs:19`

```rust
const ZED_REPL_DOCUMENTATION: &str = "https://github.com/ShamirSecret/gearbox-zed";
```

值已指向 gearbox 仓库，但常量名仍为 `ZED_`。属于微小不一致。

---

## P3 — 翻译质量 / 测试

### 11. `flush_sentence_token` 丢弃原文大小写

**文件：** `crates/ui/src/gearbox_text.rs:248-259`

Token 翻译查找小写版本。若命中翻译表（如 `"Should" → "应"），原文大写被丢弃；若未命中，保留原文（保持大小写）。因此句子开头 `"Should"` → `"应"` 而非 `"Should" → "应"`（大小写不应影响翻译结果，但首字母大小写在中文中无意义，实为无害）。

### 12. 测试未覆盖环境变量回退路径

**文件：** `crates/agent/src/agent.rs`（`gear_worker_config_from_env` 行 2563、`gear_verification_commands_from_env` 行 2582）

这两个函数是 Gear 功能与外部 Worker 集成的关键入口，但没有单元测试。在环境变量设置/不设置/无效值等各种场景下的行为未经验证。

---

## 实施优先级建议

```
迭代 1（P0）：修复 settings_sentence_prefix 空串问题、缩写逗号/句点替换问题
迭代 2（P1）：worker_config 无效值警告、cancel() 去重、测试修复、run_raw_git 环境变量
迭代 3（P2）：GEARBOX_* 环境变量别名、is_safe_brand_text 词边界、WorkerRegistry 精简
迭代 4（P3）：常量命名、翻译测试补充
```
