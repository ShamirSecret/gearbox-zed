# Gearbox

Gearbox 是一个面向中文用户的本地优先代码编辑器 GUI。它基于开源 Rust/GPUI 编辑器架构改造，目标是在保留高性能编辑体验、项目管理、终端、搜索、调试和 AI 辅助能力的同时，提供中文界面、独立品牌和独立资源层。


## 项目定位

Gearbox 的定位是：给中文用户使用的高性能本地开发工作台。

它适合这些场景：

- 希望获得现代代码编辑器体验，但更偏好中文界面。
- 希望使用 Rust/GPUI 原生 GUI，而不是浏览器套壳。
- 希望编辑器具备项目管理、搜索、终端、Git、调试、语言服务器和 AI 辅助能力。

## Gearbox 的优点

### 1. 原生高性能 GUI

Gearbox 继承 GPUI 原生界面架构，界面不是 WebView 套壳。它面向桌面端高响应交互，适合大型项目、频繁文件切换、快速搜索和长时间编辑。

### 2. Rust 核心和工程化基础

项目核心由 Rust 编写，具备清晰的类型系统、并发模型和工程组织。对需要长期维护的桌面开发工具来说，Rust 的可靠性和可维护性是重要基础。

### 3. GPU 加速渲染

GUI 渲染路径使用 GPU 能力。正常驱动和图形环境下，界面滚动、输入、切换和绘制应保持较好的流畅度。如果系统退回软件渲染，性能会明显下降，需要优先检查显卡驱动、Vulkan/Wayland/X11 环境。

### 4. 完整编辑器能力

Gearbox 保留现代代码编辑器的关键能力：

- 多项目和工作区管理。
- 文件树、最近项目、快速打开文件。
- 全局搜索、文件内搜索、命令面板。
- 终端、任务、调试、Git 面板。
- 语言服务器、语法高亮、格式化和补全。
- AI/Agent 面板和模型 Provider 配置。

### 5. 中文优先体验

Gearbox 的目标不是简单改一个标题，而是逐步把真实使用路径中的英文界面替换成中文，包括：

- 启动和欢迎页。
- 菜单栏和常用操作。
- 项目面板、最近项目、文件查找。
- 设置页、设置项说明、主题和字体选择器。
- Agent/AI 相关入口和提示。
- 打包元数据、桌面文件和用户可见说明。

### 6. 独立品牌和资源层

Gearbox 使用自己的名称、图标和说明。用户可见的产品文案应使用 Gearbox，而不是暴露上游品牌。

内部代码中仍可能保留一些历史类型名、协议名或兼容性标识，例如某些 action、enum、keymap context 或服务协议名称。这些不应为了表面重命名而盲目修改，只有确认不会破坏功能、编译和上游合并路径时才改。

### 7. 尽量不污染上游路径

Gearbox 的原则是：能放在 Gearbox 独立层的，不改共享核心；必须改共享源码的，尽量用 `GEARBOX_GUI=1` 条件分支控制。

这样做的好处是：

- 原 Zed 行为保持不变。
- Gearbox 可以继续合并上游更新。
- 共享源码改动可审计、可回放、可迁移。
- 后续重构时能清楚区分“上游能力”和“Gearbox 产品层”。

## 当前结构

- `crates/gearbox`：Gearbox GUI 入口、应用菜单、打包资源和品牌相关代码。
- `crates/gearbox_settings`：Gearbox 专用设置、快捷键、默认资源和同步说明。
- `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`：记录所有为了 Gearbox 在共享源码中做过的改动，合并上游或修改核心代码前必须查看。
- `crates/settings_ui`：部分 Gearbox 条件汉化显示层位于这里。
- `crates/onboarding`、`crates/workspace`、`crates/project_panel`、`crates/file_finder` 等：包含少量 `GEARBOX_GUI=1` 条件文案。

## 构建与运行

安装 Rust 和系统依赖后，可以在仓库根目录运行：

```bash
cargo run -p gearbox
```

只做编译检查：

```bash
cargo check -p gearbox
```

如果修改了设置、Agent、AI Provider 或上下文服务相关代码，建议使用更完整的检查：

```bash
cargo check -p settings_ui -p agent_ui -p ui -p language_models -p context_server -p gearbox
```

## 上游同步流程

本仓库保留两个远程：

- `origin`：Gearbox 自己的远程仓库。
- `upstream`：上游开源编辑器仓库。

同步上游的一般流程：

```bash
git fetch upstream main
git merge upstream/main
cargo check -p gearbox
git push
```

如果合并时出现冲突，优先查看：

```bash
crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md
```

这个文件记录了 Gearbox 修改过哪些共享源码、为什么改、哪些地方是条件汉化、哪些内部标识暂时不能改。

## 修改核心代码的规则

修改 Gearbox 相关代码时，优先级如下：

1. 优先改 `crates/gearbox` 和 `crates/gearbox_settings`。
2. 如果必须改共享源码，优先使用 `GEARBOX_GUI=1` 条件分支。
3. 修改共享源码后，必须更新 `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`。
4. 不要为了消除字符串中的 `Zed` 盲目改内部类型名、协议名、enum variant、keymap context 或测试 fixture。
5. 用户可见文案应使用 Gearbox；内部兼容标识可以暂时保留，但应在同步说明中记录。


## 许可证

本项目继承上游开源许可证结构，主要代码遵循 GPL-3.0-or-later，部分组件按其所在目录或文件声明使用 Apache-2.0 等许可证。

第三方依赖的许可证信息需要保持完整。新增 crate 或依赖时，应同步维护许可证配置，确保 CI 和发行包可以正确生成许可证说明。

## 当前状态

Gearbox 已经具备独立 GUI crate、独立设置资源层、中文 README、部分中文界面和上游同步记录。后续工作重点是继续补齐深层界面汉化、减少用户可见品牌残留、完善发行资源，并持续验证上游合并路径。
