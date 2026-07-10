# Gearbox GUI 入口 crate

## 概述

Gearbox 应用进程的入口点。启动 `main()`、初始化 GPUI 窗口、加载设置、注册面板和 action，并把上游 zed crate 的初始化逻辑组合到 Gearbox 的启动路径中。

## 结构

| 文件 | 用途 |
|------|------|
| `main.rs` | `main()` 入口。设置 `GEARBOX_GUI=1`，初始化路径(`~/.local/share/gearbox`)，加载设置(`gearbox_settings::load`)，构建 Application 实例 |
| `zed.rs` | 核心模块：`initialize_workspace()`、`init()`、面板注册（AgentPanel/ProjectPanel 等）、action 绑定、窗口选项(`build_window_options`) |
| `zed/app_menus.rs` | 全中文应用菜单：文件、编辑、选择、视图、跳转、运行、窗口、帮助 |
| `zed/open_listener.rs` | URL/open request 解析（`zed://` 协议处理） |
| `zed/open_url_modal.rs` | 打开 URL 的模态框 |
| `zed/quick_action_bar.rs` | 快速操作栏 |
| `zed/edit_prediction_registry.rs` | 编辑预测注册 |
| `zed/telemetry_log.rs` | 遥测日志查看器 |
| `zed/remote_debug.rs` | 远程调试支持 |
| `zed/migrate.rs` | 设置/快捷键迁移 |
| `zed/mac_only_instance.rs` | macOS 单实例锁 |
| `zed/windows_only_instance.rs` | Windows 单实例 |
| `reliability.rs` | 崩溃上报、内存用量日志、minidump 上传 |
| `reliability/hang_detection.rs` | 卡死检测 |
| `visual_test_runner.rs` | macOS 视觉回归测试 runner |
| `build.rs` | 构建脚本：rpath、Git SHA、图标、Windows conpty |

## 查看重点

- `main()` 第 208 行: 设置 `GEARBOX_GUI=1`，决定独立数据目录和应用品牌
- `main()` 第 519 行: `gearbox_settings::load` 挂载独立设置资源
- `zed/app_menus.rs`: 全部中文菜单项，品牌字符串指向 `github.com/ShamirSecret/gearbox-zed`
- `Cargo.toml` 第 278-308 行: bundle 元数据，使用 `dev.gearbox.*` 标识符
- `build.rs` 第 222 行: 按 release channel 选择图标资源

## 约定

- 数据目录: `~/.local/share/gearbox`（非上游的 `~/.local/share/zed`）
- 设置资源: 通过 `gearbox_settings::load` 加载，不用 `zed_settings::load`
- 错误提示指向 Gearbox 品牌的故障排除页面
- `GEARBOX_GUI=1` 环境变量在 main() 开头强制设置，其他 crate 用此变量做条件判断
- 窗口选项、崩溃处理中的 binary 名均为 `gearbox`

## 命令

```
cargo run -p gearbox
cargo check -p gearbox
cargo test -p gearbox
```

## 反模式

- 不要修改 `.omo/` 目录内容
- 不要直接编辑共享源码而不先读 `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`
- 不要为消除 `Zed` 字样重命名内部类型名、action、enum variant 或 keymap context
