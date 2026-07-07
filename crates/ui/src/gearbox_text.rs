use gpui::SharedString;

pub fn translate(text: impl Into<SharedString>) -> SharedString {
    let text = text.into();
    if std::env::var("GEARBOX_GUI").as_deref() != Ok("1") {
        return text;
    }

    if let Some(translated) = exact_translation(text.as_ref()) {
        return SharedString::new_static(translated);
    }

    if let Some(translated) = title_translation(text.as_ref()) {
        return translated;
    }

    if is_safe_brand_text(text.as_ref()) {
        return text.as_ref().replace("Zed", "Gearbox").into();
    }

    text
}

fn is_safe_brand_text(text: &str) -> bool {
    text.contains("Zed")
        && !text.contains('/')
        && !text.contains('\\')
        && !text.contains(':')
        && !text.contains('@')
        && text.len() <= 120
}

fn title_translation(text: &str) -> Option<SharedString> {
    if !is_safe_title_text(text) {
        return None;
    }

    let mut translated = String::new();
    let mut token = String::new();
    let mut translated_any = false;

    for character in text.chars() {
        if character.is_ascii_alphanumeric() {
            token.push(character);
            continue;
        }

        flush_title_token(&mut translated, &mut token, &mut translated_any)?;

        match character {
            ' ' => {}
            '&' => translated.push_str("和"),
            '/' => translated.push('/'),
            '-' => {
                if !translated.ends_with(' ') {
                    translated.push(' ');
                }
                translated.push('-');
                translated.push(' ');
            }
            '(' | ')' => translated.push(character),
            _ => return None,
        }
    }

    flush_title_token(&mut translated, &mut token, &mut translated_any)?;

    if translated_any && !translated.is_empty() {
        Some(translated.trim().into())
    } else {
        None
    }
}

fn is_safe_title_text(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 90
        && text.chars().any(|character| character.is_ascii_alphabetic())
        && text.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || character.is_ascii_whitespace()
                || matches!(character, '&' | '/' | '-' | '(' | ')')
        })
}

fn flush_title_token(
    translated: &mut String,
    token: &mut String,
    translated_any: &mut bool,
) -> Option<()> {
    if token.is_empty() {
        return Some(());
    }

    let translated_token = title_token_translation(token.as_str())?;
    if translated_token != token {
        *translated_any = true;
    }
    translated.push_str(translated_token);
    token.clear();
    Some(())
}

fn title_token_translation(token: &str) -> Option<&'static str> {
    Some(match token {
        "About" => "关于",
        "Actions" => "操作",
        "Active" => "活动",
        "Add" => "添加",
        "Advanced" => "高级",
        "Agent" => "Agent",
        "AI" => "AI",
        "Allowed" => "允许",
        "And" => "",
        "Anthropic" => "Anthropic",
        "Appearance" => "外观",
        "Audio" => "音频",
        "Auto" => "自动",
        "Autoclose" => "自动闭合",
        "Autoscroll" => "自动滚动",
        "Background" => "背景",
        "Base" => "基础",
        "Behavior" => "行为",
        "Beyond" => "超出",
        "Blink" => "闪烁",
        "Bookmarks" => "书签",
        "Border" => "边框",
        "Box" => "框",
        "Branch" => "分支",
        "Breakpoints" => "断点",
        "Breadcrumbs" => "面包屑",
        "Brackets" => "括号",
        "Buffer" => "缓冲区",
        "Buffers" => "缓冲区",
        "Calls" => "通话",
        "Channel" => "频道",
        "Click" => "点击",
        "Clicks" => "点击",
        "Clipboard" => "剪贴板",
        "Close" => "关闭",
        "Closing" => "关闭",
        "Code" => "代码",
        "Collection" => "收集",
        "Collaboration" => "协作",
        "Colorize" => "着色",
        "Colors" => "颜色",
        "Column" => "列",
        "Columns" => "列",
        "Comment" => "注释",
        "Completions" => "补全",
        "Configure" => "配置",
        "Content" => "内容",
        "Context" => "上下文",
        "Contrast" => "对比度",
        "Control" => "控制",
        "Controls" => "控件",
        "Cursor" => "光标",
        "Cursors" => "光标",
        "Custom" => "自定义",
        "Data" => "数据",
        "Dark" => "深色",
        "Debounce" => "防抖",
        "Debug" => "调试",
        "Debugger" => "调试器",
        "Debuggers" => "调试器",
        "Default" => "默认",
        "Definition" => "定义",
        "Delay" => "延迟",
        "Diagnostics" => "诊断",
        "Diff" => "Diff",
        "Digraphs" => "二合字母",
        "Disable" => "禁用",
        "Display" => "显示",
        "Document" => "文档",
        "Double" => "双击",
        "Drag" => "拖拽",
        "Drop" => "放置",
        "Duration" => "持续时间",
        "Edit" => "编辑",
        "Editing" => "编辑",
        "Editor" => "编辑器",
        "Edits" => "编辑",
        "Emacs" => "Emacs",
        "Enabled" => "启用",
        "Excerpt" => "摘录",
        "Expand" => "展开",
        "External" => "外部",
        "Fallback" => "回退",
        "Fallbacks" => "回退字体",
        "Fast" => "快速",
        "Feature" => "功能",
        "Fetch" => "获取",
        "File" => "文件",
        "Files" => "文件",
        "Flags" => "开关",
        "Folds" => "折叠",
        "Font" => "字体",
        "Format" => "格式化",
        "Formatting" => "格式化",
        "General" => "通用",
        "Git" => "Git",
        "Global" => "全局",
        "Go" => "跳转",
        "Guides" => "辅助线",
        "Gutter" => "边栏",
        "Height" => "高度",
        "Help" => "帮助",
        "Helix" => "Helix",
        "Hide" => "隐藏",
        "Highlight" => "高亮",
        "Highlights" => "高亮",
        "Hiding" => "隐藏",
        "Hints" => "提示",
        "History" => "历史",
        "Horizontal" => "水平",
        "Hover" => "悬停",
        "Icon" => "图标",
        "Image" => "图片",
        "Inactive" => "非活动",
        "Include" => "包含",
        "Indent" => "缩进",
        "Indentation" => "缩进",
        "Inline" => "行内",
        "Insert" => "插入",
        "Instrumentation" => "性能诊断",
        "Integration" => "集成",
        "Item" => "项",
        "JSX" => "JSX",
        "Key" => "键",
        "Keybindings" => "按键绑定",
        "Keymap" => "快捷键方案",
        "Language" => "语言",
        "Languages" => "语言",
        "Last" => "最后",
        "Layout" => "布局",
        "Lens" => "透镜",
        "Light" => "浅色",
        "Line" => "行",
        "Lines" => "行",
        "Linked" => "联动",
        "LLM" => "LLM",
        "LSP" => "LSP",
        "Markdown" => "Markdown",
        "Margin" => "边距",
        "Max" => "最大",
        "MCP" => "MCP",
        "Menu" => "菜单",
        "Metrics" => "指标",
        "Middle" => "中键",
        "Min" => "最小",
        "Minimum" => "最小",
        "Minimap" => "缩略图",
        "Modal" => "模态",
        "Mode" => "模式",
        "Model" => "模型",
        "Modeline" => "模式行",
        "Modified" => "已修改",
        "Mouse" => "鼠标",
        "Move" => "移动",
        "Multibuffer" => "多缓冲区",
        "Name" => "名称",
        "Network" => "网络",
        "Newline" => "换行",
        "No" => "无",
        "Normal" => "普通",
        "Number" => "数字",
        "Numbers" => "行号",
        "On" => "在",
        "Options" => "选项",
        "Order" => "顺序",
        "Other" => "其他",
        "Outlines" => "大纲",
        "Output" => "输出",
        "Padding" => "内边距",
        "Panel" => "面板",
        "Panels" => "面板",
        "Parameter" => "参数",
        "Parser" => "解析器",
        "Paste" => "粘贴",
        "Path" => "路径",
        "Picker" => "选择器",
        "Plugins" => "插件",
        "Popover" => "弹出层",
        "Predictions" => "预测",
        "Prefer" => "优先",
        "Prettier" => "Prettier",
        "Preview" => "预览",
        "Privacy" => "隐私",
        "Profiler" => "性能分析器",
        "Profiles" => "配置档案",
        "Project" => "项目",
        "Providers" => "提供商",
        "Quick" => "快速",
        "Ranges" => "范围",
        "Rebase" => "变基",
        "Regex" => "正则",
        "Relative" => "相对",
        "Rendering" => "渲染",
        "Replace" => "替换",
        "Restore" => "恢复",
        "Restoration" => "恢复",
        "Results" => "结果",
        "Retention" => "保留",
        "Review" => "评审",
        "Rounded" => "圆角",
        "Runnables" => "可运行项",
        "Sandbox" => "沙箱",
        "Save" => "保存",
        "Scopes" => "作用域",
        "Scroll" => "滚动",
        "Scrollbar" => "滚动条",
        "Scrolling" => "滚动",
        "Search" => "搜索",
        "Security" => "安全",
        "Selected" => "选中",
        "Selection" => "选择",
        "Selections" => "选择",
        "Semantic" => "语义",
        "Sensitivity" => "灵敏度",
        "Server" => "服务器",
        "Servers" => "服务器",
        "Settings" => "设置",
        "Shape" => "形状",
        "Show" => "显示",
        "Signature" => "签名",
        "Size" => "大小",
        "Skill" => "技能",
        "Skills" => "技能",
        "Smartcase" => "智能大小写",
        "Snippet" => "代码片段",
        "Sort" => "排序",
        "Space" => "空格",
        "Split" => "拆分",
        "Startup" => "启动",
        "Sticky" => "固定",
        "Strategy" => "策略",
        "Substitution" => "替换",
        "Support" => "支持",
        "Symbol" => "符号",
        "System" => "系统",
        "Tab" => "制表符",
        "Tabs" => "标签页",
        "Target" => "目标",
        "Tasks" => "任务",
        "Telemetry" => "遥测",
        "Terminal" => "终端",
        "Text" => "文本",
        "Theme" => "主题",
        "Themes" => "主题",
        "Thumb" => "滑块",
        "Timeout" => "超时",
        "Tokens" => "令牌",
        "Toolbar" => "工具栏",
        "Tool" => "工具",
        "Tools" => "工具",
        "Toggle" => "切换",
        "Type" => "类型",
        "UI" => "UI",
        "Unsaved" => "未保存",
        "Update" => "更新",
        "Use" => "使用",
        "User" => "用户",
        "Variables" => "变量",
        "Version" => "版本",
        "Vertical" => "垂直",
        "View" => "视图",
        "Viewer" => "查看器",
        "Vim" => "Vim",
        "Visual" => "可视",
        "Warnings" => "警告",
        "Wheel" => "滚轮",
        "Whitespace" => "空白符",
        "Whitespaces" => "空白符",
        "Width" => "宽度",
        "Window" => "窗口",
        "With" => "",
        "Words" => "词语",
        "Workspace" => "工作区",
        "Wrap" => "换行",
        "Wrapping" => "换行",
        "Yank" => "复制",
        "Zoom" => "缩放",
        "key" => "键",
        "milliseconds" => "毫秒",
        _ => return None,
    })
}

fn exact_translation(text: &str) -> Option<&'static str> {
    Some(match text {
        "Open" => "打开",
        "Close" => "关闭",
        "Cancel" => "取消",
        "Save" => "保存",
        "Save As" => "另存为",
        "Delete" => "删除",
        "Remove" => "移除",
        "Rename" => "重命名",
        "Copy" => "复制",
        "Cut" => "剪切",
        "Paste" => "粘贴",
        "Duplicate" => "复制副本",
        "Undo" => "撤销",
        "Redo" => "重做",
        "Create" => "创建",
        "New" => "新建",
        "Add" => "添加",
        "Edit" => "编辑",
        "Apply" => "应用",
        "Reset" => "重置",
        "Retry" => "重试",
        "Refresh" => "刷新",
        "Reload" => "重新加载",
        "Install" => "安装",
        "Uninstall" => "卸载",
        "Update" => "更新",
        "Download" => "下载",
        "Upload" => "上传",
        "Search" => "搜索",
        "Filter" => "筛选",
        "Configure" => "配置",
        "Settings" => "设置",
        "Preferences" => "偏好设置",
        "Extensions" => "扩展",
        "Terminal" => "终端",
        "Project" => "项目",
        "Projects" => "项目",
        "Workspace" => "工作区",
        "File" => "文件",
        "Files" => "文件",
        "Folder" => "文件夹",
        "Folders" => "文件夹",
        "Window" => "窗口",
        "Windows" => "窗口",
        "Help" => "帮助",
        "About" => "关于",
        "Actions" => "操作",
        "More" => "更多",
        "Back" => "返回",
        "Next" => "下一步",
        "Previous" => "上一步",
        "Continue" => "继续",
        "Done" => "完成",
        "OK" => "确定",
        "Yes" => "是",
        "No" => "否",
        "Enabled" => "已启用",
        "Disabled" => "已禁用",
        "Loading" => "正在加载",
        "Running" => "正在运行",
        "Stopped" => "已停止",
        "Error" => "错误",
        "Warning" => "警告",
        "Info" => "信息",
        "Success" => "成功",
        "Failed" => "失败",
        "Unknown" => "未知",
        "None" => "无",
        "Default" => "默认",
        "Custom" => "自定义",
        "System" => "跟随系统",
        "Light" => "浅色",
        "Dark" => "深色",
        "Left" => "左侧",
        "Right" => "右侧",
        "Top" => "顶部",
        "Bottom" => "底部",
        "Up" => "上方",
        "Down" => "下方",
        "Today" => "今天",
        "Yesterday" => "昨天",
        "Modified" => "已修改",
        "Created" => "已创建",
        "Deleted" => "已删除",
        "Active" => "活动",
        "Inactive" => "非活动",
        "User" => "用户",
        "Project Settings" => "项目设置",
        "User Settings" => "用户设置",
        "Open Settings" => "打开设置",
        "Open Keymap" => "打开快捷键配置",
        "Open Folder" => "打开文件夹",
        "Open File" => "打开文件",
        "Open Project" => "打开项目",
        "Open Recent" => "打开最近项目",
        "Open in New Window" => "在新窗口打开",
        "Open in This Window" => "在当前窗口打开",
        "New Window" => "新窗口",
        "New File" => "新建文件",
        "New Folder" => "新建文件夹",
        "Close Window" => "关闭窗口",
        "Close Tab" => "关闭标签页",
        "Close All" => "全部关闭",
        "Find" => "查找",
        "Find in Files" => "在文件中查找",
        "Replace" => "替换",
        "Replace All" => "全部替换",
        "Command Palette" => "命令面板",
        "Open Command Palette" => "打开命令面板",
        "Recent Projects" => "最近项目",
        "No matches" => "没有匹配项",
        "No results" => "没有结果",
        "No Results" => "没有结果",
        "No file selected" => "未选择文件",
        "No project opened" => "未打开项目",
        "Search files" => "搜索文件",
        "Search projects" => "搜索项目",
        "Search settings" => "搜索设置",
        "Search extensions" => "搜索扩展",
        "Search symbols" => "搜索符号",
        "Show in Finder" => "在 Finder 中显示",
        "Show in File Manager" => "在文件管理器中显示",
        "Reveal in Project Panel" => "在项目面板中显示",
        "Copy Path" => "复制路径",
        "Copy Relative Path" => "复制相对路径",
        "Copy Link" => "复制链接",
        "Copy Permalink" => "复制永久链接",
        "Copy Message" => "复制消息",
        "Copy Thread" => "复制会话",
        "Paste from Clipboard" => "从剪贴板粘贴",
        "Move to Trash" => "移到回收站",
        "Restore" => "恢复",
        "Sign In" => "登录",
        "Sign Out" => "退出登录",
        "Log Out" => "退出登录",
        "Reauthenticate" => "重新认证",
        "Account" => "账户",
        "Manage" => "管理",
        "Manage Profiles" => "管理配置档案",
        "Manage Skills" => "管理技能",
        "Profiles" => "配置档案",
        "Skills" => "技能",
        "Tools" => "工具",
        "Agent" => "Agent",
        "Agent Panel" => "Agent 面板",
        "Zed Agent" => "Gearbox Agent",
        "Gearbox Agent" => "Gearbox Agent",
        "Selected Agent" => "已选择 Agent",
        "External Agents" => "外部 Agent",
        "Add More Agents" => "添加更多 Agent",
        "Current Thread" => "当前会话",
        "New Thread" => "新会话",
        "Open Thread" => "打开会话",
        "Open Thread as Markdown" => "以 Markdown 打开会话",
        "Regenerate Thread Title" => "重新生成会话标题",
        "Edit Thread Title" => "编辑会话标题",
        "Thread copied to clipboard" => "会话已复制到剪贴板",
        "Thread loaded from clipboard" => "已从剪贴板加载会话",
        "No active thread" => "没有活动会话",
        "No active native thread to copy" => "没有可复制的活动原生会话",
        "Open a project to load a thread" => "打开项目后加载会话",
        "No clipboard content available" => "剪贴板没有可用内容",
        "Clipboard does not contain text" => "剪贴板不包含文本",
        "Terminal Panel" => "终端面板",
        "New Terminal" => "新建终端",
        "Kill Terminal" => "终止终端",
        "Restart Terminal" => "重启终端",
        "Clear Terminal" => "清空终端",
        "Git" => "Git",
        "Git Panel" => "Git 面板",
        "Commit" => "提交",
        "Commit Changes" => "提交更改",
        "Stage" => "暂存",
        "Unstage" => "取消暂存",
        "Stage All" => "全部暂存",
        "Unstage All" => "全部取消暂存",
        "Restore File" => "恢复文件",
        "Discard Changes" => "丢弃更改",
        "Pull" => "拉取",
        "Push" => "推送",
        "Fetch" => "获取",
        "Branch" => "分支",
        "Branches" => "分支",
        "Checkout" => "检出",
        "Merge" => "合并",
        "Rebase" => "变基",
        "Stash" => "贮藏",
        "Diff" => "Diff",
        "History" => "历史",
        "View History" => "查看历史",
        "Diagnostics" => "诊断",
        "Problems" => "问题",
        "Debugger" => "调试器",
        "Debug" => "调试",
        "Run" => "运行",
        "Stop" => "停止",
        "Restart" => "重启",
        "Step Over" => "单步跳过",
        "Step Into" => "单步进入",
        "Step Out" => "单步跳出",
        "Breakpoints" => "断点",
        "Language Server" => "语言服务器",
        "Language Servers" => "语言服务器",
        "Format" => "格式化",
        "Format Document" => "格式化文档",
        "Code Actions" => "代码操作",
        "Go to Definition" => "跳转到定义",
        "Go to Declaration" => "跳转到声明",
        "Go to Implementation" => "跳转到实现",
        "Go to References" => "跳转到引用",
        "Rename Symbol" => "重命名符号",
        "Markdown Preview" => "Markdown 预览",
        "Preview" => "预览",
        "Toggle Preview" => "切换预览",
        "Install Extension" => "安装扩展",
        "Uninstall Extension" => "卸载扩展",
        "Enable Extension" => "启用扩展",
        "Disable Extension" => "禁用扩展",
        "Themes" => "主题",
        "Theme" => "主题",
        "Icon Theme" => "图标主题",
        "Font" => "字体",
        "Zoom In" => "放大",
        "Zoom Out" => "缩小",
        "Reset Zoom" => "重置缩放",
        "Release Notes" => "发行说明",
        "Licenses" => "许可证",
        "Documentation" => "文档",
        "Report a Bug" => "报告 Bug",
        "Request a Feature" => "请求功能",
        "Submit Feedback" => "提交反馈",
        "Start Free Trial" => "开始免费试用",
        "Manage Subscription" => "管理订阅",
        "Upgrade to Pro" => "升级到 Pro",
        "Checking for Updates" => "正在检查更新",
        "Downloading Update" => "正在下载更新",
        "Installing Update" => "正在安装更新",
        "Search settings..." => "搜索设置...",
        "Search settings…" => "搜索设置…",
        "Last Session" => "上次会话",
        "Blank Workspace" => "空白工作区",
        "Welcome Page" => "欢迎页",
        "Prompt" => "提示",
        "Always" => "始终",
        "Never" => "从不",
        "Ask" => "询问",
        "All" => "全部",
        "On" => "开启",
        "Off" => "关闭",
        "Collect timing data for foreground and background executor tasks so they can be inspected via `zed: open performance profiler`. May lead to increased memory usage." => {
            "收集前台和后台执行器任务的耗时数据，便于通过性能分析器检查。可能增加内存占用。"
        }
        "What to do when using the 'close active item' action with no tabs." => {
            "没有标签页时执行“关闭当前项目”动作的处理方式。"
        }
        "What to do when the last window is closed." => "最后一个窗口关闭时的处理方式。",
        "Use native OS dialogs for 'Open' and 'Save As'." => {
            "打开和另存为时使用操作系统原生对话框。"
        }
        "Use native OS dialogs for confirmations." => "确认操作时使用操作系统原生对话框。",
        "Hide the values of variables in private files." => "隐藏私密文件中的变量值。",
        "Globs to match against file paths to determine if a file is private." => {
            "用于匹配文件路径并判断文件是否为私密文件的 glob 规则。"
        }
        "How `zed <path>` opens directories when no flag is specified." => {
            "`gearbox <path>` 未指定参数时打开目录的方式。"
        }
        "How projects open from the UI by default." => "从界面打开项目时的默认方式。",
        "When opening Zed, avoid Restricted Mode by auto-trusting all projects, enabling use of all features without having to give permission to each new project." => {
            "打开 Gearbox 时自动信任所有项目，避免进入受限模式，无需为每个新项目单独授权即可使用全部功能。"
        }
        "Whether or not to restore unsaved buffers on restart." => "重启后是否恢复未保存的缓冲区。",
        "What to restore from the previous session when opening Zed." => {
            "打开 Gearbox 时从上一次会话中恢复哪些内容。"
        }
        "Which settings should be activated only in Preview build of Zed." => {
            "哪些设置只在 Gearbox Preview 构建中启用。"
        }
        "Any number of settings profiles that are temporarily applied on top of your existing user settings." => {
            "可配置任意数量的设置配置档案，临时叠加在现有用户设置之上。"
        }
        "Send debug information like crash reports." => "发送崩溃报告等调试信息。",
        "Send anonymized usage data like what languages you're using Zed with." => {
            "发送匿名使用数据，例如你在 Gearbox 中使用哪些语言。"
        }
        "Allow sending requests to Anthropic models that cannot be offered with Zero Data Retention." => {
            "允许向无法提供零数据保留的 Anthropic 模型发送请求。"
        }
        "Whether or not to automatically check for updates." => "是否自动检查更新。",
        "Choose a static, fixed theme or dynamically select themes based on appearance and light/dark modes." => {
            "选择固定主题，或根据外观和浅色/深色模式动态选择主题。"
        }
        "The name of your selected theme." => "当前选择的主题名称。",
        "Choose whether to use the selected light or dark theme or to follow your OS appearance configuration." => {
            "选择使用指定的浅色/深色主题，或跟随操作系统外观配置。"
        }
        "The theme to use when mode is set to light, or when mode is set to system and it is in light mode." => {
            "模式为浅色时使用的主题；系统模式处于浅色时也使用该主题。"
        }
        "The theme to use when mode is set to dark, or when mode is set to system and it is in dark mode." => {
            "模式为深色时使用的主题；系统模式处于深色时也使用该主题。"
        }
        "The custom set of icons Zed will associate with files and directories." => {
            "Gearbox 用于关联文件和目录的自定义图标集。"
        }
        "The name of your selected icon theme." => "当前选择的图标主题名称。",
        "Choose whether to use the selected light or dark icon theme or to follow your OS appearance configuration." => {
            "选择使用指定的浅色/深色图标主题，或跟随操作系统外观配置。"
        }
        "The icon theme to use when mode is set to light, or when mode is set to system and it is in light mode." => {
            "模式为浅色时使用的图标主题；系统模式处于浅色时也使用该图标主题。"
        }
        "The icon theme to use when mode is set to dark, or when mode is set to system and it is in dark mode." => {
            "模式为深色时使用的图标主题；系统模式处于深色时也使用该图标主题。"
        }
        "Font family for editor text." => "编辑器文本使用的字体族。",
        "Font size for editor text." => "编辑器文本使用的字号。",
        "Font weight for editor text (100-900)." => "编辑器文本使用的字重 (100-900)。",
        "Line height for editor text." => "编辑器文本使用的行高。",
        "Custom line height value (must be at least 1.0)." => "自定义行高值，必须至少为 1.0。",
        "The OpenType features to enable for rendering in text buffers." => {
            "在文本缓冲区渲染时启用的 OpenType 特性。"
        }
        "The font fallbacks to use for rendering in text buffers." => "文本缓冲区渲染使用的回退字体。",
        "Font family for UI elements." => "界面元素使用的字体族。",
        "Font size for UI elements." => "界面元素使用的字号。",
        "Font weight for UI elements (100-900)." => "界面元素使用的字重 (100-900)。",
        "The OpenType features to enable for rendering in UI elements." => {
            "界面元素渲染时启用的 OpenType 特性。"
        }
        "The font fallbacks to use for rendering in the UI." => "界面渲染使用的回退字体。",
        "Font size for agent response text in the agent panel. Falls back to the regular UI font size." => {
            "Agent 面板中回复文本使用的字号，未设置时回退到常规界面字号。"
        }
        "Font size for user messages text in the agent panel." => "Agent 面板中用户消息文本使用的字号。",
        "Font family for the markdown preview. Falls back to the UI font family." => {
            "Markdown 预览使用的字体族，未设置时回退到界面字体族。"
        }
        "Font family for code blocks in the markdown preview. Falls back to the editor font family." => {
            "Markdown 预览中代码块使用的字体族，未设置时回退到编辑器字体族。"
        }
        "Font size for the markdown preview. Falls back to the editor font size." => {
            "Markdown 预览使用的字号，未设置时回退到编辑器字号。"
        }
        "The text rendering mode to use." => "要使用的文本渲染模式。",
        "Modifier key for adding multiple cursors." => "添加多个光标使用的修饰键。",
        "Whether the cursor blinks in the editor." => "编辑器中的光标是否闪烁。",
        "Cursor shape for the editor." => "编辑器中的光标形状。",
        "When to hide the mouse cursor." => "何时隐藏鼠标光标。",
        "How much to fade out unused code (0.0 - 0.9)." => "未使用代码的淡化程度 (0.0 - 0.9)。",
        "How to highlight the current line." => "如何高亮当前行。",
        "Highlight all occurrences of selected text." => "高亮选中文本的所有出现位置。",
        "Whether the text selection should have rounded corners." => "文本选择区域是否使用圆角。",
        "The minimum APCA perceptual contrast to maintain when rendering text over highlight backgrounds." => {
            "在高亮背景上渲染文本时需要保持的最小 APCA 感知对比度。"
        }
        "Show wrap guides (vertical rulers)." => "显示换行辅助线（垂直标尺）。",
        "Character counts at which to show wrap guides." => "显示换行辅助线的字符列数。",
        "The name of a base set of key bindings to use." => "要使用的基础快捷键方案名称。",
        "Enable Vim mode and key bindings." => "启用 Vim 模式和快捷键。",
        "Enable Helix mode and key bindings." => "启用 Helix 模式和快捷键。",
        "When to auto save buffer changes." => "何时自动保存缓冲区变更。",
        "Save after inactivity period (in milliseconds)." => "无操作达到指定时间后保存（毫秒）。",
        "Display the which-key menu with matching bindings while a multi-stroke binding is pending." => {
            "多键快捷键等待后续输入时，显示匹配按键绑定的 which-key 菜单。"
        }
        "Delay in milliseconds before the which-key menu appears." => "which-key 菜单显示前的延迟（毫秒）。",
        "What to do when multibuffer is double-clicked in some of its excerpts." => {
            "在多缓冲区摘录中双击时的处理方式。"
        }
        "How many lines to expand the multibuffer excerpts by default." => "默认展开多缓冲区摘录的行数。",
        "How many lines of context to provide in multibuffer excerpts by default." => {
            "多缓冲区摘录默认提供的上下文行数。"
        }
        "Default depth to expand outline items in the current file." => "当前文件中大纲项默认展开深度。",
        "How to display diffs in the editor." => "编辑器中显示 diff 的方式。",
        "Whether the editor will scroll beyond the last line." => "编辑器是否允许滚动超过最后一行。",
        "The number of lines to keep above/below the cursor when auto-scrolling." => {
            "自动滚动时在光标上下方保留的行数。"
        }
        "The number of characters to keep on either side when scrolling with the mouse." => {
            "使用鼠标滚动时在两侧保留的字符数。"
        }
        "Scroll sensitivity multiplier for both horizontal and vertical scrolling." => {
            "水平和垂直滚动的灵敏度倍数。"
        }
        "Whether to zoom the editor font size with the mouse wheel while holding the primary modifier key." => {
            "按住主修饰键并滚动鼠标滚轮时，是否缩放编辑器字号。"
        }
        "Fast scroll sensitivity multiplier for both horizontal and vertical scrolling." => {
            "水平和垂直快速滚动的灵敏度倍数。"
        }
        "Whether to scroll when clicking near the edge of the visible text area." => {
            "点击可见文本区域边缘附近时是否滚动。"
        }
        "Whether to stick scopes to the top of the editor" => "是否将作用域固定在编辑器顶部",
        "Automatically show a signature help pop-up." => "自动显示签名帮助弹窗。",
        "Show the signature help pop-up after completions or bracket pairs are inserted." => {
            "补全或括号对插入后显示签名帮助弹窗。"
        }
        "Determines how snippets are sorted relative to other completion items." => {
            "决定代码片段相对于其他补全项的排序方式。"
        }
        "Show the informational hover box when moving the mouse over symbols in the editor." => {
            "鼠标移到编辑器符号上时显示信息悬停框。"
        }
        "Time to wait in milliseconds before showing the informational hover box." => {
            "显示信息悬停框前等待的时间（毫秒）。"
        }
        "Whether the hover popover sticks when the mouse moves toward it, allowing interaction with its contents." => {
            "鼠标移向悬停弹层时是否保持显示，以便与其中内容交互。"
        }
        "Time to wait in milliseconds before hiding the hover popover after the mouse moves away." => {
            "鼠标移开后隐藏悬停弹层前等待的时间（毫秒）。"
        }
        "Enable drag and drop selection." => "启用拖拽选择。",
        "Delay in milliseconds before drag and drop selection starts." => "拖拽选择开始前的延迟（毫秒）。",
        "Show line numbers in the gutter." => "在边栏中显示行号。",
        "Show runnable buttons in the gutter." => "在边栏中显示可运行按钮。",
        "Show breakpoints in the gutter." => "在边栏中显示断点。",
        "Show bookmarks in the gutter." => "在边栏中显示书签。",
        "Show code folding controls in the gutter." => "在边栏中显示代码折叠控件。",
        "Minimum number of characters to reserve space for in the gutter." => {
            "边栏中为行号预留的最小字符数。"
        }
        "Show code action button at start of buffer line." => "在缓冲区行首显示代码操作按钮。",
        "When to show the scrollbar in the editor." => "何时在编辑器中显示滚动条。",
        "Show cursor positions in the scrollbar." => "在滚动条中显示光标位置。",
        "Show Git diff indicators in the scrollbar." => "在滚动条中显示 Git diff 指示器。",
        "Show buffer search result indicators in the scrollbar." => "在滚动条中显示缓冲区搜索结果指示器。",
        "Show selected text occurrences in the scrollbar." => "在滚动条中显示选中文本出现位置。",
        "Show selected symbol occurrences in the scrollbar." => "在滚动条中显示选中符号出现位置。",
        "Which diagnostic indicators to show in the scrollbar." => "在滚动条中显示哪些诊断指示器。",
        "When false, forcefully disables the horizontal scrollbar." => "为 false 时强制禁用水平滚动条。",
        "When false, forcefully disables the vertical scrollbar." => "为 false 时强制禁用垂直滚动条。",
        "When to show the minimap in the editor." => "何时在编辑器中显示缩略图。",
        "Where to show the minimap in the editor." => "在编辑器中的哪个位置显示缩略图。",
        "When to show the minimap thumb." => "何时显示缩略图滑块。",
        "Border style for the minimap's scrollbar thumb." => "缩略图滚动条滑块的边框样式。",
        "How to highlight the current line in the minimap." => "如何在缩略图中高亮当前行。",
        "Maximum number of columns to display in the minimap." => "缩略图中显示的最大列数。",
        "Show breadcrumbs." => "显示面包屑导航。",
        "Show quick action buttons (e.g., search, selection, editor controls, etc.)." => {
            "显示快速操作按钮（例如搜索、选择、编辑器控件等）。"
        }
        "Show the selections menu in the editor toolbar." => "在编辑器工具栏中显示选择菜单。",
        "Show agent review buttons in the editor toolbar." => "在编辑器工具栏中显示 Agent 评审按钮。",
        "Show code action buttons in the editor toolbar." => "在编辑器工具栏中显示代码操作按钮。",
        "The default mode when Vim starts." => "Vim 启动时的默认模式。",
        "Toggle relative line numbers in Vim mode." => "在 Vim 模式中切换相对行号。",
        "Controls when to use system clipboard in Vim mode." => "控制 Vim 模式中何时使用系统剪贴板。",
        "Enable smartcase searching in Vim mode." => "在 Vim 模式中启用智能大小写搜索。",
        "Duration in milliseconds to highlight yanked text in Vim mode." => {
            "Vim 模式中高亮已复制文本的持续时间（毫秒）。"
        }
        "Use regex search by default in Vim search." => "Vim 搜索默认使用正则搜索。",
        "Whether edit predictions are shown in normal mode. By default, edit predictions are only shown in insert and replace modes." => {
            "普通模式下是否显示编辑预测。默认只在插入和替换模式下显示编辑预测。"
        }
        "Cursor shape for normal mode." => "普通模式的光标形状。",
        "Cursor shape for insert mode. Inherit uses the editor's cursor shape." => {
            "插入模式的光标形状。继承模式使用编辑器的光标形状。"
        }
        "Cursor shape for replace mode." => "替换模式的光标形状。",
        "Cursor shape for visual mode." => "可视模式的光标形状。",
        "Custom digraph mappings for Vim mode." => "Vim 模式的自定义二合字母映射。",
        "A mapping from languages to files and file extensions that should be treated as that language." => {
            "语言到文件和文件扩展名的映射，用于判断文件应按哪种语言处理。"
        }
        "Which level to use to filter out diagnostics displayed in the editor." => {
            "用于过滤编辑器中显示诊断信息的级别。"
        }
        "Whether to show warnings or not by default." => "默认是否显示警告。",
        "Whether to show diagnostics inline or not." => "是否以内联方式显示诊断。",
        "The delay in milliseconds to show inline diagnostics after the last diagnostic update." => {
            "最后一次诊断更新后显示内联诊断的延迟（毫秒）。"
        }
        "The amount of padding between the end of the source line and the start of the inline diagnostic." => {
            "源码行末尾与内联诊断起始位置之间的内边距。"
        }
        "The minimum column at which to display inline diagnostics." => "显示内联诊断的最小列位置。",
        "Whether to pull for language server-powered diagnostics or not." => {
            "是否拉取由语言服务器提供的诊断。"
        }
        "Minimum time to wait before pulling diagnostics from the language server(s)." => {
            "从语言服务器拉取诊断前等待的最短时间。"
        }
        _ => return None,
    })
}
