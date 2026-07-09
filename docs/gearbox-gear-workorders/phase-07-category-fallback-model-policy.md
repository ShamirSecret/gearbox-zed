# Phase 07：Category、fallback、provider/model policy

## 目标

把 route policy 从 worker-kind MVP 升级为 provider/model-aware policy：category 解析有类型化失败、fallback 链按配置长度重试、跳过 no-op/unreachable，模型元数据安全扫描，category prompt_append 可注入。

## 主要文件

- `crates/gearbox_agent/src/runtime.rs`
- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/cli.rs`
- `crates/agent/src/agent.rs`

## 具体工单

1. 扩展 `CategoryResolution`：
   - `prompt_append: Option<String>`
   - `available_categories: Vec<String>`
   - `nearest_fallback: Option<FallbackRoute>`
   - `fallback_chain: Vec<FallbackRoute>`
   - `tools: WorkerToolPolicy`
   - `nearest_fallback` 只能表示下一条不同的可尝试 fallback route；没有不同 route 时必须是 `None`。
   - 当前 route 由 `worker_category` / `worker_model` / `route_reason` 记录，不通过 `nearest_fallback` 回填。
2. 新增 `CategoryResolutionResult`：
   - `Resolved`
   - `Disabled`
   - `NotFound`
   - `ModelUnavailable`
3. 错误结果必须包含：
   - requested category
   - available categories
   - attempted provider/model
   - nearest fallback
   - 如果 provider/model unavailable 但没有不同 fallback，错误结果写 attempted provider/model，同时 `nearest_fallback: none`。
4. 新增 secret-like model field scan：
   - 字段名标准化：去掉非字母数字，转小写。
   - 阻止字段名包含 `apikey`、`authorization`、`bearertoken`、`clientsecret`、`password`、`privatekey`、`secret`、`secretkey`、`token` 等。
   - 扫描发生在写 ledger/worker packet/review prompt 前。
5. fallback chain 改造：
   - chain item 是 `{ providers: Vec<String>, model: String, variant: Option<String> }`。
   - `has_more_fallbacks` 使用 chain 长度，不再只用 `MAX_SAME_FAILURE_RETRIES=2`。
   - budget 仍可单独限制 premium call。
6. no-op fallback 检测：
   - provider id case-insensitive 比较。
   - model id canonicalize：`.` 和 `-` 等价，转小写。
   - fallback 候选与当前 provider/model 相同则跳过并写 artifact。
7. `nearest_fallback` 语义修正：
   - `category_resolution_for_route()` 不允许用 selected route 作为 `nearest_fallback` 的 fallback value。
   - `CategoryResolutionResult` / worker packet / coordinator review input / GUI artifact 要直接展示 `nearest_fallback`，而当前 route 继续由 `worker_category` / `worker_model` / `route_reason` 提供。
   - `model_unavailable` / `no_fallback` 报告不能暗示还有当前 route 可作为下一步 fallback。
8. unreachable provider skip：
   - provider registry snapshot 中不可用的 provider/model 不启动 worker。
   - 写 `skipped_unreachable_provider` attempt。
9. provider/model transform artifact：
   - 记录 previous provider/model/session。
   - 记录 failed provider/model。
   - 记录 next provider/model。
   - 输入 ReviewEngine。
10. category prompt_append：
   - 支持静态 append。
   - 支持根据 model/provider 动态 append。
   - 用户配置 append 与内置 append 拼接。
11. tool policy：
   - worker 默认 `question: false`。
   - worker 默认不能递归创建 Gear task，除非显式允许。
   - write worker 与 review/explore worker 使用不同工具策略。

## 测试

1. `category_not_found_lists_available_categories`
2. `disabled_category_returns_disabled_result`
3. `model_unavailable_returns_nearest_fallback`
4. `secret_like_model_field_is_rejected_before_packet_write`
5. `fallback_skips_noop_provider_model`
6. `fallback_attempt_count_follows_chain_length`
7. `prompt_append_combines_builtin_and_user_append`
8. `worker_tool_policy_disables_question_by_default`
9. `model_unavailable_without_distinct_fallback_has_no_nearest_fallback`

## 验收

- route 失败不再只有泛化 `Unavailable`。
- fallback artifact 能说明跳过了哪些 provider/model 以及为什么。
- category prompt 能根据任务类别改变 worker 指令。
- 不会把疑似 API key 的 model metadata 写进 artifacts 或 review prompt。
- 没有下一条不同 fallback 时，review input 和 GUI 不会把当前 route 展示成 `nearest_fallback`。

## 当前状态

- 已完成：`CategoryRouter` 现在会把 category-scoped `prompt_append` 和 `WorkerToolPolicy` 一起注入 `WorkerPacket`，并允许 `GEARBOX_GEAR_WORKER_PROMPT_APPEND` 作为用户附加说明与 builtin append 合并。
- 已完成：`category_resolution_for_route()` 现在会把 `CategoryResolution` / `CategoryResolutionResult` 一起写进 worker packet 和 coordinator review 输入，review 能直接看到 requested/available category、nearest fallback 和 attempted provider/model。
- 已完成：worker prompt 和 coordinator review prompt 都接了 sanitized model metadata block，`sanitize_model_fields()` 现在真正用于写入前清洗，而不是只停留在单测里。
- 已完成：fallback retry 现在会写 `workers/<task_id>/route-transform-*.md` artifact，并在 `goal-review-iteration-*.md` 里引用 fallback history，包含前后 attempt / provider / model / session / decision。
- 已完成：`queue_next_attempt()` 已把 no-op fallback 升级为共享 route identity canonicalization，比对 `worker_kind` / `worker_model` / `worker_command` 时会把 provider id 统一小写、model punctuation 归一，避免同一 provider/model 的写法差异导致重复回到同一 route。
- 已完成：`model_unavailable_error_for_task()` 也复用了同一套 provider/model canonicalization，因此 GUI / TaskManager / workers 对不可用 route 的判断现在是一致的。
- 已完成：`category_resolution_for_route()` 现在只返回真正的下一条不同 fallback route；如果没有不同候选，`nearest_fallback` 就是 `None`，不会再回填当前 route。
- 这一阶段已经和计划里的 provider/model policy 主线对齐；后续更高层的 review / budget / stagnation 融合继续放到 Phase 08。
