# OMO vs Gear 细粒度对比 - Part 7: Category Resolution

> 文件：`packages/senpi-task/src/category/` — resolver.ts、types.ts、fallback-chains.ts

---

## 7.1 五种解析结果

### OMO `CategoryResolutionResult`

```typescript
type CategoryResolutionResult =
  | { kind: "resolved";           spec; config; modelSelection; availableCategories }
  | { kind: "disabled";           reason; availableCategories }
  | { kind: "not_found";          availableCategories }
  | { kind: "model_unavailable";  attemptedModel?; availableModels; nearestFallback?; fallbackEntry? }
```

**四种失败类型的区分：**

| 结果 | 含义 | 用户看到 |
|------|------|---------|
| `disabled` | 用户在配置中显式禁用了此 category | "Category X is disabled by config" |
| `not_found` | category 名称不存在 | "Category X not found. Available: quick, deep..." |
| `model_unavailable` | category 存在但无可用模型 | "Model Y is unavailable for category X. Nearest fallback: Z" |
| `resolved` | 成功 | 直接使用 |

### Gear `FallbackDecision`

```rust
enum FallbackDecision {
    NextRoute { route },
    NoFallbackRoute,
    RepeatedFailureLimit,
    Unavailable { reason, failure_kind },
}
```

**差异：** Gear 没有 `Disabled` 或 `NotFound` 状态。category 解析失败只通过 `FallbackDecision::Unavailable` 表达。

---

## 7.2 模型安全扫描

### OMO

```typescript
// resolver.ts:49-67
const SECRET_LIKE_MODEL_FIELD_NAMES = new Set([
    "accesstoken", "apikey", "auth", "authorization",
    "bearertoken", "clientsecret", "password", "privatekey",
    "privatetoken", "secret", "secretkey", "token",
]);
function hasSecretLikeModelField(model): boolean {
    return Object.getOwnPropertyNames(model).some(key =>
        SECRET_LIKE_MODEL_FIELD_NAMES.has(normalizeModelFieldName(key))
    );
}
```

属性名标准化：去除所有非字母数字字符、转小写。所以 `API_Key` → `apikey` → 匹配。

**`isSenpiModelPort()`** 使用此扫描：如果 model 对象有看起来像机密的字段，整个 model 被拒绝。目的是阻止模型元数据中的 API key 泄漏到 worker session 中。

### Gear: 无安全扫描

`CoordinatorModel`（provider_id, model_id, name）直接写入 goal ledger 和 worker packets。如果配置错误导致 provider_id 包含 API key，会泄漏。

---

## 7.3 Fallback Chain 结构

### OMO

```typescript
// fallback-chains.ts
// 每条记录：{ providers: string[], model: string, variant?: string }
// providers 是多 provider 优先序列表
"quick": [
    { providers: ["openai", "github-copilot", "opencode", "vercel"], model: "gpt-5.4-mini" },
    { providers: ["anthropic", "github-copilot", "vercel"], model: "claude-haiku-4-5" },
    { providers: ["google", "github-copilot", "opencode", "vercel"], model: "gemini-3-flash" },
    { providers: ["opencode-go", "vercel"], model: "minimax-m3" },
    { providers: ["minimax-coding-plan", "minimax-cn-coding-plan"], model: "MiniMax-M3" },
    { providers: ["opencode-go", "vercel"], model: "minimax-m2.7" },
    { providers: ["opencode", "vercel"], model: "gpt-5-nano" },
],
```

**Key 设计点：**
- `providers` 是优先级列表 —— 按顺序尝试 provider
- 链长度 = 最大重试次数（`hasMoreFallbacks`）
- 每个 fallback 是 `(providers[], model, variant)` 三元组，不是简单的 model 字符串
- provider 可达性检查：只尝试已连接的 provider

### Gear

```rust
// 当前：MAX_SAME_FAILURE_RETRIES = 2（常数）
// 与 fallback chain 长度无关
```

**差异：** Gear 的重试次数固定为 2，不随 fallback chain 长度变化。`WorkerSequence` 可以定义 route 链，但 fallback 的重试次数是常数。

---

## 7.4 `prompt_append` — 模型感知的 category 指令注入

### OMO

```typescript
// resolver.ts:139-148
function promptAppendForCategory(categoryName, model, userPromptAppend) {
    const basePromptAppend =
        CATEGORY_PROMPT_APPEND_RESOLVERS[categoryName]?.(model)  // 动态解析器（接收 model 参数）
        ?? CATEGORY_PROMPT_APPENDS[categoryName]                  // 静态文本
        ?? "";
    if (!userPromptAppend) return basePromptAppend || undefined;
    return basePromptAppend ? `${basePromptAppend}\n\n${userPromptAppend}` : userPromptAppend;
}
```

两种方式：
1. `CATEGORY_PROMPT_APPEND_RESOLVERS` — 函数，接收选中的 model 作为参数，可以返回 model 相关的提示
2. `CATEGORY_PROMPT_APPENDS` — 静态字符串
3. 用户配置的 `prompt_append` 与内置的拼接

### Gear: 无 per-category prompt injection

`coordinator_brief` 是全局的，不会根据 category 注入不同的指令。

---

## 7.5 `availableCategories` — 结果中总是包含可用列表

**OMO 在每个 `CategoryResolutionResult` 中都包含 `availableCategories: readonly string[]`。** 这样调用方可以在错误消息中告诉用户"Category X not found. Available: quick, deep, repair..."

Gear 的 fallback 错误中不返回可用 category 列表。

---

## 7.6 `nearestFallback` — 失败时的建议

### OMO

```typescript
// resolver.ts:238-248
if (!parsedModel || !foundModel) {
    const fallback = nearestFallback(selection);
    return {
        kind: "model_unavailable",
        attemptedModel: selection.selectedModel,
        nearestFallback: fallback,  // ← 建议的 fallback
    };
}
```

当解析失败时，`nearestFallback` 包含 fallback 链中的第一个 provider/model 组合，方便用户快速重试。

### Gear: 无 nearest fallback 建议

---

## 7.7 Gear 修复清单

| # | 缺失 | 影响 | 建议 |
|---|------|------|------|
| 1 | 模型元数据安全扫描 | 配置错误可能导致 API key 泄漏到 artifact 中 | 在 `CoordinatorModel` 写入前扫描字段名 |
| 2 | `prompt_append` per category | Gear category router 不能注入 category 特定指令 | `CategoryResolution` 加 `prompt_append` 字段 |
| 3 | `availableCategories` 返回 | 错误消息无法列出可用 categories | 所有错误返回中增加 |
| 4 | `nearestFallback` | fallback 失败时无建议 | `NoFallbackRoute` 错误中建议下一可用 route |
| 5 | fallback 链长度 = 重试次数 | 硬编码 2 次，限制多于配置的 fallback | `has_more_fallbacks` 改为检查链长度 |
