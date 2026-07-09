# Gear ↔ Omo 功能对齐 - 决策记录

## 执行批次规划

### Batch 1 (并行): P0-1, P0-2, P0-3, P0-4
- 无相互依赖，可并行执行
- 都修改 runtime.rs 的不同区域

### Batch 2 (串行): P0-5
- 依赖 P0-3 (streak 重置逻辑)
- 也依赖 workers.rs 的 category_resolution_result

### Batch 3 (并行): P1-1, P1-3
- P1-1 依赖 P0-5
- P1-3 依赖 P0-1, P0-2, P0-3

### Batch 4 (串行): P1-2
- 依赖 P1-1

### Batch 5 (并行): P2-1, P2-2
- P2-1 依赖 P0-4
- P2-2 依赖 P1-1

### Batch 6 (串行): P2-3
- 依赖所有 P0, P1
