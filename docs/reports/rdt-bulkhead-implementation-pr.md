# RDT Bulkhead Resource Plugins

## PR 标题

```text
feat(rdt): add bulkhead CPUList and CAT resource reconciliation
```

## PR 描述

```markdown
## 背景

resctrl CLOS 的目录生命周期、CPU 关联、CAT way 和 MB/MBA `schemata` 更新此前分散在 memory policy、CPU policy 与 MB allocator 中。不同模块对 CLOS 名称、目录创建/清理和 `schemata` 文件缺少统一边界，无法安全地扩展 CPUList、CAT 与后续 MBA 的独立收敛能力。

本 PR 将 resctrl 提升为 CPU 与 memory 共用的基础能力：由 `resctrl.Manager` 管理目录生命周期，由 CPU bulkhead 插件分别收敛 `cpu_list` 与 CAT L3 资源，并由 `RDTManager` 协调 CAT、MB/MBA 对共享 `schemata` 文件的更新。

## 改动内容

### 通用 resctrl 配置与 CLOS 解析

- 将 `ResctrlConfig`、`ResctrlOptions`、默认值、flags 和配置校验从 memory plugin 迁移到通用 resctrl 配置域。
- 新增 `pkg/util/resctrl`，集中维护 pool 到 CLOS 的解析、shared subgroup 命名与 CAT key 解析。
- 新增独立 `ClosAssignmentMeta`，仅包含 `QoSLevel` 和 `OwnerPool`。
- 为 `commonstate.AllocationMeta` 增加 `ToClosAssignmentMeta()`；CPU 与 memory 通过该转换复用同一 CLOS 解析逻辑。
- 新增 `DefaultClosIDs`，默认为空；`reclaim` 不再默认保活。`DisableRDT=false` 时 manager 确保默认 CLOS 存在且常规清理不会删除它。

### CLOS 生命周期

- 保留 `Create` 作为 admission 同步创建入口：在返回 Pod CLOS ID 前确保目录和 pod mon group 存在。
- 新增/改造 `ReconcileClos` 作为旁路周期收敛入口：补建期望 CLOS，并清理不再需要且现场状态满足条件的 CLOS。
- 新增 AQC 字段 `QRMPluginConfig.RDTConfig.DisableRDT`：
  - `false`：创建默认/期望 CLOS，常规清理要求 `tasks`、pod mon group、`cpu_list` 均为空。
  - `true`：强制删除 `SkipCleanupClosIDs` 之外的全部 CLOS；不检查 `tasks`、pod mon group 或 `cpu_list`。
- 目录创建、删除和 resource 写入复用 per-CLOS 操作锁，避免 `DisableRDT` 与资源收敛交错。

### CPUList bulkhead 插件

- 新增 CPUList 插件，只收敛现存受管 CLOS 的 `cpu_list`。
- 基于 `CPUSetPartitionView.SharePoolMap` 和 dedicated CPUSet 计算 CLOS target；多个 pool 映射到同一 shared subgroup 时取 CPUSet 并集。
- 支持动态 `EnableCPUList` 开关与 disabled transition 的全量清空。
- 新增成功写入缓存：相同 `(ClosID, ClosEpoch, CPUSet target)` 跳过重复 `cpu_list` 写入；失败不缓存，目录删除/重建、插件重新启用和 agent 重启均会失效缓存。

### CAT bulkhead 插件与 schemata 协调

- 新增 CAT 插件，只计算并请求写入 CAT L3 target。
- 新增 `BulkheadRDTConfig`：`EnableCAT`、`DefaultCATWays`、`ClosCATWays`。
- 对所有 cache domain 使用相同的 CAT way 数；由 capability provider 读取每个 domain 的 CBM 能力并生成对应 mask。
- 任一 domain 不满足目标 way 数时拒绝整轮 CAT 更新。
- `RDTManager` 内部引入 `SchemataCoordinator`：
  - CAT 仅更新 `L3:` 行。
  - 现有 MB allocator 与未来 MBA 仅更新 `MB:` 行。
  - 对同一 CLOS 的 `schemata` read-modify-write 串行化，保留未知资源行。
  - CAT 写入失败时回滚本次已写 L3 行；回滚失败标记 degraded 并记录错误。
- 为 L3 target 增加成功写入缓存，命中时跳过 `schemata` 读写；MB 更新不会错误使 L3 缓存失效。

## 配置变更

```go
type RDTConfig struct {
    DisableRDT *bool
}

type BulkheadRDTConfig struct {
    EnableCPUList  *bool
    EnableCAT      *bool
    DefaultCATWays *int64
    ClosCATWays    map[string]int64
}
```

静态 `resctrl.ResctrlConfig` 新增：

```go
DefaultClosIDs []string // 默认: []string{}
```

## 行为变化

- sandbox plugin 后续固定返回 `reclaim` CLOS ID；即使节点没有 reclaimed Pod，常规 reconcile 也会预创建 `reclaim`。
- `SkipCleanupClosIDs` 是目录删除保护名单，不是 CPUList/CAT 写保护名单。
- `DefaultClosIDs` 只在常规模式保活；`DisableRDT=true` 时仍会删除，除非同时位于 `SkipCleanupClosIDs`。
- `DisableRDT=true` 是强制撤销操作，调用方必须接受其中 task 脱离已删除 CLOS 的后果。

## 兼容性与风险

- 原有 `resctrl-*` flag 保持名称兼容，但所有权迁移到通用 `ResctrlOptions`。
- `EnableResctrlGroupLifecycleManagement` 被移除/废弃，由 AQC `DisableRDT` 统一控制强制删除。
- 现有 MB allocator 必须切换到 `RDTManager.ApplyMBA`，不能继续直接覆盖 `schemata`。
- CAT 与 MBA 不创建或删除 CLOS；CPUList 仅可在写入非空 target 时通过共享 per-CLOS 锁按需创建缺失 CLOS，不删除或重命名目录。常规目录生命周期仍由 `resctrl.Manager` 负责。

## 测试

- [ ] CLOS util：shared subgroup、pool/CLOS 映射、非法 CAT key、`AllocationMeta.ToClosAssignmentMeta()`。
- [ ] resctrl manager：`Create` 与 `ReconcileClos` 并发、显式默认 CLOS 创建/保活、常规清理、`DisableRDT` 强制删除、skip 保护。
- [ ] CPUList：shared subgroup CPUSet 并集、disabled 清空、相同 target 缓存命中、写失败重试、同名 CLOS 重建后缓存失效。
- [ ] CAT：全 cache domain 对称 mask、能力不足拒绝、CAT disabled 回滚、相同 L3 target 缓存命中。
- [ ] SchemataCoordinator：CAT/MB 行保留、并发更新、CAT 写失败回滚、`DisableRDT` 删除竞争。
- [ ] bulkhead：CPUList/CAT 动态 enable/disable transition、插件顺序、重复 reconcile 不重复写文件。
- [ ] 回归：memory ResctrlHinter、现有 MB allocator、CPU dynamic policy 与全量 Go test。

## 非本 PR 范围

- sandbox plugin 的具体实现。
- MBA 的独立动态配置策略和 bulkhead 插件实现。
- kubelet/sandmanlet 的 task 迁移协议改造。
```

## 建议提交序列

以下提交按依赖顺序组织。每个提交都应包含本提交对应的单测，并在提交前运行相关包测试。

| 顺序 | 提交信息 | 主要内容 |
|---|---|---|
| 1 | `refactor(resctrl): centralize static configuration and clos resolver` | 迁移 `ResctrlConfig`/`ResctrlOptions`；新增通用 CLOS util 与 `ClosAssignmentMeta` 转换 |
| 2 | `feat(resctrl): reconcile default and expected clos directories` | 新增 `DefaultClosIDs`、`ReconcileClos`、`DisableRDT`、目录 epoch 与 create/reconcile 串行保护 |
| 3 | `feat(qrm): add dynamic rdt and bulkhead configuration` | 在 API/core 增加 `RDTConfig.DisableRDT`、`BulkheadRDTConfig`，补齐 deepcopy、CRD、动态配置装配 |
| 4 | `refactor(rdt): coordinate schemata updates through rdt manager` | 实现 `RDTManager`/`SchemataCoordinator`、迁移 MB allocator 到 `ApplyMBA`、实现 per-CLOS 锁和行级 read-modify-write |
| 5 | `feat(bulkhead): add cached rdt cpulist reconciliation` | 新增 CPUList 插件、target 计算、缓存、disabled transition 与 registry 注册 |
| 6 | `feat(bulkhead): add cached cat reconciliation` | 新增 CAT capability provider、对称 domain mask、CAT 插件、L3 缓存、回滚与 registry 注册 |
| 7 | `test(rdt): cover clos lifecycle and resource reconciliation` | 补齐跨模块竞态、缓存失效、CAT/MB 行保留、强制删除和回归测试 |

## 单个提交模板

```text
<type>(<scope>): <summary>

<why>

<what changed>

Tests:
- <command 1>
- <command 2>
```

示例：

```text
feat(bulkhead): add cached rdt cpulist reconciliation

Avoid repeated resctrl cpu_list writes while preserving synchronous
CPUSetAdjustmentHandler semantics under DynamicPolicy.Lock.

- derive managed CLOS CPUSet targets from CPUSetPartitionView
- cache successful writes by CLOS ID, directory epoch and normalized CPUSet
- retry failed writes and invalidate stale entries on directory recreation

Tests:
- go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cpulist
- go test ./pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl
```
