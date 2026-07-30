# RDT Bulkhead Resource Plugins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 resctrl 增加统一的 CLOS 生命周期、CPUList/CAT bulkhead 收敛和共享 `schemata` 协调能力。

**Architecture:** `resctrl.Manager` 负责 CLOS 的同步创建与旁路 reconcile；CPUList/CAT 作为 CPU bulkhead 插件只写资源文件。`RDTManager` 统一协调 `schemata` 的 L3/MB 行 read-modify-write。所有资源收敛仍在 `DynamicPolicy.Lock` 内同步执行，但以成功写入缓存避免重复 I/O。

**Tech Stack:** Go、Kubernetes API/AQC、resctrl FS、Katalyst QRM、Go testing。

---

### Task 1: 通用 CLOS 解析元数据

**Files:**
- Create: `pkg/util/resctrl/clos.go`
- Create: `pkg/util/resctrl/clos_test.go`
- Modify: `pkg/agent/qrm-plugins/commonstate/state.go`
- Modify: `pkg/agent/qrm-plugins/commonstate/state_test.go`

- [ ] 先为 `ClosAssignmentMeta{QoSLevel, OwnerPool}`、`SharedSubgroupClosID` 和 `AllocationMeta.ToClosAssignmentMeta()` 编写失败测试。
- [ ] 运行 `go test ./pkg/util/resctrl ./pkg/agent/qrm-plugins/commonstate`，确认测试因缺失实现失败。
- [ ] 实现独立 `pkg/util/resctrl` 元数据和纯 CLOS 解析函数；`commonstate` 仅提供转换 helper。
- [ ] 重新运行上述测试并提交 `refactor(resctrl): add common clos assignment metadata`。

### Task 2: 统一静态 resctrl 配置

**Files:**
- Create: `pkg/config/agent/qrm/resctrl/resctrl_config.go`
- Create: `cmd/katalyst-agent/app/options/qrm/resctrl_options.go`
- Modify: `pkg/config/agent/qrm/qrm_base.go`
- Modify: `pkg/config/agent/qrm/memory_plugin.go`
- Modify: `cmd/katalyst-agent/app/options/qrm/qrm_base.go`
- Modify: `cmd/katalyst-agent/app/options/qrm/memory_plugin.go`
- Modify: memory policy 和测试中 `ResctrlConfig` 的引用

- [ ] 为 `DefaultClosIDs` 默认为空；`reclaim` 不再默认保活、options flag 装配和 memory consumer 使用顶层配置编写失败测试。
- [ ] 实现通用 `resctrl.ResctrlConfig`/`ResctrlOptions`，将 config 注入 QRM 顶层，删除 memory 对配置所有权。
- [ ] 运行 options、memory dynamic policy 和 config 包测试。
- [ ] 提交 `refactor(resctrl): centralize static resctrl configuration`。

### Task 3: CLOS 创建与 reconcile 生命周期

**Files:**
- Modify: `pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl/manager.go`
- Modify: `pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl/manager_test.go`
- Modify: manager 构造和周期调用点

- [ ] 为 `Create` 同步创建、`ReconcileClos` 显式默认 CLOS 创建、常规清理、`DisableRDT` 强制删除和 `SkipCleanupClosIDs` 编写失败测试。
- [ ] 实现 `ClosReconcileState`、默认 CLOS 合并、常规清理与强制删除；为目录创建/删除维护 CLOS epoch。
- [ ] 确保 `Create` 与 `ReconcileClos` 复用同一 manager 互斥保护和创建 helper。
- [ ] 运行 resctrl manager 测试并提交 `feat(resctrl): reconcile default and expected clos directories`。

### Task 4: AQC RDT 配置

**Files:**
- Modify: `katalyst-api/pkg/apis/config/v1alpha1/adminqos.go`
- Modify: `katalyst-api` 对应 deepcopy、CRD 与生成文件
- Modify: `pkg/config/agent/dynamic/adminqos/qrm/*.go`
- Modify: 动态配置测试

- [ ] 在 API worktree 为 `RDTConfig.DisableRDT` 与 `CPUPluginConfig.BulkheadConfig.BulkheadRDTConfig` 写失败测试。
- [ ] 更新 API 类型、运行 codegen、提交 API 仓库变更。
- [ ] 更新 core 的动态配置转换、默认值与测试，消费新 API 字段。
- [ ] 运行 core 动态配置测试并提交 `feat(qrm): add dynamic rdt bulkhead configuration`。

### Task 5: RDTManager schemata 协调

**Files:**
- Modify: `pkg/util/external/rdt/manager.go`
- Modify: `pkg/util/external/rdt/manager_linux.go`
- Modify: `pkg/util/external/rdt/manager_unsupported.go`
- Create: `pkg/util/external/rdt/schemata.go`
- Create: `pkg/util/external/rdt/schemata_test.go`
- Modify: `pkg/agent/qrm-plugins/mb/allocator/resctrl_allocator.go`
- Modify: allocator 测试

- [ ] 为 L3/MB 行保留、每 CLOS 串行 read-modify-write、缓存命中、写失败缓存失效与删除竞争写失败编写失败测试。
- [ ] 实现 `SchemataCoordinator`，让 `ApplyCAT` 只更新 `L3:`、`ApplyMBA` 只更新 `MB:`。
- [ ] 将 MB allocator 迁移到 `RDTManager.ApplyMBA`。
- [ ] 运行 rdt/MB allocator 测试并提交 `refactor(rdt): coordinate schemata updates through rdt manager`。

### Task 6: CPUList bulkhead 插件

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cpulist/plugin.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cpulist/plugin_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/registry/registry.go`
- Modify: bulkhead API/manager 测试

- [ ] 为 pool CPUSet 并集、空 target、相同 target 缓存命中、写失败重试和同名 CLOS 重建编写失败测试。
- [ ] 实现只操作受管目录的 CPUList 插件；缓存 key 使用 `ClosID + ClosEpoch`，只缓存成功写入。
- [ ] 在 registry 注册插件，保留动态 enable/disable transition。
- [ ] 运行 bulkhead 和插件测试并提交 `feat(bulkhead): add cached rdt cpulist reconciliation`。

### Task 7: CAT bulkhead 插件

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/registry/registry.go`

- [ ] 为对称 cache-domain mask、能力不足拒绝、`ClosCATWays` 解析、disabled 回滚与 coordinator 缓存命中编写失败测试。
- [ ] 实现 CAT capability 读取、每 domain 对称 target 构建和对 `RDTManager.ApplyCAT` 的调用。
- [ ] 在 registry 注册插件，运行 CAT、RDT manager、bulkhead 测试。
- [ ] 提交 `feat(bulkhead): add cached cat reconciliation`。

### Task 8: 端到端回归

**Files:**
- Modify: 相关单测、配置样例和设计文档

- [ ] 运行所有受影响包测试、`go test ./pkg/agent/qrm-plugins/...` 和 `go test ./pkg/util/external/rdt/...`。
- [ ] 运行 `go vet` 或项目既有静态检查命令，并执行 `git diff --check`。
- [ ] 更新 PR 描述中的验证结果，提交 `test(rdt): cover resource reconciliation integration`。
