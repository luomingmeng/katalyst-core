# 代码评审报告

- 仓库：katalyst-core
- 检测模式：RED 6 目标树质量审查
- 检测范围：工作区 RED 6 相关未提交变更
- 生成时间：2026-09-16 09:42
- 检查文件：12
- 变更行数：624

## 缺陷统计

- P0：0
- P1：3
- P2：0
- 合计：3

## 缺陷详情

### 1. [P1][业务语义问题] Admission 预算按单个 plan 重置，无法在首次写前预留完整 Drain→Expand 闭包

- 位置：`pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go:1267-1285`
- 置信度：10/10

**问题描述**

checkAdmissionExecutionBudget 只计算当前 PhasePlan 的 requiredWrites，既不包含后续 drain frontier/expand，也不扣减此前已消费的物理写。多个各自低于 MaxRequiredWrites 的 plan 可累计越过上限，并在后续预算失败前留下已执行的 drain 写，违反 reservation failure => zero writes。

**修复建议**

在首次物理写前从完整 canonical closure 生成 invocation-scoped ticket，累计记录 CPU/Mems forward 与 worst-case rollback；后续 frontier/phase 只能消费该 ticket，不得重新按局部 plan 比较总上限。

---

### 2. [P1][业务语义问题] 预算计入 rollback 但写失败路径没有执行 rollback

- 位置：`pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/safe_writer.go:110-159`
- 置信度：10/10

**问题描述**

预算按每次物理写乘二声称覆盖 worst rollback，但 execute 在 mems 成功、cpus 失败或 post-write read-back 失败时直接返回，不恢复已经改变的资源。具体触发是 WriteMems=true 且随后 WriteCPUs/ReadEntry 失败，此时调用返回错误但 cgroup 已部分变更。

**修复建议**

让 reservation ticket 记录每次成功 forward write 的旧值；后续写或 fresh read-back 失败时按逆序恢复 cpus/mems，并消费预留的 rollback slot。rollback 失败必须返回同时包含原始错误和恢复错误的 fail-closed 结果。

---

### 3. [P1][业务语义问题] RequiredCPUSetByRel 从可变 DAG 重建而非使用冻结的 canonical contract

- 位置：`pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go:383-468`
- 置信度：9/10

**问题描述**

当前 helper 在 CPUSetAdjustmentHandler 内重新读取 DAG 节点 CPUSet。DAG 可能只承载当前 stage 或在 attempt 边界后重建，因而无法证明 coordinator 收到的是规划与验证使用的同一 canonical 14→18 required set，形成第二 floor owner。

**修复建议**

由 advisor precommit 从 immutable canonical DesiredView 派生 RequiredCPUSetByRel，并通过已有调用输入原样传递至 plugin/coordinator；删除 plugin 内 requiredReclaimCPUSetByRel 的二次推导。

---
