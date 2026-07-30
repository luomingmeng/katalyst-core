# 代码评审报告

- 仓库：katalyst-core
- 检测模式：设计与架构审查
- 检测范围：RDT bulkhead resource plugins 设计方案及相关现有实现
- 生成时间：2026-07-29 00:50
- 检查文件：7
- 变更行数：680

## 缺陷统计

- P0：0
- P1：5
- P2：0
- 合计：5

## 缺陷详情

### 1. [P1][业务语义问题] 关闭 RDT lifecycle 后仍可能继续分配 CLOS 并阻塞清理

- 位置：`docs/superpowers/specs/2026-07-28-rdt-bulkhead-resource-plugins-design.md:253-253`
- 置信度：9/10

**问题描述**

方案把 EnableRDTLifecycle 的 owner 限定为 resctrl.Manager，但 memory ResctrlHinter 的 CLOS hint 发放路径和未来 sandbox plugin 均未绑定该开关。关闭 lifecycle 后，kubelet/sandmanlet 仍可通过 runtime 把新 task 写入 CLOS；manager 因 tasks 非空又不能删除目录，结果“关闭 RDT 自动清理 CLOS”无法收敛，也没有真正停止 RDT 分配。

**修复建议**

把开关语义改为统一的 lifecycle 状态机：关闭时先阻止 ResctrlHinter 和 sandbox plugin 发放新 CLOS ID，再驱动 runtime 将已分配 task 移回 root/default CLOS，等待 tasks 和 mon_groups 排空后才删除目录。状态机至少应有 enabled、draining、disabled 三态，并为 drain 超时输出指标和可诊断错误。

---

### 2. [P1][并发问题] CLOS 删除与 runtime 写 tasks 存在检查后使用竞态

- 位置：`docs/superpowers/specs/2026-07-28-rdt-bulkhead-resource-plugins-design.md:289-307`
- 置信度：9/10

**问题描述**

ReconcileClos 以一次 tasks 为空的检查作为删除条件，而 runtime 写 tasks 的链路独立于 manager。当前 manager 实现也是先读取 tasks 再 RemoveAll，二者没有共享锁、generation 或 runtime 侧重试契约。检查完成后 runtime 可以立即向同一 CLOS 写 task，随后目录被删除，导致容器启动/迁移失败或 task 落入未定义状态。

**修复建议**

为 CLOS 引入可并发验证的 lease/generation 协议：manager 删除前将 CLOS 标记为 draining，runtime 仅接受当前 generation 的可用 CLOS；删除时再次以 generation 原子校验并等待 active assignment 归零。若无法改 runtime 协议，则 manager 不得删除近期被 hint/assignment 使用的 CLOS，并必须定义 runtime 对 ENOENT 的重试和重新取 hint 流程。

---

### 3. [P1][业务语义问题] CAT way 数量缺少到每个 cache domain schemata 的确定映射

- 位置：`docs/superpowers/specs/2026-07-28-rdt-bulkhead-resource-plugins-design.md:402-416`
- 置信度：9/10

**问题描述**

配置只给出 defaultCATWays 和 closCATWays 的“way 数量”，但 CAT 实际需要针对每个 L3/cache domain 写 schemata 位掩码，并受可用位、关联掩码、NUMA/L3 拓扑和其他 CLOS 分区约束。现有 RDTManager 的 ApplyCAT 也仍是未实现接口。没有 mask 分配策略、冲突校验和默认 root schemata 获取方式，14/15 way 无法可靠转换为安全、可复现的内核配置。

**修复建议**

新增 CAT topology/capability provider，读取每个 cache domain 的 CBM mask 与最小 bit 数；将 CAT policy 明确定义为 per-domain mask 分配算法，而非单一整数。生成完整 schemata 后执行“掩码在允许范围内、各 CLOS 策略满足预期共享/隔离、root 保留策略正确”的预写校验；失败时不得部分写入任何 domain。

---

### 4. [P1][安全漏洞][已解决] CPUList 禁用会修改未受 Katalyst 管理的外部 CLOS

- 位置：`docs/superpowers/specs/2026-07-28-rdt-bulkhead-resource-plugins-design.md:381-388`
- 置信度：8/10

**问题描述**

禁用路径要求扫描全部 non-root CLOS 并清空除 SkipCleanupClosIDs 外的 cpu_list，且明确不受 manager managed CLOS 命名范围限制。resctrl 根目录可能包含其他组件或运维预置的 CLOS；遗漏一项 skip 配置就会篡改外部 workload 的 CPU 绑定。这是跨组件资源越权，可能造成隔离失效或业务中断。

**修复建议**

把“可写 CLOS”与“可删除 CLOS”分别建模为显式 allowlist。CPUList 仅清空 manager 标记为 Katalyst-owned、或由配置显式声明由 CPUList 接管的 CLOS；对外部组默认只观测并报告。若必须保留全量清空语义，应新增单独的危险开关，默认关闭，并在启动和每次执行时审计记录受影响 CLOS。

**当前状态**

已按资源维度拆分 CPUList 写入边界：CPUList 只写 `dedicated` 与 shared CLOS，不再写 `reclaim`、`system`、显式默认 CLOS 或未知外部 CLOS；`SkipCleanupClosIDs` 只作为 manager 目录删除保护，不再作为 CPUList 写保护。

---

### 5. [P1][业务语义问题] AllocationMeta 无法表达 sandbox 身份和跨来源 CLOS 冲突规则

- 位置：`pkg/agent/qrm-plugins/commonstate/state.go:31-45`
- 置信度：9/10

**问题描述**

方案将 AllocationMeta 定义为 CPU 与 memory 共同的唯一 CLOS 解析输入，并计划供未来 sandbox plugin 使用；但现有类型只包含 Pod UID、namespace、container 等 Kubernetes Pod 字段，没有 workload kind、sandbox ID、来源 state、版本/generation 或 CLOS ownership。它既不能唯一标识 sandbox，也不能在 CPU/memory 对同一主体给出不同 QoS/owner pool 时定义去重、优先级和拒绝策略。

**修复建议**

不要直接复用当前 Pod 专用 AllocationMeta。定义独立的 ResctrlSubject 或 ClosAssignmentMeta，至少包含 SubjectKind(Pod/Sandbox)、SubjectID、Container/Sandbox task selector、QoS、OwnerPool、Producer、Generation。BuildExpectedClosPools 应按 SubjectKind+SubjectID 去重，并在同一主体解析到不同 CLOS 时返回冲突错误和指标，而不是静默并集。

---
