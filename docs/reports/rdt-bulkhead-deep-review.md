# 代码评审报告

- 仓库：katalyst-core
- 检测模式：方案与关联代码深度审查
- 检测范围：RDT bulkhead 设计文档及关联 resctrl、memory、CPU bulkhead、RDT 代码
- 生成时间：2026-07-29 02:07
- 检查文件：12
- 变更行数：761

## 缺陷统计

- P0：0
- P1：5
- P2：0
- 合计：5

## 缺陷详情

### 1. [P1][业务语义问题] RDT lifecycle 关闭会被仍启用的 CPUList 永久阻塞

- 位置：`docs/superpowers/specs/2026-07-28-rdt-bulkhead-resource-plugins-design.md:348-356`
- 置信度：9/10

**问题描述**

设计要求 draining 后只有 tasks、pod mon group、cpu_list 都排空才能进入 disabled；同时明确 lifecycle 不触发资源插件 disabled handler，CPUList 与 lifecycle 开关独立。只关闭 EnableRDTLifecycle 而 CPUList 保持启用时，后续 CPU adjustment 会持续把 shared/dedicated target 写回 cpu_list，manager 永远不能删除 CLOS，也无法结束 draining。

**修复建议**

定义 lifecycle drain 的资源排空协议：进入 draining 时暂停 CPUList 正常收敛并清空可回收受管 CLOS 的 cpu_list，或将 lifecycle 关闭实现为先完成 CPUList 回滚再开始 drain。不能要求运维手工按顺序关闭多个 AQC 开关；至少应在 AQC 校验层拒绝该不收敛组合。

---

### 2. [P1][业务语义问题] draining 所需的 task 回迁在现有接口中不可执行

- 位置：`pkg/util/external/rdt/manager_linux.go:45-48`
- 置信度：10/10

**问题描述**

设计要求 runtime adapter 枚举受管 Pod/sandbox task、逐项迁回 root/default、确认结果并重试；但 manager 接口没有 task inventory 或 migrator 注入，现有 RDTManager.ApplyTasks 在 Linux 上直接返回 not implemented。关闭 lifecycle 时没有可调用的执行器来完成 drain，已运行 task 会留在 CLOS 中。

**修复建议**

新增受管 task registry 与 TaskMigrator 契约，包含 ListManagedTasks、MoveToRoot、Verify、Generation 和可恢复的 RetryState；分别实现 kubelet Pod 与 sandmanlet sandbox adapter。manager 仅迁移 registry 中由 Katalyst 接管的对象，adapter 确认全部 task 离开后才允许删除对应 CLOS。

---

### 3. [P1][业务语义问题] sandbox-only 节点不会创建 reclaim CLOS

- 位置：`docs/superpowers/specs/2026-07-28-rdt-bulkhead-resource-plugins-design.md:136-146`
- 置信度：9/10

**问题描述**

sandbox plugin 固定返回 reclaim，但 manager 的 ExpectedClosSet 仅由 CPU/memory state 的 AllocationMeta 转换结果构建。若节点没有 Kubernetes reclaimed Pod，state 中不存在 reclaim 需求，manager 不会创建 reclaim；资源插件也禁止创建目录，sandmanlet/runtime 将无法迁移 sandbox task。

**修复建议**

将 sandbox 活跃需求纳入 ClosReconcileState，例如 ExpectedSandboxClosIDs 与 ActiveSandboxIDs；sandbox owner 在功能启用且存在任务时注册 reclaim 为期望 CLOS。manager 应以 Pod 与 sandbox 的联合期望集创建和保活 reclaim，而不能依赖 Kubernetes reclaimed workload 偶然存在。

---

### 4. [P1][业务语义问题] CAT 与现有 MB/MBA 会互相覆盖同一个 schemata 文件

- 位置：`pkg/agent/qrm-plugins/mb/allocator/resctrl_allocator.go:135-138`
- 置信度：10/10

**问题描述**

设计要求 CAT 生成并写入完整 schemata，现有 MB allocator 也整体写 schemata 中的 MB 配置。resctrl 的 schemata 是同一文件，CAT 完整写会抹掉 MB 行，MB 后续写也会抹掉 L3/CAT 行，直接违反文档中 CAT、MBA 资源独立 owner 的原则。

**修复建议**

以 schemata 行而非文件定义资源所有权。实现单一 read-modify-write coordinator 和同一把锁/串行队列：CAT 只更新 L3 行，MB/MBA 只更新 MB 行，并保留内核返回的其他行。写前保存完整旧值；同一 CAT 资源写入中途失败时回滚已写目标，避免对称 cache-domain 策略被破坏。

---

### 5. [P1][性能问题] RDT 插件的文件 I/O 实际运行在 DynamicPolicy.Lock 内

- 位置：`pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go:1235-1239`
- 置信度：10/10

**问题描述**

设计承诺所有 RDT 文件写入在 DynamicPolicy.Lock 之外，但现有 CPU policy 在持有 DynamicPolicy.Lock 的分配/advisor 链路中同步调用 bulkhead handlers。CPUList/CAT 对 resctrl 的目录扫描和文件写入会因此阻塞 CPU Allocate、Remove 与 advisor 更新；挂载卡顿或内核拒绝时会放大为全局策略延迟。

**修复建议**

锁内只构造不可变的 CPUSetPartitionView、动态配置快照和 generation；释放 DynamicPolicy.Lock 后执行资源插件 I/O。完成后以 generation/CAS 确认结果仍对应最新 state，再更新插件成功快照。为异步 I/O 设置单飞/合并策略和 bounded queue，避免 CPU adjustment 高频触发重复写入。

---
