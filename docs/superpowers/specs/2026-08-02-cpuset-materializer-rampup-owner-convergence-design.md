# CPUSet Materializer 与 RampUp 单一所有权收口设计

## 文档状态

- 文档类型：技术设计
- 目标组件：QRM DynamicPolicy、RampUp planner、CPUSet materialization、Bulkhead Manager、CpusetTopology plugin
- 基线文档：`2026-08-01-reclaim-generation-identity-stability-design.md`
- 实施策略：一次性收口，内部 legacy 路径 delete-first
- 状态：已完成设计确认，等待实施计划

本文修正当前实现相对基线设计的职责漂移。若本文与旧实现或旧设计在
DynamicPolicy、CPUSet adjustment、Bulkhead Manager、CpusetTopology
所有权上冲突，以本文为准；`TargetState` durable transaction、
Bulkhead cgroup DAG 写序和 checkpoint wire format 继续沿用基线设计。

## 结论

收口后只保留四个核心角色：

```text
DynamicPolicy
    唯一策略与事务 owner

Domain Planner
    TargetState → TargetState 的纯规划

cpusetmaterializer
    中立、只读的物化契约

Bulkhead Manager
    CPUSetMaterializer 的当前唯一实现
```

核心调用链：

```text
PrepareDurableTarget
→ Plan complete TargetState
→ ValidateTarget
→ Build immutable MaterializationTarget
→ CPUSetMaterializer.Materialize
→ require converged
→ CommitTarget
```

删除以下重复概念：

- legacy cpuset adjustment handler type；
- `CPUSetAdjustmentHandlerCtx`；
- handler registry；
- `newPlanningPolicy`；
- `planningState`；
- planning mode boolean；
- Bulkhead Manager 双入口；
- Manager state-based view rebuild；
- `TargetState` legacy mutable State adapter。

## 第一性原则

### 策略目标

```text
TargetState 是唯一完整策略事实源。
```

任何 planner 必须一次生成完整、一致的 `next TargetState`。不能先发布
container allocation，再补 reclaim pool；不能只修改 reclaim pool，再依赖
materializer 修正 share pool。

### 事务成功

```text
DynamicPolicy 返回 mutation 成功
⇒ next TargetState 已通过策略验证
且 CPUSet materialization 已收敛
且 next TargetState 已 durable commit
```

没有配置 materializer 时，物化步骤是空操作并直接视为收敛；这表示当前环境
没有外部 CPUSet 执行面，不伪造 cgroup convergence evidence。

### 物化边界

```text
Materializer 只消费已规划目标并返回执行结果。
```

Materializer 不得：

- 读取或修改 `state.State`；
- 接收 `TargetState`；
- 修改策略目标；
- 提交 checkpoint；
- 计算 RampUp hard floor；
- 重新生成 share/reclaim pool。

### 单一实现

`DynamicPolicy` 最多持有一个 `CPUSetMaterializer`。不提供 runtime registry、
排序、重复注册或 handler chain。Bulkhead Manager 是当前唯一实现，但
DynamicPolicy、RampUp planner 和中立契约均不依赖 Bulkhead。

## 当前问题

### 影子 DynamicPolicy

`newPlanningPolicy` 创建第二个 `DynamicPolicy`，共享 Manager、MetaServer、
mutable maps 和 runtime components，再通过 `planningRampUpAdmission`
改变同一方法的副作用。

这导致：

- live policy 与 planning policy 形成重复 owner；
- 同一函数根据 mode bit 既可能规划，也可能写 live state；
- legacy helper 可意外触发外部组件；
- 锁、state 和 lifecycle 语义无法由类型表达。

### 双物化入口

Bulkhead Manager 同时存在：

```text
Apply(owned View)
RunCPUSetAdjustmentHandlers(ReadonlyState)
```

后一入口重新从 state 构建 view，允许直接传 live state，破坏
“一个 transaction 只从 owned target 构建一次物化目标”的边界。

### 可变 View

CpusetTopology 直接修改传入的 `CPUSetPartitionView` 注入 pending
protection。dependent plugins 因此消费被隐式改写的共享指针，Manager
最终返回的 reclaim 也可能不是原始目标。

### RampUp target 不完整

当前 `PlanRampUpReclaimPoolTarget` 只更新 reclaim pool。真实节点验证显示：

```text
ReclaimRaw = current hard reclaim
SharePool  = still contains current hard reclaim
ReclaimEffective = empty
```

在 `allowSharedCoresOverlapReclaimedCores=false` 时，Bulkhead view 正确地将
重叠部分从 effective reclaim 中排除，最终导致 fresh convergence 失败。
根因属于 planner target 一致性，不应由 CpusetTopology 修补。

## 目标架构

```text
┌──────────────────────────────────────────────┐
│ DynamicPolicy                                │
│ request validation / main lock / readiness   │
│ transaction / commit                         │
└──────────────────────┬───────────────────────┘
                       │ owned Base
                       ▼
┌──────────────────────────────────────────────┐
│ Domain Planner                               │
│ complete TargetState planning and validation │
└──────────────────────┬───────────────────────┘
                       │ immutable projection
                       ▼
┌──────────────────────────────────────────────┐
│ cpusetmaterializer.Materializer              │
│ neutral target/result contract               │
└──────────────────────┬───────────────────────┘
                       │ current implementation
                       ▼
┌──────────────────────────────────────────────┐
│ Bulkhead Manager                             │
│ private plugin orchestration                 │
└──────────────────────┬───────────────────────┘
                       ▼
┌──────────────────────────────────────────────┐
│ CpusetTopology                               │
│ runtime observation / DAG apply / evidence   │
└──────────────────────────────────────────────┘
```

依赖关系：

```text
DynamicPolicy ───────→ cpusetmaterializer ←────── Bulkhead Manager
RampUp planner ──────→ state.TargetState
Bulkhead Manager ────→ private Bulkhead plugins
CpusetTopology ──────→ cgroup client / MetaServer / topology writer
```

禁止：

```text
DynamicPolicy → bulkhead/api
RampUp planner → Bulkhead
Bulkhead Manager → state.State
CpusetTopology → TargetState
CpusetTopology → mutable policy View
```

## 中立契约

新增：

```text
pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/
    interface.go
    target.go
    result.go
```

该包只定义契约，不包含：

- registry；
- manager；
- 默认实现；
- config 判断；
- DynamicPolicy；
- Bulkhead plugin；
- RampUp 规划。

### Materializer

```go
type Materializer interface {
    Materialize(
        ctx context.Context,
        target Target,
    ) (Result, error)
}
```

不增加 `Name`、`Enable`、`Reset` 或 `DisabledHandler`。启用状态由
DynamicPolicy 构造阶段决定：

- `materializer == nil`：没有外部物化执行面；
- `materializer != nil`：所有 CPUSet target mutation 都调用同一个实例。

### Target

`Target` 是从 `TargetState` 派生的一次性不可变值：

```go
type Target struct {
    ReserveCPUSet        machine.CPUSet
    ReclaimCPUSet        machine.CPUSet
    NonReclaimCPUSet     machine.CPUSet
    ReclaimCPUSetByNUMA  map[int]machine.CPUSet
    ContainerCPUSetByPod map[string]map[string]machine.CPUSet
    AllowReclaimOverlap  bool
}
```

实现要求：

- 构造时 defensive clone；
- 字段不直接公开可变 map；
- getter 返回 clone 或只读迭代结果；
- materializer 不能获得 `TargetState` 或 `ReadonlyState`；
- 每个 transaction 只构建一次；
- 不持久化；
- 不成为第二个策略事实源。

`NonReclaimCPUSet` 是 planner 完整目标的派生结果，不由 materializer
重新计算。Materializer 可以基于运行时 pending observation 调整 cgroup
写序或判定不收敛，但不能构造另一份 CPUSet target。

### Result

```go
type Result struct {
    Converged bool
    Evidence  Evidence
}
```

`Evidence` 是诊断信息，不参与策略规划。它至少表达：

- materialization 是否执行；
- controlled rel 的 target/observed；
- pending protection；
- deferred/failed/missing rel；
- convergence failure reason。

成功合同：

```text
error == nil
⇒ Converged == true
且所有 controlled CPUSet 精确等于 Target
```

若实现返回 `false, nil`，DynamicPolicy 将其转换为 typed
`ErrCPUSetNotConverged`。

同步首版不允许 materializer 返回“已收敛但 applied reclaim 只是 target
子集”的结果。该语义会制造 committed desired 与 runtime applied 两份事实源。
任何运行时约束导致实际结果无法精确等于 target 时，本轮必须不收敛并返回证据。

### nil Materializer

```text
materializer == nil
⇒ skip external materialization
⇒ Converged = true
```

不增加 Noop 实现或默认 registry。nil 语义由 DynamicPolicy 直接处理。

## DynamicPolicy

### 唯一职责

DynamicPolicy 负责：

- RPC/request 校验；
- CPU policy readiness；
- mutation 主锁；
- durable Base；
- 调用 domain planner；
- target validation；
- 构建一次 `cpusetmaterializer.Target`；
- 调用唯一 materializer；
- durable commit；
- post-commit external notification；
- failure 时恢复 Base 或进入 blocked。

DynamicPolicy 不负责：

- Bulkhead plugin 编排；
- cgroup DAG；
- plugin enable/disable；
- materializer 内部执行顺序；
- pending protection；
- controlled rel convergence。

### 构造注入

```go
type DynamicPolicy struct {
    // existing fields...
    cpuSetMaterializer cpusetmaterializer.Materializer
}
```

Materializer 在构造阶段注入，不提供注册 API。重复注册问题从类型层消失。

### 不保存 runtime applied reclaim

同步强收敛合同保证：

```text
Materialize success
⇒ actual controlled CPUSet layout == MaterializationTarget
```

因此成功 commit 后，`TargetState` 中的 committed `ReclaimRaw` 同时就是
已验证的实际 reclaim 身份，不需要 `appliedReclaim/appliedValid` 第二份缓存。

身份选择收敛为：

```text
current committed ReclaimRaw
→ fresh eligible CPUs
```

启动 recovery 在 policy 进入 ready 前精确验证 committed target，因此也不需要
额外 valid bit。若未来引入异步 materialization、deferred commit 或 desired/applied
允许长期分离，必须单独设计 applied generation；不得提前在同步路径保留缓存。

## 删除影子规划模型

删除：

- `newPlanningPolicy`；
- `planningState`；
- `planningContext` 对影子 Policy 的依赖；
- `planningRampUpAdmission`；
- `storeRequested`；
- mode-bit 分支；
- shadow policy shared managers/maps/components；
- 通过 `Set*`/`StoreState` 伪装 candidate mutation 的逻辑。

替代为显式规划函数：

```go
type TargetPlan func(
    base *state.TargetState,
) (*state.TargetState, error)
```

统一 transaction：

```go
func (p *DynamicPolicy) transact(
    ctx context.Context,
    plan TargetPlan,
) error
```

规划 helper 接受 owned `TargetState` 或更窄的领域 editor，不接收
`DynamicPolicy` 副本，不调用 live repository。

## RampUp 完整目标

### Planner 输入

```go
type ReclaimHardConstraint struct {
    CPUs          machine.CPUSet
    AffectedNUMAs sets.Set[int]
    OwnerPodUID   string
}
```

删除 `CommittedRaw`。Committed floor 必须从 Base 派生。

删除 owner 排除逻辑。Base 中所有 committed `RampUp=true` owner 都参与
active floor；本次未提交 floor 通过 `current.CPUs` 增量表达。

### Final reclaim

```text
FinalReclaimRaw
    = merge(Base.ReclaimRaw, ReclaimTargetUpdate)
    ∪ committed active RampUp floor
    ∪ current request floor
```

### 原子调整

Planner 必须在一个 `next TargetState` 内同步完成：

1. reclaim pool 设置为 `FinalReclaimRaw`；
2. 所有 share pool allocation 删除 `FinalReclaimRaw`；
3. 所有 `RampUp=true` container allocation 删除 `FinalReclaimRaw`；
4. 更新 `OriginalAllocationResult`；
5. 重建受影响 allocation 的 `TopologyAwareAssignments`；
6. 重建 `OriginalTopologyAwareAssignments`；
7. 重建与 allocation 一致的 `MachineState`；
8. 保留未受影响 NUMA 和其它 target 字段。

Planner 不依赖 Materializer。Materializer 不允许纠正 Planner 输出。

### Target validation

所有 runtime transaction 提交物化前统一验证：

```text
FinalReclaimRaw ⊇ EffectiveHardFloor

ReclaimRaw ∩ SharePools = ∅

ReclaimRaw ∩ RampUpContainerAllocations = ∅

Reserve、Reclaim、NonReclaim
共同覆盖合法 machine CPUs

每个 TopologyAwareAssignments
等于 AllocationResult 的 NUMA 投影

MachineState
与 PodEntries 的 allocation 和 NUMA ownership 一致
```

validator 返回 typed error，不做自动修复。

## Bulkhead Manager

Bulkhead Manager 实现唯一接口：

```go
func (m *Manager) Materialize(
    ctx context.Context,
    target cpusetmaterializer.Target,
) (cpusetmaterializer.Result, error)
```

删除：

- `Apply`；
- `RunCPUSetAdjustmentHandlers`；
- `CPUSetAdjustmentHandlerCtx`；
- state-based view build；
- `LatestAppliedReclaim`；
- `PublishAppliedReclaim`；
- 两套成功合同。

Manager 私有职责：

```text
Target
→ build private immutable execution view
→ select enabled private plugins
→ unique topology executor
→ require fresh full convergence
→ dependent plugins
→ aggregate evidence
→ Result
```

Bulkhead 全局开关关闭时，不构造 Manager、不注入 DynamicPolicy。
DynamicPolicy 因 materializer 为 nil 直接视为收敛。插件 enable/disable
属于 Manager 私有配置，不泄漏到 DynamicPolicy。

Manager 可以保留内部 plugin registry，因为它是 Bulkhead 实现细节；
DynamicPolicy 侧不再有第二个 registry。

## CpusetTopology

CpusetTopology 是 Bulkhead Manager 的私有 topology executor，不再实现
generic CPUSet adjustment wrapper。

职责：

- runtime container rel discovery；
- pending container observation；
- 验证 pending protection 已包含在输入 target 的 non-reclaim domain；
- sibling discovery；
- topology node spec；
- DAG apply；
- complete controlled-rel fresh read；
- convergence evidence；
- full convergence 后 prune。

### 输入不可变

禁止：

```go
ApplyTransientProtectedNonReclaim(in.Target, ...)
```

CpusetTopology 只能基于输入 target 生成 cgroup 写序和运行时保护约束，
不能派生另一份 CPUSet target。dependent plugins 始终消费同一输入 target。

### Pending protection

Pending protection 是 runtime execution constraint，不是策略目标：

- 不写 `TargetState`；
- 不修改 materialization input；
- 只能保护已属于 `NonReclaimCPUSet` 的 CPU；
- 若 pending protection 与 `ReclaimCPUSet` 相交，本轮返回不收敛及冲突证据；
- 仅影响本轮 cgroup 写序和 convergence，不能改变最终目标；
- cache 由 CpusetTopology 自己加锁，不能依赖 Manager 外部锁；
- stale protection 只按明确 TTL/identity 规则清理。

### Complete convergence

Convergence 必须基于显式 controlled-rel inventory，而不是仅遍历 DAG
实际节点：

- 配置中的 primary/system/reclaim/reclaim-per-NUMA rel；
- dynamic reclaim siblings；
- 当前可解析 container leaves；
- 期望为空的 controlled rel。

任何 missing、extra、unreadable、target mismatch 或 containment breach
均返回 `Converged=false`。

## 统一事务

```go
func (p *DynamicPolicy) transact(
    ctx context.Context,
    plan TargetPlan,
) error {
    p.Lock()
    defer p.Unlock()

    if err := p.requireCPUPolicyReady(); err != nil {
        return err
    }

    base, err := p.state.PrepareDurableTarget()
    if err != nil {
        return err
    }

    next, err := plan(base)
    if err != nil {
        return err
    }
    if err := ValidateTarget(next, p.machineInfo.CPUTopology); err != nil {
        return err
    }

    target := BuildMaterializationTarget(next, p.machineInfo.CPUTopology)
    _, err = p.materialize(ctx, target)
    if err != nil {
        return p.restoreBaseOrBlock(ctx, base, err)
    }

    if err := p.state.CommitTarget(next); err != nil {
        return p.restoreBaseOrBlock(ctx, base, err)
    }

    return nil
}
```

`materialize` 统一 nil 语义、结果验证和 typed errors。

### Failure

| 阶段 | 失败行为 |
|---|---|
| plan | 不物化、不提交 |
| validate | 不物化、不提交 |
| materialize error | 恢复 durable Base |
| materialize non-converged | 转 typed error，恢复 Base |
| materialized layout 与 target 不一致 | 不收敛，恢复 Base |
| commit error | 恢复 Base |
| Base restore error | CPU policy 进入 blocked |
| post-commit notification error | 不回滚已提交 target |

## Legacy delete-first

### Anti-Entropy Declaration

- Deletion Class：internal code retirement
- Old Path：影子 Policy、legacy state writers、CPUSet handler registry、Manager 双入口
- New Canonical Owner：DynamicPolicy transaction + cpusetmaterializer contract
- Preserved Behavior：外部 QRM API、checkpoint schema、Bulkhead plugin 功能
- Retired Behavior：内部双 owner、state-based materialization、mode-bit planning
- External Boundary Touched：no
- Source-of-Truth Data Risk：none
- User Confirmation Required：no

### 必删符号与路径

- `newPlanningPolicy`
- `planningState`
- `planningRampUpAdmission`
- `storeRequested`
- legacy cpuset adjustment handler type
- `CPUSetAdjustmentHandlerCtx`
- `RegisterCPUSetAdjustmentHandler`
- `cpuSetAdjustmentHandlers`
- `runCPUSetAdjustmentHandlers`
- `runCPUSetAdjustmentHandlersWithState`
- `runRegisteredCPUSetAdjustmentHandlers`
- 旧 `materializeCPUSetTarget` 分支实现
- `Manager.Apply`
- `Manager.RunCPUSetAdjustmentHandlers`
- `Manager.PublishAppliedReclaim`
- `Manager.LatestAppliedReclaim`
- Manager state-based view builder
- `TargetState` legacy writer methods
- runtime `State.Set* → StoreState`
- `ReclaimHardConstraint.CommittedRaw`
- effective floor owner exclusion
- CpusetTopology generic adjustment wrapper
- 旧 disabled handler 双路径
- 不收敛返回 nil 的兼容逻辑
- 已删除接口专用 tests/fixtures

不保留：

- deprecated wrapper；
- type alias；
- forwarding method；
- compatibility adapter；
- 双入口；
- “暂时保留但无生产调用”的定义。

### 无核心调用代码清理

实施结束后对本次涉及 package 做可达性审计。满足以下全部条件的函数、类型、
字段、常量和测试 fixture 直接删除：

1. production main path 无调用；
2. 不是外部公开 API；
3. 不承担 checkpoint wire format；
4. 不用于注册反射、生成代码或 build tag；
5. 唯一调用方是已删除 legacy path 或仅验证该 legacy path 的测试。

不能只依据 Go compiler 的 exported 可见性保留代码。若 exported symbol
仅在仓库内使用，必须搜索跨 module 调用；没有外部契约证据时按 internal
code retirement 删除。

### 保留边界

只保留：

- 外部 QRM API；
- checkpoint wire DTO；
- 已有 checkpoint 读取兼容；
- Bulkhead plugin 对外配置字段；
- 有真实生产调用的 periodical 接口。

## 文件级修改

| 文件 | 修改 |
|---|---|
| `cpusetmaterializer/interface.go` | 新增唯一 Materializer 接口 |
| `cpusetmaterializer/target.go` | 新增不可变 target 与 builder 输入类型 |
| `cpusetmaterializer/result.go` | 新增 result/evidence/typed errors |
| `policy.go` | 注入唯一 materializer，删除 handler map 与 planning mode |
| `policy_lifecycle.go` | 删除影子 Policy，保留统一 transact/recovery/readiness |
| `policy_allocation_handlers.go` | planner 化 allocation/RampUp helper |
| `policy_advisor_handler.go` | 使用完整 target planner |
| `policy_async_handler.go` | residual/system-exclusive 使用 target planner |
| `policy_irq_tuner.go` | IRQ 使用 target planner |
| `policy_resource_package.go` | resource-package 使用 target planner |
| `cpuset_adjustment_handler.go` | 删除；中立 builder/调用逻辑迁入明确文件 |
| `util/cpuset_adjustment.go` | 删除 |
| `planner/ramp_up_reclaim_admission.go` | 完整更新 reclaim/share/container/MachineState |
| `planner/target_validator.go` | 新增统一策略不变量验证 |
| `state/target_state.go` | 删除 legacy mutable State adapter |
| `state/state.go` | runtime repository 只保留 readonly + prepare/commit |
| `bulkhead/api/types.go` | 删除 legacy handler context；使用中立 target |
| `bulkhead/manager.go` | 合并为 Materialize，删除双入口和 applied store |
| `bulkhead/plugins/cpusettopology/plugin.go` | 输入不可变、pending 冲突验证、单一 reconcile |
| `bulkhead/utils/view.go` | 降为 Bulkhead 私有执行 view，不接受 State |
| `bulkhead/utils/topology_inputs.go` | 显式 controlled-rel inventory |

实际实施中若文件清理后内容为空，直接删除文件，不保留空壳。

## 测试

### Contract

| 场景 | 预期 |
|---|---|
| materializer nil | 直接 converged |
| materializer success | 精确收敛后 commit |
| materializer error | 恢复 Base |
| `Converged=false,nil` | typed error，不 commit |
| observed layout 与 target 不一致 | 不收敛，不 commit |
| materializer 修改输入 | immutable contract test 失败 |
| 第二个 materializer | 构造模型不存在注册入口 |

### RampUp

| 场景 | 预期 |
|---|---|
| Full reclaim update | reclaim/share/container/MachineState 一致 |
| Partial NUMA update | 未声明 NUMA 保留 |
| active RampUp floor | 从 Base 派生 |
| current request floor | 与 active floor union |
| reclaim/share overlap | validator 拒绝 |
| reclaim/RampUp container overlap | validator 拒绝 |
| topology assignment mismatch | validator 拒绝 |
| planner base mutation | Base 保持不变 |

### Bulkhead

| 场景 | 预期 |
|---|---|
| Manager Materialize | 唯一公开物化入口 |
| topology non-converged | Result false/error，不运行 dependent plugin |
| dependent plugin failure | 整体失败 |
| pending protection | 输入 target 不变 |
| empty per-NUMA rel | 仍进入 controlled inventory |
| rel missing/mismatch | 不收敛 |
| Bulkhead disabled | Manager 不注入 DynamicPolicy |

### Static retirement

必须有静态 contract test 或 repository scan，确认 production code 不存在：

```text
newPlanningPolicy
planningState
planningRampUpAdmission
legacy cpuset adjustment handler type
RunCPUSetAdjustmentHandlers
Manager.Apply
TargetState.Set*
State.StoreState runtime call
```

对本次涉及目录执行 unused/dead-code 审计，确保没有仅由废弃测试引用的空壳定义。

### 验证命令

```text
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
go vet ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
git diff --check
```

E2E 固定顺序：

```text
stable RampUp
→ delete/recreate
→ high churn
→ restart recovery
→ reset cleanup
```

每轮必须验证：

- target validator 成功；
- reclaim/share/container 无 overlap；
- materialization convergence；
- checkpoint 与 live target 一致；
- failure 不 commit；
- 结束后 RUN_TAG Pod/state 为 0。

## 兼容与回滚

- 外部 QRM API 不变；
- checkpoint schema 不变；
- SysAdvisor proto 不变；
- 旧 checkpoint 可直接读取；
- Bulkhead 关闭时 materializer 为 nil；
- 不保留内部 legacy fallback；
- feature gate 回滚只能关闭 identity preference，不能恢复双 owner 或 state-based materialization。

## 验收标准

1. DynamicPolicy 是唯一策略与事务 owner。
2. `TargetState` 是唯一完整策略事实源。
3. planner 不创建影子 DynamicPolicy。
4. RampUp planner 一次产生完整一致 target。
5. 中立 `cpusetmaterializer` 包不依赖 Bulkhead 或 DynamicPolicy。
6. DynamicPolicy 最多持有一个 materializer。
7. Bulkhead Manager 只有一个 `Materialize` 入口。
8. Manager 不读取 state，不保存 runtime applied state。
9. CpusetTopology 不修改输入 target。
10. materializer nil 时直接收敛。
11. materializer 非 nil 时成功必须蕴含 full convergence。
12. DynamicPolicy 和 Manager 均不缓存 applied reclaim；committed `ReclaimRaw` 是唯一身份事实源。
13. checkpoint-first transaction 保持不变。
14. 所有内部 legacy path、wrapper、adapter 和无核心调用定义均删除。
15. dynamicpolicy/bulkhead tests、race、vet、diff check 通过。
16. stable、recreate、churn、restart E2E 通过并完成 reset。

## 最终不变量

```text
一个策略目标
一个事务 owner
一个 materializer
一个物化入口
一个成功合同
```

```text
RampUp 成功
⇒ 完整 TargetState 合法
且 CPUSet materialization 已收敛
且 checkpoint 已提交
```

```text
Materializer 只执行目标
不规划目标
不修改目标
不提交目标
```

```text
内部旧路径没有兼容价值时直接删除
```
