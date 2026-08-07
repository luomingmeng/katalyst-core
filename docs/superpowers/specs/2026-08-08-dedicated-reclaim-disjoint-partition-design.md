# Dedicated 与 Reclaim 解耦分区设计

## 背景

当前 CPU Advisor 同时支持 shared、dedicated、isolation、reclaim、NUMA binding、
NUMA exclusive 和 Resource Package。`AllowSharedCoresOverlapReclaimedCores`
可以控制 shared 与 reclaim 是否重叠，
`DisableDedicatedCoresOverlapReclaimedCores` 也已经出现在动态配置、API 和
QRM state 中，但后者尚未形成端到端闭环。

主要缺口包括：

- Sysadvisor assembler 没有把 `DisableDedicatedCoresOverlapReclaimedCores`
  传播到最终计算结果。
- CPU server 的同步与异步响应没有携带该字段。
- `assembleWithoutNUMAExclusivePool` 仍然只能选择“整体 overlap”或“整体
  non-overlap”，无法表达 shared 可重叠而 dedicated 不可重叠。
- `assembleDedicatedNUMAExclusiveRegion` 总是把 reclaim 表达成 dedicated
  Pod/container 的 overlap block。
- QRM `generateBlockCPUSet` 只对部分 pool 稳定复用旧 CPUSet，仍可能因为
  map 顺序、BlockId 变化或 source carve 重算而跳核。
- `applyBlocks` 之后的 reclaim fallback 可能重新注入 dedicated CPU，
  破坏已经生成的 disjoint partition。
- `allocateNumaBindingCPUs` 用原始 CPU request 检查 exclusive-DNB 的最终
  cpuset，无法表达“优先保留 reclaim reserve，dedicated 使用剩余 CPU”。

## 目标

当 `DisableDedicatedCoresOverlapReclaimedCores=true` 时：

- dedicated 与 reclaim 在最终物理 CPUSet 上严格不重叠。
- Pod 是否贡献 opportunistic reclaim 仅由 Pod `EnableReclaim` 决定。
- `ReserveForReclaim` 始终优先保留。
- exclusive-DNB 可以得到小于原始 CPU request 的结果。
- 普通 DNB 可以在必要时占用之前属于 SNB/shared pool 的 CPU，但不能占用
  reclaim-only reserve。
- shared 与 reclaim 是否重叠仍由
  `AllowSharedCoresOverlapReclaimedCores` 独立决定。
- 所有 pool 优先复用之前的 CPUSet，避免无业务变化时跳核。
- Resource Package pinned、unpinned 和 non-reclaimable 约束在所有分配路径
  中一致生效。

当 `DisableDedicatedCoresOverlapReclaimedCores=false` 时，保持现有行为，
包括 exclusive-DNB whole-NUMA overlap、quota 模式和旧 validator 约束。

## 非目标

- 不新增一套替代 CPU Advisor 的 wire schema。
- 不把 `non-reclaimed-cpu-requirement` 的策略计算复制到 QRM。
- 不改变 shared/reclaim overlap 的既有配置语义。
- 不在本次设计中改变 Resource Package 的配置模型。
- 不把 advisor `BlockId` 持久化为跨帧状态。

## Source of truth

职责边界如下：

- Sysadvisor 决定每个 block 的数量、owner、NUMA scope 和 overlap 关系。
- QRM 决定具体 CPU ID，并保证拓扑、旧 CPUSet 复用、Resource Package 和
  disjoint 约束。
- `BlockId` 只用于一帧 response 内的 alias，不是跨帧稳定身份。
- Pod `EnableReclaim` 是 dedicated 是否贡献 opportunistic reclaim 的唯一
  source of truth。
- `ReserveForReclaim` 是不可被 opportunistic 策略缩减的硬下限。

## 符号

对一个 NUMA scope 或 fake-NUMA scope：

```text
S  = 扣除 static/system/forbidden 后的 scope CPUSet
D  = 当前 dedicated owner 在 S 内合法使用的 CPUSet
G  = reclaim 在 S 内合法使用的 CPUSet
P  = D union G
A  = |P|
R  = ReserveForReclaim
N  = NonReclaimedCPURequirement
E  = Pod 或 region 的 EnableReclaim
DD = DisableDedicatedCoresOverlapReclaimedCores
AS = AllowSharedCoresOverlapReclaimedCores
Q  = ReclaimedCoresCPUQuota
NRP = selector 禁止 reclaim 的未使用 pinned CPU 数量
```

输入首先规范化：

```text
A = |P|
R = clamp(R, 0, |G|)
N = clamp(N, 0, |D|)
```

`ReclaimedCPUMaxRatio` 继续通过现有 `clampByReclaimedCPUMaxRatio`
执行，但结果必须满足：

```text
finalReclaim >= R
finalReclaim <= A
```

## 总体不变量

对每个真实 NUMA：

```text
reclaim subset-of physicalNUMA
dedicated subset-of physicalNUMA
```

当 `DD=true`：

```text
dedicated intersect reclaim = empty
```

当 `AS=false`：

```text
shared intersect reclaim = empty
```

当 exclusive-DNB 且 `DD=true`：

```text
dedicated union reclaim = partitionEligible
dedicated intersect reclaim = empty
```

其中：

```text
partitionEligible = P = D union G
dedicated subset-of D
reclaim subset-of G
```

`partitionEligible` 不一定等于整个物理 NUMA。

## 配置传播

`DisableDedicatedCoresOverlapReclaimedCores` 必须完成以下链路：

```text
dynamic configuration
-> ResourceEssentials
-> InternalCPUCalculationResult
-> cpuInternalResult
-> GetAdviceResponse/ListAndWatchResponse
-> QRM unified ListAndWatchResponse
-> CPU plugin state
```

同步 `GetAdvice` 和异步 `ListAndWatch` 必须具有相同语义。任何中间层都不能
使用默认 `false` 覆盖上游值。

## Exclusive-DNB assembler

### 兼容路径

当 `DD=false` 时保持现有公式和 block 表达。

```text
E=true:
    reclaimTarget = max(R, A-N)

E=false:
    reclaimTarget = R
```

非 quota 模式：

```text
dedicated physical size = A
reclaim overlap size = reclaimTarget
reclaim standalone size = 0
```

quota 模式继续允许 reclaim cpuset 覆盖 whole NUMA，并通过 quota limit 控制
实际可用量。

### Disjoint 路径

当 `DD=true`：

```text
E=true:
    reclaimTarget = max(R, A-N)

E=false:
    reclaimTarget = R

reclaimTarget = ratioClamp(reclaimTarget)
dedicatedTarget = A-reclaimTarget
```

还必须满足 eligibility 下界和上界：

```text
reclaimLowerBound =
    max(
        R,
        |P-D|,     // 只能由 reclaim 覆盖的 CPU
        A-|D|,     // 保证 dedicatedTarget <= |D|
    )

reclaimUpperBound =
    min(
        |G|,
        A-1,       // dedicated 必须非空
    )
```

计算顺序：

```text
candidate = E ? max(R, A-N) : R
candidate = ratioClamp(candidate)

if candidate < reclaimLowerBound:
    return error
if candidate > reclaimUpperBound:
    return error

reclaimTarget = candidate
dedicatedTarget = A-reclaimTarget
```

不能通过把 candidate 静默调整到 eligibility bound 掩盖配置冲突。

输出两个独立 block：

```text
dedicated block size = dedicatedTarget
reclaim-only block size = reclaimTarget
dedicated overlap metadata = none
```

region 中每个 Pod 对应：

```go
result.SetPoolEntry(podUID, numaID, dedicatedTarget, -1)
```

reclaim 对应：

```go
result.SetPoolEntry(commonstate.PoolNameReclaim, numaID, reclaimTarget, quota)
```

禁止调用 dedicated 的 `SetPoolOverlapPodContainerInfo`。

必须满足：

```text
reclaimTarget >= R
dedicatedTarget + reclaimTarget = A
0 < dedicatedTarget <= |D|
R <= reclaimTarget <= |G|
```

`dedicatedTarget==0` 时返回错误，避免生成空 exclusive-DNB allocation。

### Quota 与 ratio

分别定义：

```text
physicalReclaimTarget = reclaimTarget
ratioPhysicalCap =
    ratio <= 0
        ? unlimited
        : max(R, evenFloor(ratio*physicalCPUCount))
reclaimQuotaLimit = reclaim block 的运行 quota
```

`evenFloor` 与现有实现一致，先向下取整，再向下对齐偶数。

`DD=true` 时：

```text
physicalReclaimTarget =
    min(rawPhysicalTarget, ratioPhysicalCap)
```

若 ratio cap 使 physical target 低于 `reclaimLowerBound`，返回错误；不能破坏
eligibility coverage。

quota control 规则：

```text
quota knob 缺失或值 < 0:
    reclaimQuotaLimit = -1

quota knob >= 0:
    reclaimQuotaLimit = min(quota knob, physicalReclaimTarget)

ratio cap 开启且 reclaimQuotaLimit >= 0:
    reclaimQuotaLimit = min(reclaimQuotaLimit, ratioPhysicalCap)
```

`ReserveForReclaim` 是物理 CPUSet 下限，不是 quota 下限。因此 `Q<R` 或
`Q=0` 不缩小物理 reserve，只限制 reclaim workload 的运行 quota。

`DD=false` 的 quota size、limit、whole-NUMA overlap 和 clamp 顺序逐字段保持
旧行为。

## 普通 DNB assembler

`assembleWithoutNUMAExclusivePool` 不再只根据 `AS` 在
`calculateOverlapReclaimPool` 和 `calculateNonOverlapReclaimPool` 之间二选一，
而是分别计算 shared 和 dedicated policy。

对 dedicated pool `j`：

```text
rawRequest_j        = region request
rawRequirement_j    = non-reclaimed requirement
desiredPhysical_j   = 启用 DD 公式后的目标
regulatedPhysical_j = 容量调节后的最终 dedicated size
enable_j            = Pod EnableReclaim
```

期望 dedicated size：

```text
DD=false:
    desiredPhysical_j = rawRequest_j

DD=true && enable_j=false:
    desiredPhysical_j = rawRequest_j

DD=true && enable_j=true:
    desiredPhysical_j = min(rawRequest_j, rawRequirement_j)
```

`ReserveForReclaim` 优先于所有 dedicated request。容量不足时，使用现有
`regulatePoolSizes` 的确定性比例规则调节 dedicated pools：

```text
0 <= regulatedPhysical_j <= desiredPhysical_j
```

即使 `enable_j=false`，也允许为了 mandatory reserve 把 dedicated 压到
request 以下；这不是 opportunistic reclaim，而是节点硬分区约束。

任一仍有 active Pod 的 dedicated pool 被压到 0 时，整帧返回错误。

释放给 standalone reclaim 的容量：

```text
dedicatedFreed_j = rawRequest_j-regulatedPhysical_j
```

dedicated overlap：

```text
DD=true:
    dedicatedOverlap_j = 0

DD=false && enable_j=true:
    dedicatedOverlap_j = max(rawRequest_j-rawRequirement_j, 0)

DD=false && enable_j=false:
    dedicatedOverlap_j = 0
```

shared overlap：

```text
AS=true && shareEnable_i=true:
    sharedOverlap_i = max(shareSize_i-shareRequirement_i, 0)

otherwise:
    sharedOverlap_i = 0
```

因此以下组合可以被准确表达：

```text
AS=true
DD=true
shared/reclaim overlap allowed
dedicated/reclaim overlap forbidden
```

## 容量调节顺序

当 `DD=true` 时，容量按以下优先级调节：

```text
1. ReserveForReclaim
2. dedicated non-reclaimed requirement
3. isolation lower bound
4. shared/SNB minimum requirement
5. dedicated extra request
6. shared/SNB expansion
```

如果 `R + sum(desiredPhysical) > effectiveCapacity`，按确定性比例继续压缩
dedicated，不能缩减 reserve，也不能回退到 overlap。

普通 DNB 的 CPU ID 可以从之前属于 SNB/shared pool 的 CPU 中选择，但这是
QRM planner 的 preference，不改变 Sysadvisor 的数量公式。

## 总 reclaim 公式

容量不能只按整个 NUMA 的标量计算。先按 Resource Package eligibility
domain 分组；每个 domain 分别计算：

```text
effectiveCapacity_domain =
    |domain reclaim-eligible CPUs|

physicalPoolUsage =
    sum(regulatedPhysical)
  + sum(sharePoolSizes)
  + sum(isolationPoolSizes)

freeStandalone_domain =
    max(effectiveCapacity_domain-physicalPoolUsage_domain, 0)
```

NUMA scope 的 `freeStandalone` 是各 domain 可行 free capacity 的总和。禁止
用全局空闲标量替代 domain 可行性。

构造 overlap atoms：

```text
share atoms = sharedOverlap_i
dedicated atoms = dedicatedOverlap_j
```

原始 target：

```text
rawReclaimTarget =
    max(
        R,
        freeStandalone
        + sum(shared overlap atoms)
        + sum(dedicated overlap atoms),
    )
```

应用 ratio cap：

```text
reclaimTarget = ratioClamp(rawReclaimTarget)
```

clamp 顺序：

```text
1. 保留 ReserveForReclaim
2. 缩小 overlap atoms
3. 缩小额外 standalone reclaim
4. 禁止低于 ReserveForReclaim
```

最终输出：

```text
overlapBudget = min(reclaimTarget, sum(selected overlap atoms))
standaloneReclaim = reclaimTarget-overlapBudget
```

```go
result.SetPoolEntry(commonstate.PoolNameReclaim, numaID, standaloneReclaim, quota)
```

shared atoms 使用 `SetPoolOverlapInfo`。dedicated atoms 仅在 `DD=false` 时使用
`SetPoolOverlapPodContainerInfo`。

## Overlap atom 与 aliases

同一个物理 overlap block 可能被 main/sidecar 等多个 container 引用。
这些 container 是 aliases，不是独立容量。

```text
atom.size = 4
main alias = 4
sidecar alias = 4
aggregate physical usage = 4
```

`clampReclaimOverlapMetadata` 必须先按物理 atom 扣减 budget，再把最终 size
写回所有 aliases。不能按 container 逐项消耗 budget，也不能依赖 container
名称排序删除后续 alias。

## Resource Package

每个 owner 的 eligible CPUSet：

```text
owner has pinned package:
    eligible = S intersect packagePinnedCPUSet

owner has no package:
    eligible = S difference allPinnedCPUSets

reclaim:
    eligible = S difference nonReclaimablePinnedCPUSet
```

alias block：

```text
blockEligible = intersection(ownerEligible...)
```

alias owners 属于不兼容 package 或交集不足时，整帧 fail-closed。

exclusive-DNB：

```text
owner has pinned package:
    D = S intersect packagePinnedCPUSet

owner has no package:
    D = S difference allPinnedCPUSets

G = S difference nonReclaimablePinnedCPUSet
partitionEligible = dedicatedEligible union reclaimEligible
```

coverage 和 disjoint 校验针对 `partitionEligible`，而不是整个物理 NUMA。

当 `D intersect G` 非空时，dedicated/reclaim 必须联合求解。reclaim 优先选择
`G-D`，然后才从 `D intersect G` 取 reserve；dedicated 使用剩余的 D。
若 quantity 满足但集合约束不存在可行 assignment，整帧失败。

reserve placement preference：

```text
1. unpinned reclaimable free capacity
2. reclaimable package unused capacity
3. reclaimable package workload slack
4. never use non-reclaimable package
```

## QRM block planner

`generateBlockCPUSet` 在分配 CPU 前先把 response 规范化为 descriptors：

```go
type blockDescriptor struct {
    blockID        string
    numaID         int
    class          blockClass
    quantity       int
    owners         []blockOwner
    eligible       machine.CPUSet
    preferred      machine.CPUSet
    stableOwnerKey string
}
```

`BlockId` 只作为当前 response 的引用。跨帧 preference 以 owner component
为粒度从当前 state 反查：

```text
NUMA scope
+ canonical sorted owner identities
+ block class
+ Resource Package domain
```

该 key 不承诺唯一标识 component 内的每个物理 block。多个 block 具有相同
component key 时，稳定性合同是：

```text
owner/component 聚合 CPUSet 保持稳定
单个 response-local block 边界可以重新匹配
```

component 内按 quantity、canonical alias signature 排序；`BlockId` 只作为
最终 deterministic tie-break。BlockId 全量变化时，owner 聚合 CPUSet 仍必须
不变。

所有动态 pool 使用统一选择器：

```text
Tier 0: 旧 CPUSet 中仍满足全部约束的 CPU
Tier 1: 同 source pool 或同 owner component 的旧 CPU
Tier 2: 同 NUMA、core/socket 迁移代价最低的 free CPU
Tier 3: 其他合法 CPU
```

稳定性要求：

```text
size unchanged:
    new = old, unless an old CPU becomes illegal

grow N:
    keep all legal old CPUs and add exactly N

shrink N:
    remove exactly N and keep the remainder

one old CPU becomes illegal:
    replace only that CPU
```

NUMA 按 ID 升序处理，fake NUMA 最后处理。block 按 class、owner signature、
Resource Package 和 BlockId 排序。不得依赖 Go map 遍历顺序。

## Planner 阶段顺序

`DD=true`：

```text
1. static/system/forbidden
2. dedicated + reclaim-only mandatory partition 联合求解
4. NUMA-binding shared/SNB/isolation
5. global shared/source pools
6. remaining shared/reclaim overlap blocks
```

第二阶段不能使用 reclaim-first 贪心。对 eligibility 有竞争的 dedicated 与
reclaim descriptors 建立 CPU-to-partition 容量图，执行确定性的最小代价
可行分配：

```text
硬约束:
    quantity、D/G eligibility、disjoint

代价优先级:
    0: 保留旧 owner CPU
    1: reclaim 使用 G-D
    2: 使用同 component 旧 CPU
    3: topology migration cost
```

可使用确定性的 min-cost max-flow，或者等价的带增广路径容量匹配。禁止使用
无法证明可行性的单向贪心。无合法解时不产生部分 plan。

普通 DNB preference：

```text
1. old DNB CPUSet
2. free non-reclaim CPU
3. old compatible SNB/shared CPUSet
4. other eligible CPU
```

source pool 和 isolation carve 必须联合考虑旧 source 与旧 isolation CPUSet，
不能先重新选择 source candidate 再 carve。

## `applyBlocks`

`applyBlocks` 不再选择或扩展 CPUSet，只负责：

- 将 planner 输出转换成 pool/container entries。
- 运行 allocation hooks。
- 构造 machine state。
- 执行最终 partition validation。
- 使用 state revision 原子提交。

当 `DD=true` 时：

- `reviseReclaimPool` 不得扩大 planner 的 reclaim CPUSet。
- reserved fallback 必须在 planner 阶段完成。
- `buildAdjustmentCommitOverride` 不能只由 `AS=false` 触发。
- mixed 模式 `AS=true && DD=true` 也必须保护 dedicated/reclaim disjoint。
- checkpoint 失败必须返回错误并恢复内存 desired state。

Sysadvisor 稳态 advice 将持续输出 exclusive reclaim partition，因此
`applyBlocks` 在第一帧清除 `RampUp` 后，下一帧仍然保留 partition，不再依赖
transient `RampUp` 标记维持 floor。

### 事务边界

`applyBlocks` 提交的是 desired state，不直接保证 cgroup 已经同步：

```text
纯计算:
    normalize -> plan -> validate -> build target

durable commit:
    PodEntries + MachineState + flags + checkpoint

eventual reconcile:
    cgroup/cpuset adjustment、headroom、外部配置
```

allocation hooks 必须是纯 target mutation；禁止包含不可回滚外部副作用。
现有 hooks 在实施前需要审计。若发现外部副作用，必须移到 durable commit
后的幂等 reconcile。

durable commit 失败时 desired state 保持不变。durable commit 成功但 cgroup
apply 失败时，desired state 不回滚，记录 degraded 状态并重试 reconcile。
不得同时声称“state 未变化”和“外部 reconcile 最终一致”。

## Validator

### Response 结构校验

- `DD=true` 时 dedicated/reclaim 不得共享 BlockId。
- `AS=false` 时 shared/reclaim 不得共享 BlockId。
- `AS=true` 时 shared/reclaim 允许共享 BlockId。
- aliases 的 Resource Package domain 必须兼容。
- 同一 BlockId 的 NUMA 和 quantity 必须唯一。

### Planner 输出校验

- 每个 block 数量精确满足。
- 每个 CPU 属于 descriptor eligible。
- owner CPUSet 等于其 blocks 的并集。
- pinned owner 不越出 package。
- unpinned owner 不占 exclusive pinned CPU。
- reclaim 不进入 non-reclaimable package。

### Commit 前校验

- `DD=true` 时 dedicated/reclaim 全局和逐 NUMA 互斥。
- exclusive-DNB 覆盖 `partitionEligible`。
- 必需 partition 非空。
- state revision 未变化。

现有 dedicated quantity validator：

```text
DD=false:
    保持 advice quantity 等于旧 allocation size

DD=true:
    允许 advice quantity 按合法 partition 缩小
```

## `allocateNumaBindingCPUs`

`leftNumCPUs` 检查改为：

```text
non-exclusive:
    result size must satisfy request

exclusive && DD=false:
    result size must satisfy request

exclusive && DD=true:
    result may be smaller than request
    result must be non-empty
    result intersect reclaim = empty
    result union reclaim = partitionEligible
```

admission 阶段没有 Sysadvisor 的 non-reclaimed requirement，因此先建立
`ReserveForReclaim` 最小 partition。稳态 advice 到达后，再根据控制量扩大
reclaim 并最小化缩小 dedicated。

Pod `EnableReclaim=false` 或查询失败时，不贡献 opportunistic reclaim，但
reserve 仍然保留。

## 错误语义

以下情况必须 fail-closed：

- reclaim reserve 无合法 CPU。
- aliases 的 Resource Package 约束交集不足。
- dedicated/reclaim 或禁止 overlap 的 shared/reclaim 共享 block。
- planner 无法满足 block quantity。
- exclusive partition 为空、相交或 coverage 不完整。
- checkpoint 写入失败。
- state revision 已变化。

禁止通过缩小 reserve、临时开启 overlap 或忽略 Resource Package 约束降级。

## 兼容与能力协商

普通 proto3 `bool` 无法区分“明确 false”和“旧端未发送”。因此新增
CPU Advisor capability：

```text
DedicatedReclaimDisjointPartition
```

协商规则：

```text
new QRM + new Sysadvisor:
    QRM request wanted feature
    Sysadvisor response supported feature
    DD=true 时启用新 assembler/planner

new QRM + old Sysadvisor:
    DD=true 时拒绝 legacy advice，保留之前 desired state 并重试

old QRM + new Sysadvisor:
    未请求 capability，Sysadvisor 输出 legacy DD=false 语义
```

`DD=true` 必须依赖 capability，禁止仅依赖默认 bool。

flag transition：

```text
false -> true:
    仅接受带 capability 的新帧
    通过新 planner 迁移到 disjoint partition

true -> false:
    接受 legacy overlap 语义

stale frame:
    通过 state revision 和当前 negotiated mode 拒绝
```

## 兼容边界

`DD=false` 时保持：

- existing whole-NUMA exclusive overlap。
- existing quota-mode whole-NUMA reclaim cpuset。
- existing dedicated quantity validator。
- existing overlap metadata wire format。

未协商 capability 或 `DD=false` 时继续使用 legacy planner/apply 路径。
新 descriptor planner 仅在 capability 已协商且 `DD=true` 时启用，避免把
本次重构扩展为所有 legacy 流量的隐式迁移。

## 可观测性

新增或补充以下指标：

- advisor response 中两个 overlap flag。
- 每个 block 的 reused、added、removed CPU 数量。
- forced migration CPU 数量与原因。
- dedicated/reclaim disjoint validation failure。
- Resource Package constraint failure。
- reclaim reserve fallback 来源。
- exclusive partition 中 dedicated/reclaim size。

日志必须包含 stable owner key、BlockId、NUMA、old/new CPUSet 和 eligibility，
但错误消息保持小写。

## 测试矩阵

### Exclusive assembler

固定 `A=16`：

| DD | E | R | N | ratio | dedicated | reclaim-only | overlap |
|---:|---:|---:|---:|---:|---:|---:|---:|
| false | false | 4 | 8 | off | 16 | 0 | 4 |
| false | true | 4 | 10 | off | 16 | 0 | 6 |
| true | false | 4 | 8 | off | 12 | 4 | 0 |
| true | true | 4 | 10 | off | 10 | 6 | 0 |
| true | true | 4 | 3 | off | 3 | 13 | 0 |
| true | true | 4 | 15 | off | 12 | 4 | 0 |
| true | true | 4 | 10 | 0.25 | 12 | 4 | 0 |
| true | true | 4 | 0 | off | error | error | 0 |

额外覆盖 quota on/off、quota 边界、`R > ratio cap`、`N<0`、`N>A`、
多 container aliases 和 Pod reclaim 查询失败。

quota 单独覆盖：

| Q | physical reclaim | quota limit |
|---:|---:|---:|
| missing | 6 | -1 |
| -1 | 6 | -1 |
| 0 | 6 | 0 |
| 2 (`Q<R`) | 6 | 2 |
| 4 (`Q=R`) | 6 | 4 |
| 5 | 6 | 5 |
| 8 | 6 | 6 |

并覆盖 quota 与 ratio 同时开启、ratio 奇数结果向下偶数对齐，以及 `DD=false`
quota response 的逐字段 golden compatibility。

### 普通 DNB policy

| AS | DD | shared E | dedicated E | 期望 |
|---:|---:|---:|---:|---|
| false | false | false | false | shared、DNB 都 non-overlap |
| false | false | true | true | shared non-overlap；DNB legacy overlap |
| true | false | true | true | shared、DNB 都可 overlap |
| true | false | false | true | shared non-overlap；DNB legacy overlap |
| true | true | true | true | shared overlap；DNB disjoint shrink |
| false | true | true | true | shared、DNB 都 disjoint |
| true | true | true | false | shared overlap；DNB 保持 desired request 且 disjoint |
| false | true | false | false | 全部 disjoint；reserve 优先 |

每项断言 pool entry、standalone reclaim、overlap metadata、aggregate target 和
`DD=false` golden compatibility。

多 DNB pool 容量不足时，额外验证：

- `regulatePoolSizes` 输出不受 map 顺序影响。
- 各 pool 按同一比例/既有 regulator 规则压缩。
- 任一 active dedicated pool 变为 0 时整帧失败。
- pinned 与 unpinned domain 分别出现容量不足。
- fake-NUMA 与 real-NUMA 使用一致规则。

### 容量压力

| Capacity | Reserve | DNB requirement | SNB minimum | 期望 |
|---:|---:|---:|---:|---|
| 16 | 4 | 8 | 4 | 全满足 |
| 12 | 4 | 8 | 4 | SNB 压到 minimum |
| 10 | 4 | 8 | 4 | dedicated 压到 6 |
| 4 | 4 | 8 | 4 | dedicated 为空，返回错误 |
| 8 | 4 | 2 | 4 | reserve 4；dedicated 2；SNB 按 minimum 规则处理 |

### Transport

同步和异步接口分别验证：

```text
config
-> essentials
-> internal result
-> cpu server result
-> API response
-> QRM unified response
-> committed state
```

重点覆盖 `AS=true && DD=true`。

### CPUSet 稳定性

对 dedicated、SNB、share、isolation、reclaim 和 source pool：

- size 不变时零迁移。
- grow `d` 时只新增 `d`。
- shrink `d` 时只删除 `d`。
- 单个旧 CPU 失效时只替换该 CPU。
- BlockId 变化但 owner 不变时 CPUSet 不变。
- response map 随机插入顺序循环 1000 次时输出一致。
- 两个 block 具有相同 component key 时，owner 聚合 CPUSet 不变。
- BlockId 全量重生成、互换时 owner 聚合 CPUSet 不变。
- reclaim 与 pinned dedicated 竞争 eligibility 时存在合法解即可成功。
- 单向贪心会失败但联合匹配存在合法解时必须成功。
- 无合法解时不产生部分 plan。

### Resource Package

- pinned dedicated、SNB、share 只使用 package CPU。
- unpinned pool 不使用 exclusive pinned CPU。
- reclaim 不使用 non-reclaimable package。
- source pool 与 isolation carve 不越 package。
- aliases 同 package 时成功。
- aliases package 不兼容时失败。
- RP exclusive-DNB 使用 `partitionEligible` 做 coverage。
- pinned dedicated capacity 小于 `A-R` 时按 eligibility bound 失败。
- unpinned exclusive-DNB 遇到 non-reclaimable pinned CPU。
- package 跨 NUMA 或包含 forbidden/static CPU。
- `D intersect G` 为空、部分重叠和完全重叠。
- quantity 总量正确但集合匹配无合法解。

### Validator

- `DD=true` dedicated/reclaim 共享 BlockId 时拒绝。
- `AS=true` shared/reclaim 共享 BlockId 时允许。
- `AS=false` shared/reclaim 共享 BlockId 时拒绝。
- block 越 NUMA、越 package 或进入 non-reclaimable package 时拒绝。
- `DD=true` dedicated quantity 合法缩小时允许。
- `DD=false` quantity 与旧 allocation 不一致时拒绝。
- capability 缺失但 response 声明 `DD=true` 时拒绝。
- negotiated mode 与 stale response 不一致时拒绝。

### `applyBlocks`

- planner reclaim 不被 revise 扩大。
- `AS=true && DD=true` fallback 不引入 dedicated CPU。
- pure hook、revision、checkpoint 失败时 desired state 不变。
- bulkhead override 与 planner reclaim 一致。
- 清除 `RampUp` 后第二帧 partition 不变。
- agent restart 后 partition 不变。
- durable commit 成功但 cgroup apply 失败时 desired state 保留并触发重试。
- 重启发生在 durable commit 前后时都恢复到完整 generation。

### Admission

| Exclusive | DD | result 小于 request | 期望 |
|---:|---:|---:|---|
| false | 任意 | 是 | error |
| true | false | 是 | error |
| true | true | 是且非空 | success |
| true | true | 空 | error |

同时验证 disjoint、coverage、RP、Pod reclaim false 和查询失败。

### 兼容与状态切换

- new QRM 与 old Sysadvisor 在 `DD=true` 时拒绝 legacy advice。
- old QRM 请求不含 capability 时 new Sysadvisor 输出 legacy response。
- `DD=false -> true` 只接受 negotiated 新帧。
- `DD=true -> false` 恢复 legacy overlap。
- AS 独立切换不覆盖 DD。
- stale frame 不改变 desired state。
- `DD=false` 全链路 response、CPUSet、machine state、quota 和 validator golden
  与基线一致。

## 实施边界

实现按以下逻辑拆分：

1. 配置和 transport 字段传播。
2. Exclusive assembler disjoint 公式。
3. 普通 DNB workload-specific reclaim 公式。
4. overlap atom/alias clamp。
5. QRM descriptor 与稳定 block planner。
6. Resource Package 统一 eligibility。
7. validator 与 `applyBlocks` fail-closed。
8. `allocateNumaBindingCPUs` admission 语义。
9. 生命周期、稳定性和 E2E 验证。

每个步骤独立补充测试并以原子 commit 提交。
