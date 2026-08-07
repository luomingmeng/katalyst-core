# Dedicated 与 Reclaim 解耦分区实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 当 `DisableDedicatedCoresOverlapReclaimedCores=true` 时，实现 Sysadvisor 到 QRM 的 dedicated/reclaim 严格解耦分区，同时保证旧 CPUSet 稳定复用、Resource Package 约束和 fail-closed 一致性。

**Architecture:** Sysadvisor 是 block quantity、owner 和 overlap 关系的唯一 source of truth；QRM 将 response 规范化为稳定 descriptors，在 negotiated capability 下联合求解 dedicated/reclaim CPU ID，并将 planner 结果作为不可再扩展的 desired state 原子提交。`DisableDedicatedCoresOverlapReclaimedCores=false` 或 capability 未协商时继续使用 legacy 路径。

**Tech Stack:** Go、gRPC/gogo protobuf、Kubernetes CPUSet、Katalyst QRM state、CPU Advisor feature-gate negotiation、table-driven tests、Go race detector。

---

## 文件结构

### 新增文件

- `pkg/agent/utilcomponent/featuregatenegotiation/finders/feature_cpu/dedicated_reclaim_disjoint.go`
  - 定义 mutually-supported capability。
- `pkg/agent/utilcomponent/featuregatenegotiation/finders/feature_cpu/dedicated_reclaim_disjoint_test.go`
  - 验证动态配置到 capability 的映射。
- `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/dedicated_reclaim_partition.go`
  - 保存纯 quantity/eligibility 公式，避免 assembler 与 QRM 各自复制边界算法。
- `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/dedicated_reclaim_partition_test.go`
  - 覆盖 exclusive 和普通 DNB 公式。
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner.go`
  - block descriptor、owner component、stable preference 和确定性排序。
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner_test.go`
  - block normalization、BlockId 旋转和 map 顺序测试。
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go`
  - dedicated/reclaim eligibility 联合分配。
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver_test.go`
  - RP、可行解、无解和最小迁移测试。

### 修改文件

- `pkg/agent/utilcomponent/featuregatenegotiation/registry.go`
- `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor.go`
- `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go`
- `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common_test.go`
- `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server.go`
- `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/validator/validator_cpu_advisor.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/validator/validator_cpu_advisor_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

## Task 1：能力协商与字段传播

**Files:**
- Create: `pkg/agent/utilcomponent/featuregatenegotiation/finders/feature_cpu/dedicated_reclaim_disjoint.go`
- Create: `pkg/agent/utilcomponent/featuregatenegotiation/finders/feature_cpu/dedicated_reclaim_disjoint_test.go`
- Modify: `pkg/agent/utilcomponent/featuregatenegotiation/registry.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server.go`
- Test: `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`

- [ ] **Step 1：先写 capability finder 失败测试**

```go
func TestDedicatedReclaimDisjointPartition_GetFeatureGate(t *testing.T) {
	t.Parallel()
	finder := &DedicatedReclaimDisjointPartition{}

	disabled := config.NewConfiguration()
	require.Nil(t, finder.GetFeatureGate(disabled))

	enabled := config.NewConfiguration()
	enabled.GetDynamicConfiguration().DisableDedicatedCoresOverlapReclaimedCores = true
	got := finder.GetFeatureGate(enabled)
	require.Equal(t, NegotiationFeatureGateDedicatedReclaimDisjointPartition, got.Name)
	require.Equal(t, finders.FeatureGateTypeCPU, got.Type)
	require.True(t, got.MustMutuallySupported)
}
```

- [ ] **Step 2：运行测试确认 RED**

Run:

```bash
go test ./pkg/agent/utilcomponent/featuregatenegotiation/finders/feature_cpu \
  -run TestDedicatedReclaimDisjointPartition_GetFeatureGate -count=1
```

Expected: FAIL，提示 finder 或常量不存在。

- [ ] **Step 3：实现 capability 并注册**

```go
const NegotiationFeatureGateDedicatedReclaimDisjointPartition =
	"feature_gate_dedicated_reclaim_disjoint_partition"

type DedicatedReclaimDisjointPartition struct{}

func (f *DedicatedReclaimDisjointPartition) GetFeatureGate(
	conf *config.Configuration,
) *advisorsvc.FeatureGate {
	if conf == nil ||
		!conf.GetDynamicConfiguration().DisableDedicatedCoresOverlapReclaimedCores {
		return nil
	}
	return &advisorsvc.FeatureGate{
		Name:                  NegotiationFeatureGateDedicatedReclaimDisjointPartition,
		Type:                  finders.FeatureGateTypeCPU,
		MustMutuallySupported: true,
	}
}
```

在 `registry.go` 的 `init()` 中注册：

```go
RegisterNegotiationTypeFeatureGatesFinder(
	feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition,
	&feature_cpu.DedicatedReclaimDisjointPartition{},
)
```

- [ ] **Step 4：写同步 transport round-trip 测试**

测试必须断言：

```go
require.True(t, internal.DisableDedicatedCoresOverlapReclaimedCores)
require.True(t, getAdviceResp.DisableDedicatedCoresOverlapReclaimedCores)
require.Contains(t, getAdviceResp.SupportedFeatureGates,
	feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition)
require.True(t, unifiedResp.DisableDedicatedCoresOverlapReclaimedCores)
```

同时增加缺 capability 的失败用例：

```go
require.ErrorContains(t, err,
	"feature_gate_dedicated_reclaim_disjoint_partition")
```

- [ ] **Step 5：补齐字段传播**

在 `advisor.go` 的 `ResourceEssentials` 中写入：

```go
DisableDedicatedCoresOverlapReclaimedCores:
	cra.conf.GetDynamicConfiguration().DisableDedicatedCoresOverlapReclaimedCores,
```

在 `AssembleProvision()` 初始化结果时写入：

```go
DisableDedicatedCoresOverlapReclaimedCores:
	*pa.disableDedicatedCoresOverlapReclaimedCores,
```

在 `cpuInternalResult`、`assembleResponse()`、`GetAdviceResponse` 和
`ListAndWatchResponse` 中逐层复制该字段。同步 GetAdvice 转统一 response 时：

```go
DisableDedicatedCoresOverlapReclaimedCores:
	resp.DisableDedicatedCoresOverlapReclaimedCores,
```

异步 ListAndWatch 不支持 negotiation；当 response 中 `DD=true` 时直接拒绝：

```go
if resp.DisableDedicatedCoresOverlapReclaimedCores {
	return fmt.Errorf("dedicated reclaim disjoint partition requires negotiated GetAdvice")
}
```

- [ ] **Step 6：运行 transport 与 negotiation 测试**

```bash
go test ./pkg/agent/utilcomponent/featuregatenegotiation/... -count=1
go test ./pkg/agent/sysadvisor/plugin/qosaware/server \
  -run 'Test.*(Advice|FeatureGate|DisableDedicated)' -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test.*(Advice|FeatureGate|DisableDedicated)' -count=1
```

Expected: PASS。

- [ ] **Step 7：提交**

```bash
git add \
  pkg/agent/utilcomponent/featuregatenegotiation \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go \
  pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server.go \
  pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go
git commit -m "feat(dynamicpolicy): negotiate dedicated reclaim partition"
```

## Task 2：纯 eligibility 与 quantity 公式

**Files:**
- Create: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/dedicated_reclaim_partition.go`
- Create: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/dedicated_reclaim_partition_test.go`

- [ ] **Step 1：写 exclusive 公式失败测试**

```go
func TestCalculateExclusiveDisjointTargets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		in            exclusivePartitionInput
		wantDedicated int
		wantReclaim   int
		wantErr       bool
	}{
		{
			name: "reclaim enabled uses non reclaimed requirement",
			in: exclusivePartitionInput{
				PartitionCapacity: 16,
				DedicatedCapacity: 16,
				ReclaimCapacity:   16,
				Reserved:          4,
				NonReclaimed:      10,
				EnableReclaim:     true,
			},
			wantDedicated: 10,
			wantReclaim:   6,
		},
		{
			name: "reclaim disabled keeps reserve only",
			in: exclusivePartitionInput{
				PartitionCapacity: 16,
				DedicatedCapacity: 16,
				ReclaimCapacity:   16,
				Reserved:          4,
				EnableReclaim:     false,
			},
			wantDedicated: 12,
			wantReclaim:   4,
		},
		{
			name: "eligibility lower bound exceeds ratio cap",
			in: exclusivePartitionInput{
				PartitionCapacity: 16,
				DedicatedCapacity: 8,
				ReclaimCapacity:   16,
				Reserved:          4,
				NonReclaimed:      10,
				EnableReclaim:     true,
				RatioPhysicalCap:  4,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dedicated, reclaim, err := calculateExclusiveDisjointTargets(tt.in)
			require.Equal(t, tt.wantErr, err != nil)
			require.Equal(t, tt.wantDedicated, dedicated)
			require.Equal(t, tt.wantReclaim, reclaim)
		})
	}
}
```

- [ ] **Step 2：运行测试确认 RED**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler \
  -run TestCalculateExclusiveDisjointTargets -count=1
```

Expected: FAIL，提示类型或函数不存在。

- [ ] **Step 3：实现纯公式**

```go
type exclusivePartitionInput struct {
	PartitionCapacity int
	DedicatedCapacity int
	ReclaimCapacity   int
	Reserved          int
	NonReclaimed      int
	EnableReclaim     bool
	RatioPhysicalCap  int
}

func calculateExclusiveDisjointTargets(
	in exclusivePartitionInput,
) (dedicated, reclaim int, err error) {
	capacity := max(in.PartitionCapacity, 0)
	reserved := min(max(in.Reserved, 0), max(in.ReclaimCapacity, 0))
	nonReclaimed := min(max(in.NonReclaimed, 0), max(in.DedicatedCapacity, 0))
	candidate := reserved
	if in.EnableReclaim {
		candidate = max(reserved, capacity-nonReclaimed)
	}
	if in.RatioPhysicalCap > 0 {
		candidate = min(candidate, in.RatioPhysicalCap)
	}
	lower := max(reserved, capacity-in.DedicatedCapacity)
	upper := min(in.ReclaimCapacity, capacity-1)
	if candidate < lower || candidate > upper {
		return 0, 0, fmt.Errorf(
			"exclusive partition target %d outside reclaim bounds [%d,%d]",
			candidate, lower, upper)
	}
	return capacity - candidate, candidate, nil
}
```

- [ ] **Step 4：补 quota 和普通 DNB regulation 测试**

定义并测试：

```go
func calculateReclaimQuotaLimit(
	physicalTarget int,
	quotaKnob float64,
	ratioCap int,
) float64

func desiredDedicatedPhysical(
	rawRequest, rawRequirement int,
	enableReclaim, disableOverlap bool,
) int
```

断言：

```go
require.Equal(t, float64(-1), calculateReclaimQuotaLimit(6, -1, 0))
require.Equal(t, float64(0), calculateReclaimQuotaLimit(6, 0, 0))
require.Equal(t, float64(2), calculateReclaimQuotaLimit(6, 2, 0))
require.Equal(t, float64(6), calculateReclaimQuotaLimit(6, 8, 0))
require.Equal(t, 10, desiredDedicatedPhysical(16, 10, true, true))
require.Equal(t, 16, desiredDedicatedPhysical(16, 10, false, true))
require.Equal(t, 16, desiredDedicatedPhysical(16, 10, true, false))
```

- [ ] **Step 5：运行测试**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler \
  -run 'Test(CalculateExclusiveDisjointTargets|CalculateReclaimQuotaLimit|DesiredDedicatedPhysical)' \
  -count=1
```

Expected: PASS。

- [ ] **Step 6：提交**

```bash
git add pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/dedicated_reclaim_partition*
git commit -m "feat(sysadvisor): define dedicated reclaim partition formulas"
```

## Task 3：Exclusive-DNB assembler 输出独立 blocks

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go`
- Test: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common_test.go`

- [ ] **Step 1：写 `DD=true` assembler 失败测试**

```go
func TestAssembleDedicatedNUMAExclusiveRegionDisjoint(t *testing.T) {
	pa, region, result := newExclusiveAssemblerFixture(t, 16, 4, true, true)
	region.SetProvision(nonReclaimedProvision(10))

	require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(region, result))
	require.Equal(t, 10, result.PoolEntries["pod"][0].Size)
	require.Equal(t, 6, result.PoolEntries[commonstate.PoolNameReclaim][0].Size)
	require.Empty(t,
		result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0])
}
```

再增加：

```go
require.Equal(t, 12, unavailablePodResult.PoolEntries["pod"][0].Size)
require.Equal(t, 4,
	unavailablePodResult.PoolEntries[commonstate.PoolNameReclaim][0].Size)
```

- [ ] **Step 2：运行确认 RED**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler \
  -run TestAssembleDedicatedNUMAExclusiveRegionDisjoint -count=1
```

Expected: FAIL，当前实现仍生成 whole-NUMA dedicated overlap。

- [ ] **Step 3：接入纯公式并保留 legacy 分支**

在 `assembleDedicatedNUMAExclusiveRegion` 中显式分支：

```go
if !result.DisableDedicatedCoresOverlapReclaimedCores {
	return pa.assembleLegacyDedicatedNUMAExclusiveRegion(r, result)
}

dedicatedTarget, reclaimTarget, err := calculateExclusiveDisjointTargets(input)
if err != nil {
	return err
}
for podUID := range r.GetPods() {
	result.SetPoolEntry(podUID, regionNuma, dedicatedTarget, -1)
}
result.SetPoolEntry(
	commonstate.PoolNameReclaim,
	regionNuma,
	reclaimTarget,
	reclaimQuotaLimit,
)
```

legacy helper 保持原代码逐行语义，不在本任务中重构。

- [ ] **Step 4：补 RP eligibility 和 quota 测试**

覆盖：

```text
pinned dedicated capacity < target -> error
non-reclaimable pinned CPUs excluded from G
quota 0 keeps physical reclaim
ratio cap below eligibility lower bound -> error
DD=false golden unchanged
```

- [ ] **Step 5：运行 assembler 包**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler -count=1
```

Expected: PASS。

- [ ] **Step 6：提交**

```bash
git add \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common_test.go
git commit -m "feat(sysadvisor): emit disjoint exclusive dedicated blocks"
```

## Task 4：普通 DNB mixed overlap 与 alias atom

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go`
- Test: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common_test.go`

- [ ] **Step 1：写 `AS=true && DD=true` 失败测试**

```go
func TestAssembleWithoutNUMAExclusivePoolMixedOverlap(t *testing.T) {
	result := runAssemblerCase(t, assemblerCase{
		Capacity:               16,
		Reserve:                4,
		AllowSharedOverlap:     true,
		DisableDedicatedOverlap: true,
		Shared: poolInput{Request: 8, Requirement: 4, EnableReclaim: true},
		Dedicated: poolInput{Request: 8, Requirement: 6, EnableReclaim: true},
	})

	require.Equal(t, 6, result.PoolEntries["dedicated-pod"][0].Size)
	require.Contains(t,
		result.PoolOverlapInfo[commonstate.PoolNameReclaim][0], "share")
	require.Empty(t,
		result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0])
}
```

- [ ] **Step 2：写 alias atom 失败测试**

```go
func TestClampReclaimOverlapMetadataKeepsContainerAliases(t *testing.T) {
	result := resultWithSharedOverlapAliases(4, "pod", "main", "sidecar")
	actual := clampReclaimOverlapMetadata(result, 0, 4)
	require.Equal(t, 4, actual)
	require.Equal(t, 4,
		result.PoolOverlapPodContainerInfo["reclaim"][0]["pod"]["main"])
	require.Equal(t, 4,
		result.PoolOverlapPodContainerInfo["reclaim"][0]["pod"]["sidecar"])
}
```

- [ ] **Step 3：运行确认 RED**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler \
  -run 'Test(AssembleWithoutNUMAExclusivePoolMixedOverlap|ClampReclaimOverlapMetadataKeepsContainerAliases)' \
  -count=1
```

Expected: FAIL；当前 dedicated 仍贡献 overlap，alias 被重复扣 budget。

- [ ] **Step 4：拆分 shared/dedicated overlap 计算**

将 `calculateReclaimPool` 输入扩展为：

```go
type reclaimPoolCalculationPolicy struct {
	allowSharedOverlap     bool
	allowDedicatedOverlap  bool
}
```

调用时：

```go
policy := reclaimPoolCalculationPolicy{
	allowSharedOverlap: *pa.allowSharedCoresOverlapReclaimedCores,
	allowDedicatedOverlap:
		!result.DisableDedicatedCoresOverlapReclaimedCores,
}
```

dedicated physical size 使用 `desiredDedicatedPhysical`，容量不足继续通过
`regulatePoolSizes` 调节；任一 active dedicated pool 变成 0 时返回小写错误。

- [ ] **Step 5：把 aliases 绑定到 overlap atom**

```go
type overlapAtom struct {
	key     string
	size    int
	aliases []podContainerAlias
}
```

clamp 对 `atom.size` 只扣一次，再把相同结果写回全部 aliases。

- [ ] **Step 6：运行完整 assembler 测试**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler -count=1
```

Expected: PASS，包括全部 `DD=false` golden。

- [ ] **Step 7：提交**

```bash
git add \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common_test.go
git commit -m "feat(sysadvisor): separate shared and dedicated reclaim policy"
```

## Task 5：规范化 block descriptors 与 RP eligibility

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner_test.go`

- [ ] **Step 1：写 normalization 失败测试**

```go
func TestBuildBlockDescriptorsDeterministic(t *testing.T) {
	respA := responseWithRandomizedMaps(1)
	respB := responseWithRandomizedMaps(2)
	descriptorsA, err := buildBlockDescriptors(respA, fixtureSnapshot())
	require.NoError(t, err)
	descriptorsB, err := buildBlockDescriptors(respB, fixtureSnapshot())
	require.NoError(t, err)
	require.Equal(t, descriptorsA, descriptorsB)
}
```

写 RP alias 冲突测试：

```go
_, err := buildBlockDescriptors(
	responseWithAliasedPackages("pkg-a", "pkg-b"),
	fixtureSnapshot(),
)
require.ErrorContains(t, err, "incompatible resource package")
```

- [ ] **Step 2：运行确认 RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestBuildBlockDescriptors(Deterministic|RejectsIncompatibleResourcePackages)' \
  -count=1
```

Expected: FAIL，planner 文件尚不存在。

- [ ] **Step 3：定义 descriptor**

```go
type advisorBlockClass int

const (
	advisorBlockStatic advisorBlockClass = iota
	advisorBlockMandatoryReclaim
	advisorBlockDedicated
	advisorBlockShared
	advisorBlockReclaimOverlap
)

type advisorBlockOwner struct {
	poolName       string
	entryName      string
	subEntryName   string
	resourcePackage string
}

type advisorBlockDescriptor struct {
	blockID       string
	numaID        int
	class         advisorBlockClass
	quantity      int
	owners        []advisorBlockOwner
	componentKey  string
	eligible      machine.CPUSet
	oldPreferred  machine.CPUSet
}
```

- [ ] **Step 4：实现 owner eligibility**

```go
func ownerEligibleCPUSet(
	scope machine.CPUSet,
	owner advisorBlockOwner,
	rpPinned map[string]machine.CPUSet,
	allPinned machine.CPUSet,
	nonReclaimablePinned machine.CPUSet,
	reclaim bool,
) machine.CPUSet {
	if reclaim {
		return scope.Difference(nonReclaimablePinned)
	}
	if owner.resourcePackage != "" &&
		!rpPinned[owner.resourcePackage].IsEmpty() {
		return scope.Intersection(rpPinned[owner.resourcePackage])
	}
	return scope.Difference(allPinned)
}
```

alias block 的 `eligible` 是所有 owner eligible 的交集；交集容量不足立即报错。

- [ ] **Step 5：实现稳定排序与 component key**

owners 按 `poolName/entryName/subEntryName/resourcePackage` 排序。descriptor 按：

```text
numaID
class
componentKey
quantity
canonical alias signature
BlockId
```

排序前禁止使用 map 中“第一个 owner”决定 package 或 class。

- [ ] **Step 6：运行 planner normalization 测试**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestBuildBlockDescriptors' -count=1
```

Expected: PASS。

- [ ] **Step 7：提交**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner*
git commit -m "feat(dynamicpolicy): normalize advisor block constraints"
```

## Task 6：Dedicated/Reclaim 联合 partition solver

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver_test.go`

- [ ] **Step 1：写贪心失败但存在合法解的测试**

```go
func TestSolveDisjointPartitionsFindsConstrainedAssignment(t *testing.T) {
	input := []partitionDemand{
		{
			key:       "reclaim",
			quantity:  2,
			eligible:  machine.NewCPUSet(0, 1, 2, 3),
			preferred: machine.NewCPUSet(0, 1),
		},
		{
			key:       "dedicated-rp",
			quantity:  2,
			eligible:  machine.NewCPUSet(0, 1),
			preferred: machine.NewCPUSet(0, 1),
		},
	}
	got, err := solveDisjointPartitions(input, fixtureTopology())
	require.NoError(t, err)
	require.True(t, got["dedicated-rp"].Equals(machine.NewCPUSet(0, 1)))
	require.True(t, got["reclaim"].Equals(machine.NewCPUSet(2, 3)))
}
```

- [ ] **Step 2：写最小迁移测试**

```go
func TestSolveDisjointPartitionsKeepsLegalOldCPUs(t *testing.T) {
	input := []partitionDemand{
		{
			key:       "dedicated",
			quantity:  3,
			eligible:  machine.NewCPUSet(0, 1, 2, 3),
			preferred: machine.NewCPUSet(0, 1),
		},
		{
			key:       "reclaim",
			quantity:  1,
			eligible:  machine.NewCPUSet(2, 3),
			preferred: machine.NewCPUSet(3),
		},
	}
	got, err := solveDisjointPartitions(input, fixtureTopology())
	require.NoError(t, err)
	require.True(t, machine.NewCPUSet(0, 1).IsSubsetOf(got["dedicated"]))
	require.True(t, got["reclaim"].Equals(machine.NewCPUSet(3)))
}
```

- [ ] **Step 3：运行确认 RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestSolveDisjointPartitions' -count=1
```

Expected: FAIL，solver 不存在。

- [ ] **Step 4：实现确定性容量匹配**

```go
type partitionDemand struct {
	key       string
	quantity  int
	eligible  machine.CPUSet
	preferred machine.CPUSet
}

func solveDisjointPartitions(
	demands []partitionDemand,
	topology *machine.CPUTopology,
) (map[string]machine.CPUSet, error)
```

实现使用 CPU-to-demand 容量图：

- source 到每个 CPU 容量 1。
- CPU 到 eligible demand 容量 1。
- demand 到 sink 容量 `quantity`。
- edge cost 依次表达 old preferred、`G-D` reclaim preference 和 topology cost。
- 按 demand key、CPU ID 稳定排序。
- 使用 successive shortest augmenting path；流量不足时返回
  `partition demands have no feasible assignment`。

- [ ] **Step 5：补无解、BlockId 旋转和 RP 测试**

断言：

```go
require.ErrorContains(t, err, "no feasible assignment")
require.Equal(t, oldOwnerUnion, newOwnerUnion)
require.Empty(t, dedicated.Intersection(reclaim))
```

- [ ] **Step 6：运行 solver 与 race 测试**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestSolveDisjointPartitions' -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestSolveDisjointPartitions' -count=1
```

Expected: PASS。

- [ ] **Step 7：提交**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver*
git commit -m "feat(dynamicpolicy): solve dedicated reclaim partitions jointly"
```

## Task 7：接入 `generateBlockCPUSet`

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_reclaim_reuse_test.go`

- [ ] **Step 1：写同输入零迁移失败测试**

```go
func TestGenerateBlockCPUSetDisjointPlannerReusesEveryPool(t *testing.T) {
	p := newPlannerPolicyFixture(t)
	resp := disjointResponseWithAllPoolClasses()
	first, err := p.generateBlockCPUSet(resp)
	require.NoError(t, err)
	commitBlockCPUSetToFixtureState(t, p, resp, first)
	second, err := p.generateBlockCPUSet(resp)
	require.NoError(t, err)
	require.Equal(t, first, second)
}
```

- [ ] **Step 2：写 grow/shrink/invalid-old-CPU 测试**

每类 pool 断言：

```go
changed := old.SymmetricDifference(new).Size()
require.Equal(t, delta, changed)
require.True(t, old.Difference(invalid).IsSubsetOf(new))
```

- [ ] **Step 3：运行确认 RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestGenerateBlockCPUSetDisjointPlanner' -count=1
```

Expected: FAIL，real-NUMA share/source pool 发生跳核或新 planner 未接入。

- [ ] **Step 4：添加 negotiated planner 分支**

```go
func (p *DynamicPolicy) generateBlockCPUSet(
	resp *advisorapi.ListAndWatchResponse,
	featureGates map[string]*advisorsvc.FeatureGate,
) (advisorapi.BlockCPUSet, error) {
	if resp.DisableDedicatedCoresOverlapReclaimedCores {
		if !hasFeatureGate(featureGates,
			feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition) {
			return nil, fmt.Errorf(
				"dedicated reclaim disjoint partition capability is not negotiated")
		}
		return p.planDisjointAdvisorBlocks(resp)
	}
	return p.generateLegacyBlockCPUSet(resp)
}
```

保留 legacy 实现，避免 `DD=false` 行为变化。

- [ ] **Step 5：统一所有 dynamic pool preference**

`planDisjointAdvisorBlocks`：

```text
normalize descriptors
-> allocate static/system/forbidden
-> solve dedicated + mandatory reclaim
-> solve source/isolation components
-> allocate remaining shared
-> allocate overlap reclaim aliases
-> validate every descriptor quantity
```

source/isolation component 同时把旧 source 和旧 isolation CPUSet 作为
preference；删除“先重新选 source candidate 再 carve”的新路径。

- [ ] **Step 6：运行稳定性和 RP 测试**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestGenerateBlockCPUSet|TestAdvisorSourcePool|Test.*ResourcePackage' \
  -count=1
```

Expected: PASS。

- [ ] **Step 7：提交**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_reclaim_reuse_test.go
git commit -m "feat(dynamicpolicy): apply stable advisor block planner"
```

## Task 8：Validator 与 `applyBlocks` fail-closed

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/validator/validator_cpu_advisor.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/validator/validator_cpu_advisor_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

- [ ] **Step 1：写结构 validator 失败测试**

```go
func TestValidateRejectsDedicatedReclaimAliasWhenDisabled(t *testing.T) {
	resp := responseWithAliasedDedicatedAndReclaimBlock()
	resp.DisableDedicatedCoresOverlapReclaimedCores = true
	err := validator.Validate(resp)
	require.ErrorContains(t, err,
		"dedicated and reclaim share block")
}
```

同时覆盖：

```go
require.NoError(t, validate(sharedReclaimAliasResp(true, true)))
require.Error(t, validate(sharedReclaimAliasResp(false, true)))
```

- [ ] **Step 2：运行确认 RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/validator \
  -run 'TestValidate.*(Dedicated|Shared).*Reclaim' -count=1
```

Expected: FAIL，当前 validator 不检查 owner class alias。

- [ ] **Step 3：实现 response 结构校验**

在 `Validate()` pipeline 增加：

```go
c.validateOverlapPolicy,
c.validateResourcePackageOwners,
```

核心判断：

```go
if resp.DisableDedicatedCoresOverlapReclaimedCores &&
	isDedicatedBlock(block) && hasReclaimOwner(block) {
	return fmt.Errorf(
		"dedicated and reclaim share block %s while overlap is disabled",
		block.BlockId)
}
```

`DD=true` 时允许 dedicated quantity 与旧 allocation 不同，但要求非零并由
planner/output validator 完成 eligibility 校验。

- [ ] **Step 4：写 `applyBlocks` 不扩展 reclaim 测试**

```go
func TestApplyBlocksDoesNotReviseDisjointReclaim(t *testing.T) {
	p := newApplyBlocksFixture(t)
	resp, blocks := disjointApplyFixture()
	require.NoError(t, p.applyBlocks(blocks, resp, true))
	reclaim := p.state.GetAllocationInfo("reclaim", "")
	require.True(t, reclaim.AllocationResult.Equals(blocks["reclaim-only"]))
	require.Empty(t,
		reclaim.AllocationResult.Intersection(dedicatedCPUSet(p.state)))
}
```

- [ ] **Step 5：修改 `applyBlocks`**

```go
if resp.DisableDedicatedCoresOverlapReclaimedCores {
	if newEntries.CheckPoolEmpty(commonstate.PoolNameReclaim) {
		return fmt.Errorf("disjoint advisor response has no reclaim partition")
	}
} else {
	if err := p.reviseReclaimPool(...); err != nil {
		return err
	}
}
```

`buildAdjustmentCommitOverrideFromPodEntries` 增加 `disableDedicated` 参数；当
`AS=true && DD=true` 时也构造只保护 dedicated/reclaim disjoint 的 override。

- [ ] **Step 6：审计 allocation hooks**

逐个读取 `allocationHooks` 注册点。只允许修改 target annotations 的纯函数。
若发现外部 I/O，将其移动到 commit 后的 adjustment reconcile，并增加幂等
重试测试。

- [ ] **Step 7：运行 validator/apply/race**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/validator -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test(ApplyBlocks|DynamicPolicyApplyBlocks|CPUSetAdjustment)' -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test(ApplyBlocks|DynamicPolicyApplyBlocks|CPUSetAdjustment)' -count=1
```

Expected: PASS。

- [ ] **Step 8：提交**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/validator \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go
git commit -m "fix(dynamicpolicy): enforce advisor partition before commit"
```

## Task 9：修正 `allocateNumaBindingCPUs`

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_resource_package_test.go`

- [ ] **Step 1：写 exclusive 小结果失败测试**

```go
func TestAllocateNumaBindingCPUsAllowsSmallerExclusiveDisjointResult(t *testing.T) {
	p, machineState := newExclusiveAllocationFixture(t, 16, 4)
	result, reclaim, err := p.allocateNumaBindingCPUs(
		16,
		&pluginapi.TopologyHint{Nodes: []uint64{0}},
		machineState,
		exclusiveAnnotations(),
		false,
	)
	require.NoError(t, err)
	require.Equal(t, 12, result.Size())
	require.Equal(t, 4, reclaim.Size())
	require.Empty(t, result.Intersection(reclaim))
}
```

- [ ] **Step 2：写兼容和 RP coverage 测试**

```go
require.Error(t, allocateExclusiveSmallerResultWithDDDisabled())
require.NoError(t, allocateExclusivePinnedPartitionAgainstEligibleUnion())
require.Error(t, allocateExclusiveEmptyDedicatedResult())
```

- [ ] **Step 3：运行确认 RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestAllocateNumaBindingCPUs.*(Exclusive|ResourcePackage)' -count=1
```

Expected: FAIL，当前 `leftNumCPUs > 0` 返回 error。

- [ ] **Step 4：重写结果校验**

```go
disableDedicatedOverlap :=
	p.conf.GetDynamicConfiguration().
		DisableDedicatedCoresOverlapReclaimedCores

if numaExclusive && disableDedicatedOverlap {
	if result.IsEmpty() {
		return machine.NewCPUSet(), machine.NewCPUSet(),
			fmt.Errorf("exclusive disjoint dedicated result is empty")
	}
	partitionEligible := dedicatedEligible.Union(reclaimEligible)
	if !result.Intersection(hardReclaimCPUs).IsEmpty() {
		return machine.NewCPUSet(), machine.NewCPUSet(),
			fmt.Errorf("exclusive dedicated result overlaps reclaim partition")
	}
	if !result.Union(hardReclaimCPUs).Equals(partitionEligible) {
		return machine.NewCPUSet(), machine.NewCPUSet(),
			fmt.Errorf("exclusive dedicated and reclaim do not cover eligible partition")
	}
} else if result.Size() < numCPUs {
	return machine.NewCPUSet(), machine.NewCPUSet(),
		fmt.Errorf("results can't meet cpu request")
}
```

- [ ] **Step 5：让 reserve 选择遵守 RP**

`dedicatedEligible` 与 `reclaimEligible` 使用 Task 5 相同 helper。reserve 优先
从 `reclaimEligible.Difference(dedicatedEligible)` 选择，再进入交集。

- [ ] **Step 6：运行 allocation 与 RP 测试**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test(AllocateNumaBindingCPUs|.*ResourcePackage|.*DNB.*RampUp)' \
  -count=1
```

Expected: PASS。

- [ ] **Step 7：提交**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_resource_package_test.go
git commit -m "fix(dynamicpolicy): admit exclusive disjoint numa results"
```

## Task 10：生命周期、稳定性与兼容回归

**Files:**
- Test: `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_cpuset_adjustment_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_resource_package_test.go`
- Modify: `docs/proposals/qos-management/qos-resource-manager/20260729-bulkhead-cpuset-phase-plan.md`

- [ ] **Step 1：增加两帧 lifecycle 测试**

```go
func TestExclusiveDisjointPartitionSurvivesRampUpCompletion(t *testing.T) {
	p, advisor := newEndToEndAdvisorFixture(t)
	first := advisor.NextDisjointResponse()
	require.NoError(t, p.allocateByCPUAdvisor(nil, first, negotiatedFeatureGates()))
	require.False(t, p.state.GetAllocationInfo("pod", "main").RampUp)

	second := advisor.NextDisjointResponse()
	require.NoError(t, p.allocateByCPUAdvisor(nil, second, negotiatedFeatureGates()))
	assertDisjointPartition(t, p.state.GetPodEntries())
	assertSamePartitionCPUSet(t, first, second, p.state.GetPodEntries())
}
```

- [ ] **Step 2：增加 map/BlockId 稳定性循环**

```go
baseline := advisorapi.BlockCPUSet(nil)
for seed := int64(0); seed < 1000; seed++ {
	resp := randomizedEquivalentResponse(seed)
	got, err := p.generateBlockCPUSet(resp, negotiatedFeatureGates())
	require.NoError(t, err)
	if seed == 0 {
		baseline = got
		continue
	}
	require.Equal(t, baselineOwnerUnions, ownerUnions(resp, got))
}
```

- [ ] **Step 3：增加 flag transition 测试**

覆盖：

```text
DD false -> true：只接受 negotiated response
DD true -> false：恢复 legacy overlap
AS 切换不覆盖 DD
stale revision 不提交
```

- [ ] **Step 4：增加 restart/checkpoint 测试**

持久化 disjoint state，重建 policy，再断言：

```go
require.Empty(t,
	dedicatedCPUSet(restored).Intersection(reclaimCPUSet(restored)))
```

- [ ] **Step 5：更新 phase 文档**

记录：

- capability 名称和升级顺序。
- desired state 与 cgroup applied state 边界。
- rollback/fail-closed 行为。
- `DD=false` legacy fallback。

- [ ] **Step 6：运行完整验证**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/... -count=1
go test ./pkg/agent/sysadvisor/plugin/qosaware/server -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test.*(Disjoint|Partition|BlockCPUSet|ApplyBlocks|NumaBinding)' \
  -count=1
git diff --check
```

Expected: 全部 PASS，`git diff --check` 无输出。

- [ ] **Step 7：提交**

```bash
git add \
  pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  docs/proposals/qos-management/qos-resource-manager/20260729-bulkhead-cpuset-phase-plan.md
git commit -m "test(dynamicpolicy): cover dedicated reclaim partition lifecycle"
```

## Task 11：最终审计与交付

**Files:**
- Review: all files changed by Tasks 1-10
- Reference: `docs/superpowers/specs/2026-08-08-dedicated-reclaim-disjoint-partition-design.md`

- [ ] **Step 1：逐项核对设计不变量**

确认以下条件均有对应测试：

```text
DD=true  => dedicated intersect reclaim = empty
AS=false => shared intersect reclaim = empty
exclusive DD=true => dedicated union reclaim = partitionEligible
size unchanged => owner aggregate CPUSet unchanged
RP owner => result subset-of package eligible
checkpoint failure => desired state unchanged
cgroup failure => desired state retained and reconcile retried
```

- [ ] **Step 2：执行完整测试与 race**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/... -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
go test -race ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/... -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test.*(Disjoint|Partition|BlockCPUSet|ApplyBlocks|NumaBinding)' \
  -count=1
```

Expected: 全部 PASS，无 race。

- [ ] **Step 3：检查提交边界**

```bash
git status --short
git log --oneline --reverse adcdb1c3a..HEAD
git diff --check adcdb1c3a..HEAD
```

Expected:

- 只剩本地开发用 `go.mod replace`，不进入 commit。
- 每个 commit 对应一个任务。
- diff check 无输出。

- [ ] **Step 4：独立代码审查**

审查重点：

- formula 与 eligibility bounds 是否一致。
- `DD=false` 是否走 legacy。
- alias/RP 是否 fail-closed。
- planner 是否完全确定。
- `applyBlocks` 是否还会 revise disjoint reclaim。
- error message 是否全小写。

- [ ] **Step 5：修复所有 P1 后复跑验证**

任何 P1 阻塞提交和 PR。修复后重新执行 Task 11 Step 2 与 Step 3。

- [ ] **Step 6：准备交付摘要**

摘要包含：

- 原子 commit 列表。
- 完整测试/race 证据。
- capability 升级顺序。
- 本地 `go.mod replace` 未提交说明。
- 尚未覆盖的真实节点 E2E 风险。
