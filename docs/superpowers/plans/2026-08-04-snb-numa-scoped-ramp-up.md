# SNB NUMA-scoped Ramp-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit SNB ramp-up semantics while keeping SNB ramp-up constrained to its hinted NUMA, without changing NUMA-exclusive DNB, NUMA-non-exclusive DNB, or non-SNB behavior.

**Architecture:** Introduce a dedicated SNB ramp-up path instead of reusing the existing node-wide shared ramp-up path. SNB may carry `AllocationInfo.RampUp=true`, but `adjustPoolsAndIsolatedEntriesWithRampUpFloor` must detect `CheckSharedNUMABinding()` and assign only NUMA-local ramp-up CPUs for the hinted NUMA. Node-wide `rampUpCPUs` remains reserved for ordinary non-SNB shared ramp-up.

**Tech Stack:** Go, Katalyst QRM CPU dynamic policy, existing `state.AllocationInfo`, cpuset `machine.CPUSet`, existing policy unit tests and QRM bulkhead E2E scripts.

---

## Code path summary

Current allocation paths:

- non-SNB: `sharedCoresAllocationHandler` calls `shouldSharedCoresRampUp`, may set `AllocationInfo.RampUp=true`, and later `adjustPoolsAndIsolatedEntriesWithRampUpFloor` maps it to node-wide `rampUpCPUs`.
- SNB: `sharedCoresWithNUMABindingAllocationHandler` calls `allocateSharedNumaBindingCPUs`, which sets NUMA-binding annotations and hint but does not set `RampUp`.
- NUMA-exclusive DNB and NUMA-non-exclusive DNB: `dedicatedCoresWithNUMABindingAllocationHandler` sets `RampUp=true` on the dedicated NUMA-binding allocation path and uses the existing atomic ramp-up floor flow.

Risk if implemented naively:

- If `allocateSharedNumaBindingCPUs` simply sets `RampUp=true`, existing code at `adjustPoolsAndIsolatedEntriesWithRampUpFloor` will treat it like ordinary shared ramp-up and assign node-wide `rampUpCPUs`.
- That violates the required SNB semantics: “SNB 的 ramp_up 只在其所在 numa 进行”.

Required invariant:

- `AllocationInfo.RampUp=true && CheckSharedNUMABinding()` means SNB NUMA-scoped ramp-up.
- `AllocationInfo.RampUp=true && CheckShared() && !CheckSharedNUMABinding()` means ordinary node-wide shared ramp-up.
- `AllocationInfo.RampUp=true && CheckDedicatedNUMABinding()` keeps the current DNB semantics.

## Files

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- Optional update after verification: `qrm-bulkhead-test-artifacts/fourcase-rampup-e2e-report.md`

## Task 1: Mark SNB cold-start allocation as RampUp

- [ ] **Step 1: Add failing test for SNB allocation flag**

Add a new test near `TestAllocateSharedNumaBindingCPUs` in `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`:

```go
func TestAllocateSharedNumaBindingCPUsMarksColdStartRampUp(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)

	policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	policy.dynamicConfig.GetDynamicConfiguration().DisableSharedCoresRampUp = false

	req := &pluginapi.ResourceRequest{
		PodUid:         "snb-ramp-up",
		PodNamespace:   "default",
		PodName:        "snb-ramp-up",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                  apiconsts.PodAnnotationQoSLevelSharedCores,
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
	}

	allocation, err := policy.allocateSharedNumaBindingCPUs(req, req.Hint, false)
	require.NoError(t, err)
	require.NotNil(t, allocation)
	require.True(t, allocation.CheckSharedNUMABinding())
	require.True(t, allocation.RampUp)
	require.Equal(t, "0", allocation.Annotations[cpuconsts.CPUStateAnnotationKeyNUMAHint])
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
go test -run TestAllocateSharedNumaBindingCPUsMarksColdStartRampUp -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy
```

Expected: FAIL because SNB allocation currently returns `RampUp=false`.

- [ ] **Step 3: Implement minimal flagging logic**

In `allocateSharedNumaBindingCPUs`, after:

```go
allocationInfo.SetSpecifiedNUMABindingNUMAID(hint.Nodes)
```

add:

```go
allocationInfo.RampUp = p.shouldSharedCoresRampUp(req.PodUid)
```

Do not call `deriveRampUpReclaimFloor` here yet. The immediate goal is only to mark the allocation; Task 2 will keep adjustment semantics safe.

- [ ] **Step 4: Run focused test**

Run:

```bash
go test -run TestAllocateSharedNumaBindingCPUsMarksColdStartRampUp -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy
```

Expected: PASS.

## Task 2: Add NUMA-scoped SNB ramp-up assignment

- [ ] **Step 1: Add failing test for NUMA-scoped SNB ramp-up**

Add a test in `policy_allocation_handlers_test.go`:

```go
func TestSharedNUMABindingRampUpStaysWithinHintedNUMA(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25
	p.dynamicConfig.GetDynamicConfiguration().DisableSharedCoresRampUp = false
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false)

	allocation := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "snb-ramp-up",
			PodNamespace:  "default",
			PodName:       "snb-ramp-up",
			ContainerName: "main",
			ContainerType: pluginapi.ContainerType_MAIN.String(),
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			},
		},
		RampUp:          true,
		RequestQuantity: 2,
		InitTimestamp:   time.Now().Format(util.QRMTimeFormat),
	}
	allocation.SetSpecifiedNUMABindingNUMAID([]uint64{0})
	p.state.SetAllocationInfo(allocation.PodUid, allocation.ContainerName, allocation, false)

	err = p.adjustAllocationEntriesWithRampUpFloor(
		p.state.GetPodEntries(),
		p.state.GetMachineState(),
		false,
		machine.NewCPUSet(),
		false,
	)
	require.NoError(t, err)

	updated := p.state.GetAllocationInfo(allocation.PodUid, allocation.ContainerName)
	require.NotNil(t, updated)
	require.True(t, updated.RampUp)
	require.True(t, updated.CheckSharedNUMABinding())

	numa0 := p.machineInfo.CPUDetails.CPUsInNUMANodes(0)
	numa1 := p.machineInfo.CPUDetails.CPUsInNUMANodes(1)
	require.False(t, updated.AllocationResult.IsEmpty())
	require.True(t, updated.AllocationResult.IsSubsetOf(numa0),
		"SNB ramp-up allocation=%s must stay within hinted NUMA0=%s", updated.AllocationResult, numa0)
	require.True(t, updated.AllocationResult.Intersection(numa1).IsEmpty(),
		"SNB ramp-up allocation=%s must not use NUMA1=%s", updated.AllocationResult, numa1)
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
go test -run TestSharedNUMABindingRampUpStaysWithinHintedNUMA -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy
```

Expected: FAIL because existing `allocationInfo.RampUp` branch uses node-wide `rampUpCPUs`.

- [ ] **Step 3: Add helper to compute SNB ramp-up CPUs**

In `policy_allocation_handlers.go`, add helper near `deriveRampUpReclaimFloor`:

```go
func (p *DynamicPolicy) getSharedNUMABindingRampUpCPUSet(
	allocationInfo *state.AllocationInfo,
	machineState state.NUMANodeMap,
	unionDedicatedIsolatedCPUSet machine.CPUSet,
	notAllocatablePoolsCPUs machine.CPUSet,
	rampUpReclaimFloor machine.CPUSet,
) (machine.CPUSet, map[int]machine.CPUSet, error) {
	if allocationInfo == nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("nil allocationInfo")
	}
	numaHintStr := allocationInfo.Annotations[cpuconsts.CPUStateAnnotationKeyNUMAHint]
	numaSet, err := machine.Parse(numaHintStr)
	if err != nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("parse SNB numa hint %q failed: %w", numaHintStr, err)
	}
	if numaSet.Size() != 1 {
		return machine.NewCPUSet(), nil, fmt.Errorf("SNB ramp-up requires exactly one NUMA hint, got %s", numaSet.String())
	}
	numaID := numaSet.ToSliceNoSortInt()[0]
	numaState := machineState[numaID]
	if numaState == nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("SNB ramp-up missing machine state for NUMA %d", numaID)
	}
	snbRampUpCPUs := numaState.GetAvailableCPUSet(p.reservedCPUs).
		Difference(unionDedicatedIsolatedCPUSet).
		Difference(notAllocatablePoolsCPUs).
		Difference(rampUpReclaimFloor)
	if snbRampUpCPUs.IsEmpty() {
		return machine.NewCPUSet(), nil, fmt.Errorf("SNB ramp-up CPUs are empty for NUMA %d", numaID)
	}
	assignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, snbRampUpCPUs)
	if err != nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("calculate SNB ramp-up assignments for NUMA %d CPUs %s failed: %w",
			numaID, snbRampUpCPUs.String(), err)
	}
	if len(assignments) != 1 {
		return machine.NewCPUSet(), nil, fmt.Errorf("SNB ramp-up assignments crossed NUMA: %+v", assignments)
	}
	return snbRampUpCPUs, assignments, nil
}
```

The helper intentionally does not subtract `sharedBindingNUMACPUs` because the current SNB is itself a shared binding workload on that NUMA. It still subtracts dedicated isolated CPUs, not-allocatable pools, and the hard reclaim floor.

- [ ] **Step 4: Route RampUp SNB before node-wide shared ramp-up**

In `adjustPoolsAndIsolatedEntriesWithRampUpFloor`, replace:

```go
if allocationInfo.RampUp {
	general.Infof("pod: %s/%s container: %s is in ramp up, set its allocation result from %s to rampUpCPUs :%s",
		allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName,
		allocationInfo.AllocationResult.String(), rampUpCPUs.String())

	newPodEntries[podUID][containerName].OwnerPoolName = commonstate.EmptyOwnerPoolName
	newPodEntries[podUID][containerName].AllocationResult = rampUpCPUs.Clone()
	newPodEntries[podUID][containerName].OriginalAllocationResult = rampUpCPUs.Clone()
	newPodEntries[podUID][containerName].TopologyAwareAssignments = machine.DeepcopyCPUAssignment(rampUpCPUsTopologyAwareAssignments)
	newPodEntries[podUID][containerName].OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(rampUpCPUsTopologyAwareAssignments)
} else {
```

with:

```go
if allocationInfo.RampUp {
	if allocationInfo.CheckSharedNUMABinding() {
		snbRampUpCPUs, snbAssignments, err := p.getSharedNUMABindingRampUpCPUSet(
			allocationInfo, machineState, unionDedicatedIsolatedCPUSet, notAllocatablePoolsCPUs, rampUpReclaimFloor)
		if err != nil {
			return err
		}
		general.Infof("pod: %s/%s container: %s is SNB ramp up, set its allocation result from %s to NUMA-scoped rampUpCPUs: %s",
			allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName,
			allocationInfo.AllocationResult.String(), snbRampUpCPUs.String())

		newPodEntries[podUID][containerName].OwnerPoolName = commonstate.EmptyOwnerPoolName
		newPodEntries[podUID][containerName].AllocationResult = snbRampUpCPUs.Clone()
		newPodEntries[podUID][containerName].OriginalAllocationResult = snbRampUpCPUs.Clone()
		newPodEntries[podUID][containerName].TopologyAwareAssignments = machine.DeepcopyCPUAssignment(snbAssignments)
		newPodEntries[podUID][containerName].OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(snbAssignments)
	} else {
		general.Infof("pod: %s/%s container: %s is in ramp up, set its allocation result from %s to rampUpCPUs: %s",
			allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName,
			allocationInfo.AllocationResult.String(), rampUpCPUs.String())

		newPodEntries[podUID][containerName].OwnerPoolName = commonstate.EmptyOwnerPoolName
		newPodEntries[podUID][containerName].AllocationResult = rampUpCPUs.Clone()
		newPodEntries[podUID][containerName].OriginalAllocationResult = rampUpCPUs.Clone()
		newPodEntries[podUID][containerName].TopologyAwareAssignments = machine.DeepcopyCPUAssignment(rampUpCPUsTopologyAwareAssignments)
		newPodEntries[podUID][containerName].OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(rampUpCPUsTopologyAwareAssignments)
	}
} else {
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
go test -run 'TestAllocateSharedNumaBindingCPUsMarksColdStartRampUp|TestSharedNUMABindingRampUpStaysWithinHintedNUMA' -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy
```

Expected: PASS.

## Task 3: Preserve existing DNB and non-SNB semantics

- [ ] **Step 1: Add regression test for non-SNB still node-wide**

Add a test that creates ordinary `shared_cores + numa_binding=false` with `RampUp=true`, runs `adjustAllocationEntriesWithRampUpFloor`, and asserts assignment spans all eligible NUMAs rather than one hinted NUMA.

```go
func TestNonSNBRampUpRemainsNodeWide(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false)

	allocation := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "non-snb-ramp-up",
			PodNamespace:  "default",
			PodName:       "non-snb-ramp-up",
			ContainerName: "main",
			ContainerType: pluginapi.ContainerType_MAIN.String(),
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		RampUp:          true,
		RequestQuantity: 1,
		InitTimestamp:   time.Now().Format(util.QRMTimeFormat),
	}
	p.state.SetAllocationInfo(allocation.PodUid, allocation.ContainerName, allocation, false)

	err = p.adjustAllocationEntriesWithRampUpFloor(p.state.GetPodEntries(), p.state.GetMachineState(), false, machine.NewCPUSet(), false)
	require.NoError(t, err)

	updated := p.state.GetAllocationInfo(allocation.PodUid, allocation.ContainerName)
	require.NotNil(t, updated)
	require.True(t, updated.RampUp)
	require.False(t, updated.CheckSharedNUMABinding())
	require.GreaterOrEqual(t, len(updated.TopologyAwareAssignments), 2,
		"non-SNB ramp-up should remain node-wide, got %+v", updated.TopologyAwareAssignments)
}
```

- [ ] **Step 2: Run regression test**

Run:

```bash
go test -run TestNonSNBRampUpRemainsNodeWide -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy
```

Expected: PASS.

- [ ] **Step 3: Run existing DNB ramp-up tests**

Run:

```bash
go test -run 'TestDedicatedNUMAExclusiveRampUp|TestAllocateRestoresPreviousDNBWhenAtomicCommitFails|TestDynamicPolicyDeriveRampUpReclaimFloorCoversAllNUMAs' -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy
```

Expected: PASS.

## Task 4: Validate integration behavior

- [ ] **Step 1: Run dynamic policy package tests**

Run:

```bash
go test -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy
```

Expected: PASS.

- [ ] **Step 2: Run bulkhead-related tests**

Run:

```bash
go test -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/... ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/util ./pkg/agent/qrm-plugins/cpu/dynamicpolicy
```

Expected: PASS.

- [ ] **Step 3: Run race-focused topology regression**

Run:

```bash
go test -race -run 'TestTopologyCoordinator(ReplansStalePublishWithinInvocation|PublishFailureRetriesWithoutWrites)|TestCPUSetTopologyPluginPublishesOnlyContainerLeavesProvenByFinalSnapshot' -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/...
```

Expected: PASS.

- [ ] **Step 4: Run static checks**

Run:

```bash
go vet ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
git diff --check
```

Expected: both commands exit `0`.

## Task 5: Re-run four-case E2E

- [ ] **Step 1: Build fresh agent**

Use the adapter worktree and temporary replace to the current core worktree, then restore `go.mod`:

```bash
ADAPTER=/Users/bytedance/go/src/github.com/kubewharf/katalyst-adapter-rdt-rampup-merge
CORE=/Users/bytedance/go/src/github.com/kubewharf/katalyst-core/.worktrees/rdt-rampup-bulkhead-merge
API=/Users/bytedance/go/src/github.com/kubewharf/katalyst-api-rdt-rampup-merge
TAG=snb-numa-rampup-$(date +%Y%m%d%H%M%S)
cp "$ADAPTER/go.mod" "/tmp/adapter-go.mod.$TAG.bak"
cd "$ADAPTER"
go mod edit -replace=github.com/kubewharf/katalyst-core="$CORE"
go mod edit -replace=github.com/kubewharf/katalyst-api="$API"
GO111MODULE=on GOOS=linux GOARCH=amd64 GOFLAGS=-tags=SKIPCGO go build -o "./output/agent.$TAG" ./cmd/katalyst-agent/main.go
cp "/tmp/adapter-go.mod.$TAG.bak" "$ADAPTER/go.mod"
shasum -a 256 "./output/agent.$TAG"
git diff --exit-code -- go.mod
```

Expected: build succeeds, sha printed, adapter `go.mod` clean.

- [ ] **Step 2: Deploy to test node**

Use the established two-hop upload flow and verify:

```text
/proc/<agent_pid>/exe sha == uploaded agent sha
/proc/<runsv_pid>/root/opt/tiger/katalyst/agent sha == uploaded agent sha
/data00/tiger/katalyst/agent sha == uploaded agent sha
```

- [ ] **Step 3: Run four-case E2E**

Run with a fresh `RUN_TAG`:

```bash
cd /root/qrm-bulkhead-e2e/scripts
RUN_TAG=snbnuma$(date +%H%M%S) /tmp/run_fourcase_rampup_e2e.sh
```

Expected:

- NUMA-exclusive DNB: `ramp_up=True`, effective reclaim overlap `0`.
- NUMA-non-exclusive DNB: `ramp_up=True`, effective reclaim overlap `0`.
- SNB: `ramp_up=True`, `TopologyAwareAssignments` contains only the hinted NUMA.
- non-SNB: `ramp_up=True`, `TopologyAwareAssignments` may span multiple NUMAs.
- Final reset: `EXPECTED_STATE=OK mode=reset`.

- [ ] **Step 4: Package logs**

Package and pull logs into `qrm-bulkhead-test-artifacts/`, then verify:

```bash
tar -tzf qrm_bulkhead_<RUN_TAG>_logs.tgz >/dev/null
shasum -a 256 qrm_bulkhead_<RUN_TAG>_logs.tgz
```

Expected: local sha matches remote sha.

## Acceptance criteria

- SNB cold-start allocation carries `RampUp=true`.
- SNB ramp-up allocation never escapes its hinted NUMA.
- non-SNB shared ramp-up remains node-wide.
- DNB ramp-up semantics and atomic commit behavior remain unchanged.
- Effective reclaim cgroup remains disjoint from Pod allocation in E2E.
- Final reset succeeds after E2E.
