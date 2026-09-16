# Hard-Partition SNB Pool Ratio Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Size shared-core NUMA-binding pools at request ratio `1.0` while hard reclaim partitioning is active, preserving ratio `2.0` in all other modes.

**Architecture:** `DynamicPolicy` owns configuration interpretation and selects the effective SNB ratio. Configuration-agnostic state helpers receive that ratio explicitly and apply it only to shared-core NUMA-binding entries in both incremental and full-reconstruction quantity paths.

**Tech Stack:** Go 1.18, Kubernetes CPU plugin state, `testify/require`, table-driven unit tests.

---

## File Structure

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util.go`: accept an explicit SNB ratio in pool quantity helpers.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util_test.go`: prove configurable SNB sizing and unchanged non-binding shared sizing.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`: select the ratio from the effective hard-partition predicate and pass it through every quantity path.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`: prove the configuration matrix and update direct helper calls.

### Task 1: Make SNB Ratio Explicit in State Helpers

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util.go`

- [ ] **Step 1: Add a failing configurable-ratio test**

Extend `TestCountAllocationInfosToPoolsQuantityMap` so its test arguments carry `sharedNUMABindingCPUIncrRatio`. Add cases using four `1 CPU` SNB entries in one NUMA pool:

```go
{
	name: "SNB ratio one",
	args: args{
		allocationInfos: []*AllocationInfo{
			newTestSharedNUMABindingAllocation("pod-0", "snb-pool", 0, 1),
			newTestSharedNUMABindingAllocation("pod-1", "snb-pool", 0, 1),
			newTestSharedNUMABindingAllocation("pod-2", "snb-pool", 0, 1),
			newTestSharedNUMABindingAllocation("pod-3", "snb-pool", 0, 1),
		},
		poolsQuantityMap:                map[string]map[int]int{},
		sharedNUMABindingCPUIncrRatio: 1.0,
	},
	want: map[string]map[int]int{"snb-pool": {0: 4}},
},
{
	name: "SNB ratio two",
	args: args{
		allocationInfos: []*AllocationInfo{
			newTestSharedNUMABindingAllocation("pod-0", "snb-pool", 0, 1),
			newTestSharedNUMABindingAllocation("pod-1", "snb-pool", 0, 1),
			newTestSharedNUMABindingAllocation("pod-2", "snb-pool", 0, 1),
			newTestSharedNUMABindingAllocation("pod-3", "snb-pool", 0, 1),
		},
		poolsQuantityMap:                map[string]map[int]int{},
		sharedNUMABindingCPUIncrRatio: 2.0,
	},
	want: map[string]map[int]int{"snb-pool": {0: 8}},
},
```

Add a non-binding shared case with ratio `2.0` that still expects its unscaled request quantity. Pass the new argument from the test table to `CountAllocationInfosToPoolsQuantityMap`.

- [ ] **Step 2: Run the state test and verify RED**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state \
  -run '^TestCountAllocationInfosToPoolsQuantityMap$' -count=1
```

Expected: compile failure because the state helper does not yet accept `sharedNUMABindingCPUIncrRatio`.

- [ ] **Step 3: Implement the explicit ratio**

Change the helper signatures:

```go
func GetSharedQuantityMapFromPodEntries(
	numaResourcePackagePinnedCPUSet map[int]map[string]machine.CPUSet,
	podEntries PodEntries,
	ignoreAllocationInfos []*AllocationInfo,
	getContainerRequestedCores GetContainerRequestedCoresFunc,
	sharedNUMABindingCPUIncrRatio float64,
) (map[string]map[int]int, error)

func CountAllocationInfosToPoolsQuantityMap(
	numaResourcePackagePinnedCPUSet map[int]map[string]machine.CPUSet,
	allocationInfos []*AllocationInfo,
	poolsQuantityMap map[string]map[int]int,
	getContainerRequestedCores GetContainerRequestedCoresFunc,
	sharedNUMABindingCPUIncrRatio float64,
) error
```

Replace the fixed ratio selection with:

```go
cpuIncrRatio := cpuconsts.CPUIncrRatioDefault
if allocationInfo.CheckSharedNUMABinding() {
	cpuIncrRatio = sharedNUMABindingCPUIncrRatio
}
reqFloat64 := getContainerRequestedCores(allocationInfo) * cpuIncrRatio
```

Pass the ratio unchanged from `GetSharedQuantityMapFromPodEntries` into `CountAllocationInfosToPoolsQuantityMap`. Remove `GetCPUIncrRatio` after all callers are migrated; it must not remain as a second owner of the ratio decision.

- [ ] **Step 4: Run the state test and verify GREEN**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state \
  -run '^(TestCountAllocationInfosToPoolsQuantityMap|TestCPUPreciseCeil)$' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit the state contract**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util_test.go
git commit -m "refactor(qrm-cpu): make snb pool ratio explicit"
```

### Task 2: Select the Ratio in DynamicPolicy

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`

- [ ] **Step 1: Add the failing configuration matrix test**

Add a table-driven test:

```go
func TestSharedNUMABindingCPUIncrRatio(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name          string
		enableReclaim bool
		enableHard    bool
		want          float64
	}{
		{name: "both disabled", want: cpuconsts.CPUIncrRatioSharedCoresNUMABinding},
		{name: "hard only", enableHard: true, want: cpuconsts.CPUIncrRatioSharedCoresNUMABinding},
		{name: "reclaim only", enableReclaim: true, want: cpuconsts.CPUIncrRatioSharedCoresNUMABinding},
		{name: "hard partition active", enableReclaim: true, enableHard: true, want: cpuconsts.CPUIncrRatioDefault},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
			require.NoError(t, err)
			p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
			require.NoError(t, err)
			dyn := p.dynamicConfig.GetDynamicConfiguration()
			dyn.EnableReclaim = tt.enableReclaim
			dyn.EnableRampUpReclaimHardPartition = tt.enableHard
			require.Equal(t, tt.want, p.getSharedNUMABindingCPUIncrRatio())
		})
	}
}
```

- [ ] **Step 2: Run the policy test and verify RED**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestSharedNUMABindingCPUIncrRatio$' -count=1
```

Expected: compile failure because `getSharedNUMABindingCPUIncrRatio` does not exist.

- [ ] **Step 3: Implement the policy selector**

Add next to `isRampUpReclaimHardPartitionEnabled`:

```go
func (p *DynamicPolicy) getSharedNUMABindingCPUIncrRatio() float64 {
	if p.isRampUpReclaimHardPartitionEnabled() {
		return cpuconsts.CPUIncrRatioDefault
	}
	return cpuconsts.CPUIncrRatioSharedCoresNUMABinding
}
```

At the start of request-derived pool quantity calculation, capture the ratio once:

```go
sharedNUMABindingCPUIncrRatio := p.getSharedNUMABindingCPUIncrRatio()
```

Pass it to all calls to:

```go
state.CountAllocationInfosToPoolsQuantityMap(...)
state.GetSharedQuantityMapFromPodEntries(...)
```

Update the direct reconstruction call in `adjustAllocationEntriesAtRevisionWithContext` as well. Update test-only direct calls to pass the policy-selected ratio, rather than hard-coding `1.0` or `2.0`.

- [ ] **Step 4: Run policy tests and verify GREEN**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestSharedNUMABindingCPUIncrRatio|TestAdjustPoolsAndIsolatedEntriesWithRampUpFloorRejectsPinnedSNBPoolShrinkAtomically|TestAdjustPoolsAndIsolatedEntriesWithRampUpFloorRejectsBareOwnedPinnedSNBPoolShrinkAtomically)$' \
  -count=1
```

Expected: PASS. Existing hard-partition tests that intentionally construct legacy `2.0` quantities must explicitly request the legacy ratio only when that quantity is part of the test fixture rather than policy behavior.

- [ ] **Step 5: Commit policy wiring**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go
git commit -m "fix(qrm-cpu): use unit snb ratio in hard partition"
```

### Task 3: Verify Boundaries

**Files:**
- Verify only; no planned source changes.

- [ ] **Step 1: Run state and dynamic-policy package tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
```

Expected: both packages PASS.

- [ ] **Step 2: Run race-sensitive targeted tests**

```bash
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'SharedNUMABindingCPUIncrRatio|AdjustPoolsAndIsolatedEntriesWithRampUpFloor' \
  -count=1
```

Expected: PASS with no race reports.

- [ ] **Step 3: Check formatting, diff, and worktree state**

```bash
gofmt -w \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go
git diff --check
git status --short
```

Expected: no formatting or whitespace errors; only intentional plan/implementation commits are present.

- [ ] **Step 4: Review the final diff against the design**

Confirm:

- hard partition active is exactly `EnableReclaim && EnableRampUpReclaimHardPartition`;
- SNB request-derived quantity uses `1.0` only in that mode;
- ordinary shared and dedicated accounting are unchanged;
- advisor-provided existing pool CPU sets remain authoritative;
- no duplicate ratio-selection owner remains in `state`.

- [ ] **Step 5: Squash implementation commits if requested**

Keep the design commit separate. If a logical squash is requested, squash the two implementation commits into:

```text
fix(qrm-cpu): use unit snb ratio in hard partition
```
