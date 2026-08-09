# Hard-Partition Reclaim Balance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guarantee a minimum of two reclaim CPUs per eligible NUMA and balanced concrete reclaim allocation whenever ramp-up reclaim hard partition is enabled.

**Architecture:** Sysadvisor raises only the hard-partition reserve floor to `2 * NUMACount`. QRM expands each FakedNUMA mandatory reclaim descriptor into deterministic per-NUMA sub-demands inside the existing disjoint min-cost-flow phase, unions sub-assignments back into the original block ID, and validates the final per-NUMA distribution before returning a block plan.

**Tech Stack:** Go, Sysadvisor CPU provision assembler, QRM advisor descriptor planner, min-cost-flow partition solver, testify.

---

### Task 1: Sysadvisor Hard-Partition Reserve Floor

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_helper.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_helper_test.go`

- [ ] **Step 1: Add failing table tests**

Add tests around a small helper:

```go
func hardPartitionReservedReclaimCores(
    ratioReserved, numaCount int,
) int
```

Cases:

```text
hard floor: ratio=0, NUMA=2 => 4
hard floor: ratio=3, NUMA=2 => 4
ratio wins: ratio=8, NUMA=2 => 8
four NUMAs: ratio=2, NUMA=4 => 8
no NUMA: ratio=3, NUMA=0 => 3
```

- [ ] **Step 2: Verify RED**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu \
  -run 'TestHardPartitionReservedReclaimCores' -count=1
```

Expected: compile failure because the helper is undefined.

- [ ] **Step 3: Implement the helper**

```go
func hardPartitionReservedReclaimCores(ratioReserved, numaCount int) int {
    if numaCount <= 0 {
        return ratioReserved
    }
    return general.Max(ratioReserved, numaCount*2)
}
```

In `getReservedForReclaim`, apply it only inside:

```go
if EnableRampUpReclaimHardPartition {
    ratioReserved := int(
        float64(totalAvailable) *
            InitialRampUpReclaimCPUSetRatio,
    )
    numReservedCores = hardPartitionReservedReclaimCores(
        ratioReserved,
        len(numaAvailable),
    )
}
```

Leave `machine.GetCoreNumReservedForReclaim` unchanged.

- [ ] **Step 4: Verify GREEN**

Run the focused test and the package test:

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu -count=1
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_helper.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_helper_test.go
git commit -m "fix(sysadvisor): raise hard reclaim floor per NUMA"
```

---

### Task 2: Balanced FakedNUMA Reclaim Demands

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner_test.go`

- [ ] **Step 1: Add failing pure helper tests**

Add:

```go
func balancedHardReclaimQuotas(
    quantity int,
    eligibleNUMAs []int,
    minimumPerNUMA int,
) (map[int]int, error)
```

Cases:

```text
4 over [0,1] => {0:2,1:2}
5 over [0,1] => {0:3,1:2}
8 over [0,1,2,3] => {0:2,1:2,2:2,3:2}
3 over [0,1] with minimum=2 => error
empty NUMAs => error
input NUMA order [1,0] => deterministic {0:2,1:2}
```

- [ ] **Step 2: Verify RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestBalancedHardReclaimQuotas' -count=1
```

- [ ] **Step 3: Implement quota calculation**

Sort and deduplicate NUMA IDs. Reject:

```text
no eligible NUMA
quantity < minimumPerNUMA * NUMACount
```

Distribute:

```go
base := quantity / len(numaIDs)
remainder := quantity % len(numaIDs)
```

Assign `base+1` to the first `remainder` NUMAs.

- [ ] **Step 4: Add descriptor expansion tests**

Add:

```go
func expandHardPartitionReclaimDemands(
    descriptor advisorBlockDescriptor,
    available machine.CPUSet,
    topology *machine.CPUTopology,
) ([]partitionDemand, error)
```

Verify:

- A four-CPU FakedNUMA mandatory reclaim descriptor becomes two demands of two.
- Each demand eligible set is limited to its physical NUMA.
- Previous reclaim preference is limited to the same NUMA.
- A real-NUMA descriptor remains one demand.
- A non-reclaim descriptor remains one demand.
- Insufficient per-NUMA eligible capacity returns an error.

- [ ] **Step 5: Implement descriptor expansion**

For `MandatoryReclaim + FakedNUMAID`:

1. Derive eligible NUMAs from `descriptor.Eligible`.
2. Calculate quotas with minimum two.
3. Create stable demand keys containing original component identity and NUMA ID.
4. Keep class `advisorBlockClassMandatoryReclaim`.
5. Restrict eligible and preferred sets to the physical NUMA.

For other descriptors, return the existing single demand.

- [ ] **Step 6: Run focused tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test(BalancedHardReclaimQuotas|ExpandHardPartitionReclaimDemands)' \
  -count=1
```

- [ ] **Step 7: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner_test.go
git commit -m "feat(qrm): expand hard reclaim across NUMAs"
```

---

### Task 3: Integrate Balanced Demands into the Disjoint Planner

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`

- [ ] **Step 1: Add failing integration tests**

Build a two-NUMA topology and a response with:

```text
reclaim block: NUMA=-1, quantity=4
hard partition=true
DD response=true
```

Assert:

```text
result[reclaimBlock] contains two CPUs from NUMA0
result[reclaimBlock] contains two CPUs from NUMA1
```

Add:

- Five CPUs produce `3/2`.
- Previous reclaim concentrated on NUMA1 still produces `2/2`.
- Quantity below four returns an error.
- hard partition disabled preserves existing global allocation.
- hard partition enabled with `DD response=false` fails before legacy allocation.

- [ ] **Step 2: Verify RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestPlanDisjointAdvisorBlocksBalancesHardReclaim' -count=1
```

- [ ] **Step 3: Expand demands in `solveAdvisorDescriptorPhase`**

When:

```text
preserveClass=true
hard partition enabled
descriptor class=mandatory-reclaim
descriptor NUMA=FakedNUMAID
```

append the per-NUMA demands returned by
`expandHardPartitionReclaimDemands`.

Map every sub-demand back to the original block ID.

- [ ] **Step 4: Union sub-demand assignments**

Change assignment collection from overwrite:

```go
result[blockID] = cpus
```

to:

```go
result[blockID] = result[blockID].Union(cpus)
```

Keep descriptor-level validation against the original global quantity and
eligible set.

- [ ] **Step 5: Reject unsupported hard/legacy combination**

In `generateBlockCPUSet`, before selecting the legacy path:

```go
if p.isRampUpReclaimHardPartitionEnabled() &&
    !resp.DisableDedicatedCoresOverlapReclaimedCores {
    return nil, fmt.Errorf(
        "hard-partition reclaim requires negotiated disjoint advisor planning",
    )
}
```

This avoids silently violating the hard-partition balance contract.

- [ ] **Step 6: Run integration and package tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
```

- [ ] **Step 7: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go
git commit -m "fix(qrm): balance hard reclaim in disjoint planning"
```

---

### Task 4: Pre-Commit and Bulkhead Validation

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view_test.go`

- [ ] **Step 1: Add failing validation tests**

Add:

```go
func validateHardPartitionReclaimDistribution(
    reclaim machine.CPUSet,
    eligible machine.CPUSet,
    topology *machine.CPUTopology,
    minimumPerNUMA int,
) error
```

Cases:

```text
2/2 succeeds
3/2 succeeds
4/0 fails
3/1 fails because NUMA1 is below two
CPUs outside eligible fail
hard partition disabled skips validation
```

- [ ] **Step 2: Implement validation**

Derive eligible NUMAs from `eligible`. Require:

```text
each NUMA size >= 2
max-min <= 1
reclaim subset of eligible
```

Call after block planning and before `applyBlocks` mutates state.

- [ ] **Step 3: Add Bulkhead view defense**

Add:

```go
HardPartitionEnabled bool
```

to `CPUSetPartitionViewOptions` and populate it from the current dynamic
configuration at every production call site.

When hard partition is enabled, `BuildValidatedCPUSetPartitionView` validates
`ReclaimEffectivePerNUMA`:

```text
every physical NUMA represented by the reclaim eligible domain is non-empty
each has at least two CPUs
max-min <= 1
```

Return a precise error before topology planning.

- [ ] **Step 4: Run focused and package tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestValidateHardPartitionReclaimDistribution' -count=1

go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils \
  -run 'TestBuildCPUSetPartitionView.*HardPartition' -count=1

go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
```

- [ ] **Step 5: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view_test.go
git commit -m "fix(qrm): validate hard reclaim per NUMA"
```

---

### Task 5: Cross-Package Verification

**Files:**
- No source changes expected

- [ ] **Step 1: Format and diff check**

```bash
gofmt -w <all changed Go files>
git diff --check
```

- [ ] **Step 2: Run Sysadvisor tests**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/... -count=1
```

- [ ] **Step 3: Run QRM tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
```

- [ ] **Step 4: Run repository build**

```bash
go build ./cmd/katalyst-agent
```

- [ ] **Step 5: Review commits and tree state**

```bash
git status --short --branch
git log --oneline -8
git diff HEAD~4..HEAD --check
```

Confirm the pre-existing local `go.mod` replace was not committed.

---

### Task 6: Target-Node Regression

**Files:**
- No source changes expected

- [ ] **Step 1: Build Adapter Agent**

Build the Adapter against the updated Core worktree and record its SHA256.

- [ ] **Step 2: Deploy QRM and Sysadvisor**

Back up both existing agents, deploy the same new binary, restart Sysadvisor
before QRM, and verify runtime plus identity SHA.

- [ ] **Step 3: Verify target advice**

Require:

```text
hard partition=true
DD=true
GetAdvice gate supported
reclaim quantity >= 2 * eligible NUMA count
```

- [ ] **Step 4: Run strict E2E**

Run:

```text
reset dry-run
reset actual
target dry-run
target actual
standard 3 rounds
high-churn 5 rounds
final reset
```

Target acceptance:

```text
reclaimed-0 size >= 2
reclaimed-1 size >= 2
size difference <= 1
both buckets within physical NUMA
workqueue and system equal reclaim union
no dynamic_target_mismatch
```

- [ ] **Step 5: Package evidence**

Return logs, GetAdvice request/response, QRM state snapshots, per-NUMA cpuset
checks, and final reset evidence to `qrm-bulkhead-test-artifacts`.
