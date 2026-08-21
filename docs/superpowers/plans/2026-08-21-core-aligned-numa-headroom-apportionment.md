# Core-Aligned NUMA Headroom Apportionment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve globally computed reclaimed CPU headroom across NUMA distribution while keeping every NUMA result aligned to complete physical cores in Core and Adapter.

**Architecture:** Core owns a deterministic, weighted, limit-aware NUMA CPU apportioner in `pkg/util/machine`. The Core common assembler, Core CPU reporter, and Adapter sysprobe policy call that single implementation; the generic memory reporter keeps its current behavior through optional CPU strategy injection.

**Tech Stack:** Go, Kubernetes `resource.Quantity`, Katalyst machine topology, SysAdvisor headroom assembler/reporter, QRM metaCache, Go table-driven tests.

---

## Isolated Workspaces

The worktrees already exist:

```text
Core:
  branch:   fix/core-aligned-numa-headroom-apportionment
  worktree: katalyst-core/.worktrees/core-aligned-numa-headroom-apportionment
  base:     b8290bd89

Adapter:
  branch:   fix/core-aligned-numa-headroom-apportionment-adapter
  worktree: katalyst-adapter-default-share/.worktrees/core-aligned-numa-headroom-apportionment-adapter
  base:     6e36519e
```

Baseline targeted tests pass:

```bash
# Core worktree
go test \
  ./pkg/util/machine \
  ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler \
  ./pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource

# Adapter worktree
go test \
  ./pkg/agent/sysadvisor/qosaware/test \
  ./pkg/agent/sysadvisor/qosaware/memory/reporter
```

Do not use Adapter-wide `go mod download` as a gate. The base branch currently
fails to resolve the unrelated private dependency
`code.byted.org/sys/bpfd@v0.8.3`, while the targeted packages pass from the
existing module cache.

## File Map

### Core

- Create `pkg/util/machine/numa_cpu_apportion.go`: pure deterministic CPU apportionment.
- Create `pkg/util/machine/numa_cpu_apportion_test.go`: topology quantum, limits, determinism, and real-node regression cases.
- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common.go`: replace local NUMA truncation and align binding NUMAs.
- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common_test.go`: assembler regression and invariants.
- Modify `pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic.go`: optional NUMA result strategy and atomic state update.
- Modify `pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic_test.go`: default memory-compatible behavior and strategy failure behavior.
- Modify `pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/cpu.go`: install CPU topology-aware strategy.
- Create `pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/cpu_test.go`: CPU reserve and NUMA consistency regressions.

### Adapter

- Modify `pkg/agent/sysadvisor/qosaware/cpu/assembler/headroom_sysprobe_adapter_policy.go`: use Core apportionment for the direct sysprobe path.
- Modify `pkg/agent/sysadvisor/qosaware/test/sysprobe_headroom_adapter_policy_test.go`: SMT2 and per-NUMA limit regressions.
- Modify `go.mod`: temporary local Core replace during development, then final pseudo-version.
- Modify `go.sum`: final published Core dependency checksums only.

## Task 1: Core NUMA CPU Apportioner

**Files:**

- Create: `pkg/util/machine/numa_cpu_apportion.go`
- Create: `pkg/util/machine/numa_cpu_apportion_test.go`

- [ ] **Step 1: Write failing real-node and invariant tests**

Create table-driven tests with neutral NUMA inputs:

```go
func TestApportionNUMACPU(t *testing.T) {
    t.Parallel()

    tests := []struct {
        name           string
        total          int64
        weights        map[int]int64
        limits         map[int]int64
        cpusPerCore    int
        want           map[int]int64
        wantEffective  int64
        wantErr        string
    }{
        {
            name:        "sm2 preserves 76 cpus with unequal limits",
            total:       76,
            weights:     map[int]int64{0: 32, 1: 32, 2: 32, 3: 32, 4: 32, 5: 32, 6: 32, 7: 32},
            limits:      map[int]int64{0: 10, 1: 10, 2: 10, 3: 10, 4: 10, 5: 10, 6: 8, 7: 8},
            cpusPerCore: 2,
            want:        map[int]int64{0: 10, 1: 10, 2: 10, 3: 10, 4: 10, 5: 10, 6: 8, 7: 8},
            wantEffective: 76,
        },
        {
            name:        "sm2 distributes post reserve total",
            total:       64,
            weights:     map[int]int64{0: 10, 1: 10, 2: 10, 3: 10, 4: 10, 5: 10, 6: 8, 7: 8},
            limits:      map[int]int64{0: 10, 1: 10, 2: 10, 3: 10, 4: 10, 5: 10, 6: 8, 7: 8},
            cpusPerCore: 2,
            want:        map[int]int64{0: 8, 1: 8, 2: 8, 3: 8, 4: 8, 5: 8, 6: 8, 7: 8},
            wantEffective: 64,
        },
        {
            name:        "sm2 aligns one global remainder",
            total:       37,
            weights:     map[int]int64{0: 1, 1: 1},
            limits:      map[int]int64{0: 40, 1: 40},
            cpusPerCore: 2,
            want:        map[int]int64{0: 18, 1: 18},
            wantEffective: 36,
        },
        {
            name:        "limits clamp effective total",
            total:       20,
            weights:     map[int]int64{0: 1, 1: 1},
            limits:      map[int]int64{0: 4, 1: 6},
            cpusPerCore: 2,
            want:        map[int]int64{0: 4, 1: 6},
            wantEffective: 10,
        },
        {
            name:        "reject invalid quantum",
            total:       8,
            weights:     map[int]int64{0: 1},
            limits:      map[int]int64{0: 8},
            cpusPerCore: 0,
            wantErr:     "cpus per core must be positive",
        },
    }

    for _, tt := range tests {
        tt := tt
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()
            got, effective, err := ApportionNUMACPU(
                tt.total, tt.weights, tt.limits, tt.cpusPerCore)
            if tt.wantErr != "" {
                require.EqualError(t, err, tt.wantErr)
                return
            }
            require.NoError(t, err)
            assert.Equal(t, tt.want, got)
            assert.Equal(t, tt.wantEffective, effective)
            assert.Equal(t, effective, sumNUMACPU(got))
            for numaID, value := range got {
                assert.Zero(t, value%int64(tt.cpusPerCore))
                assert.LessOrEqual(t, value, tt.limits[numaID])
            }
        })
    }
}
```

Add separate tests that construct identical maps in different insertion orders
and assert identical output. Add SMT1 and SMT4 cases, zero total, empty maps,
negative total, negative weight, negative limit, key mismatch, and a
positive-limit NUMA with zero weight.

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
go test ./pkg/util/machine -run 'TestApportionNUMACPU' -count=1
```

Expected: compilation fails because `ApportionNUMACPU` is undefined.

- [ ] **Step 3: Implement deterministic capped largest remainder**

Create:

```go
package machine

import (
    "fmt"
    "sort"
)

type numaCPUShare struct {
    numaID    int
    weight    int64
    limit     int64
    allocated int64
    remainder int64
}

func ApportionNUMACPU(
    total int64,
    weights map[int]int64,
    limits map[int]int64,
    cpusPerCore int,
) (map[int]int64, int64, error) {
    if cpusPerCore <= 0 {
        return nil, 0, fmt.Errorf("cpus per core must be positive")
    }
    if total < 0 {
        return nil, 0, fmt.Errorf("total cpu must not be negative")
    }

    quantum := int64(cpusPerCore)
    shares, err := newNUMACPUShares(weights, limits, quantum)
    if err != nil {
        return nil, 0, err
    }

    target := total / quantum
    var limitSum int64
    for i := range shares {
        limitSum += shares[i].limit
    }
    if target > limitSum {
        target = limitSum
    }

    if err := apportionPhysicalCores(shares, target); err != nil {
        return nil, 0, err
    }

    allocations := make(map[int]int64, len(shares))
    var effective int64
    for _, share := range shares {
        value := share.allocated * quantum
        allocations[share.numaID] = value
        effective += value
    }
    return allocations, effective, nil
}
```

Implement `newNUMACPUShares` to:

- require identical weight and limit key sets,
- sort NUMA IDs,
- reject negative values,
- convert limits to physical-core units,
- reject zero weight when aligned limit is positive.

Implement `apportionPhysicalCores` as a bounded loop:

1. Build the active list from entries below their limit.
2. Compute active total weight.
3. Allocate each candidate's floor share from the remaining target.
4. Cap floor shares at remaining limits.
5. Sort uncapped candidates by descending remainder, then ascending NUMA ID.
6. Allocate one remaining core to each eligible candidate in that order.
7. Repeat until the target is exhausted or no candidate has capacity.
8. Return a lower-case error if progress is impossible while target remains.

Use checked integer multiplication before calculating
`remaining*weight`; return `cpu apportionment overflow` on overflow.

- [ ] **Step 4: Run focused and package tests**

Run:

```bash
gofmt -w pkg/util/machine/numa_cpu_apportion.go \
  pkg/util/machine/numa_cpu_apportion_test.go
go test ./pkg/util/machine -run 'TestApportionNUMACPU' -count=1
go test ./pkg/util/machine -count=1
```

Expected: all tests pass.

- [ ] **Step 5: Commit the utility**

```bash
git add \
  pkg/util/machine/numa_cpu_apportion.go \
  pkg/util/machine/numa_cpu_apportion_test.go
git commit -m "feat(machine): add core-aligned numa cpu apportionment" \
  -m "Distribute logical CPU targets in physical-core quanta with deterministic weighted remainder handling and per-NUMA limits." \
  -m "Keep the effective total equal to the NUMA aggregate and reject invalid, ambiguous, or overflowing inputs."
```

## Task 2: Core Common Headroom Assembler

**Files:**

- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common_test.go`

- [ ] **Step 1: Add failing non-binding regression**

Add a test named:

```go
func TestHeadroomAssemblerCommon_PreservesCoreAlignedGlobalHeadroom(t *testing.T)
```

Build an SMT2 topology with eight equal NUMAs, a reclaim pool assignment of
`10,10,10,10,10,10,8,8`, overlap disabled, and `MaxOversoldRate=1`.
Assert:

```go
require.Equal(t, int64(76), total.Value())
require.Equal(t, map[int]int64{
    0: 10, 1: 10, 2: 10, 3: 10,
    4: 10, 5: 10, 6: 8, 7: 8,
}, headroomValues(numa))
```

Also assert every NUMA value is divisible by
`metaServer.CPUTopology.CPUsPerCore()` and does not exceed the reclaim pool
assignment.

- [ ] **Step 2: Run the regression and verify RED**

Run:

```bash
go test \
  ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler \
  -run 'TestHeadroomAssemblerCommon_PreservesCoreAlignedGlobalHeadroom' \
  -count=1
```

Expected: the current code reports 72 total with 9 on every NUMA.

- [ ] **Step 3: Add assembler helpers**

Add private helpers:

```go
func (ha *HeadroomAssemblerCommon) cpusPerCore() (int, error) {
    if ha.metaServer == nil || ha.metaServer.CPUTopology == nil {
        return 0, fmt.Errorf("cpu topology is unavailable")
    }
    cpusPerCore := ha.metaServer.CPUTopology.CPUsPerCore()
    if cpusPerCore <= 0 {
        return 0, fmt.Errorf("cpus per core must be positive")
    }
    return cpusPerCore, nil
}

func wholeCPUValue(q resource.Quantity) int64 {
    return q.MilliValue() / 1000
}
```

Add a non-binding apportion helper that constructs:

```go
weights[numaID] = int64(ha.metaServer.NUMAToCPUs.CPUSizeInNUMAs(numaID))
limits[numaID] = int64(reclaimPool.TopologyAwareAssignments[numaID].Size())
```

Call:

```go
allocations, effective, err := machine.ApportionNUMACPU(
    wholeCPUValue(headroom), weights, limits, cpusPerCore)
```

Convert allocations to `resource.Quantity` and rebuild `totalHeadroom` from
the allocation map.

- [ ] **Step 4: Align binding NUMA results independently**

For binding NUMAs:

```go
target := wholeCPUValue(headroom)
limit := int64(cpuSet.Size())
if target > limit {
    target = limit
}
target = target / int64(cpusPerCore) * int64(cpusPerCore)
```

Do not transfer binding NUMA remainder to another NUMA.

- [ ] **Step 5: Add apportionment metrics**

Emit through the existing assembler emitter:

```text
headroom_apportion_requested
headroom_apportion_effective
headroom_apportion_alignment_loss
```

Use only:

```go
metrics.MetricTag{Key: "component", Val: "assembler"}
metrics.MetricTag{Key: "resource", Val: "cpu"}
```

The loss is `requested-effective`, never negative. Detailed maps use `V(4)`.

- [ ] **Step 6: Run assembler tests**

```bash
gofmt -w \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common_test.go
go test \
  ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler \
  -count=1
```

Expected: all tests pass and the regression reports 76.

- [ ] **Step 7: Commit the assembler fix**

```bash
git add \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common_test.go
git commit -m "fix(sysadvisor): preserve assembler headroom across numas" \
  -m "Replace independent per-NUMA truncation with core-aligned weighted apportionment bounded by each reclaim topology assignment." \
  -m "Rebuild global headroom from the final NUMA map so the two representations remain a single consistent source of truth."
```

## Task 3: Core CPU Reporter

**Files:**

- Modify: `pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic_test.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/cpu.go`
- Create: `pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/cpu_test.go`

- [ ] **Step 1: Add failing CPU reserve regression**

In `cpu_test.go`, construct a CPU reporter apportioner with SMT2 topology and:

```go
current := map[int]resource.Quantity{
    0: resource.MustParse("10"),
    1: resource.MustParse("10"),
    2: resource.MustParse("10"),
    3: resource.MustParse("10"),
    4: resource.MustParse("10"),
    5: resource.MustParse("10"),
    6: resource.MustParse("8"),
    7: resource.MustParse("8"),
}
target := resource.MustParse("64")
```

Assert effective target is 64 and all eight NUMAs receive 8.

Add a sync-level regression that starts from 76 advisor headroom, applies a
12-CPU reserve, and verifies:

```text
lastReportResult = 64
sum(lastNUMAReportResult) = 64
metaCache TotalHeadroom = 64
metaCache NUMAHeadroom sum = 64
```

- [ ] **Step 2: Run reporter tests and verify RED**

```bash
go test \
  ./pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource \
  -run 'TestCPU.*Apportion|TestCPU.*Reserved' \
  -count=1
```

Expected: tests fail because the strategy API does not exist and current sync
logic independently truncates NUMA values.

- [ ] **Step 3: Add optional strategy API**

In `generic.go` add:

```go
type NUMAResultApportioner func(
    target resource.Quantity,
    current map[int]resource.Quantity,
) (resource.Quantity, map[int]resource.Quantity, error)

type GenericHeadroomManagerOption func(*GenericHeadroomManager)

func WithNUMAResultApportioner(
    apportioner NUMAResultApportioner,
) GenericHeadroomManagerOption {
    return func(manager *GenericHeadroomManager) {
        manager.numaResultApportioner = apportioner
    }
}
```

Add `numaResultApportioner NUMAResultApportioner` to the manager. Extend
`NewGenericHeadroomManager` with:

```go
opts ...GenericHeadroomManagerOption
```

Apply every option before returning. Existing callers remain source-compatible.

- [ ] **Step 4: Make custom strategy updates atomic**

After reserve and minimum handling, deep-copy the per-NUMA window results into
`map[int]resource.Quantity`.

When a strategy is installed:

```go
effective, allocations, err := m.numaResultApportioner(
    reportResult.DeepCopy(), current)
if err != nil {
    klog.Errorf("apportion numa result failed: %v", err)
    return
}
reportResult = &effective
```

Validate that the returned NUMA sum equals `effective`. Only then update:

- `lastReportResult`,
- `lastNUMAReportResult`,
- `HeadroomInfo`,
- metaCache.

When no strategy is installed, retain the existing generic ratio behavior so
memory output remains unchanged.

- [ ] **Step 5: Install CPU strategy**

In `cpu.go`, add:

```go
func newCPUNUMAResultApportioner(
    metaServer *metaserver.MetaServer,
) NUMAResultApportioner
```

The closure:

1. Reads `metaServer.CPUTopology.CPUsPerCore()`.
2. Converts target with `target.MilliValue()/1000`.
3. Uses each current NUMA value as both weight and limit.
4. Calls `machine.ApportionNUMACPU`.
5. Converts allocations and effective total back to decimal quantities.

Pass it with:

```go
WithNUMAResultApportioner(newCPUNUMAResultApportioner(metaServer))
```

- [ ] **Step 6: Test failure and memory compatibility**

In `generic_test.go` add:

- a strategy returning `apportion failure`; assert last valid state and
  metaCache are unchanged,
- a strategy returning mismatched total and NUMA sum; assert update is rejected,
- an existing memory-style manager without a strategy; assert its prior output
  remains unchanged.

- [ ] **Step 7: Emit reporter metrics**

Emit requested, effective, and alignment loss with:

```go
metrics.MetricTag{Key: "component", Val: "reporter"}
metrics.MetricTag{Key: "resource", Val: "cpu"}
```

Only the CPU strategy path emits these metrics.

- [ ] **Step 8: Run reporter and dependent tests**

```bash
gofmt -w \
  pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic.go \
  pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic_test.go \
  pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/cpu.go \
  pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/cpu_test.go
go test \
  ./pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource \
  -count=1
go test \
  ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler \
  ./pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource \
  -count=1
```

Expected: all tests pass.

- [ ] **Step 9: Commit the reporter fix**

```bash
git add \
  pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic.go \
  pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic_test.go \
  pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/cpu.go \
  pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/cpu_test.go
git commit -m "fix(sysadvisor): preserve reported headroom across numas" \
  -m "Inject CPU-specific core-aligned NUMA apportionment after sliding-window and reserved-resource processing." \
  -m "Update total and per-NUMA state atomically while retaining the existing generic memory reporter behavior."
```

## Task 4: Core Verification

**Files:** No new files.

- [ ] **Step 1: Run focused Core tests**

```bash
go test \
  ./pkg/util/machine \
  ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler \
  ./pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -count=1
```

Expected: all packages pass.

- [ ] **Step 2: Run focused Core race tests**

```bash
go test -race \
  ./pkg/util/machine \
  ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler \
  ./pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource \
  -count=1
```

Expected: all packages pass without race reports.

- [ ] **Step 3: Verify invariants statically**

```bash
git diff feat/default-share-residual-backfill...HEAD --check
git status --short
git log --oneline feat/default-share-residual-backfill..HEAD
```

Expected:

- no whitespace errors,
- clean worktree,
- exactly the intended utility, assembler, and reporter commits.

Do not push Core until Adapter passes against the local replace.

## Task 5: Adapter Local Replace and Sysprobe Policy

**Files:**

- Modify: `go.mod` temporarily
- Modify: `pkg/agent/sysadvisor/qosaware/cpu/assembler/headroom_sysprobe_adapter_policy.go`
- Modify: `pkg/agent/sysadvisor/qosaware/test/sysprobe_headroom_adapter_policy_test.go`

- [ ] **Step 1: Point Adapter at the local Core worktree**

From the Adapter implementation worktree:

```bash
go mod edit \
  -replace github.com/kubewharf/katalyst-core=../../../katalyst-core/.worktrees/core-aligned-numa-headroom-apportionment
```

Verify:

```bash
go list -m -f '{{.Replace.Dir}}' github.com/kubewharf/katalyst-core
```

Expected: the local Core implementation worktree path.

- [ ] **Step 2: Update the normal sysprobe test to RED**

For the existing 36-CPU, four-NUMA, SMT2 case, change expected NUMA headroom:

```go
numaHeadroom: map[int]resource.Quantity{
    0: *resource.NewQuantity(10, resource.DecimalSI),
    1: *resource.NewQuantity(10, resource.DecimalSI),
    2: *resource.NewQuantity(8, resource.DecimalSI),
    3: *resource.NewQuantity(8, resource.DecimalSI),
},
```

Add assertions that total equals the NUMA sum and every NUMA value is divisible
by `CPUsPerCore()`.

Add a case with unequal reclaimed cpuset limits and verify no NUMA exceeds its
local reclaimed intersection.

- [ ] **Step 3: Run the Adapter regression and verify RED**

```bash
go test \
  ./pkg/agent/sysadvisor/qosaware/test \
  -run 'TestHeadroomSysProbeAdapter' \
  -count=1
```

Expected: the existing implementation still returns `9,9,9,9`.

- [ ] **Step 4: Replace Adapter-local truncation**

Change the helper to:

```go
func (p *HeadroomSysProbeAdapter) getNUMAHeadroom(
    headroom float64,
    reclaimed machine.CPUSet,
) (map[int]resource.Quantity, int64, error)
```

Build inputs:

```go
weights := make(map[int]int64)
limits := make(map[int]int64)
for _, numaID := range p.metaServer.CPUDetails.NUMANodes().ToSliceInt() {
    numaCPUs := p.metaServer.CPUDetails.CPUsInNUMANodes(numaID)
    weights[numaID] = int64(numaCPUs.Size())
    limits[numaID] = int64(reclaimed.Intersection(numaCPUs).Size())
}
```

Call:

```go
allocations, effective, err := machine.ApportionNUMACPU(
    int64(headroom),
    weights,
    limits,
    p.metaServer.CPUTopology.CPUsPerCore(),
)
```

Return quantities plus `effective`. In `GetHeadroom`, use `effective` for the
global total. Leave the wrapped policy return path unchanged.

- [ ] **Step 5: Emit sysprobe metrics**

Emit requested, effective, and alignment loss using the existing emitter and:

```go
metrics.MetricTag{Key: "component", Val: "sysprobe"}
metrics.MetricTag{Key: "resource", Val: "cpu"}
```

- [ ] **Step 6: Run Adapter tests**

```bash
gofmt -w \
  pkg/agent/sysadvisor/qosaware/cpu/assembler/headroom_sysprobe_adapter_policy.go \
  pkg/agent/sysadvisor/qosaware/test/sysprobe_headroom_adapter_policy_test.go
go test \
  ./pkg/agent/sysadvisor/qosaware/test \
  ./pkg/agent/sysadvisor/qosaware/memory/reporter \
  -count=1
go test -race \
  ./pkg/agent/sysadvisor/qosaware/test \
  -count=1
```

Expected: all targeted tests pass.

- [ ] **Step 7: Commit Adapter code without the temporary replace**

Restore only the temporary dependency files:

```bash
git restore -- go.mod go.sum
```

Confirm no local path remains:

```bash
git diff -- go.mod go.sum
```

Expected: no output.

Commit:

```bash
git add \
  pkg/agent/sysadvisor/qosaware/cpu/assembler/headroom_sysprobe_adapter_policy.go \
  pkg/agent/sysadvisor/qosaware/test/sysprobe_headroom_adapter_policy_test.go
git commit -m "fix(sysadvisor): preserve sysprobe headroom across numas" \
  -m "Reuse Core physical-core apportionment for the direct sysprobe path and bound every NUMA by its reclaimed cpuset intersection." \
  -m "Keep global and per-NUMA headroom consistent while leaving the wrapped Core policy path unchanged."
```

## Task 6: Publish Core and Finalize Adapter Dependency

**Files:**

- Modify: Adapter `go.mod`
- Modify: Adapter `go.sum`

- [ ] **Step 1: Review Core before publication**

```bash
git status --short
git log --oneline feat/default-share-residual-backfill..HEAD
git diff --stat feat/default-share-residual-backfill...HEAD
```

Expected: clean Core worktree and only the planned changes.

- [ ] **Step 2: Push Core implementation branch**

```bash
git push -u luomingmeng fix/core-aligned-numa-headroom-apportionment
```

Record the pushed Core commit SHA.

- [ ] **Step 3: Update Adapter to the published Core commit**

From the Adapter worktree:

```bash
core_sha="$(
  git -C ../../../katalyst-core/.worktrees/core-aligned-numa-headroom-apportionment \
    rev-parse HEAD
)"
go get "github.com/kubewharf/katalyst-core@${core_sha}"
go mod tidy
```

Verify the exact Core SHA used by the command:

```bash
git -C ../../../katalyst-core/.worktrees/core-aligned-numa-headroom-apportionment rev-parse HEAD
```

Verify no filesystem replace remains:

```bash
go list -m -f '{{.Version}} {{if .Replace}}{{.Replace.Path}} {{.Replace.Version}}{{end}}' \
  github.com/kubewharf/katalyst-core
```

Expected: a pseudo-version ending in the pushed Core SHA prefix, not a local
directory.

- [ ] **Step 4: Re-run Adapter tests against the published dependency**

```bash
go test \
  ./pkg/agent/sysadvisor/qosaware/test \
  ./pkg/agent/sysadvisor/qosaware/memory/reporter \
  -count=1
go test -race \
  ./pkg/agent/sysadvisor/qosaware/test \
  -count=1
```

Expected: all tests pass.

- [ ] **Step 5: Commit the dependency update**

```bash
git add go.mod go.sum
git commit -m "build(deps): update core for numa headroom apportionment" \
  -m "Consume the published Core implementation that preserves global CPU headroom while enforcing physical-core-aligned NUMA results." \
  -m "The Adapter sysprobe policy was validated first with a local replace and then against this final pseudo-version."
```

- [ ] **Step 6: Push Adapter implementation branch**

```bash
git push -u origin fix/core-aligned-numa-headroom-apportionment-adapter
```

## Task 7: Real-Node Verification

**Files:** No source changes.

- [ ] **Step 1: Deploy Core and Adapter canary artifacts**

Use the existing AQC canary nodes. Do not change
`reservedResourceForReport.cpu=12`.

- [ ] **Step 2: Wait for reporter convergence**

Wait at least one full configured sliding-window interval plus two reporter
sync periods before comparing steady-state values.

- [ ] **Step 3: Verify the standard round**

On `dc05-pd-t44e-n017`, verify:

```text
reclaim sandbox cpuset sum = 76
every reclaim cpuset contains complete SMT siblings
SysAdvisor assembler total = 76
SysAdvisor assembler NUMA sum = 76
SysAdvisor reporter total = 64
SysAdvisor reporter NUMA sum = 64
QRM NUMA headroom = 8 on every NUMA
KCNR reclaimed_millicpu = 64000
```

Capture SysAdvisor and QRM log evidence for requested, effective, limits, and
allocations.

- [ ] **Step 4: Verify the high-churn round**

Repeatedly delete and recreate sandbox workloads. Verify:

- no split physical cores,
- no stranded SMT siblings,
- no total/NUMA mismatch,
- no persistent NUMA headroom oscillation,
- no QRM state corruption,
- KCNR reconverges to 64000m after churn.

- [ ] **Step 5: Run final repository checks**

Core:

```bash
git status --short
git log --oneline feat/default-share-residual-backfill..HEAD
```

Adapter:

```bash
git status --short
git log --oneline feat/default-share-residual-backfill-adapter..HEAD
```

Expected: both worktrees are clean and contain only planned atomic commits.

## Final Acceptance

- The shared Core utility is the only NUMA CPU remainder implementation.
- Core assembler preserves 76 CPUs on the target topology.
- Core reporter applies the 12-CPU AQC reserve and publishes 64 CPUs.
- Adapter direct sysprobe policy uses the same Core utility.
- All per-NUMA values are multiples of `CPUsPerCore`.
- All global totals equal their NUMA sums.
- No NUMA exceeds its reclaim supply.
- Memory reporter output is unchanged.
- No new AQC field, command-line flag, or compatibility alias exists.
- Local-replace and published-dependency Adapter tests both pass.
- Standard and high-churn real-node verification pass.
