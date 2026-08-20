# Core-Aligned NUMA Headroom Apportionment Design

## Status

Approved for implementation planning.

## Context

The `feat/default-share-residual-backfill` Core branch and the
`feat/default-share-residual-backfill-adapter` Adapter branch make reclaim
supply overlap-aware and keep the materialized reclaim cpuset aligned to
complete physical cores.

On node `dc05-pd-t44e-n017`, the materialized reclaim sandbox cpuset contains
76 logical CPUs:

- NUMA 0-5 contain 10 logical CPUs each.
- NUMA 6-7 contain 8 logical CPUs each.
- Every selected logical CPU has its SMT sibling in the same reclaim cpuset.

The active AQC is `default/sandbox-on-gpu-host`. Its relevant settings are:

```yaml
advisorConfig:
  cpuAdvisorConfig:
    allowSharedCoresOverlapReclaimedCores: false
    disableDedicatedCoresOverlapReclaimedCores: true
    cpuProvisionConfig:
      reclaimedCPUMaxRatio: 0.3
reclaimedResourceConfig:
  cpuHeadroomConfig:
    utilBasedConfig:
      enable: true
      maxHeadroomCapacityRate: 0.3
      maxOversoldRate: 1
      targetReclaimedCoreUtilization: 0.65
      maxReclaimedCoreUtilization: 0.9
  reservedResourceForReport:
    cpu: "12"
```

The current reporting path loses capacity twice:

1. The Core common headroom assembler independently truncates every
   proportional NUMA result. A 76-CPU global result becomes 9 CPUs on each of
   eight NUMAs, for a 72-CPU aggregate.
2. The Core generic reporter subtracts the 12-CPU report reserve, scales every
   NUMA independently, and truncates each scaled value. The resulting 7 CPUs
   per NUMA produce a 56-CPU aggregate.

The Adapter sysprobe headroom policy contains another independent
`headroom / numaCount` followed by per-NUMA integer truncation.

These losses are not utilization policy decisions. They are artifacts of
performing local rounding without reconciling the global remainder.

## Goals

- Preserve the globally computed CPU headroom when distributing it across
  NUMA nodes.
- Keep every NUMA CPU headroom value aligned to complete physical cores.
- Keep global headroom and the sum of NUMA headroom values identical.
- Respect each NUMA node's actual reclaim supply limit.
- Produce deterministic output independent of Go map iteration order.
- Use one implementation across Core and Adapter.
- Leave memory headroom behavior unchanged.
- Preserve the existing AQC and command-line configuration surface.
- Verify Core and Adapter together before publishing the final Core dependency.

## Non-Goals

- Changing utilization-based headroom estimation.
- Changing reclaim overlap semantics.
- Changing `ReservedResourceForReport` precedence or value.
- Introducing a feature gate for correct apportionment.
- Rotating remainder ownership between NUMA nodes across reporting cycles.
- Carrying fractional CPU debt across process restarts or reporting cycles.
- Changing the QRM advisor protocol to use millicpu values end to end.

## Hard Invariants

For a requested logical CPU total `T`, per-NUMA allocation `A[n]`, per-NUMA
limit `L[n]`, and physical-core quantum `Q`:

```text
Q > 0
A[n] >= 0
A[n] % Q == 0
A[n] <= floor(L[n] / Q) * Q
sum(A[n]) == effectiveTotal
effectiveTotal <= T
T - effectiveTotal < Q, unless the aligned limits cannot satisfy T
```

When aligned limits cannot satisfy the requested total:

```text
effectiveTotal = min(
  floor(T / Q) * Q,
  sum(floor(L[n] / Q) * Q),
)
```

The published total must always equal `sum(A[n])`. A caller must never publish
the pre-apportion target together with a smaller NUMA aggregate.

## Chosen Approach

Use deterministic weighted largest-remainder apportionment in physical-core
quanta.

The implementation converts logical CPU quantities into physical-core units,
performs weighted and capped apportionment, and converts the result back into
logical CPU quantities. Integer arithmetic is used for remainder ordering so
that floating-point precision cannot affect allocation order.

This approach is preferred over end-to-end millicpu because it:

- preserves the existing integer CPU contract,
- directly enforces the core-aligned invariant,
- avoids advisor protocol changes,
- can be shared by Core and Adapter,
- has no persistent state,
- bounds unavoidable loss to less than one physical core.

## Shared Core Utility

Add:

```text
pkg/util/machine/numa_cpu_apportion.go
pkg/util/machine/numa_cpu_apportion_test.go
```

The exported API is:

```go
func ApportionNUMACPU(
    total int64,
    weights map[int]int64,
    limits map[int]int64,
    cpusPerCore int,
) (
    allocations map[int]int64,
    effectiveTotal int64,
    err error,
)
```

All input and output quantities are logical CPU counts.

### Validation

The function returns an error when:

- `cpusPerCore <= 0`,
- `total < 0`,
- a weight or limit is negative,
- a NUMA with a positive aligned limit has no usable weight,
- the key sets are inconsistent in a way that would silently drop capacity.

Zero total and empty capacity return an empty allocation and zero effective
total without error.

### Algorithm

1. Sort NUMA IDs in ascending order.
2. Align `total` and every limit down to `cpusPerCore`.
3. Convert total and limits to physical-core units.
4. Clamp the target to the sum of aligned limits.
5. Calculate each NUMA's ideal weighted share.
6. Assign the integer floor of each share, capped by its limit.
7. Recalculate over the remaining uncapped candidates when a cap is reached.
8. Assign remaining physical cores by descending fractional remainder.
9. Break equal remainders by ascending NUMA ID.
10. Convert physical-core allocations back to logical CPU quantities.
11. Validate all invariants before returning.

Remainders are compared with integer cross multiplication instead of
`float64`.

## Core Common Headroom Assembler

Modify:

```text
pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common.go
pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/headroomassembler/assembler_common_test.go
```

### Non-Binding NUMAs

Replace the loop-local proportional conversion:

```go
headroomForNUMA :=
    float64(headroom.Value()) *
    float64(numaCPUSize) /
    float64(totalCPUSize)
q := *resource.NewQuantity(int64(headroomForNUMA), resource.DecimalSI)
```

with one `machine.ApportionNUMACPU` call.

Inputs are:

- `total`: the utilization estimator result converted to whole logical CPUs
  and aligned down to `CPUsPerCore`.
- `weights`: machine CPU capacity in each non-binding NUMA.
- `limits`: reclaim pool topology-aware assignment size in each NUMA, aligned
  down to `CPUsPerCore`.
- `cpusPerCore`: `metaServer.CPUTopology.CPUsPerCore()`.

The returned `effectiveTotal` and NUMA allocation become the assembler result.

### Binding NUMAs

Binding NUMAs have independent cgroup metrics and utilization calculations.
Their fractional capacity must not be transferred to another binding NUMA.

Each independently estimated result is therefore:

```text
floor(min(estimatedHeadroom, reclaimSupply) / CPUsPerCore) * CPUsPerCore
```

The less-than-one-core difference is an unavoidable topology alignment loss,
not an apportionment loss.

### Total Result

The assembler's total result is rebuilt from the final NUMA map. The
pre-apportion estimator result must not be returned as a separate source of
truth.

## Core Reporter

Modify:

```text
pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic.go
pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/generic_test.go
pkg/agent/sysadvisor/plugin/qosaware/reporter/manager/resource/cpu.go
```

The generic reporter serves both CPU and memory. It must not directly depend
on CPU topology.

### Strategy Injection

Add:

```go
type NUMAResultApportioner func(
    target resource.Quantity,
    current map[int]resource.Quantity,
) (
    effectiveTarget resource.Quantity,
    allocations map[int]resource.Quantity,
    err error,
)
```

Add a variadic functional option:

```go
type GenericHeadroomManagerOption func(*GenericHeadroomManager)

func WithNUMAResultApportioner(
    apportioner NUMAResultApportioner,
) GenericHeadroomManagerOption
```

`NewGenericHeadroomManager` accepts variadic options after its existing
arguments. Existing callers continue to compile unchanged.

### CPU Strategy

`NewCPUHeadroomManager` injects a CPU-specific strategy backed by
`machine.ApportionNUMACPU`.

After sliding-window processing, reserve subtraction, and minimum-result
handling:

- `total` is the global report target.
- `weights` are the current per-NUMA sliding-window results.
- `limits` are the same per-NUMA sliding-window results, aligned down to
  complete cores.
- `cpusPerCore` comes from machine topology.

The strategy cannot increase a NUMA above the advisor result.

After successful apportionment:

```text
lastReportResult = effectiveTarget
lastNUMAReportResult = allocations
HeadroomInfo.TotalHeadroom = effectiveTarget
HeadroomInfo.NUMAHeadroom = allocations
```

On apportionment failure, the reporter keeps the previous valid state and does
not partially update metaCache.

### Memory Strategy

The memory reporter does not install the CPU strategy. Its current byte-level
behavior remains unchanged.

## Adapter Sysprobe Policy

Modify on the Adapter implementation branch:

```text
pkg/agent/sysadvisor/qosaware/cpu/assembler/headroom_sysprobe_adapter_policy.go
pkg/agent/sysadvisor/qosaware/test/sysprobe_headroom_adapter_policy_test.go
go.mod
go.sum
```

Change:

```go
func (p *HeadroomSysProbeAdapter) getNUMAHeadroom(
    headroom float64,
    reclaimed machine.CPUSet,
) (map[int]resource.Quantity, int64, error)
```

Inputs to `machine.ApportionNUMACPU` are:

- `total`: the whole logical CPU headroom, aligned to `CPUsPerCore`.
- `weights`: machine CPU capacity per NUMA.
- `limits`: the size of `reclaimed` intersected with each NUMA CPU set.
- `cpusPerCore`: topology `CPUsPerCore()`.

The direct sysprobe path uses this result for both total and NUMA headroom.
The wrapped Core policy path returns the Core result unchanged and does not
apportion it a second time.

No Adapter startup parameter or AQC mapping is added.

## Expected Node Result

For `dc05-pd-t44e-n017`:

```text
reclaim supply:       76 logical CPUs
CPUsPerCore:           2
report reserve:       12 logical CPUs
```

Assembler output:

```text
NUMA 0-5: 10 logical CPUs each
NUMA 6-7:  8 logical CPUs each
total:    76 logical CPUs
```

Reporter and QRM output:

```text
NUMA 0-7: 8 logical CPUs each
total:   64 logical CPUs
KCNR:    64000m
```

The expected behavior replaces the current accidental result:

```text
76 -> 72 -> 60 -> 56
```

with:

```text
76 -> 76 -> 64 -> 64
```

## Observability

Add low-cardinality metrics:

```text
headroom_apportion_requested
headroom_apportion_effective
headroom_apportion_alignment_loss
```

Allowed labels:

```text
component=assembler|reporter|sysprobe
resource=cpu
```

Detailed weights, limits, and allocations are logged at `V(4)`. Info-level
logging is limited to alignment loss, insufficient limits, and state changes.
Errors use lower-case messages.

## Test Plan

### Core Utility

Cover:

- SMT1, SMT2, and SMT4,
- equal and unequal weights,
- equal and unequal limits,
- zero total,
- total below one physical core,
- total not divisible by the core quantum,
- limits smaller than the requested total,
- zero-limit NUMAs,
- invalid inputs,
- deterministic output across map insertion orders,
- equal remainder tie-breaking,
- the real 76-CPU node case,
- the 64-CPU post-reserve case.

### Core Assembler

Verify:

- 76 global CPUs remain 76 after NUMA distribution,
- every NUMA value is core-aligned,
- no NUMA exceeds its reclaim topology assignment,
- total equals the NUMA sum,
- binding NUMAs align independently,
- SMT1 behavior remains valid,
- invalid topology fails explicitly.

### Core Reporter

Verify:

- 76 minus a 12-CPU reserve becomes a 64-CPU effective result,
- total and NUMA sum remain identical,
- an apportionment error preserves the previous valid state,
- a non-ready sliding window does not update state,
- minimum report handling remains aligned,
- reserve larger than headroom cannot create negative allocation,
- memory reporter behavior is unchanged.

### Adapter

Verify:

- the direct sysprobe path uses Core apportionment,
- a 36-CPU SMT2 result over four NUMAs becomes a valid distribution such as
  `10,10,8,8`, rather than four non-aligned 9-CPU values,
- unequal reclaimed cpuset limits are respected,
- total equals the NUMA sum,
- the wrapped path is not apportioned twice,
- Adapter memory reporter tests still compile and pass.

### Runtime

Run standard and high-churn real-node rounds. Validate:

- reclaim cpusets always contain complete physical cores,
- assembler total equals assembler NUMA sum,
- reporter total equals reporter NUMA sum,
- QRM NUMA headroom sums to the reported total,
- KCNR reaches 64000m after the configured sliding window converges,
- no periodic NUMA headroom oscillation occurs,
- pod deletion and recreation do not strand or split physical cores.

## Worktree and Branch Isolation

Implementation must not occur in the existing feature worktrees.

Create a new Core worktree from `feat/default-share-residual-backfill`:

```text
branch:   fix/core-aligned-numa-headroom-apportionment
worktree: .worktrees/core-aligned-numa-headroom-apportionment
```

Create a new Adapter worktree from
`feat/default-share-residual-backfill-adapter`:

```text
branch:   fix/core-aligned-numa-headroom-apportionment-adapter
worktree: .worktrees/core-aligned-numa-headroom-apportionment-adapter
```

Before creation:

- fetch the relevant remotes,
- verify both base branches are clean and at the expected tips,
- verify the proposed worktree directories do not already exist,
- verify the proposed branches do not already exist.

## Cross-Repository Validation

During implementation, the Adapter worktree temporarily replaces Core with
the local Core implementation worktree:

```bash
go mod edit \
  -replace github.com/kubewharf/katalyst-core=../../../katalyst-core/.worktrees/core-aligned-numa-headroom-apportionment
```

The temporary local replace path must not be committed.

Validation order:

1. Implement and test the Core utility.
2. Implement and test the Core assembler.
3. Implement and test the Core reporter.
4. Point Adapter at the local Core worktree.
5. Implement and test the Adapter sysprobe policy.
6. Run targeted race tests in both repositories.
7. Run cross-repository compile and test checks with the local replace.
8. Remove the temporary local replace.
9. Commit and push Core.
10. Update Adapter to the published Core pseudo-version.
11. Regenerate and commit Adapter `go.mod` and `go.sum`.
12. Repeat Adapter tests without the local replace.
13. Run real-node standard and high-churn validation.

## Commit Plan

Core commit 1:

```text
feat(machine): add core-aligned numa cpu apportionment
```

Core commit 2:

```text
fix(sysadvisor): preserve cpu headroom across numa distribution
```

Adapter commit:

```text
fix(sysadvisor): preserve sysprobe headroom across numas
```

Each commit must include a detailed body describing invariants, behavior
changes, and verification. Unrelated files must not be staged.

## Rollout and Risk

The fix raises this node's final reclaimed CPU report from 56 to 64. This is an
intentional recovery of capacity already allowed by the 76-CPU reclaim supply
and the 12-CPU AQC reserve; it does not change the utilization target or
oversold limit.

The main operational risk is increased reclaimed workload admission. Rollout
must therefore:

1. start with the existing AQC canary nodes,
2. wait for reporter sliding-window convergence,
3. compare reclaim utilization, scheduling pressure, and QRM pool stability,
4. complete standard and high-churn rounds,
5. expand only after total/NUMA consistency and core alignment remain stable.

No fallback alias or compatibility flag is introduced. If validation fails,
rollback uses the previous Core and Adapter artifacts.

## Acceptance Criteria

- Core and Adapter use the same apportionment implementation.
- No local proportional CPU truncation remains in the three affected paths.
- CPU NUMA headroom is aligned to `CPUsPerCore`.
- Global CPU headroom always equals the NUMA aggregate.
- Per-NUMA limits are never exceeded.
- Memory reporting is unchanged.
- No new AQC or command-line option is introduced.
- Core and Adapter targeted unit and race tests pass.
- Adapter passes with a temporary local Core replace and with the final
  published Core pseudo-version.
- The target node converges to 64 CPUs of reported reclaimed capacity.
- Standard and high-churn real-node validation pass.
