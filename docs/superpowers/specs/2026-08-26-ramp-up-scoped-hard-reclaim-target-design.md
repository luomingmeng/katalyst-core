# Ramp-Up-Scoped Hard Reclaim Target Design

## Status

Proposed. This design supersedes
`2026-08-17-always-on-hard-reclaim-floor-design.md`.

## Goal

Separate the steady reclaim reservation from the temporary ramp-up hard target:

- `reservedForReclaim` always represents the steady reclaim floor.
- `InitialRampUpReclaimCPUSetRatio` affects reclaim ownership only while at
  least one workload is entering or remains in ramp-up.
- QRM remains the only owner of concrete CPU identities.
- SysAdvisor, QRM precommit, and Bulkhead validate the same runtime target.

No API, protobuf, checkpoint schema, or dynamic configuration field changes are
introduced.

## Baseline Conflict

The configuration contract says that
`EnableRampUpReclaimHardPartition` applies while a workload is in ramp-up.
The always-on design instead made the ratio-derived target a persistent
per-NUMA invariant even when no allocation had `RampUp=true`.

That behavior leaks `InitialRampUpReclaimCPUSetRatio` into steady-state region
upper bounds, reclaim pool quantities, default-share residual capacity, and
Bulkhead validation. On a 32-CPU NUMA with ratio `0.2`, the steady
`reservedForReclaim` becomes six logical CPUs instead of the configured
whole-core steady reserve.

The configuration contract is authoritative for this change. The always-on
design and its tests are retired.

## Semantic Model

### Steady floor

For every update:

```text
steadyReservedByNUMA =
    ResolvePerNUMAReservedForReclaim(dynamicConfig, topology)
```

This value is independent of:

- `EnableRampUpReclaimHardPartition`;
- `InitialRampUpReclaimCPUSetRatio`;
- the presence of ramp-up workloads.

### Runtime activation

Hard partition is active when:

```text
EnableReclaim
&& EnableRampUpReclaimHardPartition
&& (enteringRampUp || candidateEntries contain RampUp=true)
```

Candidate entries, not only committed state, are used during admission and
precommit. Pool entries and pre-occupation entries do not activate the feature.

### Active target

The feature remains node-level while active. Any active ramp-up workload
activates a target on every physical NUMA:

```text
activeTargetByNUMA =
    ResolveHardPartitionReclaimTargets(
        dynamicConfig,
        topology,
        globalReservedFallback=0,
        perNUMAReservedFloor=steadyReservedByNUMA,
    )
```

The target is whole-core aligned and cannot be lower than the steady floor.
When the ratio is zero, the active target equals the steady floor.
While active, the target is also the effective reclaim reserve used by region
upper-bound and pool-capacity calculations. Treating it only as a final cap can
allow non-reclaim pools to consume capacity that the hard target must own.

### Deactivation

After the last ramp-up allocation becomes non-ramp-up:

- the active target disappears on the next candidate calculation;
- steady reserve remains;
- region reclaim behavior returns to normal policy:
  non-mixable regions keep the steady floor, while mixable regions may reclaim
  more according to their control knob;
- stale advisor responses cannot reactivate the target.

## Ownership

| Concern | Canonical owner |
| --- | --- |
| Steady per-NUMA reserve quantity | SysAdvisor using `pkg/util/machine` |
| Active hard target mathematics | `pkg/util/machine` pure resolver |
| Active ramp-up detection in QRM state | `dynamicpolicy/state` utility |
| Active target publication in advice | SysAdvisor |
| Concrete CPU identity selection and commit | QRM DynamicPolicy |
| Candidate validation | QRM precommit |
| Applied partition projection | Bulkhead |

Bulkhead does not derive a different target and does not repair an invalid
candidate. It only validates the runtime target supplied from the same
configuration, topology, and candidate-state activation decision.

## Data Flow

```text
immutable config snapshot
    -> steadyReservedByNUMA
    -> scan ramp-up metadata once
    -> activeTargetByNUMA or empty
    -> SysAdvisor quantity assembly
    -> QRM candidate CPU identity planning
    -> precommit validation
    -> checkpoint commit
    -> Bulkhead projection/apply
```

The target maps are rebuilt and atomically replaced per calculation. They are
never updated key by key and are not persisted.

For synchronous advice, QRM derives the ramp-up generation from the original
`GetAdviceRequest` and compares it with the locked current state before applying
the response. A mismatch rejects the stale response without side effects. The
legacy ListAndWatch path has no request snapshot and retains current-state
behavior.

## Complexity and Performance

- SysAdvisor performs one existing container scan per update. The scan both
  determines activation and builds the active target map.
- The assembler copies `RampUpActive` and `RampUpHardPartitionActive` into the
  in-process-only `InternalCPUCalculationResult` so CPU server does not scan
  MetaCache a second time. Neither bit enters advisor proto or checkpoint
  state. Live reclaim fallback is used only for legacy ramp-up without hard
  partition; it is disabled after final ramp-up exit so the pool can return to
  steady reserve.
- QRM exposes one `PodEntries` utility for activation. Admission, advisor
  planning, precommit, and Bulkhead reuse it instead of defining local scans.
- `ResolveHardPartitionReclaimTargets` is called only when active.
- No new goroutine, cache, timer, fallback, retry, or compatibility adapter is
  added.
- Existing large handler files receive only gate plumbing; pure state semantics
  stay in `state/util.go`, and quantity math stays in `pkg/util/machine`.

## Compatibility

- Hard partition disabled: behavior unchanged.
- Reclaim disabled: no hard target.
- Hard partition configured but inactive: steady behavior matches the legacy
  non-hard path.
- Active shared, SNB, DNB, and exclusive DNB ramp-up: the existing hard
  partition selection and fail-closed validation remain.
- Dedicated/reclaim and shared/reclaim overlap switches retain their current
  ownership semantics.
- Kubernetes 1.18 compatibility is unaffected.

## Retirement

The following behaviors are removed:

- hard ratio replacing steady `reservedForReclaim`;
- unconditional hard-floor derivation without ramp-up workload;
- ignoring `enteringRampUp`;
- config-only hard target validation in Bulkhead;
- suppressing live reclaim backfill merely because the feature is configured;
- comments and tests describing the ratio target as persistent.

`ResolveHardPartitionReclaimTargets` remains because it still owns active
target mathematics.

## Acceptance

1. With no active ramp-up, enabling the hard-partition flag and changing the
   initial ratio does not change steady advice, default-share capacity, region
   upper bounds, or Bulkhead validation.
2. The first entering ramp-up allocation establishes the target before commit.
3. While active, every physical NUMA satisfies the same whole-core target in
   SysAdvisor advice, QRM state, precommit, and Bulkhead view.
4. The last ramp-up exit removes the temporary target while preserving the
   steady reserve.
5. Candidate failure causes no checkpoint, cgroup, or RDT mutation.
6. Focused tests, package tests, race tests, and the real-node DNB transition
   test pass without overlap or hard-floor rejection.
