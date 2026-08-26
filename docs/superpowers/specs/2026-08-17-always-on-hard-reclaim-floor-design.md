# Always-On Hard Reclaim Floor Design

## Status

Superseded by
`2026-08-26-ramp-up-scoped-hard-reclaim-target-design.md`.

This document is retained only as historical incident and design context. Its
always-on activation rule is not current behavior.

## Historical Goal

The superseded proposal treated the hard-partition flag as an always-on
ownership rule. The replacement design restores `reservedForReclaim` as the
steady floor and applies the ratio-derived target only while a real workload
is entering or remains in ramp-up.

## Incident

On an eight-NUMA node, the effective dynamic configuration contained:

```text
DisableSharedCoresRampUp=true
EnableRampUpReclaimHardPartition=true
InitialRampUpReclaimCPUSetRatio=0.2
```

Because all shared and shared-NUMA-binding allocations had `RampUp=false`,
`adjustPoolsAndIsolatedEntriesWithRampUpFloorAtRevision` did not derive an
explicit reclaim floor. Repeated pool recalculations preserved the current
reclaim pool only after subtracting newly placed non-reclaim pools. The reclaim
set ratcheted down until NUMA 5 changed from `{80,81}` to `{81}`.

The post-allocation bulkhead validator then rejected the candidate:

```text
bulkhead hard-partition reclaim NUMA 5 has 1 CPUs, minimum is 2
```

This failure happens while building the desired view, before topology apply,
final snapshot construction, or RDT CPUList reconciliation.

## Required Semantics

Hard partition is active when:

```text
EnableReclaim && EnableRampUpReclaimHardPartition
```

While active:

```text
floorSize(numa) >= 2
HardReclaimFloor subset-of candidate ReclaimRaw
HardReclaimFloor disjoint-from every non-reclaim owner
```

The floor target remains:

```text
max(
  configured minimum reclaim quantity,
  InitialRampUpReclaimCPUSetRatio * eligible CPUs,
  2 * physical NUMA count,
)
```

The selected CPU IDs remain deterministic:

1. exact configured reserved-reclaimed CPUs;
2. CPUs already in the live reclaim pool;
3. other eligible CPUs selected by topology preference.

The accepted capacity cost is that the floor remains reserved even when no
allocation is marked as ramp-up.

This historical design was replaced because it contradicted the active-ramp-up
scope in `2026-08-04-ramp-up-reclaim-floor-all-qos-design.md`. The selection
and cross-QoS rules remain valid only while runtime ramp-up activation is true.

## Allocation Flow

`adjustPoolsAndIsolatedEntriesWithRampUpFloorAtRevision` must establish the
floor before allocating any ordinary pool:

1. Clone a caller-provided explicit floor when present.
2. The historical implementation derived the node-level floor whenever the
   feature was configured; the replacement design gates this step on runtime
   ramp-up activation.
3. Remove the floor from `availableCPUs`.
4. Allocate dedicated, shared, shared-NUMA-binding, isolation, and system pools
   from the remaining CPUs.
5. Union the floor into the candidate reclaim pool.
6. Validate candidate ownership and per-NUMA balance before state commit.

The existing scan for any `AllocationInfo.RampUp` is removed from floor
activation. `RampUp` remains workload metadata but no longer controls the
node-level ownership invariant.

## Candidate Validation

Before checkpoint mutation or cgroup writes, validate:

```text
floor subset-of ReclaimRaw
ReclaimRaw disjoint-from Reserve, SharePool, Dedicated, and Isolation
ReclaimEffectivePerNUMA(numa) >= 2
max(perNUMACount) - min(perNUMACount) <= 1
```

The existing `BuildValidatedCPUSetPartitionView` check remains enabled whenever
hard partition is active. It is defense in depth; the normal allocator must
already have produced a valid candidate.

If floor derivation or candidate validation fails:

- reject the candidate transaction;
- do not mutate QRM checkpoint state;
- do not execute cpuset or RDT writes;
- keep the previous applied partition authoritative;
- return an error naming the offending NUMA, required floor, available
  capacity, and conflicting owners.

Do not retry without the floor and do not silently disable hard partition.

## Configuration Precedence

Runtime behavior must be diagnosed from the effective dynamic configuration,
not only process environment variables. E2E reset and target checks must compare
the checkpoint values with the intended mode and fail before workload creation
when they differ.

This design does not change configuration precedence. It makes the allocation
path safe for any valid effective configuration, including:

```text
DisableSharedCoresRampUp=true
EnableRampUpReclaimHardPartition=true
```

## Compatibility

- No protobuf or checkpoint schema change.
- No behavior change when reclaim or hard partition is disabled.
- `DisableSharedCoresRampUp` continues to control per-container ramp-up
  metadata and pooled CPU behavior.
- The hard reclaim floor remains active independently.
- AppliedView ownership restoration and RDT CPUList behavior are unchanged.

## Tests

### Unit

- Hard partition enabled with no `RampUp=true` entries still derives a floor.
- Eight NUMAs retain at least two reclaim CPUs each.
- `DisableSharedCoresRampUp=true` does not disable the floor.
- A caller-provided floor remains authoritative.
- Every non-reclaim pool is disjoint from the floor.
- Insufficient per-NUMA capacity fails before state mutation.
- Hard partition disabled preserves existing allocation behavior.

### Regression

Reproduce the incident:

1. Use eight NUMAs and an initial reclaim pool with nine CPUs per NUMA.
2. Enable hard partition and disable shared-core ramp-up.
3. Add four one-CPU shared pools.
4. Add four one-CPU shared-NUMA-binding workloads whose pools use two logical
   CPUs on NUMAs 2 through 5.
5. Recalculate all pools after each admission.

Expected result:

```text
every admission succeeds
every reclaim NUMA retains at least two CPUs
no reclaim ratchet occurs
candidate validation passes before commit
```

### Integration

- Effective checkpoint configuration is verified before workload creation.
- Standard E2E completes three rounds.
- No `hard-partition reclaim NUMA ... minimum is 2` error occurs.
- File-level cpuset, sched-domain, and schedstat checks remain `OK`.
- Final reset validates the effective dynamic configuration, not only env.
