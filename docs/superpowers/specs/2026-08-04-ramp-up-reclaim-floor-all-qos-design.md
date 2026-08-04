# Ramp-up Reclaim Floor for All QoS Design

## Semantics

`InitialRampUpReclaimCPUSetRatio` is a node-level ramp-up hard-partition
setting shared by all ramp-up workloads.

- `ratio=0` uses only `reservedReclaimedCPUSet`, distributed across every
  reclaim NUMA. It does not synthesize an extra one-core floor.
- `ratio>0` uses the larger of the reserved-reclaimed floor and the
  SMT-aligned ratio target.
- The ratio is independent of a Pod's reclaim annotation.

## Scope

When hard partitioning is enabled and a ramp-up allocation is entering or
already checkpointed, derive one deterministic floor for every machine NUMA
that participates in the reclaim pool. Prefer CPUs already in the current
reclaim allocation, then fill by topology. With no active ramp-up workload,
the floor is empty and does not reserve steady-state capacity.

The same floor is excluded from:

- initial shared-core ramp-up pooled CPUs;
- assembler recomputed `rampUpCPUs`;
- advisor recomputed `rampUpCPUs`;
- dedicated and shared-NUMA-binding allocation candidates.

The floor is derived from checkpointed allocation state and topology. The
selected floor itself is not persisted.

## View invariant

The existing ownership formula remains unchanged:

```text
ReclaimEffective =
  (Machine - NonReclaimPool - Reserve) intersect ReclaimRaw
```

Every adjustment verifies:

```text
RampUpReclaimFloor subset-of ReclaimEffective
```

The view must fail closed if SysAdvisor or an ownership pool removes any floor
CPU.

## Regression

The two-NUMA regression uses:

```text
ratio=0
reservedReclaimedCPUsSize=4
shared ramp_up=true
dedicated hint=[0]
```

Both NUMAs retain a reserved-reclaimed floor. No shared or dedicated ramp-up
allocation contains floor CPUs, and neither reclaim NUMA target is empty.
