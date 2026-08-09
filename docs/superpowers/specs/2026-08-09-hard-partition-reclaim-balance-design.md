# Hard-Partition Reclaim Balance Design

## Goal

When `EnableRampUpReclaimHardPartition=true`, guarantee that every eligible
NUMA receives at least two logical reclaim CPUs and that reclaim quantities are
distributed as evenly as possible across eligible NUMAs.

No new configuration or feature gate is introduced.

## Existing Behavior

Sysadvisor already calculates a per-NUMA reserve map through
`machine.GetCoreNumReservedForReclaim`. The current minimum is one logical CPU
per NUMA:

```text
numReservedCores >= number of NUMAs
```

The non-NUMA-binding assembler aggregates this map into one FakedNUMA reclaim
quantity. QRM therefore receives the correct global minimum quantity but loses
the original per-NUMA placement intent.

For a two-NUMA, four-CPU reclaim allocation, QRM can currently materialize all
four CPUs on one NUMA.

## Required Semantics

### Hard Partition Disabled

Keep all existing behavior:

- Existing `ReservedReclaimedCoresSize` minimum.
- FakedNUMA reclaim can use global placement.
- No per-NUMA minimum or balance validation is added.

### Hard Partition Enabled

For eligible NUMAs `N` and reclaim quantity `Q`:

```text
Q >= 2 * len(N)
reclaimSize(numa) >= 2 for every numa in N
max(reclaimSize) - min(reclaimSize) <= 1
sum(reclaimSize) = Q
reclaimCPUs(numa) is a subset of physicalCPUs(numa)
```

The concrete CPU IDs may vary according to availability and stable preference.

## Sysadvisor Change

In the hard-partition branch of `getReservedForReclaim`, calculate:

```go
ratioReserved := int(
    float64(totalAvailable) *
        InitialRampUpReclaimCPUSetRatio,
)

minimumReserved := len(numaAvailable) * 2
numReservedCores := max(ratioReserved, minimumReserved)
```

Then continue using:

```go
machine.GetCoreNumReservedForReclaim(
    numReservedCores,
    len(numaAvailable),
)
```

The generic utility remains unchanged. The minimum of two logical CPUs per
NUMA applies only when hard partition is enabled.

If any NUMA has fewer than two available logical CPUs, Sysadvisor must return a
capacity error instead of producing an unsatisfiable advice result.

## QRM Change

### Eligible NUMAs

For a FakedNUMA reclaim block, derive eligible NUMAs from the block's eligible
CPUSet:

```text
eligibleNUMAs =
    online NUMAs whose physical CPUSet intersects descriptor.Eligible
```

NUMAs already represented by fixed real-NUMA reclaim blocks remain part of the
final balance calculation, but their fixed allocations are assigned first.

### Quantity Distribution

Distribute global reclaim quantity across eligible NUMAs:

```text
base      = quantity / numaCount
remainder = quantity % numaCount
```

NUMAs are sorted by ID. The first `remainder` NUMAs receive `base+1`; the
remaining NUMAs receive `base`.

For mixed real-NUMA and FakedNUMA blocks:

1. Allocate real-NUMA blocks first.
2. Track current reclaim size for every eligible NUMA.
3. Allocate each residual FakedNUMA CPU to the currently smallest NUMA.
4. Break ties by NUMA ID.
5. Respect each NUMA's eligible CPUSet and available capacity.

The final difference between the largest and smallest reclaim allocation must
not exceed one.

### CPU Selection

Within a NUMA, select concrete CPUs in this order:

1. Previous reclaim CPUs within that NUMA.
2. Other available eligible CPUs within that NUMA.

Sticky preference chooses CPU IDs only. It never changes the required quantity
for a NUMA.

### Allocation Order

Hard-partition reclaim is mandatory and must be allocated before ordinary
shared FakedNUMA blocks.

The DD/disjoint planner path handles mandatory reclaim in its core phase. The
legacy path must add an equivalent hard-partition branch so that
`EnableRampUpReclaimHardPartition=true` has the same semantics regardless of
the dedicated-overlap setting.

## Validation

Before QRM state commit:

- FakedNUMA reclaim quantity must be at least `2 * eligibleNUMACount`.
- Every eligible NUMA must have at least two reclaim CPUs.
- Per-NUMA sizes must differ by no more than one.
- Every per-NUMA reclaim CPUSet must be contained in the physical NUMA CPUSet.
- Reserve, dedicated, share, and reclaim disjointness rules must still hold.

Validation failure causes:

```text
zero QRM state mutation
zero cgroup writes
previous applied partition remains authoritative
health reports the precise hard-partition violation
```

## Bulkhead Defense

When hard partition is enabled, `BuildCPUSetPartitionView` validates that every
eligible NUMA has a non-empty reclaim target with at least two logical CPUs and
that the distribution is balanced.

This is a defense-in-depth check. The normal path is guaranteed by QRM concrete
allocation before state commit.

The existing cgroup v1 empty-target fallback remains a separate compatibility
and recovery concern. It must not be used to compensate for an invalid
hard-partition state.

## Compatibility

- No behavior change when hard partition is disabled.
- No protobuf change.
- No new feature gate.
- Existing dedicated-reclaim disjoint negotiation remains unchanged.
- Existing FakedNUMA response format remains valid.
- QRM becomes responsible for recovering the per-NUMA intent from the global
  quantity when hard partition is enabled.

## Tests

### Sysadvisor

- Hard partition disabled preserves the existing minimum.
- Two NUMAs and a ratio-derived value below four produce `2/2`.
- Four NUMAs and a ratio-derived value below eight produce `2/2/2/2`.
- A ratio-derived value above the minimum retains the existing
  `GetCoreNumReservedForReclaim` rounding behavior while keeping every NUMA at
  or above two logical CPUs.
- A NUMA with fewer than two available logical CPUs returns a capacity error.

### QRM

- Four CPUs over two NUMAs produce `2/2`.
- Five CPUs over two NUMAs produce `3/2`.
- Eight CPUs over four NUMAs produce `2/2/2/2`.
- A previous reclaim allocation concentrated on one NUMA is rebalanced.
- Quantity below `2 * NUMACount` is rejected before state commit.
- Insufficient capacity on one NUMA is rejected.
- Mixed real-NUMA and FakedNUMA blocks remain balanced.
- Map order and block ID changes do not alter the concrete partition.
- Hard partition disabled preserves current global behavior.

### Integration

- `applyBlocks` produces non-empty balanced
  `TopologyAwareAssignments` for every eligible NUMA.
- `BuildCPUSetPartitionView` produces balanced non-empty
  `ReclaimEffectivePerNUMA`.
- Standard E2E runs three rounds.
- High-churn E2E runs five rounds.
- Agent restart preserves the distribution.
- Final reset succeeds.
