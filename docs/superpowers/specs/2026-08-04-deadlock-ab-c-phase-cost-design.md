# Deadlock A-B-C Phase Cost Design

## Goal

Reproduce the standard E2E transition from an initial applied partition A,
through a transient-protected applied partition B, to final desired partition
C. Identify the dominant deadlock context cost before changing projection
algorithms, then optimize one cost source at a time.

## Verified reserve state

The node reserves two logical CPUs:

```text
configured count: 2
selected CPUs: 0,24
state reserve allocation_result: 0,24
```

Bulkhead reads the reserve pool allocation from QRM state. No reserve
production behavior changes are required. Tests and diagnostics must assert
both the set and its cardinality.

## E2E transition

The fixture models:

```text
entries=262
child_edges=249
transfer_atoms=23
protected_rels=0
protected_pending_cpus=0
reserve=0,24
```

Partition B and C differ by exactly 23 CPUs. Dynamic descendants repeat
transfer CPUs across multiple hierarchy levels and are not inserted into
`DynamicByRel`, so static-required pruning cannot hide the E2E cost.

## Phase accounting

`DeadlockProbeStats` records:

```text
BaseOperations
RelIndexOperations
ChildIndexOperations
FrontierIndexOperations
AncestorClosureOperations
AtomOperations
ChildMembershipsScanned
FrontierMembershipsScanned
```

Every charged context operation increments exactly one logical phase counter.
Their sum equals `ContextOperations`; membership scan counts are performance
diagnostics and do not consume the logical rel/edge budget.

## Conditional optimization

The first RED test determines the dominant phase.

The RED fixture proved that membership accounting dominated context cost.
Both child-count and ownership-frontier indexes remain eager because they keep
per-atom work small. Each child edge and each rel frontier is charged once,
while the exact number of CPU memberships scanned is reported separately.

The full fixture still requires 9008 logical operations because all 23 atoms
classify as `v1_empty`. Default budgets therefore auto-scale per invocation:

```text
required = 4096 + 3*(entries+edges) + atoms*(entries+edges+1)
```

Only default/zero budgets auto-scale. Explicit limits remain hard. A shared
tracker grows to `current usage + required` on each coordinator round.

## Correctness tests

- The 262/249/23 fixture reproduces `AtomIndex=-1` before accounting and
  budget changes.
- Phase counters sum exactly to context operations.
- The final fixture completes all 23 atoms with `probe_operations=9008` and
  `probe_limit=17405`.
- Two rounds sharing one tracker complete with usage 18016 and limit 26413.
- Every atom projection equals canonical full projection.
- A→B→C lifecycle ends with desired, applied, and observed equal to C.
- A topology plan failure leaves applied and observed at B without partial
  writes.
- Reserve remains `0,24` with cardinality two.
