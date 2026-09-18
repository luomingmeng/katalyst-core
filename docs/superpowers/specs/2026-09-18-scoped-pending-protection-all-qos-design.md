# Scoped Pending Protection for All QoS Classes

## Status

Approved for implementation on 2026-09-18.

## Context

The frozen ParentSafe admission path times out on a cold hierarchy before any
physical cgroup write. Live evidence from `fdbd-dc02-27-49--14` shows the same
first-round deadline signature for ordinary shared-core, shared-core NUMA
binding (SNB), and a later two-CPU pending admission:

```text
rounds=1
atoms=0
protected_rels=0
attempted=0
applied=0
```

Four ordinary shared-core admissions with 80 pending CPUs and four SNB
admissions with 40 pending CPUs each spent approximately 5.3 to 5.6 seconds
after the deadlock probe and then exceeded the ParentSafe deadline. A two-CPU
pending admission also failed while the hierarchy was cold. Once a no-pending
round applied 193 hierarchy writes, an equivalent two-CPU pending admission
reached ParentSafe in 326 milliseconds and fully converged in the next pass.

The failure is therefore not SNB-specific. It is a common unresolved-pending
admission defect whose manifestation depends on hierarchy state.

## First-Principles Invariants

The admission path must satisfy all of the following:

1. Every pending CPU is contained by every controlled primary ancestor of the
   future Pod cgroup.
2. No pending CPU is contained by the final reclaim domain.
3. Only operations required to prove ParentSafe may execute during admission.
4. Every forward write and the complete inverse rollback prefix must be
   reserved before the first physical write.
5. The execution ticket must bind one strictly ordered frozen trace.
6. Missing scope, stale identity, incomplete snapshots, projection cycles, and
   budget exhaustion must fail closed.
7. Full convergence remains exact-match; ParentSafe remains the only path that
   may publish a proved safe superset.
8. Admission cost must scale with the scoped safety closure, not with unrelated
   dynamic leaves in the complete snapshot.

Increasing the deadline, skipping proof, or accepting an incomplete snapshot
does not satisfy these invariants.

## Root Cause

The current input loses cgroup ownership when a Pod cgroup does not yet exist.
`PendingCPUSetUnion()` retains only a global CPU set. Target normalization then
adds that set to every primary DAG node. Admission splitting classifies a grow
as required whenever its added CPUs intersect the global pending set.

This creates two amplification effects:

1. Unrelated primary siblings and dynamic descendants enter the required
   admission closure.
2. Projected execution recomputes effective state, domain unions, and the full
   snapshot fingerprint after each operation.

The cold live hierarchy contained 194 snapshot entries and later required 193
physical writes to establish its baseline. The admission compiler attempted to
prove ParentSafe against this operation-heavy shape and exhausted the five
second context inside its first round.

The previously added all-protected transfer guard addresses a distinct
transfer-atom stall. It cannot match this failure because the live deadlock
analysis reports zero transfer atoms.

## Ownership Model

The cpuset-topology plugin owns the conversion from QRM pending allocation
state to a proved cgroup scope. The topology coordinator owns normalization,
planning, proof, ticket reservation, execution, and rollback over the validated
scoped input.

Introduce one canonical input:

```go
type PendingProtection struct {
    ScopeRel string
    CPUs     machine.CPUSet
    PodUID   string
    Source   PendingProtectionSource
}

type CoordinatorInput struct {
    PendingProtections []PendingProtection
}
```

The coordinator derives the union, per-rel requirements, and per-domain
protection from this input. Callers must not maintain independent global and
scoped pending representations.

## Scope Resolution

The plugin resolves each pending Pod in this order:

1. Use the existing Pod relative cgroup path when it is available.
2. Otherwise ask `pkg/util/cgroup/common` for every pure Pod-path candidate
   implied by its configured Kubernetes roots. Candidate generation performs no
   filesystem lookup.
3. After `BuildDAG`, select the unique candidate whose controlled-primary
   ancestor is deepest.
4. Fail closed when no candidate matches or distinct candidates tie at the
   deepest ancestor depth.

The fallback must not hard-code one cgroup layout inside the planner. It must
use a resolver at the plugin boundary so cgroupfs and systemd naming remain
separate from topology semantics.

An unresolved scope must fail closed. The old fallback that widens every
primary node is deleted rather than retained as a compatibility path.

## Scoped Target Normalization

For each pending protection, only controlled ancestors of `ScopeRel` receive
the protected CPUs:

```go
for _, protection := range protections {
    for _, node := range primaryNodes {
        if isRelAtOrUnder(protection.ScopeRel, node.Rel) {
            effective[node.Rel] =
                effective[node.Rel].Union(protection.CPUs)
        }
    }
}
```

Primary siblings, historical Pod cgroups, and unrelated container leaves keep
their canonical targets.

The coordinator constructs `pendingRequiredByRel` by walking the controlled
ancestor chain for every scope. Multiple pending Pods merge only where their
ancestor chains overlap.

## Admission Closure

`SplitPlanForAdmission` must stop using global CPU intersection as the owner of
required grows. A grow is required only when it repairs a deficit in
`pendingRequiredByRel` or is an ordered dependency of another required
operation.

The required closure contains:

- scoped ancestor grows;
- source drains required to preserve primary/reclaim disjointness;
- required hard-floor repairs;
- ancestor dependencies of required child operations;
- ordered predecessors required by the same-rel transition.

Unrelated sibling updates, historical dynamic-leaf cleanup, and exact
full-convergence work remain deferred.

The frozen ticket contains only the complete ordered ParentSafe closure and its
complete inverse rollback. Deferred work is replanned from a fresh snapshot by
the periodical full-convergence path.

## ParentSafe Proof

Extend `ParentSafetyReport` with scoped deficits:

```go
type ParentSafetyReport struct {
    PendingScopeDeficit map[string]machine.CPUSet
}
```

ParentSafe publication requires:

1. every pending CPU belongs to the primary domain;
2. no pending CPU belongs to the reclaim domain;
3. every controlled ancestor of every pending scope contains its protected
   CPUs;
4. every required floor is satisfied;
5. primary and reclaim domains remain disjoint;
6. every non-deferred required relation has converged.

Full convergence continues to require exact targets. The safe-superset
exception remains limited to ParentSafe finalization and does not weaken this
scoped proof.

## Projection Cost

Projected hierarchy execution currently rebuilds evidence after each
operation. Change the projected session to settle evidence once per independent
frontier:

1. Validate and apply configured-state changes in frozen order.
2. Recompute every affected effective subtree.
3. Rebuild domain unions and the snapshot fingerprint once after the frontier.
4. Expose a settled snapshot before planning the next frontier.

A compiled frontier must not contain an ancestor and descendant that depend on
each other's intermediate effective state. Compilation fails closed if this
independence contract is violated.

This changes the dominant cost from per-operation full-snapshot rebuilding to
per-frontier rebuilding while preserving ordered operation semantics.

## Deterministic Progress Guards

Add internal projection guards:

- repeated `(SnapshotID, PlanID)` returns `ProjectedPhaseCycleError`;
- a non-empty frontier whose settled snapshot ID does not change returns
  `ProjectedPhaseNoProgressError`;
- `Applied` must equal the frozen frontier operation count;
- projected operation and frontier counts consume explicit compile budgets;
- every rebase records the remaining operation count and plan ID.

Context deadline remains the final safety net, not the normal way to detect a
cycle or exhausted projected budget.

Preserve the internal cause when a deadline occurs:

```go
type AdmissionDeadlineError struct {
    Stage               FixedPointStage
    Round               int
    SnapshotID          SnapshotID
    RequiredOperations  int
    DeferredOperations  int
    ProjectedOperations int
    Frontiers           int
    Cause               error
}
```

`errors.Is(err, context.DeadlineExceeded)` must remain true.

## Verification Matrix

### QoS and Allocation Modes

| Workload | Cold admission | Materialized leaf | Retry after rollback | Full convergence |
| --- | --- | --- | --- | --- |
| ordinary `shared_cores` | required | required | required | required |
| SNB `shared_cores` | required | required | required | required |
| dedicated/DNB | required | required | required | required |
| reclaimed QoS interactions | disjointness proof | exact leaf | rollback proof | exact-match |

### Scope Cases

| Case | Expected result |
| --- | --- |
| existing Pod cgroup | exact Pod scope |
| missing Burstable Pod cgroup | scoped QoS ancestor |
| missing BestEffort Pod cgroup | scoped QoS ancestor |
| missing Guaranteed Pod cgroup | scoped primary ancestor |
| multiple QoS scopes | independent ancestor closures |
| unknown cgroup layout or missing Pod proof | fail closed |

### Required RED Tests

1. A missing Burstable Pod widens only its controlled ancestor chain.
2. Unrelated primary siblings and historical leaves do not enter the required
   trace.
3. Unknown pending scope returns a deterministic fail-closed error.
4. Multiple scopes merge only on shared ancestors.
5. Any scoped ancestor deficit prevents ParentSafe publication.
6. A 194-entry live-shaped snapshot produces a required trace bounded by the
   scoped ancestor closure rather than total leaf count.
7. Projected evidence is rebuilt once per frontier.
8. A repeated `(SnapshotID, PlanID)` returns a cycle error.
9. A non-empty no-effect frontier returns a no-progress error.
10. Forward and full inverse rollback costs remain reserved.

### Performance Gate

Add an explicit scale case where operation count approaches node count. Verify
linear memory and allocation growth, and compare 100, 1,000, and 10,000
relations. Keep the high-cost gate behind:

```bash
KATALYST_TOPOLOGY_SCALE_TEST=1
```

The default package tests must remain fast.

### Runtime Validation

1. Run topology, cpusettopology, dynamicpolicy, and race regression tests.
2. Build natively on Linux amd64 with Go 1.18.10 and CGO.
3. Verify the deployed executable SHA on QRM and SysAdvisor.
4. Run one cold ordinary shared admission.
5. Run one cold SNB admission.
6. Run one cold dedicated/DNB admission.
7. Run the standard three-round E2E suite.
8. Run five high-churn rounds.
9. Run final reset and verify QRM/SysAdvisor health.

## Acceptance Criteria

- Cold ordinary shared, SNB, and DNB admissions do not reach the five-second
  ParentSafe deadline.
- Required operation count is bounded by scoped ancestors plus hard-floor
  repair, not by unrelated snapshot leaves.
- Every scoped ancestor contains its pending CPUs before publication.
- Pending CPUs remain disjoint from reclaim.
- Frozen tickets cover every forward write and the complete inverse rollback.
- Operation-heavy scale results remain linear.
- Standard and high-churn E2E suites pass.
- Final reset succeeds and both agents report all health checks ready.

## Retirement

Delete the following old ownership paths in the same implementation series:

- unscoped pending target widening;
- global CPU-intersection ownership of required grows;
- any compatibility fallback that widens all primary nodes when scope proof is
  absent.

The existing all-protected transfer guard remains because it proves a separate
transfer-atom failure class. It must not become a fallback for unresolved
pending scope.
