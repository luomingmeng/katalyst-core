# Solver Budget and Container Lifecycle Proof Design

## Scope

This design eliminates avoidable Allocate failures from two independent
production failure classes:

1. hard-reclaim replacement incorrectly exhausts a per-graph edge budget
   across multiple residual candidates;
2. cpuset-topology finalization re-resolves raw desired containers instead of
   consuming the lifecycle decision made from one strict fresh Pod snapshot.

The design also closes the stale-plan recompile and advisor-target liveness
gaps that would otherwise translate the same underlying races into
`Allocate failed`.

It does not promise that the generic `alloc_failed` metric becomes zero.
Requests that exceed real capacity, violate donor floors, or have no valid
whole-core solution must continue to fail closed with a typed capacity or
constraint error. The target outcome is that maintenance convergence,
container churn, and internal search accounting never cause an otherwise
feasible Allocate to fail.

The change does not increase allocation timeouts, add fixed sleeps, relax
whole-core or NUMA invariants, or change feature-gate semantics.

## Solver Budget Ownership

### Problem

`partitionSolverBudget` currently owns both assignment-edge accounting and
flow-operation accounting. The replacement solver reuses one instance across
all terminal candidates.

Assignment edges represent the memory footprint of one eligibility graph.
Accumulating them across sequential graphs changes a per-graph capacity limit
into an unrelated whole-search work limit. A set of individually valid graphs
can therefore fail with `errPartitionAssignmentEdgeBudget`.

Flow operations represent cumulative search work and must remain shared across
all candidates.

### Interfaces

Replace the mixed budget with two explicit owners:

```go
type partitionGraphBudget struct {
	maxAssignmentEdges int
	assignmentEdges    int
}

type partitionSearchBudget struct {
	maxFlowOperations int
	flowOperations    int
}
```

The partition solver receives both:

```go
func solveDisjointPartitionsWithBudgets(
	demands []partitionDemand,
	topology *machine.CPUTopology,
	graphBudget *partitionGraphBudget,
	searchBudget *partitionSearchBudget,
) (map[string]machine.CPUSet, error)
```

Each residual candidate gets a new graph budget. The replacement invocation
shares one search budget.

### Residual Solve Cache

The replacement invocation owns one in-memory cache:

```go
type partitionResidualSolveCache struct {
	entries map[partitionResidualSignature]partitionResidualSolveResult
}
```

`partitionResidualSignature` is a canonical, order-independent encoding of
every solver input that can affect the assignment:

- demand key and request-group key;
- quantity and request quantity;
- advisor block class;
- eligible and preferred CPU sets;
- topology identity relevant to cost and locality.

Demand records are sorted before encoding. CPU sets use their canonical string
form. The cache exists only for one replacement invocation and is never
persisted.

Cache entries contain deep-copied assignments and one of:

- successful deterministic assignment;
- semantic infeasibility.

Context cancellation, graph-budget failure, flow-budget failure, malformed
input, and internal errors are never cached. A cache hit consumes no graph
edges or flow operations because it performs no graph build or flow solve.

Different replacement states may reuse a cached assignment, but each state
still builds and validates its own replacement proof. The cache does not own
proof selection or result ordering.

### Invariants

- Assignment edges are counted only for the graph currently being built.
- A single graph exceeding `maxAssignmentEdges` fails closed with
  `errPartitionAssignmentEdgeBudget`.
- Sequential graphs do not inherit assignment-edge counts.
- Flow operations accumulate across the entire replacement search.
- Exceeding `maxFlowOperations` fails with
  `errPartitionFlowOperationBudget`.
- Equivalent residual-demand graphs are solved once per replacement
  invocation.
- Cache keys and returned assignments are independent of map iteration order.
- Candidate ordering and output remain deterministic.
- Budget failure never changes canonical state, stages an advisor WAL, or
  creates an advisor post-commit target.

### Search Completion

Separating the graph budget fixes the observed false edge-budget exhaustion,
but does not by itself prove that the search completes before the global flow,
candidate-state, or terminal-solve limits.

The implementation therefore records, for every replacement invocation:

- generated and deduplicated candidate states;
- terminal states;
- residual cache hits and misses;
- maximum edges in one graph;
- cumulative flow operations;
- retained reclaim CPUs and donor groups touched by the selected result.

The implementation may prune a candidate only when the current best result is
provably no worse under the complete ordering used by
`hardReclaimReplacementResultLess`. A heuristic lower bound must not discard a
candidate.

If no candidate succeeds, the solver returns one of two typed outcomes:

- `hardReclaimNoFeasibleReplacement`: all candidates were evaluated and
  violated a semantic constraint;
- `hardReclaimSearchBudgetExceeded`: completeness was not proven because a
  search budget was exhausted.

These outcomes must not be conflated.

### Compatibility

The public policy behavior and existing error values remain unchanged. Tests
and internal helper call sites migrate to the separated budget interface.

No limit is increased. If a future cumulative graph-build work limit is
needed, it must use a separate counter whose name and error describe search
work rather than graph memory.

## Container Lifecycle Proof

### Problem

`buildExpectedCPUSetByRel` already classifies containers using:

- a cache-only snapshot;
- one strict fresh Pod snapshot;
- typed cgroup-path absence;
- fresh Pod Spec ownership;
- old and current container generation identity.

Finalization discards this result. It iterates
`DesiredView.ContainerCPUSetByPod`, queries MetaServer again, and resolves each
container path again. A container retired during the build phase can therefore
reappear as a hard `ENOENT` failure while deriving the final applied view.

Adding `os.ErrNotExist` to a generic absence predicate is insufficient. Leaf
absence has different meanings depending on whether the fresh Pod Spec still
owns the container name.

### Lifecycle Model

Introduce an internal immutable proof:

```go
type containerLifecycleState string

const (
	containerLifecycleResolved containerLifecycleState = "resolved"
	containerLifecyclePending  containerLifecycleState = "pending"
	containerLifecycleRetired  containerLifecycleState = "retired"
)

type containerLifecycleProof struct {
	PodUID        string
	ContainerName string
	ContainerID   string
	RelativePath  string
	DesiredCPUSet machine.CPUSet
	State         containerLifecycleState
}
```

The exact field representation may use existing internal key types, but it
must preserve the same identity and ownership information. The proof is owned
by the expected-view compiler and is not persisted in the QRM checkpoint.

Proofs are carried in:

```go
type containerLifecycleProofSet struct {
	SourceSnapshotDigest string
	OrderedProofs        []containerLifecycleProof
}
```

`SourceSnapshotDigest` canonically hashes the involved Pod UIDs, owned
container names, and current container IDs from the one strict fresh snapshot
used by the compiler. It is diagnostic evidence and a whole-snapshot drift
fence, not a replacement for per-proof validation.

### State Semantics

`resolved`

- The strict fresh Pod snapshot still owns the container name.
- The selected container generation and relative path are known.
- Finalization reads the final leaf CPUSet from the frozen DAG snapshot.

`pending`

- The strict fresh Pod snapshot owns the container name.
- The selected generation has not materialized a valid leaf yet.
- The generation remains part of deferred protection.
- Finalization does not publish a fabricated container leaf.

`retired`

- The strict fresh Pod snapshot no longer owns the container name, or the
  whole Pod outcome is stale under the existing typed-absence rules.
- Finalization omits the entry and must not resolve its path again.

### Data Flow

`buildExpectedCPUSetByRel` returns a result object containing:

- expected CPUSet by relative path;
- deferred leaf protection;
- ordered container lifecycle proofs.

The topology transaction carries the same result through compile, execute, and
finalization. `containerCPUSetByPodFromFinalSnapshotWithDeferredCleanup`
consumes lifecycle proofs instead of raw
`DesiredView.ContainerCPUSetByPod`.

The proof list is ordered by Pod UID, container name, and generation identity
before finalization so map iteration cannot affect output or errors.

After physical convergence, finalization obtains exactly one new strict fresh
Pod snapshot for all involved proofs. A validator compares that snapshot with
the frozen proof set. The validator can only answer whether each proof remains
valid; it cannot independently reclassify containers. Any new lifecycle state
requires recompilation by the expected-view compiler.

### Finalization Rules

- A retired proof is omitted when the post-execution snapshot confirms that
  the owner is still absent.
- A pending proof remains deferred and is absent from the applied container
  view when its owner and generation are unchanged. Finalization does not
  probe whether its leaf materialized; the next periodic compile promotes it
  to resolved.
- A resolved proof must retain the same fresh owner and container ID, and map
  to the expected final DAG leaf.
- Owner-set, Pod-generation, or container-ID drift invalidates the proof set.
- If a resolved leaf disappears after compilation, or a retired owner
  reappears, finalization returns the typed stale-plan result.
- A pending proof whose container ID changes, including an absent ID becoming
  present, returns stale so the next compile can select the new generation.
- Permission, I/O, malformed-path, and transport failures remain hard errors.
- Finalization never reclassifies container ownership independently.

The caller recompiles a stale plan from a newer strict fresh snapshot only
when the topology coordinator explicitly authorizes replan. It must not retry
the same frozen proof indefinitely.

## Replan Disposition

### Problem

`CPUSetAdjustmentHandler` currently returns every coordinator error to the
caller. Changing finalization errors to `PlanStaleError` would improve
classification but still fail Allocate.

`errors.Is(err, topology.ErrCoordinatorPlanStale)` is not a sufficient retry
condition. Existing rollback errors preserve the stale error chain even when
rollback failed.

### Interface

Topology convergence returns an explicit disposition:

```go
type ReplanDisposition string

const (
	ReplanNotAllowed                  ReplanDisposition = "not_allowed"
	ReplanSafeNoPhysicalWrites        ReplanDisposition = "safe_no_physical_writes"
	ReplanSafeAfterVerifiedRollback   ReplanDisposition = "safe_after_verified_rollback"
	ReplanSafeFromVerifiedFinalState  ReplanDisposition = "safe_from_verified_final_state"
)
```

The disposition belongs to `topology.ConvergenceResult`, whose coordinator is
the only owner able to prove physical-write and rollback state.

- `SafeNoPhysicalWrites` means stale detection occurred before any physical
  mutation.
- `SafeAfterVerifiedRollback` means all physical mutations were reverted and
  the rollback snapshot was independently verified.
- `SafeFromVerifiedFinalState` means convergence reached a complete, current
  final physical snapshot, but publication proof validation detected a
  lifecycle change. The next compile uses that verified final state as its
  baseline.
- Any partial write, failed rollback, incomplete final snapshot, hard I/O
  error, or unknown state is `NotAllowed`.

Error wrapping must preserve the original stale cause, but callers make retry
decisions only from `ReplanDisposition`.

### Recompile Owner

`CPUSetTopologyPlugin` owns the recompile loop because it owns expected-view
compilation, DAG construction, lifecycle proofs, and final publication.

The current handler is split into:

```go
func (p *CPUSetTopologyPlugin) CPUSetAdjustmentHandler(...) error
func (p *CPUSetTopologyPlugin) adjustOnce(...) (ConvergenceResult, error)
```

The outer handler:

1. creates one monotonic adjustment budget;
2. invokes `adjustOnce`;
3. returns on success or `ReplanNotAllowed`;
4. recompiles immediately from a new strict fresh snapshot for a safe
   disposition;
5. stops when the original context deadline, write budget, or maximum replan
   count is exhausted.

There is no sleep and no reset of consumed budget between attempts. The
adjustment budget accounts for:

- replan attempts;
- physical writes;
- rollback writes;
- elapsed deadline;
- topology convergence rounds.

Exhaustion returns a typed
`topologyReplanBudgetExceeded` containing the last stale reason and cumulative
attempt evidence.

## Advisor Target Liveness

Solver and lifecycle fixes prevent the two observed triggers, but they do not
guarantee that a different physical apply failure cannot block Allocate behind
an advisor writer fence.

The advisor post-commit owner must therefore complete the existing handoff
design:

- writer wait includes a timer for
  `lastProgressAt + advisorPostCommitStuckThreshold`;
- retry and periodic reconcile autonomously invoke handoff after bounded
  retries or the stuck deadline;
- handoff uses the shared execution lease and revalidates target identity;
- response-owned effects are replayed before releasing the writer fence;
- replay failure keeps the fence and WAL fail closed;
- a new advisor frame may atomically supersede a durable target whose writer
  fence has been released;
- restart recovery uses durable revision relations, not in-memory age alone.

Allocate request contexts never own target recovery. They may observe and wait
for the owner, but recovery progress cannot depend on a new Allocate,
RemovePod, or residual-cleanup call arriving.

## Avoidable Allocate Failure Contract

For these failure classes, Allocate must have one of three outcomes:

1. the original adjustment succeeds;
2. a safely stale plan is recompiled within the original bounded budget and
   succeeds;
3. the request returns a typed, semantic capacity or constraint rejection that
   proves no feasible allocation exists.

Internal graph-accounting mistakes, stale container generations, recoverable
holder drift, and an abandoned writer fence are forbidden terminal outcomes.

The generic `alloc_failed` metric remains valid for outcome 3 and unrelated
request errors. A new low-cardinality `failure_class` distinguishes:

- `capacity_exhausted`;
- `constraint_unsatisfied`;
- `search_budget_exhausted`;
- `replan_budget_exhausted`;
- `post_commit_stalled`;
- `internal_error`.

The success criterion is zero avoidable failure classes, not suppression of
legitimate admission rejection.

### Ownership

The expected-view compiler is the only owner of container lifecycle
classification for one topology transaction.

MetaServer remains the source of Pod and container observations. The strict
fresh Pod snapshot remains the authority for container-name ownership. The
final DAG snapshot remains the authority for the physical CPUSet of a resolved
leaf.

### Ownership Retirement

This is an internal delete-first migration:

- the expected-view compiler becomes the only lifecycle classifier;
- finalization retires direct per-container MetaServer lookup and ownership
  classification;
- the post-execution validator only validates frozen proofs against one fresh
  snapshot;
- Allocate request contexts retire as advisor-target recovery triggers;
- retry and periodic reconciliation become the canonical handoff owner.

No external compatibility boundary or persistent checkpoint schema is changed.
The lifecycle proof and residual solve cache are transaction-local derived
state. The old finalization lookup path must be deleted after all internal
callers consume proofs; it must not remain as a fallback.

## Error Handling

The solver preserves the existing budget errors. New wrapping must retain
`errors.Is`.

Finalization uses a typed stale-plan error only for proof invalidation caused
by concurrent lifecycle change. The transaction remains fail closed for
unclassified errors.

Retired entries are not errors. Pending entries are not published as resolved.

Only the coordinator can authorize replan. A stale error with
`ReplanNotAllowed` is terminal for the current adjustment and schedules
latest-state repair without re-entering the physical writer.

## Test Design

### Solver RED Tests

1. Multiple candidates each remain below the edge limit, while their
   cumulative edge count exceeds it. The replacement must succeed.
2. One candidate graph exceeds the edge limit. The solver must return
   `errPartitionAssignmentEdgeBudget`.
3. Flow operations accumulate across candidates and still enforce the global
   limit.
4. Equivalent residual-demand signatures produce one cache miss followed by
   deterministic cache hits.
5. Budget, context, and internal failures are not cached.
6. A production-shaped fixture has multiple NUMA nodes and one complete-core
   deficit. The result must preserve quantity, NUMA locality, SMT closure, and
   determinism.
7. Captured fixtures from every affected production topology either produce a
   valid replacement or a complete no-feasible proof without exhausting a
   search budget.
8. Solver failure must leave revision, staging WAL, active WAL, and
   post-commit target unchanged.

### Lifecycle RED Tests

1. A fresh Pod Spec removes an old container before finalization. The retired
   entry must not be resolved again.
2. A fresh-owned resolved leaf disappears between compile and finalization.
   The result must be a typed stale-plan error.
3. A fresh-owned generation without a leaf remains pending and absent from the
   applied container view.
4. A retired stale Pod UID does not re-enter finalization through the raw
   desired view.
5. Admission does not fail because an unrelated retired leaf returns typed
   cgroup-path `ENOENT`.
6. Advisor post-commit apply does not mark retry for the same retired-leaf
   case.
7. Non-absence path errors remain hard failures.
8. Input map order does not change proof order or final output.
9. Container ID changes after compile while the old leaf remains present.
   Post-execution validation must return stale, not publish the old
   generation.
10. A pending generation keeps the same identity while its leaf materializes.
    Finalization keeps it deferred and performs no path probe; the next
    periodic compile promotes it.
11. A pending container ID changes after compile. The proof is invalidated and
    recompiled before publication.

### Replan RED Tests

1. Pre-write holder drift returns `ReplanSafeNoPhysicalWrites` and succeeds
   after one recompile.
2. Verified rollback returns `ReplanSafeAfterVerifiedRollback` and succeeds
   after one recompile.
3. Final publication drift with a complete final snapshot returns
   `ReplanSafeFromVerifiedFinalState`.
4. Rollback failure preserves the stale error but returns
   `ReplanNotAllowed`; the handler does not re-enter.
5. Replans share the original deadline and cumulative write budget.
6. Continuous churn exhausts the replan budget deterministically without an
   infinite loop or fixed sleep.

### Target Liveness RED Tests

1. A writer waiting before the stuck threshold is woken by the threshold
   timer and observes handoff without waiting for its context deadline.
2. Retry ownership hands off an aged physical target without a business
   request.
3. Replay failure preserves the writer fence and active WAL.
4. A new advisor frame supersedes a released target atomically.
5. Restart before handoff, after handoff, and after supersede preserves
   revision and WAL invariants.

## Commit Boundaries

1. `docs(qrm-cpu): design solver budget and lifecycle proof`
2. `fix(qrm-cpu): separate graph and search solver budgets`
3. `perf(qrm-cpu): cache equivalent replacement residual solves`
4. `fix(qrm-cpu): carry and validate lifecycle proofs`
5. `fix(qrm-cpu): safely recompile stale topology plans`
6. `fix(qrm-cpu): autonomously hand off stalled advisor targets`

The implementation commits include their own RED-first tests. Documentation
changes remain isolated from code and test changes.

## Acceptance Criteria

- Both new RED groups fail against the pre-change implementation.
- Captured affected-node fixtures do not terminate on graph, flow, candidate,
  or terminal search budgets.
- All dynamicpolicy and cpuset-topology tests pass after implementation.
- Focused race tests pass for proof validation, replan, execution lease, and
  advisor target supersede.
- `go vet`, `gofmt`, and `git diff --check` pass.
- The full dynamicpolicy package tree passes with the repository-required
  Mockey compiler flags.
- Standard three-round, high-churn five-round, and affected checkpoint replay
  validation pass before rollout.
- A canary on one affected p1 node and the affected p3 node runs for six hours
  with zero avoidable Allocate failure classes, healthy advisor communication,
  and monotonically advancing revision.
- Legitimate capacity and constraint rejection remains fail closed and is
  reported under its semantic failure class.
