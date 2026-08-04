# Deadlock Single-Atom Incremental Projection Design

## Goal

Reduce the reconstructed 96-CPU overlap-churn fixture from 16,320 deadlock
probe operations to fewer than the unchanged default budget of 4,096, without
changing drain targets, safe-seed selection, atom order, or fail-closed
behavior.

The incremental path is used only by deadlock probing. Phase planning and
cgroup mutation continue to use the complete canonical projection.

## Current Cost

Protected-rel preaggregation reduced a complete replay from 115,080 to 16,320
operations:

```text
preaggregation: 24
84 probeable atoms × (98 snapshot rels + 96 child edges): 16,296
total: 16,320
```

Twelve of the 96 transfer atoms are protected and skipped. Each of the
remaining 84 atoms still recomputes the complete snapshot even though its
drain batch contains one CPU.

## Semantic Constraint

An atom can affect more than the leaf that owns the CPU. A child target is
part of its parent's required set, so a change propagates through every
ancestor up to the snapshot root.

The incremental algorithm cannot use only a boolean domain-membership test.
`buildPhaseTransition` preserves the entire current CPUSet when a non-empty
target would otherwise become empty:

```go
if !AllowEmptyTarget && target.IsEmpty() && !Current.IsEmpty() {
    return Current.Clone(), nil
}
```

Reclaim bucket upper bounds and dynamic descendants add the same requirement:
incremental evaluation must construct the complete target for each affected
rel and call the canonical transition logic.

## Architecture

### Immutable context

Extend `drainProjectionContext` with data built once per deadlock analysis:

```go
type drainProjectionContext struct {
    protectedDescendantUnionByRel map[string]machine.CPUSet

    baseProjection        DrainProjection
    staticRequiredByRel   map[string]machine.CPUSet
    baseChildUnionByRel   map[string]machine.CPUSet
    baseChildCPUCount     map[string]map[int]int
    bucketUpperByRel      map[string]machine.CPUSet
    finalByRel            map[string]machine.CPUSet

    frontierRelsByCPU     map[int][]string
    ancestorsByRel        map[string][]string
    affectedRelsByCPU     map[int][]string
    parentByRel           map[string]string
    depthByRel            map[string]int
}
```

`baseProjection` is the canonical projection with an empty drain batch and the
same `LeavingByDomain`. It provides immutable targets for all unaffected rels.

`staticRequiredByRel` contains required CPUs that do not depend on child
targets:

- controlled desired CPUs;
- primary protected-pending CPUs;
- explicit dynamic CPUs;
- preaggregated protected-descendant CPUs.

`baseChildUnionByRel` and `baseChildCPUCount` allow one CPU to be removed from a
parent's child union only when no unchanged or changed child still requires it.
This avoids rescanning all siblings. The count is needed because CPUSet union
is not invertible.

### CPU index and affected closure

Indexing every rel that contains a CPU duplicates ownership at each hierarchy
level. On a 384-CPU snapshot, both a controlled root and its singleton leaves
can contain the same 384 CPUs, consuming the remaining budget before any atom
is evaluated.

The index therefore uses the minimum ownership frontier:

```text
frontier(rel) = current(rel) - union(current(direct children))
```

`frontierRelsByCPU[cpu]` contains rels whose frontier contains the CPU. A parent
that also contains the CPU is recovered through the ancestor closure rather
than indexed again. Overlapping siblings remain separate frontier owners;
deduplication happens when their ancestor closures are combined.

If a controlled root owns a CPU that no observed child owns, that CPU remains
in the root frontier and is indexed there. The optimization therefore does not
assume that ownership always terminates at a leaf.

`ancestorsByRel` is computed once per snapshot rel, with cycle detection. The
same closure is reused by every CPU owned by that rel; parent edges are not
re-walked per CPU.

For each CPU, `affectedRelsByCPU[cpu]` is the union of:

1. every rel in `frontierRelsByCPU[cpu]`;
2. the precomputed `ancestorsByRel` of each frontier rel.

The result is sorted by descending depth. Ancestors are included across domain
boundaries because a changed child target can affect a parent target and its
domain union. Duplicate rels are removed.

The context builder rejects parent cycles. Missing parent entries terminate the
closure at the observed snapshot boundary.

### Transfer CPU filtering

The transfer graph is built before the incremental context. Its atom union is
the complete set of CPUs that deadlock probing can remove:

```go
transferCPUs := machine.NewCPUSet()
for _, atom := range atoms {
    transferCPUs = transferCPUs.Union(atom.CPUs)
}
```

Context indexing receives `transferCPUs`. The base projection remains complete
and canonical, but index cardinality is restricted:

```text
relevant child target = child target ∩ transferCPUs
relevant frontier     = ownership frontier ∩ transferCPUs
```

`baseChildUnionByRel` remains the complete child union because per-rel
projection semantics require all required CPUs. Only `baseChildCountByRel`,
`frontierRelsByCPU`, and `affectedRelsByCPU` omit non-transfer CPUs.

This distinction is required by the 232-entry E2E snapshot. Indexing every CPU
on every child edge exhausts the 4096 budget before atom evaluation even though
only 16–20 CPUs appear in the transfer graph.

## Per-Atom Algorithm

The fast path accepts only a singleton `DrainAtom`. A multi-CPU atom or an
incomplete context falls back to `projectDrainTargets`.

For CPU `c`:

1. Load `affectedRelsByCPU[c]`.
2. Start with an empty overlay:

   ```go
   changedTargetByRel := map[string]CPUSetTarget{}
   changedEmptyBlockerByRel := map[string]machine.CPUSet{}
   ```

3. Visit affected rels from deepest to shallowest.
4. Start `required` from `staticRequiredByRel[rel]`.
5. Start the child-required set from `baseChildUnionByRel[rel]`.
6. For children changed earlier in this atom:
   - subtract their base membership contribution for `c`;
   - add their overlay membership contribution for `c`.
7. Remove `c` from the child union only when the resulting count is zero;
   otherwise retain it.
8. Union the adjusted child set into `required`.
9. Call the same per-rel projection helper used by the full projection:
   - reclaim bucket drain handling;
   - `buildPhaseTransition`;
   - non-empty fallback;
   - dynamic final target;
   - nearest bucket upper bound;
   - empty-blocker classification.
10. Store the complete target in the overlay only when it differs from
    `baseProjection.TargetByRel[rel]`.
11. Update domain-union membership for `c` from the overlay. All other CPUs and
    rels reuse `baseProjection`.

The deadlock probe only asks whether the source domain still contains `c`, but
the implementation returns a normal `DrainProjection` view so tests can
compare every affected target and empty blocker against the full oracle.

## Canonical Rel Helper

Extract the body of the existing full projection loop into:

```go
func projectDrainRel(input drainRelProjectionInput) (
    CPUSetTarget,
    machine.CPUSet,
    error,
)
```

Both full and incremental projection call this helper. It owns:

- safe target construction;
- reclaim bucket behavior;
- `buildPhaseTransition`;
- dynamic descendant fallback;
- upper-bound enforcement;
- mems selection;
- empty-blocker calculation.

The incremental implementation must not duplicate these rules.

## Domain Union

Rebuilding the complete `DomainUnion` map per atom would reintroduce
snapshot-wide work. The fast path derives the source-domain result from the
base union:

```text
base source union
minus c if every affected source-domain target loses c
plus c if any affected source-domain target retains c
```

For oracle tests, a materialization helper overlays changed targets onto the
base projection and reconstructs the complete maps. Production deadlock
analysis uses only the source-domain membership result.

## Budget Accounting

The budget counts logical evidence operations rather than hidden CPUSet
implementation details.

Context construction charges:

- one operation per protected-rel ancestor propagation;
- one operation per snapshot rel indexed;
- one operation per frontier rel/CPU membership inserted into
  `frontierRelsByCPU`;
- one operation per child-target CPU membership inserted into
  `baseChildCPUCount`;
- one operation per unique parent edge used to build `ancestorsByRel`;
- the canonical base projection rel and child-edge costs.

Each atom charges:

- one operation per affected rel;
- one operation per changed child contribution inspected;
- one operation for the source-domain membership decision.

No atom is charged for unaffected rels or sibling children.

For the reconstructed 96-CPU fixture, the expected upper bound is:

```text
protected preaggregation                         24
base full projection                            194
snapshot rel indexing                            98
frontier rel/CPU memberships                     96
base child-target CPU memberships                96
unique ancestor closure edges                    96
84 atoms × (2 affected rels + 1 child delta
            + 1 domain decision)                336
---------------------------------------------------
expected upper bound                              940
```

The final replay test must record the exact observed value and lock it as a
golden assertion before merge. The acceptance threshold remains 4,096, not the
estimate.

## CPU Scale

The production target includes machines up to 384 CPUs. Tests must generate a
fixture family with the same shape as the reconstructed overlap snapshot:

- two controlled roots;
- one singleton dynamic leaf per CPU;
- primary/reclaim current and desired sets swapped;
- 12 protected atoms;
- hierarchy depth of two.

With `N` CPUs, the expected operation model is:

```text
protected preaggregation                    24
base projection                         2N + 2
snapshot rel indexing                    N + 2
frontier memberships                         N
child-target memberships                     N
unique ancestor edges                        N
unprotected atoms                    4 × (N - 12)
------------------------------------------------
total                                  10N - 20
```

| CPUs | Expected operations | Remaining budget |
| ---: | ---: | ---: |
| 96 | 940 | 3,156 |
| 192 | 1,900 | 2,196 |
| 384 | 3,820 | 276 |

The 384-CPU result is a hard regression gate. Because the margin is only 276
operations, tests must assert the exact count as well as `<4096`. If actual
implementation accounting exceeds the estimate, implementation must reduce
work rather than increase the default budget.

The model constrains CPU-count growth for the observed depth-two shape. A
separate test varies hierarchy depth and sibling overlap; it may fail closed
when structural complexity, rather than CPU count alone, exceeds the fixed
budget.

## Fallback and Failure

The fast path falls back to the canonical full projection when:

- the atom contains zero or multiple CPUs;
- the CPU index has no owner for an atom present in the transfer graph;
- an affected rel is absent from the snapshot;
- a parent cycle is detected;
- base projection or precomputation is incomplete;
- an invariant required for child-count adjustment cannot be proven.

Fallback consumes the same shared `BudgetTracker`. If it exceeds the remaining
budget, the analysis stays `ProbeIndeterminate` and returns
`ErrDeadlockProbeBudgetExceeded`.

The implementation must never treat an incomplete incremental result as
`SafeSeed`.

## Tests

### Replay budget

Run the reconstructed fixture with `DefaultConvergenceBudget()`:

- `analyzeV1Deadlock` returns no budget error;
- `Completeness == ProbeComplete`;
- `ProbeOperations < 4096`;
- the exact operation count is logged and then fixed as a golden assertion;
- the result is either a valid safe seed or a complete structural-deadlock
  classification.

Run generated 96-, 192-, and 384-CPU variants with the same assertions. The
rel/edge logical-accounting implementation locks exact operation counts
`942`, `1902`, and `3822`.

Add a reconstructed E2E-shape fixture with:

- 232 snapshot entries;
- 219 child edges;
- 12 protected-pending CPUs;
- 2 protected rels;
- 16–20 singleton transfer atoms;
- extra non-transfer descendants that repeat large CPU sets across hierarchy
  levels.

The fixture must complete under the default budget and report
`AtomIndex >= 0` during atom processing. Its context count must remain stable
when additional non-transfer CPU memberships are added.

The implemented reconstructed fixture locks:

```text
probe_operations=1437
context_operations=1357
base_operations=451
atom_operations=80
context_phase=complete
```

### Full-oracle equivalence

For every unprotected atom in the 96-CPU fixture, run:

1. complete `projectDrainTargets` with a high budget;
2. incremental projection.

Compare:

- error classification;
- every affected `TargetByRel` CPUSet and mems;
- `DomainUnion`;
- `EmptyBlockers`;
- source CPU released/retained decision.

### Hierarchy cases

Table tests cover:

- a singleton dynamic leaf under primary;
- multiple siblings sharing the same CPU;
- a three-level ancestor chain;
- primary protected-pending CPUs;
- protected rel at leaf and intermediate levels;
- reclaim NUMA bucket upper bound;
- non-empty fallback retaining current;
- a rel crossing a domain boundary;
- missing parent at the snapshot boundary;
- a multi-CPU atom falling back to full projection.
- a controlled root with residual CPU ownership not present in any child;
- overlapping sibling frontiers that share the same CPU;
- generated 96-, 192-, and 384-CPU snapshots.

### Fail-closed behavior

Keep the existing tiny-budget test. Add cases where:

- context construction exhausts the budget;
- incremental rel evaluation exhausts the budget;
- fallback full projection exhausts the remaining budget.

All must return `ProbeIndeterminate`, no safe seed, and
`ErrDeadlockProbeBudgetExceeded`.

## Acceptance Criteria

- The same 96-CPU fixture completes under the unchanged 4,096 operation
  budget.
- Generated 192- and 384-CPU variants also complete under 4,096; the 384-CPU
  operation count is an exact golden assertion with no budget increase.
- Incremental and full projections are equivalent for every fixture atom.
- Full phase planning still uses canonical complete projection.
- The existing topology package passes.
- No budget increase, retry, or relaxed fail-closed behavior is introduced.
- Probe statistics report context-build, base-projection, and per-atom
  operation costs separately so a future regression identifies the growth
  source.

## Non-goals

- Changing allocation rollback behavior.
- Changing transfer graph construction or atom order.
- Caching across separate snapshots or convergence generations.
- Parallelizing atom probes.
- Raising the default deadlock probe budget.
