# Deadlock Probe ProtectedByRel Preaggregation

## Goal

Reduce deadlock probe work by preaggregating protected CPU sets by snapshot
ancestor, while preserving the canonical drain projection semantics and the
fail-closed budget contract.

## Design

Build an immutable projection context once per `analyzeV1Deadlock` invocation.
It contains the sorted snapshot rels, reclaim bucket upper bounds, and a
`protectedDescendantUnionByRel` map. The protected map is computed by locating
each protected rel in the snapshot and propagating its CPU set through
`ParentByRel` to every ancestor.

`projectDrainTargets` receives this context. For each rel it replaces the full
scan over `ProtectedByRel` with one map lookup. Planning calls that do not
provide a context build one locally, so the existing canonical projection API
retains identical behavior.

## Budget Accounting

Preaggregation charges deadlock probe operations only when used by deadlock
analysis:

- one operation for each protected rel inspected;
- one operation for each ancestor propagation step;
- projection still charges one operation per rel and child edge;
- per-rel scans over all protected rels are removed.

The default budget remains 4096. Budget exhaustion remains fail-closed.

## Tests

- Replay the reconstructed 96-CPU overlap-churn fixture.
- Record the pre-optimization baseline as 4096 exhausted operations.
- Use a high diagnostic budget to compare complete legacy and optimized runs
  and report exact `ProbeOperations`.
- Keep the default 4096 budget fail-closed; compare how far each path advances
  rather than claiming preaggregation alone closes the P0.
- Compare preaggregated protected unions with the legacy full-scan result on a
  multi-level snapshot.
- Keep the existing tiny-budget exhaustion test passing.
- Run the complete topology package.

## Non-goals

- No incremental single-atom projection.
- No budget increase.
- No safe-seed, atom ordering, target, or cgroup mutation changes.
- No allocation rollback changes.
