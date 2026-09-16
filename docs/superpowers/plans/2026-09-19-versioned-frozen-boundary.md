# Versioned Frozen Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `ExpandedRels` exact replay with a compiler-owned, versioned `FrozenBoundary` whose shared evaluator ignores unrelated dynamic sibling churn, binds exact direct-child membership/union only for shrink operations, and fails closed on controlled or relevant-CPU-holder drift.

**Architecture:** The trace compiler derives and freezes one semantic boundary from its DAG, evaluation inputs, operations, and initial snapshot. Preflight and finalization both perform a fresh root scan and call the same evaluator against their expected snapshot. Shrink operations retain exact child membership/identity/union proof; grow operations allow direct-child additions and removals while preserving controlled identity, configured/effective predecessor, parent containment, and relevant-CPU ownership checks. `ExpandedRels` remains diagnostic only and is removed from replay, fingerprint, and stale-decision ownership.

**Tech Stack:** Go 1.18.10, Kubernetes CPUSet utilities, existing topology snapshot/compiler/executor and fake hierarchy drivers.

---

## Constraints

- Only modify this worktree.
- No deployment, remote node, cgroup, checkpoint, schema, WAL, or configuration changes.
- Every production behavior change follows RED → observed failure → minimal GREEN → focused test.
- Keep logical commits separate.
- Preserve operation-level predecessor checks and full-prefix rollback.

## File Map

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go`
  to retire exact replay and make `ExpandedRels` diagnostic-only.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot_test.go`
  to lock diagnostic-only fingerprint semantics.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary.go`
  for versioned types, compiler, validation, cloning, hashing, and shared evaluator.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary_test.go`
  for compiler/evaluator contract tests.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go`
  to make compiler own and freeze/hash the boundary.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go`
  for freeze/version/hash ownership tests.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go`
  so preflight/finalization share the evaluator.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go`
  for churn and fail-closed integration tests.

## Task 1: Define the Versioned Compiler-Owned Contract

- [ ] Add RED tests:

```go
func TestCompileFrozenBoundaryV1ClassifiesControlledDirectChildrenAndRelevantHolders(t *testing.T)
func TestCompileFrozenBoundaryV1ExcludesUnrelatedDynamicSibling(t *testing.T)
func TestValidateFrozenBoundaryRejectsUnknownVersion(t *testing.T)
func TestCloneFrozenBoundaryIsDeeplyIsolated(t *testing.T)
```

The fixture contains a controlled root, its direct dynamic child, a deeper holder intersecting
`RelevantCPUs`, and a deeper sibling on disjoint CPUs.

- [ ] Run RED:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestCompileFrozenBoundary|TestValidateFrozenBoundary|TestCloneFrozenBoundary' -count=1
```

Expected: build failure because `FrozenBoundary` and its compiler do not exist.

- [ ] Implement `FrozenBoundaryVersionV1`, immutable clone/normalize/validate helpers, relevant CPU
  derivation, and compiler-only construction.

- [ ] Run focused GREEN and commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary_test.go
git commit -m "feat(qrm-cpu): define versioned frozen topology boundary"
```

## Task 2: Bind FrozenBoundary to Trace Compilation

- [ ] Add RED tests:

```go
func TestCompileFixedPointTraceOwnsFrozenBoundary(t *testing.T)
func TestFreezePhaseTraceRejectsBoundaryNotDerivedFromCompilerInputs(t *testing.T)
func TestFreezePhaseTraceBoundaryMutationDoesNotChangeFrozenTrace(t *testing.T)
func TestFrozenBoundaryChangesTraceID(t *testing.T)
```

- [ ] Run RED with `-run 'FrozenBoundary|CompileFixedPointTraceOwns'`.

- [ ] Add `FrozenBoundary FrozenBoundary` to `CompiledPhaseTrace`; derive it in
  `compileFixedPointTrace`; validate and clone it in `FreezePhaseTrace`; include version, roots,
  controlled rels, direct-child refs, relevant CPUs, and relevant holders in `TraceID`.

- [ ] Run focused GREEN and commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary.go
git commit -m "feat(qrm-cpu): bind frozen boundary to trace compiler"
```

## Task 3: Implement One Shared Boundary Evaluator

- [ ] Add RED table tests:

```go
func TestEvaluateFrozenBoundaryAllowsUnrelatedDynamicSiblingChurn(t *testing.T)
func TestEvaluateFrozenBoundaryRejectsControlledRelDrift(t *testing.T)
func TestEvaluateFrozenBoundaryRejectsDirectChildChurn(t *testing.T)
func TestEvaluateFrozenBoundaryRejectsRelevantCPUHolderDrift(t *testing.T)
func TestEvaluateFrozenBoundaryRejectsNewRelevantCPUHolder(t *testing.T)
```

For allowed churn, cover create, delete, identity, CPU and mems changes on a deep dynamic sibling
whose effective CPUs are disjoint from `RelevantCPUs`. For fail-closed cases, cover deletion,
identity replacement, configured/effective CPU, and mems changes.

- [ ] Run RED with `-run '^TestEvaluateFrozenBoundary'`.

- [ ] Implement `EvaluateFrozenBoundary`: fresh-scan `Roots`, validate the version, compare controlled
  states/direct children/relevant holders, and reject newly classified relevant holders.

- [ ] Run focused GREEN and commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary_test.go
git commit -m "feat(qrm-cpu): evaluate frozen topology boundaries"
```

## Task 4: Share Evaluator Between Preflight and Finalization

- [ ] Add RED integration tests:

```go
func TestTracePreflightAllowsUnrelatedDynamicSiblingChurn(t *testing.T)
func TestTraceFinalizationAllowsUnrelatedDynamicSiblingChurn(t *testing.T)
func TestTracePreflightAndFinalizationRejectSameBoundaryDrift(t *testing.T)
func TestTraceBoundaryDriftBeforeWritePerformsZeroWrites(t *testing.T)
func TestTraceBoundaryDriftAtFinalizationRollsBackPrefix(t *testing.T)
```

- [ ] Run RED. The first tests must fail because current code exact-replays `ExpandedRels` and
  compares complete snapshot IDs.

- [ ] Replace both `BuildCompleteSnapshotForBoundary` calls and snapshot-ID equality checks with
  `EvaluateFrozenBoundary`. Preflight projects every operation from the evaluator snapshot;
  finalization runs the existing frozen `EvaluationInput.evaluate` on the evaluator snapshot.

- [ ] Run focused GREEN and commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go
git commit -m "fix(qrm-cpu): share frozen boundary evaluation"
```

## Task 5: Retire ExpandedRels Ownership

- [ ] Add RED tests:

```go
func TestSnapshotFingerprintIgnoresExpandedRelsDiagnostics(t *testing.T)
func TestSnapshotStillRecordsExpandedRelsForDiagnostics(t *testing.T)
```

- [ ] Run RED. The fingerprint test must fail because `fingerprintSnapshot` currently hashes
  `ExpandedRels`.

- [ ] Delete `BuildCompleteSnapshotForBoundary`, `snapshotBuilder.exactExpansion`,
  `ErrSnapshotBoundaryExpansionMismatch`, exact replay branches, and obsolete tests. Remove
  `ExpandedRels` from `fingerprintSnapshot` and from evidence ownership validation while retaining
  scan-time population and cloning.

- [ ] Verify no owner remains:

```bash
git grep -n -E 'BuildCompleteSnapshotForBoundary|exactExpansion|ErrSnapshotBoundaryExpansionMismatch'
```

Expected: no output.

- [ ] Run topology GREEN and commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
git commit -m "refactor(qrm-cpu): make expanded rels diagnostic only"
```

## Task 6: Race and Scale Gates

- [ ] Add/adjust scale tests so boundary compile/evaluate covers 100, 1000, and gated 10000
  relations, deep irrelevant churn, and new relevant holders.

- [ ] Add benchmark:

```go
func BenchmarkCompileFrozenBoundary(b *testing.B)
func BenchmarkEvaluateFrozenBoundary(b *testing.B)
```

- [ ] Run scale correctness and benchmark:

```bash
KATALYST_TOPOLOGY_SCALE_TEST=1 go test \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Scale|TenThousand|FrozenBoundary' -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run '^$' -bench 'Benchmark(Compile|Evaluate)FrozenBoundary' -benchmem -count=1
```

- [ ] Commit test-only scale changes:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
git commit -m "test(qrm-cpu): gate frozen boundary scale"
```

## Task 7: Final Local Verification

- [ ] topology:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology -count=1
```

- [ ] cpusettopology:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology -count=1
```

- [ ] race:

```bash
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology -count=1
```

- [ ] scale:

```bash
KATALYST_TOPOLOGY_SCALE_TEST=1 go test \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Scale|TenThousand|FrozenBoundary' -count=1
```

- [ ] repository hygiene:

```bash
git diff --check
git status --short
git log --oneline c23cea45a..HEAD
```

No deployment is performed.

## Task 8: Make Direct-Child Proof Operation-Directional

- [ ] Add RED integration tests:

```go
func TestTraceGrowDirectChildRemovalSucceeds(t *testing.T)
func TestTraceShrinkDirectChildAdditionOutsideTargetFailsBeforeUnsafeWrite(t *testing.T)
func TestTraceGrowNewRelevantCPUHolderFails(t *testing.T)
```

The grow-removal fixture removes a non-relevant direct child after trace compilation and must
complete without restoring or depending on that child. The shrink fixture adds a direct child whose
CPU lies outside the shrink target and asserts zero unsafe forward writes. The grow-holder fixture
adds a direct child that owns a transition CPU and must fail during frozen-boundary preflight.

- [ ] Run RED before production edits:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestTrace(GrowDirectChildRemovalSucceeds|ShrinkDirectChildAdditionOutsideTargetFailsBeforeUnsafeWrite|GrowNewRelevantCPUHolderFails)' \
  -count=1
```

Expected: grow child removal fails under the old direction-agnostic direct-child contract.

- [ ] Replace `DirectChildrenByRel` with shrink-only `ShrinkChildrenByRel`; compile, validate,
  clone, hash, and evaluate exact child evidence only for relations with shrink operations.
- [ ] In projected preflight, require `ExpectedChildren` and child union only for shrink operations.
- [ ] In frozen execution, freeze and revalidate exact child membership/identity/union only for
  shrink operations. Grow operations continue to verify controlled identity, complete configured
  and effective predecessor, parent identity/containment, and evaluator-owned relevant CPU holders.
- [ ] Delete direction-agnostic direct-child tests and wording; retain tests for shrink exactness.
- [ ] Run focused GREEN, all verification gates, and create one independent commit:

```bash
git add docs/superpowers/specs/2026-09-19-versioned-frozen-boundary-design.md \
  docs/superpowers/plans/2026-09-19-versioned-frozen-boundary.md \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
git commit -m "fix(qrm-cpu): make frozen child proof direction-aware"
```

## Acceptance Matrix

| Requirement | Evidence |
|---|---|
| Versioned FrozenBoundary | unknown-version rejection and freeze/hash tests |
| Compiler is sole owner | derived-boundary equality test and no caller injection path |
| Shared preflight/final evaluator | parity integration test and one evaluator call path |
| Unrelated dynamic sibling churn valid | preflight and finalization create/delete/change cases |
| Shrink direct child changes fail closed | shrink direct-child identity/set/union matrix |
| Grow non-relevant direct child churn valid | grow direct-child removal success |
| Controlled rel changes fail closed | controlled state matrix |
| Relevant CPU holder changes fail closed | existing/new holder matrix |
| ExpandedRels diagnostic only | fingerprint test and retired exact-replay symbols |
| Zero-write preflight failure | physical write count assertion |
| Final drift rollback | full-prefix rollback assertion |
| No deployment | local commands only |
