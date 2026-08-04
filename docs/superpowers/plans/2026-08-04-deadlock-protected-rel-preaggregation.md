# Deadlock ProtectedByRel Preaggregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preaggregate protected descendant CPU sets once per deadlock analysis and compare complete legacy/optimized operation counts while retaining the 4096 fail-closed budget.

**Architecture:** Add an immutable `drainProjectionContext` built from snapshot relations and `ProtectedByRel`. Deadlock analysis builds it once and reuses it for every atom projection; ordinary planning builds it locally. Budget accounting charges preaggregation work once and retains per-rel/per-child projection charges.

**Tech Stack:** Go, standard `testing`, existing topology planner fixtures.

---

### Task 1: Lock the operation baseline

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock_test.go`

- [ ] Add a helper that runs the replay with the legacy full protected scan and asserts `ErrDeadlockProbeBudgetExceeded` with `ProbeOperations == 4096`.
- [ ] Run legacy and optimized paths with a high diagnostic budget and require optimized `ProbeOperations` to be lower.
- [ ] Run:

```bash
go test -run '^TestDeadlockAnalysisOverlapChurnReplay' -count=1 -v \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
```

Expected before implementation: optimized-path assertion fails.

### Task 2: Preaggregate protected descendants

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/drain_projection.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock.go`

- [ ] Define:

```go
type drainProjectionContext struct {
    rels                          []string
    bucketUpperByRel              map[string]machine.CPUSet
    protectedDescendantUnionByRel map[string]machine.CPUSet
}
```

- [ ] Build protected unions by walking each protected rel through `ParentByRel` to the root. Charge one probe operation for each protected rel and each ancestor propagation only when a probe budget is present.
- [ ] Add the context to `DrainProjectionInput`.
- [ ] Replace the nested `for protectedRel := range in.ProtectedByRel` scan with one lookup:

```go
required = required.Union(projectionContext.protectedDescendantUnionByRel[rel])
```

- [ ] Build the context once in `analyzeV1Deadlock` and pass it to every atom projection.
- [ ] Keep ordinary planner calls compatible by building a local unbudgeted context when none is supplied.

### Task 3: Prove semantic equivalence

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/drain_projection_test.go`

- [ ] Add a multi-level snapshot with protected rels at leaf and intermediate levels.
- [ ] Compare each preaggregated union with a test-local legacy scan:

```go
for rel := range snapshot.Entries {
    want := legacyProtectedDescendantUnion(rel, protectedByRel)
    got := ctx.protectedDescendantUnionByRel[rel]
    if !got.Equals(want) {
        t.Fatalf(...)
    }
}
```

- [ ] Verify a complete projection produces identical targets with and without a supplied context.

### Task 4: Verify budgets and report delta

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock_test.go`

- [ ] Keep `TestDeadlockAnalysisBudgetExhaustionFailsClosed` passing.
- [ ] Log and assert:

```go
t.Logf("probe_operations before=%d after=%d reduction=%.2f%%", before, after, reduction)
```

- [ ] Run:

```bash
go test -run 'TestDeadlockAnalysis(BudgetExhaustionFailsClosed|OverlapChurnReplay)' \
  -count=1 -v ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
go test -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
git diff --check -- pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
```

Expected: all commands pass; replay reports exact before/after operations; both paths remain fail-closed when their work exceeds the unchanged 4096 default.
