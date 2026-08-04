# Deadlock Transfer CPU Context Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep deadlock context construction below 4096 operations on the real 232-entry hierarchy by indexing only CPUs present in transfer atoms.

**Architecture:** Build the complete canonical base projection unchanged. Derive `transferCPUs` from the transfer graph before context preparation and intersect only child-count/frontier indexing with that set. Add context phase diagnostics and a reconstructed 232-entry/219-edge regression fixture.

**Tech Stack:** Go, standard `testing`, existing topology planner fixtures.

---

### Task 1: Reproduce the E2E hierarchy failure

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock_test.go`

- [ ] Add `buildE2EShapeDeadlockInput` producing exactly 232 entries and 219 child edges with 12 protected-pending CPUs, 2 protected rels, and 16 singleton transfer atoms.
- [ ] Add repeated non-transfer descendants whose targets contain large CPU sets.
- [ ] Run:

```bash
go test -run '^TestDeadlockAnalysisE2EShapeCompletesWithinDefaultBudget$' \
  -count=1 -v ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
```

Expected before implementation: `ErrDeadlockProbeBudgetExceeded` with
`AtomIndex == -1`, `SnapshotEntries == 232`, and
`SnapshotChildEdges == 219`.

### Task 2: Filter context indexes by transfer CPUs

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/drain_projection.go`

- [ ] Build `transferCPUs` from `analysis.Atoms` before context preparation.
- [ ] Pass `transferCPUs` to `prepareIncrementalDrainProjectionContext`.
- [ ] Keep `baseChildUnionByRel` complete.
- [ ] Intersect only charged/indexed membership:

```go
relevantChildTarget := childTarget.Intersection(transferCPUs)
relevantFrontier := frontier.Intersection(transferCPUs)
```

- [ ] Re-run Task 1 and require `ProbeComplete` with
`ProbeOperations < 4096`.

### Task 3: Lock invariance and diagnostics

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock_test.go`

- [ ] Add `ContextPhase` and context operation counts to budget errors.
- [ ] Compare two equal transfer graphs where one snapshot has additional
non-transfer CPU memberships; context index operation counts must match.
- [ ] Keep the existing 96/192/384 golden tests passing.
- [ ] Run:

```bash
go test -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
go test -race -count=1 ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
git diff --check -- pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
```

Expected: all commands pass; no budget increase.
