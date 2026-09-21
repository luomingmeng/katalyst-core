# Steady Reclaim Repair Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent invalid committed-reclaim repair from dropping a positive real-NUMA floor demand to zero.

**Architecture:** Preserve the temporary committed-state repair solve, but validate every repair candidate against the original immutable advisor block quantities before returning it. Expanded per-NUMA demands are aggregated by block identity so legal staged placement can differ from the final balanced quota. Make the core-floor solver independently reject non-positive floor demands and empty assignments.

**Tech Stack:** Go, `machine.CPUSet`, table-driven unit tests, Go test.

---

### Task 1: Reproduce the production repair failure

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_whole_core_test.go`

- [ ] Add a test with non-empty committed fake reclaim, an empty committed real-NUMA floor, and positive original quantities.
- [ ] Call `projectSteadyFakeNUMAStage` and assert every returned assignment has its original quantity.
- [ ] Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run TestProjectSteadyFakeNUMAStagePreservesOriginalDemandContractDuringInvalidCommittedRepair -count=1
```

Expected: FAIL because the real-NUMA floor assignment has size zero.

### Task 2: Reproduce floor fail-open behavior

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_constraints_test.go`

- [ ] Add a test that supplies a zero-quantity demand referenced by a core-floor constraint.
- [ ] Assert the solver returns an error containing `non-positive quantity`.
- [ ] Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run TestSolveDisjointPartitionsWithCoreFloorsRejectsNonPositiveFloorDemand -count=1
```

Expected: FAIL because the empty assignment is currently accepted as core-aligned.

### Task 3: Enforce the repair and floor contracts

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_whole_core.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_constraints.go`

- [ ] Validate a repair candidate by aggregating original demands and assignments by advisor block identity.
- [ ] Reject core-floor demands with non-positive quantity before checking alignment.
- [ ] Reject empty floor assignments explicitly.
- [ ] Use `general.InfoS` only if diagnostic logging is needed.
- [ ] Run both focused tests and expect PASS.

### Task 4: Regression verification

**Files:**
- No additional changes expected.

- [ ] Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run 'SteadyFakeNUMA|CoreFloor' -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run 'SteadyFakeNUMA|CoreFloor' -count=20
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run 'SteadyFakeNUMA|CoreFloor' -count=1
```

- [ ] Review `git diff --check`, `git status`, and the final diff for unrelated changes.
- [ ] Record RED and GREEN outputs in the delivery summary.
