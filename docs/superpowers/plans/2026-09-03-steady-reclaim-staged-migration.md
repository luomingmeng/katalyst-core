# Steady Reclaim Staged Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow valid whole-core reclaim allocations to converge toward a legal desired allocation over multiple commits while limiting each commit to eight replacement CPU IDs.

**Architecture:** Keep the existing unrestricted whole-core solver as the desired-state owner. Add a deterministic staged projection between committed and desired assignments; it preserves quantity and all hard constraints, advances only from committed state, and fails closed for invalid baselines or bounded-search exhaustion.

**Tech Stack:** Go, `machine.CPUSet`, existing dynamicpolicy partition solver, `testify/require`.

**Commit policy:** Do not create commits unless explicitly requested.

---

### Task 1: Lock the migration-cost contract

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_whole_core.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_whole_core_test.go`

- [ ] **Step 1: Add table-driven tests for replacement churn**

Cover pure expansion, pure shrink, equal-size replacement, expansion with
replacement, and shrink with replacement:

```go
func TestSteadyFakeNUMAMigrationChurn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		old  machine.CPUSet
		next machine.CPUSet
		want int
	}{
		{"pure expansion", machine.NewCPUSet(0, 1), machine.NewCPUSet(0, 1, 2, 3), 0},
		{"pure shrink", machine.NewCPUSet(0, 1, 2, 3), machine.NewCPUSet(0, 1), 0},
		{"one replacement", machine.NewCPUSet(0, 1), machine.NewCPUSet(0, 2), 2},
		{"expansion with replacement", machine.NewCPUSet(0, 1), machine.NewCPUSet(0, 2, 3, 4), 2},
		{"shrink with replacement", machine.NewCPUSet(0, 1, 2, 3), machine.NewCPUSet(0, 4), 2},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, steadyFakeNUMAMigrationChurn(tt.old, tt.next))
		})
	}
}
```

- [ ] **Step 2: Run the focused test**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run TestSteadyFakeNUMAMigrationChurn -count=1
```

Expected: PASS for the already corrected expansion/shrink accounting.

- [ ] **Step 3: Keep score and final validation on the same helper**

Verify both `steadyFakeNUMAAssignmentScore` and
`validateSteadyFakeNUMAFinal` call `steadyFakeNUMAMigrationChurn`; remove any
remaining raw symmetric-difference migration calculation.

---

### Task 2: Reproduce multi-cycle replacement

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_whole_core_test.go`

- [ ] **Step 1: Add a failing multi-cycle convergence test**

Build an SMT2 topology with a valid committed whole-core fake allocation and a
legal desired allocation requiring three physical-core replacements, or 12
changed CPU IDs. Re-run the solver using the prior returned allocation as
`preferred`:

```go
current := initial
for cycle := 0; cycle < 3 && !current.Equals(desired); cycle++ {
	demands := stagedMigrationDemands(topology, current, desired)
	next, err := solveSteadyFakeNUMAWholeCore(demands, []string{"fake"}, topology)
	require.NoError(t, err)
	require.NoError(t, assertCoreAligned(next["fake"], topology))
	require.Equal(t, desired.Size(), next["fake"].Size())
	require.LessOrEqual(t,
		steadyFakeNUMAMigrationChurn(current, next["fake"]),
		steadyFakeNUMAMaxMigratedCPUs)
	current = next["fake"]
}
require.Equal(t, desired, current)
```

The fixture must constrain the non-fake demands so `desired` is unique.

- [ ] **Step 2: Verify RED**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run TestSolveSteadyFakeNUMAWholeCoreConvergesInBoundedStages -count=1
```

Expected: FAIL because the current planner rejects the unrestricted target
instead of returning an intermediate allocation.

---

### Task 3: Add the staged projection

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_whole_core.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_whole_core_test.go`

- [ ] **Step 1: Separate unrestricted desired from committed state**

Add a helper that receives the committed fake union and unrestricted desired
assignments:

```go
func projectSteadyFakeNUMAStage(
	demands []partitionDemand,
	fakeKeys []string,
	committed machine.CPUSet,
	desired map[string]machine.CPUSet,
	floors []partitionCoreFloorConstraint,
	topology *machine.CPUTopology,
) (map[string]machine.CPUSet, error)
```

Return `desired` immediately when replacement churn is at most eight.

- [ ] **Step 2: Reject staged repair from an invalid committed baseline**

Before generating an intermediate state:

```go
if err := assertCoreAligned(committed, topology); err != nil {
	return nil, fmt.Errorf(
		"invalid committed reclaim requires atomic repair: %w", err)
}
```

The existing atomic-repair path remains responsible for fragmented baselines.

- [ ] **Step 3: Build deterministic complete-core replacement actions**

Create actions pairing complete cores from `committed-desired` with complete
cores from `desired-committed`. Sort actions by existing reclaim candidate
preferences and stable topology identity. Limit selected actions so:

```go
2 * selectedChangedCPUCount <= steadyFakeNUMAMaxMigratedCPUs
```

For SMT2 this permits two complete-core replacements per cycle.

- [ ] **Step 4: Solve the intermediate state through existing constraints**

Pin retained committed cores plus the selected desired replacement cores, then
call `solveSteadyFakeNUMAWithPins`. Do not construct the final assignment by
set arithmetic alone. Validate the result through
`validateSteadyFakeNUMAFinal`, core alignment, quantity, floors, eligibility,
and disjointness.

- [ ] **Step 5: Make bounded failure explicit**

If no legal intermediate state exists:

```go
return nil, fmt.Errorf(
	"no legal staged reclaim migration within %d changed CPU IDs",
	steadyFakeNUMAMaxMigratedCPUs)
```

- [ ] **Step 6: Verify GREEN**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestSolveSteadyFakeNUMAWholeCoreConvergesInBoundedStages|TestSteadyFakeNUMAMigrationChurn' \
  -count=1
```

Expected: PASS.

---

### Task 4: Prove intermediate-state safety

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_whole_core_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go`

- [ ] **Step 1: Add SMT1, SMT2, and SMT4 convergence cases**

For every cycle assert:

```go
require.NoError(t, assertCoreAligned(next, topology))
require.Equal(t, targetQuantity, next.Size())
require.LessOrEqual(t,
	steadyFakeNUMAMigrationChurn(current, next),
	steadyFakeNUMAMaxMigratedCPUs)
```

- [ ] **Step 2: Add per-NUMA and donor-floor assertions**

Use a fixture where only one replacement ordering preserves a constrained donor
floor. Assert every intermediate assignment satisfies the exact per-NUMA quota
and donor minimum.

- [ ] **Step 3: Add latest-state redirection tests**

After the first staged result, change eligibility and desired placement. Assert
the next result is derived from the committed intermediate state and does not
continue toward the superseded target.

- [ ] **Step 4: Add retry idempotency**

Call the planner twice with identical committed state and advice:

```go
first := solve(committed, advice)
retry := solve(committed, advice)
require.Equal(t, first, retry)
```

Then use `first` as committed state and assert the next cycle advances.

- [ ] **Step 5: Keep invalid-baseline fail-closed coverage**

Retain the fragmented baseline test requiring atomic repair. Add a case where
atomic repair needs more than eight changed IDs and assert no intermediate
fragmented result is returned.

---

### Task 5: Validate integration and regressions

**Files:**
- Verify all modified dynamicpolicy files.

- [ ] **Step 1: Run focused staged-migration tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestSolveSteadyFakeNUMAWholeCore|TestSteadyFakeNUMAMigrationChurn' \
  -count=1
```

- [ ] **Step 2: Run the complete package**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
```

- [ ] **Step 3: Run race tests**

```bash
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
```

- [ ] **Step 4: Check the diff**

```bash
git diff --check
git status --short
```

- [ ] **Step 5: Rebuild and run node validation**

Build the native CGO agent on the x86_64 Linux build host, deploy it to
`fdbd:dc05:d:44e::17`, and run the AQC-controlled standard three-round and
high-churn five-round Pod workflows without reset/target configuration changes.

Acceptance evidence:

```text
all stable/recreate phases: Running=12
all delete phases: remaining=0
Failed Pod count: 0
healthz: HTTP 200 after final cleanup
WHOLE_CORE_ALIGNMENT=OK
no "fake reclaim migration ... exceeds limit" errors
```

---

### Task 6: Persist the lightweight migration target

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_migration_checkpoint.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_migration_checkpoint_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/steady_fake_numa_whole_core.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`

- [x] **Step 1: Write failing restart and corruption tests**

Cover a target surviving reconstruction of `DynamicPolicy` from the same state
directory and malformed target JSON causing initialization to fail while the
file remains intact.

- [x] **Step 2: Implement strict versioned target storage**

Persist only the sorted target CPU IDs, canonical constraint digest, version,
and checksum. Publish with temp-file write, file fsync, rename, and directory
fsync. Strictly reject malformed/unknown/trailing JSON, unsupported versions,
empty digests, checksum mismatch, duplicate/out-of-topology CPUs, and
non-whole-core targets.

- [x] **Step 3: Write failing digest, continuation, replacement, and cleanup tests**

Assert that changing only committed `preferred` placement leaves the digest
unchanged, while eligibility or another actual constraint changes it. Assert
same-digest restart continues toward the durable target, changed constraints
replace it, and a committed state equal to target removes it.

- [x] **Step 4: Integrate target-aware final projection**

Keep unrestricted solving as desired-state owner. Before final projection,
reuse a same-digest durable fake union by reconstructing legal per-demand
assignments through the existing pin solver. Persist a newly computed target
only when replacement churn exceeds eight.

- [x] **Step 5: Run focused tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestSteadyFakeNUMAMigrationTarget|TestSteadyFakeNUMAConstraintDigest|TestSolveSteadyFakeNUMAWholeCoreDelegatesFinalProjection|TestProjectSteadyFakeNUMAStage' \
  -count=1
```

Expected: PASS.

---

### Task 7: Close WAL and DNB rollback durability gaps

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`

- [x] **Step 1: Write a failing active-plus-staging WAL cleanup test**

Create both slots for the same pending target, reconcile successfully, and
assert that memory, active, and staging are all cleared.

- [x] **Step 2: Remove both WAL slots on success and stale cleanup**

Use one helper that removes and directory-fsyncs active followed by staging.
Do not use it on the failed-new-commit path, where the old active slot must
remain and only uncommitted staging is removed.

- [x] **Step 3: Write a failing single rollback write-failure test**

Inject an adjustment failure and then one explicit `StoreState` failure.
Assert candidate commit used persistence, rollback CAS used `persist=false`,
exactly one explicit store was attempted, and source state remains restored in
memory despite the disk error.

- [x] **Step 4: Restore first, persist once**

For exact-source and stale-replanned DNB rollback, commit restored state with
`persist=false`; when persistence is requested, call `StoreState` exactly once
after successful in-memory restoration.

- [x] **Step 5: Run focused tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestAdvisorPostCommitReconcileCleansActiveAndStagingCheckpoints|TestAdvisorPostCommitCheckpointRevisionMismatchIsCleaned|TestAllocateDedicatedNUMAExclusiveRollbackRestoresInMemoryBeforeSingleStoreFailure|TestAllocateDedicatedNUMAExclusiveApplyFailureRestoresSourceAndRetriesSameStage' \
  -count=1
```

Expected: PASS.

---

### Task 8: Final local verification

**Files:**
- Verify all modified files under `pkg/agent/qrm-plugins/cpu/dynamicpolicy`.
- Verify this design and implementation plan.

- [x] **Step 1: Run the root dynamicpolicy package**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
```

Expected: PASS.

- [x] **Step 2: Run the root dynamicpolicy package with race detection**

```bash
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
```

Expected: PASS.

- [x] **Step 3: Check formatting and diff hygiene**

```bash
gofmt -w <modified-go-files>
git diff --check
git status --short
```

Expected: clean formatting, no whitespace errors, and only intended uncommitted
changes in the requested worktree.
