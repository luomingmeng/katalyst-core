# Dedicated–Reclaim Atomic Replacement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent new exclusive DNB admissions from creating a partial-core reclaim boundary and repair historical dedicated/reclaim boundaries through same-NUMA, quantity-preserving atomic replacement.

**Architecture:** Admission derives a soft reclaim target from complete physical cores and leaves any down-rounded remainder in the exclusive DNB complement. Steady repair keeps the existing hard-reclaim planner as a fast path, then uses a bounded replacement-aware terminal solver that fixes reclaim to complete cores and proves exact residual dedicated assignments before entering the existing pending-state, WAL, revision-CAS, frozen-trace, and rollback pipeline.

**Tech Stack:** Go, `machine.CPUSet`, QRM dynamic policy state, bounded frontier search, existing disjoint min-cost-flow solver, table-driven tests, race tests, Bulkhead frozen trace.

---

## File Structure

Production changes:

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
  - Admission-only reclaim target derivation and soft-target degradation.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go`
  - Typed selection failures, replacement candidate search, terminal residual assignment, and final proof.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go`
  - Replacement fallback routing and exact assignment pinning.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit.go`
  - Defense-in-depth validation for final whole-core reclaim and unchanged owner scope.

Test changes:

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go`

No protobuf, checkpoint, WAL, API, or `go.mod` changes are expected.

---

### Task 1: Add Admission RED Tests

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`

- [ ] **Step 1: Add a focused target-rounding test**

Add `TestDynamicPolicyDeriveSteadyReclaimFloorRoundsSoftTargetDown` with an SMT2
topology and a NUMA eligibility set whose ratio-derived logical quantity is not
representable as complete cores.

The assertions must express the contract directly:

```go
floor, err := policy.deriveSteadyReclaimFloor(map[int]machine.CPUSet{
    0: eligible,
})
require.NoError(t, err)
require.Equal(t, expectedWholeCoreQuantity, floor.Size())
require.NoError(t, assertCoreAligned(floor, topology))
require.True(t, floor.IsSubsetOf(eligible))
```

Use a ratio budget that falls between two complete-core capacities. Do not use
internal or environment-specific cgroup path names in the fixture.

- [ ] **Step 2: Add the end-to-end admission remainder test**

Add `TestDedicatedNUMAExclusiveAdmissionReturnsReclaimRemainderToDNB`. Build one
exclusive hinted NUMA with a reclaim target that must be reduced by one SMT
width. Invoke `dedicatedCoresWithNUMABindingAllocationHandler` and assert:

```go
require.True(t, dnb.AllocationResult.Intersection(reclaim.AllocationResult).IsEmpty())
require.True(t, dnb.AllocationResult.Union(reclaim.AllocationResult).Equals(partitionEligible))
require.NoError(t, assertCoreAligned(reclaim.AllocationResult, topology))
require.Greater(t, dnb.AllocationResult.Size(), advisorBlockResult)
require.Equal(t, partitionEligible.Size()-reclaim.AllocationResult.Size(), dnb.AllocationResult.Size())
```

- [ ] **Step 3: Add mandatory-identity and multi-NUMA cases**

Add:

```go
func TestDynamicPolicyDeriveSteadyReclaimFloorPreservesMandatoryIdentity(t *testing.T)
func TestDedicatedNUMAExclusiveAdmissionKeepsRemainderInOriginalNUMA(t *testing.T)
```

The first test must prove that a configured reclaim identity is not removed by
soft down-rounding. The second must compare cardinality per NUMA, not only the
global union.

- [ ] **Step 4: Run the RED tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'DeriveSteadyReclaimFloorRoundsSoftTargetDown|AdmissionReturnsReclaimRemainder|PreservesMandatoryIdentity|KeepsRemainderInOriginalNUMA' \
  -count=1
```

Expected: at least the soft-target case fails because
`deriveSteadyReclaimFloor` currently requires the rounded-up target exactly.

- [ ] **Step 5: Commit the RED tests**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go
git commit -m "test(qrm-cpu): cover admission reclaim remainder"
```

---

### Task 2: Implement Admission Whole-Core Degradation

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`

- [ ] **Step 1: Add an admission-scoped selector**

Add a helper adjacent to `deriveSteadyReclaimFloor`:

```go
func selectAdmissionSteadyReclaimTarget(
    topology *machine.CPUTopology,
    eligible machine.CPUSet,
    preferred machine.CPUSet,
    mandatory machine.CPUSet,
    softTarget int,
) (machine.CPUSet, error) {
    completedMandatory, err := completeEligibleCoresForPreferredCPUSet(
        topology, eligible, mandatory)
    if err != nil {
        return machine.NewCPUSet(), err
    }
    if !completedMandatory.IsSubsetOf(eligible) {
        return machine.NewCPUSet(), fmt.Errorf(
            "mandatory reclaim CPUs %s are outside eligibility %s",
            completedMandatory.String(), eligible.String())
    }

    selected := completedMandatory.Clone()
    budget := softTarget - selected.Size()
    if budget <= 0 {
        return selected, nil
    }
    supplementEligible := eligible.Difference(selected)
    supplement := takeCoreAlignedCPUSet(
        topology,
        supplementEligible,
        preferred.Intersection(supplementEligible),
        budget,
    )
    return selected.Union(supplement), nil
}
```

The helper deliberately accepts a result smaller than `softTarget`; it must
never remove `completedMandatory`.

- [ ] **Step 2: Integrate it into `deriveSteadyReclaimFloor`**

Replace the exact-size supplement requirement with the helper:

```go
selected, err := selectAdmissionSteadyReclaimTarget(
    p.machineInfo.CPUTopology,
    eligible,
    currentReclaim.Intersection(eligible),
    reservedIdentities,
    targetByNUMA[numaID],
)
if err != nil {
    return machine.NewCPUSet(), fmt.Errorf(
        "derive steady reclaim floor for NUMA %d: %w", numaID, err)
}
floor = floor.Union(selected)
```

Remove the error that requires `supplement.Size() == additional`. Keep errors
for missing eligibility and invalid mandatory identities.

- [ ] **Step 3: Add a degradation log**

When the selected size is below the soft target, emit one structured log:

```go
general.InfoS("degrade admission reclaim target to complete-core capacity",
    "numaID", numaID,
    "requestedReclaimQuantity", target,
    "actualReclaimQuantity", selected.Size(),
    "remainderForDedicated", target-selected.Size(),
    "reason", "whole_core_capacity")
```

Do not add unbounded labels or a new metric owner in this task.

- [ ] **Step 4: Keep complement ownership unchanged**

Do not add a manual union into the DNB allocation. Confirm the existing
`shrinkAllocationInfoForHardReclaimFloor` call leaves:

```text
final DNB = exclusive partition - selected reclaim
```

- [ ] **Step 5: Run focused and adjacent tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'DeriveSteadyReclaimFloor|DedicatedNUMAExclusive' \
  -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go
git commit -m "fix(qrm-cpu): round admission reclaim to complete cores"
```

---

### Task 3: Add Steady Replacement RED Tests

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go`

- [ ] **Step 1: Encode the production topology**

Add `TestPlanHardReclaimPartitionRepairsDedicatedBoundaryByReplacement`.
Represent NUMA 2 with SMT pairs equivalent to:

```text
reclaim before = 32,46,47,96,108,109,110,111
dedicated before = 33-45,97-107
target reclaim = 8
dedicated quantity = 24
canonical request = 62
```

The expected terminal state must be deterministic:

```text
reclaim after = 32,33,46,47,96,97,110,111
dedicated after = 34-45,98-109
```

Assert final reclaim alignment, exact quantities, disjointness, and same-NUMA
ownership.

- [ ] **Step 2: Add donor protection tests**

Add:

```go
func TestHardReclaimReplacementPreservesUnderprovisionedDonorSize(t *testing.T)
func TestHardReclaimReplacementRejectsAdditionalDonorLoss(t *testing.T)
func TestHardReclaimReplacementRejectsCrossNUMACompensation(t *testing.T)
```

Use:

```text
protectedFloor = min(oldOwned, ceil(requestQuantity))
```

The first test must allow `oldOwned=48`, `request=62`, `newOwned=48`. The second
must reject `newOwned=46`.

- [ ] **Step 3: Add final-state, alias, and determinism tests**

Add:

```go
func TestHardReclaimReplacementRejectsPartialFinalReclaimCore(t *testing.T)
func TestHardReclaimReplacementDoesNotShareCreditAcrossOwnerGroups(t *testing.T)
func TestHardReclaimReplacementDeduplicatesAliasesByRequestGroup(t *testing.T)
func TestHardReclaimReplacementIsDeterministic(t *testing.T)
func TestHardReclaimReplacementRejectsBudgetExhaustion(t *testing.T)
```

The final-state test must not require the incoming or outgoing deltas to be
individually core-aligned.

- [ ] **Step 4: Run the RED tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'HardReclaim.*Replacement|PlanHardReclaimPartitionRepairsDedicatedBoundary' \
  -count=1
```

Expected: FAIL because replacement-aware symbols and behavior do not exist.

- [ ] **Step 5: Commit the RED tests**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go
git commit -m "test(qrm-cpu): reproduce dedicated reclaim replacement gap"
```

---

### Task 4: Implement Typed Failures and Replacement Solver

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go`

- [ ] **Step 1: Introduce typed selection failures**

Add the types from the design:

```go
type hardReclaimSelectionFailureReason string

const (
    hardReclaimFailureInsufficientWholeCore hardReclaimSelectionFailureReason = "insufficient_whole_core"
    hardReclaimFailureDonorFloor            hardReclaimSelectionFailureReason = "donor_floor"
    hardReclaimFailureSearchBudget          hardReclaimSelectionFailureReason = "search_budget"
)

type hardReclaimSelectionError struct {
    reason  hardReclaimSelectionFailureReason
    numaID  int
    deficit int
    cause   error
}

func (e *hardReclaimSelectionError) Error() string {
    if e.cause != nil {
        return fmt.Sprintf("%s: %v", e.reason, e.cause)
    }
    return fmt.Sprintf("%s: NUMA %d deficit %d", e.reason, e.numaID, e.deficit)
}

func (e *hardReclaimSelectionError) Unwrap() error {
    return e.cause
}
```

Preserve the existing user-visible error text in the wrapped cause.

- [ ] **Step 2: Add replacement options and proof**

```go
type hardReclaimReplacementOptions struct {
    maxCandidateStates int
    maxTerminalSolves  int
}

type hardReclaimReplacementProof struct {
    reclaimBefore machine.CPUSet
    reclaimAfter  machine.CPUSet

    dedicatedBeforeByGroup map[string]machine.CPUSet
    dedicatedAfterByGroup  map[string]machine.CPUSet
}
```

Use package constants for production budgets and lower injected budgets in
tests.

- [ ] **Step 3: Implement terminal validation**

Add:

```go
func validateHardReclaimReplacement(
    demands []partitionDemand,
    assignments map[string]machine.CPUSet,
    topology *machine.CPUTopology,
    targetByNUMA map[int]int,
) (*hardReclaimReplacementProof, error)
```

For each demand, check exact quantity, eligibility, and pairwise disjointness.
Aggregate dedicated aliases by `requestGroupKey`. For each group and NUMA,
require unchanged cardinality during replacement. Validate:

```go
protectedFloor := general.Min(oldOwned, int(math.Ceil(requestQuantity)))
if newOwned < protectedFloor {
    return nil, fmt.Errorf(
        "dedicated group %q replacement reduced ownership from %d to %d below protected floor %d",
        groupKey, oldOwned, newOwned, protectedFloor)
}
```

Call `assertCoreAligned` only for the final reclaim union.

- [ ] **Step 4: Implement bounded terminal solving**

Add:

```go
func solveHardReclaimWithReplacement(
    demands []partitionDemand,
    available machine.CPUSet,
    topology *machine.CPUTopology,
    options hardReclaimReplacementOptions,
) (map[string]machine.CPUSet, *hardReclaimReplacementProof, error)
```

Implementation sequence:

1. Derive exact `targetByNUMA`.
2. Build complete-core candidates from current reclaim, free, and dedicated.
3. Reuse deterministic candidate ordering from `coreAlignedCandidates`.
4. Enumerate terminal reclaim sets under `maxCandidateStates`.
5. Pin reclaim demands to each terminal set.
6. Solve residual dedicated demands with `solveDisjointPartitions`.
7. Validate the complete assignment.
8. Select the candidate with maximum retained ownership and minimum churn.
9. Return `hardReclaimFailureSearchBudget` when bounded search cannot prove a
   result.

Do not return a partial assignment.

- [ ] **Step 5: Run focused tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'HardReclaim.*Replacement|PlanHardReclaimPartitionRepairsDedicatedBoundary' \
  -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go
git commit -m "fix(qrm-cpu): solve dedicated reclaim replacement"
```

---

### Task 5: Integrate Exact Assignment Pinning

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go`

- [ ] **Step 1: Add the exact pin helper**

```go
func pinPartitionDemandsToAssignments(
    demands []partitionDemand,
    assignments map[string]machine.CPUSet,
) ([]partitionDemand, error) {
    pinned := append([]partitionDemand(nil), demands...)
    for i := range pinned {
        assignment, ok := assignments[pinned[i].key]
        if !ok {
            return nil, fmt.Errorf(
                "missing exact assignment for partition demand %q", pinned[i].key)
        }
        if assignment.Size() != pinned[i].quantity {
            return nil, fmt.Errorf(
                "exact assignment %q has size %d, want %d",
                pinned[i].key, assignment.Size(), pinned[i].quantity)
        }
        pinned[i].eligible = assignment.Clone()
        pinned[i].preferred = assignment.Clone()
    }
    return pinned, nil
}
```

- [ ] **Step 2: Route only replaceable failures**

Update `pinHardReclaimPartitionDemands` or add a narrow wrapper:

```go
plan, err := planHardReclaimPartition(input)
if err == nil {
    return pinFromHardReclaimPlan(demands, plan)
}

var selectionErr *hardReclaimSelectionError
if !errors.As(err, &selectionErr) ||
    (selectionErr.reason != hardReclaimFailureInsufficientWholeCore &&
        selectionErr.reason != hardReclaimFailureDonorFloor) {
    return nil, err
}

assignments, _, replacementErr := solveHardReclaimWithReplacement(
    demands, available, topology, defaultHardReclaimReplacementOptions())
if replacementErr != nil {
    return nil, fmt.Errorf(
        "hard reclaim fast path failed: %v; replacement failed: %w",
        err, replacementErr)
}
return pinPartitionDemandsToAssignments(demands, assignments)
```

- [ ] **Step 3: Add source-pool integration tests**

Add:

```go
func TestPlanDisjointAdvisorBlocksRepairsDedicatedReclaimBoundary(t *testing.T)
func TestPlanDisjointAdvisorBlocksKeepsUnreplaceableFailureClosed(t *testing.T)
func TestPlanDisjointAdvisorBlocksPreservesReplacementAssignment(t *testing.T)
```

The first test uses the production topology shape. The second preserves the
existing failure where no same-NUMA replacement exists. The third verifies the
downstream solver cannot change exact pinned assignments.

- [ ] **Step 4: Run focused tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'PlanDisjointAdvisorBlocks.*Replacement|HardReclaim.*Replacement' \
  -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go
git commit -m "fix(qrm-cpu): pin atomic dedicated reclaim assignments"
```

---

### Task 6: Add Precommit Defense

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit_test.go`

- [ ] **Step 1: Add RED tests for mutation after planning**

Add:

```go
func TestPreparePendingCPUPartitionRejectsReplacementQuantityMutation(t *testing.T)
func TestPreparePendingCPUPartitionRejectsReplacementNUMAMigration(t *testing.T)
func TestPreparePendingCPUPartitionRejectsPartialReclaimCore(t *testing.T)
```

Inject hooks that respectively:

- remove one dedicated CPU;
- move replacement ownership to another NUMA;
- leave one reclaim SMT sibling behind.

Assert revision, `PodEntries`, `MachineState`, and persisted checkpoint remain
unchanged.

- [ ] **Step 2: Add final partition validation**

Extend the existing precommit validator rather than creating another commit
path. Validate the final candidate from rebuilt `PodEntries`:

```text
reclaim is core-aligned
dedicated and reclaim are disjoint
exclusive partition coverage is exact
NUMA binding scope is unchanged
advisor demand quantities are unchanged
```

Use the frozen baseline already carried by `pendingCPUPartition`; do not read
live state during validation.

- [ ] **Step 3: Run precommit tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'PreparePendingCPUPartition.*Replacement|Precommit|PartitionValidation' \
  -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit_test.go
git commit -m "fix(qrm-cpu): validate replacement before partition commit"
```

---

### Task 7: Verify WAL and Physical Rollback

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go`

- [ ] **Step 1: Add advisor transaction integration test**

Add `TestAdvisorReplacementTransactionRetainsExactTargetAcrossRetry`. Assert:

```text
staging WAL exists before canonical CAS
canonical revision advances exactly once
active WAL retains the same response and target
new Advisor frames are blocked while physical reconciliation is pending
retry uses the same revision and target
applied marker is persisted before WAL cleanup
writer fence is released only after cleanup
```

- [ ] **Step 2: Add physical failure injection tests**

Add:

```go
func TestExecuteValidatedFrozenTraceRollsBackDedicatedReclaimReplacement(t *testing.T)
func TestExecuteValidatedFrozenTraceReportsReplacementRollbackFailure(t *testing.T)
```

The forward trace must contain both ownership removals and additions. Inject a
failure after at least one drain and one expand. Assert successful rollback
restores every touched cgroup to the before image.

- [ ] **Step 3: Run transaction and topology tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'AdvisorReplacementTransaction|WAL|WriterFence' \
  -count=1

go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'DedicatedReclaimReplacement|FrozenTrace|Rollback' \
  -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go
git commit -m "test(qrm-cpu): verify replacement recovery boundaries"
```

---

### Task 8: Run Regression and Race Gates

**Files:**

- No production changes expected.

- [ ] **Step 1: Run focused tests repeatedly**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Admission.*Reclaim|DeriveSteadyReclaimFloor|HardReclaim.*Replacement|Advisor.*Replacement' \
  -count=20
```

Expected: PASS for all 20 runs.

- [ ] **Step 2: Run the full dynamic policy package**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
```

Expected: PASS.

- [ ] **Step 3: Run focused race tests**

```bash
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'HardReclaim.*Replacement|Advisor.*Replacement|PreparePendingCPUPartition' \
  -count=1
```

Expected: PASS with no race report.

- [ ] **Step 4: Run Bulkhead topology tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Trace|Rollback|ParentSafe|Frozen' \
  -count=1
```

Expected: PASS.

- [ ] **Step 5: Check repository hygiene**

```bash
git diff --check
git status --short
git diff -- go.mod go.sum
```

Expected:

- no whitespace errors;
- only planned files are modified;
- no `go.mod` or `go.sum` changes.

---

### Task 9: Build and Host Verification

**Files:**

- No repository changes expected.
- Store build scripts and temporary logs outside the repository worktree.

- [ ] **Step 1: Build the Linux/amd64 CGO agent**

Use the dedicated x86_64 Linux build machine and preserve the adapter/core
relative `replace` layout.

```bash
export PATH="$HOME/toolchain/go1.18/bin:$PATH"
export GO111MODULE=on CGO_ENABLED=1 GOOS=linux GOARCH=amd64
go build -v -o ./output/agent ./cmd/katalyst-agent/main.go
```

- [ ] **Step 2: Verify the artifact**

```bash
file ./output/agent
nm ./output/agent | grep -c cgo
ldd ./output/agent | grep -i 'pci\|libc'
sha256sum ./output/agent
```

Expected:

- ELF 64-bit x86-64;
- dynamically linked;
- cgo symbols present;
- `libpci.so.3` and `libc.so.6` resolved;
- SHA recorded for every transfer hop.

- [ ] **Step 3: Deploy with backup and identity checks**

On the selected cgroup v1 validation node:

```text
backup current agent and real_run.sh
copy the verified binary into the QRM mount namespace
verify SHA inside and outside the namespace
restart only QRM and SysAdvisor as required
verify PID, command line, environment, healthz, and checkpoint revision
```

- [ ] **Step 4: Verify the production-shaped steady repair**

Capture evidence that:

```text
NUMA 2 reclaim becomes four complete physical cores
NUMA 3 reclaim becomes four complete physical cores
dedicated quantities remain unchanged
no cross-NUMA movement occurs
generateBlockCPUSet no longer reports "needs 2 more reclaim CPUs"
```

- [ ] **Step 5: Verify new admission degradation**

Admit an exclusive DNB fixture whose soft reclaim target is not exactly
representable by complete cores. Capture:

```text
requested reclaim quantity
actual down-rounded reclaim quantity
over-allocated DNB quantity
exact DNB/reclaim coverage
whole-core reclaim proof
```

- [ ] **Step 6: Run the existing eight-stage churn E2E**

Run:

```text
reset
target setup
three standard rounds
five high/overlap churn rounds
final reset
```

Expected:

- every stage returns `rc=0`;
- no `generateBlockCPUSet failed`;
- no partial physical state after failure injection;
- WAL and writer fence are clean after convergence.

- [ ] **Step 7: Record delivery evidence**

Record:

- source commits;
- build host and artifact SHA;
- node identity and cgroup mode;
- focused, repeated, package, race, and E2E results;
- before/after CPUSet topology;
- rollback injection result;
- final repository status and `go.mod` purity.

Do not commit host logs or temporary build artifacts.
