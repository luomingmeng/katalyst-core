# NUMA-Scoped Target-Driven Dedicated–Reclaim Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make target-driven dedicated/shared advisor blocks obey their exact per-domain quantities, including targets below request and previous ownership, while preserving real-NUMA/FakeNUMA locality, whole-core reclaim, legacy floors, bounded search, and atomic state application.

**Architecture:** Reuse the existing runtime dynamic-configuration triple
`EnableReclaim && EnableRampUpReclaimHardPartition &&
DisableDedicatedCoresOverlapReclaimedCores` as the gate. QRM treats the advisor
block `quantity` for a proven single real-NUMA source as a retained floor
(target), allows the dedicated pool to shrink below old ownership and lend the
delta to local reclaim, and validates a complete assignment before the
existing WAL/CAS/physical-apply transaction. Legacy and non-qualifying blocks
continue through the complete existing floor and ownership path. No proto
fields, no new FeatureGate, no new wire fields.

**Tech Stack:** Go, Kubernetes CPUSet, min-cost flow partition solver, table-driven tests, Go race detector, native Linux/amd64 CGO build.

> **Historical note.** An earlier revision of this plan planned a
> `negotiated-v1` wire protocol extension (new proto enums/fields, a new
> mutually-supported FeatureGate `feature_gate_numa_scoped_mixable_reclaim_v1`,
> and SysAdvisor annotation of `block.mixability` / `source_key` /
> `reclaim_source_quota`). That approach was **not adopted**. The shipped
> implementation derives the same in-memory domain and target accounting from
> existing block descriptors and the existing runtime gate triple. See the
> spec's
> [Historical design: negotiated protocol extension (deprecated, not implemented)](../../specs/2026-09-24-numa-scoped-target-driven-dedicated-reclaim-design.md#historical-design-negotiated-protocol-extension-deprecated-not-implemented).
> Older plan steps referencing `cpu.proto`, `cpu.pb.go`, the feature-gate
> finder, or SysAdvisor wire annotation are superseded by this plan.

---

## Preconditions

- The implementation must run in a dedicated clean worktree created from
  `feat/fake-numa-reclaim-balance`.
- Do not import the unrelated dirty topology/replan changes currently present
  in the source worktree.
- Use the adapter branch and core revision selected for the final canary
  build; record both revisions and the Linux binary SHA256.
- Every behavior-changing task uses RED-first development and ends with a
  single logical commit. The production-fixture task first commits a passing
  characterization of the current failure; the next behavior-changing task
  flips it to the desired assertion and observes RED before implementation.
- After every implementation task, run a specification review and a
  code-quality review before starting the next task.
- Focused tests, race tests, vet, and the Linux build run with Go 1.18.x.
  Stop if `go version` reports another major/minor version.
- **Do not modify `cpu.proto`, `cpu.pb.go`, or add a new
  `featuregatenegotiation` finder.** The gate is the existing runtime dynamic
  configuration triple.

## File Map

### New test files

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_numa_donation_test.go`
  - Four-NUMA production-shaped fixture; asserts the planner shrinks NUMA 0 /
    NUMA 1 to the frozen advisor targets and lends the delta to local
    reclaim.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_donor_test.go`
  - Donor-level accounting: frozen target honored, per-source quota not
    exceeded, legacy donors not consumed by target-driven reclaim.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_chain_test.go`
  - Planner chain: gate-triple gating, proven-single-source detection,
    FakeNUMA exclusion, alias / sidecar counting, mixed legacy+target-driven
    group behavior.

### Modified files

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go`
  - Uses exact targets for target-driven donors, computes
    `oldOwned - target` as the donation limit, and validates target ownership
    in the replacement proof.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go`
  - Retains all legacy regressions.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache.go`
  - Includes domain/target-driven identity in residual signatures.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache_test.go`
  - Proves cache isolation across domains and target-driven/legacy semantics.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner.go`
  - Builds domain-aware descriptors without changing raw quantities;
    derives the `targetDriven` bool from the runtime gate triple plus
    proven single-source identity on a real NUMA.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner_test.go`
  - Covers descriptor canonicalization and raw-target freezing.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go`
  - Carries domain/target-driven metadata on demands and validates
    domain-aware assignment.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver_test.go`
  - Covers domain-aware validation and assignment.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go`
  - Keeps real-NUMA and FakeNUMA capacity ownership separate.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go`
  - Covers mixed-domain final assignment and source quota ownership.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit.go`
  - Freezes target/domain/target-driven snapshot digest; rejects mutation
    between planning and precommit.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit_test.go`
  - Covers frozen target/domain mutation and precommit rejection.

### Explicitly NOT touched

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor/cpu.proto`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor/cpu.pb.go`
- `pkg/agent/utilcomponent/featuregatenegotiation/**`
- `pkg/agent/sysadvisor/plugin/qosaware/server/**` publication path

These were the abandoned `negotiated-v1` design; do not reintroduce them.

## Task 1: Characterize the production regression

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_numa_donation_test.go`

- [ ] **Step 1: Add the production-shaped fixture**

Build 128 logical CPUs with SMT sibling pairs `(0,64)` through `(63,127)` and
four NUMA nodes. Use the exact before sets:

```go
reclaimBefore := machine.MustParse("0,16,32-35,48-51,64,80,96-99,112-115")
dedicatedA    := machine.MustParse("1-15,17-31,65-79,81-95")
dedicatedB    := machine.MustParse("36-47,52-63,100-111,116-127")
```

The fixture mirrors a four-NUMA production-shaped node (4 NUMA, 32 logical
CPUs each, SMT2) with the documented pre-fix checkpoint.

- [ ] **Step 2: Define the two scenarios**

```go
// scenario: shrinks NUMA 0
dedicatedTargets := map[int]int{0: 28, 1: 30, 2: 24, 3: 24}
reclaimTargets   := map[int]int{0: 4, 1: 2, 2: 8, 3: 8}

// scenario: shrinks NUMA 1 (targets swapped on NUMA 0/1)
dedicatedTargets := map[int]int{0: 30, 1: 28, 2: 24, 3: 24}
reclaimTargets   := map[int]int{0: 2, 1: 4, 2: 8, 3: 8}
```

- [ ] **Step 3: Add passing characterization tests of the current failure**

```go
func TestTargetDrivenNUMADonationShrinksNUMA0(t *testing.T) {
    // Before the fix: planner rejects with
    // "no global hard reclaim replacement is feasible".
    // After the fix: dedicated NUMA 0 == 28, reclaim NUMA 0 == 4,
    // dedicated NUMA 1 == 30, reclaim NUMA 1 == 2.
}

func TestTargetDrivenNUMADonationShrinksNUMA1(t *testing.T) {
    // Swapped targets; symmetric outcome.
}
```

- [ ] **Step 4: Add legacy control cases**

Clone the same demands with the gate triple off, or with the block not a
proven single source on one real NUMA. Assert the existing donor-floor /
ownership rejection remains.

- [ ] **Step 5: Run the current-behavior characterization**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestTargetDrivenNUMADonation' -count=1
```

Expected: characterization passes against the old implementation (it records
the observed rejection), and fails once the planner is flipped to the new
assertion.

- [ ] **Step 6: Commit RED tests**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_numa_donation_test.go
git commit -m "test(qrm-cpu): reproduce target-driven NUMA reclaim failure"
```

## Task 2: Make the fast path target-aware, gated on the runtime triple

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_numa_donation_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_donor_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go`

- [ ] **Step 1: Derive the `targetDriven` flag per demand**

In the block planner, a demand is `targetDriven` only when all of:

```
conf.EnableReclaim
conf.EnableRampUpReclaimHardPartition
conf.DisableDedicatedCoresOverlapReclaimedCores
block is on a real NUMA (not FakedNUMAID)
block can be proven a single canonical source on that NUMA
```

Disjoint block IDs on the same NUMA that do not share a proven allocation
identity, FakeNUMA blocks, and reserve / isolation / system pools are
`targetDriven=false`. Do not infer target-driven from QoS class names.

- [ ] **Step 2: Extend the donor representation**

```go
type hardReclaimPartitionDonor struct {
    key             string
    groupKey        string
    domain          partitionDomain
    cpus            machine.CPUSet
    target          int
    requestQuantity float64
    targetDriven    bool
}
```

- [ ] **Step 3: Split donation accounting by semantics**

Build two maps:

```go
legacyGroupMinimum  := map[string]int{}
legacyGroupCPUs     := map[string]machine.CPUSet{}
targetDrivenTarget  := map[partitionTargetKey]int{}
targetDrivenOwned    := map[partitionTargetKey]machine.CPUSet{}
```

For legacy donors, preserve the existing request-floor computation:

```go
legacyGroupMinimum[groupKey] = general.Max(
    legacyGroupMinimum[groupKey],
    int(math.Ceil(donor.requestQuantity)),
)
```

For target-driven donors:

```go
key := partitionTargetKey{groupKey: donor.groupKey, domain: donor.domain}
targetDrivenTarget[key] += donor.target
targetDrivenOwned[key] = targetDrivenOwned[key].Union(donor.cpus)

limit := targetDrivenOwned[key].Size() - targetDrivenTarget[key]
if limit < 0 {
    limit = 0
}
```

Track donation per canonical source. A candidate that exceeds any source's
limit is rejected even if its group-level total is within limit.

- [ ] **Step 4: Restrict donor CPUs to the reclaim domain**

When constructing a real-NUMA candidate, skip target-driven donors whose
domain is FakeNUMA or whose real NUMA id differs from the reclaim target's
NUMA. Never add FakeNUMA donor CPUs to a real-NUMA candidate.

- [ ] **Step 5: Add donor-level RED tests**

```go
func TestPlanHardReclaimPartitionTargetDrivenUsesFrozenTargetAndQuota(t *testing.T)
func TestPlanHardReclaimPartitionTargetDrivenCannotDonateBeyondQuota(t *testing.T)
func TestPlanHardReclaimPartitionLegacyDonorNotConsumedByTargetDrivenReclaim(t *testing.T)
```

The over-donation case: same NUMA, source A old=20/target=20, source B
old=10/target=8, reclaim old=2/target=4. Only source B may lend two CPUs;
source A must remain at 20.

- [ ] **Step 6: Run RED then GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test(TargetDrivenNUMADonation|PlanHardReclaimPartitionTargetDriven|PlanHardReclaimPartitionLegacyDonor)' \
  -count=1
```

Expected: RED on first run (donors have no target/domain/targetDriven), GREEN
after the planner change. Legacy tests must remain green.

- [ ] **Step 7: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_numa_donation_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_donor_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go
git commit -m "fix(qrm-cpu): honor target-driven donor frozen targets"
```

## Task 3: Validate replacement against frozen targets

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_donor_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache_test.go`

- [ ] **Step 1: Add proof RED tests**

```go
func TestReplacementProof_TargetDrivenDedicatedShrinksToFrozenTarget(t *testing.T)
func TestReplacementProof_LegacyGroupStillEnforcesBeforeAfter(t *testing.T)
func TestReplacementProof_RejectsCrossDomainTarget(t *testing.T)
func TestReplacementProof_RejectsSourceQuotaSwap(t *testing.T)
```

The positive case uses old `30`, request `62`, target `28`, final `28`. The
negative source case keeps the same NUMA aggregate but lets source A pay
source B's quota; it must fail.

- [ ] **Step 2: Extend the proof**

```go
type hardReclaimReplacementProof struct {
    reclaimBefore machine.CPUSet
    reclaimAfter  machine.CPUSet

    dedicatedBeforeByGroup map[string]machine.CPUSet
    dedicatedAfterByGroup   map[string]machine.CPUSet

    dedicatedTargetByGroupDomain map[partitionTargetKey]int
    sharedTargetByGroupDomain    map[partitionTargetKey]int
    reclaimTargetByDomain        map[partitionDomain]int

    partialBeforeCores int
}
```

- [ ] **Step 3: Replace only the target-driven equality**

For target-driven groups, compare assigned to frozen target:

```go
for key, target := range proof.dedicatedTargetByGroupDomain {
    if got := assignedDedicatedByGroupDomain(assignments, demands)[key]; got != target {
        return nil, fmt.Errorf(
            "target-driven dedicated target mismatch: group=%q domain=%q got=%d target=%d",
            key.groupKey, key.domain.key(), got, target)
    }
}
```

Keep the old `before == after` and request-floor checks for legacy groups.

- [ ] **Step 4: Preserve target fields through residual solving and pinning**

`pinPartitionDemandsToAssignments` changes only `eligible` and `preferred`.
Assert quantity, domain, targetDriven, and source identity are unchanged
before and after pinning.

- [ ] **Step 5: Run the production fixture and replacement suites**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestTargetDrivenNUMADonation|TestReplacementProof|HardReclaim.*Replacement' \
  -count=1

go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestTargetDrivenNUMADonation|HardReclaim.*Replacement' \
  -count=20
```

Expected: both scenarios pass; legacy and existing replacement tests remain
unchanged; repeated runs are deterministic.

- [ ] **Step 6: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_donor_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache_test.go
git commit -m "fix(qrm-cpu): validate target-driven replacement targets"
```

## Task 4: Gate chain, alias, and FakeNUMA isolation

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_chain_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit_test.go`

- [ ] **Step 1: Gate-chain RED tests**

```go
func TestPerNUMADegradationIsolatesAmbiguousNUMAs(t *testing.T)
func TestDistinctBlockIDsOnSameNUMAStayLegacy(t *testing.T)
func TestSameBlockIDSidecarCountsAsSingleSource(t *testing.T)
func TestPinDerivesTargetDrivenReclaimNUMAFromDedicatedDonor(t *testing.T)
func TestPinTargetDrivenDonorSatisfiesReclaim(t *testing.T)
func TestMixedPolicyGroupPerNUMAImmunity(t *testing.T)
func TestMixedPolicyGroupLegacyNUMABeforeAfterEnforced(t *testing.T)
func TestAllLegacyGroupKeepsBeforeAfter(t *testing.T)
func TestFakeNUMAExcludedFromGateUpgrade(t *testing.T)
```

Cover: one gate off → legacy; disjoint block IDs on the same NUMA → legacy;
`mps` + main container sharing one block ID → counted once; FakeNUMA →
never upgraded.

- [ ] **Step 2: Mixed-domain integration tests**

Construct a response with real NUMA 0 (dedicated target=28, reclaim=4) and
FakeNUMA (shared target=20, reclaim=6). Assert final assignments are disjoint,
real reclaim stays on NUMA 0, FakeNUMA reclaim is not bound to a real NUMA,
and no capacity is borrowed across domains. Add negative cases where only
FakeNUMA capacity could satisfy a real-NUMA target (and vice versa); expect
typed `no_feasible`.

- [ ] **Step 3: Precommit target digest**

The precommit snapshot must include canonical encodings of block quantity,
domain kind/id, source identity, and the `targetDriven` flag. Mutating any
field between planning and precommit returns a `stale_target` error before
WAL staging or state CAS.

- [ ] **Step 4: Stage-specific transaction tests**

```go
func TestTargetDrivenPartition_SolverFailureHasNoSideEffects(t *testing.T)
func TestTargetDrivenPartition_CASConflictDoesNotApplyPhysicalState(t *testing.T)
func TestTargetDrivenPartition_PostCASWriteFailureRollsBackPrefix(t *testing.T)
func TestTargetDrivenPartition_WriterFenceRejectsCompetingFrame(t *testing.T)
func TestTargetDrivenPartition_SuccessAdvancesRevisionOnce(t *testing.T)
func TestTargetDrivenPartition_StagesWALBeforeCAS(t *testing.T)
func TestTargetDrivenPartition_RetryKeepsCanonicalTarget(t *testing.T)
func TestTargetDrivenPartition_TargetDigestRejectsMutation(t *testing.T)
```

Do not assert zero physical writes after a post-CAS injected write failure.
Assert the written prefix is rolled back and WAL/retry target ownership remains
recoverable.

- [ ] **Step 5: Run integration tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test(TargetDriven|MixedRealAndFakeNUMA|PartitionPrecommit)' \
  -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_chain_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit_test.go
git commit -m "fix(qrm-cpu): gate chain, alias, and domain isolation"
```

## Task 5: Diagnostics, verification, and canary

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_donor_test.go`

- [ ] **Step 1: Emit bounded diagnostics**

One summary record per solve and one record per failed domain. Fields:

```
domainKind
domainID
sourceKey
groupKey
requestQuantity
oldPreferredSize
advisorTargetSize
finalAssignedSize
reclaimSourceQuota
oldReclaimSize
reclaimTargetSize
finalReclaimSize
generatedCandidateStates
deduplicatedCandidateStates
terminalStates
residualCacheHits
residualCacheMisses
maxAssignmentEdgesInGraph
flowOperations
complete
outcome
```

`outcome` is one of:

```
selected
no_feasible
graph_budget
search_budget
stale_target
invalid_domain
whole_core_infeasible
```

Do not log under-provision as `donor_floor` merely because
`RequestQuantity > advisorTargetSize`.

- [ ] **Step 2: Run focused, repeated, package, and race gates**

```bash
go version
# Expected: go version go1.18.x ...

go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/... -count=1

go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'HardReclaim.*TargetDriven|HardReclaim.*Replacement|Advisor.*NUMA' \
  -count=1

go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'HardReclaim.*TargetDriven|HardReclaim.*Replacement|Advisor.*NUMA' \
  -count=20

go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1

go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'HardReclaim.*TargetDriven|HardReclaim.*Replacement' \
  -count=1

go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/... -count=1
go vet ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
BASE=$(git merge-base HEAD feat/fake-numa-reclaim-balance)
gofmt -w $(git diff --name-only --diff-filter=ACM "$BASE"...HEAD -- '*.go')
git diff --check
```

Expected: every command exits `0`; repeated tests show no order-dependent
assignment.

- [ ] **Step 3: Benchmark the production fixture**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^$' -bench 'BenchmarkHardReclaimTargetDriven' \
  -benchmem -count=5
```

Record median wall time, allocations, candidate states, terminal solves, cache
hits/misses, maximum graph edges, and flow operations. Compare against the
advisor loop latency budget; do not use the old `≈343000` operation count
alone as a failure criterion.

- [ ] **Step 4: Commit diagnostics and verification tests**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_block_planner.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_target_driven_donor_test.go
git commit -m "test(qrm-cpu): verify target-driven reclaim convergence"
```

- [ ] **Step 5: Build the native Linux CGO binary**

Use the dedicated x86_64 Linux build host. Build the selected adapter/core
pair and verify:

```text
ELF 64-bit LSB executable, x86-64
CGO linked
libpci.so.3 present
libc.so.6 present
adapter revision recorded
core revision recorded
SHA256 recorded
```

- [ ] **Step 6: Run passive canary**

On the selected production-shaped node:

1. Record QRM and SysAdvisor PID, executable SHA, start time, command line,
   and health.
2. Confirm the runtime dynamic-configuration triple
   (`EnableReclaim`, `EnableRampUpReclaimHardPartition`,
   `DisableDedicatedCoresOverlapReclaimedCores`) is on.
3. Replace one component instance at a time according to the approved canary
   procedure.
4. Capture one same-revision raw advisor response, QRM checkpoint, solver
   diagnostics, and cgroup state.
5. Observe existing advisor rounds without creating traffic.
6. Confirm exact per-source quantities, real-NUMA locality, FakeNUMA identity,
   whole-core reclaim, stable PID, and continuous health.
7. Confirm no recurring `needs 2 more reclaim CPUs`, `no global hard reclaim
   replacement is feasible`, panic, partial checkpoint, or rollback residue.

- [ ] **Step 7: Final review and logical squash**

Verify:

```bash
git status --short
git log --oneline --decorate -n 20
BASE=$(git merge-base HEAD feat/fake-numa-reclaim-balance)
git diff "$BASE"...HEAD --check
```

Keep documentation, planner behavior, replacement proof, gate chain /
FakeNUMA isolation, and verification as separate logical commits. Squash only
fixup commits inside the same logical unit.

## Review Gates

After each task:

1. Specification review against
   `docs/superpowers/specs/2026-09-24-numa-scoped-target-driven-dedicated-reclaim-design.md`.
2. Code-quality review for typed errors, deterministic ordering, alias
   canonicalization, domain leakage, budget accounting, and transaction side
   effects.
3. Verify no unrelated dirty file entered the commit.
4. Verify target-driven behavior is gated only by the existing runtime
   dynamic-configuration triple; no new proto field, wire field, or FeatureGate
   was added.

Stop implementation if any review finds:

* target-driven eligibility inferred only from QoS class;
* a legacy block (gate triple off, or not a proven single real-NUMA source)
  entering the target-driven shrink path;
* raw advisor quantity changed by normalization;
* real-NUMA and FakeNUMA capacity borrowed across domains;
* one source paying another source's quota;
* `RequestQuantity` used as a target-driven feasibility floor;
* previous ownership used as a target-driven equality target;
* whole-core alignment weakened;
* incomplete search reported as semantic infeasibility;
* solver or validation failure mutating canonical state;
* post-CAS failure described as a generic zero-write outcome;
* any change to `cpu.proto`, `cpu.pb.go`, or `featuregatenegotiation/**`
  (the abandoned `negotiated-v1` path).
