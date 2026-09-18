# Scoped Pending Protection for All QoS Classes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace global unresolved-pending CPU propagation with scope-owned ParentSafe closure for ordinary shared, SNB, and DNB admissions, then make operation-heavy projection bounded and observable.

**Architecture:** The cpuset-topology plugin resolves every pending Pod to its expected relative cgroup path even before the path exists. The topology package maps that scope to controlled primary ancestors, derives the only required admission operations from that closure, proves every scoped ancestor before publication, and compiles the frozen trace with deterministic projection progress guards and frontier-level evidence settlement.

**Tech Stack:** Go 1.18.10, Kubernetes CPUSet and Pod QoS utilities, cgroup v1/v2 relative-path handlers, existing topology DAG/planner/frozen-trace abstractions, testify, Linux amd64 CGO, qrm-bulkhead E2E scripts.

---

## Baseline and Constraints

Design:

```text
docs/superpowers/specs/2026-09-18-scoped-pending-protection-all-qos-design.md
```

Design commit:

```text
08e96b3fd docs(qrm-cpu): design scoped pending protection
```

The worktree also contains nine verified but uncommitted code files from the
previous live-E2E repair. Task 0 separates them into three code commits before
new implementation begins.

Required non-edits:

- do not increase the five-second admission deadline;
- do not weaken full-convergence exact-match;
- do not skip whole-trace preflight;
- do not reduce inverse rollback reservation;
- do not retain global all-primary widening as a fallback;
- do not add a second pending-protection owner;
- do not modify checkpoint schemas;
- do not place the scale gate in default package tests.

## File Map

### New files

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/pending_protection.go`
  - canonical pending-protection type, validation, union, and ancestor closure.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/pending_protection_test.go`
  - scope validation and ancestor-closure contracts.

### Modified topology files

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
  - replace global pending input, populate scoped round state, preserve deadline causes.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/target_normalization.go`
  - widen only controlled ancestors of each pending scope.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/target_normalization_test.go`
  - all-QoS scoped normalization cases.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan.go`
  - classify required admission operations by scoped rel ownership.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan_test.go`
  - unrelated sibling exclusion and multiple-scope closure.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/convergence.go`
  - scoped pending ancestor deficits in ParentSafety.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/convergence_test.go`
  - fail-closed scoped ancestor proof tests.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy.go`
  - frontier-level evidence settlement.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy_test.go`
  - one evidence rebuild per frontier and unchanged-snapshot behavior.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go`
  - projection stage diagnostics and cycle/no-progress guards.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go`
  - live-shaped RED case and projection guards.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_benchmark_test.go`
  - operation-heavy linearity gate.

### Modified plugin files

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
  - retain expected Pod rel for missing cgroups and emit scoped protections.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`
  - ordinary shared, SNB, DNB, missing-path, and unknown-scope tests.

## Task 0: Freeze the Verified Live-E2E Baseline

**Files:**

- Modify: the existing nine dirty code files reported by `git status --short`.

- [ ] **Step 1: Re-run focused tests for the three existing repair groups**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run 'TestParentSafeFinalization|TestFullConvergenceFinalization' -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestTracePreflightRejectsInitialSnapshotDrift|TestFinalSnapshotDrift|TestFixedPointEngineFailsClosedWhenAllTransferCPUsRemainProtected|TestProtectedTransferStall' \
  -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestAdmissionRetries|TestAdmissionFrozen|TestAdmissionDoesNotRetry' -count=1
```

Expected: all commands pass.

- [ ] **Step 2: Commit ParentSafe dynamic-leaf finalization**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/test_only_helpers_test.go
git commit -m "fix(qrm-cpu): allow proved parent-safe leaf supersets"
```

- [ ] **Step 3: Commit verified frozen-snapshot replan**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go
git commit -m "fix(qrm-cpu): replan verified frozen snapshot drift"
```

- [ ] **Step 4: Commit the protected-transfer stall guard**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go
git commit -m "fix(qrm-cpu): fail closed on protected transfer stalls"
```

- [ ] **Step 5: Verify a clean code baseline**

```bash
git status --short
```

Expected: no modified Go file. The design and baseline code commits remain
separate.

## Task 1: Lock the Scoped Pending Contract with RED Tests

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/pending_protection_test.go`

- [ ] **Step 1: Add the canonical contract tests**

```go
func TestPendingProtectionClosureScopesCPUsToControlledAncestors(t *testing.T) {
    dag := mustBuildPendingProtectionDAG(t)
    protections := []PendingProtection{{
        ScopeRel: "kubepods/burstable/pod-a",
        CPUs:     machine.NewCPUSet(2, 3),
        PodUID:   "pod-a",
    }}

    required, err := pendingRequiredCPUSetByRel(dag, protections)

    require.NoError(t, err)
    require.Equal(t, machine.NewCPUSet(2, 3), required["kubepods"])
    require.Equal(t, machine.NewCPUSet(2, 3), required["kubepods/burstable"])
    require.NotContains(t, required, "kubepods/besteffort")
    require.NotContains(t, required, "kubepods/burstable/pod-old")
}

func TestPendingProtectionClosureRejectsUnknownScope(t *testing.T) {
    dag := mustBuildPendingProtectionDAG(t)

    _, err := pendingRequiredCPUSetByRel(dag, []PendingProtection{{
        ScopeRel: "unmanaged/pod-a",
        CPUs:     machine.NewCPUSet(2, 3),
        PodUID:   "pod-a",
    }})

    require.ErrorIs(t, err, ErrPendingProtectionScopeUnknown)
}
```

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestPendingProtectionClosure' \
  -count=1
```

Expected: build failure because `PendingProtection`,
`ErrPendingProtectionScopeUnknown`, and `pendingRequiredCPUSetByRel` do not
exist.

- [ ] **Step 3: Commit RED**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/pending_protection_test.go
git commit -m "test(qrm-cpu): define scoped pending protection"
```

## Task 2: Implement the Canonical Scoped Pending Owner

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/pending_protection.go`

- [ ] **Step 1: Add the type and fail-closed validation**

```go
var ErrPendingProtectionScopeUnknown = errors.New(
    "pending protection scope is outside controlled primary topology")

type PendingProtectionSource string

const (
    PendingProtectionSourceExistingPod PendingProtectionSource = "existing_pod"
    PendingProtectionSourceExpectedPod PendingProtectionSource = "expected_pod"
)

type PendingProtection struct {
    ScopeRel string
    CPUs     machine.CPUSet
    PodUID   string
    Source   PendingProtectionSource
}
```

Implement `pendingRequiredCPUSetByRel` by canonicalizing every scope, rejecting
empty CPU sets and unsafe paths, and selecting every primary DAG node for which
`scope == node.Rel` or `scope` is below `node.Rel`.

- [ ] **Step 2: Add canonical derivation helpers**

Add:

```go
func pendingProtectionUnion(protections []PendingProtection) machine.CPUSet {
    out := machine.NewCPUSet()
    for _, protection := range protections {
        out = out.Union(protection.CPUs)
    }
    return out
}
```

Keep this task additive. Do not add `PendingProtections` to `CoordinatorInput`
until the atomic cutover task.

- [ ] **Step 3: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestPendingProtectionClosure' \
  -count=1
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/pending_protection.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/pending_protection_test.go
git commit -m "feat(qrm-cpu): scope pending protection by cgroup ancestry"
```

## Task 3: Resolve Missing Pod Paths for Every QoS Mode

**Files:**

- Modify: `pkg/util/cgroup/common/path.go`
- Modify: `pkg/util/cgroup/common/path_filter_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/dag.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/dag_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`

- [ ] **Step 1: Add RED resolver tests**

Add a pure common API that expands the configured Kubernetes roots into:

```text
kubepods/burstable/pod-shared
kubepods/burstable/pod-snb
kubepods/pod-dnb
```

It must not inspect the filesystem. Build the topology DAG, select the unique
candidate with the deepest controlled-primary ancestor, and assert that
`pendingProtections` returns the expected `ScopeRel`, CPU set, Pod UID, and
`PendingProtectionSourceExpectedPod` while every candidate path is absent.
Cover the production primary-root shape (`kubepods`) with all configured
Kubernetes QoS roots present: native Guaranteed must resolve only the direct
root candidate, while Burstable and BestEffort select their canonical roots.

Add no-match and equal-depth ambiguity cases and assert deterministic
fail-closed errors.

- [ ] **Step 2: Verify RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run 'TestPendingProtectionScope' -count=1
```

Expected: build failure because the scoped resolver is absent.

- [ ] **Step 3: Implement expected-path retention**

Add:

```go
func GetPodRelativeCgroupPathCandidates(podUID string) []string

func GetPodRelativeCgroupPathCandidatesForQOS(
    podUID string,
    qosClass v1.PodQOSClass,
) []string

func (d *TopoDAG) SelectUniqueControlledPrimaryCandidate(
    candidates []string,
) (string, error)
```

For each pending Pod:

1. retain a previously proved cached relation when present;
2. otherwise use native Kubernetes QoS retained in allocation metadata from the
   kubelet request, falling back to the cached Pod specification only when it is
   already visible, and generate candidates from the corresponding canonical
   configured root; unknown QoS retains all roots and therefore cannot bypass
   ambiguity checks;
3. after DAG construction, select the sole candidate under the deepest
   controlled-primary ancestor;
4. fail closed on no match or ambiguity;
5. use `ExistingPod` when physical read-back succeeds;
6. use `ExpectedPod` when the selected Pod path is not materialized.

Deduplicate by `(ScopeRel, PodUID)` and union CPUs for repeated containers.

- [ ] **Step 4: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run 'TestPendingProtectionScope' -count=1
```

Expected: pass for ordinary shared, SNB, and DNB cases.

- [ ] **Step 5: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go
git commit -m "fix(qrm-cpu): retain expected pending pod scopes"
```

## Task 4: Make Admission Closure Scope-Owned

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan_test.go`

- [ ] **Step 1: Add RED closure tests**

Build a drain plan containing:

```text
kubepods grow 0-7
kubepods/burstable grow 0-7
kubepods/besteffort grow 0-7
kubepods/burstable/pod-old grow 0-7
```

Set:

```go
PendingRequiredByRel: map[string]machine.CPUSet{
    "kubepods":           machine.NewCPUSet(6, 7),
    "kubepods/burstable": machine.NewCPUSet(6, 7),
}
```

Assert that only `kubepods` and `kubepods/burstable`, plus their explicit
ordered dependencies, appear in the required plan.

- [ ] **Step 2: Verify RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestSplitPlanForAdmissionScopesPendingGrow' -count=1
```

Expected: failure because the existing global CPU-intersection rule marks
unrelated grows as required.

- [ ] **Step 3: Replace global classification**

Add:

```go
PendingRequiredByRel map[string]machine.CPUSet
```

to `AdmissionSafetyInput`. A grow is pending-required only when:

```go
required, ok := in.PendingRequiredByRel[operation.Rel]
ok && required.Intersection(
    operation.Target.CPUs.Difference(current.ConfiguredCPUs),
).Equals(required.Intersection(operation.Target.CPUs))
```

Retain hard-floor, shrink, and dependency closure rules. Delete the branch that
marks any grow required solely because it intersects the global pending union.

- [ ] **Step 4: Run GREEN and regressions**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestSplitPlanForAdmission' -count=1
```

Expected: all admission split tests pass.

- [ ] **Step 5: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan_test.go
git commit -m "fix(qrm-cpu): bind admission grows to pending scopes"
```

## Task 5: Prove Every Scoped Ancestor Before ParentSafe Publication

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/convergence.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/convergence_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`

- [ ] **Step 1: Add RED ParentSafety tests**

```go
func TestEvaluateParentSafetyRejectsScopedAncestorDeficit(t *testing.T) {
    report := evaluateParentSafety(ParentSafetyInput{
        Snapshot: snapshotWith(
            entry("kubepods", "0-7"),
            entry("kubepods/burstable", "0-5"),
        ),
        PendingRequiredByRel: map[string]machine.CPUSet{
            "kubepods":           machine.NewCPUSet(6, 7),
            "kubepods/burstable": machine.NewCPUSet(6, 7),
        },
    })

    require.False(t, report.Safe)
    require.Equal(t, machine.NewCPUSet(6, 7),
        report.PendingScopeDeficit["kubepods/burstable"])
}
```

Add a passing case where both ancestors contain CPUs 6-7 and reclaim excludes
them.

- [ ] **Step 2: Verify RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestEvaluateParentSafety.*Scoped' -count=1
```

Expected: build failure because `PendingScopeDeficit` is absent.

- [ ] **Step 3: Implement scoped proof**

Add:

```go
PendingScopeDeficit map[string]machine.CPUSet
```

to `ParentSafetyReport`. For every required rel, calculate:

```go
deficit := required.Difference(snapshot.Entries[rel].CPUs)
```

Missing entries, identity mismatch, or non-empty deficits make `Safe` false.
Keep the primary-domain, reclaim-domain, overlap, and required-floor proofs.

- [ ] **Step 4: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestEvaluateParentSafety' -count=1
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/convergence.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/convergence_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go
git commit -m "feat(qrm-cpu): prove scoped pending ancestors"
```

## Task 6: Cut Over Atomically and Delete the Global Owner

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/target_normalization.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/target_normalization_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/writer_test.go`

- [ ] **Step 1: Add all-QoS normalization RED cases**

Create table cases:

```go
[]struct {
    name       string
    scopeRel   string
    wantRels   []string
    rejectRels []string
}{
    {
        name: "ordinary shared burstable",
        scopeRel: "kubepods/burstable/pod-shared",
        wantRels: []string{"kubepods", "kubepods/burstable"},
        rejectRels: []string{"kubepods/besteffort", "kubepods/pod-dnb"},
    },
    {
        name: "snb burstable",
        scopeRel: "kubepods/burstable/pod-snb",
        wantRels: []string{"kubepods", "kubepods/burstable"},
        rejectRels: []string{"kubepods/besteffort"},
    },
    {
        name: "dnb guaranteed",
        scopeRel: "kubepods/pod-dnb",
        wantRels: []string{"kubepods"},
        rejectRels: []string{"kubepods/burstable", "kubepods/besteffort"},
    },
}
```

Each case calls the new scoped target normalization path and asserts that only
`wantRels` receive pending CPUs.

- [ ] **Step 2: Verify RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestEffectiveTargetsScopePendingProtection' -count=1
```

Expected: build failure because the coordinator still accepts the old global
pending field.

- [ ] **Step 3: Replace the coordinator input**

Replace:

```go
ProtectedPendingCPUSet machine.CPUSet
```

with:

```go
PendingProtections []PendingProtection
```

Derive `protectedPending`, `pendingRequiredByRel`, and all domain-level unions
inside `convergeNormal`. Pass `pendingRequiredByRel` into normalization,
admission splitting, and ParentSafety evaluation.

- [ ] **Step 4: Wire the plugin**

Populate:

```go
PendingProtections: protections,
```

Keep `pending_count` and `pending_cpu_count` logs derived from the canonical
list. Remove the plugin's independent pending union from coordinator input.

- [ ] **Step 5: Scope target widening**

Change `computeEffectiveTargets` to merge pending CPUs only from:

```go
pendingRequiredByRel map[string]machine.CPUSet
```

The function must preserve hard-floor and existing protected-by-rel
normalization.

- [ ] **Step 6: Delete the old owner**

Update all production and test call sites, then run:

```bash
git grep -n 'ProtectedPendingCPUSet\|widenPrimaryTargetsWithProtectedCPUs'
```

Expected: no match. Do not add a compatibility fallback.

- [ ] **Step 7: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestEffectiveTargetsScopePendingProtection|TestSplitPlanForAdmission|TestEvaluateParentSafety' \
  -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run 'TestPendingProtectionScope' -count=1
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/target_normalization.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/target_normalization_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/writer_test.go
git commit -m "refactor(qrm-cpu): retire unscoped pending propagation"
```

## Task 7: Bound Projected Frontier Cost and Detect Cycles

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go`

- [ ] **Step 1: Add RED evidence-settlement tests**

Instrument the projected hierarchy with a test-only evidence rebuild counter.
Apply a frontier containing independent sibling operations and assert:

```go
require.Equal(t, 1, projected.evidenceRebuildCount())
```

Add a compile-time validation test that rejects one frontier containing an
ancestor and descendant operation.

- [ ] **Step 2: Add RED cycle tests**

Create a projected session that returns the same settled snapshot and rebased
plan twice. Assert:

```go
require.ErrorIs(t, err, ErrNoProgress)
var cycle *ProjectedPhaseCycleError
require.ErrorAs(t, err, &cycle)
require.Equal(t, 1, fixture.round.round)
```

- [ ] **Step 3: Verify RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestProjectedHierarchySettlesEvidenceOncePerFrontier|TestProjectedPhaseCycle|TestCompiledFrontierRejectsDependency' \
  -count=1
```

Expected: failures because evidence is rebuilt per operation and cycle
diagnostics are absent.

- [ ] **Step 4: Settle evidence once per frontier**

Split projected mutation into:

```go
func (h *projectedHierarchy) applyConfiguredOperation(operation PlanOperation) error
func (h *projectedHierarchy) settleEvidence() error
```

`projectedPhaseSession.Apply` applies every operation in frozen order, then
calls `settleEvidence()` exactly once. On any operation error, discard the
projected session; do not expose a partially settled snapshot.

- [ ] **Step 5: Add cycle and no-effect guards**

Track:

```go
type phaseProgressKey struct {
    SnapshotID SnapshotID
    PlanID     string
}
```

Return `ProjectedPhaseCycleError` when a key repeats. Return
`ProjectedPhaseNoProgressError` when a non-empty frontier settles to the same
snapshot ID. Both errors unwrap `ErrNoProgress`.

- [ ] **Step 6: Run GREEN and race**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestProjectedHierarchySettlesEvidenceOncePerFrontier|TestProjectedPhaseCycle|TestCompiledFrontierRejectsDependency' \
  -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestProjectedHierarchySettlesEvidenceOncePerFrontier|TestProjectedPhaseCycle' \
  -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go
git commit -m "perf(qrm-cpu): settle projected evidence per frontier"
```

## Task 8: Add Live-Shaped and Linear-Cost Gates

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_benchmark_test.go`

- [ ] **Step 1: Add the 194-entry contract test**

Construct 17 controlled specs, 13 sibling roots, and 177 dynamic descendants.
Add one unresolved pending protection. Assert:

```go
require.LessOrEqual(t, trace.OperationCount(),
    controlledAncestorCount+requiredFloorRepairCount)
require.True(t, trace.FinalEvaluation.ParentSafety.Safe)
require.Zero(t, fixture.driver.PhysicalWriteCount())
```

Run this as a normal contract test without a wall-clock assertion.

- [ ] **Step 2: Add operation-heavy scale measurement**

Add scale cases where both relation count and potential cleanup operation count
are 100, 1,000, and 10,000. Record elapsed time, allocations, and peak live
bytes. Assert bounded linear ratios using the existing scale-test tolerance and
guard the test with:

```go
if os.Getenv(topologyScaleTestEnv) != "1" {
    t.Skip("set KATALYST_TOPOLOGY_SCALE_TEST=1")
}
```

- [ ] **Step 3: Run default and explicit gates**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestCompileFixedPointTraceLivePendingShape' -count=1
KATALYST_TOPOLOGY_SCALE_TEST=1 \
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestCompileFixedPointTraceOperationHeavyScale' -count=1
```

Expected: both pass; the default package run does not execute the expensive
scale test.

- [ ] **Step 4: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_benchmark_test.go
git commit -m "test(qrm-cpu): gate operation-heavy pending traces"
```

## Task 9: Run Complete Local Verification

**Files:**

- Verify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology`
- Verify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology`
- Verify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy`

- [ ] **Step 1: Reconfirm the old owner is gone**

```bash
git grep -n 'ProtectedPendingCPUSet\|widenPrimaryTargetsWithProtectedCPUs'
```

Expected: no production match. Test fixtures must also use
`PendingProtections`.

- [ ] **Step 2: Run focused packages**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
```

Expected: pass.

- [ ] **Step 3: Run race verification**

```bash
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology -count=1
```

Expected: pass.

- [ ] **Step 4: Verify the staged tree**

Stage only implementation files, create a temporary worktree from the Git
index, and run the three focused package commands there. This proves the exact
staged tree rather than the mutable worktree.

- [ ] **Step 5: Commit any test-only integration fixes**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy
git commit -m "test(qrm-cpu): verify scoped pending integration"
```

Skip this commit when verification requires no integration fix.

## Task 10: Linux Build and All-QoS E2E

**Files:**

- Modify only E2E evidence artifacts and reports outside the source tree.

- [ ] **Step 1: Build on the dedicated Linux amd64 machine**

Build with Go 1.18.10, CGO enabled, and the required `libpci.so.3` and
`libc.so.6` runtime dependencies. Record:

```text
git HEAD
go version
GOOS/GOARCH
CGO_ENABLED
binary SHA256
ldd output
```

- [ ] **Step 2: Deploy and verify identity**

Deploy through the architecture jump host. Verify QRM and SysAdvisor process
executable SHA values match the uploaded binary before creating any workload.

- [ ] **Step 3: Run cold single-Pod probes**

Run from reset state, resetting between probes:

```text
ordinary shared_cores
SNB shared_cores
dedicated/DNB
```

For each probe, require:

```text
no ParentSafe deadline
pending_count is non-zero and a `pending pod scope selected` record identifies
the native QoS, selected scope, source, and protected CPU set
pending CPUs contained by scoped ancestors
pending CPUs disjoint from reclaim
Pod reaches Running
```

- [ ] **Step 4: Run the full matrix**

Run:

```text
standard: 3 rounds
high churn: 5 rounds
overlap/rollback cases
final reset
```

- [ ] **Step 5: Package evidence**

Archive QRM logs, SysAdvisor logs, Pod JSON, node checks, configuration, binary
SHA, and per-round summaries. Verify the archive SHA256 and `tar -tzf`.

- [ ] **Step 6: Update the E2E report**

Record each QoS mode separately. A passing DNB result must not mask ordinary
shared or SNB failure, and a warm-hierarchy retry must not replace a required
cold-start result.

## Final Acceptance

The implementation is complete only when:

```text
ordinary shared cold admission: PASS
SNB cold admission: PASS
DNB cold admission: PASS
scoped ancestor proof: PASS
primary/reclaim disjointness: PASS
forward + full inverse reservation: PASS
operation-heavy linear gate: PASS
standard 3 rounds: PASS
high churn 5 rounds: PASS
final reset: PASS
QRM and SysAdvisor health: READY
```
