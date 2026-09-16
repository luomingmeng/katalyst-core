# Frozen Admission Fixed-Point Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compile the complete hard-floor Drain→Expand fixed point on a cloned hierarchy, freeze one ordered trace, and use that trace as the sole proof, reservation, execution, and rollback contract.

**Architecture:** Extract the existing coordinator fixed-point behavior behind a hierarchy session boundary. Run it first with a clone-backed session to compile an immutable trace, then reserve and execute that exact trace with the live driver. Remove the synthetic single-plan admission closure and replace current-operation rollback with full-prefix rollback.

**Tech Stack:** Go 1.18.10, Kubernetes CPUSet utilities, cgroup v1/v2 hierarchy drivers, existing topology planner and fake driver tests.

---

## Baseline and Constraints

Implementation baseline:

```text
ef3e6f094 docs(qrm-cpu): record target tree closure review
5fcfb072e feat(qrm-cpu): close fake numa balance and hard-floor admission
```

Required non-edits:

- no WAL;
- no transaction identity or state fence;
- no checkpoint file, field, or schema change;
- no API or configuration schema change;
- no second required-floor owner;
- no fallback to the old admission closure;
- no manual cgroup or checkpoint reset in E2E.

The complete design is:

```text
docs/superpowers/specs/2026-09-16-frozen-admission-fixed-point-design.md
```

## File Map

### New production files

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go`
  - immutable trace types, clone compiler, deterministic trace ID, freeze validation.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy.go`
  - clone-backed hierarchy state and capability-aware CPU/memory projection.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_ticket.go`
  - ordered trace reservation and consumption.
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go`
  - whole-trace preflight, execution journal, and full-prefix rollback.

### New test files

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_ticket_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_benchmark_test.go`

### Modified production files

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/safe_writer.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/budget.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`

### Deleted files

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_closure.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_closure_test.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_ticket.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_reservation_test.go`

The deletion occurs only after replacement tests cover every retained behavior.

## Task 1: Lock the Trace Contract with RED Tests

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go`

- [ ] **Step 1: Add a RED test for staged dynamic descendant closure**

Create a fixture whose first drain phase cannot directly reach ParentSafe and
whose expand phase must grow `kubepods/besteffort`.

```go
func TestCompileFixedPointTraceIncludesStagedDynamicDescendantGrow(t *testing.T) {
    fixture := newAdmissionTraceFixture(t)
    fixture.selection.MaxCPUsDrainRatio = 0.5
    fixture.addPrimary("kubepods", "0-3", "0")
    fixture.addDynamicDescendant("kubepods/besteffort", "1-3", "0")
    fixture.addReclaim("reclaimed-0", "0", "0")
    fixture.requireCPUSet("kubepods", "0-3")

    trace, err := fixture.round.compileFixedPointTrace(
        context.Background(),
        fixture.snapshot(),
    )
    require.NoError(t, err)

    require.Greater(t, len(trace.Phases), 1)
    require.True(t, traceContainsOperation(
        trace,
        "kubepods/besteffort",
        WriteGrow,
        machine.MustParse("0-3"),
    ))
    require.True(t, trace.FinalEvaluation.ParentSafety.Safe)
    require.Zero(t, fixture.driver.PhysicalWriteCount())
}
```

- [ ] **Step 2: Add a RED test that compares compiled and live traces**

```go
func TestCompiledTraceMatchesFixedPointEngineTrace(t *testing.T) {
    fixture := newAdmissionTraceFixture(t)
    fixture.configureStagedSMTTransferWithDynamicDescendant()

    compiled, err := fixture.round.compileFixedPointTrace(
        context.Background(),
        fixture.snapshot(),
    )
    require.NoError(t, err)

    observed := fixture.runFixedPointAgainstRecordingDriver(t)
    require.Equal(t, flattenTraceOperations(compiled), observed)
}
```

This test must compare ordered operations, phase boundaries, expected-current
values, targets, identities, and memory ownership flags.

- [ ] **Step 3: Add RED tests for termination**

```go
func TestCompileFixedPointTraceRejectsNoProgress(t *testing.T) {
    fixture := newAdmissionTraceFixture(t)
    fixture.projectedDriver.IgnoreWrites = true

    _, err := fixture.round.compileFixedPointTrace(
        context.Background(),
        fixture.snapshot(),
    )
    require.ErrorIs(t, err, ErrNoProgress)
    require.Zero(t, fixture.driver.PhysicalWriteCount())
}

func TestCompileFixedPointTraceRejectsUnprovableRequiredFloor(t *testing.T) {
    fixture := newAdmissionTraceFixture(t)
    fixture.requireCPUSet("reclaimed-0", "0-3")
    fixture.setCanonicalTarget("reclaimed-0", "0-1")

    _, err := fixture.round.compileFixedPointTrace(
        context.Background(),
        fixture.snapshot(),
    )
    require.Error(t, err)
    require.Zero(t, fixture.driver.PhysicalWriteCount())
}
```

- [ ] **Step 4: Run RED**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestCompileFixedPointTrace|TestCompiledTraceMatchesFixedPointEngineTrace' \
  -count=1
```

Expected: build failure because `CompiledPhaseTrace` and
`compileFixedPointTrace` do not exist.

- [ ] **Step 5: Commit RED**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go
git commit -m "test(qrm-cpu): define frozen admission trace contract"
```

## Task 2: Implement Deep Snapshot Cloning

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy_test.go`

- [ ] **Step 1: Add a RED clone-isolation test**

```go
func TestCloneCompleteSnapshotIsDeeplyIsolated(t *testing.T) {
    original := completeSnapshotFixture(t)
    clone := CloneCompleteSnapshot(original)

    original.Entries["child"] = EntryState{
        Rel:            "child",
        CPUs:           machine.NewCPUSet(9),
        ConfiguredCPUs: machine.NewCPUSet(9),
        Mems:           "1",
        Children:       []string{"changed"},
    }
    original.DomainUnion["primary"] = machine.NewCPUSet(9)

    require.Equal(t, "0-1", clone.Entries["child"].CPUs.String())
    require.Equal(t, []string{"grandchild"}, clone.Entries["child"].Children)
    require.Equal(t, "0-3", clone.DomainUnion["primary"].String())
}
```

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run '^TestCloneCompleteSnapshotIsDeeplyIsolated$' -count=1
```

Expected: build failure because `CloneCompleteSnapshot` does not exist.

- [ ] **Step 3: Implement the deep clone**

Add:

```go
func CloneCompleteSnapshot(in *CompleteSnapshot) *CompleteSnapshot {
    if in == nil {
        return nil
    }
    out := *in
    out.Entries = make(map[string]EntryState, len(in.Entries))
    for rel, entry := range in.Entries {
        cloned := entry
        cloned.CPUs = entry.CPUs.Clone()
        cloned.ConfiguredCPUs = entry.ConfiguredCPUs.Clone()
        cloned.Children = append([]string(nil), entry.Children...)
        out.Entries[rel] = cloned
    }
    out.DomainByRel = cloneStringMap(in.DomainByRel)
    out.DomainUnion = cloneCPUSetMap(in.DomainUnion)
    return &out
}
```

Use existing clone helpers where available. Do not leave aliases to map,
slice, or CPUSet state.

- [ ] **Step 4: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run '^TestCloneCompleteSnapshotIsDeeplyIsolated$' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy_test.go
git commit -m "refactor(qrm-cpu): add isolated topology snapshots"
```

## Task 3: Implement Capability-Aware Projected Hierarchy

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go`

- [ ] **Step 1: Add the v1/v2 RED matrix**

```go
func TestProjectedHierarchyCPUSetSemantics(t *testing.T) {
    tests := []struct {
        name          string
        capabilities  HierarchyCapabilities
        configured    machine.CPUSet
        target        machine.CPUSet
        parentTarget  machine.CPUSet
        wantConfigured string
        wantEffective  string
        wantErr         error
    }{
        {
            name: "v1 non-empty target",
            capabilities: v1Capabilities(),
            configured: machine.NewCPUSet(0, 1),
            target: machine.NewCPUSet(0),
            wantConfigured: "0",
            wantEffective: "0",
        },
        {
            name: "v1 empty target is rejected",
            capabilities: v1Capabilities(),
            configured: machine.NewCPUSet(0),
            target: machine.NewCPUSet(),
            wantErr: ErrEmptyCPUSetUnsupported,
        },
        {
            name: "v2 empty target inherits parent",
            capabilities: v2Capabilities(),
            configured: machine.NewCPUSet(0),
            target: machine.NewCPUSet(),
            parentTarget: machine.NewCPUSet(0, 1),
            wantConfigured: "",
            wantEffective: "0-1",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            hierarchy := projectedHierarchyFixture(t, tt.capabilities)
            hierarchy.setParentTarget(tt.parentTarget)
            err := hierarchy.applyOperation(cpuOperation(
                "child",
                tt.configured,
                tt.target,
            ))
            require.ErrorIs(t, err, tt.wantErr)
            if tt.wantErr != nil {
                return
            }
            got := hierarchy.snapshot.Entries["child"]
            require.Equal(t, tt.wantConfigured, got.ConfiguredCPUs.String())
            require.Equal(t, tt.wantEffective, got.CPUs.String())
        })
    }
}
```

- [ ] **Step 2: Add recursive inheritance tests**

Cover:

- parent grow updates effective CPUs of every empty-configured descendant;
- parent shrink updates inherited descendants;
- non-empty configured descendants retain their target;
- invalid parent containment is rejected;
- CPU and memory inheritance are independent;
- domain unions and snapshot evidence IDs change after projection.

- [ ] **Step 3: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestProjectedHierarchy' -count=1
```

Expected: build failure because `projectedHierarchy` does not exist.

- [ ] **Step 4: Implement the projected hierarchy**

Define:

```go
type projectedHierarchy struct {
    snapshot     *CompleteSnapshot
    capabilities HierarchyCapabilities
    phases       []CompiledPhase
}

func newProjectedHierarchy(
    base *CompleteSnapshot,
    capabilities HierarchyCapabilities,
) (*projectedHierarchy, error)

func (h *projectedHierarchy) applyOperation(operation PlanOperation) error
func (h *projectedHierarchy) recomputeEffectiveSubtree(rel string) error
func (h *projectedHierarchy) recomputeEvidence() error
```

`applyOperation` must validate `ExpectedCurrent`, identity, parent identity,
and child fingerprint against the projected predecessor before changing the
clone.

- [ ] **Step 5: Run GREEN and parity tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestProjectedHierarchy|TestObservedCPUsForTargetProof' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/projected_hierarchy_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go
git commit -m "feat(qrm-cpu): model projected cpuset hierarchy"
```

## Task 4: Extract the Shared Fixed-Point Engine

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan.go`

- [ ] **Step 1: Add a RED engine-parity test**

```go
func TestFixedPointEngineUsesSamePlanSequenceForProjectedAndRecordingSessions(
    t *testing.T,
) {
    input := stagedDynamicDescendantFixture(t)
    projected := newProjectedSessionForTest(t, input)
    recording := newRecordingSessionForTest(t, input)

    projectedResult, err := input.round.runFixedPointEngine(
        context.Background(),
        projected,
    )
    require.NoError(t, err)

    recordingResult, err := input.round.runFixedPointEngine(
        context.Background(),
        recording,
    )
    require.NoError(t, err)

    require.Equal(t, projectedResult.Phases, recordingResult.Phases)
}
```

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run '^TestFixedPointEngineUsesSamePlanSequence' -count=1
```

Expected: build failure because `runFixedPointEngine` does not exist.

- [ ] **Step 3: Extract the session boundary**

Add an internal interface:

```go
type phaseExecutionSession interface {
    Snapshot(ctx context.Context) (*CompleteSnapshot, error)
    Apply(
        ctx context.Context,
        phase PhaseKind,
        operations []PlanOperation,
    ) error
}
```

Move the fixed-point decisions currently embedded in
`executeFixedPointRound` into:

```go
func (r *coordinatorRound) runFixedPointEngine(
    ctx context.Context,
    session phaseExecutionSession,
) (fixedPointEngineResult, error)
```

The result contains phases, final snapshot, convergence report, ParentSafe
report, round count, and deferred cleanup.

- [ ] **Step 4: Keep live behavior unchanged**

Create a live session adapter around the existing snapshot and writer paths.
At this task boundary, normal coordinator still uses the live adapter.
Admission does not switch to frozen execution yet.

- [ ] **Step 5: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestFixedPointEngine|TestTopologyCoordinator.*Handoff|TestTopologyCoordinator.*SMT' \
  -count=1
```

Expected: PASS with unchanged live operation order.

- [ ] **Step 6: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan.go
git commit -m "refactor(qrm-cpu): share topology fixed-point engine"
```

## Task 5: Compile and Freeze Complete Phase Traces

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan.go`

- [ ] **Step 1: Define the immutable trace types**

```go
type CompiledPhaseTrace struct {
    TraceID              string
    ConvergenceID        string
    Objective            ConvergenceObjective
    InitialSnapshot      *CompleteSnapshot
    CanonicalTargetByRel map[string]CPUSetTarget
    RequiredCPUSetByRel  map[string]machine.CPUSet
    Capabilities         HierarchyCapabilities
    Phases               []CompiledPhase
    FinalSnapshot        *CompleteSnapshot
    FinalEvaluation      coordinatorSnapshotEvaluation
    Cost                 ExecutionReservationCost
}

type CompiledPhase struct {
    Kind       PhaseKind
    Operations []PlanOperation
}
```

- [ ] **Step 2: Implement clone compilation**

```go
func (r *coordinatorRound) compileFixedPointTrace(
    ctx context.Context,
    base *CompleteSnapshot,
) (*CompiledPhaseTrace, error) {
    session, err := newProjectedPhaseSession(base, r.capabilities)
    if err != nil {
        return nil, err
    }
    result, err := r.runFixedPointEngine(ctx, session)
    if err != nil {
        return nil, err
    }
    trace := traceFromEngineResult(r, base, result)
    return FreezePhaseTrace(trace)
}
```

Compilation must not use the live hierarchy driver.

- [ ] **Step 3: Implement deterministic freezing**

```go
func FreezePhaseTrace(
    in *CompiledPhaseTrace,
) (*CompiledPhaseTrace, error)

func canonicalPhaseTraceID(
    trace *CompiledPhaseTrace,
) string

func validateFrozenPhaseTrace(
    trace *CompiledPhaseTrace,
) error
```

Sort map keys before hashing. Preserve operation and phase order. Reject:

- nil snapshots;
- missing identity;
- operation expected-current mismatch;
- final evaluator not satisfying the requested objective;
- required CPUs absent from final physical proof;
- zero-progress traces that do not already satisfy the objective.

- [ ] **Step 4: Add freeze isolation and determinism tests**

```go
func TestFreezePhaseTraceIsolatedFromMutableInputs(t *testing.T)
func TestFreezePhaseTraceIDIsIndependentOfMapInsertionOrder(t *testing.T)
func TestFreezePhaseTraceRejectsUnsafeFinalSnapshot(t *testing.T)
```

- [ ] **Step 5: Run compiler GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestCompileFixedPointTrace|TestCompiledTrace|TestFreezePhaseTrace' \
  -count=1
```

Expected: PASS, including the real `kubepods/besteffort` grow fixture.

- [ ] **Step 6: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/plan.go
git commit -m "feat(qrm-cpu): compile frozen admission phase traces"
```

## Task 6: Replace Signature Tickets with Ordered Trace Tickets

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_ticket.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_ticket_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/budget.go`

- [ ] **Step 1: Add RED reservation boundary tests**

```go
func TestReservePhaseTraceRejectsOneWriteShortWithoutPhysicalWrites(t *testing.T) {
    trace := compiledTraceWithCPUAndMemoryWrites(t)
    required := phaseTracePhysicalWriteCost(trace).Total()
    driver := newCountingHierarchyDriver()

    _, err := NewBudgetTracker(ConvergenceBudget{}).ReservePhaseTrace(
        trace,
        required-1,
    )
    require.ErrorIs(t, err, ErrAdmissionReservationExceeded)
    require.Zero(t, driver.PhysicalWriteCount())
}

func TestReservePhaseTraceAcceptsExactWriteBoundary(t *testing.T) {
    trace := compiledTraceWithCPUAndMemoryWrites(t)
    required := phaseTracePhysicalWriteCost(trace).Total()

    ticket, err := NewBudgetTracker(ConvergenceBudget{}).ReservePhaseTrace(
        trace,
        required,
    )
    require.NoError(t, err)
    require.Equal(t, required, ticket.reserved.Total())
}
```

- [ ] **Step 2: Add RED ordered authorization tests**

```go
func TestTraceTicketRejectsSkippedOperation(t *testing.T)
func TestTraceTicketRejectsRepeatedOperation(t *testing.T)
func TestTraceTicketRejectsReorderedOperation(t *testing.T)
func TestTraceTicketRejectsTamperedTarget(t *testing.T)
func TestTraceTicketRejectsTamperedIdentity(t *testing.T)
func TestTraceTicketAuthorizesRepeatedRelationByIndex(t *testing.T)
func TestTraceTicketSeparatesForwardAndRollbackConsumption(t *testing.T)
```

- [ ] **Step 3: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestReservePhaseTrace|TestTraceTicket' -count=1
```

Expected: build failure because `ExecutionReservationTicket` does not exist.

- [ ] **Step 4: Implement ordered tickets**

```go
type ExecutionReservationCost struct {
    Forward  PhysicalWriteCost
    Rollback PhysicalWriteCost
}

type ExecutionReservationTicket struct {
    mu               sync.Mutex
    traceID          string
    operations       []frozenOperationAuthorization
    nextOperation    int
    reserved         ExecutionReservationCost
    consumedForward  PhysicalWriteCost
    consumedRollback PhysicalWriteCost
    released         bool
}
```

Implement:

```go
func (b *BudgetTracker) ReservePhaseTrace(
    trace *CompiledPhaseTrace,
    maxRequiredWrites int,
) (*ExecutionReservationTicket, error)

func (t *ExecutionReservationTicket) AuthorizeNext(
    traceID string,
    operationIndex int,
    operation PlanOperation,
) error
```

Do not run compilation under `BudgetTracker.mu`.

- [ ] **Step 5: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestReservePhaseTrace|TestTraceTicket' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_ticket.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_ticket_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/budget.go
git commit -m "feat(qrm-cpu): reserve ordered topology traces"
```

## Task 7: Implement Whole-Trace Preflight

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/safe_writer.go`

- [ ] **Step 1: Add RED preflight tests**

```go
func TestTracePreflightRejectsInitialSnapshotDriftWithoutWrites(t *testing.T)
func TestTracePreflightRejectsIdentityDriftWithoutWrites(t *testing.T)
func TestTracePreflightRejectsChildFingerprintDriftWithoutWrites(t *testing.T)
func TestTracePreflightValidatesLaterOperationsAgainstOverlay(t *testing.T)
func TestTracePreflightRejectsInvalidV2InheritanceWithoutWrites(t *testing.T)
```

The common assertion is:

```go
require.Error(t, err)
require.Zero(t, driver.PhysicalWriteCount())
require.Equal(t, initialState, driver.SnapshotState())
```

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestTracePreflight' -count=1
```

Expected: build failure because `preflightFrozenTrace` does not exist.

- [ ] **Step 3: Implement preflight**

```go
func (w safeCPSetWriter) preflightFrozenTrace(
    ctx context.Context,
    trace *CompiledPhaseTrace,
) error
```

Preflight:

1. obtains one fresh complete snapshot;
2. compares execution-relevant state to `trace.InitialSnapshot`;
3. creates a projected hierarchy from the fresh snapshot;
4. validates and projects every frozen operation in order;
5. compares the resulting model with `trace.FinalSnapshot`;
6. performs no live write.

- [ ] **Step 4: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestTracePreflight' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/safe_writer.go
git commit -m "feat(qrm-cpu): preflight frozen topology traces"
```

## Task 8: Implement Full-Prefix Rollback

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/safe_writer.go`

- [ ] **Step 1: Add the failure-injection RED matrix**

Add table-driven tests for failures at:

```text
second operation memory write
second operation CPU write
later drain frontier
first expand operation
middle expand operation
post-write read-back
false-success write
final ParentSafe proof
context cancellation after first write
```

Example:

```go
func TestFrozenTraceFailureRollsBackCompleteAppliedPrefix(t *testing.T) {
    tests := []struct {
        name       string
        failAt     injectedFailure
        wantWrites []recordedPhysicalWrite
    }{
        {
            name:   "middle expand CPU write",
            failAt: failCPUWrite(4),
            wantWrites: append(
                forwardPrefix(3),
                reverseRollbackPrefix(3)...,
            ),
        },
        {
            name:   "final proof",
            failAt: failFinalProof(),
            wantWrites: append(
                allForwardWrites(),
                reverseRollbackAllWrites()...,
            ),
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            fixture := frozenExecutionFixture(t, tt.failAt)
            initial := fixture.driver.SnapshotState()

            err := fixture.execute()
            require.Error(t, err)
            require.Equal(t, initial, fixture.driver.SnapshotState())
            require.Equal(t, tt.wantWrites, fixture.driver.RecordedWrites())
            require.False(t, fixture.result.ParentSafe)
            require.False(t, fixture.result.Converged)
        })
    }
}
```

- [ ] **Step 2: Add rollback-error continuation tests**

```go
func TestRollbackPrefixContinuesAfterIntermediateRollbackFailure(t *testing.T)
func TestRollbackIdentityDriftDoesNotWriteReplacementGeneration(t *testing.T)
func TestRepeatedRelationRollsBackToInvocationInitialValue(t *testing.T)
func TestSuccessfulRollbackRemovesNetJournalAndAppliedProgress(t *testing.T)
```

- [ ] **Step 3: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestFrozenTraceFailure|TestRollbackPrefix|TestRepeatedRelationRollsBack' \
  -count=1
```

Expected: failures showing that only the current operation is restored.

- [ ] **Step 4: Implement the mutation stack**

```go
type AppliedPhysicalWrite struct {
    Rel      string
    Identity CgroupIdentity
    Resource HierarchyOperation
    Before   string
    After    string
}

type traceMutationStack struct {
    writes []AppliedPhysicalWrite
}
```

Every successful CPU or memory write appends one record. A failed write with
uncertain physical outcome performs a pinned read-back before deciding whether
to append its inverse.

- [ ] **Step 5: Implement reverse rollback**

```go
func (w safeCPSetWriter) rollbackTracePrefix(
    ctx context.Context,
    stack *traceMutationStack,
    ticket *ExecutionReservationTicket,
) error
```

Continue after rollback errors and aggregate all failures. Never write through
a changed cgroup identity.

- [ ] **Step 6: Restore result net state**

Before execution capture:

```go
journalStart := len(res.Journal)
appliedStart := res.Applied
```

After successful rollback:

```go
res.Journal = res.Journal[:journalStart]
res.Applied = appliedStart
res.ParentSafe = false
res.Converged = false
res.FinalSnapshotCurrent = false
```

- [ ] **Step 7: Run GREEN and race**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestFrozenTraceFailure|TestRollbackPrefix|TestRepeatedRelationRollsBack' \
  -count=1

go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestFrozenTraceFailure|TestRollbackPrefix' -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/safe_writer.go
git commit -m "fix(qrm-cpu): roll back frozen topology trace prefixes"
```

## Task 9: Switch Admission to Compile, Reserve, and Execute

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`

- [ ] **Step 1: Add RED single-path tests**

```go
func TestParentSafeAdmissionCompilesReservesAndExecutesOneFrozenTrace(t *testing.T)
func TestAdmissionCompileFailurePerformsZeroPhysicalWrites(t *testing.T)
func TestAdmissionReservationFailurePerformsZeroPhysicalWrites(t *testing.T)
func TestAdmissionPreflightDriftPerformsZeroPhysicalWrites(t *testing.T)
func TestAdmissionPostWriteDriftRollsBackBeforeReturning(t *testing.T)
func TestAdmissionPublishesOnlyAfterFreshFinalParentSafeProof(t *testing.T)
func TestDeferredCleanupRemainsOutsideParentSafeTrace(t *testing.T)
```

The principal assertion is:

```go
require.Equal(t, 1, fixture.compiler.InvocationCount())
require.Equal(t, 1, fixture.reservation.InvocationCount())
require.Equal(t, flattenTraceOperations(trace), fixture.driver.RecordedForwardOperations())
require.True(t, result.ParentSafe)
require.True(t, result.FinalSnapshotCurrent)
```

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestParentSafeAdmission|TestAdmissionCompileFailure|TestAdmissionPreflightDrift' \
  -count=1
```

Expected: failure because coordinator still reserves a single-plan closure and
rebuilds live plans.

- [ ] **Step 3: Change `coordinatorRound` ownership**

Replace the current admission ticket fields with:

```go
type coordinatorRound struct {
    frozenTrace     *CompiledPhaseTrace
    executionTicket *ExecutionReservationTicket
    // existing frozen attempt inputs remain
}
```

- [ ] **Step 4: Replace the ParentSafe flow**

The ParentSafe branch becomes:

```go
base, err := r.nextSnapshot(ctx)
trace, err := r.compileFixedPointTrace(ctx, base)
ticket, err := r.tracker.ReservePhaseTrace(
    trace,
    r.admissionBudget.MaxRequiredWrites,
)
outcome, err := r.executeFrozenTrace(ctx, trace, ticket, res)
```

No live plan rebuild is allowed after execution starts.

- [ ] **Step 5: Preserve periodic full convergence**

Full convergence uses the same fixed-point engine. It may continue with the
live session where no admission atomicity is required, but it must not call the
retired admission closure or trace ticket.

- [ ] **Step 6: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestParentSafeAdmission|TestAdmissionCompileFailure|TestAdmissionReservationFailure|TestAdmissionPreflightDrift|TestDeferredCleanup' \
  -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go
git commit -m "feat(qrm-cpu): execute frozen hard-floor topology traces"
```

## Task 10: Delete the Old Closure and Ticket Owners

**Files:**

- Delete: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_closure.go`
- Delete: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_closure_test.go`
- Delete: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_ticket.go`
- Delete: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_reservation_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/safe_writer.go`

- [ ] **Step 1: Move retained tests**

Before deletion, confirm every retained behavior is covered in the new test
files:

- exact physical CPU/memory cost;
- zero-write reservation rejection;
- rollback accounting;
- identity and children validation;
- fresh final proof;
- dynamic descendant grow;
- ParentSafe required floor.

- [ ] **Step 2: Delete old files and call sites**

```bash
git rm \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_closure.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_closure_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_ticket.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/admission_reservation_test.go
```

- [ ] **Step 3: Verify no stale owner remains**

```bash
git grep -n -E \
  'admissionRequiredOperationClosure|proveAdmissionRequiredClosure|cloneAdmissionSnapshot|projectAdmissionOperation|recomputeAdmissionDomainUnion|admissionClosureSafetyReport|admissionClosureOperationCounts|reserveAdmissionClosure|ReserveAdmissionBudget'
```

Expected: no output.

- [ ] **Step 4: Run topology tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
git commit -m "refactor(qrm-cpu): retire single-plan admission closure"
```

## Task 11: Add Performance and Scale Gates

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_benchmark_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go`

- [ ] **Step 1: Add benchmark shapes**

```go
func BenchmarkCompileFixedPointTrace(b *testing.B) {
    for _, nodes := range []int{100, 1000, 10000} {
        for _, depth := range []int{4, 8, 16} {
            b.Run(fmt.Sprintf("nodes=%d/depth=%d", nodes, depth), func(b *testing.B) {
                fixture := benchmarkTraceFixture(b, nodes, depth)
                b.ReportAllocs()
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    _, err := fixture.compile()
                    if err != nil {
                        b.Fatal(err)
                    }
                }
            })
        }
    }
}

func BenchmarkFreezePhaseTrace(b *testing.B)
func BenchmarkPreflightFrozenTrace(b *testing.B)
func BenchmarkRollbackFrozenTracePrefix(b *testing.B)
```

- [ ] **Step 2: Add explicit scale correctness tests**

```go
func TestCompileFixedPointTraceScalesAcross1024CPUShapes(t *testing.T)
func TestCompileFixedPointTraceHandlesTenThousandRelationsWithinBudget(t *testing.T)
func TestCompileFixedPointTracePerformsNoPhysicalHierarchyIO(t *testing.T)
```

- [ ] **Step 3: Run benchmark baseline**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run '^$' \
  -bench 'Benchmark(CompileFixedPointTrace|FreezePhaseTrace|PreflightFrozenTrace)' \
  -benchmem -count=5
```

Record:

- `ns/op`;
- `B/op`;
- `allocs/op`;
- compile rounds;
- compiled operations;
- physical driver reads and writes.

- [ ] **Step 4: Check complexity**

The 1000- and 10000-relation cases must not show quadratic allocation or time
growth relative to `nodes + edges + operations`. If they do, optimize the
projected hierarchy with copy-on-write overlays without changing its public
contract.

- [ ] **Step 5: Commit**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_benchmark_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_test.go
git commit -m "test(qrm-cpu): gate frozen trace scale and cost"
```

## Task 12: Run Local Verification

**Files:**

- No production file changes expected.

- [ ] **Step 1: Run focused trace tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Trace|ProjectedHierarchy|ParentSafe|HardFloor|Admission' \
  -count=1
```

- [ ] **Step 2: Run topology and bulkhead suites**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/... -count=1
```

- [ ] **Step 3: Run dynamicpolicy vertical contracts**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestFakeNUMASteadyBalanceAndHardFloorVerticalContract|TestPlanWholeCoreCapacityQuotasScalesAcross1024CPUShapes|TestPlanSteadyFakeNUMAQuotaRebalancesLiveFourteenFortyTwo|TestProjectSteadyFakeNUMAStageStrictlyApproachesFrozenTarget|Precommit.*FrozenConfig' \
  -count=1
```

- [ ] **Step 4: Run race**

```bash
go test -race \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Trace|ProjectedHierarchy|ParentSafe|Rollback' \
  -count=1
```

- [ ] **Step 5: Check mechanism drift**

```bash
git grep -n -E \
  'WAL|transaction identity|state fence|checkpoint.*schema' \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology

git diff --check
```

Expected:

- no newly introduced durability mechanism;
- no whitespace errors.

## Task 13: Linux Go 1.18 CGO Build

**Files:**

- No repository file changes expected.

- [ ] **Step 1: Sync the exact worktree**

Use the existing native build path on `10.37.68.154` and preserve the adapter
relative replace layout.

- [ ] **Step 2: Run focused tests with Go 1.18.10**

```bash
go version
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Trace|ProjectedHierarchy|ParentSafe|Rollback' -count=1
```

Expected:

```text
go version go1.18.10 linux/amd64
PASS
```

- [ ] **Step 3: Build with CGO**

```bash
export GO111MODULE=on
export CGO_ENABLED=1
export GOOS=linux
export GOARCH=amd64
go build -o /tmp/agent-frozen-trace ./cmd/katalyst-agent/main.go
```

- [ ] **Step 4: Validate the artifact**

```bash
file /tmp/agent-frozen-trace
nm /tmp/agent-frozen-trace | grep -c cgo
ldd /tmp/agent-frozen-trace | grep -E 'libpci|libc'
sha256sum /tmp/agent-frozen-trace
```

Expected:

- ELF 64-bit x86-64;
- dynamically linked;
- cgo symbols present;
- `libpci.so.3` and `libc.so.6` linked;
- one SHA256 recorded across every transfer hop.

## Task 14: c546 Live E2E

**Files:**

- Reuse external harness:
  - `.qrm-fake-numa-readonly-preflight.sh`
  - `.qrm-fake-numa-deploy-b3d42c16.sh`
  - `.qrm-fake-numa-stage-evidence.sh`
  - `.qrm-fake-numa-hardfloor-e2e.sh`

- [ ] **Step 1: Read-only preflight**

Capture:

- node identity and cluster;
- QRM/SysAdvisor binary SHA;
- checkpoint revision;
- reclaim checkpoint CPU sets;
- physical reclaimed CPU/memory cgroups;
- active AQC;
- existing E2E run tags.

Do not modify AQC, state, or cgroups.

- [ ] **Step 2: Deploy with per-hop SHA validation**

Backup the current agent before replacing it. Verify QRM and SysAdvisor run the
same new SHA and all health endpoints pass.

- [ ] **Step 3: Verify complete ParentSafe admission**

Create a unique run tag and execute:

```text
4 dedicated workloads
then
4 shared/SNB workloads
```

Required evidence:

- all eight workloads reach Running;
- one trace compile/proof/reserve/execute lifecycle per admission;
- the frozen trace contains the previously missing
  `kubepods/besteffort` grow;
- no `outside proved closure`;
- no `canonical target changed after ticket lock`;
- no runtime ticket extension;
- final required floor is physically present before allocation success;
- checkpoint and cgroup observations agree.

- [ ] **Step 4: Verify exact budget rejection**

On a controlled test configuration, set the required-write limit to
`trace cost - 1`.

Required evidence:

- admission fails;
- CPU and memory physical write counts are zero;
- all affected cgroups match the pre-attempt snapshot;
- no AppliedView publication.

- [ ] **Step 5: Verify restart and checkpoint compatibility**

Restart QRM during a steady staged migration, not during a live admission
transaction.

Required evidence:

- existing checkpoint loads without migration;
- state revision remains continuous;
- no WAL or new state file appears;
- steady migration resumes its frozen target;
- admission traces are rebuilt from fresh physical state after restart.

- [ ] **Step 6: Repeat convergence**

Run dedicated → shared → cleanup → re-admit at least twice with unique run
tags. Confirm no overlap, floor deficit, partial drain, ticket exhaustion, or
false AppliedView.

- [ ] **Step 7: Archive evidence**

Archive:

- binary SHA;
- build metadata;
- workload manifests and run tags;
- trace lifecycle logs;
- budget and rollback counters;
- checkpoint/cgroup observations;
- final health checks.

Record the archive SHA256.

## Task 15: Independent Review and Logical Squash

**Files:**

- Review all modified files.

- [ ] **Step 1: Request independent review**

Review focus:

- one fixed-point owner;
- clone/live parity;
- v1/v2 semantics;
- ordered authorization;
- full-prefix rollback;
- zero-write failure;
- no persistence/schema drift;
- all configuration combinations and resource subtypes.

- [ ] **Step 2: Resolve review findings with RED tests**

Every accepted defect receives a failing test before production changes.

- [ ] **Step 3: Run final verification**

Repeat Task 12, Task 13, and the relevant Task 14 E2E gates.

- [ ] **Step 4: Squash by logical boundary**

Keep documentation and code physically separate. Recommended final commits:

```text
docs(qrm-cpu): design frozen admission fixed-point execution
test(qrm-cpu): define frozen admission execution contract
feat(qrm-cpu): compile and execute frozen topology traces
fix(qrm-cpu): roll back frozen trace prefixes
refactor(qrm-cpu): retire single-plan admission closure
test(qrm-cpu): verify frozen trace scale and integration
```

Do not squash code and design documents into the same commit.

## Acceptance Matrix

| Requirement | Primary test |
|---|---|
| Complete staged closure before first write | `TestCompileFixedPointTraceIncludesStagedDynamicDescendantGrow` |
| Same planner owns compile and live semantics | `TestCompiledTraceMatchesFixedPointEngineTrace` |
| Full ParentSafe predicate | `TestFreezePhaseTraceRejectsUnsafeFinalSnapshot` |
| v1/v2 projection parity | `TestProjectedHierarchyCPUSetSemantics` and recursive inheritance tests |
| Exact full-sequence reservation | `TestReservePhaseTraceRejectsOneWriteShortWithoutPhysicalWrites` |
| Ordered authorization | `TestTraceTicketRejectsSkippedOperation` and related tests |
| Zero-write stale/preflight failure | `TestTracePreflightRejectsInitialSnapshotDriftWithoutWrites` |
| Complete prefix rollback | `TestFrozenTraceFailureRollsBackCompleteAppliedPrefix` |
| Rollback failure remains fail-closed | `TestRollbackPrefixContinuesAfterIntermediateRollbackFailure` |
| No runtime second owner | old-symbol `git grep` returns no output |
| Physical-before-success | c546 complete ParentSafe admission evidence |
| No durable state change | source diff check and restart compatibility evidence |
