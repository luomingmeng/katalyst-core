/*
Copyright 2026 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topology

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestTracePreflightRejectsInitialSnapshotDriftWithoutWrites(t *testing.T) {
	trace, driver := compiledTraceWithCPUAndMemoryWrites(t)
	operation := flattenTraceOperations(trace)[0]
	driver.nodes[operation.Rel].cpus = operation.ExpectedCurrent.CPUs.Union(machine.NewCPUSet(99))
	driver.nodes[operation.Rel].configuredCPUs = driver.nodes[operation.Rel].cpus.Clone()
	initialState := driver.snapshot()

	err := newTracePreflightWriter(driver).preflightFrozenTrace(context.Background(), trace)

	require.Error(t, err)
	require.Zero(t, driver.PhysicalWriteCount())
	require.Equal(t, initialState, driver.snapshot())
}

func TestTracePreflightRejectsIdentityDriftWithoutWrites(t *testing.T) {
	trace, driver := compiledTraceWithCPUAndMemoryWrites(t)
	driver.bumpIdentity(flattenTraceOperations(trace)[0].Rel)
	initialState := driver.snapshot()

	err := newTracePreflightWriter(driver).preflightFrozenTrace(context.Background(), trace)

	require.Error(t, err)
	require.Zero(t, driver.PhysicalWriteCount())
	require.Equal(t, initialState, driver.snapshot())
}

func TestTracePreflightRejectsChildFingerprintDriftWithoutWrites(t *testing.T) {
	trace, driver := compiledTraceWithCPUAndMemoryWrites(t)
	operation := operationWithCapturedChildren(t, trace)
	driver.add(
		filepath.Join(operation.Rel, "preflight-drift"),
		CgroupIdentity{Device: 1, Inode: 1000},
		"",
		"0",
	)
	initialState := driver.snapshot()

	err := newTracePreflightWriter(driver).preflightFrozenTrace(context.Background(), trace)

	require.Error(t, err)
	require.Zero(t, driver.PhysicalWriteCount())
	require.Equal(t, initialState, driver.snapshot())
}

func TestTracePreflightValidatesLaterOperationsAgainstOverlay(t *testing.T) {
	trace, driver := compiledTraceWithCPUAndMemoryWrites(t)
	require.True(t, traceRequiresEarlierParentOverlay(trace))
	initialState := driver.snapshot()

	err := newTracePreflightWriter(driver).preflightFrozenTrace(context.Background(), trace)

	require.NoError(t, err)
	require.Zero(t, driver.PhysicalWriteCount())
	require.Equal(t, initialState, driver.snapshot())
}

func TestTracePreflightProducesEveryProjectedPhysicalPredecessorIncludingV2EmptyConfigured(t *testing.T) {
	fixture, base := newTask9ParentSafeFixture(t)
	candidate, err := fixture.round.compileFixedPointTrace(context.Background(), base)
	require.NoError(t, err)
	candidateOperations := flattenTraceOperations(candidate)
	require.NotEmpty(t, candidateOperations)
	inheritedRel := candidateOperations[0].Rel
	fixture.driver.nodes[inheritedRel].configuredCPUs = machine.NewCPUSet()
	fixture.driver.nodes[inheritedRel].configuredMems = ""
	base = fixture.snapshot()
	trace, err := fixture.round.compileFixedPointTrace(context.Background(), base)
	require.NoError(t, err)
	fixture.round.round = 0

	evidence, err := newTracePreflightWriter(fixture.driver).
		preflightFrozenTraceOperations(context.Background(), trace)

	require.NoError(t, err)
	require.Len(t, evidence, len(flattenTraceOperations(trace)))
	foundInherited := false
	for index, operation := range flattenTraceOperations(trace) {
		require.Equal(t, operation.ExpectedIdentity, evidence[index].before.Identity)
		require.Equal(t, operation.ExpectedIdentity, evidence[index].after.Identity)
		if operation.Rel != inheritedRel {
			continue
		}
		foundInherited = true
		require.True(t, evidence[index].before.ConfiguredCPUs.IsEmpty())
		require.Empty(t, evidence[index].before.ConfiguredMems)
		require.False(t, evidence[index].before.EffectiveCPUs.IsEmpty())
		require.NotEmpty(t, evidence[index].before.EffectiveMems)
	}
	require.True(t, foundInherited,
		"fixture must retain the v2 inherited relation in the global trace")
	require.Zero(t, fixture.driver.PhysicalWriteCount())
}

func TestTracePreflightReplaysAncestorOnlyBoundaryWithoutReadingExtraDescendants(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.driver.capabilities = cgroupV2Policy.capabilities(true)
	fixture.round.allowEmptyTarget = true
	fixture.addPrimary("kubepods", "0-3", "0")
	fixture.addDynamicDescendant("kubepods/besteffort", "0-3", "0")
	_ = fixture.snapshot()

	base, err := BuildCompleteSnapshot(
		context.Background(),
		fixture.driver,
		fixture.round.dag,
		SnapshotRequest{
			Purpose:      ScanForPlan,
			AffectedRels: []string{"kubepods/besteffort"},
		},
		NewBudgetTracker(ConvergenceBudget{}),
	)
	require.NoError(t, err)
	require.Equal(t, []string{"kubepods", "kubepods/besteffort"}, base.ScanBoundary.Roots)
	require.Equal(t, []string{"kubepods/besteffort"}, base.ScanBoundary.ExpandedRels)

	trace, err := fixture.round.compileFixedPointTrace(context.Background(), base)
	require.NoError(t, err)
	traceID := trace.TraceID
	fixture.driver.add(
		"kubepods/unrelated",
		CgroupIdentity{Device: 1, Inode: 999},
		"0-3",
		"0",
	)
	var calls []string
	fixture.driver.beforeCall = func(op HierarchyOperation, rel string) error {
		calls = append(calls, string(op)+":"+rel)
		if rel == "kubepods/unrelated" {
			return errors.New("ancestor-only replay must not read unrelated descendant")
		}
		return nil
	}

	err = newTracePreflightWriter(fixture.driver).preflightFrozenTrace(context.Background(), trace)

	require.NoError(t, err)
	require.Equal(t, traceID, trace.TraceID)
	require.NotContains(t, calls, string(HierarchyOperationList)+":kubepods")
	for _, call := range calls {
		require.NotContains(t, call, "kubepods/unrelated")
	}
	require.Zero(t, fixture.driver.PhysicalWriteCount())
}

func TestTracePreflightRejectsInvalidV2InheritanceWithoutWrites(t *testing.T) {
	trace, driver := compiledTraceWithCPUAndMemoryWrites(t)
	operation := operationWithParent(t, trace)
	parent := trace.InitialSnapshot.Entries[operation.ParentRel]
	operation.Target.CPUs = parent.CPUs.Union(machine.NewCPUSet(99))
	operation.Direction = WriteGrow
	replaceTraceOperation(t, trace, operation)
	fresh, err := BuildCompleteSnapshotForBoundary(
		context.Background(),
		driver,
		mustBuildTraceDAG(t, trace),
		trace.InitialSnapshot.ScanBoundary,
		NewBudgetTracker(ConvergenceBudget{}),
	)
	require.NoError(t, err)
	require.Equal(t, trace.InitialSnapshot.ID, fresh.ID)
	initialState := driver.snapshot()

	err = newTracePreflightWriter(driver).preflightFrozenTrace(context.Background(), trace)

	require.ErrorIs(t, err, ErrProjectedParentContainment)
	require.ErrorContains(t, err, "validate frozen phase trace operation")
	require.Zero(t, driver.PhysicalWriteCount())
	require.Equal(t, initialState, driver.snapshot())
}

func TestFrozenTraceDriftAfterPreflightBeforeFirstWritePerformsNoUnauthorizedWrite(t *testing.T) {
	trace, live := compiledTraceWithCPUAndMemoryWrites(t)
	live.invariants = nil
	initial := live.snapshot()
	driver := newPreWriteDriftDriver(live, trace.InitialSnapshot, 0, "")
	round := frozenExecutionRound(t, trace, driver)
	ticket := reserveTraceWithBudget(t, round.budget, trace)
	res := &ConvergenceResult{}

	_, err := round.executeFrozenTrace(context.Background(), trace, ticket, res)

	var stale *PlanStaleError
	require.ErrorAs(t, err, &stale)
	require.Zero(t, driver.forwardWrites,
		"state drift after preflight must be rejected before the first physical write")
	require.Zero(t, ticket.consumedForward.Total())
	require.NotEmpty(t, driver.driftedRel)
	driftedState := live.snapshot()
	delete(initial, driver.driftedRel)
	delete(driftedState, driver.driftedRel)
	require.Equal(t, initial, driftedState,
		"executor must not change any relation after rejecting external drift")
	require.Empty(t, res.Journal)
	require.Zero(t, res.Applied)
}

func TestFrozenTraceDriftBetweenOperationsRollsBackAppliedPrefix(t *testing.T) {
	trace, live := compiledTraceWithCPUAndMemoryWrites(t)
	live.invariants = nil
	operations := flattenTraceOperations(trace)
	require.GreaterOrEqual(t, len(operations), 2)
	nextRel := ""
	for _, operation := range operations[1:] {
		if operation.Rel != operations[0].Rel {
			nextRel = operation.Rel
			break
		}
	}
	require.NotEmpty(t, nextRel, "fixture needs a later operation on another relation")
	initial := live.snapshot()
	driver := newPreWriteDriftDriver(live, trace.InitialSnapshot, 1, nextRel)
	round := frozenExecutionRound(t, trace, driver)
	ticket := reserveTraceWithBudget(t, round.budget, trace)
	res := &ConvergenceResult{}

	_, err := round.executeFrozenTrace(context.Background(), trace, ticket, res)

	var stale *PlanStaleError
	require.ErrorAs(t, err, &stale)
	require.Positive(t, driver.forwardWrites,
		"the first operation must be applied before the injected inter-operation drift")
	driftedState := live.snapshot()
	delete(initial, driver.driftedRel)
	delete(driftedState, driver.driftedRel)
	require.Equal(t, initial, driftedState,
		"the completed physical-write prefix must be rolled back; only external drift may remain")
	require.Empty(t, res.Journal)
	require.Zero(t, res.Applied)
	require.Positive(t, ticket.consumedRollback.Total())
}

func TestFrozenTraceValidatesCompletePredecessorBeforeOperationFirstWrite(t *testing.T) {
	type driftCase struct {
		name     string
		selectOp func(PlanOperation, *CompiledPhaseTrace) bool
		mutate   func(*fakeHierarchyDriver, PlanOperation)
	}
	tests := []driftCase{
		{
			name:     "current identity",
			selectOp: func(PlanOperation, *CompiledPhaseTrace) bool { return true },
			mutate: func(live *fakeHierarchyDriver, operation PlanOperation) {
				live.bumpIdentity(operation.Rel)
			},
		},
		{
			name: "parent identity",
			selectOp: func(operation PlanOperation, _ *CompiledPhaseTrace) bool {
				return operation.ParentRel != ""
			},
			mutate: func(live *fakeHierarchyDriver, operation PlanOperation) {
				live.bumpIdentity(operation.ParentRel)
			},
		},
		{
			name: "children fingerprint",
			selectOp: func(operation PlanOperation, trace *CompiledPhaseTrace) bool {
				return len(trace.InitialSnapshot.Children[operation.Rel]) > 0
			},
			mutate: func(live *fakeHierarchyDriver, operation PlanOperation) {
				childRel := filepath.Join(operation.Rel, "external-child")
				live.add(childRel, CgroupIdentity{Device: 99, Inode: 99}, "0", "0")
			},
		},
		{
			name: "child identity",
			selectOp: func(operation PlanOperation, trace *CompiledPhaseTrace) bool {
				return len(trace.InitialSnapshot.Children[operation.Rel]) > 0
			},
			mutate: func(live *fakeHierarchyDriver, operation PlanOperation) {
				child := traceChildRel(t, operation.Rel, live)
				live.bumpIdentity(child)
			},
		},
		{
			name: "child CPU union",
			selectOp: func(operation PlanOperation, trace *CompiledPhaseTrace) bool {
				return len(trace.InitialSnapshot.Children[operation.Rel]) > 0
			},
			mutate: func(live *fakeHierarchyDriver, operation PlanOperation) {
				child := traceChildRel(t, operation.Rel, live)
				live.nodes[child].configuredCPUs =
					live.nodes[child].configuredCPUs.Union(machine.NewCPUSet(99))
				live.nodes[child].cpus =
					live.nodes[child].cpus.Union(machine.NewCPUSet(99))
			},
		},
		{
			name: "child mems union",
			selectOp: func(operation PlanOperation, trace *CompiledPhaseTrace) bool {
				return len(trace.InitialSnapshot.Children[operation.Rel]) > 0
			},
			mutate: func(live *fakeHierarchyDriver, operation PlanOperation) {
				child := traceChildRel(t, operation.Rel, live)
				live.nodes[child].configuredMems = "0-99"
				live.nodes[child].mems = "0-99"
			},
		},
		{
			name: "CPU resource before mems write",
			selectOp: func(operation PlanOperation, _ *CompiledPhaseTrace) bool {
				return operation.WriteMems &&
					!operation.ExpectedCurrent.CPUs.Equals(operation.Target.CPUs)
			},
			mutate: func(live *fakeHierarchyDriver, operation PlanOperation) {
				live.nodes[operation.Rel].configuredCPUs =
					live.nodes[operation.Rel].configuredCPUs.Union(machine.NewCPUSet(99))
				live.nodes[operation.Rel].cpus =
					live.nodes[operation.Rel].cpus.Union(machine.NewCPUSet(99))
			},
		},
		{
			name: "mems resource before CPU write",
			selectOp: func(operation PlanOperation, _ *CompiledPhaseTrace) bool {
				return !operation.WriteMems &&
					!operation.ExpectedCurrent.CPUs.Equals(operation.Target.CPUs)
			},
			mutate: func(live *fakeHierarchyDriver, operation PlanOperation) {
				live.nodes[operation.Rel].configuredMems = "0-99"
				live.nodes[operation.Rel].mems = "0-99"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trace, live := compiledTraceWithCPUAndMemoryWrites(t)
			live.invariants = nil
			operation := selectTraceOperation(t, trace, tt.selectOp)
			driver := newFrozenPredecessorDriftDriver(live, trace.InitialSnapshot, operation, tt.mutate)
			round := frozenExecutionRound(t, trace, driver)
			ticket := reserveTraceWithBudget(t, round.budget, trace)
			res := &ConvergenceResult{}

			_, err := round.executeFrozenTrace(context.Background(), trace, ticket, res)

			var stale *PlanStaleError
			require.ErrorAs(t, err, &stale)
			require.True(t, driver.drifted)
			require.Zero(t, driver.writesToDriftedOperation,
				"the drifted operation must perform no physical write")
		})
	}
}

func TestFrozenTraceV2EmptyConfiguredTargetIsNoOpWithoutForwardTicket(t *testing.T) {
	writer, live, operation, ticket := v2InheritedRollbackFixture(
		t, HierarchyOperationWriteCPUs)
	operation.ExpectedCurrent.CPUs = machine.MustParse("0-1")
	operation.Target.CPUs = machine.NewCPUSet()
	ticket.traceID = "v2-empty"
	ticket.operations = []frozenOperationAuthorization{{operation: operation}}
	ticket.reserved.Forward.CPUSetWrites = 1
	current, err := live.ReadEntry(context.Background(), operation.Rel)
	require.NoError(t, err)
	preflight := frozenOperationPreflight{
		before: frozenOperationState{
			Identity:       current.Identity,
			ConfiguredCPUs: machine.MustParse("0-1"),
			EffectiveCPUs:  machine.MustParse("0-1"),
			ConfiguredMems: current.ConfiguredMems,
			EffectiveMems:  current.Mems,
		},
		after: freezeOperationState(current),
	}
	stack := &traceMutationStack{}

	applied, err := writer.applyFrozenOperation(
		context.Background(), PhaseDrain, 0, operation, preflight, stack, ticket, "v2-empty")

	require.NoError(t, err)
	require.Equal(t, operation.PlanID, applied.PlanID)
	require.Zero(t, live.PhysicalWriteCount())
	require.Zero(t, ticket.consumedForward.Total())
	require.Empty(t, stack.writes)
	require.True(t, live.nodes[operation.Rel].configuredCPUs.IsEmpty())
	require.Equal(t, "0-3", live.nodes[operation.Rel].cpus.String())
}

func TestFrozenTraceFailureRollsBackCompleteAppliedPrefix(t *testing.T) {
	type failureCase struct {
		name         string
		traceFactory func(*testing.T) (*CompiledPhaseTrace, *fakeHierarchyDriver)
		point        func([]expectedTraceWrite) traceFailureInjection
	}
	tests := []failureCase{
		{
			name: "second operation memory write",
			point: func(writes []expectedTraceWrite) traceFailureInjection {
				return failAtNthResourceWrite(t, writes, HierarchyOperationWriteMems, 2)
			},
		},
		{
			name: "second operation CPU write",
			point: func(writes []expectedTraceWrite) traceFailureInjection {
				injection := failAtNthResourceWrite(t, writes, HierarchyOperationWriteCPUs, 2)
				injection.mutateThenFailWriteAt = injection.failWriteAt
				injection.failWriteAt = 0
				return injection
			},
		},
		{
			name:         "later drain frontier",
			traceFactory: compiledMultiFrontierTrace,
			point: func(writes []expectedTraceWrite) traceFailureInjection {
				return failAtPhaseWrite(t, writes, PhaseDrain, -1)
			},
		},
		{
			name: "first expand operation",
			point: func(writes []expectedTraceWrite) traceFailureInjection {
				return failAtPhaseWrite(t, writes, PhaseExpand, 0)
			},
		},
		{
			name: "middle expand operation",
			point: func(writes []expectedTraceWrite) traceFailureInjection {
				return failAtPhaseWrite(t, writes, PhaseExpand, 1)
			},
		},
		{name: "post-write read-back", point: func([]expectedTraceWrite) traceFailureInjection {
			return traceFailureInjection{failPostWriteReadback: true}
		}},
		{name: "false-success write", point: func([]expectedTraceWrite) traceFailureInjection {
			return traceFailureInjection{falseSuccessWriteAt: 1}
		}},
		{name: "final ParentSafe proof", point: func([]expectedTraceWrite) traceFailureInjection {
			return traceFailureInjection{failFinalProof: true}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			traceFactory := tt.traceFactory
			if traceFactory == nil {
				traceFactory = compiledTraceWithCPUAndMemoryWrites
			}
			trace, live := traceFactory(t)
			initial := live.snapshot()
			// The compiled fixture intentionally starts with one transient v2
			// inheritance shape that the fake invariant rejects when restored.
			// Rollback tests exercise executor ordering, not that synthetic
			// fixture invariant.
			live.invariants = nil
			writes := expectedPhysicalWrites(trace)
			require.NotEmpty(t, writes)
			injection := tt.point(writes)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			driver := &injectedTraceDriver{
				HierarchyDriver: live,
				injection:       injection,
				expectedForward: len(writes),
				cancel:          cancel,
			}
			round := frozenExecutionRound(t, trace, driver)
			ticket := reserveTraceWithBudget(t, round.budget, trace)
			seedJournal := []AppliedPlanOperation{{PlanID: "before-invocation"}}
			res := &ConvergenceResult{
				Applied:              7,
				Journal:              append([]AppliedPlanOperation(nil), seedJournal...),
				ParentSafe:           true,
				Converged:            true,
				FinalSnapshotCurrent: true,
			}

			_, err := round.executeFrozenTrace(ctx, trace, ticket, res)

			require.Error(t, err)
			require.True(t, driver.injected, "failure point was not reached")
			require.Equal(t, initial, live.snapshot())
			require.Equal(t, seedJournal, res.Journal)
			require.Equal(t, 7, res.Applied)
			require.False(t, res.ParentSafe)
			require.False(t, res.Converged)
			require.False(t, res.FinalSnapshotCurrent)
			if injection.failWriteAt != 1 && injection.falseSuccessWriteAt == 0 {
				require.Positive(t, ticket.consumedRollback.Total())
			}
		})
	}
}

func TestFrozenTraceMutateThenErrorWithFailedPinnedReadback(t *testing.T) {
	tests := []struct {
		name          string
		identityDrift bool
	}{
		{name: "original identity is rolled back"},
		{name: "replacement identity is never written", identityDrift: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trace, live := compiledTraceWithCPUAndMemoryWrites(t)
			live.invariants = nil
			initial := live.snapshot()
			writes := expectedPhysicalWrites(trace)
			require.GreaterOrEqual(t, len(writes), 3)
			driver := &injectedTraceDriver{
				HierarchyDriver: live,
				injection: traceFailureInjection{
					mutateThenFailWriteAt:         3,
					failUncertainReadback:         true,
					driftIdentityOnFailedReadback: tt.identityDrift,
				},
				expectedForward: len(writes),
			}
			round := frozenExecutionRound(t, trace, driver)
			ticket := reserveTraceWithBudget(t, round.budget, trace)
			seed := AppliedPlanOperation{PlanID: "before-invocation"}
			res := &ConvergenceResult{
				Applied:              4,
				Journal:              []AppliedPlanOperation{seed},
				ParentSafe:           true,
				Converged:            true,
				FinalSnapshotCurrent: true,
			}

			_, err := round.executeFrozenTrace(context.Background(), trace, ticket, res)

			require.Error(t, err)
			require.ErrorContains(t, err, "injected uncertain")
			require.ErrorContains(t, err, "injected uncertain write read-back failure")
			require.False(t, res.ParentSafe)
			require.False(t, res.Converged)
			require.False(t, res.FinalSnapshotCurrent)
			if tt.identityDrift {
				require.ErrorIs(t, err, ErrCgroupIdentityChanged)
				require.Greater(t, len(res.Journal), 1,
					"unverified physical recovery must retain the invocation journal")
				require.Greater(t, res.Applied, 4,
					"unverified physical recovery must retain applied evidence")
				impact := res.Journal[len(res.Journal)-1]
				require.Equal(t, PhysicalImpactUncertain, impact.PhysicalImpact)
				require.Equal(t, writes[2].resource, impact.Resource)
				require.Equal(t, live.writes[2].rel, impact.Rel)
				for _, write := range live.writes[3:] {
					require.NotEqual(t, write.rel, live.writes[2].rel,
						"rollback must not write the replacement identity")
				}
				return
			}
			require.Equal(t, initial, live.snapshot())
			require.Equal(t, []AppliedPlanOperation{seed}, res.Journal)
			require.Equal(t, 4, res.Applied)
		})
	}
}

func TestFirstUncertainWriteIdentityDriftRetainsPhysicalImpact(t *testing.T) {
	trace, live := compiledTraceWithCPUAndMemoryWrites(t)
	live.invariants = nil
	writes := expectedPhysicalWrites(trace)
	driver := &injectedTraceDriver{
		HierarchyDriver: live,
		injection: traceFailureInjection{
			mutateThenFailWriteAt:         1,
			failUncertainReadback:         true,
			driftIdentityOnFailedReadback: true,
		},
		expectedForward: len(writes),
	}
	round := frozenExecutionRound(t, trace, driver)
	ticket := reserveTraceWithBudget(t, round.budget, trace)
	seed := AppliedPlanOperation{PlanID: "before-invocation"}
	res := &ConvergenceResult{
		Applied: 3,
		Journal: []AppliedPlanOperation{seed},
	}

	_, err := round.executeFrozenTrace(context.Background(), trace, ticket, res)

	require.ErrorIs(t, err, ErrCgroupIdentityChanged)
	require.Len(t, res.Journal, 2)
	require.Equal(t, 4, res.Applied)
	impact := res.Journal[1]
	require.Equal(t, PhysicalImpactUncertain, impact.PhysicalImpact)
	require.Equal(t, writes[0].resource, impact.Resource)
	require.NotEmpty(t, impact.Rel)
}

func TestFrozenTraceRollbackAfterForwardCancellationRestoresInitialState(t *testing.T) {
	trace, live := compiledTraceWithCPUAndMemoryWrites(t)
	live.invariants = nil
	initial := live.snapshot()
	ctx, cancel := context.WithCancel(context.Background())
	driver := &injectedTraceDriver{
		HierarchyDriver: live,
		injection:       traceFailureInjection{cancelAfterWrite: 1},
		expectedForward: len(expectedPhysicalWrites(trace)),
		cancel:          cancel,
	}
	round := frozenExecutionRound(t, trace, driver)
	ticket := reserveTraceWithBudget(t, round.budget, trace)
	res := &ConvergenceResult{}

	_, err := round.executeFrozenTrace(ctx, trace, ticket, res)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, initial, live.snapshot())
	require.Empty(t, res.Journal)
	require.Zero(t, res.Applied)
	require.Positive(t, ticket.consumedRollback.Total())
	require.NotContains(t, err.Error(), "rollback failed")
}

func TestFrozenTraceRollbackAfterForwardDeadlineRestoresInitialState(t *testing.T) {
	trace, live := compiledTraceWithCPUAndMemoryWrites(t)
	live.invariants = nil
	initial := live.snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	driver := &injectedTraceDriver{
		HierarchyDriver: live,
		injection: traceFailureInjection{
			blockAfterWriteUntilDeadline: 1,
		},
		expectedForward: len(expectedPhysicalWrites(trace)),
	}
	round := frozenExecutionRound(t, trace, driver)
	round.budget = NewBudgetTracker(ConvergenceBudget{Deadline: deadline})
	ticket := reserveTraceWithBudget(t, round.budget, trace)
	res := &ConvergenceResult{}

	_, err := round.executeFrozenTrace(ctx, trace, ticket, res)

	require.ErrorIs(t, err, context.DeadlineExceeded,
		"the original forward deadline must remain the primary unwrap chain")
	require.Equal(t, initial, live.snapshot(),
		"rollback must use an independent deadline after the forward deadline expires")
	require.Empty(t, res.Journal)
	require.Zero(t, res.Applied)
	require.Positive(t, ticket.consumedRollback.Total())
	require.NotContains(t, err.Error(), "rollback failed")
}

func TestFrozenTraceRecoveryDeadlineRetainsPhysicalImpactEvidence(t *testing.T) {
	trace, live := compiledTraceWithCPUAndMemoryWrites(t)
	live.invariants = nil
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	driver := &injectedTraceDriver{
		HierarchyDriver: live,
		injection: traceFailureInjection{
			cancelAfterWrite:      1,
			blockRollbackIdentity: true,
		},
		expectedForward: len(expectedPhysicalWrites(trace)),
		cancel:          cancel,
	}
	round := frozenExecutionRound(t, trace, driver)
	ticket := reserveTraceWithBudget(t, round.budget, trace)
	res := &ConvergenceResult{}

	_, err := round.executeFrozenTrace(ctx, trace, ticket, res)

	require.ErrorIs(t, err, context.Canceled,
		"the forward cancellation must remain the primary unwrap chain")
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"the bounded recovery deadline must be attached")
	require.Equal(t,
		"execution failed: context canceled; rollback failed: [read entry before rollback write_cpus for \"reclaimed/leaf\": context deadline exceeded, read \"reclaimed/leaf\" after rollback: context deadline exceeded]",
		err.Error(),
	)
	require.NotEmpty(t, res.Journal)
	require.Positive(t, res.Applied)
	require.NotEqual(t, PhysicalImpactNone, res.Journal[len(res.Journal)-1].PhysicalImpact)
}

func compiledMultiFrontierTrace(
	t *testing.T,
) (*CompiledPhaseTrace, *fakeHierarchyDriver) {
	t.Helper()
	fixture := newAdmissionTraceFixture(t)
	fixture.configureMultiFrontierParentSafeDrain()
	trace, err := fixture.round.compileFixedPointTrace(
		context.Background(),
		fixture.snapshot(),
	)
	require.NoError(t, err)
	drainWrites := 0
	for _, write := range expectedPhysicalWrites(trace) {
		if write.phase == PhaseDrain {
			drainWrites++
		}
	}
	require.GreaterOrEqual(t, drainWrites, 2)
	return trace, fixture.driver
}

func TestRollbackPrefixContinuesAfterIntermediateRollbackFailure(t *testing.T) {
	live := newFakeHierarchyDriver()
	live.allowUnwitnessedExpansion = true
	live.add("a", CgroupIdentity{Device: 1, Inode: 1}, "0", "0")
	live.add("b", CgroupIdentity{Device: 1, Inode: 2}, "1", "0")
	live.add("c", CgroupIdentity{Device: 1, Inode: 3}, "2", "0")
	stack := &traceMutationStack{}
	applyCPUWriteForRollbackTest(t, live, stack, "a", "0", "0-1")
	applyCPUWriteForRollbackTest(t, live, stack, "b", "1", "1-2")
	applyCPUWriteForRollbackTest(t, live, stack, "c", "2", "2-3")
	driver := &injectedTraceDriver{
		HierarchyDriver: live,
		injection:       traceFailureInjection{failWriteAt: 2},
	}
	ticket := rollbackOnlyTicket(3, 0)
	writer := newSafeCPUSetWriter(driver, NewBudgetTracker(ConvergenceBudget{}), nil)

	err := writer.rollbackTracePrefix(context.Background(), stack, ticket)

	require.Error(t, err)
	require.Equal(t, 3, driver.writeCalls, "rollback must continue after the middle inverse fails")
	require.Equal(t, machine.MustParse("0"), live.nodes["a"].cpus)
	require.Equal(t, machine.MustParse("1-2"), live.nodes["b"].cpus)
	require.Equal(t, machine.MustParse("2"), live.nodes["c"].cpus)
	require.Equal(t, 3, ticket.consumedRollback.CPUSetWrites)
	require.Zero(t, ticket.consumedForward.Total())
}

func TestFailedRollbackRetainsOnlyNetPhysicalImpact(t *testing.T) {
	live := newFakeHierarchyDriver()
	live.allowUnwitnessedExpansion = true
	live.add("a", CgroupIdentity{Device: 1, Inode: 1}, "0", "0")
	live.add("b", CgroupIdentity{Device: 1, Inode: 2}, "1", "0")
	stack := &traceMutationStack{}
	applyCPUWriteForRollbackTest(t, live, stack, "a", "0", "0-1")
	applyCPUWriteForRollbackTest(t, live, stack, "b", "1", "1-2")
	driver := &injectedTraceDriver{
		HierarchyDriver: live,
		injection:       traceFailureInjection{failWriteAt: 1},
	}
	writer := newSafeCPUSetWriter(driver, NewBudgetTracker(ConvergenceBudget{}), nil)
	seed := AppliedPlanOperation{PlanID: "existing"}
	res := &ConvergenceResult{
		Applied: 7,
		Journal: []AppliedPlanOperation{
			seed,
			{Rel: "a", Resource: HierarchyOperationWriteCPUs},
			{Rel: "b", Resource: HierarchyOperationWriteCPUs},
		},
	}

	err := writer.failFrozenTrace(
		context.Background(), errors.New("forward failed"), stack,
		rollbackOnlyTicket(2, 0), res, 1, 5)

	require.ErrorContains(t, err, "rollback failed")
	require.Equal(t, machine.MustParse("0"), live.nodes["a"].cpus,
		"successfully restored resource must not remain as net progress")
	require.Equal(t, machine.MustParse("1-2"), live.nodes["b"].cpus)
	require.Equal(t, 6, res.Applied)
	require.Len(t, res.Journal, 2)
	require.Equal(t, seed, res.Journal[0])
	require.Equal(t, "b", res.Journal[1].Rel)
	require.Equal(t, HierarchyOperationWriteCPUs, res.Journal[1].Resource)
	require.Equal(t, PhysicalImpactConfirmed, res.Journal[1].PhysicalImpact)
	require.Equal(t, "1-2", res.Journal[1].Observed.CPUs.String())
}

func TestPartialRollbackAttributesRepeatedResourceToStillEffectiveLogicalOperation(t *testing.T) {
	identity := CgroupIdentity{Device: 1, Inode: 1}
	stack := &traceMutationStack{
		writes: []AppliedPhysicalWrite{
			{
				PlanID: "zero-to-one", Rel: "a", Identity: identity,
				Direction: WriteGrow, Resource: HierarchyOperationWriteCPUs,
				Before: "0", BeforeEffective: "0", After: "0-1",
				Impact:                PhysicalImpactConfirmed,
				LogicalOperationIndex: 3, Phase: PhaseExpand,
			},
			{
				PlanID: "one-to-two", Rel: "a", Identity: identity,
				Direction: WriteGrow, Resource: HierarchyOperationWriteCPUs,
				Before: "0-1", BeforeEffective: "0-1", After: "0-2",
				Impact:                PhysicalImpactConfirmed,
				LogicalOperationIndex: 8, Phase: PhaseExpand,
			},
		},
		rollbackObservations: map[string]rollbackObservation{
			"a": {
				current: EntryState{
					Rel: "a", Identity: identity,
					CPUs: machine.MustParse("0-1"), ConfiguredCPUs: machine.MustParse("0-1"),
				},
			},
		},
	}
	res := &ConvergenceResult{}
	writer := newSafeCPUSetWriter(nil, nil, nil)

	writer.rebuildPhysicalImpactEvidence(stack, res, 0, 0)

	require.Equal(t, 1, res.Applied)
	require.Len(t, res.Journal, 1)
	evidence := res.Journal[0]
	require.Equal(t, "zero-to-one", evidence.PlanID)
	require.Equal(t, 3, evidence.LogicalOperationIndex)
	require.Equal(t, PhaseExpand, evidence.Phase)
	require.Equal(t, "0-1", evidence.Target.CPUs.String())
	require.Equal(t, "0-1", evidence.Observed.CPUs.String())
	require.Equal(t, PhysicalImpactConfirmed, evidence.PhysicalImpact)
}

func TestPartialRollbackMergesCPUAndMemsForOneLogicalOperation(t *testing.T) {
	identity := CgroupIdentity{Device: 1, Inode: 1}
	stack := &traceMutationStack{
		writes: []AppliedPhysicalWrite{
			{
				PlanID: "joint", Rel: "a", Identity: identity,
				Direction: WriteGrow, Resource: HierarchyOperationWriteMems,
				Before: "0", BeforeEffective: "0", After: "0-1",
				Impact:                PhysicalImpactConfirmed,
				LogicalOperationIndex: 5, Phase: PhaseExpand,
			},
			{
				PlanID: "joint", Rel: "a", Identity: identity,
				Direction: WriteGrow, Resource: HierarchyOperationWriteCPUs,
				Before: "0", BeforeEffective: "0", After: "0-1",
				Impact:                PhysicalImpactConfirmed,
				LogicalOperationIndex: 5, Phase: PhaseExpand,
			},
		},
		rollbackObservations: map[string]rollbackObservation{
			"a": {
				current: EntryState{
					Rel: "a", Identity: identity,
					CPUs: machine.MustParse("0-1"), ConfiguredCPUs: machine.MustParse("0-1"),
					Mems: "0-1", ConfiguredMems: "0-1",
				},
			},
		},
	}
	res := &ConvergenceResult{}
	writer := newSafeCPUSetWriter(nil, nil, nil)

	writer.rebuildPhysicalImpactEvidence(stack, res, 0, 0)

	require.Equal(t, 1, res.Applied)
	require.Len(t, res.Journal, 1)
	evidence := res.Journal[0]
	require.Equal(t, "joint", evidence.PlanID)
	require.Equal(t, 5, evidence.LogicalOperationIndex)
	require.Equal(t, PhaseExpand, evidence.Phase)
	require.Equal(t, "0-1", evidence.Target.CPUs.String())
	require.Equal(t, "0-1", evidence.Target.Mems)
	require.Equal(t, evidence.Target, evidence.Observed)
	require.Equal(t, PhysicalImpactConfirmed, evidence.PhysicalImpact)
}

func TestPartialRollbackReportsUnmatchedIntermediateAsUncertainObservedValue(t *testing.T) {
	identity := CgroupIdentity{Device: 1, Inode: 1}
	stack := &traceMutationStack{
		writes: []AppliedPhysicalWrite{
			{
				PlanID: "zero-to-two", Rel: "a", Identity: identity,
				Direction: WriteGrow, Resource: HierarchyOperationWriteCPUs,
				Before: "0", BeforeEffective: "0", After: "0-2",
				Impact:                PhysicalImpactConfirmed,
				LogicalOperationIndex: 2, Phase: PhaseExpand,
			},
		},
		rollbackObservations: map[string]rollbackObservation{
			"a": {
				current: EntryState{
					Rel: "a", Identity: identity,
					CPUs: machine.MustParse("0-1"), ConfiguredCPUs: machine.MustParse("0-1"),
				},
			},
		},
	}
	res := &ConvergenceResult{}
	writer := newSafeCPUSetWriter(nil, nil, nil)

	writer.rebuildPhysicalImpactEvidence(stack, res, 0, 0)

	require.Equal(t, 1, res.Applied)
	require.Len(t, res.Journal, 1)
	require.Equal(t, PhysicalImpactUncertain, res.Journal[0].PhysicalImpact)
	require.Equal(t, "0-2", res.Journal[0].Target.CPUs.String())
	require.Equal(t, "0-1", res.Journal[0].Observed.CPUs.String())
}

func TestUncertainPartialWriteReadbackRetainsIntermediateValue(t *testing.T) {
	for _, resource := range []HierarchyOperation{
		HierarchyOperationWriteCPUs,
		HierarchyOperationWriteMems,
	} {
		resource := resource
		t.Run(string(resource), func(t *testing.T) {
			live := newFakeHierarchyDriver()
			live.allowUnwitnessedExpansion = true
			identity := CgroupIdentity{Device: 1, Inode: 1}
			live.add("a", identity, "0", "0")
			writer := newSafeCPUSetWriter(
				live, NewBudgetTracker(ConvergenceBudget{}), nil)
			operation := PlanOperation{
				PlanID: "plan", Rel: "a", ExpectedIdentity: identity,
				Direction: WriteGrow,
			}
			after := "0-2"
			write, err := writer.capturePhysicalWriteBefore(
				context.Background(), operation, resource, after, 0, PhaseExpand)
			require.NoError(t, err)
			switch resource {
			case HierarchyOperationWriteCPUs:
				live.nodes["a"].configuredCPUs = machine.MustParse("0-1")
				live.nodes["a"].cpus = machine.MustParse("0-1")
			case HierarchyOperationWriteMems:
				live.nodes["a"].configuredMems = "0-1"
				live.nodes["a"].mems = "0-1"
			}
			stack := &traceMutationStack{}

			err = writer.recordUncertainPhysicalWrite(
				context.Background(), write, stack)

			require.NoError(t, err)
			require.Len(t, stack.writes, 1,
				"a successful read-back that differs from Before must be rolled back")
			require.Equal(t, PhysicalImpactUncertain, stack.writes[0].Impact)
		})
	}
}

func TestRollbackUsesTicketAfterOrdinaryHierarchyIOBudgetIsExhausted(t *testing.T) {
	live := newFakeHierarchyDriver()
	live.allowUnwitnessedExpansion = true
	live.add("a", CgroupIdentity{Device: 1, Inode: 1}, "0", "0")
	stack := &traceMutationStack{}
	applyCPUWriteForRollbackTest(t, live, stack, "a", "0", "0-1")
	budget := NewBudgetTracker(ConvergenceBudget{MaxHierarchyIOOperations: 1})
	require.NoError(t, budget.beforeHierarchyIOOperation(context.Background()))
	writer := newSafeCPUSetWriter(NewBudgetedHierarchyDriver(live, budget), budget, nil)
	ticket := rollbackOnlyTicket(1, 0)

	err := writer.rollbackTracePrefix(context.Background(), stack, ticket)

	require.NoError(t, err)
	require.Equal(t, machine.MustParse("0"), live.nodes["a"].cpus)
	require.Equal(t, 3, ticket.consumedRollbackIOOperations)
}

func TestRollbackTicketBoundsIndependentHierarchyIO(t *testing.T) {
	live := newFakeHierarchyDriver()
	live.allowUnwitnessedExpansion = true
	live.add("a", CgroupIdentity{Device: 1, Inode: 1}, "0", "0")
	stack := &traceMutationStack{}
	applyCPUWriteForRollbackTest(t, live, stack, "a", "0", "0-1")
	ticket := rollbackOnlyTicket(1, 0)
	ticket.rollbackIOOperations = 1
	writer := newSafeCPUSetWriter(live, NewBudgetTracker(ConvergenceBudget{}), nil)

	err := writer.rollbackTracePrefix(context.Background(), stack, ticket)

	require.ErrorIs(t, err, ErrAdmissionReservationExceeded)
	require.Equal(t, machine.MustParse("0-1"), live.nodes["a"].cpus)
	require.Equal(t, 1, ticket.consumedRollbackIOOperations)
}

func TestExecutionRollbackErrorPreservesPlanStaleChain(t *testing.T) {
	executionErr := &PlanStaleError{
		Rel: "a", Direction: WriteShrink, Resource: "cpuset.cpus",
		Current: "0-1", Target: "0",
	}
	rollbackErr := errors.New("rollback failed")

	err := newExecutionRollbackError(executionErr, rollbackErr)

	require.Equal(t,
		"execution failed: coordinator plan is stale and requires replan: rel=\"a\" direction=shrink resource=cpuset.cpus current=0-1 target=0; rollback failed: rollback failed",
		err.Error())
	require.ErrorIs(t, err, ErrCoordinatorPlanStale)
	var stale *PlanStaleError
	require.ErrorAs(t, err, &stale)
	require.Same(t, executionErr, stale)
	_, hasSingleUnwrap := err.(interface{ Unwrap() error })
	require.True(t, hasSingleUnwrap, "Go 1.18 requires a single-error unwrap chain")
}

func TestRollbackIdentityDriftDoesNotWriteReplacementGeneration(t *testing.T) {
	live := newFakeHierarchyDriver()
	live.allowUnwitnessedExpansion = true
	identity := CgroupIdentity{Device: 1, Inode: 1}
	live.add("a", identity, "0", "0")
	stack := &traceMutationStack{}
	applyCPUWriteForRollbackTest(t, live, stack, "a", "0", "0-1")
	writesBeforeRollback := live.PhysicalWriteCount()
	live.bumpIdentity("a")
	writer := newSafeCPUSetWriter(live, NewBudgetTracker(ConvergenceBudget{}), nil)

	err := writer.rollbackTracePrefix(context.Background(), stack, rollbackOnlyTicket(1, 0))

	require.ErrorIs(t, err, ErrCgroupIdentityChanged)
	require.Equal(t, writesBeforeRollback, live.PhysicalWriteCount())
	require.Equal(t, machine.MustParse("0-1"), live.nodes["a"].cpus)
}

func TestRollbackPinnedReadSkipsAlreadyRestoredStateWithoutWriteBudget(t *testing.T) {
	for _, resource := range []HierarchyOperation{
		HierarchyOperationWriteCPUs,
		HierarchyOperationWriteMems,
	} {
		resource := resource
		t.Run(string(resource), func(t *testing.T) {
			live := newFakeHierarchyDriver()
			live.allowUnwitnessedExpansion = true
			identity := CgroupIdentity{Device: 1, Inode: 1}
			live.add("a", identity, "0", "0")
			write := AppliedPhysicalWrite{
				PlanID: "plan", Rel: "a", Identity: identity,
				Direction: WriteGrow, Resource: resource,
				Before: "0", BeforeEffective: "0",
				After: "0-1", AfterEffective: "0-1",
				Impact: PhysicalImpactConfirmed,
			}
			stack := &traceMutationStack{writes: []AppliedPhysicalWrite{write}}
			cpuWrites, memWrites := 0, 0
			if resource == HierarchyOperationWriteCPUs {
				cpuWrites = 1
			} else {
				memWrites = 1
			}
			ticket := rollbackOnlyTicket(cpuWrites, memWrites)
			writer := newSafeCPUSetWriter(live, NewBudgetTracker(ConvergenceBudget{}), nil)

			err := writer.rollbackTracePrefix(context.Background(), stack, ticket)

			require.NoError(t, err)
			require.Zero(t, live.PhysicalWriteCount())
			require.Zero(t, ticket.consumedRollback.Total())
		})
	}
}

func TestRollbackPinnedReadRejectsSameGenerationThirdStateAndPreservesImpact(t *testing.T) {
	for _, resource := range []HierarchyOperation{
		HierarchyOperationWriteCPUs,
		HierarchyOperationWriteMems,
	} {
		resource := resource
		t.Run(string(resource), func(t *testing.T) {
			live := newFakeHierarchyDriver()
			live.allowUnwitnessedExpansion = true
			identity := CgroupIdentity{Device: 1, Inode: 1}
			live.add("a", identity, "0", "0")
			write := AppliedPhysicalWrite{
				PlanID: "plan", Rel: "a", Identity: identity,
				Direction: WriteGrow, Resource: resource,
				Before: "0", BeforeEffective: "0",
				After: "0-1", AfterEffective: "0-1",
				Impact:                PhysicalImpactConfirmed,
				LogicalOperationIndex: 2, Phase: PhaseExpand,
			}
			cpuWrites, memWrites := 0, 0
			switch resource {
			case HierarchyOperationWriteCPUs:
				cpuWrites = 1
				live.nodes["a"].configuredCPUs = machine.MustParse(write.After)
				live.nodes["a"].cpus = machine.MustParse("0-2")
			case HierarchyOperationWriteMems:
				memWrites = 1
				live.nodes["a"].configuredMems = write.After
				live.nodes["a"].mems = "0-2"
			}
			stack := &traceMutationStack{writes: []AppliedPhysicalWrite{write}}
			ticket := rollbackOnlyTicket(cpuWrites, memWrites)
			res := &ConvergenceResult{}
			writer := newSafeCPUSetWriter(live, NewBudgetTracker(ConvergenceBudget{}), res)

			err := writer.failFrozenTrace(
				context.Background(), errors.New("forward failed"), stack, ticket, res, 0, 0)

			require.ErrorContains(t, err, "neither rollback after nor before")
			require.Zero(t, live.PhysicalWriteCount())
			require.Zero(t, ticket.consumedRollback.Total())
			require.Len(t, res.Journal, 1)
			require.Equal(t, PhysicalImpactUncertain, res.Journal[0].PhysicalImpact)
			if resource == HierarchyOperationWriteCPUs {
				require.Equal(t, "0-2", res.Journal[0].Observed.CPUs.String())
			} else {
				require.Equal(t, "0-2", res.Journal[0].Observed.Mems)
			}
		})
	}
}

func TestRepeatedRelationRollsBackToInvocationInitialValue(t *testing.T) {
	live := newFakeHierarchyDriver()
	live.allowUnwitnessedExpansion = true
	live.add("a", CgroupIdentity{Device: 1, Inode: 1}, "0", "0")
	stack := &traceMutationStack{}
	applyCPUWriteForRollbackTest(t, live, stack, "a", "0", "0-1")
	applyCPUWriteForRollbackTest(t, live, stack, "a", "0-1", "0-2")
	writer := newSafeCPUSetWriter(live, NewBudgetTracker(ConvergenceBudget{}), nil)

	err := writer.rollbackTracePrefix(context.Background(), stack, rollbackOnlyTicket(2, 0))

	require.NoError(t, err)
	require.Equal(t, machine.MustParse("0"), live.nodes["a"].cpus)
	require.Equal(t, []string{"0-1", "0"}, cpuWriteTargets(live.writes[2:]))
}

func TestFrozenTraceRollbackRestoresV2EmptyConfiguredState(t *testing.T) {
	for _, resource := range []HierarchyOperation{
		HierarchyOperationWriteCPUs,
		HierarchyOperationWriteMems,
	} {
		resource := resource
		t.Run(string(resource), func(t *testing.T) {
			writer, live, operation, ticket := v2InheritedRollbackFixture(t, resource)
			ticket.operations = make([]frozenOperationAuthorization, 12)
			for i := range ticket.operations {
				ticket.operations[i].operation = operation
			}
			ticket.nextOperation = 11
			stack := &traceMutationStack{}

			_, err := writer.applyFrozenOperation(
				context.Background(), PhaseDrain, 11, operation,
				preflightForDirectOperation(t, live, operation), stack, ticket, "direct")
			require.NoError(t, err)
			require.Len(t, stack.writes, 1)
			require.Equal(t, 11, stack.writes[0].LogicalOperationIndex)
			require.Equal(t, PhaseDrain, stack.writes[0].Phase)
			require.Empty(t, stack.writes[0].Before,
				"mutation before must preserve the empty configured inheritance value")

			require.NoError(t, writer.rollbackTracePrefix(
				context.Background(), stack, ticket))
			assertV2InheritedState(t, live, operation.Rel)
		})
	}
}

func TestFrozenTraceUncertainWriteRollbackRestoresV2EmptyConfiguredState(t *testing.T) {
	for _, resource := range []HierarchyOperation{
		HierarchyOperationWriteCPUs,
		HierarchyOperationWriteMems,
	} {
		resource := resource
		t.Run(string(resource), func(t *testing.T) {
			writer, live, operation, ticket := v2InheritedRollbackFixture(t, resource)
			injected := &injectedTraceDriver{
				HierarchyDriver: live,
				injection: traceFailureInjection{
					mutateThenFailWriteAt: 1,
					failUncertainReadback: true,
				},
			}
			writer.driver = injected
			stack := &traceMutationStack{}

			_, err := writer.applyFrozenOperation(
				context.Background(), PhaseDrain, 0, operation,
				preflightForDirectOperation(t, live, operation), stack, ticket, "direct")
			require.ErrorContains(t, err, "injected uncertain")
			require.Len(t, stack.writes, 1)
			require.Equal(t, PhysicalImpactUncertain, stack.writes[0].Impact)
			require.Empty(t, stack.writes[0].Before,
				"uncertain mutation must retain the real empty configured value")

			require.NoError(t, writer.rollbackTracePrefix(
				context.Background(), stack, ticket))
			assertV2InheritedState(t, live, operation.Rel)
		})
	}
}

func v2InheritedRollbackFixture(
	t *testing.T,
	resource HierarchyOperation,
) (safeCPSetWriter, *fakeHierarchyDriver, PlanOperation, *ExecutionReservationTicket) {
	t.Helper()
	live := newFakeHierarchyDriver()
	live.allowUnwitnessedExpansion = true
	live.capabilities = cgroupV2Policy.capabilities(true)
	live.add("root", CgroupIdentity{Device: 1, Inode: 1}, "0-3", "0-1")
	live.add("root/leaf", CgroupIdentity{Device: 1, Inode: 2}, "0-3", "0-1")
	live.nodes["root/leaf"].configuredCPUs = machine.NewCPUSet()
	live.nodes["root/leaf"].configuredMems = ""

	operation := PlanOperation{
		Rel:              "root/leaf",
		ExpectedIdentity: live.nodes["root/leaf"].identity,
		ExpectedCurrent:  CPUSetTarget{CPUs: machine.MustParse("0-3"), Mems: "0-1"},
		Target:           CPUSetTarget{CPUs: machine.MustParse("0-3"), Mems: "0-1"},
		Direction:        WriteShrink,
		OwnsMems:         true,
	}
	forward := PhysicalWriteCost{}
	switch resource {
	case HierarchyOperationWriteCPUs:
		operation.Target.CPUs = machine.MustParse("0-1")
		forward.CPUSetWrites = 1
	case HierarchyOperationWriteMems:
		operation.Target.Mems = "0"
		operation.WriteMems = true
		forward.MemsWrites = 1
	default:
		t.Fatalf("unsupported resource %q", resource)
	}
	ticket := &ExecutionReservationTicket{
		traceID:    "direct",
		operations: []frozenOperationAuthorization{{operation: operation}},
		reserved: ExecutionReservationCost{
			Forward:  forward,
			Rollback: forward,
		},
		rollbackIOOperations: 3,
	}
	return newSafeCPUSetWriter(
		live, NewBudgetTracker(ConvergenceBudget{}), nil,
	), live, operation, ticket
}

func preflightForDirectOperation(
	t *testing.T,
	live *fakeHierarchyDriver,
	operation PlanOperation,
) frozenOperationPreflight {
	t.Helper()
	current, err := live.ReadEntry(context.Background(), operation.Rel)
	require.NoError(t, err)
	after := current
	if !operation.ExpectedCurrent.CPUs.Equals(operation.Target.CPUs) {
		after.ConfiguredCPUs = operation.Target.CPUs.Clone()
		after.CPUs = operation.Target.CPUs.Clone()
	}
	if operation.WriteMems {
		after.ConfiguredMems = operation.Target.Mems
		after.Mems = operation.Target.Mems
	}
	return frozenOperationPreflight{
		before: freezeOperationState(current),
		after:  freezeOperationState(after),
		children: stableLiveChildren{
			cpus:  machine.NewCPUSet(),
			mems:  machine.NewCPUSet(),
			byRel: make(map[string]EntryState),
		},
	}
}

func assertV2InheritedState(t *testing.T, live *fakeHierarchyDriver, rel string) {
	t.Helper()
	current, err := live.ReadEntry(context.Background(), rel)
	require.NoError(t, err)
	require.True(t, current.ConfiguredCPUs.IsEmpty())
	require.Empty(t, current.ConfiguredMems)
	require.Equal(t, "0-3", current.CPUs.String())
	require.Equal(t, "0-1", current.Mems)
}

func TestRollbackVerificationRelsAreSorted(t *testing.T) {
	stack := &traceMutationStack{writes: []AppliedPhysicalWrite{
		{Rel: "z"},
		{Rel: "a"},
		{Rel: "m"},
		{Rel: "a"},
	}}

	require.Equal(t, []string{"a", "m", "z"}, rollbackVerificationRels(stack))
}

func TestUncertainPhysicalImpactIsNotVerifiedProgress(t *testing.T) {
	outcome := RoundOutcome{Journal: []AppliedPlanOperation{{
		Rel: "a",
		Target: CPUSetTarget{
			CPUs: machine.NewCPUSet(0),
			Mems: "0",
		},
		Observed: CPUSetTarget{
			CPUs: machine.NewCPUSet(0),
			Mems: "0",
		},
		PhysicalImpact: PhysicalImpactUncertain,
	}}}

	require.False(t, roundOutcomeMadeNetProgress(outcome))
}

func TestSuccessfulRollbackRemovesNetJournalAndAppliedProgress(t *testing.T) {
	trace, live := compiledTraceWithCPUAndMemoryWrites(t)
	live.invariants = nil
	driver := &injectedTraceDriver{
		HierarchyDriver: live,
		injection:       traceFailureInjection{failWriteAt: 3},
	}
	round := frozenExecutionRound(t, trace, driver)
	ticket := reserveTraceWithBudget(t, round.budget, trace)
	seed := AppliedPlanOperation{PlanID: "existing"}
	res := &ConvergenceResult{
		Applied:              4,
		Journal:              []AppliedPlanOperation{seed},
		ParentSafe:           true,
		Converged:            true,
		FinalSnapshotCurrent: true,
	}

	_, err := round.executeFrozenTrace(context.Background(), trace, ticket, res)

	require.Error(t, err)
	require.Equal(t, []AppliedPlanOperation{seed}, res.Journal)
	require.Equal(t, 4, res.Applied)
	require.False(t, res.ParentSafe)
	require.False(t, res.Converged)
	require.False(t, res.FinalSnapshotCurrent)
	require.Positive(t, ticket.consumedRollback.Total())
}

func TestFrozenTraceExecutionPublishesOnlyFreshFinalProof(t *testing.T) {
	trace, live := compiledTraceWithCPUAndMemoryWrites(t)
	live.invariants = nil
	round := frozenExecutionRound(t, trace, live)
	ticket := reserveTraceWithBudget(t, round.budget, trace)
	res := &ConvergenceResult{}

	outcome, err := round.executeFrozenTrace(context.Background(), trace, ticket, res)

	require.NoError(t, err)
	require.Equal(t, trace.FinalSnapshot.ID, outcome.Snapshot.ID)
	require.Equal(t, trace.FinalSnapshot.ID, res.FinalSnapshot.ID)
	require.True(t, res.FinalSnapshotCurrent)
	require.True(t, res.ParentSafe)
	require.Equal(t, len(flattenTraceOperations(trace)), res.Applied)
	require.Equal(t, res.Applied, len(res.Journal))
	require.Equal(t, res.Journal, outcome.Journal)
	require.Zero(t, ticket.consumedRollback.Total())
}

type expectedTraceWrite struct {
	phase    PhaseKind
	resource HierarchyOperation
}

func expectedPhysicalWrites(trace *CompiledPhaseTrace) []expectedTraceWrite {
	var writes []expectedTraceWrite
	for _, phase := range trace.Phases {
		for _, operation := range phase.Operations {
			if operation.WriteMems && operation.ExpectedCurrent.Mems != operation.Target.Mems {
				writes = append(writes, expectedTraceWrite{phase: phase.Kind, resource: HierarchyOperationWriteMems})
			}
			if !operation.ExpectedCurrent.CPUs.Equals(operation.Target.CPUs) {
				writes = append(writes, expectedTraceWrite{phase: phase.Kind, resource: HierarchyOperationWriteCPUs})
			}
		}
	}
	return writes
}

func failAtNthResourceWrite(
	t *testing.T,
	writes []expectedTraceWrite,
	resource HierarchyOperation,
	n int,
) traceFailureInjection {
	t.Helper()
	seen := 0
	for i, write := range writes {
		if write.resource == resource {
			seen++
			if seen == n {
				return traceFailureInjection{failWriteAt: i + 1}
			}
		}
	}
	t.Fatalf("trace has only %d %s writes, need %d", seen, resource, n)
	return traceFailureInjection{}
}

func failAtPhaseWrite(
	t *testing.T,
	writes []expectedTraceWrite,
	phase PhaseKind,
	phaseWriteIndex int,
) traceFailureInjection {
	t.Helper()
	var indexes []int
	for i, write := range writes {
		if write.phase == phase {
			indexes = append(indexes, i+1)
		}
	}
	require.NotEmpty(t, indexes, "trace has no %s writes", phase)
	if phaseWriteIndex < 0 {
		phaseWriteIndex = len(indexes) - 1
	}
	require.Less(t, phaseWriteIndex, len(indexes))
	return traceFailureInjection{failWriteAt: indexes[phaseWriteIndex]}
}

type traceFailureInjection struct {
	failWriteAt                   int
	mutateThenFailWriteAt         int
	falseSuccessWriteAt           int
	failPostWriteReadback         bool
	failUncertainReadback         bool
	driftIdentityOnFailedReadback bool
	failFinalProof                bool
	cancelAfterWrite              int
	blockAfterWriteUntilDeadline  int
	blockRollbackIdentity         bool
}

type injectedTraceDriver struct {
	HierarchyDriver
	injection       traceFailureInjection
	expectedForward int
	writeCalls      int
	forwardSuccess  int
	pendingReadback bool
	injected        bool
	cancel          context.CancelFunc
}

type preWriteDriftDriver struct {
	HierarchyDriver
	live                 *fakeHierarchyDriver
	initialRels          map[string]struct{}
	preflightSeen        map[string]struct{}
	preflightComplete    bool
	driftAfterWriteCount int
	driftRel             string
	driftedRel           string
	drifted              bool
	forwardWrites        int
}

type frozenPredecessorDriftDriver struct {
	HierarchyDriver
	live                     *fakeHierarchyDriver
	initialRels              map[string]struct{}
	preflightSeen            map[string]struct{}
	preflightComplete        bool
	operation                PlanOperation
	mutate                   func(*fakeHierarchyDriver, PlanOperation)
	drifted                  bool
	writesToDriftedOperation int
}

func selectTraceOperation(
	t *testing.T,
	trace *CompiledPhaseTrace,
	matches func(PlanOperation, *CompiledPhaseTrace) bool,
) PlanOperation {
	t.Helper()
	for _, operation := range flattenTraceOperations(trace) {
		if matches(operation, trace) {
			return operation
		}
	}
	t.Fatal("compiled trace has no operation matching predecessor drift case")
	return PlanOperation{}
}

func traceChildRel(t *testing.T, parentRel string, live *fakeHierarchyDriver) string {
	t.Helper()
	children := make([]string, 0)
	for rel := range live.nodes {
		if filepath.Dir(rel) == parentRel {
			children = append(children, rel)
		}
	}
	require.NotEmpty(t, children, "predecessor drift case requires a child")
	sort.Strings(children)
	return children[0]
}

func newFrozenPredecessorDriftDriver(
	live *fakeHierarchyDriver,
	initial *CompleteSnapshot,
	operation PlanOperation,
	mutate func(*fakeHierarchyDriver, PlanOperation),
) *frozenPredecessorDriftDriver {
	rels := make(map[string]struct{}, len(initial.Entries))
	for rel := range initial.Entries {
		rels[rel] = struct{}{}
	}
	return &frozenPredecessorDriftDriver{
		HierarchyDriver: live,
		live:            live,
		initialRels:     rels,
		preflightSeen:   make(map[string]struct{}, len(rels)),
		operation:       operation,
		mutate:          mutate,
	}
}

func (d *frozenPredecessorDriftDriver) ReadEntry(
	ctx context.Context,
	rel string,
) (EntryState, error) {
	if !d.preflightComplete {
		if _, expected := d.initialRels[rel]; expected {
			d.preflightSeen[rel] = struct{}{}
			if len(d.preflightSeen) == len(d.initialRels) {
				d.preflightComplete = true
			}
		}
		return d.HierarchyDriver.ReadEntry(ctx, rel)
	}
	if !d.drifted && rel == d.operation.Rel {
		d.mutate(d.live, d.operation)
		d.drifted = true
	}
	return d.HierarchyDriver.ReadEntry(ctx, rel)
}

func (d *frozenPredecessorDriftDriver) WriteCPUs(
	ctx context.Context,
	rel string,
	identity CgroupIdentity,
	cpus machine.CPUSet,
) error {
	if d.drifted && rel == d.operation.Rel {
		d.writesToDriftedOperation++
	}
	return d.HierarchyDriver.WriteCPUs(ctx, rel, identity, cpus)
}

func (d *frozenPredecessorDriftDriver) WriteMems(
	ctx context.Context,
	rel string,
	identity CgroupIdentity,
	mems string,
) error {
	if d.drifted && rel == d.operation.Rel {
		d.writesToDriftedOperation++
	}
	return d.HierarchyDriver.WriteMems(ctx, rel, identity, mems)
}

func newPreWriteDriftDriver(
	live *fakeHierarchyDriver,
	initial *CompleteSnapshot,
	driftAfterWriteCount int,
	driftRel string,
) *preWriteDriftDriver {
	rels := make(map[string]struct{}, len(initial.Entries))
	for rel := range initial.Entries {
		rels[rel] = struct{}{}
	}
	return &preWriteDriftDriver{
		HierarchyDriver:      live,
		live:                 live,
		initialRels:          rels,
		preflightSeen:        make(map[string]struct{}, len(rels)),
		driftAfterWriteCount: driftAfterWriteCount,
		driftRel:             driftRel,
	}
}

func (d *preWriteDriftDriver) ReadEntry(ctx context.Context, rel string) (EntryState, error) {
	if !d.preflightComplete {
		if _, expected := d.initialRels[rel]; expected {
			d.preflightSeen[rel] = struct{}{}
			if len(d.preflightSeen) == len(d.initialRels) {
				d.preflightComplete = true
			}
		}
		return d.HierarchyDriver.ReadEntry(ctx, rel)
	}
	if !d.drifted && d.forwardWrites >= d.driftAfterWriteCount &&
		(d.driftRel == "" || d.driftRel == rel) {
		node := d.live.nodes[rel]
		node.configuredCPUs = node.configuredCPUs.Union(machine.NewCPUSet(99))
		node.cpus = node.cpus.Union(machine.NewCPUSet(99))
		d.driftedRel = rel
		d.drifted = true
	}
	return d.HierarchyDriver.ReadEntry(ctx, rel)
}

func (d *preWriteDriftDriver) WriteCPUs(
	ctx context.Context,
	rel string,
	identity CgroupIdentity,
	cpus machine.CPUSet,
) error {
	d.forwardWrites++
	return d.HierarchyDriver.WriteCPUs(ctx, rel, identity, cpus)
}

func (d *preWriteDriftDriver) WriteMems(
	ctx context.Context,
	rel string,
	identity CgroupIdentity,
	mems string,
) error {
	d.forwardWrites++
	return d.HierarchyDriver.WriteMems(ctx, rel, identity, mems)
}

func (d *injectedTraceDriver) StatIdentity(ctx context.Context, rel string) (CgroupIdentity, error) {
	if d.injected && d.injection.blockRollbackIdentity {
		<-ctx.Done()
		return CgroupIdentity{}, ctx.Err()
	}
	return d.HierarchyDriver.StatIdentity(ctx, rel)
}

func (d *injectedTraceDriver) Roots(ctx context.Context) ([]RootRef, error) {
	if !d.injected && d.injection.failFinalProof && d.forwardSuccess == d.expectedForward {
		d.injected = true
		return nil, errors.New("injected final proof failure")
	}
	return d.HierarchyDriver.Roots(ctx)
}

func (d *injectedTraceDriver) ReadEntry(ctx context.Context, rel string) (EntryState, error) {
	if !d.injected && d.pendingReadback {
		d.pendingReadback = false
		d.injected = true
		if d.injection.driftIdentityOnFailedReadback {
			if live, ok := d.HierarchyDriver.(*fakeHierarchyDriver); ok {
				live.bumpIdentity(rel)
			}
		}
		return EntryState{}, errors.New("injected post-write read-back failure")
	}
	if d.pendingReadback {
		d.pendingReadback = false
		if d.injection.driftIdentityOnFailedReadback {
			if live, ok := d.HierarchyDriver.(*fakeHierarchyDriver); ok {
				live.bumpIdentity(rel)
			}
		}
		return EntryState{}, errors.New("injected uncertain write read-back failure")
	}
	if d.injected && d.injection.blockRollbackIdentity {
		<-ctx.Done()
		return EntryState{}, ctx.Err()
	}
	return d.HierarchyDriver.ReadEntry(ctx, rel)
}

func (d *injectedTraceDriver) ListChildren(ctx context.Context, rel string) ([]ChildRef, error) {
	if !d.injected && d.injection.failFinalProof && d.forwardSuccess == d.expectedForward {
		d.injected = true
		return nil, errors.New("injected final proof failure")
	}
	return d.HierarchyDriver.ListChildren(ctx, rel)
}

func (d *injectedTraceDriver) WriteCPUs(
	ctx context.Context,
	rel string,
	identity CgroupIdentity,
	cpus machine.CPUSet,
) error {
	return d.write(ctx, HierarchyOperationWriteCPUs, rel, func() error {
		return d.HierarchyDriver.WriteCPUs(ctx, rel, identity, cpus)
	})
}

func (d *injectedTraceDriver) WriteMems(
	ctx context.Context,
	rel string,
	identity CgroupIdentity,
	mems string,
) error {
	return d.write(ctx, HierarchyOperationWriteMems, rel, func() error {
		return d.HierarchyDriver.WriteMems(ctx, rel, identity, mems)
	})
}

func (d *injectedTraceDriver) write(
	ctx context.Context,
	resource HierarchyOperation,
	rel string,
	delegate func() error,
) error {
	d.writeCalls++
	if !d.injected && d.injection.failWriteAt == d.writeCalls {
		d.injected = true
		return fmt.Errorf("injected %s failure for %q", resource, rel)
	}
	if !d.injected && d.injection.mutateThenFailWriteAt == d.writeCalls {
		if err := delegate(); err != nil {
			return err
		}
		if d.injection.failUncertainReadback {
			d.pendingReadback = true
		}
		d.injected = true
		return fmt.Errorf("injected uncertain %s failure for %q", resource, rel)
	}
	if !d.injected && d.injection.falseSuccessWriteAt == d.writeCalls {
		d.injected = true
		d.forwardSuccess++
		return nil
	}
	err := delegate()
	if err != nil {
		return err
	}
	if !d.injected {
		d.forwardSuccess++
		if d.injection.failPostWriteReadback {
			d.pendingReadback = true
		}
		if d.injection.blockAfterWriteUntilDeadline == d.forwardSuccess {
			d.injected = true
			<-ctx.Done()
		}
		if d.injection.cancelAfterWrite == d.forwardSuccess {
			d.injected = true
			if d.cancel != nil {
				d.cancel()
			}
		}
	}
	return nil
}

func frozenExecutionRound(
	t *testing.T,
	trace *CompiledPhaseTrace,
	driver HierarchyDriver,
) *coordinatorRound {
	t.Helper()
	return &coordinatorRound{
		dag:       mustBuildTraceDAG(t, trace),
		driver:    driver,
		budget:    NewBudgetTracker(ConvergenceBudget{}),
		objective: trace.Objective,
	}
}

func reserveTraceWithBudget(
	t *testing.T,
	budget *BudgetTracker,
	trace *CompiledPhaseTrace,
) *ExecutionReservationTicket {
	t.Helper()
	ticket, err := budget.ReservePhaseTrace(trace, trace.Cost.Total())
	require.NoError(t, err)
	return ticket
}

func rollbackOnlyTicket(cpuWrites, memWrites int) *ExecutionReservationTicket {
	rollback := PhysicalWriteCost{CPUSetWrites: cpuWrites, MemsWrites: memWrites}
	return &ExecutionReservationTicket{
		reserved: ExecutionReservationCost{
			Rollback: rollback,
		},
		rollbackIOOperations: saturatingMultiply(rollback.Total(), 3),
	}
}

func applyCPUWriteForRollbackTest(
	t *testing.T,
	driver *fakeHierarchyDriver,
	stack *traceMutationStack,
	rel, before, after string,
) {
	t.Helper()
	identity := driver.nodes[rel].identity
	require.NoError(t, driver.WriteCPUs(context.Background(), rel, identity, machine.MustParse(after)))
	stack.writes = append(stack.writes, AppliedPhysicalWrite{
		Rel: rel, Identity: identity, Resource: HierarchyOperationWriteCPUs,
		Before: before, BeforeEffective: before, After: after,
	})
}

func cpuWriteTargets(writes []fakeHierarchyWrite) []string {
	targets := make([]string, 0, len(writes))
	for _, write := range writes {
		targets = append(targets, write.cpus.String())
	}
	return targets
}

func newTracePreflightWriter(driver *fakeHierarchyDriver) safeCPSetWriter {
	return newSafeCPUSetWriter(driver, NewBudgetTracker(ConvergenceBudget{}), nil)
}

func operationWithCapturedChildren(t *testing.T, trace *CompiledPhaseTrace) PlanOperation {
	t.Helper()
	for _, operation := range flattenTraceOperations(trace) {
		if len(trace.InitialSnapshot.Children[operation.Rel]) > 0 {
			return operation
		}
	}
	t.Fatal("compiled trace has no operation with captured children")
	return PlanOperation{}
}

func operationWithParent(t *testing.T, trace *CompiledPhaseTrace) PlanOperation {
	t.Helper()
	for _, operation := range flattenTraceOperations(trace) {
		if operation.ParentRel != "" {
			return operation
		}
	}
	t.Fatal("compiled trace has no operation with a parent")
	return PlanOperation{}
}

func replaceTraceOperation(t *testing.T, trace *CompiledPhaseTrace, replacement PlanOperation) {
	t.Helper()
	for phaseIndex := range trace.Phases {
		for operationIndex := range trace.Phases[phaseIndex].Operations {
			operation := &trace.Phases[phaseIndex].Operations[operationIndex]
			if operation.PlanID == replacement.PlanID && operation.Rel == replacement.Rel {
				*operation = replacement
				trace.TraceID = ""
				trace.Cost = executionReservationCost(trace.Phases)
				return
			}
		}
	}
	t.Fatalf("compiled trace operation for rel %q was not found", replacement.Rel)
}

func mustBuildTraceDAG(t *testing.T, trace *CompiledPhaseTrace) *TopoDAG {
	t.Helper()
	dag, err := BuildDAG(cloneNodeSpecs(trace.EvaluationInput.DAGSpecs))
	require.NoError(t, err)
	return dag
}

func traceRequiresEarlierParentOverlay(trace *CompiledPhaseTrace) bool {
	if trace == nil || trace.InitialSnapshot == nil {
		return false
	}
	seen := make(map[string]struct{})
	for _, operation := range flattenTraceOperations(trace) {
		if operation.ParentRel != "" {
			parent, ok := trace.InitialSnapshot.Entries[operation.ParentRel]
			_, parentWrittenEarlier := seen[operation.ParentRel]
			if ok && parentWrittenEarlier && !operation.Target.CPUs.IsSubsetOf(parent.CPUs) {
				return true
			}
		}
		seen[operation.Rel] = struct{}{}
	}
	return false
}
