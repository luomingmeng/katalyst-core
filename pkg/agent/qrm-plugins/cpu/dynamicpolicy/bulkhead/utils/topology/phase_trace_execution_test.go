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
	"path/filepath"
	"testing"

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
