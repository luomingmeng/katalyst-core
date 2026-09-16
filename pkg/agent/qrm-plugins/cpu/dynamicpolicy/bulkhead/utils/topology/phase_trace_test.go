/*
Copyright 2022 The Katalyst Authors.

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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// admissionTraceFixture wires a coordinatorRound to a fake hierarchy driver so
// tests can compile a complete Drain->Expand fixed point on a cloned snapshot
// without touching any live driver. The live fake driver is only inspected to
// prove that compilation performs zero physical writes.
type admissionTraceFixture struct {
	t *testing.T

	// driver is the live hierarchy driver. Compilation must never write to it.
	driver *fakeHierarchyDriver
	// projectedDriver records writes attempted against the projected clone and
	// can be told to drop them to simulate a non-converging fixed point.
	projectedDriver *recordingProjectedDriver

	round     *coordinatorRound
	selection DrainSelectionPolicy

	specs          []NodeSpec
	targetByRel    map[string]machine.CPUSet
	requiredByRel  map[string]machine.CPUSet
	dynamicByRel   map[string]machine.CPUSet
	canonicalByRel map[string]machine.CPUSet

	cpuDetails machine.CPUDetails
	budget     *BudgetTracker

	nextInode uint64
}

// recordingProjectedDriver is a test double for the clone-backed projection the
// compiler uses. IgnoreWrites drops every projected mutation so the fixed point
// can never make progress.
type recordingProjectedDriver struct {
	IgnoreWrites bool
	writes       []fakeHierarchyWrite
}

// liveTraceSession adapts the live fake driver to the phase execution session
// contract so the engine can be replayed against real snapshots for parity.
type liveTraceSession struct {
	round *coordinatorRound
	base  *CompleteSnapshot
}

func newLiveTraceSession(round *coordinatorRound, base *CompleteSnapshot) *liveTraceSession {
	return &liveTraceSession{round: round, base: base}
}

func (s *liveTraceSession) Snapshot(ctx context.Context) (*CompleteSnapshot, error) {
	if s.round.snapshotSource == nil {
		return s.base, nil
	}
	return s.round.snapshotSource(ctx)
}

func (s *liveTraceSession) Apply(ctx context.Context, phase PhaseKind, operations []PlanOperation) error {
	for _, op := range operations {
		if op.WriteMems {
			if err := s.round.driver.WriteMems(ctx, op.Rel, op.ExpectedIdentity, op.Target.Mems); err != nil {
				return err
			}
			continue
		}
		if err := s.round.driver.WriteCPUs(ctx, op.Rel, op.ExpectedIdentity, op.Target.CPUs); err != nil {
			return err
		}
	}
	return nil
}

func newAdmissionTraceFixture(t *testing.T) *admissionTraceFixture {
	t.Helper()
	f := &admissionTraceFixture{
		t:               t,
		driver:          newFakeHierarchyDriver(),
		projectedDriver: &recordingProjectedDriver{},
		selection:       DefaultDrainSelectionPolicy(),
		targetByRel:     map[string]machine.CPUSet{},
		requiredByRel:   map[string]machine.CPUSet{},
		dynamicByRel:    map[string]machine.CPUSet{},
		canonicalByRel:  map[string]machine.CPUSet{},
		cpuDetails: machine.CPUDetails{
			0: {NUMANodeID: 0},
			1: {NUMANodeID: 0},
			2: {NUMANodeID: 0},
			3: {NUMANodeID: 0},
		},
		budget:    NewBudgetTracker(ConvergenceBudget{}),
		nextInode: 1,
	}
	// Unwitnessed expansion is authorized by the engine's own proof, not by the
	// invariant driver used only for negative low-level tests here.
	f.driver.allowUnwitnessedExpansion = true
	f.round = &coordinatorRound{
		objective:    ConvergenceObjectiveParentSafe,
		budget:       f.budget,
		cpuDetails:   f.cpuDetails,
		reservedCPUs: machine.NewCPUSet(),
		blocked:      map[DomainID]machine.CPUSet{},
		maxRounds:    64,
	}
	return f
}

func (f *admissionTraceFixture) allocInode() uint64 {
	inode := f.nextInode
	f.nextInode++
	return inode
}

func (f *admissionTraceFixture) parentOf(rel string) string {
	if idx := strings.LastIndex(rel, "/"); idx >= 0 {
		return rel[:idx]
	}
	return ""
}

func (f *admissionTraceFixture) addPrimary(rel, cpus, mems string) {
	f.driver.add(rel, CgroupIdentity{Device: 1, Inode: f.allocInode()}, cpus, mems)
	f.specs = append(f.specs, NodeSpec{
		Rel: rel, Role: TopoNodeRolePrimary, Domain: DomainPrimary,
		CPUs: machine.MustParse(cpus), Mems: mems,
		ParentRel: f.parentOf(rel), TrustAnchor: f.parentOf(rel) == "",
	})
	f.targetByRel[rel] = machine.MustParse(cpus)
}

func (f *admissionTraceFixture) addDynamicDescendant(rel, cpus, mems string) {
	f.driver.add(rel, CgroupIdentity{Device: 1, Inode: f.allocInode()}, cpus, mems)
	f.specs = append(f.specs, NodeSpec{
		Rel: rel, Role: TopoNodeRolePrimary, Domain: DomainPrimary,
		CPUs: machine.MustParse(cpus), Mems: mems,
		ParentRel: f.parentOf(rel),
	})
	f.targetByRel[rel] = machine.MustParse(cpus)
	f.dynamicByRel[rel] = machine.MustParse(cpus)
}

func (f *admissionTraceFixture) addReclaim(rel, cpus, mems string) {
	f.driver.add(rel, CgroupIdentity{Device: 1, Inode: f.allocInode()}, cpus, mems)
	f.specs = append(f.specs, NodeSpec{
		Rel: rel, Role: TopoNodeRoleReclaim, Domain: DomainReclaim,
		CPUs: machine.MustParse(cpus), Mems: mems,
		ParentRel: f.parentOf(rel), TrustAnchor: f.parentOf(rel) == "",
	})
	f.targetByRel[rel] = machine.MustParse(cpus)
}

func (f *admissionTraceFixture) requireCPUSet(rel, cpus string) {
	set := machine.MustParse(cpus)
	f.requiredByRel[rel] = set
	f.targetByRel[rel] = set
}

func (f *admissionTraceFixture) setCanonicalTarget(rel, cpus string) {
	f.canonicalByRel[rel] = machine.MustParse(cpus)
}

func (f *admissionTraceFixture) configureStagedSMTTransferWithDynamicDescendant() {
	f.selection.MaxCPUsDrainRatio = 0.5
	f.addPrimary("kubepods", "0-3", "0")
	f.addDynamicDescendant("kubepods/besteffort", "1-3", "0")
	f.addReclaim("reclaimed-0", "0", "0")
	f.requireCPUSet("kubepods", "0-3")
}

// snapshot wires the round to the current fixture topology and returns a fresh
// complete snapshot captured from the live fake driver.
func (f *admissionTraceFixture) snapshot() *CompleteSnapshot {
	f.t.Helper()
	dag, err := BuildDAG(f.specs)
	require.NoError(f.t, err)

	f.round.dag = dag
	f.round.driver = f.driver
	f.round.selection = NormalizeDrainSelectionPolicy(f.selection)
	f.round.targetByRel = cloneCPUSetMap(f.targetByRel)
	f.round.requiredByRel = cloneCPUSetMap(f.requiredByRel)
	f.round.dynamicByRel = cloneCPUSetMap(f.dynamicByRel)
	f.round.snapshotSource = newCompleteSnapshotSource(f.driver, dag, f.budget)

	base, err := f.round.snapshotSource(context.Background())
	require.NoError(f.t, err)
	return base
}

func (f *admissionTraceFixture) runFixedPointAgainstRecordingDriver(t *testing.T) []PlanOperation {
	t.Helper()
	base := f.snapshot()
	result, err := f.round.runFixedPointEngine(
		context.Background(),
		newLiveTraceSession(f.round, base),
	)
	require.NoError(t, err)
	return flattenCompiledPhases(result.Phases)
}

// flattenCompiledPhases collapses ordered phases into a single operation slice
// so compiled and live traces can be compared operation-by-operation.
func flattenCompiledPhases(phases []CompiledPhase) []PlanOperation {
	operations := make([]PlanOperation, 0, len(phases))
	for _, phase := range phases {
		operations = append(operations, phase.Operations...)
	}
	return operations
}

func flattenTraceOperations(trace *CompiledPhaseTrace) []PlanOperation {
	if trace == nil {
		return nil
	}
	return flattenCompiledPhases(trace.Phases)
}

func traceContainsOperation(
	trace *CompiledPhaseTrace,
	rel string,
	direction WriteDirection,
	target machine.CPUSet,
) bool {
	if trace == nil {
		return false
	}
	for _, op := range flattenTraceOperations(trace) {
		if op.Rel == rel && op.Direction == direction && op.Target.CPUs.Equals(target) {
			return true
		}
	}
	return false
}

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

func TestCompileFixedPointTraceRejectsNoProgress(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.addPrimary("kubepods", "0-3", "0")
	fixture.addDynamicDescendant("kubepods/besteffort", "1-3", "0")
	fixture.addReclaim("reclaimed-0", "0", "0")
	fixture.requireCPUSet("kubepods", "0-3")
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
	fixture.addPrimary("kubepods", "0-3", "0")
	fixture.addReclaim("reclaimed-0", "0", "0")
	fixture.requireCPUSet("reclaimed-0", "0-3")
	fixture.setCanonicalTarget("reclaimed-0", "0-1")

	_, err := fixture.round.compileFixedPointTrace(
		context.Background(),
		fixture.snapshot(),
	)
	require.Error(t, err)
	require.Zero(t, fixture.driver.PhysicalWriteCount())
}
