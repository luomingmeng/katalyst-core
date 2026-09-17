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
	"errors"
	"sort"
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

func (s *liveTraceSession) Apply(ctx context.Context, plan PhasePlan) (phaseSessionApplyResult, error) {
	result := phaseSessionApplyResult{}
	for _, op := range plan.Operations {
		if op.WriteMems {
			if err := s.round.driver.WriteMems(ctx, op.Rel, op.ExpectedIdentity, op.Target.Mems); err != nil {
				return result, err
			}
		}
		if !op.ExpectedCurrent.CPUs.Equals(op.Target.CPUs) {
			if err := s.round.driver.WriteCPUs(ctx, op.Rel, op.ExpectedIdentity, op.Target.CPUs); err != nil {
				return result, err
			}
		}
		result.Applied++
		result.Journal = append(result.Journal, AppliedPlanOperation{
			PlanID: op.PlanID, Rel: op.Rel, Direction: op.Direction,
			Target: op.Target, Observed: op.Target,
		})
	}
	return result, nil
}

func (s *liveTraceSession) Capabilities() HierarchyCapabilities {
	return s.round.driver.Capabilities()
}

// projectedTraceSession adapts a clone-backed projectedHierarchy to the phase
// execution session contract. Snapshot returns an isolated clone so the engine
// can freeze plan.Base before Apply mutates the projection in place.
type projectedTraceSession struct {
	hierarchy    *projectedHierarchy
	ignoreWrites bool
}

func newProjectedTraceSession(t *testing.T, base *CompleteSnapshot, capabilities HierarchyCapabilities) *projectedTraceSession {
	t.Helper()
	hierarchy, err := newProjectedHierarchy(base, capabilities)
	require.NoError(t, err)
	return &projectedTraceSession{hierarchy: hierarchy}
}

func (s *projectedTraceSession) Snapshot(_ context.Context) (*CompleteSnapshot, error) {
	return CloneCompleteSnapshot(s.hierarchy.snapshot), nil
}

func (s *projectedTraceSession) Apply(_ context.Context, plan PhasePlan) (phaseSessionApplyResult, error) {
	if s.ignoreWrites {
		return phaseSessionApplyResult{}, nil
	}
	result := phaseSessionApplyResult{}
	for _, operation := range plan.Operations {
		if err := s.hierarchy.applyOperation(operation); err != nil {
			return result, err
		}
		result.Applied++
		result.Journal = append(result.Journal, AppliedPlanOperation{
			PlanID: operation.PlanID, Rel: operation.Rel, Direction: operation.Direction,
			Target: operation.Target, Observed: operation.Target,
		})
	}
	return result, nil
}

func (s *projectedTraceSession) Capabilities() HierarchyCapabilities {
	return s.hierarchy.capabilities
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
	f.driver.capabilities = cgroupV2Policy.capabilities(true)
	f.round.allowEmptyTarget = true
	f.cpuDetails[4] = machine.CPUTopoInfo{NUMANodeID: 0}
	f.addPrimary("kubepods", "1-3", "0")
	f.addDynamicDescendant("kubepods/besteffort", "1-3", "0")
	f.addReclaim("reclaimed", "4", "0")
	f.addReclaim("reclaimed/leaf", "0", "0")
	f.specs[len(f.specs)-1].CPUs = machine.NewCPUSet()
	f.targetByRel["reclaimed/leaf"] = machine.NewCPUSet()
	f.requireCPUSet("kubepods", "0-3")
	f.requireCPUSet("kubepods/besteffort", "0-3")
}

func (f *admissionTraceFixture) configureMultiFrontierParentSafeDrain() {
	f.driver.capabilities = cgroupV2Policy.capabilities(true)
	f.addPrimary("kubepods", "0-3", "0")
	f.addDynamicDescendant("kubepods/besteffort", "0-3", "0")
	f.addReclaim("reclaimed", "4", "0")
	f.targetByRel["kubepods"] = machine.MustParse("0-2")
	f.targetByRel["kubepods/besteffort"] = machine.MustParse("0-2")
	f.dynamicByRel["kubepods/besteffort"] = machine.MustParse("0-2")
	f.requireCPUSet("reclaimed", "3-4")
}

func (f *admissionTraceFixture) addUnavailableDynamicChild(rel, cpus, mems string) {
	f.driver.add(rel, CgroupIdentity{Device: 1, Inode: f.allocInode()}, cpus, mems)
	f.driver.beforeCall = func(op HierarchyOperation, candidate string) error {
		if op == HierarchyOperationRead && candidate == rel {
			return ErrCgroupControllerUnavailable
		}
		return nil
	}
}

func (f *admissionTraceFixture) configureProtectedDeferredEvaluationInputs() {
	f.addDynamicDescendant("kubepods/deferred", "1-3", "0")
	f.targetByRel["kubepods/deferred"] = machine.MustParse("0-3")
	f.round.protectedPending = machine.NewCPUSet(1)
	f.round.deferredByRel = map[string]machine.CPUSet{
		"kubepods/deferred": machine.MustParse("1-3"),
	}
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
	fixture.configureStagedSMTTransferWithDynamicDescendant()

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
	require.NotEmpty(t, trace.TraceID)
	require.NotEmpty(t, trace.ConvergenceID)
	require.Equal(t, ConvergenceObjectiveParentSafe, trace.Objective)
	require.NotNil(t, trace.InitialSnapshot)
	require.NotNil(t, trace.FinalSnapshot)
	require.Equal(t, fixture.driver.Capabilities(), trace.Capabilities)
	require.Equal(t, machine.MustParse("0-3"), trace.RequiredCPUSetByRel["kubepods"])
	require.Equal(t, machine.MustParse("0-3"), trace.CanonicalTargetByRel["kubepods"].CPUs)
	require.Positive(t, trace.Cost.Forward.Total())
	require.Equal(t, trace.Cost.Forward, trace.Cost.Rollback)
	require.Zero(t, fixture.driver.PhysicalWriteCount())
}

// TestFixedPointEngineUsesSamePlanSequenceForProjectedAndRecordingSessions
// proves the engine is session-agnostic: given the same starting snapshot, a
// clone-backed projected session and a live recording session must produce a
// byte-for-byte identical ordered phase trace. The engine only decides and
// sequences operations; each session is responsible for faithfully applying
// them, so parity here is the frozen-vs-live equivalence the compiler relies on.
func TestFixedPointEngineUsesSamePlanSequenceForProjectedAndRecordingSessions(t *testing.T) {
	projectedFixture := newAdmissionTraceFixture(t)
	projectedFixture.configureStagedSMTTransferWithDynamicDescendant()
	projectedBase := projectedFixture.snapshot()
	projectedResult, err := projectedFixture.round.runFixedPointEngine(
		context.Background(),
		newProjectedTraceSession(t, projectedBase, projectedFixture.driver.Capabilities()),
	)
	require.NoError(t, err)

	recordingFixture := newAdmissionTraceFixture(t)
	recordingFixture.configureStagedSMTTransferWithDynamicDescendant()
	recordingBase := recordingFixture.snapshot()
	recordingResult, err := recordingFixture.round.runFixedPointEngine(
		context.Background(),
		newLiveTraceSession(recordingFixture.round, recordingBase),
	)
	require.NoError(t, err)

	require.Equal(t, projectedResult.Phases, recordingResult.Phases)
}

func TestFixedPointEngineSingleRoundReturnsNeutralOutcome(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()
	base := fixture.snapshot()
	session := newProjectedTraceSession(t, base, fixture.driver.Capabilities())

	result, err := fixture.round.runFixedPointEngine(
		context.Background(),
		session,
		fixedPointEngineSingleRound,
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.Rounds)
	require.NotNil(t, result.FinalSnapshot)
	require.NotEmpty(t, result.Phases)
	require.Equal(t, result.FinalSnapshot, result.Outcome.Snapshot)
	require.Equal(t, RoundStatusConverged, result.Outcome.Status)
	require.True(t, result.ObjectiveSatisfied)
}

func TestFixedPointEngineSessionReceivesCompletePhasePlan(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()
	base := fixture.snapshot()
	projected := newProjectedTraceSession(t, base, fixture.driver.Capabilities())
	session := &planRecordingSession{phaseExecutionSession: projected}

	_, err := fixture.round.runFixedPointEngine(
		context.Background(),
		session,
		fixedPointEngineSingleRound,
	)
	require.NoError(t, err)
	require.NotEmpty(t, session.plans)
	for _, plan := range session.plans {
		require.NotEmpty(t, plan.PlanID)
		require.NotNil(t, plan.Base)
		require.NotEmpty(t, plan.CanonicalTargetByRel)
		require.NotEmpty(t, plan.Operations)
	}
}

type planRecordingSession struct {
	phaseExecutionSession
	plans []PhasePlan
}

func (s *planRecordingSession) Apply(ctx context.Context, plan PhasePlan) (phaseSessionApplyResult, error) {
	s.plans = append(s.plans, plan)
	return s.phaseExecutionSession.Apply(ctx, plan)
}

type firstSnapshotErrorSession struct {
	phaseExecutionSession
	err error
}

func (s *firstSnapshotErrorSession) Snapshot(context.Context) (*CompleteSnapshot, error) {
	return nil, s.err
}

func TestFixedPointEngineFirstSnapshotFailureReturnsStaleOutcome(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()
	base := fixture.snapshot()
	fixture.budget = NewBudgetTracker(ConvergenceBudget{MaxRounds: 2})
	fixture.round.budget = fixture.budget
	fixture.round.round = 0
	snapshotErr := errors.New("initial snapshot preflight failed")
	session := &firstSnapshotErrorSession{
		phaseExecutionSession: newProjectedTraceSession(t, base, fixture.driver.Capabilities()),
		err:                   snapshotErr,
	}

	result, err := fixture.round.runFixedPointEngine(
		context.Background(),
		session,
		fixedPointEngineSingleRound,
	)

	require.ErrorIs(t, err, snapshotErr)
	require.NotNil(t, result)
	require.Equal(t, RoundStatusStale, result.Outcome.Status)
	require.ErrorIs(t, result.Outcome.Blocker, snapshotErr)
	require.Equal(t, fixture.budget.Usage(), result.Outcome.Cost)
	require.Equal(t, 1, result.Outcome.Cost.Rounds)
	require.Equal(t, 1, result.Rounds)
}

func TestCompiledTraceMatchesFixedPointEngineTrace(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()

	compiled, err := fixture.round.compileFixedPointTrace(
		context.Background(),
		fixture.snapshot(),
	)
	require.NoError(t, err)

	recordingFixture := newAdmissionTraceFixture(t)
	recordingFixture.configureStagedSMTTransferWithDynamicDescendant()
	observed := recordingFixture.runFixedPointAgainstRecordingDriver(t)
	require.Equal(t, flattenTraceOperations(compiled), observed)
}

func TestParentSafeCompileSkipsFrozenUnavailableChild(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureMultiFrontierParentSafeDrain()
	fixture.addUnavailableDynamicChild("kubepods/besteffort/container", "0-2", "0")
	base := fixture.snapshot()
	childIdentity := fixture.driver.nodes["kubepods/besteffort/container"].identity
	wantEvidence := UnavailableChildEvidence{
		Identity: childIdentity,
		Reason:   UnavailableChildReasonControllerUnavailable,
	}
	require.Equal(t, wantEvidence, base.UnavailableChildren["kubepods/besteffort/container"])

	trace, err := fixture.round.compileFixedPointTrace(context.Background(), base)
	require.NoError(t, err)
	require.True(t, trace.FinalEvaluation.ParentSafety.Safe)
	require.Equal(t, wantEvidence, trace.InitialSnapshot.UnavailableChildren["kubepods/besteffort/container"])
}

func TestFreezePhaseTraceRejectsNoProgress(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()
	trace, err := fixture.round.compileFixedPointTrace(context.Background(), fixture.snapshot())
	require.NoError(t, err)
	trace.Phases = nil
	trace.FinalSnapshot = CloneCompleteSnapshot(trace.InitialSnapshot)
	trace.FinalEvaluation.ParentSafety.Safe = false

	_, err = FreezePhaseTrace(trace)
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

func TestFreezePhaseTraceIsolatedFromMutableInputs(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()
	fixture.configureProtectedDeferredEvaluationInputs()
	trace, err := fixture.round.compileFixedPointTrace(context.Background(), fixture.snapshot())
	require.NoError(t, err)

	frozen, err := FreezePhaseTrace(trace)
	require.NoError(t, err)
	originalID := frozen.TraceID
	originalInitial := CloneCompleteSnapshot(frozen.InitialSnapshot)
	originalFinal := CloneCompleteSnapshot(frozen.FinalSnapshot)
	originalOperation := frozen.Phases[0].Operations[0]
	originalCanonical := frozen.CanonicalTargetByRel["kubepods"]
	originalRequired := frozen.RequiredCPUSetByRel["kubepods"]
	originalEvaluationInput := cloneFrozenCoordinatorEvaluationInput(frozen.EvaluationInput)

	trace.InitialSnapshot.Entries["kubepods"] = EntryState{}
	trace.FinalSnapshot.Entries["kubepods"] = EntryState{}
	trace.CanonicalTargetByRel["kubepods"] = CPUSetTarget{CPUs: machine.NewCPUSet(99)}
	trace.RequiredCPUSetByRel["kubepods"] = machine.NewCPUSet(99)
	trace.Phases[0].Operations[0] = PlanOperation{}
	trace.FinalEvaluation.ParentSafety.RequiredFloorDeficit["kubepods"] = machine.NewCPUSet(99)
	trace.EvaluationInput.ProtectedPending = machine.NewCPUSet(99)
	trace.EvaluationInput.DeferredByRel["kubepods/deferred"] = machine.NewCPUSet(99)
	for rel := range trace.EvaluationInput.DeferredCleanupRels {
		delete(trace.EvaluationInput.DeferredCleanupRels, rel)
	}
	trace.EvaluationInput.DAGSpecs[0].CPUs = machine.NewCPUSet(99)

	require.Equal(t, originalID, frozen.TraceID)
	require.Equal(t, originalInitial, frozen.InitialSnapshot)
	require.Equal(t, originalFinal, frozen.FinalSnapshot)
	require.Equal(t, originalOperation, frozen.Phases[0].Operations[0])
	require.Equal(t, originalCanonical, frozen.CanonicalTargetByRel["kubepods"])
	require.Equal(t, originalRequired, frozen.RequiredCPUSetByRel["kubepods"])
	require.Equal(t, originalEvaluationInput, frozen.EvaluationInput)
	require.Empty(t, frozen.FinalEvaluation.ParentSafety.RequiredFloorDeficit)
}

func TestFreezePhaseTraceIDIsIndependentOfMapInsertionOrder(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()
	trace, err := fixture.round.compileFixedPointTrace(context.Background(), fixture.snapshot())
	require.NoError(t, err)

	reordered := *trace
	reordered.TraceID = ""
	reordered.CanonicalTargetByRel = make(map[string]CPUSetTarget, len(trace.CanonicalTargetByRel))
	reordered.RequiredCPUSetByRel = make(map[string]machine.CPUSet, len(trace.RequiredCPUSetByRel))
	rels := make([]string, 0, len(trace.CanonicalTargetByRel))
	for rel := range trace.CanonicalTargetByRel {
		rels = append(rels, rel)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(rels)))
	for _, rel := range rels {
		if target, ok := trace.CanonicalTargetByRel[rel]; ok {
			reordered.CanonicalTargetByRel[rel] = target
		}
		if required, ok := trace.RequiredCPUSetByRel[rel]; ok {
			reordered.RequiredCPUSetByRel[rel] = required
		}
	}

	frozen, err := FreezePhaseTrace(&reordered)
	require.NoError(t, err)
	require.Equal(t, trace.TraceID, frozen.TraceID)
	require.Equal(t, trace.Phases, frozen.Phases)
}

func TestFreezePhaseTraceRejectsTamperedFinalEvaluation(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()
	trace, err := fixture.round.compileFixedPointTrace(context.Background(), fixture.snapshot())
	require.NoError(t, err)

	trace.FinalEvaluation.Report.PendingToPrimary = machine.NewCPUSet(99)

	_, err = FreezePhaseTrace(trace)
	require.ErrorContains(t, err, "final evaluation")
}

func TestFreezePhaseTraceProductionEvaluationParityWithProtectedDeferredInputs(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()
	fixture.configureProtectedDeferredEvaluationInputs()

	trace, err := fixture.round.compileFixedPointTrace(context.Background(), fixture.snapshot())
	require.NoError(t, err)
	require.False(t, trace.EvaluationInput.ProtectedPending.IsEmpty())
	require.NotEmpty(t, trace.EvaluationInput.DeferredByRel)
	require.NotEmpty(t, trace.EvaluationInput.DeferredCleanupRels)

	recomputed, err := trace.EvaluationInput.evaluate(trace.FinalSnapshot)
	require.NoError(t, err)
	require.Equal(
		t,
		normalizeCoordinatorSnapshotEvaluation(trace.FinalEvaluation),
		normalizeCoordinatorSnapshotEvaluation(recomputed),
	)
	require.Zero(t, fixture.driver.PhysicalWriteCount())
}

func TestFreezePhaseTraceRejectsProtectedDeferredInputTampering(t *testing.T) {
	newTrace := func(t *testing.T) *CompiledPhaseTrace {
		t.Helper()
		fixture := newAdmissionTraceFixture(t)
		fixture.configureStagedSMTTransferWithDynamicDescendant()
		fixture.configureProtectedDeferredEvaluationInputs()
		trace, err := fixture.round.compileFixedPointTrace(context.Background(), fixture.snapshot())
		require.NoError(t, err)
		require.NotEmpty(t, trace.EvaluationInput.DeferredCleanupRels)
		return trace
	}

	tests := []struct {
		name       string
		mutate     func(*CompiledPhaseTrace)
		wantSubstr string
	}{
		{
			name: "protected pending",
			mutate: func(trace *CompiledPhaseTrace) {
				trace.EvaluationInput.ProtectedPending = machine.NewCPUSet(4)
			},
			wantSubstr: "final evaluation",
		},
		{
			name: "deferred leaf",
			mutate: func(trace *CompiledPhaseTrace) {
				trace.EvaluationInput.DeferredByRel["kubepods/deferred"] = machine.NewCPUSet(4)
			},
			wantSubstr: "final evaluation",
		},
		{
			name: "deferred cleanup rel",
			mutate: func(trace *CompiledPhaseTrace) {
				for rel := range trace.EvaluationInput.DeferredCleanupRels {
					delete(trace.EvaluationInput.DeferredCleanupRels, rel)
				}
			},
			wantSubstr: "identity",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trace := newTrace(t)
			tc.mutate(trace)

			_, err := FreezePhaseTrace(trace)
			require.ErrorContains(t, err, tc.wantSubstr)
		})
	}
}

func TestCanonicalPhaseTraceIDBindsFinalEvaluation(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	fixture.configureStagedSMTTransferWithDynamicDescendant()
	trace, err := fixture.round.compileFixedPointTrace(context.Background(), fixture.snapshot())
	require.NoError(t, err)

	tampered := *trace
	tampered.FinalEvaluation = cloneCoordinatorSnapshotEvaluation(trace.FinalEvaluation)
	tampered.FinalEvaluation.Report.PendingToPrimary = machine.NewCPUSet(99)

	require.NotEqual(t, canonicalPhaseTraceID(trace), canonicalPhaseTraceID(&tampered))
}

func TestCloneForProjectionDeepCopiesCPUDetails(t *testing.T) {
	fixture := newAdmissionTraceFixture(t)
	clone := fixture.round.cloneForProjection()

	clone.cpuDetails[0] = machine.CPUTopoInfo{NUMANodeID: 99}
	delete(clone.cpuDetails, 1)

	require.Equal(t, 0, fixture.round.cpuDetails[0].NUMANodeID)
	require.Contains(t, fixture.round.cpuDetails, 1)
}

func TestFreezePhaseTraceRejectsOrphanOrInconsistentSnapshotEvidence(t *testing.T) {
	newTrace := func(t *testing.T) *CompiledPhaseTrace {
		t.Helper()
		fixture := newAdmissionTraceFixture(t)
		fixture.configureStagedSMTTransferWithDynamicDescendant()
		trace, err := fixture.round.compileFixedPointTrace(context.Background(), fixture.snapshot())
		require.NoError(t, err)
		return trace
	}

	tests := []struct {
		name   string
		mutate func(*CompleteSnapshot)
	}{
		{
			name: "orphan children key",
			mutate: func(snapshot *CompleteSnapshot) {
				snapshot.Children["orphan"] = []ChildRef{{
					Name: "leaf", Identity: CgroupIdentity{Device: 1, Inode: 999},
				}}
			},
		},
		{
			name: "orphan domain key",
			mutate: func(snapshot *CompleteSnapshot) {
				snapshot.DomainByRel["orphan"] = DomainPrimary
			},
		},
		{
			name: "entry missing domain",
			mutate: func(snapshot *CompleteSnapshot) {
				delete(snapshot.DomainByRel, "kubepods")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trace := newTrace(t)
			tc.mutate(trace.InitialSnapshot)
			trace.InitialSnapshot.ID = fingerprintSnapshot(trace.InitialSnapshot)

			_, err := FreezePhaseTrace(trace)
			require.ErrorContains(t, err, "snapshot evidence")
		})
	}
}

func TestFreezePhaseTraceRejectsUnsafeFinalSnapshot(t *testing.T) {
	newTrace := func(t *testing.T) *CompiledPhaseTrace {
		t.Helper()
		fixture := newAdmissionTraceFixture(t)
		fixture.configureStagedSMTTransferWithDynamicDescendant()
		trace, err := fixture.round.compileFixedPointTrace(context.Background(), fixture.snapshot())
		require.NoError(t, err)
		return trace
	}

	tests := []struct {
		name       string
		mutate     func(*CompiledPhaseTrace)
		wantSubstr string
	}{
		{
			name: "nil initial snapshot",
			mutate: func(trace *CompiledPhaseTrace) {
				trace.InitialSnapshot = nil
			},
			wantSubstr: "snapshot",
		},
		{
			name: "missing operation identity",
			mutate: func(trace *CompiledPhaseTrace) {
				trace.Phases[0].Operations[0].ExpectedIdentity = CgroupIdentity{}
			},
			wantSubstr: "identity",
		},
		{
			name: "expected current mismatch",
			mutate: func(trace *CompiledPhaseTrace) {
				trace.Phases[0].Operations[0].ExpectedCurrent.CPUs = machine.NewCPUSet(99)
			},
			wantSubstr: "expected current",
		},
		{
			name: "objective not satisfied",
			mutate: func(trace *CompiledPhaseTrace) {
				trace.FinalEvaluation.ParentSafety.Safe = false
			},
			wantSubstr: "objective",
		},
		{
			name: "required CPUs absent",
			mutate: func(trace *CompiledPhaseTrace) {
				trace.RequiredCPUSetByRel["kubepods"] = machine.NewCPUSet(99)
			},
			wantSubstr: "required CPUs",
		},
		{
			name: "zero progress before objective",
			mutate: func(trace *CompiledPhaseTrace) {
				trace.Phases = nil
				trace.FinalSnapshot = CloneCompleteSnapshot(trace.InitialSnapshot)
				trace.FinalEvaluation.ParentSafety.Safe = false
			},
			wantSubstr: "no progress",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trace := newTrace(t)
			tc.mutate(trace)
			_, err := FreezePhaseTrace(trace)
			require.ErrorContains(t, err, tc.wantSubstr)
		})
	}
}
