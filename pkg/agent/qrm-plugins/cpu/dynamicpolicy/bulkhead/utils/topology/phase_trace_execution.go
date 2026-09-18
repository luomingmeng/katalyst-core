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
	"sort"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const frozenTraceRecoveryTimeout = time.Second

type AppliedPhysicalWrite struct {
	PlanID                string
	Rel                   string
	Identity              CgroupIdentity
	Direction             WriteDirection
	Resource              HierarchyOperation
	LogicalOperationIndex int
	Phase                 PhaseKind
	Before                string
	// BeforeEffective records the effective value paired with the configured
	// Before value by the same identity-pinned read immediately before write.
	BeforeEffective string
	After           string
	AfterEffective  string
	Impact          PhysicalImpact
}

type traceMutationStack struct {
	writes               []AppliedPhysicalWrite
	rollbackObservations map[string]rollbackObservation
}

type rollbackObservation struct {
	current EntryState
	err     error
}

type frozenTraceFinalization struct {
	snapshot   *CompleteSnapshot
	evaluation coordinatorSnapshotEvaluation
}

type frozenTraceFinalizer func(
	context.Context,
	*CompiledPhaseTrace,
) (frozenTraceFinalization, error)

type frozenOperationState struct {
	Identity       CgroupIdentity
	ConfiguredCPUs machine.CPUSet
	EffectiveCPUs  machine.CPUSet
	ConfiguredMems string
	EffectiveMems  string
}

type frozenOperationPreflight struct {
	before         frozenOperationState
	after          frozenOperationState
	parentIdentity CgroupIdentity
	children       stableLiveChildren
}

// frozenInitialSnapshotDriftError carries the fresh snapshot that invalidated a
// frozen trace before any live hierarchy write. ParentSafe admission may safely
// recompile from this snapshot while the same invocation budget and deadline
// remain in force.
type frozenInitialSnapshotDriftError struct {
	current              *CompleteSnapshot
	expected             *CompleteSnapshot
	currentEvidenceID    SnapshotID
	physicalWritesBefore int
	physicalWritesAfter  int
	cause                error
	stale                *PlanStaleError
}

func (e *frozenInitialSnapshotDriftError) Error() string {
	if e == nil {
		return "frozen trace initial snapshot drift"
	}
	var currentID, expectedID SnapshotID
	if e.current != nil {
		currentID = e.current.ID
	} else {
		currentID = e.currentEvidenceID
	}
	if e.expected != nil {
		expectedID = e.expected.ID
	}
	message := fmt.Sprintf(
		"frozen trace initial snapshot drift: current=%x expected=%x physical_writes_before=%d physical_writes_after=%d: %v",
		currentID, expectedID, e.physicalWritesBefore, e.physicalWritesAfter, e.cause,
	)
	if e.cause == nil {
		message = fmt.Sprintf(
			"frozen trace initial snapshot drift: current=%x expected=%x physical_writes_before=%d physical_writes_after=%d",
			currentID, expectedID, e.physicalWritesBefore, e.physicalWritesAfter,
		)
	}
	return message
}

func (e *frozenInitialSnapshotDriftError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.stale
}

func (e *frozenInitialSnapshotDriftError) ReplanRequired() bool { return true }

func (e *frozenInitialSnapshotDriftError) FrozenInitialSnapshotDrift() bool { return true }

func newFrozenInitialSnapshotDriftError(
	current *CompleteSnapshot,
	expected *CompleteSnapshot,
	currentEvidenceID SnapshotID,
	cause error,
	physicalWritesBefore, physicalWritesAfter int,
) error {
	currentState := fmt.Sprintf("%x", currentEvidenceID)
	if current != nil {
		currentState = snapshotLogicalState(current)
	}
	stale := &PlanStaleError{
		Rel:       "controlled",
		Direction: WritePublish,
		Resource:  "initial_snapshot",
		Current:   currentState,
		Target:    snapshotLogicalState(expected),
		Err:       cause,
	}
	if physicalWritesBefore != 0 || physicalWritesAfter != 0 {
		return fmt.Errorf(
			"frozen trace initial snapshot drift is not replan-safe: current=%x expected=%x physical_writes_before=%d physical_writes_after=%d: %w",
			currentEvidenceID, expected.ID, physicalWritesBefore, physicalWritesAfter, stale,
		)
	}
	return &frozenInitialSnapshotDriftError{
		current:              current,
		expected:             expected,
		currentEvidenceID:    currentEvidenceID,
		physicalWritesBefore: physicalWritesBefore,
		physicalWritesAfter:  physicalWritesAfter,
		cause:                cause,
		stale:                stale,
	}
}

func wrapFrozenInitialPreflightError(
	err error,
	expected *CompleteSnapshot,
	physicalWritesBefore, physicalWritesAfter int,
) error {
	if physicalWritesBefore != 0 || physicalWritesAfter != 0 {
		return err
	}
	var snapshotErr *SnapshotError
	if !errors.As(err, &snapshotErr) {
		return err
	}
	if snapshotErr.Class != HierarchyErrorStale &&
		!errors.Is(err, ErrSnapshotBoundaryExpansionMismatch) {
		return err
	}
	return newFrozenInitialSnapshotDriftError(
		nil, expected, snapshotErr.EvidenceID, err,
		physicalWritesBefore, physicalWritesAfter)
}

// frozenFinalSnapshotDriftError records external hierarchy drift discovered only
// after the frozen write sequence. It is not safe to replan until failFrozenTrace
// has completed and verified rollback of the full physical-write prefix.
type frozenFinalSnapshotDriftError struct {
	current  *CompleteSnapshot
	expected *CompleteSnapshot
	stale    *PlanStaleError
}

func (e *frozenFinalSnapshotDriftError) Error() string {
	if e == nil || e.current == nil || e.expected == nil {
		return "frozen trace final snapshot drift"
	}
	return fmt.Sprintf("frozen trace final snapshot drift: current=%x expected=%x",
		e.current.ID, e.expected.ID)
}

func (e *frozenFinalSnapshotDriftError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.stale
}

func (e *frozenFinalSnapshotDriftError) ReplanRequired() bool { return true }

func (e *frozenFinalSnapshotDriftError) FrozenFinalSnapshotDrift() bool { return true }

// frozenSnapshotDriftAfterVerifiedRollbackError is issued only after every
// physical write made by the failed frozen trace has been rolled back and read
// back at its original configured and effective value. This marker is the sole
// authorization for the admission boundary to compile and execute a fresh plan.
type frozenSnapshotDriftAfterVerifiedRollbackError struct {
	err error
}

func (e *frozenSnapshotDriftAfterVerifiedRollbackError) Error() string {
	if e == nil || e.err == nil {
		return "frozen snapshot drift after verified rollback"
	}
	return e.err.Error()
}

func (e *frozenSnapshotDriftAfterVerifiedRollbackError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (*frozenSnapshotDriftAfterVerifiedRollbackError) FrozenSnapshotDriftReplanSafe() bool {
	return true
}

// preflightFrozenTrace proves that a frozen trace is executable from one fresh
// complete snapshot. Every operation is validated and applied to an isolated
// projected hierarchy in global trace order; no live hierarchy write occurs.
func (w safeCPSetWriter) preflightFrozenTrace(
	ctx context.Context,
	trace *CompiledPhaseTrace,
) error {
	_, err := w.preflightFrozenTraceOperations(ctx, trace)
	return err
}

func (w safeCPSetWriter) preflightFrozenTraceOperations(
	ctx context.Context,
	trace *CompiledPhaseTrace,
) ([]frozenOperationPreflight, error) {
	if w.driver == nil {
		return nil, fmt.Errorf("frozen trace preflight requires hierarchy driver")
	}
	if w.budget == nil {
		return nil, fmt.Errorf("frozen trace preflight requires convergence budget")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	frozen, err := FreezePhaseTrace(trace)
	if err != nil {
		return nil, fmt.Errorf("freeze phase trace before preflight: %w", err)
	}
	if frozen.InitialSnapshot.ScanBoundary.Purpose != ScanForPlan {
		return nil, fmt.Errorf(
			"frozen trace preflight requires plan snapshot evidence, got %q",
			frozen.InitialSnapshot.ScanBoundary.Purpose,
		)
	}
	dag, err := BuildDAG(cloneNodeSpecs(frozen.EvaluationInput.DAGSpecs))
	if err != nil {
		return nil, fmt.Errorf("rebuild frozen trace DAG for preflight: %w", err)
	}
	physicalWritesBefore := w.physicalWriteCount()
	fresh, err := BuildCompleteSnapshotForBoundary(
		ctx,
		w.driver,
		dag,
		cloneScanBoundary(frozen.InitialSnapshot.ScanBoundary),
		w.budget,
	)
	physicalWritesAfter := w.physicalWriteCount()
	if err != nil {
		err = fmt.Errorf("capture frozen trace preflight snapshot: %w", err)
		return nil, wrapFrozenInitialPreflightError(
			err, frozen.InitialSnapshot, physicalWritesBefore, physicalWritesAfter)
	}
	if fresh.ID != frozen.InitialSnapshot.ID {
		return nil, newFrozenInitialSnapshotDriftError(
			fresh,
			frozen.InitialSnapshot,
			fresh.ID,
			fmt.Errorf("fresh preflight snapshot differs from frozen trace base"),
			physicalWritesBefore,
			physicalWritesAfter,
		)
	}

	projection, err := newProjectedHierarchy(fresh, frozen.Capabilities)
	if err != nil {
		return nil, fmt.Errorf("create frozen trace preflight projection: %w", err)
	}
	operationCount := 0
	for _, phase := range frozen.Phases {
		operationCount = saturatingAdd(operationCount, len(phase.Operations))
	}
	evidence := make([]frozenOperationPreflight, 0, operationCount)
	for phaseIndex, phase := range frozen.Phases {
		for operationIndex, operation := range phase.Operations {
			before, ok := projection.snapshot.Entries[operation.Rel]
			if !ok {
				return nil, fmt.Errorf(
					"preflight frozen phase trace operation %d/%d has no predecessor for %q",
					phaseIndex, operationIndex, operation.Rel,
				)
			}
			children, err := frozenChildrenFromSnapshot(projection.snapshot, operation.Rel)
			if err != nil {
				return nil, fmt.Errorf(
					"preflight frozen phase trace operation %d/%d children: %w",
					phaseIndex, operationIndex, err,
				)
			}
			var parentIdentity CgroupIdentity
			if operation.ParentRel != "" {
				parent, parentOK := projection.snapshot.Entries[operation.ParentRel]
				if !parentOK {
					return nil, fmt.Errorf(
						"preflight frozen phase trace operation %d/%d has no parent predecessor for %q",
						phaseIndex, operationIndex, operation.ParentRel,
					)
				}
				parentIdentity = parent.Identity
			}
			if err := projection.applyOperation(operation); err != nil {
				return nil, fmt.Errorf(
					"preflight frozen phase trace operation %d/%d: %w",
					phaseIndex, operationIndex, err,
				)
			}
			after := projection.snapshot.Entries[operation.Rel]
			evidence = append(evidence, frozenOperationPreflight{
				before:         freezeOperationState(before),
				after:          freezeOperationState(after),
				parentIdentity: parentIdentity,
				children:       children,
			})
		}
	}
	if projection.snapshot.ID != frozen.FinalSnapshot.ID {
		return nil, fmt.Errorf(
			"frozen trace projected final snapshot drift: projected=%x expected=%x",
			projection.snapshot.ID, frozen.FinalSnapshot.ID,
		)
	}
	return evidence, nil
}

func (w safeCPSetWriter) physicalWriteCount() int {
	if w.physicalWriteAttempts == nil {
		return 0
	}
	return *w.physicalWriteAttempts
}

func (w safeCPSetWriter) recordPhysicalWriteAttempt() {
	if w.physicalWriteAttempts != nil {
		*w.physicalWriteAttempts++
	}
}

func frozenChildrenFromSnapshot(
	snapshot *CompleteSnapshot,
	rel string,
) (stableLiveChildren, error) {
	children := stableLiveChildren{
		cpus:  machine.NewCPUSet(),
		mems:  machine.NewCPUSet(),
		refs:  append([]ChildRef(nil), snapshot.Children[rel]...),
		byRel: make(map[string]EntryState, len(snapshot.Children[rel])),
	}
	for _, child := range children.refs {
		childRel := child.Name
		if rel != "" {
			childRel = rel + "/" + child.Name
		}
		entry, ok := snapshot.Entries[childRel]
		if !ok {
			if _, unavailable := snapshot.UnavailableChildren[childRel]; unavailable {
				continue
			}
			return stableLiveChildren{}, fmt.Errorf(
				"child %q has neither entry nor unavailable evidence", childRel)
		}
		if entry.Identity != child.Identity {
			return stableLiveChildren{}, fmt.Errorf(
				"child %q identity disagrees with frozen listing", childRel)
		}
		children.byRel[childRel] = entry
		children.cpus = children.cpus.Union(entry.CPUs)
		if entry.Mems != "" {
			mems, err := machine.Parse(entry.Mems)
			if err != nil {
				return stableLiveChildren{}, fmt.Errorf(
					"parse frozen child %q cpuset.mems=%q: %w",
					childRel, entry.Mems, err)
			}
			children.mems = children.mems.Union(mems)
		}
	}
	return children, nil
}

func freezeOperationState(entry EntryState) frozenOperationState {
	return frozenOperationState{
		Identity:       entry.Identity,
		ConfiguredCPUs: entry.ConfiguredCPUs.Clone(),
		EffectiveCPUs:  entry.CPUs.Clone(),
		ConfiguredMems: entry.ConfiguredMems,
		EffectiveMems:  entry.Mems,
	}
}

// executeFrozenTrace applies exactly the globally ordered operations authorized
// by ticket. It never replans. Any execution or final-proof failure rolls back
// the complete physical-write prefix accumulated by this invocation.
func (r *coordinatorRound) executeFrozenTrace(
	ctx context.Context,
	trace *CompiledPhaseTrace,
	ticket *ExecutionReservationTicket,
	res *ConvergenceResult,
	finalizers ...frozenTraceFinalizer,
) (RoundOutcome, error) {
	outcome := RoundOutcome{Status: RoundStatusBlocked}
	if r == nil || r.driver == nil {
		return outcome, fmt.Errorf("frozen trace execution requires hierarchy driver")
	}
	if r.budget == nil {
		return outcome, fmt.Errorf("frozen trace execution requires convergence budget")
	}
	if ticket == nil {
		return outcome, fmt.Errorf("%w: frozen trace execution requires reservation ticket",
			ErrAdmissionReservationExceeded)
	}
	if res == nil {
		return outcome, fmt.Errorf("frozen trace execution requires convergence result")
	}
	if len(finalizers) > 1 {
		return outcome, fmt.Errorf("frozen trace execution accepts at most one finalizer")
	}
	frozen, err := FreezePhaseTrace(trace)
	if err != nil {
		return outcome, err
	}
	defer ticket.ReleaseUnused()

	writer := newSafeCPUSetWriter(r.driver, r.budget, res)
	preflight, err := writer.preflightFrozenTraceOperations(ctx, frozen)
	if err != nil {
		return outcome, err
	}
	writer.driver = NewBudgetedHierarchyDriver(r.driver, r.budget)

	journalStart := len(res.Journal)
	appliedStart := res.Applied
	stack := &traceMutationStack{}
	operationIndex := 0
	for _, phase := range frozen.Phases {
		for _, operation := range phase.Operations {
			res.Attempted++
			applied, applyErr := writer.applyFrozenOperation(
				ctx, phase.Kind, operationIndex, operation,
				preflight[operationIndex], stack, ticket, frozen.TraceID)
			operationIndex++
			if applied.PlanID != "" {
				res.Journal = append(res.Journal, applied)
			}
			if applyErr != nil {
				res.Failed++
				return outcome, writer.failFrozenTrace(
					ctx, applyErr, stack, ticket, res, journalStart, appliedStart)
			}
			res.Applied++
		}
	}

	finalize := r.proveFrozenTraceFinalState
	if len(finalizers) == 1 {
		if finalizers[0] == nil {
			return outcome, writer.failFrozenTrace(
				ctx, fmt.Errorf("frozen trace execution requires non-nil finalizer"),
				stack, ticket, res, journalStart, appliedStart)
		}
		finalize = finalizers[0]
	}
	finalization, err := finalize(ctx, frozen)
	if err != nil {
		res.Failed++
		return outcome, writer.failFrozenTrace(
			ctx, err, stack, ticket, res, journalStart, appliedStart)
	}

	res.FinalSnapshot = finalization.snapshot
	res.FinalSnapshotCurrent = true
	res.ConvergenceReport = finalization.evaluation.Report
	res.ParentSafe = frozen.Objective == ConvergenceObjectiveParentSafe &&
		finalization.evaluation.ParentSafety.Safe &&
		!finalization.evaluation.Report.FullyConverged
	res.Converged = finalization.evaluation.Report.FullyConverged
	if res.Converged {
		res.State = ConvergenceStateConverged
		outcome.Status = RoundStatusConverged
	} else {
		res.State = ConvergenceStateParentSafeLeafDeferred
		outcome.Status = RoundStatusProgress
	}
	outcome.Snapshot = finalization.snapshot
	outcome.Journal = append(
		outcome.Journal, res.Journal[journalStart:]...)
	return outcome, nil
}

func (r *coordinatorRound) proveFrozenTraceFinalState(
	ctx context.Context,
	frozen *CompiledPhaseTrace,
) (frozenTraceFinalization, error) {
	var finalization frozenTraceFinalization
	fresh, err := BuildCompleteSnapshotForBoundary(
		ctx,
		r.driver,
		r.dag,
		cloneScanBoundary(frozen.FinalSnapshot.ScanBoundary),
		r.budget,
	)
	if err == nil && fresh.ID != frozen.FinalSnapshot.ID {
		err = &frozenFinalSnapshotDriftError{
			current:  fresh,
			expected: frozen.FinalSnapshot,
			stale: &PlanStaleError{
				Rel:       "controlled",
				Direction: WritePublish,
				Resource:  "final_snapshot",
				Current:   snapshotLogicalState(fresh),
				Target:    snapshotLogicalState(frozen.FinalSnapshot),
				Err:       fmt.Errorf("fresh final snapshot differs from frozen trace target"),
			},
		}
	}
	var freshEvaluation coordinatorSnapshotEvaluation
	if err == nil {
		freshEvaluation, err = frozen.EvaluationInput.evaluate(fresh)
	}
	if err == nil {
		switch frozen.Objective {
		case ConvergenceObjectiveParentSafe:
			if !freshEvaluation.ParentSafety.Safe {
				err = fmt.Errorf("fresh frozen trace final state is not ParentSafe")
			}
		case ConvergenceObjectiveFull:
			if !freshEvaluation.Report.FullyConverged {
				err = fmt.Errorf("fresh frozen trace final state is not fully converged")
			}
		default:
			err = fmt.Errorf("frozen trace final objective is unsupported: %q", frozen.Objective)
		}
	}
	if err != nil {
		return finalization, fmt.Errorf("prove frozen trace final state: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return finalization, err
	}
	finalization.snapshot = fresh
	finalization.evaluation = freshEvaluation
	return finalization, nil
}

func (w safeCPSetWriter) applyFrozenOperation(
	ctx context.Context,
	phase PhaseKind,
	logicalOperationIndex int,
	operation PlanOperation,
	preflight frozenOperationPreflight,
	stack *traceMutationStack,
	ticket *ExecutionReservationTicket,
	traceID string,
) (AppliedPlanOperation, error) {
	if err := ctx.Err(); err != nil {
		return AppliedPlanOperation{}, err
	}
	current, alreadyAtTarget, err := w.validateFrozenOperationPredecessor(
		ctx, operation, preflight)
	if err != nil {
		return AppliedPlanOperation{}, err
	}
	if err := ticket.AuthorizeNext(traceID, logicalOperationIndex, operation); err != nil {
		return AppliedPlanOperation{}, err
	}
	if alreadyAtTarget {
		return w.readAfterWrite(ctx, operation)
	}

	if operation.WriteMems && operation.ExpectedCurrent.Mems != operation.Target.Mems {
		write := physicalWriteBeforeFromEntry(
			current, operation, HierarchyOperationWriteMems, operation.Target.Mems,
			logicalOperationIndex, phase, preflight.after.EffectiveMems)
		if err := ticket.consumeForward(PhysicalWriteCost{MemsWrites: 1}); err != nil {
			return AppliedPlanOperation{}, err
		}
		w.recordPhysicalWriteAttempt()
		if err := w.driver.WriteMems(
			ctx, operation.Rel, operation.ExpectedIdentity, operation.Target.Mems,
		); err != nil {
			writeErr := w.classifyWriteError(
				err, phase, HierarchyOperationWriteMems, operation, "cpuset.mems",
				operation.ExpectedCurrent.Mems, operation.Target.Mems)
			evidenceErr := w.recordUncertainPhysicalWrite(ctx, write, stack)
			if evidenceErr != nil {
				return AppliedPlanOperation{}, newExecutionEvidenceError(writeErr, evidenceErr)
			}
			return AppliedPlanOperation{}, writeErr
		}
		stack.writes = append(stack.writes, write)
	}
	if !operation.ExpectedCurrent.CPUs.Equals(operation.Target.CPUs) {
		write := physicalWriteBeforeFromEntry(
			current, operation, HierarchyOperationWriteCPUs, operation.Target.CPUs.String(),
			logicalOperationIndex, phase, preflight.after.EffectiveCPUs.String())
		if err := ticket.consumeForward(PhysicalWriteCost{CPUSetWrites: 1}); err != nil {
			return AppliedPlanOperation{}, err
		}
		w.recordPhysicalWriteAttempt()
		if err := w.driver.WriteCPUs(
			ctx, operation.Rel, operation.ExpectedIdentity, operation.Target.CPUs,
		); err != nil {
			writeErr := w.classifyWriteError(
				err, phase, HierarchyOperationWriteCPUs, operation, "cpuset.cpus",
				operation.ExpectedCurrent.CPUs.String(), operation.Target.CPUs.String())
			evidenceErr := w.recordUncertainPhysicalWrite(ctx, write, stack)
			if evidenceErr != nil {
				return AppliedPlanOperation{}, newExecutionEvidenceError(writeErr, evidenceErr)
			}
			return AppliedPlanOperation{}, writeErr
		}
		stack.writes = append(stack.writes, write)
	}
	return w.readAfterWrite(ctx, operation)
}

func (w safeCPSetWriter) validateFrozenOperationPredecessor(
	ctx context.Context,
	operation PlanOperation,
	preflight frozenOperationPreflight,
) (EntryState, bool, error) {
	current, err := w.driver.ReadEntry(ctx, operation.Rel)
	if err != nil {
		return EntryState{}, false, w.classifyHierarchyReadError(err, operation)
	}
	if current.Identity != preflight.before.Identity ||
		current.Identity != preflight.after.Identity {
		return EntryState{}, false, &PlanStaleError{
			Rel: operation.Rel, Direction: operation.Direction, Resource: "identity",
			Current: fmt.Sprint(current.Identity), Target: fmt.Sprint(preflight.before.Identity),
			Err: fmt.Errorf("%w: frozen pre-write identity changed", ErrCgroupIdentityChanged),
		}
	}
	if frozenOperationStateEqual(current, preflight.after) {
		return current, true, nil
	}
	if !frozenOperationStateEqual(current, preflight.before) {
		return EntryState{}, false, &PlanStaleError{
			Rel: operation.Rel, Direction: operation.Direction,
			Resource: "pre_write_predecessor",
			Current:  frozenOperationStateString(freezeOperationState(current)),
			Target:   frozenOperationStateString(preflight.before),
			Err:      fmt.Errorf("live physical state does not match the complete frozen predecessor"),
		}
	}

	if operation.ParentRel != "" {
		parent, parentErr := w.driver.ReadEntry(ctx, operation.ParentRel)
		if parentErr != nil {
			return EntryState{}, false, w.classifyHierarchyReadError(parentErr, operation)
		}
		if parent.Identity != preflight.parentIdentity ||
			parent.Identity != operation.ExpectedParentIdentity {
			return EntryState{}, false, &PlanStaleError{
				Rel: operation.Rel, Direction: operation.Direction,
				Resource: "parent_identity",
				Current:  fmt.Sprint(parent.Identity),
				Target:   fmt.Sprint(preflight.parentIdentity),
				Err:      fmt.Errorf("%w: frozen predecessor parent identity changed", ErrCgroupIdentityChanged),
			}
		}
	}

	children, err := scanFrozenLiveChildrenOnce(
		ctx, w.driver, operation, preflight.children, true, nil)
	if err != nil {
		return EntryState{}, false, err
	}
	if !children.cpus.Equals(preflight.children.cpus) {
		return EntryState{}, false, &PlanStaleError{
			Rel: operation.Rel, Direction: operation.Direction,
			Resource: "child_union",
			Current:  children.cpus.String(),
			Target:   preflight.children.cpus.String(),
			Err:      fmt.Errorf("live child CPU union changed from frozen predecessor"),
		}
	}
	if !children.mems.Equals(preflight.children.mems) {
		return EntryState{}, false, &PlanStaleError{
			Rel: operation.Rel, Direction: operation.Direction,
			Resource: "child_union_cpuset.mems",
			Current:  children.mems.String(),
			Target:   preflight.children.mems.String(),
			Err:      fmt.Errorf("live child mems union changed from frozen predecessor"),
		}
	}
	return current, false, nil
}

func frozenOperationStateEqual(
	current EntryState,
	expected frozenOperationState,
) bool {
	return current.Identity == expected.Identity &&
		current.ConfiguredCPUs.Equals(expected.ConfiguredCPUs) &&
		current.CPUs.Equals(expected.EffectiveCPUs) &&
		cpusetListValuesEqual(current.ConfiguredMems, expected.ConfiguredMems) &&
		cpusetListValuesEqual(current.Mems, expected.EffectiveMems)
}

func cpusetListValuesEqual(left, right string) bool {
	if left == "" || right == "" {
		return left == right
	}
	leftSet, leftErr := machine.Parse(left)
	rightSet, rightErr := machine.Parse(right)
	return leftErr == nil && rightErr == nil && leftSet.Equals(rightSet)
}

func frozenOperationStateString(
	state frozenOperationState,
) string {
	return fmt.Sprintf(
		"configured_cpus=%s effective_cpus=%s configured_mems=%s effective_mems=%s",
		state.ConfiguredCPUs.String(), state.EffectiveCPUs.String(),
		state.ConfiguredMems, state.EffectiveMems)
}

func (w safeCPSetWriter) capturePhysicalWriteBefore(
	ctx context.Context,
	operation PlanOperation,
	resource HierarchyOperation,
	after string,
	logicalOperationIndex int,
	phase PhaseKind,
) (AppliedPhysicalWrite, error) {
	current, err := w.driver.ReadEntry(ctx, operation.Rel)
	if err != nil {
		return AppliedPhysicalWrite{}, w.classifyHierarchyReadError(err, operation)
	}
	if current.Identity != operation.ExpectedIdentity {
		return AppliedPhysicalWrite{}, fmt.Errorf(
			"%w: pre-write rel=%q expected=%v current=%v",
			ErrCgroupIdentityChanged, operation.Rel,
			operation.ExpectedIdentity, current.Identity)
	}
	if resource != HierarchyOperationWriteCPUs &&
		resource != HierarchyOperationWriteMems {
		return AppliedPhysicalWrite{}, fmt.Errorf(
			"unsupported physical write resource %q for %q", resource, operation.Rel)
	}
	return physicalWriteBeforeFromEntry(
		current, operation, resource, after, logicalOperationIndex, phase,
	), nil
}

func physicalWriteBeforeFromEntry(
	current EntryState,
	operation PlanOperation,
	resource HierarchyOperation,
	after string,
	logicalOperationIndex int,
	phase PhaseKind,
	afterEffective ...string,
) AppliedPhysicalWrite {
	write := AppliedPhysicalWrite{
		PlanID: operation.PlanID, Rel: operation.Rel, Identity: current.Identity,
		Direction: operation.Direction, Resource: resource, After: after,
		Impact: PhysicalImpactConfirmed, LogicalOperationIndex: logicalOperationIndex,
		Phase: phase,
	}
	write.AfterEffective = after
	if len(afterEffective) > 0 {
		write.AfterEffective = afterEffective[0]
	}
	switch resource {
	case HierarchyOperationWriteCPUs:
		write.Before = current.ConfiguredCPUs.String()
		write.BeforeEffective = current.CPUs.String()
	case HierarchyOperationWriteMems:
		write.Before = current.ConfiguredMems
		write.BeforeEffective = current.Mems
	default:
		return AppliedPhysicalWrite{}
	}
	return write
}

// recordUncertainPhysicalWrite performs a generation-pinned read-back after a
// failed write. When read-back cannot establish the physical result, it keeps a
// conservative inverse candidate so rollback still uses the original identity.
// A replacement generation is never written by the rollback path.
func (w safeCPSetWriter) recordUncertainPhysicalWrite(
	ctx context.Context,
	write AppliedPhysicalWrite,
	stack *traceMutationStack,
) error {
	current, err := w.driver.ReadEntry(ctx, write.Rel)
	if err != nil {
		write.Impact = PhysicalImpactUncertain
		stack.writes = append(stack.writes, write)
		return fmt.Errorf("read back uncertain %s write for %q: %w",
			write.Resource, write.Rel, err)
	}
	if current.Identity != write.Identity {
		write.Impact = PhysicalImpactUncertain
		stack.writes = append(stack.writes, write)
		return fmt.Errorf(
			"%w: uncertain %s write read-back rel=%q expected=%v current=%v",
			ErrCgroupIdentityChanged, write.Resource, write.Rel,
			write.Identity, current.Identity)
	}
	switch write.Resource {
	case HierarchyOperationWriteCPUs:
		configured := current.ConfiguredCPUs.String()
		if configured != write.Before {
			if configured != write.After {
				write.Impact = PhysicalImpactUncertain
			}
			stack.writes = append(stack.writes, write)
		}
	case HierarchyOperationWriteMems:
		if current.ConfiguredMems != write.Before {
			if current.ConfiguredMems != write.After {
				write.Impact = PhysicalImpactUncertain
			}
			stack.writes = append(stack.writes, write)
		}
	}
	return nil
}

func (w safeCPSetWriter) failFrozenTrace(
	ctx context.Context,
	executionErr error,
	stack *traceMutationStack,
	ticket *ExecutionReservationTicket,
	res *ConvergenceResult,
	journalStart, appliedStart int,
) error {
	res.ParentSafe = false
	res.Converged = false
	res.State = ConvergenceStateNonConverged
	res.ConvergenceReport = ConvergenceReport{}
	res.FinalSnapshot = nil
	res.FinalSnapshotCurrent = false
	res.DeferredLeafCount = 0
	res.DeferredCPUCount = 0

	recoveryCtx, cancelRecovery := newFrozenTraceRecoveryContext()
	defer cancelRecovery()
	rollbackErr := w.rollbackTracePrefix(recoveryCtx, stack, ticket)
	if rollbackErr == nil {
		res.Journal = res.Journal[:journalStart]
		res.Applied = appliedStart
		var finalDrift interface{ FrozenFinalSnapshotDrift() bool }
		if errors.As(executionErr, &finalDrift) && finalDrift.FrozenFinalSnapshotDrift() {
			return &frozenSnapshotDriftAfterVerifiedRollbackError{err: executionErr}
		}
		return executionErr
	}
	w.rebuildPhysicalImpactEvidence(stack, res, journalStart, appliedStart)
	return newExecutionRollbackError(executionErr, rollbackErr)
}

// newFrozenTraceRecoveryContext detaches rollback from forward cancellation and
// deadlines. The recovery window starts now and remains bounded by its short
// internal cap; rollback work is bounded separately by the ticket's reserved
// write and hierarchy-I/O quotas.
func newFrozenTraceRecoveryContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), frozenTraceRecoveryTimeout)
}

func (w safeCPSetWriter) rollbackTracePrefix(
	ctx context.Context,
	stack *traceMutationStack,
	ticket *ExecutionReservationTicket,
) error {
	if stack == nil || len(stack.writes) == 0 {
		return nil
	}
	rollbackDriver := newRollbackHierarchyDriver(w.driver, ticket)
	var rollbackErrors []error
	for i := len(stack.writes) - 1; i >= 0; i-- {
		write := stack.writes[i]
		current, err := rollbackDriver.ReadEntry(ctx, write.Rel)
		if err != nil {
			rollbackErrors = append(rollbackErrors,
				fmt.Errorf("read entry before rollback %s for %q: %w",
					write.Resource, write.Rel, err))
			continue
		}
		if current.Identity != write.Identity {
			rollbackErrors = append(rollbackErrors, fmt.Errorf(
				"%w: refuse rollback %s for replacement rel=%q expected=%v current=%v",
				ErrCgroupIdentityChanged, write.Resource, write.Rel,
				write.Identity, current.Identity))
			continue
		}
		switch classifyRollbackPhysicalState(write, current) {
		case rollbackPhysicalStateBefore:
			continue
		case rollbackPhysicalStateAfter:
		case rollbackPhysicalStateThird:
			rollbackErrors = append(rollbackErrors, fmt.Errorf(
				"refuse rollback %s for %q: same-generation state matches neither rollback after nor before: current=%s after=%s before=%s",
				write.Resource, write.Rel,
				rollbackPhysicalStateString(write.Resource, current),
				rollbackPhysicalExpectedString(write.Resource, write.After, write.AfterEffective),
				rollbackPhysicalExpectedString(write.Resource, write.Before, write.BeforeEffective),
			))
			continue
		}

		switch write.Resource {
		case HierarchyOperationWriteCPUs:
			if err := ticket.consumeRollback(PhysicalWriteCost{CPUSetWrites: 1}); err != nil {
				rollbackErrors = append(rollbackErrors, err)
				continue
			}
			before, err := machine.Parse(write.Before)
			if err != nil {
				rollbackErrors = append(rollbackErrors,
					fmt.Errorf("parse rollback cpuset.cpus for %q: %w", write.Rel, err))
				continue
			}
			w.recordPhysicalWriteAttempt()
			if err := rollbackDriver.WriteCPUs(ctx, write.Rel, write.Identity, before); err != nil {
				rollbackErrors = append(rollbackErrors,
					fmt.Errorf("rollback cpuset.cpus for %q: %w", write.Rel, err))
			}
		case HierarchyOperationWriteMems:
			if err := ticket.consumeRollback(PhysicalWriteCost{MemsWrites: 1}); err != nil {
				rollbackErrors = append(rollbackErrors, err)
				continue
			}
			w.recordPhysicalWriteAttempt()
			if err := rollbackDriver.WriteMems(ctx, write.Rel, write.Identity, write.Before); err != nil {
				rollbackErrors = append(rollbackErrors,
					fmt.Errorf("rollback cpuset.mems for %q: %w", write.Rel, err))
			}
		default:
			rollbackErrors = append(rollbackErrors,
				fmt.Errorf("unsupported rollback resource %q for %q", write.Resource, write.Rel))
		}
	}
	rollbackErrors = append(rollbackErrors, w.verifyRolledBackPrefix(ctx, rollbackDriver, stack)...)
	return utilerrors.NewAggregate(rollbackErrors)
}

type rollbackPhysicalState uint8

const (
	rollbackPhysicalStateAfter rollbackPhysicalState = iota
	rollbackPhysicalStateBefore
	rollbackPhysicalStateThird
)

func classifyRollbackPhysicalState(
	write AppliedPhysicalWrite,
	current EntryState,
) rollbackPhysicalState {
	if rollbackPhysicalResourceEqual(
		write.Resource, current, write.After, write.AfterEffective) {
		return rollbackPhysicalStateAfter
	}
	if rollbackPhysicalResourceEqual(
		write.Resource, current, write.Before, write.BeforeEffective) {
		return rollbackPhysicalStateBefore
	}
	return rollbackPhysicalStateThird
}

func rollbackPhysicalResourceEqual(
	resource HierarchyOperation,
	current EntryState,
	configured, effective string,
) bool {
	if effective == "" && configured != "" {
		effective = configured
	}
	switch resource {
	case HierarchyOperationWriteCPUs:
		configuredCPUs, configuredErr := machine.Parse(configured)
		effectiveCPUs, effectiveErr := machine.Parse(effective)
		return configuredErr == nil && effectiveErr == nil &&
			current.ConfiguredCPUs.Equals(configuredCPUs) &&
			current.CPUs.Equals(effectiveCPUs)
	case HierarchyOperationWriteMems:
		return cpusetListValuesEqual(current.ConfiguredMems, configured) &&
			cpusetListValuesEqual(current.Mems, effective)
	default:
		return false
	}
}

func rollbackPhysicalStateString(
	resource HierarchyOperation,
	current EntryState,
) string {
	switch resource {
	case HierarchyOperationWriteCPUs:
		return rollbackPhysicalExpectedString(
			resource, current.ConfiguredCPUs.String(), current.CPUs.String())
	case HierarchyOperationWriteMems:
		return rollbackPhysicalExpectedString(
			resource, current.ConfiguredMems, current.Mems)
	default:
		return "<unsupported>"
	}
}

func rollbackPhysicalExpectedString(
	resource HierarchyOperation,
	configured, effective string,
) string {
	if effective == "" && configured != "" {
		effective = configured
	}
	return fmt.Sprintf("%s(configured=%s effective=%s)",
		resource, configured, effective)
}

func (w safeCPSetWriter) verifyRolledBackPrefix(
	ctx context.Context,
	driver HierarchyDriver,
	stack *traceMutationStack,
) []error {
	type initialPhysicalState struct {
		identity       CgroupIdentity
		configuredCPUs *string
		effectiveCPUs  *string
		configuredMems *string
		effectiveMems  *string
	}
	initialByRel := make(map[string]*initialPhysicalState)
	for _, write := range stack.writes {
		initial := initialByRel[write.Rel]
		if initial == nil {
			initial = &initialPhysicalState{identity: write.Identity}
			initialByRel[write.Rel] = initial
		}
		switch write.Resource {
		case HierarchyOperationWriteCPUs:
			if initial.configuredCPUs == nil {
				configured := write.Before
				effective := write.BeforeEffective
				initial.configuredCPUs = &configured
				initial.effectiveCPUs = &effective
			}
		case HierarchyOperationWriteMems:
			if initial.configuredMems == nil {
				configured := write.Before
				effective := write.BeforeEffective
				initial.configuredMems = &configured
				initial.effectiveMems = &effective
			}
		}
	}

	var verificationErrors []error
	stack.rollbackObservations = make(map[string]rollbackObservation, len(initialByRel))
	for _, rel := range rollbackVerificationRels(stack) {
		initial := initialByRel[rel]
		current, err := driver.ReadEntry(ctx, rel)
		if err != nil {
			stack.rollbackObservations[rel] = rollbackObservation{err: err}
			verificationErrors = append(verificationErrors,
				fmt.Errorf("read %q after rollback: %w", rel, err))
			continue
		}
		if current.Identity != initial.identity {
			identityErr := fmt.Errorf(
				"%w: rollback verification rel=%q expected=%v current=%v",
				ErrCgroupIdentityChanged, rel, initial.identity, current.Identity)
			stack.rollbackObservations[rel] = rollbackObservation{
				current: current,
				err:     identityErr,
			}
			verificationErrors = append(verificationErrors, identityErr)
			continue
		}
		stack.rollbackObservations[rel] = rollbackObservation{current: current}
		if initial.configuredCPUs != nil &&
			current.ConfiguredCPUs.String() != *initial.configuredCPUs {
			verificationErrors = append(verificationErrors, fmt.Errorf(
				"rollback configured cpuset.cpus verification for %q: current=%s expected=%s",
				rel, current.ConfiguredCPUs.String(), *initial.configuredCPUs))
		}
		if initial.effectiveCPUs != nil &&
			current.CPUs.String() != *initial.effectiveCPUs {
			verificationErrors = append(verificationErrors, fmt.Errorf(
				"rollback effective cpuset.cpus verification for %q: current=%s expected=%s",
				rel, current.CPUs.String(), *initial.effectiveCPUs))
		}
		if initial.configuredMems != nil &&
			current.ConfiguredMems != *initial.configuredMems {
			verificationErrors = append(verificationErrors, fmt.Errorf(
				"rollback configured cpuset.mems verification for %q: current=%s expected=%s",
				rel, current.ConfiguredMems, *initial.configuredMems))
		}
		if initial.effectiveMems != nil && current.Mems != *initial.effectiveMems {
			verificationErrors = append(verificationErrors, fmt.Errorf(
				"rollback effective cpuset.mems verification for %q: current=%s expected=%s",
				rel, current.Mems, *initial.effectiveMems))
		}
	}
	return verificationErrors
}

func rollbackVerificationRels(stack *traceMutationStack) []string {
	if stack == nil {
		return nil
	}
	unique := make(map[string]struct{}, len(stack.writes))
	for _, write := range stack.writes {
		unique[write.Rel] = struct{}{}
	}
	rels := make([]string, 0, len(unique))
	for rel := range unique {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	return rels
}

func (w safeCPSetWriter) rebuildPhysicalImpactEvidence(
	stack *traceMutationStack,
	res *ConvergenceResult,
	journalStart, appliedStart int,
) {
	if stack == nil || res == nil {
		return
	}
	res.Journal = res.Journal[:journalStart]
	res.Applied = appliedStart

	type resourceChain struct {
		initialConfigured string
		initialEffective  string
		writes            []AppliedPhysicalWrite
	}
	chains := make(map[string]*resourceChain)
	chainKeys := make([]string, 0)
	for _, write := range stack.writes {
		key := write.Rel + "\x00" + string(write.Resource)
		chain := chains[key]
		if chain == nil {
			chain = &resourceChain{
				initialConfigured: write.Before,
				initialEffective:  write.BeforeEffective,
			}
			chains[key] = chain
			chainKeys = append(chainKeys, key)
		}
		chain.writes = append(chain.writes, write)
	}

	type logicalOperationKey struct {
		index  int
		phase  PhaseKind
		rel    string
		planID string
	}
	type logicalOperationEvidence struct {
		key     logicalOperationKey
		applied AppliedPlanOperation
	}
	evidenceByOperation := make(map[logicalOperationKey]*logicalOperationEvidence)
	evidence := make([]*logicalOperationEvidence, 0, len(chainKeys))

	for _, chainKey := range chainKeys {
		chain := chains[chainKey]
		last := chain.writes[len(chain.writes)-1]
		observation, observed := stack.rollbackObservations[last.Rel]
		impact := PhysicalImpactConfirmed
		if !observed || observation.err != nil {
			impact = PhysicalImpactUncertain
		} else if physicalResourceRestored(
			last.Resource, observation.current,
			chain.initialConfigured, chain.initialEffective,
		) {
			continue
		}

		source := last
		if observed && observation.err == nil {
			matched := false
			for i := len(chain.writes) - 1; i >= 0; i-- {
				if rollbackPhysicalResourceEqual(
					last.Resource,
					observation.current,
					chain.writes[i].After,
					chain.writes[i].AfterEffective,
				) {
					source = chain.writes[i]
					matched = true
					break
				}
			}
			if !matched {
				impact = PhysicalImpactUncertain
			}
		}

		key := logicalOperationKey{
			index:  source.LogicalOperationIndex,
			phase:  source.Phase,
			rel:    source.Rel,
			planID: source.PlanID,
		}
		item := evidenceByOperation[key]
		if item == nil {
			item = &logicalOperationEvidence{
				key: key,
				applied: AppliedPlanOperation{
					PlanID: source.PlanID, Rel: source.Rel, Direction: source.Direction,
					Resource: source.Resource, PhysicalImpact: impact,
					LogicalOperationIndex: source.LogicalOperationIndex, Phase: source.Phase,
				},
			}
			evidenceByOperation[key] = item
			evidence = append(evidence, item)
		} else {
			// Preserve the latest physical resource as the representative
			// resource while CPU and mems evidence is merged logically.
			item.applied.Resource = source.Resource
			if impact == PhysicalImpactUncertain {
				item.applied.PhysicalImpact = PhysicalImpactUncertain
			}
		}

		switch source.Resource {
		case HierarchyOperationWriteCPUs:
			if cpus, err := machine.Parse(source.After); err == nil {
				item.applied.Target.CPUs = cpus
				item.applied.Observed.CPUs = cpus.Clone()
			}
			if observed && observation.err == nil {
				item.applied.Observed.CPUs = observation.current.CPUs.Clone()
			}
		case HierarchyOperationWriteMems:
			item.applied.Target.Mems = source.After
			item.applied.Observed.Mems = source.After
			if observed && observation.err == nil {
				item.applied.Observed.Mems = observation.current.Mems
			}
		}
	}

	sort.SliceStable(evidence, func(i, j int) bool {
		left, right := evidence[i].key, evidence[j].key
		if left.index != right.index {
			return left.index < right.index
		}
		if left.phase != right.phase {
			return left.phase < right.phase
		}
		if left.rel != right.rel {
			return left.rel < right.rel
		}
		return left.planID < right.planID
	})
	for _, item := range evidence {
		res.Journal = append(res.Journal, item.applied)
		res.Applied++
	}
}

func physicalResourceRestored(
	resource HierarchyOperation,
	current EntryState,
	initialConfigured, initialEffective string,
) bool {
	switch resource {
	case HierarchyOperationWriteCPUs:
		return current.ConfiguredCPUs.String() == initialConfigured &&
			current.CPUs.String() == initialEffective
	case HierarchyOperationWriteMems:
		return current.ConfiguredMems == initialConfigured &&
			current.Mems == initialEffective
	default:
		return false
	}
}

type executionRelatedError struct {
	execution error
	relation  string
	related   error
}

func (e *executionRelatedError) Error() string {
	return fmt.Sprintf("execution failed: %v; %s: %v", e.execution, e.relation, e.related)
}

// Unwrap deliberately returns the execution error as a single chain so
// errors.Is/errors.As retain PlanStaleError behavior on Go 1.18.
func (e *executionRelatedError) Unwrap() error { return e.execution }

func (e *executionRelatedError) Is(target error) bool {
	return errors.Is(e.execution, target) || errors.Is(e.related, target)
}

func (e *executionRelatedError) As(target interface{}) bool {
	return errors.As(e.execution, target) || errors.As(e.related, target)
}

func newExecutionRollbackError(executionErr, rollbackErr error) error {
	if rollbackErr == nil {
		return executionErr
	}
	return &executionRelatedError{
		execution: executionErr,
		relation:  "rollback failed",
		related:   rollbackErr,
	}
}

func newExecutionEvidenceError(executionErr, evidenceErr error) error {
	if evidenceErr == nil {
		return executionErr
	}
	return &executionRelatedError{
		execution: executionErr,
		relation:  "physical impact evidence failed",
		related:   evidenceErr,
	}
}

type rollbackHierarchyDriver struct {
	HierarchyDriver
	ticket *ExecutionReservationTicket
}

func newRollbackHierarchyDriver(
	driver HierarchyDriver,
	ticket *ExecutionReservationTicket,
) HierarchyDriver {
	for {
		switch wrapped := driver.(type) {
		case *budgetedHierarchyDriver:
			driver = wrapped.driver
		case *strictReservedHierarchyDriver:
			driver = wrapped.HierarchyDriver
		default:
			return &rollbackHierarchyDriver{
				HierarchyDriver: driver,
				ticket:          ticket,
			}
		}
	}
}

func (d *rollbackHierarchyDriver) consume(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.ticket.consumeRollbackIO()
}

func (d *rollbackHierarchyDriver) Roots(ctx context.Context) ([]RootRef, error) {
	if err := d.consume(ctx); err != nil {
		return nil, err
	}
	return d.HierarchyDriver.Roots(ctx)
}

func (d *rollbackHierarchyDriver) StatIdentity(ctx context.Context, rel string) (CgroupIdentity, error) {
	if err := d.consume(ctx); err != nil {
		return CgroupIdentity{}, err
	}
	return d.HierarchyDriver.StatIdentity(ctx, rel)
}

func (d *rollbackHierarchyDriver) ReadEntry(ctx context.Context, rel string) (EntryState, error) {
	if err := d.consume(ctx); err != nil {
		return EntryState{}, err
	}
	return d.HierarchyDriver.ReadEntry(ctx, rel)
}

func (d *rollbackHierarchyDriver) ListChildren(ctx context.Context, rel string) ([]ChildRef, error) {
	if err := d.consume(ctx); err != nil {
		return nil, err
	}
	return d.HierarchyDriver.ListChildren(ctx, rel)
}

func (d *rollbackHierarchyDriver) WriteCPUs(
	ctx context.Context,
	rel string,
	expected CgroupIdentity,
	cpus machine.CPUSet,
) error {
	if err := d.consume(ctx); err != nil {
		return err
	}
	return d.HierarchyDriver.WriteCPUs(ctx, rel, expected, cpus)
}

func (d *rollbackHierarchyDriver) WriteMems(
	ctx context.Context,
	rel string,
	expected CgroupIdentity,
	mems string,
) error {
	if err := d.consume(ctx); err != nil {
		return err
	}
	return d.HierarchyDriver.WriteMems(ctx, rel, expected, mems)
}
