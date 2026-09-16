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

// preflightFrozenTrace proves that a frozen trace is executable from one fresh
// complete snapshot. Every operation is validated and applied to an isolated
// projected hierarchy in global trace order; no live hierarchy write occurs.
func (w safeCPSetWriter) preflightFrozenTrace(
	ctx context.Context,
	trace *CompiledPhaseTrace,
) error {
	if w.driver == nil {
		return fmt.Errorf("frozen trace preflight requires hierarchy driver")
	}
	if w.budget == nil {
		return fmt.Errorf("frozen trace preflight requires convergence budget")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	frozen, err := FreezePhaseTrace(trace)
	if err != nil {
		return fmt.Errorf("freeze phase trace before preflight: %w", err)
	}
	if frozen.InitialSnapshot.ScanBoundary.Purpose != ScanForPlan {
		return fmt.Errorf(
			"frozen trace preflight requires plan snapshot evidence, got %q",
			frozen.InitialSnapshot.ScanBoundary.Purpose,
		)
	}
	dag, err := BuildDAG(cloneNodeSpecs(frozen.EvaluationInput.DAGSpecs))
	if err != nil {
		return fmt.Errorf("rebuild frozen trace DAG for preflight: %w", err)
	}
	fresh, err := BuildCompleteSnapshotForBoundary(
		ctx,
		w.driver,
		dag,
		cloneScanBoundary(frozen.InitialSnapshot.ScanBoundary),
		w.budget,
	)
	if err != nil {
		return fmt.Errorf("capture frozen trace preflight snapshot: %w", err)
	}
	if fresh.ID != frozen.InitialSnapshot.ID {
		return fmt.Errorf(
			"frozen trace initial snapshot drift: current=%x expected=%x",
			fresh.ID, frozen.InitialSnapshot.ID,
		)
	}

	projection, err := newProjectedHierarchy(fresh, frozen.Capabilities)
	if err != nil {
		return fmt.Errorf("create frozen trace preflight projection: %w", err)
	}
	for phaseIndex, phase := range frozen.Phases {
		for operationIndex, operation := range phase.Operations {
			if err := projection.applyOperation(operation); err != nil {
				return fmt.Errorf(
					"preflight frozen phase trace operation %d/%d: %w",
					phaseIndex, operationIndex, err,
				)
			}
		}
	}
	if projection.snapshot.ID != frozen.FinalSnapshot.ID {
		return fmt.Errorf(
			"frozen trace projected final snapshot drift: projected=%x expected=%x",
			projection.snapshot.ID, frozen.FinalSnapshot.ID,
		)
	}
	return nil
}

// executeFrozenTrace applies exactly the globally ordered operations authorized
// by ticket. It never replans. Any execution or final-proof failure rolls back
// the complete physical-write prefix accumulated by this invocation.
func (r *coordinatorRound) executeFrozenTrace(
	ctx context.Context,
	trace *CompiledPhaseTrace,
	ticket *ExecutionReservationTicket,
	res *ConvergenceResult,
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
	frozen, err := FreezePhaseTrace(trace)
	if err != nil {
		return outcome, err
	}
	defer ticket.ReleaseUnused()

	writer := newSafeCPUSetWriter(r.driver, r.budget, res)
	if err := writer.preflightFrozenTrace(ctx, frozen); err != nil {
		return outcome, err
	}
	writer.driver = NewBudgetedHierarchyDriver(r.driver, r.budget)

	journalStart := len(res.Journal)
	appliedStart := res.Applied
	stack := &traceMutationStack{}
	operationIndex := 0
	for _, phase := range frozen.Phases {
		for _, operation := range phase.Operations {
			if err := ticket.AuthorizeNext(frozen.TraceID, operationIndex, operation); err != nil {
				return outcome, writer.failFrozenTrace(
					ctx, err, stack, ticket, res, journalStart, appliedStart)
			}
			operationIndex++
			res.Attempted++
			applied, applyErr := writer.applyFrozenOperation(
				ctx, phase.Kind, operationIndex-1, operation, stack, ticket)
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

	fresh, err := BuildCompleteSnapshotForBoundary(
		ctx,
		r.driver,
		r.dag,
		cloneScanBoundary(frozen.FinalSnapshot.ScanBoundary),
		r.budget,
	)
	if err == nil && fresh.ID != frozen.FinalSnapshot.ID {
		err = fmt.Errorf(
			"frozen trace final snapshot drift: current=%x expected=%x",
			fresh.ID, frozen.FinalSnapshot.ID,
		)
	}
	if err == nil && !traceObjectiveSatisfied(frozen) {
		err = fmt.Errorf("frozen trace final objective is not satisfied")
	}
	if err != nil {
		res.Failed++
		return outcome, writer.failFrozenTrace(
			ctx,
			fmt.Errorf("prove frozen trace final state: %w", err),
			stack, ticket, res, journalStart, appliedStart)
	}

	res.FinalSnapshot = fresh
	res.FinalSnapshotCurrent = true
	res.ConvergenceReport = frozen.FinalEvaluation.Report
	res.ParentSafe = frozen.Objective == ConvergenceObjectiveParentSafe &&
		frozen.FinalEvaluation.ParentSafety.Safe
	res.Converged = frozen.FinalEvaluation.Report.FullyConverged
	if res.Converged {
		res.State = ConvergenceStateConverged
		outcome.Status = RoundStatusConverged
	} else {
		res.State = ConvergenceStateParentSafeLeafDeferred
		outcome.Status = RoundStatusProgress
	}
	outcome.Snapshot = fresh
	outcome.Journal = append(
		outcome.Journal, res.Journal[journalStart:]...)
	return outcome, nil
}

func (w safeCPSetWriter) applyFrozenOperation(
	ctx context.Context,
	phase PhaseKind,
	logicalOperationIndex int,
	operation PlanOperation,
	stack *traceMutationStack,
	ticket *ExecutionReservationTicket,
) (AppliedPlanOperation, error) {
	if err := ctx.Err(); err != nil {
		return AppliedPlanOperation{}, err
	}
	if operation.WriteMems && operation.ExpectedCurrent.Mems != operation.Target.Mems {
		write, err := w.capturePhysicalWriteBefore(
			ctx, operation, HierarchyOperationWriteMems, operation.Target.Mems,
			logicalOperationIndex, phase)
		if err != nil {
			return AppliedPlanOperation{}, err
		}
		if err := ticket.consumeForward(PhysicalWriteCost{MemsWrites: 1}); err != nil {
			return AppliedPlanOperation{}, err
		}
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
		write, err := w.capturePhysicalWriteBefore(
			ctx, operation, HierarchyOperationWriteCPUs, operation.Target.CPUs.String(),
			logicalOperationIndex, phase)
		if err != nil {
			return AppliedPlanOperation{}, err
		}
		if err := ticket.consumeForward(PhysicalWriteCost{CPUSetWrites: 1}); err != nil {
			return AppliedPlanOperation{}, err
		}
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
	write := AppliedPhysicalWrite{
		PlanID: operation.PlanID, Rel: operation.Rel, Identity: current.Identity,
		Direction: operation.Direction, Resource: resource, After: after,
		Impact: PhysicalImpactConfirmed, LogicalOperationIndex: logicalOperationIndex,
		Phase: phase,
	}
	switch resource {
	case HierarchyOperationWriteCPUs:
		write.Before = current.ConfiguredCPUs.String()
		write.BeforeEffective = current.CPUs.String()
	case HierarchyOperationWriteMems:
		write.Before = current.ConfiguredMems
		write.BeforeEffective = current.Mems
	default:
		return AppliedPhysicalWrite{}, fmt.Errorf(
			"unsupported physical write resource %q for %q", resource, operation.Rel)
	}
	return write, nil
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
	res.FinalSnapshotCurrent = false

	recoveryCtx, cancelRecovery := newFrozenTraceRecoveryContext(ctx, w.budget)
	defer cancelRecovery()
	rollbackErr := w.rollbackTracePrefix(recoveryCtx, stack, ticket)
	if rollbackErr == nil {
		res.Journal = res.Journal[:journalStart]
		res.Applied = appliedStart
		return executionErr
	}
	w.rebuildPhysicalImpactEvidence(stack, res, journalStart, appliedStart)
	return newExecutionRollbackError(executionErr, rollbackErr)
}

// newFrozenTraceRecoveryContext detaches rollback from forward cancellation
// while retaining the invocation's absolute deadline and a short internal cap.
// The rollback driver additionally enforces the ticket's reserved write and I/O
// budgets, so detached recovery remains bounded on both time and work.
func newFrozenTraceRecoveryContext(
	invocationCtx context.Context,
	budget *BudgetTracker,
) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(frozenTraceRecoveryTimeout)
	if invocationCtx != nil {
		if invocationDeadline, ok := invocationCtx.Deadline(); ok {
			deadline = earliestDeadline(deadline, invocationDeadline)
		}
	}
	if budget != nil {
		deadline = earliestDeadline(deadline, budget.Deadline())
	}
	return context.WithDeadline(context.Background(), deadline)
}

func (w safeCPSetWriter) rollbackTracePrefix(
	ctx context.Context,
	stack *traceMutationStack,
	ticket *ExecutionReservationTicket,
) error {
	if stack == nil || len(stack.writes) == 0 {
		return nil
	}
	rollbackDriver := newRollbackHierarchyDriver(w.driver, w.budget, ticket)
	var rollbackErrors []error
	for i := len(stack.writes) - 1; i >= 0; i-- {
		write := stack.writes[i]
		currentIdentity, err := rollbackDriver.StatIdentity(ctx, write.Rel)
		if err != nil {
			rollbackErrors = append(rollbackErrors,
				fmt.Errorf("stat identity before rollback %s for %q: %w",
					write.Resource, write.Rel, err))
			continue
		}
		if currentIdentity != write.Identity {
			rollbackErrors = append(rollbackErrors, fmt.Errorf(
				"%w: refuse rollback %s for replacement rel=%q expected=%v current=%v",
				ErrCgroupIdentityChanged, write.Resource, write.Rel,
				write.Identity, currentIdentity))
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
			if err := rollbackDriver.WriteCPUs(ctx, write.Rel, write.Identity, before); err != nil {
				rollbackErrors = append(rollbackErrors,
					fmt.Errorf("rollback cpuset.cpus for %q: %w", write.Rel, err))
			}
		case HierarchyOperationWriteMems:
			if err := ticket.consumeRollback(PhysicalWriteCost{MemsWrites: 1}); err != nil {
				rollbackErrors = append(rollbackErrors, err)
				continue
			}
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
			configured := configuredPhysicalResource(last.Resource, observation.current)
			matched := false
			for i := len(chain.writes) - 1; i >= 0; i-- {
				if configured == chain.writes[i].After {
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

func configuredPhysicalResource(resource HierarchyOperation, current EntryState) string {
	switch resource {
	case HierarchyOperationWriteCPUs:
		return current.ConfiguredCPUs.String()
	case HierarchyOperationWriteMems:
		return current.ConfiguredMems
	default:
		return ""
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
	budget *BudgetTracker
	ticket *ExecutionReservationTicket
}

func newRollbackHierarchyDriver(
	driver HierarchyDriver,
	budget *BudgetTracker,
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
				budget:          budget,
				ticket:          ticket,
			}
		}
	}
}

func (d *rollbackHierarchyDriver) consume(ctx context.Context) error {
	if d.budget == nil {
		return fmt.Errorf("rollback hierarchy driver requires convergence budget")
	}
	if err := d.budget.checkContextDeadline(ctx); err != nil {
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
