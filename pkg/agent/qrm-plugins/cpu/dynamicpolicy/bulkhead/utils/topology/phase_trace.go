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
	"fmt"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// This file declares the frozen-admission trace contract. The concrete
// compilation and execution engine lands in later tasks; the declarations here
// only fix the shared vocabulary so the package builds while the Task 1
// contract tests remain red at runtime.

// ErrNoProgress reports that a fixed-point compilation could not advance toward
// ParentSafe: a projected phase produced no state change, so freezing the trace
// would loop forever instead of converging.
var ErrNoProgress = errors.New("fixed-point compilation made no progress")

// errFixedPointEngineNotImplemented keeps the Task 1 contract tests red at
// runtime until the shared engine is extracted. It must never leak into a
// production path; those callers arrive only after the engine exists.
var errFixedPointEngineNotImplemented = errors.New("fixed-point engine not implemented")

// CompiledPhase is one ordered, frozen phase of a fixed-point trace. Operations
// are already sequenced; execution replays them verbatim without re-planning.
type CompiledPhase struct {
	Kind       PhaseKind
	Operations []PlanOperation
}

// CompiledPhaseTrace is the frozen Drain->Expand fixed point compiled on a
// cloned snapshot. Phases are strictly ordered and FinalEvaluation records the
// convergence and parent-safety proof observed on the projected end state.
type CompiledPhaseTrace struct {
	Phases          []CompiledPhase
	FinalEvaluation coordinatorSnapshotEvaluation
}

// phaseExecutionSession abstracts the surface the fixed-point engine drives:
// it reads a complete snapshot and applies an ordered phase. The compiler binds
// it to a clone-backed projection; live replay binds it to the real driver.
type phaseExecutionSession interface {
	Snapshot(ctx context.Context) (*CompleteSnapshot, error)
	Apply(ctx context.Context, phase PhaseKind, operations []PlanOperation) error
}

// compileFixedPointTrace compiles a complete, frozen Drain->Expand trace on a
// clone of base without touching any live driver.
//
// The concrete compiler lands in a later task; this declaration only fixes the
// contract signature so dependent tasks can compile and be verified.
func (r *coordinatorRound) compileFixedPointTrace(
	_ context.Context,
	_ *CompleteSnapshot,
) (*CompiledPhaseTrace, error) {
	return nil, errFixedPointEngineNotImplemented
}

// runFixedPointEngine drives the shared fixed-point engine against an execution
// session, returning the ordered trace it produced.
//
// The engine is session-agnostic: it only decides and sequences operations,
// reading state through session.Snapshot and mutating it through session.Apply.
// Given the same starting snapshot, a clone-backed projection and a live driver
// therefore compile a byte-for-byte identical Drain->Expand phase sequence. The
// same buildPlan/drainFrontier/rebaseDrainPlan decision helpers that back the
// live coordinator are reused verbatim so compiled and live traces cannot drift.
//
// Each round drains the current frontier, accumulates the release witnesses that
// authorize the following expand, expands toward the desired envelope, and then
// evaluates the projected end state. The loop advances until the snapshot is
// fully converged or, under the ParentSafe objective, proven parent-safe. A
// round that mutates nothing while still short of the objective cannot advance a
// frozen trace, so the engine returns ErrNoProgress instead of looping forever.
func (r *coordinatorRound) runFixedPointEngine(
	ctx context.Context,
	session phaseExecutionSession,
) (*CompiledPhaseTrace, error) {
	if session == nil {
		return nil, fmt.Errorf("fixed-point engine requires an execution session")
	}
	if r.dag == nil || r.driver == nil {
		return nil, fmt.Errorf("fixed-point engine requires a bound DAG and driver")
	}
	capabilities := r.driver.Capabilities()
	parentSafetyTargets := desiredTargets(r.dag)

	phases := make([]CompiledPhase, 0)
	for {
		if r.round >= r.maxRounds {
			return nil, fmt.Errorf("%w: limit=%d used=%d", ErrRoundBudgetExceeded, r.maxRounds, r.round)
		}
		if err := r.budget.ConsumeRound(); err != nil {
			return nil, err
		}
		r.round++
		r.deferredCleanupRels = make(map[string]struct{})

		start, err := session.Snapshot(ctx)
		if err != nil {
			return nil, err
		}

		drain, err := r.buildPlan(ctx, PhaseDrain, start)
		if err != nil {
			return nil, err
		}
		fresh, released, err := r.applyDrainPhases(ctx, session, drain, &phases)
		if err != nil {
			return nil, err
		}

		r.witnesses = r.witnesses[:0]
		for source, destinations := range released {
			for destination, cpus := range destinations {
				if cpus.IsEmpty() {
					continue
				}
				witness := NewReleaseWitness(drain.ConvergenceID, source, destination, cpus, fresh)
				if witness.CPUs.IsEmpty() {
					continue
				}
				r.witnesses = append(r.witnesses, witness)
			}
		}

		expand, err := r.buildPlan(ctx, PhaseExpand, fresh)
		if err != nil {
			return nil, err
		}
		if len(expand.Operations) > 0 {
			if err := session.Apply(ctx, PhaseExpand, expand.Operations); err != nil {
				return nil, err
			}
			phases = append(phases, CompiledPhase{
				Kind:       PhaseExpand,
				Operations: append([]PlanOperation(nil), expand.Operations...),
			})
		}

		final, err := session.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		r.recomputeBlocked(final)

		evaluation, err := evaluateCoordinatorSnapshot(
			final, r.dag, r.targetByRel, parentSafetyTargets, r.desiredMemsByRel(),
			r.desiredDomainUnion(), r.allowedCPUs(),
			r.dynamicByRel, r.requiredByRel, r.deferredByRel,
			r.deferredCleanupRels,
			r.admissionSafetyCPUSet(), capabilities, r.allowEmptyTarget,
		)
		if err != nil {
			return nil, err
		}

		parentSafe := r.objective == ConvergenceObjectiveParentSafe && evaluation.ParentSafety.Safe
		if evaluation.Report.FullyConverged || parentSafe {
			return &CompiledPhaseTrace{Phases: phases, FinalEvaluation: evaluation}, nil
		}
		if final.ID == start.ID {
			return nil, ErrNoProgress
		}
	}
}

// applyDrainPhases executes a drain plan one frontier batch at a time against
// the session, recording every non-empty batch as an ordered CompiledPhase. It
// mirrors executeDrainBatches exactly, substituting session.Snapshot/Apply for
// the live driver so the projected and live traces share the same batch
// boundaries, rebasing, and admission-safety split.
func (r *coordinatorRound) applyDrainPhases(
	ctx context.Context,
	session phaseExecutionSession,
	plan PhasePlan,
	phases *[]CompiledPhase,
) (*CompleteSnapshot, map[DomainID]map[DomainID]machine.CPUSet, error) {
	fresh := plan.Base
	released := make(map[DomainID]map[DomainID]machine.CPUSet)
	if len(plan.Operations) == 0 {
		next, err := session.Snapshot(ctx)
		return next, released, err
	}
	for len(plan.Operations) > 0 {
		batch, err := drainFrontier(plan)
		if err != nil {
			return fresh, released, err
		}
		r.planID = batch.PlanID
		accumulateDrainTransfers(released, plan.TransferGraph, plan.DrainBatch)
		if err := session.Apply(ctx, PhaseDrain, batch.Operations); err != nil {
			if recovered, snapshotErr := session.Snapshot(ctx); snapshotErr == nil {
				fresh = recovered
			}
			return fresh, released, err
		}
		*phases = append(*phases, CompiledPhase{
			Kind:       PhaseDrain,
			Operations: append([]PlanOperation(nil), batch.Operations...),
		})
		next, err := session.Snapshot(ctx)
		if err != nil {
			return fresh, released, err
		}
		fresh = next
		plan, err = rebaseDrainPlan(plan, fresh, r.dag, r.budget)
		if err != nil {
			return fresh, released, err
		}
		if r.objective == ConvergenceObjectiveParentSafe {
			required, _, splitErr := SplitPlanForAdmission(&plan, AdmissionSafetyInput{
				ProtectedPendingCPUSet: r.admissionSafetyCPUSet(),
				DeferredCPUSetByRel:    r.deferredByRel,
			})
			if splitErr != nil {
				return fresh, released, splitErr
			}
			plan = *required
		}
	}
	return fresh, released, nil
}
