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
// The concrete engine lands in a later task; this declaration only fixes the
// contract signature so dependent tasks can compile and be verified.
func (r *coordinatorRound) runFixedPointEngine(
	_ context.Context,
	_ phaseExecutionSession,
) (*CompiledPhaseTrace, error) {
	return nil, errFixedPointEngineNotImplemented
}
