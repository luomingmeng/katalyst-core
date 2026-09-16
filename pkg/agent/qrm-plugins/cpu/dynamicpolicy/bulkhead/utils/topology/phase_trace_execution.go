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
	"fmt"
)

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
