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

package cpu

import (
	"k8s.io/klog/v2"

	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
)

type reclaimConstraintGuard struct {
	hardEnabled               bool
	ceilings                  map[provisionassembler.ReclaimConstraintScope]int
	targets                   map[string]reclaimConstraintTarget
	lastPublishedReclaimTotal int
	hasPublishedReclaimTotal  bool
}

type reclaimConstraintTarget = types.ReclaimConstraintTarget

// constraint decides the reclaim constraint and per-scope ceilings for the current cycle.
//
// The ACK check compares observedReclaimTotal against lastPublishedReclaimTotal to confirm that QRM has
// actually applied the previously published ceiling before ramping up further. This equality relies on the
// hard-partition invariant that both counters describe the same reclaim quantity in the same unit:
//   - observedReclaimTotal is the CPU count of the reclaim pool's topology-aware assignment
//     (machine.CountCPUAssignmentCPUs), i.e. what QRM currently holds.
//   - lastPublishedReclaimTotal is the sum of the reclaim PoolEntries.Size we published last cycle.
// Under hard partition the reclaim pool is exclusive, so its cpuset cardinality equals the published pool
// size; the two are therefore directly comparable. Outside hard partition this guard is inactive
// (hardEnabled is false), so the assumption never has to hold there.
func (g *reclaimConstraintGuard) constraint(
	hardEnabled bool,
	observedReclaimTotal int,
	reclaimObserved bool,
	maxRampUpStep int,
) (
	provisionassembler.ReclaimConstraint,
	map[provisionassembler.ReclaimConstraintScope]int,
) {
	if !hardEnabled {
		return provisionassembler.ReclaimConstraintNone, nil
	}
	if maxRampUpStep <= 0 {
		return provisionassembler.ReclaimConstraintReservedFloor, nil
	}

	ceilings := cloneReclaimCeilings(g.ceilings)
	// Only advance the ceiling once the observed reclaim total matches the last published total (ACK);
	// otherwise hold at the reserved floor to avoid stacking a new ramp-up on an un-acknowledged one.
	if !g.hardEnabled || !g.hasPublishedReclaimTotal || !reclaimObserved ||
		observedReclaimTotal != g.lastPublishedReclaimTotal {
		return provisionassembler.ReclaimConstraintReservedFloor, ceilings
	}
	return provisionassembler.ReclaimConstraintReservedFloor,
		advanceReclaimCeilings(ceilings, g.targets, maxRampUpStep)
}

func (g *reclaimConstraintGuard) commit(
	hardEnabled bool,
	ceilings map[provisionassembler.ReclaimConstraintScope]int,
	targets map[string]reclaimConstraintTarget,
	publishedReclaimTotal int,
	maxRampUpStep int,
) {
	if !hardEnabled {
		*g = reclaimConstraintGuard{}
		return
	}

	g.hardEnabled = true
	g.targets = cloneReclaimTargets(targets)
	g.lastPublishedReclaimTotal = publishedReclaimTotal
	g.hasPublishedReclaimTotal = true
	if maxRampUpStep <= 0 {
		g.ceilings = nil
		klog.Warningf("[qosaware-cpu] keep reclaim constraint at reserved floor because MaxRampUpStep is non-positive: %d", maxRampUpStep)
		return
	}
	g.ceilings = publishedReclaimCeilings(ceilings, targets)
}

func cloneReclaimCeilings(
	ceilings map[provisionassembler.ReclaimConstraintScope]int,
) map[provisionassembler.ReclaimConstraintScope]int {
	cloned := make(map[provisionassembler.ReclaimConstraintScope]int, len(ceilings))
	for scope, ceiling := range ceilings {
		cloned[scope] = ceiling
	}
	return cloned
}

func cloneReclaimTargets(targets map[string]reclaimConstraintTarget) map[string]reclaimConstraintTarget {
	cloned := make(map[string]reclaimConstraintTarget, len(targets))
	for scope, target := range targets {
		cloned[scope] = target
	}
	return cloned
}

func advanceReclaimCeilings(
	ceilings map[provisionassembler.ReclaimConstraintScope]int,
	targets map[string]reclaimConstraintTarget,
	maxRampUpStep int,
) map[provisionassembler.ReclaimConstraintScope]int {
	nextCeilings := make(map[provisionassembler.ReclaimConstraintScope]int, len(targets))
	for scope, target := range targets {
		constraintScope := provisionassembler.ReclaimConstraintScope(scope)
		currentCeiling := target.Floor
		if previous, ok := ceilings[constraintScope]; ok && previous > currentCeiling {
			currentCeiling = previous
		}
		nextCeiling := currentCeiling
		if target.Desired > currentCeiling {
			increment := target.Desired - currentCeiling
			if increment > maxRampUpStep {
				increment = maxRampUpStep
			}
			nextCeiling += increment
		} else {
			nextCeiling = target.Desired
		}
		nextCeilings[constraintScope] = nextCeiling
	}
	return nextCeilings
}

func publishedReclaimCeilings(
	ceilings map[provisionassembler.ReclaimConstraintScope]int,
	targets map[string]reclaimConstraintTarget,
) map[provisionassembler.ReclaimConstraintScope]int {
	published := make(map[provisionassembler.ReclaimConstraintScope]int, len(targets))
	for scope, target := range targets {
		constraintScope := provisionassembler.ReclaimConstraintScope(scope)
		ceiling := target.Floor
		if configured, ok := ceilings[constraintScope]; ok && configured > ceiling {
			ceiling = configured
		}
		size := target.Desired
		if size > ceiling {
			size = ceiling
		}
		published[constraintScope] = size
	}
	return published
}
