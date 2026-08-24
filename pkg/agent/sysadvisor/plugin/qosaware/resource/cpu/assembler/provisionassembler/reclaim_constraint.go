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

package provisionassembler

import (
	"fmt"

	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

// ReclaimConstraintScope identifies an independently constrained reclaim
// target. The distinct type prevents callers from accidentally using unrelated
// strings as keys in per-scope ceiling maps.
type ReclaimConstraintScope string

// NewNonExclusiveReclaimConstraintScope returns the stable scope key for a
// non-exclusive NUMA allocation. Faked NUMA IDs are preserved verbatim.
func NewNonExclusiveReclaimConstraintScope(numaID int) ReclaimConstraintScope {
	return ReclaimConstraintScope(fmt.Sprintf("non-exclusive/%d", numaID))
}

// NewExclusiveReclaimConstraintScope returns the stable scope key for a
// disjoint dedicated NUMA-exclusive region.
func NewExclusiveReclaimConstraintScope(regionName string) ReclaimConstraintScope {
	return ReclaimConstraintScope(fmt.Sprintf("exclusive/%s", regionName))
}

// NewLegacyExclusiveReclaimConstraintScope returns the stable scope key for a
// legacy overlapping dedicated NUMA-exclusive region.
func NewLegacyExclusiveReclaimConstraintScope(regionName string) ReclaimConstraintScope {
	return ReclaimConstraintScope(fmt.Sprintf("legacy-exclusive/%s", regionName))
}

// ApplyReclaimConstraint clamps size and a non-negative quota limit to the
// reserved floor or the scope's dynamic ceiling. A nil ceilings map is valid
// and behaves as if no dynamic ceiling were configured.
func ApplyReclaimConstraint(scope ReclaimConstraintScope, size int, limit float64, reservedForReclaim int,
	constraint ReclaimConstraint, ceilings map[ReclaimConstraintScope]int,
) (int, float64, int) {
	if constraint != ReclaimConstraintReservedFloor || size <= reservedForReclaim {
		return size, limit, 0
	}

	excess := general.Max(size-reservedForReclaim, 0)
	ceiling := reservedForReclaim
	if configured, ok := ceilings[scope]; ok {
		ceiling = general.Max(ceiling, configured)
	}
	if size > ceiling {
		size = ceiling
	}
	if limit >= 0 && limit > float64(size) {
		limit = float64(size)
	}
	return size, limit, excess
}

// RecordReclaimConstraintTarget records the unconstrained target and floor for
// one scope and retains the maximum excess across all scopes. It initializes a
// nil target map and safely ignores a nil result.
func RecordReclaimConstraintTarget(result *types.InternalCPUCalculationResult, constraint ReclaimConstraint,
	scope ReclaimConstraintScope, desired, floor, excess int,
) {
	if result == nil || constraint != ReclaimConstraintReservedFloor {
		return
	}
	if result.ReclaimConstraintTargets == nil {
		result.ReclaimConstraintTargets = make(map[string]types.ReclaimConstraintTarget)
	}
	result.ReclaimConstraintTargets[string(scope)] = types.ReclaimConstraintTarget{
		Desired: desired,
		Floor:   floor,
	}
	result.ReclaimConstraintExcess = general.Max(result.ReclaimConstraintExcess, excess)
}
