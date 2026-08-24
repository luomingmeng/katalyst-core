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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler"
)

func TestReclaimConstraintGuardAdvancesPerScopeCeiling(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	const scope = "non-exclusive/0"
	targets := map[string]reclaimConstraintTarget{
		scope: {Desired: 38, Floor: 24},
	}

	constraint, ceilings := guard.constraint(true, 0, true, 10)
	require.Equal(t, provisionassembler.ReclaimConstraintReservedFloor, constraint)
	require.Empty(t, ceilings)

	guard.commit(true, ceilings, targets, 24, 10)
	_, ceilings = guard.constraint(true, 24, true, 10)
	require.Equal(t, map[provisionassembler.ReclaimConstraintScope]int{scope: 34}, ceilings)

	guard.commit(true, ceilings, targets, 34, 10)
	_, ceilings = guard.constraint(true, 34, true, 10)
	require.Equal(t, map[provisionassembler.ReclaimConstraintScope]int{scope: 38}, ceilings)
}

func TestReclaimConstraintGuardPersistentOrGrowingDemandNeverJumps(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	const scope = "exclusive/region-a"
	targets := map[string]reclaimConstraintTarget{
		scope: {Desired: 30, Floor: 24},
	}
	_, ceilings := guard.constraint(true, 0, true, 4)
	guard.commit(true, ceilings, targets, 24, 4)
	_, ceilings = guard.constraint(true, 24, true, 4)
	require.Equal(t, 28, ceilings[scope])

	guard.commit(true, ceilings, map[string]reclaimConstraintTarget{
		scope: {Desired: 50, Floor: 24},
	}, 28, 4)
	_, ceilings = guard.constraint(true, 28, true, 4)
	require.Equal(t, 32, ceilings[scope])
}

func TestReclaimConstraintGuardDoesNotCommitFailedRound(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	const scope = "non-exclusive/0"

	_, ceilings := guard.constraint(true, 0, true, 10)
	require.Empty(t, ceilings)
	// The caller does not invoke commit when assembly fails.
	_, ceilings = guard.constraint(true, 0, true, 10)
	require.Empty(t, ceilings)

	targets := map[string]reclaimConstraintTarget{
		scope: {Desired: 38, Floor: 24},
	}
	guard.commit(true, ceilings, targets, 24, 10)
	_, ceilings = guard.constraint(true, 24, true, 10)
	require.Equal(t, 34, ceilings[scope])
	// A failed follow-up round must not advance the ceiling.
	_, ceilings = guard.constraint(true, 24, true, 10)
	require.Equal(t, 34, ceilings[scope])
}

func TestReclaimConstraintGuardFailsClosedForNonPositiveRampUpStep(t *testing.T) {
	t.Parallel()

	for _, maxRampUpStep := range []int{0, -1} {
		maxRampUpStep := maxRampUpStep
		t.Run(fmt.Sprintf("max-ramp-up-step-%d", maxRampUpStep), func(t *testing.T) {
			t.Parallel()

			guard := reclaimConstraintGuard{}
			const scope = "legacy-exclusive/region-a"
			targets := map[string]reclaimConstraintTarget{
				scope: {Desired: 30, Floor: 24},
			}

			_, ceilings := guard.constraint(true, 0, true, maxRampUpStep)
			guard.commit(true, ceilings, targets, 24, maxRampUpStep)
			constraint, ceilings := guard.constraint(true, 24, true, maxRampUpStep)
			require.Equal(t, provisionassembler.ReclaimConstraintReservedFloor, constraint)
			require.Empty(t, ceilings)

			guard.commit(true, ceilings, targets, 24, maxRampUpStep)
			_, ceilings = guard.constraint(true, 24, true, maxRampUpStep)
			require.Empty(t, ceilings)
		})
	}
}

func TestReclaimConstraintGuardTracksScopesIndependently(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	targets := map[string]reclaimConstraintTarget{
		"non-exclusive/0":      {Desired: 38, Floor: 24},
		"exclusive/region-a":   {Desired: 12, Floor: 4},
		"legacy-exclusive/old": {Desired: 9, Floor: 3},
	}
	_, ceilings := guard.constraint(true, 0, true, 5)
	guard.commit(true, ceilings, targets, 31, 5)

	_, ceilings = guard.constraint(true, 31, true, 5)
	require.Equal(t, map[provisionassembler.ReclaimConstraintScope]int{
		"non-exclusive/0":      29,
		"exclusive/region-a":   9,
		"legacy-exclusive/old": 8,
	}, ceilings)
}

func TestReclaimConstraintGuardShrinksCeilingWithDemandAcrossAllScopes(t *testing.T) {
	t.Parallel()

	for _, scope := range []string{
		"non-exclusive/0",
		"exclusive/region-a",
		"legacy-exclusive/region-b",
	} {
		scope := scope
		t.Run(scope, func(t *testing.T) {
			t.Parallel()

			constraintScope := provisionassembler.ReclaimConstraintScope(scope)
			guard := reclaimConstraintGuard{
				hardEnabled:               true,
				ceilings:                  map[provisionassembler.ReclaimConstraintScope]int{constraintScope: 32},
				targets:                   map[string]reclaimConstraintTarget{scope: {Desired: 40, Floor: 4}},
				lastPublishedReclaimTotal: 32,
				hasPublishedReclaimTotal:  true,
			}

			_, ceilings := guard.constraint(true, 32, true, 4)
			guard.commit(true, ceilings, map[string]reclaimConstraintTarget{
				scope: {Desired: 24, Floor: 4},
			}, 24, 4)
			_, ceilings = guard.constraint(true, 0, true, 4)
			require.Equal(t, 24, ceilings[constraintScope],
				"demand reduction must synchronously shrink the historical ceiling")

			guard.commit(true, ceilings, map[string]reclaimConstraintTarget{
				scope: {Desired: 40, Floor: 4},
			}, 24, 4)
			_, ceilings = guard.constraint(true, 24, true, 4)
			require.Equal(t, 28, ceilings[constraintScope],
				"the next ceiling must use the published 24 as its ramp-up baseline")
		})
	}
}

func TestReclaimConstraintGuardNonPositiveStepImmediatelyDropsPriorCeilings(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	targets := map[string]reclaimConstraintTarget{
		"non-exclusive/0": {Desired: 38, Floor: 24},
	}
	_, ceilings := guard.constraint(true, 0, true, 10)
	guard.commit(true, ceilings, targets, 24, 10)

	_, ceilings = guard.constraint(true, 24, true, 0)
	require.Empty(t, ceilings)
}
