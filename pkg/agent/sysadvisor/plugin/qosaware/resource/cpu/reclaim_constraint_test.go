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

func activeScope(s provisionassembler.ReclaimConstraintScope) map[provisionassembler.ReclaimConstraintScope]bool {
	return map[provisionassembler.ReclaimConstraintScope]bool{s: true}
}

func observedScope(s provisionassembler.ReclaimConstraintScope, v int) map[provisionassembler.ReclaimConstraintScope]int {
	return map[provisionassembler.ReclaimConstraintScope]int{s: v}
}

func observedOKScope(s provisionassembler.ReclaimConstraintScope, ok bool) map[provisionassembler.ReclaimConstraintScope]bool {
	return map[provisionassembler.ReclaimConstraintScope]bool{s: ok}
}

func publishedScope(s provisionassembler.ReclaimConstraintScope, v int) map[provisionassembler.ReclaimConstraintScope]int {
	return map[provisionassembler.ReclaimConstraintScope]int{s: v}
}

func TestReclaimConstraintGuardAdvancesPerScopeCeiling(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	const scope = provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	targets := map[string]reclaimConstraintTarget{
		string(scope): {Desired: 38, Floor: 24},
	}

	constraint, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	require.Equal(t, provisionassembler.ReclaimConstraintReservedFloor, constraint)
	require.Equal(t, 0, ceilings[scope])

	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), 10)
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 10)
	require.Equal(t, map[provisionassembler.ReclaimConstraintScope]int{scope: 34}, ceilings)

	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 34), 10)
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 34), observedOKScope(scope, true), 10)
	require.Equal(t, map[provisionassembler.ReclaimConstraintScope]int{scope: 38}, ceilings)
}

func TestReclaimConstraintGuardPersistentOrGrowingDemandNeverJumps(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	const scope = provisionassembler.ReclaimConstraintScope("exclusive/region-a")
	targets := map[string]reclaimConstraintTarget{
		string(scope): {Desired: 30, Floor: 24},
	}
	_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 4)
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), 4)
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 4)
	require.Equal(t, 28, ceilings[scope])

	guard.commit(activeScope(scope), ceilings, map[string]reclaimConstraintTarget{
		string(scope): {Desired: 50, Floor: 24},
	}, publishedScope(scope, 28), 4)
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 28), observedOKScope(scope, true), 4)
	require.Equal(t, 32, ceilings[scope])
}

func TestReclaimConstraintGuardDoesNotCommitFailedRound(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	const scope = provisionassembler.ReclaimConstraintScope("non-exclusive/0")

	_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	require.Equal(t, 0, ceilings[scope])
	// The caller does not invoke commit when assembly fails.
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	require.Equal(t, 0, ceilings[scope])

	targets := map[string]reclaimConstraintTarget{
		string(scope): {Desired: 38, Floor: 24},
	}
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), 10)
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 10)
	require.Equal(t, 34, ceilings[scope])
	// A failed follow-up round must not advance the ceiling.
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 10)
	require.Equal(t, 34, ceilings[scope])
}

func TestReclaimConstraintGuardFailsClosedForNonPositiveRampUpStep(t *testing.T) {
	t.Parallel()

	for _, maxRampUpStep := range []int{0, -1} {
		maxRampUpStep := maxRampUpStep
		t.Run(fmt.Sprintf("max-ramp-up-step-%d", maxRampUpStep), func(t *testing.T) {
			t.Parallel()

			guard := reclaimConstraintGuard{}
			const scope = provisionassembler.ReclaimConstraintScope("legacy-exclusive/region-a")
			targets := map[string]reclaimConstraintTarget{
				string(scope): {Desired: 30, Floor: 24},
			}

			_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), maxRampUpStep)
			guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), maxRampUpStep)
			constraint, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), maxRampUpStep)
			require.Equal(t, provisionassembler.ReclaimConstraintReservedFloor, constraint)
			require.Empty(t, ceilings)

			guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), maxRampUpStep)
			_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), maxRampUpStep)
			require.Empty(t, ceilings)
		})
	}
}

func TestReclaimConstraintGuardTracksScopesIndependently(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	scopeA := provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	scopeB := provisionassembler.ReclaimConstraintScope("exclusive/region-a")
	scopeC := provisionassembler.ReclaimConstraintScope("legacy-exclusive/old")
	activeScopes := map[provisionassembler.ReclaimConstraintScope]bool{scopeA: true, scopeB: true, scopeC: true}
	targets := map[string]reclaimConstraintTarget{
		string(scopeA): {Desired: 38, Floor: 24},
		string(scopeB): {Desired: 12, Floor: 4},
		string(scopeC): {Desired: 9, Floor: 3},
	}
	observed := map[provisionassembler.ReclaimConstraintScope]int{scopeA: 0, scopeB: 0, scopeC: 0}
	observedOK := map[provisionassembler.ReclaimConstraintScope]bool{scopeA: true, scopeB: true, scopeC: true}
	published := map[provisionassembler.ReclaimConstraintScope]int{scopeA: 24, scopeB: 4, scopeC: 3}

	_, ceilings, _ := guard.constraint(activeScopes, observed, observedOK, 5)
	guard.commit(activeScopes, ceilings, targets, published, 5)

	observed = map[provisionassembler.ReclaimConstraintScope]int{scopeA: 24, scopeB: 4, scopeC: 3}
	_, ceilings, _ = guard.constraint(activeScopes, observed, observedOK, 5)
	require.Equal(t, map[provisionassembler.ReclaimConstraintScope]int{
		scopeA: 29,
		scopeB: 9,
		scopeC: 8,
	}, ceilings)
}

func TestReclaimConstraintGuardOneUnACKedScopeDoesNotBlockAnother(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	scopeA := provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	scopeB := provisionassembler.ReclaimConstraintScope("non-exclusive/1")
	activeScopes := map[provisionassembler.ReclaimConstraintScope]bool{scopeA: true, scopeB: true}
	targets := map[string]reclaimConstraintTarget{
		string(scopeA): {Desired: 38, Floor: 24},
		string(scopeB): {Desired: 38, Floor: 24},
	}

	// Cycle 1: both publish floor.
	observed := map[provisionassembler.ReclaimConstraintScope]int{scopeA: 0, scopeB: 0}
	observedOK := map[provisionassembler.ReclaimConstraintScope]bool{scopeA: true, scopeB: true}
	_, ceilings, _ := guard.constraint(activeScopes, observed, observedOK, 10)
	guard.commit(activeScopes, ceilings, targets,
		map[provisionassembler.ReclaimConstraintScope]int{scopeA: 24, scopeB: 24}, 10)

	// Cycle 2: scopeA ACKs (observed=24), scopeB does not (observed=0, stale).
	observed = map[provisionassembler.ReclaimConstraintScope]int{scopeA: 24, scopeB: 0}
	_, ceilings, _ = guard.constraint(activeScopes, observed, observedOK, 10)
	require.Equal(t, 34, ceilings[scopeA], "ACKed scope must advance")
	require.Equal(t, 24, ceilings[scopeB], "un-ACKed scope must hold at its published ceiling")
}

func TestReclaimConstraintGuardInactiveScopePassthrough(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	scopeA := provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	scopeB := provisionassembler.ReclaimConstraintScope("exclusive/region-a")

	// Only scopeA is active; scopeB must not appear in ceilings.
	_, ceilings, active := guard.constraint(
		activeScope(scopeA),
		observedScope(scopeA, 0),
		observedOKScope(scopeA, true),
		10,
	)
	require.Equal(t, provisionassembler.ReclaimConstraintReservedFloor, constraintOrNone(ceilings))
	require.Contains(t, ceilings, scopeA)
	require.NotContains(t, ceilings, scopeB)
	require.True(t, active[scopeA])
	require.False(t, active[scopeB])
}

func constraintOrNone(ceilings map[provisionassembler.ReclaimConstraintScope]int) provisionassembler.ReclaimConstraint {
	if len(ceilings) == 0 {
		return provisionassembler.ReclaimConstraintNone
	}
	return provisionassembler.ReclaimConstraintReservedFloor
}

func TestReclaimConstraintGuardRampUpExitPreservesCeilingResetsACK(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	const scope = provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	targets := map[string]reclaimConstraintTarget{
		string(scope): {Desired: 38, Floor: 24},
	}

	// Advance to ceiling 34.
	_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), 10)
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 10)
	require.Equal(t, 34, ceilings[scope])
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 34), 10)

	// RampUp exits: no active scopes clears the guard entirely.
	guard.commit(nil, nil, nil, nil, 10)
	require.Empty(t, guard.scopeState)
	require.Empty(t, guard.targets)

	// Re-activation bootstraps from floor (no stale ACK).
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 34), observedOKScope(scope, true), 10)
	require.Equal(t, 0, ceilings[scope], "after full deactivation the ceiling must reset")
}

func TestReclaimConstraintGuardScopeRemovalNoLeak(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	scopeA := provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	scopeB := provisionassembler.ReclaimConstraintScope("exclusive/region-a")
	targets := map[string]reclaimConstraintTarget{
		string(scopeA): {Desired: 38, Floor: 24},
		string(scopeB): {Desired: 12, Floor: 4},
	}

	// Both active, both advance.
	bothActive := map[provisionassembler.ReclaimConstraintScope]bool{scopeA: true, scopeB: true}
	observed := map[provisionassembler.ReclaimConstraintScope]int{scopeA: 0, scopeB: 0}
	observedOK := map[provisionassembler.ReclaimConstraintScope]bool{scopeA: true, scopeB: true}
	_, ceilings, _ := guard.constraint(bothActive, observed, observedOK, 10)
	guard.commit(bothActive, ceilings, targets,
		map[provisionassembler.ReclaimConstraintScope]int{scopeA: 24, scopeB: 4}, 10)
	observed = map[provisionassembler.ReclaimConstraintScope]int{scopeA: 24, scopeB: 4}
	_, ceilings, _ = guard.constraint(bothActive, observed, observedOK, 10)
	guard.commit(bothActive, ceilings, targets,
		map[provisionassembler.ReclaimConstraintScope]int{scopeA: 34, scopeB: 12}, 10)

	// scopeB removed: only scopeA active. scopeB state preserved but de-ACKed.
	_, ceilings, active := guard.constraint(activeScope(scopeA),
		observedScope(scopeA, 34), observedOKScope(scopeA, true), 10)
	require.True(t, active[scopeA])
	require.False(t, active[scopeB])
	require.NotContains(t, ceilings, scopeB)
	guard.commit(activeScope(scopeA), ceilings, targets, publishedScope(scopeA, 38), 10)

	// scopeB state retained (ceiling=12) but hasPublished=false.
	stateB, ok := guard.scopeState[scopeB]
	require.True(t, ok)
	require.Equal(t, 12, stateB.ceiling)
	require.False(t, stateB.hasPublished)
}

func TestReclaimConstraintGuardShrinksCeilingWithDemandAcrossAllScopes(t *testing.T) {
	t.Parallel()

	for _, scopeStr := range []string{
		"non-exclusive/0",
		"exclusive/region-a",
		"legacy-exclusive/region-b",
	} {
		scopeStr := scopeStr
		t.Run(scopeStr, func(t *testing.T) {
			t.Parallel()

			scope := provisionassembler.ReclaimConstraintScope(scopeStr)
			guard := reclaimConstraintGuard{
				scopeState: map[provisionassembler.ReclaimConstraintScope]reclaimConstraintScopeState{
					scope: {lastPublished: 32, hasPublished: true, ceiling: 32},
				},
				targets: map[string]reclaimConstraintTarget{scopeStr: {Desired: 40, Floor: 4}},
			}

			_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 32), observedOKScope(scope, true), 4)
			guard.commit(activeScope(scope), ceilings, map[string]reclaimConstraintTarget{
				scopeStr: {Desired: 24, Floor: 4},
			}, publishedScope(scope, 24), 4)
			_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 4)
			require.Equal(t, 24, ceilings[scope],
				"demand reduction must synchronously shrink the historical ceiling")

			guard.commit(activeScope(scope), ceilings, map[string]reclaimConstraintTarget{
				scopeStr: {Desired: 40, Floor: 4},
			}, publishedScope(scope, 24), 4)
			_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 4)
			require.Equal(t, 28, ceilings[scope],
				"the next ceiling must use the published 24 as its ramp-up baseline")
		})
	}
}

func TestReclaimConstraintGuardNonPositiveStepImmediatelyDropsPriorCeilings(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	const scope = provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	targets := map[string]reclaimConstraintTarget{
		string(scope): {Desired: 38, Floor: 24},
	}
	_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), 10)

	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 0)
	require.Empty(t, ceilings)
}

func TestReclaimConstraintGuardNoActiveScopesReturnsNone(t *testing.T) {
	t.Parallel()

	guard := reclaimConstraintGuard{}
	constraint, ceilings, active := guard.constraint(nil, nil, nil, 10)
	require.Equal(t, provisionassembler.ReclaimConstraintNone, constraint)
	require.Nil(t, ceilings)
	require.Nil(t, active)
}
