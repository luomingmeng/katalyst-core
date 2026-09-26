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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestReclaimConstraintGuardWaitsForPublishedACK(t *testing.T) {
	t.Parallel()

	const scope = provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	targets := map[string]reclaimConstraintTarget{
		string(scope): {Desired: 50, Floor: 24},
	}
	guard := reclaimConstraintGuard{}

	_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	require.Equal(t, 0, ceilings[scope], "the first hard-partition publication must use the floor")
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), 10)

	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	require.Equal(t, 24, ceilings[scope],
		"QRM skipped the first publication, so the ceiling must stay at the floor")

	// A failed round does not call commit. Re-reading the constraint must not
	// mutate or advance guard state.
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	require.Equal(t, 24, ceilings[scope])

	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 10)
	require.Equal(t, 34, ceilings[scope],
		"an observed ACK permits exactly one ramp-up step")
}

func TestReclaimConstraintGuardClampsLargeAndSmallDesiredAfterACK(t *testing.T) {
	t.Parallel()

	const scope = provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	tests := []struct {
		name        string
		desired     int
		wantCeiling int
	}{
		{name: "large desired advances one step", desired: 50, wantCeiling: 34},
		{name: "small desired stops at desired", desired: 28, wantCeiling: 28},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			guard := reclaimConstraintGuard{}
			targets := map[string]reclaimConstraintTarget{
				string(scope): {Desired: tc.desired, Floor: 24},
			}
			_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
			guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), 10)

			_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 10)
			require.Equal(t, tc.wantCeiling, ceilings[scope])
		})
	}
}

func TestReclaimConstraintGuardHoldsUntilLatestPublicationIsObserved(t *testing.T) {
	t.Parallel()

	const scope = provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	targets := map[string]reclaimConstraintTarget{
		string(scope): {Desired: 50, Floor: 24},
	}
	guard := reclaimConstraintGuard{}

	_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), 10)
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 10)
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 34), 10)

	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 10)
	require.Equal(t, 34, ceilings[scope], "a stale ACK must not advance past the latest publication")
	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 34), observedOKScope(scope, true), 10)
	require.Equal(t, 44, ceilings[scope])
}

func TestReclaimConstraintGuardDisableResetsACKState(t *testing.T) {
	t.Parallel()

	const scope = provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	targets := map[string]reclaimConstraintTarget{
		string(scope): {Desired: 50, Floor: 24},
	}
	guard := reclaimConstraintGuard{}

	_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 24), 10)
	// Disabling = no active scopes = full reset.
	guard.commit(nil, nil, nil, nil, 10)

	constraint, ceilings, active := guard.constraint(nil, nil, nil, 10)
	require.Equal(t, provisionassembler.ReclaimConstraintNone, constraint)
	require.Nil(t, ceilings)
	require.Nil(t, active)

	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 24), observedOKScope(scope, true), 10)
	require.Equal(t, 0, ceilings[scope], "re-enabling hard partition must bootstrap from the floor again")
}

func TestReclaimConstraintGuardDoesNotTreatMissingZeroPoolAsACK(t *testing.T) {
	t.Parallel()

	const scope = provisionassembler.ReclaimConstraintScope("non-exclusive/0")
	targets := map[string]reclaimConstraintTarget{
		string(scope): {Desired: 10, Floor: 0},
	}
	guard := reclaimConstraintGuard{}
	_, ceilings, _ := guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, false), 10)
	guard.commit(activeScope(scope), ceilings, targets, publishedScope(scope, 0), 10)

	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, false), 10)
	require.Equal(t, 0, ceilings[scope], "a missing pool is not an ACK even when the last publication was zero")

	_, ceilings, _ = guard.constraint(activeScope(scope), observedScope(scope, 0), observedOKScope(scope, true), 10)
	require.Equal(t, 10, ceilings[scope], "an observed zero-sized pool acknowledges a zero publication")
}

func TestAdvisorObservedReclaimByScope(t *testing.T) {
	t.Parallel()

	metaCache := metacache.NewDummyMetaCacheImp()
	advisor := &cpuResourceAdvisor{metaCache: metaCache}

	scope0 := provisionassembler.NewNonExclusiveReclaimConstraintScope(0)
	scope1 := provisionassembler.NewNonExclusiveReclaimConstraintScope(1)
	scopeNumas := map[provisionassembler.ReclaimConstraintScope][]int{
		scope0: {0},
		scope1: {1},
	}

	// Missing pool: no scope is ACK-eligible.
	observed, observedOK := advisor.observedReclaimByScope(scopeNumas)
	require.Empty(t, observed)
	require.Empty(t, observedOK)

	// Nil pool: same as missing.
	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameReclaim, nil))
	observed, observedOK = advisor.observedReclaimByScope(scopeNumas)
	require.Empty(t, observed)
	require.Empty(t, observedOK)

	// Populated pool: per-NUMA sizes aggregate to the correct scope.
	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameReclaim, &types.PoolInfo{
		TopologyAwareAssignments: map[int]machine.CPUSet{
			0: machine.MustParse("0-2"),
			1: machine.MustParse("8-9"),
		},
	}))
	observed, observedOK = advisor.observedReclaimByScope(scopeNumas)
	require.Equal(t, 3, observed[scope0])
	require.Equal(t, 2, observed[scope1])
	require.True(t, observedOK[scope0])
	require.True(t, observedOK[scope1])
}

func TestAdvisorPublishedReclaimByScope(t *testing.T) {
	t.Parallel()

	advisor := &cpuResourceAdvisor{}
	scope0 := provisionassembler.NewNonExclusiveReclaimConstraintScope(0)
	scope1 := provisionassembler.NewNonExclusiveReclaimConstraintScope(1)
	globalScope := provisionassembler.NewNonExclusiveReclaimConstraintScope(commonstate.FakedNUMAID)
	scopeNumas := map[provisionassembler.ReclaimConstraintScope][]int{
		scope0:      {0},
		scope1:      {1},
		globalScope: {commonstate.FakedNUMAID},
	}

	// Nil result.
	require.Empty(t, advisor.publishedReclaimByScope(nil, scopeNumas))

	// Per-NUMA entries aggregate to scopes; FakedNUMAID maps to global scope.
	result := &types.InternalCPUCalculationResult{
		PoolEntries: map[string]map[int]types.CPUResource{
			commonstate.PoolNameReclaim: {
				0:                       {Size: 7},
				1:                       {Size: 11},
				commonstate.FakedNUMAID: {Size: 5},
			},
		},
	}
	published := advisor.publishedReclaimByScope(result, scopeNumas)
	require.Equal(t, 7, published[scope0])
	require.Equal(t, 11, published[scope1])
	require.Equal(t, 5, published[globalScope])

	// Empty result.
	require.Empty(t, advisor.publishedReclaimByScope(&types.InternalCPUCalculationResult{}, scopeNumas))
}
