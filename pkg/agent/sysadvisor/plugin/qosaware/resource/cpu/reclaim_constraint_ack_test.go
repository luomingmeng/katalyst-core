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

func TestReclaimConstraintGuardWaitsForPublishedTotalACK(t *testing.T) {
	t.Parallel()

	const scope = "non-exclusive/0"
	targets := map[string]reclaimConstraintTarget{
		scope: {Desired: 50, Floor: 24},
	}
	guard := reclaimConstraintGuard{}

	_, ceilings := guard.constraint(true, 0, true, 10)
	require.Empty(t, ceilings, "the first hard-partition publication must use the floor")
	guard.commit(true, ceilings, targets, 24, 10)

	_, ceilings = guard.constraint(true, 0, true, 10)
	require.Equal(t, map[provisionassembler.ReclaimConstraintScope]int{scope: 24}, ceilings,
		"QRM skipped the first publication, so the ceiling must stay at the floor")

	// A failed round does not call commit. Re-reading the constraint must not
	// mutate or advance guard state.
	_, ceilings = guard.constraint(true, 0, true, 10)
	require.Equal(t, map[provisionassembler.ReclaimConstraintScope]int{scope: 24}, ceilings)

	_, ceilings = guard.constraint(true, 24, true, 10)
	require.Equal(t, map[provisionassembler.ReclaimConstraintScope]int{scope: 34}, ceilings,
		"an observed ACK permits exactly one ramp-up step")
}

func TestReclaimConstraintGuardClampsLargeAndSmallDesiredAfterACK(t *testing.T) {
	t.Parallel()

	const scope = "non-exclusive/0"
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
				scope: {Desired: tc.desired, Floor: 24},
			}
			_, ceilings := guard.constraint(true, 0, true, 10)
			guard.commit(true, ceilings, targets, 24, 10)

			_, ceilings = guard.constraint(true, 24, true, 10)
			require.Equal(t, tc.wantCeiling, ceilings[scope])
		})
	}
}

func TestReclaimConstraintGuardHoldsUntilLatestPublicationIsObserved(t *testing.T) {
	t.Parallel()

	const scope = "non-exclusive/0"
	targets := map[string]reclaimConstraintTarget{
		scope: {Desired: 50, Floor: 24},
	}
	guard := reclaimConstraintGuard{}

	_, ceilings := guard.constraint(true, 0, true, 10)
	guard.commit(true, ceilings, targets, 24, 10)
	_, ceilings = guard.constraint(true, 24, true, 10)
	guard.commit(true, ceilings, targets, 34, 10)

	_, ceilings = guard.constraint(true, 24, true, 10)
	require.Equal(t, 34, ceilings[scope], "a stale ACK must not advance past the latest publication")
	_, ceilings = guard.constraint(true, 34, true, 10)
	require.Equal(t, 44, ceilings[scope])
}

func TestReclaimConstraintGuardDisableResetsACKState(t *testing.T) {
	t.Parallel()

	const scope = "non-exclusive/0"
	targets := map[string]reclaimConstraintTarget{
		scope: {Desired: 50, Floor: 24},
	}
	guard := reclaimConstraintGuard{}

	_, ceilings := guard.constraint(true, 0, true, 10)
	guard.commit(true, ceilings, targets, 24, 10)
	guard.commit(false, nil, nil, 0, 10)

	constraint, ceilings := guard.constraint(false, 24, true, 10)
	require.Equal(t, provisionassembler.ReclaimConstraintNone, constraint)
	require.Nil(t, ceilings)

	_, ceilings = guard.constraint(true, 24, true, 10)
	require.Empty(t, ceilings, "re-enabling hard partition must bootstrap from the floor again")
}

func TestAdvisorReclaimTotalsUseObservedTopologyAndPublishedPoolEntries(t *testing.T) {
	t.Parallel()

	metaCache := metacache.NewDummyMetaCacheImp()
	advisor := &cpuResourceAdvisor{metaCache: metaCache}
	total, observed := advisor.observedReclaimTotal()
	require.Zero(t, total)
	require.False(t, observed, "a missing reclaim pool must not acknowledge a publication")

	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameReclaim, nil))
	total, observed = advisor.observedReclaimTotal()
	require.Zero(t, total)
	require.False(t, observed, "a nil reclaim pool must not acknowledge a publication")

	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameReclaim, &types.PoolInfo{
		TopologyAwareAssignments: map[int]machine.CPUSet{
			0: machine.MustParse("0-2"),
			1: machine.MustParse("8-9"),
		},
	}))
	total, observed = advisor.observedReclaimTotal()
	require.Equal(t, 5, total)
	require.True(t, observed)

	result := &types.InternalCPUCalculationResult{
		PoolEntries: map[string]map[int]types.CPUResource{
			commonstate.PoolNameReclaim: {
				0: {Size: 7},
				1: {Size: 11},
			},
		},
	}
	require.Equal(t, 18, publishedReclaimTotal(result))
	require.Zero(t, publishedReclaimTotal(nil))
	require.Zero(t, publishedReclaimTotal(&types.InternalCPUCalculationResult{}))
}

func TestReclaimConstraintGuardDoesNotTreatMissingZeroPoolAsACK(t *testing.T) {
	t.Parallel()

	const scope = "non-exclusive/0"
	targets := map[string]reclaimConstraintTarget{
		scope: {Desired: 10, Floor: 0},
	}
	guard := reclaimConstraintGuard{}
	_, ceilings := guard.constraint(true, 0, false, 10)
	guard.commit(true, ceilings, targets, 0, 10)

	_, ceilings = guard.constraint(true, 0, false, 10)
	require.Equal(t, 0, ceilings[scope], "a missing pool is not an ACK even when the last publication was zero")

	_, ceilings = guard.constraint(true, 0, true, 10)
	require.Equal(t, 10, ceilings[scope], "an observed zero-sized pool acknowledges a zero publication")
}
