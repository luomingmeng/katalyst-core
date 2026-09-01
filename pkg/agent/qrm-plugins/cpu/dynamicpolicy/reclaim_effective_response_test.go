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

package dynamicpolicy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestEffectiveAdvisorResponseForReclaimTargetPreservesRealNUMABlocksFirst(t *testing.T) {
	t.Parallel()

	resp := effectiveResponseRealAndFakeReclaimResponse(3, 5)

	effective, err := effectiveAdvisorResponseForReclaimTarget(
		resp, reclaimEffectiveTargetPolicy{acceptedTotal: 4})
	require.NoError(t, err)
	require.NotSame(t, resp, effective)

	originalInfo := resp.Entries[commonstate.PoolNameReclaim].Entries[commonstate.FakedContainerName]
	require.Equal(t, uint64(3), originalInfo.CalculationResultsByNumas[0].Blocks[0].Result)
	require.Equal(t, uint64(5),
		originalInfo.CalculationResultsByNumas[commonstate.FakedNUMAID].Blocks[0].Result)

	effectiveInfo := effective.Entries[commonstate.PoolNameReclaim].Entries[commonstate.FakedContainerName]
	require.Equal(t, uint64(3), effectiveInfo.CalculationResultsByNumas[0].Blocks[0].Result)
	require.Equal(t, uint64(1),
		effectiveInfo.CalculationResultsByNumas[commonstate.FakedNUMAID].Blocks[0].Result)
}

func TestEffectiveAdvisorResponseForReclaimTargetRejectsNonReclaimAliasBelowFloor(t *testing.T) {
	t.Parallel()

	resp := &advisorapi.ListAndWatchResponse{
		Entries: map[string]*advisorapi.CalculationEntries{
			commonstate.PoolNameReclaim: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameReclaim,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							0: {
								Blocks: []*advisorapi.Block{{BlockId: "reclaim", Result: 4}},
							},
						},
					},
				},
			},
			commonstate.PoolNameShare: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameShare,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							0: {
								Blocks: []*advisorapi.Block{{BlockId: "reclaim", Result: 4}},
							},
						},
					},
				},
			},
		},
	}

	_, err := effectiveAdvisorResponseForReclaimTarget(
		resp, reclaimEffectiveTargetPolicy{acceptedTotal: 2})
	require.ErrorContains(t, err, "below non-reclaim alias floor 4")
}

func TestEffectiveAdvisorResponseForReclaimTargetHonorsHardNUMAFloors(t *testing.T) {
	t.Parallel()

	resp := &advisorapi.ListAndWatchResponse{
		Entries: map[string]*advisorapi.CalculationEntries{
			commonstate.PoolNameReclaim: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameReclaim,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							0: {Blocks: []*advisorapi.Block{{BlockId: "reclaim-0", Result: 4}}},
							1: {Blocks: []*advisorapi.Block{{BlockId: "reclaim-1", Result: 4}}},
						},
					},
				},
			},
		},
	}

	effective, err := effectiveAdvisorResponseForReclaimTarget(resp, reclaimEffectiveTargetPolicy{
		acceptedTotal: 4,
		fixedReserveByNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(0, 4),
			1: machine.NewCPUSet(2, 6),
		},
	})
	require.NoError(t, err)

	effectiveInfo := effective.Entries[commonstate.PoolNameReclaim].Entries[commonstate.FakedContainerName]
	require.Equal(t, uint64(2), effectiveInfo.CalculationResultsByNumas[0].Blocks[0].Result)
	require.Equal(t, uint64(2), effectiveInfo.CalculationResultsByNumas[1].Blocks[0].Result)
}

func effectiveResponseRealAndFakeReclaimResponse(
	realQuantity uint64,
	fakeQuantity uint64,
) *advisorapi.ListAndWatchResponse {
	return &advisorapi.ListAndWatchResponse{
		Entries: map[string]*advisorapi.CalculationEntries{
			commonstate.PoolNameReclaim: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameReclaim,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							0: {
								Blocks: []*advisorapi.Block{{
									BlockId: "real-reclaim",
									Result:  realQuantity,
								}},
							},
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{
									BlockId: "fake-reclaim",
									Result:  fakeQuantity,
								}},
							},
						},
					},
				},
			},
		},
	}
}

func TestEffectiveAdvisorResponseForReclaimTargetPreservesAliasWhileTrimmingOtherNUMA(t *testing.T) {
	t.Parallel()

	resp := &advisorapi.ListAndWatchResponse{
		Entries: map[string]*advisorapi.CalculationEntries{
			commonstate.PoolNameReclaim: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameReclaim,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							0: {Blocks: []*advisorapi.Block{{BlockId: "alias", Result: 4}}},
							1: {Blocks: []*advisorapi.Block{{BlockId: "reclaim-1", Result: 4}}},
						},
					},
				},
			},
			commonstate.PoolNameDedicated: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameDedicated,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							0: {Blocks: []*advisorapi.Block{{BlockId: "alias", Result: 4}}},
						},
					},
				},
			},
		},
	}

	effective, err := effectiveAdvisorResponseForReclaimTarget(resp, reclaimEffectiveTargetPolicy{
		acceptedTotal: 6,
	})
	require.NoError(t, err)

	effectiveInfo := effective.Entries[commonstate.PoolNameReclaim].Entries[commonstate.FakedContainerName]
	require.Equal(t, uint64(4), effectiveInfo.CalculationResultsByNumas[0].Blocks[0].Result)
	require.Equal(t, uint64(2), effectiveInfo.CalculationResultsByNumas[1].Blocks[0].Result)
}

func TestEffectiveAdvisorResponseForReclaimTargetIgnoresDefaultShareBlockIDCollision(t *testing.T) {
	t.Parallel()

	resp := &advisorapi.ListAndWatchResponse{
		Entries: map[string]*advisorapi.CalculationEntries{
			commonstate.PoolNameReclaim: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameReclaim,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{BlockId: "shared-id", Result: 4}},
							},
						},
					},
				},
			},
			commonstate.PoolNameShare: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameShare,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{BlockId: "shared-id", Result: 6}},
							},
						},
					},
				},
			},
		},
	}

	effective, err := effectiveAdvisorResponseForReclaimTarget(
		resp, reclaimEffectiveTargetPolicy{
			acceptedTotal:      2,
			ignoreDefaultShare: true,
		})
	require.NoError(t, err)
	require.Equal(t, uint64(2), effective.Entries[commonstate.PoolNameReclaim].
		Entries[commonstate.FakedContainerName].
		CalculationResultsByNumas[commonstate.FakedNUMAID].Blocks[0].Result)
	require.Equal(t, uint64(6), effective.Entries[commonstate.PoolNameShare].
		Entries[commonstate.FakedContainerName].
		CalculationResultsByNumas[commonstate.FakedNUMAID].Blocks[0].Result)
	require.Equal(t, uint64(6), resp.Entries[commonstate.PoolNameShare].
		Entries[commonstate.FakedContainerName].
		CalculationResultsByNumas[commonstate.FakedNUMAID].Blocks[0].Result)
}
