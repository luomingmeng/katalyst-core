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

package dynamicpolicy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestAllocateByCPUAdvisorClampsLegacyReclaimToBulkheadFloor(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		reclaimSize      uint64
		minSize          int64
		defaultMinSize   int64
		disableBulkhead  bool
		canonicalFloor   machine.CPUSet
		fillDefaultShare bool
		wantReclaimSize  int
		wantShareSize    int
	}{
		{
			name:            "minimum clamps legacy reclaim",
			reclaimSize:     4,
			minSize:         4,
			wantReclaimSize: 2,
		},
		{
			name:            "minimum supports a non-core-multiple legacy ceiling",
			reclaimSize:     4,
			minSize:         3,
			wantReclaimSize: 3,
		},
		{
			name:            "request within ceiling is unchanged",
			reclaimSize:     4,
			minSize:         2,
			wantReclaimSize: 4,
		},
		{
			name:            "zero minimum falls back to startup default",
			reclaimSize:     4,
			defaultMinSize:  4,
			wantReclaimSize: 2,
		},
		{
			name:            "disabled bulkhead leaves request unchanged",
			reclaimSize:     4,
			minSize:         4,
			disableBulkhead: true,
			wantReclaimSize: 4,
		},
		{
			name:            "ceiling below canonical floor degrades to floor",
			reclaimSize:     4,
			minSize:         6,
			canonicalFloor:  machine.NewCPUSet(1, 5),
			wantReclaimSize: 2,
		},
		{
			name:            "target equal to canonical floor preserves floor identity",
			reclaimSize:     2,
			minSize:         6,
			canonicalFloor:  machine.NewCPUSet(1, 5),
			wantReclaimSize: 2,
		},
		{
			name:             "clamp materializes default share from residual",
			reclaimSize:      4,
			minSize:          4,
			fillDefaultShare: true,
			wantReclaimSize:  2,
			wantShareSize:    4,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(8, 1, 2)
			require.NoError(t, err)
			policy, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
			require.NoError(t, err)
			policy.advisorPostCommitCheckpointDir = t.TempDir()

			if tc.defaultMinSize > 0 {
				defaultDynamicConf := policy.conf.DynamicAgentConfiguration.GetDynamicConfiguration()
				defaultDynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.
					BulkheadConfig.NonReclaimPoolMinSize = tc.defaultMinSize
				policy.bulkheadManager, err = bulkhead.NewManager(policy.conf)
				require.NoError(t, err)
			}
			dynamicConf := policy.dynamicConfig.GetDynamicConfiguration()
			dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable = !tc.disableBulkhead
			dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize = tc.minSize
			dynamicConf.FillDefaultSharePoolWithNonReclaimCPUs = tc.fillDefaultShare
			if !tc.canonicalFloor.IsEmpty() {
				policy.reservedReclaimedCPUSet = tc.canonicalFloor.Clone()
				policy.reservedReclaimedTopologyAwareAssignments = map[int]machine.CPUSet{
					0: tc.canonicalFloor.Intersection(topology.CPUDetails.CPUsInNUMANodes(0)),
					1: tc.canonicalFloor.Intersection(topology.CPUDetails.CPUsInNUMANodes(1)),
				}
			}
			policy.state.SetPodEntries(state.PodEntries{
				commonstate.PoolNameReserve: {
					commonstate.FakedContainerName: &state.AllocationInfo{
						AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
						AllocationResult: machine.NewCPUSet(0, 4),
					},
				},
				commonstate.PoolNameReclaim: {
					commonstate.FakedContainerName: &state.AllocationInfo{
						AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
						AllocationResult: machine.NewCPUSet(3, 7),
					},
				},
			}, false)

			revision := policy.state.GetRevision()

			resp := advisorBulkheadResponse(tc.reclaimSize)
			if tc.fillDefaultShare {
				resp.Entries[commonstate.PoolNameShare] = &advisorapi.CalculationEntries{
					Entries: map[string]*advisorapi.CalculationInfo{
						commonstate.FakedContainerName: {
							OwnerPoolName: commonstate.PoolNameShare,
							CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
								commonstate.FakedNUMAID: {
									Blocks: []*advisorapi.Block{{BlockId: "share", Result: 6}},
								},
							},
						},
					},
				}
			}
			err = policy.allocateByCPUAdvisor(nil, resp, nil)
			require.Equal(t, tc.reclaimSize,
				resp.Entries[commonstate.PoolNameReclaim].Entries[commonstate.FakedContainerName].
					CalculationResultsByNumas[commonstate.FakedNUMAID].Blocks[0].Result)
			require.NoError(t, err)
			require.Greater(t, policy.state.GetRevision(), revision)
			reclaim := policy.state.GetPodEntries()[commonstate.PoolNameReclaim][commonstate.FakedContainerName].
				AllocationResult
			require.Equal(t, tc.wantReclaimSize, reclaim.Size())
			if !tc.canonicalFloor.IsEmpty() {
				require.True(t, tc.canonicalFloor.IsSubsetOf(reclaim))
			}
			if tc.wantShareSize > 0 {
				share := policy.state.GetPodEntries()[commonstate.PoolNameShare][commonstate.FakedContainerName]
				require.NotNil(t, share)
				require.Equal(t, tc.wantShareSize, share.AllocationResult.Size())
			}
		})
	}
}

func TestEffectiveAdvisorResponseForBulkheadRejectsAdvisorTargetBelowCanonicalFloor(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(8, 1, 2)
	require.NoError(t, err)
	policy, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)

	dynamicConf := policy.dynamicConfig.GetDynamicConfiguration()
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable = true
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize = 6
	policy.reservedReclaimedCPUSet = machine.NewCPUSet(3, 7)
	policy.reservedReclaimedTopologyAwareAssignments = map[int]machine.CPUSet{
		0: machine.NewCPUSet(3),
		1: machine.NewCPUSet(7),
	}

	_, _, err = policy.effectiveAdvisorResponseForBulkhead(advisorBulkheadResponse(1), false)
	require.ErrorContains(t, err, "advisor reclaim target 1 is below required floor 2")
}

func advisorBulkheadResponse(reclaimSize uint64) *advisorapi.ListAndWatchResponse {
	return &advisorapi.ListAndWatchResponse{
		Entries: map[string]*advisorapi.CalculationEntries{
			commonstate.PoolNameReserve: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameReserve,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{BlockId: "reserve", Result: 2}},
							},
						},
					},
				},
			},
			commonstate.PoolNameReclaim: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameReclaim,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{BlockId: "reclaim", Result: reclaimSize}},
							},
						},
					},
				},
			},
		},
	}
}
