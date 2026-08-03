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
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestBuildMaterializationTargetUsesOnlyOwnedTarget(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 2, 2)
	require.NoError(t, err)

	owned := &state.TargetState{
		PodEntries: state.PodEntries{
			commonstate.PoolNameReserve: {
				commonstate.FakedContainerName: {AllocationResult: machine.NewCPUSet(0)},
			},
			commonstate.PoolNameReclaim: {
				commonstate.FakedContainerName: {AllocationResult: machine.NewCPUSet(4, 5)},
			},
			"pod-a": {
				"main": {
					AllocationResult: machine.NewCPUSet(1, 2),
				},
			},
		},
		AllowSharedCoresOverlapReclaimedCores: true,
	}

	target, err := BuildMaterializationTarget(owned, topology, true)
	require.NoError(t, err)
	require.True(t, target.ReserveCPUSet().Equals(machine.NewCPUSet(0)))
	require.True(t, target.ReclaimCPUSet().Equals(machine.NewCPUSet(4, 5)))
	require.True(t, target.NonReclaimCPUSet().Equals(machine.NewCPUSet(1, 2, 3, 6, 7)))
	require.True(t, target.AllowReclaimOverlap())
	require.True(t, target.ContainerCPUSetByPod()["pod-a"]["main"].Equals(machine.NewCPUSet(1, 2)))
	projected := machine.NewCPUSet()
	for numaID, cpus := range target.ReclaimCPUSetByNUMA() {
		require.True(t, cpus.IsSubsetOf(topology.CPUDetails.CPUsInNUMANodes(numaID)))
		projected = projected.Union(cpus)
	}
	require.True(t, projected.Equals(machine.NewCPUSet(4, 5)))

	owned.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].AllocationResult.Add(6)
	owned.PodEntries["pod-a"]["main"].AllocationResult.Add(7)
	require.True(t, target.ReclaimCPUSet().Equals(machine.NewCPUSet(4, 5)))
	require.True(t, target.ContainerCPUSetByPod()["pod-a"]["main"].Equals(machine.NewCPUSet(1, 2)))

	containers := target.ContainerCPUSetByPod()
	containers["pod-a"]["main"].Add(7)
	require.True(t, target.ContainerCPUSetByPod()["pod-a"]["main"].Equals(machine.NewCPUSet(1, 2)))
}

func TestBuildMaterializationTargetRejectsIncompleteInputs(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 2, 2)
	require.NoError(t, err)

	_, err = BuildMaterializationTarget(nil, topology, false)
	require.ErrorContains(t, err, "target")
	_, err = BuildMaterializationTarget(&state.TargetState{}, nil, false)
	require.ErrorContains(t, err, "topology")
}

func TestBuildMaterializationTargetUsesExplicitEffectiveReclaimOverlap(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 2, 2)
	require.NoError(t, err)

	for _, tt := range []struct {
		name             string
		rawOverlap       bool
		dedicatedOverlap bool
		effectiveOverlap bool
	}{
		{
			name:       "explicit off ignores raw on",
			rawOverlap: true,
		},
		{
			name:             "explicit off ignores dedicated policy",
			dedicatedOverlap: true,
		},
		{
			name:             "explicit on ignores raw off and dedicated policy",
			dedicatedOverlap: true,
			effectiveOverlap: true,
		},
		{
			name:             "explicit on preserves raw on",
			rawOverlap:       true,
			effectiveOverlap: true,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			target, err := BuildMaterializationTarget(&state.TargetState{
				PodEntries: state.PodEntries{
					commonstate.PoolNameReclaim: {
						commonstate.FakedContainerName: {AllocationResult: machine.NewCPUSet(4, 5)},
					},
				},
				AllowSharedCoresOverlapReclaimedCores:      tt.rawOverlap,
				DisableDedicatedCoresOverlapReclaimedCores: tt.dedicatedOverlap,
			}, topology, tt.effectiveOverlap)
			require.NoError(t, err)
			require.Equal(t, tt.effectiveOverlap, target.AllowReclaimOverlap())
		})
	}
}

func TestBuildMaterializationTargetIncludesInitializedEmptyEntryForEveryNUMA(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 2, 2)
	require.NoError(t, err)
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceNoSortInt()
	require.Len(t, numaIDs, 2)

	reclaim := topology.CPUDetails.CPUsInNUMANodes(numaIDs[0])
	owned := &state.TargetState{
		PodEntries: state.PodEntries{
			commonstate.PoolNameReclaim: {
				commonstate.FakedContainerName: {AllocationResult: reclaim},
			},
		},
	}

	target, err := BuildMaterializationTarget(owned, topology, false)
	require.NoError(t, err)
	byNUMA := target.ReclaimCPUSetByNUMA()
	require.Len(t, byNUMA, len(numaIDs))
	require.True(t, byNUMA[numaIDs[0]].Equals(reclaim))
	empty, ok := byNUMA[numaIDs[1]]
	require.True(t, ok, "an explicit empty entry is required to clear a NUMA")
	require.True(t, empty.IsEmpty())
}
