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

package planner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestValidateTargetRejectsHardFloorDrop(t *testing.T) {
	t.Parallel()

	topology, target := validTarget(t)
	err := ValidateTarget(target, topology, machine.NewCPUSet(0, 2), true)
	require.ErrorIs(t, err, ErrHardFloorDropped)
}

func TestValidateTargetRejectsReclaimShareOverlap(t *testing.T) {
	t.Parallel()

	topology, target := validTarget(t)
	share := target.PodEntries[commonstate.PoolNameShare][commonstate.FakedContainerName]
	share.AllocationResult = share.AllocationResult.Union(machine.NewCPUSet(0))
	share.OriginalAllocationResult = share.AllocationResult.Clone()
	share.TopologyAwareAssignments = assignmentsFor(topology, share.AllocationResult)
	share.OriginalTopologyAwareAssignments = assignmentsFor(topology, share.AllocationResult)

	err := ValidateTarget(target, topology, machine.NewCPUSet(), true)
	require.ErrorIs(t, err, ErrReclaimOverlapsShare)
}

func TestValidateTargetRejectsReclaimOverlapWithEveryShareTypePool(t *testing.T) {
	t.Parallel()

	for _, poolName := range []string{
		commonstate.GetNUMAPoolName(commonstate.PoolNameShare, 0),
		"custom-share-pool",
	} {
		poolName := poolName
		t.Run(poolName, func(t *testing.T) {
			t.Parallel()

			topology, target := validTarget(t)
			delete(target.PodEntries, commonstate.PoolNameShare)
			target.PodEntries[poolName] = state.ContainerEntries{
				commonstate.FakedContainerName: {
					AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(poolName),
					AllocationResult:                 machine.NewCPUSet(0, 1),
					OriginalAllocationResult:         machine.NewCPUSet(0, 1),
					TopologyAwareAssignments:         assignmentsFor(topology, machine.NewCPUSet(0, 1)),
					OriginalTopologyAwareAssignments: assignmentsFor(topology, machine.NewCPUSet(0, 1)),
				},
			}
			target.MachineState, _ = state.GenerateMachineStateFromPodEntries(topology, target.PodEntries, nil)

			err := ValidateTarget(target, topology, machine.NewCPUSet(0), true)
			require.ErrorIs(t, err, ErrReclaimOverlapsShare)
		})
	}
}

func TestValidateTargetRejectsRampUpContainerOverlap(t *testing.T) {
	t.Parallel()

	topology, target := validTarget(t)
	target.PodEntries["ramp-pod"] = state.ContainerEntries{
		"main": {
			RampUp:                           true,
			AllocationResult:                 machine.NewCPUSet(0, 2),
			OriginalAllocationResult:         machine.NewCPUSet(0, 2),
			TopologyAwareAssignments:         assignmentsFor(topology, machine.NewCPUSet(0, 2)),
			OriginalTopologyAwareAssignments: assignmentsFor(topology, machine.NewCPUSet(0, 2)),
		},
	}
	target.MachineState, _ = state.GenerateMachineStateFromPodEntries(topology, target.PodEntries, nil)

	err := ValidateTarget(target, topology, machine.NewCPUSet(), true)
	require.ErrorIs(t, err, ErrReclaimOverlapsRampUp)
}

func TestValidateTargetAllowsLegacyOverlapWhenDisjointnessNotRequired(t *testing.T) {
	t.Parallel()

	topology, target := validTarget(t)
	share := target.PodEntries[commonstate.PoolNameShare][commonstate.FakedContainerName]
	share.AllocationResult = share.AllocationResult.Union(machine.NewCPUSet(0))
	share.OriginalAllocationResult = share.AllocationResult.Clone()
	share.TopologyAwareAssignments = assignmentsFor(topology, share.AllocationResult)
	share.OriginalTopologyAwareAssignments = assignmentsFor(topology, share.AllocationResult)
	target.PodEntries["ramp-pod"] = state.ContainerEntries{
		"main": {
			RampUp:                           true,
			AllocationResult:                 machine.NewCPUSet(0, 2),
			OriginalAllocationResult:         machine.NewCPUSet(0, 2),
			TopologyAwareAssignments:         assignmentsFor(topology, machine.NewCPUSet(0, 2)),
			OriginalTopologyAwareAssignments: assignmentsFor(topology, machine.NewCPUSet(0, 2)),
		},
	}
	target.MachineState, _ = state.GenerateMachineStateFromPodEntries(topology, target.PodEntries, nil)

	require.NoError(t, ValidateTarget(target, topology, machine.NewCPUSet(0), false))
}

func TestValidateTargetRejectsTopologyProjectionMismatch(t *testing.T) {
	t.Parallel()

	topology, target := validTarget(t)
	reclaim := target.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName]
	reclaim.TopologyAwareAssignments = map[int]machine.CPUSet{}

	err := ValidateTarget(target, topology, machine.NewCPUSet(0), true)
	require.ErrorIs(t, err, ErrTopologyProjectionMismatch)
}

func TestValidateTargetRejectsMachineStateMismatch(t *testing.T) {
	t.Parallel()

	topology, target := validTarget(t)
	for _, numaState := range target.MachineState {
		numaState.DefaultCPUSet = machine.NewCPUSet()
		break
	}

	err := ValidateTarget(target, topology, machine.NewCPUSet(0), true)
	require.ErrorIs(t, err, ErrMachineStateMismatch)
}

func TestValidateTargetDoesNotMutateMachineStatePreOccPodEntries(t *testing.T) {
	t.Parallel()

	topology, target := validTarget(t)
	target.MachineState[0].PreOccPodEntries = state.PodEntries{
		"pre-occ-pod": {
			"main": {
				AllocationMeta: commonstate.AllocationMeta{
					Annotations: map[string]string{"sentinel": "unchanged"},
				},
				AllocationResult:         machine.NewCPUSet(0),
				OriginalAllocationResult: machine.NewCPUSet(0),
			},
		},
	}
	before := target.MachineState.Clone()

	_ = ValidateTarget(target, topology, machine.NewCPUSet(0), true)
	require.Equal(t, before, target.MachineState)
	require.Equal(t,
		map[string]string{"sentinel": "unchanged"},
		target.MachineState[0].PreOccPodEntries["pre-occ-pod"]["main"].Annotations,
	)
}

func validTarget(t *testing.T) (*machine.CPUTopology, *state.TargetState) {
	t.Helper()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	target := targetWithReclaimAndShare(
		machine.NewCPUSet(0),
		machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7),
		topology,
	)
	require.NoError(t, ValidateTarget(target, topology, machine.NewCPUSet(0), true))
	return topology, target
}
