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
	"k8s.io/apimachinery/pkg/util/sets"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestPlanRampUpReclaimPoolTargetFullUpdateUsesTargetState(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	cleanNUMA := &state.NUMANodeState{
		DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
	}
	existingReclaim := &state.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(0),
	}
	otherInfo := &state.AllocationInfo{AllocationResult: machine.NewCPUSet(2)}
	base := &state.TargetState{
		PodEntries: state.PodEntries{
			commonstate.PoolNameReclaim: {
				commonstate.FakedContainerName: existingReclaim,
			},
			"other-pod": {"container": otherInfo},
		},
		MachineState: state.NUMANodeMap{
			0: cleanNUMA,
		},
		NUMAHeadroom:                               map[int]float64{0: 3.5},
		AllowSharedCoresOverlapReclaimedCores:      true,
		DisableDedicatedCoresOverlapReclaimedCores: true,
	}
	target := machine.NewCPUSet(0, 4)

	planned, err := PlanRampUpReclaimPoolTarget(base, ReclaimTargetUpdate{
		Mode:   ReclaimUpdateFull,
		Target: target,
	}, ReclaimHardConstraint{
		CPUs: machine.NewCPUSet(1),
	}, topology, false)
	require.NoError(t, err)

	reclaimInfo := planned.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName]
	require.True(t, reclaimInfo.AllocationResult.Equals(target))
	require.True(t, reclaimInfo.OriginalAllocationResult.Equals(target))
	require.True(t, reclaimInfo.TopologyAwareAssignments[0].Equals(machine.NewCPUSet(0, 4)))
	require.Equal(t, reclaimInfo.TopologyAwareAssignments, reclaimInfo.OriginalTopologyAwareAssignments)

	require.True(t, existingReclaim.AllocationResult.Equals(machine.NewCPUSet(0)))
	require.NotSame(t, otherInfo, planned.PodEntries["other-pod"]["container"])
	require.Equal(t, base.NUMAHeadroom, planned.NUMAHeadroom)
	require.True(t, planned.AllowSharedCoresOverlapReclaimedCores)
	require.True(t, planned.DisableDedicatedCoresOverlapReclaimedCores)
}

func TestPlanRampUpReclaimPoolTargetDoesNotMutateBaseMachineStatePreOccPodEntries(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	preOccInfo := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			QoSLevel: apiconsts.PodAnnotationQoSLevelDedicatedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				"sentinel": "keep",
			},
		},
		AllocationResult:         machine.NewCPUSet(1),
		OriginalAllocationResult: machine.NewCPUSet(),
	}
	base := targetWithReclaim(machine.NewCPUSet(0))
	base.MachineState = state.NUMANodeMap{
		0: {
			DefaultCPUSet:   machine.NewCPUSet(),
			AllocatedCPUSet: machine.NewCPUSet(),
			PreOccPodEntries: state.PodEntries{
				"pre-occ-pod": {
					"main": preOccInfo,
				},
			},
		},
	}
	baseBefore := base.Clone()

	_, err = PlanRampUpReclaimPoolTarget(base, ReclaimTargetUpdate{
		Mode:   ReclaimUpdateFull,
		Target: machine.NewCPUSet(0),
	}, ReclaimHardConstraint{}, topology, false)
	require.NoError(t, err)

	require.Equal(t, baseBefore.MachineState, base.MachineState)
	require.Equal(t, map[string]string{
		apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		"sentinel": "keep",
	}, preOccInfo.Annotations)
	require.NotContains(t, preOccInfo.Annotations, "pre-occ-delete-timestamp")
}

func TestPlanRampUpReclaimPoolTargetProducesCompleteTarget(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	base := targetWithReclaimAndShare(
		machine.NewCPUSet(2, 3),
		machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
		topology,
	)
	base.PodEntries["pod-ramp"] = state.ContainerEntries{
		"main": {
			AllocationResult:                 machine.NewCPUSet(0, 1, 2, 3),
			OriginalAllocationResult:         machine.NewCPUSet(0, 1, 2, 3),
			TopologyAwareAssignments:         assignmentsFor(topology, machine.NewCPUSet(0, 1, 2, 3)),
			OriginalTopologyAwareAssignments: assignmentsFor(topology, machine.NewCPUSet(0, 1, 2, 3)),
			RampUp:                           true,
		},
	}
	baseBefore := base.Clone()

	next, err := PlanRampUpReclaimPoolTarget(base, ReclaimTargetUpdate{
		Mode:   ReclaimUpdateFull,
		Target: machine.NewCPUSet(2, 3),
	}, ReclaimHardConstraint{CPUs: machine.NewCPUSet(2, 3)}, topology, true)
	require.NoError(t, err)

	require.True(t, reclaimRaw(next).Equals(machine.NewCPUSet(2, 3)))
	require.Empty(t, reclaimRaw(next).Intersection(poolCPUSet(next, commonstate.PoolNameShare)).ToSliceInt())
	require.Empty(t, reclaimRaw(next).Intersection(
		next.PodEntries["pod-ramp"]["main"].AllocationResult).ToSliceInt())
	require.True(t, next.PodEntries["pod-ramp"]["main"].AllocationResult.Equals(machine.NewCPUSet(0, 1)))
	require.True(t, next.PodEntries["pod-ramp"]["main"].OriginalAllocationResult.Equals(machine.NewCPUSet(0, 1)))
	require.NoError(t, ValidateTarget(next, topology, machine.NewCPUSet(2, 3), true))
	require.Equal(t, baseBefore, base)
}

func TestPlanRampUpReclaimPoolTargetRemovesReclaimFromEveryShareTypePool(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	reclaim := machine.NewCPUSet(0)
	base := targetWithReclaim(reclaim)
	for _, poolName := range []string{
		commonstate.GetNUMAPoolName(commonstate.PoolNameShare, 0),
		"custom-share-pool",
	} {
		base.PodEntries[poolName] = state.ContainerEntries{
			commonstate.FakedContainerName: {
				AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(poolName),
				AllocationResult:                 machine.NewCPUSet(0, 1),
				OriginalAllocationResult:         machine.NewCPUSet(0, 1),
				TopologyAwareAssignments:         assignmentsFor(topology, machine.NewCPUSet(0, 1)),
				OriginalTopologyAwareAssignments: assignmentsFor(topology, machine.NewCPUSet(0, 1)),
			},
		}
	}

	planned, err := PlanRampUpReclaimPoolTarget(base, ReclaimTargetUpdate{
		Mode:   ReclaimUpdateFull,
		Target: reclaim,
	}, ReclaimHardConstraint{CPUs: reclaim}, topology, true)
	require.NoError(t, err)

	for _, poolName := range []string{
		commonstate.GetNUMAPoolName(commonstate.PoolNameShare, 0),
		"custom-share-pool",
	} {
		require.True(t, planned.PodEntries[poolName][commonstate.FakedContainerName].
			AllocationResult.Equals(machine.NewCPUSet(1)))
	}
	require.NoError(t, ValidateTarget(planned, topology, reclaim, true))
}

func TestPlanRampUpReclaimPoolTargetPartialUpdatePreservesUnaffectedNUMAs(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	require.Len(t, numaIDs, 2)
	firstNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[0])
	secondNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[1])
	oldFirst := machine.NewCPUSet(firstNUMA.ToSliceInt()[0])
	oldSecond := machine.NewCPUSet(secondNUMA.ToSliceInt()[0])
	newFirst := machine.NewCPUSet(firstNUMA.ToSliceInt()[1])
	base := targetWithReclaim(oldFirst.Union(oldSecond))

	planned, err := PlanRampUpReclaimPoolTarget(base, ReclaimTargetUpdate{
		Mode:          ReclaimUpdatePartial,
		AffectedNUMAs: sets.NewInt(numaIDs[0]),
		Target:        newFirst,
	}, ReclaimHardConstraint{}, topology, false)
	require.NoError(t, err)
	require.True(t, reclaimRaw(planned).Equals(newFirst.Union(oldSecond)))
	require.True(t, reclaimRaw(base).Equals(oldFirst.Union(oldSecond)))
}

func TestPlanRampUpReclaimPoolTargetPartialUpdateCanClearAffectedNUMA(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	firstNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[0])
	secondNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[1])
	oldFirst := machine.NewCPUSet(firstNUMA.ToSliceInt()[0])
	oldSecond := machine.NewCPUSet(secondNUMA.ToSliceInt()[0])

	planned, err := PlanRampUpReclaimPoolTarget(targetWithReclaim(oldFirst.Union(oldSecond)), ReclaimTargetUpdate{
		Mode:          ReclaimUpdatePartial,
		AffectedNUMAs: sets.NewInt(numaIDs[0]),
		Target:        machine.NewCPUSet(),
	}, ReclaimHardConstraint{}, topology, false)
	require.NoError(t, err)
	require.True(t, reclaimRaw(planned).Equals(oldSecond))
}

func TestPlanRampUpReclaimPoolTargetRejectsInvalidPartialUpdate(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	secondNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[1])
	outsideCPU := secondNUMA.ToSliceInt()[0]

	_, err = PlanRampUpReclaimPoolTarget(&state.TargetState{}, ReclaimTargetUpdate{
		Mode: ReclaimUpdatePartial,
	}, ReclaimHardConstraint{}, topology, false)
	require.ErrorIs(t, err, ErrMissingAffectedNUMAs)

	_, err = PlanRampUpReclaimPoolTarget(&state.TargetState{}, ReclaimTargetUpdate{
		Mode:          ReclaimUpdatePartial,
		AffectedNUMAs: sets.NewInt(numaIDs[0]),
		Target:        machine.NewCPUSet(outsideCPU),
	}, ReclaimHardConstraint{}, topology, false)
	require.ErrorIs(t, err, ErrTargetOutsideAffectedNUMAs)

	_, err = PlanRampUpReclaimPoolTarget(&state.TargetState{}, ReclaimTargetUpdate{
		Mode: "unknown",
	}, ReclaimHardConstraint{}, topology, false)
	require.ErrorIs(t, err, ErrUnknownUpdateMode)
}

func TestEffectiveHardFloorCombinesCommittedActiveNUMAsAndCurrentRequest(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	firstNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[0])
	secondNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[1])
	activeCommitted := machine.NewCPUSet(firstNUMA.ToSliceInt()[0])
	ordinaryCommitted := machine.NewCPUSet(secondNUMA.ToSliceInt()[0])
	current := machine.NewCPUSet(secondNUMA.ToSliceInt()[1])
	base := targetWithReclaim(activeCommitted.Union(ordinaryCommitted))
	base.PodEntries["active-pod"] = state.ContainerEntries{
		"container": {
			RampUp: true,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				numaIDs[0]: firstNUMA,
			},
		},
	}

	floor := effectiveHardFloor(base, ReclaimHardConstraint{
		CPUs:          current,
		AffectedNUMAs: sets.NewInt(numaIDs[1]),
	}, topology, true)
	require.True(t, floor.Equals(activeCommitted.Union(current)))
}

func TestEffectiveHardFloorDerivesAllActiveOwnersFromBase(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	firstNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[0])
	secondNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[1])
	committed := machine.NewCPUSet(firstNUMA.ToSliceInt()[0], secondNUMA.ToSliceInt()[0])
	base := targetWithReclaim(committed)
	base.PodEntries["current-owner"] = state.ContainerEntries{
		"main": {
			RampUp: true,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				numaIDs[1]: secondNUMA,
			},
		},
	}

	floor := effectiveHardFloor(base, ReclaimHardConstraint{}, topology, true)
	require.True(t, floor.Equals(machine.NewCPUSet(secondNUMA.ToSliceInt()[0])))
}

func TestEffectiveHardFloorProtectsGlobalRawWhenRampUpMetadataMissing(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	committed := machine.NewCPUSet(0, 1)
	base := targetWithReclaim(committed)
	base.PodEntries["active-pod"] = state.ContainerEntries{
		"container": {
			RampUp: true,
		},
	}

	floor := effectiveHardFloor(base, ReclaimHardConstraint{}, topology, true)
	require.True(t, floor.Equals(committed))
}

func TestEffectiveHardFloorProtectsGlobalRawWhenAnyRampUpOwnerMetadataMissing(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	firstNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[0])
	secondNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[1])
	committed := machine.NewCPUSet(firstNUMA.ToSliceInt()[0], secondNUMA.ToSliceInt()[0])
	base := targetWithReclaim(committed)
	base.PodEntries["owner-with-assignments"] = state.ContainerEntries{
		"main": {
			RampUp: true,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				numaIDs[0]: firstNUMA,
			},
		},
	}
	base.PodEntries["owner-without-assignments"] = state.ContainerEntries{
		"main": {
			RampUp: true,
		},
	}

	floor := effectiveHardFloor(base, ReclaimHardConstraint{}, topology, true)
	require.True(t, floor.Equals(committed))
}

func TestEffectiveHardFloorDisabledReturnsEmpty(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	base := targetWithReclaim(machine.NewCPUSet(0, 1))
	base.PodEntries["active-pod"] = state.ContainerEntries{
		"container": {
			RampUp: true,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.NewCPUSet(0),
			},
		},
	}

	floor := effectiveHardFloor(base, ReclaimHardConstraint{
		CPUs: machine.NewCPUSet(2),
	}, topology, false)
	require.True(t, floor.IsEmpty())
}

func TestActiveRampUpReclaimFloorUsesCanonicalFloorWithEmptyCurrentConstraint(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	firstNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[0])
	secondNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[1])
	committed := machine.NewCPUSet(firstNUMA.ToSliceInt()[0], secondNUMA.ToSliceInt()[0])
	base := targetWithReclaim(committed)
	base.PodEntries["active-owner"] = state.ContainerEntries{
		"main": {
			RampUp: true,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				numaIDs[0]: firstNUMA,
			},
		},
	}

	got := ActiveRampUpReclaimFloor(base, topology, true)
	want := effectiveHardFloor(base, ReclaimHardConstraint{}, topology, true)

	require.True(t, got.Equals(want))
	require.True(t, got.Equals(machine.NewCPUSet(firstNUMA.ToSliceInt()[0])))
}

func TestActiveRampUpReclaimFloorKeepsIncompleteOwnerFailSafe(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	committed := machine.NewCPUSet(0, 1)
	base := targetWithReclaim(committed)
	base.PodEntries["incomplete-owner"] = state.ContainerEntries{
		"main": {
			RampUp: true,
		},
	}

	floor := ActiveRampUpReclaimFloor(base, topology, true)

	require.True(t, floor.Equals(committed))
}

func TestPlanRampUpReclaimPoolTargetEnforcesHardFloor(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	firstNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[0])
	secondNUMA := topology.CPUDetails.CPUsInNUMANodes(numaIDs[1])
	activeCommitted := machine.NewCPUSet(firstNUMA.ToSliceInt()[0])
	ordinaryCommitted := machine.NewCPUSet(secondNUMA.ToSliceInt()[0])
	current := machine.NewCPUSet(secondNUMA.ToSliceInt()[1])
	base := targetWithReclaim(activeCommitted.Union(ordinaryCommitted))
	base.PodEntries["active-pod"] = state.ContainerEntries{
		"container": {
			RampUp: true,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				numaIDs[0]: firstNUMA,
			},
		},
	}

	planned, err := PlanRampUpReclaimPoolTarget(base, ReclaimTargetUpdate{
		Mode:   ReclaimUpdateFull,
		Target: machine.NewCPUSet(),
	}, ReclaimHardConstraint{
		CPUs: current,
	}, topology, true)
	require.NoError(t, err)
	require.True(t, reclaimRaw(planned).Equals(activeCommitted.Union(current)))
}

func TestPlanRampUpReclaimPoolTargetRejectsNilTopology(t *testing.T) {
	t.Parallel()

	_, err := PlanRampUpReclaimPoolTarget(&state.TargetState{}, ReclaimTargetUpdate{
		Mode: ReclaimUpdateFull,
	}, ReclaimHardConstraint{}, nil, true)
	require.ErrorContains(t, err, "cpu topology is nil")
}

func targetWithReclaim(cpus machine.CPUSet) *state.TargetState {
	return &state.TargetState{
		PodEntries: state.PodEntries{
			commonstate.PoolNameReclaim: {
				commonstate.FakedContainerName: {
					AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
					AllocationResult: cpus,
				},
			},
		},
	}
}

func targetWithReclaimAndShare(
	reclaim machine.CPUSet,
	share machine.CPUSet,
	topology *machine.CPUTopology,
) *state.TargetState {
	target := targetWithReclaim(reclaim)
	target.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].OriginalAllocationResult = reclaim.Clone()
	target.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].TopologyAwareAssignments =
		assignmentsFor(topology, reclaim)
	target.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].OriginalTopologyAwareAssignments =
		assignmentsFor(topology, reclaim)
	target.PodEntries[commonstate.PoolNameShare] = state.ContainerEntries{
		commonstate.FakedContainerName: {
			AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
			AllocationResult:                 share.Clone(),
			OriginalAllocationResult:         share.Clone(),
			TopologyAwareAssignments:         assignmentsFor(topology, share),
			OriginalTopologyAwareAssignments: assignmentsFor(topology, share),
		},
	}
	target.MachineState, _ = state.GenerateMachineStateFromPodEntries(topology, target.PodEntries, nil)
	return target
}

func assignmentsFor(topology *machine.CPUTopology, cpus machine.CPUSet) map[int]machine.CPUSet {
	assignments := make(map[int]machine.CPUSet)
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceInt() {
		if inNUMA := cpus.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID)); !inNUMA.IsEmpty() {
			assignments[numaID] = inNUMA
		}
	}
	return assignments
}

func poolCPUSet(target *state.TargetState, poolName string) machine.CPUSet {
	return target.PodEntries[poolName][commonstate.FakedContainerName].AllocationResult
}

func reclaimRaw(target *state.TargetState) machine.CPUSet {
	return target.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].AllocationResult
}
