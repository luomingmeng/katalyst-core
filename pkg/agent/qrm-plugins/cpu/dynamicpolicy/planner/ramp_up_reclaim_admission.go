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
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type ReclaimUpdateMode string

const (
	ReclaimUpdateFull    ReclaimUpdateMode = "full"
	ReclaimUpdatePartial ReclaimUpdateMode = "partial"
)

var (
	ErrUnknownUpdateMode          = errors.New("unknown reclaim update mode")
	ErrMissingAffectedNUMAs       = errors.New("partial reclaim update has no affected NUMAs")
	ErrTargetOutsideAffectedNUMAs = errors.New("partial reclaim target contains CPUs outside affected NUMAs")
)

type ReclaimTargetUpdate struct {
	Mode          ReclaimUpdateMode
	AffectedNUMAs sets.Int
	Target        machine.CPUSet
}

type ReclaimHardConstraint struct {
	CPUs          machine.CPUSet
	AffectedNUMAs sets.Int
}

// PlanRampUpReclaimPoolTarget returns a copy-on-write target whose reclaim
// pool entry contains the complete global reclaim raw set.
func PlanRampUpReclaimPoolTarget(
	base *state.TargetState,
	update ReclaimTargetUpdate,
	currentFloor ReclaimHardConstraint,
	topology *machine.CPUTopology,
	hardPartitionEnabled bool,
) (*state.TargetState, error) {
	if topology == nil {
		return nil, fmt.Errorf("cpu topology is nil")
	}
	if base == nil {
		base = &state.TargetState{}
	}

	currentReclaimInfo := currentReclaimPoolEntry(base.PodEntries)
	oldReclaim := machine.NewCPUSet()
	oldAssignments := map[int]machine.CPUSet{}
	if currentReclaimInfo != nil {
		oldReclaim = currentReclaimInfo.AllocationResult.Clone()
		oldAssignments = machine.DeepcopyCPUAssignment(currentReclaimInfo.TopologyAwareAssignments)
	}

	merged, err := mergeReclaimTarget(oldReclaim, update, topology)
	if err != nil {
		return nil, err
	}
	hardFloor := effectiveHardFloor(base, currentFloor, topology, hardPartitionEnabled)
	finalReclaim := merged.Union(hardFloor)

	planned := base.Clone()
	setReclaimPool(planned, finalReclaim, topology)
	removeCPUSetFromSharePools(planned, finalReclaim, topology)
	removeCPUSetFromRampUpContainers(planned, finalReclaim, topology)
	planned.MachineState, err = state.GenerateMachineStateFromPodEntries(topology, planned.PodEntries, base.MachineState.Clone())
	if err != nil {
		return nil, fmt.Errorf("generate machine state from planned pod entries: %w", err)
	}

	general.InfoS("ramp-up reclaim planner committing hard target",
		"oldReclaimRaw", oldReclaim.String(),
		"newHardReclaim", finalReclaim.String(),
		"removedFromReclaimRaw", oldReclaim.Difference(finalReclaim).String(),
		"addedToReclaimRaw", finalReclaim.Difference(oldReclaim).String(),
		"oldTopologyAssignments", oldAssignments,
		"newTopologyAssignments", planned.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].TopologyAwareAssignments)

	return planned, nil
}

func mergeReclaimTarget(old machine.CPUSet, update ReclaimTargetUpdate, topology *machine.CPUTopology) (machine.CPUSet, error) {
	switch update.Mode {
	case ReclaimUpdateFull:
		return update.Target.Clone(), nil
	case ReclaimUpdatePartial:
		if update.AffectedNUMAs.Len() == 0 {
			return machine.NewCPUSet(), ErrMissingAffectedNUMAs
		}
	default:
		return machine.NewCPUSet(), ErrUnknownUpdateMode
	}

	affectedCPUs := cpusInNUMAs(topology, update.AffectedNUMAs)
	if !update.Target.IsSubsetOf(affectedCPUs) {
		return machine.NewCPUSet(), ErrTargetOutsideAffectedNUMAs
	}

	merged := old.Clone()
	for numaID := range update.AffectedNUMAs {
		numaCPUs := topology.CPUDetails.CPUsInNUMANodes(numaID)
		merged = merged.Difference(numaCPUs).Union(update.Target.Intersection(numaCPUs))
	}
	return merged, nil
}

func effectiveHardFloor(
	base *state.TargetState,
	current ReclaimHardConstraint,
	topology *machine.CPUTopology,
	hardPartitionEnabled bool,
) machine.CPUSet {
	if !hardPartitionEnabled {
		return machine.NewCPUSet()
	}

	activeNUMAs := sets.NewInt()
	hasActiveRampUp := false
	hasRampUpWithoutAssignments := false
	if base != nil {
		for _, containers := range base.PodEntries {
			for _, info := range containers {
				if info == nil || !info.RampUp {
					continue
				}
				hasActiveRampUp = true
				if len(info.TopologyAwareAssignments) == 0 {
					hasRampUpWithoutAssignments = true
				}
				for numaID := range info.TopologyAwareAssignments {
					activeNUMAs.Insert(numaID)
				}
			}
		}
	}

	committedReclaim := machine.NewCPUSet()
	if base != nil {
		if reclaimInfo := currentReclaimPoolEntry(base.PodEntries); reclaimInfo != nil {
			committedReclaim = reclaimInfo.AllocationResult.Clone()
		}
	}

	activeFloor := machine.NewCPUSet()
	switch {
	case !hasActiveRampUp:
	case hasRampUpWithoutAssignments:
		activeFloor = committedReclaim
	default:
		activeFloor = committedReclaim.Intersection(cpusInNUMAs(topology, activeNUMAs))
	}
	return activeFloor.Union(current.CPUs)
}

// ActiveRampUpReclaimFloor returns the committed reclaim floor owned by active
// ramp-up containers. It applies the canonical hard-floor rules without adding
// a current-request constraint.
func ActiveRampUpReclaimFloor(
	base *state.TargetState,
	topology *machine.CPUTopology,
	hardPartitionEnabled bool,
) machine.CPUSet {
	return effectiveHardFloor(base, ReclaimHardConstraint{}, topology, hardPartitionEnabled)
}

func cpusInNUMAs(topology *machine.CPUTopology, numaIDs sets.Int) machine.CPUSet {
	cpus := machine.NewCPUSet()
	if topology == nil {
		return cpus
	}
	for numaID := range numaIDs {
		cpus = cpus.Union(topology.CPUDetails.CPUsInNUMANodes(numaID))
	}
	return cpus
}

func setReclaimPool(target *state.TargetState, reclaim machine.CPUSet, topology *machine.CPUTopology) {
	if target.PodEntries == nil {
		target.PodEntries = make(state.PodEntries)
	}
	if target.PodEntries[commonstate.PoolNameReclaim] == nil {
		target.PodEntries[commonstate.PoolNameReclaim] = make(state.ContainerEntries)
	}
	reclaimInfo := target.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName]
	if reclaimInfo == nil {
		reclaimInfo = &state.AllocationInfo{
			AllocationMeta: commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		}
		target.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName] = reclaimInfo
	}
	setAllocationCPUSet(reclaimInfo, reclaim, topology)
}

func removeCPUSetFromSharePools(target *state.TargetState, removed machine.CPUSet, topology *machine.CPUTopology) {
	for poolName, entries := range target.PodEntries {
		if commonstate.GetPoolType(poolName) != commonstate.PoolNameShare {
			continue
		}
		info := entries.GetPoolEntry()
		if info == nil {
			continue
		}
		setAllocationCPUSet(info, info.AllocationResult.Difference(removed), topology)
	}
}

func removeCPUSetFromRampUpContainers(target *state.TargetState, removed machine.CPUSet, topology *machine.CPUTopology) {
	for podUID, containers := range target.PodEntries {
		if podUID == commonstate.PoolNameReclaim || podUID == commonstate.PoolNameShare {
			continue
		}
		for _, info := range containers {
			if info != nil && info.RampUp {
				setAllocationCPUSet(info, info.AllocationResult.Difference(removed), topology)
			}
		}
	}
}

func setAllocationCPUSet(info *state.AllocationInfo, cpus machine.CPUSet, topology *machine.CPUTopology) {
	info.AllocationResult = cpus.Clone()
	info.OriginalAllocationResult = cpus.Clone()
	info.TopologyAwareAssignments = projectCPUSetByNUMA(cpus, topology)
	info.OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(info.TopologyAwareAssignments)
}

func projectCPUSetByNUMA(cpus machine.CPUSet, topology *machine.CPUTopology) map[int]machine.CPUSet {
	assignments := make(map[int]machine.CPUSet)
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceInt() {
		assigned := cpus.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		if !assigned.IsEmpty() {
			assignments[numaID] = assigned
		}
	}
	return assignments
}

func currentReclaimPoolEntry(entries state.PodEntries) *state.AllocationInfo {
	if entries == nil || entries[commonstate.PoolNameReclaim] == nil {
		return nil
	}
	return entries[commonstate.PoolNameReclaim][commonstate.FakedContainerName]
}
