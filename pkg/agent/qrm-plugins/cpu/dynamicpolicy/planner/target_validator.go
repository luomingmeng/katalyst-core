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
	"reflect"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

var (
	ErrHardFloorDropped           = errors.New("reclaim hard floor dropped")
	ErrReclaimOverlapsShare       = errors.New("reclaim overlaps share pool")
	ErrReclaimOverlapsRampUp      = errors.New("reclaim overlaps ramp-up allocation")
	ErrTopologyProjectionMismatch = errors.New("topology assignment projection mismatch")
	ErrMachineStateMismatch       = errors.New("machine state does not match pod entries")
)

func ValidateTarget(
	target *state.TargetState,
	topology *machine.CPUTopology,
	requiredFloor machine.CPUSet,
	requireDisjoint bool,
) error {
	if target == nil {
		return fmt.Errorf("target state is nil")
	}
	if topology == nil {
		return fmt.Errorf("cpu topology is nil")
	}

	reclaim := machine.NewCPUSet()
	if reclaimInfo := currentReclaimPoolEntry(target.PodEntries); reclaimInfo != nil {
		reclaim = reclaimInfo.AllocationResult.Clone()
	}
	if !requiredFloor.IsSubsetOf(reclaim) {
		return fmt.Errorf("%w: required %s, reclaim %s", ErrHardFloorDropped, requiredFloor.String(), reclaim.String())
	}

	if requireDisjoint {
		for poolName, entries := range target.PodEntries {
			if commonstate.GetPoolType(poolName) != commonstate.PoolNameShare {
				continue
			}
			info := entries.GetPoolEntry()
			if info == nil {
				continue
			}
			overlap := reclaim.Intersection(info.AllocationResult)
			if !overlap.IsEmpty() {
				return fmt.Errorf("%w: pool %q overlap %s", ErrReclaimOverlapsShare, poolName, overlap.String())
			}
		}
		for podUID, containers := range target.PodEntries {
			if podUID == commonstate.PoolNameReclaim || podUID == commonstate.PoolNameShare {
				continue
			}
			for _, info := range containers {
				if info != nil && info.RampUp && !reclaim.Intersection(info.AllocationResult).IsEmpty() {
					return fmt.Errorf("%w: overlap %s", ErrReclaimOverlapsRampUp,
						reclaim.Intersection(info.AllocationResult).String())
				}
			}
		}
	}

	for podUID, containers := range target.PodEntries {
		for containerName, info := range containers {
			if info == nil {
				continue
			}
			if !assignmentsEqual(info.TopologyAwareAssignments, projectCPUSetByNUMA(info.AllocationResult, topology)) ||
				!assignmentsEqual(info.OriginalTopologyAwareAssignments,
					projectCPUSetByNUMA(info.OriginalAllocationResult, topology)) {
				return fmt.Errorf("%w: pod %q container %q", ErrTopologyProjectionMismatch, podUID, containerName)
			}
		}
	}

	expected, err := state.GenerateMachineStateFromPodEntries(topology, target.PodEntries, target.MachineState.Clone())
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMachineStateMismatch, err)
	}
	if !reflect.DeepEqual(expected, target.MachineState) {
		return ErrMachineStateMismatch
	}
	return nil
}

func assignmentsEqual(left, right map[int]machine.CPUSet) bool {
	if len(left) != len(right) {
		return false
	}
	for numaID, leftCPUs := range left {
		rightCPUs, ok := right[numaID]
		if !ok || !leftCPUs.Equals(rightCPUs) {
			return false
		}
	}
	return true
}
