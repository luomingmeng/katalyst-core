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
	"fmt"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// BuildMaterializationTarget projects one owned durable target into the
// immutable value consumed by the external CPUSet materializer.
func BuildMaterializationTarget(
	target *state.TargetState,
	topology *machine.CPUTopology,
	effectiveReclaimOverlap bool,
) (cpusetmaterializer.Target, error) {
	if target == nil {
		return cpusetmaterializer.Target{}, fmt.Errorf("target state is nil")
	}
	if topology == nil {
		return cpusetmaterializer.Target{}, fmt.Errorf("cpu topology is nil")
	}

	reserve := machine.NewCPUSet()
	reclaim := machine.NewCPUSet()
	containers := make(map[string]map[string]machine.CPUSet)
	for podUID, entries := range target.PodEntries {
		if entries.IsPoolEntry() {
			entry := entries.GetPoolEntry()
			switch commonstate.GetPoolType(commonstate.OwnerPoolNameTranslator.Translate(podUID)) {
			case commonstate.PoolNameReserve:
				reserve = reserve.Union(entry.AllocationResult)
			case commonstate.PoolNameReclaim:
				reclaim = reclaim.Union(entry.AllocationResult)
			}
			continue
		}

		for containerName, allocation := range entries {
			if allocation == nil {
				continue
			}
			if containers[podUID] == nil {
				containers[podUID] = make(map[string]machine.CPUSet)
			}
			containers[podUID][containerName] = allocation.AllocationResult.Clone()
		}
	}

	allCPUs := topology.CPUDetails.CPUs()
	if !reserve.Union(reclaim).IsSubsetOf(allCPUs) {
		return cpusetmaterializer.Target{}, fmt.Errorf(
			"reserve/reclaim cpus %s are outside machine cpus %s",
			reserve.Union(reclaim).String(), allCPUs.String())
	}
	nonReclaim := allCPUs.Difference(reserve).Difference(reclaim)
	reclaimByNUMA := make(map[int]machine.CPUSet)
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceNoSortInt() {
		cpus := reclaim.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		reclaimByNUMA[numaID] = cpus
	}

	return cpusetmaterializer.NewTarget(cpusetmaterializer.TargetInput{
		ReserveCPUSet:        reserve,
		ReclaimCPUSet:        reclaim,
		NonReclaimCPUSet:     nonReclaim,
		ReclaimCPUSetByNUMA:  reclaimByNUMA,
		ContainerCPUSetByPod: containers,
		AllowReclaimOverlap:  effectiveReclaimOverlap,
	}), nil
}

func (p *DynamicPolicy) effectiveReclaimOverlapForTarget(target *state.TargetState) bool {
	return target != nil &&
		p.effectiveReclaimOverlap(target.AllowSharedCoresOverlapReclaimedCores)
}
