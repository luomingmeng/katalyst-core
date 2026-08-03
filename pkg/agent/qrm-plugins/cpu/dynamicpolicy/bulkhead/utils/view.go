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

package utils

import (
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type CPUSetPartitionView struct {
	Reserve                        machine.CPUSet
	Dedicated                      machine.CPUSet
	ReclaimRaw                     machine.CPUSet
	SharePool                      machine.CPUSet
	SharePoolMap                   map[string]machine.CPUSet
	Isolation                      machine.CPUSet
	DesiredNonReclaimPool          machine.CPUSet
	DesiredReclaimEffective        machine.CPUSet
	DesiredReclaimEffectivePerNUMA map[int]machine.CPUSet
	NonReclaimPool                 machine.CPUSet
	ReclaimEffective               machine.CPUSet
	ReclaimEffectivePerNUMA        map[int]machine.CPUSet
	ContainerCPUSetByPod           map[string]map[string]machine.CPUSet
}

func BuildCPUSetPartitionViewFromTarget(target cpusetmaterializer.Target) *CPUSetPartitionView {
	reclaim := target.ReclaimCPUSet()
	nonReclaim := target.NonReclaimCPUSet()
	reclaimByNUMA := target.ReclaimCPUSetByNUMA()
	return &CPUSetPartitionView{
		Reserve:                        target.ReserveCPUSet(),
		Dedicated:                      machine.NewCPUSet(),
		ReclaimRaw:                     reclaim.Clone(),
		SharePool:                      machine.NewCPUSet(),
		SharePoolMap:                   map[string]machine.CPUSet{},
		Isolation:                      machine.NewCPUSet(),
		DesiredNonReclaimPool:          nonReclaim.Clone(),
		DesiredReclaimEffective:        reclaim.Clone(),
		DesiredReclaimEffectivePerNUMA: cloneCPUSetByNUMA(reclaimByNUMA),
		NonReclaimPool:                 nonReclaim,
		ReclaimEffective:               reclaim,
		ReclaimEffectivePerNUMA:        cloneCPUSetByNUMA(reclaimByNUMA),
		ContainerCPUSetByPod:           cloneContainerCPUSetByPod(target.ContainerCPUSetByPod()),
	}
}

func cloneCPUSetByNUMA(in map[int]machine.CPUSet) map[int]machine.CPUSet {
	out := make(map[int]machine.CPUSet, len(in))
	for numaID, cpus := range in {
		out[numaID] = cpus.Clone()
	}
	return out
}

func cloneContainerCPUSetByPod(in map[string]map[string]machine.CPUSet) map[string]map[string]machine.CPUSet {
	out := make(map[string]map[string]machine.CPUSet, len(in))
	for podUID, containers := range in {
		out[podUID] = make(map[string]machine.CPUSet, len(containers))
		for containerName, cpus := range containers {
			out[podUID][containerName] = cpus.Clone()
		}
	}
	return out
}

func rebuildDesiredReclaimEffectivePerNUMA(view *CPUSetPartitionView, topology *machine.CPUTopology) {
	if view == nil {
		return
	}
	view.DesiredReclaimEffectivePerNUMA = map[int]machine.CPUSet{}
	if topology == nil {
		return
	}
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceNoSortInt() {
		intersection := view.DesiredReclaimEffective.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		view.DesiredReclaimEffectivePerNUMA[numaID] = intersection
	}
}

func (v *CPUSetPartitionView) DeepCopy() *CPUSetPartitionView {
	if v == nil {
		return nil
	}
	out := &CPUSetPartitionView{
		Reserve:                        v.Reserve.Clone(),
		Dedicated:                      v.Dedicated.Clone(),
		ReclaimRaw:                     v.ReclaimRaw.Clone(),
		SharePool:                      v.SharePool.Clone(),
		SharePoolMap:                   map[string]machine.CPUSet{},
		Isolation:                      v.Isolation.Clone(),
		DesiredNonReclaimPool:          v.DesiredNonReclaimPool.Clone(),
		DesiredReclaimEffective:        v.DesiredReclaimEffective.Clone(),
		DesiredReclaimEffectivePerNUMA: map[int]machine.CPUSet{},
		NonReclaimPool:                 v.NonReclaimPool.Clone(),
		ReclaimEffective:               v.ReclaimEffective.Clone(),
		ReclaimEffectivePerNUMA:        map[int]machine.CPUSet{},
		ContainerCPUSetByPod:           map[string]map[string]machine.CPUSet{},
	}
	for numaID, cpus := range v.ReclaimEffectivePerNUMA {
		out.ReclaimEffectivePerNUMA[numaID] = cpus.Clone()
	}
	for numaID, cpus := range v.DesiredReclaimEffectivePerNUMA {
		out.DesiredReclaimEffectivePerNUMA[numaID] = cpus.Clone()
	}
	for poolName, cpus := range v.SharePoolMap {
		out.SharePoolMap[poolName] = cpus.Clone()
	}
	for podUID, containers := range v.ContainerCPUSetByPod {
		out.ContainerCPUSetByPod[podUID] = map[string]machine.CPUSet{}
		for containerName, cpus := range containers {
			out.ContainerCPUSetByPod[podUID][containerName] = cpus.Clone()
		}
	}
	return out
}
