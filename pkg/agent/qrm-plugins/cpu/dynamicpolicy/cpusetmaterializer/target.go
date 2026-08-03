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

package cpusetmaterializer

import "github.com/kubewharf/katalyst-core/pkg/util/machine"

type TargetInput struct {
	ReserveCPUSet        machine.CPUSet
	ReclaimCPUSet        machine.CPUSet
	NonReclaimCPUSet     machine.CPUSet
	ReclaimCPUSetByNUMA  map[int]machine.CPUSet
	ContainerCPUSetByPod map[string]map[string]machine.CPUSet
	AllowReclaimOverlap  bool
}

type Target struct {
	reserveCPUSet        machine.CPUSet
	reclaimCPUSet        machine.CPUSet
	nonReclaimCPUSet     machine.CPUSet
	reclaimCPUSetByNUMA  map[int]machine.CPUSet
	containerCPUSetByPod map[string]map[string]machine.CPUSet
	allowReclaimOverlap  bool
}

func NewTarget(in TargetInput) Target {
	return Target{
		reserveCPUSet:        cloneCPUSet(in.ReserveCPUSet),
		reclaimCPUSet:        cloneCPUSet(in.ReclaimCPUSet),
		nonReclaimCPUSet:     cloneCPUSet(in.NonReclaimCPUSet),
		reclaimCPUSetByNUMA:  cloneCPUSetByNUMA(in.ReclaimCPUSetByNUMA),
		containerCPUSetByPod: cloneContainerCPUSetByPod(in.ContainerCPUSetByPod),
		allowReclaimOverlap:  in.AllowReclaimOverlap,
	}
}

func (t Target) ReserveCPUSet() machine.CPUSet {
	return cloneCPUSet(t.reserveCPUSet)
}

func (t Target) ReclaimCPUSet() machine.CPUSet {
	return cloneCPUSet(t.reclaimCPUSet)
}

func (t Target) NonReclaimCPUSet() machine.CPUSet {
	return cloneCPUSet(t.nonReclaimCPUSet)
}

func (t Target) ReclaimCPUSetByNUMA() map[int]machine.CPUSet {
	return cloneCPUSetByNUMA(t.reclaimCPUSetByNUMA)
}

func (t Target) ContainerCPUSetByPod() map[string]map[string]machine.CPUSet {
	return cloneContainerCPUSetByPod(t.containerCPUSetByPod)
}

func (t Target) AllowReclaimOverlap() bool {
	return t.allowReclaimOverlap
}

// cloneCPUSet preserves the semantic distinction between an uninitialized
// zero-value CPUSet and an initialized empty CPUSet while isolating elems.
func cloneCPUSet(in machine.CPUSet) machine.CPUSet {
	if !in.Initialed {
		return machine.CPUSet{}
	}
	return in.Clone()
}

func cloneCPUSetByNUMA(in map[int]machine.CPUSet) map[int]machine.CPUSet {
	if in == nil {
		return nil
	}

	out := make(map[int]machine.CPUSet, len(in))
	for numaID, cpus := range in {
		out[numaID] = cloneCPUSet(cpus)
	}
	return out
}

func cloneContainerCPUSetByPod(in map[string]map[string]machine.CPUSet) map[string]map[string]machine.CPUSet {
	if in == nil {
		return nil
	}

	out := make(map[string]map[string]machine.CPUSet, len(in))
	for podUID, containers := range in {
		if containers == nil {
			out[podUID] = nil
			continue
		}

		containerCPUSet := make(map[string]machine.CPUSet, len(containers))
		for containerName, cpus := range containers {
			containerCPUSet[containerName] = cloneCPUSet(cpus)
		}
		out[podUID] = containerCPUSet
	}
	return out
}
