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

package state

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// ReconcileMachineState fills missing device entries without replacing existing
// allocation state. Generators sharing a topology registry read one frozen view.
// An allocated device absent from discovery is retained with zero allocatable;
// its PodResourceEntries remain authoritative, including across checkpoint restore.
func ReconcileMachineState(current AllocationResourcesMap, entries PodResourceEntries, registry *DefaultResourceStateGeneratorRegistry) (AllocationResourcesMap, error) {
	if registry == nil {
		return nil, fmt.Errorf("nil resource state generator registry")
	}
	generators := registry.GetGenerators()
	views := make(map[*machine.DeviceTopologyRegistry]map[string]*machine.DeviceTopology)
	names := make(map[*machine.DeviceTopologyRegistry][]string)
	for _, generator := range generators {
		if g, ok := generator.(*genericDefaultResourceStateGenerator); ok {
			names[g.topologyRegistry] = append(names[g.topologyRegistry], g.deviceNames...)
		}
	}
	for r, deviceNames := range names {
		if r == nil {
			return nil, fmt.Errorf("nil device topology registry")
		}
		topologies, _ := r.GetDeviceTopologies(deviceNames)
		views[r] = make(map[string]*machine.DeviceTopology)
		for name, topology := range topologies {
			views[r][name] = topology.Clone()
		}
	}
	candidate := current.Clone()
	if candidate == nil {
		candidate = make(AllocationResourcesMap)
	}
	for name, generator := range generators {
		resourceName := v1.ResourceName(name)
		_, exists := current[resourceName]
		if !generator.MustInitDefaultResourceState() && !exists && len(entries[resourceName]) == 0 {
			continue
		}
		frozen := generator
		if g, ok := generator.(*genericDefaultResourceStateGenerator); ok {
			topologies := make(map[string]*machine.DeviceTopology)
			for _, deviceName := range g.deviceNames {
				if t := views[g.topologyRegistry][deviceName]; t != nil {
					topologies[deviceName] = t
				}
			}
			topology := MergeDeviceTopologies(topologies)
			if topology == nil {
				// Optional resources and pending discovery must not erase existing state.
				continue
			}
			if topologyStateMatches(current[resourceName], entries[resourceName], topology, g.allocatable) {
				continue
			}
			frozen = &frozenResourceStateGenerator{defaults: defaultStateForTopology(topology, g.allocatable), mustInit: g.mustInitDefaultResourceState}
		}
		generated, err := GenerateResourceStateFromPodEntries(entries[resourceName], frozen)
		if err != nil {
			return nil, fmt.Errorf("generate resource %s: %w", name, err)
		}
		merged := candidate[resourceName]
		if merged == nil {
			merged = make(AllocationMap)
		}
		for id, generatedState := range generated {
			old := merged[id]
			if old == nil || old.Allocatable == 0 {
				merged[id] = generatedState
			}
		}
		// For concrete topology-backed resources, mark disappeared devices unavailable
		// but retain allocations. Unallocated residual entries need not accumulate.
		if g, ok := generator.(*genericDefaultResourceStateGenerator); ok {
			topologyMap := make(map[string]*machine.DeviceTopology)
			for _, n := range g.deviceNames {
				if t := views[g.topologyRegistry][n]; t != nil {
					topologyMap[n] = t
				}
			}
			topology := MergeDeviceTopologies(topologyMap)
			for id := range merged {
				if _, present := topology.Devices[id]; present {
					continue
				}
				if retained := generated[id]; retained != nil {
					merged[id] = retained
				} else {
					delete(merged, id)
				}
			}
		}
		candidate[resourceName] = merged
	}
	retainResourceGroupDevices(candidate, entries, registry)
	return candidate, nil
}

// topologyStateMatches avoids regenerating all Pod allocations on unchanged
// discovery. Zero-capacity devices are valid configuration, not evidence of
// missing state; a disappeared allocated ID must remain a zero-capacity entry.
func topologyStateMatches(current AllocationMap, entries PodEntries, topology *machine.DeviceTopology, capacity float64) bool {
	if current == nil {
		return false
	}
	for id := range topology.Devices {
		old := current[id]
		if old == nil || (old.Allocatable == 0 && capacity != 0) {
			return false
		}
	}
	for id, old := range current {
		if _, present := topology.Devices[id]; present {
			continue
		}
		if old == nil || old.Allocatable != 0 || len(old.PodEntries) == 0 {
			return false
		}
	}
	for _, containers := range entries {
		for container, info := range containers {
			if container == "" || info == nil {
				continue
			}
			for id := range info.TopologyAwareAllocations {
				if current[id] == nil {
					return false
				}
			}
		}
	}
	return true
}

type frozenResourceStateGenerator struct {
	defaults AllocationMap
	mustInit bool
}

func (g *frozenResourceStateGenerator) MustInitDefaultResourceState() bool { return g.mustInit }
func (g *frozenResourceStateGenerator) GenerateDefaultResourceState() (AllocationMap, error) {
	return g.defaults.Clone(), nil
}

func retainAllocatedDevices(machineState AllocationMap, podEntries PodEntries) {
	for _, containers := range podEntries {
		for container, info := range containers {
			if container == "" || info == nil {
				continue
			}
			for id := range info.TopologyAwareAllocations {
				if machineState[id] == nil {
					machineState[id] = &AllocationState{PodEntries: make(PodEntries)}
				}
			}
		}
	}
}

func defaultStateForTopology(topology *machine.DeviceTopology, allocatable float64) AllocationMap {
	result := make(AllocationMap)
	for id := range topology.Devices {
		result[id] = &AllocationState{Allocatable: allocatable, PodEntries: make(PodEntries)}
	}
	return result
}
