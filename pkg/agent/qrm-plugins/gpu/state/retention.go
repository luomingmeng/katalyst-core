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
	v1 "k8s.io/api/core/v1"
)

// retainResourceGroupDevices keeps one physical device entry in every member
// resource while any member still owns an allocation on it. A device deleted
// from one resource view must not become detached from the others.
func retainResourceGroupDevices(machineState AllocationResourcesMap, entries PodResourceEntries, registry *DefaultResourceStateGeneratorRegistry) {
	if machineState == nil || registry == nil {
		return
	}
	for _, members := range registry.GetResourceStateRetentionGroups() {
		allocated := make(map[string]struct{})
		for _, member := range members {
			for _, containers := range entries[v1.ResourceName(member)] {
				for container, info := range containers {
					if container == "" || info == nil {
						continue
					}
					for id := range info.TopologyAwareAllocations {
						allocated[id] = struct{}{}
					}
				}
			}
		}
		if len(allocated) == 0 {
			continue
		}
		for _, member := range members {
			resourceName := v1.ResourceName(member)
			resourceState, exists := machineState[resourceName]
			if !exists {
				// Optional resources are only visible after their first use.
				continue
			}
			if resourceState == nil {
				resourceState = make(AllocationMap)
				machineState[resourceName] = resourceState
			}
			for id := range allocated {
				state, exists := resourceState[id]
				if !exists {
					state = &AllocationState{PodEntries: make(PodEntries)}
					resourceState[id] = state
				}
				if len(state.PodEntries) == 0 {
					// A member without its own accounting still shares the
					// physical device: it is a placeholder, not free capacity.
					state.Allocatable = 0
				}
			}
		}
	}
}
