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
	"sort"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// MergeDeviceTopologies unions the latest snapshot of each configured name.
// Absence in one name does not imply absence in another. For overlapping IDs,
// the newest snapshot wins; equal timestamps use lexical name order.
func MergeDeviceTopologies(topologies map[string]*machine.DeviceTopology) *machine.DeviceTopology {
	names := make([]string, 0, len(topologies))
	for name, t := range topologies {
		if t != nil {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool {
		a, b := topologies[names[i]], topologies[names[j]]
		if a.UpdateTime == b.UpdateTime {
			return names[i] < names[j]
		}
		return a.UpdateTime < b.UpdateTime
	})
	if len(names) == 0 {
		return nil
	}
	result := &machine.DeviceTopology{Devices: make(map[string]machine.DeviceInfo)}
	for _, name := range names {
		t := topologies[name].Clone()
		result.UpdateTime = t.UpdateTime
		result.PriorityDimensions = t.PriorityDimensions
		for id, info := range t.Devices {
			result.Devices[id] = info
		}
	}
	return result
}

func GetMergedDeviceTopology(registry *machine.DeviceTopologyRegistry, names []string) (*machine.DeviceTopology, error) {
	if registry == nil {
		return nil, fmt.Errorf("nil device topology registry")
	}
	topologies, _ := registry.GetDeviceTopologies(names)
	result := MergeDeviceTopologies(topologies)
	if result == nil {
		return nil, fmt.Errorf("no device topology for %v", names)
	}
	return result, nil
}
