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

package machine

import (
	"sort"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

// DeviceAffinityProvider knows how to form affinity between devices
type DeviceAffinityProvider interface {
	// SetDeviceAffinity modifies DeviceTopology by retrieving each device's affinity to other devices
	SetDeviceAffinity(*DeviceTopology)

	// WatchTopologyChanged monitors topology and returns a channel.
	// The channel is notified when the topology has changed.
	// Stops when stopCh is closed.
	WatchTopologyChanged(stopCh <-chan struct{}) <-chan struct{}
}

// generateAndSetDeviceAffinity is a common method that accepts a DeviceAffinityProvider and DeviceTopology.
// It first calls the provider's SetDeviceAffinity method, and then dynamically calculates
// and overrides PriorityDimensions of the DeviceTopology based on the total number of unique values
// for each dimension key.
func generateAndSetDeviceAffinity(provider DeviceAffinityProvider, topology *DeviceTopology) {
	if provider != nil {
		provider.SetDeviceAffinity(topology)
	}

	if topology == nil || len(topology.Devices) == 0 {
		return
	}

	topology.PriorityDimensions = generateDynamicPriorityDimensions(topology)
}

// generateDynamicPriorityDimensions calculates PriorityDimensions based on the total number
// of unique values for each dimension key across all devices in descending order.
// A dimension with a higher priority (fewer unique values initially, sorted to the end)
// must completely cover the lower priority dimensions. Therefore, we validate that the counts
// of unique values are strict multiples of each other.
func generateDynamicPriorityDimensions(topology *DeviceTopology) []string {
	// Collate unique values for each dimension key across all devices
	dimensionValuesMap := make(map[string]sets.String)
	for _, deviceInfo := range topology.Devices {
		for key, val := range deviceInfo.Dimensions {
			if _, ok := dimensionValuesMap[key]; !ok {
				dimensionValuesMap[key] = sets.NewString()
			}
			dimensionValuesMap[key].Insert(val)
		}
	}

	if len(dimensionValuesMap) == 0 {
		return nil
	}

	// Calculate counts for sorting
	type dimCount struct {
		key   string
		count int
	}

	var dimCounts []dimCount
	for key, values := range dimensionValuesMap {
		dimCounts = append(dimCounts, dimCount{
			key:   key,
			count: values.Len(),
		})
	}

	// Sort by count descending, then by key ascending (as tie-breaker)
	sort.Slice(dimCounts, func(i, j int) bool {
		if dimCounts[i].count == dimCounts[j].count {
			return dimCounts[i].key < dimCounts[j].key
		}
		return dimCounts[i].count > dimCounts[j].count
	})

	// Validate that counts are in multiples of each other
	for i := 0; i < len(dimCounts)-1; i++ {
		if dimCounts[i+1].count == 0 || dimCounts[i].count%dimCounts[i+1].count != 0 {
			general.Errorf("Validation failed for dynamic priority dimensions: dimension %s count (%d) is not a multiple of dimension %s count (%d)",
				dimCounts[i].key, dimCounts[i].count, dimCounts[i+1].key, dimCounts[i+1].count)
			return nil
		}
	}

	// Override PriorityDimensions
	priorityDimensions := make([]string, 0, len(dimCounts))
	for _, dc := range dimCounts {
		priorityDimensions = append(priorityDimensions, dc.key)
	}

	return priorityDimensions
}
