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

	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

const (
	MetricNameDeviceTopologyValidationFailed = "device_topology_validation_failed"
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
// and sets PriorityDimensions of the DeviceTopology based on the total number of unique values
// for each dimension key.
func generateAndSetDeviceAffinity(provider DeviceAffinityProvider, topology *DeviceTopology, emitter metrics.MetricEmitter) {
	if provider != nil {
		provider.SetDeviceAffinity(topology)
	}

	if topology == nil || len(topology.Devices) == 0 {
		return
	}

	generateDynamicPriorityDimensions(topology, emitter)
}

// generateDynamicPriorityDimensions calculates PriorityDimensions based on the total number
// of unique values for each dimension key across all devices in descending order.
// To ensure the dimension topology is symmetric and well-formed, we iteratively build
// composite keys (from highest priority down to lowest) and verify that the devices
// are perfectly partitioned at each level.
func generateDynamicPriorityDimensions(topology *DeviceTopology, emitter metrics.MetricEmitter) {
	dimCounts := getSortedDimensionCounts(topology)
	if dimCounts == nil {
		return
	}

	if !validateTopologySymmetry(topology, dimCounts, emitter) {
		return
	}

	priorityDimensions := make([]string, 0, len(dimCounts))
	for _, dc := range dimCounts {
		priorityDimensions = append(priorityDimensions, dc.key)
	}

	topology.PriorityDimensions = priorityDimensions
	general.Infof("successfully set priority dimensions to %v", priorityDimensions)
}

type dimCount struct {
	key   string
	count int
}

// getSortedDimensionCounts collates unique dimension values and sorts them in ascending order of unique counts.
func getSortedDimensionCounts(topology *DeviceTopology) []dimCount {
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

	var dimCounts []dimCount
	for key, values := range dimensionValuesMap {
		dimCounts = append(dimCounts, dimCount{
			key:   key,
			count: values.Len(),
		})
	}

	// Sort by count descending, then by key ascending as tie-breaker
	sort.Slice(dimCounts, func(i, j int) bool {
		if dimCounts[i].count == dimCounts[j].count {
			return dimCounts[i].key < dimCounts[j].key
		}
		return dimCounts[i].count > dimCounts[j].count
	})

	return dimCounts
}

// validateTopologySymmetry ensures perfect symmetry by using composite dimension values for hashing to partition devices.
// It iterates backwards through the descending-sorted dimensions (from fewest unique values to most)
// and groups devices by their composite dimension values.
// At each dimension level, all subsets must have the exact same number of devices.
func validateTopologySymmetry(topology *DeviceTopology, dimCounts []dimCount, emitter metrics.MetricEmitter) bool {
	// We build a composite key string for each device as we iterate through the dimensions.
	// For example, if dimCounts is [core, numa, socket] (descending), at level 1 the key is "socketVal".
	// At level 2 the key is "socketVal-numaVal".
	deviceCompositeKeys := make(map[string]string)
	for devID := range topology.Devices {
		deviceCompositeKeys[devID] = ""
	}

	// We iterate backwards because validation MUST be top-down (fewest unique values -> most unique values).
	// If we validated bottom-up, lower levels (e.g. core) would naturally have a group size of 1,
	// which falsely passes validation and completely misses higher-level asymmetric imbalances
	// (e.g. socket0 having 2 NUMA nodes while socket1 has 1 NUMA node).
	for i := len(dimCounts) - 1; i >= 0; i-- {
		dimKey := dimCounts[i].key
		groupCounts := make(map[string]int)

		for devID, deviceInfo := range topology.Devices {
			dimVal, ok := deviceInfo.Dimensions[dimKey]
			if !ok {
				// If a device is missing a dimension entirely, it's malformed
				general.Errorf("Validation failed: device %s is missing dimension %s", devID, dimKey)
				if emitter != nil {
					_ = emitter.StoreInt64(MetricNameDeviceTopologyValidationFailed, 1, metrics.MetricTypeNameCount, metrics.MetricTag{Key: "reason", Val: "missing_dimension"})
				}
				return false
			}

			// Append current dimension to the composite key
			newKey := deviceCompositeKeys[devID] + "-" + dimVal
			deviceCompositeKeys[devID] = newKey

			groupCounts[newKey]++
		}

		// Verify that all groups at this level have the exact same number of devices.
		expectedCount := -1

		for groupKey, count := range groupCounts {
			if expectedCount == -1 {
				expectedCount = count
			} else if count != expectedCount {
				general.Errorf("Validation failed: asymmetric topology at dimension %s (group %s has %d devices, expected %d)",
					dimKey, groupKey, count, expectedCount)
				if emitter != nil {
					_ = emitter.StoreInt64(MetricNameDeviceTopologyValidationFailed, 1, metrics.MetricTypeNameCount, metrics.MetricTag{Key: "reason", Val: "asymmetric_topology"})
				}
				return false
			}
		}
	}

	return true
}
