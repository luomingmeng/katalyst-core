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
	"reflect"
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/metrics"
)

func TestGenerateDynamicPriorityDimensions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		topology *DeviceTopology
		want     []string
	}{
		{
			name: "empty topology",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{},
			},
			want: nil,
		},
		{
			name: "topology with no dimensions",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {
						Dimensions: DeviceDimensions{},
					},
				},
			},
			want: nil,
		},
		{
			name: "single dimension",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {
						Dimensions: DeviceDimensions{"numa": "0"},
					},
					"dev2": {
						Dimensions: DeviceDimensions{"numa": "1"},
					},
				},
			},
			want: []string{"numa"},
		},
		{
			name: "multiple dimensions with different unique counts",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {
						Dimensions: DeviceDimensions{"socket": "0", "numa": "0"},
					},
					"dev2": {
						Dimensions: DeviceDimensions{"socket": "0", "numa": "1"},
					},
					"dev3": {
						Dimensions: DeviceDimensions{"socket": "1", "numa": "2"},
					},
					"dev4": {
						Dimensions: DeviceDimensions{"socket": "1", "numa": "3"},
					},
				},
			},
			// "socket" has 2 unique values ("0", "1")
			// "numa" has 4 unique values ("0", "1", "2", "3")
			// Descending order: "numa" (4), "socket" (2)
			want: []string{"numa", "socket"},
		},
		{
			name: "multiple dimensions with tie-breaker",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {
						Dimensions: DeviceDimensions{"a": "val1", "b": "val2", "c": "val3"},
					},
					"dev2": {
						Dimensions: DeviceDimensions{"a": "val4", "b": "val5", "c": "val6"},
					},
				},
			},
			// All have 2 unique values.
			// Sorted alphabetically: "a", "b", "c"
			want: []string{"a", "b", "c"},
		},
		{
			name: "three dimensions with different unique counts",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "1", "dimC": "1"},
					},
					"dev2": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "1", "dimC": "2"},
					},
					"dev3": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "2", "dimC": "3"},
					},
					"dev4": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "2", "dimC": "4"},
					},
					"dev5": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "3", "dimC": "5"},
					},
					"dev6": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "3", "dimC": "6"},
					},
				},
			},
			// "dimA" has 1 unique value ("1")
			// "dimB" has 3 unique values ("1", "2", "3")
			// "dimC" has 6 unique values ("1", "2", "3", "4", "5", "6")
			// All nodes at dimB level have 2 deviceIDs, all nodes at dimC level have 1 deviceID
			// Descending order: "dimC" (6), "dimB" (3), "dimA" (1)
			want: []string{"dimC", "dimB", "dimA"},
		},
		{
			name: "invalid multiples",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "1", "dimC": "1"},
					},
					"dev2": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "2", "dimC": "2"},
					},
					"dev3": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "3", "dimC": "3"},
					},
					"dev4": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "3", "dimC": "4"},
					},
					"dev5": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "3", "dimC": "5"},
					},
				},
			},
			// "dimA" has 1 unique value ("1")
			// "dimB" has 3 unique values ("1", "2", "3")
			// "dimC" has 5 unique values ("1", "2", "3", "4", "5")
			// Multiples check fails (specifically tree validation: child node device count not equal across the level)
			want: nil,
		},
		{
			name: "asymmetric topology but valid multiples",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {
						Dimensions: DeviceDimensions{"socket": "0", "numa": "0"},
					},
					"dev2": {
						Dimensions: DeviceDimensions{"socket": "0", "numa": "0"},
					},
					"dev3": {
						Dimensions: DeviceDimensions{"socket": "1", "numa": "1"},
					},
					"dev4": {
						Dimensions: DeviceDimensions{"socket": "1", "numa": "2"},
					},
					"dev5": {
						Dimensions: DeviceDimensions{"socket": "1", "numa": "3"},
					},
				},
			},
			// "socket" has 2 unique values ("0", "1")
			// "numa" has 4 unique values ("0", "1", "2", "3")
			// 4 % 2 == 0, so it passed the old validation.
			// However, numa 0 has 2 devices. numa 1, 2, 3 have 1 device each.
			// The tree validation should fail.
			want: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			generateDynamicPriorityDimensions(tt.topology, metrics.DummyMetrics{})
			if !reflect.DeepEqual(tt.topology.PriorityDimensions, tt.want) {
				t.Errorf("generateDynamicPriorityDimensions() resulted in %v, want %v", tt.topology.PriorityDimensions, tt.want)
			}
		})
	}
}

func TestValidateTopologySymmetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topology  *DeviceTopology
		dimCounts []dimCount
		want      bool
	}{
		{
			name: "valid symmetric topology",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {Dimensions: DeviceDimensions{"socket": "0", "numa": "0"}},
					"dev2": {Dimensions: DeviceDimensions{"socket": "0", "numa": "1"}},
					"dev3": {Dimensions: DeviceDimensions{"socket": "1", "numa": "2"}},
					"dev4": {Dimensions: DeviceDimensions{"socket": "1", "numa": "3"}},
				},
			},
			dimCounts: []dimCount{
				{key: "numa", count: 4},
				{key: "socket", count: 2},
			},
			want: true,
		},
		{
			name: "missing dimension",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {Dimensions: DeviceDimensions{"socket": "0", "numa": "0"}},
					"dev2": {Dimensions: DeviceDimensions{"socket": "0"}}, // missing numa
				},
			},
			dimCounts: []dimCount{
				{key: "numa", count: 2},
				{key: "socket", count: 1},
			},
			want: false,
		},
		{
			name: "asymmetric topology - different sizes at top level",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {Dimensions: DeviceDimensions{"socket": "0"}},
					"dev2": {Dimensions: DeviceDimensions{"socket": "0"}},
					"dev3": {Dimensions: DeviceDimensions{"socket": "1"}},
				},
			},
			dimCounts: []dimCount{
				{key: "socket", count: 2},
			},
			want: false, // socket 0 has 2, socket 1 has 1
		},
		{
			name: "asymmetric topology - different sizes at lower level",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {Dimensions: DeviceDimensions{"socket": "0", "numa": "0"}},
					"dev2": {Dimensions: DeviceDimensions{"socket": "0", "numa": "0"}},
					"dev3": {Dimensions: DeviceDimensions{"socket": "1", "numa": "1"}},
					"dev4": {Dimensions: DeviceDimensions{"socket": "1", "numa": "2"}},
				},
			},
			dimCounts: []dimCount{
				{key: "numa", count: 3},
				{key: "socket", count: 2},
			},
			want: false, // numa 0 has 2 devices. numa 1 and numa 2 have 1 device each.
		},
		{
			name: "valid 3-dimensional symmetric topology",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "0"}},
					"dev2": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "0"}},
					"dev3": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "1"}},
					"dev4": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "1"}},
					"dev5": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "2"}},
					"dev6": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "2"}},
					"dev7": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "3"}},
					"dev8": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "3"}},
				},
			},
			dimCounts: []dimCount{
				{key: "numa", count: 4},
				{key: "socket", count: 2},
				{key: "node", count: 1},
			},
			want: true,
		},
		{
			name: "invalid 3-dimensional topology - asymmetric at the middle level",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "0"}},
					"dev2": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "0"}},
					"dev3": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "1"}},
					"dev4": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "1"}},
					"dev5": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "2"}},
					"dev6": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "2"}},
				},
			},
			dimCounts: []dimCount{
				{key: "numa", count: 3},
				{key: "socket", count: 2},
				{key: "node", count: 1},
			},
			// node0-socket0 has 4 devices, node0-socket1 has 2 devices
			want: false,
		},
		{
			name: "invalid 3-dimensional topology - asymmetric at the lowest level",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					"dev1": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "0"}},
					"dev2": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "0"}},
					"dev3": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "0"}}, // 3 devices in numa 0
					"dev4": {Dimensions: DeviceDimensions{"node": "0", "socket": "0", "numa": "1"}}, // 1 device in numa 1
					"dev5": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "2"}},
					"dev6": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "2"}},
					"dev7": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "3"}},
					"dev8": {Dimensions: DeviceDimensions{"node": "0", "socket": "1", "numa": "3"}},
				},
			},
			dimCounts: []dimCount{
				{key: "numa", count: 4},
				{key: "socket", count: 2},
				{key: "node", count: 1},
			},
			// node0-socket0-numa0 has 3, node0-socket0-numa1 has 1
			want: false,
		},
		{
			name: "cross-level mismatch - 2 groups of 3 and 3 groups of 2",
			topology: &DeviceTopology{
				Devices: map[string]DeviceInfo{
					// dim1=0 has 3 devices (dev1, dev2, dev3)
					// dim1=1 has 3 devices (dev4, dev5, dev6)
					// dim2=0 has 2 devices (dev1, dev2)
					// dim2=1 has 2 devices (dev3, dev4)
					// dim2=2 has 2 devices (dev5, dev6)
					"dev1": {Dimensions: DeviceDimensions{"dim1": "0", "dim2": "0"}},
					"dev2": {Dimensions: DeviceDimensions{"dim1": "0", "dim2": "0"}},
					"dev3": {Dimensions: DeviceDimensions{"dim1": "0", "dim2": "1"}},
					"dev4": {Dimensions: DeviceDimensions{"dim1": "1", "dim2": "1"}},
					"dev5": {Dimensions: DeviceDimensions{"dim1": "1", "dim2": "2"}},
					"dev6": {Dimensions: DeviceDimensions{"dim1": "1", "dim2": "2"}},
				},
			},
			dimCounts: []dimCount{
				{key: "dim2", count: 3},
				{key: "dim1", count: 2},
			},
			// group sizes at dim1-dim2 level are 2, 1, 1, 2. Not all equal, so it fails.
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := validateTopologySymmetry(tt.topology, tt.dimCounts, metrics.DummyMetrics{}); got != tt.want {
				t.Errorf("validateTopologySymmetry() = %v, want %v", got, tt.want)
			}
		})
	}
}
