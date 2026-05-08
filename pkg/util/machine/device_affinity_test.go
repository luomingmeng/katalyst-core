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
					"dev6": {
						Dimensions: DeviceDimensions{"dimA": "1", "dimB": "3", "dimC": "6"},
					},
				},
			},
			// "dimA" has 1 unique value ("1")
			// "dimB" has 3 unique values ("1", "2", "3")
			// "dimC" has 6 unique values ("1", "2", "3", "4", "5", "6")
			// Multiples: 6 % 3 == 0, 3 % 1 == 0
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
			// Multiples check fails: 5 % 3 != 0
			want: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := generateDynamicPriorityDimensions(tt.topology); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("generateDynamicPriorityDimensions() = %v, want %v", got, tt.want)
			}
		})
	}
}
