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

package machine

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculatePerNUMAHardReclaimTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		capacity          int
		ratio             float64
		minimum           int
		configuredReserve int
		want              int
		wantErr           string
	}{
		{
			name:     "floor ratio then align down to even",
			capacity: 32,
			ratio:    0.2,
			minimum:  2,
			want:     6,
		},
		{
			name:     "heterogeneous smaller NUMA",
			capacity: 24,
			ratio:    0.2,
			minimum:  2,
			want:     4,
		},
		{
			name:     "maximum int capacity at full ratio",
			capacity: math.MaxInt,
			ratio:    1,
			want:     math.MaxInt,
		},
		{
			name:              "configured reserve wins",
			capacity:          32,
			ratio:             0.2,
			minimum:           2,
			configuredReserve: 8,
			want:              8,
		},
		{
			name:     "minimum wins",
			capacity: 8,
			ratio:    0,
			minimum:  2,
			want:     2,
		},
		{
			name:     "negative ratio is invalid",
			capacity: 32,
			ratio:    -0.1,
			minimum:  2,
			wantErr:  "ratio must be within [0,1], got -0.1",
		},
		{
			name:     "ratio above one is invalid",
			capacity: 32,
			ratio:    1.1,
			minimum:  2,
			wantErr:  "ratio must be within [0,1], got 1.1",
		},
		{
			name:     "NaN ratio is invalid",
			capacity: 32,
			ratio:    math.NaN(),
			minimum:  2,
			wantErr:  "ratio must be within [0,1], got NaN",
		},
		{
			name:     "positive infinity ratio is invalid",
			capacity: 32,
			ratio:    math.Inf(1),
			minimum:  2,
			wantErr:  "ratio must be within [0,1], got +Inf",
		},
		{
			name:     "negative infinity ratio is invalid",
			capacity: 32,
			ratio:    math.Inf(-1),
			minimum:  2,
			wantErr:  "ratio must be within [0,1], got -Inf",
		},
		{
			name:              "target cannot exceed capacity",
			capacity:          4,
			ratio:             0.2,
			minimum:           2,
			configuredReserve: 6,
			wantErr:           "hard reclaim target 6 exceeds NUMA capacity 4",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CalculatePerNUMAHardReclaimTarget(
				tt.capacity, tt.ratio, tt.minimum, tt.configuredReserve)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDistributeConfiguredHardReclaimFloorRespectsNUMACapacity(t *testing.T) {
	t.Parallel()

	got, err := DistributeConfiguredHardReclaimFloor(
		map[int]int{0: 24, 1: 32},
		map[int]int{0: 2, 1: 2},
		50,
	)
	require.NoError(t, err)
	require.Equal(t, 50, got[0]+got[1])
	require.LessOrEqual(t, got[0], 24)
	require.LessOrEqual(t, got[1], 32)
	require.Equal(t, map[int]int{0: 24, 1: 26}, got)
}
