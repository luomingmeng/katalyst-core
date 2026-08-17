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
	"fmt"
	"math"
)

// CalculatePerNUMAHardReclaimTarget derives a stable reclaim target from the
// immutable CPU capacity of one NUMA node.
func CalculatePerNUMAHardReclaimTarget(capacity int, ratio float64, minimum, configuredReserve int) (int, error) {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
		return 0, fmt.Errorf("ratio must be within [0,1], got %v", ratio)
	}

	target := capacity
	if ratio < 1 {
		target = int(math.Floor(float64(capacity) * ratio))
		target -= target % 2
	}
	if target < minimum {
		target = minimum
	}
	if target < configuredReserve {
		target = configuredReserve
	}
	if target > capacity {
		return 0, fmt.Errorf("hard reclaim target %d exceeds NUMA capacity %d", target, capacity)
	}
	return target, nil
}
