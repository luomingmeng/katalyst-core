package reclaim

import (
	"fmt"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
)

// ScaleNUMAHeadroom returns a scaled copy of per-NUMA headroom values.
// percentage must be non-negative; unitScale converts the input unit to the
// caller's output unit, such as cores to milliCPU.
func ScaleNUMAHeadroom(headroom map[int]float64, percentage, unitScale float64) map[int]float64 {
	if percentage < 0 || unitScale < 0 {
		return nil
	}
	scaled := make(map[int]float64, len(headroom))
	for numaID, value := range headroom {
		scaled[numaID] = value * unitScale * percentage / 100
	}
	return scaled
}

// GetReclaimedNUMAHeadroom returns the per-NUMA headroom attributable to the
// configured consumers. The result uses the same unit as headroom.
func GetReclaimedNUMAHeadroom(
	headroom map[int]float64,
	dynamicConfig *dynamic.Configuration,
	consumerNames ...string,
) map[int]float64 {
	percentage := GetSummedReclaimedPercentage(dynamicConfig, consumerNames)
	return ScaleNUMAHeadroom(headroom, percentage, 1)
}

// ValidateNUMAHeadroom verifies that all resource headroom maps are non-empty,
// have the same NUMA IDs, and contain no negative values.
func ValidateNUMAHeadroom(headrooms ...map[int]float64) error {
	if len(headrooms) == 0 || len(headrooms[0]) == 0 {
		return fmt.Errorf("NUMA headroom is empty")
	}
	for numaID, value := range headrooms[0] {
		if value < 0 {
			return fmt.Errorf("NUMA headroom is negative for NUMA %d", numaID)
		}
	}
	for _, headroom := range headrooms[1:] {
		if len(headroom) != len(headrooms[0]) {
			return fmt.Errorf("NUMA headroom IDs do not match")
		}
		for numaID, value := range headroom {
			if _, ok := headrooms[0][numaID]; !ok {
				return fmt.Errorf("NUMA headroom is missing NUMA %d", numaID)
			}
			if value < 0 {
				return fmt.Errorf("NUMA headroom is negative for NUMA %d", numaID)
			}
		}
	}
	return nil
}
