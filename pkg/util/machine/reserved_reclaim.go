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

	v1 "k8s.io/api/core/v1"

	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
)

// extractReclaimReservationKnobs unpacks the three reclaim reservation knobs
// from the dynamic configuration so every reclaim path decodes them identically:
//
//   - numaReservedRatio: NumaMinReclaimedResourceRatioForAllocate, gated to
//     activate only when its integer-rounded value is non-zero;
//   - numaReservedFloor: NumaMinReclaimedResourceForAllocate;
//   - globalReservedCores: MinReclaimedResourceForAllocate, defaulting to
//     globalFallback when the key is absent.
//
// A nil conf yields (0, 0, globalFallback).
func extractReclaimReservationKnobs(
	conf *dynamicconfig.Configuration,
	globalFallback int,
) (numaReservedRatio float64, numaReservedFloor int, globalReservedCores int) {
	globalReservedCores = globalFallback
	if conf == nil {
		return 0, 0, globalReservedCores
	}

	numaReservedRatioQuantity := conf.NumaMinReclaimedResourceRatioForAllocate[v1.ResourceCPU]
	// keep the historical enablement gate: the NUMA ratio path activates only
	// when its integer-rounded value is non-zero.
	if numaReservedRatioQuantity.Value() != 0 {
		numaReservedRatio = numaReservedRatioQuantity.AsApproximateFloat64()
	}

	numaReservedQuantity := conf.NumaMinReclaimedResourceForAllocate[v1.ResourceCPU]
	numaReservedFloor = int(numaReservedQuantity.AsApproximateFloat64())

	if quantity, ok := conf.MinReclaimedResourceForAllocate[v1.ResourceCPU]; ok {
		globalReservedCores = int(quantity.Value())
	}
	return numaReservedRatio, numaReservedFloor, globalReservedCores
}

// ResolvePerNUMAReservedForReclaim resolves the per-NUMA reserved-for-reclaim
// map on the regular (non hard-partition) reclaim path directly from the
// dynamic configuration and CPU topology, applying the unified three-state
// override semantics so every caller stays aligned:
//
//  1. when NumaMinReclaimedResourceRatioForAllocate is enabled (its
//     integer-rounded value is non-zero), each NUMA reserves
//     max(ceil(ratio*numaCPUSize), NumaMinReclaimedResourceForAllocate);
//  2. otherwise fall back to MinReclaimedResourceForAllocate, clamped to
//     NumCPUs and lifted to at least NumNUMANodes, then spread evenly across
//     NUMA nodes via GetCoreNumReservedForReclaim.
//
// The topology owns both the NUMA capacity view (NUMAToCPUs) and the node
// counts, so this stays byte-for-byte aligned with the hard-partition helpers
// that also derive everything from conf + topology. A nil topology yields an
// empty map and a nil conf resolves to a zero reserve.
func ResolvePerNUMAReservedForReclaim(
	conf *dynamicconfig.Configuration,
	topology *CPUTopology,
) map[int]int {
	if topology == nil {
		return map[int]int{}
	}

	numaReservedRatio, numaReservedFloor, globalReservedCores := extractReclaimReservationKnobs(conf, 0)
	numCPUs := topology.NumCPUs
	numNUMANodes := topology.NumNUMANodes
	numaCPUSize := func(numaID int) int { return topology.NUMAToCPUs.CPUSizeInNUMAs(numaID) }

	if numaReservedRatio > 0 {
		reservedForReclaim := make(map[int]int, numNUMANodes)
		for id := 0; id < numNUMANodes; id++ {
			size := 0
			if numaCPUSize != nil {
				size = numaCPUSize(id)
			}
			reserved := math.Ceil(numaReservedRatio * float64(size))
			reservedForReclaim[id] = int(math.Max(float64(numaReservedFloor), reserved))
		}
		return reservedForReclaim
	}

	coreNumReservedForReclaim := globalReservedCores
	if coreNumReservedForReclaim > numCPUs {
		coreNumReservedForReclaim = numCPUs
	}
	if coreNumReservedForReclaim < numNUMANodes {
		coreNumReservedForReclaim = numNUMANodes
	}
	return GetCoreNumReservedForReclaim(coreNumReservedForReclaim, numNUMANodes)
}

// ResolveConfiguredReclaimFloor resolves the scalar total reserved-for-reclaim
// floor consumed by the hard-partition path (fed into
// DistributeConfiguredHardReclaimFloor). It applies the same three-state
// override semantics as ResolvePerNUMAReservedForReclaim so both paths stay
// aligned:
//
//  1. when numaReservedRatio > 0, the floor is the sum of the per-NUMA
//     reservations max(ceil(ratio*numaCPUSize), numaReservedFloor);
//  2. otherwise fall back to the raw global reserved cores.
//
// The hard-partition path keeps its own per-NUMA ramp-up ratio and minimum
// semantics; this helper only unifies how the configured floor is derived from
// the reclaim reservation knobs. numaCPUSize returns the CPU capacity of a
// given NUMA id and numNUMANodes must match the range fed to it.
func ResolveConfiguredReclaimFloor(
	numaReservedRatio float64,
	numaReservedFloor int,
	globalReservedCores int,
	numNUMANodes int,
	numaCPUSize func(numaID int) int,
) int {
	if numaReservedRatio > 0 {
		total := 0
		for id := 0; id < numNUMANodes; id++ {
			size := 0
			if numaCPUSize != nil {
				size = numaCPUSize(id)
			}
			reserved := math.Ceil(numaReservedRatio * float64(size))
			total += int(math.Max(float64(numaReservedFloor), reserved))
		}
		return total
	}

	return globalReservedCores
}

// hardPartitionReclaimBaselinePerNUMA is the immutable minimum reclaim floor a
// single NUMA node always keeps on the hard-partition path.
const hardPartitionReclaimBaselinePerNUMA = 2

// ResolveConfiguredReclaimFloorFromConfig resolves the scalar total
// reserved-for-reclaim floor consumed by the hard-partition path directly from
// the dynamic configuration and CPU topology. It is a config-aware wrapper over
// ResolveConfiguredReclaimFloor so every hard-partition caller derives the
// configured floor identically: NumaMinReclaimedResourceRatioForAllocate (when
// its integer-rounded value is non-zero) takes precedence over the global
// MinReclaimedResourceForAllocate scalar, which itself falls back to
// globalReservedFallback when the key is absent.
func ResolveConfiguredReclaimFloorFromConfig(
	conf *dynamicconfig.Configuration,
	topology *CPUTopology,
	globalReservedFallback int,
) int {
	if conf == nil || topology == nil {
		return globalReservedFallback
	}

	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()

	numaReservedRatio, numaReservedFloor, globalReserved := extractReclaimReservationKnobs(conf, globalReservedFallback)

	return ResolveConfiguredReclaimFloor(
		numaReservedRatio,
		numaReservedFloor,
		globalReserved,
		len(numaIDs),
		func(numaID int) int { return topology.CPUDetails.CPUsInNUMANodes(numaID).Size() },
	)
}

// ResolveHardPartitionReclaimTargets encapsulates the full hard-partition
// per-NUMA reclaim target derivation shared by QRM, bulkhead and sysadvisor so
// all three stay byte-for-byte aligned. It:
//
//  1. derives the configured global floor via
//     ResolveConfiguredReclaimFloorFromConfig (three-state override semantics);
//  2. computes each NUMA's stable baseline with CalculatePerNUMAHardReclaimTarget
//     using InitialRampUpReclaimCPUSetRatio, the always-on per-NUMA minimum, and
//     the optional caller-provided per-NUMA reserved floor;
//  3. lifts the baselines to meet the configured floor without exceeding any
//     NUMA capacity via DistributeConfiguredHardReclaimFloor.
//
// perNUMAReservedFloor is optional; when nil, no extra per-NUMA reserve is
// applied. This subsumes the previous ad-hoc "lift the configured reserve to at
// least numaCount*2" step, which is redundant because the per-NUMA baseline
// already guarantees a total of at least hardPartitionReclaimBaselinePerNUMA per
// NUMA and the distribution never lowers it.
func ResolveHardPartitionReclaimTargets(
	conf *dynamicconfig.Configuration,
	topology *CPUTopology,
	globalReservedFallback int,
	perNUMAReservedFloor func(numaID int) int,
) (map[int]int, error) {
	if topology == nil {
		return nil, fmt.Errorf("resolve hard-partition reclaim targets: nil topology")
	}

	ratio := 0.0
	if conf != nil {
		ratio = conf.InitialRampUpReclaimCPUSetRatio
	}
	configuredFloor := ResolveConfiguredReclaimFloorFromConfig(conf, topology, globalReservedFallback)

	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	capacityByNUMA := make(map[int]int, len(numaIDs))
	baselineByNUMA := make(map[int]int, len(numaIDs))
	for _, numaID := range numaIDs {
		capacity := topology.CPUDetails.CPUsInNUMANodes(numaID).Size()
		capacityByNUMA[numaID] = capacity
		reservedFloor := 0
		if perNUMAReservedFloor != nil {
			reservedFloor = perNUMAReservedFloor(numaID)
		}
		target, err := CalculatePerNUMAHardReclaimTarget(
			capacity, ratio, hardPartitionReclaimBaselinePerNUMA, reservedFloor)
		if err != nil {
			return nil, fmt.Errorf("calculate hard-partition reclaim target for NUMA %d: %w", numaID, err)
		}
		baselineByNUMA[numaID] = target
	}

	targets, err := DistributeConfiguredHardReclaimFloor(capacityByNUMA, baselineByNUMA, configuredFloor)
	if err != nil {
		return nil, fmt.Errorf("distribute configured hard-partition reclaim floor: %w", err)
	}
	return targets, nil
}
