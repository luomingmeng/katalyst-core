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

package dynamicpolicy

import (
	"fmt"
	"sort"

	"github.com/gogo/protobuf/proto"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

type reclaimBlockTarget struct {
	blockID  string
	numaID   int64
	quantity uint64
}

type reclaimEffectiveTargetPolicy struct {
	acceptedTotal                int
	fixedReserveByNUMA           map[int]machine.CPUSet
	steadyExclusiveReclaimByNUMA map[int]machine.CPUSet
	ignoreDefaultShare           bool
}

// effectiveAdvisorResponseForReclaimTarget returns an immutable terminal view
// of the advisor response whose reclaim blocks sum to acceptedTotal. Real-NUMA
// blocks are preserved before fake-NUMA blocks because the latter have the
// broadest placement freedom. Duplicate references to one block ID are updated
// consistently.
func effectiveAdvisorResponseForReclaimTarget(
	resp *advisorapi.ListAndWatchResponse,
	targetPolicy reclaimEffectiveTargetPolicy,
) (*advisorapi.ListAndWatchResponse, error) {
	advisedTotal, _, err := advisorReclaimPlan(resp, nil)
	if err != nil {
		return nil, err
	}
	acceptedTotal := targetPolicy.acceptedTotal
	if acceptedTotal < 0 || acceptedTotal > advisedTotal {
		return nil, fmt.Errorf(
			"accepted reclaim target %d is outside advisor range [0,%d]",
			acceptedTotal, advisedTotal)
	}
	if acceptedTotal == advisedTotal {
		return resp, nil
	}

	cloned := proto.Clone(resp).(*advisorapi.ListAndWatchResponse)
	reclaimInfo := cloned.Entries[commonstate.PoolNameReclaim].
		Entries[commonstate.FakedContainerName]

	targetByBlockID := make(map[string]reclaimBlockTarget)
	for numaID, numaResult := range reclaimInfo.CalculationResultsByNumas {
		if numaResult == nil {
			continue
		}
		for _, block := range numaResult.Blocks {
			if block == nil {
				continue
			}
			if existing, found := targetByBlockID[block.BlockId]; found {
				if existing.quantity != block.Result {
					return nil, fmt.Errorf(
						"reclaim block %s has inconsistent quantities %d and %d",
						block.BlockId, existing.quantity, block.Result)
				}
				continue
			}
			targetByBlockID[block.BlockId] = reclaimBlockTarget{
				blockID:  block.BlockId,
				numaID:   numaID,
				quantity: block.Result,
			}
		}
	}

	clampView := cloned
	if targetPolicy.ignoreDefaultShare {
		clampView = cloned.WithoutDefaultShareEntry()
	}
	hasNonReclaimOwner := make(map[string]bool, len(targetByBlockID))
	for _, calculationEntries := range clampView.Entries {
		if calculationEntries == nil {
			continue
		}
		for _, calculationInfo := range calculationEntries.Entries {
			if calculationInfo == nil {
				continue
			}
			ownerPoolName, _ := resourcepackage.UnwrapOwnerPoolName(calculationInfo.OwnerPoolName)
			isReclaimOwner := ownerPoolName == commonstate.PoolNameReclaim
			for _, numaResult := range calculationInfo.CalculationResultsByNumas {
				if numaResult == nil {
					continue
				}
				for _, block := range numaResult.Blocks {
					if block == nil {
						continue
					}
					if _, found := targetByBlockID[block.BlockId]; found && !isReclaimOwner {
						hasNonReclaimOwner[block.BlockId] = true
					}
				}
			}
		}
	}

	targets := make([]reclaimBlockTarget, 0, len(targetByBlockID))
	for _, target := range targetByBlockID {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		iFake := targets[i].numaID == commonstate.FakedNUMAID
		jFake := targets[j].numaID == commonstate.FakedNUMAID
		if iFake != jFake {
			return !iFake
		}
		if targets[i].numaID != targets[j].numaID {
			return targets[i].numaID < targets[j].numaID
		}
		return targets[i].blockID < targets[j].blockID
	})

	effectiveQuantityByBlockID := make(map[string]uint64, len(targets))
	remaining := uint64(acceptedTotal)
	fixedByNUMA := make(map[int64]uint64)
	for _, target := range targets {
		if !hasNonReclaimOwner[target.blockID] {
			continue
		}
		if target.quantity > remaining {
			return nil, fmt.Errorf(
				"accepted reclaim target %d is below non-reclaim alias floor %d",
				acceptedTotal, acceptedTotal+int(target.quantity-remaining))
		}
		effectiveQuantityByBlockID[target.blockID] = target.quantity
		remaining -= target.quantity
		if target.numaID != commonstate.FakedNUMAID {
			fixedByNUMA[target.numaID] += target.quantity
		}
	}

	hardFloorByNUMA := make(map[int]machine.CPUSet, len(targetPolicy.fixedReserveByNUMA))
	for numaID, fixed := range targetPolicy.fixedReserveByNUMA {
		hardFloorByNUMA[numaID] = fixed.Clone()
	}
	for numaID, steady := range targetPolicy.steadyExclusiveReclaimByNUMA {
		hardFloorByNUMA[numaID] = hardFloorByNUMA[numaID].Union(steady)
	}
	allocatedByNUMA := make(map[int64]uint64)
	for _, target := range targets {
		if hasNonReclaimOwner[target.blockID] ||
			target.numaID == commonstate.FakedNUMAID {
			continue
		}
		hardFloor := uint64(hardFloorByNUMA[int(target.numaID)].Size())
		if fixedByNUMA[target.numaID]+allocatedByNUMA[target.numaID] >= hardFloor {
			effectiveQuantityByBlockID[target.blockID] = 0
			continue
		}
		quantity := hardFloor - fixedByNUMA[target.numaID] - allocatedByNUMA[target.numaID]
		if quantity > target.quantity {
			quantity = target.quantity
		}
		if quantity > remaining {
			quantity = remaining
		}
		effectiveQuantityByBlockID[target.blockID] = quantity
		remaining -= quantity
		allocatedByNUMA[target.numaID] += quantity
	}
	for _, target := range targets {
		if hasNonReclaimOwner[target.blockID] {
			continue
		}
		quantity := target.quantity - effectiveQuantityByBlockID[target.blockID]
		if quantity > remaining {
			quantity = remaining
		}
		effectiveQuantityByBlockID[target.blockID] += quantity
		remaining -= quantity
	}
	if remaining != 0 {
		return nil, fmt.Errorf(
			"accepted reclaim target %d cannot be materialized from advisor blocks",
			acceptedTotal)
	}

	for _, calculationEntries := range clampView.Entries {
		if calculationEntries == nil {
			continue
		}
		for _, calculationInfo := range calculationEntries.Entries {
			if calculationInfo == nil {
				continue
			}
			for _, numaResult := range calculationInfo.CalculationResultsByNumas {
				if numaResult == nil {
					continue
				}
				for _, block := range numaResult.Blocks {
					if block == nil {
						continue
					}
					if quantity, found := effectiveQuantityByBlockID[block.BlockId]; found {
						block.Result = quantity
					}
				}
			}
		}
	}

	effectiveTotal, _, err := advisorReclaimPlan(cloned, nil)
	if err != nil {
		return nil, err
	}
	if effectiveTotal != acceptedTotal {
		return nil, fmt.Errorf(
			"effective reclaim target mismatch: accepted=%d materialized=%d",
			acceptedTotal, effectiveTotal)
	}
	return cloned, nil
}

func advisorReclaimPlan(
	resp *advisorapi.ListAndWatchResponse,
	blockCPUSet advisorapi.BlockCPUSet,
) (int, machine.CPUSet, error) {
	if resp == nil {
		return 0, machine.NewCPUSet(), fmt.Errorf("advisor response has no reclaim plan")
	}
	reclaimEntries := resp.Entries[commonstate.PoolNameReclaim]
	if reclaimEntries == nil {
		return 0, machine.NewCPUSet(), fmt.Errorf("advisor response has no reclaim plan")
	}
	reclaimInfo := reclaimEntries.Entries[commonstate.FakedContainerName]
	if reclaimInfo == nil || reclaimInfo.OwnerPoolName != commonstate.PoolNameReclaim {
		return 0, machine.NewCPUSet(), fmt.Errorf("advisor response has no reclaim plan")
	}

	planned := machine.NewCPUSet()
	quantityByBlock := make(map[string]int)
	advisedQuantity := 0
	for _, numaResult := range reclaimInfo.CalculationResultsByNumas {
		if numaResult == nil {
			continue
		}
		for _, block := range numaResult.Blocks {
			if block == nil {
				continue
			}
			quantity, err := general.CovertUInt64ToInt(block.Result)
			if err != nil {
				return 0, machine.NewCPUSet(), fmt.Errorf(
					"convert reclaim block %s quantity: %w", block.BlockId, err)
			}
			if previous, found := quantityByBlock[block.BlockId]; found {
				if previous != quantity {
					return 0, machine.NewCPUSet(), fmt.Errorf(
						"reclaim block %s has inconsistent quantities %d and %d",
						block.BlockId, previous, quantity)
				}
				continue
			}
			if blockCPUSet != nil {
				cpuset, found := blockCPUSet[block.BlockId]
				if !found {
					return 0, machine.NewCPUSet(), fmt.Errorf(
						"reclaim planner result missing block %s", block.BlockId)
				}
				if cpuset.Size() != quantity {
					return 0, machine.NewCPUSet(), fmt.Errorf(
						"reclaim block %s quantity mismatch: advised=%d planned=%d",
						block.BlockId, quantity, cpuset.Size())
				}
				planned = planned.Union(cpuset)
			}
			quantityByBlock[block.BlockId] = quantity
			advisedQuantity += quantity
		}
	}
	if len(quantityByBlock) == 0 {
		return 0, machine.NewCPUSet(), fmt.Errorf("advisor response has no reclaim plan")
	}
	if blockCPUSet != nil && planned.Size() != advisedQuantity {
		return 0, machine.NewCPUSet(), fmt.Errorf(
			"advisor reclaim quantity mismatch: advised=%d planned=%d",
			advisedQuantity, planned.Size())
	}
	return advisedQuantity, planned, nil
}

func validateRequiredReclaimFloor(
	resp *advisorapi.ListAndWatchResponse,
	blockCPUSet advisorapi.BlockCPUSet,
	requiredFloor machine.CPUSet,
) error {
	if requiredFloor.IsEmpty() {
		return nil
	}
	_, planned, err := advisorReclaimPlan(resp, blockCPUSet)
	if err != nil {
		return err
	}
	if missing := requiredFloor.Difference(planned); !missing.IsEmpty() {
		return fmt.Errorf(
			"effective reclaim plan dropped required floor CPUs: %s", missing.String())
	}
	return nil
}
