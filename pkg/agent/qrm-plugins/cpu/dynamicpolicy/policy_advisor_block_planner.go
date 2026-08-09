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
	"strings"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

type advisorBlockClass string

const (
	advisorBlockClassStatic           advisorBlockClass = "static"
	advisorBlockClassMandatoryReclaim advisorBlockClass = "mandatory-reclaim"
	advisorBlockClassDedicated        advisorBlockClass = "dedicated"
	advisorBlockClassShared           advisorBlockClass = "shared"
	advisorBlockClassReclaimOverlap   advisorBlockClass = "reclaim-overlap"

	minimumHardReclaimCPUsPerNUMA = 2
)

type advisorBlockDescriptor struct {
	BlockID      string
	Owners       []string
	Class        advisorBlockClass
	NUMAID       int
	Quantity     int
	ComponentKey string
	Eligible     machine.CPUSet
	OldPreferred machine.CPUSet
}

type advisorBlockDescriptorBuilder struct {
	advisorBlockDescriptor
	ownerSeen          map[string]struct{}
	resourcePackage    string
	resourcePackageSet bool
}

func buildAdvisorBlockDescriptors(
	resp *advisorapi.ListAndWatchResponse,
	cpuDetails machine.CPUDetails,
	podEntries state.PodEntries,
	rpPinnedCPUSet map[string]machine.CPUSet,
	nonReclaimableCPUSet machine.CPUSet,
) ([]advisorBlockDescriptor, error) {
	if resp == nil {
		return nil, fmt.Errorf("got nil advisor response")
	}

	allCPUs := cpuDetails.CPUs()
	allPinnedCPUs := machine.NewCPUSet()
	for _, cpus := range rpPinnedCPUSet {
		allPinnedCPUs = allPinnedCPUs.Union(cpus)
	}

	builders := make(map[string]*advisorBlockDescriptorBuilder)
	for entryName, calculationEntries := range resp.Entries {
		if calculationEntries == nil {
			return nil, fmt.Errorf("entry %q has nil calculation entries", entryName)
		}
		for subEntryName, calculationInfo := range calculationEntries.Entries {
			if calculationInfo == nil {
				return nil, fmt.Errorf("entry %q sub-entry %q has nil calculation info", entryName, subEntryName)
			}

			ownerPoolName, ownerResourcePackage := resourcepackage.UnwrapOwnerPoolName(calculationInfo.OwnerPoolName)
			ownerKey := canonicalAdvisorBlockOwner(ownerPoolName, entryName, subEntryName, ownerResourcePackage)
			for numaID64, result := range calculationInfo.CalculationResultsByNumas {
				if result == nil {
					return nil, fmt.Errorf("entry %q sub-entry %q NUMA %d has nil result", entryName, subEntryName, numaID64)
				}
				numaID := int(numaID64)
				if int64(numaID) != numaID64 {
					return nil, fmt.Errorf("NUMA id %d overflows int", numaID64)
				}

				numaCPUs, err := advisorBlockNUMACPUSet(allCPUs, cpuDetails, numaID)
				if err != nil {
					return nil, err
				}
				for _, block := range result.Blocks {
					if block == nil {
						return nil, fmt.Errorf("entry %q sub-entry %q NUMA %d has nil block", entryName, subEntryName, numaID)
					}
					if block.BlockId == "" {
						return nil, fmt.Errorf("entry %q sub-entry %q NUMA %d has empty block id", entryName, subEntryName, numaID)
					}
					quantity, err := uint64ToAdvisorBlockQuantity(block.Result)
					if err != nil {
						return nil, fmt.Errorf("block %q: %w", block.BlockId, err)
					}
					ownerClass, err := classifyAdvisorBlockOwner(ownerPoolName, block.OverlapTargets)
					if err != nil {
						return nil, fmt.Errorf("block %q: %w", block.BlockId, err)
					}
					ownerEligible := advisorBlockOwnerEligible(
						ownerPoolName,
						ownerResourcePackage,
						numaCPUs,
						allPinnedCPUs,
						rpPinnedCPUSet,
						nonReclaimableCPUSet,
					)
					ownerResourcePackage, ownerResourcePackageApplies := advisorBlockOwnerResourcePackageDomain(
						ownerPoolName, ownerResourcePackage,
					)

					builder, found := builders[block.BlockId]
					if !found {
						builder = &advisorBlockDescriptorBuilder{
							advisorBlockDescriptor: advisorBlockDescriptor{
								BlockID:      block.BlockId,
								Class:        ownerClass,
								NUMAID:       numaID,
								Quantity:     quantity,
								Eligible:     ownerEligible.Clone(),
								OldPreferred: machine.NewCPUSet(),
							},
							ownerSeen: make(map[string]struct{}),
						}
						if ownerResourcePackageApplies {
							builder.resourcePackage = ownerResourcePackage
							builder.resourcePackageSet = true
						}
						builders[block.BlockId] = builder
					} else {
						if builder.NUMAID != numaID || builder.Quantity != quantity {
							return nil, fmt.Errorf("block %q aliases disagree on NUMA or quantity", block.BlockId)
						}
						if builder.Class != ownerClass {
							return nil, fmt.Errorf("block %q aliases have incompatible owner classes", block.BlockId)
						}
						if ownerResourcePackageApplies {
							if builder.resourcePackageSet && builder.resourcePackage != ownerResourcePackage {
								return nil, fmt.Errorf("block %q aliases have incompatible resource packages", block.BlockId)
							}
							builder.resourcePackage = ownerResourcePackage
							builder.resourcePackageSet = true
						}
						builder.Eligible = builder.Eligible.Intersection(ownerEligible)
					}

					if _, found := builder.ownerSeen[ownerKey]; !found {
						builder.ownerSeen[ownerKey] = struct{}{}
						builder.Owners = append(builder.Owners, ownerKey)
					}
					builder.OldPreferred = builder.OldPreferred.Union(
						advisorBlockOwnerOldPreferred(podEntries, entryName, subEntryName, numaID, numaCPUs),
					)
				}
			}
		}
	}

	descriptors := make([]advisorBlockDescriptor, 0, len(builders))
	for _, builder := range builders {
		sort.Strings(builder.Owners)
		builder.ComponentKey = fmt.Sprintf("%s|%s|%d",
			builder.Class, strings.Join(builder.Owners, "\x1f"), builder.NUMAID)
		if builder.Eligible.Size() < builder.Quantity {
			return nil, fmt.Errorf("block %q eligible capacity %d is smaller than quantity %d",
				builder.BlockID, builder.Eligible.Size(), builder.Quantity)
		}
		builder.OldPreferred = builder.OldPreferred.Intersection(builder.Eligible)
		descriptors = append(descriptors, builder.advisorBlockDescriptor)
	}

	sort.Slice(descriptors, func(i, j int) bool {
		return advisorBlockDescriptorLess(descriptors[i], descriptors[j])
	})
	return descriptors, nil
}

func advisorBlockDescriptorLess(left, right advisorBlockDescriptor) bool {
	if left.NUMAID != right.NUMAID {
		if left.NUMAID == commonstate.FakedNUMAID {
			return false
		}
		if right.NUMAID == commonstate.FakedNUMAID {
			return true
		}
		return left.NUMAID < right.NUMAID
	}
	if left.Class != right.Class {
		return advisorBlockClassRank(left.Class) < advisorBlockClassRank(right.Class)
	}
	if left.ComponentKey != right.ComponentKey {
		return left.ComponentKey < right.ComponentKey
	}
	if left.Quantity != right.Quantity {
		return left.Quantity < right.Quantity
	}
	leftAliases, rightAliases := strings.Join(left.Owners, "\x1f"), strings.Join(right.Owners, "\x1f")
	if leftAliases != rightAliases {
		return leftAliases < rightAliases
	}
	return left.BlockID < right.BlockID
}

func classifyAdvisorBlockOwner(
	poolName string,
	overlapTargets []*advisorapi.OverlapTarget,
) (advisorBlockClass, error) {
	if poolName == "" {
		return "", fmt.Errorf("cannot classify empty owner pool")
	}
	for _, target := range overlapTargets {
		if target == nil {
			return "", fmt.Errorf("cannot classify nil overlap target")
		}
	}

	poolType := commonstate.GetPoolType(poolName)
	switch poolType {
	case commonstate.PoolNameReserve, commonstate.PoolNamePrefixSystem,
		commonstate.PoolNameInterrupt, commonstate.PoolNameFallback:
		if len(overlapTargets) != 0 {
			return "", fmt.Errorf("cannot classify static owner pool %q with overlap targets", poolName)
		}
		return advisorBlockClassStatic, nil
	case commonstate.PoolNameDedicated:
		return advisorBlockClassDedicated, nil
	case commonstate.PoolNameReclaim:
		if len(overlapTargets) == 0 {
			return advisorBlockClassMandatoryReclaim, nil
		}
		return advisorBlockClassReclaimOverlap, nil
	default:
		if len(overlapTargets) != 0 {
			return advisorBlockClassReclaimOverlap, nil
		}
		return advisorBlockClassShared, nil
	}
}

func canonicalAdvisorBlockOwner(poolName, entryName, subEntryName, resourcePackageName string) string {
	return poolName + "\x00" + entryName + "\x00" + subEntryName + "\x00" + resourcePackageName
}

func advisorBlockOwnerResourcePackageDomain(poolName, resourcePackageName string) (string, bool) {
	if commonstate.GetPoolType(poolName) == commonstate.PoolNameReclaim && resourcePackageName == "" {
		return "", false
	}
	return resourcePackageName, true
}

func advisorBlockNUMACPUSet(allCPUs machine.CPUSet, cpuDetails machine.CPUDetails, numaID int) (machine.CPUSet, error) {
	if numaID == commonstate.FakedNUMAID {
		return allCPUs.Clone(), nil
	}
	numaCPUs := cpuDetails.CPUsInNUMANodes(numaID)
	if numaCPUs.IsEmpty() {
		return machine.NewCPUSet(), fmt.Errorf("NUMA %d has no CPUs", numaID)
	}
	return numaCPUs, nil
}

func advisorBlockOwnerEligible(
	ownerPoolName string,
	resourcePackageName string,
	numaCPUs machine.CPUSet,
	allPinnedCPUs machine.CPUSet,
	rpPinnedCPUSet map[string]machine.CPUSet,
	nonReclaimableCPUSet machine.CPUSet,
) machine.CPUSet {
	if commonstate.GetPoolType(ownerPoolName) == commonstate.PoolNameReclaim {
		return numaCPUs.Difference(nonReclaimableCPUSet)
	}
	if resourcePackageName != "" && !rpPinnedCPUSet[resourcePackageName].IsEmpty() {
		return numaCPUs.Intersection(rpPinnedCPUSet[resourcePackageName])
	}
	return numaCPUs.Difference(allPinnedCPUs)
}

func advisorBlockOwnerOldPreferred(
	podEntries state.PodEntries,
	entryName, subEntryName string,
	numaID int,
	numaCPUs machine.CPUSet,
) machine.CPUSet {
	if podEntries == nil || podEntries[entryName] == nil {
		return machine.NewCPUSet()
	}
	allocationInfo := podEntries[entryName][subEntryName]
	if allocationInfo == nil {
		return machine.NewCPUSet()
	}
	if numaID != commonstate.FakedNUMAID {
		if cpus, found := allocationInfo.TopologyAwareAssignments[numaID]; found {
			return cpus.Clone()
		}
	}
	return allocationInfo.AllocationResult.Intersection(numaCPUs)
}

func uint64ToAdvisorBlockQuantity(quantity uint64) (int, error) {
	converted := int(quantity)
	if converted < 0 || uint64(converted) != quantity {
		return 0, fmt.Errorf("quantity %d overflows int", quantity)
	}
	return converted, nil
}

func advisorBlockClassRank(class advisorBlockClass) int {
	switch class {
	case advisorBlockClassStatic:
		return 0
	case advisorBlockClassMandatoryReclaim:
		return 1
	case advisorBlockClassDedicated:
		return 2
	case advisorBlockClassShared:
		return 3
	case advisorBlockClassReclaimOverlap:
		return 4
	default:
		return 5
	}
}

func expandHardPartitionReclaimPhase(
	descriptors []advisorBlockDescriptor,
	available machine.CPUSet,
	topology *machine.CPUTopology,
) ([]partitionDemand, map[string]string, error) {
	if topology == nil {
		return nil, nil, fmt.Errorf("cannot expand hard reclaim phase with nil CPU topology")
	}

	mandatory := filterAdvisorDescriptors(descriptors, func(descriptor advisorBlockDescriptor) bool {
		return descriptor.Class == advisorBlockClassMandatoryReclaim
	})
	sort.Slice(mandatory, func(i, j int) bool {
		return advisorBlockDescriptorLess(mandatory[i], mandatory[j])
	})
	if len(mandatory) == 0 {
		return nil, map[string]string{}, nil
	}

	demands := make([]partitionDemand, 0, len(mandatory))
	blockIDByDemandKey := make(map[string]string, len(mandatory))
	finalByNUMA := make(map[int]int)
	fixedDedicatedByNUMA := make(map[int]int)
	capacityByNUMA := make(map[int]int)
	eligibleNUMAs := make(map[int]struct{})
	fakeDescriptors := make([]advisorBlockDescriptor, 0, len(mandatory))
	totalQuantity := 0

	for _, descriptor := range descriptors {
		if descriptor.Class == advisorBlockClassDedicated && descriptor.NUMAID != commonstate.FakedNUMAID {
			fixedDedicatedByNUMA[descriptor.NUMAID] += descriptor.Quantity
		}
	}

	for _, descriptor := range mandatory {
		totalQuantity += descriptor.Quantity
		finalEligible := descriptor.Eligible.Intersection(available)
		if descriptor.NUMAID == commonstate.FakedNUMAID {
			fakeDescriptors = append(fakeDescriptors, descriptor)
			for _, numaID := range topology.CPUDetails.KeepOnly(finalEligible).NUMANodes().ToSliceInt() {
				eligibleNUMAs[numaID] = struct{}{}
				capacityByNUMA[numaID] = available.Intersection(
					topology.CPUDetails.CPUsInNUMANodes(numaID)).Size()
			}
			continue
		}

		numaCPUs := topology.CPUDetails.CPUsInNUMANodes(descriptor.NUMAID)
		eligible := finalEligible.Intersection(numaCPUs)
		if eligible.Size() < descriptor.Quantity {
			return nil, nil, fmt.Errorf(
				"hard reclaim block %q NUMA %d eligible capacity %d is smaller than quantity %d",
				descriptor.BlockID, descriptor.NUMAID, eligible.Size(), descriptor.Quantity)
		}
		eligibleNUMAs[descriptor.NUMAID] = struct{}{}
		capacityByNUMA[descriptor.NUMAID] = available.Intersection(numaCPUs).Size()
		finalByNUMA[descriptor.NUMAID] += descriptor.Quantity
		key := hardReclaimPhaseDemandKey(descriptor, descriptor.NUMAID)
		demands = append(demands, partitionDemand{
			key:       key,
			quantity:  descriptor.Quantity,
			eligible:  eligible,
			preferred: descriptor.OldPreferred.Intersection(eligible),
			class:     advisorBlockClassMandatoryReclaim,
		})
		blockIDByDemandKey[key] = descriptor.BlockID
	}

	if len(fakeDescriptors) == 0 {
		return demands, blockIDByDemandKey, nil
	}
	requiredMinimum := minimumHardReclaimCPUsPerNUMA * len(eligibleNUMAs)
	if totalQuantity < requiredMinimum {
		return nil, nil, fmt.Errorf(
			"hard reclaim quantity %d is smaller than required minimum %d",
			totalQuantity, requiredMinimum)
	}

	for numaID, quantity := range finalByNUMA {
		if fixedDedicatedByNUMA[numaID]+quantity > capacityByNUMA[numaID] {
			return nil, nil, fmt.Errorf(
				"hard reclaim NUMA %d initial quantity %d with fixed dedicated load %d exceeds capacity %d",
				numaID, quantity, fixedDedicatedByNUMA[numaID], capacityByNUMA[numaID])
		}
	}

	numaIDs := make([]int, 0, len(eligibleNUMAs))
	for numaID := range eligibleNUMAs {
		numaIDs = append(numaIDs, numaID)
	}
	sort.Ints(numaIDs)
	fakeQuotas, ok := solveHardReclaimFakeQuotas(
		fakeDescriptors,
		descriptors,
		demands,
		available,
		topology,
		numaIDs,
		finalByNUMA,
		fixedDedicatedByNUMA,
		capacityByNUMA,
		totalQuantity,
	)
	if !ok {
		return nil, nil, fmt.Errorf(
			"hard reclaim fake blocks have insufficient aggregate capacity for balanced assignment")
	}

	for _, descriptor := range fakeDescriptors {
		quotas := fakeQuotas[descriptor.BlockID]
		for _, numaID := range numaIDs {
			if quotas[numaID] == 0 {
				continue
			}
			numaCPUs := topology.CPUDetails.CPUsInNUMANodes(numaID)
			eligible := descriptor.Eligible.Intersection(available).Intersection(numaCPUs)
			key := hardReclaimPhaseDemandKey(descriptor, numaID)
			demands = append(demands, partitionDemand{
				key:       key,
				quantity:  quotas[numaID],
				eligible:  eligible,
				preferred: descriptor.OldPreferred.Intersection(eligible),
				class:     advisorBlockClassMandatoryReclaim,
			})
			blockIDByDemandKey[key] = descriptor.BlockID
			finalByNUMA[numaID] += quotas[numaID]
		}
	}

	minimum, maximum := finalByNUMA[numaIDs[0]], finalByNUMA[numaIDs[0]]
	for _, numaID := range numaIDs {
		if finalByNUMA[numaID] < minimumHardReclaimCPUsPerNUMA {
			return nil, nil, fmt.Errorf(
				"hard reclaim NUMA %d final quantity %d is smaller than minimum %d",
				numaID, finalByNUMA[numaID], minimumHardReclaimCPUsPerNUMA)
		}
		if finalByNUMA[numaID] < minimum {
			minimum = finalByNUMA[numaID]
		}
		if finalByNUMA[numaID] > maximum {
			maximum = finalByNUMA[numaID]
		}
	}
	if maximum-minimum > 1 {
		return nil, nil, fmt.Errorf(
			"hard reclaim final NUMA quantities are imbalanced: min %d max %d", minimum, maximum)
	}
	return demands, blockIDByDemandKey, nil
}

func solveHardReclaimFakeQuotas(
	fakeDescriptors, descriptors []advisorBlockDescriptor,
	realDemands []partitionDemand,
	available machine.CPUSet,
	topology *machine.CPUTopology,
	numaIDs []int,
	realByNUMA, fixedDedicatedByNUMA, capacityByNUMA map[int]int,
	totalQuantity int,
) (map[string]map[int]int, bool) {
	sortedFake := append([]advisorBlockDescriptor(nil), fakeDescriptors...)
	sort.Slice(sortedFake, func(i, j int) bool {
		leftNUMAs := topology.CPUDetails.KeepOnly(
			sortedFake[i].Eligible.Intersection(available)).NUMANodes().Size()
		rightNUMAs := topology.CPUDetails.KeepOnly(
			sortedFake[j].Eligible.Intersection(available)).NUMANodes().Size()
		if leftNUMAs != rightNUMAs {
			return leftNUMAs < rightNUMAs
		}
		return advisorBlockDescriptorLess(sortedFake[i], sortedFake[j])
	})

	baseTarget := totalQuantity / len(numaIDs)
	highTargetCount := totalQuantity % len(numaIDs)
	targetByNUMA := make(map[int]int, len(numaIDs))
	var solveTargets func(index, highsLeft int) (map[string]map[int]int, bool)
	solveTargets = func(index, highsLeft int) (map[string]map[int]int, bool) {
		if index == len(numaIDs) {
			if highsLeft != 0 {
				return nil, false
			}
			return solveHardReclaimQuotasForTargets(
				sortedFake, descriptors, realDemands, available, topology,
				numaIDs, targetByNUMA, realByNUMA)
		}

		numaID := numaIDs[index]
		targets := []int{baseTarget}
		if highsLeft > 0 {
			targets = append(targets, baseTarget+1)
		}
		for _, target := range targets {
			isHigh := 0
			if target == baseTarget+1 {
				isHigh = 1
			}
			remainingHighs := highsLeft - isHigh
			remainingNUMAs := len(numaIDs) - index - 1
			if remainingHighs < 0 || remainingHighs > remainingNUMAs ||
				target < minimumHardReclaimCPUsPerNUMA ||
				target < realByNUMA[numaID] ||
				fixedDedicatedByNUMA[numaID]+target > capacityByNUMA[numaID] {
				continue
			}
			targetByNUMA[numaID] = target
			if quotas, ok := solveTargets(index+1, remainingHighs); ok {
				return quotas, true
			}
		}
		delete(targetByNUMA, numaID)
		return nil, false
	}
	return solveTargets(0, highTargetCount)
}

func solveHardReclaimQuotasForTargets(
	fakeDescriptors, descriptors []advisorBlockDescriptor,
	realDemands []partitionDemand,
	available machine.CPUSet,
	topology *machine.CPUTopology,
	numaIDs []int,
	targetByNUMA, realByNUMA map[int]int,
) (map[string]map[int]int, bool) {
	remainingByNUMA := make(map[int]int, len(numaIDs))
	for _, numaID := range numaIDs {
		remainingByNUMA[numaID] = targetByNUMA[numaID] - realByNUMA[numaID]
	}
	quotas := make(map[string]map[int]int, len(fakeDescriptors))

	var assignDescriptor func(index int) bool
	assignDescriptor = func(index int) bool {
		if index == len(fakeDescriptors) {
			for _, remaining := range remainingByNUMA {
				if remaining != 0 {
					return false
				}
			}
			return hardReclaimQuotasHaveFeasibleCPUAssignment(
				fakeDescriptors, descriptors, realDemands, quotas, available, topology)
		}

		descriptor := fakeDescriptors[index]
		finalEligible := descriptor.Eligible.Intersection(available)
		descriptorQuotas := make(map[int]int)
		var distribute func(numaIndex, quantityLeft int) bool
		distribute = func(numaIndex, quantityLeft int) bool {
			if numaIndex == len(numaIDs) {
				if quantityLeft != 0 {
					return false
				}
				quotas[descriptor.BlockID] = descriptorQuotas
				if assignDescriptor(index + 1) {
					return true
				}
				delete(quotas, descriptor.BlockID)
				return false
			}

			numaID := numaIDs[numaIndex]
			eligibleCapacity := finalEligible.Intersection(
				topology.CPUDetails.CPUsInNUMANodes(numaID)).Size()
			maximum := quantityLeft
			if maximum > remainingByNUMA[numaID] {
				maximum = remainingByNUMA[numaID]
			}
			if maximum > eligibleCapacity {
				maximum = eligibleCapacity
			}
			for quantity := 0; quantity <= maximum; quantity++ {
				descriptorQuotas[numaID] = quantity
				remainingByNUMA[numaID] -= quantity
				if distribute(numaIndex+1, quantityLeft-quantity) {
					return true
				}
				remainingByNUMA[numaID] += quantity
			}
			delete(descriptorQuotas, numaID)
			return false
		}
		return distribute(0, descriptor.Quantity)
	}

	if !assignDescriptor(0) {
		return nil, false
	}
	return quotas, true
}

func hardReclaimQuotasHaveFeasibleCPUAssignment(
	fakeDescriptors, descriptors []advisorBlockDescriptor,
	realDemands []partitionDemand,
	quotas map[string]map[int]int,
	available machine.CPUSet,
	topology *machine.CPUTopology,
) bool {
	demands := append([]partitionDemand(nil), realDemands...)
	for _, descriptor := range fakeDescriptors {
		for numaID, quantity := range quotas[descriptor.BlockID] {
			if quantity == 0 {
				continue
			}
			eligible := descriptor.Eligible.Intersection(available).Intersection(
				topology.CPUDetails.CPUsInNUMANodes(numaID))
			demands = append(demands, partitionDemand{
				key:       hardReclaimPhaseDemandKey(descriptor, numaID),
				quantity:  quantity,
				eligible:  eligible,
				preferred: descriptor.OldPreferred.Intersection(eligible),
				class:     advisorBlockClassMandatoryReclaim,
			})
		}
	}
	for index, descriptor := range descriptors {
		if descriptor.Class != advisorBlockClassDedicated {
			continue
		}
		demands = append(demands, partitionDemand{
			key:       fmt.Sprintf("\xffhard-reclaim-dedicated\x00%d\x00%s", index, descriptor.BlockID),
			quantity:  descriptor.Quantity,
			eligible:  descriptor.Eligible.Intersection(available),
			preferred: descriptor.OldPreferred.Intersection(available),
			class:     advisorBlockClassDedicated,
		})
	}
	_, err := solveDisjointPartitions(demands, topology)
	return err == nil
}

func hardReclaimPhaseDemandKey(descriptor advisorBlockDescriptor, numaID int) string {
	return fmt.Sprintf("%s\x00block\x00%s\x00numa\x00%d",
		descriptor.ComponentKey, descriptor.BlockID, numaID)
}
