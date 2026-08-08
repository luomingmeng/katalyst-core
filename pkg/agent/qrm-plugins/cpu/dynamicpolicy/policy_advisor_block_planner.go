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
	ownerSeen map[string]struct{}
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
						ownerClass,
						ownerResourcePackage,
						numaCPUs,
						allPinnedCPUs,
						rpPinnedCPUSet,
						nonReclaimableCPUSet,
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
						builders[block.BlockId] = builder
					} else {
						if builder.NUMAID != numaID || builder.Quantity != quantity {
							return nil, fmt.Errorf("block %q aliases disagree on NUMA or quantity", block.BlockId)
						}
						if builder.Class != ownerClass {
							return nil, fmt.Errorf("block %q aliases have incompatible owner classes", block.BlockId)
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
	class advisorBlockClass,
	resourcePackageName string,
	numaCPUs machine.CPUSet,
	allPinnedCPUs machine.CPUSet,
	rpPinnedCPUSet map[string]machine.CPUSet,
	nonReclaimableCPUSet machine.CPUSet,
) machine.CPUSet {
	if class == advisorBlockClassMandatoryReclaim || class == advisorBlockClassReclaimOverlap {
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
