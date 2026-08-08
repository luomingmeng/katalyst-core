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
	advisorBlockClassDedicated advisorBlockClass = "dedicated"
	advisorBlockClassShared    advisorBlockClass = "shared"
	advisorBlockClassReclaim   advisorBlockClass = "reclaim"
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
	ownerSeen       map[string]struct{}
	resourcePackage string
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

			ownerClass, ownerResourcePackage := classifyAdvisorBlockOwner(calculationInfo.OwnerPoolName)
			ownerKey := canonicalAdvisorBlockOwner(calculationInfo.OwnerPoolName, entryName, subEntryName)
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
				ownerEligible := advisorBlockOwnerEligible(
					ownerClass,
					ownerResourcePackage,
					numaCPUs,
					allPinnedCPUs,
					rpPinnedCPUSet,
					nonReclaimableCPUSet,
				)

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
							ownerSeen:       make(map[string]struct{}),
							resourcePackage: ownerResourcePackage,
						}
						builders[block.BlockId] = builder
					} else {
						if builder.NUMAID != numaID || builder.Quantity != quantity {
							return nil, fmt.Errorf("block %q aliases disagree on NUMA or quantity", block.BlockId)
						}
						if builder.Class != ownerClass {
							return nil, fmt.Errorf("block %q aliases have incompatible owner classes", block.BlockId)
						}
						if builder.resourcePackage != "" && ownerResourcePackage != "" &&
							builder.resourcePackage != ownerResourcePackage {
							return nil, fmt.Errorf("block %q aliases use incompatible resource packages %q and %q",
								block.BlockId, builder.resourcePackage, ownerResourcePackage)
						}
						if builder.resourcePackage == "" {
							builder.resourcePackage = ownerResourcePackage
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
		builder.ComponentKey = fmt.Sprintf("%s|%s|%d|%d",
			builder.Class, strings.Join(builder.Owners, "\x1f"), builder.NUMAID, builder.Quantity)
		if builder.Eligible.Size() < builder.Quantity {
			return nil, fmt.Errorf("block %q eligible capacity %d is smaller than quantity %d",
				builder.BlockID, builder.Eligible.Size(), builder.Quantity)
		}
		builder.OldPreferred = builder.OldPreferred.Intersection(builder.Eligible)
		descriptors = append(descriptors, builder.advisorBlockDescriptor)
	}

	sort.Slice(descriptors, func(i, j int) bool {
		left, right := descriptors[i], descriptors[j]
		leftOwners, rightOwners := strings.Join(left.Owners, "\x1f"), strings.Join(right.Owners, "\x1f")
		if leftOwners != rightOwners {
			return leftOwners < rightOwners
		}
		if left.Class != right.Class {
			return advisorBlockClassRank(left.Class) < advisorBlockClassRank(right.Class)
		}
		if left.NUMAID != right.NUMAID {
			return left.NUMAID < right.NUMAID
		}
		if left.Quantity != right.Quantity {
			return left.Quantity < right.Quantity
		}
		if left.ComponentKey != right.ComponentKey {
			return left.ComponentKey < right.ComponentKey
		}
		return left.BlockID < right.BlockID
	})
	return descriptors, nil
}

func classifyAdvisorBlockOwner(ownerPoolName string) (advisorBlockClass, string) {
	poolName, packageName := resourcepackage.UnwrapOwnerPoolName(ownerPoolName)
	switch poolName {
	case commonstate.PoolNameDedicated:
		return advisorBlockClassDedicated, packageName
	case commonstate.PoolNameReclaim:
		return advisorBlockClassReclaim, packageName
	default:
		return advisorBlockClassShared, packageName
	}
}

func canonicalAdvisorBlockOwner(ownerPoolName, entryName, subEntryName string) string {
	return ownerPoolName + "\x00" + entryName + "\x00" + subEntryName
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
	if class == advisorBlockClassReclaim {
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
	case advisorBlockClassDedicated:
		return 0
	case advisorBlockClassShared:
		return 1
	case advisorBlockClassReclaim:
		return 2
	default:
		return 3
	}
}
