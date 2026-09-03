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

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
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

	// minimumHardReclaimCoresPerNUMA is the immutable minimum reclaim floor a
	// single eligible NUMA always keeps on the hard-partition path, expressed in
	// complete physical cores. The CPU magnitude is derived from CPUsPerCore() so
	// the guard is core-granular on any topology (no hard-coded SMT factor).
	minimumHardReclaimCoresPerNUMA = 1
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
	for _, descriptor := range descriptors {
		general.InfoS("advisor block descriptor built",
			"blockID", descriptor.BlockID,
			"class", descriptor.Class,
			"numaID", descriptor.NUMAID,
			"quantity", descriptor.Quantity,
			"componentKey", descriptor.ComponentKey,
			"owners", descriptor.Owners,
			"eligibleSize", descriptor.Eligible.Size(),
			"eligible", descriptor.Eligible.String(),
			"oldPreferredSize", descriptor.OldPreferred.Size(),
			"oldPreferred", descriptor.OldPreferred.String())
	}
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

func hasFakeNUMAMandatoryReclaimDescriptor(descriptors []advisorBlockDescriptor) bool {
	for _, descriptor := range descriptors {
		if descriptor.Class == advisorBlockClassMandatoryReclaim &&
			descriptor.NUMAID == commonstate.FakedNUMAID {
			return true
		}
	}
	return false
}

func normalizeAdvisorDescriptorsForWholeCoreReclaim(
	descriptors []advisorBlockDescriptor,
	topology *machine.CPUTopology,
) ([]advisorBlockDescriptor, error) {
	if topology == nil {
		return nil, fmt.Errorf("cannot normalize advisor descriptors with nil CPU topology")
	}
	cpusPerCore := topology.CPUsPerCore()
	if cpusPerCore <= 1 {
		return append([]advisorBlockDescriptor(nil), descriptors...), nil
	}

	normalized := append([]advisorBlockDescriptor(nil), descriptors...)
	mandatoryByNUMA := make(map[int][]int)
	dedicatedByNUMA := make(map[int][]int)
	mandatoryQuantityByNUMA := make(map[int]int)
	for i, descriptor := range normalized {
		if descriptor.NUMAID == commonstate.FakedNUMAID {
			continue
		}
		switch descriptor.Class {
		case advisorBlockClassMandatoryReclaim:
			mandatoryByNUMA[descriptor.NUMAID] = append(mandatoryByNUMA[descriptor.NUMAID], i)
			mandatoryQuantityByNUMA[descriptor.NUMAID] += descriptor.Quantity
		case advisorBlockClassDedicated:
			dedicatedByNUMA[descriptor.NUMAID] = append(dedicatedByNUMA[descriptor.NUMAID], i)
		}
	}

	numaIDs := make([]int, 0, len(mandatoryQuantityByNUMA))
	for numaID := range mandatoryQuantityByNUMA {
		numaIDs = append(numaIDs, numaID)
	}
	sort.Ints(numaIDs)
	for _, numaID := range numaIDs {
		quantity := mandatoryQuantityByNUMA[numaID]
		remainder := quantity % cpusPerCore
		if remainder == 0 {
			continue
		}
		deficit := cpusPerCore - remainder
		mandatoryIndexes := append([]int(nil), mandatoryByNUMA[numaID]...)
		dedicatedIndexes := append([]int(nil), dedicatedByNUMA[numaID]...)
		sort.Slice(mandatoryIndexes, func(i, j int) bool {
			return advisorBlockDescriptorLess(normalized[mandatoryIndexes[i]], normalized[mandatoryIndexes[j]])
		})
		sort.Slice(dedicatedIndexes, func(i, j int) bool {
			return advisorBlockDescriptorLess(normalized[dedicatedIndexes[i]], normalized[dedicatedIndexes[j]])
		})
		if len(mandatoryIndexes) == 0 {
			return nil, fmt.Errorf("NUMA %d has mandatory reclaim quantity %d but no reclaim descriptor",
				numaID, quantity)
		}
		if len(dedicatedIndexes) == 0 {
			return nil, fmt.Errorf(
				"NUMA %d mandatory reclaim quantity %d needs %d CPUs to become a whole-core multiple of %d, but no dedicated descriptor can shrink",
				numaID, quantity, deficit, cpusPerCore)
		}
		remaining := deficit
		for _, index := range dedicatedIndexes {
			if remaining == 0 {
				break
			}
			take := general.Min(normalized[index].Quantity, remaining)
			normalized[index].Quantity -= take
			remaining -= take
		}
		if remaining > 0 {
			return nil, fmt.Errorf(
				"NUMA %d mandatory reclaim quantity %d needs %d CPUs to become a whole-core multiple of %d, but dedicated descriptors are short by %d",
				numaID, quantity, deficit, cpusPerCore, remaining)
		}
		normalized[mandatoryIndexes[0]].Quantity += deficit
		general.InfoS("normalize advisor descriptors to preserve whole-core reclaim",
			"numaID", numaID,
			"cpusPerCore", cpusPerCore,
			"mandatoryQuantityBefore", quantity,
			"deficit", deficit,
			"mandatoryBlockID", normalized[mandatoryIndexes[0]].BlockID,
			"mandatoryQuantityAfter", normalized[mandatoryIndexes[0]].Quantity)
	}
	return normalized, nil
}

// expandSteadyFakeNUMAReclaimPhase reserves one complete physical core on each
// eligible NUMA before leaving the remaining fake-NUMA quantity to the regular
// joint solver. This preserves the advisor-published total while preventing a
// stable preferred set from concentrating the whole reclaim pool on a subset of
// NUMAs. The residual remains global so narrow share and dedicated demands can
// still participate in the same feasibility solve.
func expandSteadyFakeNUMAReclaimPhase(
	descriptors []advisorBlockDescriptor,
	available machine.CPUSet,
	topology *machine.CPUTopology,
	skipNUMAs sets.Int,
) ([]partitionDemand, map[string]string, []partitionCoreFloorConstraint, error) {
	if topology == nil {
		return nil, nil, nil, fmt.Errorf("cannot expand steady reclaim phase with nil CPU topology")
	}
	cpusPerCore := topology.CPUsPerCore()
	if cpusPerCore <= 0 {
		return nil, nil, nil, fmt.Errorf(
			"cannot expand steady reclaim phase with non-positive cpus per core %d", cpusPerCore)
	}

	mandatory := filterAdvisorDescriptors(descriptors, func(descriptor advisorBlockDescriptor) bool {
		return descriptor.Class == advisorBlockClassMandatoryReclaim
	})
	sort.Slice(mandatory, func(i, j int) bool {
		return advisorBlockDescriptorLess(mandatory[i], mandatory[j])
	})
	fakeDescriptors := filterAdvisorDescriptors(mandatory, func(descriptor advisorBlockDescriptor) bool {
		return descriptor.NUMAID == commonstate.FakedNUMAID
	})
	if len(fakeDescriptors) != 1 {
		return nil, nil, nil, fmt.Errorf(
			"steady reclaim protocol error: expected exactly one fake-NUMA mandatory reclaim block, got %d",
			len(fakeDescriptors))
	}

	demands := make([]partitionDemand, 0, len(mandatory)+topology.NumNUMANodes)
	blockIDByDemandKey := make(map[string]string, len(mandatory)+topology.NumNUMANodes)
	floors := make([]partitionCoreFloorConstraint, 0, topology.NumNUMANodes)
	for _, descriptor := range mandatory {
		if descriptor.NUMAID == commonstate.FakedNUMAID {
			continue
		}
		numaCPUs := topology.CPUDetails.CPUsInNUMANodes(descriptor.NUMAID)
		eligible := descriptor.Eligible.Intersection(available).Intersection(numaCPUs)
		if eligible.Size() < descriptor.Quantity {
			return nil, nil, nil, fmt.Errorf(
				"steady reclaim block %q NUMA %d eligible capacity %d is smaller than quantity %d",
				descriptor.BlockID, descriptor.NUMAID, eligible.Size(), descriptor.Quantity)
		}
		key := hardReclaimPhaseDemandKey(descriptor, descriptor.NUMAID)
		coreCandidates := coreAlignedCandidates(
			topology, eligible, descriptor.OldPreferred.Intersection(eligible))
		if descriptor.Quantity >= cpusPerCore && len(coreCandidates) > 0 {
			floorEligible := machine.NewCPUSet()
			for _, candidate := range coreCandidates {
				floorEligible = floorEligible.Union(candidate.cpus)
			}
			demands = append(demands, partitionDemand{
				key:       key,
				quantity:  cpusPerCore,
				eligible:  floorEligible,
				preferred: descriptor.OldPreferred.Intersection(floorEligible),
				class:     advisorBlockClassMandatoryReclaim,
			})
			blockIDByDemandKey[key] = descriptor.BlockID
			floors = append(floors, partitionCoreFloorConstraint{demandKey: key})

			residual := descriptor.Quantity - cpusPerCore
			if residual > 0 {
				residualKey := key + "\x00residual"
				demands = append(demands, partitionDemand{
					key:      residualKey,
					quantity: residual,
					eligible: eligible,
					preferred: takeCoreAlignedCPUSet(
						topology, eligible, descriptor.OldPreferred.Intersection(eligible), residual),
					class: advisorBlockClassMandatoryReclaim,
				})
				blockIDByDemandKey[residualKey] = descriptor.BlockID
			}
			continue
		}
		demands = append(demands, partitionDemand{
			key:       key,
			quantity:  descriptor.Quantity,
			eligible:  eligible,
			preferred: descriptor.OldPreferred.Intersection(eligible),
			class:     advisorBlockClassMandatoryReclaim,
		})
		blockIDByDemandKey[key] = descriptor.BlockID
	}

	fake := fakeDescriptors[0]
	fakeEligible := fake.Eligible.Intersection(available)
	realMandatoryPreferred := machine.NewCPUSet()
	for _, descriptor := range mandatory {
		if descriptor.NUMAID != commonstate.FakedNUMAID {
			realMandatoryPreferred = realMandatoryPreferred.Union(descriptor.OldPreferred)
		}
	}
	fakeOldPreferred := fake.OldPreferred.Difference(realMandatoryPreferred).Intersection(fakeEligible)
	numaIDs := topology.CPUDetails.KeepOnly(fakeEligible).NUMANodes().ToSliceInt()
	sort.Ints(numaIDs)
	minimumByNUMA := make(map[int]int, len(numaIDs))
	maximumByNUMA := make(map[int]int, len(numaIDs))
	fixedQuantityByNUMA := make(map[int]int, len(numaIDs))
	realMandatoryQuantityByNUMA := make(map[int]int, len(numaIDs))
	for _, descriptor := range descriptors {
		if descriptor.BlockID == fake.BlockID {
			continue
		}
		finalEligible := descriptor.Eligible.Intersection(available)
		eligibleNUMAs := topology.CPUDetails.KeepOnly(finalEligible).NUMANodes().ToSliceInt()
		if len(eligibleNUMAs) == 1 {
			fixedQuantityByNUMA[eligibleNUMAs[0]] += descriptor.Quantity
		}
		if descriptor.Class == advisorBlockClassMandatoryReclaim &&
			descriptor.NUMAID != commonstate.FakedNUMAID {
			realMandatoryQuantityByNUMA[descriptor.NUMAID] += descriptor.Quantity
		}
	}
	for _, numaID := range numaIDs {
		if skipNUMAs.Has(numaID) {
			maximumByNUMA[numaID] = general.Max(
				0,
				available.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID)).Size()-
					fixedQuantityByNUMA[numaID],
			)
		} else {
			numaEligible := fakeEligible.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
			realMandatory := realMandatoryQuantityByNUMA[numaID]
			if realMandatory == 0 {
				coreCandidates := coreAlignedCandidates(
					topology, numaEligible, fakeOldPreferred.Intersection(numaEligible))
				if len(coreCandidates) > 0 {
					minimumByNUMA[numaID] = cpusPerCore
				}
			} else if remainder := realMandatory % cpusPerCore; remainder != 0 {
				minimumByNUMA[numaID] = cpusPerCore - remainder
			}
			maximumByNUMA[numaID] = general.Max(
				0,
				available.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID)).Size()-
					fixedQuantityByNUMA[numaID],
			)
		}
	}
	quotas, err := planSteadyFakeNUMACoreCapacityQuotasWithLimits(
		fake.Quantity,
		fakeOldPreferred,
		fakeEligible,
		topology,
		skipNUMAs,
		minimumByNUMA,
		maximumByNUMA,
		realMandatoryQuantityByNUMA,
	)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, numaID := range numaIDs {
		quota := quotas[numaID]
		if quota == 0 {
			continue
		}
		eligible := fakeEligible.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		key := hardReclaimPhaseDemandKey(fake, numaID)
		demands = append(demands, partitionDemand{
			key:       key,
			quantity:  quota,
			eligible:  eligible,
			preferred: fakeOldPreferred.Intersection(eligible),
			class:     advisorBlockClassMandatoryReclaim,
		})
		blockIDByDemandKey[key] = fake.BlockID
	}
	return demands, blockIDByDemandKey, floors, nil
}

func expandHardPartitionReclaimPhase(
	descriptors []advisorBlockDescriptor,
	available machine.CPUSet,
	topology *machine.CPUTopology,
	skipNUMAs sets.Int,
) ([]partitionDemand, map[string]string, error) {
	if topology == nil {
		return nil, nil, fmt.Errorf("cannot expand hard reclaim phase with nil CPU topology")
	}
	cpusPerCore := topology.CPUsPerCore()
	if cpusPerCore <= 0 {
		return nil, nil, fmt.Errorf(
			"cannot expand hard reclaim phase with non-positive cpus per core %d", cpusPerCore)
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
	fakeDescriptors := filterAdvisorDescriptors(mandatory, func(descriptor advisorBlockDescriptor) bool {
		return descriptor.NUMAID == commonstate.FakedNUMAID
	})
	if len(fakeDescriptors) > 1 {
		return nil, nil, fmt.Errorf(
			"hard reclaim protocol error: expected at most one fake-NUMA mandatory reclaim block, got %d",
			len(fakeDescriptors))
	}

	demands := make([]partitionDemand, 0, len(mandatory))
	blockIDByDemandKey := make(map[string]string, len(mandatory))
	finalByNUMA := make(map[int]int)
	fixedDedicatedByNUMA := make(map[int]int)
	capacityByNUMA := make(map[int]int)
	eligibleNUMAs := make(map[int]struct{})
	totalQuantity := 0

	for _, descriptor := range descriptors {
		if descriptor.Class == advisorBlockClassDedicated && descriptor.NUMAID != commonstate.FakedNUMAID {
			fixedDedicatedByNUMA[descriptor.NUMAID] += descriptor.Quantity
		}
	}
	general.InfoS("hard reclaim phase input",
		"availableSize", available.Size(),
		"available", available.String(),
		"descriptorCount", len(descriptors),
		"mandatoryCount", len(mandatory),
		"fakeMandatoryCount", len(fakeDescriptors),
		"fixedDedicatedByNUMA", fixedDedicatedByNUMA)

	for _, descriptor := range mandatory {
		totalQuantity += descriptor.Quantity
		finalEligible := descriptor.Eligible.Intersection(available)
		general.InfoS("hard reclaim mandatory descriptor",
			"blockID", descriptor.BlockID,
			"numaID", descriptor.NUMAID,
			"quantity", descriptor.Quantity,
			"componentKey", descriptor.ComponentKey,
			"eligibleSize", descriptor.Eligible.Size(),
			"eligible", descriptor.Eligible.String(),
			"finalEligibleSize", finalEligible.Size(),
			"finalEligible", finalEligible.String())
		if descriptor.NUMAID == commonstate.FakedNUMAID {
			if topology.CPUDetails.KeepOnly(finalEligible).NUMANodes().IsEmpty() {
				return nil, nil, fmt.Errorf(
					"hard reclaim fake block %q has quantity %d but no eligible NUMA",
					descriptor.BlockID, descriptor.Quantity)
			}
			for _, numaID := range topology.CPUDetails.KeepOnly(finalEligible).NUMANodes().ToSliceInt() {
				numaEligible := finalEligible.Intersection(
					available).Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
				if len(coreAlignedCandidates(
					topology, numaEligible, descriptor.OldPreferred.Intersection(numaEligible))) == 0 {
					continue
				}
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
	general.InfoS("hard reclaim phase seeded real mandatory",
		"totalQuantity", totalQuantity,
		"eligibleNUMAs", sortedIntKeys(eligibleNUMAs),
		"capacityByNUMA", capacityByNUMA,
		"finalByNUMA", finalByNUMA,
		"fixedDedicatedByNUMA", fixedDedicatedByNUMA)

	if len(fakeDescriptors) == 0 {
		return demands, blockIDByDemandKey, nil
	}
	if len(eligibleNUMAs) == 0 {
		return demands, blockIDByDemandKey, nil
	}
	requiredMinimum := minimumHardReclaimCoresPerNUMA * cpusPerCore * len(eligibleNUMAs)
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
	fake := fakeDescriptors[0]
	finalEligible := fake.Eligible.Intersection(available)
	eligibleCapacityByNUMA := make(map[int]int, len(numaIDs))
	for _, numaID := range numaIDs {
		numaEligible := finalEligible.Intersection(
			available).Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		completeCapacity := len(coreAlignedCandidates(
			topology, numaEligible, fake.OldPreferred.Intersection(numaEligible))) * cpusPerCore
		eligibleCapacityByNUMA[numaID] = general.Max(
			0, completeCapacity-finalByNUMA[numaID])
	}
	general.InfoS("hard reclaim fake mandatory water-filling input",
		"blockID", fake.BlockID,
		"quantity", fake.Quantity,
		"componentKey", fake.ComponentKey,
		"eligibleNUMAs", numaIDs,
		"eligibleCapacityByNUMA", eligibleCapacityByNUMA,
		"capacityByNUMA", capacityByNUMA,
		"fixedDedicatedByNUMA", fixedDedicatedByNUMA,
		"finalByNUMABeforeFake", finalByNUMA)
	quotas := make(map[int]int, len(numaIDs))
	if fake.Quantity%cpusPerCore != 0 {
		return nil, nil, fmt.Errorf(
			"hard reclaim fake block %q quantity %d is not a whole-core multiple of %d",
			fake.BlockID, fake.Quantity, cpusPerCore)
	}
	// water-fill complete physical cores round-robin: every step grants a full core
	// (cpusPerCore cpus) to the currently least-loaded eligible NUMA, so a balanced
	// result never strands a lone SMT sibling. On non-SMT topologies cpusPerCore==1
	// and this reduces byte-for-byte to the historical single-CPU water-filling.
	for allocated := 0; allocated < fake.Quantity; allocated += cpusPerCore {
		selectedNUMA := 0
		selected := false
		for _, numaID := range numaIDs {
			if quotas[numaID]+cpusPerCore > eligibleCapacityByNUMA[numaID] ||
				fixedDedicatedByNUMA[numaID]+finalByNUMA[numaID]+cpusPerCore > capacityByNUMA[numaID] {
				continue
			}
			if !selected || finalByNUMA[numaID] < finalByNUMA[selectedNUMA] {
				selectedNUMA = numaID
				selected = true
			}
		}
		if !selected {
			return nil, nil, fmt.Errorf(
				"hard reclaim fake block %q has insufficient aggregate capacity for quantity %d",
				fake.BlockID, fake.Quantity)
		}
		quotas[selectedNUMA] += cpusPerCore
		finalByNUMA[selectedNUMA] += cpusPerCore
	}
	general.InfoS("hard reclaim fake mandatory water-filling result",
		"blockID", fake.BlockID,
		"quotas", quotas,
		"finalByNUMAAfterFake", finalByNUMA)
	for _, numaID := range numaIDs {
		if quotas[numaID] == 0 {
			continue
		}
		eligible := finalEligible.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		key := hardReclaimPhaseDemandKey(fake, numaID)
		demands = append(demands, partitionDemand{
			key:       key,
			quantity:  quotas[numaID],
			eligible:  eligible,
			preferred: fake.OldPreferred.Intersection(eligible),
			class:     advisorBlockClassMandatoryReclaim,
		})
		blockIDByDemandKey[key] = fake.BlockID
	}

	minimum, maximum := 0, 0
	seen := false
	perNUMAMinimum := minimumHardReclaimCoresPerNUMA * cpusPerCore
	for _, numaID := range numaIDs {
		// NUMAs already owned by a committed steady exclusive DNB keep only their
		// finalized reserve once ramp-up ends, regardless of reclaimability. The
		// node-level per-NUMA minimum and cross-NUMA imbalance guards must not
		// re-impose the ratio-derived target on them, otherwise the planner fails
		// closed for every other ramp-up QoS on the node.
		if skipNUMAs.Has(numaID) {
			continue
		}
		if finalByNUMA[numaID] < perNUMAMinimum {
			return nil, nil, fmt.Errorf(
				"hard reclaim NUMA %d final quantity %d is smaller than minimum %d",
				numaID, finalByNUMA[numaID], perNUMAMinimum)
		}
		if !seen || finalByNUMA[numaID] < minimum {
			minimum = finalByNUMA[numaID]
		}
		if !seen || finalByNUMA[numaID] > maximum {
			maximum = finalByNUMA[numaID]
		}
		seen = true
	}
	// imbalance tolerance is one complete core: core-granular water-filling can leave
	// at most one NUMA a single core ahead, never a fractional-core skew.
	if seen && maximum-minimum > cpusPerCore {
		general.InfoS("hard reclaim final NUMA quantities imbalanced",
			"minimum", minimum,
			"maximum", maximum,
			"finalByNUMA", finalByNUMA,
			"capacityByNUMA", capacityByNUMA,
			"fixedDedicatedByNUMA", fixedDedicatedByNUMA,
			"fakeQuotas", quotas)
		return nil, nil, fmt.Errorf(
			"hard reclaim final NUMA quantities are imbalanced: min %d max %d", minimum, maximum)
	}
	return demands, blockIDByDemandKey, nil
}

func sortedIntKeys(keys map[int]struct{}) []int {
	result := make([]int, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Ints(result)
	return result
}

func hardReclaimPhaseDemandKey(descriptor advisorBlockDescriptor, numaID int) string {
	return fmt.Sprintf("%s\x00block\x00%s\x00numa\x00%d",
		descriptor.ComponentKey, descriptor.BlockID, numaID)
}
