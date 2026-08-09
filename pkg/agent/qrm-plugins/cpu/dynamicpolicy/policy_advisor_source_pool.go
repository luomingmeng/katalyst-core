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

package dynamicpolicy

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/calculator"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	cpuutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/util"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

// deriveAdvisorIsolationSourcePool derives the source share pool for a shared_cores
// isolation block from the advisor block owner entries and the current state. It returns
// false on derivation failure so callers can preserve the legacy allocation path.
func deriveAdvisorIsolationSourcePool(block *advisorapi.BlockInfo, entries state.PodEntries) (string, bool) {
	sourcePool, _, ok := deriveAdvisorIsolationSourceDomain(block, entries)
	return sourcePool, ok
}

func deriveAdvisorIsolationSourceDomain(
	block *advisorapi.BlockInfo,
	entries state.PodEntries,
) (string, string, bool) {
	if block == nil {
		return "", "", false
	}

	for ownerPoolName, entry := range block.OwnerPoolEntryMap {
		poolName, resourcePackageName := resourcepackage.UnwrapOwnerPoolName(ownerPoolName)
		if !commonstate.IsIsolationPool(poolName) && !commonstate.IsShareNUMABindingPool(poolName) {
			continue
		}

		if allocationInfo := entries[entry.EntryName][entry.SubEntryName]; allocationInfo != nil {
			sourcePool, ok := deriveIsolationSourceSharePool(allocationInfo)
			return sourcePool, resourcePackageName, ok
		}

		for _, containerEntries := range entries {
			if containerEntries.IsPoolEntry() {
				continue
			}
			for _, allocationInfo := range containerEntries {
				if allocationInfo == nil {
					continue
				}
				allocationPool, allocationResourcePackage := resourcepackage.UnwrapOwnerPoolName(
					allocationInfo.GetOwnerPoolName(),
				)
				if allocationPool != poolName || allocationResourcePackage != resourcePackageName {
					continue
				}
				sourcePool, ok := deriveIsolationSourceSharePool(allocationInfo)
				return sourcePool, resourcePackageName, ok
			}
		}
	}

	return "", "", false
}

// buildAdvisorSourceBlockByPool builds a source pool -> blockID mapping. dedicated,
// isolation, and reclaim blocks are excluded because they are not source share pools.
func buildAdvisorSourceBlockByPool(numaToBlocks map[int][]*advisorapi.BlockInfo) map[string]string {
	sourceBlockByPool := make(map[string]string)
	for _, blocks := range numaToBlocks {
		for _, block := range blocks {
			if block == nil {
				continue
			}
			for ownerPoolName := range block.OwnerPoolEntryMap {
				if ownerPoolName == commonstate.PoolNameDedicated ||
					ownerPoolName == commonstate.PoolNameReclaim ||
					commonstate.IsIsolationPool(ownerPoolName) {
					continue
				}
				if _, found := sourceBlockByPool[ownerPoolName]; !found {
					sourceBlockByPool[ownerPoolName] = block.BlockId
				}
			}
		}
	}
	return sourceBlockByPool
}

func buildAdvisorSourceBlockResultByID(blocks []*advisorapi.BlockInfo, sourceBlockByPool map[string]string) (map[string]int, error) {
	sourceBlockResultByID := make(map[string]int)
	if len(sourceBlockByPool) == 0 {
		return sourceBlockResultByID, nil
	}
	for _, block := range blocks {
		if block == nil {
			continue
		}
		for ownerPoolName := range block.OwnerPoolEntryMap {
			if sourceBlockByPool[ownerPoolName] != block.BlockId {
				continue
			}
			result, err := general.CovertUInt64ToInt(block.Result)
			if err != nil {
				return nil, fmt.Errorf("parse source block: %s result failed with error: %v", block.BlockId, err)
			}
			sourceBlockResultByID[block.BlockId] = result
			break
		}
	}
	return sourceBlockResultByID, nil
}

// allocateAdvisorSourceBlocksForCarve preallocates a normal source share block with a
// sourceResult + isolationResult candidate cpuset. allocateShareBlocks then reuses
// tryCarveAdvisorBlockFromSource to carve isolation from this candidate, leaving the
// source block at the sourceResult size requested by the advisor.
func (p *DynamicPolicy) allocateAdvisorSourceBlocksForCarve(
	sourceShareBlocks []*advisorapi.BlockInfo,
	isolationBlocks []*advisorapi.BlockInfo,
	blockCPUSet advisorapi.BlockCPUSet,
	availableCPUs *machine.CPUSet,
	nodeRemainingCPUs *machine.CPUSet,
	globalNonReclaimableCPUSet machine.CPUSet,
	sourceBlockByPool map[string]string,
) error {
	isolationQuantityBySourceBlock := make(map[string]int)
	for _, block := range isolationBlocks {
		sourcePoolName, ok := deriveAdvisorIsolationSourcePool(block, p.state.GetPodEntries())
		if !ok {
			continue
		}
		sourceBlockID, ok := sourceBlockByPool[sourcePoolName]
		if !ok {
			continue
		}

		blockResult, err := general.CovertUInt64ToInt(block.Result)
		if err != nil {
			return fmt.Errorf("parse isolation block: %s result failed with error: %v", block.BlockId, err)
		}
		isolationQuantityBySourceBlock[sourceBlockID] += blockResult
	}
	if len(isolationQuantityBySourceBlock) == 0 {
		return nil
	}

	for _, block := range sourceShareBlocks {
		if block == nil {
			continue
		}
		if _, found := blockCPUSet[block.BlockId]; found {
			continue
		}

		isolationQuantity := isolationQuantityBySourceBlock[block.BlockId]
		if isolationQuantity == 0 {
			continue
		}

		sourceResult, err := general.CovertUInt64ToInt(block.Result)
		if err != nil {
			return fmt.Errorf("parse source block: %s result failed with error: %v", block.BlockId, err)
		}

		combinedResult := sourceResult + isolationQuantity
		currentAvailableCPUs := availableCPUs.Difference(globalNonReclaimableCPUSet)
		cpuset, _, err := calculator.TakeByNUMABalance(p.machineInfo, currentAvailableCPUs, combinedResult)
		if err != nil {
			return fmt.Errorf("allocate source block: %s with combined req: %d failed with error: %v",
				block.BlockId, combinedResult, err)
		}

		blockCPUSet[block.BlockId] = cpuset
		*availableCPUs = availableCPUs.Difference(cpuset)
		*nodeRemainingCPUs = nodeRemainingCPUs.Difference(cpuset)
		general.InfoS("preallocated advisor source block for isolation carve",
			"blockID", block.BlockId,
			"sourceResult", sourceResult,
			"isolationResult", isolationQuantity,
			"allocatedCPUSet", cpuset.String())
	}

	return nil
}

// tryCarveAdvisorBlockFromSource tries to carve an isolation block from an already
// allocated source share block. If the source block does not exist or has not been
// allocated yet, it returns carved=false so the caller can continue the legacy path.
func (p *DynamicPolicy) tryCarveAdvisorBlockFromSource(
	block *advisorapi.BlockInfo,
	sourceBlockByPool map[string]string,
	sourceBlockResultByID map[string]int,
	blockCPUSet advisorapi.BlockCPUSet,
	fallbackCandidate machine.CPUSet,
	availableCPUs *machine.CPUSet,
	nodeRemainingCPUs *machine.CPUSet,
	numaID int,
	blockResult int,
) (bool, error) {
	if block == nil {
		return false, nil
	}
	if _, found := blockCPUSet[block.BlockId]; found {
		return true, nil
	}

	sourcePoolName, ok := deriveAdvisorIsolationSourcePool(block, p.state.GetPodEntries())
	if !ok {
		return false, nil
	}

	sourceBlockID, ok := sourceBlockByPool[sourcePoolName]
	if !ok {
		return false, nil
	}

	sourceCPUSet, ok := blockCPUSet[sourceBlockID]
	if !ok || sourceCPUSet.IsEmpty() {
		return false, nil
	}

	sourceCandidate := sourceCPUSet
	if numaID != commonstate.FakedNUMAID {
		sourceCandidate = sourceCandidate.Intersection(p.machineInfo.CPUDetails.CPUsInNUMANodes(numaID))
	}
	if sourceResult, ok := sourceBlockResultByID[sourceBlockID]; ok {
		sourceSurplusSize := sourceCPUSet.Size() - sourceResult
		if sourceSurplusSize <= 0 {
			sourceCandidate = machine.NewCPUSet()
		} else if sourceCandidate.Size() > sourceSurplusSize {
			var err error
			sourceCandidate, err = calculator.TakeByTopology(p.machineInfo, sourceCandidate, sourceSurplusSize, true)
			if err != nil {
				return false, fmt.Errorf("reserve source block: %s result: %d failed with error: %v",
					sourceBlockID, sourceResult, err)
			}
		}
	}

	carveCandidates := sourceCandidate.Union(fallbackCandidate)
	taken, remainingCandidates, err := p.takeByTieredPreferredCPUs(carveCandidates, []machine.CPUSet{sourceCandidate}, blockResult)
	if err != nil {
		return false, fmt.Errorf("carve advisor block: %s from source pool: %s failed with error: %v",
			block.BlockId, sourcePoolName, err)
	}

	takenFromSource := taken.Intersection(sourceCPUSet)
	takenFromFallback := taken.Difference(sourceCPUSet)
	blockCPUSet[block.BlockId] = taken
	blockCPUSet[sourceBlockID] = sourceCPUSet.Difference(takenFromSource)
	*availableCPUs = availableCPUs.Difference(takenFromFallback)
	*nodeRemainingCPUs = nodeRemainingCPUs.Difference(takenFromFallback)

	general.InfoS("carved advisor block from source share block",
		"blockID", block.BlockId,
		"sourcePoolName", sourcePoolName,
		"sourceBlockID", sourceBlockID,
		"taken", taken.String(),
		"remainingCandidates", remainingCandidates.String())
	return true, nil
}

// planDisjointAdvisorBlocks materializes the negotiated descriptor plan in strict
// phase order. Every dynamic phase receives descriptors sorted by stable owner
// identity, and no partial result escapes on an infeasible phase.
func (p *DynamicPolicy) planDisjointAdvisorBlocks(
	resp *advisorapi.ListAndWatchResponse,
) (advisorapi.BlockCPUSet, error) {
	topology := p.machineInfo.CPUTopology
	allCPUs := topology.CPUDetails.CPUs()
	machineState := p.state.GetMachineState()
	rpPinnedCPUSet := machineState.GetResourcePackagePinnedCPUSet()

	selectorText := p.conf.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector
	disableReclaimSelector, err := general.ParseSelector(selectorText)
	if err != nil {
		return nil, err
	}
	nonReclaimableCPUSet := cpuutil.GetAggResourcePackagePinnedCPUSet(disableReclaimSelector, machineState)
	descriptors, err := buildAdvisorBlockDescriptors(
		resp, topology.CPUDetails, p.state.GetPodEntries(), rpPinnedCPUSet, nonReclaimableCPUSet,
	)
	if err != nil {
		return nil, err
	}

	result := advisorapi.NewBlockCPUSet()
	available, err := p.allocateStaticAndForbiddenPools(resp, result, allCPUs)
	if err != nil {
		return nil, err
	}
	baseAllocatable := available.Clone()
	if err := allocateAdvisorStaticDescriptors(descriptors, result); err != nil {
		return nil, err
	}

	core := filterAdvisorDescriptors(descriptors, func(descriptor advisorBlockDescriptor) bool {
		return descriptor.Class == advisorBlockClassDedicated ||
			descriptor.Class == advisorBlockClassMandatoryReclaim
	})
	available, err = p.solveAdvisorDescriptorPhase(core, available, result, true)
	if err != nil {
		return nil, fmt.Errorf("solve dedicated and mandatory reclaim: %w", err)
	}

	sourceComponents, componentMembers, err := p.advisorSourceIsolationComponents(descriptors)
	if err != nil {
		return nil, err
	}
	for _, sourceBlockID := range sourceComponents {
		available, err = p.solveAdvisorDescriptorPhase(componentMembers[sourceBlockID], available, result, false)
		if err != nil {
			return nil, fmt.Errorf("solve source/isolation component %q: %w", sourceBlockID, err)
		}
	}

	remainingShared := filterAdvisorDescriptors(descriptors, func(descriptor advisorBlockDescriptor) bool {
		_, allocated := result[descriptor.BlockID]
		return descriptor.Class == advisorBlockClassShared && !allocated
	})
	available, err = p.allocateStableAdvisorDescriptors(remainingShared, available, result)
	if err != nil {
		return nil, fmt.Errorf("allocate remaining shared blocks: %w", err)
	}

	protected := advisorDescriptorClassUnion(
		descriptors, result,
		advisorBlockClassStatic, advisorBlockClassDedicated, advisorBlockClassMandatoryReclaim,
	)
	overlapCandidates := baseAllocatable.Difference(protected)
	overlap := filterAdvisorDescriptors(descriptors, func(descriptor advisorBlockDescriptor) bool {
		return descriptor.Class == advisorBlockClassReclaimOverlap
	})
	if _, err := p.allocateStableAdvisorDescriptors(overlap, overlapCandidates, result); err != nil {
		return nil, fmt.Errorf("allocate overlap reclaim blocks: %w", err)
	}

	if err := validateAdvisorDescriptorPlan(descriptors, result); err != nil {
		return nil, err
	}
	return result, nil
}

func allocateAdvisorStaticDescriptors(
	descriptors []advisorBlockDescriptor,
	result advisorapi.BlockCPUSet,
) error {
	for _, descriptor := range descriptors {
		if descriptor.Class != advisorBlockClassStatic {
			continue
		}
		if cpus, found := result[descriptor.BlockID]; found {
			if cpus.Size() != descriptor.Quantity || !cpus.IsSubsetOf(descriptor.Eligible) {
				return fmt.Errorf("static block %q does not satisfy descriptor", descriptor.BlockID)
			}
			continue
		}
		cpus := descriptor.OldPreferred.Intersection(descriptor.Eligible)
		if cpus.Size() != descriptor.Quantity {
			return fmt.Errorf("static block %q has no stable allocation", descriptor.BlockID)
		}
		result[descriptor.BlockID] = cpus
	}
	return nil
}

func (p *DynamicPolicy) solveAdvisorDescriptorPhase(
	descriptors []advisorBlockDescriptor,
	available machine.CPUSet,
	result advisorapi.BlockCPUSet,
	preserveClass bool,
) (machine.CPUSet, error) {
	if len(descriptors) == 0 {
		return available, nil
	}
	demands := make([]partitionDemand, 0, len(descriptors))
	blockIDByDemandKey := make(map[string]string, len(descriptors))
	ordinalByStableKey := make(map[string]int, len(descriptors))
	for _, descriptor := range descriptors {
		if preserveClass &&
			p.isRampUpReclaimHardPartitionEnabled() &&
			descriptor.Class == advisorBlockClassMandatoryReclaim &&
			descriptor.NUMAID == commonstate.FakedNUMAID {
			expanded, err := expandHardPartitionReclaimDemands(
				descriptor, available, p.machineInfo.CPUTopology)
			if err != nil {
				return available, fmt.Errorf("expand hard reclaim block %q: %w", descriptor.BlockID, err)
			}
			for _, demand := range expanded {
				demands = append(demands, demand)
				blockIDByDemandKey[demand.key] = descriptor.BlockID
			}
			continue
		}

		class := descriptor.Class
		if !preserveClass {
			class = advisorBlockClassDedicated
		}
		stableKey := fmt.Sprintf("%s\x00%d\x00%s",
			descriptor.ComponentKey, descriptor.Quantity, strings.Join(descriptor.Owners, "\x1f"))
		ordinal := ordinalByStableKey[stableKey]
		ordinalByStableKey[stableKey] = ordinal + 1
		demandKey := fmt.Sprintf("%s\x00%d", stableKey, ordinal)
		demands = append(demands, partitionDemand{
			key:       demandKey,
			quantity:  descriptor.Quantity,
			eligible:  descriptor.Eligible.Intersection(available),
			preferred: descriptor.OldPreferred,
			class:     class,
		})
		blockIDByDemandKey[demandKey] = descriptor.BlockID
	}
	assignments, err := solveDisjointPartitions(demands, p.machineInfo.CPUTopology)
	if err != nil {
		return available, err
	}
	used := machine.NewCPUSet()
	for demandKey, cpus := range assignments {
		blockID := blockIDByDemandKey[demandKey]
		result[blockID] = result[blockID].Union(cpus)
		used = used.Union(cpus)
	}
	return available.Difference(used), nil
}

func (p *DynamicPolicy) allocateStableAdvisorDescriptors(
	descriptors []advisorBlockDescriptor,
	available machine.CPUSet,
	result advisorapi.BlockCPUSet,
) (machine.CPUSet, error) {
	for _, descriptor := range descriptors {
		candidates := available.Intersection(descriptor.Eligible)
		cpus, remaining, err := p.takeByTieredPreferredCPUs(
			candidates, []machine.CPUSet{descriptor.OldPreferred}, descriptor.Quantity,
		)
		if err != nil {
			return available, fmt.Errorf("allocate block %q: %w", descriptor.BlockID, err)
		}
		result[descriptor.BlockID] = cpus
		available = available.Difference(candidates).Union(remaining)
	}
	return available, nil
}

func (p *DynamicPolicy) advisorSourceIsolationComponents(
	descriptors []advisorBlockDescriptor,
) ([]string, map[string][]advisorBlockDescriptor, error) {
	type sourceDomain struct {
		poolName            string
		resourcePackageName string
		numaID              int
	}

	sourceByDomain := make(map[sourceDomain]advisorBlockDescriptor)
	for _, descriptor := range descriptors {
		if descriptor.Class != advisorBlockClassShared || advisorDescriptorIsIsolation(descriptor) {
			continue
		}
		for _, owner := range descriptor.Owners {
			poolName, resourcePackageName, ok := advisorDescriptorOwnerDomain(owner)
			if !ok {
				continue
			}
			domain := sourceDomain{
				poolName: poolName, resourcePackageName: resourcePackageName, numaID: descriptor.NUMAID,
			}
			if source, found := sourceByDomain[domain]; found && source.BlockID != descriptor.BlockID {
				firstBlockID, secondBlockID := source.BlockID, descriptor.BlockID
				if secondBlockID < firstBlockID {
					firstBlockID, secondBlockID = secondBlockID, firstBlockID
				}
				return nil, nil, fmt.Errorf(
					"source domain pool %q resource package %q numa %d has conflicting descriptors %q and %q",
					poolName, resourcePackageName, descriptor.NUMAID, firstBlockID, secondBlockID,
				)
			}
			sourceByDomain[domain] = descriptor
		}
	}

	members := make(map[string][]advisorBlockDescriptor)
	for _, descriptor := range descriptors {
		if descriptor.Class != advisorBlockClassShared || !advisorDescriptorIsIsolation(descriptor) {
			continue
		}
		domain, err := advisorIsolationDescriptorSourceDomain(descriptor, p.state.GetPodEntries())
		if err != nil {
			return nil, nil, fmt.Errorf("derive isolation block %q source domain: %w", descriptor.BlockID, err)
		}
		source, ok := sourceByDomain[domain]
		if !ok {
			continue
		}
		if len(members[source.BlockID]) == 0 {
			members[source.BlockID] = append(members[source.BlockID], source)
		}
		members[source.BlockID] = append(members[source.BlockID], descriptor)
	}

	keys := make([]string, 0, len(members))
	for _, descriptor := range descriptors {
		if _, found := members[descriptor.BlockID]; !found {
			continue
		}
		keys = append(keys, descriptor.BlockID)
		sort.Slice(members[descriptor.BlockID], func(i, j int) bool {
			return advisorBlockDescriptorLess(members[descriptor.BlockID][i], members[descriptor.BlockID][j])
		})
	}
	return keys, members, nil
}

func advisorIsolationDescriptorSourceDomain(
	descriptor advisorBlockDescriptor,
	entries state.PodEntries,
) (struct {
	poolName            string
	resourcePackageName string
	numaID              int
}, error) {
	var resolved struct {
		poolName            string
		resourcePackageName string
		numaID              int
	}
	resolvedSet := false
	for _, owner := range descriptor.Owners {
		poolName, entryName, subEntryName, resourcePackageName, ok := advisorDescriptorOwner(owner)
		if !ok {
			return resolved, fmt.Errorf("owner %q is malformed", owner)
		}
		if !commonstate.IsIsolationPool(poolName) {
			continue
		}
		allocationInfo := entries[entryName][subEntryName]
		if allocationInfo == nil {
			return resolved, fmt.Errorf("owner %q has no state allocation", owner)
		}
		allocationPool, allocationResourcePackage := resourcepackage.UnwrapOwnerPoolName(
			allocationInfo.GetOwnerPoolName())
		if allocationPool != poolName || allocationResourcePackage != resourcePackageName {
			return resolved, fmt.Errorf("owner %q disagrees with state owner domain", owner)
		}
		sourcePool, ok := deriveIsolationSourceSharePool(allocationInfo)
		if !ok {
			return resolved, fmt.Errorf("owner %q has no source pool", owner)
		}
		sourcePool, _ = resourcepackage.UnwrapOwnerPoolName(sourcePool)
		current := struct {
			poolName            string
			resourcePackageName string
			numaID              int
		}{
			poolName: sourcePool, resourcePackageName: resourcePackageName, numaID: descriptor.NUMAID,
		}
		if resolvedSet && resolved != current {
			return resolved, fmt.Errorf("aliases resolve to different source domains")
		}
		resolved = current
		resolvedSet = true
	}
	if !resolvedSet {
		return resolved, fmt.Errorf("descriptor has no resolvable isolation owners")
	}
	return resolved, nil
}

func filterAdvisorDescriptors(
	descriptors []advisorBlockDescriptor,
	keep func(advisorBlockDescriptor) bool,
) []advisorBlockDescriptor {
	result := make([]advisorBlockDescriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if keep(descriptor) {
			result = append(result, descriptor)
		}
	}
	return result
}

func advisorDescriptorPoolNames(descriptor advisorBlockDescriptor) []string {
	pools := make([]string, 0, len(descriptor.Owners))
	for _, owner := range descriptor.Owners {
		poolName, _, ok := advisorDescriptorOwnerDomain(owner)
		if ok {
			pools = append(pools, poolName)
		}
	}
	return pools
}

func advisorDescriptorOwnerDomain(owner string) (string, string, bool) {
	poolName, _, _, resourcePackageName, ok := advisorDescriptorOwner(owner)
	return poolName, resourcePackageName, ok
}

func advisorDescriptorOwner(owner string) (string, string, string, string, bool) {
	parts := strings.Split(owner, "\x00")
	if len(parts) != 4 {
		return "", "", "", "", false
	}
	return parts[0], parts[1], parts[2], parts[3], true
}

func advisorDescriptorIsIsolation(descriptor advisorBlockDescriptor) bool {
	for _, poolName := range advisorDescriptorPoolNames(descriptor) {
		unwrappedPoolName, _ := resourcepackage.UnwrapOwnerPoolName(poolName)
		if commonstate.IsIsolationPool(unwrappedPoolName) {
			return true
		}
	}
	return false
}

func advisorDescriptorClassUnion(
	descriptors []advisorBlockDescriptor,
	result advisorapi.BlockCPUSet,
	classes ...advisorBlockClass,
) machine.CPUSet {
	wanted := make(map[advisorBlockClass]struct{}, len(classes))
	for _, class := range classes {
		wanted[class] = struct{}{}
	}
	union := machine.NewCPUSet()
	for _, descriptor := range descriptors {
		if _, ok := wanted[descriptor.Class]; ok {
			union = union.Union(result[descriptor.BlockID])
		}
	}
	return union
}

func validateAdvisorDescriptorPlan(
	descriptors []advisorBlockDescriptor,
	result advisorapi.BlockCPUSet,
) error {
	for _, descriptor := range descriptors {
		cpus, found := result[descriptor.BlockID]
		if !found {
			return fmt.Errorf("block %q has no planned cpuset", descriptor.BlockID)
		}
		if cpus.Size() != descriptor.Quantity {
			return fmt.Errorf("block %q planned quantity %d does not match %d",
				descriptor.BlockID, cpus.Size(), descriptor.Quantity)
		}
		if !cpus.IsSubsetOf(descriptor.Eligible) {
			return fmt.Errorf("block %q planned cpuset violates eligibility", descriptor.BlockID)
		}
	}
	return nil
}
