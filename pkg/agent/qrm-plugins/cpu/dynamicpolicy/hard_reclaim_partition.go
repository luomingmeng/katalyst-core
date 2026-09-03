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
	"math"
	"sort"

	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type hardReclaimPartitionDonor struct {
	key             string
	groupKey        string
	cpus            machine.CPUSet
	requestQuantity float64
}

type hardReclaimPartitionInput struct {
	topology        *machine.CPUTopology
	targetByNUMA    map[int]int
	currentReclaim  machine.CPUSet
	free            machine.CPUSet
	reclaimEligible machine.CPUSet
	donors          []hardReclaimPartitionDonor
}

type hardReclaimPartitionPlan struct {
	reclaim   machine.CPUSet
	donorCPUs map[string]machine.CPUSet
}

const (
	hardReclaimCoreSelectionFrontierWidth = 64
	hardReclaimCoreSelectionMaxStates     = 4096
)

type hardReclaimCoreSelectionState struct {
	selected       machine.CPUSet
	selectedByNUMA []int
	donations      []int
	retained       int
	donated        int
}

type hardReclaimCoreSelectionCandidate struct {
	coreAlignedCandidate
	numaIndex int
}

func planHardReclaimPartition(in hardReclaimPartitionInput) (*hardReclaimPartitionPlan, error) {
	if in.topology == nil {
		return nil, fmt.Errorf("hard reclaim partition topology is nil")
	}

	plan := &hardReclaimPartitionPlan{
		reclaim:   machine.NewCPUSet(),
		donorCPUs: make(map[string]machine.CPUSet, len(in.donors)),
	}
	donorByKey := make(map[string]hardReclaimPartitionDonor, len(in.donors))
	groupMinimum := make(map[string]int)
	groupCPUs := make(map[string]machine.CPUSet)
	groupByCPU := make(map[int]string)
	allDonorCPUs := machine.NewCPUSet()
	for _, donor := range in.donors {
		if donor.key == "" {
			return nil, fmt.Errorf("hard reclaim partition donor has empty key")
		}
		if _, found := donorByKey[donor.key]; found {
			return nil, fmt.Errorf("hard reclaim partition has duplicate donor %q", donor.key)
		}
		if math.IsNaN(donor.requestQuantity) || math.IsInf(donor.requestQuantity, 0) || donor.requestQuantity < 0 {
			return nil, fmt.Errorf("hard reclaim partition donor %q has invalid request quantity %v",
				donor.key, donor.requestQuantity)
		}
		groupKey := donor.groupKey
		if groupKey == "" {
			groupKey = donor.key
		}
		donorByKey[donor.key] = donor
		groupMinimum[groupKey] = general.Max(groupMinimum[groupKey], int(math.Ceil(donor.requestQuantity)))
		plan.donorCPUs[donor.key] = donor.cpus.Clone()
		for _, cpu := range donor.cpus.ToSliceInt() {
			if existing, found := groupByCPU[cpu]; found && existing != groupKey {
				return nil, fmt.Errorf(
					"hard reclaim partition has overlapping donor ownership on CPU %d: %q and %q",
					cpu, existing, groupKey)
			}
			groupByCPU[cpu] = groupKey
		}
		groupCPUs[groupKey] = groupCPUs[groupKey].Union(donor.cpus)
		allDonorCPUs = allDonorCPUs.Union(donor.cpus)
	}
	groupDonationLimit := make(map[string]int, len(groupCPUs))
	for groupKey, cpus := range groupCPUs {
		groupDonationLimit[groupKey] = cpus.Size() - groupMinimum[groupKey]
		if groupDonationLimit[groupKey] < 0 {
			groupDonationLimit[groupKey] = 0
		}
	}

	numaIDs := make([]int, 0, len(in.targetByNUMA))
	for numaID := range in.targetByNUMA {
		numaIDs = append(numaIDs, numaID)
	}
	sort.Ints(numaIDs)

	candidates := make([]hardReclaimCoreSelectionCandidate, 0)
	targets := make([]int, len(numaIDs))
	for numaIndex, numaID := range numaIDs {
		target := in.targetByNUMA[numaID]
		targets[numaIndex] = target
		if target < 0 {
			return nil, fmt.Errorf("NUMA %d has negative hard reclaim target %d", numaID, target)
		}
		numaCPUs := in.topology.CPUDetails.CPUsInNUMANodes(numaID)
		if numaCPUs.IsEmpty() {
			return nil, fmt.Errorf("hard reclaim target references unknown NUMA %d", numaID)
		}
		eligible := in.reclaimEligible.Intersection(numaCPUs)
		if eligible.Size() < target {
			return nil, fmt.Errorf("NUMA %d reclaim eligibility %d is smaller than target %d",
				numaID, eligible.Size(), target)
		}

		source := in.currentReclaim.Union(in.free).Union(allDonorCPUs).Intersection(eligible)
		for _, candidate := range coreAlignedCandidates(in.topology, source, in.currentReclaim) {
			candidates = append(candidates, hardReclaimCoreSelectionCandidate{
				coreAlignedCandidate: candidate,
				numaIndex:            numaIndex,
			})
		}
	}
	selected, err := selectHardReclaimCoresByNUMAWithFrontier(
		candidates, numaIDs, targets, in.currentReclaim, groupCPUs, groupDonationLimit)
	if err != nil {
		return nil, err
	}
	plan.reclaim = selected

	if err := assertCoreAligned(plan.reclaim, in.topology); err != nil {
		return nil, fmt.Errorf("hard reclaim partition plan violated core alignment: %w", err)
	}
	for key, donor := range donorByKey {
		plan.donorCPUs[key] = donor.cpus.Difference(plan.reclaim)
	}
	return plan, nil
}

func selectHardReclaimCoresWithFrontier(
	candidates []coreAlignedCandidate,
	target int,
	currentReclaim machine.CPUSet,
	groupCPUs map[string]machine.CPUSet,
	groupDonationLimit map[string]int,
) (machine.CPUSet, error) {
	candidatesByNUMA := make([]hardReclaimCoreSelectionCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		candidatesByNUMA = append(candidatesByNUMA, hardReclaimCoreSelectionCandidate{
			coreAlignedCandidate: candidate,
		})
	}
	return selectHardReclaimCoresByNUMAWithFrontier(
		candidatesByNUMA, []int{0}, []int{target}, currentReclaim, groupCPUs, groupDonationLimit)
}

func selectHardReclaimCoresByNUMAWithFrontier(
	candidates []hardReclaimCoreSelectionCandidate,
	numaIDs []int,
	targets []int,
	currentReclaim machine.CPUSet,
	groupCPUs map[string]machine.CPUSet,
	groupDonationLimit map[string]int,
) (machine.CPUSet, error) {
	groupKeys := make([]string, 0, len(groupCPUs))
	for groupKey := range groupCPUs {
		groupKeys = append(groupKeys, groupKey)
	}
	sort.Strings(groupKeys)

	states := []hardReclaimCoreSelectionState{{
		selected:       machine.NewCPUSet(),
		selectedByNUMA: make([]int, len(targets)),
		donations:      make([]int, len(groupKeys)),
	}}
	frontierTruncated := false
	for _, candidate := range candidates {
		nextByKey := make(map[string]hardReclaimCoreSelectionState, len(states)*2)
		for _, state := range states {
			addHardReclaimCoreSelectionState(nextByKey, state)
			if state.selectedByNUMA[candidate.numaIndex]+candidate.cpus.Size() >
				targets[candidate.numaIndex] {
				continue
			}
			next := hardReclaimCoreSelectionState{
				selected:       state.selected.Union(candidate.cpus),
				selectedByNUMA: append([]int(nil), state.selectedByNUMA...),
				donations:      append([]int(nil), state.donations...),
				retained:       state.retained + candidate.cpus.Intersection(currentReclaim).Size(),
				donated:        state.donated,
			}
			next.selectedByNUMA[candidate.numaIndex] += candidate.cpus.Size()
			allowed := true
			for i, groupKey := range groupKeys {
				donated := candidate.cpus.Intersection(groupCPUs[groupKey]).Size()
				next.donations[i] += donated
				next.donated += donated
				if next.donations[i] > groupDonationLimit[groupKey] {
					allowed = false
					break
				}
			}
			if allowed {
				addHardReclaimCoreSelectionState(nextByKey, next)
			}
		}
		var truncated bool
		states, truncated = pruneHardReclaimCoreSelectionStates(nextByKey)
		frontierTruncated = frontierTruncated || truncated
	}

	var best *hardReclaimCoreSelectionState
	for i := range states {
		if !intSlicesEqual(states[i].selectedByNUMA, targets) {
			continue
		}
		if best == nil || hardReclaimCoreSelectionStateLess(states[i], *best) {
			candidate := states[i]
			best = &candidate
		}
	}
	if best == nil {
		if frontierTruncated {
			return machine.NewCPUSet(), fmt.Errorf(
				"search frontier truncated at width %d before proving a feasible reclaim selection",
				hardReclaimCoreSelectionFrontierWidth)
		}
		var bestPartial *hardReclaimCoreSelectionState
		for _, state := range states {
			if bestPartial == nil || state.selected.Size() > bestPartial.selected.Size() ||
				(state.selected.Size() == bestPartial.selected.Size() &&
					hardReclaimCoreSelectionStateLess(state, *bestPartial)) {
				candidate := state
				bestPartial = &candidate
			}
		}
		for i, target := range targets {
			if bestPartial.selectedByNUMA[i] < target {
				return machine.NewCPUSet(), fmt.Errorf(
					"NUMA %d needs %d more reclaim CPUs",
					numaIDs[i], target-bestPartial.selectedByNUMA[i])
			}
		}
		return machine.NewCPUSet(), fmt.Errorf("no feasible hard reclaim selection")
	}
	return best.selected, nil
}

func intSlicesEqual(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func addHardReclaimCoreSelectionState(
	states map[string]hardReclaimCoreSelectionState,
	candidate hardReclaimCoreSelectionState,
) {
	key := fmt.Sprintf("%v/%v", candidate.selectedByNUMA, candidate.donations)
	current, found := states[key]
	if !found || hardReclaimCoreSelectionStateLess(candidate, current) {
		states[key] = candidate
	}
}

func pruneHardReclaimCoreSelectionStates(
	states map[string]hardReclaimCoreSelectionState,
) ([]hardReclaimCoreSelectionState, bool) {
	byProgress := make(map[string][]hardReclaimCoreSelectionState)
	for _, state := range states {
		progress := fmt.Sprint(state.selectedByNUMA)
		byProgress[progress] = append(byProgress[progress], state)
	}
	progresses := make([]string, 0, len(byProgress))
	for progress := range byProgress {
		progresses = append(progresses, progress)
	}
	sort.Strings(progresses)

	result := make([]hardReclaimCoreSelectionState, 0, len(states))
	truncated := false
	for _, progress := range progresses {
		bucket := byProgress[progress]
		sort.Slice(bucket, func(i, j int) bool {
			return hardReclaimCoreSelectionStateLess(bucket[i], bucket[j])
		})
		if len(bucket) > hardReclaimCoreSelectionFrontierWidth {
			truncated = true
			bucket = bucket[:hardReclaimCoreSelectionFrontierWidth]
		}
		result = append(result, bucket...)
	}
	if len(result) > hardReclaimCoreSelectionMaxStates {
		truncated = true
		sort.Slice(result, func(i, j int) bool {
			if result[i].selected.Size() != result[j].selected.Size() {
				return result[i].selected.Size() > result[j].selected.Size()
			}
			for k := range result[i].selectedByNUMA {
				if result[i].selectedByNUMA[k] != result[j].selectedByNUMA[k] {
					return result[i].selectedByNUMA[k] < result[j].selectedByNUMA[k]
				}
			}
			return hardReclaimCoreSelectionStateLess(result[i], result[j])
		})
		result = result[:hardReclaimCoreSelectionMaxStates]
	}
	return result, truncated
}

func hardReclaimCoreSelectionStateLess(
	left, right hardReclaimCoreSelectionState,
) bool {
	if left.retained != right.retained {
		return left.retained > right.retained
	}
	if left.donated != right.donated {
		return left.donated < right.donated
	}
	leftCPUs, rightCPUs := left.selected.ToSliceInt(), right.selected.ToSliceInt()
	for i := range leftCPUs {
		if leftCPUs[i] != rightCPUs[i] {
			return leftCPUs[i] < rightCPUs[i]
		}
	}
	return false
}

func pinHardReclaimPartitionDemands(
	demands []partitionDemand,
	available machine.CPUSet,
	topology *machine.CPUTopology,
) ([]partitionDemand, error) {
	targetByNUMA := make(map[int]int)
	currentReclaim := machine.NewCPUSet()
	reclaimEligible := machine.NewCPUSet()
	allCurrentlyOwned := machine.NewCPUSet()
	donors := make([]hardReclaimPartitionDonor, 0)

	for _, demand := range demands {
		allCurrentlyOwned = allCurrentlyOwned.Union(demand.preferred)
		switch demand.class {
		case advisorBlockClassMandatoryReclaim:
			numaIDs := topology.CPUDetails.KeepOnly(demand.eligible).NUMANodes().ToSliceInt()
			if len(numaIDs) != 1 {
				return nil, fmt.Errorf("hard reclaim demand %q must belong to exactly one NUMA", demand.key)
			}
			targetByNUMA[numaIDs[0]] += demand.quantity
			currentReclaim = currentReclaim.Union(demand.preferred)
			reclaimEligible = reclaimEligible.Union(demand.eligible)
		case advisorBlockClassDedicated:
			requestQuantity := demand.requestQuantity
			if requestQuantity <= 0 {
				requestQuantity = float64(demand.quantity)
			}
			donors = append(donors, hardReclaimPartitionDonor{
				key:             demand.key,
				groupKey:        demand.requestGroupKey,
				cpus:            demand.preferred,
				requestQuantity: requestQuantity,
			})
		}
	}
	if len(targetByNUMA) == 0 {
		return append([]partitionDemand(nil), demands...), nil
	}

	plan, err := planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    targetByNUMA,
		currentReclaim:  currentReclaim,
		free:            available.Difference(allCurrentlyOwned),
		reclaimEligible: reclaimEligible,
		donors:          donors,
	})
	if err != nil {
		return nil, err
	}

	pinned := append([]partitionDemand(nil), demands...)
	for i := range pinned {
		switch pinned[i].class {
		case advisorBlockClassMandatoryReclaim:
			pinned[i].eligible = pinned[i].eligible.Intersection(plan.reclaim)
			pinned[i].preferred = pinned[i].preferred.Intersection(plan.reclaim)
		case advisorBlockClassDedicated:
			pinned[i].eligible = pinned[i].eligible.Difference(plan.reclaim)
			pinned[i].preferred = plan.donorCPUs[pinned[i].key].Intersection(pinned[i].eligible)
		}
	}
	return pinned, nil
}
