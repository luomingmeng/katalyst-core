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

func planHardReclaimPartition(in hardReclaimPartitionInput) (*hardReclaimPartitionPlan, error) {
	if in.topology == nil {
		return nil, fmt.Errorf("hard reclaim partition topology is nil")
	}

	plan := &hardReclaimPartitionPlan{
		reclaim:   machine.NewCPUSet(),
		donorCPUs: make(map[string]machine.CPUSet, len(in.donors)),
	}
	donorByKey := make(map[string]hardReclaimPartitionDonor, len(in.donors))
	donorKeys := make([]string, 0, len(in.donors))
	donorGroupByKey := make(map[string]string, len(in.donors))
	groupMinimum := make(map[string]int)
	groupDonorKeys := make(map[string][]string)
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
		donorGroupByKey[donor.key] = groupKey
		groupDonorKeys[groupKey] = append(groupDonorKeys[groupKey], donor.key)
		groupMinimum[groupKey] = general.Max(groupMinimum[groupKey], int(math.Ceil(donor.requestQuantity)))
		donorKeys = append(donorKeys, donor.key)
		plan.donorCPUs[donor.key] = donor.cpus.Clone()
		allDonorCPUs = allDonorCPUs.Union(donor.cpus)
	}
	sort.Strings(donorKeys)

	numaIDs := make([]int, 0, len(in.targetByNUMA))
	for numaID := range in.targetByNUMA {
		numaIDs = append(numaIDs, numaID)
	}
	sort.Ints(numaIDs)

	for _, numaID := range numaIDs {
		target := in.targetByNUMA[numaID]
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

		// fill from the non-donor pool first (currently pinned reclaim + free
		// cpus), selecting complete physical cores only. stable and free are
		// merged into one candidate universe so a core whose siblings are split
		// across the two (one sibling already reclaimed, the other still free)
		// is still selectable as a whole core; `currentReclaim` is passed as the
		// preference so stable cores are consumed before fresh ones (minimizing
		// churn) — invariant B.
		stableReclaim := in.currentReclaim.Intersection(eligible).Difference(allDonorCPUs)
		free := in.free.Intersection(eligible).
			Difference(in.currentReclaim).
			Difference(allDonorCPUs)
		nonDonorPool := stableReclaim.Union(free)
		selected := takeCoreAlignedCPUSet(in.topology, nonDonorPool, in.currentReclaim, target)
		need := target - selected.Size()

		for _, key := range donorKeys {
			if need == 0 {
				break
			}
			remainingDonor := plan.donorCPUs[key]
			groupKey := donorGroupByKey[key]
			groupRemaining := 0
			for _, groupDonorKey := range groupDonorKeys[groupKey] {
				groupRemaining += plan.donorCPUs[groupDonorKey].Size()
			}
			excess := groupRemaining - groupMinimum[groupKey]
			if excess <= 0 {
				continue
			}
			candidates := remainingDonor.Intersection(eligible)
			limit := need
			if excess < limit {
				limit = excess
			}
			// hand donor excess back in complete cores only: preferring the cores
			// that already overlap the current reclaim set keeps churn low, and
			// takeCoreAlignedCPUSet crops `limit` down to a whole-core multiple so
			// the donor never loses (nor reclaim gains) a lone SMT sibling.
			taken := takeCoreAlignedCPUSet(in.topology, candidates, in.currentReclaim, limit)
			selected = selected.Union(taken)
			plan.donorCPUs[key] = remainingDonor.Difference(taken)
			need -= taken.Size()
		}
		if need > 0 {
			return nil, fmt.Errorf("NUMA %d needs %d more reclaim CPUs", numaID, need)
		}
		plan.reclaim = plan.reclaim.Union(selected)
	}

	if err := assertCoreAligned(plan.reclaim, in.topology); err != nil {
		return nil, fmt.Errorf("hard reclaim partition plan violated core alignment: %w", err)
	}
	return plan, nil
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
