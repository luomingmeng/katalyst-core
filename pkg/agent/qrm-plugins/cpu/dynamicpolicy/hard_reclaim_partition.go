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
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

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

	hardReclaimReplacementMaxCandidateStates = 100_000
	hardReclaimReplacementMaxTerminalSolves  = 4096
)

type hardReclaimSelectionFailureReason string

const (
	hardReclaimFailureInsufficientWholeCore hardReclaimSelectionFailureReason = "insufficient_whole_core"
	hardReclaimFailureDonorFloor            hardReclaimSelectionFailureReason = "donor_floor"
	hardReclaimFailureSearchBudget          hardReclaimSelectionFailureReason = "search_budget"
)

type hardReclaimSelectionError struct {
	reason  hardReclaimSelectionFailureReason
	numaID  int
	deficit int
	cause   error
}

func (e *hardReclaimSelectionError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return fmt.Sprintf("%s: NUMA %d deficit %d", e.reason, e.numaID, e.deficit)
}

func (e *hardReclaimSelectionError) Unwrap() error {
	return e.cause
}

type hardReclaimReplacementOptions struct {
	maxCandidateStates          int
	maxTerminalSolves           int
	maxPartitionAssignmentEdges int
	maxPartitionFlowOperations  int
}

type hardReclaimReplacementProof struct {
	reclaimBefore machine.CPUSet
	reclaimAfter  machine.CPUSet

	dedicatedBeforeByGroup map[string]machine.CPUSet
	dedicatedAfterByGroup  map[string]machine.CPUSet
	partialBeforeCores     int
}

func defaultHardReclaimReplacementOptions() hardReclaimReplacementOptions {
	return hardReclaimReplacementOptions{
		maxCandidateStates:          hardReclaimReplacementMaxCandidateStates,
		maxTerminalSolves:           hardReclaimReplacementMaxTerminalSolves,
		maxPartitionAssignmentEdges: partitionAssignmentEdgeBudget,
		maxPartitionFlowOperations:  partitionFlowOperationBudget,
	}
}

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

type hardReclaimReplacementTerminal struct {
	cpus               machine.CPUSet
	partialBeforeCores int
	retained           int
}

type hardReclaimReplacementResult struct {
	proof *hardReclaimReplacementProof
}

type hardReclaimReplacementGlobalState struct {
	reclaimAfter    machine.CPUSet
	reclaimRetained int
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
	if frontierTruncated {
		cause := fmt.Errorf(
			"search frontier truncated at width %d before proving an optimal reclaim selection",
			hardReclaimCoreSelectionFrontierWidth)
		return machine.NewCPUSet(), &hardReclaimSelectionError{
			reason: hardReclaimFailureSearchBudget,
			cause:  cause,
		}
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
				deficit := target - bestPartial.selectedByNUMA[i]
				reason := hardReclaimFailureDonorFloor
				wholeCoreCapacity := 0
				for _, candidate := range candidates {
					if candidate.numaIndex == i {
						wholeCoreCapacity += candidate.cpus.Size()
					}
				}
				if wholeCoreCapacity < target {
					reason = hardReclaimFailureInsufficientWholeCore
				}
				cause := fmt.Errorf("NUMA %d needs %d more reclaim CPUs", numaIDs[i], deficit)
				return machine.NewCPUSet(), &hardReclaimSelectionError{
					reason:  reason,
					numaID:  numaIDs[i],
					deficit: deficit,
					cause:   cause,
				}
			}
		}
		cause := fmt.Errorf("no feasible hard reclaim selection")
		return machine.NewCPUSet(), &hardReclaimSelectionError{
			reason: hardReclaimFailureDonorFloor,
			cause:  cause,
		}
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

func validateHardReclaimReplacement(
	demands []partitionDemand,
	assignments map[string]machine.CPUSet,
	topology *machine.CPUTopology,
	targetByNUMA map[int]int,
) (*hardReclaimReplacementProof, error) {
	if topology == nil {
		return nil, fmt.Errorf("hard reclaim replacement topology is nil")
	}

	sortedDemands := append([]partitionDemand(nil), demands...)
	sort.Slice(sortedDemands, func(i, j int) bool {
		return sortedDemands[i].key < sortedDemands[j].key
	})
	proof := &hardReclaimReplacementProof{
		reclaimBefore:          machine.NewCPUSet(),
		reclaimAfter:           machine.NewCPUSet(),
		dedicatedBeforeByGroup: make(map[string]machine.CPUSet),
		dedicatedAfterByGroup:  make(map[string]machine.CPUSet),
	}
	assigned := machine.NewCPUSet()
	seenKeys := make(map[string]struct{}, len(sortedDemands))
	for _, demand := range sortedDemands {
		if demand.key == "" {
			return nil, fmt.Errorf("hard reclaim replacement demand has empty key")
		}
		if _, found := seenKeys[demand.key]; found {
			return nil, fmt.Errorf("hard reclaim replacement has duplicate demand %q", demand.key)
		}
		seenKeys[demand.key] = struct{}{}
		for _, cpu := range demand.preferred.ToSliceInt() {
			if _, found := topology.CPUDetails[cpu]; !found {
				return nil, fmt.Errorf(
					"hard reclaim replacement demand %q preferred CPU %d is missing from topology",
					demand.key, cpu)
			}
		}

		assignment, found := assignments[demand.key]
		if !found {
			return nil, fmt.Errorf("hard reclaim replacement is missing assignment %q", demand.key)
		}
		if assignment.Size() != demand.quantity {
			return nil, fmt.Errorf(
				"hard reclaim replacement assignment %q has size %d, want %d",
				demand.key, assignment.Size(), demand.quantity)
		}
		if !assignment.IsSubsetOf(demand.eligible) {
			return nil, fmt.Errorf(
				"hard reclaim replacement assignment %q is outside eligibility", demand.key)
		}
		if !assignment.Intersection(assigned).IsEmpty() {
			return nil, fmt.Errorf(
				"hard reclaim replacement assignment %q overlaps another assignment", demand.key)
		}
		for _, cpu := range assignment.ToSliceInt() {
			if _, found := topology.CPUDetails[cpu]; !found {
				return nil, fmt.Errorf(
					"hard reclaim replacement assignment %q references CPU %d missing from topology",
					demand.key, cpu)
			}
		}
		assigned = assigned.Union(assignment)

		switch demand.class {
		case advisorBlockClassMandatoryReclaim:
			proof.reclaimBefore = proof.reclaimBefore.Union(demand.preferred)
			proof.reclaimAfter = proof.reclaimAfter.Union(assignment)
		case advisorBlockClassDedicated:
			groupKey := demand.requestGroupKey
			if groupKey == "" {
				groupKey = demand.key
			}
			if math.IsNaN(demand.requestQuantity) ||
				math.IsInf(demand.requestQuantity, 0) ||
				demand.requestQuantity < 0 {
				return nil, fmt.Errorf(
					"hard reclaim replacement demand %q has invalid request quantity %v",
					demand.key, demand.requestQuantity)
			}
			proof.dedicatedBeforeByGroup[groupKey] =
				proof.dedicatedBeforeByGroup[groupKey].Union(demand.preferred)
			proof.dedicatedAfterByGroup[groupKey] =
				proof.dedicatedAfterByGroup[groupKey].Union(assignment)
		case advisorBlockClassShared:
		default:
			return nil, fmt.Errorf(
				"hard reclaim replacement demand %q has unsupported class %q",
				demand.key, demand.class)
		}
	}
	if len(assignments) != len(seenKeys) {
		for key := range assignments {
			if _, found := seenKeys[key]; !found {
				return nil, fmt.Errorf("hard reclaim replacement has unexpected assignment %q", key)
			}
		}
	}

	if err := assertCoreAligned(proof.reclaimAfter, topology); err != nil {
		return nil, fmt.Errorf("hard reclaim replacement final reclaim is not core-aligned: %w", err)
	}
	for _, candidate := range coreAlignedCandidates(topology, proof.reclaimAfter, proof.reclaimBefore) {
		retained := candidate.cpus.Intersection(proof.reclaimBefore).Size()
		if retained > 0 && retained < candidate.cpus.Size() {
			proof.partialBeforeCores++
		}
	}
	actualByNUMA := make(map[int]int)
	for _, cpu := range proof.reclaimAfter.ToSliceInt() {
		actualByNUMA[topology.CPUDetails[cpu].NUMANodeID]++
	}
	numaIDs := make(map[int]struct{}, len(targetByNUMA)+len(actualByNUMA))
	for numaID := range targetByNUMA {
		numaIDs[numaID] = struct{}{}
	}
	for numaID := range actualByNUMA {
		numaIDs[numaID] = struct{}{}
	}
	for _, numaID := range sortedHardReclaimNUMAIDs(numaIDs) {
		if actualByNUMA[numaID] != targetByNUMA[numaID] {
			return nil, fmt.Errorf(
				"hard reclaim replacement NUMA %d has reclaim size %d, want %d",
				numaID, actualByNUMA[numaID], targetByNUMA[numaID])
		}
	}

	groupKeys := make([]string, 0, len(proof.dedicatedBeforeByGroup))
	for groupKey := range proof.dedicatedBeforeByGroup {
		groupKeys = append(groupKeys, groupKey)
	}
	sort.Strings(groupKeys)
	for _, groupKey := range groupKeys {
		before := proof.dedicatedBeforeByGroup[groupKey]
		after := proof.dedicatedAfterByGroup[groupKey]
		for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceInt() {
			numaCPUs := topology.CPUDetails.CPUsInNUMANodes(numaID)
			oldOwned := before.Intersection(numaCPUs).Size()
			newOwned := after.Intersection(numaCPUs).Size()
			if oldOwned != newOwned {
				return nil, fmt.Errorf(
					"dedicated group %q replacement changed NUMA %d ownership from %d to %d",
					groupKey, numaID, oldOwned, newOwned)
			}
		}
	}
	return proof, nil
}

func sortedHardReclaimNUMAIDs(values map[int]struct{}) []int {
	keys := make([]int, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

func solveHardReclaimWithReplacement(
	demands []partitionDemand,
	available machine.CPUSet,
	topology *machine.CPUTopology,
	options hardReclaimReplacementOptions,
) (map[string]machine.CPUSet, *hardReclaimReplacementProof, error) {
	if topology == nil {
		return nil, nil, fmt.Errorf("hard reclaim replacement topology is nil")
	}
	if options.maxCandidateStates <= 0 || options.maxTerminalSolves <= 0 {
		return nil, nil, hardReclaimBudgetError(
			fmt.Errorf("hard reclaim replacement search budget must be positive"))
	}
	if _, _, _, err := validatePartitionDemands(demands, topology); err != nil {
		return nil, nil, err
	}

	targetByNUMA := make(map[int]int)
	activeNUMAs := make(map[int]struct{})
	for _, demand := range demands {
		if demand.class != advisorBlockClassMandatoryReclaim {
			continue
		}
		numaIDs := topology.CPUDetails.KeepOnly(demand.eligible).NUMANodes().ToSliceInt()
		if len(numaIDs) != 1 {
			return nil, nil, fmt.Errorf(
				"hard reclaim replacement demand %q must belong to exactly one NUMA", demand.key)
		}
		activeNUMAs[numaIDs[0]] = struct{}{}
		targetByNUMA[numaIDs[0]] += demand.quantity
	}
	if len(targetByNUMA) == 0 {
		budget := hardReclaimPartitionSolverBudget(options)
		assignments, err := solveDisjointPartitionsWithSharedBudget(
			intersectDemandEligibility(demands, available), topology, &budget)
		if err != nil {
			if isPartitionSolverBudgetError(err) {
				return nil, nil, hardReclaimBudgetError(err)
			}
			return nil, nil, err
		}
		proof, err := validateHardReclaimReplacement(demands, assignments, topology, targetByNUMA)
		return assignments, proof, err
	}

	states := []hardReclaimReplacementGlobalState{{
		reclaimAfter: machine.NewCPUSet(),
	}}
	for _, numaID := range sortedHardReclaimNUMAIDs(activeNUMAs) {
		localResults, err := enumerateHardReclaimReplacementInNUMA(
			demands, available, topology, numaID, targetByNUMA[numaID], options)
		if err != nil {
			return nil, nil, err
		}
		nextByKey := make(map[string]hardReclaimReplacementGlobalState)
		combinations := 0
		for _, state := range states {
			for _, local := range localResults {
				combinations++
				if combinations > options.maxCandidateStates {
					return nil, nil, hardReclaimBudgetError(fmt.Errorf(
						"hard reclaim replacement global merge exceeded state budget %d",
						options.maxCandidateStates))
				}
				next := mergeHardReclaimReplacementState(state, local)
				key := hardReclaimGlobalStateKey(next)
				current, found := nextByKey[key]
				if !found || hardReclaimGlobalStateLess(next, current) {
					nextByKey[key] = next
				}
			}
		}
		if len(nextByKey) > options.maxCandidateStates {
			return nil, nil, hardReclaimBudgetError(fmt.Errorf(
				"hard reclaim replacement global frontier exceeded state budget %d",
				options.maxCandidateStates))
		}
		states = states[:0]
		for _, state := range nextByKey {
			states = append(states, state)
		}
	}

	var bestAssignments map[string]machine.CPUSet
	var bestProof *hardReclaimReplacementProof
	partitionBudget := hardReclaimPartitionSolverBudget(options)
	for _, state := range states {
		residualDemands := hardReclaimResidualDemands(demands, available, state.reclaimAfter)
		assignments, solveErr := solveDisjointPartitionsWithSharedBudget(
			residualDemands, topology, &partitionBudget)
		if solveErr != nil {
			if isPartitionSolverBudgetError(solveErr) {
				return nil, nil, hardReclaimBudgetError(solveErr)
			}
			continue
		}
		proof, validationErr := validateHardReclaimReplacement(
			demands, assignments, topology, targetByNUMA)
		if validationErr != nil {
			continue
		}
		if bestAssignments == nil || hardReclaimReplacementResultLess(
			assignments, proof, bestAssignments, bestProof) {
			bestAssignments = assignments
			bestProof = proof
		}
	}
	if bestAssignments == nil {
		return nil, nil, &hardReclaimSelectionError{
			reason: hardReclaimFailureDonorFloor,
			cause:  fmt.Errorf("no global hard reclaim replacement is feasible"),
		}
	}
	return bestAssignments, bestProof, nil
}

func hardReclaimBudgetError(cause error) error {
	return &hardReclaimSelectionError{
		reason: hardReclaimFailureSearchBudget,
		cause:  cause,
	}
}

func isPartitionSolverBudgetError(err error) bool {
	return errors.Is(err, errPartitionAssignmentEdgeBudget) ||
		errors.Is(err, errPartitionFlowOperationBudget)
}

func enumerateHardReclaimReplacementInNUMA(
	demands []partitionDemand,
	available machine.CPUSet,
	topology *machine.CPUTopology,
	numaID, target int,
	options hardReclaimReplacementOptions,
) ([]hardReclaimReplacementResult, error) {
	numaCPUs := topology.CPUDetails.CPUsInNUMANodes(numaID)
	localAvailable := available.Intersection(numaCPUs)
	reclaimBefore := machine.NewCPUSet()
	reclaimEligible := machine.NewCPUSet()
	dedicatedBefore := machine.NewCPUSet()
	allCurrentlyOwned := machine.NewCPUSet()
	for _, demand := range demands {
		preferred := demand.preferred.Intersection(numaCPUs)
		allCurrentlyOwned = allCurrentlyOwned.Union(preferred)
		switch demand.class {
		case advisorBlockClassMandatoryReclaim:
			reclaimBefore = reclaimBefore.Union(preferred)
			reclaimEligible = reclaimEligible.Union(demand.eligible.Intersection(localAvailable))
		case advisorBlockClassDedicated:
			dedicatedBefore = dedicatedBefore.Union(preferred)
		}
	}

	terminals := []machine.CPUSet{machine.NewCPUSet()}
	if target > 0 {
		free := localAvailable.Difference(allCurrentlyOwned)
		source := reclaimBefore.Union(free).Union(dedicatedBefore).
			Intersection(reclaimEligible)
		candidates := coreAlignedCandidates(topology, source, reclaimBefore)
		var candidateBudgetExceeded, terminalTruncated bool
		terminals, candidateBudgetExceeded, terminalTruncated =
			enumerateHardReclaimReplacementNUMATerminals(
				candidates, target, options.maxCandidateStates, options.maxTerminalSolves)
		if candidateBudgetExceeded {
			return nil, hardReclaimBudgetError(fmt.Errorf(
				"hard reclaim replacement NUMA %d search exceeded candidate state budget %d",
				numaID, options.maxCandidateStates))
		}
		if terminalTruncated {
			return nil, hardReclaimBudgetError(fmt.Errorf(
				"hard reclaim replacement NUMA %d exceeded terminal solve budget %d",
				numaID, options.maxTerminalSolves))
		}
		if len(terminals) == 0 {
			return nil, &hardReclaimSelectionError{
				reason:  hardReclaimFailureInsufficientWholeCore,
				numaID:  numaID,
				deficit: target,
				cause: fmt.Errorf(
					"no complete-core reclaim replacement satisfies NUMA %d target %d",
					numaID, target),
			}
		}
	}

	resultsBySignature := make(map[string]hardReclaimReplacementResult)
	for _, terminal := range terminals {
		result := hardReclaimReplacementResult{
			proof: &hardReclaimReplacementProof{
				reclaimBefore: reclaimBefore,
				reclaimAfter:  terminal,
			},
		}
		signature := hardReclaimTerminalSignature(terminal, demands, topology)
		current, found := resultsBySignature[signature]
		if !found || hardReclaimLocalFrontierLess(result, current) {
			resultsBySignature[signature] = result
		}
	}
	results := make([]hardReclaimReplacementResult, 0, len(resultsBySignature))
	for _, result := range resultsBySignature {
		results = append(results, result)
	}
	if len(results) == 0 {
		return nil, &hardReclaimSelectionError{
			reason: hardReclaimFailureDonorFloor,
			numaID: numaID,
			cause:  fmt.Errorf("no reclaim frontier satisfies NUMA %d target %d", numaID, target),
		}
	}
	sort.Slice(results, func(i, j int) bool {
		left, right := results[i].proof, results[j].proof
		leftRetained := left.reclaimAfter.Intersection(left.reclaimBefore).Size()
		rightRetained := right.reclaimAfter.Intersection(right.reclaimBefore).Size()
		if leftRetained != rightRetained {
			return leftRetained > rightRetained
		}
		return compareCPUSetLexicographically(left.reclaimAfter, right.reclaimAfter) < 0
	})
	return results, nil
}

func hardReclaimTerminalSignature(
	terminal machine.CPUSet,
	demands []partitionDemand,
	topology *machine.CPUTopology,
) string {
	sortedDemands := append([]partitionDemand(nil), demands...)
	sort.Slice(sortedDemands, func(i, j int) bool {
		return sortedDemands[i].key < sortedDemands[j].key
	})
	classCounts := make(map[string]int)
	for _, cpu := range terminal.ToSliceInt() {
		var signature strings.Builder
		for _, demand := range sortedDemands {
			if demand.eligible.Contains(cpu) {
				signature.WriteByte('e')
			} else {
				signature.WriteByte('-')
			}
			if demand.preferred.Contains(cpu) {
				signature.WriteByte('p')
			} else {
				signature.WriteByte('-')
			}
			signature.WriteByte(byte('0' + partitionTopologyDistance(cpu, demand.preferred, topology)))
		}
		classCounts[signature.String()]++
	}
	classes := make([]string, 0, len(classCounts))
	for class := range classCounts {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	var result strings.Builder
	for _, class := range classes {
		fmt.Fprintf(&result, "%s=%d;", class, classCounts[class])
	}
	return result.String()
}

func hardReclaimLocalFrontierLess(
	left, right hardReclaimReplacementResult,
) bool {
	leftRetained := left.proof.reclaimAfter.Intersection(left.proof.reclaimBefore).Size()
	rightRetained := right.proof.reclaimAfter.Intersection(right.proof.reclaimBefore).Size()
	if leftRetained != rightRetained {
		return leftRetained > rightRetained
	}
	return compareCPUSetLexicographically(left.proof.reclaimAfter, right.proof.reclaimAfter) < 0
}

func hardReclaimPartitionSolverBudget(
	options hardReclaimReplacementOptions,
) partitionSolverBudget {
	budget := defaultPartitionSolverBudget()
	if options.maxPartitionAssignmentEdges > 0 {
		budget.maxAssignmentEdges = options.maxPartitionAssignmentEdges
	}
	if options.maxPartitionFlowOperations > 0 {
		budget.maxFlowOperations = options.maxPartitionFlowOperations
	}
	return budget
}

func mergeHardReclaimReplacementState(
	state hardReclaimReplacementGlobalState,
	local hardReclaimReplacementResult,
) hardReclaimReplacementGlobalState {
	next := hardReclaimReplacementGlobalState{
		reclaimRetained: state.reclaimRetained +
			local.proof.reclaimAfter.Intersection(local.proof.reclaimBefore).Size(),
		reclaimAfter: state.reclaimAfter.Union(local.proof.reclaimAfter),
	}
	return next
}

func hardReclaimGlobalStateKey(state hardReclaimReplacementGlobalState) string {
	return fmt.Sprintf("%d/%s",
		state.reclaimRetained,
		state.reclaimAfter.String())
}

func hardReclaimGlobalStateLess(
	left, right hardReclaimReplacementGlobalState,
) bool {
	if comparison := compareCPUSetLexicographically(
		left.reclaimAfter, right.reclaimAfter); comparison != 0 {
		return comparison < 0
	}
	return false
}

func hardReclaimResidualDemands(
	demands []partitionDemand,
	available, reclaim machine.CPUSet,
) []partitionDemand {
	result := append([]partitionDemand(nil), demands...)
	for i := range result {
		result[i].eligible = result[i].eligible.Intersection(available)
		if result[i].class == advisorBlockClassMandatoryReclaim {
			result[i].eligible = result[i].eligible.Intersection(reclaim)
		} else {
			result[i].eligible = result[i].eligible.Difference(reclaim)
		}
	}
	return result
}

func intersectDemandEligibility(
	demands []partitionDemand,
	available machine.CPUSet,
) []partitionDemand {
	result := append([]partitionDemand(nil), demands...)
	for i := range result {
		result[i].eligible = result[i].eligible.Intersection(available)
	}
	return result
}

func enumerateHardReclaimReplacementNUMATerminals(
	candidates []coreAlignedCandidate,
	target int,
	maxCandidateStates, maxTerminalSolves int,
) ([]machine.CPUSet, bool, bool) {
	visited := 0
	budgetExceeded := false
	frontier := make([]hardReclaimReplacementTerminal, 0)
	var visit func(int, int, hardReclaimReplacementTerminal)
	visit = func(index, selected int, terminal hardReclaimReplacementTerminal) {
		if budgetExceeded {
			return
		}
		visited++
		if visited > maxCandidateStates {
			budgetExceeded = true
			return
		}
		if selected == target {
			frontier = append(frontier, terminal)
			return
		}
		if index == len(candidates) {
			return
		}

		candidate := candidates[index]
		if selected+candidate.cpus.Size() <= target {
			next := hardReclaimReplacementTerminal{
				cpus:               terminal.cpus.Union(candidate.cpus),
				partialBeforeCores: terminal.partialBeforeCores,
				retained:           terminal.retained + candidate.preferredHit,
			}
			if candidate.preferredHit > 0 && candidate.preferredHit < candidate.cpus.Size() {
				next.partialBeforeCores++
			}
			visit(index+1, selected+candidate.cpus.Size(), next)
		}
		visit(index+1, selected, terminal)
	}
	visit(0, 0, hardReclaimReplacementTerminal{cpus: machine.NewCPUSet()})
	if budgetExceeded {
		return nil, true, false
	}
	if len(frontier) == 0 {
		return nil, false, false
	}
	sort.Slice(frontier, func(i, j int) bool {
		return hardReclaimReplacementTerminalLess(frontier[i], frontier[j])
	})
	truncated := len(frontier) > maxTerminalSolves
	if truncated {
		frontier = frontier[:maxTerminalSolves]
	}
	terminals := make([]machine.CPUSet, len(frontier))
	for i := range frontier {
		terminals[i] = frontier[i].cpus
	}
	return terminals, false, truncated
}

func hardReclaimReplacementTerminalLess(
	left, right hardReclaimReplacementTerminal,
) bool {
	if left.retained != right.retained {
		return left.retained > right.retained
	}
	if left.partialBeforeCores != right.partialBeforeCores {
		return left.partialBeforeCores < right.partialBeforeCores
	}
	return compareCPUSetLexicographically(left.cpus, right.cpus) < 0
}

func hardReclaimReplacementResultLess(
	leftAssignments map[string]machine.CPUSet,
	leftProof *hardReclaimReplacementProof,
	rightAssignments map[string]machine.CPUSet,
	rightProof *hardReclaimReplacementProof,
) bool {
	leftReclaimRetained := leftProof.reclaimAfter.Intersection(leftProof.reclaimBefore).Size()
	rightReclaimRetained := rightProof.reclaimAfter.Intersection(rightProof.reclaimBefore).Size()
	if leftReclaimRetained != rightReclaimRetained {
		return leftReclaimRetained > rightReclaimRetained
	}
	if leftProof.partialBeforeCores != rightProof.partialBeforeCores {
		return leftProof.partialBeforeCores < rightProof.partialBeforeCores
	}

	leftDedicatedRetained, rightDedicatedRetained := 0, 0
	leftTouched, rightTouched := 0, 0
	groupKeys := make([]string, 0, len(leftProof.dedicatedBeforeByGroup))
	for groupKey := range leftProof.dedicatedBeforeByGroup {
		groupKeys = append(groupKeys, groupKey)
	}
	sort.Strings(groupKeys)
	for _, groupKey := range groupKeys {
		leftBefore := leftProof.dedicatedBeforeByGroup[groupKey]
		leftAfter := leftProof.dedicatedAfterByGroup[groupKey]
		rightBefore := rightProof.dedicatedBeforeByGroup[groupKey]
		rightAfter := rightProof.dedicatedAfterByGroup[groupKey]
		leftDedicatedRetained += leftBefore.Intersection(leftAfter).Size()
		rightDedicatedRetained += rightBefore.Intersection(rightAfter).Size()
		if !leftBefore.Equals(leftAfter) {
			leftTouched++
		}
		if !rightBefore.Equals(rightAfter) {
			rightTouched++
		}
	}
	if leftDedicatedRetained != rightDedicatedRetained {
		return leftDedicatedRetained > rightDedicatedRetained
	}
	if leftTouched != rightTouched {
		return leftTouched < rightTouched
	}
	if comparison := compareCPUSetLexicographically(
		leftProof.reclaimAfter, rightProof.reclaimAfter); comparison != 0 {
		return comparison < 0
	}

	keys := make([]string, 0, len(leftAssignments))
	for key := range leftAssignments {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if comparison := compareCPUSetLexicographically(
			leftAssignments[key], rightAssignments[key]); comparison != 0 {
			return comparison < 0
		}
	}
	return false
}

func compareCPUSetLexicographically(left, right machine.CPUSet) int {
	leftCPUs, rightCPUs := left.ToSliceInt(), right.ToSliceInt()
	limit := general.Min(len(leftCPUs), len(rightCPUs))
	for i := 0; i < limit; i++ {
		if leftCPUs[i] < rightCPUs[i] {
			return -1
		}
		if leftCPUs[i] > rightCPUs[i] {
			return 1
		}
	}
	switch {
	case len(leftCPUs) < len(rightCPUs):
		return -1
	case len(leftCPUs) > len(rightCPUs):
		return 1
	default:
		return 0
	}
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
