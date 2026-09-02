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

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type coreAlignedCandidate struct {
	coreID       int
	cpus         machine.CPUSet
	preferredHit int
}

// takeCoreAlignedCPUSet selects cpus from candidates in complete physical cores
// only. It picks up to quantity cpus, but a core is chosen only when every one
// of its cpusPerCore siblings is present in candidates, so the returned set is
// always core-aligned (invariant B). quantity is cropped DOWN to a whole-core
// multiple: a request that is not a whole-core amount never pulls a lone SMT
// sibling — Tasks 1-3 make the reclaim demands core-aligned, so this crop is a
// defensive net rather than an expected path.
//
// Cores are ordered by how many of their siblings live in the prefer set
// (descending) so stability (the currently pinned reclaim cpuset) is kept before
// fresh cpus are pulled in, then by ascending CoreID for a deterministic
// tie-break. On non-SMT topologies (CPUsPerCore()==1) every cpu is its own core
// and the result reduces to a prefer-first, lowest-id take with zero drift.
func takeCoreAlignedCPUSet(
	topology *machine.CPUTopology,
	candidates machine.CPUSet,
	prefer machine.CPUSet,
	quantity int,
) machine.CPUSet {
	if quantity <= 0 || candidates.IsEmpty() || topology == nil {
		return machine.NewCPUSet()
	}
	cpusPerCore := topology.CPUsPerCore()
	if cpusPerCore <= 0 {
		return machine.NewCPUSet()
	}
	coresWanted := quantity / cpusPerCore
	if coresWanted <= 0 {
		return machine.NewCPUSet()
	}

	completeCores := coreAlignedCandidates(topology, candidates, prefer)
	selected := machine.NewCPUSet()
	for _, core := range completeCores {
		if coresWanted == 0 {
			break
		}
		selected = selected.Union(core.cpus)
		coresWanted--
	}
	return selected
}

// coreAlignedCandidates returns complete physical cores in deterministic
// tiered-preference order: cores with more siblings in prefer come first, then
// lower core IDs. Callers can use the individual candidates when a full core
// must be expressed as a solver constraint instead of being selected eagerly.
func coreAlignedCandidates(
	topology *machine.CPUTopology,
	candidates machine.CPUSet,
	prefer machine.CPUSet,
) []coreAlignedCandidate {
	if topology == nil {
		return nil
	}
	cpusPerCore := topology.CPUsPerCore()
	if cpusPerCore <= 0 {
		return nil
	}

	cpusByCore := make(map[int]machine.CPUSet)
	for _, cpu := range candidates.ToSliceInt() {
		info, ok := topology.CPUDetails[cpu]
		if !ok {
			continue
		}
		set := cpusByCore[info.CoreID]
		if !set.Initialed {
			set = machine.NewCPUSet()
		}
		set.Add(cpu)
		cpusByCore[info.CoreID] = set
	}

	completeCores := make([]coreAlignedCandidate, 0, len(cpusByCore))
	for coreID, cpus := range cpusByCore {
		if cpus.Size() != cpusPerCore {
			continue
		}
		completeCores = append(completeCores, coreAlignedCandidate{
			coreID:       coreID,
			cpus:         cpus,
			preferredHit: cpus.Intersection(prefer).Size(),
		})
	}

	sort.Slice(completeCores, func(i, j int) bool {
		if completeCores[i].preferredHit != completeCores[j].preferredHit {
			return completeCores[i].preferredHit > completeCores[j].preferredHit
		}
		return completeCores[i].coreID < completeCores[j].coreID
	})
	return completeCores
}

func completeCoresForCPUSet(topology *machine.CPUTopology, cpus machine.CPUSet) (machine.CPUSet, error) {
	if topology == nil {
		return machine.NewCPUSet(), fmt.Errorf("cannot complete cores with nil cpu topology")
	}
	coreIDs := make([]int, 0, cpus.Size())
	seen := make(map[int]struct{}, cpus.Size())
	for _, cpu := range cpus.ToSliceInt() {
		info, ok := topology.CPUDetails[cpu]
		if !ok {
			return machine.NewCPUSet(), fmt.Errorf("cpu %d has no topology metadata", cpu)
		}
		if _, ok := seen[info.CoreID]; ok {
			continue
		}
		seen[info.CoreID] = struct{}{}
		coreIDs = append(coreIDs, info.CoreID)
	}
	return topology.CPUDetails.CPUsInCores(coreIDs...), nil
}

func completeEligibleCoresForPreferredCPUSet(
	topology *machine.CPUTopology,
	eligible machine.CPUSet,
	prefer machine.CPUSet,
) (machine.CPUSet, error) {
	if topology == nil {
		return machine.NewCPUSet(), fmt.Errorf("cannot select eligible preferred cores with nil cpu topology")
	}
	if topology.CPUsPerCore() <= 0 {
		return machine.NewCPUSet(), fmt.Errorf("cannot select eligible preferred cores with non-positive cpus per core %d", topology.CPUsPerCore())
	}

	selected := machine.NewCPUSet()
	for _, core := range coreAlignedCandidates(topology, eligible, prefer) {
		if core.preferredHit == 0 {
			break
		}
		selected = selected.Union(core.cpus)
	}
	return selected, nil
}

// assertCoreAligned is a fail-loud safety net: it returns a lowercase error when
// reclaim holds a partial physical core (a CoreID whose sibling count differs
// from CPUsPerCore()). It never repairs silently; a violation signals an upstream
// invariant break (quantity/reserve/selection) that must be surfaced, not masked.
func assertCoreAligned(reclaim machine.CPUSet, topology *machine.CPUTopology) error {
	if topology == nil {
		return fmt.Errorf("cannot assert core alignment with nil cpu topology")
	}
	cpusPerCore := topology.CPUsPerCore()
	if cpusPerCore <= 0 {
		return fmt.Errorf("cannot assert core alignment with non-positive cpus per core %d", cpusPerCore)
	}
	if reclaim.IsEmpty() {
		return nil
	}

	countByCore := make(map[int]int)
	orphanCPUs := machine.NewCPUSet()
	for _, cpu := range reclaim.ToSliceInt() {
		info, ok := topology.CPUDetails[cpu]
		if !ok {
			orphanCPUs.Add(cpu)
			continue
		}
		countByCore[info.CoreID]++
	}
	if !orphanCPUs.IsEmpty() {
		return fmt.Errorf("reclaim set %s contains cpus without topology metadata: %s",
			reclaim.String(), orphanCPUs.String())
	}

	coreIDs := make([]int, 0, len(countByCore))
	for coreID := range countByCore {
		coreIDs = append(coreIDs, coreID)
	}
	sort.Ints(coreIDs)
	for _, coreID := range coreIDs {
		if countByCore[coreID] != cpusPerCore {
			return fmt.Errorf("reclaim set %s is not core-aligned: core %d has %d of %d siblings",
				reclaim.String(), coreID, countByCore[coreID], cpusPerCore)
		}
	}
	return nil
}
