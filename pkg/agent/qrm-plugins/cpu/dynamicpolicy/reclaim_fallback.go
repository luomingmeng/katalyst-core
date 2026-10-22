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

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// reclaimFallbackMode is the per-attempt snapshot of the dynamic config flags that
// decide how the reclaim pool fallback may be bounded. It is computed once from the
// current revision/dynamicConf and threaded through the attempt; it must not be
// re-read live during revise.
type reclaimFallbackMode struct {
	enableReclaim           bool
	allowSharedOverlap      bool
	disableDedicatedOverlap bool
	// requireCoreAligned is an internal selection discipline (NOT a config flag and NOT
	// part of strict()): it forces complete-physical-core selection on the per-NUMA
	// backfill without turning on the strict capacity hard-floor. strict() stays the real
	// 3-flag disjoint mode; this only changes whether choose() may pick partial cores.
	requireCoreAligned bool
}

// strict identifies active disjoint sizing. Disabled mode freezes a committed
// partition and therefore does not re-run strict capacity sizing.
func (m reclaimFallbackMode) strict() bool {
	return m.enableReclaim && !m.allowSharedOverlap && m.disableDedicatedOverlap
}

// error classes for the fallback selection.
var (
	errReclaimFallbackCapacityInsufficient = errors.New("reclaim fallback: eligible capacity insufficient")
	errReclaimFallbackInvariant            = errors.New("reclaim fallback: initialization invariant violated")
	errReclaimFallbackEmptyRequired        = errors.New("reclaim fallback: non-empty reclaim pool required but no candidate")
)

// withRequiredReclaimFallback moves an omitted disjoint reclaim target into the
// planning frame. It never mutates the received response or overwrites positive
// advice; the synthetic block is subject to the normal ownership/scope checks.
func (p *DynamicPolicy) withRequiredReclaimFallback(resp *advisorapi.ListAndWatchResponse, dyn *dynamicconfig.Configuration) (*advisorapi.ListAndWatchResponse, error) {
	if resp == nil || dyn == nil || !dyn.EnableReclaim || !resp.DisableDedicatedCoresOverlapReclaimedCores || p.reclaim.reservedReclaimedCPUsSize <= 0 {
		return resp, nil
	}
	if entries := resp.Entries[commonstate.PoolNameReclaim]; entries != nil {
		for _, info := range entries.Entries {
			if info == nil {
				continue
			}
			for _, result := range info.CalculationResultsByNumas {
				if result == nil {
					continue
				}
				for _, block := range result.Blocks {
					if block != nil && block.Result > 0 {
						return resp, nil
					}
				}
			}
		}
	}
	blocks, err := resp.GetBlocks()
	if err != nil {
		return nil, err
	}
	id := "reclaim-fallback"
	for suffix := 0; ; suffix++ {
		used := false
		for _, numaBlocks := range blocks {
			for _, block := range numaBlocks {
				if block != nil && block.BlockId == id {
					used = true
				}
			}
		}
		if !used {
			break
		}
		id = fmt.Sprintf("reclaim-fallback-%d", suffix)
	}
	out := *resp
	out.Entries = make(map[string]*advisorapi.CalculationEntries, len(resp.Entries)+1)
	for name, entries := range resp.Entries {
		out.Entries[name] = entries
	}
	out.Entries[commonstate.PoolNameReclaim] = &advisorapi.CalculationEntries{Entries: map[string]*advisorapi.CalculationInfo{
		commonstate.FakedContainerName: {OwnerPoolName: commonstate.PoolNameReclaim, CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
			commonstate.FakedNUMAID: {Blocks: []*advisorapi.Block{{BlockId: id, Result: uint64(p.reclaim.reservedReclaimedCPUsSize)}}},
		}},
	}}
	return &out, nil
}

// carveDedicatedReclaim forms the final candidate before precommit freezes its
// shape. RequestQuantity is scheduling intent, not a floor on the physical set.
// Isolation and package ownership are not reclassified as ordinary dedicated.
func (p *DynamicPolicy) carveDedicatedReclaim(entries state.PodEntries, reclaim machine.CPUSet) error {
	if reclaim.IsEmpty() {
		return nil
	}
	type update struct {
		allocation  *state.AllocationInfo
		cpus        machine.CPUSet
		assignments map[int]machine.CPUSet
	}
	var updates []update
	for name, containers := range entries {
		for _, allocation := range containers {
			if allocation == nil || allocation.CheckSideCar() {
				continue
			}
			owner := commonstate.GetPoolType(commonstate.OwnerPoolNameTranslator.Translate(allocation.OwnerPoolName))
			ordinaryDedicated := owner == commonstate.PoolNameDedicated &&
				(containers.IsPoolEntry() || allocation.CheckDedicated())
			if !ordinaryDedicated || allocation.AllocationResult.Intersection(reclaim).IsEmpty() {
				continue
			}
			remaining := allocation.AllocationResult.Difference(reclaim)
			if remaining.IsEmpty() && !containers.IsPoolEntry() {
				return fmt.Errorf("dedicated reclaim transfer leaves empty allocation for %s/%s", name, allocation.ContainerName)
			}
			assignments, err := machine.GetNumaAwareAssignments(p.machine.machineInfo.CPUTopology, remaining)
			if err != nil {
				return err
			}
			updates = append(updates, update{allocation, remaining, assignments})
		}
	}
	// Validate every affected owner before changing any candidate entry.
	for _, u := range updates {
		u.allocation.AllocationResult = u.cpus
		u.allocation.OriginalAllocationResult = u.cpus.Clone()
		u.allocation.TopologyAwareAssignments = u.assignments
		u.allocation.OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(u.assignments)
	}
	p.syncCandidateSidecarsFromMain(entries)
	return nil
}

// preserveCommittedReclaimPool freezes the last successful reclaim identity in
// a new candidate. Disabled reclaim may stop service and expansion, but it must
// not erase or relocate the cpuset that still backs existing offline workloads.
func preserveCommittedReclaimPool(candidate, committed state.PodEntries) error {
	entry := committed[commonstate.PoolNameReclaim][commonstate.FakedContainerName]
	if entry == nil || entry.AllocationResult.IsEmpty() {
		return errReclaimFallbackEmptyRequired
	}
	if candidate[commonstate.PoolNameReclaim] == nil {
		candidate[commonstate.PoolNameReclaim] = make(state.ContainerEntries)
	}
	candidate[commonstate.PoolNameReclaim][commonstate.FakedContainerName] = entry.Clone()
	return nil
}

// validateFinalReclaimPool is the terminal state contract shared by every
// successful CPU partition writer. Runtime reclaim enablement controls whether
// the pool may be expanded; it never makes an empty committed pool legal.
func validateFinalReclaimPool(entries state.PodEntries) error {
	if entries.CheckPoolEmpty(commonstate.PoolNameReclaim) {
		return errReclaimFallbackEmptyRequired
	}
	return nil
}

// selectBoundedReclaimFallback picks the reclaim pool CPUs when the advisor/build yields an
// empty reclaim entry. It is pure: no state read, no state write.
//
// Invariants enforced by every branch:
//   - The result is always a subset of eligible (never superset, never whole machine).
//   - The result size never exceeds want.
//   - want==0 recovers exactly one complete physical core; it never makes an
//     empty successful state legal.
//   - committedPrior is intersected with eligible before use, so a stale or oversized
//     prior is never materialized unfiltered.
func selectBoundedReclaimFallback(
	topology *machine.CPUTopology,
	eligible machine.CPUSet,
	committedPrior machine.CPUSet,
	defaultPreference machine.CPUSet,
	want int,
	mode reclaimFallbackMode,
) (machine.CPUSet, error) {
	if topology == nil {
		return machine.NewCPUSet(), errReclaimFallbackInvariant
	}
	if want < 0 {
		return machine.NewCPUSet(), errReclaimFallbackInvariant
	}
	if want == 0 {
		// A zero configured target disables expansion, not partition existence.
		// Recover the smallest complete physical core; if none is legal, fail closed.
		want = topology.CPUsPerCore()
	}

	// comm is the committed prior that still fits the filtered eligible set.
	comm := committedPrior.Intersection(eligible)

	// choose draws up to quantity CPUs from source, honoring core-alignment for
	// strict/non-overlap and allowing partial-core legacy overlap otherwise.
	choose := func(source machine.CPUSet, quantity int) machine.CPUSet {
		if mode.allowSharedOverlap && !mode.strict() && !mode.requireCoreAligned {
			return takeLegacyOverlapCPUSet(source, defaultPreference, quantity)
		}
		return takeCoreAlignedCPUSet(topology, source, defaultPreference, quantity)
	}

	// Step 1: extract legal complete cores (strict) or partial overlap cores (legacy)
	// from the committed prior. This preserves prior identity intent rather than
	// discarding a partial prior and re-seeding from scratch.
	base := choose(comm, want)
	result := base.Clone()

	// Step 2: if short, fill from eligible minus already-selected, preferring default
	// as remaining preference. Never unfiltered-clone; result stays subset(eligible).
	remaining := want - result.Size()
	if remaining > 0 {
		fill := choose(eligible.Difference(result), remaining)
		result = result.Union(fill)
	}

	if result.IsEmpty() {
		return result, errReclaimFallbackEmptyRequired
	}
	if result.Size() < want && mode.strict() {
		return result, errReclaimFallbackCapacityInsufficient
	}
	return result, nil
}

// takeLegacyOverlapCPUSet picks up to quantity CPUs from eligible, preferring the
// defaultPreference set first, without requiring complete physical cores.
func takeLegacyOverlapCPUSet(eligible, prefer machine.CPUSet, quantity int) machine.CPUSet {
	if quantity <= 0 || eligible.IsEmpty() {
		return machine.NewCPUSet()
	}
	selected := prefer.Clone().Intersection(eligible)
	if selected.Size() >= quantity {
		return takeFirstNCPUSet(selected, quantity)
	}
	for _, cpu := range eligible.ToSliceInt() {
		if selected.Size() >= quantity {
			break
		}
		selected.Add(cpu)
	}
	return selected
}

// poolIsReclaimProhibited reports whether a pool's CPUs must be subtracted from the
// reclaim-fallback eligible set under the given attempt mode. It is a pure predicate
// over an already-built candidate snapshot:
//   - Dedicated overlap is disabled independently of share-overlap/strict: whenever
//     disableDedicatedOverlap is set, the dedicated pool (and non-isolated dedicated
//     ownership) is off-limits even when allowSharedOverlap is true (strict() is false).
//   - Share-type pools (default share, custom shared_cores, share-NUMA-binding
//     "*-NUMAx") are off-limits only when reclaim is enabled AND share overlap is NOT
//     allowed. The legacy disabled-reclaim path (enableReclaim=false) keeps its bounded
//     share-overlap exception, so it never subtracts share here.
//
// Forbidden/system/reserve/reclaim are excluded by the hard-eligible base, and
// isolated dedicated container CPUs are subtracted separately.
func poolIsReclaimProhibited(poolName string, mode reclaimFallbackMode) bool {
	switch commonstate.GetPoolType(poolName) {
	case commonstate.PoolNameDedicated:
		return mode.disableDedicatedOverlap
	case commonstate.PoolNameShare:
		return mode.enableReclaim && !mode.allowSharedOverlap
	default:
		return false
	}
}

// modeProhibitedPoolCPUs returns the union of the pool CPUs that are off-limits to the
// reclaim fallback under the given mode, read purely from an already-built candidate
// pool→cpuset map. It reads no live state.
func modeProhibitedPoolCPUs(poolsCPUSet map[string]machine.CPUSet, mode reclaimFallbackMode) machine.CPUSet {
	prohibited := machine.NewCPUSet()
	for poolName, cset := range poolsCPUSet {
		if poolIsReclaimProhibited(poolName, mode) {
			prohibited = prohibited.Union(cset)
		}
	}
	return prohibited
}

// takeFirstNCPUSet returns a CPUSet with at most n CPUs from s, in sorted order.
func takeFirstNCPUSet(s machine.CPUSet, n int) machine.CPUSet {
	if n <= 0 {
		return machine.NewCPUSet()
	}
	selected := machine.NewCPUSet()
	for _, cpu := range s.ToSliceInt() {
		if selected.Size() >= n {
			break
		}
		selected.Add(cpu)
	}
	return selected
}
