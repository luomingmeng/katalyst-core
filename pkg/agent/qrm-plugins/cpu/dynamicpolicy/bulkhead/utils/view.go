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

package utils

import (
	"fmt"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	cpustate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

type CPUSetPartitionViewOptions struct {
	NonReclaimPoolMinSize        int64
	HardPartitionEnabled         bool
	ReserveCPUReversely          bool
	TransientProtectedNonReclaim machine.CPUSet
}

const minimumHardPartitionReclaimCPUsPerNUMA = 2

func NewCPUSetPartitionViewOptions(
	coreConf *config.Configuration,
	dynamicConf *dynamicconfig.Configuration,
) CPUSetPartitionViewOptions {
	nonReclaimPoolMinSize := configuredNonReclaimPoolMinSize(dynamicConf)
	if nonReclaimPoolMinSize <= 0 && coreConf != nil && coreConf.DynamicAgentConfiguration != nil {
		nonReclaimPoolMinSize = configuredNonReclaimPoolMinSize(coreConf.DynamicAgentConfiguration.GetDynamicConfiguration())
	}

	opts := CPUSetPartitionViewOptions{
		NonReclaimPoolMinSize: nonReclaimPoolMinSize,
		HardPartitionEnabled:  hardPartitionEnabled(dynamicConf),
	}
	if coreConf != nil {
		opts.ReserveCPUReversely = coreConf.EnableReserveCPUReversely
	}
	return opts
}

func configuredNonReclaimPoolMinSize(conf *dynamicconfig.Configuration) int64 {
	if conf == nil || conf.AdminQoSConfiguration == nil || conf.AdminQoSConfiguration.CPUPluginConfiguration == nil {
		return 0
	}
	return conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize
}

func hardPartitionEnabled(conf *dynamicconfig.Configuration) bool {
	if conf == nil || conf.AdminQoSConfiguration == nil || conf.AdminQoSConfiguration.CPUPluginConfiguration == nil {
		return false
	}
	return conf.AdminQoSConfiguration.CPUPluginConfiguration.EnableRampUpReclaimHardPartition
}

func BuildCPUSetPartitionView(state cpustate.ReadonlyState, topology *machine.CPUTopology, opts CPUSetPartitionViewOptions) *model.DesiredView {
	view := model.NewDesiredView()
	if state == nil || topology == nil {
		return view
	}

	allowOverlap := state.GetAllowSharedCoresOverlapReclaimedCores()
	podEntries := state.GetPodEntries()
	poolTypeOf := func(poolName string) string {
		return commonstate.GetPoolType(commonstate.OwnerPoolNameTranslator.Translate(poolName))
	}
	recordContainerCPUSet := func(podUID, containerName string, cpus machine.CPUSet) {
		if _, ok := view.ContainerCPUSetByPod[podUID]; !ok {
			view.ContainerCPUSetByPod[podUID] = map[string]machine.CPUSet{}
		}
		view.ContainerCPUSetByPod[podUID][containerName] = cpus.Clone()
	}
	sharedRampUp := machine.NewCPUSet()
	sharedRampUpByPool := map[string]machine.CPUSet{}

	for _, containerEntries := range podEntries {
		if containerEntries.IsPoolEntry() {
			continue
		}
		for _, allocation := range containerEntries {
			if allocation != nil {
				recordContainerCPUSet(allocation.PodUid, allocation.ContainerName, allocation.AllocationResult)
				if allocation.RampUp && allocation.CheckSharedNUMABinding() {
					view.SharePool = view.SharePool.Union(allocation.AllocationResult)
					sharedRampUp = sharedRampUp.Union(allocation.AllocationResult)
					poolName, err := allocation.GetSpecifiedNUMABindingPoolName()
					if err != nil {
						poolName = commonstate.PoolNameShare
					}
					packageName := allocation.GetResourcePackageName()
					poolName = resourcepackage.WrapOwnerPoolName(poolName, packageName)
					sharedRampUpByPool[poolName] = sharedRampUpByPool[poolName].Union(allocation.AllocationResult)
				}
			}
		}
	}

	for poolName, containerEntries := range podEntries {
		if !containerEntries.IsPoolEntry() {
			continue
		}
		entry := containerEntries[commonstate.FakedContainerName]
		if entry == nil {
			continue
		}
		switch poolTypeOf(poolName) {
		case commonstate.PoolNameReserve:
			view.Reserve = view.Reserve.Union(entry.AllocationResult)
		case commonstate.PoolNameShare:
			view.SharePool = view.SharePool.Union(entry.AllocationResult)
			view.SharePoolMap[poolName] = entry.AllocationResult.Clone()
		case commonstate.PoolNamePrefixIsolation:
			view.Isolation = view.Isolation.Union(entry.AllocationResult)
		}
	}
	for poolName, cpus := range sharedRampUpByPool {
		view.SharePoolMap[poolName] = view.SharePoolMap[poolName].Union(cpus)
	}

	for _, containerEntries := range podEntries {
		if containerEntries.IsPoolEntry() {
			continue
		}
		for _, allocation := range containerEntries {
			if allocation != nil && allocation.CheckDedicated() {
				view.Dedicated = view.Dedicated.Union(allocation.AllocationResult)
			}
		}
	}

	if reclaimEntries, ok := podEntries[commonstate.PoolNameReclaim]; ok {
		if entry := reclaimEntries[commonstate.FakedContainerName]; entry != nil {
			view.ReclaimRaw = entry.AllocationResult.Clone()
		}
	}
	if allowOverlap && !view.ReclaimRaw.IsEmpty() {
		view.SharePool = view.SharePool.Difference(view.ReclaimRaw)
		for poolName, cpus := range view.SharePoolMap {
			view.SharePoolMap[poolName] = cpus.Difference(view.ReclaimRaw)
		}
	}
	defaultShareBaseline := topology.CPUDetails.CPUs().Difference(view.Reserve)
	if !allowOverlap && opts.HardPartitionEnabled && !view.ReclaimRaw.IsEmpty() && view.SharePool.Equals(defaultShareBaseline) {
		// A reset default-share entry can still cover every non-reserved CPU.
		// In hard-partition mode reclaim raw owns its CPUs, while real shared
		// NUMA-binding ramp-up allocations remain explicit non-reclaim owners.
		view.SharePool = view.SharePool.Difference(view.ReclaimRaw).Union(sharedRampUp)
		for poolName, cpus := range view.SharePoolMap {
			view.SharePoolMap[poolName] = cpus.Difference(view.ReclaimRaw)
		}
		for poolName, cpus := range sharedRampUpByPool {
			view.SharePoolMap[poolName] = view.SharePoolMap[poolName].Union(cpus)
		}
	}
	view.NonReclaimPool = view.SharePool.Union(view.Dedicated).Union(view.Isolation)
	if allowOverlap {
		view.ReclaimEffective = view.ReclaimRaw.Clone()
	} else {
		// ReclaimRaw is the affinity budget produced by SysAdvisor after applying
		// reclaimed-cpu-max-ratio. QRM must never widen it when deriving the
		// cgroup target: every CPU outside the effective reclaim set belongs to
		// the non-reclaim domain instead. The effective reclaim set is therefore
		// derived in three steps, each of which can only shrink it:
		//   1. start from all machine CPUs minus the non-reclaim pool and reserve
		//      (the CPUs structurally available to reclaim);
		//   2. intersect with ReclaimRaw so we never exceed SysAdvisor's budget;
		//   3. recompute NonReclaimPool as the complement so the two domains stay
		//      mutually exclusive and jointly cover every non-reserved CPU.
		view.ReclaimEffective = topology.CPUDetails.CPUs().Difference(view.NonReclaimPool).Difference(view.Reserve)
		view.ReclaimEffective = view.ReclaimEffective.Intersection(view.ReclaimRaw)
		view.NonReclaimPool = topology.CPUDetails.CPUs().Difference(view.ReclaimEffective).Difference(view.Reserve)
		// Step 3 may leave CPUs in NonReclaimPool that no share/dedicated/isolation
		// pool claims (e.g. reclaim was clamped below its structural budget). When
		// the share pool is otherwise empty, seed it with these spare CPUs so they
		// are not left unassigned; this only fills a gap and never overrides an
		// existing share allocation.
		if view.SharePool.IsEmpty() {
			spareShare := view.NonReclaimPool.Difference(view.Dedicated).Difference(view.Isolation)
			if !spareShare.IsEmpty() {
				view.SharePool = spareShare
				view.SharePoolMap[commonstate.PoolNameShare] = spareShare.Clone()
			}
		}
		padNonReclaimPoolToMinSize(view, topology, opts)
	}
	// Under a hard partition the mandatory ramp-up floor is balanced across
	// NUMAs, so any asymmetric excess carried by the reclaim pool is stale
	// advisor raw slack that must not be reclaimed. Eliminate it at the source,
	// before the desired snapshot is frozen, so both the commit-override path
	// and the pre-commit validation observe a strictly balanced reclaim domain.
	if opts.HardPartitionEnabled {
		rebalanceHardPartitionReclaimEffective(view, topology)
	}
	view.DesiredNonReclaimPool = view.NonReclaimPool.Clone()
	view.DesiredReclaimEffective = view.ReclaimEffective.Clone()
	rebuildDesiredReclaimEffectivePerNUMA(view, topology)
	ApplyTransientProtectedNonReclaim(view, topology, opts.TransientProtectedNonReclaim)
	rebuildReclaimEffectivePerNUMA(view, topology)
	return view
}

func BuildValidatedCPUSetPartitionView(state cpustate.ReadonlyState, topology *machine.CPUTopology, opts CPUSetPartitionViewOptions) (*model.DesiredView, error) {
	view := BuildCPUSetPartitionView(state, topology, opts)
	if err := ValidateCPUSetPartitionView(view, topology); err != nil {
		return nil, err
	}
	if opts.HardPartitionEnabled {
		if err := validateHardPartitionReclaimPerNUMA(view.ReclaimEffectivePerNUMA, topology); err != nil {
			return nil, err
		}
	}
	return view, nil
}

func validateHardPartitionReclaimPerNUMA(
	reclaimPerNUMA map[int]machine.CPUSet,
	topology *machine.CPUTopology,
) error {
	if topology == nil {
		return nil
	}

	minimum, maximum := 0, 0
	for i, numaID := range topology.CPUDetails.NUMANodes().ToSliceInt() {
		count := reclaimPerNUMA[numaID].Size()
		if count < minimumHardPartitionReclaimCPUsPerNUMA {
			return fmt.Errorf("bulkhead hard-partition reclaim NUMA %d has %d CPUs, minimum is %d", numaID, count, minimumHardPartitionReclaimCPUsPerNUMA)
		}
		if i == 0 || count < minimum {
			minimum = count
		}
		if i == 0 || count > maximum {
			maximum = count
		}
	}
	if maximum-minimum > 1 {
		return fmt.Errorf("bulkhead hard-partition reclaim is imbalanced across physical NUMAs: max=%d min=%d", maximum, minimum)
	}
	return nil
}

// BuildCPUSetPartitionViewFromTarget creates an owned partition view whose
// reclaim domain is the topology layer's write-verified target. Metadata that
// is independent of partition ownership is copied from desired.
func BuildCPUSetPartitionViewFromTarget(
	desired *model.DesiredView,
	reclaimTarget machine.CPUSet,
	topology *machine.CPUTopology,
) *model.CPUSetPartitionView {
	view := model.NewCPUSetPartitionView()
	if desired != nil {
		view = *desired.CPUSetPartitionView.DeepCopy()
	}

	view.ReclaimEffective = reclaimTarget.Clone()
	view.ReclaimEffectivePerNUMA = map[int]machine.CPUSet{}
	if topology == nil {
		view.NonReclaimPool = machine.NewCPUSet()
		return &view
	}

	available := topology.CPUDetails.CPUs().Difference(view.Reserve)
	view.NonReclaimPool = available.Difference(view.ReclaimEffective)
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceNoSortInt() {
		view.ReclaimEffectivePerNUMA[numaID] = view.ReclaimEffective.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
	}
	return &view
}

func ValidateCPUSetPartitionView(view *model.DesiredView, topology *machine.CPUTopology) error {
	if view == nil || topology == nil {
		return nil
	}
	machineCPUs := topology.CPUDetails.CPUs()
	for name, cpus := range map[string]machine.CPUSet{
		"reserve":                         view.Reserve,
		"share pool":                      view.SharePool,
		"dedicated":                       view.Dedicated,
		"isolation":                       view.Isolation,
		"reclaim raw":                     view.ReclaimRaw,
		"non-reclaim pool":                view.NonReclaimPool,
		"reclaim effective":               view.ReclaimEffective,
		"desired non-reclaim pool":        view.DesiredNonReclaimPool,
		"desired reclaim effective":       view.DesiredReclaimEffective,
		"transient protected non-reclaim": view.TransientProtectedNonReclaim,
	} {
		if outside := cpus.Difference(machineCPUs); !outside.IsEmpty() {
			return fmt.Errorf("bulkhead cpuset partition %s has CPUs outside machine topology: %s", name, outside.String())
		}
	}
	if overlap := view.NonReclaimPool.Intersection(view.ReclaimEffective); !overlap.IsEmpty() {
		return fmt.Errorf("bulkhead cpuset partition has non-reclaim/reclaim overlap: %s", overlap.String())
	}
	if err := validateExactNUMAProjection(
		"reclaim effective", view.ReclaimEffective, view.ReclaimEffectivePerNUMA, topology); err != nil {
		return err
	}
	if err := validateExactNUMAProjection(
		"desired reclaim effective", view.DesiredReclaimEffective, view.DesiredReclaimEffectivePerNUMA, topology); err != nil {
		return err
	}
	return nil
}

func validateExactNUMAProjection(
	name string,
	global machine.CPUSet,
	perNUMA map[int]machine.CPUSet,
	topology *machine.CPUTopology,
) error {
	numaIDs := topology.CPUDetails.NUMANodes()
	if len(perNUMA) != numaIDs.Size() {
		return fmt.Errorf("bulkhead cpuset partition %s NUMA bucket count %d does not match machine NUMA count %d",
			name, len(perNUMA), numaIDs.Size())
	}
	for numaID := range perNUMA {
		if !numaIDs.Contains(numaID) {
			return fmt.Errorf("bulkhead cpuset partition %s has unknown NUMA bucket: %d", name, numaID)
		}
	}
	for _, numaID := range numaIDs.ToSliceInt() {
		actual, ok := perNUMA[numaID]
		if !ok {
			return fmt.Errorf("bulkhead cpuset partition missing %s NUMA bucket: %d", name, numaID)
		}
		expected := global.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		if !actual.Equals(expected) {
			return fmt.Errorf("bulkhead cpuset partition %s NUMA bucket %d is %s, want exact global projection %s",
				name, numaID, actual.String(), expected.String())
		}
	}
	return nil
}

func ApplyTransientProtectedNonReclaim(view *model.DesiredView, topology *machine.CPUTopology, protected machine.CPUSet) {
	if view == nil || topology == nil {
		return
	}
	if protected.IsEmpty() {
		view.TransientProtectedNonReclaim = machine.NewCPUSet()
		rebuildTransientProtectedNonReclaimPerNUMA(view, topology)
		return
	}
	available := topology.CPUDetails.CPUs().Difference(view.Reserve)
	view.TransientProtectedNonReclaim = protected.Intersection(available)
	view.NonReclaimPool = view.DesiredNonReclaimPool.Union(view.TransientProtectedNonReclaim).Intersection(available)
	view.ReclaimEffective = view.DesiredReclaimEffective.Difference(view.NonReclaimPool)
	rebuildReclaimEffectivePerNUMA(view, topology)
	rebuildTransientProtectedNonReclaimPerNUMA(view, topology)
}

func padNonReclaimPoolToMinSize(view *model.DesiredView, topology *machine.CPUTopology, opts CPUSetPartitionViewOptions) {
	if view == nil || topology == nil || opts.NonReclaimPoolMinSize <= 0 {
		return
	}
	currentSize := view.NonReclaimPool.Size()
	if currentSize >= int(opts.NonReclaimPoolMinSize) {
		return
	}
	deficit := int(opts.NonReclaimPoolMinSize) - currentSize
	candidates := view.ReclaimEffective.Clone()
	if candidates.IsEmpty() {
		return
	}
	if deficit > candidates.Size() {
		deficit = candidates.Size()
	}

	padding := takeCPUsByNUMABalanceWithSeed(topology, candidates, view.NonReclaimPool, deficit, opts.ReserveCPUReversely)
	view.NonReclaimPool = view.NonReclaimPool.Union(padding)
	view.ReclaimEffective = view.ReclaimEffective.Difference(padding)
}

func takeCPUsByNUMABalanceWithSeed(topology *machine.CPUTopology, candidates, seed machine.CPUSet, count int, reverse bool) machine.CPUSet {
	if topology == nil || count <= 0 || candidates.IsEmpty() {
		return machine.NewCPUSet()
	}

	candidateByNUMA := map[int][]int{}
	currentCountByNUMA := map[int]int{}
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	for _, numaID := range numaIDs {
		numaCPUs := topology.CPUDetails.CPUsInNUMANodes(numaID)
		currentCountByNUMA[numaID] = seed.Intersection(numaCPUs).Size()
		numaCandidates := candidates.Intersection(numaCPUs)
		if reverse {
			candidateByNUMA[numaID] = numaCandidates.ToSliceIntReversely()
		} else {
			candidateByNUMA[numaID] = numaCandidates.ToSliceInt()
		}
	}

	result := machine.NewCPUSet()
	for result.Size() < count {
		selectedNUMA := -1
		for _, numaID := range numaIDs {
			if len(candidateByNUMA[numaID]) == 0 {
				continue
			}
			if selectedNUMA == -1 || currentCountByNUMA[numaID] < currentCountByNUMA[selectedNUMA] {
				selectedNUMA = numaID
			}
		}
		if selectedNUMA == -1 {
			break
		}
		cpu := candidateByNUMA[selectedNUMA][0]
		candidateByNUMA[selectedNUMA] = candidateByNUMA[selectedNUMA][1:]
		result.Add(cpu)
		currentCountByNUMA[selectedNUMA]++
	}
	return result
}

// rebalanceHardPartitionReclaimEffective removes asymmetric advisor raw slack
// from the effective reclaim domain so hard-partition reclaim stays balanced
// across physical NUMAs. The mandatory ramp-up floor is distributed evenly by
// the global target, therefore any NUMA carrying more than the global minimum
// per-NUMA count is holding stale raw slack. Those extra (highest-ID) CPUs are
// moved back to the non-reclaim domain, which both eliminates the slack and
// keeps the two domains mutually exclusive. NUMAs that already sit at or below
// the minimum are never shrunk, so a structurally under-provisioned NUMA is
// still surfaced by validateHardPartitionReclaimPerNUMA rather than masked.
func rebalanceHardPartitionReclaimEffective(view *model.DesiredView, topology *machine.CPUTopology) {
	if view == nil || topology == nil {
		return
	}
	numaIDs := topology.CPUDetails.NUMANodes().ToSliceInt()
	if len(numaIDs) == 0 {
		return
	}

	minCount := -1
	countByNUMA := make(map[int]int, len(numaIDs))
	for _, numaID := range numaIDs {
		count := view.ReclaimEffective.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID)).Size()
		countByNUMA[numaID] = count
		if minCount == -1 || count < minCount {
			minCount = count
		}
	}
	if minCount <= 0 {
		return
	}
	// A minimum below the hard floor means the partition is structurally
	// under-provisioned, not merely carrying slack. Trimming toward a broken
	// minimum would destroy valid reclaim CPUs and mask the real defect, so
	// leave the raw shape untouched and let validateHardPartitionReclaimPerNUMA
	// report the offending NUMA with its stable per-NUMA diagnostic.
	if minCount < minimumHardPartitionReclaimCPUsPerNUMA {
		return
	}

	trimmed := machine.NewCPUSet()
	for _, numaID := range numaIDs {
		excess := countByNUMA[numaID] - minCount
		if excess <= 0 {
			continue
		}
		numaReclaim := view.ReclaimEffective.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		// Trim the highest-ID CPUs first so the retained floor is deterministic
		// and matches the ascending selection used elsewhere in the view.
		for _, cpu := range numaReclaim.ToSliceIntReversely()[:excess] {
			trimmed.Add(cpu)
		}
	}
	if trimmed.IsEmpty() {
		return
	}

	view.ReclaimEffective = view.ReclaimEffective.Difference(trimmed)
	available := topology.CPUDetails.CPUs().Difference(view.Reserve)
	view.NonReclaimPool = available.Difference(view.ReclaimEffective)
	rebuildReclaimEffectivePerNUMA(view, topology)
}

func rebuildReclaimEffectivePerNUMA(view *model.DesiredView, topology *machine.CPUTopology) {
	if view == nil {
		return
	}
	view.ReclaimEffectivePerNUMA = map[int]machine.CPUSet{}
	if topology == nil {
		return
	}
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceNoSortInt() {
		intersection := view.ReclaimEffective.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		view.ReclaimEffectivePerNUMA[numaID] = intersection
	}
}

func rebuildDesiredReclaimEffectivePerNUMA(view *model.DesiredView, topology *machine.CPUTopology) {
	if view == nil {
		return
	}
	view.DesiredReclaimEffectivePerNUMA = map[int]machine.CPUSet{}
	if topology == nil {
		return
	}
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceNoSortInt() {
		intersection := view.DesiredReclaimEffective.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		view.DesiredReclaimEffectivePerNUMA[numaID] = intersection
	}
}

func rebuildTransientProtectedNonReclaimPerNUMA(view *model.DesiredView, topology *machine.CPUTopology) {
	if view == nil {
		return
	}
	view.TransientProtectedNonReclaimPerNUMA = map[int]machine.CPUSet{}
	if topology == nil {
		return
	}
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceNoSortInt() {
		intersection := view.TransientProtectedNonReclaim.Intersection(topology.CPUDetails.CPUsInNUMANodes(numaID))
		view.TransientProtectedNonReclaimPerNUMA[numaID] = intersection
	}
}
