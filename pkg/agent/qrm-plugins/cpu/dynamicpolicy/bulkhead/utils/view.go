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

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	cpustate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

type CPUSetPartitionViewOptions struct {
	NonReclaimPoolMinSize             int64
	HardPartitionEnabled              bool
	HardPartitionReclaimTargetPerNUMA map[int]int
	HardPartitionTargetError          error
	ReserveCPUReversely               bool
	TransientProtectedNonReclaim      machine.CPUSet
}

type CPUSetPartitionViewState struct {
	State                         cpustate.ReadonlyState
	ReservedCPUs                  machine.CPUSet
	ReservedReclaimedCPUs         machine.CPUSet
	ReservedReclaimedCPUsFallback int
}

func NewCPUSetPartitionViewOptions(
	coreConf *config.Configuration,
	dynamicConf *dynamicconfig.Configuration,
	topology *machine.CPUTopology,
	hardActive bool,
) CPUSetPartitionViewOptions {
	opts := newCPUSetPartitionViewOptions(coreConf, dynamicConf, hardActive)
	if !opts.HardPartitionEnabled {
		return opts
	}
	if topology == nil {
		opts.HardPartitionTargetError = fmt.Errorf(
			"resolve bulkhead hard-partition reclaim targets: missing topology")
		return opts
	}

	targets, err := machine.ResolveHardPartitionReclaimTargets(dynamicConf, topology, 0, nil, nil)
	if err != nil {
		opts.HardPartitionTargetError = err
	} else {
		opts.HardPartitionReclaimTargetPerNUMA = targets
	}
	return opts
}

func NewCPUSetPartitionViewOptionsWithState(
	coreConf *config.Configuration,
	dynamicConf *dynamicconfig.Configuration,
	topology *machine.CPUTopology,
	viewState CPUSetPartitionViewState,
	hardActive bool,
) CPUSetPartitionViewOptions {
	opts := newCPUSetPartitionViewOptions(coreConf, dynamicConf, hardActive)
	if !opts.HardPartitionEnabled {
		return opts
	}
	if topology == nil {
		opts.HardPartitionTargetError = fmt.Errorf(
			"resolve bulkhead hard-partition reclaim targets: missing topology")
		return opts
	}

	eligibleByNUMA, err := eligibleCPUSetByNUMA(viewState.State, topology, viewState.ReservedCPUs)
	if err != nil {
		opts.HardPartitionTargetError = err
		return opts
	}
	targets, err := machine.ResolveHardPartitionReclaimTargets(
		dynamicConf,
		topology,
		viewState.ReservedReclaimedCPUsFallback,
		func(numaID int) int {
			return viewState.ReservedReclaimedCPUs.Intersection(eligibleByNUMA[numaID]).Size()
		},
		func(numaID int) int { return eligibleByNUMA[numaID].Size() },
	)
	if err != nil {
		opts.HardPartitionTargetError = err
	} else {
		opts.HardPartitionReclaimTargetPerNUMA = targets
	}
	return opts
}

func newCPUSetPartitionViewOptions(
	coreConf *config.Configuration,
	dynamicConf *dynamicconfig.Configuration,
	hardActive bool,
) CPUSetPartitionViewOptions {
	nonReclaimPoolMinSize := configuredNonReclaimPoolMinSize(dynamicConf)
	if nonReclaimPoolMinSize <= 0 && coreConf != nil && coreConf.DynamicAgentConfiguration != nil {
		nonReclaimPoolMinSize = configuredNonReclaimPoolMinSize(coreConf.DynamicAgentConfiguration.GetDynamicConfiguration())
	}

	opts := CPUSetPartitionViewOptions{
		NonReclaimPoolMinSize:             nonReclaimPoolMinSize,
		HardPartitionEnabled:              hardActive && hardPartitionEnabled(dynamicConf),
		HardPartitionReclaimTargetPerNUMA: map[int]int{},
	}
	if coreConf != nil {
		opts.ReserveCPUReversely = coreConf.EnableReserveCPUReversely
	}
	return opts
}

func eligibleCPUSetByNUMA(
	state cpustate.ReadonlyState,
	topology *machine.CPUTopology,
	reservedCPUs machine.CPUSet,
) (map[int]machine.CPUSet, error) {
	if state == nil {
		return nil, fmt.Errorf("resolve bulkhead hard-partition reclaim targets: missing state")
	}

	machineState := state.GetMachineState()
	eligibleByNUMA := make(map[int]machine.CPUSet, topology.CPUDetails.NUMANodes().Size())
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceInt() {
		numaState := machineState[numaID]
		if numaState == nil {
			return nil, fmt.Errorf(
				"resolve bulkhead hard-partition reclaim targets: missing machine state for NUMA %d",
				numaID,
			)
		}

		numaCPUs := topology.CPUDetails.CPUsInNUMANodes(numaID)
		stateCPUs := numaState.DefaultCPUSet.Union(numaState.AllocatedCPUSet)
		if outside := stateCPUs.Difference(numaCPUs); !outside.IsEmpty() {
			return nil, fmt.Errorf(
				"resolve bulkhead hard-partition reclaim targets: machine state for NUMA %d contains CPUs outside NUMA topology: %s",
				numaID,
				outside.String(),
			)
		}
		if overlap := numaState.DefaultCPUSet.Intersection(numaState.AllocatedCPUSet); !overlap.IsEmpty() {
			return nil, fmt.Errorf(
				"resolve bulkhead hard-partition reclaim targets: machine state for NUMA %d has default/allocated overlap: %s",
				numaID,
				overlap.String(),
			)
		}
		if !stateCPUs.Equals(numaCPUs) {
			return nil, fmt.Errorf(
				"resolve bulkhead hard-partition reclaim targets: machine state for NUMA %d does not cover NUMA topology: state=%s topology=%s",
				numaID,
				stateCPUs.String(),
				numaCPUs.String(),
			)
		}
		eligibleByNUMA[numaID] = numaState.GetAvailableCPUSet(reservedCPUs)
	}
	return eligibleByNUMA, nil
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
	return conf.EnableReclaim &&
		conf.AdminQoSConfiguration.CPUPluginConfiguration.EnableRampUpReclaimHardPartition
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
	sharedPoolNameForAllocation := func(allocation *cpustate.AllocationInfo) string {
		poolName, err := allocation.GetSpecifiedNUMABindingPoolName()
		if err != nil {
			poolName = commonstate.PoolNameShare
		}
		return resourcepackage.WrapOwnerPoolName(poolName, allocation.GetResourcePackageName())
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
					poolName := sharedPoolNameForAllocation(allocation)
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
		// In hard-partition mode reclaim raw, dedicated, and isolation owners must
		// be removed from shared targets, while real shared NUMA-binding ramp-up
		// allocations remain explicit non-reclaim owners.
		fixedNonShare := view.ReclaimRaw.Union(view.Dedicated).Union(view.Isolation)
		view.SharePool = view.SharePool.Difference(fixedNonShare).Union(sharedRampUp)
		for poolName, cpus := range view.SharePoolMap {
			view.SharePoolMap[poolName] = cpus.Difference(fixedNonShare)
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
		// Hard-partition reclaim is already materialized and validated by QRM.
		// Bulkhead only projects that ownership; it must not trim legal reclaim
		// to satisfy its independent non-reclaim padding preference.
		if !opts.HardPartitionEnabled {
			padNonReclaimPoolToMinSize(view, topology, opts)
		}
	}
	view.DesiredNonReclaimPool = view.NonReclaimPool.Clone()
	view.DesiredReclaimEffective = view.ReclaimEffective.Clone()
	rebuildDesiredReclaimEffectivePerNUMA(view, topology)
	ApplyTransientProtectedNonReclaim(view, topology, opts.TransientProtectedNonReclaim)
	rebuildReclaimEffectivePerNUMA(view, topology)
	rebuildDesiredPoolOwners(view, podEntries)
	return view
}

func rebuildDesiredPoolOwners(view *model.DesiredView, podEntries cpustate.PodEntries) {
	view.PoolOwners = map[model.CPUSetPoolIdentity]model.DesiredPoolOwner{}
	recordDesiredPoolOwner(
		view.PoolOwners,
		model.CPUSetPoolIdentity{Kind: model.CPUSetPoolKindReclaim},
		"",
		"",
		view.ReclaimEffective,
	)
	for poolName, cpus := range view.SharePoolMap {
		recordDesiredPoolOwner(
			view.PoolOwners,
			model.CPUSetPoolIdentity{Kind: model.CPUSetPoolKindShare, Name: poolName},
			"",
			"",
			cpus,
		)
	}

	for _, containerEntries := range podEntries {
		if containerEntries.IsPoolEntry() {
			continue
		}
		for _, allocation := range containerEntries {
			identity, ok := poolIdentityForAllocation(allocation)
			if !ok {
				continue
			}
			containerName := allocation.ContainerName
			if identity.Kind == model.CPUSetPoolKindShare {
				containerName = ""
			}
			recordDesiredPoolOwner(
				view.PoolOwners,
				identity,
				allocation.PodUid,
				containerName,
				allocation.AllocationResult,
			)
		}
	}
}

func poolIdentityForAllocation(allocation *cpustate.AllocationInfo) (model.CPUSetPoolIdentity, bool) {
	if allocation == nil {
		return model.CPUSetPoolIdentity{}, false
	}
	if allocation.RampUp && allocation.CheckSharedNUMABinding() {
		return model.CPUSetPoolIdentity{}, false
	}

	poolType := commonstate.GetPoolType(
		commonstate.OwnerPoolNameTranslator.Translate(allocation.OwnerPoolName))
	if poolType == commonstate.PoolNamePrefixIsolation {
		return model.CPUSetPoolIdentity{
			Kind:         model.CPUSetPoolKindIsolation,
			PodNamespace: allocation.PodNamespace,
			PodName:      allocation.PodName,
		}, true
	}
	switch poolType {
	case commonstate.PoolNameReserve, commonstate.PoolNamePrefixSystem, commonstate.PoolNameReclaim:
		return model.CPUSetPoolIdentity{}, false
	}
	if allocation.CheckDedicated() {
		return model.CPUSetPoolIdentity{
			Kind:         model.CPUSetPoolKindDedicated,
			PodNamespace: allocation.PodNamespace,
			PodName:      allocation.PodName,
		}, true
	}
	return model.CPUSetPoolIdentity{}, false
}

func recordDesiredPoolOwner(
	owners map[model.CPUSetPoolIdentity]model.DesiredPoolOwner,
	identity model.CPUSetPoolIdentity,
	podUID string,
	containerName string,
	cpus machine.CPUSet,
) {
	owner := owners[identity]
	if owner.ProofPodUID == "" {
		owner.ProofPodUID = podUID
	}
	owner.ExpectedCPUSet = owner.ExpectedCPUSet.Union(cpus)
	if containerName != "" {
		if owner.ContainerCPUSetByName == nil {
			owner.ContainerCPUSetByName = map[string]machine.CPUSet{}
		}
		owner.ContainerCPUSetByName[containerName] = owner.ContainerCPUSetByName[containerName].Union(cpus)
	}
	owners[identity] = owner
}

func BuildValidatedCPUSetPartitionView(state cpustate.ReadonlyState, topology *machine.CPUTopology, opts CPUSetPartitionViewOptions) (*model.DesiredView, error) {
	if opts.HardPartitionTargetError != nil {
		return nil, opts.HardPartitionTargetError
	}
	if opts.HardPartitionEnabled && state == nil {
		return nil, fmt.Errorf("build bulkhead hard-partition view: missing state")
	}
	view := BuildCPUSetPartitionView(state, topology, opts)
	if err := ValidateCPUSetPartitionView(view, topology); err != nil {
		return nil, err
	}
	if opts.HardPartitionEnabled {
		// NUMAs owned by a committed steady exclusive DNB keep only their
		// finalized reserve once ramp-up ends, regardless of reclaimability; the
		// precommit validator must skip them via the shared state helper,
		// otherwise it rejects every other ramp-up QoS.
		skipNUMAs := state.GetPodEntries().SteadyExclusiveNUMAs(topology)
		if err := validateHardPartitionReclaimPerNUMA(
			view.ReclaimEffectivePerNUMA, opts.HardPartitionReclaimTargetPerNUMA,
			skipNUMAs, topology); err != nil {
			return nil, err
		}
	}
	return view, nil
}

func validateHardPartitionReclaimPerNUMA(
	reclaimPerNUMA map[int]machine.CPUSet,
	targetPerNUMA map[int]int,
	skipNUMAs sets.Int,
	topology *machine.CPUTopology,
) error {
	if topology == nil {
		return nil
	}

	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceInt() {
		if skipNUMAs.Has(numaID) {
			continue
		}
		target, ok := targetPerNUMA[numaID]
		if !ok {
			return fmt.Errorf("bulkhead hard-partition reclaim target missing for NUMA %d", numaID)
		}
		count := reclaimPerNUMA[numaID].Size()
		if count < target {
			return fmt.Errorf("bulkhead hard-partition reclaim NUMA %d has %d CPUs, target is %d", numaID, count, target)
		}
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
