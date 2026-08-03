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
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func commitStateTargetForTest(t *testing.T, repository state.State, mutate func(*state.TargetState)) {
	t.Helper()
	target, err := repository.PrepareDurableTarget()
	if err != nil {
		t.Fatal(err)
	}
	mutate(target)
	if err := repository.CommitTarget(target); err != nil {
		t.Fatal(err)
	}
}

func setPodEntriesForTest(t *testing.T, repository state.State, entries state.PodEntries, _ ...bool) {
	t.Helper()
	commitStateTargetForTest(t, repository, func(target *state.TargetState) {
		target.PodEntries = entries.Clone()
	})
}

func setMachineStateForTest(t *testing.T, repository state.State, machineState state.NUMANodeMap, _ ...bool) {
	t.Helper()
	commitStateTargetForTest(t, repository, func(target *state.TargetState) {
		target.MachineState = machineState.Clone()
	})
}

func setNUMAHeadroomForTest(t *testing.T, repository state.State, headroom map[int]float64, _ ...bool) {
	t.Helper()
	commitStateTargetForTest(t, repository, func(target *state.TargetState) {
		target.NUMAHeadroom = make(map[int]float64, len(headroom))
		for numaID, value := range headroom {
			target.NUMAHeadroom[numaID] = value
		}
	})
}

func setAllocationInfoForTest(
	t *testing.T,
	repository state.State,
	podUID, containerName string,
	allocationInfo *state.AllocationInfo,
	_ ...bool,
) {
	t.Helper()
	commitStateTargetForTest(t, repository, func(target *state.TargetState) {
		putTargetAllocation(target, podUID, containerName, allocationInfo)
	})
}

func setAllowSharedOverlapForTest(t *testing.T, repository state.State, enabled bool, _ ...bool) {
	t.Helper()
	commitStateTargetForTest(t, repository, func(target *state.TargetState) {
		target.AllowSharedCoresOverlapReclaimedCores = enabled
	})
}

func setDisableDedicatedOverlapForTest(t *testing.T, repository state.State, enabled bool, _ ...bool) {
	t.Helper()
	commitStateTargetForTest(t, repository, func(target *state.TargetState) {
		target.DisableDedicatedCoresOverlapReclaimedCores = enabled
	})
}

func deleteAllocationForTest(t *testing.T, repository state.State, podUID, containerName string, _ ...bool) {
	t.Helper()
	commitStateTargetForTest(t, repository, func(target *state.TargetState) {
		deleteTargetAllocation(target, podUID, containerName)
	})
}

func (p *DynamicPolicy) sharedCoresWithoutNUMABindingAllocationHandler(
	ctx context.Context, req *pluginapi.ResourceRequest, persist bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return nil, err
	}
	resp, err := p.sharedCoresWithoutNUMABindingAllocationHandlerOnTarget(ctx, req, persist, target)
	if err != nil {
		return nil, err
	}
	return resp, p.state.CommitTarget(target)
}

func (p *DynamicPolicy) dedicatedCoresWithNUMABindingAllocationHandler(
	ctx context.Context, req *pluginapi.ResourceRequest, persist bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return nil, err
	}
	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandlerOnTarget(ctx, req, persist, target)
	if err != nil {
		return nil, err
	}
	return resp, p.state.CommitTarget(target)
}

func (p *DynamicPolicy) getReclaimOverlapShareRatio(entries state.PodEntries) (map[string]float64, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return nil, err
	}
	return p.getReclaimOverlapShareRatioForTarget(entries, target)
}

func (p *DynamicPolicy) writeRampUpReclaimPoolTarget(
	ctx context.Context,
	info *state.AllocationInfo,
	persist bool,
	rollbackEntries state.PodEntries,
	rollbackMachineState state.NUMANodeMap,
) error {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return err
	}
	err = p.writeRampUpReclaimPoolTargetOnTarget(
		ctx, info, persist, rollbackEntries, rollbackMachineState, target)
	if err != nil {
		return err
	}
	return p.state.CommitTarget(target)
}

func (p *DynamicPolicy) allocateNumaBindingCPUs(
	numCPUs int,
	hint *pluginapi.TopologyHint,
	machineState state.NUMANodeMap,
	annotations map[string]string,
	podReclaimEnabled bool,
) (machine.CPUSet, machine.CPUSet, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return machine.NewCPUSet(), machine.NewCPUSet(), err
	}
	return p.allocateNumaBindingCPUsForTarget(
		numCPUs, hint, machineState, annotations, podReclaimEnabled, target)
}

func (p *DynamicPolicy) apportionReclaimedPool(
	pools map[string]machine.CPUSet,
	reclaimed machine.CPUSet,
	quantities map[string]int,
) machine.CPUSet {
	target, _ := p.state.PrepareDurableTarget()
	return p.apportionReclaimedPoolForTarget(pools, reclaimed, quantities, target)
}

func (p *DynamicPolicy) selectRampUpHardReclaimFromEligible(
	eligible machine.CPUSet, exclusive, podReclaimEnabled bool,
) (machine.CPUSet, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return machine.NewCPUSet(), err
	}
	return p.selectRampUpHardReclaimFromEligibleForTarget(
		eligible, exclusive, podReclaimEnabled, target)
}

func (p *DynamicPolicy) generatePoolsAndIsolation(
	pools map[string]map[int]int,
	isolated map[string]map[string]int,
	available machine.CPUSet,
	overlapRatio map[string]float64,
) (map[string]machine.CPUSet, map[string]map[string]machine.CPUSet, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return nil, nil, err
	}
	return p.generatePoolsAndIsolationForTarget(
		pools, isolated, available, overlapRatio, target)
}

func (p *DynamicPolicy) groupAndAllocatePools(
	pools map[string]map[int]int,
	isolated map[string]map[string]int,
	available machine.CPUSet,
	pinned map[string]machine.CPUSet,
	overlapRatio map[string]float64,
) (map[string]machine.CPUSet, map[string]map[string]machine.CPUSet, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return nil, nil, err
	}
	return p.groupAndAllocatePoolsForTarget(
		pools, isolated, available, pinned, overlapRatio, target)
}

func (p *DynamicPolicy) adjustAllocationEntries(
	entries state.PodEntries, machineState state.NUMANodeMap, persist bool,
) error {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return err
	}
	if err := p.adjustAllocationEntriesOnTarget(entries, machineState, persist, target); err != nil {
		return err
	}
	return p.state.CommitTarget(target)
}

func (p *DynamicPolicy) allocateSharedNumaBindingCPUs(
	ctx context.Context, req *pluginapi.ResourceRequest, hint *pluginapi.TopologyHint, persist bool,
) (*state.AllocationInfo, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return nil, err
	}
	info, err := p.allocateSharedNumaBindingCPUsOnTarget(ctx, req, hint, persist, target)
	if err != nil {
		return nil, err
	}
	return info, p.state.CommitTarget(target)
}

func (p *DynamicPolicy) selectRampUpHardReclaimFromEligibleWithOptions(
	eligible machine.CPUSet, exclusive, podReclaimEnabled, preserveExistingFloor bool,
) (machine.CPUSet, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return machine.NewCPUSet(), err
	}
	return p.selectRampUpHardReclaimFromEligibleWithOptionsForTarget(
		eligible, exclusive, podReclaimEnabled, preserveExistingFloor, target)
}

func (p *DynamicPolicy) generateNUMABindingPoolsCPUSetInPlace(
	pools map[string]machine.CPUSet,
	quantities map[string]map[int]int,
	available machine.CPUSet,
) (machine.CPUSet, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return machine.NewCPUSet(), err
	}
	return p.generateNUMABindingPoolsCPUSetInPlaceForTarget(pools, quantities, available, target)
}

func (p *DynamicPolicy) adjustPoolsAndIsolatedEntries(
	pools map[string]map[int]int,
	isolated map[string]map[string]int,
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	persist bool,
) error {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return err
	}
	if err := p.adjustPoolsAndIsolatedEntriesOnTarget(
		pools, isolated, entries, machineState, persist, target); err != nil {
		return err
	}
	return p.state.CommitTarget(target)
}

func (p *DynamicPolicy) applyPoolsAndIsolatedInfo(
	pools map[string]machine.CPUSet,
	isolated map[string]map[string]machine.CPUSet,
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	sharedBindingNUMAs sets.Int,
	persist bool,
) error {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return err
	}
	if err := p.applyPoolsAndIsolatedInfoOnTarget(
		pools, isolated, entries, machineState, sharedBindingNUMAs, persist, target); err != nil {
		return err
	}
	return p.state.CommitTarget(target)
}

func (p *DynamicPolicy) cleanPools() error {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return err
	}
	if err := p.cleanPoolsOnTarget(target); err != nil {
		return err
	}
	return p.state.CommitTarget(target)
}

func (p *DynamicPolicy) allocateFakeNUMANormalShareBlocks(
	blocks []*advisorapi.BlockInfo,
	blockCPUSet advisorapi.BlockCPUSet,
	availableCPUs, nodeRemainingCPUs *machine.CPUSet,
	globalNonReclaimableCPUSet machine.CPUSet,
) error {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return err
	}
	return p.allocateFakeNUMANormalShareBlocksForTarget(
		blocks, blockCPUSet, availableCPUs, nodeRemainingCPUs, globalNonReclaimableCPUSet, target)
}

func (p *DynamicPolicy) applySystemExclusivePoolChanges(
	toCreate, toUpdate map[string]int, toDelete sets.String,
) error {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return err
	}
	if err := p.applySystemExclusivePoolChangesForTarget(toCreate, toUpdate, toDelete, target); err != nil {
		return err
	}
	return p.state.CommitTarget(target)
}

func (p *DynamicPolicy) updateSystemExclusivePool(
	toUpdate map[string]int, available machine.CPUSet,
) (machine.CPUSet, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return machine.NewCPUSet(), err
	}
	result, err := p.updateSystemExclusivePoolForTarget(toUpdate, available, target)
	if err != nil {
		return machine.NewCPUSet(), err
	}
	return result, p.state.CommitTarget(target)
}

func (p *DynamicPolicy) createSystemExclusivePool(
	toCreate map[string]int, available machine.CPUSet,
) (machine.CPUSet, error) {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return machine.NewCPUSet(), err
	}
	result, err := p.createSystemExclusivePoolForTarget(toCreate, available, target)
	if err != nil {
		return machine.NewCPUSet(), err
	}
	return result, p.state.CommitTarget(target)
}

func (p *DynamicPolicy) adjustSystemCoresPodAllocation() error {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return err
	}
	if err := p.adjustSystemCoresPodAllocationForTarget(target); err != nil {
		return err
	}
	return p.state.CommitTarget(target)
}
