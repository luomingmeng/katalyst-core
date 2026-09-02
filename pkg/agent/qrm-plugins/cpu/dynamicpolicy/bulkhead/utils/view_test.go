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
	"strings"
	"testing"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	cpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	cpustate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNewCPUSetPartitionViewOptionsUsesProductionConfigurationConsistently(t *testing.T) {
	t.Parallel()

	defaultDynamic := dynamicconfig.NewDynamicAgentConfiguration()
	defaultDynamic.GetDynamicConfiguration().AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize = 7
	coreConf := config.NewConfiguration()
	coreConf.EnableReserveCPUReversely = true
	coreConf.DynamicAgentConfiguration = defaultDynamic
	currentDynamic := dynamicconfig.NewDynamicAgentConfiguration().GetDynamicConfiguration()
	currentDynamic.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize = 5
	currentDynamic.EnableReclaim = true
	currentDynamic.AdminQoSConfiguration.CPUPluginConfiguration.EnableRampUpReclaimHardPartition = true
	currentDynamic.AdminQoSConfiguration.CPUPluginConfiguration.InitialRampUpReclaimCPUSetRatio = 0.2
	topology := testTwoNUMATopologyN(32)

	inactiveOpts := NewCPUSetPartitionViewOptionsWithState(
		coreConf, currentDynamic, topology, CPUSetPartitionViewState{}, false)
	if inactiveOpts.HardPartitionEnabled || len(inactiveOpts.HardPartitionReclaimTargetPerNUMA) != 0 {
		t.Fatalf("configured inactive hard partition must not resolve targets: %+v", inactiveOpts)
	}

	state := cpustate.NewCPUPluginState(topology)
	opts := NewCPUSetPartitionViewOptionsWithState(
		coreConf, currentDynamic, topology, CPUSetPartitionViewState{State: state}, true)
	if opts.NonReclaimPoolMinSize != 5 || !opts.ReserveCPUReversely || !opts.HardPartitionEnabled {
		t.Fatalf("unexpected current options: %+v", opts)
	}
	if got := opts.HardPartitionReclaimTargetPerNUMA[0]; got != 6 {
		t.Fatalf("NUMA 0 immutable reclaim target = %d, want 6 for 32 CPUs at ratio 0.2", got)
	}
	if got := opts.HardPartitionReclaimTargetPerNUMA[1]; got != 6 {
		t.Fatalf("NUMA 1 immutable reclaim target = %d, want 6 for 32 CPUs at ratio 0.2", got)
	}

	currentDynamic.MinReclaimedResourceForAllocate = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("16"),
	}
	opts = NewCPUSetPartitionViewOptionsWithState(
		coreConf, currentDynamic, topology, CPUSetPartitionViewState{State: state}, true)
	if got := opts.HardPartitionReclaimTargetPerNUMA[0]; got != 8 {
		t.Fatalf("NUMA 0 configured reclaim target = %d, want 8", got)
	}
	if got := opts.HardPartitionReclaimTargetPerNUMA[1]; got != 8 {
		t.Fatalf("NUMA 1 configured reclaim target = %d, want 8", got)
	}

	currentDynamic.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize = 0
	opts = NewCPUSetPartitionViewOptionsWithState(
		coreConf, currentDynamic, topology, CPUSetPartitionViewState{State: state}, true)
	if opts.NonReclaimPoolMinSize != 7 {
		t.Fatalf("fallback NonReclaimPoolMinSize = %d, want 7", opts.NonReclaimPoolMinSize)
	}

	currentDynamic.EnableReclaim = false
	opts = NewCPUSetPartitionViewOptionsWithState(
		coreConf, currentDynamic, topology, CPUSetPartitionViewState{State: state}, true)
	if opts.HardPartitionEnabled {
		t.Fatal("hard partition must be ineffective when reclaim is disabled")
	}
}

func TestNewCPUSetPartitionViewOptionsUsesEligibleCapacityFromState(t *testing.T) {
	t.Parallel()

	topology := testTwoNUMATopologyN(32)
	dynamicConf := dynamicconfig.NewDynamicAgentConfiguration().GetDynamicConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.EnableRampUpReclaimHardPartition = true
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.InitialRampUpReclaimCPUSetRatio = 0.2

	state := cpustate.NewCPUPluginState(topology)
	machineState := state.GetMachineState()
	machineState[0].DefaultCPUSet = machine.NewCPUSet()
	for cpu := 0; cpu < 21; cpu++ {
		machineState[0].DefaultCPUSet.Add(cpu)
	}
	machineState[0].AllocatedCPUSet = machine.NewCPUSet()
	for cpu := 21; cpu < 32; cpu++ {
		machineState[0].AllocatedCPUSet.Add(cpu)
	}
	state.SetMachineState(machineState)

	opts := NewCPUSetPartitionViewOptionsWithState(
		nil, dynamicConf, topology, CPUSetPartitionViewState{
			State:        state,
			ReservedCPUs: machine.NewCPUSet(0),
		}, true)
	if opts.HardPartitionTargetError != nil {
		t.Fatalf("NewCPUSetPartitionViewOptions() error = %v", opts.HardPartitionTargetError)
	}
	if got := opts.HardPartitionReclaimTargetPerNUMA[0]; got != 4 {
		t.Fatalf("NUMA 0 target = %d, want 4 from 20 eligible CPUs", got)
	}
	if got := opts.HardPartitionReclaimTargetPerNUMA[1]; got != 6 {
		t.Fatalf("NUMA 1 target = %d, want 6 from 32 eligible CPUs", got)
	}
}

func TestNewCPUSetPartitionViewOptionsWithStateUsesReservedReclaimFloors(t *testing.T) {
	t.Parallel()

	topology := testTwoNUMASMTTopology()
	dynamicConf := dynamicconfig.NewDynamicAgentConfiguration().GetDynamicConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.EnableRampUpReclaimHardPartition = true

	state := cpustate.NewCPUPluginState(topology)
	opts := NewCPUSetPartitionViewOptionsWithState(
		nil,
		dynamicConf,
		topology,
		CPUSetPartitionViewState{
			State:                         state,
			ReservedCPUs:                  machine.NewCPUSet(),
			ReservedReclaimedCPUs:         machine.NewCPUSet(0, 1, 2, 3),
			ReservedReclaimedCPUsFallback: 4,
		},
		true,
	)
	if opts.HardPartitionTargetError != nil {
		t.Fatalf("NewCPUSetPartitionViewOptionsWithState() error = %v", opts.HardPartitionTargetError)
	}
	if got := opts.HardPartitionReclaimTargetPerNUMA[0]; got != 4 {
		t.Fatalf("NUMA 0 target = %d, want reserved reclaim floor 4", got)
	}
	if got := opts.HardPartitionReclaimTargetPerNUMA[1]; got != 2 {
		t.Fatalf("NUMA 1 target = %d, want one-core baseline 2", got)
	}
}

func TestNewCPUSetPartitionViewOptionsRejectsIncompleteEligibleCapacityState(t *testing.T) {
	t.Parallel()

	topology := testTwoNUMATopologyN(32)
	dynamicConf := dynamicconfig.NewDynamicAgentConfiguration().GetDynamicConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.EnableRampUpReclaimHardPartition = true

	tests := []struct {
		name  string
		state cpustate.ReadonlyState
		want  string
	}{
		{
			name: "nil state",
			want: "missing state",
		},
		{
			name: "missing NUMA state",
			state: func() cpustate.ReadonlyState {
				state := cpustate.NewCPUPluginState(topology)
				machineState := state.GetMachineState()
				delete(machineState, 1)
				state.SetMachineState(machineState)
				return state
			}(),
			want: "missing machine state for NUMA 1",
		},
		{
			name: "incomplete NUMA state",
			state: func() cpustate.ReadonlyState {
				state := cpustate.NewCPUPluginState(topology)
				machineState := state.GetMachineState()
				machineState[0].DefaultCPUSet = machineState[0].DefaultCPUSet.Difference(machine.NewCPUSet(0))
				state.SetMachineState(machineState)
				return state
			}(),
			want: "does not cover NUMA topology",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := NewCPUSetPartitionViewOptionsWithState(
				nil, dynamicConf, topology, CPUSetPartitionViewState{State: tt.state}, true)
			if opts.HardPartitionTargetError == nil ||
				!strings.Contains(opts.HardPartitionTargetError.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", opts.HardPartitionTargetError, tt.want)
			}
		})
	}
}

func TestNewCPUSetPartitionViewOptionsHandlesEligibleCapacityCornerCases(t *testing.T) {
	t.Parallel()

	topology := testTwoNUMASMTTopology()
	dynamicConf := dynamicconfig.NewDynamicAgentConfiguration().GetDynamicConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.EnableRampUpReclaimHardPartition = true
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.InitialRampUpReclaimCPUSetRatio = 0.5

	t.Run("less than one complete core resolves to zero", func(t *testing.T) {
		t.Parallel()
		state := cpustate.NewCPUPluginState(topology)
		machineState := state.GetMachineState()
		machineState[0].DefaultCPUSet = machine.NewCPUSet(0)
		machineState[0].AllocatedCPUSet = machine.NewCPUSet(1, 2, 3)
		state.SetMachineState(machineState)

		opts := NewCPUSetPartitionViewOptionsWithState(
			nil, dynamicConf, topology, CPUSetPartitionViewState{State: state}, true)
		if opts.HardPartitionTargetError != nil {
			t.Fatalf("NewCPUSetPartitionViewOptions() error = %v", opts.HardPartitionTargetError)
		}
		if got := opts.HardPartitionReclaimTargetPerNUMA[0]; got != 0 {
			t.Fatalf("NUMA 0 target = %d, want 0 when only one SMT sibling is eligible", got)
		}
		if got := opts.HardPartitionReclaimTargetPerNUMA[1]; got != 2 {
			t.Fatalf("NUMA 1 target = %d, want one complete physical core", got)
		}
	})

	t.Run("reserved CPUs are deducted only when eligible", func(t *testing.T) {
		t.Parallel()
		state := cpustate.NewCPUPluginState(topology)
		opts := NewCPUSetPartitionViewOptionsWithState(
			nil, dynamicConf, topology, CPUSetPartitionViewState{
				State:        state,
				ReservedCPUs: machine.NewCPUSet(0, 4, 99),
			}, true)
		if opts.HardPartitionTargetError != nil {
			t.Fatalf("NewCPUSetPartitionViewOptions() error = %v", opts.HardPartitionTargetError)
		}
		if got := opts.HardPartitionReclaimTargetPerNUMA[0]; got != 2 {
			t.Fatalf("NUMA 0 target = %d, want 2 after deducting one eligible sibling", got)
		}
		if got := opts.HardPartitionReclaimTargetPerNUMA[1]; got != 2 {
			t.Fatalf("NUMA 1 target = %d, want 2 after deducting one eligible sibling", got)
		}
	})

	t.Run("cross NUMA machine state is rejected", func(t *testing.T) {
		t.Parallel()
		state := cpustate.NewCPUPluginState(topology)
		machineState := state.GetMachineState()
		machineState[0].DefaultCPUSet.Add(4)
		state.SetMachineState(machineState)

		opts := NewCPUSetPartitionViewOptionsWithState(
			nil, dynamicConf, topology, CPUSetPartitionViewState{State: state}, true)
		if opts.HardPartitionTargetError == nil ||
			!strings.Contains(opts.HardPartitionTargetError.Error(), "outside NUMA topology: 4") {
			t.Fatalf("error = %v, want cross-NUMA machine-state error", opts.HardPartitionTargetError)
		}
	})
}

func TestNewCPUSetPartitionViewOptionsDoesNotRequireStateWhenHardPartitionInactive(t *testing.T) {
	t.Parallel()

	topology := testTwoNUMATopology()
	dynamicConf := dynamicconfig.NewDynamicAgentConfiguration().GetDynamicConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.EnableRampUpReclaimHardPartition = true

	opts := NewCPUSetPartitionViewOptionsWithState(
		nil, dynamicConf, topology, CPUSetPartitionViewState{}, false)
	if opts.HardPartitionTargetError != nil {
		t.Fatalf("inactive hard partition unexpectedly resolved state: %v", opts.HardPartitionTargetError)
	}
	if opts.HardPartitionEnabled {
		t.Fatal("hard partition must stay inactive")
	}
}

func TestNewCPUSetPartitionViewOptionsRequiresTopologyWhenHardPartitionActive(t *testing.T) {
	t.Parallel()

	dynamicConf := dynamicconfig.NewDynamicAgentConfiguration().GetDynamicConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.EnableRampUpReclaimHardPartition = true

	opts := NewCPUSetPartitionViewOptionsWithState(
		nil, dynamicConf, nil, CPUSetPartitionViewState{
			State: cpustate.NewCPUPluginState(nil),
		}, true)
	if opts.HardPartitionTargetError == nil ||
		!strings.Contains(opts.HardPartitionTargetError.Error(), "missing topology") {
		t.Fatalf("error = %v, want missing-topology error", opts.HardPartitionTargetError)
	}
}

func TestBuildValidatedCPUSetPartitionViewRejectsNilStateForHardPartition(t *testing.T) {
	t.Parallel()

	_, err := BuildValidatedCPUSetPartitionView(nil, testTwoNUMATopology(), CPUSetPartitionViewOptions{
		HardPartitionEnabled:              true,
		HardPartitionReclaimTargetPerNUMA: map[int]int{0: 2, 1: 2},
	})
	if err == nil || !strings.Contains(err.Error(), "missing state") {
		t.Fatalf("error = %v, want missing-state error", err)
	}
}

func TestValidateCPUSetPartitionViewRequiresExactNUMABucketProjection(t *testing.T) {
	t.Parallel()

	valid := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		NonReclaimPool:          machine.NewCPUSet(0, 1, 2, 3, 4),
		ReclaimEffective:        machine.NewCPUSet(5, 6, 7),
		DesiredReclaimEffective: machine.NewCPUSet(5, 6, 7),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(),
			1: machine.NewCPUSet(5, 6, 7),
		},
		DesiredReclaimEffectivePerNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(),
			1: machine.NewCPUSet(5, 6, 7),
		},
	}}
	if err := ValidateCPUSetPartitionView(valid, testTwoNUMATopology()); err != nil {
		t.Fatalf("exact projection with empty NUMA bucket rejected: %v", err)
	}

	invalid := valid.DeepCopy()
	invalid.ReclaimEffectivePerNUMA[0] = machine.NewCPUSet(0)
	invalid.ReclaimEffectivePerNUMA[1] = machine.NewCPUSet(6, 7)
	if err := ValidateCPUSetPartitionView(invalid, testTwoNUMATopology()); err == nil ||
		!strings.Contains(err.Error(), "reclaim effective NUMA bucket 0") {
		t.Fatalf("ValidateCPUSetPartitionView() error = %v, want exact NUMA projection error", err)
	}
}

func TestValidateCPUSetPartitionViewRejectsCPUsOutsideTopology(t *testing.T) {
	t.Parallel()

	view := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		NonReclaimPool:          machine.NewCPUSet(0, 1, 8),
		ReclaimEffective:        machine.NewCPUSet(2, 3),
		DesiredReclaimEffective: machine.NewCPUSet(2, 3),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(2, 3),
			1: machine.NewCPUSet(),
		},
		DesiredReclaimEffectivePerNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(2, 3),
			1: machine.NewCPUSet(),
		},
	}}

	err := ValidateCPUSetPartitionView(view, testTwoNUMATopology())
	if err == nil || !strings.Contains(err.Error(), "outside machine topology") {
		t.Fatalf("ValidateCPUSetPartitionView() error = %v, want outside-topology error", err)
	}
}

func TestBuildCPUSetPartitionViewFromTargetOwnsVerifiedReclaim(t *testing.T) {
	t.Parallel()

	desired := model.NewDesiredView()
	desired.Reserve = machine.NewCPUSet(0)
	desired.Dedicated = machine.NewCPUSet(1)
	desired.SharePool = machine.NewCPUSet(2)
	desired.ReclaimRaw = machine.NewCPUSet(3, 4, 5)
	desired.ContainerCPUSetByPod["pod"] = map[string]machine.CPUSet{
		"container": machine.NewCPUSet(1),
	}
	topology := &machine.CPUTopology{CPUDetails: machine.CPUDetails{
		0: {NUMANodeID: 0},
		1: {NUMANodeID: 0},
		2: {NUMANodeID: 0},
		3: {NUMANodeID: 0},
		4: {NUMANodeID: 1},
		5: {NUMANodeID: 1},
	}}
	target := machine.NewCPUSet(3, 5)

	got := BuildCPUSetPartitionViewFromTarget(desired, target, topology)

	if got == nil {
		t.Fatal("BuildCPUSetPartitionViewFromTarget() returned nil")
	}
	if !got.ReclaimEffective.Equals(target) {
		t.Fatalf("reclaim effective = %s, want %s", got.ReclaimEffective.String(), target.String())
	}
	if !got.NonReclaimPool.Equals(machine.NewCPUSet(1, 2, 4)) {
		t.Fatalf("non-reclaim pool = %s, want 1-2,4", got.NonReclaimPool.String())
	}
	if !got.ReclaimEffectivePerNUMA[0].Equals(machine.NewCPUSet(3)) ||
		!got.ReclaimEffectivePerNUMA[1].Equals(machine.NewCPUSet(5)) {
		t.Fatalf("reclaim per NUMA = %#v, want numa0=3 numa1=5", got.ReclaimEffectivePerNUMA)
	}

	target.Add(99)
	desired.ContainerCPUSetByPod["pod"]["container"].Add(98)
	if got.ReclaimEffective.Contains(99) || got.ContainerCPUSetByPod["pod"]["container"].Contains(98) {
		t.Fatal("returned view aliases caller-owned target or desired view")
	}
}

func TestBuildCPUSetPartitionViewAndDeepCopy(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllowSharedCoresOverlapReclaimedCores(true)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameShare, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
		AllocationResult: machine.NewCPUSet(1, 2, 3),
	})
	state.SetAllocationInfo("share-NUMA0", commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta("share-NUMA0"),
		AllocationResult: machine.NewCPUSet(6),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(2, 3),
	})
	state.SetAllocationInfo("isolation-0", commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta("isolation-0"),
		AllocationResult: machine.NewCPUSet(4),
	})
	state.SetAllocationInfo("pod-1", "main", &cpustate.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "pod-1",
			ContainerName: "main",
			OwnerPoolName: commonstate.PoolNameDedicated,
			QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		AllocationResult: machine.NewCPUSet(5),
	})

	view := BuildCPUSetPartitionView(state, &machine.CPUTopology{CPUDetails: machine.CPUDetails{
		0: {NUMANodeID: 0}, 1: {NUMANodeID: 0}, 2: {NUMANodeID: 1}, 3: {NUMANodeID: 1}, 4: {NUMANodeID: 1}, 5: {NUMANodeID: 1},
	}}, CPUSetPartitionViewOptions{})

	assertCPUSet(t, "reserve", view.Reserve, "0")
	assertCPUSet(t, "share", view.SharePool, "1,6")
	assertCPUSet(t, "share map default", view.SharePoolMap[commonstate.PoolNameShare], "1")
	assertCPUSet(t, "share map numa", view.SharePoolMap["share-NUMA0"], "6")
	assertCPUSet(t, "reclaim raw", view.ReclaimRaw, "2-3")
	assertCPUSet(t, "dedicated", view.Dedicated, "5")
	assertCPUSet(t, "non reclaim", view.NonReclaimPool, "1,4-6")
	assertCPUSet(t, "reclaim effective", view.ReclaimEffective, "2-3")
	assertCPUSet(t, "reclaim numa 1", view.ReclaimEffectivePerNUMA[1], "2-3")
	assertCPUSet(t, "container", view.ContainerCPUSetByPod["pod-1"]["main"], "5")

	copied := view.DeepCopy()
	assertCPUSet(t, "copied reserve", copied.Reserve, "0")
	assertCPUSet(t, "copied share", copied.SharePool, "1,6")
	assertCPUSet(t, "copied share map default", copied.SharePoolMap[commonstate.PoolNameShare], "1")
	assertCPUSet(t, "copied share map numa", copied.SharePoolMap["share-NUMA0"], "6")
	assertCPUSet(t, "copied reclaim raw", copied.ReclaimRaw, "2-3")
	assertCPUSet(t, "copied dedicated", copied.Dedicated, "5")
	assertCPUSet(t, "copied non reclaim", copied.NonReclaimPool, "1,4-6")
	assertCPUSet(t, "copied reclaim effective", copied.ReclaimEffective, "2-3")
	assertCPUSet(t, "copied reclaim numa 1", copied.ReclaimEffectivePerNUMA[1], "2-3")
	assertCPUSet(t, "copied container", copied.ContainerCPUSetByPod["pod-1"]["main"], "5")
	copied.ContainerCPUSetByPod["pod-1"]["main"] = machine.NewCPUSet(6)
	assertCPUSet(t, "original container unchanged", view.ContainerCPUSetByPod["pod-1"]["main"], "5")
	copied.SharePoolMap["share-NUMA0"] = machine.NewCPUSet(7)
	assertCPUSet(t, "original share pool map unchanged", view.SharePoolMap["share-NUMA0"], "6")
}

func TestBuildCPUSetPartitionViewPoolOwners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		buildState func() cpustate.ReadonlyState
		wantOwners map[model.CPUSetPoolIdentity]model.DesiredPoolOwner
	}{
		{
			name: "records final pool owners and excludes non-reportable containers",
			buildState: func() cpustate.ReadonlyState {
				state := cpustate.NewCPUPluginState(nil)
				state.SetAllowSharedCoresOverlapReclaimedCores(false)
				for poolName, cpus := range map[string]machine.CPUSet{
					commonstate.PoolNameReserve: machine.NewCPUSet(15),
					commonstate.PoolNameReclaim: machine.NewCPUSet(6, 7),
					"rp-a/share-NUMA0":          machine.NewCPUSet(0, 1),
					"rp-b/share-NUMA1":          machine.NewCPUSet(8, 9),
					"isolation-final":           machine.NewCPUSet(4, 5, 10),
					"system-services":           machine.NewCPUSet(11),
				} {
					state.SetAllocationInfo(poolName, commonstate.FakedContainerName, &cpustate.AllocationInfo{
						AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(poolName),
						AllocationResult: cpus,
					})
				}

				state.SetAllocationInfo("snb-ramp-up", "main", &cpustate.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:        "snb-ramp-up",
						ContainerName: "main",
						OwnerPoolName: "isolation-transient",
						QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
						Annotations: map[string]string{
							apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
							apiconsts.PodAnnotationResourcePackageKey:           "rp-c",
							cpuconsts.CPUStateAnnotationKeyNUMAHint:             "0",
						},
					},
					RampUp:           true,
					AllocationResult: machine.NewCPUSet(6),
				})
				for containerName, cpus := range map[string]machine.CPUSet{
					"main":    machine.NewCPUSet(2),
					"sidecar": machine.NewCPUSet(3),
				} {
					state.SetAllocationInfo("dedicated-pod", containerName, &cpustate.AllocationInfo{
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:        "dedicated-pod",
							ContainerName: containerName,
							OwnerPoolName: commonstate.PoolNameDedicated,
							QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
						},
						AllocationResult: cpus,
					})
				}
				state.SetAllocationInfo("isolation-pod", "main", &cpustate.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:        "isolation-pod",
						ContainerName: "main",
						OwnerPoolName: "rp-a/isolation-workload",
						QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
					},
					AllocationResult: machine.NewCPUSet(4, 5),
				})
				state.SetAllocationInfo("malformed-isolation", "main", &cpustate.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						ContainerName: "main",
						OwnerPoolName: "rp-a/isolation-malformed",
						QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
					},
					AllocationResult: machine.NewCPUSet(10),
				})
				for podUID, allocation := range map[string]*cpustate.AllocationInfo{
					"reserve-container": {
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:        "reserve-container",
							ContainerName: "main",
							OwnerPoolName: commonstate.PoolNameReserve,
						},
						AllocationResult: machine.NewCPUSet(12),
					},
					"system-container": {
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:        "system-container",
							ContainerName: "main",
							OwnerPoolName: "system-services",
							QoSLevel:      apiconsts.PodAnnotationQoSLevelSystemCores,
						},
						AllocationResult: machine.NewCPUSet(11),
					},
					"reclaimed-container": {
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:        "reclaimed-container",
							ContainerName: "main",
							OwnerPoolName: commonstate.PoolNameReclaim,
							QoSLevel:      apiconsts.PodAnnotationQoSLevelReclaimedCores,
						},
						AllocationResult: machine.NewCPUSet(7),
					},
					"shared-container": {
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:        "shared-container",
							ContainerName: "main",
							OwnerPoolName: commonstate.PoolNameShare,
							QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
						},
						AllocationResult: machine.NewCPUSet(13),
					},
				} {
					state.SetAllocationInfo(podUID, "main", allocation)
				}
				state.SetAllocationInfo("nil-entry", "main", nil)
				return state
			},
			wantOwners: map[model.CPUSetPoolIdentity]model.DesiredPoolOwner{
				{Kind: model.CPUSetPoolKindReclaim}: {
					ExpectedCPUSet: machine.NewCPUSet(7),
				},
				{Kind: model.CPUSetPoolKindShare, Name: "rp-a/share-NUMA0"}: {
					ExpectedCPUSet: machine.NewCPUSet(0, 1),
				},
				{Kind: model.CPUSetPoolKindShare, Name: "rp-b/share-NUMA1"}: {
					ExpectedCPUSet: machine.NewCPUSet(8, 9),
				},
				{Kind: model.CPUSetPoolKindShare, Name: "rp-c/share-NUMA0"}: {
					ExpectedCPUSet: machine.NewCPUSet(6),
				},
				{Kind: model.CPUSetPoolKindDedicated, PodUID: "dedicated-pod"}: {
					ExpectedCPUSet: machine.NewCPUSet(2, 3),
					ContainerCPUSetByName: map[string]machine.CPUSet{
						"main":    machine.NewCPUSet(2),
						"sidecar": machine.NewCPUSet(3),
					},
				},
				{Kind: model.CPUSetPoolKindIsolation, PodUID: "isolation-pod"}: {
					ExpectedCPUSet: machine.NewCPUSet(4, 5),
					ContainerCPUSetByName: map[string]machine.CPUSet{
						"main": machine.NewCPUSet(4, 5),
					},
				},
				{Kind: model.CPUSetPoolKindIsolation}: {
					ExpectedCPUSet: machine.NewCPUSet(10),
					ContainerCPUSetByName: map[string]machine.CPUSet{
						"main": machine.NewCPUSet(10),
					},
				},
			},
		},
		{
			name: "shared owner uses final overlap-filtered share pool map",
			buildState: func() cpustate.ReadonlyState {
				state := cpustate.NewCPUPluginState(nil)
				state.SetAllowSharedCoresOverlapReclaimedCores(true)
				state.SetAllocationInfo("share-NUMA0", commonstate.FakedContainerName, &cpustate.AllocationInfo{
					AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta("share-NUMA0"),
					AllocationResult: machine.NewCPUSet(1),
				})
				state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
					AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
					AllocationResult: machine.NewCPUSet(2),
				})
				state.SetAllocationInfo("snb-ramp-up", "main", &cpustate.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:        "snb-ramp-up",
						ContainerName: "main",
						QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
						Annotations: map[string]string{
							apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
							cpuconsts.CPUStateAnnotationKeyNUMAHint:             "0",
						},
					},
					RampUp:           true,
					AllocationResult: machine.NewCPUSet(2, 3),
				})
				return state
			},
			wantOwners: map[model.CPUSetPoolIdentity]model.DesiredPoolOwner{
				{Kind: model.CPUSetPoolKindReclaim}: {
					ExpectedCPUSet: machine.NewCPUSet(2),
				},
				{Kind: model.CPUSetPoolKindShare, Name: "share-NUMA0"}: {
					ExpectedCPUSet: machine.NewCPUSet(1, 3),
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			view := BuildCPUSetPartitionView(tt.buildState(), testTwoNUMATopologyN(8), CPUSetPartitionViewOptions{})
			if len(view.PoolOwners) != len(tt.wantOwners) {
				t.Fatalf("PoolOwners count = %d, want %d: %#v", len(view.PoolOwners), len(tt.wantOwners), view.PoolOwners)
			}
			for identity, want := range tt.wantOwners {
				got, ok := view.PoolOwners[identity]
				if !ok {
					t.Errorf("PoolOwners missing identity %+v", identity)
					continue
				}
				if !got.ExpectedCPUSet.Equals(want.ExpectedCPUSet) {
					t.Errorf("PoolOwners[%+v].ExpectedCPUSet = %s, want %s",
						identity, got.ExpectedCPUSet.String(), want.ExpectedCPUSet.String())
				}
				if len(got.ContainerCPUSetByName) != len(want.ContainerCPUSetByName) {
					t.Errorf("PoolOwners[%+v] container count = %d, want %d",
						identity, len(got.ContainerCPUSetByName), len(want.ContainerCPUSetByName))
					continue
				}
				for containerName, wantCPUSet := range want.ContainerCPUSetByName {
					if gotCPUSet, ok := got.ContainerCPUSetByName[containerName]; !ok || !gotCPUSet.Equals(wantCPUSet) {
						t.Errorf("PoolOwners[%+v].ContainerCPUSetByName[%q] = %s, want %s",
							identity, containerName, gotCPUSet.String(), wantCPUSet.String())
					}
				}
			}
		})
	}
}

func TestBuildCPUSetPartitionViewIncludesSNBRampUpInSharePool(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllowSharedCoresOverlapReclaimedCores(false)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameShare, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
		AllocationResult: machine.NewCPUSet(1),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(2, 3),
	})
	state.SetAllocationInfo("snb-pod", "main", &cpustate.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "snb-pod",
			ContainerName: "main",
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			},
		},
		RampUp:           true,
		AllocationResult: machine.NewCPUSet(2),
	})

	view := BuildCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{})

	assertCPUSet(t, "snb container", view.ContainerCPUSetByPod["snb-pod"]["main"], "2")
	assertCPUSet(t, "share includes snb ramp-up", view.SharePool, "1-2")
	assertCPUSet(t, "non reclaim protects snb ramp-up", view.NonReclaimPool, "1-2,4-7")
	assertCPUSet(t, "reclaim excludes snb ramp-up", view.ReclaimEffective, "3")
}

func TestGateAHardPartitionDefaultShareBaselineDoesNotSwallowReclaim(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllowSharedCoresOverlapReclaimedCores(false)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0, 4),
	})
	state.SetAllocationInfo(commonstate.PoolNameShare, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
		AllocationResult: machine.NewCPUSet(1, 2, 3, 5, 6, 7),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(2, 3, 6, 7),
	})

	view, err := BuildValidatedCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{
		HardPartitionEnabled:              true,
		HardPartitionReclaimTargetPerNUMA: map[int]int{0: 2, 1: 2},
	})
	if err != nil {
		t.Fatalf("BuildValidatedCPUSetPartitionView() error = %v", err)
	}

	assertCPUSet(t, "default share residual", view.SharePool, "1,5")
	assertCPUSet(t, "default share map residual", view.SharePoolMap[commonstate.PoolNameShare], "1,5")
	assertCPUSet(t, "non reclaim residual", view.NonReclaimPool, "1,5")
	assertCPUSet(t, "reclaim effective", view.ReclaimEffective, "2-3,6-7")
	assertCPUSet(t, "reclaim numa 0", view.ReclaimEffectivePerNUMA[0], "2-3")
	assertCPUSet(t, "reclaim numa 1", view.ReclaimEffectivePerNUMA[1], "6-7")
}

func TestGateAHardPartitionDefaultShareBaselineExcludesFixedOwners(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllowSharedCoresOverlapReclaimedCores(false)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameShare, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
		AllocationResult: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(2, 3, 6, 7),
	})
	state.SetAllocationInfo("isolation-0", commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta("isolation-0"),
		AllocationResult: machine.NewCPUSet(4),
	})
	state.SetAllocationInfo("dedicated-pod", "main", &cpustate.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "dedicated-pod",
			ContainerName: "main",
			OwnerPoolName: commonstate.PoolNameDedicated,
			QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		AllocationResult: machine.NewCPUSet(5),
	})

	view, err := BuildValidatedCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{
		HardPartitionEnabled:              true,
		HardPartitionReclaimTargetPerNUMA: map[int]int{0: 2, 1: 2},
	})
	if err != nil {
		t.Fatalf("BuildValidatedCPUSetPartitionView() error = %v", err)
	}

	assertCPUSet(t, "default share excludes fixed owners", view.SharePool, "1")
	assertCPUSet(t, "default share map excludes fixed owners", view.SharePoolMap[commonstate.PoolNameShare], "1")
	assertCPUSet(t, "dedicated remains dedicated", view.Dedicated, "5")
	assertCPUSet(t, "isolation remains isolated", view.Isolation, "4")
	assertCPUSet(t, "reclaim effective", view.ReclaimEffective, "2-3,6-7")
}

func TestGateBSNBRampUpUsesDeclaredShareNUMAPoolForRDT(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllowSharedCoresOverlapReclaimedCores(false)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0, 4),
	})
	state.SetAllocationInfo(commonstate.PoolNameShare, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
		AllocationResult: machine.NewCPUSet(1, 5),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(2, 3, 6, 7),
	})
	state.SetAllocationInfo("snb-ramp-up", "main", &cpustate.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "snb-ramp-up",
			ContainerName: "main",
			OwnerPoolName: "isolation-snb-ramp-up",
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				cpuconsts.CPUStateAnnotationKeyNUMAHint:             "0",
			},
		},
		RampUp:           true,
		AllocationResult: machine.NewCPUSet(2),
	})

	view := BuildCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{
		HardPartitionEnabled: true,
	})

	assertCPUSet(t, "aggregate share protects SNB ramp-up", view.SharePool, "1-2,5")
	assertCPUSet(t, "default share RDT target", view.SharePoolMap[commonstate.PoolNameShare], "1,5")
	assertCPUSet(t, "declared share-NUMA0 RDT target", view.SharePoolMap["share-NUMA0"], "2")
	if _, ok := view.SharePoolMap["isolation-snb-ramp-up"]; ok {
		t.Fatal("SharePoolMap contains isolation owner instead of declared share-NUMA0 pool")
	}
}

func TestBuildCPUSetPartitionViewPreservesTwoReservedCPUs(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0, 24),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(1, 25),
	})
	view := BuildCPUSetPartitionView(state, &machine.CPUTopology{CPUDetails: machine.CPUDetails{
		0: {NUMANodeID: 0}, 1: {NUMANodeID: 0}, 24: {NUMANodeID: 1}, 25: {NUMANodeID: 1},
	}}, CPUSetPartitionViewOptions{})

	assertCPUSet(t, "reserve", view.Reserve, "0,24")
	if got := view.Reserve.Size(); got != 2 {
		t.Fatalf("reserve size = %d, want 2", got)
	}
}

func TestBuildCPUSetPartitionViewNilInputs(t *testing.T) {
	t.Parallel()

	view := BuildCPUSetPartitionView(nil, nil, CPUSetPartitionViewOptions{})
	if view == nil || !view.ReclaimEffective.IsEmpty() || len(view.ContainerCPUSetByPod) != 0 {
		t.Fatalf("unexpected nil input view: %#v", view)
	}
	if view.DeepCopy() == nil {
		t.Fatalf("DeepCopy of non-nil empty view returned nil")
	}
	if (*model.DesiredView)(nil).DeepCopy() != nil {
		t.Fatalf("DeepCopy of nil view should be nil")
	}
}

func TestBuildCPUSetPartitionViewKeepsDenseReclaimPerNUMAKeys(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(5, 6),
	})

	view := BuildCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{})

	assertCPUSet(t, "desired reclaim numa 0 empty", view.DesiredReclaimEffectivePerNUMA[0], "")
	assertCPUSet(t, "desired reclaim numa 1", view.DesiredReclaimEffectivePerNUMA[1], "5-6")
	assertCPUSet(t, "effective reclaim numa 0 empty", view.ReclaimEffectivePerNUMA[0], "")
	assertCPUSet(t, "effective reclaim numa 1", view.ReclaimEffectivePerNUMA[1], "5-6")
	if _, ok := view.ReclaimEffectivePerNUMA[0]; !ok {
		t.Fatalf("empty NUMA 0 reclaim bucket must be preserved")
	}
	if _, ok := view.DesiredReclaimEffectivePerNUMA[0]; !ok {
		t.Fatalf("empty NUMA 0 desired reclaim bucket must be preserved")
	}
}

func TestBuildCPUSetPartitionViewPadsNonReclaimPoolToMinSize(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameShare, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
		AllocationResult: machine.NewCPUSet(2),
	})

	view := BuildCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{
		NonReclaimPoolMinSize: 4,
	})

	assertCPUSet(t, "reserve", view.Reserve, "0")
	assertCPUSet(t, "non reclaim absorbs CPUs without raw reclaim", view.NonReclaimPool, "1-7")
	assertCPUSet(t, "reclaim effective is empty without raw reclaim", view.ReclaimEffective, "")
	assertCPUSet(t, "empty reclaim numa 0", view.ReclaimEffectivePerNUMA[0], "")
	assertCPUSet(t, "empty reclaim numa 1", view.ReclaimEffectivePerNUMA[1], "")
}

func TestBuildCPUSetPartitionViewAppliesTransientProtectedNonReclaim(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameShare, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
		AllocationResult: machine.NewCPUSet(1, 2),
	})

	view := BuildCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{
		TransientProtectedNonReclaim: machine.NewCPUSet(3, 4),
	})

	assertCPUSet(t, "desired non reclaim", view.DesiredNonReclaimPool, "1-7")
	assertCPUSet(t, "desired reclaim", view.DesiredReclaimEffective, "")
	assertCPUSet(t, "protected non reclaim", view.TransientProtectedNonReclaim, "3-4")
	assertCPUSet(t, "applied non reclaim", view.NonReclaimPool, "1-7")
	assertCPUSet(t, "applied reclaim", view.ReclaimEffective, "")
	if !view.NonReclaimPool.Intersection(view.ReclaimEffective).IsEmpty() {
		t.Fatalf("applied non reclaim and reclaim overlap: non=%s reclaim=%s", view.NonReclaimPool.String(), view.ReclaimEffective.String())
	}
	assertCPUSet(t, "empty reclaim numa 0", view.ReclaimEffectivePerNUMA[0], "")
	assertCPUSet(t, "empty reclaim numa 1", view.ReclaimEffectivePerNUMA[1], "")
}

func TestApplyTransientProtectedNonReclaimRebuildsReclaimPerNUMA(t *testing.T) {
	t.Parallel()

	view := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		Reserve:                        machine.NewCPUSet(),
		DesiredNonReclaimPool:          machine.NewCPUSet(1, 2),
		DesiredReclaimEffective:        machine.NewCPUSet(3, 4, 5, 6, 7),
		NonReclaimPool:                 machine.NewCPUSet(1, 2),
		ReclaimEffective:               machine.NewCPUSet(3, 4, 5, 6, 7),
		ReclaimEffectivePerNUMA:        map[int]machine.CPUSet{0: machine.NewCPUSet(3), 1: machine.NewCPUSet(4, 5, 6, 7)},
		DesiredReclaimEffectivePerNUMA: map[int]machine.CPUSet{0: machine.NewCPUSet(3), 1: machine.NewCPUSet(4, 5, 6, 7)},
	}}

	ApplyTransientProtectedNonReclaim(view, testTwoNUMATopology(), machine.NewCPUSet(3, 4))

	assertCPUSet(t, "applied reclaim", view.ReclaimEffective, "5-7")
	assertCPUSet(t, "applied reclaim numa 0 empty", view.ReclaimEffectivePerNUMA[0], "")
	assertCPUSet(t, "applied reclaim numa 1", view.ReclaimEffectivePerNUMA[1], "5-7")
	err := ValidateCPUSetPartitionView(view, testTwoNUMATopology())
	if err != nil {
		t.Fatalf("ValidateCPUSetPartitionView() rejected exact projection with empty NUMA bucket: %v", err)
	}
}

func TestBuildCPUSetPartitionViewPadsNonReclaimPoolReversely(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})

	view := BuildCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{
		NonReclaimPoolMinSize: 4,
		ReserveCPUReversely:   true,
	})

	assertCPUSet(t, "reverse non reclaim absorbs CPUs without raw reclaim", view.NonReclaimPool, "1-7")
	assertCPUSet(t, "reverse reclaim effective is empty without raw reclaim", view.ReclaimEffective, "")
	assertCPUSet(t, "reverse empty reclaim numa 0", view.ReclaimEffectivePerNUMA[0], "")
	assertCPUSet(t, "reverse empty reclaim numa 1", view.ReclaimEffectivePerNUMA[1], "")
}

func TestBuildCPUSetPartitionViewDoesNotPadWhenOverlapAllowed(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllowSharedCoresOverlapReclaimedCores(true)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(1, 2, 3),
	})

	view := BuildCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{
		NonReclaimPoolMinSize: 4,
	})

	assertCPUSet(t, "non reclaim remains empty", view.NonReclaimPool, "")
	assertCPUSet(t, "reclaim effective remains raw", view.ReclaimEffective, "1-3")
}

func TestBuildCPUSetPartitionViewBoundsReclaimEffectiveByReclaimRaw(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(3, 4),
	})

	view := BuildCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{})

	assertCPUSet(t, "reclaim effective is bounded by raw", view.ReclaimEffective, "3-4")
	assertCPUSet(t, "non reclaim absorbs CPUs outside raw", view.NonReclaimPool, "1-2,5-7")
	if excess := view.ReclaimEffective.Difference(view.ReclaimRaw); !excess.IsEmpty() {
		t.Fatalf("reclaim effective exceeds reclaim raw: effective=%s raw=%s excess=%s",
			view.ReclaimEffective.String(), view.ReclaimRaw.String(), excess.String())
	}
}

func TestBuildCPUSetPartitionViewCapsPaddingToCandidates(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0, 1, 2, 3, 4, 5),
	})

	view := BuildCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{
		NonReclaimPoolMinSize: 4,
	})

	assertCPUSet(t, "non reclaim capped to candidates", view.NonReclaimPool, "6-7")
	assertCPUSet(t, "reclaim effective exhausted", view.ReclaimEffective, "")
}

func TestBuildCPUSetPartitionViewValidatesHardPartitionDistribution(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                 string
		reclaim              machine.CPUSet
		hardPartitionEnabled bool
		targetPerNUMA        map[int]int
		wantErr              string
	}{
		{
			name:                 "balanced two per NUMA",
			reclaim:              machine.NewCPUSet(0, 1, 4, 5),
			hardPartitionEnabled: true,
			targetPerNUMA:        map[int]int{0: 2, 1: 2},
		},
		{
			name:                 "asymmetric reclaim above immutable targets",
			reclaim:              machine.NewCPUSet(0, 1, 2, 3, 4, 5),
			hardPartitionEnabled: true,
			targetPerNUMA:        map[int]int{0: 2, 1: 2},
		},
		{
			name:                 "hard partition rejects four and zero",
			reclaim:              machine.NewCPUSet(0, 1, 2, 3),
			hardPartitionEnabled: true,
			targetPerNUMA:        map[int]int{0: 2, 1: 2},
			wantErr:              "NUMA 1 has 0 CPUs, target is 2",
		},
		{
			name:                 "hard partition rejects reclaim below configured target",
			reclaim:              machine.NewCPUSet(0, 1, 2, 4, 5),
			hardPartitionEnabled: true,
			targetPerNUMA:        map[int]int{0: 3, 1: 3},
			wantErr:              "NUMA 1 has 2 CPUs, target is 3",
		},
		{
			name:                 "disabled skips validation",
			reclaim:              machine.NewCPUSet(0, 1, 2, 3),
			hardPartitionEnabled: false,
			targetPerNUMA:        map[int]int{0: 2, 1: 2},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			state := cpustate.NewCPUPluginState(nil)
			state.SetAllowSharedCoresOverlapReclaimedCores(true)
			state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: tc.reclaim,
			})

			view, err := BuildValidatedCPUSetPartitionView(state, testTwoNUMATopology(), CPUSetPartitionViewOptions{
				HardPartitionEnabled:              tc.hardPartitionEnabled,
				HardPartitionReclaimTargetPerNUMA: tc.targetPerNUMA,
			})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("BuildValidatedCPUSetPartitionView() error = %v", err)
				}
				if view == nil {
					t.Fatal("BuildValidatedCPUSetPartitionView() returned nil view")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("BuildValidatedCPUSetPartitionView() error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestBuildValidatedCPUSetPartitionViewHardPartitionPreservesLegalReclaim(t *testing.T) {
	t.Parallel()

	const perNUMA = 96
	topology := testTwoNUMATopologyN(perNUMA)

	// NUMA0: cpus 0..32 (33 = 28 floor + 5 raw slack); NUMA1: cpus 96..123 (28 floor).
	reclaimCPUs := make([]int, 0, 61)
	for cpu := 0; cpu <= 32; cpu++ {
		reclaimCPUs = append(reclaimCPUs, cpu)
	}
	for cpu := perNUMA; cpu <= perNUMA+27; cpu++ {
		reclaimCPUs = append(reclaimCPUs, cpu)
	}
	reclaim := machine.NewCPUSet(reclaimCPUs...)

	newReclaimState := func() cpustate.ReadonlyState {
		state := cpustate.NewCPUPluginState(nil)
		// Production hard-partition path runs with advisor overlap disabled.
		state.SetAllowSharedCoresOverlapReclaimedCores(false)
		state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
			AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
			AllocationResult: reclaim,
		})
		return state
	}

	// Bulkhead projects the already materialized partition. It must not trim
	// legal reclaim merely because the per-NUMA quantities are asymmetric.
	view, err := BuildValidatedCPUSetPartitionView(newReclaimState(), topology, CPUSetPartitionViewOptions{
		HardPartitionEnabled:              true,
		HardPartitionReclaimTargetPerNUMA: map[int]int{0: 28, 1: 28},
	})
	if err != nil {
		t.Fatalf("BuildValidatedCPUSetPartitionView(hard=true) error = %v, want legal 33/28 projection", err)
	}
	if view == nil {
		t.Fatal("BuildValidatedCPUSetPartitionView(hard=true) returned nil view")
	}
	numa0 := view.ReclaimEffectivePerNUMA[0].Size()
	numa1 := view.ReclaimEffectivePerNUMA[1].Size()
	if numa0 != 33 || numa1 != 28 {
		t.Fatalf("hard=true reclaim effective per NUMA = %d/%d, want unchanged 33/28", numa0, numa1)
	}
	// Projection must preserve the existing disjoint partition.
	if overlap := view.NonReclaimPool.Intersection(view.ReclaimEffective); !overlap.IsEmpty() {
		t.Fatalf("hard=true produced non-reclaim/reclaim overlap: %s", overlap.String())
	}
	if desired0 := view.DesiredReclaimEffectivePerNUMA[0].Size(); desired0 != 33 {
		t.Fatalf("hard=true desired reclaim NUMA0 = %d, want 33", desired0)
	}

	// hard=false: behavior unchanged, raw slack shape (33/28) preserved.
	softView := BuildCPUSetPartitionView(newReclaimState(), topology, CPUSetPartitionViewOptions{
		HardPartitionEnabled: false,
	})
	if softView == nil {
		t.Fatal("BuildCPUSetPartitionView(hard=false) returned nil view")
	}
	if got0, got1 := softView.ReclaimEffectivePerNUMA[0].Size(), softView.ReclaimEffectivePerNUMA[1].Size(); got0 != 33 || got1 != 28 {
		t.Fatalf("hard=false reclaim effective per NUMA = %d/%d, want unchanged 33/28", got0, got1)
	}
}

func TestBuildValidatedCPUSetPartitionViewSkipsSteadyExclusiveNUMA(t *testing.T) {
	t.Parallel()

	const perNUMA = 32
	topology := testTwoNUMATopologyN(perNUMA)

	// NUMA0 is wholly owned by a committed steady (RampUp=false) non-reclaim
	// exclusive DNB: 30 dedicated CPUs leave only a 2-CPU steady reclaim reserve
	// ({0,1}). NUMA1 carries the ratio-derived ramp-up reclaim (6 CPUs). The
	// per-NUMA hard-partition target is 6 for both NUMAs; without the carve-out
	// NUMA0 fails admission closed (2 < 6) for every other ramp-up QoS.
	dedicatedNUMA0 := make([]int, 0, perNUMA-2)
	for cpu := 2; cpu < perNUMA; cpu++ {
		dedicatedNUMA0 = append(dedicatedNUMA0, cpu)
	}
	reclaimCPUs := []int{0, 1}
	for cpu := perNUMA; cpu < perNUMA+6; cpu++ {
		reclaimCPUs = append(reclaimCPUs, cpu)
	}

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllowSharedCoresOverlapReclaimedCores(false)
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(reclaimCPUs...),
	})
	state.SetAllocationInfo("steady-exclusive-dnb", "main", &cpustate.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "steady-exclusive-dnb",
			ContainerName: "main",
			OwnerPoolName: commonstate.PoolNameDedicated,
			QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
			},
		},
		RampUp:                   false,
		AllocationResult:         machine.NewCPUSet(dedicatedNUMA0...),
		TopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(dedicatedNUMA0...)},
	})

	view, err := BuildValidatedCPUSetPartitionView(state, topology, CPUSetPartitionViewOptions{
		HardPartitionEnabled:              true,
		HardPartitionReclaimTargetPerNUMA: map[int]int{0: 6, 1: 6},
	})
	if err != nil {
		t.Fatalf("BuildValidatedCPUSetPartitionView() error = %v, want steady exclusive NUMA0 skipped", err)
	}
	if view == nil {
		t.Fatal("BuildValidatedCPUSetPartitionView() returned nil view")
	}
	if got := view.ReclaimEffectivePerNUMA[0].Size(); got != 2 {
		t.Fatalf("steady exclusive NUMA0 reclaim effective = %d, want unchanged 2", got)
	}
	if got := view.ReclaimEffectivePerNUMA[1].Size(); got != 6 {
		t.Fatalf("ramp-up NUMA1 reclaim effective = %d, want 6", got)
	}
}

func testTwoNUMATopologyN(perNUMA int) *machine.CPUTopology {
	details := machine.CPUDetails{}
	for cpu := 0; cpu < perNUMA; cpu++ {
		details[cpu] = machine.CPUTopoInfo{NUMANodeID: 0, SocketID: 0, CoreID: cpu}
	}
	for i := 0; i < perNUMA; i++ {
		cpu := perNUMA + i
		details[cpu] = machine.CPUTopoInfo{NUMANodeID: 1, SocketID: 1, CoreID: cpu}
	}
	return &machine.CPUTopology{
		NumCPUs:      2 * perNUMA,
		NumCores:     2 * perNUMA,
		NumSockets:   2,
		NumNUMANodes: 2,
		CPUDetails:   details,
	}
}

func testTwoNUMATopology() *machine.CPUTopology {
	return &machine.CPUTopology{
		NumCPUs:      8,
		NumCores:     8,
		NumSockets:   2,
		NumNUMANodes: 2,
		CPUDetails: machine.CPUDetails{
			0: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			1: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			2: {NUMANodeID: 0, SocketID: 0, CoreID: 2},
			3: {NUMANodeID: 0, SocketID: 0, CoreID: 3},
			4: {NUMANodeID: 1, SocketID: 1, CoreID: 4},
			5: {NUMANodeID: 1, SocketID: 1, CoreID: 5},
			6: {NUMANodeID: 1, SocketID: 1, CoreID: 6},
			7: {NUMANodeID: 1, SocketID: 1, CoreID: 7},
		},
	}
}

func testTwoNUMASMTTopology() *machine.CPUTopology {
	return &machine.CPUTopology{
		NumCPUs:      8,
		NumCores:     4,
		NumSockets:   2,
		NumNUMANodes: 2,
		CPUDetails: machine.CPUDetails{
			0: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			1: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			2: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			3: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			4: {NUMANodeID: 1, SocketID: 1, CoreID: 2},
			5: {NUMANodeID: 1, SocketID: 1, CoreID: 2},
			6: {NUMANodeID: 1, SocketID: 1, CoreID: 3},
			7: {NUMANodeID: 1, SocketID: 1, CoreID: 3},
		},
	}
}

func assertCPUSet(t *testing.T, name string, got machine.CPUSet, want string) {
	t.Helper()
	if got.String() != want {
		t.Fatalf("%s cpuset = %s, want %s", name, got.String(), want)
	}
}
