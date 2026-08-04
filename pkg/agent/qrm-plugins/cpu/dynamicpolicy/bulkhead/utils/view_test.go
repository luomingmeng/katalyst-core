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
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	cpustate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

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

func assertCPUSet(t *testing.T, name string, got machine.CPUSet, want string) {
	t.Helper()
	if got.String() != want {
		t.Fatalf("%s cpuset = %s, want %s", name, got.String(), want)
	}
}
