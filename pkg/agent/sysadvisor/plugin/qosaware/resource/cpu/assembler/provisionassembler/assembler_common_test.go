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

package provisionassembler

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-api/pkg/consts"
	katalyst_base "github.com/kubewharf/katalyst-core/cmd/base"
	"github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/options"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/agent/utilcomponent/featuregatenegotiation/finders"
	"github.com/kubewharf/katalyst-core/pkg/agent/utilcomponent/featuregatenegotiation/finders/feature_cpu"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	metaagent "github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	metricspool "github.com/kubewharf/katalyst-core/pkg/metrics/metrics-pool"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/kubewharf/katalyst-core/pkg/util/reclaim"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

func TestCanonicalizeDefaultShareEntries(t *testing.T) {
	t.Parallel()

	t.Run("preserve fake NUMA share and unrelated pools", func(t *testing.T) {
		t.Parallel()

		result := &types.InternalCPUCalculationResult{
			PoolEntries: make(map[string]map[int]types.CPUResource),
		}
		result.SetPoolEntry(commonstate.PoolNameShare, commonstate.FakedNUMAID, 4, -1)
		result.SetPoolEntry(commonstate.PoolNameShare, 0, 6, -1)
		result.SetPoolEntry(commonstate.PoolNameShare, 1, 8, -1)
		result.SetPoolEntry("share-NUMA0", 0, 2, -1)
		result.SetPoolEntry(commonstate.PoolNameReclaim, 0, 4, -1)

		before := canonicalizeDefaultShareEntries(result)

		require.Equal(t, 4, before)
		require.Len(t, result.PoolEntries[commonstate.PoolNameShare], 1)
		require.Contains(t, result.PoolEntries[commonstate.PoolNameShare], commonstate.FakedNUMAID)
		require.Contains(t, result.PoolEntries, "share-NUMA0")
		require.Contains(t, result.PoolEntries, commonstate.PoolNameReclaim)
	})

	t.Run("remove generic share with only real NUMA entries", func(t *testing.T) {
		t.Parallel()

		result := &types.InternalCPUCalculationResult{
			PoolEntries: make(map[string]map[int]types.CPUResource),
		}
		result.SetPoolEntry(commonstate.PoolNameShare, 0, 6, -1)
		result.SetPoolEntry(commonstate.PoolNameShare, 1, 8, -1)
		result.SetPoolEntry("share-NUMA0", 0, 2, -1)
		result.SetPoolEntry(commonstate.PoolNameReclaim, 0, 4, -1)

		before := canonicalizeDefaultShareEntries(result)

		require.Zero(t, before)
		require.NotContains(t, result.PoolEntries, commonstate.PoolNameShare)
		require.Contains(t, result.PoolEntries, "share-NUMA0")
		require.Contains(t, result.PoolEntries, commonstate.PoolNameReclaim)
	})
}

type FakeRegion struct {
	name                       string
	ownerPoolName              string
	regionType                 configapi.QoSRegionType
	bindingNumas               machine.CPUSet
	isNumaBinding              bool
	isNumaExclusive            bool
	enableReclaim              bool
	podSets                    types.PodSet
	controlKnob                types.ControlKnob
	podsRequest                float64
	headroom                   float64
	throttled                  bool
	provisionCurrentPolicyName types.CPUProvisionPolicyName
	provisionPolicyTopPriority types.CPUProvisionPolicyName
	headroomCurrentPolicyName  types.CPUHeadroomPolicyName
	headroomPolicyTopPriority  types.CPUHeadroomPolicyName
	controlEssentials          types.ControlEssentials
	essentials                 types.ResourceEssentials
}

func NewFakeRegion(name string, regionType configapi.QoSRegionType, ownerPoolName string) *FakeRegion {
	return &FakeRegion{
		name:          name,
		regionType:    regionType,
		ownerPoolName: ownerPoolName,
		enableReclaim: true,
	}
}

func (fake *FakeRegion) Name() string {
	return fake.name
}

func (fake *FakeRegion) Type() configapi.QoSRegionType {
	return fake.regionType
}

func (fake *FakeRegion) GetMetaInfo() string {
	return "fake"
}

func (fake *FakeRegion) OwnerPoolName() string {
	return fake.ownerPoolName
}

func (fake *FakeRegion) GetResourcePackageName() string {
	_, pkgName := resourcepackage.UnwrapOwnerPoolName(fake.ownerPoolName)
	return pkgName
}

func (fake *FakeRegion) IsEmpty() bool {
	return false
}
func (fake *FakeRegion) Clear() {}
func (fake *FakeRegion) GetBindingNumas() machine.CPUSet {
	return fake.bindingNumas
}

func (fake *FakeRegion) SetPods(podSet types.PodSet) {
	fake.podSets = podSet
}

func (fake *FakeRegion) GetPods() types.PodSet {
	return fake.podSets
}

func (fake *FakeRegion) GetPodsRequest() float64 {
	return fake.podsRequest
}

func (fake *FakeRegion) SetBindingNumas(bindingNumas machine.CPUSet) {
	fake.bindingNumas = bindingNumas
}

func (fake *FakeRegion) SetEssentials(essentials types.ResourceEssentials) {
	fake.essentials = essentials
}

func (fake *FakeRegion) SetIsNumaBinding(isNumaBinding bool) {
	fake.isNumaBinding = isNumaBinding
}

func (fake *FakeRegion) IsNumaBinding() bool {
	return fake.isNumaBinding
}
func (fake *FakeRegion) IsNumaExclusive() bool                      { return fake.isNumaExclusive }
func (fake *FakeRegion) SetThrottled(throttled bool)                { fake.throttled = throttled }
func (fake *FakeRegion) EnableReclaim() bool                        { return fake.enableReclaim }
func (fake *FakeRegion) AddContainer(ci *types.ContainerInfo) error { return nil }
func (fake *FakeRegion) TryUpdateProvision()                        {}
func (fake *FakeRegion) TryUpdateHeadroom()                         {}
func (fake *FakeRegion) UpdateStatus()                              {}
func (fake *FakeRegion) SetProvision(controlKnob types.ControlKnob) {
	fake.controlKnob = controlKnob
}

func (fake *FakeRegion) GetProvision() (types.ControlKnob, error) {
	return fake.controlKnob, nil
}

func (fake *FakeRegion) SetHeadroom(value float64) {
	fake.headroom = value
}

func (fake *FakeRegion) GetHeadroom() (float64, error) {
	return fake.headroom, nil
}

func (fake *FakeRegion) IsThrottled() bool {
	return fake.throttled
}

func (fake *FakeRegion) SetProvisionPolicy(policyTopPriority, currentPolicyName types.CPUProvisionPolicyName) {
	fake.provisionPolicyTopPriority = policyTopPriority
	fake.provisionCurrentPolicyName = currentPolicyName
}

func (fake *FakeRegion) GetProvisionPolicy() (types.CPUProvisionPolicyName, types.CPUProvisionPolicyName) {
	return fake.provisionPolicyTopPriority, fake.provisionCurrentPolicyName
}

func (fake *FakeRegion) SetHeadRoomPolicy(policyTopPriority, currentPolicyName types.CPUHeadroomPolicyName) {
	fake.headroomPolicyTopPriority = policyTopPriority
	fake.headroomCurrentPolicyName = currentPolicyName
}

func (fake *FakeRegion) GetHeadRoomPolicy() (types.CPUHeadroomPolicyName, types.CPUHeadroomPolicyName) {
	return fake.headroomPolicyTopPriority, fake.headroomCurrentPolicyName
}

func (fake *FakeRegion) GetStatus() types.RegionStatus {
	return types.RegionStatus{}
}

func (fake *FakeRegion) SetControlEssentials(controlEssentials types.ControlEssentials) {
	fake.controlEssentials = controlEssentials
}

func (fake *FakeRegion) GetControlEssentials() types.ControlEssentials {
	return fake.controlEssentials
}

type testCasePoolConfig struct {
	poolName      string
	poolType      configapi.QoSRegionType
	numa          machine.CPUSet
	isNumaBinding bool
	provision     types.ControlKnob
}

func TestAssembleProvision(t *testing.T) {
	containerInfos := []types.ContainerInfo{
		{
			PodUID:              "pod1",
			ContainerName:       "container1",
			QoSLevel:            consts.PodAnnotationQoSLevelSharedCores,
			RegionNames:         sets.NewString("share-NUMA1"),
			OriginOwnerPoolName: "share-NUMA1",
			OwnerPoolName:       "share-NUMA1",
			TopologyAwareAssignments: map[int]machine.CPUSet{
				1: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8),
			},
			OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
				1: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8),
			},
		},
	}
	_ = containerInfos

	poolInfos := map[string]types.PoolInfo{
		"share-NUMA1": {
			PoolName: "share-NUMA1",
			TopologyAwareAssignments: map[int]machine.CPUSet{
				1: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8),
			},
			OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
				1: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8),
			},
			RegionNames: sets.NewString("share-NUMA1"),
		},
		"share": {
			PoolName: "share",
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			},
			OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			},
		},
		"isolation-NUMA1": {
			PoolName: "isolation-NUMA1",
			TopologyAwareAssignments: map[int]machine.CPUSet{
				1: machine.NewCPUSet(20, 21, 22, 23),
			},
			OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
				1: machine.NewCPUSet(20, 21, 22, 23),
			},
			RegionNames: sets.NewString("isolation-NUMA1"),
		},
		"isolation-NUMA1-pod2": {
			PoolName: "isolation-NUMA1-pod2",
			TopologyAwareAssignments: map[int]machine.CPUSet{
				1: machine.NewCPUSet(20, 21, 22, 23),
			},
			OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
				1: machine.NewCPUSet(20, 21, 22, 23),
			},
			RegionNames: sets.NewString("isolation-NUMA1-pod2"),
		},
		"share-a": {
			PoolName: "share",
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			},
			OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			},
		},
		"share-b": {
			PoolName: "share",
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			},
			OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			},
		},
	}
	tests := []struct {
		name                                       string
		enableReclaimed                            bool
		allowSharedCoresOverlapReclaimedCores      bool
		disableDedicatedCoresOverlapReclaimedCores bool
		disableReclaimSelector                     string
		resourcePackageConfig                      types.ResourcePackageConfig
		poolInfos                                  []testCasePoolConfig
		wantErr                                    bool
		expectPoolEntries                          map[string]map[int]types.CPUResource
		expectPoolOverlapInfo                      map[string]map[int]map[string]int
	}{
		{
			name:                                  "test-disable-reclaim-pkg-complex",
			enableReclaimed:                       true,
			disableReclaimSelector:                "disable-reclaim=true",
			allowSharedCoresOverlapReclaimedCores: true,
			disableDedicatedCoresOverlapReclaimedCores: true,
			resourcePackageConfig: types.ResourcePackageConfig{
				0: map[string]*types.ResourcePackageState{
					"pkg1": {
						Attributes:   map[string]string{"disable-reclaim": "true"},
						PinnedCPUSet: machine.NewCPUSet(1, 2, 3), // size 3
					},
					"pkg2": {
						Attributes:   map[string]string{"disable-reclaim": "false"},
						PinnedCPUSet: machine.NewCPUSet(4, 5), // size 2
					},
				},
				1: map[string]*types.ResourcePackageState{
					"pkg1": {
						Attributes:   map[string]string{"disable-reclaim": "true"},
						PinnedCPUSet: machine.NewCPUSet(1, 2, 3, 4, 5), // size 5
					},
				},
			},
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share", // ownerPoolName is share, pkg is empty
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1", // ownerPoolName is share-NUMA1, pkg is NUMA1
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 19, Quota: -1}, // allow expand to full size
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 11, Quota: -1}, // NUMA1 total 24, isolation 8, share req 8. allow expand but max is 24-8=16?
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					// NUMA 0: available 24, isolated 0, unused non-reclaimable: pkg1(size 3) - allocated 0 = 3
					// overlapReclaim pool calculation: shareReclaimCoresSize = 24 - 0 - 0 - 6 - 0 - 3 = 15
					// reclaimedCoresSize = 15 + 0 = 15
					// overlapSharePoolSizes = 24, overlapReclaimSize = 15
					-1: types.CPUResource{Size: 2, Quota: -1},
					// NUMA 1: available 24, isolated 8, unused non-reclaimable: pkg1(size 5) - allocated 0 = 5
					// shareReclaimCoresSize = 24 - 8 - 0 - 8 - 0 - 5 = 3
					// reclaimedCoresSize = 3 (but reservedForReclaim is 4, so it should be regulated to 4)
					// if regulated to 4, then overlapReclaimSize is 4
					// nonOverlap is 4-4=0
					1: types.CPUResource{Size: 0, Quota: -1},
				},
			},
			expectPoolOverlapInfo: map[string]map[int]map[string]int{
				"reclaim": {
					-1: map[string]int{"share": 13}, // total unused non-reclaimable is 3. share size is 24, req is 6, max reclaim is 15. overlap is 15.
					1:  map[string]int{"share-NUMA1": 4},
				},
			},
		},
		{
			name:                   "test-disable-reclaim-pkg",
			enableReclaimed:        true,
			disableReclaimSelector: "disable-reclaim=true",
			resourcePackageConfig: types.ResourcePackageConfig{
				0: map[string]*types.ResourcePackageState{
					"pkg1": {
						Attributes:   map[string]string{"disable-reclaim": "true"},
						PinnedCPUSet: machine.NewCPUSet(1, 2, 3), // size 3
					},
					"pkg2": {
						Attributes:   map[string]string{"disable-reclaim": "false"},
						PinnedCPUSet: machine.NewCPUSet(4, 5), // size 2
					},
				},
			},
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 6, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 15, Quota: -1}, // Originally 18, but we deducted 3 unused non-reclaimable
					1:  types.CPUResource{Size: 16, Quota: -1},
				},
			},
		},
		{
			name:            "test1",
			enableReclaimed: true,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 6, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 18, Quota: -1},
					1:  types.CPUResource{Size: 16, Quota: -1},
				},
			},
		},
		{
			name:            "test2",
			enableReclaimed: false,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 20, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 20, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 4, Quota: -1},
					1:  types.CPUResource{Size: 4, Quota: -1},
				},
			},
		},
		{
			name:            "test3",
			enableReclaimed: true,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 6, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 18, Quota: -1},
					1:  types.CPUResource{Size: 8, Quota: -1},
				},
			},
		},
		{
			name:            "test4",
			enableReclaimed: false,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 20, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 12, Quota: -1},
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 4, Quota: -1},
					1:  types.CPUResource{Size: 4, Quota: -1},
				},
			},
		},
		{
			name:            "test5",
			enableReclaimed: false,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 15},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 20, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 15, Quota: -1},
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 5, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 4, Quota: -1},
					1:  types.CPUResource{Size: 4, Quota: -1},
				},
			},
		},
		{
			name:            "test6",
			enableReclaimed: true,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 15},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 6, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 15, Quota: -1},
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 5, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 18, Quota: -1},
					1:  types.CPUResource{Size: 4, Quota: -1},
				},
			},
		},
		{
			name:            "test7",
			enableReclaimed: true,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 4},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
				{
					poolName:      "isolation-NUMA1-pod2",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 6, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 4, Quota: -1},
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"isolation-NUMA1-pod2": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 18, Quota: -1},
					1:  types.CPUResource{Size: 4, Quota: -1},
				},
			},
		},
		{
			name:            "test8",
			enableReclaimed: true,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
				{
					poolName:      "isolation-NUMA1-pod2",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 6, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 6, Quota: -1},
				},
				"isolation-NUMA1-pod2": {
					1: types.CPUResource{Size: 6, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 18, Quota: -1},
					1:  types.CPUResource{Size: 4, Quota: -1},
				},
			},
		},
		{
			name:            "test9",
			enableReclaimed: false,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
				{
					poolName:      "isolation-NUMA1-pod2",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 20, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 6, Quota: -1},
				},
				"isolation-NUMA1-pod2": {
					1: types.CPUResource{Size: 6, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 4, Quota: -1},
					1:  types.CPUResource{Size: 4, Quota: -1},
				},
			},
		},
		{
			name:                                  "share and isolated pool not throttled, overlap reclaimed cores",
			enableReclaimed:                       true,
			allowSharedCoresOverlapReclaimedCores: true,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
				{
					poolName:      "isolation-NUMA1-pod2",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 24, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"isolation-NUMA1-pod2": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 0, Quota: -1},
					1:  types.CPUResource{Size: 0, Quota: -1},
				},
			},
			expectPoolOverlapInfo: map[string]map[int]map[string]int{
				"reclaim": {-1: map[string]int{"share": 18}, 1: map[string]int{"share-NUMA1": 4}},
			},
		},
		{
			name:                                  "no share pool and isolated pool, allow shared_cores overlap reclaimed_cores",
			enableReclaimed:                       true,
			allowSharedCoresOverlapReclaimedCores: true,
			poolInfos:                             []testCasePoolConfig{},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 48, Quota: -1},
				},
			},
		},
		{
			name:                                  "share and isolated pool not throttled, overlap reclaimed cores, reclaim disabled",
			enableReclaimed:                       false,
			allowSharedCoresOverlapReclaimedCores: true,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-NUMA1",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
				{
					poolName:      "isolation-NUMA1-pod2",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share": {
					-1: types.CPUResource{Size: 24, Quota: -1},
				},
				"share-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"isolation-NUMA1-pod2": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 0, Quota: -1},
					1:  types.CPUResource{Size: 0, Quota: -1},
				},
			},
			expectPoolOverlapInfo: map[string]map[int]map[string]int{
				"reclaim": {-1: map[string]int{"share": 4}, 1: map[string]int{"share-NUMA1": 4}},
			},
		},
		{
			name:                                  "isolated pools only, with numa binding",
			enableReclaimed:                       true,
			allowSharedCoresOverlapReclaimedCores: true,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "isolation-NUMA1",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
				{
					poolName:      "isolation-NUMA1-pod2",
					poolType:      configapi.QoSRegionTypeIsolation,
					numa:          machine.NewCPUSet(1),
					isNumaBinding: true,
					provision: types.ControlKnob{
						configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: 8},
						configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: 4},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"isolation-NUMA1": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"isolation-NUMA1-pod2": {
					1: types.CPUResource{Size: 8, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					1:  types.CPUResource{Size: 8, Quota: -1},
					-1: types.CPUResource{Size: 24, Quota: -1},
				},
			},
		},
		{
			name:                                  "share and bach pool non binding NUMAs, overlap reclaimed cores",
			enableReclaimed:                       true,
			allowSharedCoresOverlapReclaimedCores: true,
			poolInfos: []testCasePoolConfig{
				{
					poolName:      "share-a",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0, 1),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 6},
					},
				},
				{
					poolName:      "share-b",
					poolType:      configapi.QoSRegionTypeShare,
					numa:          machine.NewCPUSet(0, 1),
					isNumaBinding: false,
					provision: types.ControlKnob{
						configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
					},
				},
			},
			expectPoolEntries: map[string]map[int]types.CPUResource{
				"share-a": {
					-1: types.CPUResource{Size: 24, Quota: -1},
				},
				"share-b": {
					-1: types.CPUResource{Size: 24, Quota: -1},
				},
				"reserve": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
				"reclaim": {
					-1: types.CPUResource{Size: 0, Quota: -1},
				},
			},
			expectPoolOverlapInfo: map[string]map[int]map[string]int{
				"reclaim": {-1: map[string]int{"share-a": 18, "share-b": 16}},
			},
		},
		{
			name:                   "test with invalid disable-reclaim selector",
			disableReclaimSelector: "disable-reclaim=true,,invalid",
			wantErr:                true,
		},
	}

	reservedForReclaim := map[int]int{
		0: 4,
		1: 4,
	}

	numaAvailable := map[int]int{
		0: 24,
		1: 24,
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			conf := generateTestConf(t, tt.enableReclaimed, tt.disableReclaimSelector)
			genericCtx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{})
			require.NoError(t, err)

			reclaim.UnregisterConsumer(reclaim.GenericConsumerName)
			t.Cleanup(func() {
				reclaim.UnregisterConsumer(reclaim.GenericConsumerName)
			})
			metaServer, err := metaserver.NewMetaServer(genericCtx.Client, metrics.DummyMetrics{}, conf)
			require.NoError(t, err)
			defer func() {
				os.RemoveAll(conf.GenericSysAdvisorConfiguration.StateFileDirectory)
				os.RemoveAll(conf.MetaServerConfiguration.CheckpointManagerDir)
			}()

			metaCache, err := metacache.NewMetaCacheImp(conf, metricspool.DummyMetricsEmitterPool{}, metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}))
			require.NoError(t, err)
			if tt.resourcePackageConfig != nil {
				require.NoError(t, metaCache.SetResourcePackageConfig(tt.resourcePackageConfig))
			} else {
				require.NoError(t, metaCache.SetResourcePackageConfig(types.ResourcePackageConfig{0: map[string]*types.ResourcePackageState{}}))
			}

			nonBindingNumas := machine.NewCPUSet()
			for numaID := range numaAvailable {
				nonBindingNumas.Add(numaID)
			}

			regionMap := map[string]region.QoSRegion{}
			for _, poolConfig := range tt.poolInfos {
				poolInfo, ok := poolInfos[poolConfig.poolName]
				require.True(t, ok, "pool config doesn't exist")
				require.NoError(t, metaCache.SetPoolInfo(poolInfo.PoolName, &poolInfo), "failed to set pool info %s", poolInfo.PoolName)
				region := NewFakeRegion(poolConfig.poolName, poolConfig.poolType, poolConfig.poolName)
				region.SetBindingNumas(poolConfig.numa)
				region.SetIsNumaBinding(poolConfig.isNumaBinding)
				region.SetProvision(poolConfig.provision)
				region.TryUpdateProvision()
				require.Equal(t, poolConfig.isNumaBinding, region.IsNumaBinding(), "invalid numa binding state")
				regionMap[region.name] = region

				if region.IsNumaBinding() {
					nonBindingNumas = nonBindingNumas.Difference(region.GetBindingNumas())
				}
			}

			rampUpReclaimCPUSetCap := map[int]int{}
			common := NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &rampUpReclaimCPUSetCap, &numaAvailable,
				&nonBindingNumas, &tt.allowSharedCoresOverlapReclaimedCores,
				&tt.disableDedicatedCoresOverlapReclaimedCores, metaCache, metaServer, metrics.DummyMetrics{})
			result, err := common.AssembleProvision(ProvisionContext{
				DynamicConfiguration: conf.GetDynamicConfiguration(),
			})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
			require.NotNil(t, result, "invalid assembler result")
			require.Equal(t, tt.disableDedicatedCoresOverlapReclaimedCores,
				result.DisableDedicatedCoresOverlapReclaimedCores)
			t.Logf("%v", result)
			require.Equal(t, tt.expectPoolEntries, result.PoolEntries, "unexpected result")
			if len(tt.expectPoolOverlapInfo) > 0 {
				require.Equal(t, tt.expectPoolOverlapInfo, result.PoolOverlapInfo, "unexpected result")
			}
		})
	}
}

func generateTestConf(t *testing.T, enableReclaim bool, disableReclaimSelector string) *config.Configuration {
	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	require.NotNil(t, conf)

	suffix := rand.String(10)
	stateFileDir := "stateFileDir." + suffix
	checkpointDir := "checkpointDir." + suffix

	conf.GenericSysAdvisorConfiguration.StateFileDirectory = stateFileDir
	conf.MetaServerConfiguration.CheckpointManagerDir = checkpointDir
	conf.RestrictRefPolicy = nil
	conf.CPUAdvisorConfiguration.ProvisionPolicies = map[configapi.QoSRegionType][]types.CPUProvisionPolicyName{
		configapi.QoSRegionTypeShare: {types.CPUProvisionPolicyCanonical},
	}
	conf.GetDynamicConfiguration().EnableReclaim = enableReclaim
	if disableReclaimSelector != "" {
		conf.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector = disableReclaimSelector
	}
	return conf
}

func TestClampReclaimOverlapMetadata(t *testing.T) {
	t.Parallel()

	result := &types.InternalCPUCalculationResult{
		PoolOverlapInfo: map[string]map[int]map[string]int{
			commonstate.PoolNameReclaim: {
				0: {
					"share-a": 5,
					"share-b": 3,
				},
			},
		},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{
			commonstate.PoolNameReclaim: {
				0: {
					"pod-a": {
						"main": 4,
					},
				},
			},
		},
	}

	got := clampReclaimOverlapMetadata(result, 0, 3,
		overlapAtom{key: "0/pool/share-a", size: 5, poolAlias: "share-a"},
		overlapAtom{key: "0/pool/share-b", size: 3, poolAlias: "share-b"},
		overlapAtom{
			key:            "1/dedicated/block-a",
			size:           4,
			containerAlias: []podContainerAlias{{podUID: "pod-a", containerName: "main"}},
		},
	)

	require.Equal(t, 3, got)
	require.Equal(t, map[string]int{"share-a": 3}, result.PoolOverlapInfo[commonstate.PoolNameReclaim][0])
	require.Empty(t, result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0])
}

func TestClampReclaimOverlapMetadataClearsZeroBudget(t *testing.T) {
	t.Parallel()

	result := &types.InternalCPUCalculationResult{
		PoolOverlapInfo: map[string]map[int]map[string]int{
			commonstate.PoolNameReclaim: {
				0: {"share": 2},
			},
		},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{
			commonstate.PoolNameReclaim: {
				0: {"pod": {"main": 1}},
			},
		},
	}

	got := clampReclaimOverlapMetadata(result, 0, 0)

	require.Zero(t, got)
	require.Empty(t, result.PoolOverlapInfo[commonstate.PoolNameReclaim][0])
	require.Empty(t, result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0])
}

func TestClampReclaimOverlapMetadataKeepsContainerAliases(t *testing.T) {
	t.Parallel()

	result := &types.InternalCPUCalculationResult{
		PoolOverlapInfo: map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{
			commonstate.PoolNameReclaim: {
				0: {
					"pod": {
						"main":    4,
						"sidecar": 4,
					},
				},
			},
		},
	}

	got := clampReclaimOverlapMetadata(result, 0, 4, overlapAtom{
		key:  "dedicated/block-a",
		size: 4,
		containerAlias: []podContainerAlias{
			{podUID: "pod", containerName: "main"},
			{podUID: "pod", containerName: "sidecar"},
		},
	})

	require.Equal(t, 4, got)
	require.Equal(t, map[string]int{"main": 4, "sidecar": 4},
		result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod"])
}

func TestClampReclaimOverlapMetadataRejectsImplicitPodUIDGrouping(t *testing.T) {
	t.Parallel()

	result := &types.InternalCPUCalculationResult{
		PoolOverlapInfo: map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{
			commonstate.PoolNameReclaim: {
				0: {
					"pod": {
						"main":    4,
						"sidecar": 2,
					},
				},
			},
		},
	}

	got := clampReclaimOverlapMetadata(result, 0, 4)

	require.Zero(t, got)
	require.Empty(t, result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0])
}

func TestClampReclaimOverlapMetadataMultipleDedicatedBlocks(t *testing.T) {
	t.Parallel()

	result := &types.InternalCPUCalculationResult{
		PoolOverlapInfo:             map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
	}
	got := clampReclaimOverlapMetadata(result, 0, 5,
		overlapAtom{
			key:  "dedicated/block-a",
			size: 3,
			containerAlias: []podContainerAlias{
				{podUID: "pod-a", containerName: "main"},
				{podUID: "pod-a", containerName: "sidecar"},
			},
		},
		overlapAtom{
			key:  "dedicated/block-b",
			size: 4,
			containerAlias: []podContainerAlias{
				{podUID: "pod-b", containerName: "main"},
				{podUID: "pod-b", containerName: "sidecar"},
			},
		},
	)

	require.Equal(t, 5, got)
	require.Equal(t, map[string]int{"main": 3, "sidecar": 3},
		result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod-a"])
	require.Equal(t, map[string]int{"main": 2, "sidecar": 2},
		result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod-b"])
}

func TestAllocatePoolSizesByPriority(t *testing.T) {
	t.Parallel()

	got, throttled := allocatePoolSizesByPriority(
		9,
		map[string]int{"dedicated": 4},
		map[string]int{"isolation": 3},
		map[string]int{"share": 2},
		map[string]int{
			"dedicated": 6,
			"isolation": 5,
			"share":     6,
		},
	)

	require.True(t, throttled)
	require.Equal(t, map[string]int{
		"dedicated": 4,
		"isolation": 3,
		"share":     2,
	}, got)
}

func TestAllocatePoolSizesByPriorityIsMapOrderIndependent(t *testing.T) {
	t.Parallel()

	first, firstThrottled := allocatePoolSizesByPriority(
		12,
		map[string]int{"dedicated-b": 3, "dedicated-a": 3},
		map[string]int{"isolation-b": 2, "isolation-a": 2},
		map[string]int{"share-b": 2, "share-a": 2},
		map[string]int{"share-a": 4, "share-b": 4},
	)
	second, secondThrottled := allocatePoolSizesByPriority(
		12,
		map[string]int{"dedicated-a": 3, "dedicated-b": 3},
		map[string]int{"isolation-a": 2, "isolation-b": 2},
		map[string]int{"share-a": 2, "share-b": 2},
		map[string]int{"share-b": 4, "share-a": 4},
	)

	require.Equal(t, firstThrottled, secondThrottled)
	require.Equal(t, first, second)
	require.Equal(t, 12, general.SumUpMapValues(first))
}

func TestPriorityAllocationAndDedicatedAtomPressure(t *testing.T) {
	t.Parallel()

	for i := 0; i < 256; i++ {
		dedicated := make(map[string]int)
		isolation := make(map[string]int)
		shared := make(map[string]int)
		if i%2 == 0 {
			dedicated["dedicated-b"], dedicated["dedicated-a"] = 3, 3
			isolation["isolation-b"], isolation["isolation-a"] = 2, 2
			shared["share-b"], shared["share-a"] = 2, 2
		} else {
			dedicated["dedicated-a"], dedicated["dedicated-b"] = 3, 3
			isolation["isolation-a"], isolation["isolation-b"] = 2, 2
			shared["share-a"], shared["share-b"] = 2, 2
		}
		got, throttled := allocatePoolSizesByPriority(9, dedicated, isolation, shared, nil)
		require.True(t, throttled || general.SumUpMapValues(got) == 9)
		require.Equal(t, map[string]int{
			"dedicated-a": 3,
			"dedicated-b": 3,
			"isolation-a": 2,
			"isolation-b": 1,
		}, got)

		result := &types.InternalCPUCalculationResult{
			PoolOverlapInfo:             map[string]map[int]map[string]int{},
			PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
		}
		atoms := []overlapAtom{
			{
				key:            "dedicated/block-b",
				size:           4,
				containerAlias: []podContainerAlias{{podUID: "pod-b", containerName: "main"}},
			},
			{
				key:            "dedicated/block-a",
				size:           3,
				containerAlias: []podContainerAlias{{podUID: "pod-a", containerName: "main"}},
			},
		}
		if i%2 != 0 {
			atoms[0], atoms[1] = atoms[1], atoms[0]
		}
		require.Equal(t, 5, clampReclaimOverlapMetadata(result, 0, 5, atoms...))
		require.Equal(t, 3,
			result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod-a"]["main"])
		require.Equal(t, 2,
			result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod-b"]["main"])
	}
}

func TestFinalizeDefaultShareBackfillAllowsFullyExclusiveNode(t *testing.T) {
	t.Parallel()

	conf := generateTestConf(t, true, "")
	conf.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs = true

	genericCtx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{})
	require.NoError(t, err)

	reclaim.UnregisterConsumer(reclaim.GenericConsumerName)
	t.Cleanup(func() {
		reclaim.UnregisterConsumer(reclaim.GenericConsumerName)
	})
	metaServer, err := metaserver.NewMetaServer(genericCtx.Client, metrics.DummyMetrics{}, conf)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(conf.GenericSysAdvisorConfiguration.StateFileDirectory)
		_ = os.RemoveAll(conf.MetaServerConfiguration.CheckpointManagerDir)
	})

	metaCache, err := metacache.NewMetaCacheImp(conf, metricspool.DummyMetricsEmitterPool{}, metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}))
	require.NoError(t, err)
	require.NoError(t, metaCache.SetResourcePackageConfig(types.ResourcePackageConfig{0: map[string]*types.ResourcePackageState{}}))

	region0 := NewFakeRegion("dedicated-numa0", configapi.QoSRegionTypeDedicated, "dedicated-numa0")
	region0.SetBindingNumas(machine.NewCPUSet(0))
	region0.SetIsNumaBinding(true)
	region0.isNumaExclusive = true

	region1 := NewFakeRegion("dedicated-numa1", configapi.QoSRegionTypeDedicated, "dedicated-numa1")
	region1.SetBindingNumas(machine.NewCPUSet(1))
	region1.SetIsNumaBinding(true)
	region1.isNumaExclusive = true

	regionMap := map[string]region.QoSRegion{
		region0.name: region0,
		region1.name: region1,
	}
	reservedForReclaim := map[int]int{0: 0, 1: 0}
	rampUpReclaimCPUSetCap := map[int]int{}
	numaAvailable := map[int]int{0: 8, 1: 8}
	nonBindingNumas := machine.NewCPUSet()
	allowSharedOverlap := false
	disableDedicatedOverlap := true

	common := NewProvisionAssemblerCommon(
		conf, nil, &regionMap, &reservedForReclaim, &rampUpReclaimCPUSetCap, &numaAvailable, &nonBindingNumas,
		&allowSharedOverlap, &disableDedicatedOverlap, metaCache, metaServer, metrics.DummyMetrics{},
	).(*ProvisionAssemblerCommon)

	result := &types.InternalCPUCalculationResult{
		PoolEntries:                 map[string]map[int]types.CPUResource{},
		PoolOverlapInfo:             map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
	}
	result.DefaultShareBackfill.Enabled = true

	err = common.finalizeDefaultShareBackfill(NewRegionMapHelper(regionMap), result)
	require.NoError(t, err)
	require.Contains(t, result.PoolEntries, commonstate.PoolNameShare)
	require.Equal(t, 0, result.PoolEntries[commonstate.PoolNameShare][commonstate.FakedNUMAID].Size)
	require.Equal(t, 0, result.DefaultShareBackfill.DefaultShareFinal)
}

func TestAssembleWithoutNUMAExclusivePoolOverlapPolicyMatrix(t *testing.T) {
	tests := []struct {
		name                   string
		sharedEnableReclaim    bool
		dedicatedEnableReclaim bool
		want                   types.InternalCPUCalculationResult
	}{
		{
			name: "AS0_DD0_SE0_DE0",
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 8, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 4, Quota: -1}},
				},
				PoolOverlapInfo:             map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
				TimeStamp:                   time.Time{},
			},
		},
		{
			name:                   "AS0_DD0_SE0_DE1",
			dedicatedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 8, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 4, Quota: -1}},
				},
				PoolOverlapInfo: map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{
					commonstate.PoolNameReclaim: {0: {"dedicated-pod": {"main": 2, "sidecar": 2}}},
				},
				TimeStamp: time.Time{},
			},
		},
		{
			name:                "AS0_DD0_SE1_DE0",
			sharedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 4, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 8, Quota: -1}},
				},
				PoolOverlapInfo:             map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
				TimeStamp:                   time.Time{},
			},
		},
		{
			name:                   "AS0_DD0_SE1_DE1",
			sharedEnableReclaim:    true,
			dedicatedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 4, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 8, Quota: -1}},
				},
				PoolOverlapInfo: map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{
					commonstate.PoolNameReclaim: {0: {"dedicated-pod": {"main": 2, "sidecar": 2}}},
				},
				TimeStamp: time.Time{},
			},
		},
		{
			name: "AS0_DD1_SE0_DE0",
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 8, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 4, Quota: -1}},
				},
				PoolOverlapInfo:             map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
				TimeStamp:                   time.Time{},
				DisableDedicatedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name:                   "AS0_DD1_SE0_DE1",
			dedicatedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 8, Quota: -1}}, "dedicated-pod": {0: {Size: 6, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 6, Quota: -1}},
				},
				PoolOverlapInfo:             map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
				TimeStamp:                   time.Time{},
				DisableDedicatedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name:                "AS0_DD1_SE1_DE0",
			sharedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 4, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 8, Quota: -1}},
				},
				PoolOverlapInfo:             map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
				TimeStamp:                   time.Time{},
				DisableDedicatedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name:                   "AS0_DD1_SE1_DE1",
			sharedEnableReclaim:    true,
			dedicatedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 4, Quota: -1}}, "dedicated-pod": {0: {Size: 6, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 10, Quota: -1}},
				},
				PoolOverlapInfo:             map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
				TimeStamp:                   time.Time{},
				DisableDedicatedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name: "AS1_DD0_SE0_DE0",
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 8, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 4, Quota: -1}},
				},
				PoolOverlapInfo:                       map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo:           map[string]map[int]map[string]map[string]int{},
				TimeStamp:                             time.Time{},
				AllowSharedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name:                   "AS1_DD0_SE0_DE1",
			dedicatedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 10, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 2, Quota: -1}},
				},
				PoolOverlapInfo: map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{
					commonstate.PoolNameReclaim: {0: {"dedicated-pod": {"main": 2, "sidecar": 2}}},
				},
				TimeStamp:                             time.Time{},
				AllowSharedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name:                "AS1_DD0_SE1_DE0",
			sharedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 12, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 0, Quota: -1}},
				},
				PoolOverlapInfo: map[string]map[int]map[string]int{
					commonstate.PoolNameReclaim: {0: {"share": 8}},
				},
				PoolOverlapPodContainerInfo:           map[string]map[int]map[string]map[string]int{},
				TimeStamp:                             time.Time{},
				AllowSharedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name:                   "AS1_DD0_SE1_DE1",
			sharedEnableReclaim:    true,
			dedicatedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 12, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 0, Quota: -1}},
				},
				PoolOverlapInfo: map[string]map[int]map[string]int{
					commonstate.PoolNameReclaim: {0: {"share": 8}},
				},
				PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{
					commonstate.PoolNameReclaim: {0: {"dedicated-pod": {"main": 2, "sidecar": 2}}},
				},
				TimeStamp:                             time.Time{},
				AllowSharedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name: "AS1_DD1_SE0_DE0",
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 8, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 4, Quota: -1}},
				},
				PoolOverlapInfo:                            map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo:                map[string]map[int]map[string]map[string]int{},
				TimeStamp:                                  time.Time{},
				AllowSharedCoresOverlapReclaimedCores:      true,
				DisableDedicatedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name:                   "AS1_DD1_SE0_DE1",
			dedicatedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 10, Quota: -1}}, "dedicated-pod": {0: {Size: 6, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 4, Quota: -1}},
				},
				PoolOverlapInfo:                            map[string]map[int]map[string]int{},
				PoolOverlapPodContainerInfo:                map[string]map[int]map[string]map[string]int{},
				TimeStamp:                                  time.Time{},
				AllowSharedCoresOverlapReclaimedCores:      true,
				DisableDedicatedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name:                "AS1_DD1_SE1_DE0",
			sharedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 12, Quota: -1}}, "dedicated-pod": {0: {Size: 8, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 0, Quota: -1}},
				},
				PoolOverlapInfo: map[string]map[int]map[string]int{
					commonstate.PoolNameReclaim: {0: {"share": 8}},
				},
				PoolOverlapPodContainerInfo:                map[string]map[int]map[string]map[string]int{},
				TimeStamp:                                  time.Time{},
				AllowSharedCoresOverlapReclaimedCores:      true,
				DisableDedicatedCoresOverlapReclaimedCores: true,
			},
		},
		{
			name:                   "AS1_DD1_SE1_DE1",
			sharedEnableReclaim:    true,
			dedicatedEnableReclaim: true,
			want: types.InternalCPUCalculationResult{
				PoolEntries: map[string]map[int]types.CPUResource{
					"share": {0: {Size: 14, Quota: -1}}, "dedicated-pod": {0: {Size: 6, Quota: -1}},
					commonstate.PoolNameReclaim: {0: {Size: 0, Quota: -1}},
				},
				PoolOverlapInfo: map[string]map[int]map[string]int{
					commonstate.PoolNameReclaim: {0: {"share": 10}},
				},
				PoolOverlapPodContainerInfo:                map[string]map[int]map[string]map[string]int{},
				TimeStamp:                                  time.Time{},
				AllowSharedCoresOverlapReclaimedCores:      true,
				DisableDedicatedCoresOverlapReclaimedCores: true,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
				capacity:                20,
				reserved:                4,
				allowSharedOverlap:      tt.want.AllowSharedCoresOverlapReclaimedCores,
				disableDedicatedOverlap: tt.want.DisableDedicatedCoresOverlapReclaimedCores,
				sharedEnableReclaim:     tt.sharedEnableReclaim,
				dedicatedEnableReclaim:  tt.dedicatedEnableReclaim,
				sharedRequest:           8,
				sharedRequirement:       4,
				dedicatedRequest:        8,
				dedicatedRequirement:    6,
			})
			require.NoError(t, err)
			// This matrix validates overlap policy outputs. Reclaim diagnostics
			// are covered separately, including the ratio-disabled path.
			tt.want.DefaultShareBackfill = result.DefaultShareBackfill
			require.Equal(t, tt.want, *result)
		})
	}
}

func TestAssembleWithoutNUMAExclusivePoolDisjointDedicatedCapacityPressure(t *testing.T) {
	t.Run("unsupported reserve compresses dedicated candidate", func(t *testing.T) {
		result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
			capacity:                5,
			reserved:                4,
			allowSharedOverlap:      false,
			disableDedicatedOverlap: false,
			dedicatedEnableReclaim:  true,
			dedicatedRequest:        8,
			dedicatedRequirement:    6,
		})
		require.NoError(t, err)
		require.Equal(t, 1, result.PoolEntries["dedicated-pod"][0].Size)
		require.Equal(t, 4, result.PoolEntries[commonstate.PoolNameReclaim][0].Size)
		require.Empty(t, result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0])
	})

	t.Run("reserve remains available alongside shared overlap", func(t *testing.T) {
		result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
			capacity:                20,
			reserved:                4,
			allowSharedOverlap:      true,
			disableDedicatedOverlap: true,
			sharedEnableReclaim:     true,
			dedicatedEnableReclaim:  true,
			sharedRequest:           8,
			sharedRequirement:       4,
			dedicatedRequest:        8,
			dedicatedRequirement:    6,
		})
		require.NoError(t, err)
		require.Zero(t, result.PoolEntries[commonstate.PoolNameReclaim][0].Size)
		require.Equal(t, 10, result.PoolOverlapInfo[commonstate.PoolNameReclaim][0]["share"])
		require.Empty(t, result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0])
	})

	t.Run("reserve is retained and dedicated uses regulated physical size", func(t *testing.T) {
		result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
			capacity:                5,
			reserved:                4,
			disableDedicatedOverlap: true,
			dedicatedEnableReclaim:  true,
			dedicatedRequest:        8,
			dedicatedRequirement:    6,
		})
		require.NoError(t, err)
		require.Equal(t, 1, result.PoolEntries["dedicated-pod"][0].Size)
		require.Equal(t, 4, result.PoolEntries[commonstate.PoolNameReclaim][0].Size)
		require.Empty(t, result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0])
	})

	t.Run("active dedicated pool cannot be regulated to zero in any policy mode", func(t *testing.T) {
		for _, allowSharedOverlap := range []bool{false, true} {
			for _, disableDedicatedOverlap := range []bool{false, true} {
				_, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
					capacity:                0,
					reserved:                0,
					allowSharedOverlap:      allowSharedOverlap,
					disableDedicatedOverlap: disableDedicatedOverlap,
					dedicatedEnableReclaim:  true,
					dedicatedRequest:        8,
					dedicatedRequirement:    6,
				})
				require.EqualError(t, err, `active dedicated pool "dedicated" was regulated to zero`,
					"AS=%t DD=%t", allowSharedOverlap, disableDedicatedOverlap)
			}
		}
	})

	t.Run("active shared and isolation pools cannot be regulated to zero", func(t *testing.T) {
		tests := []struct {
			name    string
			tc      ordinaryOverlapAssemblerCase
			wantErr string
		}{
			{
				name: "shared",
				tc: ordinaryOverlapAssemblerCase{
					capacity:          0,
					sharedRequest:     8,
					sharedRequirement: 4,
				},
				wantErr: `active shared pool "share" was regulated to zero`,
			},
			{
				name: "isolation",
				tc: ordinaryOverlapAssemblerCase{
					capacity:       0,
					isolationUpper: 8,
					isolationLower: 4,
				},
				wantErr: `active isolation pool "isolation" was regulated to zero`,
			},
		}
		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				_, err := runOrdinaryOverlapAssemblerCase(t, tt.tc)
				require.EqualError(t, err, tt.wantErr)
			})
		}
	})
}

func TestAssembleWithoutNUMAExclusivePoolDeductsReserveFromPinnedEligibilityDomain(t *testing.T) {
	result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
		capacity:                16,
		reserved:                4,
		disableDedicatedOverlap: true,
		sharedRequest:           8,
		sharedRequirement:       8,
		dedicatedEnableReclaim:  true,
		dedicatedRequest:        8,
		dedicatedRequirement:    6,
		dedicatedPackage:        "rp-a",
		pinnedCPUSet:            machine.MustParse("0-7"),
		reserveCPUSet:           machine.MustParse("0-3"),
	})
	require.NoError(t, err)
	require.Equal(t, 4, result.PoolEntries["dedicated-pod"][0].Size)
	require.Equal(t, 8, result.PoolEntries["share"][0].Size)
	require.Equal(t, 4, result.PoolEntries[commonstate.PoolNameReclaim][0].Size)
}

func TestAssembleProvisionConfiguredInactiveHardPartitionDoesNotPublishPhysicalTargetsWithoutRegions(t *testing.T) {
	t.Parallel()

	conf := generateTestConf(t, true, "")
	conf.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	regionMap := map[string]region.QoSRegion{}
	reservedForReclaim := map[int]int{0: 4, 1: 6}
	rampUpReclaimCPUSetCap := map[int]int{}
	numaAvailable := map[int]int{0: 24, 1: 32}
	nonBindingNUMAs := machine.NewCPUSet()
	allowSharedOverlap := false
	disableDedicatedOverlap := true
	metaReader := metacache.NewDummyMetaCacheImp()
	require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{}))
	metaServer := newTestMetaServer(numaAvailable, 1)

	assembler := NewProvisionAssemblerCommon(
		conf, nil, &regionMap, &reservedForReclaim, &rampUpReclaimCPUSetCap, &numaAvailable, &nonBindingNUMAs,
		&allowSharedOverlap, &disableDedicatedOverlap, metaReader, metaServer, metrics.DummyMetrics{},
	)
	result, err := assembler.AssembleProvision(ProvisionContext{
		DynamicConfiguration: conf.GetDynamicConfiguration(),
		RampUpActive:         true,
	})
	require.NoError(t, err)
	require.True(t, result.RampUpActive)
	require.False(t, result.RampUpHardPartitionActive)
	require.NotContains(t, result.PoolEntries[commonstate.PoolNameReclaim], 0)
	require.NotContains(t, result.PoolEntries[commonstate.PoolNameReclaim], 1)
}

func TestAssembleProvisionPublishesActiveHardReclaimTargetsForEmptyPhysicalNUMAs(t *testing.T) {
	t.Parallel()

	conf := generateTestConf(t, true, "")
	conf.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	regionMap := map[string]region.QoSRegion{}
	reservedForReclaim := map[int]int{0: 4, 1: 6}
	rampUpReclaimCPUSetCap := map[int]int{0: 8, 1: 10}
	numaAvailable := map[int]int{0: 24, 1: 32}
	nonBindingNUMAs := machine.NewCPUSet(0, 1)
	allowSharedOverlap := false
	disableDedicatedOverlap := true
	metaReader := metacache.NewDummyMetaCacheImp()
	require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{}))
	metaServer := newTestMetaServer(numaAvailable, 1)

	assembler := NewProvisionAssemblerCommon(
		conf, nil, &regionMap, &reservedForReclaim, &rampUpReclaimCPUSetCap, &numaAvailable, &nonBindingNUMAs,
		&allowSharedOverlap, &disableDedicatedOverlap, metaReader, metaServer, metrics.DummyMetrics{},
	)
	result, err := assembler.AssembleProvision(ProvisionContext{
		DynamicConfiguration: conf.GetDynamicConfiguration(),
	})
	require.NoError(t, err)
	require.True(t, result.RampUpHardPartitionActive)
	require.Equal(t, map[int]types.CPUResource{
		commonstate.FakedNUMAID: {Size: 0, Quota: -1},
		0:                       {Size: 8, Quota: -1},
		1:                       {Size: 10, Quota: -1},
	}, result.PoolEntries[commonstate.PoolNameReclaim])
}

func TestAssembleWithoutNUMAExclusivePoolKeepsDedicatedExcessOutsideHardReclaimTarget(t *testing.T) {
	t.Parallel()

	result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
		capacity:                16,
		reserved:                4,
		hardPartition:           true,
		disableDedicatedOverlap: true,
		dedicatedEnableReclaim:  true,
		dedicatedRequest:        12,
		dedicatedRequirement:    8,
	})
	require.NoError(t, err)
	require.Equal(t, 8, result.PoolEntries["dedicated-pod"][0].Size)
	require.Equal(t, 4, result.PoolEntries[commonstate.PoolNameReclaim][0].Size)
}

func TestAssembleWithoutNUMAExclusivePoolKeepsHardReclaimFloorAcrossOverlapPolicies(t *testing.T) {
	t.Parallel()

	for _, allowSharedOverlap := range []bool{false, true} {
		for _, disableDedicatedOverlap := range []bool{false, true} {
			result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
				capacity:                20,
				reserved:                4,
				hardPartition:           true,
				allowSharedOverlap:      allowSharedOverlap,
				disableDedicatedOverlap: disableDedicatedOverlap,
				sharedEnableReclaim:     true,
				dedicatedEnableReclaim:  true,
				sharedRequest:           8,
				sharedRequirement:       4,
				dedicatedRequest:        8,
				dedicatedRequirement:    6,
			})
			require.NoError(t, err)
			require.GreaterOrEqual(t, result.PoolEntries[commonstate.PoolNameReclaim][0].Size, 4,
				"allowSharedOverlap=%t disableDedicatedOverlap=%t",
				allowSharedOverlap, disableDedicatedOverlap)
		}
	}
}

func TestAssembleWithoutNUMAExclusivePoolReservesHardReclaimFloorFromSaturatedNUMACapacity(t *testing.T) {
	t.Parallel()

	for _, allowSharedOverlap := range []bool{false, true} {
		for _, disableDedicatedOverlap := range []bool{false, true} {
			result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
				capacity:                20,
				reserved:                4,
				hardPartition:           true,
				allowSharedOverlap:      allowSharedOverlap,
				disableDedicatedOverlap: disableDedicatedOverlap,
				sharedEnableReclaim:     true,
				dedicatedEnableReclaim:  true,
				sharedRequest:           12,
				sharedRequirement:       12,
				dedicatedRequest:        8,
				dedicatedRequirement:    6,
			})
			require.NoError(t, err)
			require.Equal(t, 16,
				result.PoolEntries["share"][0].Size+result.PoolEntries["dedicated-pod"][0].Size,
				"allowSharedOverlap=%t disableDedicatedOverlap=%t",
				allowSharedOverlap, disableDedicatedOverlap)
			require.GreaterOrEqual(t, result.PoolEntries[commonstate.PoolNameReclaim][0].Size, 4,
				"allowSharedOverlap=%t disableDedicatedOverlap=%t",
				allowSharedOverlap, disableDedicatedOverlap)
		}
	}
}

func TestAssembleWithoutNUMAExclusivePoolAddsReservedFloorBackForSharedOnlyNUMA(t *testing.T) {
	t.Parallel()

	result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
		capacity:            20,
		reserved:            4,
		hardPartition:       true,
		allowSharedOverlap:  true,
		sharedEnableReclaim: true,
		sharedRequest:       8,
		sharedRequirement:   4,
	})
	require.NoError(t, err)
	require.Equal(t, 16, result.PoolEntries["share"][0].Size)
	require.Equal(t, 4, result.PoolEntries[commonstate.PoolNameReclaim][0].Size)
	require.Empty(t, result.PoolOverlapInfo[commonstate.PoolNameReclaim][0])
}

func TestAssembleWithoutNUMAExclusivePoolUsesActiveHardTargetAsEffectiveReserve(t *testing.T) {
	t.Parallel()

	result, err := runOrdinaryOverlapAssemblerCase(t, ordinaryOverlapAssemblerCase{
		capacity:          32,
		reserved:          2,
		hardPartition:     true,
		hardTarget:        6,
		sharedRequest:     30,
		sharedRequirement: 30,
	})
	require.NoError(t, err)
	require.Equal(t, 26, result.PoolEntries["share"][0].Size)
	require.Equal(t, 6, result.PoolEntries[commonstate.PoolNameReclaim][0].Size)
}

type ordinaryOverlapAssemblerCase struct {
	capacity                int
	reserved                int
	hardPartition           bool
	hardTarget              int
	allowSharedOverlap      bool
	disableDedicatedOverlap bool
	sharedEnableReclaim     bool
	dedicatedEnableReclaim  bool
	sharedRequest           int
	sharedRequirement       int
	isolationUpper          int
	isolationLower          int
	dedicatedRequest        int
	dedicatedRequirement    int
	dedicatedPackage        string
	pinnedCPUSet            machine.CPUSet
	reserveCPUSet           machine.CPUSet
}

func runOrdinaryOverlapAssemblerCase(
	t *testing.T,
	tc ordinaryOverlapAssemblerCase,
) (*types.InternalCPUCalculationResult, error) {
	t.Helper()

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	conf.GetDynamicConfiguration().EnableReclaim = true
	conf.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = tc.hardPartition

	regionMap := map[string]region.QoSRegion{}
	if tc.sharedRequest > 0 {
		shared := NewFakeRegion("share", configapi.QoSRegionTypeShare, "share")
		shared.SetBindingNumas(machine.NewCPUSet(0))
		shared.SetIsNumaBinding(true)
		shared.enableReclaim = tc.sharedEnableReclaim
		shared.podsRequest = float64(tc.sharedRequest)
		shared.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: float64(tc.sharedRequirement)},
		})
		regionMap[shared.Name()] = shared
	}
	if tc.isolationUpper > 0 {
		isolation := NewFakeRegion("isolation", configapi.QoSRegionTypeIsolation, "isolation")
		isolation.SetBindingNumas(machine.NewCPUSet(0))
		isolation.SetIsNumaBinding(true)
		isolation.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonIsolatedUpperCPUSize: {Value: float64(tc.isolationUpper)},
			configapi.ControlKnobNonIsolatedLowerCPUSize: {Value: float64(tc.isolationLower)},
		})
		regionMap[isolation.Name()] = isolation
	}
	if tc.dedicatedRequest > 0 {
		ownerPoolName := "dedicated"
		if tc.dedicatedPackage != "" {
			ownerPoolName = tc.dedicatedPackage + "/dedicated"
		}
		dedicated := NewFakeRegion("dedicated", configapi.QoSRegionTypeDedicated, ownerPoolName)
		dedicated.SetBindingNumas(machine.NewCPUSet(0))
		dedicated.SetIsNumaBinding(true)
		dedicated.enableReclaim = tc.dedicatedEnableReclaim
		dedicated.podsRequest = float64(tc.dedicatedRequest)
		dedicated.SetPods(types.PodSet{
			"dedicated-pod": sets.NewString("main", "sidecar"),
		})
		dedicated.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: float64(tc.dedicatedRequirement)},
		})
		regionMap[dedicated.Name()] = dedicated
	}

	reservedForReclaim := map[int]int{0: tc.reserved}
	rampUpReclaimCPUSetCap := map[int]int{}
	if tc.hardPartition {
		rampUpReclaimCPUSetCap[0] = tc.hardTarget
		if tc.hardTarget == 0 {
			rampUpReclaimCPUSetCap[0] = tc.reserved
		}
	}
	numaAvailable := map[int]int{0: tc.capacity}
	nonBindingNUMAs := machine.NewCPUSet()
	metaReader := metacache.NewDummyMetaCacheImp()
	resourcePackageConfig := types.ResourcePackageConfig{}
	if tc.dedicatedPackage != "" {
		resourcePackageConfig[0] = map[string]*types.ResourcePackageState{
			tc.dedicatedPackage: {PinnedCPUSet: tc.pinnedCPUSet},
		}
	}
	require.NoError(t, metaReader.SetResourcePackageConfig(resourcePackageConfig))
	if !tc.reserveCPUSet.IsEmpty() {
		require.NoError(t, metaReader.SetPoolInfo(commonstate.PoolNameReserve, &types.PoolInfo{
			PoolName: commonstate.PoolNameReserve,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: tc.reserveCPUSet,
			},
		}))
	}
	pa := NewProvisionAssemblerCommon(
		conf, nil, &regionMap, &reservedForReclaim, &rampUpReclaimCPUSetCap, &numaAvailable, &nonBindingNUMAs,
		&tc.allowSharedOverlap, &tc.disableDedicatedOverlap, metaReader,
		newTestMetaServer(numaAvailable, 1), metrics.DummyMetrics{},
	).(*ProvisionAssemblerCommon)
	result := &types.InternalCPUCalculationResult{
		PoolEntries:                                map[string]map[int]types.CPUResource{},
		PoolOverlapInfo:                            map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo:                map[string]map[int]map[string]map[string]int{},
		AllowSharedCoresOverlapReclaimedCores:      tc.allowSharedOverlap,
		DisableDedicatedCoresOverlapReclaimedCores: tc.disableDedicatedOverlap,
	}

	err = pa.assembleWithoutNUMAExclusivePool(NewRegionMapHelper(regionMap), 0, result)
	return result, err
}

func TestAssembleProvisionMultiDedicatedDomainsIsDeterministic(t *testing.T) {
	t.Parallel()

	type regionSpec struct {
		name, owner, pod string
		numa             *int
	}
	realNUMA := 0
	specs := []regionSpec{
		{name: "real-pinned", owner: "rp-real/dedicated", pod: "pod-real-pinned", numa: &realNUMA},
		{name: "real-unpinned", owner: "dedicated-real", pod: "pod-real-unpinned", numa: &realNUMA},
		{name: "fake-rp-a", owner: "rp-a/dedicated", pod: "pod-fake-rp-a"},
		{name: "fake-rp-b", owner: "rp-b/dedicated", pod: "pod-fake-rp-b"},
		{name: "fake-unpinned-a", owner: "dedicated-fake-a", pod: "pod-fake-unpinned-a"},
		{name: "fake-unpinned-b", owner: "dedicated-fake-b", pod: "pod-fake-unpinned-b"},
	}
	wantEntries := map[string]map[int]types.CPUResource{
		commonstate.PoolNameReserve: {commonstate.FakedNUMAID: {Size: 5, Quota: -1}},
		commonstate.PoolNameReclaim: {
			0:                       {Size: 4, Quota: -1},
			commonstate.FakedNUMAID: {Size: 5, Quota: -1},
		},
		"pod-real-pinned":     {0: {Size: 4, Quota: -1}},
		"pod-real-unpinned":   {0: {Size: 4, Quota: -1}},
		"pod-fake-rp-a":       {commonstate.FakedNUMAID: {Size: 4, Quota: -1}},
		"pod-fake-rp-b":       {commonstate.FakedNUMAID: {Size: 4, Quota: -1}},
		"pod-fake-unpinned-a": {commonstate.FakedNUMAID: {Size: 3, Quota: -1}},
		"pod-fake-unpinned-b": {commonstate.FakedNUMAID: {Size: 2, Quota: -1}},
	}

	for iteration := 0; iteration < 128; iteration++ {
		regionMap := make(map[string]region.QoSRegion, len(specs))
		for offset := range specs {
			index := offset
			if iteration%2 == 1 {
				index = len(specs) - 1 - offset
			}
			spec := specs[index]
			r := NewFakeRegion(spec.name, configapi.QoSRegionTypeDedicated, spec.owner)
			if spec.numa != nil {
				r.SetBindingNumas(machine.NewCPUSet(*spec.numa))
				r.SetIsNumaBinding(true)
			}
			r.enableReclaim = true
			r.podsRequest = 8
			r.SetPods(types.PodSet{spec.pod: sets.NewString("main", "sidecar")})
			r.SetProvision(types.ControlKnob{
				configapi.ControlKnobNonReclaimedCPURequirement: {Value: 4},
			})
			regionMap[r.Name()] = r
		}

		conf := generateTestConf(t, true, "")
		metaReader := metacache.NewDummyMetaCacheImp()
		require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{
			0: {
				"rp-real": {PinnedCPUSet: machine.MustParse("0-5")},
			},
			1: {
				"rp-a": {PinnedCPUSet: machine.MustParse("12-17")},
				"rp-b": {PinnedCPUSet: machine.MustParse("18-23")},
			},
		}))
		require.NoError(t, metaReader.SetPoolInfo(commonstate.PoolNameReserve, &types.PoolInfo{
			PoolName: commonstate.PoolNameReserve,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.MustParse("0,6"),
				1: machine.MustParse("12,18,24"),
			},
		}))
		reservedForReclaim := map[int]int{0: 2, 1: 3}
		rampUpReclaimCPUSetCap := map[int]int{}
		numaAvailable := map[int]int{0: 12, 1: 18}
		nonBindingNUMAs := machine.NewCPUSet(1)
		allowSharedOverlap := false
		disableDedicatedOverlap := true
		assembler := NewProvisionAssemblerCommon(
			conf, nil, &regionMap, &reservedForReclaim, &rampUpReclaimCPUSetCap, &numaAvailable, &nonBindingNUMAs,
			&allowSharedOverlap, &disableDedicatedOverlap, metaReader, nil, metrics.DummyMetrics{},
		)

		result, err := assembler.AssembleProvision(ProvisionContext{
			DynamicConfiguration: conf.GetDynamicConfiguration(),
		})
		require.NoError(t, err, "iteration %d", iteration)
		require.Equal(t, wantEntries, result.PoolEntries, "iteration %d", iteration)
		require.Empty(t, result.PoolOverlapInfo, "iteration %d", iteration)
		require.Empty(t, result.PoolOverlapPodContainerInfo, "iteration %d", iteration)
	}
}

func TestClampByReclaimedCPUMaxRatio(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		size               int
		limit              float64
		ratio              float64
		cpuCount           int
		reservedForReclaim int
		cpusPerCore        int
		wantSize           int
		wantLimit          float64
	}{
		{
			name:               "ratio disabled",
			size:               40,
			limit:              30,
			ratio:              0,
			cpuCount:           96,
			reservedForReclaim: 30,
			wantSize:           40,
			wantLimit:          30,
		},
		{
			name:               "floor fractional cap to even",
			size:               40,
			limit:              40,
			ratio:              0.3,
			cpuCount:           24,
			reservedForReclaim: 2,
			wantSize:           6,
			wantLimit:          6,
		},
		{
			name:               "size below cap is unchanged",
			size:               8,
			limit:              8,
			ratio:              0.5,
			cpuCount:           24,
			reservedForReclaim: 2,
			wantSize:           8,
			wantLimit:          8,
		},
		{
			name:               "negative limit sentinel is unchanged",
			size:               20,
			limit:              -1,
			ratio:              0.5,
			cpuCount:           24,
			reservedForReclaim: 2,
			wantSize:           12,
			wantLimit:          -1,
		},
		{
			name:               "cap equal to reserved is allowed",
			size:               8,
			limit:              8,
			ratio:              0.25,
			cpuCount:           8,
			reservedForReclaim: 2,
			wantSize:           2,
			wantLimit:          2,
		},
		{
			name:               "cap below reserved returns reserved",
			size:               8,
			limit:              8,
			ratio:              0.1,
			cpuCount:           8,
			reservedForReclaim: 3,
			wantSize:           3,
			wantLimit:          3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotSize, gotLimit, err := clampByReclaimedCPUMaxRatio(
				tt.size,
				tt.limit,
				tt.ratio,
				tt.cpuCount,
				tt.reservedForReclaim,
				general.Max(tt.cpusPerCore, 1),
			)
			require.NoError(t, err)
			require.Equal(t, tt.wantSize, gotSize)
			require.Equal(t, tt.wantLimit, gotLimit)
		})
	}
}

func TestClampByReclaimedCPUMaxRatioWithDiagnostics(t *testing.T) {
	tests := []struct {
		name               string
		size               int
		limit              float64
		ratio              float64
		cpuCount           int
		reservedForReclaim int
		cpusPerCore        int
		want               reclaimClampResult
	}{
		{
			name: "size is capped and aligned to even cores",
			size: 186, limit: -1, ratio: 0.3, cpuCount: 192, reservedForReclaim: 38,
			want: reclaimClampResult{RawSize: 186, FinalSize: 56, ReleasedSize: 130, FinalLimit: -1},
		},
		{
			name: "quota and size are both capped by existing helper",
			size: 186, limit: 120, ratio: 0.3, cpuCount: 192, reservedForReclaim: 38,
			want: reclaimClampResult{RawSize: 186, FinalSize: 56, ReleasedSize: 130, FinalLimit: 56},
		},
		{
			name: "ratio zero keeps no-cap semantics",
			size: 186, limit: -1, ratio: 0, cpuCount: 192, reservedForReclaim: 38,
			want: reclaimClampResult{RawSize: 186, FinalSize: 186, ReleasedSize: 0, FinalLimit: -1},
		},
		{
			name: "hard reclaim cap derives complete physical core target",
			size: 96, limit: 96, ratio: 0.2, cpuCount: 96, reservedForReclaim: 16, cpusPerCore: 2,
			want: reclaimClampResult{RawSize: 96, FinalSize: 18, ReleasedSize: 78, FinalLimit: 18},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := clampByReclaimedCPUMaxRatioWithDiagnostics(
				tc.size, tc.limit, tc.ratio, tc.cpuCount, tc.reservedForReclaim,
				general.Max(tc.cpusPerCore, 1),
			)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestReclaimPoolRampUpCapAppliedAsUpperBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		capByNUMA          map[int]int
		numas              machine.CPUSet
		size               int
		limit              float64
		reservedForReclaim int
		wantSize           int
		wantLimit          float64
	}{
		{
			name:               "caps size above configured per-numa upper bound",
			capByNUMA:          map[int]int{0: 12},
			numas:              machine.NewCPUSet(0),
			size:               20,
			limit:              20,
			reservedForReclaim: 8,
			wantSize:           12,
			wantLimit:          12,
		},
		{
			name:               "zero cap keeps original size and quota",
			capByNUMA:          map[int]int{0: 0},
			numas:              machine.NewCPUSet(0),
			size:               20,
			limit:              20,
			reservedForReclaim: 8,
			wantSize:           20,
			wantLimit:          20,
		},
		{
			name:               "missing cap keeps original size and quota",
			capByNUMA:          map[int]int{},
			numas:              machine.NewCPUSet(0),
			size:               20,
			limit:              20,
			reservedForReclaim: 8,
			wantSize:           20,
			wantLimit:          20,
		},
		{
			name:               "global scope uses sum only when every spanned numa has cap",
			capByNUMA:          map[int]int{0: 12, 1: 10},
			numas:              machine.NewCPUSet(0, 1),
			size:               30,
			limit:              25,
			reservedForReclaim: 8,
			wantSize:           22,
			wantLimit:          22,
		},
		{
			name:               "global scope ignores cap when any spanned numa is missing",
			capByNUMA:          map[int]int{0: 12},
			numas:              machine.NewCPUSet(0, 1),
			size:               30,
			limit:              25,
			reservedForReclaim: 8,
			wantSize:           30,
			wantLimit:          25,
		},
		{
			name:               "reserved floor wins over lower cap",
			capByNUMA:          map[int]int{0: 12},
			numas:              machine.NewCPUSet(0),
			size:               20,
			limit:              20,
			reservedForReclaim: 14,
			wantSize:           14,
			wantLimit:          14,
		},
		{
			name:               "negative quota sentinel is preserved",
			capByNUMA:          map[int]int{0: 12},
			numas:              machine.NewCPUSet(0),
			size:               20,
			limit:              -1,
			reservedForReclaim: 8,
			wantSize:           12,
			wantLimit:          -1,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pa := &ProvisionAssemblerCommon{
				rampUpReclaimCPUSetCap: &tc.capByNUMA,
			}
			gotSize, gotLimit := pa.applyRampUpReclaimCap(
				tc.size,
				tc.limit,
				tc.numas,
				tc.reservedForReclaim,
			)
			require.Equal(t, tc.wantSize, gotSize)
			require.Equal(t, tc.wantLimit, gotLimit)
		})
	}
}

func TestReclaimConstraintScopeConstructors(t *testing.T) {
	t.Parallel()

	require.Equal(t, ReclaimConstraintScope("non-exclusive/-1"), NewNonExclusiveReclaimConstraintScope(-1))
	require.Equal(t, ReclaimConstraintScope("exclusive/region-a"), NewExclusiveReclaimConstraintScope("region-a"))
	require.Equal(t, ReclaimConstraintScope("legacy-exclusive/region-b"), NewLegacyExclusiveReclaimConstraintScope("region-b"))
}

func TestApplyReclaimConstraint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		constraint         ReclaimConstraint
		size               int
		limit              float64
		reservedForReclaim int
		ceilings           map[ReclaimConstraintScope]int
		wantSize           int
		wantLimit          float64
		wantExcess         int
	}{
		{
			name:               "reserved floor caps reclaim above floor",
			constraint:         ReclaimConstraintReservedFloor,
			size:               38,
			limit:              -1,
			reservedForReclaim: 24,
			wantSize:           24,
			wantLimit:          -1,
			wantExcess:         14,
		},
		{
			name:               "reserved floor caps quota with size",
			constraint:         ReclaimConstraintReservedFloor,
			size:               38,
			limit:              38,
			reservedForReclaim: 24,
			wantSize:           24,
			wantLimit:          24,
			wantExcess:         14,
		},
		{
			name:               "reserved floor reports convergence at floor",
			constraint:         ReclaimConstraintReservedFloor,
			size:               24,
			limit:              24,
			reservedForReclaim: 24,
			wantSize:           24,
			wantLimit:          24,
			wantExcess:         0,
		},
		{
			name:               "dynamic ceiling permits one bounded step above floor",
			constraint:         ReclaimConstraintReservedFloor,
			size:               38,
			limit:              38,
			reservedForReclaim: 24,
			ceilings:           map[ReclaimConstraintScope]int{ReclaimConstraintScope("scope"): 34},
			wantSize:           34,
			wantLimit:          34,
			wantExcess:         14,
		},
		{
			name:               "none keeps calculated reclaim",
			constraint:         ReclaimConstraintNone,
			size:               38,
			limit:              38,
			reservedForReclaim: 24,
			wantSize:           38,
			wantLimit:          38,
			wantExcess:         0,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotSize, gotLimit, gotExcess := ApplyReclaimConstraint(
				ReclaimConstraintScope("scope"),
				tc.size,
				tc.limit,
				tc.reservedForReclaim,
				tc.constraint,
				tc.ceilings,
			)
			require.Equal(t, tc.wantSize, gotSize)
			require.Equal(t, tc.wantLimit, gotLimit)
			require.Equal(t, tc.wantExcess, gotExcess)
		})
	}
}

func TestRecordReclaimConstraintTargetKeepsScopesAndMaximumExcess(t *testing.T) {
	t.Parallel()

	result := &types.InternalCPUCalculationResult{}
	RecordReclaimConstraintTarget(result, ReclaimConstraintReservedFloor, NewNonExclusiveReclaimConstraintScope(0), 28, 24, 4)
	RecordReclaimConstraintTarget(result, ReclaimConstraintReservedFloor, NewExclusiveReclaimConstraintScope("a"), 18, 4, 14)
	RecordReclaimConstraintTarget(result, ReclaimConstraintReservedFloor, NewLegacyExclusiveReclaimConstraintScope("b"), 10, 4, 6)

	require.Equal(t, 14, result.ReclaimConstraintExcess)
	require.Equal(t, map[string]types.ReclaimConstraintTarget{
		"non-exclusive/0":    {Desired: 28, Floor: 24},
		"exclusive/a":        {Desired: 18, Floor: 4},
		"legacy-exclusive/b": {Desired: 10, Floor: 4},
	}, result.ReclaimConstraintTargets)
}

func TestDefaultShareBackfillDiagnosticsWhenRatioDisabled(t *testing.T) {
	t.Parallel()

	pa := newDefaultShareAssembler(t, map[int]int{0: 20}, machine.NewCPUSet(0), nil,
		map[int]int{0: 2}, false, true, nil)
	pa.conf.GetDynamicConfiguration().EnableReclaim = true
	pa.conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio = 0
	result := newDefaultShareResult(true)
	result.DisableDedicatedCoresOverlapReclaimedCores = true

	err := pa.assembleWithoutNUMAExclusivePool(
		NewRegionMapHelper(*pa.regionMap),
		commonstate.FakedNUMAID,
		result,
	)
	require.NoError(t, err)
	require.Equal(t, 20, result.DefaultShareBackfill.RawReclaimSize)
	require.Equal(t, 20, result.DefaultShareBackfill.FinalReclaimSize)
	require.Zero(t, result.DefaultShareBackfill.ReleasedReclaimSize)
}

func TestDefaultShareBackfillHardRatioMatchesCompletePhysicalCoreTarget(t *testing.T) {
	t.Parallel()

	pa := newDefaultShareAssembler(t, map[int]int{0: 96}, machine.NewCPUSet(0), nil,
		map[int]int{0: 16}, false, true, nil)
	pa.metaServer.CPUTopology.NumCores = 48
	dynamicConf := pa.conf.GetDynamicConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.EnableRampUpReclaimHardPartition = true
	dynamicConf.ReclaimedCPUMaxRatio = 0.2
	dynamicConf.FillDefaultSharePoolWithNonReclaimCPUs = true
	(*pa.rampUpReclaimCPUSetCap)[0] = 18

	result, err := pa.AssembleProvision(pa.calculationContext)
	require.NoError(t, err)
	require.Equal(t, 96, result.DefaultShareBackfill.RawReclaimSize)
	require.Equal(t, 18, result.DefaultShareBackfill.FinalReclaimSize)
	require.Equal(t, 78, result.DefaultShareBackfill.ReleasedReclaimSize)
	require.Equal(t, types.CPUResource{Size: 96, Quota: -1},
		result.PoolEntries[commonstate.PoolNameShare][commonstate.FakedNUMAID])
}

func TestDefaultShareBackfillReservedFloorConstraintReportsClamp(t *testing.T) {
	t.Parallel()

	pa := newDefaultShareAssembler(t, map[int]int{0: 128}, machine.NewCPUSet(0), nil,
		map[int]int{0: 24}, false, true, nil)
	pa.metaServer.CPUTopology.NumCores = 64
	dynamicConf := dynamic.NewConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.EnableRampUpReclaimHardPartition = true
	dynamicConf.ReclaimedCPUMaxRatio = 0.3
	dynamicConf.FillDefaultSharePoolWithNonReclaimCPUs = true
	(*pa.rampUpReclaimCPUSetCap)[0] = 38

	result, err := pa.AssembleProvision(ProvisionContext{
		DynamicConfiguration: dynamicConf,
		ReclaimConstraint:    ReclaimConstraintReservedFloor,
		ReclaimCeilings:      map[ReclaimConstraintScope]int{NewNonExclusiveReclaimConstraintScope(-1): 34},
	})
	require.NoError(t, err)
	require.Zero(t, result.ReclaimConstraintExcess)
	require.Equal(t, types.ReclaimConstraintTarget{Desired: 38, Floor: 38},
		result.ReclaimConstraintTargets["non-exclusive/-1"])
	require.Equal(t, 128, result.DefaultShareBackfill.RawReclaimSize)
	require.Equal(t, 38, result.DefaultShareBackfill.FinalReclaimSize)
	require.Equal(t, 90, result.DefaultShareBackfill.ReleasedReclaimSize)
	require.Equal(t, types.CPUResource{Size: 0, Quota: -1},
		result.PoolEntries[commonstate.PoolNameReclaim][commonstate.FakedNUMAID])
	require.Equal(t, types.CPUResource{Size: 128, Quota: -1},
		result.PoolEntries[commonstate.PoolNameShare][commonstate.FakedNUMAID])
}

func TestDefaultShareBackfillReleasedAccumulatesAcrossScopes(t *testing.T) {
	t.Parallel()

	// Build a non-exclusive assembler whose reclaim pool is larger than the
	// ratio cap, so each pass through assembleWithoutNUMAExclusivePool releases
	// cores that must be backfilled into the default share diagnostics.
	//
	// With EnableReclaim and no regions, calculateReclaimPool yields
	// reclaimedCoresSize == available (20). The ratio cap is
	// floor(0.5*20)=10 (even, above reserved=2), so each pass raw=20,
	// final=10, released=10. Driving the real production accumulation twice
	// (two non-exclusive scopes) must add up across scopes rather than
	// overwrite; this fails if the three "+=" writes in
	// assembleWithoutNUMAExclusivePool are removed.
	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	conf.GetDynamicConfiguration().EnableReclaim = true
	conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio = 0.5

	const cpuCount = 20
	cpuDetails := machine.CPUDetails{}
	for cpuID := 0; cpuID < cpuCount; cpuID++ {
		cpuDetails[cpuID] = machine.CPUTopoInfo{NUMANodeID: 0}
	}
	metaServer := &metaserver.MetaServer{
		MetaAgent: &metaagent.MetaAgent{
			KatalystMachineInfo: &machine.KatalystMachineInfo{
				CPUTopology: &machine.CPUTopology{
					NumCPUs:      cpuCount,
					NumCores:     cpuCount,
					NumSockets:   1,
					NumNUMANodes: 1,
					CPUDetails:   cpuDetails,
				},
			},
		},
	}
	metaReader := metacache.NewDummyMetaCacheImp()

	regionMap := map[string]region.QoSRegion{}
	reservedForReclaim := map[int]int{0: 2}
	rampUpReclaimCPUSetCap := map[int]int{}
	numaAvailable := map[int]int{0: cpuCount}
	nonBindingNUMAs := machine.NewCPUSet(0)
	allowOverlap := true
	disableDedicatedOverlap := false
	pa := NewProvisionAssemblerCommon(
		conf,
		nil,
		&regionMap,
		&reservedForReclaim,
		&rampUpReclaimCPUSetCap,
		&numaAvailable,
		&nonBindingNUMAs,
		&allowOverlap,
		&disableDedicatedOverlap,
		metaReader,
		metaServer,
		metrics.DummyMetrics{},
	).(*ProvisionAssemblerCommon)

	result := &types.InternalCPUCalculationResult{
		PoolEntries:                 map[string]map[int]types.CPUResource{},
		PoolOverlapInfo:             map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
	}
	regionHelper := NewRegionMapHelper(regionMap)

	// first non-exclusive scope
	require.NoError(t, pa.assembleWithoutNUMAExclusivePool(regionHelper, commonstate.FakedNUMAID, result))
	require.Equal(t, 20, result.DefaultShareBackfill.RawReclaimSize)
	require.Equal(t, 10, result.DefaultShareBackfill.FinalReclaimSize)
	require.Equal(t, 10, result.DefaultShareBackfill.ReleasedReclaimSize)

	// second non-exclusive scope must accumulate on top of the first
	require.NoError(t, pa.assembleWithoutNUMAExclusivePool(regionHelper, commonstate.FakedNUMAID, result))
	require.Equal(t, 40, result.DefaultShareBackfill.RawReclaimSize)
	require.Equal(t, 20, result.DefaultShareBackfill.FinalReclaimSize)
	require.Equal(t, 20, result.DefaultShareBackfill.ReleasedReclaimSize)
}

func TestAssembleProvisionUsesReservedWhenEvenRatioCapIsLower(t *testing.T) {
	t.Parallel()

	newAssembler := func(t *testing.T, regionMap map[string]region.QoSRegion, nonBindingNUMAs machine.CPUSet) *ProvisionAssemblerCommon {
		t.Helper()

		conf, err := options.NewOptions().Config()
		require.NoError(t, err)
		conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio = 0.1

		cpuDetails := machine.CPUDetails{}
		for cpuID := 0; cpuID < 8; cpuID++ {
			cpuDetails[cpuID] = machine.CPUTopoInfo{NUMANodeID: 0}
		}
		metaServer := &metaserver.MetaServer{
			MetaAgent: &metaagent.MetaAgent{
				KatalystMachineInfo: &machine.KatalystMachineInfo{
					CPUTopology: &machine.CPUTopology{
						NumCPUs:      8,
						NumCores:     8,
						NumSockets:   1,
						NumNUMANodes: 1,
						CPUDetails:   cpuDetails,
					},
				},
			},
		}

		metaReader := metacache.NewDummyMetaCacheImp()

		reservedForReclaim := map[int]int{0: 2}
		rampUpReclaimCPUSetCap := map[int]int{}
		numaAvailable := map[int]int{0: 8}
		allowOverlap := true
		disableDedicatedOverlap := false
		return NewProvisionAssemblerCommon(
			conf,
			nil,
			&regionMap,
			&reservedForReclaim,
			&rampUpReclaimCPUSetCap,
			&numaAvailable,
			&nonBindingNUMAs,
			&allowOverlap,
			&disableDedicatedOverlap,
			metaReader,
			metaServer,
			metrics.DummyMetrics{},
		).(*ProvisionAssemblerCommon)
	}

	t.Run("ordinary NUMA pool", func(t *testing.T) {
		t.Parallel()

		pa := newAssembler(t, map[string]region.QoSRegion{}, machine.NewCPUSet(0))
		result, err := pa.AssembleProvision(pa.calculationContext)
		require.NoError(t, err)
		require.Equal(t, 2, result.PoolEntries[commonstate.PoolNameReclaim][commonstate.FakedNUMAID].Size)
	})

	t.Run("dedicated NUMA-exclusive region", func(t *testing.T) {
		t.Parallel()

		dedicatedRegion := NewFakeRegion("dedicated-exclusive", configapi.QoSRegionTypeDedicated, "dedicated-exclusive")
		dedicatedRegion.SetBindingNumas(machine.NewCPUSet(0))
		dedicatedRegion.SetIsNumaBinding(true)
		dedicatedRegion.isNumaExclusive = true
		dedicatedRegion.SetPods(types.PodSet{"pod": {"container": {}}})
		regionMap := map[string]region.QoSRegion{dedicatedRegion.Name(): dedicatedRegion}

		pa := newAssembler(t, regionMap, machine.NewCPUSet())
		result := &types.InternalCPUCalculationResult{
			PoolEntries:                 map[string]map[int]types.CPUResource{},
			PoolOverlapInfo:             map[string]map[int]map[string]int{},
			PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
		}
		err := pa.assembleDedicatedNUMAExclusiveRegion(dedicatedRegion, result)
		require.NoError(t, err)
		require.Equal(t, 2, result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod"]["container"])
	})
}

func newExclusiveAssemblerFixture(
	t *testing.T,
	capacity, reserved int,
	enableReclaim, disableDedicatedOverlap bool,
) (*ProvisionAssemblerCommon, *FakeRegion, *types.InternalCPUCalculationResult, *metacache.MetaCacheImp) {
	t.Helper()

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)

	cpuDetails := machine.CPUDetails{}
	for cpuID := 0; cpuID < capacity; cpuID++ {
		cpuDetails[cpuID] = machine.CPUTopoInfo{NUMANodeID: 0}
	}
	metaServer := &metaserver.MetaServer{
		MetaAgent: &metaagent.MetaAgent{
			KatalystMachineInfo: &machine.KatalystMachineInfo{
				CPUTopology: &machine.CPUTopology{
					NumCPUs:      capacity,
					NumCores:     capacity,
					NumSockets:   1,
					NumNUMANodes: 1,
					CPUDetails:   cpuDetails,
				},
			},
		},
	}
	metaReader := metacache.NewDummyMetaCacheImp()
	require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{}))

	exclusiveRegion := NewFakeRegion(
		"dedicated-exclusive",
		configapi.QoSRegionTypeDedicated,
		"dedicated-exclusive",
	)
	exclusiveRegion.SetBindingNumas(machine.NewCPUSet(0))
	exclusiveRegion.SetIsNumaBinding(true)
	exclusiveRegion.isNumaExclusive = true
	exclusiveRegion.enableReclaim = enableReclaim
	exclusiveRegion.SetPods(types.PodSet{
		"pod":       sets.NewString("main", "sidecar"),
		"other-pod": sets.NewString("main"),
	})

	regionMap := map[string]region.QoSRegion{exclusiveRegion.Name(): exclusiveRegion}
	reservedForReclaim := map[int]int{0: reserved}
	rampUpReclaimCPUSetCap := map[int]int{}
	numaAvailable := map[int]int{0: capacity}
	nonBindingNUMAs := machine.NewCPUSet()
	allowSharedOverlap := true
	pa := NewProvisionAssemblerCommon(
		conf,
		nil,
		&regionMap,
		&reservedForReclaim,
		&rampUpReclaimCPUSetCap,
		&numaAvailable,
		&nonBindingNUMAs,
		&allowSharedOverlap,
		&disableDedicatedOverlap,
		metaReader,
		metaServer,
		metrics.DummyMetrics{},
	).(*ProvisionAssemblerCommon)

	result := &types.InternalCPUCalculationResult{
		PoolEntries:                 map[string]map[int]types.CPUResource{},
		PoolOverlapInfo:             map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
		DisableDedicatedCoresOverlapReclaimedCores: disableDedicatedOverlap,
	}
	return pa, exclusiveRegion, result, metaReader
}

func TestGetExclusivePartitionCapacitiesUsesActualAvailableCPUSet(t *testing.T) {
	tests := []struct {
		name                   string
		ownerPackage           string
		disableReclaimSelector string
		packages               map[string]*types.ResourcePackageState
		wantPartition          int
		wantDedicated          int
		wantReclaim            int
	}{
		{
			name:         "pinned owner partially overlaps reserve and forbidden pools",
			ownerPackage: "pinned",
			packages: map[string]*types.ResourcePackageState{
				"pinned": {PinnedCPUSet: machine.MustParse("0-7")},
			},
			wantPartition: 12,
			wantDedicated: 4,
			wantReclaim:   12,
		},
		{
			name: "unpinned owner excludes only available pinned CPUs",
			packages: map[string]*types.ResourcePackageState{
				"pinned": {PinnedCPUSet: machine.MustParse("0-7")},
			},
			wantPartition: 12,
			wantDedicated: 8,
			wantReclaim:   12,
		},
		{
			name:                   "non reclaimable pinned CPUs are intersected with availability",
			ownerPackage:           "protected",
			disableReclaimSelector: "disable-reclaim=true",
			packages: map[string]*types.ResourcePackageState{
				"protected": {
					Attributes:   map[string]string{"disable-reclaim": "true"},
					PinnedCPUSet: machine.MustParse("0-7"),
				},
			},
			wantPartition: 12,
			wantDedicated: 4,
			wantReclaim:   8,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			pa, exclusiveRegion, _, metaReader := newExclusiveAssemblerFixture(t, 16, 0, true, true)
			pa.conf.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector = tt.disableReclaimSelector
			(*pa.numaAvailable)[0] = 12
			exclusiveRegion.ownerPoolName = "dedicated-exclusive"
			if tt.ownerPackage != "" {
				exclusiveRegion.ownerPoolName = tt.ownerPackage + "/dedicated-exclusive"
			}

			require.NoError(t, metaReader.SetPoolInfo(commonstate.PoolNameReserve, &types.PoolInfo{
				PoolName: commonstate.PoolNameReserve,
				TopologyAwareAssignments: map[int]machine.CPUSet{
					0: machine.MustParse("0-1"),
				},
			}))
			require.NoError(t, metaReader.SetPoolInfo(commonstate.PoolNameInterrupt, &types.PoolInfo{
				PoolName: commonstate.PoolNameInterrupt,
				TopologyAwareAssignments: map[int]machine.CPUSet{
					0: machine.MustParse("2-3"),
				},
			}))
			require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{0: tt.packages}))

			partition, dedicated, reclaim, err := pa.getExclusivePartitionCapacities(exclusiveRegion, 0, 12)
			require.NoError(t, err)
			require.Equal(t, tt.wantPartition, partition)
			require.Equal(t, tt.wantDedicated, dedicated)
			require.Equal(t, tt.wantReclaim, reclaim)
		})
	}

	t.Run("system pool is excluded from available CPUSet", func(t *testing.T) {
		pa, exclusiveRegion, _, metaReader := newExclusiveAssemblerFixture(t, 16, 0, true, true)
		(*pa.numaAvailable)[0] = 12
		require.NoError(t, metaReader.SetPoolInfo("system-test", &types.PoolInfo{
			PoolName: "system-test",
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.MustParse("0-3"),
			},
		}))

		partition, dedicated, reclaim, err := pa.getExclusivePartitionCapacities(exclusiveRegion, 0, 12)
		require.NoError(t, err)
		require.Equal(t, 12, partition)
		require.Equal(t, 12, dedicated)
		require.Equal(t, 12, reclaim)
	})

	t.Run("available CPUSet size must match numaAvailable", func(t *testing.T) {
		pa, exclusiveRegion, _, metaReader := newExclusiveAssemblerFixture(t, 16, 0, true, true)
		require.NoError(t, metaReader.SetPoolInfo(commonstate.PoolNameReserve, &types.PoolInfo{
			PoolName: commonstate.PoolNameReserve,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.MustParse("0-1"),
			},
		}))
		require.NoError(t, metaReader.SetPoolInfo(commonstate.PoolNameInterrupt, &types.PoolInfo{
			PoolName: commonstate.PoolNameInterrupt,
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.MustParse("2-3"),
			},
		}))

		_, _, _, err := pa.getExclusivePartitionCapacities(exclusiveRegion, 0, 11)
		require.ErrorContains(t, err, "does not match numaAvailable")
	})
}

func TestAssembleDedicatedNUMAExclusiveRegionDisjoint(t *testing.T) {
	t.Run("enable reclaim emits one dedicated entry per pod and standalone reclaim", func(t *testing.T) {
		pa, exclusiveRegion, result, _ := newExclusiveAssemblerFixture(t, 16, 4, true, true)
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
		})

		require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Equal(t, types.CPUResource{Size: 10, Quota: -1}, result.PoolEntries["pod"][0])
		require.Equal(t, types.CPUResource{Size: 10, Quota: -1}, result.PoolEntries["other-pod"][0])
		require.Equal(t, types.CPUResource{Size: 6, Quota: -1},
			result.PoolEntries[commonstate.PoolNameReclaim][0])
		require.Empty(t, result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0])
	})

	t.Run("disable reclaim retains only reserve", func(t *testing.T) {
		pa, exclusiveRegion, result, _ := newExclusiveAssemblerFixture(t, 16, 4, false, true)
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
		})

		require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Equal(t, types.CPUResource{Size: 12, Quota: -1}, result.PoolEntries["pod"][0])
		require.Equal(t, types.CPUResource{Size: 4, Quota: -1},
			result.PoolEntries[commonstate.PoolNameReclaim][0])
	})

	t.Run("quota limits only reclaim block", func(t *testing.T) {
		pa, exclusiveRegion, result, metaReader := newExclusiveAssemblerFixture(t, 16, 4, true, true)
		require.NoError(t, metaReader.SetSupportedWantedFeatureGates(
			finders.FeatureGateTypeCPU,
			map[string]*advisorsvc.FeatureGate{
				feature_cpu.NegotiationFeatureGateQuotaCtrlKnob: {
					Name: feature_cpu.NegotiationFeatureGateQuotaCtrlKnob,
					Type: finders.FeatureGateTypeCPU,
				},
			},
		))
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
			configapi.ControlKnobReclaimedCoresCPUQuota:     {Value: 0},
		})

		require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Equal(t, types.CPUResource{Size: 10, Quota: -1}, result.PoolEntries["pod"][0])
		require.Equal(t, types.CPUResource{Size: 6, Quota: 0},
			result.PoolEntries[commonstate.PoolNameReclaim][0])
	})

	t.Run("ramp up cap limits reclaim block and quota", func(t *testing.T) {
		pa, exclusiveRegion, result, metaReader := newExclusiveAssemblerFixture(t, 16, 4, true, true)
		(*pa.rampUpReclaimCPUSetCap)[0] = 5
		require.NoError(t, metaReader.SetSupportedWantedFeatureGates(
			finders.FeatureGateTypeCPU,
			map[string]*advisorsvc.FeatureGate{
				feature_cpu.NegotiationFeatureGateQuotaCtrlKnob: {
					Name: feature_cpu.NegotiationFeatureGateQuotaCtrlKnob,
					Type: finders.FeatureGateTypeCPU,
				},
			},
		))
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
			configapi.ControlKnobReclaimedCoresCPUQuota:     {Value: 6},
		})

		require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Equal(t, types.CPUResource{Size: 11, Quota: -1}, result.PoolEntries["pod"][0])
		require.Equal(t, types.CPUResource{Size: 11, Quota: -1}, result.PoolEntries["other-pod"][0])
		require.Equal(t, types.CPUResource{Size: 5, Quota: 5},
			result.PoolEntries[commonstate.PoolNameReclaim][0])
		require.Equal(t, 16, result.PoolEntries["pod"][0].Size+result.PoolEntries[commonstate.PoolNameReclaim][0].Size)
	})

	t.Run("dynamic ceiling caps disjoint reclaim and reports target", func(t *testing.T) {
		pa, exclusiveRegion, result, _ := newExclusiveAssemblerFixture(t, 16, 4, true, true)
		pa.calculationContext.ReclaimConstraint = ReclaimConstraintReservedFloor
		pa.calculationContext.ReclaimCeilings = map[ReclaimConstraintScope]int{
			NewExclusiveReclaimConstraintScope("dedicated-exclusive"): 5,
		}
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
		})

		require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Equal(t, 2, result.ReclaimConstraintExcess)
		require.Equal(t, types.ReclaimConstraintTarget{Desired: 6, Floor: 4},
			result.ReclaimConstraintTargets["exclusive/dedicated-exclusive"])
		require.Equal(t, types.CPUResource{Size: 11, Quota: -1}, result.PoolEntries["pod"][0])
		require.Equal(t, types.CPUResource{Size: 5, Quota: -1},
			result.PoolEntries[commonstate.PoolNameReclaim][0])
	})

	t.Run("dynamic ceiling caps legacy overlap reclaim and reports target", func(t *testing.T) {
		pa, exclusiveRegion, result, _ := newExclusiveAssemblerFixture(t, 16, 4, true, false)
		pa.calculationContext.ReclaimConstraint = ReclaimConstraintReservedFloor
		pa.calculationContext.ReclaimCeilings = map[ReclaimConstraintScope]int{
			NewLegacyExclusiveReclaimConstraintScope("dedicated-exclusive"): 5,
		}
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
		})

		require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Equal(t, 2, result.ReclaimConstraintExcess)
		require.Equal(t, types.ReclaimConstraintTarget{Desired: 6, Floor: 4},
			result.ReclaimConstraintTargets["legacy-exclusive/dedicated-exclusive"])
		require.Equal(t, 5,
			result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod"]["main"])
	})

	t.Run("ramp up cap requiring dedicated beyond package capacity is rejected", func(t *testing.T) {
		pa, exclusiveRegion, result, metaReader := newExclusiveAssemblerFixture(t, 16, 4, true, true)
		(*pa.rampUpReclaimCPUSetCap)[0] = 5
		exclusiveRegion.ownerPoolName = "dedicated-pkg/dedicated-exclusive"
		require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{
			0: {
				"dedicated-pkg": {
					PinnedCPUSet: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
				},
			},
		}))
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
		})

		err := pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result)
		require.Error(t, err)
		require.Equal(t, strings.ToLower(err.Error()), err.Error())
		require.ErrorContains(t, err, "dedicated target")
		require.Empty(t, result.PoolEntries)
	})

	t.Run("ratio clamps physical reclaim target", func(t *testing.T) {
		pa, exclusiveRegion, result, _ := newExclusiveAssemblerFixture(t, 16, 4, true, true)
		pa.conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio = 0.25
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
		})

		require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Equal(t, types.CPUResource{Size: 12, Quota: -1}, result.PoolEntries["pod"][0])
		require.Equal(t, types.CPUResource{Size: 4, Quota: -1},
			result.PoolEntries[commonstate.PoolNameReclaim][0])
	})

	t.Run("ratio clamp derives complete physical core target", func(t *testing.T) {
		pa, exclusiveRegion, result, _ := newExclusiveAssemblerFixture(t, 96, 16, true, true)
		pa.metaServer.CPUTopology.NumCores = 48
		pa.conf.GetDynamicConfiguration().EnableReclaim = true
		pa.conf.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
		pa.conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio = 0.2
		(*pa.rampUpReclaimCPUSetCap)[0] = 18
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 60},
		})

		require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Equal(t, types.CPUResource{Size: 18, Quota: -1},
			result.PoolEntries[commonstate.PoolNameReclaim][0])
		require.Equal(t, types.CPUResource{Size: 78, Quota: -1}, result.PoolEntries["pod"][0])
	})

	t.Run("empty dedicated target is rejected", func(t *testing.T) {
		pa, exclusiveRegion, result, _ := newExclusiveAssemblerFixture(t, 16, 4, true, true)
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 0},
		})

		require.Error(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Empty(t, result.PoolEntries)
	})

	t.Run("ratio below resource package eligibility is rejected", func(t *testing.T) {
		pa, exclusiveRegion, result, metaReader := newExclusiveAssemblerFixture(t, 16, 4, true, true)
		pa.conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio = 0.25
		exclusiveRegion.ownerPoolName = "dedicated-pkg/dedicated-exclusive"
		require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{
			0: {
				"dedicated-pkg": {
					PinnedCPUSet: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
				},
			},
		}))
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
		})

		require.Error(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Empty(t, result.PoolEntries)
	})

	t.Run("resource package dedicated capacity below target is rejected", func(t *testing.T) {
		pa, exclusiveRegion, result, metaReader := newExclusiveAssemblerFixture(t, 16, 4, false, true)
		exclusiveRegion.ownerPoolName = "dedicated-pkg/dedicated-exclusive"
		require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{
			0: {
				"dedicated-pkg": {
					PinnedCPUSet: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
				},
			},
		}))
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
		})

		require.Error(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Empty(t, result.PoolEntries)
	})

	t.Run("non reclaimable pinned CPUs are excluded from partition eligibility", func(t *testing.T) {
		pa, exclusiveRegion, result, metaReader := newExclusiveAssemblerFixture(t, 16, 4, true, true)
		pa.conf.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector = "disable-reclaim=true"
		require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{
			0: {
				"protected": {
					Attributes:   map[string]string{"disable-reclaim": "true"},
					PinnedCPUSet: machine.NewCPUSet(0, 1, 2, 3),
				},
			},
		}))
		exclusiveRegion.SetProvision(types.ControlKnob{
			configapi.ControlKnobNonReclaimedCPURequirement: {Value: 8},
		})

		require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
		require.Equal(t, types.CPUResource{Size: 8, Quota: -1}, result.PoolEntries["pod"][0])
		require.Equal(t, types.CPUResource{Size: 4, Quota: -1},
			result.PoolEntries[commonstate.PoolNameReclaim][0])
	})
}

func TestAssembleDedicatedNUMAExclusiveRegionLegacyGolden(t *testing.T) {
	pa, exclusiveRegion, result, _ := newExclusiveAssemblerFixture(t, 16, 4, true, false)
	exclusiveRegion.SetProvision(types.ControlKnob{
		configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
	})

	require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
	require.NotContains(t, result.PoolEntries, "pod")
	require.Equal(t, types.CPUResource{Size: 0, Quota: -1},
		result.PoolEntries[commonstate.PoolNameReclaim][0])
	require.Equal(t, 6,
		result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod"]["main"])
	require.Equal(t, 6,
		result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod"]["sidecar"])
}

func TestAssembleDedicatedNUMAExclusiveRegionLegacyRampUpCap(t *testing.T) {
	pa, exclusiveRegion, result, metaReader := newExclusiveAssemblerFixture(t, 16, 4, true, false)
	(*pa.rampUpReclaimCPUSetCap)[0] = 5
	require.NoError(t, metaReader.SetSupportedWantedFeatureGates(
		finders.FeatureGateTypeCPU,
		map[string]*advisorsvc.FeatureGate{
			feature_cpu.NegotiationFeatureGateQuotaCtrlKnob: {
				Name: feature_cpu.NegotiationFeatureGateQuotaCtrlKnob,
				Type: finders.FeatureGateTypeCPU,
			},
		},
	))
	exclusiveRegion.SetProvision(types.ControlKnob{
		configapi.ControlKnobNonReclaimedCPURequirement: {Value: 10},
		configapi.ControlKnobReclaimedCoresCPUQuota:     {Value: 6},
	})

	require.NoError(t, pa.assembleDedicatedNUMAExclusiveRegion(exclusiveRegion, result))
	require.Equal(t, types.CPUResource{Size: 0, Quota: 5},
		result.PoolEntries[commonstate.PoolNameReclaim][0])
	require.Equal(t, 5,
		result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod"]["main"])
	require.Equal(t, 5,
		result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim][0]["pod"]["sidecar"])
}

func TestCalculateDefaultShareTargetSize(t *testing.T) {
	tests := []struct {
		name    string
		budget  map[int]defaultShareNUMABudget
		want    int
		wantErr string
	}{
		{
			name: "reclaim quantity does not lower qrm residual upper bound",
			budget: map[int]defaultShareNUMABudget{
				0: {UnpinnedAllocatableSize: 95, FinalUnpinnedReclaimSize: 28},
				1: {UnpinnedAllocatableSize: 95, FinalUnpinnedReclaimSize: 28},
			},
			want: 190,
		},
		{
			name: "fixed pools do not lower qrm residual upper bound",
			budget: map[int]defaultShareNUMABudget{
				0: {UnpinnedAllocatableSize: 91, FinalUnpinnedReclaimSize: 28, FixedUnpinnedPoolSize: 10},
				1: {UnpinnedAllocatableSize: 91, FinalUnpinnedReclaimSize: 28, FixedUnpinnedPoolSize: 10},
			},
			want: 182,
		},
		{
			name: "exclusive numa ignores nested reclaim and pinned quantities",
			budget: map[int]defaultShareNUMABudget{
				0: {UnpinnedAllocatableSize: 95, FinalUnpinnedReclaimSize: 28},
				1: {UnpinnedAllocatableSize: 80, FinalUnpinnedReclaimSize: 20, FixedUnpinnedPoolSize: 8, Exclusive: true},
			},
			want: 95,
		},
		{
			name: "fixed pool overcommit does not lower qrm residual upper bound",
			budget: map[int]defaultShareNUMABudget{
				0: {UnpinnedAllocatableSize: 30, FinalUnpinnedReclaimSize: 28, FixedUnpinnedPoolSize: 8},
				1: {UnpinnedAllocatableSize: 86, FinalUnpinnedReclaimSize: 28},
			},
			want: 116,
		},
		{
			name: "reclaim cannot exceed allocatable budget",
			budget: map[int]defaultShareNUMABudget{
				0: {UnpinnedAllocatableSize: 16, FinalUnpinnedReclaimSize: 18, FixedUnpinnedPoolSize: 1},
			},
			wantErr: "default share reclaim exceeds unpinned allocatable",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := calculateDefaultShareTargetSize(tc.budget)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// newDefaultShareAssembler builds a ProvisionAssemblerCommon suitable for
// exercising the default share backfill helpers with directly constructed
// pool entries. The topology has a single socket with one CPU per NUMA slot so
// cpuCountInNUMAs reflects numaAvailable when every NUMA is fully available.
func newDefaultShareAssembler(
	t *testing.T,
	numaAvailable map[int]int,
	nonBinding machine.CPUSet,
	regionMap map[string]region.QoSRegion,
	reserved map[int]int,
	allowShared, disableDedicated bool,
	cfg types.ResourcePackageConfig,
) *ProvisionAssemblerCommon {
	t.Helper()

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)

	metaServer := newTestMetaServer(numaAvailable, 1)

	metaReader := metacache.NewDummyMetaCacheImp()
	if cfg != nil {
		require.NoError(t, metaReader.SetResourcePackageConfig(cfg))
	}

	numaAvailableCopy := map[int]int{}
	for k, v := range numaAvailable {
		numaAvailableCopy[k] = v
	}
	reservedCopy := map[int]int{}
	for k, v := range reserved {
		reservedCopy[k] = v
	}
	rampUpReclaimCPUSetCap := map[int]int{}
	if regionMap == nil {
		regionMap = map[string]region.QoSRegion{}
	}

	return NewProvisionAssemblerCommon(
		conf,
		nil,
		&regionMap,
		&reservedCopy,
		&rampUpReclaimCPUSetCap,
		&numaAvailableCopy,
		&nonBinding,
		&allowShared,
		&disableDedicated,
		metaReader,
		metaServer,
		metrics.DummyMetrics{},
	).(*ProvisionAssemblerCommon)
}

func newTestMetaServer(numaAvailable map[int]int, cpusPerCore int) *metaserver.MetaServer {
	totalCPUs := 0
	cpuDetails := machine.CPUDetails{}
	cpuID := 0
	for numaID, size := range numaAvailable {
		totalCPUs += size
		for i := 0; i < size; i++ {
			cpuDetails[cpuID] = machine.CPUTopoInfo{
				NUMANodeID: numaID,
				CoreID:     cpuID / cpusPerCore,
			}
			cpuID++
		}
	}

	return &metaserver.MetaServer{
		MetaAgent: &metaagent.MetaAgent{
			KatalystMachineInfo: &machine.KatalystMachineInfo{
				CPUTopology: &machine.CPUTopology{
					NumCPUs:      totalCPUs,
					NumCores:     totalCPUs / cpusPerCore,
					NumSockets:   1,
					NumNUMANodes: len(numaAvailable),
					CPUDetails:   cpuDetails,
				},
			},
		},
	}
}

func newDefaultShareResult(enabled bool) *types.InternalCPUCalculationResult {
	result := &types.InternalCPUCalculationResult{
		PoolEntries:                 map[string]map[int]types.CPUResource{},
		PoolOverlapInfo:             map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
	}
	result.DefaultShareBackfill.Enabled = enabled
	return result
}

// TestValidateDefaultShareBackfillConfig covers the gate-compatibility rule from
// Step 4/5: backfill requires shared and dedicated reclaim overlap disabled.
func TestValidateDefaultShareBackfillConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		fill             bool
		allowShared      bool
		disableDedicated bool
		wantErr          string
	}{
		{name: "gate disabled is always valid", fill: false, allowShared: true, disableDedicated: false},
		{name: "gate enabled with overlap disabled is valid", fill: true, allowShared: false, disableDedicated: true},
		{
			name: "shared overlap enabled is rejected", fill: true, allowShared: true, disableDedicated: true,
			wantErr: "fill default share pool requires shared and dedicated reclaim overlap disabled",
		},
		{
			name: "dedicated overlap enabled is rejected", fill: true, allowShared: false, disableDedicated: false,
			wantErr: "fill default share pool requires shared and dedicated reclaim overlap disabled",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pa := newDefaultShareAssembler(t, map[int]int{0: 8}, machine.NewCPUSet(0), nil,
				map[int]int{0: 2}, tc.allowShared, tc.disableDedicated, nil)
			pa.conf.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs = tc.fill
			err := pa.validateDefaultShareBackfillConfig()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestAssembleProvisionRejectsIncompatibleBackfillConfig verifies AssembleProvision
// returns an empty result together with the gate error.
func TestAssembleProvisionRejectsIncompatibleBackfillConfig(t *testing.T) {
	t.Parallel()

	pa := newDefaultShareAssembler(t, map[int]int{0: 8}, machine.NewCPUSet(0), nil,
		map[int]int{0: 2}, true /*allowShared*/, true, nil)
	pa.conf.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs = true

	result, err := pa.AssembleProvision(pa.calculationContext)
	require.ErrorContains(t, err, "fill default share pool requires shared and dedicated reclaim overlap disabled")
	require.Nil(t, result.PoolEntries)
}

// TestBuildDefaultShareBudgetRules covers Step 6: each classification rule of
// the canonical budget builder is asserted in isolation.
func TestBuildDefaultShareBudgetRules(t *testing.T) {
	t.Parallel()

	t.Run("faked numa share entry does not double count real numa", func(t *testing.T) {
		t.Parallel()
		// binding NUMAs 0 and 1, reclaim keyed per real NUMA, share at FakedNUMAID.
		pa := newDefaultShareAssembler(t, map[int]int{0: 95, 1: 95}, machine.NewCPUSet(), nil,
			map[int]int{0: 0, 1: 0}, false, true, nil)
		result := newDefaultShareResult(true)
		result.SetPoolEntry(commonstate.PoolNameReclaim, 0, 28, -1)
		result.SetPoolEntry(commonstate.PoolNameReclaim, 1, 28, -1)
		result.SetPoolEntry(commonstate.PoolNameShare, commonstate.FakedNUMAID, 4, -1)

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		require.Equal(t, 95, budget[0].UnpinnedAllocatableSize)
		require.Equal(t, 28, budget[0].FinalUnpinnedReclaimSize)
		require.Equal(t, 95, budget[1].UnpinnedAllocatableSize)
		require.Equal(t, 190, summary.AllocatableSize)
		// the FakedNUMAID share entry must not create a phantom bucket.
		_, hasFaked := budget[commonstate.FakedNUMAID]
		require.False(t, hasFaked)

		target, err := calculateDefaultShareTargetSize(budget)
		require.NoError(t, err)
		require.Equal(t, 190, target)
	})

	t.Run("unused pinned cpu is deducted from allocatable", func(t *testing.T) {
		t.Parallel()
		cfg := types.ResourcePackageConfig{
			0: {"pkg": {PinnedCPUSet: machine.MustParse("0-9")}},
		}
		pa := newDefaultShareAssembler(t, map[int]int{0: 95}, machine.NewCPUSet(), nil,
			map[int]int{0: 0}, false, true, cfg)
		result := newDefaultShareResult(true)
		result.SetPoolEntry(commonstate.PoolNameReclaim, 0, 28, -1)

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		// even though the pinned CPUs are unused, they are removed from the
		// unpinned allocatable budget.
		require.Equal(t, 85, budget[0].UnpinnedAllocatableSize)
		require.Equal(t, 10, summary.PinnedCPUSize)

		target, err := calculateDefaultShareTargetSize(budget)
		require.NoError(t, err)
		require.Equal(t, 85, target)
	})

	t.Run("exclusive numa ignores nested reclaim pinned dedicated", func(t *testing.T) {
		t.Parallel()
		exclusive := NewFakeRegion("dedicated-exclusive", configapi.QoSRegionTypeDedicated, "dedicated-exclusive")
		exclusive.SetBindingNumas(machine.NewCPUSet(1))
		exclusive.SetIsNumaBinding(true)
		exclusive.isNumaExclusive = true
		exclusive.SetPods(types.PodSet{"ded-pod": sets.NewString("main")})
		regionMap := map[string]region.QoSRegion{exclusive.Name(): exclusive}

		// pinned CPUs live inside the exclusive NUMA 1.
		cfg := types.ResourcePackageConfig{
			1: {"pkg": {PinnedCPUSet: machine.MustParse("0-7")}},
		}
		pa := newDefaultShareAssembler(t, map[int]int{0: 95, 1: 80}, machine.NewCPUSet(), regionMap,
			map[int]int{0: 0, 1: 0}, false, true, cfg)
		result := newDefaultShareResult(true)
		result.SetPoolEntry(commonstate.PoolNameReclaim, 0, 28, -1)
		// nested reclaim and dedicated entries inside the exclusive NUMA must be
		// ignored entirely.
		result.SetPoolEntry(commonstate.PoolNameReclaim, 1, 20, -1)
		result.SetPoolEntry("ded-pod", 1, 60, -1)

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		require.True(t, budget[1].Exclusive)
		require.Equal(t, 0, budget[1].FinalUnpinnedReclaimSize)
		require.Equal(t, 0, budget[1].FixedUnpinnedPoolSize)
		// the whole exclusive NUMA (72 unpinned after 8 pinned) counts as
		// exclusive size, not as residual.
		require.Equal(t, 72, summary.ExclusiveNUMASize)
		require.Equal(t, 0, summary.DedicatedSize, "dedicated inside exclusive numa must not be classified")

		target, err := calculateDefaultShareTargetSize(budget)
		require.NoError(t, err)
		// only NUMA 0 contributes its full unpinned allocatable upper bound.
		require.Equal(t, 95, target)
	})

	t.Run("fixed pools classified without lowering upper bound", func(t *testing.T) {
		t.Parallel()
		dedicated := NewFakeRegion("ded-region", configapi.QoSRegionTypeDedicated, "ded-region")
		dedicated.SetBindingNumas(machine.NewCPUSet(0))
		dedicated.SetIsNumaBinding(true)
		dedicated.SetPods(types.PodSet{"ded-pod": sets.NewString("main")})
		regionMap := map[string]region.QoSRegion{dedicated.Name(): dedicated}

		pa := newDefaultShareAssembler(t, map[int]int{0: 95}, machine.NewCPUSet(), regionMap,
			map[int]int{0: 0}, false, true, nil)
		result := newDefaultShareResult(true)
		result.SetPoolEntry(commonstate.PoolNameReclaim, 0, 28, -1)
		result.SetPoolEntry("ded-pod", 0, 20, -1)                            // dedicated (pod-uid keyed)
		result.SetPoolEntry("isolation-x", 0, 6, -1)                         // isolation
		result.SetPoolEntry("batch"+commonstate.NUMAPoolInfix+"0", 0, 8, -1) // SNB
		result.SetPoolEntry("custom-shared", 0, 5, -1)                       // custom shared

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		require.Equal(t, 39, budget[0].FixedUnpinnedPoolSize) // 20+6+8+5
		require.Equal(t, 20, summary.DedicatedSize)
		require.Equal(t, 6, summary.IsolationSize)
		require.Equal(t, 8, summary.SNBSize)
		require.Equal(t, 5, summary.CustomSharedSize)

		target, err := calculateDefaultShareTargetSize(budget)
		require.NoError(t, err)
		require.Equal(t, 95, target) // reclaim and fixed pools are diagnostics-only
	})

	t.Run("distinct dedicated pod uids are each counted", func(t *testing.T) {
		t.Parallel()
		// dedicated pool entries are keyed by pod uid; two distinct dedicated
		// pods on the same NUMA are independent allocations and must both be
		// counted, regardless of how their regions are grouped.
		first := NewFakeRegion("ded-region-a", configapi.QoSRegionTypeDedicated, "dedicated")
		first.SetBindingNumas(machine.NewCPUSet(0))
		first.SetIsNumaBinding(true)
		first.SetPods(types.PodSet{"ded-pod-a": sets.NewString("main")})
		second := NewFakeRegion("ded-region-b", configapi.QoSRegionTypeDedicated, "dedicated")
		second.SetBindingNumas(machine.NewCPUSet(0))
		second.SetIsNumaBinding(true)
		second.SetPods(types.PodSet{"ded-pod-b": sets.NewString("main")})
		regionMap := map[string]region.QoSRegion{
			first.Name():  first,
			second.Name(): second,
		}

		pa := newDefaultShareAssembler(t, map[int]int{0: 40}, machine.NewCPUSet(), regionMap,
			map[int]int{0: 0}, false, true, nil)
		result := newDefaultShareResult(true)
		result.SetPoolEntry("ded-pod-a", 0, 20, -1)
		result.SetPoolEntry("ded-pod-b", 0, 20, -1)

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		require.Equal(t, 40, budget[0].FixedUnpinnedPoolSize)
		require.Equal(t, 40, summary.DedicatedSize)

		target, err := calculateDefaultShareTargetSize(budget)
		require.NoError(t, err)
		require.Equal(t, 40, target)
	})

	t.Run("fully pinned dedicated pool contributes no fixed size", func(t *testing.T) {
		t.Parallel()
		dedicated := NewFakeRegion(
			"ded-region",
			configapi.QoSRegionTypeDedicated,
			resourcepackage.WrapOwnerPoolName("dedicated", "pkg"),
		)
		dedicated.SetBindingNumas(machine.NewCPUSet(0))
		dedicated.SetIsNumaBinding(true)
		dedicated.SetPods(types.PodSet{"ded-pod": sets.NewString("main")})
		regionMap := map[string]region.QoSRegion{dedicated.Name(): dedicated}
		cfg := types.ResourcePackageConfig{
			0: {"pkg": {PinnedCPUSet: machine.MustParse("0-9")}},
		}

		pa := newDefaultShareAssembler(t, map[int]int{0: 20}, machine.NewCPUSet(), regionMap,
			map[int]int{0: 0}, false, true, cfg)
		result := newDefaultShareResult(true)
		result.SetPoolEntry("ded-pod", 0, 10, -1)

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		require.Equal(t, 10, budget[0].UnpinnedAllocatableSize)
		require.Zero(t, budget[0].FixedUnpinnedPoolSize)
		require.Zero(t, summary.DedicatedSize)

		target, err := calculateDefaultShareTargetSize(budget)
		require.NoError(t, err)
		require.Equal(t, 10, target)
	})

	t.Run("partially pinned dedicated pool contributes only unpinned remainder", func(t *testing.T) {
		t.Parallel()
		dedicated := NewFakeRegion(
			"ded-region",
			configapi.QoSRegionTypeDedicated,
			resourcepackage.WrapOwnerPoolName("dedicated", "pkg"),
		)
		dedicated.SetBindingNumas(machine.NewCPUSet(0))
		dedicated.SetIsNumaBinding(true)
		dedicated.SetPods(types.PodSet{"ded-pod": sets.NewString("main")})
		regionMap := map[string]region.QoSRegion{dedicated.Name(): dedicated}
		cfg := types.ResourcePackageConfig{
			0: {"pkg": {PinnedCPUSet: machine.MustParse("0-3")}},
		}

		pa := newDefaultShareAssembler(t, map[int]int{0: 20}, machine.NewCPUSet(), regionMap,
			map[int]int{0: 0}, false, true, cfg)
		result := newDefaultShareResult(true)
		result.SetPoolEntry("ded-pod", 0, 10, -1)

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		require.Equal(t, 16, budget[0].UnpinnedAllocatableSize)
		require.Equal(t, 6, budget[0].FixedUnpinnedPoolSize)
		require.Equal(t, 6, summary.DedicatedSize)

		target, err := calculateDefaultShareTargetSize(budget)
		require.NoError(t, err)
		require.Equal(t, 16, target)
	})

	for _, tc := range []struct {
		name       string
		cfg        types.ResourcePackageConfig
		wantTarget int
	}{
		{
			name:       "dedicated owner package missing from config is fully fixed",
			cfg:        types.ResourcePackageConfig{},
			wantTarget: 40,
		},
		{
			name: "dedicated owner package pinned only in another numa is fully fixed in this bucket",
			cfg: types.ResourcePackageConfig{
				1: {"pkg": {PinnedCPUSet: machine.MustParse("20-29")}},
			},
			wantTarget: 30,
		},
		{
			name: "dedicated owner package with empty pinned cpu set is fully fixed",
			cfg: types.ResourcePackageConfig{
				0: {"pkg": {PinnedCPUSet: machine.NewCPUSet()}},
			},
			wantTarget: 40,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dedicated := NewFakeRegion(
				"ded-region",
				configapi.QoSRegionTypeDedicated,
				resourcepackage.WrapOwnerPoolName("dedicated", "pkg"),
			)
			dedicated.SetBindingNumas(machine.NewCPUSet(0))
			dedicated.SetIsNumaBinding(true)
			dedicated.SetPods(types.PodSet{"ded-pod": sets.NewString("main")})
			regionMap := map[string]region.QoSRegion{dedicated.Name(): dedicated}

			pa := newDefaultShareAssembler(t, map[int]int{0: 20, 1: 20}, machine.NewCPUSet(), regionMap,
				map[int]int{0: 0, 1: 0}, false, true, tc.cfg)
			result := newDefaultShareResult(true)
			result.SetPoolEntry("ded-pod", 0, 10, -1)

			budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
			require.NoError(t, err)
			require.Equal(t, 10, budget[0].FixedUnpinnedPoolSize)
			require.Equal(t, 10, summary.DedicatedSize)

			target, err := calculateDefaultShareTargetSize(budget)
			require.NoError(t, err)
			require.Equal(t, tc.wantTarget, target)
		})
	}

	t.Run("pod uid split across per-numa sibling regions is merged", func(t *testing.T) {
		t.Parallel()
		// a numa-binding dedicated pod spanning NUMA 0 and NUMA 1 is split into
		// one sibling region per NUMA; both share the same owner pool and must
		// be treated as one logical dedicated pod rather than a conflict.
		first := NewFakeRegion("dedicated-region-a", configapi.QoSRegionTypeDedicated, "dedicated")
		first.SetBindingNumas(machine.NewCPUSet(0))
		first.SetIsNumaBinding(true)
		first.SetPods(types.PodSet{"spanning-pod": sets.NewString("main")})
		second := NewFakeRegion("dedicated-region-b", configapi.QoSRegionTypeDedicated, "dedicated")
		second.SetBindingNumas(machine.NewCPUSet(1))
		second.SetIsNumaBinding(true)
		second.SetPods(types.PodSet{"spanning-pod": sets.NewString("main")})
		regionMap := map[string]region.QoSRegion{
			first.Name():  first,
			second.Name(): second,
		}

		pa := newDefaultShareAssembler(t, map[int]int{0: 20, 1: 20}, machine.NewCPUSet(), regionMap,
			map[int]int{0: 0, 1: 0}, false, true, nil)
		result := newDefaultShareResult(true)
		// the dedicated pool entry is keyed by pod uid and carries per-numa sizes.
		result.SetPoolEntry("spanning-pod", 0, 10, -1)
		result.SetPoolEntry("spanning-pod", 1, 10, -1)

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		// each NUMA is accounted for exactly once, without double counting the
		// shared owner pool across the sibling regions.
		require.Equal(t, 10, budget[0].FixedUnpinnedPoolSize)
		require.Equal(t, 10, budget[1].FixedUnpinnedPoolSize)
		require.Equal(t, 20, summary.DedicatedSize)
	})

	t.Run("pod uid split across non-binding sibling numas folds without dropping any numa", func(t *testing.T) {
		t.Parallel()
		// regression for the FakedNUMAID bucket-folding dedup bug: a numa-binding
		// (non-exclusive) dedicated pod is split into one sibling region per NUMA,
		// and both bound NUMAs happen to be non-binding, so effectiveBucket folds
		// them into the shared FakedNUMAID bucket. Since the dedup representative
		// is a single region name shared by both siblings, keying the dedup set by
		// region name alone would treat the second NUMA entry as a duplicate and
		// silently drop it. Keying by "<name>/<numaID>" keeps every real NUMA.
		first := NewFakeRegion("dedicated-region-a", configapi.QoSRegionTypeDedicated, "dedicated")
		first.SetBindingNumas(machine.NewCPUSet(0))
		first.SetIsNumaBinding(true)
		first.SetPods(types.PodSet{"spanning-pod": sets.NewString("main")})
		second := NewFakeRegion("dedicated-region-b", configapi.QoSRegionTypeDedicated, "dedicated")
		second.SetBindingNumas(machine.NewCPUSet(1))
		second.SetIsNumaBinding(true)
		second.SetPods(types.PodSet{"spanning-pod": sets.NewString("main")})
		regionMap := map[string]region.QoSRegion{
			first.Name():  first,
			second.Name(): second,
		}

		// both NUMA 0 and NUMA 1 are non-binding, so they collapse into the
		// FakedNUMAID bucket.
		pa := newDefaultShareAssembler(t, map[int]int{0: 20, 1: 20}, machine.NewCPUSet(0, 1), regionMap,
			map[int]int{0: 0, 1: 0}, false, true, nil)
		result := newDefaultShareResult(true)
		result.SetPoolEntry("spanning-pod", 0, 10, -1)
		result.SetPoolEntry("spanning-pod", 1, 10, -1)

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		// the real NUMAs collapse into a single FakedNUMAID bucket; both binding
		// NUMAs must be preserved rather than deduped away by region name.
		_, hasReal0 := budget[0]
		require.False(t, hasReal0)
		_, hasReal1 := budget[1]
		require.False(t, hasReal1)
		faked := budget[commonstate.FakedNUMAID]
		require.Equal(t, 40, faked.UnpinnedAllocatableSize)
		// 10 (numa 0) + 10 (numa 1); the pre-fix code would report only 10.
		require.Equal(t, 20, faked.FixedUnpinnedPoolSize)
		require.Equal(t, 20, summary.DedicatedSize)

		target, err := calculateDefaultShareTargetSize(budget)
		require.NoError(t, err)
		require.Equal(t, 40, target)
	})

	t.Run("distinct dedicated pod uids folded into faked bucket are each counted", func(t *testing.T) {
		t.Parallel()
		// two distinct dedicated pods bound to the same non-binding NUMA fold
		// into the FakedNUMAID bucket. They are keyed by distinct pod uids, so
		// both must be counted; folding into the shared bucket must not collapse
		// them into one.
		first := NewFakeRegion("ded-region-a", configapi.QoSRegionTypeDedicated, "dedicated")
		first.SetBindingNumas(machine.NewCPUSet(0))
		first.SetIsNumaBinding(true)
		first.SetPods(types.PodSet{"ded-pod-a": sets.NewString("main")})
		second := NewFakeRegion("ded-region-b", configapi.QoSRegionTypeDedicated, "dedicated")
		second.SetBindingNumas(machine.NewCPUSet(0))
		second.SetIsNumaBinding(true)
		second.SetPods(types.PodSet{"ded-pod-b": sets.NewString("main")})
		regionMap := map[string]region.QoSRegion{
			first.Name():  first,
			second.Name(): second,
		}

		pa := newDefaultShareAssembler(t, map[int]int{0: 20, 1: 20}, machine.NewCPUSet(0, 1), regionMap,
			map[int]int{0: 0, 1: 0}, false, true, nil)
		result := newDefaultShareResult(true)
		result.SetPoolEntry("ded-pod-a", 0, 12, -1)
		result.SetPoolEntry("ded-pod-b", 0, 12, -1)

		budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.NoError(t, err)
		faked := budget[commonstate.FakedNUMAID]
		require.Equal(t, 40, faked.UnpinnedAllocatableSize)
		// both distinct pod uids are counted: 12 + 12.
		require.Equal(t, 24, faked.FixedUnpinnedPoolSize)
		require.Equal(t, 24, summary.DedicatedSize)
	})

	t.Run("pod uid mapped to regions with conflicting owner pools is rejected", func(t *testing.T) {
		t.Parallel()
		// a single dedicated pod uid must resolve to a single owner pool name so
		// its pinned resource-package deduction is unambiguous. Two regions that
		// share a pod uid but disagree on the owner pool signal a genuinely
		// ambiguous mapping that must be rejected.
		first := NewFakeRegion("ded-region-a", configapi.QoSRegionTypeDedicated,
			resourcepackage.WrapOwnerPoolName("dedicated", "pkg-a"))
		first.SetBindingNumas(machine.NewCPUSet(0))
		first.SetIsNumaBinding(true)
		first.SetPods(types.PodSet{"duplicate-pod": sets.NewString("main")})
		second := NewFakeRegion("ded-region-b", configapi.QoSRegionTypeDedicated,
			resourcepackage.WrapOwnerPoolName("dedicated", "pkg-b"))
		second.SetBindingNumas(machine.NewCPUSet(1))
		second.SetIsNumaBinding(true)
		second.SetPods(types.PodSet{"duplicate-pod": sets.NewString("main")})
		regionMap := map[string]region.QoSRegion{
			first.Name():  first,
			second.Name(): second,
		}

		pa := newDefaultShareAssembler(t, map[int]int{0: 20, 1: 20}, machine.NewCPUSet(), regionMap,
			map[int]int{0: 0, 1: 0}, false, true, nil)
		result := newDefaultShareResult(true)
		result.SetPoolEntry("duplicate-pod", 0, 10, -1)

		_, _, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
		require.Error(t, err)
		require.Contains(t, err.Error(), `pod uid "duplicate-pod" maps to dedicated regions with conflicting owner pools`)
	})
}

// TestFinalizeDefaultShareBackfillMatrix covers Step 8 quantity matrix via the
// production write path finalizeDefaultShareBackfill.
func TestFinalizeDefaultShareBackfillMatrix(t *testing.T) {
	t.Parallel()

	type entry struct {
		pool   string
		numaID int
		size   int
	}
	tests := []struct {
		name          string
		numaAvailable map[int]int
		nonBinding    machine.CPUSet
		regionMap     map[string]region.QoSRegion
		cfg           types.ResourcePackageConfig
		entries       []entry
		enabled       bool
		wantShare     int
		wantErr       string
		// wantAllocatable/wantReclaim/wantFixed are independent per-case
		// constants (derived by hand from the entries above, not from a call to
		// buildDefaultShareBudget). They let the aggregate invariant compare the
		// production budget fields against fixed references instead of comparing
		// values that all originate from the same rebuild.
		wantAllocatable int
		wantReclaim     int
		wantFixed       int
	}{
		{
			name:            "reclaim advice does not lower qrm residual upper bound",
			numaAvailable:   map[int]int{0: 95, 1: 95},
			nonBinding:      machine.NewCPUSet(),
			entries: []entry{
				{commonstate.PoolNameReclaim, 0, 28},
				{commonstate.PoolNameReclaim, 1, 28},
				{commonstate.PoolNameShare, commonstate.FakedNUMAID, 4},
				{commonstate.PoolNameShare, 0, 6},
				{commonstate.PoolNameShare, 1, 8},
			},
			enabled:         true,
			wantShare:       190,
			wantAllocatable: 190,
			wantReclaim:     56,
			wantFixed:       0,
		},
		{
			name:            "reclaim disabled publishes full qrm residual upper bound",
			numaAvailable:   map[int]int{0: 95},
			nonBinding:      machine.NewCPUSet(),
			entries:         []entry{{commonstate.PoolNameReclaim, 0, 0}, {"custom-shared", 0, 10}, {commonstate.PoolNameShare, commonstate.FakedNUMAID, 8}},
			enabled:         true,
			wantShare:       95,
			wantAllocatable: 95,
			wantReclaim:     0,
			wantFixed:       10,
		},
		{
			name:            "full reclaim advice still publishes allocatable upper bound",
			numaAvailable:   map[int]int{0: 20},
			nonBinding:      machine.NewCPUSet(0),
			entries:         []entry{{commonstate.PoolNameReclaim, commonstate.FakedNUMAID, 20}, {commonstate.PoolNameShare, commonstate.FakedNUMAID, 4}},
			enabled:         true,
			wantShare:       20,
			wantAllocatable: 20,
			wantReclaim:     20,
			wantFixed:       0,
		},
		{
			name:            "pool overcommit keeps allocatable upper bound",
			numaAvailable:   map[int]int{0: 16},
			nonBinding:      machine.NewCPUSet(),
			entries:         []entry{{commonstate.PoolNameReclaim, 0, 8}, {"custom-shared", 0, 10}},
			enabled:         true,
			wantShare:       16,
			wantAllocatable: 16,
			wantReclaim:     8,
			wantFixed:       10,
		},
		{
			name:          "gate disabled preserves existing share entry",
			numaAvailable: map[int]int{0: 95, 1: 95},
			nonBinding:    machine.NewCPUSet(),
			entries:       []entry{{commonstate.PoolNameReclaim, 0, 28}, {commonstate.PoolNameReclaim, 1, 28}, {commonstate.PoolNameShare, commonstate.FakedNUMAID, 4}},
			enabled:       false,
			wantShare:     4,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reserved := map[int]int{}
			for numaID := range tc.numaAvailable {
				reserved[numaID] = 0
			}
			pa := newDefaultShareAssembler(t, tc.numaAvailable, tc.nonBinding, tc.regionMap,
				reserved, false, true, tc.cfg)
			result := newDefaultShareResult(tc.enabled)
			for _, e := range tc.entries {
				result.SetPoolEntry(e.pool, e.numaID, e.size, -1)
			}

			err := pa.finalizeDefaultShareBackfill(NewRegionMapHelper(*pa.regionMap), result)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantShare,
				result.PoolEntries[commonstate.PoolNameShare][commonstate.FakedNUMAID].Size)

			if !tc.enabled {
				return
			}
			require.Len(t, result.PoolEntries[commonstate.PoolNameShare], 1)
			require.Equal(t, tc.wantAllocatable, result.DefaultShareBackfill.AllocatableBudget)
			require.Equal(t, tc.wantShare, result.DefaultShareBackfill.DefaultShareFinal)

			// upper-bound invariant: share = allocatable (summed over
			// non-exclusive NUMAs). Reclaim and fixed quantities remain
			// diagnostics because QRM materializes their CPUSet-level union.
			// To avoid a near-tautology, the
			// production budget aggregates are first pinned against independent
			// per-case constants (wantAllocatable/wantReclaim/wantFixed derived
			// by hand from the entries), and only then combined with the
			// production-computed wantShare in the balance check. If the
			// residual math in buildDefaultShareBudget or
			// calculateDefaultShareTargetSize regresses, either these field-level
			// checks or the wantShare check above will fail.
			budget, summary, err := pa.buildDefaultShareBudget(NewRegionMapHelper(*pa.regionMap), result)
			require.NoError(t, err)
			totalReclaim, totalFixed, totalNonExclusiveAlloc := 0, 0, 0
			for _, b := range budget {
				if b.Exclusive {
					continue
				}
				totalReclaim += b.FinalUnpinnedReclaimSize
				totalFixed += b.FixedUnpinnedPoolSize
				totalNonExclusiveAlloc += b.UnpinnedAllocatableSize
			}
			require.Equal(t, tc.wantAllocatable, totalNonExclusiveAlloc)
			require.Equal(t, tc.wantReclaim, totalReclaim)
			require.Equal(t, tc.wantFixed, totalFixed)
			require.Equal(t, tc.wantAllocatable, tc.wantShare)
			require.Equal(t, summary.FixedCommonPoolSize, totalFixed)
		})
	}
}

// TestAssembleProvisionBackfillEndToEnd drives the full AssembleProvision path
// with the backfill gate enabled on a no-region single NUMA node, confirming the
// default share pool absorbs all residual non-reclaim CPUs.
func TestAssembleProvisionBackfillEndToEnd(t *testing.T) {
	t.Parallel()

	pa := newDefaultShareAssembler(t, map[int]int{0: 20}, machine.NewCPUSet(0), nil,
		map[int]int{0: 2}, false /*allowShared*/, true /*disableDedicated*/, nil)
	pa.conf.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs = true
	pa.conf.GetDynamicConfiguration().EnableReclaim = true
	pa.conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio = 0.3

	result, err := pa.AssembleProvision(pa.calculationContext)
	require.NoError(t, err)
	require.True(t, result.DefaultShareBackfill.Enabled)

	reclaim := result.PoolEntries[commonstate.PoolNameReclaim][commonstate.FakedNUMAID].Size
	share := result.PoolEntries[commonstate.PoolNameShare][commonstate.FakedNUMAID].Size
	require.Positive(t, reclaim)
	// SysAdvisor publishes the allocatable upper bound; QRM subtracts the current
	// reclaim and fixed CPUSet union when materializing the actual share CPUSet.
	require.Equal(t, 20, share)
	require.Equal(t, share, result.DefaultShareBackfill.DefaultShareFinal)
}
