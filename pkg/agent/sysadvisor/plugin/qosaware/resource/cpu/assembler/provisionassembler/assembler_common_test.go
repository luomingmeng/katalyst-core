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
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	metaagent "github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	metricspool "github.com/kubewharf/katalyst-core/pkg/metrics/metrics-pool"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

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
	t.Parallel()

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
			t.Parallel()

			conf := generateTestConf(t, tt.enableReclaimed, tt.disableReclaimSelector)
			genericCtx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{})
			require.NoError(t, err)

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

			common := NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &numaAvailable,
				&nonBindingNumas, &tt.allowSharedCoresOverlapReclaimedCores,
				&tt.disableDedicatedCoresOverlapReclaimedCores, metaCache, metaServer, metrics.DummyMetrics{})
			result, err := common.AssembleProvision()
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
				require.ErrorContains(t, err, "active dedicated pool",
					"AS=%t DD=%t", allowSharedOverlap, disableDedicatedOverlap)
			}
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

type ordinaryOverlapAssemblerCase struct {
	capacity                int
	reserved                int
	allowSharedOverlap      bool
	disableDedicatedOverlap bool
	sharedEnableReclaim     bool
	dedicatedEnableReclaim  bool
	sharedRequest           int
	sharedRequirement       int
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
		conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNUMAs,
		&tc.allowSharedOverlap, &tc.disableDedicatedOverlap, metaReader, nil, metrics.DummyMetrics{},
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
		numaAvailable := map[int]int{0: 12, 1: 18}
		nonBindingNUMAs := machine.NewCPUSet(1)
		allowSharedOverlap := false
		disableDedicatedOverlap := true
		assembler := NewProvisionAssemblerCommon(
			conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNUMAs,
			&allowSharedOverlap, &disableDedicatedOverlap, metaReader, nil, metrics.DummyMetrics{},
		)

		result, err := assembler.AssembleProvision()
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
			gotSize, gotLimit := clampByReclaimedCPUMaxRatio(
				tt.size,
				tt.limit,
				tt.ratio,
				tt.cpuCount,
				tt.reservedForReclaim,
			)
			require.Equal(t, tt.wantSize, gotSize)
			require.Equal(t, tt.wantLimit, gotLimit)
		})
	}
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
		numaAvailable := map[int]int{0: 8}
		allowOverlap := true
		disableDedicatedOverlap := false
		return NewProvisionAssemblerCommon(
			conf,
			nil,
			&regionMap,
			&reservedForReclaim,
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
		result, err := pa.AssembleProvision()
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
	numaAvailable := map[int]int{0: capacity}
	nonBindingNUMAs := machine.NewCPUSet()
	allowSharedOverlap := true
	pa := NewProvisionAssemblerCommon(
		conf,
		nil,
		&regionMap,
		&reservedForReclaim,
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
