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

package reporter

import (
	"testing"

	nodeapi "github.com/kubewharf/katalyst-api/pkg/apis/node/v1alpha1"
	apiConsts "github.com/kubewharf/katalyst-api/pkg/consts"
	gpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/util"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestUnhealthyGPUReportsZeroVirtualAllocatable(t *testing.T) {
	p := &gpuReporterPlugin{deviceTypeToNames: map[string]sets.String{gpuconsts.GPUDeviceType: sets.NewString("gpu")}}
	topologies := map[string]*machine.DeviceTopology{"gpu": {Devices: map[string]machine.DeviceInfo{"device": {Health: "Unhealthy"}}}}
	resources := state.AllocationResourcesMap{}
	for name, capacity := range map[v1.ResourceName]float64{gpuconsts.GPUDeviceType: 1, apiConsts.ResourceGPUMemory: 100, apiConsts.ResourceMilliGPU: 1000} {
		resources[name] = state.AllocationMap{"device": {Allocatable: capacity}}
	}
	zones := p.getZoneResources(topologies, resources)
	zone := util.GenerateDeviceZoneNode("device", string(nodeapi.TopologyTypeGPU))
	for _, name := range []v1.ResourceName{"gpu", apiConsts.ResourceGPUMemory, apiConsts.ResourceMilliGPU} {
		allocated := (*zones[zone].Allocatable)[name]
		capacity := (*zones[zone].Capacity)[name]
		require.True(t, allocated.IsZero())
		require.False(t, capacity.IsZero())
	}
}
