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

package virtualgpu

import (
	"context"
	"testing"

	apiConsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/stretchr/testify/require"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"
)

func TestVirtualResourcesWaitForDeviceStateAndCoverAliases(t *testing.T) {
	base := makeTestBasePlugin(t)
	base.Conf.GPUDeviceNames = []string{"test-gpu", "gpu-subset"}
	base.DeviceTopologyRegistry.RegisterDeviceTopologyProvider("gpu-subset", machine.NewDeviceTopologyProvider())
	require.NoError(t, base.DeviceTopologyRegistry.SetDeviceTopology("test-gpu", &machine.DeviceTopology{UpdateTime: 1, Devices: map[string]machine.DeviceInfo{"gpu-0": {Health: "Healthy"}, "gpu-1": {Health: "Healthy"}}}))
	require.NoError(t, base.DeviceTopologyRegistry.SetDeviceTopology("gpu-subset", &machine.DeviceTopology{UpdateTime: 2, Devices: map[string]machine.DeviceInfo{"gpu-0": {Health: "Healthy"}}}))
	plugin := NewVirtualGPUPlugin(base).(*VirtualGPUPlugin)
	// Discovery alone must not publish capacity before checkpoint ownership is read.
	resp, err := plugin.GetTopologyAwareAllocatableResources(context.Background())
	require.NoError(t, err)
	require.Zero(t, resp[string(apiConsts.ResourceMilliGPU)].AggregatedAllocatableQuantity)
	_, err = plugin.Allocate(context.Background(), &pluginapi.ResourceRequest{PodUid: "new", ContainerName: "container", ContainerType: pluginapi.ContainerType_MAIN, ResourceRequests: map[string]float64{string(apiConsts.ResourceMilliGPU): 100}}, nil)
	require.Error(t, err)
	prepareGPUDeviceState(t, base)
	// The newest alias omits gpu-1 and gpu-0 is occupied. A fabricated request
	// must use gpu-1 through its actual device name, not silently lose that card.
	m := base.GetState().GetMachineState()
	m["gpu_device"]["gpu-0"].PodEntries = state.PodEntries{"old": state.ContainerEntries{"container": &state.AllocationInfo{}}}
	base.GetState().SetMachineState(m, false)
	resp, err = plugin.GetTopologyAwareAllocatableResources(context.Background())
	require.NoError(t, err)
	require.Equal(t, float64(2000), resp[string(apiConsts.ResourceMilliGPU)].AggregatedAllocatableQuantity)
	_, err = plugin.Allocate(context.Background(), &pluginapi.ResourceRequest{PodUid: "new", ContainerName: "container", ContainerType: pluginapi.ContainerType_MAIN, ResourceRequests: map[string]float64{string(apiConsts.ResourceMilliGPU): 100}}, nil)
	require.NoError(t, err)
	ai := base.GetState().GetAllocationInfo(apiConsts.ResourceMilliGPU, "new", "container")
	require.NotNil(t, ai)
	require.Equal(t, map[string]state.Allocation{"gpu-1": {Quantity: 100, NUMANodes: []int{}}}, ai.TopologyAwareAllocations)
}
