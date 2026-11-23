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

package accompanyresource

import (
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/qrm"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/stretchr/testify/require"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"
	"testing"
)

func TestRetainedGPUDoesNotDistortAccompanyDeviceRatio(t *testing.T) {
	r := machine.NewDeviceTopologyRegistry(metrics.DummyMetrics{})
	r.RegisterDeviceTopologyProvider("gpu", machine.NewDeviceTopologyProvider())
	r.RegisterDeviceTopologyProvider("rdma", machine.NewDeviceTopologyProvider())
	require.NoError(t, r.SetDeviceTopology("gpu", &machine.DeviceTopology{Devices: map[string]machine.DeviceInfo{"gpu-0": {Health: "Healthy"}, "gpu-2": {Health: "Healthy"}}}))
	require.NoError(t, r.SetDeviceTopology("rdma", &machine.DeviceTopology{Devices: map[string]machine.DeviceInfo{"rdma-0": {Health: "Healthy"}, "rdma-1": {Health: "Healthy"}}}))
	reg := state.NewDefaultResourceStateGeneratorRegistry()
	reg.RegisterResourceStateGenerator("gpu_device", state.NewGenericDefaultResourceStateGenerator([]string{"gpu"}, r, 1, true))
	reg.RegisterResourceStateGenerator("rdma_device", state.NewGenericDefaultResourceStateGenerator([]string{"rdma"}, r, 1, true))
	entries := state.PodResourceEntries{"gpu_device": state.PodEntries{}}
	entries["gpu_device"]["old-pod"] = state.ContainerEntries{"container": &state.AllocationInfo{DeviceName: "gpu", TopologyAwareAllocations: map[string]state.Allocation{"gpu-1": {Quantity: 1}}}}
	entries["gpu_device"]["new-pod"] = state.ContainerEntries{"container": &state.AllocationInfo{DeviceName: "gpu", TopologyAwareAllocations: map[string]state.Allocation{"gpu-0": {Quantity: 1}}}}
	m, err := state.GenerateMachineStateFromPodEntries(entries, reg)
	require.NoError(t, err)
	// RDMA is lazy; its real public ensureState path initializes this map.
	rdmaGen, _ := reg.GetGenerator("rdma_device")
	m["rdma_device"], err = rdmaGen.GenerateDefaultResourceState()
	require.NoError(t, err)
	ctx := &allocate.AllocationContext{ResourceReq: &pluginapi.ResourceRequest{PodUid: "new-pod", ContainerName: "container"}, DeviceReq: &pluginapi.DeviceRequest{DeviceName: "rdma", DeviceRequest: 1, AvailableDevices: []string{"rdma-0", "rdma-1"}}, ResourceName: "rdma", AccompanyResourceName: "gpu", DeviceNameToTypeMap: map[string]string{"gpu": "gpu_device", "rdma": "rdma_device"}, MachineState: m, DeviceTopologyRegistry: r, GPUQRMPluginConfig: qrm.NewGPUQRMPluginConfig(), Emitter: metrics.DummyMetrics{}}
	result, err := NewAccompanyResourceStrategy().Bind(ctx, []string{"rdma-0", "rdma-1"})
	t.Logf("present=2 GPUs+2 RDMA, state GPU count=%d, ratio=%v, bind=%v error=%v", len(m["gpu_device"]), m.GetRatioOfAccompanyResourceToTargetResource("gpu_device", "rdma_device"), result, err)
	require.Zero(t, m["gpu_device"]["gpu-1"].Allocatable)
	require.Equal(t, float64(1), m["gpu_device"]["gpu-1"].GetQuantityAllocated())
	require.Equal(t, float64(1), m.GetRatioOfAccompanyResourceToTargetResource("gpu_device", "rdma_device"))
	require.NoError(t, err, "retained dropped GPU is not live capacity and must not reject another pod's RDMA allocation")
	require.True(t, result.Success)
	require.Empty(t, result.AllocatedDevices) // unchanged no-affinity behavior
}
