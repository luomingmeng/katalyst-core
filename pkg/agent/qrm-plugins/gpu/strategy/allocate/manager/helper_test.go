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

package manager

import (
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate/registry"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/qrm"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/stretchr/testify/require"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"
)

type candidateCaptureStrategy struct{ request *pluginapi.DeviceRequest }

func (s *candidateCaptureStrategy) Name() string { return "candidate-capture" }
func (s *candidateCaptureStrategy) Allocate(ctx *allocate.AllocationContext) (*allocate.AllocationResult, error) {
	s.request = ctx.DeviceReq
	return &allocate.AllocationResult{Success: true, AllocatedDevices: ctx.DeviceReq.AvailableDevices}, nil
}

func TestAllocateDevicesFiltersUnavailableCandidatesBeforeCustomStrategy(t *testing.T) {
	cfg := qrm.NewGPUQRMPluginConfig()
	cfg.GPUDeviceNames = []string{"gpu"}
	cfg.CustomAllocationStrategy = map[string]string{"gpu": "candidate-capture"}
	manager := GetGlobalStrategyManager(cfg)
	capture := &candidateCaptureStrategy{}
	previousRegistry := manager.StrategyRegistry
	manager.StrategyRegistry = registry.NewStrategyRegistry()
	t.Cleanup(func() { manager.StrategyRegistry = previousRegistry })
	require.NoError(t, manager.RegisterAllocationStrategy(capture))
	r := machine.NewDeviceTopologyRegistry(metrics.DummyMetrics{})
	r.RegisterDeviceTopologyProvider("gpu", machine.NewDeviceTopologyProvider())
	require.NoError(t, r.SetDeviceTopology("gpu", &machine.DeviceTopology{Devices: map[string]machine.DeviceInfo{"healthy": {Health: "Healthy"}, "unhealthy": {Health: "Unhealthy"}}}))
	request := &pluginapi.DeviceRequest{DeviceName: "gpu", AvailableDevices: []string{"unhealthy", "missing", "healthy"}, ReusableDevices: []string{"missing", "unhealthy", "healthy"}, DeviceRequest: 1}
	original := *request
	result, err := AllocateDevicesUsingStrategy(&pluginapi.ResourceRequest{}, request, r, cfg, metrics.DummyMetrics{}, nil, state.AllocationResourcesMap{"gpu_device": {"healthy": {Allocatable: 1}}}, "", "gpu", "", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"healthy"}, result.AllocatedDevices)
	require.Equal(t, []string{"healthy"}, capture.request.ReusableDevices)
	require.Equal(t, original, *request)
}
