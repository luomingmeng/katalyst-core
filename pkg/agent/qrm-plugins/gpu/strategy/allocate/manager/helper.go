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
	"fmt"

	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	gpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/qrm"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// AllocateDevicesUsingStrategy performs device allocation using the strategy framework
func AllocateDevicesUsingStrategy(
	resourceReq *pluginapi.ResourceRequest,
	deviceReq *pluginapi.DeviceRequest,
	deviceTopologyRegistry *machine.DeviceTopologyRegistry,
	gpuConfig *qrm.GPUQRMPluginConfig,
	emitter metrics.MetricEmitter,
	metaServer *metaserver.MetaServer,
	machineState state.AllocationResourcesMap,
	qosLevel, resourceName, accompanyResourceName string,
	deviceNameToTypeMap map[string]string,
) (*allocate.AllocationResult, error) {
	if resourceReq == nil || deviceReq == nil || gpuConfig == nil || deviceTopologyRegistry == nil {
		return nil, fmt.Errorf("nil device allocation input")
	}
	// GPU resource requests must not use retained state for a disappeared or
	// unhealthy device, including when custom strategies replace all filters.
	for _, name := range gpuConfig.GPUDeviceNames {
		if name != deviceReq.DeviceName {
			continue
		}
		topology, err := deviceTopologyRegistry.GetDeviceTopology(name)
		if err != nil {
			return nil, err
		}
		requestCopy := *deviceReq
		filter := func(ids []string) []string {
			result := make([]string, 0, len(ids))
			for _, id := range ids {
				if healthy, exists := topology.IsDeviceHealthy(id); exists && healthy {
					allocation := machineState[gpuconsts.GPUDeviceType][id]
					if allocation != nil && allocation.Allocatable > 0 {
						result = append(result, id)
					}
				}
			}
			return result
		}
		requestCopy.AvailableDevices = filter(deviceReq.AvailableDevices)
		requestCopy.ReusableDevices = filter(deviceReq.ReusableDevices)
		deviceReq = &requestCopy
		break
	}

	// Get hint nodes
	hintNodes, err := machine.NewCPUSetUint64(deviceReq.GetHint().GetNodes()...)
	if err != nil {
		return &allocate.AllocationResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to get hint nodes: %v", err),
		}, err
	}

	// Create allocation context
	ctx := &allocate.AllocationContext{
		ResourceReq:            resourceReq,
		DeviceReq:              deviceReq,
		DeviceTopologyRegistry: deviceTopologyRegistry,
		GPUQRMPluginConfig:     gpuConfig,
		Emitter:                emitter,
		MetaServer:             metaServer,
		MachineState:           machineState,
		QoSLevel:               qosLevel,
		HintNodes:              hintNodes,
		ResourceName:           resourceName,
		AccompanyResourceName:  accompanyResourceName,
		DeviceNameToTypeMap:    deviceNameToTypeMap,
	}

	// Get the global strategy manager and perform allocation
	manager := GetGlobalStrategyManager(gpuConfig)
	return manager.AllocateUsingStrategy(ctx)
}
