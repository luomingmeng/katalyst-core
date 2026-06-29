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

package virtual_gpu

import (
	"fmt"
	"sort"

	"github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/util"
	qrmutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

// Sort sorts the filtered GPU devices based on available GPU resources (memory & milligpu)
// It prioritizes devices with less available memory and considers NUMA affinity.
// When the request only carries ResourceMilliGPU (no GPU memory), sorting falls back to
// milligpu headroom so that milligpu-only requests still benefit from spread/pack ordering
// rather than returning the unsorted slice.
// TODO: support multiple resources, prioritize virtual_gpu for sorting, then milligpu
func (s *VirtualGPUStrategy) Sort(ctx *allocate.AllocationContext, filteredDevices []string) ([]string, error) {
	if ctx.DeviceTopologyRegistry == nil {
		return nil, fmt.Errorf("GPU topology registry is nil")
	}

	gpuTopology, err := ctx.DeviceTopologyRegistry.GetDeviceTopology(ctx.ResourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get gpu topology: %w", err)
	}

	_, gpuMemory, memErr := qrmutil.GetQuantityFromResourceRequests(ctx.ResourceReq.ResourceRequests, string(consts.ResourceGPUMemory), nil)
	_, milliGPU, milliErr := qrmutil.GetQuantityFromResourceRequests(ctx.ResourceReq.ResourceRequests, string(consts.ResourceMilliGPU), nil)
	if memErr != nil && milliErr != nil {
		general.Warningf("getReqQuantityFromResourceReq failed for both gpu_memory (err=%v) and milligpu (err=%v), use default filtered devices", memErr, milliErr)
		return filteredDevices, nil
	}
	if gpuMemory == 0 && milliGPU == 0 {
		general.Infof("no non-zero gpu_memory or milligpu request, use default available devices")
		return filteredDevices, nil
	}

	gpuMemoryAllocatablePerGPU := ctx.GPUQRMPluginConfig.GPUMemoryAllocatablePerGPU.Value()
	milliGPUAllocatablePerGPU := ctx.GPUQRMPluginConfig.MilliGPUAllocatablePerGPU.Value()
	gpuMemoryMachineState := ctx.MachineState[consts.ResourceGPUMemory]
	milliGPUMachineState := ctx.MachineState[consts.ResourceMilliGPU]

	// Create a slice of device info with available memory and available milligpu
	type deviceInfo struct {
		ID                string
		AvailableMemory   int64
		AvailableMilliGPU int64
		NUMAAffinity      bool
	}

	devices := make([]deviceInfo, 0, len(filteredDevices))

	for _, deviceID := range filteredDevices {
		availableMemory := gpuMemoryAllocatablePerGPU - int64(gpuMemoryMachineState.GetQuantityAllocated(deviceID))
		availableMilliGPU := milliGPUAllocatablePerGPU - int64(milliGPUMachineState.GetQuantityAllocated(deviceID))
		devices = append(devices, deviceInfo{
			ID:                deviceID,
			AvailableMemory:   availableMemory,
			AvailableMilliGPU: availableMilliGPU,
			NUMAAffinity:      util.IsNUMAAffinityDevice(deviceID, gpuTopology, ctx.HintNodes),
		})
	}

	// Get config flag for spreading/packing.
	// If spreading is true, sort in descending order to allocate from devices with more available resources.
	// If false (packing), sort in ascending order to pack resources tightly.
	spreading := ctx.GPUQRMPluginConfig.VirtualGPUPrefersSpreading

	// Sort devices: first by NUMA affinity (preferred), then by available memory (ascending for packing, descending for spreading), then by available milligpu (same order)
	sort.Slice(devices, func(i, j int) bool {
		// First, check NUMA affinity
		if devices[i].NUMAAffinity != devices[j].NUMAAffinity {
			return devices[i].NUMAAffinity && !devices[j].NUMAAffinity
		}
		// Next, check available GPU memory
		if devices[i].AvailableMemory != devices[j].AvailableMemory {
			if spreading {
				return devices[i].AvailableMemory > devices[j].AvailableMemory
			}
			return devices[i].AvailableMemory < devices[j].AvailableMemory
		}
		// Finally, check available milligpu
		if spreading {
			return devices[i].AvailableMilliGPU > devices[j].AvailableMilliGPU
		}
		return devices[i].AvailableMilliGPU < devices[j].AvailableMilliGPU
	})

	// Extract sorted device IDs
	sortedDevices := make([]string, len(devices))
	for i, device := range devices {
		sortedDevices[i] = device.ID
	}

	general.InfoS("Sorted devices", "count", len(sortedDevices), "devices", sortedDevices)
	return sortedDevices, nil
}
