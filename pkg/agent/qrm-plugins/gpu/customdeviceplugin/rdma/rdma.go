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

package rdma

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/baseplugin"
	gpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/customdeviceplugin"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate/manager"
	gpuutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/util"
	qrmutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const RDMACustomDevicePluginName = "rdma-custom-device-plugin"

type RDMADevicePlugin struct {
	*baseplugin.BasePlugin
	deviceNames []string
}

func NewRDMADevicePlugin(base *baseplugin.BasePlugin) customdeviceplugin.CustomDevicePlugin {
	for _, deviceName := range base.Conf.RDMADeviceNames {
		rdmaTopologyProvider := machine.NewDeviceTopologyProvider()
		base.DeviceTopologyRegistry.RegisterDeviceTopologyProvider(deviceName, rdmaTopologyProvider)
	}

	// RDMADeviceType is the key used for RDMA state management in the QRM framework,
	// while RDMADeviceNames are the actual resource names used to fetch the RDMA device topologies
	base.DefaultResourceStateGeneratorRegistry.RegisterResourceStateGenerator(gpuconsts.RDMADeviceType,
		state.NewGenericDefaultResourceStateGenerator(base.Conf.RDMADeviceNames, base.DeviceTopologyRegistry, 1, false))
	base.RegisterDeviceNames(base.Conf.RDMADeviceNames, gpuconsts.RDMADeviceType)

	return &RDMADevicePlugin{
		BasePlugin:  base,
		deviceNames: base.Conf.RDMADeviceNames,
	}
}

func (p *RDMADevicePlugin) DefaultPreAllocateResourceName() string {
	return ""
}

func (p *RDMADevicePlugin) DeviceNames() []string {
	return p.deviceNames
}

func (p *RDMADevicePlugin) UpdateAllocatableAssociatedDevices(ctx context.Context, request *pluginapi.UpdateAllocatableAssociatedDevicesRequest) (*pluginapi.UpdateAllocatableAssociatedDevicesResponse, error) {
	return p.BasePlugin.UpdateAllocatableAssociatedDevices(request)
}

func (p *RDMADevicePlugin) GetAssociatedDeviceTopologyHints(ctx context.Context, req *pluginapi.AssociatedDeviceRequest) (*pluginapi.AssociatedDeviceHintsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("GetAssociatedDeviceTopologyHints got nil req")
	}

	resReq := req.ResourceRequest
	if resReq == nil {
		return nil, fmt.Errorf("GetAssociatedDeviceTopologyHints got nil resReq")
	}

	qosLevel, err := qrmutil.GetKatalystQoSLevelFromResourceReq(p.Conf.QoSConfiguration, resReq, p.PodAnnotationKeptKeys, p.PodLabelKeptKeys)
	if err != nil {
		err = fmt.Errorf("GetKatalystQoSLevelFromResourceReq for pod: %s/%s, container: %s failed with error: %v",
			resReq.PodNamespace, resReq.PodName, resReq.ContainerName, err)
		general.Errorf("%s", err.Error())
		return nil, err
	}

	general.InfoS("called",
		"podNamespace", resReq.PodNamespace,
		"podName", resReq.PodName,
		"containerName", resReq.ContainerName,
		"deviceName", req.DeviceName,
		"qosLevel", qosLevel,
	)

	// Find the target device request
	var targetDeviceReq *pluginapi.DeviceRequest
	for _, deviceRequest := range req.DeviceRequest {
		if deviceRequest.DeviceName == req.DeviceName {
			targetDeviceReq = deviceRequest
			break
		}
	}

	if targetDeviceReq == nil {
		return nil, fmt.Errorf("no target device plugin found for target device %s", req.DeviceName)
	}

	var hints []*pluginapi.TopologyHint

	// 1. Check if RDMA device allocation already exists.
	rdmaAllocationInfo := p.GetState().GetAllocationInfo(gpuconsts.RDMADeviceType, resReq.PodUid, resReq.ContainerName)
	if rdmaAllocationInfo != nil && rdmaAllocationInfo.TopologyAwareAllocations != nil {
		general.InfoS("generating hints from existing RDMA allocation",
			"podNamespace", resReq.PodNamespace,
			"podName", resReq.PodName,
			"containerName", resReq.ContainerName,
			"deviceName", req.DeviceName,
		)
		hints = p.generateHintsFromAllocation(rdmaAllocationInfo)
	} else {
		// 2. If it does not exist, get the rdmaTopology
		rdmaTopology, err := p.DeviceTopologyRegistry.GetDeviceTopology(targetDeviceReq.DeviceName)
		if err != nil {
			general.Warningf("failed to get rdma topology: %v", err)
			return nil, fmt.Errorf("failed to get rdma topology: %w", err)
		}

		hints = p.generateDeviceTopologyHints(targetDeviceReq, rdmaTopology, req.AccompanyResourceName, resReq)
	}

	if len(hints) == 0 {
		return nil, fmt.Errorf("GetAssociatedDeviceTopologyHints got empty hints")
	}

	return p.buildAssociatedDeviceHintsResponse(req, hints), nil
}

// AllocateAssociatedDevice check if rdma is allocated to other containers, make sure they do not share rdma
func (p *RDMADevicePlugin) AllocateAssociatedDevice(
	ctx context.Context, resReq *pluginapi.ResourceRequest, deviceReq *pluginapi.DeviceRequest, accompanyResourceName string,
) (*pluginapi.AssociatedDeviceAllocationResponse, error) {
	qosLevel, err := qrmutil.GetKatalystQoSLevelFromResourceReq(p.Conf.QoSConfiguration, resReq, p.PodAnnotationKeptKeys, p.PodLabelKeptKeys)
	if err != nil {
		err = fmt.Errorf("GetKatalystQoSLevelFromResourceReq for pod: %s/%s, container: %s failed with error: %v",
			resReq.PodNamespace, resReq.PodName, resReq.ContainerName, err)
		general.Errorf("%s", err.Error())
		return nil, err
	}

	general.InfoS("called",
		"podNamespace", resReq.PodNamespace,
		"podName", resReq.PodName,
		"containerName", resReq.ContainerName,
		"qosLevel", qosLevel,
		"reqAnnotations", resReq.Annotations,
		"resourceRequests", resReq.ResourceRequests,
		"deviceName", deviceReq.DeviceName,
		"resourceHint", resReq.Hint,
		"deviceHint", deviceReq.Hint,
		"availableDevices", deviceReq.AvailableDevices,
		"reusableDevices", deviceReq.ReusableDevices,
		"deviceRequest", deviceReq.DeviceRequest,
	)

	// Check if there is state for the device name
	rdmaAllocationInfo := p.GetState().GetAllocationInfo(gpuconsts.RDMADeviceType, resReq.PodUid, resReq.ContainerName)
	if rdmaAllocationInfo != nil && rdmaAllocationInfo.TopologyAwareAllocations != nil {
		allocatedDevices := make([]string, 0, len(rdmaAllocationInfo.TopologyAwareAllocations))
		for rdmaID := range rdmaAllocationInfo.TopologyAwareAllocations {
			allocatedDevices = append(allocatedDevices, rdmaID)
		}
		return &pluginapi.AssociatedDeviceAllocationResponse{
			AllocationResult: &pluginapi.AssociatedDeviceAllocation{
				AllocatedDevices: allocatedDevices,
			},
		}, nil
	}

	rdmaDeviceName := deviceReq.DeviceName
	rdmaTopology, err := p.DeviceTopologyRegistry.GetDeviceTopology(rdmaDeviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get gpu device topology: %v", err)
	}

	hintNodes, err := machine.NewCPUSetUint64(deviceReq.GetHint().GetNodes()...)
	if err != nil {
		general.Warningf("failed to get hint nodes: %v", err)
		return nil, err
	}

	// Use strategy framework to allocate RDMA devices
	result, err := manager.AllocateDevicesUsingStrategy(
		resReq,
		deviceReq,
		p.DeviceTopologyRegistry,
		p.Conf.GPUQRMPluginConfig,
		p.Emitter,
		p.MetaServer,
		p.GetState().GetMachineState(),
		qosLevel,
		rdmaDeviceName,
		accompanyResourceName,
		p.GetDeviceNameToTypeMap(),
	)
	if err != nil {
		return nil, fmt.Errorf("RDMA allocation using strategy failed: %v", err)
	}

	if !result.Success {
		return nil, fmt.Errorf("RDMA allocation failed: %v", result.ErrorMessage)
	}

	allocatedRdmaDevices := result.AllocatedDevices

	// Modify rdma state
	topologyAwareAllocations := make(map[string]state.Allocation)
	for _, deviceID := range allocatedRdmaDevices {
		info, ok := rdmaTopology.Devices[deviceID]
		if !ok {
			return nil, fmt.Errorf("failed to get rdma info for device %s", deviceID)
		}

		topologyAwareAllocations[deviceID] = state.Allocation{
			Quantity:  1,
			NUMANodes: info.GetNUMANodes(),
		}
	}

	allocationInfo := &state.AllocationInfo{
		AllocationMeta: commonstate.GenerateGenericContainerAllocationMeta(resReq, commonstate.EmptyOwnerPoolName, qosLevel),
		AllocatedAllocation: state.Allocation{
			Quantity:  float64(len(allocatedRdmaDevices)),
			NUMANodes: hintNodes.ToSliceInt(),
		},
		DeviceName: deviceReq.DeviceName,
	}

	allocationInfo.TopologyAwareAllocations = topologyAwareAllocations
	p.GetState().SetAllocationInfo(gpuconsts.RDMADeviceType, resReq.PodUid, resReq.ContainerName, allocationInfo, false)
	resourceState, err := p.GenerateResourceStateFromPodEntries(gpuconsts.RDMADeviceType, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to generate rdma device state from pod entries: %v", err)
	}

	p.GetState().SetResourceState(gpuconsts.RDMADeviceType, resourceState, true)

	general.InfoS("allocated rdma devices",
		"podNamespace", resReq.PodNamespace,
		"podName", resReq.PodName,
		"containerName", resReq.ContainerName,
		"qosLevel", qosLevel,
		"allocatedRdmaDevices", allocatedRdmaDevices)

	return &pluginapi.AssociatedDeviceAllocationResponse{
		AllocationResult: &pluginapi.AssociatedDeviceAllocation{
			AllocatedDevices: allocatedRdmaDevices,
		},
	}, nil
}

func (p *RDMADevicePlugin) generateHintsFromAllocation(allocationInfo *state.AllocationInfo) []*pluginapi.TopologyHint {
	nodesSet := sets.NewInt()
	for _, alloc := range allocationInfo.TopologyAwareAllocations {
		nodesSet.Insert(alloc.NUMANodes...)
	}

	nodes := make([]uint64, 0, nodesSet.Len())
	for _, node := range nodesSet.List() {
		nodes = append(nodes, uint64(node))
	}

	return []*pluginapi.TopologyHint{
		{
			Nodes:     nodes,
			Preferred: true,
		},
	}
}

func (p *RDMADevicePlugin) generateDeviceTopologyHints(
	deviceReq *pluginapi.DeviceRequest,
	rdmaTopology *machine.DeviceTopology,
	accompanyResourceName string,
	resReq *pluginapi.ResourceRequest,
) []*pluginapi.TopologyHint {
	request := int(deviceReq.DeviceRequest)
	available := sets.NewString(deviceReq.AvailableDevices...)
	reusable := sets.NewString(deviceReq.ReusableDevices...)

	// In scenarios where RDMA is requested implicitly (request == 0) alongside an accompany resource (e.g., GPU),
	// the number of RDMA devices needed is proportional to the number of accompany devices allocated to the container.
	if request == 0 && accompanyResourceName != "" {
		machineState := p.GetState().GetMachineState()

		accompanyResourceNameInState := gpuutil.ResolveResourceName(p.GetDeviceNameToTypeMap(), accompanyResourceName, false)
		resourceNameInState := gpuutil.ResolveResourceName(p.GetDeviceNameToTypeMap(), deviceReq.DeviceName, false)

		accompanyAllocatedDeviceIDs := machineState.GetAllocatedDeviceIDs(v1.ResourceName(accompanyResourceNameInState), resReq.PodUid, resReq.ContainerName)

		if len(accompanyAllocatedDeviceIDs) > 0 {
			// Use the node-level ratio of accompany resources to target resources to determine
			// the fair share of RDMA devices for the already allocated accompany devices.
			accompanyResourceToDeviceRatio := machineState.GetRatioOfAccompanyResourceToTargetResource(accompanyResourceNameInState, resourceNameInState)
			request = machineState.CalculateTargetDevicesToAllocate(accompanyResourceToDeviceRatio, len(accompanyAllocatedDeviceIDs))
		}
	}

	if available.Union(reusable).Len() < request {
		general.Warningf("Unable to generate topology hints: requested number of devices unavailable, request: %d, available: %d",
			request, available.Union(reusable).Len())
		return nil
	}

	// Gather all NUMA nodes that have healthy RDMAs
	numaNodesSet := sets.NewInt()
	for _, dev := range rdmaTopology.Devices {
		if dev.Health != pluginapi.Healthy {
			continue
		}

		numaNodesSet.Insert(dev.NumaNodes...)
	}
	numaNodes := numaNodesSet.List()

	// minAffinitySize tracks the minimum mask size that satisfies the Strategy
	minAffinitySize := len(numaNodes)
	var hints []*pluginapi.TopologyHint

	// Iterate through all combinations of NUMA Nodes and build hints from them.
	machine.IterateBitMasks(numaNodes, len(numaNodes), func(mask machine.BitMask) {
		// Fast Path: Check to see if all of the reusable devices are part of the bitmask.
		numMatching := 0
		for d := range reusable {
			dev, ok := rdmaTopology.Devices[d]
			if !ok {
				general.Errorf("Reusable device %s not found in topology", d)
				continue
			}
			if len(dev.NumaNodes) == 0 {
				general.Errorf("Reusable device %s has no NUMA nodes", d)
				continue
			}
			if !mask.AnySet(dev.NumaNodes) {
				return
			}
			numMatching++
		}

		// Fast Path: Check to see if enough available devices remain on the
		// current NUMA node combination to satisfy the device request.
		for d := range available {
			dev, ok := rdmaTopology.Devices[d]
			if !ok {
				general.Errorf("Available device %s not found in topology", d)
				continue
			}
			if len(dev.NumaNodes) == 0 {
				general.Errorf("Available device %s has no NUMA nodes", d)
				continue
			}
			if mask.AnySet(dev.NumaNodes) {
				numMatching++
			}
		}

		// If they don't, then move onto the next combination.
		if numMatching < request {
			return
		}

		bits := mask.GetBits()
		nodes := make([]uint64, len(bits))
		for i, bit := range bits {
			nodes[i] = uint64(bit)
		}

		if mask.Count() < minAffinitySize {
			minAffinitySize = mask.Count()
		}

		// Add it to the list of hints. We set all hint preferences to 'false' on the first pass through.
		hints = append(hints, &pluginapi.TopologyHint{
			Nodes:     nodes,
			Preferred: false,
		})
	})

	for i := range hints {
		if len(hints[i].Nodes) == minAffinitySize {
			hints[i].Preferred = true
		}
	}

	return hints
}

func (p *RDMADevicePlugin) buildAssociatedDeviceHintsResponse(
	req *pluginapi.AssociatedDeviceRequest,
	hints []*pluginapi.TopologyHint,
) *pluginapi.AssociatedDeviceHintsResponse {
	resReq := req.ResourceRequest
	var deviceHints *pluginapi.ListOfTopologyHints
	if hints != nil {
		deviceHints = &pluginapi.ListOfTopologyHints{Hints: hints}
	}
	return &pluginapi.AssociatedDeviceHintsResponse{
		PodUid:         resReq.PodUid,
		PodNamespace:   resReq.PodNamespace,
		PodName:        resReq.PodName,
		ContainerName:  resReq.ContainerName,
		ContainerType:  resReq.ContainerType,
		ContainerIndex: resReq.ContainerIndex,
		PodRole:        resReq.PodRole,
		PodType:        resReq.PodType,
		DeviceName:     req.DeviceName,
		DeviceHints:    deviceHints,
		Labels:         resReq.Labels,
		Annotations:    resReq.Annotations,
	}
}
