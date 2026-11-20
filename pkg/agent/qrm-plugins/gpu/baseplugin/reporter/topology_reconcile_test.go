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
	"context"
	"encoding/json"
	"testing"

	reportapi "github.com/kubewharf/katalyst-api/pkg/protocol/reporterplugin/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/metrics"

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

func TestDroppedGPUZoneLivesUntilLastAllocationIsReleased(t *testing.T) {
	r := machine.NewDeviceTopologyRegistry(metrics.DummyMetrics{})
	r.RegisterDeviceTopologyProvider("gpu", machine.NewDeviceTopologyProvider())
	update := func(ids ...string) {
		t.Helper()
		topology := &machine.DeviceTopology{Devices: map[string]machine.DeviceInfo{}}
		for _, id := range ids {
			topology.Devices[id] = machine.DeviceInfo{Health: "Healthy", NumaNodes: []int{0}}
		}
		require.NoError(t, r.SetDeviceTopology("gpu", topology))
	}
	update("gpu-0", "gpu-1")
	registry := state.NewDefaultResourceStateGeneratorRegistry()
	for name, capacity := range map[string]float64{gpuconsts.GPUDeviceType: 1, string(apiConsts.ResourceGPUMemory): 100, string(apiConsts.ResourceMilliGPU): 1000} {
		registry.RegisterResourceStateGenerator(name, state.NewGenericDefaultResourceStateGenerator([]string{"gpu"}, r, capacity, true))
	}
	registry.RegisterResourceStateRetentionGroup(gpuconsts.GPUResourceRetentionGroup,
		gpuconsts.GPUDeviceType, string(apiConsts.ResourceGPUMemory), string(apiConsts.ResourceMilliGPU))
	s, err := state.NewGPUPluginState(nil, registry)
	require.NoError(t, err)
	entries := state.PodResourceEntries{}
	for name, quantity := range map[v1.ResourceName]float64{gpuconsts.GPUDeviceType: 1, apiConsts.ResourceGPUMemory: 20, apiConsts.ResourceMilliGPU: 250} {
		deviceName := ""
		if name == gpuconsts.GPUDeviceType {
			deviceName = "gpu"
		}
		entries[name] = state.PodEntries{"existing": state.ContainerEntries{"container": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{PodUid: "existing", PodNamespace: "default", PodName: "existing", ContainerName: "container"}, DeviceName: deviceName, AllocatedAllocation: state.Allocation{Quantity: quantity, NUMANodes: []int{0}}, TopologyAwareAllocations: map[string]state.Allocation{"gpu-1": {Quantity: quantity, NUMANodes: []int{0}}}}}}
	}
	commit := func() {
		t.Helper()
		m, err := state.GenerateMachineStateFromPodEntries(entries, registry)
		require.NoError(t, err)
		s.SetPodResourceEntries(entries, false)
		s.SetMachineState(m, false)
	}
	commit()
	p := &gpuReporterPlugin{ctx: context.Background(), deviceTopologyRegistry: r, stateGetter: func() state.State { return s }, gpuDeviceNames: []string{"gpu"}, deviceTypeToNames: map[string]sets.String{gpuconsts.GPUDeviceType: sets.NewString("gpu")}, numaSocketZoneNodeMap: map[util.ZoneNode]util.ZoneNode{util.GenerateNumaZoneNode(0): util.GenerateSocketZoneNode(0)}}
	report := func() *nodeapi.TopologyZone {
		t.Helper()
		p.Trigger()
		resp, err := p.GetReportContent(context.Background(), &reportapi.Empty{})
		require.NoError(t, err)
		var zones []*nodeapi.TopologyZone
		require.NoError(t, json.Unmarshal(resp.Content[0].Field[0].Value, &zones))
		var visit func([]*nodeapi.TopologyZone) *nodeapi.TopologyZone
		visit = func(zs []*nodeapi.TopologyZone) *nodeapi.TopologyZone {
			for _, z := range zs {
				if z.Name == "gpu-1" && z.Type == nodeapi.TopologyTypeGPU {
					return z
				}
				if found := visit(z.Children); found != nil {
					return found
				}
			}
			return nil
		}
		return visit(zones)
	}
	full := report()
	require.NotNil(t, full)
	require.NotEmpty(t, full.Allocations)
	update("gpu-0")
	m, err := state.ReconcileMachineState(s.GetMachineState(), entries, registry)
	require.NoError(t, err)
	s.SetMachineState(m, false)
	dropped := report()
	require.NotNil(t, dropped, "allocated GPU zone must survive missing discovery")
	require.Equal(t, full.Allocations, dropped.Allocations)
	require.NotNil(t, dropped.Resources.Allocatable)
	for _, name := range []v1.ResourceName{"gpu", apiConsts.ResourceGPUMemory, apiConsts.ResourceMilliGPU} {
		q := (*dropped.Resources.Allocatable)[name]
		require.True(t, q.IsZero())
	}
	// Same ID returns before release: capacity returns, allocation remains exactly once.
	update("gpu-0", "gpu-1")
	m, err = state.ReconcileMachineState(s.GetMachineState(), entries, registry)
	require.NoError(t, err)
	s.SetMachineState(m, false)
	restored := report()
	require.NotNil(t, restored)
	require.Equal(t, full.Allocations, restored.Allocations)
	for _, name := range []v1.ResourceName{"gpu", apiConsts.ResourceGPUMemory, apiConsts.ResourceMilliGPU} {
		q := (*restored.Resources.Allocatable)[name]
		require.False(t, q.IsZero())
	}
	update("gpu-0")
	commit()
	delete(entries, gpuconsts.GPUDeviceType)
	commit()
	require.Contains(t, s.GetMachineState()[gpuconsts.GPUDeviceType], "gpu-1", "virtual resource allocation must retain physical device entry")
	partial := report()
	require.NotNil(t, partial)
	require.NotEmpty(t, partial.Allocations)
	foundMemory := false
	for _, allocation := range partial.Allocations {
		requests := *allocation.Requests
		require.NotContains(t, requests, v1.ResourceName("gpu"))
		if q, ok := requests[apiConsts.ResourceGPUMemory]; ok {
			require.Equal(t, int64(20), q.Value())
			foundMemory = true
		}
	}
	require.True(t, foundMemory, "GPU memory allocation is retained on the unavailable card")
	// If the same ID returns while only a virtual allocation is left, the
	// physical card must stay unallocatable rather than being handed out twice.
	update("gpu-0", "gpu-1")
	commit()
	recoveredPhysical := report()
	require.NotNil(t, recoveredPhysical)
	physical := (*recoveredPhysical.Resources.Allocatable)[v1.ResourceName("gpu")]
	require.True(t, physical.IsZero())
	virtual := (*recoveredPhysical.Resources.Allocatable)[apiConsts.ResourceGPUMemory]
	require.False(t, virtual.IsZero())
	update("gpu-0")
	commit()
	delete(entries, apiConsts.ResourceGPUMemory)
	commit()
	require.NotNil(t, report(), "milligpu is still allocated")
	delete(entries, apiConsts.ResourceMilliGPU)
	commit()
	require.Nil(t, report(), "delete GPU zone only after its last allocation is released")
	for _, allocMap := range s.GetMachineState() {
		require.NotContains(t, allocMap, "gpu-1")
	}
	require.Contains(t, rMustTopology(t, r).Devices, "gpu-0")
}

func rMustTopology(t *testing.T, r *machine.DeviceTopologyRegistry) *machine.DeviceTopology {
	t.Helper()
	topology, err := r.GetDeviceTopology("gpu")
	require.NoError(t, err)
	return topology
}

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
