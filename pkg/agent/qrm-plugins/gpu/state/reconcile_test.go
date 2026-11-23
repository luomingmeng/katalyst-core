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

package state

import (
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/qrm"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/statedirectory"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
)

func TestReconcileLateDevicesAndAllocatedDisappearance(t *testing.T) {
	r := machine.NewDeviceTopologyRegistry(metrics.DummyMetrics{})
	r.RegisterDeviceTopologyProvider("gpu", machine.NewDeviceTopologyProvider())
	update := func(ids ...string) {
		t.Helper()
		topo := &machine.DeviceTopology{Devices: map[string]machine.DeviceInfo{}, UpdateTime: 1}
		for _, id := range ids {
			topo.Devices[id] = machine.DeviceInfo{Health: "Healthy", NumaNodes: []int{0}}
		}
		require.NoError(t, r.SetDeviceTopology("gpu", topo))
	}
	registry := NewDefaultResourceStateGeneratorRegistry()
	for name, capacity := range map[string]float64{"gpu_device": 1, "memory": 100, "compute": 1000} {
		registry.RegisterResourceStateGenerator(name, NewGenericDefaultResourceStateGenerator([]string{"gpu"}, r, capacity, true))
	}
	entries := PodResourceEntries{}
	for name, quantity := range map[v1.ResourceName]float64{"gpu_device": 1, "memory": 20, "compute": 250} {
		entries[name] = PodEntries{"pod": ContainerEntries{"container": &AllocationInfo{AllocatedAllocation: Allocation{Quantity: quantity, NUMANodes: []int{0}}, DeviceName: "gpu", TopologyAwareAllocations: map[string]Allocation{"gpu-0": {Quantity: quantity, NUMANodes: []int{0}}}}}}
	}
	update("gpu-0")
	current, err := GenerateMachineStateFromPodEntries(entries, registry)
	require.NoError(t, err)
	// Only one resource misses a device: each resource must be checked independently.
	current["memory"] = AllocationMap{}
	update("gpu-0", "gpu-1")
	next, err := ReconcileMachineState(current, entries, registry)
	require.NoError(t, err)
	for name := range entries {
		require.Len(t, next[name], 2)
		require.Equal(t, entries[name]["pod"]["container"].TopologyAwareAllocations["gpu-0"].Quantity, next[name]["gpu-0"].GetQuantityAllocated())
		require.Empty(t, next[name]["gpu-1"].PodEntries)
	}
	require.Empty(t, current["memory"])
	again, err := ReconcileMachineState(next, entries, registry)
	require.NoError(t, err)
	require.Equal(t, next, again)
	update("gpu-1")
	dropped, err := ReconcileMachineState(next, entries, registry)
	require.NoError(t, err)
	for name := range entries {
		require.Zero(t, dropped[name]["gpu-0"].Allocatable)
		require.NotEmpty(t, dropped[name]["gpu-0"].PodEntries)
	}
	// Other rebuild paths and checkpoint restoration use the same generation helper.
	rebuilt, err := GenerateMachineStateFromPodEntries(entries, registry)
	require.NoError(t, err)
	for name := range entries {
		require.Equal(t, dropped[name]["gpu-0"], rebuilt[name]["gpu-0"])
	}
	// Exercise real checkpoint write/read with an allocated ID absent from
	// current discovery; the checkpoint schema remains unchanged.
	directory := &statedirectory.StateDirectoryConfiguration{StateFileDirectory: t.TempDir()}
	conf := qrm.NewQRMPluginsConfiguration()
	checkpoint, err := NewCheckpointState(directory, conf, "gpu-state", "static", registry, false, metrics.DummyMetrics{})
	require.NoError(t, err)
	checkpoint.SetPodResourceEntries(entries, false)
	checkpoint.SetMachineState(dropped, false)
	require.NoError(t, checkpoint.StoreState())
	loaded, err := NewCheckpointState(directory, conf, "gpu-state", "static", registry, false, metrics.DummyMetrics{})
	require.NoError(t, err)
	require.Equal(t, entries, loaded.GetPodResourceEntries())
	for name := range entries {
		require.Equal(t, dropped[name]["gpu-0"], loaded.GetMachineState()[name]["gpu-0"])
	}
	update("gpu-0", "gpu-1")
	restored, err := ReconcileMachineState(dropped, entries, registry)
	require.NoError(t, err)
	require.Equal(t, next, restored)
	delete(entries["gpu_device"], "pod")
	update("gpu-1")
	released, err := GenerateMachineStateFromPodEntries(entries, registry)
	require.NoError(t, err)
	require.NotContains(t, released["gpu_device"], "gpu-0")
}
