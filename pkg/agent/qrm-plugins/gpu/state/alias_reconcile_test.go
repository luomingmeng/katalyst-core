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
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewAliasDoesNotRemoveDevicesStillPresentInOtherAlias(t *testing.T) {
	r := machine.NewDeviceTopologyRegistry(metrics.DummyMetrics{})
	r.RegisterDeviceTopologyProvider("gpu", machine.NewDeviceTopologyProvider())
	r.RegisterDeviceTopologyProvider("gpu-subset", machine.NewDeviceTopologyProvider())
	require.NoError(t, r.SetDeviceTopology("gpu", &machine.DeviceTopology{UpdateTime: 1, Devices: map[string]machine.DeviceInfo{"gpu-0": {Health: "Healthy"}, "gpu-1": {Health: "Healthy"}}}))
	reg := NewDefaultResourceStateGeneratorRegistry()
	reg.RegisterResourceStateGenerator("gpu_device", NewGenericDefaultResourceStateGenerator([]string{"gpu", "gpu-subset"}, r, 1, true))
	current, err := GenerateMachineState(reg)
	require.NoError(t, err)
	require.Len(t, current["gpu_device"], 2)
	require.NoError(t, r.SetDeviceTopology("gpu-subset", &machine.DeviceTopology{UpdateTime: 2, Devices: map[string]machine.DeviceInfo{"gpu-0": {Health: "Healthy"}}}))
	result, err := ReconcileMachineState(current, nil, reg)
	require.NoError(t, err)
	valid, err := r.GetDeviceTopology("gpu")
	require.NoError(t, err)
	require.Contains(t, valid.Devices, "gpu-1")
	t.Logf("newest alias gpu-subset contains gpu-0; gpu still lists both healthy; resource devices after reconcile=%v", result["gpu_device"])
	require.Contains(t, result["gpu_device"], "gpu-1", "absence in another resource-name snapshot must not remove present device")
	rebuilt, err := GenerateMachineStateFromPodEntries(nil, reg)
	require.NoError(t, err)
	require.Equal(t, result, rebuilt)
	// Overlapping ID data uses its latest source; absent IDs remain in the union.
	require.NoError(t, r.SetDeviceTopology("gpu-subset", &machine.DeviceTopology{UpdateTime: 3, Devices: map[string]machine.DeviceInfo{"gpu-0": {Health: "Unhealthy", NumaNodes: []int{1}}}}))
	merged, err := GetMergedDeviceTopology(r, []string{"gpu", "gpu-subset"})
	require.NoError(t, err)
	require.Len(t, merged.Devices, 2)
	require.Equal(t, "Unhealthy", merged.Devices["gpu-0"].Health)
	require.Equal(t, []int{1}, merged.Devices["gpu-0"].NumaNodes)
	require.Equal(t, "Healthy", merged.Devices["gpu-1"].Health)
	// Only absence from every relevant name removes unallocated resource state.
	require.NoError(t, r.SetDeviceTopology("gpu", &machine.DeviceTopology{UpdateTime: 4, Devices: map[string]machine.DeviceInfo{"gpu-0": {Health: "Healthy"}}}))
	dropped, err := ReconcileMachineState(result, nil, reg)
	require.NoError(t, err)
	require.NotContains(t, dropped["gpu_device"], "gpu-1")
}
