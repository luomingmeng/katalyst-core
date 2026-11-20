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

package staticpolicy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	commonstate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	gpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// A device update must repair state without a Pod admission or resource-query RPC.
func TestTopologyUpdateRepairsPartialState(t *testing.T) {
	p := makeTestStaticPolicy(t)
	update := func(ids ...string) {
		t.Helper()
		req := &pluginapi.UpdateAllocatableAssociatedDevicesRequest{DeviceName: testResourcePluginName}
		for _, id := range ids {
			req.Devices = append(req.Devices, &pluginapi.AssociatedDevice{ID: id, Health: "Healthy"})
		}
		_, err := p.UpdateAllocatableAssociatedDevices(context.Background(), req)
		require.NoError(t, err)
	}
	update("gpu-0")
	initialState, err := state.GenerateMachineState(p.DefaultResourceStateGeneratorRegistry)
	require.NoError(t, err)
	p.GetState().SetMachineState(initialState, false)
	before := p.GetState().GetMachineState()
	require.Len(t, before[gpuconsts.GPUDeviceType], 1)
	p.GetState().SetAllocationInfo(gpuconsts.GPUDeviceType, "pod", "container", &state.AllocationInfo{
		DeviceName:               testResourcePluginName,
		TopologyAwareAllocations: map[string]state.Allocation{"gpu-0": {Quantity: 1, NUMANodes: []int{0}}},
	}, false)
	generated, err := state.GenerateMachineStateFromPodEntries(p.GetState().GetPodResourceEntries(), p.DefaultResourceStateGeneratorRegistry)
	require.NoError(t, err)
	p.GetState().SetMachineState(generated, false)
	probe := &reconcileStoreProbe{State: p.GetState(), stored: make(chan struct{}, 1), skipDisk: true}
	p.SetState(probe)
	// Force one initial repair so that the worker's first pass is observed
	// before the late topology arrives. Otherwise the test could pass without
	// any topology notification being consumed.
	initial := probe.GetMachineState()
	delete(initial, "resource.katalyst.kubewharf.io/gpu_memory")
	probe.SetMachineState(initial, false)
	require.NoError(t, p.Start())
	t.Cleanup(func() { require.NoError(t, p.Stop()) })
	select {
	case <-probe.stored:
	case <-time.After(time.Second):
		t.Fatal("initial reconcile did not finish")
	}
	update("gpu-0", "gpu-1")
	require.Eventually(t, func() bool {
		m := p.GetState().GetMachineState()
		return len(m[gpuconsts.GPUDeviceType]) == 2
	}, time.Second, time.Millisecond*10)
	require.NotNil(t, p.GetState().GetAllocationInfo(gpuconsts.GPUDeviceType, "pod", "container"))
}

type reconcileStoreProbe struct {
	state.State
	mu       sync.Mutex
	attempts int
	fail     bool
	skipDisk bool
	stored   chan struct{}
}

func (s *reconcileStoreProbe) StoreState() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.fail {
		return errors.New("checkpoint unavailable")
	}
	if s.stored != nil {
		select {
		case s.stored <- struct{}{}:
		default:
		}
	}
	if s.skipDisk {
		return nil
	}
	return s.State.StoreState()
}

func TestReconcilePersistenceRetryWithoutStateDifference(t *testing.T) {
	p := makeTestStaticPolicy(t)
	probe := &reconcileStoreProbe{State: p.GetState(), fail: true}
	p.SetState(probe)
	entries := probe.GetPodResourceEntries()
	require.Error(t, p.reconcileDeviceState())
	require.True(t, p.persistPending)
	machineState := probe.GetMachineState()
	require.Len(t, machineState[gpuconsts.GPUDeviceType], 2)
	require.Error(t, p.reconcileDeviceState())
	require.Equal(t, 2, probe.attempts)
	probe.fail = false
	require.NoError(t, p.reconcileDeviceState())
	require.False(t, p.persistPending)
	require.Equal(t, 3, probe.attempts)
	require.NoError(t, p.reconcileDeviceState())
	require.Equal(t, 3, probe.attempts)
	require.Equal(t, machineState, probe.GetMachineState())
	require.Equal(t, entries, probe.GetPodResourceEntries())
}

func TestReconcilePeriodicFallbackAndStoppedWorker(t *testing.T) {
	p := makeTestStaticPolicy(t)
	probe := &reconcileStoreProbe{State: p.GetState(), stored: make(chan struct{}, 1), skipDisk: true}
	p.SetState(probe)
	stop := make(chan struct{})
	ticks := make(chan time.Time)
	done := make(chan struct{})
	go func() { defer close(done); p.reconcileDeviceStateLoop(stop, nil, ticks) }()
	t.Cleanup(func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		<-done
	})
	select {
	case <-probe.stored:
	case <-time.After(time.Second):
		t.Fatal("initial reconcile did not finish")
	}
	// No notifier is registered for this loop; only a periodic tick can repair it.
	topo := &machine.DeviceTopology{Devices: map[string]machine.DeviceInfo{"gpu-0": {Health: "Healthy"}, "gpu-1": {Health: "Healthy"}, "gpu-2": {Health: "Healthy"}}}
	require.NoError(t, p.DeviceTopologyRegistry.SetDeviceTopology(testResourcePluginName, topo))
	ticks <- time.Now()
	select {
	case <-probe.stored:
	case <-time.After(time.Second):
		t.Fatal("periodic reconcile did not finish")
	}
	require.Len(t, probe.GetMachineState()[gpuconsts.GPUDeviceType], 3)
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	// A closed stop channel must win before a queued notification starts new work.
	stopped := make(chan struct{})
	close(stopped)
	before := probe.attempts
	p.reconcileDeviceStateLoop(stopped, make(chan struct{}, 1), ticks)
	require.Equal(t, before, probe.attempts)
}

func TestEmptyTopologyWaitsAndEmptyStateIsRepaired(t *testing.T) {
	p := makeTestStaticPolicy(t)
	p.SetState(nil)
	require.NoError(t, p.DeviceTopologyRegistry.SetDeviceTopology(testResourcePluginName, &machine.DeviceTopology{Devices: map[string]machine.DeviceInfo{}}))
	require.Error(t, p.ensureState(""))
	require.Nil(t, p.GetState())
	require.NoError(t, p.reconcileDeviceState())
	require.Nil(t, p.GetState())
	// Simulate a checkpoint/state initialized by an older fork on empty discovery.
	require.NoError(t, p.InitState())
	require.Empty(t, p.GetState().GetMachineState()[gpuconsts.GPUDeviceType])
	_, err := p.UpdateAllocatableAssociatedDevices(context.Background(), &pluginapi.UpdateAllocatableAssociatedDevicesRequest{DeviceName: testResourcePluginName, Devices: []*pluginapi.AssociatedDevice{{ID: "gpu-0", Health: "Healthy"}}})
	require.NoError(t, err)
	require.NoError(t, p.reconcileDeviceState())
	require.Len(t, p.GetState().GetMachineState()[gpuconsts.GPUDeviceType], 1)
}

// TestColdStartEmptyDiscoveryWaitsThenLateRPCInitializesState drives the cold-start wait branch
// through the real RPC entry: empty discovery leaves state nil, and a late non-empty RPC
// initializes the store with no pod entry.
func TestColdStartEmptyDiscoveryWaitsThenLateRPCInitializesState(t *testing.T) {
	p := makeTestStaticPolicy(t)
	p.SetState(nil)

	// Empty discovery through the real entry: the cold-start wait branch leaves state nil.
	emptyReq := &pluginapi.UpdateAllocatableAssociatedDevicesRequest{DeviceName: testResourcePluginName}
	_, err := p.UpdateAllocatableAssociatedDevices(context.Background(), emptyReq)
	require.NoError(t, err)
	require.NoError(t, p.reconcileDeviceState())
	require.Nil(t, p.GetState())

	// Late non-empty discovery through the real entry initializes the store with no pod entry.
	_, err = p.UpdateAllocatableAssociatedDevices(context.Background(), &pluginapi.UpdateAllocatableAssociatedDevicesRequest{
		DeviceName: testResourcePluginName,
		Devices:    []*pluginapi.AssociatedDevice{{ID: "gpu-0", Health: "Healthy"}},
	})
	require.NoError(t, err)
	require.NoError(t, p.reconcileDeviceState())
	require.NotNil(t, p.GetState())
	require.Len(t, p.GetState().GetMachineState()[gpuconsts.GPUDeviceType], 1)
	require.Empty(t, p.GetState().GetPodResourceEntries())
}

// TestEmptyDiscoveryRetainsOwnedDevicesAndForbidsNewAllocation drives the real policy entry with
// an empty inventory after a card has been allocated. Each case owns the card through exactly one
// retention-group member; the other members only keep zero-capacity placeholders with no pod
// entries. The RPC reaches the registry, then a synchronous reconcile barrier converges state;
// topology-notifier and periodic worker convergence are covered by the existing worker tests.
func TestEmptyDiscoveryRetainsOwnedDevicesAndForbidsNewAllocation(t *testing.T) {
	ownerViews := []struct {
		name     string
		resource v1.ResourceName
		quantity float64
	}{
		{name: "gpu_device", resource: gpuconsts.GPUDeviceType, quantity: 1},
		{name: "gpu_memory", resource: apiconsts.ResourceGPUMemory, quantity: 20},
		{name: "gpu_milligpu", resource: apiconsts.ResourceMilliGPU, quantity: 250},
	}
	allViews := []v1.ResourceName{gpuconsts.GPUDeviceType, apiconsts.ResourceGPUMemory, apiconsts.ResourceMilliGPU}

	for _, tc := range ownerViews {
		t.Run(tc.name, func(t *testing.T) {
			p := makeTestStaticPolicy(t)

			entries := state.PodResourceEntries{
				tc.resource: state.PodEntries{"existing": state.ContainerEntries{"container": &state.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:        "existing",
						PodNamespace:  "default",
						PodName:       "existing",
						ContainerName: "container",
					},
					DeviceName:               testResourcePluginName,
					AllocatedAllocation:      state.Allocation{Quantity: tc.quantity, NUMANodes: []int{0}},
					TopologyAwareAllocations: map[string]state.Allocation{"gpu-0": {Quantity: tc.quantity, NUMANodes: []int{0}}},
				}}},
			}
			m, err := state.GenerateMachineStateFromPodEntries(entries, p.DefaultResourceStateGeneratorRegistry)
			require.NoError(t, err)
			p.GetState().SetPodResourceEntries(entries, false)
			p.GetState().SetMachineState(m, false)

			// Full discovery converges the owned card alongside an unowned one.
			require.NoError(t, p.reconcileDeviceState())

			// Real empty-inventory RPC through the policy entry, not a direct registry write.
			_, err = p.UpdateAllocatableAssociatedDevices(context.Background(),
				&pluginapi.UpdateAllocatableAssociatedDevicesRequest{DeviceName: testResourcePluginName})
			require.NoError(t, err)
			require.NoError(t, p.reconcileDeviceState())

			machineState := p.GetState().GetMachineState()
			owned, ok := machineState[tc.resource]["gpu-0"]
			require.True(t, ok, "gpu-0 must stay in the owner view %s after empty discovery", tc.resource)
			require.Equal(t, float64(0), owned.Allocatable, "owned gpu-0 must be unavailable, not free capacity")
			require.NotEmpty(t, owned.PodEntries, "owner view keeps the real accounting entries")
			for _, sibling := range allViews {
				if sibling == tc.resource {
					continue
				}
				placeholder, ok := machineState[sibling]["gpu-0"]
				require.True(t, ok, "gpu-0 must stay in sibling view %s", sibling)
				require.Equal(t, float64(0), placeholder.Allocatable, "sibling placeholder is not free capacity")
				require.Empty(t, placeholder.PodEntries, "sibling view holds no accounting entry")
			}
			require.NotContains(t, machineState[gpuconsts.GPUDeviceType], "gpu-1", "unowned residual must be removed")
			require.NotNil(t, p.GetState().GetAllocationInfo(tc.resource, "existing", "container"))

			// A new pod cannot be handed the disappeared, zero-capacity card.
			_, allocErr := p.Allocate(context.Background(), &pluginapi.ResourceRequest{
				PodUid:        "new",
				PodName:       "new",
				PodNamespace:  "default",
				ContainerName: "container",
				ContainerType: pluginapi.ContainerType_MAIN,
				ResourceName:  string(apiconsts.ResourceGPUMemory),
				ResourceRequests: map[string]float64{
					string(apiconsts.ResourceMilliGPU): 100,
				},
				Annotations: map[string]string{
					apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
				},
			})
			require.Error(t, allocErr)
			require.Nil(t, p.GetState().GetAllocationInfo(apiconsts.ResourceMilliGPU, "new", "container"))

			// Same-ID rediscovery through a non-empty real RPC restores the existing allocation
			// without duplicating it; only the original owner quantity is booked.
			_, err = p.UpdateAllocatableAssociatedDevices(context.Background(), &pluginapi.UpdateAllocatableAssociatedDevicesRequest{
				DeviceName: testResourcePluginName,
				Devices: []*pluginapi.AssociatedDevice{{
					ID:       "gpu-0",
					Health:   "Healthy",
					Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 0}}},
				}},
			})
			require.NoError(t, err)
			require.NoError(t, p.reconcileDeviceState())

			machineState = p.GetState().GetMachineState()
			allocation := p.GetState().GetAllocationInfo(tc.resource, "existing", "container")
			require.NotNil(t, allocation)
			require.Len(t, allocation.TopologyAwareAllocations, 1)
			require.Contains(t, allocation.TopologyAwareAllocations, "gpu-0")
			require.Equal(t, tc.quantity, allocation.AllocatedAllocation.Quantity)
			require.Equal(t, tc.quantity, machineState[tc.resource].GetQuantityAllocated("gpu-0"))
			require.Equal(t, []string{"gpu-0"}, machineState.GetAllocatedDeviceIDs(tc.resource, "existing", "container"))
			if tc.resource == gpuconsts.GPUDeviceType {
				require.Greater(t, machineState[tc.resource]["gpu-0"].Allocatable, float64(0))
			}

			// Empty discovery again after restore: the vanished ID is still held in the owner
			// view until its last release.
			_, err = p.UpdateAllocatableAssociatedDevices(context.Background(),
				&pluginapi.UpdateAllocatableAssociatedDevicesRequest{DeviceName: testResourcePluginName})
			require.NoError(t, err)
			require.NoError(t, p.reconcileDeviceState())
			require.NotNil(t, p.GetState().GetAllocationInfo(tc.resource, "existing", "container"))
			require.Contains(t, p.GetState().GetMachineState()[tc.resource], "gpu-0")

			// Last release of the vanished ID removes it from every retained view.
			_, err = p.RemovePod(context.Background(), &pluginapi.RemovePodRequest{PodUid: "existing"})
			require.NoError(t, err)
			require.NoError(t, p.reconcileDeviceState())
			for _, name := range allViews {
				require.NotContains(t, p.GetState().GetMachineState()[name], "gpu-0")
			}
		})
	}
}

func TestTopologyReconcilerRestartDoesNotLeaveOldWorker(t *testing.T) {
	p := makeTestStaticPolicy(t)
	// Keep stateInitializedCh open so the test does not start reporter sockets.
	require.NoError(t, p.Start())
	oldDone := p.topologyReconcileDone
	require.NoError(t, p.Start())
	require.Equal(t, oldDone, p.topologyReconcileDone)
	require.NoError(t, p.Stop())
	select {
	case <-oldDone:
	default:
		t.Fatal("old worker still running")
	}
	require.NoError(t, p.Start())
	require.NotEqual(t, oldDone, p.topologyReconcileDone)
	require.NoError(t, p.Stop())
	require.NoError(t, p.Stop())
}
