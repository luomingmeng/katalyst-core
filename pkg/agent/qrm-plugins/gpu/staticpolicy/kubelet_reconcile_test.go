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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	gpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	metaagent "github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	podfetch "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/checkpoint"
)

func TestPartialDiscoveryPreservesKubeletOwnership(t *testing.T) {
	p := makeTestStaticPolicy(t)
	p.SetState(nil)
	p.Conf.EnableKubeletCheckpointFallback = true
	p.Conf.MainContainerAnnotationKey = "main-container"
	p.MetaServer.MetaAgent = &metaagent.MetaAgent{PodFetcher: &podfetch.PodFetcherStub{PodList: []*v1.Pod{{ObjectMeta: metav1.ObjectMeta{UID: "existing", Name: "existing", Namespace: "default", Annotations: map[string]string{"main-container": "container", apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores}}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "container"}}}}}}}
	cm, err := checkpointmanager.NewCheckpointManager(p.Conf.KubeletDevicePluginPath)
	require.NoError(t, err)
	require.NoError(t, cm.CreateCheckpoint(native.KubeletDeviceManagerCheckpoint, checkpoint.New([]checkpoint.PodDevicesEntry{{PodUID: "existing", ContainerName: "container", ResourceName: testResourcePluginName, DeviceIDs: checkpoint.DevicesPerNUMA{0: {"gpu-0", "gpu-1"}}}}, nil)))
	update := func(ids ...string) {
		t.Helper()
		req := &pluginapi.UpdateAllocatableAssociatedDevicesRequest{DeviceName: testResourcePluginName}
		for _, id := range ids {
			req.Devices = append(req.Devices, &pluginapi.AssociatedDevice{ID: id, Health: "Healthy", Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 0}}}})
		}
		_, err := p.UpdateAllocatableAssociatedDevices(context.Background(), req)
		require.NoError(t, err)
	}
	update("gpu-0")
	require.NoError(t, p.reconcileDeviceState())
	ai := p.GetState().GetAllocationInfo(gpuconsts.GPUDeviceType, "existing", "container")
	require.NotNil(t, ai)
	require.Len(t, ai.TopologyAwareAllocations, 2)
	require.Zero(t, p.GetState().GetMachineState()[gpuconsts.GPUDeviceType]["gpu-1"].Allocatable)
	require.Equal(t, float64(1), p.GetState().GetMachineState()[gpuconsts.GPUDeviceType]["gpu-1"].GetQuantityAllocated())
	// Fill only missing topology metadata when discovery becomes complete.
	update("gpu-0", "gpu-1")
	require.NoError(t, p.reconcileDeviceState())
	ai = p.GetState().GetAllocationInfo(gpuconsts.GPUDeviceType, "existing", "container")
	require.Equal(t, []int{0}, ai.TopologyAwareAllocations["gpu-1"].NUMANodes)
	// Simulate the partially hydrated checkpoint left by an older version.
	// Missing IDs must be merged into an existing container without replacing metadata.
	originalMeta := ai.AllocationMeta
	delete(ai.TopologyAwareAllocations, "gpu-1")
	ai.AllocatedAllocation.Quantity = 1
	p.GetState().SetAllocationInfo(gpuconsts.GPUDeviceType, "existing", "container", ai, false)
	m, err := state.GenerateMachineStateFromPodEntries(p.GetState().GetPodResourceEntries(), p.DefaultResourceStateGeneratorRegistry)
	require.NoError(t, err)
	p.GetState().SetMachineState(m, false)
	// Importing GPU ownership must not reset an initialized idle RDMA map.
	require.NoError(t, p.ensureState(testRDMADeviceName))
	rdmaBefore := p.GetState().GetMachineState()[gpuconsts.RDMADeviceType]
	update("gpu-0", "gpu-1")
	require.NoError(t, p.reconcileDeviceState())
	require.Equal(t, rdmaBefore, p.GetState().GetMachineState()[gpuconsts.RDMADeviceType])
	ai = p.GetState().GetAllocationInfo(gpuconsts.GPUDeviceType, "existing", "container")
	require.Equal(t, originalMeta, ai.AllocationMeta)
	require.Len(t, ai.TopologyAwareAllocations, 2)
	require.Equal(t, float64(2), ai.AllocatedAllocation.Quantity)
	require.Equal(t, float64(1), p.GetState().GetMachineState()[gpuconsts.GPUDeviceType]["gpu-1"].GetQuantityAllocated())
	before := p.GetState().GetPodResourceEntries()
	require.NoError(t, p.reconcileDeviceState())
	require.Equal(t, before, p.GetState().GetPodResourceEntries())
	// Pause ownership publication while reconciliation holds the policy lock.
	// Concurrent public Allocate cannot consume newly discovered capacity.
	ai = p.GetState().GetAllocationInfo(gpuconsts.GPUDeviceType, "existing", "container")
	delete(ai.TopologyAwareAllocations, "gpu-1")
	ai.AllocatedAllocation.Quantity = 1
	p.GetState().SetAllocationInfo(gpuconsts.GPUDeviceType, "existing", "container", ai, false)
	gate := &ownershipPublishGate{State: p.GetState(), entered: make(chan struct{}), release: make(chan struct{})}
	p.SetState(gate)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(release)
	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- p.reconcileDeviceState() }()
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("ownership merge did not enter publication")
	}
	allocateDone := make(chan error, 1)
	go func() {
		_, err := p.Allocate(context.Background(), &pluginapi.ResourceRequest{PodUid: "concurrent", ContainerName: "container", ContainerType: pluginapi.ContainerType_MAIN, ResourceName: string(apiconsts.ResourceGPUMemory), ResourceRequests: map[string]float64{string(apiconsts.ResourceMilliGPU): 100}})
		allocateDone <- err
	}()
	release()
	select {
	case err := <-reconcileDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reconcile did not finish")
	}
	select {
	case err := <-allocateDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("allocation did not finish")
	}
	require.Nil(t, p.GetState().GetAllocationInfo(apiconsts.ResourceMilliGPU, "concurrent", "container"))
	p.Conf.GPUMemoryAllocatablePerGPU = resource.MustParse("100")
	p.Conf.MilliGPUAllocatablePerGPU = resource.MustParse("1000")
	_, allocErr := p.Allocate(context.Background(), &pluginapi.ResourceRequest{PodUid: "new", PodName: "new", PodNamespace: "default", ContainerName: "container", ContainerType: pluginapi.ContainerType_MAIN, ResourceName: string(apiconsts.ResourceGPUMemory), ResourceRequests: map[string]float64{string(apiconsts.ResourceMilliGPU): 100}, Annotations: map[string]string{apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores}})
	require.Error(t, allocErr, "all GPUs belong to the existing kubelet pod")
	require.Nil(t, p.GetState().GetAllocationInfo(apiconsts.ResourceMilliGPU, "new", "container"))
}

type ownershipPublishGate struct {
	state.State
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *ownershipPublishGate) SetPodResourceEntries(entries state.PodResourceEntries, persist bool) {
	s.once.Do(func() { close(s.entered); <-s.release })
	s.State.SetPodResourceEntries(entries, persist)
}

func TestReconcileCheckpointReadFailureDoesNotExposeLateCapacity(t *testing.T) {
	p := makeTestStaticPolicy(t)
	require.NoError(t, p.reconcileDeviceState())
	p.Conf.EnableKubeletCheckpointFallback = true
	before := p.GetState().GetMachineState()
	entries := p.GetState().GetPodResourceEntries()
	require.NoError(t, os.WriteFile(filepath.Join(p.Conf.KubeletDevicePluginPath, native.KubeletDeviceManagerCheckpoint), []byte("invalid checkpoint"), 0600))
	_, err := p.UpdateAllocatableAssociatedDevices(context.Background(), &pluginapi.UpdateAllocatableAssociatedDevicesRequest{DeviceName: testResourcePluginName, Devices: []*pluginapi.AssociatedDevice{{ID: "gpu-0", Health: "Healthy"}, {ID: "gpu-1", Health: "Healthy"}, {ID: "gpu-2", Health: "Healthy"}}})
	require.NoError(t, err)
	require.Error(t, p.reconcileDeviceState())
	require.Equal(t, before, p.GetState().GetMachineState())
	require.Equal(t, entries, p.GetState().GetPodResourceEntries())
	require.NotContains(t, p.GetState().GetMachineState()[gpuconsts.GPUDeviceType], "gpu-2")
	// A different rebuild can expose capacity; Allocate must still recheck
	// checkpoint ownership, and its failure must not erase existing allocation.
	p.GetState().SetAllocationInfo(apiconsts.ResourceMilliGPU, "existing", "container", &state.AllocationInfo{TopologyAwareAllocations: map[string]state.Allocation{"gpu-0": {Quantity: 100}}}, false)
	protected := p.GetState().GetPodResourceEntries()
	_, err = p.Allocate(context.Background(), &pluginapi.ResourceRequest{PodUid: "existing", ContainerName: "container", ContainerType: pluginapi.ContainerType_MAIN, ResourceName: string(apiconsts.ResourceMilliGPU)})
	require.Error(t, err)
	require.Equal(t, protected, p.GetState().GetPodResourceEntries())
	_, err = p.AllocateAssociatedDevice(context.Background(), &pluginapi.AssociatedDeviceRequest{DeviceName: testResourcePluginName, ResourceRequest: &pluginapi.ResourceRequest{PodUid: "existing", ContainerName: "container"}, DeviceRequest: []*pluginapi.DeviceRequest{{DeviceName: testResourcePluginName}}})
	require.Error(t, err)
	require.Equal(t, protected, p.GetState().GetPodResourceEntries())
	_, err = p.RemovePod(context.Background(), &pluginapi.RemovePodRequest{PodUid: "existing"})
	require.NoError(t, err, "checkpoint read failure must not prevent RemovePod")
	require.Nil(t, p.GetState().GetAllocationInfo(apiconsts.ResourceMilliGPU, "existing", "container"))
	require.NoError(t, os.Remove(filepath.Join(p.Conf.KubeletDevicePluginPath, native.KubeletDeviceManagerCheckpoint)))
	require.NoError(t, p.reconcileDeviceState())
	require.Contains(t, p.GetState().GetMachineState()[gpuconsts.GPUDeviceType], "gpu-2")
}
