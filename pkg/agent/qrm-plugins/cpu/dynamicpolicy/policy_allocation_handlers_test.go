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

package dynamicpolicy

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	dynamicpolicyutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	rputil "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

type atomicCommitTrackingState struct {
	state.State
	commitErr   error
	failCommits int
	commitCalls int
	storeCalls  int
}

func (s *atomicCommitTrackingState) CommitAdvisorState(
	podEntries state.PodEntries,
	machineState state.NUMANodeMap,
	allowOverlap, disableDedicatedOverlap, persist bool,
) error {
	s.commitCalls++
	if s.commitErr != nil && (s.failCommits < 0 || s.commitCalls <= s.failCommits) {
		return s.commitErr
	}
	return s.State.CommitAdvisorState(podEntries, machineState, allowOverlap, disableDedicatedOverlap, persist)
}

func (s *atomicCommitTrackingState) StoreState() error {
	s.storeCalls++
	return s.State.StoreState()
}

func TestDynamicPolicy_getReclaimOverlapShareRatio(t *testing.T) {
	t.Parallel()

	type fields struct {
		allowSharedCoresOverlapReclaimedCores bool
	}
	type args struct {
		entries state.PodEntries
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]float64
		wantErr bool
	}{
		{
			name: "overlap disabled",
			fields: fields{
				allowSharedCoresOverlapReclaimedCores: false,
			},
			args: args{
				entries: state.PodEntries{},
			},
			want: nil,
		},
		{
			name: "overlap enabled, no reclaim",
			fields: fields{
				allowSharedCoresOverlapReclaimedCores: true,
			},
			args: args{
				entries: state.PodEntries{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "overlap enabled, reclaim and share normal",
			fields: fields{
				allowSharedCoresOverlapReclaimedCores: true,
			},
			args: args{
				entries: state.PodEntries{
					commonstate.PoolNameReclaim: {
						commonstate.FakedContainerName: &state.AllocationInfo{
							AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
							AllocationResult: machine.NewCPUSet(0, 1, 2, 3),
							TopologyAwareAssignments: map[int]machine.CPUSet{
								0: machine.NewCPUSet(0),
								1: machine.NewCPUSet(1),
								2: machine.NewCPUSet(2),
								3: machine.NewCPUSet(3),
							},
						},
					},
					commonstate.PoolNameShare: {
						commonstate.FakedContainerName: &state.AllocationInfo{
							AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
							AllocationResult: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
							TopologyAwareAssignments: map[int]machine.CPUSet{
								0: machine.NewCPUSet(0, 4),
								1: machine.NewCPUSet(1, 5),
								2: machine.NewCPUSet(2, 6),
								3: machine.NewCPUSet(3, 7),
							},
						},
					},
				},
			},
			want: map[string]float64{
				commonstate.PoolNameShare: 0.5,
			},
			wantErr: false,
		},
		{
			name: "overlap enabled, reclaim and share ramp up",
			fields: fields{
				allowSharedCoresOverlapReclaimedCores: true,
			},
			args: args{
				entries: state.PodEntries{
					commonstate.PoolNameReclaim: {
						commonstate.FakedContainerName: &state.AllocationInfo{
							AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
							AllocationResult: machine.NewCPUSet(0, 1, 2, 3),
							TopologyAwareAssignments: map[int]machine.CPUSet{
								0: machine.NewCPUSet(0),
								1: machine.NewCPUSet(1),
								2: machine.NewCPUSet(2),
								3: machine.NewCPUSet(3),
							},
						},
					},
					"pod1": {
						"container1": &state.AllocationInfo{
							AllocationMeta: commonstate.GenerateGenericContainerAllocationMeta(&pluginapi.ResourceRequest{
								PodUid:        "pod1",
								PodNamespace:  "pod1",
								PodName:       "pod1",
								ContainerName: "container1",
							}, commonstate.EmptyOwnerPoolName, apiconsts.PodAnnotationQoSLevelSharedCores),
							RequestQuantity:  4,
							AllocationResult: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
							TopologyAwareAssignments: map[int]machine.CPUSet{
								0: machine.NewCPUSet(0, 4),
								1: machine.NewCPUSet(1, 5),
								2: machine.NewCPUSet(2, 6),
								3: machine.NewCPUSet(3, 7),
							},
						},
					},
				},
			},
			want: map[string]float64{
				commonstate.PoolNameShare: 0.5,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			as := require.New(t)
			cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
			as.Nil(err)

			tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_getReclaimOverlapShareRatio")
			as.Nil(err)

			p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
			as.Nil(err)

			if tt.fields.allowSharedCoresOverlapReclaimedCores {
				p.state.SetAllowSharedCoresOverlapReclaimedCores(true, true)
			}

			got, err := p.getReclaimOverlapShareRatio(tt.args.entries)
			if (err != nil) != tt.wantErr {
				t.Errorf("getReclaimOverlapShareRatio() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getReclaimOverlapShareRatio() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllocateSharedNumaBindingCPUs(t *testing.T) {
	t.Parallel()
	as := require.New(t)

	// Setup
	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	as.Nil(err)

	podName := "test-pod"
	containerName := "test-container"
	podUID := "test-uid"

	// Helper to create request
	createReq := func(reqQuantity float64, inplaceUpdate bool) *pluginapi.ResourceRequest {
		req := &pluginapi.ResourceRequest{
			PodUid:        podUID,
			PodNamespace:  "default",
			PodName:       podName,
			ContainerName: containerName,
			ResourceName:  string(v1.ResourceCPU),
			ResourceRequests: map[string]float64{
				string(v1.ResourceCPU): reqQuantity,
			},
			Annotations: map[string]string{
				apiconsts.PodAnnotationQoSLevelKey:                  apiconsts.PodAnnotationQoSLevelSharedCores,
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			},
			Hint: &pluginapi.TopologyHint{
				Nodes:     []uint64{0},
				Preferred: true,
			},
		}
		if inplaceUpdate {
			req.Annotations[apiconsts.PodAnnotationInplaceUpdateResizingKey] = "true"
		}
		return req
	}

	// Case 1: Inplace Update Error - Origin is not SNB
	t.Run("inplace_update_error_origin_not_snb", func(t *testing.T) {
		t.Parallel()

		tmpDir, err := ioutil.TempDir("", "checkpoint-TestAllocateSharedNumaBindingCPUs")
		as.Nil(err)
		defer os.RemoveAll(tmpDir)

		policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
		as.Nil(err)
		// Setup origin allocation info (Normal SharedCores, NOT SNB)
		originAllocationInfo := &state.AllocationInfo{
			AllocationMeta: commonstate.AllocationMeta{
				PodUid:        podUID,
				PodNamespace:  "default",
				PodName:       podName,
				ContainerName: containerName,
				QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			},
			RequestQuantity: 2,
		}
		policy.state.SetAllocationInfo(podUID, containerName, originAllocationInfo, false)

		req := createReq(4, true)
		_, err = policy.allocateSharedNumaBindingCPUs(context.Background(), req, req.Hint, false)
		as.Error(err)
		as.Contains(err.Error(), "cannot change from non-snb to snb during inplace update")
	})

	// Case 2: Inplace Update Success - Origin is SNB
	t.Run("inplace_update_success_origin_snb", func(t *testing.T) {
		t.Parallel()
		tmpDir, err := ioutil.TempDir("", "checkpoint-TestAllocateSharedNumaBindingCPUs")
		as.Nil(err)
		defer os.RemoveAll(tmpDir)

		policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
		as.Nil(err)

		// Setup origin allocation info (SNB)
		originAllocationInfo := &state.AllocationInfo{
			AllocationMeta: commonstate.AllocationMeta{
				PodUid:        podUID,
				PodNamespace:  "default",
				PodName:       podName,
				ContainerName: containerName,
				QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				Annotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				},
			},
			RequestQuantity:  2,
			AllocationResult: machine.NewCPUSet(0, 1),
			TopologyAwareAssignments: map[int]machine.CPUSet{
				0: machine.NewCPUSet(0, 1),
			},
		}
		originAllocationInfo.SetSpecifiedNUMABindingNUMAID([]uint64{0})

		policy.state.SetAllocationInfo(podUID, containerName, originAllocationInfo, false)

		req := createReq(4, true)
		_, err = policy.allocateSharedNumaBindingCPUs(context.Background(), req, req.Hint, false)
		if err != nil {
			as.NotContains(err.Error(), "cannot change from non-snb to snb during inplace update")
		}
	})

	// Case 3: Normal Allocation (Not Inplace Update)
	t.Run("normal_allocation", func(t *testing.T) {
		t.Parallel()
		tmpDir, err := ioutil.TempDir("", "checkpoint-TestAllocateSharedNumaBindingCPUs")
		as.Nil(err)
		defer os.RemoveAll(tmpDir)

		policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
		as.Nil(err)

		req := createReq(2, false)
		// Clean up previous state
		policy.state.Delete(podUID, containerName, false)

		_, err = policy.allocateSharedNumaBindingCPUs(context.Background(), req, req.Hint, false)
		// This might fail due to pool issues but it covers the else branch
		// We expect it NOT to fail with the inplace update error
		if err != nil {
			as.NotContains(err.Error(), "inplace update")
		}
	})

	// Case 4: Invalid Inputs
	t.Run("invalid_inputs", func(t *testing.T) {
		t.Parallel()
		tmpDir, err := ioutil.TempDir("", "checkpoint-TestAllocateSharedNumaBindingCPUs")
		as.Nil(err)
		defer os.RemoveAll(tmpDir)

		policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
		as.Nil(err)

		req := createReq(2, false)

		// Nil req
		_, err = policy.allocateSharedNumaBindingCPUs(context.Background(), nil, req.Hint, false)
		as.Error(err)
		as.Contains(err.Error(), "nil req")

		// Nil hint
		_, err = policy.allocateSharedNumaBindingCPUs(context.Background(), req, nil, false)
		as.Error(err)
		as.Contains(err.Error(), "hint is nil")

		// Empty hint
		emptyHintReq := createReq(2, false)
		emptyHintReq.Hint = &pluginapi.TopologyHint{Nodes: []uint64{}}
		_, err = policy.allocateSharedNumaBindingCPUs(context.Background(), req, emptyHintReq.Hint, false)
		as.Error(err)
		as.Contains(err.Error(), "hint is empty")

		// Hint with multiple nodes
		multiNodeHintReq := createReq(2, false)
		multiNodeHintReq.Hint = &pluginapi.TopologyHint{Nodes: []uint64{0, 1}}
		_, err = policy.allocateSharedNumaBindingCPUs(context.Background(), req, multiNodeHintReq.Hint, false)
		as.Error(err)
		as.Contains(err.Error(), "larger than 1 NUMA")
	})
}

func TestAllocateSharedNumaBindingCPUsMarksColdStartRampUp(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)

	policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	policy.dynamicConfig.GetDynamicConfiguration().DisableSharedCoresRampUp = false
	policy.metaServer.MetaAgent.PodFetcher = &pod.PodFetcherStub{PodList: []*v1.Pod{{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID("snb-ramp-up"),
			Namespace: "default",
			Name:      "snb-ramp-up",
		},
		Status: v1.PodStatus{Phase: v1.PodPending},
	}}}

	req := &pluginapi.ResourceRequest{
		PodUid:        "snb-ramp-up",
		PodNamespace:  "default",
		PodName:       "snb-ramp-up",
		ContainerName: "main",
		ResourceName:  string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                  apiconsts.PodAnnotationQoSLevelSharedCores,
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		},
		Hint: &pluginapi.TopologyHint{
			Nodes:     []uint64{0},
			Preferred: true,
		},
	}

	allocation, err := policy.allocateSharedNumaBindingCPUs(context.Background(), req, req.Hint, false)
	require.NoError(t, err)
	require.NotNil(t, allocation)
	require.True(t, allocation.CheckSharedNUMABinding())
	require.True(t, allocation.RampUp)
}

func TestSharedNUMABindingRampUpStaysWithinHintedNUMA(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)

	policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	policy.reservedCPUs = machine.NewCPUSet()
	policy.dynamicConfig.GetDynamicConfiguration().DisableSharedCoresRampUp = false
	policy.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	policy.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25
	policy.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)

	allocation := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "snb-ramp-up-numa-scoped",
			PodNamespace:  "default",
			PodName:       "snb-ramp-up-numa-scoped",
			ContainerName: "main",
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			},
		},
		RampUp:          true,
		RequestQuantity: 2,
	}
	allocation.SetSpecifiedNUMABindingNUMAID([]uint64{0})
	policy.state.SetAllocationInfo(allocation.PodUid, allocation.ContainerName, allocation, false)

	err = policy.adjustAllocationEntriesWithRampUpFloor(
		policy.state.GetPodEntries(),
		policy.state.GetMachineState(),
		false,
		machine.NewCPUSet(1),
		false,
	)
	require.NoError(t, err)

	updated := policy.state.GetAllocationInfo(allocation.PodUid, allocation.ContainerName)
	require.NotNil(t, updated)
	require.True(t, updated.RampUp)
	require.True(t, updated.CheckSharedNUMABinding())

	hintedNUMACPUSet := policy.machineInfo.CPUDetails.CPUsInNUMANodes(0)
	require.False(t, updated.AllocationResult.IsEmpty())
	require.True(t, updated.AllocationResult.IsSubsetOf(hintedNUMACPUSet),
		"SNB ramp-up allocation=%s must stay within hinted NUMA0=%s", updated.AllocationResult, hintedNUMACPUSet)
	for numaID := range updated.TopologyAwareAssignments {
		require.Equal(t, 0, numaID, "SNB ramp-up assignment escaped hinted NUMA")
	}
}

func TestNonSNBRampUpRemainsNodeWide(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)

	policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	policy.reservedCPUs = machine.NewCPUSet()
	policy.dynamicConfig.GetDynamicConfiguration().DisableSharedCoresRampUp = false
	policy.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	policy.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25
	policy.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)

	allocation := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "non-snb-ramp-up",
			PodNamespace:  "default",
			PodName:       "non-snb-ramp-up",
			ContainerName: "main",
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		RampUp:          true,
		RequestQuantity: 1,
	}
	policy.state.SetAllocationInfo(allocation.PodUid, allocation.ContainerName, allocation, false)

	err = policy.adjustAllocationEntriesWithRampUpFloor(
		policy.state.GetPodEntries(),
		policy.state.GetMachineState(),
		false,
		machine.NewCPUSet(1),
		false,
	)
	require.NoError(t, err)

	updated := policy.state.GetAllocationInfo(allocation.PodUid, allocation.ContainerName)
	require.NotNil(t, updated)
	require.True(t, updated.RampUp)
	require.False(t, updated.CheckSharedNUMABinding())
	require.Greater(t, len(updated.TopologyAwareAssignments), 1,
		"non-SNB ramp-up should remain node-wide, got %+v", updated.TopologyAwareAssignments)
}

func TestDynamicPolicy_allocateNumaBindingCPUs(t *testing.T) {
	t.Parallel()

	type args struct {
		numCPUs        int
		hint           *pluginapi.TopologyHint
		machineState   state.NUMANodeMap
		reqAnnotations map[string]string
		// reclaimCPUs, when non-empty, is written into the reclaim pool before the
		// call so that dedicated allocation can prefer reclaim-free cpus.
		reclaimCPUs machine.CPUSet
	}
	tests := []struct {
		name    string
		args    args
		want    machine.CPUSet
		wantErr bool
	}{
		{
			name: "normal allocation without pinning",
			args: args{
				numCPUs: 2,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				},
			},
			want:    machine.NewCPUSet(0, 1),
			wantErr: false,
		},
		{
			name: "allocation with pinned resource package",
			args: args{
				numCPUs: 2,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
						ResourcePackageStates: map[string]*state.ResourcePackageState{
							"pkg1": {
								PinnedCPUSet: machine.NewCPUSet(2, 3),
							},
						},
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationResourcePackageKey:           "pkg1",
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				},
			},
			want:    machine.NewCPUSet(2, 3),
			wantErr: false,
		},
		{
			name: "allocation without pinned resource package but with other pinned packages",
			args: args{
				numCPUs: 2,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
						ResourcePackageStates: map[string]*state.ResourcePackageState{
							"pkg1": {
								PinnedCPUSet: machine.NewCPUSet(2, 3),
							},
						},
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				},
			},
			want:    machine.NewCPUSet(0, 1),
			wantErr: false,
		},
		{
			name: "distribute evenly with pinned resource package",
			args: args{
				numCPUs: 2,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0, 1},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
						ResourcePackageStates: map[string]*state.ResourcePackageState{
							"pkg1": {
								PinnedCPUSet: machine.NewCPUSet(2, 3),
							},
						},
					},
					1: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(4, 5, 6, 7),
						ResourcePackageStates: map[string]*state.ResourcePackageState{
							"pkg1": {
								PinnedCPUSet: machine.NewCPUSet(6, 7),
							},
						},
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationResourcePackageKey:                       "pkg1",
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding:             apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					apiconsts.PodAnnotationCPUEnhancementNumaNumber:                 "2",
					apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNuma: apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNumaEnable,
				},
			},
			want:    machine.NewCPUSet(2, 6),
			wantErr: false,
		},
		{
			name: "distribute evenly without pinned resource package but with other pinned packages",
			args: args{
				numCPUs: 2,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0, 1},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
						ResourcePackageStates: map[string]*state.ResourcePackageState{
							"pkg1": {
								PinnedCPUSet: machine.NewCPUSet(2, 3),
							},
						},
					},
					1: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(4, 5, 6, 7),
						ResourcePackageStates: map[string]*state.ResourcePackageState{
							"pkg1": {
								PinnedCPUSet: machine.NewCPUSet(6, 7),
							},
						},
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding:             apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					apiconsts.PodAnnotationCPUEnhancementNumaNumber:                 "2",
					apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNuma: apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNumaEnable,
				},
			},
			want:    machine.NewCPUSet(0, 4),
			wantErr: false,
		},
		{
			name: "distribute evenly with pinned resource package on some NUMAs but not others",
			args: args{
				numCPUs: 2,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0, 1},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
						ResourcePackageStates: map[string]*state.ResourcePackageState{
							"pkg1": {
								PinnedCPUSet: machine.NewCPUSet(2, 3),
							},
						},
					},
					1: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(4, 5, 6, 7),
						// pkg1 is not pinned on NUMA 1
						ResourcePackageStates: map[string]*state.ResourcePackageState{},
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationResourcePackageKey:                       "pkg1",
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding:             apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					apiconsts.PodAnnotationCPUEnhancementNumaNumber:                 "2",
					apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNuma: apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNumaEnable,
				},
			},
			want:    machine.NewCPUSet(2, 4),
			wantErr: false,
		},
		{
			// Case 1: reclaim-free set can fully satisfy the request, so dedicated
			// allocation must avoid the reclaim cpus entirely.
			name: "prefer reclaim-free cpus when sufficient",
			args: args{
				numCPUs: 2,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				},
				reclaimCPUs: machine.NewCPUSet(0, 1),
			},
			want:    machine.NewCPUSet(2, 3),
			wantErr: false,
		},
		{
			// Case 2: reclaim-free set is insufficient, so allocate reclaim-free
			// CPUs first and then borrow the minimum remaining CPUs from reclaim.
			name: "prefer reclaim-free first when insufficient",
			args: args{
				numCPUs: 3,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				},
				// only 0,1 reclaim-free => size 2 < 3, must borrow one reclaim CPU.
				reclaimCPUs: machine.NewCPUSet(2, 3),
			},
			want:    machine.NewCPUSet(0, 1, 2),
			wantErr: false,
		},
		{
			name: "prefer reclaim-free first instead of full-set topology order",
			args: args{
				numCPUs: 3,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				},
				reclaimCPUs: machine.NewCPUSet(0, 1),
			},
			want:    machine.NewCPUSet(0, 2, 3),
			wantErr: false,
		},
		{
			// Case 5: distribute-evenly across NUMA must also avoid reclaim cpus
			// on every NUMA when the reclaim-free set is sufficient per NUMA.
			name: "distribute evenly avoids reclaim cpus",
			args: args{
				numCPUs: 2,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0, 1},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
					},
					1: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(4, 5, 6, 7),
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding:             apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					apiconsts.PodAnnotationCPUEnhancementNumaNumber:                 "2",
					apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNuma: apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNumaEnable,
				},
				// reclaim occupies the lowest cpu of each NUMA => allocation must skip them.
				reclaimCPUs: machine.NewCPUSet(0, 4),
			},
			want:    machine.NewCPUSet(1, 5),
			wantErr: false,
		},
		{
			// Case 5b (regression): distribute-evenly where the GLOBAL reclaim-free set
			// is sufficient (>= numCPUs) but one NUMA cannot meet its per-NUMA share from
			// reclaim-free cpus alone. Allocation must succeed by borrowing the remainder
			// on that NUMA instead of failing on a global reclaim-free shortcut.
			name: "distribute evenly borrows reclaim when a numa is short",
			args: args{
				numCPUs: 4,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0, 1},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
					},
					1: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(4, 5, 6, 7),
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding:             apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					apiconsts.PodAnnotationCPUEnhancementNumaNumber:                 "2",
					apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNuma: apiconsts.PodAnnotationCPUEnhancementDistributeEvenlyAcrossNumaEnable,
				},
				// NUMA0 fully reclaim-free (4 cpus); NUMA1 reclaim-free only {4}.
				// Global reclaim-free {0,1,2,3,4} size 5 >= 4, but NUMA1 needs 2 and has
				// only 1 reclaim-free cpu, so it must borrow one cpu from {5,6,7}.
				reclaimCPUs: machine.NewCPUSet(5, 6, 7),
			},
			want:    machine.NewCPUSet(0, 1, 4, 5),
			wantErr: false,
		},
		{
			// Case 4c: numaExclusive keeps whole-NUMA exclusivity and must NOT
			// subtract reclaim cpus, otherwise the exclusive dedicated_cores would
			// no longer own the full NUMA.
			name: "numa exclusive keeps whole numa despite reclaim overlap",
			args: args{
				numCPUs: 4,
				hint: &pluginapi.TopologyHint{
					Nodes: []uint64{0},
				},
				machineState: state.NUMANodeMap{
					0: &state.NUMANodeState{
						DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
					},
				},
				reqAnnotations: map[string]string{
					apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
				},
				reclaimCPUs: machine.NewCPUSet(0, 1),
			},
			want:    machine.NewCPUSet(0, 1, 2, 3),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			as := require.New(t)
			cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
			as.Nil(err)
			tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateNumaBindingCPUs")
			as.Nil(err)

			p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
			as.Nil(err)
			p.reservedCPUs = machine.NewCPUSet()
			t.Logf("Reserved: %s", p.reservedCPUs.String())

			// Explicitly control the reclaim pool so allocation's reclaim-avoidance
			// only sees the cpus declared by this case (the default init would
			// otherwise seed an unrelated reclaim pool).
			p.state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: tt.args.reclaimCPUs.Clone(),
			}, false)

			got, _, err := p.allocateNumaBindingCPUs(tt.args.numCPUs, tt.args.hint, tt.args.machineState, tt.args.reqAnnotations, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("allocateNumaBindingCPUs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !got.Equals(tt.want) {
				t.Errorf("allocateNumaBindingCPUs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDynamicPolicy_takeByTopologyPreferring_invariants(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_takeByTopologyPreferring_invariants")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)

	tests := []struct {
		name      string
		available machine.CPUSet
		preferred machine.CPUSet
		numCPUs   int
		want      machine.CPUSet
	}{
		{
			name:      "preferred is clipped to available",
			available: machine.NewCPUSet(1, 2),
			preferred: machine.NewCPUSet(0, 1),
			numCPUs:   1,
			want:      machine.NewCPUSet(1),
		},
		{
			name:      "empty preferred falls back to available",
			available: machine.NewCPUSet(2, 3),
			preferred: machine.NewCPUSet(),
			numCPUs:   1,
			want:      machine.NewCPUSet(2),
		},
		{
			name:      "full reclaim falls back to all available",
			available: machine.NewCPUSet(4, 5),
			preferred: machine.NewCPUSet(),
			numCPUs:   2,
			want:      machine.NewCPUSet(4, 5),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.takeByTopologyPreferring(tt.available, tt.preferred, tt.numCPUs)
			require.NoError(t, err)
			require.True(t, got.IsSubsetOf(tt.available), "got=%s available=%s", got.String(), tt.available.String())
			require.Equal(t, tt.numCPUs, got.Size())
			require.True(t, got.Equals(tt.want), "got=%s want=%s", got.String(), tt.want.String())
		})
	}
}

func TestDynamicPolicy_allocateNumaBindingCPUs_reclaimPreferenceRespectsResourcePackageOrder(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateNumaBindingCPUs_resource_package_order")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &state.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(0),
	}, false)

	machineState := state.NUMANodeMap{
		0: &state.NUMANodeState{
			DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3),
			ResourcePackageStates: map[string]*state.ResourcePackageState{
				"pkg1": {PinnedCPUSet: machine.NewCPUSet(0, 1, 2)},
			},
		},
	}
	got, _, err := p.allocateNumaBindingCPUs(2, &pluginapi.TopologyHint{Nodes: []uint64{0}}, machineState, map[string]string{
		apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		apiconsts.PodAnnotationResourcePackageKey:           "pkg1",
	}, false)
	require.NoError(t, err)
	require.True(t, got.Equals(machine.NewCPUSet(1, 2)), "got=%s", got.String())
	require.True(t, got.IsSubsetOf(machine.NewCPUSet(0, 1, 2)), "got=%s", got.String())
	require.Equal(t, 2, got.Size())
}

func TestDynamicPolicy_allocateNumaBindingCPUs_fullReclaimFallsBackToAvailable(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateNumaBindingCPUs_full_reclaim")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &state.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(0, 1, 2, 3),
	}, false)

	available := machine.NewCPUSet(0, 1, 2, 3)
	machineState := state.NUMANodeMap{
		0: &state.NUMANodeState{DefaultCPUSet: available},
	}
	got, _, err := p.allocateNumaBindingCPUs(2, &pluginapi.TopologyHint{Nodes: []uint64{0}}, machineState, map[string]string{
		apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
	}, false)
	require.NoError(t, err)
	require.Equal(t, 2, got.Size())
	require.True(t, got.IsSubsetOf(available), "got=%s available=%s", got.String(), available.String())
}

// TestDynamicPolicy_generateNUMABindingPoolsCPUSetInPlace verifies the logic of generating CPU sets for NUMA-binding pools.
// It simulates a scenario with specific CPU topology and available CPUs, checking if the allocation strategies (like packing full cores) work as expected.
// Topology Assumption for mustGenerateDummyCPUTopology(16, 2, 2):
// - 16 CPUs total, 2 NUMA Nodes (0 and 1).
// - HT enabled, siblings are separated by 16/2 = 8.
// - NUMA 0: CPUs {0, 1, 2, 3} (Logic Cores) and {8, 9, 10, 11} (Siblings).
//   - Core 0: {0, 8}, Core 1: {1, 9}, Core 2: {2, 10}, Core 3: {3, 11}.
//
// - NUMA 1: CPUs {4, 5, 6, 7} (Logic Cores) and {12, 13, 14, 15} (Siblings).
func TestDynamicPolicy_generateNUMABindingPoolsCPUSetInPlace(t *testing.T) {
	t.Parallel()

	type args struct {
		poolsCPUSet      map[string]machine.CPUSet
		poolsQuantityMap map[string]map[int]int
		availableCPUs    machine.CPUSet
	}
	tests := []struct {
		name          string
		cpuTopology   *machine.CPUTopology
		args          args
		wantPools     map[string]machine.CPUSet
		wantLeft      machine.CPUSet
		wantErr       bool
		enableReclaim bool
	}{
		// Case 1: Single pool allocation in NUMA 0.
		// Available CPUs: {8, 9, 10} (All in NUMA 0).
		// - Core 0: {0, 8} (Only 8 available).
		// - Core 1: {1, 9} (Only 9 available).
		// - Core 2: {2, 10} (Only 10 available).
		// Request: pool1 needs 2 CPUs from NUMA 0.
		// Allocation: No full cores available, so it picks {8, 9}.
		{
			name:        "single pool, ample cpus",
			cpuTopology: mustGenerateDummyCPUTopology(16, 2, 2),
			args: args{
				poolsCPUSet: make(map[string]machine.CPUSet),
				poolsQuantityMap: map[string]map[int]int{
					"pool1": {
						0: 2,
					},
				},
				availableCPUs: machine.NewCPUSet(8, 9, 10),
			},
			wantPools: map[string]machine.CPUSet{
				"pool1": machine.NewCPUSet(8, 9),
			},
			wantLeft:      machine.NewCPUSet(10),
			wantErr:       false,
			enableReclaim: true,
		},
		// Case 2: Multiple pools allocation across NUMA 0 and NUMA 1.
		// Available CPUs: {2, 3, 4, 5, 10}.
		// NUMA 0 Available: {2, 3, 10}.
		// - Core 2: {2, 10} (Both available -> Full Core).
		// - Core 3: {3, 11} (Only 3 available).
		// NUMA 1 Available: {4, 5}.
		// - Core 4: {4, 12} (Only 4 available).
		// - Core 5: {5, 13} (Only 5 available).
		// Request: pool1 needs 2 from NUMA 0; pool2 needs 2 from NUMA 1.
		// Allocation:
		// - pool1 (NUMA 0): Prefers full core {2, 10}.
		// - pool2 (NUMA 1): Takes {4, 5}.
		{
			name:        "multiple pools, ample cpus",
			cpuTopology: mustGenerateDummyCPUTopology(16, 2, 2),
			args: args{
				poolsCPUSet: make(map[string]machine.CPUSet),
				poolsQuantityMap: map[string]map[int]int{
					"pool1": {
						0: 2,
					},
					"pool2": {
						1: 2,
					},
				},
				availableCPUs: machine.NewCPUSet(2, 3, 4, 5, 10),
			},
			wantPools: map[string]machine.CPUSet{
				"pool1": machine.NewCPUSet(2, 10),
				"pool2": machine.NewCPUSet(4, 5),
			},
			wantLeft:      machine.NewCPUSet(3),
			wantErr:       false,
			enableReclaim: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			as := require.New(t)

			tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_generateNUMABindingPoolsCPUSetInPlace")
			as.Nil(err)
			defer os.RemoveAll(tmpDir) // Added cleanup

			p, err := getTestDynamicPolicyWithInitialization(tt.cpuTopology, tmpDir)
			as.Nil(err)

			// Clear state to ensure clean slate
			p.state.SetPodEntries(state.PodEntries{}, false)
			p.reservedCPUs = machine.NewCPUSet()

			p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = tt.enableReclaim

			gotLeft, err := p.generateNUMABindingPoolsCPUSetInPlace(tt.args.poolsCPUSet, tt.args.poolsQuantityMap, tt.args.availableCPUs)
			if (err != nil) != tt.wantErr {
				t.Errorf("generateNUMABindingPoolsCPUSetInPlace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !reflect.DeepEqual(tt.args.poolsCPUSet, tt.wantPools) {
					t.Errorf("generateNUMABindingPoolsCPUSetInPlace() poolsCPUSet = %v, want %v", tt.args.poolsCPUSet, tt.wantPools)
				}
				if !gotLeft.Equals(tt.wantLeft) {
					t.Errorf("generateNUMABindingPoolsCPUSetInPlace() gotLeft = %v, want %v", gotLeft, tt.wantLeft)
				}
			}
		})
	}
}

func TestDynamicPolicy_generatePoolsAndIsolation_reclaimLeftoverOnlyWhenReclaimDisabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		enableReclaim bool
		wantReclaim   machine.CPUSet
		wantShare     machine.CPUSet
	}{
		{
			name:          "enable reclaim respects existing reclaim pool and leaves leftover out",
			enableReclaim: true,
			wantReclaim:   machine.NewCPUSet(0, 4),
			wantShare:     machine.NewCPUSet(1, 5),
		},
		{
			name:          "disable reclaim keeps legacy leftover apportion path",
			enableReclaim: false,
			wantReclaim:   machine.NewCPUSet(2, 3, 4, 5),
			wantShare:     machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
			require.NoError(t, err)

			tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_generatePoolsAndIsolation_reclaim_leftover")
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)

			p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
			require.NoError(t, err)

			p.reservedCPUs = machine.NewCPUSet()
			p.reservedReclaimedCPUsSize = 0
			p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = tt.enableReclaim
			p.state.SetAllowSharedCoresOverlapReclaimedCores(false, true)
			p.state.SetPodEntries(state.PodEntries{}, false)

			poolsCPUSet, _, err := p.generatePoolsAndIsolation(
				map[string]map[int]int{
					commonstate.PoolNameShare:   {commonstate.FakedNUMAID: 2},
					commonstate.PoolNameReclaim: {commonstate.FakedNUMAID: 2},
				},
				map[string]map[string]int{},
				machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
				map[string]float64{},
			)
			require.NoError(t, err)

			require.True(t, poolsCPUSet[commonstate.PoolNameReclaim].Equals(tt.wantReclaim),
				"reclaim=%s want=%s", poolsCPUSet[commonstate.PoolNameReclaim].String(), tt.wantReclaim.String())
			require.True(t, poolsCPUSet[commonstate.PoolNameShare].Equals(tt.wantShare),
				"share=%s want=%s", poolsCPUSet[commonstate.PoolNameShare].String(), tt.wantShare.String())
		})
	}
}

func TestDynamicPolicy_generatePoolsAndIsolation_prefersHistoricalReclaimPool(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_generatePoolsAndIsolation_prefers_reclaim")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)

	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, true)
	p.state.SetPodEntries(state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(10, 11, 12, 13),
			},
		},
	}, false)

	poolsCPUSet, _, err := p.generatePoolsAndIsolation(
		map[string]map[int]int{
			commonstate.PoolNameReclaim: {commonstate.FakedNUMAID: 4},
			commonstate.PoolNameShare:   {commonstate.FakedNUMAID: 4},
		},
		map[string]map[string]int{},
		machine.NewCPUSet(0, 1, 2, 3, 8, 9, 10, 11, 12, 13),
		map[string]float64{},
	)
	require.NoError(t, err)

	historicalReclaim := machine.NewCPUSet(10, 11, 12, 13)
	require.True(t, poolsCPUSet[commonstate.PoolNameReclaim].Intersection(historicalReclaim).Equals(historicalReclaim),
		"reclaim pool should include its historical cpuset when still available, got %s",
		poolsCPUSet[commonstate.PoolNameReclaim].String())
	require.True(t, poolsCPUSet[commonstate.PoolNameShare].Intersection(historicalReclaim).IsEmpty(),
		"share pool should not take historical reclaim cpuset, got %s",
		poolsCPUSet[commonstate.PoolNameShare].String())
}

func TestDynamicPolicy_generatePoolsAndIsolation_preservesAdvisorReclaimForSeedPool(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(96, 2, 24)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_generatePoolsAndIsolation_preserves_advisor_reclaim")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)

	p.reservedCPUs = machine.NewCPUSet(0, 24)
	p.reservedReclaimedCPUSet = machine.NewCPUSet(1, 2, 25, 26)
	p.reservedReclaimedCPUsSize = 4
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, true)
	p.state.SetPodEntries(state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 25, 26, 27, 28, 29, 30, 31, 49, 50, 51, 52, 53, 54, 55, 73, 74, 75, 76, 77, 78, 79),
			},
		},
	}, false)

	poolsCPUSet, _, err := p.generatePoolsAndIsolation(
		map[string]map[int]int{
			"seedpool-stable-0": {commonstate.FakedNUMAID: 1},
		},
		map[string]map[string]int{},
		machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
			25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
			48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
			71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93,
			94, 95),
		map[string]float64{},
	)
	require.NoError(t, err)

	wantReclaim := machine.NewCPUSet(2, 3, 4, 5, 6, 7, 25, 26, 27, 28, 29, 30, 31, 49, 50, 51, 52, 53, 54, 55, 73, 74, 75, 76, 77, 78, 79)
	require.True(t, poolsCPUSet[commonstate.PoolNameReclaim].Equals(wantReclaim),
		"reclaim should preserve existing advisor reclaim minus seed allocation, got %s want %s",
		poolsCPUSet[commonstate.PoolNameReclaim].String(), wantReclaim.String())
	require.True(t, poolsCPUSet["seedpool-stable-0"].Equals(machine.NewCPUSet(1)),
		"seed pool should take the first available cpu, got %s", poolsCPUSet["seedpool-stable-0"].String())
}

func TestDynamicPolicyDeriveRampUpReclaimFloorCoversAllNUMAs(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(96, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicyDeriveRampUpReclaimFloorCoversAllNUMAs")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet(0, 24)
	p.reservedReclaimedCPUSet = machine.NewCPUSet(14, 38, 62, 86)
	p.reservedReclaimedCPUsSize = 4
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0
	p.state.SetPodEntries(state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(1, 25, 49, 73),
			},
		},
	}, false)

	floor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), true)
	require.NoError(t, err)
	require.True(t, floor.Equals(machine.NewCPUSet(14, 38, 62, 86)),
		"floor=%s, want all-NUMA reserved reclaim CPUs", floor)
	require.Equal(t, 2, floor.Intersection(p.machineInfo.CPUDetails.CPUsInNUMANodes(0)).Size())
	require.Equal(t, 2, floor.Intersection(p.machineInfo.CPUDetails.CPUsInNUMANodes(1)).Size())

	inactiveFloor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), false)
	require.NoError(t, err)
	require.True(t, inactiveFloor.IsEmpty(), "floor must not reserve capacity without an active ramp-up workload")

	p.state.SetAllocationInfo("ramp-up-pod", "main", &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid: "ramp-up-pod", ContainerName: "main",
		},
		AllocationResult: machine.NewCPUSet(1),
		RampUp:           true,
	}, false)
	activeFloor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), false)
	require.NoError(t, err)
	require.True(t, activeFloor.Equals(machine.NewCPUSet(14, 38, 62, 86)))
}

func TestDynamicPolicyDeriveRampUpReclaimFloorAllowsFullNonExclusiveRatio(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 1

	floor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), true)
	require.NoError(t, err)
	require.True(t, floor.Equals(machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7)),
		"floor=%s, want every eligible CPU", floor)
}

func TestDedicatedNUMAExclusiveRampUpCommitsAllocationAndReclaimAtomically(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25
	tracked := &atomicCommitTrackingState{State: p.state}
	p.state = tracked

	req := &pluginapi.ResourceRequest{
		PodUid:         "exclusive-dnb-atomic-ramp-up",
		PodNamespace:   "default",
		PodName:        "exclusive-dnb-atomic-ramp-up",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                    apiconsts.PodAnnotationQoSLevelDedicatedCores,
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
	}

	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(context.Background(), req, true)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 1, tracked.commitCalls)
	require.Zero(t, tracked.storeCalls)

	allocation := p.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	require.NotNil(t, allocation)
	require.True(t, allocation.CheckDedicatedNUMABindingNUMAExclusive())
	reclaim := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.NotNil(t, reclaim)
	require.False(t, reclaim.AllocationResult.IsEmpty())
	available := cpuTopology.CPUDetails.CPUs()
	require.True(t, allocation.AllocationResult.Intersection(reclaim.AllocationResult).IsEmpty(),
		"allocation=%s reclaim=%s", allocation.AllocationResult, reclaim.AllocationResult)
	require.True(t, allocation.AllocationResult.Union(reclaim.AllocationResult).Equals(available),
		"allocation=%s reclaim=%s available=%s", allocation.AllocationResult, reclaim.AllocationResult, available)
}

func TestAllocateDedicatedNUMAExclusiveAdjustmentFailureDoesNotRollbackNewerState(t *testing.T) {
	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25

	const failedPodUID = "exclusive-dnb-adjustment-failure"
	adjustmentStarted := make(chan struct{})
	releaseAdjustment := make(chan struct{})
	adjustmentCalls := 0
	failedCandidateCPUs := machine.NewCPUSet()
	p.cpuSetAdjustmentHandlers = map[string]dynamicpolicyutil.CPUSetAdjustmentHandler{
		"failing": func(_ context.Context, in dynamicpolicyutil.CPUSetAdjustmentHandlerCtx) error {
			adjustmentCalls++
			if adjustmentCalls == 1 {
				failedCandidate := in.State.GetAllocationInfo(failedPodUID, "main")
				if failedCandidate != nil {
					failedCandidateCPUs = failedCandidate.AllocationResult.Clone()
				}
				close(adjustmentStarted)
				<-releaseAdjustment
			}
			return errors.New("injected adjustment failure")
		},
	}
	req := &pluginapi.ResourceRequest{
		PodUid: failedPodUID, PodNamespace: "default",
		PodName: "exclusive-dnb-adjustment-failure", ContainerName: "main",
		ContainerType: pluginapi.ContainerType_MAIN, ResourceName: string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{string(v1.ResourceCPU): 2},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:          apiconsts.PodAnnotationQoSLevelDedicatedCores,
			apiconsts.PodAnnotationMemoryEnhancementKey: `{"numa_binding":"true","numa_exclusive":"true"}`,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
	}

	result := make(chan error, 1)
	go func() {
		_, err := p.Allocate(context.Background(), req)
		result <- err
	}()
	<-adjustmentStarted

	const newerPodUID = "newer-concurrent-submission"
	p.Lock()
	p.state.SetAllocationInfo(newerPodUID, "main", &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid: newerPodUID, ContainerName: "main",
			OwnerPoolName: commonstate.PoolNameDedicated,
			QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		AllocationResult:         machine.NewCPUSet(7),
		OriginalAllocationResult: machine.NewCPUSet(7),
	}, false)
	p.Unlock()
	close(releaseAdjustment)

	adjustmentErr := <-result
	require.ErrorContains(t, adjustmentErr, "injected adjustment failure")
	require.NotNil(t, p.state.GetAllocationInfo(newerPodUID, "main"),
		"failed older adjustment rollback overwrote a newer concurrent state submission")
	require.Nil(t, p.state.GetAllocationInfo(req.PodUid, req.ContainerName),
		"failed request left a ghost allocation in the latest pod entries")
	require.False(t, failedCandidateCPUs.IsEmpty())
	require.True(t, p.state.GetMachineState()[0].AllocatedCPUSet.Intersection(failedCandidateCPUs).IsEmpty(),
		"failed request left ghost CPUs %s in machine state %s",
		failedCandidateCPUs, p.state.GetMachineState()[0].AllocatedCPUSet)
	reclaim := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.NotNil(t, reclaim)
	require.True(t, reclaim.AllocationResult.Equals(machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6)),
		"reclaim floor/pool was not recomputed from latest state: %s", reclaim.AllocationResult)
}

func TestAllocateDedicatedNUMAExclusiveAdjustmentFailureReportsOwnershipLostAndReconcilesLatestState(t *testing.T) {
	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25

	const podUID = "exclusive-dnb-ownership-lost"
	adjustmentStarted := make(chan struct{})
	releaseAdjustment := make(chan struct{})
	latestStateReconciled := make(chan struct{}, 1)
	p.cpuSetAdjustmentHandlers = map[string]dynamicpolicyutil.CPUSetAdjustmentHandler{
		"failing": func(_ context.Context, in dynamicpolicyutil.CPUSetAdjustmentHandlerCtx) error {
			if in.Mode == dynamicpolicyutil.CPUSetAdjustmentModeAdmission {
				close(adjustmentStarted)
				<-releaseAdjustment
				return errors.New("injected adjustment failure")
			}
			latestStateReconciled <- struct{}{}
			return nil
		},
	}
	req := &pluginapi.ResourceRequest{
		PodUid: podUID, PodNamespace: "default", PodName: podUID, ContainerName: "main",
		ContainerType: pluginapi.ContainerType_MAIN, ResourceName: string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{string(v1.ResourceCPU): 2},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:          apiconsts.PodAnnotationQoSLevelDedicatedCores,
			apiconsts.PodAnnotationMemoryEnhancementKey: `{"numa_binding":"true","numa_exclusive":"true"}`,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
	}

	result := make(chan error, 1)
	go func() {
		_, err := p.Allocate(context.Background(), req)
		result <- err
	}()
	<-adjustmentStarted

	advanced := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid: podUID, ContainerName: "main",
			OwnerPoolName: commonstate.PoolNameDedicated,
			QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		AllocationResult:         machine.NewCPUSet(6, 7),
		OriginalAllocationResult: machine.NewCPUSet(6, 7),
	}
	p.Lock()
	p.state.SetAllocationInfo(podUID, "main", advanced, false)
	p.Unlock()
	close(releaseAdjustment)

	allocationErr := <-result
	require.Error(t, allocationErr)
	var compensated *requestStateCompensatedError
	require.ErrorAs(t, allocationErr, &compensated)
	var ownershipLost interface{ OwnershipLost() bool }
	require.ErrorAs(t, allocationErr, &ownershipLost)
	require.True(t, ownershipLost.OwnershipLost())
	require.ErrorContains(t, allocationErr, "ownership lost")
	require.True(t, p.state.GetAllocationInfo(podUID, "main").AllocationResult.Equals(advanced.AllocationResult),
		"outer stale snapshot rollback overwrote the advanced allocation")
	select {
	case <-latestStateReconciled:
	case <-time.After(time.Second):
		t.Fatal("ownership loss did not schedule a latest-state reconciliation")
	}
}

func TestAllocateDedicatedNUMAExclusiveRestoreFailureMarksDirtyAndSchedulesBoundedFullRetry(t *testing.T) {
	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25

	retryAttempts := make(chan struct{}, cpuSetAdjustmentRetryMaxAttempts+1)
	p.cpuSetAdjustmentHandlers = map[string]dynamicpolicyutil.CPUSetAdjustmentHandler{
		"failing": func(_ context.Context, in dynamicpolicyutil.CPUSetAdjustmentHandlerCtx) error {
			if in.Mode == dynamicpolicyutil.CPUSetAdjustmentModeAdmission {
				return errors.New("injected admission adjustment failure")
			}
			retryAttempts <- struct{}{}
			return errors.New("injected restore failure")
		},
	}
	req := &pluginapi.ResourceRequest{
		PodUid: "exclusive-dnb-restore-failure", PodNamespace: "default",
		PodName: "exclusive-dnb-restore-failure", ContainerName: "main",
		ContainerType: pluginapi.ContainerType_MAIN, ResourceName: string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{string(v1.ResourceCPU): 2},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:          apiconsts.PodAnnotationQoSLevelDedicatedCores,
			apiconsts.PodAnnotationMemoryEnhancementKey: `{"numa_binding":"true","numa_exclusive":"true"}`,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
	}

	_, allocationErr := p.Allocate(context.Background(), req)
	require.ErrorContains(t, allocationErr, "injected restore failure")
	var compensated *requestStateCompensatedError
	require.ErrorAs(t, allocationErr, &compensated)
	require.Nil(t, p.state.GetAllocationInfo(req.PodUid, req.ContainerName),
		"state compensation did not remove the failed allocation")

	deadline := time.After(2 * time.Second)
	for i := 0; i < cpuSetAdjustmentRetryMaxAttempts+1; i++ {
		select {
		case <-retryAttempts:
		case <-deadline:
			t.Fatalf("restore/full retry attempts = %d, want synchronous restore plus %d bounded retries",
				i, cpuSetAdjustmentRetryMaxAttempts)
		}
	}
	for {
		p.cpuSetAdjustmentRetryMu.Lock()
		queued := p.cpuSetAdjustmentRetryQueued
		dirty := p.cpuSetAdjustmentRetryDirty
		p.cpuSetAdjustmentRetryMu.Unlock()
		if !queued {
			require.True(t, dirty, "bounded full retry exhaustion must leave latest-state reconciliation dirty")
			break
		}
		select {
		case <-deadline:
			t.Fatal("latest-state full retry did not stop within the bounded retry window")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestDedicatedNUMAExclusiveRampUpCommitFailureKeepsPreviousState(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25

	beforeEntries := p.state.GetPodEntries()
	beforeMachine := p.state.GetMachineState()
	tracked := &atomicCommitTrackingState{
		State:       p.state,
		commitErr:   errors.New("injected atomic commit failure"),
		failCommits: -1,
	}
	p.state = tracked
	req := &pluginapi.ResourceRequest{
		PodUid: "exclusive-dnb-commit-failure", PodNamespace: "default",
		PodName: "exclusive-dnb-commit-failure", ContainerName: "main",
		ContainerType: pluginapi.ContainerType_MAIN, ResourceName: string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{string(v1.ResourceCPU): 2},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                    apiconsts.PodAnnotationQoSLevelDedicatedCores,
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
	}

	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(context.Background(), req, true)
	require.Nil(t, resp)
	require.ErrorContains(t, err, "injected atomic commit failure")
	require.Equal(t, 1, tracked.commitCalls)
	require.Zero(t, tracked.storeCalls)
	require.True(t, reflect.DeepEqual(beforeEntries, p.state.GetPodEntries()))
	require.True(t, reflect.DeepEqual(beforeMachine, p.state.GetMachineState()))
}

func TestDedicatedNUMAExclusiveRampUpRejectsEmptyReclaimFloor(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0
	req := &pluginapi.ResourceRequest{
		PodUid: "exclusive-dnb-empty-floor", PodNamespace: "default",
		PodName: "exclusive-dnb-empty-floor", ContainerName: "main",
		ContainerType: pluginapi.ContainerType_MAIN, ResourceName: string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{string(v1.ResourceCPU): 2},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                    apiconsts.PodAnnotationQoSLevelDedicatedCores,
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
	}

	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(context.Background(), req, true)
	require.Nil(t, resp)
	require.ErrorContains(t, err, "requires non-empty reclaim floor on NUMA 0")
	require.Nil(t, p.state.GetAllocationInfo(req.PodUid, req.ContainerName))
}

func TestAllocateRestoresPreviousDNBWhenAtomicCommitFails(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25
	const podUID = "existing-exclusive-dnb"
	oldAllocation := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid: podUID, PodNamespace: "default", PodName: podUID,
			ContainerName: "main", ContainerType: pluginapi.ContainerType_MAIN.String(),
			QoSLevel: apiconsts.PodAnnotationQoSLevelDedicatedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
			},
		},
		AllocationResult:         machine.NewCPUSet(0, 1),
		OriginalAllocationResult: machine.NewCPUSet(0, 1),
		RampUp:                   true,
	}
	p.state.SetAllocationInfo(podUID, "main", oldAllocation, false)
	tracked := &atomicCommitTrackingState{
		State: p.state, commitErr: errors.New("injected atomic commit failure"), failCommits: 1,
	}
	p.state = tracked
	req := &pluginapi.ResourceRequest{
		PodUid: podUID, PodNamespace: "default", PodName: podUID,
		ContainerName: "main", ContainerType: pluginapi.ContainerType_MAIN,
		ResourceName:     string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{string(v1.ResourceCPU): 4},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:          apiconsts.PodAnnotationQoSLevelDedicatedCores,
			apiconsts.PodAnnotationMemoryEnhancementKey: `{"numa_binding":"true","numa_exclusive":"true"}`,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
	}

	resp, err := p.Allocate(context.Background(), req)
	require.Nil(t, resp)
	require.ErrorContains(t, err, "injected atomic commit failure")
	restored := p.state.GetAllocationInfo(podUID, "main")
	require.NotNil(t, restored)
	require.True(t, restored.AllocationResult.Equals(oldAllocation.AllocationResult))
}

func TestDynamicPolicy_adjustPoolsAndIsolatedEntries_Pinned(t *testing.T) {
	t.Parallel()
	as := require.New(t)

	// Setup topology: 2 sockets, 8 cores each. Total 16 CPUs.
	// S0: 0-7, S1: 8-15.
	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	as.Nil(err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_adjustPoolsAndIsolatedEntries_Pinned")
	as.Nil(err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	as.Nil(err)

	// Clear reserved CPUs to ensure deterministic allocation for test
	p.reservedCPUs = machine.NewCPUSet()

	// Enable Reclaim
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	// Disable overlap to ensure pool2 gets exactly what it requests (4 cores)
	// If enabled, it would take all available cores (12) which is also correct behavior but makes checking "exactly 4" fail.
	// We want to verify it can successfully allocate 4 from the remaining unpinned set.
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, true)

	// Setup Pinned CPUSets
	// pkg1 pinned to {0, 1} (NUMA 0)
	// pkg2 pinned to {2, 3} (NUMA 0) -- BUT no pools use it!
	machineState := p.state.GetMachineState()
	for numaID, numaState := range machineState {
		if numaID == 0 {
			if numaState.ResourcePackageStates == nil {
				numaState.ResourcePackageStates = make(map[string]*state.ResourcePackageState)
			}
			numaState.ResourcePackageStates["pkg1"] = &state.ResourcePackageState{PinnedCPUSet: machine.NewCPUSet(0, 1)}
			numaState.ResourcePackageStates["pkg2"] = &state.ResourcePackageState{PinnedCPUSet: machine.NewCPUSet(2, 3)}
		}
	}
	p.state.SetMachineState(machineState, false)

	// Setup Pools Quantity
	// pkg1/pool1: 2 cores (should take 0, 1)
	// pool2 (common): 4 cores (should take from available excluding 0, 1 AND 2, 3)
	// commonAvailableCPUs should be {4-15}.
	// pool2 needs 4 cores. It should get 4, 5, 6, 7 (if taking from NUMA 0 first) or spread.
	// Since NUMA 0 has 4,5,6,7 available (4 cores).
	// NUMA 1 has 8-15 available (8 cores).
	// pool2 is FakedNUMAID.
	poolsQuantityMap := map[string]map[int]int{
		"pkg1/pool1": {
			commonstate.FakedNUMAID: 2,
		},
		"pool2": {
			commonstate.FakedNUMAID: 4,
		},
		commonstate.PoolNameReclaim: {
			commonstate.FakedNUMAID: 0,
		},
	}

	isolatedQuantityMap := map[string]map[string]int{}

	// Seed entries for Reclaim pool (needed for reclaimOverlapNUMABinding check)
	// And seed containers to prevent cleanPools from removing the pools
	entries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult:         machine.NewCPUSet(14, 15),
				OriginalAllocationResult: machine.NewCPUSet(14, 15),
				TopologyAwareAssignments: map[int]machine.CPUSet{1: machine.NewCPUSet(14, 15)},
			},
		},
		"pod1": {
			"container1": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					PodNamespace:  "default",
					PodName:       "pod1",
					ContainerName: "container1",
					OwnerPoolName: "pkg1/pool1",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				},
			},
		},
		"pod2": {
			"container2": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod2",
					PodNamespace:  "default",
					PodName:       "pod2",
					ContainerName: "container2",
					OwnerPoolName: "pool2",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				},
			},
		},
	}

	err = p.adjustPoolsAndIsolatedEntries(poolsQuantityMap, isolatedQuantityMap, entries, machineState, false)
	as.Nil(err)

	updatedEntries := p.state.GetPodEntries()

	// Verify Results
	// pkg1/pool1 should be {0, 1}
	pool1Entry := updatedEntries["pkg1/pool1"][commonstate.FakedContainerName]
	as.NotNil(pool1Entry)
	as.True(pool1Entry.AllocationResult.Equals(machine.NewCPUSet(0, 1)), "pool1 should have pinned CPUs 0,1, got %s", pool1Entry.AllocationResult.String())

	// pool2 should NOT contain 0, 1 (used by pkg1) AND should NOT contain 2, 3 (reserved by pkg2 even if unused)
	pool2Entry := updatedEntries["pool2"][commonstate.FakedContainerName]
	as.NotNil(pool2Entry)
	// Check intersection with pkg1 pinned
	as.False(pool2Entry.AllocationResult.Intersection(machine.NewCPUSet(0, 1)).Size() > 0, "pool2 should not use pinned CPUs 0,1, got %s", pool2Entry.AllocationResult.String())
	// Check intersection with pkg2 pinned (unused but reserved)
	as.False(pool2Entry.AllocationResult.Intersection(machine.NewCPUSet(2, 3)).Size() > 0, "pool2 should not use pinned CPUs 2,3 (reserved for pkg2), got %s", pool2Entry.AllocationResult.String())

	// Verify pool2 size
	as.Equal(4, pool2Entry.AllocationResult.Size(), "pool2 should have 4 cores")
}

// TestDynamicPolicy_groupAndAllocatePools tests the groupAndAllocatePools function.
// It verifies that pools are correctly grouped into pinned and common categories,
// and that CPUs are allocated according to availability and constraints.
func TestDynamicPolicy_groupAndAllocatePools(t *testing.T) {
	t.Parallel()

	type args struct {
		poolsQuantityMap         map[string]map[int]int
		isolatedQuantityMap      map[string]map[string]int
		availableCPUs            machine.CPUSet
		rpPinnedCPUSet           map[string]machine.CPUSet
		reclaimOverlapShareRatio map[string]float64
	}
	tests := []struct {
		name         string
		args         args
		wantPools    map[string]machine.CPUSet
		wantIsolated map[string]map[string]machine.CPUSet
		wantErr      bool
	}{
		{
			name: "Scenario 1: Common Pools Only - Verifies that when no pools are pinned, all pools are treated as common and allocated from the general available CPU set.",
			args: args{
				poolsQuantityMap: map[string]map[int]int{
					"pool1": {commonstate.FakedNUMAID: 2},
				},
				availableCPUs: machine.NewCPUSet(0, 1, 2, 3),
			},
			wantPools: map[string]machine.CPUSet{
				"pool1": machine.NewCPUSet(0, 1),
			},
			wantErr: false,
		},
		{
			name: "Scenario 2: Pinned Pools Only - Verifies that pools belonging to a resource package are correctly identified and allocated exclusively from that package's pinned CPU set.",
			args: args{
				poolsQuantityMap: map[string]map[int]int{
					rputil.WrapOwnerPoolName("pool1", "pkg1"): {commonstate.FakedNUMAID: 2},
				},
				availableCPUs: machine.NewCPUSet(0, 1, 2, 3),
				rpPinnedCPUSet: map[string]machine.CPUSet{
					"pkg1": machine.NewCPUSet(0, 1),
				},
			},
			wantPools: map[string]machine.CPUSet{
				rputil.WrapOwnerPoolName("pool1", "pkg1"): machine.NewCPUSet(0, 1),
			},
			wantErr: false,
		},
		{
			name: "Scenario 3: Mixed Pinned and Common Pools - Verifies that the function correctly splits pinned and common pools, allocating pinned pools from their specific sets and common pools from the remaining available CPUs.",
			args: args{
				poolsQuantityMap: map[string]map[int]int{
					rputil.WrapOwnerPoolName("pool1", "pkg1"): {commonstate.FakedNUMAID: 2},
					"pool2": {commonstate.FakedNUMAID: 2},
				},
				availableCPUs: machine.NewCPUSet(0, 1, 2, 3),
				rpPinnedCPUSet: map[string]machine.CPUSet{
					"pkg1": machine.NewCPUSet(0, 1),
				},
			},
			wantPools: map[string]machine.CPUSet{
				rputil.WrapOwnerPoolName("pool1", "pkg1"): machine.NewCPUSet(0, 1),
				"pool2": machine.NewCPUSet(2, 3),
			},
			wantErr: false,
		},
		{
			name: "Scenario 4: Isolated Containers - Verifies that isolated containers are allocated dedicated CPUs from the common available set alongside common pools.",
			args: args{
				poolsQuantityMap: map[string]map[int]int{
					"pool1": {commonstate.FakedNUMAID: 2},
				},
				isolatedQuantityMap: map[string]map[string]int{
					"pod1": {"container1": 2},
				},
				availableCPUs: machine.NewCPUSet(0, 1, 2, 3),
			},
			wantPools: map[string]machine.CPUSet{
				"pool1": machine.NewCPUSet(2, 3),
			},
			wantIsolated: map[string]map[string]machine.CPUSet{
				"pod1": {"container1": machine.NewCPUSet(0, 1)},
			},
			wantErr: false,
		},
		{
			name: "Scenario 5: Error - Pinned Pool Insufficient CPUs - Verifies that the function degrades gracefully and allocates available CPUs (partial) if a pinned pool requests more CPUs than are available in its pinned set.",
			args: args{
				poolsQuantityMap: map[string]map[int]int{
					rputil.WrapOwnerPoolName("pool1", "pkg1"): {commonstate.FakedNUMAID: 4},
				},
				availableCPUs: machine.NewCPUSet(0, 1, 2, 3),
				rpPinnedCPUSet: map[string]machine.CPUSet{
					"pkg1": machine.NewCPUSet(0, 1),
				},
			},
			wantPools: map[string]machine.CPUSet{
				rputil.WrapOwnerPoolName("pool1", "pkg1"): machine.NewCPUSet(0, 1),
			},
			wantErr: false,
		},
		{
			name: "Scenario 6: Error - Common Pool Insufficient CPUs - Verifies that the function degrades gracefully and allocates available CPUs (partial) if common pools request more CPUs than are available in the shared pool.",
			args: args{
				poolsQuantityMap: map[string]map[int]int{
					"pool1": {commonstate.FakedNUMAID: 4},
				},
				availableCPUs: machine.NewCPUSet(0, 1, 2, 3),
				rpPinnedCPUSet: map[string]machine.CPUSet{
					"pkg1": machine.NewCPUSet(0, 1),
				},
			},
			wantPools: map[string]machine.CPUSet{
				"pool1": machine.NewCPUSet(2, 3),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			as := require.New(t)

			cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
			as.Nil(err)

			tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_groupAndAllocatePools")
			as.Nil(err)
			defer os.RemoveAll(tmpDir)

			p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
			as.Nil(err)

			// Clear state
			p.state.SetPodEntries(state.PodEntries{}, false)
			p.reservedCPUs = machine.NewCPUSet()

			gotPools, gotIsolated, err := p.groupAndAllocatePools(tt.args.poolsQuantityMap, tt.args.isolatedQuantityMap, tt.args.availableCPUs, tt.args.rpPinnedCPUSet, tt.args.reclaimOverlapShareRatio)
			if (err != nil) != tt.wantErr {
				t.Errorf("groupAndAllocatePools() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				// Filter out system pools (reclaim, reserve) for comparison
				filteredPools := make(map[string]machine.CPUSet)
				for k, v := range gotPools {
					if k != commonstate.PoolNameReclaim && k != commonstate.PoolNameReserve {
						filteredPools[k] = v
					}
				}

				if !reflect.DeepEqual(filteredPools, tt.wantPools) {
					t.Errorf("groupAndAllocatePools() gotPools = %v, want %v", filteredPools, tt.wantPools)
				}

				if len(gotIsolated) == 0 && len(tt.wantIsolated) == 0 {
					// Both empty/nil, treat as equal
				} else if !reflect.DeepEqual(gotIsolated, tt.wantIsolated) {
					t.Errorf("groupAndAllocatePools() gotIsolated = %v, want %v", gotIsolated, tt.wantIsolated)
				}
			}
		})
	}
}

func mustGenerateDummyCPUTopology(numCPUs, numSockets, numaNum int) *machine.CPUTopology {
	topo, err := machine.GenerateDummyCPUTopology(numCPUs, numSockets, numaNum)
	if err != nil {
		panic(err)
	}
	return topo
}
