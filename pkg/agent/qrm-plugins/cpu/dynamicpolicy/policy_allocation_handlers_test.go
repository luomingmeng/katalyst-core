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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/accompanyresource"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	dynamicpolicyutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/spd"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	rputil "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
	"github.com/kubewharf/katalyst-core/pkg/util/timemonitor"
)

type getPodErrorPodFetcher struct {
	*pod.PodFetcherStub
	err error
	pod *v1.Pod
}

type recordingServiceProfilingManager struct {
	*spd.DummyServiceProfilingManager
	performanceLevel spd.PerformanceLevel
	performanceErr   error
	baseline         bool
	baselineErr      error
	baselineCalls    int
	observedPodMeta  []metav1.ObjectMeta
}

func (m *recordingServiceProfilingManager) ServiceBusinessPerformanceLevel(
	ctx context.Context, podMeta metav1.ObjectMeta,
) (spd.PerformanceLevel, error) {
	m.observedPodMeta = append(m.observedPodMeta, podMeta)
	if err := ctx.Err(); err != nil {
		return spd.PerformanceLevelUnknown, err
	}
	return m.performanceLevel, m.performanceErr
}

func (m *recordingServiceProfilingManager) ServiceBaseline(
	ctx context.Context, podMeta metav1.ObjectMeta,
) (bool, error) {
	m.baselineCalls++
	m.observedPodMeta = append(m.observedPodMeta, podMeta)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return m.baseline, m.baselineErr
}

func (f *getPodErrorPodFetcher) GetPod(_ context.Context, _ string) (*v1.Pod, error) {
	return f.pod, f.err
}

type failingAllocateAccompanyPlugin struct {
	err error
}

func (*failingAllocateAccompanyPlugin) ResourceName() string {
	return "failing-allocate"
}

func (*failingAllocateAccompanyPlugin) GetAccompanyResourceTopologyHints(
	_ *pluginapi.ResourceRequest,
	_ *pluginapi.ListOfTopologyHints,
) error {
	return nil
}

func (p *failingAllocateAccompanyPlugin) AllocateAccompanyResource(
	_ *pluginapi.ResourceRequest,
	_ *pluginapi.ResourceAllocationResponse,
) error {
	return p.err
}

func (*failingAllocateAccompanyPlugin) ReleaseAccompanyResource(_ *pluginapi.RemovePodRequest) error {
	return nil
}

type atomicCommitTrackingState struct {
	state.State
	commitErr   error
	failCommits int
	commitCalls int
	storeCalls  int
}

type applyPoolsCommitGuardState struct {
	state.State
	setPodEntriesCalls    int
	setMachineStateCalls  int
	storeCalls            int
	conditionalCalls      int
	conditionalRevision   uint64
	conditionalAllow      bool
	conditionalDisable    bool
	conditionalPersist    bool
	injectRevisionAdvance bool
}

type missingAllocationInfoState struct {
	state.State
	missingPodUID        string
	missingContainerName string
}

type revisionAdvanceOnOverlapReadState struct {
	state.State
	advanceOnce bool
}

func (s *revisionAdvanceOnOverlapReadState) GetAllowSharedCoresOverlapReclaimedCores() bool {
	allow := s.State.GetAllowSharedCoresOverlapReclaimedCores()
	if s.advanceOnce {
		s.advanceOnce = false
		s.State.SetDisableDedicatedCoresOverlapReclaimedCores(
			!s.State.GetDisableDedicatedCoresOverlapReclaimedCores(), false)
	}
	return allow
}

func (s *missingAllocationInfoState) GetAllocationInfo(podUID string, containerName string) *state.AllocationInfo {
	if podUID == s.missingPodUID && containerName == s.missingContainerName {
		return nil
	}
	return s.State.GetAllocationInfo(podUID, containerName)
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

func (s *atomicCommitTrackingState) CommitAdvisorStateIfRevision(
	expectedRevision uint64,
	podEntries state.PodEntries,
	machineState state.NUMANodeMap,
	allowOverlap, disableDedicatedOverlap, persist bool,
) error {
	s.commitCalls++
	if s.commitErr != nil && (s.failCommits < 0 || s.commitCalls <= s.failCommits) {
		return s.commitErr
	}
	return s.State.CommitAdvisorStateIfRevision(
		expectedRevision, podEntries, machineState, allowOverlap, disableDedicatedOverlap, persist)
}

func (s *atomicCommitTrackingState) StoreState() error {
	s.storeCalls++
	return s.State.StoreState()
}

func TestIsRampUpReclaimHardPartitionEnabledRequiresNodeReclaim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		enableReclaim   bool
		enableHardFloor bool
		want            bool
	}{
		{
			name:            "hard partition disabled",
			enableReclaim:   true,
			enableHardFloor: false,
			want:            false,
		},
		{
			name:            "reclaim disabled disables hard partition",
			enableReclaim:   false,
			enableHardFloor: true,
			want:            false,
		},
		{
			name:            "reclaim and hard partition enabled",
			enableReclaim:   true,
			enableHardFloor: true,
			want:            true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dyn := dynamicconfig.NewDynamicAgentConfiguration()
			dynamicConf := dyn.GetDynamicConfiguration()
			dynamicConf.EnableReclaim = tt.enableReclaim
			dynamicConf.EnableRampUpReclaimHardPartition = tt.enableHardFloor

			p := &DynamicPolicy{dynamicConfig: dyn}
			assert.Equal(t, tt.want, p.isRampUpReclaimHardPartitionEnabled())
		})
	}
}

func (s *applyPoolsCommitGuardState) SetPodEntries(entries state.PodEntries, persist bool) {
	s.setPodEntriesCalls++
	s.State.SetPodEntries(entries, persist)
}

func (s *applyPoolsCommitGuardState) SetMachineState(machineState state.NUMANodeMap, persist bool) {
	s.setMachineStateCalls++
	s.State.SetMachineState(machineState, persist)
}

func (s *applyPoolsCommitGuardState) StoreState() error {
	s.storeCalls++
	return s.State.StoreState()
}

func (s *applyPoolsCommitGuardState) CommitAdvisorStateIfRevision(
	expectedRevision uint64,
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	allowOverlap, disableDedicatedOverlap, persist bool,
) error {
	s.conditionalCalls++
	s.conditionalRevision = expectedRevision
	s.conditionalAllow = allowOverlap
	s.conditionalDisable = disableDedicatedOverlap
	s.conditionalPersist = persist
	if s.injectRevisionAdvance {
		s.State.SetAllowSharedCoresOverlapReclaimedCores(
			!s.State.GetAllowSharedCoresOverlapReclaimedCores(), false)
	}
	return s.State.CommitAdvisorStateIfRevision(
		expectedRevision, entries, machineState, allowOverlap, disableDedicatedOverlap, persist)
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
	policy.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
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
	policy.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
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

func TestDynamicPolicy_allocateNumaBindingCPUs_exclusiveDisjointPartition(t *testing.T) {
	t.Parallel()

	exclusiveAnnotations := func(resourcePackage string) map[string]string {
		annotations := map[string]string{
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
		}
		if resourcePackage != "" {
			annotations[apiconsts.PodAnnotationResourcePackageKey] = resourcePackage
		}
		return annotations
	}
	newPolicy := func(t *testing.T) *DynamicPolicy {
		topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
		require.NoError(t, err)
		p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
		require.NoError(t, err)
		p.reservedCPUs = machine.NewCPUSet()
		p.conf.SetDynamicConfiguration(nil)
		p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
		p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
		p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0
		p.dynamicConfig.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector = "disable-reclaim=true"
		p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
		return p
	}
	call := func(p *DynamicPolicy, machineState state.NUMANodeMap, annotations map[string]string, podReclaimEnabled bool) (machine.CPUSet, machine.CPUSet, error) {
		return p.allocateNumaBindingCPUs(
			8,
			&pluginapi.TopologyHint{Nodes: []uint64{0}},
			machineState,
			annotations,
			podReclaimEnabled,
		)
	}

	t.Run("pinned package preserves reserve and reclaim-only eligibility when pod reclaim is disabled", func(t *testing.T) {
		p := newPolicy(t)
		p.reservedReclaimedCPUSet = machine.NewCPUSet(0, 4)
		p.reservedReclaimedCPUsSize = 2
		machineState := state.NUMANodeMap{
			0: {
				DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
				ResourcePackageStates: map[string]*state.ResourcePackageState{
					"work": {
						PinnedCPUSet: machine.NewCPUSet(0, 1, 4, 5),
					},
					"protected": {
						Attributes:   map[string]string{"disable-reclaim": "true"},
						PinnedCPUSet: machine.NewCPUSet(3, 7),
					},
				},
			},
		}

		result, reclaim, err := call(p, machineState, exclusiveAnnotations("work"), false)
		require.NoError(t, err)
		require.True(t, result.Equals(machine.NewCPUSet(1, 5)), "result=%s", result)
		require.True(t, reclaim.Equals(machine.NewCPUSet(0, 2, 4, 6)), "reclaim=%s", reclaim)
		require.True(t, result.Intersection(reclaim).IsEmpty())
		require.True(t, result.Union(reclaim).Equals(machine.NewCPUSet(0, 1, 2, 4, 5, 6)))
		require.True(t, reclaim.Intersection(machine.NewCPUSet(3, 7)).IsEmpty())
	})

	t.Run("unpinned package preserves reserve and reclaim-only eligibility", func(t *testing.T) {
		p := newPolicy(t)
		p.reservedReclaimedCPUSet = machine.NewCPUSet(2, 6)
		p.reservedReclaimedCPUsSize = 2
		machineState := state.NUMANodeMap{
			0: {
				DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
				ResourcePackageStates: map[string]*state.ResourcePackageState{
					"other": {
						PinnedCPUSet: machine.NewCPUSet(0, 4),
					},
					"protected": {
						Attributes:   map[string]string{"disable-reclaim": "true"},
						PinnedCPUSet: machine.NewCPUSet(3, 7),
					},
				},
			},
		}

		result, reclaim, err := call(p, machineState, exclusiveAnnotations(""), true)
		require.NoError(t, err)
		require.True(t, result.Equals(machine.NewCPUSet(1, 5)), "result=%s", result)
		require.True(t, reclaim.Equals(machine.NewCPUSet(0, 2, 4, 6)), "reclaim=%s", reclaim)
		require.True(t, result.Intersection(reclaim).IsEmpty())
		require.True(t, result.Union(reclaim).Equals(machine.NewCPUSet(0, 1, 2, 4, 5, 6)))
	})

	t.Run("rejects empty dedicated partition", func(t *testing.T) {
		p := newPolicy(t)
		p.reservedReclaimedCPUSet = machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7)
		p.reservedReclaimedCPUsSize = 8
		machineState := state.NUMANodeMap{
			0: {DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7)},
		}

		_, _, err := call(p, machineState, exclusiveAnnotations(""), true)
		require.ErrorContains(t, err, "dedicated result is empty")
	})

	t.Run("rejects reserve when reclaim eligibility is insufficient", func(t *testing.T) {
		p := newPolicy(t)
		p.reservedReclaimedCPUSet = machine.NewCPUSet(0, 1)
		p.reservedReclaimedCPUsSize = 2
		machineState := state.NUMANodeMap{
			0: {
				DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
				ResourcePackageStates: map[string]*state.ResourcePackageState{
					"protected": {
						Attributes:   map[string]string{"disable-reclaim": "true"},
						PinnedCPUSet: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
					},
				},
			},
		}

		_, _, err := call(p, machineState, exclusiveAnnotations(""), false)
		require.Error(t, err)
	})

	t.Run("legacy overlap mode still requires request size", func(t *testing.T) {
		p := newPolicy(t)
		p.state.SetDisableDedicatedCoresOverlapReclaimedCores(false, false)
		p.reservedReclaimedCPUSet = machine.NewCPUSet(0, 1)
		p.reservedReclaimedCPUsSize = 2
		machineState := state.NUMANodeMap{
			0: {DefaultCPUSet: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7)},
		}

		_, _, err := call(p, machineState, exclusiveAnnotations(""), true)
		require.ErrorContains(t, err, "results can't meet cpus request")
	})
}

func TestDynamicPolicyPodEnableReclaimForNumaBindingAllocation(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	podFetcherErr := errors.New("pod fetcher failed")
	spdErr := errors.New("spd failed")
	baselineErr := errors.New("baseline failed")

	newPolicy := func(t *testing.T, fetchedPod *v1.Pod, getPodErr error, manager *recordingServiceProfilingManager) *DynamicPolicy {
		p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
		require.NoError(t, err)
		p.metaServer.MetaAgent.PodFetcher = &getPodErrorPodFetcher{
			PodFetcherStub: &pod.PodFetcherStub{},
			err:            getPodErr,
			pod:            fetchedPod,
		}
		p.metaServer.ServiceProfilingManager = manager
		p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
		p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
		p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
		return p
	}

	originalPodMeta := metav1.ObjectMeta{
		UID:       types.UID("pod-uid"),
		Namespace: "pod-namespace",
		Name:      "pod-name",
		Labels:    map[string]string{"spd-label": "value"},
		Annotations: map[string]string{
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
			"spd-annotation": "value",
		},
	}
	filteredExclusive := map[string]string{
		apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
	}
	req := &pluginapi.ResourceRequest{
		PodUid:       "pod-uid",
		PodNamespace: "pod-namespace",
		PodName:      "pod-name",
		Annotations:  filteredExclusive,
	}
	ctx := context.WithValue(context.Background(), allocationPodMetaContextKey{}, originalPodMeta)

	t.Run("poor request metadata disables reclaim", func(t *testing.T) {
		manager := &recordingServiceProfilingManager{
			DummyServiceProfilingManager: &spd.DummyServiceProfilingManager{},
			performanceLevel:             spd.PerformanceLevelPoor,
		}
		p := newPolicy(t, nil, podFetcherErr, manager)

		got, err := p.podEnableReclaimForNumaBindingAllocation(ctx, req)
		require.NoError(t, err)
		require.False(t, got)
		require.Equal(t, []metav1.ObjectMeta{originalPodMeta}, manager.observedPodMeta)
		require.Zero(t, manager.baselineCalls)
	})

	t.Run("good non-baseline metadata enables reclaim", func(t *testing.T) {
		manager := &recordingServiceProfilingManager{
			DummyServiceProfilingManager: &spd.DummyServiceProfilingManager{},
			performanceLevel:             spd.PerformanceLevelGood,
		}
		p := newPolicy(t, nil, pod.NewPodNotFoundError(req.PodUid), manager)

		got, err := p.podEnableReclaimForNumaBindingAllocation(ctx, req)
		require.NoError(t, err)
		require.True(t, got)
		require.Equal(t, []metav1.ObjectMeta{originalPodMeta}, manager.observedPodMeta)
		require.Zero(t, manager.baselineCalls)
	})

	t.Run("baseline error propagates", func(t *testing.T) {
		storedPodMeta := metav1.ObjectMeta{
			UID:               types.UID(req.PodUid),
			CreationTimestamp: metav1.Now(),
		}
		manager := &recordingServiceProfilingManager{
			DummyServiceProfilingManager: &spd.DummyServiceProfilingManager{},
			performanceLevel:             spd.PerformanceLevelGood,
			baselineErr:                  podFetcherErr,
		}
		p := newPolicy(t, &v1.Pod{ObjectMeta: storedPodMeta}, nil, manager)

		got, err := p.podEnableReclaimForNumaBindingAllocation(ctx, req)
		require.ErrorIs(t, err, podFetcherErr)
		require.False(t, got)
		require.Equal(t, []metav1.ObjectMeta{originalPodMeta, storedPodMeta}, manager.observedPodMeta)
	})

	t.Run("request metadata SPD error propagates", func(t *testing.T) {
		manager := &recordingServiceProfilingManager{
			DummyServiceProfilingManager: &spd.DummyServiceProfilingManager{},
			performanceErr:               spdErr,
		}
		p := newPolicy(t, nil, nil, manager)

		got, err := p.podEnableReclaimForNumaBindingAllocation(ctx, req)
		require.ErrorIs(t, err, spdErr)
		require.False(t, got)
		require.Zero(t, manager.baselineCalls)
	})

	t.Run("good metadata uses full baseline decision", func(t *testing.T) {
		storedPodMeta := metav1.ObjectMeta{
			UID:               types.UID(req.PodUid),
			CreationTimestamp: metav1.Now(),
		}
		manager := &recordingServiceProfilingManager{
			DummyServiceProfilingManager: &spd.DummyServiceProfilingManager{},
			performanceLevel:             spd.PerformanceLevelGood,
			baseline:                     true,
		}
		p := newPolicy(t, &v1.Pod{ObjectMeta: storedPodMeta}, nil, manager)

		got, err := p.podEnableReclaimForNumaBindingAllocation(ctx, req)
		require.NoError(t, err)
		require.False(t, got)
		require.Equal(t, 1, manager.baselineCalls)
		require.Equal(t, []metav1.ObjectMeta{originalPodMeta, storedPodMeta}, manager.observedPodMeta)
	})

	t.Run("baseline error propagates", func(t *testing.T) {
		storedPodMeta := metav1.ObjectMeta{
			UID:               types.UID(req.PodUid),
			CreationTimestamp: metav1.Now(),
		}
		manager := &recordingServiceProfilingManager{
			DummyServiceProfilingManager: &spd.DummyServiceProfilingManager{},
			performanceLevel:             spd.PerformanceLevelGood,
			baselineErr:                  baselineErr,
		}
		p := newPolicy(t, &v1.Pod{ObjectMeta: storedPodMeta}, nil, manager)

		got, err := p.podEnableReclaimForNumaBindingAllocation(ctx, req)
		require.ErrorIs(t, err, baselineErr)
		require.False(t, got)
		require.Equal(t, 1, manager.baselineCalls)
	})

	t.Run("context cancellation propagates", func(t *testing.T) {
		manager := &recordingServiceProfilingManager{
			DummyServiceProfilingManager: &spd.DummyServiceProfilingManager{},
			performanceLevel:             spd.PerformanceLevelGood,
		}
		p := newPolicy(t, nil, nil, manager)
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		got, err := p.podEnableReclaimForNumaBindingAllocation(canceledCtx, req)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, got)
		require.Zero(t, manager.baselineCalls)
	})

	t.Run("non-exclusive preserves legacy fetch error fallback", func(t *testing.T) {
		manager := &recordingServiceProfilingManager{
			DummyServiceProfilingManager: &spd.DummyServiceProfilingManager{},
		}
		p := newPolicy(t, nil, podFetcherErr, manager)
		nonExclusiveReq := *req
		nonExclusiveReq.Annotations = map[string]string{
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		}

		got, err := p.podEnableReclaimForNumaBindingAllocation(context.Background(), &nonExclusiveReq)
		require.NoError(t, err)
		require.False(t, got)
		require.Empty(t, manager.observedPodMeta)
	})
}

func TestDynamicPolicy_allocateNumaBindingCPUs_partitionEligibilityGate(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	newPolicy := func(t *testing.T) *DynamicPolicy {
		p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
		require.NoError(t, err)
		p.reservedCPUs = machine.NewCPUSet()
		p.reservedReclaimedCPUSet = machine.NewCPUSet(0)
		p.reservedReclaimedCPUsSize = 1
		p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0
		p.dynamicConfig.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector = "invalid,,selector"
		return p
	}
	machineState := state.NUMANodeMap{
		0: {DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(0)},
		1: nil,
	}
	exclusive := map[string]string{
		apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
	}

	t.Run("feature disabled ignores selector and unrelated nil NUMA", func(t *testing.T) {
		p := newPolicy(t)
		p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = false
		p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)

		result, reclaim, err := p.allocateNumaBindingCPUs(
			4, &pluginapi.TopologyHint{Nodes: []uint64{0}}, machineState, exclusive, false)
		require.NoError(t, err)
		require.True(t, result.Equals(machineState[0].DefaultCPUSet), "result=%s", result)
		require.True(t, reclaim.IsEmpty(), "reclaim=%s", reclaim)
	})

	t.Run("nonexclusive uses legacy path before selector", func(t *testing.T) {
		p := newPolicy(t)
		p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
		p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
		p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
		completeState := state.NUMANodeMap{
			0: {DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(0)},
			1: {DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(1)},
		}
		annotations := map[string]string{
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		}

		result, reclaim, err := p.allocateNumaBindingCPUs(
			2, &pluginapi.TopologyHint{Nodes: []uint64{0}}, completeState, annotations, false)
		require.NoError(t, err)
		require.Equal(t, 2, result.Size())
		require.Equal(t, 4, reclaim.Size())
		require.Equal(t, 2, reclaim.Intersection(topology.CPUDetails.CPUsInNUMANodes(0)).Size())
		require.Equal(t, 2, reclaim.Intersection(topology.CPUDetails.CPUsInNUMANodes(1)).Size())
	})

	t.Run("DD false uses legacy path before selector", func(t *testing.T) {
		p := newPolicy(t)
		p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
		p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
		p.state.SetDisableDedicatedCoresOverlapReclaimedCores(false, false)
		completeState := state.NUMANodeMap{
			0: {DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(0)},
			1: {DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(1)},
		}

		result, reclaim, err := p.allocateNumaBindingCPUs(
			2, &pluginapi.TopologyHint{Nodes: []uint64{0}}, completeState, exclusive, false)
		require.NoError(t, err)
		require.Equal(t, 4, reclaim.Size())
		require.Equal(t, 2, reclaim.Intersection(topology.CPUDetails.CPUsInNUMANodes(0)).Size())
		require.Equal(t, 2, reclaim.Intersection(topology.CPUDetails.CPUsInNUMANodes(1)).Size())
		require.True(t, result.Equals(completeState[0].DefaultCPUSet.Difference(reclaim)), "result=%s", result)
	})
}

func TestDynamicPolicy_allocateNumaBindingCPUs_exclusiveDisjointMultiNUMA(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet(0, 2, 4, 6)
	p.reservedReclaimedCPUsSize = 4
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0
	p.dynamicConfig.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector = "disable-reclaim=true"
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
	machineState := state.NUMANodeMap{
		0: {
			DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(0),
			ResourcePackageStates: map[string]*state.ResourcePackageState{
				"work":      {PinnedCPUSet: machine.NewCPUSet(0, 1)},
				"protected": {Attributes: map[string]string{"disable-reclaim": "true"}, PinnedCPUSet: machine.NewCPUSet(5)},
			},
		},
		1: {
			DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(1),
			ResourcePackageStates: map[string]*state.ResourcePackageState{
				"work":      {PinnedCPUSet: machine.NewCPUSet(2, 3)},
				"protected": {Attributes: map[string]string{"disable-reclaim": "true"}, PinnedCPUSet: machine.NewCPUSet(7)},
			},
		},
	}
	annotations := map[string]string{
		apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
		apiconsts.PodAnnotationResourcePackageKey:             "work",
	}

	result, reclaim, err := p.allocateNumaBindingCPUs(
		8, &pluginapi.TopologyHint{Nodes: []uint64{0, 1}}, machineState, annotations, false)
	require.NoError(t, err)
	require.True(t, reclaim.Equals(machine.NewCPUSet(0, 2, 4, 6)), "reclaim=%s", reclaim)
	require.True(t, result.Equals(machine.NewCPUSet(1, 3)), "result=%s", result)
	require.True(t, result.Intersection(reclaim).IsEmpty())
	require.True(t, result.Union(reclaim).Equals(machine.NewCPUSet(0, 1, 2, 3, 4, 6)))
}

func TestDynamicPolicy_allocateNumaBindingCPUs_nonReclaimableUsesConfiguredSteadyFloor(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet(0, 4)
	p.reservedReclaimedCPUsSize = 2
	dynamicConf := p.dynamicConfig.GetDynamicConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.EnableRampUpReclaimHardPartition = true
	dynamicConf.InitialRampUpReclaimCPUSetRatio = 0.5
	dynamicConf.NumaMinReclaimedResourceForAllocate = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("2"),
	}
	dynamicConf.DisableReclaimPinnedCPUSetResourcePackageSelector = "disable-reclaim=true"
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)

	protected := coresInNUMA(topology, 1, 1, 2)
	machineState := state.NUMANodeMap{
		0: {DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(0)},
		1: {
			DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(1),
			ResourcePackageStates: map[string]*state.ResourcePackageState{
				"protected": {
					Attributes:   map[string]string{"disable-reclaim": "true"},
					PinnedCPUSet: protected,
				},
			},
		},
	}
	p.state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &state.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: p.reservedReclaimedCPUSet.Union(protected),
	}, false)
	annotations := map[string]string{
		apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
	}

	result, reclaim, err := p.allocateNumaBindingCPUs(
		8, &pluginapi.TopologyHint{Nodes: []uint64{0}}, machineState, annotations, false)
	require.NoError(t, err)
	require.Equal(t, 6, result.Size(), "result=%s", result)
	require.Equal(t, 2, reclaim.Intersection(topology.CPUDetails.CPUsInNUMANodes(0)).Size(), "reclaim=%s", reclaim)
	require.Equal(t, 2, reclaim.Intersection(topology.CPUDetails.CPUsInNUMANodes(1)).Size(), "reclaim=%s", reclaim)
	require.True(t, p.reservedReclaimedCPUSet.IsSubsetOf(reclaim), "reserve=%s reclaim=%s", p.reservedReclaimedCPUSet, reclaim)
	require.True(t, reclaim.Intersection(protected).IsEmpty(), "protected=%s reclaim=%s", protected, reclaim)
	requireCoreAligned(t, topology, reclaim)
}

func TestDynamicPolicy_deriveSteadyReclaimFloorRejectsIdentitiesAboveConfiguredTarget(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().NumaMinReclaimedResourceForAllocate = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("2"),
	}
	p.reservedReclaimedCPUSet = machine.NewCPUSet(0, 1)
	p.reservedReclaimedCPUsSize = p.reservedReclaimedCPUSet.Size()

	got, err := p.deriveSteadyReclaimFloor(map[int]machine.CPUSet{
		0: topology.CPUDetails.CPUsInNUMANodes(0),
	})
	require.ErrorContains(t, err, "mandatory reserve size 4 exceeds steady target 2")
	require.True(t, got.IsEmpty(), "got=%s", got)
}

func TestDynamicPolicy_allocateNumaBindingCPUs_preservesMandatoryOnPodLookupFailure(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		reserved machine.CPUSet
		ratio    float64
		wantSize int
	}{
		{name: "pod lookup failure preserves reserve", reserved: machine.NewCPUSet(0, 4), ratio: 0, wantSize: 2},
		{name: "identity-less checkpoint uses configured steady floor", reserved: machine.NewCPUSet(), ratio: 0.25, wantSize: 2},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
			require.NoError(t, err)
			p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
			require.NoError(t, err)
			p.reservedCPUs = machine.NewCPUSet()
			p.reservedReclaimedCPUSet = tc.reserved
			p.reservedReclaimedCPUsSize = tc.reserved.Size()
			p.conf.SetDynamicConfiguration(nil)
			p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
			p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = tc.ratio
			p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
			p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
			machineState := state.NUMANodeMap{
				0: {DefaultCPUSet: topology.CPUDetails.CPUsInNUMANodes(0)},
			}
			annotations := map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
			}

			podReclaimEnabled := false
			result, reclaim, err := p.allocateNumaBindingCPUs(
				8, &pluginapi.TopologyHint{Nodes: []uint64{0}}, machineState, annotations, podReclaimEnabled)
			require.NoError(t, err)
			require.Equal(t, tc.wantSize, reclaim.Size(), "reclaim=%s", reclaim)
			require.Equal(t, 8-tc.wantSize, result.Size(), "result=%s", result)
			require.True(t, result.Union(reclaim).Equals(machineState[0].DefaultCPUSet))
		})
	}
}

func TestDynamicPolicy_selectNumaBindingReclaimPartitionPreservesSelectedFloor(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.reservedReclaimedCPUSet = coresInNUMA(topology, 0, 0, 1)
	p.reservedReclaimedCPUsSize = p.reservedReclaimedCPUSet.Size()

	derivedFloor := coresInNUMA(topology, 0, 0, 2)
	dedicatedEligiblePerNUMA := map[int]machine.CPUSet{
		0: topology.CPUDetails.CPUsInNUMANodes(0),
	}
	reclaimEligiblePerNUMA := map[int]machine.CPUSet{
		0: derivedFloor,
	}

	got, err := p.selectNumaBindingReclaimPartition(
		derivedFloor,
		dedicatedEligiblePerNUMA,
		reclaimEligiblePerNUMA,
		[]uint64{0},
		true,
	)
	require.NoError(t, err)
	require.True(t, derivedFloor.Equals(got), "want=%s got=%s", derivedFloor.String(), got.String())
}

func TestDynamicPolicy_selectNumaBindingReclaimPartitionPreservesMandatoryIdentities(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true

	derivedFloor := coresInNUMA(topology, 0, 0, 2)
	for _, tc := range []struct {
		name         string
		mandatory    machine.CPUSet
		derivedFloor machine.CPUSet
		wantSize     int
		wantErr      string
	}{
		{
			name:         "supplements mandatory to selected floor",
			mandatory:    coresInNUMA(topology, 0, 3, 4),
			derivedFloor: derivedFloor,
			wantSize:     derivedFloor.Size(),
		},
		{
			name:         "mandatory identities exceed selected floor",
			mandatory:    coresInNUMA(topology, 0, 2, 4),
			derivedFloor: coresInNUMA(topology, 0, 0, 1),
			wantErr:      "mandatory reclaim CPUs exceed selected floor",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			p.reservedReclaimedCPUSet = tc.mandatory
			p.reservedReclaimedCPUsSize = tc.mandatory.Size()
			got, err := p.selectNumaBindingReclaimPartition(
				tc.derivedFloor,
				map[int]machine.CPUSet{
					0: topology.CPUDetails.CPUsInNUMANodes(0),
				},
				map[int]machine.CPUSet{
					0: tc.derivedFloor.Union(tc.mandatory),
				},
				[]uint64{0},
				true,
			)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.True(t, got.IsEmpty(), "got=%s", got)
				return
			}
			require.NoError(t, err)
			require.True(t, tc.mandatory.IsSubsetOf(got), "mandatory=%s got=%s", tc.mandatory, got)
			require.Equal(t, tc.wantSize, got.Size(), "got=%s", got)
		})
	}
}

func TestDynamicPolicy_selectNumaBindingReclaimPartitionRejectsIneligibleMandatoryIdentity(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.reservedReclaimedCPUSet = machine.NewCPUSet(7)
	p.reservedReclaimedCPUsSize = 1

	got, err := p.selectNumaBindingReclaimPartition(
		machine.NewCPUSet(0, 1),
		map[int]machine.CPUSet{0: machine.NewCPUSet(0, 1, 2, 3)},
		map[int]machine.CPUSet{0: machine.NewCPUSet(0, 1, 2, 3)},
		[]uint64{0},
		true,
	)
	require.ErrorContains(t, err, "not a subset of reclaim eligibility")
	require.True(t, got.IsEmpty(), "got=%s", got)
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

func TestDynamicPolicyDoAndCheckPutAllocationInfoReportsMissingAllocationInfo(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)

	allocationInfo := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "missing-after-put",
			PodNamespace:  "default",
			PodName:       "missing-after-put",
			ContainerName: "main",
			Annotations: map[string]string{
				apiconsts.PodAnnotationQoSLevelKey:          apiconsts.PodAnnotationQoSLevelSharedCores,
				apiconsts.PodAnnotationCPUEnhancementCPUSet: "seedpool-missing-after-put",
			},
			Labels: map[string]string{
				apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
			},
			QoSLevel: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		AllocationResult: machine.NewCPUSet(1, 2, 3),
		TopologyAwareAssignments: map[int]machine.CPUSet{
			0: machine.NewCPUSet(1, 2),
			1: machine.NewCPUSet(3),
		},
		RequestQuantity: 1,
	}
	allocationInfo.OriginalAllocationResult = allocationInfo.AllocationResult.Clone()
	allocationInfo.OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(allocationInfo.TopologyAwareAssignments)

	p.state.SetAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, allocationInfo, false)
	p.state = &missingAllocationInfoState{
		State:                p.state,
		missingPodUID:        allocationInfo.PodUid,
		missingContainerName: allocationInfo.ContainerName,
	}

	_, err = p.doAndCheckPutAllocationInfoPodResizingAware(nil, allocationInfo, true, false, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "allocationInfo missing after putAllocationsAndAdjustAllocationEntries")
	require.NotContains(t, err.Error(), "<nil>")
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
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
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

	committedEntries := p.state.GetPodEntries()
	floor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), committedEntries, true)
	require.NoError(t, err)
	require.True(t, floor.Equals(machine.NewCPUSet(14, 38, 62, 86)),
		"floor=%s, want all-NUMA reserved reclaim CPUs", floor)
	require.Equal(t, 2, floor.Intersection(p.machineInfo.CPUDetails.CPUsInNUMANodes(0)).Size())
	require.Equal(t, 2, floor.Intersection(p.machineInfo.CPUDetails.CPUsInNUMANodes(1)).Size())

	inactiveFloor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), committedEntries, false)
	require.NoError(t, err)
	require.True(t, inactiveFloor.IsEmpty(), "inactive candidate must not receive a temporary hard floor")

	activeCandidate := committedEntries.Clone()
	activeCandidate["ramp-up-pod"] = state.ContainerEntries{"main": &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid: "ramp-up-pod", ContainerName: "main",
		},
		AllocationResult: machine.NewCPUSet(1),
		RampUp:           true,
	}}
	activeFloor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), activeCandidate, false)
	require.NoError(t, err)
	require.True(t, activeFloor.Equals(machine.NewCPUSet(14, 38, 62, 86)))

	p.state.SetPodEntries(activeCandidate, false)
	finalExitCandidate := committedEntries.Clone()
	finalExitFloor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), finalExitCandidate, false)
	require.NoError(t, err)
	require.True(t, finalExitFloor.IsEmpty(), "candidate final exit must override committed active state")
}

func TestApplyPoolsAndIsolatedInfoAddsExplicitHardFloorToReclaim(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)

	numa0 := topology.CPUDetails.CPUsInNUMANodes(0).ToSliceInt()
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1).ToSliceInt()
	require.GreaterOrEqual(t, len(numa0), 8)
	require.GreaterOrEqual(t, len(numa1), 8)

	floor := machine.NewCPUSet(numa0[0], numa0[1], numa1[0], numa1[1])
	numa1Floor := machine.NewCPUSet(numa1[0], numa1[1])
	shareBefore := numa1Floor.Union(machine.NewCPUSet(numa0[2], numa1[2]))
	isolationBefore := numa1Floor.Union(machine.NewCPUSet(numa0[3], numa1[3]))
	customBefore := numa1Floor.Union(machine.NewCPUSet(numa0[4], numa1[4]))
	reserveBefore := machine.NewCPUSet(numa0[5], numa1[5])
	dedicatedBefore := machine.NewCPUSet(numa0[6], numa1[6])
	reclaimBefore := machine.NewCPUSet(numa0[7], numa1[7])

	poolsCPUSet := map[string]machine.CPUSet{
		commonstate.PoolNameShare:     shareBefore.Clone(),
		"isolation-regression":        isolationBefore.Clone(),
		"custom-regression":           customBefore.Clone(),
		commonstate.PoolNameReserve:   reserveBefore.Clone(),
		commonstate.PoolNameDedicated: dedicatedBefore.Clone(),
		commonstate.PoolNameReclaim:   reclaimBefore.Clone(),
	}
	curEntries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(),
			},
		},
	}

	err = p.applyPoolsAndIsolatedInfo(
		poolsCPUSet,
		map[string]map[string]machine.CPUSet{},
		curEntries,
		p.state.GetMachineState(),
		sets.NewInt(),
		false,
		floor,
		defaultShareMaterializationPlan{},
		p.state.GetRevision(),
	)
	require.NoError(t, err)

	entries := p.state.GetPodEntries()
	reclaim := entries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].AllocationResult
	require.True(t, reclaim.Equals(reclaimBefore.Union(floor)), "reclaim=%s floor=%s", reclaim, floor)

	require.True(t, entries[commonstate.PoolNameShare][commonstate.FakedContainerName].AllocationResult.Equals(shareBefore))
	require.True(t, entries["isolation-regression"][commonstate.FakedContainerName].AllocationResult.Equals(isolationBefore))
	require.True(t, entries["custom-regression"][commonstate.FakedContainerName].AllocationResult.Equals(customBefore))
	require.True(t, entries[commonstate.PoolNameReserve][commonstate.FakedContainerName].AllocationResult.Equals(reserveBefore))
	require.True(t, entries[commonstate.PoolNameDedicated][commonstate.FakedContainerName].AllocationResult.Equals(dedicatedBefore))
}

func TestApplyPoolsAndIsolatedInfoUsesSingleRevisionGuardedCommit(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)

	expectedRevision := p.state.GetRevision()
	guard := &applyPoolsCommitGuardState{State: p.state}
	p.state = guard

	err = applyPoolsAndIsolatedInfoForCommitTest(p, true)
	require.NoError(t, err)
	require.Equal(t, 1, guard.conditionalCalls)
	require.Equal(t, expectedRevision, guard.conditionalRevision)
	require.False(t, guard.conditionalAllow)
	require.True(t, guard.conditionalDisable)
	require.True(t, guard.conditionalPersist)
	require.Zero(t, guard.setPodEntriesCalls)
	require.Zero(t, guard.setMachineStateCalls)
	require.Zero(t, guard.storeCalls)
}

func TestApplyPoolsAndIsolatedInfoReturnsStaleRevisionError(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()

	beforeEntries := p.state.GetPodEntries()
	guard := &applyPoolsCommitGuardState{
		State:                 p.state,
		injectRevisionAdvance: true,
	}
	p.state = guard

	err = applyPoolsAndIsolatedInfoForCommitTest(p, false)
	require.ErrorIs(t, err, state.ErrStaleStateRevision)
	require.Equal(t, 1, guard.conditionalCalls)
	require.Zero(t, guard.setPodEntriesCalls)
	require.Zero(t, guard.setMachineStateCalls)
	require.Zero(t, guard.storeCalls)
	require.True(t, reflect.DeepEqual(beforeEntries, p.state.GetPodEntries()),
		"stale advisor result must not replace newer state")
}

func applyPoolsAndIsolatedInfoForCommitTest(p *DynamicPolicy, persist bool) error {
	topology := p.machineInfo.CPUTopology
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0).ToSliceInt()
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1).ToSliceInt()
	poolsCPUSet := map[string]machine.CPUSet{
		commonstate.PoolNameShare:     machine.NewCPUSet(numa0[0], numa1[0]),
		"isolation-commit-test":       machine.NewCPUSet(numa0[1], numa1[1]),
		"custom-commit-test":          machine.NewCPUSet(numa0[2], numa1[2]),
		commonstate.PoolNameReserve:   machine.NewCPUSet(numa0[3], numa1[3]),
		commonstate.PoolNameDedicated: machine.NewCPUSet(numa0[4], numa1[4]),
		commonstate.PoolNameReclaim:   machine.NewCPUSet(numa0[5], numa1[5]),
	}
	curEntries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(),
			},
		},
	}
	return p.applyPoolsAndIsolatedInfo(
		poolsCPUSet,
		map[string]map[string]machine.CPUSet{},
		curEntries,
		p.state.GetMachineState(),
		sets.NewInt(),
		persist,
		machine.NewCPUSet(),
		defaultShareMaterializationPlan{},
		p.state.GetRevision(),
	)
}

func TestAdjustPoolsAndIsolatedEntriesWithRampUpFloorAllowsNonBindingSharedPoolShrink(t *testing.T) {
	t.Parallel()

	newPolicy := func(t *testing.T) *DynamicPolicy {
		t.Helper()

		topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
		require.NoError(t, err)
		p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
		require.NoError(t, err)
		p.reservedCPUs = machine.NewCPUSet()
		p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
		p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
		p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)
		p.state.SetAllocationInfo("owned-share-pod", "main", &state.AllocationInfo{
			AllocationMeta: commonstate.AllocationMeta{
				PodUid:        "owned-share-pod",
				PodNamespace:  "default",
				PodName:       "owned-share-pod",
				ContainerName: "main",
				OwnerPoolName: commonstate.PoolNameShare,
				QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			},
			AllocationResult: machine.NewCPUSet(0, 1, 2, 3),
			RequestQuantity:  4,
		}, false)
		return p
	}

	t.Run("allocates the requested quantity outside the explicit floor", func(t *testing.T) {
		p := newPolicy(t)
		quantities := map[string]map[int]int{
			commonstate.PoolNameShare: {commonstate.FakedNUMAID: 4},
		}
		expectedQuantities := map[string]map[int]int{
			commonstate.PoolNameShare: {commonstate.FakedNUMAID: 4},
		}
		allCPUs := p.machineInfo.CPUDetails.CPUs()
		unreserved, _, err := p.groupAndAllocatePools(
			quantities, nil, allCPUs, nil, map[string]float64{})
		require.NoError(t, err)
		floor := unreserved[commonstate.PoolNameShare].Clone()
		require.Equal(t, 4, floor.Size())

		err = p.adjustPoolsAndIsolatedEntriesWithRampUpFloor(
			quantities,
			nil,
			p.state.GetPodEntries(),
			p.state.GetMachineState(),
			false,
			floor,
			false,
		)
		require.NoError(t, err)
		require.Equal(t, expectedQuantities, quantities)

		share, err := p.state.GetPodEntries().GetCPUSetForPool(commonstate.PoolNameShare)
		require.NoError(t, err)
		require.Equal(t, 4, share.Size())
		require.True(t, share.Intersection(floor).IsEmpty(), "share=%s floor=%s", share, floor)
		owner := p.state.GetAllocationInfo("owned-share-pod", "main")
		require.NotNil(t, owner)
		require.Equal(t, 4, owner.AllocationResult.Size())
		require.True(t, owner.AllocationResult.Equals(share))
	})

	t.Run("does not derive hard floor without active ramp-up allocation", func(t *testing.T) {
		p := newPolicy(t)
		p.reservedReclaimedCPUSet = machine.NewCPUSet(0, 1)
		p.reservedReclaimedCPUsSize = 2
		quantities := map[string]map[int]int{
			commonstate.PoolNameShare: {commonstate.FakedNUMAID: 4},
		}

		err := p.adjustPoolsAndIsolatedEntriesWithRampUpFloor(
			quantities,
			nil,
			p.state.GetPodEntries(),
			p.state.GetMachineState(),
			false,
			machine.NewCPUSet(),
			false,
		)
		require.NoError(t, err)

		entries := p.state.GetPodEntries()
		reclaim, err := entries.GetCPUSetForPool(commonstate.PoolNameReclaim)
		require.NoError(t, err)
		require.False(t, machine.NewCPUSet(0, 1).IsSubsetOf(reclaim), "reclaim=%s", reclaim)
		share, err := entries.GetCPUSetForPool(commonstate.PoolNameShare)
		require.NoError(t, err)
		require.Equal(t, 4, share.Size())
	})

	t.Run("proportionally shrinks global share pool", func(t *testing.T) {
		p := newPolicy(t)
		quantities := map[string]map[int]int{
			commonstate.PoolNameShare: {commonstate.FakedNUMAID: 4},
		}
		expectedQuantities := map[string]map[int]int{
			commonstate.PoolNameShare: {commonstate.FakedNUMAID: 4},
		}
		cpus := p.machineInfo.CPUDetails.CPUs().ToSliceInt()
		require.Len(t, cpus, 8)
		floor := machine.NewCPUSet(cpus[:5]...)

		err := p.adjustPoolsAndIsolatedEntriesWithRampUpFloor(
			quantities,
			nil,
			p.state.GetPodEntries(),
			p.state.GetMachineState(),
			false,
			floor,
			false,
		)
		require.NoError(t, err)
		require.Equal(t, expectedQuantities, quantities)
		share, err := p.state.GetPodEntries().GetCPUSetForPool(commonstate.PoolNameShare)
		require.NoError(t, err)
		require.Equal(t, 3, share.Size())
		owner := p.state.GetAllocationInfo("owned-share-pod", "main")
		require.NotNil(t, owner)
		require.True(t, owner.AllocationResult.Equals(share))
	})
}

func TestAdjustPoolsAndIsolatedEntriesWithRampUpFloorRejectsPinnedSNBPoolShrinkAtomically(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)

	const resourcePackageName = "pinned-package"
	numaCPUs := topology.CPUDetails.CPUsInNUMANodes(0).ToSliceInt()
	require.Len(t, numaCPUs, 8)
	pinnedCPUs := machine.NewCPUSet(numaCPUs[:4]...)
	machineState := p.state.GetMachineState()
	machineState[0].ResourcePackageStates = map[string]*state.ResourcePackageState{
		resourcePackageName: {PinnedCPUSet: pinnedCPUs},
	}
	p.state.SetMachineState(machineState, false)

	allocation := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "pinned-snb-ramp-up",
			PodNamespace:  "default",
			PodName:       "pinned-snb-ramp-up",
			ContainerName: "main",
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				apiconsts.PodAnnotationResourcePackageKey:           resourcePackageName,
			},
		},
		RampUp:          true,
		RequestQuantity: 2,
	}
	allocation.SetSpecifiedNUMABindingNUMAID([]uint64{0})
	p.state.SetAllocationInfo(allocation.PodUid, allocation.ContainerName, allocation, false)

	quantities := make(map[string]map[int]int)
	require.NoError(t, state.CountAllocationInfosToPoolsQuantityMap(
		machineState.GetNUMAResourcePackagePinnedCPUSet(),
		[]*state.AllocationInfo{allocation},
		quantities,
		p.getContainerRequestedCores,
	))
	specifiedPoolName, err := allocation.GetSpecifiedNUMABindingPoolName()
	require.NoError(t, err)
	pinnedPoolName := rputil.WrapOwnerPoolName(specifiedPoolName, resourcePackageName)
	require.Equal(t, map[string]map[int]int{
		pinnedPoolName: {0: 4},
	}, quantities)

	alreadyWrapped := allocation.Clone()
	alreadyWrapped.OwnerPoolName = pinnedPoolName
	alreadyWrapped.AllocationResult = pinnedCPUs.Clone()
	alreadyWrapped.TopologyAwareAssignments = map[int]machine.CPUSet{0: pinnedCPUs.Clone()}
	alreadyWrappedQuantities := make(map[string]map[int]int)
	require.NoError(t, state.CountAllocationInfosToPoolsQuantityMap(
		machineState.GetNUMAResourcePackagePinnedCPUSet(),
		[]*state.AllocationInfo{alreadyWrapped},
		alreadyWrappedQuantities,
		p.getContainerRequestedCores,
	))
	require.Equal(t, map[string]map[int]int{
		pinnedPoolName: {0: 4},
	}, alreadyWrappedQuantities)

	initialEntries := p.state.GetPodEntries()
	initialMachineState := p.state.GetMachineState()
	initialRevision := p.state.GetRevision()
	floor := machine.NewCPUSet(numaCPUs[:2]...)

	err = p.adjustPoolsAndIsolatedEntriesWithRampUpFloor(
		quantities,
		nil,
		initialEntries,
		initialMachineState,
		false,
		floor,
		false,
	)
	require.ErrorContains(t, err,
		`insufficient capacity for owned pool "`+strings.ToLower(pinnedPoolName)+`" in numa 0: requested 4 cpus, allocated 2`)
	require.Equal(t, strings.ToLower(err.Error()), err.Error())
	require.Equal(t, initialEntries, p.state.GetPodEntries())
	require.Equal(t, initialMachineState, p.state.GetMachineState())
	require.Equal(t, initialRevision, p.state.GetRevision())
}

func TestAdjustPoolsAndIsolatedEntriesWithRampUpFloorRejectsBareOwnedPinnedSNBPoolShrinkAtomically(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)

	const resourcePackageName = "pinned-package"
	numaCPUs := topology.CPUDetails.CPUsInNUMANodes(0).ToSliceInt()
	require.Len(t, numaCPUs, 8)
	pinnedCPUs := machine.NewCPUSet(numaCPUs[:4]...)
	machineState := p.state.GetMachineState()
	machineState[0].ResourcePackageStates = map[string]*state.ResourcePackageState{
		resourcePackageName: {PinnedCPUSet: pinnedCPUs},
	}
	p.state.SetMachineState(machineState, false)

	allocation := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "bare-owned-pinned-snb-ramp-up",
			PodNamespace:  "default",
			PodName:       "bare-owned-pinned-snb-ramp-up",
			ContainerName: "main",
			OwnerPoolName: "share-NUMA0",
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				apiconsts.PodAnnotationResourcePackageKey:           resourcePackageName,
			},
		},
		RampUp:                   true,
		RequestQuantity:          2,
		AllocationResult:         pinnedCPUs.Clone(),
		TopologyAwareAssignments: map[int]machine.CPUSet{0: pinnedCPUs.Clone()},
	}
	allocation.SetSpecifiedNUMABindingNUMAID([]uint64{0})
	p.state.SetAllocationInfo(allocation.PodUid, allocation.ContainerName, allocation, false)

	quantities := make(map[string]map[int]int)
	require.NoError(t, state.CountAllocationInfosToPoolsQuantityMap(
		machineState.GetNUMAResourcePackagePinnedCPUSet(),
		[]*state.AllocationInfo{allocation},
		quantities,
		p.getContainerRequestedCores,
	))
	pinnedPoolName := rputil.WrapOwnerPoolName(allocation.OwnerPoolName, resourcePackageName)
	require.Equal(t, map[string]map[int]int{
		pinnedPoolName: {0: 4},
	}, quantities)

	initialEntries := p.state.GetPodEntries()
	initialMachineState := p.state.GetMachineState()
	initialRevision := p.state.GetRevision()
	floor := machine.NewCPUSet(numaCPUs[:2]...)

	err = p.adjustPoolsAndIsolatedEntriesWithRampUpFloor(
		quantities,
		nil,
		initialEntries,
		initialMachineState,
		false,
		floor,
		false,
	)
	assert.ErrorContains(t, err,
		`insufficient capacity for owned pool "`+strings.ToLower(pinnedPoolName)+`" in numa 0: requested 4 cpus, allocated 2`)
	assert.Equal(t, initialEntries, p.state.GetPodEntries())
	assert.Equal(t, initialMachineState, p.state.GetMachineState())
	assert.Equal(t, initialRevision, p.state.GetRevision())
}

func TestValidateOwnedPoolsQuantityRejectsMalformedSharedNUMABindingEntriesAtomically(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		assignments map[int]machine.CPUSet
	}{
		{
			name:        "empty topology assignments",
			assignments: nil,
		},
		{
			name: "cross numa topology assignments",
			assignments: map[int]machine.CPUSet{
				0: machine.NewCPUSet(0),
				1: machine.NewCPUSet(4),
			},
		},
		{
			name: "invalid topology assignments",
			assignments: map[int]machine.CPUSet{
				-1: machine.NewCPUSet(0),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
			require.NoError(t, err)
			p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
			require.NoError(t, err)

			allocation := &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "malformed-snb",
					PodNamespace:  "default",
					PodName:       "malformed-snb",
					ContainerName: "main",
					OwnerPoolName: "share-NUMA0",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					Annotations: map[string]string{
						apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					},
				},
				AllocationResult:         machine.NewCPUSet(0),
				TopologyAwareAssignments: tt.assignments,
			}
			p.state.SetAllocationInfo(allocation.PodUid, allocation.ContainerName, allocation, false)

			initialEntries := p.state.GetPodEntries()
			initialMachineState := p.state.GetMachineState()
			initialRevision := p.state.GetRevision()
			err = p.validateOwnedPoolsQuantity(
				map[string]map[int]int{allocation.OwnerPoolName: {0: 1}},
				map[string]machine.CPUSet{allocation.OwnerPoolName: machine.NewCPUSet(0)},
				initialEntries,
				initialMachineState.GetNUMAResourcePackagePinnedCPUSet(),
			)

			require.ErrorContains(t, err,
				"get canonical shared numa-binding pool key for default/malformed-snb/main failed")
			require.Equal(t, strings.ToLower(err.Error()), err.Error())
			require.Equal(t, initialEntries, p.state.GetPodEntries())
			require.Equal(t, initialMachineState, p.state.GetMachineState())
			require.Equal(t, initialRevision, p.state.GetRevision())
		})
	}
}

func TestAdjustAllocationEntriesWithRampUpFloorKeepsCanonicalSNBCapacityErrorLowercase(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.5
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)
	p.state.SetAllocationInfo("owned-snb-pod", "main", &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "owned-snb-pod",
			PodNamespace:  "default",
			PodName:       "owned-snb-pod",
			ContainerName: "main",
			ContainerType: pluginapi.ContainerType_MAIN.String(),
			OwnerPoolName: "share-NUMA0",
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			Annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			},
		},
		AllocationResult:         machine.NewCPUSet(0, 1, 2, 3),
		TopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(0, 1, 2, 3)},
		RequestQuantity:          4,
	}, false)

	req := &pluginapi.ResourceRequest{
		PodUid:         "exclusive-dnb-hard-floor-capacity",
		PodNamespace:   "default",
		PodName:        "exclusive-dnb-hard-floor-capacity",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 4,
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

	_, err = p.dedicatedCoresWithNUMABindingAllocationHandler(withAllocationPodMeta(context.Background(), req), req, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "adjustallocationentries failed with error: adjustpoolsandisolatedentries failed with error: insufficient capacity")
	require.Equal(t, strings.ToLower(err.Error()), err.Error())
}

func TestAllocateSharedNUMABindingRampUpRejectsLateHardFloorAtomically(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0).ToSliceInt()
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1).ToSliceInt()
	eligible := machine.NewCPUSet(append(numa0[:2], numa1[:2]...)...)
	p.reservedCPUs = topology.CPUDetails.CPUs().Difference(eligible)
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)
	p.podAnnotationKeptKeys = []string{apiconsts.PodAnnotationMemoryEnhancementNumaBinding}

	initialEntries := p.state.GetPodEntries()
	initialMachineState := p.state.GetMachineState()
	initialRevision := p.state.GetRevision()
	req := &pluginapi.ResourceRequest{
		PodUid:         "snb-late-hard-floor",
		PodNamespace:   "default",
		PodName:        "snb-late-hard-floor",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                  apiconsts.PodAnnotationQoSLevelSharedCores,
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{1}},
	}

	resp, err := p.Allocate(context.Background(), req)
	require.Nil(t, resp)
	require.ErrorContains(t, err,
		`insufficient capacity for owned pool "share-numa1" in numa 1: requested 4 cpus, allocated 0`)
	require.Equal(t, strings.ToLower(err.Error()), err.Error())
	require.Equal(t, initialEntries, p.state.GetPodEntries())
	require.Equal(t, initialMachineState, p.state.GetMachineState())
	require.Equal(t, initialRevision, p.state.GetRevision())
}

func TestAllocateSharedNUMABindingWithoutHardPartitionUsesAtomicPoolCommitAndPreservesHookOrder(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = false
	p.dynamicConfig.GetDynamicConfiguration().DisableSharedCoresRampUp = false

	req := &pluginapi.ResourceRequest{
		PodUid:         "snb-soft-partition",
		PodNamespace:   "default",
		PodName:        "snb-soft-partition",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                  apiconsts.PodAnnotationQoSLevelSharedCores,
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
		},
		Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
	}

	initialRevision := p.state.GetRevision()
	var hookRevisions []uint64
	var hookAllocationSizes [][2]int
	p.RegisterAllocationHook(func(oldInfo, newInfo *state.AllocationInfo) error {
		oldSize := -1
		if oldInfo != nil {
			oldSize = oldInfo.AllocationResult.Size()
		}
		newSize := -1
		if newInfo != nil {
			newSize = newInfo.AllocationResult.Size()
		}
		hookRevisions = append(hookRevisions, p.state.GetRevision())
		hookAllocationSizes = append(hookAllocationSizes, [2]int{oldSize, newSize})
		return nil
	})

	allocation, err := p.allocateSharedNumaBindingCPUs(context.Background(), req, req.Hint, false)
	require.NoError(t, err)
	require.NotNil(t, allocation)
	require.Equal(t, initialRevision+4, p.state.GetRevision())
	require.Equal(t, []uint64{initialRevision, initialRevision + 1}, hookRevisions)
	require.Equal(t, [][2]int{{-1, 0}, {0, allocation.AllocationResult.Size()}}, hookAllocationSizes)
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
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 1

	floor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), p.state.GetPodEntries(), true)
	require.NoError(t, err)
	require.True(t, floor.Equals(machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7)),
		"floor=%s, want every eligible CPU", floor)
}

func TestDynamicPolicyDeriveRampUpReclaimFloorUsesImmutablePerNUMACapacity(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.2
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)

	numa0 := topology.CPUDetails.CPUsInNUMANodes(0)
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1)
	p.state.SetMachineState(state.NUMANodeMap{
		0: {DefaultCPUSet: machine.NewCPUSet(numa0.ToSliceInt()[:8]...)},
		1: {DefaultCPUSet: machine.NewCPUSet(numa1.ToSliceInt()[:8]...)},
	}, false)

	// cpusPerCore==2, 32 CPUs (16 cores) per NUMA. ratio 0.2 yields
	// floor(16*0.2)=3 complete cores => 6 CPUs per NUMA. the immutable
	// per-NUMA capacity drives the target regardless of the smaller live
	// DefaultCPUSet, and the result is always a whole-core multiple.
	floor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), p.state.GetPodEntries(), true)
	require.NoError(t, err)
	require.Equal(t, 6, floor.Intersection(numa0).Size())
	require.Equal(t, 6, floor.Intersection(numa1).Size())
}

func TestDynamicPolicyDeriveRampUpReclaimFloorSkipsSteadyExclusiveNUMA(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.2
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)

	numa0 := topology.CPUDetails.CPUsInNUMANodes(0).ToSliceInt()
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1)
	// NUMA 0 is fully owned by a committed steady (RampUp=false) non-reclaim
	// exclusive DNB pod: 30 dedicated CPUs leave only a 2-CPU steady reclaim
	// floor eligible, which is smaller than the ratio-derived immutable target
	// of 6. A later ramp-up workload on NUMA 1 must not re-impose that target on
	// NUMA 0, otherwise admission fails closed.
	steadyReclaimNUMA0 := machine.NewCPUSet(numa0[:2]...)
	dedicatedNUMA0 := machine.NewCPUSet(numa0[2:]...)
	p.state.SetMachineState(state.NUMANodeMap{
		0: {DefaultCPUSet: steadyReclaimNUMA0},
		1: {DefaultCPUSet: numa1},
	}, false)
	candidate := state.PodEntries{
		"steady-exclusive-dnb": state.ContainerEntries{
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "steady-exclusive-dnb",
					ContainerName: "main",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
					Annotations: map[string]string{
						apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
						apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
					},
				},
				RampUp:                   false,
				AllocationResult:         dedicatedNUMA0,
				TopologyAwareAssignments: map[int]machine.CPUSet{0: dedicatedNUMA0},
			},
		},
	}

	floor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), candidate, true)
	require.NoError(t, err)
	require.Equal(t, 0, floor.Intersection(topology.CPUDetails.CPUsInNUMANodes(0)).Size(),
		"steady exclusive NUMA must not receive a ramp-up immutable target, floor=%s", floor)
	require.Equal(t, 6, floor.Intersection(numa1).Size(), "floor=%s", floor)
}

func TestDynamicPolicyDeriveRampUpReclaimFloorPreservesLegacyOverlapAlgorithm(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	allCPUs := topology.CPUDetails.CPUs().ToSliceInt()
	p.reservedCPUs = machine.NewCPUSet(allCPUs[8:]...)
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.5
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(false, false)

	floor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), p.state.GetPodEntries(), true)
	require.NoError(t, err)
	require.Equal(t, 4, floor.Size())
}

func TestDynamicPolicyDeriveRampUpReclaimFloorUsesDynamicConfiguredMinimum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		dynamicFloor  *v1.ResourceList
		wantFloorSize int
	}{
		{
			name: "dynamic floor overrides initialized fallback",
			dynamicFloor: &v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("8"),
			},
			wantFloorSize: 8,
		},
		{
			name: "odd dynamic floor is preserved",
			dynamicFloor: &v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("5"),
			},
			wantFloorSize: 5,
		},
		{
			name:          "nil dynamic configuration falls back",
			dynamicFloor:  nil,
			wantFloorSize: 4,
		},
		{
			name:          "missing CPU key falls back",
			dynamicFloor:  &v1.ResourceList{},
			wantFloorSize: 4,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
			require.NoError(t, err)
			p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
			require.NoError(t, err)

			p.reservedCPUs = machine.NewCPUSet()
			p.reservedReclaimedCPUSet = machine.NewCPUSet()
			p.reservedReclaimedCPUsSize = 4
			p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
			p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
			p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0
			if tt.dynamicFloor == nil {
				p.conf.SetDynamicConfiguration(nil)
			} else {
				p.conf.GetDynamicConfiguration().MinReclaimedResourceForAllocate = *tt.dynamicFloor
			}

			floor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), p.state.GetPodEntries(), true)
			require.NoError(t, err)
			require.Equal(t, tt.wantFloorSize, floor.Size())
			require.Equal(t, tt.wantFloorSize, machine.CalculateGlobalRampUpReclaimTarget(
				cpuTopology.NumCPUs, 0, tt.wantFloorSize),
				"QRM and Sysadvisor must derive the same global target")
		})
	}
}

func TestDynamicPolicyDeriveRampUpReclaimFloorBalancesGlobalTargetAcrossUnevenNUMAs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		hardEnabled       bool
		withReserved      bool
		configuredReserve int
		ratio             float64
		wantPerNUMA       map[int]int
		wantErr           string
	}{
		{
			name:              "half ratio uses immutable capacity",
			hardEnabled:       true,
			withReserved:      true,
			configuredReserve: 4,
			ratio:             0.5,
			wantErr:           "eligible capacity 4 is smaller than immutable target 8",
		},
		{
			name:              "fractional ratio is floored and aligned from immutable capacity",
			hardEnabled:       true,
			withReserved:      true,
			configuredReserve: 4,
			ratio:             0.5625,
			wantErr:           "eligible capacity 4 is smaller than immutable target 8",
		},
		{
			name:              "configured reserve floor wins",
			hardEnabled:       true,
			withReserved:      true,
			configuredReserve: 8,
			ratio:             0.25,
			wantPerNUMA:       map[int]int{0: 4, 1: 4},
		},
		{
			name:              "three quarter ratio exceeds current eligible capacity",
			hardEnabled:       true,
			withReserved:      true,
			configuredReserve: 4,
			ratio:             0.75,
			wantErr:           "eligible capacity 4 is smaller than immutable target 12",
		},
		{
			name:        "zero ratio still keeps two per NUMA",
			hardEnabled: true,
			ratio:       0,
			wantPerNUMA: map[int]int{0: 2, 1: 2},
		},
		{
			name:         "disabled hard partition remains empty",
			hardEnabled:  false,
			withReserved: true,
			ratio:        0.75,
			wantPerNUMA:  map[int]int{0: 0, 1: 0},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cpuTopology, err := machine.GenerateDummyCPUTopology(32, 2, 2)
			require.NoError(t, err)
			p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
			require.NoError(t, err)

			numa0CPUs := p.machineInfo.CPUDetails.CPUsInNUMANodes(0).ToSliceInt()
			numa1CPUs := p.machineInfo.CPUDetails.CPUsInNUMANodes(1).ToSliceInt()
			eligible := machine.NewCPUSet(append(numa0CPUs[:4], numa1CPUs[:12]...)...)
			p.reservedCPUs = p.machineInfo.CPUDetails.CPUs().Difference(eligible)
			if tt.withReserved {
				p.reservedReclaimedCPUSet = machine.NewCPUSet(numa0CPUs[0], numa0CPUs[1], numa1CPUs[0], numa1CPUs[1])
				p.reservedReclaimedCPUsSize = tt.configuredReserve
			} else {
				p.reservedReclaimedCPUSet = machine.NewCPUSet()
				p.reservedReclaimedCPUsSize = 0
			}
			p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = tt.hardEnabled
			p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = tt.hardEnabled
			p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = tt.ratio
			p.state.SetDisableDedicatedCoresOverlapReclaimedCores(tt.hardEnabled, false)
			if tt.configuredReserve > 0 {
				p.conf.GetDynamicConfiguration().MinReclaimedResourceForAllocate = v1.ResourceList{
					v1.ResourceCPU: *resource.NewQuantity(int64(tt.configuredReserve), resource.DecimalSI),
				}
			}

			floor, err := p.deriveRampUpReclaimFloor(p.state.GetMachineState(), p.state.GetPodEntries(), true)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.hardEnabled && tt.withReserved {
				require.True(t, p.reservedReclaimedCPUSet.IsSubsetOf(floor), "reserved reclaim CPUs must remain preferred")
			}
			for numaID, want := range tt.wantPerNUMA {
				require.Equal(t, want, floor.Intersection(p.machineInfo.CPUDetails.CPUsInNUMANodes(numaID)).Size())
			}
		})
	}
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
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
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
	p.metaServer.MetaAgent.PodFetcher = &pod.PodFetcherStub{PodList: []*v1.Pod{{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(req.PodUid), Namespace: req.PodNamespace, Name: req.PodName},
	}}}

	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(withAllocationPodMeta(context.Background(), req), req, true)
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

func TestDedicatedNUMAExclusiveNonReclaimableStartsSteady(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet(0)
	p.reservedReclaimedCPUsSize = 1
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.2
	p.dynamicConfig.GetDynamicConfiguration().NumaMinReclaimedResourceForAllocate = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("2"),
	}
	p.dynamicConfig.GetDynamicConfiguration().AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable = true
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
	p.state.SetMachineState(state.NUMANodeMap{
		0: {DefaultCPUSet: cpuTopology.CPUDetails.CPUs()},
	}, false)
	p.metaServer.ServiceProfilingManager = &recordingServiceProfilingManager{
		DummyServiceProfilingManager: &spd.DummyServiceProfilingManager{},
		performanceLevel:             spd.PerformanceLevelPoor,
	}

	req := &pluginapi.ResourceRequest{
		PodUid:         "exclusive-dnb-steady",
		PodNamespace:   "default",
		PodName:        "exclusive-dnb-steady",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 30,
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
	candidateSeen := false
	p.allocationHooks = append(p.allocationHooks, func(_, allocation *state.AllocationInfo) error {
		if allocation.PodUid == req.PodUid && allocation.ContainerName == req.ContainerName {
			candidateSeen = true
			require.False(t, allocation.RampUp,
				"non-reclaimable exclusive DNB must enter precommit in steady state")
		}
		return nil
	})

	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(withAllocationPodMeta(context.Background(), req), req, false)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, candidateSeen, "allocation hook did not observe the precommit candidate")

	allocation := p.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	require.NotNil(t, allocation)
	require.False(t, allocation.RampUp)
	require.Equal(t, 30, allocation.AllocationResult.Size())
	reclaim := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.NotNil(t, reclaim)
	require.True(t, reclaim.AllocationResult.Equals(machine.NewCPUSet(0, 16)),
		"reclaim=%s", reclaim.AllocationResult)
}

func TestShouldNumaBindingAllocationRampUpUsesPartitionSnapshot(t *testing.T) {
	t.Parallel()

	hardPartition := &numaBindingPartitionEligibilitySnapshot{}
	for _, tc := range []struct {
		name              string
		podReclaimEnabled bool
		eligibility       *numaBindingPartitionEligibilitySnapshot
		want              bool
	}{
		{name: "reclaimable legacy allocation ramps up", podReclaimEnabled: true, want: true},
		{name: "reclaimable hard partition ramps up", podReclaimEnabled: true, eligibility: hardPartition, want: true},
		{name: "non-reclaimable legacy allocation ramps up", want: true},
		{name: "non-reclaimable hard partition starts steady", eligibility: hardPartition, want: false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldNumaBindingAllocationRampUp(tc.podReclaimEnabled, tc.eligibility))
		})
	}
}

func TestDedicatedNUMAExclusivePackResponseFailureKeepsAllStateUnchanged(t *testing.T) {
	p, req := newDedicatedNUMAExclusiveFailureFixture(t, "exclusive-dnb-pack-failure")
	policyTestMutex.Lock()
	defer policyTestMutex.Unlock()
	beforeEntries := p.state.GetPodEntries()
	beforeMachine := p.state.GetMachineState()
	beforeAllowOverlap := p.state.GetAllowSharedCoresOverlapReclaimedCores()
	beforeDisableDedicated := p.state.GetDisableDedicatedCoresOverlapReclaimedCores()
	beforeRevision := p.state.GetRevision()

	originalPack := packAllocationResponse
	packAllocationResponse = func(
		_ *state.AllocationInfo, _, _ string, _, _ bool, _ *pluginapi.ResourceRequest, _ ...map[string]string,
	) (*pluginapi.ResourceAllocationResponse, error) {
		return nil, errors.New("injected pack allocation response failure")
	}
	defer func() {
		packAllocationResponse = originalPack
	}()

	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(withAllocationPodMeta(context.Background(), req), req, true)
	require.Nil(t, resp)
	require.ErrorContains(t, err, "injected pack allocation response failure")
	require.Equal(t, beforeEntries, p.state.GetPodEntries())
	require.Equal(t, beforeMachine, p.state.GetMachineState())
	require.Equal(t, beforeAllowOverlap, p.state.GetAllowSharedCoresOverlapReclaimedCores())
	require.Equal(t, beforeDisableDedicated, p.state.GetDisableDedicatedCoresOverlapReclaimedCores())
	require.Equal(t, beforeRevision, p.state.GetRevision())
}

func TestDedicatedNUMAExclusiveAccompanyFailureKeepsAllStateUnchanged(t *testing.T) {
	p, req := newDedicatedNUMAExclusiveFailureFixture(t, "exclusive-dnb-accompany-failure")
	policyTestMutex.Lock()
	defer policyTestMutex.Unlock()
	beforeEntries := p.state.GetPodEntries()
	beforeMachine := p.state.GetMachineState()
	beforeAllowOverlap := p.state.GetAllowSharedCoresOverlapReclaimedCores()
	beforeDisableDedicated := p.state.GetDisableDedicatedCoresOverlapReclaimedCores()
	beforeRevision := p.state.GetRevision()

	originalRegistry := AccompanyResourceRegistry
	AccompanyResourceRegistry = accompanyresource.NewRegistry()
	require.NoError(t, AccompanyResourceRegistry.RegisterPlugin(&failingAllocateAccompanyPlugin{
		err: errors.New("injected accompany allocation failure"),
	}))
	defer func() {
		AccompanyResourceRegistry = originalRegistry
	}()

	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(withAllocationPodMeta(context.Background(), req), req, true)
	require.Nil(t, resp)
	require.ErrorContains(t, err, "injected accompany allocation failure")
	require.Equal(t, beforeEntries, p.state.GetPodEntries())
	require.Equal(t, beforeMachine, p.state.GetMachineState())
	require.Equal(t, beforeAllowOverlap, p.state.GetAllowSharedCoresOverlapReclaimedCores())
	require.Equal(t, beforeDisableDedicated, p.state.GetDisableDedicatedCoresOverlapReclaimedCores())
	require.Equal(t, beforeRevision, p.state.GetRevision())
}

func newDedicatedNUMAExclusiveFailureFixture(
	t *testing.T,
	podUID string,
) (*DynamicPolicy, *pluginapi.ResourceRequest) {
	t.Helper()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0.25

	req := &pluginapi.ResourceRequest{
		PodUid: podUID, PodNamespace: "default", PodName: podUID, ContainerName: "main",
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
	p.metaServer.MetaAgent.PodFetcher = &pod.PodFetcherStub{PodList: []*v1.Pod{{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(req.PodUid), Namespace: req.PodNamespace, Name: req.PodName,
		},
	}}}
	return p, req
}

func TestAllocateDedicatedNUMAExclusiveAdjustmentFailureDoesNotRollbackNewerState(t *testing.T) {
	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
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
	require.True(t, reclaim.AllocationResult.Intersection(machine.NewCPUSet(7)).IsEmpty(),
		"reclaim floor/pool overlaps newer dedicated allocation: %s", reclaim.AllocationResult)
	require.True(t, reclaim.AllocationResult.Intersection(failedCandidateCPUs).IsEmpty(),
		"reclaim floor/pool overlaps failed candidate allocation: %s", reclaim.AllocationResult)
}

func TestAllocateDedicatedNUMAExclusiveAdjustmentFailureReportsOwnershipLostAndReconcilesLatestState(t *testing.T) {
	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
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
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
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
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
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
	p.metaServer.MetaAgent.PodFetcher = &pod.PodFetcherStub{PodList: []*v1.Pod{{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(req.PodUid), Namespace: req.PodNamespace, Name: req.PodName},
	}}}

	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(withAllocationPodMeta(context.Background(), req), req, true)
	require.Nil(t, resp)
	require.ErrorContains(t, err, "injected atomic commit failure")
	require.Equal(t, 1, tracked.commitCalls)
	require.Zero(t, tracked.storeCalls)
	require.True(t, reflect.DeepEqual(beforeEntries, p.state.GetPodEntries()))
	require.True(t, reflect.DeepEqual(beforeMachine, p.state.GetMachineState()))
}

func TestDedicatedNUMAExclusiveRampUpKeepsMinimumReclaimFloor(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.reservedReclaimedCPUSet = machine.NewCPUSet()
	p.reservedReclaimedCPUsSize = 0
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
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
	p.metaServer.MetaAgent.PodFetcher = &pod.PodFetcherStub{PodList: []*v1.Pod{{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(req.PodUid), Namespace: req.PodNamespace, Name: req.PodName},
	}}}

	resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(withAllocationPodMeta(context.Background(), req), req, true)
	require.NoError(t, err)
	require.NotNil(t, resp)
	reclaimInfo := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.NotNil(t, reclaimInfo)
	require.GreaterOrEqual(t, reclaimInfo.AllocationResult.Size(), 2)
}

func TestDedicatedNUMAExclusiveRampUpValidatesPartitionEligibleCoverage(t *testing.T) {
	t.Parallel()

	newFixture := func(t *testing.T) (*DynamicPolicy, *pluginapi.ResourceRequest) {
		cpuTopology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
		require.NoError(t, err)
		p, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
		require.NoError(t, err)
		p.reservedCPUs = machine.NewCPUSet()
		p.reservedReclaimedCPUSet = coresInNUMA(cpuTopology, 0, 2, 3)
		p.reservedReclaimedCPUsSize = p.reservedReclaimedCPUSet.Size()
		p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
		p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
		p.dynamicConfig.GetDynamicConfiguration().InitialRampUpReclaimCPUSetRatio = 0
		p.dynamicConfig.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector = "disable-reclaim=true"
		p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
		p.state.SetMachineState(state.NUMANodeMap{
			0: {
				DefaultCPUSet: cpuTopology.CPUDetails.CPUsInNUMANodes(0),
				ResourcePackageStates: map[string]*state.ResourcePackageState{
					"work": {
						PinnedCPUSet: coresInNUMA(cpuTopology, 0, 0, 1),
					},
					"protected": {
						Attributes:   map[string]string{"disable-reclaim": "true"},
						PinnedCPUSet: coresInNUMA(cpuTopology, 0, 3, 4),
					},
				},
			},
		}, false)

		req := &pluginapi.ResourceRequest{
			PodUid: "exclusive-dnb-partition-coverage", PodNamespace: "default",
			PodName: "exclusive-dnb-partition-coverage", ContainerName: "main",
			ContainerType: pluginapi.ContainerType_MAIN, ResourceName: string(v1.ResourceCPU),
			ResourceRequests: map[string]float64{string(v1.ResourceCPU): 2},
			Labels: map[string]string{
				apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores,
			},
			Annotations: map[string]string{
				apiconsts.PodAnnotationQoSLevelKey:                    apiconsts.PodAnnotationQoSLevelDedicatedCores,
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
				apiconsts.PodAnnotationResourcePackageKey:             "work",
			},
			Hint: &pluginapi.TopologyHint{Nodes: []uint64{0}},
		}
		p.metaServer.MetaAgent.PodFetcher = &pod.PodFetcherStub{PodList: []*v1.Pod{{
			ObjectMeta: metav1.ObjectMeta{UID: types.UID(req.PodUid), Namespace: req.PodNamespace, Name: req.PodName},
		}}}
		return p, req
	}

	t.Run("disable-reclaim pinned CPU outside dedicated and reclaim eligibility is legal", func(t *testing.T) {
		p, req := newFixture(t)

		resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(withAllocationPodMeta(context.Background(), req), req, true)
		require.NoError(t, err)
		require.NotNil(t, resp)

		allocation := p.state.GetAllocationInfo(req.PodUid, req.ContainerName)
		reclaim := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
		require.NotNil(t, allocation)
		require.NotNil(t, reclaim)
		protected := coresInNUMA(p.machineInfo.CPUTopology, 0, 3, 4)
		admissionFloor := coresInNUMA(p.machineInfo.CPUTopology, 0, 1, 3)
		require.True(t, allocation.AllocationResult.Union(admissionFloor).
			Equals(p.machineInfo.CPUDetails.CPUs().Difference(protected)))
		require.True(t, allocation.AllocationResult.Intersection(protected).IsEmpty())
		require.True(t, admissionFloor.IsSubsetOf(reclaim.AllocationResult))
	})

	t.Run("incorrect dedicated and reclaim union is rejected", func(t *testing.T) {
		p, req := newFixture(t)
		p.state.SetAllocationInfo(commonstate.PoolNameInterrupt, commonstate.FakedContainerName, &state.AllocationInfo{
			AllocationMeta: commonstate.AllocationMeta{
				PodUid:        commonstate.PoolNameInterrupt,
				ContainerName: commonstate.FakedContainerName,
			},
			AllocationResult: coresInNUMA(p.machineInfo.CPUTopology, 0, 0, 1),
		}, false)

		resp, err := p.dedicatedCoresWithNUMABindingAllocationHandler(withAllocationPodMeta(context.Background(), req), req, true)
		require.Nil(t, resp)
		require.ErrorContains(t, err, "do not cover NUMA 0")
		require.Nil(t, p.state.GetAllocationInfo(req.PodUid, req.ContainerName))
	})
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
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
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

func TestMaterializeDefaultShareCPUSet(t *testing.T) {
	t.Parallel()

	available := machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7)
	pools := map[string]machine.CPUSet{
		commonstate.PoolNameReclaim: machine.NewCPUSet(0, 1),
		"custom":                    machine.NewCPUSet(2),
		"snb-NUMA0":                 machine.NewCPUSet(3),
	}
	isolated := map[string]map[string]machine.CPUSet{
		"pod": {"container": machine.NewCPUSet(4)},
	}
	got, err := materializeDefaultShareCPUSet(3, available, pools, isolated)
	require.NoError(t, err)
	require.True(t, got.Equals(machine.NewCPUSet(5, 6, 7)))
}

func TestBuildDefaultShareEligibleCPUSetUsesFullTopologyAfterDNBMigration(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	numa0 := topology.CPUDetails.CPUsInNUMANodes(0)
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1)
	p.reservedCPUs = machine.NewCPUSet(numa0.ToSliceInt()[0])

	// machineState still reflects the pre-migration DNB placement on NUMA 0.
	staleMachineState := p.state.GetMachineState()
	staleMachineState[0].DefaultCPUSet = machine.NewCPUSet()
	staleMachineState[0].AllocatedCPUSet = numa0.Clone()
	staleMachineState[1].ResourcePackageStates = map[string]*state.ResourcePackageState{
		"pinned": {PinnedCPUSet: machine.NewCPUSet(numa1.ToSliceInt()[0])},
	}

	// Finalized entries already place the exclusive DNB container on NUMA 1.
	finalizedEntries := state.PodEntries{
		"migrated-dnb": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "migrated-dnb",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameDedicated,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
					Annotations: map[string]string{
						apiconsts.PodAnnotationMemoryEnhancementNumaBinding:   apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
						apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: apiconsts.PodAnnotationMemoryEnhancementNumaExclusiveEnable,
					},
				},
				AllocationResult:         numa1.Clone(),
				TopologyAwareAssignments: map[int]machine.CPUSet{1: numa1.Clone()},
			},
		},
		"system": {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta("system"),
				AllocationResult: machine.NewCPUSet(numa0.ToSliceInt()[1]),
			},
		},
	}
	rampFloor := machine.NewCPUSet(numa0.ToSliceInt()[2])

	got := p.buildDefaultShareEligibleCPUSet(finalizedEntries, staleMachineState, rampFloor)
	want := numa0.
		Difference(p.reservedCPUs).
		Difference(machine.NewCPUSet(numa0.ToSliceInt()[1])).
		Difference(rampFloor)
	require.True(t, got.Equals(want), "got=%s want=%s", got, want)
	require.False(t, got.Intersection(numa0).IsEmpty(),
		"old DNB NUMA must return to default-share eligibility after migration")
	require.True(t, got.Intersection(numa1).IsEmpty(),
		"new exclusive DNB NUMA must be excluded as a whole")
}

func TestMaterializeDefaultShareCPUSetRejectsQuantityMismatch(t *testing.T) {
	t.Parallel()

	actual, err := materializeDefaultShareCPUSet(
		2,
		machine.NewCPUSet(0, 1, 2, 3),
		map[string]machine.CPUSet{commonstate.PoolNameReclaim: machine.NewCPUSet(0)},
		nil,
	)
	require.ErrorIs(t, err, ErrDefaultShareResidualQuantityMismatch)
	var quantityErr *DefaultShareResidualQuantityError
	require.ErrorAs(t, err, &quantityErr)
	require.Equal(t, 2, quantityErr.AdvisedQuantity)
	require.Equal(t, 3, quantityErr.ResidualSize)
	require.True(t, actual.IsEmpty())
}

func TestMaterializeDefaultShareCPUSetAllowsAdvisorShrinkLag(t *testing.T) {
	t.Parallel()

	got, err := materializeDefaultShareCPUSet(
		7,
		machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
		map[string]machine.CPUSet{
			commonstate.PoolNameReclaim: machine.NewCPUSet(0),
			"snb-NUMA0":                 machine.NewCPUSet(1),
		},
		nil,
	)
	require.NoError(t, err)
	require.True(t, got.Equals(machine.NewCPUSet(2, 3, 4, 5, 6, 7)),
		"got=%s want=2-7", got)
}

func TestMaterializeDefaultShareCPUSetScenarios(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name          string
		expected      int
		available     machine.CPUSet
		pools         map[string]machine.CPUSet
		isolated      map[string]map[string]machine.CPUSet
		wantShare     machine.CPUSet
		wantErr       bool
		wantErrSubstr string
	}{
		{
			name:      "default share absorbs residual from NUMA with SNB",
			expected:  5,
			available: machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
			pools: map[string]machine.CPUSet{
				commonstate.PoolNameReclaim: machine.NewCPUSet(0, 4),
				"snb-NUMA0":                 machine.NewCPUSet(1),
			},
			wantShare: machine.NewCPUSet(2, 3, 5, 6, 7),
		},
		{
			name:      "exclusive NUMA is already absent from available",
			expected:  3,
			available: machine.NewCPUSet(0, 1, 2, 3),
			pools: map[string]machine.CPUSet{
				commonstate.PoolNameReclaim: machine.NewCPUSet(0),
			},
			wantShare: machine.NewCPUSet(1, 2, 3),
		},
		{
			name:      "stale default share pool value is excluded from fixed",
			expected:  2,
			available: machine.NewCPUSet(0, 1, 2, 3),
			pools: map[string]machine.CPUSet{
				commonstate.PoolNameReclaim: machine.NewCPUSet(0),
				// stale default share must NOT count as fixed; if it were
				// counted, residual would be {1} (size 1) and match no case.
				commonstate.PoolNameShare: machine.NewCPUSet(2, 3),
			},
			// residual = available - reclaim(0) = {1,2,3}; expected 2 => mismatch
			// proves the stale share value {2,3} was excluded (not subtracted).
			wantErr:       true,
			wantErrSubstr: "default share quantity 2 is smaller than residual cpuset size 3",
		},
		{
			name:     "resource-package pinned unused cpu never enters share",
			expected: 2,
			// candidate already had pinned cpus removed by the caller.
			available: machine.NewCPUSet(0, 1, 2, 3),
			pools: map[string]machine.CPUSet{
				commonstate.PoolNameReclaim: machine.NewCPUSet(0),
				"custom":                    machine.NewCPUSet(1),
			},
			wantShare: machine.NewCPUSet(2, 3),
		},
		{
			name:          "under-reported residual fails closed",
			expected:      1,
			available:     machine.NewCPUSet(0, 1, 2, 3),
			pools:         map[string]machine.CPUSet{commonstate.PoolNameReclaim: machine.NewCPUSet(0, 1)},
			wantErr:       true,
			wantErrSubstr: "default share quantity 1 is smaller than residual cpuset size 2",
		},
		{
			name:      "new fixed pool can shrink share before advisor quantity catches up",
			expected:  3,
			available: machine.NewCPUSet(0, 1, 2, 3),
			pools: map[string]machine.CPUSet{
				commonstate.PoolNameReclaim: machine.NewCPUSet(0),
				"snb-NUMA0":                 machine.NewCPUSet(1),
			},
			wantShare: machine.NewCPUSet(2, 3),
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := materializeDefaultShareCPUSet(tt.expected, tt.available, tt.pools, tt.isolated)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrSubstr != "" {
					require.ErrorContains(t, err, tt.wantErrSubstr)
				}
				return
			}
			require.NoError(t, err)
			require.True(t, got.Equals(tt.wantShare), "got=%s want=%s", got, tt.wantShare)
		})
	}
}

// TestMaterializeDefaultShareResidualIsCoreAlignedUnderComplementClosure covers
// the FillDefaultSharePoolWithNonReclaimCPUs=true third path (R12 / Task 4A). In
// that mode the default share pool is NOT sized from a tier layer; it is the
// residual complement `available - (fixed pools ∪ isolation)`. This test builds a
// mixed SMT2 node — SNB + exclusive-DNB(reclaim overlap) + isolation + reclaim —
// where every subtracted set is core-aligned, and proves complement closure: the
// residual share pool is itself core-aligned, and materializeDefaultShareCPUSet is
// left unmodified (no floor/crop), so its residual.Size()==expectedQuantity
// fail-closed contract still holds.
func TestMaterializeDefaultShareResidualIsCoreAlignedUnderComplementClosure(t *testing.T) {
	t.Parallel()

	// two-NUMA SMT2 host, 16 cpus / 8 cores each.
	topology, err := machine.GenerateDummyCPUTopology(32, 1, 2)
	require.NoError(t, err)
	require.Equal(t, 2, topology.CPUsPerCore())

	// core-aligned fixed consumers, disjoint per core:
	//   reclaim   : 2 cores/NUMA (subtracted as a non-share pool, mirrors rampUpReclaimFloor)
	//   snb       : 1 core on NUMA0
	//   exclusive : 2 cores on NUMA1 (the DNB donor's retained set)
	//   isolation : 1 core on NUMA0
	reclaim := coresInNUMA(topology, 0, 0, 2).Union(coresInNUMA(topology, 1, 0, 2))
	snb := coresInNUMA(topology, 0, 2, 3)
	exclusive := coresInNUMA(topology, 1, 2, 4)
	isolation := coresInNUMA(topology, 0, 3, 4)
	requireCoreAligned(t, topology, reclaim)
	requireCoreAligned(t, topology, snb)
	requireCoreAligned(t, topology, exclusive)
	requireCoreAligned(t, topology, isolation)

	available := topology.CPUDetails.CPUs()
	pools := map[string]machine.CPUSet{
		commonstate.PoolNameReclaim: reclaim,
		"snb-NUMA0":                 snb,
		"exclusive-NUMA1":           exclusive,
	}
	isolated := map[string]map[string]machine.CPUSet{
		"pod-iso": {"container-iso": isolation},
	}

	// expectedQuantity is the exact residual size: complement closure means it is
	// the sum of complete cores left over, so it is core-aligned by construction.
	fixed := reclaim.Union(snb).Union(exclusive).Union(isolation)
	expected := available.Difference(fixed).Size()
	require.Equal(t, 0, expected%topology.CPUsPerCore(),
		"complement of core-aligned sets must itself be a whole-core multiple")

	share, err := materializeDefaultShareCPUSet(expected, available, pools, isolated)
	require.NoError(t, err)
	require.Equal(t, expected, share.Size(),
		"residual.Size() must equal expectedQuantity (fail-closed contract intact)")
	require.True(t, share.Intersection(fixed).IsEmpty(),
		"share residual must not overlap any fixed consumer")
	requireCoreAligned(t, topology, share)
	// reclaim buckets themselves stay untouched by the fill path and core-aligned.
	requireCoreAligned(t, topology, share.Intersection(topology.CPUDetails.CPUsInNUMANodes(0)))
	requireCoreAligned(t, topology, share.Intersection(topology.CPUDetails.CPUsInNUMANodes(1)))
}

// TestMaterializeDefaultShareResidualComplementClosureNonSMTZeroDrift is the
// CPUsPerCore()==1 variant: with SMT disabled every cpu is its own core, so the
// residual complement is trivially "core-aligned" and behavior is identical to
// the pre-alignment code — proving the fill=true path carries zero drift on
// non-SMT hosts.
func TestMaterializeDefaultShareResidualComplementClosureNonSMTZeroDrift(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(16, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, topology.CPUsPerCore())

	available := topology.CPUDetails.CPUs()
	reclaim := machine.NewCPUSet(0, 1, 2)
	snb := machine.NewCPUSet(3)
	pools := map[string]machine.CPUSet{
		commonstate.PoolNameReclaim: reclaim,
		"snb-NUMA0":                 snb,
	}
	fixed := reclaim.Union(snb)
	expected := available.Difference(fixed).Size()

	share, err := materializeDefaultShareCPUSet(expected, available, pools, nil)
	require.NoError(t, err)
	require.Equal(t, expected, share.Size())
	require.True(t, share.Equals(available.Difference(fixed)),
		"non-smt residual must be the plain complement, got %s", share.String())
	requireCoreAligned(t, topology, share)
}

// TestFinalizeDefaultShareEntryReadsPostReviseState verifies finalizeDefaultShareEntry
// derives the default share cpuset from the live newPodEntries pool allocation
// results (i.e. post reclaimOverlapNUMABinding / post reviseReclaimPool), so a
// change to a non-share pool changes the resulting share cpuset.
func TestFinalizeDefaultShareEntryReadsPostReviseState(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	newPodEntries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(0, 1),
			},
		},
		"custom": {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta("custom"),
				AllocationResult: machine.NewCPUSet(2),
			},
		},
		"pod": {
			"container": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod",
					ContainerName: "container",
					OwnerPoolName: commonstate.PoolNameDedicated,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
				},
				AllocationResult: machine.NewCPUSet(3),
			},
		},
	}
	candidate := machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7)

	// residual = candidate - reclaim(0,1) - custom(2) - isolated(3) = {4,5,6,7}
	require.NoError(t, p.finalizeDefaultShareEntry(newPodEntries, newPodEntries, 4, candidate))
	require.True(t, newPodEntries[commonstate.PoolNameShare][commonstate.FakedContainerName].
		AllocationResult.Equals(machine.NewCPUSet(4, 5, 6, 7)))
	require.NotEmpty(t, newPodEntries[commonstate.PoolNameShare][commonstate.FakedContainerName].
		TopologyAwareAssignments)

	// simulate reviseReclaimPool growing the reclaim pool; share must shrink.
	newPodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].AllocationResult = machine.NewCPUSet(0, 1, 4)
	require.NoError(t, p.finalizeDefaultShareEntry(newPodEntries, newPodEntries, 3, candidate))
	require.True(t, newPodEntries[commonstate.PoolNameShare][commonstate.FakedContainerName].
		AllocationResult.Equals(machine.NewCPUSet(5, 6, 7)))

	// fail-closed on quantity mismatch.
	require.Error(t, p.finalizeDefaultShareEntry(newPodEntries, newPodEntries, 2, candidate))
}

func TestDynamicPolicyFinalizeDefaultShareEntryExcludesDNB(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	dnbCPUSet := machine.NewCPUSet(3)
	newPodEntries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(0, 1),
			},
		},
		"dedicated-pod": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "dedicated-pod",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameDedicated,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
					Annotations: map[string]string{
						apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					},
				},
				AllocationResult: dnbCPUSet.Clone(),
			},
		},
	}

	require.NoError(t, p.finalizeDefaultShareEntry(
		newPodEntries,
		newPodEntries,
		6,
		machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7),
	))
	shareCPUSet := newPodEntries[commonstate.PoolNameShare][commonstate.FakedContainerName].AllocationResult
	require.True(t, shareCPUSet.Equals(machine.NewCPUSet(2, 4, 5, 6, 7)))
	require.True(t, shareCPUSet.Intersection(dnbCPUSet).IsEmpty())
}

func TestFinalizeDefaultShareEntryAllowsAdvisorShrinkLag(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	newPodEntries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(0),
			},
		},
		"snb-NUMA0": {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta("snb-NUMA0"),
				AllocationResult: machine.NewCPUSet(1),
			},
		},
		commonstate.PoolNameShare: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
				AllocationResult: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7),
			},
		},
	}

	require.NoError(t, p.finalizeDefaultShareEntry(
		newPodEntries, newPodEntries, 7, machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7)))
	require.True(t, newPodEntries[commonstate.PoolNameShare][commonstate.FakedContainerName].
		AllocationResult.Equals(machine.NewCPUSet(2, 3, 4, 5, 6, 7)))
}

// TestAdjustPoolsAndIsolatedEntriesWithRampUpFloorBackfillsDefaultShareResidual is an
// entry-level integration test for the residual-backfill gate. It exercises the whole
// adjustPoolsAndIsolatedEntriesWithRampUpFloor entry segment with
// FillDefaultSharePoolWithNonReclaimCPUs enabled and a stale materialized share
// quantity, covering the chain:
//
//	copyPoolQuantityMap -> gate branch extracts+deletes the default share quantity ->
//	constructs the default share materialization plan -> groupAndAllocatePools (share pool absent) ->
//	applyPoolsAndIsolatedInfo backfills newPodEntries[share] with the residual cpuset.
//
// It asserts the local rebuild derives the upper bound from the current eligible
// CPUSet instead of treating the stale checkpoint share size as advisor advice.
func TestAdjustPoolsAndIsolatedEntriesUsesEligibleDefaultShareUpperBoundAndExcludesDNB(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)

	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	// deterministic allocation: no reserved cpus, reclaim enabled, overlap disabled
	// (the backfill gate is mutually exclusive with overlap, see materializeDefaultShareCPUSet).
	p.reservedCPUs = machine.NewCPUSet()
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)

	// open the residual-backfill gate.
	p.dynamicConfig.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs = true

	// seed a historical reclaim pool so takeCPUsForPoolsInPlaceWithPreferred pins the
	// reclaim pool to one complete core deterministically, plus a non-binding
	// shared_cores container that owns the default share pool so cleanPools keeps the
	// backfilled entry, plus a DNB container whose allocation must remain outside the
	// default-share residual. the seed is core-aligned (one whole core) because the
	// tier layer now forces whole-core reclaim selection (invariant B').
	reclaimSeed := coresInNUMA(topology, 0, 0, 1)
	dnbCPUSet := coresInNUMA(topology, 0, 1, 2)
	seedEntries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult:                 reclaimSeed.Clone(),
				OriginalAllocationResult:         reclaimSeed.Clone(),
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: reclaimSeed.Clone()},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: reclaimSeed.Clone()},
			},
		},
		"share-pod": {
			"container": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "share-pod",
					PodNamespace:  "default",
					PodName:       "share-pod",
					ContainerName: "container",
					OwnerPoolName: commonstate.PoolNameShare,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				},
				RequestQuantity: 6,
			},
		},
		"dedicated-pod": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "dedicated-pod",
					PodNamespace:  "default",
					PodName:       "dedicated-pod",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameDedicated,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
					Annotations: map[string]string{
						apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					},
				},
				RequestQuantity:                  float64(dnbCPUSet.Size()),
				AllocationResult:                 dnbCPUSet.Clone(),
				OriginalAllocationResult:         dnbCPUSet.Clone(),
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: dnbCPUSet.Clone()},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: dnbCPUSet.Clone()},
			},
		},
	}
	p.state.SetPodEntries(seedEntries, false)

	// The checkpoint share quantity is stale and smaller than the current
	// residual: candidate {0..7} minus reclaim {0,1} and DNB {2} = 5 cpus.
	poolsQuantityMap := map[string]map[int]int{
		commonstate.PoolNameShare: {
			commonstate.FakedNUMAID: 1,
		},
		commonstate.PoolNameReclaim: {
			commonstate.FakedNUMAID: 2,
		},
	}

	err = p.adjustPoolsAndIsolatedEntriesWithRampUpFloor(
		poolsQuantityMap,
		map[string]map[string]int{},
		p.state.GetPodEntries(),
		p.state.GetMachineState(),
		false,
		machine.NewCPUSet(),
		false,
	)
	require.NoError(t, err)

	updatedEntries := p.state.GetPodEntries()

	// reclaim pool stays pinned to its historical whole core.
	reclaim, err := updatedEntries.GetCPUSetForPool(commonstate.PoolNameReclaim)
	require.NoError(t, err)
	require.True(t, reclaim.Equals(reclaimSeed), "reclaim=%s want=%s", reclaim, reclaimSeed)
	requireCoreAligned(t, topology, reclaim)

	// the default share pool entry is the backfilled residual = candidate - reclaim - DNB.
	wantShare := topology.CPUDetails.CPUs().Difference(reclaimSeed).Difference(dnbCPUSet)
	share, err := updatedEntries.GetCPUSetForPool(commonstate.PoolNameShare)
	require.NoError(t, err)
	require.True(t, share.Equals(wantShare),
		"share=%s want=%s", share, wantShare)
	require.True(t, share.Intersection(dnbCPUSet).IsEmpty())
	requireCoreAligned(t, topology, share)

	dnb := updatedEntries["dedicated-pod"]["main"]
	require.NotNil(t, dnb)
	require.True(t, dnb.AllocationResult.Equals(dnbCPUSet))

	// the owning shared_cores container inherits the backfilled share cpuset.
	owner := updatedEntries["share-pod"]["container"]
	require.NotNil(t, owner)
	require.True(t, owner.AllocationResult.Equals(share),
		"owner=%s share=%s", owner.AllocationResult, share)
}

func TestAdjustPoolsAndIsolatedEntriesRejectsRevisionAdvancedBeforeApply(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.reservedCPUs = machine.NewCPUSet()
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)

	entries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult:                 machine.NewCPUSet(0, 1),
				OriginalAllocationResult:         machine.NewCPUSet(0, 1),
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: machine.NewCPUSet(0, 1)},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(0, 1)},
			},
		},
		commonstate.PoolNameShare: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
				AllocationResult:                 machine.NewCPUSet(2, 3, 4, 5, 6, 7),
				OriginalAllocationResult:         machine.NewCPUSet(2, 3, 4, 5, 6, 7),
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: machine.NewCPUSet(2, 3, 4, 5, 6, 7)},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(2, 3, 4, 5, 6, 7)},
			},
		},
	}
	p.state.SetPodEntries(entries, false)
	calculationRevision := p.state.GetRevision()
	p.state = &revisionAdvanceOnOverlapReadState{State: p.state, advanceOnce: true}

	err = p.adjustPoolsAndIsolatedEntriesWithRampUpFloor(
		map[string]map[int]int{
			commonstate.PoolNameShare:   {commonstate.FakedNUMAID: 6},
			commonstate.PoolNameReclaim: {commonstate.FakedNUMAID: 2},
		},
		nil,
		entries,
		p.state.GetMachineState(),
		false,
		machine.NewCPUSet(),
		false,
	)
	require.ErrorContains(t, err, state.ErrStaleStateRevision.Error())
	require.Equal(t, calculationRevision+1, p.state.GetRevision(),
		"the concurrent state update must remain the only committed revision")
	require.Equal(t, entries, p.state.GetPodEntries(),
		"the stale calculation must not overwrite the concurrent state")
}

func TestAdjustAllocationEntriesAtRevisionRejectsStateAdvancedAfterInputRead(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	entries := p.state.GetPodEntries()
	machineState := p.state.GetMachineState()
	expectedRevision := p.state.GetRevision()

	p.state.SetAllowSharedCoresOverlapReclaimedCores(true, false)
	require.Greater(t, p.state.GetRevision(), expectedRevision)

	err = p.adjustAllocationEntriesAtRevision(
		entries, machineState, false, expectedRevision)
	require.ErrorIs(t, err, state.ErrStaleStateRevision)
	require.True(t, p.state.GetAllowSharedCoresOverlapReclaimedCores(),
		"stale calculation must not replace the concurrent state")
}

func TestAdjustAllocationEntriesRejectsDefaultShareFallbackWithoutHealthyAdvisor(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.dynamicConfig.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs = true
	p.enableCPUAdvisor = false

	err = p.adjustAllocationEntriesAtRevision(
		p.state.GetPodEntries(),
		p.state.GetMachineState(),
		false,
		p.state.GetRevision(),
	)
	require.EqualError(t, err, "default share residual quantity requires a healthy cpu advisor")
}

func TestAdjustPoolsAndIsolatedEntriesRejectsMixedDefaultShareNUMAQuantities(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.dynamicConfig.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs = true

	err = p.adjustPoolsAndIsolatedEntriesWithRampUpFloor(
		map[string]map[int]int{
			commonstate.PoolNameShare: {
				commonstate.FakedNUMAID: 6,
				0:                       1,
			},
		},
		nil,
		p.state.GetPodEntries(),
		p.state.GetMachineState(),
		false,
		machine.NewCPUSet(),
		false,
	)
	require.Error(t, err)
	require.Equal(t, strings.ToLower(err.Error()), err.Error())
	require.ErrorContains(t, err, "default share quantity map must contain only faked numa id")
}

// TestAdjustPoolsAndIsolatedEntriesWithRampUpFloorBackfillsDefaultShareResidualWithSystemPool
// reproduces the SNB failure where advisor computes default share quantity from
// machine - reserve - system/forbidden - final reclaim - fixed pools, while QRM
// residual cpuset was derived from an entries snapshot that did not contain the
// system pool. The test intentionally keeps the canonical state aware of the
// system pool but passes a per-round entries/machineState snapshot without it,
// matching the observed source-of-truth skew.
func TestAdjustPoolsAndIsolatedEntriesWithRampUpFloorBackfillsDefaultShareResidualWithSystemPool(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)

	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	p.reservedCPUs = machine.NewCPUSet(0)
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs = true
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)

	entriesWithoutSystemPool := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult:                 machine.NewCPUSet(6, 7),
				OriginalAllocationResult:         machine.NewCPUSet(6, 7),
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: machine.NewCPUSet(6, 7)},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(6, 7)},
			},
		},
		"snbpool": {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta("snbpool"),
				AllocationResult:                 machine.NewCPUSet(4),
				OriginalAllocationResult:         machine.NewCPUSet(4),
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: machine.NewCPUSet(4)},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(4)},
			},
		},
		"share-pod": {
			"container": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "share-pod",
					PodNamespace:  "default",
					PodName:       "share-pod",
					ContainerName: "container",
					OwnerPoolName: commonstate.PoolNameShare,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				},
				RequestQuantity: 3,
			},
		},
	}
	p.state.SetPodEntries(entriesWithoutSystemPool, false)
	machineStateWithoutSystemPool := p.state.GetMachineState()

	canonicalEntries := entriesWithoutSystemPool.Clone()
	canonicalEntries["system"] = state.ContainerEntries{
		commonstate.FakedContainerName: &state.AllocationInfo{
			AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta("system"),
			AllocationResult:                 machine.NewCPUSet(1),
			OriginalAllocationResult:         machine.NewCPUSet(1),
			TopologyAwareAssignments:         map[int]machine.CPUSet{0: machine.NewCPUSet(1)},
			OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(1)},
		},
	}
	p.state.SetPodEntries(canonicalEntries, false)

	// Advisor-side quantity has already excluded reserve {0}, system {1},
	// reclaim {6,7}, and the SNB pool {2}, leaving default share {3,4,5}.
	poolsQuantityMap := map[string]map[int]int{
		commonstate.PoolNameShare: {
			commonstate.FakedNUMAID: 3,
		},
		commonstate.PoolNameReclaim: {
			commonstate.FakedNUMAID: 2,
		},
		"snbpool": {
			0: 1,
		},
	}

	err = p.adjustPoolsAndIsolatedEntriesWithRampUpFloor(
		poolsQuantityMap,
		map[string]map[string]int{},
		entriesWithoutSystemPool,
		machineStateWithoutSystemPool,
		false,
		machine.NewCPUSet(),
		false,
	)
	require.NoError(t, err)

	share, err := p.state.GetPodEntries().GetCPUSetForPool(commonstate.PoolNameShare)
	require.NoError(t, err)
	require.True(t, share.Equals(machine.NewCPUSet(3, 4, 5)),
		"share=%s want=3-5", share)
	require.False(t, share.Contains(1), "default share must exclude system cpu 1")
}

func TestNewRampUpPlanningPolicyPreservesCPUAdvisorState(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)

	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.emitter = &recordingMetricEmitter{}
	p.enableCPUAdvisor = true
	p.advisorMonitor, err = timemonitor.NewTimeMonitor(
		"advisor",
		time.Second,
		time.Minute,
		time.Minute,
		"advisor_unhealthy",
		&metrics.DummyMetrics{},
		1,
		true,
	)
	require.NoError(t, err)

	planningPolicy := p.newRampUpPlanningPolicy(state.NewTransientState(topology))

	require.True(t, planningPolicy.enableCPUAdvisor,
		"planning policy should keep advisor quantity source when outer policy uses advisor")
	require.IsType(t, metrics.DummyMetrics{}, planningPolicy.emitter,
		"planning policy must use a no-op metric emitter")
	require.Same(t, p.advisorMonitor, planningPolicy.advisorMonitor,
		"planning policy should share advisor health monitor with outer policy")
}
