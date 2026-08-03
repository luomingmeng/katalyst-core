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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestBuildCPUSetPodStateMap(t *testing.T) {
	t.Parallel()
	t.Run("test-empty-input", func(t *testing.T) {
		t.Parallel()
		p := &DynamicPolicy{}
		result := p.buildCPUSetPodStateMap(context.Background(), state.PodEntries{}, make(map[string]map[string]machine.CPUSet))
		assert.Empty(t, result)
	})

	t.Run("test-with-pods", func(t *testing.T) {
		t.Parallel()
		p := &DynamicPolicy{
			metaServer: &metaserver.MetaServer{
				MetaAgent: &agent.MetaAgent{
					PodFetcher: &pod.PodFetcherStub{
						PodList: []*v1.Pod{
							{
								ObjectMeta: metav1.ObjectMeta{
									UID:       "test-pod",
									Namespace: "test-namespace",
									Name:      "test-pod",
								},
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name: "test-container",
											Resources: v1.ResourceRequirements{
												Requests: v1.ResourceList{
													v1.ResourceCPU: resource.MustParse("4"),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		podUID := "test-pod"
		containerName := "test-container"
		cpuset := machine.MustParse("0-3")

		podEntries := state.PodEntries{
			podUID: {
				containerName: &state.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:        podUID,
						PodNamespace:  "test-namespace",
						PodName:       "test-pod",
						ContainerName: containerName,
						ContainerType: v1alpha1.ContainerType_MAIN.String(),
						QoSLevel:      consts.PodAnnotationQoSLevelDedicatedCores,
					},
					AllocationResult: cpuset,
				},
			},
		}

		actualCPUSets := map[string]map[string]machine.CPUSet{
			podUID: {
				containerName: cpuset,
			},
		}

		result := p.buildCPUSetPodStateMap(context.Background(), podEntries, actualCPUSets)
		require.Len(t, result, 1)
		assert.Equal(t, cpuset, result["0-3"].cpuset)
	})
}

func TestCalculateTotalCPURequest(t *testing.T) {
	t.Parallel()
	t.Run("test-no-subset", func(t *testing.T) {
		t.Parallel()
		p := &DynamicPolicy{}
		cs := &cpusetPodState{
			totalMilliCPURequest: 1000,
		}
		result := p.calculateTotalCPURequest("0-3", cs, make(map[string]*cpusetPodState))
		assert.Equal(t, int64(1000), result)
	})

	t.Run("test-with-subset", func(t *testing.T) {
		t.Parallel()
		p := &DynamicPolicy{}
		cs := &cpusetPodState{
			cpuset:               machine.MustParse("0-3"),
			totalMilliCPURequest: 1000,
		}

		cpusetPodStateMap := map[string]*cpusetPodState{
			"0-1": {
				cpuset:               machine.MustParse("0-1"),
				totalMilliCPURequest: 500,
			},
			"2-3": {
				cpuset:               machine.MustParse("2-3"),
				totalMilliCPURequest: 500,
			},
		}

		result := p.calculateTotalCPURequest("0-3", cs, cpusetPodStateMap)
		assert.Equal(t, int64(2000), result)
	})
}

func TestEmitExceededMetrics(t *testing.T) {
	t.Parallel()
	t.Run("test-shared-cores", func(t *testing.T) {
		t.Parallel()
		p := &DynamicPolicy{
			emitter:       &metrics.DummyMetrics{},
			dynamicConfig: dynamicconfig.NewDynamicAgentConfiguration(),
		}

		podUID := "test-pod"
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "test-pod",
				Namespace: "test-namespace",
				Name:      "test-pod",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "test-container",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("2"),
							},
						},
					},
				},
			},
		}

		cs := &cpusetPodState{
			cpuset:               machine.MustParse("0-1"),
			totalMilliCPURequest: 3000,
			podMap: map[string]*v1.Pod{
				podUID: pod,
			},
		}

		podEntries := state.PodEntries{
			podUID: {
				"test-container": &state.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:        podUID,
						ContainerName: "test-container",
						ContainerType: v1alpha1.ContainerType_MAIN.String(),
						QoSLevel:      consts.PodAnnotationQoSLevelSharedCores,
					},
				},
			},
		}

		p.emitExceededMetrics(podEntries, "0-1", cs, 0, false)
	})

	t.Run("test-dedicated-cores", func(t *testing.T) {
		t.Parallel()
		p := &DynamicPolicy{
			emitter:       &metrics.DummyMetrics{},
			dynamicConfig: dynamicconfig.NewDynamicAgentConfiguration(),
		}

		podUID := "test-pod"
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "test-pod",
				Namespace: "test-namespace",
				Name:      "test-pod",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "test-container",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("2"),
							},
						},
					},
				},
			},
		}

		cs := &cpusetPodState{
			cpuset:               machine.MustParse("0-1"),
			totalMilliCPURequest: 3000,
			podMap: map[string]*v1.Pod{
				podUID: pod,
			},
		}

		podEntries := state.PodEntries{
			podUID: {
				"test-container": &state.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:        podUID,
						ContainerName: "test-container",
						ContainerType: v1alpha1.ContainerType_MAIN.String(),
						QoSLevel:      consts.PodAnnotationQoSLevelDedicatedCores,
					},
				},
			},
		}

		p.emitExceededMetrics(podEntries, "0-1", cs, 0, false)
	})
}

type stubNodeFetcher struct {
	node *v1.Node
	err  error
}

func (s *stubNodeFetcher) Run(_ context.Context) {}

func (s *stubNodeFetcher) GetNode(_ context.Context) (*v1.Node, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.node, nil
}

func newCPUSet(start, size int) machine.CPUSet {
	cpus := make([]int, 0, size)
	for i := 0; i < size; i++ {
		cpus = append(cpus, start+i)
	}
	return machine.NewCPUSet(cpus...)
}

func newPoolAllocationInfo(t *testing.T, policy *DynamicPolicy, poolName string, allocation machine.CPUSet) *state.AllocationInfo {
	t.Helper()

	assignments, err := machine.GetNumaAwareAssignments(policy.machineInfo.CPUTopology, allocation)
	require.NoError(t, err)

	return &state.AllocationInfo{
		AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(poolName),
		AllocationResult:                 allocation,
		OriginalAllocationResult:         allocation.Clone(),
		TopologyAwareAssignments:         assignments,
		OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(assignments),
	}
}

func newSystemAllocationInfo(t *testing.T, policy *DynamicPolicy, podUID, containerName, specifiedPool string, allocation machine.CPUSet) *state.AllocationInfo {
	t.Helper()

	assignments, err := machine.GetNumaAwareAssignments(policy.machineInfo.CPUTopology, allocation)
	require.NoError(t, err)

	return &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        podUID,
			PodNamespace:  "test-ns",
			PodName:       podUID,
			ContainerName: containerName,
			ContainerType: v1alpha1.ContainerType_MAIN.String(),
			QoSLevel:      consts.PodAnnotationQoSLevelSystemCores,
			OwnerPoolName: commonstate.GetSystemPoolName(specifiedPool),
			Annotations: map[string]string{
				consts.PodAnnotationCPUEnhancementCPUSet: specifiedPool,
			},
		},
		AllocationResult:                 allocation,
		OriginalAllocationResult:         allocation.Clone(),
		TopologyAwareAssignments:         assignments,
		OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(assignments),
	}
}

func TestGetExpectedSystemExclusivePools(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		policy        *DynamicPolicy
		expectedPools map[string]int
		wantNil       bool
	}{
		{
			name: "skip invalid size and add system prefix",
			policy: &DynamicPolicy{
				dynamicConfig: func() *dynamicconfig.DynamicAgentConfiguration {
					conf := dynamicconfig.NewConfiguration()
					conf.SystemExclusivePool = map[string]int{
						"latency": 2,
						"broken":  0,
					}
					cfg := dynamicconfig.NewDynamicAgentConfiguration()
					cfg.SetDynamicConfiguration(conf)
					return cfg
				}(),
			},
			expectedPools: map[string]int{
				commonstate.GetSystemPoolName("latency"): 2,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.policy.getExpectedSystemExclusivePools()

			if tc.wantNil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, tc.expectedPools, got)
		})
	}
}

func TestCalculateSystemExclusivePoolChanges(t *testing.T) {
	t.Parallel()

	t.Run("respect configured shrink bounds", func(t *testing.T) {
		t.Parallel()

		ratio := 0.2
		minShrink := int64(1)
		maxShrink := int64(2)
		conf := dynamicconfig.NewConfiguration()
		conf.SystemExclusivePoolShrinkRatio = &ratio
		conf.SystemExclusivePoolShrinkMin = &minShrink
		conf.SystemExclusivePoolShrinkMax = &maxShrink

		policy := newTestDynamicPolicy(t, "calculate-system-exclusive-pool-changes-0")
		policy.dynamicConfig.SetDynamicConfiguration(conf)

		currentPools := map[string]*state.AllocationInfo{
			"system-latency": {AllocationResult: newCPUSet(0, 10)},
			"system-batch":   {AllocationResult: newCPUSet(10, 4)},
			"system-unused":  {AllocationResult: newCPUSet(14, 2)},
		}
		expectedPools := map[string]int{
			"system-latency": 7,
			"system-batch":   7,
		}

		toCreate, toUpdate, toDelete := policy.calculateSystemExclusivePoolChanges(currentPools, expectedPools)

		assert.Empty(t, toCreate)
		assert.Equal(t, map[string]int{
			"system-latency": -2,
			"system-batch":   3,
		}, toUpdate)
		assert.Equal(t, []string{"system-unused"}, toDelete.List())
	})

	t.Run("shrink is capped by expected delta", func(t *testing.T) {
		t.Parallel()

		policy := newTestDynamicPolicy(t, "calculate-system-exclusive-pool-changes-1")

		currentPools := map[string]*state.AllocationInfo{
			"system-latency": {AllocationResult: newCPUSet(0, 10)},
		}
		expectedPools := map[string]int{
			"system-latency": 9,
		}

		toCreate, toUpdate, toDelete := policy.calculateSystemExclusivePoolChanges(currentPools, expectedPools)

		assert.Empty(t, toCreate)
		assert.Equal(t, map[string]int{"system-latency": -1}, toUpdate)
		assert.Empty(t, toDelete.List())
	})

	t.Run("create pool only with pods", func(t *testing.T) {
		t.Parallel()

		policy := newTestDynamicPolicy(t, "calculate-system-exclusive-pool-changes-2")
		setPodEntriesForTest(t, policy.state, state.PodEntries{
			"pod-with-pool": {
				"main": newSystemAllocationInfo(t, policy, "pod-with-pool", "main", "new1", newCPUSet(4, 2)),
			},
		}, false)

		expectedPools := map[string]int{
			"system-new1": 2,
			"system-new2": 2,
		}

		toCreate, toUpdate, toDelete := policy.calculateSystemExclusivePoolChanges(nil, expectedPools)

		assert.Equal(t, map[string]int{"system-new1": 2}, toCreate)
		assert.Empty(t, toUpdate)
		assert.Empty(t, toDelete.List())
	})
}

func TestUpdateSystemExclusivePool(t *testing.T) {
	t.Parallel()

	policy := newTestDynamicPolicy(t, "update-system-exclusive-pool")
	poolToShrink := commonstate.GetSystemPoolName("latency")
	poolToExpand := commonstate.GetSystemPoolName("batch")

	setAllocationInfoForTest(t, policy.state, poolToShrink, commonstate.FakedContainerName,
		newPoolAllocationInfo(t, policy, poolToShrink, newCPUSet(0, 4)), false)
	setAllocationInfoForTest(t, policy.state, poolToExpand, commonstate.FakedContainerName,
		newPoolAllocationInfo(t, policy, poolToExpand, newCPUSet(4, 2)), false)

	availableAfterUpdate, err := policy.updateSystemExclusivePool(map[string]int{
		poolToShrink: -2,
		poolToExpand: 2,
	}, machine.NewCPUSet())
	require.NoError(t, err)

	shrunkPool := policy.state.GetAllocationInfo(poolToShrink, commonstate.FakedContainerName)
	expandedPool := policy.state.GetAllocationInfo(poolToExpand, commonstate.FakedContainerName)
	require.NotNil(t, shrunkPool)
	require.NotNil(t, expandedPool)

	assert.Equal(t, 2, shrunkPool.AllocationResult.Size())
	assert.Equal(t, 4, expandedPool.AllocationResult.Size())
	assert.True(t, availableAfterUpdate.IsEmpty())
	assert.True(t, shrunkPool.AllocationResult.Intersection(expandedPool.AllocationResult).IsEmpty())
}

func TestCreateSystemExclusivePool(t *testing.T) {
	t.Parallel()

	policy := newTestDynamicPolicy(t, "create-system-exclusive-pool")
	poolName := commonstate.GetSystemPoolName("latency")
	availableCPUs := newCPUSet(0, 4)

	remainingCPUs, err := policy.createSystemExclusivePool(map[string]int{poolName: 2}, availableCPUs)
	require.NoError(t, err)

	createdPool := policy.state.GetAllocationInfo(poolName, commonstate.FakedContainerName)
	require.NotNil(t, createdPool)

	assert.Equal(t, 2, createdPool.AllocationResult.Size())
	assert.Equal(t, 2, remainingCPUs.Size())
	assert.True(t, createdPool.AllocationResult.Intersection(remainingCPUs).IsEmpty())
	assert.True(t, createdPool.AllocationResult.Union(remainingCPUs).Equals(availableCPUs))
}

func TestAdjustSystemCoresPodAllocation(t *testing.T) {
	t.Parallel()

	policy := newTestDynamicPolicy(t, "adjust-system-cores-pod-allocation")
	defaultSystemCPUs := policy.machineInfo.CPUDetails.CPUs()
	poolName := commonstate.GetSystemPoolName("latency")
	poolAllocation := newCPUSet(0, 2)

	setAllocationInfoForTest(t, policy.state, poolName, commonstate.FakedContainerName,
		newPoolAllocationInfo(t, policy, poolName, poolAllocation), false)
	setAllocationInfoForTest(t, policy.state, "pod-with-pool", "main",
		newSystemAllocationInfo(t, policy, "pod-with-pool", "main", "latency", newCPUSet(4, 2)), false)
	setAllocationInfoForTest(t, policy.state, "pod-with-missing-pool", "main",
		newSystemAllocationInfo(t, policy, "pod-with-missing-pool", "main", "missing", newCPUSet(6, 2)), false)

	err := policy.adjustSystemCoresPodAllocation()
	require.NoError(t, err)

	adjustedPod := policy.state.GetAllocationInfo("pod-with-pool", "main")
	missingPoolPod := policy.state.GetAllocationInfo("pod-with-missing-pool", "main")
	require.NotNil(t, adjustedPod)
	require.NotNil(t, missingPoolPod)

	assert.True(t, adjustedPod.AllocationResult.Equals(poolAllocation))
	assert.True(t, missingPoolPod.AllocationResult.Equals(defaultSystemCPUs))
}

func TestApplySystemExclusivePoolChanges(t *testing.T) {
	t.Parallel()

	policy := newTestDynamicPolicy(t, "apply-system-exclusive-pool-changes")
	policy.reservedCPUs = machine.NewCPUSet()
	defaultSystemCPUs := policy.machineInfo.CPUDetails.CPUs()

	setAllocationInfoForTest(t, policy.state, "pod-with-pool", "main",
		newSystemAllocationInfo(t, policy, "pod-with-pool", "main", "latency", defaultSystemCPUs), false)

	err := policy.applySystemExclusivePoolChanges(
		map[string]int{commonstate.GetSystemPoolName("latency"): 2},
		nil,
		nil,
	)
	require.NoError(t, err)

	poolAllocationInfo := policy.state.GetAllocationInfo(commonstate.GetSystemPoolName("latency"), commonstate.FakedContainerName)
	podAllocationInfo := policy.state.GetAllocationInfo("pod-with-pool", "main")
	require.NotNil(t, poolAllocationInfo)
	require.NotNil(t, podAllocationInfo)

	assert.Equal(t, 2, poolAllocationInfo.AllocationResult.Size())
	assert.True(t, podAllocationInfo.AllocationResult.Equals(poolAllocationInfo.AllocationResult))
}

func TestSyncSystemExclusivePoolBulkheadFailureKeepsDurableBase(t *testing.T) {
	policy := newTestDynamicPolicy(t, "sync-system-exclusive-owned-target")
	policy.reservedCPUs = machine.NewCPUSet()
	policy.conf.EnableSystemExclusivePool = true
	conf := dynamicconfig.NewConfiguration()
	conf.SystemExclusivePool = map[string]int{"latency": 2}
	policy.dynamicConfig.SetDynamicConfiguration(conf)
	setAllocationInfoForTest(t, policy.state, "pod-with-pool", "main",
		newSystemAllocationInfo(t, policy, "pod-with-pool", "main", "latency", policy.machineInfo.CPUDetails.CPUs()), false)

	recordingState := &advisorTargetRecordingState{State: policy.state}
	policy.state = recordingState
	base, err := policy.state.PrepareDurableTarget()
	require.NoError(t, err)
	recordingState.events = nil
	policy.cpuSetMaterializer = &transactionRecordingMaterializer{
		events:  &recordingState.events,
		results: []cpusetmaterializer.Result{{}, {Converged: true}},
		errs:    []error{errors.New("injected system-exclusive materializer failure"), nil},
	}
	var logs bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&logs)
	t.Cleanup(func() {
		klog.LogToStderr(true)
	})

	policy.syncSystemExclusivePool(nil, nil, nil, nil, nil)

	require.Equal(t, []string{"prepare", "materialize", "materialize"}, recordingState.events)
	require.Zero(t, recordingState.commitCalls)
	require.Zero(t, recordingState.setCalls)
	require.Zero(t, recordingState.storeCalls)
	require.Equal(t, base, cloneAdvisorState(policy.state))
	require.Contains(t, logs.String(), "[SystemExclusivePool] failed with error:")
	require.NotContains(t, logs.String(), "[SystemExclusivePool] completed successfully",
		"failed worker transaction must not log success")
}

func TestSyncSystemExclusivePoolCommitsOwnedTargetUnderPolicyLock(t *testing.T) {
	policy := newTestDynamicPolicy(t, "sync-system-exclusive-owned-target-success")
	policy.reservedCPUs = machine.NewCPUSet()
	policy.conf.EnableSystemExclusivePool = true
	conf := dynamicconfig.NewConfiguration()
	conf.SystemExclusivePool = map[string]int{"latency": 2}
	policy.dynamicConfig.SetDynamicConfiguration(conf)
	setAllocationInfoForTest(t, policy.state, "pod-with-pool", "main",
		newSystemAllocationInfo(t, policy, "pod-with-pool", "main", "latency", policy.machineInfo.CPUDetails.CPUs()), false)

	recordingState := &advisorTargetRecordingState{State: policy.state}
	policy.state = recordingState
	policy.cpuSetMaterializer = &transactionRecordingMaterializer{
		events:  &recordingState.events,
		results: []cpusetmaterializer.Result{{Converged: true}},
		onCall: func(target cpusetmaterializer.Target) {
			if policy.TryLock() {
				policy.Unlock()
				t.Error("policy lock was not held during materialization")
			}
			require.Equal(t, 2, target.ContainerCPUSetByPod()["pod-with-pool"]["main"].Size())
		},
	}

	policy.syncSystemExclusivePool(nil, nil, nil, nil, nil)

	require.Equal(t, []string{"prepare", "materialize", "commit"}, recordingState.events)
	require.Equal(t, 1, recordingState.commitCalls)
	require.Zero(t, recordingState.setCalls)
	require.Zero(t, recordingState.storeCalls)
	pool := policy.state.GetAllocationInfo(commonstate.GetSystemPoolName("latency"), commonstate.FakedContainerName)
	require.NotNil(t, pool)
	require.Equal(t, 2, pool.AllocationResult.Size())
}

func TestClearResidualStateBulkheadFailureKeepsDurableBase(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	policy, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	const podUID = "residual-pod"
	setAllocationInfoForTest(t, policy.state, podUID, "main", &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        podUID,
			ContainerName: "main",
			ContainerType: v1alpha1.ContainerType_MAIN.String(),
			QoSLevel:      consts.PodAnnotationQoSLevelSharedCores,
			OwnerPoolName: commonstate.PoolNameShare,
		},
		AllocationResult: policy.machineInfo.CPUDetails.CPUs(),
	}, false)
	policy.residualHitMap = map[string]int64{
		podUID: int64(maxResidualTime/stateCheckPeriod) - 1,
	}
	policy.metaServer = &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{
			PodFetcher:          &pod.PodFetcherStub{},
			KatalystMachineInfo: policy.machineInfo,
		},
	}

	recordingState := &advisorTargetRecordingState{State: policy.state}
	policy.state = recordingState
	base, err := policy.state.PrepareDurableTarget()
	require.NoError(t, err)
	recordingState.events = nil
	policy.cpuSetMaterializer = &transactionRecordingMaterializer{
		events:  &recordingState.events,
		results: []cpusetmaterializer.Result{{}, {Converged: true}},
		errs:    []error{errors.New("injected residual cleanup materializer failure"), nil},
	}

	policy.clearResidualState(nil, nil, nil, nil, nil)

	require.Equal(t, []string{"prepare", "materialize", "materialize"}, recordingState.events)
	require.Zero(t, recordingState.commitCalls)
	require.Zero(t, recordingState.setCalls)
	require.Zero(t, recordingState.storeCalls)
	require.Equal(t, base, cloneAdvisorState(policy.state))
}

func TestClearResidualStateCommitCoversFailedPendingAddWithOutboxRemove(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	policy, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	for i, podUID := range []string{"residual-a", "residual-b"} {
		cpus := machine.NewCPUSet(i)
		assignments, assignmentErr := machine.GetNumaAwareAssignments(policy.machineInfo.CPUTopology, cpus)
		require.NoError(t, assignmentErr)
		setAllocationInfoForTest(t, policy.state, podUID, "main", &state.AllocationInfo{
			AllocationMeta: commonstate.AllocationMeta{
				PodUid:        podUID,
				ContainerName: "main",
				ContainerType: v1alpha1.ContainerType_MAIN.String(),
				QoSLevel:      consts.PodAnnotationQoSLevelSharedCores,
				OwnerPoolName: commonstate.PoolNameShare,
			},
			AllocationResult:                 cpus,
			OriginalAllocationResult:         cpus.Clone(),
			TopologyAwareAssignments:         assignments,
			OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(assignments),
		}, false)
	}
	policy.residualHitMap = map[string]int64{
		"residual-a": int64(maxResidualTime/stateCheckPeriod) - 1,
		"residual-b": int64(maxResidualTime/stateCheckPeriod) - 1,
	}
	policy.metaServer = &metaserver.MetaServer{MetaAgent: &agent.MetaAgent{
		PodFetcher:          &pod.PodFetcherStub{},
		KatalystMachineInfo: policy.machineInfo,
	}}

	recordingState := &advisorTargetRecordingState{State: policy.state}
	policy.state = recordingState
	policy.cpuSetMaterializer = &transactionRecordingMaterializer{
		events:  &recordingState.events,
		results: []cpusetmaterializer.Result{{Converged: true}},
	}
	policy.enableCPUAdvisor = true
	advisor := &outboxRecordingAdvisor{
		addEntered:  make(chan struct{}),
		addRelease:  make(chan struct{}),
		addFailures: 1,
	}
	policy.advisorClient = advisor
	emitter := NewMockMetricsEmitter()
	policy.emitter = emitter
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)
	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "residual-a", ContainerName: "main"})
	<-advisor.addEntered

	policy.clearResidualState(nil, nil, nil, nil, nil)
	close(advisor.addRelease)

	require.Equal(t, []string{"prepare", "materialize", "commit"}, recordingState.events)
	require.Equal(t, []recordedAdvisorCall{
		{operation: "add", podUID: "residual-a", containerName: "main"},
		{operation: "remove", podUID: "residual-a"},
		{operation: "remove", podUID: "residual-b"},
	}, waitForAdvisorCalls(t, advisor, 3))
	require.Nil(t, policy.state.GetAllocationInfo("residual-a", "main"),
		"failed AddContainer must not roll back committed residual cleanup")
	require.Nil(t, policy.state.GetAllocationInfo("residual-b", "main"))
}
