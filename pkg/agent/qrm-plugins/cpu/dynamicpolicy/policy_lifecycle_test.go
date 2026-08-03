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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	advisorsvc "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type failingAddContainerAdvisor struct {
	mu       sync.Mutex
	attempts int
}

type lifecycleRecordingIRQTuner struct {
	mu        sync.Mutex
	runCount  int
	stopCount int
}

func (t *lifecycleRecordingIRQTuner) Run(stopCh <-chan struct{}) {
	t.mu.Lock()
	t.runCount++
	t.mu.Unlock()
	<-stopCh
}

func (t *lifecycleRecordingIRQTuner) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopCount++
}

func (t *lifecycleRecordingIRQTuner) counts() (int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.runCount, t.stopCount
}

func (f *failingAddContainerAdvisor) AddContainer(context.Context, *advisorsvc.ContainerMetadata, ...grpc.CallOption) (*advisorsvc.AddContainerResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	return nil, errors.New("advisor unavailable")
}

func (f *failingAddContainerAdvisor) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

func (f *failingAddContainerAdvisor) RemovePod(context.Context, *advisorsvc.RemovePodRequest, ...grpc.CallOption) (*advisorsvc.RemovePodResponse, error) {
	return &advisorsvc.RemovePodResponse{}, nil
}

func (f *failingAddContainerAdvisor) ListAndWatch(context.Context, *advisorsvc.Empty, ...grpc.CallOption) (advisorapi.CPUAdvisor_ListAndWatchClient, error) {
	return nil, errors.New("not implemented")
}

func (f *failingAddContainerAdvisor) GetAdvice(context.Context, *advisorapi.GetAdviceRequest, ...grpc.CallOption) (*advisorapi.GetAdviceResponse, error) {
	return nil, errors.New("not implemented")
}

func TestDynamicPolicyLifecycleGatePrecedesRequestValidation(t *testing.T) {
	t.Parallel()

	p := &DynamicPolicy{lifecycleState: policyLifecycleRecovering}
	assertRecovering := func(err error) {
		t.Helper()
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovering")
	}

	_, err := p.GetTopologyHints(context.Background(), nil)
	assertRecovering(err)
	_, err = p.Allocate(context.Background(), nil)
	assertRecovering(err)
	_, err = p.RemovePod(context.Background(), nil)
	assertRecovering(err)
	_, err = p.GetResourcesAllocation(context.Background(), nil)
	assertRecovering(err)
}

func TestDynamicPolicyLifecycleUnknownFailsClosed(t *testing.T) {
	t.Parallel()

	p := &DynamicPolicy{}
	err := p.requireReady()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown")
}

func TestDynamicPolicyTestHelperIsExplicitlyReady(t *testing.T) {
	topologyInfo, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topologyInfo, t.TempDir())
	require.NoError(t, err)
	require.Equal(t, policyLifecycleReady, p.lifecycleState)
	require.NoError(t, p.requireReady())
}

func TestStartComponentsNthFailureRollsBackInReverseAndRetryHasNoDuplicates(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	events := make([]string, 0)
	active := map[string]int{}
	failThird := true
	component := func(name string, ordinal int) policyStartupComponent {
		return policyStartupComponent{
			name: name,
			start: func() (policyComponentStopper, error) {
				mu.Lock()
				defer mu.Unlock()
				events = append(events, "start-"+name)
				if ordinal == 3 && failThird {
					return policyComponentStopper{}, errors.New("third worker failed")
				}
				active[name]++
				return policyComponentStopper{
					name: name,
					stop: func() error {
						mu.Lock()
						defer mu.Unlock()
						events = append(events, "stop-"+name)
						active[name]--
						return nil
					},
				}, nil
			},
		}
	}
	components := []policyStartupComponent{
		component("periodical", 1),
		component("irq", 2),
		component("advisor", 3),
		component("optimizers", 4),
	}
	p := &DynamicPolicy{lifecycleState: policyLifecycleRecovering}

	err := p.startComponents(components)
	require.ErrorContains(t, err, "third worker failed")
	require.Equal(t, []string{
		"start-periodical", "start-irq", "start-advisor",
		"stop-irq", "stop-periodical",
	}, events)
	require.Equal(t, map[string]int{"periodical": 0, "irq": 0}, active)
	require.Empty(t, p.startedComponentStoppers)

	failThird = false
	events = nil
	require.NoError(t, p.startComponents(components))
	require.Equal(t, map[string]int{
		"periodical": 1,
		"irq":        1,
		"advisor":    1,
		"optimizers": 1,
	}, active)
	require.Len(t, p.startedComponentStoppers, len(components))
	require.NoError(t, p.stopStartedComponents())
	require.Equal(t, []string{
		"start-periodical", "start-irq", "start-advisor", "start-optimizers",
		"stop-optimizers", "stop-advisor", "stop-irq", "stop-periodical",
	}, events)
	for name, count := range active {
		require.Zero(t, count, "%s remained registered after stop", name)
	}
}

func TestStopClosesLifecycleGateBeforeConcurrentCleanup(t *testing.T) {
	t.Parallel()

	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	p := &DynamicPolicy{
		started:        true,
		lifecycleState: policyLifecycleReady,
		startedComponentStoppers: []policyComponentStopper{{
			name: "blocking",
			stop: func() error {
				close(cleanupStarted)
				<-releaseCleanup
				return nil
			},
		}},
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- p.Stop()
	}()
	<-cleanupStarted

	gateDone := make(chan error, 1)
	go func() {
		gateDone <- p.requireReady()
	}()
	select {
	case err := <-gateDone:
		require.ErrorContains(t, err, "recovering")
	case <-time.After(time.Second):
		t.Fatal("lifecycle gate remained blocked behind component cleanup")
	}

	close(releaseCleanup)
	require.NoError(t, <-stopDone)
}

func TestMutationGateChecksReadinessOnlyAfterOwningMainLock(t *testing.T) {
	t.Parallel()

	p := &DynamicPolicy{lifecycleState: policyLifecycleReady}
	p.Lock()
	gateDone := make(chan error, 1)
	go func() {
		err := p.lockReadyForMutation()
		if err == nil {
			p.Unlock()
		}
		gateDone <- err
	}()

	p.lifecycleState = policyLifecycleBlocked
	p.lifecycleErr = errors.New("stop won the race")
	p.Unlock()

	err := <-gateDone
	require.ErrorContains(t, err, "blocked")
	require.ErrorContains(t, err, "stop won the race")
}

func TestDynamicPolicyStartFailureCleansRegisteredComponentsAndRetryIsIdempotent(t *testing.T) {
	topologyInfo, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topologyInfo, t.TempDir())
	require.NoError(t, err)

	irq := &lifecycleRecordingIRQTuner{}
	p.irqTuner = irq
	p.enableSyncingCPUIdle = true
	p.reclaimRelativeRootCgroupPaths = nil

	err = p.Start()
	require.ErrorContains(t, err, "not set reclaiemd relative root cgroup path")
	require.Equal(t, policyLifecycleBlocked, p.lifecycleState)
	require.Empty(t, p.startedComponentStoppers)
	_, stopCount := irq.counts()
	require.Equal(t, 1, stopCount)

	p.enableSyncingCPUIdle = false
	require.NoError(t, p.Start())
	stopperCount := len(p.startedComponentStoppers)
	require.NotZero(t, stopperCount)
	require.NoError(t, p.Start())
	require.Len(t, p.startedComponentStoppers, stopperCount, "idempotent Start must not register duplicate components")
	require.NoError(t, p.Stop())
	require.Empty(t, p.startedComponentStoppers)
	_, stopCount = irq.counts()
	require.Equal(t, 2, stopCount)
}

func TestGetTopologyHintsHoldsStableReadLockThroughHandler(t *testing.T) {
	topologyInfo, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topologyInfo, t.TempDir())
	require.NoError(t, err)

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	p.hintHandlers[apiconsts.PodAnnotationQoSLevelSharedCores] = func(
		context.Context, *pluginapi.ResourceRequest,
	) (*pluginapi.ResourceHintsResponse, error) {
		close(handlerEntered)
		<-releaseHandler
		return &pluginapi.ResourceHintsResponse{}, nil
	}
	req := &pluginapi.ResourceRequest{
		PodUid:        "stable-read",
		PodNamespace:  "default",
		PodName:       "stable-read",
		ContainerName: "main",
		ContainerType: pluginapi.ContainerType_MAIN,
		ResourceName:  string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 1,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
	}

	hintsDone := make(chan error, 1)
	go func() {
		_, callErr := p.GetTopologyHints(context.Background(), req)
		hintsDone <- callErr
	}()
	<-handlerEntered

	writerAcquired := make(chan struct{})
	go func() {
		p.Lock()
		close(writerAcquired)
		p.Unlock()
	}()
	select {
	case <-writerAcquired:
		t.Fatal("writer entered while topology hint handler was reading policy state")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseHandler)
	require.NoError(t, <-hintsDone)
	select {
	case <-writerAcquired:
	case <-time.After(time.Second):
		t.Fatal("writer did not acquire policy lock after topology hint read completed")
	}
}

func TestDynamicPolicyStartHardRecoveryFailureStopsAfterOneAttemptAndBlocksAllMutations(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	tmpDir := t.TempDir()

	p, err := getTestDynamicPolicyWithInitialization(topology, tmpDir)
	require.NoError(t, err)

	recoveryErr := errors.New("bootstrap did not fully converge")
	events := []string{}
	materializer := &transactionRecordingMaterializer{
		events: &events,
		errs:   []error{recoveryErr, recoveryErr, recoveryErr},
	}
	p.cpuSetMaterializer = materializer

	err = p.Start()
	require.ErrorIs(t, err, recoveryErr)
	require.Len(t, materializer.targets, 1, "hard recovery errors must not be retried")

	assertBlocked := func(err error) {
		t.Helper()
		require.Error(t, err)
		require.Contains(t, err.Error(), "blocked")
	}
	_, err = p.GetTopologyHints(context.Background(), nil)
	assertBlocked(err)
	_, err = p.Allocate(context.Background(), nil)
	assertBlocked(err)
	_, err = p.RemovePod(context.Background(), nil)
	assertBlocked(err)
	_, err = p.GetResourcesAllocation(context.Background(), nil)
	assertBlocked(err)
	assertBlocked(p.allocateByCPUAdvisor(nil, nil, nil))
	assertBlocked(p.SetExclusiveIRQCPUSet(machine.NewCPUSet(1)))
}

func TestDynamicPolicyStartRecoversBeforeReady(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	tmpDir := t.TempDir()

	p, err := getTestDynamicPolicyWithInitialization(topology, tmpDir)
	require.NoError(t, err)
	events := []string{}
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{Converged: true}},
		onCall: func(cpusetmaterializer.Target) {
			require.Equal(t, policyLifecycleRecovering, p.lifecycleState,
				"committed target must converge before the policy becomes ready")
		},
	}
	p.cpuSetMaterializer = materializer

	require.NoError(t, p.Start())
	t.Cleanup(func() {
		require.NoError(t, p.Stop())
	})

	_, err = p.GetTopologyHints(context.Background(), nil)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "nil req") && !strings.Contains(err.Error(), "recovering"),
		"ready policy should reach request validation, got %v", err)
}

func TestDynamicPolicyStartRetriesDeferredBootstrapConvergence(t *testing.T) {
	topologyInfo, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)

	p, err := getTestDynamicPolicyWithInitialization(topologyInfo, t.TempDir())
	require.NoError(t, err)
	recoveryCalls := 0
	events := []string{}
	materializer := &transactionRecordingMaterializer{
		events: &events,
		results: []cpusetmaterializer.Result{
			{Converged: false},
			{Converged: true},
		},
	}
	p.cpuSetMaterializer = materializer

	require.NoError(t, p.Start())
	t.Cleanup(func() {
		require.NoError(t, p.Stop())
	})
	recoveryCalls = len(materializer.targets)
	require.Equal(t, 2, recoveryCalls)
}

func TestLifecycleRPCsReturnPrepareCheckpointError(t *testing.T) {
	topologyInfo, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topologyInfo, t.TempDir())
	require.NoError(t, err)

	checkpointErr := errors.New("prepare checkpoint failed")
	p.state = &advisorTargetRecordingState{State: p.state, prepareErr: checkpointErr}

	_, err = p.GetResourcesAllocation(context.Background(), &pluginapi.GetResourcesAllocationRequest{})
	require.ErrorIs(t, err, checkpointErr)

	_, err = p.RemovePod(context.Background(), &pluginapi.RemovePodRequest{PodUid: "missing"})
	require.ErrorIs(t, err, checkpointErr)

	_, err = p.Allocate(context.Background(), &pluginapi.ResourceRequest{
		PodUid:        "shared-pod",
		PodNamespace:  "default",
		PodName:       "shared-pod",
		ContainerName: "main",
		ContainerType: pluginapi.ContainerType_MAIN,
		ResourceName:  string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
	})
	require.ErrorIs(t, err, checkpointErr)
}

func TestAllocateAdvisorFailureAfterCommitDoesNotRollback(t *testing.T) {
	topologyInfo, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topologyInfo, t.TempDir())
	require.NoError(t, err)
	advisor := &failingAddContainerAdvisor{}
	p.enableCPUAdvisor = true
	p.advisorClient = advisor
	p.startAdvisorPostCommitWorker()
	t.Cleanup(p.stopAdvisorPostCommitWorker)

	req := &pluginapi.ResourceRequest{
		PodUid:        "advisor-failure",
		PodNamespace:  "default",
		PodName:       "advisor-failure",
		ContainerName: "main",
		ContainerType: pluginapi.ContainerType_MAIN,
		ResourceName:  string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
	}

	resp, err := p.Allocate(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Eventually(t, func() bool {
		return advisor.attemptCount() >= 3
	}, time.Second, 10*time.Millisecond)
	require.NotNil(t, p.state.GetAllocationInfo(req.PodUid, req.ContainerName),
		"post-commit advisor failure must not roll back allocation")
}

func TestRestoreBaseOrBlockUsesIndependentContextWhenRequestCanceled(t *testing.T) {
	topologyInfo, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topologyInfo, t.TempDir())
	require.NoError(t, err)
	base, err := p.state.PrepareDurableTarget()
	require.NoError(t, err)

	restoreErr := errors.New("restore failed")
	events := []string{}
	materializer := &transactionRecordingMaterializer{
		events: &events,
		errs:   []error{restoreErr},
	}
	p.cpuSetMaterializer = materializer
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	cause := errors.New("candidate failed")
	startedAt := time.Now()

	err = p.restoreBaseOrBlock(requestCtx, base, cause)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, restoreErr)
	require.Len(t, materializer.contexts, 1)
	require.NoError(t, materializer.contextErrs[0])
	deadline, ok := materializer.contexts[0].Deadline()
	require.True(t, ok)
	require.WithinDuration(t, startedAt.Add(restoreBaseTimeout), deadline, 250*time.Millisecond)
	require.NotEqual(t, requestCtx, materializer.contexts[0])
	require.Equal(t, []string{"materialize"}, events)
	require.Equal(t, policyLifecycleBlocked, p.lifecycleState)
	require.ErrorContains(t, p.requireReady(), "blocked")
}
