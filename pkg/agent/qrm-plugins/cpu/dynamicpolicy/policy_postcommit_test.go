/*
Copyright 2026 The Katalyst Authors.

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
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type blockingPostCommitAdvisor struct {
	advisorapi.CPUAdvisorClient

	addEntered    chan struct{}
	addRelease    chan struct{}
	removeEntered chan struct{}
	removeRelease chan struct{}

	mu             sync.Mutex
	addAttempts    int
	removeAttempts int
	addErr         error
	removeErr      error
	waitForContext bool
	addRequests    []*advisorsvc.ContainerMetadata
	removeRequests []*advisorsvc.RemovePodRequest
	outgoingMD     []metadata.MD
	deadlines      []time.Time
}

func (a *blockingPostCommitAdvisor) AddContainer(
	ctx context.Context,
	req *advisorsvc.ContainerMetadata,
	_ ...grpc.CallOption,
) (*advisorsvc.AddContainerResponse, error) {
	a.mu.Lock()
	a.addAttempts++
	attempt := a.addAttempts
	a.addRequests = append(a.addRequests, proto.Clone(req).(*advisorsvc.ContainerMetadata))
	md, _ := metadata.FromOutgoingContext(ctx)
	a.outgoingMD = append(a.outgoingMD, md.Copy())
	deadline, _ := ctx.Deadline()
	a.deadlines = append(a.deadlines, deadline)
	a.mu.Unlock()
	if attempt == 1 && a.addEntered != nil {
		close(a.addEntered)
	}
	if a.addRelease != nil {
		<-a.addRelease
	}
	if a.waitForContext {
		<-ctx.Done()
	}
	return &advisorsvc.AddContainerResponse{}, a.addErr
}

func (a *blockingPostCommitAdvisor) RemovePod(
	ctx context.Context,
	req *advisorsvc.RemovePodRequest,
	_ ...grpc.CallOption,
) (*advisorsvc.RemovePodResponse, error) {
	a.mu.Lock()
	a.removeAttempts++
	attempt := a.removeAttempts
	a.removeRequests = append(a.removeRequests, proto.Clone(req).(*advisorsvc.RemovePodRequest))
	md, _ := metadata.FromOutgoingContext(ctx)
	a.outgoingMD = append(a.outgoingMD, md.Copy())
	deadline, _ := ctx.Deadline()
	a.deadlines = append(a.deadlines, deadline)
	a.mu.Unlock()
	if attempt == 1 && a.removeEntered != nil {
		close(a.removeEntered)
	}
	if a.removeRelease != nil {
		<-a.removeRelease
	}
	if a.waitForContext {
		<-ctx.Done()
	}
	return &advisorsvc.RemovePodResponse{}, a.removeErr
}

func newPostCommitTestPolicy(t *testing.T, advisor advisorapi.CPUAdvisorClient) *DynamicPolicy {
	t.Helper()
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	policy, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	policy.enableCPUAdvisor = true
	policy.advisorClient = advisor
	return policy
}

func postCommitAllocationRequest(uid string) *pluginapi.ResourceRequest {
	return &pluginapi.ResourceRequest{
		PodUid:        uid,
		PodNamespace:  "default",
		PodName:       uid,
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
}

func TestAllocateReturnsWithoutWaitingForPostCommitAdvisorAdd(t *testing.T) {
	advisor := &blockingPostCommitAdvisor{
		addEntered: make(chan struct{}),
		addRelease: make(chan struct{}),
	}
	policy := newPostCommitTestPolicy(t, advisor)
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)
	done := make(chan error, 1)

	go func() {
		_, err := policy.Allocate(context.Background(), postCommitAllocationRequest("async-add"))
		done <- err
	}()
	<-advisor.addEntered

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		close(advisor.addRelease)
		require.NoError(t, <-done)
		t.Fatal("Allocate waited for post-commit advisor AddContainer")
	}
	close(advisor.addRelease)
}

func TestRemovePodReturnsWithoutWaitingForPostCommitAdvisorRemove(t *testing.T) {
	advisor := &blockingPostCommitAdvisor{
		removeEntered: make(chan struct{}),
		removeRelease: make(chan struct{}),
	}
	policy := newPostCommitTestPolicy(t, advisor)
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)
	done := make(chan error, 1)

	go func() {
		_, err := policy.RemovePod(context.Background(), &pluginapi.RemovePodRequest{PodUid: "async-remove"})
		done <- err
	}()
	<-advisor.removeEntered

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		close(advisor.removeRelease)
		require.NoError(t, <-done)
		t.Fatal("RemovePod waited for post-commit advisor RemovePod")
	}
	close(advisor.removeRelease)
}

func TestPushCPUAdvisorEnqueuesCommittedSnapshotWithoutDirectRPC(t *testing.T) {
	advisor := &outboxRecordingAdvisor{}
	policy := newPostCommitTestPolicy(t, advisor)
	policy.enableCPUAdvisor = false
	_, err := policy.Allocate(context.Background(), postCommitAllocationRequest("snapshot-pod"))
	require.NoError(t, err)
	policy.enableCPUAdvisor = true

	require.NoError(t, policy.pushCPUAdvisor())
	calls, _ := advisor.snapshot()
	require.Empty(t, calls, "pushCPUAdvisor must not call AddContainer directly")

	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)
	require.Equal(t, []recordedAdvisorCall{{
		operation:     "add",
		podUID:        "snapshot-pod",
		containerName: "main",
	}}, waitForAdvisorCalls(t, advisor, 1))
}

type recordedAdvisorCall struct {
	operation     string
	podUID        string
	containerName string
}

type outboxRecordingAdvisor struct {
	advisorapi.CPUAdvisorClient

	mu            sync.Mutex
	calls         []recordedAdvisorCall
	active        int
	maxActive     int
	addAttempts   int
	addEntered    chan struct{}
	addRelease    chan struct{}
	addFailures   int
	waitForCancel bool
	cancelled     chan struct{}
	deadlines     []time.Time
	callTimes     []time.Time
}

func (a *outboxRecordingAdvisor) AddContainer(
	ctx context.Context,
	req *advisorsvc.ContainerMetadata,
	_ ...grpc.CallOption,
) (*advisorsvc.AddContainerResponse, error) {
	a.mu.Lock()
	a.active++
	if a.active > a.maxActive {
		a.maxActive = a.active
	}
	a.addAttempts++
	attempt := a.addAttempts
	a.calls = append(a.calls, recordedAdvisorCall{
		operation:     "add",
		podUID:        req.PodUid,
		containerName: req.ContainerName,
	})
	deadline, _ := ctx.Deadline()
	a.deadlines = append(a.deadlines, deadline)
	a.callTimes = append(a.callTimes, time.Now())
	if attempt == 1 && a.addEntered != nil {
		close(a.addEntered)
	}
	a.mu.Unlock()

	if a.addRelease != nil {
		<-a.addRelease
	}
	if a.waitForCancel {
		<-ctx.Done()
		close(a.cancelled)
	}

	a.mu.Lock()
	a.active--
	shouldFail := attempt <= a.addFailures
	a.mu.Unlock()
	if shouldFail {
		return nil, errors.New("temporary add failure")
	}
	return &advisorsvc.AddContainerResponse{}, nil
}

func (a *outboxRecordingAdvisor) RemovePod(
	_ context.Context,
	req *advisorsvc.RemovePodRequest,
	_ ...grpc.CallOption,
) (*advisorsvc.RemovePodResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active++
	if a.active > a.maxActive {
		a.maxActive = a.active
	}
	a.calls = append(a.calls, recordedAdvisorCall{operation: "remove", podUID: req.PodUid})
	a.active--
	return &advisorsvc.RemovePodResponse{}, nil
}

func (a *outboxRecordingAdvisor) snapshot() ([]recordedAdvisorCall, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]recordedAdvisorCall(nil), a.calls...), a.maxActive
}

func waitForAdvisorCalls(t *testing.T, advisor *outboxRecordingAdvisor, count int) []recordedAdvisorCall {
	t.Helper()
	require.Eventually(t, func() bool {
		calls, _ := advisor.snapshot()
		return len(calls) >= count
	}, time.Second, time.Millisecond)
	calls, _ := advisor.snapshot()
	return calls
}

func waitForPolicyWriteLockHeld(t *testing.T, policy *DynamicPolicy) {
	t.Helper()
	require.Eventually(t, func() bool {
		if !policy.TryLock() {
			return true
		}
		policy.Unlock()
		return false
	}, time.Second, time.Millisecond)
}

func TestAllocateCommitDelayedEnqueueCannotBeOvertakenByRemoveCommit(t *testing.T) {
	advisor := &outboxRecordingAdvisor{}
	policy := newPostCommitTestPolicy(t, advisor)
	outbox := policy.advisorPostCommitOutboxInstance()
	outbox.mu.Lock()

	allocateDone := make(chan error, 1)
	go func() {
		_, err := policy.Allocate(context.Background(), postCommitAllocationRequest("ordered-pod"))
		allocateDone <- err
	}()
	require.Eventually(t, func() bool {
		return policy.state.GetAllocationInfo("ordered-pod", "main") != nil
	}, time.Second, time.Millisecond, "Allocate never committed")
	waitForPolicyWriteLockHeld(t, policy)

	removeDone := make(chan error, 1)
	go func() {
		_, err := policy.RemovePod(context.Background(), &pluginapi.RemovePodRequest{PodUid: "ordered-pod"})
		removeDone <- err
	}()
	select {
	case err := <-removeDone:
		t.Fatalf("RemovePod overtook the committed Add callback: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	outbox.mu.Unlock()
	require.NoError(t, <-allocateDone)
	require.NoError(t, <-removeDone)
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)

	require.Equal(t, []recordedAdvisorCall{{operation: "remove", podUID: "ordered-pod"}},
		waitForAdvisorCalls(t, advisor, 1))
}

func TestPushSnapshotAndConcurrentRemovePreserveCommitOrder(t *testing.T) {
	advisor := &outboxRecordingAdvisor{}
	policy := newPostCommitTestPolicy(t, advisor)
	policy.enableCPUAdvisor = false
	_, err := policy.Allocate(context.Background(), postCommitAllocationRequest("snapshot-remove-pod"))
	require.NoError(t, err)
	policy.enableCPUAdvisor = true

	outbox := policy.advisorPostCommitOutboxInstance()
	outbox.mu.Lock()
	pushDone := make(chan error, 1)
	go func() {
		pushDone <- policy.pushCPUAdvisor()
	}()
	waitForPolicyWriteLockHeld(t, policy)

	removeDone := make(chan error, 1)
	go func() {
		_, removeErr := policy.RemovePod(context.Background(), &pluginapi.RemovePodRequest{PodUid: "snapshot-remove-pod"})
		removeDone <- removeErr
	}()
	select {
	case removeErr := <-removeDone:
		t.Fatalf("RemovePod committed while snapshot enqueue still held the policy read lock: %v", removeErr)
	case <-time.After(50 * time.Millisecond):
	}

	outbox.mu.Unlock()
	require.NoError(t, <-pushDone)
	require.NoError(t, <-removeDone)
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)

	require.Equal(t, []recordedAdvisorCall{{operation: "remove", podUID: "snapshot-remove-pod"}},
		waitForAdvisorCalls(t, advisor, 1))
}

func TestAdvisorOutboxMergesPendingEventsByPod(t *testing.T) {
	advisor := &outboxRecordingAdvisor{}
	policy := &DynamicPolicy{advisorClient: advisor}

	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod", ContainerName: "main"})
	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod", ContainerName: "sidecar"})
	policy.enqueueAdvisorRemove("pod")
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)

	require.Equal(t, []recordedAdvisorCall{{operation: "remove", podUID: "pod"}},
		waitForAdvisorCalls(t, advisor, 1))
}

func TestAdvisorOutboxPreservesContainerAddsAndSerializesDelivery(t *testing.T) {
	advisor := &outboxRecordingAdvisor{}
	policy := &DynamicPolicy{advisorClient: advisor}

	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-a", ContainerName: "main"})
	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-a", ContainerName: "sidecar"})
	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-b", ContainerName: "main"})
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)

	calls := waitForAdvisorCalls(t, advisor, 3)
	require.Equal(t, []recordedAdvisorCall{
		{operation: "add", podUID: "pod-a", containerName: "main"},
		{operation: "add", podUID: "pod-a", containerName: "sidecar"},
		{operation: "add", podUID: "pod-b", containerName: "main"},
	}, calls)
	_, maxActive := advisor.snapshot()
	require.Equal(t, 1, maxActive)
}

func TestAdvisorOutboxSendsGlobalMinimumRevisionAcrossPods(t *testing.T) {
	advisor := &outboxRecordingAdvisor{}
	policy := &DynamicPolicy{advisorClient: advisor}

	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-a", ContainerName: "a1"})
	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-b", ContainerName: "b2"})
	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-a", ContainerName: "a3"})
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)

	require.Equal(t, []recordedAdvisorCall{
		{operation: "add", podUID: "pod-a", containerName: "a1"},
		{operation: "add", podUID: "pod-b", containerName: "b2"},
		{operation: "add", podUID: "pod-a", containerName: "a3"},
	}, waitForAdvisorCalls(t, advisor, 3))
}

func TestAdvisorOutboxRemoveOnlyDropsEventsCoveredByItsRevision(t *testing.T) {
	outbox := newAdvisorPostCommitOutbox(func(context.Context, advisorPostCommitEvent) error { return nil })
	outbox.nextRevision = 1
	outbox.pending["pod"] = []advisorPostCommitEvent{
		{revision: 1, operation: advisorPostCommitAdd, podUID: "pod"},
		{revision: 3, operation: advisorPostCommitAdd, podUID: "pod"},
	}
	outbox.queuePodLocked("pod")

	outbox.enqueueRemove("pod")

	require.Equal(t, []advisorPostCommitEvent{
		{revision: 2, operation: advisorPostCommitRemove, podUID: "pod"},
		{revision: 3, operation: advisorPostCommitAdd, podUID: "pod"},
	}, outbox.pending["pod"])
}

func TestAdvisorOutboxFailedStaleRevisionCannotDeleteCoveringRemove(t *testing.T) {
	advisor := &outboxRecordingAdvisor{
		addEntered:  make(chan struct{}),
		addRelease:  make(chan struct{}),
		addFailures: 1,
	}
	policy := &DynamicPolicy{advisorClient: advisor}
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)

	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod", ContainerName: "main"})
	<-advisor.addEntered
	policy.enqueueAdvisorRemove("pod")
	close(advisor.addRelease)

	calls := waitForAdvisorCalls(t, advisor, 2)
	require.Equal(t, []recordedAdvisorCall{
		{operation: "add", podUID: "pod", containerName: "main"},
		{operation: "remove", podUID: "pod"},
	}, calls)
}

func TestAdvisorOutboxSuccessfulStaleRevisionCannotDeleteCoveringRemove(t *testing.T) {
	advisor := &outboxRecordingAdvisor{
		addEntered: make(chan struct{}),
		addRelease: make(chan struct{}),
	}
	policy := &DynamicPolicy{advisorClient: advisor}
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)

	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod", ContainerName: "main"})
	<-advisor.addEntered
	policy.enqueueAdvisorRemove("pod")
	close(advisor.addRelease)

	calls := waitForAdvisorCalls(t, advisor, 2)
	require.Equal(t, []recordedAdvisorCall{
		{operation: "add", podUID: "pod", containerName: "main"},
		{operation: "remove", podUID: "pod"},
	}, calls)
}

func TestAdvisorOutboxRetriesUntilRecovery(t *testing.T) {
	advisor := &outboxRecordingAdvisor{addFailures: 2}
	policy := &DynamicPolicy{advisorClient: advisor}
	policy.startAdvisorPostCommitWorker()
	t.Cleanup(policy.stopAdvisorPostCommitWorker)

	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod", ContainerName: "main"})
	calls := waitForAdvisorCalls(t, advisor, 3)
	require.Equal(t, []recordedAdvisorCall{
		{operation: "add", podUID: "pod", containerName: "main"},
		{operation: "add", podUID: "pod", containerName: "main"},
		{operation: "add", podUID: "pod", containerName: "main"},
	}, calls)
	advisor.mu.Lock()
	defer advisor.mu.Unlock()
	require.Len(t, advisor.deadlines, 3)
	require.Len(t, advisor.callTimes, 3)
	for i, deadline := range advisor.deadlines {
		require.False(t, deadline.IsZero())
		if i > 0 {
			require.True(t, deadline.After(advisor.deadlines[i-1]),
				"each retry must receive a fresh independent deadline")
			require.GreaterOrEqual(t,
				advisor.callTimes[i].Sub(advisor.callTimes[i-1]),
				advisorPostCommitRetryDelay,
				"each failed attempt must back off before retrying")
		}
	}
}

func TestAdvisorOutboxRetryExpiringAfterScanIsRetriedImmediately(t *testing.T) {
	base := time.Unix(1, 0)
	now := base
	sent := make(chan advisorPostCommitEvent, 1)
	outbox := newAdvisorPostCommitOutbox(func(_ context.Context, event advisorPostCommitEvent) error {
		sent <- event
		return nil
	})
	outbox.now = func() time.Time {
		return now
	}
	outbox.afterRetryScan = func() {
		now = base.Add(advisorPostCommitRetryDelay)
		outbox.afterRetryScan = nil
	}
	event := advisorPostCommitEvent{
		revision:  1,
		operation: advisorPostCommitAdd,
		podUID:    "pod",
		add:       &advisorsvc.ContainerMetadata{PodUid: "pod", ContainerName: "main"},
	}
	outbox.pending[event.podUID] = []advisorPostCommitEvent{event}
	outbox.queuePodLocked(event.podUID)
	outbox.retries[event.podUID] = advisorPostCommitRetry{
		revision: event.revision,
		backoff:  advisorPostCommitRetryDelay,
		readyAt:  base.Add(advisorPostCommitRetryDelay),
	}

	outbox.start()
	t.Cleanup(outbox.stop)

	select {
	case got := <-sent:
		require.Equal(t, event, got)
	case <-time.After(time.Second):
		t.Fatal("retry that expired after scanning was left asleep without a timer")
	}
}

func TestAdvisorOutboxTransientBackoffDoesNotBlockAnotherPod(t *testing.T) {
	var (
		mu        sync.Mutex
		attempts  = make(map[string]int)
		calls     []recordedAdvisorCall
		callTimes []time.Time
	)
	outbox := newAdvisorPostCommitOutbox(func(_ context.Context, event advisorPostCommitEvent) error {
		mu.Lock()
		defer mu.Unlock()
		attempts[event.podUID]++
		calls = append(calls, recordedAdvisorCall{
			operation:     string(event.operation),
			podUID:        event.podUID,
			containerName: event.containerName(),
		})
		callTimes = append(callTimes, time.Now())
		if event.podUID == "pod-a" && attempts[event.podUID] == 1 {
			return status.Error(codes.Unavailable, "retry pod-a")
		}
		return nil
	})
	outbox.enqueueAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-a", ContainerName: "a1"})
	outbox.enqueueAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-a", ContainerName: "a2"})
	outbox.enqueueAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-b", ContainerName: "b1"})
	outbox.start()
	t.Cleanup(outbox.stop)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) >= 4
	}, time.Second, time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []recordedAdvisorCall{
		{operation: "add", podUID: "pod-a", containerName: "a1"},
		{operation: "add", podUID: "pod-b", containerName: "b1"},
		{operation: "add", podUID: "pod-a", containerName: "a1"},
		{operation: "add", podUID: "pod-a", containerName: "a2"},
	}, calls)
	require.GreaterOrEqual(t, callTimes[2].Sub(callTimes[0]), advisorPostCommitRetryDelay,
		"processing another pod must not bypass pod-a's retry backoff")
}

func TestAdvisorOutboxPermanentFailureDeadLettersAndDoesNotBlockPods(t *testing.T) {
	var (
		mu          sync.Mutex
		calls       []recordedAdvisorCall
		deadLetters []advisorPostCommitEvent
	)
	outbox := newAdvisorPostCommitOutbox(
		func(_ context.Context, event advisorPostCommitEvent) error {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, recordedAdvisorCall{
				operation:     string(event.operation),
				podUID:        event.podUID,
				containerName: event.containerName(),
			})
			if event.podUID == "pod-a" {
				return status.Error(codes.InvalidArgument, "reject pod-a")
			}
			return nil
		},
	)
	outbox.deadLetter = func(event advisorPostCommitEvent, _ error) {
		mu.Lock()
		defer mu.Unlock()
		deadLetters = append(deadLetters, event)
	}
	outbox.enqueueAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-a", ContainerName: "a1"})
	outbox.enqueueAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-a", ContainerName: "a2"})
	outbox.enqueueAdd(&advisorsvc.ContainerMetadata{PodUid: "pod-b", ContainerName: "b1"})
	outbox.start()
	t.Cleanup(outbox.stop)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) >= 3 && len(deadLetters) == 2
	}, time.Second, time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []recordedAdvisorCall{
		{operation: "add", podUID: "pod-a", containerName: "a1"},
		{operation: "add", podUID: "pod-a", containerName: "a2"},
		{operation: "add", podUID: "pod-b", containerName: "b1"},
	}, calls)
	require.Equal(t, []uint64{1, 2}, []uint64{deadLetters[0].revision, deadLetters[1].revision})
}

func TestObserveAdvisorPostCommitDeadLetterEmitsGRPCCode(t *testing.T) {
	emitter := NewMockMetricsEmitter()
	policy := &DynamicPolicy{emitter: emitter}
	event := advisorPostCommitEvent{
		revision:  7,
		operation: advisorPostCommitAdd,
		podUID:    "pod",
		add:       &advisorsvc.ContainerMetadata{PodUid: "pod", ContainerName: "main"},
	}

	policy.observeAdvisorPostCommitDeadLetter(
		event,
		status.Error(codes.InvalidArgument, "rejected"),
	)

	require.Equal(t, []int64{1}, emitter.storedInt64[util.MetricNameAdvisorPostCommitNotification])
	require.Equal(t, [][]metrics.MetricTag{{{
		Key: "operation", Val: "add",
	}, {
		Key: "status", Val: "dead_lettered",
	}, {
		Key: "revision", Val: "7",
	}, {
		Key: "grpc_code", Val: codes.InvalidArgument.String(),
	}}}, emitter.storedTags[util.MetricNameAdvisorPostCommitNotification])
}

func TestAdvisorOutboxStopCancelsInFlightRPCAndWaits(t *testing.T) {
	advisor := &outboxRecordingAdvisor{
		addEntered:    make(chan struct{}),
		waitForCancel: true,
		cancelled:     make(chan struct{}),
	}
	policy := &DynamicPolicy{advisorClient: advisor}
	policy.startAdvisorPostCommitWorker()
	policy.enqueueAdvisorAdd(&advisorsvc.ContainerMetadata{PodUid: "pod", ContainerName: "main"})
	<-advisor.addEntered

	stopped := make(chan struct{})
	go func() {
		policy.stopAdvisorPostCommitWorker()
		close(stopped)
	}()

	select {
	case <-advisor.cancelled:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the in-flight advisor RPC")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not wait for the advisor outbox worker")
	}
}
