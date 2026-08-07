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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/config"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	podmeta "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type cacheSyncRegistrarPodFetcher struct {
	*podmeta.PodFetcherStub
	events       chan podmeta.KubeletPodCacheSyncEvent
	unregistered chan struct{}
}

type cpusetOverrideCommitGuardState struct {
	state.State
	unconditionalCommitCalls int
	conditionalCommitCalls   int
	conditionalRevision      uint64
}

func (s *cpusetOverrideCommitGuardState) CommitAdvisorState(
	state.PodEntries,
	state.NUMANodeMap,
	bool,
	bool,
	bool,
) error {
	s.unconditionalCommitCalls++
	return fmt.Errorf("cpuset adjustment override must use CommitAdvisorStateIfRevision")
}

func (s *cpusetOverrideCommitGuardState) CommitAdvisorStateIfRevision(
	expectedRevision uint64,
	podEntries state.PodEntries,
	machineState state.NUMANodeMap,
	allowOverlap bool,
	disableDedicatedOverlap bool,
	persist bool,
) error {
	s.conditionalCommitCalls++
	s.conditionalRevision = expectedRevision
	return s.State.CommitAdvisorStateIfRevision(
		expectedRevision, podEntries, machineState, allowOverlap, disableDedicatedOverlap, persist)
}

func (f *cacheSyncRegistrarPodFetcher) RegisterKubeletPodCacheSyncListener(string) (
	<-chan podmeta.KubeletPodCacheSyncEvent, func(),
) {
	return f.events, func() {
		select {
		case <-f.unregistered:
		default:
			close(f.unregistered)
		}
	}
}

func TestCPUSetAdjustmentHandlerTimeoutCoversTopologyConvergenceBudget(t *testing.T) {
	t.Parallel()

	conf := config.NewConfiguration()
	if cpuSetAdjustmentHandlerTimeout(conf) <= bulkheadconfig.DefaultTopologyConvergenceDeadline {
		t.Fatalf("outer cpuset adjustment timeout %s must exceed topology convergence budget %s",
			cpuSetAdjustmentHandlerTimeout(conf), bulkheadconfig.DefaultTopologyConvergenceDeadline)
	}
}

func TestCPUSetAdjustmentHandlerTimeoutDerivesFromConfiguredTopologyDeadline(t *testing.T) {
	t.Parallel()

	conf := config.NewConfiguration()
	conf.CPUQRMPluginConfig.BulkheadConfiguration.TopologyConvergenceBudget.DeadlineDuration = 750 * time.Millisecond
	got := cpuSetAdjustmentHandlerTimeout(conf)
	if got <= 750*time.Millisecond || got >= 15*time.Second {
		t.Fatalf("derived outer timeout = %s, want bounded margin above configured 750ms", got)
	}
}

func TestRunCPUSetAdjustmentHandlersPropagatesMode(t *testing.T) {
	t.Parallel()

	got := make(chan cpusetutil.CPUSetAdjustmentMode, 1)
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"mode": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				got <- in.Mode
				return nil
			},
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModeAdmission)
	p.Unlock()
	if err != nil {
		t.Fatalf("runCPUSetAdjustmentHandlers() error = %v", err)
	}
	if mode := <-got; mode != cpusetutil.CPUSetAdjustmentModeAdmission {
		t.Fatalf("handler mode = %q, want admission", mode)
	}
}

func TestDeferredFullRetryCoalescesQueuedRequestsIntoTrailingRetry(t *testing.T) {
	t.Parallel()

	retried := make(chan cpusetutil.CPUSetAdjustmentMode, 2)
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"retry": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				if in.Mode == cpusetutil.CPUSetAdjustmentModeAdmission {
					in.ScheduleFullRetry(cpusetutil.RetryReasonDeferredLeaf)
					in.ScheduleFullRetry(cpusetutil.RetryReasonDeferredLeaf)
					in.ScheduleFullRetry(cpusetutil.RetryReasonDeferredLeaf)
					return nil
				}
				retried <- in.Mode
				return nil
			},
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModeAdmission)
	p.Unlock()
	if err != nil {
		t.Fatalf("admission adjustment error = %v", err)
	}
	select {
	case mode := <-retried:
		if mode != cpusetutil.CPUSetAdjustmentModeRetry {
			t.Fatalf("async mode = %q, want retry", mode)
		}
	case <-time.After(time.Second):
		t.Fatal("deferred full retry was not executed")
	}
	select {
	case mode := <-retried:
		if mode != cpusetutil.CPUSetAdjustmentModeRetry {
			t.Fatalf("trailing mode = %q, want retry", mode)
		}
	case <-time.After(time.Second):
		t.Fatal("queued deferred requests were not coalesced into a trailing retry")
	}
	select {
	case mode := <-retried:
		t.Fatalf("queued deferred requests produced more than one trailing retry, extra mode=%q", mode)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDeferredFullRetryRetriesFailureWithBackoff(t *testing.T) {
	t.Parallel()

	attempts := make(chan time.Time, 2)
	attemptCount := 0
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"retry": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				if in.Mode != cpusetutil.CPUSetAdjustmentModeRetry {
					return nil
				}
				attemptCount++
				attempts <- time.Now()
				if attemptCount == 1 {
					return errors.New("transient retry failure")
				}
				return nil
			},
		},
	}

	p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonDeferredLeaf)
	first := <-attempts
	select {
	case second := <-attempts:
		if elapsed := second.Sub(first); elapsed < 10*time.Millisecond {
			t.Fatalf("retry failure was retried without backoff: elapsed=%s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("deferred latest-state retry was lost after a transient failure")
	}
}

func TestDeferredFullRetryExhaustionStaysDirtyUntilPeriodicLatestStateReconcile(t *testing.T) {
	t.Parallel()

	attempts := make(chan struct{}, 8)
	recovered := false
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"retry": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				if in.Mode == cpusetutil.CPUSetAdjustmentModeRetry {
					attempts <- struct{}{}
				}
				if recovered {
					return nil
				}
				return errors.New("persistent retry failure")
			},
		},
	}

	p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonDeferredLeaf)
	deadline := time.Now().Add(time.Second)
	for {
		p.cpuSetAdjustmentRetryMu.Lock()
		queued := p.cpuSetAdjustmentRetryQueued
		p.cpuSetAdjustmentRetryMu.Unlock()
		if !queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("persistent retry failure did not stop within the bounded retry window")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(attempts); got != 4 {
		t.Fatalf("retry attempts = %d, want bounded 4 attempts", got)
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	dirtyAfterExhaustion := p.cpuSetAdjustmentRetryDirty
	p.cpuSetAdjustmentRetryMu.Unlock()
	if !dirtyAfterExhaustion {
		t.Fatal("retry exhaustion cleared dirty latest-state reconciliation")
	}

	recovered = true
	p.runBulkheadPeriodicalHandlers(nil, nil, nil, nil, nil)
	p.cpuSetAdjustmentRetryMu.Lock()
	dirtyAfterPeriodic := p.cpuSetAdjustmentRetryDirty
	p.cpuSetAdjustmentRetryMu.Unlock()
	if dirtyAfterPeriodic {
		t.Fatal("successful periodic latest-state reconciliation did not clear dirty state")
	}
}

func TestDirtyCPUSetAdjustmentReconcileReturnsFailure(t *testing.T) {
	t.Parallel()

	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"persistent-failure": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				return fmt.Errorf("persistent failure")
			},
		},
		cpuSetAdjustmentRetryDirty: true,
	}

	if err := p.reconcileDirtyCPUSetAdjustment(); err == nil {
		t.Fatal("reconcileDirtyCPUSetAdjustment() error = nil, want persistent failure")
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if !p.cpuSetAdjustmentRetryDirty {
		t.Fatal("failed periodic reconciliation cleared dirty state")
	}
}

func TestPeriodicAdjustmentPreservesRetryScheduledDuringRound(t *testing.T) {
	t.Parallel()

	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{},
	}
	p.cpuSetAdjustmentHandlers["schedule-during-round"] = func(
		context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx,
	) error {
		p.cpuSetAdjustmentRetryMu.Lock()
		p.cpuSetAdjustmentRetryDirty = true
		p.cpuSetAdjustmentRetryQueued = true
		p.cpuSetAdjustmentRetryReasons = map[cpusetutil.CPUSetAdjustmentRetryReason]struct{}{
			cpusetutil.RetryReasonDeferredLeaf: {},
		}
		p.cpuSetAdjustmentRetryMu.Unlock()
		return nil
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModePeriodic)
	p.Unlock()
	if err != nil {
		t.Fatalf("runCPUSetAdjustmentHandlers() error = %v", err)
	}

	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if !p.cpuSetAdjustmentRetryDirty {
		t.Fatal("successful periodic round cleared a retry scheduled during handler execution")
	}
	if _, ok := p.cpuSetAdjustmentRetryReasons[cpusetutil.RetryReasonDeferredLeaf]; !ok {
		t.Fatal("successful periodic round dropped deferred-leaf retry reason scheduled during handler execution")
	}
}

func TestCPUSetAdjustmentCommitsTopologyReclaimOverride(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	setReclaimPoolCPUSet(t, p, machine.NewCPUSet(0, 1))
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"topology-override": func(_ context.Context, handlerCtx cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if handlerCtx.CommitOverride == nil {
				t.Fatal("CPUSet adjustment runner did not provide a commit override")
			}
			handlerCtx.CommitOverride.ReclaimEffective = machine.NewCPUSet(2, 3)
			handlerCtx.CommitOverride.Source = "cpuset_topology"
			return nil
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModePeriodic)
	p.Unlock()
	require.NoError(t, err)

	reclaim := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.NotNil(t, reclaim)
	require.True(t, reclaim.AllocationResult.Equals(machine.NewCPUSet(2, 3)),
		"reclaim allocation=%s, want topology verified override 2-3", reclaim.AllocationResult)
}

func TestCPUSetAdjustmentCommitOverrideUsesRevisionGuard(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	setReclaimPoolCPUSet(t, p, machine.NewCPUSet(0, 1))
	guardState := &cpusetOverrideCommitGuardState{State: p.state}
	p.state = guardState
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"topology-override": func(_ context.Context, handlerCtx cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if handlerCtx.CommitOverride == nil {
				t.Fatal("CPUSet adjustment runner did not provide a commit override")
			}
			handlerCtx.CommitOverride.ReclaimEffective = machine.NewCPUSet(2, 3)
			handlerCtx.CommitOverride.Source = "cpuset_topology"
			return nil
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModePeriodic)
	p.Unlock()
	require.NoError(t, err)
	require.Equal(t, 0, guardState.unconditionalCommitCalls)
	require.Equal(t, 1, guardState.conditionalCommitCalls)
	require.NotZero(t, guardState.conditionalRevision)

	reclaim := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.NotNil(t, reclaim)
	require.True(t, reclaim.AllocationResult.Equals(machine.NewCPUSet(2, 3)),
		"reclaim allocation=%s, want topology verified override 2-3", reclaim.AllocationResult)
}

func TestCgroupCreateRetriesOnlyDeferredLeafDirtyAdjustment(t *testing.T) {
	t.Parallel()

	t.Run("deferred leaf schedules retry", func(t *testing.T) {
		attempted := make(chan struct{}, 1)
		p := &DynamicPolicy{
			cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
				"retry": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
					if in.Mode == cpusetutil.CPUSetAdjustmentModeRetry {
						attempted <- struct{}{}
					}
					return nil
				},
			},
			cpuSetAdjustmentRetryDirty: true,
			cpuSetAdjustmentRetryReasons: map[cpusetutil.CPUSetAdjustmentRetryReason]struct{}{
				cpusetutil.RetryReasonDeferredLeaf: {},
			},
		}

		p.handleCgroupCreateEvent()
		select {
		case <-attempted:
		case <-time.After(time.Second):
			t.Fatal("deferred leaf dirty adjustment was not retried after cgroup create")
		}
	})

	t.Run("unrelated dirty reason does not schedule retry", func(t *testing.T) {
		attempted := make(chan struct{}, 1)
		p := &DynamicPolicy{
			cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
				"retry": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
					if in.Mode == cpusetutil.CPUSetAdjustmentModeRetry {
						attempted <- struct{}{}
					}
					return nil
				},
			},
			cpuSetAdjustmentRetryDirty: true,
			cpuSetAdjustmentRetryReasons: map[cpusetutil.CPUSetAdjustmentRetryReason]struct{}{
				cpusetutil.RetryReasonStaleState: {},
			},
		}

		p.handleCgroupCreateEvent()
		select {
		case <-attempted:
			t.Fatal("cgroup create retried a non-deferred dirty adjustment")
		case <-time.After(100 * time.Millisecond):
		}
	})
}

func TestDynamicPolicyConsumesRegisteredCacheSyncEvents(t *testing.T) {
	t.Parallel()

	attempted := make(chan struct{}, 1)
	fetcher := &cacheSyncRegistrarPodFetcher{
		PodFetcherStub: &podmeta.PodFetcherStub{},
		events:         make(chan podmeta.KubeletPodCacheSyncEvent, 1),
		unregistered:   make(chan struct{}),
	}
	stopCh := make(chan struct{})
	p := &DynamicPolicy{
		metaServer: &metaserver.MetaServer{
			MetaAgent: &agent.MetaAgent{PodFetcher: fetcher},
		},
		stopCh: stopCh,
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"retry": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				if in.Mode == cpusetutil.CPUSetAdjustmentModeRetry {
					attempted <- struct{}{}
				}
				return nil
			},
		},
		cpuSetAdjustmentRetryDirty: true,
		cpuSetAdjustmentRetryReasons: map[cpusetutil.CPUSetAdjustmentRetryReason]struct{}{
			cpusetutil.RetryReasonDeferredLeaf: {},
		},
	}
	p.startKubeletPodCacheSyncDrivenCPUSetRetry()
	fetcher.events <- podmeta.KubeletPodCacheSyncEvent{
		CgroupCreated: true,
		Revision:      1,
		SyncedAt:      time.Now(),
	}
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("cache sync event did not trigger deferred CPUSet retry")
	}
	close(stopCh)
	select {
	case <-fetcher.unregistered:
	case <-time.After(time.Second):
		t.Fatal("cache sync listener was not unregistered on stop")
	}
}

func TestStopCancelsCPUSetAdjustmentRetryWorker(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	stopCh := make(chan struct{})
	p := &DynamicPolicy{
		started:                     true,
		stopCh:                      stopCh,
		cpuSetAdjustmentRetryStopCh: stopCh,
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"wait-for-stop": func(ctx context.Context, _ cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				select {
				case <-started:
				default:
					close(started)
				}
				<-ctx.Done()
				return ctx.Err()
			},
		},
	}

	p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonDeferredLeaf)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("retry worker did not start")
	}

	stopped := make(chan error, 1)
	go func() {
		stopped <- p.Stop()
	}()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop() did not cancel and wait for retry worker")
	}
}

func TestRunCPUSetAdjustmentHandlersDoesNotHoldPolicyLockDuringExecution(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"blocking-io": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				close(started)
				<-release
				return nil
			},
		},
	}

	runDone := make(chan error, 1)
	go func() {
		p.Lock()
		defer p.Unlock()
		runDone <- p.runCPUSetAdjustmentHandlers(context.Background())
	}()
	<-started

	lockAcquired := make(chan struct{})
	go func() {
		p.Lock()
		close(lockAcquired)
		p.Unlock()
	}()

	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		close(release)
		<-runDone
		t.Fatal("DynamicPolicy lock remained held while adjustment handler executed")
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("runCPUSetAdjustmentHandlers() error = %v", err)
	}
}

func TestRunCPUSetAdjustmentHandlersFenceRejectsStaleStateBeforeRetry(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	committed := make(chan bool, 2)
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 4)
	if err != nil {
		t.Fatalf("GenerateDummyCPUTopology() error = %v", err)
	}
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	if err != nil {
		t.Fatalf("getTestDynamicPolicyWithInitialization() error = %v", err)
	}
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"blocking-io": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if in.Generation == 1 {
				close(started)
				<-release
			}
			committed <- in.CommitIfGenerationCurrent(in.Generation, func() {})
			return nil
		},
	}

	runDone := make(chan error, 1)
	go func() {
		p.Lock()
		defer p.Unlock()
		runDone <- p.runCPUSetAdjustmentHandlers(context.Background())
	}()
	<-started

	p.Lock()
	p.state.SetAllowSharedCoresOverlapReclaimedCores(true, false)
	p.Unlock()
	close(release)

	if err := <-runDone; err != nil {
		t.Fatalf("runCPUSetAdjustmentHandlers() error = %v", err)
	}
	if <-committed {
		t.Fatal("generation fence accepted a result calculated from stale policy state")
	}
	if !<-committed {
		t.Fatal("latest generation did not converge after rejecting stale policy state")
	}
}

func TestRunCPUSetAdjustmentHandlersRetriesLatestStateAfterFenceRejection(t *testing.T) {
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	committedGenerations := make(chan uint64, 1)
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 4)
	if err != nil {
		t.Fatalf("GenerateDummyCPUTopology() error = %v", err)
	}
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	if err != nil {
		t.Fatalf("getTestDynamicPolicyWithInitialization() error = %v", err)
	}
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"generation-aware": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if in.Generation == 1 {
				close(firstStarted)
				<-firstRelease
			}
			if in.CommitIfGenerationCurrent(in.Generation, func() {}) {
				committedGenerations <- in.Generation
			}
			return nil
		},
	}

	runDone := make(chan error, 1)
	go func() {
		p.Lock()
		defer p.Unlock()
		runDone <- p.runCPUSetAdjustmentHandlers(context.Background())
	}()
	<-firstStarted
	p.Lock()
	p.state.SetAllowSharedCoresOverlapReclaimedCores(true, false)
	p.Unlock()
	close(firstRelease)

	if err := <-runDone; err != nil {
		t.Fatalf("runCPUSetAdjustmentHandlers() error = %v", err)
	}
	select {
	case generation := <-committedGenerations:
		if generation != 2 {
			t.Fatalf("committed generation = %d, want latest generation 2", generation)
		}
	case <-time.After(time.Second):
		t.Fatal("latest policy state was not scheduled after stale generation rejection")
	}
}

func TestRunCPUSetAdjustmentHandlersSchedulesLatestStateAfterCanceledStaleRound(t *testing.T) {
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	latestCommitted := make(chan struct{})
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 4)
	if err != nil {
		t.Fatalf("GenerateDummyCPUTopology() error = %v", err)
	}
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	if err != nil {
		t.Fatalf("getTestDynamicPolicyWithInitialization() error = %v", err)
	}
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"generation-aware": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if in.Generation == 1 {
				close(firstStarted)
				<-firstRelease
			}
			if in.CommitIfGenerationCurrent(in.Generation, func() {}) && in.Generation > 1 {
				close(latestCommitted)
			}
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		p.Lock()
		defer p.Unlock()
		runDone <- p.runCPUSetAdjustmentHandlers(ctx)
	}()
	<-firstStarted
	p.Lock()
	p.state.SetAllowSharedCoresOverlapReclaimedCores(true, false)
	p.Unlock()
	cancel()
	close(firstRelease)

	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("runCPUSetAdjustmentHandlers() error = %v, want context.Canceled", err)
	}
	select {
	case <-latestCommitted:
	case <-time.After(time.Second):
		t.Fatal("latest policy state was not scheduled after canceled stale round")
	}
}

func TestRunCPUSetAdjustmentHandlersSerializesLockFreeRounds(t *testing.T) {
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	p := &DynamicPolicy{}
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"blocking-io": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if in.Generation == 1 {
				close(firstStarted)
				<-firstRelease
			} else {
				close(secondStarted)
			}
			return nil
		},
	}

	run := func(done chan<- error) {
		p.Lock()
		defer p.Unlock()
		done <- p.runCPUSetAdjustmentHandlers(context.Background())
	}
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go run(firstDone)
	<-firstStarted
	go run(secondDone)

	select {
	case <-secondStarted:
		close(firstRelease)
		t.Fatal("second adjustment round executed concurrently with the first")
	case <-time.After(100 * time.Millisecond):
	}
	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first run error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second run error = %v", err)
	}
}

func TestRunCPUSetAdjustmentHandlersCancelsWhileWaitingForExecutionLock(t *testing.T) {
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	var calls int
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"blocking-io": func(_ context.Context, _ cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				calls++
				if calls == 1 {
					close(firstStarted)
					<-firstRelease
				}
				return nil
			},
		},
	}

	firstDone := make(chan error, 1)
	go func() {
		p.Lock()
		defer p.Unlock()
		firstDone <- p.runCPUSetAdjustmentHandlers(context.Background())
	}()
	<-firstStarted

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		p.Lock()
		defer p.Unlock()
		secondDone <- p.runCPUSetAdjustmentHandlers(ctx, cpusetutil.CPUSetAdjustmentModeAdmission)
	}()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued admission error = %v, want context deadline exceeded", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("queued admission ignored its deadline while waiting for the execution lock")
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d, want canceled queued admission not to execute", calls)
	}

	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first run error = %v", err)
	}
}

func TestQueuedRetryRequestsAlwaysProduceOneTrailingLatestStateRound(t *testing.T) {
	for _, reason := range []cpusetutil.CPUSetAdjustmentRetryReason{
		cpusetutil.RetryReasonDeferredLeaf,
		cpusetutil.RetryReasonStaleState,
	} {
		reason := reason
		t.Run(string(reason), func(t *testing.T) {
			firstStarted := make(chan struct{})
			firstRelease := make(chan struct{})
			rounds := make(chan struct{}, 3)
			calls := 0
			p := &DynamicPolicy{
				cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
					"retry": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
						if in.Mode != cpusetutil.CPUSetAdjustmentModeRetry {
							return nil
						}
						calls++
						rounds <- struct{}{}
						if calls == 1 {
							close(firstStarted)
							<-firstRelease
						}
						return nil
					},
				},
			}

			p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonDeferredLeaf)
			<-firstStarted
			p.scheduleCPUSetAdjustmentRetry(reason)
			close(firstRelease)

			for i := 0; i < 2; i++ {
				select {
				case <-rounds:
				case <-time.After(time.Second):
					t.Fatalf("retry rounds = %d, want initial plus one trailing latest-state round", i)
				}
			}
			select {
			case <-rounds:
				t.Fatal("queued retry requests produced more than one trailing round")
			case <-time.After(100 * time.Millisecond):
			}
			deadline := time.Now().Add(time.Second)
			for {
				p.cpuSetAdjustmentRetryMu.Lock()
				queued := p.cpuSetAdjustmentRetryQueued
				p.cpuSetAdjustmentRetryMu.Unlock()
				if !queued {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("retry worker did not finish")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}
