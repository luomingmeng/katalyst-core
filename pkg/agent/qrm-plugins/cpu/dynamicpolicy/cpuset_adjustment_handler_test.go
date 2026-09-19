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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/config"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/statedirectory"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	podmeta "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
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

func TestCPUSetAdjustmentExecutionLeaseStaleContextAfterRelease(t *testing.T) {
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"noop": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				return nil
			},
		},
	}

	p.Lock()
	staleLease, err := p.acquireCPUSetAdjustmentExecutionLocked(context.Background())
	require.NoError(t, err)
	staleCtx := context.WithValue(
		context.Background(), cpuSetAdjustmentExecutionLeaseContextKey{}, staleLease)
	staleLease.release()
	activeLease, err := p.acquireCPUSetAdjustmentExecutionLocked(context.Background())
	require.NoError(t, err)
	p.Unlock()

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		p.Lock()
		close(started)
		done <- p.runCPUSetAdjustmentHandlers(staleCtx)
		p.Unlock()
	}()
	<-started

	select {
	case err := <-done:
		activeLease.release()
		t.Fatalf("stale context bypassed the active execution lease: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	activeLease.release()
	require.NoError(t, <-done)
	require.Nil(t, cpuSetAdjustmentExecutionLeaseFromContext(staleCtx, p))

	p.Lock()
	concurrentLease, err := p.acquireCPUSetAdjustmentExecutionLocked(context.Background())
	require.NoError(t, err)
	p.Unlock()
	var releases sync.WaitGroup
	releases.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer releases.Done()
			concurrentLease.release()
		}()
	}
	released := make(chan struct{})
	go func() {
		releases.Wait()
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("concurrent execution lease release must be idempotent")
	}
}

func (s *cpusetOverrideCommitGuardState) CommitAdvisorState(
	state.PodEntries,
	state.NUMANodeMap,
	bool,
	bool,
	bool,
	...*state.WritePermit,
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
	permits ...*state.WritePermit,
) error {
	s.conditionalCommitCalls++
	s.conditionalRevision = expectedRevision
	return s.State.CommitAdvisorStateIfRevision(
		expectedRevision, podEntries, machineState, allowOverlap, disableDedicatedOverlap, persist, permits...)
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

type frozenInitialSnapshotDriftTestError struct{}

func (*frozenInitialSnapshotDriftTestError) Error() string {
	return "frozen trace initial snapshot drift"
}

func (*frozenInitialSnapshotDriftTestError) FrozenInitialSnapshotDrift() bool { return true }

func TestAdmissionRetriesFrozenInitialSnapshotDriftInPlace(t *testing.T) {
	t.Parallel()

	firstCalls := 0
	secondCalls := 0
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"a-stale-once": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				firstCalls++
				if firstCalls == 1 {
					return &frozenInitialSnapshotDriftTestError{}
				}
				return nil
			},
			"b-success": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				secondCalls++
				return nil
			},
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModeAdmission)
	p.Unlock()

	require.NoError(t, err)
	require.Equal(t, 2, firstCalls)
	require.Equal(t, 1, secondCalls)
}

func TestAdmissionFrozenInitialSnapshotDriftRetryIsBounded(t *testing.T) {
	t.Parallel()

	calls := 0
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"always-stale": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				calls++
				return &frozenInitialSnapshotDriftTestError{}
			},
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModeAdmission)
	p.Unlock()

	require.Error(t, err)
	require.Equal(t, cpuSetAdjustmentAdmissionReplans, calls)
	require.ErrorContains(t, err, "frozen trace initial snapshot drift")
}

type frozenSnapshotDriftAfterVerifiedRollbackTestError struct{}

func (*frozenSnapshotDriftAfterVerifiedRollbackTestError) Error() string {
	return "frozen trace final snapshot drift after verified rollback"
}

func (*frozenSnapshotDriftAfterVerifiedRollbackTestError) FrozenSnapshotDriftReplanSafe() bool {
	return true
}

type frozenFinalSnapshotDriftUnverifiedTestError struct{}

func (*frozenFinalSnapshotDriftUnverifiedTestError) Error() string {
	return "frozen trace final snapshot drift"
}

func (*frozenFinalSnapshotDriftUnverifiedTestError) FrozenFinalSnapshotDrift() bool { return true }

func TestAdmissionRetriesFinalSnapshotDriftAfterVerifiedRollback(t *testing.T) {
	t.Parallel()

	calls := 0
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"final-drift-once": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				calls++
				if calls == 1 {
					return &frozenSnapshotDriftAfterVerifiedRollbackTestError{}
				}
				return nil
			},
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModeAdmission)
	p.Unlock()

	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestAdmissionDoesNotRetryUnverifiedFinalSnapshotDrift(t *testing.T) {
	t.Parallel()

	calls := 0
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"unsafe-final-drift": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				calls++
				return &frozenFinalSnapshotDriftUnverifiedTestError{}
			},
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModeAdmission)
	p.Unlock()

	require.Error(t, err)
	require.Equal(t, 1, calls)
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

func TestDeferredFullRetryCountsTrailingRoundsTowardAttemptBudget(t *testing.T) {
	t.Parallel()

	attempts := make(chan time.Time, cpuSetAdjustmentRetryMaxAttempts+1)
	var attemptCount int32
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"retry": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				if in.Mode != cpusetutil.CPUSetAdjustmentModeRetry {
					return nil
				}
				attempt := atomic.AddInt32(&attemptCount, 1)
				attempts <- time.Now()
				if attempt <= cpuSetAdjustmentRetryMaxAttempts {
					in.ScheduleFullRetry(cpusetutil.RetryReasonDeferredLeaf)
				}
				return nil
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
			t.Fatal("self-scheduled trailing retries did not stop within the bounded attempt window")
		}
		time.Sleep(time.Millisecond)
	}

	require.Equal(t, int32(cpuSetAdjustmentRetryMaxAttempts), atomic.LoadInt32(&attemptCount),
		"every worker round, including successful trailing rounds, must consume the shared attempt budget")
	var previous time.Time
	for i := 0; i < cpuSetAdjustmentRetryMaxAttempts; i++ {
		current := <-attempts
		if !previous.IsZero() {
			require.GreaterOrEqual(t, current.Sub(previous), cpuSetAdjustmentRetryInitialBackoff,
				"trailing retry %d ran without backoff", i+1)
		}
		previous = current
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	require.False(t, p.cpuSetAdjustmentRetryQueued)
	require.False(t, p.cpuSetAdjustmentRetryAgain)
	require.True(t, p.cpuSetAdjustmentRetryDirty)
	require.Contains(t, p.cpuSetAdjustmentRetryReasons, cpusetutil.RetryReasonDeferredLeaf)
}

func TestDeferredFullRetrySuccessfulTrailingRoundClearsTrimmedRequest(t *testing.T) {
	t.Parallel()

	var attemptCount int32
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"retry": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				if in.Mode != cpusetutil.CPUSetAdjustmentModeRetry {
					return nil
				}
				if atomic.AddInt32(&attemptCount, 1) == 1 {
					in.ScheduleFullRetry(cpusetutil.RetryReasonDeferredLeaf)
					in.ScheduleFullRetry(cpusetutil.RetryReasonDeferredLeaf)
				}
				return nil
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
			t.Fatal("successful trailing retry did not finish")
		}
		time.Sleep(time.Millisecond)
	}

	require.Equal(t, int32(2), atomic.LoadInt32(&attemptCount))
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	require.False(t, p.cpuSetAdjustmentRetryDirty)
	require.Nil(t, p.cpuSetAdjustmentRetryReasons)
	require.False(t, p.cpuSetAdjustmentRetryAgain)
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

func TestCPUSetAdjustmentAlignsAdmissionReclaimOverrideToWholeCores(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	setReclaimPoolCPUSet(t, p, machine.NewCPUSet(0, 1, 48, 49))
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
	var handlerCalls int32
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"topology-override": func(_ context.Context, handlerCtx cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if atomic.AddInt32(&handlerCalls, 1) == 1 {
				handlerCtx.CommitOverride.ReclaimEffective = machine.NewCPUSet(1, 48, 49)
			} else {
				handlerCtx.CommitOverride.ReclaimEffective = machine.NewCPUSet(1, 49)
			}
			handlerCtx.CommitOverride.Source = "cpuset_topology"
			return nil
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModeAdmission)
	p.Unlock()
	require.NoError(t, err)

	reclaim := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.NotNil(t, reclaim)
	require.True(t, reclaim.AllocationResult.Equals(machine.NewCPUSet(1, 49)),
		"admission override must retain complete physical cores only, got %s", reclaim.AllocationResult)
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&handlerCalls) >= 2
	}, time.Second, 10*time.Millisecond,
		"trimming an applied override must schedule a latest-state convergence pass")
}

func TestCPUSetAdjustmentRetrySchedulesAgainWhenReclaimOverrideTrimmed(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	setReclaimPoolCPUSet(t, p, machine.NewCPUSet(0, 1, 48, 49))
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
	p.cpuSetAdjustmentRetryQueued = true
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"topology-override": func(_ context.Context, handlerCtx cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if handlerCtx.CommitOverride == nil {
				t.Fatal("CPUSet adjustment runner did not provide a commit override")
			}
			handlerCtx.CommitOverride.ReclaimEffective = machine.NewCPUSet(1, 48, 49)
			handlerCtx.CommitOverride.Source = "cpuset_topology"
			return nil
		},
	}

	p.Lock()
	err := p.runCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentModeRetry)
	p.Unlock()
	require.NoError(t, err)

	reclaim := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.NotNil(t, reclaim)
	require.True(t, reclaim.AllocationResult.Equals(machine.NewCPUSet(1, 49)),
		"retry override must commit only complete physical cores, got %s", reclaim.AllocationResult)
	require.True(t, p.cpuSetAdjustmentRetryAgain,
		"trimming during a retry must request another latest-state pass to align runtime side effects with the committed checkpoint")
	require.Contains(t, p.cpuSetAdjustmentRetryReasons, cpusetutil.RetryReasonRecoveryCommit)
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

func TestAdvisorCPUSetAdjustmentFailureRetainsDesiredStateAndRetries(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	desired := machine.NewCPUSet(0, 1)
	setReclaimPoolCPUSet(t, p, desired)

	retried := make(chan machine.CPUSet, 1)
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"transient-cgroup-failure": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			reclaim := in.State.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
			if in.Mode == cpusetutil.CPUSetAdjustmentModeRetry {
				retried <- reclaim.AllocationResult.Clone()
				return nil
			}
			return errors.New("transient cgroup write failure")
		},
	}

	p.Lock()
	target := p.publishAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{}, p.state.GetRevision())
	err := p.reconcileAdvisorPostCommitTarget(context.Background(), target)
	p.Unlock()
	require.ErrorContains(t, err, "transient cgroup write failure")
	require.True(t, p.state.GetAllocationInfo(
		commonstate.PoolNameReclaim, commonstate.FakedContainerName).AllocationResult.Equals(desired),
		"failed cgroup apply must retain committed desired state")

	select {
	case got := <-retried:
		require.True(t, got.Equals(desired), "retry must consume retained desired state, got %s", got)
	case <-time.After(time.Second):
		t.Fatal("failed advisor cgroup apply was not retried")
	}
}

func TestPostAdvisorCommitApplyFailureRunsInOrderAndMarksRevisionOnce(t *testing.T) {
	for _, tc := range []struct {
		name           string
		headroomErr    error
		cgroupErr      error
		adjustmentErr  error
		wantErrStrings []string
	}{
		{
			name:           "headroom failure",
			headroomErr:    errors.New("headroom failure"),
			wantErrStrings: []string{"headroom failure"},
		},
		{
			name:           "cgroup config failure",
			cgroupErr:      errors.New("cgroup config failure"),
			wantErrStrings: []string{"cgroup config failure"},
		},
		{
			name:           "cpuset adjustment failure",
			adjustmentErr:  errors.New("cpuset adjustment failure"),
			wantErrStrings: []string{"cpuset adjustment failure"},
		},
		{
			name:           "headroom and cgroup failures",
			headroomErr:    errors.New("headroom failure"),
			cgroupErr:      errors.New("cgroup config failure"),
			wantErrStrings: []string{"headroom failure", "cgroup config failure"},
		},
		{
			name:           "headroom and cpuset failures",
			headroomErr:    errors.New("headroom failure"),
			adjustmentErr:  errors.New("cpuset adjustment failure"),
			wantErrStrings: []string{"headroom failure", "cpuset adjustment failure"},
		},
		{
			name:           "cgroup and cpuset failures",
			cgroupErr:      errors.New("cgroup config failure"),
			adjustmentErr:  errors.New("cpuset adjustment failure"),
			wantErrStrings: []string{"cgroup config failure", "cpuset adjustment failure"},
		},
		{
			name:           "all apply stages fail",
			headroomErr:    errors.New("headroom failure"),
			cgroupErr:      errors.New("cgroup config failure"),
			adjustmentErr:  errors.New("cpuset adjustment failure"),
			wantErrStrings: []string{"headroom failure", "cgroup config failure", "cpuset adjustment failure"},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			p, cleanup := newReclaimReuseTestPolicy(t)
			defer cleanup()
			desired := machine.NewCPUSet(0, 1)
			setReclaimPoolCPUSet(t, p, desired)
			revision := p.state.GetRevision()

			var calls []string
			mockey.PatchConvey(tc.name, t, func() {
				mockey.Mock((*DynamicPolicy).applyHeadroom).IncludeCurrentGoRoutine().
					To(func(_ *DynamicPolicy, _ *advisorapi.ListAndWatchResponse) error {
						calls = append(calls, "apply-headroom")
						return tc.headroomErr
					}).Build()
				mockey.Mock((*DynamicPolicy).applyCgroupConfigs).IncludeCurrentGoRoutine().
					To(func(_ *DynamicPolicy, _ *advisorapi.ListAndWatchResponse) error {
						calls = append(calls, "apply-cgroup-configs")
						return tc.cgroupErr
					}).Build()
				mockey.Mock((*DynamicPolicy).runCPUSetAdjustmentHandlers).IncludeCurrentGoRoutine().
					To(func(_ *DynamicPolicy, _ context.Context, _ ...cpusetutil.CPUSetAdjustmentMode) error {
						calls = append(calls, "adjust-cpuset")
						return tc.adjustmentErr
					}).Build()
				mockey.Mock((*DynamicPolicy).markAdvisorApplyFailed).IncludeCurrentGoRoutine().
					To(func(_ *DynamicPolicy, gotRevision uint64) {
						require.Equal(t, revision, gotRevision)
						calls = append(calls, "mark-apply-failed")
					}).Build()

				p.Lock()
				target := p.publishAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{}, revision)
				err := p.reconcileAdvisorPostCommitTarget(context.Background(), target)
				p.Unlock()

				for _, wantErr := range tc.wantErrStrings {
					require.ErrorContains(t, err, wantErr)
				}
				require.Equal(t, []string{"apply-headroom", "apply-cgroup-configs", "adjust-cpuset", "mark-apply-failed"}, calls)
				require.True(t, p.state.GetAllocationInfo(
					commonstate.PoolNameReclaim, commonstate.FakedContainerName).AllocationResult.Equals(desired),
					"post-commit apply failures must not roll desired state back")
			})
		})
	}
}

func TestAdvisorPostCommitTargetClonesResponseExtraEntries(t *testing.T) {
	t.Parallel()

	p := &DynamicPolicy{}
	resp := &advisorapi.ListAndWatchResponse{
		ExtraEntries: []*advisorsvc.CalculationInfo{{
			CgroupPath: "/old",
			CalculationResult: &advisorsvc.CalculationResult{
				Values: map[string]string{"key": "old"},
			},
		}},
	}

	target := p.publishAdvisorPostCommitTarget(resp, 7)
	resp.ExtraEntries[0].CgroupPath = "/mutated"
	resp.ExtraEntries[0].CalculationResult.Values["key"] = "mutated"
	resp.ExtraEntries = nil

	require.Equal(t, uint64(7), target.revision)
	require.Len(t, target.response.ExtraEntries, 1)
	require.Equal(t, "/old", target.response.ExtraEntries[0].CgroupPath)
	require.Equal(t, "old", target.response.ExtraEntries[0].CalculationResult.Values["key"])
}

func TestAdvisorPostCommitRetryReplaysAllStagesUntilConverged(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	var calls []string
	attempt := 0
	mockey.PatchConvey("retry replays every stage", t, func() {
		mockey.Mock((*DynamicPolicy).applyHeadroom).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, _ *advisorapi.ListAndWatchResponse) error {
				calls = append(calls, "headroom")
				return nil
			}).Build()
		mockey.Mock((*DynamicPolicy).applyCgroupConfigs).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, _ *advisorapi.ListAndWatchResponse) error {
				calls = append(calls, "cgroup")
				if attempt == 0 {
					return errors.New("transient cgroup failure")
				}
				return nil
			}).Build()
		mockey.Mock((*DynamicPolicy).runCPUSetAdjustmentHandlers).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, _ context.Context, _ ...cpusetutil.CPUSetAdjustmentMode) error {
				calls = append(calls, "cpuset")
				attempt++
				return nil
			}).Build()
		mockey.Mock((*DynamicPolicy).markAdvisorApplyFailed).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, _ uint64) {}).Build()

		p.Lock()
		target := p.publishAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{}, p.state.GetRevision())
		err := p.reconcileAdvisorPostCommitTarget(context.Background(), target)
		p.Unlock()
		require.ErrorContains(t, err, "transient cgroup failure")
		require.Equal(t, []string{"headroom", "cgroup", "cpuset"}, calls)
		require.True(t, p.hasPendingAdvisorPostCommitTarget(target.revision))
		p.cpuSetAdjustmentRetryMu.Lock()
		p.cpuSetAdjustmentRetryDirty = true
		p.cpuSetAdjustmentRetryReasons = map[cpusetutil.CPUSetAdjustmentRetryReason]struct{}{
			cpusetutil.RetryReasonApplyFailed: {},
		}
		p.cpuSetAdjustmentRetryMu.Unlock()

		p.Lock()
		err = p.reconcileAdvisorPostCommitTarget(context.Background(), target)
		p.Unlock()
		require.NoError(t, err)
		require.Equal(t, []string{"headroom", "cgroup", "cpuset", "headroom", "cgroup", "cpuset"}, calls)
		require.False(t, p.hasPendingAdvisorPostCommitTarget(target.revision))
		p.cpuSetAdjustmentRetryMu.Lock()
		require.False(t, p.cpuSetAdjustmentRetryDirty)
		p.cpuSetAdjustmentRetryMu.Unlock()
	})
}

func TestAdvisorPostCommitNewRevisionSupersedesStaleTarget(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	var paths []string
	mockey.PatchConvey("stale target is not replayed", t, func() {
		mockey.Mock((*DynamicPolicy).applyHeadroom).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, resp *advisorapi.ListAndWatchResponse) error {
				paths = append(paths, resp.ExtraEntries[0].CgroupPath)
				return nil
			}).Build()
		mockey.Mock((*DynamicPolicy).applyCgroupConfigs).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, _ *advisorapi.ListAndWatchResponse) error { return nil }).Build()
		mockey.Mock((*DynamicPolicy).runCPUSetAdjustmentHandlers).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, _ context.Context, _ ...cpusetutil.CPUSetAdjustmentMode) error { return nil }).Build()

		oldTarget := p.publishAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{
			ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/old"}},
		}, p.state.GetRevision())
		p.state.SetAllowSharedCoresOverlapReclaimedCores(
			!p.state.GetAllowSharedCoresOverlapReclaimedCores(), false)
		newTarget := p.publishAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{
			ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/new"}},
		}, p.state.GetRevision())

		p.Lock()
		require.NoError(t, p.reconcileAdvisorPostCommitTarget(context.Background(), oldTarget))
		require.NoError(t, p.reconcileAdvisorPostCommitTarget(context.Background(), newTarget))
		p.Unlock()

		require.Equal(t, []string{"/new"}, paths)
		require.False(t, p.hasAnyPendingAdvisorPostCommitTarget())
	})
}

func TestAdvisorPostCommitTargetPreservesWALWhenRevisionChangesBetweenStages(t *testing.T) {
	for _, tc := range []struct {
		name         string
		advanceAfter string
		wantCalls    []string
	}{
		{name: "before headroom", advanceAfter: "publish"},
		{name: "before cgroup", advanceAfter: "headroom", wantCalls: []string{"headroom"}},
		{name: "before cpuset", advanceAfter: "cgroup", wantCalls: []string{"headroom", "cgroup"}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			p, cleanup := newReclaimReuseTestPolicy(t)
			defer cleanup()
			var calls []string

			mockey.PatchConvey(tc.name, t, func() {
				advanceRevision := func() {
					p.state.SetAllowSharedCoresOverlapReclaimedCores(
						!p.state.GetAllowSharedCoresOverlapReclaimedCores(), false)
				}
				mockey.Mock((*DynamicPolicy).applyHeadroom).IncludeCurrentGoRoutine().
					To(func(_ *DynamicPolicy, _ *advisorapi.ListAndWatchResponse) error {
						calls = append(calls, "headroom")
						if tc.advanceAfter == "headroom" {
							advanceRevision()
						}
						return nil
					}).Build()
				mockey.Mock((*DynamicPolicy).applyCgroupConfigs).IncludeCurrentGoRoutine().
					To(func(_ *DynamicPolicy, _ *advisorapi.ListAndWatchResponse) error {
						calls = append(calls, "cgroup")
						if tc.advanceAfter == "cgroup" {
							advanceRevision()
						}
						return nil
					}).Build()
				mockey.Mock((*DynamicPolicy).runCPUSetAdjustmentHandlers).IncludeCurrentGoRoutine().
					To(func(_ *DynamicPolicy, _ context.Context, _ ...cpusetutil.CPUSetAdjustmentMode) error {
						calls = append(calls, "cpuset")
						return nil
					}).Build()

				target := p.publishAdvisorPostCommitTarget(
					&advisorapi.ListAndWatchResponse{}, p.state.GetRevision())
				if tc.advanceAfter == "publish" {
					advanceRevision()
				}
				p.Lock()
				err := p.reconcileAdvisorPostCommitTarget(context.Background(), target)
				p.Unlock()

				require.Error(t, err)
				var retryable retryablePartitionCommitError
				require.ErrorAs(t, err, &retryable)
				require.True(t, retryable.Retryable())
				require.Equal(t, tc.wantCalls, calls)
				require.Same(t, target, p.currentAdvisorPostCommitTarget(),
					"a revision race must preserve the exact pending target")
			})
		})
	}
}

func TestAdvisorPostCommitTargetChecksCurrentPointerBetweenStages(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	var calls []string

	mockey.PatchConvey("superseded between stages", t, func() {
		mockey.Mock((*DynamicPolicy).applyHeadroom).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, _ *advisorapi.ListAndWatchResponse) error {
				calls = append(calls, "old-headroom")
				p.publishAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{
					ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/new"}},
				}, p.state.GetRevision())
				return nil
			}).Build()
		mockey.Mock((*DynamicPolicy).applyCgroupConfigs).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, _ *advisorapi.ListAndWatchResponse) error {
				calls = append(calls, "old-cgroup")
				return nil
			}).Build()
		mockey.Mock((*DynamicPolicy).runCPUSetAdjustmentHandlers).IncludeCurrentGoRoutine().
			To(func(_ *DynamicPolicy, _ context.Context, _ ...cpusetutil.CPUSetAdjustmentMode) error {
				calls = append(calls, "old-cpuset")
				return nil
			}).Build()

		old := p.publishAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{
			ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/old"}},
		}, p.state.GetRevision())
		p.Lock()
		require.NoError(t, p.reconcileAdvisorPostCommitTarget(context.Background(), old))
		p.Unlock()

		require.Equal(t, []string{"old-headroom"}, calls)
		require.True(t, p.hasAnyPendingAdvisorPostCommitTarget(),
			"superseding target must remain pending")
	})
}

func TestAdvisorPostCommitCheckpointCrashRecoveryAndSuccessfulCleanup(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	require.NoError(t, p.state.SetMachineState(p.state.GetMachineState(), false))
	revision := p.state.GetRevision()
	resp := &advisorapi.ListAndWatchResponse{
		Entries: map[string]*advisorapi.CalculationEntries{
			"pool": {Entries: map[string]*advisorapi.CalculationInfo{
				"block": {OwnerPoolName: "persisted-pool"},
			}},
		},
		ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/persisted"}},
	}

	target := p.publishAdvisorPostCommitTarget(resp, revision)
	require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))

	restarted := &DynamicPolicy{
		state:                          p.state,
		advisorPostCommitCheckpointDir: dir,
		cpuSetAdjustmentHandlers:       map[string]cpusetutil.CPUSetAdjustmentHandler{},
	}
	require.NoError(t, restarted.restoreAdvisorPostCommitTarget())
	restored := restarted.currentAdvisorPostCommitTarget()
	require.NotNil(t, restored)
	require.Equal(t, revision, restored.revision)
	require.True(t, proto.Equal(target.response, restored.response),
		"checkpoint must retain the complete proto response")

	restarted.Lock()
	require.NoError(t, restarted.reconcileAdvisorPostCommitTarget(context.Background(), restored))
	restarted.Unlock()
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
}

const (
	advisorCheckpointSubprocessRoleEnv = "KATALYST_ADVISOR_CHECKPOINT_SUBPROCESS_ROLE"
	advisorCheckpointSubprocessDirEnv  = "KATALYST_ADVISOR_CHECKPOINT_SUBPROCESS_DIR"
	advisorCheckpointSubprocessTimeout = 10 * time.Second
)

func TestAdvisorCheckpointSubprocessRestoresDisjointPartitionRevisionPendingAndRetry(t *testing.T) {
	switch os.Getenv(advisorCheckpointSubprocessRoleEnv) {
	case "writer":
		runAdvisorCheckpointWriter(t, os.Getenv(advisorCheckpointSubprocessDirEnv))
		return
	case "reader":
		runAdvisorCheckpointReader(t, os.Getenv(advisorCheckpointSubprocessDirEnv))
		return
	case "timeout-probe":
		fmt.Fprintln(os.Stderr, "advisor checkpoint timeout probe started")
		select {}
	}

	dir := t.TempDir()
	run := func(role string) {
		t.Helper()
		_, err := runAdvisorCheckpointSubprocess(role, dir, advisorCheckpointSubprocessTimeout)
		require.NoError(t, err)
	}
	run("writer")
	require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
	run("reader")
}

func TestAdvisorCheckpointSubprocessTimeoutKillsChild(t *testing.T) {
	output, err := runAdvisorCheckpointSubprocess("timeout-probe", t.TempDir(), time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "timeout-probe subprocess timed out after 1s and was killed")
	require.Contains(t, string(output), "advisor checkpoint timeout probe started")
}

func runAdvisorCheckpointSubprocess(role, dir string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	childTestTimeout := timeout + 5*time.Second
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestAdvisorCheckpointSubprocessRestoresDisjointPartitionRevisionPendingAndRetry$",
		"-test.timeout="+childTestTimeout.String())
	cmd.Env = advisorCheckpointSubprocessEnv(role, dir)
	output, err := cmd.CombinedOutput()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return output, fmt.Errorf(
			"%s subprocess timed out after %s and was killed: %w\ncaptured output:\n%s",
			role, timeout, ctxErr, output)
	}
	if err != nil {
		return output, fmt.Errorf("%s subprocess failed: %w\ncaptured output:\n%s", role, err, output)
	}
	return output, nil
}

func advisorCheckpointSubprocessEnv(role, dir string) []string {
	env := make([]string, 0, len(os.Environ())+2)
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, advisorCheckpointSubprocessRoleEnv+"=") ||
			strings.HasPrefix(entry, advisorCheckpointSubprocessDirEnv+"=") {
			continue
		}
		env = append(env, entry)
	}
	return append(env,
		advisorCheckpointSubprocessRoleEnv+"="+role,
		advisorCheckpointSubprocessDirEnv+"="+dir)
}

func runAdvisorCheckpointWriter(t *testing.T, dir string) {
	t.Helper()
	require.NotEmpty(t, dir)
	topology, err := machine.GenerateDummyCPUTopology(4, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	p.advisorPostCommitCheckpointDir = dir

	dedicated := machine.NewCPUSet(0, 1)
	reclaim := machine.NewCPUSet(2, 3)
	entries := advisorCheckpointPartitionEntries(dedicated, reclaim)
	machineState, err := generateMachineStateFromPodEntries(
		topology, entries, p.state.GetMachineState())
	require.NoError(t, err)
	preCommitRevision := p.state.GetRevision()
	target, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{
			DisableDedicatedCoresOverlapReclaimedCores: true,
		},
		preCommitRevision,
		func(target *advisorPostCommitTarget) error {
			return p.state.CommitAdvisorStateIfRevision(
				preCommitRevision, entries, machineState, false, true, true,
				p.newAdvisorStateWritePermit(target))
		},
	)
	require.NoError(t, err)
	require.Equal(t, preCommitRevision+1, p.state.GetRevision())
	require.Equal(t, p.state.GetRevision(), target.revision)
	require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
}

func runAdvisorCheckpointReader(t *testing.T, dir string) {
	t.Helper()
	require.NotEmpty(t, dir)
	topology, err := machine.GenerateDummyCPUTopology(4, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	p.advisorPostCommitCheckpointDir = dir
	retryCalls := make(chan cpusetutil.CPUSetAdjustmentHandlerCtx, 1)
	var retryCallCount int32
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"keep-pending": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			atomic.AddInt32(&retryCallCount, 1)
			select {
			case retryCalls <- in:
			default:
			}
			return errors.New("keep recovered target pending")
		},
	}

	require.NoError(t, p.Start())
	defer func() { require.NoError(t, p.Stop()) }()

	select {
	case call := <-retryCalls:
		require.Equal(t, cpusetutil.CPUSetAdjustmentModeRetry, call.Mode)
	case <-time.After(2 * time.Second):
		t.Fatalf("retry handler was not called after Start; calls=%d", atomic.LoadInt32(&retryCallCount))
	}
	require.GreaterOrEqual(t, atomic.LoadInt32(&retryCallCount), int32(1))

	dedicated := p.state.GetAllocationInfo("pod-dedicated", "main").AllocationResult
	reclaim := p.state.GetAllocationInfo(
		commonstate.PoolNameReclaim, commonstate.FakedContainerName).AllocationResult
	require.Equal(t, machine.NewCPUSet(0, 1), dedicated)
	require.Equal(t, machine.NewCPUSet(2, 3), reclaim)
	require.True(t, dedicated.Intersection(reclaim).IsEmpty())
	require.Equal(t, machine.NewCPUSet(0, 1, 2, 3), dedicated.Union(reclaim))
	require.True(t, p.state.GetDisableDedicatedCoresOverlapReclaimedCores())

	target := p.currentAdvisorPostCommitTarget()
	require.NotNil(t, target)
	require.Equal(t, p.state.GetRevision(), target.revision)
	require.Greater(t, target.revision, uint64(0))
	require.True(t, p.hasPendingAdvisorPostCommitTarget(p.state.GetRevision()))
	p.cpuSetAdjustmentRetryMu.Lock()
	dirty := p.cpuSetAdjustmentRetryDirty
	_, hasApplyFailedReason := p.cpuSetAdjustmentRetryReasons[cpusetutil.RetryReasonApplyFailed]
	p.cpuSetAdjustmentRetryMu.Unlock()
	require.True(t, dirty)
	require.True(t, hasApplyFailedReason)
}

func advisorCheckpointPartitionEntries(dedicated, reclaim machine.CPUSet) state.PodEntries {
	return state.PodEntries{
		"pod-dedicated": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod-dedicated",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameDedicated,
				},
				AllocationResult:                 dedicated,
				OriginalAllocationResult:         dedicated,
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: dedicated},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: dedicated},
			},
		},
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult:                 reclaim,
				OriginalAllocationResult:         reclaim,
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: reclaim},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: reclaim},
			},
		},
	}
}

func TestAdvisorWriteAheadTargetRejectsRevisionOverflow(t *testing.T) {
	_, err := nextAdvisorRevision(math.MaxUint64)
	require.ErrorContains(t, err, "revision overflow")
}

func TestAdvisorMigrationTransitionFailureRetainsWALAndStopsLaterSideEffects(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir
	topology := p.machineInfo.CPUTopology
	oldTarget := coresInNUMA(topology, 0, 0, 1)
	newTarget := coresInNUMA(topology, 0, 1, 2)
	require.NoError(t, p.storeSteadyFakeNUMAMigrationTarget(
		&steadyFakeNUMAMigrationTarget{constraintDigest: "old", target: oldTarget}))
	preCommitRevision := p.state.GetRevision()

	target, err := p.commitAdvisorResponseWithWriteAheadTransition(
		&advisorapi.ListAndWatchResponse{
			ExtraEntries: []*advisorsvc.CalculationInfo{{
				CalculationResult: &advisorsvc.CalculationResult{
					Values: map[string]string{
						string(advisorapi.ControlKnobKeyCPUNUMAHeadroom): `{"0":7.5}`,
					},
				},
			}},
		},
		steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointReplace,
			target: &steadyFakeNUMAMigrationTarget{
				constraintDigest: "new",
				target:           newTarget,
			},
		},
		preCommitRevision,
		func(target *advisorPostCommitTarget) error {
			return p.state.CommitAdvisorStateIfRevision(
				preCommitRevision,
				p.state.GetPodEntries(),
				p.state.GetMachineState(),
				p.state.GetAllowSharedCoresOverlapReclaimedCores(),
				p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
				true,
				p.newAdvisorStateWritePermit(target),
			)
		},
	)
	require.NoError(t, err)
	require.Equal(t, oldTarget, p.steadyFakeNUMAMigrationTarget.target,
		"CAS must not perform migration checkpoint I/O")

	checkpointPath := p.steadyFakeNUMAMigrationCheckpointPath()
	require.NoError(t, os.Remove(checkpointPath))
	require.NoError(t, os.Mkdir(checkpointPath, 0o700))
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryStopping = true
	p.cpuSetAdjustmentRetryMu.Unlock()

	var adjustmentCalls int
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"observe": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			adjustmentCalls++
			return nil
		},
	}
	beforeHeadroom := p.state.GetNUMAHeadroom()
	p.Lock()
	err = p.reconcileAdvisorPostCommitTarget(context.Background(), target)
	p.Unlock()
	require.ErrorContains(t, err, "migration checkpoint transition")
	require.Equal(t, beforeHeadroom, p.state.GetNUMAHeadroom())
	require.Zero(t, adjustmentCalls)
	require.Same(t, target, p.currentAdvisorPostCommitTarget())
	require.FileExists(t, p.advisorPostCommitCheckpointPath())

	require.NoError(t, os.Remove(checkpointPath))
	p.Lock()
	err = p.reconcileAdvisorPostCommitTarget(context.Background(), target)
	p.Unlock()
	require.NoError(t, err)
	require.Equal(t, map[int]float64{0: 7.5}, p.state.GetNUMAHeadroom())
	require.Equal(t, 1, adjustmentCalls)
	require.Equal(t, "new", p.steadyFakeNUMAMigrationTarget.constraintDigest)
	require.Equal(t, newTarget, p.steadyFakeNUMAMigrationTarget.target)
	require.NoFileExists(t, p.advisorPostCommitCheckpointPath())
}

func TestAdvisorMigrationTransitionSurvivesCommitCrashAndRestart(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(4, 1, 1)
	require.NoError(t, err)
	dir := t.TempDir()
	first, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	targetCPUSet := coresInNUMA(topology, 0, 2, 4)
	preCommitRevision := first.state.GetRevision()

	_, err = first.commitAdvisorResponseWithWriteAheadTransition(
		&advisorapi.ListAndWatchResponse{},
		steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointReplace,
			target: &steadyFakeNUMAMigrationTarget{
				constraintDigest: "restart",
				target:           targetCPUSet,
			},
		},
		preCommitRevision,
		func(target *advisorPostCommitTarget) error {
			return first.state.CommitAdvisorStateIfRevision(
				preCommitRevision,
				first.state.GetPodEntries(),
				first.state.GetMachineState(),
				first.state.GetAllowSharedCoresOverlapReclaimedCores(),
				first.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
				true,
				first.newAdvisorStateWritePermit(target),
			)
		},
	)
	require.NoError(t, err)
	require.Nil(t, first.steadyFakeNUMAMigrationTarget)
	require.NoFileExists(t, first.steadyFakeNUMAMigrationCheckpointPath())

	restarted, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	require.NoError(t, restarted.restoreAdvisorPostCommitTarget())
	restored := restarted.currentAdvisorPostCommitTarget()
	require.NotNil(t, restored)
	require.Equal(t, steadyFakeNUMAMigrationCheckpointReplace,
		restored.migrationCheckpointTransition.kind)
	require.Equal(t, "restart",
		restored.migrationCheckpointTransition.target.constraintDigest)

	observedTransition := false
	restarted.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"observe-transition": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			observedTransition = restarted.steadyFakeNUMAMigrationTarget != nil &&
				restarted.steadyFakeNUMAMigrationTarget.constraintDigest == "restart"
			return nil
		},
	}
	restarted.Lock()
	err = restarted.reconcileAdvisorPostCommitTarget(context.Background(), restored)
	restarted.Unlock()
	require.NoError(t, err)
	require.True(t, observedTransition,
		"migration transition must be the first post-commit stage")
	require.Equal(t, targetCPUSet, restarted.steadyFakeNUMAMigrationTarget.target)
	require.NoFileExists(t, restarted.advisorPostCommitCheckpointPath())
}

func TestLegacyAdvisorWALDefaultsMigrationTransitionToKeep(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir
	oldTarget := coresInNUMA(p.machineInfo.CPUTopology, 0, 0, 1)
	require.NoError(t, p.storeSteadyFakeNUMAMigrationTarget(
		&steadyFakeNUMAMigrationTarget{constraintDigest: "legacy", target: oldTarget}))
	response, err := proto.Marshal(&advisorapi.ListAndWatchResponse{})
	require.NoError(t, err)
	data, err := json.Marshal(advisorPostCommitCheckpoint{
		Revision: p.state.GetRevision(),
		Response: response,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(p.advisorPostCommitCheckpointPath(), data, 0o600))

	require.NoError(t, p.restoreAdvisorPostCommitTarget())
	target := p.currentAdvisorPostCommitTarget()
	require.NotNil(t, target)
	require.Equal(t, steadyFakeNUMAMigrationCheckpointKeep,
		target.migrationCheckpointTransition.kind)
	p.Lock()
	err = p.reconcileAdvisorPostCommitTarget(context.Background(), target)
	p.Unlock()
	require.NoError(t, err)
	require.Equal(t, "legacy", p.steadyFakeNUMAMigrationTarget.constraintDigest)
	require.Equal(t, oldTarget, p.steadyFakeNUMAMigrationTarget.target)
}

func TestAdvisorWALV2IsVersionedChecksummedAndFencedFromLegacyReaders(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir
	preCommitRevision := p.state.GetRevision()
	require.NoError(t, p.state.SetMachineState(p.state.GetMachineState(), false))
	response := &advisorapi.ListAndWatchResponse{
		ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/v2"}},
	}
	targetCPUs := coresInNUMA(p.machineInfo.CPUTopology, 0, 0, 1)
	target := cloneAdvisorPostCommitTarget(
		response,
		p.state.GetRevision(),
		steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointReplace,
			target: &steadyFakeNUMAMigrationTarget{
				constraintDigest: "v2",
				target:           targetCPUs,
			},
		},
	)
	target.preCommitRevision = preCommitRevision

	require.NoError(t, p.storeAdvisorPostCommitTarget(
		target, p.advisorPostCommitCheckpointPath()))
	data, err := os.ReadFile(p.advisorPostCommitCheckpointPath())
	require.NoError(t, err)
	var checkpoint advisorPostCommitCheckpoint
	require.NoError(t, json.Unmarshal(data, &checkpoint))
	require.Equal(t, advisorPostCommitCheckpointVersion, checkpoint.Version)
	require.NotNil(t, checkpoint.PreCommitRevision)
	require.Equal(t, target.preCommitRevision, *checkpoint.PreCommitRevision)
	require.NotEmpty(t, checkpoint.Checksum)
	require.True(t, bytes.HasPrefix(
		checkpoint.Response, []byte("\x00KATALYST_CPU_ADVISOR_WAL_V2\x00")))

	legacyResponse := &advisorapi.ListAndWatchResponse{}
	require.Error(t, proto.Unmarshal(checkpoint.Response, legacyResponse),
		"a legacy reader must fail closed instead of silently dropping the V2 migration transition")

	require.NoError(t, p.restoreAdvisorPostCommitTarget())
	restored := p.currentAdvisorPostCommitTarget()
	require.NotNil(t, restored)
	require.Equal(t, target.preCommitRevision, restored.preCommitRevision)
	require.True(t, proto.Equal(response, restored.response))
	require.Equal(t, steadyFakeNUMAMigrationCheckpointReplace,
		restored.migrationCheckpointTransition.kind)
	require.Equal(t, "v2", restored.migrationCheckpointTransition.target.constraintDigest)
	require.Equal(t, targetCPUs, restored.migrationCheckpointTransition.target.target)
}

func TestAdvisorWALV2RejectsMissingOrCorruptedEnvelope(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = t.TempDir()
	preCommitRevision := p.state.GetRevision()
	target := cloneAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{},
		preCommitRevision+1,
		steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointRemove,
		},
	)
	target.preCommitRevision = preCommitRevision
	require.NoError(t, p.storeAdvisorPostCommitTarget(
		target, p.advisorPostCommitCheckpointPath()))
	original, err := os.ReadFile(p.advisorPostCommitCheckpointPath())
	require.NoError(t, err)

	tests := []struct {
		name   string
		mutate func(*advisorPostCommitCheckpoint)
		want   string
	}{
		{
			name: "missing checksum",
			mutate: func(checkpoint *advisorPostCommitCheckpoint) {
				checkpoint.Checksum = ""
			},
			want: "checksum",
		},
		{
			name: "unsupported version",
			mutate: func(checkpoint *advisorPostCommitCheckpoint) {
				checkpoint.Version++
			},
			want: "unsupported advisor checkpoint version",
		},
		{
			name: "revision corruption",
			mutate: func(checkpoint *advisorPostCommitCheckpoint) {
				checkpoint.Revision++
			},
			want: "checksum mismatch",
		},
		{
			name: "pre-commit revision corruption",
			mutate: func(checkpoint *advisorPostCommitCheckpoint) {
				require.NotNil(t, checkpoint.PreCommitRevision)
				*checkpoint.PreCommitRevision++
			},
			want: "checksum mismatch",
		},
		{
			name: "response corruption",
			mutate: func(checkpoint *advisorPostCommitCheckpoint) {
				checkpoint.Response = append(checkpoint.Response, 0)
			},
			want: "checksum mismatch",
		},
		{
			name: "transition corruption",
			mutate: func(checkpoint *advisorPostCommitCheckpoint) {
				checkpoint.MigrationCheckpointTransition.Kind =
					steadyFakeNUMAMigrationCheckpointReplace
			},
			want: "checksum mismatch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var checkpoint advisorPostCommitCheckpoint
			require.NoError(t, json.Unmarshal(original, &checkpoint))
			tt.mutate(&checkpoint)
			data, err := json.Marshal(checkpoint)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(
				p.advisorPostCommitCheckpointPath(), data, 0o600))
			require.ErrorContains(t, p.restoreAdvisorPostCommitTarget(), tt.want)
		})
	}
}

func TestAdvisorWALV2RejectsInvalidRevisionTransition(t *testing.T) {
	response, err := proto.Marshal(&advisorapi.ListAndWatchResponse{})
	require.NoError(t, err)

	tests := []struct {
		name string
		pre  uint64
		post uint64
	}{
		{name: "equal revisions", pre: 7, post: 7},
		{name: "skipped revision", pre: 7, post: 9},
		{name: "pre-commit revision overflow", pre: math.MaxUint64, post: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkpoint := advisorPostCommitCheckpoint{
				Version:           advisorPostCommitCheckpointVersion,
				PreCommitRevision: &tt.pre,
				Revision:          tt.post,
				Response:          append([]byte(advisorPostCommitWALV2Magic), response...),
			}
			checkpoint.Checksum = advisorPostCommitCheckpointChecksum(
				checkpoint.Version,
				checkpoint.PreCommitRevision,
				checkpoint.Revision,
				response,
				nil,
			)
			path := filepath.Join(t.TempDir(), advisorPostCommitCheckpointName)
			data, err := json.Marshal(checkpoint)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, data, 0o600))

			_, err = loadAdvisorPostCommitTarget(path, nil)
			require.ErrorContains(t, err, "invalid advisor checkpoint revision transition")
		})
	}
}

func TestAdvisorWALV2UsesStrictJSONDecoding(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = t.TempDir()
	target := cloneAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{},
		p.state.GetRevision(),
		steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointRemove,
		},
	)
	require.NoError(t, p.storeAdvisorPostCommitTarget(
		target, p.advisorPostCommitCheckpointPath()))
	data, err := os.ReadFile(p.advisorPostCommitCheckpointPath())
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	raw["future_field"] = true
	unknownTopLevel, err := json.Marshal(raw)
	require.NoError(t, err)

	raw = nil
	require.NoError(t, json.Unmarshal(data, &raw))
	raw["migration_checkpoint_transition"].(map[string]any)["future_field"] = true
	unknownTransition, err := json.Marshal(raw)
	require.NoError(t, err)

	tests := []struct {
		name string
		data []byte
		want string
	}{
		{name: "unknown top-level field", data: unknownTopLevel, want: "unknown field"},
		{name: "unknown transition field", data: unknownTransition, want: "unknown field"},
		{name: "trailing JSON value", data: append(data, []byte(` {}`)...), want: "trailing"},
		{name: "trailing garbage", data: append(data, []byte(` garbage`)...), want: "trailing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(
				p.advisorPostCommitCheckpointPath(), tt.data, 0o600))
			require.ErrorContains(t, p.restoreAdvisorPostCommitTarget(), tt.want)
		})
	}
}

func TestAdvisorWALV2RejectsReplaceOutsideTopologyOrPartialCoreOnWrite(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(4, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)

	tests := []struct {
		name string
		cpus machine.CPUSet
		want string
	}{
		{name: "outside topology", cpus: machine.NewCPUSet(99), want: "outside topology"},
		{name: "partial SMT core", cpus: machine.NewCPUSet(0), want: "core aligned"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := cloneAdvisorPostCommitTarget(
				&advisorapi.ListAndWatchResponse{},
				p.state.GetRevision(),
				steadyFakeNUMAMigrationCheckpointTransition{
					kind: steadyFakeNUMAMigrationCheckpointReplace,
					target: &steadyFakeNUMAMigrationTarget{
						constraintDigest: "invalid",
						target:           tt.cpus,
					},
				},
			)
			err := p.storeAdvisorPostCommitTarget(
				target, p.advisorPostCommitCheckpointPath())
			require.ErrorContains(t, err, tt.want)
			require.NoFileExists(t, p.advisorPostCommitCheckpointPath())
		})
	}
}

func TestAdvisorWALV2RejectsReplaceOutsideTopologyOrPartialCoreOnRead(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(4, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	response, err := proto.Marshal(&advisorapi.ListAndWatchResponse{})
	require.NoError(t, err)

	tests := []struct {
		name string
		cpus []int
		want string
	}{
		{name: "outside topology", cpus: []int{99}, want: "outside topology"},
		{name: "partial SMT core", cpus: []int{0}, want: "core aligned"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preCommitRevision := p.state.GetRevision()
			checkpoint := advisorPostCommitCheckpoint{
				Version:           advisorPostCommitCheckpointVersion,
				PreCommitRevision: &preCommitRevision,
				Revision:          preCommitRevision + 1,
				Response:          append([]byte(advisorPostCommitWALV2Magic), response...),
				MigrationCheckpointTransition: &advisorMigrationCheckpointTransitionWAL{
					Kind:             steadyFakeNUMAMigrationCheckpointReplace,
					ConstraintDigest: "invalid",
					TargetCPUs:       tt.cpus,
				},
			}
			checkpoint.Checksum = advisorPostCommitCheckpointChecksum(
				checkpoint.Version,
				checkpoint.PreCommitRevision,
				checkpoint.Revision,
				response,
				checkpoint.MigrationCheckpointTransition,
			)
			data, err := json.Marshal(checkpoint)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(
				p.advisorPostCommitCheckpointPath(), data, 0o600))
			require.ErrorContains(t, p.restoreAdvisorPostCommitTarget(), tt.want)
		})
	}
}

func TestAdvisorMigrationTransitionStaleCASDoesNotApplyAndRemovesStagingWAL(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir
	topology := p.machineInfo.CPUTopology
	oldTarget := coresInNUMA(topology, 0, 0, 1)
	require.NoError(t, p.storeSteadyFakeNUMAMigrationTarget(
		&steadyFakeNUMAMigrationTarget{constraintDigest: "old", target: oldTarget}))
	revision := p.state.GetRevision()

	_, err := p.commitAdvisorResponseWithWriteAheadTransition(
		&advisorapi.ListAndWatchResponse{},
		steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointRemove,
		},
		revision,
		func(target *advisorPostCommitTarget) error {
			return p.state.CommitAdvisorStateIfRevision(
				revision+1,
				p.state.GetPodEntries(),
				p.state.GetMachineState(),
				p.state.GetAllowSharedCoresOverlapReclaimedCores(),
				p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
				true,
				p.newAdvisorStateWritePermit(target),
			)
		},
	)

	require.ErrorIs(t, err, state.ErrStaleStateRevision)
	require.Equal(t, revision, p.state.GetRevision())
	require.Equal(t, "old", p.steadyFakeNUMAMigrationTarget.constraintDigest)
	require.Equal(t, oldTarget, p.steadyFakeNUMAMigrationTarget.target)
	require.Nil(t, p.currentAdvisorPostCommitTarget())
	require.NoFileExists(t, p.advisorPostCommitCheckpointPath())
	require.NoFileExists(t, p.advisorPostCommitStagingPath())
}

func TestAdvisorWriteAheadTargetFailureDoesNotCommitDesired(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	blockingFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blockingFile, []byte("x"), 0o600))
	p.advisorPostCommitCheckpointDir = blockingFile

	committed := false
	_, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{}, p.state.GetRevision(), func(_ *advisorPostCommitTarget) error {
			committed = true
			return nil
		})
	require.Error(t, err)
	require.False(t, committed, "desired state must not commit when WAL target persistence fails")
	require.Nil(t, p.currentAdvisorPostCommitTarget())
}

func TestAdvisorWriteAheadPromoteFailureKeepsCommittedTargetPendingAndRecoverable(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryStopping = true
	p.cpuSetAdjustmentRetryMu.Unlock()
	preCommitRevision := p.state.GetRevision()
	activePath := filepath.Join(dir, advisorPostCommitCheckpointName)

	target, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{
			ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/committed"}},
		},
		preCommitRevision,
		func(target *advisorPostCommitTarget) error {
			if err := p.state.CommitAdvisorStateIfRevision(
				preCommitRevision,
				p.state.GetPodEntries(),
				p.state.GetMachineState(),
				p.state.GetAllowSharedCoresOverlapReclaimedCores(),
				p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
				true,
				p.newAdvisorStateWritePermit(target),
			); err != nil {
				return err
			}
			return os.Mkdir(activePath, 0o700)
		},
	)

	require.ErrorContains(t, err, "promote advisor post-commit target")
	require.Nil(t, target)
	require.Equal(t, preCommitRevision+1, p.state.GetRevision())
	pending := p.currentAdvisorPostCommitTarget()
	require.NotNil(t, pending, "committed target must block later frames even when WAL promotion fails")
	require.Equal(t, p.state.GetRevision(), pending.revision)
	require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))

	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"fail": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			return errors.New("keep committed target pending")
		},
	}
	err = p.allocateByCPUAdvisor(nil, &advisorapi.ListAndWatchResponse{}, nil)
	require.ErrorContains(t, err, "publish committed advisor post-commit target")

	require.NoError(t, os.Remove(activePath))
	p.cpuSetAdjustmentRetryMu.Lock()
	p.advisorPostCommitTarget = nil
	p.cpuSetAdjustmentRetryMu.Unlock()
	require.NoError(t, p.restoreAdvisorPostCommitTarget())
	require.NotNil(t, p.currentAdvisorPostCommitTarget())
	require.Equal(t, "/committed",
		p.currentAdvisorPostCommitTarget().response.ExtraEntries[0].CgroupPath)
	require.FileExists(t, activePath)
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))
}

func TestAdvisorWriteAheadPromoteFailureRetriesPublicationBeforeApply(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryStopping = true
	p.cpuSetAdjustmentRetryMu.Unlock()
	preCommitRevision := p.state.GetRevision()
	activePath := filepath.Join(dir, advisorPostCommitCheckpointName)
	applied := make(chan map[int]float64, 1)
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"observe": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if _, err := os.Stat(activePath); err != nil {
				return fmt.Errorf("active WAL is not durable before apply: %w", err)
			}
			applied <- in.State.GetNUMAHeadroom()
			return nil
		},
	}

	_, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{
			ExtraEntries: []*advisorsvc.CalculationInfo{{
				CalculationResult: &advisorsvc.CalculationResult{
					Values: map[string]string{
						string(advisorapi.ControlKnobKeyCPUNUMAHeadroom): `{"0":7.5}`,
					},
				},
			}},
		},
		preCommitRevision,
		func(target *advisorPostCommitTarget) error {
			if err := p.state.CommitAdvisorStateIfRevision(
				preCommitRevision,
				p.state.GetPodEntries(),
				p.state.GetMachineState(),
				p.state.GetAllowSharedCoresOverlapReclaimedCores(),
				p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
				true,
				p.newAdvisorStateWritePermit(target),
			); err != nil {
				return err
			}
			return os.Mkdir(activePath, 0o700)
		},
	)
	require.ErrorContains(t, err, "promote advisor post-commit target")

	select {
	case <-applied:
		t.Fatal("post-commit side effects ran before the WAL publication barrier")
	default:
	}
	require.NoError(t, os.Remove(activePath))
	p.Lock()
	err = p.retryLatestCPUSetAdjustment(
		context.Background(), cpusetutil.CPUSetAdjustmentModeRetry)
	p.Unlock()
	require.NoError(t, err)
	select {
	case headroom := <-applied:
		require.Equal(t, map[int]float64{0: 7.5}, headroom)
	default:
		t.Fatal("published target was not applied")
	}
}

func TestAdvisorWriteAheadTargetIsRemovedWhenDesiredCommitFails(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir

	_, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{}, p.state.GetRevision(), func(target *advisorPostCommitTarget) error {
			require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"),
				"target must be durable before applyBlocks commits desired state")
			require.Same(t, target, p.currentAdvisorPostCommitTarget(),
				"prepared target must fence writers before desired commit")
			return errors.New("applyBlocks failed")
		})
	require.ErrorContains(t, err, "applyBlocks failed")
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))
	require.Nil(t, p.currentAdvisorPostCommitTarget())
}

func TestAdvisorWriteAheadCleanupFailureKeepsPreparedFence(t *testing.T) {
	for _, tc := range []struct {
		name          string
		commitDesired func() error
		wantError     string
	}{
		{
			name: "desired commit failure",
			commitDesired: func() error {
				return errors.New("desired commit failed")
			},
			wantError: "desired commit failed",
		},
		{
			name:          "revision check failure",
			commitDesired: func() error { return nil },
			wantError:     "revision mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, cleanup := newReclaimReuseTestPolicy(t)
			defer cleanup()
			p.installCPUStateWritePermit()
			p.cpuSetAdjustmentRetryMu.Lock()
			p.cpuSetAdjustmentRetryStopping = true
			p.cpuSetAdjustmentRetryMu.Unlock()
			dir := t.TempDir()
			p.advisorPostCommitCheckpointDir = dir
			stagingPath := filepath.Join(dir, advisorPostCommitCheckpointName+".staging")
			preCommitRevision := p.state.GetRevision()
			var prepared *advisorPostCommitTarget

			_, err := p.commitAdvisorResponseWithWriteAhead(
				&advisorapi.ListAndWatchResponse{},
				preCommitRevision,
				func(target *advisorPostCommitTarget) error {
					prepared = target
					require.NoError(t, os.Remove(stagingPath))
					require.NoError(t, os.Mkdir(stagingPath, 0o700))
					require.NoError(t, os.WriteFile(
						filepath.Join(stagingPath, "blocks-remove"), []byte("x"), 0o600))
					return tc.commitDesired()
				},
			)

			require.ErrorContains(t, err, tc.wantError)
			require.ErrorContains(t, err, "remove")
			require.Same(t, prepared, p.currentAdvisorPostCommitTarget())
			require.True(t, prepared.prepared)
			require.Equal(t, preCommitRevision, p.state.GetRevision())

			err = p.state.SetMachineState(p.state.GetMachineState(), false)
			var retryable retryablePartitionCommitError
			require.ErrorAs(t, err, &retryable,
				"failed staging cleanup must keep unrelated writers fenced")

			require.NoError(t, os.Remove(filepath.Join(stagingPath, "blocks-remove")))
			p.Lock()
			err = p.retryLatestCPUSetAdjustment(
				context.Background(), cpusetutil.CPUSetAdjustmentModeRetry)
			p.Unlock()
			require.NoError(t, err)
			require.Nil(t, p.currentAdvisorPostCommitTarget())
			require.NoError(t, p.ensureCPUStateWriterAllowed(p.state.GetRevision(), "test", nil))
		})
	}
}

func TestAdvisorWriterGateSnapshotsPreparedTargetUnderRetryLock(t *testing.T) {
	p := &DynamicPolicy{}
	target := &advisorPostCommitTarget{
		preCommitRevision: 1,
		prepared:          true,
		revision:          2,
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	p.advisorPostCommitTarget = target
	p.cpuSetAdjustmentRetryMu.Unlock()

	const iterations = 10000
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			p.cpuSetAdjustmentRetryMu.Lock()
			target.prepared = i%2 == 0
			target.preCommitRevision = uint64(i)
			target.revision = uint64(i + 1)
			p.cpuSetAdjustmentRetryMu.Unlock()
		}
	}()
	close(start)
	for i := 0; i < iterations; i++ {
		_ = p.ensureCPUStateWriterAllowed(uint64(i), "test", target)
	}
	wg.Wait()
}

func TestAdvisorWriteAheadPreparedTargetFencesConcurrentStateWriter(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = t.TempDir()
	p.installCPUStateWritePermit()
	preCommitRevision := p.state.GetRevision()

	prepared := make(chan *advisorPostCommitTarget, 1)
	releaseCommit := make(chan struct{})
	type commitResult struct {
		target *advisorPostCommitTarget
		err    error
	}
	committed := make(chan commitResult, 1)
	go func() {
		target, err := p.commitAdvisorResponseWithWriteAhead(
			&advisorapi.ListAndWatchResponse{},
			preCommitRevision,
			func(target *advisorPostCommitTarget) error {
				prepared <- target
				<-releaseCommit
				return p.state.CommitAdvisorStateIfRevision(
					preCommitRevision,
					p.state.GetPodEntries(),
					p.state.GetMachineState(),
					p.state.GetAllowSharedCoresOverlapReclaimedCores(),
					p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
					true,
					p.newAdvisorStateWritePermit(target),
				)
			},
		)
		committed <- commitResult{target: target, err: err}
	}()

	target := <-prepared
	require.Same(t, target, p.currentAdvisorPostCommitTarget())
	require.Equal(t, preCommitRevision, p.state.GetRevision(),
		"publishing the prepared target must not advance canonical state")
	err := p.state.SetMachineState(p.state.GetMachineState(), false)
	require.Error(t, err, "an unrelated writer must be fenced while the durable target is prepared")
	var retryable retryablePartitionCommitError
	require.ErrorAs(t, err, &retryable)
	require.True(t, retryable.Retryable())
	require.Equal(t, preCommitRevision, p.state.GetRevision())

	close(releaseCommit)
	result := <-committed
	require.NoError(t, result.err)
	require.Same(t, target, result.target)
	require.Same(t, target, p.currentAdvisorPostCommitTarget())
	require.Equal(t, preCommitRevision+1, p.state.GetRevision())
}

func TestAdvisorWriteAheadCommitFailurePreservesActiveTarget(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir
	require.NoError(t, p.state.SetMachineState(p.state.GetMachineState(), false))
	revision := p.state.GetRevision()
	active := p.publishAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{
			ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/active"}},
		},
		revision,
	)

	_, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{
			ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/staging"}},
		},
		revision,
		func(target *advisorPostCommitTarget) error {
			require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
			require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))
			require.NotSame(t, active, target)
			require.Same(t, target, p.currentAdvisorPostCommitTarget())
			return errors.New("desired commit failed")
		},
	)
	require.ErrorContains(t, err, "desired commit failed")
	require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))
	require.Same(t, active, p.currentAdvisorPostCommitTarget())

	p.cpuSetAdjustmentRetryMu.Lock()
	p.advisorPostCommitTarget = nil
	p.cpuSetAdjustmentRetryMu.Unlock()
	require.NoError(t, p.restoreAdvisorPostCommitTarget())
	require.Equal(t, "/active", p.currentAdvisorPostCommitTarget().response.ExtraEntries[0].CgroupPath)
}

func TestAdvisorWriteAheadRecoverySelectsMainRevisionAndCleansOtherSlot(t *testing.T) {
	for _, tc := range []struct {
		name          string
		commitDesired bool
		wantPath      string
	}{
		{name: "old active survives crash before desired commit", wantPath: "/active"},
		{name: "staging is promoted after desired commit", commitDesired: true, wantPath: "/staging"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			topology, err := machine.GenerateDummyCPUTopology(2, 1, 1)
			require.NoError(t, err)
			dir := t.TempDir()
			config := &statedirectory.StateDirectoryConfiguration{StateFileDirectory: dir}
			firstState, err := state.NewCheckpointState(
				config, "cpu_plugin_state", "dynamic", topology, false,
				generateMachineStateFromPodEntries, metrics.DummyMetrics{})
			require.NoError(t, err)
			first := &DynamicPolicy{
				state:                          firstState,
				advisorPostCommitCheckpointDir: dir,
			}
			require.NoError(t, firstState.CommitAdvisorStateIfRevision(
				firstState.GetRevision(),
				firstState.GetPodEntries(),
				firstState.GetMachineState(),
				firstState.GetAllowSharedCoresOverlapReclaimedCores(),
				firstState.GetDisableDedicatedCoresOverlapReclaimedCores(),
				true,
			))
			revision := firstState.GetRevision()
			first.publishAdvisorPostCommitTarget(
				&advisorapi.ListAndWatchResponse{
					ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/active"}},
				},
				revision,
			)
			_, err = first.prepareAdvisorPostCommitTarget(
				&advisorapi.ListAndWatchResponse{
					ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/staging"}},
				},
				revision+1,
			)
			require.NoError(t, err)
			require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
			require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))

			if tc.commitDesired {
				require.NoError(t, firstState.CommitAdvisorStateIfRevision(
					revision,
					firstState.GetPodEntries(),
					firstState.GetMachineState(),
					firstState.GetAllowSharedCoresOverlapReclaimedCores(),
					firstState.GetDisableDedicatedCoresOverlapReclaimedCores(),
					true,
				))
			}

			restartedState, err := state.NewCheckpointState(
				config, "cpu_plugin_state", "dynamic", topology, false,
				generateMachineStateFromPodEntries, metrics.DummyMetrics{})
			require.NoError(t, err)
			restarted := &DynamicPolicy{
				state:                          restartedState,
				advisorPostCommitCheckpointDir: dir,
			}
			require.NoError(t, restarted.restoreAdvisorPostCommitTarget())
			require.NotNil(t, restarted.currentAdvisorPostCommitTarget())
			require.Equal(t, tc.wantPath,
				restarted.currentAdvisorPostCommitTarget().response.ExtraEntries[0].CgroupPath)
			require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
			require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))
		})
	}
}

func TestAdvisorWALRecoveryUsesPreCommitRevisionOnlyForV2(t *testing.T) {
	t.Run("V2 pre-commit revision is cleaned without replay", func(t *testing.T) {
		p, cleanup := newReclaimReuseTestPolicy(t)
		defer cleanup()
		p.advisorPostCommitCheckpointDir = t.TempDir()
		preCommitRevision := p.state.GetRevision()
		postCommitRevision, err := nextAdvisorRevision(preCommitRevision)
		require.NoError(t, err)
		target := cloneAdvisorPostCommitTarget(
			&advisorapi.ListAndWatchResponse{
				ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/v2-uncommitted"}},
			},
			postCommitRevision,
		)
		target.preCommitRevision = preCommitRevision
		require.NoError(t, p.storeAdvisorPostCommitTarget(
			target, p.advisorPostCommitStagingPath()))

		require.NoError(t, p.restoreAdvisorPostCommitTarget())
		require.Nil(t, p.currentAdvisorPostCommitTarget())
		require.NoFileExists(t, p.advisorPostCommitCheckpointPath())
		require.NoFileExists(t, p.advisorPostCommitStagingPath())
	})

	t.Run("V2 post-commit revision is replayed", func(t *testing.T) {
		p, cleanup := newReclaimReuseTestPolicy(t)
		defer cleanup()
		p.advisorPostCommitCheckpointDir = t.TempDir()
		preCommitRevision := p.state.GetRevision()
		postCommitRevision, err := nextAdvisorRevision(preCommitRevision)
		require.NoError(t, err)
		target := cloneAdvisorPostCommitTarget(
			&advisorapi.ListAndWatchResponse{
				ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/v2-committed"}},
			},
			postCommitRevision,
		)
		target.preCommitRevision = preCommitRevision
		require.NoError(t, p.storeAdvisorPostCommitTarget(
			target, p.advisorPostCommitStagingPath()))
		require.NoError(t, p.state.CommitAdvisorStateIfRevision(
			preCommitRevision,
			p.state.GetPodEntries(),
			p.state.GetMachineState(),
			p.state.GetAllowSharedCoresOverlapReclaimedCores(),
			p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
			true,
		))

		require.NoError(t, p.restoreAdvisorPostCommitTarget())
		restored := p.currentAdvisorPostCommitTarget()
		require.NotNil(t, restored)
		require.Equal(t, preCommitRevision, restored.preCommitRevision)
		require.Equal(t, postCommitRevision, restored.revision)
		require.Equal(t, "/v2-committed", restored.response.ExtraEntries[0].CgroupPath)
		require.FileExists(t, p.advisorPostCommitCheckpointPath())
		require.NoFileExists(t, p.advisorPostCommitStagingPath())
	})

	t.Run("legacy exact revision is replayed without pre-commit metadata", func(t *testing.T) {
		p, cleanup := newReclaimReuseTestPolicy(t)
		defer cleanup()
		p.advisorPostCommitCheckpointDir = t.TempDir()
		response := &advisorapi.ListAndWatchResponse{
			ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/legacy"}},
		}
		responseBytes, err := proto.Marshal(response)
		require.NoError(t, err)
		data, err := json.Marshal(advisorPostCommitCheckpoint{
			Revision: p.state.GetRevision(),
			Response: responseBytes,
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(p.advisorPostCommitStagingPath(), data, 0o600))

		require.NoError(t, p.restoreAdvisorPostCommitTarget())
		restored := p.currentAdvisorPostCommitTarget()
		require.NotNil(t, restored)
		require.Equal(t, "/legacy", restored.response.ExtraEntries[0].CgroupPath)
		require.FileExists(t, p.advisorPostCommitCheckpointPath())
		require.NoFileExists(t, p.advisorPostCommitStagingPath())
	})

	t.Run("legacy mismatched revision is cleaned", func(t *testing.T) {
		p, cleanup := newReclaimReuseTestPolicy(t)
		defer cleanup()
		p.advisorPostCommitCheckpointDir = t.TempDir()
		responseBytes, err := proto.Marshal(&advisorapi.ListAndWatchResponse{})
		require.NoError(t, err)
		data, err := json.Marshal(advisorPostCommitCheckpoint{
			Revision: p.state.GetRevision() + 1,
			Response: responseBytes,
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(p.advisorPostCommitStagingPath(), data, 0o600))

		require.NoError(t, p.restoreAdvisorPostCommitTarget())
		require.Nil(t, p.currentAdvisorPostCommitTarget())
		require.NoFileExists(t, p.advisorPostCommitCheckpointPath())
		require.NoFileExists(t, p.advisorPostCommitStagingPath())
	})
}

func TestAdvisorWriteAheadTargetRealRestartAtCommitCrashPoints(t *testing.T) {
	for _, tc := range []struct {
		name          string
		commitDesired bool
		publishTarget bool
		wantRecovered bool
	}{
		{name: "after target before desired commit", wantRecovered: false},
		{name: "after desired commit before memory publish", commitDesired: true, wantRecovered: true},
		{name: "after memory publish before reconcile", commitDesired: true, publishTarget: true, wantRecovered: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			topology, err := machine.GenerateDummyCPUTopology(2, 1, 1)
			require.NoError(t, err)
			dir := t.TempDir()
			config := &statedirectory.StateDirectoryConfiguration{StateFileDirectory: dir}
			firstState, err := state.NewCheckpointState(
				config, "cpu_plugin_state", "dynamic", topology, false,
				generateMachineStateFromPodEntries, metrics.DummyMetrics{})
			require.NoError(t, err)
			first := &DynamicPolicy{
				state:                          firstState,
				advisorPostCommitCheckpointDir: dir,
			}
			postCommitRevision, err := nextAdvisorRevision(firstState.GetRevision())
			require.NoError(t, err)
			target, err := first.prepareAdvisorPostCommitTarget(
				&advisorapi.ListAndWatchResponse{ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/durable"}}},
				postCommitRevision)
			require.NoError(t, err)
			require.NotNil(t, target)

			if tc.commitDesired {
				require.NoError(t, firstState.CommitAdvisorStateIfRevision(
					firstState.GetRevision(),
					firstState.GetPodEntries(),
					firstState.GetMachineState(),
					firstState.GetAllowSharedCoresOverlapReclaimedCores(),
					firstState.GetDisableDedicatedCoresOverlapReclaimedCores(),
					true))
				require.Equal(t, postCommitRevision, firstState.GetRevision())
			}
			if tc.publishTarget {
				first.publishPreparedAdvisorPostCommitTarget(target)
				require.Same(t, target, first.currentAdvisorPostCommitTarget())
			}

			restartedState, err := state.NewCheckpointState(
				config, "cpu_plugin_state", "dynamic", topology, false,
				generateMachineStateFromPodEntries, metrics.DummyMetrics{})
			require.NoError(t, err)
			restarted := &DynamicPolicy{
				state:                          restartedState,
				advisorPostCommitCheckpointDir: dir,
			}
			require.NoError(t, restarted.restoreAdvisorPostCommitTarget())
			if tc.wantRecovered {
				require.NotNil(t, restarted.currentAdvisorPostCommitTarget())
				require.Equal(t, postCommitRevision, restarted.currentAdvisorPostCommitTarget().revision)
				require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
			} else {
				require.Nil(t, restarted.currentAdvisorPostCommitTarget())
				require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
			}
		})
	}
}

func TestAdvisorPostCommitCheckpointStopStartRequeuesPendingTarget(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	target := p.publishAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{ExtraEntries: []*advisorsvc.CalculationInfo{{CgroupPath: "/pending"}}},
		p.state.GetRevision())

	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryStopping = true
	p.cpuSetAdjustmentRetryDirty = false
	p.cpuSetAdjustmentRetryMu.Unlock()
	require.NoError(t, p.prepareAdvisorPostCommitTargetOnStart())

	require.Same(t, target, p.currentAdvisorPostCommitTarget())
	p.cpuSetAdjustmentRetryMu.Lock()
	require.True(t, p.cpuSetAdjustmentRetryDirty)
	require.Contains(t, p.cpuSetAdjustmentRetryReasons, cpusetutil.RetryReasonApplyFailed)
	p.cpuSetAdjustmentRetryMu.Unlock()
	require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName),
		"Stop/Start must retain the pending checkpoint")
}

func TestAdvisorPostCommitCheckpointRevisionMismatchIsCleaned(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	require.NoError(t, p.state.SetMachineState(p.state.GetMachineState(), false))
	p.publishAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{}, p.state.GetRevision())
	p.state.SetAllowSharedCoresOverlapReclaimedCores(
		!p.state.GetAllowSharedCoresOverlapReclaimedCores(), false)
	p.cpuSetAdjustmentRetryMu.Lock()
	p.advisorPostCommitTarget = nil
	p.cpuSetAdjustmentRetryMu.Unlock()

	require.NoError(t, p.restoreAdvisorPostCommitTarget())
	require.Nil(t, p.currentAdvisorPostCommitTarget())
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
}

func TestAdvisorPostCommitInitialStoreFailureCleansStagingAndFence(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryStopping = true
	p.cpuSetAdjustmentRetryMu.Unlock()
	stagingPath := p.advisorPostCommitStagingPath()
	require.NoError(t, os.Mkdir(stagingPath, 0o750))
	commitCalled := false

	_, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{},
		p.state.GetRevision(),
		func(*advisorPostCommitTarget) error {
			commitCalled = true
			return nil
		},
	)

	require.ErrorContains(t, err, "persist advisor post-commit target")
	require.False(t, commitCalled)
	require.NoFileExists(t, stagingPath)
	require.Nil(t, p.currentAdvisorPostCommitTarget())
	require.NoError(t, p.ensureCPUStateWriterAllowed(p.state.GetRevision(), "test", nil))
}

func TestAdvisorPostCommitInitialStoreCleanupFailureRetainsFence(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryStopping = true
	p.cpuSetAdjustmentRetryMu.Unlock()
	stagingPath := p.advisorPostCommitStagingPath()
	require.NoError(t, os.Mkdir(stagingPath, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(stagingPath, "blocker"), []byte("x"), 0o600))

	_, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{},
		p.state.GetRevision(),
		func(*advisorPostCommitTarget) error {
			return nil
		},
	)

	require.ErrorContains(t, err, "remove incomplete target while retaining writer fence")
	target := p.currentAdvisorPostCommitTarget()
	require.NotNil(t, target)
	require.True(t, target.prepared)
	require.Error(t, p.ensureCPUStateWriterAllowed(p.state.GetRevision(), "test", nil))
}

func TestAdvisorPostCommitTargetCurrentPreservesWALOnUnrelatedRevision(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	target := p.publishAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{}, p.state.GetRevision())
	stagingPath := filepath.Join(dir, advisorPostCommitCheckpointName+".staging")
	require.NoError(t, p.storeAdvisorPostCommitTarget(target, stagingPath))
	p.state.SetAllowSharedCoresOverlapReclaimedCores(
		!p.state.GetAllowSharedCoresOverlapReclaimedCores(), false)

	require.False(t, p.advisorPostCommitTargetCurrent(target))
	p.Lock()
	err := p.reconcileAdvisorPostCommitTarget(context.Background(), target)
	p.Unlock()
	require.Error(t, err)
	var retryable retryablePartitionCommitError
	require.ErrorAs(t, err, &retryable)
	require.True(t, retryable.Retryable())

	require.Same(t, target, p.currentAdvisorPostCommitTarget(),
		"an unrelated revision must not silently discard the pending target")
	require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
	require.FileExists(t, stagingPath)
}

func TestAdvisorPostCommitExactReconcileMayCommitAdjustmentOverride(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	p.installCPUStateWritePermit()
	setReclaimPoolCPUSet(t, p, machine.NewCPUSet(0, 1))
	revision := p.state.GetRevision()
	target := p.publishAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{}, revision)
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"override": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			in.CommitOverride.ReclaimEffective = machine.NewCPUSet(2, 3)
			in.CommitOverride.Source = "exact advisor reconcile"
			return nil
		},
	}

	p.Lock()
	err := p.reconcileAdvisorPostCommitTarget(context.Background(), target)
	p.Unlock()

	require.NoError(t, err)
	require.Equal(t, revision+1, p.state.GetRevision())
	require.True(t, p.state.GetAllocationInfo(
		commonstate.PoolNameReclaim,
		commonstate.FakedContainerName,
	).AllocationResult.Equals(machine.NewCPUSet(2, 3)))
	require.Nil(t, p.currentAdvisorPostCommitTarget())
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
}

func TestAdvisorPostCommitReconcileCleansActiveAndStagingCheckpoints(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	target := p.publishAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{}, p.state.GetRevision())
	stagingPath := filepath.Join(dir, advisorPostCommitCheckpointName+".staging")
	require.NoError(t, p.storeAdvisorPostCommitTarget(target, stagingPath))

	p.Lock()
	err := p.reconcileAdvisorPostCommitTarget(context.Background(), target)
	p.Unlock()
	require.NoError(t, err)

	require.Nil(t, p.currentAdvisorPostCommitTarget())
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
	require.NoFileExists(t, stagingPath)
}

func TestAdvisorPostCommitCleanupRetryDoesNotRepeatSideEffects(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryStopping = true
	p.cpuSetAdjustmentRetryMu.Unlock()
	target := p.publishAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{}, p.state.GetRevision())
	stagingPath := p.advisorPostCommitStagingPath()
	require.NoError(t, os.Mkdir(stagingPath, 0o750))
	blocker := filepath.Join(stagingPath, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
	applied := 0
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"count": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			applied++
			return nil
		},
	}

	p.Lock()
	err := p.reconcileAdvisorPostCommitTarget(context.Background(), target)
	p.Unlock()
	require.Error(t, err)
	require.Equal(t, 1, applied)
	require.Same(t, target, p.currentAdvisorPostCommitTarget())

	require.NoError(t, os.Remove(blocker))
	p.Lock()
	err = p.retryLatestCPUSetAdjustment(
		context.Background(), cpusetutil.CPUSetAdjustmentModeRetry)
	p.Unlock()
	require.NoError(t, err)
	require.Equal(t, 1, applied, "cleanup retry must not repeat post-commit side effects")
	require.Nil(t, p.currentAdvisorPostCommitTarget())
}

func TestAdvisorPostCommitAppliedMarkerSkipsSideEffectsAfterRestore(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	require.NoError(t, p.state.SetMachineState(p.state.GetMachineState(), false))
	target := p.publishAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{}, p.state.GetRevision())
	target.applied = true
	require.NoError(t, p.storeAdvisorPostCommitTarget(
		target, p.advisorPostCommitCheckpointPath()))
	p.cpuSetAdjustmentRetryMu.Lock()
	p.advisorPostCommitTarget = nil
	p.cpuSetAdjustmentRetryMu.Unlock()
	require.NoError(t, p.restoreAdvisorPostCommitTarget())
	restored := p.currentAdvisorPostCommitTarget()
	require.NotNil(t, restored)
	require.True(t, restored.applied)
	applied := 0
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"count": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			applied++
			return nil
		},
	}

	p.Lock()
	err := p.reconcileAdvisorPostCommitTarget(context.Background(), restored)
	p.Unlock()

	require.NoError(t, err)
	require.Zero(t, applied, "restored applied target must only clean its WAL")
	require.Nil(t, p.currentAdvisorPostCommitTarget())
	require.NoFileExists(t, p.advisorPostCommitCheckpointPath())
}

func TestAdvisorPostCommitCheckpointCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.advisorPostCommitCheckpointDir = dir
	checkpointPath := filepath.Join(dir, advisorPostCommitCheckpointName)
	require.NoError(t, os.WriteFile(checkpointPath, []byte("{broken"), 0o600))

	err := p.restoreAdvisorPostCommitTarget()

	require.ErrorContains(t, err, "corrupted active advisor post-commit checkpoint")
	require.Nil(t, p.currentAdvisorPostCommitTarget())
	require.FileExists(t, checkpointPath,
		"a corrupted checkpoint for the committed revision must remain for operator recovery")
}

func TestAdvisorPostCommitProgressIdentityPhaseGenerationAndNotification(t *testing.T) {
	p := &DynamicPolicy{}
	target := cloneAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{}, 7)

	p.publishPreparedAdvisorPostCommitTarget(target)
	published := p.currentAdvisorPostCommitProgress()
	require.Same(t, target, published.target)
	require.Equal(t, advisorPostCommitPhasePublished, published.phase)
	require.NotZero(t, published.generation)
	require.False(t, published.createdAt.IsZero())
	require.False(t, published.lastProgressAt.IsZero())

	_, changed := p.currentAdvisorPostCommitTargetAndChange()
	p.recordAdvisorPostCommitProgress(target, advisorPostCommitPhasePhysicalApply)
	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("progress update did not notify waiters")
	}

	applying := p.currentAdvisorPostCommitProgress()
	require.Same(t, target, applying.target)
	require.Equal(t, advisorPostCommitPhasePhysicalApply, applying.phase)
	require.Greater(t, applying.generation, published.generation)
	require.Equal(t, published.createdAt, applying.createdAt)
	require.False(t, applying.lastProgressAt.Before(published.lastProgressAt))

	replacedTargetChanged := applying.changed
	replacement := cloneAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{}, 8)
	p.publishPreparedAdvisorPostCommitTarget(replacement)
	select {
	case <-replacedTargetChanged:
	case <-time.After(time.Second):
		t.Fatal("replacing a non-nil target did not close the previous target's changed channel")
	}
	replaced := p.currentAdvisorPostCommitProgress()
	require.Same(t, replacement, replaced.target)
	require.NotEqual(t, applying.target, replaced.target)
	require.Equal(t, advisorPostCommitPhasePublished, replaced.phase)
}

func TestRecoveredAdvisorPostCommitTargetStartsFreshProgressClock(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	dir := t.TempDir()
	first, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	first.state.SetPodEntries(state.PodEntries{}, true)
	target := cloneAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{}, first.state.GetRevision())
	require.NoError(t, first.storeAdvisorPostCommitTarget(target, first.advisorPostCommitCheckpointPath()))

	before := time.Now()
	restarted, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	require.NoError(t, restarted.restoreAdvisorPostCommitTarget())
	progress := restarted.currentAdvisorPostCommitProgress()
	require.NotNil(t, progress.target)
	require.Equal(t, advisorPostCommitPhasePublished, progress.phase)
	require.NotZero(t, progress.generation)
	require.False(t, progress.createdAt.Before(before))
	require.False(t, progress.lastProgressAt.Before(before))
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
