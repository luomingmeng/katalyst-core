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
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

func TestAdvisorPostCommitTargetChecksRevisionBeforeEveryExternalStage(t *testing.T) {
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
				require.NoError(t, p.reconcileAdvisorPostCommitTarget(context.Background(), target))
				p.Unlock()

				require.Equal(t, tc.wantCalls, calls)
				require.False(t, p.hasAnyPendingAdvisorPostCommitTarget(),
					"stale target must be discarded")
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
		return
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
		func() error {
			return p.state.CommitAdvisorStateIfRevision(
				preCommitRevision, entries, machineState, false, true, true)
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
	var retryCallCount atomic.Int32
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"keep-pending": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			retryCallCount.Add(1)
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
		t.Fatalf("retry handler was not called after Start; calls=%d", retryCallCount.Load())
	}
	require.GreaterOrEqual(t, retryCallCount.Load(), int32(1))

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

func TestAdvisorWriteAheadTargetFailureDoesNotCommitDesired(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	blockingFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blockingFile, []byte("x"), 0o600))
	p.advisorPostCommitCheckpointDir = blockingFile

	committed := false
	_, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{}, p.state.GetRevision(), func() error {
			committed = true
			return nil
		})
	require.Error(t, err)
	require.False(t, committed, "desired state must not commit when WAL target persistence fails")
	require.Nil(t, p.currentAdvisorPostCommitTarget())
}

func TestAdvisorWriteAheadTargetIsRemovedWhenDesiredCommitFails(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir

	_, err := p.commitAdvisorResponseWithWriteAhead(
		&advisorapi.ListAndWatchResponse{}, p.state.GetRevision(), func() error {
			require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"),
				"target must be durable before applyBlocks commits desired state")
			require.Nil(t, p.currentAdvisorPostCommitTarget(),
				"future target must not be published in memory before desired commit")
			return errors.New("applyBlocks failed")
		})
	require.ErrorContains(t, err, "applyBlocks failed")
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))
	require.Nil(t, p.currentAdvisorPostCommitTarget())
}

func TestAdvisorWriteAheadCommitFailurePreservesActiveTarget(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir
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
		func() error {
			require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
			require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))
			return errors.New("desired commit failed")
		},
	)
	require.ErrorContains(t, err, "desired commit failed")
	require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
	require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName+".staging"))
	require.Same(t, active, p.currentAdvisorPostCommitTarget())

	p.advisorPostCommitTarget = nil
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

func TestAdvisorPostCommitCheckpointRevisionMismatchAndCorruptionAreCleaned(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, p *DynamicPolicy, dir string)
	}{
		{
			name: "revision mismatch",
			prepare: func(t *testing.T, p *DynamicPolicy, _ string) {
				p.publishAdvisorPostCommitTarget(&advisorapi.ListAndWatchResponse{}, p.state.GetRevision())
				p.state.SetAllowSharedCoresOverlapReclaimedCores(
					!p.state.GetAllowSharedCoresOverlapReclaimedCores(), false)
			},
		},
		{
			name: "corrupted checkpoint",
			prepare: func(t *testing.T, _ *DynamicPolicy, dir string) {
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, advisorPostCommitCheckpointName), []byte("{broken"), 0o600))
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p, cleanup := newReclaimReuseTestPolicy(t)
			defer cleanup()
			p.advisorPostCommitCheckpointDir = dir
			tc.prepare(t, p, dir)
			p.advisorPostCommitTarget = nil

			require.NoError(t, p.restoreAdvisorPostCommitTarget())
			require.Nil(t, p.currentAdvisorPostCommitTarget())
			require.NoFileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
		})
	}
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
