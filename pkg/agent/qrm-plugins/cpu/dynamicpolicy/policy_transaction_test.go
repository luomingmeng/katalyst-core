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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	qrmutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"
)

type transactionRecordingRepository struct {
	state.State
	base        *state.TargetState
	events      *[]string
	commitErr   error
	commitCalls int
}

func (r *transactionRecordingRepository) PrepareDurableTarget() (*state.TargetState, error) {
	*r.events = append(*r.events, "prepare")
	return r.base.Clone(), nil
}

func (r *transactionRecordingRepository) CommitTarget(next *state.TargetState) error {
	*r.events = append(*r.events, "commit")
	r.commitCalls++
	if r.commitErr != nil {
		return r.commitErr
	}
	r.base = next.Clone()
	return nil
}

type transactionRecordingMaterializer struct {
	events      *[]string
	results     []cpusetmaterializer.Result
	errs        []error
	targets     []cpusetmaterializer.Target
	contexts    []context.Context
	contextErrs []error
	onCall      func(cpusetmaterializer.Target)
}

func (m *transactionRecordingMaterializer) Materialize(
	ctx context.Context,
	target cpusetmaterializer.Target,
) (cpusetmaterializer.Result, error) {
	*m.events = append(*m.events, "materialize")
	m.targets = append(m.targets, target)
	m.contexts = append(m.contexts, ctx)
	m.contextErrs = append(m.contextErrs, ctx.Err())
	if m.onCall != nil {
		m.onCall(target)
	}
	index := len(m.targets) - 1
	var result cpusetmaterializer.Result
	if index < len(m.results) {
		result = m.results[index]
	}
	var err error
	if index < len(m.errs) {
		err = m.errs[index]
	}
	return result, err
}

func newTransactionTestPolicy(
	t *testing.T,
	repository *transactionRecordingRepository,
	materializer cpusetmaterializer.Materializer,
) *DynamicPolicy {
	t.Helper()
	topology, err := machine.GenerateDummyCPUTopology(8, 2, 2)
	require.NoError(t, err)
	if repository.base == nil {
		machineState, generateErr := state.GenerateMachineStateFromPodEntries(
			topology, state.PodEntries{}, state.NUMANodeMap{})
		require.NoError(t, generateErr)
		repository.base = &state.TargetState{
			PodEntries:   state.PodEntries{},
			MachineState: machineState,
		}
	}
	return &DynamicPolicy{
		lifecycleState:     policyLifecycleReady,
		state:              repository,
		machineInfo:        &machine.KatalystMachineInfo{CPUTopology: topology},
		cpuSetMaterializer: materializer,
	}
}

func TestTransactMaterializesBeforeCommit(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{Converged: true}},
	}
	p := newTransactionTestPolicy(t, repository, materializer)

	err := p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		events = append(events, "plan")
		return base.Clone(), nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"prepare", "plan", "materialize", "commit"}, events)
	require.Len(t, materializer.targets, 1)
}

func TestTransactMaterializesEffectiveReclaimOverlapFromPolicyConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name        string
		hard        bool
		rawOverlap  bool
		wantOverlap bool
	}{
		{name: "hard off raw off"},
		{name: "hard off raw on", rawOverlap: true, wantOverlap: true},
		{name: "hard on raw off", hard: true},
		{name: "hard on raw on", hard: true, rawOverlap: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			events := []string{}
			repository := &transactionRecordingRepository{events: &events}
			materializer := &transactionRecordingMaterializer{
				events:  &events,
				results: []cpusetmaterializer.Result{{Converged: true}},
			}
			p := newTransactionTestPolicy(t, repository, materializer)
			p.dynamicConfig = dynamic.NewDynamicAgentConfiguration()
			p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = tt.hard
			repository.base.AllowSharedCoresOverlapReclaimedCores = tt.rawOverlap
			repository.base.DisableDedicatedCoresOverlapReclaimedCores = !tt.hard

			require.NoError(t, p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
				return base, nil
			}))
			require.Len(t, materializer.targets, 1)
			require.Equal(t, tt.wantOverlap, materializer.targets[0].AllowReclaimOverlap())
			require.Equal(t, !tt.hard, repository.base.DisableDedicatedCoresOverlapReclaimedCores,
				"dedicated overlap policy must retain its independent meaning")
		})
	}
}

func TestTransactNilMaterializerCommitsWithoutExecution(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	p := newTransactionTestPolicy(t, repository, nil)

	require.NoError(t, p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		return base.Clone(), nil
	}))
	require.Equal(t, []string{"prepare", "commit"}, events)
}

func TestTransactWithPostCommitSerializesCallbackBeforeNextCommit(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	p := newTransactionTestPolicy(t, repository, nil)
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	firstDone := make(chan error, 1)

	go func() {
		firstDone <- p.transactWithPostCommit(context.Background(),
			func(base *state.TargetState) (*state.TargetState, error) {
				return base.Clone(), nil
			},
			func() {
				events = append(events, "post-commit")
				close(callbackEntered)
				<-callbackRelease
			})
	}()
	<-callbackEntered

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
			return base.Clone(), nil
		})
	}()
	require.Never(t, func() bool {
		return len(events) > 3
	}, 50*time.Millisecond, time.Millisecond,
		"the next transaction committed before the prior post-commit callback released the policy lock")

	close(callbackRelease)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.Equal(t, []string{"prepare", "commit", "post-commit", "prepare", "commit"}, events)
}

func TestTransactWithPostCommitSkipsCallbackWhenCommitFails(t *testing.T) {
	events := []string{}
	commitErr := errors.New("commit failed")
	repository := &transactionRecordingRepository{events: &events, commitErr: commitErr}
	p := newTransactionTestPolicy(t, repository, nil)
	callbackCalled := false

	err := p.transactWithPostCommit(context.Background(),
		func(base *state.TargetState) (*state.TargetState, error) {
			return base.Clone(), nil
		},
		func() {
			callbackCalled = true
		})

	require.ErrorIs(t, err, commitErr)
	require.False(t, callbackCalled)
	require.Equal(t, []string{"prepare", "commit"}, events)
}

func TestTransactAdvancesInMemoryRevisionOnlyAfterSuccessfulCommit(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	p := newTransactionTestPolicy(t, repository, nil)

	require.NoError(t, p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		return base, nil
	}))
	require.Equal(t, uint64(1), p.inMemoryRevision)

	repository.commitErr = errors.New("commit failed")
	err := p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		return base, nil
	})
	require.Error(t, err)
	require.Equal(t, uint64(1), p.inMemoryRevision)
}

func TestRemovePodRecordsTransactionFailureAtRPCBoundary(t *testing.T) {
	events := []string{}
	transactionErr := errors.New("commit failed")
	repository := &transactionRecordingRepository{
		events:    &events,
		commitErr: transactionErr,
	}
	p := newTransactionTestPolicy(t, repository, nil)
	emitter := NewMockMetricsEmitter()
	p.emitter = emitter

	resp, err := p.RemovePod(context.Background(), &pluginapi.RemovePodRequest{PodUid: "pod"})

	require.Nil(t, resp)
	require.ErrorContains(t, err, transactionErr.Error())
	require.Equal(t, []int64{1}, emitter.storedInt64[qrmutil.MetricNameRemovePodFailed])
	require.Len(t, emitter.storedTags[qrmutil.MetricNameRemovePodFailed], 1)
}

func TestTransactNeverPublishesCandidateThroughPolicyState(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{Converged: true}},
	}
	p := newTransactionTestPolicy(t, repository, materializer)

	require.NoError(t, p.transact(context.Background(), func(candidate *state.TargetState) (*state.TargetState, error) {
		candidate.AllowSharedCoresOverlapReclaimedCores = true
		require.Same(t, repository, p.state, "background readers must retain the durable repository")
		require.False(t, repository.base.AllowSharedCoresOverlapReclaimedCores,
			"background readers must never observe the uncommitted candidate")
		return candidate, nil
	}))
	require.Len(t, materializer.targets, 1)
}

func TestTransactIfBaseUnchangedRejectsStaleBaseBeforePlanMaterializeOrCommit(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{Converged: true}},
	}
	p := newTransactionTestPolicy(t, repository, materializer)
	captured := repository.base.Clone()
	repository.base.AllowSharedCoresOverlapReclaimedCores = true
	planCalled := false

	err := p.transactIfBaseUnchanged(context.Background(), captured, func(base *state.TargetState) (*state.TargetState, error) {
		planCalled = true
		return base, nil
	})

	var staleErr *StaleTargetBaseError
	require.ErrorAs(t, err, &staleErr)
	require.False(t, planCalled)
	require.Empty(t, materializer.targets)
	require.Zero(t, repository.commitCalls)
	require.Equal(t, []string{"prepare"}, events)
}

func TestTransactRejectsPlanThatDropsActiveRampUpReclaimFloorBeforeMaterializeOrCommit(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{Converged: true}},
	}
	p := newTransactionTestPolicy(t, repository, materializer)
	p.dynamicConfig = dynamic.NewDynamicAgentConfiguration()
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true

	numaID := p.cpuTopology().CPUDetails.NUMANodes().ToSliceNoSortInt()[0]
	numaCPUs := p.cpuTopology().CPUDetails.CPUsInNUMANodes(numaID).ToSliceInt()
	require.GreaterOrEqual(t, len(numaCPUs), 2)
	rampUpCPU := machine.NewCPUSet(numaCPUs[0])
	floorCPU := machine.NewCPUSet(numaCPUs[1])
	entries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: {
				AllocationResult:                 floorCPU.Clone(),
				OriginalAllocationResult:         floorCPU.Clone(),
				TopologyAwareAssignments:         map[int]machine.CPUSet{numaID: floorCPU.Clone()},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{numaID: floorCPU.Clone()},
			},
		},
		"ramp-up-pod": {
			"main": {
				RampUp:                           true,
				AllocationResult:                 rampUpCPU.Clone(),
				OriginalAllocationResult:         rampUpCPU.Clone(),
				TopologyAwareAssignments:         map[int]machine.CPUSet{numaID: rampUpCPU.Clone()},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{numaID: rampUpCPU.Clone()},
			},
		},
	}
	machineState, err := state.GenerateMachineStateFromPodEntries(p.cpuTopology(), entries, state.NUMANodeMap{})
	require.NoError(t, err)
	repository.base = &state.TargetState{PodEntries: entries, MachineState: machineState}

	err = p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		reclaim := base.PodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName]
		reclaim.AllocationResult = machine.NewCPUSet()
		reclaim.OriginalAllocationResult = machine.NewCPUSet()
		reclaim.TopologyAwareAssignments = map[int]machine.CPUSet{}
		reclaim.OriginalTopologyAwareAssignments = map[int]machine.CPUSet{}
		return base, nil
	})

	require.ErrorIs(t, err, planner.ErrHardFloorDropped)
	require.Empty(t, materializer.targets)
	require.Zero(t, repository.commitCalls)
	require.Equal(t, []string{"prepare"}, events)
}

func TestTransactRejectsEveryValidateTargetErrorBeforeMaterializeOrCommit(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{Converged: true}},
	}
	p := newTransactionTestPolicy(t, repository, materializer)
	p.dynamicConfig = dynamic.NewDynamicAgentConfiguration()
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true

	numaID := p.cpuTopology().CPUDetails.NUMANodes().ToSliceNoSortInt()[0]
	cpu := p.cpuTopology().CPUDetails.CPUsInNUMANodes(numaID).ToSliceInt()[0]
	overlap := machine.NewCPUSet(cpu)
	assignments := map[int]machine.CPUSet{numaID: overlap.Clone()}
	entries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: {
				AllocationResult:                 overlap.Clone(),
				OriginalAllocationResult:         overlap.Clone(),
				TopologyAwareAssignments:         assignments,
				OriginalTopologyAwareAssignments: assignments,
			},
		},
		commonstate.PoolNameShare: {
			commonstate.FakedContainerName: {
				AllocationResult:                 overlap.Clone(),
				OriginalAllocationResult:         overlap.Clone(),
				TopologyAwareAssignments:         assignments,
				OriginalTopologyAwareAssignments: assignments,
			},
		},
		"ramp-up-pod": {
			"main": {
				RampUp:                           true,
				AllocationResult:                 overlap.Clone(),
				OriginalAllocationResult:         overlap.Clone(),
				TopologyAwareAssignments:         assignments,
				OriginalTopologyAwareAssignments: assignments,
			},
		},
	}
	machineState, err := state.GenerateMachineStateFromPodEntries(p.cpuTopology(), entries, state.NUMANodeMap{})
	require.NoError(t, err)
	repository.base = &state.TargetState{PodEntries: entries, MachineState: machineState}

	err = p.transact(context.Background(), func(candidate *state.TargetState) (*state.TargetState, error) {
		return candidate, nil
	})

	require.ErrorIs(t, err, planner.ErrReclaimOverlapsShare)
	require.Empty(t, materializer.targets)
	require.Zero(t, repository.commitCalls)
	require.Equal(t, []string{"prepare"}, events)
}

func TestTransactSelectsDisjointValidationFromPolicyAndTargetFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		hardPartitionEnabled bool
		allowOverlap         bool
		wantDisjointError    bool
	}{
		{
			name:                 "hard partition rejects overlap with empty floor",
			hardPartitionEnabled: true,
			allowOverlap:         true,
			wantDisjointError:    true,
		},
		{
			name:              "overlap flag false rejects overlap",
			allowOverlap:      false,
			wantDisjointError: true,
		},
		{
			name:         "legacy overlap flag true preserves overlap",
			allowOverlap: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			events := []string{}
			repository := &transactionRecordingRepository{events: &events}
			materializer := &transactionRecordingMaterializer{
				events:  &events,
				results: []cpusetmaterializer.Result{{Converged: true}},
			}
			p := newTransactionTestPolicy(t, repository, materializer)
			p.dynamicConfig = dynamic.NewDynamicAgentConfiguration()
			p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = tt.hardPartitionEnabled

			numaID := p.cpuTopology().CPUDetails.NUMANodes().ToSliceNoSortInt()[0]
			cpu := p.cpuTopology().CPUDetails.CPUsInNUMANodes(numaID).ToSliceInt()[0]
			overlap := machine.NewCPUSet(cpu)
			assignments := map[int]machine.CPUSet{numaID: overlap.Clone()}
			entries := state.PodEntries{
				commonstate.PoolNameReclaim: {
					commonstate.FakedContainerName: {
						AllocationResult:                 overlap.Clone(),
						OriginalAllocationResult:         overlap.Clone(),
						TopologyAwareAssignments:         assignments,
						OriginalTopologyAwareAssignments: assignments,
					},
				},
				commonstate.PoolNameShare: {
					commonstate.FakedContainerName: {
						AllocationResult:                 overlap.Clone(),
						OriginalAllocationResult:         overlap.Clone(),
						TopologyAwareAssignments:         assignments,
						OriginalTopologyAwareAssignments: assignments,
					},
				},
			}
			machineState, err := state.GenerateMachineStateFromPodEntries(
				p.cpuTopology(), entries, state.NUMANodeMap{})
			require.NoError(t, err)
			repository.base = &state.TargetState{
				PodEntries:                            entries,
				MachineState:                          machineState,
				AllowSharedCoresOverlapReclaimedCores: tt.allowOverlap,
			}

			err = p.transact(context.Background(), func(candidate *state.TargetState) (*state.TargetState, error) {
				return candidate, nil
			})
			if tt.wantDisjointError {
				require.ErrorIs(t, err, planner.ErrReclaimOverlapsShare)
				require.Empty(t, materializer.targets)
				require.Zero(t, repository.commitCalls)
				return
			}
			require.NoError(t, err)
			require.Len(t, materializer.targets, 1)
			require.Equal(t, 1, repository.commitCalls)
		})
	}
}

func TestTransactRejectsNonConvergedWithoutCommit(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	materializer := &transactionRecordingMaterializer{
		events: &events,
		results: []cpusetmaterializer.Result{
			{Converged: false},
			{Converged: true},
		},
	}
	p := newTransactionTestPolicy(t, repository, materializer)

	err := p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		return base.Clone(), nil
	})
	require.ErrorIs(t, err, cpusetmaterializer.ErrCPUSetNotConverged)
	require.Zero(t, repository.commitCalls)
	require.Len(t, materializer.targets, 2, "candidate failure must restore the durable base")
}

func TestTransactRestoresBaseAfterMaterializerError(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	cause := errors.New("materialization failed")
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{}, {Converged: true}},
		errs:    []error{cause, nil},
	}
	p := newTransactionTestPolicy(t, repository, materializer)

	err := p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		return base.Clone(), nil
	})
	require.ErrorIs(t, err, cause)
	require.Zero(t, repository.commitCalls)
	require.Len(t, materializer.targets, 2)
	require.Equal(t, policyLifecycleReady, p.lifecycleState)
}

func TestTransactPreservesDirtyBaseForRecovery(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	cause := errors.New("candidate failed")
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{}, {Converged: true}},
		errs:    []error{cause, nil},
	}
	p := newTransactionTestPolicy(t, repository, materializer)

	err := p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		base.AllowSharedCoresOverlapReclaimedCores = true
		return base, nil
	})
	require.ErrorIs(t, err, cause)
	require.Len(t, materializer.targets, 2)
	require.True(t, materializer.targets[0].AllowReclaimOverlap())
	require.False(t, materializer.targets[1].AllowReclaimOverlap(),
		"base recovery must use the pristine durable snapshot")
}

func TestTransactRestoresBaseAfterCommitError(t *testing.T) {
	events := []string{}
	commitErr := errors.New("commit failed")
	repository := &transactionRecordingRepository{events: &events, commitErr: commitErr}
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{Converged: true}, {Converged: true}},
	}
	p := newTransactionTestPolicy(t, repository, materializer)

	err := p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		return base.Clone(), nil
	})
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, 1, repository.commitCalls)
	require.Len(t, materializer.targets, 2)
}

func TestTransactBlocksPolicyWhenBaseRestoreFails(t *testing.T) {
	events := []string{}
	candidateErr := errors.New("candidate failed")
	restoreErr := errors.New("restore failed")
	repository := &transactionRecordingRepository{events: &events}
	materializer := &transactionRecordingMaterializer{
		events: &events,
		errs:   []error{candidateErr, restoreErr},
	}
	p := newTransactionTestPolicy(t, repository, materializer)

	err := p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		return base.Clone(), nil
	})
	require.ErrorIs(t, err, candidateErr)
	require.ErrorIs(t, err, restoreErr)
	require.Equal(t, policyLifecycleBlocked, p.lifecycleState)
	require.ErrorIs(t, p.lifecycleErr, restoreErr)
}

func TestBootstrapTransactRunsWhilePolicyIsRecovering(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{Converged: true}},
	}
	p := newTransactionTestPolicy(t, repository, materializer)
	p.lifecycleState = policyLifecycleRecovering

	err := p.transactBootstrap(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		events = append(events, "bootstrap-plan")
		return base, nil
	})

	require.NoError(t, err)
	require.Equal(t, []string{"prepare", "bootstrap-plan", "materialize", "commit"}, events)
	require.Equal(t, 1, repository.commitCalls)
}

func TestBootstrapTransactRestoresDurableBaseOnMaterializationFailure(t *testing.T) {
	events := []string{}
	repository := &transactionRecordingRepository{events: &events}
	cause := errors.New("bootstrap materialization failed")
	materializer := &transactionRecordingMaterializer{
		events:  &events,
		results: []cpusetmaterializer.Result{{}, {Converged: true}},
		errs:    []error{cause, nil},
	}
	p := newTransactionTestPolicy(t, repository, materializer)
	p.lifecycleState = policyLifecycleRecovering

	err := p.transactBootstrap(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		base.AllowSharedCoresOverlapReclaimedCores = true
		return base, nil
	})

	require.ErrorIs(t, err, cause)
	require.Zero(t, repository.commitCalls)
	require.Len(t, materializer.targets, 2)
	require.True(t, materializer.targets[0].AllowReclaimOverlap())
	require.False(t, materializer.targets[1].AllowReclaimOverlap())
}
