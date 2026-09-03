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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestCommitPendingCPUPartitionRunsHooksBeforeValidation(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	emitter := &recordingMetricEmitter{}
	p.emitter = emitter

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(2, 3))
	original := candidate.Clone()
	p.allocationHooks = []AllocationHook{func(_, allocation *state.AllocationInfo) error {
		if allocation.PodUid == "dedicated-pod" {
			allocation.AllocationResult = machine.NewCPUSet(1, 2)
		}
		return nil
	}}

	_, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		disableDedicated: true,
		persist:          false,
	})
	require.ErrorContains(t, err, "overlaps reclaim pool")
	require.Equal(t, original, candidate, "precommit must only mutate its cloned candidate")
	require.Nil(t, p.state.GetAllocationInfo("dedicated-pod", "main"),
		"candidate made invalid by a hook must not be committed")
	require.Empty(t, emitter.records, "failed commit must not emit pool size metrics")
}

func TestCommitPendingCPUPartitionNormalizesBeforeRebuildingMachineState(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	emitter := &recordingMetricEmitter{}
	p.emitter = emitter

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(2, 3))
	dedicated := candidate["dedicated-pod"]["main"]
	dedicated.TopologyAwareAssignments = map[int]machine.CPUSet{1: machine.NewCPUSet(2, 3)}
	dedicated.OriginalTopologyAwareAssignments = map[int]machine.CPUSet{1: machine.NewCPUSet(2, 3)}

	committed, machineState, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		disableDedicated: true,
		persist:          false,
	})
	require.NoError(t, err)
	require.True(t, committed["dedicated-pod"]["main"].TopologyAwareAssignments[0].Equals(machine.NewCPUSet(2, 3)))
	require.Empty(t, committed["dedicated-pod"]["main"].TopologyAwareAssignments[1])
	require.True(t, machineState[0].PodEntries["dedicated-pod"]["main"].AllocationResult.Equals(machine.NewCPUSet(2, 3)))
	require.Len(t, emitter.records, 1)
	requirePoolSizeMetric(t, emitter.records, commonstate.PoolNameReclaim, commonstate.PoolNameReclaim, 0, 2)
}

func TestCommitPendingCPUPartitionRejectsRevisionChangedByHook(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(2, 3))
	p.allocationHooks = []AllocationHook{func(_, _ *state.AllocationInfo) error {
		p.state.SetAllowSharedCoresOverlapReclaimedCores(
			!p.state.GetAllowSharedCoresOverlapReclaimedCores(), false)
		return nil
	}}

	_, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		disableDedicated: true,
		persist:          false,
	})
	require.ErrorIs(t, err, state.ErrStaleStateRevision)
	require.Nil(t, p.state.GetAllocationInfo("dedicated-pod", "main"))
}

func TestPreparePendingCPUPartitionRevalidatesQuantityAfterHooks(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 48), machine.NewCPUSet(1, 49))
	p.allocationHooks = []AllocationHook{func(_, allocation *state.AllocationInfo) error {
		if allocation.PodUid == "dedicated-pod" {
			allocation.AllocationResult = machine.NewCPUSet(1)
		}
		return nil
	}}

	_, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		disableDedicated: true,
		persist:          false,
	})

	require.ErrorContains(t, err, "allocation quantity changed after hooks")
	require.Nil(t, p.state.GetAllocationInfo("dedicated-pod", "main"))
}

func TestValidateSteadyReclaimPrecommitInvariant(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(32, 2, 2)
	require.NoError(t, err)
	committed := coresInNUMA(topology, 0, 0, 4)
	planned := coresInNUMA(topology, 0, 2, 4).
		Union(coresInNUMA(topology, 1, 0, 2))

	for _, tc := range []struct {
		name      string
		candidate machine.CPUSet
		want      string
	}{
		{
			name: "fragmented whole core",
			candidate: planned.Difference(machine.NewCPUSet(planned.ToSliceInt()[0])).
				Union(machine.NewCPUSet(coresInNUMA(topology, 1, 2, 3).ToSliceInt()[0])),
			want: "not core-aligned",
		},
		{
			name:      "quantity changed",
			candidate: coresInNUMA(topology, 0, 2, 4),
			want:      "quantity changed",
		},
		{
			name:      "per-NUMA floor changed",
			candidate: coresInNUMA(topology, 0, 0, 4),
			want:      "NUMA distribution changed",
		},
		{
			name:      "committed churn exceeds limit",
			candidate: coresInNUMA(topology, 0, 4, 8),
			want:      "migration churn",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateSteadyReclaimPrecommitInvariant(
				planned, tc.candidate, committed, topology)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestCommitPendingCPUPartitionRejectsInvalidOverrideAndDeletionFallback(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	for _, source := range []string{"cpuset override", "deletion fallback"} {
		t.Run(source, func(t *testing.T) {
			candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(1, 2))
			_, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
				expectedRevision: p.state.GetRevision(),
				entries:          candidate,
				disableDedicated: true,
				persist:          false,
				source:           source,
			})
			require.Error(t, err)
			require.False(t, errors.Is(err, state.ErrStaleStateRevision))
			require.ErrorContains(t, err, "overlaps reclaim pool")
		})
	}
}

func TestValidateAdvisorPartitionBeforeCommitAllowsDedicatedOverlapWhenEnabled(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	err := p.validateAdvisorPartitionBeforeCommit(
		precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(1, 2)),
		nil,
		false,
		false,
	)
	require.NoError(t, err)
}

func TestValidateAdvisorPartitionBeforeCommitRejectsDedicatedOverlapWhenDisabled(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	err := p.validateAdvisorPartitionBeforeCommit(
		precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(1, 2)),
		nil,
		false,
		true,
	)
	require.ErrorContains(t, err, "overlaps reclaim pool")
}

func TestValidateAdvisorPartitionBeforeCommitRejectsSharedOverlapWhenDisabled(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	err := p.validateAdvisorPartitionBeforeCommit(
		precommitPartitionEntriesWithShare(
			machine.NewCPUSet(0, 1),
			machine.NewCPUSet(2, 3),
			machine.NewCPUSet(1, 4),
		),
		nil,
		false,
		false,
	)
	require.ErrorContains(t, err, "reclaim pool overlaps disallowed shared partition before commit")
}

func TestValidateAdvisorPartitionBeforeCommitAllowsSharedOverlapWhenEnabled(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	err := p.validateAdvisorPartitionBeforeCommit(
		precommitPartitionEntriesWithShare(
			machine.NewCPUSet(0, 1),
			machine.NewCPUSet(1, 2),
			machine.NewCPUSet(0, 3),
		),
		nil,
		true,
		false,
	)
	require.NoError(t, err)
}

func precommitPartitionEntries(reclaim, dedicated machine.CPUSet) state.PodEntries {
	return state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult:         reclaim.Clone(),
				OriginalAllocationResult: reclaim.Clone(),
			},
		},
		"dedicated-pod": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "dedicated-pod",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameDedicated,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
				},
				AllocationResult:         dedicated.Clone(),
				OriginalAllocationResult: dedicated.Clone(),
			},
		},
	}
}

func precommitPartitionEntriesWithShare(reclaim, dedicated, share machine.CPUSet) state.PodEntries {
	entries := precommitPartitionEntries(reclaim, dedicated)
	entries[commonstate.PoolNameShare] = state.ContainerEntries{
		commonstate.FakedContainerName: &state.AllocationInfo{
			AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
			AllocationResult:         share.Clone(),
			OriginalAllocationResult: share.Clone(),
		},
	}
	return entries
}
