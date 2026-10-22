// Copyright 2026 The Katalyst Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dynamicpolicy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func nonEmptyReclaimGuardEntries() state.PodEntries {
	return state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationResult: machine.MustParse("0-3"),
			},
		},
	}
}

func TestValidateFinalReclaimPool(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, validateFinalReclaimPool(emptyReclaimEntries()), errReclaimFallbackEmptyRequired)
	require.NoError(t, validateFinalReclaimPool(nonEmptyReclaimGuardEntries()))
}

func TestPreserveCommittedReclaimPool(t *testing.T) {
	t.Parallel()

	candidate := emptyReclaimEntries()
	committed := nonEmptyReclaimGuardEntries()
	require.NoError(t, preserveCommittedReclaimPool(candidate, committed))
	require.True(t, reclaimPoolCPUSet(candidate).Equals(machine.MustParse("0-3")))

	candidate[commonstate.PoolNameReclaim][commonstate.FakedContainerName].AllocationResult = machine.NewCPUSet(7)
	require.True(t, reclaimPoolCPUSet(committed).Equals(machine.MustParse("0-3")),
		"candidate must receive a clone, not alias committed state")

	require.ErrorIs(t, preserveCommittedReclaimPool(emptyReclaimEntries(), emptyReclaimEntries()),
		errReclaimFallbackEmptyRequired)
}

func TestPreparePendingCPUPartitionDisabledReclaimRejectsEmptyWithoutPublishing(t *testing.T) {
	t.Parallel()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	p.config.dynamicConfig.GetDynamicConfiguration().EnableReclaim = false

	committedEntries := precommitPartitionEntries(machine.NewCPUSet(4, 52), machine.NewCPUSet(2, 3))
	committedMachineState, err := generateMachineStateFromPodEntries(
		p.machine.machineInfo.CPUTopology, committedEntries, p.state.GetMachineState())
	require.NoError(t, err)
	require.NoError(t, p.state.CommitAdvisorState(
		committedEntries, committedMachineState, false, true, false))

	beforeRevision := p.state.GetRevision()
	beforeReclaim := reclaimPoolCPUSet(p.state.GetPodEntries())
	require.False(t, beforeReclaim.IsEmpty())

	candidate := precommitPartitionEntries(machine.NewCPUSet(), machine.NewCPUSet(2, 3))
	_, _, err = p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		disableDedicated: true,
		persist:          false,
	})
	require.ErrorIs(t, err, errReclaimFallbackEmptyRequired)
	require.Equal(t, beforeRevision, p.state.GetRevision(), "failed empty candidate must not advance revision")
	require.True(t, reclaimPoolCPUSet(p.state.GetPodEntries()).Equals(beforeReclaim),
		"failed empty candidate must not overwrite the committed reclaim pool")
}

func TestDisabledReclaimDedicatedOverlapProjectionRemainsNonEmpty(t *testing.T) {
	t.Parallel()
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dynamicConf := p.config.dynamicConfig.GetDynamicConfiguration()
	dynamicConf.EnableReclaim = false

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(0, 1))
	override, err := p.buildAdjustmentCommitOverrideFromPodEntriesWithDynamicConfig(
		candidate, false, false, dynamicConf)
	require.NoError(t, err)
	require.NotNil(t, override)
	require.True(t, override.ReclaimEffective.Equals(machine.NewCPUSet(0, 1)),
		"disabled reclaim must preserve the authorized ordinary-dedicated overlap")
	require.NoError(t, p.syncReclaimPoolWithAdjustmentCommitOverrideWithDynamicConfig(
		candidate, override, dynamicConf))

	beforeRevision := p.state.GetRevision()
	committed, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		allowOverlap:     false,
		disableDedicated: false,
		persist:          false,
		dynamicConfig:    dynamicConf,
	})
	require.NoError(t, err)
	require.Greater(t, p.state.GetRevision(), beforeRevision)
	require.True(t, reclaimPoolCPUSet(committed).Equals(machine.NewCPUSet(0, 1)))
	require.True(t,
		committed["dedicated-pod"]["main"].AllocationResult.Equals(machine.NewCPUSet(0, 1)),
		"ordinary dedicated ownership must remain unchanged in overlap mode")
}
