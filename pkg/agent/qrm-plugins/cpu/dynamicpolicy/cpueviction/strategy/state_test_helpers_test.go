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

package strategy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
)

func commitTargetForTest(t *testing.T, repository state.State, mutate func(*state.TargetState)) {
	t.Helper()
	target, err := repository.PrepareDurableTarget()
	require.NoError(t, err)
	mutate(target)
	require.NoError(t, repository.CommitTarget(target))
}

func setAllocationInfoForTest(
	t *testing.T,
	repository state.State,
	podUID, containerName string,
	allocationInfo *state.AllocationInfo,
) {
	t.Helper()
	commitTargetForTest(t, repository, func(target *state.TargetState) {
		if target.PodEntries[podUID] == nil {
			target.PodEntries[podUID] = make(state.ContainerEntries)
		}
		target.PodEntries[podUID][containerName] = allocationInfo.Clone()
	})
}

func setMachineStateForTest(t *testing.T, repository state.State, machineState state.NUMANodeMap) {
	t.Helper()
	commitTargetForTest(t, repository, func(target *state.TargetState) {
		target.MachineState = machineState.Clone()
	})
}

func setPodEntriesForTest(t *testing.T, repository state.State, podEntries state.PodEntries) {
	t.Helper()
	commitTargetForTest(t, repository, func(target *state.TargetState) {
		target.PodEntries = podEntries.Clone()
	})
}
