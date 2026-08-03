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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestTargetStateCloneDeepCopiesMutableFields(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)

	target := &TargetState{
		PodEntries: PodEntries{
			"pod": {
				"container": &AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						Labels: map[string]string{"source": "base"},
					},
				},
			},
		},
		MachineState:                               GetDefaultMachineState(topology),
		NUMAHeadroom:                               map[int]float64{0: 1.5},
		AllowSharedCoresOverlapReclaimedCores:      true,
		DisableDedicatedCoresOverlapReclaimedCores: true,
	}

	cloned := target.Clone()
	cloned.PodEntries["pod"]["container"].Labels["source"] = "clone"
	cloned.MachineState[0].PodEntries["new-pod"] = ContainerEntries{}
	cloned.NUMAHeadroom[0] = 9.5

	require.Equal(t, "base", target.PodEntries["pod"]["container"].Labels["source"])
	require.NotContains(t, target.MachineState[0].PodEntries, "new-pod")
	require.Equal(t, 1.5, target.NUMAHeadroom[0])
	require.True(t, cloned.AllowSharedCoresOverlapReclaimedCores)
	require.True(t, cloned.DisableDedicatedCoresOverlapReclaimedCores)
	require.Nil(t, (*TargetState)(nil).Clone())
}
