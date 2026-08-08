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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

type advisorBlockTestAlias struct {
	entry    string
	subEntry string
	owner    string
	numaID   int64
	blockID  string
	quantity uint64
}

func advisorBlockTestResponse(aliases []advisorBlockTestAlias, r *rand.Rand) *advisorapi.ListAndWatchResponse {
	shuffled := append([]advisorBlockTestAlias(nil), aliases...)
	r.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	resp := &advisorapi.ListAndWatchResponse{Entries: make(map[string]*advisorapi.CalculationEntries)}
	for _, alias := range shuffled {
		if resp.Entries[alias.entry] == nil {
			resp.Entries[alias.entry] = &advisorapi.CalculationEntries{
				Entries: make(map[string]*advisorapi.CalculationInfo),
			}
		}
		resp.Entries[alias.entry].Entries[alias.subEntry] = &advisorapi.CalculationInfo{
			OwnerPoolName: alias.owner,
			CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
				alias.numaID: {
					Blocks: []*advisorapi.Block{{
						BlockId: alias.blockID,
						Result:  alias.quantity,
					}},
				},
			},
		}
	}
	return resp
}

func TestBuildAdvisorBlockDescriptors_StableAcrossMapOrderAndBlockIDRotation(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	aliases := []advisorBlockTestAlias{
		{entry: "pool-b", subEntry: commonstate.FakedContainerName, owner: "pool-b", numaID: 0, blockID: "old-b", quantity: 2},
		{entry: "pool-a", subEntry: commonstate.FakedContainerName, owner: "pool-a", numaID: 0, blockID: "old-a", quantity: 2},
		{entry: commonstate.PoolNameReclaim, subEntry: commonstate.FakedContainerName, owner: commonstate.PoolNameReclaim, numaID: commonstate.FakedNUMAID, blockID: "old-r", quantity: 4},
	}

	var stableKeys []string
	for seed := int64(0); seed < 20; seed++ {
		rotated := append([]advisorBlockTestAlias(nil), aliases...)
		for i := range rotated {
			rotated[i].blockID = string(rune('z'-i)) + "-" + string(rune('a'+int(seed%7)))
		}
		resp := advisorBlockTestResponse(rotated, rand.New(rand.NewSource(seed)))
		descriptors, err := buildAdvisorBlockDescriptors(
			resp,
			p.machineInfo.CPUDetails,
			p.state.GetPodEntries(),
			nil,
			machine.NewCPUSet(),
		)
		require.NoError(t, err)

		keys := make([]string, 0, len(descriptors))
		for _, descriptor := range descriptors {
			keys = append(keys, descriptor.ComponentKey)
		}
		if seed == 0 {
			stableKeys = keys
		} else {
			require.Equal(t, stableKeys, keys)
		}
	}
}

func TestBuildAdvisorBlockDescriptors_IntersectsAliasEligibilityAndAggregatesOldPreferred(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	p.state.SetPodEntries(state.PodEntries{
		"pod-a": {
			"container-a": &state.AllocationInfo{
				AllocationResult: machine.NewCPUSet(1, 2),
			},
		},
		"pool-a": {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationResult: machine.NewCPUSet(2, 3),
			},
		},
	}, false)

	resp := advisorBlockTestResponse([]advisorBlockTestAlias{
		{entry: "pod-a", subEntry: "container-a", owner: "pool-a", numaID: 0, blockID: "alias", quantity: 2},
		{entry: "pool-a", subEntry: commonstate.FakedContainerName, owner: "pool-a", numaID: 0, blockID: "alias", quantity: 2},
	}, rand.New(rand.NewSource(1)))

	descriptors, err := buildAdvisorBlockDescriptors(
		resp,
		p.machineInfo.CPUDetails,
		p.state.GetPodEntries(),
		nil,
		machine.NewCPUSet(),
	)
	require.NoError(t, err)
	require.Len(t, descriptors, 1)
	require.Equal(t, []string{
		"pool-a\x00pod-a\x00container-a",
		"pool-a\x00pool-a\x00" + commonstate.FakedContainerName,
	}, descriptors[0].Owners)
	require.Equal(t, machine.NewCPUSet(1, 2, 3), descriptors[0].OldPreferred)
}

func TestBuildAdvisorBlockDescriptors_FailsClosedForAliasRPConflict(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	resp := advisorBlockTestResponse([]advisorBlockTestAlias{
		{entry: "pod-a", subEntry: "container-a", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "alias", quantity: 1},
		{entry: "pod-b", subEntry: "container-b", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-b"), numaID: 0, blockID: "alias", quantity: 1},
	}, rand.New(rand.NewSource(2)))

	_, err := buildAdvisorBlockDescriptors(
		resp,
		p.machineInfo.CPUDetails,
		nil,
		map[string]machine.CPUSet{
			"rp-a": machine.NewCPUSet(0, 1),
			"rp-b": machine.NewCPUSet(2, 3),
		},
		machine.NewCPUSet(),
	)
	require.ErrorContains(t, err, "incompatible resource packages")
}

func TestBuildAdvisorBlockDescriptors_EnforcesRPAndReclaimBoundaries(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	resp := advisorBlockTestResponse([]advisorBlockTestAlias{
		{entry: "pinned", subEntry: commonstate.FakedContainerName, owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "pinned", quantity: 2},
		{entry: "unpinned", subEntry: commonstate.FakedContainerName, owner: "pool-b", numaID: 0, blockID: "unpinned", quantity: 2},
		{entry: "reclaim", subEntry: commonstate.FakedContainerName, owner: commonstate.PoolNameReclaim, numaID: 0, blockID: "reclaim", quantity: 2},
	}, rand.New(rand.NewSource(3)))

	numa0 := p.machineInfo.CPUDetails.CPUsInNUMANodes(0)
	numa0CPUs := numa0.ToSliceInt()
	pinned := machine.NewCPUSet(numa0CPUs[0], numa0CPUs[1])
	nonReclaimable := machine.NewCPUSet(numa0CPUs[2])
	descriptors, err := buildAdvisorBlockDescriptors(
		resp,
		p.machineInfo.CPUDetails,
		nil,
		map[string]machine.CPUSet{"rp-a": pinned},
		nonReclaimable,
	)
	require.NoError(t, err)

	byID := make(map[string]advisorBlockDescriptor)
	for _, descriptor := range descriptors {
		byID[descriptor.BlockID] = descriptor
	}
	require.Equal(t, pinned, byID["pinned"].Eligible)
	require.True(t, byID["unpinned"].Eligible.Intersection(pinned).IsEmpty())
	require.True(t, byID["reclaim"].Eligible.Intersection(nonReclaimable).IsEmpty())
	require.True(t, pinned.IsSubsetOf(byID["reclaim"].Eligible))
}

func TestBuildAdvisorBlockDescriptors_FailsClosedWhenEligibleCapacityIsInsufficient(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	resp := advisorBlockTestResponse([]advisorBlockTestAlias{
		{entry: "pinned", subEntry: commonstate.FakedContainerName, owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "pinned", quantity: 3},
	}, rand.New(rand.NewSource(4)))

	_, err := buildAdvisorBlockDescriptors(
		resp,
		p.machineInfo.CPUDetails,
		nil,
		map[string]machine.CPUSet{"rp-a": machine.NewCPUSet(0, 1)},
		machine.NewCPUSet(),
	)
	require.ErrorContains(t, err, "eligible capacity")
}
