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
	overlap  bool
}

func advisorBlockTestResponse(aliases []advisorBlockTestAlias, r *rand.Rand) *advisorapi.ListAndWatchResponse {
	shuffled := append([]advisorBlockTestAlias(nil), aliases...)
	r.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	resp := &advisorapi.ListAndWatchResponse{Entries: make(map[string]*advisorapi.CalculationEntries)}
	for _, alias := range shuffled {
		var overlapTargets []*advisorapi.OverlapTarget
		if alias.overlap {
			overlapTargets = []*advisorapi.OverlapTarget{{
				OverlapTargetPoolName: commonstate.PoolNameShare,
				OverlapType:           advisorapi.OverlapType_OverlapWithPool,
			}}
		}
		if resp.Entries[alias.entry] == nil {
			resp.Entries[alias.entry] = &advisorapi.CalculationEntries{
				Entries: make(map[string]*advisorapi.CalculationInfo),
			}
		}
		info := resp.Entries[alias.entry].Entries[alias.subEntry]
		if info == nil {
			info = &advisorapi.CalculationInfo{
				OwnerPoolName:             alias.owner,
				CalculationResultsByNumas: make(map[int64]*advisorapi.NumaCalculationResult),
			}
			resp.Entries[alias.entry].Entries[alias.subEntry] = info
		}
		if info.OwnerPoolName != alias.owner {
			panic("test aliases reuse an entry/sub-entry with different owners")
		}
		if info.CalculationResultsByNumas[alias.numaID] == nil {
			info.CalculationResultsByNumas[alias.numaID] = &advisorapi.NumaCalculationResult{}
		}
		info.CalculationResultsByNumas[alias.numaID].Blocks = append(
			info.CalculationResultsByNumas[alias.numaID].Blocks,
			&advisorapi.Block{
				BlockId:        alias.blockID,
				Result:         alias.quantity,
				OverlapTargets: overlapTargets,
			},
		)
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

	var stableDescriptors []advisorBlockDescriptor
	for seed := int64(0); seed < 20; seed++ {
		rotated := append([]advisorBlockTestAlias(nil), aliases...)
		resp := advisorBlockTestResponse(rotated, rand.New(rand.NewSource(seed)))
		descriptors, err := buildAdvisorBlockDescriptors(
			resp,
			p.machineInfo.CPUDetails,
			p.state.GetPodEntries(),
			nil,
			machine.NewCPUSet(),
		)
		require.NoError(t, err)

		if seed == 0 {
			stableDescriptors = descriptors
		} else {
			require.Equal(t, stableDescriptors, descriptors)
		}
	}
}

func TestBuildAdvisorBlockDescriptors_BlockIDIsOnlyFinalTieBreak(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	build := func(ids []string) []advisorBlockDescriptor {
		resp := advisorBlockTestResponse([]advisorBlockTestAlias{
			{entry: "pool-a", subEntry: commonstate.FakedContainerName, owner: "pool-a", numaID: 0, blockID: ids[0], quantity: 2},
			{entry: "pool-a", subEntry: commonstate.FakedContainerName, owner: "pool-a", numaID: 0, blockID: ids[1], quantity: 2},
		}, rand.New(rand.NewSource(7)))
		descriptors, err := buildAdvisorBlockDescriptors(resp, p.machineInfo.CPUDetails, nil, nil, machine.NewCPUSet())
		require.NoError(t, err)
		return descriptors
	}

	first := build([]string{"block-b", "block-a"})
	rotated := build([]string{"new-z", "new-y"})
	require.Equal(t, []string{"block-a", "block-b"}, []string{first[0].BlockID, first[1].BlockID})
	require.Equal(t, []string{"new-y", "new-z"}, []string{rotated[0].BlockID, rotated[1].BlockID})
	for i := range first {
		first[i].BlockID = ""
		rotated[i].BlockID = ""
	}
	require.Equal(t, first, rotated)
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
		"pool-a\x00pod-a\x00container-a\x00",
		"pool-a\x00pool-a\x00" + commonstate.FakedContainerName + "\x00",
	}, descriptors[0].Owners)
	require.Equal(t, machine.NewCPUSet(1, 2, 3), descriptors[0].OldPreferred)
}

func TestBuildAdvisorBlockDescriptors_IntersectsDifferentAliasRPEligibility(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	resp := advisorBlockTestResponse([]advisorBlockTestAlias{
		{entry: "pod-b", subEntry: "container-b", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-b"), numaID: 0, blockID: "alias", quantity: 2},
		{entry: "pod-a", subEntry: "container-a", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "alias", quantity: 2},
	}, rand.New(rand.NewSource(2)))

	descriptors, err := buildAdvisorBlockDescriptors(
		resp,
		p.machineInfo.CPUDetails,
		nil,
		map[string]machine.CPUSet{
			"rp-a": machine.NewCPUSet(0, 1, 2),
			"rp-b": machine.NewCPUSet(1, 2, 3),
		},
		machine.NewCPUSet(),
	)
	require.NoError(t, err)
	require.Equal(t, []advisorBlockDescriptor{{
		BlockID: "alias",
		Owners: []string{
			"pool-a\x00pod-a\x00container-a\x00rp-a",
			"pool-a\x00pod-b\x00container-b\x00rp-b",
		},
		Class:        advisorBlockClassShared,
		NUMAID:       0,
		Quantity:     2,
		ComponentKey: "shared|pool-a\x00pod-a\x00container-a\x00rp-a\x1fpool-a\x00pod-b\x00container-b\x00rp-b|0",
		Eligible:     machine.NewCPUSet(1, 2),
		OldPreferred: machine.NewCPUSet(),
	}}, descriptors)
}

func TestBuildAdvisorBlockDescriptors_FailsClosedWhenAliasEligibilityIntersectionIsInsufficient(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	resp := advisorBlockTestResponse([]advisorBlockTestAlias{
		{entry: "pod-a", subEntry: "container-a", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "alias", quantity: 2},
		{entry: "pod-b", subEntry: "container-b", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-b"), numaID: 0, blockID: "alias", quantity: 2},
	}, rand.New(rand.NewSource(5)))

	_, err := buildAdvisorBlockDescriptors(
		resp,
		p.machineInfo.CPUDetails,
		nil,
		map[string]machine.CPUSet{
			"rp-a": machine.NewCPUSet(0, 1),
			"rp-b": machine.NewCPUSet(1, 2),
		},
		machine.NewCPUSet(),
	)
	require.ErrorContains(t, err, "eligible capacity 1 is smaller than quantity 2")
}

func TestBuildAdvisorBlockDescriptors_ClassifiesAllBlockClasses(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	resp := advisorBlockTestResponse([]advisorBlockTestAlias{
		{entry: "static", owner: commonstate.PoolNameReserve, numaID: 0, blockID: "static", quantity: 1},
		{entry: "mandatory", owner: commonstate.PoolNameReclaim, numaID: 0, blockID: "mandatory", quantity: 1},
		{entry: "dedicated", owner: commonstate.PoolNameDedicated, numaID: 0, blockID: "dedicated", quantity: 1},
		{entry: "shared", owner: commonstate.PoolNameShare, numaID: 0, blockID: "shared", quantity: 1},
		{entry: "overlap", owner: commonstate.PoolNameReclaim, numaID: 0, blockID: "overlap", quantity: 1, overlap: true},
	}, rand.New(rand.NewSource(6)))

	descriptors, err := buildAdvisorBlockDescriptors(
		resp, p.machineInfo.CPUDetails, nil, nil, machine.NewCPUSet(),
	)
	require.NoError(t, err)
	require.Equal(t, []advisorBlockClass{
		advisorBlockClassStatic,
		advisorBlockClassMandatoryReclaim,
		advisorBlockClassDedicated,
		advisorBlockClassShared,
		advisorBlockClassReclaimOverlap,
	}, []advisorBlockClass{
		descriptors[0].Class,
		descriptors[1].Class,
		descriptors[2].Class,
		descriptors[3].Class,
		descriptors[4].Class,
	})
}

func TestBuildAdvisorBlockDescriptors_ReportsClassificationErrors(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	t.Run("empty owner", func(t *testing.T) {
		resp := advisorBlockTestResponse([]advisorBlockTestAlias{{
			entry: "bad", numaID: 0, blockID: "bad", quantity: 1,
		}}, rand.New(rand.NewSource(8)))
		_, err := buildAdvisorBlockDescriptors(resp, p.machineInfo.CPUDetails, nil, nil, machine.NewCPUSet())
		require.ErrorContains(t, err, "cannot classify empty owner pool")
	})

	t.Run("nil overlap target", func(t *testing.T) {
		resp := advisorBlockTestResponse([]advisorBlockTestAlias{{
			entry: "bad", owner: commonstate.PoolNameReclaim, numaID: 0, blockID: "bad", quantity: 1,
		}}, rand.New(rand.NewSource(9)))
		resp.Entries["bad"].Entries[""].CalculationResultsByNumas[0].Blocks[0].OverlapTargets =
			[]*advisorapi.OverlapTarget{nil}
		_, err := buildAdvisorBlockDescriptors(resp, p.machineInfo.CPUDetails, nil, nil, machine.NewCPUSet())
		require.ErrorContains(t, err, "cannot classify nil overlap target")
	})
}

func TestAdvisorBlockDescriptorLess_UsesCanonicalKeyOrder(t *testing.T) {
	t.Parallel()

	base := advisorBlockDescriptor{
		BlockID:      "block-b",
		Owners:       []string{"owner-b"},
		Class:        advisorBlockClassShared,
		NUMAID:       1,
		Quantity:     2,
		ComponentKey: "component-b",
	}
	tests := []struct {
		name  string
		left  advisorBlockDescriptor
		right advisorBlockDescriptor
	}{
		{
			name:  "NUMA precedes class",
			left:  advisorBlockDescriptor{NUMAID: 0, Class: advisorBlockClassReclaimOverlap},
			right: advisorBlockDescriptor{NUMAID: 1, Class: advisorBlockClassStatic},
		},
		{
			name: "class precedes component key",
			left: func() advisorBlockDescriptor {
				d := base
				d.Class, d.ComponentKey = advisorBlockClassDedicated, "component-z"
				return d
			}(),
			right: func() advisorBlockDescriptor {
				d := base
				d.Class, d.ComponentKey = advisorBlockClassShared, "component-a"
				return d
			}(),
		},
		{
			name: "component key precedes quantity",
			left: func() advisorBlockDescriptor {
				d := base
				d.ComponentKey, d.Quantity = "component-a", 9
				return d
			}(),
			right: func() advisorBlockDescriptor {
				d := base
				d.ComponentKey, d.Quantity = "component-b", 1
				return d
			}(),
		},
		{
			name: "quantity precedes alias signature",
			left: func() advisorBlockDescriptor {
				d := base
				d.Quantity, d.Owners = 1, []string{"owner-z"}
				return d
			}(),
			right: func() advisorBlockDescriptor {
				d := base
				d.Quantity, d.Owners = 2, []string{"owner-a"}
				return d
			}(),
		},
		{
			name: "alias signature precedes block id",
			left: func() advisorBlockDescriptor {
				d := base
				d.Owners, d.BlockID = []string{"owner-a"}, "block-z"
				return d
			}(),
			right: func() advisorBlockDescriptor {
				d := base
				d.Owners, d.BlockID = []string{"owner-b"}, "block-a"
				return d
			}(),
		},
		{
			name: "block id is final tie break",
			left: func() advisorBlockDescriptor {
				d := base
				d.BlockID = "block-a"
				return d
			}(),
			right: base,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.True(t, advisorBlockDescriptorLess(tt.left, tt.right))
			require.False(t, advisorBlockDescriptorLess(tt.right, tt.left))
		})
	}
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
