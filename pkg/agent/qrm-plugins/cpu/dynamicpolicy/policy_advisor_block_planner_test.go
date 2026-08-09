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
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/utilcomponent/featuregatenegotiation/finders/feature_cpu"
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

func TestGenerateBlockCPUSetOwnerUnionsStableAcrossRandomMapOrderAndBlockIDRotation(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	p.state.SetPodEntries(state.PodEntries{
		"pod-a": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod-a",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameDedicated,
				},
				AllocationResult: machine.NewCPUSet(0, 1),
				TopologyAwareAssignments: map[int]machine.CPUSet{
					0: machine.NewCPUSet(0, 1),
				},
			},
		},
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(2, 3),
				TopologyAwareAssignments: map[int]machine.CPUSet{
					0: machine.NewCPUSet(2, 3),
				},
			},
		},
	}, false)

	featureGates := map[string]*advisorsvc.FeatureGate{
		feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition: {
			Name: feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition,
		},
	}
	ownerUnions := func(t *testing.T, resp *advisorapi.ListAndWatchResponse, blocks advisorapi.BlockCPUSet) map[string]machine.CPUSet {
		t.Helper()
		got := make(map[string]machine.CPUSet)
		for entryName, entries := range resp.Entries {
			for subEntryName, info := range entries.Entries {
				cpus, err := info.GetCPUSet(entryName, subEntryName, blocks)
				require.NoError(t, err)
				owner := canonicalAdvisorBlockOwner(info.OwnerPoolName, entryName, subEntryName, "")
				got[owner] = got[owner].Union(cpus)
			}
		}
		return got
	}

	dedicatedOwner := canonicalAdvisorBlockOwner(
		commonstate.PoolNameDedicated, "pod-a", "main", "")
	reclaimOwner := canonicalAdvisorBlockOwner(
		commonstate.PoolNameReclaim, commonstate.PoolNameReclaim,
		commonstate.FakedContainerName, "")
	wantOwners := map[string]machine.CPUSet{
		dedicatedOwner: machine.NewCPUSet(0, 1),
		reclaimOwner:   machine.NewCPUSet(2, 3),
	}
	wantUnion := machine.NewCPUSet(0, 1, 2, 3)
	for seed := int64(0); seed < 1000; seed++ {
		aliases := []advisorBlockTestAlias{
			{
				entry: "pod-a", subEntry: "main", owner: commonstate.PoolNameDedicated,
				numaID: 0, blockID: fmt.Sprintf("dedicated-a-%d", seed), quantity: 1,
			},
			{
				entry: "pod-a", subEntry: "main", owner: commonstate.PoolNameDedicated,
				numaID: 0, blockID: fmt.Sprintf("dedicated-b-%d", seed), quantity: 1,
			},
			{
				entry: commonstate.PoolNameReclaim, subEntry: commonstate.FakedContainerName,
				owner: commonstate.PoolNameReclaim, numaID: 0,
				blockID: fmt.Sprintf("reclaim-%d", seed), quantity: 2,
			},
		}
		resp := advisorBlockTestResponse(aliases, rand.New(rand.NewSource(seed)))
		resp.DisableDedicatedCoresOverlapReclaimedCores = true

		blocks, err := p.generateBlockCPUSet(resp, featureGates)
		require.NoError(t, err, "seed %d", seed)
		got := ownerUnions(t, resp, blocks)
		require.Equal(t, wantOwners, got, "seed %d owner keys or assignments", seed)
		require.True(t, got[dedicatedOwner].Intersection(got[reclaimOwner]).IsEmpty(),
			"seed %d dedicated and reclaim owners overlap", seed)
		require.Equal(t, wantUnion, got[dedicatedOwner].Union(got[reclaimOwner]),
			"seed %d owner union does not cover the complete partition", seed)
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

func TestBuildAdvisorBlockDescriptors_FailsClosedForDifferentAliasResourcePackages(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	resp := advisorBlockTestResponse([]advisorBlockTestAlias{
		{entry: "pod-b", subEntry: "container-b", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-b"), numaID: 0, blockID: "alias", quantity: 2},
		{entry: "pod-a", subEntry: "container-a", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "alias", quantity: 2},
	}, rand.New(rand.NewSource(2)))

	_, err := buildAdvisorBlockDescriptors(
		resp,
		p.machineInfo.CPUDetails,
		nil,
		map[string]machine.CPUSet{
			"rp-a": machine.NewCPUSet(0, 1, 2),
			"rp-b": machine.NewCPUSet(1, 2, 3),
		},
		machine.NewCPUSet(),
	)
	require.ErrorContains(t, err, `block "alias" aliases have incompatible resource packages`)
}

func TestBuildAdvisorBlockDescriptors_EnforcesAliasResourcePackageCompatibility(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	tests := []struct {
		name      string
		aliases   []advisorBlockTestAlias
		wantError bool
	}{
		{
			name: "same non-empty resource package",
			aliases: []advisorBlockTestAlias{
				{entry: "pod-a", subEntry: "container-a", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "alias", quantity: 2},
				{entry: "pod-b", subEntry: "container-b", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "alias", quantity: 2},
			},
		},
		{
			name: "all unpinned",
			aliases: []advisorBlockTestAlias{
				{entry: "pod-a", subEntry: "container-a", owner: "pool-a", numaID: 0, blockID: "alias", quantity: 2},
				{entry: "pod-b", subEntry: "container-b", owner: "pool-a", numaID: 0, blockID: "alias", quantity: 2},
			},
		},
		{
			name: "pinned and unpinned",
			aliases: []advisorBlockTestAlias{
				{entry: "pod-a", subEntry: "container-a", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "alias", quantity: 2},
				{entry: "pod-b", subEntry: "container-b", owner: "pool-a", numaID: 0, blockID: "alias", quantity: 2},
			},
			wantError: true,
		},
		{
			name: "empty resource package reclaim owner does not conflict",
			aliases: []advisorBlockTestAlias{
				{entry: "pod-a", subEntry: "container-a", owner: resourcepackage.WrapOwnerPoolName("pool-a", "rp-a"), numaID: 0, blockID: "alias", quantity: 2, overlap: true},
				{entry: "reclaim", subEntry: commonstate.FakedContainerName, owner: commonstate.PoolNameReclaim, numaID: 0, blockID: "alias", quantity: 2, overlap: true},
			},
		},
	}

	for i, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			resp := advisorBlockTestResponse(tt.aliases, rand.New(rand.NewSource(int64(i))))
			_, err := buildAdvisorBlockDescriptors(
				resp,
				p.machineInfo.CPUDetails,
				nil,
				map[string]machine.CPUSet{"rp-a": machine.NewCPUSet(0, 1, 2)},
				machine.NewCPUSet(),
			)
			if tt.wantError {
				require.ErrorContains(t, err, `block "alias" aliases have incompatible resource packages`)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBuildAdvisorBlockDescriptors_RejectsDifferentResourcePackagesBeforeCapacityCheck(t *testing.T) {
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
	require.ErrorContains(t, err, `block "alias" aliases have incompatible resource packages`)
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
			name:  "real NUMA zero precedes fake NUMA",
			left:  advisorBlockDescriptor{NUMAID: 0},
			right: advisorBlockDescriptor{NUMAID: commonstate.FakedNUMAID},
		},
		{
			name:  "real NUMA one precedes fake NUMA",
			left:  advisorBlockDescriptor{NUMAID: 1},
			right: advisorBlockDescriptor{NUMAID: commonstate.FakedNUMAID},
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

func TestAdvisorBlockDescriptorLess_SortsRealNUMAsAscendingAndFakeNUMALast(t *testing.T) {
	t.Parallel()

	const iterations = 100
	for seed := int64(0); seed < iterations; seed++ {
		descriptors := []advisorBlockDescriptor{
			{BlockID: "fake", NUMAID: commonstate.FakedNUMAID},
			{BlockID: "one", NUMAID: 1},
			{BlockID: "zero", NUMAID: 0},
		}
		r := rand.New(rand.NewSource(seed))
		r.Shuffle(len(descriptors), func(i, j int) {
			descriptors[i], descriptors[j] = descriptors[j], descriptors[i]
		})
		sort.Slice(descriptors, func(i, j int) bool {
			return advisorBlockDescriptorLess(descriptors[i], descriptors[j])
		})
		require.Equal(t, []int{0, 1, commonstate.FakedNUMAID}, []int{
			descriptors[0].NUMAID,
			descriptors[1].NUMAID,
			descriptors[2].NUMAID,
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

func TestBuildAdvisorBlockDescriptors_AppliesEligibilityByOwnerPoolSemantics(t *testing.T) {
	t.Parallel()

	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	numa0 := p.machineInfo.CPUDetails.CPUsInNUMANodes(0)
	numa0CPUs := numa0.ToSliceInt()
	require.GreaterOrEqual(t, len(numa0CPUs), 4)
	pinned := machine.NewCPUSet(numa0CPUs[0], numa0CPUs[1])
	nonReclaimable := machine.NewCPUSet(numa0CPUs[len(numa0CPUs)-1])
	rpPinnedCPUSet := map[string]machine.CPUSet{"rp-a": pinned}

	t.Run("pinned shared and reclaim aliases intersect owner eligibility", func(t *testing.T) {
		resp := advisorBlockTestResponse([]advisorBlockTestAlias{
			{
				entry: "pod-a", subEntry: "container-a",
				owner:  resourcepackage.WrapOwnerPoolName(commonstate.PoolNameShare, "rp-a"),
				numaID: 0, blockID: "shared-reclaim", quantity: 1, overlap: true,
			},
			{
				entry: commonstate.PoolNameReclaim, subEntry: commonstate.FakedContainerName,
				owner:  commonstate.PoolNameReclaim,
				numaID: 0, blockID: "shared-reclaim", quantity: 1, overlap: true,
			},
		}, rand.New(rand.NewSource(10)))

		descriptors, err := buildAdvisorBlockDescriptors(
			resp, p.machineInfo.CPUDetails, nil, rpPinnedCPUSet, nonReclaimable,
		)
		require.NoError(t, err)
		require.Len(t, descriptors, 1)
		require.Equal(t, pinned, descriptors[0].Eligible)
	})

	t.Run("unpinned shared overlap excludes every pinned resource package", func(t *testing.T) {
		resp := advisorBlockTestResponse([]advisorBlockTestAlias{{
			entry: "pod-b", subEntry: "container-b", owner: commonstate.PoolNameShare,
			numaID: 0, blockID: "unpinned-shared", quantity: 1, overlap: true,
		}}, rand.New(rand.NewSource(11)))

		descriptors, err := buildAdvisorBlockDescriptors(
			resp, p.machineInfo.CPUDetails, nil, rpPinnedCPUSet, nonReclaimable,
		)
		require.NoError(t, err)
		require.Len(t, descriptors, 1)
		require.Equal(t, numa0.Difference(pinned), descriptors[0].Eligible)
	})

	t.Run("mandatory reclaim keeps excluding only non-reclaimable CPUs", func(t *testing.T) {
		resp := advisorBlockTestResponse([]advisorBlockTestAlias{{
			entry: commonstate.PoolNameReclaim, subEntry: commonstate.FakedContainerName,
			owner:  commonstate.PoolNameReclaim,
			numaID: 0, blockID: "mandatory-reclaim", quantity: 1,
		}}, rand.New(rand.NewSource(12)))

		descriptors, err := buildAdvisorBlockDescriptors(
			resp, p.machineInfo.CPUDetails, nil, rpPinnedCPUSet, nonReclaimable,
		)
		require.NoError(t, err)
		require.Len(t, descriptors, 1)
		require.Equal(t, numa0.Difference(nonReclaimable), descriptors[0].Eligible)
		require.True(t, pinned.IsSubsetOf(descriptors[0].Eligible))
	})
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

func TestBalancedHardReclaimQuotas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		quantity   int
		capacities map[int]int
		want       map[int]int
		wantError  string
	}{
		{
			name:       "four over two",
			quantity:   4,
			capacities: map[int]int{0: 4, 1: 4},
			want:       map[int]int{0: 2, 1: 2},
		},
		{
			name:       "five over two with both NUMAs sufficient",
			quantity:   5,
			capacities: map[int]int{0: 3, 1: 3},
			want:       map[int]int{0: 3, 1: 2},
		},
		{
			name:       "five over capacities two and three",
			quantity:   5,
			capacities: map[int]int{0: 2, 1: 3},
			want:       map[int]int{0: 2, 1: 3},
		},
		{
			name:       "eight over four",
			quantity:   8,
			capacities: map[int]int{0: 2, 1: 2, 2: 2, 3: 2},
			want:       map[int]int{0: 2, 1: 2, 2: 2, 3: 2},
		},
		{
			name:       "quantity below minimum",
			quantity:   3,
			capacities: map[int]int{0: 2, 1: 2},
			wantError:  "smaller than required minimum",
		},
		{
			name:      "no eligible NUMA",
			quantity:  4,
			wantError: "no eligible NUMA",
		},
		{
			name:       "infeasible base quota",
			quantity:   5,
			capacities: map[int]int{0: 1, 1: 4},
			wantError:  "NUMA 0 eligible capacity 1 is smaller than base quota 2",
		},
		{
			name:       "infeasible remainder",
			quantity:   5,
			capacities: map[int]int{0: 2, 1: 2},
			wantError:  "eligible capacities cannot satisfy quantity 5",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := balancedHardReclaimQuotas(tt.quantity, tt.capacities, 2)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExpandHardPartitionReclaimDemands(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0)
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1)
	available := topology.CPUDetails.CPUs()

	t.Run("fake NUMA mandatory reclaim is split and scoped by NUMA", func(t *testing.T) {
		descriptor := advisorBlockDescriptor{
			BlockID:      "reclaim",
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     4,
			ComponentKey: "mandatory-reclaim|owner|-1",
			Eligible:     numa0.Union(numa1),
			OldPreferred: machine.NewCPUSet(numa0.ToSliceInt()[0], numa1.ToSliceInt()[0]),
		}

		got, err := expandHardPartitionReclaimDemands(descriptor, available, topology)
		require.NoError(t, err)
		require.Equal(t, []partitionDemand{
			{
				key:       "mandatory-reclaim|owner|-1\x00block\x00reclaim\x00numa\x000",
				quantity:  2,
				eligible:  numa0,
				preferred: descriptor.OldPreferred.Intersection(numa0),
				class:     advisorBlockClassMandatoryReclaim,
			},
			{
				key:       "mandatory-reclaim|owner|-1\x00block\x00reclaim\x00numa\x001",
				quantity:  2,
				eligible:  numa1,
				preferred: descriptor.OldPreferred.Intersection(numa1),
				class:     advisorBlockClassMandatoryReclaim,
			},
		}, got)
	})

	t.Run("real NUMA mandatory reclaim remains one demand", func(t *testing.T) {
		numa1CPUs := numa1.ToSliceInt()
		descriptor := advisorBlockDescriptor{
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       1,
			Quantity:     2,
			ComponentKey: "real-reclaim",
			Eligible:     numa1,
			OldPreferred: numa1,
		}
		constrainedAvailable := machine.NewCPUSet(numa1CPUs[0], numa1CPUs[1])

		got, err := expandHardPartitionReclaimDemands(descriptor, constrainedAvailable, topology)
		require.NoError(t, err)
		require.Equal(t, []partitionDemand{{
			key:       descriptor.ComponentKey,
			quantity:  descriptor.Quantity,
			eligible:  constrainedAvailable,
			preferred: constrainedAvailable,
			class:     descriptor.Class,
		}}, got)
	})

	t.Run("non-reclaim fake NUMA remains one demand", func(t *testing.T) {
		descriptor := advisorBlockDescriptor{
			Class:        advisorBlockClassShared,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     3,
			ComponentKey: "shared",
			Eligible:     available,
			OldPreferred: numa0,
		}

		got, err := expandHardPartitionReclaimDemands(descriptor, available, topology)
		require.NoError(t, err)
		require.Equal(t, []partitionDemand{{
			key:       descriptor.ComponentKey,
			quantity:  descriptor.Quantity,
			eligible:  descriptor.Eligible,
			preferred: descriptor.OldPreferred,
			class:     descriptor.Class,
		}}, got)
	})

	t.Run("available and descriptor eligibility both constrain each NUMA", func(t *testing.T) {
		numa0CPUs, numa1CPUs := numa0.ToSliceInt(), numa1.ToSliceInt()
		descriptor := advisorBlockDescriptor{
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     4,
			ComponentKey: "constrained",
			Eligible: machine.NewCPUSet(
				numa0CPUs[0], numa0CPUs[1], numa0CPUs[2],
				numa1CPUs[0], numa1CPUs[1], numa1CPUs[2],
			),
			OldPreferred: available,
		}
		constrainedAvailable := machine.NewCPUSet(
			numa0CPUs[0], numa0CPUs[1],
			numa1CPUs[1], numa1CPUs[2],
		)

		got, err := expandHardPartitionReclaimDemands(descriptor, constrainedAvailable, topology)
		require.NoError(t, err)
		require.Equal(t, machine.NewCPUSet(numa0CPUs[0], numa0CPUs[1]), got[0].eligible)
		require.Equal(t, got[0].eligible, got[0].preferred)
		require.Equal(t, machine.NewCPUSet(numa1CPUs[1], numa1CPUs[2]), got[1].eligible)
		require.Equal(t, got[1].eligible, got[1].preferred)
	})

	t.Run("remainder skips NUMA without base plus one capacity", func(t *testing.T) {
		numa0CPUs, numa1CPUs := numa0.ToSliceInt(), numa1.ToSliceInt()
		descriptor := advisorBlockDescriptor{
			BlockID:      "capacity-aware",
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     5,
			ComponentKey: "capacity-aware-component",
			Eligible:     available,
		}
		constrainedAvailable := machine.NewCPUSet(
			numa0CPUs[0], numa0CPUs[1],
			numa1CPUs[0], numa1CPUs[1], numa1CPUs[2],
		)

		got, err := expandHardPartitionReclaimDemands(descriptor, constrainedAvailable, topology)
		require.NoError(t, err)
		require.Equal(t, 2, got[0].quantity)
		require.Equal(t, 3, got[1].quantity)
	})

	t.Run("expanded keys distinguish blocks with the same component key", func(t *testing.T) {
		first := advisorBlockDescriptor{
			BlockID:      "block-a",
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     4,
			ComponentKey: "same-component",
			Eligible:     available,
		}
		second := first
		second.BlockID = "block-b"

		firstDemands, err := expandHardPartitionReclaimDemands(first, available, topology)
		require.NoError(t, err)
		secondDemands, err := expandHardPartitionReclaimDemands(second, available, topology)
		require.NoError(t, err)

		keys := make(map[string]struct{})
		for _, demand := range append(firstDemands, secondDemands...) {
			keys[demand.key] = struct{}{}
		}
		require.Len(t, keys, len(firstDemands)+len(secondDemands))
	})

	t.Run("insufficient per NUMA capacity returns an error", func(t *testing.T) {
		numa0CPUs, numa1CPUs := numa0.ToSliceInt(), numa1.ToSliceInt()
		descriptor := advisorBlockDescriptor{
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     4,
			ComponentKey: "insufficient",
			Eligible: machine.NewCPUSet(
				numa0CPUs[0], numa0CPUs[1], numa0CPUs[2],
				numa1CPUs[0],
			),
		}

		_, err := expandHardPartitionReclaimDemands(descriptor, available, topology)
		require.ErrorContains(t, err, "NUMA 1 eligible capacity 1 is smaller than base quota 2")
	})
}
