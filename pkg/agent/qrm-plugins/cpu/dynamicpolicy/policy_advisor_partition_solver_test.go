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

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestSolveDisjointPartitionsFindsConstrainedAssignment(t *testing.T) {
	input := []partitionDemand{
		{
			key:       "a-reclaim",
			quantity:  2,
			eligible:  machine.NewCPUSet(0, 1, 2, 3),
			preferred: machine.NewCPUSet(0, 1),
			class:     advisorBlockClassMandatoryReclaim,
		},
		{
			key:       "z-dedicated-rp",
			quantity:  2,
			eligible:  machine.NewCPUSet(0, 1),
			preferred: machine.NewCPUSet(),
			class:     advisorBlockClassDedicated,
		},
	}

	got, err := solveDisjointPartitions(input, partitionSolverFixtureTopology())
	require.NoError(t, err)
	require.Equal(t, machine.NewCPUSet(0, 1), got["z-dedicated-rp"])
	require.Equal(t, machine.NewCPUSet(2, 3), got["a-reclaim"])
}

func TestSolveDisjointPartitionsKeepsLegalOldCPUs(t *testing.T) {
	input := []partitionDemand{
		{
			key:       "dedicated",
			quantity:  3,
			eligible:  machine.NewCPUSet(0, 1, 2, 3),
			preferred: machine.NewCPUSet(0, 1),
			class:     advisorBlockClassDedicated,
		},
		{
			key:       "reclaim",
			quantity:  1,
			eligible:  machine.NewCPUSet(2, 3),
			preferred: machine.NewCPUSet(3),
			class:     advisorBlockClassMandatoryReclaim,
		},
	}

	got, err := solveDisjointPartitions(input, partitionSolverFixtureTopology())
	require.NoError(t, err)
	require.True(t, machine.NewCPUSet(0, 1).IsSubsetOf(got["dedicated"]))
	require.Equal(t, machine.NewCPUSet(3), got["reclaim"])
}

func TestSolveDisjointPartitionsHonorsResourcePackageEligibility(t *testing.T) {
	input := []partitionDemand{
		{
			key:       "reclaim",
			quantity:  2,
			eligible:  machine.NewCPUSet(0, 1, 2, 3, 4, 5),
			preferred: machine.NewCPUSet(),
			class:     advisorBlockClassMandatoryReclaim,
		},
		{
			key:       "dedicated-rp-a",
			quantity:  2,
			eligible:  machine.NewCPUSet(4, 5),
			preferred: machine.NewCPUSet(),
			class:     advisorBlockClassDedicated,
		},
	}

	got, err := solveDisjointPartitions(input, partitionSolverFixtureTopology())
	require.NoError(t, err)
	require.Equal(t, machine.NewCPUSet(4, 5), got["dedicated-rp-a"])
	require.Equal(t, machine.NewCPUSet(0, 1), got["reclaim"])
	require.True(t, got["dedicated-rp-a"].Intersection(got["reclaim"]).IsEmpty())
}

func TestSolveDisjointPartitionsPrioritizesOldCPUsBeforeReclaimGD(t *testing.T) {
	input := []partitionDemand{
		{key: "dedicated", quantity: 1, eligible: machine.NewCPUSet(0, 1), preferred: machine.NewCPUSet(0), class: advisorBlockClassDedicated},
		{key: "reclaim", quantity: 1, eligible: machine.NewCPUSet(0, 1, 2), preferred: machine.NewCPUSet(1), class: advisorBlockClassMandatoryReclaim},
	}

	got, err := solveDisjointPartitions(input, partitionSolverFixtureTopology())
	require.NoError(t, err)
	require.Equal(t, machine.NewCPUSet(0), got["dedicated"])
	require.Equal(t, machine.NewCPUSet(1), got["reclaim"])
}

func TestSolveDisjointPartitionsUsesCPUIDAsFinalTieBreak(t *testing.T) {
	got, err := solveDisjointPartitions([]partitionDemand{{
		key:      "dedicated",
		quantity: 2,
		eligible: machine.NewCPUSet(0, 1, 2, 3),
		class:    advisorBlockClassDedicated,
	}}, partitionSolverFixtureTopology())

	require.NoError(t, err)
	require.Equal(t, machine.NewCPUSet(0, 1), got["dedicated"])
}

func TestSolveDisjointPartitionsPrefersSmallestTopologyMigration(t *testing.T) {
	input := []partitionDemand{{
		key:       "dedicated",
		quantity:  1,
		eligible:  machine.NewCPUSet(1, 2, 4),
		preferred: machine.NewCPUSet(0),
		class:     advisorBlockClassDedicated,
	}}

	got, err := solveDisjointPartitions(input, partitionSolverFixtureTopology())
	require.NoError(t, err)
	require.Equal(t, machine.NewCPUSet(1), got["dedicated"])
}

func TestSolveDisjointPartitionsIsIndependentOfDemandOrder(t *testing.T) {
	base := []partitionDemand{
		{key: "dedicated-b", quantity: 1, eligible: machine.NewCPUSet(0, 1, 2), preferred: machine.NewCPUSet(2), class: advisorBlockClassDedicated},
		{key: "reclaim", quantity: 2, eligible: machine.NewCPUSet(0, 1, 2, 3, 4), preferred: machine.NewCPUSet(0), class: advisorBlockClassMandatoryReclaim},
		{key: "dedicated-a", quantity: 1, eligible: machine.NewCPUSet(0, 1), preferred: machine.NewCPUSet(0), class: advisorBlockClassDedicated},
	}
	want, err := solveDisjointPartitions(base, partitionSolverFixtureTopology())
	require.NoError(t, err)

	for seed := int64(0); seed < 100; seed++ {
		shuffled := append([]partitionDemand(nil), base...)
		rand.New(rand.NewSource(seed)).Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		got, solveErr := solveDisjointPartitions(shuffled, partitionSolverFixtureTopology())
		require.NoError(t, solveErr, "seed %d", seed)
		require.Equal(t, want, got, "seed %d", seed)
	}
}

func TestSolveDisjointPartitionsKeepsComponentUnionAcrossResponseKeyRotation(t *testing.T) {
	type componentDemand struct {
		component string
		demand    partitionDemand
	}

	solveComponentUnions := func(frame []componentDemand) map[string]machine.CPUSet {
		demands := make([]partitionDemand, 0, len(frame))
		componentsByKey := make(map[string]string, len(frame))
		for _, item := range frame {
			demands = append(demands, item.demand)
			componentsByKey[item.demand.key] = item.component
		}

		assignments, err := solveDisjointPartitions(demands, partitionSolverFixtureTopology())
		require.NoError(t, err)

		unions := make(map[string]machine.CPUSet)
		for key, cpus := range assignments {
			component := componentsByKey[key]
			unions[component] = unions[component].Union(cpus)
		}
		return unions
	}

	firstFrame := []componentDemand{
		{
			component: "owner-a/component-a",
			demand: partitionDemand{
				key:       "response-key-1",
				quantity:  1,
				eligible:  machine.NewCPUSet(0, 1, 2),
				preferred: machine.NewCPUSet(0),
				class:     advisorBlockClassDedicated,
			},
		},
		{
			component: "owner-a/component-a",
			demand: partitionDemand{
				key:       "response-key-2",
				quantity:  1,
				eligible:  machine.NewCPUSet(0, 1, 2),
				preferred: machine.NewCPUSet(1),
				class:     advisorBlockClassDedicated,
			},
		},
		{
			component: "owner-b/component-b",
			demand: partitionDemand{
				key:       "response-key-3",
				quantity:  1,
				eligible:  machine.NewCPUSet(1, 2, 3),
				preferred: machine.NewCPUSet(2),
				class:     advisorBlockClassMandatoryReclaim,
			},
		},
	}
	secondFrame := []componentDemand{
		{
			component: "owner-a/component-a",
			demand: partitionDemand{
				key:       "response-key-2",
				quantity:  1,
				eligible:  machine.NewCPUSet(0, 1, 2),
				preferred: machine.NewCPUSet(0),
				class:     advisorBlockClassDedicated,
			},
		},
		{
			component: "owner-a/component-a",
			demand: partitionDemand{
				key:       "response-key-1",
				quantity:  1,
				eligible:  machine.NewCPUSet(0, 1, 2),
				preferred: machine.NewCPUSet(1),
				class:     advisorBlockClassDedicated,
			},
		},
		{
			component: "owner-b/component-b",
			demand: partitionDemand{
				key:       "response-key-3",
				quantity:  1,
				eligible:  machine.NewCPUSet(1, 2, 3),
				preferred: machine.NewCPUSet(2),
				class:     advisorBlockClassMandatoryReclaim,
			},
		},
	}

	require.Equal(t, solveComponentUnions(firstFrame), solveComponentUnions(secondFrame))
}

func TestSolveDisjointPartitionsFailsClosed(t *testing.T) {
	got, err := solveDisjointPartitions([]partitionDemand{
		{key: "dedicated", quantity: 2, eligible: machine.NewCPUSet(0, 1), class: advisorBlockClassDedicated},
		{key: "reclaim", quantity: 1, eligible: machine.NewCPUSet(1), class: advisorBlockClassMandatoryReclaim},
	}, partitionSolverFixtureTopology())

	require.ErrorContains(t, err, "partition demands have no feasible assignment")
	require.Nil(t, got)
}

func partitionSolverFixtureTopology() *machine.CPUTopology {
	return &machine.CPUTopology{
		NumCPUs:      8,
		NumCores:     4,
		NumSockets:   2,
		NumNUMANodes: 2,
		CPUDetails: machine.CPUDetails{
			0: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			1: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			2: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			3: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			4: {NUMANodeID: 1, SocketID: 1, CoreID: 2},
			5: {NUMANodeID: 1, SocketID: 1, CoreID: 2},
			6: {NUMANodeID: 1, SocketID: 1, CoreID: 3},
			7: {NUMANodeID: 1, SocketID: 1, CoreID: 3},
		},
	}
}
