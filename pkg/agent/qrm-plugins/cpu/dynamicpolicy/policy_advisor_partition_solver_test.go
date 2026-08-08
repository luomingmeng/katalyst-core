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
	"testing"
	"time"

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

func TestPartitionCostWeightsBoundary(t *testing.T) {
	oldWeight, reclaimWeight, topologyWeight, err := partitionCostWeights(8192, 8192, 681)
	require.NoError(t, err)
	require.Greater(t, topologyWeight, int64(0))
	require.Greater(t, reclaimWeight, topologyWeight)
	require.Greater(t, oldWeight, reclaimWeight)

	oldWeight, reclaimWeight, topologyWeight, err = partitionCostWeights(8192, 8192, 682)
	require.ErrorContains(t, err, "partition cost overflow")
	require.Zero(t, oldWeight)
	require.Zero(t, reclaimWeight)
	require.Zero(t, topologyWeight)
}

func TestSolveDisjointPartitionsCostOverflowFailsClosed(t *testing.T) {
	demands, topology := densePartitionSolverFixture(8192, 682)
	for i := range demands {
		demands[i].quantity = 12
		if i < 8 {
			demands[i].quantity++
		}
	}

	got, err := solveDisjointPartitions(demands, topology)

	require.ErrorContains(t, err, "partition cost overflow")
	require.Nil(t, got)
}

func TestSolveDisjointPartitionsDense1024CompletesQuickly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dense performance probe in short mode")
	}

	demands, topology := densePartitionSolverFixture(1024, 1024)
	started := time.Now()
	got, err := solveDisjointPartitions(demands, topology)
	elapsed := time.Since(started)

	require.NoError(t, err)
	require.Len(t, got, len(demands))
	require.Less(t, elapsed, 2*time.Second, "dense solver probe took %s", elapsed)
}

func TestSolveDisjointPartitionsFailsFastAtEdgeBudget(t *testing.T) {
	demands, topology := densePartitionSolverFixture(1024, 1025)
	demands[len(demands)-1].quantity = 0

	got, err := solveDisjointPartitions(demands, topology)

	require.ErrorContains(t, err, "partition graph edge budget exceeded")
	require.Nil(t, got)
}

func BenchmarkSolveDisjointPartitionsDense1024(b *testing.B) {
	demands, topology := densePartitionSolverFixture(1024, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := solveDisjointPartitions(demands, topology)
		require.NoError(b, err)
		require.Len(b, got, len(demands))
	}
}

func densePartitionSolverFixture(cpuCount, demandCount int) ([]partitionDemand, *machine.CPUTopology) {
	cpus := make([]int, cpuCount)
	details := make(machine.CPUDetails, cpuCount)
	for cpu := range cpus {
		cpus[cpu] = cpu
		details[cpu] = machine.CPUTopoInfo{
			NUMANodeID: cpu / 64,
			SocketID:   cpu / 512,
			CoreID:     cpu / 2,
		}
	}
	eligible := machine.NewCPUSet(cpus...)
	demands := make([]partitionDemand, demandCount)
	for i := range demands {
		demands[i] = partitionDemand{
			key:      fmt.Sprintf("demand-%04d", i),
			quantity: 1,
			eligible: eligible,
			class:    advisorBlockClassDedicated,
		}
	}
	return demands, &machine.CPUTopology{
		NumCPUs:      cpuCount,
		NumCores:     (cpuCount + 1) / 2,
		NumSockets:   (cpuCount + 511) / 512,
		NumNUMANodes: (cpuCount + 63) / 64,
		CPUDetails:   details,
	}
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
