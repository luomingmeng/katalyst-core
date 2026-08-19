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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// coresInNUMA returns the cpuset of the [start,end) cores (both SMT siblings per
// core) of the given NUMA, ordered by ascending core id. It lets tests build
// core-aligned inputs regardless of the sibling-id offset the dummy topology
// uses, so a "reclaim" or "donor" fixture never accidentally holds a half core.
func coresInNUMA(topology *machine.CPUTopology, numaID, start, end int) machine.CPUSet {
	cores := topology.CPUDetails.CoresInNUMANodes(numaID).ToSliceInt()
	if start < 0 {
		start = 0
	}
	if end > len(cores) {
		end = len(cores)
	}
	if start >= end {
		return machine.NewCPUSet()
	}
	return topology.CPUDetails.CPUsInCores(cores[start:end]...)
}

// requireCoreAligned fails when reclaim holds a partial physical core.
func requireCoreAligned(t *testing.T, topology *machine.CPUTopology, reclaim machine.CPUSet) {
	t.Helper()
	require.NoErrorf(t, assertCoreAligned(reclaim, topology),
		"reclaim %s must be core-aligned", reclaim.String())
}

func TestPlanHardReclaimPartitionKeepsSixCPUsOnEvery32CPUNUMA(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
	require.NoError(t, err)
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0)
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1)
	require.Equal(t, 32, numa0.Size())
	require.Equal(t, 32, numa1.Size())

	for _, tc := range []struct {
		name            string
		free            machine.CPUSet
		currentReclaim  machine.CPUSet
		donors          []hardReclaimPartitionDonor
		reclaimEligible machine.CPUSet
	}{
		{
			// shared / SNB: the whole NUMA is free, reclaim carves three complete
			// cores per NUMA.
			name:            "shared and SNB",
			free:            numa0.Union(numa1),
			currentReclaim:  machine.NewCPUSet(),
			reclaimEligible: numa0.Union(numa1),
		},
		{
			// ordinary DNB: two free cores plus one core of donor excess per NUMA.
			name: "ordinary DNB",
			free: coresInNUMA(topology, 0, 0, 2).Union(coresInNUMA(topology, 1, 0, 2)),
			donors: []hardReclaimPartitionDonor{
				{key: "dnb-0", cpus: coresInNUMA(topology, 0, 2, 7), requestQuantity: 8},
				{key: "dnb-1", cpus: coresInNUMA(topology, 1, 2, 7), requestQuantity: 8},
			},
			reclaimEligible: numa0.Union(numa1),
		},
		{
			// exclusive DNB: reclaim comes purely from three free cores; the donor
			// holds exactly its request so no excess is handed back.
			name: "exclusive DNB",
			free: coresInNUMA(topology, 0, 0, 3).Union(coresInNUMA(topology, 1, 0, 3)),
			donors: []hardReclaimPartitionDonor{
				{key: "exclusive-0", cpus: coresInNUMA(topology, 0, 3, 16), requestQuantity: 26},
				{key: "exclusive-1", cpus: coresInNUMA(topology, 1, 3, 16), requestQuantity: 26},
			},
			reclaimEligible: numa0.Union(numa1),
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plan, err := planHardReclaimPartition(hardReclaimPartitionInput{
				topology:        topology,
				targetByNUMA:    map[int]int{0: 6, 1: 6},
				currentReclaim:  tc.currentReclaim,
				free:            tc.free,
				reclaimEligible: tc.reclaimEligible,
				donors:          tc.donors,
			})
			require.NoError(t, err)
			require.Equal(t, 6, plan.reclaim.Intersection(numa0).Size())
			require.Equal(t, 6, plan.reclaim.Intersection(numa1).Size())
			requireCoreAligned(t, topology, plan.reclaim)
		})
	}
}

// TestPlanHardReclaimPartitionSelectsCompletePhysicalCores reconstructs the exact
// node symptom: a naive lowest-id fill on an SMT2 topology strands the high
// siblings and yields {0-20,96-102}-style half cores. The core-aligned selection
// must instead keep both siblings of every chosen core.
func TestPlanHardReclaimPartitionSelectsCompletePhysicalCores(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
	require.NoError(t, err)
	require.Equal(t, 2, topology.CPUsPerCore())
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0)

	plan, err := planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 6},
		currentReclaim:  machine.NewCPUSet(),
		free:            numa0,
		reclaimEligible: numa0,
	})
	require.NoError(t, err)
	require.Equal(t, 6, plan.reclaim.Size())
	requireCoreAligned(t, topology, plan.reclaim)
}

// TestPlanHardReclaimPartitionIsIdempotentOnAlignedInput proves an already
// core-aligned currentReclaim is reused byte-for-byte (no churn).
func TestPlanHardReclaimPartitionIsIdempotentOnAlignedInput(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
	require.NoError(t, err)
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0)
	current := coresInNUMA(topology, 0, 0, 3)

	plan, err := planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 6},
		currentReclaim:  current,
		free:            numa0.Difference(current),
		reclaimEligible: numa0,
	})
	require.NoError(t, err)
	require.True(t, plan.reclaim.Equals(current), "aligned reclaim must be reused: got %s want %s",
		plan.reclaim.String(), current.String())
}

// TestPlanHardReclaimPartitionNonSMTZeroDrift proves that on a non-SMT topology
// (CPUsPerCore()==1) every cpu is its own core, so selection reduces to a
// prefer-first lowest-id take with no behavioral drift.
func TestPlanHardReclaimPartitionNonSMTZeroDrift(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(16, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, topology.CPUsPerCore())
	numa := topology.CPUDetails.CPUsInNUMANodes(0)

	plan, err := planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 5},
		currentReclaim:  machine.NewCPUSet(),
		free:            numa,
		reclaimEligible: numa,
	})
	require.NoError(t, err)
	require.Equal(t, 5, plan.reclaim.Size())
	requireCoreAligned(t, topology, plan.reclaim)
}

func TestPlanHardReclaimPartitionSubtractsSatisfiedFloorBeforeAllocation(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	// three complete cores already reclaimed; the fourth core comes from free.
	currentFloor := coresInNUMA(topology, 0, 0, 3)
	free := coresInNUMA(topology, 0, 3, 5)

	plan, err := planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 8},
		currentReclaim:  currentFloor,
		free:            free,
		reclaimEligible: numa,
	})
	require.NoError(t, err)
	require.Equal(t, 8, plan.reclaim.Size(), "advisor quantity already includes the six-CPU floor")
	require.True(t, currentFloor.IsSubsetOf(plan.reclaim))
	require.Equal(t, 2, plan.reclaim.Difference(currentFloor).Size())
	requireCoreAligned(t, topology, plan.reclaim)
}

func TestPlanHardReclaimPartitionDonatesSameNUMADedicatedExcess(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	free := coresInNUMA(topology, 0, 0, 2)      // two free cores
	dedicated := coresInNUMA(topology, 0, 2, 6) // four dedicated cores, request 6 cpus (three cores)

	plan, err := planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 6},
		free:            free,
		reclaimEligible: numa,
		donors: []hardReclaimPartitionDonor{{
			key: "dnb", cpus: dedicated, requestQuantity: 6,
		}},
	})
	require.NoError(t, err)
	require.Equal(t, 6, plan.reclaim.Size())
	require.Equal(t, 6, plan.donorCPUs["dnb"].Size())
	require.Equal(t, 2, plan.reclaim.Intersection(dedicated).Size())
	require.True(t, plan.reclaim.Intersection(plan.donorCPUs["dnb"]).IsEmpty())
	requireCoreAligned(t, topology, plan.reclaim)
	// the donor's retained set must also stay core-aligned: excess is handed back
	// in complete cores, never a lone SMT sibling.
	requireCoreAligned(t, topology, plan.donorCPUs["dnb"])
}

func TestPlanHardReclaimPartitionRejectsDonationBelowCeilRequest(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	free := coresInNUMA(topology, 0, 0, 2)      // two free cores (four cpus)
	dedicated := coresInNUMA(topology, 0, 2, 6) // four dedicated cores (eight cpus)

	// request floor ceil(6.2)=7 leaves only one cpu of excess, which is less than a
	// complete core, so no core can be handed back; the fourth reclaim core cannot
	// be satisfied.
	_, err = planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 6},
		free:            free,
		reclaimEligible: numa,
		donors: []hardReclaimPartitionDonor{{
			key: "dnb", cpus: dedicated, requestQuantity: 6.2,
		}},
	})
	require.ErrorContains(t, err, "NUMA 0 needs 2 more reclaim CPUs")
}

func TestPlanHardReclaimPartitionChecksRequestFloorForLegacyOverlappingReclaim(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	free := coresInNUMA(topology, 0, 0, 2)
	dedicated := coresInNUMA(topology, 0, 2, 6)
	currentReclaim := coresInNUMA(topology, 0, 2, 3) // one core already overlapping the donor

	// even with an overlapping legacy reclaim core, the request floor ceil(6.2)=7
	// leaves under one core of donatable excess, so the extra reclaim core is
	// rejected rather than stealing below the request floor.
	_, err = planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 6},
		currentReclaim:  currentReclaim,
		free:            free,
		reclaimEligible: numa,
		donors: []hardReclaimPartitionDonor{{
			key: "dnb", cpus: dedicated, requestQuantity: 6.2,
		}},
	})
	require.ErrorContains(t, err, "NUMA 0 needs")
}

func TestPlanHardReclaimPartitionSharesRequestFloorAcrossNUMADonors(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 1, 2)
	require.NoError(t, err)
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0)
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1)
	donor0 := coresInNUMA(topology, 0, 0, 3) // three cores (six cpus)
	donor1 := coresInNUMA(topology, 1, 0, 3) // three cores (six cpus)

	plan, err := planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 2, 1: 2},
		reclaimEligible: numa0.Union(numa1),
		donors: []hardReclaimPartitionDonor{
			{key: "dnb-numa0", groupKey: "pod/main", cpus: donor0, requestQuantity: 8},
			{key: "dnb-numa1", groupKey: "pod/main", cpus: donor1, requestQuantity: 8},
		},
	})

	require.NoError(t, err)
	require.Equal(t, 2, plan.reclaim.Intersection(numa0).Size())
	require.Equal(t, 2, plan.reclaim.Intersection(numa1).Size())
	require.Equal(t, 8, plan.donorCPUs["dnb-numa0"].Size()+plan.donorCPUs["dnb-numa1"].Size())
	requireCoreAligned(t, topology, plan.reclaim)
}
