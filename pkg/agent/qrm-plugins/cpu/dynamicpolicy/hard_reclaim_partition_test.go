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
			name:            "shared and SNB",
			free:            numa0.Union(numa1),
			currentReclaim:  machine.NewCPUSet(),
			reclaimEligible: numa0.Union(numa1),
		},
		{
			name: "ordinary DNB",
			free: machine.NewCPUSet(
				append(numa0.ToSliceInt()[:4], numa1.ToSliceInt()[:4]...)...),
			donors: []hardReclaimPartitionDonor{
				{key: "dnb-0", cpus: machine.NewCPUSet(numa0.ToSliceInt()[4:14]...), requestQuantity: 8},
				{key: "dnb-1", cpus: machine.NewCPUSet(numa1.ToSliceInt()[4:14]...), requestQuantity: 8},
			},
			reclaimEligible: numa0.Union(numa1),
		},
		{
			name: "exclusive DNB",
			free: machine.NewCPUSet(
				append(numa0.ToSliceInt()[:6], numa1.ToSliceInt()[:6]...)...),
			donors: []hardReclaimPartitionDonor{
				{key: "exclusive-0", cpus: machine.NewCPUSet(numa0.ToSliceInt()[6:]...), requestQuantity: 26},
				{key: "exclusive-1", cpus: machine.NewCPUSet(numa1.ToSliceInt()[6:]...), requestQuantity: 26},
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
		})
	}
}

func TestPlanHardReclaimPartitionSubtractsSatisfiedFloorBeforeAllocation(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	cpus := numa.ToSliceInt()
	currentFloor := machine.NewCPUSet(cpus[:6]...)
	free := machine.NewCPUSet(cpus[6:10]...)

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
}

func TestPlanHardReclaimPartitionDonatesSameNUMADedicatedExcess(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	cpus := numa.ToSliceInt()
	free := machine.NewCPUSet(cpus[:4]...)
	dedicated := machine.NewCPUSet(cpus[4:12]...)

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
}

func TestPlanHardReclaimPartitionRejectsDonationBelowCeilRequestAndNonReclaimableCPUs(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	cpus := numa.ToSliceInt()
	free := machine.NewCPUSet(cpus[:4]...)
	dedicated := machine.NewCPUSet(cpus[4:12]...)
	nonReclaimable := machine.NewCPUSet(cpus[11])

	_, err = planHardReclaimPartition(hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 6},
		free:            free,
		reclaimEligible: numa.Difference(nonReclaimable),
		donors: []hardReclaimPartitionDonor{{
			key: "dnb", cpus: dedicated, requestQuantity: 6.2,
		}},
	})
	require.ErrorContains(t, err, "NUMA 0 needs 1 more reclaim CPUs")
}

func TestPlanHardReclaimPartitionChecksRequestFloorForLegacyOverlappingReclaim(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	free := machine.NewCPUSet(numa.ToSliceInt()[:4]...)
	dedicated := machine.NewCPUSet(numa.ToSliceInt()[4:12]...)
	currentReclaim := machine.NewCPUSet(numa.ToSliceInt()[4:6]...)

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

	require.ErrorContains(t, err, "NUMA 0 needs 1 more reclaim CPUs")
}

func TestPlanHardReclaimPartitionSharesRequestFloorAcrossNUMADonors(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 1, 2)
	require.NoError(t, err)
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0)
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1)
	donor0 := machine.NewCPUSet(numa0.ToSliceInt()[:6]...)
	donor1 := machine.NewCPUSet(numa1.ToSliceInt()[:6]...)

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
}
