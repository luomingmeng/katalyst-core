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
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestSteadyFakeNUMAMigrationChurn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		old  machine.CPUSet
		next machine.CPUSet
		want int
	}{
		{"pure expansion", machine.NewCPUSet(0, 1), machine.NewCPUSet(0, 1, 2, 3), 0},
		{"pure shrink", machine.NewCPUSet(0, 1, 2, 3), machine.NewCPUSet(0, 1), 0},
		{"one replacement", machine.NewCPUSet(0, 1), machine.NewCPUSet(0, 2), 2},
		{"expansion with replacement", machine.NewCPUSet(0, 1), machine.NewCPUSet(0, 2, 3, 4), 2},
		{"shrink with replacement", machine.NewCPUSet(0, 1, 2, 3), machine.NewCPUSet(0, 4), 2},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, steadyFakeNUMAMigrationChurn(tt.old, tt.next))
		})
	}
}

func TestSolveSteadyFakeNUMAWholeCoreDelegatesFinalProjection(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	fake := coresInNUMA(topology, 0, 0, 2)
	demands := stagedMigrationDemands(all, fake, fake.Size())
	called := false

	got, err := solveSteadyFakeNUMAWholeCoreWithFloorsAndProject(
		demands,
		[]string{"fake"},
		nil,
		topology,
		func(
			_ []partitionDemand,
			_ []string,
			committed machine.CPUSet,
			desired map[string]machine.CPUSet,
			_ []partitionCoreFloorConstraint,
			_ *machine.CPUTopology,
		) (map[string]machine.CPUSet, error) {
			called = true
			require.Equal(t, fake, committed)
			return desired, nil
		},
	)

	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, fake, got["fake"])
}

func TestProjectSteadyFakeNUMAStageConvergesInBoundedStages(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	current := coresInNUMA(topology, 0, 0, 6)
	target := coresInNUMA(topology, 0, 6, 12)
	desired := map[string]machine.CPUSet{
		"fake":  target,
		"share": all.Difference(target),
	}

	for cycle := 0; cycle < 3 && !current.Equals(target); cycle++ {
		demands := []partitionDemand{
			{
				key: "fake", quantity: target.Size(), eligible: all,
				preferred: current, class: advisorBlockClassMandatoryReclaim,
			},
			{
				key: "share", quantity: all.Size() - target.Size(), eligible: all,
				preferred: all.Difference(current), class: advisorBlockClassShared,
			},
		}
		next, solveErr := projectSteadyFakeNUMAStage(
			demands, []string{"fake"}, current, desired, nil, topology)
		require.NoError(t, solveErr)
		require.NoError(t, assertCoreAligned(next["fake"], topology))
		require.Equal(t, target.Size(), next["fake"].Size())
		require.LessOrEqual(t,
			steadyFakeNUMAMigrationChurn(current, next["fake"]),
			steadyFakeNUMAMaxMigratedCPUs)
		require.NotEqual(t, current, next["fake"])
		current = next["fake"]
	}
	require.Equal(t, target, current)
}

func TestProjectSteadyFakeNUMAStageSupportsSMT1SMT2AndSMT4(t *testing.T) {
	t.Parallel()

	smt1, err := machine.GenerateDummyCPUTopologyWithoutSMT(12, 1, 1)
	require.NoError(t, err)
	smt2, err := machine.GenerateDummyCPUTopology(24, 1, 1)
	require.NoError(t, err)
	smt4 := testSteadyFakeNUMATopology(12, 4)

	for _, tc := range []struct {
		name     string
		topology *machine.CPUTopology
		cores    int
	}{
		{name: "SMT1", topology: smt1, cores: 6},
		{name: "SMT2", topology: smt2, cores: 6},
		{name: "SMT4", topology: smt4, cores: 3},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			all := tc.topology.CPUDetails.CPUs()
			current := coresInNUMA(tc.topology, 0, 0, tc.cores)
			target := coresInNUMA(tc.topology, 0, tc.cores, 2*tc.cores)
			desired := map[string]machine.CPUSet{
				"fake":  target,
				"share": all.Difference(target),
			}
			maxCycles := steadyFakeNUMAMigrationChurn(current, target)/
				steadyFakeNUMAMaxMigratedCPUs + 1

			for cycle := 0; cycle < maxCycles && !current.Equals(target); cycle++ {
				demands := stagedMigrationDemands(all, current, target.Size())
				next, solveErr := projectSteadyFakeNUMAStage(
					demands, []string{"fake"}, current, desired, nil, tc.topology)
				require.NoError(t, solveErr)
				require.NoError(t, assertCoreAligned(next["fake"], tc.topology))
				require.Equal(t, target.Size(), next["fake"].Size())
				require.LessOrEqual(t,
					steadyFakeNUMAMigrationChurn(current, next["fake"]),
					steadyFakeNUMAMaxMigratedCPUs)
				current = next["fake"]
			}
			require.Equal(t, target, current)
		})
	}
}

func TestProjectSteadyFakeNUMAStagePreservesNUMAQuotaAndDonorFloor(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(32, 1, 2)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	current0 := coresInNUMA(topology, 0, 0, 3)
	current1 := coresInNUMA(topology, 1, 0, 3)
	target0 := coresInNUMA(topology, 0, 3, 6)
	target1 := coresInNUMA(topology, 1, 3, 6)
	current := current0.Union(current1)
	target := target0.Union(target1)
	donorTarget := coresInNUMA(topology, 0, 6, 7)
	desired := map[string]machine.CPUSet{
		"fake-0": target0,
		"fake-1": target1,
		"donor":  donorTarget,
		"share":  all.Difference(target).Difference(donorTarget),
	}
	demands := []partitionDemand{
		{
			key: "fake-0", quantity: target0.Size(),
			eligible:  topology.CPUDetails.CPUsInNUMANodes(0),
			preferred: current0, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "fake-1", quantity: target1.Size(),
			eligible:  topology.CPUDetails.CPUsInNUMANodes(1),
			preferred: current1, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "donor", quantity: donorTarget.Size(), eligible: all,
			preferred: donorTarget, class: advisorBlockClassDedicated,
		},
		{
			key: "share", quantity: all.Size() - target.Size() - donorTarget.Size(),
			eligible: all, preferred: all.Difference(current).Difference(donorTarget),
			class: advisorBlockClassShared,
		},
	}

	next, err := projectSteadyFakeNUMAStage(
		demands,
		[]string{"fake-0", "fake-1"},
		current,
		desired,
		[]partitionCoreFloorConstraint{{demandKey: "donor"}},
		topology,
	)

	require.NoError(t, err)
	require.Equal(t, target0.Size(), next["fake-0"].Size())
	require.Equal(t, target1.Size(), next["fake-1"].Size())
	require.NoError(t, assertCoreAligned(next["fake-0"].Union(next["fake-1"]), topology))
	require.NoError(t, assertCoreAligned(next["donor"], topology))
	require.LessOrEqual(t,
		steadyFakeNUMAMigrationChurn(current, next["fake-0"].Union(next["fake-1"])),
		steadyFakeNUMAMaxMigratedCPUs)
}

func TestProjectSteadyFakeNUMAStageUsesLatestCommittedStateAndIsIdempotent(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	initial := coresInNUMA(topology, 0, 0, 6)
	targetA := coresInNUMA(topology, 0, 6, 12)
	targetB := initial
	desiredA := map[string]machine.CPUSet{
		"fake": targetA, "share": all.Difference(targetA),
	}

	demands := stagedMigrationDemands(all, initial, targetA.Size())
	first, err := projectSteadyFakeNUMAStage(
		demands, []string{"fake"}, initial, desiredA, nil, topology)
	require.NoError(t, err)
	retry, err := projectSteadyFakeNUMAStage(
		demands, []string{"fake"}, initial, desiredA, nil, topology)
	require.NoError(t, err)
	require.Equal(t, first, retry)

	committed := first["fake"]
	nextTowardA, err := projectSteadyFakeNUMAStage(
		stagedMigrationDemands(all, committed, targetA.Size()),
		[]string{"fake"}, committed, desiredA, nil, topology)
	require.NoError(t, err)
	require.NotEqual(t, committed, nextTowardA["fake"])

	desiredB := map[string]machine.CPUSet{
		"fake": targetB, "share": all.Difference(targetB),
	}
	redirected, err := projectSteadyFakeNUMAStage(
		stagedMigrationDemands(all, committed, targetB.Size()),
		[]string{"fake"}, committed, desiredB, nil, topology)
	require.NoError(t, err)
	require.Equal(t, targetB, redirected["fake"])
	require.NotEqual(t, nextTowardA["fake"], redirected["fake"])
	require.LessOrEqual(t,
		steadyFakeNUMAMigrationChurn(committed, redirected["fake"]),
		steadyFakeNUMAMaxMigratedCPUs)
	converged, err := projectSteadyFakeNUMAStage(
		stagedMigrationDemands(all, targetB, targetB.Size()),
		[]string{"fake"}, targetB, desiredB, nil, topology)
	require.NoError(t, err)
	require.Equal(t, desiredB, converged)
}

func TestProjectSteadyFakeNUMAStageDoesNotChargePureResize(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	small := coresInNUMA(topology, 0, 0, 2)
	large := coresInNUMA(topology, 0, 0, 6)

	for _, tc := range []struct {
		name      string
		committed machine.CPUSet
		target    machine.CPUSet
	}{
		{name: "expansion", committed: small, target: large},
		{name: "shrink", committed: large, target: small},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			desired := map[string]machine.CPUSet{
				"fake": tc.target, "share": all.Difference(tc.target),
			}
			next, solveErr := projectSteadyFakeNUMAStage(
				stagedMigrationDemands(all, tc.committed, tc.target.Size()),
				[]string{"fake"}, tc.committed, desired, nil, topology)
			require.NoError(t, solveErr)
			require.Equal(t, desired, next)
			require.Zero(t, steadyFakeNUMAMigrationChurn(tc.committed, next["fake"]))
		})
	}
}

func TestProjectSteadyFakeNUMAStageRejectsPartialRepairOfFragmentedCommittedState(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	committed := machine.NewCPUSet()
	for _, coreID := range topology.CPUDetails.Cores().ToSliceInt()[:20] {
		committed.Add(topology.CPUDetails.CPUsInCores(coreID).ToSliceInt()[0])
	}
	target := coresInNUMA(topology, 0, 0, 10)
	desired := map[string]machine.CPUSet{
		"fake": target, "share": all.Difference(target),
	}

	next, err := projectSteadyFakeNUMAStage(
		stagedMigrationDemands(all, committed, target.Size()),
		[]string{"fake"}, committed, desired, nil, topology)

	require.Nil(t, next)
	require.ErrorContains(t, err, "invalid committed reclaim requires atomic repair")
}

func TestProjectSteadyFakeNUMAStageValidatesCommittedBeforeFastPath(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	target := coresInNUMA(topology, 0, 0, 3)
	desired := map[string]machine.CPUSet{
		"fake":  target,
		"share": all.Difference(target),
	}

	for _, tc := range []struct {
		name      string
		committed machine.CPUSet
		want      string
	}{
		{
			name: "fragmented",
			committed: func() machine.CPUSet {
				result := machine.NewCPUSet()
				for _, coreID := range topology.CPUDetails.Cores().ToSliceInt()[2:] {
					result.Add(topology.CPUDetails.CPUsInCores(coreID).ToSliceInt()[0])
				}
				return result
			}(),
			want: "invalid committed reclaim requires atomic repair",
		},
		{
			name:      "outside topology",
			committed: target.Union(machine.NewCPUSet(100, 101)),
			want:      "outside machine topology",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, projectErr := projectSteadyFakeNUMAStage(
				stagedMigrationDemands(all, tc.committed, target.Size()),
				[]string{"fake"}, tc.committed, desired, nil, topology)
			require.Nil(t, got)
			require.ErrorContains(t, projectErr, tc.want)
		})
	}
}

func TestProjectSteadyFakeNUMAStagePreservesPreferredDonorCore(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	desiredFake := coresInNUMA(topology, 0, 0, 3)
	currentFake := coresInNUMA(topology, 0, 3, 6)
	preferredDonor := coresInNUMA(topology, 0, 0, 1)
	desired := map[string]machine.CPUSet{
		"fake":  desiredFake,
		"donor": coresInNUMA(topology, 0, 3, 4),
		"share": all.Difference(desiredFake).Difference(coresInNUMA(topology, 0, 3, 4)),
	}
	demands := []partitionDemand{
		{
			key: "fake", quantity: desiredFake.Size(), eligible: all,
			preferred: currentFake, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "donor", quantity: preferredDonor.Size(), eligible: all,
			preferred: preferredDonor, class: advisorBlockClassDedicated,
		},
		{
			key: "share", quantity: all.Size() - desiredFake.Size() - preferredDonor.Size(),
			eligible: all, preferred: all.Difference(currentFake).Difference(preferredDonor),
			class: advisorBlockClassShared,
		},
	}

	next, err := projectSteadyFakeNUMAStage(
		demands, []string{"fake"}, currentFake, desired, nil, topology)

	require.NoError(t, err)
	require.True(t, preferredDonor.IsSubsetOf(next["donor"]),
		"stage displaced preferred donor core %s: donor=%s fake=%s",
		preferredDonor, next["donor"], next["fake"])
	require.LessOrEqual(t,
		steadyFakeNUMAMigrationChurn(currentFake, next["fake"]),
		steadyFakeNUMAMaxMigratedCPUs)
}

func TestProjectSteadyFakeNUMAStageFailsClosedWhenSearchIsTruncatedAfterFindingBest(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	current := coresInNUMA(topology, 0, 0, 8)
	target := coresInNUMA(topology, 0, 8, 16)
	desired := map[string]machine.CPUSet{
		"fake": target, "share": all.Difference(target),
	}
	demands := stagedMigrationDemands(all, current, target.Size())

	for _, tc := range []struct {
		name   string
		budget steadyFakeNUMASearchBudget
		want   string
	}{
		{
			name: "candidate budget",
			budget: steadyFakeNUMASearchBudget{
				maxSolveAttempts:    10_000,
				maxCandidateActions: 4,
			},
			want: "staged migration search budget 4 exhausted",
		},
		{
			name: "solve budget",
			budget: steadyFakeNUMASearchBudget{
				maxSolveAttempts:    1,
				maxCandidateActions: 10_000,
			},
			want: "staged migration solve budget 1 exhausted",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, projectErr := projectSteadyFakeNUMAStageWithBudget(
				demands, []string{"fake"}, current, desired, nil, topology, tc.budget)

			require.Nil(t, got,
				"a truncated search must not return a provisional best assignment")
			require.ErrorContains(t, projectErr, tc.want)
		})
	}
}

func TestProjectSteadyFakeNUMAStageDoesNotSwallowPinsForUnionBudgetExhaustion(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	current := coresInNUMA(topology, 0, 0, 8)
	target := coresInNUMA(topology, 0, 8, 16)
	desired := map[string]machine.CPUSet{
		"fake": target, "share": all.Difference(target),
	}
	demands := stagedMigrationDemands(all, current, target.Size())
	calls := 0

	got, projectErr := projectSteadyFakeNUMAStageWithBudgetAndPins(
		demands,
		[]string{"fake"},
		current,
		desired,
		nil,
		topology,
		steadyFakeNUMASearchBudget{
			maxSolveAttempts:    10_000,
			maxCandidateActions: 10_000,
		},
		func(
			target machine.CPUSet,
			fakeKeys []string,
			demandByKey map[string]partitionDemand,
			desired map[string]machine.CPUSet,
			topology *machine.CPUTopology,
			_ *steadyFakeNUMASearchTracker,
		) (map[string][]machine.CPUSet, error) {
			calls++
			if calls == 2 {
				return nil, newSteadyFakeNUMAPinBudgetExhaustedError(7)
			}
			return steadyFakeNUMAPinsForUnion(
				target, fakeKeys, demandByKey, desired, topology)
		},
	)

	require.Equal(t, 2, calls, "the first candidate must establish a provisional best")
	require.Nil(t, got, "pin-search truncation must invalidate a provisional best")
	require.ErrorContains(t, projectErr, "pin assignment budget 7 exhausted")
}

func TestProjectSteadyFakeNUMAStageSharesCandidateAndPinBudget(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(10, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	current := coresInNUMA(topology, 0, 0, 5)
	target := coresInNUMA(topology, 0, 5, 10)
	desired := map[string]machine.CPUSet{
		"fake": target, "share": all.Difference(target),
	}

	got, projectErr := projectSteadyFakeNUMAStageWithBudget(
		stagedMigrationDemands(all, current, target.Size()),
		[]string{"fake"},
		current,
		desired,
		nil,
		topology,
		steadyFakeNUMASearchBudget{
			maxSolveAttempts:    10_000,
			maxCandidateActions: 25,
		},
	)

	require.Nil(t, got, "a pin search that consumes the shared budget must fail closed")
	require.ErrorContains(t, projectErr, "staged migration search budget 25 exhausted")
}

func TestProjectSteadyFakeNUMAStageRejectsInvalidCommittedSourceSnapshotBeyondAtomicBudget(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	current := coresInNUMA(topology, 0, 0, 8)
	target := coresInNUMA(topology, 0, 8, 16)
	desired := map[string]machine.CPUSet{
		"fake":  target,
		"donor": coresInNUMA(topology, 0, 16, 17),
		"share": all.Difference(target).Difference(coresInNUMA(topology, 0, 16, 17)),
	}

	for _, tc := range []struct {
		name    string
		demands []partitionDemand
		want    string
	}{
		{
			name: "quantity snapshot mismatch",
			demands: []partitionDemand{
				{
					key: "fake", quantity: target.Size(), eligible: all,
					preferred: current.Difference(machine.NewCPUSet(7)),
					class:     advisorBlockClassMandatoryReclaim,
				},
				{
					key: "donor", quantity: 1, eligible: all,
					preferred: desired["donor"], class: advisorBlockClassDedicated,
				},
				{
					key: "share", quantity: all.Size() - target.Size() - 1, eligible: all,
					preferred: desired["share"], class: advisorBlockClassShared,
				},
			},
			want: "committed fake snapshot",
		},
		{
			name: "fake eligibility violation",
			demands: []partitionDemand{
				{
					key: "fake", quantity: current.Size(),
					eligible:  all.Difference(machine.NewCPUSet(0)),
					preferred: current, class: advisorBlockClassMandatoryReclaim,
				},
				{
					key: "donor", quantity: 1, eligible: all,
					preferred: desired["donor"], class: advisorBlockClassDedicated,
				},
				{
					key: "share", quantity: all.Size() - target.Size() - 1, eligible: all,
					preferred: desired["share"], class: advisorBlockClassShared,
				},
			},
			want: "outside eligibility",
		},
		{
			name: "donor overlap",
			demands: []partitionDemand{
				{
					key: "fake", quantity: current.Size(), eligible: all,
					preferred: current, class: advisorBlockClassMandatoryReclaim,
				},
				{
					key: "donor", quantity: 1, eligible: all,
					preferred: machine.NewCPUSet(0), class: advisorBlockClassDedicated,
				},
				{
					key: "share", quantity: all.Size() - target.Size() - 1, eligible: all,
					preferred: desired["share"], class: advisorBlockClassShared,
				},
			},
			want: "overlaps",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, projectErr := projectSteadyFakeNUMAStage(
				tc.demands, []string{"fake"}, current, desired, nil, topology)

			require.Nil(t, got)
			require.ErrorContains(t, projectErr, "invalid committed reclaim requires atomic repair")
			require.ErrorContains(t, projectErr, tc.want)
		})
	}
}

func TestProjectSteadyFakeNUMAStageRejectsCommittedNUMAFloorViolationBeyondAtomicBudget(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	current := coresInNUMA(topology, 0, 0, 4)
	target0 := coresInNUMA(topology, 0, 4, 6)
	target1 := coresInNUMA(topology, 0, 6, 8)
	target := target0.Union(target1)
	fragmentedFloor := machine.NewCPUSet()
	fragmentedPeer := machine.NewCPUSet()
	for _, coreID := range topology.CPUDetails.CoresInNUMANodes(0).ToSliceInt()[:4] {
		threads := topology.CPUDetails.CPUsInCores(coreID).ToSliceInt()
		fragmentedFloor.Add(threads[0])
		fragmentedPeer.Add(threads[1])
	}
	demands := []partitionDemand{
		{
			key: "fake-0", quantity: target0.Size(),
			eligible:  all,
			preferred: fragmentedFloor, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "fake-1", quantity: target1.Size(),
			eligible:  all,
			preferred: fragmentedPeer, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "share", quantity: all.Size() - target.Size(), eligible: all,
			preferred: all.Difference(current), class: advisorBlockClassShared,
		},
	}
	desired := map[string]machine.CPUSet{
		"fake-0": target0, "fake-1": target1, "share": all.Difference(target),
	}

	got, projectErr := projectSteadyFakeNUMAStage(
		demands, []string{"fake-0", "fake-1"}, current, desired,
		[]partitionCoreFloorConstraint{{demandKey: "fake-0"}}, topology)

	require.Nil(t, got)
	require.ErrorContains(t, projectErr, "invalid committed reclaim requires atomic repair")
	require.ErrorContains(t, projectErr, "core floor")
}

func TestProjectSteadyFakeNUMAStageAllowsBudgetedAtomicRepairOfInvalidCommittedSnapshot(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	current := coresInNUMA(topology, 0, 0, 1)
	target := coresInNUMA(topology, 0, 1, 2)
	desired := map[string]machine.CPUSet{
		"fake": target, "share": all.Difference(target),
	}
	demands := stagedMigrationDemands(all, current, target.Size())
	demands[0].eligible = all.Difference(machine.NewCPUSet(current.ToSliceInt()[0]))

	got, projectErr := projectSteadyFakeNUMAStage(
		demands, []string{"fake"}, current, desired, nil, topology)

	require.NoError(t, projectErr)
	require.Equal(t, desired, got)
}

func TestProjectSteadyFakeNUMAStageAtomicRepairUsesReplacementChurnForPureShrink(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	committed := coresInNUMA(topology, 0, 0, 6)
	target := coresInNUMA(topology, 0, 0, 1)
	desired := map[string]machine.CPUSet{
		"fake": target, "share": all.Difference(target),
	}
	demands := stagedMigrationDemands(all, committed, target.Size())
	demands[0].eligible = all.Difference(
		machine.NewCPUSet(committed.Difference(target).ToSliceInt()[0]))

	got, projectErr := projectSteadyFakeNUMAStage(
		demands, []string{"fake"}, committed, desired, nil, topology)

	require.NoError(t, projectErr)
	require.Equal(t, desired, got)
	require.Zero(t, steadyFakeNUMAMigrationChurn(committed, target),
		"pure shrink has no replacement churn even when its symmetric difference exceeds the budget")
}

func stagedMigrationDemands(
	all, committed machine.CPUSet,
	fakeQuantity int,
) []partitionDemand {
	return []partitionDemand{
		{
			key: "fake", quantity: fakeQuantity, eligible: all,
			preferred: committed, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "share", quantity: all.Size() - fakeQuantity, eligible: all,
			preferred: all.Difference(committed), class: advisorBlockClassShared,
		},
	}
}

func testSteadyFakeNUMATopology(coreCount, cpusPerCore int) *machine.CPUTopology {
	details := make(machine.CPUDetails, coreCount*cpusPerCore)
	for coreID := 0; coreID < coreCount; coreID++ {
		for thread := 0; thread < cpusPerCore; thread++ {
			cpuID := coreID*cpusPerCore + thread
			details[cpuID] = machine.CPUTopoInfo{
				NUMANodeID: 0,
				SocketID:   0,
				CoreID:     coreID,
			}
		}
	}
	return &machine.CPUTopology{
		NumCPUs:      coreCount * cpusPerCore,
		NumCores:     coreCount,
		NumSockets:   1,
		NumNUMANodes: 1,
		CPUDetails:   details,
	}
}

func TestSolveSteadyFakeNUMAWholeCoreRepairsFragmentedPreferred(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	complete := coresInNUMA(topology, 0, 0, 3)
	partialA := coresInNUMA(topology, 0, 3, 4).ToSliceInt()
	partialB := coresInNUMA(topology, 0, 4, 5).ToSliceInt()
	oldFake := complete.Union(machine.NewCPUSet(partialA[0], partialB[0]))

	demands := []partitionDemand{
		{
			key:       "fake",
			quantity:  oldFake.Size(),
			eligible:  all,
			preferred: oldFake,
			class:     advisorBlockClassMandatoryReclaim,
		},
		{
			key:       "share",
			quantity:  all.Size() - oldFake.Size(),
			eligible:  all,
			preferred: all.Difference(oldFake),
			class:     advisorBlockClassShared,
		},
	}

	got, err := solveSteadyFakeNUMAWholeCore(demands, []string{"fake"}, topology)
	require.NoError(t, err)
	require.Equal(t, oldFake.Size(), got["fake"].Size())
	require.NoError(t, assertCoreAligned(got["fake"], topology))
	require.LessOrEqual(t, oldFake.Difference(got["fake"]).Size(), steadyFakeNUMAMaxMigratedCPUs)
	require.True(t, got["fake"].Intersection(got["share"]).IsEmpty())
	require.Equal(t, all, got["fake"].Union(got["share"]))
}

func TestSolveSteadyFakeNUMAWholeCoreKeepsAlignedBaseline(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	oldFake := coresInNUMA(topology, 0, 2, 6)

	got, err := solveSteadyFakeNUMAWholeCore([]partitionDemand{
		{
			key:       "fake",
			quantity:  oldFake.Size(),
			eligible:  all,
			preferred: oldFake,
			class:     advisorBlockClassMandatoryReclaim,
		},
		{
			key:       "share",
			quantity:  all.Size() - oldFake.Size(),
			eligible:  all,
			preferred: all.Difference(oldFake),
			class:     advisorBlockClassShared,
		},
	}, []string{"fake"}, topology)

	require.NoError(t, err)
	require.Equal(t, oldFake, got["fake"])
}

func TestSolveSteadyFakeNUMAWholeCorePreservesNarrowDonor(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(12, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	narrow := coresInNUMA(topology, 0, 0, 1)
	oldFake := coresInNUMA(topology, 0, 1, 3)

	got, err := solveSteadyFakeNUMAWholeCore([]partitionDemand{
		{
			key:       "narrow",
			quantity:  narrow.Size(),
			eligible:  narrow,
			preferred: narrow,
			class:     advisorBlockClassDedicated,
		},
		{
			key:       "fake",
			quantity:  oldFake.Size(),
			eligible:  all,
			preferred: oldFake,
			class:     advisorBlockClassMandatoryReclaim,
		},
		{
			key:      "share",
			quantity: all.Size() - narrow.Size() - oldFake.Size(),
			eligible: all,
			class:    advisorBlockClassShared,
		},
	}, []string{"fake"}, topology)

	require.NoError(t, err)
	require.Equal(t, narrow, got["narrow"])
	require.True(t, got["fake"].Intersection(narrow).IsEmpty())
	require.NoError(t, assertCoreAligned(got["fake"], topology))
}

func TestSolveSteadyFakeNUMAWholeCoreRejectsIllegalBaseline(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()

	got, err := solveSteadyFakeNUMAWholeCore([]partitionDemand{
		{
			key:      "fake",
			quantity: 6,
			eligible: all,
			class:    advisorBlockClassMandatoryReclaim,
		},
		{
			key:      "share",
			quantity: 4,
			eligible: all,
			class:    advisorBlockClassShared,
		},
	}, []string{"fake"}, topology)

	require.Nil(t, got)
	require.ErrorContains(t, err, "steady fake-NUMA baseline")
}

func TestSolveSteadyFakeNUMAWholeCoreRejectsOddQuantity(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(12, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()

	got, err := solveSteadyFakeNUMAWholeCore([]partitionDemand{
		{
			key:      "fake",
			quantity: 5,
			eligible: all,
			class:    advisorBlockClassMandatoryReclaim,
		},
		{
			key:      "share",
			quantity: all.Size() - 5,
			eligible: all,
			class:    advisorBlockClassShared,
		},
	}, []string{"fake"}, topology)

	require.Nil(t, got)
	require.ErrorContains(t, err, "not a whole-core multiple")
}

func TestSolveSteadyFakeNUMAWholeCoreRejectsIncompleteRepairBeyondEightChangedCPUs(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	oldFake := machine.NewCPUSet()
	for _, coreID := range topology.CPUDetails.Cores().ToSliceInt()[:20] {
		oldFake.Add(topology.CPUDetails.CPUsInCores(coreID).ToSliceInt()[0])
	}

	got, err := solveSteadyFakeNUMAWholeCore([]partitionDemand{
		{
			key:       "fake",
			quantity:  oldFake.Size(),
			eligible:  all,
			preferred: oldFake,
			class:     advisorBlockClassMandatoryReclaim,
		},
		{
			key:       "share",
			quantity:  all.Size() - oldFake.Size(),
			eligible:  all,
			preferred: all.Difference(oldFake),
			class:     advisorBlockClassShared,
		},
	}, []string{"fake"}, topology)

	require.Nil(t, got)
	require.ErrorContains(t, err, "invalid committed reclaim requires atomic repair")
}

func TestSolveSteadyFakeNUMAWholeCoreAllowsAlignedExpansionBeyondMigrationBudget(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	fakeEligible := coresInNUMA(topology, 0, 0, 5)

	got, err := solveSteadyFakeNUMAWholeCore([]partitionDemand{
		{
			key: "fake", quantity: 10, eligible: fakeEligible,
			preferred: machine.NewCPUSet(), class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "share", quantity: all.Size() - 10, eligible: all.Difference(fakeEligible),
			preferred: all.Difference(fakeEligible), class: advisorBlockClassShared,
		},
	}, []string{"fake"}, topology)

	require.NoError(t, err)
	require.Equal(t, fakeEligible, got["fake"])
}

func TestSolveSteadyFakeNUMAWholeCoreUsesDonorUnion(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	cores := topology.CPUDetails.Cores().ToSliceInt()
	oldFake := machine.NewCPUSet()
	for _, coreID := range cores[:4] {
		oldFake.Add(topology.CPUDetails.CPUsInCores(coreID).ToSliceInt()[0])
	}
	donorA := all.Difference(oldFake).Intersection(coresInNUMA(topology, 0, 0, 4))
	donorB := all.Difference(oldFake).Difference(donorA)

	got, err := solveSteadyFakeNUMAWholeCore([]partitionDemand{
		{
			key:       "fake",
			quantity:  oldFake.Size(),
			eligible:  all,
			preferred: oldFake,
			class:     advisorBlockClassMandatoryReclaim,
		},
		{
			key:       "donor-a",
			quantity:  donorA.Size(),
			eligible:  all,
			preferred: donorA,
			class:     advisorBlockClassShared,
		},
		{
			key:       "donor-b",
			quantity:  donorB.Size(),
			eligible:  all,
			preferred: donorB,
			class:     advisorBlockClassDedicated,
		},
	}, []string{"fake"}, topology)

	require.NoError(t, err)
	require.NoError(t, assertCoreAligned(got["fake"], topology))
	require.Equal(t, donorA.Size(), got["donor-a"].Size())
	require.Equal(t, donorB.Size(), got["donor-b"].Size())
	require.Equal(t, all, got["fake"].Union(got["donor-a"]).Union(got["donor-b"]))
}

func TestSolveSteadyFakeNUMAWholeCoreIsDeterministicAcrossDemandOrder(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	oldFake := machine.NewCPUSet(0, 1, 2, 3)
	base := []partitionDemand{
		{key: "fake", quantity: 4, eligible: all, preferred: oldFake, class: advisorBlockClassMandatoryReclaim},
		{key: "share-a", quantity: 4, eligible: all, class: advisorBlockClassShared},
		{key: "share-b", quantity: 8, eligible: all, class: advisorBlockClassShared},
	}
	var want map[string]machine.CPUSet
	for seed := int64(0); seed < 20; seed++ {
		demands := append([]partitionDemand(nil), base...)
		rand.New(rand.NewSource(seed)).Shuffle(len(demands), func(i, j int) {
			demands[i], demands[j] = demands[j], demands[i]
		})
		got, solveErr := solveSteadyFakeNUMAWholeCore(
			demands, []string{"fake"}, topology)
		require.NoError(t, solveErr)
		if seed == 0 {
			want = got
		} else {
			require.Equal(t, want, got)
		}
	}
}

func TestPlanSteadyFakeNUMACoreCapacityQuotasPreservesOldCounts(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 2)
	require.NoError(t, err)
	eligible := topology.CPUDetails.CPUs()
	old := coresInNUMA(topology, 0, 0, 2).
		Union(coresInNUMA(topology, 1, 0, 3))

	quotas, err := planSteadyFakeNUMACoreCapacityQuotas(
		old.Size(), old, eligible, topology, nil, nil)

	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 4, 1: 6}, quotas)
}

func TestPlanSteadyFakeNUMACoreCapacityQuotasUsesCoreCapacityBeforeOddOldCounts(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 2)
	require.NoError(t, err)
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0).ToSliceInt()
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1).ToSliceInt()
	old := machine.NewCPUSet(numa0[0], numa0[1], numa0[2], numa1[0], numa1[1], numa1[2])

	quotas, err := planSteadyFakeNUMACoreCapacityQuotas(
		old.Size(), old, topology.CPUDetails.CPUs(), topology, nil, nil)

	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 2, 1: 4}, quotas)
}

func TestPlanSteadyFakeNUMACoreCapacityQuotasFailsWhenFloorsExceedQuantity(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)

	quotas, err := planSteadyFakeNUMACoreCapacityQuotas(
		2,
		machine.NewCPUSet(),
		topology.CPUDetails.CPUs(),
		topology,
		nil,
		map[int]int{0: 2, 1: 2},
	)

	require.Nil(t, quotas)
	require.ErrorContains(t, err, "smaller than required steady minimum")
}

func TestSolveAdvisorDescriptorPhaseSteadyFakeNUMAPreservesQuotaAndAlignsUnion(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	oldFake := coresInNUMA(topology, 0, 0, 2).
		Union(coresInNUMA(topology, 1, 0, 3))
	oldShare := all.Difference(oldFake)
	result := make(map[string]machine.CPUSet)

	remaining, err := p.solveAdvisorDescriptorPhase([]advisorBlockDescriptor{
		{
			BlockID:      "fake",
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     oldFake.Size(),
			ComponentKey: "fake",
			Eligible:     all,
			OldPreferred: oldFake,
		},
		{
			BlockID:      "share",
			Class:        advisorBlockClassShared,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     oldShare.Size(),
			ComponentKey: "share",
			Eligible:     all,
			OldPreferred: oldShare,
		},
	}, all, result, true, false)

	require.NoError(t, err)
	require.True(t, remaining.IsEmpty())
	require.NoError(t, assertCoreAligned(result["fake"], topology))
	require.Equal(t, 4, result["fake"].Intersection(
		topology.CPUDetails.CPUsInNUMANodes(0)).Size())
	require.Equal(t, 6, result["fake"].Intersection(
		topology.CPUDetails.CPUsInNUMANodes(1)).Size())
}

func TestSolveAdvisorDescriptorPhaseSteadyFakeNUMARepairsProductionShape(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(256, 2, 8)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	cores := topology.CPUDetails.Cores().ToSliceInt()
	require.GreaterOrEqual(t, len(cores), 106)
	oldFake := topology.CPUDetails.CPUsInCores(cores[:100]...)
	for _, coreID := range cores[100:104] {
		siblings := topology.CPUDetails.CPUsInCores(coreID).ToSliceInt()
		oldFake.Add(siblings[0])
	}
	require.Equal(t, 204, oldFake.Size())
	require.Equal(t, 4, fragmentedLogicalCPUCount(oldFake, topology))
	result := make(map[string]machine.CPUSet)

	_, err = p.solveAdvisorDescriptorPhase([]advisorBlockDescriptor{
		{
			BlockID:      "fake",
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     oldFake.Size(),
			ComponentKey: "fake",
			Eligible:     all,
			OldPreferred: oldFake,
		},
		{
			BlockID:      "share",
			Class:        advisorBlockClassShared,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     all.Size() - oldFake.Size(),
			ComponentKey: "share",
			Eligible:     all,
			OldPreferred: all.Difference(oldFake),
		},
	}, all, result, true, false)

	require.NoError(t, err)
	require.Equal(t, 204, result["fake"].Size())
	require.Equal(t, 102, wholeCoreCount(result["fake"], topology))
	require.Zero(t, fragmentedLogicalCPUCount(result["fake"], topology))
	require.LessOrEqual(t,
		oldFake.Difference(result["fake"]).Union(result["fake"].Difference(oldFake)).Size(),
		steadyFakeNUMAMaxMigratedCPUs)
}

func TestSolveSteadyFakeNUMADesiredWholeCoreRepairsDC05FlowFixture(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(256, 2, 8)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	committed, err := machine.Parse("0-100,112,128-228,240")
	require.NoError(t, err)
	ordinaryDesired, err := machine.Parse(
		"1,6-8,11-28,32-44,48-60,80-81,96-97,112-113,129,134-154,160-170,176-186,192-193")
	require.NoError(t, err)
	require.Equal(t, 204, committed.Size())
	require.NoError(t, assertCoreAligned(committed, topology))
	require.Equal(t, 100, ordinaryDesired.Size())
	require.Greater(t, fragmentedLogicalCPUCount(ordinaryDesired, topology),
		steadyFakeNUMAMaxMigratedCPUs)

	demands := []partitionDemand{
		{
			key: "fake", quantity: ordinaryDesired.Size(), eligible: all,
			preferred: committed, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "share", quantity: all.Size() - ordinaryDesired.Size(), eligible: all,
			preferred: all.Difference(committed), class: advisorBlockClassShared,
		},
	}
	baseline := map[string]machine.CPUSet{
		"fake":  ordinaryDesired,
		"share": all.Difference(ordinaryDesired),
	}

	got, err := solveSteadyFakeNUMADesiredWholeCore(
		demands, []string{"fake"}, nil, topology, baseline)

	require.NoError(t, err)
	require.Equal(t, ordinaryDesired.Size(), got["fake"].Size())
	require.NoError(t, assertCoreAligned(got["fake"], topology))
	require.True(t, got["fake"].Intersection(got["share"]).IsEmpty())
	require.Equal(t, all, got["fake"].Union(got["share"]))
}

func TestSolveSteadyFakeNUMADesiredWholeCoreRepairsHighlyFragmentedPreferred(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	fragmented := machine.NewCPUSet()
	for _, coreID := range topology.CPUDetails.Cores().ToSliceInt()[:20] {
		fragmented.Add(topology.CPUDetails.CPUsInCores(coreID).ToSliceInt()[0])
	}
	demands := []partitionDemand{
		{
			key: "fake", quantity: fragmented.Size(), eligible: all,
			preferred: fragmented, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "share", quantity: all.Size() - fragmented.Size(), eligible: all,
			preferred: all.Difference(fragmented), class: advisorBlockClassShared,
		},
	}
	baseline := map[string]machine.CPUSet{
		"fake": fragmented, "share": all.Difference(fragmented),
	}

	got, err := solveSteadyFakeNUMADesiredWholeCore(
		demands, []string{"fake"}, nil, topology, baseline)

	require.NoError(t, err)
	require.Equal(t, fragmented.Size(), got["fake"].Size())
	require.NoError(t, assertCoreAligned(got["fake"], topology))
}

func TestSolveAdvisorDescriptorPhaseRepairsDC05CheckpointWithinEightChangedCPUs(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(256, 2, 8)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	oldFake, err := machine.Parse("0-104,112,128-224,240")
	require.NoError(t, err)
	oldByNUMA := make(map[int]int)
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceInt() {
		oldByNUMA[numaID] = oldFake.Intersection(
			topology.CPUDetails.CPUsInNUMANodes(numaID)).Size()
	}
	result := make(map[string]machine.CPUSet)

	_, err = p.solveAdvisorDescriptorPhase([]advisorBlockDescriptor{
		{
			BlockID:      "fake",
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     oldFake.Size(),
			ComponentKey: "fake",
			Eligible:     all,
			OldPreferred: oldFake,
		},
		{
			BlockID:      "share",
			Class:        advisorBlockClassShared,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     all.Size() - oldFake.Size(),
			ComponentKey: "share",
			Eligible:     all,
			OldPreferred: all.Difference(oldFake),
		},
	}, all, result, true, false)

	require.NoError(t, err)
	require.Equal(t, oldFake.Size(), result["fake"].Size())
	require.NoError(t, assertCoreAligned(result["fake"], topology))
	require.LessOrEqual(t,
		oldFake.Difference(result["fake"]).Union(result["fake"].Difference(oldFake)).Size(),
		steadyFakeNUMAMaxMigratedCPUs)
	for numaID, oldQuantity := range oldByNUMA {
		require.Equal(t, oldQuantity, result["fake"].Intersection(
			topology.CPUDetails.CPUsInNUMANodes(numaID)).Size(), "NUMA %d", numaID)
	}
}

func TestSolveSteadyFakeNUMAWholeCoreSupportsSMT1AndSMT4(t *testing.T) {
	t.Parallel()

	smt1, err := machine.GenerateDummyCPUTopologyWithoutSMT(4, 1, 1)
	require.NoError(t, err)
	smt4 := &machine.CPUTopology{
		NumCPUs: 8, NumCores: 2, NumSockets: 1, NumNUMANodes: 1,
		CPUDetails: machine.CPUDetails{
			0: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			1: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			2: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			3: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			4: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			5: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			6: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			7: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
		},
	}

	for _, tc := range []struct {
		name     string
		topology *machine.CPUTopology
		old      machine.CPUSet
	}{
		{name: "SMT1", topology: smt1, old: machine.NewCPUSet(0, 2, 3)},
		{name: "SMT4 fragmented repair", topology: smt4, old: machine.NewCPUSet(0, 1, 4, 5)},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			all := tc.topology.CPUDetails.CPUs()
			got, solveErr := solveSteadyFakeNUMAWholeCore([]partitionDemand{
				{
					key: "fake", quantity: tc.old.Size(), eligible: all,
					preferred: tc.old, class: advisorBlockClassMandatoryReclaim,
				},
				{
					key: "share", quantity: all.Size() - tc.old.Size(), eligible: all,
					preferred: all.Difference(tc.old), class: advisorBlockClassShared,
				},
			}, []string{"fake"}, tc.topology)
			require.NoError(t, solveErr)
			require.NoError(t, assertCoreAligned(got["fake"], tc.topology))
			require.Equal(t, tc.old.Size(), got["fake"].Size())
		})
	}
}

func TestPlanSteadyFakeNUMACoreCapacityQuotasRoutesAroundFragmentedNUMA(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numa0Cores := topology.CPUDetails.CoresInNUMANodes(0).ToSliceInt()
	fragmentedNUMA0 := machine.NewCPUSet(
		topology.CPUDetails.CPUsInCores(numa0Cores[0]).ToSliceInt()[0],
		topology.CPUDetails.CPUsInCores(numa0Cores[1]).ToSliceInt()[0],
	)
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1)
	eligible := fragmentedNUMA0.Union(numa1)

	quotas, err := planSteadyFakeNUMACoreCapacityQuotas(
		4, fragmentedNUMA0, eligible, topology, nil, map[int]int{1: 2})

	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 0, 1: 4}, quotas)
}

func TestPlanSteadyFakeNUMACoreCapacityQuotasRejectsFloorWithoutCompleteCore(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numa0Cores := topology.CPUDetails.CoresInNUMANodes(0).ToSliceInt()
	fragmentedNUMA0 := machine.NewCPUSet(
		topology.CPUDetails.CPUsInCores(numa0Cores[0]).ToSliceInt()[0],
		topology.CPUDetails.CPUsInCores(numa0Cores[1]).ToSliceInt()[0],
	)
	eligible := fragmentedNUMA0.Union(coresInNUMA(topology, 1, 0, 1))

	quotas, err := planSteadyFakeNUMACoreCapacityQuotas(
		4, machine.NewCPUSet(), eligible, topology, nil, map[int]int{0: 2, 1: 2})

	require.Nil(t, quotas)
	require.ErrorContains(t, err, "maximum 0 is smaller than minimum 2")
}

func TestPlanSteadyFakeNUMACoreCapacityQuotasAllowsCapacitySaturatedImbalance(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 2)
	require.NoError(t, err)
	eligible := coresInNUMA(topology, 0, 0, 1).
		Union(coresInNUMA(topology, 1, 0, 3))

	quotas, err := planSteadyFakeNUMACoreCapacityQuotas(
		8, eligible, eligible, topology, nil, map[int]int{0: 2, 1: 2})

	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 2, 1: 6}, quotas)
}

func TestSolveSteadyFakeNUMAWholeCoreIsStableAcrossBlockIDChurn(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	old := coresInNUMA(topology, 0, 2, 6)
	solve := func(fakeKey string) machine.CPUSet {
		got, solveErr := solveSteadyFakeNUMAWholeCore([]partitionDemand{
			{
				key: fakeKey, quantity: old.Size(), eligible: all,
				preferred: old, class: advisorBlockClassMandatoryReclaim,
			},
			{
				key: "share", quantity: all.Size() - old.Size(), eligible: all,
				preferred: all.Difference(old), class: advisorBlockClassShared,
			},
		}, []string{fakeKey}, topology)
		require.NoError(t, solveErr)
		return got[fakeKey]
	}

	require.Equal(t, solve("fake-old-id"), solve("fake-new-id"))
}

func TestSolveSteadyFakeNUMAWholeCoreAcceptsZeroQuantity(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	got, err := solveSteadyFakeNUMAWholeCore([]partitionDemand{
		{key: "fake", quantity: 0, eligible: all, class: advisorBlockClassMandatoryReclaim},
		{key: "share", quantity: all.Size(), eligible: all, class: advisorBlockClassShared},
	}, []string{"fake"}, topology)

	require.NoError(t, err)
	require.True(t, got["fake"].IsEmpty())
	require.Equal(t, all, got["share"])
}

func TestSolveSteadyFakeNUMAWholeCoreRepairsAfterSiblingBecomesIneligible(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	old := coresInNUMA(topology, 0, 0, 1)
	oldThreads := old.ToSliceInt()
	available := all.Difference(machine.NewCPUSet(oldThreads[1]))
	demands := []partitionDemand{
		{
			key: "fake", quantity: 2, eligible: available,
			preferred: old, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "share", quantity: available.Size() - 2, eligible: available,
			preferred: available.Difference(old), class: advisorBlockClassShared,
		},
	}
	got, err := solveSteadyFakeNUMAWholeCore(demands, []string{"fake"}, topology)

	require.NoError(t, err)
	require.NoError(t, assertCoreAligned(got["fake"], topology))
	require.True(t, got["fake"].Intersection(machine.NewCPUSet(oldThreads[1])).IsEmpty())
	repeated, err := solveSteadyFakeNUMAWholeCore(demands, []string{"fake"}, topology)
	require.NoError(t, err)
	require.Equal(t, got["fake"], repeated["fake"])
}
