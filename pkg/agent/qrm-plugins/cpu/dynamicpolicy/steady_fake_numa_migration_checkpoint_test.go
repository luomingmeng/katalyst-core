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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestSteadyFakeNUMAMigrationBudgetIsEightCPUIds(t *testing.T) {
	t.Parallel()

	require.Equal(t, 8, steadyFakeNUMAMaxMigratedCPUs)
}

func TestProjectSteadyFakeNUMAStageWithCheckpointKeepsFixedReplacementChurnBudget(t *testing.T) {
	t.Run("large topology does not increase budget", func(t *testing.T) {
		topology, err := machine.GenerateDummyCPUTopology(256, 2, 8)
		require.NoError(t, err)
		all := topology.CPUDetails.CPUs()
		committed := coresInNUMA(topology, 0, 0, 6)
		target := coresInNUMA(topology, 0, 6, 12)
		policy, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
		require.NoError(t, err)

		next, err := policy.projectSteadyFakeNUMAStageWithCheckpoint(
			stagedMigrationDemands(all, committed, target.Size()),
			[]string{"fake"},
			newSteadyFakeNUMACommittedSnapshotForTest(committed, all),
			map[string]machine.CPUSet{
				"fake":  target,
				"share": all.Difference(target),
			},
			nil,
		)

		require.NoError(t, err)
		require.LessOrEqual(t,
			steadyFakeNUMAMigrationChurn(committed, next["fake"]),
			steadyFakeNUMAMaxMigratedCPUs)
		require.NotNil(t, policy.steadyFakeNUMAMigrationTarget)
		require.Equal(t, target, policy.steadyFakeNUMAMigrationTarget.target)
	})

	t.Run("offsetting demand shrink and grow do not increase budget", func(t *testing.T) {
		topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(20, 1, 1)
		require.NoError(t, err)
		all := topology.CPUDetails.CPUs()
		oldShrink := coresInNUMA(topology, 0, 0, 6)
		oldGrow := machine.NewCPUSet()
		committed := oldShrink.Union(oldGrow)
		targetShrink := coresInNUMA(topology, 0, 6, 7)
		targetGrow := coresInNUMA(topology, 0, 7, 12)
		target := targetShrink.Union(targetGrow)
		policy, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
		require.NoError(t, err)
		demands := []partitionDemand{
			{
				key: "shrink", quantity: targetShrink.Size(), eligible: all,
				preferred: oldShrink, class: advisorBlockClassMandatoryReclaim,
			},
			{
				key: "grow", quantity: targetGrow.Size(), eligible: all,
				preferred: oldGrow, class: advisorBlockClassMandatoryReclaim,
			},
			{
				key: "share", quantity: all.Size() - target.Size(), eligible: all,
				preferred: all.Difference(committed), class: advisorBlockClassShared,
			},
		}

		next, err := policy.projectSteadyFakeNUMAStageWithCheckpoint(
			demands,
			[]string{"shrink", "grow"},
			newSteadyFakeNUMACommittedSnapshotForTest(committed, all),
			map[string]machine.CPUSet{
				"shrink": targetShrink,
				"grow":   targetGrow,
				"share":  all.Difference(target),
			},
			nil,
		)

		require.NoError(t, err)
		nextReclaim := next["shrink"].Union(next["grow"])
		require.LessOrEqual(t,
			steadyFakeNUMAMigrationChurn(committed, nextReclaim),
			steadyFakeNUMAMaxMigratedCPUs)
		require.NotNil(t, policy.steadyFakeNUMAMigrationTarget)
		require.Equal(t, target, policy.steadyFakeNUMAMigrationTarget.target)
	})
}

func TestSteadyFakeNUMAMigrationTargetSurvivesRestart(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	dir := t.TempDir()
	first, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	want := &steadyFakeNUMAMigrationTarget{
		constraintDigest: "stable-constraints",
		target:           machine.NewCPUSet(0, 1, 4, 5),
	}
	require.NoError(t, first.storeSteadyFakeNUMAMigrationTarget(want))

	restarted, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)

	require.Equal(t, want.constraintDigest, restarted.steadyFakeNUMAMigrationTarget.constraintDigest)
	require.Equal(t, want.target, restarted.steadyFakeNUMAMigrationTarget.target)
}

func TestSteadyFakeNUMAMigrationTargetCorruptionFailsInitialization(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, steadyFakeNUMAMigrationCheckpointName),
		[]byte("{broken"),
		0o600,
	))

	policy, err := getTestDynamicPolicyWithoutInitialization(topology, dir)

	require.Nil(t, policy)
	require.ErrorContains(t, err, "restore steady fake-NUMA migration target")
	require.FileExists(t, filepath.Join(dir, steadyFakeNUMAMigrationCheckpointName))
}

func TestSteadyFakeNUMAConstraintDigestChangesOnlyWithConstraints(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	demands := []partitionDemand{{
		key: "fake", quantity: 4,
		eligible:  machine.NewCPUSet(0, 1, 2, 3, 4, 5),
		preferred: machine.NewCPUSet(0, 1, 2, 3),
		class:     advisorBlockClassMandatoryReclaim,
	}}

	first, err := steadyFakeNUMAConstraintDigest(demands, []string{"fake"}, nil, topology)
	require.NoError(t, err)
	demands[0].preferred = machine.NewCPUSet(2, 3, 4, 5)
	same, err := steadyFakeNUMAConstraintDigest(demands, []string{"fake"}, nil, topology)
	require.NoError(t, err)
	require.Equal(t, first, same, "committed placement is progress, not a target constraint")

	demands[0].eligible = machine.NewCPUSet(2, 3, 4, 5, 6, 7)
	changed, err := steadyFakeNUMAConstraintDigest(demands, []string{"fake"}, nil, topology)
	require.NoError(t, err)
	require.NotEqual(t, first, changed)
}

func TestSteadyFakeNUMAMigrationTargetContinuesAfterRestartAndIsReplacedOnConstraintChange(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	initial := coresInNUMA(topology, 0, 0, 6)
	targetA := coresInNUMA(topology, 0, 6, 12)
	targetB := coresInNUMA(topology, 0, 3, 9)
	dir := t.TempDir()
	policy, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)

	demands := stagedMigrationDemands(all, initial, targetA.Size())
	first, err := policy.projectSteadyFakeNUMAStageWithCheckpoint(
		demands,
		[]string{"fake"},
		newSteadyFakeNUMACommittedSnapshotForTest(initial, all),
		map[string]machine.CPUSet{"fake": targetA, "share": all.Difference(targetA)},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, targetA, policy.steadyFakeNUMAMigrationTarget.target)
	require.FileExists(t, filepath.Join(dir, steadyFakeNUMAMigrationCheckpointName))

	restarted, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	committed := first["fake"]
	demands = stagedMigrationDemands(all, committed, targetA.Size())
	second, err := restarted.projectSteadyFakeNUMAStageWithCheckpoint(
		demands,
		[]string{"fake"},
		newSteadyFakeNUMACommittedSnapshotForTest(committed, all),
		map[string]machine.CPUSet{"fake": targetB, "share": all.Difference(targetB)},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, targetA, restarted.steadyFakeNUMAMigrationTarget.target,
		"same constraints must continue toward the durable target, not a current-state-biased recomputation")
	require.Less(t,
		steadyFakeNUMAMigrationChurn(second["fake"], targetA),
		steadyFakeNUMAMigrationChurn(committed, targetA))

	changedDemands := stagedMigrationDemands(all, second["fake"], targetB.Size())
	changedDemands[0].requestGroupKey = "changed-constraint"
	_, err = restarted.projectSteadyFakeNUMAStageWithCheckpoint(
		changedDemands,
		[]string{"fake"},
		newSteadyFakeNUMACommittedSnapshotForTest(second["fake"], all),
		map[string]machine.CPUSet{"fake": targetB, "share": all.Difference(targetB)},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, targetB, restarted.steadyFakeNUMAMigrationTarget.target,
		"constraint changes must atomically replace the durable target")
}

func TestPlanSteadyFakeNUMAStageWithCheckpointIgnoresStaleTargetOwnedByFixedDemand(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	committedFake := coresInNUMA(topology, 0, 0, 6)
	fixed := coresInNUMA(topology, 0, 6, 12)
	freshTarget := coresInNUMA(topology, 0, 12, 18)
	share := all.Difference(committedFake).Difference(fixed)
	policy, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, policy.storeSteadyFakeNUMAMigrationTarget(
		&steadyFakeNUMAMigrationTarget{
			constraintDigest: "stale-constraints",
			target:           fixed,
		}))

	demands := []partitionDemand{
		{
			key: "fake", quantity: freshTarget.Size(), eligible: all,
			preferred: committedFake, class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "fixed", quantity: fixed.Size(), eligible: fixed,
			preferred: fixed, class: advisorBlockClassDedicated,
		},
		{
			key: "share", quantity: share.Size(), eligible: all,
			preferred: share, class: advisorBlockClassShared,
		},
	}
	committed := steadyFakeNUMACommittedSnapshot{
		assignments: []steadyFakeNUMACommittedAssignment{
			{
				blockID: "fake", class: advisorBlockClassMandatoryReclaim,
				numaID: commonstate.FakedNUMAID, cpus: committedFake, eligible: all,
			},
			{
				blockID: "fixed", class: advisorBlockClassDedicated,
				numaID: 0, cpus: fixed, eligible: fixed,
			},
			{
				blockID: "share", class: advisorBlockClassShared,
				numaID: commonstate.FakedNUMAID, cpus: share, eligible: all,
			},
		},
		rawReclaimAggregate: committedFake,
		reclaim:             committedFake,
	}
	freshDesired := map[string]machine.CPUSet{
		"fake":  freshTarget,
		"fixed": fixed,
		"share": all.Difference(freshTarget).Difference(fixed),
	}

	assignments, transition, err := policy.planSteadyFakeNUMAStageWithCheckpoint(
		demands, []string{"fake"}, committed, freshDesired, nil)

	require.NoError(t, err,
		"a stale target must not be validated against ownership under new constraints")
	require.Equal(t, fixed, assignments["fixed"])
	require.True(t, assignments["fake"].Intersection(fixed).IsEmpty())
	require.Equal(t, steadyFakeNUMAMigrationCheckpointReplace, transition.kind)
	require.NotNil(t, transition.target)
	require.Equal(t, freshTarget, transition.target.target)
	require.NotEqual(t, "stale-constraints", transition.target.constraintDigest)
}

func TestSteadyFakeNUMAMigrationTargetIsRemovedAtConvergence(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	target := coresInNUMA(topology, 0, 6, 12)
	dir := t.TempDir()
	policy, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	demands := stagedMigrationDemands(all, target, target.Size())
	digest, err := steadyFakeNUMAConstraintDigest(demands, []string{"fake"}, nil, topology)
	require.NoError(t, err)
	require.NoError(t, policy.storeSteadyFakeNUMAMigrationTarget(&steadyFakeNUMAMigrationTarget{
		constraintDigest: digest,
		target:           target,
	}))

	got, err := policy.projectSteadyFakeNUMAStageWithCheckpoint(
		demands,
		[]string{"fake"},
		newSteadyFakeNUMACommittedSnapshotForTest(target, all),
		map[string]machine.CPUSet{"fake": target, "share": all.Difference(target)},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, target, got["fake"])
	require.Nil(t, policy.steadyFakeNUMAMigrationTarget)
	require.NoFileExists(t, filepath.Join(dir, steadyFakeNUMAMigrationCheckpointName))
}

func TestSteadyFakeNUMAMigrationTargetIsNotStoredForAtomicCommittedRepair(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	committed := coresInNUMA(topology, 0, 0, 8)
	target := coresInNUMA(topology, 0, 8, 16)
	dir := t.TempDir()
	policy, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	demands := stagedMigrationDemands(all, committed, target.Size())
	snapshot := newSteadyFakeNUMACommittedSnapshotForTest(
		committed, all.Difference(machine.NewCPUSet(committed.ToSliceInt()[0])))

	got, err := policy.projectSteadyFakeNUMAStageWithCheckpoint(
		demands,
		[]string{"fake"},
		snapshot,
		map[string]machine.CPUSet{"fake": target, "share": all.Difference(target)},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, committed, got["fake"])
	require.Equal(t, all.Difference(committed), got["share"])
	require.Nil(t, policy.steadyFakeNUMAMigrationTarget)
	require.NoFileExists(t, filepath.Join(dir, steadyFakeNUMAMigrationCheckpointName))
}

func TestPlanSteadyFakeNUMAStageWithCheckpointReturnsTypedReplaceWithoutSideEffects(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	committed := coresInNUMA(topology, 0, 0, 6)
	oldTarget := coresInNUMA(topology, 0, 6, 12)
	freshTarget := coresInNUMA(topology, 0, 12, 18)
	policy, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, policy.storeSteadyFakeNUMAMigrationTarget(
		&steadyFakeNUMAMigrationTarget{constraintDigest: "old", target: oldTarget}))
	beforeTarget := cloneSteadyFakeNUMAMigrationTargetForTest(
		policy.steadyFakeNUMAMigrationTarget)
	beforeBytes, err := os.ReadFile(policy.steadyFakeNUMAMigrationCheckpointPath())
	require.NoError(t, err)

	demands := stagedMigrationDemands(all, committed, freshTarget.Size())
	demands[0].requestGroupKey = "new-constraints"
	assignments, transition, err := policy.planSteadyFakeNUMAStageWithCheckpoint(
		demands,
		[]string{"fake"},
		newSteadyFakeNUMACommittedSnapshotForTest(committed, all),
		map[string]machine.CPUSet{
			"fake":  freshTarget,
			"share": all.Difference(freshTarget),
		},
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, assignments)
	require.Equal(t, steadyFakeNUMAMigrationCheckpointReplace, transition.kind)
	require.NotNil(t, transition.target)
	require.Equal(t, freshTarget, transition.target.target)
	require.Equal(t, beforeTarget, policy.steadyFakeNUMAMigrationTarget)
	afterBytes, err := os.ReadFile(policy.steadyFakeNUMAMigrationCheckpointPath())
	require.NoError(t, err)
	require.Equal(t, beforeBytes, afterBytes)
}

func TestCommitPendingAdvisorStateAppliesCheckpointTransitionOnlyAfterSuccessfulCAS(t *testing.T) {
	for _, tc := range []struct {
		name             string
		expectedRevision func(uint64) uint64
		entries          func(*machine.CPUTopology) state.PodEntries
		wantError        string
	}{
		{
			name:             "precommit failure",
			expectedRevision: func(revision uint64) uint64 { return revision },
			entries: func(topology *machine.CPUTopology) state.PodEntries {
				core := coresInNUMA(topology, 0, 0, 1)
				fragmented := machine.NewCPUSet(core.ToSliceInt()[0])
				return precommitPartitionEntries(
					fragmented, coresInNUMA(topology, 0, 1, 2))
			},
			wantError: "is not core-aligned",
		},
		{
			name:             "revision CAS failure",
			expectedRevision: func(revision uint64) uint64 { return revision + 1 },
			entries: func(topology *machine.CPUTopology) state.PodEntries {
				return precommitPartitionEntries(
					coresInNUMA(topology, 0, 0, 1),
					coresInNUMA(topology, 0, 1, 2))
			},
			wantError: "revision",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			policy, cleanup := newReclaimReuseTestPolicy(t)
			defer cleanup()
			topology := policy.machineInfo.CPUTopology
			oldTarget := coresInNUMA(topology, 0, 2, 3)
			newTarget := coresInNUMA(topology, 0, 3, 4)
			require.NoError(t, policy.storeSteadyFakeNUMAMigrationTarget(
				&steadyFakeNUMAMigrationTarget{constraintDigest: "old", target: oldTarget}))
			beforeTarget := cloneSteadyFakeNUMAMigrationTargetForTest(
				policy.steadyFakeNUMAMigrationTarget)
			beforeBytes, err := os.ReadFile(policy.steadyFakeNUMAMigrationCheckpointPath())
			require.NoError(t, err)
			revision := policy.state.GetRevision()

			err = policy.commitPendingAdvisorState(&pendingAdvisorState{
				preCommitRevision: tc.expectedRevision(revision),
				entries:           tc.entries(topology),
				disableDedicated:  true,
				migrationCheckpointTransition: steadyFakeNUMAMigrationCheckpointTransition{
					kind: steadyFakeNUMAMigrationCheckpointReplace,
					target: &steadyFakeNUMAMigrationTarget{
						constraintDigest: "new",
						target:           newTarget,
					},
				},
			})

			require.ErrorContains(t, err, tc.wantError)
			require.Equal(t, revision, policy.state.GetRevision())
			require.Equal(t, beforeTarget, policy.steadyFakeNUMAMigrationTarget)
			afterBytes, readErr := os.ReadFile(policy.steadyFakeNUMAMigrationCheckpointPath())
			require.NoError(t, readErr)
			require.Equal(t, beforeBytes, afterBytes)
		})
	}
}

func TestCommitPendingAdvisorStateDoesNotApplyCheckpointTransitionAfterSuccessfulCAS(t *testing.T) {
	policy, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	topology := policy.machineInfo.CPUTopology
	oldTarget := coresInNUMA(topology, 0, 2, 3)
	newTarget := coresInNUMA(topology, 0, 3, 4)
	require.NoError(t, policy.storeSteadyFakeNUMAMigrationTarget(
		&steadyFakeNUMAMigrationTarget{constraintDigest: "old", target: oldTarget}))
	revision := policy.state.GetRevision()

	err := policy.commitPendingAdvisorState(&pendingAdvisorState{
		preCommitRevision: revision,
		entries: precommitPartitionEntries(
			coresInNUMA(topology, 0, 0, 1),
			coresInNUMA(topology, 0, 1, 2)),
		disableDedicated: true,
		migrationCheckpointTransition: steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointReplace,
			target: &steadyFakeNUMAMigrationTarget{
				constraintDigest: "new",
				target:           newTarget,
			},
		},
	})

	require.NoError(t, err)
	require.Equal(t, revision+1, policy.state.GetRevision())
	require.Equal(t, "old", policy.steadyFakeNUMAMigrationTarget.constraintDigest)
	require.Equal(t, oldTarget, policy.steadyFakeNUMAMigrationTarget.target)
	require.FileExists(t, policy.steadyFakeNUMAMigrationCheckpointPath())
}

func TestSteadyFakeNUMAMigrationTargetIgnoresPreferenceOnlyChanges(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	committed := coresInNUMA(topology, 0, 0, 6)
	target := coresInNUMA(topology, 0, 6, 12)
	policy, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	demands := stagedMigrationDemands(all, committed, target.Size())
	digest, err := steadyFakeNUMAConstraintDigest(demands, []string{"fake"}, nil, topology)
	require.NoError(t, err)
	require.NoError(t, policy.storeSteadyFakeNUMAMigrationTarget(
		&steadyFakeNUMAMigrationTarget{constraintDigest: digest, target: target}))
	beforeBytes, err := os.ReadFile(policy.steadyFakeNUMAMigrationCheckpointPath())
	require.NoError(t, err)

	demands[0].preferred = coresInNUMA(topology, 0, 12, 18)
	snapshot := steadyFakeNUMACommittedSnapshot{
		assignments: []steadyFakeNUMACommittedAssignment{
			{
				blockID: "fake", class: advisorBlockClassMandatoryReclaim,
				numaID: 0, cpus: committed, eligible: all,
			},
			{
				blockID: "share", class: advisorBlockClassShared,
				numaID: 0, cpus: all.Difference(committed), eligible: all,
			},
		},
		rawReclaimAggregate: committed,
		reclaim:             committed,
	}

	_, err = policy.projectSteadyFakeNUMAStageWithCheckpoint(
		demands,
		[]string{"fake"},
		snapshot,
		map[string]machine.CPUSet{
			"fake":  target,
			"share": all.Difference(target),
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, target, policy.steadyFakeNUMAMigrationTarget.target)
	afterBytes, err := os.ReadFile(policy.steadyFakeNUMAMigrationCheckpointPath())
	require.NoError(t, err)
	require.Equal(t, beforeBytes, afterBytes,
		"preference-only changes must not replace the durable target")
}

func TestSolveAdvisorDescriptorPhaseValidationFailureHasNoSideEffects(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(256, 2, 8)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	policy, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	target := &steadyFakeNUMAMigrationTarget{
		constraintDigest: "existing-target",
		target:           machine.NewCPUSet(68, 196),
	}
	require.NoError(t, policy.storeSteadyFakeNUMAMigrationTarget(target))
	beforeBytes, err := os.ReadFile(policy.steadyFakeNUMAMigrationCheckpointPath())
	require.NoError(t, err)
	beforeTarget := cloneSteadyFakeNUMAMigrationTargetForTest(
		policy.steadyFakeNUMAMigrationTarget)
	result := map[string]machine.CPUSet{
		"existing": machine.NewCPUSet(0, 128),
	}
	beforeResult := cloneBlockCPUSetForTest(result)
	descriptors := testSteadyFakeNUMACommittedDescriptors(topology)
	descriptors[0].Committed.Add(999)
	descriptors[1].Committed.Add(999)

	// This is a planner-only call boundary. Apply, cgroup, and Bulkhead writers
	// are invoked only after a successful planner result and are not reachable
	// from solveAdvisorDescriptorPhase itself.
	_, err = policy.solveAdvisorDescriptorPhase(
		descriptors, all, result, true, false)

	require.ErrorContains(t, err, "outside machine topology")
	require.Equal(t, beforeResult, result)
	require.Equal(t, beforeTarget, policy.steadyFakeNUMAMigrationTarget)
	afterBytes, readErr := os.ReadFile(policy.steadyFakeNUMAMigrationCheckpointPath())
	require.NoError(t, readErr)
	require.Equal(t, beforeBytes, afterBytes)
}

func cloneBlockCPUSetForTest(
	source map[string]machine.CPUSet,
) map[string]machine.CPUSet {
	cloned := make(map[string]machine.CPUSet, len(source))
	for blockID, cpus := range source {
		cloned[blockID] = cpus.Clone()
	}
	return cloned
}

func cloneSteadyFakeNUMAMigrationTargetForTest(
	source *steadyFakeNUMAMigrationTarget,
) *steadyFakeNUMAMigrationTarget {
	if source == nil {
		return nil
	}
	return &steadyFakeNUMAMigrationTarget{
		constraintDigest: source.constraintDigest,
		target:           source.target.Clone(),
	}
}
