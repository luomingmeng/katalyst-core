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

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

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
		initial,
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
		committed,
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
		second["fake"],
		map[string]machine.CPUSet{"fake": targetB, "share": all.Difference(targetB)},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, targetB, restarted.steadyFakeNUMAMigrationTarget.target,
		"constraint changes must atomically replace the durable target")
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
		target,
		map[string]machine.CPUSet{"fake": target, "share": all.Difference(target)},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, target, got["fake"])
	require.Nil(t, policy.steadyFakeNUMAMigrationTarget)
	require.NoFileExists(t, filepath.Join(dir, steadyFakeNUMAMigrationCheckpointName))
}

func TestSteadyFakeNUMAMigrationTargetIsNotStoredWhenProjectionFails(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(24, 1, 1)
	require.NoError(t, err)
	all := topology.CPUDetails.CPUs()
	committed := coresInNUMA(topology, 0, 0, 8)
	target := coresInNUMA(topology, 0, 8, 16)
	dir := t.TempDir()
	policy, err := getTestDynamicPolicyWithoutInitialization(topology, dir)
	require.NoError(t, err)
	demands := stagedMigrationDemands(all, committed, target.Size())
	demands[0].preferred = committed.Difference(machine.NewCPUSet(committed.ToSliceInt()[0]))

	_, err = policy.projectSteadyFakeNUMAStageWithCheckpoint(
		demands,
		[]string{"fake"},
		committed,
		map[string]machine.CPUSet{"fake": target, "share": all.Difference(target)},
		nil,
	)

	require.ErrorContains(t, err, "invalid committed reclaim requires atomic repair")
	require.Nil(t, policy.steadyFakeNUMAMigrationTarget)
	require.NoFileExists(t, filepath.Join(dir, steadyFakeNUMAMigrationCheckpointName))
}
