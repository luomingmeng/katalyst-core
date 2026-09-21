/*
Copyright 2022 The Katalyst Authors.

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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type retryablePartitionCommitError interface {
	error
	Retryable() bool
}

type alternatingDynamicConfigurationSource struct {
	configs []*dynamicconfig.Configuration
	reads   int
}

func (s *alternatingDynamicConfigurationSource) GetDynamicConfiguration() *dynamicconfig.Configuration {
	index := s.reads
	s.reads++
	if index >= len(s.configs) {
		index = len(s.configs) - 1
	}
	return s.configs[index]
}

func TestCommitPendingCPUPartitionRejectsOrdinaryWriterWhileAdvisorTargetPending(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	dir := t.TempDir()
	p.advisorPostCommitCheckpointDir = dir
	initialRevision := p.state.GetRevision()
	target := p.publishAdvisorPostCommitTarget(
		&advisorapi.ListAndWatchResponse{}, initialRevision)
	initialEntries := p.state.GetPodEntries()

	_, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: initialRevision,
		entries:          initialEntries,
		allowOverlap:     p.state.GetAllowSharedCoresOverlapReclaimedCores(),
		disableDedicated: p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
		persist:          false,
		source:           "ordinary writer test",
	})

	require.Error(t, err)
	var retryable retryablePartitionCommitError
	require.ErrorAs(t, err, &retryable)
	require.True(t, retryable.Retryable())
	require.Equal(t, initialRevision, p.state.GetRevision())
	require.Equal(t, initialEntries, p.state.GetPodEntries())
	require.Same(t, target, p.currentAdvisorPostCommitTarget())
	require.FileExists(t, filepath.Join(dir, advisorPostCommitCheckpointName))
}

func TestCommitPendingCPUPartitionRunsHooksBeforeValidation(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	emitter := &recordingMetricEmitter{}
	p.emitter = emitter

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(2, 3))
	original := candidate.Clone()
	p.allocationHooks = []AllocationHook{func(_, allocation *state.AllocationInfo) error {
		if allocation.PodUid == "dedicated-pod" {
			allocation.AllocationResult = machine.NewCPUSet(1, 2)
		}
		return nil
	}}

	_, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		disableDedicated: true,
		persist:          false,
	})
	require.ErrorContains(t, err, "overlaps reclaim pool")
	require.Equal(t, original, candidate, "precommit must only mutate its cloned candidate")
	require.Nil(t, p.state.GetAllocationInfo("dedicated-pod", "main"),
		"candidate made invalid by a hook must not be committed")
	require.Empty(t, emitter.records, "failed commit must not emit pool size metrics")
}

func TestPreparePendingCPUPartitionUsesFrozenDynamicConfiguration(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	frozen := p.dynamicConfig.GetDynamicConfiguration()
	frozen.FillDefaultSharePoolWithNonReclaimCPUs = false
	replacement := dynamicconfig.NewConfiguration()
	replacement.FillDefaultSharePoolWithNonReclaimCPUs = true
	p.dynamicConfig.SetDynamicConfiguration(replacement)

	_, err := p.preparePendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries: precommitPartitionEntries(
			machine.NewCPUSet(0, 1),
			machine.NewCPUSet(2, 3),
		),
		dynamicConfig: frozen,
		persist:       false,
		source:        "frozen dynamic configuration test",
	})
	require.NoError(t, err)
}

func TestCaptureAdvisorAttemptConfigurationClonesMutableFields(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	current := p.dynamicConfig.GetDynamicConfiguration()
	current.EnableRampUpReclaimHardPartition = true
	current.InitialRampUpReclaimCPUSetRatio = 0.2
	current.SystemExclusivePool = map[string]int{"system": 2}
	shrinkRatio := 0.5
	current.SystemExclusivePoolShrinkRatio = &shrinkRatio

	frozen, err := p.captureAdvisorAttemptConfiguration()
	require.NoError(t, err)
	require.NotNil(t, frozen.dynamic)
	require.NotNil(t, frozen.floor)

	current.EnableRampUpReclaimHardPartition = false
	current.InitialRampUpReclaimCPUSetRatio = 0
	current.SystemExclusivePool["system"] = 4
	*current.SystemExclusivePoolShrinkRatio = 0.9

	require.True(t, frozen.dynamic.EnableRampUpReclaimHardPartition)
	require.Equal(t, 0.2, frozen.dynamic.InitialRampUpReclaimCPUSetRatio)
	require.Equal(t, map[string]int{"system": 2}, frozen.dynamic.SystemExclusivePool)
	require.Equal(t, 0.5, *frozen.dynamic.SystemExclusivePoolShrinkRatio)
}

func TestCaptureAdvisorAttemptConfigurationReadsOncePerAttempt(t *testing.T) {
	first := dynamicconfig.NewConfiguration()
	first.EnableReclaim = true
	first.EnableRampUpReclaimHardPartition = true
	first.InitialRampUpReclaimCPUSetRatio = 0.2
	second := dynamicconfig.NewConfiguration()
	second.EnableReclaim = false
	second.EnableRampUpReclaimHardPartition = false
	second.InitialRampUpReclaimCPUSetRatio = 0
	source := &alternatingDynamicConfigurationSource{
		configs: []*dynamicconfig.Configuration{first, second},
	}

	attemptOne, err := captureAdvisorAttemptConfigurationFrom(source)
	require.NoError(t, err)
	require.Equal(t, 1, source.reads)
	require.True(t, attemptOne.dynamic.EnableReclaim)
	require.True(t, attemptOne.dynamic.EnableRampUpReclaimHardPartition)
	require.Equal(t, 0.2, attemptOne.dynamic.InitialRampUpReclaimCPUSetRatio)
	require.Same(t, attemptOne.dynamic, attemptOne.floor)

	attemptTwo, err := captureAdvisorAttemptConfigurationFrom(source)
	require.NoError(t, err)
	require.Equal(t, 2, source.reads)
	require.False(t, attemptTwo.dynamic.EnableReclaim)
	require.False(t, attemptTwo.dynamic.EnableRampUpReclaimHardPartition)
	require.Zero(t, attemptTwo.dynamic.InitialRampUpReclaimCPUSetRatio)
}

func TestPreparePendingCPUPartitionDerivesHardFloorFromFinalCandidate(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	current := p.dynamicConfig.GetDynamicConfiguration()
	current.EnableReclaim = true
	current.EnableRampUpReclaimHardPartition = true
	current.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable = true
	frozen, err := p.captureAdvisorAttemptConfiguration()
	require.NoError(t, err)

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(2, 3))
	require.False(t, candidate.HasActiveRampUp())
	p.allocationHooks = []AllocationHook{func(_, allocation *state.AllocationInfo) error {
		if allocation.PodUid == "dedicated-pod" {
			allocation.RampUp = true
		}
		return nil
	}}

	validated := false
	_, err = p.preparePendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		dynamicConfig:    frozen.dynamic,
		persist:          false,
		source:           "final candidate hard floor test",
		validate: func(entries state.PodEntries, _ state.NUMANodeMap, _, _ bool) error {
			validated = true
			require.True(t, entries.HasActiveRampUp())
			options := p.cpuSetPartitionViewOptionsWithDynamicConfig(
				p.state, entries.HasActiveRampUp(), frozen.dynamic)
			require.True(t, options.HardPartitionEnabled)
			return nil
		},
	})
	require.NoError(t, err)
	require.True(t, validated)
	require.False(t, candidate.HasActiveRampUp(), "precommit must not mutate the caller's candidate")
}

func TestCommitPendingCPUPartitionValidatesResidualBackfillAfterHooks(t *testing.T) {
	t.Run("rejects dedicated change that overlaps share", func(t *testing.T) {
		p, err := newResidualBackfillPrecommitTestPolicy(t)
		require.NoError(t, err)
		require.False(t, p.hardBulkheadPartitionValidationEnabled())

		candidate := residualBackfillPrecommitEntries(
			machine.NewCPUSet(2, 3),
			machine.NewCPUSet(4, 5, 6, 7),
		)
		initialEntries := p.state.GetPodEntries()
		initialRevision := p.state.GetRevision()
		p.allocationHooks = []AllocationHook{func(_, allocation *state.AllocationInfo) error {
			if allocation.PodUid == "dedicated-pod" {
				allocation.AllocationResult = machine.NewCPUSet(4, 5)
				allocation.OriginalAllocationResult = machine.NewCPUSet(4, 5)
			}
			return nil
		}}

		_, _, err = p.commitPendingCPUPartition(pendingCPUPartition{
			expectedRevision: initialRevision,
			entries:          candidate,
			persist:          false,
			source:           "residual hook test",
		})
		require.ErrorContains(t, err, "validate residual backfill candidate")
		require.ErrorContains(t, err, "default share")
		require.Equal(t, initialRevision, p.state.GetRevision())
		require.Equal(t, initialEntries, p.state.GetPodEntries(),
			"candidate made invalid by a hook must not reach the revision CAS")
	})

	t.Run("rejects dedicated quantity change before residual validation", func(t *testing.T) {
		p, err := newResidualBackfillPrecommitTestPolicy(t)
		require.NoError(t, err)
		require.False(t, p.hardBulkheadPartitionValidationEnabled())

		candidate := residualBackfillPrecommitEntries(
			machine.NewCPUSet(2, 3),
			machine.NewCPUSet(4, 5, 6, 7),
		)
		initialEntries := p.state.GetPodEntries()
		initialRevision := p.state.GetRevision()
		p.allocationHooks = []AllocationHook{func(_, allocation *state.AllocationInfo) error {
			if allocation.PodUid == "dedicated-pod" {
				allocation.AllocationResult = machine.NewCPUSet(2)
				allocation.OriginalAllocationResult = machine.NewCPUSet(2)
			}
			return nil
		}}

		_, _, err = p.commitPendingCPUPartition(pendingCPUPartition{
			expectedRevision: initialRevision,
			entries:          candidate,
			persist:          false,
			source:           "residual hook test",
		})
		require.ErrorContains(t, err, "revalidate allocation shape after hooks")
		require.ErrorContains(t, err, "allocation quantity changed after hooks for dedicated-pod/main")
		require.Equal(t, initialRevision, p.state.GetRevision())
		require.Equal(t, initialEntries, p.state.GetPodEntries(),
			"candidate made invalid by a hook must not reach the revision CAS")
	})

	t.Run("accepts dedicated change with matching disjoint residual", func(t *testing.T) {
		p, err := newResidualBackfillPrecommitTestPolicy(t)
		require.NoError(t, err)
		require.False(t, p.hardBulkheadPartitionValidationEnabled())

		candidate := residualBackfillPrecommitEntries(
			machine.NewCPUSet(2, 4),
			machine.NewCPUSet(4, 5, 6, 7),
		)
		p.allocationHooks = []AllocationHook{func(_, allocation *state.AllocationInfo) error {
			if allocation.PodUid == "dedicated-pod" {
				allocation.AllocationResult = machine.NewCPUSet(2, 3)
				allocation.OriginalAllocationResult = machine.NewCPUSet(2, 3)
			}
			return nil
		}}

		committed, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
			expectedRevision: p.state.GetRevision(),
			entries:          candidate,
			persist:          false,
			source:           "residual hook test",
		})
		require.NoError(t, err)
		require.True(t, committed[commonstate.PoolNameShare][commonstate.FakedContainerName].
			AllocationResult.Equals(machine.NewCPUSet(4, 5, 6, 7)))
		require.True(t, cpuAssignmentsEqual(
			committed[commonstate.PoolNameShare][commonstate.FakedContainerName].TopologyAwareAssignments,
			map[int]machine.CPUSet{0: machine.NewCPUSet(4, 5, 6, 7)},
		))
		require.True(t, committed["dedicated-pod"]["main"].
			AllocationResult.Equals(machine.NewCPUSet(2, 3)))
	})

	t.Run("accepts share residual excluding ramp-up shared allocation", func(t *testing.T) {
		p, err := newResidualBackfillPrecommitTestPolicy(t)
		require.NoError(t, err)
		require.False(t, p.hardBulkheadPartitionValidationEnabled())

		candidate := residualBackfillPrecommitEntries(
			machine.NewCPUSet(3),
			machine.NewCPUSet(4, 5, 6, 7),
		)
		candidate["ramp-up-shared-pod"] = state.ContainerEntries{
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "ramp-up-shared-pod",
					ContainerName: "main",
					OwnerPoolName: commonstate.EmptyOwnerPoolName,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				},
				RampUp:                   true,
				AllocationResult:         machine.NewCPUSet(2),
				TopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(2)},
			},
		}

		committed, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
			expectedRevision: p.state.GetRevision(),
			entries:          candidate,
			persist:          false,
			source:           "residual ramp-up test",
		})
		require.NoError(t, err)
		require.True(t, committed[commonstate.PoolNameShare][commonstate.FakedContainerName].
			AllocationResult.Equals(machine.NewCPUSet(4, 5, 6, 7)))
		require.True(t, committed["ramp-up-shared-pod"]["main"].RampUp)
	})

	t.Run("rejects share topology assignments inconsistent with cpuset", func(t *testing.T) {
		p, err := newResidualBackfillPrecommitTestPolicy(t)
		require.NoError(t, err)

		candidate := residualBackfillPrecommitEntries(
			machine.NewCPUSet(2, 3),
			machine.NewCPUSet(4, 5, 6, 7),
		)
		machineState, err := generateMachineStateFromPodEntries(
			p.machineInfo.CPUTopology, candidate, p.state.GetMachineState())
		require.NoError(t, err)
		candidate[commonstate.PoolNameShare][commonstate.FakedContainerName].
			TopologyAwareAssignments = map[int]machine.CPUSet{0: machine.NewCPUSet(4, 5)}

		err = p.validateResidualBackfillCandidate(candidate, machineState, machine.NewCPUSet())
		require.ErrorContains(t, err, "topology assignments are inconsistent")
	})
}

func TestCommitPendingCPUPartitionNormalizesBeforeRebuildingMachineState(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()
	emitter := &recordingMetricEmitter{}
	p.emitter = emitter

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(2, 3))
	dedicated := candidate["dedicated-pod"]["main"]
	dedicated.TopologyAwareAssignments = map[int]machine.CPUSet{1: machine.NewCPUSet(2, 3)}
	dedicated.OriginalTopologyAwareAssignments = map[int]machine.CPUSet{1: machine.NewCPUSet(2, 3)}

	committed, machineState, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		disableDedicated: true,
		persist:          false,
	})
	require.NoError(t, err)
	require.True(t, committed["dedicated-pod"]["main"].TopologyAwareAssignments[0].Equals(machine.NewCPUSet(2, 3)))
	require.Empty(t, committed["dedicated-pod"]["main"].TopologyAwareAssignments[1])
	require.True(t, machineState[0].PodEntries["dedicated-pod"]["main"].AllocationResult.Equals(machine.NewCPUSet(2, 3)))
	require.Len(t, emitter.records, 1)
	requirePoolSizeMetric(t, emitter.records, commonstate.PoolNameReclaim, commonstate.PoolNameReclaim, 0, 2)
}

func TestCommitPendingCPUPartitionRejectsRevisionChangedByHook(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(2, 3))
	p.allocationHooks = []AllocationHook{func(_, _ *state.AllocationInfo) error {
		p.state.SetAllowSharedCoresOverlapReclaimedCores(
			!p.state.GetAllowSharedCoresOverlapReclaimedCores(), false)
		return nil
	}}

	_, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		disableDedicated: true,
		persist:          false,
	})
	require.ErrorIs(t, err, state.ErrStaleStateRevision)
	require.Nil(t, p.state.GetAllocationInfo("dedicated-pod", "main"))
}

func TestPreparePendingCPUPartitionRevalidatesQuantityAfterHooks(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	candidate := precommitPartitionEntries(machine.NewCPUSet(0, 48), machine.NewCPUSet(1, 49))
	p.allocationHooks = []AllocationHook{func(_, allocation *state.AllocationInfo) error {
		if allocation.PodUid == "dedicated-pod" {
			allocation.AllocationResult = machine.NewCPUSet(1)
		}
		return nil
	}}

	_, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision: p.state.GetRevision(),
		entries:          candidate,
		disableDedicated: true,
		persist:          false,
	})

	require.ErrorContains(t, err, "allocation quantity changed after hooks")
	require.Nil(t, p.state.GetAllocationInfo("dedicated-pod", "main"))
}

func TestPreparePendingCPUPartitionRejectsReplacementQuantityMutation(t *testing.T) {
	testPreparePendingCPUPartitionRejectsReplacementMutation(t,
		func(topology *machine.CPUTopology, entries state.PodEntries) {
			dedicated := coresInNUMA(topology, 0, 1, 2)
			entries["dedicated-pod"]["main"].AllocationResult =
				machine.NewCPUSet(dedicated.ToSliceInt()[0])
		},
		"allocation quantity changed",
	)
}

func TestPreparePendingCPUPartitionRejectsReplacementNUMAMigration(t *testing.T) {
	testPreparePendingCPUPartitionRejectsReplacementMutation(t,
		func(topology *machine.CPUTopology, entries state.PodEntries) {
			entries["dedicated-pod"]["main"].AllocationResult =
				coresInNUMA(topology, 1, 0, 1)
		},
		"allocation NUMA distribution changed",
	)
}

func TestPreparePendingCPUPartitionRejectsPartialReclaimCore(t *testing.T) {
	testPreparePendingCPUPartitionRejectsReplacementMutation(t,
		func(topology *machine.CPUTopology, entries state.PodEntries) {
			firstCore := coresInNUMA(topology, 0, 0, 1).ToSliceInt()
			secondCore := coresInNUMA(topology, 0, 1, 2).ToSliceInt()
			entries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].
				AllocationResult = machine.NewCPUSet(firstCore[0], secondCore[0])
		},
		"reclaim is not core-aligned",
	)
}

func testPreparePendingCPUPartitionRejectsReplacementMutation(
	t *testing.T,
	mutate func(*machine.CPUTopology, state.PodEntries),
	wantError string,
) {
	t.Helper()

	topology, err := machine.GenerateDummyCPUTopology(96, 2, 2)
	require.NoError(t, err)
	stateDir := t.TempDir()
	p, err := getTestDynamicPolicyWithoutInitialization(topology, stateDir)
	require.NoError(t, err)

	require.NoError(t, p.state.StoreState())
	initialRevision := p.state.GetRevision()
	initialEntries := p.state.GetPodEntries()
	initialMachineState := p.state.GetMachineState()
	checkpointPath := filepath.Join(stateDir, cpuPluginStateFileName)
	initialCheckpoint, err := os.ReadFile(checkpointPath)
	require.NoError(t, err)

	planned := precommitPartitionEntries(
		coresInNUMA(topology, 0, 0, 1),
		coresInNUMA(topology, 0, 1, 2),
	)

	_, _, err = p.commitPendingCPUPartition(pendingCPUPartition{
		expectedRevision:          initialRevision,
		entries:                   planned,
		disableDedicated:          true,
		persist:                   true,
		source:                    "replacement mutation test",
		requireCoreAlignedReclaim: true,
		enforceSteadyReclaim:      true,
		validate: func(entries state.PodEntries, _ state.NUMANodeMap, _, _ bool) error {
			mutate(topology, entries)
			return nil
		},
	})

	require.ErrorContains(t, err, wantError)
	require.Equal(t, initialRevision, p.state.GetRevision())
	require.Equal(t, initialEntries, p.state.GetPodEntries())
	require.Equal(t, initialMachineState, p.state.GetMachineState())
	afterCheckpoint, readErr := os.ReadFile(checkpointPath)
	require.NoError(t, readErr)
	require.Equal(t, initialCheckpoint, afterCheckpoint)
}

func TestValidateSteadyReclaimPrecommitInvariant(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(32, 2, 2)
	require.NoError(t, err)
	committed := coresInNUMA(topology, 0, 0, 4)
	planned := coresInNUMA(topology, 0, 2, 4).
		Union(coresInNUMA(topology, 1, 0, 2))

	t.Run("accepts planner migration above fixed limit when hooks preserve plan", func(t *testing.T) {
		largePlan := coresInNUMA(topology, 1, 0, 4)
		require.Greater(t,
			steadyFakeNUMAMigrationChurn(committed, largePlan),
			steadyFakeNUMAMaxMigratedCPUs,
		)
		require.NoError(t, validateSteadyReclaimPrecommitInvariant(
			largePlan, largePlan, topology))
	})

	for _, tc := range []struct {
		name      string
		candidate machine.CPUSet
		want      string
	}{
		{
			name: "fragmented whole core",
			candidate: planned.Difference(machine.NewCPUSet(planned.ToSliceInt()[0])).
				Union(machine.NewCPUSet(coresInNUMA(topology, 1, 2, 3).ToSliceInt()[0])),
			want: "not core-aligned",
		},
		{
			name:      "quantity changed",
			candidate: coresInNUMA(topology, 0, 2, 4),
			want:      "quantity changed",
		},
		{
			name:      "per-NUMA floor changed",
			candidate: coresInNUMA(topology, 0, 0, 4),
			want:      "NUMA distribution changed",
		},
		{
			name: "hook churn exceeds limit",
			candidate: coresInNUMA(topology, 0, 4, 6).
				Union(coresInNUMA(topology, 1, 2, 4)),
			want: "migration churn",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateSteadyReclaimPrecommitInvariant(
				planned, tc.candidate, topology)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestCommitPendingAdvisorStateRejectsFragmentedReclaimInDisjointMode(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	revision := p.state.GetRevision()
	core := coresInNUMA(p.machineInfo.CPUTopology, 0, 0, 1)
	fragmentedReclaim := machine.NewCPUSet(core.ToSliceInt()[0])
	dedicated := coresInNUMA(p.machineInfo.CPUTopology, 0, 1, 2)

	err := p.commitPendingAdvisorState(&pendingAdvisorState{
		preCommitRevision: revision,
		entries:           precommitPartitionEntries(fragmentedReclaim, dedicated),
		allowOverlap:      false,
		disableDedicated:  true,
	})

	require.ErrorContains(t, err, "reclaim set")
	require.ErrorContains(t, err, "is not core-aligned")
	require.Equal(t, revision, p.state.GetRevision(),
		"fragmented reclaim must be rejected before the revision CAS")
}

func TestPoolAdjustmentRejectsFragmentedReclaimBeforeCommit(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(96, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)
	p.state.SetDisableDedicatedCoresOverlapReclaimedCores(true, false)
	currentReclaim := machine.NewCPUSet(1, 25, 49, 73)
	currentAssignments, err := machine.GetNumaAwareAssignments(topology, currentReclaim)
	require.NoError(t, err)
	p.state.SetAllocationInfo(
		commonstate.PoolNameReclaim,
		commonstate.FakedContainerName,
		&state.AllocationInfo{
			AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
			AllocationResult:                 currentReclaim.Clone(),
			OriginalAllocationResult:         currentReclaim.Clone(),
			TopologyAwareAssignments:         currentAssignments,
			OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(currentAssignments),
		},
		false,
	)

	revision := p.state.GetRevision()
	currentEntries := p.state.GetPodEntries()
	require.NoError(t, assertCoreAligned(currentReclaim, p.machineInfo.CPUTopology))
	require.Equal(t, 4, currentReclaim.Size())

	err = p.applyPoolsAndIsolatedInfo(
		map[string]machine.CPUSet{
			commonstate.PoolNameReclaim: machine.NewCPUSet(1, 2, 25, 26),
			commonstate.PoolNameReserve: p.reservedCPUs.Clone(),
		},
		map[string]map[string]machine.CPUSet{},
		currentEntries,
		p.state.GetMachineState(),
		sets.NewInt(),
		false,
		machine.NewCPUSet(),
		defaultShareMaterializationPlan{},
		revision,
	)

	require.ErrorContains(t, err, "reclaim set")
	require.ErrorContains(t, err, "is not core-aligned")
	require.Equal(t, revision, p.state.GetRevision(),
		"fragmented pool adjustment must be rejected before the revision CAS")
	require.True(t, p.state.GetAllocationInfo(
		commonstate.PoolNameReclaim, commonstate.FakedContainerName).
		AllocationResult.Equals(currentReclaim))
}

func TestCommitPendingCPUPartitionRejectsInvalidOverrideAndDeletionFallback(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	for _, source := range []string{"cpuset override", "deletion fallback"} {
		t.Run(source, func(t *testing.T) {
			candidate := precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(1, 2))
			_, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
				expectedRevision: p.state.GetRevision(),
				entries:          candidate,
				disableDedicated: true,
				persist:          false,
				source:           source,
			})
			require.Error(t, err)
			require.False(t, errors.Is(err, state.ErrStaleStateRevision))
			require.ErrorContains(t, err, "overlaps reclaim pool")
		})
	}
}

func TestValidateAdvisorPartitionBeforeCommitAllowsDedicatedOverlapWhenEnabled(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	err := p.validateAdvisorPartitionBeforeCommit(
		precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(1, 2)),
		nil,
		false,
		false,
	)
	require.NoError(t, err)
}

func TestValidateAdvisorPartitionBeforeCommitRejectsDedicatedOverlapWhenDisabled(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	err := p.validateAdvisorPartitionBeforeCommit(
		precommitPartitionEntries(machine.NewCPUSet(0, 1), machine.NewCPUSet(1, 2)),
		nil,
		false,
		true,
	)
	require.ErrorContains(t, err, "overlaps reclaim pool")
}

func TestValidateAdvisorPartitionBeforeCommitRejectsSharedOverlapWhenDisabled(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	err := p.validateAdvisorPartitionBeforeCommit(
		precommitPartitionEntriesWithShare(
			machine.NewCPUSet(0, 1),
			machine.NewCPUSet(2, 3),
			machine.NewCPUSet(1, 4),
		),
		nil,
		false,
		false,
	)
	require.ErrorContains(t, err, "reclaim pool overlaps disallowed shared partition before commit")
}

func TestValidateAdvisorPartitionBeforeCommitAllowsSharedOverlapWhenEnabled(t *testing.T) {
	p, cleanup := newReclaimReuseTestPolicy(t)
	defer cleanup()

	err := p.validateAdvisorPartitionBeforeCommit(
		precommitPartitionEntriesWithShare(
			machine.NewCPUSet(0, 1),
			machine.NewCPUSet(1, 2),
			machine.NewCPUSet(0, 3),
		),
		nil,
		true,
		false,
	)
	require.NoError(t, err)
}

func TestValidatePendingPoolOwnershipSupportsReclaimModesAndExactOrdinaryPools(t *testing.T) {
	reclaim := machine.NewCPUSet(0, 1, 4, 5)
	targetNUMA := machine.NewCPUSet(0, 1)
	nonRNB := machine.NewCPUSet(4, 5)
	share := machine.NewCPUSet(6, 7)
	entries := state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult:                 reclaim.Clone(),
				OriginalAllocationResult:         reclaim.Clone(),
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: targetNUMA.Clone(), 1: machine.NewCPUSet(4, 5)},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: targetNUMA.Clone(), 1: machine.NewCPUSet(4, 5)},
			},
		},
		"rnb-pod": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "rnb-pod",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameReclaim,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelReclaimedCores,
					Annotations: map[string]string{
						apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
					},
				},
				AllocationResult:         targetNUMA.Clone(),
				TopologyAwareAssignments: map[int]machine.CPUSet{0: targetNUMA.Clone()},
			},
		},
		"non-rnb-pod": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "non-rnb-pod",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameReclaim,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelReclaimedCores,
				},
				AllocationResult:         nonRNB.Clone(),
				TopologyAwareAssignments: map[int]machine.CPUSet{1: nonRNB.Clone()},
			},
		},
		commonstate.PoolNameShare: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
				AllocationResult:                 share.Clone(),
				OriginalAllocationResult:         share.Clone(),
				TopologyAwareAssignments:         map[int]machine.CPUSet{1: share.Clone()},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{1: share.Clone()},
			},
		},
		"shared-pod": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "shared-pod",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameShare,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				},
				AllocationResult:         share.Clone(),
				TopologyAwareAssignments: map[int]machine.CPUSet{1: share.Clone()},
			},
		},
	}
	entries["rnb-pod"]["main"].SetSpecifiedNUMABindingNUMAID([]uint64{0})

	require.NoError(t, validatePendingPoolOwnership(entries))

	entries["rnb-pod"]["main"].AllocationResult = machine.NewCPUSet(4, 5)
	require.ErrorContains(t, validatePendingPoolOwnership(entries), "differs from owner pool")
	entries["rnb-pod"]["main"].AllocationResult = targetNUMA.Clone()

	entries["non-rnb-pod"]["main"].AllocationResult = reclaim.Clone()
	require.ErrorContains(t, validatePendingPoolOwnership(entries), "differs from owner pool")
	entries["non-rnb-pod"]["main"].AllocationResult = nonRNB.Clone()

	entries["shared-pod"]["main"].AllocationResult = machine.NewCPUSet(6)
	require.ErrorContains(t, validatePendingPoolOwnership(entries), "differs from owner pool")
}

func TestRampUpSidecarMirrorsMainContainerThroughPrecommitAndRampUpExit(t *testing.T) {
	for _, tc := range []struct {
		name        string
		annotations map[string]string
	}{
		{
			name: "shared",
		},
		{
			name: "shared numa binding",
			annotations: map[string]string{
				apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, cleanup := newReclaimReuseTestPolicy(t)
			defer cleanup()

			rampUpCPUs := machine.NewCPUSet(2, 3)
			rampUpAssignments := map[int]machine.CPUSet{0: rampUpCPUs.Clone()}
			main := &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "shared-pod",
					ContainerName: "main",
					ContainerType: pluginapi.ContainerType_MAIN.String(),
					OwnerPoolName: commonstate.EmptyOwnerPoolName,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					Annotations:   tc.annotations,
				},
				RampUp:                           true,
				AllocationResult:                 rampUpCPUs.Clone(),
				OriginalAllocationResult:         rampUpCPUs.Clone(),
				TopologyAwareAssignments:         machine.DeepcopyCPUAssignment(rampUpAssignments),
				OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(rampUpAssignments),
			}
			sidecar := &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "shared-pod",
					ContainerName: "sidecar",
					ContainerType: pluginapi.ContainerType_SIDECAR.String(),
					OwnerPoolName: commonstate.EmptyOwnerPoolName,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					Annotations:   map[string]string{},
				},
				RampUp:                           false,
				AllocationResult:                 machine.NewCPUSet(4, 5),
				OriginalAllocationResult:         machine.NewCPUSet(4, 5),
				TopologyAwareAssignments:         map[int]machine.CPUSet{0: machine.NewCPUSet(4, 5)},
				OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(4, 5)},
			}

			require.NoError(t, validatePendingPoolOwnership(state.PodEntries{
				"shared-pod": {"main": main},
			}), "a ramp-up main container may have an empty owner")
			require.ErrorContains(t, validatePendingPoolOwnership(state.PodEntries{
				"shared-pod": {"main": main, "sidecar": sidecar},
			}), "empty owner pool", "a stale non-ramp-up sidecar must not bypass ownership validation")

			require.True(t, p.applySidecarAllocationInfoFromMainContainer(sidecar, main))
			require.Equal(t, main.RampUp, sidecar.RampUp)
			require.Equal(t, main.OwnerPoolName, sidecar.OwnerPoolName)
			require.True(t, sidecar.AllocationResult.Equals(main.AllocationResult))
			require.True(t, sidecar.OriginalAllocationResult.Equals(main.OriginalAllocationResult))
			require.True(t, state.CheckAllocationInfoTopologyAwareAssignments(sidecar, main))
			require.True(t, state.CheckAllocationInfoOriginTopologyAwareAssignments(sidecar, main))

			entries := precommitPartitionEntriesWithShare(
				machine.NewCPUSet(0, 1),
				machine.NewCPUSet(6, 7),
				rampUpCPUs,
			)
			entries["shared-pod"] = state.ContainerEntries{"main": main, "sidecar": sidecar}
			committed, _, err := p.commitPendingCPUPartition(pendingCPUPartition{
				expectedRevision: p.state.GetRevision(),
				entries:          entries,
				disableDedicated: true,
				persist:          false,
				source:           "ramp-up sidecar test",
			})
			require.NoError(t, err)
			require.True(t, committed["shared-pod"]["sidecar"].RampUp)

			main = committed["shared-pod"]["main"].Clone()
			sidecar = committed["shared-pod"]["sidecar"].Clone()
			main.RampUp = false
			main.OwnerPoolName = commonstate.PoolNameShare
			require.True(t, p.applySidecarAllocationInfoFromMainContainer(sidecar, main))
			require.False(t, sidecar.RampUp)
			require.Equal(t, commonstate.PoolNameShare, sidecar.OwnerPoolName)
			require.NoError(t, validatePendingPoolOwnership(state.PodEntries{
				commonstate.PoolNameShare: entries[commonstate.PoolNameShare],
				"shared-pod":              {"main": main, "sidecar": sidecar},
			}))
		})
	}
}

func precommitPartitionEntries(reclaim, dedicated machine.CPUSet) state.PodEntries {
	return state.PodEntries{
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult:         reclaim.Clone(),
				OriginalAllocationResult: reclaim.Clone(),
			},
		},
		"dedicated-pod": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "dedicated-pod",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameDedicated,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
				},
				AllocationResult:         dedicated.Clone(),
				OriginalAllocationResult: dedicated.Clone(),
			},
		},
	}
}

func precommitPartitionEntriesWithShare(reclaim, dedicated, share machine.CPUSet) state.PodEntries {
	entries := precommitPartitionEntries(reclaim, dedicated)
	entries[commonstate.PoolNameShare] = state.ContainerEntries{
		commonstate.FakedContainerName: &state.AllocationInfo{
			AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
			AllocationResult:         share.Clone(),
			OriginalAllocationResult: share.Clone(),
		},
	}
	return entries
}

func newResidualBackfillPrecommitTestPolicy(t *testing.T) (*DynamicPolicy, error) {
	t.Helper()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	if err != nil {
		return nil, err
	}
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	if err != nil {
		return nil, err
	}
	p.reservedCPUs = machine.NewCPUSet()
	dynamicConf := p.dynamicConfig.GetDynamicConfiguration()
	dynamicConf.FillDefaultSharePoolWithNonReclaimCPUs = true
	dynamicConf.EnableRampUpReclaimHardPartition = false
	dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable = false
	return p, nil
}

func residualBackfillPrecommitEntries(dedicated, share machine.CPUSet) state.PodEntries {
	return precommitPartitionEntriesWithShare(
		machine.NewCPUSet(0, 1),
		dedicated,
		share,
	)
}
