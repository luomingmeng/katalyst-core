package dynamicpolicy

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestInitialReclaimPoolRepairsEmptyAndMissing(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("existing=%t/enabled=%t", existing, enabled), func(t *testing.T) {
				topology, err := machine.GenerateDummyCPUTopology(32, 1, 2)
				require.NoError(t, err)
				stateDir := t.TempDir()
				p, err := getTestDynamicPolicyWithoutInitialization(topology, stateDir)
				require.NoError(t, err)
				p.config.dynamicConfig.GetDynamicConfiguration().EnableReclaim = enabled
				if existing {
					require.NoError(t, p.state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName,
						&state.AllocationInfo{AllocationMeta: commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
							AllocationResult: machine.NewCPUSet(), OriginalAllocationResult: machine.NewCPUSet()}, true))
				}
				require.NoError(t, p.installDerivedReclaimPreference())
				before := p.state.GetRevision()
				require.NoError(t, p.ensureInitialReclaimPool())
				require.Equal(t, before+1, p.state.GetRevision(), "pool and machine state must publish once")
				pool := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
				require.NotNil(t, pool)
				require.False(t, pool.AllocationResult.IsEmpty())
				require.NoError(t, assertCoreAligned(pool.AllocationResult, topology))
				reopened := openCheckpointState(t, topology, stateDir)
				require.Equal(t, p.state.GetPodEntries(), reopened.GetPodEntries())
				require.Equal(t, p.state.GetMachineState(), reopened.GetMachineState())
				before = p.state.GetRevision()
				require.NoError(t, p.ensureInitialReclaimPool())
				require.Equal(t, before, p.state.GetRevision(), "non-empty identity must not be rewritten")
			})
		}
	}
}

func TestInitialReclaimPoolNoCompleteCandidateDoesNotWrite(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.config.reservedCPUs = topology.CPUDetails.CPUs()
	p.reclaim.reservedReclaimedCPUsSize = 2
	entries, ms, revision := p.state.GetAdvisorStateSnapshot()
	err = p.ensureInitialReclaimPool()
	require.Error(t, err)
	require.Equal(t, revision, p.state.GetRevision())
	require.Equal(t, entries, p.state.GetPodEntries())
	require.Equal(t, ms, p.state.GetMachineState())
}

func TestInitialReclaimPoolPersistenceFailureRestoresState(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(32, 1, 2)
	require.NoError(t, err)
	stateDir := t.TempDir()
	p, err := getTestDynamicPolicyWithoutInitialization(topology, stateDir)
	require.NoError(t, err)
	require.NoError(t, p.installDerivedReclaimPreference())
	entries, ms, revision := p.state.GetAdvisorStateSnapshot()
	// A directory at the checkpoint destination forces the real atomic rename
	// to fail after the in-memory CAS; the checkpoint store must undo that CAS.
	checkpoint := filepath.Join(stateDir, cpuPluginStateFileName)
	require.NoError(t, os.Remove(checkpoint))
	require.NoError(t, os.Mkdir(checkpoint, 0700))
	err = p.ensureInitialReclaimPool()
	require.Error(t, err)
	require.Equal(t, revision, p.state.GetRevision())
	require.Equal(t, entries, p.state.GetPodEntries())
	require.Equal(t, ms, p.state.GetMachineState())
}

func TestInitialReclaimPoolStaleRevisionPreservesConcurrentWriter(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(32, 1, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, p.installDerivedReclaimPreference())
	tracked := &bypassTrackingState{State: p.state, bumpBeforeCommit: true}
	p.state = tracked
	err = p.ensureInitialReclaimPool()
	require.Error(t, err)
	require.Equal(t, 1, tracked.commitIfRevisionCalls)
	require.NotNil(t, p.state.GetAllocationInfo("bump-pod", "bump-container"))
	require.Nil(t, p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName))
}

func TestNewDynamicPolicyRepairsEmptyReclaimCheckpoint(t *testing.T) {
	topology := contiguous180CPUTopology(t)
	stateDir := t.TempDir()
	p, err := getTestDynamicPolicyWithInitialization(topology, stateDir)
	require.NoError(t, err)
	require.NoError(t, p.state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName,
		&state.AllocationInfo{AllocationMeta: commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
			AllocationResult: machine.NewCPUSet(), OriginalAllocationResult: machine.NewCPUSet()}, true))
	ok, _, err := NewDynamicPolicy(freshRealConstructorContext(t, topology, stateDir),
		freshRealConstructorConf(t, stateDir), nil, "empty-reclaim-bootstrap")
	require.NoError(t, err)
	require.True(t, ok)
	pool := openCheckpointState(t, topology, stateDir).GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.NotNil(t, pool)
	require.False(t, pool.AllocationResult.IsEmpty())
	require.NoError(t, assertCoreAligned(pool.AllocationResult, topology))
}

func TestInitialReclaimPoolResyncsExistingReclaimedMembers(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(32, 1, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, p.state.SetAllocationInfo("reclaimed-pod", "main", &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{PodUid: "reclaimed-pod", ContainerName: "main",
			ContainerType: pluginapi.ContainerType_MAIN.String(), OwnerPoolName: commonstate.PoolNameReclaim,
			QoSLevel: apiconsts.PodAnnotationQoSLevelReclaimedCores},
		AllocationResult: machine.NewCPUSet(), OriginalAllocationResult: machine.NewCPUSet(),
	}, false))
	require.NoError(t, p.installDerivedReclaimPreference())
	require.NoError(t, p.ensureInitialReclaimPool())
	pool := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	member := p.state.GetAllocationInfo("reclaimed-pod", "main")
	require.True(t, pool.AllocationResult.Equals(member.AllocationResult))
	require.NoError(t, validatePendingPoolOwnership(p.state.GetPodEntries()))
}

func TestInitialReclaimPoolPreservesShareScalarWithStaleBuckets(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(32, 1, 2)
	require.NoError(t, err)
	stateDir := t.TempDir()
	p, err := getTestDynamicPolicyWithoutInitialization(topology, stateDir)
	require.NoError(t, err)
	p.config.reservedCPUs = machine.NewCPUSet()
	require.NoError(t, p.installDerivedReclaimPreference())
	share := &state.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
		AllocationResult: machine.NewCPUSet(2, 18), OriginalAllocationResult: machine.NewCPUSet(2, 18),
		TopologyAwareAssignments:         map[int]machine.CPUSet{0: machine.NewCPUSet(0, 16)},
		OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(0, 16)},
	}
	main := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{PodUid: "shared-pod", ContainerName: "main",
			OwnerPoolName: commonstate.PoolNameShare, ContainerType: pluginapi.ContainerType_MAIN.String(),
			QoSLevel: apiconsts.PodAnnotationQoSLevelSharedCores},
		AllocationResult: machine.NewCPUSet(2, 18), OriginalAllocationResult: machine.NewCPUSet(2, 18),
		TopologyAwareAssignments:         map[int]machine.CPUSet{0: machine.NewCPUSet(2, 18)},
		OriginalTopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(2, 18)},
	}
	entries := state.PodEntries{commonstate.PoolNameShare: {commonstate.FakedContainerName: share}, "shared-pod": {"main": main}}
	ms, err := generateMachineStateFromPodEntries(topology, entries, p.state.GetMachineState())
	require.NoError(t, err)
	require.NoError(t, p.state.CommitAdvisorState(entries, ms, false, false, true))
	ok, _, err := NewDynamicPolicy(freshRealConstructorContext(t, topology, stateDir), freshRealConstructorConf(t, stateDir), nil, "stale-pool-buckets")
	require.NoError(t, err)
	require.True(t, ok)
	reopened := openCheckpointState(t, topology, stateDir)
	published := reopened.GetPodEntries()
	finalShare := reopened.GetAllocationInfo(commonstate.PoolNameShare, commonstate.FakedContainerName)
	require.True(t, finalShare.AllocationResult.Equals(share.AllocationResult))
	require.True(t, reclaimPoolCPUSet(published).Intersection(finalShare.AllocationResult).IsEmpty())
	require.Equal(t, main, reopened.GetAllocationInfo("shared-pod", "main"))
	expectedAssignments, err := machine.GetNumaAwareAssignments(topology, share.AllocationResult)
	require.NoError(t, err)
	require.Equal(t, expectedAssignments, finalShare.TopologyAwareAssignments)
}

func TestInitialReclaimPoolBacksActiveRNBBeforePreference(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(48, 1, 3)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.config.reservedCPUs = machine.NewCPUSet()
	p.reclaim.reservedReclaimedCPUsSize = 4
	require.NoError(t, p.installDerivedReclaimPreference())
	member := makeReclaimedRNBAllocation("rnb-pod", "main", 2, machine.NewCPUSet())
	member.ContainerType = pluginapi.ContainerType_MAIN.String()
	sidecar := member.Clone()
	sidecar.ContainerName = "sidecar"
	sidecar.ContainerType = pluginapi.ContainerType_SIDECAR.String()
	entries := state.PodEntries{"rnb-pod": {"main": member, "sidecar": sidecar}}
	ms, err := generateMachineStateFromPodEntries(topology, entries, p.state.GetMachineState())
	require.NoError(t, err)
	require.NoError(t, p.state.CommitAdvisorState(entries, ms, false, false, false))
	before := p.state.GetRevision()
	require.NoError(t, p.ensureInitialReclaimPool())
	require.Equal(t, before+1, p.state.GetRevision())
	pool := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	require.Equal(t, 4, pool.AllocationResult.Size())
	require.False(t, pool.TopologyAwareAssignments[2].IsEmpty())
	require.NoError(t, assertCoreAligned(pool.AllocationResult, topology))
	require.Equal(t, p.state.GetAllocationInfo("rnb-pod", "main").AllocationResult,
		p.state.GetAllocationInfo("rnb-pod", "sidecar").AllocationResult)
	require.NoError(t, validatePendingPoolOwnership(p.state.GetPodEntries()))
}

func TestInitialReclaimPoolRequiredNUMAShortageDoesNotWrite(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(48, 1, 3)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.config.reservedCPUs = topology.CPUDetails.CPUsInNUMANodes(2)
	require.NoError(t, p.installDerivedReclaimPreference())
	member := makeReclaimedRNBAllocation("rnb-pod", "main", 2, machine.NewCPUSet())
	require.NoError(t, p.state.SetAllocationInfo("rnb-pod", "main", member, false))
	entries, ms, revision := p.state.GetAdvisorStateSnapshot()
	err = p.ensureInitialReclaimPool()
	require.ErrorIs(t, err, errReclaimFallbackEmptyRequired)
	require.Equal(t, revision, p.state.GetRevision())
	require.Equal(t, entries, p.state.GetPodEntries())
	require.Equal(t, ms, p.state.GetMachineState())
}

func TestInitialReclaimPoolRejectsUnrelatedOwnershipMismatch(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(32, 1, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, p.installDerivedReclaimPreference())
	require.NoError(t, p.state.SetAllocationInfo(commonstate.PoolNameShare, commonstate.FakedContainerName, &state.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
		AllocationResult: machine.NewCPUSet(2, 18), OriginalAllocationResult: machine.NewCPUSet(2, 18),
	}, false))
	require.NoError(t, p.state.SetAllocationInfo("shared-pod", "main", &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{PodUid: "shared-pod", ContainerName: "main", OwnerPoolName: commonstate.PoolNameShare,
			ContainerType: pluginapi.ContainerType_MAIN.String(), QoSLevel: apiconsts.PodAnnotationQoSLevelSharedCores},
		AllocationResult: machine.NewCPUSet(4, 20), OriginalAllocationResult: machine.NewCPUSet(4, 20),
	}, false))
	entries, ms, revision := p.state.GetAdvisorStateSnapshot()
	err = p.ensureInitialReclaimPool()
	require.ErrorContains(t, err, "validate initial reclaim ownership")
	require.Equal(t, revision, p.state.GetRevision())
	require.Equal(t, entries, p.state.GetPodEntries())
	require.Equal(t, ms, p.state.GetMachineState())
}

func TestInitialReclaimPoolBacksEveryActiveRNBNUMA(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(48, 1, 3)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.config.reservedCPUs = machine.NewCPUSet()
	p.reclaim.reservedReclaimedCPUsSize = 4
	require.NoError(t, p.installDerivedReclaimPreference())
	for numaID := 0; numaID < 3; numaID++ {
		uid := fmt.Sprintf("rnb-%d", numaID)
		require.NoError(t, p.state.SetAllocationInfo(uid, "main",
			makeReclaimedRNBAllocation(uid, "main", numaID, machine.NewCPUSet()), false))
	}
	require.NoError(t, p.ensureInitialReclaimPool())
	pool := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	// Required buckets outrank the four-CPU preference; each needs a whole core.
	require.Equal(t, 6, pool.AllocationResult.Size())
	for numaID := 0; numaID < 3; numaID++ {
		require.Equal(t, 2, pool.TopologyAwareAssignments[numaID].Size())
	}
	require.NoError(t, validatePendingPoolOwnership(p.state.GetPodEntries()))
}

func TestInitialReclaimPoolRejectsRampUpPartitionOverlap(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(32, 1, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	dyn := p.config.dynamicConfig.GetDynamicConfiguration()
	dyn.EnableReclaim = true
	dyn.EnableRampUpReclaimHardPartition = true
	require.NoError(t, p.installDerivedReclaimPreference())
	all := topology.CPUDetails.CPUs()
	require.NoError(t, p.state.SetAllocationInfo("shared-pod", "main", &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{PodUid: "shared-pod", ContainerName: "main",
			ContainerType: pluginapi.ContainerType_MAIN.String(), QoSLevel: apiconsts.PodAnnotationQoSLevelSharedCores},
		AllocationResult: all.Clone(), OriginalAllocationResult: all.Clone(), RampUp: true,
	}, false))
	entries, ms, revision := p.state.GetAdvisorStateSnapshot()
	err = p.ensureInitialReclaimPool()
	require.ErrorContains(t, err, "validate initial reclaim partition")
	require.Equal(t, revision, p.state.GetRevision())
	require.Equal(t, entries, p.state.GetPodEntries())
	require.Equal(t, ms, p.state.GetMachineState())
}
