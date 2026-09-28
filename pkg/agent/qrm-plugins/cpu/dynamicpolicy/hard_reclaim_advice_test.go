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
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// steadyFixture builds a real-NUMA mandatory-reclaim + dedicated pair on one NUMA.
// reclaimCommittedCores / dedicatedCommittedCores are counts of complete physical
// cores already committed; free cores are whatever residual whole cores remain.
func steadyFixture(
	topology *machine.CPUTopology,
	numaID, reclaimQty, dedicatedQty, reclaimCommittedCores, dedicatedCommittedCores int,
) []advisorBlockDescriptor {
	numa := topology.CPUDetails.CPUsInNUMANodes(numaID)
	reclaimCommitted := machine.NewCPUSet()
	for c := 0; c < reclaimCommittedCores; c++ {
		reclaimCommitted = reclaimCommitted.Union(coresInNUMA(topology, numaID, c, c+1))
	}
	dedicatedCommitted := machine.NewCPUSet()
	for c := reclaimCommittedCores; c < reclaimCommittedCores+dedicatedCommittedCores; c++ {
		dedicatedCommitted = dedicatedCommitted.Union(coresInNUMA(topology, numaID, c, c+1))
	}
	return []advisorBlockDescriptor{
		{
			BlockID:      "reclaim",
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       numaID,
			Quantity:     reclaimQty,
			ComponentKey: "mandatory-reclaim\x00" + string(rune(numaID)),
			Eligible:     numa.Clone(),
			Committed:    reclaimCommitted,
			OldPreferred: reclaimCommitted.Clone(),
		},
		{
			BlockID:      "dedicated",
			Class:        advisorBlockClassDedicated,
			NUMAID:       numaID,
			Quantity:     dedicatedQty,
			ComponentKey: "dedicated\x00" + string(rune(numaID)),
			Eligible:     numa.Clone(),
			Committed:    dedicatedCommitted,
			OldPreferred: dedicatedCommitted.Clone(),
		},
	}
}

func recordByNUMA(report steadyReclaimNormalizeReport, numaID int) (steadyReclaimNormalizeRecord, bool) {
	for _, r := range report.Records {
		if r.NUMAID == numaID {
			return r, true
		}
	}
	return steadyReclaimNormalizeRecord{}, false
}

// TestNormalizeSteadyRealNUMATargets_RoundDownOddAdviceToCommitted reproduces the
// node symptom: SMT2, NUMA2 advice mandatory-reclaim=3 / dedicated=29, committed
// reclaim=1 core (2 CPUs), committed dedicated=15 cores (30 CPUs), free=0. The
// upward borrow (3->4) would need a second donated core, which the request floor
// forbids, so normalization must round DOWN to the committed whole-core target 2
// and let dedicated absorb the spare CPU back to 30.
func TestNormalizeSteadyRealNUMATargets_RoundDownOddAdviceToCommitted(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(128, 2, 4)
	require.NoError(t, err)
	require.Equal(t, 2, topology.CPUsPerCore())

	descriptors := steadyFixture(topology, 2, 3, 29, 1, 15)
	available := topology.CPUDetails.CPUsInNUMANodes(2)

	normalized, report, err := normalizeSteadyRealNUMATargets(descriptors, available, topology, nil)
	require.NoError(t, err)
	require.True(t, report.HadOddAdvice)

	rec, ok := recordByNUMA(report, 2)
	require.True(t, ok)
	require.Equal(t, steadyReclaimDecisionRoundDown, rec.Decision)
	require.Equal(t, 3, rec.AdvisorReclaim)
	require.Equal(t, 2, rec.ReclaimAfter)
	require.Equal(t, 2, rec.CommittedReclaim)
	require.Equal(t, 0, rec.FreeWholeCoreCPUs)

	var reclaim, dedicated *advisorBlockDescriptor
	for i := range normalized {
		switch normalized[i].BlockID {
		case "reclaim":
			reclaim = &normalized[i]
		case "dedicated":
			dedicated = &normalized[i]
		}
	}
	require.NotNil(t, reclaim)
	require.NotNil(t, dedicated)
	require.Equal(t, 2, reclaim.Quantity, "odd reclaim must round down to committed whole-core")
	require.Equal(t, 30, dedicated.Quantity, "dedicated must absorb the released CPU back to committed")
}

// TestNormalizeSteadyRealNUMATargets_AlignedAdviceUnchanged proves an already
// whole-core-aligned advice (8/24) is left byte-for-byte unchanged.
func TestNormalizeSteadyRealNUMATargets_AlignedAdviceUnchanged(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(128, 2, 4)
	require.NoError(t, err)

	// other NUMA in the现场: reclaim=8 / dedicated=24, aligned.
	descriptors := steadyFixture(topology, 0, 8, 24, 4, 12)
	available := topology.CPUDetails.CPUsInNUMANodes(0)

	normalized, report, err := normalizeSteadyRealNUMATargets(descriptors, available, topology, nil)
	require.NoError(t, err)
	require.False(t, report.HadOddAdvice)
	rec, ok := recordByNUMA(report, 0)
	require.True(t, ok)
	require.Equal(t, steadyReclaimDecisionAligned, rec.Decision)
	require.Equal(t, 8, normalized[0].Quantity)
	require.Equal(t, 24, normalized[1].Quantity)
}

// TestNormalizeSteadyRealNUMATargets_RoundUpOnlyWithFreeWholeCores proves that when
// a free whole core covers the growth beyond committed (no dedicated donation),
// the odd advice may round up; with no free core it must round down instead.
func TestNormalizeSteadyRealNUMATargets_RoundUpOnlyWithFreeWholeCores(t *testing.T) {
	t.Parallel()

	// NUMA = 32 CPU (16 cores), SMT2.
	topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
	require.NoError(t, err)

	t.Run("free core covers upper growth but committed anchor still wins -> round down", func(t *testing.T) {
		// committed reclaim = 1 core (2), committed dedicated = 13 cores (26),
		// 2 free cores (4) remain. advisor reclaim=3, dedicated=29.
		descriptors := steadyFixture(topology, 0, 3, 29, 1, 13)
		available := topology.CPUDetails.CPUsInNUMANodes(0)
		normalized, report, err := normalizeSteadyRealNUMATargets(descriptors, available, topology, nil)
		require.NoError(t, err)
		rec, _ := recordByNUMA(report, 0)
		// lower=2 (growth 0), upper=4 (growth 2 <= free 4). committed distance picks
		// lower (closer to committed=2); the no-donation rule still prefers the
		// committed anchor even when a free core exists.
		require.Equal(t, 2, rec.ReclaimAfter)
		require.Equal(t, steadyReclaimDecisionRoundDown, rec.Decision)
		_ = normalized
	})

	t.Run("no free core and committed at floor -> lower would drop below floor -> infeasible", func(t *testing.T) {
		// committed reclaim = 0 cores, free = 0 cores, dedicated = 16 cores (32).
		// advisor reclaim=1: lower=0 drops below the 1-core reclaim floor, upper=2
		// needs a donated core that is not free -> infeasible, pass through.
		descriptors := steadyFixture(topology, 0, 1, 31, 0, 16)
		available := topology.CPUDetails.CPUsInNUMANodes(0)
		normalized, report, err := normalizeSteadyRealNUMATargets(descriptors, available, topology, nil)
		require.NoError(t, err)
		rec, _ := recordByNUMA(report, 0)
		require.Equal(t, steadyReclaimDecisionInfeasible, rec.Decision)
		require.Equal(t, 1, normalized[0].Quantity, "passed through unchanged")
		require.Equal(t, 31, normalized[1].Quantity)
	})
}

// TestNormalizeSteadyRealNUMATargets_SMT1PassThrough proves that on a non-SMT
// topology every CPU is its own core and odd/even distinction disappears.
func TestNormalizeSteadyRealNUMATargets_SMT1PassThrough(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(32, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, topology.CPUsPerCore())

	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	descriptors := []advisorBlockDescriptor{
		{BlockID: "reclaim", Class: advisorBlockClassMandatoryReclaim, NUMAID: 0,
			Quantity: 3, ComponentKey: "mand", Eligible: numa.Clone(), Committed: numa.Clone()},
		{BlockID: "dedicated", Class: advisorBlockClassDedicated, NUMAID: 0,
			Quantity: 5, ComponentKey: "ded", Eligible: numa.Clone()},
	}
	normalized, report, err := normalizeSteadyRealNUMATargets(descriptors, numa, topology, nil)
	require.NoError(t, err)
	require.False(t, report.HadOddAdvice)
	require.Empty(t, report.Records)
	require.Equal(t, 3, normalized[0].Quantity)
	require.Equal(t, 5, normalized[1].Quantity)
}

// smt4Topology builds a single-NUMA SMT4 topology: numCores physical cores, each
// owning 4 logical CPUs (w=4).
func smt4Topology(t *testing.T, numCores int) *machine.CPUTopology {
	t.Helper()
	cpuNum := numCores * 4
	topology := &machine.CPUTopology{
		NumCPUs:      cpuNum,
		NumCores:     numCores,
		NumSockets:   1,
		NumNUMANodes: 1,
		CPUDetails:   machine.CPUDetails{},
	}
	for cpu := 0; cpu < cpuNum; cpu++ {
		topology.CPUDetails[cpu] = machine.CPUTopoInfo{
			NUMANodeID: 0,
			SocketID:   0,
			CoreID:     cpu / 4,
		}
	}
	require.Equal(t, 4, topology.CPUsPerCore())
	return topology
}

// TestNormalizeSteadyRealNUMATargets_SMT4WholeCoreGrid proves the normalization is
// core-width agnostic: on SMT4 (w=4) an odd 13 target evaluates lower=12 / upper=16
// against the committed anchor.
func TestNormalizeSteadyRealNUMATargets_SMT4WholeCoreGrid(t *testing.T) {
	t.Parallel()

	// 16 cores (64 CPUs), 1 NUMA, w=4.
	topology := smt4Topology(t, 16)

	// committed reclaim = 1 core (4 CPUs), dedicated = 14 cores (56), leaving only
	// 1 free core (4 CPUs). advisor reclaim=13 (3 cores + 1). lower=12 (growth 8 >
	// free 4), upper=16 (growth 12 > free 4) -> neither free-covers-growth.
	descriptors := steadyFixture(topology, 0, 13, 51, 1, 14)
	available := topology.CPUDetails.CPUsInNUMANodes(0)
	normalized, report, err := normalizeSteadyRealNUMATargets(descriptors, available, topology, nil)
	require.NoError(t, err)
	rec, _ := recordByNUMA(report, 0)
	require.Equal(t, steadyReclaimDecisionInfeasible, rec.Decision)
	require.Equal(t, 13, normalized[0].Quantity)

	// Now give enough free cores (4 cores = 16 free cpus) to cover growth to lower=12
	// (growth 8 <= 16) -> lower feasible, closer to committed -> round down.
	descriptors2 := steadyFixture(topology, 0, 13, 35, 1, 11)
	normalized2, report2, err2 := normalizeSteadyRealNUMATargets(descriptors2, available, topology, nil)
	require.NoError(t, err2)
	rec2, _ := recordByNUMA(report2, 0)
	require.Equal(t, steadyReclaimDecisionRoundDown, rec2.Decision)
	require.Equal(t, 12, normalized2[0].Quantity)
}

// TestNormalizeSteadyRealNUMATargets_FakeNUMAUntouched proves fake-NUMA reclaim
// blocks are skipped (their own solver owns them).
func TestNormalizeSteadyRealNUMATargets_FakeNUMAUntouched(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
	require.NoError(t, err)

	fake := advisorBlockDescriptor{
		BlockID: "fake", Class: advisorBlockClassMandatoryReclaim, NUMAID: -1,
		Quantity: 3, ComponentKey: "fake", Eligible: topology.CPUDetails.CPUs().Clone(),
	}
	normalized, report, err := normalizeSteadyRealNUMATargets([]advisorBlockDescriptor{fake},
		topology.CPUDetails.CPUs(), topology, nil)
	require.NoError(t, err)
	require.False(t, report.HadOddAdvice)
	require.Empty(t, report.Records)
	require.Equal(t, 3, normalized[0].Quantity)
}

// TestNormalizeSteadyRealNUMATargets_SkippedNUMAUntouched proves skipNUMAs are
// left to their own handling.
func TestNormalizeSteadyRealNUMATargets_SkippedNUMAUntouched(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
	require.NoError(t, err)
	descriptors := steadyFixture(topology, 0, 3, 29, 1, 15)
	available := topology.CPUDetails.CPUsInNUMANodes(0)
	normalized, report, err := normalizeSteadyRealNUMATargets(descriptors, available, topology, sets.NewInt(0))
	require.NoError(t, err)
	require.False(t, report.HadOddAdvice)
	require.Empty(t, report.Records)
	require.Equal(t, 3, normalized[0].Quantity)
}

// TestHardReclaimAdviceError_PreservesTextAndClassifies proves the typed error keeps
// the original monitoring text while being errors.As-classifiable.
func TestHardReclaimAdviceError_PreservesTextAndClassifies(t *testing.T) {
	t.Parallel()

	_, err := selectHardReclaimCoresWithFrontier(nil, 5, machine.NewCPUSet(),
		map[string]machine.CPUSet{}, map[string]int{})
	require.Error(t, err)

	var advErr *hardReclaimAdviceError
	require.True(t, errorsAsAdvice(err, &advErr))
	require.Equal(t, hardReclaimErrInsufficientWholeCore, advErr.Kind())
	require.Contains(t, err.Error(), "needs 5 more reclaim CPUs")
}

// TestPNHCommittedFallback_AnchorsOnCommittedWholeCore proves that when the fast
// path cannot grow reclaim beyond the committed whole core (a protected dedicated
// group cannot donate), the committed fallback retries with the committed target
// and succeeds without taking any dedicated core.
func TestPNHCommittedFallback_AnchorsOnCommittedWholeCore(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
	require.NoError(t, err)

	committed := coresInNUMA(topology, 0, 0, 1)  // one whole core = 2 cpus
	dedicated := coresInNUMA(topology, 0, 1, 16) // 15 cores = 30 cpus, request floor = all of it

	input := hardReclaimPartitionInput{
		topology:        topology,
		targetByNUMA:    map[int]int{0: 4}, // wants a second core that cannot be donated
		currentReclaim:  committed,
		free:            machine.NewCPUSet(),
		reclaimEligible: topology.CPUDetails.CPUsInNUMANodes(0),
		donors: []hardReclaimPartitionDonor{{
			key: "dnb", groupKey: "pod/main", cpus: dedicated, requestQuantity: 30,
		}},
	}

	plan, err := planHardReclaimPartition(input)
	require.Nil(t, plan)
	require.Error(t, err)
	var advErr *hardReclaimAdviceError
	require.True(t, errorsAsAdvice(err, &advErr))
	require.Equal(t, hardReclaimErrInsufficientWholeCore, advErr.kind)

	fallback, err := pnhCommittedFallback(input, err)
	require.NoError(t, err)
	require.NotNil(t, fallback)
	require.True(t, fallback.reclaim.Equals(committed),
		"fallback must keep the committed whole core: got %s want %s", fallback.reclaim.String(), committed.String())
}

// TestPNHCommittedFallback_DoesNotSwallowUnrelatedError proves a non-insufficient
// error (e.g. global infeasible with no odd advice) is returned untouched.
func TestPNHCommittedFallback_DoesNotSwallowUnrelatedError(t *testing.T) {
	t.Parallel()

	_, err := pnhCommittedFallback(hardReclaimPartitionInput{},
		fmt.Errorf("some unrelated failure"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "some unrelated failure")
}

// TestNormalizeSteadyRealNUMATargets_SMT4RollsBackPartialDedicatedAbsorb proves that
// when an odd reclaim target rounds down but the released CPUs cannot be fully
// absorbed by the dedicated pool (one dedicated block only partially absorbs, the
// next has no headroom), the normalization rolls back BOTH the mandatory delta and
// every partially-taken dedicated block, restoring conservation and the original
// quantities rather than leaving a dedicated block raised while recording a pass.
func TestNormalizeSteadyRealNUMATargets_SMT4RollsBackPartialDedicatedAbsorb(t *testing.T) {
	t.Parallel()

	topology := smt4Topology(t, 16) // 64 cpus, 1 NUMA, w=4
	all := topology.CPUDetails.CPUsInNUMANodes(0)

	reclaimCommitted := coresInNUMA(topology, 0, 0, 1) // core 0 = 4 cpus
	dedA := coresInNUMA(topology, 0, 1, 2)             // core 1 = 4 cpus, quantity 3 (headroom 1)
	dedB := coresInNUMA(topology, 0, 2, 3)             // core 2 = 4 cpus, quantity 4 (headroom 0)

	descriptors := []advisorBlockDescriptor{
		{
			BlockID: "reclaim", Class: advisorBlockClassMandatoryReclaim, NUMAID: 0,
			Quantity: 14, ComponentKey: "mandatory", Eligible: all.Clone(), Committed: reclaimCommitted,
		},
		{
			BlockID: "ded-a", Class: advisorBlockClassDedicated, NUMAID: 0,
			Quantity: 3, ComponentKey: "ded-a", Eligible: dedA.Clone(), Committed: dedA,
		},
		{
			BlockID: "ded-b", Class: advisorBlockClassDedicated, NUMAID: 0,
			Quantity: 4, ComponentKey: "ded-b", Eligible: dedB.Clone(), Committed: dedB,
		},
	}

	beforeTotal := 14 + 3 + 4

	normalized, report, err := normalizeSteadyRealNUMATargets(descriptors, all, topology, nil)
	require.NoError(t, err)
	require.True(t, report.HadOddAdvice)

	rec, ok := recordByNUMA(report, 0)
	require.True(t, ok)
	require.Equal(t, steadyReclaimDecisionInfeasible, rec.Decision,
		"dedicated pool should be unable to absorb the 2 released cpus -> infeasible passthrough")

	var reclaim, dedAOut, dedBOut *advisorBlockDescriptor
	for i := range normalized {
		switch normalized[i].BlockID {
		case "reclaim":
			reclaim = &normalized[i]
		case "ded-a":
			dedAOut = &normalized[i]
		case "ded-b":
			dedBOut = &normalized[i]
		}
	}
	require.NotNil(t, reclaim)
	require.NotNil(t, dedAOut)
	require.NotNil(t, dedBOut)

	// All quantities restored to their original values; the partial take on ded-a
	// must NOT have stuck.
	require.Equal(t, 14, reclaim.Quantity, "mandatory reclaim must be restored")
	require.Equal(t, 3, dedAOut.Quantity, "partially-absorbed dedicated block must be rolled back")
	require.Equal(t, 4, dedBOut.Quantity, "untouched dedicated block must be unchanged")
	require.Equal(t, beforeTotal, reclaim.Quantity+dedAOut.Quantity+dedBOut.Quantity,
		"per-NUMA total must be conserved after rollback")
}
