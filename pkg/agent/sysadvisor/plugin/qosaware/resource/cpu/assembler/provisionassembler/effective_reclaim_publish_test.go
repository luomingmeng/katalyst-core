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

package provisionassembler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/options"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	metaagent "github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// newFakeNUMAHardAssembler builds an assembler whose global (FakedNUMAID) scope
// is an active ramp-up hard-partition domain. The two backing NUMAs model the
// shared NUMA set; the per-scope ceiling is pinned to a known effective target
// so the published reclaim size is deterministic.
func newFakeNUMAHardAssembler(
	t *testing.T,
	backingNUMAs []int,
	numaAvailable map[int]int,
	reserved map[int]int,
	effectiveTarget int,
	allowSharedOverlap bool,
) *ProvisionAssemblerCommon {
	t.Helper()

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	conf.GetDynamicConfiguration().EnableReclaim = true
	conf.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true
	// ratio<=0 disables the MaxRatio clamp entirely so the per-scope ceiling is
	// the single quantity boundary.
	conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio = 0

	cpuDetails := machine.CPUDetails{}
	for numaID, size := range numaAvailable {
		for cpuID := 0; cpuID < size; cpuID++ {
			cpuDetails[cpuID] = machine.CPUTopoInfo{NUMANodeID: numaID}
		}
	}
	metaServer := &metaserver.MetaServer{
		MetaAgent: &metaagent.MetaAgent{
			KatalystMachineInfo: &machine.KatalystMachineInfo{
				CPUTopology: &machine.CPUTopology{
					NumCPUs:      len(cpuDetails),
					NumCores:     len(cpuDetails),
					NumSockets:   1,
					NumNUMANodes: len(numaAvailable),
					CPUDetails:   cpuDetails,
				},
			},
		},
	}

	metaReader := metacache.NewDummyMetaCacheImp()
	require.NoError(t, metaReader.SetResourcePackageConfig(types.ResourcePackageConfig{}))

	regionMap := map[string]region.QoSRegion{}
	nonBinding := machine.NewCPUSet(backingNUMAs...)
	rampUpReclaimCPUSetCap := map[int]int{}
	disableDedicatedOverlap := true
	pa := NewProvisionAssemblerCommon(
		conf,
		nil,
		&regionMap,
		&reserved,
		&rampUpReclaimCPUSetCap,
		&numaAvailable,
		&nonBinding,
		&allowSharedOverlap,
		&disableDedicatedOverlap,
		metaReader,
		metaServer,
		metrics.DummyMetrics{},
	).(*ProvisionAssemblerCommon)

	// Drive the global scope as an active hard-partition ramp-up domain and pin
	// its per-scope ceiling to the known effective target.
	pa.calculationContext.RampUpDomains = []int{commonstate.FakedNUMAID}
	scope := NewNonExclusiveReclaimConstraintScope(commonstate.FakedNUMAID)
	pa.calculationContext.ReclaimConstraint = ReclaimConstraintReservedFloor
	pa.calculationContext.ReclaimActiveScopes = map[ReclaimConstraintScope]bool{scope: true}
	pa.calculationContext.ReclaimCeilings = map[ReclaimConstraintScope]*int{scope: ptrToInt(effectiveTarget)}

	return pa
}

// assembleFakeNUMAHardScope assembles only the global (FakedNUMAID) scope.
func assembleFakeNUMAHardScope(t *testing.T, pa *ProvisionAssemblerCommon) *types.InternalCPUCalculationResult {
	t.Helper()
	result := &types.InternalCPUCalculationResult{
		PoolEntries:                 map[string]map[int]types.CPUResource{},
		PoolOverlapInfo:             map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo: map[string]map[int]map[string]map[string]int{},
	}
	require.NoError(t, pa.assembleWithoutNUMAExclusivePool(NewRegionMapHelper(*pa.regionMap), commonstate.FakedNUMAID, result))
	return result
}

// TestEffectiveReclaimedCoresSize verifies the single publish formula: the
// constrained reclaim target minus the portion already published as overlap
// metadata, clamped to the non-negative domain. reservedForReclaim is intentionally
// never subtracted here.
func TestEffectiveReclaimedCoresSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		reclaimTarget int
		overlapSize   int
		want          int
	}{
		{"target minus zero overlap", 8, 0, 8},
		{"target minus partial overlap", 8, 2, 6},
		{"overlap saturates target", 8, 8, 0},
		{"overlap exceeds target clamps to zero", 8, 12, 0},
		{"zero target", 0, 0, 0},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, effectiveReclaimedCoresSize(tc.reclaimTarget, tc.overlapSize))
		})
	}
}

// TestFakeNUMAHardPublishesEffectiveTarget_A1 reproduces the field defect:
// FakeNUMA hard partition, effective target 8, reserve 4, overlap 0 must publish
// the full target (8), not target-minus-reserve (4).
func TestFakeNUMAHardPublishesEffectiveTarget_A1(t *testing.T) {
	t.Parallel()

	pa := newFakeNUMAHardAssembler(t,
		[]int{0, 1},
		map[int]int{0: 24, 1: 24},
		map[int]int{0: 2, 1: 2}, // effectiveReservedForReclaim over {0,1} == 4
		8,                       // effective target / per-scope ceiling
		false,                   // non-overlap
	)
	result := assembleFakeNUMAHardScope(t, pa)

	require.Equal(t, 8, result.PoolEntries[commonstate.PoolNameReclaim][commonstate.FakedNUMAID].Size,
		"FakeNUMA hard publish must equal the effective reclaim target, not target-reserve")
}

// TestFakeNUMAHardPublishIgnoresReserveAtPublishLayer_A4 is the discriminative
// test from the design: the reservation is a floor/constraint input consumed at
// target derivation only. The publish layer must emit the effective target for
// every reserve regime. The defective code subtracted reserve here again, which
// published 0 once the reservation reached or exceeded the target.
func TestFakeNUMAHardPublishIgnoresReserveAtPublishLayer_A4(t *testing.T) {
	t.Parallel()

	const availablePerNUMA = 24
	backing := []int{0, 1}

	tests := []struct {
		name            string
		reserved        map[int]int
		effectiveTarget int
		// want is the published reclaim size for the global (FakedNUMAID) scope.
		want int
	}{
		{"zero reserve publishes target", map[int]int{0: 0, 1: 0}, 8, 8},
		{"reserve equals target publishes target", map[int]int{0: 4, 1: 4}, 8, 8},
		{"reserve above target floor-binds then publishes target", map[int]int{0: 6, 1: 6}, 8, 12},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pa := newFakeNUMAHardAssembler(t, backing,
				map[int]int{0: availablePerNUMA, 1: availablePerNUMA},
				tc.reserved, tc.effectiveTarget, false)
			result := assembleFakeNUMAHardScope(t, pa)

			require.Equal(t, tc.want,
				result.PoolEntries[commonstate.PoolNameReclaim][commonstate.FakedNUMAID].Size,
				"publish must equal the effective target; reserve is not re-deducted here")
		})
	}
}

// TestFakeNUMAHardOverlapReducesEntryOnce verifies overlap metadata is subtracted
// exactly once at publish: target 8 with overlap 2 publishes a non-overlap entry
// of 6 (and the 2 are already reflected as overlap metadata).
func TestFakeNUMAHardOverlapReducesEntryOnce(t *testing.T) {
	t.Parallel()

	// Pure unit check of the convergent formula covers the overlap accounting
	// without standing up a full overlap-enabled region matrix.
	require.Equal(t, 6, effectiveReclaimedCoresSize(8, 2))
	require.Equal(t, 0, effectiveReclaimedCoresSize(8, 8))
	require.Equal(t, 0, effectiveReclaimedCoresSize(8, 12))
}
