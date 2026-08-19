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

package provisionassembler

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	qrmstate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

type ProvisionAssemblerCommon struct {
	conf                                       *config.Configuration
	regionMap                                  *map[string]region.QoSRegion
	reservedForReclaim                         *map[int]int
	numaAvailable                              *map[int]int
	nonBindingNumas                            *machine.CPUSet
	allowSharedCoresOverlapReclaimedCores      *bool
	disableDedicatedCoresOverlapReclaimedCores *bool

	metaReader metacache.MetaReader
	metaServer *metaserver.MetaServer
	emitter    metrics.MetricEmitter
}

func NewProvisionAssemblerCommon(conf *config.Configuration, _ interface{}, regionMap *map[string]region.QoSRegion,
	reservedForReclaim *map[int]int, numaAvailable *map[int]int, nonBindingNumas *machine.CPUSet,
	allowSharedCoresOverlapReclaimedCores *bool, disableDedicatedCoresOverlapReclaimedCores *bool,
	metaReader metacache.MetaReader, metaServer *metaserver.MetaServer, emitter metrics.MetricEmitter,
) ProvisionAssembler {
	return &ProvisionAssemblerCommon{
		conf:                                  conf,
		regionMap:                             regionMap,
		reservedForReclaim:                    reservedForReclaim,
		numaAvailable:                         numaAvailable,
		nonBindingNumas:                       nonBindingNumas,
		allowSharedCoresOverlapReclaimedCores: allowSharedCoresOverlapReclaimedCores,
		disableDedicatedCoresOverlapReclaimedCores: disableDedicatedCoresOverlapReclaimedCores,

		metaReader: metaReader,
		metaServer: metaServer,
		emitter:    emitter,
	}
}

// cpuCountInNUMAs returns the total number of CPUs within the given NUMA set.
// It returns 0 when metaServer or its CPUDetails is not ready, so callers can
// skip clamping instead of mistakenly capping reclaim to 0.
func (pa *ProvisionAssemblerCommon) cpuCountInNUMAs(numas machine.CPUSet) int {
	if pa.metaServer == nil || pa.metaServer.CPUDetails == nil {
		return 0
	}
	return pa.metaServer.CPUDetails.CPUsInNUMANodes(numas.ToSliceInt()...).Size()
}

func (pa *ProvisionAssemblerCommon) reclaimRatioCPUsPerCore() (int, error) {
	dynamicConf := pa.conf.GetDynamicConfiguration()
	if !dynamicConf.EnableReclaim || !dynamicConf.EnableRampUpReclaimHardPartition {
		return 1, nil
	}
	if pa.metaServer == nil || pa.metaServer.CPUTopology == nil {
		return 0, fmt.Errorf("cpu topology is unavailable for hard reclaim ratio cap")
	}
	cpusPerCore := pa.metaServer.CPUTopology.CPUsPerCore()
	if cpusPerCore <= 0 {
		return 0, fmt.Errorf("invalid cpus per core %d for hard reclaim ratio cap", cpusPerCore)
	}
	return cpusPerCore, nil
}

// clampByReclaimedCPUMaxRatio caps the reclaim pool size (and quota when it is
// not the -1 sentinel) by ratio*cpuCount. When ratio<=0 it disables clamping and
// returns the inputs unchanged; a negative limit (e.g. the -1 "no quota limit"
// sentinel) is preserved as-is.
//
// In legacy mode the cap is rounded down to an even logical CPU count. In hard
// partition mode the ratio is applied to physical cores, rounded down to an
// even core count, and converted back to logical CPUs. The reclaim reservation
// remains a floor in both modes.
func clampByReclaimedCPUMaxRatio(
	size int,
	limit float64,
	ratio float64,
	cpuCount int,
	reservedForReclaim int,
	cpusPerCore int,
) (int, float64, error) {
	if ratio <= 0 {
		return size, limit, nil
	}
	capCPUs, err := calculateReclaimRatioCap(cpuCount, ratio, reservedForReclaim, cpusPerCore)
	if err != nil {
		return 0, 0, err
	}
	if size > capCPUs {
		size = capCPUs
	}
	if limit >= 0 && limit > float64(capCPUs) {
		limit = float64(capCPUs)
	}
	return size, limit, nil
}

func calculateReclaimRatioCap(cpuCount int, ratio float64, reservedForReclaim, cpusPerCore int) (int, error) {
	if cpusPerCore <= 1 {
		capCPUs := int(math.Floor(ratio * float64(cpuCount)))
		capCPUs -= capCPUs % 2
		return general.Max(capCPUs, reservedForReclaim), nil
	}
	return machine.CalculatePerNUMAHardReclaimTarget(cpuCount, ratio, 0, reservedForReclaim, cpusPerCore)
}

// reclaimClampResult carries the structured diagnostics of a single reclaim
// max-ratio clamp: the raw size before clamping, the final size and limit after
// clamping, and the amount of reclaim cores released by the clamp. It exposes
// the released residual so callers can later backfill the default share pool.
type reclaimClampResult struct {
	RawSize      int
	FinalSize    int
	ReleasedSize int
	FinalLimit   float64
}

// clampByReclaimedCPUMaxRatioWithDiagnostics wraps clampByReclaimedCPUMaxRatio
// without changing its clamp semantics; it only augments the return value with
// diagnostics (raw size and released cores). It does not write into
// InternalCPUCalculationResult and does not perform any accumulation.
func clampByReclaimedCPUMaxRatioWithDiagnostics(size int, limit float64, ratio float64,
	cpuCount int, reservedForReclaim int, cpusPerCore int,
) (reclaimClampResult, error) {
	finalSize, finalLimit, err := clampByReclaimedCPUMaxRatio(
		size, limit, ratio, cpuCount, reservedForReclaim, cpusPerCore)
	if err != nil {
		return reclaimClampResult{}, err
	}
	return reclaimClampResult{
		RawSize:      size,
		FinalSize:    finalSize,
		ReleasedSize: general.Max(0, size-finalSize),
		FinalLimit:   finalLimit,
	}, nil
}

// defaultShareNUMABudget captures the per-NUMA canonical quantity budget used to
// compute the default share pool residual. All fields are expressed in unpinned
// CPU quantities (i.e. after excluding pinned resource-package CPUs).
type defaultShareNUMABudget struct {
	// UnpinnedAllocatableSize is numaAvailable[numaID] - pinnedCPUSizeInNUMA;
	// numaAvailable already excludes reserve and forbidden/system pools.
	UnpinnedAllocatableSize int
	// FinalUnpinnedReclaimSize is the post-clamp reclaim quantity that lives in
	// the unpinned eligibility domain of this NUMA.
	FinalUnpinnedReclaimSize int
	// FixedUnpinnedPoolSize is the sum of non-default, non-reclaim, non-reserve,
	// non-pinned, non-exclusive fixed pool quantities on this NUMA. The sum may
	// exceed the remaining capacity because QRM proportionally shrinks or
	// overlaps fixed pools when their requested quantities cannot fit.
	FixedUnpinnedPoolSize int
	// Exclusive marks a NUMA that is owned by a NUMA-exclusive region; such a
	// NUMA contributes zero default share residual and its nested
	// reclaim/pinned/dedicated quantities are not deducted again.
	Exclusive bool
}

// defaultShareBudgetSummary carries classified quantities purely for diagnostics
// and metrics. It never participates in the residual computation.
type defaultShareBudgetSummary struct {
	AllocatableSize     int
	FixedCommonPoolSize int
	ReserveSize         int
	DedicatedSize       int
	IsolationSize       int
	CustomSharedSize    int
	SNBSize             int
	PinnedCPUSize       int
	ExclusiveNUMASize   int
}

// calculateDefaultShareTargetSize computes the default share pool target size as
// the sum over non-exclusive NUMAs of (unpinned allocatable - final unpinned
// reclaim - materialized fixed unpinned pools). Exclusive NUMAs contribute
// zero. Reclaim exceeding allocatable capacity is an invariant violation.
// Fixed-pool quantity overcommit instead saturates the residual at zero because
// QRM proportionally shrinks or overlaps those pools within the same capacity.
func calculateDefaultShareTargetSize(budgetByNUMA map[int]defaultShareNUMABudget) (int, error) {
	target := 0
	for numaID, budget := range budgetByNUMA {
		if budget.Exclusive {
			continue
		}
		numaTarget := budget.UnpinnedAllocatableSize - budget.FinalUnpinnedReclaimSize
		if numaTarget < 0 {
			return 0, fmt.Errorf("default share reclaim exceeds unpinned allocatable in numa %d: unpinned=%d reclaim=%d",
				numaID, budget.UnpinnedAllocatableSize, budget.FinalUnpinnedReclaimSize)
		}
		if budget.FixedUnpinnedPoolSize >= numaTarget {
			numaTarget = 0
		} else {
			numaTarget -= budget.FixedUnpinnedPoolSize
		}
		target += numaTarget
	}
	return target, nil
}

// buildDefaultShareBudget collects the canonical per-NUMA quantity budget that
// drives the default share pool residual backfill. It reads only the already
// assembled result.PoolEntries plus the region topology; it never mutates the
// result.
//
// Accounting model (see rule set in the design doc):
//   - Each real NUMA yields UnpinnedAllocatableSize = numaAvailable[numaID] -
//     pinnedCPUSizeInNUMA. numaAvailable already excludes reserve and
//     forbidden/system pools, so only resource-package pinned CPUs are removed
//     here.
//   - A NUMA owned by a NUMA-exclusive dedicated region is marked Exclusive and
//     contributes zero residual; its nested reclaim/pinned/fixed quantities are
//     NOT deducted again to avoid double counting.
//   - Non-binding NUMAs are folded into a single combined bucket keyed by
//     FakedNUMAID, because the default share pool and its sibling non-binding
//     pools live at FakedNUMAID and collectively span the non-binding NUMAs.
//   - FinalUnpinnedReclaimSize only counts the unpinned reclaim entries (the
//     reclaim entry written per scope); resource-package reclaim is already
//     excluded through the pinned budget.
//   - FixedUnpinnedPoolSize only counts non-default, non-reclaim, non-reserve,
//     non-pinned, non-exclusive pool quantities.
//
// Assumption: dedicated pool entries are keyed by pod UID (not by a name that
// GetPoolType can classify), so we identify them authoritatively via the
// dedicated regions in regionHelper. Pinned (resource-package) pools are
// identified by the package prefix in their wrapped owner pool name.
func (pa *ProvisionAssemblerCommon) buildDefaultShareBudget(
	regionHelper *RegionMapHelper,
	result *types.InternalCPUCalculationResult,
) (map[int]defaultShareNUMABudget, defaultShareBudgetSummary, error) {
	var summary defaultShareBudgetSummary

	numaAvailable := *pa.numaAvailable
	nonBinding := *pa.nonBindingNumas
	cfg := pa.metaReader.GetResourcePackageConfig()

	// collect exclusive NUMAs and map dedicated pod UIDs back to their regions.
	// Dedicated pool entries are keyed by pod UID, while the region retains both
	// the physical pool identity and its resource-package owner.
	exclusiveNUMAs := sets.NewInt()
	dedicatedRegionByPodUID := make(map[string]region.QoSRegion)
	recordDedicatedRegion := func(podUID string, r region.QoSRegion) error {
		if existing := dedicatedRegionByPodUID[podUID]; existing != nil && existing.Name() != r.Name() {
			regionNames := []string{existing.Name(), r.Name()}
			sort.Strings(regionNames)
			return fmt.Errorf("pod uid %q maps to multiple dedicated regions %q and %q",
				podUID, regionNames[0], regionNames[1])
		}
		dedicatedRegionByPodUID[podUID] = r
		return nil
	}
	for numaID := range numaAvailable {
		for _, r := range regionHelper.GetRegions(numaID, configapi.QoSRegionTypeDedicated) {
			if r.IsNumaBinding() && r.IsNumaExclusive() {
				for _, bindingNUMA := range r.GetBindingNumas().ToSliceInt() {
					exclusiveNUMAs.Insert(bindingNUMA)
				}
			}
			for podUID := range r.GetPods() {
				if err := recordDedicatedRegion(podUID, r); err != nil {
					return nil, summary, err
				}
			}
		}
	}
	// non-binding dedicated regions live at FakedNUMAID scope.
	for _, r := range regionHelper.GetRegions(commonstate.FakedNUMAID, configapi.QoSRegionTypeDedicated) {
		for podUID := range r.GetPods() {
			if err := recordDedicatedRegion(podUID, r); err != nil {
				return nil, summary, err
			}
		}
	}

	// effectiveBucket folds non-binding real NUMAs into the FakedNUMAID bucket.
	effectiveBucket := func(numaID int) int {
		if numaID == commonstate.FakedNUMAID || nonBinding.Contains(numaID) {
			return commonstate.FakedNUMAID
		}
		return numaID
	}

	// seed per-NUMA allocatable budgets.
	budgetByNUMA := make(map[int]defaultShareNUMABudget)
	pinnedCPUSizeByPackageByBucket := make(map[int]map[string]int)
	combined := defaultShareNUMABudget{}
	hasCombined := false
	for numaID := range numaAvailable {
		pinnedCPUSizeByPackage := pa.getPinnedCPUSizeByPackage(machine.NewCPUSet(numaID), cfg)
		bucket := effectiveBucket(numaID)
		if pinnedCPUSizeByPackageByBucket[bucket] == nil {
			pinnedCPUSizeByPackageByBucket[bucket] = make(map[string]int)
		}
		for pkgName, size := range pinnedCPUSizeByPackage {
			pinnedCPUSizeByPackageByBucket[bucket][pkgName] += size
		}
		pinned := general.SumUpMapValues(pinnedCPUSizeByPackage)
		alloc := numaAvailable[numaID] - pinned
		summary.PinnedCPUSize += pinned
		summary.AllocatableSize += alloc

		if exclusiveNUMAs.Has(numaID) {
			budgetByNUMA[numaID] = defaultShareNUMABudget{UnpinnedAllocatableSize: alloc, Exclusive: true}
			summary.ExclusiveNUMASize += alloc
			continue
		}
		if nonBinding.Contains(numaID) {
			combined.UnpinnedAllocatableSize += alloc
			hasCombined = true
		} else {
			budgetByNUMA[numaID] = defaultShareNUMABudget{UnpinnedAllocatableSize: alloc}
		}
	}

	// accumulate reclaim and fixed pool quantities per effective bucket.
	fixedByBucket := make(map[int]int)
	reclaimByBucket := make(map[int]int)
	countedDedicatedRegionsByBucket := make(map[int]sets.String)
	for poolName, byNUMA := range result.PoolEntries {
		if poolName == commonstate.PoolNameReserve {
			for _, res := range byNUMA {
				summary.ReserveSize += res.Size
			}
			continue
		}
		ownerPoolName := poolName
		dedicatedRegion := dedicatedRegionByPodUID[poolName]
		if dedicatedRegion != nil {
			ownerPoolName = dedicatedRegion.OwnerPoolName()
		}
		_, pkgName := resourcepackage.UnwrapOwnerPoolName(ownerPoolName)
		for numaID, res := range byNUMA {
			// nested quantities inside exclusive NUMAs are ignored.
			if exclusiveNUMAs.Has(numaID) {
				continue
			}
			bucket := effectiveBucket(numaID)
			if poolName == commonstate.PoolNameReclaim {
				reclaimByBucket[bucket] += res.Size
				if bucket == commonstate.FakedNUMAID {
					hasCombined = true
				}
				continue
			}
			// the default share pool is exactly what we are computing; skip it.
			//
			// Precondition: the default share pool only ever exists at
			// FakedNUMAID, while share NUMA-binding (SNB) pools carry a
			// "-NUMA" suffix and live on real numaIDs. If upstream ever emits a
			// plain PoolNameShare entry on a real numaID, it would fall through
			// to the fixed-pool branch below and be counted into
			// FixedUnpinnedPoolSize, inflating the fixed budget and lowering the
			// computed target. This is a known precondition/constraint rather
			// than a case handled here.
			if numaID == commonstate.FakedNUMAID && poolName == commonstate.PoolNameShare {
				continue
			}
			if dedicatedRegion != nil {
				if countedDedicatedRegionsByBucket[bucket] == nil {
					countedDedicatedRegionsByBucket[bucket] = sets.NewString()
				}
				if countedDedicatedRegionsByBucket[bucket].Has(dedicatedRegion.Name()) {
					continue
				}
				countedDedicatedRegionsByBucket[bucket].Insert(dedicatedRegion.Name())
			}
			fixedSize := res.Size
			if pkgName != "" {
				pinnedSizeInBucket := pinnedCPUSizeByPackageByBucket[bucket][pkgName]
				if dedicatedRegion != nil {
					fixedSize = general.Max(res.Size-pinnedSizeInBucket, 0)
					if fixedSize == 0 {
						continue
					}
				} else if pinnedSizeInBucket > 0 {
					// Non-dedicated resource-package pools are already excluded
					// via the pinned budget deducted from UnpinnedAllocatableSize.
					continue
				}
			}
			// classify the fixed unpinned pool for diagnostics only.
			//
			// This classification (in particular the IsShareNUMABindingPool /
			// "-NUMA" suffix check for SNB) feeds only summary/metrics and does
			// not participate in the target computation. A custom shared pool
			// whose name happens to contain "-NUMA" may be misclassified as SNB
			// here; that is an accepted metrics-bucketing approximation and has
			// no effect on the residual result.
			switch {
			case commonstate.IsIsolationPool(poolName):
				summary.IsolationSize += fixedSize
			case dedicatedRegion != nil:
				summary.DedicatedSize += fixedSize
			case commonstate.IsShareNUMABindingPool(poolName):
				summary.SNBSize += fixedSize
			default:
				summary.CustomSharedSize += fixedSize
			}
			fixedByBucket[bucket] += fixedSize
			if bucket == commonstate.FakedNUMAID {
				hasCombined = true
			}
		}
	}

	// attach the combined non-binding bucket.
	//
	// Precondition on upstream assemble behavior: when there are no non-binding
	// NUMAs, the FakedNUMAID scope is expected to carry neither available
	// capacity (combined.UnpinnedAllocatableSize stays 0) nor reclaim/fixed
	// entries. If that assumption is violated (e.g. a reclaim entry exists at
	// FakedNUMAID while nonBinding is empty), the combined bucket ends up with
	// alloc=0 but reclaim>0, so calculateDefaultShareTargetSize reports reclaim
	// exceeding allocatable and the whole provision fails. This fail-closed outcome
	// is intentional: it surfaces the upstream inconsistency instead of silently
	// producing an incorrect default share target.
	if hasCombined {
		combined.FixedUnpinnedPoolSize = fixedByBucket[commonstate.FakedNUMAID]
		combined.FinalUnpinnedReclaimSize = reclaimByBucket[commonstate.FakedNUMAID]
		budgetByNUMA[commonstate.FakedNUMAID] = combined
	}
	// fill fixed/reclaim into the binding real-NUMA budgets.
	for numaID, budget := range budgetByNUMA {
		if numaID == commonstate.FakedNUMAID || budget.Exclusive {
			continue
		}
		budget.FixedUnpinnedPoolSize = fixedByBucket[numaID]
		budget.FinalUnpinnedReclaimSize = reclaimByBucket[numaID]
		budgetByNUMA[numaID] = budget
	}

	summary.FixedCommonPoolSize = 0
	for _, size := range fixedByBucket {
		summary.FixedCommonPoolSize += size
	}

	return budgetByNUMA, summary, nil
}

// finalizeDefaultShareBackfill overrides the default share pool quantity with the
// canonical residual budget once every scope has been assembled. It is a no-op
// when the backfill feature is disabled. On success it also records the
// structured diagnostics for metrics.
func (pa *ProvisionAssemblerCommon) finalizeDefaultShareBackfill(
	regionHelper *RegionMapHelper,
	result *types.InternalCPUCalculationResult,
) error {
	if !result.DefaultShareBackfill.Enabled {
		return nil
	}
	budgetByNUMA, summary, err := pa.buildDefaultShareBudget(regionHelper, result)
	if err != nil {
		return err
	}
	target, err := calculateDefaultShareTargetSize(budgetByNUMA)
	if err != nil {
		return err
	}
	if target == 0 {
		return fmt.Errorf("default share target is zero before sysadvisor publish")
	}
	before := 0
	if byNUMA := result.PoolEntries[commonstate.PoolNameShare]; byNUMA != nil {
		before = byNUMA[commonstate.FakedNUMAID].Size
	}
	result.SetPoolEntry(commonstate.PoolNameShare, commonstate.FakedNUMAID, target, -1)
	result.DefaultShareBackfill.AllocatableBudget = summary.AllocatableSize
	result.DefaultShareBackfill.FixedPoolSize = summary.FixedCommonPoolSize
	result.DefaultShareBackfill.ReserveSize = summary.ReserveSize
	result.DefaultShareBackfill.DedicatedSize = summary.DedicatedSize
	result.DefaultShareBackfill.IsolationSize = summary.IsolationSize
	result.DefaultShareBackfill.CustomSharedSize = summary.CustomSharedSize
	result.DefaultShareBackfill.SNBSize = summary.SNBSize
	result.DefaultShareBackfill.PinnedCPUSize = summary.PinnedCPUSize
	result.DefaultShareBackfill.ExclusiveNUMASize = summary.ExclusiveNUMASize
	result.DefaultShareBackfill.DefaultShareBeforeBackfill = before
	result.DefaultShareBackfill.DefaultShareBackfilled = target - before
	result.DefaultShareBackfill.DefaultShareFinal = target
	general.InfoS("default share residual backfill",
		"allocatableBudget", result.DefaultShareBackfill.AllocatableBudget,
		"reserveSize", result.DefaultShareBackfill.ReserveSize,
		"rawReclaimSize", result.DefaultShareBackfill.RawReclaimSize,
		"finalReclaimSize", result.DefaultShareBackfill.FinalReclaimSize,
		"releasedReclaimSize", result.DefaultShareBackfill.ReleasedReclaimSize,
		"fixedPoolSize", result.DefaultShareBackfill.FixedPoolSize,
		"pinnedCPUSize", result.DefaultShareBackfill.PinnedCPUSize,
		"exclusiveNUMASize", result.DefaultShareBackfill.ExclusiveNUMASize,
		"defaultShareBefore", result.DefaultShareBackfill.DefaultShareBeforeBackfill,
		"defaultShareAfter", result.DefaultShareBackfill.DefaultShareFinal,
		"unassignedNonReclaimSize", result.DefaultShareBackfill.UnassignedNonReclaimSize,
	)
	return nil
}

func (pa *ProvisionAssemblerCommon) assembleDedicatedNUMAExclusiveRegion(r region.QoSRegion, result *types.InternalCPUCalculationResult) error {
	if !result.DisableDedicatedCoresOverlapReclaimedCores {
		return pa.assembleLegacyDedicatedNUMAExclusiveRegion(r, result)
	}

	controlKnob, err := r.GetProvision()
	if err != nil {
		return err
	}

	regionNuma := r.GetBindingNumas().ToSliceInt()[0] // always one binding numa for this type of region
	reservedForReclaim := getNUMAsResource(*pa.reservedForReclaim, r.GetBindingNumas())
	available := getNUMAsResource(*pa.numaAvailable, r.GetBindingNumas())
	partitionCapacity, dedicatedCapacity, reclaimCapacity, err := pa.getExclusivePartitionCapacities(r, regionNuma, available)
	if err != nil {
		return err
	}

	ratioPhysicalCap := 0
	if ratio := pa.conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio; ratio > 0 {
		if cpuCount := pa.cpuCountInNUMAs(r.GetBindingNumas()); cpuCount > 0 {
			cpusPerCore, err := pa.reclaimRatioCPUsPerCore()
			if err != nil {
				return err
			}
			ratioPhysicalCap, err = calculateReclaimRatioCap(
				cpuCount, ratio, reservedForReclaim, cpusPerCore)
			if err != nil {
				return fmt.Errorf("calculate reclaim ratio cap: %w", err)
			}
		}
	}

	nonReclaimed := int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirement].Value)
	dedicatedTarget, reclaimTarget, err := calculateExclusiveDisjointTargets(exclusivePartitionInput{
		PartitionCapacity: partitionCapacity,
		DedicatedCapacity: dedicatedCapacity,
		ReclaimCapacity:   reclaimCapacity,
		Reserved:          reservedForReclaim,
		NonReclaimed:      nonReclaimed,
		EnableReclaim:     r.EnableReclaim(),
		RatioPhysicalCap:  ratioPhysicalCap,
	})
	if err != nil {
		return fmt.Errorf("calculate disjoint targets for exclusive region %q: %w", r.Name(), err)
	}

	reclaimQuotaLimit := float64(-1)
	quotaCtrlKnobEnabled, err := metacache.IsQuotaCtrlKnobEnabled(pa.metaReader)
	if err != nil {
		return err
	}
	if quotaCtrlKnobEnabled {
		if quota, ok := controlKnob[configapi.ControlKnobReclaimedCoresCPUQuota]; ok {
			reclaimQuotaLimit = calculateReclaimQuotaLimit(reclaimTarget, quota.Value, ratioPhysicalCap)
		}
	}

	for podUID := range r.GetPods() {
		result.SetPoolEntry(podUID, regionNuma, dedicatedTarget, -1)
	}
	result.SetPoolEntry(commonstate.PoolNameReclaim, regionNuma, reclaimTarget, reclaimQuotaLimit)

	klog.InfoS("assemble disjoint dedicated NUMA-exclusive region",
		"regionName", r.Name(),
		"partitionCapacity", partitionCapacity,
		"dedicatedCapacity", dedicatedCapacity,
		"reclaimCapacity", reclaimCapacity,
		"dedicatedTarget", dedicatedTarget,
		"reclaimTarget", reclaimTarget,
		"reclaimQuotaLimit", reclaimQuotaLimit,
		"reservedForReclaim", reservedForReclaim,
		"ratioPhysicalCap", ratioPhysicalCap)
	return nil
}

func (pa *ProvisionAssemblerCommon) getExclusivePartitionCapacities(
	r region.QoSRegion,
	regionNuma, available int,
) (partition, dedicated, reclaim int, err error) {
	if pa.metaServer == nil || pa.metaServer.CPUDetails == nil {
		return 0, 0, 0, fmt.Errorf("CPU topology is unavailable for NUMA %d", regionNuma)
	}

	availableCPUSet := pa.metaServer.CPUDetails.CPUsInNUMANodes(regionNuma)
	if reservePool, ok := pa.metaReader.GetPoolInfo(commonstate.PoolNameReserve); ok && reservePool != nil {
		availableCPUSet = availableCPUSet.Difference(reservePool.TopologyAwareAssignments[regionNuma])
	}
	pa.metaReader.RangePool(func(poolName string, poolInfo *types.PoolInfo) bool {
		if poolInfo != nil && (qrmstate.ForbiddenPools.Has(poolName) || commonstate.IsSystemPool(poolName)) {
			availableCPUSet = availableCPUSet.Difference(poolInfo.TopologyAwareAssignments[regionNuma])
		}
		return true
	})
	if availableCPUSet.Size() != available {
		return 0, 0, 0, fmt.Errorf(
			"available CPUSet size %d for NUMA %d does not match numaAvailable %d",
			availableCPUSet.Size(), regionNuma, available,
		)
	}

	cfg := pa.metaReader.GetResourcePackageConfig()
	pkgMap := cfg[regionNuma]
	allPinned := machine.NewCPUSet()
	for _, state := range pkgMap {
		if state != nil {
			allPinned = allPinned.Union(state.PinnedCPUSet)
		}
	}

	disableReclaimSelector, err := general.ParseSelector(
		pa.conf.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector,
	)
	if err != nil {
		return 0, 0, 0, err
	}
	nonReclaimablePinned := resourcepackage.GetMatchedPinnedCPUSet(pkgMap, disableReclaimSelector)

	pkgName := r.GetResourcePackageName()
	dedicatedCPUSet := machine.NewCPUSet()
	if pkgName == "" {
		dedicatedCPUSet = availableCPUSet.Difference(allPinned)
	} else if state, ok := pkgMap[pkgName]; ok && state != nil {
		dedicatedCPUSet = availableCPUSet.Intersection(state.PinnedCPUSet)
	}

	reclaimCPUSet := availableCPUSet.Difference(nonReclaimablePinned)
	partitionCPUSet := dedicatedCPUSet.Union(reclaimCPUSet)
	return partitionCPUSet.Size(), dedicatedCPUSet.Size(), reclaimCPUSet.Size(), nil
}

func (pa *ProvisionAssemblerCommon) assembleLegacyDedicatedNUMAExclusiveRegion(r region.QoSRegion, result *types.InternalCPUCalculationResult) error {
	controlKnob, err := r.GetProvision()
	if err != nil {
		return err
	}

	regionNuma := r.GetBindingNumas().ToSliceInt()[0] // always one binding numa for this type of region
	reservedForReclaim := getNUMAsResource(*pa.reservedForReclaim, r.GetBindingNumas())
	available := getNUMAsResource(*pa.numaAvailable, r.GetBindingNumas())
	var reclaimedCoresSize int
	reclaimedCoresLimit := float64(-1)

	// fill in reclaim pool entry for dedicated numa exclusive regions
	nonReclaimRequirement := int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirement].Value)
	if !r.EnableReclaim() {
		nonReclaimRequirement = available
	}

	quotaCtrlKnobEnabled, err := metacache.IsQuotaCtrlKnobEnabled(pa.metaReader)
	if err != nil {
		return err
	}

	if quotaCtrlKnobEnabled {
		reclaimedCoresSize = available
		reclaimedCoresLimit = general.MaxFloat64(float64(reservedForReclaim), float64(available-nonReclaimRequirement))

		if quota, ok := controlKnob[configapi.ControlKnobReclaimedCoresCPUQuota]; ok {
			reclaimedCoresLimit = general.MinFloat64(reclaimedCoresLimit, quota.Value)
		}
	} else {
		reclaimedCoresSize = general.Max(reservedForReclaim, available-nonReclaimRequirement)
	}

	if ratio := pa.conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio; ratio > 0 {
		if cpuCount := pa.cpuCountInNUMAs(r.GetBindingNumas()); cpuCount > 0 {
			cpusPerCore, err := pa.reclaimRatioCPUsPerCore()
			if err != nil {
				return err
			}
			reclaimedCoresSize, reclaimedCoresLimit, err = clampByReclaimedCPUMaxRatio(
				reclaimedCoresSize,
				reclaimedCoresLimit,
				ratio,
				cpuCount,
				reservedForReclaim,
				cpusPerCore,
			)
			if err != nil {
				return fmt.Errorf("clamp reclaim by max ratio: %w", err)
			}
		}
	}

	klog.InfoS("assembleDedicatedNUMAExclusive info", "regionName", r.Name(), "reclaimedCoresSize", reclaimedCoresSize,
		"reclaimedCoresLimit", reclaimedCoresLimit,
		"available", available, "nonReclaimRequirement", nonReclaimRequirement,
		"reservedForReclaim", reservedForReclaim, "controlKnob", controlKnob)

	// set pool overlap info for dedicated pool
	for podUID, containerSet := range r.GetPods() {
		for containerName := range containerSet {
			general.InfoS("set pool overlap pod container info",
				"poolName", commonstate.PoolNameReclaim,
				"numaID", regionNuma,
				"podUID", podUID,
				"containerName", containerName,
				"reclaimSize", reclaimedCoresSize)
			result.SetPoolOverlapPodContainerInfo(commonstate.PoolNameReclaim, regionNuma, podUID, containerName, reclaimedCoresSize)
		}
	}

	// set reclaim pool cpu limit
	result.SetPoolEntry(commonstate.PoolNameReclaim, regionNuma, 0, reclaimedCoresLimit)
	return nil
}

func (pa *ProvisionAssemblerCommon) assembleReserve(result *types.InternalCPUCalculationResult) {
	// fill in reserve pool entry
	reservePoolSize, _ := pa.metaReader.GetPoolSize(commonstate.PoolNameReserve)
	result.SetPoolEntry(commonstate.PoolNameReserve, commonstate.FakedNUMAID, reservePoolSize, -1)
}

// validateDefaultShareBackfillConfig rejects incompatible feature combinations:
// backfilling the default share pool with all residual non-reclaim CPUs only
// makes sense when neither shared nor dedicated cores overlap reclaimed cores,
// otherwise the residual accounting would double-count overlapped CPUs.
func (pa *ProvisionAssemblerCommon) validateDefaultShareBackfillConfig() error {
	conf := pa.conf.GetDynamicConfiguration()
	if !conf.FillDefaultSharePoolWithNonReclaimCPUs {
		return nil
	}
	if *pa.allowSharedCoresOverlapReclaimedCores || !*pa.disableDedicatedCoresOverlapReclaimedCores {
		return fmt.Errorf("fill default share pool requires shared and dedicated reclaim overlap disabled")
	}
	return nil
}

func (pa *ProvisionAssemblerCommon) AssembleProvision() (types.InternalCPUCalculationResult, error) {
	if err := pa.validateDefaultShareBackfillConfig(); err != nil {
		general.Errorf("validateDefaultShareBackfillConfig failed with error: %v", err)
		return types.InternalCPUCalculationResult{}, err
	}

	calculationResult := types.InternalCPUCalculationResult{
		PoolEntries:                                make(map[string]map[int]types.CPUResource),
		PoolOverlapInfo:                            map[string]map[int]map[string]int{},
		PoolOverlapPodContainerInfo:                map[string]map[int]map[string]map[string]int{},
		TimeStamp:                                  time.Now(),
		AllowSharedCoresOverlapReclaimedCores:      *pa.allowSharedCoresOverlapReclaimedCores,
		DisableDedicatedCoresOverlapReclaimedCores: *pa.disableDedicatedCoresOverlapReclaimedCores,
	}
	// mark the backfill enabled once so downstream finalize can decide whether to
	// override the default share pool quantity with the residual target.
	calculationResult.DefaultShareBackfill.Enabled = pa.conf.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs

	pa.assembleReserve(&calculationResult)

	regionHelper := NewRegionMapHelper(*pa.regionMap)

	err := pa.assembleWithNUMABinding(regionHelper, &calculationResult)
	if err != nil {
		general.Errorf("assembleWithNUMABinding failed with error: %v", err)
		return types.InternalCPUCalculationResult{}, err
	}

	err = pa.assembleWithoutNUMABinding(regionHelper, &calculationResult)
	if err != nil {
		general.Errorf("assembleWithoutNUMABinding failed with error: %v", err)
		return types.InternalCPUCalculationResult{}, err
	}

	err = pa.assembleNUMABindingNUMAExclusive(regionHelper, &calculationResult)
	if err != nil {
		general.Errorf("assembleNUMABindingNUMAExclusive failed with error: %v", err)
		return types.InternalCPUCalculationResult{}, err
	}

	// after every scope is assembled, override the default share pool quantity
	// with the canonical residual budget when the backfill is enabled.
	if err = pa.finalizeDefaultShareBackfill(regionHelper, &calculationResult); err != nil {
		general.Errorf("finalizeDefaultShareBackfill failed with error: %v", err)
		return types.InternalCPUCalculationResult{}, err
	}

	return calculationResult, nil
}

func (pa *ProvisionAssemblerCommon) assembleWithoutNUMABinding(regionHelper *RegionMapHelper, result *types.InternalCPUCalculationResult) error {
	return pa.assembleWithoutNUMAExclusivePool(regionHelper, commonstate.FakedNUMAID, result)
}

func (pa *ProvisionAssemblerCommon) assembleWithNUMABinding(regionHelper *RegionMapHelper, result *types.InternalCPUCalculationResult) error {
	for numaID := range *pa.numaAvailable {
		err := pa.assembleWithoutNUMAExclusivePool(regionHelper, numaID, result)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pa *ProvisionAssemblerCommon) assembleNUMABindingNUMAExclusive(regionHelper *RegionMapHelper, result *types.InternalCPUCalculationResult) error {
	for numaID := range *pa.numaAvailable {
		dedicatedNUMAExclusiveRegions := regionHelper.GetRegions(numaID, configapi.QoSRegionTypeDedicated)
		for _, r := range dedicatedNUMAExclusiveRegions {
			if !r.IsNumaBinding() || !r.IsNumaExclusive() {
				continue
			}

			if err := pa.assembleDedicatedNUMAExclusiveRegion(r, result); err != nil {
				return fmt.Errorf("failed to assemble dedicatedNUMAExclusiveRegion: %v", err)
			}
		}
	}

	return nil
}

func (pa *ProvisionAssemblerCommon) assembleWithoutNUMAExclusivePool(
	regionHelper *RegionMapHelper,
	numaID int,
	result *types.InternalCPUCalculationResult,
) error {
	shareRegions := regionHelper.GetRegions(numaID, configapi.QoSRegionTypeShare)
	var numaSet machine.CPUSet
	if numaID == commonstate.FakedNUMAID {
		numaSet = *pa.nonBindingNumas
	} else {
		numaSet = machine.NewCPUSet(numaID)
	}

	cfg := pa.metaReader.GetResourcePackageConfig()
	pinnedCPUSizeByPkg := pa.getPinnedCPUSizeByPackage(numaSet, cfg)
	totalPinnedCPUSize := general.SumUpMapValues(pinnedCPUSizeByPkg)

	disableReclaimSelectorStr := pa.conf.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector
	disableReclaimSelector, err := general.ParseSelector(disableReclaimSelectorStr)
	if err != nil {
		return err
	}
	nonReclaimablePackages := sets.NewString()
	for _, numaID := range numaSet.ToSliceInt() {
		if pkgMap, ok := cfg[numaID]; ok {
			nonReclaimablePackages = nonReclaimablePackages.Union(resourcepackage.GetMatchedPackages(pkgMap, disableReclaimSelector))
		}
	}

	unpinnedShareRegionInfo, pinnedShareRegionInfos, err := extractShareRegionInfo(shareRegions, pinnedCPUSizeByPkg, nonReclaimablePackages)
	if err != nil {
		return err
	}

	isolationRegions := regionHelper.GetRegions(numaID, configapi.QoSRegionTypeIsolation)
	unpinnedIsolationInfo, pinnedIsolationInfo, err := extractIsolationRegionInfo(isolationRegions, pinnedCPUSizeByPkg, nonReclaimablePackages)
	if err != nil {
		return err
	}

	dedicatedRegions := regionHelper.GetRegions(numaID, configapi.QoSRegionTypeDedicated)
	unpinnedDedicatedInfo, pinnedDedicatedInfo, err := extractDedicatedRegionInfo(dedicatedRegions, pinnedCPUSizeByPkg, nonReclaimablePackages)
	if err != nil {
		return err
	}

	dynamicConf := pa.conf.GetDynamicConfiguration()
	effectiveHard := dynamicConf.EnableReclaim && dynamicConf.EnableRampUpReclaimHardPartition

	// A hard partition is a persistent per-physical-NUMA ownership invariant.
	// Publish the canonical floor even when this NUMA has no workload region;
	// otherwise QRM cannot distinguish an intentionally empty scope from a
	// missing reclaim target.
	if len(shareRegions) == 0 && len(isolationRegions) == 0 && len(dedicatedRegions) == 0 && numaID != commonstate.FakedNUMAID {
		if effectiveHard {
			result.SetPoolEntry(
				commonstate.PoolNameReclaim,
				numaID,
				getNUMAsResource(*pa.reservedForReclaim, numaSet),
				-1,
			)
		}
		return nil
	}

	nodeEnableReclaim := dynamicConf.EnableReclaim

	reservedForReclaim := getNUMAsResource(*pa.reservedForReclaim, numaSet)
	poolAvailableBeforeReserve := getNUMAsResource(*pa.numaAvailable, numaSet)
	pinnedPoolAvailableByPkg, pinnedReserveByPkg,
		unpinnedShareAndIsolatedDedicatedPoolAvailable, unpinnedReserve := getEligibilityDomainCapacities(
		numaSet,
		cfg,
		pinnedCPUSizeByPkg,
		poolAvailableBeforeReserve,
		reservedForReclaim,
		pa.metaReader,
	)
	shareAndIsolatedDedicatedPoolAvailable := poolAvailableBeforeReserve
	legacySharedOnly := len(dedicatedRegions) == 0
	reserveHeldOutsidePools := effectiveHard
	if reserveHeldOutsidePools || (legacySharedOnly && !*pa.allowSharedCoresOverlapReclaimedCores) {
		for pkgName, reserve := range pinnedReserveByPkg {
			pinnedPoolAvailableByPkg[pkgName] = general.Max(pinnedPoolAvailableByPkg[pkgName]-reserve, 0)
		}
		unpinnedShareAndIsolatedDedicatedPoolAvailable = general.Max(unpinnedShareAndIsolatedDedicatedPoolAvailable-unpinnedReserve, 0)
		shareAndIsolatedDedicatedPoolAvailable = general.Max(shareAndIsolatedDedicatedPoolAvailable-reservedForReclaim, 0)
	}

	getShareAndIsolateDedicatedPoolSizesFunc := func(
		shareAndIsolatedDedicatedPoolAvailable, reserveInDomain int,
		shareRegionInfo, dedicatedRegionInfo regionInfo,
		isolationRegionInfo isolationRegionInfo,
	) map[string]int {
		sharePoolSizeRequirements := getPoolSizeRequirements(shareRegionInfo)
		allowExpand := !nodeEnableReclaim || *pa.allowSharedCoresOverlapReclaimedCores
		dedicatedMinimums := getPoolSizeRequirements(dedicatedRegionInfo)
		shareExpansionTargets := sharePoolSizeRequirements
		if allowExpand {
			shareExpansionTargets = shareRegionInfo.requests
		}
		expansionTargets := general.MergeMapInt(shareExpansionTargets, isolationRegionInfo.isolationUpperSizes)
		for poolName, request := range dedicatedRegionInfo.requests {
			expansionTargets[poolName] = desiredDedicatedPhysical(
				request,
				dedicatedRegionInfo.requirements[poolName],
				dedicatedRegionInfo.reclaimEnable[poolName],
				result.DisableDedicatedCoresOverlapReclaimedCores,
			)
		}

		general.InfoS("getShareAndIsolateDedicatedPoolSizesFunc pre priority allocation",
			"shareAndIsolatedDedicatedPoolAvailable", shareAndIsolatedDedicatedPoolAvailable,
			"allowExpand", allowExpand,
			"dedicatedMinimums", dedicatedMinimums,
			"isolationLowerSizes", isolationRegionInfo.isolationLowerSizes,
			"sharePoolSizeRequirements", sharePoolSizeRequirements,
			"expansionTargets", expansionTargets)

		allocateAtCapacity := func(capacity int) (map[string]int, bool) {
			sharedAvailable := capacity
			if !reserveHeldOutsidePools && !legacySharedOnly && !*pa.allowSharedCoresOverlapReclaimedCores {
				sharedAvailable = general.Max(sharedAvailable-reserveInDomain, 0)
			}
			dedicatedAvailable := capacity
			if !reserveHeldOutsidePools && result.DisableDedicatedCoresOverlapReclaimedCores {
				dedicatedAvailable = general.Max(dedicatedAvailable-reserveInDomain, 0)
			}

			var poolSizes map[string]int
			var throttled bool
			if legacySharedOnly {
				poolSizes, throttled = allocatePoolSizesByPriority(
					capacity,
					dedicatedMinimums,
					isolationRegionInfo.isolationLowerSizes,
					sharePoolSizeRequirements,
					expansionTargets,
				)
			} else {
				poolSizes, throttled = allocatePoolSizesByWorkloadPriority(
					capacity,
					dedicatedAvailable,
					sharedAvailable,
					dedicatedMinimums,
					isolationRegionInfo.isolationLowerSizes,
					sharePoolSizeRequirements,
					expansionTargets,
				)
			}
			if allowExpand {
				expandSharePoolsToCapacity(poolSizes, shareRegionInfo.requests, sharedAvailable)
			}
			return poolSizes, throttled
		}

		allocationCapacity := shareAndIsolatedDedicatedPoolAvailable
		shareAndIsolateDedicatedPoolSizes, poolThrottled := allocateAtCapacity(allocationCapacity)
		if nodeEnableReclaim && !legacySharedOnly && !reserveHeldOutsidePools {
			for {
				overlapCapacity := 0
				if *pa.allowSharedCoresOverlapReclaimedCores {
					for poolName, requirement := range shareRegionInfo.requirements {
						if shareRegionInfo.reclaimEnable[poolName] {
							overlapCapacity += general.Max(shareAndIsolateDedicatedPoolSizes[poolName]-requirement, 0)
						}
					}
				}
				if !result.DisableDedicatedCoresOverlapReclaimedCores {
					for poolName, requirement := range dedicatedRegionInfo.requirements {
						if dedicatedRegionInfo.reclaimEnable[poolName] {
							overlapCapacity += general.Max(shareAndIsolateDedicatedPoolSizes[poolName]-requirement, 0)
						}
					}
				}
				freeCapacity := general.Max(
					shareAndIsolatedDedicatedPoolAvailable-general.SumUpMapValues(shareAndIsolateDedicatedPoolSizes),
					0,
				)
				deficit := general.Max(reserveInDomain-freeCapacity-overlapCapacity, 0)
				if deficit == 0 || allocationCapacity == 0 {
					break
				}
				allocationCapacity = general.Max(allocationCapacity-deficit, 0)
				shareAndIsolateDedicatedPoolSizes, poolThrottled = allocateAtCapacity(allocationCapacity)
			}
		}

		general.InfoS("getShareAndIsolateDedicatedPoolSizesFunc post priority allocation",
			"shareAndIsolateDedicatedPoolSizes", shareAndIsolateDedicatedPoolSizes,
			"poolThrottled", poolThrottled)

		for _, r := range shareRegionInfo.regionMap {
			r.SetThrottled(poolThrottled)
		}

		return shareAndIsolateDedicatedPoolSizes
	}

	shareInfo := initRegionInfo()
	isolationInfo := initIsolationRegionInfo()
	dedicatedInfo := initRegionInfo()
	shareAndIsolateDedicatedPoolSizes := make(map[string]int)
	pinnedCPUSetAllInfo := getPinnedCPUSetAllRegionInfo(pinnedShareRegionInfos, pinnedIsolationInfo, pinnedDedicatedInfo)
	totalUnusedNonReclaimablePinnedCPUSize := 0

	general.InfoS("pool info start",
		"numaID", numaID,
		"shareAndIsolatedDedicatedPoolAvailable", shareAndIsolatedDedicatedPoolAvailable,
		"totalPinnedCPUSize", totalPinnedCPUSize,
		"unpinnedShareAndIsolatedDedicatedPoolAvailable", unpinnedShareAndIsolatedDedicatedPoolAvailable,
		"nonReclaimablePackages", nonReclaimablePackages,
		"disableReclaimSelector", disableReclaimSelector)

	// first calculate share and isolate dedicated pool sizes for each pinned region
	for pkgName, pinnedCPUSize := range pinnedCPUSizeByPkg {
		pinnedPoolAvailable := pinnedPoolAvailableByPkg[pkgName]
		allInfo, ok := pinnedCPUSetAllInfo[pkgName]
		if !ok {
			// No regions for this package, so allocated size is 0
			if nonReclaimablePackages.Has(pkgName) {
				totalUnusedNonReclaimablePinnedCPUSize += pinnedPoolAvailable
			}
			continue
		}

		poolSizes := getShareAndIsolateDedicatedPoolSizesFunc(
			pinnedPoolAvailable,
			pinnedReserveByPkg[pkgName],
			allInfo.shareRegionInfo,
			allInfo.dedicatedRegionInfos,
			allInfo.isolationRegionInfo,
		)

		allocatedForPkg := general.SumUpMapValues(poolSizes)
		unusedForPkg := pinnedPoolAvailable - allocatedForPkg
		if nonReclaimablePackages.Has(pkgName) {
			totalUnusedNonReclaimablePinnedCPUSize += unusedForPkg
		}

		for poolName, size := range poolSizes {
			shareAndIsolateDedicatedPoolSizes[poolName] = size
		}

		shareInfo.merge(allInfo.shareRegionInfo)
		isolationInfo.merge(allInfo.isolationRegionInfo)
		dedicatedInfo.merge(allInfo.dedicatedRegionInfos)

		general.InfoS("pinned pool info",
			"numaID", numaID,
			"pkgName", pkgName,
			"shareRegionInfo", allInfo.shareRegionInfo,
			"isolationRegionInfo", allInfo.isolationRegionInfo,
			"dedicatedRegionInfos", allInfo.dedicatedRegionInfos,
			"pinnedCPUSize", pinnedCPUSize,
			"pinnedPoolAvailable", pinnedPoolAvailable,
			"poolSizes", poolSizes)
	}

	unpinnedPoolSizes := getShareAndIsolateDedicatedPoolSizesFunc(
		unpinnedShareAndIsolatedDedicatedPoolAvailable,
		unpinnedReserve,
		unpinnedShareRegionInfo,
		unpinnedDedicatedInfo,
		unpinnedIsolationInfo,
	)
	for poolName, size := range unpinnedPoolSizes {
		shareAndIsolateDedicatedPoolSizes[poolName] = size
	}

	shareInfo.merge(unpinnedShareRegionInfo)
	isolationInfo.merge(unpinnedIsolationInfo)
	dedicatedInfo.merge(unpinnedDedicatedInfo)

	general.InfoS("unpinned pool info",
		"numaID", numaID,
		"unpinnedShareRegionInfo", unpinnedShareRegionInfo,
		"unpinnedIsolationRegionInfo", unpinnedIsolationInfo,
		"unpinnedDedicatedRegionInfos", unpinnedDedicatedInfo,
		"unpinnedShareAndIsolatedDedicatedPoolAvailable", unpinnedShareAndIsolatedDedicatedPoolAvailable,
		"poolSizes", unpinnedPoolSizes)

	dedicatedPoolSizes := make(map[string]int)
	for poolName := range dedicatedInfo.requests {
		if size, ok := shareAndIsolateDedicatedPoolSizes[poolName]; ok {
			dedicatedPoolSizes[poolName] = size
		}
	}
	dedicatedPoolAvailable := general.SumUpMapValues(dedicatedPoolSizes)
	dedicatedPoolSizeRequirements := getPoolSizeRequirements(dedicatedInfo)
	dedicatedReclaimCoresSize := dedicatedPoolAvailable - general.SumUpMapValues(dedicatedPoolSizeRequirements)
	if result.DisableDedicatedCoresOverlapReclaimedCores {
		dedicatedReclaimCoresSize = general.Max(dedicatedReclaimCoresSize, 0)
	}

	for poolName, podSet := range dedicatedInfo.podSet {
		if len(podSet) > 0 && shareAndIsolateDedicatedPoolSizes[poolName] <= 0 {
			return fmt.Errorf("active dedicated pool %q was regulated to zero", poolName)
		}
	}
	for poolName := range isolationInfo.isolationUpperSizes {
		if shareAndIsolateDedicatedPoolSizes[poolName] <= 0 {
			return fmt.Errorf("active isolation pool %q was regulated to zero", poolName)
		}
	}
	for poolName := range shareInfo.regionMap {
		if shareAndIsolateDedicatedPoolSizes[poolName] <= 0 {
			return fmt.Errorf("active shared pool %q was regulated to zero", poolName)
		}
	}

	general.InfoS("pool info",
		"numaID", numaID,
		"reservedForReclaim", reservedForReclaim,
		"shareRequirements", shareInfo.requirements,
		"shareRequests", shareInfo.requests,
		"shareReclaimEnable", shareInfo.reclaimEnable,
		"shareMinReclaimedCoresCPUQuota", shareInfo.minReclaimedCoresCPUQuota,
		"dedicatedRequirements", dedicatedInfo.requirements,
		"dedicatedRequests", dedicatedInfo.requests,
		"dedicatedReclaimEnable", dedicatedInfo.reclaimEnable,
		"dedicatedMinReclaimedCoresCPUQuota", dedicatedInfo.minReclaimedCoresCPUQuota,
		"dedicatedPoolAvailable", dedicatedPoolAvailable,
		"dedicatedPoolSizeRequirements", dedicatedPoolSizeRequirements,
		"dedicatedReclaimCoresSize", dedicatedReclaimCoresSize,
		"sharePoolSizeRequirements", getPoolSizeRequirements(shareInfo),
		"isolationUpperSizes", isolationInfo.isolationUpperSizes,
		"isolationLowerSizes", isolationInfo.isolationLowerSizes,
		"shareAndIsolateDedicatedPoolSizes", shareAndIsolateDedicatedPoolSizes,
		"shareAndIsolatedDedicatedPoolAvailable", shareAndIsolatedDedicatedPoolAvailable,
		"totalUnusedNonReclaimablePinnedCPUSize", totalUnusedNonReclaimablePinnedCPUSize,
		"unpinnedShareAndIsolatedDedicatedPoolAvailable", unpinnedShareAndIsolatedDedicatedPoolAvailable)

	// fill in regulated share-and-isolated pool entries
	for poolName, poolSize := range shareAndIsolateDedicatedPoolSizes {
		if podSet, ok := dedicatedInfo.podSet[poolName]; ok {
			// fill in dedicated pool entries with pod uid for each pod
			for uid := range podSet {
				result.SetPoolEntry(uid, numaID, poolSize, -1)
			}
		} else {
			// fill in share pool or isolation pool entries with pool name for each pod
			result.SetPoolEntry(poolName, numaID, poolSize, -1)
		}
	}

	reclaimPoolData := &reclaimPoolCalculationData{
		shareInfo:                              shareInfo,
		isolationInfo:                          isolationInfo,
		dedicatedInfo:                          dedicatedInfo,
		shareAndIsolateDedicatedPoolSizes:      shareAndIsolateDedicatedPoolSizes,
		dedicatedPoolSizes:                     dedicatedPoolSizes,
		dedicatedReclaimCoresSize:              dedicatedReclaimCoresSize,
		shareAndIsolatedDedicatedPoolAvailable: shareAndIsolatedDedicatedPoolAvailable,
		reservedForReclaim:                     reservedForReclaim,
		nodeEnableReclaim:                      nodeEnableReclaim,
		numaID:                                 numaID,
		totalUnusedNonReclaimablePinnedCPUSize: totalUnusedNonReclaimablePinnedCPUSize,
		reserveHeldOutsidePools:                reserveHeldOutsidePools,
	}

	policy := reclaimPoolCalculationPolicy{
		allowSharedOverlap:    *pa.allowSharedCoresOverlapReclaimedCores,
		allowDedicatedOverlap: !result.DisableDedicatedCoresOverlapReclaimedCores,
	}
	general.InfoS("reclaim pool calculation input",
		"numaID", numaID,
		"nodeEnableReclaim", nodeEnableReclaim,
		"policy", policy,
		"reservedForReclaim", reservedForReclaim,
		"shareAndIsolatedDedicatedPoolAvailable", shareAndIsolatedDedicatedPoolAvailable,
		"shareAndIsolateDedicatedPoolSizes", shareAndIsolateDedicatedPoolSizes,
		"dedicatedPoolSizes", dedicatedPoolSizes,
		"dedicatedPoolSizeRequirements", dedicatedPoolSizeRequirements,
		"dedicatedReclaimCoresSize", dedicatedReclaimCoresSize,
		"totalUnusedNonReclaimablePinnedCPUSize", totalUnusedNonReclaimablePinnedCPUSize,
		"shareRequirements", shareInfo.requirements,
		"shareReclaimEnable", shareInfo.reclaimEnable,
		"dedicatedRequirements", dedicatedInfo.requirements,
		"dedicatedReclaimEnable", dedicatedInfo.reclaimEnable)
	reclaimedCoresSize, _, reclaimedCoresQuota, err := pa.calculateReclaimPool(reclaimPoolData, policy, result)
	if err != nil {
		return err
	}

	ratio := pa.conf.GetDynamicConfiguration().ReclaimedCPUMaxRatio
	cpuCount := 0
	if ratio > 0 {
		cpuCount = pa.cpuCountInNUMAs(numaSet)
	}
	if ratio <= 0 || cpuCount > 0 {
		cpusPerCore, err := pa.reclaimRatioCPUsPerCore()
		if err != nil {
			return err
		}
		clamp, err := clampByReclaimedCPUMaxRatioWithDiagnostics(
			reclaimedCoresSize,
			reclaimedCoresQuota,
			ratio,
			cpuCount,
			reservedForReclaim,
			cpusPerCore,
		)
		if err != nil {
			return fmt.Errorf("clamp reclaim by max ratio: %w", err)
		}
		// preserve the original write semantics: the final reclaim size and
		// quota are exactly what the underlying helper returns.
		reclaimedCoresSize = clamp.FinalSize
		reclaimedCoresQuota = clamp.FinalLimit
		// accumulate the default share backfill diagnostics for this scope
		// once; released cores from exclusive NUMA scopes are intentionally
		// excluded and must not be counted here.
		result.DefaultShareBackfill.RawReclaimSize += clamp.RawSize
		result.DefaultShareBackfill.FinalReclaimSize += clamp.FinalSize
		result.DefaultShareBackfill.ReleasedReclaimSize += clamp.ReleasedSize
	}

	overlapBudget := reclaimedCoresSize
	if effectiveHard {
		overlapBudget = general.Max(overlapBudget-reservedForReclaim, 0)
	}
	overlapReclaimedCoresSize := clampReclaimOverlapMetadata(
		result,
		numaID,
		overlapBudget,
		reclaimPoolData.overlapAtoms...,
	)
	nonOverlapReclaimedCoresSize := general.Max(reclaimedCoresSize-overlapReclaimedCoresSize, 0)
	if effectiveHard && numaID == commonstate.FakedNUMAID {
		nonOverlapReclaimedCoresSize = general.Max(nonOverlapReclaimedCoresSize-reservedForReclaim, 0)
	}
	general.InfoS("reclaim pool calculation output",
		"numaID", numaID,
		"nodeEnableReclaim", nodeEnableReclaim,
		"policy", policy,
		"ratio", ratio,
		"cpuCount", cpuCount,
		"reservedForReclaim", reservedForReclaim,
		"reclaimedCoresSize", reclaimedCoresSize,
		"overlapReclaimedCoresSize", overlapReclaimedCoresSize,
		"nonOverlapReclaimedCoresSize", nonOverlapReclaimedCoresSize,
		"reclaimedCoresQuota", reclaimedCoresQuota,
		"overlapAtoms", summarizeOverlapAtoms(reclaimPoolData.overlapAtoms))
	result.SetPoolEntry(commonstate.PoolNameReclaim, numaID, nonOverlapReclaimedCoresSize, reclaimedCoresQuota)

	general.InfoS("assemble reclaim pool entry",
		"numaID", numaID,
		"reservedForReclaim", reservedForReclaim,
		"reclaimedCoresSize", reclaimedCoresSize,
		"overlapReclaimedCoresSize", overlapReclaimedCoresSize,
		"nonOverlapReclaimedCoresSize", nonOverlapReclaimedCoresSize,
		"reclaimedCoresQuota", reclaimedCoresQuota)

	return nil
}

// clampReclaimOverlapMetadata bounds generated reclaim-overlap metadata by the
// aggregate reclaim budget. calculateReclaimPool builds the overlap metadata
// before ratio-based clamping may shrink the aggregate size; this keeps both
// outputs from describing conflicting reclaim capacity.
type podContainerAlias struct {
	podUID        string
	containerName string
}

type overlapAtom struct {
	key            string
	size           int
	poolAlias      string
	containerAlias []podContainerAlias
}

func summarizeOverlapAtoms(atoms []overlapAtom) []string {
	if len(atoms) == 0 {
		return nil
	}
	result := make([]string, 0, len(atoms))
	for _, atom := range atoms {
		aliases := make([]string, 0, len(atom.containerAlias))
		for _, alias := range atom.containerAlias {
			aliases = append(aliases, alias.podUID+"/"+alias.containerName)
		}
		sort.Strings(aliases)
		result = append(result, fmt.Sprintf(
			"key=%s,size=%d,pool=%s,containers=%s",
			atom.key, atom.size, atom.poolAlias, strings.Join(aliases, "|"),
		))
	}
	sort.Strings(result)
	return result
}

func clampReclaimOverlapMetadata(
	result *types.InternalCPUCalculationResult,
	numaID, budget int,
	atoms ...overlapAtom,
) int {
	if result == nil {
		return 0
	}
	if budget <= 0 {
		if byNUMA := result.PoolOverlapInfo[commonstate.PoolNameReclaim]; byNUMA != nil {
			delete(byNUMA, numaID)
		}
		if byNUMA := result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim]; byNUMA != nil {
			delete(byNUMA, numaID)
		}
		return 0
	}

	if len(atoms) == 0 {
		if byNUMA := result.PoolOverlapInfo[commonstate.PoolNameReclaim]; byNUMA != nil {
			delete(byNUMA, numaID)
		}
		if byNUMA := result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim]; byNUMA != nil {
			delete(byNUMA, numaID)
		}
		return 0
	}
	sort.Slice(atoms, func(i, j int) bool {
		return atoms[i].key < atoms[j].key
	})

	if byNUMA := result.PoolOverlapInfo[commonstate.PoolNameReclaim]; byNUMA != nil {
		delete(byNUMA, numaID)
	}
	if byNUMA := result.PoolOverlapPodContainerInfo[commonstate.PoolNameReclaim]; byNUMA != nil {
		delete(byNUMA, numaID)
	}

	remaining := budget
	actual := 0
	for _, atom := range atoms {
		if atom.size <= 0 || remaining == 0 {
			continue
		}
		size := general.Min(atom.size, remaining)
		remaining -= size
		actual += size
		if atom.poolAlias != "" {
			result.SetPoolOverlapInfo(commonstate.PoolNameReclaim, numaID, atom.poolAlias, size)
		}
		for _, alias := range atom.containerAlias {
			result.SetPoolOverlapPodContainerInfo(
				commonstate.PoolNameReclaim,
				numaID,
				alias.podUID,
				alias.containerName,
				size,
			)
		}
	}
	return actual
}

type reclaimPoolCalculationData struct {
	shareInfo                              regionInfo
	isolationInfo                          isolationRegionInfo
	dedicatedInfo                          regionInfo
	shareAndIsolateDedicatedPoolSizes      map[string]int
	dedicatedPoolSizes                     map[string]int
	dedicatedReclaimCoresSize              int
	shareAndIsolatedDedicatedPoolAvailable int
	reservedForReclaim                     int
	nodeEnableReclaim                      bool
	numaID                                 int
	totalUnusedNonReclaimablePinnedCPUSize int
	reserveHeldOutsidePools                bool
	overlapAtoms                           []overlapAtom
}

type reclaimPoolCalculationPolicy struct {
	allowSharedOverlap    bool
	allowDedicatedOverlap bool
}

func (pa *ProvisionAssemblerCommon) calculateReclaimPool(
	data *reclaimPoolCalculationData,
	policy reclaimPoolCalculationPolicy,
	result *types.InternalCPUCalculationResult,
) (int, int, float64, error) {
	if data.nodeEnableReclaim && len(data.dedicatedInfo.requests) > 0 {
		return pa.calculateEnabledReclaimPool(data, policy)
	}
	if policy.allowSharedOverlap {
		return pa.calculateOverlapReclaimPool(data, policy, result)
	}
	return pa.calculateNonOverlapReclaimPool(data, policy, result)
}

func (pa *ProvisionAssemblerCommon) calculateEnabledReclaimPool(
	data *reclaimPoolCalculationData,
	policy reclaimPoolCalculationPolicy,
) (int, int, float64, error) {
	overlapSize := 0
	if policy.allowSharedOverlap {
		for poolName, requirement := range data.shareInfo.requirements {
			if !data.shareInfo.reclaimEnable[poolName] {
				continue
			}
			size := data.shareAndIsolateDedicatedPoolSizes[poolName] - requirement
			if size <= 0 {
				continue
			}
			data.overlapAtoms = append(data.overlapAtoms, overlapAtom{
				key:       "0/pool/" + poolName,
				size:      size,
				poolAlias: poolName,
			})
			overlapSize += size
		}
	}
	if policy.allowDedicatedOverlap {
		for poolName, requirement := range data.dedicatedInfo.requirements {
			if !data.dedicatedInfo.reclaimEnable[poolName] {
				continue
			}
			size := data.dedicatedPoolSizes[poolName] - requirement
			if size <= 0 {
				continue
			}
			podSet := data.dedicatedInfo.podSet[poolName]
			if len(podSet) == 0 {
				continue
			}
			atom := overlapAtom{
				key:  "1/dedicated/" + poolName,
				size: size,
			}
			for podUID, containerSet := range podSet {
				for containerName := range containerSet {
					atom.containerAlias = append(atom.containerAlias, podContainerAlias{
						podUID:        podUID,
						containerName: containerName,
					})
				}
			}
			data.overlapAtoms = append(data.overlapAtoms, atom)
			overlapSize += size
		}
	}

	physicalUsage := general.SumUpMapValues(data.shareAndIsolateDedicatedPoolSizes)
	freeStandalone := general.Max(
		data.shareAndIsolatedDedicatedPoolAvailable-physicalUsage-data.totalUnusedNonReclaimablePinnedCPUSize,
		0,
	)
	reclaimedCoresSize := freeStandalone + overlapSize
	if data.reserveHeldOutsidePools {
		reclaimedCoresSize += data.reservedForReclaim
	}
	reclaimedCoresQuota := float64(-1)
	general.InfoS("enabled reclaim pool calculation",
		"numaID", data.numaID,
		"policy", policy,
		"physicalUsage", physicalUsage,
		"freeStandalone", freeStandalone,
		"overlapSize", overlapSize,
		"reclaimedCoresSizeBeforeQuota", reclaimedCoresSize,
		"shareAndIsolatedDedicatedPoolAvailable", data.shareAndIsolatedDedicatedPoolAvailable,
		"totalUnusedNonReclaimablePinnedCPUSize", data.totalUnusedNonReclaimablePinnedCPUSize,
		"shareAndIsolateDedicatedPoolSizes", data.shareAndIsolateDedicatedPoolSizes,
		"dedicatedPoolSizes", data.dedicatedPoolSizes,
		"dedicatedReclaimCoresSize", data.dedicatedReclaimCoresSize,
		"reservedForReclaim", data.reservedForReclaim,
		"overlapAtoms", summarizeOverlapAtoms(data.overlapAtoms))

	quotaCtrlKnobEnabled, err := metacache.IsQuotaCtrlKnobEnabled(pa.metaReader)
	if err != nil {
		return 0, 0, 0, err
	}
	if quotaCtrlKnobEnabled && data.numaID != commonstate.FakedNUMAID && physicalUsage > 0 {
		reclaimedCoresQuota = float64(reclaimedCoresSize)
		for _, quota := range []float64{
			data.shareInfo.minReclaimedCoresCPUQuota,
			data.dedicatedInfo.minReclaimedCoresCPUQuota,
		} {
			if quota >= 0 {
				reclaimedCoresQuota = general.MinFloat64(reclaimedCoresQuota, quota)
			}
		}
		reclaimedCoresQuota = general.MaxFloat64(reclaimedCoresQuota, float64(data.reservedForReclaim))
	}
	general.InfoS("enabled reclaim pool calculation result",
		"numaID", data.numaID,
		"reclaimedCoresSize", reclaimedCoresSize,
		"overlapSize", overlapSize,
		"reclaimedCoresQuota", reclaimedCoresQuota,
		"quotaCtrlKnobEnabled", quotaCtrlKnobEnabled,
		"overlapAtoms", summarizeOverlapAtoms(data.overlapAtoms))
	return reclaimedCoresSize, overlapSize, reclaimedCoresQuota, nil
}

func (pa *ProvisionAssemblerCommon) calculateOverlapReclaimPool(
	data *reclaimPoolCalculationData,
	policy reclaimPoolCalculationPolicy,
	result *types.InternalCPUCalculationResult,
) (int, int, float64, error) {
	var reclaimedCoresSize, overlapReclaimedCoresSize int
	reclaimedCoresQuota := float64(-1)

	isolated := 0
	poolSizes := make(map[string]int)
	sharePoolSizes := make(map[string]int)
	reclaimablePoolSizes := make(map[string]int)
	nonReclaimableSharePoolSizes := make(map[string]int)
	reclaimableShareRequirements := make(map[string]int)
	reclaimableRequirements := make(map[string]int)

	for poolName, size := range data.shareAndIsolateDedicatedPoolSizes {
		_, ok := data.shareInfo.requirements[poolName]
		if ok {
			if data.shareInfo.reclaimEnable[poolName] {
				reclaimablePoolSizes[poolName] = size
				reclaimableShareRequirements[poolName] = data.shareInfo.requirements[poolName]
				reclaimableRequirements[poolName] = data.shareInfo.requirements[poolName]
			} else {
				nonReclaimableSharePoolSizes[poolName] = size
			}
			poolSizes[poolName] = size
			sharePoolSizes[poolName] = size
		}

		_, ok = data.isolationInfo.isolationUpperSizes[poolName]
		if ok {
			isolated += size
		}

		_, ok = data.dedicatedInfo.requests[poolName]
		if ok {
			if policy.allowDedicatedOverlap && data.dedicatedInfo.reclaimEnable[poolName] {
				reclaimablePoolSizes[poolName] = size
				reclaimableRequirements[poolName] = data.dedicatedInfo.requirements[poolName]
			}
			poolSizes[poolName] = size
		}
	}

	overlapReclaimSize := make(map[string]int)
	// We deduct totalUnusedNonReclaimablePinnedCPUSize here to ensure that the unused portion of non-reclaimable
	// resource packages is not added to the reclaim pool, preventing those CPUs from being reclaimed.
	shareReclaimCoresSize := data.shareAndIsolatedDedicatedPoolAvailable - isolated -
		general.SumUpMapValues(nonReclaimableSharePoolSizes) - general.SumUpMapValues(reclaimableShareRequirements) -
		general.SumUpMapValues(data.dedicatedPoolSizes) - data.totalUnusedNonReclaimablePinnedCPUSize

	if data.nodeEnableReclaim {
		reclaimedCoresSize = shareReclaimCoresSize + data.dedicatedReclaimCoresSize
		if data.reserveHeldOutsidePools {
			reclaimedCoresSize += data.reservedForReclaim
		}
		if reclaimedCoresSize < data.reservedForReclaim {
			reclaimedCoresSize = data.reservedForReclaim
			overlapCandidates := reclaimablePoolSizes
			if len(overlapCandidates) > 0 {
				overlapTarget := general.Min(reclaimedCoresSize, general.SumUpMapValues(overlapCandidates))
				regulatedOverlapReclaimPoolSize, err := regulateOverlapReclaimPoolSize(overlapCandidates, overlapTarget)
				if err != nil {
					return 0, 0, 0, fmt.Errorf("failed to regulateOverlapReclaimPoolSize for NUMAs reserved for reclaim: %w", err)
				}
				overlapReclaimSize = regulatedOverlapReclaimPoolSize
			}
		} else {
			for poolName, size := range reclaimablePoolSizes {
				requirement, ok := reclaimableRequirements[poolName]
				if !ok {
					continue
				}
				reclaimSize := size - requirement
				if reclaimSize > 0 {
					overlapReclaimSize[poolName] = reclaimSize
				} else {
					overlapReclaimSize[poolName] = 1
				}
			}
		}
	} else {
		reclaimedCoresSize = data.reservedForReclaim
		if len(poolSizes) > 0 && reclaimedCoresSize > shareReclaimCoresSize {
			reclaimedCoresSize = general.Min(reclaimedCoresSize, general.SumUpMapValues(poolSizes))
			var overlapSharePoolSizes map[string]int
			if reclaimedCoresSize <= general.SumUpMapValues(reclaimablePoolSizes) {
				overlapSharePoolSizes = reclaimablePoolSizes
			} else {
				overlapSharePoolSizes = poolSizes
			}

			reclaimSizes, err := regulateOverlapReclaimPoolSize(overlapSharePoolSizes, reclaimedCoresSize)
			if err != nil {
				return 0, 0, 0, fmt.Errorf("failed to regulateOverlapReclaimPoolSize: %w", err)
			}
			overlapReclaimSize = reclaimSizes
		} else if len(sharePoolSizes) > 0 && reclaimedCoresSize <= general.SumUpMapValues(sharePoolSizes) {
			reclaimSizes, err := regulateOverlapReclaimPoolSize(sharePoolSizes, reclaimedCoresSize)
			if err != nil {
				return 0, 0, 0, fmt.Errorf("failed to regulateOverlapReclaimPoolSize: %w", err)
			}
			overlapReclaimSize = reclaimSizes
		}
	}

	quotaCtrlKnobEnabled, err := metacache.IsQuotaCtrlKnobEnabled(pa.metaReader)
	if err != nil {
		return 0, 0, 0, err
	}

	if quotaCtrlKnobEnabled && data.numaID != commonstate.FakedNUMAID && len(poolSizes) > 0 {
		reclaimedCoresQuota = float64(general.Max(data.reservedForReclaim, reclaimedCoresSize))
		if data.shareInfo.minReclaimedCoresCPUQuota != -1 || data.dedicatedInfo.minReclaimedCoresCPUQuota != -1 {
			if data.shareInfo.minReclaimedCoresCPUQuota != -1 {
				reclaimedCoresQuota = data.shareInfo.minReclaimedCoresCPUQuota
			}

			if data.dedicatedInfo.minReclaimedCoresCPUQuota != -1 {
				reclaimedCoresQuota = general.MinFloat64(reclaimedCoresQuota, data.dedicatedInfo.minReclaimedCoresCPUQuota)
			}

			reclaimedCoresQuota = general.MaxFloat64(reclaimedCoresQuota, float64(data.reservedForReclaim))
		}

		// if cpu quota enabled, set all reclaimable share pool size to reclaimablePoolSizes
		for poolName := range overlapReclaimSize {
			overlapReclaimSize[poolName] = general.Max(overlapReclaimSize[poolName], reclaimablePoolSizes[poolName])
		}
	}

	for overlapPoolName, reclaimSize := range overlapReclaimSize {
		if _, ok := data.shareInfo.requests[overlapPoolName]; ok {
			if !policy.allowSharedOverlap {
				continue
			}
			general.InfoS("set pool overlap info",
				"poolName", commonstate.PoolNameReclaim,
				"numaID", data.numaID,
				"poolName", overlapPoolName,
				"reclaimSize", reclaimSize)
			result.SetPoolOverlapInfo(commonstate.PoolNameReclaim, data.numaID, overlapPoolName, reclaimSize)
			data.overlapAtoms = append(data.overlapAtoms, overlapAtom{
				key:       "0/pool/" + overlapPoolName,
				size:      reclaimSize,
				poolAlias: overlapPoolName,
			})
			overlapReclaimedCoresSize += reclaimSize
			continue
		}

		if podSet, ok := data.dedicatedInfo.podSet[overlapPoolName]; ok {
			if !policy.allowDedicatedOverlap {
				continue
			}
			// set pool overlap info for dedicated pool
			atom := overlapAtom{
				key:  "1/dedicated/" + overlapPoolName,
				size: reclaimSize,
			}
			for podUID, containerSet := range podSet {
				for containerName := range containerSet {
					general.InfoS("set pool overlap pod container info",
						"poolName", commonstate.PoolNameReclaim,
						"numaID", data.numaID,
						"podUID", podUID,
						"containerName", containerName,
						"reclaimSize", reclaimSize)
					result.SetPoolOverlapPodContainerInfo(commonstate.PoolNameReclaim, data.numaID, podUID, containerName, reclaimSize)
					atom.containerAlias = append(atom.containerAlias, podContainerAlias{
						podUID:        podUID,
						containerName: containerName,
					})
				}
			}
			data.overlapAtoms = append(data.overlapAtoms, atom)
			overlapReclaimedCoresSize += reclaimSize
			continue
		}
	}

	return reclaimedCoresSize, overlapReclaimedCoresSize, reclaimedCoresQuota, nil
}

func (pa *ProvisionAssemblerCommon) calculateNonOverlapReclaimPool(
	data *reclaimPoolCalculationData,
	policy reclaimPoolCalculationPolicy,
	result *types.InternalCPUCalculationResult,
) (int, int, float64, error) {
	var reclaimedCoresSize, overlapReclaimedCoresSize int
	reclaimedCoresQuota := float64(-1)

	if data.nodeEnableReclaim {
		if policy.allowDedicatedOverlap {
			for poolName, size := range data.dedicatedInfo.requests {
				if data.dedicatedInfo.reclaimEnable[poolName] {
					reclaimSize := size - data.dedicatedInfo.requirements[poolName]
					if reclaimSize <= 0 {
						continue
					}
					if podSet, ok := data.dedicatedInfo.podSet[poolName]; ok {
						atom := overlapAtom{
							key:  "1/dedicated/" + poolName,
							size: reclaimSize,
						}
						for podUID, containerSet := range podSet {
							for containerName := range containerSet {
								general.InfoS("set pool overlap pod container info",
									"poolName", commonstate.PoolNameReclaim,
									"numaID", data.numaID,
									"podUID", podUID,
									"containerName", containerName,
									"reclaimSize", reclaimSize)
								result.SetPoolOverlapPodContainerInfo(commonstate.PoolNameReclaim, data.numaID, podUID, containerName, reclaimSize)
								atom.containerAlias = append(atom.containerAlias, podContainerAlias{
									podUID:        podUID,
									containerName: containerName,
								})
							}
						}
						data.overlapAtoms = append(data.overlapAtoms, atom)
						overlapReclaimedCoresSize += reclaimSize
						continue
					}
				}
			}
		}

		// We deduct totalUnusedNonReclaimablePinnedCPUSize here to ensure that the unused portion of non-reclaimable
		// resource packages is not added to the reclaim pool, preventing those CPUs from being reclaimed.
		shareReclaimedCoresSize := data.shareAndIsolatedDedicatedPoolAvailable - general.SumUpMapValues(data.shareAndIsolateDedicatedPoolSizes) - data.totalUnusedNonReclaimablePinnedCPUSize
		reclaimedCoresSize = shareReclaimedCoresSize + data.dedicatedReclaimCoresSize + data.reservedForReclaim
	} else {
		reclaimedCoresSize = data.reservedForReclaim
	}

	general.InfoS("non-overlap reclaim pool calculation result",
		"numaID", data.numaID,
		"policy", policy,
		"nodeEnableReclaim", data.nodeEnableReclaim,
		"reclaimedCoresSize", reclaimedCoresSize,
		"overlapReclaimedCoresSize", overlapReclaimedCoresSize,
		"reclaimedCoresQuota", reclaimedCoresQuota,
		"shareAndIsolatedDedicatedPoolAvailable", data.shareAndIsolatedDedicatedPoolAvailable,
		"shareAndIsolateDedicatedPoolSizes", data.shareAndIsolateDedicatedPoolSizes,
		"dedicatedReclaimCoresSize", data.dedicatedReclaimCoresSize,
		"reservedForReclaim", data.reservedForReclaim,
		"totalUnusedNonReclaimablePinnedCPUSize", data.totalUnusedNonReclaimablePinnedCPUSize,
		"overlapAtoms", summarizeOverlapAtoms(data.overlapAtoms))
	return reclaimedCoresSize, overlapReclaimedCoresSize, reclaimedCoresQuota, nil
}

// regionInfo is a struct that contains region information
// for share region the key of map is owner pool name
// for dedicated region the key of map is region name
type regionInfo struct {
	requirements              map[string]int
	requests                  map[string]int
	reclaimEnable             map[string]bool
	podSet                    map[string]types.PodSet
	minReclaimedCoresCPUQuota float64
	regionMap                 map[string]region.QoSRegion
}

func (r *regionInfo) merge(other regionInfo) {
	for poolName, size := range other.requirements {
		r.requirements[poolName] = size
	}

	for poolName, size := range other.requests {
		r.requests[poolName] = size
	}

	for poolName, enable := range other.reclaimEnable {
		r.reclaimEnable[poolName] = enable
	}

	for poolName, podSet := range other.podSet {
		r.podSet[poolName] = podSet
	}

	if r.minReclaimedCoresCPUQuota == -1 || other.minReclaimedCoresCPUQuota < r.minReclaimedCoresCPUQuota {
		r.minReclaimedCoresCPUQuota = other.minReclaimedCoresCPUQuota
	}

	for poolName, reg := range other.regionMap {
		r.regionMap[poolName] = reg
	}
}

func initRegionInfo() regionInfo {
	return regionInfo{
		requirements:              make(map[string]int),
		requests:                  make(map[string]int),
		reclaimEnable:             make(map[string]bool),
		podSet:                    make(map[string]types.PodSet),
		minReclaimedCoresCPUQuota: -1,
		regionMap:                 make(map[string]region.QoSRegion),
	}
}

func (pa *ProvisionAssemblerCommon) getPinnedCPUSizeByPackage(numaSet machine.CPUSet, cfg types.ResourcePackageConfig) map[string]int {
	pinnedCPUSizeByPkg := make(map[string]int)

	if len(cfg) > 0 {
		for _, numaID := range numaSet.ToSliceInt() {
			pkgMap, ok := cfg[numaID]
			if !ok {
				continue
			}
			for pkgName, state := range pkgMap {
				if state == nil {
					continue
				}
				size := state.PinnedCPUSet.Size()
				if size <= 0 {
					continue
				}
				pinnedCPUSizeByPkg[pkgName] += size
			}
		}
		return pinnedCPUSizeByPkg
	}
	return pinnedCPUSizeByPkg
}

func getEligibilityDomainCapacities(
	numaSet machine.CPUSet,
	cfg types.ResourcePackageConfig,
	pinnedCPUSizeByPkg map[string]int,
	availableBeforeReserve,
	reservedForReclaim int,
	metaReader metacache.MetaReader,
) (map[string]int, map[string]int, int, int) {
	capacityByPkg := general.DeepCopyIntMap(pinnedCPUSizeByPkg)
	totalPinned := general.SumUpMapValues(pinnedCPUSizeByPkg)
	reserveByPkg := make(map[string]int)

	reserveCPUSet := machine.NewCPUSet()
	if reservePool, ok := metaReader.GetPoolInfo(commonstate.PoolNameReserve); ok && reservePool != nil {
		for _, numaID := range numaSet.ToSliceInt() {
			reserveCPUSet = reserveCPUSet.Union(reservePool.TopologyAwareAssignments[numaID])
		}
	}

	reservePinnedCPUSet := machine.NewCPUSet()
	for pkgName := range pinnedCPUSizeByPkg {
		pkgCPUSet := machine.NewCPUSet()
		for _, numaID := range numaSet.ToSliceInt() {
			if state := cfg[numaID][pkgName]; state != nil {
				pkgCPUSet = pkgCPUSet.Union(state.PinnedCPUSet)
			}
		}
		reserveInPkg := reserveCPUSet.Intersection(pkgCPUSet)
		reserveByPkg[pkgName] = reserveInPkg.Size()
		reservePinnedCPUSet = reservePinnedCPUSet.Union(reserveInPkg)
	}

	unpinnedReserve := general.Max(reservedForReclaim-reservePinnedCPUSet.Size(), 0)
	return capacityByPkg,
		reserveByPkg,
		general.Max(availableBeforeReserve-totalPinned, 0),
		unpinnedReserve
}

func extractShareRegionInfo(shareRegions []region.QoSRegion, pinnedCPUSizeByPkg map[string]int, nonReclaimablePackages sets.String) (regionInfo, map[string]*regionInfo, error) {
	unpinnedRegionInfo := initRegionInfo()
	pinnedRegionInfos := make(map[string]*regionInfo)

	for _, r := range shareRegions {
		controlKnob, err := r.GetProvision()
		if err != nil {
			return regionInfo{}, nil, err
		}

		ri := &unpinnedRegionInfo
		pkgName := r.GetResourcePackageName()
		if pkgName != "" {
			if _, ok := pinnedCPUSizeByPkg[pkgName]; ok {
				if _, exists := pinnedRegionInfos[pkgName]; !exists {
					info := initRegionInfo()
					pinnedRegionInfos[pkgName] = &info
				}
				ri = pinnedRegionInfos[pkgName]
			}
		}

		reclaimEnable := r.EnableReclaim()
		if pkgName != "" && nonReclaimablePackages.Has(pkgName) {
			reclaimEnable = false // override reclaim Enable if the resource package is non-reclaimable
		}

		ri.requirements[r.OwnerPoolName()] = general.Max(1, int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirement].Value))
		ri.requests[r.OwnerPoolName()] = general.Max(1, int(math.Ceil(r.GetPodsRequest())))
		ri.reclaimEnable[r.OwnerPoolName()] = reclaimEnable
		if reclaimEnable {
			if quota, ok := controlKnob[configapi.ControlKnobReclaimedCoresCPUQuota]; ok {
				if ri.minReclaimedCoresCPUQuota == -1 || quota.Value < ri.minReclaimedCoresCPUQuota {
					ri.minReclaimedCoresCPUQuota = quota.Value
				}
			}
		}
		ri.regionMap[r.OwnerPoolName()] = r
	}

	return unpinnedRegionInfo, pinnedRegionInfos, nil
}

func getPoolSizeRequirements(info regionInfo) map[string]int {
	result := make(map[string]int)
	for name, reclaimEnable := range info.reclaimEnable {
		if !reclaimEnable {
			result[name] = info.requests[name]
		} else {
			result[name] = info.requirements[name]
		}
	}
	return result
}

type isolationRegionInfo struct {
	isolationUpperSizes map[string]int
	isolationLowerSizes map[string]int
}

func (r *isolationRegionInfo) merge(other isolationRegionInfo) {
	for poolName, size := range other.isolationUpperSizes {
		r.isolationUpperSizes[poolName] = size
	}

	for poolName, size := range other.isolationLowerSizes {
		r.isolationLowerSizes[poolName] = size
	}
}

func initIsolationRegionInfo() isolationRegionInfo {
	return isolationRegionInfo{
		isolationUpperSizes: make(map[string]int),
		isolationLowerSizes: make(map[string]int),
	}
}

func extractIsolationRegionInfo(isolationRegions []region.QoSRegion, pinnedCPUSizeByPkg map[string]int, _ sets.String) (isolationRegionInfo, map[string]*isolationRegionInfo, error) {
	unpinnedRegionInfo := initIsolationRegionInfo()
	pinnedRegionInfos := make(map[string]*isolationRegionInfo)

	for _, r := range isolationRegions {
		controlKnob, err := r.GetProvision()
		if err != nil {
			return isolationRegionInfo{}, nil, err
		}

		ri := &unpinnedRegionInfo
		pkgName := r.GetResourcePackageName()
		if pkgName != "" {
			if _, ok := pinnedCPUSizeByPkg[pkgName]; ok {
				if _, exists := pinnedRegionInfos[pkgName]; !exists {
					info := initIsolationRegionInfo()
					pinnedRegionInfos[pkgName] = &info
				}
				ri = pinnedRegionInfos[pkgName]
			}
		}

		// Isolation region currently doesn't use reclaimEnable in the same way as Share and Dedicated,
		// but we still process it just in case, though it only sets upper/lower sizes.
		ri.isolationUpperSizes[r.Name()] = int(controlKnob[configapi.ControlKnobNonIsolatedUpperCPUSize].Value)
		ri.isolationLowerSizes[r.Name()] = int(controlKnob[configapi.ControlKnobNonIsolatedLowerCPUSize].Value)
	}

	return unpinnedRegionInfo, pinnedRegionInfos, nil
}

func extractDedicatedRegionInfo(regions []region.QoSRegion, pinnedCPUSizeByPkg map[string]int, nonReclaimablePackages sets.String) (regionInfo, map[string]*regionInfo, error) {
	unpinnedRegionInfo := initRegionInfo()
	pinnedRegionInfos := make(map[string]*regionInfo)

	for _, r := range regions {
		if r.IsNumaExclusive() {
			continue
		}

		controlKnob, err := r.GetProvision()
		if err != nil {
			return regionInfo{}, nil, err
		}

		ri := &unpinnedRegionInfo
		pkgName := r.GetResourcePackageName()
		if pkgName != "" {
			if _, ok := pinnedCPUSizeByPkg[pkgName]; ok {
				if _, exists := pinnedRegionInfos[pkgName]; !exists {
					info := initRegionInfo()
					pinnedRegionInfos[pkgName] = &info
				}
				ri = pinnedRegionInfos[pkgName]
			}
		}

		reclaimEnable := r.EnableReclaim()
		if pkgName != "" && nonReclaimablePackages.Has(pkgName) {
			reclaimEnable = false // override reclaim Enable if the resource package is non-reclaimable
		}

		regionName := r.Name()
		ri.requirements[regionName] = general.Max(1, int(controlKnob[configapi.ControlKnobNonReclaimedCPURequirement].Value))
		if r.IsNumaBinding() {
			numaBindingSize := r.GetBindingNumas().Size()
			if numaBindingSize == 0 {
				return regionInfo{}, nil, fmt.Errorf("numa binding size is zero, region name: %s", r.Name())
			}
			ri.requests[regionName] = int(math.Ceil(r.GetPodsRequest() / float64(numaBindingSize)))
		} else {
			ri.requests[regionName] = int(math.Ceil(r.GetPodsRequest()))
		}
		ri.reclaimEnable[regionName] = reclaimEnable
		ri.podSet[regionName] = r.GetPods()
		if reclaimEnable {
			if quota, ok := controlKnob[configapi.ControlKnobReclaimedCoresCPUQuota]; ok {
				if ri.minReclaimedCoresCPUQuota == -1 || quota.Value < ri.minReclaimedCoresCPUQuota {
					ri.minReclaimedCoresCPUQuota = quota.Value
				}
			}
		}
		ri.regionMap[regionName] = r
	}

	return unpinnedRegionInfo, pinnedRegionInfos, nil
}

type pinnedCPUSetAllRegionInfo struct {
	shareRegionInfo      regionInfo
	isolationRegionInfo  isolationRegionInfo
	dedicatedRegionInfos regionInfo
}

func initPinnedCPUSetAllRegionInfo() *pinnedCPUSetAllRegionInfo {
	return &pinnedCPUSetAllRegionInfo{
		shareRegionInfo:      initRegionInfo(),
		isolationRegionInfo:  initIsolationRegionInfo(),
		dedicatedRegionInfos: initRegionInfo(),
	}
}

func getPinnedCPUSetAllRegionInfo(
	shareRegionInfo map[string]*regionInfo,
	isolationRegionInfo map[string]*isolationRegionInfo,
	dedicatedRegionInfos map[string]*regionInfo,
) map[string]*pinnedCPUSetAllRegionInfo {
	res := make(map[string]*pinnedCPUSetAllRegionInfo)
	for pkgName, info := range shareRegionInfo {
		_, ok := res[pkgName]
		if !ok {
			res[pkgName] = initPinnedCPUSetAllRegionInfo()
		}
		res[pkgName].shareRegionInfo = *info
	}

	for regionName, info := range isolationRegionInfo {
		_, ok := res[regionName]
		if !ok {
			res[regionName] = initPinnedCPUSetAllRegionInfo()
		}
		res[regionName].isolationRegionInfo = *info
	}

	for regionName, info := range dedicatedRegionInfos {
		_, ok := res[regionName]
		if !ok {
			res[regionName] = initPinnedCPUSetAllRegionInfo()
		}
		res[regionName].dedicatedRegionInfos = *info
	}

	return res
}
