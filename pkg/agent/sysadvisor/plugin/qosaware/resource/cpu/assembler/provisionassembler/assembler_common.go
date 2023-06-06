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
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type ProvisionAssemblerCommon struct {
	conf               *config.Configuration
	regionMap          *map[string]region.QoSRegion
	reservedForReclaim *map[int]int
	numaAvailable      *map[int]int
	nonBindingNumas    *machine.CPUSet

	metaCache  metacache.MetaCache
	metaServer *metaserver.MetaServer
	emitter    metrics.MetricEmitter
}

func NewProvisionAssemblerCommon(conf *config.Configuration, regionMap *map[string]region.QoSRegion,
	reservedForReclaim *map[int]int, numaAvailable *map[int]int, nonBindingNumas *machine.CPUSet,
	metaCache metacache.MetaCache, metaServer *metaserver.MetaServer, emitter metrics.MetricEmitter) ProvisionAssembler {
	return &ProvisionAssemblerCommon{
		conf:               conf,
		regionMap:          regionMap,
		reservedForReclaim: reservedForReclaim,
		numaAvailable:      numaAvailable,
		nonBindingNumas:    nonBindingNumas,

		metaCache:  metaCache,
		metaServer: metaServer,
		emitter:    emitter,
	}
}

func (pa *ProvisionAssemblerCommon) AssembleProvision() (types.InternalCalculationResult, error) {
	calculationResult := types.InternalCalculationResult{
		PoolEntries: make(map[string]map[int]int),
	}

	// fill in reserve pool entry
	reservePoolSize, _ := pa.metaCache.GetPoolSize(state.PoolNameReserve)
	calculationResult.SetPoolEntry(state.PoolNameReserve, cpuadvisor.FakedNUMAID, reservePoolSize)

	sharePoolSizes := make(map[string]int)

	for _, r := range *pa.regionMap {
		controlKnob, err := r.GetProvision()
		if err != nil {
			return types.InternalCalculationResult{}, err
		}

		if r.Type() == types.QoSRegionTypeShare {
			// save raw share pool sizes
			sharePoolSizes[r.OwnerPoolName()] = int(controlKnob[types.ControlKnobNonReclaimedCPUSetSize].Value)

		} else if r.Type() == types.QoSRegionTypeDedicatedNumaExclusive {
			// fill in reclaim pool entry for dedicated numa exclusive regions
			regionNuma := r.GetBindingNumas().ToSliceInt()[0] // always one binding numa for this type of region
			reclaimPoolSize := int(controlKnob[types.ControlKnobReclaimedCPUSupplied].Value)
			reclaimPoolSize += pa.getNumasReservedForReclaim(machine.NewCPUSet(regionNuma))
			calculationResult.SetPoolEntry(state.PoolNameReclaim, regionNuma, reclaimPoolSize)
		}
	}

	// regulate share pool sizes
	enableReclaim := pa.conf.GetDynamicConfiguration().EnableReclaim
	sharePoolAvailable := getNumasAvailableResource(*pa.numaAvailable, *pa.nonBindingNumas)
	regulatePoolSizes(sharePoolSizes, sharePoolAvailable, enableReclaim)

	// fill in regulated share pool entries
	for poolName, poolSize := range sharePoolSizes {
		calculationResult.SetPoolEntry(poolName, cpuadvisor.FakedNUMAID, poolSize)
	}

	// fill in reclaim pool entry for non binding numas
	reclaimPoolSizeOfNonBindingNumas := sharePoolAvailable - general.SumUpMapValues(sharePoolSizes) + pa.getNumasReservedForReclaim(*pa.nonBindingNumas)
	calculationResult.SetPoolEntry(state.PoolNameReclaim, cpuadvisor.FakedNUMAID, reclaimPoolSizeOfNonBindingNumas)

	return calculationResult, nil
}

func (pa *ProvisionAssemblerCommon) getNumasReservedForReclaim(numas machine.CPUSet) int {
	res := 0
	for _, id := range numas.ToSliceInt() {
		if v, ok := (*pa.reservedForReclaim)[id]; ok {
			res += v
		}
	}
	return res
}
