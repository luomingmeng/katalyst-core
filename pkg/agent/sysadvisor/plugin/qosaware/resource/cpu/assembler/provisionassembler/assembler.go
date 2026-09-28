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
	"sync"

	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// ProvisionAssembler assembles internal node provision result.
// Advisor data elements are shared ONLY by assemblers as pointer to avoid rebuild in advisor,
// and NOT supposed to be used by other components.
type ProvisionAssembler interface {
	AssembleProvision(ctx ProvisionContext) (types.InternalCPUCalculationResult, error)
}

type ReclaimConstraint uint8

const (
	ReclaimConstraintNone ReclaimConstraint = iota
	ReclaimConstraintReservedFloor
)

type ProvisionContext struct {
	DynamicConfiguration *dynamic.Configuration
	RampUpActive         bool
	// RampUpDomains is the per-NUMA domain set that hosts an active ramp-up at
	// cycle start (FakedNUMAID -1 denotes the global/non-binding domain).
	RampUpDomains     []int
	ReclaimConstraint ReclaimConstraint
	ReclaimCeilings   map[ReclaimConstraintScope]*int
	// ReclaimActiveScopes identifies the scopes that are constrained this cycle
	// (actively ramped scopes plus scopes still draining back to steadyCap).
	ReclaimActiveScopes map[ReclaimConstraintScope]bool
	// ReclaimDomainTargets carries the per-scope descriptive contract built from the
	// ramp-up domains this cycle: the ramp-up Desired, the steady reservation Floor,
	// the steady upper SteadyCap and the member NUMAs. It is recorded into the result
	// for the guard commit and diagnostics; the assembler claps only to Ceilings.
	ReclaimDomainTargets map[ReclaimConstraintScope]types.ReclaimConstraintTarget
	// LiveReclaimByNUMA is the QRM-committed reclaim pool cpuset size per NUMA
	// at cycle start. It is a best-effort lower bound: when a real-NUMA ramp-up
	// domain is active and capacity allows, the assembler keeps reclaim at least
	// at this value. Global domain (-1) does NOT broadcast to real NUMAs.
	// CPURequest is a hard guarantee for reclaim-disabled non-exclusive dedicated
	// pools; live continuity MUST NOT reduce dedicated to inflate reclaim.
	LiveReclaimByNUMA map[int]int
}

type InitFunc func(conf *config.Configuration, extraConf interface{}, regionMap *map[string]region.QoSRegion,
	reservedForReclaim *map[int]int, rampUpReclaimCPUSetCap *map[int]int, numaAvailable *map[int]int, nonBindingNumas *machine.CPUSet,
	allowSharedCoresOverlapReclaimedCores *bool, disableDedicatedCoresOverlapReclaimedCores *bool,
	reader metacache.MetaReader, metaServer *metaserver.MetaServer, emitter metrics.MetricEmitter) ProvisionAssembler

var initializers sync.Map

func RegisterInitializer(name types.CPUProvisionAssemblerName, initFunc InitFunc) {
	initializers.Store(name, initFunc)
}

func GetRegisteredInitializers() map[types.CPUProvisionAssemblerName]InitFunc {
	res := make(map[types.CPUProvisionAssemblerName]InitFunc)
	initializers.Range(func(key, value interface{}) bool {
		res[key.(types.CPUProvisionAssemblerName)] = value.(InitFunc)
		return true
	})
	return res
}
