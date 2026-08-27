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
	"strconv"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

func (p *DynamicPolicy) emitFinalPoolSizeMetrics(entries state.PodEntries) {
	for poolName, containerEntries := range entries {
		if !containerEntries.IsPoolEntry() {
			continue
		}

		allocationInfo := containerEntries[commonstate.FakedContainerName]
		if allocationInfo == nil {
			general.Warningf("skip emitting pool size for malformed pool %s", poolName)
			continue
		}

		for numaID, cpus := range allocationInfo.TopologyAwareAssignments {
			_ = p.emitter.StoreInt64(util.MetricNamePoolSize, int64(cpus.Size()),
				metrics.MetricTypeNameRaw,
				metrics.MetricTag{Key: "poolName", Val: poolName},
				metrics.MetricTag{Key: "pool_type", Val: commonstate.GetPoolType(poolName)},
				metrics.MetricTag{Key: "numa_id", Val: strconv.Itoa(numaID)})
		}
	}
}
