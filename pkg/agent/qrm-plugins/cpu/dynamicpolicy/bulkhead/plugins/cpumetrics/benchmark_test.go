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

package cpumetrics

import (
	"context"
	"fmt"
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func BenchmarkCPUMetricsPlugin256Pods8NUMA(b *testing.B) {
	const (
		podCount  = 256
		numaCount = 8
	)

	pools := make(map[model.CPUSetPoolIdentity]machine.CPUSet, podCount)
	cpus := make([]int, 0, podCount)
	details := make(machine.CPUDetails, podCount)
	for cpu := 0; cpu < podCount; cpu++ {
		pools[model.CPUSetPoolIdentity{
			Kind:   model.CPUSetPoolKindDedicated,
			PodUID: fmt.Sprintf("benchmark-pod-%03d", cpu),
		}] = machine.NewCPUSet(cpu)
		cpus = append(cpus, cpu)
		details[cpu] = machine.CPUTopoInfo{
			NUMANodeID: cpu % numaCount,
			SocketID:   cpu % numaCount,
			CoreID:     cpu,
		}
	}

	emitter := &captureEmitter{}
	ctx := periodicalContext(
		viewWithProjection(model.AppliedViewLevelFull, pools),
		emitter,
		newFetcherWithMetrics(completeSamples(cpus...)),
		details,
	)
	plugin := &CPUMetricsPlugin{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		emitter.reset()
		if err := plugin.PeriodicalHandler(context.Background(), ctx); err != nil {
			b.Fatal(err)
		}
	}
}
