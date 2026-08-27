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
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type metricRecord struct {
	key      string
	val      int64
	emitType metrics.MetricTypeName
	tags     []metrics.MetricTag
}

type recordingMetricEmitter struct {
	records []metricRecord
}

func requirePoolSizeMetric(t *testing.T, records []metricRecord, poolName string, numaID int, value int64) {
	t.Helper()

	for _, record := range records {
		tags := make(map[string]string, len(record.tags))
		for _, tag := range record.tags {
			tags[tag.Key] = tag.Val
		}
		if record.key == util.MetricNamePoolSize &&
			tags["poolName"] == poolName &&
			tags["numa_id"] == strconv.Itoa(numaID) {
			require.Equal(t, value, record.val)
			return
		}
	}
	t.Fatalf("missing pool size metric for pool %q numa %d", poolName, numaID)
}

func (e *recordingMetricEmitter) StoreInt64(key string, val int64, emitType metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	e.records = append(e.records, metricRecord{
		key:      key,
		val:      val,
		emitType: emitType,
		tags:     append([]metrics.MetricTag(nil), tags...),
	})
	return nil
}

func (e *recordingMetricEmitter) StoreFloat64(_ string, _ float64, _ metrics.MetricTypeName, _ ...metrics.MetricTag) error {
	return nil
}

func (e *recordingMetricEmitter) WithTags(_ string, _ ...metrics.MetricTag) metrics.MetricEmitter {
	return e
}

func (e *recordingMetricEmitter) Run(_ context.Context) {}

func TestDynamicPolicyEmitRuntimeConfigMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		dynamicConfig *dynamicconfig.DynamicAgentConfiguration
		wantValue     int64
	}{
		{
			name:      "nil dynamic config emits disabled",
			wantValue: 0,
		},
		{
			name:          "enable reclaim false emits disabled",
			dynamicConfig: newDynamicConfigWithEnableReclaim(false),
			wantValue:     0,
		},
		{
			name:          "enable reclaim true emits enabled",
			dynamicConfig: newDynamicConfigWithEnableReclaim(true),
			wantValue:     1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			emitter := &recordingMetricEmitter{}
			policy := &DynamicPolicy{
				emitter:       emitter,
				dynamicConfig: tt.dynamicConfig,
			}

			policy.emitRuntimeConfigMetrics()

			require.Len(t, emitter.records, 1)
			require.Equal(t, util.MetricNameReclaimEnabled, emitter.records[0].key)
			require.Equal(t, tt.wantValue, emitter.records[0].val)
			require.Equal(t, metrics.MetricTypeNameRaw, emitter.records[0].emitType)
			require.Empty(t, emitter.records[0].tags)
		})
	}
}

func TestEmitFinalPoolSizeMetrics(t *testing.T) {
	t.Parallel()

	emitter := &recordingMetricEmitter{}
	policy := &DynamicPolicy{emitter: emitter}
	entries := state.PodEntries{
		commonstate.PoolNameShare: {
			commonstate.FakedContainerName: {
				AllocationMeta: commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
				TopologyAwareAssignments: map[int]machine.CPUSet{
					0: machine.NewCPUSet(0, 1),
					1: machine.NewCPUSet(2, 3),
				},
			},
		},
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: {
				AllocationMeta: commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				TopologyAwareAssignments: map[int]machine.CPUSet{
					0: machine.NewCPUSet(4),
					1: machine.NewCPUSet(5),
				},
			},
		},
		"pod": {
			"container": {
				AllocationMeta: commonstate.AllocationMeta{PodUid: "pod"},
			},
		},
		"malformed-pool": {
			commonstate.FakedContainerName: nil,
		},
	}

	policy.emitFinalPoolSizeMetrics(entries)

	actual := make(map[string]int64)
	for _, record := range emitter.records {
		require.Equal(t, util.MetricNamePoolSize, record.key)
		require.Equal(t, metrics.MetricTypeNameRaw, record.emitType)

		tags := make(map[string]string, len(record.tags))
		for _, tag := range record.tags {
			tags[tag.Key] = tag.Val
		}
		actual[tags["poolName"]+"/"+tags["numa_id"]] = record.val
	}
	require.Equal(t, map[string]int64{
		commonstate.PoolNameShare + "/0":   2,
		commonstate.PoolNameShare + "/1":   2,
		commonstate.PoolNameReclaim + "/0": 1,
		commonstate.PoolNameReclaim + "/1": 1,
	}, actual)
}

func newDynamicConfigWithEnableReclaim(enabled bool) *dynamicconfig.DynamicAgentConfiguration {
	conf := dynamicconfig.NewDynamicAgentConfiguration()
	dyn := conf.GetDynamicConfiguration()
	dyn.EnableReclaim = enabled
	conf.SetDynamicConfiguration(dyn)
	return conf
}
