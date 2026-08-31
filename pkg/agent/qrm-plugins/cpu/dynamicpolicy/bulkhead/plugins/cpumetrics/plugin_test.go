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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	pkgconsts "github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	metricfetcher "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

// capturedMetric records a single StoreFloat64 emission for assertions.
type capturedMetric struct {
	key  string
	val  float64
	tags map[string]string
}

// captureEmitter is a MetricEmitter that records every StoreFloat64 call so a
// test can assert the exact emitted metric name, value, and core_type tag.
type captureEmitter struct {
	sync.Mutex
	stored []capturedMetric
}

func (e *captureEmitter) StoreInt64(string, int64, metrics.MetricTypeName, ...metrics.MetricTag) error {
	return nil
}

func (e *captureEmitter) StoreFloat64(key string, val float64, _ metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	e.Lock()
	defer e.Unlock()
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		tagMap[tag.Key] = tag.Val
	}
	e.stored = append(e.stored, capturedMetric{key: key, val: val, tags: tagMap})
	return nil
}

func (e *captureEmitter) WithTags(string, ...metrics.MetricTag) metrics.MetricEmitter {
	return e
}

func (e *captureEmitter) Run(context.Context) {}

func (e *captureEmitter) snapshot() []capturedMetric {
	e.Lock()
	defer e.Unlock()
	out := make([]capturedMetric, len(e.stored))
	copy(out, e.stored)
	sort.Slice(out, func(i, j int) bool {
		if out[i].tags[coreTypeTagKey] != out[j].tags[coreTypeTagKey] {
			return out[i].tags[coreTypeTagKey] < out[j].tags[coreTypeTagKey]
		}
		return out[i].key < out[j].key
	})
	return out
}

// perCPUMetric describes one per-CPU sample to seed into the fake fetcher.
type perCPUMetric struct {
	cpu        int
	metricName string
	value      float64
}

func newFetcherWithMetrics(samples []perCPUMetric) metrictypes.MetricsFetcher {
	fetcher := metricfetcher.NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*metricfetcher.FakeMetricsFetcher)
	now := time.Now()
	for _, sample := range samples {
		fetcher.SetCPUMetric(sample.cpu, sample.metricName,
			utilmetric.MetricData{Value: sample.value, Time: &now})
	}
	return fetcher
}

func metaServerWith(fetcher metrictypes.MetricsFetcher) *metaserver.MetaServer {
	return &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{MetricsFetcher: fetcher},
	}
}

func appliedView(reclaim, nonReclaim machine.CPUSet) *model.AppliedView {
	view := model.NewCPUSetPartitionView()
	view.ReclaimEffective = reclaim
	view.NonReclaimPool = nonReclaim
	return &model.AppliedView{
		CPUSetPartitionView: view,
		Level:               model.AppliedViewLevelFull,
	}
}

func TestCPUMetricsPluginPeriodicalHandler(t *testing.T) {
	t.Parallel()

	// reclaim cores 2,3 and non-reclaim cores 0,1 with distinct per-CPU
	// values so avg (ratio metrics) and sum (l3misses) are unambiguous.
	samples := []perCPUMetric{
		{cpu: 0, metricName: pkgconsts.MetricCPUIOWaitRatio, value: 0.10},
		{cpu: 1, metricName: pkgconsts.MetricCPUIOWaitRatio, value: 0.30},
		{cpu: 2, metricName: pkgconsts.MetricCPUIOWaitRatio, value: 0.50},
		{cpu: 3, metricName: pkgconsts.MetricCPUIOWaitRatio, value: 0.70},

		{cpu: 0, metricName: pkgconsts.MetricCPUSchedwait, value: 10},
		{cpu: 1, metricName: pkgconsts.MetricCPUSchedwait, value: 30},
		{cpu: 2, metricName: pkgconsts.MetricCPUSchedwait, value: 50},
		{cpu: 3, metricName: pkgconsts.MetricCPUSchedwait, value: 70},

		{cpu: 0, metricName: pkgconsts.MetricCPUIrqRatio, value: 0.02},
		{cpu: 1, metricName: pkgconsts.MetricCPUIrqRatio, value: 0.04},
		{cpu: 2, metricName: pkgconsts.MetricCPUIrqRatio, value: 0.06},
		{cpu: 3, metricName: pkgconsts.MetricCPUIrqRatio, value: 0.08},

		{cpu: 0, metricName: pkgconsts.MetricCPUCPI, value: 1.0},
		{cpu: 1, metricName: pkgconsts.MetricCPUCPI, value: 3.0},
		{cpu: 2, metricName: pkgconsts.MetricCPUCPI, value: 5.0},
		{cpu: 3, metricName: pkgconsts.MetricCPUCPI, value: 7.0},

		{cpu: 0, metricName: pkgconsts.MetricCPUL3Misses, value: 100},
		{cpu: 1, metricName: pkgconsts.MetricCPUL3Misses, value: 200},
		{cpu: 2, metricName: pkgconsts.MetricCPUL3Misses, value: 400},
		{cpu: 3, metricName: pkgconsts.MetricCPUL3Misses, value: 800},
	}

	tests := []struct {
		name string
		ctx  func(fetcher metrictypes.MetricsFetcher, emitter metrics.MetricEmitter) bulkheadapi.PeriodicalHandlerContext
		want []capturedMetric
	}{
		{
			name: "valid applied view emits both core types aggregated per descriptor",
			ctx: func(fetcher metrictypes.MetricsFetcher, emitter metrics.MetricEmitter) bulkheadapi.PeriodicalHandlerContext {
				return bulkheadapi.PeriodicalHandlerContext{
					Emitter:                       emitter,
					MetaServer:                    metaServerWith(fetcher),
					AppliedView:                   appliedView(machine.NewCPUSet(2, 3), machine.NewCPUSet(0, 1)),
					AppliedViewValidForPeriodical: true,
				}
			},
			want: []capturedMetric{
				// non_reclaim: cores 0,1
				{key: metricBulkheadCoreCPI, val: 2.0, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
				{key: metricBulkheadCoreIOWaitRatio, val: 0.20, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
				{key: metricBulkheadCoreIrqRatio, val: 0.03, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
				{key: metricBulkheadCoreL3Misses, val: 300, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
				{key: metricBulkheadCoreSchedWait, val: 20, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
				// reclaim: cores 2,3
				{key: metricBulkheadCoreCPI, val: 6.0, tags: map[string]string{coreTypeTagKey: coreTypeReclaim}},
				{key: metricBulkheadCoreIOWaitRatio, val: 0.60, tags: map[string]string{coreTypeTagKey: coreTypeReclaim}},
				{key: metricBulkheadCoreIrqRatio, val: 0.07, tags: map[string]string{coreTypeTagKey: coreTypeReclaim}},
				{key: metricBulkheadCoreL3Misses, val: 1200, tags: map[string]string{coreTypeTagKey: coreTypeReclaim}},
				{key: metricBulkheadCoreSchedWait, val: 60, tags: map[string]string{coreTypeTagKey: coreTypeReclaim}},
			},
		},
		{
			name: "empty reclaim core set is skipped and only non_reclaim is emitted",
			ctx: func(fetcher metrictypes.MetricsFetcher, emitter metrics.MetricEmitter) bulkheadapi.PeriodicalHandlerContext {
				return bulkheadapi.PeriodicalHandlerContext{
					Emitter:                       emitter,
					MetaServer:                    metaServerWith(fetcher),
					AppliedView:                   appliedView(machine.NewCPUSet(), machine.NewCPUSet(0, 1)),
					AppliedViewValidForPeriodical: true,
				}
			},
			want: []capturedMetric{
				{key: metricBulkheadCoreCPI, val: 2.0, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
				{key: metricBulkheadCoreIOWaitRatio, val: 0.20, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
				{key: metricBulkheadCoreIrqRatio, val: 0.03, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
				{key: metricBulkheadCoreL3Misses, val: 300, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
				{key: metricBulkheadCoreSchedWait, val: 20, tags: map[string]string{coreTypeTagKey: coreTypeNonReclaim}},
			},
		},
		{
			name: "applied view not valid for periodical is a no-op",
			ctx: func(fetcher metrictypes.MetricsFetcher, emitter metrics.MetricEmitter) bulkheadapi.PeriodicalHandlerContext {
				return bulkheadapi.PeriodicalHandlerContext{
					Emitter:                       emitter,
					MetaServer:                    metaServerWith(fetcher),
					AppliedView:                   appliedView(machine.NewCPUSet(2, 3), machine.NewCPUSet(0, 1)),
					AppliedViewValidForPeriodical: false,
				}
			},
			want: nil,
		},
		{
			name: "nil applied view is a no-op",
			ctx: func(fetcher metrictypes.MetricsFetcher, emitter metrics.MetricEmitter) bulkheadapi.PeriodicalHandlerContext {
				return bulkheadapi.PeriodicalHandlerContext{
					Emitter:                       emitter,
					MetaServer:                    metaServerWith(fetcher),
					AppliedView:                   nil,
					AppliedViewValidForPeriodical: true,
				}
			},
			want: nil,
		},
		{
			name: "nil emitter is a no-op",
			ctx: func(fetcher metrictypes.MetricsFetcher, _ metrics.MetricEmitter) bulkheadapi.PeriodicalHandlerContext {
				return bulkheadapi.PeriodicalHandlerContext{
					Emitter:                       nil,
					MetaServer:                    metaServerWith(fetcher),
					AppliedView:                   appliedView(machine.NewCPUSet(2, 3), machine.NewCPUSet(0, 1)),
					AppliedViewValidForPeriodical: true,
				}
			},
			want: nil,
		},
		{
			name: "nil metrics fetcher is a no-op",
			ctx: func(_ metrictypes.MetricsFetcher, emitter metrics.MetricEmitter) bulkheadapi.PeriodicalHandlerContext {
				return bulkheadapi.PeriodicalHandlerContext{
					Emitter:                       emitter,
					MetaServer:                    metaServerWith(nil),
					AppliedView:                   appliedView(machine.NewCPUSet(2, 3), machine.NewCPUSet(0, 1)),
					AppliedViewValidForPeriodical: true,
				}
			},
			want: nil,
		},
		{
			name: "nil meta server is a no-op",
			ctx: func(_ metrictypes.MetricsFetcher, emitter metrics.MetricEmitter) bulkheadapi.PeriodicalHandlerContext {
				return bulkheadapi.PeriodicalHandlerContext{
					Emitter:                       emitter,
					MetaServer:                    nil,
					AppliedView:                   appliedView(machine.NewCPUSet(2, 3), machine.NewCPUSet(0, 1)),
					AppliedViewValidForPeriodical: true,
				}
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			plugin := &CPUMetricsPlugin{}
			fetcher := newFetcherWithMetrics(samples)
			emitter := &captureEmitter{}

			require.NoError(t, plugin.PeriodicalHandler(context.Background(), tt.ctx(fetcher, emitter)))

			got := emitter.snapshot()
			if len(tt.want) == 0 {
				require.Empty(t, got)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCPUMetricsPluginStaticContract(t *testing.T) {
	t.Parallel()

	plugin := NewCPUMetricsPlugin(nil)
	require.Equal(t, CPUMetricsPluginName, plugin.Name())
	require.True(t, plugin.Enable(bulkheadapi.HandlerContext{}))
	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{}))
	require.NoError(t, plugin.CPUSetAdjustmentDisabledHandler(context.Background(), bulkheadapi.HandlerContext{}))
}
