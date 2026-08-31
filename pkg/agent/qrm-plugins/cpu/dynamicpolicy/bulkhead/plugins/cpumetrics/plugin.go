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

// Package cpumetrics provides a read-only bulkhead plugin that samples per-CPU
// runtime-quality metrics for the reclaim and non-reclaim core sets and emits
// them aggregated per core type. It never mutates cgroup or topology state; it
// only reports the finalized (applied) partitioning, so it is gated on the
// manager's AppliedView finality signal.
package cpumetrics

import (
	"context"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/config"
	pkgconsts "github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

const CPUMetricsPluginName = "cpu_metrics"

// emitted metric names; reclaim / non_reclaim are distinguished by the
// core_type tag rather than by separate metric names, keeping metric
// cardinality bounded and consistent with the existing bulkhead partition
// metrics that tag by view/partition.
const (
	metricBulkheadCoreIOWaitRatio = "bulkhead_core_iowait_ratio"
	metricBulkheadCoreSchedWait   = "bulkhead_core_schedwait"
	metricBulkheadCoreIrqRatio    = "bulkhead_core_irq_ratio"
	metricBulkheadCoreCPI         = "bulkhead_core_cpi"
	metricBulkheadCoreL3Misses    = "bulkhead_core_l3misses"

	coreTypeTagKey     = "core_type"
	coreTypeReclaim    = "reclaim"
	coreTypeNonReclaim = "non_reclaim"
)

var _ bulkheadapi.Plugin = (*CPUMetricsPlugin)(nil)

// coreMetricDescriptor binds an emitted metric name to the per-CPU source
// metric and the aggregator used to fold it over a core set. Ratio-like
// metrics use avg (a per-core intensity), while L3Misses is a count and uses
// sum (a whole-set volume).
type coreMetricDescriptor struct {
	emitName   string
	sourceName string
	agg        utilmetric.Aggregator
}

var coreMetricDescriptors = []coreMetricDescriptor{
	{emitName: metricBulkheadCoreIOWaitRatio, sourceName: pkgconsts.MetricCPUIOWaitRatio, agg: utilmetric.AggregatorAvg},
	{emitName: metricBulkheadCoreSchedWait, sourceName: pkgconsts.MetricCPUSchedwait, agg: utilmetric.AggregatorAvg},
	{emitName: metricBulkheadCoreIrqRatio, sourceName: pkgconsts.MetricCPUIrqRatio, agg: utilmetric.AggregatorAvg},
	{emitName: metricBulkheadCoreCPI, sourceName: pkgconsts.MetricCPUCPI, agg: utilmetric.AggregatorAvg},
	{emitName: metricBulkheadCoreL3Misses, sourceName: pkgconsts.MetricCPUL3Misses, agg: utilmetric.AggregatorSum},
}

type CPUMetricsPlugin struct{}

func NewCPUMetricsPlugin(_ *config.Configuration) bulkheadapi.Plugin {
	return &CPUMetricsPlugin{}
}

func (p *CPUMetricsPlugin) Name() string {
	return CPUMetricsPluginName
}

// Enable follows the bulkhead global gate; this plugin owns no dynamic switch
// of its own. Returning true keeps it running whenever bulkhead is enabled.
func (p *CPUMetricsPlugin) Enable(bulkheadapi.HandlerContext) bool {
	return true
}

func (p *CPUMetricsPlugin) CPUSetAdjustmentHandler(context.Context, bulkheadapi.HandlerContext) error {
	return nil
}

func (p *CPUMetricsPlugin) CPUSetAdjustmentDisabledHandler(context.Context, bulkheadapi.HandlerContext) error {
	return nil
}

// PeriodicalHandler samples per-CPU metrics for the reclaim and non-reclaim
// core sets and emits them aggregated per core type. It reports only the
// finalized applied partitioning, so it no-ops unless the manager published a
// valid AppliedView in the latest adjustment round.
func (p *CPUMetricsPlugin) PeriodicalHandler(_ context.Context, in bulkheadapi.PeriodicalHandlerContext) error {
	if !in.AppliedViewValidForPeriodical || in.AppliedView == nil {
		general.InfofV(6, "bulkhead cpu_metrics: applied view not valid for periodical, skipping")
		return nil
	}
	if in.Emitter == nil {
		general.InfofV(6, "bulkhead cpu_metrics: nil emitter, skipping")
		return nil
	}
	fetcher := metricsFetcherFrom(in.MetaServer)
	if fetcher == nil {
		general.InfofV(6, "bulkhead cpu_metrics: nil metrics fetcher, skipping")
		return nil
	}

	p.emitForCoreSet(in.Emitter, fetcher, coreTypeReclaim, in.AppliedView.ReclaimEffective)
	p.emitForCoreSet(in.Emitter, fetcher, coreTypeNonReclaim, in.AppliedView.NonReclaimPool)
	return nil
}

// emitForCoreSet aggregates and emits every descriptor for a single core set.
// An empty core set is skipped so an absent partition does not report a
// misleading zero-valued sample.
func (p *CPUMetricsPlugin) emitForCoreSet(
	emitter metrics.MetricEmitter,
	fetcher metrictypes.MetricsFetcher,
	coreType string,
	cpus machine.CPUSet,
) {
	if cpus.IsEmpty() {
		general.InfofV(6, "bulkhead cpu_metrics: empty core set core_type=%s, skipping", coreType)
		return
	}
	for _, descriptor := range coreMetricDescriptors {
		data := fetcher.AggregateCoreMetric(cpus, descriptor.sourceName, descriptor.agg)
		_ = emitter.StoreFloat64(descriptor.emitName, data.Value, metrics.MetricTypeNameRaw,
			metrics.MetricTag{Key: coreTypeTagKey, Val: coreType},
		)
	}
}

func metricsFetcherFrom(ms *metaserver.MetaServer) metrictypes.MetricsFetcher {
	if ms == nil || ms.MetaAgent == nil {
		return nil
	}
	return ms.MetricsFetcher
}
