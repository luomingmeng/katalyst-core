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

package bulkhead

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	cpustate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	metricutil "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type fakePlugin struct {
	name                   string
	adjustViews            []*model.DesiredView
	adjustOwnedViews       []*model.CPUSetPartitionView
	adjustApplied          []*model.AppliedView
	adjustRevision         []uint64
	periodicCalls          int
	periodicStates         []interface{}
	periodicApplied        []*model.AppliedView
	periodicRevision       []uint64
	periodicValid          []bool
	disabledCalls          int
	enableStates           []interface{}
	enabled                bool
	adjustErr              error
	periodicErr            error
	disabledErr            error
	topologyResult         *bulkheadapi.TopologyResult
	afterReport            func()
	adjustStarted          chan struct{}
	adjustRelease          chan struct{}
	periodicWaitForContext bool
	periodicContextErr     error
}

type capturedMetric struct {
	key      string
	val      float64
	emitType metrics.MetricTypeName
	tags     []metrics.MetricTag
}

type capturingEmitter struct {
	records []capturedMetric
}

func (e *capturingEmitter) StoreInt64(key string, val int64, emitType metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	e.records = append(e.records, capturedMetric{
		key:      key,
		val:      float64(val),
		emitType: emitType,
		tags:     append([]metrics.MetricTag(nil), tags...),
	})
	return nil
}

func (e *capturingEmitter) StoreFloat64(key string, val float64, emitType metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	e.records = append(e.records, capturedMetric{
		key:      key,
		val:      val,
		emitType: emitType,
		tags:     append([]metrics.MetricTag(nil), tags...),
	})
	return nil
}

func (e *capturingEmitter) WithTags(string, ...metrics.MetricTag) metrics.MetricEmitter {
	return e
}

func (e *capturingEmitter) Run(context.Context) {}

func (p *fakePlugin) Name() string { return p.name }

func (p *fakePlugin) Enable(in bulkheadapi.HandlerContext) bool {
	p.enableStates = append(p.enableStates, in.State)
	return p.enabled
}

func (p *fakePlugin) CPUSetAdjustmentHandler(ctx context.Context, in bulkheadapi.HandlerContext) error {
	p.adjustViews = append(p.adjustViews, in.DesiredView)
	p.adjustOwnedViews = append(p.adjustOwnedViews, in.View)
	p.adjustApplied = append(p.adjustApplied, in.AppliedView)
	p.adjustRevision = append(p.adjustRevision, in.AppliedViewRevision)
	if p.topologyResult != nil && in.ReportTopologyResult != nil {
		if p.afterReport != nil {
			p.afterReport()
		}
		in.ReportTopologyResult(*p.topologyResult)
	}
	if p.adjustStarted != nil {
		close(p.adjustStarted)
	}
	if p.adjustRelease != nil {
		select {
		case <-p.adjustRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.adjustErr
}

type fakeTopologyPlugin struct {
	*fakePlugin
	result        bulkheadapi.DAGApplyResult
	err           error
	reportLegacy  bool
	afterApply    func()
	mutateDesired func(*model.DesiredView)
}

func (p *fakeTopologyPlugin) Apply(_ context.Context, in bulkheadapi.HandlerContext) (bulkheadapi.DAGApplyResult, error) {
	if p.reportLegacy && in.ReportTopologyResult != nil {
		in.ReportTopologyResult(bulkheadapi.TopologyResult{
			Converged:            p.result.FullyConverged,
			FinalSnapshotCurrent: p.result.FinalSnapshotCurrent,
			AppliedView:          p.result.AppliedView.DeepCopy(),
		})
	}
	if p.mutateDesired != nil {
		p.mutateDesired(in.DesiredView)
	}
	if p.afterApply != nil {
		p.afterApply()
	}
	return p.result, p.err
}

func (p *fakePlugin) PeriodicalHandler(
	ctx context.Context,
	in bulkheadapi.PeriodicalHandlerContext,
) error {
	p.periodicCalls++
	p.periodicApplied = append(p.periodicApplied, in.AppliedView)
	p.periodicRevision = append(p.periodicRevision, in.AppliedViewRevision)
	p.periodicValid = append(p.periodicValid, in.AppliedViewValidForPeriodical)
	if in.EffectiveEnabled == nil {
		p.periodicStates = append(p.periodicStates, nil)
	} else {
		p.periodicStates = append(p.periodicStates, *in.EffectiveEnabled)
	}
	if p.periodicWaitForContext {
		<-ctx.Done()
		p.periodicContextErr = ctx.Err()
		return ctx.Err()
	}
	return p.periodicErr
}

func (p *fakePlugin) CPUSetAdjustmentDisabledHandler(_ context.Context, _ bulkheadapi.HandlerContext) error {
	p.disabledCalls++
	return p.disabledErr
}

func TestManagerApplyLockWaitIsCancelable(t *testing.T) {
	blocking := &fakePlugin{
		name:          "blocking",
		enabled:       true,
		adjustStarted: make(chan struct{}),
		adjustRelease: make(chan struct{}),
	}
	m := &Manager{plugins: []bulkheadapi.Plugin{blocking}}
	firstDone := make(chan error, 1)
	go func() {
		_, err := m.Apply(context.Background(), enabledCPUSetAdjustmentCtx())
		firstDone <- err
	}()
	<-blocking.adjustStarted

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := m.Apply(ctx, enabledCPUSetAdjustmentCtx())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Apply error = %v, want context deadline while waiting for manager lock", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("canceled lock wait took %s", elapsed)
	}

	close(blocking.adjustRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Apply: %v", err)
	}
}

func TestManagerPeriodicalHandlerUsesBoundedContext(t *testing.T) {
	plugin := &fakePlugin{name: "periodical", periodicWaitForContext: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}
	coreConf := config.NewConfiguration()
	coreConf.CPUQRMPluginConfig.BulkheadConfiguration.TopologyConvergenceBudget.DeadlineDuration = time.Millisecond

	started := time.Now()
	m.RunPeriodicalHandlers(coreConf, nil, enabledDynamicAgentConf(), nil, nil)
	if !errors.Is(plugin.periodicContextErr, context.DeadlineExceeded) {
		t.Fatalf("periodical context error = %v, want deadline exceeded", plugin.periodicContextErr)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded periodical handler took %s", elapsed)
	}
}

func TestManagerApplyRequiresFullyConvergedTopologyBeforeDependents(t *testing.T) {
	t.Parallel()

	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			FullyConverged:       false,
			FinalSnapshotCurrent: true,
		},
	}
	dependent := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin, dependent}}

	got, err := m.Apply(context.Background(), enabledCPUSetAdjustmentCtx())
	var nonConverged *NonConvergedError
	if !errors.As(err, &nonConverged) {
		t.Fatalf("Apply() error = %v, want *NonConvergedError", err)
	}
	if !got.IsEmpty() {
		t.Fatalf("Apply() reclaim = %s, want empty on non-convergence", got.String())
	}
	if len(dependent.adjustViews) != 0 {
		t.Fatalf("dependent calls = %d, want 0", len(dependent.adjustViews))
	}
	if !m.LatestAppliedReclaim().IsEmpty() {
		t.Fatalf("non-converged apply published reclaim %s", m.LatestAppliedReclaim().String())
	}
}

func TestManagerApplyAcceptsParentSafeTopologyWithoutRunningDependents(t *testing.T) {
	t.Parallel()

	applied := &model.AppliedView{
		Level: model.AppliedViewLevelParentSafe,
		CPUSetPartitionView: model.CPUSetPartitionView{
			ReclaimEffective: machine.NewCPUSet(2, 3),
		},
	}
	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			ParentSafe:           true,
			DeferredLeafCount:    1,
			FinalSnapshotCurrent: true,
			AppliedView:          applied,
		},
	}
	dependent := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin, dependent}}

	got, err := m.Apply(context.Background(), enabledCPUSetAdjustmentCtx())
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !got.Equals(machine.NewCPUSet(2, 3)) {
		t.Fatalf("Apply() reclaim = %s, want 2-3", got.String())
	}
	if len(dependent.adjustViews) != 0 {
		t.Fatalf("dependent calls = %d, want none for parent-safe view", len(dependent.adjustViews))
	}
	if m.appliedViewValidForPeriodical {
		t.Fatal("parent-safe view must not authorize periodical leaf-dependent plugins")
	}
}

func TestManagerApplyAcceptsExactEmptyNUMABucketBeforePluginWrites(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(
		commonstate.PoolNameReserve,
		commonstate.FakedContainerName,
		&cpustate.AllocationInfo{
			AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
			AllocationResult: machine.NewCPUSet(0),
		},
	)
	state.SetAllocationInfo(
		commonstate.PoolNameReclaim,
		commonstate.FakedContainerName,
		&cpustate.AllocationInfo{
			AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
			AllocationResult: machine.NewCPUSet(1),
		},
	)
	_, topology := testBulkheadStateAndTopology()
	plugin := &fakePlugin{name: "writer", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}

	got, err := m.Apply(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	})
	if err != nil {
		t.Fatalf("Apply() error = %v, want exact empty NUMA bucket accepted", err)
	}
	if !got.IsEmpty() {
		t.Fatalf("Apply() reclaim = %s, want empty without topology owner", got.String())
	}
	if len(plugin.enableStates) != 1 || len(plugin.adjustViews) != 1 {
		t.Fatalf("valid exact NUMA projection did not reach plugin: enable=%d adjust=%d",
			len(plugin.enableStates), len(plugin.adjustViews))
	}
}

func TestManagerApplyPassesOwnedVerifiedViewToDependentsAndReturnsReclaim(t *testing.T) {
	t.Parallel()

	verified := machine.NewCPUSet(2, 3)
	applied := &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
		NonReclaimPool:   machine.NewCPUSet(0),
		ReclaimEffective: verified,
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(),
			1: verified,
		},
	}}
	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			FullyConverged:       true,
			FinalSnapshotCurrent: true,
			AppliedView:          applied,
		},
	}
	dependent := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin, dependent}}

	got, err := m.Apply(context.Background(), enabledCPUSetAdjustmentCtx())
	if err != nil {
		t.Fatalf("Apply() error: %v", err)
	}
	if !got.Equals(verified) {
		t.Fatalf("Apply() reclaim = %s, want %s", got.String(), verified.String())
	}
	if len(dependent.adjustOwnedViews) != 1 || dependent.adjustOwnedViews[0] == nil {
		t.Fatalf("dependent owned views = %#v, want one explicit view", dependent.adjustOwnedViews)
	}
	if !dependent.adjustOwnedViews[0].ReclaimEffective.Equals(verified) {
		t.Fatalf("dependent reclaim = %s, want %s",
			dependent.adjustOwnedViews[0].ReclaimEffective.String(), verified.String())
	}
	if !dependent.adjustOwnedViews[0].NonReclaimPool.Equals(machine.NewCPUSet(0)) {
		t.Fatalf("dependent non-reclaim = %s, want final-snapshot value 0",
			dependent.adjustOwnedViews[0].NonReclaimPool.String())
	}
	if !dependent.adjustOwnedViews[0].ReclaimEffectivePerNUMA[1].Equals(verified) {
		t.Fatalf("dependent reclaim NUMA bucket = %s, want final-snapshot value %s",
			dependent.adjustOwnedViews[0].ReclaimEffectivePerNUMA[1].String(), verified.String())
	}
	if !m.LatestAppliedReclaim().Equals(verified) {
		t.Fatalf("latest reclaim = %s, want %s", m.LatestAppliedReclaim().String(), verified.String())
	}
	applied.NonReclaimPool.Add(1)
	if dependent.adjustOwnedViews[0].NonReclaimPool.Contains(1) {
		t.Fatal("dependent view aliases topology result AppliedView")
	}
}

func TestManagerApplyPublishesPartitionMetricsAfterAppliedViewCommit(t *testing.T) {
	t.Parallel()

	state, topology := testBulkheadStateAndTopology()
	applied := model.NewDesiredView().ToAppliedView()
	applied.Reserve = machine.NewCPUSet(0)
	applied.ReclaimEffective = machine.NewCPUSet(1, 2, 3)
	applied.NonReclaimPool = machine.NewCPUSet(0)
	applied.SharePoolMap[commonstate.PoolNameShare] = machine.NewCPUSet()
	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			FullyConverged:       true,
			FinalSnapshotCurrent: true,
			AppliedView:          applied,
		},
	}
	emitter := &capturingEmitter{}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin}}
	in := enabledCPUSetAdjustmentCtx()
	in.DynamicConf.FillDefaultSharePoolWithNonReclaimCPUs = true
	in.State = state
	in.Topology = topology
	in.Emitter = emitter

	if _, err := m.Apply(context.Background(), in); err != nil {
		t.Fatalf("Apply() error: %v", err)
	}
	for _, viewName := range []string{"desired", "applied"} {
		if !hasMetricTags(emitter.records, metricBulkheadPartitionCPUCores, "view", viewName) {
			t.Fatalf("partition metrics missing view=%s: %#v", viewName, emitter.records)
		}
		if !hasMetricTags(emitter.records, metricBulkheadDefaultShareResidualCPUCores, "view", viewName) {
			t.Fatalf("default-share residual metric missing view=%s: %#v", viewName, emitter.records)
		}
	}
	if !hasMetric(emitter.records, metricBulkheadPartitionCPUDiffCores) {
		t.Fatalf("partition diff metrics missing after committed applied view: %#v", emitter.records)
	}
}

func TestManagerApplyPartitionMetricsUsePostTopologyDesiredView(t *testing.T) {
	t.Parallel()

	state, topology := testBulkheadStateAndTopology()
	applied := model.NewDesiredView().ToAppliedView()
	applied.Reserve = machine.NewCPUSet(0)
	applied.ReclaimEffective = machine.NewCPUSet(1, 2)
	applied.NonReclaimPool = machine.NewCPUSet(3)
	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			FullyConverged:       true,
			FinalSnapshotCurrent: true,
			AppliedView:          applied,
		},
		mutateDesired: func(desired *model.DesiredView) {
			desired.ReclaimEffective = machine.NewCPUSet(1, 2)
			desired.NonReclaimPool = machine.NewCPUSet(3)
		},
	}
	emitter := &capturingEmitter{}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin}}
	in := enabledCPUSetAdjustmentCtx()
	in.State = state
	in.Topology = topology
	in.Emitter = emitter

	if _, err := m.Apply(context.Background(), in); err != nil {
		t.Fatalf("Apply() error: %v", err)
	}
	want := map[string]float64{
		metricKey(metricBulkheadPartitionCPUCores, "view", "desired", "partition", "reclaim"):               2,
		metricKey(metricBulkheadPartitionCPUDiffCores, "partition", "reclaim", "direction", "desired_only"): 0,
		metricKey(metricBulkheadPartitionCPUDiffCores, "partition", "reclaim", "direction", "applied_only"): 0,
	}
	assertMetricValues(t, emitter.records, want)
}

func TestManagerApplyDoesNotPublishResidualMetricWhenBackfillDisabled(t *testing.T) {
	t.Parallel()

	state, topology := testBulkheadStateAndTopology()
	emitter := &capturingEmitter{}
	m := &Manager{plugins: []bulkheadapi.Plugin{
		&fakePlugin{name: "writer", enabled: true},
	}}
	in := enabledCPUSetAdjustmentCtx()
	in.State = state
	in.Topology = topology
	in.Emitter = emitter

	if _, err := m.Apply(context.Background(), in); err != nil {
		t.Fatalf("Apply() error: %v", err)
	}
	if hasMetric(emitter.records, metricBulkheadDefaultShareResidualCPUCores) {
		t.Fatalf("disabled backfill published residual metrics: %#v", emitter.records)
	}
	if !hasMetricTags(emitter.records, metricBulkheadPartitionCPUCores, "view", "desired", "partition", "default_share") {
		t.Fatalf("disabled backfill must retain generic default-share partition metric: %#v", emitter.records)
	}
}

func TestManagerApplyDoesNotPublishAppliedPartitionMetricsWhenTopologyDoesNotConverge(t *testing.T) {
	t.Parallel()

	state, topology := testBulkheadStateAndTopology()
	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			FinalSnapshotCurrent: true,
			AppliedView:          model.NewDesiredView().ToAppliedView(),
		},
	}
	emitter := &capturingEmitter{}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin}}
	in := enabledCPUSetAdjustmentCtx()
	in.State = state
	in.Topology = topology
	in.Emitter = emitter

	if _, err := m.Apply(context.Background(), in); err == nil {
		t.Fatal("Apply() error = nil, want non-converged result rejected")
	}
	if !hasMetricTags(emitter.records, metricBulkheadPartitionCPUCores, "view", "desired") {
		t.Fatalf("desired partition metrics missing: %#v", emitter.records)
	}
	if hasMetricTags(emitter.records, metricBulkheadPartitionCPUCores, "view", "applied") {
		t.Fatalf("non-converged result published applied partition metrics: %#v", emitter.records)
	}
	if hasMetric(emitter.records, metricBulkheadPartitionCPUDiffCores) {
		t.Fatalf("non-converged result published diff metrics: %#v", emitter.records)
	}
}

func TestManagerLatestAppliedReclaimPublishesAndReturnsClones(t *testing.T) {
	t.Parallel()

	m := &Manager{}
	source := machine.NewCPUSet(1, 2)
	m.publishLatestAppliedReclaim(source)
	source.Add(3)

	first := m.LatestAppliedReclaim()
	if !first.Equals(machine.NewCPUSet(1, 2)) {
		t.Fatalf("first latest reclaim = %s, want 1-2", first.String())
	}
	first.Add(4)
	second := m.LatestAppliedReclaim()
	if !second.Equals(machine.NewCPUSet(1, 2)) {
		t.Fatalf("second latest reclaim = %s, want 1-2", second.String())
	}
}

func TestManagerApplyDoesNotPublishTypedTopologyResultBeforeDependentsSucceed(t *testing.T) {
	t.Parallel()

	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin:   &fakePlugin{name: "cpuset_topology", enabled: true},
		reportLegacy: true,
		result: bulkheadapi.DAGApplyResult{
			FullyConverged:       true,
			FinalSnapshotCurrent: true,
			AppliedView: &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
				ReclaimEffective: machine.NewCPUSet(2, 3),
			}},
		},
	}
	dependent := &fakePlugin{name: "workqueue", enabled: true, adjustErr: errors.New("dependent failed")}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin, dependent}}
	state, topology := testBulkheadStateAndTopology()
	in := enabledCPUSetAdjustmentCtx()
	in.State = state
	in.Topology = topology

	if _, err := m.Apply(context.Background(), in); err == nil {
		t.Fatal("Apply() error = nil, want dependent failure")
	}
	if m.appliedView != nil {
		t.Fatalf("failed transaction published applied view: %#v", m.appliedView)
	}
	if m.appliedViewRevision != 0 {
		t.Fatalf("failed transaction revision = %d, want 0", m.appliedViewRevision)
	}
	if !m.LatestAppliedReclaim().IsEmpty() {
		t.Fatalf("failed transaction published reclaim %s", m.LatestAppliedReclaim().String())
	}
}

func TestManagerApplyRejectsStaleGenerationBeforeDependentSideEffects(t *testing.T) {
	t.Parallel()

	oldApplied := &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
		ReclaimEffective: machine.NewCPUSet(3),
	}}
	newApplied := &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
		ReclaimEffective: machine.NewCPUSet(1, 2),
	}}
	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			FullyConverged:       true,
			FinalSnapshotCurrent: true,
			AppliedView:          newApplied,
		},
	}
	dependent := &fakePlugin{name: "rdt_cpulist", enabled: true}
	m := &Manager{
		plugins:                       []bulkheadapi.Plugin{topologyPlugin, dependent},
		appliedView:                   oldApplied,
		appliedViewRevision:           7,
		appliedViewValidForPeriodical: true,
	}
	m.publishLatestAppliedReclaim(oldApplied.ReclaimEffective)
	state, topology := testBulkheadStateAndTopology()
	fenceCalls := 0
	emitter := &capturingEmitter{}

	_, err := m.Apply(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
		Emitter:     emitter,
		Generation:  11,
		CommitIfGenerationCurrent: func(generation uint64, commit func()) bool {
			if generation != 11 {
				t.Fatalf("commit generation = %d, want 11", generation)
			}
			fenceCalls++
			if fenceCalls <= 2 {
				commit()
				return true
			}
			return false
		},
	})
	var nonConverged *NonConvergedError
	if !errors.As(err, &nonConverged) {
		t.Fatalf("Apply() error = %v, want stale generation rejected as *NonConvergedError", err)
	}
	if got := len(dependent.adjustViews); got != 0 {
		t.Fatalf("dependent calls = %d, want stale generation rejected before dependent side effects", got)
	}
	if m.appliedViewRevision != 7 {
		t.Fatalf("applied revision = %d, want old revision 7 retained", m.appliedViewRevision)
	}
	assertCPUSet(t, "retained applied reclaim", m.appliedView.ReclaimEffective, "3")
	assertCPUSet(t, "retained latest reclaim", m.LatestAppliedReclaim(), "3")
	if m.appliedViewValidForPeriodical {
		t.Fatal("stale generation must not authorize periodical handlers")
	}
	if hasMetricTags(emitter.records, metricBulkheadPartitionCPUCores, "view", "applied") {
		t.Fatalf("stale generation published applied partition metrics: %#v", emitter.records)
	}
	if hasMetric(emitter.records, metricBulkheadPartitionCPUDiffCores) {
		t.Fatalf("stale generation published partition diff metrics: %#v", emitter.records)
	}
}

func TestManagerApplyRejectsStaleGenerationBeforeSideEffects(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "rdt_cpulist", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}

	_, err := m.Apply(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		Generation:  10,
		CommitIfGenerationCurrent: func(uint64, func()) bool {
			return false
		},
	})
	var nonConverged *NonConvergedError
	if !errors.As(err, &nonConverged) {
		t.Fatalf("Apply() error = %v, want stale generation rejected as *NonConvergedError", err)
	}
	if got := len(plugin.adjustViews); got != 0 {
		t.Fatalf("plugin calls = %d, want stale round rejected before side effects", got)
	}
}

func TestManagerApplyChecksGenerationAfterFailedSideEffect(t *testing.T) {
	t.Parallel()

	pluginErr := errors.New("partial side effect failed")
	plugin := &fakePlugin{name: "rdt_cpulist", enabled: true, adjustErr: pluginErr}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}
	fenceCalls := 0

	_, err := m.Apply(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		Generation:  10,
		CommitIfGenerationCurrent: func(uint64, func()) bool {
			fenceCalls++
			return fenceCalls <= 2
		},
	})

	var nonConverged *NonConvergedError
	if !errors.As(err, &nonConverged) {
		t.Fatalf("Apply() error = %v, want stale generation after failed side effect", err)
	}
	if fenceCalls != 3 {
		t.Fatalf("generation fence calls = %d, want entry, pre-, and post-side-effect checks", fenceCalls)
	}
	if got := len(plugin.adjustViews); got != 1 {
		t.Fatalf("plugin calls = %d, want failed side effect to run once", got)
	}
}

func TestManagerApplyChecksGenerationAfterDisabledHandlerError(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{
		name:        "rdt_cpulist",
		enabled:     false,
		disabledErr: errors.New("partial disabled reset failed"),
	}
	m := &Manager{
		plugins:                     []bulkheadapi.Plugin{plugin},
		lastCPUSetAdjustmentEnabled: map[string]bool{plugin.name: true},
	}
	fenceCalls := 0

	_, err := m.Apply(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		Generation:  10,
		CommitIfGenerationCurrent: func(uint64, func()) bool {
			fenceCalls++
			return fenceCalls <= 2
		},
	})

	var nonConverged *NonConvergedError
	if !errors.As(err, &nonConverged) {
		t.Fatalf("Apply() error = %v, want stale generation after failed disabled reset", err)
	}
	if fenceCalls != 3 {
		t.Fatalf("generation fence calls = %d, want entry, pre-, and post-disabled checks", fenceCalls)
	}
	if plugin.disabledCalls != 1 {
		t.Fatalf("disabled calls = %d, want failed reset to run once", plugin.disabledCalls)
	}
}

func TestManagerApplyChecksGenerationAfterTopologyError(t *testing.T) {
	t.Parallel()

	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		err:        errors.New("partial topology apply failed"),
	}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin}}
	fenceCalls := 0

	_, err := m.Apply(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		Generation:  10,
		CommitIfGenerationCurrent: func(uint64, func()) bool {
			fenceCalls++
			return fenceCalls <= 2
		},
	})

	var nonConverged *NonConvergedError
	if !errors.As(err, &nonConverged) {
		t.Fatalf("Apply() error = %v, want stale generation after failed topology apply", err)
	}
	if fenceCalls != 3 {
		t.Fatalf("generation fence calls = %d, want entry, pre-, and post-topology checks", fenceCalls)
	}
}

func TestManagerApplyRejectsTypedTopologyResultWhenDesiredViewChanges(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(1, 2, 3),
	})
	_, topology := testBulkheadStateAndTopology()
	oldApplied := &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
		ReclaimEffective: machine.NewCPUSet(3),
	}}
	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			FullyConverged:       true,
			FinalSnapshotCurrent: true,
			AppliedView: &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
				ReclaimEffective: machine.NewCPUSet(1),
			}},
		},
		afterApply: func() {
			state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(2, 3),
			})
		},
	}
	dependent := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{
		plugins:             []bulkheadapi.Plugin{topologyPlugin, dependent},
		appliedView:         oldApplied,
		appliedViewRevision: 7,
	}

	_, err := m.Apply(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	})
	var nonConverged *NonConvergedError
	if !errors.As(err, &nonConverged) {
		t.Fatalf("Apply() error = %v, want stale typed result rejected as *NonConvergedError", err)
	}
	if got := len(dependent.adjustViews); got != 0 {
		t.Fatalf("dependent calls = %d, want 0 after desired view changes", got)
	}
	if m.appliedViewRevision != 7 {
		t.Fatalf("applied revision = %d, want old revision 7 retained", m.appliedViewRevision)
	}
	assertCPUSet(t, "retained applied reclaim", m.appliedView.ReclaimEffective, "3")
}

func TestManagerApplyRetainsRevisionForConsecutiveIdenticalTypedAppliedViews(t *testing.T) {
	t.Parallel()

	state, topology := testBulkheadStateAndTopology()
	applied := &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
		NonReclaimPool:   machine.NewCPUSet(0),
		ReclaimEffective: machine.NewCPUSet(1, 2, 3),
	}}
	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			FullyConverged:       true,
			FinalSnapshotCurrent: true,
			AppliedView:          applied,
		},
	}
	dependent := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin, dependent}}
	in := cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	}

	if _, err := m.Apply(context.Background(), in); err != nil {
		t.Fatalf("first Apply() error: %v", err)
	}
	if _, err := m.Apply(context.Background(), in); err != nil {
		t.Fatalf("second Apply() error: %v", err)
	}
	if m.appliedViewRevision != 1 {
		t.Fatalf("applied revision = %d, want 1 for identical payloads", m.appliedViewRevision)
	}
	if !reflect.DeepEqual(dependent.adjustRevision, []uint64{1, 1}) {
		t.Fatalf("dependent revisions = %v, want [1 1]", dependent.adjustRevision)
	}
}

func TestRunCPUSetAdjustmentHandlersCallsEnabledPluginEveryRun(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if got := len(plugin.adjustViews); got != 2 {
		t.Fatalf("manager should not skip enabled plugin, got %d calls", got)
	}
}

func TestRunCPUSetAdjustmentHandlersReconcilesWhenNonReclaimPoolMinSizeChanges(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}
	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	// SysAdvisor emits a reclaim affinity budget (ReclaimRaw). QRM bounds the
	// effective reclaim set by this budget and derives non-reclaim from the
	// remainder, so the min-size padding pulls CPUs back out of advisor reclaim.
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(1, 2, 3),
	})
	topology := &machine.CPUTopology{
		NumCPUs:      4,
		NumCores:     4,
		NumSockets:   1,
		NumNUMANodes: 1,
		CPUDetails: machine.CPUDetails{
			0: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			1: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			2: {NUMANodeID: 0, SocketID: 0, CoreID: 2},
			3: {NUMANodeID: 0, SocketID: 0, CoreID: 3},
		},
	}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConfWithMinSize(true, 0),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConfWithMinSize(true, 2),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if got := len(plugin.adjustViews); got != 2 {
		t.Fatalf("min size change should trigger plugin, got %d calls", got)
	}
	assertCPUSet(t, "first non reclaim", plugin.adjustViews[0].NonReclaimPool, "")
	assertCPUSet(t, "second non reclaim", plugin.adjustViews[1].NonReclaimPool, "1-2")
}

func TestRunCPUSetAdjustmentHandlersUsesDefaultNonReclaimPoolMinSize(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true}
	m := &Manager{
		plugins:                      []bulkheadapi.Plugin{plugin},
		defaultNonReclaimPoolMinSize: 2,
	}
	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	// Advisor reclaim budget: effective reclaim is bounded by this set and the
	// default min-size pads non-reclaim back out of it.
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(1, 2, 3),
	})
	topology := &machine.CPUTopology{
		NumCPUs:      4,
		NumCores:     4,
		NumSockets:   1,
		NumNUMANodes: 1,
		CPUDetails: machine.CPUDetails{
			0: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			1: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			2: {NUMANodeID: 0, SocketID: 0, CoreID: 2},
			3: {NUMANodeID: 0, SocketID: 0, CoreID: 3},
		},
	}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConfWithMinSize(true, 0),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := len(plugin.adjustViews); got != 1 {
		t.Fatalf("adjust calls = %d, want 1", got)
	}
	assertCPUSet(t, "non reclaim", plugin.adjustViews[0].NonReclaimPool, "1-2")
	assertCPUSet(t, "reclaim effective", plugin.adjustViews[0].ReclaimEffective, "3")
}

func TestRunCPUSetAdjustmentHandlersPassesHandlerContextToEnable(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}
	in := cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       cpustate.NewCPUPluginState(nil),
	}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), in); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if len(plugin.enableStates) != 1 {
		t.Fatalf("Enable calls = %d, want 1", len(plugin.enableStates))
	}
	if plugin.enableStates[0] != in.State {
		t.Fatalf("Enable did not receive handler context state")
	}
}

func TestRunCPUSetAdjustmentHandlersSkipsAllPluginLogicWhenBulkheadDisabled(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true}
	m := &Manager{
		plugins:                     []bulkheadapi.Plugin{plugin},
		lastCPUSetAdjustmentEnabled: map[string]bool{plugin.Name(): true},
	}

	err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(false),
		State:       cpustate.NewCPUPluginState(nil),
	})
	if err != nil {
		t.Fatalf("RunCPUSetAdjustmentHandlers failed: %v", err)
	}
	if len(plugin.enableStates) != 0 {
		t.Fatalf("plugin Enable calls = %d, want 0", len(plugin.enableStates))
	}
	if len(plugin.adjustViews) != 0 {
		t.Fatalf("adjust calls = %d, want 0", len(plugin.adjustViews))
	}
	if plugin.disabledCalls != 0 {
		t.Fatalf("disabled calls = %d, want 0", plugin.disabledCalls)
	}
	if m.lastCPUSetAdjustmentEnabled != nil {
		t.Fatalf("last enabled state should be cleared when bulkhead is globally disabled")
	}
}

func TestRunCPUSetAdjustmentHandlersReconcilesAfterBulkheadReenabledWithSameView(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("first enabled run failed: %v", err)
	}
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(false),
	}); err != nil {
		t.Fatalf("disabled bulkhead run failed: %v", err)
	}
	if plugin.disabledCalls != 0 {
		t.Fatalf("disabled calls = %d, want 0", plugin.disabledCalls)
	}
	if len(plugin.enableStates) != 1 {
		t.Fatalf("plugin Enable calls = %d, want 1", len(plugin.enableStates))
	}
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("second enabled run failed: %v", err)
	}
	if got := len(plugin.adjustViews); got != 2 {
		t.Fatalf("adjust calls = %d, want 2", got)
	}
}

func TestRunCPUSetAdjustmentHandlersCallsDisabledTransitionWhenPluginDisabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		pluginEnabled      bool
		preRunEnabledState bool
		wantAdjustCalls    int
		wantDisabledCalls  int
	}{
		{
			name:            "plugin enabled",
			pluginEnabled:   true,
			wantAdjustCalls: 1,
		},
		{
			name:              "plugin disabled without previous enabled state",
			wantDisabledCalls: 1,
		},
		{
			name:               "bulkhead disabled after previous enabled",
			preRunEnabledState: true,
			wantDisabledCalls:  1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			plugin := &fakePlugin{name: "fake", enabled: tt.pluginEnabled}
			m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}
			if tt.preRunEnabledState {
				m.lastCPUSetAdjustmentEnabled = map[string]bool{plugin.Name(): true}
			}

			err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
				DynamicConf: dynamicBulkheadConf(true),
			})
			if err != nil {
				t.Fatalf("RunCPUSetAdjustmentHandlers failed: %v", err)
			}
			if got := len(plugin.adjustViews); got != tt.wantAdjustCalls {
				t.Fatalf("adjust calls = %d, want %d", got, tt.wantAdjustCalls)
			}
			if plugin.disabledCalls != tt.wantDisabledCalls {
				t.Fatalf("disabled calls = %d, want %d", plugin.disabledCalls, tt.wantDisabledCalls)
			}
		})
	}
}

func TestRunCPUSetAdjustmentHandlersResetsDisabledPluginWhenLastEnabledNil(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: false}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("first disabled run failed: %v", err)
	}
	if plugin.disabledCalls != 1 {
		t.Fatalf("disabled calls = %d, want 1", plugin.disabledCalls)
	}
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("stable disabled run failed: %v", err)
	}
	if plugin.disabledCalls != 1 {
		t.Fatalf("stable disabled calls = %d, want 1", plugin.disabledCalls)
	}
	if len(plugin.adjustViews) != 0 {
		t.Fatalf("adjust calls = %d, want 0", len(plugin.adjustViews))
	}
	if got := m.lastCPUSetAdjustmentEnabled[plugin.Name()]; got {
		t.Fatalf("last enabled state = %t, want false", got)
	}
}

func TestRunCPUSetAdjustmentHandlersDoesNotCacheFailedView(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true, adjustErr: errors.New("boom")}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err == nil {
		t.Fatal("expected first run to fail")
	}
	plugin.adjustErr = nil
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if got := len(plugin.adjustViews); got != 2 {
		t.Fatalf("expected failed view not cached, got %d calls", got)
	}
}

func TestRunCPUSetAdjustmentHandlersPublishesAppliedViewAfterTopologyConverges_BitsUT(t *testing.T) {
	t.Parallel()

	topologyApplied := &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
		NonReclaimPool:   machine.NewCPUSet(0, 2, 3),
		ReclaimEffective: machine.NewCPUSet(1),
	}}
	topologyPlugin := &fakePlugin{name: "cpuset_topology", enabled: true, topologyResult: &bulkheadapi.TopologyResult{
		Converged:            true,
		FinalSnapshotCurrent: true,
		AppliedView:          topologyApplied,
	}}
	consumer := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin, consumer}}
	state, topology := testBulkheadStateAndTopology()

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got := len(consumer.adjustApplied); got != 1 {
		t.Fatalf("consumer applied view calls = %d, want 1", got)
	}
	if consumer.adjustApplied[0] == nil {
		t.Fatalf("consumer should receive published AppliedView")
	}
	if consumer.adjustRevision[0] != 1 {
		t.Fatalf("consumer applied revision = %d, want 1", consumer.adjustRevision[0])
	}
	if topologyPlugin.adjustApplied[0] != nil {
		t.Fatalf("topology plugin should see previous applied view before publish")
	}
	assertCPUSet(t, "consumer applied reclaim from final snapshot", consumer.adjustApplied[0].ReclaimEffective, "1")

	consumer.adjustApplied[0].ReclaimEffective.Add(99)
	if m.appliedView.ReclaimEffective.Contains(99) {
		t.Fatalf("mutating consumer AppliedView copy should not mutate manager published view")
	}
}

func TestRunCPUSetAdjustmentHandlersABCLifecyclePreservesBOnPlanningFailure(t *testing.T) {
	t.Parallel()

	reclaimB := machine.MustParse("9-13,20-21,36,39-40,42,57,59-60,68,84,87-88,90")
	reclaimC := machine.MustParse("13-18,20-21,35-40,42,62-66,68,83-88,90")
	topologyPlugin := &fakeTopologyPlugin{
		fakePlugin: &fakePlugin{name: "cpuset_topology", enabled: true},
		result: bulkheadapi.DAGApplyResult{
			FullyConverged:       true,
			FinalSnapshotCurrent: true,
			AppliedView: &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
				Reserve:          machine.NewCPUSet(0, 24),
				ReclaimEffective: reclaimB,
			}},
		},
	}
	consumer := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin, consumer}}
	state, topology := testBulkheadStateAndTopology()
	run := func() error {
		return m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
			DynamicConf: dynamicBulkheadConf(true),
			State:       state,
			Topology:    topology,
		})
	}

	if err := run(); err != nil {
		t.Fatalf("publish B: %v", err)
	}
	assertCPUSet(t, "applied B", m.appliedView.ReclaimEffective, reclaimB.String())
	if m.appliedViewRevision != 1 {
		t.Fatalf("B revision = %d, want 1", m.appliedViewRevision)
	}

	topologyPlugin.err = errors.New("plan C: deadlock probe budget exceeded")
	if err := run(); err == nil {
		t.Fatal("planning C should fail")
	}
	assertCPUSet(t, "applied remains B", m.appliedView.ReclaimEffective, reclaimB.String())
	if m.appliedViewRevision != 1 {
		t.Fatalf("failed C revision = %d, want 1", m.appliedViewRevision)
	}
	if got := len(consumer.adjustApplied); got != 1 {
		t.Fatalf("consumer calls after failed C = %d, want 1", got)
	}

	topologyPlugin.err = nil
	topologyPlugin.result = bulkheadapi.DAGApplyResult{
		FullyConverged:       true,
		FinalSnapshotCurrent: true,
		AppliedView: &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
			Reserve:          machine.NewCPUSet(0, 24),
			ReclaimEffective: reclaimC,
		}},
	}
	if err := run(); err != nil {
		t.Fatalf("publish C: %v", err)
	}
	assertCPUSet(t, "applied C", m.appliedView.ReclaimEffective, reclaimC.String())
	assertCPUSet(t, "reserve C", m.appliedView.Reserve, "0,24")
	if m.appliedViewRevision != 2 {
		t.Fatalf("C revision = %d, want 2", m.appliedViewRevision)
	}
	if got := len(consumer.adjustApplied); got != 2 {
		t.Fatalf("consumer calls after C = %d, want 2", got)
	}
	assertCPUSet(t, "consumer sees C", consumer.adjustApplied[1].ReclaimEffective, reclaimC.String())
}

func TestRunCPUSetAdjustmentHandlersShortCircuitsConsumersUntilTopologyConverges_BitsUT(t *testing.T) {
	t.Parallel()

	topologyPlugin := &fakePlugin{name: "cpuset_topology", enabled: true}
	consumer := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin, consumer}}
	state, topology := testBulkheadStateAndTopology()

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got := len(consumer.adjustViews); got != 0 {
		t.Fatalf("consumer calls = %d, want 0 before topology convergence", got)
	}
	if m.appliedView != nil || m.appliedViewRevision != 0 {
		t.Fatalf("manager should not publish applied view before convergence, view=%v revision=%d", m.appliedView, m.appliedViewRevision)
	}
}

func TestRunCPUSetAdjustmentHandlersRetainsRevisionWhenTopologyResultIsNotCurrent_BitsUT(t *testing.T) {
	t.Parallel()

	oldApplied := &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
		ReclaimEffective: machine.NewCPUSet(3),
	}}
	topologyPlugin := &fakePlugin{name: "cpuset_topology", enabled: true, topologyResult: &bulkheadapi.TopologyResult{
		Converged:            true,
		FinalSnapshotCurrent: false,
		AppliedView: &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
			ReclaimEffective: machine.NewCPUSet(1),
		}},
	}}
	consumer := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{
		plugins:             []bulkheadapi.Plugin{topologyPlugin, consumer},
		appliedView:         oldApplied,
		appliedViewRevision: 7,
	}
	state, topology := testBulkheadStateAndTopology()

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got := len(consumer.adjustViews); got != 0 {
		t.Fatalf("consumer calls = %d, want 0 for stale final snapshot", got)
	}
	if m.appliedViewRevision != 7 {
		t.Fatalf("applied revision = %d, want old revision 7 retained", m.appliedViewRevision)
	}
	assertCPUSet(t, "retained applied reclaim", m.appliedView.ReclaimEffective, "3")
}

func TestRunCPUSetAdjustmentHandlersRetainsRevisionWhenDesiredChangesBeforeTopologyCallback_BitsUT(t *testing.T) {
	t.Parallel()

	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(1, 2, 3),
	})
	_, topology := testBulkheadStateAndTopology()
	oldApplied := &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
		ReclaimEffective: machine.NewCPUSet(3),
	}}
	topologyPlugin := &fakePlugin{
		name:    "cpuset_topology",
		enabled: true,
		topologyResult: &bulkheadapi.TopologyResult{
			Converged:            true,
			FinalSnapshotCurrent: true,
			AppliedView: &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
				ReclaimEffective: machine.NewCPUSet(1),
			}},
		},
		afterReport: func() {
			state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: machine.NewCPUSet(2, 3),
			})
		},
	}
	consumer := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{
		plugins:             []bulkheadapi.Plugin{topologyPlugin, consumer},
		appliedView:         oldApplied,
		appliedViewRevision: 7,
	}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got := len(consumer.adjustViews); got != 0 {
		t.Fatalf("consumer calls = %d, want 0 after desired intent changes", got)
	}
	if m.appliedViewRevision != 7 {
		t.Fatalf("applied revision = %d, want old revision 7 retained", m.appliedViewRevision)
	}
	assertCPUSet(t, "retained applied reclaim", m.appliedView.ReclaimEffective, "3")
}

func TestRunCPUSetAdjustmentHandlersShortCircuitsConsumersWhenTopologyDisabled_BitsUT(t *testing.T) {
	t.Parallel()

	topologyPlugin := &fakePlugin{name: "cpuset_topology", enabled: false}
	consumer := &fakePlugin{name: "workqueue", enabled: true}
	m := &Manager{
		plugins:                     []bulkheadapi.Plugin{topologyPlugin, consumer},
		appliedView:                 (&model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{ReclaimEffective: machine.NewCPUSet(1)}}).ToAppliedView(),
		appliedViewRevision:         1,
		lastCPUSetAdjustmentEnabled: map[string]bool{topologyPlugin.Name(): true, consumer.Name(): true},
	}
	state, topology := testBulkheadStateAndTopology()

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got := len(consumer.adjustViews); got != 0 {
		t.Fatalf("consumer calls = %d, want 0 when topology is disabled", got)
	}
	if m.appliedViewRevision != 1 {
		t.Fatalf("applied revision = %d, want old revision 1 retained", m.appliedViewRevision)
	}
}

func TestRunCPUSetAdjustmentHandlersRunsDisabledResetAfterTopologyDisabled_BitsUT(t *testing.T) {
	t.Parallel()

	topologyPlugin := &fakePlugin{name: "cpuset_topology", enabled: false}
	enabledConsumer := &fakePlugin{name: "cpuset_mems", enabled: true}
	disabledReset := &fakePlugin{name: "workqueue", enabled: false}
	m := &Manager{
		plugins: []bulkheadapi.Plugin{topologyPlugin, enabledConsumer, disabledReset},
		lastCPUSetAdjustmentEnabled: map[string]bool{
			topologyPlugin.Name():  true,
			enabledConsumer.Name(): true,
			disabledReset.Name():   true,
		},
	}
	state, topology := testBulkheadStateAndTopology()

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got := len(enabledConsumer.adjustViews); got != 0 {
		t.Fatalf("enabled consumer calls = %d, want 0 when topology is disabled", got)
	}
	if topologyPlugin.disabledCalls != 1 {
		t.Fatalf("topology disabled calls = %d, want 1", topologyPlugin.disabledCalls)
	}
	if disabledReset.disabledCalls != 1 {
		t.Fatalf("downstream disabled reset calls = %d, want 1", disabledReset.disabledCalls)
	}
	if m.appliedViewValidForPeriodical {
		t.Fatalf("periodical AppliedView must remain invalid when topology is disabled")
	}
}

func TestRunPeriodicalHandlersWithholdsOldAppliedViewWhenTopologyNotPublished_BitsUT(t *testing.T) {
	t.Parallel()

	topologyPlugin := &fakePlugin{name: "cpuset_topology", enabled: true, topologyResult: &bulkheadapi.TopologyResult{
		Converged:            true,
		FinalSnapshotCurrent: true,
		AppliedView: &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
			ReclaimEffective: machine.NewCPUSet(1, 2, 3),
		}},
	}}
	systemService := &fakePlugin{name: "system_service", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{topologyPlugin, systemService}}
	state, topology := testBulkheadStateAndTopology()

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("initial converged run failed: %v", err)
	}
	if m.appliedView == nil || m.appliedViewRevision != 1 {
		t.Fatalf("initial run should publish internal applied view, view=%v revision=%d", m.appliedView, m.appliedViewRevision)
	}

	topologyPlugin.topologyResult = nil
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
		State:       state,
		Topology:    topology,
	}); err != nil {
		t.Fatalf("non-converged run failed: %v", err)
	}
	if m.appliedView == nil || m.appliedViewRevision != 1 {
		t.Fatalf("non-publish run must retain internal old applied view/revision, view=%v revision=%d", m.appliedView, m.appliedViewRevision)
	}

	m.RunPeriodicalHandlers(nil, nil, enabledDynamicAgentConf(), nil, nil)
	if systemService.periodicCalls != 1 {
		t.Fatalf("periodical calls = %d, want 1", systemService.periodicCalls)
	}
	if systemService.periodicApplied[0] != nil {
		t.Fatalf("periodical context should withhold old AppliedView when current round did not publish")
	}
	if systemService.periodicRevision[0] != 0 {
		t.Fatalf("periodical revision = %d, want 0 for unpublished current round", systemService.periodicRevision[0])
	}
	if systemService.periodicValid[0] {
		t.Fatalf("periodical valid flag = true, want false for unpublished current round")
	}
}

func TestRunCPUSetAdjustmentHandlersDisabledTransitionInvalidatesCache(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("first enabled run failed: %v", err)
	}

	plugin.enabled = false
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("disabled transition failed: %v", err)
	}

	plugin.enabled = true
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("second enabled run failed: %v", err)
	}

	if got := len(plugin.adjustViews); got != 2 {
		t.Fatalf("expected second enabled run not to be skipped after disabled transition, got %d calls", got)
	}
}

func TestRunCPUSetAdjustmentHandlersCallsDisabledTransitionOnceForPluginDisable(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("enabled run failed: %v", err)
	}

	plugin.enabled = false
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("disabled transition failed: %v", err)
	}
	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("stable disabled run failed: %v", err)
	}

	if plugin.disabledCalls != 1 {
		t.Fatalf("disabled transition calls = %d, want 1", plugin.disabledCalls)
	}
}

func TestRunCPUSetAdjustmentHandlersReturnsDisabledHandlerError(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake", enabled: true}
	m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}

	if err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx()); err != nil {
		t.Fatalf("enabled run failed: %v", err)
	}

	plugin.enabled = false
	plugin.disabledErr = errors.New("disabled failed")
	err := m.RunCPUSetAdjustmentHandlers(context.Background(), enabledCPUSetAdjustmentCtx())
	if err == nil {
		t.Fatalf("expected disabled transition error")
	}
	if got := err.Error(); !strings.Contains(got, "disabled transition failed") {
		t.Fatalf("error = %q, want disabled transition failed", got)
	}
}

func TestRunPeriodicalHandlersContinuesAfterErrors(t *testing.T) {
	t.Parallel()

	pluginA := &fakePlugin{name: "a", periodicErr: errors.New("a failed")}
	pluginB := &fakePlugin{name: "b"}
	m := &Manager{plugins: []bulkheadapi.Plugin{pluginA, pluginB}}

	m.RunPeriodicalHandlers(nil, nil, enabledDynamicAgentConf(), nil, nil)
	if pluginA.periodicCalls != 1 || pluginB.periodicCalls != 1 {
		t.Fatalf("expected both plugins to run, got a=%d b=%d", pluginA.periodicCalls, pluginB.periodicCalls)
	}
}

func TestRunPeriodicalHandlersSkipsAllPluginLogicWhenBulkheadDisabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		bulkheadEnabled bool
		wantCalls       int
	}{
		{
			name: "bulkhead disabled",
		},
		{
			name:            "bulkhead enabled",
			bulkheadEnabled: true,
			wantCalls:       1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			plugin := &fakePlugin{name: "fake"}
			m := &Manager{plugins: []bulkheadapi.Plugin{plugin}}
			dynamicConf := dynamicconfig.NewDynamicAgentConfiguration()
			dynamicConf.SetDynamicConfiguration(dynamicBulkheadConf(tt.bulkheadEnabled))

			m.RunPeriodicalHandlers(nil, nil, dynamicConf, nil, nil)
			if plugin.periodicCalls != tt.wantCalls {
				t.Fatalf("periodic calls = %d, want %d", plugin.periodicCalls, tt.wantCalls)
			}
			if len(plugin.periodicStates) != tt.wantCalls {
				t.Fatalf("periodic states = %d, want %d", len(plugin.periodicStates), tt.wantCalls)
			}
		})
	}
}

func TestEmitBulkheadPluginResultFormatsReasonTag(t *testing.T) {
	t.Parallel()

	emitter := &capturingEmitter{}
	rawReason := "apply cpuset failed at kubepods/podabc/container with no such file or directory and many details " + strings.Repeat("x", 200)

	emitBulkheadPluginResult(emitter, "periodical", "cpuset_topology", "failed", rawReason)

	if len(emitter.records) != 1 {
		t.Fatalf("records = %d, want 1", len(emitter.records))
	}
	var gotReason string
	for _, tag := range emitter.records[0].tags {
		if tag.Key == "reason" {
			gotReason = tag.Val
			break
		}
	}
	wantReason := metricutil.MetricTagValueFormat(rawReason)
	if gotReason != wantReason {
		t.Fatalf("reason tag = %q, want formatted %q", gotReason, wantReason)
	}
}

func TestEmitBulkheadDesiredViewMetrics(t *testing.T) {
	t.Parallel()

	emitter := &capturingEmitter{}
	desired := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		Reserve:          machine.NewCPUSet(0),
		Dedicated:        machine.NewCPUSet(1, 2),
		SharePool:        machine.NewCPUSet(3, 4, 5),
		ReclaimEffective: machine.NewCPUSet(6, 7),
		NonReclaimPool:   machine.NewCPUSet(0, 1, 2, 3, 4, 5),
		Isolation:        machine.NewCPUSet(2),
		SharePoolMap: map[string]machine.CPUSet{
			commonstate.PoolNameShare: machine.NewCPUSet(4, 5),
		},
	}}

	emitBulkheadPartitionViewMetrics(emitter, "desired", &desired.CPUSetPartitionView)
	emitBulkheadDefaultShareResidualMetric(emitter, "desired", &desired.CPUSetPartitionView)

	want := map[string]float64{
		metricKey(metricBulkheadPartitionCPUCores, "view", "desired", "partition", "reserve"):       1,
		metricKey(metricBulkheadPartitionCPUCores, "view", "desired", "partition", "dedicated"):     2,
		metricKey(metricBulkheadPartitionCPUCores, "view", "desired", "partition", "share"):         3,
		metricKey(metricBulkheadPartitionCPUCores, "view", "desired", "partition", "reclaim"):       2,
		metricKey(metricBulkheadPartitionCPUCores, "view", "desired", "partition", "non_reclaim"):   6,
		metricKey(metricBulkheadPartitionCPUCores, "view", "desired", "partition", "isolation"):     1,
		metricKey(metricBulkheadPartitionCPUCores, "view", "desired", "partition", "default_share"): 2,
		metricKey(metricBulkheadDefaultShareResidualCPUCores, "view", "desired"):                    2,
	}
	assertMetricValues(t, emitter.records, want)
}

func TestEmitBulkheadAppliedViewMetrics(t *testing.T) {
	t.Parallel()

	emitter := &capturingEmitter{}
	desired := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		ReclaimEffective: machine.NewCPUSet(6, 7),
		NonReclaimPool:   machine.NewCPUSet(0, 1, 2, 3, 4, 5),
		SharePoolMap: map[string]machine.CPUSet{
			commonstate.PoolNameShare: machine.NewCPUSet(4, 5),
		},
	}}
	applied := &model.AppliedView{CPUSetPartitionView: model.CPUSetPartitionView{
		ReclaimEffective: machine.NewCPUSet(7, 8),
		NonReclaimPool:   machine.NewCPUSet(0, 1, 2, 3, 4, 8),
		SharePoolMap: map[string]machine.CPUSet{
			commonstate.PoolNameShare: machine.NewCPUSet(4, 8),
		},
	}}

	emitBulkheadPartitionViewMetrics(emitter, "applied", &applied.CPUSetPartitionView)
	emitBulkheadDefaultShareResidualMetric(emitter, "applied", &applied.CPUSetPartitionView)
	emitBulkheadPartitionDiffMetrics(emitter, desired, applied)

	want := map[string]float64{
		metricKey(metricBulkheadPartitionCPUCores, "view", "applied", "partition", "reclaim"):       2,
		metricKey(metricBulkheadPartitionCPUCores, "view", "applied", "partition", "non_reclaim"):   6,
		metricKey(metricBulkheadPartitionCPUCores, "view", "applied", "partition", "default_share"): 2,
		metricKey(metricBulkheadDefaultShareResidualCPUCores, "view", "applied"):                    2,
	}
	for _, partition := range []string{"reclaim", "non_reclaim", "default_share"} {
		want[metricKey(metricBulkheadPartitionCPUDiffCores, "partition", partition, "direction", "desired_only")] = 1
		want[metricKey(metricBulkheadPartitionCPUDiffCores, "partition", partition, "direction", "applied_only")] = 1
	}
	assertMetricValues(t, emitter.records, want)
}

func metricKey(name string, tags ...string) string {
	parts := []string{name}
	for i := 0; i < len(tags); i += 2 {
		parts = append(parts, tags[i]+"="+tags[i+1])
	}
	return strings.Join(parts, ",")
}

func assertMetricValues(t *testing.T, records []capturedMetric, want map[string]float64) {
	t.Helper()
	got := make(map[string]float64, len(records))
	for _, record := range records {
		tags := make([]string, 0, len(record.tags)*2)
		for _, tag := range record.tags {
			tags = append(tags, tag.Key, tag.Val)
		}
		got[metricKey(record.key, tags...)] = record.val
	}
	for key, wantValue := range want {
		if gotValue, ok := got[key]; !ok || gotValue != wantValue {
			t.Fatalf("metric %q = %v, present=%t, want %v; all metrics: %#v", key, gotValue, ok, wantValue, got)
		}
	}
}

func hasMetric(records []capturedMetric, key string) bool {
	for _, record := range records {
		if record.key == key {
			return true
		}
	}
	return false
}

func hasMetricTags(records []capturedMetric, key string, tags ...string) bool {
	for _, record := range records {
		if record.key != key {
			continue
		}
		matched := true
		for i := 0; i < len(tags); i += 2 {
			found := false
			for _, tag := range record.tags {
				if tag.Key == tags[i] && tag.Val == tags[i+1] {
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func TestBulkheadSlowHandlerThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		elapsed time.Duration
		want    bool
	}{
		{name: "below threshold", elapsed: bulkheadSlowHandlerThreshold - time.Nanosecond, want: false},
		{name: "at threshold", elapsed: bulkheadSlowHandlerThreshold, want: true},
		{name: "above threshold", elapsed: bulkheadSlowHandlerThreshold + time.Nanosecond, want: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.elapsed >= bulkheadSlowHandlerThreshold
			if got != tt.want {
				t.Fatalf("slow classification for %s = %t, want %t", tt.elapsed, got, tt.want)
			}
		})
	}
}

func dynamicBulkheadConf(enabled bool) *dynamicconfig.Configuration {
	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable = enabled
	return conf
}

func dynamicBulkheadConfWithMinSize(enabled bool, minSize int64) *dynamicconfig.Configuration {
	conf := dynamicBulkheadConf(enabled)
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize = minSize
	return conf
}

func enabledCPUSetAdjustmentCtx() cpusetutil.CPUSetAdjustmentHandlerCtx {
	return cpusetutil.CPUSetAdjustmentHandlerCtx{
		DynamicConf: dynamicBulkheadConf(true),
	}
}

func enabledDynamicAgentConf() *dynamicconfig.DynamicAgentConfiguration {
	conf := dynamicconfig.NewDynamicAgentConfiguration()
	conf.SetDynamicConfiguration(dynamicBulkheadConf(true))
	return conf
}

func testBulkheadStateAndTopology() (cpustate.ReadonlyState, *machine.CPUTopology) {
	state := cpustate.NewCPUPluginState(nil)
	state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult: machine.NewCPUSet(0),
	})
	state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, &cpustate.AllocationInfo{
		AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
		AllocationResult: machine.NewCPUSet(1, 2, 3),
	})
	topology := &machine.CPUTopology{
		NumCPUs:      4,
		NumCores:     4,
		NumSockets:   2,
		NumNUMANodes: 2,
		CPUDetails: machine.CPUDetails{
			0: {NUMANodeID: 0, SocketID: 0, CoreID: 0},
			1: {NUMANodeID: 0, SocketID: 0, CoreID: 1},
			2: {NUMANodeID: 1, SocketID: 1, CoreID: 2},
			3: {NUMANodeID: 1, SocketID: 1, CoreID: 3},
		},
	}
	return state, topology
}

func assertCPUSet(t *testing.T, name string, got machine.CPUSet, want string) {
	t.Helper()
	if got.String() != want {
		t.Fatalf("%s cpuset = %s, want %s", name, got.String(), want)
	}
}

func TestNewManagerRegistersDefaultPluginsInOrder(t *testing.T) {
	t.Parallel()

	m, err := NewManager(nil)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	got := make([]string, 0, len(m.plugins))
	for _, plugin := range m.plugins {
		got = append(got, plugin.Name())
	}
	want := []string{"cpuset_topology", "cpuset_mems", "workqueue", "system_service", "rdt_cpulist", "rdt_cat"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected plugin names, got %v want %v", got, want)
	}
}
