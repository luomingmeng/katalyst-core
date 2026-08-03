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

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	bulkheadtopology "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	metricutil "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

var _ cpusetmaterializer.Materializer = (*Manager)(nil)

type fakePlugin struct {
	name            string
	enabled         bool
	enable          func(bulkheadapi.HandlerContext) bool
	adjustErr       error
	periodicErr     error
	disabledErr     error
	adjustCalls     int
	disabledCalls   int
	periodicCalls   int
	adjustContexts  []bulkheadapi.HandlerContext
	periodicStates  []interface{}
	periodicEnabled []bool
	periodicConfigs []*dynamicconfig.Configuration
	periodicEnable  func(*dynamicconfig.Configuration) bool
}

func (p *fakePlugin) Name() string { return p.name }

func (p *fakePlugin) Enable(in bulkheadapi.HandlerContext) bool {
	if p.enable != nil {
		return p.enable(in)
	}
	return p.enabled
}

func (p *fakePlugin) Reconcile(_ context.Context, in bulkheadapi.HandlerContext) error {
	p.adjustCalls++
	p.adjustContexts = append(p.adjustContexts, in)
	return p.adjustErr
}

func (p *fakePlugin) Reset(context.Context, bulkheadapi.HandlerContext) error {
	p.disabledCalls++
	return p.disabledErr
}

func (p *fakePlugin) PeriodicalHandler(_ context.Context, in bulkheadapi.PeriodicalHandlerContext) error {
	p.periodicCalls++
	p.periodicConfigs = append(p.periodicConfigs, in.DynamicConf)
	enabled := false
	if p.periodicEnable != nil {
		enabled = p.periodicEnable(in.DynamicConf)
	}
	if in.EffectiveEnabled == nil {
		p.periodicStates = append(p.periodicStates, nil)
	} else {
		p.periodicStates = append(p.periodicStates, *in.EffectiveEnabled)
		enabled = *in.EffectiveEnabled
	}
	p.periodicEnabled = append(p.periodicEnabled, enabled)
	return p.periodicErr
}

type fakeTopologyPlugin struct {
	fakePlugin
	result         bulkheadtopology.DAGApplyResult
	reconcileErr   error
	reconcileCalls int
	reconcile      func(bulkheadapi.HandlerContext)
}

func (p *fakeTopologyPlugin) ReconcileTopology(
	_ context.Context,
	in bulkheadapi.HandlerContext,
) (bulkheadtopology.DAGApplyResult, error) {
	p.reconcileCalls++
	if p.reconcile != nil {
		p.reconcile(in)
	}
	return p.result, p.reconcileErr
}

func TestManagerMaterializeRejectsNonConvergedBeforeDependentPlugins(t *testing.T) {
	t.Parallel()

	topologyPlugin := newFakeTopology("topology", false)
	dependent := &fakePlugin{name: "dependent", enabled: true}
	manager := newTestManager(topologyPlugin, dependent)

	result, err := manager.Materialize(context.Background(), testMaterializationTarget())
	if !errors.Is(err, cpusetmaterializer.ErrCPUSetNotConverged) {
		t.Fatalf("Materialize error = %v, want ErrCPUSetNotConverged", err)
	}
	if result.Converged {
		t.Fatalf("Materialize result unexpectedly converged: %+v", result)
	}
	if dependent.adjustCalls != 0 {
		t.Fatalf("dependent calls = %d, want 0", dependent.adjustCalls)
	}
}

func TestManagerMaterializeBuildsViewOnlyFromTarget(t *testing.T) {
	t.Parallel()

	topologyPlugin := newFakeTopology("topology", true)
	dependent := &fakePlugin{name: "dependent", enabled: true}
	manager := newTestManager(topologyPlugin, dependent)
	target := testMaterializationTarget()

	result, err := manager.Materialize(context.Background(), target)
	if err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}
	if !result.Converged {
		t.Fatalf("Materialize result = %+v, want converged", result)
	}
	if len(dependent.adjustContexts) != 1 {
		t.Fatalf("dependent contexts = %d, want 1", len(dependent.adjustContexts))
	}
	view := dependent.adjustContexts[0].View
	assertCPUSet(t, "reserve", view.Reserve, "0")
	assertCPUSet(t, "reclaim", view.ReclaimEffective, "2-3")
	assertCPUSet(t, "non reclaim", view.NonReclaimPool, "1,4-5")
	assertCPUSet(t, "numa reclaim", view.ReclaimEffectivePerNUMA[0], "2-3")
	assertCPUSet(t, "container", view.ContainerCPUSetByPod["pod"]["main"], "1,4")
}

func TestManagerMaterializeRequiresExactlyOneTopologyExecutor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		plugins []bulkheadapi.Plugin
	}{
		{name: "none", plugins: []bulkheadapi.Plugin{&fakePlugin{name: "dependent", enabled: true}}},
		{name: "two", plugins: []bulkheadapi.Plugin{
			newFakeTopology("topology-a", true),
			newFakeTopologyWithEnabled("topology-b", true, false),
		}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			manager := newTestManager(tt.plugins...)
			result, err := manager.Materialize(context.Background(), testMaterializationTarget())
			if err == nil || !strings.Contains(err.Error(), "exactly one topology plugin") {
				t.Fatalf("Materialize error = %v, want topology executor contract error", err)
			}
			if result.Converged {
				t.Fatalf("Materialize result unexpectedly converged: %+v", result)
			}
		})
	}
}

func TestManagerMaterializeTreatsAllPluginsDisabledWhenTopologyDisabled(t *testing.T) {
	t.Parallel()

	topologyPlugin := newFakeTopologyWithEnabled("topology", true, false)
	dependent := &fakePlugin{name: "dependent", enabled: true}
	manager := newTestManager(topologyPlugin, dependent)
	manager.lastCPUSetAdjustmentEnabled = map[string]bool{
		topologyPlugin.Name(): true,
		dependent.Name():      true,
	}

	result, err := manager.Materialize(context.Background(), testMaterializationTarget())
	if err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}
	if !result.Converged {
		t.Fatalf("Materialize result = %+v, want converged disabled reset", result)
	}
	if topologyPlugin.reconcileCalls != 0 {
		t.Fatalf("topology reconcile calls = %d, want 0", topologyPlugin.reconcileCalls)
	}
	if topologyPlugin.disabledCalls != 1 || dependent.disabledCalls != 1 {
		t.Fatalf("disabled calls topology=%d dependent=%d, want 1 each",
			topologyPlugin.disabledCalls, dependent.disabledCalls)
	}
	if dependent.adjustCalls != 0 {
		t.Fatalf("dependent adjustment calls = %d, want 0", dependent.adjustCalls)
	}
	if manager.lastCPUSetAdjustmentEnabled[topologyPlugin.Name()] ||
		manager.lastCPUSetAdjustmentEnabled[dependent.Name()] {
		t.Fatalf("last enabled state = %+v, want all disabled", manager.lastCPUSetAdjustmentEnabled)
	}

	if _, err := manager.Materialize(context.Background(), testMaterializationTarget()); err != nil {
		t.Fatalf("second Materialize failed: %v", err)
	}
	if topologyPlugin.disabledCalls != 1 || dependent.disabledCalls != 1 {
		t.Fatalf("stable disabled calls topology=%d dependent=%d, want 1 each",
			topologyPlugin.disabledCalls, dependent.disabledCalls)
	}
}

func TestManagerMaterializeTreatsAllPluginsDisabledWhenReclaimOverlapAllowed(t *testing.T) {
	t.Parallel()

	topologyPlugin := newFakeTopology("topology", true)
	topologyPlugin.enable = func(in bulkheadapi.HandlerContext) bool {
		return !in.Target.AllowReclaimOverlap()
	}
	dependent := &fakePlugin{name: "dependent", enabled: true}
	manager := newTestManager(topologyPlugin, dependent)
	manager.lastCPUSetAdjustmentEnabled = map[string]bool{
		topologyPlugin.Name(): true,
		dependent.Name():      true,
	}

	result, err := manager.Materialize(context.Background(), testMaterializationTargetWithOverlap())
	if err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}
	if !result.Converged {
		t.Fatalf("Materialize result = %+v, want converged disabled reset", result)
	}
	if topologyPlugin.reconcileCalls != 0 || dependent.adjustCalls != 0 {
		t.Fatalf("unexpected active calls topology=%d dependent=%d",
			topologyPlugin.reconcileCalls, dependent.adjustCalls)
	}
	if topologyPlugin.disabledCalls != 1 || dependent.disabledCalls != 1 {
		t.Fatalf("disabled calls topology=%d dependent=%d, want 1 each",
			topologyPlugin.disabledCalls, dependent.disabledCalls)
	}
}

func TestManagerMaterializePropagatesDependentPluginFailure(t *testing.T) {
	t.Parallel()

	dependentErr := errors.New("dependent failed")
	dependent := &fakePlugin{name: "dependent", enabled: true, adjustErr: dependentErr}
	manager := newTestManager(newFakeTopology("topology", true), dependent)

	result, err := manager.Materialize(context.Background(), testMaterializationTarget())
	if !errors.Is(err, dependentErr) {
		t.Fatalf("Materialize error = %v, want dependent error", err)
	}
	if result.Converged {
		t.Fatalf("Materialize result unexpectedly converged: %+v", result)
	}
	if result.Evidence.FailureReason != dependentErr.Error() {
		t.Fatalf("failure reason = %q, want %q", result.Evidence.FailureReason, dependentErr)
	}
}

func TestManagerMaterializeTopologyFailurePreservesPluginAndCauseWithoutConvergenceSentinel(t *testing.T) {
	t.Parallel()

	cause := errors.New("topology apply failed")
	topologyPlugin := newFakeTopology("topology", false)
	topologyPlugin.reconcileErr = cause
	manager := newTestManager(topologyPlugin)

	_, err := manager.Materialize(context.Background(), testMaterializationTarget())
	if errors.Is(err, cpusetmaterializer.ErrCPUSetNotConverged) {
		t.Fatalf("Materialize error = %v, must not contain ErrCPUSetNotConverged", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("Materialize error = %v, want topology cause", err)
	}
	var pluginErr *PluginApplyError
	if !errors.As(err, &pluginErr) {
		t.Fatalf("Materialize error = %v, want PluginApplyError", err)
	}
	if pluginErr.Plugin != topologyPlugin.Name() || pluginErr.Phase != "topology reconcile" {
		t.Fatalf("PluginApplyError = %+v, want topology reconcile metadata", pluginErr)
	}
}

func TestManagerMaterializeReturnsCompleteEvidence(t *testing.T) {
	t.Parallel()

	topologyPlugin := newFakeTopology("topology", false)
	topologyPlugin.result.ConvergenceReport = bulkheadtopology.ConvergenceReport{
		NonConvergedTargets: []bulkheadtopology.RelConvergence{{
			Rel:      "reclaimed/reclaimed-0",
			Target:   machine.NewCPUSet(2, 3),
			Observed: machine.NewCPUSet(2),
			Reason:   "target_mismatch",
		}},
		PendingToPrimary: machine.NewCPUSet(4),
		PendingToReclaim: machine.NewCPUSet(5),
	}
	manager := newTestManager(topologyPlugin)

	result, err := manager.Materialize(context.Background(), testMaterializationTarget())
	if !errors.Is(err, cpusetmaterializer.ErrCPUSetNotConverged) {
		t.Fatalf("Materialize error = %v, want ErrCPUSetNotConverged", err)
	}
	if !result.Evidence.Executed {
		t.Fatal("evidence must record execution")
	}
	rel, ok := result.Evidence.ControlledRels["reclaimed/reclaimed-0"]
	if !ok {
		t.Fatalf("controlled rel evidence missing: %+v", result.Evidence)
	}
	assertCPUSet(t, "evidence target", rel.Target, "2-3")
	assertCPUSet(t, "evidence observed", rel.Observed, "2")
	if rel.Reason != "target_mismatch" {
		t.Fatalf("evidence reason = %q", rel.Reason)
	}
	assertCPUSet(t, "pending protection", result.Evidence.PendingProtection, "4-5")
	if result.Evidence.FailureReason == "" {
		t.Fatal("failure reason must be populated")
	}
}

func TestManagerMaterializeDoesNotMutateTarget(t *testing.T) {
	t.Parallel()

	topologyPlugin := newFakeTopology("topology", true)
	topologyPlugin.reconcile = func(in bulkheadapi.HandlerContext) {
		in.View.ReclaimEffective = machine.NewCPUSet(99)
		in.View.ReclaimEffectivePerNUMA[0] = machine.NewCPUSet(99)
		in.View.ContainerCPUSetByPod["pod"]["main"] = machine.NewCPUSet(99)
	}
	manager := newTestManager(topologyPlugin)
	target := testMaterializationTarget()

	if _, err := manager.Materialize(context.Background(), target); err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}
	assertCPUSet(t, "target reclaim", target.ReclaimCPUSet(), "2-3")
	assertCPUSet(t, "target numa reclaim", target.ReclaimCPUSetByNUMA()[0], "2-3")
	assertCPUSet(t, "target container", target.ContainerCPUSetByPod()["pod"]["main"], "1,4")
}

func TestManagerMaterializeDisabledPluginTransition(t *testing.T) {
	t.Parallel()

	dependent := &fakePlugin{name: "dependent", enabled: false}
	manager := newTestManager(newFakeTopology("topology", true), dependent)
	if _, err := manager.Materialize(context.Background(), testMaterializationTarget()); err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}
	if dependent.disabledCalls != 1 {
		t.Fatalf("disabled calls = %d, want 1", dependent.disabledCalls)
	}
	if _, err := manager.Materialize(context.Background(), testMaterializationTarget()); err != nil {
		t.Fatalf("second Materialize failed: %v", err)
	}
	if dependent.disabledCalls != 1 {
		t.Fatalf("stable disabled calls = %d, want 1", dependent.disabledCalls)
	}
}

func TestRunPeriodicalHandlersContinuesAfterErrors(t *testing.T) {
	t.Parallel()

	pluginA := &fakePlugin{name: "a", periodicErr: errors.New("a failed")}
	pluginB := &fakePlugin{name: "b"}
	manager := newTestManager(pluginA, pluginB)
	manager.RunPeriodicalHandlers()
	if pluginA.periodicCalls != 1 || pluginB.periodicCalls != 1 {
		t.Fatalf("expected both plugins to run, got a=%d b=%d", pluginA.periodicCalls, pluginB.periodicCalls)
	}
}

func TestRunPeriodicalHandlersSkipsPluginsWhenBulkheadDisabled(t *testing.T) {
	t.Parallel()

	plugin := &fakePlugin{name: "fake"}
	manager := newTestManager(plugin)
	manager.dynamic.SetDynamicConfiguration(dynamicBulkheadConf(false))
	manager.RunPeriodicalHandlers()
	if plugin.periodicCalls != 0 {
		t.Fatalf("periodic calls = %d, want 0", plugin.periodicCalls)
	}
}

func TestManagerReadsLatestDynamicConfigurationForEveryOperation(t *testing.T) {
	t.Parallel()

	provider := dynamicconfig.NewDynamicAgentConfiguration()
	provider.SetDynamicConfiguration(dynamicBulkheadConf(false))
	topologyPlugin := newFakeTopology("topology", true)
	var materializeConfig *dynamicconfig.Configuration
	topologyPlugin.reconcile = func(in bulkheadapi.HandlerContext) {
		materializeConfig = in.DynamicConf
	}
	periodicalPlugin := &fakePlugin{name: "periodical", enabled: true}
	manager := &Manager{
		plugins:  []bulkheadapi.Plugin{topologyPlugin, periodicalPlugin},
		dynamic:  provider,
		topology: testTopology(),
	}

	if result, err := manager.Materialize(context.Background(), testMaterializationTarget()); err != nil || !result.Converged {
		t.Fatalf("Materialize must treat the initially disabled configuration as converged no-op: result=%+v err=%v",
			result, err)
	}
	if materializeConfig != nil {
		t.Fatalf("disabled Materialize reached topology plugin with config %p", materializeConfig)
	}
	manager.RunPeriodicalHandlers()
	if periodicalPlugin.periodicCalls != 0 {
		t.Fatalf("periodical calls while disabled = %d, want 0", periodicalPlugin.periodicCalls)
	}

	enabled := dynamicBulkheadConf(true)
	provider.SetDynamicConfiguration(enabled)
	if _, err := manager.Materialize(context.Background(), testMaterializationTarget()); err != nil {
		t.Fatalf("Materialize must observe the updated enabled configuration: %v", err)
	}
	if materializeConfig != enabled {
		t.Fatalf("materialize dynamic config = %p, want latest %p", materializeConfig, enabled)
	}
	manager.RunPeriodicalHandlers()
	if periodicalPlugin.periodicCalls != 1 {
		t.Fatalf("periodical calls after enable = %d, want 1", periodicalPlugin.periodicCalls)
	}
	if got := periodicalPlugin.periodicConfigs[0]; got != enabled {
		t.Fatalf("periodical dynamic config = %p, want latest %p", got, enabled)
	}
}

func TestManagerMaterializeRuntimeDisableCommitsWithoutWritesAndReenableReconciles(t *testing.T) {
	t.Parallel()

	provider := dynamicconfig.NewDynamicAgentConfiguration()
	provider.SetDynamicConfiguration(dynamicBulkheadConf(true))
	topologyPlugin := newFakeTopology("topology", true)
	dependent := &fakePlugin{name: "dependent", enabled: true}
	manager := &Manager{
		plugins:  []bulkheadapi.Plugin{topologyPlugin, dependent},
		dynamic:  provider,
		topology: testTopology(),
	}

	if result, err := manager.Materialize(context.Background(), testMaterializationTarget()); err != nil || !result.Converged {
		t.Fatalf("initial enabled Materialize result=%+v error=%v, want converged", result, err)
	}
	if topologyPlugin.reconcileCalls != 1 || dependent.adjustCalls != 1 {
		t.Fatalf("initial writes topology=%d dependent=%d, want 1 each",
			topologyPlugin.reconcileCalls, dependent.adjustCalls)
	}

	provider.SetDynamicConfiguration(dynamicBulkheadConf(false))
	result, err := manager.Materialize(context.Background(), testMaterializationTarget())
	if err != nil {
		t.Fatalf("runtime-disabled Materialize failed: %v", err)
	}
	if !result.Converged {
		t.Fatalf("runtime-disabled Materialize result=%+v, want converged no-op", result)
	}
	if result.Evidence.Executed || len(result.Evidence.ControlledRels) != 0 ||
		!result.Evidence.PendingProtection.IsEmpty() || result.Evidence.FailureReason != "" {
		t.Fatalf("runtime-disabled evidence=%+v, want successful no-execution evidence", result.Evidence)
	}
	if topologyPlugin.reconcileCalls != 1 || dependent.adjustCalls != 1 ||
		topologyPlugin.disabledCalls != 0 || dependent.disabledCalls != 0 {
		t.Fatalf("runtime disable wrote externally: topology reconcile=%d reset=%d dependent reconcile=%d reset=%d",
			topologyPlugin.reconcileCalls, topologyPlugin.disabledCalls,
			dependent.adjustCalls, dependent.disabledCalls)
	}
	if manager.lastCPUSetAdjustmentEnabled != nil {
		t.Fatalf("runtime disable left pending plugin transition: %+v", manager.lastCPUSetAdjustmentEnabled)
	}

	provider.SetDynamicConfiguration(dynamicBulkheadConf(true))
	if result, err := manager.Materialize(context.Background(), testMaterializationTarget()); err != nil || !result.Converged {
		t.Fatalf("re-enabled Materialize result=%+v error=%v, want converged", result, err)
	}
	if topologyPlugin.reconcileCalls != 2 || dependent.adjustCalls != 2 {
		t.Fatalf("re-enabled writes topology=%d dependent=%d, want 2 each",
			topologyPlugin.reconcileCalls, dependent.adjustCalls)
	}
}

func TestRunPeriodicalHandlersLatestDynamicConfigCanVetoLastEnabledState(t *testing.T) {
	t.Parallel()

	latest := dynamicBulkheadConf(true)
	latest.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadCpusetTopology = false
	plugin := &fakePlugin{
		name:    "periodical",
		enabled: true,
		periodicEnable: func(conf *dynamicconfig.Configuration) bool {
			return conf.AdminQoSConfiguration.CPUPluginConfiguration.
				BulkheadConfig.EnableBulkheadCpusetTopology
		},
	}
	manager := newTestManager(plugin)
	manager.lastCPUSetAdjustmentEnabled = map[string]bool{plugin.Name(): true}
	manager.dynamic.SetDynamicConfiguration(latest)

	manager.RunPeriodicalHandlers()

	if plugin.periodicCalls != 1 {
		t.Fatalf("periodical calls = %d, want 1", plugin.periodicCalls)
	}
	if plugin.periodicEnabled[0] {
		t.Fatal("latest dynamic configuration disabled the plugin, but stale enabled state overrode it")
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
		}
	}
	if want := metricutil.MetricTagValueFormat(rawReason); gotReason != want {
		t.Fatalf("reason tag = %q, want %q", gotReason, want)
	}
}

func TestBulkheadSlowHandlerThreshold(t *testing.T) {
	t.Parallel()
	if bulkheadSlowHandlerThreshold != 500*time.Millisecond {
		t.Fatalf("slow handler threshold = %s", bulkheadSlowHandlerThreshold)
	}
}

func TestNewManagerRegistersDefaultPluginsInOrder(t *testing.T) {
	t.Parallel()

	manager, err := NewManager(nil, RuntimeDependencies{})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	got := make([]string, 0, len(manager.plugins))
	for _, plugin := range manager.plugins {
		got = append(got, plugin.Name())
	}
	want := []string{"cpuset_topology", "cpuset_mems", "workqueue", "system_service"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected plugin names, got %v want %v", got, want)
	}
}

func newFakeTopology(name string, converged bool) *fakeTopologyPlugin {
	return newFakeTopologyWithEnabled(name, converged, true)
}

func newFakeTopologyWithEnabled(name string, converged, enabled bool) *fakeTopologyPlugin {
	return &fakeTopologyPlugin{
		fakePlugin: fakePlugin{name: name, enabled: enabled},
		result: bulkheadtopology.DAGApplyResult{
			FullyConverged: converged,
			ConvergenceReport: bulkheadtopology.ConvergenceReport{
				FullyConverged: converged,
			},
		},
	}
}

func newTestManager(plugins ...bulkheadapi.Plugin) *Manager {
	provider := dynamicconfig.NewDynamicAgentConfiguration()
	provider.SetDynamicConfiguration(dynamicBulkheadConf(true))
	return &Manager{
		plugins:  plugins,
		dynamic:  provider,
		topology: testTopology(),
	}
}

func testMaterializationTarget() cpusetmaterializer.Target {
	return testMaterializationTargetInput(false)
}

func testMaterializationTargetWithOverlap() cpusetmaterializer.Target {
	return testMaterializationTargetInput(true)
}

func testMaterializationTargetInput(allowReclaimOverlap bool) cpusetmaterializer.Target {
	return cpusetmaterializer.NewTarget(cpusetmaterializer.TargetInput{
		ReserveCPUSet:        machine.NewCPUSet(0),
		ReclaimCPUSet:        machine.NewCPUSet(2, 3),
		NonReclaimCPUSet:     machine.NewCPUSet(1, 4, 5),
		ReclaimCPUSetByNUMA:  map[int]machine.CPUSet{0: machine.NewCPUSet(2, 3)},
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{"pod": {"main": machine.NewCPUSet(1, 4)}},
		AllowReclaimOverlap:  allowReclaimOverlap,
	})
}

func testTopology() *machine.CPUTopology {
	return &machine.CPUTopology{CPUDetails: machine.CPUDetails{
		0: {NUMANodeID: 0},
		1: {NUMANodeID: 0},
		2: {NUMANodeID: 0},
		3: {NUMANodeID: 0},
		4: {NUMANodeID: 0},
		5: {NUMANodeID: 0},
	}}
}

func dynamicBulkheadConf(enabled bool) *dynamicconfig.Configuration {
	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable = enabled
	return conf
}

func assertCPUSet(t *testing.T, name string, got machine.CPUSet, want string) {
	t.Helper()
	if got.String() != want {
		t.Fatalf("%s cpuset = %s, want %s", name, got.String(), want)
	}
}

type capturedMetric struct {
	tags []metrics.MetricTag
}

type capturingEmitter struct {
	records []capturedMetric
}

func (e *capturingEmitter) StoreInt64(_ string, _ int64, _ metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	e.records = append(e.records, capturedMetric{tags: append([]metrics.MetricTag(nil), tags...)})
	return nil
}

func (*capturingEmitter) StoreFloat64(string, float64, metrics.MetricTypeName, ...metrics.MetricTag) error {
	return nil
}

func (e *capturingEmitter) WithTags(string, ...metrics.MetricTag) metrics.MetricEmitter {
	return e
}

func (*capturingEmitter) Run(context.Context) {}
