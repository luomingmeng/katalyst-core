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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/util/errors"

	cpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/consts"
	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/registry"
	bulkheadutils "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils"
	bulkheadtopology "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	metricutil "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type RuntimeDependencies struct {
	DynamicConf *dynamicconfig.DynamicAgentConfiguration
	Emitter     metrics.MetricEmitter
	MetaServer  *metaserver.MetaServer
	Topology    *machine.CPUTopology
}

type Manager struct {
	mu                          sync.Mutex
	plugins                     []bulkheadapi.Plugin
	conf                        *config.Configuration
	dynamic                     *dynamicconfig.DynamicAgentConfiguration
	emitter                     metrics.MetricEmitter
	meta                        *metaserver.MetaServer
	topology                    *machine.CPUTopology
	lastCPUSetAdjustmentEnabled map[string]bool
}

const (
	metricBulkheadHandlerResult  = "bulkhead_handler_result"
	metricBulkheadViewChanged    = "bulkhead_view_changed"
	bulkheadSlowHandlerThreshold = 500 * time.Millisecond
)

func NewManager(conf *config.Configuration, runtime RuntimeDependencies) (*Manager, error) {
	plugins, err := registry.NewDefaultPlugins(conf)
	if err != nil {
		return nil, err
	}
	return &Manager{
		plugins:  plugins,
		conf:     conf,
		dynamic:  runtime.DynamicConf,
		emitter:  runtime.Emitter,
		meta:     runtime.MetaServer,
		topology: runtime.Topology,
	}, nil
}

type PluginApplyError struct {
	Plugin string
	Phase  string
	Err    error
}

type joinedError struct {
	errs []error
}

func (e *joinedError) Error() string {
	messages := make([]string, 0, len(e.errs))
	for _, err := range e.errs {
		messages = append(messages, err.Error())
	}
	return strings.Join(messages, "\n")
}

func (e *joinedError) Is(target error) bool {
	for _, err := range e.errs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func (e *joinedError) As(target interface{}) bool {
	for _, err := range e.errs {
		if errors.As(err, target) {
			return true
		}
	}
	return false
}

// joinErrors provides errors.Join semantics while this module still supports Go 1.18.
func joinErrors(errs ...error) error {
	joined := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			joined = append(joined, err)
		}
	}
	switch len(joined) {
	case 0:
		return nil
	case 1:
		return joined[0]
	default:
		return &joinedError{errs: joined}
	}
}

func (e *PluginApplyError) Error() string {
	return fmt.Sprintf("bulkhead plugin %q %s failed: %v", e.Plugin, e.Phase, e.Err)
}

func (e *PluginApplyError) Unwrap() error {
	return e.Err
}

func (m *Manager) Materialize(
	ctx context.Context,
	target cpusetmaterializer.Target,
) (cpusetmaterializer.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	evidence := cpusetmaterializer.Evidence{Executed: true}
	result := cpusetmaterializer.Result{Evidence: evidence}
	dynamicConf := m.dynamic.GetDynamicConfiguration()
	if !bulkheadEnabled(dynamicConf) {
		// A registered materializer remains part of the transaction contract
		// after the global switch is turned off. Disabled means there is no
		// external state to materialize, not that the transaction failed.
		// Forget plugin transition state so re-enabling starts a fresh
		// reconciliation without running Reset or leaving pending evidence.
		m.lastCPUSetAdjustmentEnabled = nil
		return cpusetmaterializer.Result{Converged: true}, nil
	}
	if m.topology == nil || len(m.topology.CPUDetails) == 0 {
		result.Evidence.FailureReason = "machine topology unavailable"
		return result, errors.New("bulkhead materializer requires machine topology")
	}
	if len(m.plugins) == 0 {
		result.Evidence.FailureReason = "no plugins"
		return result, errors.New("bulkhead materializer has no plugins")
	}

	view := bulkheadutils.BuildCPUSetPartitionViewFromTarget(target)
	handlerCtx := bulkheadapi.HandlerContext{
		CoreConf:    m.conf,
		DynamicConf: dynamicConf,
		Emitter:     m.emitter,
		MetaServer:  m.meta,
		Topology:    m.topology,
		Target:      target,
		View:        view,
	}
	enabled := m.buildPluginEnabledState(handlerCtx)
	topologyPlugin, err := m.topologyPlugin()
	if err != nil {
		result.Evidence.FailureReason = err.Error()
		return result, err
	}
	if !enabled[topologyPlugin.Name()] {
		for name := range enabled {
			enabled[name] = false
		}
		for _, plugin := range m.plugins {
			if !m.needsDisabledReset(plugin.Name()) {
				continue
			}
			if err := plugin.Reset(ctx, handlerCtx); err != nil {
				result.Evidence.FailureReason = err.Error()
				return result, &PluginApplyError{
					Plugin: plugin.Name(),
					Phase:  "disabled transition",
					Err:    err,
				}
			}
		}
		m.lastCPUSetAdjustmentEnabled = enabled
		result.Converged = true
		return result, nil
	}

	topologyResult, reconcileErr := topologyPlugin.ReconcileTopology(ctx, handlerCtx)
	result.Evidence = evidenceFromTopologyResult(topologyResult)
	if err := requireFullyConverged(topologyResult, reconcileErr); err != nil {
		result.Evidence.FailureReason = err.Error()
		pluginErr := &PluginApplyError{
			Plugin: topologyPlugin.Name(),
			Phase:  "topology reconcile",
			Err:    err,
		}
		if reconcileErr != nil {
			return result, pluginErr
		}
		return result, joinErrors(cpusetmaterializer.ErrCPUSetNotConverged, pluginErr)
	}

	for _, plugin := range m.plugins {
		if plugin.Name() == topologyPlugin.Name() {
			continue
		}
		if !enabled[plugin.Name()] {
			if !m.needsDisabledReset(plugin.Name()) {
				continue
			}
			if err := plugin.Reset(ctx, handlerCtx); err != nil {
				result.Evidence.FailureReason = err.Error()
				return result, &PluginApplyError{
					Plugin: plugin.Name(),
					Phase:  "disabled transition",
					Err:    err,
				}
			}
			continue
		}
		if err := plugin.Reconcile(ctx, handlerCtx); err != nil {
			result.Evidence.FailureReason = err.Error()
			return result, &PluginApplyError{
				Plugin: plugin.Name(),
				Phase:  "cpuset adjustment",
				Err:    err,
			}
		}
	}

	emitBulkheadViewChanged(m.emitter, true)
	m.lastCPUSetAdjustmentEnabled = enabled
	result.Converged = true
	return result, nil
}

func (m *Manager) topologyPlugin() (bulkheadapi.TopologyPlugin, error) {
	var topologyPlugin bulkheadapi.TopologyPlugin
	for _, plugin := range m.plugins {
		candidate, ok := plugin.(bulkheadapi.TopologyPlugin)
		if !ok {
			continue
		}
		if topologyPlugin != nil {
			return nil, fmt.Errorf(
				"bulkhead materializer requires exactly one topology plugin, found %q and %q",
				topologyPlugin.Name(), plugin.Name())
		}
		topologyPlugin = candidate
	}
	if topologyPlugin == nil {
		return nil, errors.New("bulkhead materializer requires exactly one topology plugin")
	}
	return topologyPlugin, nil
}

func evidenceFromTopologyResult(result bulkheadtopology.DAGApplyResult) cpusetmaterializer.Evidence {
	evidence := cpusetmaterializer.Evidence{
		Executed:       true,
		ControlledRels: make(map[string]cpusetmaterializer.RelEvidence, len(result.ConvergenceReport.NonConvergedTargets)),
		PendingProtection: result.ConvergenceReport.PendingToPrimary.
			Union(result.ConvergenceReport.PendingToReclaim).
			Union(result.ConvergenceReport.CleanupPendingPrimary).
			Union(result.ConvergenceReport.CleanupPendingReclaim),
	}
	for _, rel := range result.ConvergenceReport.NonConvergedTargets {
		evidence.ControlledRels[rel.Rel] = cpusetmaterializer.RelEvidence{
			Target:   rel.Target.Clone(),
			Observed: rel.Observed.Clone(),
			Reason:   rel.Reason,
		}
	}
	return evidence
}

func requireFullyConverged(result bulkheadtopology.DAGApplyResult, err error) error {
	if err != nil {
		return err
	}
	if !result.FullyConverged {
		return &bulkheadtopology.ConvergenceError{Report: result.ConvergenceReport}
	}
	return nil
}

func (m *Manager) buildPluginEnabledState(in bulkheadapi.HandlerContext) map[string]bool {
	out := make(map[string]bool, len(m.plugins))
	for _, plugin := range m.plugins {
		out[plugin.Name()] = plugin.Enable(in)
	}
	return out
}

func (m *Manager) needsDisabledReset(name string) bool {
	return m.lastCPUSetAdjustmentEnabled == nil || m.lastCPUSetAdjustmentEnabled[name]
}

func bulkheadEnabled(conf *dynamicconfig.Configuration) bool {
	if conf == nil || conf.AdminQoSConfiguration == nil || conf.AdminQoSConfiguration.CPUPluginConfiguration == nil {
		return false
	}
	return conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable
}

func (m *Manager) RunPeriodicalHandlers() {
	m.mu.Lock()
	defer m.mu.Unlock()

	started := time.Now()
	var err error
	defer func() {
		elapsed := time.Since(started)
		if elapsed >= bulkheadSlowHandlerThreshold {
			general.InfofV(2, "bulkhead periodical handlers slow elapsed=%s", elapsed)
		}
		_ = general.UpdateHealthzStateByError(cpuconsts.SyncBulkhead, err)
		if err != nil {
			general.ErrorS(err, "bulkhead periodical handlers failed")
		}
	}()

	dynamicConf := m.dynamic.GetDynamicConfiguration()
	if !bulkheadEnabled(dynamicConf) {
		return
	}
	handlerCtx := bulkheadapi.PeriodicalHandlerContext{
		CoreConf:    m.conf,
		DynamicConf: dynamicConf,
		Emitter:     m.emitter,
		MetaServer:  m.meta,
	}
	var errs []error
	for _, plugin := range m.plugins {
		pluginCtx := handlerCtx
		if enabled, ok := m.lastCPUSetAdjustmentEnabled[plugin.Name()]; ok && !enabled {
			pluginCtx.EffectiveEnabled = &enabled
		}
		handlerStarted := time.Now()
		pluginErr := plugin.PeriodicalHandler(context.Background(), pluginCtx)
		if time.Since(handlerStarted) >= bulkheadSlowHandlerThreshold {
			general.InfofV(2, "bulkhead periodical slow plugin=%s elapsed=%s", plugin.Name(), time.Since(handlerStarted))
		}
		if pluginErr != nil {
			wrapped := fmt.Errorf("bulkhead plugin %q periodical failed: %w", plugin.Name(), pluginErr)
			general.ErrorS(wrapped, "bulkhead periodical handler failed")
			emitBulkheadPluginResult(m.emitter, "periodical", plugin.Name(), "failed", pluginErr.Error())
			errs = append(errs, wrapped)
			continue
		}
		emitBulkheadPluginResult(m.emitter, "periodical", plugin.Name(), "success", "")
	}
	err = apierrors.NewAggregate(errs)
}

func emitBulkheadPluginResult(emitter metrics.MetricEmitter, phase, plugin, status, reason string) {
	if emitter == nil {
		return
	}
	_ = emitter.StoreInt64(metricBulkheadHandlerResult, 1, metrics.MetricTypeNameCount,
		metrics.MetricTag{Key: "phase", Val: phase},
		metrics.MetricTag{Key: "plugin", Val: plugin},
		metrics.MetricTag{Key: "status", Val: status},
		metrics.MetricTag{Key: "reason", Val: metricutil.MetricTagValueFormat(reason)},
	)
}

func emitBulkheadViewChanged(emitter metrics.MetricEmitter, changed bool) {
	if emitter == nil {
		return
	}
	_ = emitter.StoreInt64(metricBulkheadViewChanged, 1, metrics.MetricTypeNameCount,
		metrics.MetricTag{Key: "changed", Val: strconv.FormatBool(changed)},
	)
}
