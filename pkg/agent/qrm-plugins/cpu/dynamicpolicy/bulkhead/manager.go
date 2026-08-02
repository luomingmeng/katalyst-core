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
	"fmt"
	"strconv"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/util/errors"

	cpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/consts"
	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/registry"
	bulkheadutils "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	metricutil "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type Manager struct {
	mu                            cancelableMutex
	latestAppliedReclaimMu        sync.RWMutex
	plugins                       []bulkheadapi.Plugin
	defaultNonReclaimPoolMinSize  int64
	lastCPUSetAdjustmentEnabled   map[string]bool
	appliedView                   *model.AppliedView
	appliedViewRevision           uint64
	appliedViewValidForPeriodical bool
	latestAppliedReclaim          machine.CPUSet
}

// cancelableMutex is a zero-value-ready binary semaphore. Unlike sync.Mutex,
// acquisition can stop when the caller's context expires.
type cancelableMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *cancelableMutex) init() {
	m.once.Do(func() {
		m.token = make(chan struct{}, 1)
		m.token <- struct{}{}
	})
}

func (m *cancelableMutex) Lock(ctx context.Context) error {
	m.init()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-m.token:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *cancelableMutex) Unlock() {
	m.init()
	m.token <- struct{}{}
}

// NonConvergedError reports a retryable topology outcome that must not be
// treated as successful Bulkhead apply or authorize dependent plugins.
type NonConvergedError struct {
	Result bulkheadapi.DAGApplyResult
}

func (e *NonConvergedError) Error() string {
	return fmt.Sprintf("bulkhead topology not fully converged: current=%t deferred=%d report=%+v",
		e.Result.FinalSnapshotCurrent, e.Result.Deferred, e.Result.ConvergenceReport)
}

const (
	metricBulkheadHandlerResult  = "bulkhead_handler_result"
	metricBulkheadViewChanged    = "bulkhead_view_changed"
	bulkheadSlowHandlerThreshold = 500 * time.Millisecond
)

func NewManager(conf *config.Configuration) (*Manager, error) {
	plugins, err := registry.NewDefaultPlugins(conf)
	if err != nil {
		return nil, err
	}
	var defaultNonReclaimPoolMinSize int64
	if conf != nil && conf.DynamicAgentConfiguration != nil {
		defaultConf := conf.DynamicAgentConfiguration.GetDynamicConfiguration()
		defaultNonReclaimPoolMinSize = bulkheadNonReclaimPoolMinSize(defaultConf)
	}
	return &Manager{
		plugins:                      plugins,
		defaultNonReclaimPoolMinSize: defaultNonReclaimPoolMinSize,
	}, nil
}

func (m *Manager) RunCPUSetAdjustmentHandlers(ctx context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
	_, err := m.Apply(ctx, in)
	return err
}

// Apply converges topology before running partition-dependent plugins and
// returns the reclaim CPUSet verified by the topology layer's final snapshot.
func (m *Manager) Apply(ctx context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) (machine.CPUSet, error) {
	if err := m.mu.Lock(ctx); err != nil {
		return machine.NewCPUSet(), fmt.Errorf("acquire bulkhead manager lock: %w", err)
	}
	defer m.mu.Unlock()

	empty := machine.NewCPUSet()
	if !commitIfGenerationCurrent(in, func() {
		m.appliedViewValidForPeriodical = false
	}) {
		return empty, staleGenerationError()
	}
	handlerCtx := bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: in,
		AppliedView:                m.appliedView.DeepCopy(),
		AppliedViewRevision:        m.appliedViewRevision,
	}
	if !bulkheadEnabled(in.DynamicConf) {
		// The global bulkhead switch is a hard gate: when it is off, do not run
		// plugin Enable/adjust/disabled handlers. Disabled handlers may write
		// cgroup or sysfs rollback state, which is still bulkhead-owned behavior
		// and can introduce unexpected changes after the user explicitly turns
		// bulkhead off.
		if !commitIfGenerationCurrent(in, func() {
			m.lastCPUSetAdjustmentEnabled = nil
		}) {
			return empty, staleGenerationError()
		}
		emitBulkheadViewChanged(handlerCtx.Emitter, false)
		return empty, nil
	}
	if in.State != nil {
		desiredView, err := bulkheadutils.BuildValidatedCPUSetPartitionView(in.State, in.Topology, m.cpuSetPartitionViewOptions(in))
		if err != nil {
			return empty, fmt.Errorf("build bulkhead desired view failed: %w", err)
		}
		handlerCtx.DesiredView = desiredView
		handlerCtx.View = desiredView.CPUSetPartitionView.DeepCopy()
	}
	currentEnabled := m.buildPluginEnabledState(handlerCtx)
	anyAdjusted := false
	topologyPublished := false
	topologyStopped := false
	topologyApplied := false
	verifiedReclaim := machine.NewCPUSet()
	var topologyResult bulkheadapi.DAGApplyResult
	desiredSnapshot := handlerCtx.DesiredView.DeepCopy()
	handlerCtx.ReportTopologyResult = func(result bulkheadapi.TopologyResult) {
		result.AppliedView = result.AppliedView.DeepCopy()
		topologyPublished = m.tryPublishAppliedView(&handlerCtx, desiredSnapshot, &result)
	}

	for _, p := range m.plugins {
		if !commitIfGenerationCurrent(in, func() {}) {
			return empty, staleGenerationError()
		}
		if !currentEnabled[p.Name()] {
			if !m.needsDisabledReset(p.Name()) {
				if p.Name() == "cpuset_topology" {
					topologyStopped = true
				}
				continue
			}
			if err := p.CPUSetAdjustmentDisabledHandler(ctx, handlerCtx); err != nil {
				emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment_disabled", p.Name(), "failed", err.Error())
				return empty, fmt.Errorf("bulkhead plugin %q disabled transition failed: %w", p.Name(), err)
			}
			emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment_disabled", p.Name(), "success", "")
			anyAdjusted = true
			if p.Name() == "cpuset_topology" {
				topologyStopped = true
			}
			continue
		}
		if topologyStopped {
			continue
		}
		if topologyPlugin, ok := p.(bulkheadapi.TopologyPlugin); ok {
			topologyCtx := handlerCtx
			// The typed result is the sole publication path for TopologyPlugin.
			// Suppress the legacy callback so a dependent failure cannot publish
			// manager state from the middle of this transaction.
			topologyCtx.ReportTopologyResult = nil
			result, err := topologyPlugin.Apply(ctx, topologyCtx)
			if err != nil {
				emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "failed", err.Error())
				return empty, fmt.Errorf("bulkhead plugin %q cpuset adjustment failed: %w", p.Name(), err)
			}
			if !result.FullyConverged || !result.FinalSnapshotCurrent || result.AppliedView == nil {
				nonConverged := &NonConvergedError{Result: result}
				emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "failed", nonConverged.Error())
				return empty, nonConverged
			}
			if desiredSnapshot != nil {
				// Rebuild desired intent after topology Apply so a result cannot be
				// accepted, or authorize dependent side effects, after state changed.
				currentDesired, err := bulkheadutils.BuildValidatedCPUSetPartitionView(
					in.State,
					in.Topology,
					m.cpuSetPartitionViewOptions(in),
				)
				if err != nil {
					return empty, fmt.Errorf("rebuild bulkhead desired view after topology apply failed: %w", err)
				}
				if !model.EqualDesiredView(currentDesired, desiredSnapshot) {
					result.FinalSnapshotCurrent = false
					nonConverged := &NonConvergedError{Result: result}
					emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "failed", nonConverged.Error())
					return empty, nonConverged
				}
			}
			handlerCtx.AppliedView = result.AppliedView.DeepCopy()
			handlerCtx.View = handlerCtx.AppliedView.CPUSetPartitionView.DeepCopy()
			verifiedReclaim = handlerCtx.AppliedView.ReclaimEffective.Clone()
			handlerCtx.AppliedViewRevision = m.appliedViewRevision
			if !model.EqualAppliedView(m.appliedView, handlerCtx.AppliedView) {
				handlerCtx.AppliedViewRevision++
			}
			topologyResult = result
			topologyApplied = true
			topologyPublished = true
			anyAdjusted = true
			emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "success", "")
			continue
		}
		if err := p.CPUSetAdjustmentHandler(ctx, handlerCtx); err != nil {
			emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "failed", err.Error())
			return empty, fmt.Errorf("bulkhead plugin %q cpuset adjustment failed: %w", p.Name(), err)
		}
		emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "success", "")
		anyAdjusted = true
		if p.Name() == "cpuset_topology" {
			if !topologyPublished {
				topologyStopped = true
				continue
			}
		}
	}
	if topologyApplied {
		if !commitIfGenerationCurrent(in, func() {
			m.appliedView = handlerCtx.AppliedView.DeepCopy()
			m.appliedViewRevision = handlerCtx.AppliedViewRevision
			m.appliedViewValidForPeriodical = true
			m.publishLatestAppliedReclaim(verifiedReclaim)
			m.lastCPUSetAdjustmentEnabled = currentEnabled
		}) {
			topologyResult.FinalSnapshotCurrent = false
			nonConverged := staleGenerationError()
			nonConverged.Result = topologyResult
			emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", "generation_fence", "failed", nonConverged.Error())
			return empty, nonConverged
		}
	} else {
		if !commitIfGenerationCurrent(in, func() {
			m.lastCPUSetAdjustmentEnabled = currentEnabled
		}) {
			return empty, staleGenerationError()
		}
	}
	emitBulkheadViewChanged(handlerCtx.Emitter, anyAdjusted)
	if topologyApplied {
		return verifiedReclaim.Clone(), nil
	}
	if topologyPublished && m.appliedView != nil {
		return m.appliedView.ReclaimEffective.Clone(), nil
	}
	return empty, nil
}

func (m *Manager) publishLatestAppliedReclaim(cpus machine.CPUSet) {
	m.latestAppliedReclaimMu.Lock()
	defer m.latestAppliedReclaimMu.Unlock()
	m.latestAppliedReclaim = cpus.Clone()
}

func (m *Manager) LatestAppliedReclaim() machine.CPUSet {
	m.latestAppliedReclaimMu.RLock()
	defer m.latestAppliedReclaimMu.RUnlock()
	return m.latestAppliedReclaim.Clone()
}

func (m *Manager) tryPublishAppliedView(
	in *bulkheadapi.HandlerContext,
	desiredSnapshot *model.DesiredView,
	result *bulkheadapi.TopologyResult,
) bool {
	if in == nil || in.DesiredView == nil || desiredSnapshot == nil ||
		result == nil || !result.Converged || !result.FinalSnapshotCurrent || result.AppliedView == nil {
		return false
	}
	opts := m.cpuSetPartitionViewOptions(in.CPUSetAdjustmentHandlerCtx)
	finalDesired, err := bulkheadutils.BuildValidatedCPUSetPartitionView(in.State, in.Topology, opts)
	if err != nil {
		return false
	}
	if !model.EqualDesiredView(finalDesired, desiredSnapshot) {
		return false
	}
	return commitIfGenerationCurrent(in.CPUSetAdjustmentHandlerCtx, func() {
		m.appliedView = result.AppliedView.DeepCopy()
		m.appliedViewRevision++
		m.appliedViewValidForPeriodical = true
		in.AppliedView = m.appliedView.DeepCopy()
		in.AppliedViewRevision = m.appliedViewRevision
	})
}

func commitIfGenerationCurrent(in cpusetutil.CPUSetAdjustmentHandlerCtx, commit func()) bool {
	if in.CommitIfGenerationCurrent == nil {
		commit()
		return true
	}
	return in.CommitIfGenerationCurrent(in.Generation, commit)
}

func staleGenerationError() *NonConvergedError {
	return &NonConvergedError{Result: bulkheadapi.DAGApplyResult{
		FinalSnapshotCurrent: false,
	}}
}

func (m *Manager) cpuSetPartitionViewOptions(in cpusetutil.CPUSetAdjustmentHandlerCtx) bulkheadutils.CPUSetPartitionViewOptions {
	nonReclaimPoolMinSize := bulkheadNonReclaimPoolMinSize(in.DynamicConf)
	if nonReclaimPoolMinSize <= 0 {
		nonReclaimPoolMinSize = m.defaultNonReclaimPoolMinSize
	}
	opts := bulkheadutils.CPUSetPartitionViewOptions{
		NonReclaimPoolMinSize: nonReclaimPoolMinSize,
	}
	if in.CoreConf != nil {
		opts.ReserveCPUReversely = in.CoreConf.EnableReserveCPUReversely
	}
	return opts
}

func (m *Manager) buildPluginEnabledState(in bulkheadapi.HandlerContext) map[string]bool {
	out := make(map[string]bool, len(m.plugins))
	for _, p := range m.plugins {
		out[p.Name()] = p.Enable(in)
	}
	return out
}

// needsDisabledReset reports whether a currently-disabled plugin should run its
// disabled reset handler. A nil lastCPUSetAdjustmentEnabled means we have no
// prior state (e.g. after restart) and must reset once to converge.
func (m *Manager) needsDisabledReset(name string) bool {
	return m.lastCPUSetAdjustmentEnabled == nil || m.lastCPUSetAdjustmentEnabled[name]
}

func bulkheadEnabled(conf *dynamicconfig.Configuration) bool {
	if conf == nil || conf.AdminQoSConfiguration == nil || conf.AdminQoSConfiguration.CPUPluginConfiguration == nil {
		return false
	}
	return conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable
}

func bulkheadNonReclaimPoolMinSize(conf *dynamicconfig.Configuration) int64 {
	if conf == nil || conf.AdminQoSConfiguration == nil || conf.AdminQoSConfiguration.CPUPluginConfiguration == nil {
		return 0
	}
	return conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize
}

func (m *Manager) RunPeriodicalHandlers(
	coreConf *config.Configuration,
	extraConf interface{},
	dynamicConf *dynamicconfig.DynamicAgentConfiguration,
	emitter metrics.MetricEmitter,
	metaServer *metaserver.MetaServer,
) {
	ctx, cancel := context.WithTimeout(context.Background(), managerHandlerTimeout(coreConf))
	defer cancel()
	if err := m.mu.Lock(ctx); err != nil {
		_ = general.UpdateHealthzStateByError(cpuconsts.SyncBulkhead, err)
		general.ErrorS(err, "bulkhead periodical handlers failed to acquire manager lock")
		return
	}
	defer m.mu.Unlock()

	// Start timing after acquiring m.mu so the slow-handler log reflects the
	// actual handler execution time rather than lock-contention wait, which
	// would otherwise inflate elapsed and produce misleading slow warnings.
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

	var conf *dynamicconfig.Configuration
	if dynamicConf != nil {
		conf = dynamicConf.GetDynamicConfiguration()
	}
	if !bulkheadEnabled(conf) {
		// Keep the periodical path behind the same hard global gate as the
		// cpuset adjustment path. Periodical handlers may reconcile external
		// resources such as cpuset partitions or workqueue masks, so running them
		// while bulkhead is globally disabled would still mutate bulkhead-owned
		// state.
		return
	}
	handlerCtx := bulkheadapi.PeriodicalHandlerContext{
		CoreConf:                      coreConf,
		ExtraConf:                     extraConf,
		DynamicConf:                   conf,
		Emitter:                       emitter,
		MetaServer:                    metaServer,
		AppliedViewValidForPeriodical: m.appliedViewValidForPeriodical,
	}
	if m.appliedViewValidForPeriodical {
		handlerCtx.AppliedView = m.appliedView.DeepCopy()
		handlerCtx.AppliedViewRevision = m.appliedViewRevision
	}
	var errs []error
	for _, p := range m.plugins {
		pluginCtx := handlerCtx
		if enabled, ok := m.lastCPUSetAdjustmentEnabled[p.Name()]; ok {
			pluginCtx.EffectiveEnabled = &enabled
		}
		handlerStarted := time.Now()
		pluginErr := p.PeriodicalHandler(ctx, pluginCtx)
		handlerElapsed := time.Since(handlerStarted)
		if handlerElapsed >= bulkheadSlowHandlerThreshold {
			general.InfofV(2, "bulkhead periodical slow plugin=%s elapsed=%s", p.Name(), handlerElapsed)
		}
		if pluginErr != nil {
			wrapped := fmt.Errorf("bulkhead plugin %q periodical failed: %w", p.Name(), pluginErr)
			general.ErrorS(wrapped, "bulkhead periodical handler failed")
			emitBulkheadPluginResult(emitter, "periodical", p.Name(), "failed", pluginErr.Error())
			errs = append(errs, wrapped)
			continue
		}
		emitBulkheadPluginResult(emitter, "periodical", p.Name(), "success", "")
	}
	err = apierrors.NewAggregate(errs)
}

func managerHandlerTimeout(coreConf *config.Configuration) time.Duration {
	if coreConf == nil || coreConf.CPUQRMPluginConfig == nil {
		return bulkheadconfig.TopologyHandlerTimeout(nil)
	}
	return bulkheadconfig.TopologyHandlerTimeout(coreConf.CPUQRMPluginConfig.BulkheadConfiguration)
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
