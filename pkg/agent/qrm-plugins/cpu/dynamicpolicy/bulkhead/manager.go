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
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
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

type disabledTopologyResetState uint8

const (
	disabledTopologyResetNone disabledTopologyResetState = iota
	disabledTopologyResetPending
	disabledTopologyResetComplete
)

type Manager struct {
	mu                            cancelableMutex
	latestAppliedReclaimMu        sync.RWMutex
	plugins                       []bulkheadapi.Plugin
	defaultNonReclaimPoolMinSize  int64
	lastCPUSetAdjustmentEnabled   map[string]bool
	disabledTopologyResetStates   map[string]disabledTopologyResetState
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
	metricBulkheadHandlerResult                = "bulkhead_handler_result"
	metricBulkheadViewChanged                  = "bulkhead_view_changed"
	metricBulkheadPartitionCPUCores            = "bulkhead_partition_cpu_cores"
	metricBulkheadPartitionCPUDiffCores        = "bulkhead_partition_cpu_diff_cores"
	metricBulkheadDefaultShareResidualCPUCores = "bulkhead_default_share_residual_cpu_cores"
	bulkheadSlowHandlerThreshold               = 500 * time.Millisecond

	// cpusetTopologyPluginName is the well-known name of the topology plugin
	// that owns the authoritative applied view. It is referenced both when
	// gating dependent plugins and when stamping the reclaim commit override.
	cpusetTopologyPluginName = "cpuset_topology"
)

// applyLoopAccumulator groups the per-round mutable state threaded through the
// plugin execution loop. It is created fresh at the start of every Apply call
// and replaces the handful of bare local variables that were previously shared
// between the loop body, the topology callback, and the publish phase.
type applyLoopAccumulator struct {
	anyAdjusted       bool
	topologyPublished bool
	topologyStopped   bool
	topologyApplied   bool
	verifiedReclaim   machine.CPUSet
	topologyResult    bulkheadapi.DAGApplyResult
}

// preparedApply carries everything Apply needs after the preparation phase.
// desiredDefer, when non-nil, must be deferred by Apply so the "desired" view
// metrics are emitted on every return path, matching the original inline defer.
// shortCircuitDisabledGate, when true, tells Apply to return the empty CPUSet
// with a nil error (the global bulkhead-disabled gate).
type preparedApply struct {
	handlerCtx               *bulkheadapi.HandlerContext
	acc                      *applyLoopAccumulator
	currentEnabled           map[string]bool
	desiredSnapshot          *model.DesiredView
	desiredDefer             func()
	shortCircuitDisabledGate bool
}

type bulkheadPartitionMetricDescriptor struct {
	name     string
	cpuSet   func(*model.CPUSetPartitionView) machine.CPUSet
	emitDiff bool
}

var bulkheadPartitionMetricDescriptors = []bulkheadPartitionMetricDescriptor{
	{name: "reserve", cpuSet: func(view *model.CPUSetPartitionView) machine.CPUSet { return view.Reserve }},
	{name: "dedicated", cpuSet: func(view *model.CPUSetPartitionView) machine.CPUSet { return view.Dedicated }},
	{name: "share", cpuSet: func(view *model.CPUSetPartitionView) machine.CPUSet { return view.SharePool }},
	{name: "reclaim", cpuSet: func(view *model.CPUSetPartitionView) machine.CPUSet { return view.ReclaimEffective }, emitDiff: true},
	{name: "non_reclaim", cpuSet: func(view *model.CPUSetPartitionView) machine.CPUSet { return view.NonReclaimPool }, emitDiff: true},
	{name: "isolation", cpuSet: func(view *model.CPUSetPartitionView) machine.CPUSet { return view.Isolation }},
	{name: "default_share", cpuSet: func(view *model.CPUSetPartitionView) machine.CPUSet {
		return view.SharePoolMap[commonstate.PoolNameShare]
	}, emitDiff: true},
}

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
//
// The work is split into three phases: prepareApply validates inputs and builds
// the per-round handler context, executeApply runs the plugin loop, and
// publishApply commits the converged view and decides the return value. The
// manager lock, the apply-finished logging defer, and the "desired" metrics
// defer stay here so their ordering and late binding match the original.
func (m *Manager) Apply(ctx context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) (out machine.CPUSet, err error) {
	start := time.Now()
	var topologyAppliedLog bool
	var topologyPublishedLog bool
	var anyAdjustedLog bool
	defer func() {
		general.Infof("bulkhead: manager apply finished duration=%s err=%v any_adjusted=%t topology_applied=%t topology_published=%t generation=%d",
			time.Since(start), err, anyAdjustedLog, topologyAppliedLog, topologyPublishedLog, in.Generation)
	}()

	if err := m.mu.Lock(ctx); err != nil {
		return machine.NewCPUSet(), fmt.Errorf("acquire bulkhead manager lock: %w", err)
	}
	defer m.mu.Unlock()
	disabledRoundStart := time.Now()
	empty := machine.NewCPUSet()

	prepared, prepErr := m.prepareApply(in)
	if prepErr != nil {
		return empty, prepErr
	}
	if prepared.desiredDefer != nil {
		defer prepared.desiredDefer()
	}
	if prepared.shortCircuitDisabledGate {
		return empty, nil
	}

	if execErr := m.executeApply(ctx, in, prepared.handlerCtx, prepared.acc, prepared.currentEnabled, prepared.desiredSnapshot, disabledRoundStart); execErr != nil {
		return empty, execErr
	}

	resultCPUs, pubErr := m.publishApply(in, prepared.handlerCtx, prepared.acc, prepared.currentEnabled)
	if pubErr != nil {
		return empty, pubErr
	}
	topologyAppliedLog = prepared.acc.topologyApplied
	topologyPublishedLog = prepared.acc.topologyPublished
	anyAdjustedLog = prepared.acc.anyAdjusted
	return resultCPUs, nil
}

// prepareApply builds the per-round handler context, resolves ramp-up domains,
// applies the global bulkhead-disabled gate, and constructs the topology result
// callback. It returns the prepared state plus an optional desired-view metrics
// defer and an optional short-circuit CPUSet for the disabled-gate path.
func (m *Manager) prepareApply(in cpusetutil.CPUSetAdjustmentHandlerCtx) (*preparedApply, error) {
	prepared := &preparedApply{
		handlerCtx: &bulkheadapi.HandlerContext{
			CPUSetAdjustmentHandlerCtx: in,
			AppliedView:                m.appliedView.DeepCopy(),
			AppliedViewRevision:        m.appliedViewRevision,
		},
		acc: &applyLoopAccumulator{
			verifiedReclaim: machine.NewCPUSet(),
		},
	}
	if !commitIfGenerationCurrent(in, func() {
		m.appliedViewValidForPeriodical = false
	}) {
		return nil, staleGenerationError()
	}
	rampUpDomains, dErr := m.activeRampUpDomains(in)
	if dErr != nil {
		return nil, fmt.Errorf("resolve active ramp-up domains for bulkhead apply: %w", dErr)
	}
	viewOptions := m.cpuSetPartitionViewOptions(in, rampUpDomains)
	if !bulkheadEnabled(in.DynamicConf) {
		// The global bulkhead switch is a hard gate: when it is off, do not run
		// plugin Enable/adjust/disabled handlers. Disabled handlers may write
		// cgroup or sysfs rollback state, which is still bulkhead-owned behavior
		// and can introduce unexpected changes after the user explicitly turns
		// bulkhead off.
		if !commitIfGenerationCurrent(in, func() {
			m.lastCPUSetAdjustmentEnabled = nil
			m.disabledTopologyResetStates = nil
		}) {
			return nil, staleGenerationError()
		}
		emitBulkheadViewChanged(prepared.handlerCtx.Emitter, false)
		prepared.shortCircuitDisabledGate = true
		return prepared, nil
	}
	if in.State != nil {
		desiredView, err := bulkheadutils.BuildValidatedCPUSetPartitionView(in.State, in.Topology, viewOptions)
		if err != nil {
			return nil, fmt.Errorf("build bulkhead desired view failed: %w", err)
		}
		prepared.handlerCtx.DesiredView = desiredView
		prepared.handlerCtx.View = desiredView.CPUSetPartitionView.DeepCopy()
		hc := prepared.handlerCtx
		prepared.desiredDefer = func() {
			emitBulkheadPartitionViewMetrics(hc.Emitter, "desired", &hc.DesiredView.CPUSetPartitionView)
			if defaultShareResidualEnabled(in.DynamicConf) {
				emitBulkheadDefaultShareResidualMetric(hc.Emitter, "desired", &hc.DesiredView.CPUSetPartitionView)
			}
		}
	}
	prepared.currentEnabled = m.buildPluginEnabledState(*prepared.handlerCtx)
	prepared.desiredSnapshot = prepared.handlerCtx.DesiredView.DeepCopy()
	desiredSnapshot := prepared.desiredSnapshot
	hc := prepared.handlerCtx
	acc := prepared.acc
	prepared.handlerCtx.ReportTopologyResult = func(result bulkheadapi.TopologyResult) {
		result.AppliedView = result.AppliedView.DeepCopy()
		acc.topologyPublished = m.tryPublishAppliedView(hc, desiredSnapshot, &result)
	}
	return prepared, nil
}

// executeApply runs the plugin loop: it reconciles disabled plugins, runs the
// topology plugin, and then runs the remaining adjustment-capable plugins. It
// mutates the accumulator and handler context in place and returns the first
// terminal error. Every early return in the original loop returned the empty
// CPUSet together with the error, so collapsing them to a bare error preserves
// behavior.
func (m *Manager) executeApply(
	ctx context.Context,
	in cpusetutil.CPUSetAdjustmentHandlerCtx,
	handlerCtx *bulkheadapi.HandlerContext,
	acc *applyLoopAccumulator,
	currentEnabled map[string]bool,
	desiredSnapshot *model.DesiredView,
	disabledRoundStart time.Time,
) error {
	for _, p := range m.plugins {
		if !commitIfGenerationCurrent(in, func() {}) {
			return staleGenerationError()
		}
		if !currentEnabled[p.Name()] {
			leavingDisabledReconcile := false
			if reconciler, ok := p.(bulkheadapi.DisabledTopologyReconciler); ok {
				if reconciler.ShouldReconcileWhenDisabled(ctx, *handlerCtx) {
					disabledCtx, cancel := context.WithDeadline(ctx, disabledRoundStart.Add(managerTopologyDeadline(in.CoreConf)))
					if m.disabledTopologyResetState(p.Name()) != disabledTopologyResetComplete {
						if !commitIfGenerationCurrent(in, func() {
							m.setDisabledTopologyResetState(p.Name(), disabledTopologyResetPending)
						}) {
							cancel()
							return staleGenerationError()
						}
						adjuster, ok := p.(bulkheadapi.AdjustmentCapable)
						if !ok {
							cancel()
							return fmt.Errorf("bulkhead plugin %q is a disabled-topology reconciler without adjustment capability", p.Name())
						}
						err := adjuster.CPUSetAdjustmentDisabledHandler(disabledCtx, *handlerCtx)
						if err != nil {
							cancel()
							emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment_disabled", p.Name(), "failed", err.Error())
							return fmt.Errorf("bulkhead plugin %q disabled transition failed: %w", p.Name(), err)
						}
						if !commitIfGenerationCurrent(in, func() {
							m.setDisabledTopologyResetState(p.Name(), disabledTopologyResetComplete)
						}) {
							cancel()
							return staleGenerationError()
						}
						emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment_disabled", p.Name(), "success", "")
						acc.anyAdjusted = true
					}
					topologyCtx := *handlerCtx
					topologyCtx.ReportTopologyResult = nil
					result, err := reconciler.ReconcileDisabled(disabledCtx, topologyCtx)
					cancel()
					if !commitIfGenerationCurrent(in, func() {}) {
						return staleGenerationError()
					}
					if err != nil {
						emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "failed", err.Error())
						return fmt.Errorf("bulkhead plugin %q disabled reconciliation failed: %w", p.Name(), err)
					}
					if !result.FullyConverged || !result.FinalSnapshotCurrent || result.AppliedView == nil ||
						result.AppliedView.Level != model.AppliedViewLevelReclaimOnly {
						nonConverged := &NonConvergedError{Result: result}
						emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "failed", nonConverged.Error())
						return nonConverged
					}
					if err := m.acceptConvergedTopologyResult(in, handlerCtx, p.Name(), result, desiredSnapshot, acc, "after disabled topology reconcile"); err != nil {
						return err
					}
					continue
				}
				leavingDisabledReconcile = m.disabledTopologyResetState(p.Name()) != disabledTopologyResetNone
			}
			if !leavingDisabledReconcile && !m.needsDisabledReset(p.Name()) {
				if p.Name() == cpusetTopologyPluginName {
					acc.topologyStopped = true
				}
				continue
			}
			if leavingDisabledReconcile && !commitIfGenerationCurrent(in, func() {
				m.setDisabledTopologyResetState(p.Name(), disabledTopologyResetPending)
			}) {
				return staleGenerationError()
			}
			var err error
			if adjuster, ok := p.(bulkheadapi.AdjustmentCapable); ok {
				err = adjuster.CPUSetAdjustmentDisabledHandler(ctx, *handlerCtx)
			}
			if !commitIfGenerationCurrent(in, func() {
				if err == nil && leavingDisabledReconcile {
					m.setDisabledTopologyResetState(p.Name(), disabledTopologyResetNone)
				}
			}) {
				return staleGenerationError()
			}
			if err != nil {
				emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment_disabled", p.Name(), "failed", err.Error())
				return fmt.Errorf("bulkhead plugin %q disabled transition failed: %w", p.Name(), err)
			}
			emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment_disabled", p.Name(), "success", "")
			acc.anyAdjusted = true
			if p.Name() == cpusetTopologyPluginName {
				acc.topologyStopped = true
			}
			continue
		}
		if acc.topologyStopped {
			continue
		}
		if _, ok := p.(bulkheadapi.DisabledTopologyReconciler); ok {
			if !commitIfGenerationCurrent(in, func() {
				m.setDisabledTopologyResetState(p.Name(), disabledTopologyResetPending)
			}) {
				return staleGenerationError()
			}
		}
		if topologyPlugin, ok := p.(bulkheadapi.TopologyPlugin); ok {
			topologyCtx := *handlerCtx
			// The typed result is the sole publication path for TopologyPlugin.
			// Suppress the legacy callback so a dependent failure cannot publish
			// manager state from the middle of this transaction.
			topologyCtx.ReportTopologyResult = nil
			// A full topology attempt may write part of the hierarchy before it
			// fails. Mark the owner as requiring an authoritative disabled reset
			// before starting the attempt, rather than only after convergence.
			if !commitIfGenerationCurrent(in, func() {
				if m.lastCPUSetAdjustmentEnabled == nil {
					m.lastCPUSetAdjustmentEnabled = make(map[string]bool)
				}
				m.lastCPUSetAdjustmentEnabled[p.Name()] = true
			}) {
				return staleGenerationError()
			}
			result, err := topologyPlugin.Apply(ctx, topologyCtx)
			if !commitIfGenerationCurrent(in, func() {}) {
				return staleGenerationError()
			}
			if err != nil {
				emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "failed", err.Error())
				return fmt.Errorf("bulkhead plugin %q cpuset adjustment failed: %w", p.Name(), err)
			}
			successfulTopology := result.FullyConverged || result.ParentSafe
			if !successfulTopology || !result.FinalSnapshotCurrent || result.AppliedView == nil {
				nonConverged := &NonConvergedError{Result: result}
				emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "failed", nonConverged.Error())
				return nonConverged
			}
			// Rebuild desired intent after topology Apply so a result cannot be
			// accepted, or authorize dependent side effects, after state changed.
			if err := m.acceptConvergedTopologyResult(in, handlerCtx, p.Name(), result, desiredSnapshot, acc, "after topology apply"); err != nil {
				return err
			}
			if result.ParentSafe {
				// A parent-safe view proves partition/reclaim safety only. Do not
				// authorize dependent plugins that may require exact leaf state.
				acc.topologyStopped = true
			}
			continue
		}
		var err error
		if adjuster, ok := p.(bulkheadapi.AdjustmentCapable); ok {
			err = adjuster.CPUSetAdjustmentHandler(ctx, *handlerCtx)
		}
		if !commitIfGenerationCurrent(in, func() {}) {
			return staleGenerationError()
		}
		if err != nil {
			emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "failed", err.Error())
			return fmt.Errorf("bulkhead plugin %q cpuset adjustment failed: %w", p.Name(), err)
		}
		emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", p.Name(), "success", "")
		acc.anyAdjusted = true
		if p.Name() == cpusetTopologyPluginName {
			if !acc.topologyPublished {
				acc.topologyStopped = true
				continue
			}
		}
	}
	return nil
}

// acceptConvergedTopologyResult is shared by the disabled-reconcile and the
// full topology-apply success paths. It rebuilds the desired intent, rejects the
// result when the intent drifted, validates the hard partition, and commits the
// converged applied view into the per-round handler context and accumulator.
// phase is embedded in the rebuild error strings to preserve the original
// messages.
func (m *Manager) acceptConvergedTopologyResult(
	in cpusetutil.CPUSetAdjustmentHandlerCtx,
	handlerCtx *bulkheadapi.HandlerContext,
	pluginName string,
	result bulkheadapi.DAGApplyResult,
	desiredSnapshot *model.DesiredView,
	acc *applyLoopAccumulator,
	phase string,
) error {
	if desiredSnapshot != nil {
		currentRampUpDomains, dErr := m.activeRampUpDomains(in)
		if dErr != nil {
			return fmt.Errorf("resolve active ramp-up domains %s: %w", phase, dErr)
		}
		currentDesired, err := bulkheadutils.BuildValidatedCPUSetPartitionView(
			in.State,
			in.Topology,
			m.cpuSetPartitionViewOptions(in, currentRampUpDomains),
		)
		if err != nil {
			return fmt.Errorf("rebuild bulkhead desired view %s failed: %w", phase, err)
		}
		if !model.EqualDesiredView(currentDesired, desiredSnapshot) {
			result.FinalSnapshotCurrent = false
			nonConverged := &NonConvergedError{Result: result}
			emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", pluginName, "failed", nonConverged.Error())
			return nonConverged
		}
	}
	if err := m.validateAppliedHardPartition(in, result.AppliedView); err != nil {
		return fmt.Errorf("validate bulkhead topology applied view: %w", err)
	}
	handlerCtx.AppliedView = result.AppliedView.DeepCopy()
	handlerCtx.View = handlerCtx.AppliedView.CPUSetPartitionView.DeepCopy()
	acc.verifiedReclaim = handlerCtx.AppliedView.ReclaimEffective.Clone()
	handlerCtx.AppliedViewRevision = m.appliedViewRevision
	if !model.EqualAppliedView(m.appliedView, handlerCtx.AppliedView) {
		handlerCtx.AppliedViewRevision++
	}
	acc.topologyResult = result
	acc.topologyApplied = true
	acc.topologyPublished = true
	acc.anyAdjusted = true
	emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", pluginName, "success", "")
	return nil
}

// publishApply commits the converged applied view (or merely the enabled-state
// map when topology did not run), emits the applied metrics, and decides the
// reclaim CPUSet to return. The caller records the apply-finished log flags
// from the accumulator after a successful publish.
func (m *Manager) publishApply(
	in cpusetutil.CPUSetAdjustmentHandlerCtx,
	handlerCtx *bulkheadapi.HandlerContext,
	acc *applyLoopAccumulator,
	currentEnabled map[string]bool,
) (machine.CPUSet, error) {
	empty := machine.NewCPUSet()
	if acc.topologyApplied {
		if !commitIfGenerationCurrent(in, func() {
			m.appliedView = handlerCtx.AppliedView.DeepCopy()
			m.appliedViewRevision = handlerCtx.AppliedViewRevision
			m.appliedViewValidForPeriodical = acc.topologyResult.FullyConverged
			m.publishLatestAppliedReclaim(acc.verifiedReclaim)
			m.lastCPUSetAdjustmentEnabled = currentEnabled
		}) {
			acc.topologyResult.FinalSnapshotCurrent = false
			nonConverged := staleGenerationError()
			nonConverged.Result = acc.topologyResult
			emitBulkheadPluginResult(handlerCtx.Emitter, "cpuset_adjustment", "generation_fence", "failed", nonConverged.Error())
			return empty, nonConverged
		}
		emitBulkheadPartitionViewMetrics(handlerCtx.Emitter, "applied", &handlerCtx.AppliedView.CPUSetPartitionView)
		emitBulkheadPartitionDiffMetrics(handlerCtx.Emitter, handlerCtx.DesiredView, handlerCtx.AppliedView)
		if defaultShareResidualEnabled(in.DynamicConf) {
			emitBulkheadDefaultShareResidualMetric(handlerCtx.Emitter, "applied", &handlerCtx.AppliedView.CPUSetPartitionView)
		}
	} else {
		if !commitIfGenerationCurrent(in, func() {
			m.lastCPUSetAdjustmentEnabled = currentEnabled
		}) {
			return empty, staleGenerationError()
		}
	}
	emitBulkheadViewChanged(handlerCtx.Emitter, acc.anyAdjusted)
	if acc.topologyApplied {
		if acc.topologyResult.FullyConverged && handlerCtx.CommitOverride != nil && !acc.verifiedReclaim.IsEmpty() {
			handlerCtx.CommitOverride.ReclaimEffective = acc.verifiedReclaim.Clone()
			handlerCtx.CommitOverride.Source = cpusetTopologyPluginName
		}
		return acc.verifiedReclaim.Clone(), nil
	}
	if acc.topologyPublished && m.appliedView != nil {
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
	rampUpDomains, dErr := m.activeRampUpDomains(in.CPUSetAdjustmentHandlerCtx)
	if dErr != nil {
		klog.Errorf("bulkhead: skip publishing applied view because active ramp-up domains could not be resolved: %v", dErr)
		return false
	}
	opts := m.cpuSetPartitionViewOptions(in.CPUSetAdjustmentHandlerCtx, rampUpDomains)
	finalDesired, err := bulkheadutils.BuildValidatedCPUSetPartitionView(in.State, in.Topology, opts)
	if err != nil {
		return false
	}
	if !model.EqualDesiredView(finalDesired, desiredSnapshot) {
		return false
	}
	if err := bulkheadutils.ValidateHardPartitionAppliedView(result.AppliedView, in.State, in.Topology, opts); err != nil {
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

// activeRampUpDomains resolves the ramp-up reclaim domains recorded in the
// current pod entries. A resolution error is propagated to the caller: silently
// collapsing to an empty set would disable the hard-partition reclaim floor on
// ambiguous state, which is under-protection rather than a safe fallback.
func (m *Manager) activeRampUpDomains(in cpusetutil.CPUSetAdjustmentHandlerCtx) (sets.Int, error) {
	if in.State == nil || in.Topology == nil {
		return sets.NewInt(), nil
	}
	return in.State.GetPodEntries().ActiveRampUpDomains(in.Topology)
}

func (m *Manager) cpuSetPartitionViewOptions(
	in cpusetutil.CPUSetAdjustmentHandlerCtx,
	rampUpDomains sets.Int,
) bulkheadutils.CPUSetPartitionViewOptions {
	opts := bulkheadutils.NewCPUSetPartitionViewOptionsWithState(
		in.CoreConf,
		in.DynamicConf,
		in.Topology,
		bulkheadutils.CPUSetPartitionViewState{
			State:                         in.State,
			ReservedCPUs:                  in.ReservedCPUs,
			ReservedReclaimedCPUs:         in.ReservedReclaimedCPUs,
			ReservedReclaimedCPUsFallback: in.ReservedReclaimedCPUsSize,
		},
		rampUpDomains,
	)
	if opts.NonReclaimPoolMinSize <= 0 {
		opts.NonReclaimPoolMinSize = m.defaultNonReclaimPoolMinSize
	}
	return opts
}

func (m *Manager) validateAppliedHardPartition(
	in cpusetutil.CPUSetAdjustmentHandlerCtx,
	applied *model.AppliedView,
) error {
	if applied != nil &&
		applied.Level == model.AppliedViewLevelReclaimOnly &&
		applied.ReclaimEffective.IsEmpty() {
		return nil
	}
	rampUpDomains, dErr := m.activeRampUpDomains(in)
	if dErr != nil {
		return fmt.Errorf("resolve active ramp-up domains for hard-partition validation: %w", dErr)
	}
	opts := m.cpuSetPartitionViewOptions(in, rampUpDomains)
	if opts.HardPartitionTargetError != nil {
		return opts.HardPartitionTargetError
	}
	if !opts.HardPartitionEnabled {
		return nil
	}
	if applied == nil {
		return fmt.Errorf("missing applied view")
	}
	return bulkheadutils.ValidateHardPartitionAppliedView(applied, in.State, in.Topology, opts)
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

func (m *Manager) disabledTopologyResetState(name string) disabledTopologyResetState {
	if m.disabledTopologyResetStates == nil {
		return disabledTopologyResetNone
	}
	return m.disabledTopologyResetStates[name]
}

func (m *Manager) setDisabledTopologyResetState(name string, state disabledTopologyResetState) {
	if m.disabledTopologyResetStates == nil {
		m.disabledTopologyResetStates = make(map[string]disabledTopologyResetState)
	}
	m.disabledTopologyResetStates[name] = state
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
		var pluginErr error
		if periodical, ok := p.(bulkheadapi.PeriodicalCapable); ok {
			pluginErr = periodical.PeriodicalHandler(ctx, pluginCtx)
		}
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

func managerTopologyDeadline(coreConf *config.Configuration) time.Duration {
	if coreConf == nil || coreConf.CPUQRMPluginConfig == nil ||
		coreConf.CPUQRMPluginConfig.BulkheadConfiguration == nil {
		return bulkheadconfig.DefaultTopologyConvergenceDeadline
	}
	deadline := coreConf.CPUQRMPluginConfig.BulkheadConfiguration.TopologyConvergenceBudget.DeadlineDuration
	if deadline <= 0 {
		return bulkheadconfig.DefaultTopologyConvergenceDeadline
	}
	return deadline
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

func emitBulkheadPartitionViewMetrics(
	emitter metrics.MetricEmitter,
	viewName string,
	view *model.CPUSetPartitionView,
) {
	if emitter == nil || view == nil {
		return
	}
	for _, descriptor := range bulkheadPartitionMetricDescriptors {
		_ = emitter.StoreInt64(metricBulkheadPartitionCPUCores, int64(descriptor.cpuSet(view).Size()), metrics.MetricTypeNameRaw,
			metrics.MetricTag{Key: "view", Val: viewName},
			metrics.MetricTag{Key: "partition", Val: descriptor.name},
		)
	}
}

func emitBulkheadDefaultShareResidualMetric(
	emitter metrics.MetricEmitter,
	viewName string,
	view *model.CPUSetPartitionView,
) {
	if emitter == nil || view == nil {
		return
	}
	_ = emitter.StoreInt64(metricBulkheadDefaultShareResidualCPUCores,
		int64(view.SharePoolMap[commonstate.PoolNameShare].Size()), metrics.MetricTypeNameRaw,
		metrics.MetricTag{Key: "view", Val: viewName},
	)
}

func defaultShareResidualEnabled(conf *dynamicconfig.Configuration) bool {
	return conf != nil && conf.FillDefaultSharePoolWithNonReclaimCPUs
}

func emitBulkheadPartitionDiffMetrics(
	emitter metrics.MetricEmitter,
	desired *model.DesiredView,
	applied *model.AppliedView,
) {
	if emitter == nil || desired == nil || applied == nil {
		return
	}
	for _, descriptor := range bulkheadPartitionMetricDescriptors {
		if !descriptor.emitDiff {
			continue
		}
		desiredCPUSet := descriptor.cpuSet(&desired.CPUSetPartitionView)
		appliedCPUSet := descriptor.cpuSet(&applied.CPUSetPartitionView)
		_ = emitter.StoreInt64(metricBulkheadPartitionCPUDiffCores,
			int64(desiredCPUSet.Difference(appliedCPUSet).Size()), metrics.MetricTypeNameRaw,
			metrics.MetricTag{Key: "partition", Val: descriptor.name},
			metrics.MetricTag{Key: "direction", Val: "desired_only"},
		)
		_ = emitter.StoreInt64(metricBulkheadPartitionCPUDiffCores,
			int64(appliedCPUSet.Difference(desiredCPUSet).Size()), metrics.MetricTypeNameRaw,
			metrics.MetricTag{Key: "partition", Val: descriptor.name},
			metrics.MetricTag{Key: "direction", Val: "applied_only"},
		)
	}
}
