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

package cpusettopology

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/util/errors"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	bulkheadutils "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	metapod "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	cgroupclient "github.com/kubewharf/katalyst-core/pkg/util/cgroup/client"
	cgcommon "github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
)

const CPUSetTopologyPluginName = "cpuset_topology"

const (
	defaultPendingPodProtectionTTL = 10 * time.Second
	defaultDeferredLeafDrainTTL    = 2 * time.Minute
)

var (
	_ bulkheadapi.Plugin                     = (*CPUSetTopologyPlugin)(nil)
	_ bulkheadapi.TopologyPlugin             = (*CPUSetTopologyPlugin)(nil)
	_ bulkheadapi.DisabledTopologyReconciler = (*CPUSetTopologyPlugin)(nil)
)

var errReclaimClassificationChanged = errors.New("reclaim path classification changed")
var errPendingPodScopeAmbiguous = errors.New("pending pod scope is ambiguous")

type CPUSetTopologyPlugin struct {
	cfg                bulkheadconfig.BulkheadConfiguration
	cgroup             cgroupclient.CgroupClient
	now                func() time.Time
	pendingProtections map[string]pendingPodProtection
	deferredLeafDrains map[string]deferredLeafDrain
	modeGateMu         sync.Mutex
	modeGate           *topology.ModeGate
}

type pendingPodProtection struct {
	rel          string
	current      machine.CPUSet
	protectUntil time.Time
}

type deferredLeafDrain struct {
	target       machine.CPUSet
	firstSeen    time.Time
	lastSeen     time.Time
	protectUntil time.Time
}

type disabledResetNotConvergedError struct {
	state   topology.ConvergenceState
	applied int
	report  topology.ConvergenceReport
}

func (e *disabledResetNotConvergedError) Error() string {
	return fmt.Sprintf("disabled reset topology dag not converged: state=%s applied=%d report=%+v", e.state, e.applied, e.report)
}

type topologyApplyNonConvergedError struct {
	result topology.ConvergenceResult
}

func (e *topologyApplyNonConvergedError) Error() string {
	return fmt.Sprintf("apply bulkhead topology dag not converged: state=%s report=%+v",
		e.result.State, e.result.ConvergenceReport)
}

func NewCPUSetTopologyPlugin(conf *config.Configuration) bulkheadapi.Plugin {
	var cfg bulkheadconfig.BulkheadConfiguration
	if conf != nil && conf.CPUQRMPluginConfig != nil && conf.CPUQRMPluginConfig.BulkheadConfiguration != nil {
		cfg = *conf.CPUQRMPluginConfig.BulkheadConfiguration
	}
	return &CPUSetTopologyPlugin{
		cfg:                cfg,
		cgroup:             cgroupclient.NewCgroupClient(),
		now:                time.Now,
		pendingProtections: map[string]pendingPodProtection{},
		deferredLeafDrains: map[string]deferredLeafDrain{},
		modeGate:           topology.NewModeGate(),
	}
}

func (p *CPUSetTopologyPlugin) Name() string { return CPUSetTopologyPluginName }

func (p *CPUSetTopologyPlugin) Enable(in bulkheadapi.HandlerContext) bool {
	if p.disabledOnCgroupV2(context.Background()) {
		return false
	}
	return enableBulkheadCpusetTopology(in)
}

func (p *CPUSetTopologyPlugin) ShouldReconcileWhenDisabled(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
) bool {
	if !p.cfg.PreserveReclaimCPUSetWhenTopologyDisabled ||
		enableBulkheadCpusetTopology(in) ||
		(in.State != nil && in.State.GetAllowSharedCoresOverlapReclaimedCores()) {
		return false
	}
	return !p.disabledOnCgroupV2(ctx)
}

// disabledOnCgroupV2 reports whether the cpuset_topology plugin must stay inert
// on the current host. On cgroup v2 the plugin runs only when explicitly opted
// in via EnableBulkheadCpusetTopologyOnCgroupV2; cgroup v1 is never gated. This
// gate is applied at every mutation entry point (Enable, the disabled reset
// handler, and the periodical handler) so a cgroup v2 host is never touched
// when the opt-in is off, even though the manager may still invoke the disabled
// reset handler after a previously-enabled round.
func (p *CPUSetTopologyPlugin) disabledOnCgroupV2(ctx context.Context) bool {
	if p.cfg.EnableBulkheadCpusetTopologyOnCgroupV2 {
		return false
	}
	return p.cgroup.Version(ctx) == cgroupclient.CgroupVersionV2
}

func (p *CPUSetTopologyPlugin) Apply(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
) (out bulkheadapi.DAGApplyResult, err error) {
	start := time.Now()
	defer func() {
		general.Infof("cpuset_topology: plugin apply finished duration=%s err=%v desired_view_nil=%t",
			time.Since(start), err, in.DesiredView == nil)
	}()

	var published *bulkheadapi.TopologyResult
	report := in.ReportTopologyResult
	in.ReportTopologyResult = func(result bulkheadapi.TopologyResult) {
		copied := result
		copied.AppliedView = result.AppliedView.DeepCopy()
		published = &copied
		if report != nil {
			report(result)
		}
	}

	err = p.CPUSetAdjustmentHandler(ctx, in)
	var nonConverged *topologyApplyNonConvergedError
	if errors.As(err, &nonConverged) {
		return dagApplyResultFromConvergence(nonConverged.result), nil
	}
	if err != nil {
		return bulkheadapi.DAGApplyResult{}, err
	}
	if published == nil {
		return bulkheadapi.DAGApplyResult{
			FullyConverged:       in.DesiredView == nil,
			FinalSnapshotCurrent: in.DesiredView == nil,
			AppliedView:          nil,
		}, nil
	}
	if published.AppliedView == nil {
		return bulkheadapi.DAGApplyResult{}, fmt.Errorf("converged topology result is missing final-snapshot AppliedView")
	}
	return dagApplyResultFromTopologyResult(*published), nil
}

func dagApplyResultFromTopologyResult(result bulkheadapi.TopologyResult) bulkheadapi.DAGApplyResult {
	return bulkheadapi.DAGApplyResult{
		Attempted:            result.Attempted,
		Applied:              result.Applied,
		Skipped:              result.Skipped,
		Failed:               result.Failed,
		Deferred:             result.Deferred,
		FullyConverged:       result.Converged,
		ParentSafe:           result.ParentSafe,
		DeferredLeafCount:    result.DeferredLeafCount,
		DeferredCPUCount:     result.DeferredCPUCount,
		FinalSnapshotCurrent: result.FinalSnapshotCurrent,
		ConvergenceReport:    result.ConvergenceReport,
		AppliedView:          result.AppliedView.DeepCopy(),
	}
}

func dagApplyResultFromConvergence(result topology.ConvergenceResult) bulkheadapi.DAGApplyResult {
	out := bulkheadapi.DAGApplyResult{
		Attempted:            result.Attempted,
		Applied:              result.Applied,
		Skipped:              result.Skipped,
		Failed:               result.Failed,
		Deferred:             result.Deferred,
		FullyConverged:       result.Converged,
		ParentSafe:           result.ParentSafe,
		DeferredLeafCount:    result.DeferredLeafCount,
		DeferredCPUCount:     result.DeferredCPUCount,
		FinalSnapshotCurrent: result.FinalSnapshotCurrent,
		ConvergenceReport:    result.ConvergenceReport,
	}
	return out
}

func topologyResultFromFinalConvergence(
	result topology.ConvergenceResult,
	appliedView *model.AppliedView,
) bulkheadapi.TopologyResult {
	applied := appliedView.DeepCopy()
	if applied != nil {
		applied.Level = model.AppliedViewLevelFull
		if result.ParentSafe {
			applied.Level = model.AppliedViewLevelParentSafe
		}
	}
	return bulkheadapi.TopologyResult{
		Attempted:            result.Attempted,
		Applied:              result.Applied,
		Skipped:              result.Skipped,
		Failed:               result.Failed,
		Deferred:             result.Deferred,
		Converged:            result.Converged,
		ParentSafe:           result.ParentSafe,
		LeafDeferred:         result.ParentSafe && result.DeferredLeafCount > 0,
		DeferredLeafCount:    result.DeferredLeafCount,
		DeferredCPUCount:     result.DeferredCPUCount,
		FinalSnapshotCurrent: result.FinalSnapshotCurrent,
		ConvergenceReport:    result.ConvergenceReport,
		AppliedView:          applied,
	}
}

type topologyAdjustmentAttempt func(
	context.Context,
	bulkheadapi.HandlerContext,
	*topology.AdjustmentBudget,
) (topology.ConvergenceResult, error)

func (p *CPUSetTopologyPlugin) CPUSetAdjustmentHandler(ctx context.Context, in bulkheadapi.HandlerContext) error {
	handlerStartedAt := time.Now()
	if p.cfg.EnableAdmissionLeafDefer &&
		in.Mode.OrFullDefault() == cpusetutil.CPUSetAdjustmentModeAdmission &&
		p.cfg.AdmissionSafeDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, handlerStartedAt.Add(p.cfg.AdmissionSafeDuration))
		defer cancel()
	}
	if in.DesiredView == nil {
		return nil
	}
	budget := topology.NewAdjustmentBudget(
		ctx, topologyBudgetFromConfig(p.cfg.TopologyConvergenceBudget))
	return runCPUSetTopologyAdjustment(ctx, in, budget, p.adjustOnce)
}

func runCPUSetTopologyAdjustment(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
	budget *topology.AdjustmentBudget,
	attempt topologyAdjustmentAttempt,
) error {
	_, err := runCPUSetTopologyAdjustmentWithResult(ctx, in, budget, attempt)
	return err
}

func runCPUSetTopologyAdjustmentWithResult(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
	budget *topology.AdjustmentBudget,
	attempt topologyAdjustmentAttempt,
) (topology.ConvergenceResult, error) {
	if in.DesiredView == nil {
		return topology.ConvergenceResult{}, nil
	}
	pristineDesired := in.DesiredView.DeepCopy()
	var cumulative topology.ConvergenceResult
	var lastStale error
	terminal := func(err error) (topology.ConvergenceResult, error) {
		cumulative.Converged = false
		cumulative.ParentSafe = false
		cumulative.State = topology.ConvergenceStateNonConverged
		cumulative.FinalSnapshot = nil
		cumulative.FinalSnapshotCurrent = false
		cumulative.ConvergenceReport.FullyConverged = false
		cumulative.DeferredLeafCount = 0
		cumulative.DeferredCPUCount = 0
		cumulative.Published = false
		cumulative.ReplanDisposition = topology.ReplanNotAllowed
		return cumulative, err
	}
	iteration := 0
	for {
		if err := ctx.Err(); err != nil {
			return terminal(budget.ExhaustionError(topology.ErrAdjustmentDeadlineExceeded, lastStale))
		}
		iteration++
		attemptInput := in
		attemptInput.DesiredView = pristineDesired.DeepCopy()
		result, err := attempt(ctx, attemptInput, budget)
		accumulateConvergenceResult(&cumulative, result)
		general.InfofV(4, "cpuset_topology: adjustment attempt finished iteration=%d disposition=%d err=%v",
			iteration, result.ReplanDisposition, err)
		deadlinePrimary, deadlineHistory := err, lastStale
		if deadlinePrimary == nil {
			deadlinePrimary, deadlineHistory = lastStale, nil
		}
		if deadlineErr := budget.DeadlineErrorWithHistory(
			deadlinePrimary, deadlineHistory); deadlineErr != nil {
			return terminal(deadlineErr)
		}
		if err == nil {
			return cumulative, nil
		}
		if errors.Is(err, topology.ErrAdjustmentWriteBudgetExceeded) {
			if lastStale == nil {
				lastStale = err
			}
			return terminal(budget.ExhaustionError(topology.ErrAdjustmentWriteBudgetExceeded, lastStale))
		}
		if !budget.ReplanSafe(result.ReplanDisposition) {
			return terminal(err)
		}
		lastStale = err
		if result.ReplanDisposition == topology.ReplanSafeFromVerifiedFinalState {
			if !result.FinalSnapshotCurrent || result.FinalSnapshot == nil {
				return terminal(err)
			}
			budget.StartFromVerifiedFinalSnapshot(result.FinalSnapshot)
		} else {
			budget.ClearInitialSnapshot()
		}
		if exhaustionErr := budget.ConsumeReplan(err); exhaustionErr != nil {
			return terminal(exhaustionErr)
		}
	}
}

func (p *CPUSetTopologyPlugin) adjustOnce(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
	budget *topology.AdjustmentBudget,
) (topology.ConvergenceResult, error) {
	var result topology.ConvergenceResult
	err := p.adjustOnceWithResult(ctx, in, budget, &result)
	return result, err
}

func (p *CPUSetTopologyPlugin) adjustOnceWithResult(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
	adjustmentBudget *topology.AdjustmentBudget,
	attemptResult *topology.ConvergenceResult,
) error {
	relExists := func(rel string) error {
		_, err := p.cgroup.StatDir(ctx, rel)
		return err
	}
	buildExpectedStart := time.Now()
	expectedRes, err := p.buildExpectedCPUSetByRel(ctx, in)
	if expectedRes == nil {
		expectedRes = &expectedCPUSetBuildResult{}
	}
	general.Infof("cpuset_topology: build expected cpuset finished duration=%s err=%v expected_leaf_count=%d pending_count=%d pending_cpu_count=%d",
		time.Since(buildExpectedStart), err, len(expectedRes.ExpectedByRel), len(expectedRes.PendingByPod), expectedRes.PendingCPUSetUnion().Size())
	if deadlineErr := admissionStageDeadlineError(ctx, "build expected container cpuset"); deadlineErr != nil {
		return deadlineErr
	}
	if err != nil {
		// Only non-pending resolve failures (illegal rel, cgroup/metaserver
		// internal error) reach here. Pending containers (admit window, no
		// container id yet) are classified as protected-pending and do NOT
		// produce an error, so a normal new-pod admit is never rejected.
		emitBulkheadPruneResult(in.Emitter, "skipped", "container_error")
		return fmt.Errorf("build expected container cpuset: %w", err)
	}
	discoveredSiblings, err := p.discoverBulkheadReclaimSiblings(ctx, in.DesiredView)
	if deadlineErr := admissionStageDeadlineError(ctx, "discover bulkhead reclaim siblings"); deadlineErr != nil {
		return deadlineErr
	}
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", "discover_error")
		return fmt.Errorf("discover bulkhead reclaim siblings: %w", err)
	}
	siblings := p.mergeBulkheadReclaimSiblings(
		discoveredSiblings,
		p.configuredBulkheadReclaimSiblings(),
		desiredCPUSetPartitionView(in.DesiredView),
	)
	var cpuDetails machine.CPUDetails
	if in.Topology != nil {
		cpuDetails = in.Topology.CPUDetails
	}
	specs, err := bulkheadutils.BuildTopologyNodeSpecsFromView(p.cfg, desiredCPUSetPartitionView(in.DesiredView), cpuDetails, siblings, relExists)
	if deadlineErr := admissionStageDeadlineError(ctx, "build bulkhead topology inputs"); deadlineErr != nil {
		return deadlineErr
	}
	if err != nil {
		return fmt.Errorf("build bulkhead topology inputs: %w", err)
	}
	dag, err := topology.BuildDAG(specs)
	if deadlineErr := admissionStageDeadlineError(ctx, "build bulkhead topology dag"); deadlineErr != nil {
		return deadlineErr
	}
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", "dag_error")
		return fmt.Errorf("build bulkhead topology dag: %w", err)
	}
	protections, err := p.pendingProtectionScopes(ctx, dag, expectedRes.PendingByPod)
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", "pending_scope_error")
		return fmt.Errorf("resolve pending protection scopes: %w", err)
	}
	if err := p.ensureBulkheadReclaimSiblingDirs(ctx, siblings); err != nil {
		return fmt.Errorf("ensure bulkhead reclaim sibling cgroups: %w", err)
	}
	protectedPending := pendingProtectionCPUSetUnion(protections)
	protectedByRel := pendingProtectedCPUSetByResolvedScopes(protections)
	if deadlineErr := admissionStageDeadlineError(ctx, "protect pending container cpuset"); deadlineErr != nil {
		return deadlineErr
	}
	if protected := protectedPending.Union(unionCPUSetByRel(protectedByRel)); topologyCoversProtectedView(in.Topology, in.DesiredView, protected) {
		general.InfofV(5, "bulkhead: applying transient pending protection, pending_count=%d protected_rel_count=%d protected_union=%s protected_by_rel=%s desired_reclaim=%s desired_reclaim_per_numa=%s reclaim_before=%s reclaim_per_numa_before=%s",
			len(expectedRes.PendingByPod), len(protectedByRel), protected.String(), formatCPUSetByRel(protectedByRel),
			in.DesiredView.DesiredReclaimEffective.String(), formatCPUSetByNUMA(in.DesiredView.DesiredReclaimEffectivePerNUMA),
			in.DesiredView.ReclaimEffective.String(), formatCPUSetByNUMA(in.DesiredView.ReclaimEffectivePerNUMA))
		// Pending allocations have no leaf cgroup to update yet. Keep their CPUs
		// protected in controlled ancestors so those ancestors do not shrink during
		// the admission creation window before the leaf becomes available.
		bulkheadutils.ApplyTransientProtectedNonReclaim(in.DesiredView, in.Topology, protected)
		general.InfofV(5, "bulkhead: transient pending protection applied, protected_union=%s transient_per_numa=%s reclaim_after=%s reclaim_per_numa_after=%s non_reclaim_after=%s",
			protected.String(), formatCPUSetByNUMA(in.DesiredView.TransientProtectedNonReclaimPerNUMA),
			in.DesiredView.ReclaimEffective.String(), formatCPUSetByNUMA(in.DesiredView.ReclaimEffectivePerNUMA),
			in.DesiredView.NonReclaimPool.String())
		if err := bulkheadutils.ValidateCPUSetPartitionView(in.DesiredView, in.Topology); err != nil {
			emitBulkheadPruneResult(in.Emitter, "skipped", "view_error")
			return fmt.Errorf("validate bulkhead desired view after transient pending protection: %w", err)
		}
		if p.cfg.EnableAdmissionLeafDefer && in.Mode.OrFullDefault() == cpusetutil.CPUSetAdjustmentModeAdmission {
			p.reclassifyAdmissionDeferredLeaves(ctx, in.DesiredView, expectedRes)
		}
		// Transient protection changes desired primary/reclaim CPU sets. Rebuild
		// the DAG so the coordinator receives those updated targets while keeping
		// scope resolution anchored to the already validated DAG boundary.
		specs, err = bulkheadutils.BuildTopologyNodeSpecsFromView(
			p.cfg, desiredCPUSetPartitionView(in.DesiredView), cpuDetails, siblings, relExists)
		if err != nil {
			return fmt.Errorf("rebuild protected bulkhead topology inputs: %w", err)
		}
		dag, err = topology.BuildDAG(specs)
		if err != nil {
			return fmt.Errorf("rebuild protected bulkhead topology dag: %w", err)
		}
	}
	requiredCPUSetByRel := topology.RequiredCPUSetByRelFromNodeSpecs(specs)
	p.recordDeferredLeafDrains(expectedRes.DeferredLeafByRel)
	if err := p.drainSafeDeferredLeaves(ctx, in.DesiredView, dag, adjustmentBudget); err != nil {
		return fmt.Errorf("drain deferred cpuset leaves: %w", err)
	}
	general.InfofV(5, "cpuset_topology: apply start specs=%d siblings=%d expected_leaf_count=%d pending_count=%d protected_pending=%s protected_rel_count=%d",
		len(specs), len(siblings), len(expectedRes.ExpectedByRel), len(protections),
		protectedPending.String(), len(protectedByRel))
	reservedCPUSet := in.DesiredView.Reserve
	objective := topology.ConvergenceObjectiveFull
	if p.cfg.EnableAdmissionLeafDefer && in.Mode.OrFullDefault() == cpusetutil.CPUSetAdjustmentModeAdmission {
		objective = topology.ConvergenceObjectiveParentSafe
	}
	// Normal adjustment passes the topology explicitly so TopologyCoordinator can
	// derive its allowed CPUs from this round's machine view. Any apply error is
	// returned to the bulkhead manager through this handler; this plugin does
	// not attempt a local retry or partial recovery.
	convergeStart := time.Now()
	var finalAppliedView *model.AppliedView
	convergenceBudget := adjustmentBudget.RemainingConvergenceBudget()
	res, err := (topology.TopologyCoordinator{}).Converge(ctx, topology.CoordinatorInput{
		DAG:                 dag,
		Cgroup:              p.cgroup,
		Mode:                topology.NormalModeGuardWithGate(p.sharedModeGate()),
		Budget:              convergenceBudget,
		AdjustmentBudget:    adjustmentBudget,
		DrainSelection:      topologyDrainSelectionFromConfig(p.cfg.TopologyDrainSelection),
		CPUDetails:          cpuDetails,
		ReservedCPUSet:      reservedCPUSet,
		InitialSnapshot:     adjustmentBudget.InitialSnapshot(),
		ExpectedCPUSetByRel: expectedRes.ExpectedByRel,
		RequiredCPUSetByRel: requiredCPUSetByRel,
		Objective:           objective,
		DeferredCPUSetByRel: expectedRes.DeferredLeafByRel,
		AdmissionBudget: &topology.AdmissionConvergenceBudget{
			MaxRequiredWrites: p.cfg.AdmissionMaxRequiredWrites,
		},
		PendingProtections:   protections,
		ProtectedCPUSetByRel: protectedByRel,
		PublishFinalSnapshot: func(snapshot *topology.CompleteSnapshot) error {
			appliedView, err := appliedViewFromFinalSnapshotWithContext(
				ctx, in.MetaServer, in.DesiredView, dag, snapshot,
				expectedRes.LifecycleProofs, nil)
			if err != nil {
				return fmt.Errorf("derive applied view from final topology snapshot: %w", err)
			}
			finalAppliedView = appliedView
			return nil
		},
		PublishParentSafeSnapshot: func(snapshot *topology.CompleteSnapshot, deferredCleanupRels map[string]struct{}) error {
			appliedView, err := appliedViewFromFinalSnapshotWithDeferredCleanup(
				ctx, in.MetaServer, in.DesiredView, dag, snapshot, deferredCleanupRels,
				expectedRes.LifecycleProofs)
			if err != nil {
				return fmt.Errorf("derive parent-safe applied view from final topology snapshot: %w", err)
			}
			p.recordDeferredLeafDrains(expectedRes.DeferredLeafByRel)
			finalAppliedView = appliedView
			return nil
		},
	})
	if attemptResult != nil {
		*attemptResult = res
	}
	general.Infof("cpuset_topology: coordinator converge finished duration=%s err=%v attempted=%d applied=%d skipped=%d failed=%d deferred=%d converged=%t final_snapshot_current=%t state=%s expected_leaf_count=%d pending_count=%d pending_cpu_count=%d protected_rel_count=%d specs=%d siblings=%d",
		time.Since(convergeStart), err, res.Attempted, res.Applied, res.Skipped, res.Failed, res.Deferred,
		res.Converged, res.FinalSnapshotCurrent, res.State, len(expectedRes.ExpectedByRel), len(protections),
		protectedPending.Size(), len(protectedByRel), len(specs), len(siblings))
	if err != nil {
		emitBulkheadTopologySummary(in.Emitter, "normal", res, err)
		emitBulkheadPruneResult(in.Emitter, "skipped", "dag_error")
		return fmt.Errorf("apply bulkhead topology dag: %w", err)
	}
	if (res.Converged || res.ParentSafe) && res.FinalSnapshotCurrent {
		if finalAppliedView == nil {
			return fmt.Errorf("final convergence result is missing final-snapshot AppliedView")
		}
		if in.ReportTopologyResult != nil {
			in.ReportTopologyResult(topologyResultFromFinalConvergence(res, finalAppliedView))
		}
	}
	emitBulkheadTopologySummary(in.Emitter, "normal", res, nil)
	if !res.Converged && !res.ParentSafe {
		general.InfofV(4, "cpuset_topology: apply not fully converged, deferred=%d state=%s report=%+v", res.Deferred, res.State, res.ConvergenceReport)
		reason := "not_converged"
		if res.Deferred > 0 {
			reason = "deferred_convergence"
		}
		emitBulkheadPruneResult(in.Emitter, "skipped", reason)
		return &topologyApplyNonConvergedError{result: res}
	}
	if res.ParentSafe {
		if err := handleDeferredLeafRetry(in.Mode, in.ScheduleFullRetry); err != nil {
			return err
		}
	}

	activeRels := bulkheadutils.CollectActiveRels(p.cfg, desiredCPUSetPartitionView(in.DesiredView), in.MetaServer, siblings, relExists)
	p.cgroup.Prune(activeRels)
	emitBulkheadPruneResult(in.Emitter, "success", "")
	emitBulkheadPruneActiveRels(in.Emitter, len(activeRels), "success", "")
	return nil
}

func (p *CPUSetTopologyPlugin) ensureBulkheadReclaimSiblingDirs(ctx context.Context, siblings []string) error {
	for _, rel := range siblings {
		rel = strings.Trim(rel, "/")
		if rel == "" {
			continue
		}
		if err := p.cgroup.EnsureDir(ctx, rel); err != nil {
			return fmt.Errorf("ensure reclaim sibling rel path %q: %w", rel, err)
		}
	}
	return nil
}

func handleDeferredLeafRetry(
	mode cpusetutil.CPUSetAdjustmentMode,
	schedule func(cpusetutil.CPUSetAdjustmentRetryReason),
) error {
	if mode == cpusetutil.CPUSetAdjustmentModeRetry {
		return fmt.Errorf("deferred cpuset leaf is still pending")
	}
	if schedule != nil {
		schedule(cpusetutil.RetryReasonDeferredLeaf)
	}
	return nil
}

func admissionStageDeadlineError(ctx context.Context, stage string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", stage, err)
	}
	return nil
}

func appliedViewFromFinalSnapshotWithContext(
	ctx context.Context,
	metaServer *metaserver.MetaServer,
	desired *model.DesiredView,
	dag *topology.TopoDAG,
	snapshot *topology.CompleteSnapshot,
	lifecycleProofs containerLifecycleProofSet,
	deferredCleanupRels map[string]struct{},
) (*model.AppliedView, error) {
	return appliedViewFromFinalSnapshotWithDeferredCleanup(
		ctx, metaServer, desired, dag, snapshot, deferredCleanupRels, lifecycleProofs)
}

func appliedViewFromFinalSnapshotWithDeferredCleanup(
	ctx context.Context,
	metaServer *metaserver.MetaServer,
	desired *model.DesiredView,
	dag *topology.TopoDAG,
	snapshot *topology.CompleteSnapshot,
	deferredCleanupRels map[string]struct{},
	lifecycleProofs containerLifecycleProofSet,
) (*model.AppliedView, error) {
	if desired == nil || dag == nil || snapshot == nil {
		return nil, fmt.Errorf("desired view, topology dag and final snapshot are required")
	}
	partition := model.NewCPUSetPartitionView()
	partition.Dedicated = desired.Dedicated.Clone()
	for poolName, cpus := range desired.SharePoolMap {
		partition.SharePoolMap[poolName] = cpus.Clone()
	}
	applied := &model.AppliedView{
		CPUSetPartitionView: partition,
		Level:               model.AppliedViewLevelFull,
		CPUSetByRel:         make(map[string]machine.CPUSet, len(dag.Nodes())),
		RelProofByRel:       make(map[string]model.CgroupRelProof, len(dag.Nodes())),
		PoolProjection:      model.NewAppliedPoolProjection(),
	}
	applied.ReclaimEffectivePerNUMA = map[int]machine.CPUSet{}
	for _, node := range dag.Nodes() {
		proof, ok := snapshot.TargetProofCPUs(node.Rel, node.CPUs)
		if !ok {
			return nil, fmt.Errorf("final snapshot misses controlled rel %q", node.Rel)
		}
		applied.CPUSetByRel[node.Rel] = proof.Clone()
		entry := snapshot.Entries[node.Rel]
		applied.RelProofByRel[node.Rel] = model.CgroupRelProof{
			Device: entry.Identity.Device,
			Inode:  entry.Identity.Inode,
			CPUSet: proof.Clone(),
		}
		switch node.Domain {
		case topology.DomainPrimary:
			applied.NonReclaimPool = applied.NonReclaimPool.Union(proof)
		case topology.DomainReclaim:
			applied.ReclaimEffective = applied.ReclaimEffective.Union(proof)
		}
		if node.Role != topology.TopoNodeRoleReclaimNUMABucket {
			continue
		}
		if !node.Constraint.CPUUpperBound.IsEmpty() && !proof.IsSubsetOf(node.Constraint.CPUUpperBound) {
			return nil, fmt.Errorf(
				"reclaim NUMA bucket %q target proof %s exceeds CPU upper bound %s",
				node.Rel, proof.String(), node.Constraint.CPUUpperBound.String(),
			)
		}
		numaID, err := strconv.Atoi(node.Metadata["numa"])
		if err != nil {
			return nil, fmt.Errorf("reclaim NUMA bucket %q has invalid numa metadata %q", node.Rel, node.Metadata["numa"])
		}
		applied.ReclaimEffectivePerNUMA[numaID] = applied.ReclaimEffectivePerNUMA[numaID].Union(proof)
	}
	validatedProofs, err := validateContainerLifecycleProofs(ctx, metaServer, lifecycleProofs)
	if err != nil {
		return nil, err
	}
	containerCPUSetByPod, err := containerCPUSetByPodFromFinalSnapshot(
		snapshot, validatedProofs, deferredCleanupRels)
	if err != nil {
		return nil, err
	}
	applied.ContainerCPUSetByPod = containerCPUSetByPod
	applied.PoolProjection = buildAppliedPoolProjection(model.AppliedViewLevelFull, desired, applied)
	return applied, nil
}

func (p *CPUSetTopologyPlugin) CPUSetAdjustmentDisabledHandler(ctx context.Context, in bulkheadapi.HandlerContext) error {
	if p.disabledOnCgroupV2(ctx) {
		return nil
	}
	p.pendingProtections = map[string]pendingPodProtection{}
	budget := topology.NewAdjustmentBudget(
		ctx, topologyBudgetFromConfig(p.cfg.TopologyConvergenceBudget))
	return runCPUSetTopologyAdjustment(ctx, in, budget, p.resetCPUSetTopology)
}

func (p *CPUSetTopologyPlugin) ReconcileDisabled(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
) (result bulkheadapi.DAGApplyResult, terminalErr error) {
	if !p.ShouldReconcileWhenDisabled(ctx, in) || in.DesiredView == nil {
		return bulkheadapi.DAGApplyResult{}, nil
	}
	budget := topology.NewAdjustmentBudget(
		ctx, topologyBudgetFromConfig(p.cfg.TopologyConvergenceBudget))
	var finalAttempt bulkheadapi.DAGApplyResult
	cumulative, err := runCPUSetTopologyAdjustmentWithResult(
		ctx,
		in,
		budget,
		func(
			ctx context.Context,
			attemptInput bulkheadapi.HandlerContext,
			adjustmentBudget *topology.AdjustmentBudget,
		) (topology.ConvergenceResult, error) {
			attemptResult, convergence, attemptErr :=
				p.reconcileDisabledOnce(ctx, attemptInput, adjustmentBudget)
			finalAttempt = attemptResult
			return convergence, attemptErr
		},
	)
	defer func() {
		emitBulkheadTopologySummary(in.Emitter, "reclaim_only", cumulative, terminalErr)
	}()
	result = dagApplyResultFromConvergence(cumulative)
	if err == nil {
		result.AppliedView = finalAttempt.AppliedView.DeepCopy()
	}
	return result, err
}

func disabledReconcileShouldRetry(err error, convergence topology.ConvergenceResult) bool {
	return err != nil && convergence.ReplanDisposition.AllowsReplan()
}

func accumulateConvergenceResult(total *topology.ConvergenceResult, attempt topology.ConvergenceResult) {
	if total == nil {
		return
	}
	total.Attempted += attempt.Attempted
	total.Applied += attempt.Applied
	total.Skipped += attempt.Skipped
	total.Failed += attempt.Failed
	total.Deferred += attempt.Deferred
	total.Journal = append(total.Journal, attempt.Journal...)
	total.Rounds = append(total.Rounds, attempt.Rounds...)
	total.Converged = attempt.Converged
	total.ParentSafe = attempt.ParentSafe
	total.State = attempt.State
	total.ConvergenceReport = cloneConvergenceReport(attempt.ConvergenceReport)
	total.FinalSnapshot = topology.CloneCompleteSnapshot(attempt.FinalSnapshot)
	total.FinalSnapshotCurrent = attempt.FinalSnapshotCurrent
	total.DeferredLeafCount = attempt.DeferredLeafCount
	total.DeferredCPUCount = attempt.DeferredCPUCount
	total.ReplanDisposition = attempt.ReplanDisposition
	total.Published = attempt.Published
}

func cloneConvergenceReport(in topology.ConvergenceReport) topology.ConvergenceReport {
	out := in
	if in.NonConvergedTargets != nil {
		out.NonConvergedTargets = make([]topology.RelConvergence, len(in.NonConvergedTargets))
		for i, target := range in.NonConvergedTargets {
			target.Observed = cloneCPUSet(target.Observed)
			target.Target = cloneCPUSet(target.Target)
			out.NonConvergedTargets[i] = target
		}
	}
	out.PendingToPrimary = cloneCPUSet(in.PendingToPrimary)
	out.PendingToReclaim = cloneCPUSet(in.PendingToReclaim)
	out.CleanupPendingPrimary = cloneCPUSet(in.CleanupPendingPrimary)
	out.CleanupPendingReclaim = cloneCPUSet(in.CleanupPendingReclaim)
	return out
}

func cloneCPUSet(in machine.CPUSet) machine.CPUSet {
	if !in.Initialed {
		return machine.CPUSet{}
	}
	return in.Clone()
}

func (p *CPUSetTopologyPlugin) reconcileDisabledOnce(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
	adjustmentBudget *topology.AdjustmentBudget,
) (bulkheadapi.DAGApplyResult, topology.ConvergenceResult, error) {
	configured := p.configuredReclaimRels(in.DesiredView, in.Topology)
	observed, err := topology.ObserveConfiguredRels(ctx, p.cgroup, configured)
	if err != nil {
		return bulkheadapi.DAGApplyResult{}, topology.ConvergenceResult{}, fmt.Errorf("classify reclaim-only rels: %w", err)
	}
	specs, activeRoots, err := p.buildReclaimOnlySpecs(in, observed)
	if err != nil {
		return bulkheadapi.DAGApplyResult{}, topology.ConvergenceResult{}, err
	}
	if len(specs) == 0 {
		current, err := topology.ObserveConfiguredRels(ctx, p.cgroup, configured)
		if err != nil {
			return bulkheadapi.DAGApplyResult{}, topology.ConvergenceResult{}, fmt.Errorf("recheck absent reclaim-only rels: %w", err)
		}
		if !equalRelObservations(observed, current) {
			return bulkheadapi.DAGApplyResult{}, topology.ConvergenceResult{
				ReplanDisposition: topology.ReplanSafeNoPhysicalWrites,
			}, errReclaimClassificationChanged
		}
		convergence := topology.ConvergenceResult{
			Converged:            true,
			State:                topology.ConvergenceStateConverged,
			FinalSnapshotCurrent: true,
		}
		return bulkheadapi.DAGApplyResult{
			FullyConverged:       true,
			FinalSnapshotCurrent: true,
			AppliedView:          reclaimOnlyAppliedView(in.DesiredView, nil, nil),
		}, convergence, nil
	}

	dag, err := topology.BuildDAG(specs)
	if err != nil {
		return bulkheadapi.DAGApplyResult{}, topology.ConvergenceResult{}, fmt.Errorf("build reclaim-only topology dag: %w", err)
	}
	expectedRes, err := p.buildExpectedCPUSetByRel(ctx, in)
	if err != nil {
		return bulkheadapi.DAGApplyResult{}, topology.ConvergenceResult{}, fmt.Errorf("build reclaim-only expected container cpuset: %w", err)
	}
	expected := filterCPUSetByRoots(expectedRes.ExpectedByRel, activeRoots)
	deferred := filterCPUSetByRoots(expectedRes.DeferredLeafByRel, activeRoots)
	protected := filterCPUSetByRoots(p.pendingProtectedCPUSetByRel(ctx, expectedRes.PendingByPod), activeRoots)
	absentBoundaries := make(map[string]struct{})
	requiredIdentities := make(map[string]topology.CgroupIdentity)
	activeRels := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		activeRels[spec.Rel] = struct{}{}
	}
	for rel, observation := range observed {
		if !observation.Exists {
			absentBoundaries[rel] = struct{}{}
			continue
		}
		if _, ok := activeRels[rel]; ok {
			requiredIdentities[rel] = observation.Identity
		}
	}

	var cpuDetails machine.CPUDetails
	if in.Topology != nil {
		cpuDetails = in.Topology.CPUDetails
	}
	var finalAppliedView *model.AppliedView
	res, err := (topology.TopologyCoordinator{}).Converge(ctx, topology.CoordinatorInput{
		DAG:                   dag,
		Cgroup:                p.cgroup,
		Mode:                  topology.NormalModeGuardWithGate(p.sharedModeGate()),
		Budget:                adjustmentBudget.RemainingConvergenceBudget(),
		AdjustmentBudget:      adjustmentBudget,
		InitialSnapshot:       adjustmentBudget.InitialSnapshot(),
		DrainSelection:        topologyDrainSelectionFromConfig(p.cfg.TopologyDrainSelection),
		CPUDetails:            cpuDetails,
		ReservedCPUSet:        in.DesiredView.Reserve,
		ExpectedCPUSetByRel:   expected,
		DeferredCPUSetByRel:   deferred,
		ProtectedCPUSetByRel:  protected,
		TraversalBoundaries:   absentBoundaries,
		RequiredIdentityByRel: requiredIdentities,
		ExpectedAbsentRels:    absentBoundaries,
		Objective:             topology.ConvergenceObjectiveFull,
		PublishFinalSnapshot: func(snapshot *topology.CompleteSnapshot) error {
			current, err := topology.ObserveConfiguredRels(ctx, p.cgroup, configured)
			if err != nil {
				return err
			}
			if !equalRelObservations(observed, current) {
				return &topology.PlanStaleError{
					Rel:       "reclaim",
					Direction: topology.WritePublish,
					Resource:  "reclaim_classification",
					Current:   "changed",
					Target:    "preflight_observation",
					Err:       errReclaimClassificationChanged,
				}
			}
			for rel, required := range requiredIdentities {
				entry, ok := snapshot.Entries[rel]
				if !ok || entry.Identity != required {
					return &topology.PlanStaleError{
						Rel:       rel,
						Direction: topology.WritePublish,
						Resource:  "reclaim_identity",
						Current:   fmt.Sprintf("%v", entry.Identity),
						Target:    fmt.Sprintf("%v", required),
						Err:       errReclaimClassificationChanged,
					}
				}
			}
			finalAppliedView = reclaimOnlyAppliedView(in.DesiredView, dag, snapshot)
			if finalAppliedView == nil {
				return fmt.Errorf("derive reclaim-only applied view")
			}
			return nil
		},
	})
	if err != nil {
		return dagApplyResultFromConvergence(res), res, fmt.Errorf("apply reclaim-only topology dag: %w", err)
	}
	result := dagApplyResultFromConvergence(res)
	result.AppliedView = finalAppliedView.DeepCopy()
	if result.FullyConverged && result.FinalSnapshotCurrent && result.AppliedView == nil {
		return bulkheadapi.DAGApplyResult{}, res, fmt.Errorf("reclaim-only convergence is missing final-snapshot applied view")
	}
	return result, res, nil
}

func (p *CPUSetTopologyPlugin) configuredReclaimRels(
	desired *model.DesiredView,
	cpuTopology *machine.CPUTopology,
) []string {
	rels := make([]string, 0, len(p.cfg.BulkheadReclaimRelPaths))
	numaIDs := map[int]struct{}{}
	if desired != nil {
		for numaID := range desired.ReclaimEffectivePerNUMA {
			numaIDs[numaID] = struct{}{}
		}
	}
	if cpuTopology != nil {
		for _, numaID := range cpuTopology.CPUDetails.NUMANodes().ToSliceInt() {
			numaIDs[numaID] = struct{}{}
		}
	}
	sortedNUMAIDs := make([]int, 0, len(numaIDs))
	for numaID := range numaIDs {
		sortedNUMAIDs = append(sortedNUMAIDs, numaID)
	}
	sort.Ints(sortedNUMAIDs)
	for reclaimIndex, root := range p.cfg.BulkheadReclaimRelPaths {
		root = strings.Trim(root, "/")
		if root == "" {
			continue
		}
		rels = append(rels, root)
		for _, numaID := range sortedNUMAIDs {
			if rel := p.cfg.ReclaimPerNUMA(reclaimIndex, numaID); rel != "" {
				rels = append(rels, strings.Trim(rel, "/"))
			}
		}
	}
	sort.Strings(rels)
	return rels
}

func (p *CPUSetTopologyPlugin) buildReclaimOnlySpecs(
	in bulkheadapi.HandlerContext,
	observed map[string]topology.RelObservation,
) ([]topology.NodeSpec, []string, error) {
	var cpuDetails machine.CPUDetails
	if in.Topology != nil {
		cpuDetails = in.Topology.CPUDetails
	}
	all, err := bulkheadutils.BuildTopologyNodeSpecsFromView(
		p.cfg, desiredCPUSetPartitionView(in.DesiredView), cpuDetails, nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build reclaim-only topology inputs: %w", err)
	}
	activeRootSet := make(map[string]struct{}, len(p.cfg.BulkheadReclaimRelPaths))
	rootParent := make(map[string]string, len(p.cfg.BulkheadReclaimRelPaths))
	for _, spec := range all {
		if spec.Role != topology.TopoNodeRoleReclaim {
			continue
		}
		rel := strings.Trim(spec.Rel, "/")
		if observed[rel].Exists {
			activeRootSet[rel] = struct{}{}
			rootParent[rel] = strings.Trim(spec.ParentRel, "/")
		} else if rel != "" {
			general.InfofV(4, "cpuset_topology: reclaim-only rel path does not exist, skipping, rel=%q", rel)
		}
	}
	for changed := true; changed; {
		changed = false
		for root := range activeRootSet {
			parent := rootParent[root]
			if parent == "" {
				continue
			}
			if _, ok := activeRootSet[parent]; ok {
				continue
			}
			delete(activeRootSet, root)
			changed = true
		}
	}
	activeRoots := make([]string, 0, len(activeRootSet))
	for root := range activeRootSet {
		activeRoots = append(activeRoots, root)
	}
	sort.Strings(activeRoots)
	specs := make([]topology.NodeSpec, 0, len(all))
	for _, spec := range all {
		rel := strings.Trim(spec.Rel, "/")
		switch spec.Role {
		case topology.TopoNodeRoleReclaim:
			if _, ok := activeRootSet[rel]; !ok {
				continue
			}
		case topology.TopoNodeRoleReclaimNUMABucket:
			if !observed[rel].Exists || !withinAnyRelRoot(rel, activeRoots) {
				if rel != "" && !observed[rel].Exists {
					general.InfofV(4, "cpuset_topology: reclaim-only rel path does not exist, skipping, rel=%q", rel)
				}
				continue
			}
			if spec.ParentRel != "" {
				if _, ok := activeRootSet[spec.ParentRel]; !ok {
					continue
				}
			}
		default:
			continue
		}
		spec.Rel = rel
		specs = append(specs, spec)
	}
	return specs, activeRoots, nil
}

func reclaimOnlyAppliedView(
	desired *model.DesiredView,
	dag *topology.TopoDAG,
	snapshot *topology.CompleteSnapshot,
) *model.AppliedView {
	if desired == nil {
		return nil
	}
	partition := *desired.CPUSetPartitionView.DeepCopy()
	partition.TransientProtectedNonReclaim = machine.NewCPUSet()
	partition.TransientProtectedNonReclaimPerNUMA = map[int]machine.CPUSet{}
	partition.NonReclaimPool = machine.NewCPUSet()
	partition.ReclaimEffective = machine.NewCPUSet()
	partition.ReclaimEffectivePerNUMA = map[int]machine.CPUSet{}
	partition.ContainerCPUSetByPod = map[string]map[string]machine.CPUSet{}
	applied := &model.AppliedView{
		CPUSetPartitionView: partition,
		Level:               model.AppliedViewLevelReclaimOnly,
		CPUSetByRel:         map[string]machine.CPUSet{},
		RelProofByRel:       map[string]model.CgroupRelProof{},
		PoolProjection:      model.NewAppliedPoolProjection(),
	}
	if dag == nil || snapshot == nil {
		applied.PoolProjection = buildAppliedPoolProjection(model.AppliedViewLevelReclaimOnly, desired, applied)
		return applied
	}
	for _, node := range dag.Nodes() {
		proof, ok := snapshot.TargetProofCPUs(node.Rel, node.CPUs)
		if !ok {
			return nil
		}
		entry := snapshot.Entries[node.Rel]
		applied.CPUSetByRel[node.Rel] = proof.Clone()
		applied.RelProofByRel[node.Rel] = model.CgroupRelProof{
			Device: entry.Identity.Device,
			Inode:  entry.Identity.Inode,
			CPUSet: proof.Clone(),
		}
		if node.Role == topology.TopoNodeRoleReclaim {
			applied.ReclaimEffective = applied.ReclaimEffective.Union(proof)
		}
		if node.Role == topology.TopoNodeRoleReclaimNUMABucket {
			numaID, err := strconv.Atoi(node.Metadata["numa"])
			if err != nil {
				return nil
			}
			applied.ReclaimEffectivePerNUMA[numaID] =
				applied.ReclaimEffectivePerNUMA[numaID].Union(proof)
		}
	}
	applied.PoolProjection = buildAppliedPoolProjection(model.AppliedViewLevelReclaimOnly, desired, applied)
	return applied
}

func filterCPUSetByRoots(in map[string]machine.CPUSet, roots []string) map[string]machine.CPUSet {
	out := make(map[string]machine.CPUSet)
	for rel, cpus := range in {
		if withinAnyRelRoot(rel, roots) {
			out[rel] = cpus.Clone()
		}
	}
	return out
}

func withinAnyRelRoot(rel string, roots []string) bool {
	rel = strings.Trim(rel, "/")
	for _, root := range roots {
		root = strings.Trim(root, "/")
		if rel == root || strings.HasPrefix(rel, root+"/") {
			return true
		}
	}
	return false
}

func equalRelObservations(a, b map[string]topology.RelObservation) bool {
	if len(a) != len(b) {
		return false
	}
	for rel, left := range a {
		if right, ok := b[rel]; !ok || left != right {
			return false
		}
	}
	return true
}

func (p *CPUSetTopologyPlugin) disabledResetCPUSet(ctx context.Context, in bulkheadapi.HandlerContext) (machine.CPUSet, error) {
	if p.cgroup.Version(ctx) == cgroupclient.CgroupVersionV2 {
		return machine.NewCPUSet(), nil
	}
	if in.Topology == nil {
		return machine.CPUSet{}, fmt.Errorf("nil topology for v1 disabled cpuset reset")
	}
	target := in.Topology.CPUDetails.CPUs()
	if target.IsEmpty() {
		return machine.CPUSet{}, fmt.Errorf("empty machine cpuset for v1 disabled cpuset reset")
	}
	return target, nil
}

func (p *CPUSetTopologyPlugin) buildDisabledResetDAG(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
	target machine.CPUSet,
) (*topology.TopoDAG, error) {
	relExists := func(rel string) error {
		_, err := p.cgroup.StatDir(ctx, rel)
		return err
	}

	discoveredSiblings, err := p.discoverBulkheadReclaimSiblings(ctx, in.DesiredView)
	if err != nil {
		return nil, fmt.Errorf("discover bulkhead reclaim siblings: %w", err)
	}
	siblings := p.mergeBulkheadReclaimSiblings(
		discoveredSiblings,
		p.configuredBulkheadReclaimSiblings(),
		desiredCPUSetPartitionView(in.DesiredView),
	)
	if err := p.ensureBulkheadReclaimSiblingDirs(ctx, siblings); err != nil {
		return nil, fmt.Errorf("ensure bulkhead reclaim sibling cgroups: %w", err)
	}

	var cpuDetails machine.CPUDetails
	if in.Topology != nil {
		cpuDetails = in.Topology.CPUDetails
	}
	specs, err := bulkheadutils.BuildTopologyNodeSpecsFromView(p.cfg, desiredCPUSetPartitionView(in.DesiredView), cpuDetails, siblings, relExists)
	if err != nil {
		return nil, fmt.Errorf("build disabled reset topology inputs: %w", err)
	}
	if p.ShouldReconcileWhenDisabled(ctx, in) {
		reclaimRoots := normalizedReclaimRoots(p.cfg.BulkheadReclaimRelPaths)
		filtered := specs[:0]
		for _, spec := range specs {
			if withinAnyRelRoot(spec.Rel, reclaimRoots) {
				continue
			}
			filtered = append(filtered, spec)
		}
		specs = filtered
	}
	specs, err = p.filterExistingDisabledResetSpecs(ctx, specs)
	if err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, nil
	}
	for i := range specs {
		specs[i].CPUs = target
		specs[i].Mems = ""
	}

	dag, err := topology.BuildDAG(specs)
	if err != nil {
		return nil, fmt.Errorf("build disabled reset topology dag: %w", err)
	}
	return dag, nil
}

func (p *CPUSetTopologyPlugin) filterExistingDisabledResetSpecs(ctx context.Context, specs []topology.NodeSpec) ([]topology.NodeSpec, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	out := specs[:0]
	for _, spec := range specs {
		rel := strings.Trim(spec.Rel, "/")
		if rel == "" {
			continue
		}
		if _, err := p.cgroup.StatDir(ctx, rel); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				general.InfofV(4, "cpuset_topology: disabled reset rel path does not exist, skipping, rel=%q err=%v", rel, err)
				continue
			}
			return nil, fmt.Errorf("stat disabled reset rel path %q: %w", rel, err)
		}
		spec.Rel = rel
		out = append(out, spec)
	}
	return out, nil
}

func (p *CPUSetTopologyPlugin) resetCPUSetTopology(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
	adjustmentBudget *topology.AdjustmentBudget,
) (topology.ConvergenceResult, error) {
	target, err := p.disabledResetCPUSet(ctx, in)
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", "reset_target_error")
		return topology.ConvergenceResult{}, err
	}

	// Reset (disabled transition) applies reset targets back towards the
	// machine/root cpuset. Pending allocations whose leaves cannot be resolved
	// are not protected here: protection addresses ancestor shrink during normal
	// adjustment in the admission creation window, while reset cannot directly
	// protect a leaf that does not exist. Any classification error is intentionally
	// ignored so reset can relax a stale transient-pool cpuset instead of being
	// blocked by a transient resolve failure.
	expectedRes, _ := p.buildExpectedCPUSetByRel(ctx, in)
	var expected map[string]machine.CPUSet
	if expectedRes != nil {
		expected = expectedRes.ExpectedByRel
	}

	dag, err := p.buildDisabledResetDAG(ctx, in, target)
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", "dag_error")
		return topology.ConvergenceResult{}, err
	}
	if dag == nil {
		emitBulkheadPruneResult(in.Emitter, "success", "")
		return topology.ConvergenceResult{
			Converged:            true,
			State:                topology.ConvergenceStateConverged,
			FinalSnapshotCurrent: true,
		}, nil
	}

	var traversalBoundaries map[string]struct{}
	if p.ShouldReconcileWhenDisabled(ctx, in) {
		traversalBoundaries = reclaimResetBoundaries(p.cfg, in.DesiredView)
	}
	res, err := (topology.TopologyCoordinator{}).Converge(ctx, topology.CoordinatorInput{
		DAG:                 dag,
		Cgroup:              p.cgroup,
		Mode:                topology.ResetModeGuardWithGate(p.sharedModeGate()),
		ExpectedCPUSetByRel: expected,
		TraversalBoundaries: traversalBoundaries,
		Budget:              adjustmentBudget.RemainingConvergenceBudget(),
		AdjustmentBudget:    adjustmentBudget,
		InitialSnapshot:     adjustmentBudget.InitialSnapshot(),
		DrainSelection:      topologyDrainSelectionFromConfig(p.cfg.TopologyDrainSelection),
	})
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", "dag_error")
		emitBulkheadTopologySummary(in.Emitter, "reset", res, err)
		return res, fmt.Errorf("apply disabled reset topology dag: %w", err)
	}
	emitBulkheadTopologySummary(in.Emitter, "reset", res, nil)
	if !res.Converged {
		general.InfofV(4, "cpuset_topology: disabled reset not fully converged, report=%+v", res.ConvergenceReport)
		emitBulkheadPruneResult(in.Emitter, "skipped", "reset_not_converged")
		return res, &disabledResetNotConvergedError{
			state:   res.State,
			applied: res.Applied,
			report:  res.ConvergenceReport,
		}
	}

	emitBulkheadPruneResult(in.Emitter, "success", "")
	return res, nil
}

func normalizedReclaimRoots(rels []string) []string {
	out := make([]string, 0, len(rels))
	for _, rel := range rels {
		if rel = strings.Trim(rel, "/"); rel != "" {
			out = append(out, rel)
		}
	}
	return out
}

func reclaimResetBoundaries(
	cfg bulkheadconfig.BulkheadConfiguration,
	desired *model.DesiredView,
) map[string]struct{} {
	if !cfg.PreserveReclaimCPUSetWhenTopologyDisabled {
		return nil
	}
	out := make(map[string]struct{})
	for reclaimIndex, root := range cfg.BulkheadReclaimRelPaths {
		if root = strings.Trim(root, "/"); root == "" {
			continue
		}
		out[root] = struct{}{}
		if desired == nil {
			continue
		}
		for numaID := range desired.ReclaimEffectivePerNUMA {
			if rel := cfg.ReclaimPerNUMA(reclaimIndex, numaID); rel != "" {
				out[strings.Trim(rel, "/")] = struct{}{}
			}
		}
	}
	return out
}

func (p *CPUSetTopologyPlugin) sharedModeGate() *topology.ModeGate {
	p.modeGateMu.Lock()
	defer p.modeGateMu.Unlock()
	if p.modeGate == nil {
		p.modeGate = topology.NewModeGate()
	}
	return p.modeGate
}

func (p *CPUSetTopologyPlugin) PeriodicalHandler(
	ctx context.Context,
	in bulkheadapi.PeriodicalHandlerContext,
) error {
	if p.disabledOnCgroupV2(ctx) {
		return nil
	}
	enabled := enableBulkheadCpusetTopologyByDynamicConf(in.DynamicConf)
	if in.EffectiveEnabled != nil {
		enabled = *in.EffectiveEnabled
	}
	if enabled && in.EffectiveEnabled != nil && in.AppliedView == nil {
		return nil
	}
	if p.cgroup.Version(ctx) == cgroupclient.CgroupVersionV1 {
		schedLoadBalance := !enabled
		if err := p.cgroup.ApplySchedLoadBalance(ctx, "", schedLoadBalance); err != nil {
			return fmt.Errorf("apply root cpuset.sched_load_balance=%t: %w", schedLoadBalance, err)
		}
		return nil
	}

	flag := cgcommon.CPUSetPartitionFlagMember
	if enabled {
		flag = cgcommon.CPUSetPartitionFlagRoot
	}
	return p.applyBulkheadPartitionFlag(ctx, flag)
}

func (p *CPUSetTopologyPlugin) applyBulkheadPartitionFlag(ctx context.Context, flag cgcommon.CPUSetPartitionFlag) error {
	var errs []error
	for _, rel := range p.cfg.BulkheadPartitionRelPaths {
		rel = strings.Trim(rel, "/")
		if rel == "" {
			continue
		}
		if _, err := p.cgroup.StatDir(ctx, rel); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				errs = append(errs, fmt.Errorf("stat partition rel path %q: %w", rel, err))
				continue
			}
			general.InfofV(4, "bulkhead: partition rel path does not exist, skipping, rel=%q err=%v", rel, err)
			continue
		}
		if err := p.cgroup.ApplyCPUSetPartition(ctx, rel, flag); err != nil {
			if errors.Is(err, cgcommon.ErrNotSupported) {
				general.InfofV(4, "bulkhead: cpuset partition not supported, skipping, rel=%q", rel)
				continue
			}
			errs = append(errs, fmt.Errorf("apply cpuset.cpus.partition=%s @ %s: %w", flag, rel, err))
			continue
		}
	}
	return apierrors.NewAggregate(errs)
}

// pendingContainerCPUSet records a container whose allocation already exists in
// QRM state but whose cgroup rel cannot be resolved yet (typically the pod
// admit window before kubelet/containerd creates the container). With no leaf
// available to write, its cpuset protects the controlled ancestors' effective
// targets from shrinking below the allocation; the absent leaf is not written.
type pendingContainerCPUSet struct {
	PodUID         string
	ContainerName  string
	ContainerID    string
	CPUs           machine.CPUSet
	Reason         string
	Cause          error
	NativeQOSClass v1.PodQOSClass
	ScopeRel       string
}

type resolvedContainerCPUSet struct {
	PodUID        string
	ContainerName string
	ContainerID   string
	Rel           string
	CPUs          machine.CPUSet
}

type podContainerCPUSetOutcomes struct {
	resolved []resolvedContainerCPUSet
	pending  []pendingContainerCPUSet
}

type podLifecycleClassification struct {
	Resolved []resolvedContainerCPUSet
	Pending  []pendingContainerCPUSet
	Retired  []containerLifecycleProof
	ScopeRel string
	QOSClass v1.PodQOSClass
	Stale    bool
}

// expectedCPUSetBuildResult separates resolvable container leaves (ExpectedByRel,
// written precisely) from admit-pending containers (PendingByPod, protected but
// not written).
type expectedCPUSetBuildResult struct {
	ExpectedByRel     map[string]machine.CPUSet
	DeferredLeafByRel map[string]machine.CPUSet
	PendingByPod      []pendingContainerCPUSet
	LifecycleProofs   containerLifecycleProofSet
}

type expectedCPUSetOwnerBinder struct {
	ownerByRel map[string]containerLifecycleProofKey
}

func (b *expectedCPUSetOwnerBinder) bind(
	target map[string]machine.CPUSet,
	container resolvedContainerCPUSet,
) error {
	owner := lifecycleProofLogicalKey(container.PodUID, container.ContainerName)
	if previous, exists := b.ownerByRel[container.Rel]; exists && previous != owner {
		return fmt.Errorf(
			"relative path %q is owned by both pod=%q container=%q and pod=%q container=%q",
			container.Rel, previous.podUID, previous.containerName, owner.podUID, owner.containerName)
	}
	b.ownerByRel[container.Rel] = owner
	target[container.Rel] = container.CPUs
	return nil
}

// PendingCPUSetUnion returns the union of all pending container allocations. The
// writer folds this into the primary effective target so the parent cgroup never
// shrinks below an allocation whose leaf has not been created yet.
func (r *expectedCPUSetBuildResult) PendingCPUSetUnion() machine.CPUSet {
	out := machine.NewCPUSet()
	if r == nil {
		return out
	}
	for _, p := range r.PendingByPod {
		out = out.Union(p.CPUs)
	}
	return out
}

// isContainerAbsentErr reports whether the pod or container has no active
// runtime leaf during the normal admission creation or restart window.
func isContainerAbsentErr(err error) bool {
	return metapod.IsPodNotFound(err) ||
		errors.Is(err, metapod.ErrContainerNotFound) ||
		errors.Is(err, bulkheadutils.ErrContainerNotRunning)
}

// isContainerPendingErr additionally treats a confirmed identity change as an
// admission-safe transition. Cache synchronization, kubelet transport, and
// context errors must fail closed.
func isContainerPendingErr(err error) bool {
	if isContainerAbsentErr(err) || errors.Is(err, bulkheadutils.ErrContainerIdentityChanged) {
		return true
	}
	var resolveErr *bulkheadutils.ContainerRelPathResolveError
	return errors.As(err, &resolveErr) &&
		resolveErr.Stage == bulkheadutils.ContainerRelPathResolveStageCgroupPath &&
		errors.Is(resolveErr.Err, os.ErrNotExist)
}

// buildExpectedCPUSetByRel uses a two-snapshot protocol. It first resolves
// candidate leaves from one cache-only Pod snapshot without triggering kubelet
// I/O, then obtains exactly one strict fresh Pod snapshot for the whole round.
// Every stale, pending, and generation decision is derived from that same fresh
// snapshot; per-container refreshes are forbidden because they could mix Pod
// generations inside one topology plan.
func (p *CPUSetTopologyPlugin) buildExpectedCPUSetByRel(ctx context.Context, in bulkheadapi.HandlerContext) (*expectedCPUSetBuildResult, error) {
	out := &expectedCPUSetBuildResult{
		ExpectedByRel:     map[string]machine.CPUSet{},
		DeferredLeafByRel: map[string]machine.CPUSet{},
	}
	if in.DesiredView == nil || !hasNonEmptyDesiredCPUSet(in.DesiredView.ContainerCPUSetByPod) {
		var err error
		out.LifecycleProofs, err = freezeContainerLifecycleProofs(nil, nil)
		if err != nil {
			return nil, fmt.Errorf("freeze empty container lifecycle proofs: %w", err)
		}
		return out, nil
	}
	if in.MetaServer == nil {
		return nil, fmt.Errorf("build container lifecycle proofs: metaserver is nil")
	}
	var errs []error
	cachedPodsByUID, cacheOnly, err := cachedPodSnapshotByUID(ctx, in.MetaServer)
	if err != nil {
		return nil, fmt.Errorf("get cache-only pod snapshot: %w", err)
	}
	outcomesByPod := make(map[string]*podContainerCPUSetOutcomes)
	for podUID, containers := range in.DesiredView.ContainerCPUSetByPod {
		outcomes := &podContainerCPUSetOutcomes{}
		outcomesByPod[podUID] = outcomes
		for containerName, cpus := range containers {
			if cpus.IsEmpty() {
				continue
			}
			// Reuse ResolveContainerRelPath so that the rel-key format stays in sync
			// with everywhere else in bulkhead (BulkheadPrimaryRelPath,
			// BulkheadReclaimRelPaths, CollectActiveRels, controlledRels, and the
			// childRel constructed by writer.TopologyCoordinatorConverge via filepath.Join(parent, name)).
			// ResolveContainerRelPath does the GetContainerID + GetContainerRelativeCgroupPath
			// lookup and, crucially, trims the leading "/" that
			// GetKubernetesAnyExistRelativeCgroupPath prepends. Without this trim, the
			// expected map key would never match the childRel that expandDescendants
			// produces during recursion, causing per-container cpuset enforcement to
			// silently degrade to inheriting the parent pool target.
			var rel, containerID string
			var err error
			if cacheOnly {
				containerID, _ = podStatusContainerID(cachedPodsByUID[podUID], containerName)
				if containerID == "" {
					err = &bulkheadutils.ContainerRelPathResolveError{
						Stage: bulkheadutils.ContainerRelPathResolveStageContainerID,
						Err:   metapod.ErrContainerNotFound,
					}
				} else {
					rel, err = resolveContainerRelPathFromID(podUID, containerID)
				}
			} else {
				rel, containerID, err = bulkheadutils.ResolveContainerRelPathAndIDCacheOnlyWithContext(
					ctx, in.MetaServer, podUID, containerName)
			}
			if err != nil {
				if isContainerPendingErr(err) {
					outcomes.pending = append(outcomes.pending, pendingContainerCPUSet{
						PodUID: podUID, ContainerName: containerName, ContainerID: containerID,
						CPUs: cpus, Reason: err.Error(), Cause: err,
					})
					continue
				}
				// A real internal error (illegal rel, cgroup/metaserver failure, or a
				// refresh/transport failure): block this round rather than apply a
				// partial/wrong topology.
				errs = append(errs, fmt.Errorf("pod=%s container=%s cpuset=%s: %w",
					podUID, containerName, cpus.String(), err))
				continue
			}
			if rel == "" {
				errs = append(errs, fmt.Errorf("pod=%s container=%s cpuset=%s: empty relative cgroup path",
					podUID, containerName, cpus.String()))
				continue
			}
			outcomes.resolved = append(outcomes.resolved, resolvedContainerCPUSet{
				PodUID: podUID, ContainerName: containerName, ContainerID: containerID, Rel: rel, CPUs: cpus,
			})
		}
	}
	hasOutcomes := false
	for _, outcomes := range outcomesByPod {
		if len(outcomes.resolved) > 0 || len(outcomes.pending) > 0 {
			hasOutcomes = true
			break
		}
	}
	if !hasOutcomes {
		if len(errs) > 0 {
			return nil, apierrors.NewAggregate(errs)
		}
		out.LifecycleProofs, err = freezeContainerLifecycleProofs(nil, nil)
		if err != nil {
			return nil, fmt.Errorf("freeze empty container lifecycle proofs: %w", err)
		}
		return out, nil
	}
	refreshCtx := context.WithValue(ctx, metapod.BypassCacheKey, metapod.BypassCacheTrue)
	refreshCtx = context.WithValue(refreshCtx, metapod.StrictBypassCacheKey, metapod.BypassCacheTrue)
	freshPods, err := in.MetaServer.GetPodList(refreshCtx, nil)
	if err != nil {
		return nil, fmt.Errorf("get strict fresh pod snapshot: %w", err)
	}
	freshPodsByUID := make(map[string]*v1.Pod, len(freshPods))
	for _, pod := range freshPods {
		if pod == nil {
			return nil, fmt.Errorf("strict fresh pod snapshot contains nil pod")
		}
		podUID := string(pod.UID)
		if podUID == "" {
			return nil, fmt.Errorf("strict fresh pod snapshot contains pod with empty UID")
		}
		if _, exists := freshPodsByUID[podUID]; exists {
			return nil, fmt.Errorf("strict fresh pod snapshot contains duplicate UID %q", podUID)
		}
		freshPodsByUID[podUID] = pod
	}
	var lifecycleProofs []containerLifecycleProof
	ownerBinder := expectedCPUSetOwnerBinder{
		ownerByRel: make(map[string]containerLifecycleProofKey),
	}
	for podUID, outcomes := range outcomesByPod {
		if len(outcomes.resolved) == 0 && len(outcomes.pending) == 0 {
			continue
		}
		freshPod, exists := freshPodsByUID[podUID]
		classification, scopeErr := p.filterPodOutcomesAgainstFreshPod(
			ctx, podUID, outcomes, freshPod, exists)
		if scopeErr != nil {
			errs = append(errs, fmt.Errorf("resolve pod outcomes: pod=%s: %w", podUID, scopeErr))
			continue
		}
		if classification.Stale {
			for _, container := range outcomes.resolved {
				general.Infof("bulkhead: stale checkpoint allocation skipped from expected leaves, pod=%q container=%q cpuset=%s",
					podUID, container.ContainerName, container.CPUs.String())
			}
			for _, container := range outcomes.pending {
				general.Infof("bulkhead: stale checkpoint allocation skipped from pending protection, pod=%q container=%q cpuset=%s",
					podUID, container.ContainerName, container.CPUs.String())
			}
		}
		podProofs := lifecycleProofsForDesiredPod(
			podUID, in.DesiredView.ContainerCPUSetByPod[podUID], freshPod, exists, classification)
		lifecycleProofs = append(lifecycleProofs, podProofs...)
		if classification.Stale {
			continue
		}
		for _, container := range classification.Resolved {
			if p.cfg.EnableAdmissionLeafDefer && in.Mode.OrFullDefault() == cpusetutil.CPUSetAdjustmentModeAdmission {
				current, readErr := p.cgroup.ReadCPUSet(ctx, container.Rel)
				if readErr == nil && !current.Equals(container.CPUs) && container.CPUs.IsSubsetOf(current) &&
					current.Intersection(in.DesiredView.DesiredReclaimEffective).IsEmpty() {
					if bindErr := ownerBinder.bind(out.DeferredLeafByRel, container); bindErr != nil {
						errs = append(errs, bindErr)
					}
					continue
				}
			}
			if bindErr := ownerBinder.bind(out.ExpectedByRel, container); bindErr != nil {
				errs = append(errs, bindErr)
			}
		}
		for i := range classification.Pending {
			classification.Pending[i].NativeQOSClass = classification.QOSClass
			classification.Pending[i].ScopeRel = classification.ScopeRel
			general.InfofV(5, "bulkhead: container rel pending, protecting allocation, pod=%q container=%q cpuset=%s cpuset_size=%d reason=%s",
				podUID, classification.Pending[i].ContainerName, classification.Pending[i].CPUs.String(), classification.Pending[i].CPUs.Size(), classification.Pending[i].Reason)
		}
		out.PendingByPod = append(out.PendingByPod, classification.Pending...)
	}
	if coverageErr := validateLifecycleProofCoverage(
		in.DesiredView.ContainerCPUSetByPod, lifecycleProofs); coverageErr != nil {
		errs = append(errs, fmt.Errorf("validate container lifecycle proof coverage: %w", coverageErr))
	}
	if len(errs) > 0 {
		return nil, apierrors.NewAggregate(errs)
	}
	out.LifecycleProofs, err = freezeContainerLifecycleProofs(lifecycleProofs, freshPodsByUID)
	if err != nil {
		return nil, fmt.Errorf("freeze container lifecycle proofs: %w", err)
	}
	return out, nil
}

func hasNonEmptyDesiredCPUSet(desiredByPod map[string]map[string]machine.CPUSet) bool {
	for _, desiredByContainer := range desiredByPod {
		for _, cpus := range desiredByContainer {
			if !cpus.IsEmpty() {
				return true
			}
		}
	}
	return false
}

// cachedPodSnapshotByUID uses the optional cache-only extension when available.
// Legacy fetchers may fall back to per-container cache reads, but every outcome
// is still reconciled against one strict snapshot before it can affect a plan.
// Cache snapshot errors are fatal and never select the legacy path silently.
func cachedPodSnapshotByUID(
	ctx context.Context,
	metaServer *metaserver.MetaServer,
) (map[string]*v1.Pod, bool, error) {
	if metaServer == nil || metaServer.MetaAgent == nil || metaServer.PodFetcher == nil {
		return nil, false, fmt.Errorf("nil pod fetcher")
	}
	fetcher, ok := metaServer.PodFetcher.(metapod.CachedPodSnapshotFetcher)
	if !ok {
		return nil, false, nil
	}
	pods, err := fetcher.GetPodListFromCache(ctx, nil)
	if err != nil {
		return nil, true, err
	}
	byUID := make(map[string]*v1.Pod, len(pods))
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		byUID[string(pod.UID)] = pod
	}
	return byUID, true, nil
}

func resolveContainerRelPathFromID(podUID, containerID string) (string, error) {
	rel, err := cgcommon.GetContainerRelativeCgroupPath(podUID, containerID)
	if err != nil {
		return "", &bulkheadutils.ContainerRelPathResolveError{
			Stage: bulkheadutils.ContainerRelPathResolveStageCgroupPath,
			Err:   err,
		}
	}
	rel = strings.Trim(rel, "/")
	if rel == "" {
		return "", &bulkheadutils.ContainerRelPathResolveError{
			Stage: bulkheadutils.ContainerRelPathResolveStageCgroupPath,
			Err:   fmt.Errorf("empty relative cgroup path"),
		}
	}
	return rel, nil
}

// filterPodOutcomesAgainstFreshPod applies one round-wide strict Pod snapshot
// to all resolved and pending outcomes for one Pod.
func (p *CPUSetTopologyPlugin) filterPodOutcomesAgainstFreshPod(
	ctx context.Context,
	podUID string,
	outcomes *podContainerCPUSetOutcomes,
	pod *v1.Pod,
	podExists bool,
) (podLifecycleClassification, error) {
	var classification podLifecycleClassification
	validResolved, resolvedPending, retiredResolved, err := p.reconcileResolvedContainersWithFreshPod(
		ctx, podUID, outcomes.resolved, pod)
	if err != nil {
		return classification, err
	}
	pendingResolved, validPending, retiredPending, err := p.reconcilePendingContainersWithFreshPod(
		ctx, podUID, outcomes.pending, pod)
	if err != nil {
		return classification, err
	}
	classification.Resolved = append(validResolved, pendingResolved...)
	classification.Pending = append(validPending, resolvedPending...)
	classification.Retired = append(retiredResolved, retiredPending...)
	if podExists {
		classification.QOSClass = v1qos.GetPodQOS(pod)
		if len(classification.Pending) == 0 {
			return classification, nil
		}
		candidates := p.pendingPodScopeCandidatesForQOS(podUID, classification.QOSClass)
		if len(candidates) == 1 {
			classification.ScopeRel = strings.Trim(candidates[0], "/")
			return classification, nil
		}
		// Prefer a uniquely materialized scope and fail closed if multiple
		// candidates exist. No materialized candidate is still a live pending
		// Pod; the topology DAG selects its controlled primary scope later.
		scopeRel, _, err := p.selectConcretePendingPodScope(ctx, podUID, candidates)
		classification.ScopeRel = scopeRel
		return classification, err
	}
	if len(classification.Pending) == 0 {
		classification.Stale = len(classification.Resolved) == 0
		return classification, nil
	}
	scopeRel, stale, err := p.selectConcretePendingPodScope(
		ctx, podUID, relativePendingPodScopeCandidates(
			cgcommon.GetPodRelativeCgroupPathCandidates(podUID)))
	classification.ScopeRel = scopeRel
	classification.Stale = stale && len(classification.Resolved) == 0
	if classification.Stale {
		for _, pending := range classification.Pending {
			classification.Retired = append(classification.Retired,
				lifecycleProofFromPending(pending, containerLifecycleRetired))
		}
		classification.Pending = nil
	}
	return classification, err
}

// reconcileResolvedContainersWithFreshPod treats container identity as
// generation-scoped. A fresh status may name a new generation while the
// previously resolved cgroup still exists and can still hold tasks. The old
// generation remains protected until typed absence proves its physical
// retirement, while the fresh generation is independently resolved or kept
// pending. Any non-absence stat or path error aborts the round.
func (p *CPUSetTopologyPlugin) reconcileResolvedContainersWithFreshPod(
	ctx context.Context,
	podUID string,
	resolved []resolvedContainerCPUSet,
	pod *v1.Pod,
) ([]resolvedContainerCPUSet, []pendingContainerCPUSet, []containerLifecycleProof, error) {
	valid := make([]resolvedContainerCPUSet, 0, len(resolved)*2)
	var pending []pendingContainerCPUSet
	var retired []containerLifecycleProof
	for _, container := range resolved {
		freshName := pod != nil && podSpecHasContainerName(pod, container.ContainerName)
		freshContainerID, hasFreshID := podStatusContainerID(pod, container.ContainerName)
		sameGeneration := freshName && hasFreshID && freshContainerID == container.ContainerID
		if !sameGeneration {
			if p.cgroup == nil {
				return nil, nil, nil, fmt.Errorf("stat previously resolved container rel %q: cgroup client is nil", container.Rel)
			}
			if _, statErr := p.cgroup.StatDir(ctx, container.Rel); statErr == nil {
				valid = append(valid, container)
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return nil, nil, nil, fmt.Errorf("stat previously resolved container rel %q: %w", container.Rel, statErr)
			} else if !freshName {
				retired = append(retired, lifecycleProofFromResolved(container, containerLifecycleRetired))
			}
		}

		if !freshName {
			continue
		}
		if !hasFreshID {
			pending = append(pending, pendingFromResolved(container, "",
				"fresh pod status has no current container identity"))
			continue
		}
		fresh, resolveErr := p.resolveMaterializedContainerFromFreshID(
			ctx, podUID, container, freshContainerID)
		if resolveErr == nil {
			valid = append(valid, fresh)
			continue
		}
		if errors.Is(resolveErr, os.ErrNotExist) {
			pending = append(pending, pendingFromResolved(container, freshContainerID,
				fmt.Sprintf("fresh container identity %q has no cgroup leaf", freshContainerID)))
			continue
		}
		return nil, nil, nil, fmt.Errorf(
			"resolve fresh container generation pod=%q container=%q id=%q: %w",
			podUID, container.ContainerName, freshContainerID, resolveErr)
	}
	return valid, pending, retired, nil
}

// resolveMaterializedContainerFromFreshID does not trust a successful path
// lookup alone: resolver caches may outlive the cgroup leaf they identify.
func (p *CPUSetTopologyPlugin) resolveMaterializedContainerFromFreshID(
	ctx context.Context,
	podUID string,
	previous resolvedContainerCPUSet,
	containerID string,
) (resolvedContainerCPUSet, error) {
	rel, err := resolveContainerRelPathFromID(podUID, containerID)
	if err != nil {
		return resolvedContainerCPUSet{}, err
	}
	if p.cgroup == nil {
		return resolvedContainerCPUSet{}, fmt.Errorf("stat fresh container rel %q: cgroup client is nil", rel)
	}
	if _, err := p.cgroup.StatDir(ctx, rel); err != nil {
		return resolvedContainerCPUSet{}, err
	}
	return resolvedContainerCPUSet{
		PodUID:        podUID,
		ContainerName: previous.ContainerName,
		ContainerID:   containerID,
		Rel:           rel,
		CPUs:          previous.CPUs,
	}, nil
}

func pendingFromResolved(
	container resolvedContainerCPUSet,
	containerID, reason string,
) pendingContainerCPUSet {
	return pendingContainerCPUSet{
		PodUID:        container.PodUID,
		ContainerName: container.ContainerName,
		ContainerID:   containerID,
		CPUs:          container.CPUs,
		Reason:        reason,
		Cause:         os.ErrNotExist,
	}
}

func lifecycleProofFromResolved(
	container resolvedContainerCPUSet,
	state containerLifecycleState,
) containerLifecycleProof {
	return containerLifecycleProof{
		PodUID:        container.PodUID,
		ContainerName: container.ContainerName,
		ContainerID:   container.ContainerID,
		RelativePath:  container.Rel,
		DesiredCPUSet: container.CPUs,
		State:         state,
	}
}

func lifecycleProofFromPending(
	container pendingContainerCPUSet,
	state containerLifecycleState,
) containerLifecycleProof {
	return containerLifecycleProof{
		PodUID:        container.PodUID,
		ContainerName: container.ContainerName,
		ContainerID:   container.ContainerID,
		DesiredCPUSet: container.CPUs,
		State:         state,
	}
}

// lifecycleProofsForDesiredPod freezes exactly one lifecycle decision for every
// non-empty desired entry. The strict fresh Pod snapshot alone determines proof
// state: an absent Pod or owner is retired, an owner without a current runtime
// identity is pending, and an owner with a current runtime identity is resolved.
// Physical classification may enrich the proof with the matching rel or an old
// retired identity, but it must never change that snapshot-owned state.
func lifecycleProofsForDesiredPod(
	podUID string,
	desired map[string]machine.CPUSet,
	freshPod *v1.Pod,
	podExists bool,
	classification podLifecycleClassification,
) []containerLifecycleProof {
	proofs := make([]containerLifecycleProof, 0, len(desired))
	for containerName, cpus := range desired {
		if cpus.IsEmpty() {
			continue
		}
		ownerExists := podExists && podSpecHasContainerName(freshPod, containerName)
		freshID, hasFreshID := podStatusContainerID(freshPod, containerName)
		proof := containerLifecycleProof{
			PodUID:        podUID,
			ContainerName: containerName,
			ContainerID:   freshID,
			DesiredCPUSet: cpus,
			State:         containerLifecycleRetired,
		}
		if ownerExists {
			proof.State = containerLifecyclePending
			if hasFreshID {
				if resolved, ok := findResolvedLifecycleContainer(
					classification.Resolved, containerName, freshID); ok && resolved.Rel != "" {
					proof.State = containerLifecycleResolved
					proof.RelativePath = resolved.Rel
				}
			}
		} else {
			if retired, ok := findRetiredLifecycleContainer(
				classification.Retired, containerName); ok {
				proof.ContainerID = retired.ContainerID
				proof.RelativePath = retired.RelativePath
			} else if resolved, ok := findResolvedLifecycleContainer(
				classification.Resolved, containerName, ""); ok {
				proof.ContainerID = resolved.ContainerID
				proof.RelativePath = resolved.Rel
			}
		}
		proofs = append(proofs, proof)
	}
	return proofs
}

func findResolvedLifecycleContainer(
	containers []resolvedContainerCPUSet,
	name, containerID string,
) (resolvedContainerCPUSet, bool) {
	for _, container := range containers {
		if container.ContainerName == name &&
			(containerID == "" || container.ContainerID == containerID) {
			return container, true
		}
	}
	return resolvedContainerCPUSet{}, false
}

func findRetiredLifecycleContainer(
	proofs []containerLifecycleProof,
	name string,
) (containerLifecycleProof, bool) {
	for _, proof := range proofs {
		if proof.ContainerName == name {
			return proof, true
		}
	}
	return containerLifecycleProof{}, false
}

func podStatusContainerID(pod *v1.Pod, name string) (string, bool) {
	if pod == nil {
		return "", false
	}
	statusGroups := [][]v1.ContainerStatus{
		pod.Status.ContainerStatuses,
		pod.Status.InitContainerStatuses,
		pod.Status.EphemeralContainerStatuses,
	}
	for _, statuses := range statusGroups {
		for _, status := range statuses {
			if status.Name == name {
				containerID := native.TrimContainerIDPrefix(status.ContainerID)
				if containerID != "" {
					return containerID, true
				}
			}
		}
	}
	return "", false
}

// reconcilePendingContainersWithFreshPod treats the round-wide fresh Pod Spec
// as the canonical owner of container names. A pending checkpoint entry whose
// name is absent from that Spec is retired even while the Pod cgroup remains:
// pod-level liveness cannot prove ownership for one container. Entries with a
// current Spec name are rebound only to the fresh status ID; if its cgroup leaf
// is not materialized yet, the allocation remains pending instead of falling
// back to a stale cached identity.
func (p *CPUSetTopologyPlugin) reconcilePendingContainersWithFreshPod(
	ctx context.Context,
	podUID string,
	pending []pendingContainerCPUSet,
	pod *v1.Pod,
) ([]resolvedContainerCPUSet, []pendingContainerCPUSet, []containerLifecycleProof, error) {
	var resolved []resolvedContainerCPUSet
	valid := make([]pendingContainerCPUSet, 0, len(pending))
	var retired []containerLifecycleProof
	for _, container := range pending {
		if pod == nil {
			valid = append(valid, container)
			continue
		}
		if !podSpecHasContainerName(pod, container.ContainerName) {
			retired = append(retired, lifecycleProofFromPending(container, containerLifecycleRetired))
			continue
		}
		containerID, ok := podStatusContainerID(pod, container.ContainerName)
		if !ok {
			valid = append(valid, container)
			continue
		}
		fresh, err := p.resolveMaterializedContainerFromFreshID(ctx, podUID, resolvedContainerCPUSet{
			PodUID:        podUID,
			ContainerName: container.ContainerName,
			CPUs:          container.CPUs,
		}, containerID)
		if err == nil {
			resolved = append(resolved, fresh)
			continue
		}
		if errors.Is(err, os.ErrNotExist) {
			container.ContainerID = containerID
			valid = append(valid, container)
			continue
		}
		return nil, nil, nil, fmt.Errorf(
			"resolve current generation for pending pod=%q container=%q id=%q: %w",
			podUID, container.ContainerName, containerID, err)
	}
	return resolved, valid, retired, nil
}

func podSpecHasContainerName(pod *v1.Pod, name string) bool {
	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			return true
		}
	}
	for _, container := range pod.Spec.InitContainers {
		if container.Name == name {
			return true
		}
	}
	for _, container := range pod.Spec.EphemeralContainers {
		if container.Name == name {
			return true
		}
	}
	return false
}

func relativePendingPodScopeCandidates(candidates []string) []string {
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, strings.Trim(candidate, "/"))
	}
	return out
}

func (p *CPUSetTopologyPlugin) pendingPodScopeCandidatesForQOS(
	podUID string,
	qosClass v1.PodQOSClass,
) []string {
	primary := strings.Trim(p.cfg.BulkheadPrimaryRelPath, "/")
	if primary == "" {
		return relativePendingPodScopeCandidates(
			cgcommon.GetPodRelativeCgroupPathCandidatesForQOS(podUID, qosClass))
	}
	configured := cgcommon.GetPodRelativeCgroupPathCandidatesForQOS(podUID, qosClass)
	underPrimary := make([]string, 0, len(configured))
	for _, candidate := range configured {
		rel := strings.Trim(candidate, "/")
		if rel == primary || strings.HasPrefix(rel, primary+"/") {
			underPrimary = append(underPrimary, rel)
		}
	}
	if len(underPrimary) > 0 {
		return underPrimary
	}

	// Custom primary roots are outside the Kubernetes root registry. Preserve
	// the cgroupfs hierarchy shape relative to that explicitly configured root.
	podName := cgcommon.PodCgroupPathPrefix + podUID
	switch qosClass {
	case v1.PodQOSGuaranteed:
		return []string{path.Join(primary, podName)}
	case v1.PodQOSBurstable:
		return []string{path.Join(primary, "burstable", podName)}
	case v1.PodQOSBestEffort:
		return []string{path.Join(primary, "besteffort", podName)}
	default:
		return cgcommon.GetPodRelativeCgroupPathCandidates(podUID)
	}
}

// selectConcretePendingPodScope observes each normalized, allowed candidate
// once. An all-typed-ENOENT result only authorizes the caller to classify the
// allocation as stale after a fresh Pod lookup also reports that the Pod is
// absent; every other observation error fails closed.
func (p *CPUSetTopologyPlugin) selectConcretePendingPodScope(
	ctx context.Context,
	podUID string,
	candidates []string,
) (string, bool, error) {
	if p.cgroup == nil {
		return "", false, fmt.Errorf("cgroup client is nil")
	}
	allowed := p.allowedPendingPodScopeCandidates(podUID)
	normalized := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		rel, err := normalizePendingPodScopeCandidate(candidate)
		if err != nil {
			return "", false, err
		}
		if _, ok := allowed[rel]; !ok {
			return "", false, fmt.Errorf("pending pod scope candidate %q is outside allowed roots", candidate)
		}
		if _, ok := seen[rel]; ok {
			continue
		}
		seen[rel] = struct{}{}
		normalized = append(normalized, rel)
	}

	var concrete []string
	for _, rel := range normalized {
		if _, err := p.cgroup.StatDir(ctx, rel); err == nil {
			concrete = append(concrete, rel)
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", false, err
		}
	}
	switch len(concrete) {
	case 0:
		return "", true, nil
	case 1:
		return concrete[0], false, nil
	default:
		return "", false, fmt.Errorf("%w: pod=%q candidates=%v",
			errPendingPodScopeAmbiguous, podUID, concrete)
	}
}

func normalizePendingPodScopeCandidate(candidate string) (string, error) {
	if candidate == "" || strings.HasPrefix(candidate, "/") {
		return "", fmt.Errorf("unsafe pending pod scope candidate %q", candidate)
	}
	for _, component := range strings.Split(candidate, "/") {
		if component == "." || component == ".." {
			return "", fmt.Errorf("unsafe pending pod scope candidate %q", candidate)
		}
	}
	rel := path.Clean(candidate)
	if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("unsafe pending pod scope candidate %q", candidate)
	}
	return rel, nil
}

func (p *CPUSetTopologyPlugin) allowedPendingPodScopeCandidates(podUID string) map[string]struct{} {
	allowed := make(map[string]struct{})
	add := func(candidates []string) {
		for _, candidate := range candidates {
			rel := path.Clean(strings.Trim(candidate, "/"))
			if rel != "" && rel != "." && rel != ".." && !strings.HasPrefix(rel, "../") {
				allowed[rel] = struct{}{}
			}
		}
	}
	add(cgcommon.GetPodRelativeCgroupPathCandidates(podUID))
	add(p.pendingPodScopeCandidatesForQOS(podUID, v1.PodQOSGuaranteed))
	add(p.pendingPodScopeCandidatesForQOS(podUID, v1.PodQOSBurstable))
	add(p.pendingPodScopeCandidatesForQOS(podUID, v1.PodQOSBestEffort))
	return allowed
}

func (p *CPUSetTopologyPlugin) reclassifyAdmissionDeferredLeaves(ctx context.Context, view *model.DesiredView, expectedRes *expectedCPUSetBuildResult) {
	if view == nil || expectedRes == nil || len(expectedRes.ExpectedByRel) == 0 {
		return
	}
	for rel, cpus := range expectedRes.ExpectedByRel {
		current, readErr := p.cgroup.ReadCPUSet(ctx, rel)
		if readErr != nil || current.Equals(cpus) {
			continue
		}
		// Admission parent-safe only needs materialized leaves to avoid the
		// reclaim domain while the exact leaf write catches up. This accepts both
		// old superset cleanup and primary-internal relocation after transient
		// pending protection has removed pending CPUs from DesiredReclaimEffective.
		if current.Intersection(view.DesiredReclaimEffective).IsEmpty() &&
			cpus.Intersection(view.DesiredReclaimEffective).IsEmpty() {
			expectedRes.DeferredLeafByRel[rel] = cpus.Clone()
			delete(expectedRes.ExpectedByRel, rel)
		}
	}
}

func (p *CPUSetTopologyPlugin) recordDeferredLeafDrains(deferred map[string]machine.CPUSet) {
	if len(deferred) == 0 {
		return
	}
	if p.now == nil {
		p.now = time.Now
	}
	if p.deferredLeafDrains == nil {
		p.deferredLeafDrains = map[string]deferredLeafDrain{}
	}

	now := p.now()
	for rel, target := range deferred {
		old := p.deferredLeafDrains[rel]
		firstSeen := old.firstSeen
		if firstSeen.IsZero() {
			firstSeen = now
		}
		p.deferredLeafDrains[rel] = deferredLeafDrain{
			target:       target.Clone(),
			firstSeen:    firstSeen,
			lastSeen:     now,
			protectUntil: now.Add(defaultDeferredLeafDrainTTL),
		}
	}
}

func (p *CPUSetTopologyPlugin) drainSafeDeferredLeaves(
	ctx context.Context,
	view *model.DesiredView,
	dag *topology.TopoDAG,
	adjustmentBudget *topology.AdjustmentBudget,
) error {
	if len(p.deferredLeafDrains) == 0 || view == nil || dag == nil {
		return nil
	}
	if p.now == nil {
		p.now = time.Now
	}

	now := p.now()
	for rel, drain := range p.deferredLeafDrains {
		if !now.Before(drain.protectUntil) {
			general.Warningf("bulkhead: deferred leaf drain expired, rel=%q target=%s first_seen=%s last_seen=%s",
				rel, drain.target.String(), drain.firstSeen.Format(time.RFC3339Nano), drain.lastSeen.Format(time.RFC3339Nano))
			delete(p.deferredLeafDrains, rel)
			continue
		}

		done, wrote, err := p.tryDrainOneDeferredLeaf(
			ctx, view, dag, rel, drain.target, adjustmentBudget)
		if err != nil {
			general.Warningf("bulkhead: deferred leaf drain skipped, rel=%q target=%s err=%v", rel, drain.target.String(), err)
			if wrote || errors.Is(err, topology.ErrAdjustmentWriteBudgetExceeded) {
				return err
			}
			continue
		}
		if done {
			delete(p.deferredLeafDrains, rel)
		}
	}
	return nil
}

func (p *CPUSetTopologyPlugin) tryDrainOneDeferredLeaf(
	ctx context.Context,
	view *model.DesiredView,
	dag *topology.TopoDAG,
	rel string,
	target machine.CPUSet,
	adjustmentBudget *topology.AdjustmentBudget,
) (bool, bool, error) {
	if target.IsEmpty() {
		return true, false, nil
	}

	current, err := p.cgroup.ReadCPUSet(ctx, rel)
	if err != nil {
		if _, statErr := p.cgroup.StatDir(ctx, rel); statErr != nil {
			return true, false, nil
		}
		return false, false, fmt.Errorf("read leaf cpuset %q: %w", rel, err)
	}
	if current.Equals(target) {
		return true, false, nil
	}

	parentRel := path.Dir(rel)
	parent, err := p.cgroup.ReadCPUSet(ctx, parentRel)
	if err != nil {
		return false, false, fmt.Errorf("read parent cpuset %q: %w", parentRel, err)
	}
	if !target.IsSubsetOf(parent) {
		return false, false, fmt.Errorf("target %s is outside parent %q cpuset %s", target.String(), parentRel, parent.String())
	}
	if !target.Intersection(view.DesiredReclaimEffective).IsEmpty() {
		return false, false, fmt.Errorf("target %s overlaps desired reclaim %s", target.String(), view.DesiredReclaimEffective.String())
	}

	actualReclaim, err := p.readActualReclaimUnion(ctx, dag)
	if err != nil {
		return false, false, err
	}
	if !target.Intersection(actualReclaim).IsEmpty() {
		return false, false, fmt.Errorf("target %s overlaps actual reclaim %s", target.String(), actualReclaim.String())
	}

	reservation, err := adjustmentBudget.ReserveExecution(topology.ExecutionReservationCost{
		Forward: topology.PhysicalWriteCost{CPUSetWrites: 1},
	})
	if err != nil {
		return false, false, err
	}
	wrote := false
	defer func() {
		if wrote {
			_ = reservation.Settle(
				topology.PhysicalWriteCost{CPUSetWrites: 1},
				topology.PhysicalWriteCost{},
			)
		} else {
			_ = reservation.Settle(
				topology.PhysicalWriteCost{},
				topology.PhysicalWriteCost{},
			)
		}
	}()
	if err := reservation.RecordWriteAttempt(
		ctx, false, topology.PhysicalWriteCost{CPUSetWrites: 1}); err != nil {
		return false, false, err
	}
	wrote = true
	if err := p.cgroup.ApplyCPUSet(ctx, rel, &cgcommon.CPUSetData{CPUs: target.String(), WriteEmptyCPUs: target.IsEmpty()}); err != nil {
		return false, wrote, fmt.Errorf("write deferred leaf cpuset %q target=%s: %w", rel, target.String(), err)
	}
	after, err := p.cgroup.ReadCPUSet(ctx, rel)
	if err != nil {
		return false, wrote, fmt.Errorf("verify deferred leaf cpuset %q: %w", rel, err)
	}
	if !after.Equals(target) {
		return false, wrote, fmt.Errorf("verify deferred leaf cpuset %q got=%s want=%s", rel, after.String(), target.String())
	}

	general.Infof("bulkhead: deferred leaf exact drained, rel=%q target=%s", rel, target.String())
	return true, wrote, nil
}

func (p *CPUSetTopologyPlugin) readActualReclaimUnion(ctx context.Context, dag *topology.TopoDAG) (machine.CPUSet, error) {
	out := machine.NewCPUSet()
	for _, node := range dag.Nodes() {
		if node.Domain != topology.DomainReclaim {
			continue
		}
		cpus, err := p.cgroup.ReadCPUSet(ctx, node.Rel)
		if err != nil {
			if _, statErr := p.cgroup.StatDir(ctx, node.Rel); statErr != nil {
				continue
			}
			return machine.NewCPUSet(), fmt.Errorf("read actual reclaim cpuset %q: %w", node.Rel, err)
		}
		out = out.Union(cpus)
	}
	return out, nil
}

func topologyCoversProtectedView(topology *machine.CPUTopology, view *model.DesiredView, protected machine.CPUSet) bool {
	if topology == nil || view == nil || protected.IsEmpty() || view.DesiredReclaimEffective.IsEmpty() || topology.CPUDetails.NUMANodes().Size() == 0 {
		return false
	}
	covered := machine.NewCPUSet()
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceNoSortInt() {
		covered = covered.Union(topology.CPUDetails.CPUsInNUMANodes(numaID))
	}
	return protected.IsSubsetOf(covered) && view.DesiredReclaimEffective.IsSubsetOf(covered)
}

func (p *CPUSetTopologyPlugin) pendingProtectedCPUSetByRel(ctx context.Context, pendingByPod []pendingContainerCPUSet) map[string]machine.CPUSet {
	if len(pendingByPod) == 0 {
		p.pendingProtections = map[string]pendingPodProtection{}
		return nil
	}
	if p.now == nil {
		p.now = time.Now
	}
	if p.pendingProtections == nil {
		p.pendingProtections = map[string]pendingPodProtection{}
	}
	now := p.now()
	pendingByPodIndex := make(map[string]int, len(pendingByPod))
	aggregatedPending := make([]pendingContainerCPUSet, 0, len(pendingByPod))
	active := map[string]struct{}{}
	for _, pending := range pendingByPod {
		active[pending.PodUID] = struct{}{}
		if index, ok := pendingByPodIndex[pending.PodUID]; ok {
			aggregatedPending[index].CPUs = aggregatedPending[index].CPUs.Union(pending.CPUs)
			continue
		}
		pendingByPodIndex[pending.PodUID] = len(aggregatedPending)
		aggregatedPending = append(aggregatedPending, pending)
	}
	out := make(map[string]machine.CPUSet, len(aggregatedPending))
	for _, pending := range aggregatedPending {
		protection, ok := p.pendingProtections[pending.PodUID]
		if !ok {
			protection = pendingPodProtection{
				protectUntil: now.Add(defaultPendingPodProtectionTTL),
			}
		} else if !now.Before(protection.protectUntil) {
			expiredAt := protection.protectUntil
			protection.protectUntil = now.Add(defaultPendingPodProtectionTTL)
			general.Warningf("bulkhead: pending pod protection TTL expired while live pending state remains; renewing protection, pod=%q allocation=%s allocation_size=%d expired_at=%s protect_until=%s",
				pending.PodUID, pending.CPUs.String(), pending.CPUs.Size(),
				expiredAt.Format(time.RFC3339Nano), protection.protectUntil.Format(time.RFC3339Nano))
		}
		rel := protection.rel
		if rel == "" {
			var err error
			rel, err = cgcommon.GetPodRelativeCgroupPath(pending.PodUID)
			if err != nil {
				p.pendingProtections[pending.PodUID] = protection
				continue
			}
		}
		rel = strings.Trim(rel, "/")
		if rel == "" {
			p.pendingProtections[pending.PodUID] = protection
			continue
		}
		current, err := p.cgroup.ReadCPUSet(ctx, rel)
		if err != nil || current.IsEmpty() {
			general.InfofV(5, "bulkhead: pending protected rel skipped, pod=%q container=%q rel=%q allocation=%s allocation_size=%d current=%s err=%v reason=missing_or_empty_pod_cgroup protect_until=%s",
				pending.PodUID, pending.ContainerName, rel, pending.CPUs.String(), pending.CPUs.Size(),
				current.String(), err, protection.protectUntil.Format(time.RFC3339Nano))
			p.pendingProtections[pending.PodUID] = protection
			continue
		}
		protection.rel = rel
		protection.current = current
		p.pendingProtections[pending.PodUID] = protection
		// Only protect the pending allocation itself, never the pod cgroup
		// current. The pod cgroup current of a pending pod frequently inherits a
		// much wider cpuset (e.g. the kubepods primary set) before its own
		// allocation is applied. Protecting that wide current would let the
		// transient protected union swallow the reclaim pool and drive
		// ReclaimEffective(PerNUMA) to empty. current is kept for diagnostics
		// only.
		protected := pending.CPUs
		out[rel] = protected
		general.InfofV(5, "bulkhead: pending protected rel, pod=%q container=%q rel=%q allocation=%s allocation_size=%d current=%s protected=%s protected_size=%d overlap=%s dropped_extra=%s protect_until=%s",
			pending.PodUID, pending.ContainerName, rel, pending.CPUs.String(), pending.CPUs.Size(),
			current.String(), protected.String(), protected.Size(),
			current.Intersection(pending.CPUs).String(), current.Difference(pending.CPUs).String(),
			protection.protectUntil.Format(time.RFC3339Nano))
	}
	for podUID := range p.pendingProtections {
		if _, ok := active[podUID]; !ok {
			delete(p.pendingProtections, podUID)
		}
	}
	return out
}

// pendingProtectionScopes owns the single normal-path filesystem read for each
// pending scope. Downstream normal-path consumers must use the returned source
// classification instead of reading the scope again.
// pendingProtectionScopes resolves one stable cgroup observation per scope.
// A successful read proves that the scope exists even when its configured
// cpuset is empty; only typed ENOENT authorizes treating it as not materialized.
// Every other read or stat failure remains fail-closed.
func (p *CPUSetTopologyPlugin) pendingProtectionScopes(
	ctx context.Context,
	dag *topology.TopoDAG,
	pendingByPod []pendingContainerCPUSet,
) ([]topology.PendingProtection, error) {
	if p.now == nil {
		p.now = time.Now
	}
	if p.pendingProtections == nil {
		p.pendingProtections = map[string]pendingPodProtection{}
	}

	now := p.now()
	aggregated := make(map[string]pendingContainerCPUSet, len(pendingByPod))
	active := make(map[string]struct{}, len(pendingByPod))
	for _, pending := range pendingByPod {
		active[pending.PodUID] = struct{}{}
		current, ok := aggregated[pending.PodUID]
		if !ok {
			aggregated[pending.PodUID] = pending
			continue
		}
		current.CPUs = current.CPUs.Union(pending.CPUs)
		if current.ScopeRel == "" {
			current.ScopeRel = pending.ScopeRel
		} else if pending.ScopeRel != "" && current.ScopeRel != pending.ScopeRel {
			return nil, fmt.Errorf("%w: pod=%q scopes=%q,%q",
				errPendingPodScopeAmbiguous, pending.PodUID, current.ScopeRel, pending.ScopeRel)
		}
		if current.NativeQOSClass == "" {
			current.NativeQOSClass = pending.NativeQOSClass
		}
		aggregated[pending.PodUID] = current
	}

	out := make([]topology.PendingProtection, 0, len(aggregated))
	// This invocation owns one observation per scope. Multiple pending Pod
	// records may resolve to the same scope, so they must share that observation.
	type scopeObservation struct {
		current machine.CPUSet
		source  topology.PendingProtectionSource
		err     error
	}
	observedByRel := make(map[string]scopeObservation)
	for podUID, pending := range aggregated {
		protection, ok := p.pendingProtections[podUID]
		if !ok || !now.Before(protection.protectUntil) {
			protection.protectUntil = now.Add(defaultPendingPodProtectionTTL)
		}
		rel := pending.ScopeRel
		if rel == "" {
			rel = protection.rel
		}
		if rel == "" {
			var err error
			candidates := cgcommon.GetPodRelativeCgroupPathCandidatesForQOS(
				podUID, pending.NativeQOSClass)
			rel, err = dag.SelectUniqueControlledPrimaryCandidate(candidates)
			if err != nil {
				return nil, fmt.Errorf("resolve pending pod scope %q with native qos %q from candidates %v: %w",
					podUID, pending.NativeQOSClass, candidates, err)
			}
		}
		rel = path.Clean(strings.Trim(rel, "/"))
		if rel == "." || rel == "" || rel == ".." || strings.HasPrefix(rel, "../") {
			return nil, fmt.Errorf("%w: pod=%q scope=%q",
				topology.ErrInvalidPendingProtection, podUID, rel)
		}

		observation, observed := observedByRel[rel]
		if !observed {
			current, readErr := p.cgroup.ReadCPUSet(ctx, rel)
			observation.current = current
			switch {
			case readErr == nil:
				observation.source = topology.PendingProtectionSourceExistingPod
			default:
				_, statErr := p.cgroup.StatDir(ctx, rel)
				switch {
				case statErr == nil:
					observation.err = fmt.Errorf("read cpuset for existing pending pod scope %q: %w", rel, readErr)
				case errors.Is(statErr, syscall.ENOENT):
					observation.source = topology.PendingProtectionSourceExpectedPod
				default:
					observation.err = fmt.Errorf("stat pending pod scope %q after cpuset read failed: %w", rel, statErr)
				}
			}
			observedByRel[rel] = observation
		}
		if observation.err != nil {
			return nil, observation.err
		}
		if observation.source == topology.PendingProtectionSourceExistingPod {
			protection.current = observation.current
		}
		protection.rel = rel
		p.pendingProtections[podUID] = protection
		general.Infof("bulkhead: pending pod scope selected, pod=%q native_qos=%q scope=%q source=%q cpuset=%s",
			podUID, pending.NativeQOSClass, rel, observation.source, pending.CPUs.String())
		out = append(out, topology.PendingProtection{
			ScopeRel: rel,
			CPUs:     pending.CPUs.Clone(),
			PodUID:   podUID,
			Source:   observation.source,
		})
	}
	for podUID := range p.pendingProtections {
		if _, ok := active[podUID]; !ok {
			delete(p.pendingProtections, podUID)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ScopeRel != out[j].ScopeRel {
			return out[i].ScopeRel < out[j].ScopeRel
		}
		return out[i].PodUID < out[j].PodUID
	})
	return out, nil
}

// pendingProtectedCPUSetByResolvedScopes derives protection only from the
// already resolved scope classifications, avoiding duplicate cgroup I/O.
func pendingProtectedCPUSetByResolvedScopes(protections []topology.PendingProtection) map[string]machine.CPUSet {
	out := make(map[string]machine.CPUSet)
	for _, protection := range protections {
		if protection.Source != topology.PendingProtectionSourceExistingPod {
			continue
		}
		out[protection.ScopeRel] = out[protection.ScopeRel].Union(protection.CPUs)
	}
	return out
}

func unionCPUSetByRel(byRel map[string]machine.CPUSet) machine.CPUSet {
	union := machine.NewCPUSet()
	for _, cpus := range byRel {
		union = union.Union(cpus)
	}
	return union
}

func pendingProtectionCPUSetUnion(protections []topology.PendingProtection) machine.CPUSet {
	union := machine.NewCPUSet()
	for _, protection := range protections {
		union = union.Union(protection.CPUs)
	}
	return union
}

func formatCPUSetByRel(byRel map[string]machine.CPUSet) string {
	if len(byRel) == 0 {
		return "{}"
	}
	rels := make([]string, 0, len(byRel))
	for rel := range byRel {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	parts := make([]string, 0, len(rels))
	for _, rel := range rels {
		parts = append(parts, fmt.Sprintf("%s=%s", rel, byRel[rel].String()))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatCPUSetByNUMA(byNUMA map[int]machine.CPUSet) string {
	if len(byNUMA) == 0 {
		return "{}"
	}
	numaIDs := make([]int, 0, len(byNUMA))
	for numaID := range byNUMA {
		numaIDs = append(numaIDs, numaID)
	}
	sort.Ints(numaIDs)
	parts := make([]string, 0, len(numaIDs))
	for _, numaID := range numaIDs {
		parts = append(parts, fmt.Sprintf("%d=%s", numaID, byNUMA[numaID].String()))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (p *CPUSetTopologyPlugin) discoverBulkheadReclaimSiblings(ctx context.Context, view *model.DesiredView) ([]string, error) {
	if !p.cfg.EnableBulkheadReclaimSiblings || p.cgroup.Version(ctx) != cgroupclient.CgroupVersionV1 {
		return nil, nil
	}

	excluded := map[string]struct{}{}
	addExcluded := func(rel string) {
		rel = strings.Trim(rel, "/")
		if rel != "" {
			excluded[rel] = struct{}{}
		}
	}
	addExcluded(p.cfg.BulkheadPrimaryRelPath)
	for _, rel := range p.cfg.BulkheadReclaimRelPaths {
		addExcluded(rel)
	}
	for _, rel := range p.cfg.BulkheadPartitionRelPaths {
		addExcluded(rel)
	}
	if view != nil {
		for reclaimIdx := range p.cfg.BulkheadReclaimRelPaths {
			for numaID := range view.ReclaimEffectivePerNUMA {
				addExcluded(p.cfg.ReclaimPerNUMA(reclaimIdx, numaID))
			}
		}
	}

	seen := map[string]struct{}{}
	var out []string
	for _, reclaimRel := range p.cfg.BulkheadReclaimRelPaths {
		reclaimRel = strings.Trim(reclaimRel, "/")
		if reclaimRel == "" {
			continue
		}
		parentRel := path.Dir(reclaimRel)
		if parentRel == "." {
			parentRel = ""
		}
		children, err := p.cgroup.ListChildren(ctx, parentRel)
		if err != nil {
			return nil, fmt.Errorf("list reclaim sibling parent %q: %w", parentRel, err)
		}
		for _, child := range children {
			rel := strings.Trim(path.Join(parentRel, child), "/")
			if rel == "" {
				continue
			}
			if _, skip := excluded[rel]; skip {
				continue
			}
			if p.isConfiguredReclaimNUMARel(rel) {
				continue
			}
			if _, ok := seen[rel]; ok {
				continue
			}
			seen[rel] = struct{}{}
			out = append(out, rel)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (p *CPUSetTopologyPlugin) configuredBulkheadReclaimSiblings() []string {
	out := make([]string, 0, len(p.cfg.BulkheadReclaimSiblingRelPaths))
	for _, rel := range p.cfg.BulkheadReclaimSiblingRelPaths {
		rel = strings.Trim(rel, "/")
		if rel == "" {
			continue
		}
		out = append(out, rel)
	}
	sort.Strings(out)
	return out
}

func (p *CPUSetTopologyPlugin) mergeBulkheadReclaimSiblings(
	discovered []string,
	configured []string,
	view *model.CPUSetPartitionView,
) []string {
	excluded := map[string]struct{}{}
	addExcluded := func(rel string) {
		rel = strings.Trim(rel, "/")
		if rel != "" {
			excluded[rel] = struct{}{}
		}
	}
	addExcluded(p.cfg.BulkheadPrimaryRelPath)
	for _, rel := range p.cfg.BulkheadReclaimRelPaths {
		addExcluded(rel)
	}
	for _, rel := range p.cfg.BulkheadPartitionRelPaths {
		addExcluded(rel)
	}
	if view != nil {
		for reclaimIdx := range p.cfg.BulkheadReclaimRelPaths {
			for numaID := range view.ReclaimEffectivePerNUMA {
				addExcluded(p.cfg.ReclaimPerNUMA(reclaimIdx, numaID))
			}
		}
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(discovered)+len(configured))
	add := func(rel string) {
		rel = strings.Trim(rel, "/")
		if rel == "" {
			return
		}
		if _, skip := excluded[rel]; skip {
			return
		}
		if p.isConfiguredReclaimNUMARel(rel) {
			return
		}
		if _, ok := seen[rel]; ok {
			return
		}
		seen[rel] = struct{}{}
		out = append(out, rel)
	}
	for _, rel := range discovered {
		add(rel)
	}
	for _, rel := range configured {
		add(rel)
	}
	sort.Strings(out)
	return out
}

func desiredCPUSetPartitionView(view *model.DesiredView) *model.CPUSetPartitionView {
	if view == nil {
		return nil
	}
	return &view.CPUSetPartitionView
}

func enableBulkheadCpusetTopology(in bulkheadapi.HandlerContext) bool {
	if in.State != nil && in.State.GetAllowSharedCoresOverlapReclaimedCores() {
		return false
	}
	return enableBulkheadCpusetTopologyByDynamicConf(in.DynamicConf)
}

func enableBulkheadCpusetTopologyByDynamicConf(conf *dynamicconfig.Configuration) bool {
	if conf == nil || conf.AdminQoSConfiguration == nil || conf.AdminQoSConfiguration.CPUPluginConfiguration == nil {
		return false
	}
	return conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadCpusetTopology
}

func (p *CPUSetTopologyPlugin) isConfiguredReclaimNUMARel(rel string) bool {
	rel = strings.Trim(rel, "/")
	for _, prefix := range p.cfg.BulkheadReclaimNumaPrefixes {
		prefix = strings.Trim(prefix, "/")
		if prefix == "" || !strings.HasPrefix(rel, prefix) {
			continue
		}
		suffix := strings.TrimPrefix(rel, prefix)
		if suffix == "" {
			continue
		}
		if _, err := strconv.Atoi(suffix); err == nil {
			return true
		}
	}
	return false
}

func topologyBudgetFromConfig(in bulkheadconfig.ConvergenceBudget) topology.ConvergenceBudget {
	return topology.ConvergenceBudget{
		MaxRounds:                  in.MaxRounds,
		MaxHierarchyIOOperations:   in.MaxHierarchyIOOperations,
		MaxSnapshotNodes:           in.MaxSnapshotNodes,
		MaxSnapshotDepth:           in.MaxSnapshotDepth,
		MaxDomains:                 in.MaxDomains,
		MaxTransferEdges:           in.MaxTransferEdges,
		MaxPlanOperations:          in.MaxPlanOperations,
		MaxDeadlockProbeOperations: in.MaxDeadlockProbeOperations,
		DeadlineDuration:           in.DeadlineDuration,
	}
}

func topologyDrainSelectionFromConfig(in bulkheadconfig.DrainSelectionPolicy) topology.DrainSelectionPolicy {
	return topology.DrainSelectionPolicy{
		MaxCPUsDrainRatio:         in.MaxCPUsDrainRatio,
		GroupByNUMA:               in.GroupByNUMA,
		RequirePairedSwapProgress: in.RequirePairedSwapProgress,
	}
}

const (
	metricBulkheadPruneResult                  = "bulkhead_prune_result"
	metricBulkheadPruneActiveRels              = "bulkhead_prune_active_rels"
	metricBulkheadTopologyRoundTotal           = "bulkhead_topology_round_total"
	metricBulkheadTopologyRoundsPerApply       = "bulkhead_topology_rounds_per_apply"
	metricBulkheadTopologyBudgetExhaustedTotal = "bulkhead_topology_budget_exhausted_total"
	metricBulkheadTopologyScanNodes            = "bulkhead_topology_scan_nodes"
	metricBulkheadTopologyScanDepth            = "bulkhead_topology_scan_depth"
	metricBulkheadTopologyDrainBatch           = "bulkhead_topology_drain_batch"
	metricBulkheadTopologyIdentityChangedTotal = "bulkhead_topology_identity_changed_total"
)

var (
	allowedTopologyMetricPhases      = map[string]struct{}{"normal": {}, "reclaim_only": {}, "reset": {}}
	allowedTopologyMetricStatuses    = map[string]struct{}{"progress": {}, "stale": {}, "blocked": {}, "converged": {}, "error": {}}
	allowedTopologyMetricReasons     = map[string]struct{}{"none": {}, "stale": {}, "blocked": {}, "budget": {}, "identity_changed": {}, "external_write": {}, "invalid": {}}
	allowedTopologyMetricDomainRoles = map[string]struct{}{"primary": {}, "reclaim": {}, "reclaim_numa": {}, "dynamic": {}, "unknown": {}}
	allowedPruneMetricStatuses       = map[string]struct{}{"success": {}, "skipped": {}}
	allowedPruneMetricReasons        = map[string]struct{}{
		"none": {}, "container_error": {}, "view_error": {}, "discover_error": {},
		"dag_error": {}, "not_converged": {}, "deferred_convergence": {},
		"reset_target_error": {}, "reset_not_converged": {}, "invalid": {},
	}
)

func emitBulkheadTopologySummary(emitter metrics.MetricEmitter, phase string, res topology.ConvergenceResult, err error) {
	if emitter == nil {
		return
	}
	phase = boundedTopologyLabel(phase, allowedTopologyMetricPhases, "normal")
	status := "converged"
	reason := "none"
	if err != nil {
		status = "error"
		reason = topologyErrorReason(err)
	} else if !res.Converged {
		status = string(res.State)
		reason = topologyStateReason(res.State)
	}
	status = boundedTopologyLabel(status, allowedTopologyMetricStatuses, "error")
	reason = boundedTopologyLabel(reason, allowedTopologyMetricReasons, "invalid")
	general.Infof("cpuset_topology: apply summary phase=%s status=%s reason=%s rounds=%d applied=%d skipped=%d failed=%d deferred=%d first_blocker=%q",
		phase, status, reason, len(res.Rounds), res.Applied, res.Skipped, res.Failed, res.Deferred, res.FirstBlocker())

	_ = emitter.StoreInt64(metricBulkheadTopologyRoundsPerApply, int64(len(res.Rounds)), metrics.MetricTypeNameRaw,
		metrics.MetricTag{Key: "phase", Val: phase},
		metrics.MetricTag{Key: "status", Val: status},
		metrics.MetricTag{Key: "reason", Val: reason},
	)
	for _, round := range res.Rounds {
		roundStatus := boundedTopologyLabel(string(round.Status), allowedTopologyMetricStatuses, status)
		roundReason := topologyRoundReason(round.Status)
		_ = emitter.StoreInt64(metricBulkheadTopologyRoundTotal, 1, metrics.MetricTypeNameCount,
			metrics.MetricTag{Key: "phase", Val: phase},
			metrics.MetricTag{Key: "status", Val: roundStatus},
			metrics.MetricTag{Key: "reason", Val: roundReason},
		)
		_ = emitter.StoreInt64(metricBulkheadTopologyScanNodes, int64(round.Cost.Nodes), metrics.MetricTypeNameRaw,
			metrics.MetricTag{Key: "phase", Val: phase},
			metrics.MetricTag{Key: "status", Val: roundStatus},
			metrics.MetricTag{Key: "reason", Val: roundReason},
		)
		_ = emitter.StoreInt64(metricBulkheadTopologyScanDepth, int64(round.Cost.MaxDepth), metrics.MetricTypeNameRaw,
			metrics.MetricTag{Key: "phase", Val: phase},
			metrics.MetricTag{Key: "status", Val: roundStatus},
			metrics.MetricTag{Key: "reason", Val: roundReason},
		)
		drained := 0
		for _, witness := range round.Witnesses {
			drained += witness.CPUs.Size()
		}
		_ = emitter.StoreInt64(metricBulkheadTopologyDrainBatch, int64(drained), metrics.MetricTypeNameRaw,
			metrics.MetricTag{Key: "phase", Val: phase},
			metrics.MetricTag{Key: "domain_role", Val: "unknown"},
		)
	}
	if reason == "budget" {
		_ = emitter.StoreInt64(metricBulkheadTopologyBudgetExhaustedTotal, 1, metrics.MetricTypeNameCount,
			metrics.MetricTag{Key: "kind", Val: topologyBudgetKind(err)},
		)
	}
	if reason == "identity_changed" {
		_ = emitter.StoreInt64(metricBulkheadTopologyIdentityChangedTotal, 1, metrics.MetricTypeNameCount)
	}
}

func topologyStateReason(state topology.ConvergenceState) string {
	switch state {
	case topology.ConvergenceStateBlocked:
		return "blocked"
	case topology.ConvergenceStateNonConverged:
		return "stale"
	default:
		return "none"
	}
}

func topologyRoundReason(status topology.RoundStatus) string {
	switch status {
	case topology.RoundStatusStale:
		return "stale"
	case topology.RoundStatusBlocked:
		return "blocked"
	default:
		return "none"
	}
}

func topologyErrorReason(err error) string {
	switch {
	case errors.Is(err, topology.ErrRoundBudgetExceeded),
		errors.Is(err, topology.ErrHierarchyIOOperationBudgetExceeded),
		errors.Is(err, topology.ErrNodeBudgetExceeded),
		errors.Is(err, topology.ErrHierarchyDepthBudget),
		errors.Is(err, topology.ErrDomainBudgetExceeded),
		errors.Is(err, topology.ErrTransferEdgeBudgetExceeded),
		errors.Is(err, topology.ErrPlanOperationBudgetExceeded),
		errors.Is(err, topology.ErrDeadlockProbeBudgetExceeded),
		errors.Is(err, topology.ErrConvergenceDeadlineExceeded),
		errors.Is(err, topology.ErrAdjustmentReplanBudgetExceeded),
		errors.Is(err, topology.ErrAdjustmentWriteBudgetExceeded),
		errors.Is(err, topology.ErrAdjustmentDeadlineExceeded),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return "budget"
	case errors.Is(err, topology.ErrCgroupIdentityChanged):
		return "identity_changed"
	default:
		return "invalid"
	}
}

func topologyBudgetKind(err error) string {
	switch {
	case errors.Is(err, topology.ErrRoundBudgetExceeded):
		return "round"
	case errors.Is(err, topology.ErrHierarchyIOOperationBudgetExceeded):
		return "hierarchy_io"
	case errors.Is(err, topology.ErrNodeBudgetExceeded):
		return "node"
	case errors.Is(err, topology.ErrHierarchyDepthBudget):
		return "depth"
	case errors.Is(err, topology.ErrDomainBudgetExceeded):
		return "domain"
	case errors.Is(err, topology.ErrTransferEdgeBudgetExceeded):
		return "edge"
	case errors.Is(err, topology.ErrPlanOperationBudgetExceeded):
		return "operation"
	case errors.Is(err, topology.ErrDeadlockProbeBudgetExceeded):
		return "deadlock_probe"
	case errors.Is(err, topology.ErrAdjustmentReplanBudgetExceeded):
		return "adjustment_replan"
	case errors.Is(err, topology.ErrAdjustmentWriteBudgetExceeded):
		return "adjustment_write"
	case errors.Is(err, topology.ErrAdjustmentDeadlineExceeded):
		return "adjustment_deadline"
	case errors.Is(err, topology.ErrConvergenceDeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	default:
		return "context"
	}
}

func boundedTopologyLabel(value string, allowed map[string]struct{}, fallback string) string {
	if _, ok := allowed[value]; ok {
		return value
	}
	return fallback
}

func normalizePruneMetricLabels(status, reason string) (string, string) {
	status = boundedTopologyLabel(status, allowedPruneMetricStatuses, "skipped")
	if reason == "" {
		reason = "none"
	}
	return status, boundedTopologyLabel(reason, allowedPruneMetricReasons, "invalid")
}

func emitBulkheadPruneResult(emitter metrics.MetricEmitter, status, reason string) {
	if emitter == nil {
		return
	}
	status, reason = normalizePruneMetricLabels(status, reason)
	_ = emitter.StoreInt64(metricBulkheadPruneResult, 1, metrics.MetricTypeNameCount,
		metrics.MetricTag{Key: "status", Val: status},
		metrics.MetricTag{Key: "reason", Val: reason},
	)
}

func emitBulkheadPruneActiveRels(
	emitter metrics.MetricEmitter,
	activeRelsCount int,
	status, reason string,
) {
	if emitter == nil {
		return
	}
	status, reason = normalizePruneMetricLabels(status, reason)
	_ = emitter.StoreInt64(metricBulkheadPruneActiveRels, int64(activeRelsCount), metrics.MetricTypeNameRaw,
		metrics.MetricTag{Key: "status", Val: status},
		metrics.MetricTag{Key: "reason", Val: reason},
	)
}
