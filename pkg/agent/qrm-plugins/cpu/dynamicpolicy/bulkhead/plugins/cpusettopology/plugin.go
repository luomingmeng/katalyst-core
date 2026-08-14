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
	"time"

	apierrors "k8s.io/apimachinery/pkg/util/errors"

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
)

const CPUSetTopologyPluginName = "cpuset_topology"

const (
	defaultPendingPodProtectionTTL = 10 * time.Second
	defaultDeferredLeafDrainTTL    = 2 * time.Minute
)

var (
	_ bulkheadapi.Plugin         = (*CPUSetTopologyPlugin)(nil)
	_ bulkheadapi.TopologyPlugin = (*CPUSetTopologyPlugin)(nil)
)

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
	return enableBulkheadCpusetTopology(in)
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

func (p *CPUSetTopologyPlugin) CPUSetAdjustmentHandler(ctx context.Context, in bulkheadapi.HandlerContext) error {
	handlerStartedAt := time.Now()
	var admissionDeadline time.Time
	if p.cfg.EnableAdmissionLeafDefer &&
		in.Mode.OrFullDefault() == cpusetutil.CPUSetAdjustmentModeAdmission &&
		p.cfg.AdmissionSafeDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, handlerStartedAt.Add(p.cfg.AdmissionSafeDuration))
		defer cancel()
		admissionDeadline, _ = ctx.Deadline()
	}
	if in.DesiredView == nil {
		return nil
	}
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
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "container_error")
		return fmt.Errorf("build expected container cpuset: %w", err)
	}
	protectedByRel := p.pendingProtectedCPUSetByRel(ctx, expectedRes.PendingByPod)
	if deadlineErr := admissionStageDeadlineError(ctx, "protect pending container cpuset"); deadlineErr != nil {
		return deadlineErr
	}
	if protected := expectedRes.PendingCPUSetUnion().Union(unionCPUSetByRel(protectedByRel)); topologyCoversProtectedView(in.Topology, in.DesiredView, protected) {
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
			emitBulkheadPruneResult(in.Emitter, "skipped", 0, "view_error")
			return fmt.Errorf("validate bulkhead desired view after transient pending protection: %w", err)
		}
		if p.cfg.EnableAdmissionLeafDefer && in.Mode.OrFullDefault() == cpusetutil.CPUSetAdjustmentModeAdmission {
			p.reclassifyAdmissionDeferredLeaves(ctx, in.DesiredView, expectedRes)
		}
	}
	p.recordDeferredLeafDrains(expectedRes.DeferredLeafByRel)
	siblings, err := p.discoverBulkheadReclaimSiblings(ctx, in.DesiredView)
	if deadlineErr := admissionStageDeadlineError(ctx, "discover bulkhead reclaim siblings"); deadlineErr != nil {
		return deadlineErr
	}
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "discover_error")
		return fmt.Errorf("discover bulkhead reclaim siblings: %w", err)
	}
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
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "dag_error")
		return fmt.Errorf("build bulkhead topology dag: %w", err)
	}
	p.drainSafeDeferredLeaves(ctx, in.DesiredView, dag)
	general.InfofV(5, "cpuset_topology: apply start specs=%d siblings=%d expected_leaf_count=%d pending_count=%d protected_pending=%s protected_rel_count=%d",
		len(specs), len(siblings), len(expectedRes.ExpectedByRel), len(expectedRes.PendingByPod),
		expectedRes.PendingCPUSetUnion().String(), len(protectedByRel))
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
	convergenceBudget := topologyBudgetFromConfig(p.cfg.TopologyConvergenceBudget)
	if !admissionDeadline.IsZero() {
		convergenceBudget.Deadline = admissionDeadline
	}
	res, err := (topology.TopologyCoordinator{}).Converge(ctx, topology.CoordinatorInput{
		DAG:                 dag,
		Cgroup:              p.cgroup,
		Mode:                topology.NormalModeGuardWithGate(p.sharedModeGate()),
		Budget:              convergenceBudget,
		DrainSelection:      topologyDrainSelectionFromConfig(p.cfg.TopologyDrainSelection),
		CPUDetails:          cpuDetails,
		ReservedCPUSet:      reservedCPUSet,
		ExpectedCPUSetByRel: expectedRes.ExpectedByRel,
		Objective:           objective,
		DeferredCPUSetByRel: expectedRes.DeferredLeafByRel,
		AdmissionBudget: &topology.AdmissionConvergenceBudget{
			MaxRequiredWrites: p.cfg.AdmissionMaxRequiredWrites,
		},
		ProtectedPendingCPUSet: expectedRes.PendingCPUSetUnion(),
		ProtectedCPUSetByRel:   protectedByRel,
		PublishFinalSnapshot: func(snapshot *topology.CompleteSnapshot) error {
			appliedView, err := appliedViewFromFinalSnapshotWithContext(
				ctx, in.MetaServer, in.DesiredView, dag, snapshot,
				expectedRes.ExpectedByRel, expectedRes.DeferredLeafByRel)
			if err != nil {
				return fmt.Errorf("derive applied view from final topology snapshot: %w", err)
			}
			finalAppliedView = appliedView
			return nil
		},
		PublishParentSafeSnapshot: func(snapshot *topology.CompleteSnapshot, deferredCleanupRels map[string]struct{}) error {
			appliedView, err := appliedViewFromFinalSnapshotWithDeferredCleanup(
				ctx, in.MetaServer, in.DesiredView, dag, snapshot, deferredCleanupRels,
				expectedRes.ExpectedByRel, expectedRes.DeferredLeafByRel)
			if err != nil {
				return fmt.Errorf("derive parent-safe applied view from final topology snapshot: %w", err)
			}
			finalAppliedView = appliedView
			return nil
		},
	})
	general.Infof("cpuset_topology: coordinator converge finished duration=%s err=%v attempted=%d applied=%d skipped=%d failed=%d deferred=%d converged=%t final_snapshot_current=%t state=%s expected_leaf_count=%d pending_count=%d pending_cpu_count=%d protected_rel_count=%d specs=%d siblings=%d",
		time.Since(convergeStart), err, res.Attempted, res.Applied, res.Skipped, res.Failed, res.Deferred,
		res.Converged, res.FinalSnapshotCurrent, res.State, len(expectedRes.ExpectedByRel), len(expectedRes.PendingByPod),
		expectedRes.PendingCPUSetUnion().Size(), len(protectedByRel), len(specs), len(siblings))
	if err != nil {
		emitBulkheadTopologySummary(in.Emitter, "normal", res, err)
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "dag_error")
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
	if res.ParentSafe && len(expectedRes.DeferredLeafByRel) > 0 {
		p.drainSafeDeferredLeaves(ctx, in.DesiredView, dag)
	}
	emitBulkheadTopologySummary(in.Emitter, "normal", res, nil)
	if !res.Converged && !res.ParentSafe {
		general.InfofV(4, "cpuset_topology: apply not fully converged, deferred=%d state=%s report=%+v", res.Deferred, res.State, res.ConvergenceReport)
		reason := "not_converged"
		if res.Deferred > 0 {
			reason = "deferred_convergence"
		}
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, reason)
		return &topologyApplyNonConvergedError{result: res}
	}
	if res.ParentSafe {
		if err := handleDeferredLeafRetry(in.Mode, in.ScheduleFullRetry); err != nil {
			return err
		}
	}

	activeRels := bulkheadutils.CollectActiveRels(p.cfg, desiredCPUSetPartitionView(in.DesiredView), in.MetaServer, siblings, relExists)
	p.cgroup.Prune(activeRels)
	emitBulkheadPruneResult(in.Emitter, "success", len(activeRels), "")
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

func appliedViewFromFinalSnapshot(
	metaServer *metaserver.MetaServer,
	desired *model.DesiredView,
	dag *topology.TopoDAG,
	snapshot *topology.CompleteSnapshot,
	expectedCPUSetByRel ...map[string]machine.CPUSet,
) (*model.AppliedView, error) {
	return appliedViewFromFinalSnapshotWithContext(
		context.Background(), metaServer, desired, dag, snapshot, expectedCPUSetByRel...)
}

func appliedViewFromFinalSnapshotWithContext(
	ctx context.Context,
	metaServer *metaserver.MetaServer,
	desired *model.DesiredView,
	dag *topology.TopoDAG,
	snapshot *topology.CompleteSnapshot,
	expectedCPUSetByRel ...map[string]machine.CPUSet,
) (*model.AppliedView, error) {
	return appliedViewFromFinalSnapshotWithDeferredCleanup(
		ctx, metaServer, desired, dag, snapshot, nil, expectedCPUSetByRel...)
}

func appliedViewFromFinalSnapshotWithDeferredCleanup(
	ctx context.Context,
	metaServer *metaserver.MetaServer,
	desired *model.DesiredView,
	dag *topology.TopoDAG,
	snapshot *topology.CompleteSnapshot,
	deferredCleanupRels map[string]struct{},
	expectedCPUSetByRel ...map[string]machine.CPUSet,
) (*model.AppliedView, error) {
	if desired == nil || dag == nil || snapshot == nil {
		return nil, fmt.Errorf("desired view, topology dag and final snapshot are required")
	}
	applied := &model.AppliedView{
		CPUSetPartitionView: model.NewCPUSetPartitionView(),
		CPUSetByRel:         make(map[string]machine.CPUSet, len(dag.Nodes())),
		RelProofByRel:       make(map[string]model.CgroupRelProof, len(dag.Nodes())),
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
	var expected map[string]machine.CPUSet
	var deferred map[string]machine.CPUSet
	if len(expectedCPUSetByRel) > 0 {
		expected = expectedCPUSetByRel[0]
	}
	if len(expectedCPUSetByRel) > 1 {
		deferred = expectedCPUSetByRel[1]
	}
	containerCPUSetByPod, err := containerCPUSetByPodFromFinalSnapshotWithDeferredCleanup(
		ctx, metaServer, desired, snapshot, expected, deferred, deferredCleanupRels)
	if err != nil {
		return nil, err
	}
	applied.ContainerCPUSetByPod = containerCPUSetByPod
	return applied, nil
}

func containerCPUSetByPodFromFinalSnapshotWithContext(
	ctx context.Context,
	metaServer *metaserver.MetaServer,
	desired *model.DesiredView,
	snapshot *topology.CompleteSnapshot,
	expectedCPUSetByRel map[string]machine.CPUSet,
	deferredCPUSetMaps ...map[string]machine.CPUSet,
) (map[string]map[string]machine.CPUSet, error) {
	var deferredCPUSetByRel map[string]machine.CPUSet
	if len(deferredCPUSetMaps) > 0 {
		deferredCPUSetByRel = deferredCPUSetMaps[0]
	}
	return containerCPUSetByPodFromFinalSnapshotWithDeferredCleanup(
		ctx, metaServer, desired, snapshot, expectedCPUSetByRel, deferredCPUSetByRel, nil)
}

func containerCPUSetByPodFromFinalSnapshotWithDeferredCleanup(
	ctx context.Context,
	metaServer *metaserver.MetaServer,
	desired *model.DesiredView,
	snapshot *topology.CompleteSnapshot,
	expectedCPUSetByRel map[string]machine.CPUSet,
	deferredCPUSetByRel map[string]machine.CPUSet,
	deferredCleanupRels map[string]struct{},
) (map[string]map[string]machine.CPUSet, error) {
	out := map[string]map[string]machine.CPUSet{}
	if desired == nil || len(desired.ContainerCPUSetByPod) == 0 {
		return out, nil
	}
	if metaServer == nil {
		return nil, fmt.Errorf("meta server is required to prove container leaves from final snapshot")
	}
	for podUID, containers := range desired.ContainerCPUSetByPod {
		for containerName, desiredCPUs := range containers {
			if desiredCPUs.IsEmpty() {
				continue
			}
			rel, err := bulkheadutils.ResolveContainerRelPathWithContext(ctx, metaServer, podUID, containerName)
			if err != nil {
				if isContainerNotCreatedErr(err) {
					continue
				}
				return nil, fmt.Errorf("resolve final container leaf pod=%q container=%q: %w", podUID, containerName, err)
			}
			proof, ok := snapshot.TargetProofCPUs(rel, desiredCPUs)
			if !ok {
				if _, deferred := deferredCleanupRels[rel]; deferred {
					continue
				}
				if expectedCPUSetByRel != nil {
					expectedCPUSetByRel[rel] = desiredCPUs.Clone()
				}
				return nil, &topology.PlanStaleError{
					Rel: rel, Direction: topology.WriteDirection("publish"),
					Resource: "container_cpuset", Current: "<missing>", Target: desiredCPUs.String(),
					Err: fmt.Errorf("final snapshot misses container leaf for pod=%q container=%q", podUID, containerName),
				}
			}
			if !proof.Equals(desiredCPUs) {
				if _, deferred := deferredCleanupRels[rel]; deferred {
					if out[podUID] == nil {
						out[podUID] = map[string]machine.CPUSet{}
					}
					out[podUID][containerName] = proof.Clone()
					continue
				}
				if deferred, ok := deferredCPUSetByRel[rel]; ok &&
					deferred.Equals(desiredCPUs) && desiredCPUs.IsSubsetOf(proof) {
					if out[podUID] == nil {
						out[podUID] = map[string]machine.CPUSet{}
					}
					out[podUID][containerName] = proof.Clone()
					continue
				}
				if expectedCPUSetByRel != nil {
					expectedCPUSetByRel[rel] = desiredCPUs.Clone()
				}
				return nil, &topology.PlanStaleError{
					Rel: rel, Direction: topology.WriteDirection("publish"),
					Resource: "container_cpuset", Current: proof.String(), Target: desiredCPUs.String(),
					Err: fmt.Errorf("final snapshot container leaf does not match desired for pod=%q container=%q",
						podUID, containerName),
				}
			}
			if out[podUID] == nil {
				out[podUID] = map[string]machine.CPUSet{}
			}
			out[podUID][containerName] = proof.Clone()
		}
	}
	return out, nil
}

func (p *CPUSetTopologyPlugin) CPUSetAdjustmentDisabledHandler(ctx context.Context, in bulkheadapi.HandlerContext) error {
	p.pendingProtections = map[string]pendingPodProtection{}
	return p.resetCPUSetTopology(ctx, in)
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

	siblings, err := p.discoverBulkheadReclaimSiblings(ctx, in.DesiredView)
	if err != nil {
		return nil, fmt.Errorf("discover bulkhead reclaim siblings: %w", err)
	}

	var cpuDetails machine.CPUDetails
	if in.Topology != nil {
		cpuDetails = in.Topology.CPUDetails
	}
	specs, err := bulkheadutils.BuildTopologyNodeSpecsFromView(p.cfg, desiredCPUSetPartitionView(in.DesiredView), cpuDetails, siblings, relExists)
	if err != nil {
		return nil, fmt.Errorf("build disabled reset topology inputs: %w", err)
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

func (p *CPUSetTopologyPlugin) resetCPUSetTopology(ctx context.Context, in bulkheadapi.HandlerContext) error {
	target, err := p.disabledResetCPUSet(ctx, in)
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "reset_target_error")
		return err
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
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "dag_error")
		return err
	}
	if dag == nil {
		emitBulkheadPruneResult(in.Emitter, "success", 0, "")
		return nil
	}

	res, err := (topology.TopologyCoordinator{}).Converge(ctx, topology.CoordinatorInput{
		DAG:                 dag,
		Cgroup:              p.cgroup,
		Mode:                topology.ResetModeGuardWithGate(p.sharedModeGate()),
		ExpectedCPUSetByRel: expected,
		Budget:              topologyBudgetFromConfig(p.cfg.TopologyConvergenceBudget),
		DrainSelection:      topologyDrainSelectionFromConfig(p.cfg.TopologyDrainSelection),
	})
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", res.Applied, "dag_error")
		emitBulkheadTopologySummary(in.Emitter, "reset", res, err)
		return fmt.Errorf("apply disabled reset topology dag: %w", err)
	}
	emitBulkheadTopologySummary(in.Emitter, "reset", res, nil)
	if !res.Converged {
		general.InfofV(4, "cpuset_topology: disabled reset not fully converged, report=%+v", res.ConvergenceReport)
		emitBulkheadPruneResult(in.Emitter, "skipped", res.Applied, "reset_not_converged")
		return &disabledResetNotConvergedError{
			state:   res.State,
			applied: res.Applied,
			report:  res.ConvergenceReport,
		}
	}

	emitBulkheadPruneResult(in.Emitter, "success", res.Applied, "")
	return nil
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
	PodUID        string
	ContainerName string
	CPUs          machine.CPUSet
	Reason        string
}

// expectedCPUSetBuildResult separates resolvable container leaves (ExpectedByRel,
// written precisely) from admit-pending containers (PendingByPod, protected but
// not written).
type expectedCPUSetBuildResult struct {
	ExpectedByRel     map[string]machine.CPUSet
	DeferredLeafByRel map[string]machine.CPUSet
	PendingByPod      []pendingContainerCPUSet
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

// isContainerNotCreatedErr reports whether a ResolveContainerRelPath error means
// the pod or container is absent during the normal admission creation window.
// Cache synchronization, kubelet transport, and context errors must fail closed.
func isContainerNotCreatedErr(err error) bool {
	return errors.Is(err, metapod.ErrPodNotFound) ||
		errors.Is(err, metapod.ErrContainerNotFound)
}

func (p *CPUSetTopologyPlugin) buildExpectedCPUSetByRel(ctx context.Context, in bulkheadapi.HandlerContext) (*expectedCPUSetBuildResult, error) {
	if in.MetaServer == nil || in.DesiredView == nil || len(in.DesiredView.ContainerCPUSetByPod) == 0 {
		return &expectedCPUSetBuildResult{}, nil
	}
	out := &expectedCPUSetBuildResult{
		ExpectedByRel:     map[string]machine.CPUSet{},
		DeferredLeafByRel: map[string]machine.CPUSet{},
	}
	var errs []error
	for podUID, containers := range in.DesiredView.ContainerCPUSetByPod {
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
			rel, err := bulkheadutils.ResolveContainerRelPathWithContext(ctx, in.MetaServer, podUID, containerName)
			if err != nil {
				if isContainerNotCreatedErr(err) {
					// admit-safe pending: state has the allocation but the container
					// cgroup does not exist yet. Do NOT fail (that would reject pod
					// admit); record it so the writer keeps the parent a superset.
					general.InfofV(5, "bulkhead: container rel pending, protecting allocation, pod=%q container=%q cpuset=%s cpuset_size=%d err=%v",
						podUID, containerName, cpus.String(), cpus.Size(), err)
					out.PendingByPod = append(out.PendingByPod, pendingContainerCPUSet{
						PodUID: podUID, ContainerName: containerName, CPUs: cpus, Reason: err.Error(),
					})
					continue
				}
				// A real internal error (illegal rel, cgroup/metaserver failure):
				// block this round rather than apply a partial/wrong topology.
				errs = append(errs, fmt.Errorf("pod=%s container=%s cpuset=%s: %w",
					podUID, containerName, cpus.String(), err))
				continue
			}
			if rel == "" {
				errs = append(errs, fmt.Errorf("pod=%s container=%s cpuset=%s: empty relative cgroup path",
					podUID, containerName, cpus.String()))
				continue
			}
			if p.cfg.EnableAdmissionLeafDefer && in.Mode.OrFullDefault() == cpusetutil.CPUSetAdjustmentModeAdmission {
				current, readErr := p.cgroup.ReadCPUSet(ctx, rel)
				if readErr == nil && !current.Equals(cpus) && cpus.IsSubsetOf(current) &&
					current.Intersection(in.DesiredView.DesiredReclaimEffective).IsEmpty() {
					out.DeferredLeafByRel[rel] = cpus
					continue
				}
			}
			out.ExpectedByRel[rel] = cpus
		}
	}
	if len(errs) > 0 {
		return nil, apierrors.NewAggregate(errs)
	}
	return out, nil
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

func (p *CPUSetTopologyPlugin) drainSafeDeferredLeaves(ctx context.Context, view *model.DesiredView, dag *topology.TopoDAG) {
	if len(p.deferredLeafDrains) == 0 || view == nil || dag == nil {
		return
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

		done, err := p.tryDrainOneDeferredLeaf(ctx, view, dag, rel, drain.target)
		if err != nil {
			general.Warningf("bulkhead: deferred leaf drain skipped, rel=%q target=%s err=%v", rel, drain.target.String(), err)
			continue
		}
		if done {
			delete(p.deferredLeafDrains, rel)
		}
	}
}

func (p *CPUSetTopologyPlugin) tryDrainOneDeferredLeaf(ctx context.Context, view *model.DesiredView, dag *topology.TopoDAG, rel string, target machine.CPUSet) (bool, error) {
	if target.IsEmpty() {
		return true, nil
	}

	current, err := p.cgroup.ReadCPUSet(ctx, rel)
	if err != nil {
		if _, statErr := p.cgroup.StatDir(ctx, rel); statErr != nil {
			return true, nil
		}
		return false, fmt.Errorf("read leaf cpuset %q: %w", rel, err)
	}
	if current.Equals(target) {
		return true, nil
	}

	parentRel := path.Dir(rel)
	parent, err := p.cgroup.ReadCPUSet(ctx, parentRel)
	if err != nil {
		return false, fmt.Errorf("read parent cpuset %q: %w", parentRel, err)
	}
	if !target.IsSubsetOf(parent) {
		return false, fmt.Errorf("target %s is outside parent %q cpuset %s", target.String(), parentRel, parent.String())
	}
	if !target.Intersection(view.DesiredReclaimEffective).IsEmpty() {
		return false, fmt.Errorf("target %s overlaps desired reclaim %s", target.String(), view.DesiredReclaimEffective.String())
	}

	actualReclaim, err := p.readActualReclaimUnion(ctx, dag)
	if err != nil {
		return false, err
	}
	if !target.Intersection(actualReclaim).IsEmpty() {
		return false, fmt.Errorf("target %s overlaps actual reclaim %s", target.String(), actualReclaim.String())
	}

	if err := p.cgroup.ApplyCPUSet(ctx, rel, &cgcommon.CPUSetData{CPUs: target.String(), WriteEmptyCPUs: target.IsEmpty()}); err != nil {
		return false, fmt.Errorf("write deferred leaf cpuset %q target=%s: %w", rel, target.String(), err)
	}
	after, err := p.cgroup.ReadCPUSet(ctx, rel)
	if err != nil {
		return false, fmt.Errorf("verify deferred leaf cpuset %q: %w", rel, err)
	}
	if !after.Equals(target) {
		return false, fmt.Errorf("verify deferred leaf cpuset %q got=%s want=%s", rel, after.String(), target.String())
	}

	general.Infof("bulkhead: deferred leaf exact drained, rel=%q target=%s", rel, target.String())
	return true, nil
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

func unionCPUSetByRel(byRel map[string]machine.CPUSet) machine.CPUSet {
	union := machine.NewCPUSet()
	for _, cpus := range byRel {
		union = union.Union(cpus)
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
	metricBulkheadPruneResult                   = "bulkhead_prune_result"
	metricBulkheadTopologyRoundTotal            = "bulkhead_topology_round_total"
	metricBulkheadTopologyRoundsPerApply        = "bulkhead_topology_rounds_per_apply"
	metricBulkheadTopologyBudgetExhaustedTotal  = "bulkhead_topology_budget_exhausted_total"
	metricBulkheadTopologyScanNodes             = "bulkhead_topology_scan_nodes"
	metricBulkheadTopologyScanDepth             = "bulkhead_topology_scan_depth"
	metricBulkheadTopologyDrainBatch            = "bulkhead_topology_drain_batch"
	metricBulkheadTopologyIdentityChangedTotal  = "bulkhead_topology_identity_changed_total"
	metricBulkheadTopologyHandoffLatencySeconds = "bulkhead_topology_handoff_latency_seconds"
)

var (
	allowedTopologyMetricPhases      = map[string]struct{}{"normal": {}, "reset": {}}
	allowedTopologyMetricStatuses    = map[string]struct{}{"progress": {}, "stale": {}, "blocked": {}, "converged": {}, "error": {}}
	allowedTopologyMetricReasons     = map[string]struct{}{"none": {}, "stale": {}, "blocked": {}, "budget": {}, "identity_changed": {}, "external_write": {}, "invalid": {}}
	allowedTopologyMetricDomainRoles = map[string]struct{}{"primary": {}, "reclaim": {}, "reclaim_numa": {}, "dynamic": {}, "unknown": {}}
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
	_ = emitter.StoreFloat64(metricBulkheadTopologyHandoffLatencySeconds, 0, metrics.MetricTypeNameRaw,
		metrics.MetricTag{Key: "phase", Val: phase},
		metrics.MetricTag{Key: "status", Val: status},
	)
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

func emitBulkheadPruneResult(emitter metrics.MetricEmitter, status string, activeRelsCount int, reason string) {
	if emitter == nil {
		return
	}
	_ = emitter.StoreInt64(metricBulkheadPruneResult, 1, metrics.MetricTypeNameCount,
		metrics.MetricTag{Key: "status", Val: status},
		metrics.MetricTag{Key: "active_rels_count", Val: strconv.Itoa(activeRelsCount)},
		metrics.MetricTag{Key: "reason", Val: reason},
	)
}
