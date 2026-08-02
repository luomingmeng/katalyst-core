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
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	cgroupclient "github.com/kubewharf/katalyst-core/pkg/util/cgroup/client"
	cgcommon "github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const CPUSetTopologyPluginName = "cpuset_topology"

const defaultPendingPodProtectionTTL = 10 * time.Second

var _ bulkheadapi.Plugin = (*CPUSetTopologyPlugin)(nil)
var _ bulkheadapi.TopologyPlugin = (*CPUSetTopologyPlugin)(nil)

type CPUSetTopologyPlugin struct {
	cfg                bulkheadconfig.BulkheadConfiguration
	cgroup             cgroupclient.CgroupClient
	now                func() time.Time
	pendingProtections map[string]pendingPodProtection
	modeGateMu         sync.Mutex
	modeGate           *topology.ModeGate
}

type pendingPodProtection struct {
	rel          string
	current      machine.CPUSet
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
) (bulkheadapi.DAGApplyResult, error) {
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

	err := p.CPUSetAdjustmentHandler(ctx, in)
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
	return bulkheadapi.DAGApplyResult{
		FullyConverged:       published.Converged,
		FinalSnapshotCurrent: published.FinalSnapshotCurrent,
		AppliedView:          published.AppliedView.DeepCopy(),
	}, nil
}

func dagApplyResultFromConvergence(result topology.ConvergenceResult) bulkheadapi.DAGApplyResult {
	out := bulkheadapi.DAGApplyResult{
		Attempted:            result.Attempted,
		Applied:              result.Applied,
		Skipped:              result.Skipped,
		Failed:               result.Failed,
		Deferred:             result.Deferred,
		FullyConverged:       result.Converged,
		FinalSnapshotCurrent: result.FinalSnapshotCurrent,
		ConvergenceReport:    result.ConvergenceReport,
	}
	return out
}

func (p *CPUSetTopologyPlugin) CPUSetAdjustmentHandler(ctx context.Context, in bulkheadapi.HandlerContext) error {
	if in.DesiredView == nil {
		return nil
	}
	relExists := func(rel string) error {
		_, err := p.cgroup.StatDir(ctx, rel)
		return err
	}
	expectedRes, err := p.buildExpectedCPUSetByRel(ctx, in)
	if err != nil {
		// Only non-pending resolve failures (illegal rel, cgroup/metaserver
		// internal error) reach here. Pending containers (admit window, no
		// container id yet) are classified as protected-pending and do NOT
		// produce an error, so a normal new-pod admit is never rejected.
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "container_error")
		return fmt.Errorf("build expected container cpuset: %w", err)
	}
	protectedByRel := p.pendingProtectedCPUSetByRel(ctx, expectedRes.PendingByPod)
	if protected := unionCPUSetByRel(protectedByRel); !protected.IsEmpty() {
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
	}
	siblings, err := p.discoverBulkheadReclaimSiblings(ctx, in.DesiredView)
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "discover_error")
		return fmt.Errorf("discover bulkhead reclaim siblings: %w", err)
	}
	var cpuDetails machine.CPUDetails
	if in.Topology != nil {
		cpuDetails = in.Topology.CPUDetails
	}
	specs, err := bulkheadutils.BuildTopologyNodeSpecsFromView(p.cfg, desiredCPUSetPartitionView(in.DesiredView), cpuDetails, siblings, relExists)
	if err != nil {
		return fmt.Errorf("build bulkhead topology inputs: %w", err)
	}
	dag, err := topology.BuildDAG(specs)
	if err != nil {
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "dag_error")
		return fmt.Errorf("build bulkhead topology dag: %w", err)
	}
	general.InfofV(5, "cpuset_topology: apply start specs=%d siblings=%d expected_leaf_count=%d pending_count=%d protected_pending=%s protected_rel_count=%d",
		len(specs), len(siblings), len(expectedRes.ExpectedByRel), len(expectedRes.PendingByPod),
		expectedRes.PendingCPUSetUnion().String(), len(protectedByRel))
	reservedCPUSet := in.DesiredView.Reserve
	// Normal adjustment passes the topology explicitly so TopologyCoordinator can
	// derive its allowed CPUs from this round's machine view. Any apply error is
	// returned to the bulkhead manager through this handler; this plugin does
	// not attempt a local retry or partial recovery.
	res, err := (topology.TopologyCoordinator{}).Converge(ctx, topology.CoordinatorInput{
		DAG:                    dag,
		Cgroup:                 p.cgroup,
		Mode:                   topology.NormalModeGuardWithGate(p.sharedModeGate()),
		Budget:                 topologyBudgetFromConfig(p.cfg.TopologyConvergenceBudget),
		DrainSelection:         topologyDrainSelectionFromConfig(p.cfg.TopologyDrainSelection),
		CPUDetails:             cpuDetails,
		ReservedCPUSet:         reservedCPUSet,
		ExpectedCPUSetByRel:    expectedRes.ExpectedByRel,
		KubeManagedRelPrefix:   p.cfg.BulkheadPrimaryRelPath,
		ProtectedPendingCPUSet: expectedRes.PendingCPUSetUnion(),
		ProtectedCPUSetByRel:   protectedByRel,
		PublishFinalSnapshot: func(snapshot *topology.CompleteSnapshot) error {
			appliedView, err := appliedViewFromFinalSnapshot(in.MetaServer, in.DesiredView, dag, snapshot)
			if err != nil {
				return fmt.Errorf("derive applied view from final topology snapshot: %w", err)
			}
			if in.ReportTopologyResult != nil {
				in.ReportTopologyResult(bulkheadapi.TopologyResult{
					Converged:            true,
					FinalSnapshotCurrent: true,
					AppliedView:          appliedView,
				})
			}
			return nil
		},
	})
	if err != nil {
		emitBulkheadTopologySummary(in.Emitter, "normal", res, err)
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, "dag_error")
		return fmt.Errorf("apply bulkhead topology dag: %w", err)
	}
	emitBulkheadTopologySummary(in.Emitter, "normal", res, nil)
	if !res.Converged {
		general.InfofV(4, "cpuset_topology: apply not fully converged, deferred=%d state=%s report=%+v", res.Deferred, res.State, res.ConvergenceReport)
		reason := "not_converged"
		if res.Deferred > 0 {
			reason = "deferred_convergence"
		}
		emitBulkheadPruneResult(in.Emitter, "skipped", 0, reason)
		return &topologyApplyNonConvergedError{result: res}
	}

	activeRels := bulkheadutils.CollectActiveRels(p.cfg, desiredCPUSetPartitionView(in.DesiredView), in.MetaServer, siblings, relExists)
	p.cgroup.Prune(activeRels)
	emitBulkheadPruneResult(in.Emitter, "success", len(activeRels), "")
	return nil
}

func appliedViewFromFinalSnapshot(
	metaServer *metaserver.MetaServer,
	desired *model.DesiredView,
	dag *topology.TopoDAG,
	snapshot *topology.CompleteSnapshot,
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
	containerCPUSetByPod, err := containerCPUSetByPodFromFinalSnapshot(metaServer, desired, snapshot)
	if err != nil {
		return nil, err
	}
	applied.ContainerCPUSetByPod = containerCPUSetByPod
	return applied, nil
}

func containerCPUSetByPodFromFinalSnapshot(
	metaServer *metaserver.MetaServer,
	desired *model.DesiredView,
	snapshot *topology.CompleteSnapshot,
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
			rel, err := bulkheadutils.ResolveContainerRelPath(metaServer, podUID, containerName)
			if err != nil {
				if isContainerNotCreatedErr(err) {
					continue
				}
				return nil, fmt.Errorf("resolve final container leaf pod=%q container=%q: %w", podUID, containerName, err)
			}
			proof, ok := snapshot.TargetProofCPUs(rel, desiredCPUs)
			if !ok {
				return nil, fmt.Errorf("final snapshot misses container leaf %q for pod=%q container=%q", rel, podUID, containerName)
			}
			if !proof.Equals(desiredCPUs) {
				return nil, fmt.Errorf(
					"final snapshot container leaf %q cpuset=%s does not match desired=%s for pod=%q container=%q",
					rel, proof.String(), desiredCPUs.String(), podUID, containerName,
				)
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
	ExpectedByRel map[string]machine.CPUSet
	PendingByPod  []pendingContainerCPUSet
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
// the container simply has not been created yet (admit-safe pending), as opposed
// to a real internal error. A failure while resolving the container ID means
// kubelet/containerd has not created the container yet - the normal pod admit
// window - so it must NOT fail the round. A failure after the ID is known (for
// example resolving the relative cgroup path) is a real problem and is surfaced.
func isContainerNotCreatedErr(err error) bool {
	if err == nil {
		return false
	}
	var resolveErr *bulkheadutils.ContainerRelPathResolveError
	if errors.As(err, &resolveErr) {
		return resolveErr.Stage == bulkheadutils.ContainerRelPathResolveStageContainerID
	}
	// Backward-compatible fallback for callers/tests that may still wrap errors
	// with the old text-only context. Keep this intentionally narrow: a generic
	// "not found" from cgroup path resolution must stay fail-closed.
	return strings.Contains(err.Error(), "resolve container id:")
}

func (p *CPUSetTopologyPlugin) buildExpectedCPUSetByRel(_ context.Context, in bulkheadapi.HandlerContext) (*expectedCPUSetBuildResult, error) {
	if in.MetaServer == nil || in.DesiredView == nil || len(in.DesiredView.ContainerCPUSetByPod) == 0 {
		return &expectedCPUSetBuildResult{}, nil
	}
	out := &expectedCPUSetBuildResult{ExpectedByRel: map[string]machine.CPUSet{}}
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
			rel, err := bulkheadutils.ResolveContainerRelPath(in.MetaServer, podUID, containerName)
			if err != nil {
				if isContainerNotCreatedErr(err) {
					// admit-safe pending: state has the allocation but the container
					// cgroup does not exist yet. Do NOT fail (that would reject pod
					// admit); record it so the writer keeps the parent a superset.
					general.InfofV(5, "bulkhead: container rel pending, protecting allocation, pod=%q container=%q cpuset=%s err=%v",
						podUID, containerName, cpus.String(), err)
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
			out.ExpectedByRel[rel] = cpus
		}
	}
	if len(errs) > 0 {
		return nil, apierrors.NewAggregate(errs)
	}
	return out, nil
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
	out := make(map[string]machine.CPUSet, len(pendingByPod))
	active := map[string]struct{}{}
	seen := map[string]struct{}{}
	for _, pending := range pendingByPod {
		if _, ok := seen[pending.PodUID]; ok {
			continue
		}
		seen[pending.PodUID] = struct{}{}
		active[pending.PodUID] = struct{}{}
		protection, ok := p.pendingProtections[pending.PodUID]
		if !ok {
			protection = pendingPodProtection{
				protectUntil: now.Add(defaultPendingPodProtectionTTL),
			}
		}
		if now.After(protection.protectUntil) {
			delete(p.pendingProtections, pending.PodUID)
			continue
		}
		rel, err := cgcommon.GetPodRelativeCgroupPath(pending.PodUID)
		if err != nil {
			p.pendingProtections[pending.PodUID] = protection
			continue
		}
		rel = strings.Trim(rel, "/")
		if rel == "" {
			p.pendingProtections[pending.PodUID] = protection
			continue
		}
		current, err := p.cgroup.ReadCPUSet(ctx, rel)
		if err != nil || current.IsEmpty() {
			general.InfofV(6, "bulkhead: pending protected rel skipped, pod=%q container=%q rel=%q allocation=%s current=%s err=%v reason=missing_or_empty_pod_cgroup",
				pending.PodUID, pending.ContainerName, rel, pending.CPUs.String(), current.String(), err)
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
		general.InfofV(5, "bulkhead: pending protected rel, pod=%q container=%q rel=%q allocation=%s current=%s protected=%s overlap=%s dropped_extra=%s protect_until=%s",
			pending.PodUID, pending.ContainerName, rel, pending.CPUs.String(), current.String(),
			protected.String(), current.Intersection(pending.CPUs).String(), current.Difference(pending.CPUs).String(),
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
	metricBulkheadTopologyExternalWriteTotal    = "bulkhead_topology_external_controlled_write_total"
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
