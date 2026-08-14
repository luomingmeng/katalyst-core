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
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology"
	cpustate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	metapod "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	cgroupclient "github.com/kubewharf/katalyst-core/pkg/util/cgroup/client"
	cgcommon "github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestTopologyResultAndDAGApplyResultPreserveExecutionStatistics(t *testing.T) {
	convergence := topology.ConvergenceResult{
		Attempted:            11,
		Applied:              7,
		Skipped:              2,
		Failed:               1,
		Deferred:             1,
		ParentSafe:           true,
		DeferredLeafCount:    3,
		DeferredCPUCount:     5,
		FinalSnapshotCurrent: true,
		ConvergenceReport: topology.ConvergenceReport{
			FullyConverged: false,
		},
	}
	applied := &model.AppliedView{}

	topologyResult := topologyResultFromFinalConvergence(convergence, applied)
	if topologyResult.Attempted != 11 || topologyResult.Applied != 7 ||
		topologyResult.Skipped != 2 || topologyResult.Failed != 1 ||
		topologyResult.Deferred != 1 || topologyResult.DeferredCPUCount != 5 {
		t.Fatalf("topology result lost execution statistics: %+v", topologyResult)
	}

	dagResult := dagApplyResultFromTopologyResult(topologyResult)
	if dagResult.Attempted != 11 || dagResult.Applied != 7 ||
		dagResult.Skipped != 2 || dagResult.Failed != 1 ||
		dagResult.Deferred != 1 || dagResult.DeferredCPUCount != 5 ||
		dagResult.ConvergenceReport.FullyConverged {
		t.Fatalf("DAG apply result lost execution statistics: %+v", dagResult)
	}
}

func TestHandleDeferredLeafRetryByAdjustmentMode(t *testing.T) {
	t.Parallel()

	scheduled := 0
	schedule := func(cpusetutil.CPUSetAdjustmentRetryReason) {
		scheduled++
	}
	if err := handleDeferredLeafRetry(cpusetutil.CPUSetAdjustmentModeAdmission, schedule); err != nil {
		t.Fatalf("admission deferred leaf returned error: %v", err)
	}
	if scheduled != 1 {
		t.Fatalf("admission scheduled retries = %d, want 1", scheduled)
	}

	scheduled = 0
	if err := handleDeferredLeafRetry(cpusetutil.CPUSetAdjustmentModeRetry, schedule); err == nil {
		t.Fatal("retry deferred leaf returned nil, want pending error")
	}
	if scheduled != 0 {
		t.Fatalf("retry mode self-scheduled %d retries, want 0", scheduled)
	}
}

func TestAppliedViewFromFinalSnapshotUsesPerRelV2TargetProof(t *testing.T) {
	dag, err := topology.BuildDAG([]topology.NodeSpec{
		{
			Rel: "primary", Role: topology.TopoNodeRolePrimary, Domain: topology.DomainPrimary,
			CPUs: machine.NewCPUSet(0, 1), ControlledRoot: true, TrustAnchor: true,
		},
		{
			Rel: "reclaim", Role: topology.TopoNodeRoleReclaim, Domain: topology.DomainReclaim,
			CPUs: machine.NewCPUSet(2, 3), ControlledRoot: true, TrustAnchor: true,
		},
		{
			Rel: "reclaim/numa-0", ParentRel: "reclaim", Role: topology.TopoNodeRoleReclaimNUMABucket,
			Domain: topology.DomainReclaim, CPUs: machine.NewCPUSet(), TrustAnchor: true,
			Constraint: topology.TopologyConstraint{
				CPUUpperBound: machine.NewCPUSet(2, 3),
				Scope:         topology.TopologyScopeNUMANode,
			},
			Metadata: map[string]string{"numa": strconv.Itoa(0)},
		},
	})
	if err != nil {
		t.Fatalf("BuildDAG() error = %v", err)
	}
	snapshot := &topology.CompleteSnapshot{
		Capabilities: topology.HierarchyCapabilities{
			StableIdentity: true, EmptyConfiguredCPUSet: true, EffectiveCPUSet: true,
		},
		Entries: map[string]topology.EntryState{
			"primary": {
				Rel: "primary", Identity: topology.CgroupIdentity{Device: 7, Inode: 10},
				CPUs: machine.NewCPUSet(0, 1), ConfiguredCPUs: machine.NewCPUSet(0, 1),
			},
			"reclaim": {
				Rel: "reclaim", Identity: topology.CgroupIdentity{Device: 7, Inode: 11},
				CPUs: machine.NewCPUSet(2, 3), ConfiguredCPUs: machine.NewCPUSet(2, 3),
			},
			"reclaim/numa-0": {
				Rel: "reclaim/numa-0", Identity: topology.CgroupIdentity{Device: 7, Inode: 12},
				CPUs: machine.NewCPUSet(2, 3), ConfiguredCPUs: machine.NewCPUSet(),
			},
		},
	}

	applied, err := appliedViewFromFinalSnapshot(nil, model.NewDesiredView(), dag, snapshot)
	if err != nil {
		t.Fatalf("appliedViewFromFinalSnapshot() error = %v", err)
	}
	if got := applied.CPUSetByRel["reclaim/numa-0"]; !got.IsEmpty() {
		t.Fatalf("empty v2 target published inherited effective CPUs %s", got.String())
	}
	if got := applied.CPUSetByRel["reclaim"]; !got.Equals(machine.NewCPUSet(2, 3)) {
		t.Fatalf("non-empty v2 target published %s, want effective 2-3", got.String())
	}
	if got := applied.RelProofByRel["reclaim"]; got.Device != 7 || got.Inode != 11 ||
		!got.CPUSet.Equals(machine.NewCPUSet(2, 3)) {
		t.Fatalf("reclaim rel proof = %+v, want device=7 inode=11 cpuset=2-3", got)
	}
	if got := applied.RelProofByRel["reclaim/numa-0"]; got.Device != 7 || got.Inode != 12 ||
		!got.CPUSet.IsEmpty() {
		t.Fatalf("empty target rel proof = %+v, want device=7 inode=12 empty cpuset", got)
	}
	if got := applied.ReclaimEffectivePerNUMA[0]; !got.IsEmpty() {
		t.Fatalf("empty v2 NUMA target published inherited effective CPUs %s", got.String())
	}
}

func TestAppliedViewFromFinalSnapshotRejectsNUMATargetProofOutsideUpperBound(t *testing.T) {
	dag, err := topology.BuildDAG([]topology.NodeSpec{
		{
			Rel: "reclaim", Role: topology.TopoNodeRoleReclaim, Domain: topology.DomainReclaim,
			CPUs: machine.NewCPUSet(0, 1), ControlledRoot: true, TrustAnchor: true,
		},
		{
			Rel: "reclaim/numa-0", ParentRel: "reclaim", Role: topology.TopoNodeRoleReclaimNUMABucket,
			Domain: topology.DomainReclaim, CPUs: machine.NewCPUSet(0, 1), TrustAnchor: true,
			Constraint: topology.TopologyConstraint{
				CPUUpperBound: machine.NewCPUSet(0),
				Scope:         topology.TopologyScopeNUMANode,
			},
			Metadata: map[string]string{"numa": "0"},
		},
	})
	if err != nil {
		t.Fatalf("BuildDAG() error = %v", err)
	}
	snapshot := &topology.CompleteSnapshot{
		Entries: map[string]topology.EntryState{
			"reclaim":        {Rel: "reclaim", CPUs: machine.NewCPUSet(0, 1)},
			"reclaim/numa-0": {Rel: "reclaim/numa-0", CPUs: machine.NewCPUSet(0, 1)},
		},
	}

	_, err = appliedViewFromFinalSnapshot(nil, model.NewDesiredView(), dag, snapshot)
	if err == nil || !strings.Contains(err.Error(), "CPU upper bound") {
		t.Fatalf("appliedViewFromFinalSnapshot() error = %v, want NUMA CPU upper-bound rejection", err)
	}
}

type capturedMetric struct {
	key  string
	tags []metrics.MetricTag
}

type captureMetricEmitter struct {
	metrics []capturedMetric
}

func TestTopologyDrainSelectionFromConfigPassesRatio(t *testing.T) {
	t.Parallel()

	got := topologyDrainSelectionFromConfig(bulkheadconfig.DrainSelectionPolicy{
		MaxCPUsDrainRatio:         0.25,
		GroupByNUMA:               true,
		RequirePairedSwapProgress: true,
	})
	if got.MaxCPUsDrainRatio != 0.25 || !got.GroupByNUMA || !got.RequirePairedSwapProgress {
		t.Fatalf("topology drain selection = %+v, want ratio and existing policy fields preserved", got)
	}
}

func (e *captureMetricEmitter) StoreInt64(key string, _ int64, _ metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	e.metrics = append(e.metrics, capturedMetric{key: key, tags: append([]metrics.MetricTag(nil), tags...)})
	return nil
}

func (e *captureMetricEmitter) StoreFloat64(key string, _ float64, _ metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	e.metrics = append(e.metrics, capturedMetric{key: key, tags: append([]metrics.MetricTag(nil), tags...)})
	return nil
}

func (e *captureMetricEmitter) WithTags(string, ...metrics.MetricTag) metrics.MetricEmitter {
	return e
}

func (e *captureMetricEmitter) Run(context.Context) {}

func TestCPUSetTopologyPluginIsConfiguredReclaimNUMARel(t *testing.T) {
	t.Parallel()

	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadReclaimNumaPrefixes: []string{"reclaimed/reclaimed-", "/foo/bar-"},
		},
	}

	for _, rel := range []string{
		"reclaimed/reclaimed-0",
		"/reclaimed/reclaimed-1",
		"foo/bar-2",
	} {
		if !p.isConfiguredReclaimNUMARel(rel) {
			t.Fatalf("expected %q to be recognized as reclaim NUMA rel", rel)
		}
	}

	for _, rel := range []string{
		"reclaimed/reclaimed",
		"reclaimed/reclaimed-a",
		"reclaimed/reclaimed-0-extra",
		"foo/bar",
		"other/bar-0",
	} {
		if p.isConfiguredReclaimNUMARel(rel) {
			t.Fatalf("expected %q not to be recognized as reclaim NUMA rel", rel)
		}
	}
}

func TestTopologyMetricLabelsAreBoundedEnums(t *testing.T) {
	t.Parallel()

	emitter := &captureMetricEmitter{}
	emitBulkheadTopologySummary(emitter, "normal", topology.ConvergenceResult{
		Converged: true,
		State:     topology.ConvergenceStateConverged,
		Rounds: []topology.RoundOutcome{{
			Status: topology.RoundStatusProgress,
			Cost:   topology.BudgetUsage{Nodes: 10, MaxDepth: 2},
			Witnesses: []topology.ReleaseWitness{{
				CPUs: machine.NewCPUSet(0, 1),
			}},
		}},
	}, nil)
	emitBulkheadTopologySummary(emitter, "normal", topology.ConvergenceResult{}, topology.ErrNodeBudgetExceeded)

	if len(emitter.metrics) == 0 {
		t.Fatal("expected topology metrics")
	}
	for _, item := range emitter.metrics {
		for _, tag := range item.tags {
			switch tag.Key {
			case "phase":
				assertMetricEnum(t, item.key, tag, allowedTopologyMetricPhases)
			case "status":
				assertMetricEnum(t, item.key, tag, allowedTopologyMetricStatuses)
			case "reason":
				assertMetricEnum(t, item.key, tag, allowedTopologyMetricReasons)
			case "domain_role":
				assertMetricEnum(t, item.key, tag, allowedTopologyMetricDomainRoles)
			case "kind":
				if tag.Val == "" || strings.Contains(tag.Val, "/") {
					t.Fatalf("metric %s has unbounded kind label: %+v", item.key, tag)
				}
			}
		}
	}
}

func assertMetricEnum(t *testing.T, key string, tag metrics.MetricTag, allowed map[string]struct{}) {
	t.Helper()
	if _, ok := allowed[tag.Val]; !ok {
		t.Fatalf("metric %s has unbounded %s label %q", key, tag.Key, tag.Val)
	}
}

type fakeCgroupClient struct {
	cgroupclient.FakeCgroupClient

	version           cgroupclient.CgroupVersion
	existing          map[string]bool
	cpus              map[string]machine.CPUSet
	mems              map[string]string
	children          map[string][]string
	statErrors        map[string]error
	writes            map[string]string
	applyCounts       map[string]int
	cpusetWrites      map[string]cgcommon.CPUSetData
	pruned            map[string]struct{}
	schedLoadBalance  map[string]bool
	partitionWrites   map[string]cgcommon.CPUSetPartitionFlag
	partitionErrByRel map[string]error
	listErr           error
	listChildrenHook  func(context.Context, string) ([]string, error)
	afterApply        func(rel string, data *cgcommon.CPUSetData)
	readOverride      func(rel string) (machine.CPUSet, bool)
	readErrByRel      map[string]error
}

type fakeSnapshotDriver struct {
	cg *fakeCgroupClient
}

func (f *fakeCgroupClient) SnapshotDriver() topology.HierarchyDriver {
	return &fakeSnapshotDriver{cg: f}
}

func (d *fakeSnapshotDriver) Close() error { return nil }

func (d *fakeSnapshotDriver) Roots(context.Context) ([]topology.RootRef, error) {
	return nil, nil
}

func (d *fakeSnapshotDriver) StatIdentity(_ context.Context, rel string) (topology.CgroupIdentity, error) {
	return pluginFakeIdentity(rel), nil
}

func (d *fakeSnapshotDriver) ReadEntry(ctx context.Context, rel string) (topology.EntryState, error) {
	cpus, err := d.cg.ReadCPUSet(ctx, rel)
	if err != nil {
		return topology.EntryState{}, err
	}
	mems := d.cg.mems[rel]
	if mems == "" {
		mems = "0"
	}
	return topology.EntryState{Rel: rel, Identity: pluginFakeIdentity(rel), CPUs: cpus, Mems: mems}, nil
}

func (d *fakeSnapshotDriver) ListChildren(ctx context.Context, rel string) ([]topology.ChildRef, error) {
	names, err := d.cg.ListChildren(ctx, rel)
	if err != nil {
		return nil, err
	}
	children := make([]topology.ChildRef, 0, len(names))
	for _, name := range names {
		children = append(children, topology.ChildRef{Name: name, Identity: pluginFakeIdentity(strings.Trim(rel, "/") + "/" + name)})
	}
	return children, nil
}

func (d *fakeSnapshotDriver) WriteCPUs(ctx context.Context, rel string, expected topology.CgroupIdentity, cpus machine.CPUSet) error {
	if pluginFakeIdentity(rel) != expected {
		return topology.ErrCgroupIdentityChanged
	}
	return d.cg.ApplyCPUSet(ctx, rel, &cgcommon.CPUSetData{
		CPUs: cpus.String(), WriteEmptyCPUs: cpus.IsEmpty(),
	})
}

func (d *fakeSnapshotDriver) WriteMems(ctx context.Context, rel string, expected topology.CgroupIdentity, mems string) error {
	if pluginFakeIdentity(rel) != expected {
		return topology.ErrCgroupIdentityChanged
	}
	return d.cg.ApplyCPUSet(ctx, rel, &cgcommon.CPUSetData{
		Mems: mems, WriteEmptyMems: mems == "",
	})
}

func (d *fakeSnapshotDriver) Classify(error, topology.HierarchyOperation) topology.HierarchyErrorClass {
	return topology.HierarchyErrorInvalid
}

func (d *fakeSnapshotDriver) Capabilities() topology.HierarchyCapabilities {
	return topology.HierarchyCapabilities{StableIdentity: true, KernelParentContainment: true}
}

func pluginFakeIdentity(rel string) topology.CgroupIdentity {
	var inode uint64 = 1
	for _, value := range []byte(rel) {
		inode = inode*131 + uint64(value)
	}
	return topology.CgroupIdentity{Device: 1, Inode: inode}
}

func (f *fakeCgroupClient) StatDir(_ context.Context, rel string) (time.Time, error) {
	if err := f.statErrors[rel]; err != nil {
		return time.Time{}, err
	}
	if f.existing[rel] {
		return time.Time{}, nil
	}
	return time.Time{}, errors.New("missing")
}

func (f *fakeCgroupClient) Version(context.Context) cgroupclient.CgroupVersion {
	if f.version != "" {
		return f.version
	}
	return cgroupclient.CgroupVersionV1
}

func (f *fakeCgroupClient) ReadCPUSet(_ context.Context, rel string) (machine.CPUSet, error) {
	if err := f.readErrByRel[rel]; err != nil {
		return machine.NewCPUSet(), err
	}
	if f.readOverride != nil {
		if cpus, ok := f.readOverride(rel); ok {
			return cpus.Clone(), nil
		}
	}
	if cpus, ok := f.cpus[rel]; ok {
		return cpus.Clone(), nil
	}
	return machine.NewCPUSet(), nil
}

func TestPendingProtectedCPUSetByRelAggregatesAllContainersByPod(t *testing.T) {
	t.Parallel()

	const (
		podUID = "pending-multi-container-pod"
		podRel = "kubepods/burstable/pod-pending-multi-container"
	)
	now := time.Date(2026, time.August, 5, 10, 0, 0, 0, time.UTC)
	p := &CPUSetTopologyPlugin{
		cgroup: &fakeCgroupClient{cpus: map[string]machine.CPUSet{
			podRel: machine.NewCPUSet(0, 1, 2, 3),
		}},
		now: func() time.Time { return now },
		pendingProtections: map[string]pendingPodProtection{
			podUID: {
				rel:          podRel,
				protectUntil: now.Add(defaultPendingPodProtectionTTL),
			},
		},
	}

	got := p.pendingProtectedCPUSetByRel(context.Background(), []pendingContainerCPUSet{
		{PodUID: podUID, ContainerName: "main", CPUs: machine.NewCPUSet(0, 1)},
		{PodUID: podUID, ContainerName: "sidecar", CPUs: machine.NewCPUSet(2, 3)},
	})
	if protected := got[podRel]; !protected.Equals(machine.NewCPUSet(0, 1, 2, 3)) {
		t.Fatalf("protected cpuset = %s, want union 0-3", protected.String())
	}
}

func TestPendingProtectedCPUSetByRelRenewsExpiredActiveProtection(t *testing.T) {
	t.Parallel()

	const (
		podUID = "pending-after-ttl-pod"
		podRel = "kubepods/burstable/pod-pending-after-ttl"
	)
	now := time.Date(2026, time.August, 5, 10, 0, 0, 0, time.UTC)
	p := &CPUSetTopologyPlugin{
		cgroup: &fakeCgroupClient{cpus: map[string]machine.CPUSet{
			podRel: machine.NewCPUSet(4, 5),
		}},
		now: func() time.Time { return now },
		pendingProtections: map[string]pendingPodProtection{
			podUID: {
				rel:          podRel,
				protectUntil: now,
			},
		},
	}

	got := p.pendingProtectedCPUSetByRel(context.Background(), []pendingContainerCPUSet{
		{PodUID: podUID, ContainerName: "main", CPUs: machine.NewCPUSet(4, 5)},
	})
	if protected := got[podRel]; !protected.Equals(machine.NewCPUSet(4, 5)) {
		t.Fatalf("protected cpuset after TTL = %s, want 4-5", protected.String())
	}
	protection, ok := p.pendingProtections[podUID]
	if !ok {
		t.Fatal("active pending pod protection was removed after TTL")
	}
	if want := now.Add(defaultPendingPodProtectionTTL); !protection.protectUntil.Equal(want) {
		t.Fatalf("renewed protectUntil = %s, want %s", protection.protectUntil, want)
	}
}

func TestPendingProtectedCPUSetByRelClearsPodNoLongerPending(t *testing.T) {
	t.Parallel()

	const podUID = "completed-pending-pod"
	p := &CPUSetTopologyPlugin{
		pendingProtections: map[string]pendingPodProtection{
			podUID: {rel: "kubepods/burstable/pod-completed"},
		},
	}

	got := p.pendingProtectedCPUSetByRel(context.Background(), nil)
	if len(got) != 0 {
		t.Fatalf("protected cpus = %#v, want none", got)
	}
	if len(p.pendingProtections) != 0 {
		t.Fatalf("pending protections = %#v, want cleared", p.pendingProtections)
	}
}

func TestDeferredLeafDrainWritesSafeLeafDespiteGlobalMismatch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 13, 30, 0, 0, time.UTC)
	const rel = "kubepods/pod-a/container-a"
	cg := &fakeCgroupClient{cpus: map[string]machine.CPUSet{
		"kubepods":                machine.NewCPUSet(0, 1, 2, 3),
		"kubesandbox":             machine.NewCPUSet(4, 5, 6, 7),
		"kubesandbox/reclaimed-0": machine.NewCPUSet(4, 5),
		"system":                  machine.NewCPUSet(0, 1, 2, 3, 4, 5), // unrelated global mismatch
		"kubepods/pod-a":          machine.NewCPUSet(0, 1, 2, 3),
		rel:                       machine.NewCPUSet(0, 1, 2, 3),
	}}
	p := &CPUSetTopologyPlugin{
		cgroup: cg,
		now:    func() time.Time { return now },
		deferredLeafDrains: map[string]deferredLeafDrain{
			rel: {
				target:       machine.NewCPUSet(0, 1),
				firstSeen:    now,
				lastSeen:     now,
				protectUntil: now.Add(time.Minute),
			},
		},
	}
	view := model.NewDesiredView()
	view.DesiredReclaimEffective = machine.NewCPUSet(4, 5)
	dag := mustBuildDrainTestDAG(t)

	p.drainSafeDeferredLeaves(context.Background(), view, dag)

	if got := cg.cpus[rel]; !got.Equals(machine.NewCPUSet(0, 1)) {
		t.Fatalf("leaf cpuset = %s, want exact target 0-1", got.String())
	}
	if _, ok := p.deferredLeafDrains[rel]; ok {
		t.Fatalf("safe leaf drain was not cleared after successful write")
	}
}

func TestDeferredLeafDrainSkipsTargetOverlappingActualReclaim(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 13, 30, 0, 0, time.UTC)
	const rel = "kubepods/pod-a/container-a"
	cg := &fakeCgroupClient{cpus: map[string]machine.CPUSet{
		"kubepods":                machine.NewCPUSet(0, 1, 2, 3),
		"kubepods/pod-a":          machine.NewCPUSet(0, 1, 2, 3),
		"kubesandbox":             machine.NewCPUSet(1, 4, 5),
		"kubesandbox/reclaimed-0": machine.NewCPUSet(1, 4, 5),
		rel:                       machine.NewCPUSet(0, 1, 2, 3),
	}}
	p := &CPUSetTopologyPlugin{
		cgroup: cg,
		now:    func() time.Time { return now },
		deferredLeafDrains: map[string]deferredLeafDrain{
			rel: {
				target:       machine.NewCPUSet(0, 1),
				firstSeen:    now,
				lastSeen:     now,
				protectUntil: now.Add(time.Minute),
			},
		},
	}
	view := model.NewDesiredView()
	view.DesiredReclaimEffective = machine.NewCPUSet(4, 5)
	dag := mustBuildDrainTestDAG(t)

	p.drainSafeDeferredLeaves(context.Background(), view, dag)

	if got := cg.cpus[rel]; !got.Equals(machine.NewCPUSet(0, 1, 2, 3)) {
		t.Fatalf("unsafe leaf cpuset was written as %s, want unchanged 0-3", got.String())
	}
	if _, ok := p.deferredLeafDrains[rel]; !ok {
		t.Fatalf("unsafe leaf drain was cleared despite actual reclaim overlap")
	}
}

func mustBuildDrainTestDAG(t *testing.T) *topology.TopoDAG {
	t.Helper()
	dag, err := topology.BuildDAG([]topology.NodeSpec{
		{
			Rel:            "kubepods",
			Role:           topology.TopoNodeRolePrimary,
			Domain:         topology.DomainPrimary,
			CPUs:           machine.NewCPUSet(0, 1, 2, 3),
			ControlledRoot: true,
			TrustAnchor:    true,
		},
		{
			Rel:            "kubesandbox",
			Role:           topology.TopoNodeRoleReclaim,
			Domain:         topology.DomainReclaim,
			CPUs:           machine.NewCPUSet(4, 5),
			ControlledRoot: true,
			TrustAnchor:    true,
		},
		{
			Rel:       "kubesandbox/reclaimed-0",
			ParentRel: "kubesandbox",
			Role:      topology.TopoNodeRoleReclaimNUMABucket,
			Domain:    topology.DomainReclaim,
			CPUs:      machine.NewCPUSet(4, 5),
			Constraint: topology.TopologyConstraint{
				CPUUpperBound: machine.NewCPUSet(4, 5),
				Scope:         topology.TopologyScopeNUMANode,
			},
			TrustAnchor: true,
		},
	})
	if err != nil {
		t.Fatalf("BuildDAG() error = %v", err)
	}
	return dag
}

func (f *fakeCgroupClient) ApplyCPUSet(_ context.Context, rel string, data *cgcommon.CPUSetData) error {
	if f.writes == nil {
		f.writes = map[string]string{}
	}
	if f.cpusetWrites == nil {
		f.cpusetWrites = map[string]cgcommon.CPUSetData{}
	}
	if f.applyCounts == nil {
		f.applyCounts = map[string]int{}
	}
	f.writes[rel] = data.CPUs
	f.applyCounts[rel]++
	f.cpusetWrites[rel] = *data
	if f.cpus == nil {
		f.cpus = map[string]machine.CPUSet{}
	}
	if data.CPUs != "" || data.WriteEmptyCPUs {
		f.cpus[rel] = machine.MustParse(data.CPUs)
	}
	if data.Mems != "" || data.WriteEmptyMems {
		if f.mems == nil {
			f.mems = map[string]string{}
		}
		f.mems[rel] = data.Mems
	}
	if f.afterApply != nil {
		f.afterApply(rel, data)
	}
	return nil
}

func (f *fakeCgroupClient) Prune(active map[string]struct{}) {
	f.pruned = active
}

func (f *fakeCgroupClient) ListChildren(ctx context.Context, rel string) ([]string, error) {
	if f.listChildrenHook != nil {
		return f.listChildrenHook(ctx, rel)
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]string(nil), f.children[rel]...), nil
}

func (f *fakeCgroupClient) ApplySchedLoadBalance(_ context.Context, rel string, enabled bool) error {
	if f.schedLoadBalance == nil {
		f.schedLoadBalance = map[string]bool{}
	}
	f.schedLoadBalance[rel] = enabled
	return nil
}

func (f *fakeCgroupClient) ApplyCPUSetPartition(_ context.Context, rel string, flag cgcommon.CPUSetPartitionFlag) error {
	if err := f.partitionErrByRel[rel]; err != nil {
		return err
	}
	if f.partitionWrites == nil {
		f.partitionWrites = map[string]cgcommon.CPUSetPartitionFlag{}
	}
	f.partitionWrites[rel] = flag
	return nil
}

func (f *fakeCgroupClient) ReadCgroupFile(_ context.Context, rel, file string) ([]byte, error) {
	if file == "cpuset.mems" {
		if mems := f.mems[rel]; mems != "" {
			return []byte(mems), nil
		}
		return []byte("0"), nil
	}
	return nil, nil
}

func TestCPUSetTopologyPluginReconcilesPrimaryWhenReclaimEmpty(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{existing: map[string]bool{
		"primary": true,
		"reclaim": true,
	}}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:  "primary",
			BulkheadReclaimRelPaths: []string{"reclaim"},
		},
		cgroup: cg,
	}

	err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Topology: &machine.CPUTopology{
				CPUDetails: machine.CPUDetails{
					0: {},
					1: {},
					2: {},
					3: {},
				},
			},
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1, 2, 3),
			ReclaimEffective: machine.NewCPUSet(),
		}},
	})
	if err != nil {
		t.Fatalf("CPUSetAdjustmentHandler: %v", err)
	}
	if got := cg.writes["primary"]; got != "0-3" {
		t.Fatalf("primary cpuset = %q, want 0-3; writes=%v", got, cg.writes)
	}
	if _, ok := cg.pruned["primary"]; !ok {
		t.Fatalf("primary rel not pruned as active: %#v", cg.pruned)
	}
}

func TestCPUSetTopologyPluginDefaultAutoBudgetConverges96CPUHandoffAtPointZeroOne(t *testing.T) {
	t.Parallel()

	details := make(machine.CPUDetails, 96)
	for cpu := 0; cpu < 96; cpu++ {
		details[cpu] = machine.CPUTopoInfo{CoreID: cpu}
	}
	allCPUs := details.CPUs()
	cg := &fakeCgroupClient{
		version:  cgroupclient.CgroupVersionV2,
		existing: map[string]bool{"primary": true, "reclaim": true},
		cpus: map[string]machine.CPUSet{
			"primary": allCPUs,
			"reclaim": machine.NewCPUSet(),
		},
		children: map[string][]string{},
	}
	cfg := bulkheadconfig.NewBulkheadConfiguration()
	cfg.BulkheadPrimaryRelPath = "primary"
	cfg.BulkheadReclaimRelPaths = []string{"reclaim"}
	cfg.TopologyDrainSelection.MaxCPUsDrainRatio = 0.01
	p := &CPUSetTopologyPlugin{cfg: *cfg, cgroup: cg}

	err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Topology: &machine.CPUTopology{CPUDetails: details},
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(),
			ReclaimEffective: allCPUs,
		}},
	})
	if err != nil {
		t.Fatalf("CPUSetAdjustmentHandler: %v", err)
	}
	if got := cg.cpus["primary"]; !got.IsEmpty() {
		t.Fatalf("primary cpuset = %s, want empty", got.String())
	}
	if got := cg.cpus["reclaim"]; !got.Equals(allCPUs) {
		t.Fatalf("reclaim cpuset = %s, want %s", got.String(), allCPUs.String())
	}
	if cg.applyCounts["primary"] <= 32 {
		t.Fatalf("primary writes = %d, want real plugin path to exceed the old 32-round default", cg.applyCounts["primary"])
	}
}

func TestCPUSetTopologyPluginReportsAppliedViewFromFinalSnapshot(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{
		existing: map[string]bool{"primary": true, "reclaim": true},
		cpus: map[string]machine.CPUSet{
			"primary": machine.NewCPUSet(0, 1),
			"reclaim": machine.NewCPUSet(2, 3),
		},
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:  "primary",
			BulkheadReclaimRelPaths: []string{"reclaim"},
		},
		cgroup: cg,
	}
	desired := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		NonReclaimPool:   machine.NewCPUSet(0, 1),
		ReclaimEffective: machine.NewCPUSet(2, 3),
	}}
	var result bulkheadapi.TopologyResult

	dagResult, err := p.Apply(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Topology: &machine.CPUTopology{CPUDetails: machine.CPUDetails{
				0: {}, 1: {}, 2: {}, 3: {},
			}},
		},
		DesiredView: desired,
		ReportTopologyResult: func(got bulkheadapi.TopologyResult) {
			result = got
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !result.Converged || !result.FinalSnapshotCurrent {
		t.Fatalf("topology result = %+v, want converged current final snapshot", result)
	}
	if result.AppliedView == nil {
		t.Fatalf("topology result should carry an AppliedView")
	}
	if got := result.AppliedView.NonReclaimPool.String(); got != "0-1" {
		t.Fatalf("applied non reclaim = %q, want 0-1", got)
	}
	if got := result.AppliedView.ReclaimEffective.String(); got != "2-3" {
		t.Fatalf("applied reclaim = %q, want 2-3", got)
	}
	for rel, want := range map[string]string{"primary": "0-1", "reclaim": "2-3"} {
		if got := result.AppliedView.CPUSetByRel[rel].String(); got != want {
			t.Fatalf("applied rel %q = %q, want final snapshot value %q", rel, got, want)
		}
	}
	if dagResult.AppliedView == nil {
		t.Fatal("DAGApplyResult should carry the final-snapshot AppliedView")
	}
	if got := dagResult.AppliedView.ReclaimEffective.String(); got != "2-3" {
		t.Fatalf("DAG applied reclaim = %q, want 2-3", got)
	}

	desired.ReclaimEffective = machine.NewCPUSet(0, 1, 2, 3)
	if got := result.AppliedView.ReclaimEffective.String(); got != "2-3" {
		t.Fatalf("applied reclaim after desired mutation = %q, want snapshot value 2-3", got)
	}
	result.AppliedView.ReclaimEffective.Add(0)
	if dagResult.AppliedView.ReclaimEffective.Contains(0) {
		t.Fatal("DAGApplyResult AppliedView aliases callback result")
	}
	result.AppliedView.CPUSetByRel["reclaim"].Add(0)
	if dagResult.AppliedView.CPUSetByRel["reclaim"].Contains(0) {
		t.Fatal("DAGApplyResult AppliedView per-rel proof aliases callback result")
	}
}

func TestTopologyResultFromFinalConvergenceDeterminesAppliedViewLevel(t *testing.T) {
	applied := &model.AppliedView{
		Level:               model.AppliedViewLevelParentSafe,
		CPUSetPartitionView: model.NewCPUSetPartitionView(),
	}
	result := topologyResultFromFinalConvergence(topology.ConvergenceResult{
		Converged:            true,
		ParentSafe:           false,
		DeferredLeafCount:    0,
		FinalSnapshotCurrent: true,
	}, applied)

	if !result.Converged || result.ParentSafe || result.LeafDeferred {
		t.Fatalf("topology result = %+v, want final fully-converged result to override admission intent", result)
	}
	if result.AppliedView == nil || result.AppliedView.Level != model.AppliedViewLevelFull {
		t.Fatalf("applied view = %+v, want level %q from final convergence", result.AppliedView, model.AppliedViewLevelFull)
	}
}

func TestCPUSetTopologyPluginPublishesOnlyContainerLeavesProvenByFinalSnapshot(t *testing.T) {
	const (
		podUID       = "pod-materialized-during-convergence"
		containerID  = "container-materialized-during-convergence"
		containerRel = "primary/container-a"
	)
	cgcommon.RegisterRelativeCgroupPathHandler(cgcommon.RelativeCgroupPathHandler{
		Name: "bulkhead-final-snapshot-container-proof",
		Handler: func(gotPodUID, gotContainerID string) (string, bool, error) {
			if gotPodUID == podUID && gotContainerID == containerID {
				return containerRel, false, nil
			}
			return "", false, errors.New("not the final-snapshot container proof fixture")
		},
	})

	for _, tt := range []struct {
		name        string
		desiredLeaf machine.CPUSet
		wantErr     bool
	}{
		{
			name:        "pending leaf materializes with inherited parent cpuset",
			desiredLeaf: machine.NewCPUSet(0),
		},
		{
			name:        "materialized leaf matches final snapshot",
			desiredLeaf: machine.NewCPUSet(0, 1),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(podUID)}}
			cg := &fakeCgroupClient{
				existing: map[string]bool{"primary": true, "reclaim": true},
				cpus: map[string]machine.CPUSet{
					"primary": machine.NewCPUSet(0, 1),
					"reclaim": machine.NewCPUSet(2, 3),
				},
				children: map[string][]string{},
			}
			materialized := false
			cg.readOverride = func(rel string) (machine.CPUSet, bool) {
				if rel == "primary" && !materialized {
					materialized = true
					pod.Status.ContainerStatuses = []v1.ContainerStatus{{
						Name:        "main",
						ContainerID: "containerd://" + containerID,
					}}
					cg.existing[containerRel] = true
					cg.children["primary"] = []string{"container-a"}
					cg.cpus[containerRel] = machine.NewCPUSet(0, 1)
				}
				return machine.CPUSet{}, false
			}
			p := &CPUSetTopologyPlugin{
				cfg: bulkheadconfig.BulkheadConfiguration{
					BulkheadPrimaryRelPath:  "primary",
					BulkheadReclaimRelPaths: []string{"reclaim"},
				},
				cgroup: cg,
			}
			var result bulkheadapi.TopologyResult
			err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{
				CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
					MetaServer: &metaserver.MetaServer{MetaAgent: &agent.MetaAgent{
						PodFetcher: &metapod.PodFetcherStub{PodList: []*v1.Pod{pod}},
					}},
					Topology: &machine.CPUTopology{CPUDetails: machine.CPUDetails{
						0: {}, 1: {}, 2: {}, 3: {},
					}},
				},
				DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
					NonReclaimPool:   machine.NewCPUSet(0, 1),
					ReclaimEffective: machine.NewCPUSet(2, 3),
					ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
						0: machine.NewCPUSet(2, 3),
					},
					ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
						podUID: {"main": tt.desiredLeaf},
					},
				}},
				ReportTopologyResult: func(got bulkheadapi.TopologyResult) {
					result = got
				},
			})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("CPUSetAdjustmentHandler error = nil, want final snapshot leaf mismatch rejection; result=%+v", result)
				}
				if result.AppliedView != nil {
					t.Fatalf("mismatched materialized leaf must not publish AppliedView: %+v", result)
				}
				return
			}
			if err != nil {
				t.Fatalf("CPUSetAdjustmentHandler: %v", err)
			}
			if result.AppliedView == nil {
				t.Fatal("matching final snapshot leaf should publish AppliedView")
			}
			if got := result.AppliedView.ContainerCPUSetByPod[podUID]["main"]; !got.Equals(tt.desiredLeaf) {
				t.Fatalf("published container cpuset = %q, want final snapshot value %s", got.String(), tt.desiredLeaf.String())
			}
		})
	}
}

func TestCPUSetTopologyPluginNormalEmptyTopologyReturnsErrorWithoutWrites(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{existing: map[string]bool{
		"primary": true,
		"reclaim": true,
	}}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:  "primary",
			BulkheadReclaimRelPaths: []string{"reclaim"},
		},
		cgroup: cg,
	}

	err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "empty CPUDetails") {
		t.Fatalf("expected empty CPUDetails error, got %v", err)
	}
	if len(cg.writes) != 0 {
		t.Fatalf("normal empty topology must not write cgroups, writes=%#v", cg.writes)
	}
}

func TestCPUSetTopologyPluginSkipsUnchangedApplyTarget(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{existing: map[string]bool{
		"primary": true,
		"reclaim": true,
	}}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:  "primary",
			BulkheadReclaimRelPaths: []string{"reclaim"},
		},
		cgroup: cg,
	}
	in := bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Topology: &machine.CPUTopology{CPUDetails: machine.CPUDetails{
				0: {}, 1: {}, 2: {}, 3: {},
			}},
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
		}},
	}

	if err := p.CPUSetAdjustmentHandler(context.Background(), in); err != nil {
		t.Fatalf("first CPUSetAdjustmentHandler: %v", err)
	}
	firstApplyCount := totalApplyCount(cg.applyCounts)
	if firstApplyCount == 0 {
		t.Fatalf("first run should apply cpuset, writes=%v", cg.writes)
	}
	if err := p.CPUSetAdjustmentHandler(context.Background(), in); err != nil {
		t.Fatalf("second CPUSetAdjustmentHandler: %v", err)
	}
	if got := totalApplyCount(cg.applyCounts); got != firstApplyCount {
		t.Fatalf("unchanged apply target should not write again, got %d writes want %d", got, firstApplyCount)
	}
}

func TestCPUSetTopologyPluginReconcilesExternalCgroupDrift(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{existing: map[string]bool{
		"primary": true,
		"reclaim": true,
	}}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:  "primary",
			BulkheadReclaimRelPaths: []string{"reclaim"},
		},
		cgroup: cg,
	}
	in := bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Topology: &machine.CPUTopology{CPUDetails: machine.CPUDetails{
				0: {}, 1: {}, 2: {}, 3: {},
			}},
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
		}},
	}

	if err := p.CPUSetAdjustmentHandler(context.Background(), in); err != nil {
		t.Fatalf("first CPUSetAdjustmentHandler: %v", err)
	}
	firstPrimaryApplyCount := cg.applyCounts["primary"]
	cg.cpus["primary"] = machine.NewCPUSet(0)

	if err := p.CPUSetAdjustmentHandler(context.Background(), in); err != nil {
		t.Fatalf("second CPUSetAdjustmentHandler: %v", err)
	}
	if got := cg.applyCounts["primary"]; got <= firstPrimaryApplyCount {
		t.Fatalf("external primary drift was not reconciled, primary apply count=%d want > %d", got, firstPrimaryApplyCount)
	}
	if got := cg.writes["primary"]; got != "0-1" {
		t.Fatalf("primary cpuset after drift = %q, want 0-1", got)
	}
}

func TestCPUSetTopologyPluginReturnsErrorWhenNormalConvergeNonConverged(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{existing: map[string]bool{
		"primary": true,
		"reclaim": true,
	}}
	cg.afterApply = func(rel string, _ *cgcommon.CPUSetData) {
		if rel == "primary" {
			cg.cpus[rel] = machine.NewCPUSet(0)
		}
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:  "primary",
			BulkheadReclaimRelPaths: []string{"reclaim"},
		},
		cgroup: cg,
	}

	reported := false
	err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Topology: &machine.CPUTopology{CPUDetails: machine.CPUDetails{
				0: {}, 1: {}, 2: {}, 3: {},
			}},
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
		}},
		ReportTopologyResult: func(bulkheadapi.TopologyResult) {
			reported = true
		},
	})
	var blocked *topology.CoordinatorBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("CPUSetAdjustmentHandler error = %v, want typed blocked failure", err)
	}
	if cg.pruned != nil {
		t.Fatalf("non-converged normal apply must not prune, got %#v", cg.pruned)
	}
	if reported {
		t.Fatalf("failed topology convergence must not report a publishable result")
	}
}

func TestCPUSetTopologyPluginAppliesNestedNUMABucketsWithParentMemsEnvelopes(t *testing.T) {
	t.Parallel()

	const (
		primary       = "primary"
		reclaim       = "reclaim"
		directBucket0 = "reclaim/direct-0"
		directBucket1 = "reclaim/direct-1"
		nested        = "reclaim/nested"
		deep          = "reclaim/nested/deep"
		nestedBucket0 = "reclaim/nested/deep/bucket-0"
		nestedBucket1 = "reclaim/nested/deep/bucket-1"
	)
	cg := &fakeCgroupClient{
		existing: map[string]bool{
			primary: true, reclaim: true, directBucket0: true, directBucket1: true,
			nested: true, nestedBucket0: true, nestedBucket1: true,
		},
		cpus: map[string]machine.CPUSet{
			primary: machine.NewCPUSet(0, 1), reclaim: machine.NewCPUSet(2, 3),
			directBucket0: machine.NewCPUSet(2), directBucket1: machine.NewCPUSet(3),
			nested: machine.NewCPUSet(2, 3), deep: machine.NewCPUSet(2, 3),
			nestedBucket0: machine.NewCPUSet(2), nestedBucket1: machine.NewCPUSet(3),
		},
		mems: map[string]string{
			primary: "0-1", reclaim: "0-1", directBucket0: "0", directBucket1: "1",
			nested: "0-1", deep: "0-1", nestedBucket0: "0", nestedBucket1: "1",
		},
		children: map[string][]string{
			reclaim: {"direct-0", "direct-1", "nested"},
			nested:  {"deep"},
			deep:    {"bucket-0", "bucket-1"},
		},
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:      primary,
			BulkheadReclaimRelPaths:     []string{reclaim, nested},
			BulkheadReclaimNumaPrefixes: []string{"reclaim/direct-", "reclaim/nested/deep/bucket-"},
		},
		cgroup: cg,
	}
	in := bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Topology: &machine.CPUTopology{CPUDetails: machine.CPUDetails{
				0: {NUMANodeID: 0}, 1: {NUMANodeID: 0},
				2: {NUMANodeID: 0}, 3: {NUMANodeID: 1},
			}},
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
			ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
				0: machine.NewCPUSet(2),
				1: machine.NewCPUSet(3),
			},
		}},
	}

	if err := p.CPUSetAdjustmentHandler(context.Background(), in); err != nil {
		t.Fatalf("CPUSetAdjustmentHandler nested NUMA buckets: %v", err)
	}
	for _, rel := range []string{
		reclaim, directBucket0, directBucket1, nested, nestedBucket0, nestedBucket1,
	} {
		if _, ok := cg.pruned[rel]; !ok {
			t.Fatalf("successful nested NUMA apply did not publish active rel %q: %#v", rel, cg.pruned)
		}
	}
}

func TestCPUSetTopologyPluginHandlesConfiguredNUMABucketTransitionToEmpty(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		version cgroupclient.CgroupVersion
	}{
		{name: "v1 fails closed", version: cgroupclient.CgroupVersionV1},
		{name: "v2 clears and publishes proof", version: cgroupclient.CgroupVersionV2},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const (
				primary = "primary"
				reclaim = "reclaim"
				bucket0 = "reclaim/bucket-0"
				bucket1 = "reclaim/bucket-1"
			)
			cg := &fakeCgroupClient{
				version: tc.version,
				existing: map[string]bool{
					primary: true, reclaim: true, bucket0: true, bucket1: true,
				},
				cpus: map[string]machine.CPUSet{
					primary: machine.NewCPUSet(0, 1),
					reclaim: machine.NewCPUSet(2, 3),
					bucket0: machine.NewCPUSet(2),
					bucket1: machine.NewCPUSet(3),
				},
				mems: map[string]string{
					primary: "0-1", reclaim: "0-1", bucket0: "0", bucket1: "1",
				},
				children: map[string][]string{
					reclaim: {"bucket-0", "bucket-1"},
				},
			}
			p := &CPUSetTopologyPlugin{
				cfg: bulkheadconfig.BulkheadConfiguration{
					BulkheadPrimaryRelPath:      primary,
					BulkheadReclaimRelPaths:     []string{reclaim},
					BulkheadReclaimNumaPrefixes: []string{"reclaim/bucket-"},
				},
				cgroup: cg,
			}
			var result bulkheadapi.TopologyResult
			err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{
				CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
					Topology: &machine.CPUTopology{CPUDetails: machine.CPUDetails{
						0: {NUMANodeID: 0}, 1: {NUMANodeID: 0},
						2: {NUMANodeID: 0}, 3: {NUMANodeID: 1},
					}},
				},
				DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
					NonReclaimPool:   machine.NewCPUSet(0, 1, 3),
					ReclaimEffective: machine.NewCPUSet(2),
					ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
						0: machine.NewCPUSet(2),
						1: machine.NewCPUSet(),
					},
				}},
				ReportTopologyResult: func(got bulkheadapi.TopologyResult) {
					result = got
				},
			})

			if tc.version == cgroupclient.CgroupVersionV1 {
				if err != nil {
					t.Fatalf("CPUSetAdjustmentHandler: %v", err)
				}
				if got := cg.cpus[bucket1]; got.IsEmpty() || !got.Equals(machine.NewCPUSet(3)) {
					t.Fatalf("v1 bucket %q cpuset = %s, want preserved non-empty current 3", bucket1, got.String())
				}
				if got := cg.cpus[reclaim]; !got.Equals(machine.NewCPUSet(2, 3)) {
					t.Fatalf("v1 parent reclaim cpuset = %s, want it to cover preserved bucket %q", got.String(), bucket1)
				}
				return
			}

			if err != nil {
				t.Fatalf("CPUSetAdjustmentHandler: %v", err)
			}
			if write, ok := cg.cpusetWrites[bucket1]; !ok || !write.WriteEmptyCPUs {
				t.Fatalf("v2 configured empty bucket write = %+v, present=%v; want explicit empty write", write, ok)
			}
			if result.AppliedView == nil {
				t.Fatal("v2 configured empty bucket did not publish AppliedView")
			}
			bucket, ok := result.AppliedView.ReclaimEffectivePerNUMA[1]
			if !ok || !bucket.IsEmpty() {
				t.Fatalf("AppliedView NUMA 1 proof = %s, present=%v; want present empty bucket", bucket.String(), ok)
			}
			if relCPUSet, ok := result.AppliedView.CPUSetByRel[bucket1]; !ok || !relCPUSet.IsEmpty() {
				t.Fatalf("AppliedView rel proof for %q = %s, present=%v; want present empty cpuset", bucket1, relCPUSet.String(), ok)
			}
		})
	}
}

func TestCPUSetTopologyPluginRejectsNUMABucketOutsidePhysicalEnvelope(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{
		existing: map[string]bool{
			"primary": true, "reclaim": true, "reclaim/bucket-0": true,
		},
		cpus: map[string]machine.CPUSet{
			"primary": machine.NewCPUSet(3), "reclaim": machine.NewCPUSet(0),
			"reclaim/bucket-0": machine.NewCPUSet(0),
		},
		mems: map[string]string{
			"primary": "0-1", "reclaim": "0", "reclaim/bucket-0": "0",
		},
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:      "primary",
			BulkheadReclaimRelPaths:     []string{"reclaim"},
			BulkheadReclaimNumaPrefixes: []string{"reclaim/bucket-"},
		},
		cgroup: cg,
	}
	in := bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Topology: &machine.CPUTopology{CPUDetails: machine.CPUDetails{
				0: {NUMANodeID: 0}, 1: {NUMANodeID: 0},
				2: {NUMANodeID: 1}, 3: {NUMANodeID: 1},
			}},
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:          machine.NewCPUSet(3),
			ReclaimEffective:        machine.NewCPUSet(0, 2),
			ReclaimEffectivePerNUMA: map[int]machine.CPUSet{0: machine.NewCPUSet(0, 2)},
		}},
	}

	err := p.CPUSetAdjustmentHandler(context.Background(), in)
	if err == nil {
		t.Fatal("CPUSetAdjustmentHandler accepted NUMA bucket outside physical envelope")
	}
	if len(cg.writes) != 0 {
		t.Fatalf("invalid NUMA target published writes: %#v", cg.writes)
	}
}

func TestCPUSetTopologyPluginReturnsSiblingDiscoveryError(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{
		existing: map[string]bool{"primary": true, "reclaim": true},
		listErr:  errors.New("list failed"),
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:        "primary",
			BulkheadReclaimRelPaths:       []string{"reclaim"},
			EnableBulkheadReclaimSiblings: true,
		},
		cgroup: cg,
	}

	err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
		}},
	})
	if err == nil {
		t.Fatalf("expected sibling discovery error")
	}
}

func TestCPUSetTopologyPluginAdmissionDeadlineStartsBeforeSiblingDiscovery(t *testing.T) {
	t.Parallel()

	const safeDuration = 250 * time.Millisecond
	deadlineObserved := false
	cg := &fakeCgroupClient{
		existing: map[string]bool{"primary": true, "reclaim": true},
		listChildrenHook: func(ctx context.Context, _ string) ([]string, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				return nil, errors.New("admission context has no deadline")
			}
			remaining := time.Until(deadline)
			if remaining <= 0 || remaining > safeDuration {
				return nil, fmt.Errorf("admission deadline remaining = %s, want within (0, %s]", remaining, safeDuration)
			}
			deadlineObserved = true
			return nil, errors.New("stop after observing admission deadline")
		},
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:        "primary",
			BulkheadReclaimRelPaths:       []string{"reclaim"},
			EnableBulkheadReclaimSiblings: true,
			EnableAdmissionLeafDefer:      true,
			AdmissionSafeDuration:         safeDuration,
		},
		cgroup: cg,
	}

	err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Mode: cpusetutil.CPUSetAdjustmentModeAdmission,
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
		}},
	})
	if err == nil {
		t.Fatal("CPUSetAdjustmentHandler() error = nil, want sibling discovery sentinel")
	}
	if !deadlineObserved {
		t.Fatalf("sibling discovery did not receive admission deadline: %v", err)
	}
}

func TestCPUSetTopologyPluginAdmissionDeadlineWinsAfterPreCoordinatorStage(t *testing.T) {
	t.Parallel()

	const safeDuration = 20 * time.Millisecond
	stageErr := errors.New("late sibling discovery result")
	cg := &fakeCgroupClient{
		existing: map[string]bool{"primary": true, "reclaim": true},
		listChildrenHook: func(ctx context.Context, _ string) ([]string, error) {
			<-ctx.Done()
			return nil, stageErr
		},
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:        "primary",
			BulkheadReclaimRelPaths:       []string{"reclaim"},
			EnableBulkheadReclaimSiblings: true,
			EnableAdmissionLeafDefer:      true,
			AdmissionSafeDuration:         safeDuration,
		},
		cgroup: cg,
	}

	err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			Mode: cpusetutil.CPUSetAdjustmentModeAdmission,
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
		}},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CPUSetAdjustmentHandler() error = %v, want admission deadline instead of late stage error %v", err, stageErr)
	}
}

func TestCPUSetTopologyPluginDisabledTransitionUsesTopologySpecsAndDAGExpandV1(t *testing.T) {
	t.Parallel()

	p, cg, in, containerRel := newDisabledTransitionTestPlugin(
		t,
		cgroupclient.CgroupVersionV1,
		"bulkhead-disabled-v1-pod",
		"bulkhead-disabled-v1-container",
	)

	err := p.CPUSetAdjustmentDisabledHandler(context.Background(), in)
	if err != nil {
		t.Fatalf("CPUSetAdjustmentDisabledHandler: %v", err)
	}

	wantMachine := "0-3"
	for _, rel := range []string{
		"primary",
		"reclaim",
		"reclaim/reclaim-0",
		"sibling",
		"primary/burstable",
		"primary/burstable/pod-a",
	} {
		if got := cg.writes[rel]; got != wantMachine {
			t.Fatalf("cpuset @ %s = %q, want %q; writes=%#v", rel, got, wantMachine, cg.writes)
		}
	}
	if got := cg.writes[containerRel]; got != "0" {
		t.Fatalf("container cpuset = %q, want 0; writes=%#v", got, cg.writes)
	}
	if got := cg.cpusetWrites["reclaim/reclaim-0"].Mems; got != "" {
		t.Fatalf("cpuset_topology wrote reclaim NUMA cpuset.mems = %q, want empty", got)
	}
	if _, ok := cg.writes["partition"]; ok {
		t.Fatalf("partition should not receive cpuset.cpus write, writes=%#v", cg.writes)
	}
	if len(cg.schedLoadBalance) != 0 {
		t.Fatalf("disabled transition should not write sched_load_balance, got %#v", cg.schedLoadBalance)
	}
	if cg.pruned != nil {
		t.Fatalf("disabled transition should not prune, got %#v", cg.pruned)
	}
}

func TestCPUSetTopologyPluginDisabledTransitionUsesTopologySpecsAndDAGExpandV2ToEmpty(t *testing.T) {
	t.Parallel()

	p, cg, in, containerRel := newDisabledTransitionTestPlugin(
		t,
		cgroupclient.CgroupVersionV2,
		"bulkhead-disabled-v2-pod",
		"bulkhead-disabled-v2-container",
	)

	err := p.CPUSetAdjustmentDisabledHandler(context.Background(), in)
	if err != nil {
		t.Fatalf("CPUSetAdjustmentDisabledHandler: %v", err)
	}

	for _, rel := range []string{
		"primary",
		"reclaim",
		"reclaim/reclaim-0",
		"primary/burstable",
		"primary/burstable/pod-a",
	} {
		if got := cg.writes[rel]; got != "" {
			t.Fatalf("cpuset @ %s = %q, want empty; writes=%#v", rel, got, cg.writes)
		}
	}
	if got := cg.writes[containerRel]; got != "0" {
		t.Fatalf("container cpuset = %q, want 0; writes=%#v", got, cg.writes)
	}
	if got := cg.cpusetWrites["reclaim/reclaim-0"].Mems; got != "" {
		t.Fatalf("cpuset_topology wrote reclaim NUMA cpuset.mems = %q, want empty", got)
	}
	if write := cg.cpusetWrites["reclaim/reclaim-0"]; write.CPUs != "" || !write.WriteEmptyCPUs {
		t.Fatalf("v2 reclaim NUMA cpuset write = %+v, want empty cpus with WriteEmptyCPUs", write)
	}
	if _, ok := cg.writes["partition"]; ok {
		t.Fatalf("partition should not receive cpuset.cpus write, writes=%#v", cg.writes)
	}
	if len(cg.schedLoadBalance) != 0 {
		t.Fatalf("disabled transition should not write sched_load_balance, got %#v", cg.schedLoadBalance)
	}
	if cg.pruned != nil {
		t.Fatalf("disabled transition should not prune, got %#v", cg.pruned)
	}
}

func TestCPUSetTopologyPluginDisabledTransitionV2EmptyResetWritesEmptyCPUs(t *testing.T) {
	t.Parallel()

	p, cg, in, _ := newDisabledTransitionTestPlugin(
		t,
		cgroupclient.CgroupVersionV2,
		"bulkhead-disabled-v2-fallback-pod",
		"bulkhead-disabled-v2-fallback-container",
	)
	for _, rel := range []string{
		"primary/burstable",
		"primary/burstable/pod-a",
		"primary/burstable/pod-a/container-a",
	} {
		cg.existing[rel] = true
	}
	if err := p.CPUSetAdjustmentDisabledHandler(context.Background(), in); err != nil {
		t.Fatalf("CPUSetAdjustmentDisabledHandler: %v", err)
	}

	for _, rel := range []string{"primary", "reclaim", "reclaim/reclaim-0"} {
		write := cg.cpusetWrites[rel]
		if write.CPUs != "" || !write.WriteEmptyCPUs {
			t.Fatalf("v2 cpuset write @ %s = %+v, want empty CPUSetData with WriteEmptyCPUs", rel, write)
		}
	}
}

func TestCPUSetTopologyPluginDisabledTransitionSkipsMissingResetRel(t *testing.T) {
	t.Parallel()

	p, cg, in, _ := newDisabledTransitionTestPlugin(
		t,
		cgroupclient.CgroupVersionV1,
		"bulkhead-disabled-missing-sandbox-pod",
		"bulkhead-disabled-missing-sandbox-container",
	)
	missingRel := "sandboxes"
	p.cfg.BulkheadReclaimRelPaths = append(p.cfg.BulkheadReclaimRelPaths, missingRel)
	cg.statErrors = map[string]error{missingRel: os.ErrNotExist}
	cg.readErrByRel = map[string]error{missingRel: os.ErrNotExist}

	if err := p.CPUSetAdjustmentDisabledHandler(context.Background(), in); err != nil {
		t.Fatalf("CPUSetAdjustmentDisabledHandler: %v", err)
	}
	if _, ok := cg.writes[missingRel]; ok {
		t.Fatalf("missing reset rel %q received write; writes=%#v", missingRel, cg.writes)
	}
}

func TestCPUSetTopologyPluginDisabledTransitionReturnsErrorForInvalidV1Target(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		topology *machine.CPUTopology
	}{
		{name: "nil topology"},
		{name: "empty machine cpuset", topology: &machine.CPUTopology{CPUDetails: machine.CPUDetails{}}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, _, in, _ := newDisabledTransitionTestPlugin(
				t,
				cgroupclient.CgroupVersionV1,
				"bulkhead-disabled-invalid-"+tt.name,
				"bulkhead-disabled-invalid-container",
			)
			in.Topology = tt.topology

			err := p.CPUSetAdjustmentDisabledHandler(context.Background(), in)
			if err == nil {
				t.Fatalf("expected invalid v1 reset target error")
			}
		})
	}
}

func TestCPUSetTopologyPluginDisabledTransitionReturnsSiblingDiscoveryError(t *testing.T) {
	t.Parallel()

	p, _, in, _ := newDisabledTransitionTestPlugin(
		t,
		cgroupclient.CgroupVersionV1,
		"bulkhead-disabled-list-error-pod",
		"bulkhead-disabled-list-error-container",
	)
	p.cgroup.(*fakeCgroupClient).listErr = errors.New("list failed")

	err := p.CPUSetAdjustmentDisabledHandler(context.Background(), in)
	if err == nil {
		t.Fatalf("expected sibling discovery error")
	}
}

func TestCPUSetTopologyPluginDisabledTransitionReturnsTypedErrorWhenResetNotConverged(t *testing.T) {
	t.Parallel()

	p, cg, in, _ := newDisabledTransitionTestPlugin(
		t,
		cgroupclient.CgroupVersionV1,
		"bulkhead-disabled-reset-mismatch-pod",
		"bulkhead-disabled-reset-mismatch-container",
	)
	cg.afterApply = func(rel string, data *cgcommon.CPUSetData) {
		if rel == "primary" && (data.CPUs != "" || data.WriteEmptyCPUs) {
			cg.cpus[rel] = machine.NewCPUSet(0, 1)
		}
	}

	err := p.CPUSetAdjustmentDisabledHandler(context.Background(), in)
	if err == nil {
		t.Fatalf("expected disabled reset not converged error")
	}
	var notConverged *disabledResetNotConvergedError
	if !errors.As(err, &notConverged) {
		t.Fatalf("error type = %T, want *disabledResetNotConvergedError: %v", err, err)
	}
	if notConverged.state != topology.ConvergenceStateNonConverged {
		t.Fatalf("state = %s, want %s", notConverged.state, topology.ConvergenceStateNonConverged)
	}
	if len(notConverged.report.NonConvergedTargets) == 0 {
		t.Fatalf("expected non-converged targets in report: %+v", notConverged.report)
	}
	if !strings.Contains(err.Error(), "not converged") {
		t.Fatalf("error = %v, want explicit not converged message", err)
	}
}

func TestCPUSetTopologyPluginSharedModeGuardRejectsResetWhileNormalHeldWithoutWrites(t *testing.T) {
	t.Parallel()

	p, cg, in, _ := newDisabledTransitionTestPlugin(
		t,
		cgroupclient.CgroupVersionV1,
		"bulkhead-disabled-shared-guard-pod",
		"bulkhead-disabled-shared-guard-container",
	)
	token, err := topology.NormalModeGuardWithGate(p.sharedModeGate()).TryEnter()
	if err != nil {
		t.Fatalf("TryEnter normal: %v", err)
	}
	defer token.Exit()

	err = p.CPUSetAdjustmentDisabledHandler(context.Background(), in)
	var busy *topology.CoordinatorBusyError
	if !errors.As(err, &busy) {
		t.Fatalf("CPUSetAdjustmentDisabledHandler error = %T %v, want *CoordinatorBusyError", err, err)
	}
	if len(cg.writes) != 0 {
		t.Fatalf("busy reset must not write cgroups, writes=%#v", cg.writes)
	}
}

func newDisabledTransitionTestPlugin(
	t *testing.T,
	version cgroupclient.CgroupVersion,
	podUID string,
	containerID string,
) (*CPUSetTopologyPlugin, *fakeCgroupClient, bulkheadapi.HandlerContext, string) {
	t.Helper()

	containerRel := "primary/burstable/pod-a/container-a"
	cgcommon.RegisterRelativeCgroupPathHandler(cgcommon.RelativeCgroupPathHandler{
		Name: "bulkhead-disabled-" + podUID,
		Handler: func(gotPodUID, gotContainerID string) (string, bool, error) {
			if gotPodUID == podUID && gotContainerID == containerID {
				return containerRel, false, nil
			}
			return "", false, errors.New("not a bulkhead disabled transition test container")
		},
	})

	cg := &fakeCgroupClient{
		version: version,
		existing: map[string]bool{
			"primary":           true,
			"reclaim":           true,
			"reclaim/reclaim-0": true,
			"sibling":           true,
			"partition":         true,
		},
		cpus: map[string]machine.CPUSet{
			"primary":                             machine.NewCPUSet(0, 1, 2, 3),
			"primary/burstable":                   machine.NewCPUSet(0, 1, 2, 3),
			"primary/burstable/pod-a":             machine.NewCPUSet(0, 1),
			"primary/burstable/pod-a/container-a": machine.NewCPUSet(0),
			"reclaim":                             machine.NewCPUSet(2, 3),
			"reclaim/reclaim-0":                   machine.NewCPUSet(2, 3),
			"sibling":                             machine.NewCPUSet(2, 3),
			"partition":                           machine.NewCPUSet(0, 1, 2, 3),
		},
		children: map[string][]string{
			"":                        {"primary", "reclaim", "sibling", "partition"},
			"primary":                 {"burstable"},
			"primary/burstable":       {"pod-a"},
			"primary/burstable/pod-a": {"container-a"},
			"reclaim":                 {"reclaim-0"},
		},
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPrimaryRelPath:        "primary",
			BulkheadReclaimRelPaths:       []string{"reclaim"},
			BulkheadReclaimNumaPrefixes:   []string{"reclaim/reclaim-"},
			BulkheadPartitionRelPaths:     []string{"partition"},
			EnableBulkheadReclaimSiblings: true,
		},
		cgroup: cg,
	}
	in := bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			MetaServer: &metaserver.MetaServer{
				MetaAgent: &agent.MetaAgent{
					PodFetcher: &metapod.PodFetcherStub{PodList: []*v1.Pod{{
						ObjectMeta: metav1.ObjectMeta{UID: types.UID(podUID)},
						Status: v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{{
							Name:        "main",
							ContainerID: "containerd://" + containerID,
						}}},
					}}},
				},
			},
			Topology: &machine.CPUTopology{
				CPUDetails: machine.CPUDetails{
					0: {},
					1: {},
					2: {},
					3: {},
				},
			},
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
			ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
				0: machine.NewCPUSet(2, 3),
			},
			ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
				podUID: {
					"main": machine.NewCPUSet(0),
				},
			},
		}},
	}
	return p, cg, in, containerRel
}

func TestCPUSetTopologyPluginPeriodicalHandlerResetsSchedLoadBalanceWhenDisabledV1(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{version: cgroupclient.CgroupVersionV1}
	p := &CPUSetTopologyPlugin{cgroup: cg}

	err := p.PeriodicalHandler(context.Background(), bulkheadapi.PeriodicalHandlerContext{})
	if err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if got := cg.schedLoadBalance[""]; !got {
		t.Fatalf("root sched_load_balance = %t, want true", got)
	}
}

func TestCPUSetTopologyPluginPeriodicalHandlerAppliesSchedLoadBalanceFalseWhenEnabledV1(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{version: cgroupclient.CgroupVersionV1}
	p := &CPUSetTopologyPlugin{cgroup: cg}

	err := p.PeriodicalHandler(context.Background(), bulkheadapi.PeriodicalHandlerContext{
		DynamicConf: enabledBulkheadCpusetTopologyDynamicConf(),
	})
	if err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if got := cg.schedLoadBalance[""]; got {
		t.Fatalf("root sched_load_balance = %t, want false", got)
	}
}

func TestCPUSetTopologyPluginPeriodicalHandlerResetsPartitionWhenDisabledV2(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{
		version:  cgroupclient.CgroupVersionV2,
		existing: map[string]bool{"partition": true},
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPartitionRelPaths: []string{"partition"},
		},
		cgroup: cg,
	}

	err := p.PeriodicalHandler(context.Background(), bulkheadapi.PeriodicalHandlerContext{})
	if err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if got := cg.partitionWrites["partition"]; got != cgcommon.CPUSetPartitionFlagMember {
		t.Fatalf("partition flag = %s, want %s", got, cgcommon.CPUSetPartitionFlagMember)
	}
}

func TestCPUSetTopologyPluginPeriodicalHandlerAppliesPartitionRootWhenEnabledV2(t *testing.T) {
	t.Parallel()

	cg := &fakeCgroupClient{
		version:  cgroupclient.CgroupVersionV2,
		existing: map[string]bool{"partition": true},
	}
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadconfig.BulkheadConfiguration{
			BulkheadPartitionRelPaths: []string{"partition"},
		},
		cgroup: cg,
	}

	err := p.PeriodicalHandler(context.Background(), bulkheadapi.PeriodicalHandlerContext{
		DynamicConf: enabledBulkheadCpusetTopologyDynamicConf(),
	})
	if err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if got := cg.partitionWrites["partition"]; got != cgcommon.CPUSetPartitionFlagRoot {
		t.Fatalf("partition flag = %s, want %s", got, cgcommon.CPUSetPartitionFlagRoot)
	}
}

func TestEnableBulkheadCpusetTopologyRequiresNonOverlapReclaimedCores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                                       string
		enableBulkheadCpusetTopology               bool
		stateAllowSharedCoresOverlapReclaimedCores bool
		confAllowSharedCoresOverlapReclaimedCores  bool
		want                                       bool
	}{
		{
			name:                         "enabled and non overlap",
			enableBulkheadCpusetTopology: true,
			want:                         true,
		},
		{
			name:                         "enabled but overlap",
			enableBulkheadCpusetTopology: true,
			stateAllowSharedCoresOverlapReclaimedCores: true,
		},
		{
			name: "disabled and non overlap",
		},
		{
			name:                         "uses state overlap instead of dynamic config overlap",
			enableBulkheadCpusetTopology: true,
			confAllowSharedCoresOverlapReclaimedCores: true,
			want: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conf := bulkheadCpusetTopologyDynamicConf(
				tt.enableBulkheadCpusetTopology,
				tt.confAllowSharedCoresOverlapReclaimedCores,
			)
			state := cpustate.NewCPUPluginState(nil)
			state.SetAllowSharedCoresOverlapReclaimedCores(tt.stateAllowSharedCoresOverlapReclaimedCores)
			if got := enableBulkheadCpusetTopology(bulkheadapi.HandlerContext{
				CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
					DynamicConf: conf,
					State:       state,
				},
			}); got != tt.want {
				t.Fatalf("enableBulkheadCpusetTopology() = %t, want %t", got, tt.want)
			}
		})
	}
}

func enabledBulkheadCpusetTopologyDynamicConf() *dynamicconfig.Configuration {
	return bulkheadCpusetTopologyDynamicConf(true, false)
}

func bulkheadCpusetTopologyDynamicConf(enableBulkheadCpusetTopology, allowSharedCoresOverlapReclaimedCores bool) *dynamicconfig.Configuration {
	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadCpusetTopology = enableBulkheadCpusetTopology
	conf.AdminQoSConfiguration.CPUProvisionConfiguration.AllowSharedCoresOverlapReclaimedCores = allowSharedCoresOverlapReclaimedCores
	return conf
}

func TestCPUSetTopologyPluginSkipsExpectedCPUSetForMissingPod(t *testing.T) {
	t.Parallel()

	p := &CPUSetTopologyPlugin{}
	view := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
			"missing-pod": {
				"main": machine.NewCPUSet(0, 1),
			},
		},
	}}
	metaServer := &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{
			PodFetcher: &metapod.PodFetcherStub{},
		},
	}

	// A missing pod fails at the container-id stage, which is the admit-safe
	// pending case: no error, no expected leaf, but the allocation is recorded
	// as protected-pending so the writer keeps the parent a superset.
	res, err := p.buildExpectedCPUSetByRel(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{MetaServer: metaServer},
		DesiredView:                view,
	})
	if err != nil {
		t.Fatalf("missing pod must not error (admit-safe pending), got %v", err)
	}
	if len(res.ExpectedByRel) != 0 {
		t.Fatalf("expected no resolved leaves, got %#v", res.ExpectedByRel)
	}
	if len(res.PendingByPod) != 1 {
		t.Fatalf("expected one protected-pending entry, got %#v", res.PendingByPod)
	}
	if got := res.PendingCPUSetUnion().String(); got != "0-1" {
		t.Fatalf("pending union = %s, want 0-1", got)
	}
}

type admissionContextKey struct{}

type admissionContextContainerIDFetcher struct {
	metapod.PodFetcherStub
	wantValue string
	calls     int
}

func (f *admissionContextContainerIDFetcher) GetContainerIDWithContext(
	ctx context.Context, _, _ string,
) (string, error) {
	if got := ctx.Value(admissionContextKey{}); got != f.wantValue {
		return "", fmt.Errorf("container ID lookup context value = %v, want %q", got, f.wantValue)
	}
	f.calls++
	return "context-aware-container", nil
}

type failingAdmissionContainerIDFetcher struct {
	metapod.PodFetcherStub
	err error
}

func (f *failingAdmissionContainerIDFetcher) GetContainerIDWithContext(
	context.Context, string, string,
) (string, error) {
	return "", f.err
}

func TestCPUSetTopologyExpectedBuildAndFinalPublishUseAdmissionContext(t *testing.T) {
	t.Parallel()

	const (
		podUID        = "context-aware-pod"
		containerName = "main"
		containerRel  = "kubepods/context-aware-pod/context-aware-container"
		contextValue  = "admission"
	)
	cgcommon.RegisterRelativeCgroupPathHandler(cgcommon.RelativeCgroupPathHandler{
		Name: "cpuset-topology-admission-context",
		Handler: func(gotPodUID, gotContainerID string) (string, bool, error) {
			if gotPodUID == podUID && gotContainerID == "context-aware-container" {
				return containerRel, false, nil
			}
			return "", false, errors.New("not the admission context test container")
		},
	})
	fetcher := &admissionContextContainerIDFetcher{wantValue: contextValue}
	metaServer := &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{PodFetcher: fetcher},
	}
	desired := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
			podUID: {containerName: machine.NewCPUSet(0, 1)},
		},
	}}
	ctx := context.WithValue(context.Background(), admissionContextKey{}, contextValue)

	p := &CPUSetTopologyPlugin{}
	expected, err := p.buildExpectedCPUSetByRel(ctx, bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{MetaServer: metaServer},
		DesiredView:                desired,
	})
	if err != nil {
		t.Fatalf("buildExpectedCPUSetByRel() error = %v", err)
	}
	if got := expected.ExpectedByRel[containerRel].String(); got != "0-1" {
		t.Fatalf("expected cpuset = %q, want 0-1", got)
	}

	snapshot := &topology.CompleteSnapshot{Entries: map[string]topology.EntryState{
		containerRel: {Identity: topology.CgroupIdentity{Inode: 1}, CPUs: machine.NewCPUSet(0, 1)},
	}}
	published, err := containerCPUSetByPodFromFinalSnapshotWithContext(
		ctx, metaServer, desired, snapshot, expected.ExpectedByRel)
	if err != nil {
		t.Fatalf("containerCPUSetByPodFromFinalSnapshotWithContext() error = %v", err)
	}
	if got := published[podUID][containerName].String(); got != "0-1" {
		t.Fatalf("published cpuset = %q, want 0-1", got)
	}
	if fetcher.calls != 2 {
		t.Fatalf("context-aware container ID lookup calls = %d, want 2", fetcher.calls)
	}
}

func TestCPUSetTopologyPluginSkipsExpectedCPUSetForMissingContainer(t *testing.T) {
	t.Parallel()

	p := &CPUSetTopologyPlugin{}
	metaServer := &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{
			PodFetcher: &metapod.PodFetcherStub{PodList: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{UID: types.UID("pod-1")},
			}}},
		},
	}
	view := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
			"pod-1": {
				"missing-container": machine.NewCPUSet(0, 1),
			},
		},
	}}

	// A container with no status yet also fails at the container-id stage:
	// admit-safe pending, not an error.
	res, err := p.buildExpectedCPUSetByRel(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{MetaServer: metaServer},
		DesiredView:                view,
	})
	if err != nil {
		t.Fatalf("missing container must not error (admit-safe pending), got %v", err)
	}
	if len(res.ExpectedByRel) != 0 {
		t.Fatalf("expected no resolved leaves, got %#v", res.ExpectedByRel)
	}
	if len(res.PendingByPod) != 1 {
		t.Fatalf("expected one protected-pending entry, got %#v", res.PendingByPod)
	}
}

func TestCPUSetTopologyPluginFailsExpectedCPUSetForContainerIDDeadline(t *testing.T) {
	t.Parallel()

	p := &CPUSetTopologyPlugin{}
	metaServer := &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{
			PodFetcher: &failingAdmissionContainerIDFetcher{err: context.DeadlineExceeded},
		},
	}
	view := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
			"pod-1": {
				"main": machine.NewCPUSet(0, 1),
			},
		},
	}}

	res, err := p.buildExpectedCPUSetByRel(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{MetaServer: metaServer},
		DesiredView:                view,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("container ID deadline must fail-closed, got res=%#v err=%v", res, err)
	}
}

func TestCPUSetTopologyPluginFailsExpectedCPUSetForUnresolvedContainerRel(t *testing.T) {
	t.Parallel()

	p := &CPUSetTopologyPlugin{}
	metaServer := &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{
			PodFetcher: &metapod.PodFetcherStub{PodList: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{UID: types.UID("pod-1")},
				Status: v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{{
					Name:        "main",
					ContainerID: "invalid-container-id",
				}}},
			}}},
		},
	}
	view := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
			"pod-1": {
				"main": machine.NewCPUSet(0, 1),
			},
		},
	}}

	// The container id resolves, but the relative cgroup path cannot be resolved
	// (no handler / broken layout). This is a real error, NOT the admit window,
	// so the round must fail-closed rather than apply a partial topology.
	res, err := p.buildExpectedCPUSetByRel(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{MetaServer: metaServer},
		DesiredView:                view,
	})
	if err == nil {
		t.Fatalf("unresolved container rel (id known) must fail-closed, got res=%#v", res)
	}
}

// TestCPUSetTopologyPluginBuildExpectedCPUSetByRelTrimsLeadingSlash is a
// regression guard: buildExpectedCPUSetByRel must produce map keys WITHOUT a
// leading "/", so they match the childRel format that
// utils/topology/writer.expandDescendants constructs via filepath.Join. If the
// key kept the leading "/" that GetKubernetesAnyExistRelativeCgroupPath
// prepends, per-container cpuset enforcement would silently degrade to
// inheriting the parent pool target inside TopologyCoordinatorConverge.
func TestCPUSetTopologyPluginBuildExpectedCPUSetByRelTrimsLeadingSlash(t *testing.T) {
	t.Parallel()

	const (
		podUID      = "pod-build-expected-trim"
		containerID = "container-build-expected-trim"
		prefixedRel = "/kubepods/burstable/pod-a/container-a"
		expectedRel = "kubepods/burstable/pod-a/container-a"
		containerNm = "main"
	)

	cgcommon.RegisterRelativeCgroupPathHandler(cgcommon.RelativeCgroupPathHandler{
		Name: "bulkhead-build-expected-trim-" + podUID,
		Handler: func(gotPodUID, gotContainerID string) (string, bool, error) {
			if gotPodUID == podUID && gotContainerID == containerID {
				return prefixedRel, false, nil
			}
			return "", false, errors.New("not a build-expected-trim test container")
		},
	})

	p := &CPUSetTopologyPlugin{}
	res, err := p.buildExpectedCPUSetByRel(context.Background(), bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			MetaServer: &metaserver.MetaServer{
				MetaAgent: &agent.MetaAgent{
					PodFetcher: &metapod.PodFetcherStub{PodList: []*v1.Pod{{
						ObjectMeta: metav1.ObjectMeta{UID: types.UID(podUID)},
						Status: v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{{
							Name:        containerNm,
							ContainerID: "containerd://" + containerID,
						}}},
					}}},
				},
			},
		},
		DesiredView: &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
			ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
				podUID: {
					containerNm: machine.NewCPUSet(0, 1),
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("buildExpectedCPUSetByRel: %v", err)
	}
	expected := res.ExpectedByRel
	if _, ok := expected[prefixedRel]; ok {
		t.Fatalf("map key still has leading '/': %q; keys=%v", prefixedRel, keysOf(expected))
	}
	cpus, ok := expected[expectedRel]
	if !ok {
		t.Fatalf("expected key %q not found; keys=%v", expectedRel, keysOf(expected))
	}
	if got := cpus.String(); got != "0-1" {
		t.Fatalf("cpuset @ %s = %q, want 0-1", expectedRel, got)
	}
}

func keysOf(m map[string]machine.CPUSet) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func totalApplyCount(counts map[string]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}
