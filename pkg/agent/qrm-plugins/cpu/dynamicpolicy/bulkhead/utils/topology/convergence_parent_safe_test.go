/*
Copyright 2026 The Katalyst Authors.

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

package topology

import (
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestBuildParentSafetyReportAllowsOnlySafeDeferredLeafSuperset(t *testing.T) {
	t.Parallel()

	dag := mustPlanDAG(t, []NodeSpec{
		{Rel: "primary", Domain: DomainPrimary, CPUs: machine.NewCPUSet(0, 1), TrustAnchor: true},
		{Rel: "reclaim", Domain: DomainReclaim, CPUs: machine.NewCPUSet(2, 3), TrustAnchor: true},
	})
	snapshot := planSnapshot(map[string]EntryState{
		"primary":               {Identity: CgroupIdentity{Inode: 1}, CPUs: machine.NewCPUSet(0, 1)},
		"primary/pod":           {Identity: CgroupIdentity{Inode: 2}, CPUs: machine.NewCPUSet(0, 1)},
		"primary/pod/container": {Identity: CgroupIdentity{Inode: 3}, CPUs: machine.NewCPUSet(0, 1)},
		"reclaim":               {Identity: CgroupIdentity{Inode: 4}, CPUs: machine.NewCPUSet(2, 3)},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: machine.NewCPUSet(0, 1),
		DomainReclaim: machine.NewCPUSet(2, 3),
	})
	snapshot.Children = map[string][]ChildRef{
		"primary":     {{Name: "pod"}},
		"primary/pod": {{Name: "container"}},
	}
	report := ConvergenceReport{
		NonConvergedTargets: []RelConvergence{{
			Rel: "primary/pod/container", Observed: machine.NewCPUSet(0, 1),
			Target: machine.NewCPUSet(1), Reason: "dynamic_target_mismatch",
		}},
	}

	got := buildParentSafetyReport(
		snapshot, dag,
		map[string]machine.CPUSet{"primary": machine.NewCPUSet(0, 1), "reclaim": machine.NewCPUSet(2, 3)},
		report, machine.NewCPUSet(1),
		map[string]machine.CPUSet{"primary/pod/container": machine.NewCPUSet(1)},
		nil,
		HierarchyCapabilities{},
	)
	if !got.Safe {
		t.Fatalf("parent safety report = %+v, want safe", got)
	}

	snapshot.DomainUnion[DomainReclaim] = machine.NewCPUSet(1, 2, 3)
	got = buildParentSafetyReport(
		snapshot, dag,
		map[string]machine.CPUSet{"primary": machine.NewCPUSet(0, 1), "reclaim": machine.NewCPUSet(2, 3)},
		report, machine.NewCPUSet(1),
		map[string]machine.CPUSet{"primary/pod/container": machine.NewCPUSet(1)},
		nil,
		HierarchyCapabilities{},
	)
	if got.Safe || !got.PendingInsideReclaim.Equals(machine.NewCPUSet(1)) {
		t.Fatalf("parent safety report = %+v, want pending/reclaim overlap rejected", got)
	}

	freshProof, err := evaluateCoordinatorSnapshot(
		snapshot,
		dag,
		map[string]machine.CPUSet{"primary": machine.NewCPUSet(0, 1), "reclaim": machine.NewCPUSet(2, 3)},
		nil,
		map[DomainID]machine.CPUSet{
			DomainPrimary: machine.NewCPUSet(0, 1),
			DomainReclaim: machine.NewCPUSet(2, 3),
		},
		machine.NewCPUSet(0, 1, 2, 3),
		nil,
		map[string]machine.CPUSet{"primary/pod/container": machine.NewCPUSet(1)},
		nil,
		machine.NewCPUSet(1),
		HierarchyCapabilities{},
		false,
	)
	if err != nil {
		t.Fatalf("evaluateCoordinatorSnapshot(fresh) error = %v", err)
	}
	if freshProof.ParentSafety.Safe {
		t.Fatalf("fresh proof = %+v, want fresh reclaim overlap to invalidate the earlier parent-safe decision", freshProof)
	}
}

func TestParentSafetyAllowsPlannerDeferredCleanupMismatch(t *testing.T) {
	t.Parallel()

	dag := mustPlanDAG(t, []NodeSpec{
		{Rel: "primary", Domain: DomainPrimary, CPUs: machine.NewCPUSet(0, 1), TrustAnchor: true},
		{Rel: "reclaim", Domain: DomainReclaim, CPUs: machine.NewCPUSet(2, 3), TrustAnchor: true},
	})
	snapshot := planSnapshot(map[string]EntryState{
		"primary": {Identity: CgroupIdentity{Inode: 1}, CPUs: machine.NewCPUSet(0, 1)},
		"reclaim": {Identity: CgroupIdentity{Inode: 2}, CPUs: machine.NewCPUSet(2, 3)},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: machine.NewCPUSet(0, 1),
		DomainReclaim: machine.NewCPUSet(2, 3),
	})
	report := ConvergenceReport{NonConvergedTargets: []RelConvergence{{
		Rel: "reclaim", Observed: machine.NewCPUSet(2, 3), Target: machine.NewCPUSet(2),
		Reason: convergenceReasonTargetMismatch,
	}}}

	got := buildParentSafetyReport(
		snapshot, dag, nil, report, machine.NewCPUSet(1), nil,
		map[string]struct{}{"reclaim": {}}, HierarchyCapabilities{},
	)
	if !got.Safe || len(got.DeferredLeafMismatches) != 1 {
		t.Fatalf("parent safety report = %+v, want planner-deferred cleanup accepted", got)
	}
}

func TestParentSafetyAllowsDeferredLeafRelocationInsidePrimary(t *testing.T) {
	t.Parallel()

	dag := mustPlanDAG(t, []NodeSpec{
		{Rel: "primary", Domain: DomainPrimary, CPUs: machine.NewCPUSet(0, 1), TrustAnchor: true},
		{Rel: "reclaim", Domain: DomainReclaim, CPUs: machine.NewCPUSet(2, 3), TrustAnchor: true},
	})
	snapshot := planSnapshot(map[string]EntryState{
		"primary":               {Identity: CgroupIdentity{Inode: 1}, CPUs: machine.NewCPUSet(0, 1)},
		"primary/pod/container": {Identity: CgroupIdentity{Inode: 2}, CPUs: machine.NewCPUSet(0)},
		"reclaim":               {Identity: CgroupIdentity{Inode: 3}, CPUs: machine.NewCPUSet(2, 3)},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: machine.NewCPUSet(0, 1),
		DomainReclaim: machine.NewCPUSet(2, 3),
	})
	report := ConvergenceReport{NonConvergedTargets: []RelConvergence{{
		Rel: "primary/pod/container", Observed: machine.NewCPUSet(0),
		Target: machine.NewCPUSet(1), Reason: convergenceReasonTargetMismatch,
	}}}

	got := buildParentSafetyReport(
		snapshot, dag, nil, report, machine.NewCPUSet(),
		map[string]machine.CPUSet{"primary/pod/container": machine.NewCPUSet(1)},
		nil, HierarchyCapabilities{},
	)
	if !got.Safe || len(got.DeferredLeafMismatches) != 1 {
		t.Fatalf("parent safety report = %+v, want primary-internal relocation deferred", got)
	}

	snapshot.DomainUnion[DomainReclaim] = machine.NewCPUSet(0, 2, 3)
	got = buildParentSafetyReport(
		snapshot, dag, nil, report, machine.NewCPUSet(),
		map[string]machine.CPUSet{"primary/pod/container": machine.NewCPUSet(1)},
		nil, HierarchyCapabilities{},
	)
	if got.Safe {
		t.Fatalf("parent safety report = %+v, want observed reclaim overlap rejected", got)
	}

	snapshot.DomainUnion[DomainReclaim] = machine.NewCPUSet(1, 2, 3)
	got = buildParentSafetyReport(
		snapshot, dag, nil, report, machine.NewCPUSet(),
		map[string]machine.CPUSet{"primary/pod/container": machine.NewCPUSet(1)},
		nil, HierarchyCapabilities{},
	)
	if got.Safe {
		t.Fatalf("parent safety report = %+v, want target reclaim overlap rejected", got)
	}
}
