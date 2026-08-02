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

package topology

import (
	"errors"
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestProjectDrainTargetsMatchesPlannerAndReportsDomainUnion(t *testing.T) {
	dag := mustPlanDAG(t, []NodeSpec{
		{Rel: "primary", Role: TopoNodeRolePrimary, Domain: DomainPrimary, CPUs: machine.NewCPUSet(1), TrustAnchor: true},
		{Rel: "reclaim", Role: TopoNodeRoleReclaim, Domain: DomainReclaim, CPUs: machine.NewCPUSet(0), TrustAnchor: true},
	})
	snapshot := planSnapshot(map[string]EntryState{
		"primary": {CPUs: machine.NewCPUSet(0, 1)},
		"reclaim": {CPUs: machine.NewCPUSet(2)},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: machine.NewCPUSet(0, 1),
		DomainReclaim: machine.NewCPUSet(2),
	})
	snapshot.DomainByRel = map[string]DomainID{
		"primary": DomainPrimary, "reclaim": DomainReclaim,
	}
	input := PhasePlanInput{
		Kind: PhaseDrain, DAG: dag, Snapshot: snapshot,
		DesiredByRel: map[string]machine.CPUSet{
			"primary": machine.NewCPUSet(1), "reclaim": machine.NewCPUSet(0),
		},
		AllowedCPUs: machine.NewCPUSet(0, 1, 2),
		Budget:      NewBudgetTracker(ConvergenceBudget{}),
	}
	plan, err := BuildPhasePlan(input)
	if err != nil {
		t.Fatalf("BuildPhasePlan: %v", err)
	}
	depth := buildSnapshotDepthByRel(snapshot, nil)
	domainByRel, parentByRel := buildPlannerRelations(snapshot, dag, depth, nil)
	projection, err := projectDrainTargets(DrainProjectionInput{
		PlanInput:       input,
		DrainBatch:      plan.DrainBatch,
		LeavingByDomain: map[DomainID]machine.CPUSet{DomainPrimary: machine.NewCPUSet(0), DomainReclaim: machine.NewCPUSet(2)},
		DomainByRel:     domainByRel,
		ParentByRel:     parentByRel,
		DepthByRel:      depth,
	})
	if err != nil {
		t.Fatalf("projectDrainTargets: %v", err)
	}
	for rel, want := range plan.TargetByRel {
		if got := projection.TargetByRel[rel]; !got.CPUs.Equals(want.CPUs) {
			t.Fatalf("%s projected target = %s, planner = %s", rel, got.CPUs.String(), want.CPUs.String())
		}
	}
	if got := projection.DomainUnion[DomainPrimary]; !got.Equals(machine.NewCPUSet(1)) {
		t.Fatalf("primary projected union = %s, want 1", got.String())
	}
	if projection.Cost.Rels != len(snapshot.Entries) {
		t.Fatalf("projection cost = %+v, want %d rels", projection.Cost, len(snapshot.Entries))
	}
}

func TestProjectDrainTargetsReportsV1NonEmptyBlocker(t *testing.T) {
	dag := mustPlanDAG(t, []NodeSpec{{
		Rel: "primary", Role: TopoNodeRolePrimary, Domain: DomainPrimary,
		CPUs: machine.NewCPUSet(1), TrustAnchor: true,
	}})
	snapshot := planSnapshot(map[string]EntryState{
		"primary": {CPUs: machine.NewCPUSet(0)},
	}, map[DomainID]machine.CPUSet{DomainPrimary: machine.NewCPUSet(0)})
	snapshot.DomainByRel = map[string]DomainID{"primary": DomainPrimary}
	input := PhasePlanInput{
		Kind: PhaseDrain, DAG: dag, Snapshot: snapshot,
		DesiredByRel: map[string]machine.CPUSet{"primary": machine.NewCPUSet(1)},
		AllowedCPUs:  machine.NewCPUSet(0, 1),
	}
	depth := buildSnapshotDepthByRel(snapshot, nil)
	domainByRel, parentByRel := buildPlannerRelations(snapshot, dag, depth, nil)
	projection, err := projectDrainTargets(DrainProjectionInput{
		PlanInput: input, DrainBatch: map[DomainID]machine.CPUSet{DomainPrimary: machine.NewCPUSet(0)},
		LeavingByDomain: map[DomainID]machine.CPUSet{DomainPrimary: machine.NewCPUSet(0)},
		DomainByRel:     domainByRel, ParentByRel: parentByRel, DepthByRel: depth,
	})
	if err != nil {
		t.Fatalf("projectDrainTargets: %v", err)
	}
	if got := projection.EmptyBlockers["primary"]; !got.Equals(machine.NewCPUSet(0)) {
		t.Fatalf("empty blocker = %s, want 0", got.String())
	}
}

func TestProjectDrainTargetsKeepsV1AnchorForObservedNUMAOverflow(t *testing.T) {
	upper := machine.NewCPUSet(0)
	overflow := machine.NewCPUSet(2)
	dag := mustPlanDAG(t, []NodeSpec{
		{Rel: "reclaim", Role: TopoNodeRoleReclaim, Domain: DomainReclaim, CPUs: upper, TrustAnchor: true},
		{
			Rel: "reclaim/bucket-0", ParentRel: "reclaim", Role: TopoNodeRoleReclaimNUMABucket,
			Domain: DomainReclaim, CPUs: upper,
			Constraint: TopologyConstraint{CPUUpperBound: upper, Scope: TopologyScopeNUMANode},
		},
	})
	snapshot := planSnapshot(map[string]EntryState{
		"reclaim":                {CPUs: overflow},
		"reclaim/bucket-0":       {CPUs: overflow},
		"reclaim/bucket-0/child": {CPUs: overflow},
	}, map[DomainID]machine.CPUSet{DomainReclaim: overflow})
	snapshot.Children = map[string][]ChildRef{
		"reclaim":          {{Name: "bucket-0"}},
		"reclaim/bucket-0": {{Name: "child"}},
	}
	snapshot.DomainByRel = map[string]DomainID{
		"reclaim": DomainReclaim, "reclaim/bucket-0": DomainReclaim,
		"reclaim/bucket-0/child": DomainReclaim,
	}
	input := PhasePlanInput{
		Kind: PhaseDrain, DAG: dag, Snapshot: snapshot,
		DesiredByRel: map[string]machine.CPUSet{
			"reclaim": upper, "reclaim/bucket-0": upper,
		},
		AllowedCPUs: machine.NewCPUSet(0, 2),
	}
	depth := buildSnapshotDepthByRel(snapshot, nil)
	domainByRel, parentByRel := buildPlannerRelations(snapshot, dag, depth, nil)
	projection, err := projectDrainTargets(DrainProjectionInput{
		PlanInput:       input,
		DrainBatch:      map[DomainID]machine.CPUSet{DomainReclaim: overflow},
		LeavingByDomain: map[DomainID]machine.CPUSet{DomainReclaim: overflow},
		DomainByRel:     domainByRel, ParentByRel: parentByRel, DepthByRel: depth,
	})
	if err != nil {
		t.Fatalf("projectDrainTargets: %v", err)
	}
	if got := projection.TargetByRel["reclaim/bucket-0/child"].CPUs; !got.Equals(overflow) {
		t.Fatalf("overflow child target = %s, want v1 hold/anchor %s", got.String(), overflow.String())
	}
}

func TestBuildPhasePlanChargesActualDrainProjectionCost(t *testing.T) {
	dag := mustPlanDAG(t, []NodeSpec{
		{Rel: "primary", Role: TopoNodeRolePrimary, Domain: DomainPrimary, CPUs: machine.NewCPUSet(0), TrustAnchor: true},
		{Rel: "reclaim", Role: TopoNodeRoleReclaim, Domain: DomainReclaim, CPUs: machine.NewCPUSet(1), TrustAnchor: true},
	})
	snapshot := planSnapshot(map[string]EntryState{
		"primary": {CPUs: machine.NewCPUSet(0)},
		"reclaim": {CPUs: machine.NewCPUSet(1)},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: machine.NewCPUSet(0),
		DomainReclaim: machine.NewCPUSet(1),
	})
	snapshot.DomainByRel = map[string]DomainID{
		"primary": DomainPrimary,
		"reclaim": DomainReclaim,
	}

	_, err := BuildPhasePlan(PhasePlanInput{
		Kind: PhaseDrain, DAG: dag, Snapshot: snapshot,
		DesiredByRel: map[string]machine.CPUSet{
			"primary": machine.NewCPUSet(0),
			"reclaim": machine.NewCPUSet(1),
		},
		AllowedCPUs: machine.NewCPUSet(0, 1),
		Budget: NewBudgetTracker(ConvergenceBudget{
			MaxPlanOperations: 1,
		}),
	})
	if !errors.Is(err, ErrPlanOperationBudgetExceeded) {
		t.Fatalf("BuildPhasePlan error = %v, want projection cost to exhaust %v", err, ErrPlanOperationBudgetExceeded)
	}
}
