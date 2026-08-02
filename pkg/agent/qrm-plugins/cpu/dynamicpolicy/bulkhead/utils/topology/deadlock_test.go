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
	"context"
	"errors"
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestBuildPhasePlanDetectsPrimaryReclaimSingletonV1Deadlock(t *testing.T) {
	input := primaryReclaimSwapInput(t, machine.NewCPUSet(0), machine.NewCPUSet(1))
	_, err := BuildPhasePlan(input)
	var structural *StructuralV1NonEmptyDeadlock
	if !errors.As(err, &structural) {
		t.Fatalf("BuildPhasePlan error = %v, want StructuralV1NonEmptyDeadlock", err)
	}
	if structural.Analysis.Completeness != ProbeComplete || len(structural.Analysis.Atoms) != 2 {
		t.Fatalf("analysis = %+v, want complete two-atom proof", structural.Analysis)
	}
}

func TestBuildPhasePlanFindsSafeSeedWhenOneSideCanRelease(t *testing.T) {
	input := primaryReclaimSwapInput(t, machine.NewCPUSet(0, 2), machine.NewCPUSet(1))
	input.DesiredByRel["primary"] = machine.NewCPUSet(1, 2)
	plan, err := BuildPhasePlan(input)
	if err != nil {
		t.Fatalf("BuildPhasePlan: %v", err)
	}
	if len(plan.Operations) == 0 {
		t.Fatal("safe multi-CPU seed should produce a drain operation")
	}
}

func TestBuildPhasePlanUsesProvenSafeSeedInsteadOfArbitraryHeldCPUs(t *testing.T) {
	dag := mustPlanDAG(t, []NodeSpec{
		{Rel: "primary-a", Role: TopoNodeRolePrimary, Domain: DomainPrimary, CPUs: machine.NewCPUSet(3), TrustAnchor: true},
		{Rel: "primary-b", Role: TopoNodeRolePrimary, Domain: DomainPrimary, CPUs: machine.NewCPUSet(3), TrustAnchor: true},
		{Rel: "primary-c", Role: TopoNodeRolePrimary, Domain: DomainPrimary, CPUs: machine.NewCPUSet(4), TrustAnchor: true},
		{Rel: "reclaim", Role: TopoNodeRoleReclaim, Domain: DomainReclaim, CPUs: machine.NewCPUSet(0, 1, 2), TrustAnchor: true},
	})
	snapshot := planSnapshot(map[string]EntryState{
		"primary-a": {Identity: CgroupIdentity{Inode: 1}, CPUs: machine.NewCPUSet(0)},
		"primary-b": {Identity: CgroupIdentity{Inode: 2}, CPUs: machine.NewCPUSet(1)},
		"primary-c": {Identity: CgroupIdentity{Inode: 3}, CPUs: machine.NewCPUSet(2, 4)},
		"reclaim":   {Identity: CgroupIdentity{Inode: 4}, CPUs: machine.NewCPUSet(3)},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: machine.NewCPUSet(0, 1, 2, 4),
		DomainReclaim: machine.NewCPUSet(3),
	})
	snapshot.DomainByRel = map[string]DomainID{
		"primary-a": DomainPrimary,
		"primary-b": DomainPrimary,
		"primary-c": DomainPrimary,
		"reclaim":   DomainReclaim,
	}
	snapshot.ID = fingerprintSnapshot(snapshot)

	plan, err := BuildPhasePlan(PhasePlanInput{
		Kind: PhaseDrain, DAG: dag, Snapshot: snapshot,
		DesiredByRel: map[string]machine.CPUSet{
			"primary-a": machine.NewCPUSet(3),
			"primary-b": machine.NewCPUSet(3),
			"primary-c": machine.NewCPUSet(4),
			"reclaim":   machine.NewCPUSet(0, 1, 2),
		},
		AllowedCPUs: machine.NewCPUSet(0, 1, 2, 3, 4),
		Selection: DrainSelectionPolicy{
			MaxCPUsDrainRatio:         0.5,
			RequirePairedSwapProgress: true,
		},
		Budget: NewBudgetTracker(ConvergenceBudget{}),
	})
	if err != nil {
		t.Fatalf("BuildPhasePlan: %v", err)
	}
	if !plan.DrainBatch[DomainPrimary].Contains(2) {
		t.Fatalf("primary drain batch = %s, want proven safe seed CPU 2", plan.DrainBatch[DomainPrimary].String())
	}
	if got := plan.TargetByRel["primary-c"].CPUs; got.Contains(2) {
		t.Fatalf("primary-c drain target = %s, want safe seed CPU 2 released", got.String())
	}
}

func TestDeadlockAnalysisDoesNotCallPendingProtectionStructural(t *testing.T) {
	input := primaryReclaimSwapInput(t, machine.NewCPUSet(0), machine.NewCPUSet(1))
	input.ProtectedByRel = map[string]machine.CPUSet{"primary": machine.NewCPUSet(0)}
	_, err := BuildPhasePlan(input)
	var structural *StructuralV1NonEmptyDeadlock
	if errors.As(err, &structural) {
		t.Fatalf("pending protection must not be classified structural: %v", err)
	}
}

func TestDeadlockAnalysisDoesNotCallUnmaterializedPendingProtectionStructural(t *testing.T) {
	input := primaryReclaimSwapInput(t, machine.NewCPUSet(0), machine.NewCPUSet(1))
	input.ProtectedPending = machine.NewCPUSet(0)
	_, err := BuildPhasePlan(input)
	var structural *StructuralV1NonEmptyDeadlock
	if errors.As(err, &structural) {
		t.Fatalf("unmaterialized pending protection must not be classified structural: %v", err)
	}
}

func TestDeadlockAnalysisBudgetExhaustionFailsClosed(t *testing.T) {
	input := primaryReclaimSwapInput(t, machine.NewCPUSet(0), machine.NewCPUSet(1))
	input.Budget = NewBudgetTracker(ConvergenceBudget{MaxDeadlockProbeOperations: 2})
	analysis, err := analyzeV1Deadlock(input)
	if !errors.Is(err, ErrDeadlockProbeBudgetExceeded) {
		t.Fatalf("analyzeV1Deadlock error = %v, want ErrDeadlockProbeBudgetExceeded", err)
	}
	if analysis.Completeness != ProbeIndeterminate {
		t.Fatalf("completeness = %s, want %s", analysis.Completeness, ProbeIndeterminate)
	}
	if analysis.SafeSeed != nil {
		t.Fatalf("safe seed = %+v, want nil under incomplete probe", analysis.SafeSeed)
	}
	if after := input.Budget.Usage(); after.DeadlockProbeOperations != 2 {
		t.Fatalf("deadlock probe operations = %d, want shared-budget limit 2", after.DeadlockProbeOperations)
	}

	_, err = BuildPhasePlan(input)
	if !errors.Is(err, ErrDeadlockProbeBudgetExceeded) {
		t.Fatalf("BuildPhasePlan error = %v, want fail-closed deadlock probe budget error", err)
	}
}

func TestDeadlockAnalysisHonorsCanceledContext(t *testing.T) {
	input := primaryReclaimSwapInput(t, machine.NewCPUSet(0), machine.NewCPUSet(1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	input.Context = ctx
	input.Budget = NewBudgetTracker(ConvergenceBudget{MaxDeadlockProbeOperations: 16})

	_, err := analyzeV1Deadlock(input)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("analyzeV1Deadlock error = %v, want context.Canceled", err)
	}
}

func TestBuildPhasePlanDoesNotCallCycleStructuralWhenFinalDesiredCPUIsVerifiedUnowned(t *testing.T) {
	input := primaryReclaimSwapInput(t, machine.NewCPUSet(0), machine.NewCPUSet(1))
	input.DesiredByRel["primary"] = machine.NewCPUSet(1, 2)

	plan, err := BuildPhasePlan(input)
	if err != nil {
		t.Fatalf("BuildPhasePlan with legal final grow anchor: %v", err)
	}
	if len(plan.Operations) != 0 {
		t.Fatalf("drain operations = %#v, want no drain before verified-unowned grow anchor", plan.Operations)
	}
}

func TestDeadlockAnalysisAppliesNUMABucketConstraintToAggregateEdge(t *testing.T) {
	dag := mustPlanDAG(t, []NodeSpec{
		{Rel: "primary", Role: TopoNodeRolePrimary, Domain: DomainPrimary, CPUs: machine.NewCPUSet(1), TrustAnchor: true},
		{Rel: "reclaim", Role: TopoNodeRoleReclaim, Domain: DomainReclaim, CPUs: machine.NewCPUSet(0, 2), TrustAnchor: true},
		{
			Rel: "reclaim/bucket-0", ParentRel: "reclaim", Role: TopoNodeRoleReclaimNUMABucket,
			Domain: DomainReclaim, CPUs: machine.NewCPUSet(0),
			Constraint: TopologyConstraint{CPUUpperBound: machine.NewCPUSet(0), Scope: TopologyScopeNUMANode},
		},
		{
			Rel: "reclaim/bucket-1", ParentRel: "reclaim", Role: TopoNodeRoleReclaimNUMABucket,
			Domain: DomainReclaim, CPUs: machine.NewCPUSet(2),
			Constraint: TopologyConstraint{CPUUpperBound: machine.NewCPUSet(2), Scope: TopologyScopeNUMANode},
		},
	})
	snapshot := planSnapshot(map[string]EntryState{
		"primary":          {CPUs: machine.NewCPUSet(0)},
		"reclaim":          {CPUs: machine.NewCPUSet(1, 2)},
		"reclaim/bucket-0": {CPUs: machine.NewCPUSet(1)},
		"reclaim/bucket-1": {CPUs: machine.NewCPUSet(2)},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: machine.NewCPUSet(0), DomainReclaim: machine.NewCPUSet(1, 2),
	})
	snapshot.Children = map[string][]ChildRef{
		"reclaim": {{Name: "bucket-0"}, {Name: "bucket-1"}},
	}
	snapshot.DomainByRel = map[string]DomainID{
		"primary": DomainPrimary, "reclaim": DomainReclaim,
		"reclaim/bucket-0": DomainReclaim, "reclaim/bucket-1": DomainReclaim,
	}
	snapshot.ID = fingerprintSnapshot(snapshot)
	_, err := BuildPhasePlan(PhasePlanInput{
		Kind: PhaseDrain, DAG: dag, Snapshot: snapshot,
		DesiredByRel: map[string]machine.CPUSet{
			"primary": machine.NewCPUSet(1), "reclaim": machine.NewCPUSet(0, 2),
			"reclaim/bucket-0": machine.NewCPUSet(0), "reclaim/bucket-1": machine.NewCPUSet(2),
		},
		AllowedCPUs: machine.NewCPUSet(0, 1, 2),
		Budget:      NewBudgetTracker(ConvergenceBudget{}),
	})
	var structural *StructuralV1NonEmptyDeadlock
	if !errors.As(err, &structural) {
		t.Fatalf("BuildPhasePlan error = %v, want bucket-constrained structural deadlock", err)
	}
}

func TestStructuralV1DeadlockRequiresEveryAtomToBeV1EmptyBlocked(t *testing.T) {
	analysis := DeadlockAnalysis{
		Completeness: ProbeComplete,
		Atoms: []DrainAtom{
			{Source: DomainPrimary, Destination: DomainReclaim, CPUs: machine.NewCPUSet(0)},
			{Source: DomainReclaim, Destination: DomainPrimary, CPUs: machine.NewCPUSet(1)},
		},
		AtomClasses: []DrainAtomClass{
			DrainAtomClassV1Empty,
			DrainAtomClassHeld,
		},
		EmptyBlockers: map[string]machine.CPUSet{
			"primary": machine.NewCPUSet(0),
		},
	}
	if structuralV1Deadlock(analysis) {
		t.Fatal("mixed atom blockers must not be classified as an all-v1-empty structural deadlock")
	}
}

func TestStructuralV1DeadlockRejectsProtectedAtom(t *testing.T) {
	analysis := DeadlockAnalysis{
		Completeness: ProbeComplete,
		Atoms: []DrainAtom{
			{Source: DomainPrimary, Destination: DomainReclaim, CPUs: machine.NewCPUSet(0)},
			{Source: DomainReclaim, Destination: DomainPrimary, CPUs: machine.NewCPUSet(1)},
		},
		AtomClasses: []DrainAtomClass{
			DrainAtomClassV1Empty,
			DrainAtomClassProtected,
		},
		EmptyBlockers: map[string]machine.CPUSet{
			"primary": machine.NewCPUSet(0),
		},
		Protected: machine.NewCPUSet(1),
	}
	if structuralV1Deadlock(analysis) {
		t.Fatal("protected atom must not be classified as an all-v1-empty structural deadlock")
	}
}

func primaryReclaimSwapInput(t *testing.T, primary, reclaim machine.CPUSet) PhasePlanInput {
	t.Helper()
	dag := mustPlanDAG(t, []NodeSpec{
		{Rel: "primary", Role: TopoNodeRolePrimary, Domain: DomainPrimary, CPUs: reclaim, TrustAnchor: true},
		{Rel: "reclaim", Role: TopoNodeRoleReclaim, Domain: DomainReclaim, CPUs: primary, TrustAnchor: true},
	})
	snapshot := planSnapshot(map[string]EntryState{
		"primary": {Identity: CgroupIdentity{Inode: 1}, CPUs: primary},
		"reclaim": {Identity: CgroupIdentity{Inode: 2}, CPUs: reclaim},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: primary, DomainReclaim: reclaim,
	})
	snapshot.DomainByRel = map[string]DomainID{
		"primary": DomainPrimary, "reclaim": DomainReclaim,
	}
	snapshot.ID = fingerprintSnapshot(snapshot)
	return PhasePlanInput{
		Kind: PhaseDrain, DAG: dag, Snapshot: snapshot,
		DesiredByRel: map[string]machine.CPUSet{
			"primary": reclaim, "reclaim": primary,
		},
		AllowedCPUs: primary.Union(reclaim).Union(machine.NewCPUSet(2)),
		Selection: DrainSelectionPolicy{
			RequirePairedSwapProgress: true,
		},
		Budget: NewBudgetTracker(ConvergenceBudget{}),
	}
}
