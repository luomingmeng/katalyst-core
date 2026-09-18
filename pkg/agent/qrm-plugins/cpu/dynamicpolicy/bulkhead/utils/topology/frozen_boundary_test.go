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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestCompileFrozenBoundaryV1ClassifiesControlledDirectChildrenAndRelevantHolders(t *testing.T) {
	snapshot, input, phases := frozenBoundaryFixture()

	boundary, err := compileFrozenBoundaryV1(snapshot, input, phases)

	require.NoError(t, err)
	require.Equal(t, FrozenBoundaryVersionV1, boundary.Version)
	require.Equal(t, []string{"root"}, boundary.Roots)
	require.Equal(t, []string{"root"}, boundary.ControlledRels)
	require.Equal(t, []ChildRef{{
		Name:     "direct",
		Identity: CgroupIdentity{Device: 1, Inode: 2},
	}}, boundary.DirectChildrenByRel["root"])
	require.Equal(t, machine.MustParse("0-3"), boundary.RelevantCPUs)
	require.Equal(t, []string{"root/direct", "root/direct/holder"}, boundary.RelevantCPUHolders)
}

func TestCompileFrozenBoundaryV1ExcludesUnrelatedDynamicSibling(t *testing.T) {
	snapshot, input, phases := frozenBoundaryFixture()

	boundary, err := compileFrozenBoundaryV1(snapshot, input, phases)

	require.NoError(t, err)
	require.NotContains(t, boundary.RelevantCPUHolders, "root/direct/unrelated")
}

func TestValidateFrozenBoundaryRejectsUnknownVersion(t *testing.T) {
	snapshot, input, phases := frozenBoundaryFixture()
	boundary, err := compileFrozenBoundaryV1(snapshot, input, phases)
	require.NoError(t, err)
	boundary.Version = FrozenBoundaryVersion(99)

	require.Error(t, validateFrozenBoundary(boundary, snapshot))
}

func TestCloneFrozenBoundaryIsDeeplyIsolated(t *testing.T) {
	snapshot, input, phases := frozenBoundaryFixture()
	boundary, err := compileFrozenBoundaryV1(snapshot, input, phases)
	require.NoError(t, err)

	cloned := cloneFrozenBoundary(boundary)
	boundary.Roots[0] = "changed"
	boundary.ControlledRels[0] = "changed"
	boundary.DirectChildrenByRel["root"][0].Name = "changed"
	boundary.RelevantCPUHolders[0] = "changed"
	boundary.RelevantCPUs = machine.NewCPUSet(99)

	require.Equal(t, []string{"root"}, cloned.Roots)
	require.Equal(t, []string{"root"}, cloned.ControlledRels)
	require.Equal(t, "direct", cloned.DirectChildrenByRel["root"][0].Name)
	require.Equal(t, []string{"root/direct", "root/direct/holder"}, cloned.RelevantCPUHolders)
	require.Equal(t, "0-3", cloned.RelevantCPUs.String())
}

func TestEvaluateFrozenBoundaryAllowsUnrelatedDynamicSiblingChurn(t *testing.T) {
	snapshot, input, phases := frozenBoundaryFixture()
	boundary, err := compileFrozenBoundaryV1(snapshot, input, phases)
	require.NoError(t, err)
	driver, dag := frozenBoundaryDriver(t, snapshot, input.DAGSpecs)
	driver.nodes["root/direct/unrelated"].configuredCPUs = machine.NewCPUSet(8)
	driver.nodes["root/direct/unrelated"].cpus = machine.NewCPUSet(8)
	driver.nodes["root/direct/unrelated"].configuredMems = "1"
	driver.nodes["root/direct/unrelated"].mems = "1"
	driver.add("root/direct/new-unrelated", CgroupIdentity{Device: 1, Inode: 9}, "9", "1")

	evaluation, err := EvaluateFrozenBoundary(
		context.Background(), driver, dag, NewBudgetTracker(ConvergenceBudget{}),
		boundary, snapshot)

	require.NoError(t, err)
	require.NotNil(t, evaluation.Snapshot)
	require.NotContains(t, evaluation.Snapshot.Entries, "root/direct/new-unrelated")
}

func TestEvaluateFrozenBoundaryRejectsControlledRelDrift(t *testing.T) {
	snapshot, input, phases := frozenBoundaryFixture()
	boundary, err := compileFrozenBoundaryV1(snapshot, input, phases)
	require.NoError(t, err)
	driver, dag := frozenBoundaryDriver(t, snapshot, input.DAGSpecs)
	driver.nodes["root"].configuredCPUs = machine.MustParse("0-2")
	driver.nodes["root"].cpus = machine.MustParse("0-2")

	_, err = EvaluateFrozenBoundary(
		context.Background(), driver, dag, NewBudgetTracker(ConvergenceBudget{}),
		boundary, snapshot)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrCoordinatorPlanStale)
}

func TestEvaluateFrozenBoundaryRejectsDirectChildChurn(t *testing.T) {
	snapshot, input, phases := frozenBoundaryFixture()
	boundary, err := compileFrozenBoundaryV1(snapshot, input, phases)
	require.NoError(t, err)
	driver, dag := frozenBoundaryDriver(t, snapshot, input.DAGSpecs)
	driver.add("root/new-direct", CgroupIdentity{Device: 1, Inode: 9}, "9", "1")

	_, err = EvaluateFrozenBoundary(
		context.Background(), driver, dag, NewBudgetTracker(ConvergenceBudget{}),
		boundary, snapshot)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrCoordinatorPlanStale)
}

func TestEvaluateFrozenBoundaryRejectsRelevantCPUHolderDrift(t *testing.T) {
	snapshot, input, phases := frozenBoundaryFixture()
	boundary, err := compileFrozenBoundaryV1(snapshot, input, phases)
	require.NoError(t, err)
	driver, dag := frozenBoundaryDriver(t, snapshot, input.DAGSpecs)
	driver.nodes["root/direct/holder"].configuredCPUs = machine.NewCPUSet(2)
	driver.nodes["root/direct/holder"].cpus = machine.NewCPUSet(2)

	_, err = EvaluateFrozenBoundary(
		context.Background(), driver, dag, NewBudgetTracker(ConvergenceBudget{}),
		boundary, snapshot)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrCoordinatorPlanStale)
}

func TestEvaluateFrozenBoundaryRejectsNewRelevantCPUHolder(t *testing.T) {
	snapshot, input, phases := frozenBoundaryFixture()
	boundary, err := compileFrozenBoundaryV1(snapshot, input, phases)
	require.NoError(t, err)
	driver, dag := frozenBoundaryDriver(t, snapshot, input.DAGSpecs)
	driver.add("root/direct/new-holder", CgroupIdentity{Device: 1, Inode: 9}, "0", "0")

	_, err = EvaluateFrozenBoundary(
		context.Background(), driver, dag, NewBudgetTracker(ConvergenceBudget{}),
		boundary, snapshot)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrCoordinatorPlanStale)
}

func frozenBoundaryDriver(
	t *testing.T,
	snapshot *CompleteSnapshot,
	specs []NodeSpec,
) (*fakeHierarchyDriver, *TopoDAG) {
	t.Helper()
	driver := newFakeHierarchyDriver()
	driver.capabilities = snapshot.Capabilities
	rels := sortedStringKeys(snapshot.Entries)
	for _, rel := range rels {
		entry := snapshot.Entries[rel]
		driver.add(rel, entry.Identity, entry.CPUs.String(), entry.Mems)
		driver.nodes[rel].configuredCPUs = entry.ConfiguredCPUs.Clone()
		driver.nodes[rel].configuredMems = entry.ConfiguredMems
	}
	dag, err := BuildDAG(specs)
	require.NoError(t, err)
	return driver, dag
}

func frozenBoundaryFixture() (*CompleteSnapshot, FrozenCoordinatorEvaluationInput, []CompiledPhase) {
	identities := map[string]CgroupIdentity{
		"root":                  {Device: 1, Inode: 1},
		"root/direct":           {Device: 1, Inode: 2},
		"root/direct/holder":    {Device: 1, Inode: 3},
		"root/direct/unrelated": {Device: 1, Inode: 4},
	}
	entries := map[string]EntryState{
		"root": {
			Rel: "root", Identity: identities["root"],
			CPUs: machine.MustParse("0-3"), ConfiguredCPUs: machine.MustParse("0-3"),
			Mems: "0", ConfiguredMems: "0",
		},
		"root/direct": {
			Rel: "root/direct", Identity: identities["root/direct"],
			CPUs: machine.MustParse("0-3"), ConfiguredCPUs: machine.MustParse("0-3"),
			Mems: "0", ConfiguredMems: "0",
		},
		"root/direct/holder": {
			Rel: "root/direct/holder", Identity: identities["root/direct/holder"],
			CPUs: machine.NewCPUSet(1), ConfiguredCPUs: machine.NewCPUSet(1),
			Mems: "0", ConfiguredMems: "0",
		},
		"root/direct/unrelated": {
			Rel: "root/direct/unrelated", Identity: identities["root/direct/unrelated"],
			CPUs: machine.NewCPUSet(9), ConfiguredCPUs: machine.NewCPUSet(9),
			Mems: "0", ConfiguredMems: "0",
		},
	}
	snapshot := &CompleteSnapshot{
		Capabilities: v2Capabilities(),
		Entries:      entries,
		Children: map[string][]ChildRef{
			"root": {{Name: "direct", Identity: identities["root/direct"]}},
			"root/direct": {
				{Name: "holder", Identity: identities["root/direct/holder"]},
				{Name: "unrelated", Identity: identities["root/direct/unrelated"]},
			},
		},
		DomainByRel: map[string]DomainID{
			"root":                  DomainPrimary,
			"root/direct":           DomainPrimary,
			"root/direct/holder":    DomainPrimary,
			"root/direct/unrelated": DomainPrimary,
		},
		DomainUnion: map[DomainID]machine.CPUSet{
			DomainPrimary: machine.MustParse("0-3"),
		},
		ScanBoundary: ScanBoundary{
			Purpose: ScanForPlan,
			Roots:   []string{"root"},
			ExpandedRels: []string{
				"root",
				"root/direct",
				"root/direct/holder",
				"root/direct/unrelated",
			},
		},
	}
	snapshot.ID = fingerprintSnapshot(snapshot)
	input := FrozenCoordinatorEvaluationInput{
		DAGSpecs: []NodeSpec{{
			Rel: "root", Role: TopoNodeRolePrimary, Domain: DomainPrimary,
			ControlledRoot: true,
		}},
		RequiredByRel:           map[string]machine.CPUSet{"root": machine.NewCPUSet(0)},
		ParentSafetyTargetByRel: map[string]machine.CPUSet{"root": machine.MustParse("0-1")},
		Capabilities:            snapshot.Capabilities,
	}
	phases := []CompiledPhase{{
		Kind: PhaseDrain,
		Operations: []PlanOperation{{
			Rel:             "root",
			ExpectedCurrent: CPUSetTarget{CPUs: machine.MustParse("0-3"), Mems: "0"},
			Target:          CPUSetTarget{CPUs: machine.MustParse("0-1"), Mems: "0"},
		}},
	}}
	return snapshot, input, phases
}
