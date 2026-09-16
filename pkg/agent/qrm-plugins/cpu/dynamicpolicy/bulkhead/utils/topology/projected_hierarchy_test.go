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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// completeSnapshotFixture builds a small but fully populated CompleteSnapshot so
// clone-isolation tests can mutate every reference-bearing field of the source
// and prove the clone is unaffected.
func completeSnapshotFixture(t *testing.T) *CompleteSnapshot {
	t.Helper()
	return &CompleteSnapshot{
		CapturedAt:   time.Unix(0, 0),
		Capabilities: HierarchyCapabilities{StableIdentity: true},
		Entries: map[string]EntryState{
			"kubepods": {
				Rel:            "kubepods",
				Identity:       CgroupIdentity{Device: 1, Inode: 1},
				CPUs:           machine.MustParse("0-3"),
				ConfiguredCPUs: machine.MustParse("0-3"),
				Mems:           "0",
				ConfiguredMems: "0",
			},
			"child": {
				Rel:            "child",
				Identity:       CgroupIdentity{Device: 1, Inode: 2},
				CPUs:           machine.MustParse("0-1"),
				ConfiguredCPUs: machine.MustParse("0-1"),
				Mems:           "0",
				ConfiguredMems: "0",
			},
		},
		Children: map[string][]ChildRef{
			"kubepods": {{Name: "child", Identity: CgroupIdentity{Device: 1, Inode: 2}}},
			"child":    {{Name: "grandchild", Identity: CgroupIdentity{Device: 1, Inode: 3}}},
		},
		DomainByRel: map[string]DomainID{
			"kubepods": DomainPrimary,
			"child":    DomainPrimary,
		},
		DomainUnion: map[DomainID]machine.CPUSet{
			DomainPrimary: machine.MustParse("0-3"),
		},
		ScanBoundary: ScanBoundary{Roots: []string{"kubepods"}},
	}
}

// TestCloneCompleteSnapshotIsDeeplyIsolated proves the clone shares no mutable
// map, slice, or CPUSet state with its source.
func TestCloneCompleteSnapshotIsDeeplyIsolated(t *testing.T) {
	original := completeSnapshotFixture(t)
	clone := CloneCompleteSnapshot(original)

	// Mutate an entry's effective CPUs in place; the clone must not observe it.
	original.Entries["child"].CPUs.Add(9)
	// Replace an entry wholesale.
	original.Entries["kubepods"] = EntryState{
		Rel:            "kubepods",
		Identity:       CgroupIdentity{Device: 1, Inode: 1},
		CPUs:           machine.NewCPUSet(9),
		ConfiguredCPUs: machine.NewCPUSet(9),
		Mems:           "1",
		ConfiguredMems: "1",
	}
	// Mutate children, domain maps, and domain unions.
	original.Children["child"] = []ChildRef{{Name: "changed"}}
	original.DomainByRel["child"] = DomainReclaim
	original.DomainUnion[DomainPrimary].Add(9)

	require.Equal(t, "0-1", clone.Entries["child"].CPUs.String())
	require.Equal(t, "0-3", clone.Entries["kubepods"].CPUs.String())
	require.Equal(t, "0", clone.Entries["kubepods"].ConfiguredMems)
	require.Equal(t, []ChildRef{{Name: "grandchild", Identity: CgroupIdentity{Device: 1, Inode: 3}}}, clone.Children["child"])
	require.Equal(t, DomainPrimary, clone.DomainByRel["child"])
	require.Equal(t, "0-3", clone.DomainUnion[DomainPrimary].String())
}

// TestCloneCompleteSnapshotNilReturnsNil documents the nil contract.
func TestCloneCompleteSnapshotNilReturnsNil(t *testing.T) {
	require.Nil(t, CloneCompleteSnapshot(nil))
}

// v1Capabilities and v2Capabilities expose the immutable cgroup-version
// semantics under stable identity so projection tests exercise the exact
// backend contracts the coordinator relies on.
func v1Capabilities() HierarchyCapabilities { return cgroupV1Policy.capabilities(true) }
func v2Capabilities() HierarchyCapabilities { return cgroupV2Policy.capabilities(true) }

const (
	projectedRootRel        = "kubepods"
	projectedChildRel       = "child"
	projectedInheritRel     = "kubepods/besteffort"
	projectedGrandchildRel  = "kubepods/besteffort/pod"
	projectedRootDeviceID   = uint64(1)
	projectedRootMemsDomain = "0"
)

// projectedHierarchyFixture builds a small three-level tree so both the
// capability semantics matrix and the recursive-inheritance tests operate on
// real parent, inherited descendant, and non-inherited leaf state.
//
//	kubepods (0-3)
//	├── child            (leaf used by the semantics matrix)
//	└── kubepods/besteffort        (empty-configured, inherits on v2)
//	    └── kubepods/besteffort/pod (empty-configured, inherits on v2)
func projectedHierarchyFixture(t *testing.T, capabilities HierarchyCapabilities) *projectedHierarchy {
	t.Helper()
	base := &CompleteSnapshot{
		CapturedAt:   time.Unix(0, 0),
		Capabilities: capabilities,
		Entries: map[string]EntryState{
			projectedRootRel: {
				Rel:            projectedRootRel,
				Identity:       CgroupIdentity{Device: projectedRootDeviceID, Inode: 1},
				CPUs:           machine.MustParse("0-3"),
				ConfiguredCPUs: machine.MustParse("0-3"),
				Mems:           projectedRootMemsDomain,
				ConfiguredMems: projectedRootMemsDomain,
			},
			projectedChildRel: {
				Rel:            projectedChildRel,
				Identity:       CgroupIdentity{Device: projectedRootDeviceID, Inode: 2},
				CPUs:           machine.NewCPUSet(),
				ConfiguredCPUs: machine.NewCPUSet(),
				Mems:           projectedRootMemsDomain,
				ConfiguredMems: projectedRootMemsDomain,
			},
			projectedInheritRel: {
				Rel:            projectedInheritRel,
				Identity:       CgroupIdentity{Device: projectedRootDeviceID, Inode: 3},
				CPUs:           machine.MustParse("0-3"),
				ConfiguredCPUs: machine.NewCPUSet(),
				Mems:           projectedRootMemsDomain,
				ConfiguredMems: "",
			},
			projectedGrandchildRel: {
				Rel:            projectedGrandchildRel,
				Identity:       CgroupIdentity{Device: projectedRootDeviceID, Inode: 4},
				CPUs:           machine.MustParse("0-3"),
				ConfiguredCPUs: machine.NewCPUSet(),
				Mems:           projectedRootMemsDomain,
				ConfiguredMems: "",
			},
		},
		Children: map[string][]ChildRef{
			projectedRootRel: {
				{Name: "child", Identity: CgroupIdentity{Device: projectedRootDeviceID, Inode: 2}},
				{Name: "besteffort", Identity: CgroupIdentity{Device: projectedRootDeviceID, Inode: 3}},
			},
			projectedInheritRel: {
				{Name: "pod", Identity: CgroupIdentity{Device: projectedRootDeviceID, Inode: 4}},
			},
		},
		DomainByRel: map[string]DomainID{
			projectedRootRel:       DomainPrimary,
			projectedChildRel:      DomainPrimary,
			projectedInheritRel:    DomainPrimary,
			projectedGrandchildRel: DomainPrimary,
		},
		DomainUnion:  map[DomainID]machine.CPUSet{DomainPrimary: machine.MustParse("0-3")},
		ScanBoundary: ScanBoundary{Purpose: ScanForPlan, Roots: []string{projectedRootRel}},
	}
	hierarchy, err := newProjectedHierarchy(base, capabilities)
	require.NoError(t, err)
	return hierarchy
}

// parentRelOf mirrors the fixture layout so operation builders can resolve the
// parent rel and its expected identity from the projected snapshot.
func parentRelOf(rel string) string {
	if idx := strings.LastIndex(rel, "/"); idx >= 0 {
		return rel[:idx]
	}
	return ""
}

// setParentTarget rewrites the root effective/configured CPUs so inheritance
// tests can drive descendants purely through the parent state. It is a
// test-only seeding helper on the production type.
func (h *projectedHierarchy) setParentTarget(cpus machine.CPUSet) {
	if !cpus.Initialed {
		return
	}
	root := h.snapshot.Entries[projectedRootRel]
	root.CPUs = cpus.Clone()
	root.ConfiguredCPUs = cpus.Clone()
	h.snapshot.Entries[projectedRootRel] = root
}

// cpuOperation seeds the target rel's current configured/effective state to
// `configured` and returns a CPU PlanOperation toward `target`. Seeding keeps
// applyOperation's ExpectedCurrent and identity validation meaningful while the
// test asserts only the projected result. It is a test-only method on the
// production type (the plan's free `cpuOperation` needs fixture state to seed a
// matching predecessor, so it is bound to the hierarchy here).
func (h *projectedHierarchy) cpuOperation(rel string, configured, target machine.CPUSet) PlanOperation {
	entry := h.snapshot.Entries[rel]
	entry.ConfiguredCPUs = configured.Clone()
	if h.capabilities.EmptyConfiguredCPUSet && configured.IsEmpty() {
		parent := h.snapshot.Entries[parentRelOf(rel)]
		entry.CPUs = parent.CPUs.Clone()
	} else {
		entry.CPUs = configured.Clone()
	}
	h.snapshot.Entries[rel] = entry

	parentRel := parentRelOf(rel)
	direction := WriteShrink
	if configured.IsSubsetOf(target) {
		direction = WriteGrow
	}
	return PlanOperation{
		Rel:                    rel,
		ExpectedIdentity:       entry.Identity,
		ParentRel:              parentRel,
		ExpectedParentIdentity: h.snapshot.Entries[parentRel].Identity,
		ExpectedCurrent:        CPUSetTarget{CPUs: entry.CPUs.Clone(), Mems: entry.Mems},
		Target:                 CPUSetTarget{CPUs: target.Clone(), Mems: entry.Mems},
		Direction:              direction,
	}
}

// memsOperation builds a mems-only PlanOperation so CPU/memory inheritance can
// be proven independent.
func (h *projectedHierarchy) memsOperation(rel, configuredMems, targetMems string) PlanOperation {
	entry := h.snapshot.Entries[rel]
	entry.ConfiguredMems = configuredMems
	h.snapshot.Entries[rel] = entry
	parentRel := parentRelOf(rel)
	return PlanOperation{
		Rel:                    rel,
		ExpectedIdentity:       entry.Identity,
		ParentRel:              parentRel,
		ExpectedParentIdentity: h.snapshot.Entries[parentRel].Identity,
		ExpectedCurrent:        CPUSetTarget{CPUs: entry.CPUs.Clone(), Mems: entry.Mems},
		Target:                 CPUSetTarget{CPUs: entry.CPUs.Clone(), Mems: targetMems},
		Direction:              WriteShrink,
		WriteMems:              true,
		OwnsMems:               true,
	}
}

func TestProjectedHierarchyCPUSetSemantics(t *testing.T) {
	tests := []struct {
		name           string
		capabilities   HierarchyCapabilities
		configured     machine.CPUSet
		target         machine.CPUSet
		parentTarget   machine.CPUSet
		wantConfigured string
		wantEffective  string
		wantErr        error
	}{
		{
			name:           "v1 non-empty target",
			capabilities:   v1Capabilities(),
			configured:     machine.NewCPUSet(0, 1),
			target:         machine.NewCPUSet(0),
			parentTarget:   machine.MustParse("0-3"),
			wantConfigured: "0",
			wantEffective:  "0",
		},
		{
			name:         "v1 empty target is rejected",
			capabilities: v1Capabilities(),
			configured:   machine.NewCPUSet(0),
			target:       machine.NewCPUSet(),
			parentTarget: machine.MustParse("0-3"),
			wantErr:      ErrEmptyCPUSetUnsupported,
		},
		{
			name:           "v2 empty target inherits parent",
			capabilities:   v2Capabilities(),
			configured:     machine.NewCPUSet(0),
			target:         machine.NewCPUSet(),
			parentTarget:   machine.NewCPUSet(0, 1),
			wantConfigured: "",
			wantEffective:  "0-1",
		},
		{
			name:           "v2 non-empty target keeps configured",
			capabilities:   v2Capabilities(),
			configured:     machine.NewCPUSet(0, 1),
			target:         machine.NewCPUSet(0),
			parentTarget:   machine.MustParse("0-3"),
			wantConfigured: "0",
			wantEffective:  "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hierarchy := projectedHierarchyFixture(t, tt.capabilities)
			hierarchy.setParentTarget(tt.parentTarget)
			err := hierarchy.applyOperation(hierarchy.cpuOperation(
				projectedChildRel,
				tt.configured,
				tt.target,
			))
			require.ErrorIs(t, err, tt.wantErr)
			if tt.wantErr != nil {
				return
			}
			got := hierarchy.snapshot.Entries[projectedChildRel]
			require.Equal(t, tt.wantConfigured, got.ConfiguredCPUs.String())
			require.Equal(t, tt.wantEffective, got.CPUs.String())
		})
	}
}

// TestProjectedHierarchyParentGrowUpdatesInheritedDescendants proves a parent
// grow re-projects the effective CPUs of every empty-configured descendant.
func TestProjectedHierarchyParentGrowUpdatesInheritedDescendants(t *testing.T) {
	hierarchy := projectedHierarchyFixture(t, v2Capabilities())
	hierarchy.setParentTarget(machine.MustParse("0-1"))
	require.NoError(t, hierarchy.recomputeEffectiveSubtree(projectedRootRel))
	require.Equal(t, "0-1", hierarchy.snapshot.Entries[projectedInheritRel].CPUs.String())
	require.Equal(t, "0-1", hierarchy.snapshot.Entries[projectedGrandchildRel].CPUs.String())

	err := hierarchy.applyOperation(hierarchy.cpuOperation(
		projectedRootRel,
		machine.MustParse("0-1"),
		machine.MustParse("0-3"),
	))
	require.NoError(t, err)

	require.Equal(t, "0-3", hierarchy.snapshot.Entries[projectedRootRel].CPUs.String())
	require.Equal(t, "0-3", hierarchy.snapshot.Entries[projectedInheritRel].CPUs.String())
	require.Equal(t, "0-3", hierarchy.snapshot.Entries[projectedGrandchildRel].CPUs.String())
}

// TestProjectedHierarchyParentShrinkUpdatesInheritedDescendants proves inherited
// descendants shrink with their parent.
func TestProjectedHierarchyParentShrinkUpdatesInheritedDescendants(t *testing.T) {
	hierarchy := projectedHierarchyFixture(t, v2Capabilities())

	err := hierarchy.applyOperation(hierarchy.cpuOperation(
		projectedRootRel,
		machine.MustParse("0-3"),
		machine.MustParse("0-1"),
	))
	require.NoError(t, err)

	require.Equal(t, "0-1", hierarchy.snapshot.Entries[projectedInheritRel].CPUs.String())
	require.Equal(t, "0-1", hierarchy.snapshot.Entries[projectedGrandchildRel].CPUs.String())
}

// TestProjectedHierarchyNonEmptyConfiguredDescendantRetainsTarget proves a
// descendant with its own configured cpuset is not overwritten by parent
// inheritance, only clamped by kernel containment.
func TestProjectedHierarchyNonEmptyConfiguredDescendantRetainsTarget(t *testing.T) {
	hierarchy := projectedHierarchyFixture(t, v2Capabilities())

	err := hierarchy.applyOperation(hierarchy.cpuOperation(
		projectedInheritRel,
		machine.NewCPUSet(),
		machine.MustParse("1-2"),
	))
	require.NoError(t, err)
	require.Equal(t, "1-2", hierarchy.snapshot.Entries[projectedInheritRel].ConfiguredCPUs.String())
	require.Equal(t, "1-2", hierarchy.snapshot.Entries[projectedInheritRel].CPUs.String())

	err = hierarchy.applyOperation(hierarchy.cpuOperation(
		projectedRootRel,
		machine.MustParse("0-3"),
		machine.MustParse("0-1"),
	))
	require.NoError(t, err)
	// The descendant keeps its configured target, clamped into the parent.
	require.Equal(t, "1-2", hierarchy.snapshot.Entries[projectedInheritRel].ConfiguredCPUs.String())
	require.Equal(t, "1", hierarchy.snapshot.Entries[projectedInheritRel].CPUs.String())
}

// TestProjectedHierarchyRejectsInvalidParentContainment proves a grow outside
// the projected parent effective set is rejected before mutating the clone.
func TestProjectedHierarchyRejectsInvalidParentContainment(t *testing.T) {
	hierarchy := projectedHierarchyFixture(t, v2Capabilities())
	hierarchy.setParentTarget(machine.MustParse("0-1"))
	require.NoError(t, hierarchy.recomputeEffectiveSubtree(projectedRootRel))

	err := hierarchy.applyOperation(hierarchy.cpuOperation(
		projectedInheritRel,
		machine.NewCPUSet(),
		machine.MustParse("0-3"),
	))
	require.ErrorIs(t, err, ErrProjectedParentContainment)
	// Clone must be unchanged on rejection.
	require.Equal(t, "0-1", hierarchy.snapshot.Entries[projectedInheritRel].CPUs.String())
}

// TestProjectedHierarchyCPUAndMemsInheritanceAreIndependent proves clearing a
// descendant's configured mems inherits parent mems without touching CPUs.
func TestProjectedHierarchyCPUAndMemsInheritanceAreIndependent(t *testing.T) {
	hierarchy := projectedHierarchyFixture(t, v2Capabilities())
	root := hierarchy.snapshot.Entries[projectedRootRel]
	root.Mems = "0-1"
	root.ConfiguredMems = "0-1"
	hierarchy.snapshot.Entries[projectedRootRel] = root

	err := hierarchy.applyOperation(hierarchy.memsOperation(projectedInheritRel, "0", ""))
	require.NoError(t, err)

	got := hierarchy.snapshot.Entries[projectedInheritRel]
	require.Equal(t, "", got.ConfiguredMems)
	require.Equal(t, "0-1", got.Mems)
	// CPUs remain inherited from the untouched parent effective set.
	require.Equal(t, "0-3", got.CPUs.String())
}

// TestProjectedHierarchyRecomputesEvidence proves projection refreshes domain
// unions and the snapshot fingerprint so downstream proofs bind to end state.
func TestProjectedHierarchyRecomputesEvidence(t *testing.T) {
	hierarchy := projectedHierarchyFixture(t, v2Capabilities())
	beforeID := hierarchy.snapshot.ID
	beforeUnion := hierarchy.snapshot.DomainUnion[DomainPrimary].String()

	err := hierarchy.applyOperation(hierarchy.cpuOperation(
		projectedRootRel,
		machine.MustParse("0-3"),
		machine.MustParse("0-1"),
	))
	require.NoError(t, err)

	require.NotEqual(t, beforeID, hierarchy.snapshot.ID)
	require.NotEqual(t, beforeUnion, hierarchy.snapshot.DomainUnion[DomainPrimary].String())
	require.Equal(t, "0-1", hierarchy.snapshot.DomainUnion[DomainPrimary].String())
}
