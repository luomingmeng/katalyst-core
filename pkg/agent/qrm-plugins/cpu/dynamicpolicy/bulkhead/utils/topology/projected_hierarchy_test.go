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
