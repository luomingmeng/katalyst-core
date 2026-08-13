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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type stableIdentityTestDriver struct {
	HierarchyDriver
}

func (d *stableIdentityTestDriver) Capabilities() HierarchyCapabilities {
	capabilities := d.HierarchyDriver.Capabilities()
	capabilities.StableIdentity = true
	return capabilities
}

func TestSnapshotIncludesControlledRootsBucketsAndDynamicDescendants(t *testing.T) {
	dag := buildSnapshotTestDAG(t)
	fake := buildSnapshotTestHierarchy()

	snapshot, err := BuildCompleteSnapshot(context.Background(), fake, dag, SnapshotRequest{
		Purpose:      ScanForPlan,
		AffectedRels: []string{"primary", "reclaim", "reclaim/bucket-0"},
	}, NewBudgetTracker(ConvergenceBudget{}))
	if err != nil {
		t.Fatalf("BuildCompleteSnapshot() error = %v", err)
	}

	wantRels := []string{
		"primary", "primary/pod-a", "primary/pod-a/container-a",
		"reclaim", "reclaim/bucket-0", "reclaim/bucket-0/pod-r",
	}
	for _, rel := range wantRels {
		if _, ok := snapshot.Entries[rel]; !ok {
			t.Errorf("Entries missing %q", rel)
		}
	}
	if got := snapshot.DomainUnion[DomainPrimary].String(); got != "0-1" {
		t.Fatalf("primary union = %q, want 0-1", got)
	}
	if got := snapshot.DomainUnion[DomainReclaim].String(); got != "2-3" {
		t.Fatalf("reclaim union = %q, want 2-3", got)
	}
	if snapshot.ID == (SnapshotID{}) {
		t.Fatal("snapshot ID is zero")
	}
}

func TestSnapshotRejectsDriverWithoutStableIdentityBeforeIO(t *testing.T) {
	fake := buildSnapshotTestHierarchy()
	fake.stableIdentity = false

	snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
		Purpose:      ScanForPlan,
		AffectedRels: []string{"primary"},
	}, NewBudgetTracker(ConvergenceBudget{}))
	if snapshot != nil {
		t.Fatalf("snapshot = %#v, want nil", snapshot)
	}
	var snapshotErr *SnapshotError
	if !errors.As(err, &snapshotErr) {
		t.Fatalf("error = %T %v, want *SnapshotError", err, err)
	}
	if snapshotErr.Class != HierarchyErrorInvalid {
		t.Fatalf("error class = %q, want %q", snapshotErr.Class, HierarchyErrorInvalid)
	}
	if fake.calls != 0 {
		t.Fatalf("driver calls = %d, want 0", fake.calls)
	}
}

func TestSnapshotRejectsReadCPUSetReadMemsAndListChildrenFailure(t *testing.T) {
	tests := []struct {
		name string
		op   HierarchyOperation
		rel  string
		err  error
	}{
		{name: "cpuset read", op: HierarchyOperationRead, rel: "primary", err: errors.New("parse cpuset: invalid syntax")},
		{name: "mems read", op: HierarchyOperationRead, rel: "reclaim", err: errors.New("read cpuset.mems: permission denied")},
		{name: "children list", op: HierarchyOperationList, rel: "primary", err: errors.New("readdir: input/output error")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := buildSnapshotTestHierarchy()
			fake.beforeCall = func(op HierarchyOperation, rel string) error {
				if op == tt.op && rel == tt.rel {
					return tt.err
				}
				return nil
			}
			snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
				Purpose:      ScanForPlan,
				AffectedRels: []string{"primary", "reclaim"},
			}, NewBudgetTracker(ConvergenceBudget{}))
			if err == nil || snapshot != nil {
				t.Fatalf("snapshot=%v error=%v, want nil snapshot and error", snapshot, err)
			}
			var snapshotErr *SnapshotError
			if !errors.As(err, &snapshotErr) {
				t.Fatalf("error type = %T, want *SnapshotError", err)
			}
		})
	}
}

func TestSnapshotSkipsUncontrolledCgroupV2DescendantWithoutCpusetController(t *testing.T) {
	fake := buildSnapshotTestHierarchy()
	fake.capabilities.EffectiveCPUSet = true
	fake.beforeCall = func(op HierarchyOperation, rel string) error {
		if op == HierarchyOperationRead && rel == "primary/pod-a/container-a" {
			return ErrCgroupControllerUnavailable
		}
		return nil
	}

	snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
		Purpose:      ScanForPlan,
		AffectedRels: []string{"primary"},
	}, NewBudgetTracker(ConvergenceBudget{}))
	if err != nil {
		t.Fatalf("BuildCompleteSnapshot() error = %v", err)
	}
	if _, ok := snapshot.Entries["primary"]; !ok {
		t.Fatalf("snapshot missing controlled root")
	}
	if _, ok := snapshot.Entries["primary/pod-a"]; !ok {
		t.Fatalf("snapshot missing dynamic parent with cpuset controller")
	}
	if _, ok := snapshot.Entries["primary/pod-a/container-a"]; ok {
		t.Fatalf("snapshot included uncontrolled descendant without cpuset controller")
	}
	if got := snapshot.DomainUnion[DomainPrimary].String(); got != "0-1" {
		t.Fatalf("primary domain union = %q, want 0-1", got)
	}
}

func TestSnapshotRejectsControlledCgroupV2NodeWithoutCpusetController(t *testing.T) {
	fake := buildSnapshotTestHierarchy()
	fake.capabilities.EffectiveCPUSet = true
	fake.beforeCall = func(op HierarchyOperation, rel string) error {
		if op == HierarchyOperationRead && rel == "reclaim/bucket-0" {
			return ErrCgroupControllerUnavailable
		}
		return nil
	}

	snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
		Purpose:      ScanForPlan,
		AffectedRels: []string{"reclaim/bucket-0"},
	}, NewBudgetTracker(ConvergenceBudget{}))
	if snapshot != nil {
		t.Fatalf("snapshot = %#v, want nil", snapshot)
	}
	if !errors.Is(err, ErrCgroupControllerUnavailable) {
		t.Fatalf("error = %v, want ErrCgroupControllerUnavailable", err)
	}
}

func TestSnapshotDoesNotSkipUnavailableControllerOnCgroupV1(t *testing.T) {
	fake := buildSnapshotTestHierarchy()
	fake.capabilities.EffectiveCPUSet = false
	fake.beforeCall = func(op HierarchyOperation, rel string) error {
		if op == HierarchyOperationRead && rel == "primary/pod-a/container-a" {
			return ErrCgroupControllerUnavailable
		}
		return nil
	}

	snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
		Purpose:      ScanForPlan,
		AffectedRels: []string{"primary"},
	}, NewBudgetTracker(ConvergenceBudget{}))
	if snapshot != nil {
		t.Fatalf("snapshot = %#v, want nil", snapshot)
	}
	if !errors.Is(err, ErrCgroupControllerUnavailable) {
		t.Fatalf("error = %v, want ErrCgroupControllerUnavailable", err)
	}
}

func TestSnapshotRejectsIdentityChangeAndListStatDeleteRace(t *testing.T) {
	t.Run("identity changes around read", func(t *testing.T) {
		fake := buildSnapshotTestHierarchy()
		stats := 0
		fake.beforeCall = func(op HierarchyOperation, rel string) error {
			if op == HierarchyOperationStat && rel == "primary" {
				stats++
				if stats == 2 {
					fake.bumpIdentity(rel)
				}
			}
			return nil
		}
		snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
			Purpose: ScanForPrecheck, ParentRel: "primary",
		}, NewBudgetTracker(ConvergenceBudget{}))
		if !errors.Is(err, ErrCgroupIdentityChanged) || snapshot != nil {
			t.Fatalf("snapshot=%v error=%v, want identity-changed failure", snapshot, err)
		}
	})

	t.Run("listed child disappears before stat", func(t *testing.T) {
		fake := buildSnapshotTestHierarchy()
		fake.beforeCall = func(op HierarchyOperation, rel string) error {
			if op == HierarchyOperationStat && rel == "primary/pod-a" {
				delete(fake.nodes, rel)
			}
			return nil
		}
		snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
			Purpose: ScanForPrecheck, ParentRel: "primary",
		}, NewBudgetTracker(ConvergenceBudget{}))
		if !errors.Is(err, syscall.ENOENT) || snapshot != nil {
			t.Fatalf("snapshot=%v error=%v, want delete-race failure", snapshot, err)
		}
	})

	t.Run("parent rebuilt between read and list", func(t *testing.T) {
		fake := buildSnapshotTestHierarchy()
		fake.beforeCall = func(op HierarchyOperation, rel string) error {
			if op == HierarchyOperationList && rel == "primary" {
				previous := fake.nodes[rel]
				fake.nodes[rel] = &fakeHierarchyNode{
					identity: CgroupIdentity{Device: previous.identity.Device, Inode: previous.identity.Inode + 100},
					cpus:     previous.cpus.Clone(),
					mems:     previous.mems,
				}
			}
			return nil
		}
		snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
			Purpose: ScanForPrecheck, ParentRel: "primary",
		}, NewBudgetTracker(ConvergenceBudget{}))
		if !errors.Is(err, ErrCgroupIdentityChanged) || snapshot != nil {
			t.Fatalf("snapshot=%v error=%v, want parent identity-changed failure and nil snapshot", snapshot, err)
		}
	})
}

func TestSnapshotRejectsSymlinkWithDriverErrorAndNilResult(t *testing.T) {
	root := resolvedPath(t, t.TempDir())
	primary := filepath.Join(root, "primary")
	writeTestCgroupDirectory(t, primary)
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(primary, "escape")); err != nil {
		t.Fatal(err)
	}
	rawDriver, err := NewCgroupV1Driver(root, nil)
	if err != nil {
		t.Fatalf("NewCgroupV1Driver() error = %v", err)
	}
	defer rawDriver.Close()
	driver := &stableIdentityTestDriver{HierarchyDriver: rawDriver}

	snapshot, err := BuildCompleteSnapshot(context.Background(), driver, buildSnapshotTestDAG(t), SnapshotRequest{
		Purpose: ScanForPlan, AffectedRels: []string{"primary"},
	}, NewBudgetTracker(ConvergenceBudget{}))
	if snapshot != nil {
		t.Fatalf("snapshot = %#v, want nil", snapshot)
	}
	var snapshotErr *SnapshotError
	if !errors.As(err, &snapshotErr) {
		t.Fatalf("error = %T %v, want *SnapshotError", err, err)
	}
	if snapshotErr.Operation != HierarchyOperationList {
		t.Fatalf("operation = %q, want %q: %v", snapshotErr.Operation, HierarchyOperationList, err)
	}
	if !strings.Contains(snapshotErr.Err.Error(), "symlink is not allowed") {
		t.Fatalf("driver error = %v, want symlink rejection propagated", snapshotErr.Err)
	}
}

func TestSnapshotDoesNotCrossControlledBoundary(t *testing.T) {
	fake := buildSnapshotTestHierarchy()
	var listed []string
	fake.beforeCall = func(op HierarchyOperation, rel string) error {
		if op == HierarchyOperationList {
			listed = append(listed, rel)
		}
		return nil
	}

	snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
		Purpose:      ScanForPlan,
		AffectedRels: []string{"reclaim"},
	}, NewBudgetTracker(ConvergenceBudget{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Entries["reclaim/bucket-0/pod-r"]; ok {
		t.Fatal("plan scan crossed into an unaffected controlled bucket")
	}
	if !reflect.DeepEqual(listed, []string{"reclaim"}) {
		t.Fatalf("listed = %v, want only affected root", listed)
	}
}

func TestSnapshotNodeAndDepthBudgetFailWithoutPartialResult(t *testing.T) {
	tests := []struct {
		name   string
		budget ConvergenceBudget
		want   error
	}{
		{name: "node", budget: ConvergenceBudget{MaxSnapshotNodes: 2}, want: ErrNodeBudgetExceeded},
		{name: "depth", budget: ConvergenceBudget{MaxSnapshotDepth: 1}, want: ErrHierarchyDepthBudget},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewBudgetTracker(tt.budget)
			driver := NewBudgetedHierarchyDriver(buildSnapshotTestHierarchy(), tracker)
			snapshot, err := BuildCompleteSnapshot(context.Background(), driver, buildSnapshotTestDAG(t), SnapshotRequest{
				Purpose: ScanForPlan, AffectedRels: []string{"primary"},
			}, tracker)
			if !errors.Is(err, tt.want) || snapshot != nil {
				t.Fatalf("snapshot=%v error=%v, want %v and nil snapshot", snapshot, err, tt.want)
			}
		})
	}
}

func TestSnapshotHierarchyIOBudgetFailsWithoutPartialResult(t *testing.T) {
	tracker := NewBudgetTracker(ConvergenceBudget{MaxHierarchyIOOperations: 1})
	snapshot, err := BuildCompleteSnapshot(
		context.Background(),
		buildSnapshotTestHierarchy(),
		buildSnapshotTestDAG(t),
		SnapshotRequest{Purpose: ScanForPlan, AffectedRels: []string{"primary"}},
		tracker,
	)
	if !errors.Is(err, ErrHierarchyIOOperationBudgetExceeded) || snapshot != nil {
		t.Fatalf("snapshot=%v error=%v, want hierarchy I/O budget error and nil snapshot", snapshot, err)
	}
}

func TestSnapshotPrecheckIncludesControlledImmediateChild(t *testing.T) {
	snapshot, err := BuildCompleteSnapshot(
		context.Background(),
		buildSnapshotTestHierarchy(),
		buildSnapshotTestDAG(t),
		SnapshotRequest{Purpose: ScanForPrecheck, ParentRel: "reclaim"},
		NewBudgetTracker(ConvergenceBudget{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Entries["reclaim/bucket-0"]; !ok {
		t.Fatal("precheck snapshot omitted controlled immediate child")
	}
	if _, ok := snapshot.Entries["reclaim/bucket-0/pod-r"]; ok {
		t.Fatal("precheck snapshot expanded below immediate child")
	}
}

func TestSnapshotAppliedViewExpandsMismatchOnly(t *testing.T) {
	snapshot, err := BuildCompleteSnapshot(
		context.Background(),
		buildSnapshotTestHierarchy(),
		buildSnapshotTestDAG(t),
		SnapshotRequest{Purpose: ScanForAppliedView, MismatchRels: []string{"primary"}},
		NewBudgetTracker(ConvergenceBudget{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Entries["primary/pod-a/container-a"]; !ok {
		t.Fatal("applied-view snapshot did not expand mismatched primary subtree")
	}
	if _, ok := snapshot.Entries["reclaim/bucket-0/pod-r"]; ok {
		t.Fatal("applied-view snapshot expanded unrelated matching subtree")
	}
}

func TestSnapshotIDChangesWithIdentityChildrenCPUsMemsOrBoundary(t *testing.T) {
	baseDriver := buildSnapshotTestHierarchy()
	base := mustBuildSnapshot(t, baseDriver, ScanForPlan, []string{"primary"})
	tests := []struct {
		name   string
		mutate func(*fakeHierarchyDriver)
		req    SnapshotRequest
	}{
		{name: "identity", mutate: func(f *fakeHierarchyDriver) { f.bumpIdentity("primary") }},
		{name: "children", mutate: func(f *fakeHierarchyDriver) {
			f.add("primary/pod-b", CgroupIdentity{Device: 1, Inode: 8}, "1", "0")
		}},
		{name: "cpus", mutate: func(f *fakeHierarchyDriver) { f.nodes["primary"].cpus = machine.MustParse("0") }},
		{name: "mems", mutate: func(f *fakeHierarchyDriver) { f.nodes["primary"].mems = "0-1" }},
		{name: "boundary", req: SnapshotRequest{Purpose: ScanForPrecheck, ParentRel: "primary"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := buildSnapshotTestHierarchy()
			if tt.mutate != nil {
				tt.mutate(fake)
			}
			req := tt.req
			if req.Purpose == "" {
				req = SnapshotRequest{Purpose: ScanForPlan, AffectedRels: []string{"primary"}}
			}
			got, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), req, NewBudgetTracker(ConvergenceBudget{}))
			if err != nil {
				t.Fatal(err)
			}
			if got.ID == base.ID {
				t.Fatalf("snapshot ID did not change for %s", tt.name)
			}
		})
	}
}

func TestSnapshotIDChangesWithConfiguredCPUsOrMems(t *testing.T) {
	base := &CompleteSnapshot{
		Entries: map[string]EntryState{
			"primary": {
				Rel: "primary", Identity: CgroupIdentity{Device: 1, Inode: 1},
				CPUs: machine.MustParse("0-3"), Mems: "0",
				ConfiguredCPUs: machine.MustParse("0-3"), ConfiguredMems: "0",
			},
		},
		DomainByRel:  map[string]DomainID{"primary": DomainPrimary},
		DomainUnion:  map[DomainID]machine.CPUSet{DomainPrimary: machine.MustParse("0-3")},
		ScanBoundary: ScanBoundary{Purpose: ScanForPlan, Roots: []string{"primary"}},
	}
	baseID := fingerprintSnapshot(base)

	for _, tc := range []struct {
		name   string
		mutate func(*CompleteSnapshot)
	}{
		{
			name: "configured cpus",
			mutate: func(snapshot *CompleteSnapshot) {
				entry := snapshot.Entries["primary"]
				entry.ConfiguredCPUs = machine.MustParse("1-2")
				snapshot.Entries["primary"] = entry
			},
		},
		{
			name: "configured mems",
			mutate: func(snapshot *CompleteSnapshot) {
				entry := snapshot.Entries["primary"]
				entry.ConfiguredMems = "1"
				snapshot.Entries["primary"] = entry
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := &CompleteSnapshot{
				Entries:      map[string]EntryState{"primary": base.Entries["primary"]},
				DomainByRel:  base.DomainByRel,
				DomainUnion:  base.DomainUnion,
				ScanBoundary: base.ScanBoundary,
			}
			tc.mutate(changed)
			if got := fingerprintSnapshot(changed); got == baseID {
				t.Fatalf("snapshot ID did not change for %s drift", tc.name)
			}
		})
	}
}

func TestReleaseProofBoundaryReadsTrustAnchorsOnly(t *testing.T) {
	fake := buildSnapshotTestHierarchy()
	var calls []string
	fake.beforeCall = func(op HierarchyOperation, rel string) error {
		calls = append(calls, string(op)+":"+rel)
		if strings.Contains(rel, "pod-r") {
			return errors.New("unrelated dynamic subtree must not be read")
		}
		return nil
	}

	snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
		Purpose: ScanForWitness, SourceDomain: DomainReclaim,
	}, NewBudgetTracker(ConvergenceBudget{}))
	if err != nil {
		t.Fatalf("BuildCompleteSnapshot() error = %v calls=%v", err, calls)
	}
	for _, rel := range []string{"reclaim", "reclaim/bucket-0"} {
		if _, ok := snapshot.Entries[rel]; !ok {
			t.Errorf("witness snapshot missing trust anchor %q", rel)
		}
	}
	if _, ok := snapshot.Entries["reclaim/bucket-0/pod-r"]; ok {
		t.Fatal("witness snapshot read unrelated dynamic descendant")
	}
}

func buildSnapshotTestDAG(t *testing.T) *TopoDAG {
	t.Helper()
	dag, err := BuildDAG([]NodeSpec{
		{Rel: "primary", Role: TopoNodeRolePrimary, Domain: DomainPrimary, ControlledRoot: true, TrustAnchor: true},
		{Rel: "reclaim", Role: TopoNodeRoleReclaim, Domain: DomainReclaim, ControlledRoot: true, TrustAnchor: true},
		{Rel: "reclaim/bucket-0", ParentRel: "reclaim", Role: TopoNodeRoleReclaimNUMABucket, Domain: DomainReclaim, TrustAnchor: true, Constraint: TopologyConstraint{
			CPUUpperBound: machine.NewCPUSet(0),
			MemUpperBound: machine.NewCPUSet(0),
			Scope:         TopologyScopeNUMANode,
		}},
	})
	if err != nil {
		t.Fatalf("BuildDAG() error = %v", err)
	}
	return dag
}

func buildSnapshotTestHierarchy() *fakeHierarchyDriver {
	fake := newFakeHierarchyDriver()
	fake.add("primary", CgroupIdentity{Device: 1, Inode: 1}, "0-1", "0")
	fake.add("primary/pod-a", CgroupIdentity{Device: 1, Inode: 2}, "1", "0")
	fake.add("primary/pod-a/container-a", CgroupIdentity{Device: 1, Inode: 3}, "1", "0")
	fake.add("reclaim", CgroupIdentity{Device: 1, Inode: 4}, "2-3", "0")
	fake.add("reclaim/bucket-0", CgroupIdentity{Device: 1, Inode: 5}, "2-3", "0")
	fake.add("reclaim/bucket-0/pod-r", CgroupIdentity{Device: 1, Inode: 6}, "3", "0")
	return fake
}

func mustBuildSnapshot(t *testing.T, fake *fakeHierarchyDriver, purpose ScanPurpose, affected []string) *CompleteSnapshot {
	t.Helper()
	snapshot, err := BuildCompleteSnapshot(context.Background(), fake, buildSnapshotTestDAG(t), SnapshotRequest{
		Purpose: purpose, AffectedRels: affected,
	}, NewBudgetTracker(ConvergenceBudget{}))
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
