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

package utils

import (
	"errors"
	"os"
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestCollectActiveRelsIncludesRootPartitionsSiblingsAndPerNUMA(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:      "kubepods",
		BulkheadReclaimRelPaths:     []string{"reclaimed"},
		BulkheadReclaimNumaPrefixes: []string{"reclaimed/reclaimed-"},
		BulkheadPartitionRelPaths:   []string{"kubepods", "burstable"},
	}
	view := &model.CPUSetPartitionView{
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
			1: machine.NewCPUSet(4, 5),
		},
	}

	got := CollectActiveRels(cfg, view, nil, []string{"besteffort"}, nil)
	for _, rel := range []string{"", "kubepods", "reclaimed", "reclaimed/reclaimed-1", "burstable", "besteffort"} {
		if _, ok := got[rel]; !ok {
			t.Fatalf("expected active rel %q in %#v", rel, got)
		}
	}
}

func TestBuildTopologyNodeSpecsFromViewUsesBulkheadConfig(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:      "kubepods",
		BulkheadReclaimRelPaths:     []string{"reclaimed"},
		BulkheadReclaimNumaPrefixes: []string{"reclaimed/reclaimed-"},
	}
	view := &model.CPUSetPartitionView{
		NonReclaimPool:          machine.NewCPUSet(0, 1),
		ReclaimEffective:        machine.NewCPUSet(2, 3),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{0: machine.NewCPUSet(2)},
	}
	cpuDetails := machine.CPUDetails{
		0: {NUMANodeID: 0},
		1: {NUMANodeID: 0},
		2: {NUMANodeID: 0},
		3: {NUMANodeID: 0},
	}

	specs, err := BuildTopologyNodeSpecsFromView(cfg, view, cpuDetails, []string{"sibling"}, nil)
	if err != nil {
		t.Fatalf("BuildTopologyNodeSpecsFromView: %v", err)
	}
	rels := map[string]struct{}{}
	for _, spec := range specs {
		rels[spec.Rel] = struct{}{}
	}
	for _, rel := range []string{"kubepods", "reclaimed", "reclaimed/reclaimed-0", "sibling"} {
		if _, ok := rels[rel]; !ok {
			t.Fatalf("expected rel %q in specs %#v", rel, specs)
		}
	}
}

func TestBuildTopologyNodeSpecsFromViewRetainsEmptyPhysicalNUMABucket(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:      "kubepods",
		BulkheadReclaimRelPaths:     []string{"reclaimed"},
		BulkheadReclaimNumaPrefixes: []string{"reclaimed/reclaimed-"},
	}
	view := &model.CPUSetPartitionView{
		NonReclaimPool:   machine.NewCPUSet(0, 1, 3),
		ReclaimEffective: machine.NewCPUSet(2),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(2),
			1: machine.NewCPUSet(),
		},
	}
	cpuDetails := machine.CPUDetails{
		0: {NUMANodeID: 0}, 1: {NUMANodeID: 0},
		2: {NUMANodeID: 0}, 3: {NUMANodeID: 1},
	}

	specs, err := BuildTopologyNodeSpecsFromView(cfg, view, cpuDetails, nil, nil)
	if err != nil {
		t.Fatalf("BuildTopologyNodeSpecsFromView: %v", err)
	}
	byRel := make(map[string]topology.NodeSpec, len(specs))
	for _, spec := range specs {
		byRel[spec.Rel] = spec
	}
	emptyBucket, ok := byRel["reclaimed/reclaimed-1"]
	if !ok {
		t.Fatalf("empty physical NUMA bucket was omitted from topology specs: %#v", specs)
	}
	if !emptyBucket.CPUs.IsEmpty() {
		t.Fatalf("empty physical NUMA bucket CPUs = %s, want empty", emptyBucket.CPUs.String())
	}
	if !emptyBucket.Constraint.CPUUpperBound.Equals(machine.NewCPUSet(3)) {
		t.Fatalf("empty physical NUMA bucket upper bound = %s, want 3", emptyBucket.Constraint.CPUUpperBound.String())
	}
}

func TestBuildTopologyNodeSpecsFromViewRetainsConfiguredMissingRels(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:      "kubepods",
		BulkheadReclaimRelPaths:     []string{"reclaimed", "system"},
		BulkheadReclaimNumaPrefixes: []string{"reclaimed/reclaimed-", "system/numa-"},
	}
	view := &model.CPUSetPartitionView{
		NonReclaimPool:          machine.NewCPUSet(0, 1),
		ReclaimEffective:        machine.NewCPUSet(2, 3),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{0: machine.NewCPUSet(2, 3)},
	}
	cpuDetails := machine.CPUDetails{
		0: {NUMANodeID: 0}, 1: {NUMANodeID: 0},
		2: {NUMANodeID: 0}, 3: {NUMANodeID: 0},
	}

	specs, err := BuildTopologyNodeSpecsFromView(cfg, view, cpuDetails, nil, func(string) error {
		return os.ErrNotExist
	})
	if err != nil {
		t.Fatalf("BuildTopologyNodeSpecsFromView: %v", err)
	}
	byRel := make(map[string]topology.NodeSpec, len(specs))
	for _, spec := range specs {
		byRel[spec.Rel] = spec
	}
	for _, rel := range []string{"reclaimed", "system", "reclaimed/reclaimed-0", "system/numa-0"} {
		if _, ok := byRel[rel]; !ok {
			t.Fatalf("configured missing rel %q was removed from topology boundary: %#v", rel, specs)
		}
	}
}

func TestBuildTopologyNodeSpecsFromViewBuildsNestedReclaimParentHierarchyWithoutMems(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:      "primary",
		BulkheadReclaimRelPaths:     []string{"reclaim", "reclaim/nested"},
		BulkheadReclaimNumaPrefixes: []string{"reclaim/direct-", "reclaim/nested/deep/bucket-"},
	}
	view := &model.CPUSetPartitionView{
		NonReclaimPool:   machine.NewCPUSet(0, 1),
		ReclaimEffective: machine.NewCPUSet(2, 3),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(2),
			1: machine.NewCPUSet(3),
		},
	}
	cpuDetails := machine.CPUDetails{
		0: {NUMANodeID: 0}, 1: {NUMANodeID: 0},
		2: {NUMANodeID: 0}, 3: {NUMANodeID: 1},
	}
	existing := map[string]struct{}{
		"primary": {}, "reclaim": {}, "reclaim/direct-0": {},
		"reclaim/nested": {}, "reclaim/nested/deep/bucket-1": {},
	}
	relExists := func(rel string) error {
		if _, ok := existing[rel]; ok {
			return nil
		}
		return os.ErrNotExist
	}

	specs, err := BuildTopologyNodeSpecsFromView(cfg, view, cpuDetails, nil, relExists)
	if err != nil {
		t.Fatalf("BuildTopologyNodeSpecsFromView: %v", err)
	}
	byRel := make(map[string]topology.NodeSpec, len(specs))
	for _, spec := range specs {
		byRel[spec.Rel] = spec
	}
	for rel, want := range map[string]struct {
		parent string
	}{
		"reclaim":                      {parent: ""},
		"reclaim/direct-0":             {parent: "reclaim"},
		"reclaim/nested":               {parent: "reclaim"},
		"reclaim/nested/deep/bucket-1": {parent: "reclaim/nested"},
	} {
		got, ok := byRel[rel]
		if !ok {
			t.Fatalf("missing topology spec %q in %#v", rel, specs)
		}
		if got.ParentRel != want.parent {
			t.Fatalf("spec %q parent = %q, want %q", rel, got.ParentRel, want.parent)
		}
		if got.Mems != "" {
			t.Fatalf("spec %q mems = %q, want empty; cpuset_mems plugin owns cpuset.mems", rel, got.Mems)
		}
	}
}

func TestBuildTopologyNodeSpecsFromViewRejectsCrossNUMADesiredCPU(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:      "kubepods",
		BulkheadReclaimRelPaths:     []string{"reclaimed"},
		BulkheadReclaimNumaPrefixes: []string{"reclaimed/reclaimed-"},
	}
	view := &model.CPUSetPartitionView{
		NonReclaimPool:          machine.NewCPUSet(3),
		ReclaimEffective:        machine.NewCPUSet(0, 2),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{0: machine.NewCPUSet(0, 2)},
	}
	cpuDetails := machine.CPUDetails{
		0: {NUMANodeID: 0},
		1: {NUMANodeID: 0},
		2: {NUMANodeID: 1},
		3: {NUMANodeID: 1},
	}
	specs, err := BuildTopologyNodeSpecsFromView(cfg, view, cpuDetails, nil, nil)
	if err != nil {
		t.Fatalf("BuildTopologyNodeSpecsFromView: %v", err)
	}
	dag, err := topology.BuildDAG(specs)
	if err != nil {
		t.Fatalf("BuildDAG: %v", err)
	}

	snapshot := &topology.CompleteSnapshot{
		Entries: map[string]topology.EntryState{
			"kubepods":              {Identity: topology.CgroupIdentity{Inode: 1}, CPUs: machine.NewCPUSet(3)},
			"reclaimed":             {Identity: topology.CgroupIdentity{Inode: 2}, CPUs: machine.NewCPUSet(0)},
			"reclaimed/reclaimed-0": {Identity: topology.CgroupIdentity{Inode: 3}, CPUs: machine.NewCPUSet(0), Mems: "0"},
		},
		DomainUnion: map[topology.DomainID]machine.CPUSet{
			topology.DomainPrimary: machine.NewCPUSet(3),
			topology.DomainReclaim: machine.NewCPUSet(0),
		},
	}
	plan, err := topology.BuildPhasePlan(topology.PhasePlanInput{
		Kind:     topology.PhaseExpand,
		DAG:      dag,
		Snapshot: snapshot,
		DesiredByRel: map[string]machine.CPUSet{
			"kubepods":              machine.NewCPUSet(3),
			"reclaimed":             machine.NewCPUSet(0, 2),
			"reclaimed/reclaimed-0": machine.NewCPUSet(0, 2),
		},
		DesiredMemsByRel: map[string]string{
			"reclaimed":             "0-1",
			"reclaimed/reclaimed-0": "0",
		},
		AllowedCPUs: machine.NewCPUSet(0, 1, 2, 3),
		Budget:      topology.NewBudgetTracker(topology.ConvergenceBudget{}),
	})
	if !errors.Is(err, topology.ErrInvalidReclaimBucketTarget) {
		t.Fatalf("BuildPhasePlan error = %v, want %v", err, topology.ErrInvalidReclaimBucketTarget)
	}
	if len(plan.Operations) != 0 {
		t.Fatalf("invalid NUMA target published writes: %#v", plan.Operations)
	}
}

func TestBuildTopologyNodeSpecsFromViewDoesNotPublishMemsTargets(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:      "kubepods",
		BulkheadReclaimRelPaths:     []string{"kubesandbox"},
		BulkheadReclaimNumaPrefixes: []string{"kubesandbox/reclaimed-"},
	}
	view := &model.CPUSetPartitionView{
		NonReclaimPool:          machine.NewCPUSet(2, 3),
		ReclaimEffective:        machine.NewCPUSet(0, 1),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{0: machine.NewCPUSet(0, 1)},
	}
	cpuDetails := machine.CPUDetails{
		0: {NUMANodeID: 0},
		1: {NUMANodeID: 0},
		2: {NUMANodeID: 1},
		3: {NUMANodeID: 1},
	}
	specs, err := BuildTopologyNodeSpecsFromView(cfg, view, cpuDetails, nil, nil)
	if err != nil {
		t.Fatalf("BuildTopologyNodeSpecsFromView: %v", err)
	}
	dag, err := topology.BuildDAG(specs)
	if err != nil {
		t.Fatalf("BuildDAG: %v", err)
	}

	plan, err := topology.BuildPhasePlan(topology.PhasePlanInput{
		Kind: topology.PhaseDrain,
		DAG:  dag,
		Snapshot: &topology.CompleteSnapshot{
			Entries: map[string]topology.EntryState{
				"kubepods":                {Identity: topology.CgroupIdentity{Inode: 1}, CPUs: machine.NewCPUSet(0, 1, 2, 3), Mems: "0-1"},
				"kubesandbox":             {Identity: topology.CgroupIdentity{Inode: 2}, CPUs: machine.NewCPUSet(0, 1, 2, 3), Mems: "0-1"},
				"kubesandbox/reclaimed-0": {Identity: topology.CgroupIdentity{Inode: 3}, CPUs: machine.NewCPUSet(0, 1, 2, 3), Mems: "0"},
			},
			DomainUnion: map[topology.DomainID]machine.CPUSet{
				topology.DomainPrimary: machine.NewCPUSet(0, 1, 2, 3),
				topology.DomainReclaim: machine.NewCPUSet(0, 1, 2, 3),
			},
		},
		DesiredByRel: map[string]machine.CPUSet{
			"kubepods":                machine.NewCPUSet(2, 3),
			"kubesandbox":             machine.NewCPUSet(0, 1),
			"kubesandbox/reclaimed-0": machine.NewCPUSet(0, 1),
		},
		AllowedCPUs: machine.NewCPUSet(0, 1, 2, 3),
		Budget:      topology.NewBudgetTracker(topology.ConvergenceBudget{}),
	})
	if err != nil {
		t.Fatalf("BuildPhasePlan: %v", err)
	}
	for _, operation := range plan.Operations {
		if operation.WriteMems {
			t.Fatalf("operation %q unexpectedly writes cpuset.mems target=%q; operations=%#v",
				operation.Rel, operation.Target.Mems, plan.Operations)
		}
	}
}
