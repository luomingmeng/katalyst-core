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
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
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
	view := &CPUSetPartitionView{
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
	view := &CPUSetPartitionView{
		NonReclaimPool:          machine.NewCPUSet(0, 1),
		ReclaimEffective:        machine.NewCPUSet(2, 3),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{0: machine.NewCPUSet(2)},
	}

	specs, err := BuildTopologyNodeSpecsFromView(cfg, view, []string{"sibling"}, nil)
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

func TestBuildTopologyNodeSpecsFromViewKeepsControlledEmptyNUMARel(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:      "kubepods",
		BulkheadReclaimRelPaths:     []string{"reclaimed"},
		BulkheadReclaimNumaPrefixes: []string{"reclaimed/reclaimed-"},
	}
	view := &CPUSetPartitionView{
		NonReclaimPool:   machine.NewCPUSet(0, 1),
		ReclaimEffective: machine.NewCPUSet(2, 3),
		ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(2, 3),
			1: machine.NewCPUSet(),
		},
	}

	specs, err := BuildTopologyNodeSpecsFromView(cfg, view, nil, nil)
	if err != nil {
		t.Fatalf("BuildTopologyNodeSpecsFromView: %v", err)
	}
	for _, spec := range specs {
		if spec.Rel == "reclaimed/reclaimed-1" {
			if !spec.CPUs.IsEmpty() {
				t.Fatalf("controlled empty rel target = %s, want empty", spec.CPUs.String())
			}
			return
		}
	}
	t.Fatalf("controlled empty rel reclaimed/reclaimed-1 missing from specs: %#v", specs)
}

func TestBuildControlledRelInventoryIncludesAllExplicitTargets(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:      "kubepods",
		BulkheadSystemRelPath:       "system",
		BulkheadReclaimRelPaths:     []string{"reclaimed"},
		BulkheadReclaimNumaPrefixes: []string{"reclaimed/reclaimed-"},
	}
	target := cpusetmaterializer.NewTarget(cpusetmaterializer.TargetInput{
		ReclaimCPUSet:    machine.NewCPUSet(2, 3),
		NonReclaimCPUSet: machine.NewCPUSet(0, 1),
		ReclaimCPUSetByNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(2, 3),
			1: machine.NewCPUSet(),
		},
	})

	inventory := BuildControlledRelInventory(cfg, target, true,
		[]string{"dynamic-sibling"},
		map[string]machine.CPUSet{"kubepods/pod/container": machine.NewCPUSet(0)},
	)
	got := make(map[string]machine.CPUSet, len(inventory))
	for _, item := range inventory {
		got[item.Rel] = item.Target
	}

	want := map[string]string{
		"kubepods":               "0-1",
		"system":                 "2-3",
		"reclaimed":              "2-3",
		"reclaimed/reclaimed-0":  "2-3",
		"reclaimed/reclaimed-1":  "",
		"dynamic-sibling":        "2-3",
		"kubepods/pod/container": "0",
	}
	if len(got) != len(want) {
		t.Fatalf("inventory rel count = %d, want %d: %#v", len(got), len(want), inventory)
	}
	for rel, cpus := range want {
		actual, ok := got[rel]
		if !ok {
			t.Fatalf("controlled rel %q missing from %#v", rel, inventory)
		}
		if actual.String() != cpus {
			t.Fatalf("controlled rel %q target = %q, want %q", rel, actual.String(), cpus)
		}
	}
}

func TestBuildControlledRelInventoryOmitsDisabledSystemService(t *testing.T) {
	t.Parallel()

	cfg := bulkheadconfig.BulkheadConfiguration{
		BulkheadPrimaryRelPath:  "kubepods",
		BulkheadSystemRelPath:   "system",
		BulkheadReclaimRelPaths: []string{"reclaimed"},
	}
	target := cpusetmaterializer.NewTarget(cpusetmaterializer.TargetInput{
		ReclaimCPUSet:    machine.NewCPUSet(2, 3),
		NonReclaimCPUSet: machine.NewCPUSet(0, 1),
	})

	inventory := BuildControlledRelInventory(cfg, target, false, nil, nil)
	for _, item := range inventory {
		if item.Rel == "system" {
			t.Fatalf("disabled system-service rel must not be controlled: %#v", inventory)
		}
	}
}
