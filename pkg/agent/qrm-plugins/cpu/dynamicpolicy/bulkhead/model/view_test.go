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

package model

import (
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestDesiredViewToAppliedViewDeepCopyIsolation_BitsUT(t *testing.T) {
	t.Parallel()

	desired := NewDesiredView()
	desired.NonReclaimPool = machine.NewCPUSet(0, 1)
	desired.ReclaimEffective = machine.NewCPUSet(2, 3)
	desired.ReclaimEffectivePerNUMA[0] = machine.NewCPUSet(2)
	desired.SharePoolMap["share"] = machine.NewCPUSet(0, 1)
	desired.ContainerCPUSetByPod["pod"] = map[string]machine.CPUSet{
		"container": machine.NewCPUSet(2),
	}

	applied := desired.ToAppliedView()
	desired.ReclaimEffective.Add(99)
	desired.ReclaimEffectivePerNUMA[0].Add(99)
	desired.SharePoolMap["share"].Add(99)
	desired.ContainerCPUSetByPod["pod"]["container"].Add(99)

	if applied.ReclaimEffective.Contains(99) {
		t.Fatalf("AppliedView should be isolated from DesiredView cpuset mutation")
	}
	if applied.ReclaimEffectivePerNUMA[0].Contains(99) {
		t.Fatalf("AppliedView per-NUMA map should be isolated from DesiredView mutation")
	}
	if applied.SharePoolMap["share"].Contains(99) {
		t.Fatalf("AppliedView share-pool map should be isolated from DesiredView mutation")
	}
	if applied.ContainerCPUSetByPod["pod"]["container"].Contains(99) {
		t.Fatalf("AppliedView container map should be isolated from DesiredView mutation")
	}
}

func TestAppliedViewDeepCopyIsolation_BitsUT(t *testing.T) {
	t.Parallel()

	applied := &AppliedView{
		CPUSetByRel: map[string]machine.CPUSet{
			"system": machine.NewCPUSet(2, 3),
		},
		CPUSetPartitionView: CPUSetPartitionView{
			NonReclaimPool:   machine.NewCPUSet(0, 1),
			ReclaimEffective: machine.NewCPUSet(2, 3),
			ReclaimEffectivePerNUMA: map[int]machine.CPUSet{
				1: machine.NewCPUSet(3),
			},
			SharePoolMap: map[string]machine.CPUSet{
				"share": machine.NewCPUSet(0),
			},
			ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
				"pod": {
					"container": machine.NewCPUSet(3),
				},
			},
		},
	}

	copied := applied.DeepCopy()
	copied.ReclaimEffective.Add(99)
	copied.ReclaimEffectivePerNUMA[1].Add(99)
	copied.SharePoolMap["share"].Add(99)
	copied.ContainerCPUSetByPod["pod"]["container"].Add(99)
	copied.CPUSetByRel["system"].Add(99)

	if applied.ReclaimEffective.Contains(99) ||
		applied.ReclaimEffectivePerNUMA[1].Contains(99) ||
		applied.SharePoolMap["share"].Contains(99) ||
		applied.ContainerCPUSetByPod["pod"]["container"].Contains(99) ||
		applied.CPUSetByRel["system"].Contains(99) {
		t.Fatalf("AppliedView.DeepCopy should isolate nested CPUSet fields")
	}
}

func TestAppliedViewRelProofDeepCopyAndEqual_BitsUT(t *testing.T) {
	t.Parallel()

	applied := &AppliedView{
		CPUSetPartitionView: NewCPUSetPartitionView(),
		RelProofByRel: map[string]CgroupRelProof{
			"system": {
				Device: 7,
				Inode:  11,
				CPUSet: machine.NewCPUSet(2, 3),
			},
		},
	}

	copied := applied.DeepCopy()
	proof := copied.RelProofByRel["system"]
	proof.CPUSet.Add(99)
	copied.RelProofByRel["system"] = proof
	if applied.RelProofByRel["system"].CPUSet.Contains(99) {
		t.Fatalf("AppliedView.DeepCopy should isolate rel proof cpusets")
	}
	if EqualAppliedView(applied, copied) {
		t.Fatalf("EqualAppliedView should compare rel proof cpusets")
	}

	copied = applied.DeepCopy()
	proof = copied.RelProofByRel["system"]
	proof.Inode++
	copied.RelProofByRel["system"] = proof
	if EqualAppliedView(applied, copied) {
		t.Fatalf("EqualAppliedView should compare rel proof identity")
	}
}
