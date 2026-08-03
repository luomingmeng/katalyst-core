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
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestBuildCPUSetPartitionViewFromTargetUsesExactOwnedTarget(t *testing.T) {
	t.Parallel()

	target := cpusetmaterializer.NewTarget(cpusetmaterializer.TargetInput{
		ReserveCPUSet:        machine.NewCPUSet(0),
		ReclaimCPUSet:        machine.NewCPUSet(2, 3),
		NonReclaimCPUSet:     machine.NewCPUSet(1, 4, 5),
		ReclaimCPUSetByNUMA:  map[int]machine.CPUSet{0: machine.NewCPUSet(2), 1: machine.NewCPUSet(3)},
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{"pod": {"main": machine.NewCPUSet(1, 4)}},
	})

	view := BuildCPUSetPartitionViewFromTarget(target)
	assertViewCPUSet(t, "reserve", view.Reserve, "0")
	assertViewCPUSet(t, "reclaim raw", view.ReclaimRaw, "2-3")
	assertViewCPUSet(t, "desired reclaim", view.DesiredReclaimEffective, "2-3")
	assertViewCPUSet(t, "reclaim", view.ReclaimEffective, "2-3")
	assertViewCPUSet(t, "desired non reclaim", view.DesiredNonReclaimPool, "1,4-5")
	assertViewCPUSet(t, "non reclaim", view.NonReclaimPool, "1,4-5")
	assertViewCPUSet(t, "numa 0", view.ReclaimEffectivePerNUMA[0], "2")
	assertViewCPUSet(t, "numa 1", view.ReclaimEffectivePerNUMA[1], "3")
	assertViewCPUSet(t, "container", view.ContainerCPUSetByPod["pod"]["main"], "1,4")
}

func TestBuildCPUSetPartitionViewFromTargetOwnsValues(t *testing.T) {
	t.Parallel()

	target := cpusetmaterializer.NewTarget(cpusetmaterializer.TargetInput{
		ReclaimCPUSet:        machine.NewCPUSet(2, 3),
		NonReclaimCPUSet:     machine.NewCPUSet(1, 4),
		ReclaimCPUSetByNUMA:  map[int]machine.CPUSet{0: machine.NewCPUSet(2, 3)},
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{"pod": {"main": machine.NewCPUSet(1, 4)}},
	})
	view := BuildCPUSetPartitionViewFromTarget(target)

	view.ReclaimEffective.Add(99)
	view.ReclaimEffectivePerNUMA[0].Add(99)
	view.ContainerCPUSetByPod["pod"]["main"].Add(99)

	assertViewCPUSet(t, "target reclaim", target.ReclaimCPUSet(), "2-3")
	assertViewCPUSet(t, "target numa", target.ReclaimCPUSetByNUMA()[0], "2-3")
	assertViewCPUSet(t, "target container", target.ContainerCPUSetByPod()["pod"]["main"], "1,4")
}

func TestCPUSetPartitionViewDeepCopyOwnsNestedValues(t *testing.T) {
	t.Parallel()

	view := BuildCPUSetPartitionViewFromTarget(cpusetmaterializer.NewTarget(cpusetmaterializer.TargetInput{
		ReclaimCPUSet:        machine.NewCPUSet(2, 3),
		NonReclaimCPUSet:     machine.NewCPUSet(1, 4),
		ReclaimCPUSetByNUMA:  map[int]machine.CPUSet{0: machine.NewCPUSet(2, 3)},
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{"pod": {"main": machine.NewCPUSet(1, 4)}},
	}))
	cloned := view.DeepCopy()
	cloned.ReclaimEffective.Add(99)
	cloned.ReclaimEffectivePerNUMA[0].Add(99)
	cloned.ContainerCPUSetByPod["pod"]["main"].Add(99)

	assertViewCPUSet(t, "original reclaim", view.ReclaimEffective, "2-3")
	assertViewCPUSet(t, "original numa", view.ReclaimEffectivePerNUMA[0], "2-3")
	assertViewCPUSet(t, "original container", view.ContainerCPUSetByPod["pod"]["main"], "1,4")
}

func assertViewCPUSet(t *testing.T, name string, got machine.CPUSet, want string) {
	t.Helper()
	if got.String() != want {
		t.Fatalf("%s cpuset = %s, want %s", name, got.String(), want)
	}
}
