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

package cpusetmaterializer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestTargetOwnsConstructorInputAndGetterResults(t *testing.T) {
	in := TargetInput{
		ReserveCPUSet:    machine.NewCPUSet(0),
		ReclaimCPUSet:    machine.NewCPUSet(2, 3),
		NonReclaimCPUSet: machine.NewCPUSet(),
		ReclaimCPUSetByNUMA: map[int]machine.CPUSet{
			0: machine.NewCPUSet(2, 3),
			1: machine.NewCPUSet(),
		},
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
			"pod": {
				"main":    machine.NewCPUSet(1, 4),
				"sidecar": machine.NewCPUSet(),
			},
		},
		AllowReclaimOverlap: true,
	}
	target := NewTarget(in)

	in.ReserveCPUSet.Add(6)
	in.ReclaimCPUSet.Add(6)
	in.NonReclaimCPUSet.Add(6)
	numaInput := in.ReclaimCPUSetByNUMA[0]
	numaInput.Add(6)
	emptyNUMAInput := in.ReclaimCPUSetByNUMA[1]
	emptyNUMAInput.Add(6)
	containerInput := in.ContainerCPUSetByPod["pod"]["main"]
	containerInput.Add(6)
	emptyContainerInput := in.ContainerCPUSetByPod["pod"]["sidecar"]
	emptyContainerInput.Add(6)

	reserve := target.ReserveCPUSet()
	reclaim := target.ReclaimCPUSet()
	nonReclaim := target.NonReclaimCPUSet()
	numa := target.ReclaimCPUSetByNUMA()
	pods := target.ContainerCPUSetByPod()
	reserve.Add(7)
	reclaim.Add(7)
	nonReclaim.Add(7)
	numaCPUSet := numa[0]
	numaCPUSet.Add(7)
	emptyNUMACPUSet := numa[1]
	emptyNUMACPUSet.Add(7)
	containerCPUSet := pods["pod"]["main"]
	containerCPUSet.Add(7)
	emptyContainerCPUSet := pods["pod"]["sidecar"]
	emptyContainerCPUSet.Add(7)
	pods["new-pod"] = map[string]machine.CPUSet{"main": machine.NewCPUSet(6)}

	require.True(t, target.ReserveCPUSet().Equals(machine.NewCPUSet(0)))
	require.True(t, target.ReclaimCPUSet().Equals(machine.NewCPUSet(2, 3)))
	require.True(t, target.NonReclaimCPUSet().Equals(machine.NewCPUSet()))
	require.True(t, target.ReclaimCPUSetByNUMA()[0].Equals(machine.NewCPUSet(2, 3)))
	require.True(t, target.ReclaimCPUSetByNUMA()[1].Equals(machine.NewCPUSet()))
	require.True(t, target.ContainerCPUSetByPod()["pod"]["main"].Equals(machine.NewCPUSet(1, 4)))
	require.True(t, target.ContainerCPUSetByPod()["pod"]["sidecar"].Equals(machine.NewCPUSet()))
	require.NotContains(t, target.ContainerCPUSetByPod(), "new-pod")
	require.True(t, target.AllowReclaimOverlap())
}

func TestTargetPreservesCPUSetInitializationAndMapShape(t *testing.T) {
	t.Run("zero-value CPU sets remain uninitialized at every level", func(t *testing.T) {
		target := NewTarget(TargetInput{
			ReclaimCPUSetByNUMA: map[int]machine.CPUSet{
				0: {},
			},
			ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
				"pod": {
					"main": {},
				},
			},
		})

		require.False(t, target.ReserveCPUSet().Initialed)
		require.False(t, target.ReclaimCPUSet().Initialed)
		require.False(t, target.NonReclaimCPUSet().Initialed)
		require.False(t, target.ReclaimCPUSetByNUMA()[0].Initialed)
		require.False(t, target.ContainerCPUSetByPod()["pod"]["main"].Initialed)
	})

	t.Run("nil maps remain nil", func(t *testing.T) {
		emptyTarget := NewTarget(TargetInput{})
		target := NewTarget(TargetInput{
			ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
				"nil-pod": nil,
			},
		})

		require.Nil(t, emptyTarget.ReclaimCPUSetByNUMA())
		require.Nil(t, emptyTarget.ContainerCPUSetByPod())
		require.Nil(t, target.ContainerCPUSetByPod()["nil-pod"])
	})

	t.Run("empty maps remain non-nil and empty", func(t *testing.T) {
		emptyTarget := NewTarget(TargetInput{
			ReclaimCPUSetByNUMA:  map[int]machine.CPUSet{},
			ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{},
		})
		target := NewTarget(TargetInput{
			ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
				"empty-pod": {},
			},
		})

		require.NotNil(t, emptyTarget.ReclaimCPUSetByNUMA())
		require.Empty(t, emptyTarget.ReclaimCPUSetByNUMA())
		require.NotNil(t, emptyTarget.ContainerCPUSetByPod())
		require.Empty(t, emptyTarget.ContainerCPUSetByPod())
		require.NotNil(t, target.ContainerCPUSetByPod()["empty-pod"])
		require.Empty(t, target.ContainerCPUSetByPod()["empty-pod"])
	})
}
