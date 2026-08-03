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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestEvidenceCloneOwnsNestedValues(t *testing.T) {
	evidence := Evidence{
		Executed: true,
		ControlledRels: map[string]RelEvidence{
			"kubesandbox": {
				Target:   machine.NewCPUSet(1, 2),
				Observed: machine.NewCPUSet(),
				Reason:   "matched",
			},
		},
		PendingProtection: machine.NewCPUSet(),
		FailureReason:     "none",
	}

	cloned := evidence.Clone()
	rel := cloned.ControlledRels["kubesandbox"]
	rel.Target.Add(4)
	rel.Observed.Add(4)
	cloned.PendingProtection.Add(4)

	originalRel := evidence.ControlledRels["kubesandbox"]
	require.True(t, originalRel.Target.Equals(machine.NewCPUSet(1, 2)))
	require.True(t, originalRel.Observed.Equals(machine.NewCPUSet()))
	require.True(t, evidence.PendingProtection.Equals(machine.NewCPUSet()))
	require.True(t, cloned.Executed)
	require.Equal(t, "matched", cloned.ControlledRels["kubesandbox"].Reason)
	require.Equal(t, "none", cloned.FailureReason)
}

func TestEvidenceClonePreservesCPUSetInitializationAndMapShape(t *testing.T) {
	t.Run("zero-value CPU sets remain uninitialized at every level", func(t *testing.T) {
		cloned := (Evidence{
			ControlledRels: map[string]RelEvidence{
				"kubesandbox": {},
			},
		}).Clone()

		require.False(t, cloned.PendingProtection.Initialed)
		require.False(t, cloned.ControlledRels["kubesandbox"].Target.Initialed)
		require.False(t, cloned.ControlledRels["kubesandbox"].Observed.Initialed)
	})

	t.Run("nil map remains nil", func(t *testing.T) {
		require.Nil(t, (Evidence{}).Clone().ControlledRels)
	})

	t.Run("empty map remains non-nil and empty", func(t *testing.T) {
		cloned := (Evidence{ControlledRels: map[string]RelEvidence{}}).Clone()

		require.NotNil(t, cloned.ControlledRels)
		require.Empty(t, cloned.ControlledRels)
	})
}

func TestErrCPUSetNotConvergedIsStableSentinel(t *testing.T) {
	require.True(t, errors.Is(ErrCPUSetNotConverged, ErrCPUSetNotConverged))
	require.EqualError(t, ErrCPUSetNotConverged, "cpuset materialization not converged")
}
