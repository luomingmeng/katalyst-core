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

package cat

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	qrmresctrlmanager "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/resctrl"
	qrmresctrl "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/resctrl"
)

type fakeCPUListManager struct {
	clos []qrmresctrlmanager.CPUListClos
}

func (m *fakeCPUListManager) ListManagedClos(context.Context) ([]qrmresctrlmanager.CPUListClos, error) {
	return append([]qrmresctrlmanager.CPUListClos(nil), m.clos...), nil
}

func (*fakeCPUListManager) ApplyCPUList(context.Context, string, string) error { return nil }

func TestConfiguredClosManagerReturnsOnlyExplicitlyOwnedClos(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "ownership.json")
	require.NoError(t, qrmresctrlmanager.NewClosOwnershipStore(checkpointPath).Register("share-03"))
	manager := newConfiguredClosManager(&qrmresctrl.ResctrlConfig{
		CPUSetPoolToSharedSubgroup: map[string]int{"batch": 3},
		OwnershipCheckpointPath:    checkpointPath,
	}, &fakeCPUListManager{clos: []qrmresctrlmanager.CPUListClos{
		{ID: "dedicated", Epoch: 1}, {ID: "share-03", Epoch: 2},
		{ID: "shared-foreign", Epoch: 3}, {ID: "external", Epoch: 4},
	}})

	clos, err := manager.ListCATManagedClos(context.Background())

	require.NoError(t, err)
	require.Equal(t, []qrmresctrlmanager.CPUListClos{
		{ID: "share-03", Epoch: 2},
	}, clos)
}
