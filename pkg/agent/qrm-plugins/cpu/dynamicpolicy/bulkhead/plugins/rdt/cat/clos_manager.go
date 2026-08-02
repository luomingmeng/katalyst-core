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

	qrmresctrlmanager "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/resctrl"
	qrmresctrl "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/resctrl"
)

type configuredClosManager struct {
	manager   qrmresctrlmanager.CPUListManager
	ownership *qrmresctrlmanager.ClosOwnershipStore
}

func newConfiguredClosManager(config *qrmresctrl.ResctrlConfig, manager qrmresctrlmanager.CPUListManager) *configuredClosManager {
	if config == nil {
		config = qrmresctrl.NewResctrlConfig()
	}
	return &configuredClosManager{
		manager:   manager,
		ownership: qrmresctrlmanager.NewClosOwnershipStore(config.OwnershipCheckpointPath),
	}
}

func (m *configuredClosManager) ListCATManagedClos(ctx context.Context) ([]qrmresctrlmanager.CPUListClos, error) {
	clos, err := m.manager.ListManagedClos(ctx)
	if err != nil {
		return nil, err
	}
	owned, err := m.ownership.Load()
	if err != nil {
		return nil, err
	}
	managed := make([]qrmresctrlmanager.CPUListClos, 0, len(clos))
	for _, current := range clos {
		if owned.Has(current.ID) {
			managed = append(managed, current)
		}
	}
	return managed, nil
}
