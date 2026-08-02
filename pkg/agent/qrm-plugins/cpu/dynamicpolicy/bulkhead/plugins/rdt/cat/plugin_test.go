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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	qrmresctrlmanager "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/resctrl"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	qrmresctrl "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/resctrl"
	"github.com/kubewharf/katalyst-core/pkg/util/external/rdt"
)

type fakeClosManager struct {
	clos    []qrmresctrlmanager.CPUListClos
	managed map[string]struct{}
}

func (m *fakeClosManager) ListCATManagedClos(context.Context) ([]qrmresctrlmanager.CPUListClos, error) {
	clos := make([]qrmresctrlmanager.CPUListClos, 0, len(m.clos))
	for _, current := range m.clos {
		if m.managed == nil {
			clos = append(clos, current)
			continue
		}
		if _, ok := m.managed[current.ID]; ok {
			clos = append(clos, current)
		}
	}
	return clos, nil
}

type catWrite struct {
	clos string
	mask map[int]uint64
}

type fakeRDTManager struct {
	writes      []catWrite
	invalidated []string
	failClos    string
	failOnce    bool
}

func (*fakeRDTManager) CheckSupportRDT() (bool, error)    { return true, nil }
func (*fakeRDTManager) InitRDT() error                    { return nil }
func (*fakeRDTManager) ApplyTasks(string, []string) error { return nil }
func (m *fakeRDTManager) ApplyCAT(clos string, mask map[int]uint64) error {
	m.writes = append(m.writes, catWrite{clos: clos, mask: mask})
	if clos == m.failClos && m.failOnce {
		m.failOnce = false
		return errors.New("injected CAT write failure")
	}
	return nil
}
func (*fakeRDTManager) ApplyMBA(string, map[int]int) error { return nil }
func (*fakeRDTManager) RunClosResourceUpdate(_ string, update func() (bool, error)) error {
	_, err := update()
	return err
}
func (m *fakeRDTManager) InvalidateClos(clos string) {
	m.invalidated = append(m.invalidated, clos)
}

type fakeCapabilityProvider struct {
	capabilities map[int]rdt.CATCapability
	err          error
}

func (p fakeCapabilityProvider) GetCATCapabilities() (map[int]rdt.CATCapability, error) {
	return p.capabilities, p.err
}

func periodicalContext(conf *dynamicconfig.Configuration) bulkheadapi.PeriodicalHandlerContext {
	return bulkheadapi.PeriodicalHandlerContext{DynamicConf: conf}
}

func TestCATPluginBuildsSymmetricDomainMasksAndPrefersDirectClosOverride(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{
		CPUSetPoolToSharedSubgroup: map[string]int{"batch": 3},
	}, &fakeClosManager{clos: []qrmresctrlmanager.CPUListClos{
		{ID: "dedicated"}, {ID: "share-03"}, {ID: "external"},
	}, managed: map[string]struct{}{"dedicated": {}, "share-03": {}}}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0xf0, MinCBMBits: 2},
		1: {CBMMask: 0x0f, MinCBMBits: 2},
	}})

	_, err := plugin.reconcile(context.Background(), 2, map[string]int64{
		"batch":    3,
		"share-03": 4,
	})

	require.NoError(t, err)
	require.Equal(t, []catWrite{
		{clos: "dedicated", mask: map[int]uint64{0: 0x30, 1: 0x03}},
		{clos: "share-03", mask: map[int]uint64{0: 0xf0, 1: 0x0f}},
	}, manager.writes)
}

func TestCATPluginRejectsAllWritesWhenAnyDomainCannotSatisfyWays(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{
		clos: []qrmresctrlmanager.CPUListClos{{ID: "dedicated"}},
	}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0x0f, MinCBMBits: 2},
		1: {CBMMask: 0x03, MinCBMBits: 2},
	}})

	_, err := plugin.reconcile(context.Background(), 3, nil)

	require.Error(t, err)
	require.Empty(t, manager.writes)
}

func TestCATPluginDisabledRestoresRootAndManagedClosOnly(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{
		clos:    []qrmresctrlmanager.CPUListClos{{ID: "dedicated"}, {ID: "external"}},
		managed: map[string]struct{}{"dedicated": {}},
	}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0xff, MinCBMBits: 1},
	}})

	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.DefaultCATWays = 2
	handlerCtx := periodicalContext(conf)
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.EnableCAT = true
	require.NoError(t, plugin.PeriodicalHandler(context.Background(), handlerCtx))
	manager.writes = nil
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.EnableCAT = false
	require.NoError(t, plugin.PeriodicalHandler(context.Background(), handlerCtx))
	require.Equal(t, []catWrite{
		{clos: "dedicated", mask: map[int]uint64{0: 0x03}},
	}, manager.writes)
}

func TestCATPluginPartialFailureRollsBackEveryManagedClos(t *testing.T) {
	manager := &fakeRDTManager{failClos: "share-03", failOnce: true}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{
		clos: []qrmresctrlmanager.CPUListClos{{ID: "dedicated"}, {ID: "share-03"}},
	}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0xff, MinCBMBits: 1},
	}})

	_, err := plugin.reconcile(context.Background(), 2, map[string]int64{"dedicated": 4, "share-03": 4})

	require.ErrorContains(t, err, "apply CAT")
	require.Equal(t, []catWrite{
		{clos: "dedicated", mask: map[int]uint64{0: 0x0f}},
		{clos: "share-03", mask: map[int]uint64{0: 0x0f}},
		{clos: "dedicated", mask: map[int]uint64{0: 0x03}},
		{clos: "share-03", mask: map[int]uint64{0: 0x03}},
	}, manager.writes)
}

func TestCATPluginRestartedDisabledStateIdempotentlyRollsBack(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{
		clos: []qrmresctrlmanager.CPUListClos{{ID: "dedicated"}},
	}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0xff, MinCBMBits: 1},
	}})
	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.DefaultCATWays = 2

	require.NoError(t, plugin.PeriodicalHandler(context.Background(), periodicalContext(conf)))
	require.NoError(t, plugin.PeriodicalHandler(context.Background(), periodicalContext(conf)))

	require.Equal(t, []catWrite{
		{clos: "dedicated", mask: map[int]uint64{0: 0x03}},
		{clos: "dedicated", mask: map[int]uint64{0: 0x03}},
	}, manager.writes)
}

func TestCATPluginRestartedDisabledWithZeroDefaultRestoresCapabilityBaseline(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{
		clos: []qrmresctrlmanager.CPUListClos{{ID: "dedicated"}},
	}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0xf0, MinCBMBits: 2},
		1: {CBMMask: 0x0f, MinCBMBits: 2},
	}})
	conf := dynamicconfig.NewConfiguration()

	require.NoError(t, plugin.PeriodicalHandler(context.Background(), periodicalContext(conf)))
	require.Equal(t, []catWrite{
		{clos: "dedicated", mask: map[int]uint64{0: 0xf0, 1: 0x0f}},
	}, manager.writes)
}

func TestCATPluginDisabledByTopLevelDisableRDT(t *testing.T) {
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{}, &fakeRDTManager{},
		fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
			0: {CBMMask: 0xff, MinCBMBits: 1},
		}})
	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.EnableCAT = true
	conf.AdminQoSConfiguration.QRMPluginConfiguration.RDTConfig.DisableRDT = true

	require.NoError(t, plugin.PeriodicalHandler(context.Background(), periodicalContext(conf)))
	require.Empty(t, plugin.rdtManager.(*fakeRDTManager).writes)
}

func TestCATPluginDisabledWithoutPriorTakeoverIsNoop(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{}, manager,
		fakeCapabilityProvider{err: rdt.ErrCATUnsupported})

	conf := dynamicconfig.NewConfiguration()

	require.NoError(t, plugin.PeriodicalHandler(context.Background(), periodicalContext(conf)))
	require.Empty(t, manager.writes)
}

func TestCATPluginValidatesEveryManagedTargetBeforeApplyingAny(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{
		CPUSetPoolToSharedSubgroup: map[string]int{"batch": 3},
	}, &fakeClosManager{clos: []qrmresctrlmanager.CPUListClos{
		{ID: "dedicated"}, {ID: "share-03"},
	}}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0x0f, MinCBMBits: 1},
	}})

	_, err := plugin.reconcile(context.Background(), 2, map[string]int64{"share-03": 5})

	require.Error(t, err)
	require.Empty(t, manager.writes)
}

func TestCATPluginRollbackValidatesEveryTargetBeforeRestoringRoot(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{
		clos: []qrmresctrlmanager.CPUListClos{{ID: "dedicated"}},
	}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0x0f, MinCBMBits: 1},
	}})
	enabled := dynamicconfig.NewConfiguration()
	enabled.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.EnableCAT = true
	enabled.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.DefaultCATWays = 2
	require.NoError(t, plugin.PeriodicalHandler(context.Background(), periodicalContext(enabled)))
	manager.writes = nil

	disabled := dynamicconfig.NewConfiguration()
	disabled.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.DefaultCATWays = 5
	err := plugin.PeriodicalHandler(context.Background(), periodicalContext(disabled))

	require.Error(t, err)
	require.Empty(t, manager.writes)
}

func TestCATPluginDoesNotTreatForeignSharedPrefixAsOwned(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{
		CPUSetPoolToSharedSubgroup: map[string]int{"batch": 3},
	}, &fakeClosManager{clos: []qrmresctrlmanager.CPUListClos{
		{ID: "share-03"}, {ID: "shared-foreign"},
	}, managed: map[string]struct{}{"share-03": {}}}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0x0f, MinCBMBits: 1},
	}})

	_, err := plugin.reconcile(context.Background(), 2, nil)
	require.NoError(t, err)
	require.Equal(t, []catWrite{{clos: "share-03", mask: map[int]uint64{0: 0x03}}}, manager.writes)
}

func TestCATPluginNormalizesLegacySharedOverride(t *testing.T) {
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{
		CPUSetPoolToSharedSubgroup: map[string]int{"batch": 3},
	}, &fakeClosManager{}, &fakeRDTManager{}, fakeCapabilityProvider{})

	resolved, err := plugin.resolveOverrides(map[string]int64{
		"batch":     3,
		"shared-03": 4,
	})

	require.NoError(t, err)
	require.Equal(t, map[string]int64{"share-03": 4}, resolved)
}

func TestCATPluginTreatsUnsupportedCapabilityAsNoop(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{
		clos: []qrmresctrlmanager.CPUListClos{{ID: "dedicated"}},
	}, manager, fakeCapabilityProvider{err: rdt.ErrCATUnsupported})
	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.EnableCAT = true
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.DefaultCATWays = 2

	require.NoError(t, plugin.PeriodicalHandler(context.Background(), periodicalContext(conf)))
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.EnableCAT = false
	require.NoError(t, plugin.PeriodicalHandler(context.Background(), periodicalContext(conf)))
	require.Empty(t, manager.writes)
	require.Empty(t, manager.invalidated)
	require.False(t, plugin.active)
}

func TestCATPluginCPUSetAdjustmentHandlersAreNoop(t *testing.T) {
	manager := &fakeRDTManager{}
	plugin := NewCATPluginWithManager(&qrmresctrl.ResctrlConfig{}, &fakeClosManager{
		clos: []qrmresctrlmanager.CPUListClos{{ID: "dedicated"}},
	}, manager, fakeCapabilityProvider{capabilities: map[int]rdt.CATCapability{
		0: {CBMMask: 0xff, MinCBMBits: 1},
	}})
	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.EnableCAT = true
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.DefaultCATWays = 2

	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{}))
	require.NoError(t, plugin.CPUSetAdjustmentDisabledHandler(context.Background(), bulkheadapi.HandlerContext{}))
	require.Empty(t, manager.writes)
}
